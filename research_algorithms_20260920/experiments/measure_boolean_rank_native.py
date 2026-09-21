"""Serial complete-cost native PageRank experiment, including all refusals.

Synthetic pair sources, observed process RSS, warm/unspecified OS cache.
Not a physical-cap, Neo4j, customer-demand or general-graph benchmark.
"""

import argparse
from fractions import Fraction
from itertools import combinations
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

from boolean_rank_native_gap_source import validate_native_boolean_source
from boolean_rank_native_pipeline import publish_native_rank_result, NativeRankCertificateRefusal
from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource
from measure_boolean_rank_workflow import (
    audit_boolean_output_pair, capture_process_peak_memory, hash_measurement_file_bytes,
)


MODES = {"factor-generic": ("factor-cg", "generic"),
         "factor-native": ("factor-cg", "native-gap"),
         "class-generic": ("class-cg", "generic"),
         "class-native": ("class-cg", "native-gap")}
CONTROL = "class-native"
CASES = tuple(dict(name=f"{kind}-{suffix}", kind=kind, factors=factors, height=height,
                   alpha=alpha, epsilon=1e-10)
              for kind, factors, height in (("cycle", 16, 4), ("path", 64, 4),
                  ("chorded", 64, 4), ("star", 64, 4), ("dense", 96, 2))
              for suffix, alpha in (("normal", 0.85), ("near", math.nextafter(1., 0.))))
IMPLEMENTATION_FILES = (
    "boolean_rank_sqlite_source.py", "stream_boolean_rank_solver.py",
    "boolean_rank_output_certificate.py", "boolean_rank_pipeline.py",
    "measure_boolean_rank_workflow.py", "boolean_rank_spectral_source.py",
    "boolean_rank_deflated_certificate.py", "boolean_rank_native_gap_source.py",
    "boolean_rank_native_certificate.py", "boolean_rank_native_pipeline.py",
    "measure_boolean_rank_native.py",
)


def iterate_native_fixture_rows(case):
    factors, kind, identifier = case["factors"], case["kind"], 0
    for a, b in combinations(range(factors), 2):
        distance = b-a
        cycle = distance in (1, factors-1)
        present = (cycle if kind == "cycle" else distance == 1 if kind == "path"
                   else a == 0 if kind == "star"
                   else distance in (1, 7, 17, factors-1, factors-7, factors-17) if kind == "chorded"
                   else cycle or (17*a+31*b) % 4 != 0 if kind == "dense" else False)
        if kind not in ("cycle", "path", "star", "chorded", "dense"):
            raise ValueError("unknown native synthetic family")
        if present:
            height = case["height"]*(1+(13*a+7*b) % 3)
            for _ in range(height):
                yield identifier, (a, b), ((17*identifier) % 31+1)/32.
                identifier += 1


def prepare_native_measurement_source(root, case):
    path = root/f"{case['name']}.sqlite"
    preparation = build_boolean_rank_source(path, iterate_native_fixture_rows(case),
        factor_count=case["factors"], cache_kib=256, batch_records=4096)
    build_peak = capture_process_peak_memory()
    started = time.perf_counter()
    with SqliteBooleanRankSource(path, cache_kib=256) as source:
        topology = validate_native_boolean_source(source, max_factor_slots=source.factor_count)
    return dict(kind="preparation", case=case, preparation=preparation,
        vertices=topology["vertices"], classes=topology["class_count"],
        implied_undirected_edges=topology["volume"]//2,
        topology={key: str(value) if isinstance(value, Fraction) else value for key, value in topology.items()},
        postbuild_inspection_seconds=time.perf_counter()-started,
        process_maxrss_bytes=build_peak, database=path.name, database_sha256=hash_measurement_file_bytes(path))


def run_native_measurement_query(root, case, mode, trial):
    method, certificate = MODES[mode]
    output = root/f"{case['name']}-{trial}-{mode}.ranks"
    result = dict(kind="query", case=case["name"], mode=mode, trial=trial,
                  output=output.name, accepted=False)
    started = time.perf_counter()
    with SqliteBooleanRankSource(root/f"{case['name']}.sqlite", cache_kib=256) as source:
        try:
            result["pipeline"] = publish_native_rank_result(source, output,
                alpha=case["alpha"], epsilon=case["epsilon"], method=method,
                certificate_method=certificate, max_factor_slots=source.factor_count,
                max_class_slots=source.active_class_count, precision=60,
                tolerance=1e-13, maximum_iterations=1000)
            result["accepted"] = True
        except (ValueError, ArithmeticError, OSError) as error:
            result.update(failure_type=type(error).__name__, failure=str(error))
            if isinstance(error, NativeRankCertificateRefusal):
                result["pipeline"] = error.receipt
        result["source_events"] = dict(source.events)
        if source.events["active_cursors"]:
            raise AssertionError("native query left live source cursors")
    result.update(query_seconds=time.perf_counter()-started,
                  process_maxrss_bytes=capture_process_peak_memory())
    if result["accepted"]:
        result["postprocess_output_sha256"] = hash_measurement_file_bytes(output)
        if result["postprocess_output_sha256"] != result["pipeline"]["output_sha256"]:
            raise AssertionError("native published output changed")
    elif output.exists():
        raise AssertionError("refused native query published output")
    return result


def summarize_native_control_bracket(before, candidate, after):
    ratio = max(before["query_seconds"], after["query_seconds"])/min(before["query_seconds"], after["query_seconds"])
    result = dict(all_accepted=all(q["accepted"] for q in (before, candidate, after)),
                  control_time_ratio=ratio, timing_stable=ratio <= 1.5)
    if result["all_accepted"]:
        result["candidate_to_control_ratio"] = candidate["query_seconds"]/statistics.mean(
            [before["query_seconds"], after["query_seconds"]])
    return result


def execute_native_measurement_study(root):
    script = Path(__file__).resolve()
    prior_path = script.parent.parent/"evidence/boolean-rank-options-20260921/receipt.json"
    prior = json.loads(prior_path.read_text())
    for name, digest in prior["source_hashes"].items():
        if hash_measurement_file_bytes(script.parent/name) != digest:
            raise ValueError(f"frozen uniform implementation changed: {name}")
    root.mkdir(parents=True, exist_ok=False)
    manifest = dict(schema="boolean-rank-native-v1", platform=platform.platform(), python=sys.version,
        cases=CASES, modes=MODES, preparations=[], queries=[], brackets=[],
        source_hashes={name: hash_measurement_file_bytes(script.parent/name) for name in IMPLEMENTATION_FILES},
        design="three class-native/candidate/class-native brackets per candidate/case; serial fresh workers",
        stability_screen="control max/min <= 1.5; retain every attempt and refusal",
        scope="synthetic connected native pair sources; observed RSS; no cold cache, physical cap or Neo4j comparison")
    with (root/"events.jsonl").open("x") as events:
        def launch_native_measurement_worker(worker, case, mode=CONTROL, trial=0):
            started = time.perf_counter()
            process = subprocess.run([sys.executable, "-B", str(script), "--worker", worker,
                "--directory", str(root), "--case-json", json.dumps(case),
                "--mode", mode, "--trial", str(trial)], capture_output=True, text=True, check=True)
            result = json.loads(process.stdout)
            result["process_wall_seconds"] = time.perf_counter()-started
            events.write(json.dumps(result, sort_keys=True)+"\n")
            events.flush()
            return result

        for case in CASES:
            manifest["preparations"].append(launch_native_measurement_worker("prepare", case))
            for mode in MODES:
                if mode == CONTROL:
                    continue
                for repeat in range(3):
                    trial = len(manifest["queries"])
                    first, candidate, last = [launch_native_measurement_worker("query", case, selected, trial+offset)
                        for offset, selected in enumerate((CONTROL, mode, CONTROL))]
                    manifest["queries"].extend((first, candidate, last))
                    bracket = dict(case=case["name"], candidate=mode, repeat=repeat,
                        trials=[first["trial"], candidate["trial"], last["trial"]],
                        **summarize_native_control_bracket(first, candidate, last))
                    if bracket["all_accepted"]:
                        for label, control in (("before", first), ("after", last)):
                            bound = sum(Fraction(q["pipeline"]["certificate"]["l1_error_upper"])
                                        for q in (candidate, control))
                            bracket[f"audit_{label}"] = audit_boolean_output_pair(
                                root/candidate["output"], root/control["output"], bound)
                    manifest["brackets"].append(bracket)
            queries = [q for q in manifest["queries"] if q["case"] == case["name"]]
            print(f"{case['name']}: {sum(q['accepted'] for q in queries)}/{len(queries)} accepted", flush=True)
    for name, digest in manifest["source_hashes"].items():
        if hash_measurement_file_bytes(script.parent/name) != digest:
            raise AssertionError(f"implementation changed during serial study: {name}")
    for entry in manifest["preparations"]:
        if hash_measurement_file_bytes(root/entry["database"]) != entry["database_sha256"]:
            raise AssertionError("native database changed during study")
    (root/"receipt.json").write_text(json.dumps(manifest, indent=2, sort_keys=True)+"\n")
    print(f"Saved {len(manifest['queries'])} attempts and {len(manifest['brackets'])} brackets", flush=True)


def parse_native_measurement_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--worker", choices=("prepare", "query"))
    parser.add_argument("--case-json")
    parser.add_argument("--mode", choices=MODES, default=CONTROL)
    parser.add_argument("--trial", type=int, default=0)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_native_measurement_arguments()
    if arguments.worker:
        case = json.loads(arguments.case_json)
        result = (prepare_native_measurement_source(arguments.directory, case) if arguments.worker == "prepare"
                  else run_native_measurement_query(arguments.directory, case, arguments.mode, arguments.trial))
        print(json.dumps(result, sort_keys=True))
    else:
        execute_native_measurement_study(arguments.directory.resolve())

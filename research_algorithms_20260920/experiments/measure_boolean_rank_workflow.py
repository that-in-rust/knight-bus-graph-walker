"""Serial prepared-source/complete-publication study of two Boolean CG plans.

Fresh workers do not imply cold OS cache. Synthetic eligibility is supplied,
not discovered. Physical memory is observed, not capped. No Neo4j comparison.
"""

import argparse
from fractions import Fraction
import hashlib
from itertools import combinations
import json
import math
from pathlib import Path
import platform
import resource
import statistics
import struct
import subprocess
import sys
import time

from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource
from boolean_rank_pipeline import publish_boolean_rank_result


CASES = tuple(dict(name=name, kind=kind, active_factors=active, factors=factors,
                   height=height, isolates=isolates, alpha=alpha, epsilon=1e-10)
              for name, kind, active, factors, height, isolates, alpha in (
                  ("pairs-small", "pairs", 8, 8, 4, 0, 0.85),
                  ("pairs-medium", "pairs", 64, 64, 2, 0, 0.85),
                  ("pairs-wide", "pairs", 128, 128, 1, 0, 0.85),
                  ("repeated-pair", "repeat", 2, 2, 20000, 0, 0.85),
                  ("repeated-single", "single", 1, 1, 20000, 0, 0.85),
                  ("cycle-isolates", "cycle", 256, 256, 4, 128, 0.85),
                  ("unused-factors", "repeat", 2, 4096, 4000, 0, 0.85),
                  ("near-one", "pairs", 8, 8, 2, 0, math.nextafter(1.0, 0.0)),
                  ("underflow", "underflow", 1, 1, 1, 1, 0.85)))


def iterate_boolean_fixture_rows(case):
    if case["kind"] == "underflow":
        yield 0, (), 1.0
        yield 1, (0,), 2.0**-540
        yield 2, (0,), 2.0**-540
        return
    active = case["active_factors"]
    if case["kind"] == "pairs":
        signatures = combinations(range(active), 2)
    elif case["kind"] == "repeat":
        signatures = ((0, 1),)
    elif case["kind"] == "single":
        signatures = ((0,),)
    elif case["kind"] == "cycle":
        signatures = ((a, (a + 1) % active) for a in range(active))
    else:
        raise ValueError("unknown synthetic family")
    identifier = 0
    for groups in signatures:
        for _ in range(case["height"]):
            yield identifier, groups, ((17 * identifier) % 31 + 1) / 32.0
            identifier += 1
    for _ in range(case["isolates"]):
        yield identifier, (), 0.5
        identifier += 1


def capture_process_peak_memory():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def hash_measurement_file_bytes(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare_boolean_measurement_source(root, case):
    path = root / f"{case['name']}.sqlite"
    preparation = build_boolean_rank_source(path, iterate_boolean_fixture_rows(case),
        factor_count=case["factors"], cache_kib=256, batch_records=4096)
    peak_after_build = capture_process_peak_memory()
    before = time.perf_counter()
    with SqliteBooleanRankSource(path, cache_kib=256) as source:
        vertices, classes = source.vertex_count, source.active_class_count
        edges = sum(h * degree for _, _, h, degree, _ in source.iterate_class_records()) // 2
    return dict(kind="preparation", case=case, preparation=preparation, vertices=vertices,
                classes=classes, implied_undirected_edges=edges,
                postbuild_inspection_seconds=time.perf_counter() - before,
                process_maxrss_bytes=peak_after_build, database=path.name,
                database_sha256=hash_measurement_file_bytes(path))


def run_boolean_measurement_query(root, case, method, trial):
    path = root / f"{case['name']}.sqlite"
    output = root / f"{case['name']}-{trial}-{method}.ranks"
    result = dict(kind="query", case=case["name"], method=method, trial=trial,
                  output=output.name, accepted=False)
    before = time.perf_counter()
    with SqliteBooleanRankSource(path, cache_kib=256) as source:
        try:
            result["pipeline"] = publish_boolean_rank_result(source, output,
                alpha=case["alpha"], epsilon=case["epsilon"], method=method,
                max_factor_slots=source.factor_count, max_class_slots=source.active_class_count,
                precision=60, maximum_iterations=1000, tolerance=1e-13)
            result["accepted"] = True
        except (ValueError, ArithmeticError, OSError) as error:
            result.update(failure_type=type(error).__name__, failure=str(error))
        result["source_events"] = dict(source.events)
        if source.events["active_cursors"]:
            raise AssertionError("query left a live source cursor")
    result.update(query_seconds=time.perf_counter() - before,
                  process_maxrss_bytes=capture_process_peak_memory())
    if result["accepted"]:
        result["postprocess_output_sha256"] = hash_measurement_file_bytes(output)
        if result["postprocess_output_sha256"] != result["pipeline"]["output_sha256"]:
            raise AssertionError("published output changed after certificate")
    elif output.exists():
        raise AssertionError("refused query published an output")
    return result


def audit_boolean_output_pair(left, right, bound):
    rows, error = 0, Fraction()
    hashes = [hashlib.sha256(), hashlib.sha256()]
    with open(left, "rb") as first, open(right, "rb") as second:
        while True:
            a, b = first.read(16), second.read(16)
            if not a and not b:
                break
            if len(a) != 16 or len(b) != 16:
                raise AssertionError("incomplete pair output")
            u, x = struct.unpack("<Qd", a)
            v, y = struct.unpack("<Qd", b)
            if u != v or not math.isfinite(x) or not math.isfinite(y) or min(x, y) < 0:
                raise AssertionError("invalid paired original-ID output")
            error += abs(Fraction(x) - Fraction(y))
            hashes[0].update(a)
            hashes[1].update(b)
            rows += 1
    if error > Fraction(bound):
        raise AssertionError("two accepted answers disagree beyond summed bounds")
    return dict(rows=rows, l1_difference=str(error), bound=str(bound),
                left_sha256=hashes[0].hexdigest(), right_sha256=hashes[1].hexdigest())


def execute_boolean_workflow_measurement(root):
    root.mkdir(parents=True, exist_ok=False)
    script = Path(__file__).resolve()
    files = ("measure_boolean_rank_workflow.py", "boolean_rank_sqlite_source.py",
             "stream_boolean_rank_solver.py", "boolean_rank_pipeline.py", "boolean_rank_output_certificate.py")
    manifest = dict(schema="boolean-rank-workflow-v1", platform=platform.platform(), python=sys.version,
                    cases=list(CASES), preparations=[], queries=[], pairs=[],
                    source_hashes={name: hash_measurement_file_bytes(script.parent / name) for name in files},
                    design="three serial P/F/P triplets per case; same prepared source; fresh workers; cache not cleared",
                    stability_screen="max(P_before,P_after)/min(P_before,P_after) <= 1.5, predeclared",
                    scope="synthetic eligible native input; observed RSS, not physical cap; ordinary centered P control")
    with (root / "events.jsonl").open("x") as events:
        def launch_boolean_measurement_worker(mode, case, method="class-cg", trial=0):
            started = time.perf_counter()
            process = subprocess.run([sys.executable, "-B", str(script), "--worker", mode,
                "--directory", str(root), "--case-json", json.dumps(case), "--method", method,
                "--trial", str(trial)], capture_output=True, text=True, check=True)
            result = json.loads(process.stdout)
            result["process_wall_seconds"] = time.perf_counter() - started
            events.write(json.dumps(result, sort_keys=True) + "\n")
            events.flush()
            return result

        for case in CASES:
            manifest["preparations"].append(launch_boolean_measurement_worker("prepare", case))
            for repeat in range(3):
                triplet = [launch_boolean_measurement_worker("query", case, method, 3 * repeat + position)
                           for position, method in enumerate(("class-cg", "factor-cg", "class-cg"))]
                manifest["queries"].extend(triplet)
                pair = dict(case=case["name"], repeat=repeat,
                            trials=[entry["trial"] for entry in triplet],
                            all_accepted=all(entry["accepted"] for entry in triplet))
                if pair["all_accepted"]:
                    first, candidate, last = triplet
                    bound = sum(Fraction(entry["pipeline"]["certificate"]["l1_error_upper"])
                                for entry in (candidate, first))
                    pair["audit_before"] = audit_boolean_output_pair(root / candidate["output"], root / first["output"], bound)
                    bound = sum(Fraction(entry["pipeline"]["certificate"]["l1_error_upper"])
                                for entry in (candidate, last))
                    pair["audit_after"] = audit_boolean_output_pair(root / candidate["output"], root / last["output"], bound)
                    pair["control_time_ratio"] = max(first["query_seconds"], last["query_seconds"]) / min(first["query_seconds"], last["query_seconds"])
                    pair["timing_stable"] = pair["control_time_ratio"] <= 1.5
                    pair["candidate_to_control_ratio"] = candidate["query_seconds"] / statistics.mean(
                        [first["query_seconds"], last["query_seconds"]])
                manifest["pairs"].append(pair)
            accepted = sum(q["accepted"] for q in manifest["queries"] if q["case"] == case["name"])
            print(f"{case['name']}: {accepted}/9 accepted", flush=True)
    if any(hash_measurement_file_bytes(script.parent / name) != digest for name, digest in manifest["source_hashes"].items()):
        raise AssertionError("implementation changed during the serial experiment")
    (root / "receipt.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"Saved {len(manifest['queries'])} complete attempts to {root}", flush=True)


def parse_boolean_measurement_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--worker", choices=("prepare", "query"))
    parser.add_argument("--case-json")
    parser.add_argument("--method", choices=("factor-cg", "class-cg"), default="class-cg")
    parser.add_argument("--trial", type=int, default=0)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_boolean_measurement_arguments()
    if arguments.worker:
        case = json.loads(arguments.case_json)
        result = (prepare_boolean_measurement_source(arguments.directory, case) if arguments.worker == "prepare"
                  else run_boolean_measurement_query(arguments.directory, case, arguments.method, arguments.trial))
        print(json.dumps(result, sort_keys=True))
    else:
        execute_boolean_workflow_measurement(arguments.directory.resolve())

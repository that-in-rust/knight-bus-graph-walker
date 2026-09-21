"""Paired actual-publication options on frozen eligible Boolean sources.

Serial fresh processes, cached prepared input, observed rather than capped RSS.
No data regeneration, no reinterpretation of the previous refused outputs.
"""

import argparse
from fractions import Fraction
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

from boolean_rank_certified_options import publish_uniform_rank_result, UniformRankCertificateRefusal
from boolean_rank_sqlite_source import SqliteBooleanRankSource
from measure_boolean_rank_workflow import (
    audit_boolean_output_pair, capture_process_peak_memory, hash_measurement_file_bytes,
)


MODES = {
    "factor-generic": ("factor-cg", "generic"),
    "factor-deflated": ("factor-cg", "deflated"),
    "class-deflated": ("class-cg", "deflated"),
    "direct-generic": ("uniform-pair-direct", "generic"),
    "direct-deflated": ("uniform-pair-direct", "deflated"),
    "direct-exact": ("uniform-pair-direct", "exact-target"),
}
CONTROL = "direct-exact"
CASE_NAMES = ("pairs-small", "pairs-medium", "pairs-wide", "near-one")
IMPLEMENTATION_FILES = (
    "boolean_rank_sqlite_source.py", "stream_boolean_rank_solver.py",
    "boolean_rank_output_certificate.py", "boolean_rank_pipeline.py",
    "measure_boolean_rank_workflow.py", "boolean_rank_spectral_source.py",
    "boolean_rank_deflated_certificate.py", "boolean_rank_uniform_pair_control.py",
    "boolean_rank_certified_options.py", "measure_boolean_rank_options.py",
)


def run_boolean_option_query(source_root, output_root, case, mode, trial):
    solver, certificate = MODES[mode]
    output = output_root / f"{case['name']}-{trial}-{mode}.ranks"
    result = dict(case=case["name"], mode=mode, trial=trial, output=output.name, accepted=False)
    started = time.perf_counter()
    with SqliteBooleanRankSource(source_root / f"{case['name']}.sqlite", cache_kib=256) as source:
        try:
            result["pipeline"] = publish_uniform_rank_result(source, output,
                alpha=case["alpha"], epsilon=case["epsilon"], solver_method=solver,
                certificate_method=certificate, max_factor_slots=source.factor_count,
                max_class_slots=source.active_class_count, precision=60,
                tolerance=1e-13, maximum_iterations=1000)
            result["accepted"] = True
        except (ValueError, ArithmeticError, OSError) as error:
            result.update(failure_type=type(error).__name__, failure=str(error))
            if isinstance(error, UniformRankCertificateRefusal):
                result["pipeline"] = error.receipt
        result["source_events"] = dict(source.events)
        if source.events["active_cursors"]:
            raise AssertionError("option query leaked source cursor")
    result.update(query_seconds=time.perf_counter() - started,
                  process_maxrss_bytes=capture_process_peak_memory())
    if result["accepted"]:
        result["postprocess_output_sha256"] = hash_measurement_file_bytes(output)
        if result["postprocess_output_sha256"] != result["pipeline"]["output_sha256"]:
            raise AssertionError("published option output changed")
    elif output.exists():
        raise AssertionError("refused option published output")
    return result


def summarize_option_control_bracket(before, candidate, after):
    ratio = max(before["query_seconds"], after["query_seconds"]) / min(before["query_seconds"], after["query_seconds"])
    result = dict(all_accepted=all(entry["accepted"] for entry in (before, candidate, after)),
                  control_time_ratio=ratio, timing_stable=ratio <= 1.5)
    if result["all_accepted"]:
        result["candidate_to_control_ratio"] = candidate["query_seconds"] / statistics.mean(
            [before["query_seconds"], after["query_seconds"]])
    return result


def execute_boolean_option_study(source_root, output_root):
    prior_path = source_root / "receipt.json"
    prior = json.loads(prior_path.read_text())
    script = Path(__file__).resolve()
    for name, digest in prior["source_hashes"].items():
        if hash_measurement_file_bytes(script.parent / name) != digest:
            raise ValueError(f"frozen previous-study code changed: {name}")
    preparations = [entry for entry in prior["preparations"] if entry["case"]["name"] in CASE_NAMES]
    if tuple(entry["case"]["name"] for entry in preparations) != CASE_NAMES:
        raise ValueError("expected four previously prepared eligible cases")
    for entry in preparations:
        if hash_measurement_file_bytes(source_root / entry["database"]) != entry["database_sha256"]:
            raise ValueError("frozen input source hash mismatch")
    output_root.mkdir(parents=True, exist_ok=False)
    manifest = dict(schema="boolean-rank-options-v1", platform=platform.platform(), python=sys.version,
        prior_receipt=str(prior_path), prior_receipt_sha256=hash_measurement_file_bytes(prior_path),
        reused_preparations=preparations, queries=[], brackets=[], modes=MODES,
        source_hashes={name: hash_measurement_file_bytes(script.parent / name) for name in IMPLEMENTATION_FILES},
        design="three direct-exact/candidate/direct-exact brackets per candidate/case; fresh serial workers",
        stability_screen="control max/min <= 1.5, predeclared; retain all observations",
        scope="same frozen eligible native sources; reused prep cost separately; no cold cache, physical cap or Neo4j comparison")
    with (output_root / "events.jsonl").open("x") as events:
        def launch_boolean_option_worker(case, mode, trial):
            started = time.perf_counter()
            process = subprocess.run([sys.executable, "-B", str(script), "--worker",
                "--source-directory", str(source_root), "--directory", str(output_root),
                "--case-json", json.dumps(case), "--mode-name", mode, "--trial", str(trial)],
                capture_output=True, text=True, check=True)
            result = json.loads(process.stdout)
            result["process_wall_seconds"] = time.perf_counter() - started
            events.write(json.dumps(result, sort_keys=True) + "\n")
            events.flush()
            manifest["queries"].append(result)
            return result

        for entry in preparations:
            case = entry["case"]
            for mode in MODES:
                if mode == CONTROL:
                    continue
                for repeat in range(3):
                    trial = len(manifest["queries"])
                    first, candidate, last = [launch_boolean_option_worker(case, selected, trial + offset)
                        for offset, selected in enumerate((CONTROL, mode, CONTROL))]
                    bracket = dict(case=case["name"], candidate=mode, repeat=repeat,
                        trials=[first["trial"], candidate["trial"], last["trial"]],
                        **summarize_option_control_bracket(first, candidate, last))
                    if bracket["all_accepted"]:
                        for label, control in (("before", first), ("after", last)):
                            bound = sum(Fraction(item["pipeline"]["certificate"]["l1_error_upper"])
                                        for item in (candidate, control))
                            bracket[f"audit_{label}"] = audit_boolean_output_pair(
                                output_root / candidate["output"], output_root / control["output"], bound)
                    manifest["brackets"].append(bracket)
            queries = [q for q in manifest["queries"] if q["case"] == case["name"]]
            print(f"{case['name']}: {sum(q['accepted'] for q in queries)}/{len(queries)} accepted", flush=True)
    for name, digest in manifest["source_hashes"].items():
        if hash_measurement_file_bytes(script.parent / name) != digest:
            raise AssertionError(f"implementation changed during serial study: {name}")
    for entry in preparations:
        if hash_measurement_file_bytes(source_root / entry["database"]) != entry["database_sha256"]:
            raise AssertionError("source changed during study")
    (output_root / "receipt.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"Saved {len(manifest['queries'])} attempts and {len(manifest['brackets'])} brackets", flush=True)


def parse_boolean_option_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-directory", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--case-json")
    parser.add_argument("--mode-name", choices=MODES)
    parser.add_argument("--trial", type=int, default=0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_boolean_option_arguments()
    if args.worker:
        print(json.dumps(run_boolean_option_query(args.source_directory, args.directory,
            json.loads(args.case_json), args.mode_name, args.trial), sort_keys=True))
    else:
        execute_boolean_option_study(args.source_directory.resolve(), args.directory.resolve())

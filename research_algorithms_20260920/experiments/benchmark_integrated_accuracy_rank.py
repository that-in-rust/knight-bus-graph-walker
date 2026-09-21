"""Actual factor-CG state to certified output; no score-to-factor reconstruction.

Serial binary-direct/candidate/binary-direct workers compare solve + publication,
with prepared-source validation and full outputs. Raw import is separately scoped.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

import accuracy_frontier_rank_certificate as accuracy
import factor_rank_state_solver as solver
import benchmark_precision_publication_comparison as publication
import benchmark_rank_publication_comparison as frozen_driver
import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as streaming
from benchmark_mass_pagerank_sandwich import assess_sandwich_control_pair

METHODS = ("binary_direct", "decimal_direct", "rump_lower", "rump_midpoint", "exact_midpoint")
save_receipt = frozen_driver.persist_publication_experiment_receipt


def prepare_integrated_source_manifest(store, folder):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    manifest = streaming.build_certificate_source_manifest(store, folder/"certificate-metadata.bin")
    save_receipt(folder/"manifest.json", manifest)
    return manifest


def run_integrated_accuracy_session(store, folder, output_folder, method, epsilon):
    if method not in METHODS or not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid integrated method or epsilon")
    start, cpu_start = time.perf_counter(), time.process_time()
    folder, output_folder, store = Path(folder), Path(output_folder), Path(store)
    manifest = json.loads((folder/"manifest.json").read_text())
    metadata = folder/"certificate-metadata.bin"
    n, f, _, _ = streaming.validate_certificate_manifest_identity(store, metadata, manifest)
    identity_reads = store.stat().st_size+metadata.stat().st_size
    queries = []
    for j, source in enumerate((None, 0, n-1)):
        query_start = time.perf_counter()
        result, solved, state_hash, h = None, None, None, None
        try:
            h, solved = solver.solve_incidence_factor_state(store, source, epsilon)
            state_hasher = hashlib.sha256()
            for value in h:
                state_hasher.update(base.DOUBLE.pack(value))
            state_hash = state_hasher.hexdigest()
            output = output_folder/f"rank-{j}.bin"
            if method == "binary_direct":
                result = publication.publish_binary64_control_certificate(store, metadata, manifest, h, output, source, epsilon)
            elif method == "decimal_direct":
                result = frozen_driver.publish_direct_rank_certificate(store, metadata, manifest, h, output, source, epsilon)
            else:
                row_sum = "exact" if method == "exact_midpoint" else "rump"
                representative = "lower" if method == "rump_lower" else "midpoint"
                result = accuracy.publish_accuracy_frontier_certificate(store, metadata, manifest, h, output, source, epsilon,
                    row_sum=row_sum, representative=representative, summation_bound="rump")
        except Exception as error:
            result = {"accepted": False, "error": f"{type(error).__name__}: {error}",
                      "output_bytes": 0, "source_zero_based": source}
        finally:
            h = None
        result.update(solver=solved, state_sha256=state_hash, total_query_ms=1000*(time.perf_counter()-query_start))
        queries.append(result)
    session_ms, cpu_ms = 1000*(time.perf_counter()-start), 1000*(time.process_time()-cpu_start)
    peak = frozen_driver.measure_current_worker_rss()
    audit_start = time.perf_counter()
    for j, query in enumerate(queries):
        try:
            query["audit"] = (base.certify_published_rank_output(store, output_folder/f"rank-{j}.bin",
                query["source_zero_based"], epsilon) if query["accepted"] else None)
        except Exception as error:
            query["audit"] = {"accepted": False, "error": f"{type(error).__name__}: {error}"}
    return {"method": method, "epsilon": epsilon, "queries": queries, "total_session_ms": session_ms,
            "session_cpu_ms": cpu_ms, "wall_cpu_ratio": session_ms/cpu_ms,
            "session_identity_read_bytes": identity_reads, "score_to_factor_mapping_scans": 0,
            "peak_rss_before_audit_bytes": peak, "peak_rss_after_audit_bytes": frozen_driver.measure_current_worker_rss(),
            "audit_ms_outside_timing": 1000*(time.perf_counter()-audit_start),
            "all_accepted": all(q["accepted"] and q["audit"]["accepted"] for q in queries),
            "scope": "prepared-source identity, actual CG solve and publication; import/shared manifest build and independent audit excluded"}


def execute_integrated_worker_session(store, folder, method, epsilon):
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    with tempfile.TemporaryDirectory(prefix="rank-integrated-worker-") as output:
        command = [sys.executable, "-B", str(Path(__file__).resolve()), "--store", str(store),
                   "--folder", str(folder), "--worker", method, "--epsilon", str(epsilon), "--output-folder", output]
        start = time.perf_counter()
        done = subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        result = json.loads(done.stdout)
        result["child_wall_ms_including_audit"] = 1000*(time.perf_counter()-start)
        return result


def collect_integrated_worker_outcome(store, folder, method, epsilon):
    start = time.perf_counter()
    try:
        return execute_integrated_worker_session(store, folder, method, epsilon)
    except Exception as error:
        detail = getattr(error, "stderr", None) or str(error)
        return {"method": method, "epsilon": epsilon, "all_accepted": False, "queries": [], "total_session_ms": None,
                "error": f"{type(error).__name__}: {detail}", "failed_child_wall_ms": 1000*(time.perf_counter()-start)}


def main_integrated_accuracy_experiment():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--folder", type=Path, required=True)
    parser.add_argument("--worker", choices=METHODS)
    parser.add_argument("--output-folder", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--epsilon", type=float, default=1e-10)
    parser.add_argument("--admission-only", action="store_true")
    args = parser.parse_args()
    if args.worker:
        if args.output_folder is None:
            parser.error("worker requires output folder")
        print(json.dumps(run_integrated_accuracy_session(args.store, args.folder, args.output_folder, args.worker, args.epsilon)))
        return
    if args.receipt is None or args.repeats < 1 or not math.isfinite(args.epsilon) or args.epsilon <= 0:
        parser.error("receipt, positive repeats and finite positive epsilon required")
    manifest = prepare_integrated_source_manifest(args.store, args.folder)
    modules = [__file__, accuracy.__file__, solver.__file__, publication.__file__, publication.binary.__file__,
               frozen_driver.__file__, streaming.__file__, accuracy.placed.__file__, base.__file__]
    triplets = []
    result = {"status": "running", "manifest_preparation": manifest, "epsilon": args.epsilon,
        "triplets": triplets, "reference": "binary_direct", "admission_only": args.admission_only,
        "code_sha256": {Path(p).name: base.hash_file_content_bytes(p) for p in modules},
        "environment": {"platform": platform.platform(), "python": sys.version},
        "protocol": "three-query actualCG plus publication;fresh serial binary_direct/candidate/binary_direct;15percentcontrolscreen;alloutcomesretained",
        "scope": "solve-to-complete-certified-output; prepared source import and shared manifest build separately scoped"}
    save_receipt(args.receipt, result)
    if args.admission_only:
        result["workers"] = []
        for method in METHODS:
            result["workers"].append(collect_integrated_worker_outcome(args.store, args.folder, method, args.epsilon))
            save_receipt(args.receipt, result)
            print(json.dumps({"method": method, "all_accepted": result["workers"][-1]["all_accepted"]}), flush=True)
    else:
        for repeat in range(args.repeats):
            methods = ["rump_lower", "rump_midpoint", "exact_midpoint", "decimal_direct"]
            methods = methods[repeat % 4:]+methods[:repeat % 4]
            for method in methods:
                before = collect_integrated_worker_outcome(args.store, args.folder, "binary_direct", args.epsilon)
                candidate = collect_integrated_worker_outcome(args.store, args.folder, method, args.epsilon)
                after = collect_integrated_worker_outcome(args.store, args.folder, "binary_direct", args.epsilon)
                all_accepted = all(w["all_accepted"] for w in (before, candidate, after))
                assessment = (assess_sandwich_control_pair(before["total_session_ms"], candidate["total_session_ms"], after["total_session_ms"])
                    if all_accepted else {"locally_stable": False, "control_spread_ratio": None, "candidate_control_ratio": None,
                                          "scope": "failed admission or worker; no time comparison"})
                triplets.append(dict(assessment, method=method, repeat=repeat, all_accepted=all_accepted,
                                     before=before, candidate=candidate, after=after))
                save_receipt(args.receipt, result)
                print(json.dumps({"done": len(triplets), "method": method, "all_accepted": all_accepted, **assessment}), flush=True)
    result["status"] = "complete"
    save_receipt(args.receipt, result)


if __name__ == "__main__":
    main_integrated_accuracy_experiment()

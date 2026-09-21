"""Serial control-candidate-control sessions, retaining unstable observations."""

import argparse
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

import probe_mass_projected_pagerank as mass
import probe_native_incidence_pagerank as base


def assess_sandwich_control_pair(before_ms, candidate_ms, after_ms):
    if any(not math.isfinite(value) or value <= 0 for value in (before_ms, candidate_ms, after_ms)):
        raise ValueError("positive finite session times required")
    spread = max(before_ms, after_ms)/min(before_ms, after_ms)
    stable = spread <= 1.15
    return {"locally_stable": stable, "control_spread_ratio": spread,
            "candidate_control_ratio": candidate_ms/math.sqrt(before_ms*after_ms) if stable else None,
            "scope": "15percent endpoint consistency screen; not proof of isolation or constant interior speed"}


def execute_isolated_session_worker(store, method, env):
    with tempfile.TemporaryDirectory(prefix="knight-bus-paired-session-") as folder:
        command = [sys.executable, str(Path(__file__).resolve()), "--store", str(store),
                   "--worker", method, "--folder", folder]
        started = time.perf_counter()
        child = subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        result = json.loads(child.stdout)
        result["child_wall_ms"] = 1000*(time.perf_counter()-started)
        return result


def main_paired_session_experiment():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--worker", choices=mass.MODES)
    parser.add_argument("--folder", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--modes", choices=mass.MODES, nargs="+",
                        default=[method for method in mass.MODES if method != "fused"])
    args = parser.parse_args()
    if args.worker:
        if args.folder is None:
            parser.error("worker requires external folder")
        cpu_started = time.process_time()
        result = mass.run_selected_method_session(args.store, args.folder, args.worker)
        result["session_cpu_ms"] = 1000*(time.process_time()-cpu_started)
        result["wall_cpu_ratio"] = result["total_session_ms"]/result["session_cpu_ms"]
        print(json.dumps(result))
        return
    if not args.receipt or args.repeats < 1 or len(set(args.modes)) != len(args.modes):
        parser.error("unique modes, positive repeats and a receipt required")
    environment = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    triplets = []
    for repeat in range(args.repeats):
        order = args.modes[repeat % len(args.modes):]+args.modes[:repeat % len(args.modes)]
        for method in order:
            before = execute_isolated_session_worker(args.store, "fused", environment)
            candidate = execute_isolated_session_worker(args.store, method, environment)
            after = execute_isolated_session_worker(args.store, "fused", environment)
            assessment = assess_sandwich_control_pair(before["total_session_ms"], candidate["total_session_ms"],
                                                      after["total_session_ms"])
            triplet = dict(assessment, repeat=repeat, method=method,
                           before=before, candidate=candidate, after=after)
            triplets.append(triplet)
            print(json.dumps({"done":len(triplets), "method":method,
                              "locally_stable":assessment["locally_stable"],
                              "candidate_control_ratio":assessment["candidate_control_ratio"]}), flush=True)
    result = {"triplets":triplets, "driver_sha256":base.hash_file_content_bytes(__file__),
              "probe_sha256":base.hash_file_content_bytes(mass.__file__),
              "base_probe_sha256":base.hash_file_content_bytes(base.__file__),
              "store_sha256":base.hash_file_content_bytes(args.store),
              "environment":{"platform":platform.platform(),"python":sys.version,"thread_cap":1},
              "protocol":"fused control, candidate, fused control; all fresh serial 3query workers; retain all outcomes; 15percent predeclared endpoint screen",
              "source_reference":"PageRank-Native-Incidence-Evidence.md: separately licensed MovieLens binary incidence projection",
              "scope":"prepared-source session costs including method setup; source ingestion not remeasured; cached local Python"}
    args.receipt.write_text(json.dumps(result,indent=2)+"\n",encoding="ascii")


if __name__ == "__main__":
    main_paired_session_experiment()

"""Serial paired publication study with phased binary64 direct certification.

Frozen publication modules are reused without edits. This is a publication-stage
experiment, not a new solver or end-to-end process-memory guarantee.
"""

import argparse
from array import array
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

import benchmark_rank_publication_comparison as frozen_driver
import binary64_rank_residual_certificate as binary
import precision_placed_rank_certificate as precision
import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as streaming
from benchmark_mass_pagerank_sandwich import assess_sandwich_control_pair

prepare_shared_factor_states = frozen_driver.prepare_shared_factor_states
publish_direct_rank_certificate = frozen_driver.publish_direct_rank_certificate
persist_publication_experiment_receipt = frozen_driver.persist_publication_experiment_receipt
METHODS = ("direct", "direct_same", "stream", "stream_rump", "precision", "binary_direct")


def publish_binary64_control_certificate(store, metadata, manifest, h, output, source, epsilon, same_output=False):
    """Consume exclusively owned h before allocating direct-certificate endpoints."""
    started = time.perf_counter()
    store, metadata, output = Path(store), Path(metadata), Path(output)
    if output.resolve() in (store.resolve(), metadata.resolve()):
        raise ValueError("output must not replace source or metadata")
    precision.validate_gradual_binary64_environment()
    n, f, _, isolates = streaming.validate_certificate_manifest_identity(store, metadata, manifest)
    store_bytes, metadata_bytes = store.stat().st_size, metadata.stat().st_size
    if (source is not None and (type(source) is not int or not 0 <= source < n)
            or not math.isfinite(epsilon) or epsilon <= 0):
        raise ValueError("invalid source or tolerance")
    if not isinstance(h, array) or h.typecode != "d" or h.itemsize != 8 or len(h) != f:
        raise ValueError("one owned binary64 state required")
    if any(not math.isfinite(v) for v in h):
        raise ValueError("nonfinite factor state")
    if 16*f > 2**30:
        raise ValueError("factor payload budget exceeded")
    source_isolate = False
    if source is not None:
        with metadata.open("rb") as handle:
            handle.seek(streaming.HEADER.size+source)
            flag = handle.read(1)
        if flag not in (b"\0", b"\1"):
            raise ValueError("invalid isolate flag")
        source_isolate = flag == b"\1"
    pz = isolates/n if source is None else float(source_isolate)
    bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
    preparation_ms = 1000*(time.perf_counter()-started)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix="rank-binary-direct-", dir=output.parent, delete=False) as handle:
            temporary = Path(handle.name)
            for i, (degree, ids) in enumerate(base.iter_incidence_row_records(store)):
                b = bscale*base.select_personalization_node_value(i, n, source)
                value = degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b
                if not math.isfinite(value) or value < 0:
                    raise ValueError("inadmissible reconstructed score")
                encoded = base.DOUBLE.pack(value)
                if handle.write(encoded) != len(encoded):
                    raise OSError("incomplete rank write")
        # This is an ownership-consuming API, unlike the frozen direct wrapper.
        del h[:]
        certificate = binary.certify_binary64_rank_output(store, temporary, source, epsilon)
        digest = base.hash_file_content_bytes(temporary)
        total_ms = 1000*(time.perf_counter()-started)
        result = {"accepted": certificate["accepted"], "l1_error_upper": certificate["l1_error_upper"],
                  "certificate": certificate, "preparation_ms": preparation_ms,
                  "publication_ms": total_ms-preparation_ms, "total_ms": total_ms,
                  "publication_row_scans": 3, "retained_decimal_vector_values": 0,
                  "factor_state_consumed": True, "selected_factor_payload_peak_bytes": 16*f,
                  "output_bytes": 8*n if certificate["accepted"] else 0,
                  "output_sha256": digest if certificate["accepted"] else None,
                  "candidate_output_sha256": digest,
                  "logical_read_bytes": 4*store_bytes+metadata_bytes+24*n+int(source is not None),
                  "logical_write_bytes": 8*n, "source_zero_based": source,
                  "read_counter_scope": "modeled bulk payload; small header reads excluded",
                  "scope": "state consumed even on numerical refusal; selected payload not whole RAM"}
        if certificate["accepted"]:
            os.replace(temporary, output)
            temporary = None
        result["total_ms"] = 1000*(time.perf_counter()-started)
        result["publication_ms"] = result["total_ms"]-preparation_ms
        return result
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def load_private_comparison_driver(name):
    spec = importlib.util.spec_from_file_location(name, frozen_driver.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_precision_driver = load_private_comparison_driver("_precision_comparison_driver")
_precision_driver.streaming = SimpleNamespace(**dict(vars(streaming),
    publish_streaming_rank_certificate=precision.publish_streaming_rank_certificate))
_binary_driver = load_private_comparison_driver("_binary_comparison_driver")
_binary_driver.publish_direct_rank_certificate = publish_binary64_control_certificate


def run_publication_stage_session(store, folder, output_folder, method):
    if method == "precision":
        result = _precision_driver.run_publication_stage_session(store, folder, output_folder, "stream_rump")
    elif method == "binary_direct":
        result = _binary_driver.run_publication_stage_session(store, folder, output_folder, "direct")
    else:
        result = frozen_driver.run_publication_stage_session(store, folder, output_folder, method)
    result["method"] = method
    return result


def execute_publication_worker_session(store, folder, method):
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    with tempfile.TemporaryDirectory(prefix="precision-publication-worker-") as output:
        command = [sys.executable, str(Path(__file__).resolve()), "--store", str(store),
                   "--folder", str(folder), "--worker", method, "--output-folder", output]
        started = time.perf_counter()
        completed = subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        result = json.loads(completed.stdout)
        result["child_wall_ms_including_audit"] = 1000*(time.perf_counter()-started)
        return result


def collect_publication_worker_outcome(store, folder, method):
    started = time.perf_counter()
    try:
        return execute_publication_worker_session(store, folder, method)
    except Exception as error:
        detail = getattr(error, "stderr", None) or str(error)
        return {"method": method, "all_accepted": False, "queries": [], "total_session_ms": None,
                "error": f"{type(error).__name__}: {detail}",
                "failed_child_wall_ms": 1000*(time.perf_counter()-started)}


def main_publication_comparison_experiment():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--folder", type=Path, required=True)
    parser.add_argument("--worker", choices=METHODS)
    parser.add_argument("--output-folder", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--reuse-preparation", action="store_true")
    args = parser.parse_args()
    if args.worker:
        if args.output_folder is None:
            parser.error("worker requires output folder")
        print(json.dumps(run_publication_stage_session(args.store, args.folder, args.output_folder, args.worker)))
        return
    if args.receipt is None or args.repeats < 1:
        parser.error("receipt and positive repeats required")
    args.folder.mkdir(parents=True, exist_ok=True)
    preparation = (json.loads((args.folder/"preparation.json").read_text()) if args.reuse_preparation
                   else prepare_shared_factor_states(args.store, args.folder))
    triplets = []
    files = [__file__, precision.__file__, binary.__file__, frozen_driver.__file__, streaming.__file__, base.__file__]
    result = {"preparation": preparation, "preparation_reused": args.reuse_preparation,
              "preparation_sha256": base.hash_file_content_bytes(args.folder/"preparation.json"),
              "triplets": triplets, "status": "running",
              "code_sha256": {Path(p).name: base.hash_file_content_bytes(p) for p in files},
              "store_sha256": base.hash_file_content_bytes(args.store),
              "environment": {"platform": platform.platform(), "python": sys.version},
              "protocol": "3query direct-candidate-direct fresh serial workers;15percent control screen;all outcomes retained",
              "scope": "publication only; frozen source/state preparation separately reported; no solver or full physical-cap claim"}
    persist_publication_experiment_receipt(args.receipt, result)
    for repeat in range(args.repeats):
        methods = ["precision", "binary_direct", "stream_rump"]
        methods = methods[repeat % 3:]+methods[:repeat % 3]
        for method in methods:
            before = collect_publication_worker_outcome(args.store, args.folder, "direct")
            candidate = collect_publication_worker_outcome(args.store, args.folder, method)
            after = collect_publication_worker_outcome(args.store, args.folder, "direct")
            all_accepted = all(w["all_accepted"] for w in (before, candidate, after))
            assessment = (assess_sandwich_control_pair(before["total_session_ms"], candidate["total_session_ms"], after["total_session_ms"])
                          if all_accepted else {"locally_stable": False, "control_spread_ratio": None,
                                                "candidate_control_ratio": None, "scope": "failed admission or worker; no time comparison"})
            triplets.append(dict(assessment, method=method, repeat=repeat, all_accepted=all_accepted,
                                 before=before, candidate=candidate, after=after))
            persist_publication_experiment_receipt(args.receipt, result)
            print(json.dumps({"done": len(triplets), "method": method, "all_accepted": all_accepted, **assessment}), flush=True)
    result["status"] = "complete"
    persist_publication_experiment_receipt(args.receipt, result)


if __name__ == "__main__":
    main_publication_comparison_experiment()

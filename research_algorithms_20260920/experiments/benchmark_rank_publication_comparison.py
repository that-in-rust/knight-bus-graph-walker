"""Fresh-worker publication comparison; this is NOT a complete solver benchmark."""

import argparse
from array import array
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR
import json
import math
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import tempfile
import time

import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as streaming
from benchmark_mass_pagerank_sandwich import assess_sandwich_control_pair


def measure_current_worker_rss():
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return maximum if sys.platform == "darwin" else 1024*maximum


def prepare_shared_factor_states(store, folder):
    started = time.perf_counter()
    folder = Path(folder)
    metadata = folder/"certificate-metadata.bin"
    manifest = streaming.build_certificate_source_manifest(store, metadata)
    n, f, _ = base.read_incidence_store_header(store)
    states = []
    for j, source in enumerate((None, 0, n-1)):
        rank = folder/f"upstream-{j}.bin"
        solved = base.run_incidence_rank_solver(store, rank, "cg", source, 1e-12)
        mapping_started = time.perf_counter()
        h = base.create_zero_float_vector(f)
        with rank.open("rb") as scores:
            for degree, ids in base.iter_incidence_row_records(store):
                value = base.DOUBLE.unpack(scores.read(8))[0]
                if degree:
                    for factor in ids:
                        h[factor] += value/degree
        state = folder/f"factor-{j}.bin"
        with state.open("wb") as handle:
            for value in h:
                handle.write(base.DOUBLE.pack(value))
        states.append({"source_zero_based": source, "upstream": solved,
                       "state_sha256": base.hash_file_content_bytes(state),
                       "state_bytes": 8*f, "mapping_row_scans": 1,
                       "mapping_logical_read_bytes": Path(store).stat().st_size+8*n+8*f,
                       "mapping_logical_write_bytes": 8*f,
                       "mapping_ms": 1000*(time.perf_counter()-mapping_started)})
    result = {"manifest": manifest, "states": states, "total_preparation_ms": 1000*(time.perf_counter()-started),
              "peak_preparer_rss_bytes": measure_current_worker_rss(),
              "scope": "CG-certified scores mapped to shared factor states; paid upstream work, NOT a new solver"}
    (folder/"preparation.json").write_text(json.dumps(result, indent=2)+"\n", encoding="ascii")
    return result


def publish_direct_rank_certificate(store, metadata, manifest, h, output, source, epsilon, same_output=False):
    started = time.perf_counter()
    if Path(output).resolve() in (Path(store).resolve(), Path(metadata).resolve()):
        raise ValueError("output must not replace source or metadata")
    n, f, _, isolates = streaming.validate_certificate_manifest_identity(store, metadata, manifest)
    store_bytes, metadata_bytes = Path(store).stat().st_size, Path(metadata).stat().st_size
    source_isolate = False
    if source is not None:
        with Path(metadata).open("rb") as handle:
            handle.seek(streaming.HEADER.size+source)
            source_isolate = handle.read(1) == b"\1"
    pz = isolates/n if source is None else float(source_isolate)
    bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
    lower, upper = Context(prec=50, rounding=ROUND_FLOOR), Context(prec=50, rounding=ROUND_CEILING)
    denominator = Decimal(20*n-17*isolates)
    uniform_lo = lower.divide(Decimal(3), denominator)
    uniform_hi = upper.divide(Decimal(3), denominator)
    preparation_ms = 1000*(time.perf_counter()-started)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix="rank-direct-", dir=Path(output).parent, delete=False) as handle:
            temporary = Path(handle.name)
            for i, (degree, ids) in enumerate(base.iter_incidence_row_records(store)):
                if same_output:
                    if source is None:
                        bl, bh = uniform_lo, uniform_hi
                    elif i == source:
                        bl = bh = Decimal(1) if source_isolate else Decimal("0.15")
                    else:
                        bl = bh = Decimal(0)
                    if degree:
                        lo, _ = streaming.enclose_reconstructed_row_value(h, ids, degree, bl, bh, lower, upper)
                        value = degree*float(lo)
                    else:
                        value = float(bl)
                else:
                    b = bscale*base.select_personalization_node_value(i, n, source)
                    value = degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b
                handle.write(base.DOUBLE.pack(value))
        certificate = base.certify_published_rank_output(store, temporary, source, epsilon)
        digest = base.hash_file_content_bytes(temporary)
        total_ms = 1000*(time.perf_counter()-started)
        result = {"accepted": certificate["accepted"], "l1_error_upper": certificate["l1_error_upper"],
                "certificate": certificate, "preparation_ms": preparation_ms,
                "publication_ms": total_ms-preparation_ms, "total_ms": total_ms,
                "publication_row_scans": 3, "retained_decimal_vector_values": 2*f,
                "output_bytes": 8*n if certificate["accepted"] else 0,
                "output_sha256": digest if certificate["accepted"] else None,
                "logical_read_bytes": 2*store_bytes+metadata_bytes
                +certificate["logical_read_bytes"]+8*n+int(source is not None),
                "logical_write_bytes": 8*n, "source_zero_based": source,
                "read_counter_scope": "modeled bulk payload; small header reads excluded"}
        if certificate["accepted"]:
            os.replace(temporary, output)
            temporary = None
        result["total_ms"] = 1000*(time.perf_counter()-started)
        result["publication_ms"] = result["total_ms"]-preparation_ms
        return result
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def run_publication_stage_session(store, folder, output_folder, method):
    if method not in ("direct", "direct_same", "stream", "stream_rump"):
        raise ValueError("invalid publication method")
    started, cpu_started = time.perf_counter(), time.process_time()
    folder, output_folder = Path(folder), Path(output_folder)
    preparation = json.loads((folder/"preparation.json").read_text())
    manifest, metadata = preparation["manifest"], folder/"certificate-metadata.bin"
    n, f, _ = base.read_incidence_store_header(store)
    if 16*f > 2**30:
        raise ValueError("factor state payload budget exceeded before input allocation")
    states = preparation.get("states")
    if (not isinstance(states, list) or len(states) != 3
            or any(not isinstance(s, dict) for s in states)
            or [s.get("source_zero_based", "missing") for s in states] != [None, 0, n-1]
            or any(s.get("state_bytes") != 8*f or not isinstance(s.get("state_sha256"), str) for s in states)):
        raise ValueError("invalid three-query workload")
    for j, state in enumerate(preparation["states"]):
        state_path = folder/f"factor-{j}.bin"
        if state_path.stat().st_size != 8*f or base.hash_file_content_bytes(state_path) != state["state_sha256"]:
            raise ValueError("factor state identity or length mismatch")
    queries = []
    for j, state in enumerate(states):
        query_started = time.perf_counter()
        try:
            h = array("d")
            with (folder/f"factor-{j}.bin").open("rb") as handle:
                for _ in range(f):
                    h.append(base.DOUBLE.unpack(handle.read(8))[0])
            output = output_folder/f"rank-{j}.bin"
            if method in ("direct", "direct_same"):
                result = publish_direct_rank_certificate(store, metadata, manifest, h, output,
                                                         state["source_zero_based"], 1e-10, method == "direct_same")
            else:
                result = streaming.publish_streaming_rank_certificate(
                    store, metadata, manifest, h, output, state["source_zero_based"], 1e-10,
                    summation_bound="rump" if method == "stream_rump" else "gamma")
        except Exception as error:
            result = {"accepted": False, "error": f"{type(error).__name__}: {error}",
                      "source_zero_based": state["source_zero_based"], "output_bytes": 0,
                      "total_ms": 1000*(time.perf_counter()-query_started),
                      "scope": "failed query elapsed includes loading; work counters unavailable"}
        result["state_sha256"] = state["state_sha256"]
        result["state_load_read_bytes"] = 16*f
        queries.append(result)
    session_ms = 1000*(time.perf_counter()-started)
    cpu_ms = 1000*(time.process_time()-cpu_started)
    peak_before_audit = measure_current_worker_rss()
    # The SAME direct-output audit runs only after timed work and RSS capture.
    audit_started = time.perf_counter()
    for j, query in enumerate(queries):
        try:
            query["audit"] = (base.certify_published_rank_output(
                store, output_folder/f"rank-{j}.bin", query["source_zero_based"], 1e-10)
                if query["accepted"] else None)
        except Exception as error:
            query["audit"] = {"accepted": False, "error": f"{type(error).__name__}: {error}"}
    return {"method": method, "queries": queries, "total_session_ms": session_ms,
            "total_stage_ms": sum(q["total_ms"] for q in queries), "session_cpu_ms": cpu_ms,
            "wall_cpu_ratio": session_ms/cpu_ms, "peak_rss_before_audit_bytes": peak_before_audit,
            "audit_ms_outside_timing": 1000*(time.perf_counter()-audit_started),
            "peak_rss_after_audit_bytes": measure_current_worker_rss(),
            "all_accepted": all(q["accepted"] and q["audit"]["accepted"] for q in queries),
            "scope": "fresh publication worker, solve/build upstream; independent audit excluded and separately reported"}


def execute_publication_worker_session(store, folder, method):
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    with tempfile.TemporaryDirectory(prefix="rank-publication-worker-") as output:
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


def persist_publication_experiment_receipt(path, result):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix="publication-receipt-", dir=Path(path).parent,
                                         mode="w", encoding="ascii", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(result, handle, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main_publication_comparison_experiment():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--folder", type=Path, required=True)
    parser.add_argument("--worker", choices=("direct", "direct_same", "stream", "stream_rump"))
    parser.add_argument("--output-folder", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.worker:
        if args.output_folder is None:
            parser.error("worker requires output folder")
        print(json.dumps(run_publication_stage_session(args.store, args.folder, args.output_folder, args.worker)))
        return
    if args.receipt is None or args.repeats < 1:
        parser.error("receipt and positive repeats required")
    args.folder.mkdir(parents=True, exist_ok=True)
    preparation = prepare_shared_factor_states(args.store, args.folder)
    triplets = []
    result = {"preparation": preparation, "triplets": triplets, "status": "running",
              "driver_sha256": base.hash_file_content_bytes(__file__),
              "certificate_sha256": base.hash_file_content_bytes(streaming.__file__),
              "base_probe_sha256": base.hash_file_content_bytes(base.__file__),
              "store_sha256": base.hash_file_content_bytes(args.store),
              "environment": {"platform": platform.platform(), "python": sys.version},
              "protocol": "3query direct-candidate-direct fresh serial workers;15percent control screen;all outcomes retained",
              "scope": "publication stage only; source import and solver not improved or timed as candidate"}
    persist_publication_experiment_receipt(args.receipt, result)
    for repeat in range(args.repeats):
        methods = ["stream", "stream_rump", "direct_same"]
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

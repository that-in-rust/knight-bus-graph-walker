"""Streamed coarse correction and paid reused-dense controls, research only."""

import argparse
from array import array
import json
import math
import os
from pathlib import Path
import platform
import struct
import subprocess
import sys
import tempfile
import time

import probe_native_incidence_pagerank as base

META_HEADER = struct.Struct("<8sQQ32sd")
META_RECORD = struct.Struct("<Qd")
META_MAGIC = b"KBMASS01"
MODES = ("fused", "filepower", "reduced", "cg", "vertexcg", "projected", "dense_reuse",
         "relaxed", "normalized", "projected_relaxed")


def build_factor_correction_records(store, metadata):
    started = time.perf_counter()
    n, f, z = base.read_incidence_store_header(store)
    sizes, u = array("Q", [0])*f, base.create_zero_float_vector(f)
    for degree, ids in base.iter_incidence_row_records(store):
        if degree:
            contribution = (1-base.ALPHA)*degree/(degree+base.ALPHA*len(ids))
            for j in ids:
                sizes[j] += 1
                u[j] += contribution
    e_scalar = math.fsum(sizes[j]*u[j] for j in range(f))
    if f and (not math.isfinite(e_scalar) or e_scalar <= 0):
        raise ValueError("nonpositive coarse denominator")
    source_hash = base.hash_file_content_bytes(store)
    with Path(metadata).open("wb") as handle:
        handle.write(META_HEADER.pack(META_MAGIC, n, f, bytes.fromhex(source_hash), e_scalar))
        for j in range(f):
            handle.write(META_RECORD.pack(sizes[j], u[j]))
    return {"vertices": n, "factors": f, "memberships": z,
            "setup_ms": 1000*(time.perf_counter()-started), "entity_scans": 1,
            "logical_read_bytes": 2*Path(store).stat().st_size,
            "metadata_bytes": Path(metadata).stat().st_size,
            "builder_array_payload_bytes": 16*f, "source_sha256": source_hash,
            "scope": "resident F-sized metadata builder, not bounded external construction"}


def read_bound_metadata_header(store, metadata):
    with Path(metadata).open("rb") as handle:
        raw = handle.read(META_HEADER.size)
    if len(raw) != META_HEADER.size:
        raise ValueError("truncated correction metadata")
    magic, n, f, digest, e_scalar = META_HEADER.unpack(raw)
    actual_n, actual_f, _ = base.read_incidence_store_header(store)
    if magic != META_MAGIC or (n, f) != (actual_n, actual_f):
        raise ValueError("correction metadata source identity mismatch")
    if digest.hex() != base.hash_file_content_bytes(store):
        raise ValueError("correction metadata source identity mismatch")
    if Path(metadata).stat().st_size != META_HEADER.size+f*META_RECORD.size:
        raise ValueError("invalid correction metadata length")
    if not math.isfinite(e_scalar) or (f and e_scalar <= 0) or (not f and e_scalar != 0):
        raise ValueError("invalid coarse denominator")
    return n, f, e_scalar


def iter_factor_correction_records(metadata, f):
    with Path(metadata).open("rb") as handle:
        handle.seek(META_HEADER.size)
        for _ in range(f):
            raw = handle.read(META_RECORD.size)
            if len(raw) != META_RECORD.size:
                raise ValueError("truncated correction record")
            size, u = META_RECORD.unpack(raw)
            if size < 2 or not math.isfinite(u) or u <= 0:
                raise ValueError("invalid correction coefficient")
            yield size, u


def publish_certified_rank_stream(store, output, source, epsilon, values):
    started = time.perf_counter()
    with tempfile.NamedTemporaryFile(prefix="rank-pending-", dir=Path(output).parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            for value in values:
                handle.write(base.DOUBLE.pack(value))
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    output_ms = 1000*(time.perf_counter()-started)
    try:
        cert_start = time.perf_counter()
        certificate = base.certify_published_rank_output(store, temporary, source, epsilon)
        cert_ms = 1000*(time.perf_counter()-cert_start)
        if not certificate["accepted"]:
            raise ValueError("published result failed original residual certificate")
        os.replace(temporary, output)
        return {"output_ms": output_ms, "certificate_ms": cert_ms, "certificate": certificate,
                "output_bytes": Path(output).stat().st_size,
                "output_sha256": base.hash_file_content_bytes(output)}
    finally:
        temporary.unlink(missing_ok=True)


def run_projected_rank_solver(store, metadata, output, source, epsilon,
                              maximum_checks=20000, collect_trace=False, relaxed=False):
    started = time.perf_counter()
    if maximum_checks < 1:
        raise ValueError("iteration budget exhausted before allocation")
    if not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid tolerance")
    n, f, e_scalar = read_bound_metadata_header(store, metadata)
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid personalization source")
    row_scans, metadata_scans = 0, 0

    def scan_prepared_entity_rows():
        nonlocal row_scans
        row_scans += 1
        return base.iter_incidence_row_records(store)

    def scan_prepared_factor_records():
        nonlocal metadata_scans
        metadata_scans += 1
        return iter_factor_correction_records(metadata, f)

    h = base.create_zero_float_vector(f)
    isolate_count, source_isolate = 0, False
    g_unscaled, ratio = 0.0, 0.0
    for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
        p = base.select_personalization_node_value(i, n, source)
        if degree:
            ratio = max(ratio, len(ids)/degree)
            g_unscaled += (degree+len(ids))*p/(degree+base.ALPHA*len(ids))
            for j in ids:
                h[j] += p/degree
        else:
            isolate_count += 1
            source_isolate |= i == source
    pz = isolate_count/n if source is None else float(source_isolate)
    bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
    beta = base.ALPHA*(1+ratio)/(1+base.ALPHA*ratio)
    omega = 2/(2-beta) if relaxed else 1.0
    g_scalar = bscale*g_unscaled
    if f:
        dot = math.fsum(u*h[j] for j, (_, u) in enumerate(scan_prepared_factor_records()))
        correction = (g_scalar-dot)/e_scalar
        for j, (size, _) in enumerate(scan_prepared_factor_records()):
            h[j] += size*correction
    preparation_ms = 1000*(time.perf_counter()-started)
    solve_start = time.perf_counter()
    trace, checks = [], 0
    if f:
        for checks in range(1, maximum_checks+1):
            following = base.create_zero_float_vector(f)
            mass = 0.0
            for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                b = bscale*base.select_personalization_node_value(i, n, source)
                if degree:
                    y = (b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids))
                    for j in ids:
                        following[j] += y
                    if collect_trace:
                        mass += degree*y
                elif collect_trace:
                    mass += b
            error, dot = 0.0, 0.0
            for j, (size, u) in enumerate(scan_prepared_factor_records()):
                error += size*abs(following[j]-h[j])
                dot += u*(omega*following[j]+(1-omega)*h[j])
            screen = base.ALPHA/(1-base.ALPHA)*error
            if collect_trace:
                trace.append({"candidate_mass": mass, "raw_residual_screen": screen})
            if screen <= epsilon/8:
                break
            correction = (g_scalar-dot)/e_scalar
            for j, (size, _) in enumerate(scan_prepared_factor_records()):
                following[j] = omega*following[j]+(1-omega)*h[j]+size*correction
            h = following
        else:
            raise ValueError("projected iteration budget exhausted")
    solve_ms = 1000*(time.perf_counter()-solve_start)

    def reconstruct_original_rank_values():
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            b = bscale*base.select_personalization_node_value(i, n, source)
            yield degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b

    published = publish_certified_rank_stream(store, output, source, epsilon, reconstruct_original_rank_values())
    return dict(published, method="projected_relaxed" if relaxed else "projected",
                source_zero_based=source, checks=checks, relaxation=omega,
                preparation_ms=preparation_ms, solve_ms=solve_ms,
                total_method_ms=1000*(time.perf_counter()-started),
                noncertificate_row_scans=row_scans, metadata_scans=metadata_scans,
                noncertificate_membership_visits=row_scans*base.read_incidence_store_header(store)[2],
                logical_read_bytes=(row_scans+1)*Path(store).stat().st_size
                +META_HEADER.size+metadata_scans*f*META_RECORD.size
                +published["certificate"]["logical_read_bytes"]+8*n,
                logical_write_bytes=8*n, float_iterate_payload_bytes=16*f,
                metadata_bytes=Path(metadata).stat().st_size, trace=trace)


def run_reduced_control_solver(store, output, source, epsilon, method, maximum_checks=20000):
    started = time.perf_counter()
    n, f, z = base.read_incidence_store_header(store)
    if method not in ("relaxed", "normalized") or maximum_checks < 1:
        raise ValueError("invalid reduced control or iteration budget")
    if not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid tolerance")
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid personalization source")
    row_scans = 0

    def scan_prepared_entity_rows():
        nonlocal row_scans
        row_scans += 1
        return base.iter_incidence_row_records(store)

    h, sizes = base.create_zero_float_vector(f), base.create_zero_float_vector(f)
    isolate_count, source_isolate, ratio = 0, False, 0.0
    for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
        p = base.select_personalization_node_value(i, n, source)
        if degree:
            ratio = max(ratio, len(ids)/degree)
            for j in ids:
                h[j] += p/degree
                sizes[j] += 1
        else:
            isolate_count += 1
            source_isolate |= i == source
    pz = isolate_count/n if source is None else float(source_isolate)
    bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
    target_mass = (1-pz)/(1-base.ALPHA*pz)
    beta = base.ALPHA*(1+ratio)/(1+base.ALPHA*ratio)
    omega = 2/(2-beta) if method == "relaxed" else 1.0
    preparation_ms = 1000*(time.perf_counter()-started)
    solve_start = time.perf_counter()
    scale, residual_scans, checks = 1.0, 0, 0
    for checks in range(1, maximum_checks+1):
        following, candidate_mass = base.create_zero_float_vector(f), 0.0
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            if degree:
                b = bscale*base.select_personalization_node_value(i, n, source)
                y = (b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids))
                for j in ids:
                    following[j] += y
                if method == "normalized":
                    candidate_mass += degree*y
        raw_screen = base.ALPHA/(1-base.ALPHA)*math.fsum(sizes[j]*abs(following[j]-h[j]) for j in range(f))
        if method == "relaxed":
            if raw_screen <= epsilon/8:
                break
        elif checks == 1 or checks % 8 == 0 or raw_screen <= epsilon/8:
            if target_mass == 0 and candidate_mass == 0:
                scale = 1.0
            elif candidate_mass > 0:
                scale = target_mass/candidate_mass
            else:
                raise ValueError("normalization requires positive active mass")
            residual_scans += 1
            residual_l1 = 0.0
            for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                if degree:
                    residual = base.ALPHA*math.fsum(following[j]-h[j] for j in ids)
                    b = bscale*base.select_personalization_node_value(i, n, source)
                    residual_l1 += abs(scale*residual-(scale-1)*b)
            if residual_l1/(1-base.ALPHA) <= epsilon/8:
                break
        for j in range(f):
            following[j] = omega*following[j]+(1-omega)*h[j]
        h = following
    else:
        raise ValueError("reduced control iteration budget exhausted")
    solve_ms = 1000*(time.perf_counter()-solve_start)

    def reconstruct_control_rank_values():
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            b = bscale*base.select_personalization_node_value(i, n, source)
            yield scale*degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b

    published = publish_certified_rank_stream(store, output, source, epsilon, reconstruct_control_rank_values())
    return dict(published, method=method, source_zero_based=source, checks=checks,
                relaxation=omega, output_active_scale=scale, normalized_residual_scans=residual_scans,
                preparation_ms=preparation_ms, solve_ms=solve_ms,
                total_method_ms=1000*(time.perf_counter()-started),
                noncertificate_row_scans=row_scans, noncertificate_membership_visits=row_scans*z,
                logical_read_bytes=row_scans*Path(store).stat().st_size
                +published["certificate"]["logical_read_bytes"]+8*n,
                logical_write_bytes=8*n, float_iterate_payload_bytes=16*f,
                resident_weight_payload_bytes=8*f)


def run_reused_dense_session(store, folder, sources, epsilon):
    started = time.perf_counter()
    n, f, z = base.read_incidence_store_header(store)
    if f > 1024:
        raise ValueError("dense factor cap exceeded before allocation")
    if not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid tolerance")
    if any(source is not None and (type(source) is not int or not 0 <= source < n) for source in sources):
        raise ValueError("invalid personalization source")
    pairs, setup_rows = 0, 0
    if f:
        import numpy as np
        core = np.eye(f, dtype=np.float64)
        setup_rows = 1
        for degree, ids in base.iter_incidence_row_records(store):
            if degree:
                ix = np.array(ids, dtype=np.int64)
                core[np.ix_(ix, ix)] -= base.ALPHA/(degree+base.ALPHA*len(ids))
                pairs += len(ids)**2
        cholesky = np.linalg.cholesky(core)
        del core
    setup_ms = 1000*(time.perf_counter()-started)
    queries = []
    for query_index, source in enumerate(sources):
        query_start = time.perf_counter()
        h = base.create_zero_float_vector(f)
        rhs = base.create_zero_float_vector(f)
        isolate_count, source_isolate = 0, False
        for i, (degree, ids) in enumerate(base.iter_incidence_row_records(store)):
            p = base.select_personalization_node_value(i, n, source)
            if degree:
                for j in ids:
                    rhs[j] += p/(degree+base.ALPHA*len(ids))
            else:
                isolate_count += 1
                source_isolate |= i == source
        pz = isolate_count/n if source is None else float(source_isolate)
        bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
        preparation_ms = 1000*(time.perf_counter()-query_start)
        solve_start = time.perf_counter()
        if f:
            intermediate = np.zeros(f, dtype=np.float64)
            solution = np.zeros(f, dtype=np.float64)
            for i in range(f):
                intermediate[i] = (bscale*rhs[i]-np.dot(cholesky[i,:i], intermediate[:i]))/cholesky[i,i]
            for i in range(f-1, -1, -1):
                solution[i] = (intermediate[i]-np.dot(cholesky[i+1:,i], solution[i+1:]))/cholesky[i,i]
            h = array("d", solution)
            del intermediate, solution
        solve_ms = 1000*(time.perf_counter()-solve_start)

        def reconstruct_dense_rank_values():
            for i, (degree, ids) in enumerate(base.iter_incidence_row_records(store)):
                b = bscale*base.select_personalization_node_value(i, n, source)
                yield degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b

        output = Path(folder)/f"dense-{query_index}.bin"
        published = publish_certified_rank_stream(store, output, source, epsilon, reconstruct_dense_rank_values())
        queries.append(dict(published, method="dense_reuse", source_zero_based=source,
                            preparation_ms=preparation_ms, solve_ms=solve_ms,
                            total_method_ms=1000*(time.perf_counter()-query_start),
                            noncertificate_row_scans=2, noncertificate_membership_visits=2*z,
                            logical_read_bytes=2*Path(store).stat().st_size
                            +published["certificate"]["logical_read_bytes"]+8*n,
                            logical_write_bytes=8*n))
    return {"method": "dense_reuse", "setup_ms": setup_ms, "setup_entity_scans": setup_rows,
            "setup_logical_read_bytes": setup_rows*Path(store).stat().st_size,
            "factorizations": int(f > 0), "dense_pair_updates": pairs,
            "dense_matrix_payload_bytes": 8*f*f, "queries": queries,
            "total_session_ms": 1000*(time.perf_counter()-started)}


def run_selected_method_session(store, folder, method):
    started = time.perf_counter()
    n, _, _ = base.read_incidence_store_header(store)
    sources = [None, 0, n-1]
    if method == "dense_reuse":
        result = run_reused_dense_session(store, folder, sources, 1e-10)
    else:
        setup = None
        if method in ("projected", "projected_relaxed"):
            metadata = Path(folder)/"correction.bin"
            setup = build_factor_correction_records(store, metadata)
        queries = []
        for j, source in enumerate(sources):
            output = Path(folder)/f"{method}-{j}.bin"
            if method in ("projected", "projected_relaxed"):
                query = run_projected_rank_solver(store, metadata, output, source, 1e-10,
                                                  relaxed=method == "projected_relaxed")
            elif method in ("relaxed", "normalized"):
                query = run_reduced_control_solver(store, output, source, 1e-10, method)
            else:
                query = base.run_incidence_rank_solver(store, output, method, source, 1e-10)
            queries.append(query)
        result = {"method": method, "setup": setup, "setup_ms": setup["setup_ms"] if setup else 0,
                  "queries": queries, "total_session_ms": 1000*(time.perf_counter()-started)}
    result["peak_rss_bytes"] = base.collect_process_peak_memory()
    return result


def main_mass_projected_experiment():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--worker", choices=MODES)
    parser.add_argument("--folder", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.worker:
        if args.folder is None:
            parser.error("worker requires external --folder")
        print(json.dumps(run_selected_method_session(args.store, args.folder, args.worker)))
        return
    if not args.source or not args.receipt or args.repeats < 1:
        parser.error("driver requires --source, --receipt, positive --repeats")
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    build_start = time.perf_counter()
    built = subprocess.run([sys.executable, str(Path(base.__file__).resolve()), "--source", str(args.source),
                            "--store", str(args.store), "--build"], env=env, check=True, capture_output=True, text=True)
    build = json.loads(built.stdout)
    build["child_wall_ms"] = 1000*(time.perf_counter()-build_start)
    results = []
    for repeat in range(args.repeats):
        order = MODES[repeat % len(MODES):]+MODES[:repeat % len(MODES)]
        for method in order:
            with tempfile.TemporaryDirectory(prefix="knight-bus-mass-session-") as folder:
                command = [sys.executable, str(Path(__file__).resolve()), "--store", str(args.store),
                           "--worker", method, "--folder", folder]
                started = time.perf_counter()
                child = subprocess.run(command, env=env, check=True, capture_output=True, text=True)
                result = json.loads(child.stdout)
                result.update(repeat=repeat, child_wall_ms=1000*(time.perf_counter()-started))
                results.append(result)
                print(json.dumps({"done":len(results), "method":method,
                                  "total_session_ms":round(result["total_session_ms"],3)}), flush=True)
    receipt = {"build":build, "results":results, "probe_sha256":base.hash_file_content_bytes(__file__),
               "base_probe_sha256":base.hash_file_content_bytes(base.__file__),
               "environment":{"platform":platform.platform(),"python":sys.version,"thread_cap":1},
               "source_url":"https://files.grouplens.org/datasets/movielens/ml-100k/u.data",
               "license_url":"https://files.grouplens.org/datasets/movielens/ml-100k-README.txt",
               "attribution":"GroupLens Research, University of Minnesota; Harper and Konstan2015, DOI10.1145/2827872",
               "scope":"three-query sessions, all setup/output/certification charged; cached Python; no Neo4j or physical4GB claim"}
    args.receipt.write_text(json.dumps(receipt,indent=2)+"\n",encoding="ascii")


if __name__ == "__main__":
    main_mass_projected_experiment()

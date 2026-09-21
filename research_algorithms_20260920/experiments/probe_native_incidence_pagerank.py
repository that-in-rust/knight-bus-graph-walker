"""Research-only streamed PageRank comparison on a binary incidence source.

No source download, raw data, or answer vector is written inside the repository.
The small resident builder and row-sized decoder are NOT a bounded-RAM importer.
"""

import argparse
from array import array
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import resource
import struct
import subprocess
import sys
import tempfile
import time

HEADER = struct.Struct("<8sQQQ")
ROW = struct.Struct("<QI")
DOUBLE = struct.Struct("<d")
MAGIC = b"KBINC001"
ALPHA = 17 / 20
METHODS = ("power", "fused", "filepower", "reduced", "jacobi", "cg", "vertexcg", "dense")


def hash_file_content_bytes(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read_incidence_store_header(path):
    with Path(path).open("rb") as handle:
        raw = handle.read(HEADER.size)
    if len(raw) != HEADER.size:
        raise ValueError("truncated incidence header")
    magic, n, f, z = HEADER.unpack(raw)
    if magic != MAGIC or n == 0 or f >= 2**32:
        raise ValueError("invalid incidence header")
    return n, f, z


def iter_incidence_row_records(path):
    n, f, z = read_incidence_store_header(path)
    memberships = 0
    with Path(path).open("rb") as handle:
        handle.seek(HEADER.size)
        for _ in range(n):
            raw = handle.read(ROW.size)
            if len(raw) != ROW.size:
                raise ValueError("truncated incidence row")
            degree, count = ROW.unpack(raw)
            if count > f:
                raise ValueError("invalid incidence row size")
            raw = handle.read(4 * count)
            if len(raw) != 4 * count:
                raise ValueError("truncated incidence memberships")
            ids = struct.unpack(f"<{count}I", raw)
            if ids and (ids[-1] >= f or any(x >= y for x, y in zip(ids, ids[1:]))):
                raise ValueError("invalid incidence factor ordering")
            if bool(degree) != bool(count):
                raise ValueError("invalid pruned incidence degree")
            memberships += count
            yield degree, ids
        if handle.read(1) or memberships != z:
            raise ValueError("invalid incidence trailing data or count")


def build_incidence_row_store(path, n, groups):
    started = time.perf_counter()
    if type(n) is not int or n <= 0:
        raise ValueError("positive vertex universe required")
    rows = [[] for _ in range(n)]
    degrees = [0] * n
    sizes = []
    pruned = 0
    for group in groups:
        if any(type(i) is not int or i < 0 or i >= n for i in group):
            raise ValueError("invalid incidence vertex")
        if len(group) != len(set(group)):
            raise ValueError("duplicate incidence membership")
        if len(group) < 2:
            pruned += 1
            continue
        factor = len(sizes)
        sizes.append(len(group))
        for i in group:
            rows[i].append(factor)
            degrees[i] += len(group)-1
    z = sum(sizes)
    with Path(path).open("wb") as handle:
        handle.write(HEADER.pack(MAGIC, n, len(sizes), z))
        for degree, ids in zip(degrees, rows):
            handle.write(ROW.pack(degree, len(ids)))
            handle.write(struct.pack(f"<{len(ids)}I", *ids))
    ratios = [len(ids)/degree for degree, ids in zip(degrees, rows) if degree]
    maximum = max(ratios, default=0.0)
    return {
        "vertices": n, "factors": len(sizes), "memberships": z,
        "minimum_group_size": min(sizes, default=0), "maximum_group_size": max(sizes, default=0),
        "pruned_groups": pruned, "isolates": sum(d == 0 for d in degrees),
        "maximum_row_memberships": max(map(len, rows), default=0),
        "expanded_directed_edge_mass": sum(degrees),
        "maximum_q_over_d": maximum,
        "beta_upper": ALPHA*(1+maximum)/(1+ALPHA*maximum),
        "store_bytes": Path(path).stat().st_size,
        "store_sha256": hash_file_content_bytes(path),
        "row_build_ms": 1000*(time.perf_counter()-started),
        "builder_scope": "resident groups, entity row lists and integer degrees; not bounded-memory import",
    }


def build_movielens_source_store(source, path):
    started = time.perf_counter()
    groups = {}
    n = 0
    records = 0
    with Path(source).open(encoding="ascii") as handle:
        for line in handle:
            fields = line.split()
            if len(fields) != 4:
                raise ValueError("expected four MovieLens fields")
            user, movie, rating, timestamp = map(int, fields)
            if user < 1 or movie < 1 or not 1 <= rating <= 5 or timestamp < 0:
                raise ValueError("invalid MovieLens source row")
            groups.setdefault(user, []).append(movie-1)
            n = max(n, movie)
            records += 1
    meta = build_incidence_row_store(path, n, [groups[key] for key in sorted(groups)])
    meta.update(source_records=records, source_groups=len(groups),
                source_bytes=Path(source).stat().st_size,
                source_sha256=hash_file_content_bytes(source),
                source_build_ms=1000*(time.perf_counter()-started))
    return meta


def create_zero_float_vector(count):
    return array("d", [0.0]) * count


def select_personalization_node_value(i, n, source):
    return 1/n if source is None else float(i == source)


def certify_published_rank_output(path, output, source, epsilon, precision=50):
    """Enclose the ORIGINAL operator's residual of the published IEEE doubles."""
    n, f, z = read_incidence_store_header(path)
    if Path(output).stat().st_size != 8*n:
        raise ValueError("incorrect complete output length")
    if source is not None and not 0 <= source < n:
        raise ValueError("invalid personalization source")
    if precision < 20 or not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid certificate parameters")
    lower = Context(prec=precision, rounding=ROUND_FLOOR)
    upper = Context(prec=precision, rounding=ROUND_CEILING)
    zero, alpha, one_minus = Decimal(0), Decimal("0.85"), Decimal("0.15")
    lo, hi = [zero]*f, [zero]*f
    dangling_lo = dangling_hi = zero
    with Path(output).open("rb") as values:
        for degree, ids in iter_incidence_row_records(path):
            value = DOUBLE.unpack(values.read(8))[0]
            if not math.isfinite(value):
                raise ValueError("nonfinite published rank")
            x = Decimal.from_float(value)
            if degree:
                yl, yh = lower.divide(x, Decimal(degree)), upper.divide(x, Decimal(degree))
                for factor in ids:
                    lo[factor] = lower.add(lo[factor], yl)
                    hi[factor] = upper.add(hi[factor], yh)
            else:
                dangling_lo = lower.add(dangling_lo, x)
                dangling_hi = upper.add(dangling_hi, x)
    residual_upper = zero
    with Path(output).open("rb") as values:
        for i, (degree, ids) in enumerate(iter_incidence_row_records(path)):
            x = Decimal.from_float(DOUBLE.unpack(values.read(8))[0])
            sl = sh = zero
            for factor in ids:
                sl, sh = lower.add(sl, lo[factor]), upper.add(sh, hi[factor])
            if degree:
                q = Decimal(len(ids))
                yl, yh = lower.divide(x, Decimal(degree)), upper.divide(x, Decimal(degree))
                sl, sh = (lower.subtract(sl, upper.multiply(q, yh)),
                          upper.subtract(sh, lower.multiply(q, yl)))
            if source is None:
                sl = lower.add(sl, lower.divide(dangling_lo, Decimal(n)))
                sh = upper.add(sh, upper.divide(dangling_hi, Decimal(n)))
                bl, bh = lower.divide(one_minus, Decimal(n)), upper.divide(one_minus, Decimal(n))
            elif i == source:
                sl, sh = lower.add(sl, dangling_lo), upper.add(sh, dangling_hi)
                bl = bh = one_minus
            else:
                bl = bh = zero
            rl = lower.subtract(lower.add(bl, lower.multiply(alpha, sl)), x)
            rh = upper.subtract(upper.add(bh, upper.multiply(alpha, sh)), x)
            residual_upper = upper.add(residual_upper, max(rl.copy_abs(), rh.copy_abs()))
    error_upper = upper.divide(residual_upper, one_minus)
    return {"l1_error_upper": str(error_upper), "residual_l1_upper": str(residual_upper),
            "accepted": error_upper <= Decimal(str(epsilon)), "decimal_precision": precision,
            "row_scans": 2, "membership_visits": 2*z,
            "logical_read_bytes": 2*Path(path).stat().st_size+16*n,
            "retained_decimal_values": 2*f,
            "scope": "original loop-free weighted clique projection; actual published f64 scores"}


def run_incidence_rank_solver(path, output, method, source, epsilon, maximum_iterations=20000):
    total_start = time.perf_counter()
    n, f, z = read_incidence_store_header(path)
    if method not in METHODS or not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid solver parameters")
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid personalization source")
    if method == "dense" and f > 1024:
        raise ValueError("dense factor cap exceeded before matrix allocation")
    scans = 0

    def scan_prepared_entity_rows():
        nonlocal scans
        scans += 1
        return iter_incidence_row_records(path)

    h = create_zero_float_vector(f)
    counts = create_zero_float_vector(f) if method in ("reduced", "jacobi", "cg") else None
    diagonal = create_zero_float_vector(f) if method == "jacobi" else None
    rhs = create_zero_float_vector(f) if method == "cg" else None
    isolate_count, source_isolate, degree_sum = 0, False, 0
    for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
        degree_sum += degree
        if not degree:
            isolate_count += 1
            source_isolate |= i == source
        else:
            p = select_personalization_node_value(i, n, source)
            inverse_k = 1/(degree+ALPHA*len(ids))
            for factor in ids:
                if method != "dense":
                    h[factor] += p/degree
                if counts is not None:
                    counts[factor] += 1
                if diagonal is not None:
                    diagonal[factor] += ALPHA*inverse_k
                if rhs is not None:
                    rhs[factor] += p*inverse_k
    pz = isolate_count/n if source is None else float(source_isolate)
    bscale = (1-ALPHA)/(1-ALPHA*pz)
    if rhs is not None:
        for j in range(f):
            rhs[j] *= bscale
    preparation_ms = 1000*(time.perf_counter()-total_start)
    solve_start = time.perf_counter()
    iterations, dense_pair_updates = 0, 0
    x = None
    vertex = None
    scratch = None
    iterative_score_read_bytes = 0
    logical_write_bytes = 8*n
    screen = math.inf
    screen_target = epsilon/8

    def apply_reduced_symmetric_operator(vector):
        answer = array("d", vector)
        for degree, ids in scan_prepared_entity_rows():
            if degree:
                value = ALPHA*math.fsum(vector[j] for j in ids)/(degree+ALPHA*len(ids))
                for j in ids:
                    answer[j] -= value
        return answer

    def apply_vertex_symmetric_operator(vector):
        gather = create_zero_float_vector(f)
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            if degree:
                value = vector[i]/math.sqrt(degree)
                for j in ids:
                    gather[j] += value
        answer = create_zero_float_vector(n)
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            if degree:
                answer[i] = ((1+ALPHA*len(ids)/degree)*vector[i]
                             -ALPHA*math.fsum(gather[j] for j in ids)/math.sqrt(degree))
        return answer

    if method in ("power", "fused"):
        x = array("d", (select_personalization_node_value(i, n, source) for i in range(n)))
        for iterations in range(1, maximum_iterations+1):
            next_h = create_zero_float_vector(f) if method == "fused" else None
            change = 0.0
            if method == "power":
                h = create_zero_float_vector(f)
                for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                    if degree:
                        for j in ids:
                            h[j] += x[i]/degree
            for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                b = bscale*select_personalization_node_value(i, n, source)
                value = b+ALPHA*(math.fsum(h[j] for j in ids)-len(ids)*x[i]/degree) if degree else b
                change += abs(value-x[i])
                x[i] = value
                if method == "fused" and degree:
                    for j in ids:
                        next_h[j] += value/degree
            if method == "fused":
                h = next_h
            screen = ALPHA*change/(1-ALPHA)
            if screen <= screen_target:
                break
        else:
            raise ValueError("original power iteration budget exhausted")
    elif method == "filepower":
        scratch = tempfile.TemporaryDirectory(prefix="incidence-rank-", dir=Path(output).parent)
        current, following = Path(scratch.name)/"old", Path(scratch.name)/"new"
        with current.open("wb") as handle:
            for i in range(n):
                handle.write(DOUBLE.pack(select_personalization_node_value(i, n, source)))
        for iterations in range(1, maximum_iterations+1):
            next_h, change = create_zero_float_vector(f), 0.0
            with current.open("rb") as old, following.open("wb") as new:
                for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                    value_old = DOUBLE.unpack(old.read(8))[0]
                    b = bscale*select_personalization_node_value(i, n, source)
                    value = b+ALPHA*(math.fsum(h[j] for j in ids)-len(ids)*value_old/degree) if degree else b
                    new.write(DOUBLE.pack(value))
                    change += abs(value-value_old)
                    if degree:
                        for j in ids:
                            next_h[j] += value/degree
            h = next_h
            current, following = following, current
            screen = ALPHA*change/(1-ALPHA)
            if screen <= screen_target:
                break
        else:
            raise ValueError("file-backed iteration budget exhausted")
        iterative_score_read_bytes = iterations*8*n
        logical_write_bytes = (iterations+1)*8*n
    elif method in ("reduced", "jacobi"):
        for iterations in range(1, maximum_iterations+1):
            next_h = create_zero_float_vector(f)
            for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                if degree:
                    value = (bscale*select_personalization_node_value(i, n, source)
                             +ALPHA*math.fsum(h[j] for j in ids))/(degree+ALPHA*len(ids))
                    for j in ids:
                        next_h[j] += value
            screen = ALPHA/(1-ALPHA)*math.fsum(counts[j]*abs(next_h[j]-h[j]) for j in range(f))
            if screen <= screen_target:
                break
            if method == "jacobi":
                for j in range(f):
                    next_h[j] = (next_h[j]-diagonal[j]*h[j])/(1-diagonal[j])
            h = next_h
        else:
            raise ValueError("reduced iteration budget exhausted")
    elif method == "cg":
        product = apply_reduced_symmetric_operator(h)
        residual = array("d", (rhs[j]-product[j] for j in range(f)))
        del product
        direction = array("d", residual)
        squared = math.fsum(value*value for value in residual)
        for iterations in range(maximum_iterations+1):
            screen = ALPHA/(1-ALPHA)*math.fsum(counts[j]*abs(residual[j]) for j in range(f))
            if screen <= screen_target:
                break
            product = apply_reduced_symmetric_operator(direction)
            denominator = math.fsum(direction[j]*product[j] for j in range(f))
            if denominator <= 0 or not math.isfinite(denominator):
                raise ValueError("CG lost positive curvature")
            step = squared/denominator
            for j in range(f):
                h[j] += step*direction[j]
                residual[j] -= step*product[j]
            next_squared = math.fsum(value*value for value in residual)
            for j in range(f):
                direction[j] = residual[j]+next_squared/squared*direction[j]
            squared = next_squared
            del product
        else:
            raise ValueError("CG iteration budget exhausted")
    elif method == "vertexcg":
        h = None
        vertex, vertex_rhs = create_zero_float_vector(n), create_zero_float_vector(n)
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            if degree:
                vertex[i] = select_personalization_node_value(i, n, source)/math.sqrt(degree)
                vertex_rhs[i] = bscale*vertex[i]
        product = apply_vertex_symmetric_operator(vertex)
        residual = array("d", (vertex_rhs[i]-product[i] for i in range(n)))
        del product
        direction = array("d", residual)
        squared = math.fsum(value*value for value in residual)
        for iterations in range(maximum_iterations+1):
            screen = math.sqrt(degree_sum*squared)/(1-ALPHA)
            if screen <= screen_target:
                break
            product = apply_vertex_symmetric_operator(direction)
            denominator = math.fsum(direction[i]*product[i] for i in range(n))
            if denominator <= 0 or not math.isfinite(denominator):
                raise ValueError("vertex CG lost positive curvature")
            step = squared/denominator
            for i in range(n):
                vertex[i] += step*direction[i]
                residual[i] -= step*product[i]
            next_squared = math.fsum(value*value for value in residual)
            for i in range(n):
                direction[i] = residual[i]+next_squared/squared*direction[i]
            squared = next_squared
            del product
        else:
            raise ValueError("vertex CG iteration budget exhausted")
    elif f:
        import numpy as np
        core = np.eye(f, dtype=np.float64)
        dense_rhs = np.zeros(f, dtype=np.float64)
        for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
            if degree:
                inverse_k = 1/(degree+ALPHA*len(ids))
                ix = np.array(ids, dtype=np.int64)
                core[np.ix_(ix, ix)] -= ALPHA*inverse_k
                dense_rhs[ix] += bscale*select_personalization_node_value(i, n, source)*inverse_k
                dense_pair_updates += len(ids)**2
        cholesky = np.linalg.cholesky(core)
        del core
        solution = np.zeros(f, dtype=np.float64)
        intermediate = np.zeros(f, dtype=np.float64)
        for i in range(f):
            intermediate[i] = (dense_rhs[i]-np.dot(cholesky[i, :i], intermediate[:i]))/cholesky[i, i]
        for i in range(f-1, -1, -1):
            solution[i] = (intermediate[i]-np.dot(cholesky[i+1:, i], solution[i+1:]))/cholesky[i, i]
        h = array("d", solution)
        del cholesky, dense_rhs, intermediate, solution
        screen = None
    else:
        screen = 0.0
    solve_ms = 1000*(time.perf_counter()-solve_start)
    output_start = time.perf_counter()
    if method == "filepower":
        os.replace(current, output)
        scratch.cleanup()
    else:
        with Path(output).open("wb") as handle:
            if x is not None:
                for value in x:
                    handle.write(DOUBLE.pack(value))
            else:
                for i, (degree, ids) in enumerate(scan_prepared_entity_rows()):
                    b = bscale*select_personalization_node_value(i, n, source)
                    if vertex is not None:
                        value = math.sqrt(degree)*vertex[i] if degree else b
                    else:
                        value = degree*(b+ALPHA*math.fsum(h[j] for j in ids))/(degree+ALPHA*len(ids)) if degree else b
                    handle.write(DOUBLE.pack(value))
    output_ms = 1000*(time.perf_counter()-output_start)
    certificate_start = time.perf_counter()
    certificate = certify_published_rank_output(path, output, source, epsilon)
    certificate_ms = 1000*(time.perf_counter()-certificate_start)
    if not certificate["accepted"]:
        raise ValueError(f"published result failed original residual certificate: {certificate}")
    return {
        "method": method, "source_zero_based": source, "vertices": n, "factors": f,
        "epsilon": epsilon, "iterations_or_checks": iterations, "screen_bound_uncertified": screen,
        "preparation_ms": preparation_ms, "solve_ms": solve_ms, "output_ms": output_ms,
        "certificate_ms": certificate_ms, "total_method_ms": 1000*(time.perf_counter()-total_start),
        "noncertificate_row_scans": scans, "noncertificate_membership_visits": scans*z,
        "logical_read_bytes": scans*Path(path).stat().st_size+certificate["logical_read_bytes"]+iterative_score_read_bytes,
        "iterative_score_read_bytes": iterative_score_read_bytes, "logical_write_bytes": logical_write_bytes,
        "output_bytes": 8*n, "output_sha256": hash_file_content_bytes(output),
        "dense_pair_updates": dense_pair_updates, "dense_matrix_payload_bytes": 8*f*f if method == "dense" else 0,
        "certificate": certificate,
    }


def collect_process_peak_memory():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value*1024


def main_native_incidence_experiment():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--method", choices=METHODS)
    parser.add_argument("--node", type=int)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.build:
        result = build_movielens_source_store(args.source, args.store)
        result["peak_rss_bytes"] = collect_process_peak_memory()
        print(json.dumps(result))
        return
    if args.method:
        result = run_incidence_rank_solver(args.store, args.output, args.method, args.node, 1e-10)
        result["peak_rss_bytes"] = collect_process_peak_memory()
        print(json.dumps(result))
        return
    if not args.source or not args.receipt or args.repeats < 1:
        parser.error("driver requires --source, --receipt and positive --repeats")
    environment = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    base = [sys.executable, str(Path(__file__).resolve()), "--store", str(args.store)]
    build_start = time.perf_counter()
    process = subprocess.run(base+["--build", "--source", str(args.source)], env=environment,
                             check=True, capture_output=True, text=True)
    build = json.loads(process.stdout)
    build["child_wall_ms"] = 1000*(time.perf_counter()-build_start)
    results = []
    nodes = [None, 0, build["vertices"]-1]
    for node in nodes:
        for repeat in range(args.repeats):
            # Rotate method order without changing source, query or solver parameters.
            order = METHODS[repeat % len(METHODS):]+METHODS[:repeat % len(METHODS)]
            for method in order:
                output = args.store.with_name(f"rank-{method}-{node}-{repeat}.bin")
                command = base+["--method", method, "--output", str(output)]
                if node is not None:
                    command.extend(["--node", str(node)])
                started = time.perf_counter()
                child = subprocess.run(command, env=environment, check=True, capture_output=True, text=True)
                result = json.loads(child.stdout)
                result.update(repeat=repeat, child_wall_ms=1000*(time.perf_counter()-started))
                results.append(result)
                print(json.dumps({"done": len(results), "method": method, "node": node,
                                  "total_ms": round(result["total_method_ms"], 3)}), flush=True)
    receipt = {
        "source_url": "https://files.grouplens.org/datasets/movielens/ml-100k/u.data",
        "license_url": "https://files.grouplens.org/datasets/movielens/ml-100k-README.txt",
        "attribution": "GroupLens Research, University of Minnesota; Harper and Konstan 2015, DOI 10.1145/2827872",
        "normalization": "movies are vertices; users are binary factors; ignore rating and timestamp; weight=shared users; no self loops",
        "semantics": "stationary normalized PageRank, damping17/20, uniform or specified single-node p; dangling to p",
        "environment": {"platform": platform.platform(), "python": sys.version, "thread_cap": 1},
        "probe_sha256": hash_file_content_bytes(__file__), "build": build, "results": results,
        "scope": "small cached Python research experiment, not Neo4j/GDS timing, 4GB fit, or cold disk measurement",
    }
    args.receipt.write_text(json.dumps(receipt, indent=2)+"\n", encoding="ascii")


if __name__ == "__main__":
    main_native_incidence_experiment()

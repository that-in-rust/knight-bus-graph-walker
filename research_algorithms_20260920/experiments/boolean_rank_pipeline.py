"""Private candidate -> actual-byte certificate -> no-replace publication."""

from contextlib import closing
import hashlib
import math
import os
from pathlib import Path
import struct
import tempfile
import time

from stream_boolean_rank_solver import solve_boolean_rank_state, iterate_boolean_rank_output


def publish_boolean_rank_result(source, destination, *, alpha, epsilon, method,
                                max_factor_slots, max_class_slots, tolerance=1e-13,
                                maximum_iterations=1000, precision=60):
    """Source lifetime/pinning is caller-owned; publication is not crash-atomic."""
    started = time.perf_counter()
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    if type(epsilon) is not float or not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("positive binary64 certificate tolerance required")
    if type(precision) is not int or precision < 16:
        raise ValueError("certificate precision must be at least 16")
    from boolean_rank_output_certificate import certify_boolean_rank_output

    before = time.perf_counter()
    state = solve_boolean_rank_state(source, alpha=alpha, method=method,
        max_factor_slots=max_factor_slots, max_class_slots=max_class_slots,
        tolerance=tolerance, maximum_iterations=maximum_iterations)
    solve_seconds = time.perf_counter() - before
    digest, rows = hashlib.sha256(), 0
    with tempfile.TemporaryDirectory(prefix=f".{destination.name}.candidate-", dir=destination.parent) as scratch:
        candidate = Path(scratch) / "candidate.ranks"
        before = time.perf_counter()
        with candidate.open("xb", buffering=65536) as stream, closing(iterate_boolean_rank_output(source, state)) as values:
            for vertex, value in values:
                record = struct.pack("<Qd", vertex, value)
                stream.write(record)
                digest.update(record)
                rows += 1
            stream.flush()
            os.fsync(stream.fileno())
        output_seconds = time.perf_counter() - before
        solver_metrics = state["metrics"]
        del state
        before = time.perf_counter()
        certificate = certify_boolean_rank_output(source, candidate, alpha=alpha, epsilon=epsilon,
            precision=precision, scratch_dir=scratch, max_factor_slots=max_factor_slots)
        certificate_seconds = time.perf_counter() - before
        if certificate["output_sha256"] != digest.hexdigest() or certificate["nrows"] != rows:
            raise ValueError("certificate does not describe the staged output")
        if not certificate["accepted"]:
            raise ValueError(f"output certificate refused: L1 bound {certificate['l1_error_upper']}")
        before = time.perf_counter()
        os.link(candidate, destination)
        publish_seconds = time.perf_counter() - before
    return dict(method=method, alpha=alpha, epsilon=epsilon, snapshot_id=source.snapshot_id,
                solve_seconds=solve_seconds, output_seconds=output_seconds,
                certificate_seconds=certificate_seconds, publish_seconds=publish_seconds,
                elapsed_seconds=time.perf_counter() - started, solver=solver_metrics,
                certificate=certificate, output_rows=rows, output_bytes=16 * rows,
                output_sha256=digest.hexdigest(),
                scope="trusted pinned source; complete class-ordered output; no physical-memory cap")

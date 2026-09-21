"""Private actual-byte publication for native Boolean pair-source PageRank."""

from contextlib import closing
import hashlib
import math
import os
from pathlib import Path
import struct
import tempfile
import time

from boolean_rank_native_certificate import certify_native_boolean_output
from boolean_rank_native_gap_source import validate_native_boolean_source
from boolean_rank_output_certificate import certify_boolean_rank_output
from stream_boolean_rank_solver import solve_boolean_rank_state, iterate_boolean_rank_output


class NativeRankCertificateRefusal(ValueError):
    def __init__(self, receipt):
        super().__init__("actual-byte error certificate exceeds requested epsilon")
        self.receipt = receipt


def publish_native_rank_result(source, destination, *, alpha, epsilon, method,
                               certificate_method, max_factor_slots, max_class_slots,
                               tolerance=1e-13, maximum_iterations=1000, precision=60):
    """Publish only admitted, complete original-ID output; source stays caller-owned.

    Generic controls do not pay native topology preflight. Native-gap pays both
    preflight and the standalone verifier's own validation, without a trusted
    cached gap shortcut. Limits are logical slots, not physical RAM caps.
    File fsync plus no-replace hard link does not promise directory durability.
    """
    started = time.perf_counter()
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    if method not in ("factor-cg", "class-cg"):
        raise ValueError("unknown solver method")
    if certificate_method not in ("generic", "native-gap"):
        raise ValueError("unknown certificate method")
    if (type(alpha) is not float or not math.isfinite(alpha) or not 0 <= alpha < 1
            or type(epsilon) is not float or not math.isfinite(epsilon) or epsilon <= 0):
        raise ValueError("finite binary64 alpha in [0,1) and positive epsilon required")
    if any(type(value) is not int or value < 0 for value in (max_factor_slots, max_class_slots)):
        raise ValueError("nonnegative integer slot limits required")
    if type(precision) is not int or precision < 16:
        raise ValueError("certificate precision must be at least 16")
    snapshot, vertices = source.snapshot_id, source.vertex_count
    topology = (validate_native_boolean_source(source, max_factor_slots=max_factor_slots)
                if certificate_method == "native-gap" else None)
    preflight_seconds = time.perf_counter() - started
    before = time.perf_counter()
    state = solve_boolean_rank_state(source, alpha=alpha, method=method,
        max_factor_slots=max_factor_slots, max_class_slots=max_class_slots,
        tolerance=tolerance, maximum_iterations=maximum_iterations)
    solve_seconds = time.perf_counter() - before
    digest, rows = hashlib.sha256(), 0
    with tempfile.TemporaryDirectory(prefix=f".{destination.name}.candidate-", dir=destination.parent) as private:
        candidate = Path(private) / "candidate.ranks"
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
        verifier = (certify_native_boolean_output if certificate_method == "native-gap"
                    else certify_boolean_rank_output)
        certificate = verifier(source, candidate, alpha=alpha, epsilon=epsilon,
            precision=precision, scratch_dir=private, max_factor_slots=max_factor_slots)
        certificate_seconds = time.perf_counter() - before
        if (certificate["output_sha256"] != digest.hexdigest() or certificate["nrows"] != rows
                or rows != vertices or certificate["snapshot_id"] != snapshot
                or source.snapshot_id != snapshot):
            raise ValueError("certificate does not describe staged output and source")
        receipt = dict(solver_method=method, certificate_method=certificate_method,
            alpha=alpha, epsilon=epsilon, snapshot_id=snapshot, accepted=certificate["accepted"],
            preflight_seconds=preflight_seconds, solve_seconds=solve_seconds,
            output_seconds=output_seconds, certificate_seconds=certificate_seconds,
            publish_seconds=0.0, solver=solver_metrics, certificate=certificate,
            preflight_class_rows=topology["validation_class_rows"] if topology else 0,
            output_rows=rows, output_bytes=16*rows, output_sha256=digest.hexdigest(),
            scope="trusted pinned Boolean source; full actual-byte output; no physical cap")
        if not certificate["accepted"]:
            receipt["elapsed_seconds"] = time.perf_counter() - started
            raise NativeRankCertificateRefusal(receipt)
        before = time.perf_counter()
        os.link(candidate, destination)
        receipt["publish_seconds"] = time.perf_counter() - before
    receipt["elapsed_seconds"] = time.perf_counter() - started
    return receipt

"""Explicit uniform-source publication study; frozen generic default unchanged."""

from contextlib import closing
import hashlib
import math
import os
from pathlib import Path
import struct
import tempfile
import time

from boolean_rank_deflated_certificate import certify_boolean_deflated_output
from boolean_rank_output_certificate import certify_boolean_rank_output
from boolean_rank_spectral_source import validate_boolean_spectral_source
from boolean_rank_uniform_pair_control import (
    solve_uniform_pair_rank_state, certify_uniform_pair_exact_output,
)
from stream_boolean_rank_solver import solve_boolean_rank_state, iterate_boolean_rank_output


class UniformRankCertificateRefusal(ValueError):
    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__(f"output certificate refused: L1 bound {receipt['certificate']['l1_error_upper']}")


def publish_uniform_rank_result(source, destination, *, alpha, epsilon, solver_method,
                                certificate_method, max_factor_slots, max_class_slots,
                                tolerance=1e-13, maximum_iterations=1000, precision=60):
    """Privately stage and certify all original-ID scores before no-replace link.

    Source ownership stays with the caller. Uniform-family preflight is paid
    even for CG/generic; solver and certificate also pay their own validation.
    Slot limits are not physical RAM caps. File fsync does not provide directory
    crash durability. This separate research path does not change old defaults.
    """
    started = time.perf_counter()
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    if solver_method not in ("factor-cg", "class-cg", "uniform-pair-direct"):
        raise ValueError("unknown solver method")
    if certificate_method not in ("generic", "deflated", "exact-target"):
        raise ValueError("unknown certificate method")
    if (type(alpha) is not float or not math.isfinite(alpha) or not 0 <= alpha < 1
            or type(epsilon) is not float or not math.isfinite(epsilon) or epsilon <= 0):
        raise ValueError("finite binary64 alpha in [0,1) and positive epsilon required")
    if any(type(value) is not int or value < 0 for value in (max_factor_slots, max_class_slots)):
        raise ValueError("nonnegative integer slot limits required")
    if type(precision) is not int or precision < 16:
        raise ValueError("certificate precision must be at least 16")
    topology = validate_boolean_spectral_source(source, max_factor_slots=max_factor_slots)
    snapshot = topology["snapshot_id"]
    preflight_seconds = time.perf_counter() - started
    before = time.perf_counter()
    if solver_method == "uniform-pair-direct":
        state = solve_uniform_pair_rank_state(source, alpha=alpha, max_factor_slots=max_factor_slots)
    else:
        state = solve_boolean_rank_state(source, alpha=alpha, method=solver_method,
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
        if certificate_method == "exact-target":
            certificate = certify_uniform_pair_exact_output(source, candidate, alpha=alpha,
                epsilon=epsilon, max_factor_slots=max_factor_slots)
        else:
            verifier = (certify_boolean_deflated_output if certificate_method == "deflated"
                        else certify_boolean_rank_output)
            certificate = verifier(source, candidate, alpha=alpha, epsilon=epsilon,
                precision=precision, scratch_dir=private, max_factor_slots=max_factor_slots)
        certificate_seconds = time.perf_counter() - before
        if (certificate["output_sha256"] != digest.hexdigest() or certificate["nrows"] != rows
                or rows != topology["vertices"] or certificate["snapshot_id"] != snapshot
                or source.snapshot_id != snapshot):
            raise ValueError("certificate does not describe the staged output and source")
        receipt = dict(solver_method=solver_method, certificate_method=certificate_method,
            alpha=alpha, epsilon=epsilon, snapshot_id=snapshot, accepted=certificate["accepted"],
            preflight_seconds=preflight_seconds, solve_seconds=solve_seconds,
            output_seconds=output_seconds, certificate_seconds=certificate_seconds,
            publish_seconds=0.0, solver=solver_metrics, certificate=certificate,
            output_rows=rows, output_bytes=16 * rows, output_sha256=digest.hexdigest(),
            scope="trusted pinned uniform-pair source; full actual-byte output; no physical cap")
        if not certificate["accepted"]:
            receipt["elapsed_seconds"] = time.perf_counter() - started
            raise UniformRankCertificateRefusal(receipt)
        before = time.perf_counter()
        os.link(candidate, destination)
        receipt["publish_seconds"] = time.perf_counter() - before
    receipt["elapsed_seconds"] = time.perf_counter() - started
    return receipt

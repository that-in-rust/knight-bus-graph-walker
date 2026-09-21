"""Stronger stationary/clique controls with one-pass actual-byte certification.

Known reversible/clique identities, not new PageRank mathematics. Prepared
source is trusted and pinned; private output must remain exclusively owned.
"""

from contextlib import closing
from decimal import (Context, Decimal, DivisionByZero, InvalidOperation, MAX_EMAX,
                     MIN_EMIN, Overflow, ROUND_CEILING, ROUND_FLOOR)
from fractions import Fraction
import hashlib
import math
import os
from pathlib import Path
import struct
import tempfile
import time

from boolean_rank_native_gap_source import validate_native_boolean_source
from boolean_rank_output_certificate import (
    _enclose_exact_fraction_value, _iterate_checked_class_records,
    _iterate_aligned_class_values, _finish_checked_output_pass,
)
from boolean_rank_deflated_certificate import calculate_directed_root_upper, calculate_interval_absolute_bounds
from stream_boolean_rank_solver import manage_source_iterator_lifetime


class NativeDirectCertificateRefusal(ValueError):
    def __init__(self, receipt):
        super().__init__("direct candidate original-answer bound exceeds epsilon")
        self.receipt = receipt


def validate_native_direct_arguments(alpha, epsilon, method, precision):
    if method not in ("stationary", "clique-direct"):
        raise ValueError("unknown native direct method")
    if (type(alpha) is not float or not math.isfinite(alpha) or not 0 <= alpha < 1
            or type(epsilon) is not float or not math.isfinite(epsilon) or epsilon <= 0):
        raise ValueError("finite binary64 alpha in [0,1) and positive epsilon required")
    if type(precision) is not int or precision < 1:
        raise ValueError("positive integer precision required")


def prepare_native_direct_topology(source, method, max_factor_slots):
    topology = validate_native_boolean_source(source, max_factor_slots=max_factor_slots)
    if method == "clique-direct":
        with closing(_iterate_checked_class_records(source, topology["factors"])) as classes:
            for _, _, _, degree in classes:
                if degree != topology["vertices"]-1:
                    raise ValueError("clique direct method requires every original degree n-1")
        if source.snapshot_id != topology["snapshot_id"]:
            raise ValueError("source changed during clique eligibility scan")
    return topology


def prepare_native_direct_coefficients(topology, alpha, total):
    count = topology["vertices"]-1
    topology["clique_scale"] = (1-alpha)*count/((count+alpha)*total)
    topology["clique_offset"] = alpha/(count+alpha)


def calculate_native_direct_target(method, topology, weight):
    if method == "stationary":
        return topology["current_stationary"]
    return topology["clique_scale"]*Fraction.from_float(weight)+topology["clique_offset"]


def certify_native_direct_output(source, path, *, alpha, epsilon, method,
                                 max_factor_slots, precision=60):
    validate_native_direct_arguments(alpha, epsilon, method, precision)
    topology = prepare_native_direct_topology(source, method, max_factor_slots)
    lower = Context(prec=precision, rounding=ROUND_FLOOR, Emin=MIN_EMIN, Emax=MAX_EMAX,
                    flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
    upper = Context(prec=precision, rounding=ROUND_CEILING, Emin=MIN_EMIN, Emax=MAX_EMAX,
                    flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
    zero, one = Decimal(0), Decimal(1)
    a = Fraction.from_float(alpha)
    if method == "clique-direct":
        prepare_native_direct_coefficients(topology,a,source.total_weight)
    snapshot, n = topology["snapshot_id"], topology["vertices"]
    distance_hi = second_moment_hi = zero
    digest, rows = hashlib.sha256(), 0
    with open(path,"rb",buffering=0) as output:
        initial = os.fstat(output.fileno())
        if initial.st_size != 16*n:
            raise ValueError("output length must be exactly 16*n")
        with closing(_iterate_checked_class_records(source,topology["factors"])) as classes:
            for cid, _, height, degree in classes:
                class_squares = zero
                pi_lo, pi_hi = _enclose_exact_fraction_value(Fraction(degree,topology["volume"]),lower,upper)
                with closing(_iterate_aligned_class_values(source,cid,height,output,digest)) as values:
                    for weight, score in values:
                        if method == "stationary":
                            target_lo, target_hi = pi_lo, pi_hi
                            exact_weight = Decimal.from_float(weight)
                            class_squares = upper.add(class_squares,upper.multiply(exact_weight,exact_weight))
                        else:
                            target = calculate_native_direct_target(method,topology,weight)
                            target_lo, target_hi = _enclose_exact_fraction_value(target,lower,upper)
                        _, error = calculate_interval_absolute_bounds(
                            lower.subtract(score,target_hi),upper.subtract(score,target_lo))
                        distance_hi = upper.add(distance_hi,error)
                        rows += 1
                if method == "stationary":
                    second_moment_hi = upper.add(second_moment_hi,upper.divide(class_squares,Decimal(degree)))
        _finish_checked_output_pass(output,16*n,source,snapshot)
        final = os.fstat(output.fileno())
        if (initial.st_size,initial.st_mtime_ns,initial.st_ctime_ns) != (final.st_size,final.st_mtime_ns,final.st_ctime_ns):
            raise ValueError("output descriptor changed during direct certificate")
    bias_hi = moment_hi = zero
    denominator_lo = one
    if method == "stationary":
        total_squared_lo, _ = _enclose_exact_fraction_value(source.total_weight**2,lower,upper)
        moment_hi = upper.subtract(upper.divide(upper.multiply(Decimal(topology["volume"]),second_moment_hi),total_squared_lo),one)
        if moment_hi < 0:
            raise ValueError("negative moment contradicts normalized personalization")
        denominator_lo, _ = _enclose_exact_fraction_value(1-a+a*topology["gap_lower"],lower,upper)
        _, complement_hi = _enclose_exact_fraction_value(1-a,lower,upper)
        if denominator_lo <= 0:
            raise ValueError("stationary comparison denominator must be positive")
        bias_hi = upper.divide(upper.multiply(complement_hi,calculate_directed_root_upper(moment_hi,upper)),denominator_lo)
    error_hi = upper.add(distance_hi,bias_hi)
    return dict(accepted=error_hi<=Decimal.from_float(epsilon),l1_error_upper=str(error_hi),
        target_distance_upper=str(distance_hi),bias_upper=str(bias_hi),
        personalization_moment_upper=str(moment_hi),spectral_denominator_lower=str(denominator_lo),
        gap_lower=str(topology["gap_lower"]),method=method,snapshot_id=snapshot,
        output_sha256=digest.hexdigest(),nrows=rows,output_bytes=16*n,output_read_bytes=16*n,
        row_passes=1,scratch_bytes=0,scratch_read_bytes=0,scratch_write_bytes=0,
        retained_rank_vector_entries=0,retained_factor_decimal_entries=0,
        source_class_passes=3+int(method=="clique-direct"),
        validation_class_rows=topology["validation_class_rows"]+source.class_count*int(method=="clique-direct"),
        decimal_precision=precision,scope="known direct control; paid native validation; scalar certificate; no physical cap")


def publish_native_direct_result(source, destination, *, alpha, epsilon, method,
                                 max_factor_slots, precision=60):
    started = time.perf_counter()
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    validate_native_direct_arguments(alpha,epsilon,method,precision)
    if precision < 16:
        raise ValueError("publication precision must be at least 16")
    topology = prepare_native_direct_topology(source,method,max_factor_slots)
    snapshot, n = topology["snapshot_id"], topology["vertices"]
    preflight_seconds = time.perf_counter()-started
    digest, rows = hashlib.sha256(), 0
    a = Fraction.from_float(alpha)
    if method == "clique-direct":
        prepare_native_direct_coefficients(topology,a,source.total_weight)
    with tempfile.TemporaryDirectory(prefix=f".{destination.name}.candidate-",dir=destination.parent) as private:
        candidate = Path(private)/"candidate.ranks"
        before = time.perf_counter()
        with candidate.open("xb",buffering=65536) as stream:
            with closing(_iterate_checked_class_records(source,topology["factors"])) as classes:
                for cid, _, height, degree in classes:
                    topology["current_stationary"] = Fraction(degree,topology["volume"])
                    count, previous = 0, -1
                    with manage_source_iterator_lifetime(source.iterate_class_vertex_rows(cid)) as values:
                        for vertex, weight in values:
                            if (type(vertex) is not int or not 0 <= vertex < 1<<64 or vertex <= previous
                                    or type(weight) is not float or not math.isfinite(weight) or weight < 0):
                                raise ValueError("invalid source ID/weight in direct output")
                            count, previous = count+1, vertex
                            if count > height:
                                raise ValueError("source class exceeds height")
                            target = calculate_native_direct_target(method,topology,weight)
                            raw = struct.pack("<Qd",vertex,float(target))
                            stream.write(raw)
                            digest.update(raw)
                            rows += 1
                    if count != height:
                        raise ValueError("source class length changed")
            stream.flush()
            os.fsync(stream.fileno())
        output_seconds = time.perf_counter()-before
        before = time.perf_counter()
        certificate = certify_native_direct_output(source,candidate,alpha=alpha,epsilon=epsilon,
            method=method,max_factor_slots=max_factor_slots,precision=precision)
        certificate_seconds = time.perf_counter()-before
        if (rows != n or certificate["nrows"] != rows or certificate["output_sha256"] != digest.hexdigest()
                or source.snapshot_id != snapshot or certificate["snapshot_id"] != snapshot):
            raise ValueError("direct certificate and candidate/source disagree")
        receipt = dict(solver_method=method,certificate_method=method,accepted=certificate["accepted"],
            alpha=alpha,epsilon=epsilon,snapshot_id=snapshot,preflight_seconds=preflight_seconds,
            solve_seconds=0.,output_seconds=output_seconds,certificate_seconds=certificate_seconds,
            publish_seconds=0.,output_rows=rows,output_bytes=16*rows,output_sha256=digest.hexdigest(),
            certificate=certificate,scope="direct scalar candidate with independently checked bytes; no physical cap")
        if not certificate["accepted"]:
            receipt["elapsed_seconds"] = time.perf_counter()-started
            raise NativeDirectCertificateRefusal(receipt)
        before = time.perf_counter()
        os.link(candidate,destination)
        receipt["publish_seconds"] = time.perf_counter()-before
    receipt["elapsed_seconds"] = time.perf_counter()-started
    return receipt

"""Standalone degree-weighted actual-byte certificate for native pair graphs.

Experimental fork keeps the ten previously measured modules unchanged. It
checks the original Boolean operator, never the auxiliary incidence walk.
"""

from contextlib import closing
from decimal import (
    Context, Decimal, DivisionByZero, InvalidOperation, MAX_EMAX, MIN_EMIN,
    Overflow, ROUND_CEILING, ROUND_FLOOR,
)
from fractions import Fraction
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile

from boolean_rank_output_certificate import (
    _enclose_exact_fraction_value, _iterate_checked_class_records,
    _iterate_aligned_class_values, _finish_checked_output_pass,
)
from boolean_rank_deflated_certificate import (
    calculate_directed_root_upper, calculate_interval_absolute_bounds,
)
from boolean_rank_native_gap_source import validate_native_boolean_source


def certify_native_boolean_output(source, output_path, *, alpha, epsilon,
                                   precision=60, scratch_dir=None, max_factor_slots):
    """Enclose original L1 error without publishing the file.

    Requires trusted pinned source and exclusive ownership of stable output.
    Metadata detects observed writes, not adversarial concurrent mutation.
    Refuses insufficient accuracy; source/gap/IO failures raise after cleanup.
    """
    if (type(alpha) is not float or not math.isfinite(alpha) or not 0 <= alpha < 1
            or type(epsilon) is not float or not math.isfinite(epsilon) or epsilon <= 0):
        raise ValueError("finite binary64 alpha in [0,1) and positive epsilon required")
    if type(precision) is not int or precision < 1:
        raise ValueError("positive integer Decimal precision required")
    topology = validate_native_boolean_source(source, max_factor_slots=max_factor_slots)
    factors, n, snapshot = topology["factors"], topology["vertices"], topology["snapshot_id"]
    lower = Context(prec=precision, rounding=ROUND_FLOOR, Emin=MIN_EMIN, Emax=MAX_EMAX,
                    flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
    upper = Context(prec=precision, rounding=ROUND_CEILING, Emin=MIN_EMIN, Emax=MAX_EMAX,
                    flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
    zero, one = Decimal(0), Decimal(1)
    exact_alpha = Fraction.from_float(alpha)
    damping, tolerance = Decimal.from_float(alpha), Decimal.from_float(epsilon)
    complement_lo, complement_hi = _enclose_exact_fraction_value(1-exact_alpha, lower, upper)
    denominator_lo, _ = _enclose_exact_fraction_value(
        (1-exact_alpha)+exact_alpha*topology["gap_lower"], lower, upper)
    if denominator_lo <= 0:
        raise ValueError("native spectral denominator not provably positive")
    factor_lo, factor_hi = [zero]*factors, [zero]*factors
    mass_lo = mass_hi = weighted_squares_hi = residual_l1_hi = zero
    expected_bytes = 16*n
    with open(output_path, "rb", buffering=0) as output:
        initial_stat = os.fstat(output.fileno())
        if initial_stat.st_size != expected_bytes:
            raise ValueError("output length must be exactly 16*n")
        with tempfile.TemporaryDirectory(prefix="native-rank-certificate-", dir=scratch_dir) as private:
            with (Path(private)/"class-totals.jsonl").open("x+b") as scratch:
                first_hash = hashlib.sha256()
                rows = scratch_records = 0
                with closing(_iterate_checked_class_records(source, factors)) as classes:
                    for class_id, groups, height, degree in classes:
                        class_lo = class_hi = zero
                        divisor = Decimal(degree)
                        with closing(_iterate_aligned_class_values(source, class_id, height, output, first_hash)) as values:
                            for _, score in values:
                                mass_lo, mass_hi = lower.add(mass_lo, score), upper.add(mass_hi, score)
                                q_lo, q_hi = lower.divide(score, divisor), upper.divide(score, divisor)
                                class_lo, class_hi = lower.add(class_lo, q_lo), upper.add(class_hi, q_hi)
                                for group in groups:
                                    factor_lo[group] = lower.add(factor_lo[group], q_lo)
                                    factor_hi[group] = upper.add(factor_hi[group], q_hi)
                                rows += 1
                        scratch.write((json.dumps([class_id, str(class_lo), str(class_hi)],
                                                  separators=(",", ":"))+"\n").encode("ascii"))
                        scratch_records += 1
                _finish_checked_output_pass(output, expected_bytes, source, snapshot)
                scratch_bytes = scratch.tell()
                mass_error_lo, mass_error_hi = calculate_interval_absolute_bounds(
                    lower.subtract(one, mass_hi), upper.subtract(one, mass_lo))
                rho_abs_lo = lower.multiply(complement_lo, mass_error_lo)
                rho_squared_lo = lower.multiply(rho_abs_lo, rho_abs_lo)
                output.seek(0)
                scratch.seek(0)
                second_hash = hashlib.sha256()
                with closing(_iterate_checked_class_records(source, factors)) as classes:
                    for class_id, groups, height, degree in classes:
                        record = json.loads(scratch.readline())
                        if len(record) != 3 or record[0] != class_id:
                            raise ValueError("scratch class alignment changed")
                        class_lo, class_hi = Decimal(record[1]), Decimal(record[2])
                        neighbor_lo = lower.subtract(lower.add(factor_lo[groups[0]], factor_lo[groups[1]]), class_hi)
                        neighbor_hi = upper.subtract(upper.add(factor_hi[groups[0]], factor_hi[groups[1]]), class_lo)
                        divisor, class_squares_hi = Decimal(degree), zero
                        with closing(_iterate_aligned_class_values(source, class_id, height, output, second_hash)) as values:
                            for weight, score in values:
                                p_lo, p_hi = _enclose_exact_fraction_value(
                                    Fraction.from_float(weight)/source.total_weight, lower, upper)
                                q_lo, q_hi = lower.divide(score, divisor), upper.divide(score, divisor)
                                incoming_lo = lower.subtract(neighbor_lo, q_hi)
                                incoming_hi = upper.subtract(neighbor_hi, q_lo)
                                r_lo = lower.subtract(lower.add(lower.multiply(complement_lo, p_lo),
                                    lower.multiply(damping, incoming_lo)), score)
                                r_hi = upper.subtract(upper.add(upper.multiply(complement_hi, p_hi),
                                    upper.multiply(damping, incoming_hi)), score)
                                _, absolute_hi = calculate_interval_absolute_bounds(r_lo, r_hi)
                                residual_l1_hi = upper.add(residual_l1_hi, absolute_hi)
                                class_squares_hi = upper.add(class_squares_hi, upper.multiply(absolute_hi, absolute_hi))
                        # All vertices in this class have the same original d.
                        weighted_squares_hi = upper.add(weighted_squares_hi, upper.divide(class_squares_hi, divisor))
                if scratch.read(1):
                    raise ValueError("trailing scratch records")
                _finish_checked_output_pass(output, expected_bytes, source, snapshot)
                final_stat = os.fstat(output.fileno())
                if (final_stat.st_size, final_stat.st_mtime_ns, final_stat.st_ctime_ns) != (
                        initial_stat.st_size, initial_stat.st_mtime_ns, initial_stat.st_ctime_ns):
                    raise ValueError("output descriptor changed during certification")
                if first_hash.digest() != second_hash.digest():
                    raise ValueError("actual output changed between certificate passes")
    variance_hi = upper.subtract(upper.multiply(Decimal(topology["volume"]), weighted_squares_hi), rho_squared_lo)
    if variance_hi < 0:
        raise ValueError("negative variance upper bound contradicts native certificate premises")
    root_hi = calculate_directed_root_upper(variance_hi, upper)
    error_hi = upper.add(mass_error_hi, upper.divide(root_hi, denominator_lo))
    return dict(accepted=error_hi<=tolerance, l1_error_upper=str(error_hi), mass_error_upper=str(mass_error_hi),
                variance_upper=str(variance_hi), rho_squared_lower=str(rho_squared_lo),
                residual_l1_upper=str(residual_l1_hi), root_upper=str(root_hi),
                generic_l1_error_upper=str(upper.divide(residual_l1_hi, complement_lo)),
                gap_lower=str(topology["gap_lower"]), spectral_denominator_lower=str(denominator_lo),
                volume=topology["volume"], decimal_precision=precision, snapshot_id=snapshot,
                output_sha256=first_hash.hexdigest(), nrows=rows, row_passes=2,
                output_bytes=expected_bytes, output_read_bytes=2*expected_bytes,
                scratch_records=scratch_records, scratch_bytes=scratch_bytes,
                scratch_read_bytes=scratch_bytes, scratch_write_bytes=scratch_bytes,
                factor_count=factors, retained_decimal_values=2*factors,
                validation_class_rows=topology["validation_class_rows"], source_class_passes=4,
                scope="trusted connected native pairs; original degree-weighted actual-byte bound; no physical cap")

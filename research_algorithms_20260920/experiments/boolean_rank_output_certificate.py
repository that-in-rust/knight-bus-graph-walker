"""Directed certificate for actual <Qd output of the Boolean PageRank operator.

The prepared source is trusted and pinned: it supplies validated global ID
uniqueness, memberships, degrees, and exact input-weight totals. This module
checks output alignment, not graph preparation. No solver/source implementation
is imported. Class totals live in private sequential scratch, not a P-vector.
"""

from contextlib import closing, contextmanager
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
import struct
import tempfile


RECORD = struct.Struct("<Qd")


@contextmanager
def _manage_source_iterator_lifetime(records):
    try:
        yield records
    finally:
        close = getattr(records, "close", None)
        if close is not None:
            close()


def _enclose_exact_fraction_value(value, lower, upper):
    numerator, denominator = Decimal(value.numerator), Decimal(value.denominator)
    return lower.divide(numerator, denominator), upper.divide(numerator, denominator)


def _iterate_checked_class_records(source, factor_count):
    previous = -1
    classes = active = vertices = 0
    with _manage_source_iterator_lifetime(source.iterate_class_records()) as records:
        for class_id, groups, height, degree, weight_sum in records:
            if type(class_id) is not int or class_id <= previous:
                raise ValueError("source class IDs must be nonnegative and strictly ordered")
            if (type(groups) is not tuple or len(groups) > 2
                    or any(type(group) is not int or not 0 <= group < factor_count for group in groups)
                    or (len(groups) == 2 and groups[0] == groups[1])):
                raise ValueError("source class memberships must be distinct factor IDs")
            if (type(height) is not int or height <= 0 or type(degree) is not int
                    or degree < 0 or (degree and not groups)):
                raise ValueError("invalid source class height or degree")
            if not isinstance(weight_sum, Fraction) or weight_sum < 0:
                raise ValueError("invalid source class weight sum")
            previous = class_id
            classes += 1
            active += int(degree > 0)
            vertices += height
            yield class_id, groups, height, degree
    if (classes != source.class_count or active != source.active_class_count
            or vertices != source.vertex_count):
        raise ValueError("source class counts do not match prepared metadata")


def _iterate_aligned_class_values(source, class_id, height, values, digest):
    previous = -1
    count = 0
    with _manage_source_iterator_lifetime(source.iterate_class_vertex_rows(class_id)) as records:
        for original_id, weight in records:
            if (type(original_id) is not int or not 0 <= original_id < 1 << 64
                    or original_id <= previous):
                raise ValueError("source original IDs must be uint64 and strictly ordered within class")
            if type(weight) is not float or not math.isfinite(weight) or weight < 0:
                raise ValueError("source weight must be a finite nonnegative binary64")
            count += 1
            if count > height:
                raise ValueError("source class row count exceeds its height")
            previous = original_id
            raw = values.read(RECORD.size)
            if len(raw) != RECORD.size:
                raise ValueError("truncated output record")
            digest.update(raw)
            actual_id, score = RECORD.unpack(raw)
            if actual_id != original_id:
                raise ValueError("output original ID does not match class/source order")
            if not math.isfinite(score) or score < 0:
                raise ValueError("output score must be finite and nonnegative")
            yield weight, Decimal.from_float(score)
    if count != height:
        raise ValueError("source class row count does not match its height")


def _finish_checked_output_pass(values, expected_bytes, source, snapshot_id):
    if values.read(1) or os.fstat(values.fileno()).st_size != expected_bytes:
        raise ValueError("output has trailing bytes or changed length")
    if source.snapshot_id != snapshot_id:
        raise ValueError("source snapshot changed during certification")


def certify_boolean_rank_output(source, output_path, *, alpha, epsilon,
                                precision=60, scratch_dir=None, max_factor_slots) -> dict:
    """Certify the original stationary operator on the exact published f64 bits.

    ``max_factor_slots`` caps supplied F, including unused factors, before any
    source scan, Decimal creation, output open, or scratch creation. Two F-sized
    Decimal endpoint arrays plus constant scalar state are retained. This is not
    a physical memory cap. Precision may be any positive integer; coarse bounds
    can refuse an otherwise accurate answer.

    Malformed parameters/output/structural metadata raise ValueError. Source and
    I/O exceptions propagate. A well-formed but insufficient certificate returns
    accepted=False. Both passes read the same descriptor and must hash equally;
    the receipt identifies that descriptor's bytes, not a future pathname state.
    """
    factor_count = source.factor_count
    if (type(max_factor_slots) is not int or max_factor_slots < 0
            or type(factor_count) is not int or factor_count < 0
            or factor_count > max_factor_slots):
        raise ValueError("factor count exceeds max_factor_slots or invalid factor capacity")
    if (type(alpha) not in (int, float) or not 0 <= alpha < 1
            or type(epsilon) not in (int, float) or epsilon <= 0):
        raise ValueError("alpha must be in [0, 1); epsilon must be positive binary64")
    try:
        alpha, epsilon = float(alpha), float(epsilon)
    except OverflowError as error:
        raise ValueError("alpha and epsilon must be finite binary64") from error
    if not math.isfinite(alpha) or not math.isfinite(epsilon):
        raise ValueError("alpha and epsilon must be finite binary64")
    if type(precision) is not int or precision < 1:
        raise ValueError("precision must be a positive integer")
    vertex_count = source.vertex_count
    total_weight = source.total_weight
    if (type(vertex_count) is not int or vertex_count < 0
            or not isinstance(total_weight, Fraction)
            or (vertex_count > 0 and total_weight <= 0)
            or (vertex_count == 0 and total_weight != 0)):
        raise ValueError("invalid prepared vertex count or exact total weight")
    snapshot_id = source.snapshot_id
    lower = Context(prec=precision, rounding=ROUND_FLOOR, Emin=MIN_EMIN, Emax=MAX_EMAX,
                    capitals=1, clamp=0, flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
    upper = Context(prec=precision, rounding=ROUND_CEILING, Emin=MIN_EMIN, Emax=MAX_EMAX,
                    capitals=1, clamp=0, flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
    zero = Decimal(0)
    damping = Decimal.from_float(alpha)
    tolerance = Decimal.from_float(epsilon)
    # Computing Decimal(1) - damping in ambient precision can lose 1-alpha.
    complement_lo, complement_hi = _enclose_exact_fraction_value(
        Fraction(1) - Fraction.from_float(alpha), lower, upper)
    factor_lo, factor_hi = [zero] * factor_count, [zero] * factor_count
    dangling_lo = dangling_hi = zero
    residual_lo = residual_hi = zero
    expected_bytes = RECORD.size * vertex_count
    events = [{"event": "factor_capacity_admitted", "factor_count": factor_count,
               "max_factor_slots": max_factor_slots}]

    # Unbuffered reads make seek/rescan observe changed contents rather than an
    # old userspace read buffer. The descriptor stays open across both passes.
    with open(output_path, "rb", buffering=0) as values:
        if os.fstat(values.fileno()).st_size != expected_bytes:
            raise ValueError("output length must be exactly 16 * vertex_count bytes")
        with tempfile.TemporaryDirectory(prefix="boolean-rank-certificate-", dir=scratch_dir) as private:
            with (Path(private) / "class-totals.jsonl").open("x+b") as scratch:
                first_hash = hashlib.sha256()
                scratch_records = first_rows = 0
                with closing(_iterate_checked_class_records(source, factor_count)) as classes:
                    for class_id, groups, height, degree in classes:
                        class_lo = class_hi = zero
                        denominator = Decimal(degree)
                        with closing(_iterate_aligned_class_values(source, class_id, height, values, first_hash)) as aligned:
                            for _, score in aligned:
                                first_rows += 1
                                if degree:
                                    q_lo, q_hi = lower.divide(score, denominator), upper.divide(score, denominator)
                                    class_lo, class_hi = lower.add(class_lo, q_lo), upper.add(class_hi, q_hi)
                                    for group in groups:
                                        factor_lo[group] = lower.add(factor_lo[group], q_lo)
                                        factor_hi[group] = upper.add(factor_hi[group], q_hi)
                                else:
                                    dangling_lo = lower.add(dangling_lo, score)
                                    dangling_hi = upper.add(dangling_hi, score)
                        record = json.dumps([class_id, str(class_lo), str(class_hi)], separators=(",", ":"))
                        scratch.write((record + "\n").encode("ascii"))
                        scratch_records += 1
                _finish_checked_output_pass(values, expected_bytes, source, snapshot_id)
                scratch_bytes = scratch.tell()
                events.append({"event": "published_totals_scanned", "rows": first_rows,
                               "scratch_records": scratch_records, "scratch_bytes": scratch_bytes})

                values.seek(0)
                scratch.seek(0)
                second_hash = hashlib.sha256()
                second_rows = 0
                with closing(_iterate_checked_class_records(source, factor_count)) as classes:
                    for class_id, groups, height, degree in classes:
                        record = json.loads(scratch.readline())
                        if len(record) != 3 or record[0] != class_id:
                            raise ValueError("scratch class ID does not match source class order")
                        class_lo, class_hi = Decimal(record[1]), Decimal(record[2])
                        neighbor_lo = neighbor_hi = zero
                        if degree:
                            for group in groups:
                                neighbor_lo = lower.add(neighbor_lo, factor_lo[group])
                                neighbor_hi = upper.add(neighbor_hi, factor_hi[group])
                            if len(groups) == 2:
                                neighbor_lo, neighbor_hi = (lower.subtract(neighbor_lo, class_hi),
                                                            upper.subtract(neighbor_hi, class_lo))
                        denominator = Decimal(degree)
                        with closing(_iterate_aligned_class_values(source, class_id, height, values, second_hash)) as aligned:
                            for weight, score in aligned:
                                second_rows += 1
                                preference_lo, preference_hi = _enclose_exact_fraction_value(
                                    Fraction.from_float(weight) / total_weight, lower, upper)
                                incoming_lo = lower.multiply(preference_lo, dangling_lo)
                                incoming_hi = upper.multiply(preference_hi, dangling_hi)
                                if degree:
                                    q_lo, q_hi = lower.divide(score, denominator), upper.divide(score, denominator)
                                    incoming_lo = lower.add(incoming_lo, lower.subtract(neighbor_lo, q_hi))
                                    incoming_hi = upper.add(incoming_hi, upper.subtract(neighbor_hi, q_lo))
                                base_lo = lower.multiply(complement_lo, preference_lo)
                                base_hi = upper.multiply(complement_hi, preference_hi)
                                row_lo = lower.subtract(lower.add(base_lo, lower.multiply(damping, incoming_lo)), score)
                                row_hi = upper.subtract(upper.add(base_hi, upper.multiply(damping, incoming_hi)), score)
                                abs_lo = zero if row_lo <= zero <= row_hi else min(row_lo.copy_abs(), row_hi.copy_abs())
                                abs_hi = max(row_lo.copy_abs(), row_hi.copy_abs())
                                residual_lo = lower.add(residual_lo, abs_lo)
                                residual_hi = upper.add(residual_hi, abs_hi)
                if scratch.read(1):
                    raise ValueError("trailing scratch class records")
                _finish_checked_output_pass(values, expected_bytes, source, snapshot_id)
                if first_hash.digest() != second_hash.digest():
                    raise ValueError("output contents changed between certificate passes (SHA-256 mismatch)")
                events.append({"event": "original_residual_scanned", "rows": second_rows,
                               "output_hashes_match": True})

    error_upper = upper.divide(residual_hi, complement_lo)
    events.append({"event": "private_scratch_removed"})
    return {
        "accepted": error_upper <= tolerance,
        "residual_l1_lower": str(residual_lo), "residual_l1_upper": str(residual_hi),
        "l1_error_upper": str(error_upper), "output_sha256": first_hash.hexdigest(),
        "snapshot_id": snapshot_id, "nrows": first_rows, "row_passes": 2,
        "output_bytes": expected_bytes, "output_read_bytes": 2 * expected_bytes,
        "scratch_bytes": scratch_bytes, "scratch_records": scratch_records,
        "scratch_read_bytes": scratch_bytes, "scratch_write_bytes": scratch_bytes,
        "factor_count": factor_count, "retained_decimal_values": 2 * factor_count,
        "retained_decimal_constant_state": True, "decimal_precision": precision,
        "events": events,
        "scope": "original loop-free Boolean union; exact published binary64 scores",
    }

"""Ordinary direct residual control for immutable, validated binary incidence.

Certifies the actual serialized scores, not a reconstructed factor solution.
Requires IEEE binary64 nearest/even with gradual underflow throughout the call.
Only the two factor endpoint payloads are 16*F bytes; this is not a RAM bound.
The upstream manifest must already validate degrees and factor cardinalities.
"""

from array import array
from decimal import Context, Decimal, ROUND_CEILING
import hashlib
from itertools import repeat
import math
from pathlib import Path
import struct
import sys

import probe_native_incidence_pagerank as base


def validate_binary64_runtime_contract():
    """Probe live arithmetic, without changing a process/thread rounding mode."""
    info = sys.float_info
    if (info.radix, info.mant_dig, info.min_exp, info.max_exp) != (2, 53, -1021, 1024):
        raise ValueError("IEEE binary64 arithmetic required")
    one, half, quarter = float("1"), float.fromhex("0x1p-53"), float.fromhex("0x1p-54")
    odd = math.nextafter(one, math.inf)
    even = math.nextafter(odd, math.inf)
    if (one+half != one or one-quarter != one or odd+half != even
            or -one-half != -one or -one+quarter != -one):
        raise ValueError("runtime nearest/even rounding required")
    tiny = base.DOUBLE.unpack(struct.pack("<Q", 1))[0]
    triple = base.DOUBLE.unpack(struct.pack("<Q", 3))[0]
    half_one = float("0.5")
    # Inspect result bits: comparisons themselves may treat subnormal inputs as zero.
    witnesses = ((info.min*half_one, 1 << 51), (info.min/float("2"), 1 << 51),
                 (tiny*one, 1), (tiny/one, 1), (tiny+tiny, 2),
                 (info.min-math.nextafter(info.min, 0.0), 1),
                 (tiny*half_one, 0), (triple*half_one, 2))
    if any(base.DOUBLE.pack(value) != struct.pack("<Q", bits) for value, bits in witnesses):
        raise ValueError("runtime gradual underflow and nearest/even subnormals required")


def round_finite_binary64_down(value):
    if not math.isfinite(value):
        raise ValueError("nonfinite binary64 operation or overflow")
    result = math.nextafter(value, -math.inf)
    if not math.isfinite(result):
        raise ValueError("binary64 outward endpoint overflow")
    return result


def round_finite_binary64_up(value):
    if not math.isfinite(value):
        raise ValueError("nonfinite binary64 operation or overflow")
    result = math.nextafter(value, math.inf)
    if not math.isfinite(result):
        raise ValueError("binary64 outward endpoint overflow")
    return result


def enclose_integer_binary64_value(value):
    """Tight adjacent enclosure; never silently use float(integer) as exact."""
    if type(value) is not int or value < 0:
        raise ValueError("nonnegative exact integer required")
    try:
        rounded = float(value)
    except OverflowError as error:
        raise ValueError("integer conversion overflow") from error
    if not math.isfinite(rounded):
        raise ValueError("nonfinite integer conversion")
    numerator, denominator = rounded.as_integer_ratio()
    if numerator < value*denominator:
        return rounded, round_finite_binary64_up(rounded)
    if numerator > value*denominator:
        return round_finite_binary64_down(rounded), rounded
    return rounded, rounded


def divide_interval_positive_bounds(low, high, divisor_low, divisor_high):
    """Internal kernel: ordered finite interval, 0 < divisor_low <= divisor_high."""
    left = divisor_high if low >= 0.0 else divisor_low
    right = divisor_low if high >= 0.0 else divisor_high
    return round_finite_binary64_down(low/left), round_finite_binary64_up(high/right)


def multiply_interval_positive_bounds(low, high, factor_low, factor_high):
    """Internal kernel: ordered finite interval, 0 <= factor_low <= factor_high."""
    left = factor_low if low >= 0.0 else factor_high
    right = factor_high if high >= 0.0 else factor_low
    return round_finite_binary64_down(low*left), round_finite_binary64_up(high*right)


def stream_framed_rank_records(path, output, shape, identity):
    """Fully consume each stream once, including framing and exact-byte hashes."""
    n, f, z = shape
    source_hash, output_hash = hashlib.sha256(), hashlib.sha256()
    memberships = 0
    with path.open("rb") as rows, output.open("rb") as scores:
        raw = rows.read(base.HEADER.size)
        if len(raw) != base.HEADER.size or base.HEADER.unpack(raw) != (base.MAGIC, n, f, z):
            raise ValueError("changed or truncated incidence header")
        source_hash.update(raw)
        for _ in range(n):
            raw = rows.read(base.ROW.size)
            if len(raw) != base.ROW.size:
                raise ValueError("truncated incidence row")
            source_hash.update(raw)
            degree, count = base.ROW.unpack(raw)
            if count > f or bool(degree) != bool(count):
                raise ValueError("invalid incidence row size or degree")
            memberships += count
            if memberships > z:
                raise ValueError("invalid incidence membership count")
            raw = rows.read(4*count)
            if len(raw) != 4*count:
                raise ValueError("truncated incidence memberships")
            source_hash.update(raw)
            ids = struct.unpack(f"<{count}I", raw)
            if ids and (ids[-1] >= f or any(a >= b for a, b in zip(ids, ids[1:]))):
                raise ValueError("invalid incidence factor ordering")
            raw = scores.read(base.DOUBLE.size)
            if len(raw) != base.DOUBLE.size:
                raise ValueError("truncated complete output")
            output_hash.update(raw)
            value = base.DOUBLE.unpack(raw)[0]
            if not math.isfinite(value):
                raise ValueError("nonfinite published rank")
            yield degree, ids, value
        if rows.read(1) or memberships != z:
            raise ValueError("invalid incidence trailing data or count")
        if scores.read(1):
            raise ValueError("invalid complete output trailing data")
    identity.update(source_sha256=source_hash.hexdigest(), output_sha256=output_hash.hexdigest())


def certify_binary64_rank_output(path, output, source, epsilon, precision=50):
    """Return a finite certificate or raise ValueError; never modify either input.

    source=None means exact uniform p; otherwise p is a single-vertex impulse.
    epsilon uses Decimal(str(epsilon)), as in the frozen reference. A returned
    accepted=False means only that this upper bound exceeds that tolerance.
    Malformed input, nonfinite arithmetic, and unsupported runtimes are refused.
    """
    if type(precision) is not int or precision < 20:
        raise ValueError("invalid certificate precision")
    if type(epsilon) not in (int, float, Decimal):
        raise ValueError("invalid finite positive epsilon")
    tolerance = Decimal(str(epsilon))
    if not tolerance.is_finite() or tolerance <= 0:
        raise ValueError("invalid finite positive epsilon")
    validate_binary64_runtime_contract()
    path, output = Path(path), Path(output)
    n, f, z = base.read_incidence_store_header(path)
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid personalization source")
    source_bytes, output_bytes = path.stat().st_size, output.stat().st_size
    if source_bytes != base.HEADER.size+n*base.ROW.size+4*z or not 2*f <= z <= n*f:
        raise ValueError("invalid complete incidence length or cardinality framing")
    if output_bytes != 8*n:
        raise ValueError("incorrect complete output length")

    lows, highs = array("d", repeat(0.0, f)), array("d", repeat(0.0, f))
    if lows.itemsize != 8 or highs.itemsize != 8:
        raise ValueError("binary64 factor arrays required")
    dangling_low = dangling_high = 0.0
    first_identity, second_identity = {}, {}
    for degree, ids, value in stream_framed_rank_records(path, output, (n, f, z), first_identity):
        if degree:
            dl, dh = enclose_integer_binary64_value(degree)
            yl, yh = divide_interval_positive_bounds(value, value, dl, dh)
            for factor in ids:
                lows[factor] = round_finite_binary64_down(lows[factor]+yl)
                highs[factor] = round_finite_binary64_up(highs[factor]+yh)
        else:
            dangling_low = round_finite_binary64_down(dangling_low+value)
            dangling_high = round_finite_binary64_up(dangling_high+value)

    # Enclose exact 17/20 and 3/20 independently; do not subtract rounded alpha.
    alpha_low, alpha_high = divide_interval_positive_bounds(17.0, 17.0, 20.0, 20.0)
    base_low, base_high = divide_interval_positive_bounds(3.0, 3.0, 20.0, 20.0)
    if source is None:
        nl, nh = enclose_integer_binary64_value(n)
        dangling_low, dangling_high = divide_interval_positive_bounds(dangling_low, dangling_high, nl, nh)
        base_low, base_high = divide_interval_positive_bounds(base_low, base_high, nl, nh)
    residual_upper = 0.0
    for i, (degree, ids, value) in enumerate(
            stream_framed_rank_records(path, output, (n, f, z), second_identity)):
        sl = sh = 0.0
        for factor in ids:
            sl = round_finite_binary64_down(sl+lows[factor])
            sh = round_finite_binary64_up(sh+highs[factor])
        if degree:
            dl, dh = enclose_integer_binary64_value(degree)
            yl, yh = divide_interval_positive_bounds(value, value, dl, dh)
            ql, qh = enclose_integer_binary64_value(len(ids))
            qyl, qyh = multiply_interval_positive_bounds(yl, yh, ql, qh)
            # Q*y is subtracted with crossed endpoints. Negative values are valid.
            sl, sh = round_finite_binary64_down(sl-qyh), round_finite_binary64_up(sh-qyl)
        if source is None or i == source:
            sl = round_finite_binary64_down(sl+dangling_low)
            sh = round_finite_binary64_up(sh+dangling_high)
            bl, bh = base_low, base_high
        else:
            bl = bh = 0.0
        sl, sh = multiply_interval_positive_bounds(sl, sh, alpha_low, alpha_high)
        rl = round_finite_binary64_down(round_finite_binary64_down(bl+sl)-value)
        rh = round_finite_binary64_up(round_finite_binary64_up(bh+sh)-value)
        residual_upper = round_finite_binary64_up(residual_upper+max(abs(rl), abs(rh)))

    if first_identity != second_identity:
        raise ValueError("source or output identity changed between scans")
    validate_binary64_runtime_contract()
    upper = Context(prec=precision, rounding=ROUND_CEILING)
    exact_residual = Decimal.from_float(residual_upper)
    error_upper = upper.divide(upper.multiply(exact_residual, Decimal(20)), Decimal(3))
    if not error_upper.is_finite():
        raise ValueError("nonfinite final error bound")
    with Path(__file__).open("rb") as code:
        code_hash = hashlib.file_digest(code, "sha256").hexdigest()
    return {
        "accepted": error_upper <= tolerance,
        "l1_error_upper": str(error_upper), "residual_l1_upper": str(exact_residual),
        "decimal_precision": precision, "damping_exact": "17/20", "source_zero_based": source,
        "vertices": n, "factors": f, "memberships": z,
        "row_scans": 2, "output_scans": 2, "membership_visits": 2*z,
        "retained_factor_payload_bytes": 16*f, "retained_decimal_vector_values": 0,
        "logical_read_bytes": 2*source_bytes+2*output_bytes, "logical_write_bytes": 0,
        "output_bytes": output_bytes, **first_identity, "certificate_code_sha256": code_hash,
        "read_counter_scope": "two complete source/output payload scans; initial header and code hash excluded",
        "memory_scope": "selected endpoint payload only; excludes row decoder, Python, buffers, hashes and OS cache",
        "runtime_contract": "IEEE binary64 nearest/even, gradual underflow, finite outward endpoints throughout",
        "scope": "ordinary original-operator residual of actual serialized binary64 scores; upstream validated immutable source",
    }

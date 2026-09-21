"""Explicit row accuracy choices with the same validated graph publication ledger.

Exact dyadic accumulation and midpoint interval representatives are ordinary
numerical controls, not newly invented graph algorithms. Frozen publishers remain
unchanged; this separate implementation makes both extension points explicit.
"""

from array import array
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN
import hashlib
import math
import os
from pathlib import Path
import tempfile
import time

import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as frozen
import precision_placed_rank_certificate as placed

build_certificate_source_manifest = frozen.build_certificate_source_manifest
validate_positive_scatter_value = frozen.validate_positive_scatter_value
bound_positive_scatter_error = frozen.bound_positive_scatter_error
bound_rump_scatter_error = frozen.bound_rump_scatter_error
DYADIC_DENOMINATOR = Decimal(1 << 1074)


def accumulate_exact_dyadic_row(h, ids):
    total = 0
    for j in ids:
        value = h[j]
        if not math.isfinite(value):
            raise ValueError("nonfinite factor state")
        numerator, denominator = value.as_integer_ratio()
        total += numerator << (1074-(denominator.bit_length()-1))
    return total


def enclose_exact_dyadic_row(h, ids, lower, upper):
    exact_integer = Decimal(accumulate_exact_dyadic_row(h, ids))
    return lower.divide(exact_integer, DYADIC_DENOMINATOR), upper.divide(exact_integer, DYADIC_DENOMINATOR)


def enclose_selected_rank_row(h, ids, degree, bl, bh, lower, upper, row_sum):
    if row_sum == "decimal":
        return frozen.enclose_reconstructed_row_value(h, ids, degree, bl, bh, lower, upper)
    lo, hi = (enclose_exact_dyadic_row(h, ids, lower, upper) if row_sum == "exact"
              else placed.enclose_binary64_signed_sum(h, ids, lower, upper))
    denominator = Decimal(20*degree+17*len(ids))
    lo = lower.add(lower.multiply(Decimal(20), bl), lower.multiply(Decimal(17), lo))
    hi = upper.add(upper.multiply(Decimal(20), bh), upper.multiply(Decimal(17), hi))
    return lower.divide(lo, denominator), upper.divide(hi, denominator)


def select_interval_rank_value(low, high, representative, nearest):
    selected = low if representative == "lower" else nearest.divide(nearest.add(low, high), Decimal(2))
    return float(selected)


def publish_accuracy_frontier_certificate(store, metadata, manifest, h, output, source, epsilon,
        precision=50, maximum_factor_bytes=2**30, summation_bound="gamma", *,
        row_sum="decimal", representative="lower"):
    started = time.perf_counter()
    store, metadata, output = Path(store), Path(metadata), Path(output)
    if output.resolve() in (store.resolve(), metadata.resolve()):
        raise ValueError("output must not replace source or metadata")
    if (type(precision) is not int or precision < 20 or type(epsilon) not in (int, float, Decimal)
            or type(maximum_factor_bytes) is not int or maximum_factor_bytes < 0
            or summation_bound not in ("gamma", "rump") or row_sum not in ("decimal", "rump", "exact")
            or representative not in ("lower", "midpoint")):
        raise ValueError("invalid certificate parameters")
    tolerance = Decimal(str(epsilon))
    if not tolerance.is_finite() or tolerance <= 0:
        raise ValueError("invalid certificate tolerance")
    placed.validate_gradual_binary64_environment()
    n, f, z, isolates = frozen.validate_certificate_manifest_identity(store, metadata, manifest)
    store_bytes, metadata_bytes = store.stat().st_size, metadata.stat().st_size
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid personalization source")
    if not isinstance(h, array) or h.typecode != "d" or h.itemsize != 8 or len(h) != f:
        raise ValueError("one binary64 factor-state array required")
    if 16*f > maximum_factor_bytes:
        raise ValueError("factor payload budget exceeded before scatter allocation")
    state_hash = hashlib.sha256()
    for value in h:
        if not math.isfinite(value):
            raise ValueError("nonfinite factor state")
        state_hash.update(base.DOUBLE.pack(value))
    lower, upper = Context(prec=precision, rounding=ROUND_FLOOR), Context(prec=precision, rounding=ROUND_CEILING)
    nearest = Context(prec=precision, rounding=ROUND_HALF_EVEN)
    zero = Decimal(0)
    source_isolate = False
    if source is not None:
        with metadata.open("rb") as handle:
            handle.seek(frozen.HEADER.size+source)
            flag = handle.read(1)
        if flag not in (b"\0", b"\1"):
            raise ValueError("invalid source isolate flag")
        source_isolate = flag == b"\1"
    if source is None:
        denominator = Decimal(20*n-17*isolates)
        uniform_lo, uniform_hi = lower.divide(Decimal(3), denominator), upper.divide(Decimal(3), denominator)
    scatter = base.create_zero_float_vector(f)
    preparation_ms = 1000*(time.perf_counter()-started)
    publication_started = time.perf_counter()
    row_error = output_error = zero
    output_hash, temporary = hashlib.sha256(), None
    maximum_row_memberships = 0
    try:
        with tempfile.NamedTemporaryFile(prefix="rank-accuracy-", dir=output.parent, delete=False) as handle:
            temporary = Path(handle.name)
            for i, (degree, ids) in enumerate(base.iter_incidence_row_records(store)):
                maximum_row_memberships = max(maximum_row_memberships, len(ids))
                if source is None:
                    bl, bh = uniform_lo, uniform_hi
                elif i == source:
                    bl = bh = Decimal(1) if source_isolate else Decimal("0.15")
                else:
                    bl = bh = zero
                if degree:
                    lo, hi = enclose_selected_rank_row(h, ids, degree, bl, bh, lower, upper, row_sum)
                    yhat = select_interval_rank_value(lo, hi, representative, nearest)
                    validate_positive_scatter_value(yhat)
                    delta = frozen.measure_interval_value_distance(yhat, lo, hi, upper)
                    row_error = upper.add(row_error, upper.multiply(Decimal(degree+len(ids)), delta))
                    for j in ids:
                        value = scatter[j]+yhat
                        if not math.isfinite(value):
                            raise ValueError("scatter overflow")
                        scatter[j] = value
                    xhat = degree*yhat
                    xlo, xhi = lower.multiply(Decimal(degree), lo), upper.multiply(Decimal(degree), hi)
                else:
                    xlo, xhi = bl, bh
                    xhat = select_interval_rank_value(bl, bh, representative, nearest)
                if not math.isfinite(xhat):
                    raise ValueError("output overflow")
                encoded = base.DOUBLE.pack(xhat)
                emitted = base.DOUBLE.unpack(encoded)[0]
                if not math.isfinite(emitted):
                    raise ValueError("nonfinite encoded output")
                output_error = upper.add(output_error, frozen.measure_interval_value_distance(emitted, xlo, xhi, upper))
                if handle.write(encoded) != len(encoded):
                    raise OSError("incomplete rank write")
                output_hash.update(encoded)
        residual = scatter_error = zero
        with metadata.open("rb") as handle:
            handle.seek(frozen.HEADER.size+n)
            for j in range(f):
                raw = handle.read(frozen.CARDINALITY.size)
                if len(raw) != frozen.CARDINALITY.size:
                    raise ValueError("truncated factor cardinality")
                size = frozen.CARDINALITY.unpack(raw)[0]
                left, right = Decimal.from_float(scatter[j]), Decimal.from_float(h[j])
                distance = max(upper.subtract(left, right), upper.subtract(right, left))
                residual = upper.add(residual, upper.multiply(Decimal(size), distance))
                bounder = bound_positive_scatter_error if summation_bound == "gamma" else bound_rump_scatter_error
                scatter_error = upper.add(scatter_error, bounder(size, scatter[j], precision))
        final_bound = upper.add(upper.divide(upper.multiply(Decimal(17),
            upper.add(upper.add(residual, scatter_error), row_error)), Decimal(3)), output_error)
        accepted = final_bound <= tolerance
        if temporary.stat().st_size != 8*n:
            raise ValueError("incorrect complete output length")
        result = {"accepted": accepted, "l1_error_upper": str(final_bound),
            "factor_residual_upper": str(residual), "scatter_error_upper": str(scatter_error),
            "row_error_upper": str(row_error), "publication_error_upper": str(output_error),
            "decimal_precision": precision, "preparation_ms": preparation_ms,
            "publication_row_scans": 1, "membership_visits": z,
            "retained_decimal_vector_values": 0, "factor_state_payload_bytes": 16*f,
            "source_identity_read_bytes": store_bytes,
            "metadata_identity_read_bytes": metadata_bytes,
            "logical_read_bytes": 2*store_bytes+metadata_bytes+8*f+int(source is not None),
            "logical_write_bytes": 8*n, "output_bytes": 8*n if accepted else 0,
            "output_sha256": output_hash.hexdigest() if accepted else None,
            "candidate_output_sha256": output_hash.hexdigest(), "state_sha256": state_hash.hexdigest(),
            "source_sha256": manifest["source_sha256"], "metadata_sha256": manifest["metadata_sha256"],
            "source_zero_based": source, "damping_exact": "17/20", "summation_bound": summation_bound,
            "row_sum": row_sum, "representative": representative,
            "maximum_row_memberships": maximum_row_memberships,
            "exact_accumulator_magnitude_bits_upper": (2098+(maximum_row_memberships-1).bit_length()
                if row_sum == "exact" and maximum_row_memberships else 0),
            "high_precision_membership_operations": 2*z if row_sum == "decimal" else 0,
            "high_precision_membership_counter_scope": "legacy counter: Decimal additions only; excludes exact-integer work",
            "decimal_membership_additions": 2*z if row_sum == "decimal" else 0,
            "exact_integer_membership_accumulations": z if row_sum == "exact" else 0,
            "read_counter_scope": "modeled bulk payload; small header reads excluded",
            "scope": "same-buffer certificate, immutable validated source; no physical RAM or durability guarantee"}
        if accepted:
            os.replace(temporary, output)
            temporary = None
        result["publication_ms"] = 1000*(time.perf_counter()-publication_started)
        result["total_ms"] = 1000*(time.perf_counter()-started)
        return result
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


publish_streaming_rank_certificate = publish_accuracy_frontier_certificate

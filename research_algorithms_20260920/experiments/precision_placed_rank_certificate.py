"""Research precision-placement variant; published Rump bounds, not a new theorem.

Only row reconstruction differs from the frozen scalar-ledger publisher. A
private module instance keeps its audited publication mechanics without changing
the frozen module or the shared imported instance. Arithmetic mode and immutable
source/state ownership must hold throughout the call.
"""

from decimal import Decimal
import importlib.util
import math
from pathlib import Path
import struct
import sys

import streaming_rank_publication_certificate as frozen


def validate_gradual_binary64_environment():
    frozen.validate_binary64_rounding_environment()
    tiny = struct.unpack("<d", struct.pack("<Q", 1))[0]
    normal = sys.float_info.min
    previous = struct.unpack("<d", struct.pack("<Q", (1 << 52)-1))[0]
    # Check subnormal inputs, outputs, and signed cancellation with runtime ops.
    observed = (tiny+tiny, normal+tiny, normal-previous, previous-normal)
    expected = (2, (1 << 52)+1, 1, (1 << 63)+1)
    if tuple(struct.unpack("<Q", struct.pack("<d", x))[0] for x in observed) != expected:
        raise ValueError("gradual binary64 underflow required; FTZ/DAZ not admitted")


def enclose_binary64_signed_sum(h, ids, lower, upper):
    total = absolute = 0.0
    count = 0
    for j in ids:
        value = h[j]
        if not math.isfinite(value):
            raise ValueError("nonfinite factor state")
        total += value
        absolute += abs(value)
        count += 1
    if not math.isfinite(total) or not math.isfinite(absolute):
        raise ValueError("row summation overflow")
    exact_total = Decimal.from_float(total)
    if count <= 1 or absolute == 0:
        return exact_total, exact_total
    # Theorem 3.5 in Rump 2012. Same-order signed/absolute sums; allows underflow.
    exponent = math.frexp(absolute)[1]-1
    ufp = Decimal.from_float(math.ldexp(1.0, exponent))
    radius = upper.divide(upper.multiply(Decimal(count-1), ufp), Decimal(2**53))
    return lower.subtract(exact_total, radius), upper.add(exact_total, radius)


def enclose_reconstructed_row_value(h, ids, degree, bl, bh, lower, upper):
    lo, hi = enclose_binary64_signed_sum(h, ids, lower, upper)
    denominator = Decimal(20*degree+17*len(ids))
    lo = lower.add(lower.multiply(Decimal(20), bl), lower.multiply(Decimal(17), lo))
    hi = upper.add(upper.multiply(Decimal(20), bh), upper.multiply(Decimal(17), hi))
    return lower.divide(lo, denominator), upper.divide(hi, denominator)


_spec = importlib.util.spec_from_file_location(
    "_precision_private_publisher", Path(frozen.__file__))
_publisher = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_publisher)
_publisher.enclose_reconstructed_row_value = enclose_reconstructed_row_value

build_certificate_source_manifest = frozen.build_certificate_source_manifest
validate_certificate_manifest_identity = frozen.validate_certificate_manifest_identity
validate_positive_scatter_value = frozen.validate_positive_scatter_value
bound_positive_scatter_error = frozen.bound_positive_scatter_error
bound_rump_scatter_error = frozen.bound_rump_scatter_error


def publish_streaming_rank_certificate(store, metadata, manifest, h, output, source, epsilon,
                                       precision=50, maximum_factor_bytes=2**30, summation_bound="gamma"):
    validate_gradual_binary64_environment()
    result = _publisher.publish_streaming_rank_certificate(
        store, metadata, manifest, h, output, source, epsilon, precision,
        maximum_factor_bytes, summation_bound)
    result.update(precision_schedule="binary64-row-sums/scalar-decimal-ledger",
                  high_precision_membership_operations=0,
                  binary64_row_additions=2*manifest["memberships"],
                  high_precision_row_enclosures=manifest["vertices"]-manifest["isolates"],
                  arithmetic_contract="nearest-even with gradual underflow, unchanged throughout call")
    return result

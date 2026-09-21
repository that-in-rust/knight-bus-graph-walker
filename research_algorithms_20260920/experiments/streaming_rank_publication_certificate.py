"""Research-only scalar-ledger certification for validated binary incidence rows.

Fixed exact damping 17/20. The source and manifest must remain immutable during
publication. Payload admission is not a process or physical-memory guarantee.
"""

from array import array
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR
import hashlib
import math
import os
from pathlib import Path
import struct
import sys
import tempfile
import time

import probe_native_incidence_pagerank as base

HEADER = struct.Struct("<8sQQQQ32s")
CARDINALITY = struct.Struct("<Q")
MAGIC = b"KBCERT01"


def build_certificate_source_manifest(store, metadata):
    started = time.perf_counter()
    store, metadata = Path(store), Path(metadata)
    if store.resolve() == metadata.resolve():
        raise ValueError("source and metadata paths must differ")
    n, f, z = base.read_incidence_store_header(store)
    sizes = array("Q", [0])*f
    isolates = 0
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix="cert-manifest-", dir=metadata.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(bytes(HEADER.size))
            for degree, ids in base.iter_incidence_row_records(store):
                isolates += degree == 0
                handle.write(bytes((int(degree == 0),)))
                for j in ids:
                    sizes[j] += 1
            if any(size < 2 for size in sizes) or sum(sizes) != z:
                raise ValueError("invalid retained factor cardinality")
            for degree, ids in base.iter_incidence_row_records(store):
                if degree != sum(sizes[j]-1 for j in ids):
                    raise ValueError("source degree disagrees with exact cardinalities")
            for size in sizes:
                handle.write(CARDINALITY.pack(size))
            source_hash = base.hash_file_content_bytes(store)
            handle.seek(0)
            handle.write(HEADER.pack(MAGIC, n, f, z, isolates, bytes.fromhex(source_hash)))
        os.replace(temporary, metadata)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    metadata_hash = base.hash_file_content_bytes(metadata)
    return {"vertices": n, "factors": f, "memberships": z, "isolates": isolates,
            "source_sha256": source_hash, "metadata_sha256": metadata_hash,
            "metadata_bytes": metadata.stat().st_size,
            "builder_factor_payload_bytes": 8*f, "validation_row_scans": 2,
            "logical_read_bytes": 3*store.stat().st_size+metadata.stat().st_size,
            "logical_write_bytes": metadata.stat().st_size+HEADER.size,
            "setup_ms": 1000*(time.perf_counter()-started),
            "scope": "two validating row scans, one identity scan; full row decoder; not external-memory import"}


def validate_certificate_manifest_identity(store, metadata, manifest):
    n, f, z = base.read_incidence_store_header(store)
    if base.hash_file_content_bytes(store) != manifest["source_sha256"]:
        raise ValueError("source identity mismatch")
    if base.hash_file_content_bytes(metadata) != manifest["metadata_sha256"]:
        raise ValueError("metadata identity mismatch")
    with Path(metadata).open("rb") as handle:
        raw = handle.read(HEADER.size)
    if len(raw) != HEADER.size:
        raise ValueError("invalid metadata length")
    magic, mn, mf, mz, isolates, digest = HEADER.unpack(raw)
    if (magic != MAGIC or (mn, mf, mz) != (n, f, z) or isolates > n
            or digest.hex() != manifest["source_sha256"]
            or isolates != manifest["isolates"]
            or (n, f, z) != (manifest["vertices"], manifest["factors"], manifest["memberships"])):
        raise ValueError("metadata source identity mismatch")
    if Path(metadata).stat().st_size != HEADER.size+n+8*f:
        raise ValueError("invalid metadata length")
    return n, f, z, isolates


def validate_positive_scatter_value(value):
    if not math.isfinite(value) or value < 0 or (value != 0 and value < sys.float_info.min):
        raise ValueError("scatter requires finite nonnegative normal-or-zero values")


def validate_binary64_rounding_environment():
    if (sys.float_info.radix, sys.float_info.mant_dig) != (2, 53):
        raise ValueError("binary64 nearest rounding required")
    # Runtime operations distinguish nearest/even from the three directed modes.
    one = float("1")
    half, quarter = math.ldexp(one, -53), math.ldexp(one, -54)
    if one+half != one or one-quarter != one:
        raise ValueError("runtime nearest-even rounding required")


def bound_positive_scatter_error(size, value, precision):
    if type(size) is not int or size < 2 or size >= 2**52:
        raise ValueError("factor cardinality violates gamma admission")
    validate_positive_scatter_value(value)
    upper = Context(prec=precision, rounding=ROUND_CEILING)
    # s * gamma_s/(1-gamma_s) = s^2/(2^53-2s), with exact integers.
    coefficient = upper.divide(Decimal(size*size), Decimal(2**53-2*size))
    return upper.multiply(coefficient, Decimal.from_float(value))


def bound_rump_scatter_error(size, value, precision):
    if type(size) is not int or size < 2:
        raise ValueError("invalid factor cardinality")
    validate_positive_scatter_value(value)
    if value == 0:
        return Decimal(0)
    bits = struct.unpack("<Q", base.DOUBLE.pack(value))[0]
    exponent = ((bits >> 52) & 0x7ff)-1023
    ufp = Decimal.from_float(math.ldexp(1.0, exponent))
    upper = Context(prec=precision, rounding=ROUND_CEILING)
    # Rump (2012), Theorem 3.5; positivity makes absolute and signed sums identical.
    return upper.divide(upper.multiply(Decimal(size*(size-1)), ufp), Decimal(2**53))


def measure_interval_value_distance(value, low, high, upper):
    exact = Decimal.from_float(value)
    return max(upper.subtract(exact, low), upper.subtract(high, exact), Decimal(0))


def enclose_reconstructed_row_value(h, ids, degree, bl, bh, lower, upper):
    lo = hi = Decimal(0)
    for j in ids:
        exact = Decimal.from_float(h[j])
        lo, hi = lower.add(lo, exact), upper.add(hi, exact)
    denominator = Decimal(20*degree+17*len(ids))
    lo = lower.add(lower.multiply(Decimal(20), bl), lower.multiply(Decimal(17), lo))
    hi = upper.add(upper.multiply(Decimal(20), bh), upper.multiply(Decimal(17), hi))
    return lower.divide(lo, denominator), upper.divide(hi, denominator)


def publish_streaming_rank_certificate(store, metadata, manifest, h, output, source, epsilon,
                                       precision=50, maximum_factor_bytes=2**30, summation_bound="gamma"):
    started = time.perf_counter()
    store, metadata, output = Path(store), Path(metadata), Path(output)
    if output.resolve() in (store.resolve(), metadata.resolve()):
        raise ValueError("output must not replace source or metadata")
    if (type(precision) is not int or precision < 20 or not math.isfinite(epsilon) or epsilon <= 0
            or type(maximum_factor_bytes) is not int or maximum_factor_bytes < 0
            or summation_bound not in ("gamma", "rump")):
        raise ValueError("invalid certificate parameters")
    validate_binary64_rounding_environment()
    n, f, z, isolates = validate_certificate_manifest_identity(store, metadata, manifest)
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
    zero = Decimal(0)
    source_isolate = False
    if source is not None:
        with metadata.open("rb") as handle:
            handle.seek(HEADER.size+source)
            flag = handle.read(1)
        if flag not in (b"\0", b"\1"):
            raise ValueError("invalid source isolate flag")
        source_isolate = flag == b"\1"
    if source is None:
        denominator = Decimal(20*n-17*isolates)
        uniform_lo = lower.divide(Decimal(3), denominator)
        uniform_hi = upper.divide(Decimal(3), denominator)
    scatter = base.create_zero_float_vector(f)
    preparation_ms = 1000*(time.perf_counter()-started)
    publication_started = time.perf_counter()
    row_error = output_error = zero
    output_hash = hashlib.sha256()
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix="rank-ledger-", dir=output.parent, delete=False) as handle:
            temporary = Path(handle.name)
            for i, (degree, ids) in enumerate(base.iter_incidence_row_records(store)):
                if source is None:
                    bl, bh = uniform_lo, uniform_hi
                elif i == source:
                    bl = bh = Decimal(1) if source_isolate else Decimal("0.15")
                else:
                    bl = bh = zero
                if degree:
                    lo, hi = enclose_reconstructed_row_value(h, ids, degree, bl, bh, lower, upper)
                    yhat = float(lo)
                    validate_positive_scatter_value(yhat)
                    row_delta = measure_interval_value_distance(yhat, lo, hi, upper)
                    row_error = upper.add(row_error, upper.multiply(Decimal(degree+len(ids)), row_delta))
                    for j in ids:
                        value = scatter[j]+yhat
                        if not math.isfinite(value):
                            raise ValueError("scatter overflow")
                        scatter[j] = value
                    xhat = degree*yhat
                    xlo, xhi = lower.multiply(Decimal(degree), lo), upper.multiply(Decimal(degree), hi)
                else:
                    xlo, xhi = bl, bh
                    xhat = float(bl)
                if not math.isfinite(xhat):
                    raise ValueError("output overflow")
                encoded = base.DOUBLE.pack(xhat)
                emitted = base.DOUBLE.unpack(encoded)[0]
                if not math.isfinite(emitted):
                    raise ValueError("nonfinite encoded output")
                output_error = upper.add(output_error, measure_interval_value_distance(emitted, xlo, xhi, upper))
                if handle.write(encoded) != len(encoded):
                    raise OSError("incomplete rank write")
                output_hash.update(encoded)
        residual = scatter_error = zero
        with metadata.open("rb") as handle:
            handle.seek(HEADER.size+n)
            for j in range(f):
                raw = handle.read(CARDINALITY.size)
                if len(raw) != CARDINALITY.size:
                    raise ValueError("truncated factor cardinality")
                size = CARDINALITY.unpack(raw)[0]
                left, right = Decimal.from_float(scatter[j]), Decimal.from_float(h[j])
                distance = max(upper.subtract(left, right), upper.subtract(right, left))
                residual = upper.add(residual, upper.multiply(Decimal(size), distance))
                bounder = bound_positive_scatter_error if summation_bound == "gamma" else bound_rump_scatter_error
                scatter_error = upper.add(scatter_error, bounder(size, scatter[j], precision))
        factor_bound = upper.add(upper.add(residual, scatter_error), row_error)
        final_bound = upper.add(upper.divide(upper.multiply(Decimal(17), factor_bound), Decimal(3)), output_error)
        accepted = final_bound <= Decimal(str(epsilon))
        if temporary.stat().st_size != 8*n:
            raise ValueError("incorrect complete output length")
        publication_ms = 1000*(time.perf_counter()-publication_started)
        result = {"accepted": accepted, "l1_error_upper": str(final_bound),
                "factor_residual_upper": str(residual), "scatter_error_upper": str(scatter_error),
                "row_error_upper": str(row_error), "publication_error_upper": str(output_error),
                "decimal_precision": precision, "preparation_ms": preparation_ms,
                "publication_ms": publication_ms, "total_ms": 1000*(time.perf_counter()-started),
                "publication_row_scans": 1, "membership_visits": z,
                "retained_decimal_vector_values": 0, "factor_state_payload_bytes": 16*f,
                "source_identity_read_bytes": store_bytes,
                "metadata_identity_read_bytes": metadata_bytes,
                "logical_read_bytes": 2*store_bytes+metadata_bytes+8*f+int(source is not None),
                "logical_write_bytes": 8*n, "output_bytes": 8*n if accepted else 0,
                "output_sha256": output_hash.hexdigest() if accepted else None,
                "candidate_output_sha256": output_hash.hexdigest(), "state_sha256": state_hash.hexdigest(),
                "source_sha256": manifest["source_sha256"], "metadata_sha256": manifest["metadata_sha256"],
                "source_zero_based": source, "damping_exact": "17/20",
                "summation_bound": summation_bound,
                "read_counter_scope": "modeled bulk payload; small header reads excluded",
                "scope": "same-written bytes, immutable validated source; no readback or durable-storage guarantee"}
        if accepted:
            os.replace(temporary, output)
            temporary = None
        result["publication_ms"] = 1000*(time.perf_counter()-publication_started)
        result["total_ms"] = 1000*(time.perf_counter()-started)
        return result
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

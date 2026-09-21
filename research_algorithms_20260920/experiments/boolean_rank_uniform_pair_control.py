"""Ordinary symmetry/inverse specialization for canonical uniform pair sources.

All arithmetic before the final F binary64 conversions is exact. The resulting
state and its existing local output lift remain provisional, not certified.
The independent output entry point compares actual bytes to the exact target.
"""

from array import array
from fractions import Fraction
import hashlib
import math
import os
import struct

from boolean_rank_spectral_source import validate_boolean_spectral_source
from stream_boolean_rank_solver import manage_source_iterator_lifetime


def _prepare_uniform_exact_factors(source, *, alpha, max_factor_slots):
    """Replace F exact dyadic sums in place with F exact rational S values."""
    if type(alpha) is not float or not math.isfinite(alpha) or not 0 <= alpha < 1:
        raise ValueError("alpha must be a binary64 value in [0,1)")
    topology = validate_boolean_spectral_source(source, max_factor_slots=max_factor_slots)
    factors = topology["factors"]
    degree = topology["degree"]
    total_weight = source.total_weight
    snapshot = topology["snapshot_id"]
    exact_alpha = Fraction.from_float(alpha)
    gamma = Fraction(1) - exact_alpha
    eigenvalue = topology["height"] * (factors - 3) - 1
    group_weights = [Fraction()] * factors
    numerator_bits, denominator_bits = 0, factors
    metrics = dict(
        dimension=factors, factor_slots=factors, active_classes=topology["class_count"],
        validation_class_rows=topology["validation_class_rows"], validation_class_passes=1,
        class_passes=1, class_records=0, matrix_applications=0, iterations=0,
        exact_factor_sum_slots=factors, exact_factor_sum_updates=0,
        exact_factor_fraction_slots=factors,
        exact_factor_numerator_bits_peak=0, exact_factor_denominator_bits_peak=factors,
        factor_score_payload_bytes=0, certified=False,
        arithmetic="exact Fraction normalization and closed form",
        payload_scope="F Fraction slots: dyadic sums replaced in place by exact S; "
                      "packed scores charged separately if requested; "
                      "excludes scalar temporaries, object/container overhead, SQLite, prepared artifact, output storage",
    )
    with manage_source_iterator_lifetime(source.iterate_class_records()) as records:
        for _, groups, _, _, weight in records:
            if (not isinstance(weight, Fraction) or weight < 0
                    or weight.denominator & (weight.denominator - 1)):
                raise ValueError("prepared class weights must be nonnegative exact dyadic sums")
            metrics["class_records"] += 1
            for factor in groups:
                previous = group_weights[factor]
                combined = previous + weight
                numerator_bits += combined.numerator.bit_length() - previous.numerator.bit_length()
                denominator_bits += combined.denominator.bit_length() - previous.denominator.bit_length()
                group_weights[factor] = combined
                metrics["exact_factor_sum_updates"] += 1
                metrics["exact_factor_numerator_bits_peak"] = max(
                    metrics["exact_factor_numerator_bits_peak"], numerator_bits)
                metrics["exact_factor_denominator_bits_peak"] = max(
                    metrics["exact_factor_denominator_bits_peak"], denominator_bits)
    if (metrics["class_records"] != topology["class_count"]
            or sum(group_weights, Fraction()) != 2 * total_weight
            or source.snapshot_id != snapshot):
        raise ValueError("prepared uniform pair source changed during direct solve")
    metrics["exact_factor_numerator_bits_final"] = numerator_bits
    metrics["exact_factor_denominator_bits_final"] = denominator_bits

    # The constant mode is fixed by mass; only the F-1 factor-contrast modes
    # survive in incident totals. Never divide the mass mode by 1-alpha.
    stationary = Fraction(2, factors * degree)
    incident_uniform = Fraction(2, factors)
    contrast_scale = gamma / (degree - exact_alpha * eigenvalue)
    metrics["exact_factor_state_numerator_bits_peak"] = metrics["exact_factor_numerator_bits_peak"]
    metrics["exact_factor_state_denominator_bits_peak"] = metrics["exact_factor_denominator_bits_peak"]
    for factor in range(factors):
        previous = group_weights[factor]
        combined = stationary + contrast_scale * (previous / total_weight - incident_uniform)
        numerator_bits += combined.numerator.bit_length() - previous.numerator.bit_length()
        denominator_bits += combined.denominator.bit_length() - previous.denominator.bit_length()
        group_weights[factor] = combined
        metrics["exact_factor_state_numerator_bits_peak"] = max(
            metrics["exact_factor_state_numerator_bits_peak"], numerator_bits)
        metrics["exact_factor_state_denominator_bits_peak"] = max(
            metrics["exact_factor_state_denominator_bits_peak"], denominator_bits)
    metrics["exact_factor_state_numerator_bits_final"] = numerator_bits
    metrics["exact_factor_state_denominator_bits_final"] = denominator_bits
    return topology, group_weights, gamma, total_weight, metrics


def solve_uniform_pair_rank_state(source, *, alpha, max_factor_slots):
    """Return once-rounded F state, not an output certificate or RAM-byte cap."""
    topology, exact_scores, gamma, _, metrics = _prepare_uniform_exact_factors(
        source, alpha=alpha, max_factor_slots=max_factor_slots)
    factor_scores = array("d", (float(value) for value in exact_scores))
    metrics["factor_score_payload_bytes"] = len(factor_scores) * factor_scores.itemsize
    metrics["arithmetic"] += "; one binary64 rounding per factor"
    return dict(snapshot_id=topology["snapshot_id"], alpha=alpha, gamma=gamma, method="uniform-pair-direct",
                factor_scores=factor_scores, class_scores=None, metrics=metrics)


def certify_uniform_pair_exact_output(source, output_path, *, alpha, epsilon, max_factor_slots):
    """Compare one forward <Qd byte scan to exact analytic PageRank targets.

    epsilon is a positive finite float, integer, or Fraction, interpreted exactly.
    Malformed data raises; well-formed data always gets a complete hash/error,
    even when the tolerance is missed. The source remains caller-owned. No
    scratch, n/P arrays, rounded solver state, or output rewinds are used.

    The receipt identifies the bytes read from one descriptor, not a future
    pathname state. Descriptor size/timestamps and source snapshot are checked;
    this is not an atomic snapshot of an adversarially mutable output file.
    F Fraction slots and integer bit payloads are charged in metrics, not RSS.
    """
    if (type(epsilon) not in (int, float, Fraction) or epsilon <= 0
            or (type(epsilon) is float and not math.isfinite(epsilon))):
        raise ValueError("epsilon must be a positive finite float, integer, or Fraction")
    topology, exact_scores, gamma, total_weight, metrics = _prepare_uniform_exact_factors(
        source, alpha=alpha, max_factor_slots=max_factor_slots)
    tolerance = Fraction(epsilon)
    damping = Fraction.from_float(alpha)
    degree, height = topology["degree"], topology["height"]
    snapshot = topology["snapshot_id"]
    record_format = struct.Struct("<Qd")
    expected_bytes = record_format.size * topology["vertices"]
    weight_scale = degree * gamma / ((degree + damping) * total_weight)
    error = Fraction()
    digest = hashlib.sha256()
    classes_seen = rows_seen = 0
    metrics.update(l1_error_numerator_bits_peak=0, l1_error_denominator_bits_peak=1,
                   exact_target_numerator_bits_peak=0, exact_target_denominator_bits_peak=1)
    with open(output_path, "rb", buffering=0) as values:
        initial_stat = os.fstat(values.fileno())
        if initial_stat.st_size != expected_bytes:
            raise ValueError("output length must be exactly 16 * vertex_count bytes")
        with manage_source_iterator_lifetime(source.iterate_class_records()) as classes:
            for class_id, groups, h, d, weight in classes:
                if class_id != classes_seen or h != height or d != degree:
                    raise ValueError("uniform class metadata changed during certification")
                group_sum = sum((exact_scores[factor] for factor in groups), Fraction())
                base = gamma * weight / total_weight
                class_q = (base + damping * height * group_sum) / (degree + damping * (1 + height))
                offset = degree * damping * (group_sum - class_q) / (degree + damping)
                count, previous_id, class_weight = 0, -1, Fraction()
                with manage_source_iterator_lifetime(source.iterate_class_vertex_rows(class_id)) as vertices:
                    for original_id, original_weight in vertices:
                        if (type(original_id) is not int or not 0 <= original_id < 1 << 64
                                or original_id <= previous_id):
                            raise ValueError("source IDs must be uint64 and strictly increasing within class")
                        if (type(original_weight) is not float or not math.isfinite(original_weight)
                                or original_weight < 0):
                            raise ValueError("source weights must be finite nonnegative binary64")
                        count += 1
                        if count > height:
                            raise ValueError("source class row count exceeds height")
                        previous_id = original_id
                        raw = values.read(record_format.size)
                        if len(raw) != record_format.size:
                            raise ValueError("truncated output record")
                        actual_id, actual = record_format.unpack(raw)
                        if actual_id != original_id:
                            raise ValueError("output ID does not match exact source class/ID order")
                        if not math.isfinite(actual) or actual < 0:
                            raise ValueError("output scores must be finite and nonnegative")
                        exact_weight = Fraction.from_float(original_weight)
                        class_weight += exact_weight
                        target = weight_scale * exact_weight + offset
                        error += abs(Fraction.from_float(actual) - target)
                        metrics["l1_error_numerator_bits_peak"] = max(
                            metrics["l1_error_numerator_bits_peak"], error.numerator.bit_length())
                        metrics["l1_error_denominator_bits_peak"] = max(
                            metrics["l1_error_denominator_bits_peak"], error.denominator.bit_length())
                        metrics["exact_target_numerator_bits_peak"] = max(
                            metrics["exact_target_numerator_bits_peak"], target.numerator.bit_length())
                        metrics["exact_target_denominator_bits_peak"] = max(
                            metrics["exact_target_denominator_bits_peak"], target.denominator.bit_length())
                        digest.update(raw)
                        rows_seen += 1
                if count != height or class_weight != weight:
                    raise ValueError("source class height or exact weight sum changed")
                classes_seen += 1
        if classes_seen != topology["class_count"] or rows_seen != topology["vertices"]:
            raise ValueError("source/output universe changed during certification")
        if values.read(1):
            raise ValueError("output has trailing bytes")
        final_stat = os.fstat(values.fileno())
        if (final_stat.st_size, final_stat.st_mtime_ns, final_stat.st_ctime_ns) != (
                initial_stat.st_size, initial_stat.st_mtime_ns, initial_stat.st_ctime_ns):
            raise ValueError("output descriptor changed during certification")
        if source.snapshot_id != snapshot:
            raise ValueError("source snapshot changed during certification")
    accepted = error <= tolerance
    metrics["certified"] = accepted
    return dict(accepted=accepted, l1_error_upper=str(error), output_sha256=digest.hexdigest(),
                nrows=rows_seen, row_passes=1, scratch_bytes=0, snapshot_id=snapshot,
                output_bytes=expected_bytes, output_read_bytes=expected_bytes,
                factor_count=topology["factors"], retained_fraction_values=topology["factors"],
                class_passes=metrics["validation_class_passes"] + metrics["class_passes"] + 1,
                class_records=metrics["validation_class_rows"] + metrics["class_records"] + classes_seen,
                scope="exact analytic-target comparison for pinned canonical uniform all-pair "
                      "Boolean PageRank; actual published binary64 bytes", metrics=metrics)

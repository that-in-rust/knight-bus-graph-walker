"""Established arithmetic reductions as a same-source triangle-count control."""

import math

from probe_masked_matching_triangles import (
    intersect_three_interval_runs,
    require_integer_scalar_values,
    validate_mask_interval_runs,
)
from probe_shared_residue_triangles import (
    build_residue_histogram_runs,
    count_shared_residue_roots,
)


def sum_euclidean_floor_values(n, modulus, slope, intercept):
    require_integer_scalar_values(n, modulus, slope, intercept)
    if n < 0 or modulus < 1 or slope < 0 or intercept < 0:
        raise ValueError("nonnegative floor-sum inputs and positive modulus required")
    total, steps = 0, 0
    # The Euclidean floor-sum recurrence; Python integers do not wrap at 64 bits.
    while True:
        steps += 1
        quotient, slope = divmod(slope, modulus)
        total += n * (n - 1) // 2 * quotient
        quotient, intercept = divmod(intercept, modulus)
        total += n * quotient
        height = slope * n + intercept
        if height < modulus:
            return total, steps
        n, intercept = divmod(height, modulus)
        modulus, slope = slope, modulus


def count_two_periodic_masks(length, periods, masks):
    require_integer_scalar_values(length, *periods)
    if length < 1 or len(periods) != 2 or len(masks) != 2:
        raise ValueError("two masks and positive length required")
    if any(q < 1 or length % q for q in periods):
        raise ValueError("native periods must divide length")
    runs = [validate_mask_interval_runs(mask, q) for q, mask in zip(periods, masks)]
    modulus = math.gcd(*periods)
    histograms = [build_residue_histogram_runs(q, mask, modulus)
                  for q, mask in zip(periods, runs)]
    first, second = histograms
    i, j, total, steps = 0, 0, 0, 0
    while i < len(first) and j < len(second):
        a, b = first[i], second[j]
        left, right = max(a[0], b[0]), min(a[1], b[1])
        total += (right - left) * a[2] * b[2]
        steps += 1
        i += int(a[1] == right)
        j += int(b[1] == right)
    return total * (length // math.lcm(*periods)), {
        "histogram_runs": len(first) + len(second),
        "overlap_steps": steps, "events_processed": 0,
    }


def count_support_reduced_roots(length, periods, masks, event_budget=None):
    require_integer_scalar_values(length, *periods)
    if length < 1 or len(periods) != 3 or len(masks) != 3:
        raise ValueError("three masks and positive length required")
    if any(q < 1 or length % q for q in periods):
        raise ValueError("native periods must divide length")
    if event_budget is not None:
        require_integer_scalar_values(event_budget)
        if event_budget < 0:
            raise ValueError("event budget must be nonnegative")
    normalized = [validate_mask_interval_runs(mask, q) for q, mask in zip(periods, masks)]
    metrics = {"equal_period_merges": 0, "histogram_runs": 0,
               "overlap_steps": 0, "floor_steps": 0, "events_processed": 0}
    grouped = {}
    for q, mask in zip(periods, normalized):
        if q in grouped:
            grouped[q] = list(intersect_three_interval_runs(grouped[q], mask, [(0, q)]))
            metrics["equal_period_merges"] += 1
        else:
            grouped[q] = mask
    if any(not mask for mask in grouped.values()):
        metrics["method"] = "empty"
        return 0, metrics
    remaining = [(q, mask) for q, mask in grouped.items() if mask != [(0, q)]]
    if not remaining:
        metrics["method"] = "full"
        return length, metrics
    if len(remaining) == 1:
        q, mask = remaining[0]
        metrics["method"] = "one_mask"
        return (length // q) * sum(right - left for left, right in mask), metrics
    if len(remaining) == 2:
        qs, runs = zip(*remaining)
        value, pair_metrics = count_two_periodic_masks(length, qs, runs)
        metrics.update(pair_metrics, method="two_histograms")
        return value, metrics
    singletons = [i for i, (_, mask) in enumerate(remaining)
                  if len(mask) == 1 and mask[0][1] - mask[0][0] == 1]
    if len(singletons) >= 2:
        i, j = singletons[:2]
        q1, first = remaining[i]
        q2, second = remaining[j]
        q3, third = remaining[3 - i - j]
        residue1, residue2 = first[0][0], second[0][0]
        common = math.gcd(q1, q2)
        metrics["method"] = "singleton_crt"
        if (residue2 - residue1) % common:
            return 0, metrics
        reduced = q2 // common
        step = 0 if reduced == 1 else (
            (residue2 - residue1) // common * pow(q1 // common, -1, reduced)
        ) % reduced
        stride = math.lcm(q1, q2)
        residue = (residue1 + q1 * step) % stride
        total = 0
        for left, right in third:
            high, steps = sum_euclidean_floor_values(length // stride, q3, stride, residue + q3 - left)
            low, more = sum_euclidean_floor_values(length // stride, q3, stride, residue + q3 - right)
            total += high - low
            metrics["floor_steps"] += steps + more
        return total, metrics
    value, sweep_metrics = count_shared_residue_roots(length, periods, normalized, event_budget)
    metrics.update(sweep_metrics, method="shared_events")
    return value, metrics

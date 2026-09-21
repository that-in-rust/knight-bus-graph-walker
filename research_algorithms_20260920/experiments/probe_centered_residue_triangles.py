"""Exact centered three-mask count with an admitted sparse CRT join.

Research kernel only: arithmetic-work counters are not physical RAM or time bounds.
CRT, residue histograms, centering and inclusion-exclusion are established tools.
"""

import math
from bisect import bisect_right
from collections import defaultdict
from itertools import combinations

from probe_masked_matching_triangles import (
    require_integer_scalar_values, shift_mask_interval_runs, validate_mask_interval_runs,
)
from probe_shared_residue_triangles import (
    build_residue_histogram_runs, iterate_native_triangle_terms, normalize_native_triangle_source,
)


def calculate_histogram_product_sum(first, second):
    i, j, total, steps = 0, 0, 0, 0
    while i < len(first) and j < len(second):
        a, b = first[i], second[j]
        left, right = max(a[0], b[0]), min(a[1], b[1])
        total += (right - left) * a[2] * b[2]
        steps += 1
        i += int(a[1] == right)
        j += int(b[1] == right)
    return total, steps


def build_centered_histogram_runs(histogram):
    populations = defaultdict(int)
    for left, right, value in histogram:
        populations[value] += right - left
    center = min(populations, key=lambda value: (-populations[value], value))
    deviations = [(left, right, value - center) for left, right, value in histogram
                  if value != center]
    return center, deviations


def iterate_allowed_interval_points(first, second, modulus):
    a, b = first
    c, d = second
    if d - c >= modulus:
        yield from range(a, b)
        return
    start, stop = c % modulus, c % modulus + d - c
    arcs = [(start, min(stop, modulus))]
    if stop > modulus:
        arcs.append((0, stop - modulus))
    # Interior blocks emit at least one u; only the two boundary blocks can miss.
    for left, right in arcs:
        for block in range(a // modulus, (b - 1) // modulus + 1):
            yield from range(max(a, block * modulus + left), min(b, block * modulus + right))


def iterate_compatible_interval_pairs(first, second, modulus):
    require_integer_scalar_values(*first, *second, modulus)
    if modulus < 1 or not (0 <= first[0] < first[1] and 0 <= second[0] < second[1]):
        raise ValueError("positive modulus and nonempty nonnegative intervals required")
    c, d = second
    for u in iterate_allowed_interval_points(first, second, modulus):
        for v in range(c + (u - c) % modulus, d, modulus):
            yield u, v


def count_centered_residue_roots(length, periods, masks, *, pair_budget=None,
                                 run_pair_budget=None, stats=None):
    require_integer_scalar_values(length)
    if length < 1 or len(periods) != 3 or len(masks) != 3:
        raise ValueError("three masks and positive length required")
    require_integer_scalar_values(*periods)
    if any(q < 1 or length % q for q in periods):
        raise ValueError("native periods must divide length")
    for budget in (pair_budget, run_pair_budget):
        if budget is not None:
            require_integer_scalar_values(budget)
            if budget < 0:
                raise ValueError("budgets must be nonnegative")
    normalized = [validate_mask_interval_runs(mask, q) for q, mask in zip(periods, masks)]
    pairs = tuple(combinations(range(3), 2))
    gcds = {(i, j): math.gcd(periods[i], periods[j]) for i, j in pairs}
    marginals = tuple(math.lcm(*(g for pair, g in gcds.items() if i in pair)) for i in range(3))
    shared, native = math.lcm(*gcds.values()), math.lcm(*periods)
    histograms = [build_residue_histogram_runs(q, mask, h)
                  for q, mask, h in zip(periods, normalized, marginals)]
    centered = [build_centered_histogram_runs(histogram) for histogram in histograms]
    centers, deviations = tuple(item[0] for item in centered), [item[1] for item in centered]
    supports = [validate_mask_interval_runs([(a, b) for a, b, _ in runs], h)
                for runs, h in zip(deviations, marginals)]
    metrics = {} if stats is None else stats
    metrics.update(native_period=native, shared_period=shared, marginal_periods=marginals,
                   centers=centers, histogram_runs=sum(map(len, histograms)),
                   deviation_runs=sum(map(len, deviations)),
                   deviation_signs=tuple(sorted({1 if w > 0 else -1 for runs in deviations for _, _, w in runs})),
                   preflight_overlap_steps=0, compatible_pairs=0, run_pairs_visited=0,
                   third_histogram_lookups=0, shared_events_control=0)
    for h, histogram in zip(marginals, histograms):
        wrap = int(histogram[0][2] != histogram[-1][2])
        metrics["shared_events_control"] += (shared // h) * (len(histogram) - 1 + wrap) - wrap
    products, cardinalities = {}, {}
    for i, j in pairs:
        g = gcds[i, j]
        if math.lcm(marginals[i], marginals[j]) != shared or math.gcd(marginals[i], marginals[j]) != g:
            raise AssertionError("three-mask shared-period identity violated")
        first = build_residue_histogram_runs(periods[i], normalized[i], g)
        second = build_residue_histogram_runs(periods[j], normalized[j], g)
        products[i, j], steps = calculate_histogram_product_sum(first, second)
        metrics["preflight_overlap_steps"] += steps
        first = build_residue_histogram_runs(marginals[i], supports[i], g)
        second = build_residue_histogram_runs(marginals[j], supports[j], g)
        cardinalities[i, j], steps = calculate_histogram_product_sum(first, second)
        metrics["preflight_overlap_steps"] += steps
    i, j = min(pairs, key=lambda pair: (cardinalities[pair],
               len(deviations[pair[0]]) * len(deviations[pair[1]]), pair))
    k, planned = 3 - i - j, cardinalities[i, j]
    run_pairs = len(deviations[i]) * len(deviations[j]) if planned else 0
    metrics.update(selected_pair=(i, j), pair_cardinalities=tuple(cardinalities[p] for p in pairs),
                   planned_pairs=planned, planned_run_pairs=run_pairs)
    if pair_budget is not None and planned > pair_budget:
        raise ValueError(f"pair budget exceeded: need {planned}, allowed {pair_budget}")
    if run_pair_budget is not None and run_pairs > run_pair_budget:
        raise ValueError(f"run-pair budget exceeded: need {run_pairs}, allowed {run_pair_budget}")
    singles = [(shared // h) * sum(b - a for a, b in mask)
               for h, mask in zip(marginals, normalized)]
    total = sum(centers[3 - x - y] * products[x, y] for x, y in pairs)
    total -= sum(singles[x] * centers[(x + 1) % 3] * centers[(x + 2) % 3] for x in range(3))
    total += math.prod(centers) * shared
    correction = 0
    if planned:
        h1, h2, h3 = marginals[i], marginals[j], marginals[k]
        g, reduced = gcds[i, j], h2 // gcds[i, j]
        inverse = 0 if reduced == 1 else pow(h1 // g, -1, reduced)
        third, starts = deviations[k], [a for a, _, _ in deviations[k]]
        for a, b, wi in deviations[i]:
            for c, d, wj in deviations[j]:
                metrics["run_pairs_visited"] += 1
                for u, v in iterate_compatible_interval_pairs((a, b), (c, d), g):
                    step = 0 if reduced == 1 else (((v - u) // g) * inverse) % reduced
                    t = (u + h1 * step) % shared
                    point = t % h3
                    position = bisect_right(starts, point) - 1
                    wk = third[position][2] if position >= 0 and point < third[position][1] else 0
                    correction += wi * wj * wk
                    metrics["compatible_pairs"] += 1
                    metrics["third_histogram_lookups"] += 1
    total += correction
    metrics["signed_correction"] = correction
    if metrics["compatible_pairs"] != planned or metrics["run_pairs_visited"] != run_pairs:
        raise AssertionError("join did not match preflight reservation")
    if not 0 <= total <= native:
        raise AssertionError("invalid centered residue count")
    return (length // native) * total, metrics


def compute_centered_period_triangles(bases, length, classes, mask_periods, *,
                                      pair_budget=None, run_pair_budget=None):
    """Count a native graph; budgets are cumulative, admitted one motif at a time.

    The returned source uses the existing lazy complete degree/local-count output.
    Its output work is unchanged. A later motif may refuse after earlier work.
    """
    for budget in (pair_budget, run_pair_budget):
        if budget is not None:
            require_integer_scalar_values(budget)
            if budget < 0:
                raise ValueError("budgets must be nonnegative")
    source = normalize_native_triangle_source(bases, length, classes, mask_periods)
    metrics = {"base_neighbor_probes": 0, "partial_pair_probes": 0,
               "closed_class_triples": 0, "compatible_pairs": 0, "run_pairs_visited": 0,
               "preflight_overlap_steps": 0, "shared_events_control": 0,
               "peak_histogram_runs": 0, "retained_triangle_terms": 0}
    total = 0
    for _, _, _, shift, _, first, second, closing in iterate_native_triangle_terms(source, metrics):
        remaining_pairs = None if pair_budget is None else pair_budget - metrics["compatible_pairs"]
        remaining_runs = None if run_pair_budget is None else run_pair_budget - metrics["run_pairs_visited"]
        masks = first[1], shift_mask_interval_runs(second[1], -shift, second[0]), closing[1]
        count, work = count_centered_residue_roots(
            length, (first[0], second[0], closing[0]), masks,
            pair_budget=remaining_pairs, run_pair_budget=remaining_runs)
        total += count
        metrics["closed_class_triples"] += 1
        for key in ("compatible_pairs", "run_pairs_visited", "preflight_overlap_steps", "shared_events_control"):
            metrics[key] += work[key]
        metrics["peak_histogram_runs"] = max(metrics["peak_histogram_runs"], work["histogram_runs"])
    source["triangle_count"] = total
    return source, total, metrics

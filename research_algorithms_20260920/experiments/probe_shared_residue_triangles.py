"""Finite exact periodic-mask compiler; no native RSS guarantee."""

import heapq
import math
from bisect import bisect_right
from collections import defaultdict

from probe_masked_matching_triangles import (
    enumerate_oriented_shift_pairs,
    require_integer_scalar_values,
    shift_mask_interval_runs,
    validate_mask_interval_runs,
)


def build_residue_histogram_runs(period, intervals, modulus):
    require_integer_scalar_values(modulus)
    intervals = validate_mask_interval_runs(intervals, period)
    if modulus < 1 or period % modulus:
        raise ValueError("histogram modulus must divide the native period")
    events, baseline = defaultdict(int, {0: 0, modulus: 0}), 0
    for left, right in intervals:
        quotient, remainder = divmod(right - left, modulus)
        baseline += quotient
        if not remainder:
            continue
        start = left % modulus
        stop = start + remainder
        events[start] += 1
        if stop <= modulus:
            events[stop] -= 1
        else:
            events[modulus] -= 1
            events[0] += 1
            events[stop - modulus] -= 1
    points, runs, value = sorted(events), [], baseline
    for left, right in zip(points, points[1:]):
        value += events[left]
        if runs and runs[-1][2] == value:
            runs[-1] = runs[-1][0], right, value
        else:
            runs.append((left, right, value))
    if sum((right - left) * value for left, right, value in runs) != sum(right - left for left, right in intervals):
        raise AssertionError("histogram lost multiplicity")
    return runs


def iterate_periodic_histogram_changes(period, horizon, runs):
    if len(runs) == 1:
        return
    wraps = runs[0][2] != runs[-1][2]
    for offset in range(0, horizon, period):
        if offset and wraps:
            yield offset, runs[0][2]
        for left, _, value in runs[1:]:
            yield offset + left, value


def count_shared_residue_roots(length, periods, masks, event_budget=None):
    require_integer_scalar_values(length)
    if len(periods) != 3 or len(masks) != 3 or length < 1:
        raise ValueError("exactly three masks and positive length are required")
    require_integer_scalar_values(*periods)
    if any(q < 1 or length % q for q in periods):
        raise ValueError("native periods must divide the graph length")
    if event_budget is not None:
        require_integer_scalar_values(event_budget)
        if event_budget < 0:
            raise ValueError("event budget must be nonnegative")
    normalized = [validate_mask_interval_runs(runs, q) for q, runs in zip(periods, masks)]
    pair_gcds = math.gcd(periods[0], periods[1]), math.gcd(periods[0], periods[2]), math.gcd(periods[1], periods[2])
    marginals = math.lcm(pair_gcds[0], pair_gcds[1]), math.lcm(pair_gcds[0], pair_gcds[2]), math.lcm(pair_gcds[1], pair_gcds[2])
    shared, native = math.lcm(*pair_gcds), math.lcm(*periods)
    histograms = [build_residue_histogram_runs(q, runs, h) for q, runs, h in zip(periods, normalized, marginals)]
    cyclic, planned, raw = 0, 0, 0
    for q, mask, h, histogram in zip(periods, normalized, marginals, histograms):
        wrap = int(histogram[0][2] != histogram[-1][2])
        per_cycle = len(histogram) - 1 + wrap
        cyclic += (shared // h) * per_cycle
        planned += (shared // h) * per_cycle - wrap
        mask_boundaries = 0 if not mask else 2 * len(mask) - 2 * int(mask[0][0] == 0 and mask[-1][1] == q)
        raw += (native // q) * mask_boundaries
    metrics = {"native_period": native, "shared_period": shared, "marginal_periods": marginals,
               "histogram_runs": sum(map(len, histograms)), "planned_events": planned,
               "events_processed": 0, "shared_cyclic_events": cyclic, "raw_cyclic_events": raw}
    if not all(normalized):
        metrics["planned_events"] = 0
        return 0, metrics
    if event_budget is not None and planned > event_budget:
        raise ValueError(f"event budget exceeded: need {planned}, allowed {event_budget}")
    streams = [iterate_periodic_histogram_changes(h, shared, runs) for h, runs in zip(marginals, histograms)]
    values, heap = [runs[0][2] for runs in histograms], []
    for i, stream in enumerate(streams):
        event = next(stream, None)
        if event is not None:
            heapq.heappush(heap, (event[0], i, event[1]))
    integral, position = 0, 0
    while heap:
        boundary, i, value = heapq.heappop(heap)
        integral += (boundary - position) * math.prod(values)
        position, values[i] = boundary, value
        metrics["events_processed"] += 1
        event = next(streams[i], None)
        if event is not None:
            heapq.heappush(heap, (event[0], i, event[1]))
    integral += (shared - position) * math.prod(values)
    if not 0 <= integral <= native or metrics["events_processed"] != planned:
        raise AssertionError("invalid residue count or event plan")
    return (length // native) * integral, metrics


def normalize_native_triangle_source(bases, length, classes, mask_periods):
    require_integer_scalar_values(bases, length)
    for a, b, delta in mask_periods:
        require_integer_scalar_values(a, b, delta)
    if bases < 1 or length < 1 or classes.keys() != mask_periods.keys():
        raise ValueError("invalid dimensions or unmatched source period keys")
    normalized, pairs, adjacent, edges = {}, defaultdict(dict), defaultdict(set), 0
    for (a, b, delta), intervals in classes.items():
        q = mask_periods[a, b, delta]
        require_integer_scalar_values(a, b, delta, q)
        if not 0 <= a < b < bases or not 0 <= delta < length or q < 1 or length % q:
            raise ValueError("invalid integer native edge class")
        runs = validate_mask_interval_runs(intervals, q)
        if not runs:
            continue
        record = q, tuple(runs), tuple(left for left, _ in runs)
        normalized[a, b, delta] = record
        pairs[a, b][delta] = record
        adjacent[a].add(b)
        adjacent[b].add(a)
        edges += (length // q) * sum(right - left for left, right in runs)
    return {"bases": bases, "length": length, "classes": normalized,
            "pairs": pairs, "adjacent": adjacent, "edge_count": edges}


def iterate_native_triangle_terms(source, metrics, vertex_base=None):
    pairs, adjacent, length = source["pairs"], source["adjacent"], source["length"]
    for a, b in pairs:
        smaller, other = (adjacent[a], adjacent[b]) if len(adjacent[a]) <= len(adjacent[b]) else (adjacent[b], adjacent[a])
        for c in smaller:
            metrics["base_neighbor_probes"] += 1
            if c <= b or c not in other or (vertex_base is not None and vertex_base not in (a, b, c)):
                continue
            groups = pairs[a, b], pairs[b, c], pairs[a, c]
            for first, second, closing in enumerate_oriented_shift_pairs(groups, length, metrics):
                yield a, b, c, first, closing, groups[0][first], groups[1][second], groups[2][closing]


def compute_local_period_triangles(bases, length, classes, mask_periods, event_budget=None):
    if event_budget is not None:
        require_integer_scalar_values(event_budget)
        if event_budget < 0:
            raise ValueError("event budget must be nonnegative")
    source = normalize_native_triangle_source(bases, length, classes, mask_periods)
    metrics = {"base_neighbor_probes": 0, "partial_pair_probes": 0,
               "closed_class_triples": 0, "histogram_events": 0,
               "raw_cyclic_events": 0, "shared_cyclic_events": 0,
               "peak_histogram_runs": 0, "retained_triangle_terms": 0}
    total = 0
    for _, _, _, shift, _, first, second, closing in iterate_native_triangle_terms(source, metrics):
        remaining = None if event_budget is None else event_budget - metrics["histogram_events"]
        masks = first[1], shift_mask_interval_runs(second[1], -shift, second[0]), closing[1]
        count, work = count_shared_residue_roots(length, (first[0], second[0], closing[0]), masks, remaining)
        total += count
        metrics["closed_class_triples"] += 1
        metrics["histogram_events"] += work["events_processed"]
        metrics["raw_cyclic_events"] += work["raw_cyclic_events"]
        metrics["shared_cyclic_events"] += work["shared_cyclic_events"]
        metrics["peak_histogram_runs"] = max(metrics["peak_histogram_runs"], work["histogram_runs"])
    source["triangle_count"] = total
    return source, total, metrics


def mask_contains_native_root(record, coordinate):
    q, runs, starts = record
    point = coordinate % q
    index = bisect_right(starts, point) - 1
    return index >= 0 and point < runs[index][1]


def iterate_local_triangle_rows(source, block_size):
    require_integer_scalar_values(block_size)
    if block_size < 1:
        raise ValueError("output block size must be positive")
    degree_sum, triangle_sum = 0, 0
    length = source["length"]
    for base in range(source["bases"]):
        for start in range(0, length, block_size):
            stop = min(start + block_size, length)
            degrees, local = [0] * (stop - start), [0] * (stop - start)
            for (a, b, delta), record in source["classes"].items():
                if base not in (a, b):
                    continue
                shift = 0 if base == a else delta
                for i, coordinate in enumerate(range(start, stop)):
                    degrees[i] += mask_contains_native_root(record, coordinate - shift)
            metrics = {"base_neighbor_probes": 0, "partial_pair_probes": 0}
            for a, b, c, shift, closing_shift, first, second, closing in iterate_native_triangle_terms(source, metrics, base):
                offset = 0 if base == a else shift if base == b else closing_shift
                for i, coordinate in enumerate(range(start, stop)):
                    root = coordinate - offset
                    local[i] += (mask_contains_native_root(first, root)
                                 and mask_contains_native_root(second, root + shift)
                                 and mask_contains_native_root(closing, root))
            for degree, triangles in zip(degrees, local):
                if not 0 <= triangles <= degree * (degree - 1) // 2:
                    raise AssertionError("invalid local triangle count")
                degree_sum += degree
                triangle_sum += triangles
                yield degree, triangles
    if degree_sum != 2 * source["edge_count"] or triangle_sum != 3 * source["triangle_count"]:
        raise AssertionError("complete output conservation failure")

"""Finite research probe for the masked-matching triangle manuscript."""

from collections import defaultdict


def require_integer_scalar_values(*values):
    if any(type(value) is not int for value in values):
        raise ValueError("profile scalars must be integers, not booleans")


def validate_mask_interval_runs(intervals, period):
    require_integer_scalar_values(period)
    if period < 1:
        raise ValueError("period must be positive")
    intervals = list(intervals)
    for left, right in intervals:
        require_integer_scalar_values(left, right)
    result = []
    for left, right in sorted(intervals):
        if not 0 <= left < right <= period:
            raise ValueError("invalid half-open interval")
        if result and left < result[-1][1]:
            raise ValueError("overlapping Boolean intervals")
        if result and left == result[-1][1]:
            result[-1] = (result[-1][0], right)
        else:
            result.append((left, right))
    return result


def shift_mask_interval_runs(intervals, displacement, period):
    require_integer_scalar_values(displacement)
    shifted = []
    for left, right in validate_mask_interval_runs(intervals, period):
        start = (left + displacement) % period
        finish = start + right - left
        if finish <= period:
            shifted.append((start, finish))
        else:
            shifted.extend(((start, period), (0, finish - period)))
    return validate_mask_interval_runs(shifted, period)


def intersect_three_interval_runs(first, second, third):
    streams, positions = (first, second, third), [0, 0, 0]
    while all(positions[j] < len(streams[j]) for j in range(3)):
        current = [streams[j][positions[j]] for j in range(3)]
        left = max(item[0] for item in current)
        right = min(item[1] for item in current)
        if left < right:
            yield left, right
        for j in range(3):
            if current[j][1] == right:
                positions[j] += 1


def compile_cover_defect_masks(bases, period, base, defects):
    require_integer_scalar_values(bases, period)
    if bases < 1 or period < 1:
        raise ValueError("invalid vertex universe")
    for (a, b), delta in base.items():
        require_integer_scalar_values(a, b, delta)
        if not 0 <= a < b < bases or not 0 <= delta < period:
            raise ValueError("invalid canonical base edge")
    deleted, added, seen = defaultdict(list), defaultdict(list), set()
    for a, g, b, h, sign in defects:
        require_integer_scalar_values(a, g, b, h, sign)
        if a > b:
            a, g, b, h = b, h, a, g
        if not 0 <= a < b < bases or not (0 <= g < period and 0 <= h < period):
            raise ValueError("invalid or unsupported same-base defect")
        edge = a, g, b, h
        if edge in seen:
            raise ValueError("duplicate defect edge")
        seen.add(edge)
        delta = (h - g) % period
        exists = base.get((a, b)) == delta
        if sign not in (-1, 1) or (sign == -1) != exists:
            raise ValueError("defect does not produce Boolean graph")
        (deleted if exists else added)[(a, b, delta)].append(g)
    classes = {}
    for (a, b), delta in base.items():
        key, start, runs = (a, b, delta), 0, []
        for g in sorted(deleted[(a, b, delta)]):
            if start < g:
                runs.append((start, g))
            start = g + 1
        if start < period:
            runs.append((start, period))
        if runs:
            classes[key] = runs
    for key, points in added.items():
        classes[key] = validate_mask_interval_runs([(g, g + 1) for g in points], period)
    return classes


def record_shifted_range_events(events, base, interval, displacement, period, metric, amount=1):
    records = 0
    for left, right in shift_mask_interval_runs([interval], displacement, period):
        events[(base, left)][metric] += amount
        events[(base, right)][metric] -= amount
        records += 2
    return records


def emit_aligned_triangle_intervals(triangle, masks, period, events, metrics):
    a, b, c, first_delta, closing_delta = triangle
    first, second, closing = masks
    metrics["closed_triples"] += 1
    metrics["join_run_work"] += len(first) + len(second) + len(closing) + 1
    total = 0
    aligned = shift_mask_interval_runs(second, -first_delta, period)
    for interval in intersect_three_interval_runs(first, closing, aligned):
        metrics["root_intervals"] += 1
        total += interval[1] - interval[0]
        for base, displacement in ((a, 0), (b, first_delta), (c, closing_delta)):
            metrics["triangle_events"] += record_shifted_range_events(
                events, base, interval, displacement, period, 1)
    return total


def count_full_shift_triangles(first, second, closing, period):
    # Carry-free polynomial multiplication is a small exact reference backend.
    width = max(1, (period.bit_length() + 7) // 8)
    left = int.from_bytes(b"".join(int(g in first).to_bytes(width, "little")
                                 for g in range(period)), "little")
    right = int.from_bytes(b"".join(int(g in second).to_bytes(width, "little")
                                  for g in range(period)), "little")
    product = (left * right).to_bytes(2 * period * width, "little")
    return sum(int.from_bytes(product[g * width:(g + 1) * width], "little")
               + int.from_bytes(product[(g + period) * width:(g + period + 1) * width], "little")
               for g in closing)


def enumerate_oriented_shift_pairs(groups, period, metrics):
    if any(not group for group in groups):
        return
    i, j = min(((0, 1), (0, 2), (1, 2)), key=lambda pair: len(groups[pair[0]]) * len(groups[pair[1]]))
    missing = 3 - i - j
    for x in groups[i]:
        for y in groups[j]:
            metrics["partial_pair_probes"] += 1
            derived = (x + y) % period if (i, j) == (0, 1) else (y - x) % period
            if derived in groups[missing]:
                shifts = [None, None, None]
                shifts[i], shifts[j], shifts[missing] = x, y, derived
                yield tuple(shifts)


def compute_hybrid_triangle_events(normalized, period, events, metrics):
    pairs, adjacent = defaultdict(dict), defaultdict(set)
    for (a, b, delta), mask in normalized.items():
        pairs[(a, b)][delta] = mask
        adjacent[a].add(b)
        adjacent[b].add(a)
    total = 0
    for a, b in pairs:
        smaller, other = (adjacent[a], adjacent[b]) if len(adjacent[a]) <= len(adjacent[b]) else (adjacent[b], adjacent[a])
        for c in smaller:
            metrics["base_neighbor_probes"] += 1
            if c <= b or c not in other:
                continue
            metrics["base_triangles"] += 1
            groups = (pairs[(a, b)], pairs[(b, c)], pairs[(a, c)])
            full = tuple({d: mask for d, mask in group.items() if mask == [(0, period)]} for group in groups)
            partial = tuple({d: mask for d, mask in group.items() if mask != [(0, period)]} for group in groups)
            if all(full):
                metrics["full_convolutions"] += 1
                count = count_full_shift_triangles(*full, period)
                metrics["full_closed_triples"] += count
                total += period * count
                if count:
                    for base in (a, b, c):
                        records = record_shifted_range_events(events, base, (0, period), 0, period, 1, count)
                        metrics["full_event_records"] += records
                        metrics["triangle_events"] += records
            # First partial position owns the triangle; the four cases are disjoint.
            for selected in ((partial[0], groups[1], groups[2]),
                             (full[0], partial[1], groups[2]),
                             (full[0], full[1], partial[2])):
                for first_delta, second_delta, closing_delta in enumerate_oriented_shift_pairs(selected, period, metrics):
                    total += emit_aligned_triangle_intervals(
                        (a, b, c, first_delta, closing_delta),
                        (groups[0][first_delta], groups[1][second_delta], groups[2][closing_delta]),
                        period, events, metrics)
    return total


def compute_masked_triangle_counts(bases, period, classes, strategy="interval"):
    require_integer_scalar_values(bases, period)
    if bases < 1 or period < 1:
        raise ValueError("invalid vertex universe")
    if strategy not in ("interval", "hybrid"):
        raise ValueError("unknown exact strategy")
    normalized, outgoing = {}, defaultdict(list)
    for (a, b, delta), intervals in classes.items():
        require_integer_scalar_values(a, b, delta)
        if not 0 <= a < b < bases or not 0 <= delta < period:
            raise ValueError("invalid edge class")
        runs = validate_mask_interval_runs(intervals, period)
        if runs:
            normalized[(a, b, delta)] = runs
            outgoing[a].append((b, delta))
    metrics = {"source_classes": len(normalized),
               "source_runs": sum(map(len, normalized.values())),
               "composable_pairs": 0, "closed_triples": 0,
               "join_run_work": 0, "root_intervals": 0,
               "triangle_events": 0, "degree_events": 0,
               "base_neighbor_probes": 0, "base_triangles": 0,
               "full_convolutions": 0, "full_closed_triples": 0,
               "full_event_records": 0, "partial_pair_probes": 0}
    events = defaultdict(lambda: [0, 0])
    for (a, b, delta), runs in normalized.items():
        for interval in runs:
            metrics["degree_events"] += record_shifted_range_events(events, a, interval, 0, period, 0)
            metrics["degree_events"] += record_shifted_range_events(events, b, interval, delta, period, 0)
    total = 0
    if strategy == "hybrid":
        total = compute_hybrid_triangle_events(normalized, period, events, metrics)
    else:
        for (a, b, first_delta), first in normalized.items():
            for c, second_delta in outgoing[b]:
                metrics["composable_pairs"] += 1
                closing_delta = (first_delta + second_delta) % period
                closing = normalized.get((a, c, closing_delta))
                if closing is None:
                    continue
                total += emit_aligned_triangle_intervals(
                    (a, b, c, first_delta, closing_delta),
                    (first, normalized[(b, c, second_delta)], closing), period, events, metrics)
    rows = []
    for a in range(bases):
        degree, triangles = 0, 0
        for g in range(period):
            change = events.get((a, g), (0, 0))
            degree, triangles = degree + change[0], triangles + change[1]
            if not 0 <= triangles <= degree * (degree - 1) // 2:
                raise AssertionError("invalid local triangle count")
            rows.append((degree, triangles))
        ending = events.get((a, period), (0, 0))
        if (degree + ending[0], triangles + ending[1]) != (0, 0):
            raise AssertionError("unclosed event stream")
    if sum(row[1] for row in rows) != 3 * total:
        raise AssertionError("global/local mismatch")
    edge_count = sum(right - left for runs in normalized.values() for left, right in runs)
    if sum(row[0] for row in rows) != 2 * edge_count:
        raise AssertionError("degree/edge mismatch")
    return rows, total, metrics

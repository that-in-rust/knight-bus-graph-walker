"""Exact count-only pair envelopes with branching only at strict tree splits."""

from fractions import Fraction

from probe_overlap_frontier_similarity import validate_overlap_metadata_inputs


def compute_orientation_capacity_bound(a, minimum, capacities, counts, union_counts,
                                        *, branch_cap=1000000, work_cap=10000000, stats=None):
    groups, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    if any(type(value) is not int or value < 0 for value in (a, minimum, branch_cap, work_cap)):
        raise ValueError("sizes and budgets must be nonnegative integers")
    if a < sum(counts) or minimum > capacities[1] or minimum + capacities[1] < populations[1]:
        raise ValueError("inconsistent query or row sizes")
    gaps = [0] * groups
    for node in range(1, groups):
        gaps[node] = capacities[2 * node] + capacities[2 * node + 1] - capacities[node]
        if gaps[node] < 0:
            raise ValueError("parent exceeds sum of child maxima")
    strict = sum(gap > 0 for gap in gaps)
    branches = 1 << strict
    metrics = stats if stats is not None else {}
    metrics.update(strict_nodes=strict, reserved_branches=branches, branches_visited=0,
                   feasible_branches=0, node_visits=0, table_slots_peak=0,
                   reserved_node_visits=branches * (5 * groups - 3))
    if branches > branch_cap:
        raise ValueError("branch budget exceeded before enumeration")
    if metrics["reserved_node_visits"] > work_cap:
        raise ValueError("work budget exceeded before enumeration")
    orientation = [0] * (2 * groups)

    def evaluate_oriented_row_frontier(row, size):
        table = [None] * (2 * groups)
        metrics["table_slots_peak"] = 2 * groups - 1
        for node in range(2 * groups - 1, 0, -1):
            metrics["node_visits"] += 1
            capacity = capacities[node]
            if node >= groups:
                lower = capacity if orientation[node] == row else populations[node] - capacity
                upper, intercept, ceiling = capacity, 0, counts[node - groups]
            else:
                left, right = table[2 * node], table[2 * node + 1]
                lower, upper = left[0] + right[0], left[1] + right[1]
                base = sum(min(child[0] - child[2], child[3]) for child in (left, right))
                ceiling = sum(min(child[1] - child[2], child[3]) for child in (left, right))
                intercept = lower - base
                upper = min(upper, capacity)
                if orientation[node] == row:
                    lower = max(lower, capacity)
            if lower > upper:
                return None
            table[node] = lower, upper, intercept, ceiling
        lower, upper, intercept, ceiling = table[1]
        return min(size - intercept, ceiling) if lower <= size <= upper else None

    best = None
    for assignment in range(branches):
        metrics["branches_visited"] += 1
        bit = 0
        for node in range(1, groups):
            metrics["node_visits"] += 1
            if gaps[node]:
                orientation[2 * node] = (assignment >> bit) & 1
                orientation[2 * node + 1] = 1 - orientation[2 * node]
                bit += 1
            else:
                orientation[2 * node] = orientation[2 * node + 1] = orientation[node]
        major = evaluate_oriented_row_frontier(0, capacities[1])
        if major is None:
            continue
        minor = evaluate_oriented_row_frontier(1, minimum)
        if minor is None:
            continue
        metrics["feasible_branches"] += 1
        for overlap, size in ((major, capacities[1]), (minor, minimum)):
            score = Fraction(overlap, a + size - overlap) if overlap else Fraction(0)
            best = score if best is None else max(best, score)
    if best is None:
        raise ValueError("no complete pair realizes the metadata")
    return best

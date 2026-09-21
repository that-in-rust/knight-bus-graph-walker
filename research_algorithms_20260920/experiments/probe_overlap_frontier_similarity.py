"""Exact two-row summary envelopes; logical budgets are not physical RAM caps."""

from fractions import Fraction


def validate_overlap_metadata_inputs(capacities, counts, union_counts):
    groups = len(counts)
    if not groups or groups & (groups - 1) or len(union_counts) != groups or len(capacities) != 2 * groups:
        raise ValueError("invalid feature tree dimensions")
    if groups > 256:
        raise ValueError("feature group budget exceeded")
    if any(type(value) is not int or value < 0 for values in (capacities, counts, union_counts) for value in values):
        raise ValueError("metadata must be nonnegative integers")
    if capacities[0] or any(q > h for q, h in zip(counts, union_counts)):
        raise ValueError("invalid query or capacity metadata")
    populations = [0] * groups + list(union_counts)
    for node in range(groups - 1, 0, -1):
        populations[node] = populations[2 * node] + populations[2 * node + 1]
    if any(not capacity <= population <= 2 * capacity
           for capacity, population in zip(capacities[1:], populations[1:])):
        raise ValueError("invalid attained maximum")
    return groups, populations


def build_overlap_capacity_frontiers(capacities, counts, union_counts, intersection_cap,
                                     *, state_cap=1000000, work_cap=10000000, stats=None):
    groups, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    if any(type(value) is not int or value < 0 for value in (intersection_cap, state_cap, work_cap)):
        raise ValueError("budgets and intersection must be nonnegative integers")
    metrics = stats if stats is not None else {}
    maxima = [0] + [min(intersection_cap, 2 * capacities[node] - populations[node])
                    for node in range(1, 2 * groups)]
    bounds = [0] + [2 * (maxima[node] + 1) - int(maxima[node] == 2 * capacities[node] - populations[node])
                   for node in range(1, 2 * groups)]
    metrics.update(states=0, transitions=0, reserved_states=sum(bounds),
                   reserved_transitions=sum(bounds[2 * node] * bounds[2 * node + 1] for node in range(1, groups)))
    if metrics["reserved_states"] > state_cap:
        raise ValueError("state budget exceeded before enumeration")
    if metrics["reserved_transitions"] > work_cap:
        raise ValueError("work budget exceeded before enumeration")
    states = [{} for _ in range(2 * groups)]
    for node in range(2 * groups - 1, 0, -1):
        capacity, population = capacities[node], populations[node]
        if node >= groups:
            for shared in range(maxima[node] + 1):
                for size in {capacity, population + shared - capacity}:
                    states[node][shared, size] = min(counts[node - groups], size)
        else:
            for (left_shared, left_size), left_score in states[2 * node].items():
                for (right_shared, right_size), right_score in states[2 * node + 1].items():
                    metrics["transitions"] += 1
                    shared, size = left_shared + right_shared, left_size + right_size
                    if shared <= maxima[node] and size in (capacity, population + shared - capacity):
                        key = shared, size
                        states[node][key] = max(states[node].get(key, -1), left_score + right_score)
        metrics["states"] += len(states[node])
    return states


def compute_overlap_capacity_bound(a, minimum, capacities, counts, union_counts,
                                   *, state_cap=1000000, work_cap=10000000, stats=None):
    _, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    if any(type(value) is not int or value < 0 for value in (a, minimum)):
        raise ValueError("query and row sizes must be nonnegative integers")
    if a < sum(counts) or minimum > capacities[1]:
        raise ValueError("inconsistent query or row sizes")
    intersection = minimum + capacities[1] - populations[1]
    if intersection < 0:
        raise ValueError("inconsistent root intersection")
    metrics = stats if stats is not None else {}
    metrics["intersection"] = intersection
    states = build_overlap_capacity_frontiers(capacities, counts, union_counts, intersection,
                                              state_cap=state_cap, work_cap=work_cap, stats=metrics)
    scores = [Fraction(overlap, a + size - overlap) if overlap else Fraction(0)
              for (shared, size), overlap in states[1].items() if shared == intersection]
    if not scores:
        raise ValueError("no complete pair realizes the metadata")
    return max(scores)


def build_query_threshold_frontiers(capacities, counts, union_counts,
                                    *, state_cap=1000000, work_cap=10000000, stats=None):
    groups, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    if any(type(value) is not int or value < 0 for value in (state_cap, work_cap)):
        raise ValueError("budgets must be nonnegative integers")
    queries = [0] * groups + list(counts)
    gaps = [0] * groups
    for node in range(groups - 1, 0, -1):
        queries[node] = queries[2 * node] + queries[2 * node + 1]
        gaps[node] = capacities[2 * node] + capacities[2 * node + 1] - capacities[node]
        if gaps[node] < 0:
            raise ValueError("parent exceeds sum of child maxima")
    metrics = stats if stats is not None else {}
    reserved = 2 * sum(value + 1 for value in queries[1:])
    operations = 2 * (queries[1] + 1) + 2 * sum(value + 1 for value in queries[groups:])
    for node in range(1, groups):
        left, right = queries[2 * node], queries[2 * node + 1]
        operations += (2 * (left + 1) * (right + 1) if gaps[node] == 0
                       else 4 * (queries[node] + 1) + 2 * (left + right + 2))
    metrics.update(states=0, operations=0, reserved_states=reserved, reserved_operations=operations)
    if reserved > state_cap:
        raise ValueError("state budget exceeded before enumeration")
    if operations > work_cap:
        raise ValueError("work budget exceeded before enumeration")
    states = [None] * (2 * groups)

    def lookup_threshold_query_value(table, shared):
        result = None
        for score, threshold in enumerate(table):
            metrics["operations"] += 1
            if threshold is not None and threshold <= shared:
                result = score
        return result

    for node in range(2 * groups - 1, 0, -1):
        major, minor = [None] * (queries[node] + 1), [None] * (queries[node] + 1)
        states[node] = major, minor
        if node >= groups:
            for score in range(queries[node] + 1):
                metrics["operations"] += 2
                if score <= capacities[node]:
                    major[score] = 0
                    minor[score] = max(0, score - populations[node] + capacities[node])
        elif gaps[node] == 0:
            for role, target in enumerate((major, minor)):
                for left_score, left_cost in enumerate(states[2 * node][role]):
                    for right_score, right_cost in enumerate(states[2 * node + 1][role]):
                        metrics["operations"] += 1
                        if left_cost is None or right_cost is None:
                            continue
                        score, cost = left_score + right_score, left_cost + right_cost
                        if target[score] is None or cost < target[score]:
                            target[score] = cost
        else:
            left, right = 2 * node, 2 * node + 1
            left_limit = 2 * capacities[left] - populations[left] - gaps[node]
            right_limit = 2 * capacities[right] - populations[right] - gaps[node]
            left_values = [lookup_threshold_query_value(table, left_limit) for table in states[left]]
            right_values = [lookup_threshold_query_value(table, right_limit) for table in states[right]]
            branches = (
                ((right_limit, left_limit, states[left][0], right_values[1]),
                 (left_limit, right_limit, states[right][0], left_values[1])),
                ((left_limit, right_limit, states[right][1], left_values[0]),
                 (right_limit, left_limit, states[left][1], right_values[0])),
            )
            for target, options in zip((major, minor), branches):
                for score in range(queries[node] + 1):
                    for fixed, limit, table, fixed_score in options:
                        metrics["operations"] += 1
                        if fixed_score is None:
                            continue
                        needed = max(0, score - fixed_score)
                        if needed >= len(table) or table[needed] is None or table[needed] > limit:
                            continue
                        cost = fixed + table[needed]
                        if target[score] is None or cost < target[score]:
                            target[score] = cost
        metrics["states"] += len(major) + len(minor)
    return states


def compute_query_threshold_bound(a, minimum, capacities, counts, union_counts,
                                   *, state_cap=1000000, work_cap=10000000, stats=None):
    _, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    if any(type(value) is not int or value < 0 for value in (a, minimum)):
        raise ValueError("query and row sizes must be nonnegative integers")
    if a < sum(counts) or minimum > capacities[1]:
        raise ValueError("inconsistent query or row sizes")
    intersection = minimum + capacities[1] - populations[1]
    if intersection < 0:
        raise ValueError("inconsistent root intersection")
    metrics = stats if stats is not None else {}
    metrics["intersection"] = intersection
    states = build_query_threshold_frontiers(capacities, counts, union_counts,
                                             state_cap=state_cap, work_cap=work_cap, stats=metrics)
    scores = []
    for table, size in zip(states[1], (capacities[1], minimum)):
        overlap = None
        for score, threshold in enumerate(table):
            metrics["operations"] += 1
            if threshold is not None and threshold <= intersection:
                overlap = score
        if overlap is not None:
            scores.append(Fraction(overlap, a + size - overlap) if overlap else Fraction(0))
    if not scores:
        raise ValueError("no complete pair realizes the metadata")
    return max(scores)

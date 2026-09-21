"""Preflight exact pair evaluators; refusal is not a pruning certificate."""

from probe_overlap_frontier_similarity import (
    compute_overlap_capacity_bound, compute_query_threshold_bound,
    validate_overlap_metadata_inputs,
)


def estimate_pair_envelope_resources(a, minimum, capacities, counts, union_counts):
    groups, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    if any(type(value) is not int or value < 0 for value in (a, minimum)):
        raise ValueError("invalid query or row sizes")
    if a < sum(counts) or minimum > capacities[1]:
        raise ValueError("inconsistent query or row sizes")
    intersection = minimum + capacities[1] - populations[1]
    if intersection < 0:
        raise ValueError("inconsistent root intersection")
    queries, spans, gaps = [0] * groups + list(counts), [0] * (2 * groups), [0] * groups
    bounds = [0] * (2 * groups)
    for node in range(2 * groups - 1, 0, -1):
        limit = 2 * capacities[node] - populations[node]
        shared = min(intersection, limit)
        bounds[node] = 2 * (shared + 1) - int(shared == limit)
        if node >= groups:
            spans[node] = limit
        else:
            left, right = 2 * node, 2 * node + 1
            queries[node] = queries[left] + queries[right]
            gaps[node] = capacities[left] + capacities[right] - capacities[node]
            if gaps[node] < 0 or gaps[node] > min(spans[left], spans[right]):
                raise ValueError("no complete pair realizes the metadata")
            spans[node] = (spans[left] + spans[right] if gaps[node] == 0
                           else max(spans[left], spans[right]) - gaps[node])
    if capacities[1] - minimum > spans[1]:
        raise ValueError("no complete pair realizes the metadata")
    dense_states = sum(bounds)
    dense_pairs = sum(bounds[2 * node] * bounds[2 * node + 1] for node in range(1, groups))
    threshold_states = 2 * sum(value + 1 for value in queries[1:])
    threshold_work = 2 * (queries[1] + 1) + 2 * sum(value + 1 for value in queries[groups:])
    for node in range(1, groups):
        left, right = queries[2 * node], queries[2 * node + 1]
        threshold_work += (2 * (left + 1) * (right + 1) if gaps[node] == 0
                           else 4 * (queries[node] + 1) + 2 * (left + right + 2))
    return {"dense": (dense_states, 2 * dense_states + dense_pairs),
            "threshold": (threshold_states, threshold_work)}


def solve_admitted_pair_envelope(a, minimum, capacities, counts, union_counts,
                                 *, strategy="auto", state_cap=4096, work_cap=65536):
    if strategy not in ("auto", "dense", "threshold"):
        raise ValueError("unknown pair solver strategy")
    if any(type(value) is not int or value < 0 for value in (state_cap, work_cap)):
        raise ValueError("pair budgets must be nonnegative integers")
    reservations = estimate_pair_envelope_resources(a, minimum, capacities, counts, union_counts)
    choices = [(work, cells, name) for name, (cells, work) in reservations.items()
               if cells <= state_cap and work <= work_cap and strategy in ("auto", name)]
    metrics = {"solver": "refused", "states": 0, "work": 0, "reserved_states": 0,
               "reserved_work": 0, "reservations": reservations}
    if not choices:
        return None, metrics
    work, cells, name = min(choices)
    solver = compute_overlap_capacity_bound if name == "dense" else compute_query_threshold_bound
    local = {}
    result = solver(a, minimum, capacities, counts, union_counts,
                    state_cap=cells, work_cap=work, stats=local)
    actual = 2 * local["states"] + local["transitions"] if name == "dense" else local["operations"]
    assert local["states"] <= cells and actual <= work
    metrics.update(solver=name, states=local["states"], work=actual,
                   reserved_states=cells, reserved_work=work)
    return result, metrics

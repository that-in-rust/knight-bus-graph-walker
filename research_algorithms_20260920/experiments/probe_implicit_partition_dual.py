"""Exact partition-structured dual evaluation without enumerating vertex pairs."""

from collections import defaultdict
from fractions import Fraction as F


def accumulate_sorted_group_hinges(indices, potentials, thresholds, active, metrics):
    prefix = [F()]
    for vertex in indices:
        prefix.append(prefix[-1] + potentials[vertex])
        metrics["prefix_additions"] += 1
    total = F()
    for threshold, multiplier in thresholds:
        pointer = len(indices)
        incident = F()
        for vertex in indices:
            limit = threshold - potentials[vertex]
            while pointer and potentials[indices[pointer - 1]] >= limit:
                pointer -= 1
                metrics["pointer_decrements"] += 1
            count, contribution = pointer, pointer * limit - prefix[pointer]
            if 2 * potentials[vertex] < threshold:
                count -= 1
                contribution -= threshold - 2 * potentials[vertex]
            incident += contribution
            active[vertex] += multiplier * count
            metrics["threshold_visits"] += 1
        total += multiplier * incident / 2
    return total


def evaluate_implicit_partition_dual(residual_degrees, labels_a, labels_b, cap, potentials):
    """Feasible source degrees are a caller precondition, not solved here."""
    degrees, y, cap = tuple(map(F, residual_degrees)), tuple(map(F, potentials)), F(cap)
    size = len(degrees)
    if len(labels_a) != size or len(labels_b) != size or len(y) != size:
        raise ValueError("one degree, potential and each partition label per vertex required")
    if cap < 0 or any(r < 0 for r in degrees):
        raise ValueError("nonnegative degrees and cap required")
    ordered = sorted(range(size), key=y.__getitem__)
    by_a, by_b, cells = defaultdict(list), defaultdict(list), defaultdict(list)
    for vertex in ordered:
        by_a[labels_a[vertex]].append(vertex)
        by_b[labels_b[vertex]].append(vertex)
        cells[labels_a[vertex], labels_b[vertex]].append(vertex)
    metrics = {"membership_records": 4 * size, "implicit_pairs": size * (size - 1) // 2,
               "prefix_additions": 0, "threshold_visits": 0, "pointer_decrements": 0,
               "groups": 1 + len(by_a) + len(by_b) + len(cells)}
    active = [0] * size
    edge_term = accumulate_sorted_group_hinges(ordered, y, ((0, 1),), active, metrics)
    for indices in by_a.values():
        edge_term += accumulate_sorted_group_hinges(indices, y, ((1, 1), (0, -1)), active, metrics)
    for indices in by_b.values():
        edge_term += accumulate_sorted_group_hinges(indices, y, ((-1, 1), (0, -1)), active, metrics)
    for indices in cells.values():
        edge_term += accumulate_sorted_group_hinges(indices, y, ((1, -1), (-1, -1), (0, 2)), active, metrics)
    if any(count < 0 or count >= size for count in active) or edge_term < 0:
        raise AssertionError("invalid corrected active degrees or hinge sum")
    value = sum((r * potential for r, potential in zip(degrees, y)), F()) + cap * edge_term
    gradient = tuple(r - cap * count for r, count in zip(degrees, active))
    return {"upper": value, "gradient": gradient, "metrics": metrics}


def search_bounded_partition_dual(residual_degrees, labels_a, labels_b, cap, *, rounds=12, step=F(1, 2)):
    """Fixed-round heuristic; every retained value is a valid dual upper."""
    step = F(step)
    if type(rounds) is not int or rounds < 0 or step <= 0:
        raise ValueError("nonnegative integral rounds and positive step required")
    y = (F(),) * len(residual_degrees)
    best, best_y, trace, visits = None, y, [], 0
    for iteration in range(rounds + 1):
        result = evaluate_implicit_partition_dual(residual_degrees, labels_a, labels_b, cap, y)
        trace.append(result["upper"])
        visits += result["metrics"]["threshold_visits"]
        if best is None or result["upper"] < best:
            best, best_y = result["upper"], y
        if iteration < rounds:
            norm = max((abs(g) for g in result["gradient"]), default=F())
            if norm:
                rate = step / ((iteration + 1) * norm)
                y = tuple(p - rate * g for p, g in zip(y, result["gradient"]))
    return {"upper": best, "potentials": best_y, "trace": tuple(trace), "threshold_visits": visits}


def solve_projected_partition_dual(residual_degrees, labels_a, labels_b, cap, *, root_rounds):
    """For feasible K>=3, bound error to the capped maximum, not to each graph."""
    degrees, cap = tuple(map(F, residual_degrees)), F(cap)
    size = len(degrees)
    if size < 3 or type(root_rounds) is not int or root_rounds <= 0:
        raise ValueError("K>=3 and positive integral root_rounds required")
    if len(labels_a) != size or len(labels_b) != size or cap < 0:
        raise ValueError("one label per vertex and nonnegative cap required")
    if any(r < 0 or r > cap * (size - 1) for r in degrees):
        raise ValueError("degrees violate necessary cap feasibility")
    y = (F(),) * size
    if cap == 0:
        return {"upper": F(), "potentials": y, "optimal_gap_bound": F(), "evaluations": 0, "threshold_visits": 0}
    evaluations = root_rounds * root_rounds
    step = F(size) / (cap * (size - 1) * root_rounds)
    best, best_y, visits = None, y, 0
    for iteration in range(evaluations):
        result = evaluate_implicit_partition_dual(degrees, labels_a, labels_b, cap, y)
        visits += result["metrics"]["threshold_visits"]
        if best is None or result["upper"] < best:
            best, best_y = result["upper"], y
        if iteration + 1 < evaluations:
            y = tuple(min(F(size), max(F(-size), p - step * g)) for p, g in zip(y, result["gradient"]))
    return {"upper": best, "potentials": best_y, "optimal_gap_bound": cap * F(size * size * (size - 1), root_rounds),
            "evaluations": evaluations, "threshold_visits": visits}

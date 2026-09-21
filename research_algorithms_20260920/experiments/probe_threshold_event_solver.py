"""Proposal-free, exact two-label local moving with explicit scope handoff."""

from dataclasses import dataclass
from fractions import Fraction as F

from probe_single_flip_trace_verifier import evaluate_exact_visit_gains, validate_exact_graph_source
from probe_threshold_trace_control import RankStayThresholdTree, compute_vertex_stay_thresholds


def find_first_threshold_violation(tree, first, volume):
    if not 0 <= first <= tree.n:
        raise ValueError("search start outside visit ranks")
    stack, visited = [(1, 0, tree.size)], 0
    while stack:
        node, low, high = stack.pop()
        visited += 1
        if high <= first or low >= tree.n:
            continue
        lower, upper = tree.tree[node]
        if (lower is None or volume >= lower) and (upper is None or volume <= upper):
            continue
        if high - low == 1:
            return low, visited
        middle = (low + high) // 2
        stack.append((2 * node + 1, middle, high))
        stack.append((2 * node, low, middle))
    return None, visited


@dataclass
class ThresholdEventSolverResult:
    status: str
    trace: tuple
    labels: dict
    sweeps: int
    next_visit: tuple | None
    metrics: dict


def run_threshold_event_solver(graph, initial, gamma, *, max_moves):
    if type(max_moves) is not int or max_moves < 0:
        raise ValueError("max_moves must be a nonnegative integer")
    degrees = validate_exact_graph_source(graph, initial, gamma)
    gamma, total = F(gamma), sum(degrees.values())
    vertices, labels = sorted(graph), dict(initial)
    ranks = {u: i for i, u in enumerate(vertices)}
    counts = {label: sum(value == label for value in labels.values()) for label in ("P", "Q")}
    affinity = {u: sum((w for v, w in graph[u].items() if labels[v] == "Q"), F(0)) for u in vertices}
    volume = sum(degrees[u] for u in vertices if labels[u] == "Q")
    tree = RankStayThresholdTree([compute_vertex_stay_thresholds(total, gamma, degrees[u], affinity[u], labels[u])
                                  for u in vertices])
    metrics = {"vertices": len(graph), "edges": sum(map(len, graph.values())) // 2,
               "source_directed_entries": sum(map(len, graph.values())), "event_searches": 0,
               "search_tree_nodes": 0, "covered_visits": 0, "accepted_moves": 0,
               "neighbor_affinity_updates": 0, "threshold_tree_cells": len(tree.tree)}
    sweep, cursor, changed, trace = 1, 0, False, []

    def capture_solver_execution_state(status, pending=None):
        metrics.update(threshold_updates=tree.update_count, threshold_update_nodes=tree.update_nodes)
        return ThresholdEventSolverResult(status, tuple(trace), dict(labels), sweep, pending, dict(metrics))

    while True:
        rank, visits = find_first_threshold_violation(tree, cursor, volume)
        metrics["event_searches"] += 1
        metrics["search_tree_nodes"] += visits
        if rank is None:
            metrics["covered_visits"] += len(vertices) - cursor
            if not changed:
                return capture_solver_execution_state("complete")
            sweep, cursor, changed = sweep + 1, 0, False
            continue
        metrics["covered_visits"] += rank - cursor
        u, degree = vertices[rank], degrees[vertices[rank]]
        switch, fresh = evaluate_exact_visit_gains(total, gamma, degree, affinity[u], volume, labels[u])
        if max(switch, fresh) <= 0:
            raise AssertionError("threshold search returned a nonprofitable vertex")
        pending = sweep, u
        if fresh > switch:
            return capture_solver_execution_state("fresh_destination", pending)
        if counts[labels[u]] == 1:
            return capture_solver_execution_state("empty_community", pending)
        if len(trace) == max_moves:
            return capture_solver_execution_state("move_budget", pending)
        before = labels[u]
        destination = "Q" if before == "P" else "P"
        labels[u] = destination
        counts[before] -= 1
        counts[destination] += 1
        sign = 1 if destination == "Q" else -1
        volume += sign * degree
        tree.update_vertex_stay_threshold(rank, compute_vertex_stay_thresholds(total, gamma, degree, affinity[u], destination))
        for v, weight in graph[u].items():
            affinity[v] += sign * weight
            metrics["neighbor_affinity_updates"] += 1
            tree.update_vertex_stay_threshold(ranks[v], compute_vertex_stay_thresholds(total, gamma, degrees[v], affinity[v], labels[v]))
        trace.append((sweep, u, destination, switch))
        metrics["accepted_moves"] += 1
        metrics["covered_visits"] += 1
        cursor, changed = rank + 1, True

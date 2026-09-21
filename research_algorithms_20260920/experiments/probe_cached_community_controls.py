"""Cached serial controls; exact two-label parity and general-label handoff."""

import heapq
from fractions import Fraction as F

from probe_single_flip_trace_verifier import validate_exact_graph_source
from probe_threshold_event_solver import ThresholdEventSolverResult


def validate_scalar_move_budget(max_moves):
    if type(max_moves) is not int or max_moves < 0:
        raise ValueError("max_moves must be a nonnegative integer")


def run_cached_scalar_solver(graph, initial, gamma, *, max_moves):
    validate_scalar_move_budget(max_moves)
    degrees = validate_exact_graph_source(graph, initial, gamma)
    labels, vertices, total = dict(initial), sorted(graph), sum(degrees.values())
    counts = {label: sum(value == label for value in labels.values()) for label in ("P", "Q")}
    affinity = {u: sum((w for v, w in graph[u].items() if labels[v] == "Q"), F(0)) for u in graph}
    q_volume = sum(degrees[u] for u in graph if labels[u] == "Q")
    trace, sweep = [], 1
    metrics = {"vertices": len(graph), "source_directed_entries": sum(map(len, graph.values())),
               "scalar_visits": 0, "accepted_moves": 0, "neighbor_affinity_updates": 0}

    def capture_cached_scalar_state(status, pending=None):
        return ThresholdEventSolverResult(status, tuple(trace), dict(labels), sweep, pending, dict(metrics))

    while True:
        changed = False
        for u in vertices:
            metrics["scalar_visits"] += 1
            degree = degrees[u]
            if labels[u] == "P":
                current = total * (degree - affinity[u]) - gamma * degree * (total - q_volume - degree)
                other = total * affinity[u] - gamma * degree * q_volume
            else:
                current = total * affinity[u] - gamma * degree * (q_volume - degree)
                other = total * (degree - affinity[u]) - gamma * degree * (total - q_volume)
            gain, fresh = other - current, -current
            if max(gain, fresh) <= 0:
                continue
            if fresh > gain:
                return capture_cached_scalar_state("fresh_destination", (sweep, u))
            if counts[labels[u]] == 1:
                return capture_cached_scalar_state("empty_community", (sweep, u))
            if len(trace) == max_moves:
                return capture_cached_scalar_state("move_budget", (sweep, u))
            source = labels[u]
            destination = "Q" if source == "P" else "P"
            labels[u] = destination
            counts[source] -= 1
            counts[destination] += 1
            sign = 1 if destination == "Q" else -1
            q_volume += sign * degree
            for v, weight in graph[u].items():
                affinity[v] += sign * weight
                metrics["neighbor_affinity_updates"] += 1
            trace.append((sweep, u, destination, gain))
            metrics["accepted_moves"] += 1
            changed = True
        if not changed:
            return capture_cached_scalar_state("complete")
        sweep += 1


def validate_general_graph_source(graph, initial, gamma):
    if not isinstance(gamma, (int, F)) or isinstance(gamma, bool) or gamma <= 0:
        raise ValueError("positive rational resolution required")
    if (not graph or set(initial) != set(graph) or any(type(u) is not int for u in graph)
            or any(not isinstance(label, str) or not label for label in initial.values())):
        raise ValueError("integer vertices and nonempty string labels required")
    for u, row in graph.items():
        for v, w in row.items():
            if (type(v) is not int or u == v or v not in graph or not isinstance(w, (int, F))
                    or isinstance(w, bool) or w <= 0 or graph[v].get(u) != w):
                raise ValueError("simple symmetric positive rational source required")
    degrees = {u: sum(row.values(), F(0)) for u, row in graph.items()}
    if not sum(degrees.values()):
        raise ValueError("positive total degree required")
    return degrees


def run_general_scalar_solver(graph, initial, gamma, *, max_moves, start_visit=None):
    validate_scalar_move_budget(max_moves)
    degrees = validate_general_graph_source(graph, initial, gamma)
    labels, vertices, total = dict(initial), sorted(graph), sum(degrees.values())
    if start_visit is not None and (not isinstance(start_visit, tuple) or len(start_visit) != 2
                                   or type(start_visit[0]) is not int or start_visit[0] < 1
                                   or type(start_visit[1]) is not int or start_visit[1] not in graph):
        raise ValueError("invalid pending positive visit")
    volumes, counts = {}, {}
    for u, label in labels.items():
        counts[label] = counts.get(label, 0) + 1
        volumes[label] = volumes.get(label, F(0)) + degrees[u]
    zero_labels = {label for label, volume in volumes.items() if not volume}
    zero_heap = list(zero_labels)
    heapq.heapify(zero_heap)
    sweep, first = start_visit if start_visit is not None else (1, vertices[0])
    resumed = start_visit is not None
    trace = []
    metrics = {"vertices": len(graph), "source_directed_entries": sum(map(len, graph.values())),
               "scalar_visits": 0, "visited_adjacency_records": 0, "candidate_scores": 0,
               "accepted_moves": 0, "max_active_communities": len(counts), "max_zero_heap_records": len(zero_heap)}

    def capture_general_scalar_state(status, pending=None):
        return ThresholdEventSolverResult(status, tuple(trace), dict(labels), sweep, pending, dict(metrics))

    while True:
        changed = False
        for u in vertices:
            if u < first:
                continue
            metrics["scalar_visits"] += 1
            degree, source = degrees[u], labels[u]
            affinity = {}
            for v, weight in graph[u].items():
                label = labels[v]
                affinity[label] = affinity.get(label, F(0)) + weight
                metrics["visited_adjacency_records"] += 1
            while zero_heap and zero_heap[0] not in zero_labels:
                heapq.heappop(zero_heap)
            fresh = f"fresh:{sweep}:{u}"
            if fresh in counts:
                raise ValueError("reserved fresh label collides with initial state")
            candidates = set(affinity) | {source, fresh}
            if zero_heap:
                candidates.add(zero_heap[0])
            scores = {label: total * affinity.get(label, 0) - gamma * degree *
                      (volumes.get(label, 0) - (degree if label == source else 0)) for label in candidates}
            metrics["candidate_scores"] += len(scores)
            best = min(scores, key=lambda label: (-scores[label], label))
            gain = scores[best] - scores[source]
            if resumed:
                if gain <= 0:
                    raise ValueError("resume requires the stated pending visit to be positive")
                resumed = False
            if gain <= 0:
                continue
            if len(trace) == max_moves:
                return capture_general_scalar_state("move_budget", (sweep, u))
            counts[source] -= 1
            volumes[source] -= degree
            if counts[source] == 0:
                del counts[source]
                del volumes[source]
                zero_labels.discard(source)
            elif volumes[source] == 0:
                zero_labels.add(source)
                heapq.heappush(zero_heap, source)
            counts[best] = counts.get(best, 0) + 1
            volumes[best] = volumes.get(best, F(0)) + degree
            zero_labels.discard(best)
            labels[u] = best
            trace.append((sweep, u, best, gain))
            metrics["accepted_moves"] += 1
            metrics["max_active_communities"] = max(metrics["max_active_communities"], len(counts))
            metrics["max_zero_heap_records"] = max(metrics["max_zero_heap_records"], len(zero_heap))
            changed = True
        if not changed:
            return capture_general_scalar_state("complete")
        sweep, first = sweep + 1, vertices[0]

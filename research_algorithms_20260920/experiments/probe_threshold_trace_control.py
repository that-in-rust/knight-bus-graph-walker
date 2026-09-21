"""Chronological rank-threshold control for the same single-flip contract.

Established range aggregation, not an independent novelty claim. Source
validation/result types are shared; visit checking and scheduling are separate.
"""

from fractions import Fraction as F

from probe_single_flip_trace_verifier import SingleFlipVerificationResult, validate_exact_graph_source


def merge_vertex_stay_thresholds(left, right):
    lower = [value for value in (left[0], right[0]) if value is not None]
    upper = [value for value in (left[1], right[1]) if value is not None]
    return max(lower) if lower else None, min(upper) if upper else None


def compute_vertex_stay_thresholds(total, gamma, degree, affinity, label):
    if not degree:
        return None, None
    scale = gamma * degree
    if label == "P":
        switch = (total * (2 * affinity - degree) + scale * (total - degree)) / (2 * scale)
        fresh = total - degree - total * (degree - affinity) / scale
        return max(switch, fresh), None
    switch = (scale * (total + degree) - total * (degree - 2 * affinity)) / (2 * scale)
    fresh = degree + total * affinity / scale
    return None, min(switch, fresh)


class RankStayThresholdTree:
    def __init__(self, values):
        self.n = len(values)
        self.size = 1 << (self.n - 1).bit_length()
        self.tree = [(None, None)] * (2 * self.size)
        self.tree[self.size:self.size + self.n] = values
        for node in range(self.size - 1, 0, -1):
            self.tree[node] = merge_vertex_stay_thresholds(self.tree[2 * node], self.tree[2 * node + 1])
        self.query_count = self.query_nodes = self.update_count = self.update_nodes = 0

    def update_vertex_stay_threshold(self, rank, value):
        if not 0 <= rank < self.n:
            raise ValueError("rank outside threshold tree")
        self.update_count += 1
        node = self.size + rank
        self.tree[node] = value
        self.update_nodes += 1
        node //= 2
        while node:
            self.tree[node] = merge_vertex_stay_thresholds(self.tree[2 * node], self.tree[2 * node + 1])
            self.update_nodes += 1
            node //= 2

    def query_rank_interval_thresholds(self, low, high):
        if not 0 <= low <= high < self.n:
            raise ValueError("rank range outside threshold tree")
        self.query_count += 1
        lo, hi, result = self.size + low, self.size + high + 1, (None, None)
        while lo < hi:
            if lo & 1:
                result = merge_vertex_stay_thresholds(result, self.tree[lo])
                self.query_nodes += 1
                lo += 1
            if hi & 1:
                hi -= 1
                result = merge_vertex_stay_thresholds(result, self.tree[hi])
                self.query_nodes += 1
            lo //= 2
            hi //= 2
        return result


def verify_single_flip_trace(graph, initial, gamma, proposal, terminal_sweep):
    degrees = validate_exact_graph_source(graph, initial, gamma)
    gamma = F(gamma)
    proposal, labels = list(proposal), dict(initial)
    n, total = len(graph), sum(degrees.values())
    metrics = {"vertices": n, "edges": sum(map(len, graph.values())) // 2,
               "source_directed_entries": sum(map(len, graph.values())),
               "proposed_moves": len(proposal), "skipped_rank_intervals": 0,
               "covered_visits": 0, "neighbor_affinity_updates": 0, "accepted_point_checks": 0}
    tree = None

    def snapshot_current_control_result(accepted, reason, trace=()):
        if tree is not None:
            metrics.update(threshold_tree_cells=len(tree.tree), threshold_queries=tree.query_count,
                           threshold_query_nodes=tree.query_nodes, threshold_updates=tree.update_count,
                           threshold_update_nodes=tree.update_nodes)
        return SingleFlipVerificationResult(accepted, reason, tuple(trace), dict(labels) if accepted else {}, dict(metrics))

    if type(terminal_sweep) is not int or not 1 <= terminal_sweep <= len(proposal) + 1:
        return snapshot_current_control_result(False, "invalid terminal sweep")
    seen, occupied, previous = set(), set(), None
    counts = {label: sum(value == label for value in labels.values()) for label in ("P", "Q")}
    for event in proposal:
        if not isinstance(event, (tuple, list)) or len(event) != 3:
            return snapshot_current_control_result(False, "invalid event record")
        sweep, u, destination = event
        if (type(sweep) is not int or type(u) is not int or u not in graph or u in seen or
                not 1 <= sweep < terminal_sweep or destination != ("Q" if initial[u] == "P" else "P") or
                (previous is not None and (sweep, u) <= previous)):
            return snapshot_current_control_result(False, "invalid single-flip event order or destination")
        counts[initial[u]] -= 1
        counts[destination] += 1
        if not all(counts.values()):
            return snapshot_current_control_result(False, "proposal empties a community")
        seen.add(u)
        occupied.add(sweep)
        previous = sweep, u
    if occupied != set(range(1, terminal_sweep)):
        return snapshot_current_control_result(False, "proposal crosses an early no-move sweep")

    vertices = sorted(graph)
    ranks = {u: i for i, u in enumerate(vertices)}
    affinity = {u: sum((w for v, w in graph[u].items() if labels[v] == "Q"), F(0)) for u in vertices}
    volume = sum(degrees[u] for u in vertices if labels[u] == "Q")
    tree = RankStayThresholdTree([compute_vertex_stay_thresholds(total, gamma, degrees[u], affinity[u], labels[u])
                                  for u in vertices])
    metrics["naive_visit_count"] = n * terminal_sweep

    def check_skipped_rank_interval(low, high):
        if low > high:
            return True
        lower, upper = tree.query_rank_interval_thresholds(low, high)
        metrics["skipped_rank_intervals"] += 1
        metrics["covered_visits"] += high - low + 1
        return (lower is None or volume >= lower) and (upper is None or volume <= upper)

    cursor, trace = 0, []
    for sweep in range(1, terminal_sweep + 1):
        first = 0
        while cursor < len(proposal) and proposal[cursor][0] == sweep:
            _, u, destination = proposal[cursor]
            rank, degree = ranks[u], degrees[u]
            if not check_skipped_rank_interval(first, rank - 1):
                return snapshot_current_control_result(False, f"profitable skipped visit in sweep {sweep}")
            if labels[u] == "P":
                stay = total * (degree - affinity[u]) - gamma * degree * (total - volume - degree)
                other = total * affinity[u] - gamma * degree * volume
            else:
                stay = total * affinity[u] - gamma * degree * (volume - degree)
                other = total * (degree - affinity[u]) - gamma * degree * (total - volume)
            gain, fresh = other - stay, -stay
            metrics["accepted_point_checks"] += 1
            metrics["covered_visits"] += 1
            if gain <= 0 or gain < fresh:
                return snapshot_current_control_result(False, f"invalid accepted move at ({sweep}, {u})")
            trace.append((sweep, u, destination, gain))
            labels[u] = destination
            sign = 1 if destination == "Q" else -1
            volume += sign * degree
            tree.update_vertex_stay_threshold(rank, compute_vertex_stay_thresholds(total, gamma, degree, affinity[u], destination))
            for v, weight in graph[u].items():
                affinity[v] += sign * weight
                metrics["neighbor_affinity_updates"] += 1
                tree.update_vertex_stay_threshold(ranks[v], compute_vertex_stay_thresholds(total, gamma, degrees[v], affinity[v], labels[v]))
            first = rank + 1
            cursor += 1
        if not check_skipped_rank_interval(first, n - 1):
            return snapshot_current_control_result(False, f"profitable skipped visit in sweep {sweep}")
    return snapshot_current_control_result(True, "exact complete single-flip trace", trace)

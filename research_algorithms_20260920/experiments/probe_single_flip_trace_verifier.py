"""Resident exact verifier for a supplied, single-flip, two-label trace.

No general Louvain/Leiden parity, bounded-RAM preparation, or speed claim.
Rational arithmetic and variable-width identifiers remain fully charged.
"""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from fractions import Fraction as F


def combine_exact_volume_extrema(left, right):
    if left is None:
        return right
    if right is None:
        return left
    return min(left[0], right[0]), max(left[1], right[1])


class SweepVolumeRangeTree:
    def __init__(self, entries):
        self.sweeps = [s for s, _ in entries]
        self.size = 1 << (len(entries) - 1).bit_length()
        self.tree = [None] * (2 * self.size)
        for i, (_, value) in enumerate(entries):
            self.tree[self.size + i] = (value, value)
        for i in range(self.size - 1, 0, -1):
            self.tree[i] = combine_exact_volume_extrema(self.tree[2 * i], self.tree[2 * i + 1])

    def query_sweep_interval_extrema(self, low, high):
        lo = bisect_left(self.sweeps, low) + self.size
        hi = bisect_right(self.sweeps, high) + self.size
        result, reads = None, 0
        while lo < hi:
            if lo & 1:
                result = combine_exact_volume_extrema(result, self.tree[lo])
                reads += 1
                lo += 1
            if hi & 1:
                hi -= 1
                result = combine_exact_volume_extrema(result, self.tree[hi])
                reads += 1
            lo //= 2
            hi //= 2
        return result, reads


class SampledVolumeRangeIndex:
    def __init__(self, n, sweeps, pieces):
        self.n, self.sweeps = n, sweeps
        self.size = 1 << (n - 1).bit_length()
        self.piece_count = self.entry_count = 0
        self.query_count = self.path_nodes = self.range_nodes = 0
        entries = {}
        for sweep, low, high, volume in pieces:
            self.piece_count += 1
            lo, hi = low + self.size, high + self.size + 1
            while lo < hi:
                if lo & 1:
                    entries.setdefault(lo, []).append((sweep, volume))
                    self.entry_count += 1
                    lo += 1
                if hi & 1:
                    hi -= 1
                    entries.setdefault(hi, []).append((sweep, volume))
                    self.entry_count += 1
                lo //= 2
                hi //= 2
        self.nodes = {node: SweepVolumeRangeTree(rows) for node, rows in entries.items()}
        self.range_tree_cells = sum(len(tree.tree) for tree in self.nodes.values())

    def query_sampled_volume_extrema(self, rank, low, high):
        if not (0 <= rank < self.n and 1 <= low <= high <= self.sweeps):
            raise ValueError("sampled range outside visit grid")
        self.query_count += 1
        node, result = self.size + rank, None
        while node:
            self.path_nodes += 1
            tree = self.nodes.get(node)
            if tree is not None:
                found, reads = tree.query_sweep_interval_extrema(low, high)
                result = combine_exact_volume_extrema(result, found)
                self.range_nodes += reads
            node //= 2
        if result is None:
            raise AssertionError("constructed visit grid has uncovered samples")
        return result


def build_sampled_volume_index(n, initial_q, events, sweeps):
    if type(n) is not int or n < 1 or type(sweeps) is not int or sweeps < 1:
        raise ValueError("invalid visit grid dimensions")
    events = list(events)
    previous = (0, -1)
    for sweep, rank, _ in events:
        if not (1 <= sweep < sweeps and 0 <= rank < n and (sweep, rank) > previous):
            raise ValueError("volume events must be ordered unique nonterminal visits")
        previous = sweep, rank

    def emit_constant_volume_pieces():
        cursor, volume = 0, F(initial_q)
        for sweep in range(1, sweeps + 1):
            first = 0
            while cursor < len(events) and events[cursor][0] == sweep:
                _, rank, delta = events[cursor]
                yield sweep, first, rank, volume
                volume += delta
                first = rank + 1
                cursor += 1
            if first < n:
                yield sweep, first, n - 1, volume

    return SampledVolumeRangeIndex(n, sweeps, emit_constant_volume_pieces())


@dataclass
class SingleFlipVerificationResult:
    accepted: bool
    reason: str
    trace: tuple
    labels: dict
    metrics: dict


def validate_exact_graph_source(graph, initial, gamma):
    if not isinstance(gamma, (int, F)) or isinstance(gamma, bool) or gamma <= 0:
        raise ValueError("resolution must be a positive exact rational")
    if not graph or any(type(u) is not int for u in graph):
        raise ValueError("source requires original integer identifiers")
    if set(initial) != set(graph) or set(initial.values()) != {"P", "Q"}:
        raise ValueError("initial partition must contain exactly nonempty P and Q")
    for u, row in graph.items():
        for v, weight in row.items():
            if (type(v) is not int or v not in graph or u == v or
                    not isinstance(weight, (int, F)) or isinstance(weight, bool) or
                    weight <= 0 or graph[v].get(u) != weight):
                raise ValueError("source must be symmetric loopless positive rational graph")
    degrees = {u: sum(row.values(), F(0)) for u, row in graph.items()}
    if sum(degrees.values()) <= 0:
        raise ValueError("modularity requires positive total degree")
    return degrees


def evaluate_exact_visit_gains(total, gamma, degree, affinity, volume, label):
    if label == "P":
        switch = total * (2 * affinity - degree) - gamma * degree * (2 * volume - total + degree)
        fresh = -total * (degree - affinity) + gamma * degree * (total - volume - degree)
    else:
        switch = total * (degree - 2 * affinity) - gamma * degree * (total - 2 * volume + degree)
        fresh = -total * affinity + gamma * degree * (volume - degree)
    return switch, fresh


def verify_single_flip_trace(graph, initial, gamma, proposal, terminal_sweep):
    degrees = validate_exact_graph_source(graph, initial, gamma)
    proposal = list(proposal)
    n, total = len(graph), sum(degrees.values())
    metrics = {"vertices": n, "edges": sum(map(len, graph.values())) // 2,
               "proposed_moves": len(proposal), "source_directed_entries": sum(map(len, graph.values()))}

    def reject_current_trace_proposal(reason):
        return SingleFlipVerificationResult(False, reason, (), {}, dict(metrics))

    if type(terminal_sweep) is not int or not 1 <= terminal_sweep <= len(proposal) + 1:
        return reject_current_trace_proposal("invalid terminal sweep")
    own, occupied, previous = {}, set(), None
    counts = {label: sum(value == label for value in initial.values()) for label in ("P", "Q")}
    for event in proposal:
        if not isinstance(event, (tuple, list)) or len(event) != 3:
            return reject_current_trace_proposal("invalid event record")
        sweep, u, destination = event
        if (type(sweep) is not int or type(u) is not int or u not in graph or
                not 1 <= sweep < terminal_sweep or u in own or
                destination != ("Q" if initial[u] == "P" else "P") or
                (previous is not None and (sweep, u) <= previous)):
            return reject_current_trace_proposal("invalid single-flip event order or destination")
        own[u] = sweep
        occupied.add(sweep)
        previous = sweep, u
        counts[initial[u]] -= 1
        counts[destination] += 1
        if not all(counts.values()):
            return reject_current_trace_proposal("proposal empties a community")
    if occupied != set(range(1, terminal_sweep)):
        return reject_current_trace_proposal("proposal crosses an early no-move sweep")

    vertices = sorted(graph)
    ranks = {u: i for i, u in enumerate(vertices)}
    initial_q = sum(degrees[u] for u in vertices if initial[u] == "Q")
    volume_events = [(s, ranks[u], degrees[u] if d == "Q" else -degrees[u]) for s, u, d in proposal]
    index = build_sampled_volume_index(n, initial_q, volume_events, terminal_sweep)
    updates = {u: {} for u in vertices}
    raw_updates = 0
    for sweep, v, destination in proposal:
        sign = 1 if destination == "Q" else -1
        for u, weight in graph[v].items():
            effective = sweep if v < u else sweep + 1
            row = updates[u]
            row[effective] = row.get(effective, F(0)) + sign * weight
            raw_updates += 1
    metrics.update(volume_pieces=index.piece_count, volume_index_entries=index.entry_count,
                   volume_range_tree_cells=index.range_tree_cells,
                   affinity_update_records=raw_updates,
                   grouped_affinity_records=sum(map(len, updates.values())),
                   naive_visit_count=n * terminal_sweep, decision_intervals=0, covered_visits=0,
                   skipped_intervals=0, accepted_point_checks=0)
    gains = {}
    for u in vertices:
        affinity = sum(w for v, w in graph[u].items() if initial[v] == "Q")
        changes, own_sweep = updates[u], own.get(u)
        cuts = {1, terminal_sweep + 1} | set(changes)
        if own_sweep is not None:
            cuts.update((own_sweep, own_sweep + 1))
        ordered = sorted(cuts)
        for low, next_low in zip(ordered, ordered[1:]):
            affinity += changes.get(low, 0)
            high = next_low - 1
            label = initial[u]
            if own_sweep is not None and low > own_sweep:
                label = "Q" if label == "P" else "P"
            minimum, maximum = index.query_sampled_volume_extrema(ranks[u], low, high)
            metrics["decision_intervals"] += 1
            metrics["covered_visits"] += high - low + 1
            volume = minimum if label == "P" else maximum
            switch, fresh = evaluate_exact_visit_gains(total, gamma, degrees[u], affinity, volume, label)
            if low == own_sweep:
                metrics["accepted_point_checks"] += 1
                if low != high or minimum != maximum:
                    raise AssertionError("accepted visit was not isolated")
                if switch <= 0 or switch < fresh:
                    return reject_current_trace_proposal(f"invalid accepted move at ({low}, {u})")
                gains[u] = switch
            else:
                metrics["skipped_intervals"] += 1
                if switch > 0 or fresh > 0:
                    return reject_current_trace_proposal(f"profitable skipped visit for {u} in [{low},{high}]")
    metrics.update(range_queries=index.query_count, rank_path_nodes=index.path_nodes,
                   range_tree_nodes=index.range_nodes)
    final = dict(initial)
    for _, u, destination in proposal:
        final[u] = destination
    trace = tuple((s, u, destination, gains[u]) for s, u, destination in proposal)
    return SingleFlipVerificationResult(True, "exact complete single-flip trace", trace, final, metrics)

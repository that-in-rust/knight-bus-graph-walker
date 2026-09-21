"""Unequal-degree warm-state certificate; exact research implementation.

The source builder is resident O(n+M). Prepared immutable records must belong
to the certified source snapshot. Stream execution does not retain a label
map; caller buffers/sinks and variable-precision bit sizes remain charged.
"""

from dataclasses import dataclass
from fractions import Fraction
from itertools import accumulate


class MoverDegreeOrderStatistics:
    def __init__(self, values):
        if len(values) < 2 or any(not isinstance(x, (int, Fraction)) or x < 0 for x in values):
            raise ValueError("degree sequence must have at least two nonnegative rationals")
        self.values = tuple(Fraction(x) for x in values)
        ordered = sorted(range(len(values)), key=lambda index: (self.values[index], index))
        self.ranks = [0] * len(ordered)
        for rank, index in enumerate(ordered):
            self.ranks[index] = rank
        self.prefix = (Fraction(0), *accumulate(self.values[index] for index in ordered))

    def prefix_excluding_pair_sum(self, count, left, right):
        end = count
        excluded = sorted((self.ranks[left], self.ranks[right]))
        for rank in excluded:
            if rank < end:
                end += 1
        removed = sum((self.values[index] for index in (left, right) if self.ranks[index] < end), Fraction(0))
        return self.prefix[end] - removed

    def sum_excluding_pair_bounds(self, count, left, right):
        n = len(self.values)
        if (type(count) is not int or not 0 <= count <= n - 2 or type(left) is not int
                or type(right) is not int or left == right or not 0 <= left < n or not 0 <= right < n):
            raise ValueError("invalid count or excluded endpoint")
        low = self.prefix_excluding_pair_sum(count, left, right)
        total = self.prefix[-1] - self.values[left] - self.values[right]
        high = total - self.prefix_excluding_pair_sum(n - 2 - count, left, right)
        return low, high


@dataclass(frozen=True)
class HeterogeneousMoverVertexProfile:
    original_id: int
    mate_rank: int
    degree: Fraction
    alpha: Fraction
    beta: Fraction
    matching_weight: Fraction


@dataclass(frozen=True)
class HeterogeneousBandScalarHeader:
    b: int
    k: int
    crossing: int
    total: Fraction
    gamma: Fraction
    anchor_q_volume: Fraction
    initial_q_degree: Fraction
    minimum_mover_margin: Fraction
    minimum_anchor_margin: Fraction
    mover_configurations: int


@dataclass(frozen=True)
class PreparedHeterogeneousStreamBundle:
    header: HeterogeneousBandScalarHeader
    low: tuple
    high: tuple
    anchors: tuple


def bound_conditioned_mover_gains(total, gamma, mover, zmin, zmax, in_q, mate_q):
    own = mover.beta if in_q else mover.alpha
    other = mover.alpha if in_q else mover.beta
    if in_q == mate_q:
        own += mover.matching_weight
    else:
        other += mover.matching_weight
    if in_q:
        minimum = total * (other - own) - gamma * mover.degree * (total - 2 * zmin + mover.degree)
        maximum = total * (other - own) - gamma * mover.degree * (total - 2 * zmax + mover.degree)
        fresh = -total * own + gamma * mover.degree * (zmax - mover.degree)
    else:
        minimum = total * (other - own) - gamma * mover.degree * (2 * zmax - total + mover.degree)
        maximum = total * (other - own) - gamma * mover.degree * (2 * zmin - total + mover.degree)
        fresh = -total * own + gamma * mover.degree * (total - zmin - mover.degree)
    return minimum, maximum, fresh


def certify_heterogeneous_anchor_bounds(graph, p, q, ranks, degrees, order, total, gamma, aq, k):
    minimum = None
    for u in p | q:
        ip = sum((w for v, w in graph[u].items() if v in p), Fraction(0))
        iq = sum((w for v, w in graph[u].items() if v in q), Fraction(0))
        weights = sorted((w for v, w in graph[u].items() if v in ranks), reverse=True)
        prefix = [Fraction(0), *accumulate(weights)]
        whole, du = prefix[-1], degrees[u]
        for r in (k, k + 1, k + 2):
            zmin = aq + order.prefix[r]
            zmax = aq + order.prefix[-1] - order.prefix[len(ranks) - r]
            top = prefix[min(r if u in p else len(ranks) - r, len(weights))]
            if u in p:
                switch = total * (iq - ip - whole + 2 * top) - gamma * du * (2 * zmin - total + du)
                fresh = -total * (ip + whole - top) + gamma * du * (total - zmin - du)
            else:
                switch = total * (ip - iq - whole + 2 * top) - gamma * du * (total - 2 * zmax + du)
                fresh = -total * (iq + whole - top) + gamma * du * (zmax - du)
            if max(switch, fresh) > 0:
                raise ValueError(f"anchor bounds not certified: vertex={u}, r={r}")
            margin = min(-switch, -fresh)
            minimum = margin if minimum is None else min(minimum, margin)
    return minimum


def certify_heterogeneous_band_graph(graph, p, q, labels, gamma):
    p, q, vertices = set(p), set(q), set(graph)
    if (not p or not q or p & q or not (p | q) <= vertices or set(labels) != vertices
            or any(type(u) is not int for u in vertices)):
        raise ValueError("invalid anchor partition or IDs")
    if not isinstance(gamma, (int, Fraction)) or isinstance(gamma, bool) or gamma <= 0:
        raise ValueError("resolution must be positive exact rational")
    gamma = Fraction(gamma)
    for u, row in graph.items():
        for v, weight in row.items():
            if (v not in vertices or v == u or not isinstance(weight, (int, Fraction))
                    or isinstance(weight, bool) or weight <= 0 or graph[v].get(u) != weight):
                raise ValueError("source must be positive rational, symmetric and loopless")
    y = sorted(vertices - p - q)
    b = len(y)
    if b < 2 or b % 2 or any(labels[u] != "P" for u in p) or any(labels[u] != "Q" for u in q):
        raise ValueError("invalid mover count or anchor labels")
    k = sum(labels[u] == "Q" for u in y)
    if not 0 <= k <= b - 2 or any(labels[u] != ("Q" if j < k else "P") for j, u in enumerate(y)):
        raise ValueError("mover labels must be a Q prefix followed by a P suffix")
    ranks = {u: j for j, u in enumerate(y)}
    degrees = {u: sum(row.values(), Fraction(0)) for u, row in graph.items()}
    total, aq = sum(degrees.values()), sum(degrees[u] for u in q)
    records = []
    for u in y:
        partners = [(v, w) for v, w in graph[u].items() if v in ranks]
        if len(partners) != 1:
            raise ValueError("movers must induce only a reciprocal perfect matching")
        mate, weight = partners[0]
        alpha = sum((w for v, w in graph[u].items() if v in p), Fraction(0))
        beta = sum((w for v, w in graph[u].items() if v in q), Fraction(0))
        records.append(HeterogeneousMoverVertexProfile(u, ranks[mate], degrees[u], alpha, beta, weight))
    order = MoverDegreeOrderStatistics([record.degree for record in records])
    minimum, configurations = None, 0
    for rank, mover in enumerate(records):
        mate = records[mover.mate_rank]
        for r in (k, k + 1, k + 2):
            for in_q in (False, True):
                for mate_q in (False, True):
                    count = r - in_q - mate_q
                    if not 0 <= count <= b - 2:
                        continue
                    low, high = order.sum_excluding_pair_bounds(count, rank, mover.mate_rank)
                    fixed = aq + in_q * mover.degree + mate_q * mate.degree
                    low_gain, high_gain, fresh = bound_conditioned_mover_gains(
                        total, gamma, mover, fixed + low, fixed + high, in_q, mate_q)
                    if fresh > 0:
                        raise ValueError(f"mover fresh-community bound not certified: vertex={mover.original_id}, r={r}")
                    eligible = in_q != mate_q and (r >= k + 1 if in_q else r <= k + 1)
                    if eligible and low_gain <= 0 or not eligible and high_gain > 0:
                        raise ValueError(f"mover gain sign not certified: vertex={mover.original_id}, r={r}")
                    margin = min(-fresh, low_gain if eligible else -high_gain)
                    minimum = margin if minimum is None else min(minimum, margin)
                    configurations += 1
    anchor_margin = certify_heterogeneous_anchor_bounds(graph, p, q, ranks, degrees, order, total, gamma, aq, k)
    crossing = sum(record.mate_rank >= k for record in records[:k])
    header = HeterogeneousBandScalarHeader(b, k, crossing, total, gamma, aq,
                                          sum((row.degree for row in records[:k]), Fraction(0)),
                                          minimum, anchor_margin, configurations)
    return PreparedHeterogeneousStreamBundle(header, tuple(records[:k]), tuple(records[k:]),
                                             tuple((u, labels[u]) for u in sorted(p | q)))


def run_heterogeneous_band_streams(header, low, high, emit_low, emit_high, emit_move):
    """Constant control-record count; exact arithmetic widths remain charged."""
    streams, sinks = (iter(low), iter(high)), (emit_low, emit_high)
    cursors, ends = [0, header.k], (header.k, header.b)
    previous, crossing_reads = [None, None], [0, 0]
    completed, dq, r = 0, header.initial_q_degree, header.k

    def consume_heterogeneous_stream_record(side, allow_move):
        nonlocal completed, dq, r
        rank = cursors[side]
        try:
            record = next(streams[side])
        except StopIteration as error:
            raise ValueError("truncated heterogeneous stream") from error
        vertex, mate = record.original_id, record.mate_rank
        if (type(vertex) is not int or type(mate) is not int or not 0 <= mate < header.b
                or mate == rank or previous[side] is not None and vertex <= previous[side]):
            raise ValueError("invalid prepared heterogeneous record")
        previous[side] = vertex
        cursors[side] += 1
        crossing = (rank < header.k) != (mate < header.k)
        if crossing:
            crossing_reads[side] += 1
            live = mate >= cursors[1 - side]
            if live:
                if not allow_move:
                    raise ValueError("live pair contradicts certified crossing count")
                qvol = header.anchor_q_volume + dq
                diff = record.beta - record.alpha if side else record.alpha - record.beta
                volumes = 2 * qvol - header.total if side else header.total - 2 * qvol
                gain = header.total * (diff + record.matching_weight) - header.gamma * record.degree * (volumes + record.degree)
                if gain <= 0:
                    raise ValueError("stream contradicts certified positive gain")
                label = "Q" if side else "P"
                emit_move((1 + (completed + 2) // 4, vertex, label, gain))
                completed += 1
                dq += record.degree if side else -record.degree
                r += 1 if side else -1
            else:
                label = "P" if side else "Q"
        else:
            label = "P" if side else "Q"
        sinks[side]((vertex, label))

    while completed < header.crossing:
        side = 1 if completed // 2 % 2 == 0 else 0
        if cursors[side] >= ends[side]:
            raise ValueError("heterogeneous stream exhausted before completions")
        consume_heterogeneous_stream_record(side, True)
    for side in (0, 1):
        while cursors[side] < ends[side]:
            consume_heterogeneous_stream_record(side, False)
        try:
            next(streams[side])
        except StopIteration:
            pass
        else:
            raise ValueError("trailing heterogeneous stream records")
        if crossing_reads[side] != header.crossing:
            raise ValueError("heterogeneous crossing count mismatch")
    return {"moves": completed, "sweeps": 2 + (completed + 1) // 4 if completed else 1,
            "mover_records_read": header.b, "movers_final_q": r, "mover_q_degree": dq}

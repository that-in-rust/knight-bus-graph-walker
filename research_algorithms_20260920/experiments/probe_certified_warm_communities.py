"""Exact warm-state research kernel; resident builder, forward-only execution.

Prepared streams must be the unchanged output of certification. This is not
an untrusted-file decoder, a whole-process memory cap, or complete Louvain.
"""

from dataclasses import dataclass
from fractions import Fraction
from heapq import merge
from itertools import accumulate


@dataclass(frozen=True)
class WarmStateScalarCertificate:
    b: int
    k: int
    crossing: int
    total: Fraction
    gamma: Fraction
    degree: Fraction
    h: Fraction
    a: Fraction
    f0: Fraction
    min_anchor_margin: Fraction
    min_fresh_margin: Fraction


@dataclass(frozen=True)
class PreparedWarmStreamBundle:
    header: WarmStateScalarCertificate
    low: tuple
    high: tuple
    anchors: tuple


def merge_certified_label_streams(anchors, low, high):
    """Merge three certified sorted runs without materializing full labels."""
    return merge(anchors, low, high)


def check_anchor_stability_bounds(graph, degrees, p, q, y, gamma, total, volumes):
    minimum = None
    for u in p | q:
        ip = sum((w for v, w in graph[u].items() if v in p), Fraction(0))
        iq = sum((w for v, w in graph[u].items() if v in q), Fraction(0))
        weights = sorted((w for v, w in graph[u].items() if v in y), reverse=True)
        prefix = [Fraction(0), *accumulate(weights)]
        whole = prefix[-1]
        for r, vp, vq in volumes:
            count = r if u in p else len(y) - r
            top = prefix[min(count, len(weights))]
            own = ip if u in p else iq
            other = iq if u in p else ip
            source, destination = (vp, vq) if u in p else (vq, vp)
            move = total * (other - own - whole + 2 * top) - gamma * degrees[u] * (
                destination - source + degrees[u])
            fresh = -total * (own + whole - top) + gamma * degrees[u] * (source - degrees[u])
            if max(move, fresh) > 0:
                raise ValueError(f"anchor stability not certified: vertex={u}, r={r}")
            margin = min(-move, -fresh)
            minimum = margin if minimum is None else min(minimum, margin)
    return minimum


def certify_warm_graph_state(graph, p, q, labels, gamma):
    """Validate actual source; O(n+m) resident memory is deliberately charged."""
    p, q = set(p), set(q)
    vertices = set(graph)
    if (not p or not q or p & q or not p | q <= vertices or set(labels) != vertices
            or any(type(u) is not int for u in vertices)):
        raise ValueError("invalid anchor partition or IDs")
    if not isinstance(gamma, (int, Fraction)) or isinstance(gamma, bool) or gamma <= 0:
        raise ValueError("resolution must be positive exact rational")
    gamma = Fraction(gamma)
    for u, row in graph.items():
        for v, weight in row.items():
            if (v not in vertices or v == u or not isinstance(weight, (int, Fraction))
                    or isinstance(weight, bool) or weight <= 0 or graph[v].get(u) != weight):
                raise ValueError("source must be simple, symmetric, positive rational and loopless")
    y = sorted(vertices - p - q)
    b = len(y)
    if b < 2 or b % 2:
        raise ValueError("mover count must be positive even")
    if any(labels[u] != "P" for u in p) or any(labels[u] != "Q" for u in q):
        raise ValueError("anchor labels do not match declared sets")
    k = sum(labels[u] == "Q" for u in y)
    if not 0 <= k <= b - 2 or any(labels[u] != ("Q" if j < k else "P")
                                 for j, u in enumerate(y)):
        raise ValueError("mover labels must be a Q prefix and P suffix")
    ranks = {u: j for j, u in enumerate(y)}
    degrees = {u: sum(row.values(), Fraction(0)) for u, row in graph.items()}
    signatures, mates = set(), []
    for u in y:
        matched = [(v, w) for v, w in graph[u].items() if v in ranks]
        if len(matched) != 1:
            raise ValueError("movers must induce only a perfect matching")
        mate, weight = matched[0]
        alpha = sum((w for v, w in graph[u].items() if v in p), Fraction(0))
        beta = sum((w for v, w in graph[u].items() if v in q), Fraction(0))
        signatures.add((alpha, beta, weight))
        mates.append((u, ranks[mate]))
    if len(signatures) != 1:
        raise ValueError("mover anchor affinities and matching weights must be uniform")
    alpha, beta, weight = next(iter(signatures))
    degree = alpha + beta + weight
    total = sum(degrees.values())
    ap = sum(degrees[u] for u in p)
    aq = sum(degrees[u] for u in q)
    h = gamma * degree * degree
    a = total * weight - h
    r0 = k + 1
    f0 = total * (beta - alpha) - gamma * degree * (aq - ap - degree * b) - 2 * h * r0
    if not abs(f0) < a < 2 * h - abs(f0):
        raise ValueError("strict three-state band not certified")
    volumes = tuple((r, ap + degree * (b - r), aq + degree * r)
                    for r in (r0 - 1, r0, r0 + 1))
    fresh_margins = tuple(total * affinity - gamma * degree * (volume - degree)
                          for _, vp, vq in volumes
                          for affinity, volume in ((alpha, vp), (beta, vq)))
    if min(fresh_margins) < 0:
        raise ValueError("mover fresh-community exclusion not certified")
    anchor_margin = check_anchor_stability_bounds(
        graph, degrees, p, q, set(y), gamma, total, volumes)
    crossing = sum(mate >= k for _, mate in mates[:k])
    header = WarmStateScalarCertificate(b, k, crossing, total, gamma, degree, h, a, f0,
                                        anchor_margin, min(fresh_margins))
    anchors = tuple((u, labels[u]) for u in sorted(p | q))
    return PreparedWarmStreamBundle(header, tuple(mates[:k]), tuple(mates[k:]), anchors)


def run_certified_warm_streams(header, low, high, emit_low, emit_high, emit_move):
    """Use constant control records; sink/caller memory is not covered here."""
    streams = (iter(low), iter(high))
    sinks = (emit_low, emit_high)
    cursors, ends = [0, header.k], (header.k, header.b)
    previous_ids, cross_reads = [None, None], [0, 0]
    completed, r = 0, header.k

    def consume_certified_stream_record(side, allow_move):
        nonlocal completed, r
        rank = cursors[side]
        try:
            vertex, mate = next(streams[side])
        except StopIteration as error:
            raise ValueError("truncated certified stream") from error
        if (type(vertex) is not int or type(mate) is not int
                or not 0 <= mate < header.b or mate == rank
                or previous_ids[side] is not None and vertex <= previous_ids[side]):
            raise ValueError("invalid certified stream record")
        previous_ids[side] = vertex
        cursors[side] += 1
        crossing = (rank < header.k) != (mate < header.k)
        moved = False
        if crossing:
            cross_reads[side] += 1
            moved = mate >= cursors[1 - side]
            if moved:
                if not allow_move:
                    raise ValueError("live pair contradicts certified crossing count")
                f = header.f0 - 2 * header.h * (r - header.k - 1)
                gain = header.a + (f if side else -f)
                if gain <= 0:
                    raise ValueError("stream schedule contradicts certified gain")
                label = "Q" if side else "P"
                emit_move((1 + (completed + 2) // 4, vertex, label, gain))
                completed += 1
                r += 1 if side else -1
            else:
                label = "P" if side else "Q"
        else:
            label = "P" if side else "Q"
        sinks[side]((vertex, label))
        return moved

    while completed < header.crossing:
        side = 1 if (completed // 2) % 2 == 0 else 0
        if cursors[side] >= ends[side]:
            raise ValueError("certified stream exhausted before completions")
        consume_certified_stream_record(side, True)
    for side in (0, 1):
        while cursors[side] < ends[side]:
            consume_certified_stream_record(side, False)
        try:
            next(streams[side])
        except StopIteration:
            pass
        else:
            raise ValueError("trailing certified stream records")
        if cross_reads[side] != header.crossing:
            raise ValueError("certified crossing count mismatch")
    return {"moves": completed, "sweeps": 2 + (completed + 1) // 4 if completed else 1,
            "mover_records_read": header.b, "movers_final_q": r}

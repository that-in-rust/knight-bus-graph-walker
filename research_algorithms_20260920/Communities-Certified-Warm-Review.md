# Independent Review: Certified Warm Frontiers

Date: 2026-09-21. Scope: A04 extension only, not the other six families.
This review reads the candidate and its predecessor, but imports no lead
implementation, test, or fixture. Only this review file is reviewer-owned.
No timing, RSS, publication-priority, or end-to-end performance claim is made.

## Findings

**The trajectory theorem is sound under all the stated certificate gates.**
The proof below covers even and zero crossing counts, absent anchor-to-mover
edges, and arbitrary anchor-ID interleaving. No counterexample to the full
certificate was found. Three qualifications should accompany the result:

1. **Storage units need a width assumption.** Unique integer IDs need not fit
   an O(log n)-bit word. Use O(n) records and O(Scan(S)) transfers for serialized
   size S, or explicitly require bounded-width IDs/tokens/ranks. For example,
   relabel any admitted 64-vertex graph in order by IDs `2^L+i`. This preserves
   every certificate and decision but needs Omega(64L) explicit output bits.
   The existing rational-bit caveat should also cover IDs and exact builder
   intermediates. After certification the direction/label scheduler needs no
   rational arithmetic; emitting numerical gains would retain their bit cost.
2. **Linear execution means accepted moves and final labels, not all stays.**
   A full visit-event transcript has n times the number of sweeps records,
   potentially Theta(n*c). Say this explicitly in the new manuscript as in
   its predecessor. Logical no-op sweeps can be inferred, not materialized
   within the advertised O(n) work when c is linear.
3. **Do not weaken admission to aggregate equality or the band alone.**
   The singleton and anchor gates are essential. Concrete rejected witnesses
   below exercise each. The current in-memory builder's O(n+M) *records*,
   where M is the stored edge count rather than total edge weight T/2, are
   compatible with the claim, not an end-to-end constant-RAM algorithm.

These are scope/accounting qualifications, not a discovered algebraic error.
No closest-art search was needed to settle these mathematical questions;
novelty and safe-reduction priority remain unreviewed.

## Exact Proof

Write `Q = sum_C (2*internal_weight(C)/T - gamma*volume(C)^2/T^2)`.
Moving a degree-du vertex from C to D changes the internal-edge numerator
by `2*(eD-eC)` and the squared-volume sum by
`2*du*(VD-VC+du)`. Thus `Delta Q = 2*G/T^2` with precisely the candidate's G.
For a fresh singleton put `VD=eD=0`; no self-loop term is missing.

For a P mover, `VQ-VP = Aq-Ap-db+2dr`. A split mate contributes `+w` to
`eQ-eP`, whereas a same-P mate contributes `-w`. Substitution gives `F+a`
and `F-B`. Reversing the source gives `a-F` and `-F-B` for Q movers.

Let `f=f0`, `h>0`, `a>abs(f)`, and `a<2h-abs(f)`. The three F values are
`f+2h, f, f-2h`. Respectively they are above a, inside (-a,a), and below -a.
Also `abs(F)<=2h+abs(f)<2h+a=B`. Therefore only unresolved split endpoints
move, with eligibility P-only / both / Q-only. Reunited and initially
same-side pairs never split. The upper strict inequality is conservative:
`abs(f)<a<=2h-abs(f)` also suffices because zero gain stays. This optional
relaxation is not needed for the current theorem. Making the *lower*
inequality non-strict is unsound for the declared HHLL schedule.

For an anchor, sort its b mover-position weights, padding missing positions
with zeros; do not confuse nonzero degree with the number b of positions.
For a size-t subset S, `sum_S w <= top(t)` with equality, and
`min sum_S w = W-top(b-t)`. For a P anchor the destination subset has size r,
so the switch gain is affine increasing in its sum, with coefficient `2T`.
The singleton gain increases with the same sum, with coefficient T.
Their exact maxima over all such subsets are the two candidate bounds.
For a Q anchor the destination subset has size b-r, giving its stated bounds.
These are exact extrema at fixed aggregate volumes, although the maximizing
subsets may be unreachable. Thus admission can be conservative, never unsafe.
`top(0)=0`, `top(b)=W`, and `top(t)=W` once t exceeds the nonzero count.

For movers, dropping a source-side mate weight only increases singleton gain
by `T*w`. The two singleton inequalities are consequently sufficient at all
three states. A joint induction now closes the argument: at every visit,
anchors stay, singleton choices have nonpositive gain, and the only possible
move reunites one split pair and changes r within the three-state band.
This does not assume anchor stability first and then prove it circularly.

Initially `r=k=r0-1`. All unresolved Q endpoints precede all unresolved P
endpoints in mover visit order. The first two completions are therefore H,H
unless exhausted early. Two H moves reach `r0+1`; the next eligible L is
behind the visit cursor, forcing a new sweep. Two L moves reach `r0-1`;
the next H is later in the same sweep. Anchor visits insert only stays.
This proves HHLL repetition for any reciprocal matching, not just odd c.
Necessarily `c == k (mod 2)` and `0<=c<=min(k,b-k)` for a realizable matching.

For zero-based completion j, the sweep is `1+(j+2)//4`. If c>0 its last
moving sweep is `1+(c+1)//4`, and the terminal no-move sweep adds one.
For c=0 the very first sweep is terminal. Final r for c mod 4 equal to
0,1,2,3 is `r0-1,r0,r0+1,r0`. These include partially completed phases.

Finally, a crossing pair is live iff neither of its rank positions has been
consumed. Advancing the chosen order skips exactly pairs removed by the other
order; otherwise it selects the least live endpoint. A skipped counterpart
inherits the selecting side's final label. Use *consumed-record rank*, not
buffer read-ahead or numerical ID distance. Same-side pairs are fixed.
Sorted mover spools and the sorted anchor stream can be merged even when
anchor IDs interleave arbitrarily. No inverse-ID map is needed during
execution, but construction of the mate ranks and sorted streams is paid.

## Rejected Witnesses And Construction Cost

**Missing singleton gate:** take two isolated anchors and eight movers with
unit matching edges `(0,3),(1,4),(2,5),(6,7)`. Initially movers 0,1,2 are Q,
the rest P; gamma=3. Then `T=8,d=1,h=3,a=5,B=11,f0=0,r0=4`.
The strict band and every anchor bound pass. On the very first mover visit,
vertex 0 has switch gain -1 but singleton gain 6 (`Delta Q=3/16`). Thus the
band-plus-anchor certificate alone fails; the actual singleton gate rejects.
Zero mover affinities and isolated anchors are allowed by the stated contract.

**Non-strict lower band:** the replay constructs a positive-weight sparse
64-vertex graph with `k=c=2`, `T=18688`, `h=25921/2`,
`a=11455/2`, and `f0=-a`. All stability gates pass. Its completions are H in
sweep 1 then L in sweep 2, not H,H in sweep 1; terminal sweep is 3, not 2.
This is outside the strict certificate and demonstrates why that exclusion
matters. The code also adds a tiny cross-edge between two new anchors to a
valid graph, preserving the band and mover singleton gates but violating
anchor stability; the newly inserted first anchor has positive switch gain.

For even/zero c fixtures the checker changes anchor internal volume by
`Ap <- Ap+delta`, `Aq <- Aq-delta`, keeping T and mover affinities fixed.
Consequently `F(r0) <- F(r0)+2*gamma*d*delta`. At gamma=1/2, choose
`delta=(target_f0-F(r0))/d`. On the 2b- and b-cycles, add `delta/(4b)` and
`-delta/(2b)` to each edge, respectively. Every edge remains positive in
the retained cases. Balanced rational perturbations of different cycle
edges preserve these totals while making anchor degrees irregular. The
checker then recomputes, rather than trusts, the full certificate.

Reading and validating a supplied graph costs Omega(n+M) records. Sorting
each anchor's s_u nonzero mover weights to obtain top sums costs at most
`O(sum_u s_u log(1+s_u))` comparisons; zeros need not be expanded. Only a
constant number of top queries per anchor is necessary. ID sorting, duplicate
edge checks, membership validation and mate-rank construction must also be
charged. External construction is not supplied or proved here; its sorting,
joins and scratch storage cannot be charged to the linear scan executor.
An admitted dense source can still have Theta(n^2) source records. A trusted
prepared stream must correspond to the checked graph and partition snapshot.

## Independent Replay

Run from the repository root with Python 3.11+; only the standard library is
used. The checker keeps graphs and outputs in RAM deliberately. It verifies
mathematics, not the memory use or byte format of the lead implementation.
It asserts rather than merely prints equivalence. No lead module is imported.

```sh
awk '/^<!-- WARM-REVIEW-CHECKER-START -->/{p=1;next} /^<!-- WARM-REVIEW-CHECKER-END -->/{p=0} p && !/^```/' \
  research_algorithms_20260920/Communities-Certified-Warm-Review.md | python3 -B -
```

<!-- WARM-REVIEW-CHECKER-START -->
```python
from collections import Counter
from fractions import Fraction as R
from itertools import combinations, product
import json
import random

counts = Counter()


def insert_positive_undirected_edge(graph, u, v, weight):
    assert u != v and v not in graph[u] and weight > 0
    graph[u][v] = graph[v][u] = R(weight)


def calculate_partition_modularity_exact(graph, labels, gamma):
    total = sum(sum(row.values()) for row in graph.values())
    volumes = Counter()
    internal = R(0)
    for u, row in graph.items():
        volumes[labels[u]] += sum(row.values())
        internal += sum(w for v, w in row.items() if labels[u] == labels[v])
    return internal / total - gamma * sum(v*v for v in volumes.values()) / total**2


def calculate_vertex_move_gain(graph, labels, u, destination, gamma):
    source = labels[u]
    degree = sum(graph[u].values())
    total = sum(sum(row.values()) for row in graph.values())
    own = sum(w for v, w in graph[u].items() if labels[v] == source)
    other = sum(w for v, w in graph[u].items() if labels[v] == destination)
    source_volume = sum(sum(row.values()) for v, row in graph.items()
                        if labels[v] == source)
    target_volume = sum(sum(row.values()) for v, row in graph.items()
                        if labels[v] == destination)
    return total*(other-own) - gamma*degree*(target_volume-source_volume+degree)


def enumerate_scaled_gain_checks():
    for weights in product((R(0), R(1, 2), R(2)), repeat=3):
        if not any(weights):
            continue
        graph = {u: {} for u in range(3)}
        for (u, v), weight in zip(combinations(range(3), 2), weights):
            if weight:
                insert_positive_undirected_edge(graph, u, v, weight)
        total = 2*sum(weights)
        for gamma in (R(1, 2), R(1), R(3, 2)):
            for initial in product((0, 1), repeat=3):
                labels = dict(enumerate(initial))
                before = calculate_partition_modularity_exact(graph, labels, gamma)
                for u in graph:
                    for destination in (1-labels[u], 2):
                        gain = calculate_vertex_move_gain(graph, labels, u, destination, gamma)
                        changed = labels.copy()
                        changed[u] = destination
                        after = calculate_partition_modularity_exact(graph, changed, gamma)
                        assert after-before == 2*gain/total**2
                        counts['exact_modularity_deltas'] += 1


def enumerate_anchor_subset_extrema():
    for b in range(2, 7):
        for weights in product((R(0), R(1, 2), R(2)), repeat=b):
            nonzero = sorted((w for w in weights if w), reverse=True)
            total = sum(weights)
            degree = 12+total
            for t in range(b+1):
                sums = [sum(weights[i] for i in subset)
                        for subset in combinations(range(b), t)]
                counts['anchor_subsets'] += len(sums)
                assert max(sums) == sum(nonzero[:t])
                assert min(sums) == total-sum(nonzero[:b-t])
                for own, other in ((5, 7), (7, 5)):
                    switch = [220*(other-own-total+2*x)
                              - R(3, 2)*degree*(120-100+degree) for x in sums]
                    single = [-220*(own+total-x)
                              + R(3, 2)*degree*(100-degree) for x in sums]
                    top = sum(nonzero[:t])
                    assert max(switch) == 220*(other-own-total+2*top) - R(3, 2)*degree*(20+degree)
                    assert max(single) == -220*(own+total-top) + R(3, 2)*degree*(100-degree)
                    counts['anchor_extremum_pairs'] += 1


def enumerate_strict_band_checks():
    for h in range(1, 9):
        for a in range(1, 2*h+1):
            for f in range(-2*h, 2*h+1):
                if not abs(f) < a < 2*h-abs(f):
                    continue
                for offset, expected in ((-1, (True, False)),
                                         (0, (True, True)), (1, (False, True))):
                    value = f-2*h*offset
                    assert (value+a > 0, a-value > 0) == expected
                    assert value-(a+2*h) < 0 and -value-(a+2*h) < 0
                counts['strict_band_cases'] += 1


def make_perfect_mate_arrays(vertices):
    if not vertices:
        yield {}
        return
    u = vertices[0]
    for v in vertices[1:]:
        rest = tuple(x for x in vertices if x not in (u, v))
        for matching in make_perfect_mate_arrays(rest):
            yield {**matching, u: v, v: u}


def simulate_scalar_serial_visits(mates, k, f):
    # Aggregate graph parameters: d=1, gamma=8, T=10b, Tw=12,
    # alpha=beta=(1-w)/2. Stored volumes below are multiplied by 8.
    b, r0 = len(mates), k+1
    ap8 = 4*(10*b)-8*b+8*r0+f//2
    aq8 = 8*(10*b-b)-ap8
    labels, trace, sweep = [int(i < k) for i in range(b)], [], 0
    while True:
        sweep += 1
        moved = False
        for u in range(b):
            r = sum(labels)
            volumes = (ap8+8*(b-r), aq8+8*r)
            source, destination = labels[u], 1-labels[u]
            affinity_difference = 12 if labels[mates[u]] == destination else -12
            gain = affinity_difference-(volumes[destination]-volumes[source]+8)
            if gain > 0:
                labels[u] = destination
                trace.append((sweep, u, destination))
                moved = True
        if not moved:
            return trace, labels, sweep
        assert sweep <= b+1


def emit_two_frontier_results(mates, k):
    b = len(mates)
    c = sum(mates[i] >= k for i in range(k))
    low, high, labels, trace = 0, k, [None]*b, []
    for j in range(c):
        choose_high = j % 4 < 2
        while True:
            u = high if choose_high else low
            assert u < (b if choose_high else k)
            v = mates[u]
            if choose_high:
                high += 1
            else:
                low += 1
            fixed = (u < k) == (v < k)
            stale = v < (low if choose_high else high)
            if fixed:
                labels[u] = int(u < k)
            elif stale:
                labels[u] = 0 if choose_high else 1
            else:
                labels[u] = 1 if choose_high else 0
                trace.append((1+(j+2)//4, u, labels[u]))
                break
    for u in list(range(low, k))+list(range(high, b)):
        v = mates[u]
        if (u < k) == (v < k):
            labels[u] = int(u < k)
        else:
            assert v < (high if u < k else low)
            labels[u] = 1 if u < k else 0
    assert all(label in (0, 1) for label in labels)
    assert all(labels[u] == labels[v] for u, v in enumerate(mates))
    return trace, labels, 1 if c == 0 else 2+(c+1)//4


def enumerate_small_matching_schedules():
    seen_crossings = Counter()
    for b in range(2, 11, 2):
        for matching in make_perfect_mate_arrays(tuple(range(b))):
            mates = tuple(matching[i] for i in range(b))
            for k in range(b-1):
                expected = emit_two_frontier_results(mates, k)
                c = sum(mates[i] >= k for i in range(k))
                assert c % 2 == k % 2 and c <= min(k, b-k)
                assert sum(expected[1]) == k+(0, 1, 2, 1)[c % 4]
                seen_crossings[c] += 1
                counts['matching_prefix_cases'] += 1
                for f in (-2, 0, 2):
                    assert simulate_scalar_serial_visits(mates, k, f) == expected
                    counts['scalar_schedule_replays'] += 1
    counts['zero_crossing_prefixes'] = seen_crossings[0]
    counts['positive_even_crossing_prefixes'] = sum(v for c, v in seen_crossings.items() if c and c % 2 == 0)
    assert set(seen_crossings) == set(range(6))


def make_crossing_count_matching(b, k, c, rng):
    low, high = list(range(k)), list(range(k, b))
    rng.shuffle(low)
    rng.shuffle(high)
    pairs = list(zip(low[:c], high[:c]))
    for rest in (low[c:], high[c:]):
        assert len(rest) % 2 == 0
        pairs += list(zip(rest[::2], rest[1::2]))
    mates = [None]*b
    for u, v in pairs:
        mates[u], mates[v] = v, u
    return tuple(mates)


def make_sparse_fixture_graph(b, k, c, seed, irregular, tune=True, target=R(0)):
    rng = random.Random(seed)
    ids = list(range(4*b)) if seed == 0 else rng.sample(range(-10000, 10000), 4*b)
    p, q, y = ids[:2*b], ids[2*b:3*b], sorted(ids[3*b:])
    graph = {u: {} for u in ids}
    total, degree = 75*b*b-32*b, 10*b+1
    ap, aq = 50*b*b-22*b, 15*b*b-11*b
    f = total*(-2*b)-R(degree, 2)*(aq-ap-degree*b)-degree**2*(k+1)
    delta = (target-f)/degree if tune else R(0)
    wp = R(11*(2*b-1), 2)+delta/(4*b)
    wq = R(11*(b-1), 2)-delta/(2*b)
    for vertices, weight in ((p, wp), (q, wq)):
        order = vertices[:]
        rng.shuffle(order)
        for i, u in enumerate(order):
            insert_positive_undirected_edge(graph, u, order[(i+1) % len(order)], weight)
        if irregular:
            for edge, adjustment in ((0, R(1, 7)), (2, R(-1, 7))):
                u, v = order[edge], order[edge+1]
                graph[u][v] += adjustment
                graph[v][u] += adjustment
                assert graph[u][v] > 0
    pp, qq = p[:], q[:]
    rng.shuffle(pp)
    rng.shuffle(qq)
    mates = make_crossing_count_matching(b, k, c, rng)
    for i, u in enumerate(y):
        insert_positive_undirected_edge(graph, u, pp[2*i], 3*b)
        insert_positive_undirected_edge(graph, u, pp[2*i+1], 3*b)
        insert_positive_undirected_edge(graph, u, qq[i], 4*b)
        if i < mates[i]:
            insert_positive_undirected_edge(graph, u, y[mates[i]], 1)
    assert len(graph) == 4*b and sum(map(len, graph.values()))//2 == 13*b//2
    return graph, p, q, y, mates


def calculate_graph_certificate_errors(graph, p, q, y, k, gamma):
    assert len(set(p+q+y)) == len(p+q+y) == len(graph)
    assert p and q and len(y) % 2 == 0 and 0 <= k <= len(y)-2 and y == sorted(y)
    degree = {u: sum(row.values()) for u, row in graph.items()}
    for u, row in graph.items():
        for v, weight in row.items():
            assert u != v and weight > 0 and graph[v][u] == weight
    pset, qset, yset = set(p), set(q), set(y)
    affinities = []
    for u in y:
        partners = [(v, w) for v, w in graph[u].items() if v in yset]
        assert len(partners) == 1
        affinities.append((sum(w for v, w in graph[u].items() if v in pset),
                           sum(w for v, w in graph[u].items() if v in qset), partners[0][1]))
    assert len(set(affinities)) == 1
    alpha, beta, w = affinities[0]
    d, b, r0 = alpha+beta+w, len(y), k+1
    assert all(degree[u] == d for u in y)
    total, ap, aq = sum(degree.values()), sum(degree[u] for u in p), sum(degree[u] for u in q)
    h, a = gamma*d*d, total*w-gamma*d*d
    f = total*(beta-alpha)-gamma*d*(aq-ap-d*b)-2*h*r0
    errors = set()
    if not abs(f) < a < 2*h-abs(f):
        errors.add('band')
    for r in (r0-1, r0, r0+1):
        vp, vq = ap+d*(b-r), aq+d*r
        if max(-total*alpha+gamma*d*(vp-d), -total*beta+gamma*d*(vq-d)) > 0:
            errors.add('mover_singleton')
        for group, ownset, otherset, ownvol, othervol, t in (
                (p, pset, qset, vp, vq, r), (q, qset, pset, vq, vp, b-r)):
            for u in group:
                own = sum(w for v, w in graph[u].items() if v in ownset)
                other = sum(w for v, w in graph[u].items() if v in otherset)
                weights = sorted((w for v, w in graph[u].items() if v in yset), reverse=True)
                total_movers, top = sum(weights), sum(weights[:t])
                switch = total*(other-own-total_movers+2*top)-gamma*degree[u]*(othervol-ownvol+degree[u])
                single = -total*(own+total_movers-top)+gamma*degree[u]*(ownvol-degree[u])
                if switch > 0:
                    errors.add('anchor_move')
                if single > 0:
                    errors.add('anchor_singleton')
    return errors


def simulate_expanded_serial_visits(graph, p, q, y, k, gamma):
    labels = {**dict.fromkeys(p, 0), **dict.fromkeys(q, 1),
              **{u: int(i < k) for i, u in enumerate(y)}}
    trace, sweep = [], 0
    total = sum(sum(row.values()) for row in graph.values())
    while True:
        sweep += 1
        moved = False
        for u in sorted(graph):
            options = [(calculate_vertex_move_gain(graph, labels, u, dest, gamma), dest)
                       for dest in (1-labels[u], 2)]
            gain, destination = max(options, key=lambda pair: (pair[0], -pair[1]))
            if gain > 0:
                assert destination != 2
                before = calculate_partition_modularity_exact(graph, labels, gamma)
                labels[u] = destination
                after = calculate_partition_modularity_exact(graph, labels, gamma)
                assert after-before == 2*gain/total**2
                counts['expanded_accepted_modularity_deltas'] += 1
                trace.append((sweep, u, destination))
                moved = True
        if not moved:
            return trace, labels, sweep
        assert sweep <= len(y)+1


def enumerate_expanded_fixture_checks():
    cases = []
    for k in range(15):
        for c in range(k % 2, min(k, 16-k)+1, 2):
            for seed, irregular in ((0, False), (71, False), (109, True)):
                cases.append((16, k, c, seed, irregular, True))
    for b in (16, 24):
        k = 3*b//4-1
        for c in range(1, min(k, b-k)+1, 2):
            for seed, irregular in ((0, False), (83, True)):
                cases.append((b, k, c, seed, irregular, False))
    for b, k, c, seed, irregular, tune in cases:
        graph, p, q, y, mates = make_sparse_fixture_graph(b, k, c, seed, irregular, tune)
        assert calculate_graph_certificate_errors(graph, p, q, y, k, R(1, 2)) == set()
        expected_trace, expected_y, expected_sweeps = emit_two_frontier_results(mates, k)
        expected_trace = [(sweep, y[rank], dest) for sweep, rank, dest in expected_trace]
        expected_labels = {**dict.fromkeys(p, 0), **dict.fromkeys(q, 1), **dict(zip(y, expected_y))}
        assert simulate_expanded_serial_visits(graph, p, q, y, k, R(1, 2)) == (expected_trace, expected_labels, expected_sweeps)
        counts['expanded_admitted_graphs'] += 1
        counts['expanded_zero_crossing_graphs'] += c == 0
        counts['expanded_positive_even_graphs'] += c > 0 and c % 2 == 0
        if seed:
            assert any(y[0] < u < y[-1] for u in p+q)
            counts['expanded_interleaved_id_graphs'] += 1
        if irregular:
            assert len({sum(graph[u].values()) for u in p}) > 1
            counts['expanded_irregular_graphs'] += 1


def verify_unsafe_relaxation_witnesses():
    y, p, q = list(range(8)), [-2], [-1]
    graph = {u: {} for u in p+q+y}
    for u, v in ((0, 3), (1, 4), (2, 5), (6, 7)):
        insert_positive_undirected_edge(graph, u, v, 1)
    labels = {**dict.fromkeys(p, 0), **dict.fromkeys(q, 1), **{u: int(u < 3) for u in y}}
    assert calculate_graph_certificate_errors(graph, p, q, y, 3, R(3)) == {'mover_singleton'}
    assert calculate_vertex_move_gain(graph, labels, 0, 0, R(3)) == -1
    assert calculate_vertex_move_gain(graph, labels, 0, 2, R(3)) == 6
    counts['rejected_unsafe_witnesses'] += 1

    graph, p, q, y, mates = make_sparse_fixture_graph(16, 2, 2, 19, True, target=R(-11455, 2))
    assert calculate_graph_certificate_errors(graph, p, q, y, 2, R(1, 2)) == {'band'}
    trace, labels, sweeps = simulate_expanded_serial_visits(graph, p, q, y, 2, R(1, 2))
    assert [(s, dest) for s, u, dest in trace] == [(1, 1), (2, 0)] and sweeps == 3
    assert [(s, dest) for s, u, dest in emit_two_frontier_results(mates, 2)[0]] == [(1, 1), (1, 1)]
    counts['rejected_unsafe_witnesses'] += 1

    graph, p, q, y, mates = make_sparse_fixture_graph(16, 10, 2, 23, False)
    u, v = -20000, -19999
    graph[u], graph[v] = {}, {}
    p, q = p+[u], q+[v]
    insert_positive_undirected_edge(graph, u, v, R(1, 100))
    assert calculate_graph_certificate_errors(graph, p, q, y, 10, R(1, 2)) == {'anchor_move', 'anchor_singleton'}
    labels = {**dict.fromkeys(p, 0), **dict.fromkeys(q, 1), **{u: int(i < 10) for i, u in enumerate(y)}}
    assert min(graph) == u and calculate_vertex_move_gain(graph, labels, u, 1, R(1, 2)) > 0
    counts['rejected_unsafe_witnesses'] += 1


enumerate_scaled_gain_checks()
enumerate_anchor_subset_extrema()
enumerate_strict_band_checks()
enumerate_small_matching_schedules()
enumerate_expanded_fixture_checks()
verify_unsafe_relaxation_witnesses()
print(json.dumps(dict(sorted(counts.items())), indent=2))
```
<!-- WARM-REVIEW-CHECKER-END -->

### Replay Receipt

The complete standalone command exited successfully on two runs, with the
same counts. All comparisons use
exact arithmetic; finite checks supplement, not replace, the proof above.

| Independent check | Count |
| --- | ---: |
| Exact modularity differences, all nonempty three-vertex weighted graphs over weights 0, 1/2, 2; all binary partitions; three resolutions; switches and singleton moves | 3,744 |
| Strict-band integer cases, 1 <= h <= 8 | 344 |
| Anchor subsets over all weight vectors in {0, 1/2, 2}^b, 2 <= b <= 6 | 55,980 |
| Exact switch/singleton maximum pairs checked over both anchor sides | 14,202 |
| All perfect matchings and allowed prefixes, b=2,4,6,8,10 | 9,325 |
| Scalar serial versus frontier replays, three interior score offsets per matching/prefix | 27,975 |
| Matching/prefix cases with zero / positive even crossings | 1,415 / 3,782 |
| Independently constructed graphs admitted; full trace, labels, sweeps agree | 143 |
| Admitted graphs with zero / positive even crossings | 24 / 48 |
| Admitted graphs with interleaved IDs / irregular anchor degrees | 93 / 50 |
| Expanded-graph accepted moves checked against recomputed modularity | 409 |
| Unsafe-relaxation witnesses rejected for the intended reason | 3 |

Expanded fixtures comprise 135 graphs with 64 vertices and 104 edges, plus
eight with 96 vertices and 156 edges. Subcategories overlap. The 409 accepted
move checks include the two moves in the rejected lower-band equality
witness; the 143 admitted graphs contribute 407. Each emitted label and
chronological move is compared, not just total counts or final r.

Not covered: adversarial serialized-input validation, a packed executor,
external preparation, floating-point tolerances, arbitrary nonuniform mover
affinities, or singleton-start discovery. None is implied by this receipt.

SHA-256 of the executable text extracted by the replay command:
`2ed6a14e4624415d6846a968ac27ba8df2617ec41c121d00d4fabec7e18c3909`.

Reviewed input SHA-256 snapshots (not implementation hashes):

- Initial candidate: `e198642ae127538a1deec88bd05d1be70f5b4cc38df2ad440f059d21f1181230`.
- Candidate re-read after concurrent lead updates: `c1676f73bb1fdad587a4397e444a133b85ef63551d47520520f603a6ba360583`.
- Predecessor: `8acb188afec75786a0c88a155031e448f9968a88979f3af0365951f629864852`.

The re-read found unchanged theorem conditions and formulas. The added
implementation receipt, external-preparation design and proposed nonuniform
extension are not independently audited here. In particular, this review
does not certify that unimplemented extension. The lead may continue editing
the manuscripts; these hashes identify the reviewed snapshots. No other
files were edited.

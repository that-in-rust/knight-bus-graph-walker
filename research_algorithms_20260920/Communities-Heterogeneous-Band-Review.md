# Independent Review: Heterogeneous Band Certificates

Date: 2026-09-21. Scope: bounded mathematical challenge of A04 only.
Reviewer owns only this file. No lead implementation or test file is read;
no lead module, test or fixture is imported or reused. No agents, source
experiment, literature survey, commit, timing or RSS measurement is part of
this review.

Reviewed candidate SHA-256:
`db0433edbea61a524c8251a1a7ce53a90aad77693bcf63a14958f797c7cba5f2`.
The predecessor theorem and its separate mathematical review were read for
the inherited serial semantics, not used as executable oracles.

## Findings

The stated certificate is mathematically sufficient. No counterexample to
the complete gates was found. In particular, unequal degrees do not invalidate
HHLL: the degree sum is needed to report gains, while the sign certificate
controls decisions. The endpoint-conditioned mover extrema are exact; the
anchor bounds are safe but generally not exact joint maxima. The two-exclusion
prefix procedure is correct with **zero-based ranks** and random-access
prefix sums. It is a constant-operation preparation query, not a streaming
or constant-bit-cost claim.

Eligibility is genuinely enlarged, not merely renamed: every input passing
the old uniform certificate passes these gates, and the independently
constructed positive rational graphs below include new admissions with
unequal affinities, unequal matching-endpoint degrees and unequal pair weights
simultaneously. This says nothing about prevalence in native workloads.

The stronger exact-correlated anchor observation is also sound, with the
constants given below. Its omission can cause false refusals, not false
admissions. No production certifier using that stronger route is delivered here.

Correctness does not establish publication priority, novelty, paper readiness,
usefulness, or an end-to-end resource advantage. Matching-only mover edges,
the supplied stable anchor partition, and the Q-prefix/P-suffix visit order
remain substantial eligibility restrictions. Input discovery and physical
preparation are outside this proof.

## Gain Algebra And Exact Extrema

Use the loopless weighted objective
`Q = sum_C (2*internal_weight(C)/T - gamma*volume(C)^2/T^2)`.
For a move from C to D, direct expansion gives

```text
Delta Q = 2*G/T^2
G = T*(eD-eC) - gamma*du*(VD-VC+du).
```

The `+du` is essential: volumes here are PRE-MOVE volumes and eC excludes
u itself. For a fresh community use `VD=eD=0`, giving
`Gfresh=-T*eC+gamma*du*(VC-du)`. A singleton source has zero gain to a
fresh token, not a strictly improving move. Matching weights are positive,
so every mover has positive degree and T>0; isolated anchors can have du=0.

Fix u, its unique mate v, membership indicators i,j, and cardinality r.
The free Q subset has exactly `t=r-i-j` elements from `Y-{u,v}`. There
are no further restrictions on the subsets quantified by the certificate.
An exchange argument shows that the t-smallest/t-largest degrees minimize
and maximize their sum. Both are attained, including ties, t=0 and t=b-2.
An impossible t is skipped, not treated as a zero-volume state. Other pairs
need not be reachable or still split: quantifying over those extra states
only strengthens the certificate.

For u in P, the switch and fresh gains as functions of `z=VQ` have slopes
`-2*gamma*du` and `-gamma*du`. Consequently switch_min uses zmax;
switch_max and fresh_max use zmin. Here
`eD-eC=beta-alpha+(w if j else -w)` and `eC=alpha+(w if not j else 0)`.
For u in Q the two slopes are positive, so switch_min uses zmin and both
maxima use zmax, with
`eD-eC=alpha-beta+(w if not j else -w)` and `eC=beta+(w if j else 0)`.
This reproduces all six formulas, including every sign and endpoint degree.

**Endpoint-conditioning caution.** A genuinely enlarged subset domain gives
safe looser bounds. Omitting fixed endpoint contributions while selecting t
other degrees does not. For example, degrees `(1,9,2,3)`, u=0, v=1,
`i=0,j=1,r=1,Aq=10` give exactly z=19, not z=10. With T=40,
gamma=1 and `beta-alpha+w=-1/4`, the correct P-switch gain is -9;
the incorrectly omitted mate contribution gives +9. These scalar values
can be realized with `w=1/4,alpha=5/8,beta=1/8` at u. This is a witness
against the miscomputed bound, not against the candidate's conditioned bound.

**Lead erratum, 2026-09-21:** the original illustrative tuple was
`w=1/4,alpha=3/4,beta=0`, whose affinity difference is -1/2, not -1/4.
A new regression failed with actual switch -19 versus stated -9. The tuple
above repairs that arithmetic while retaining du=1. The lead also constructed
an explicit eight-vertex graph with T=40, Aq=10 and mover degrees (1,9,2,3),
then checked the -9 switch against a direct modularity difference. The
independent scalar checker below already used -1/4 and was unaffected; its
new two assertions connect that value to the corrected tuple. No kernel,
certificate inequality or frozen experiment result changed.

## Anchors And Correlation

For a size-r Q subset S let `a(S)=sum_S w_uv` and `d(S)=sum_S dv`.
For a P anchor, the switch gain increases with a and decreases with d;
the fresh gain has the same directions. Thus combining `a<=top(r)` with
`Aq+d>=Zmin` yields the stated two upper bounds. For a Q anchor use
`a>=W-top(b-r)` and `Aq+d<=Zmax`. This proves its two bounds without
assuming that their constituent extrema occur on one common subset.
Missing mover edges are zero positions; top(t) saturates at W once t
exceeds the number of nonzero incident edges. No b-sized zero array is needed.

For completeness set `x_v=T*w_uv-gamma*du*dv` and let Xmax(r), Xmin(r)
be the r-largest and r-smallest sums of x. Combining terms first yields
the following exact correlated maxima:

```text
P switch: T*(Iq-Ip-W) - gamma*du*(2*Aq-T+du) + 2*Xmax(r)
P fresh:  -T*(Ip+W) + gamma*du*(T-Aq-du) + Xmax(r)
Q switch: T*(Ip-Iq+W) - gamma*du*(T-2*Aq+du) - 2*Xmin(r)
Q fresh:  -T*Iq + gamma*du*(Aq-du) - Xmin(r).
```

Within one anchor side both maxima can use the same maximizing subset.
For du=0 they still hold. The independent finite check compares all four
identities to enumerated common subsets, and verifies that every conservative
upper bound dominates the exact one. Naively sorting b scores for every
anchor costs `O(|anchors|*b*log b)` comparisons, in addition to other work;
these costs cannot be charged to the scan executor. No sparse-exception
algorithm or useful extra full-graph admission is asserted by this observation.

**Concrete looseness witness (an actual graph).** Let P0={u,p}, Q0={q},
Y={a,b}, with edges `(u,p,1),(u,a,1),(q,a,2),(a,b,1)` and gamma=1.
For anchor u at r=1, `T=10,du=2,Aq=2,(da,db)=(4,1)`.
For Q mover subsets {a} and {b}, respectively, its switch gains are -8,-16
and fresh gains -6,-10. Thus its exact maxima are -8,-6. The conservative
maxima are 4,0: they reject this individually stable anchor because the
maximum incident weight and minimum degree sum belong to different subsets.
The replay checks all four gains by direct objective differences. This is
not a claim that the complete graph passes the remaining gates, or that the
stronger anchor route alone makes the graph streamable.

## Closed Band And HHLL

Induct jointly on visits, rather than presupposing anchor stability:
initially all anchors are in their declared communities and `r=k=r0-1`.
At a visit, the appropriate feasible configuration is among the tested
states. Anchor moves and fresh moves have nonpositive gain. Same-side mates
stay; split mates have exactly the required P-only / both / Q-only signs
at the lower / middle / upper cardinality. A successful move reunites one
pair and changes r by one toward or within the band. It cannot split any
other pair. Strict positivity is necessary for required completions; zero
is safely permitted only for blocked moves. This proves the induction.

Each remaining split pair still consists of its original low/Q endpoint
and high/P endpoint. Initially all eligible lows are blocked, so the first
two completions are H,H unless fewer than two pairs remain. At the upper
state the next eligible low is behind the visit cursor, so it waits for the
next sweep. Two L completions reach the lower state; the next high is later
in that same sweep. Stable interleaved anchors insert only no-ops.
Therefore the length-c HHLL prefix and zero-based sweep formula
`1+(j+2)//4` follow exactly as before. For c=0 the first sweep terminates;
otherwise the total including the last no-move sweep is `2+(c+1)//4`.

Degrees do not enter this order argument. They do enter `DQ`: add only the
moving endpoint's du on H, subtract only its du on L. The mate does not move
on that visit. An old common-d update can give wrong numerical gains even
when it accidentally leaves every decision correct. A live crossing pair
has neither endpoint consumed; stale records inherit the selecting side's
final label. Frontiers mean consumed rank, not I/O read-ahead or ID distance.

The checker exhausts all perfect matchings and all allowed k for b=2,4,6,8.
It compares a visit-by-visit sign-table process to an independent two-pointer
HHLL simulation, including unequal-degree volume updates, stale labels,
sweeps, zero crossings and truncated HHLL phases. Actual graph trajectories
are separately checked using direct modularity-objective differences.

## Two-Exclusion Prefix Proof

Let sorted zero-based excluded ranks be e1<e2, and start L=t. Process them
in that order, extending L iff e<L. If the rank is in the current prefix,
its removal leaves one too few retained elements, requiring one replacement.
If it is outside, processing a later, larger rank cannot subsequently pull
it into the prefix: that later rank is also outside unless the earlier
extension already occurred. Thus one pass handles cascaded exclusions.
At the end `L-number_of_exclusions_below_L=t`, and all retained prefix
values precede every retained suffix value. Subtract excluded prefix values
from prefix[L]. Since `t<=b-2`, L never exceeds b. At most two comparisons,
extensions and subtractions suffice. The largest-t sum is total excluding
both ranks minus the smallest-(b-2-t) sum. Ties change no sum; sorting by
ID merely fixes rank deterministically.

This includes b=2, t=0, both extremes of t, consecutive exclusions, and an
exclusion exactly at the initial prefix boundary. One-based ranks would
require a changed comparison. Random-access prepared arrays and exact
arithmetic are assumed; variable-bit operands do not have unit physical cost.
There are only a constant number of (r,i,j) queries per mover. Consequently
one `O(b log b)` degree sort and O(b) prefix/rank construction suffice for
all mover gates. Sorting each anchor's s nonzero weights costs
`O(s log(1+s))`; its subsequent top and volume queries are constant in number.
With valid source/role lookup, degree/affinity accumulation costs O(n+M).
This supports the stated arithmetic/comparison bound, with ID ordering and
source validation still paid as appropriate. It does not supply an external
bounded-memory builder or validate the concurrently written implementation.

## Genuine Eligibility Witness

Construct b movers, b in {4,6,8}, and just two anchors per side. Connect the
two anchors on each side with weight `(3*b+1)/4`. Connect every mover to
each of the four anchors with weight b/2; use any perfect matching of weight
1 on movers. Set gamma=1/2 and `k=b/2-1`. Then

```text
d=2*b+1, T=(2*b+1)^2, Ap=Aq, r0=b/2,
f0=0, a=h=T/2.
```

The old strict band holds. All stability tests are strict in these base
graphs (checked independently below), so continuity gives a nonempty open
neighborhood of positive edge weights on the same support that still passes.
The extrema are finite maxima/minima of continuous expressions; equal-degree
ties at the base do not obstruct this continuity argument.
Perturb mover i's edge to the first P anchor by `epsilon*(i+1)`, its edge
to the second Q anchor by `epsilon*(2*i+1)`, and matching pair j by
`epsilon*(j+1)`. For nonzero epsilon, both affinities vary, pair weights
vary, and within each pair the endpoint degree difference is
`3*epsilon*(i-i')`, which is nonzero. These defeat the old uniform-signature
requirement, not merely its implementation. Tiny rational epsilons explicitly
realize the continuity argument. The checker also tries larger perturbations
without discarding refused cases. No leading-source seed or fixture is used.

For inclusion of the entire old admitted class, when all mover degrees are
d every feasible conditioned volume is `Aq+r*d`; the new mover gates reduce
to the old proven signs. The old singleton bound dropped a nonnegative own
mate affinity, so it implies the new bound. Anchor extrema then have fixed
volume and agree exactly with the old anchor bounds. Thus this is a proper
superset of the old admitted class under the common source/ordering contract,
not a claim that every heterogeneous graph qualifies.

## Independent Replay And Counts

The Python below is embedded, self-contained and standard-library-only.
It writes no files, imports no repository modules and uses no random search.
It uses exact fractions; the expanded oracle computes Q before and after
each candidate move rather than importing a gain helper from the candidate.

Finite scope is deliberately small and explicit:

- Exclusions: all sorted multisets over {1,2,4}, lengths 2 through 8,
  every pair of excluded ranks and every feasible t. Each query checks both
  the smallest and largest sum, so 16,632 queries check 33,264 extrema.
- Movers: all sorted degree multisets over {1,2,4}, b=2,4,6, every ordered
  choice of u,v, and every binary membership vector. Synthetic scalar
  instances use the alpha,beta,T,Aq,gamma rules written in the checker.
  Each of the 19,008 feasible configurations checks two volume extrema,
  two switch extrema and one fresh maximum; 8,256 impossible configurations
  are explicitly rejected. These are algebra checks, not 19,008 graphs.
- Anchors: b=2,3,4, every ordered vector of the five (weight,degree) choices
  in the checker, every r and both sides. Each of 7,400 side configurations
  checks two exact correlated maxima and two conservative inequalities.
  The 329 exact-safe/conservative-positive components are single component
  comparisons, not full-graph admissions. The separate five-vertex witness
  above supplies a graph-realizable example of the correlation loss.
- Schedules: all perfect matchings for b=2,4,6,8 and every k=0,...,b-2,
  using degrees `(i+1)^2+1`. These 820 cases are sign-table replays, not
  claims that all those degree vectors pass a realizable graph certificate.
- Graphs: b=4,6,8; first, middle and last matchings in the deterministic
  recursion below; epsilon in {0,1/1000,-1/1000,1/10,1}. All 45 attempts
  remain in the counts. There are 30 admissions (9 uniform, 21 genuinely
  heterogeneous) and 15 refusals. Both tiny signed perturbations pass in all
  nine support/order cases. All admitted complete trajectories, exact
  accepted gains, terminal sweep counts and 294 final labels agree. Refused
  cases are not classified as trajectory failures or false refusals.

Observed output (Python with `-B -W error`, exit 0):

```json
{
  "anchor_extrema_side_configurations": 7400,
  "anchor_subset_side_samples": 22200,
  "conservative_refused_exact_safe_components": 329,
  "correlation_witness_objective_differences": 4,
  "direct_objective_differences": 1260,
  "endpoint_extrema_configurations": 19008,
  "endpoint_subset_samples": 56688,
  "exclusion_queries_two_extrema": 16632,
  "expanded_graph_admissions": 30,
  "expanded_graph_candidates": 45,
  "expanded_graph_exact_moves": 45,
  "expanded_graph_final_labels": 294,
  "expanded_graph_positive_even_crossings": 6,
  "expanded_graph_refusals": 15,
  "expanded_graph_zero_crossings": 3,
  "frontier_stale_records": 1252,
  "genuinely_heterogeneous_admissions": 21,
  "impossible_endpoint_configurations": 8256,
  "matching_schedule_cases": 820,
  "matching_schedule_labels": 6368,
  "matching_schedule_moves": 1252,
  "matching_schedule_zero_crossings": 170,
  "strict_conservative_slack_components": 6136,
  "strict_uniform_base_graphs": 9
}
```

The 30 admitted graphs include 6 positive-even-crossing graphs and 3
zero-crossing graphs, with 45 accepted moves in total. All graph IDs place
anchors among movers in serial order. Objective-difference counts include
the terminal no-move sweep. None of these numbers is transferred from the
predecessor receipt or the concurrent lead's experiment.

```sh
awk '/^<!-- HETEROGENEOUS-REVIEW-CHECKER-START -->/{p=1;next} /^<!-- HETEROGENEOUS-REVIEW-CHECKER-END -->/{p=0} p && !/^```/' \
  research_algorithms_20260920/Communities-Heterogeneous-Band-Review.md | python3 -B -W error -
```

<!-- HETEROGENEOUS-REVIEW-CHECKER-START -->
```python
from collections import Counter, defaultdict
from fractions import Fraction as F
from itertools import combinations, combinations_with_replacement, product
import json

counts = Counter()


def sum_two_excluded_smallest(values, prefix, e1, e2, t):
    length = t
    for rank in (e1, e2):
        if rank < length:
            length += 1
    return prefix[length] - sum(values[e] for e in (e1, e2) if e < length)


def verify_degree_exclusion_queries():
    for b in range(2, 9):
        for values in combinations_with_replacement((1, 2, 4), b):
            prefix = [0]
            for value in values:
                prefix.append(prefix[-1] + value)
            for e1, e2 in combinations(range(b), 2):
                remaining = [d for i, d in enumerate(values) if i not in (e1, e2)]
                for t in range(b - 1):
                    low = sum_two_excluded_smallest(values, prefix, e1, e2, t)
                    high = (sum(remaining) - sum_two_excluded_smallest(
                        values, prefix, e1, e2, b - 2 - t))
                    assert low == sum(remaining[:t])
                    assert high == sum(remaining[len(remaining) - t:])
                    counts['exclusion_queries_two_extrema'] += 1


def calculate_mover_extrema_bounds(ds, u, v, i, j, r, aq, total,
                                   gamma, alpha, beta, weight):
    t = r - i - j
    if not 0 <= t <= len(ds) - 2:
        return None
    rest = sorted(d for q, d in enumerate(ds) if q not in (u, v))
    zmin = aq + i*ds[u] + j*ds[v] + sum(rest[:t])
    zmax = aq + i*ds[u] + j*ds[v] + sum(rest[len(rest) - t:])
    d = ds[u]
    if not i:
        e = beta - alpha + (weight if j else -weight)
        low = total*e - gamma*d*(2*zmax - total + d)
        high = total*e - gamma*d*(2*zmin - total + d)
        fresh = -total*(alpha + (weight if not j else 0)) + gamma*d*(total-zmin-d)
    else:
        e = alpha - beta + (weight if not j else -weight)
        low = total*e - gamma*d*(total - 2*zmin + d)
        high = total*e - gamma*d*(total - 2*zmax + d)
        fresh = -total*(beta + (weight if j else 0)) + gamma*d*(zmax-d)
    return zmin, zmax, low, high, fresh


def verify_endpoint_subset_extrema():
    for b in (2, 4, 6):
        for ds in combinations_with_replacement((1, 2, 4), b):
            for u in range(b):
                for v in range(b):
                    if u == v:
                        continue
                    alpha = (ds[u]-F(1, 3))*F(u+1, b+1)
                    beta = ds[u]-F(1, 3)-alpha
                    total, aq, gamma, weight = 2*sum(ds)+37, 11, F(3, 5), F(1, 3)
                    groups = defaultdict(list)
                    for bits in product((0, 1), repeat=b):
                        i, j, r = bits[u], bits[v], sum(bits)
                        z = aq + sum(d*q for d, q in zip(ds, bits))
                        own = (beta if i else alpha) + (weight if i == j else 0)
                        other = (alpha if i else beta) + (weight if i != j else 0)
                        vc, vd = (z, total-z) if i else (total-z, z)
                        switch = total*(other-own) - gamma*ds[u]*(vd-vc+ds[u])
                        fresh = -total*own + gamma*ds[u]*(vc-ds[u])
                        groups[r, i, j].append((z, switch, fresh))
                        counts['endpoint_subset_samples'] += 1
                    for r, i, j in product(range(b+1), (0, 1), (0, 1)):
                        bound = calculate_mover_extrema_bounds(
                            ds, u, v, i, j, r, aq, total, gamma, alpha, beta, weight)
                        samples = groups.get((r, i, j))
                        if not samples:
                            assert bound is None
                            counts['impossible_endpoint_configurations'] += 1
                            continue
                        zs, switches, freshes = zip(*samples)
                        assert bound == (min(zs), max(zs), min(switches), max(switches), max(freshes))
                        counts['endpoint_extrema_configurations'] += 1
    assert F(5, 8) + F(1, 8) + F(1, 4) == 1
    assert F(1, 8) - F(5, 8) + F(1, 4) == F(-1, 4)
    assert 40*F(-1, 4) - (2*19-40+1) == -9
    assert 40*F(-1, 4) - (2*10-40+1) == 9


def calculate_anchor_extrema_bounds(weights, ds, ip, iq, aq, total, gamma, r, side):
    w, d = sum(weights), ip+iq+sum(weights)
    zmin = aq + sum(sorted(ds)[:r])
    zmax = aq + sum(sorted(ds, reverse=True)[:r])
    top = sum(sorted(weights, reverse=True)[:r if side == 0 else len(ds)-r])
    x = sorted(total*a - gamma*d*b for a, b in zip(weights, ds))
    if side == 0:
        xs = sum(x[len(x)-r:])
        conservative = (total*(iq-ip-w+2*top) - gamma*d*(2*zmin-total+d),
                        -total*(ip+w-top) + gamma*d*(total-zmin-d))
        exact = (total*(iq-ip-w) - gamma*d*(2*aq-total+d) + 2*xs,
                 -total*(ip+w) + gamma*d*(total-aq-d) + xs)
    else:
        xs = sum(x[:r])
        conservative = (total*(ip-iq-w+2*top) - gamma*d*(total-2*zmax+d),
                        -total*(iq+w-top) + gamma*d*(zmax-d))
        exact = (total*(ip-iq+w) - gamma*d*(total-2*aq+d) - 2*xs,
                 -total*iq + gamma*d*(aq-d) - xs)
    return conservative, exact


def verify_anchor_correlated_extrema():
    positions = ((0, 1), (0, 3), (1, 1), (1, 4), (3, 4))
    for b in (2, 3, 4):
        for entries in product(positions, repeat=b):
            weights, ds = zip(*entries)
            ip, iq, gamma = 3, 2, F(2, 3)
            d = ip+iq+sum(weights)
            aq, total = d+5, 2*sum(ds)+2*d+17
            for r in range(b+1):
                for side in (0, 1):
                    samples = []
                    for subset in combinations(range(b), r):
                        z = aq + sum(ds[v] for v in subset)
                        a = sum(weights[v] for v in subset)
                        p, q = ip+sum(weights)-a, iq+a
                        own, other = (p, q) if side == 0 else (q, p)
                        vc, vd = (total-z, z) if side == 0 else (z, total-z)
                        samples.append((total*(other-own)-gamma*d*(vd-vc+d),
                                        -total*own+gamma*d*(vc-d)))
                        counts['anchor_subset_side_samples'] += 1
                    conservative, exact = calculate_anchor_extrema_bounds(
                        weights, ds, ip, iq, aq, total, gamma, r, side)
                    observed = tuple(map(max, zip(*samples)))
                    assert observed == exact
                    assert all(a >= b for a, b in zip(conservative, exact))
                    counts['anchor_extrema_side_configurations'] += 1
                    counts['strict_conservative_slack_components'] += sum(
                        a > b for a, b in zip(conservative, exact))
                    counts['conservative_refused_exact_safe_components'] += sum(
                        a > 0 >= b for a, b in zip(conservative, exact))


def enumerate_all_perfect_matchings(vertices):
    if not vertices:
        yield ()
        return
    u = vertices[0]
    for v in vertices[1:]:
        rest = tuple(x for x in vertices if x not in (u, v))
        for pairs in enumerate_all_perfect_matchings(rest):
            yield ((u, v),) + pairs


def simulate_serial_sign_schedule(b, k, pairs, ds):
    mate = dict(pairs)
    mate.update((v, u) for u, v in pairs)
    labels, trace = [int(u < k) for u in range(b)], []
    sweep, dq = 0, sum(ds[:k])
    while True:
        sweep += 1
        changed = False
        for u in range(b):
            r, side = sum(labels), labels[u]
            if side == labels[mate[u]]:
                continue
            eligible = (not side and r <= k+1) or (side and r >= k+1)
            if eligible:
                labels[u] = 1-side
                trace.append((u, 1-side, sweep, dq))
                dq += ds[u] if side == 0 else -ds[u]
                assert dq == sum(d for d, q in zip(ds, labels) if q)
                assert k <= sum(labels) <= k+2
                changed = True
        if not changed:
            return trace, labels, sweep
        assert sweep <= b+1


def simulate_two_frontier_schedule(b, k, pairs, ds):
    mate = dict(pairs)
    mate.update((v, u) for u, v in pairs)
    streams = (list(range(k, b)), list(range(k)))
    cursors, trace, labels = [0, 0], [], [None]*b
    crossing = [(u, v) for u, v in pairs if (u < k) != (v < k)]
    dq = sum(ds[:k])
    for j in range(len(crossing)):
        side = (j//2) % 2
        while True:
            assert cursors[side] < len(streams[side])
            u = streams[side][cursors[side]]
            cursors[side] += 1
            v = mate[u]
            if (u < k) == (v < k):
                labels[u] = int(u < k)
                continue
            opposite_rank = v if side == 0 else v-k
            if opposite_rank < cursors[1-side]:
                labels[u] = side
                counts['frontier_stale_records'] += 1
                continue
            labels[u] = 1-side
            trace.append((u, 1-side, 1+(j+2)//4, dq))
            dq += ds[u] if side == 0 else -ds[u]
            break
    for side in (0, 1):
        for u in streams[side][cursors[side]:]:
            v = mate[u]
            if (u < k) == (v < k):
                labels[u] = int(u < k)
            else:
                opposite_rank = v if side == 0 else v-k
                assert opposite_rank < cursors[1-side]
                labels[u] = side
                counts['frontier_stale_records'] += 1
    sweeps = 1 if not crossing else 2+(len(crossing)+1)//4
    return trace, labels, sweeps


def verify_all_matching_schedules():
    for b in (2, 4, 6, 8):
        for pairs in enumerate_all_perfect_matchings(tuple(range(b))):
            for k in range(b-1):
                ds = [(i+1)**2+1 for i in range(b)]
                actual = simulate_serial_sign_schedule(b, k, pairs, ds)
                predicted = simulate_two_frontier_schedule(b, k, pairs, ds)
                assert actual == predicted
                counts['matching_schedule_cases'] += 1
                counts['matching_schedule_moves'] += len(actual[0])
                counts['matching_schedule_labels'] += b
                counts['matching_schedule_zero_crossings'] += not actual[0]


def construct_independent_weighted_graph(b, pairs, epsilon):
    anchors = (-7, 7, 17, 37)
    movers = tuple(10*i for i in range(b))
    graph = {u: {} for u in anchors+movers}
    def insert_positive_weighted_edge(u, v, w):
        assert u != v and v not in graph[u] and w > 0
        graph[u][v] = graph[v][u] = F(w)
    insert_positive_weighted_edge(anchors[0], anchors[1], F(3*b+1, 4))
    insert_positive_weighted_edge(anchors[2], anchors[3], F(3*b+1, 4))
    for i, u in enumerate(movers):
        for j, a in enumerate(anchors):
            perturb = i+1 if j == 0 else 2*i+1 if j == 3 else 0
            insert_positive_weighted_edge(u, a, F(b, 2)+epsilon*perturb)
    for j, (u, v) in enumerate(pairs):
        insert_positive_weighted_edge(movers[u], movers[v], 1+epsilon*(j+1))
    return graph, anchors, movers


def certify_independent_graph_bounds(graph, anchors, movers, pairs, k):
    degree = {u: sum(row.values()) for u, row in graph.items()}
    ds, total, gamma = [degree[u] for u in movers], sum(degree.values()), F(1, 2)
    aq = sum(degree[u] for u in anchors[2:])
    strict, blocked = [], []
    for r in (k, k+1, k+2):
        for a, b in pairs:
            for u, v in ((a, b), (b, a)):
                alpha = sum(graph[movers[u]].get(a, 0) for a in anchors[:2])
                beta = sum(graph[movers[u]].get(a, 0) for a in anchors[2:])
                weight = graph[movers[u]][movers[v]]
                for i, j in product((0, 1), repeat=2):
                    extrema = calculate_mover_extrema_bounds(
                        ds, u, v, i, j, r, aq, total, gamma, alpha, beta, weight)
                    if extrema is None:
                        continue
                    _, _, low, high, fresh = extrema
                    blocked.append(fresh)
                    eligible = i != j and ((i == 0 and r <= k+1) or (i == 1 and r >= k+1))
                    (strict if eligible else blocked).append(low if eligible else high)
        for u in anchors:
            weights = [graph[u].get(v, 0) for v in movers]
            ip = sum(graph[u].get(v, 0) for v in anchors[:2])
            iq = sum(graph[u].get(v, 0) for v in anchors[2:])
            upper, _ = calculate_anchor_extrema_bounds(
                weights, ds, ip, iq, aq, total, gamma, r, int(u in anchors[2:]))
            blocked.extend(upper)
    return all(v > 0 for v in strict) and all(v <= 0 for v in blocked), min(strict), max(blocked)


def evaluate_full_partition_objective(graph, labels, gamma):
    volumes, internal = Counter(), F(0)
    for u, row in graph.items():
        volumes[labels[u]] += sum(row.values())
        internal += sum(w for v, w in row.items() if labels[v] == labels[u])
    total = sum(volumes.values())
    return internal/total - gamma*sum(v*v for v in volumes.values())/total**2


def verify_correlated_anchor_witness():
    graph = {u: {} for u in range(5)}
    for u, v, w in ((0, 1, 1), (0, 3, 1), (2, 3, 2), (3, 4, 1)):
        graph[u][v] = graph[v][u] = F(w)
    samples = []
    for selected in (3, 4):
        labels = {0: 0, 1: 0, 2: 1, 3: int(selected == 3), 4: int(selected == 4)}
        before = evaluate_full_partition_objective(graph, labels, F(1))
        gains = []
        for target in (1, 2):
            changed = labels.copy()
            changed[0] = target
            gains.append(50*(evaluate_full_partition_objective(graph, changed, F(1))-before))
            counts['correlation_witness_objective_differences'] += 1
        samples.append(tuple(gains))
    assert samples == [(-8, -6), (-16, -10)]
    conservative, exact = calculate_anchor_extrema_bounds(
        (1, 0), (4, 1), 1, 0, 2, 10, F(1), 1, 0)
    assert conservative == (4, 0) and exact == (-8, -6)


def simulate_full_objective_trajectory(graph, initial):
    labels, trace, sweep = initial.copy(), [], 0
    while True:
        sweep += 1
        changed = False
        for u in sorted(graph):
            before = evaluate_full_partition_objective(graph, labels, F(1, 2))
            options = []
            for destination in (1-labels[u], 2):
                proposal = labels.copy()
                proposal[u] = destination
                gain = evaluate_full_partition_objective(graph, proposal, F(1, 2))-before
                options.append((gain, destination))
                counts['direct_objective_differences'] += 1
            gain, destination = max(options)
            if gain > 0:
                assert destination != 2
                labels[u] = destination
                trace.append((u, destination, sweep, gain))
                changed = True
        if not changed:
            return trace, labels, sweep
        assert sweep <= len(graph)+1


def verify_heterogeneous_graph_admissions():
    for b in (4, 6, 8):
        matchings = list(enumerate_all_perfect_matchings(tuple(range(b))))
        for index in sorted({0, len(matchings)//2, len(matchings)-1}):
            pairs, k = matchings[index], b//2-1
            for epsilon in (F(0), F(1, 1000), F(-1, 1000), F(1, 10), F(1)):
                graph, anchors, movers = construct_independent_weighted_graph(b, pairs, epsilon)
                counts['expanded_graph_candidates'] += 1
                admitted, minimum, maximum = certify_independent_graph_bounds(graph, anchors, movers, pairs, k)
                if not epsilon:
                    assert admitted and minimum > 0 and maximum < 0
                    counts['strict_uniform_base_graphs'] += 1
                if abs(epsilon) == F(1, 1000):
                    assert admitted
                if not admitted:
                    counts['expanded_graph_refusals'] += 1
                    continue
                degree = {u: sum(row.values()) for u, row in graph.items()}
                ds, total = [degree[u] for u in movers], sum(degree.values())
                if epsilon:
                    alphas = [sum(graph[u].get(a, 0) for a in anchors[:2]) for u in movers]
                    betas = [sum(graph[u].get(a, 0) for a in anchors[2:]) for u in movers]
                    weights = [graph[movers[u]][movers[v]] for u, v in pairs]
                    assert len(set(alphas)) == len(set(betas)) == b
                    assert len(set(weights)) == b//2
                    assert all(ds[u] != ds[v] for u, v in pairs)
                    counts['genuinely_heterogeneous_admissions'] += 1
                aq = sum(degree[u] for u in anchors[2:])
                initial = {a: int(a in anchors[2:]) for a in anchors}
                initial.update((u, int(i < k)) for i, u in enumerate(movers))
                actual_trace, actual_labels, actual_sweeps = simulate_full_objective_trajectory(graph, initial)
                schedule, labels, sweeps = simulate_serial_sign_schedule(b, k, pairs, ds)
                predicted = []
                for rank, target, sweep, dq in schedule:
                    u = movers[rank]
                    alpha = sum(graph[u].get(a, 0) for a in anchors[:2])
                    beta = sum(graph[u].get(a, 0) for a in anchors[2:])
                    v = next(v if a == rank else a for a, v in pairs if rank in (a, v))
                    weight, z = graph[u][movers[v]], aq+dq
                    if target == 1:
                        scaled = total*(beta-alpha+weight) - F(1, 2)*degree[u]*(2*z-total+degree[u])
                    else:
                        scaled = total*(alpha-beta+weight) - F(1, 2)*degree[u]*(total-2*z+degree[u])
                    predicted.append((u, target, sweep, 2*scaled/total**2))
                expected_labels = {a: initial[a] for a in anchors}
                expected_labels.update(zip(movers, labels))
                assert (actual_trace, actual_labels, actual_sweeps) == (predicted, expected_labels, sweeps)
                counts['expanded_graph_admissions'] += 1
                counts['expanded_graph_exact_moves'] += len(actual_trace)
                counts['expanded_graph_final_labels'] += len(actual_labels)
                counts['expanded_graph_zero_crossings'] += not actual_trace
                counts['expanded_graph_positive_even_crossings'] += bool(actual_trace) and len(actual_trace) % 2 == 0


verify_degree_exclusion_queries()
verify_endpoint_subset_extrema()
verify_anchor_correlated_extrema()
verify_all_matching_schedules()
verify_correlated_anchor_witness()
verify_heterogeneous_graph_admissions()
assert counts['genuinely_heterogeneous_admissions'] > 0
assert counts['expanded_graph_refusals'] > 0
print(json.dumps(dict(sorted(counts.items())), indent=2))
```
<!-- HETEROGENEOUS-REVIEW-CHECKER-END -->

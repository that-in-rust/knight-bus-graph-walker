# Single-Flip Community Trace: Independent Review

Date: 2026-09-21. Scope: independent mathematical sidecar, not a review of
the lead implementation. The only project input read was
`Communities-Single-Flip-Trace-Verification.md`. This file is the only owned
output. No implementation files or heterogeneous-source fixtures were read.

## Premise Check And Verdict

**Conditional mathematical approval, not an innovation or low-RAM claim.**
The sampled-volume verifier is sound and complete for admissible proposals
under the stated exact serial semantics, provided `T>0` and the structural
checks below are enforced. The chronological range-threshold control is also
sound and complete, with a better resident arithmetic-operation bound and no
two-dimensional index. No counterexample to either correctly specified
verifier was found. Concrete counterexamples to ten tempting simplifications
are given below and emitted by the checker.

There is one specification gap: positive weights on stored edges do not
exclude an empty edge set. Modularity `2G/T^2` is undefined when `T=0`.
Either require `M>0`, or separately define the edgeless behavior. This review
requires `T>0`; it does not silently call an undefined objective zero.
Isolated vertices in a graph with `T>0` are supported and always stay.

The completeness claim is **relative to the proposal language**, not to all
community-detection runs. A real run that creates a fresh community, empties
P or Q, or moves a vertex twice has no admissible proposal. Refusing every
proposal on that input is correct, not an incompleteness bug. Conversely, a
verifier must not suppress such a real move to keep its simulated run inside
the language. It must reject the incompatible proposal.

Expert lenses used: modularity algebra, discrete event/sampling semantics,
adversarial certification, and resident/external-memory cost accounting.
The strongest adversarial comparison here is not dense replay: it is the
equally informed threshold-tree verifier. The lead's separate proposal-free
solver, including fresh/empty/budget handoffs, is outside this review.

| Candidate | Exact under this contract? | Main qualification |
|---|---|---|
| Direct objective replay | Yes | Independent correctness oracle, expensive |
| Accepted-points-only checking | No | Omits profitable skipped visits |
| Sampled-volume range index | Yes | Exact sampled extrema, full construction charged |
| Neighbor-only revisit queue | Not generally | Global volume affects nonneighbors |
| Chronological threshold tree | Yes | Stronger resident control; graph storage remains |

## Exact Decision Algebra

Write `d=d_u`, `a=sum_{v in Q} w_uv`, `z=V_Q`, all before the visit.
Use the scaled objective

```text
H(C) = T^2 Q_gamma(C)
     = 2T sum_{edges internal to C} w - gamma sum_communities V_C^2.
```

Moving u from A to B changes this objective by

```text
Delta H = 2 [T(k_B-k_A) - gamma*d*(V_B-V_A+d)] = 2G.
```

For a fresh singleton, substitute `V_B=k_B=0`, while the source volume
still includes u. Substituting `V_P=T-z`, `k_P=d-a`, `V_Q=z`, `k_Q=a`
gives exactly the four gain expressions in section 1 of the source note.
This derivation charges no hypothetical removal before measuring z.

For P, the slopes of `(G_switch,G_fresh)` with respect to z are
`(-2 gamma d, -gamma d)`; for Q they are `(2 gamma d, gamma d)`.
Thus, on a nonempty set of skipped sampled visits with fixed label and a,
the **same** extremum maximizes both gains: minimum z for P, maximum z for Q.
Both maxima must be nonpositive. When `d=0`, both gains are identically zero,
so either extremum works and threshold division must be bypassed.

An accepted switch requires exactly `G_switch>0` and
`G_switch>=G_fresh`. A zero tie stays. A positive tie chooses the existing
opposite community. These are not interchangeable strictness conventions.
If the source is a singleton its fresh gain is zero, so relabeling it with
a fresh token cannot become an accepted zero-gain move. Community nonemptiness
is a vertex-count condition, not a positive-volume condition: an existing
community containing only isolates is still an eligible tie winner.

## Sampled-Index Proof

Let `J=sum_{v that flip} deg_records(v)`, so `J<=2M`. Weighted degree d
and adjacency-record count `deg_records` are different quantities.

1. **Hypothetical states.** Apply precisely the proposal prefix strictly
   earlier than `(s,u)` in lexicographic visit order. This uniquely defines
   the labels, a, and z at every proposed or skipped visit, even for a proposal
   that will later be rejected. Deriving this state does not assume validity.
2. **Affinity timing.** A neighbor v's flip at `(s_v,v)` affects u at sweep
   `s_v` iff `v<u`, otherwise at `s_v+1`. This follows directly by comparing
   the two visit pairs. Equal IDs cannot be neighbors. There are J update
   records. Same-sweep updates may cancel and must be summed exactly; their
   intermediate values are not sampled. Sweep-1 updates precede the first
   evaluation. u's own observed label changes only at `s_u+1`.
3. **Complete partition into checks.** Cut at effective affinity changes,
   at `s_u` and `s_u+1`, and at endpoints 1 and `S+1`. Isolate the accepted
   visit and check every other nonempty interval. There are at most
   `n+J+2f` nonempty segments before removing accepted points. Empty
   intervals impose no test. The terminal sweep must be covered.
4. **Volume pieces.** In each sweep, a flip at rank j ends the old-volume
   piece at j inclusive. Add its signed degree only for subsequent ranks.
   After the last rank there can be an empty suffix: omit that piece but
   retain the updated scalar for the next sweep. A move-free sweep has one
   full-rank piece. Consequently the number of nonempty pieces is at most
   `f+S<=2f+1`; no padding to an n-by-S grid is needed.
5. **Canonical-node bijection.** Decomposing a piece's rank interval into
   disjoint canonical tree nodes places exactly one of those nodes on the
   ancestor path of each covered rank, and none on paths outside it. Since
   pieces partition each sweep, each `(s,u)` contributes exactly once across
   u's ancestor lists. In particular two distinct pieces of the same sweep
   cannot put entries into the same canonical node. Restricting every list
   to sweep interval `[a,b]` therefore selects precisely u's sampled visits,
   neither other ranks nor intervening event states. Min/max of their union
   is exact. An empty list contributes no value, never numeric zero.
6. **Interval equivalence.** The monotone affine formulas make the extrema
   test equivalent to checking both gains at every skipped visit, in both
   directions. Volume need not be monotone in sweep or event order. Temporal
   endpoints alone do not suffice.
7. **Induction.** Before the first visit the real and hypothetical states
   coincide. If they agree before a visit, the relevant interval or point
   check gives exactly the real deterministic action; hence they agree after
   it. Every nonterminal sweep has a positive accepted action, preventing an
   earlier stop. The separately checked terminal sweep has none. Therefore
   acceptance implies equality of the entire trace and stop count. Conversely,
   if the real trace is the supplied admissible trace, every point check and
   every constituent skipped-visit inequality holds; exact extrema cannot
   introduce a false refusal. This proves completeness.

Structural validation is part of this theorem: increasing distinct original
IDs/ranks, a valid simple positive-rational source, positive gamma, initial
nonempty P/Q, strictly sorted event pairs, at most one event per vertex,
opposite-initial destinations, nonempty communities at every prefix, events
only in sweeps `1..S-1`, every such sweep represented, and `S=1` when `f=0`.
Reject unsorted proposals; sorting them on behalf of the proposer would
accept a malformed certificate. Extra trailing empty sweeps are also wrong.

## Chronological Threshold Control

The additional control supplied by the lead agrees algebraically with the
one independently derived here. For `d>0`, its exact stay thresholds are:

```text
P: z >= L = max((T-d)/2 + T(2a-d)/(2 gamma d),
                T-d - T(d-a)/(gamma d)).

Q: z <= U = min((T+d)/2 - T(d-2a)/(2 gamma d),
                d + T a/(gamma d)).
```

Store `(max L among P, min U among Q)` for each rank-tree interval.
The identities for missing sides are `-infinity,+infinity` respectively;
isolates contribute neither side. The embedded implementation uses explicit
`None` identities, not floating-point infinities or zero.

**Control theorem.** Process sweeps and supplied events chronologically.
For each skipped contiguous rank block before an event, query its aggregate
and require `z>=maxL` and `z<=minU`. Because no proposed event lies inside
the block, all hypothetical labels, affinities and z remain fixed throughout
it. The two conditions are therefore equivalent to every vertex in the
block staying. At the next event evaluate its pre-move gains, check strict
positivity and the fresh tie rule, then update z, the mover's label/threshold,
and all neighbors' affinities/thresholds. Check the final sweep suffix and
finally the entire terminal sweep. The preceding induction proves soundness
and completeness without a sampled-volume index.

**Boundary challenges resolved.** Do not include the accepted rank in its
preceding skipped block. A first-rank event has an empty prefix; a last-rank
event has an empty suffix but changes next sweep's starting z. Update already
visited neighbors immediately too, but never query them again in that sweep.
Their updated leaves matter only on subsequent appropriate rank queries.
Testing the whole tree after every move would be too strong: a previously
visited vertex can become profitable, legitimately waiting until next sweep.
Updating only future neighbors without a deferred-update mechanism is too
weak: it leaves next sweep's tree stale. Nonmonotone z is harmless because
each query compares against the current scalar, not a predicted crossing time.

The abstract operation count is
`O(n+M+(f+J+S) log n) = O(n+M+(f+J) log n)` after structural validation,
with the `f=0,S=1` query absorbed by O(n). Build the tree bottom-up in O(n),
not by n separate logarithmic insertions. There are at most f+S skipped
blocks, f point tests, and f+J threshold updates. An adjacency representation
costs O(n+M) resident records; the tree/labels/degrees/affinities cost O(n),
and a resident proposal/output costs O(f). A streamed proposal needs suitable
validation bookkeeping but not a prebuilt two-dimensional index.

This is a strictly better **upper bound for the specified resident designs**,
not a lower bound against every offline index, not measured speed, and not
a proof of a small-RAM implementation. Source verification can require
additional sorting costs as detailed below.

This review establishes verifier equivalence only. It neither validates a
proposal generator nor establishes a generation-versus-verification gap.
With repeated flips, sparse threshold updates remain meaningful but the
single-flip bounds `J<=2M` and `f<=n` no longer follow. Fresh-community
creation and arbitrary-community Louvain require a different state model.

## Concrete Counterexamples

IDs below are ranks; `P=0,Q=1`; unlisted vertices are isolates. Every weight,
gain and resolution shown is exact. These refute altered rules, not the
correct algorithms. The checker prints the full data for ten mutated-verifier
witnesses; these descriptions choose especially informative examples.

1. **Pre-move volume and own label.** Take n=3, edge `(1,2,1/2)`, initial
   `PPQ`, gamma=1. The exact trace is `(1,1,Q)`, terminal sweep 2.
   At that move `T=1,d=a=z=1/2`, so `(G_switch,G_fresh)=(1/4,0)`.
   Using post-move `z=1` changes the switch gain to `-1/4`, falsely refusing.
   Using Q as u's pre-move label also makes its switch gain negative. The
   same graph with gamma=1/2 exposes a one-sweep-late update to rank 2.
2. **Later neighbor used too early.** Same edge, n=3, initial `PQP`,
   gamma=1/2, proposal `(1,2,Q)`, terminal 2. At `(1,1)` the real Q vertex
   has `a=0,z=1/2,G_switch=3/8>0`; this omitted move would empty Q.
   Prematurely applying rank 2's future flip makes `a=1/2`, hides the
   profitable omitted visit, and the `neighbor_now` mutant accepts.
3. **Fresh beats a positive switch.** Edges `(0,2,1/2),(1,2,1/2)`, initial
   `PPQ`, gamma=3. At `(1,0)`, `T=2,d=a=1/2,z=1` and
   `(G_switch,G_fresh)=(1/4,3/4)`. Proposal `(1,0,Q)`, terminal 2 is wrong;
   a verifier ignoring fresh destinations accepts it.
4. **Positive tie must favor existing community.** Edge `(1,2,1/2)`,
   initial `PQQ`, gamma=3. At `(1,1)`, switch to P and fresh each gain `1/4`.
   P contains isolated vertex 0 but is nonempty and must win. Exact trace:
   `(1,1,P)`, terminal 2. Requiring `switch>fresh` falsely refuses. Restricting
   destinations to neighbor communities would also change this contract.
5. **Zero tie must stay.** n=2, edge `(0,1,1)`, initial `PQ`, gamma=2.
   Both gains are zero at both visits. Exact trace is empty with `S=1`.
   A nonnegative acceptance rule would move at zero gain and change the run.
6. **Wrong sampled extremum.** Edges `(0,2,1/2),(1,2,1/2)`, initial `PPQ`,
   gamma=2, proposal `(1,1,Q)`, terminal 2. Rank 0 is skipped twice with
   unchanged affinity `1/2` and sampled volumes `1,3/2`; its switch gains
   are `1/2,-1/2`. Testing the maximum z for this P vertex misses its
   profitable first visit. The actual trace moves rank 0, not rank 1.
7. **Global event extrema are too conservative.** Use example 4. Rank 2
   remains Q and sees `a=0,z=1/2` at its sweep-1 visit, giving switch gain
   `-1/4`. A global sweep extremum additionally includes `z=1` from before
   rank 1 moved. Pairing that unsampled z with rank 2's updated `a=0`
   gives switch gain `5/4>0` and falsely refuses the correct trace.
8. **A genuine later terminal violation.** n=6, edges
   `(1,2,1),(1,3,1/2),(2,3,3/2),(2,4,1)`, initial `QPPQQP`, gamma=1/2.
   Exact trace is `(1,2,Q),(2,1,Q)`, terminal 3. The truncated proposal
   `(1,2,Q)`, terminal 2 passes an omitted-terminal checker. At the omitted
   `(2,1)` visit `T=8,d=a=3/2,z=13/2,G_switch=57/8>0`.
   Rank 1 was already visited before rank 2's first-sweep move: this also
   checks why previously visited neighbors need next-sweep updates.
9. **Accepted-only is vacuous for empty proposals.** Example 1 proposed
   with no events and `S=1` is wrong: rank 1 profits by `1/4`. A checker
   looking only at listed moves has no condition to reject it.
10. **First/last ranks.** Edge `(0,1,1/2)`, n=3, initial `PQP`, gamma=1
    has exact trace `(1,0,Q)`, terminal 2. For a last-rank example, n=5,
    edges `(0,1,2),(0,4,1),(1,2,2)`, initial `PPPQQ`, gamma=1/2 has exact
    trace `(1,4,P)`, terminal 2. Updated volume must cross the sweep boundary
    even though the last mover has no following rank in that sweep.

An additional **scalar, not claimed whole-graph** counterexample to testing
only temporal endpoints has fixed `T=10,d=1,a=1/2,gamma=1,label=P` and
sampled volumes `5,4,5`: switch gains are `-1,+1,-1`. A nonmonotone volume
sequence can hide an interior maximum. The checker validates this identity
separately; it does not claim these scalars alone specify an accepted graph trace.

## Evidence And Exact Counts

The standalone Python checker below imports only the standard library, reads
no project code or data, and uses `Fraction` throughout. It is an independent
implementation, not a wrapper around the lead kernel. The direct oracle
recomputes H for each candidate partition and shares neither closed gain
formulas nor threshold/index logic. It stops with `None` at the first action
incompatible with any admissible trace; it does not simulate the rest of an
unrestricted many-community run. Shared graph validation is disclosed.

**Exhaustive domain.** All nonempty edge assignments in `{0,1/2,3/2}` for
n=2,3, and `{0,1}` for n=4; every nontrivial P/Q labeling; gamma in
`{1/2,1,2,3}` for n<=3 and `{1,2}` for n=4. For each labeling enumerate
every per-vertex proposed sweep in `0..n` (0 means no flip), retaining exactly
the structurally admissible traces. This covers every possible admissible
schedule in these domains, not just perturbations of successful proposals.

| Check | Exact count |
|---|---:|
| Nonempty weighted labeled-ID graphs | 91 |
| Graph/initial-partition/resolution cases | 2,404 |
| Admissible proposal comparisons, each against both verifiers | 164,972 |
| Accepted proposals | 1,120 |
| Rejected proposals | 163,852 |
| False acceptances / false refusals, either verifier | 0 / 0 |
| Direct-objective versus closed-gain identities | 17,920 |
| Threshold stay versus gain-sign identities | 8,960 |
| Independently materialized synthetic volume schedules | 160 |
| Exact nonempty sampled-range queries | 4,294 |
| Empty sampled-range queries returning no contribution | 596 |
| Fixed boundary cases, including multisweep and tiny rational margins | 10 |
| Explicit malformed proposals rejected by both verifiers | 13 |
| Explicit invalid graph/resolution inputs rejected | 8 |
| Incorrect verifier variants with concrete disagreement witnesses | 10 / 10 |
| Separate scalar interior-extremum counterexample | 1 |

The exhaustive accepted traces stop at sweep 1 in 520 cases and sweep 2 in
600 cases; **none** of these small exhaustive cases needs sweep 3. The separate
six-vertex regression supplies one genuine sweep-3 trace. Fixed cases also
use gamma `2 +/- 1/10^60`, first/last accepted ranks, positive and zero ties,
and an isolated community. They are not counted in the 164,972 proposals.
The synthetic index tests enumerate n=1..4 and S=1..4 independently of graph
realizability; they deliberately permit degenerate community occupancies to
challenge only the index. All rank and sweep subintervals in that domain are
checked against a separately materialized visit grid.

The checker began with a failing valid-empty-trace assertion before its
implementation was filled in. Mutation witnesses demonstrate actual test
sensitivity, not a claim that testing proves the general theorem. The ten
mutation searches retain the first counterexample; they do not count every
mutation failure. No timings or RSS measurements were taken or reported.

## Complete Cost Accounting

Let V be source/proposal validation work, P be proposal-generation work,
and O_out the required output-writing work. Neither P nor V is free.
For comparison, even visit-by-visit replay can maintain a and z sparsely in
`O(n+M+nS+J)` operations: do not handicap it by rescanning every neighborhood
on every visit. Objective recomputation in this checker is an independence
device, not the baseline to use for performance claims.

For a straightforward resident sampled index, with h skipped intervals:

```text
J <= 2M;  h = O(n+J+f);  K <= f+S;  R = O(K log n).

Work = P + V + O(n+M + J log(J+1) + R
                 + (h+f) log n log(S+1)) + O_out.
Resident records = O(n+M+J+f+R), with per-list linear-size min/max trees.

Chronological control work = P + V + O(n+M+(f+J) log n) + O_out.
Chronological resident records = O(n+M+f), including adjacency and proposal.
```

The offline construction can avoid sorting node lists by emitting sweeps in
order; affinity records still need grouping/sorting or an equivalent charged
construction. A sparse table instead of per-list segment trees incurs its
own additional storage and must not be described as a linear-size RMQ build.
Empty/full-range and f=0 constants are absorbed by n since n>=2 here.

Validation must read all n vertex descriptors and M records. Duplicate-edge
detection can use expected-linear hashing with O(M) memory, or deterministic
comparison sorting in `O(M log M)` work; already-canonical sorted input needs
a verified ordering scan. Arbitrary original IDs need a checked rank mapping,
possibly `O(n log n)` sorting. If hashing assumptions or canonical-input
promises are unavailable, append these costs to both kernels. The standalone
checker uses canonical endpoints plus a duplicate-detection set, not an
external-memory validator or a hardened text parser.

These are exact-arithmetic operation and **record** bounds, not fixed-width
bit bounds. Distinct rational denominators may make intermediate numerator
and denominator lengths grow with source size. Charge arithmetic/comparison
cost at the maximum encountered bit width, storage of all big integers,
and exact threshold division or cross multiplication. Avoid tolerance-based
sign tests; gamma `2 +/- 1/10^60` deliberately makes that distinction visible.

If output means the accepted trace plus final labels and stop count, charge
Omega(f+n) output records and their ID/sweep bit lengths; verification alone
may instead return a bit or a first witness. If output means a full visit log
or every sweep's partition, its Omega(nS) output cannot be removed by internal
compression. Publishing a correct trace as a fresh solve must include its
generation. An already-resident, previously validated graph/index is a
different query model, not a free source for an end-to-end low-RAM claim.

Neither method establishes bounded small workspace. Chronological adjacency
storage is O(M); transposing edges into mover-ordered update records can move
that burden to external storage but creates O(J) records, sorting, I/O and
buffer requirements that must be charged. A kinetic queue which explicitly
repairs every global threshold crossing may do much more work when z
oscillates. The rank tree avoids requiring vertices to be safe between their
actual visits; mere neighbor-frontier pruning misses global-volume effects.

For the six-vertex trace in counterexample 8, the checker also asserts these
exact construction counts (one instance, not an empirical scaling claim):

| Item | Records or checks |
|---|---:|
| Source vertices / stored undirected edges / adjacency entries | 6 / 4 / 8 |
| Accepted flips / sweeps including terminal / dense visits | 2 / 3 / 18 |
| Neighbor-update records J | 5 |
| Volume pieces / canonical insertion records / nonempty node lists | 5 / 9 / 6 |
| Allocated per-list min/max pair slots, including each unused slot 0 | 20 |
| Chronological nonempty skipped blocks / accepted point checks | 5 / 2 |
| Chronological threshold leaf updates after initialization | 7 |
| Chronological padded tree pair slots, including unused slot 0 | 16 |
| Trace events plus final-label output records, excluding stop metadata | 8 |

The proposal was obtained by the independently charged oracle in the checker;
the table does not make that generation free. These are logical/array counts,
not bytes or peak memory: the Python object, rational-integer and container
overheads are explicitly outside this illustration. The checker repeatedly
validates sources for independence and clarity and is not a performance model.

## Targeted Primary Prior Art

This is a targeted inspection of adjacent mechanisms, not an exhaustive
priority search or evidence that no identical algorithm exists. Search terms
included exact/asynchronous Louvain trace certification, dynamic community
screening, kinetic certificates, and off-line/streaming priority-queue checking.

- **Dynamic communities:** Zarayeneh and Kalyanaraman's Delta-screening
  selects a subset to reconsider after graph updates. It is directly relevant
  to sparse affected-region controls, but its stated output is updated
  communities, not a certificate reproducing a supplied deterministic serial
  trace with a fixed fresh-token tie rule.
  [Authors' paper](https://arxiv.org/abs/1904.08553).
- **Neighbor-frontier control:** Sahu's DF Louvain explicitly describes an
  approximate affected set, local expansion and incremental vertex/community
  weights. Its analysis addresses accuracy of the dynamic procedure, not
  equality to this note's exact accepted-event sequence. It should inform a
  practical comparator, not be relabeled an exact-trace baseline without a
  separate equivalence argument.
  [Author's report, sections 3-4 and appendix A.5](https://arxiv.org/html/2404.19634v1).
- **Fast local moves:** Traag, Waltman and van Eck describe a queue initialized
  in random order, then re-enqueuing selected neighbors after a move. This is
  prior art for avoiding repeated unproductive visits. Its queue schedule and
  partition guarantees do not establish identity with every increasing-ID
  sweep or this fresh-singleton policy.
  [Leiden paper, fast local move description](https://www.nature.com/articles/s41598-019-41695-z).
- **Kinetic certificates:** Basch, Guibas and Hershberger's *Data Structures
  for Mobile Data* develops maintaining structures through discrete certificate
  events. Threshold maintenance fits that established perspective; our z
  jumps at discrete moves rather than following a continuous flight plan.
  [Primary paper](https://www.ime.usp.br/~cris/aulas/19_1_6957/BaschGH-DSforMobileData.pdf).
- **Exact dynamic-operation checking:** de Nivelle and Piskac formally verify
  an off-line priority-queue checker. Its central mechanism attaches a lower
  bound to each item, based on maxima of reported minima over its lifetime.
  This is particularly close to certifying skipped greedy choices via temporal
  extrema, although it does not handle community gains or rank-sampled visits.
  [Authors' paper](https://cs.yale.edu/homes/piskac/papers/2005deNivellePiskacVerification.pdf).
- **Certification trails and greedy data structures:** McConnell, Mehlhorn,
  Naher and Schweitzer discuss certification trails, reactive data structures
  and priority-queue checking, including the Finkler-Mehlhorn construction.
  Thus checking a supplied operation history rather than recomputing all
  choices is established methodology, not itself a new contribution here.
  [Authors' survey, sections 4 and 12](https://people.mpi-inf.mpg.de/~mehlhorn/ftp/master.pdf).
- **Small-memory trace checking:** Francois and Magniez study insert/extract
  histories under limited-memory sequential access and show that pass direction
  materially changes the space tradeoff. This is closer to the stated low-RAM
  objective than an O(M)-resident certificate alone. Its priority-queue bounds
  do not transfer to modularity verification without a reduction; no such
  reduction is supplied here.
  [Primary STACS 2013 paper](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.STACS.2013.454).

The last three are the nearest inspected **exact history/greedy-operation
certification** precedents; the first three are the nearest inspected
community-update controls. None of those observations establishes novelty
or rules out closer literature. The proposed semantic specialization may be
useful; an innovation claim needs a positively identified distinction and
resource benefit over strong controls, not absent search hits.

## Final Synthesis And Open Work

Accept the restricted equivalence theorem after making the `T>0` condition
explicit. Prefer the chronological threshold tree as the principal resident
control; the 2D index is unnecessary for this fixed-order, two-community
verification problem. The separate proposal-free solver is not reviewed here.

**Lead-reported update, not independently reproduced:** both verifier
implementations admit 86 of the fixed 150 sources (the old 55 plus all 31
unchanged refusals), reject 64 changed cases, and report exact comparisons
covering 7,232 labels and 344 gains. This information came from the lead's
scope update; neither those fixtures nor their implementations were inspected.
It is separate from the independent counts above.

Still open in this review: the native lead implementation's correctness;
independent reproduction of the 150-source results; useful naturally
occurring single-flip workloads; external-memory validation, adjacency/update
access and bit complexity; and a demonstrated innovation relevant to low RAM.
This sidecar inspects none of the lead's new files and does not certify their
tests, resource behavior or outputs. No commit or push was performed.

## Standalone Checker

Run from the repository root without creating another file:

```sh
awk '/^<!-- SINGLE-FLIP-REVIEW-CHECKER-START -->$/{p=1;next} /^<!-- SINGLE-FLIP-REVIEW-CHECKER-END -->$/{p=0} p && !/^```/' research_algorithms_20260920/Communities-Single-Flip-Independent-Review.md | python3 -B
```

<!-- SINGLE-FLIP-REVIEW-CHECKER-START -->
```python
from fractions import Fraction as F
from bisect import bisect_left, bisect_right
from collections import Counter
from itertools import combinations, product
import json


def merge_optional_bound_pairs(left, right):
    lows = [x for x in (left[0], right[0]) if x is not None]
    highs = [x for x in (left[1], right[1]) if x is not None]
    return (min(lows) if lows else None, max(highs) if highs else None)


def build_minmax_range_tree(values):
    size = 1
    while size < len(values):
        size *= 2
    nodes = [(None, None)] * (2 * size)
    nodes[size:size + len(values)] = values
    for node in range(size - 1, 0, -1):
        nodes[node] = merge_optional_bound_pairs(nodes[2 * node], nodes[2 * node + 1])
    return size, nodes


def update_minmax_tree_leaf(tree, position, value):
    size, nodes = tree
    node = size + position
    nodes[node] = value
    while node > 1:
        node //= 2
        nodes[node] = merge_optional_bound_pairs(nodes[2 * node], nodes[2 * node + 1])


def query_minmax_tree_range(tree, start, stop):
    size, nodes = tree
    start, stop = start + size, stop + size
    result = (None, None)
    while start < stop:
        if start % 2:
            result = merge_optional_bound_pairs(result, nodes[start])
            start += 1
        if stop % 2:
            stop -= 1
            result = merge_optional_bound_pairs(result, nodes[stop])
        start, stop = start // 2, stop // 2
    return result


def validate_graph_input_exact(n, edges, initial, gamma):
    if type(n) is not int or n < 2 or len(initial) != n or set(initial) != {0, 1}:
        raise ValueError("nonempty P/Q partition required")
    if not isinstance(gamma, (int, F)) or gamma <= 0:
        raise ValueError("positive rational resolution required")
    degrees, neighbors, seen = [F(0)] * n, [[] for _ in range(n)], set()
    for u, v, weight in edges:
        if (type(u) is not int or type(v) is not int or not 0 <= u < v < n
                or (u, v) in seen or not isinstance(weight, (int, F)) or weight <= 0):
            raise ValueError("canonical simple positive rational edge required")
        seen.add((u, v))
        degrees[u] += weight
        degrees[v] += weight
        neighbors[u].append((v, weight))
        neighbors[v].append((u, weight))
    if sum(degrees) == 0:
        raise ValueError("T=0 needs a separate semantic convention")
    return degrees, neighbors


def validate_proposal_shape_exact(n, initial, trace, stop):
    if type(stop) is not int or not 1 <= stop <= len(trace) + 1:
        return False
    sizes, seen, sweeps, previous = [initial.count(0), initial.count(1)], set(), set(), (0, -1)
    for sweep, vertex, target in trace:
        if (type(sweep) is not int or type(vertex) is not int or type(target) is not int
                or not 0 <= vertex < n or not 1 <= sweep < stop
                or (sweep, vertex) <= previous or vertex in seen
                or target != 1 - initial[vertex]):
            return False
        sizes[initial[vertex]] -= 1
        sizes[target] += 1
        if 0 in sizes:
            return False
        seen.add(vertex)
        sweeps.add(sweep)
        previous = (sweep, vertex)
    return sweeps == set(range(1, stop))


def evaluate_objective_value_exact(edges, degrees, labels, gamma):
    total = sum(degrees)
    volumes = {}
    for degree, label in zip(degrees, labels):
        volumes[label] = volumes.get(label, F(0)) + degree
    internal = sum(w for u, v, w in edges if labels[u] == labels[v])
    return 2 * total * internal - gamma * sum(v * v for v in volumes.values())


def oracle_direct_trace_exact(n, edges, initial, gamma):
    degrees, _ = validate_graph_input_exact(n, edges, initial, gamma)
    labels, trace, moved = list(initial), [], set()
    # A first fresh move, empty community, or repeated mover makes every
    # admissible single-flip proposal incompatible; no later replay is needed.
    for sweep in range(1, n + 2):
        changed = False
        for vertex in range(n):
            base = evaluate_objective_value_exact(edges, degrees, labels, gamma)
            best_gain, destination = F(0), labels[vertex]
            fresh = max(labels) + 1
            candidates = sorted(set(labels) - {labels[vertex]}) + [fresh]
            for target in candidates:
                hypothetical = labels.copy()
                hypothetical[vertex] = target
                gain = evaluate_objective_value_exact(edges, degrees, hypothetical, gamma) - base
                if gain > best_gain:
                    best_gain, destination = gain, target
            if best_gain > 0:
                if destination == fresh or vertex in moved:
                    return None
                labels[vertex] = destination
                if set(labels) != {0, 1}:
                    return None
                moved.add(vertex)
                trace.append((sweep, vertex, destination))
                changed = True
        if not changed:
            return tuple(trace), sweep
    raise AssertionError("single-flip bound violated")


def compute_closed_gain_pair(label, degree, affinity, volume, total, gamma):
    if label == 0:
        return (total * (2 * affinity - degree) - gamma * degree * (2 * volume - total + degree),
                -total * (degree - affinity) + gamma * degree * (total - volume - degree))
    return (total * (degree - 2 * affinity) - gamma * degree * (total - 2 * volume + degree),
            -total * affinity + gamma * degree * (volume - degree))


def build_sampled_volume_index(n, degrees, initial, trace, stop, mutation=""):
    size = 1
    while size < n:
        size *= 2
    pieces, events, volume = [], {}, sum(d for d, p in zip(degrees, initial) if p == 1)
    for sweep, vertex, target in trace:
        events.setdefault(sweep, []).append((vertex, target))
    for sweep in range(1, stop + 1):
        start = 0
        for vertex, target in events.get(sweep, []):
            end = vertex - (mutation == "post_volume")
            if start <= end:
                pieces.append((sweep, start, end, volume))
            volume += degrees[vertex] * (1 if target else -1)
            start = end + 1
        if start < n:
            pieces.append((sweep, start, n - 1, volume))
    lists = {}
    for sweep, start, end, volume in pieces:
        left, right = start + size, end + size + 1
        while left < right:
            if left % 2:
                lists.setdefault(left, []).append((sweep, volume))
                left += 1
            if right % 2:
                right -= 1
                lists.setdefault(right, []).append((sweep, volume))
            left, right = left // 2, right // 2
    nodes = {}
    for node, entries in lists.items():
        sweeps = [s for s, _ in entries]
        assert sweeps == sorted(set(sweeps))
        nodes[node] = (sweeps, build_minmax_range_tree([(z, z) for _, z in entries]))
    return size, nodes, pieces


def query_sampled_volume_range(index, vertex, first, last):
    size, nodes, _ = index
    node, result = size + vertex, (None, None)
    while node:
        if node in nodes:
            sweeps, tree = nodes[node]
            lower, upper = bisect_left(sweeps, first), bisect_right(sweeps, last)
            result = merge_optional_bound_pairs(result, query_minmax_tree_range(tree, lower, upper))
        node //= 2
    return result


def verify_sampled_trace_exact(n, edges, initial, gamma, trace, stop, mutation=""):
    degrees, neighbors = validate_graph_input_exact(n, edges, initial, gamma)
    if not validate_proposal_shape_exact(n, initial, trace, stop):
        return False
    total, moves = sum(degrees), {u: (s, t) for s, u, t in trace}
    index = build_sampled_volume_index(n, degrees, initial, trace, stop, mutation)
    for u in range(n):
        changes, affinity = {}, sum(w for v, w in neighbors[u] if initial[v] == 1)
        for v, weight in neighbors[u]:
            if v in moves:
                sweep, target = moves[v]
                effective = sweep + (v > u)
                if mutation == "neighbor_now":
                    effective = sweep
                if mutation == "neighbor_late":
                    effective = sweep + 1
                changes[effective] = changes.get(effective, F(0)) + weight * (1 if target else -1)
        own = moves[u][0] if u in moves else stop + 1
        boundaries = sorted({1, stop + 1, *changes, own, own + 1})
        for first, following in zip(boundaries, boundaries[1:]):
            affinity += changes.get(first, F(0))
            if first > stop:
                break
            last = min(following - 1, stop)
            label = initial[u] ^ (first > own or (mutation == "own_early" and first == own))
            if first == own:
                low, high = query_sampled_volume_range(index, u, first, first)
                assert low == high
                switch, fresh = compute_closed_gain_pair(label, degrees[u], affinity, low, total, gamma)
                if switch <= 0:
                    return False
                if mutation != "no_fresh" and (switch < fresh or (mutation == "strict_tie" and switch == fresh)):
                    return False
            elif mutation != "accepted_only":
                if mutation == "omit_terminal":
                    last = min(last, stop - 1)
                if first > last:
                    continue
                low, high = query_sampled_volume_range(index, u, first, last)
                assert low is not None and high is not None
                if mutation == "global_times":
                    values = [z for s, _, _, z in index[2] if first <= s <= last]
                    low, high = min(values), max(values)
                volume = low if label == 0 else high
                if mutation == "wrong_extremum":
                    volume = high if label == 0 else low
                switch, fresh = compute_closed_gain_pair(label, degrees[u], affinity, volume, total, gamma)
                if switch > 0 or (mutation != "no_fresh" and fresh > 0):
                    return False
    return True


def compute_vertex_stay_bounds(label, degree, affinity, total, gamma):
    if degree == 0:
        return None, None
    if label == 0:
        lower = max((total - degree) / 2 + total * (2 * affinity - degree) / (2 * gamma * degree),
                    total - degree - total * (degree - affinity) / (gamma * degree))
        return None, lower
    upper = min((total + degree) / 2 - total * (degree - 2 * affinity) / (2 * gamma * degree),
                degree + total * affinity / (gamma * degree))
    return upper, None


def verify_sparse_blocks_exact(n, edges, initial, gamma, trace, stop):
    degrees, neighbors = validate_graph_input_exact(n, edges, initial, gamma)
    if not validate_proposal_shape_exact(n, initial, trace, stop):
        return False
    total, labels = sum(degrees), list(initial)
    volume = sum(d for d, p in zip(degrees, labels) if p == 1)
    affinity = [sum(w for v, w in neighbors[u] if labels[v] == 1) for u in range(n)]
    tree = build_minmax_range_tree([
        compute_vertex_stay_bounds(labels[u], degrees[u], affinity[u], total, gamma) for u in range(n)])
    events = {}
    for sweep, u, target in trace:
        events.setdefault(sweep, []).append((u, target))
    for sweep in range(1, stop + 1):
        start = 0
        for u, target in events.get(sweep, []) + [(n, None)]:
            upper, lower = query_minmax_tree_range(tree, start, u)
            if (upper is not None and volume > upper) or (lower is not None and volume < lower):
                return False
            if u == n:
                continue
            switch, fresh = compute_closed_gain_pair(labels[u], degrees[u], affinity[u], volume, total, gamma)
            if switch <= 0 or switch < fresh:
                return False
            labels[u] = target
            sign = 1 if target else -1
            volume += sign * degrees[u]
            for v, weight in neighbors[u]:
                affinity[v] += sign * weight
                update_minmax_tree_leaf(tree, v, compute_vertex_stay_bounds(
                    labels[v], degrees[v], affinity[v], total, gamma))
            update_minmax_tree_leaf(tree, u, compute_vertex_stay_bounds(
                labels[u], degrees[u], affinity[u], total, gamma))
            start = u + 1
    return True


def enumerate_valid_trace_proposals(n, initial):
    for times in product(range(n + 1), repeat=n):
        stop = max(times) + 1
        trace = tuple(sorted((s, u, 1 - initial[u]) for u, s in enumerate(times) if s))
        if validate_proposal_shape_exact(n, initial, trace, stop):
            yield trace, stop


def check_boundary_regressions_exact(counts):
    fixed = [
        (3, ((1, 2, F(1, 2)),), (0, 0, 1), F(1), ((1, 1, 1),), 2, True),
        (3, ((1, 2, F(1, 2)),), (0, 1, 1), F(3), ((1, 1, 0),), 2, True),
        (2, ((0, 1, F(1)),), (0, 1), F(2), (), 1, True),
        (2, ((0, 1, F(1)),), (0, 1), F(2), ((1, 0, 1),), 2, False),
        (6, ((1, 2, F(1)), (1, 3, F(1, 2)), (2, 3, F(3, 2)), (2, 4, F(1))),
         (1, 0, 0, 1, 1, 0), F(1, 2), ((1, 2, 1),), 2, False),
        (6, ((1, 2, F(1)), (1, 3, F(1, 2)), (2, 3, F(3, 2)), (2, 4, F(1))),
         (1, 0, 0, 1, 1, 0), F(1, 2), ((1, 2, 1), (2, 1, 1)), 3, True),
        (3, ((0, 1, F(1, 2)),), (0, 1, 0), F(1), ((1, 0, 1),), 2, True),
        (5, ((0, 1, F(2)), (0, 4, F(1)), (1, 2, F(2))),
         (0, 0, 0, 1, 1), F(1, 2), ((1, 4, 0),), 2, True),
        (3, ((1, 2, F(1, 2)),), (0, 0, 1), F(2) - F(1, 10**60), ((1, 1, 1),), 2, True),
        (3, ((1, 2, F(1, 2)),), (0, 0, 1), F(2) + F(1, 10**60), (), 1, True),
    ]
    for *case, expected in fixed:
        assert verify_sampled_trace_exact(*case) == expected
        assert verify_sparse_blocks_exact(*case) == expected
        truth = oracle_direct_trace_exact(*case[:4])
        assert (truth == (case[4], case[5])) == expected
        counts["fixed_boundary_cases"] += 1
    n, edges, initial, gamma, trace, stop, _ = fixed[5]
    degrees, neighbors = validate_graph_input_exact(n, edges, initial, gamma)
    index = build_sampled_volume_index(n, degrees, initial, trace, stop)
    updates = sum(len(neighbors[u]) for _, u, _ in trace)
    assert (n, len(edges), sum(map(len, neighbors)), len(trace), stop, n * stop, updates) == (6, 4, 8, 2, 3, 18, 5)
    assert (len(index[2]), sum(len(s) for s, _ in index[1].values()), len(index[1])) == (5, 9, 6)
    assert sum(len(t[1]) for _, t in index[1].values()) == 20
    assert len(trace) + updates == 7 and 2 * index[0] == 16 and n + len(trace) == 8
    assert sum(1 for s in range(1, stop + 1) for left, right in zip(
        [-1] + [u for t, u, _ in trace if t == s],
        [u for t, u, _ in trace if t == s] + [n]) if left + 1 < right) == 5
    counts["construction_count_regressions"] += 1
    assert verify_sampled_trace_exact(*fixed[4][:-1], mutation="omit_terminal")
    initial = (0, 0, 1, 1)
    valid = ((1, 0, 1), (1, 2, 0))
    assert validate_proposal_shape_exact(4, initial, valid, 2)
    malformed = [
        (tuple(reversed(valid)), 2),
        (((1, 0, 1), (2, 0, 1)), 3),
        (((1, 0, 0),), 2),
        (((1, 0, 1), (1, 1, 1)), 2),
        (((2, 0, 1),), 3),
        (((0, 0, 1),), 2),
        (((1, 4, 1),), 2),
        (((1, -1, 1),), 2),
        (((1, 0, 2),), 2),
        (valid, 1), (valid, 3), ((), 0), ((), 2),
    ]
    for trace, stop in malformed:
        assert not validate_proposal_shape_exact(4, initial, trace, stop)
        for verifier in (verify_sampled_trace_exact, verify_sparse_blocks_exact):
            assert not verifier(4, ((0, 1, F(1)),), initial, F(1), trace, stop)
        counts["malformed_proposals_rejected"] += 1
    invalid = [
        (2, (), (0, 1), F(1)),
        (2, ((0, 0, F(1)),), (0, 1), F(1)),
        (2, ((0, 1, F(1)), (0, 1, F(2))), (0, 1), F(1)),
        (2, ((0, 1, F(0)),), (0, 1), F(1)),
        (2, ((0, 1, F(-1)),), (0, 1), F(1)),
        (2, ((0, 2, F(1)),), (0, 1), F(1)),
        (2, ((0, 1, F(1)),), (0, 0), F(1)),
        (2, ((0, 1, F(1)),), (0, 1), F(0)),
    ]
    for case in invalid:
        try:
            validate_graph_input_exact(*case)
        except ValueError:
            counts["invalid_sources_rejected"] += 1
        else:
            raise AssertionError(case)
    gains = [compute_closed_gain_pair(0, F(1), F(1, 2), F(z), F(10), F(1))[0]
             for z in (5, 4, 5)]
    assert gains == [-1, 1, -1]
    counts["interior_extremum_counterexamples"] += 1


def check_sampled_index_exhaustively(counts):
    for n in range(1, 5):
        degrees = [F(u + 1, 2) for u in range(n)]
        initial = tuple(u % 2 for u in range(n))
        for stop in range(1, 5):
            for times in product(range(stop), repeat=n):
                if set(times) - {0} != set(range(1, stop)):
                    continue
                trace = tuple(sorted((s, u, 1 - initial[u]) for u, s in enumerate(times) if s))
                index = build_sampled_volume_index(n, degrees, initial, trace, stop)
                assert len(index[2]) <= len(trace) + stop
                labels, grid = list(initial), []
                events = {(s, u): target for s, u, target in trace}
                for sweep in range(1, stop + 1):
                    row = []
                    for u in range(n):
                        row.append(sum(d for d, label in zip(degrees, labels) if label == 1))
                        if (sweep, u) in events:
                            labels[u] = events[sweep, u]
                    grid.append(row)
                for u in range(n):
                    assert query_sampled_volume_range(index, u, stop + 1, stop + 1) == (None, None)
                    counts["empty_index_queries"] += 1
                    for first in range(1, stop + 1):
                        for last in range(first, stop + 1):
                            values = [grid[s - 1][u] for s in range(first, last + 1)]
                            assert query_sampled_volume_range(index, u, first, last) == (min(values), max(values))
                            counts["nonempty_index_queries"] += 1
                counts["sampled_volume_schedules"] += 1


def check_gain_identity_exhaustively(n, edges, initial, gamma, counts):
    degrees, neighbors = validate_graph_input_exact(n, edges, initial, gamma)
    total = sum(degrees)
    volume = sum(d for d, p in zip(degrees, initial) if p == 1)
    base = evaluate_objective_value_exact(edges, degrees, initial, gamma)
    for u in range(n):
        affinity = sum(w for v, w in neighbors[u] if initial[v] == 1)
        gains = compute_closed_gain_pair(initial[u], degrees[u], affinity, volume, total, gamma)
        for target, gain in zip((1 - initial[u], 2), gains):
            labels = list(initial)
            labels[u] = target
            assert evaluate_objective_value_exact(edges, degrees, labels, gamma) - base == 2 * gain
            counts["objective_gain_identities"] += 1
        upper, lower = compute_vertex_stay_bounds(initial[u], degrees[u], affinity, total, gamma)
        safe = (upper is None or volume <= upper) and (lower is None or volume >= lower)
        assert safe == (max(gains) <= 0)
        counts["threshold_identities"] += 1


def run_independent_review_checks():
    edges = ((0, 1, F(1)), (2, 3, F(1)))
    assert verify_sampled_trace_exact(4, edges, (0, 0, 1, 1), F(1), (), 1)
    assert verify_sparse_blocks_exact(4, edges, (0, 0, 1, 1), F(1), (), 1)
    counts, witnesses = Counter(), {}
    check_boundary_regressions_exact(counts)
    check_sampled_index_exhaustively(counts)
    mutations = ("accepted_only", "no_fresh", "strict_tie", "post_volume", "own_early",
                 "neighbor_now", "neighbor_late", "omit_terminal", "wrong_extremum", "global_times")
    for n in range(2, 5):
        pairs = list(combinations(range(n), 2))
        alphabet = (F(0), F(1, 2), F(3, 2)) if n <= 3 else (F(0), F(1))
        gammas = (F(1, 2), F(1), F(2), F(3)) if n <= 3 else (F(1), F(2))
        labels_list = [p for p in product((0, 1), repeat=n) if set(p) == {0, 1}]
        proposals = {p: list(enumerate_valid_trace_proposals(n, p)) for p in labels_list}
        for weights in product(alphabet, repeat=len(pairs)):
            if not any(weights):
                continue
            edges = tuple((u, v, w) for (u, v), w in zip(pairs, weights) if w)
            counts["weighted_graphs"] += 1
            for initial in labels_list:
                for gamma in gammas:
                    counts["graph_partition_resolution_cases"] += 1
                    check_gain_identity_exhaustively(n, edges, initial, gamma, counts)
                    truth = oracle_direct_trace_exact(n, edges, initial, gamma)
                    counts["representable_oracle_traces"] += truth is not None
                    if truth is not None:
                        counts["accepted_stop_sweep_" + str(truth[1])] += 1
                    for trace, stop in proposals[initial]:
                        expected = truth == (trace, stop)
                        actual = verify_sampled_trace_exact(n, edges, initial, gamma, trace, stop)
                        control = verify_sparse_blocks_exact(n, edges, initial, gamma, trace, stop)
                        case = (n, edges, initial, gamma, trace, stop)
                        assert actual == expected, ("sampled", case, truth)
                        assert control == expected, ("blocks", case, truth)
                        counts["proposal_comparisons"] += 1
                        counts["accepted_proposals" if expected else "rejected_proposals"] += 1
                        for mutation in mutations:
                            if mutation in witnesses:
                                continue
                            wrong = verify_sampled_trace_exact(*case, mutation=mutation)
                            if wrong != expected:
                                witnesses[mutation] = {"case": case, "truth": truth,
                                                       "correct": expected, "mutant": wrong}
        print(json.dumps({"finished_n": n, "counts": dict(sorted(counts.items()))}), flush=True)
    assert counts["accepted_proposals"] == counts["representable_oracle_traces"]
    assert set(witnesses) == set(mutations)
    expected_counts = {
        "weighted_graphs": 91, "graph_partition_resolution_cases": 2404,
        "proposal_comparisons": 164972, "accepted_proposals": 1120,
        "rejected_proposals": 163852, "representable_oracle_traces": 1120,
        "objective_gain_identities": 17920,
        "threshold_identities": 8960, "sampled_volume_schedules": 160,
        "nonempty_index_queries": 4294, "empty_index_queries": 596,
        "fixed_boundary_cases": 10, "malformed_proposals_rejected": 13,
        "invalid_sources_rejected": 8, "interior_extremum_counterexamples": 1,
        "accepted_stop_sweep_1": 520, "accepted_stop_sweep_2": 600,
        "construction_count_regressions": 1,
    }
    assert dict(counts) == expected_counts, dict(counts)
    print(json.dumps({"counts": dict(sorted(counts.items())), "mutation_witnesses": witnesses},
                     default=str, sort_keys=True, indent=2))


if __name__ == "__main__":
    run_independent_review_checks()
```
<!-- SINGLE-FLIP-REVIEW-CHECKER-END -->

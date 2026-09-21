# Single-Flip Community Trace Verification

Date: 2026-09-21. New A04 research direction derived from the heterogeneous
certificate's 31 unchanged-trajectory refusals. Status: two implemented exact
resident verifiers, finite direct-objective falsification, and a retained
150-source study. Independent mathematical challenge is complete and
lead-replayed for the verifiers, not the subsequent proposal-free solver. No
physical RAM, speed, native-workload prevalence or publication-priority result.

The original offline design remains below for traceability. Sections 6 onward
record the implemented chronological control, which has a better work bound,
and the evidence that changes the next research action. A range tree itself
is established machinery, not a novel contribution.

## Question And Intended Delta

Can we certify the EXACT proposed local-moving trajectory without replaying
every vertex on every sweep, and without requiring the gain sign to hold on
unreachable hypothetical partitions?

The [heterogeneous band study](Communities-Heterogeneous-Band-Certificates.md)
has 150 fixed inputs: 55 admitted, 31 refused despite preserving the same
accepted trajectory, and 64 whose trajectory really changes. All first
refusals are mover-sign failures. Stronger anchor bounds cannot resolve that
gate. The new target is a complete verifier for a supplied single-flip trace,
not another tolerance or another favorable perturbation scale.

Conventional verification simulates every one of n*S visits. The proposed
alternative combines sparse neighbor-affinity changes with batched extrema
queries over global community volume at the actual skipped visits. Both are
necessary: checking only the accepted moves is unsound.

## Exact Semantic Contract

- Simple loopless undirected graph with positive rational weights and n
  original integer IDs, ranked in increasing visit order; M>0 stored edges
  and positive total degree T. Isolates in such a graph are supported.
- Initial P/Q partition, positive rational resolution gamma, total degree T.
- Visit all IDs increasingly per sweep. Stay if no positive gain exists.
  Otherwise choose the greatest gain among the other existing community and
  a fresh singleton; ties favor P or Q over the fresh token.
- Supplied proposal contains f accepted flips (sweep, vertex, destination),
  sorted lexicographically by sweep/ID, with each vertex appearing at most
  once. Every destination is the opposite initial label. Both communities
  must remain nonempty after every proposal prefix.
- Every nonterminal sweep has at least one proposed flip; terminal sweep S
  has none, so S<=f+1. f=0 means S=1.
- The verifier must prove the proposal is exactly the routine's entire
  accepted trace, including the no-move terminal sweep. It must reject an
  omitted profitable move, a nonpositive flip, a superior fresh destination,
  a wrong visit order, an early no-move sweep or a wrong stop count.

This verifier need not assume a matching or uniform degrees. Those structural
assumptions may still be needed to GENERATE a cheap useful candidate trace.
It is not a general fast community solver merely because verification succeeds.

## 1. Express Decisions Using Two Numbers

For vertex u let du be its fixed degree. Let a_u(s) be the total weight of
its neighbors in Q immediately BEFORE u's visit in sweep s. Let z_u(s)
be the total graph degree in Q at that same instant. With u in P:

```text
G_switch = T*(2*a_u-du) - gamma*du*(2*z_u-T+du)
G_fresh  = -T*(du-a_u) + gamma*du*(T-z_u-du).
```

With u in Q:

```text
G_switch = T*(du-2*a_u) - gamma*du*(T-2*z_u+du)
G_fresh  = -T*a_u + gamma*du*(z_u-du).
```

Actual modularity change is 2G/T^2. On an interval of sweeps where u's label
and a_u are fixed, both gains are affine in z_u. For P, their maxima occur at
the minimum z_u; for Q, maxima occur at the maximum z_u. Therefore all skipped
visits in such an interval are safe iff BOTH maximum gains are <=0.

At u's proposed accepted visit, check the actual values and require
G_switch>0 and G_switch>=G_fresh. The declared token tie rule matters here.

## 2. Build Only The Affinity Change Points

Initial affinities can be computed by scanning the labeled source once.
If neighbor v flips in sweep s_v, its effect reaches u's observed affinity at

```text
effective sweep = s_v       if ID(v) < ID(u)
                  s_v + 1   if ID(v) > ID(u).
delta affinity  = +w_uv     if v flips P -> Q
                  -w_uv     if v flips Q -> P.
```

No self-loop case exists. Each vertex flips at most once, so the total number
of these directed neighbor-update records is at most 2M. Group them by vertex
and effective sweep, combining simultaneous adjustments exactly.

u's own label changes for its observed visits starting in sweep s_u+1, not
at its pre-move visit in s_u. Cut the visit timeline at all affinity changes,
at the own-label change, and around the one accepted visit. Updates effective
at sweep 1 apply before that first visit. The remaining intervals cover EVERY
skipped visit, including the final no-move sweep. There are O(n+M) intervals
in total, not n*S, because labels and affinities have few change points.

## 3. Represent Global Volume At Sampled Visits

The trace gives exact Q-volume changes: add du on P->Q and subtract du on
Q->P. For each sweep, emit constant-volume pieces in original-ID rank space.
If the next accepted vertex has rank j, the PRE-MOVE volume applies through
j inclusive. Apply its degree change only to ranks after j. Retain the final
suffix, including an entire sweep if it has no events.

There are at most f+S<=2f+1 pieces (sweep, ID-rank interval, Q-volume). They
partition the n-by-S visit grid, without materializing all its cells.

```text
              vertex visit rank
           0 ---------------------- n-1
sweep 1    [ z0       ][ z1 ][ z2      ]
sweep 2    [ z2 ][ z3                 ]
sweep 3    [ z3                       ]  terminal
                      ^
                      |
              one vertex's sampled visits
```

For a skipped interval [a,b] of vertex u, query the minimum and maximum
volume among pieces covering its rank and a sweep in [a,b]. Extrema over
ALL continuous event times would be safe but potentially conservative; use
the actual sampled visits to aim for a complete, not merely sufficient, check.

## 4. Concrete Offline Range Index

A simple research implementation can use a segment tree over ID ranks.
Decompose each constant-volume rank interval into O(log n) canonical nodes.
At each node retain its (sweep, volume) entries in increasing sweep order,
plus a range-min/range-max structure on those entries. Disjoint pieces in
one sweep cannot place conflicting values in the same canonical node.

To query a vertex's sweep interval, visit its O(log n) ancestors. Binary-search
the sweep range in each node's list and query that list's min/max structure.
Every sampled visit belongs to exactly one covering piece along that path;
combining results therefore returns its exact extrema. Empty subranges are
ignored, not interpreted as zero.

This suggests O(n+f log n) index records, O(f log n) volume insertions and
O(log n * log(S+1)) work per interval query with ordinary per-list segment
trees. Add sorting/grouping of O(n+M) affinity changes. One plausible total
bound is O((n+M) log(n+M) + (n+M+f) log n log(S+1)) operations and
O(n+M+f log n) records in a straightforward resident prototype. Bit widths
remain charged. This is NOT a constant-RAM preparation algorithm.

The two uses of segment trees are established data structures. A potential
contribution would be this exact semantic reduction from a quadratic visit
grid, its resource-aware compilation, and a useful complete workload, not
claiming range-minimum queries as new.

## 5. Soundness And Completeness Target

Assuming correct index construction, the check should be equivalent to direct
serial replay for the stated single-flip proposal:

1. Source data plus the accepted proposal determine labels and Q volumes at
   every hypothetical visit, and therefore every neighbor affinity.
2. Affinity/label change intervals partition all skipped visits. The range
   queries return exactly the relevant volume extrema, so affine sign checks
   are necessary and sufficient for every visit in that interval to stay.
3. Point checks validate every proposed positive move and its best destination.
4. Induction in visit order identifies the proposal with the actual routine,
   including the separately verified terminal sweep.

The proof is not complete merely because these four assertions are plausible.
Boundary semantics, equal-gain handling, empty ranges, effective-sweep shifts
and proposal structure must be falsified with explicit tiny counterexamples.

## Do Not Hide Answer Preparation

If a conventional expensive solver generates the proposed trace first, fast
verification does not make solving fast. Charge proposal generation, graph
validation, affinity events, range-index construction, scratch and complete
output. Replaying a verified answer cannot be advertised as a fresh query
speedup after omitting the work that computed it.

The original design proposed the HHLL generator as a cheap producer on its
matching warm-state class. It predicted recovery of the 31 unchanged refusals
while rejecting the 64 changed schedules, without universal-band
over-approximation. Section 7 now tests and confirms that prediction for the
fixed cohort. No claim is made that these constructed graphs represent users.

An equally informed sparse event or kinetic-priority control may match the
proposed bound. Such a comparator and nearest-art inspection are mandatory
before treating the reduction as paper-worthy.

## Original Verification-First Plan

1. Exhaustive tiny-grid volume pieces and min/max queries versus materialized
   visits; test events at first/last ID and adjacent sweeps.
2. Independent direct serial oracle on tiny weighted graphs. Enumerate valid
   and corrupted single-flip proposals; compare acceptance, not just outputs.
3. Implement affinity change intervals and exact fresh/switch tests. Inject
   omitted profitable moves, wrong before/after timing and early stops.
4. Replay all 150 fixed heterogeneous sources and retain every decision.
5. Count preprocessing/index/event/output records against n*S and a strong
   sparse event-driven verifier. No timing/RAM headline without complete cost.
6. Obtain a separate proof challenge, then consider external construction only
   if this removes a material gate. All seven-family objectives remain open.

Progress: steps 1-5 are implemented with the evidence below; step 6's
independent mathematical challenge is complete and lead-replayed. No external
builder is claimed.

## 6. Stronger Chronological Threshold Control

The first implementation suggested a simpler comparator. Keep the graph's
current labels, degrees and Q-neighbor affinities, but leave the changing
global Q volume z as one scalar. For a positive-degree vertex, rearrange
the two nonpositive-gain inequalities into one interval constraint on z:

```text
P vertex stays iff z >= L_u, where
  L_u = max((T*(2*a-du) + gamma*du*(T-du))/(2*gamma*du),
            T-du - T*(du-a)/(gamma*du)).

Q vertex stays iff z <= U_u, where
  U_u = min((gamma*du*(T+du) - T*(du-2*a))/(2*gamma*du),
            du + T*a/(gamma*du)).
```

An isolate has both gains zero and imposes no constraint. Strict inequality
at a bound indicates a profitable move; equality must remain admissible as
a stay. Store max(L) for P vertices and min(U) for Q vertices in a segment
tree over visit ranks. Missing lower/upper bounds mean unbounded, not zero.

```text
current sweep: [already checked] [skipped range] [proposed move] ...
                                        |              |
                        query max(L), min(U)       compare gains
                                        |              |
                        accept all iff L<=z<=U    change labels,
                                                  z and neighbors
```

Between consecutive accepted events, no labels, affinities or global volume
change. The range aggregate is therefore NECESSARY AND SUFFICIENT for every
vertex in the skipped rank interval to stay. At the proposed event, compare
its switch gain to zero and to the fresh gain. After a verified switch:

1. Update its label and threshold.
2. Add/subtract its degree to/from z.
3. Add/subtract its edge weights from neighbors' Q affinities and update
   their thresholds. Updates include already-visited neighbors, but those
   ranks are not queried again until their next legitimate sweep visit.
4. Continue after the moved rank. Verify the sweep suffix and finally the
   complete terminal no-move sweep.

Induction on accepted events proves exact trace parity: the skipped range
contains no legal move, the next declared event is the prescribed best
positive choice, and the state transition is exact. This proof uses actual
chronological ranges rather than hypothetical graph subsets or unsampled
intermediate times. The offline verifier's different grouping remains a
useful implementation cross-check, not a mandatory execution component.

With ranked source IDs supplied, construction uses O(n+M) operations. If
arbitrary original IDs must be sorted, add O(n log n) comparisons. Let
J=sum_{u flips} deg_records(u), counting stored adjacency records rather
than weighted degree. The single-flip constraint implies J<=2M. The control
uses O(n+M+(J+f) log n) arithmetic/comparison operations after ranking and
O(n+M+f) resident records including input/output; the tree itself is O(n).
There are at most f+S skipped range queries. Rational bit growth, allocation,
rank construction and output are not unit-free or omitted. This is not
constant-memory execution and does not yet implement a physical cap.

## 7. Retained Fixed-Cohort Evidence

The [receipt](evidence/community-single-flip-20260921/receipt.json) pins five
new sources and links to the immutable previous 150-source receipt. Every
input hash and previous full oracle trace is checked before comparison.
No perturbation scales, seed filters or graph sizes were added after seeing
the result. The proposal producer uses the supplied anchor/mover partition
and the ACTUAL source's mover matching and warm labels; it runs HHLL without
using the expanded oracle or computing the answer conventionally first.
Its matching discovery scan, rank map and retained proposal are counted.
Discovering a useful anchor/mover partition from arbitrary data is still open.

| Fixed input category | Cases | Offline verifier | Chronological control |
| --- | ---: | --- | --- |
| Earlier certificate admitted | 55 | All accepted | All accepted |
| Earlier certificate refused, trajectory unchanged | 31 | All accepted | All accepted |
| Actual trajectory changed | 64 | All rejected | All rejected |
| Total | 150 | 86 accepted,64 rejected | 86 accepted,64 rejected |

Each verifier checks all 7,232 final labels and 344 accepted moves with exact
gains on its 86 admissions. This changes the supported-input gate from 55
to 86 cases in this cohort. It is NOT a percentage of real graph workloads.
Rejecting changed proposals does not mean their graph cannot be clustered.

The logical accounting below sums the SAME 86 admitted cases. Summed cells
across separate jobs are not simultaneous peak RAM.

| Quantity | Expanded visit model | Offline volume index | Chronological threshold control |
| --- | ---: | ---: | ---: |
| Logical serial visits covered | 22,656 | 22,656 | 22,656 |
| Accepted point checks | 344 | 344 | 344 |
| Skipped intervals checked | Individual visits | 8,651 | 568 |
| Neighbor-affinity updates/events | Competent cached baseline also pays them | 1,376 records | 1,376 updates |
| Index query work | None | 67,421 rank-path nodes +20,680 inner tree nodes | 1,672 range-tree nodes |
| Index update work after build | None | Offline prepared lists | 1,720 updates /12,955 tree nodes |
| Retained aggregate tree cells | None | 3,402 plus1,701 sweep/index entries | 14,464 |

Initial graph scans, vertices, affinity dictionaries, proposal generation,
output and construction are additional costs. The cohort contains only a few
sweeps, making the sparse offline index small; the chronological tree has
MORE aggregate cells here. Therefore neither the query-count reduction nor
the better asymptotic bound establishes an end-to-end time or RAM win. A
competent cached scalar baseline avoids the deliberately expensive full
objective recomputation used as a correctness oracle.

Rejected proposals can require much of the preparation and checking work
before refusal. Their retained diagnostics are not a complete measurement
of failure plus fallback cost; the table above intentionally covers admissions
only. A deployed speculative fast path must charge this rejected work too.

Source-only proposal generation over these admissions scans 7,232 mover
adjacency records, retains 1,808 mover records and consumes 1,003 frontier
records while producing the344 moves. These are not free preparation.

Receipt SHA-256:
`5ab0af9c2b41c84648e9de53455090071cecad612892c39a9f81b1711b0838be`.

## 8. Falsification And Scope

The first five offline tests failed because the module was absent. The new
control then had six expected absent-module failures; its inherited volume
index regression continued to pass. The matching producer separately failed
before implementation. The final focused suite has14 passing tests;45 tests
pass with the frozen frontier/warm/heterogeneous/review suites included.

Both verifiers separately face 8,064 exhaustive three-vertex proposals across
graph supports, partitions, resolution values and event/stop choices. An
independent objective-difference oracle supplies140 seeded weighted graphs
per verifier, including unsupported fresh/repeated/empty-community traces,
omitted moves and no-move cases. Range-index tests compare materialized visits;
threshold tests cover exact boundary equality, isolates, rational weights,
updates and brute-force rank ranges. Inherited fixture tests are repeated
coverage, not an additional independent experiment. The separate mathematical
review below challenges the proof without importing these implementations.

No production Rust implementation, native-data admission prevalence, process
RSS cap, latency percentile, arbitrary-community solver or whole-Louvain/Leiden
equivalence has been established. These remain research kernels.

## 9. Next Substantive Gate

Do not spend the next turn extending favorable perturbation grids. The
chronological threshold tree suggests an exact solver that searches for the
first violating rank in the remaining sweep instead of accepting a supplied
proposal. This could remove proposal generation for a two-community warm
trajectory and permit repeated moves. It must stop or hand off explicitly
when a fresh community wins or a community empties; its state does not
silently support an arbitrary number of communities.

This next solver is NOT implemented in the trace-verifier receipt. Its work would
be output-sensitive in accepted moves and their incident records, not
universally bounded: repeated moves can make J much larger than M. Test
exact next-event selection, terminal detection and safe handoff first, then
compare complete preparation/execution/output to a cached serial baseline
on declared native warm starts. Prefer a concrete negative result over a
claim based only on skipped visit counts. The full seven-family innovation
objective remains open.

Follow-on: the separate [proposal-free threshold solver](Communities-Proposal-Free-Threshold-Solver.md)
is now implemented with its own immutable receipt. It completes all150 of
the fixed sources, including64 changed matching schedules and19 cases with
repeated movers, without a supplied proposal. It verifies13,440 labels and
813 exact moves against the oracle. Its new search/handoff procedure is not
covered by the independent verifier review and still needs that challenge
and a meaningful native-workload comparison before physical benchmarking.

## 10. Targeted Prior-Art Check

Primary sources inspected2026-09-21. This is a bounded comparison, not a
systematic novelty clearance. These sources inform the distinction between
changing the optimization trajectory and certifying a prescribed one.

- [Waltman and Van Eck, A smart local moving algorithm for large-scale modularity-based community detection](https://arxiv.org/pdf/1308.6604),
  Section2's local-moving description: repeated best positive moves, including
  possibly empty destinations, and dependence on visit order are established
  foundations. Their stated aim includes improved modularity quality, not the
  particular two-label sampled-visit certification contract developed here.
- [Grappolo source repository README](https://github.com/ECP-ExaGraph/grappolo),
  description and syncType/basicOpt options: parallelism, early termination,
  synchronization and map/vector memory tradeoffs are explicit prior work.
  This source warns that processing order and heuristics affect outcomes;
  therefore matching only the final quality value is not equivalent to our
  exact accepted-trace test. No claim is made that its optimized source has
  been exhaustively ruled out as an equivalent special-case control.
- [GenLouvain public implementation](https://github.com/GenLouvain/GenLouvain/blob/master/genlouvain.m),
  API comments: deterministic index-order and alternative move rules already
  exist. A selectable deterministic schedule is not our novelty claim.

The chronological control uses established segment-tree range aggregation.
Its introduction during this work refutes a stronger claim that the offline
two-dimensional representation is necessary. A defensible contribution would
need the exact reduction, useful-source behavior and complete resource costs
to survive comparison against equally informed event-driven implementations.
No result from these sources is represented as our measured performance.

## 11. Independent Challenge Integrated

The [independent review](Communities-Single-Flip-Independent-Review.md) is
complete. The lead read its proof and complete standalone checker, then
replayed the checker with Python 3.11 and warnings as errors (terminal exit0).
It compared164,972 admissible proposals over2,404 graph/partition/resolution
cases against an objective-difference oracle, with zero disagreements for
either verifier. It also checked17,920 gain identities,8,960 threshold
identities and4,294 nonempty sampled-volume queries, and exposed all10
deliberately incorrect verifier variants. Small exhaustive traces only reach
sweep2; separate fixed cases include a genuine sweep3 trace. These counts
do not constitute a native-input or performance experiment.

The review identified a document precondition gap: positive stored weights
do not exclude an edgeless graph. The contract now explicitly requires T>0.
The implementation already rejected zero total degree and its invalid-source
test already covered that case, so no frozen source/receipt changed.

Four separate [review regressions](experiments/test_single_flip_review_contracts.py)
check an existing-community positive tie with an isolated community, an
omitted terminal move, margins of1/10^60 around the zero threshold, and a
fresh destination beating a positive switch. Both verifiers pass; the later
solver also passes these cases. With the solver tests included, the combined
suite has53 passing tests. These lead regressions are not an independent
review of that new solver's full procedure.

The review supports restricted verifier soundness/completeness and the
stronger chronological bound. Its priority-queue history certification,
kinetic certificates and dynamic-community references further narrow the
novelty question. Native usefulness, external preparation, physical caps,
the solver's independent search/handoff challenge and the seven-family
scientific contribution remain open.

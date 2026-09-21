# Similarity: A Compressed Control Must Get The Same Advantage

Date: 2026-09-21. Equivalence proof, implementation, separate mathematical
review and sparse top-k parity study complete. This is a stronger control for
A05, not a new generic optimization method or physical RAM result.

## Premise And Design

The [larger-group study](Similarity-Large-Group-Workload-Gate.md) shows modest
extra pruning but still prepares dense query histograms and evaluates dense
laminar/interval predecessors. Compressing only the last solver cannot prove
a whole-pipeline resource improvement. The control must receive the same
representation advantage as the candidate.

Three choices: retain dense predecessors; discard them and accept a weaker
union-only baseline; or evaluate their exact values on the existing compressed
core. Choose the third. Optimization, compressed execution, verification and
skeptical cost-accounting are the relevant analytical lenses. The requested
research goal already authorizes this bounded proof/probe work; no new engine,
commits, public timing study or product promise is part of this step.

## Contract

Use a trusted modal core compiled for the existing complete-two-row summary:
attained capacity C_v at every original tree node, leaf union populations H_j,
root sizes ell <= u, and sorted positive query counts q_j <= tau. Query size
a may include features outside the union. Return exactly the existing union,
laminar and two-row interval bounds, not the stronger complete-pair envelope.

Static cores have K <= 3r+1 nodes, d modal deviations and O(r+1) owner runs.
The existing sparse query preparation produces a component bundle (L,U,A,B):

```text
L = sum_j (H_j-C_j)             U = sum_j C_j
A = L - sum_j min(q_j,H_j-C_j)  B = sum_j min(q_j,C_j)
f(x) = min(x-A,B),  L <= x <= U.
```

The old modal compiler, query validation and exact solvers remain unchanged.
No adversarial serialized-core decoding or extension of their G<=256 API is
claimed. A relaxed feasible row need not have a globally compatible companion:
preserving that distinction is part of this control's specification.

## Capped-Linear Composition Lemma

Each child has an integer interval [l_i,u_i] and achievable maximum query
overlap f_i(x)=min(x-d_i,b_i), nonnegative and increasing by zero or one at
each successive integer. Normalize its endpoint values p_i=f_i(l_i) and
t_i=f_i(u_i). For independent children with sum occupancy x, the exact
maximum overlap is

```text
l = sum_i l_i                 u = sum_i u_i
d = l - sum_i p_i             b = sum_i t_i
f(x) = min(x-d,b),  l <= x <= u.
```

Proof: starting at all lower endpoints gives sum p_i overlap. Each child has
t_i-p_i unit-reward increments followed by zero-reward increments. Allocate
unit-reward increments first. Every integer total through u is realizable;
the reward is sum p_i plus min(x-l, sum(t_i-p_i)), which is the formula.
Thus grouping and reordering independent children does not change the value.
This is ordinary separable capped-linear resource allocation, not new theory.

At an original strict node, combine its children and then intersect their
occupancy interval with [H_v-C_v,C_v]. The reward formula is unchanged on
the restricted domain. When combining this result higher up, use its newly
restricted endpoint rewards, not its former untruncated endpoints.

## Why Deleted Additive Nodes Are Redundant

At an additive node C_v=sum C_child and H_v=sum H_child. Child intervals
already imply sum lower >= sum(H_child-C_child)=H_v-C_v and
sum upper <= sum C_child=C_v. Therefore that node imposes no extra interval
restriction. Remove a connected additive region, combine all its ordinary
leaves into the generalized bundle, and leave every strict boundary child.
The composition lemma preserves the interval frontier at its top. Induction
over the compressed tree proves equality with the full-tree interval control.

For the laminar bound, a leaf contributes min(q_j,C_j). At strict nodes cap
the sum of child ranks at C_v. At additive nodes that cap is redundant;
the ordinary-leaf bundle contributes B. This proves equality with the old
laminar rank in the same O(K) traversal. The union bound only needs total
query/union intersection and root capacity.

At the root, evaluate the interval frontier at ell and u when feasible, just
as the existing population-two control does. For score overlap c and size x,
use c/(a+x-c), with zero overlap mapped to zero. This is an upper bound for
the actual rows, not a replacement for verifying candidate similarities.

Union and laminar keep a DIFFERENT scoring rule: for their rank R, return
R/(a+max(ell,R)-R), or zero when R=0. Applying interval endpoint scoring to
them would change the baseline despite equal ranks. For an empty/full pair
over two singleton leaves with one queried element, union and laminar return
1 while interval returns 1/2. The small implementation corpus includes this
case; the independent checker names it.

Clipping stays attached to its strict subtree. A child with min(x,3) restricted
to [0,1] contributes at most one, not its old ceiling three. Combining it with
a zero-reward child on [0,2] gives reward one at total size three. Equality
concerns frontiers on their current domains, not byte-identical scalar tuples.

### An Immediate Dispatch Corollary

When r=0, every original split is additive. The major row occupies C_j in
every leaf and the minor row can allocate its size in [H_j-C_j,C_j]. The
single generalized bundle therefore gives not only the interval relaxation
but the exact complete-two-row envelope. No branching or threshold DP is
needed in this case. Query/union validation and feasible root sizes remain
required. For r>0 equality may also happen, but it cannot be assumed.

The companion construction is explicit. Fix a chosen minor subset of size
x_j. Its complement has H_j-x_j<=C_j elements; fill that complement with
C_j-(H_j-x_j) minor elements to obtain the major row. Conversely, fix a
chosen major subset of size C_j, and make the minor contain its H_j-C_j
complement plus x_j-(H_j-C_j) major elements. Both constructions respect the
union/capacities in every leaf and hence every additive ancestor. Marginal
optima may use different complete pairs.

## Resource Scope

Given the prepared query core, one postorder evaluation uses K logical state
records and K-1 child reductions. A record stores interval scalars, union
population and laminar rank; it is not one byte or one machine word. O(K)
arithmetic has integer/Fraction bit costs. Sparse preparation is separately
O(r+1+s log(d+2)), retaining an additional K query-core records, and the
static summary is O(r+d+1) records. The original build remains paid.

No G-sized vector is accepted by the query evaluator. The new sparse top-k
driver below also removes dense histograms from that query path; the earlier
frozen public driver is unchanged. Evaluator-record refusal occurs before
consuming a single-pass query stream. Upstream aggregation and the caller's
resident source are not covered by this evaluator-only budget.

## Falsification And Implementation Plan

1. Add `experiments/test_compressed_interval_similarity.py`; observe an
   assertion for absent implementation before writing the implementation.
2. Add `experiments/probe_compressed_interval_similarity.py` with separate
   prepared-core and sparse-wrapper entry points. Preserve all old helpers.
3. Compare all three bounds with the independent full-tree implementations
   over complete small row/query cases and seeded larger groups. Compare
   r=0 with the exact companion-aware evaluator. Retain a strict separating
   example and an infeasible-companion relaxation example.
4. Check bundle forced occupancy, strict clipping, empty/no-overlap queries,
   invalid sparse counts, exact record-budget boundary, single-pass streams,
   numeric huge counts and fixed-r scaling. No timing assertions.
5. Obtain a separate bounded mathematical challenge while the lead implements
   and verifies code. Review owns only its own Markdown file.
6. Only after equivalence is established, plan sparse top-k integration with
   same-G interval and posting-merge controls. Do not mutate frozen receipts.

## Implemented Checks

The narrow absent-module test failed before implementation. Seven new tests
then passed: 17,408 complete small row/query/control cases at G=1,2,4,8;
180 seeded deeper cases through G=256; forced-occupancy bundle; strict
relaxation/companion distinction; single-pass/budget boundaries; fixed-r
counts; huge integers; and invalid sparse inputs. The r=0 cases also match
the existing exact evaluator. These are finite reference comparisons, not
17,408 independently sourced workloads.

| G, fixed r=1 | Control records | Child reductions | Query-core copies |
| ---: | ---: | ---: | ---: |
| 4 | 4 | 3 | 4 |
| 16 | 4 | 3 | 4 |
| 64 | 4 | 3 | 4 |
| 256 | 4 | 3 | 4 |

The [independent mathematical review](Similarity-Compressed-Interval-Review.md)
supports the qualified theorem and adds the scoring, clipping and companion
details above. Its standalone checker was read and lead-replayed without
project imports: 42,875 composition triples, 10,339 clipped domains, 16,089
retained subtree frontiers, 10,874 complete score cases and 3,962 zero-strict
exact-envelope cases. It finds 1,333 expected strict relaxation gaps. This
reviews mathematics, not the implementation or historical priority.

## Sparse Top-k Integration

The [new query probe](experiments/probe_sparse_control_similarity.py) consumes
the same sorted original-ID pair blocks and relevant-block postings. Each
feature cursor emits `(block_id, feature_id % G)`. Lexicographic merge groups
each block's positive leaf counts in order without initializing a G-entry
array, building a leaf dictionary or sorting that block's counts afterward.
Raw query deduplication/sorting still costs O(a) storage and O(a log a)
comparison work, and the merge retains O(a) cursor/heap state.

For each block, hold its s positive pairs, apply the scalar union check, and
evaluate the compressed control if required. A singleton block's union score
is already its exact Jaccard score. Profile or record refusal preserves union
and verifies candidate bodies; it cannot create an unsafe prune. No broad
exception handler hides invalid metadata. Top-k witnesses, self exclusion,
exact Fraction ties and ascending-ID zero tails retain the old semantics.

The compact query view retains neither dense capacity/population arrays nor
the target-posting index used by DAAT. It DOES retain target bodies, sizes,
block postings, modal cores and output witnesses. This is not an external-memory
engine. Its builder first uses the old resident preparation and then discards
dense structures. The build peak is not reduced or measured. The old module
remains frozen to preserve earlier receipts.

Three additional tests first observed an absent-module failure, then passed
3,072 complete tiny top-k comparisons plus odd/singleton/zero/self/tie cases,
same-body counters and actual profile/record refusals. An initial refusal
fixture was already pruned by a score-one union witness; changing that witness
to score 3/4 made the intended path reachable without changing implementation.
The fresh full similarity suite passes **132 tests**, terminal exit zero with
warnings as errors. Its 24.080-second duration is test cost, not a benchmark.

### Frozen Public Parity

The new [receipt](evidence/similarity-sparse-control-20260921/receipt.json)
checks all 180 interval outputs from the earlier large-group study: both sources,
G=4,16,64, and the same thirty query IDs each. Every complete row and declared
body/posting/zero counter matches the frozen interval receipt. This is parity
against previously oracle-checked outputs, not a new independent oracle or a
rerun of the earlier expensive exact solvers. There are sixty distinct source
queries reused across group counts.

For this study tau equals total source adjacency memberships: 28,968 for GrQc
and 176,468 for Facebook. This broad profile admits every possible query/leaf
intersection in this finite feature universe and leaves signatures unclipped.
It is NOT the old tau=8 profile and can retain more deviations. No profile or
record refusal occurred on these public queries.

| Dataset | G | Old dense histogram cells initialized | New positive pairs constructed | Peak positive pairs per block | Total query-core copies | Peak control records |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GrQc | 4 | 5,744 | 2,422 | 4 | 824 | 7 |
| GrQc | 16 | 22,976 | 3,946 | 16 | 1,848 | 16 |
| GrQc | 64 | 91,904 | 5,884 | 44 | 3,663 | 31 |
| Facebook | 4 | 50,944 | 25,852 | 4 | 3,213 | 7 |
| Facebook | 16 | 203,776 | 52,008 | 16 | 10,041 | 23 |
| Facebook | 64 | 815,104 | 96,254 | 64 | 35,525 | 65 |

At G=64 the view discards 500,611 GrQc and 385,820 Facebook dense scalar
summary values. It retains respectively 16,702/37,507 core nodes,
13,374/28,871 ownership runs, 23,039/56,301 deviating signatures, one default
slot per core node and one key per deviation. These heterogeneous records
have several fields and Python overhead. The receipt retains construction
counts; none of these numbers is measured bytes or RSS.

The new control computes interval and laminar together after union fails;
the old path sometimes stops at laminar. Thus instruction work is not identical
even though all body decisions match. G=64 surviving modeled members remain
11,940 and 206,686: this representation adds no pruning. DAAT's 7,260/175,952
expanded memberships and zero body reads remain the stronger independent
execution alternative. There is no new timing comparison against it.

Receipt SHA-256:
`d32b332814b720f3b20737ae1197c0766c98963e3edf183e331a5e14c5dd053c`.
All nine code hashes match. No elapsed-time or physical-RAM fields are recorded.

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest test_compressed_interval_similarity test_sparse_control_similarity -q
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest discover -p 'test*similarity.py' -q
```

## Next Decision

The dense-predecessor obstacle is removed in a resident query prototype. Next
share ONE sparse-prepared core between the interval control and exact branch/
dual portfolio, avoiding repeated preparation and bypassing exact solvers when
r=0. Before another timing study, charge full profile retention, preparation,
all refusal work, candidate verification and the DAAT alternative. A serialized
packed layout and bounded ingestion remain separate missing deliverables.
The present result does not supply those or establish a broad practical win
from the modest 4.1% extra pruning observed in the earlier workload gate.

## Prior Art And Contribution Boundary

[Federgruen and Groenevelt, 1986](https://pubsonline.informs.org/doi/10.1287/opre.34.6.909)
studied greedy resource allocation under polymatroid constraints with concave
objectives. The inspected publisher abstract supports crediting this broad
optimization setting; it is not evidence of a specific two-row summary theorem.
[AWARE](https://baunsgaard.github.io/assets/pdf/AWARE.pdf) explicitly makes
compression and execution planning workload-aware. Compression alone is not
a new contribution. The earlier additive-region proof is the local prerequisite.

The result sought here is a precise equivalence that strengthens our own
baseline and removes an avoidable G-dependent step. It cannot by itself meet
the user's request for seven substantial, publication-worthy innovations.

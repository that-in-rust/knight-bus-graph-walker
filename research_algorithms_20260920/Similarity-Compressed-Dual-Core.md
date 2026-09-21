# Similarity: Query-Dual Evaluation On Compressed Cores

Date: 2026-09-21. A05 mathematical and count-prototype increment. Independent
theorem challenge completed and its checker lead-replayed. No physical RAM, end-to-end speed, or
publication-priority claim.

## Abstract

Two earlier exact routes have different weaknesses. Branching on the r strict
capacity splits removes dependence on large numeric overlap, but costs 2^r.
An inverse query-overlap dynamic program avoids that enumeration, but its
original implementation retains tables on the full G-leaf feature tree.
This note composes the reviewed additive-region reduction with the inverse
recurrence. The key requirement is a generalized bundle frontier, not treating
an aggregate region as an ordinary feature group.

After the paid modal summary and sparse-query preparation, construct a binary
evaluation plan with at most 2K-1 nodes, where K<=3r+1 is the core size. Let
B=sum_j min(q_j,C_j)<=Q=sum_j q_j. The resulting exact complete-two-row envelope
uses O((r+1)(B+1)) threshold cells and O(r+1+B(r+1)+B^2) counted arithmetic work.
Neither G, the numeric target intersection I, nor 2^r appears in that repeated
evaluation bound. Static modal records still cost O(r+d+1); build time/space,
numeric bit width, raw query mapping and actual target verification are paid.

This is a composition theorem for the specified attained-maxima problem, not
a new min-plus operation, generic DP method, or universal graph algorithm.
The finite implementation checks are affirmative. Their public-workload value
and scientific priority are separate unresolved questions.

## 1. Why This Is The Next Question

The [fixed-workload profile](Similarity-Exception-Workload-Profile.md) provides
a relevant warning: at G=64 the Facebook all-pair r median/p90/maximum is
5/13/28. Small r in the old G=4 receipt cannot justify unrestricted 2^r work.
Modal signature deviations can be much smaller than G, so discarding the
compact storage merely because branching is expensive is premature.

Three choices are explicit:

1. Keep the compressed orientation evaluator and refuse large branch counts.
2. Expand back to the original feature tree and use the existing query-dual DP.
3. Preserve the compressed core but evaluate it through generalized inverse
   frontiers. This is the implemented candidate in this note.

The third option is not uniformly best. Sparse leaf support s is not small
numeric overlap Q: one nonzero leaf may contain a huge count. An honest
portfolio must retain the first route as well. No new automatic dispatcher or
complete top-k pipeline is implemented here.

Expert lenses used: exact combinatorial recovery, parameterized resource
analysis, query-engine cost accounting, and skeptical counterexample design.
These are analytical viewpoints, not independent endorsements.

## 2. Exact Contract

Use the same two-set contract as the [core manuscript](Similarity-Additive-Region-Core.md).
The union is partitioned into G leaves. Each tree node retains the **attained**
maximum C_v of exactly two rows; the root sizes are ell<=u=C_root. Query size a
includes elements outside the union. The result maximizes Jaccard over either
row and every complete pair consistent with the observations. It does not
recover the original pair, jointly maximize both row rewards, or change an
upper-bound filter into the final exact top-k output.

The query-limited modal compiler retains the original hard capacities and
component L,U, plus clipped signatures for leaf reward/validity lookup.
Only sorted positive q_j<=tau streams are admitted. Duplicate/invalid leaf
IDs, excess counts and inconsistent row sizes are errors, not zero bounds.

The prototype consumes trusted in-process cores returned by the existing
compiler. It is not an adversarial serialized-core decoder. The compiler's
balanced G=1..256 interface remains unchanged. Its O(G log G) comparisons and
O(G) temporary records are not reduced by this query evaluator.

## 3. The Generalized Bundle Lemma

An additive component retains an ordinary-leaf bundle (L,U,A,B). Write

```text
L = sum_j (H_j-C_j)           U = sum_j C_j
b = L-A = sum_j min(q_j,H_j-C_j)
B = sum_j min(q_j,C_j)
```

With that component's row orientation fixed, the major bundle row has size U
and best overlap B. The minor row may have any integer size x in [L,U], with
best overlap min(x-A,B). This is the previously reviewed capped-linear
allocation lemma, including constructive lifting to the original leaves.

Define bundle intersection e=x-L. The union population is H_bundle=L+U,
capacity C_bundle=U, and 0<=e<=U-L. For each requested overlap c=0,...,B:

```text
major_threshold[c] = 0
minor_threshold[c] = max(0,c-b)
```

Each threshold is the smallest feasible e attaining at least c overlap; every
larger e up to U-L also works. The condition B-b<=U-L follows leafwise from
min(q,C)-min(q,l)<=C-l. Therefore the last threshold is within the feasible
range, and threshold zero correctly carries the bundle's feasibility.

### Why A Companion Still Exists

The lemma optimizes one scored row at a time. For each leaf, the other row can
contain its union complement plus the required number of shared elements.
When the scored row is minor, its chosen size is at least H_j-C_j and at most
C_j, allowing a companion of size C_j. When the scored row is major, choose
the companion's occupancy in that same integer interval. Disjoint leaves
then combine. This does not assert that both individually optimal query
overlaps are simultaneously attainable by the same pair; the final objective
does not require that stronger statement.

### Falsified Shortcut: Ordinary Aggregate Leaf

For leaf triples (H,C,q)=(3,2,0),(3,2,3), the correct bundle has
(L,U,A,B)=(2,4,1,2). Its minor rewards at x=2,3,4 are 1,2,2.
Replacing it with H=6,C=4,q=3 would produce 2,3,3, which is false.
The generalized bundle's separate b and B are essential. The retained test
checks all three root sizes against exact envelope values.

## 4. Compressed Evaluation Plan

The prepared core is a tree of strict nodes and additive components. A
component's top capacity equals its bundle U plus the capacities of all its
strict-boundary children. Its population is L+U plus their populations.

Replace each component by one generalized bundle leaf and an ordered sequence
of binary additive merges with its boundary children. Keep each strict node
with its two ordered children and original capacity. This is a small logical
plan; it does not expand features or restore deleted original additive nodes.

If E_c is the number of child edges belonging to component nodes, the plan has
P=K+E_c<=2K-1 nodes. No child subtree is duplicated. A component with no ordinary
leaves still has a zero bundle, so adjacent strict nodes and empty components
need no special false-feasibility shortcut.

The bundle projection/lifting theorem preserves exactly the retained boundary
count tuples and the best scored-row overlap for each tuple. Summing additive
children preserves their independent populations and intersections. At a strict
node the original attained-maxima disjunction is unchanged. Consequently the
old inverse recurrence applies with the generalized bundle bases above.

## 5. Explicit Recurrences

For each plan node retain major/minor tables t_M[c],t_m[c], indexed by requested
overlap c up to the sum of descendant bundle B values. None denotes infeasible.
For an additive binary node, each role uses ordinary min-plus convolution:

```text
t_role[c] = min_(i+j=c) (t_left_role[i] + t_right_role[j]).
```

For a strict node let D=C_left+C_right-C_parent>0 and define
X=2*C_left-H_left-D, Y=2*C_right-H_right-D. Let val(t,z) be the greatest
index c with t[c]<=z, or infeasible if even c=0 is not admitted. Major branches:

```text
Y + t_left_M[max(0,c-val(t_right_m,Y))],  requiring t_left_M[...] <= X
X + t_right_M[max(0,c-val(t_left_m,X))],  requiring t_right_M[...] <= Y
```

Minor branches:

```text
X + t_right_m[max(0,c-val(t_left_M,X))],  requiring t_right_m[...] <= Y
Y + t_left_m[max(0,c-val(t_right_M,Y))],  requiring t_left_m[...] <= X
```

Take the minimum feasible branch. An out-of-range score, infeasible fixed child,
negative feasible-intersection limit, or absent variable threshold invalidates
that branch. The fixed child and variable child account for both rows, including
the companion. At the root evaluate at I=ell+u-H_root, then compare
c_M/(a+u-c_M) with c_m/(a+ell-c_m). Zero overlap has score zero.

This is the earlier inverse recurrence on a different, justified input plan.
Its monotonicity follows from the original laminar augmentation argument or
directly from the generalized leaf rules and the inductive recurrences. It does
not rely on concavity of the full forward frontier, which earlier examples
already falsified.

## 6. Resource Reservation And Proof

Let B_v be the sum of bundle ceilings below plan node v. The implementation
first computes the complete O(K)-record plan and the following reservations,
without allocating any threshold table:

```text
S = 2 * sum_v (B_v+1)

W = 2*(B_root+1)                       root lookups
  + sum_bundle 2*(B_v+1)               bundle table writes
  + sum_additive 2*(B_left+1)*(B_right+1)
  + sum_strict [4*(B_v+1) + 2*(B_left+B_right+2)].
```

Reject if S or W exceeds the caller's cap. An exception is explicit; it is
never interpreted as a pruning score. Unlike branch admission before sparse
query consumption, these exact dual reservations depend on query rewards:
sparse preparation and O(K) planning have already occurred. The caps cover
threshold cells and counted recurrence steps, not all Python allocations,
integer arithmetic time, validation, planning, or process RSS.

The plan is a tree with O(r+1) vertices. Each query reward unit contributes to
B_v along at most O(r+1) ancestors, giving S=O((r+1)(B+1)). For additive
convolutions, sum B_left*B_right counts each pair of reward units from distinct
bundles at most once, at its lowest common ancestor. It is at most B^2/2.
All remaining terms are O(r+1+B(r+1)). Thus

```text
sparse preparation: O(r+1 + s*log(d+2)) arithmetic
dual evaluation:    O(r+1 + B*(r+1) + B^2) arithmetic
threshold cells:    O((r+1)*(B+1))
static summary:     O(r+d+1) records
```

The implementation retains all plan tables. It does not claim an optimal
postorder lifetime schedule. Auxiliary O(r+1) plan/core references are additional
to S. Large exact counts still require their bit widths; Fractions and their
normalization/comparison are not constant-bit operations. Build scratch, target
storage, feature mapping, top-k witnesses and output remain outside this kernel.

## 7. Two Separating Regimes And A Negative

The executed one-conflict family has two leaves H=4,C=3 below a strict parent
C=4. All G-2 other leaves have H=C=2, and all other nodes are additive. The
query has two features in each conflicting leaf. The exact envelope is
3/(2G+1). The supplied-count profile has d=0 and K=4 at every tested G.

| G | r | Plan nodes | Reserved threshold cells | Counted recurrence steps |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 1 | 5 | 34 | 66 |
| 16 | 1 | 5 | 34 | 66 |
| 64 | 1 | 5 | 34 | 66 |
| 256 | 1 | 5 | 34 | 66 |

These counts remove G from evaluation, not compilation. The older branching
core also has constant work here and may be better. Different implementations'
work counters are not hardware speed ratios.

For the many-conflict family, every leaf instead has H=4,C=3, every sibling pair
has parent C=4, and all higher nodes are additive. The same four-feature query
still gives 3/(2G+1), but r=G/2:

| G | r | Plan nodes | Reserved threshold cells | Counted recurrence steps |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 2 | 9 | 50 | 88 |
| 16 | 8 | 33 | 146 | 220 |
| 64 | 32 | 129 | 530 | 748 |
| 256 | 128 | 513 | 2066 | 2860 |

The largest case executes. The 2^128 branch route is refused before enumeration;
those assignments were not run. The original full-tree dual also avoids that
exponential work. The contribution is **combining** that capability with the
compressed preparation, not asserting that the new solver invented the
polynomial-in-query route or is faster on this high-r family.

The opposite regime is retained: G=1, H=C=q=10^100, ell=C. The one-branch
evaluator returns score one using fixed-count records. The dual requests
2*(10^100+1) cells and refuses before any table allocation. Neither route
dominates the other. A small support size s=1 does not save this dual route.

## 8. Verification And Reproduction

Seven missing-module assertions were observed before implementation. The
[new tests](experiments/test_compressed_dual_similarity.py) then passed in
0.331 seconds; this duration is test cost, not a performance comparison.
The complete similarity suite subsequently passed **116 tests in 25.823
seconds**, exit zero, including the unchanged fixed-profile 52,636 accepted
and 6,244 intentionally refused actual-summary cases.

- 29,440 complete compatible-pair/query envelopes match independent enumeration
  of feature sets, with outside-union query elements. This reuses the earlier
  finite corpus rather than claiming 29,440 new datasets.
- 240 seeded deeper cases at G=1,2,4,8,16,32 match the distinct full-tree
  inverse evaluator. This is a reference cross-check, not independent exhaustive
  enumeration of those larger feature universes.
- Fixed-r G scaling, 128 strict nodes, a generalized-bundle counterexample,
  an empty-query infeasible-companion case, sorted single-pass consumption,
  invalid booleans/duplicates, exact cap boundaries and huge-count refusal pass.
- On the exhaustive corpus, actual counted cells/recurrence steps equal the
  precomputed reservations. The counters deliberately exclude other work.

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest test_compressed_dual_similarity -q
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover -p 'test*similarity.py' -q
```

The completed separate [mathematical challenge](Similarity-Compressed-Dual-Review.md)
accepts the composition with trusted preparation, valid companions, global
infeasibility detection and numeric-Q/bit-width qualifications. The lead read
and replayed its independent standard-library checker: 22,730 subtree threshold
entries, 23,954 inverse values, 8,050 root envelopes (376 correctly infeasible),
and 1,241 root frontiers compared with actual feature-set pairs. It also
executes seven Q=1 ladders, including r=32 with 388 initialized cells. Its
original-tree DP and set enumeration do not import this implementation. This
is mathematical review and finite independent corroboration, not a code audit.

After normalizing six test names to the repository naming convention, the
unchanged seven test bodies passed again in 0.336 seconds. The implementation
is unchanged from the 116-test full run; the current test-file hash is below.

| Artifact | SHA-256 at latest implementation/naming checkpoint |
| --- | --- |
| [Evaluator](experiments/probe_compressed_dual_similarity.py) | `a3b2e2af5be94f4d120090e9eaf29a35ebe021396131cd061a13afa8f2df1bbf` |
| New tests | `8bf43b2bbfcecb41eba23d0a3f8df06d2abc40aaedb8d926900012c4258b877d` |
| Independent mathematical review | `db2ecc61f6e8e76a7b0654ba21bb2d2277c21532ed5a9f036641872fb04ae847` |

## 9. Prior Art And Scientific Boundary

- [Fife and Oxley, Laminar Matroids](https://arxiv.org/abs/1606.08354)
  establish the laminar-capacity matroid setting. Their abstract also explicitly
  discusses compacting laminar presentations. We do not claim to invent
  laminar augmentation or generic compaction. This turn inspected the abstract;
  earlier original-threshold work records its more specific use.
- [Kumabe, Maehara and Sin'ya, Automaton Constrained Tree Knapsack](https://arxiv.org/abs/1807.04942)
  use heavy-light recursive DP to improve pseudopolynomial factors. The
  inspected abstract is a direct warning against claiming a new generic
  tree-DP acceleration; it is not a proof that their algorithm solves this
  attained-two-row model unchanged.
- [Baunsgaard and Boehm, AWARE](https://baunsgaard.github.io/assets/pdf/AWARE.pdf)
  supplies direct default-tuple compression precedent and emphasizes charging
  preparation together with compressed operations. Modal coding here is
  inherited machinery, not the proposed new scientific result.
- The repository's [inverse-frontier theorem](Similarity-Overlap-Frontier-Compression.md)
  and [reviewed additive core](Similarity-Additive-Core-Review.md) are explicit
  prerequisites. The added result is that generalized bundles preserve the
  required inverse frontiers and give a compact-query dual schedule.

A generic inverse DP granted these proved generalized bundles can implement
the same evaluation. That observation identifies the contribution boundary;
it neither proves historical publication of the reduction nor establishes
novelty. The independent mathematical challenge is now complete; precise
nearest-art comparison, implementation audit and practical evidence remain.

## 10. Product Decision

Retain two exact evaluation modes on the same prepared information. Prefer a
mode only after checking its actual resource reservation; do not infer it from
the words "compressed" or "query sparse." A practical dispatcher would consume
the query once, retain its O(r+1) prepared core, and compare reservations before
executing one mode. That API integration is not implemented in this note.

The earlier negative public top-k receipts remain authoritative. This result
does not create stronger pruning than the same exact envelope, and the G=4
profile has only 105 interval-tight opportunities among 12,542 admitted calls.
Whether larger G makes those opportunities useful must be measured with paid
metadata, query mapping, target reads, preparation and full output. The new
theorem removes one computational obstacle to asking that question; it does
not answer it or complete the seven-family research objective.

### Subsequent Workload And Control Evidence

The [larger-G workload gate](Similarity-Large-Group-Workload-Gate.md) now
checks 900 complete outputs. At G=64, the dual avoids 4.13%/4.10% of modeled
body memberships beyond same-G interval on GrQc/Facebook, with meaningful
recurrence work and dense-first overhead. No physical benefit is established.

The [compressed cheap control](Similarity-Compressed-Interval-Control.md)
then proves the same core also preserves union/laminar/interval values. Its
new sparse query traversal matches 180 frozen outputs/body decisions without
dense query summaries or histograms, but still retains the graph and postings.
That strengthens the baseline instead of crediting generic compression only
to this exact solver. Next expose shared prepared-core evaluation for both
exact schedules and the control; the current solver wrappers still prepare
their inputs separately. Skip unnecessary exact evaluation when r=0, and
charge the full profile, refusal and DAAT costs before a new timing claim.

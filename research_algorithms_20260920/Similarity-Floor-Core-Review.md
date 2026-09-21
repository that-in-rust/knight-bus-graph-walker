# Similarity Floor-Core Independent Review

Date: 2026-09-21. A05 mathematical sidecar. This is the sole owned file.

## Verdict And Scope

**Central falsification: floor-only admission gives no stronger pruning than
ordinary union and row lengths.** On this profile every feasible scored row can
achieve overlap `Q=sum q_j`. Conditional on complete-pair feasibility, the exact
envelope is `Q/(a+ell-Q)` for `Q>0`, and zero otherwise. This equals the ordinary
union/row-length envelope. Reusing the old branching evaluator is correct, but
its reward optimization has become unnecessary. Passing equivalence tests cannot
rescue a claim of stronger pruning on admitted floor-only queries.

**Accept mathematical correctness; reject a stronger-pruning contribution for
this profile.** The owner-run bound is correct, sharp, and admits an exact
cut-count formula. Floor updates reproduce the old component rewards exactly.
Use `s*log(r+2)`, or `s*(1+log(r+1))`, to include reading the query when `r=0`.
Sorted IDs also permit an optional `O(s+r+1)` merge scan; this review does not
assume the lead will implement it. The bounded-exception extension in Section 6
does preserve richer rewards and has a concrete strict separation, but its
exception count and useful prevalence are separate, unestablished claims.
Section 6 also proves the lead's balanced strict family, the exception-position
bit lower bound, and the safety of clipping exception reward capacities.

Read inputs: [Novelty-Baseline-Evidence-Policy.md](Novelty-Baseline-Evidence-Policy.md),
[Similarity-Additive-Region-Core.md](Similarity-Additive-Region-Core.md),
[Similarity-Additive-Core-Review.md](Similarity-Additive-Core-Review.md), and
[Similarity-Static-Core-Query-Updates.md](Similarity-Static-Core-Query-Updates.md).
No floor implementation or project test was read, imported, or executed. The
checker below is newly written, standard-library-only mathematical evidence,
not an audit of the lead's code, allocation behavior, or timings.
The lead's follow-up independently identified the same collapse before any
floor implementation; the ten reported RED tests were not inspected or run here.
The later lead report of 12 exception tests, 58,880 five-feature cases
(52,636 accepted, 6,244 refused), 192 prepared-core comparisons, and 552 collapse
checks is lead-reported only. None contributes to this review's independent
executed counters, and no new code was inspected.

## 1. Precise Contract

Use the existing nonempty ordered full binary tree, `G>=1`, exactly two rows,
exact leaf union populations `H_j`, attained maxima `C_v`, and prescribed root
sizes `(u,ell)` with `u=C_root>=ell`. Original leaf IDs must be their inorder
ranks `0,...,G-1`, as in the documented balanced heap representation. Arbitrarily
permuted external IDs require a separately paid mapping; the run bound is not
valid in an arbitrary ID order.

Keep all existing static validation and complete-pair feasibility checks. In
particular, `0<=l_j=H_j-C_j<=C_j`; the usual local inequalities alone do not prove
global feasibility. Strict nodes have `C_left+C_right>C_parent`. Delete those
nodes conceptually; ordinary leaves belong to connected additive components.
Every component, including one with no ordinary leaves, stays in the core.

For component `B` with ordinary-leaf set `J_B`, retain

```text
L_B = sum_{j in J_B} l_j
U_B = sum_{j in J_B} C_j
f_B = min_{j in J_B} l_j            if J_B is nonempty
```

An empty ordinary-leaf set has `(L,U)=(0,0)` and an unused floor `None` or zero.
It owns no leaf IDs and no runs. A component with a zero-population ordinary
leaf is different: that leaf participates in the minimum and forces `f_B=0`.
Do not ignore such leaves or interpret every zero floor as an empty component.

Admit only strictly increasing, unique, in-range original IDs with positive
integer query counts satisfying `q_j<=f_owner(j)`, and total `Q<=a`. Omitted IDs
mean zero. Preserve the documented rejection of booleans/coerced integer types
if reusing the existing Python API contract. A floor violation must produce
**REFUSE**, not clipping, a substituted upper bound, or an unannounced fallback.
Structural admission does not imply that the requested row sizes are feasible.

Retain scalar root metadata, including the leaf-ID upper endpoint `G`, exact
union size, and `u`; these cost only `O(1)` records. The intended storage claim
does not forbid this metadata, only a retained table or captured input of size G.

## 2. Exact Ownership-Run Theorem

For each strict node `v`, its subtree occupies a half-open leaf interval
`[a_v,c_v)`, with left/right child split at `b_v`. Set

```text
D = union_{strict v} {a_v,b_v,c_v} intersect {1,...,G-1}.
R = number of maximal equal-owner runs in the inorder leaf sequence.
```

Then **`R=1+|D|<=min(G,3r+1)`**.

Proof: two consecutive leaves have different owners exactly when their unique
connecting tree path contains a deleted strict vertex. If that strict vertex
is their lowest common ancestor, their gap is its child boundary `b_v`. If it
is below the common ancestor on the left leaf's path, that leaf is the rightmost
leaf of its strict subtree, so the gap is `c_v`. On the right leaf's path the
gap is `a_v`. Thus every owner change lies in D. Conversely, at an internal
subtree start/end the adjacent leaf pair straddles that subtree and its path
passes through its strict root; at `b_v` the path also passes through that root.
Every gap in D therefore changes owner. Counting the gaps proves the formula.

This proof handles all requested hazards without assuming that a component is
one interval:

- Adjacent strict nodes contribute coincident endpoints; take a set of cuts,
  keep the direct strict edge, and do not invent an intervening component.
- Nested strict subtrees contribute additional cuts inside an existing child
  interval. Their ancestors' cuts need not be disjoint, and need not all survive
  as distinct cuts.
- A component can recur before, between, and after many strict-subtree holes.
  Its runs repeat the **same owner ID**, and all use the same component floor.
- Components with no ordinary leaves contribute no runs but retain boundaries,
  capacities, and zero bundles in the evaluator.
- Zero-population leaves still occupy original ID positions. For `r=0`, all G
  leaves have one owner and exactly one run, even when the union is empty.

The bound is sharp for realizable metadata, not merely an abstract deletion
pattern. Treat `r` gadgets `S_i=(X_i,Y_i)` as leaves of an otherwise additive
skeleton. Each `X_i` is an exclusive row-0 block of size two, each `Y_i` an
exclusive row-1 singleton. Interleave shared singleton leaves so the inorder
sequence is `P_0,X_1,Y_1,P_1,...,X_r,Y_r,P_r`. Every gadget is strict; every
skeleton node has row 0 attaining the sum of its child maxima, so is additive.
The P leaves share one component across r holes. There are `G=3r+1` distinct
ownership runs, and `u=3r+1,ell=2r+1,H_root=4r+1`. The construction includes
`r=0` as one shared singleton. A right-comb skeleton suffices.

The old exact core count remains `K=3r+1-t-k`, where `t` indicates a strict root
and `k` counts original strict-to-strict edges. The run formula is separate from
the component count: `R` may exceed the number of leaf-owning components.

## 3. Exact Admitted-Query Equivalence

Let `Q_B=sum_{j in J_B} q_j`. For every queried leaf, admission and static
validation imply

```text
0 < q_j <= f_B <= l_j <= C_j <= H_j.
min(q_j,l_j) = min(q_j,C_j) = q_j.
```

Omitted leaves contribute zero. Consequently the original four-scalar bundle is
exactly

```text
(L_B,U_B,A_B,B_B) = (L_B,U_B,L_B-Q_B,Q_B).
```

Moreover `Q_B<=L_B`, so the intercept is nonnegative. Sorted uniqueness prevents
double-counting. Accumulating by owner works even if the same component reappears
in several disjoint runs. Its minimum must cover **all** its ordinary leaves,
not just queried leaves, one run, or one connected subtree interval.

All other core fields are static. Thus preparation reproduces the unrestricted
static-table/dense compiler's query sum and each component's rewards, with
identical topology, capacities, L/U, and strict branch bits. The prior boundary
projection and optimal lifting theorem applies unchanged at every retained
boundary tuple and row count. The old evaluator, retaining both root-domain
checks and rejecting impossible companions, returns exactly the old envelope.
No weaker domination argument or relaxation is being substituted for equality.

### Central Falsification: Ordinary Union/Length Bound

For any minor bundle count `x>=L_B`,

```text
min(x-(L_B-Q_B),Q_B) = Q_B.
```

The major endpoint has the same reward. More directly, every feasible count
assignment of either original row has `x_j>=l_j>=q_j`, so choose its leaf subset
to contain all q_j query features. A feasible companion can then include the
complement and enough selected-row features to reach its prescribed leaf size,
exactly as in the old lifting proof. This proves an overlap-Q witness separately
for either scored row on every feasible complete-pair orientation.

Hence, when a compatible pair exists, both marginal optima have overlap Q and
the smaller row maximizes Jaccard:

```text
envelope = Q/(a+ell-Q)       if Q>0
envelope = 0                if Q=0, including empty/empty.
```

Feasibility implies `Q<=ell`, making the positive denominator legitimate. Do
not apply the formula before proving feasibility. Do not claim both marginal
optima coexist: a single leaf with `H=2,C=1,ell=1,q=1` admits the query, but
both rows containing that query feature would fail to cover the other feature.
The maximum-over-either-row semantics permit separate witnesses.

Ordinary union/length information alone bounds a size-n row's overlap by
`min(Q,n)`. For both n=u and n=ell here, `Q<=ell<=u`, so the ordinary union/length
baseline gives exactly the same maximum `Q/(a+ell-Q)`. The strict-node count,
distribution of q among admitted leaves, and component shape cannot tighten
this value. The summary's retained extra information certifies admission and
can certify metadata feasibility; it does not improve the score for any admitted
feasible query. This is a negative scientific result, not merely an optional
evaluator optimization.

This is a newly constructed control derived from the proposed floor restriction,
not historical prior art. It removes query-dependent optimization, not generally
the need to decide whether a requested ell is feasible. If `(u,ell)` is fixed and
feasibility is established once, subsequent admitted scoring needs only admission
and scalar arithmetic. Paying an exponential feasibility pass during compilation
would exceed the stated O(G) compilation contract unless charged separately.

## 4. Hard Refusals And Unsafe Relaxation

These are valid original count queries with exact feasible envelopes. Their
refusal is an intended limitation of the restricted API, **not a correctness
bug**. All have `a=Q`, no outside-union query features.

| Example | Metadata and query | Why refused | Original exact score |
| --- | --- | --- | --- |
| Unequal positive lower bounds | Two additive leaves `(H,C)=(2,1),(4,2)`, root rows `(3,3)`, `q=(0,2)` | One component has floor 1 although queried leaf lower bound is 2 | `2/3` |
| Empty padding poisons floor | Additive leaves `(0,0),(2,1)`, rows `(1,1)`, `q=(0,1)` | The zero-population ordinary leaf makes the component floor 0 | `1` |
| Recurring component across a hole | Tree `(0,((1,2),3))`, `H=(2,1,1,4)`, leaf `C=(1,1,1,2)`; sole strict node `(1,2)` has cap 1; root rows `(4,4)`; `q=(0,0,0,2)` | Owners are `P,A,B,P`; leaf 0 makes P's global floor 1, although leaf 3 has lower bound 2 | `1/2` |
| Even identical rows | Single leaf `H=C=3`, rows `(3,3)`, `q=3` | Floor is zero even though both prescribed rows equal the union | `1` |

The first two examples scale: replace the queried leaf's lower bound by M and
its `(H,C)` by `(2M,M)`. An unrelated lower-one leaf excludes all singleton-ID
queries of sizes `2,...,M`; an empty padding leaf excludes `1,...,M`. This can
discard arbitrarily many valid count values with fixed `r=0` and `s=1`.

The representation also genuinely loses information outside the profile. Two
additive two-leaf inputs have the same root capacity 3, union size 4, `L=1,U=3`,
one owner run, and floor 0:

```text
X: leaf (H,C) = (1,1),(3,2)
Y: leaf (H,C) = (3,2),(1,1).
```

For rows `(3,1)` and query `q=(1,0),a=1`, X has envelope `1/3` but Y has envelope
`1`. Thus this retained summary cannot answer every refused query exactly.

Silently using the floor formula beyond admission is unsafe even for a single
leaf: `H=2,C=1,ell=1,q=a=2` has exact score `1/2`. The invalid update would set
`A=-1,B=2`, claim overlap 2 in a size-one row, and produce score 2. REFUSE must
be explicit. Refusal alone says nothing about whether the original query is
valid, realizable, or cheaply answerable by a richer representation.

## 5. Resource Accounting And Lead Obligations

Compile owners by an inorder traversal, coalescing consecutive equal owner IDs
as leaves are visited. Accumulate each component's L/U/minimum in the same or
another linear traversal. This takes O(G) arithmetic, including zero-population
leaves; sorting a freshly collected endpoint list is unnecessary and could add
`O(r log r)`. Paid transient inputs/stacks/validation may still occupy O(G).

The output needs `K=O(r+1)` core records, `R=O(r+1)` run records, and one floor
per component. Run records store half-open original-ID endpoints and compact
owner IDs, not a leaf array. Shared core child lists have total O(r) edges.
No original-tree object, closure, backing array, debug record, or retained
per-leaf owner table may remain reachable from a successfully compiled object.
Those are implementation obligations; this mathematical sidecar cannot certify
them. Caller-held inputs and retained exception tracebacks are outside the claim.

Binary search over run starts gives the conservative supported schedule

```text
paid supplied-count compilation: O(G) arithmetic; O(G) transient allowance
retained static state:            O(r+1) count/index records
query preparation:                O(r+1 + s*log(r+2)) arithmetic
old orientation evaluation:       O((r+1)*2^r) arithmetic
retained + prepared workspace:    O(r+1) records, excluding caller input
```

With strictly increasing IDs, a cursor can instead scan each run at most once,
giving `O(s+R)` owner lookup and `O(s+r+1)` preparation, including copying core
records. It needs neither a query list nor an s-sized set. This is ordinary
merge scanning, not a new data structure. If the implementation chooses binary
search, report its bound instead. For `r=0`, `s*log(r+1)` literally vanishes and
is not a correct read-cost term without a convention truncating the log below 1.

Count words are not constant-bit storage: original IDs/endpoints require
`log G` bits, and capacities/floors require numeric bit widths. Taking
`b=ceil(log2(2+G+H_root+a))`, a payload bound is `O((r+1)*b)` bits, with
corresponding arithmetic and exact-fraction costs. There is no encoded-size
polynomial kernel in r alone. There is no claimed small build peak or physical
RSS guarantee, and no expanded leaf/feature witness can be recovered from these
records alone within O(r) output work.

Raw graph ingestion, feature-to-leaf mapping, deduplication, population counting,
sorting/aggregation of sparse queries, build resources, and output are paid
separately. An input with huge symbolic counts is not ingestion of that many
features. Preserve budget refusal, global infeasibility, outside-union a, and
zero-score conventions from the old evaluator. No public timing was run here.

## 6. Bounded-Exception Extension

The lead's proposed extension is mathematically sound under the following
contract, without importing any forthcoming implementation. Declare a static
nonnegative integer threshold tau. Let `E={j:l_j<tau}`, `e=|E|`. Keep the same
core and owner runs; for each exception retain `(leaf_ID,owner,l_j,C_j)` in ID
order. No individual data are necessary for regular leaves because
`q_j<=tau<=l_j<=C_j` implies both minimum terms equal q_j.

Validate sorted unique positive sparse counts, `q_j<=tau`, `Q<=a`, and all
existing row-size conditions. On an exception also check `q_j<=H_j=l_j+C_j`;
the tau cap alone does not imply that. Zero-population leaves are exceptions
whenever tau is positive and cannot accept any positive q. At tau zero, only
the empty sparse query is admissible, and e is zero.

Starting at `(A_B,B_B)=(L_B,0)`, apply

```text
regular queried leaf:    A_B -= q_j;             B_B += q_j
exception queried leaf:  A_B -= min(q_j,l_j);    B_B += min(q_j,C_j).
```

Omitted exceptions contribute zero query reward, but their full lower occupancies
remain in L_B. Summing gives the exact old A/B for every admitted tau-bounded
query, not only floor-admitted ones. All original feasible boundary tuples and
optimal overlaps are therefore preserved by the existing contraction theorem.

The owner runs cost O(r+1) records and exceptions O(e), giving O(r+e+1) static
records after O(G) paid compilation. Binary search in run and exception indexes
gives `O(r+1+s*(log(r+2)+log(e+2)))` preparation; two forward cursors give the
optional `O(s+r+e+1)` alternative. The latter can be worse for a tiny s and large
e. Query-local bundles and evaluator workspace still need only O(r+1) extra
records if neither sparse queries nor exception records are copied. Branch work
remains O((r+1)*2^r). Exception membership must be exact, not a probabilistic
test that silently misses a low-l leaf. Floors are no longer needed for admission.

**Strict separation with one exception, even at r=0.** Take an additive tree
with one exceptional leaf `(H,C)=(3,2)` and m>=1 regular leaves each `(6,3)`.
Set tau=3, so only the first leaf has `l=1<tau`; every regular leaf has l=3.
Choose root sizes `u=3m+2`, `ell=3m+1`. Disjoint rows with leaf occupancies
`(2,1)` at the exception and `(3,3)` elsewhere realize all metadata. Query all
three exceptional-leaf features, and no others, with `a=Q=3`. Then

```text
e=1, r=0, G=m+1
(L,U,A,B)=(3m+1,3m+2,3m,2)
major overlap=2; minor overlap=1
exact envelope=2/(3m+3)
ordinary union/length envelope=3/(3m+1).
```

For m=1 these are `1/3` versus `3/4`. The exception update retains genuinely
stronger pruning while e stays one as G grows. The floor-only profile refuses
this query because its single component floor is one. This is an analytic
constructed separation, not a workload result or prior-art claim.

With e=0 the constant-overlap collapse returns. It also returns for any query
whose positive exception counts happen all to satisfy `q_j<=l_j`. In the worst
case e=G, including many natural zero-l leaves, so the storage advantage can
disappear. The scientific next question is whether useful workloads have small
e while actually querying enough `q_j>l_j` exceptions to gain pruning. The
proof alone gives no prevalence, timing, physical-RAM, or novelty result.

### Lead's Balanced One-Strict-Node Family

The follow-up family is correct with **G a power of two, G>=4, and even M>=4**.
Put `(C,H)=(M,3M/2)` at every leaf except IDs 2 and 3, which have `(C,H)=(3,4)`.
Their common parent has capacity 4; all other internal capacities are additive.
Use tau=2, `q_2=q_3=2`, `a=4`, and tied root sizes
`u=ell=(G-2)*M+4`.

Every ordinary lower bound is M/2>=2, while each exceptional lower bound is
one. Thus **r=1 and e=2**. The core has four nodes: the root additive component,
the strict parent, and its two singleton-leaf components. The root component
owns all ordinary IDs, including those after the exceptional subtree.

The owner-run claim needs an endpoint qualification:

```text
G=4:   [0,2)->ordinary, [2,3)->exception-2, [3,4)->exception-3     R=3
G>=8:  same three runs, then [4,G)->ordinary                     R=4
```

This is precisely the cut formula with strict-subtree endpoints `(2,3,4)`;
the endpoint 4 is external when G=4. Four is a uniform upper bound, not the
exact count at every valid G. If M=2 then every ordinary leaf is also an
exception, so e=G rather than two; odd M does not give integer union counts.

At the strict parent, each orientation gives either scored row one major leaf
of size three and one minor leaf of size at least one. The parent cap is four,
forcing the minor size to exactly one and the parent total to exactly four.
With two query features in each leaf, the exact overlap is `2+1=3`. This holds
for either scored row and either orientation. Each ordinary leaf contributes
zero query overlap. The root ties force both rows to attain each ordinary
capacity M, and these counts are feasible: choose two size-M subsets of an
ordinary union of size 3M/2, intersecting in M/2 features. At the exceptional
leaves choose disjoint sizes `(3,1)` and `(1,3)`; choose the scored row's features
query-first. These choices jointly realize the full union and all maxima.

Therefore, for every member of the family,

```text
exact maximum overlap = 3
exact exception-core envelope = 3/(u+1)
ordinary union/length envelope = 4/u
3/(u+1) < 4/u.
```

At G=4,M=4, this is `3/13 < 1/3`. The floor-only profile refuses both exceptional
q=2 values because each owner floor is one. The tau-exception profile admits
them and exactly preserves the lost overlap. The independent checker below
verifies twelve members (G=4,8,16,32; M=4,6,10) by explicit original-tree count
frontiers and constructed actual-feature witnesses, not by enumerating all
feature pairs for those larger unions. This is a useful constructive separation
with bounded r and e; it is not evidence that useful datasets follow the family.

### Exception-Position Information Lower Bound

The lead's later lower-bound construction is valid. Fix G, tau=1, `0<e<G`, the
ordered tree, and the same two-feature universe at every original leaf. For
any e-element subset E of leaf IDs, use C=2,l=0 on E and C=1,l=1 elsewhere, with
all internal capacities additive. Then r=0, `u=G+e`, `ell=G-e`, `H_root=2G`,
and every pattern has exactly the same contracted root capacity, bundle
`(L,U)=(ell,u)`, owner map (one run), and component floor zero.

The major row must attain every leaf capacity. The smaller row's root total
equals the sum of its leaf lower bounds, so every smaller-row leaf count must
equal its lower bound. Realizing rows exist and are disjoint: exceptional leaves
put both features in the larger row; other leaves put one feature in each row.
For a singleton query in leaf j, with a=q_j=1, its exact envelope is therefore

```text
j in E:      1/u
j not in E:  1/ell.
```

For the regular case, permute the two local feature identities so the smaller
row contains the queried feature. Since `0<ell<u`, the two answers differ.
Thus different e-element subsets produce different exact answer functions on
the same fixed singleton-query universe. A self-contained exact summary must
distinguish all `binom(G,e)` patterns: it carries at least
`log2(binom(G,e))` bits of information, or at least its ceiling for a fixed-size
binary state. Any variable-length coding must account for length/decoding
information as well; uncharged encoding lengths are not a free side channel.

This is an information lower bound on exception positions even at r=0, assuming
no access to original per-leaf capacities, actual rows, or external pattern
advice at query time. It does not require storing one explicit tuple per
exception, prove an Omega(e) word bound, or bound query time. In particular,
when e is near G, complement coding can be much smaller than e explicit IDs.
The familiar sparse-regime interpretation is Omega(e*log(G/e)) bits for
`1<=e<=G/2`; the full binomial expression treats both regimes correctly.

### Clipping Exception Reward Capacities

It is safe to replace **only the exception-table copy** of C_j by
`C'_j=min(C_j,tau)`. For admitted q_j<=tau,

```text
min(q_j,C'_j) = min(q_j,C_j)
q_j <= l_j+C'_j  if and only if  q_j <= l_j+C_j.
```

For the validation equivalence, if C_j<tau nothing changes. Otherwise C'_j=tau,
and both population checks hold automatically for q_j<=tau and l_j>=0. This
argument is conditioned on checking the tau cap, not a replacement for it.
Exception lower bounds remain exact; because l_j<tau and l_j<=C_j, they also
satisfy l_j<=C'_j. Their scalar widths are O(log(tau+1)), with the usual
constant-bit convention at zero. IDs, component owners, tau itself, and the
large static totals retain their separately paid widths.

**Do not clip the retained core node capacity, U, L, or prescribed root sizes.**
Those fields describe feasible row counts, not capped query rewards. The
exception table is allowed to forget the original leaf population because it
needs only its equivalent admission predicate on q<=tau. No witness-recovery
claim follows. The retained checker below uses clipped exception reward copies
and also directly checks the min/validation identities, including q values that
must fail the population test.

## 7. Closest Inspected Primary Context

The search focused on exact query-restricted representations, aggregate-view
rewriting, and what loss-less kernelization actually preserves. The following
are published methods/frameworks, not project controls. No inspected source
establishes the specific two-row floor/run theorem.

1. **Query-restricted exact indexing.** Ferrada, Gagie, Hirvola, and Puglisi,
   [Hybrid Indexes for Repetitive Datasets, arXiv:1306.4037v1 (2013), Sections
   2.2-2.3](https://arxiv.org/pdf/1306.4037v1), retain a filtered text index near
   LZ77 phrase boundaries for declared pattern-length/edit-distance limits.
   Section 2.2 preserves all relevant primary matches, maps positions using
   sorted boundary lists and binary search, and Section 2.3 recovers secondary
   matches from phrase sources. Setting edit distance zero gives exact matching.
   This is a concrete precedent for exchanging query breadth for smaller exact
   retained state with explicit mapping data. It does not supply a reduction
   from attained-max count envelopes, component floors, or our 3r+1 bound; its
   occurrence-recovery state and output costs must not be omitted in comparison.

2. **Materialized sufficient aggregates.** Cohen, Nutt, and Serebrenik,
   [Algorithms for Rewriting Aggregate Queries Using Views,
   arXiv:cs/0011024v1 (2000), Sections 4.2-4.3, Theorems 3-4](https://arxiv.org/pdf/cs/0011024v1),
   give sound count/sum rewritings, with completeness for specified linear or
   relational cases. The appropriate local analogy is materializing
   `group(owner): SUM(l), SUM(C), MIN(l)` plus the owner relation, then reducing
   sparse rewards to `SUM(q)` under the checked floor predicate. This credits
   aggregate reuse and equivalent rewriting as established machinery. The
   inequality simplification is elementary, not an application of a theorem
   proving the count-model contraction or succinctness of that owner relation.

3. **Restricted preservation is not all-solution preservation.** Carbonnel and
   Hebrard, [On the Kernelization of Global Constraints, IJCAI 2017, Section 3,
   Definitions 1-3, printed pages 579-580](https://www.ijcai.org/proceedings/2017/0081.pdf),
   define z-loss-less kernels whose support-recovery guarantee applies within
   a specified distance from optimum. Their recovery algorithms receive the
   original instance; kernel size is an encoded-size bound. This is useful
   precedent for explicitly stating what a restricted exact reduction preserves,
   not an equivalence to the present data structure. Our summary retains neither
   all variable-value supports nor the original instance and is only small in
   count/index records. Calling it their kind of loss-less kernel is unsupported.

The inspected [Parameterized Compilability, Chen, IJCAI 2005, Definition
12](https://www.ijcai.org/Proceedings/05/Papers/0644.pdf) separately formalizes
offline knowledge-base compilation and online queries, with a compiled-size
allowance and a query-length bound. It provides accounting vocabulary, not a
problem-specific compression theorem. The proposed O(G) uniform static build
and floor-dependent admission require their own proofs above.

Under the local evidence policy, ordinary minima, group aggregation, inorder
intervals, run-length encoding, binary search, and sorted merge scans receive
ordinary attribution. The problem-specific delta is the owner-run lemma plus
its composition with the already-reviewed additive core and this restrictive
admission certificate. The constant-reward control is independently derived
here from that same certificate, so it cannot establish historical priority.
No broad novelty, practical advantage, workload prevalence, or publication-ready
contribution follows from this review or its finite checks.

## 8. Retained Self-Contained Finite Checker

The checker has complementary domains: every ordered full binary shape
through eight leaves with every strict-node subset (a structural superset of
realizable masks); all bundles of zero through three leaves with
`0<=l<=C<=3` and `0<=q<=l+C`; and named complete-pair fixtures for both the floor
and tau-exception profiles. Bundle exception thresholds range from zero through
three. Structural owners
come from undirected graph connectivity after deletion, independently of the
cut formula. Numerical row frontiers use explicit integer convolutions, not
the lead's capped-linear helpers. Actual feature pairs are enumerated with each
union feature in row 0 only, row 1 only, or both. No random sampling is used.

The checker deliberately retains original trees, component memberships, dense
queries, and exhaustive tables as oracle state. It does **not** implement or
benchmark the claimed low-state production schedule. Its structural loop also
checks binary-search and monotone-scan owner lookup. The checker is retained in
this file to comply with exclusive single-file ownership.
Additional bounded checks cover twelve balanced strict-family members, every
exception-position pattern for G=2..8 and 0<e<G, and clipping identities for
`0<=l<=C<=8`, `0<=tau<=8`, `0<=q<=tau`. The larger strict-family and lower-bound
checks use explicit count frontiers plus constructed feature witnesses; they
are not exhaustive actual-feature pair enumeration.

```python
from bisect import bisect_right
from collections import Counter
from fractions import Fraction
from functools import lru_cache
from itertools import product
from math import comb

checks = Counter()


@lru_cache(None)
def enumerate_ordered_binary_trees(start, size):
    if size == 1:
        return (start,)
    return tuple((left, right) for width in range(1, size)
                 for left in enumerate_ordered_binary_trees(start, width)
                 for right in enumerate_ordered_binary_trees(start + width, size - width))


def build_balanced_leaf_tree(start, size):
    if size == 1:
        return start
    left = size // 2
    return (build_balanced_leaf_tree(start, left),
            build_balanced_leaf_tree(start + left, size - left))


@lru_cache(None)
def list_ordered_tree_nodes(tree):
    return (tree,) if isinstance(tree, int) else (
        (tree,) + list_ordered_tree_nodes(tree[0]) + list_ordered_tree_nodes(tree[1]))


@lru_cache(None)
def list_ordered_leaf_ids(tree):
    return tuple(v for v in list_ordered_tree_nodes(tree) if isinstance(v, int))


def collect_deleted_vertex_components(tree, strict):
    nodes = list_ordered_tree_nodes(tree)
    adjacency = {v: [] for v in nodes}
    for v in nodes:
        if not isinstance(v, int):
            for child in v:
                adjacency[v].append(child)
                adjacency[child].append(v)
    groups, membership = {}, {}
    for top in nodes:
        if top in strict or top in membership:
            continue
        members, stack = [], [top]
        membership[top] = top
        while stack:
            v = stack.pop()
            members.append(v)
            for child in adjacency[v]:
                if child not in strict and child not in membership:
                    membership[child] = top
                    stack.append(child)
        groups[top] = tuple(members)
    return groups, membership


def encode_owner_interval_runs(owners):
    runs = []
    for j, owner in enumerate(owners):
        if runs and runs[-1][2] == owner:
            runs[-1] = (runs[-1][0], j + 1, owner)
        else:
            runs.append((j, j + 1, owner))
    return tuple(runs)


def verify_owner_run_case(tree, strict, category):
    leaves = list_ordered_leaf_ids(tree)
    groups, membership = collect_deleted_vertex_components(tree, strict)
    owners = tuple(membership[j] for j in leaves)
    runs = encode_owner_interval_runs(owners)
    cuts = set()
    for v in strict:
        ids = list_ordered_leaf_ids(v)
        cuts.update((ids[0], list_ordered_leaf_ids(v[1])[0], ids[-1] + 1))
    cuts.difference_update((0, len(leaves)))
    r = len(strict)
    adjacency = sum(child in strict for v in strict for child in v)
    assert len(groups) == 2 * r + 1 - (tree in strict) - adjacency
    assert {run[0] for run in runs[1:]} == cuts
    assert len(runs) == 1 + len(cuts) <= min(len(leaves), 3 * r + 1)
    starts = tuple(run[0] for run in runs)
    cursor = 0
    for j, owner in enumerate(owners):
        while runs[cursor][1] <= j:
            cursor += 1
        assert runs[cursor][2] == owner == runs[bisect_right(starts, j) - 1][2]
        checks["owner_lookup_pairs"] += 1
    checks[category] += 1
    checks["zero_leaf_components"] += sum(
        not any(isinstance(v, int) for v in members) for members in groups.values())
    checks["recurring_owner_cases"] += len(runs) > len(set(owners))
    return len(runs)


def derive_row_capacity_map(tree, x, y):
    return {v: max(sum(x[j] for j in list_ordered_leaf_ids(v)),
                   sum(y[j] for j in list_ordered_leaf_ids(v)))
            for v in list_ordered_tree_nodes(tree)}


def build_packed_component_core(tree, h, cap):
    nodes = list_ordered_tree_nodes(tree)
    strict = tuple(v for v in nodes if not isinstance(v, int)
                   and cap[v[0]] + cap[v[1]] > cap[v])
    groups, membership = collect_deleted_vertex_components(tree, set(strict))
    tops = tuple(v for v in nodes if v in strict or v in groups)
    ids = {v: i for i, v in enumerate(tops)}
    packed, ordinary = [], {}
    for v in tops:
        if v in strict:
            children = tuple(ids[ch if ch in strict else membership[ch]] for ch in v)
            packed.append((True, cap[v], children, 0, 0, None))
        else:
            leaves = tuple(j for j in groups[v] if isinstance(j, int))
            ordinary[ids[v]] = leaves
            boundaries = tuple(ch for w in groups[v] if not isinstance(w, int)
                               for ch in w if ch in strict)
            lows = tuple(h[j] - cap[j] for j in leaves)
            packed.append((False, cap[v], tuple(ids[ch] for ch in boundaries),
                           sum(lows), sum(cap[j] for j in leaves), min(lows, default=None)))
    owners = tuple(ids[membership[j]] for j in range(len(h)))
    return tuple(packed), tops, ordinary, strict, encode_owner_interval_runs(owners)


def prepare_sparse_floor_records(packed, runs, a, pairs):
    if type(a) is not int or a < 0:
        raise ValueError("query size")
    totals = [0] * len(packed)
    starts = tuple(run[0] for run in runs)
    previous, total = -1, 0
    for pair in pairs:
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise ValueError("pair shape")
        j, q = pair
        if (type(j) is not int or not previous < j < runs[-1][1]
                or type(q) is not int or q <= 0):
            raise ValueError("ID or count")
        owner = runs[bisect_right(starts, j) - 1][2]
        floor = packed[owner][5]
        if floor is None or q > floor:
            raise ValueError("REFUSE floor")
        previous, total = j, total + q
        totals[owner] += q
    if total > a:
        raise ValueError("query total")
    return totals


def merge_discrete_overlap_tables(tables):
    result = {0: 0}
    for table in tables:
        merged = {}
        for x, reward in result.items():
            for y, gain in table.items():
                merged[x + y] = max(merged.get(x + y, -1), reward + gain)
        result = merged
    return result


def label_original_tree_nodes(tree, strict, bits):
    choices, labels = dict(zip(strict, bits)), {}

    def propagate_original_major_labels(v, label):
        labels[v] = label
        if not isinstance(v, int):
            propagate_original_major_labels(v[0], choices.get(v, label))
            propagate_original_major_labels(v[1], 1 - choices[v] if v in choices else label)

    propagate_original_major_labels(tree, 0)
    return labels


def evaluate_dense_row_tables(tree, h, cap, q, labels, row):
    tables = {}
    for v in reversed(list_ordered_tree_nodes(tree)):
        if isinstance(v, int):
            table = {x: min(x, q[v]) for x in range(h[v] - cap[v], cap[v] + 1)}
        else:
            table = merge_discrete_overlap_tables([tables[ch] for ch in v])
        tables[v] = {x: value for x, value in table.items()
                     if x <= cap[v] and (labels[v] != row or x == cap[v])}
    return tables


def evaluate_packed_row_tables(packed, tops, totals, labels, row, rewards=None):
    tables = {}
    for i in reversed(range(len(packed))):
        strict, cap, children, lower, upper, floor = packed[i]
        pieces = [tables[ch] for ch in children]
        if not strict:
            a, b = rewards[i] if rewards is not None else (lower - totals[i], totals[i])
            pieces.append({x: min(x - a, b)
                           for x in range(lower, upper + 1)})
        table = merge_discrete_overlap_tables(pieces)
        tables[i] = {x: value for x, value in table.items()
                     if x <= cap and (labels[tops[i]] != row or x == cap)}
    return tables


def calculate_exact_jaccard_score(overlap, size, a):
    return Fraction(overlap, a + size - overlap) if overlap else Fraction(0)


def construct_complete_pair_candidates(tree, h, cap):
    masks, offset = [], 0
    for population in h:
        masks.append(((1 << population) - 1) << offset)
        offset += population
    node_masks = {v: sum(masks[j] for j in list_ordered_leaf_ids(v))
                  for v in list_ordered_tree_nodes(tree)}
    pairs = {}
    for states in product((0, 1, 2), repeat=offset):
        x = sum(1 << j for j, state in enumerate(states) if state != 1)
        y = sum(1 << j for j, state in enumerate(states) if state != 0)
        checks["feature_assignments"] += 1
        if x.bit_count() != cap[tree]:
            continue
        if all(max((x & mask).bit_count(), (y & mask).bit_count()) == cap[v]
               for v, mask in node_masks.items()):
            pairs.setdefault(y.bit_count(), []).append((x, y))
            checks["compatible_pairs"] += 1
    return pairs


def enumerate_direct_query_mask(h, q):
    mask, offset = 0, 0
    for population, count in zip(h, q):
        mask |= ((1 << count) - 1) << offset
        offset += population
    return mask


def verify_complete_family_envelopes(name, tree, h, x, y, overrides):
    cap = derive_row_capacity_map(tree, x, y)
    cap.update(overrides)
    packed, tops, ordinary, strict, runs = build_packed_component_core(tree, h, cap)
    pairs = construct_complete_pair_candidates(tree, h, cap)
    owners = {j: owner for owner, leaves in ordinary.items() for j in leaves}
    checks["numerical_families"] += 1
    for q in product(*(range(population + 1) for population in h)):
        checks["family_query_profiles"] += 1
        admitted = all(count <= packed[owners[j]][5] for j, count in enumerate(q))
        sparse = ((j, count) for j, count in enumerate(q) if count)
        try:
            totals = prepare_sparse_floor_records(packed, runs, sum(q), sparse)
        except ValueError:
            assert not admitted, (name, q)
            checks["family_floor_refusals"] += 1
            continue
        assert admitted, (name, q)
        checks["family_admitted_profiles"] += 1
        for owner, leaves in ordinary.items():
            lower = packed[owner][3]
            assert (lower - totals[owner], totals[owner]) == (
                lower - sum(min(q[j], h[j] - cap[j]) for j in leaves),
                sum(min(q[j], cap[j]) for j in leaves))
            checks["family_bundle_equalities"] += 1
        roots = []
        for bits in product((0, 1), repeat=len(strict)):
            labels = label_original_tree_nodes(tree, strict, bits)
            both = []
            for row in (0, 1):
                dense = evaluate_dense_row_tables(tree, h, cap, q, labels, row)
                compact = evaluate_packed_row_tables(packed, tops, totals, labels, row)
                for i, v in enumerate(tops):
                    assert dense[v] == compact[i], (name, q, bits, row, v)
                    assert all(value == sum(q[j] for j in list_ordered_leaf_ids(v))
                               for value in dense[v].values())
                    checks["conditioned_frontiers"] += 1
                both.append(compact[0])
            roots.append(both)
            checks["family_orientations"] += 1
        mask = enumerate_direct_query_mask(h, q)
        for ell in range(max(0, sum(h) - cap[tree]), cap[tree] + 1):
            for outside in (0, 2):
                a = sum(q) + outside
                feasible = [root for root in roots if cap[tree] in root[0] and ell in root[1]]
                checks["one_sided_branches"] += sum(
                    (cap[tree] in root[0]) != (ell in root[1]) for root in roots)
                actual = max((max(calculate_exact_jaccard_score((v & mask).bit_count(), size, a)
                                  for v, size in ((left, cap[tree]), (right, ell)))
                              for left, right in pairs.get(ell, ())), default=None)
                result = max((max(calculate_exact_jaccard_score(root[row][size], size, a)
                                  for row, size in ((0, cap[tree]), (1, ell)))
                              for root in feasible), default=None)
                collapsed = calculate_exact_jaccard_score(sum(q), ell, a) if feasible else None
                assert actual == result == collapsed, (name, q, ell, outside, actual, result)
                if feasible:
                    baseline = max(calculate_exact_jaccard_score(min(sum(q), size), size, a)
                                   for size in (cap[tree], ell))
                    assert actual == baseline
                    checks["floor_baseline_equalities"] += 1
                checks["complete_envelopes"] += 1
                checks["infeasible_envelopes"] += actual is None


def run_exhaustive_bundle_checks():
    types = tuple((lower, upper) for upper in range(4) for lower in range(upper + 1))
    for width in range(4):
        for leaves in product(types, repeat=width):
            lower = sum(v[0] for v in leaves)
            upper = sum(v[1] for v in leaves)
            floor = min((v[0] for v in leaves), default=0)
            checks["bundle_configurations"] += 1
            for q in product(*(range(l + c + 1) for l, c in leaves)):
                checks["bundle_query_profiles"] += 1
                for tau in range(4):
                    if any(count > tau for count in q):
                        continue
                    a = lower - sum(min(count, l) if l < tau else count
                                    for count, (l, c) in zip(q, leaves))
                    b = sum(min(count, c) if l < tau else count
                            for count, (l, c) in zip(q, leaves))
                    assert (a, b) == (
                        lower - sum(min(count, l) for count, (l, c) in zip(q, leaves)),
                        sum(min(count, c) for count, (l, c) in zip(q, leaves)))
                    checks["exception_bundle_equalities"] += 1
                if any(count > floor for count in q):
                    checks["bundle_refused_profiles"] += 1
                    checks["coarse_refused_leafsafe"] += all(
                        count <= leaf[0] for count, leaf in zip(q, leaves))
                    continue
                total = sum(q)
                assert (lower - total, total) == (
                    lower - sum(min(count, leaf[0]) for count, leaf in zip(q, leaves)),
                    sum(min(count, leaf[1]) for count, leaf in zip(q, leaves)))
                frontier = {}
                for counts in product(*(range(l, c + 1) for l, c in leaves)):
                    size = sum(counts)
                    reward = sum(min(count, query) for count, query in zip(counts, q))
                    frontier[size] = max(frontier.get(size, -1), reward)
                assert frontier == {size: total for size in range(lower, upper + 1)}
                checks["bundle_admitted_profiles"] += 1
                checks["bundle_frontier_values"] += len(frontier)


def verify_exception_family_envelopes(name, tree, h, x, y, overrides, tau):
    cap = derive_row_capacity_map(tree, x, y)
    cap.update(overrides)
    packed, tops, ordinary, strict, runs = build_packed_component_core(tree, h, cap)
    exceptions = {j: (h[j] - cap[j], min(cap[j], tau))
                  for j in range(len(h)) if h[j] - cap[j] < tau}
    starts = tuple(run[0] for run in runs)
    pairs = construct_complete_pair_candidates(tree, h, cap)
    ell = sum(y)
    checks["exception_families"] += 1
    if name == "one_exception":
        assert len(exceptions) == 1 and not strict
    for q in product(*(range(min(tau, population) + 1) for population in h)):
        rewards = [[node[3], 0] for node in packed]
        for j, count in enumerate(q):
            if not count:
                continue
            owner = runs[bisect_right(starts, j) - 1][2]
            if j in exceptions:
                lower, upper = exceptions[j]
                assert count <= lower + upper
                rewards[owner][0] -= min(count, lower)
                rewards[owner][1] += min(count, upper)
            else:
                rewards[owner][0] -= count
                rewards[owner][1] += count
        for owner, leaves in ordinary.items():
            assert rewards[owner] == [
                packed[owner][3] - sum(min(q[j], h[j] - cap[j]) for j in leaves),
                sum(min(q[j], cap[j]) for j in leaves)]
        roots = []
        for bits in product((0, 1), repeat=len(strict)):
            labels = label_original_tree_nodes(tree, strict, bits)
            both = []
            for row in (0, 1):
                dense = evaluate_dense_row_tables(tree, h, cap, q, labels, row)
                compact = evaluate_packed_row_tables(packed, tops, None, labels, row, rewards)
                for i, v in enumerate(tops):
                    assert dense[v] == compact[i], (name, q, bits, row, v)
                    checks["exception_conditioned_frontiers"] += 1
                both.append(compact[0])
            roots.append(both)
            checks["exception_orientations"] += 1
        mask = enumerate_direct_query_mask(h, q)
        for outside in (0, 2):
            a = sum(q) + outside
            feasible = [root for root in roots if cap[tree] in root[0] and ell in root[1]]
            actual = max((max(calculate_exact_jaccard_score((v & mask).bit_count(), size, a)
                              for v, size in ((left, cap[tree]), (right, ell)))
                          for left, right in pairs.get(ell, ())), default=None)
            result = max((max(calculate_exact_jaccard_score(root[row][size], size, a)
                              for row, size in ((0, cap[tree]), (1, ell)))
                          for root in feasible), default=None)
            assert actual == result, (name, q, outside, actual, result)
            baseline = max(calculate_exact_jaccard_score(min(sum(q), size), size, a)
                           for size in (cap[tree], ell))
            checks["exception_complete_envelopes"] += 1
            checks["exception_strict_improvements"] += actual is not None and actual < baseline
            checks["exception_infeasible_envelopes"] += actual is None
            if name == "one_exception" and q == (3, 0) and outside == 0:
                assert actual == Fraction(1, 3) and baseline == Fraction(3, 4)
                checks["one_exception_separations"] += 1
        checks["exception_query_profiles"] += 1


def verify_balanced_strict_family(size, scale):
    tree = build_balanced_leaf_tree(0, size)
    h = tuple(4 if j in (2, 3) else 3 * scale // 2 for j in range(size))
    q = tuple(2 if j in (2, 3) else 0 for j in range(size))
    cap = {j: 3 if j in (2, 3) else scale for j in range(size)}
    for v in reversed(list_ordered_tree_nodes(tree)):
        if not isinstance(v, int):
            cap[v] = 4 if v == (2, 3) else cap[v[0]] + cap[v[1]]
    packed, tops, ordinary, strict, runs = build_packed_component_core(tree, h, cap)
    exceptions = {j: (h[j] - cap[j], min(cap[j], 2))
                  for j in range(size) if h[j] - cap[j] < 2}
    assert strict == ((2, 3),) and set(exceptions) == {2, 3}
    assert len(packed) == 4 and len(runs) == (3 if size == 4 else 4)
    u = (size - 2) * scale + 4
    assert cap[tree] == u
    rewards = [[node[3], 0] for node in packed]
    starts = tuple(run[0] for run in runs)
    for j in (2, 3):
        owner = runs[bisect_right(starts, j) - 1][2]
        lower, upper = exceptions[j]
        rewards[owner][0] -= min(q[j], lower)
        rewards[owner][1] += min(q[j], upper)
    try:
        prepare_sparse_floor_records(packed, runs, 4, iter(((2, 2), (3, 2))))
    except ValueError as error:
        assert str(error) == "REFUSE floor"
        checks["balanced_floor_refusals"] += 1
    else:
        raise AssertionError("floor must refuse exceptional counts")
    for bits in product((0, 1), repeat=1):
        labels = label_original_tree_nodes(tree, strict, bits)
        for row in (0, 1):
            dense = evaluate_dense_row_tables(tree, h, cap, q, labels, row)
            compact = evaluate_packed_row_tables(packed, tops, None, labels, row, rewards)
            for i, v in enumerate(tops):
                assert dense[v] == compact[i]
                checks["balanced_conditioned_frontiers"] += 1
            assert dense[tree][u] == 3
            checks["balanced_root_frontiers"] += 1
    rows, query, leaves, offset = [set(), set()], set(), [], 0
    for j, population in enumerate(h):
        ids = tuple(range(offset, offset + population))
        leaves.append(set(ids))
        offset += population
        if j == 2:
            rows[0].update(ids[:3])
            rows[1].update(ids[3:])
        elif j == 3:
            rows[0].update(ids[:1])
            rows[1].update(ids[1:])
        else:
            rows[0].update(ids[:scale])
            rows[1].update(ids[scale // 2:])
        if j in (2, 3):
            query.update(ids[:2])
    assert len(rows[0]) == len(rows[1]) == u
    assert rows[0] | rows[1] == set(range(sum(h)))
    counts = [[len(row & leaf) for leaf in leaves] for row in rows]
    assert derive_row_capacity_map(tree, *counts) == cap
    assert len(query) == 4 and len(rows[0] & query) == 3
    actual = max(calculate_exact_jaccard_score(len(row & query), u, 4) for row in rows)
    assert actual == Fraction(3, u + 1) < Fraction(4, u)
    checks["balanced_feature_witnesses"] += 1
    checks["balanced_strict_families"] += 1


def verify_exception_position_information():
    for size in range(2, 9):
        tree = build_balanced_leaf_tree(0, size)
        h = (2,) * size
        labels = {v: 0 for v in list_ordered_tree_nodes(tree)}
        for e in range(1, size):
            signatures = set()
            for bits in product((0, 1), repeat=size):
                if sum(bits) != e:
                    continue
                x = tuple(1 + bit for bit in bits)
                y = tuple(1 - bit for bit in bits)
                cap = derive_row_capacity_map(tree, x, y)
                packed, tops, ordinary, strict, runs = build_packed_component_core(tree, h, cap)
                u, ell = size + e, size - e
                assert not strict and packed == ((False, u, (), ell, u, 0),)
                assert runs == ((0, size, 0),)
                signature = []
                for j in range(size):
                    q = tuple(int(k == j) for k in range(size))
                    tables = [evaluate_dense_row_tables(tree, h, cap, q, labels, row)
                              for row in (0, 1)]
                    result = max(calculate_exact_jaccard_score(table[tree][n], n, 1)
                                 for table, n in zip(tables, (u, ell)))
                    expected = Fraction(1, u if bits[j] else ell)
                    full = (1 << (2 * size)) - 1
                    left = sum((3 if bit else 1) << (2 * k) for k, bit in enumerate(bits))
                    right = full ^ left
                    if not bits[j]:
                        left ^= 3 << (2 * j)
                        right ^= 3 << (2 * j)
                    assert (left.bit_count(), right.bit_count()) == (u, ell)
                    witness = max(calculate_exact_jaccard_score(
                        (row & (1 << (2 * j))).bit_count(), n, 1)
                        for row, n in ((left, u), (right, ell)))
                    assert result == witness == expected
                    signature.append(result)
                    checks["lower_bound_singleton_queries"] += 1
                assert tuple(signature) not in signatures
                signatures.add(tuple(signature))
                checks["lower_bound_patterns"] += 1
            assert len(signatures) == comb(size, e)
            checks["lower_bound_parameter_pairs"] += 1


def verify_clipped_capacity_identities():
    for cap in range(9):
        for lower in range(cap + 1):
            for tau in range(9):
                clipped = min(cap, tau)
                for q in range(tau + 1):
                    assert min(q, cap) == min(q, clipped)
                    assert (q <= lower + cap) == (q <= lower + clipped)
                    checks["clipped_capacity_identities"] += 1


for size in (4, 8, 16, 32):
    for scale in (4, 6, 10):
        verify_balanced_strict_family(size, scale)
verify_exception_position_information()
verify_clipped_capacity_identities()


for size in range(1, 9):
    for tree in enumerate_ordered_binary_trees(0, size):
        checks["ordered_tree_shapes"] += 1
        internal = tuple(v for v in list_ordered_tree_nodes(tree) if not isinstance(v, int))
        for bits in product((0, 1), repeat=len(internal)):
            strict = {v for v, bit in zip(internal, bits) if bit}
            verify_owner_run_case(tree, strict, "structural_masks")

for r in range(6):
    atoms, x, y = [0], [1], [1]
    for i in range(r):
        start = len(x)
        atoms.extend(((start, start + 1), start + 2))
        x.extend((2, 0, 1))
        y.extend((0, 1, 1))
    tree = atoms[-1]
    for atom in reversed(atoms[:-1]):
        tree = (atom, tree)
    cap = derive_row_capacity_map(tree, x, y)
    strict = {v for v in list_ordered_tree_nodes(tree) if not isinstance(v, int)
              and cap[v[0]] + cap[v[1]] > cap[v]}
    assert len(strict) == r
    assert verify_owner_run_case(tree, strict, "sharp_run_families") == 3 * r + 1

run_exhaustive_bundle_checks()

families = [
    ("empty", (0, 1), (0, 0), (0, 0), (0, 0), {}),
    ("single_tie", 0, (2,), (1,), (1,), {}),
    ("single_flexible", 0, (3,), (2,), (1,), {}),
    ("unequal_floors", (0, 1), (2, 4), (1, 2), (1, 2), {}),
    ("zero_padding", (0, 1), (0, 2), (0, 1), (0, 1), {}),
    ("recurring_hole", (0, ((1, 2), 3)), (2, 1, 1, 4), (1, 1, 0, 2), (1, 0, 1, 2), {}),
    ("identical", 0, (3,), (3,), (3,), {}),
    ("zero_bundle", ((0, 1), (2, 3)), (1, 1, 1, 1), (1, 0, 1, 0), (0, 1, 0, 1), {}),
    ("adjacent", ((0, 1), (2, 3)), (2, 1, 2, 1), (2, 0, 0, 1), (0, 1, 2, 0), {}),
    ("nested", (((0, 1), 2), 3), (2, 1, 1, 2), (2, 0, 1, 0), (0, 1, 0, 2), {}),
    ("companion", (0, 1), (1, 2), (0, 2), (1, 0), {}),
    ("impossible", (0, 1), (2, 2), (2, 1), (0, 1), {1: 2}),
    ("summary_collision_x", (0, 1), (1, 3), (1, 2), (0, 1), {}),
    ("summary_collision_y", (0, 1), (3, 1), (2, 1), (1, 0), {}),
]
for family in families:
    verify_complete_family_envelopes(*family)

verify_exception_family_envelopes("one_exception", (0, 1), (3, 6), (2, 3), (1, 3), {}, 3)
for index in (4, 5, 7, 8, 9, 11):
    verify_exception_family_envelopes(*families[index], 2)
verify_exception_family_envelopes(*families[3], 0)

hard = [(3, (0, 2), 3, Fraction(2, 3)), (4, (0, 1), 1, Fraction(1)),
        (5, (0, 0, 0, 2), 4, Fraction(1, 2)), (6, (3,), 3, Fraction(1)),
        (12, (1, 0), 1, Fraction(1, 3)), (13, (1, 0), 1, Fraction(1))]
collision = []
for index, q, ell, expected in hard:
    name, tree, h, x, y, overrides = families[index]
    cap = derive_row_capacity_map(tree, x, y)
    packed, tops, ordinary, strict, runs = build_packed_component_core(tree, h, cap)
    try:
        prepare_sparse_floor_records(packed, runs, sum(q),
                                     ((j, count) for j, count in enumerate(q) if count))
    except ValueError as error:
        assert str(error) == "REFUSE floor"
    else:
        raise AssertionError(("missed refusal", name))
    pairs = construct_complete_pair_candidates(tree, h, cap)
    mask = enumerate_direct_query_mask(h, q)
    score = max(max(calculate_exact_jaccard_score((v & mask).bit_count(), size, sum(q))
                    for v, size in ((left, cap[tree]), (right, ell)))
                for left, right in pairs[ell])
    assert score == expected, (name, score, expected)
    if index in (12, 13):
        collision.append((packed, runs, len(h), sum(h)))
    checks["hard_refusal_envelopes"] += 1
assert collision[0] == collision[1]
checks["indistinguishable_summary_pairs"] += 1
assert calculate_exact_jaccard_score(1, 1, 2) == Fraction(1, 2)
assert calculate_exact_jaccard_score(2, 1, 2) == 2
checks["unsafe_relaxation_examples"] += 1

packed, tops, ordinary, strict, runs = build_packed_component_core((0, 1), (2, 2),
                                                                  {0: 1, 1: 1, (0, 1): 2})
bad = [(2, [(1, 1), (0, 1)]), (2, [(0, 1), (0, 1)]), (2, [(-1, 1)]),
       (2, [(2, 1)]), (2, [(0, 0)]), (2, [(0, -1)]), (2, [(0, 2)]),
       (2, [(True, 1)]), (2, [(0, True)]), (2, [(0.0, 1)]), (2, [(0, 1.0)]),
       (0, [(0, 1)]), (2, [(0,)]), (-1, []), (True, [])]
for a, pairs in bad:
    try:
        prepare_sparse_floor_records(packed, runs, a, iter(pairs))
    except ValueError:
        checks["invalid_sparse_refusals"] += 1
    else:
        raise AssertionError((a, pairs))
assert prepare_sparse_floor_records(packed, runs, 2, iter([(0, 1), (1, 1)]))[0] == 2
checks["exact_floor_boundary_admissions"] += 1
assert checks["one_sided_branches"] > 0 and checks["infeasible_envelopes"] > 0
print(dict(sorted(checks.items())))
```

Reproduce from the repository root without creating another file or bytecode:

```sh
sed -n '/^```python$/,/^```$/p' research_algorithms_20260920/Similarity-Floor-Core-Review.md | sed '1d;$d' | /Users/amuldotexe/.local/bin/python3.11 -B
```

## 9. Executed Counts And Bounded Verdict

Executed the embedded checker with the command above on 2026-09-21 using Python
3.11, exit status zero. No project import, separate checker file, public timing,
or implementation/test edit was used. The first run caught a mistaken proposed
summary-collision fixture: Y with `(H,C)=(2,2),(2,1)` also gives `1/3`, not `1`.
Moving Y's positive lower bound to the queried leaf, using `(3,2),(1,1)`, gives
the asserted collision. The corrected example, subsequent exception checks, and
the final baseline-equality checks were all rerun successfully. No theorem was
inferred solely from those finite runs.
The subsequent balanced-family, lower-bound, and clipped-capacity checks also
passed in the final complete rerun. The lead's separate receipts remain excluded.

Exact emitted counters:

```text
balanced_conditioned_frontiers: 192
balanced_feature_witnesses: 12
balanced_floor_refusals: 12
balanced_root_frontiers: 48
balanced_strict_families: 12
bundle_admitted_profiles: 3343
bundle_configurations: 1111
bundle_frontier_values: 10269
bundle_query_profiles: 65641
bundle_refused_profiles: 62298
clipped_capacity_identities: 2025
coarse_refused_leafsafe: 5078
compatible_pairs: 439
complete_envelopes: 68
conditioned_frontiers: 330
exact_floor_boundary_admissions: 1
exception_bundle_equalities: 65851
exception_complete_envelopes: 306
exception_conditioned_frontiers: 7380
exception_families: 8
exception_infeasible_envelopes: 18
exception_orientations: 606
exception_query_profiles: 153
exception_strict_improvements: 108
family_admitted_profiles: 22
family_bundle_equalities: 42
family_floor_refusals: 187
family_orientations: 41
family_query_profiles: 209
feature_assignments: 45262
floor_baseline_equalities: 66
hard_refusal_envelopes: 6
indistinguishable_summary_pairs: 1
infeasible_envelopes: 2
invalid_sparse_refusals: 15
lower_bound_parameter_pairs: 28
lower_bound_patterns: 494
lower_bound_singleton_queries: 3514
numerical_families: 14
one_exception_separations: 1
one_sided_branches: 18
ordered_tree_shapes: 626
owner_lookup_pairs: 507856
recurring_owner_cases: 14690
sharp_run_families: 6
structural_masks: 64979
unsafe_relaxation_examples: 1
zero_leaf_components: 6370
```

Counter interpretation: 64,979 deletion masks span 626 shapes through G=8; six
additional realizable tight-run families cover r=0..5. Owner lookup pairs each
compare both binary search and a forward scan to graph-derived ownership. The
65,641 bundle query profiles span 1,111 ordered bundle configurations; 3,343 are
floor-admitted and 62,298 refused, including 5,078 that are nevertheless
leafwise-safe. Exception bundle equalities count repeated profiles at different
tau values, not 65,851 independent datasets.

The floor fixtures have 209 query profiles across 14 named metadata families,
22 admitted and 187 refused. Their 68 complete-pair envelopes comprise 66
feasible cases, **all equal to the ordinary union/length baseline**, and two
infeasible cases. The eight exception fixtures have 153 profiles and 306
envelopes, including 18 infeasible and 108 strictly below the union/length bound.
The 45,262 feature assignments and 439 compatible pairs include repeated families
and the six explicit refused-query envelope checks. They are not unique graphs
or public workloads. The unsafe-relaxation counter is a direct arithmetic
example, not an extra feature-pair enumeration.

The balanced-family checks add exactly 12 actual-feature witnesses, 48 root
frontiers, and 192 retained-node frontier comparisons; all 12 floor queries are
refused while the clipped-exception form achieves the claimed strict bound.
The lower-bound checker distinguishes 494 position patterns over 28 `(G,e)`
pairs using 3,514 singleton queries, each checked with an explicit witness.
Clipped min rewards and population predicates match in all 2,025 enumerated
`(l,C,tau,q)` cases. These counts are independent of the lead's similarly named
prepared-core and collapse receipts.

**Final verdict:** the sharp 3r+1 owner-run theorem and restricted exactness hold;
floor-only stronger pruning is falsified by the exact union/length collapse.
The tau-exception extension has an exact O(r+e+1)-record representation, safely
clipped reward capacities, and verified strict separations with one or two
exceptions. The balanced two-exception family has R=3 at G=4 and R=4 for G>=8,
assuming even M>=4. Its exception-position information is necessary in the
worst case for arbitrary exception patterns: at least log2(binom(G,e)) bits
even at r=0. Practical usefulness,
retained-memory implementation behavior, and scientific novelty remain
unestablished.

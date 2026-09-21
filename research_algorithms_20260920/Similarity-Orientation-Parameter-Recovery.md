# Similarity: Exact Recovery By Strict Orientation Width

Date: 2026-09-20. New count-only algorithm, finite oracle evidence and completed independent mathematical review. No complete-query integration, physical-memory measurement, independent code audit, or publication-priority claim for this new evaluator.

## Why Another Parameter Is Needed

The [overlap-frontier theorem](Similarity-Overlap-Frontier-Compression.md) replaces a table over target intersection I with one over query/union overlap Q. That helps when Q is small. It does not admit a small table when both numeric counts are huge. Meanwhile, the [complete-query experiment](Similarity-Overlap-Query-Integration.md) rejects the inference that a tighter exact bound necessarily pays for itself: on two fixed public workloads the extra filter is slower than the interval control.

This study asks a different mathematical question: can the exact complete-pair envelope depend on the number of genuinely conflicting tree splits, rather than either large cardinality? The answer developed here uses structural branching followed by two independent, linear-size row problems. It does not change which bound is returned or promise better public-query pruning.

## Contract And Parameter

Use exactly the preceding complete two-row metadata contract:

- U is the union of the two target sets, partitioned into G feature leaves of a binary tree.
- H_v is the union population in subtree v.
- C_v is the exact attained maximum of the two row populations in subtree v, not an arbitrary upper bound.
- q_leaf is the query/union count in each leaf; a is the entire query size, including features outside U.
- Root row sizes are u=C_root and ell, with ell<=u. Thus I=ell+u-H_root.
- Maximize Jaccard over either row and every complete pair consistent with those observations. A zero intersection has score zero, including empty/empty.

For each internal node set D_v=C_left+C_right-C_v. Negative D is impossible metadata. Call a node strict when D_v>0; let r be the number of strict nodes. Additive nodes have D_v=0.

Reviewed result: **exact recovery by at most 2^r assignments, O(G*2^r) tree-node visits and O(G) retained integer words**, with no table indexed by Q or I. Integer bit width and rational arithmetic still cost resources. This is fixed-parameter enumeration in r, not a polynomial-time algorithm for all metadata.

The retained implementation supports power-of-two G<=256, matching the other probes. Branch/work admission can refuse before enumeration. General binary trees are covered by the argument, but not a separate implementation claim.

## Orientations Are Mostly Forced

Label each node by a row that attains C_v. Either row can be labeled major in a tie. Name a root-major row row 0; row 1 has size ell.

At an additive node, the parent-major row has C_v=C_left+C_right members. Since its two child counts cannot exceed those maxima, it must attain both. We can label both children with that same major row, including ties.

At a strict node, child-major labels must be opposite. If the same row attained both child maxima, that row would have C_left+C_right>C_v members in the parent. A tied child is also impossible at such a split: whichever row attains the other child's maximum would violate the parent maximum. Therefore there are exactly two possible child-label directions to consider, some of which may later prove infeasible.

```text
Additive split: D=0              Strict split: D>0

       major = A                      major = A
          |                              |
     +----+----+                  +------+------+
     |         |                  |             |
     A         A             children A,B  or  B,A
     forced labels                one branch bit
```

Propagating these rules from the root enumerates at most 2^r labelings. It does not enumerate every independently labeled node. Every actual compatible pair admits one of these canonical labelings; duplicate labelings/witnesses do not affect a maximum.

## Conditioning Separates The Two Rows

Fix one such labeling. For each row separately impose:

1. At a leaf where this row is major, its count is exactly C_leaf.
2. At a leaf where it is minor, its count lies in [H_leaf-C_leaf, C_leaf].
3. At an internal node where it is major, its descendant counts sum to exactly C_v.
4. At any other internal node, their sum is at most C_v.
5. Its root size is u for row 0 and ell for row 1.

These are two independent laminar cardinality systems. To see why no hidden union coupling was lost, take any feasible pair of their leaf-count vectors x,y. One count in every leaf equals C and the other is at least H-C, so x+y>=H. Each count is at most H. Therefore subsets of an H-element union of sizes x,y covering the entire union exist: choose the first subset, then let the second contain its complement and any necessary extra elements of the first. Internal unions are automatically correct because the leaves partition U. The fixed labels make every C_v attained.

For a chosen scored row with leaf size x, choose its features to attain min(q_leaf,x) query overlap. The other row can still be completed as just described. Its internal constraints concern counts, not which features it took. Thus maximizing the scored row's overlap is exactly a one-row separable optimization, **provided the other row's count system is feasible**.

The two rows need not attain their individual optimal query overlaps simultaneously. We compute the maximum score of either possible target over all compatible pairs, so different maximizing witnesses are legitimate. Requiring simultaneous optimality would be a different problem.

The independent review makes this distinction concrete. In one leaf with union {a,b,c}, both target sizes two, and query {a,b}, either target can individually attain overlap two with a suitable companion. They cannot both equal {a,b}, because their union would omit c. Our maximum-of-either-target envelope is still exactly one; a sum-of-both-overlaps objective could not reuse the same independent optimization argument.

## Four Scalars Per Row Subtree

For one row retain a record (L,U,A,B): its feasible subtree sizes are precisely the integer interval [L,U], and its best query overlap at size x is

```text
F(x) = min(x - A, B),  L <= x <= U.
```

This is a capped linear function with marginal gains one and then zero. At a leaf, F(x)=min(x,q_leaf), so A=0 and B=q_leaf; choose the fixed or ranged leaf interval according to its label.

To merge child records, first set:

```text
L    = L_left + L_right
U    = U_left + U_right
base = F_left(L_left) + F_right(L_right)
top  = F_left(U_left) + F_right(U_right)
A    = L - base
B    = top
```

For each integer size x in [L,U], distribute x-L extra members between the children. Each child's initial increments gain one overlap until its cap, then gain zero. Taking available gain-one increments first proves the exact merged objective base+min(x-L,top-base), which is min(x-A,B). All intermediate cardinalities are feasible because sums of integer intervals are intervals.

Apply the node constraints by truncating U to min(U,C_v). If this row is labeled major, raise L to max(L,C_v); together these force size C_v. Reject if L>U. Truncation preserves the function on the remaining domain. At the root evaluate at the prescribed size, rejecting if it is outside the interval.

This closed form holds **after conditioning on orientations**. Taking a maximum over differently constrained branches need not be concave; it does not contradict the earlier nonconcavity and exponentially many pieces examples.

## Complete Algorithm

```text
validate sizes, counts, tree and nonnegative internal D
r = number of positive internal D
reserve 2^r assignments and 2^r * (5G-3) node visits
refuse if either explicit enumeration budget is exceeded

best = no feasible pair
for every assignment of the r strict-split bits:
    propagate canonical major-row labels from the root
    solve row 0 by the four-scalar tree pass at root size u
    solve row 1 by the four-scalar tree pass at root size ell
    if either row is infeasible: continue
    evaluate the two exact rational Jaccard candidates
    update best

if no branch is feasible: reject impossible metadata
otherwise return best
```

Soundness: each admitted branch supplies two feasible count vectors and the union-completion construction supplies actual target sets. Each scored-row objective is attainable by some such pair, so no returned score exceeds the exact envelope.

Completeness: every compatible pair has a canonical labeling, its two count vectors satisfy that branch, and the row optimizer is at least its realized query overlap. Taking both row scores and all branches therefore misses no envelope optimum. Together, these establish equality, not just an upper bound.

## Resources And Failure Behavior

A branch propagates G-1 internal labels and performs at most two passes over 2G-1 nodes: at most 5G-3 visited nodes. The probe can stop a row pass early when it finds infeasibility. Exact fraction construction/comparison and constant-size branch bookkeeping are additional; these counters are not CPU instructions or a latency budget.

The implementation retains populations, gaps, one orientation vector and at most one row's table at a time. The row table has 2G-1 usable four-scalar records. There is no list of all assignments, no count-sized row array, and no expanded feature set. The O(G) integer-word claim excludes the caller's input storage, metric bookkeeping and interpreter/allocator overhead; at b-bit counts the main payload needs O(G*b) bits. Big-integer additions, multiplications, comparisons and exact-rational normalization are not constant-bit operations.

The default branch cap is 1,000,000 and default reserved-node-visit cap is 10,000,000. Both are strict nonnegative integers. Computing a large 2^r reservation is safe under the supported G limit, and does not enumerate it. Refusal raises an explicit resource error; it is never an exact zero score. A future query dispatcher must retain its existing sound fallback on this error. No such integration is claimed here.

For the G=4 layout used in the two public experiments, r<=3, so any valid summary needs at most eight assignments and 136 reserved node visits. This bound follows from layout size without knowing query cardinality or target degrees. It is not a measured runtime comparison: a visited orientation-DP node is a different cost unit from a threshold cell or dense transition. Larger feature trees can still trigger exponential enumeration and require refusal.

## Separation When Both Numeric Counts Are Large

Scale every entry of this fixed summary by any positive integer s:

```text
a=4s, ell=u=20s
capacities = s * [0,20,4,16,3,3,16,0]
leaf q     = s * [2,2,0,0]
leaf H     = s * [4,4,16,0]
```

There is one strict node, since 3s+3s-4s=2s, and the other two splits are additive. Q=4s and I=16s both grow. The exact envelope is 1/7; the cheaper interval relaxation is 1/5.

| Scale s | Q | I | Strict nodes | Branches | Reserved node visits | Exact bound |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 4 | 16 | 1 | 2 | 34 | 1/7 |
| 1,000,000 | 4,000,000 | 16,000,000 | 1 | 2 | 34 | 1/7 |
| 1,000,000,000,000 | 4,000,000,000,000 | 16,000,000,000,000 | 1 | 2 | 34 | 1/7 |

All three orientation cases execute with those admission limits. On the last two, both old table solvers refuse their preflight under a 1,000-cell allowance. No trillion-feature input was read and no huge table was allocated. The input consists of a few integer counts. This establishes a distinct structural parameter regime beyond small Q or small I, not a graph ingestion or physical-RAM result.

The earlier covering-hardness construction has one strict split per item gadget. Its r can grow with the number of items, so exponential dependence on r is compatible with that obstruction. Additive-only metadata gives r=0 and a linear-tree pass, not an NP-hard case being solved for free.

## Verification And Research Boundaries

Retained [implementation](experiments/probe_orientation_parameter_similarity.py) and [tests](experiments/test_orientation_parameter_similarity.py). Four missing-function failures preceded implementation. Four focused tests then passed in 0.475 seconds:

- 29,440 complete actual-summary/query cases over two/four leaves match the independently enumerated set-pair oracle. These reuse the earlier finite family, not a new larger corpus.
- 1,024 new seeded eight-leaf cases match the dense reference evaluator. These are distinct reference comparisons, not an independently exhaustive full-set oracle.
- Both-count scaling, the strict 1/7 versus 1/5 interval separation, budget refusal before branch visits, impossible metadata, empty sets and identical large-count sets are covered.
- Node visits never exceed their reservation; only one row-table allocation is retained by the helper at a time.

The resource counters do not prove Python RSS. Tests do not replace a proof or show useful source-data prevalence. The [completed independent mathematical review](Similarity-Orientation-Parameter-Review.md) accepts the orientation enumeration, independent count-system reduction, four-scalar recurrence and complexity theorem. Its separate 28 actual-pair comparisons include r=0,1,2,3, infeasible labelings, root ties and the simultaneous-optimality counterexample. The reviewer did not inspect or run the candidate implementation or main tests, so mathematical review must not be reported as a code audit.

The review additionally gives an integral tree-flow formulation of each conditioned row problem: subtree flows carry its cardinality quotas; two leaf reward arcs model query/nonquery capacity. The four-scalar pass specializes that standard allocation problem. It also stresses intersecting quotas with the already feasible domain instead of overwriting it, and requiring both rows feasible before retaining a branch. The implementation follows those domain/feasibility rules. This observation is the lead's reading, not an independent implementation verdict.

After adding the explicit interval-separation assertion, the combined similarity suite passed **64 tests in 20.427 seconds**. This is verification duration, not an algorithm timing measurement. Reproduce with:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_orientation_parameter_similarity.py'
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
```

Implementation SHA-256: `5f8370f506306918193e0e019cc22b84488b5d2bd81b767b571ab02c687c4779`. Test SHA-256: `328ce6fc5abfc808931e0384fff89720d5187ed69488d1f2e149aaf65d68d5b4`.

Completed independent mathematical review SHA-256: `abb17b9453d21eb01fa85bae23af649fc917ab1f37cfb4999209e3f45f2e7134`. The review was read in full and the reviewer closed before this checkpoint.

## Prior Art And Honest Contribution Scope

Branching on disjunctions and solving the conditioned continuous/integer allocation problem is established methodology. Laminar cardinality systems, separable concave allocation and capped-linear convolution are not new primitives. The [profit-indexed laminar DP paper](https://arxiv.org/pdf/2304.13984) is a relevant comparison, but this construction eliminates its need for numeric-size tables only under the particular two-row attained-maxima structure. That is not a speed claim against their different problem.

[Murota's 2016 author exposition](https://www.mechanism-design.org/arch/v001-1/p_05.pdf), equations (4.34)-(4.35) and Theorem 6.1(9), explicitly treats separable/laminar concavity and closure under integer supremal convolution. Those sections were inspected. They are direct precedent for the conditioned allocation foundations, not proof that the two-row orientation reduction or its parameterization has already appeared. The four-scalar closure here is a particularly simple capped-linear instance, not a new general convolution theorem.

The candidate scientific statement is narrower: additive/strict attainment gives a 2^r canonical orientation enumeration, fixed orientations separate the two full-union row systems, and each conditioned row frontier has an exact four-scalar representation. This yields a count-independent parameterized evaluator of the same complete-pair envelope. Its assembly is newly derived in this research; first-publication novelty remains unestablished.

Known group indexing, including [LES3](https://arxiv.org/pdf/2107.10417), remains a control rather than part of this claim. A stronger metadata solver does not improve the access index or automatically beat direct posting scoring. On the preceding public experiments, automatic dense/threshold dispatch admitted every candidate and already returned this same exact envelope: changing the evaluator cannot increase its 47/481 avoided-membership totals under an unchanged schedule. It could change evaluation overhead; that has not been measured here.

## Next Decisive Work

Follow-up, 2026-09-21: the [additive-region core](Similarity-Additive-Region-Core.md) now strengthens the supplied-count work to O(G+(r+1)*2^r) with an O(r+1) compiled core. It has completed independent mathematical review and nine new tests, bringing the combined similarity suite to 73. This document and its implementation remain the original orientation baseline; the new core does not alter the existing negative public query receipts. See the linked follow-up for current evidence and remaining steps.

1. The independent theorem challenge is resolved in the linked review. Preserve its tie, full-union, domain-intersection and separate-marginal-witness qualifications when implementing any extension.
2. Compare this structural parameterization with established disjunctive/tree allocation methods at the precise input and output contract, not only by name.
3. Characterize r and the priced admission regimes on relevant inputs before integrating a third evaluator. Any practical comparison must preserve the interval/native-DAAT controls and the negative prior receipts.
4. Continue the other six algorithm families. This result does not replace the original seven-family objective with a similarity-only study.

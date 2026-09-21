# Exact Similarity Envelopes On An Additive-Region Core

Date: 2026-09-21. A05 research extension. Status: implemented count-only evaluator; independent mathematical review completed; 73 combined similarity tests pass. Publication priority, useful customer prevalence, complete-query speed and physical RAM savings are not established.

## 1. Contribution Under Investigation

The [strict-orientation evaluator](Similarity-Orientation-Parameter-Recovery.md) computes an exact two-row Jaccard envelope in O(G*2^r) arithmetic work and O(G) integer words. Here G is the number of feature-tree leaves and r is the number of internal nodes whose child maxima sum to more than the parent maximum. Large numeric counts do not force numeric-size tables, but every orientation still reprocesses the entire feature tree.

The [closest-art assessment](Similarity-Orientation-Prior-Art-Assessment.md#61-an-additive-region-core) proposed removing that repeated G factor. This note implements and challenges that suggestion. The proposed strengthened result is:

> Given exact union populations and attained maxima for exactly two target sets on a binary feature tree, compile additive regions in O(G) arithmetic work. The exact maximum-of-either-row Jaccard envelope can then be evaluated with at most 2^r assignments on an O(r+1)-node core, for O(G+(r+1)*2^r) total arithmetic work. The compiled core and branch workspace use O(r+1) integer words; preprocessing/input still use O(G).

This is a specialized preservation result using established branching, redundant-constraint elimination and capped-linear allocation. It is not a new generic flow or convolution primitive. Calling an equivalent competitor that imports this reduction an independent historical precedent would also be incorrect. Scientific priority remains a separate closest-art question.

## 2. Exact Input And Output Contract

The two target rows T0,T1 have an exact union U partitioned by a binary tree. For every node v:

```text
H_v = number of union features in that subtree
C_v = max(number from T0, number from T1) in that subtree
D_v = C_left + C_right - C_v   [internal nodes only]
```

Root row sizes are u=C_root and ell<=u. Query size is a; q_j is the query/union intersection in leaf j, with 0<=q_j<=H_j and sum q_j<=a. Outside-union query features remain in the Jaccard denominator. Both rows must exist jointly, realize all maxima, have the declared sizes, and cover U. The result maximizes Jaccard over either row and all such complete pairs. Zero overlap has score zero, including empty/empty, matching the earlier research contract.

The algorithm is not recovering the original hidden pair. A maximum for row 0 and a maximum for row 1 may require different compatible companion sets. It does not optimize a joint sum of both row scores.

The retained [probe](experiments/probe_additive_core_similarity.py) uses the existing validated balanced heap-tree representation: power-of-two G in 1..256, nonnegative built-in integers, exact union populations and attained maxima. The mathematical argument applies to arbitrary binary trees, but this implementation does not expose a general-tree API.

## 3. What Is Removed

When D_v=0, any row attaining C_v must attain both child maxima. Thus its major-row label propagates without a choice. When D_v>0, its children have opposite untied majors, providing one binary choice. Root ties can be canonically named with row 0 major; they do not add a branch bit.

Remove strict vertices conceptually and contract each remaining connected additive region into a component node. Keep every strict vertex. Each component retains its top capacity and all downward strict boundaries. Bundle its ordinary leaves into a four-scalar reward record. A component with no ordinary leaves retains a zero bundle, rather than disappearing or swallowing strict boundaries.

```text
Original feature tree                 Retained core

       additive top                   component top
        /        \                    /           \
 additive       leaves          strict boundary   leaf bundle
  /    \                              /    \        L,U,A,B
...   strict                         ...   ...
       /  \
      ... ...
```

The diagram is conceptual: bundles are fields in component records, not separately allocated graph vertices. This avoids counting a virtual leaf twice in the core-size statement.

## 4. Why Interior Constraints Are Redundant

For one additive component, repeatedly expand C_parent=C_left+C_right until reaching its boundary strict nodes and ordinary leaves. The top capacity equals the sum of all boundary capacities. The same identity holds for every removed interior subtree with its own boundary subset.

For the component-major row, every boundary count is upper-bounded by its capacity. Achieving the top capacity forces every boundary to attain its own capacity. Every removed interior equality then follows. Immediate strict boundaries inherit the component's canonical label, and their own subproblems must actually admit their maximum. At a tied root, the other row's prescribed total can also force boundary maxima; it still needs feasible boundary domains.

For the component-minor row, summing upper-bounded boundary counts cannot exceed any removed interior capacity. Therefore those interior upper quotas impose nothing beyond the retained boundaries. There are no lower quotas at an interior minor node beyond what its descendants already require.

This is not permission to drop arbitrary laminar constraints. It depends on **exact additivity plus the propagated label and all boundary constraints**. A strict node must remain because its parent quota is smaller than the sum of its child capacities.

Projection from an original feasible count vector to its component/boundary totals is consequently sound. Conversely, if boundary counts and the bundled leaf total can be lifted to individual leaves, every deleted interior constraint follows from the preceding argument. This supplies the count-domain preservation step.

## 5. A Bundle Is Not An Aggregate Leaf

For ordinary leaves J in one component, let l_j=H_j-C_j and u_j=C_j. Define:

```text
L = sum l_j                 U = sum u_j
b = sum min(q_j,l_j)        B = sum min(q_j,u_j)
A = L-b

minor frontier: F(x) = min(x-A,B), for integer L <= x <= U
major frontier: only x=U, with value B
```

At the minimum occupancies, b query elements can be taken. Increasing total occupancy by one can gain one query element until the B-b available rewarding increments are exhausted, then gains zero. Each leaf domain is an integer interval, so distributing increments creates every integer total between L and U. Reward-one increments can be taken first because they precede reward-zero increments in each leaf. This proves both the exact domain and the optimal overlap at every total, without a feature-sized array.

All bundle-optimal counts lift to actual leaf subsets: choose query elements first within each leaf. The full-union companion construction from the earlier theorem remains valid because it depends on sizes, not on which query elements were chosen. Both complete row count systems must still be feasible before accepting a branch.

**Necessary counterexample:** leaf triples (H,C,q)=(3,2,0),(3,2,3) produce L=2,U=4,A=1,B=2. The minor frontier is 1,2,2 at sizes 2,3,4. Replacing them by only total H=6,C=4,q=3 would yield 2,3,3 and falsely inflate the bound. The four scalars preserve forced nonquery occupancy; naive count aggregation does not.

The record preserves the best overlap per total, not every suboptimal overlap, the identities of selected features, or one particular original hidden pair. Explicit witnesses need the original leaves and additional reconstruction work.

## 6. Evaluation And Root Admission

Retained nodes are emitted in postorder. Strict nodes retain two ordered children and their original branch-bit index. Components retain a tuple of strict-boundary children and a bundle. The root is last. There is no list of all orientation assignments and no original G-sized array in the returned core.

For each assignment, propagate labels from root 0 through the core. At a strict node the bit selects opposite labels for its left/right child; at a component all boundaries inherit its label. For each row, merge child/bundle records using the existing exact capped-linear sum operation, then intersect with the node capacity: every row has upper bound C, and the node-major row also has lower bound C. A failed intersection is infeasible, not an empty optimum of zero.

Finally, row 0's root domain must contain u and row 1's domain must contain ell. A branch with an infeasible companion is discarded even if the scored row has an attractive optimum. For every feasible branch evaluate each row's overlap t as t/(a+size-t), or zero for t=0. Keeping the largest rational value yields the complete-pair envelope.

One row table is returned and reduced to its root record before the second row table is built. The two root records coexist, not two full row tables. Public helper records are trusted intermediate objects made by this compiler; their dictionaries are not an adversarial serialized format with a separate validation contract.

A lead ownership check found a real implementation qualification: the nested recursive compiler initially formed a self-referencing closure cycle holding its input arrays. Even after returning the small core, CPython could retain those arrays until cyclic garbage collection. A weak-reference test with cyclic GC disabled reproduced this retention. The compiler now explicitly breaks that recursive closure cycle in `finally`; the successful-return ownership test passes. This does not force the caller to release its own inputs, clear retained failure tracebacks, or prove process RSS. It prevents the compiler's otherwise-unreachable cycle from making successful input release GC-dependent.

## 7. Size, Work And Numeric Costs

Deleting r vertices of an undirected binary tree creates at most 2r+1 nonempty remaining components: a strict root has degree two, other strict vertices degree at most three, and adjacency among removed vertices can only reduce that maximum. Therefore, with c components:

```text
c <= 2r+1
K = r+c <= 3r+1          retained core nodes
E = K-1                 retained core edges
```

High-degree components are permitted. Their total child edges across the core are still K-1; charging a whole G-sized boundary list per component would be an implementation error. Every original node is visited once by the contraction traversal. Validation and strict-index construction make additional linear passes, so `preprocessing_node_visits` is explicitly the contraction traversal count, not every CPU operation.

The independent review sharpens the counts: with t=1 if the original root is strict and k original strict-to-strict edges, `c=2r+1-t-k`, `K=3r+1-t-k`, and `E=3r-t-k`. It constructs realizable skewed trees attaining the uniform upper bounds. Those general-tree witnesses are mathematical/reviewer evidence; the retained lead probe still exposes only balanced heap trees. An implementation using separate bundle vertices would instead have at most 5r+2 total vertices; our payload representation uses K.

The probe reserves these branch-work units per assignment:

```text
K label-node visits + 2*(K row-node visits + E child records + c bundles)
= 5K-2+2c <= 19r+5.
```

An infeasible child may shorten a merge, so actual work is at most the reservation. The counter excludes scalar checks, child-label assignments, rational arithmetic and Python bookkeeping. It is an auditable logical unit, not a CPU instruction count or latency deadline. Branch-cap and work-cap refusal occurs before any assignment is visited; validation/core construction is paid before that refusal.

The core and one branch use O(r+1) count words. The builder retains populations/strict indices and an additive traversal stack in O(G) words, alongside caller-owned inputs. At b-bit counts, arithmetic and allocation depend on b; rational comparisons and normalization are not constant-bit operations. This is not a polynomial kernel in encoded bit size, a constant-RAM query, or an end-to-end memory proof.

The useful parameter inequality is r <= sum D_v = sum_leaf C_j-u <= H_root-u = ell-I, where I=ell+u-H_root. It follows by telescoping deficits and C_j<=H_j. Small exclusive population can constrain r even when intersection and query counts are large. Conversely r=0 does not imply that the actual sets are nested.

## 8. Executed Evidence And Corrections

Six tests first failed because the module did not exist. After implementation, all numerical comparisons passed, but the seeded test did not cover r=3. An explicit nested-strict example T0={0,4,2}, T1={1,3,7} with four modulo feature groups was added. The finite retained-node frontier count is **2,932**, replacing an unjustified expectation that it would exceed 3,000. No mathematical mismatch was concealed by that correction.

The combined new/original focused command passed **10 tests in 1.038 seconds**:

- **29,440 actual complete-pair/query envelopes** match independent feature-set enumeration, including outside-union query elements. These reuse the preceding actual-summary corpus; they are not 29,440 new datasets.
- **2,932 conditioned retained-subtree frontier checks**, across 256 seeded fixtures and the explicit nested-strict fixture, compare every feasible integer count and its maximum overlap with direct leaf-count enumeration. They cover r=0,1,2,3 and infeasible subtrees, not just final root scores.
- **1,024 deeper eight-leaf comparisons** match the original orientation evaluator. This is a reference cross-check, not independent exhaustive feature enumeration.
- Fixed-r scaling, exact huge counts, the A>0 bundle counterexample, invalid metadata, empty sets, an infeasible companion, and pre-enumeration budget boundaries are covered.

The frontier oracle enumerates leaf cardinalities and directly checks all descendant constraints. It does not use the capped-linear merge. Final envelope tests independently enumerate compatible feature sets. These complementary oracles address different failure modes; neither is a proof for arbitrary input sizes.

The [completed independent review](Similarity-Additive-Core-Review.md) supports full boundary-tuple projection and optimal-overlap lifting, including nested/adjacent strict nodes, zero bundles, root ties, positive A and an infeasible companion. Its self-contained checker was replayed by the lead with no project imports: **902 subtree frontiers, 590 boundary maps containing 1,007 feasible tuples, and 50 complete feature-set envelopes** passed. These are repeated checks across nine small families and 25 query configurations, not independent large datasets. The reviewer did not inspect the lead code or tests, so this remains mathematical review rather than an independent code audit.

Two explicit follow-up regressions preserve the review's consequences. A child maximum larger than its parent's is now rejected before contraction; that test first failed, then passed after the local guard. A branch with a feasible scored row but an impossible companion is discarded on the two-leaf `(H,C)=(1,1),(2,2)` example, returning 1/2 rather than the false score one. The final full similarity suite passed **72 tests in 20.906 seconds**, with eight new core tests. This duration is verification cost, not an algorithm timing result.

After the ownership regression and closure-cycle fix, the current focused core/orientation run passed **13 tests in 1.049 seconds**, and the current full similarity suite passed **73 tests in 21.197 seconds**. There are now nine new core tests. The preceding 72-test result is retained as the pre-ownership-fix checkpoint, not the final verification status.

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest \
  test_additive_core_similarity test_orientation_parameter_similarity
```

Full suite from repository root: `python3.11 -B -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'`. Review-check reproduction is retained in the independent memo. The old orientation implementation remains unchanged.

| Artifact | SHA-256 |
| --- | --- |
| Core implementation | `3088145b00effe0f714d172c51c5d4176be7bde2d7ee7f4c4703efbf8cb669a5` |
| Core tests | `b3af0b22ffdd4cd7f304913824007e06e0db8a403af6db714627934d8ca0375b` |
| Independent mathematical review | `35b1da303b11b5d647be32d335f07dc24cfa65e5f3bbfecc51df85f804564df5` |
| Unchanged orientation baseline | `5f8370f506306918193e0e019cc22b84488b5d2bd81b767b571ab02c687c4779` |

## 9. A Structural Separation, Not A Timing Claim

Take two conflicting leaves with populations 4,4 and maxima 3,3. Their parent's maximum is four. Add G-2 leaves shared by both rows, each of population/maximum two, and arrange all other internal nodes additively. Put two query features in each conflicting leaf and none elsewhere. This has r=1 for any supported power-of-two G>=4 and exact envelope 3/(u+1), with u=2G.

| G | Contraction traversal visits | Core nodes | Assignments | Reserved branch work | Old reserved tree-node visits |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 7 | 4 | 2 | 48 | 34 |
| 16 | 31 | 4 | 2 | 48 | 154 |
| 64 | 127 | 4 | 2 | 48 | 634 |
| 256 | 511 | 4 | 2 | 48 | 2,554 |

All seven G values 4,8,16,32,64,128,256 were executed. The new counter charges child records and bundles, while the old counter charges tree nodes; **these columns are not a speed ratio**. They demonstrate the removal of G from repeated branch work, with linear preprocessing still visible. At small G the new representation may cost more, not less.

The retained scale-10^12 example from the previous study still returns 1/7 without numeric-count tables. It is a small symbolic count input, not a trillion-feature graph ingestion or full-output experiment.

## 10. Research Position And Product Boundaries

[Murota's discrete-convex-analysis exposition](https://www.mechanism-design.org/arch/v001-1/p_05.pdf) documents separable/laminar concavity and integer supremal convolution. Those are established foundations for the conditioned row optimizer, not a newly invented mathematical operation. The [detailed closest-art assessment](Similarity-Orientation-Prior-Art-Assessment.md) distinguishes this precise two-set/attained-maxima model from backdoor enumeration, flow relaxations, max-linear recovery and haplotype assembly.

[LES3](https://arxiv.org/abs/2107.10417) already uses set grouping and group-level indexing to prune candidates. This core does not claim invention of that access strategy. It accelerates an exact metadata envelope that might be placed in such a strategy. Whether paying for that envelope avoids enough real target reads remains the practical question.

The previous complete-query measurements remain negative: the exact pair envelope saved only 0.310%/0.203% additional body memberships and increased aggregate query time relative to the interval comparator. Returning the same envelope faster cannot increase those saved-membership totals under the same evaluation schedule. It could reduce evaluation overhead or make larger G affordable, but neither has been measured for this core.

Compilation here uses q_j, so its bundle rewards are query-dependent. A core for one query cannot be reused blindly for another. Static topology/population preparation and sparse per-query reward updates are a possible next architecture, but need their own implementation, accounting and evidence. Query-histogram construction, block selection, raw graph ingestion and output remain outside this count-only theorem.

## 11. Next Scientific Gates

1. The independent proof challenge is completed. Preserve its full boundary-tuple, separate-optimal-witness and bit-cost qualifications; a future integration still needs a scoped independent implementation review.
2. Preserve all prior negative receipts. Measure the new core only where its removed repeated G work actually occurs, with interval and ordinary posting-merge controls retained.
3. Characterize r and G jointly on useful data. A uniformly tiny G benchmark cannot demonstrate the asymptotic separation; an engineered low-r family cannot establish prevalence.
4. Investigate static-core/query-reward separation if repeated queries justify it. Charge all query-specific leaf aggregation rather than calling preprocessing free.
5. Continue the other six families. A supported new problem-specific theorem is meaningful progress, not proof of seven arXiv-worthy innovations.

# Similarity Orientation Parameter Review

Date: 2026-09-20. Bounded mathematical sidecar, separate from the completed integration code review.

## Verdict

**Accept the proposed exact `O(G*2^r)` arithmetic-work, `O(G)` integer-word theorem**, with the feasibility and witness qualifications below. Here `G` is the number of leaves in a binary feature tree with `O(G)` nodes, and `r` counts internal nodes with `D_v=C_left+C_right-C_v>0`. Neither numeric query overlap `Q` nor pair intersection `I` indexes a table. Integer bit costs, input preparation, and materializing witness features are separate.

Fixing the strict-node orientations turns each row into an independent laminar cardinality problem. The four-scalar value summary is exact for each such problem. Independence is about feasible **count assignments** and attaining either row's optimum with a compatible companion, not about simultaneously combining two independently optimized feature sets.

No counterexample to this theorem was found. A small distinct check of 28 complete-summary/query envelopes agreed with direct actual-pair enumeration. No candidate code or tests were inspected, imported, edited, or run for this mathematical review. Only this memo was written. No main suite or public timing was rerun; publication priority remains unestablished.

## 1. Assumptions and Labels

There are exactly two binary-set rows. The fixed feature tree partitions the exact union at its leaves. At node `v`, `H_v` is union population and `C_v` is the exact attained maximum of the two row occupancies. Query counts are known at leaves, `0<=q_j<=H_j`, with source size `a>=Q=sum_j q_j` including source features outside the union. The root has exact size extrema `ell,u`, with `u=C_root`.

For actual metadata, `0<=C_v<=H_v<=2C_v`, the union populations add across children, and every internal `D_v` is nonnegative. Invalid numeric metadata must be rejected, and an orientation with no feasible pair contributes nothing. Empty groups and zero overlap are allowed, using the established convention that zero overlap scores zero, including empty-empty.

A label `sigma_v` in `{0,1}` designates one row that must attain `C_v`. It does not claim that the other row is strictly smaller. Set `sigma_root=0` and root sizes `(u,ell)`. This loses no pair envelope: the two rows can be named so that row zero has maximum root size, and both targets remain candidates in the final maximum.

### Additive Nodes and Ties

If `D_v=0`, the designated parent-major row has occupancy `C_v=C_left+C_right`. Since its child occupancies are individually at most their capacities, it must attain both child maxima. Therefore assign both child labels to `sigma_v`.

This remains valid when either child is tied. If the parent is tied too, both rows attain both child maxima, so propagating the chosen parent label merely selects a valid description of the same pair. No extra branching is needed for ties.

### Strict Nodes

If `D_v>0`, no row can attain both child maxima, since that would give occupancy `C_left+C_right>C_v`. The two child maxima must consequently be attained by opposite rows. In fact, neither child can be tied: if one child's maximum were attained by both rows, whichever row attained the other child's maximum would violate the parent cap.

There are exactly two possible child-label patterns, `(0,1)` or `(1,0)`. Enumerate one bit for each strict node, independently of the inherited parent label; the subsequent cardinality feasibility tests enforce consistency with that parent label. Additive nodes propagate their inherited labels.

Every compatible pair has at least one labeling in this enumeration. Some of the `2^r` assignments may be infeasible or describe overlapping families of pairs. Such duplicates do not affect a maximum. The count is an upper bound on branches, not a claim of `2^r` distinct feasible datasets.

## 2. Independence After Labeling

For a fixed labeling, give row `i` these constraints:

| Location | Constraint on this row's occupancy |
|---|---|
| Leaf `j`, `sigma_j=i` | Exactly `C_j` |
| Leaf `j`, `sigma_j!=i` | Integer interval `[H_j-C_j,C_j]` |
| Internal node `v`, `sigma_v=i` | Exactly `C_v` |
| Internal node `v`, `sigma_v!=i` | At most `C_v`, with child occupancies adding |
| Root | Exactly `u` for row zero, exactly `ell` for row one |

These constraints are necessary. At each leaf one row has size `C_j`, and full union requires the other's size to be at least `H_j-C_j`.

They are also jointly sufficient, with no further coupling between the two count problems. Take any feasible leaf-count vector for each row, with leaf counts `x_j,y_j`. One count equals `C_j`, the other lies in `[H_j-C_j,C_j]`, and hence

```text
0<=x_j,y_j<=H_j, and x_j+y_j>=H_j.
```

For any chosen set `X_j` of size `x_j` within that leaf's union, construct the other row by including the entire complement `U_j minus X_j`, then any

```text
e_j = x_j+y_j-H_j
```

features from `X_j`. There are enough such features: `0<=e_j<=x_j`, and the resulting set has exactly size `y_j`. The leaf union is exactly `U_j`, not merely of adequate total cardinality. Taking unions across leaves realizes both count vectors simultaneously. All internal quotas and attained maxima follow from additivity and the assigned-major equalities. The root intersection automatically equals `ell+u-H_root`; no separate intersection table is needed.

Thus the feasible count family is a Cartesian product of the two row problems. Exact feature identities do not introduce another restriction because the only within-leaf information retained is population, query count, and the leaf maximum. Additional within-leaf correlations or per-feature constraints would change this claim.

### Why Only One Row Is Optimized at a Time

For either selected row, choose its features query-first at each leaf. Its achieved overlap is `sum_j min(q_j,x_j)`. The construction above then realizes **any feasible count vector** for the other row, without requiring that companion to attain its own best overlap.

The stronger simultaneous-optimality assertion is false. With one leaf `U={a,b,c}`, `C=ell=u=2`, and query `{a,b}`, each row separately can attain overlap two. They cannot both do so in the same pair: both would equal `{a,b}`, losing feature `c` from the union. Either marginal optimum is nonetheless compatible with a companion containing `c`, so the maximum over either target is exactly one.

This is not a counterexample to the proposed envelope, which takes a maximum over pairs and over either target. It would be a counterexample to combining the two overlaps as one jointly attained vector or optimizing an objective that rewards both simultaneously.

## 3. Four-Scalar Row Solver

Represent a subtree's feasible occupancy interval and optimal selected-row overlap by

```text
(L,U,A,B), with F(x)=min(x-A,B) for integer L<=x<=U.
```

At a leaf, take the fixed or ranged interval from Section 2, with `A=0`, `B=q_leaf`. Empty intervals are infeasible.

At an internal node, first require both child summaries to be feasible. Before the node's own quota, calculate

```text
L0 = L_left+L_right
U0 = U_left+U_right
base = F_left(L_left)+F_right(L_right)
gain = (F_left(U_left)-F_left(L_left))
     + (F_right(U_right)-F_right(L_right))

A = L0-base
B = base+gain
F(x) = base+min(x-L0,gain) = min(x-A,B).
```

Each child has a sequence of marginal gains equal to one followed by zeros on its feasible interval. The parent can allocate all available gain-one increments before allocating zero-gain increments, and child feasible integer intervals have no holes. This proves both the maximum formula and attainment for every total occupancy between `L0` and `U0`. It is not an assumption that the unconditioned orientation frontier is concave.

Now **intersect** the aggregate domain with this row's node quota:

```text
assigned major: [L,U] = [L0,U0] intersect [C_v,C_v]
other row:     [L,U] = [L0,U0] intersect [0,C_v].
```

At the root, also intersect with the row's prescribed singleton size. Reject an empty intersection. In particular, do not blindly overwrite `L=U=C_v` when `C_v` was outside `[L0,U0]`. Keep the curve parameters `A,B` from before clipping; at the next parent evaluate this child at its clipped endpoints.

Both row problems must be feasible before either score from a labeling is used. Their feasible count assignments can then be combined by Section 2. Checking only the objective row would admit branch descriptions with no valid companion.

## 4. Envelope and Complexity

For a feasible labeling, let

```text
c0 = F_root,row0(u)
c1 = F_root,row1(ell).

E_label = max(c0/(a+u-c0), c1/(a+ell-c1)).
```

Evaluate a zero-overlap term as zero before division. Maximize `E_label` over all feasible labelings. For fixed row size Jaccard increases with overlap. Each marginal optimum has a complete pair witness, and every compatible pair is included in some labeling. These two directions prove equality with the exact complete-pair metadata envelope, not merely safety as an upper bound.

Compute the strict-node set once in `O(G)`. Enumerate one labeling at a time, propagate labels, and solve both row trees in `O(G)` arithmetic work per labeling. Retaining labels, traversal state, both scalar-summary arrays, and the best score requires `O(G)` integer words. Do not retain all labelings or all branch tables. An `O(r)` bit-array/stack enumeration suffices without assuming that an arbitrarily large orientation mask fits one machine word.

The resulting bounds are

```text
arithmetic work: O(G*2^r)
integer words:  O(G).
```

The case `r=0` is included and gives linear work, even when `Q` and `I` are large. An unbalanced binary tree does not introduce a height multiplier: each branch uses ordinary whole-tree passes with `O(G)` nodes. Operations on large counts and exact rational scores still have bit costs. Constructing the sparse union/query histograms and explicitly emitting feature-level witnesses is not included in these post-count bounds.

This is fixed-parameter tractability in the strict-node count, not a polynomial-time result for arbitrary trees: `r` can be `G-1`. In the earlier covering construction each gadget contains a strict node, so its exponential branching is consistent with the compressed-count SUBSET SUM obstruction. Conditional one-row curves can have the four-scalar form while the maximum over all compatible orientations has complicated frontiers.

## 5. Generic Equivalence and Claim Boundary

The nearest generic formulation is **enumerating Boolean choices in max-attainment constraints, followed by two laminar resource-allocation problems**. Fixing a choice changes `max(x_v,y_v)=C_v` into one equality and one upper bound. Additive nodes force their choices, leaving only the `r` strict-node bits.

For a fixed labeling, each single-row problem also has a direct integral tree-flow formulation. The flow entering a subtree is its occupancy. Put the subtree's lower/upper quota on that edge; flow conservation enforces child sums. At a leaf, split flow into a capacity-`q_j`, reward-one arc and a capacity-`H_j-q_j`, reward-zero arc. The common leaf entry quota enforces its fixed or ranged total. Fix root flow to the prescribed row size. Its maximum reward is `sum_j min(q_j,x_j)`.

The four-scalar recurrence exploits this particular two-segment reward and tree structure instead of using a general flow optimizer. Boolean branching, tree allocation, and piecewise-linear value aggregation are not being proposed as new primitives. The specific reduction from complete two-row attained-capacity metadata, including forced additive orientations and full-union independence, is the mathematical content established here. This equivalence is derived in the memo; no new primary-literature search or global priority assessment was undertaken.

## 6. Small Independent Check

A fresh ephemeral algebra check used four fixed actual metadata families on at most six features, plus the three-feature simultaneous-optimality example. It did not import the prototype or rerun any existing oracle suite.

For the six-feature cases, the four leaves were `{0}`, `{1,2}`, `{3}`, `{4,5}`. Each row pair was converted to exact union and every attained maximum. The check directly enumerated ordered feature subsets preserving that summary and root sizes, then compared the best Jaccard score with orientation enumeration and the scalar row solvers.

| Generating pair | r | Labelings / feasible labelings | Compatible ordered pairs with row zero root-major | Queries checked |
|---|---:|---:|---:|---:|
| `{0,1,2,3,4,5}`, `{0,3}` | 0 | 1 / 1 | 15 | 6 |
| `{1,2,3}`, `{0,3}` | 1 | 2 / 2 | 5 | 6 |
| `{1,2,4,5}`, `{0,3}` | 2 | 4 / 1 | 1 | 6 |
| `{1,2,3}`, `{0,4,5}` | 3 | 8 / 2 | 2 | 6 |
| One leaf: `{0,1}`, `{1,2}` | 0 | 1 / 1 | 6 | 4 |

All 28 envelope comparisons matched, using exact cross-products. Queries included empty, full-union, asymmetric, and outside-union cases. The rejected labelings exercise the requirement that both row problems be feasible; the equal-root-size cases exercise canonical tie labeling. This small check corroborates the proof but does not certify the lead's implementation.

## 7. Scaled Count-Only Separation

The lead supplies, for a positive **integer** scale `s`,

```text
a=4s, ell=u=20s
C=s*[0,20,4,16,3,3,16,0]
q_leaf=s*[2,2,0,0]
H_leaf=s*[4,4,16,0].
```

The internal deficits are root zero, left `2s`, right zero. Thus `r=1`, with two orientation branches at every scale. The leaf on the right with population `16s` must be present in both rows: both rows have size `20s`, the left subtree is capped at `4s`, and the right at `16s`. The two queried left leaves then have row occupancies `(3s,s)` and `(s,3s)`, so either row's greatest overlap is `3s`. Therefore the exact envelope is

```text
3s/(4s+20s-3s) = 1/7.
```

The independent interval relaxation permits `(2s,2s)` in the left subtree, achieving overlap `4s` and bound `1/5`. This proves the claimed strict separation for every positive integer scale without constructing the expanded feature sets.

Here `Q=4s` and `I=16s` both grow. Under the previously specified full-table reservation formulas, threshold cells are `24s+14` and dense state reservations are `104s+7`; both exceed 1,000 at `s=10^6` and `10^12`. The orientation method's branch and node counts depend only on this fixed tree and `r`. The lead reports a maximum of 34 node visits; that exact instrumentation count was not independently audited. The mathematical claim established here is the scale-independent `O(G*2^r)` schedule, not a particular counter implementation.

This is a supported **counts-only** regime. Its union has `24s` features if expanded. It neither avoids that input-generation cost nor proves constant physical memory or constant CPU time as integer widths grow.

## 8. Reported Implementation and Economics

The lead reports a retained orientation prototype and four passing tests after four initial missing-function failures, covering 29,440 actual-summary envelopes, 1,024 new seeded `G=8` dense comparisons, count-only scaling/refusal, and invalid inputs. These reports are not this review's independent evidence. No prototype code inspection was needed for the mathematical claim and none was performed.

The lead also reports terminal public results: auto overlap was slower than interval by 10.32% on GrQc and 13.81% on FB, while saving only 47 and 481 memberships respectively. Those receipts were not independently inspected or rerun here.

For a block already admitted to an exact pair solver, this method computes the same envelope. At identical access order and exact heap state, it cannot create a tighter certificate or additional pruning for that block. It can change evaluation cost, and can admit count regimes refused by the old table budgets; neither fact establishes a public workload win. In particular, the reported savings on previously admitted work do not become larger merely because the exact envelope is evaluated differently. The interval comparator remains `O(G)`, and the empirical cost gap is not solved by this theorem alone.

## Closure

The bounded mathematical review is complete. Accept the orientation-parameter theorem subject to domain intersection, feasibility of both rows, exact union construction, and separate marginal witnesses. No kernel/test edit, code review of the new prototype, large enumeration, or public measurement was performed. Publication priority and performance remain unestablished; main goal7algorithms remains open.

### Closing Checkpoint

The lead identifies [Murota (2016), JMID, equations 4.34/4.35 and Theorem 6.1(9)](https://www.mechanism-design.org/arch/v001-1/p_05.pdf) as an inspected primary foundation for laminar/separable concavity and supremal-convolution closure. This is lead-inspected literature, not a paper independently reread in this bounded review. It supports the generic optimization foundation rather than establishing priority for the specific complete-pair metadata reduction. The branching, allocation, and closure primitives should not be claimed as new.

For a full binary tree with `G=4`, there are three internal nodes, so `r<=3` and at most eight orientation branches. Under a schedule counting two seven-node row passes and three internal orientation visits per branch, this gives at most `8*(7+7+3)=136` node visits. This is a defined schedule count, excluding preflight and other non-node work, not a CPU-time or physical-memory result; the prototype's counter implementation was not inspected.

The lead reports that the manuscript is written and 64 combined tests completed in 20.427 seconds. Neither the manuscript nor those runs were independently reviewed here. No further checks were launched for this checkpoint, and all reviewer-owned ephemeral checks had already completed. The prior negative public results remain unchanged. This closes the mathematical sidecar only, not the main research goal.

# Similarity Overlap Frontier Review

Date: 2026-09-20. Bounded mathematical follow-up; completed with the results below.

Only this new memo was written. The previous reviews were not edited. No implementation, repository tests, public benchmarks, index work, or manuscript changes were performed. The finite checks below were ephemeral and independent. The lead's reported 29,440 matching summary/query envelopes for the scalar reference implementation are not independently verified here.

## Verdict

**Latest follow-up: accept the inverse-threshold theorem.** The exact two-row envelope has an overlap-profit-indexed algorithm using `O(G+q log(G+1))` states and `O(G+q log(G+1)+q^2)` word operations on a balanced binary feature tree, after the query/union counts are paid. Here `q=|S intersect U|`, not the potentially much larger pair intersection `I`. Section 9 supplies the proof and bounded independent checks. This does not assume concavity or contradict the compressed-count hardness result below.

1. **Yes: the local feasible intersection counts always form an integer interval ending at `K=2C-H`.** There is an `O(G)` recurrence for its endpoints. This concerns subtree constraints before imposing a possibly smaller global intersection limit.
2. **No: neither winner-oriented value frontier is necessarily concave or restricted to increments zero and one.** Actual four-feature metadata gives `M(e)=[0,0,1]`; another query on the same metadata gives `m(e)=[0,1,3]`.
3. **No polynomial-size piecewise-affine frontier bound holds in general.** A two-leaf gadget encodes a fixed-charge covering choice. Combining gadgets at additive-capacity nodes yields frontiers requiring exponentially many affine pieces in the number of leaves.
4. **The obstruction extends beyond materializing a frontier.** A count-only exact two-row Jaccard envelope evaluator would solve SUBSET SUM through the construction below. Thus a general `O(G)` or `O(G^2)` evaluator independent of numeric counts, with polynomial-bit arithmetic, would imply `P=NP`. This is a compressed-count limitation, not a lower bound on scanning an explicitly expanded union, nor a proof that quadratic dependence on overlap is necessary.

The positive results now include the feasibility theorem, the sharpened winner recurrence, and the exact query-overlap-parameterized algorithm. The limitation concerns algorithms polynomial in the number of groups and count bit lengths alone; the new algorithm is polynomial in the numerical query overlap. These are derivations in this review, not claims of publication priority or paper readiness. Main goal7algorithms remains open.

## 1. Model and Orientation

There are exactly two rows. Each tree node has exact union population `H_v`, exact attained maximum `C_v`, and query count `q_v`. Children partition the node's union. Features within a leaf have no further retained constraints. All counts are integers, and the metadata comes from actual rows unless explicitly rejected as infeasible.

For local intersection `e`, define

```text
K_v = 2C_v-H_v
t   = K_v-e.
```

The two row sizes within this node are `C_v` and `C_v-t`. Thus `t` is the nonnegative size difference, not the intersection. Write

```text
P_v(t) = M_v(K_v-t): maximum first-row query overlap when its size is C_v
Q_v(t) = m_v(K_v-t): maximum first-row query overlap when its size is C_v-t.
```

At `t=0` the orientations coincide, so `P_v(0)=Q_v(0)`. Infeasible arguments have value negative infinity. A state must preserve the exact union and every attained maximum in its subtree; these are not merely independent upper bounds.

For a leaf,

```text
0 <= t <= K_v
P_v(t) = min(q_v,C_v)
Q_v(t) = min(q_v,C_v-t).
```

Attainment follows by choosing the first row's requested number of features query-first, then taking the leaf complement plus the required number of shared first-row features for the second row. Local feasibility is exactly `0<=e<=K_v`, with `0<=C_v<=H_v`.

## 2. Exact Winner Recurrence

For a parent with child capacities `A,B` and parent capacity `C`, set

```text
D = A+B-C >= 0
K = K_L+K_R-2D.
```

Actual metadata has `C<=A+B`. The recurrence rejects infeasible children or inconsistent local count domains.

### 2.1 Additive Capacity: D=0

When the first row attains `C=A+B`, it must attain both child maxima. When the second row attains the parent maximum, it must attain both child maxima instead. Consequently,

```text
P(t) = max_{s+r=t} [P_L(s)+P_R(r)]
Q(t) = max_{s+r=t} [Q_L(s)+Q_R(r)].
```

Only feasible child arguments participate. This is precisely max-plus convolution. Tied child orientations are already covered; no separate mixed case is needed.

### 2.2 Strict Capacity: D>0

The two child maxima cannot belong to the same row, since that row would have size `A+B>C`. Suppose the first row attains the left maximum. Its right occupancy must be `C-A=B-D`, so the right child's size difference is exactly `D`. The left child's difference must then be `D+t`. Exchanging children gives the other branch:

```text
P(t) = max(
    P_L(D+t) + Q_R(D),
    Q_L(D)   + P_R(D+t)
)

Q(t) = max(
    Q_L(D+t) + P_R(D),
    P_L(D)   + Q_R(D+t)
).
```

For example, the fixed right intersection in the first branch is

```text
e_R = K_R-D = C-A-H_R+B,
```

which agrees with the proposed expression. The other child's intersection is the parent intersection minus this fixed value. Both branches still require feasible child states; nonnegative parent intersection alone is insufficient.

**Exactness.** Every compatible pair falls into one of these orientation cases. Conversely, child witnesses can be united because the child feature groups are disjoint. Their row sizes, intersection counts, and query overlaps add, and the displayed equations enforce the parent maximum. This proves both upper bounds and constructive attainment by induction.

At the root, `C=u`, `I=ell+u-H`, and `t=u-ell=K-I`. For source size `a`, including features outside the union, the complete compatible-pair envelope is

```text
max( P(t)/(a+u-P(t)), Q(t)/(a+ell-Q(t)) ).
```

Return zero for a zero-overlap branch before division. This is not the actual stored pair's necessarily attained score.

## 3. Feasible-Domain Theorem

**Theorem.** For any feasible subtree, both orientations have the same gap domain `[0,T_v]`, with no missing integers. Hence the intersection domain is `[K_v-T_v,K_v]`.

The equality of orientation domains also follows from exchanging row labels. The endpoint recurrence is

```text
leaf:       T_v = K_v
D=0:        T_v = T_L+T_R
D>0:        require D <= min(T_L,T_R)
            T_v = max(T_L,T_R)-D.
```

If the requirement fails, the parent is infeasible. For arbitrary input summaries, also validate the leaf populations/capacities and `D>=0`.

**Proof.** At a leaf every permitted gap is constructible. At an additive node, sums of child intervals fill `[0,T_L+T_R]`. At a strict node, the two branches have domains `[0,T_L-D]` and `[0,T_R-D]`, provided both fixed child arguments `D` are feasible. Their union is the stated interval. If either child cannot reach `D`, neither branch is possible. The bound `T_v<=K_v` follows inductively; at a strict node it uses `D<=min(T_L,T_R)<=min(K_L,K_R)`. This also proves that the maximum feasible intersection is always `K_v`.

Computing all domains takes `O(G)` word operations and `O(G)` stored words for a tree with `O(G)` nodes. It does **not** compute the query-value frontiers.

If an evaluator restricts every subtree intersection to the known global `I`, intersect the local interval with `[0,I]`. The truncated interval need not end at `K_v`; it can be empty. Do not apply the untruncated endpoint formula to previously truncated child gap intervals without accounting for their changed lower endpoints.

## 4. Four-Feature Counterexample

Use leaf groups `L={a}` and `R={b,c,d}` and actual rows

```text
T1 = {a}
T2 = {b,c,d}.

(H_L,C_L) = (1,1)
(H_R,C_R) = (3,3)
(H,C)     = (4,3)
D=1, K=2.
```

For query `{a}`, the first row of size three can contain `a` only if the other row contains all three right-group features. Its size must therefore also be three, forcing `e=2`. The frontier is

| Intersection e | 0 | 1 | 2 |
|---|---:|---:|---:|
| M(e), query `{a}` | 0 | 0 | 1 |
| m(e), query `{a}` | 1 | 1 | 1 |
| M(e), query `{b,c,d}` | 3 | 3 | 3 |
| m(e), query `{b,c,d}` | 0 | 1 | 3 |

For the last row, at `e=0` the smaller row is `{a}`; at `e=1` it can contain `a` and one right feature; at `e=2` it can be the entire right group while the other row contains `a` and two right features. The increments are `1,2`, disproving both concavity and the proposed zero/one slope restriction.

These are frontiers for fixed local union/capacity metadata. Varying `e` varies the smaller row's size. The displayed generating pair has root sizes one and three and selects `e=0`; it does not license evaluating all three intersections under those same fixed root extrema. The counterexample applies to the subtree frontier proposed in the question. The next construction also fixes root extrema and the final score.

## 5. Covering Gadget and Frontier Size

For positive integers `d,w`, make a two-leaf subtree:

```text
left:   H=C=q=d
right:  H=C=d+w, q=0
parent: H=2d+w, C=d+w, K=w, D=d.
```

All the query features are in the left group. Direct substitution in the strict recurrence gives

```text
P(0)=d;  P(t)=0 for 1<=t<=w
Q(t)=d  for 0<=t<=w.
```

At zero gap, both rows have size `d+w`, and the first may contain the left group plus `w` right features. At positive gap, a first row attaining the parent maximum must be the entire right group; a first-row maximum in the left group would force an impossible left-child gap greater than `d`. Thus a positive gap anywhere in this gadget incurs the fixed query-overlap loss `d`, while supplying up to `w` units of gap.

Join `r` such gadgets using additive-capacity nodes. Let `Dsum=sum_i d_i`, `W=sum_i w_i`. At total gap `t`,

```text
P(t) = Dsum - cover(t)
Q(t) = Dsum

cover(t) = min { sum_{i in J} d_i : sum_{i in J} w_i >= t }.
```

Here `cover(0)=0`. For a fixed activated set of gadgets, its gap can be distributed integrally up to its total capacity. Equivalently, minimizing a fixed positive charge over gadgets with positive allocated gap gives the covering formula: a minimum-cost covering set is inclusion-minimal, and such a set has at most `t` members when `t>0`, so each activated gadget can receive at least one unit. Any unused selected gadget can simply be removed. This establishes both directions rather than treating fractional activation as legitimate.

### 5.1 Exponentially Many Affine Pieces

Choose `d_i=w_i=3^i`, for `i=0,...,r-1`. Then `cover(t)` is the smallest subset sum at least `t`. There are `2^r` distinct subset sums. Because the smallest weight is one and all others are multiples of three, sorted subset sums occur in adjacent pairs `(s,s+1)`, separated from the next pair by a gap of at least two.

Consequently `cover(t)` has `2^(r-1)-1` distinct nonterminal flat stretches containing at least two consecutive integer arguments, separated by nonzero changes. One affine piece cannot contain two such flat stretches with different values. Thus any ordinary piecewise-affine representation over contiguous integer intervals needs `Omega(2^r)` pieces. Reversing the axis from `t` to `e` does not remove this obstruction.

For the small example `w=d=(1,3,9)`,

```text
t:       0  1  2  3  4  5  6  7  8  9 10 11 12 13
cover:   0  1  3  3  4  9  9  9  9  9 10 12 12 13
P(t):   13 12 10 10  9  4  4  4  4  4  3  1  1  0
```

There are only `2r` leaves. A small expression DAG still describes the recurrence; the result rules out a polynomial bound on explicit affine pieces, not every possible implicit representation.

## 6. Hardness of the Final Exact Envelope

The preceding gadget already makes evaluation of a single winner frontier SUBSET-SUM-hard in binary-encoded counts. Its smaller-row score could nevertheless hide that difficulty in the final maximum. The following extra leaf prevents that escape.

Take positive SUBSET SUM weights `w_1,...,w_r`, total `W`, and target `b` with `1<=b<=W`. Set `d_i=w_i` in the gadgets above. Their aggregate has

```text
H_base=3W, C_base=2W, K_base=W, q_base=W
P_base(t)=W-cover(t), Q_base(t)=W,
cover(t)=minimum subset sum >= t.
```

Add an anchor leaf with `H=C=q=4W`, and an additive-capacity parent. Fix the complete pair's root sizes and source size to

```text
H=7W, u=6W, ell=2W-b, I=W-b, a=5W
root gap = u-ell = 4W+b.
```

The query consists of every gadget's left features plus every anchor feature. There are no source features outside the union in this construction.

### 6.1 Actual Metadata Witness

Choose any integers `0<=t_i<=w_i` with `sum_i t_i=b`; a greedy allocation suffices. Make the first actual row contain every right-group feature and every anchor feature. Make the second contain every left-group feature and, in right group `i`, any `w_i-t_i` features. It contains no anchor features.

The first row has size `6W`; the second has size `2W-b`. Their union has size `7W`, intersection `W-b`, and every specified leaf, gadget, aggregate, and root maximum is attained. Producing the count summary needs no solution of SUBSET SUM. The reduction therefore uses compatible actual metadata, not an unrealizable rectangle of bounds.

### 6.2 Exact Value and Decision Threshold

The anchor can absorb at most `4W` gap without lowering its winner overlap, so the base must receive at least gap `b`. Since `P_base(t)` is nonincreasing, the winner optimum uses base gap `b`:

```text
P_root(4W+b) = 5W-cover(b).
```

For the smaller-row orientation, `Q_base` is constant, and allocating the maximum base gap `W` minimizes the anchor gap. This gives

```text
Q_root(4W+b) = 2W-b = ell.
```

Their Jaccard values are therefore

```text
J_large = (5W-cover(b))/(6W+cover(b)) >= 4/7
J_small = (2W-b)/(5W) <= 2/5.
```

The large-row branch strictly wins. Since `cover(b)>=b`, the exact envelope satisfies

```text
E >= (5W-b)/(6W+b)
    if and only if cover(b)=b
    if and only if some subset of the weights sums to b.
```

All metadata, queries-as-histograms, and thresholds have polynomial binary encoding length in the SUBSET SUM input. There are `O(r)` tree nodes, and all numbers are `O(W)`. Trees can be balanced and padded with empty feature groups when a fixed power-of-two leaf layout is required; this changes the number of groups by only a constant factor.

**Scope of the hardness statement.** This establishes a numerical, SUBSET-SUM-type obstruction for an evaluator whose input is the already-paid counts. A general count-only evaluator polynomial in `G` and count bit length would imply `P=NP`. In particular, an unrestricted `O(G)` or `O(G^2)` word-RAM evaluator on `O(log H)`-bit counts would do so. The scalar pseudopolynomial DP remains consistent with this result. The proof does not establish strong NP-hardness, an `Omega(I^2)` lower bound, or a restriction on algorithms that spend time proportional to the expanded `H`, use substantial additional preprocessing, or handle a special metadata class. Explicitly listing the witness features can be exponentially larger than the binary weight input, so do not transfer the compressed-input claim unchanged to that input model.

## 7. Work and Matched Baseline

The interval-relaxation control from the prior review remains exact in `O(G)` after counts are available. Nothing here changes that control or licenses comparing against a size-convolution implementation of it.

For the exact pair evaluator, there are two orientation values per feasible intersection. Restricting to `e<=I` gives at most `2(I+1)` values per node. The winner recurrence sharpens the work accounting:

| Node or task | Straightforward exact work |
|---|---|
| All untruncated feasibility intervals | `O(G)` total |
| Leaf value array, after global intersection truncation | `O(I+1)` |
| Strict node, `D>0` | `O(I+1)`: two fixed-argument branches per state |
| Additive node, `D=0` | Up to `O((I+1)^2)` for max-plus convolution |
| General scalar evaluator | `O(G(I+1)^2)` work, `O(G(I+1))` retained state as an upper bound |

In the gap coordinate, the tighter untruncated work bound is the sum of `(T_L+1)(T_R+1)` over additive nodes plus linear frontier lengths at strict nodes. Do not convolve at strict nodes. This can improve the reference evaluator's constants or parameter-sensitive work, but it is not the requested polynomial-in-`G` compact theorem.

The reduction does not preclude a better pseudopolynomial algorithm, including removal of one capacity factor through a justified recursive evaluation strategy. The lead identifies a related heavy-light tree-automaton result and established piecewise-linear DP algebra; those sources were not re-searched here, and no transfer of their hypotheses to this disjunctive model is asserted. A general-purpose piecewise-linear representation alone does not resolve the exponential-piece family above.

## 8. Evidence and Precedent Boundary

The bounded independent checks compared the new recurrence against complete actual-pair families, not the lead's scalar implementation:

| Check | Domain | Result |
|---|---|---|
| Actual-pair frontier oracle | Four features, leaf groups `{a}` and `{b,c,d}`; all 256 ordered row pairs, grouped into 32 exact union/capacity summaries; all 16 queries | 512 summary/query cases; zero frontier or feasible-domain mismatches |
| Actual-pair frontier oracle | Six features, four leaves with populations at most `1,2,1,2`; all 4,096 ordered row pairs, 267 exact summaries; all 64 queries | 17,088 cases; zero frontier or feasible-domain mismatches |
| Covering and anchor construction | All 27 three-weight vectors in `{1,2,3}^3`, plus `(1,3,9)`; every target from zero through the total | 203 root cases; generic `(e,x)` child-state DP matched both root formulas, base winner values, and strict large-row dominance |

The pair oracles grouped by exact union and every attained maximum, allowing root size difference to vary as the frontier requires. The generic construction check used full local feasible states and direct child-state combination, independently of the winner recurrence; subset costs were computed by enumerating the eight subsets. These are finite corroboration, not the proofs of the interval theorem, the exponential family, or the reduction.

**Specific primary precedent.** The reduction led to one targeted precedent check: Karp's 1972 paper lists the equality-form 0-1 KNAPSACK problem among its complete problems, the SUBSET SUM source problem used here. This published fact is distinct from the gadget, anchor, and count-only-envelope reduction derived above. See [Karp, Reducibility Among Combinatorial Problems, original pp. 94-95, authorized reprint](https://www.cs.umd.edu/~gasarch/BLOGPAPERS/Karp.pdf). No literature priority search for the present theorem was performed.

## 9. Follow-Up: Exact Inverse Thresholds

**Status: the proposed theorem is valid with the domain conventions below.** This section adds a different compactness parameter; it does not withdraw the concavity counterexample or the magnitude-free limitation. No further literature search, repository test, or implementation inspection was performed for this follow-up.

### 9.1 Monotonicity Without Concavity

Fix a feasible subtree pair `(A,B)` where `|A|=C` and `|B|=H+e-C<=C`. The retained laminar upper capacities define a matroid on this subtree's union. Both rows are independent, and `A` has size `C`, equal to the root capacity. If `|B|<C`, matroid augmentation supplies a feature in `A minus B` that can be added to `B` while keeping it independent.

This operation preserves the union, increases the intersection by one, and cannot destroy any attained maximum: occupancies do not decrease, and independence prevents either row from exceeding any capacity. Repeating reaches `|B|=C`, hence intersection `K=2C-H`.

Holding `A` as the first row preserves its query overlap, proving that `Fmajor(e)=M(e)` is nondecreasing. Holding `A` as the second row extends the first row, proving that `Fminor(e)=m(e)` is nondecreasing. The same argument independently proves that the feasible domain is a tail interval ending at `K`.

This does not constrain optimal-value increments to zero or one. Different optimal witnesses can become available at successive intersections, as `m(e)=[0,1,3]` already demonstrates. Monotonicity, not concavity, is the needed property.

### 9.2 Threshold Definition and Leaves

Let `q_v=|S intersect U_v|`. For each orientation `R` and integer `0<=c<=q_v`, define

```text
tau_R[c] = min { e : the subtree has a compatible pair
                    with first-row query overlap at least c
                    in orientation R }.
```

An unattainable threshold is positive infinity. In particular, `tau_R[0]` is the smallest feasible intersection, not automatically zero. Extend threshold lookups beyond `q_v` by positive infinity; negative requested overlap is replaced with zero. Threshold arrays are nondecreasing in `c`.

For any `0<=e<=K`, monotonicity and augmentation give the exact inverse:

```text
F_R(e) = max { c : tau_R[c] <= e }.
```

If this set is empty, the state is infeasible. The forward implication is the definition of a threshold. For the reverse implication, a threshold witness at smaller intersection can be augmented to exactly `e` without reducing its first-row overlap. Thus minimum-intersection witnesses suffice even though the parent may require an exact intersection.

At a valid leaf, for `0<=c<=min(q_v,C_v)`,

```text
tau_major[c] = 0
tau_minor[c] = max(0,c-(H_v-C_v)).
```

All larger thresholds are infinite. The finite minor values are at most `K_v`. These formulas include `c=0`.

### 9.3 Additive Nodes

When `C=A+B`, the roles agree in both children. For either role independently,

```text
tau_R[c] = min_{c_L+c_R=c}
               (tau_R,L[c_L] + tau_R,R[c_R]),
```

where `0<=c_L<=q_L` and `0<=c_R<=q_R`. Infinite summands are rejected.

To prove necessity, split any achieved overlap of at least `c` into nonnegative requirements summing to exactly `c`, each no greater than its child's achieved overlap. The actual child intersections are at least their thresholds. For sufficiency, unite child threshold witnesses. They have the required roles, preserve every child maximum, attain the additive parent maximum, and achieve total overlap at least `c`. Their intersections add. This is ordinary min-plus convolution of profit-indexed thresholds; no concavity-based acceleration is being claimed.

### 9.4 Strict Nodes

Let

```text
d = A+B-C > 0
l = K_L-d
r = K_R-d
K_parent = l+r.
```

Reject any branch whose fixed child state is infeasible. For a valid branch, retrieve the fixed child's exact value using the inverse threshold formula above. Then the proposed recurrences are exactly

```text
tau_major[c] = min(
    r + tau_major,L[max(0,c-Fminor,R(r))], with tau_major,L[...] <= l,
    l + tau_major,R[max(0,c-Fminor,L(l))], with tau_major,R[...] <= r
)

tau_minor[c] = min(
    l + tau_minor,R[max(0,c-Fmajor,L(l))], with tau_minor,R[...] <= r,
    r + tau_minor,L[max(0,c-Fmajor,R(r))], with tau_minor,L[...] <= l
).
```

An invalid cap, an undefined fixed value, or an infinite residual threshold kills that branch. If neither branch remains, the parent threshold is infinite. Values `l,r` are intersection counts, unlike the child capacity symbols `A,B`.

For example, in the first major branch the right intersection is fixed at `r`. Maximizing its query overlap can only help the residual left requirement. The left minimum required intersection is its major threshold for that residual; the cap `<=l` is exactly the condition that the parent intersection not exceed `K_parent`. Compatible child witnesses unite, so the resulting sum is achievable. Conversely, every pair in this orientation has the right overlap at most `Fminor,R(r)` and must meet the corresponding residual threshold on the left. This proves that branch's exact minimum. The other three branches follow by exchanging children or roles.

**Zero threshold matters.** Take two leaves with `H=C=2` each and parent `H=4,C=3`. Then `d=1`, `l=r=1`, and the parent has feasible intersections `[1,2]`. Both parent zero thresholds are one. Hard-coding `tau[0]=0` at internal nodes would invent an infeasible state.

### 9.5 Root Recovery and State Cost

At the root, check that `I=ell+u-H` is feasible, then retrieve

```text
c_major = max { c : tau_major[c] <= I }
c_minor = max { c : tau_minor[c] <= I }.

E = max( c_major/(a+u-c_major),
         c_minor/(a+ell-c_minor) ).
```

Use the existing zero-overlap-as-zero convention before division. Since each orientation's row size is fixed, maximizing overlap maximizes its Jaccard value. The exactness is over complete compatible two-row datasets, not a promise that the actual stored rows attain the bound.

Let `G` be the number of leaves, `h` the tree height in edges, and `q=q_root`. Retaining two arrays of length `q_v+1` at each node uses

```text
O(sum_v (q_v+1)) = O(G+q(h+1)) words.
```

Each query/union feature contributes one to `q_v` at each of its ancestors, including its leaf. Thus a balanced tree gives `O(G+q log(G+1))` words. Writing `log(G+1)` or `1+log G` covers the single-leaf case; `q log G` alone would not cover its leaf array. On an unbalanced tree, retain the height-dependent bound rather than asserting `log G`.

### 9.6 Total Work Proof

Leaf initialization takes `O(q_v+1)`. At a strict node, the four fixed child values can be obtained by scanning their threshold arrays, followed by constant-time residual lookups for each parent `c`. This costs `O(1+q_L+q_R+q_v)=O(q_v+1)`. A separate search per parent threshold is unnecessary.

At an additive node, enumerate every child threshold-index pair once and update the corresponding parent sum for both roles. Its work is

```text
O((q_L+1)(q_R+1))
    = O(1+q_L+q_R+q_L*q_R).
```

The linear terms sum to `O(G+q(h+1))`. The quadratic terms satisfy the exact tree identity

```text
sum_{all internal v} q_L(v)*q_R(v)
    = (q^2 - sum_{leaves j} q_j^2)/2
    <= q^2/2.
```

This follows by telescoping `q_v^2=q_L^2+q_R^2+2q_L*q_R`. Equivalently, each pair of query/union features in different leaves is counted exactly once, at its lowest common ancestor. Features in the same leaf do not contribute. Summing over additive nodes only cannot increase the total.

Therefore the total evaluation cost is

```text
arbitrary binary tree: O(G+q(h+1)+q^2)
balanced binary tree:  O(G+q log(G+1)+q^2).
```

This is independent of the numerical magnitude of `I`. Threshold values and capacities still require integer bits; the statement counts word operations, not unbounded-precision arithmetic for free. Ground-union populations, query histograms, index access, source size `a`, and final exact rational comparisons remain paid. A query's features outside the union affect `a` but not these state counts.

### 9.7 What Changes, and What Does Not

Monotone integer-valued overlap frontiers have at most `q_v+1` relevant thresholds even if a single jump is large. This is the compact structure the earlier affine-frontier proposal missed. It supplies an exact algorithm parameterized by query/union overlap, replacing the scalar reference's dependence on `I` with a dependence on `q`.

There is no contradiction with Sections 5-6. In the exponential-piece family, `q` itself grows exponentially with the number of gadgets. In the final-envelope reduction, `q=a=5W`, so the threshold algorithm is pseudopolynomial in the binary SUBSET SUM input, not polynomial in its bit length. The earlier limitation never excluded such profit-indexed work. It also did not prove the reference's quadratic capacity factor necessary.

The matched independent interval control still costs `O(G)` after identical counts. The new exact-pair algorithm costs `O(G+q log(G+1)+q^2)`, not uniformly `O(G)`. Small `q` is a coherent target regime, but no speedup over that control, the scalar implementation, or native DAAT has been measured in this review. If choosing between the threshold and scalar evaluators, any dispatch rule and both paths need paid, non-oracle accounting.

Profit-indexed dynamic programming, inverse value thresholds, and min-plus convolution are established primitives, not claimed inventions. The potential mathematical contribution is the exact recovered-pair constraint theorem together with its query-overlap-parameterized realization and cost proof. Publication novelty and the transfer of the lead's related heavy-light result remain unestablished; neither is required for this proof.

### 9.8 Bounded Independent Verification

A fresh ephemeral check evaluated the threshold recurrence against complete actual-pair families, not against the lead's implementation. It reused the two small finite domains described in Section 8, with no repository tests or public timing:

| Domain | Summary/query cases | Result |
|---|---:|---|
| Four features; two leaves; all 256 ordered pairs, 32 exact summaries, all 16 queries | 512 | Every threshold and every inverse-recovered feasible/infeasible frontier value matched direct enumeration |
| Six features; four leaves; all 4,096 ordered pairs, 267 exact summaries, all 64 queries | 17,088 | Same result |

There were zero threshold, inverse-recovery, or frontier-monotonicity mismatches across the 17,600 cases. The oracle preserved exact union and all attained maxima and varied row sizes as needed to enumerate each frontier. It included zero queries, empty unions, unattainable positive thresholds, tied orientations, and both additive and strict nodes. These checks corroborate the proof; they do not establish integration correctness or runtime benefit. The lead's separate oracle results remain reported, not independently certified here.

## Final Assessment

Accept the new inverse-threshold theorem. It gives an exact complete-pair envelope in `O(G+q log(G+1)+q^2)` word operations and `O(G+q log(G+1))` words on balanced binary trees, after the common counts are paid. The concrete recurrence needs no concavity assumption, and its reconstruction at the exact global intersection follows from augmentation.

Retain all earlier counterexamples and limitations: explicit affine frontiers can be exponentially complicated in `G`, and a magnitude-free polynomial-in-`G` evaluator would encounter the compressed-count SUBSET SUM obstruction. The threshold algorithm avoids that claim by being polynomial in the numerical query overlap `q`.

Practical performance, integration correctness, and publication novelty remain unverified. The main agent retains ownership of implementation, tests, public receipts, and manuscript decisions. Main goal7algorithms remains open.

## 10. Closing Follow-Up: Threshold Theorem and Reported Checks

**Verdict: accept the theorem as a pure metadata optimization result.** The augmentation proof, inverse-threshold recovery, additive min-plus recurrence, and capped strict-node branches in Section 9 are sufficient. No new falsifier was identified in this closing review. The nonconcavity counterexamples and compressed-count hardness qualification remain unchanged.

Use the following general statement, including the single-leaf case. Let the binary feature tree have `G` leaves, `O(G)` nodes, height `h` in edges, and `Q=sum_leaf q_leaf=|S intersect U|`. Then, after the common metadata and query counts are available,

```text
time:        O(G + Q(h+1) + Q^2) word operations
table cells: O(G + Q(h+1)).
```

Two full orientation tables contain exactly `2*sum_v(q_v+1)` cells under the stated representation. Each counted query feature appears at most `h+1` times in that sum. The convolution charge follows from `sum_internal q_L*q_R=(Q^2-sum_leaf q_leaf^2)/2`. These arguments require neither concavity nor iteration over the numerical intersection `I`. When `G=1`, `h=0`, and the leaf still has `O(Q+1)` table cells; dropping the `+1` would be incorrect. For balanced trees, `h+1=O(log(G+1))` recovers the earlier specialization. Features outside the union affect source size `a` and Jaccard denominators, not `Q`.

### Preflight Contract

The lead reports that a preflight rejects excessive `Q` before allocating threshold arrays. This is compatible with the theorem: it limits the admitted inputs rather than producing an envelope on rejected inputs. The guard must precede any `Q`-sized table or scratch allocation; computing and validating the count total and reading metadata remain paid. Rejection is neither a zero bound nor permission to prune a block. Any future exact-search integration must propagate rejection or use a separately justified fallback. This review has not inspected that guard or its allocation behavior.

### Lead-Reported Evidence

The lead reports a separate threshold implementation and eight passing tests covering:

| Reported check | Scope |
|---|---|
| Actual-summary oracle | 29,440 compatible-summary/query envelopes |
| Deeper-tree comparison | 1,024 `G=8` dense-reference versus threshold comparisons |
| Nonconcave frontiers | Explicit nonconcave arrays retained |
| Cover-and-anchor construction | 25 root instances |
| Symbolic shared population | `N=1,16,1000,10^12`, each with 38 table cells and 70 scheduled operations |

These are lead-reported results, not independently rerun or audited here. They are distinct from this review's own finite checks in Section 9.8. The symbolic example is consistent with count-magnitude-independent state and transition schedules at fixed query histogram and tree shape. It is not evidence of constant physical memory or runtime: integer bit widths, object representation, allocation, arithmetic, and input access still matter, and the scheduled-operation counter's definition has not been audited.

**Closure.** The current proof/falsifier review is complete. The accepted claim is exact recovery from the specified two-row metadata with the stated `Q`- and height-dependent cost. It is not a physical-memory, speedup, implementation-certification, priority, or paper-readiness claim. No additional enumeration, repository tests, benchmarks, or literature search were performed for this closing follow-up. Only this memo was changed; main goal7algorithms remains open.

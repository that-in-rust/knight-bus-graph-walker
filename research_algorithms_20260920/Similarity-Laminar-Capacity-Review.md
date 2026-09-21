# Similarity Laminar Capacity Review

Date: 2026-09-20. Independent, narrow mathematical and primary-source review.

Scope: the proposed fixed-block, query-independent certificate and its exact binary-set Jaccard row relaxation. No probe implementation, repository test execution, index editing, benchmark, commit, or push was performed. The only written artifact is this memo. **Main goal7algorithms remains open.**

## 1. Premise Check

**Verdict: accept the row-relaxation theorem, with an explicit empty-set convention; reject whole-block tightness and a new rank-primitive claim. Scientific novelty of the full composition is not established.**

- The proposed envelope is the exact maximum over individually admissible rows. The augmentation argument is valid, and actually works for any matroid with sufficient total rank.
- Exact union, fixed population, and jointly attained capacity maxima constrain whole datasets more strongly than they constrain one row. An eight-feature counterexample gives row envelope `1` but whole-block attainable maximum `3/5`.
- Nested capacities can strictly improve on both union-plus-length and leaf-cap bounds, even on the best bound from every uniform tree depth. However, the nested calculation is exactly an ordinary minimum-cost tree-cut DP on the same paid summaries.
- Existing sources already supply group-union pruning, group cardinality/norm summaries, feature-count Jaccard bounds, and laminar matroids. They do not, in the inspected passages, explicitly state this complete exact-union/length/laminar-capacity row-envelope contract.

The relevant lenses are combinatorial optimization, exact retrieval correctness, information lost by summaries, and physical query costs. The adversarial question is not whether another implementation can copy this one, but which statements follow directly from published results and which additional results remain to be established.

## 2. Candidate Approaches

Write `q = |S intersect U|`, `a = |S|`, and, for `a > 0`, define

```text
F(x) = x / (a + max(ell, x) - x),   F(0) = 0.
```

Let `q_j = |S intersect U intersect leaf_j|` and `C_j` be its capacity.

| Comparator | Overlap bound | Jaccard bound | Information used |
|---|---|---|---|
| Union-only | `q` | `q/a` | Exact union |
| Uniform union-plus-length | `r0 = min(q,u)` | `F(r0)` | Union and both length endpoints |
| Leaf partition plus root | `rP = min(u, sum_j min(q_j,C_j))` | `F(rP)` | Above plus leaf capacities |
| Full laminar hierarchy | `r` from the tree recurrence | `F(r)` | Above plus internal capacities |
| Adaptive tree-cut control | Minimum cut sum defined below | Exactly `F(r)` | The same complete hierarchy |

The second and third rows are exact for their respective **individual-row** relaxations, by the same theorem below. They are the necessary strengthened controls; beating only `q/a` is insufficient.

These bounds satisfy

```text
max_{T in B} J(S,T) <= F(r) <= F(rP) <= F(r0) <= q/a.
```

The uniform bound jointly optimizes overlap and target length. It is stronger than merely taking the minimum of `q/a` and a separate length-only bound. For example, `a=4, q=2, ell=u=6` gives `F(r0)=1/4`; the separate minimum is `min(1/2,4/6)=1/2`.

## 3. Chosen Thesis: Exact Row Relaxation

### 3.1 Definitions and Assumptions

Assume a finite, nonempty block `B` of records with unique original IDs. Record contents are binary sets; unique IDs do not imply that contents must differ. Define the exact values

```text
U      = union_{T in B} T
ell    = min_{T in B} |T|
u      = max_{T in B} |T|
C_v    = max_{T in B} |T intersect H_v|.
```

The fixed feature hierarchy partitions `U` at its leaves. Each internal group is the disjoint union of its children, and the root is `U`, with `C_root=u`. Intersecting global groups with `U` preserves laminarity. Hash buckets are acceptable groups, but their feature counts must count distinct original features, not just occupied buckets.

Define

```text
Independent(M) = {T subset U : |T intersect H_v| <= C_v for every v}
R              = {T in Independent(M) : |T| >= ell}.
```

The upper-capacity system is the matroid; `R`, with its lower length restriction, generally is not. The root already enforces `|T| <= u`.

Every actual row is independent. A maximum-size actual row has size `u`, whereas the root forbids larger independent sets. Consequently `rank_M(U)=u`. This equality uses the existence of the actual row, not merely the label `C_root=u` on an arbitrary capacity table. In fact an actual witness for each capacity also gives `rank_M(H_v)=C_v`.

### 3.2 Theorem and Proof

For `a > 0`, let `r = rank_M(S intersect U)`. Then

```text
max_{T in R} J(S,T) = r / (a + max(ell,r) - r).
```

**Upper bound.** For an admissible row, put `c=|S intersect T|` and `b=|T|`. Its overlap is an independent subset of `S intersect U`, so `c <= r`. Also `b >= max(ell,c)`. Thus

```text
J(S,T) = c/(a+b-c) <= F(c) <= F(r).
```

The last inequality follows because `F(c)=c/(a+ell-c)` for `c <= ell`, and `F(c)=c/a` for `c >= ell`; these branches are increasing and agree at the boundary.

**Attainment.** Choose an independent `I subset S intersect U` with `|I|=r`. Put `b*=max(ell,r)`. Since `b* <= u=rank_M(U)`, matroid augmentation extends `I` to an independent `T*` of size `b*`. No added element can belong to `S`: otherwise the independent superset would contain an independent subset of `S intersect U` of size `r+1`. Therefore `|T* intersect S|=r`, and `J(S,T*)=F(r)`.

This is an existence proof, not a requirement to construct `I` or `T*` during query execution. It needs neither pairwise target overlaps nor an overlap with any endpoint row of the block. Laminarity provides the cheap rank evaluation; the attainment theorem itself only needs a matroid and `rank(U) >= ell`.

### 3.3 Empty Sets and Outside Tokens

The unconditional instruction "zero if `r=0`" needs a score convention. If `J(empty,empty)=1`, then `S=empty` and a block containing an empty row give envelope `1`, not `0`. Exact `ell=0` certifies that such a row exists. With that convention, handle `a=0` separately: the envelope is `1` when `ell=0`, otherwise `0`. If empty-empty is defined as zero, the proposed zero branch is consistent. If that score is undefined, exclude or specify this case explicitly.

Features outside `U` remain in `a`. For `S={p,z}`, `U={p}`, `ell=u=1`, and `z` outside `U`, the exact envelope is `1/2`. Replacing `a` by `q` gives the weaker value `1`, not the stated exact envelope.

## 4. Exact Limitations and Separating Examples

### 4.1 Whole-Block Tightness Is False

Use a root with two leaf groups:

```text
A = {a1,a2,a3,a4},  D = {d1,d2,d3,d4}
T1, ID 10 = {a1,a2,a3,d1}
T2, ID 20 = {a4,d2,d3,d4}
S         = {a1,a2,d1,d2}.
```

The certificate has population `2`, exact union `A union D`, `ell=u=4`, `C_A=C_D=3`, and root capacity `4`. The row `S` is individually admissible: its leaf counts are `(2,2)`. Hence `r=4` and the row envelope is `1`.

Now require a whole two-row block to preserve this certificate. Two size-four rows covering eight features must be disjoint complements. If one row were `S`, the other would also have counts `(2,2)`. Neither capacity maximum would attain `3`. Thus no compatible block can contain a score-one row. Every row has size four, so any other row has overlap at most three and score at most `3/5`. The displayed `T1` has overlap three, proving that `3/5` is the exact whole-block optimum, including with the original ID set fixed.

This disproves "tight for every observed block" and even the weaker claim "tight over all datasets preserving the complete certificate." It does **not** affect pruning safety: every real row is still in `R`.

If population and the ID set were unrestricted, appending the relaxed maximizer to the original block would preserve union, length extrema, and every attained capacity maximum. That simple argument is unavailable at fixed population. The quantifier change is substantive.

Additional population information can imply per-row lower counts, for example

```text
|T intersect H_v| >= |U intersect H_v| - (|B|-1) C_v.
```

The other rows must cover the features left uncovered by `T`. These necessary constraints still do not capture all jointly attained-maxima obligations, as the example shows.

### 4.2 Leaf Caps Strictly Improve the Strong Uniform Bound

Let leaves be `A={a1,a2}` and `D={d1,d2,d3,d4}`, and rows be `{a1,d1,d2}` and `{a2,d3,d4}`. For query `S=A`, `ell=u=3`, `q=2`, and leaf capacities are `(1,2)`.

The uniform envelope is `2/3`; the leaf-cap envelope is `1/4`, which is attained by either actual row. This is a real separation from union-plus-length, not just from union-only pruning.

### 4.3 Mixed-Depth Cuts Beat Every Uniform Depth

Use four leaves, with sibling pairs `(L0,L1)` and `(L2,L3)`:

```text
L0 = {a1,a2}         L1 = {b1,b2}
L2 = {c1,c2,c3,c4}   L3 = {d1,d2}

T1 = {a1,a2,c1,c2,d1,d2}
T2 = {b1,b2,c3,c4,d1,d2}
S  = {a1,a2,b1,b2,c1,c2,c3,c4}.
```

Here `a=8`, `ell=u=6`; leaf capacities are `(2,2,2,2)`, parent capacities are `(2,4)`, and query counts are `(2,2,4,0)`.

| Cut | Overlap bound |
|---|---:|
| Root | 6 |
| Both parents | `2+4=6` |
| All leaves | `2+2+2+0=6` |
| Left parent and right leaves | `2+2+0=4` |

Thus uniform, leaf, and best-uniform-depth envelopes are `3/4`; the adaptive laminar envelope is `4/(8+6-4)=2/5`, attained by both rows. These labeled examples can be encoded using distinct integers in the required modulo buckets without changing the algebra.

### 4.4 Important Non-Dominance and Degeneracies

- **Finer leaves do not recover arbitrary co-occurrence.** With singleton leaves `a,b,c,d`, parents `{a,b}` and `{c,d}`, and rows `{a,c}` and `{b,d}`, query `{a,d}` has envelope `1` but actual maximum `1/3`. The alternate block `{a,d}`, `{b,c}` has the same union, lengths, and capacities. The summary cannot distinguish them.
- **Lower-count summaries can be stronger.** With leaves `A={a1,a2}`, `D={d1,d2,d3}`, rows `{a1,a2}` and `{a1,d1,d2,d3}`, and query `D`, the upper-capacity envelope is `1`: `ell=2`, `u=4`, and `r=3`. A certificate also recording minimum `A` count `1` forces a nonquery feature and gives `3/4`, attained by the second row. Therefore do not claim universal dominance over a full lower/upper-count rectangle. Its extra information and cost must be counted.
- **Parent maxima are not sums of child maxima.** In Section 4.3 the left parent has capacity `2`, but its child maxima sum to `4`. Summing stored child maxima loses the correlation that makes the parent useful. Compute the parent count within each original row before maximizing across rows. Exact parent values cannot generally be reconstructed from child maxima alone.
- **More caps can be redundant.** A parent constraint implied by its children adds no pruning. If every query-visible feature combination already fits the relevant capacities, the rank remains `min(q,u)`. Modulo grouping is sound but has no distribution-free promise of useful correlations or pruning.
- **Nonlaminar additions need a new analysis.** Overlapping groups `{x,y}` and `{x,z}`, each capped at one, allow `{x}` and `{y,z}` but no augmentation of `{x}` to size two. These are even realized by those two rows with root cap two. Independent hierarchies can each provide safe bounds whose minimum is safe; jointly intersecting their constraints does not automatically preserve the matroid attainment proof.

## 5. Evidence: Published Results Versus Adaptation

### 5.1 LES3: Group Unions and Hierarchical Pruning

**Direct published content.** Li, Yu, and Koudas, *LES3: Learning-based Exact Set Similarity Search*, PVLDB 14(11), 2073-2086 (2021), Section 3.1, Equations (1)-(2), stores token presence across each target group and uses `|S intersect U|/|S|` as its Jaccard upper bound. Section 5.2 defines HTGM: pruning a parent target group eliminates verification in its descendant groups. See the [publisher paper](https://www.vldb.org/pvldb/vol14/p2073-li.pdf) and the accessible [author preprint, Sections 3.1 and 5.2](https://arxiv.org/pdf/2107.10417).

**Boundary.** This directly anticipates pre-verification union certificates and hierarchical **target-group** pruning. It is not the same hierarchy as nested **feature-group** capacities inside one fixed block. The strengthened length-aware envelope and the exact matroid row-relaxation theorem above are our adaptation, not quotations of LES3. The proposed physical boundary does not by itself distinguish the work from this source.

### 5.2 Fife/Oxley: The Matroid and Its Operations

**Direct published content.** Fife and Oxley explicitly identify laminar cardinality-upper-bound systems as matroids in the introduction. Theorem 1.5 characterizes their construction using coloops, truncation, and direct sums; Section 2 discusses redundant constraints. The inspected primary text is the [2016 author preprint](https://arxiv.org/pdf/1606.08354), introduction, Theorem 1.5, and Lemmas 2.2-2.3. The requested [revised LSU manuscript](https://www.math.lsu.edu/~oxley/LaminarMatroids_revised.pdf) could not be reliably retrieved; no version-specific assertion is based on unseen text.

**Adaptation.** A leaf is a uniform matroid, children combine by direct sum, and a parent truncates to its capacity. Their rank formulas give exactly the proposed recurrence. Applying augmentation to the lower-length Jaccard objective yields Section 3's application lemma. Neither the independence system nor the underlying rank operation is new.

### 5.3 Low/Zheng: The Capacity Is an Existing Group Norm

**Direct published content.** Low and Zheng, *Fast Top-K Similarity Queries Via Matrix Compression*, CIKM 2012, Section 2.1/Theorem 2.1 and Algorithms 2-4, bounds candidate groups using mixed norms and hierarchical refinement. Their [conference paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/short_topk.pdf) specifies a fixed norm-pair implementation; the [technical report, Lemma 2.1 and Section 2.3](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/08/topk.pdf) gives binary endpoint-norm optimality and group norms.

**Explicit adaptation.** Orient the binary matrix with features as rows and targets as columns. On feature group `H_v` and block `B`, `C_v=L^c_{1,infinity}(A_v)`. For query support restricted to `U`, the two endpoint inequalities give `C_v` and `q_v` when active, hence `min(C_v,q_v)`, with zero for an inactive group. This derives the local bound from published primitives, not from a hypothetical competitor copying code. Query-adaptive mixing, exact union restriction, Jaccard normalization, and row-tightness are additional composition steps; do not attribute the complete composition to HComp.

### 5.4 Closer Set-Search Art: Feature-Count Rectangles

**Direct published content.** Zhang, Wu, Wang, and Xing, *A Transformation-based Framework for KNN Set Similarity Search*, constructs token-group count vectors, bounds intersection by sums of coordinatewise minima, indexes them in an R-tree, and derives query-node Jaccard bounds from count rectangles. Its multiple-transformation rule takes the best whole-transformation bound. Inspect the [author full manuscript, Sections 4.1, 5.1, and 5.2; Equations (1), (3)-(5), and (8)](https://www.jinwang18.net/files/tkde19-setknn.pdf). The [published ICDE 2020 extended abstract](https://conferences.computer.org/icde/2020/pdfs/ICDE2020-5acyuqhpJ6L9P042wmjY1p/290300c040/290300c040.pdf), pp. 2040-2041, independently confirms the framework.

**Boundary.** This is closer precedent for leaf-count Jaccard pruning than union-only LES3. Upper rectangle endpoints contain leaf maxima; lower endpoints can add information missing here. The inspected procedures do not state the all-cuts laminar optimizer or its attainment theorem. Exact-union restriction plus a root length range is our strengthened control, not a verbatim published formula. Section 4.4 prevents a blanket dominance claim against richer rectangles.

### 5.5 Exact Equivalence to an Ordinary Adaptive Cut

The following is a derivation for this review, not a claim that the preceding retrieval papers print this exact DP.

Let `q_v=|S intersect U intersect H_v|` and `w_v=min(C_v,q_v)`. A cut is an antichain of tree nodes that covers every leaf exactly once. Any admissible overlap is at most

```text
sum_{v in cut} w_v.
```

Choose at each node either that node or cuts of all its child subtrees:

```text
D_leaf = min(C_leaf,q_leaf)
D_v    = min(w_v, sum_child D_child)
       = min(C_v, sum_child D_child).
```

The second equality uses `sum_child D_child <= sum_child q_child = q_v`. These are precisely the rank recurrence and its leaf initialization. Therefore

```text
rank_M(S intersect U) = min_{all tree cuts} sum_{v in cut} min(C_v,q_v).
```

This proves equality for every query, not merely equality on selected examples. The same `O(G)` postorder traversal computes both. Section 4.3 shows why a control restricted to one global depth is weaker. A query-adaptive cut can mix depths independently in different branches.

**Novelty classification:** the matroid construction is published prior art; the recurrence is an ordinary instantiation of direct sum and truncation; the cut equivalence is the explicit reduction above. An independently written equal-information DP is a correctness/control result, not proof that a particular retrieval paper published the complete certificate design.

## 6. Matched Costs and Pruning Contract

### 6.1 Costs That Must Be Charged

Let `n=|B|`, `L=sum_{T in B}|T|`, `m=|U|`, `G` be the number of leaves, and `h=log2 G` for a balanced tree. Let `I_count(S,B)` be the actual cost of obtaining the scalar source/union overlap, and `I_hist(S,B)` the cost of obtaining its per-leaf counts. These costs need not be equal.

| Work or state | Union-plus-length | Leaf caps | Full hierarchy/adaptive cut |
|---|---|---|---|
| Source/union intersection | `I_count(S,B)` | `I_hist(S,B)` | `I_hist(S,B)` |
| Bound work after counts | `O(1)` | `O(G)` dense | `O(G)` dense |
| Capacity state, besides length fields | None | `G` leaf integers | `2G-1` node integers including root |
| Preparation, besides union construction | Length extrema while scanning rows | Leaf counts/maxima in `O(L+n+G)` using touched buckets | Simple dense `O(L+nG)` or sparse ancestor accumulation `O(n+G+L(1+h))` |
| Surviving block | Read/score actual candidates | Same obligations | Same obligations |

The hierarchy does not store every group union separately: a global feature map and one exact block union suffice. A full packed capacity table needs up to `(2G-1) ceil(log2(u+1))` bits before headers/alignment, with the root reusable as `u`. Count the union representation, `ell`, original-ID minimum, offsets, directory state, and query buffers as well. Zero capacities and empty blocks may need encoding/header treatment.

Union cost is representation-dependent: sorted-list intersection can cost `O(a+m)`, a hash-backed union gives expected `O(a)` membership probes, and a dense bitset intersection touches `O(D/w)` words for feature-domain size `D`. List/hash matches can increment leaf counts while being visited. A scalar bitmap popcount does not automatically provide those counts: enumerating its matched bits adds up to `O(q)` work, while separate bucket-mask scans or a bucket-organized representation have their own costs. Query bitmap construction, decompression, and cache misses are not free. Do not report only the subsequent rank arithmetic.

After exact leaf counts are available, sparse evaluation costs `O(1+min(G,t(1+h)))` for `t` active leaves, provided clearing/initialization is also sparse. It is not generally an `O(log G)` query. A global query histogram can be reused, but its counts are not the block-restricted `q_j` unless membership in `U` has been accounted for. Unrestricted counts give a safe but potentially weaker bound.

Parent **feature** capacities must be derived from per-row counts as noted above. Merging separate **target** blocks instead takes nodewise maxima and unions; these two aggregation axes must not be confused.

Use identical block membership, feature map, query semantics, arithmetic, threshold trajectory, and paid union representation when measuring incremental bound strength. Distinguish this controlled comparison from a full published system allowed to choose its own grouping and index layout. A stronger count-rectangle control pays for its lower endpoints; an adaptive-cut control receives exactly the same internal caps and union accesses.

No bound guarantees a positive speedup: if all blocks survive, it adds metadata reads and arithmetic before the original scoring work. Gains must exceed union reads, capacity traffic, scheduling overhead, and amortized preparation. A reduction in row/posting reads can be real even if total bytes or runtime increase. All are separate measurements, none supplied by this review.

### 6.2 Safe Top-k Elimination

Assume results are ordered by descending exact Jaccard score and then ascending original ID. Once `k` eligible exact results exist, let the kth key be `(tau,id_k)` and let `id_min` be a valid lower bound on IDs in the unvisited block.

- Prune when `E=F(r) < tau`.
- Also prune when `E=tau` and `id_min > id_k`.
- Otherwise this certificate alone does not authorize pruning.

Equality without the ID condition is unsafe. For query `{p,q}`, an existing score-`1/2` result with ID `20` loses its tie to an unvisited `{p}` row with ID `5`. The latter block's exact envelope is also `1/2`.

The ID minimum need not belong to a row attaining the score bound: the combined key is still conservative, but need not be tight. Self-exclusion or other eligibility rules can make the relaxation looser. A minimum over all block rows remains a safe, possibly less useful ID certificate.

Represent `E=N/D` and `tau=P/Q` with positive denominators and compare `N*Q` with `P*D` using overflow-safe exact arithmetic. Handle empty-set cases before division. When fewer than `k` eligible exact rows have been found, do not discard score-zero blocks merely because `r=0`; they may be needed to fill the result and resolve zero-score ties. If fewer than `k` eligible rows exist globally, the output contract must say what to return.

The certificate can justify skipping a block's rows/postings **before accessing them for this query**. Its construction previously read the underlying rows and is paid. Source/union intersection is also paid. Computing source-to-endpoint overlaps first would change the proposed pre-field access contract and cannot be hidden in certificate evaluation. Previously visited blocks may legitimately supply the exact kth threshold; metadata-only optimistic scores may not.

The fixed-file snapshot is an assumption. An underestimated cap, incomplete union, excessive lower length bound, or invalid ID minimum can make pruning unsound. Conservative stale supersets/upper bounds may remain safe but lose the exactness theorem. This memo does not certify a file parser, persistence format, or update implementation.

## 7. Bounded Independent Verification

An ephemeral, in-memory JavaScript enumeration was run through the orchestration tool, with no source file written and no repository code or test runner invoked. It is a finite algebra check, not a performance experiment or the lead's probe.

**Exhaustive six-feature check:** encode features `0..5` by bits. Use node masks `[63,15,48,3,12,16,32]`: root `63`, children `15,48`, and leaves `3,12,16,32`. Enumerate all unordered pairs of row masks `0 <= x <= y < 64`, allowing identical contents under distinct IDs. Derive exact union, lengths, and every capacity from each pair. For each block enumerate all query masks `0..127`, with bit `6` an outside-universe feature.

For each certificate, enumerate all `T subset U` satisfying the upper capacities. Compare the tree rank with their maximum query overlap, enumerate the size-eligible rows' maximum exact Jaccard ratio, and check for a witness with size `max(ell,r)` and overlap `r`. Separately enumerate all five tree cuts: root; both parents; either parent with the opposite leaves; all leaves. Compare their minimum to the recurrence and check `r <= rP <= r0`. Fractions were compared by integer cross-products; empty-empty was explicitly scored zero for this check.

```text
Unordered two-ID blocks:          2,080
Distinct certificates:             707
Block/query cases:             266,240
Required extension witnesses:  266,240 found
Rank/envelope/cut/order failures:     0
```

**Whole-block counterexample check:** enumerate the 70 size-four subsets of eight features and every unordered pair. Keep exact union `255`, and maxima `3` on masks `15` and `240`. Exactly 16 unordered blocks remain. For query mask `51` (`{a1,a2,d1,d2}`), the greatest score in any compatible block is `3/5`; the individual-row envelope is `1`.

The general claims rest on the proofs, not these small counts. The enumeration does not establish large-instance speed, parser correctness, persisted-certificate validity, or publication novelty.

## 8. Final Synthesis

The defensible present result is an **exact worst-case single-row Jaccard envelope for an exact-union, length-range, laminar upper-capacity certificate**, together with its reduction to the best adaptive feature-tree cut. It improves on specified weaker summaries and requires no prior overlap with an actual target row in the block. Those are useful, precisely scoped statements.

The local capacity statistic, group pruning principle, matroid construction, and rank recurrence have direct published or elementary reductions. The explicit attainment lemma and composition are not enough, by themselves, to establish a substantial new algorithm. Conversely, equality to an independently constructed comparator does not prove that the entire retrieval contract was historically published.

A potentially meaningful delta would be evidence that retaining directly attained internal capacities and evaluating them adaptively yields a favorable **complete preparation/storage/query tradeoff** over strong count-based systems, or a genuinely additional theorem about the information/cost frontier. Neither follows from the current algebra. The whole-block counterexample also identifies a distinct, unsolved optimization problem; this review neither solves it nor recommends silently enlarging the probe's scope to do so.

## 9. Open Questions for the Lead

1. Which empty-empty convention and exact original-ID ordering does the finite-file probe promise?
2. What statement beyond the application lemma and its known primitive reductions is the intended scientific contribution?
3. Do direct internal maxima survive the paid union/capacity I/O comparison against uniform-plus-length, leaf-cap, richer count-rectangle, and equal-information adaptive-cut controls?
4. Does a narrow later literature check find an explicit multi-resolution cardinality-envelope optimizer? This bounded primary-source review did not locate the complete formula, but does not establish its absence.

**Disposition:** mathematically sound at the corrected row-relaxation scope; worthwhile to evaluate under matched costs; not a demonstrated new rank algorithm or completed scientific contribution. Main goal7algorithms remains open.

## 10. Follow-up: Exact Two-Row Complement Certificate

Date: 2026-09-20. Requested follow-up to the original review. This section evaluates the lead's new theorem, not its implementation or reported experiment. Only this section was appended; no repository tests, code, benchmark, or manuscript were run or edited. No additional literature search was undertaken.

**Verdict: the proposed two-state DP is exact under the stated eligibility conditions. No counterexample was found within those conditions.** This is a genuine strengthening of the optimization contract: it attains the maximum over complete compatible two-row datasets, not merely individually admissible rows. The eight-feature counterexample in Section 4.1 now separates the new bound from the interval relaxation instead of refuting its tightness. The original general-block qualifications remain in force.

### 10.1 Eligibility and Metadata Recovery

Require exactly two records, exact union `U`, exact cardinality extrema `ell,u`, exact attained maxima `C_v`, and a binary feature tree whose children partition their parent's features. Both rows are eligible candidates, with no additional restrictions on their contents beyond this certificate. Use the requested convention that zero overlap scores zero, including empty-empty.

Put `H=|U|`. For two rows the multiset of sizes is exactly `{ell,u}`, including when `ell=u`. Therefore

```text
|T1 intersect T2| = ell + u - H.
```

Thus `H=ell+u` proves that the rows partition `U`. It is not a heuristic for likely disjointness. Population two is essential to this inference.

For each node define `H_v=|U intersect H_v_group|`, using `H_v_group` here for the feature group to avoid confusing the group with its population. If the selected first row occupies `x_v` features there, its complement occupies `H_v-x_v`. Exact attainment gives

```text
max(x_v, H_v-x_v) = C_v
    if and only if
x_v belongs to D_v = {C_v, H_v-C_v}.
```

Deduplicate equal alternatives. Valid observed metadata necessarily satisfies `ceil(H_v/2) <= C_v <= H_v`. It also recovers the exact minimum occupancy `L_v=H_v-C_v`, without storing it separately. The stronger fact is the two-point domain, not just the recovered interval `[L_v,C_v]`. At the root, `D_root={ell,u}`.

**Complete characterization.** A subset `T subset U` can serve as a row in a compatible complete dataset if and only if its node occupancies lie in all `D_v`. Necessity follows above. For sufficiency, take the second row to be `U minus T`. Every node then attains its specified maximum, the root gives the required two sizes, and the exact union is preserved. The two contents can be attached to the two original IDs. Extra per-ID content or eligibility constraints, if any, would require an additional contract.

This recovery argument is specific to the stated metadata and eligibility. It is a derivation here, not a statement attributed to any paper in Section 5.

### 10.2 DP, Proof, and Constructive Attainment

Let `q_v=|S intersect U intersect H_v_group|` and retain the full query size `a=|S|`. For a node and state `x in D_v`, define `F_v(x)` as the greatest query overlap of a selected subset in that subtree with occupancy `x`, satisfying every descendant two-point constraint. An impossible state has value negative infinity.

At a leaf,

```text
F_v(x) = min(q_v,x).
```

This value is attainable: select `min(q_v,x)` query features and, when needed, `x-q_v` nonquery features from the leaf. Since `0 <= x <= H_v` and `q_v <= H_v`, enough nonquery features exist. There are no further constraints inside that leaf.

At a binary internal node,

```text
F_v(x) = max { F_left(y) + F_right(z) :
               y in D_left, z in D_right, y+z=x }.
```

Any feasible subset restricts to one such child pair, so its overlap cannot exceed this value. Conversely, optimal child subsets attaining a feasible pair can be united: child groups are disjoint, their occupancies sum to `x`, and all constraints in the subtree hold. Induction proves both the recurrence and its attainment at every feasible state. Choosing states independently without the sum condition would not prove the theorem.

For fixed root size `x`, `c/(a+x-c)` increases with overlap `c` wherever the denominator is positive. Consequently the exact complete-dataset envelope is

```text
E_two = max over feasible x in {ell,u} of
          F_root(x) / (a + x - F_root(x)),
```

with value zero for zero overlap, before attempting division. A winning state's child choices reconstruct a subset `T`; its complement is the second row and certifies all claimed metadata. Explicitly outputting these sets would cost additional feature visits, but evaluating the envelope does not require outputting a witness or intersecting `S` with an actual target row.

The objective is `max_compatible_dataset max_row J(S,row)`. Hence maxima from different root states may be compared even if their witnesses are different datasets. The proof does not assert that both statewise maxima occur together in one dataset, or recover the actual two rows' two scores.

For genuine observed metadata, both distinct states at each node are locally feasible: the restrictions of the original rows witness them. Negative infinity still matters for rejecting incompatible child combinations and for handling inconsistent supplied tables. An infeasible root in allegedly observed metadata is a certificate inconsistency, not evidence that the real block may safely be discarded.

### 10.3 Strict Separation from the Richer Interval Relaxation

Return to Section 4.1: two leaves each have population `4`, maximum `3`, and therefore exact minimum `1`; the root size is `4`. Query counts are `(2,2)`.

An independently lower/upper bounded row, even with exact union membership, both leaf intervals `[1,3]`, all internal intervals, and the exact permitted root sizes, can choose occupancies `(2,2)`. It can be `S` itself and attain Jaccard `1`.

The complete two-row certificate instead requires each leaf occupancy to be `1` or `3`. The root permits only `(1,3)` or `(3,1)`. Both have greatest overlap `1+2=3`, so

```text
E_interval = 1,          E_two = 3/(4+4-3) = 3/5.
```

The complement witness attains the specified maxima, whereas the interval optimizer's complement would also have occupancies `(2,2)` and attain neither maximum. This is a discrete endpoint-attainment constraint, not a tighter numerical endpoint.

For eligible blocks, a fair interval control can derive the same lower endpoints from `H_v-C_v`; it need not retain additional minimum fields. Giving it every node's interval and restricting its root to `{ell,u}` still leaves the strict example above. The feasible families imply `E_two <= E_interval <= E_row`, where `E_row` is the original upper-capacity/length-range relaxation. A control that also enforces joint two-row endpoint attainment is no longer this independent interval relaxation.

### 10.4 Challenges and Scope Boundaries

- **Population is a correctness gate.** Three rows `{a,b}`, `{a,c}`, `{c,d}` have `ell=u=2` and union size `4=ell+u`. For leaves `{a,b}` and `{c,d}`, both maxima are `2`. Illegally applying the two-state rule permits only occupancies `(2,0)` or `(0,2)`, giving `1/3` for query `{a,c}`, although an actual row scores `1`.
- **Conservative caps are not interchangeable with attained caps.** For the legitimate disjoint two-row block `{a,c}`, `{b,d}`, those same leaves have exact maxima `1`. Replacing them by conservative upper estimates `2` is safe for the original row relaxation, but treating the estimates as attained maxima in this DP again returns `1/3` for query `{a,c}` instead of its actual score `1`. Rounded, stale, or summed-child estimates cannot be inverted into exact alternatives.
- **The union identity must be exact.** If two rows overlap, `H < ell+u` and the second row is not the complement. Exact union membership, not just its cardinality, is also needed for the stated leaf attainability argument relative to `S`.
- **The hierarchy assumption matters to complexity.** A binary node has at most four child-state pairs. Unbounded fan-out does not inherit constant transition work merely because each child has two states. Introducing unstored binary intermediate groups does not magically provide their attained capacities.
- **This remains an envelope over compatible datasets.** With singleton leaves and parents `{a,b}`, `{c,d}`, the actual block `{a,c}`, `{b,d}` and query `{a,d}` have actual maximum `1/3`. The compatible block `{a,d}`, `{b,c}` has maximum `1` and the same metadata. The new DP correctly returns `1`; it does not identify the actual block's answer.
- **Score ties retain their separate ID rule.** The exact scalar bound does not authorize discarding an equal-score block without the original-ID condition from Section 6.2. Per-ID size bindings or exclusions can also restrict which row attains a key, even when a scalar block maximum remains a safe upper bound.

These are counterexamples to extensions or misapplications, not counterexamples to the eligible two-row theorem.

### 10.5 Matched Costs and Retained State

For `G` leaves there are `2G-1` binary-tree nodes. Each has at most two occupancy states and four child-state pairs. After the histograms are known, the bound costs `O(G)` arithmetic operations and `O(G)` words of transient state; witness backpointers also fit in `O(G)`. This is a word-RAM accounting with cardinalities and comparison arithmetic handled exactly, not a claim that arbitrarily large integer operations have constant bit cost.

No extra retained certificate field is mathematically necessary, **provided** existing layout metadata establishes population two and the existing sparse union and capacities are exact. Count leaf `H_v` values from that union, count leaf `q_v` values against `S`, and aggregate both upward. The domains and DP are transient.

The population histogram is paid work. A scalar union cardinality or query/union intersection histogram alone does not give every `H_v`; unmatched union features also count. For sorted sparse inputs, a full union/query merge plus aggregation has an `O(H+a+G)` upper bound. A query hash table permits a union scan after paid query preparation. An intersection procedure that otherwise skips nonquery union features may do additional work to obtain `H_v`. Thus "no new retained fields" does not mean zero additional union traffic or zero additional query memory. Compare the interval control using these same paid populations and counts.

This follow-up establishes neither the lead's serialized access behavior nor a runtime advantage. Its exact threshold and pre-row-access obligations are unchanged from Section 6.2.

### 10.6 Bounded Independent Algebra Check

An independent ephemeral enumeration was run in memory, without writing a program file or invoking repository code. It constructed the proposed finite-state recurrence solely to compare it with brute-force complete-dataset enumeration, not as a production implementation.

For each of three binary tree shapes, enumerate all unordered disjoint row pairs `0 <= x <= y < 64` on six features: 365 pairs, including an empty row and the two-empty-row case with distinct IDs. Derive their exact certificates. For each pair, examine all 128 query masks on those features plus one external feature. Brute force every possible first-row subset of the exact union; take its complement and require exact size extrema and exact maxima at every node.

The four-leaf balanced tree uses group masks `[63,15,48,3,12,16,32]` as in Section 7. The skewed tree splits `63` into `3,60`, then `60` into `12,48`, then `48` into `16,32`. The third tree refines masks `3` and `12` into singleton leaves. In addition to comparing envelope fractions by integer cross-products, check equivalence of two-point membership and full certificate matching, compare every root state's optimal overlap, and reconstruct a complement witness from each root state's maximizing choices.

```text
Tree shapes:                              3
Block/query cases per shape:         46,720
Total block/query cases:            140,160
Constructed root-state witnesses:   253,056
Characterization/DP/witness failures:     0
```

A separate consistency check used the fixed eight-feature tree with masks `[255,15,240,3,12,48,192]`. Enumerate all 720 capacity tables satisfying each local condition `ceil(H_v/2) <= C_v <= H_v`; set root sizes to `8-C_root,C_root`. Exactly 34 tables admit a complete two-row dataset. All 686 others were correctly infeasible at the root. For the feasible tables, all 512 queries on eight features plus one external feature gave 17,408 further exact-agreement cases. There were zero failures. This checks why local capacity ranges alone do not establish global consistency.

For the original two-leaf eight-feature example, direct enumeration found 32 compatible ordered first rows, equivalently 16 unordered blocks. The complete-dataset maximum was `3/5`; the stronger all-node interval relaxation with exact root size attained `1`.

These checks use the explicit zero-overlap score convention. They do not verify the lead's reported tests or live experiment, and do not supply performance or novelty evidence.

### 10.7 Scientific Assessment

The computational primitive is an elementary tree constraint-satisfaction/max-sum DP with at most two values per variable. It is not a new rank algorithm, and the discrete feasible family is not the original downward-closed matroid relaxation.

The particular additional result is the **metadata-recovery and sufficiency theorem**: population two plus `H=ell+u` recovers complementarity, and exact attained node maxima recover disjunctive occupancies that characterize complete compatible datasets. The DP then optimizes this stronger problem exactly in linear tree work. The interval separation shows a real mathematical difference from independently bounded feature counts using the same recovered populations, not merely a renamed evaluation of the original rank bound.

That is a defensible, narrowly stated contribution candidate. This review does not establish its publication priority or scientific sufficiency. An equivalent DP control demonstrates the primitive's equivalence, not historical anticipation of this metadata-recovery theorem. Any broader novelty claim still needs appropriately scoped prior-art evidence and the lead's complete cost evaluation.

**Follow-up disposition:** accept the eligible two-row complement theorem with the stated exactness, scoring, and cost conditions. Computing an exact whole-dataset envelope for general blocks remains unresolved here; novelty remains unestablished; main goal7algorithms remains open.

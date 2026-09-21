# Similarity Selective Access Review

Date: 2026-09-20. Bounded independent mathematical and access-plan review.

Only this memo was written. No production code, repository tests, benchmarks, index files, prior memo, or manuscript were edited or run. The small enumerations below were ephemeral algebra checks. The lead's report that the previous serial plan lost to DAAT on all 30 cases is accepted as reported, not independently remeasured here. Main goal7algorithms remains open.

**Completion checkpoint.** This narrow review is finished using the proofs and finite checks already gathered; no further enumeration or literature search was started for this checkpoint. The lead reports 43 passing implementation tests. That report is not independently verified here, and this memo does not certify the implementation. Selective-plan performance, physical I/O savings, and publication novelty remain unverified. Code, serial benchmarks, and manuscript decisions remain with the lead.

## Verdict

1. **Accept Part 1.** The independent interval-quota relaxation has the proposed two-slope value function and an exact linear-tree-work evaluation. Apply quotas by clipping the domain, not by incorrectly recalculating the curve parameters after clipping. Handle infeasibility explicitly.
2. **Conditionally accept the Part 2 access argument, not an implementation.** Exact feature-to-block coverage supplies the needed query/union counts without reading the block union. Complete per-block feature aggregation and positive-only heap invariants are essential to pruning and zero-tail correctness. Streaming avoids a query-sized-by-target-count accumulator, not all target-sized retained state or worst-case target-wide work.
3. **Repair Part 3 before claiming exactness.** The near-disjoint state needs local feasibility, equivalently `0 <= e_v <= min(I,2C_v-H_v)` for valid observed `C_v <= H_v`. Without that restriction, a three-feature example returns `1` instead of the exact `1/3`. With it, the constructive DP argument works and the stated transition bound is achievable.

The interval recurrence and finite-state tree DP are elementary optimization primitives. The specific paired metadata-recovery theorem remains a contribution candidate, not an established novelty claim. Selective token-group indexing itself is not the scientific delta.

## Part 1: Exact Interval Relaxation

### 1.1 Model and Recovered Lower Bounds

Let a nonempty target block contain `p` binary-set rows, with exact union `U`, exact size extrema `ell,u`, and an exhaustive feature tree. Write `A_v` for a node's feature group, `H_v=|U intersect A_v|`, `q_v=|S intersect U intersect A_v|`, and `a=|S|`, including source features outside `U`.

For an individual target row with occupancy `x_v`, the other `p-1` rows can cover at most `(p-1)C_v` features of this group. Therefore

```text
Lraw_v = max(0, H_v - (p-1)C_v) <= x_v <= C_v.
```

This is a necessary single-row constraint, not a claim that every occupancy vector meeting it can be completed to `p` rows that attain every maximum. The interval optimization below deliberately treats these lower/upper quotas independently, together with tree consistency and ground-set availability.

The lower-bound argument also works with conservative upper capacities. In contrast, the paired endpoint-recovery theorems require exact attained maxima. `p` must be valid for the block: using the physical maximum block width instead of a smaller actual population weakens this lower bound, while understating population can make it unsafe. Empty blocks require a separate path. For `p=1`, valid metadata forces the single row to equal `U`.

More generally, the theorem below works for any valid integer lower/upper quotas on the tree, including directly retained stronger lower counts, not just this particular formula.

### 1.2 Feasible Domains and Curve Closure

For each subtree, store its feasible integer occupancy interval `[L,U]` and

```text
F(x) = min(x-d,c),  for L <= x <= U.
```

`F(x)` is the greatest achievable overlap of the selected row with `S` within that subtree. All quantities count distinct features. This result does not extend unchanged to arbitrary feature weights.

At a leaf,

```text
L = Lraw_v
U = min(C_v,H_v)
d = 0
c = q_v.
```

Reject an empty domain. For every remaining occupancy, selecting query features first attains `F(x)=min(x,q_v)`; remaining positions can be filled from the leaf's nonquery features because `x <= H_v`.

For a parent, first reject any infeasible child. Let each child's already-clipped summary be `(L_i,U_i,d_i,c_i)`, and put

```text
L0 = sum_i L_i
U0 = sum_i U_i
A0 = sum_i F_i(L_i)
Z0 = sum_i F_i(U_i)

d = L0 - A0
c = Z0
L = max(L0,Lraw_v)
U = min(U0,C_v).
```

Reject if `L>U`. Retain the `d,c` calculated before this node's clipping. At the next parent, evaluate this child at its new endpoints `L,U`, not at its pre-clipping endpoints.

**Why there are no missing slopes or occupancy holes.** Before parent quotas, child feasible intervals sum to every integer in `[L0,U0]`. Starting each child at `L_i` yields overlap `A0`. Its subsequent marginal gains are a sequence of ones followed by zeros, possibly either sequence empty. Across all children, the number of available unit-gain steps is exactly `Z0-A0`. These steps can all be taken before any zero-gain steps, since no child requires a zero-gain step to unlock a later gain. Consequently, for `L0 <= x <= U0`,

```text
max_{sum_i x_i=x} sum_i F_i(x_i)
    = A0 + min(x-L0, Z0-A0)
    = min(x-d,c).
```

The remaining child slack realizes every larger feasible occupancy at constant overlap. Parent quotas only restrict the domain of this same function. This proves the proposed recurrence and constructive attainability by induction. No occupancy-indexed convolution table is needed.

Unlike the two-state disjunctive DP, this interval aggregation can also process an arbitrary number of children in time linear in their count. Total work remains linear in the number of tree nodes and edges.

### 1.3 Exact Root Optimization

Intersect the root domain with `[ell,u]`. For fixed feasible size `x`, maximizing overlap maximizes Jaccard. With zero overlap explicitly scored zero, the objective is

```text
J(x) = F(x)/(a+x-F(x)).
```

Before saturation, `F(x)=x-d` and `J(x)=(x-d)/(a+d)`, which increases with `x` whenever relevant. After saturation, `F(x)=c` and `J(x)=c/(a+x-c)`, which is nonincreasing. Thus an optimizer for a root interval is

```text
b = clamp(d+c,L,U).
```

When the optimum overlap is zero, return zero before division; this also handles the empty-empty convention. There can be multiple optimizers, but this choice is valid.

For **exactly two rows**, cardinality extrema imply that any actual row has size in `{ell,u}`, whether or not the rows overlap. Evaluate just the feasible members of that set, deduplicating equal sizes. Do not optimize over intervening sizes for this stronger two-row comparator. For general `p`, the independent relaxation permits the whole clipped interval.

Conversely, checking only length endpoints for general `p` is insufficient. Take rows `{a}`, `{b,c}`, `{a,b,c}` and query `{a,b}`. The independent interval relaxation permits size two and score `1`; the best length-endpoint value is `2/3`. Its optimum occurs at the curve's interior kink.

### 1.4 Comparison to Paired Endpoint Constraints

For an eligible disjoint pair, `p=2` and `|U|=ell+u`. The lower quota becomes `H_v-C_v`, while exact attained maxima impose the stronger alternatives `{H_v-C_v,C_v}`. Every paired feasible assignment is an interval-feasible assignment, including the same discrete root sizes. Hence

```text
E_paired <= E_interval <= E_upper_only_row.
```

Strictness survives an **exact** interval optimizer. In the earlier eight-feature example, the two leaf groups each have population four, capacity three, and query count two; both rows have size four. Intervals `[1,3]` permit occupancies `(2,2)` and score `1`. Paired alternatives `{1,3}` permit only `(1,3)` or `(3,1)` at the root, giving overlap three and score `3/5`.

Both evaluators use the same populations, capacities, source counts, and permitted root sizes. The distinction is joint endpoint attainment, not a weaker interval implementation or additional paired information. The interval control should use this `O(G)` summary, not an avoidable full size convolution. Neither interval exactness nor paired compatible-dataset exactness means that the actual stored block attains its envelope.

### 1.5 Costs

Once leaf populations and query counts are available, a tree with `O(G)` nodes needs `O(G)` arithmetic operations and at most `O(G)` transient words for these summaries. Count exact comparison arithmetic and overflow handling. This is word-RAM accounting, not a constant-bit-cost assertion for unbounded integers.

If populations are obtained from a union scan, that scan remains paid. Part 2 instead retains `H_leaf` in block metadata and obtains `q_leaf` from the selective index. Both the paired and interval controls must receive this same representation and pay its construction, bytes, reads, and maintenance. Equal asymptotic DP cost does not imply equal constants or a runtime win.

## Part 2: Selective Access Plan

### 2.1 Exact Projection to Blocks

Use `W` for the proposed physical block width, to distinguish it from a particular target block. For a nonempty exact target-feature posting interval `[lo,hi)` in the same physical row order as the block bodies, the set of blocks it meets is exactly

```text
[ floor(lo/W), floor((hi-1)/W)+1 ).
```

Require `W>0` and `0 <= lo < hi <= n`. Skip empty intervals before subtracting one. The formula handles block boundaries and the last partial block; it does not work by dividing arbitrary original IDs when physical row order differs from ID order.

Coalesce overlapping or adjacent projected intervals **within each feature**. Separate row intervals can project to the same block: `[0,1)` and `[2,3)` both map to block zero for `W=4`. That feature must contribute once, not twice, to that block's query/union count. Do not coalesce across gaps or across different features as though they were one feature.

The directory must represent exact feature presence, not postings with unmarked holes, omitted frequent features, or probabilistic membership. Source sets must be deduplicated as binary sets. Absent source features contribute no block visits but remain in `a`.

### 2.2 What the Merge Computes

For every feature `f` in the source, open its block-interval cursor when its directory entry exists. Merge the active cursors by block ID. At a block, gather **all** source features whose projected posting contains it before finalizing the counts:

```text
q_leaf[j] = number of distinct source features in leaf j
            that occur in at least one row of this block
q        = sum_j q_leaf[j] = |S intersect U_block|.
```

This is exact source-to-**union** overlap, not exact source-to-row overlap. It requires neither an endpoint-row comparison nor a union-body read. Under-counting by evaluating the bound before every cursor at the block has been consumed can falsely prune. Double-counting aliased intervals destroys exactness even when it merely makes a bound looser.

By definition, every eligible row with positive Jaccard shares a source feature, so its block appears in this stream. A touched block can nevertheless contain zero-score rows, or be touched only because of an excluded self row. "Positive block" must mean a block with positive source/union overlap, not a promise about each eligible row.

Metadata must be addressable without parsing a body: body offset, actual length extrema, original-ID minimum, leaf populations, and full capacities. Aggregate `H_leaf` and `q_leaf` up the fixed tree; then evaluate the chosen bound. On survival, score eligible rows exactly from the body. Exact row-pair scoring is sufficient only when the block actually has two rows; other populations require the corresponding exact verification path.

With ascending block traversal, keep one active `G`-leaf count buffer, `g` feature cursors, a merge queue, and the result heap. No `n`-entry per-query overlap accumulator is needed. All feature contributions at a block must be complete before that buffer is reset.

### 2.3 Heap and Zero-Tail Proof

The proof requires these invariants:

- The result heap contains only eligible, exactly scored, strictly positive rows, with deterministic score-descending/original-ID-ascending ordering. Do not insert self rows, zero rows, optimistic bounds, or oracle-provided seeds.
- Pruning at a positive kth threshold is allowed only after `k` eligible exact positive results exist. Use the exact rational comparison and the ID-aware equality rule from the prior review. Before then, only a genuine zero upper bound can dismiss a block without verification of its possible positives.
- Process every touched block, or soundly certify that it cannot improve the full heap. Coarse membership alone never supplies an exact row score.

After complete stream exhaustion, if the positive heap has size `r<k`, **all eligible positive rows are retained**. Otherwise a positive was missed or evicted. A missed positive contradicts complete block coverage plus sound verification/pruning. An eviction would require an already-full size-`k` positive heap, whose cardinality cannot subsequently shrink under these invariants. Thus neither event is possible when the final heap is short.

It follows that every other eligible ID has score zero. Iterate the inverse-ID structure in original-ID order, skip retained positive IDs and excluded IDs, and take the first `k-r` remaining IDs, or all that exist. This does not require knowing which touched bodies contained zero rows. Fill only after exhaustion; doing it early cannot certify that an unvisited ID is zero.

The inverse-ID structure must provide a correct dense mapping or ordered iterator, not a loop over a potentially huge sparse numeric ID range. With a constant-time membership set for the retained positives, the fill examines at most `k` eligible IDs plus exclusions when enough results exist. Output materialization, any zero-row payload reads required by the output contract, final ordering, and persistent inverse-ID bytes are paid. Specify behavior for `k=0`, `k` exceeding eligible population, empty queries, and empty rows; zero-overlap-as-zero is essential here.

### 2.4 Paid Work and Worst Cases

Let `m=ceil(n/W)`, `g` be the number of source features with nonempty block postings, `P_S` the number of their stored block intervals decoded, `R_S` the number of their feature/block incidences actually emitted, and `Q_S` the number of distinct touched blocks. `R_S` can be much larger than the compressed interval count.

| Component | Charge or bound |
|---|---|
| Source preparation and directory lookup | All source features, including absent features; directory searches and cursor setup |
| Streaming merge | Straightforward `O(g + P_S + R_S log(g+1))` work; interval bytes and page reads are separate physical costs |
| Metadata and bound evaluation | Up to `Q_S` metadata records and `O(Q_S G)` DP work |
| Verification | Surviving body bytes, decode work, exact row scoring, and result-heap work |
| Query scratch | `O(g+G+k)` words plus explicit I/O/decode buffers, not an `n`-entry counter array |
| Zero completion and output | Inverse-ID accesses, positive-ID membership, exclusions, ordering, and any required payload reads |

Converting sorted existing posting intervals and coalescing them is linear in the input interval count and does not require expanding every row ID. However, the derived feature directory and block intervals are additional retained structures unless something is explicitly replaced. Count their construction and storage even if their input postings were already paid.

Populations can be built by a charged row/feature scan or appropriate range-event accumulation on the projected index. Exact per-node maxima require preserving per-row count information during construction; feature/block presence alone does not recover them. Declare construction scratch, including any block-by-group buffers or event sorting. Refresh must consistently update postings, block summaries, body offsets, and inverse IDs, or queries must run against an immutable version. An incomplete index can miss positives; inconsistent exact statistics invalidate certificate claims.

Fixed-size metadata itself costs `O(mG)` retained words, besides directory and inverse-ID state. Avoid describing the absence of a query accumulator as absence of target-sized storage. Metadata and body page placement also matter: a logical body skip is not automatically a physical page-read saving.

If `gcap` bounds the number of active source-feature cursors, queries exceeding it need a declared correctness-preserving fallback. Never truncate source features. Fix the gate independently of exact answers and include its dispatch and fallback work. If `gcap` instead names a different parameter, its interpretation must be explicit; this review assumes the cursor-count interpretation for that condition.

Worst-case `Q_S=m` and `R_S=g*m`: every source feature can touch every block. If bounds remain high or the heap never fills, all touched bodies may also be read. When there are fewer than `k` eligible positives, no positive-threshold pruning can save their verification. Small target blocks can make capacity metadata and DP work larger than directly computing the few row overlaps. None of these costs is removed by interval compression alone.

**Exact expanded-membership limitation for two-row blocks.** Including self consistently in both counts, define

```text
W_target(S) = sum_target_rows T |S intersect T|
W_block(S)  = sum_blocks B |S intersect (T1_B union T2_B)|

W_target(S) - W_block(S)
    = sum_blocks B |S intersect T1_B intersect T2_B|.
```

This is inclusion-exclusion within each pair, summed over blocks. A singleton tail contributes zero difference. Consequently, eligible disjoint pairs provide exactly zero expanded-membership saving. For a two-row layout, the saving is between zero and `W_target(S)/2`; only overlapping pairs contribute it. In the literal emitting merge above, `W_block=R_S`.

This is **not** a universal no-benefit result. For width two, target intervals `[0,1)`, `[2,3)`, `[4,5)` project to one block interval `[0,3)`: both expanded counts are three, but interval count falls from three to one. Decoding, directory bytes, per-block scheduling, and candidate verification/pruning may still improve independently of expanded membership count.

The lead reports 97.8% paired eligibility in initial caGrQc blocks. That unweighted fraction does not prove a small query-weighted `W` gain: the remaining overlapping pairs could contain many or disproportionately queried shared features. Report the sum of query/shared-feature incidences above, alongside interval counts, merge events, body reads, and scoring work. No new benchmark result is inferred from the eligibility percentage.

### 2.5 Strongest Comparisons and Failure Criteria

**Controlled paired-vs-interval comparison.** Use identical physical row order, blocks, feature map, exact feature directory, block postings, `H_leaf`, full caps, ID semantics, query stream, guard/fallback policy, and common cheap prefilters. Evaluate the interval relaxation with Part 1's exact linear-time recurrence and the correct two-row root sizes. Permit sound early exits and charge actual reads rather than artificially slowing the control.

For eligible pairs, `E_paired <= E_interval`. With identical ascending block order and deterministic exact scoring, both runs have identical exact top-k heaps after each block: a block skipped by one correct bound could not have changed that heap if scored. This is an induction from the same initially empty heap, not an oracle threshold requirement. The paired bound can therefore skip a superset of the interval-pruned bodies. It does not inherently reduce the shared directory work, feature/block merge events, or number of touched metadata records.

The relevant incremental tradeoff is consequently

```text
additional paired-bound/eligibility work
    versus
body reads, decoding, and exact scoring avoided only by the paired bound.
```

Fewer surviving blocks alone is not a runtime claim. Cache effects and byte traffic remain empirical questions. Also include a cheap union/length or no-heavy-bound selective control to expose cases where sophisticated filtering costs more than direct verification.

**End-to-end comparison to DAAT.** Native DAAT can obtain exact per-row overlaps from the original feature postings and use target lengths and IDs to rank them; it need not read the same row bodies this new plan seeks to avoid. Preserve its existing interval compression, skipping, and permitted memory techniques. A paired victory over an identical-access interval control does not imply a victory over that incumbent.

Report both complete-build/lifecycle costs and, if relevant, incremental costs when the original posting index genuinely exists for both methods. Include new persistent bytes, query scratch, warm/cold assumptions, refresh, output, and fallback coverage. The reported 30 serial-plan losses remain negative evidence for that serial plan; selective indexing is a new hypothesis, not a retroactive reversal of those results.

**Priority boundary.** Token-to-target-group presence indexing and pre-verification group pruning are directly documented in [LES3, Section 3.1](https://arxiv.org/pdf/2107.10417), already inspected in the prior review. The interval-to-fixed-block projection and streaming layout are engineering adaptations here, not evidence that group indexing was invented anew. Only the paired metadata theorem and any demonstrated matched-cost benefit should carry the proposed scientific claim. Their publication novelty remains unestablished.

## Part 3: Optional Near-Disjoint Pair Extension

### 3.1 Counterexample to the Unfiltered State Rule

Take exactly two rows `{a,c}` and `{b,c}`, union `{a,b,c}`, `ell=u=2`, and `I=ell+u-H=1`. Let the root children be `A={a,b}` and `D={c}`. Their `(H_v,C_v)` values are `(2,1)` and `(1,1)`. Query `S={a,b}`.

The proposed rule with only `e<=I` permits state `(e=1,x=2)` in `A`, because `H_A+e-C_A=2`. It also permits `(e=0,x=0)` in `D`. They sum to the required root `(e=1,x=2)` and yield overlap two, hence claimed score `1`.

But the first state makes one row's `A` occupancy two, exceeding the exact maximum one. In every compatible actual pair, `c` is the shared feature and each row gets one of `a,b`; the exact envelope is `1/3`. The raw DP is therefore not exact as stated. Its extra states form an optimistic relaxation, but its constructive-attainment claim fails.

### 3.2 Correct Local Domains and Proof

With node intersection count `e`, the two row counts are `x` and `y=H_v+e-x`. Require

```text
0 <= x,y <= H_v
0 <= e <= min(x,y)
max(x,y) = C_v.
```

For exact observed `C_v <= H_v`, these reduce to

```text
0 <= e <= min(I,2C_v-H_v)
x in {C_v,H_v+e-C_v}, with equal states deduplicated.
```

If `2C_v-H_v<0`, no state is feasible. The additional inequality rejects the counterexample's `e=1` at node `A`.

For a valid leaf state, `F(e,x)=min(q_v,x)` remains exact. Choose the first row's `x` features to maximize overlap, choose any `e` of those features to be shared, and let the second row contain the leaf complement plus these shared features. The inequalities ensure this is possible and attains the required capacity.

Combine child states by adding both `e` and `x`, retaining only valid parent states and maximizing summed overlap. The children occupy disjoint feature groups, so intersections and row sizes add, and their witness pairs unite. At the root use `e=I` and `x in {ell,u}`; monotonicity in overlap gives the same Jaccard optimization as in the disjoint-pair proof. This establishes exactness over compatible complete pairs, not the actual pair's answer.

There are at most `2(I+1)` states per node. Iterating each pair of child states once, then routing its summed coordinates to a permitted parent state, gives `O(G(I+1)^2)` arithmetic work and `O(G(I+1))` state. Looping over every parent state and every child-state pair would unnecessarily add another factor of `I`. Root intersection and local state limits should be derived and checked exactly. This is useful only with an explicit small-`I` regime or a paid fallback; the bound is not linear uniformly over arbitrary overlap sizes.

The filtered extension is another elementary tree-CSP application of recovered pair constraints. Its novelty and practical benefit are not established by this review.

## Independent Finite Checks

All checks ran in ephemeral orchestration memory, not against the lead's implementation, files, test runner, or live experiment. Scores used exact integer cross-products and the stated zero-overlap convention.

| Check | Finite domain | Result |
|---|---|---|
| Arbitrary interval quotas | Four features; two size-two leaves; all 540 combinations of valid leaf/root intervals; all 32 queries including one external feature | 17,280 cases; zero curve, feasibility, or root-optimizer failures |
| Population-derived interval quotas | Four features; binary tree with singleton leaves; every unordered content multiset of population 1, 2, or 3; all 32 queries | 16, 136, and 816 blocks respectively; 30,976 cases; zero failures, including discrete roots for population two |
| Posting-to-block projection | All feature membership masks for `0<=n<=8`, block widths 1 through 4; maximal runs and individually split row intervals | 2,044 cases; projected block sets matched exact feature presence |
| Zero completion identity | All three-row datasets over three features; 16 source queries including an external feature; nonphysical ID order; no exclusion or each possible self exclusion; `k=0..4` | 163,840 cases; retained-positive-plus-zero-ID-tail results matched full exact ranking |
| Near-disjoint corrected DP | Five features; tree masks `[31,3,28,4,24]`; all 528 unordered row pairs; all 64 queries including one external feature | 33,792 cases; zero corrected state-optimum failures; unfiltered formula gave a different final score in 622 cases |

For interval checks, every feasible root occupancy's predicted overlap was compared with direct enumeration of all quota-feasible subsets, not just the final score. In the arbitrary-quota check, 12,416 cases were feasible; the others correctly remained infeasible. For the observed-block checks, exact union membership and recovered lower quotas were included. For population two, brute-force comparison used only row sizes `ell` or `u`.

For the near-disjoint check, reference pairs were enumerated directly and grouped by exact union, size extrema, and every attained capacity. The DP was compared with that complete-dataset family. For zero completion, complete knowledge of the positive set was an explicit algebraic premise; that check does not validate an implementation's cursor exhaustion or coverage.

These bounded checks support the proofs and counterexamples. They establish no speedup, serialization correctness, physical I/O avoidance, concurrency safety, or publication priority.

## Final Assessment

The lead's primary next comparison is well defined: a paid selective access plan, with the paired bound measured against an exact `O(G)` interval control sharing that plan. The interval theorem is valid, and the access argument can preserve exact top-k without a per-target query accumulator or oracle seed when the stated invariants hold.

The decisive unresolved question is whether paired-only body savings repay its bound work and the complete selective index costs, both relative to the interval control and relative to strong native DAAT. The optional overlapping-pair theorem is valid only after filtering locally impossible `(e,x)` states. None of these conclusions promotes the candidate to a novel or completed algorithm. Main goal7algorithms remains open.

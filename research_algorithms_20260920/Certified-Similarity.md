# Certified Similarity Through Edit-Transfer Rank Domains

Date: 2026-09-20. Family A05. Research proposal, not production code or a measured 4 GB implementation. Global novelty remains unresolved, especially relative to dynamic set-kNN indexing and safe-region query reuse.

**Current exact-envelope extension, 2026-09-21:** the
[modal query summary](Similarity-Query-Limit-Exceptions.md) and
[compressed query-dual evaluator](Similarity-Compressed-Dual-Core.md) provide
two independently mathematically reviewed ways to use the same compact
attained-two-row information. Branching favors small structural conflict r;
inverse thresholds favor small numeric query overlap Q. The latest similarity
suite passes 116 tests, and independent theorem checkers are lead-replayed.
The [public profile](Similarity-Exception-Workload-Profile.md) retains the
negative finding of only 105 interval-tight opportunities among 12,542 scored
calls at G=4. No new complete top-k timing, RAM claim, or seven-family novelty follows.

## Premise Check

**Proposed delta:** share a small, *universally verified rank certificate* across nonidentical source sets. Correct changed intersections through signed feature postings; cover every unchanged-intersection target using an exact cardinality-dependent rank domain. A successful query scores only the certificate and changed-intersection targets, without retaining an anchor-by-target score matrix. This eliminates comparisons on eligible queries, not just their RAM residence.

The inherited [D10-D14 decision map](../research_4gb_20260919/Architecture-Decision-Map.md), [final brief](../research_4gb_20260919/Final-Research-Decision-Brief.md), [D10 class proof](../research_4gb_20260919/Architecture-Candidates-v1.md#candidate-3-similarity-classes-with-exact-expansion), [D12 summaries](../research_4gb_20260919/02-prd06-Architectures.md#h1-certified-motif-and-similarity-block-joins), and [hub-bounded evidence](../research_4gb_20260919/03-feasibility-Evidence.md#h3-hub-bounded-exact-similarity) were read. Equality classes, `k+1` expansion, source batching, exact prefix filtering and multiresolution intersection bounds are **not** claimed as contributions here.

Resource target: 4,000,000,000 physical bytes; provisional worker ceiling 3,000,000,000 bytes, with 1,000,000,000 for OS/other processes, requiring measurement. Retained prepared blocks across the selected portfolio and pinned generations must total at most 50,000,000,000 bytes. Input, peak scratch and output staging have separate finite allowances. No hidden candidate array, complete row decode, per-thread heap or unbounded client buffer is allowed.

## Expert Lenses

The mathematical lens asks whether every omitted target is dominated. The retrieval lens asks whether this differs from known incremental kNN. The storage lens charges certificate construction, posting products and refresh. The adversarial lens attacks cardinality shifts, deleted winners, zero tails, stale universes and insufficient certificate slack. These are self-review perspectives, not an independent reviewer endorsement.

## Contract

Freeze source snapshot, feature projection, target universe `T`, and an original-ID total order. A graph node's features are its distinct selected neighbors; duplicate relationships are deduplicated before cardinalities are computed. Candidate filtering does not alter the feature universe. A different neighbor filter is a different projection.

For source set `S`, target set `T_j`, write `a=|S|`, `b_j=|T_j|`, `c_j=|S intersect T_j|`. Supported scores:

| Mode | Score | Exact ranking key for fixed source |
| --- | --- | --- |
| Set intersection | `c_j` | Integer `c_j` |
| Jaccard | `c_j/(a+b_j-c_j)` | Exact rational |
| Binary-set cosine | `c_j/sqrt(a*b_j)` | `c_j^2/b_j` when `a>0,b_j>0` |

Every mode assigns zero when either set is empty, including empty-empty. This is an explicit choice, not a claim about all libraries. A separately specified nonnegative integer-weighted cosine extension appears below; signed cosine and weighted Jaccard are outside this proposal.

Return exactly `k'=min(k, |T minus {source_id}|)` distinct target IDs, sorted by decreasing exact score, then increasing original ID. Exclude the source ID before selection. Zero scores fill the tail; tied boundary scores do not expand output beyond `k'`. `k=0` returns nothing. This version has no score cutoff, global `topN`, bottom-k or per-source target predicate. Different fixed target predicates may have their own paid certificates. No approximate-candidate stage determines completeness.

Store integer score witnesses `(a,b,c)` alongside or instead of a display score. Jaccard comparisons use cross-products; cosine comparisons use `c_x^2*b_y` versus `c_y^2*b_x`. With cardinalities at most `2^32-1`, signed 128-bit arithmetic suffices for these products and rank-domain coefficients. Larger cardinalities require checked wider arithmetic, not silent overflow. Rounded display scores never determine ties.

## Closest Primary Work

External sources below were inspected in this research pass; the [evidence ledger](similarity-embeddings-Evidence.md) records sections and limitations.

| Primary work | Mechanism already established | Boundary of the proposed delta |
| --- | --- | --- |
| [Bayardo, Ma, Srikant, All-Pairs, WWW 2007](https://www.bayardo.org/ps/www2007.pdf), sections 4.1-4.6 | Partial inverted indexing, threshold-dependent completeness, residual pruning, binary specialization and an out-of-core repeated-scan extension | Neither exact indexing nor bounded external matching is new. Here a previously verified small witness set covers unchanged intersections for many different sources. |
| [Anastasiu, Karypis, L2AP, ICDE 2014](https://davidanastasiu.net/pdf/papers/2014-AnastasiuK-ICDE-l2ap.pdf), section V | Cauchy-Schwarz prefix-norm bounds shrink indexes and prune candidate generation/verification | This proposal does not replace their bound with another name. It transfers a complete ranking domain instead of establishing each query's candidate bounds afresh. |
| [Zhang et al., transformation-based KNN set search, TKDE](https://www.jinwang18.net/files/tkde19-setknn.pdf), sections 4-5, Algorithm 2 | Token-group count representations, an R-tree, lower bounds on Jaccard distance and exact branch-and-bound search | This substantially overlaps the *existing D12* idea of conservative summaries. Our candidate stores no target hierarchy and requires no per-query best-first search frontier on the accepted path. |
| [Amagata, Hara, Xiao, Dynamic Set kNN Self-Join, ICDE 2019](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf), sections II-IV, especially IV-A/IV-G | Inverted postings, reverse-kNN lists and per-set local difference indexes support exact incremental Jaccard/cosine; the paper explicitly recognizes potentially quadratic local-index space | **Closest threat to novelty.** Signed intersection updates and changed-result discovery are old. Candidate distinction: a fixed-size cross-source certificate, batched net edits, an affine cardinality domain and an explicit rejection/fallback path, without maintaining all reverse-kNN or pairwise local indexes. |

The full [Hasan et al. safe-region paper](https://www.aamircheema.com/research/SR_SSTD09.pdf), section 2 and the dynamic extension, has now been inspected. Its region is the intersection of every retained-versus-outsider dominance half-space; its proof is directly analogous to our dominance component. It also maintains spatial impact regions for insertions/deletions. The affine integer interval is a specialization of that established principle, not new safe-region algebra. Our exception-set correction and survivor gate permit changes to the retained neighbor set, unlike simply staying inside a region with an unchanged neighbor set. Whether that bounded combination is new remains unresolved. No paper speedup is transferred to this machine.

## Candidate Approaches

1. **Metric-distance transfer:** reuse anchor neighbors with triangle-inequality radius bounds. Established metric-search territory; binary cosine additionally requires a valid metric transformation. Useful baseline, weak novelty case.
2. **Complete parametric rank arrangement:** store all target Jaccard lines and their rank crossings. Exact but can create quadratic crossing/event state. This is not admissible as an unnamed index.
3. **Bounded witness-domain transfer, selected:** retain only `h` witnesses and the intersection of their pairwise dominance domains over every outside target. Recompute the sparse net-intersection changes. Its certificate can fail; bounded fallback is part of the algorithm, not evidence of a successful fast path.

## Mechanism And Math

### 1. Counterfactual Anchor Scores

Choose an anchor feature set `P`, not necessarily a graph node. During preparation compute `c^P_j=|P intersect T_j|`. For a hypothetical positive source cardinality `a`, define

```text
J_P(j; a) = c^P_j / (a + b_j - c^P_j).
```

These are counterfactual ordering functions, not necessarily valid similarities for all `a`: a numerator can exceed `a`. Denominators remain positive because `b_j>=c^P_j` and `a>=1`. Only targets whose intersections are unchanged will later use these as actual scores.

Anchor selection is not a free nearest-anchor search. The initial profile uses a supplied source-family anchor or a fixed, capped routing choice recorded with the query; its construction/routing work is charged. Automatic discovery of useful near-core families is an unimplemented optimization. Scanning all anchors or retaining a source-to-anchor map must add its explicit CPU, I/O and storage cost.

Choose `U`, the first `h` target IDs at a construction cardinality `a0=max(1,|P|)`, under the full score-and-ID ordering. Include zero-overlap targets. Keep their `(id,b_j,c^P_j)` triples.

For each `u in U` and `v outside U`, Jaccard dominance is equivalent to

```text
A_uv * a + B_uv >= delta_uv
A_uv = c^P_u - c^P_v
B_uv = c^P_u*b_v - c^P_v*b_u
delta_uv = 0 if id_u < id_v, otherwise 1.
```

The strict inequality needed when `v` wins a tie becomes `>=1` because the expression is integral. Intersect all such half-lines with the integer domain `[1,a_max]`:

```text
A>0: lower = max(lower, ceil((delta-B)/A))
A<0: upper = min(upper, floor((B-delta)/(-A)))
A=0: infeasible if B<delta; otherwise no restriction.
```

Thus a certificate is a single exact integer interval `I_P=[lower,upper]` plus `U`, rather than a full arrangement. In this construction `a0` is feasible whenever it is admitted. Certificate validity means **every** member of `U` outranks **every** outside target throughout `I_P`; it does not promise an unchanged ordering *within* `U`.

For intersection and binary cosine the counterfactual ordering is independent of positive `a`. Build the analogous certificate from `c^P_j` or `(c^P_j)^2/b_j`, taking a zero numerator and denominator one for empty targets. The entire positive cardinality range is valid. Cosine's squared ordering is legitimate only because these scores are nonnegative.

### 2. Net Edit Support

For an actual source, externally merge its feature stream with the anchor to obtain additions `D+ = S minus P` and removals `D- = P minus S`. Let `sigma_f` be `+1` or `-1` accordingly. Scan their exact target postings and externally reduce

```text
Delta_j = sum over f in D+ union D- of sigma_f * 1[f in T_j]
c^S_j = c^P_j + Delta_j
C = {j : Delta_j != 0}.
```

This is *net* edit support. A target touched by an addition and removal can have `Delta_j=0` and remains covered by the certificate. Do not discard either sign before reduction. Discovering `C` still pays for all posting records, even when cancellation makes `C` tiny.

No persistent `anchor x target` overlap plane is required. For each `j in C` outside the small retained triples, compute `c^P_j` by an exact streamed intersection when needed. A precomputed overlap plane is an optional, separately charged alternative, not part of the stated bound.

### 3. Witness Survival Gate

Accept the fast path only if

```text
a in I_P
and |U minus C minus {source_id}| >= k'.
```

Then compute exact scores of `C union U`, exclude self, and select its first `k'` under the declared order. Empty sources directly take the smallest eligible IDs, since every score is zero. If the gate fails, use a complete exact baseline over the full target universe; do not truncate the edit support or substitute an approximate search.

### Correctness Theorem

**Claim.** An accepted query returns precisely the full-universe top-k.

**Proof.** Signed-posting reduction is the indicator identity `1_S=1_P+1_D+ -1_D-`, so every target in `C` is scored exactly. For a target `v` outside `C union U`, the intersection is unchanged, hence its actual score equals the anchor's counterfactual score at `a`. Every surviving witness in `U minus C minus {source_id}` also has its counterfactual score as its actual score. Certificate dominance supplies at least `k'` distinct eligible targets ahead of `v`, including ID ties. Consequently `v` cannot enter the first `k'`. All remaining possible winners are explicitly scored in `C union U`. Self-removal, zero scores and duplicate feature sets do not affect the argument. The empty-source and `k'=0` cases follow directly from the contract. QED.

This is a no-false-negative theorem, not a probabilistic recall guarantee. It does not bound the fraction of queries admitted by the fast path.

### Nonnegative Weighted Cosine Extension

The same theorem applies to nonnegative integer vectors, not only binary sets. Declare each feature weight in `[0,65535]`, with at most `2^32-1` nonzero coordinates. Replace cardinality `a` by `sum_f S_f^2`, `b_j` by `sum_f T_jf^2`, and `c^P_j` by the dot product `sum_f P_f*T_jf`. The source norm is common to all targets, so ranking still uses `(c^P_j)^2/b_j`; the rank domain covers every positive source norm. Zero vectors again have score zero.

An edit is now `delta_f=S_f-P_f`, with signed target-posting contribution `delta_f*T_jf`. Reduce these exactly to obtain `Delta_j`. Targets with zero net dot-product change remain covered even if many coordinates changed. The witness-survival proof is unchanged. This does not generalize the Jaccard affine formula to weighted Jaccard.

Under these declared widths, dot products and squared norms are below `2^64`, and comparison products are below `2^192`; checked unsigned 256-bit comparisons and signed 128-bit delta accumulation suffice. Target squared norms are streamed or read with the target, not held in an uncharged `n`-array. Posting entries now contain target weights, and heap/output witnesses contain larger integers; substitute their actual padded widths in every byte equation. This extension does not promise support for arbitrary floating weights or signed similarities.

## Bounded Procedure

```text
build_anchor_rank_certificate(P, targets, h, metric):
    U = bounded_exact_top_h(full_target_scan(P), h, metric, a0)
    I = [1, a_max]
    for target v in a second complete target scan:
        compute (b_v, cPv) with bounded merge cursors
        if v not in U:
            for u in U:
                intersect I with exact dominance constraint(u, v)
    publish(P_reference, U_sorted_by_ID, I, metric, universe, generation)

query_source_rank_certificate(S, source_id, certificate, k):
    pin all input, universe, metric and certificate versions
    compute a and k_prime; handle zero/empty cases
    if a outside certificate.interval: use complete_exact_baseline
    D = streamed signed symmetric difference(S, P)
    runs = bounded_external_reduce(signed_target_postings(D), key=target_ID)
    survivors = |U| minus nonzero_delta_hits_in_U minus self_if_surviving
    if survivors < k_prime: discard runs; use complete_exact_baseline
    H = empty bounded k_prime heap
    for record j in merge_union_by_ID(runs_nonzero, certificate.U):
        skip source_id
        obtain exact cP from U or streamed_intersection(P, T_j)
        cS = cP + record.delta_or_zero
        offer (j, a, b_j, cS) to H using exact comparison
    sort H by score_and_ID; stream final rows with byte backpressure
```

The run builder consumes one posting at a time. Its merge fan-in is bounded; a huge edit list does not create one open file or one heap slot per feature. A first scan of reduced runs determines survivor count; a second scores candidates. `U` is sorted by ID for the merge and membership bookkeeping; the heap's final sort is small. One active source is the base schedule. Extra sources or threads multiply their actual owned buffers and require admission.

### Executed Finite Reducer And Real Fallback

An optional stronger Jaccard admission rule is now specified in [Similarity-Envelope-Certificate.md](Similarity-Envelope-Certificate.md). It compares the kth actually surviving witness with an exact counterfactual outsider envelope, represented by at most one target per anchor-intersection count. It dominates this document's all-U interval gate and has a bounded file-backed constructor with exact probe evidence. The envelope is an additional paid artifact, not a silent replacement for the original theorem; target refresh, common-posting cost and closest-art comparison remain material.

The [third evidence listing](similarity-embeddings-Evidence.md#finite-buffer-similarity-execution) now executes this schedule with immutable target/posting files. Its kernel has no reference to the fixture's target dictionary. Posting-directory lookups are binary searches of fixed-record disk indexes. Source/anchor differences, target intersections, reduced deltas and candidate union are iterators. Only the `h` certificate triples and `k'` selector are retained; there is no candidate map.

```text
reduce_finite_posting_stream(stream, M_records, fanin, scratch_cap):
    require 2*record_width*preflight_posting_count <= scratch_cap
    fill at most M_records; sort; write run(generation=0, integer_run_id)
    retain run count, not a vector of all run paths
    while run_count > 1:
        open at most fanin runs from current generation
        merge in target-ID order; sum all signed values for each target
        discard only exact zero sums; stream other sums to one next-generation run
        close cursors and delete consumed inputs; account simultaneous live bytes
    return final sorted cursor (reduce a sole initial run on read)

score_reduced_candidate_union(reduced, U):
    first reduced scan: count unchanged nonself witnesses; reject if too few
    second scan: merge with ID-sorted U; one record per distinct target
    cP = retained witness value, or a streamed anchor/target dot product
    cS = cP + reduced_delta_or_zero
    offer exact (id,a,b,cS) to bounded k selector; never rescore via S directly
```

**Reducer invariant.** Every record in any run equals the sum of a disjoint subset of original signed posting records for its target. Merging partitions and adds those subsets; zero removal preserves the total. After the final merge, each target's emitted value is its exact `Delta`. Early removal of a zero partial sum is valid, but removal of a nonzero partial sum or one sign is not. The selection theorem above therefore applies to the actually consumed stream, not to an oracle-defined `C`.

With `X` initial records, merge fan-in `f`, and buffer `M` records, at most `ceil(log_f(max(1,ceil(X/M))))` merge generations are needed. The two-generation `2*w*X` byte reservation is safe because reductions never expand record count. The executed probe enforces live payload bytes as every output record is written, and opens at most two merge inputs. Production admission additionally reserves file headers, directory/inode costs, minimum allocation units, fixed I/O pages and numerical workspace; the tiny byte counter does not cover those.

Domain, survivor and scratch rejection all invoke the **same actual complete scan**, exact comparator and full-tail selector. The scan can finish with bounded state without scratch because it never materializes pairs. Output/deadline admission may still reject the job. A mismatch inside the pinned row/posting manifest, or a shared source from a different epoch, fails before querying; a coherent newer target snapshot with an older certificate uses the complete scan on the newer snapshot.

Before emitting edit records, a bounded pass over edit features and posting lengths can compute `P_D`. Reserve run/output space or choose the complete baseline immediately. A simple merge schedule admits at most two live run generations, conservatively `2*run_width*P_D` plus headers and final reduced-run overlap; actual allocator lifetimes decide whether that final run shares one generation. If this space is unavailable, do not begin an unfinishable spool. A baseline that also cannot meet disk/output/deadline admission is a failed job, not a truncated result.

## RAM, I/O And Lifecycle

Let `n=|T|`, `m=sum_j b_j`, `A` be anchor count, `p=|P|`, `e=|D+ union D-|`, `q=|C|`, and `P_D=sum_{f in D} posting_length(f)`. Let `L_T` be prepared target-row payload bytes; `B_P` the anchor bytes; `w` the padded certificate/heap record width, not just field widths. `Q_C` is charged page/ID-lookup traffic for the selected changed targets.

```text
RAM_query <= B_sort + B_merge + B_IO + B_row_cursors + B_ID_cache
             + w*h + w*k' + B_output + B_runtime + B_file_residency
RAM_build <= max(B_sort_build, B_intersection_cursors + w*h)
             + all concurrent I/O, ID, runtime and residency buffers
```

Large anchors are replayed with bounded cursors; caching all `p` entries is allowed only when explicitly admitted. A single target row never has to be completely decoded. There is no `n`-entry candidate accumulator. Require the worker sum to fit its measured allocation and the complete machine to remain below 4 GB.

For external sorting, define byte traffic `Sort(X) <= 2X(1+ceil(log_f(max(1,ceil(X/M_sort)))))` for a simple bounded-fan-in run/merge plan, before record-format expansion. This is a plan accounting upper bound, not a hardware service-time model; zero bytes need no sort.

```text
Preparation: two sorted/deduplicated membership orders plus ID mapping.
Per-anchor conservative build reads <= 2*(L_T + n*B_P) + ID/page overhead;
    comparison work O(n*h) plus two rounds of exact intersections.
Retained certificate payload <= A*(w*h + interval/header/reference bytes).
Accepted query reads/writes <= source/anchor edit scan
    + posting bytes(P_D) + Sort(run_width*P_D)
    + two reduced-run scans + Q_C + target bytes(C) + q*B_P
    + certificate bytes + complete output bytes.
```

The `n*B_P` and `q*B_P` terms conservatively charge replay without a resident anchor; a measured cache can lower them. Failed gates add their already-spent discovery cost to the complete baseline. The exhaustive baseline streams every eligible target and maintains only the k-heap; it needs no quadratic pair file, but worst-case time is still large. All-source output alone is `Omega(number_of_sources*k')` records.

For simple uncompressed u32 feature and target IDs, two membership orientations cost approximately `8m+8(n+F+2)` bytes of payload/offsets, before external-ID maps, cardinalities, raw provenance, page headers and selected results. Prepared retention is

```text
D_retained = unique_blocks(all pinned target/posting/ID generations)
             + selected certificates + retained result generations
             + retained raw provenance + recovery artifacts <= 50e9.
D_peak = D_retained + input_not_already_counted + live_sort_runs
         + edit_runs + output_staging + unpublished_replacement_blocks.
```

An intentionally conservative illustrative worker allocation is 256 MB sort, 64 MB merge, 128 MB I/O, 128 MB row/ID caches, 32 MB output and 192 MB runtime/catalog/residency allowance, plus the explicitly computed heaps. This is **800 MB of reservations, not measured peak RAM**; any actual excess is charged. It gives no deadline guarantee and does not establish that OS/other use fits 1 GB.

### A Structurally Different Winning Shape

Let every target be `P union {private_j}`, with distinct private features outside a shared core. Every source set is unique, so D10 cannot collapse them. For source `i`, only target `i` has nonzero edit delta relative to `P`; self-exclusion removes it. All remaining targets tie and the answer is the first k original IDs other than `i`. A certificate with `h=k+1` proves this without a per-source scan of all common-feature postings. The anchor is reused across genuinely unequal sets; equal core cardinalities give a full positive Jaccard rank domain.

For `n=10M`, `k=10`, `h=32`, one certificate has only 32 triples, but preparation still consumes the complete incidence input and full export still emits 100M rows. With 24-byte output records, that export is 2.4 GB, not 768 bytes. This is a constructed shape, not a measured improvement over a competent factor-aware or incremental baseline.

### Refresh And Recovery

A source-only edit against an unchanged target snapshot reuses the certificate after the query gate. If the source is also in the target universe, changing that node is a **target** change too: the old certificate is not automatically valid for the new snapshot.

Any target content, cardinality, eligibility or ID-order change invalidates the anchor's universal dominance claim. This initial design rebuilds affected certificate generations by full verification, or routes queries to the baseline while a new certificate is built. It does not assume a reverse dependency catalog for free. A single removed common feature can invalidate many queries and scan long postings. Revalidate all denormalized cardinalities and run identities before publication.

The manifest binds `(target membership/count generation, posting generation, eligible-ID universe, original-ID ordering, projection, metric)` together. The certificate references that manifest and its own immutable anchor. A shared source additionally references the same source/target epoch; an external source may have its independent explicitly pinned version. A new source value paired with old target postings is neither an old-snapshot nor a new-snapshot self-join.

The executed refresh witness has `P=S={0}`, old targets `10:{0,1}`, `20:{0,1,2}` and `h=k=1`. Replacing target 20 by `{0}` leaves the source-edit stream empty but makes 20 the winner. Forging away the token guard returns the incorrect old winner 10; the real path returns 20 through fallback. The same test retains a valid old reader, checks old/new self-excluded score witnesses for source 20, and rejects mixed shared-source and posting epochs. No selective certificate-refresh catalog is asserted.

Build/query/output are version pinned. Checkpoint the bounded stage and committed output boundary; an interrupted unpublished edit reduction may be regenerated. Hold old blocks only while readers need them and charge old/new overlap. A complete result is published only after all its rows are finalized. A slow sink owns only its admitted chunk; cannot detach a small slice while retaining an uncharged large buffer.

## Revised After Counterexample

| Tempting proposal | Exact challenge | Revision |
| --- | --- | --- |
| Reuse anchor top-k when no edited feature occurs in any target | `P={0,1}`, `T0={0}`, `T1={0,1,2,3,4}`. Scores are `1/2 > 2/5`. Add private features 5,6: scores become `1/4 < 2/7`, although both intersections are unchanged. | Store the exact affine rank domain; at cardinality 3 the ID tie matters, and at 4 the one-witness certificate is invalid. |
| Rescore changed targets and keep an arbitrary old `k+1` list | All its members may have nonzero delta or include self. An unseen unchanged target may become necessary after these deletions. | Require at least `k'` **unchanged, nonself** witnesses, or fall back. |
| Only enumerate positive candidates | Disjoint sets still require exactly k zero-score targets, in ID order. | Certificate construction includes zeros; empty sources and complete fallback cover the entire eligible universe. |
| Use the union of touched postings as the logical changed set | Adding `f` and removing `g` may cancel on a target containing both. Conversely, processing only one sign corrupts intersections. | External signed reduction defines `C` by nonzero *net* delta; posting work before cancellation remains charged. |
| Store all rank crossings or all anchor overlaps | `A*n` overlap cells and up to quadratic pair crossings can overwhelm 50 GB. | Retain only `O(A*h)` certificate entries and one interval per anchor; pay exact changed-target intersections at query time. |
| More witnesses always improve eligibility | `P={0,1}`, targets `0:{0,1}`, `1:{0}`, `2:{0,1,2,3,4}`: `h=1` admits all positive sizes; `h=2` only sizes through 3. | Tune domain width and survivor slack jointly. This is a sufficient domain, not maximal top-k reuse. |
| A target update is harmless when source edits are empty | The version witness above changes the winner with `C=empty`. | Validate the whole snapshot before computing `C`; rebuild or really execute fallback. |

## Evidence And Rejection Tests

The exact finite probe in [the evidence ledger](similarity-embeddings-Evidence.md#similarity-probe) checked 3,456 integer interval cases and 285,696 signed-delta identities. Across the declared finite fixture, 121,348 returned answers matched exhaustive rational ranking: 63,460 accepted transfer cases and 57,888 special empty/zero-k cases. Of accepted transfers, 61,930 used unequal source and anchor sets. Another 99,836 cases required fallback; this is not a high-acceptance claim. Binary cosine was checked through its exact squared ordering. A separate nonnegative weighted-cosine probe checked 1,422 returned answers over three-dimensional vectors with weights 0/1/2: 1,179 accepted transfers and 243 zero-source cases, with 5,139 fallbacks. These checks validate neither disk scheduling nor 4 GB behavior.

**New execution evidence:** 360 randomized file-backed queries, including integer-weighted cosine, now compare complete `(id,a,b,c)` answers against direct exact enumeration and a separately computed dense incremental-dot baseline. The routes include 39 accepted, 179 survivor fallbacks, 54 preflight scratch fallbacks, 3 zero-source scans and 85 zero-k results. A separate cardinality fixture executes the domain fallback. Canceled popular edits consume 2,000 postings through ten merge generations, produce zero net deltas, and score four witnesses. The nonidentical near-core fixture scores seven nonself witnesses after one posting, while strict no-exception safe reuse executes a 1,023-target scan. These are falsification/work-count results, not a hardware speedup.

### Strong Baselines And A Lifecycle Break-Even

| Matched comparison | What is shared; where the candidate must earn its cost |
| --- | --- |
| Pure safe-domain reuse | Same exact pairwise dominance construction. With `C=empty`, both reuse. With exceptions, this proposal may still reuse if enough witnesses survive. The executed strict comparator falls back when any net exception exists; it is **not** a reproduction of Hasan's spatial RangeNN implementation. |
| Full cached anchor dots | The test computes every target's stored dot plus net edit correction and compares exact outputs. This spends `Theta(n)` cached values per source/anchor and may rank them all; it is a useful correctness/state comparator, not the strongest dynamic algorithm. |
| LI-DSN-Join | Its tunable local difference indexes, reverse-neighbor lists, delta scan and indexed fallback are stronger than the dense toy comparator. A05 avoids those per-set indexes, but pays streamed anchor recovery, universal build and complete snapshot revalidation. A matched implementation may beat A05. [Sections IV-A through IV-G](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf). |
| D11/D12, exact indexed search | Keep source-owned heaps, complete zero tails, incidence preparation and output identical. Charge their reusable indexes against the same retained cap, not against A05's certificate payload alone. |

For one snapshot lifetime with accepted count `Q_a`, rejected count `Q_r`, extra certificate construction/refresh cost `B`, accepted path cost `C_a`, ordinary matched query cost `C_b`, and rejected discovery cost `C_r`, the candidate wins only if

```text
B + Q_r*C_r < Q_a*(C_b-C_a).
```

Use measured total time or a consistently defined byte/work cost, not mixed units. The near-core query example excludes its already paid build. The cancellation example proves why `|C|=0` cannot substitute for `C_a`: it incurs 336,128 read bytes and 302,848 write bytes in the tiny reducer/query, before metadata allocation/page amplification. Frequent shared-target refresh makes `B` recur and can eliminate any crossover. Current tests do not time a complete production preparation/refresh/output lifecycle.

Required next experiments, with the whole lifecycle timed and bytes measured:

- **A05-R1:** all-common, all-disjoint, duplicate-empty, interleaved-bin and tied-positive targets; compare exact ordered IDs and `(a,b,c)` witnesses with the complete baseline.
- **A05-R2:** nonidentical near-core families versus random sets; sweep edit posting volume independently of edit count. One globally popular edit can make `P_D=Theta(n)`.
- **A05-R3:** sweep `h`, anchors, target-size diversity and refresh cadence. Report accepted-query fraction and first-answer, repeat-query and refreshed-answer time including certificate preparation.
- **A05-R4:** compare All-Pairs/L2AP as applicable, transformation-based exact kNN, D11/D12, and LI-DSN-Join-style incremental state. Match zero/tie contracts; do not infer a victory from their different defaults.
- **A05-R5:** adversarial invalidation, full-universe replacement, exhausted scratch, a huge single set, slow output and pinned old readers. No partial result may be reported as a completed top-k.

The research claim is rejected if safe intervals collapse, winner-survival slack rarely suffices, or `build + queries + output + refresh` does not improve over the best matched baseline before the snapshot expires. In particular, an expensive failed gate followed by the baseline is a loss, not useful speedup.

## Final Synthesis And Hard Gap

The surviving candidate is a **bounded cross-source ranking certificate with exact net-edit correction**, not another similarity sketch or intersection summary. Its proof covers full zero tails, self-exclusion and deterministic ties, and it admits sources that are not equality-class members. The practical and novelty gap is substantial: expensive universal certificate construction must amortize on a real near-core workload, and the precise combination must be distinguished from the dynamic set-kNN and safe-region literature. No source-to-answer 4 GB measurement or global novelty result has been obtained.

## Additional Architecture: Runwise Shared Selection

The new [runwise selection manuscript](Similarity-Runwise-Selection.md) changes the representation, not this certificate's proof. For exact binary Jaccard on a frozen target order, source feature postings encoded as intervals produce a piecewise-constant intersection field by external endpoint summation. Two fields in one shared disk RMQ tree select minimum `(cardinality,original-ID)` for positive-count runs and minimum original ID for zero runs. Heap-based winner extraction/splitting returns the complete exact top-k without per-anchor target tables or an expanded changed-target list.

With L source posting intervals, R coalesced intersection runs and requested k'>0, selection needs at most R+k' heap entries and R+2k'-1 RMQs after pre-selection self removal. All L intervals and endpoint sorting still count. A file-only sequential-level builder avoids an n-entry RAM construction; the global tree is retained on disk. The separate finite-file probe was replayed by the lead: 2,688 oracle answers and 2,280 direct RMQs passed. In a nonidentical 2,048-target family, q=n relative to an empty anchor but L=R=1; top-10 used 19 RMQs. This establishes a query-work alternative to target expansion, not superiority over an equally run-aware retrieval engine.

| Option | Extra retained organization | Eligible small-work condition | Main loss |
| --- | --- | --- | --- |
| Cross-source certificate/envelope | Compact anchor witnesses and domains | Enough winners survive after paid edit correction | Rejection/fallback and broad edit postings |
| Complete exception merge | Linear target bucket tables per anchor | Affordable anchors and sparse net exceptions | Per-anchor retention and refresh |
| Runwise shared selection | One shared two-order RMQ plus compressed interval postings | Few queried intervals and overlap runs | Fragmented ordering, sorting, random RMQs, heap admission |

The last option currently covers binary Jaccard only; prior cosine contracts are not silently inherited. Same-run RMQ execution reproduces its bound, and RMQ splitting is explicitly prior art. Useful target ordering, paid preparation/refresh and a stronger scientific delta remain the next questions.

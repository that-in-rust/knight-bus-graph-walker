# Similarity: Selective Block Access And Matched Certificate Costs

Date: 2026-09-20. Implemented research probe, completed independent mathematical review and serial public-data comparison. Not a new inverted-index claim.

## Motivation

The [paired-capacity theorem](Similarity-Paired-Capacity-Recovery.md) gives a strictly stronger exact block envelope, but scanning every summary costs more than the row reads saved. The previous serial experiment loses to DAAT on every query. This study must remove irrelevant summary reads and strengthen the comparator before interpreting the discrete bound's usefulness.

Three alternatives are explicit: retain the old sequential summary scan; obtain source-relevant blocks by merging feature-to-block postings; or rely entirely on ordinary target DAAT. The second is the candidate access schedule, while the third remains the independent practical control. Token-group indexing is established, not the proposed scientific contribution.

## Paid Selective Index

For block size B, map each already sorted target posting interval [lo,hi) to [floor(lo/B),floor((hi-1)/B)+1). Coalesce overlapping or adjacent mapped intervals per feature. This reads the paid original postings and directory once, with O(1) interval coalescer state. A standalone path from canonical rows would first need an external feature sort; the incremental conversion does not make that work free.

Store a feature directory plus mapped block intervals. Replace each variable-length union summary with a fixed-stride record containing the old row offsets/extrema/ID minimum, all feature-tree capacities and G union-population counts. The population counts are now retained and charged instead of being recomputed by scanning the union during every query.

The query streams the source's admitted g block-posting cursors in block order. Each feature occurs at most once per block. Thus group-by-block supplies exact query-restricted leaf counts without reading unrelated unions or retaining a target-count-sized dictionary. Read one fixed metadata header per encountered block, then fetch only the bound fields needed by the selected mode. Blocks with zero overlap are not visited for positive scoring.

```text
source features
      |
      v
g sorted feature-to-block streams
      |
      v
merge + one current block histogram
      |
      v
header -> upper bounds -> interval bound -> paired bound
      |        prune at any certified stage
      v
exact surviving rows -> bounded positive top-k heap
      |
      v
if needed: original-ID zero tail -> complete staged output
```

Only positive exact rows enter the heap during block scoring. This distinction matters: unrelated blocks may contain lower-ID zeros. After exhausting all positive blocks, if fewer than k' positive results exist, every eligible positive row is retained, so the original-ID index can provide the remaining zero rows while skipping those retained IDs and self. A heap already full of positive witnesses requires no zero tail.

## Stronger Same-Information Interval Control

For a block of p rows, exact union population H_v and per-row upper capacity C_v imply a necessary lower bound Lraw_v=max(0,H_v-(p-1)C_v). The other p-1 rows cannot cover more than (p-1)C_v of the union. For complementary pairs this recovers the exact minimum H_v-C_v.

The control optimizes all independent nested lower/upper occupancy constraints exactly, not just a loose sum of minima. It is deliberately allowed the same paid populations and capacities as the paired candidate.

For a subtree, represent its best query overlap at occupancy x as F(x)=min(x-d,c) on a feasible integer interval [L,U]. At a leaf this follows from F(x)=min(x,q) and the leaf quotas. Child functions have marginal gains consisting only of ones followed by zeros. Combining two children gives:

```text
L0 = L_left + L_right
U0 = U_left + U_right
d  = L0 - F_left(L_left) - F_right(L_right)
c  = F_left(U_left) + F_right(U_right)
L  = max(L0, Lraw_parent)
U  = min(U0, C_parent)
F(x) = min(x-d,c), L <= x <= U
```

Clip the root also to [ell,u]. For a two-row block the actual row sizes are exactly ell or u, so the control evaluates both feasible endpoints, not every intervening size. For larger blocks the score increases until x=d+c and decreases afterwards; choose x=clamp(d+c,L,U). All arithmetic is exact. Empty overlap scores zero. This is ordinary laminar quota optimization, not a new DP claim; the proposed formula is subject to independent challenge and enumerated feasible-row tests.

The discrete pair method still differs: its occupancy must equal one of the two attained endpoints at EVERY node. A query count of (2,2) inside two [1,3] leaves can be feasible for the interval control and infeasible for the pair. Both get the same selective access path. The completed review supports the interval formula. Retain d,c from before parent clipping; at the next level evaluate at the clipped endpoints. For populations above two, checking only ell and u would miss interior optima, now covered by a specific regression.

## Verification-First Plan

1. Enumerate feasible lower/upper-quota rows and compare the exact interval formula, including root endpoint restrictions.
2. Build the finite-file selective index; check coalesced block postings, fixed metadata sizing, disk refusal and publication cleanup.
3. Check complete outputs against the direct-set oracle, especially sparse queries, zero-only sources, self-only positive matches and tie order.
4. Compare union, leaf, laminar, interval and paired filters on the same index, then compare all to ordinary DAAT. Expensive filters run only after a full exact positive heap exists and weaker gates fail.
5. Run timing only after all tests and review agents are terminal. Retain build costs, index bytes, logical reads, complete output and negative results.

Expected query state is O(a+g+G+k') objects, not a physical RAM claim. Body scoring and output costs remain. Interval expansion is charged as block memberships, not magically compressed constant work. The builder's input is a previously normalized/indexed snapshot and a bounded-union sidecar; both preparatory stages are paid separately. Atomic directory publication is not fsync durability or concurrent refresh support.

## Storage And Access Model

Let P=ceil(n/B), G be the leaf count, F the number of features and L_B the number of coalesced block posting intervals. The new retained index has:

```text
fixed metadata: 40 + P * (56 + 8*(3G-1)) bytes
block postings: 16 * L_B bytes
feature directory: 24 * F bytes
```

Canonical target metadata/pairs and the original-ID inverse index remain required for scoring and zero tails. The old target-posting index, RMQ and union sidecar are preparatory inputs/alternative-plan artifacts; they are not required by the selective query after conversion. Their coexistence during preparation, replacement and experiments remains paid. An optimized standalone compiler is not implemented.

All bound modes share the same full retained metadata for a matched incremental experiment, but simpler query modes do not read fields they do not need. Union-plus-length reads only the fixed header; partition reads only leaf capacities; laminar reads all capacities; interval/pair additionally read union populations. A standalone simpler index could omit unused retained fields. Thus this is not a claim that the full index is space-optimal for every mode.

The query first collects exact block histograms from g-way posting merge, then uses a staged filter: union/length, selected upper-capacity bound, exact interval bound, and finally the discrete pair bound when eligible. An expensive stage runs only after a full exact positive heap exists and earlier stages fail to reject. Never evaluate an unneeded expensive bound solely to improve a diagnostic counter.

## Membership-Expansion Obstruction

For two-row blocks B={T1,T2}, define expanded query memberships before self exclusion:

```text
W_target = sum_B (|S intersect T1| + |S intersect T2|)
W_block  = sum_B |S intersect (T1 union T2)|

W_target - W_block = sum_B |S intersect T1 intersect T2|.
```

This is inclusion-exclusion per block. On an eligible disjoint pair, the difference is exactly zero. The domain making the exact pair theorem cheap therefore cannot itself justify a reduction in expanded posting memberships. It can still reduce candidate block count, improve interval compression, avoid exact row verification or save other per-target work. These are separate claims.

For example, a feature present at target positions 0,2,4 has three singleton target intervals. With B=2 it occupies consecutive blocks 0,1,2, represented by one block interval. Both representations still expand three feature memberships. A regression preserves this distinction. A broad claim that grouped postings always reduce source work by B would be false.

## Primary Comparison

The lead inspected [Zhang et al.'s author manuscript](https://www.jinwang18.net/files/tkde19-setknn.pdf), Sections 4-5 and its query-node bound, in addition to the earlier reviewer inspection. It uses feature-count vectors, bounding rectangles and exact set-search pruning, and explicitly discusses filtering cost overwhelming cheap verification. Our laminar interval calculation is a strengthened matched control, not a reproduction of the full R-tree/partitioning system. This source supports treating count-based filtering as established and measuring its complete cost; it does not establish priority for the particular joint-attainment recovery theorem.

## Evidence Status

Sources: [implementation](experiments/probe_selective_capacity_similarity.py), [tests](experiments/test_selective_capacity_similarity.py), [driver](experiments/bench_selective_capacity_similarity.py), [raw receipt](Similarity-Selective-Access-Results.json), and [completed independent review](Similarity-Selective-Access-Review.md).

Five missing-function failures preceded implementation. A sixth missing-driver failure preceded the public driver. The final combined similarity suite contains 43 passing tests, including eight new selective tests. The new pure checks cover 4,352 two-row and 160 three-row interval cases plus the interior-optimum regression. The file matrix compares 10,240 complete ordered outputs to a direct-set oracle over 64 three-target fixtures. Sparse access, self-only positives, all-zero queries, zero-tail completion, interval coalescing, source/cursor/group/heap/output budgets, failed publication cleanup and benchmark-receipt structure are separately exercised.

The independent reviewer accepts the interval theorem and conditional selective-access proof. Its ephemeral checks include 17,280 arbitrary-quota cases, 30,976 population-derived quota cases, 2,044 posting projections and 163,840 zero-completion identities. These are reviewer-reported algebra checks, not independent execution of our Python files. The implementation's coverage and resource behavior remain supported by the lead's finite tests, not a separate code review.

## Serial Public Comparison

The retained run starts after the 43-test process and reviewer are terminal. It executes six modes on the same 30 ca-GrQc source IDs, with three rotated-order repetitions: 540 complete outputs, all equal to the independent direct-set oracle. B=2, G=4, k=10, source-feature cursor cap 128, source-vector cap 4,096, and heap capacity ten. Every selected source is admitted; no silent feature truncation occurs.

This is serial relative to this task, not a dedicated host, CPU-pinned trial or cold-cache experiment. Source creation and oracle evaluation are excluded from query timing; source reads, posting merge, metadata, filter arithmetic, surviving bodies, staged output, test readback and cleanup are included.

| Mode | Sum of 30 query median times, ms | Exact body targets | Exact body memberships |
| --- | ---: | ---: | ---: |
| Selective union plus lengths | 98.531 | 1,566 | 20,622 |
| Selective leaf partition | 105.949 | 1,566 | 20,622 |
| Selective laminar upper bounds | 107.055 | 1,566 | 20,622 |
| Selective exact interval bound | 109.181 | 1,276 | 15,184 |
| Selective paired bound | 111.366 | 1,268 | 15,137 |
| Ordinary target DAAT | 91.233 | Different operation: 1,446 positive targets scored | No canonical body scan |

These sums of medians are descriptive equal-query-weight aggregates, not measured batch elapsed times or population statistics. No confidence interval is claimed. Query-level medians on the original six strata are:

| Source ID | Degree | Union, ms | Interval, ms | Paired, ms | DAAT, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 12295 | 0 | 0.507 | 0.502 | 0.500 | 0.516 |
| 2307 | 2 | 2.441 | 2.757 | 2.843 | 1.207 |
| 10910 | 3 | 5.093 | 6.107 | 6.350 | 1.795 |
| 5862 | 6 | 2.116 | 2.449 | 2.541 | 2.003 |
| 21281 | 79 | 18.069 | 18.674 | 19.434 | 21.640 |
| 21012 | 81 | 19.684 | 19.905 | 19.917 | 22.569 |

The selective union mode is faster than DAAT on 13 sampled query medians; paired is faster on 12. Some differences are tiny, especially empty/small queries with identical effective work, so these counts are not robust win probabilities. In both high-degree examples the ordinary union-only selective plan is faster than paired. The paired-vs-DAAT crossover therefore cannot be attributed to the new discrete theorem.

### What Changed In The Attribution

All selective modes visit the same 1,436 positive-union blocks and emit 7,237 feature/block memberships across the 30 queries. The old sequential summary path visited 78,630 block summaries. This substantial access reduction is ordinary token-group indexing and is credited as such, not as a new graph algorithm.

The exact interval control accounts for almost all of the remaining body-read benefit. Paired evaluates its extra DP 377 times and obtains a strictly tighter envelope 13 times, but eliminates just four additional blocks: eight target bodies (0.627%) and 47 memberships (0.310%) beyond the interval control. The four affected source IDs are 727, 24332, 3730 and 12968. This is a real finite separation, but not the earlier 22.5% differential against the weaker upper-only schedule.

DAAT expands 7,260 matching memberships, only 23 more than grouped access (0.317%). The group directory reads 6,989 interval records across these queries versus DAAT's 7,136; neither count equals the expanded membership count. This directly measures the query-weighted effect that the 97.8% unweighted eligibility percentage alone could not establish.

### Retained Bytes And Preparation

| Selective index component | Bytes |
| --- | ---: |
| Fixed full metadata | 377,464 |
| Block posting intervals | 452,048 |
| Feature directory | 125,784 |
| New selective index total | 955,296 |

The query additionally needs canonical target metadata (83,872 bytes), canonical feature/owner pairs (463,488), and inverse IDs (83,872): 1,586,528 retained bytes for this full selective plan. Ordinary DAAT's required target postings, directory, target metadata and inverse IDs total 751,224 bytes. This is a comparison of these serialized research formats, not optimal compressed representations or measured resident RAM.

Shared canonical-file creation took 42.061 ms and the existing shared Snapshot index build 268.165 ms in this run. The intermediate full union sidecar took 94.203 ms and retained 524,056 bytes. Selective conversion then took 182.586 ms and retained the 955,296 bytes above. All are one observed run, not stable build estimates. The shared Snapshot includes an unused RMQ, so do not charge its whole build as an optimized standalone DAAT cost. Conversely, conversion from that paid source is not a free standalone compiler for the new index.

The block index has 28,253 posting intervals versus 28,606 original target intervals. The builder converts/coalesces compressed intervals without expanding every target ID. It streams the paid intermediate unions to derive populations with O(G) conversion state; the upstream bounded-union builder still has its own capacity and refusal rules. The new immutable-generation path refuses to overwrite an existing published directory. Hot refresh, fsync durability, adversarial corruption handling and a physical 4 GB cap are not implemented or benchmarked.

## Rubber-Duck Outcome

1. The old plan was slow partly because it read unrelated metadata; a competent selective access repair removes that cost.
2. The improved access path helps both the proposed bound and established simpler bounds. It is not evidence for novelty of the pair theorem.
3. A stronger interval comparator explains almost all of the previous body-read headline. This correction is required even though the pair theorem itself remains correct.
4. Exact pair information still eliminates four additional blocks here, but its extra arithmetic and larger retained plan do not establish a useful overall advantage over native DAAT.
5. The sparse coauthorship fixture does not demonstrate the practical workload prevalence needed for a paper contribution or product claim.

Do not continue reporting the weaker-comparator 22.5% figure as the incremental gain of the paired theorem. Its measured same-access, strong-interval differential here is 47 body memberships, without a total-time win in the descriptive aggregate.

## Reviewed Further Mathematical Option

The independent review analyzes a broader two-row model with overlap I=ell+u-|U|. A node state contains shared-feature count e and first-row occupancy x. The exact admissible states are:

```text
0 <= e <= min(I, 2*C_v-H_v)
x in {C_v, H_v+e-C_v}
```

At a leaf the attainable overlap is min(q_v,x). Merge children by adding e and x and maximizing query overlap. At the root require e=I and x in {ell,u}. This has O(G*(I+1)^2) transitions and O(G*(I+1)) stored states under the described pairwise-child loop.

The local e bound is essential: the earlier unfiltered proposal admitted a size-two query as score one where every compatible pair has maximum score 1/3. The review preserves this counterexample and verifies the corrected model in 33,792 ephemeral cases. This extension is mathematically reviewed but NOT implemented in this experiment. Its potential cost grows with I; the present query-weighted overlap saving is only 23 memberships, so implementing it on this same fixture solely to chase a win is not justified by current evidence.

The next useful work is either a meaningful corpus/regime with a measured information/access advantage, or a stronger theorem that compresses the overlap-state frontier without discarding exactness. Both require strong interval and posting controls. Existing primary precedents remain credited; publication novelty and the original seven-family goal remain open.

## Reproduction And Recovered Checkpoint

Run from the repository root with Python 3.11:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
python3.11 research_algorithms_20260920/experiments/bench_selective_capacity_similarity.py --data research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz --output /tmp/selective-replay.json
```

After an interrupted context, the former final test process handle was authoritatively missing. A fresh suite run passed all 43 tests in 15.545 seconds. The earlier 15.301-second run preceded the final interior-optimum assertion; neither duration is a performance benchmark. The retained public receipt was inspected, not regenerated to seek different timing results.

| Artifact | SHA-256 at recovered checkpoint |
| --- | --- |
| Selective probe | `afeff96f1615b3a90d1a954bc8a4389d3aeb5562c6c090f7de032ad77c308e10` |
| Selective tests | `33dfbb3a3c68f8a675e8d9adb630cabc6bf96a699541dc9377f1b2a3801bf840` |
| Public driver | `a5e01c1916d9a80a779db678df05cbef0d4b1dfa8cc5bcba7f27988e3061073b` |
| Serial public receipt | `1c760d04425f164efff9bce8eacd3ff295f4135d554e3c5f496467fea5f3dcae` |

These hashes cover the indicated files, not a claim that the prior external review executed them. No benchmark against Neo4j/GDS or physical RAM-cap experiment was performed.

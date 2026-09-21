# Similarity: Laminar Capacity Certificates Before Data Access

Date: 2026-09-20. A05 research continuation. Status: implemented finite-file probe, completed mathematical review and negative public-data comparison. Not a claim of a new matroid or publication readiness.

## Question And Choice

The previous run-bound experiment pruned initial RMQs but paid the complete overlap field first. Ordinary bounded document-at-a-time (DAAT) posting merge beat it on all 30 sampled ca-GrQc queries. The next candidate must remove source work, not only work introduced by its own representation.

Three options are considered on the same target blocks:

1. Exact block union plus minimum/maximum row cardinality. This is a strong simple control.
2. The same union plus per-feature-partition maximum occupancies.
3. The same union plus nested-group maximum occupancies. This is the candidate, with the first two as ablations and ordinary DAAT as an end-to-end control.

The nested option retains correlations at several resolutions. It is chosen for a falsifiable algebraic advantage over the two fixed summaries, not because that advantage establishes a new retrieval algorithm. An ordinary optimal tree-cut bound can match it.

## Semantic Contract

Targets are binary sets, each with one distinct signed 64-bit original ID. Features are unsigned 64-bit IDs. Physical target order need not match ID order. The source is a sorted unique feature file. Jaccard is intersection divided by union; a zero intersection, including empty/empty, has score zero. Return exactly min(k, eligible target count) results, excluding the source ID if present, sorted by decreasing score and then increasing original ID. Unknown source features count in the source cardinality and therefore in denominators.

Each query sees one immutable, coherently versioned snapshot. Weighted similarity, multisets, live mutation, all-pairs output and arbitrary Cypher are outside this probe's contract.

## Stored Information

Partition targets into fixed consecutive blocks B. For every nonempty block store:

- U: exact union of the target feature sets.
- ell and u: minimum and maximum target cardinalities.
- minID: minimum original ID, including self when present (conservative).
- A fixed laminar hierarchy over feature groups.
- cap(v) = max over T in B of |T intersect group(v)|.

The initial hierarchy uses G power-of-two leaf buckets, feature modulo G, with a complete binary tree over consecutive bucket numbers. This deterministic choice has no training cost and no guarantee of semantically useful grouping. Root capacity is u. Targets themselves remain in the existing canonical row file.

```text
                      ROOT: at most u features
                         /                 \
              GROUP {0,1}                 GROUP {2,3}
              at most cL                  at most cR
                /     \                     /     \
             leaf 0  leaf 1              leaf 2  leaf 3

Source -> intersect with block union -> leaf counts
       -> bottom-up capacity clipping -> overlap bound
       -> Jaccard bound -> skip block OR read exact rows
```

This is a hierarchy of FEATURE groups inside a block, not merely a hierarchy of target blocks.

## Exact Row-Relaxation Envelope

Let a=|S|. Let q[j]=|S intersect U intersect leaf(j)|. Compute:

```text
rank(leaf j) = min(q[j], cap(j))
rank(v)      = min(cap(v), rank(left(v)) + rank(right(v)))
r            = rank(root)
UB           = 0                              if r = 0
UB           = r / (a + max(ell,r) - r)       otherwise
```

### Safety

Every actual target satisfies the nested capacity constraints. Its intersection c with S satisfies c<=r. Its cardinality b satisfies b>=max(ell,c). Thus its score is at most c/(a+max(ell,c)-c). This function is nondecreasing in c on 0<=c<=a, giving UB. The rank recurrence is exact by induction: child feature domains are disjoint, their maximum independent subsets can be combined, and the parent truncation can discard arbitrary excess elements.

### Attainment In The Specified Relaxation

Laminar capacity constraints define a matroid M on U. Root capacity is u, and an actual maximum-size target witnesses rank(M)=u. Choose an independent subset I of S intersect U of size r. By matroid augmentation, I can be extended to size max(ell,r)<=u. No extension element can lie in S, since r is already the maximum independent S-subset size. The resulting row has overlap r and cardinality max(ell,r), attaining UB.

The tightness claim is ONLY over individual rows contained in U that satisfy these upper capacities and the cardinality range. It is NOT a claim about every fixed-population block dataset that preserves every exact attained maximum, union, ID assignment and minimum simultaneously. Such joint facts can further restrict possibilities. No global information-theoretic optimality is claimed.

### Controls And Separation

The union-plus-length control uses r0=min(|S intersect U|,u). The flat control uses rp=min(u,sum_j min(q[j],cap(j))). Their bounds use the same denominator formula. Always r<=rp<=r0, hence the nested bound is no larger.

Example: target rows {0,2} and {1,3}, query {0,1}, G=4. All rows have size 2. The union and leaf bounds are 1. The internal group {0,1} has capacity 1, yielding UB=1/3, attained by both rows. A flat partition that already groups {0,1} together also gets 1/3; this example separates the specified leaf partition, not every possible partition.

The recurrence is also the minimum cost of an adaptive cut through the group tree: a node can be covered by its capacity or by both child covers. This known dynamic-programming interpretation prevents relabeling ordinary adaptive bounds as a novel rank algorithm.

## Exact Top-k Pruning

Scan blocks in physical order. Maintain at most k' exact eligible results in a heap; no oracle seeding and no unpriced bound sorting. A block can be skipped only after k' distinct exact witnesses exist and either:

1. UB is strictly below the current kth score; or
2. UB equals that score and minID is greater than the current worst retained ID.

Otherwise read its canonical rows, compute exact intersections, and update the heap. Unread blocks never contribute predicted scores as witnesses. Self-exclusion happens during exact row evaluation. This also handles zero-score tails: a full heap of zero scores can discard later blocks only when their minimum ID loses the tie.

## Finite-File Plan

The builder streams normalized metadata and owner-grouped feature pairs once. It retains a bounded block union and O(G) occupancy arrays, never a target-count-sized overlap array. A block that exceeds the declared union-entry budget is refused before inserting the excess entry. The probe does not silently introduce an external union builder.

Sidecar layout:

```text
global: magic, n, G, block_size, epoch
block:  lo, hi, pair_byte_offset, ell, u, minID, union_count
        2G-1 capacity words
        sorted union feature words
```

All words are 8 bytes; minID is signed. Global header is 40 bytes and block header 56 bytes. For P blocks and total block-union cardinality H, retained sidecar bytes are 40+P*(56+8*(2G-1))+8H. The union and partition controls have specialized formats storing zero and G capacity words respectively, rather than being forced to read the candidate's full table. They recover root capacity from the block header. Because the target blocks are disjoint, H<=m, where m is total target membership count. The sidecar is additional to canonical source rows, not a replacement for them.

Build state is O(union_cap+G) Python objects. Sorting the union adds a simultaneous list of references, plus sorting workspace. Query state is O(a+G+k') Python objects, plus bounded record buffers and staged output. The test sink reads k' output records into a list; this is charged and is not a large-result streaming client demonstration. Object counts and wire bytes are NOT physical process RAM.

The direct builder costs O(m+nG+sum_B |U_B| log |U_B|) operations in the full hierarchy mode. It computes row counts before taking maxima; summing child maxima would be wrong. Replacement admission counts both the existing sidecar and the staged new sidecar. Publication uses same-filesystem rename but no fsync/crash-durability protocol. Generation checks assume the caller supplies an immutable coherent snapshot; they are not cryptographic protection against files mutated in place. The executable is an assertion-based research probe, not a hardened parser/service.

The query streams each block union, counts matching source features by bucket, evaluates its selected bound, then skips or reads row bodies. Deterministic binary search in the admitted source vector counts exact target intersections. Work includes metadata reads, union scans, source membership comparisons, exact row reads, heap updates, output and cleanup. No field is constructed. In the worst case all rows and all summary unions are read, making this strictly additional work relative to an ordinary row scan.

## Research Execution Plan

1. Add failing tests for exact rank/envelope, full output, admission, failure cleanup and a block whose body must not be read.
2. Implement the pure envelope, bounded sidecar builder and three-mode query in `experiments/probe_laminar_capacity_similarity.py`.
3. Compare the envelope against independently enumerated feasible rows, not only actual block members.
4. Compare complete query outputs against the existing direct-set oracle, including empty data, unknown tokens, ties and self exclusion.
5. Run a public-data comparison against DAAT and both summary controls. Include build time, sidecar size, body bytes and all query phases. Do not infer speed from blocks skipped.
6. Record counterexamples, costs and negative outcomes; update the portfolio and journal. Do not commit/push or mark the seven-family goal complete.

## Primary Precedents

- [LES3, Li, Yu and Koudas, PVLDB 2021, Section 3](https://arxiv.org/html/2107.10417): token-group membership supports union-based Jaccard upper bounds and group pruning. Its target partitioning and compressed matrix are strong nearest predecessors. This probe is not a reproduction of its learned partitioning, bitmap implementation or reported performance.
- [Fife and Oxley, Laminar Matroids, 2016 preprint, introduction](https://arxiv.org/html/1606.08354): nested upper-capacity constraints define matroid independent sets. The primitive is explicitly established prior art. Our proposed adaptation is the cardinality-constrained Jaccard envelope and paid block workflow, not the matroid fact.
- [Low and Zheng, Fast Top-K Similarity Queries via Matrix Compression, CIKM 2012](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/short_topk.pdf): compressed group summaries and norm bounds for top-k inner products are another relevant predecessor. It is not a measured same-contract comparator here.

Primary sections were inspected online on 2026-09-20. The Fife/Oxley HTML rendering shows an inconsistent body date; use the arXiv identifier/submission history, not that rendering date, for priority.

## Failure Hypotheses

- Union/capacity metadata may cost more to read than it saves.
- Arbitrary feature IDs may put useful correlations across crossing groups.
- Large blocks may have nearly vacuous capacities; small blocks may have excessive header overhead.
- Sparse queries may already let DAAT touch very few targets.
- A minimum-size bound can be weakened by one tiny row; an upper capacity can be weakened by one dense row.
- Sorting/group learning may improve pruning but adds build state, time, provenance and refresh obligations.
- Bounds on individual rows do not capture every joint block constraint.
- A successful synthetic example does not establish prevalence on customer graphs or novelty over competent filtered retrieval.

## Completed Independent Challenge

The [independent review](Similarity-Laminar-Capacity-Review.md) was completed, read and integrated. It supports the exact individual-row envelope, independently checks 266,240 six-feature block/query cases, and derives equality to the ordinary optimal adaptive tree-cut DP. That enumeration is reviewer-reported; it is not a lead rerun of the code probe.

It gives an eight-feature fixed-population counterexample: the row relaxation returns 1, while every complete two-row block preserving all observed facts has maximum score at most 3/5. This does not invalidate safety. It motivates the separately specified [paired-capacity recovery](Similarity-Paired-Capacity-Recovery.md), which recovers discrete occupancy constraints on eligible two-row blocks.

The review also identifies a closer count-vector/R-tree precedent, [Zhang et al., A Transformation-based Framework for KNN Set Similarity Search](https://www.jinwang18.net/files/tkde19-setknn.pdf), and explains why lower/upper count rectangles may be stronger than upper-only summaries. This source was inspected by the reviewer; no claim that this probe reproduces that complete system is made.

## Executed Verification

Source: [probe](experiments/probe_laminar_capacity_similarity.py), [tests](experiments/test_laminar_capacity_similarity.py), [driver](experiments/bench_laminar_capacity_similarity.py). Seven explicit missing-function failures preceded implementation; specialized wire-format and public-receipt tests each failed before their implementation, then nine tests passed in 2.806 seconds. The later paired extension expanded the shared tests; see its current evidence section for the combined suite.

The pure-envelope test enumerates 136 unordered two-row blocks over four features, 32 source choices including an outside feature, and every feasible relaxed row: 4,352 exact envelope comparisons. The file tests check complete ordered answers, not only positive results or one winning ID. Additional tests enforce source/union/group/heap/output admission, preserve an old sidecar after failed rebuild, reject generation mismatch, clean up a failed sink, and deliberately reject any attempt to open a pruned block body.

## Public Result: The Extra Capacities Did Not Pay

[Retained receipt](Similarity-Laminar-Capacity-Results.json): 30 ca-GrQc sources, four modes, three rotated-order repetitions, 360 exact complete-output comparisons. The graph is the same documented 5,242-node, 14,484-simple-edge projection as earlier studies. B=64 targets per block and G=16 modulo feature groups were fixed before this run. There are 82 blocks.

| Mode | Incremental sidecar bytes | One observed build, ms | Target bodies read across 30 queries | Sum of per-query median times, ms |
| --- | ---: | ---: | ---: | ---: |
| Union plus lengths | 212,608 | 63.131 | 41,168 | 725.980 |
| Leaf partition | 223,104 | 70.857 | 41,168 | 752.855 |
| Laminar hierarchy | 232,944 | 90.889 | 41,168 | 778.388 |
| Ordinary DAAT | No such sidecar | Different index path | Not the same unit: positive posting candidates | 92.242 |

All three block modes prune the same 1,816 block visits and read the same 232,995 body memberships, summed once per query. Every query reads all 25,997 union words. Across the query set this costs 6,239,280 union bytes before surviving bodies. The hierarchy adds 610,080 capacity bytes; the leaf control adds 314,880. DAAT is faster than the hierarchy on every query. The hierarchy is also slower than both smaller-summary controls on every query.

These sums of medians are descriptive equal-query-weight aggregates, NOT measured batch time, confidence intervals, population estimates or speed guarantees. Timing includes source reads, summaries, body scoring, staged output, test readback and cleanup. It excludes source-file creation and the independent oracle. Canonical normalization/oracle fixtures are resident. Shared preparation includes an RMQ unused by both this query and DAAT, so its whole cost cannot be attributed to either standalone architecture. No Neo4j/GDS process, physical RAM ceiling, cold-storage workload or refresh benchmark was run.

Timing caveat discovered during review: tests overlapped part of this exploratory run. The complete-output and logical-work evidence is unaffected, but these wall times are not isolated measurements. The follow-on [paired study's final serial rerun](Similarity-Paired-Capacity-Recovery.md#serial-timing-correction) starts after tests and the independent agent finish. Its negative total-time conclusion survives that correction. No isolated rerun of this specific 64-row configuration is claimed.

### Rubber-Duck Correction

The bound is mathematically stronger but not stronger on these sampled threshold decisions. Paying to inspect every block union is expensive on sparse queries that DAAT already answers by reading a small subset of postings. A smaller candidate set does not imply smaller total bytes or faster completion. The current general hierarchy is therefore not an observed engine improvement, and its known rank primitive is not a new graph algorithm.

The next meaningful scientific direction is information recovered from jointly attained metadata, with strong same-information controls and an access schedule that does not pay every expensive bound eagerly. Simply raising G or renaming this as a new graph format is not evidence of progress.

# Exact Run Bounds Before Similarity Range Lookups

Date: 2026-09-20. A05 follow-on to [bounded seeding and endpoint merging](Similarity-Bounded-Seed-Selection.md). Status: implemented, independently reviewed mathematically, and compared on public data. Scope remains exact full top-k binary Jaccard under the existing frozen-snapshot, self and ID-tie contract. The seven-family scientific goal remains active.

**Main finding:** the strict run bound is correct and removes 1,770 of 2,855 initial RMQs across 30 sampled sources. It improves the existing implementation, but an ordinary bounded posting-merge control is faster on every sampled source. A win against a full scan was therefore insufficient evidence of a differentiated similarity engine. This negative comparison does not disprove a benefit on genuinely long-run data, and does not turn an established pruning rule into a new algorithm.

## Question And Selected Experiment

The preceding executor reads all source posting intervals and looks up an exact winner for every nonempty overlap run. Bounded seeding removes resident R state, but does not remove R initial RMQs. Can a cheap exact upper bound certify that an entire incoming run cannot affect the answer before paying its RMQ?

For source size a>0 and exact run overlap c, every target has b>=c, so

~~~text
J = c/(a+b-c) <= c/a.
~~~

Once the seed heap contains k' distinct exact heads, let tau be its worst score. If c/a<tau, every target in the new interval has k' already-seen better witnesses. Skip that interval's RMQ. Use exact integer cross-products and strict inequality; equality cannot be pruned without a valid original-ID bound. For an empty source all scores are zero and this score-only gate prunes nothing.

Chosen path: integrate this gate into the existing seed loop, preserving exact endpoint preparation, the full run scan, in-place best-first conversion and descendant enumeration. Alternatives are sorting runs by bounds (additional paid sort and startup), or building a richer block bound index (additional preparation/storage). Neither is silently included. The gate is ordinary branch-and-bound, not a proposed new generic top-k primitive. The [completed independent review](Similarity-Run-Bound-Review.md) supports its precise theorem and identifies close primary precedent.

## Verification-First Plan

1. Add explicit missing-function tests for a pruned merged route. Compare full outputs with direct set intersections, not with a shared scoring helper.
2. Preserve a fragmented family whose first exact head has score one and all remaining intervals have smaller c/a. Check one initial RMQ instead of R, while still counting all intervals/endpoints/run records.
3. Preserve equality-with-smaller-ID, insufficient-witness, zero-source and self-first counterexamples. Run a small exhaustive full-result suite.
4. Implement the gate only after those tests fail. Reuse the existing selector and merged field builder; no copied new query runtime.
5. Compare original, bounded-sort, bounded-merge, bounded-pruned-merge and scan on the same public graph, with existing six strata plus deterministic previously unselected sources. Preserve all output checks, paid preparation and explicit record budgets. Add no learned threshold from these test timings.
6. Integrate the independent theorem/prior-art review. Report actual skipped RMQs separately from still-paid field work, and keep the scientific delta unresolved when existing bounds explain the improvement.

## Stronger Comparator Before Drawing Conclusions

The 450-result run completed correctly, and the pruned route beats the full scan on the previously losing high-degree sources. That does not make the full scan the strongest baseline. The next matched control is ordinary document-at-a-time (DAAT) merging over the same source-feature postings: expand positions lazily, merge them in physical order, count each target's overlap, fetch its cardinality once and maintain bounded exact top-k. If fewer than k' positive candidates exist, use the paid ID inverse index to append the globally smallest eligible zero-score IDs, excluding already selected positives and self. This gives O(g+k') logical state without an n-sized count array, and pays every expanded source-posting membership. It uses no overlap-field file or RMQ tree for scoring.

Write failing complete-output, sparse/long-run, zero-tail and admission tests before that comparator. Preserve both the prior 450-result receipt and a fresh six-mode comparison using exactly the same 30 source IDs. The comparator is established retrieval machinery, not another invented algorithm. Source/index build and shared-file accounting must not pretend that DAAT needs the unused tree or canonical full scan.

This plan was executed: four missing-comparator tests failed, then passed after implementation. The fresh comparison completed 540 exact checked query executions. The detailed results and the revised scientific conclusion follow.

## Algorithm And Correctness

Let U be the fixed eligible nonself population, N=|U|, k'=min(k,N), and R' the nonempty initial overlap intervals after removing self. Each interval has its exact constant intersection count c. Original IDs are unique, but feature sets need not be distinct. Score order is descending exact Jaccard, then ascending original ID. Zero intersection, including empty-empty, scores zero. Unsupported eligibility changes are not applied after pruning.

~~~text
construct the complete exact overlap field, paying all endpoints
for each initial interval after self removal:
    count the interval visit
    if k' exact distinct heads have been retained and a > 0:
        tau = worst retained head's exact rational score p/q
        if c*q < a*p:
            skip this interval's RMQ
            continue
    obtain the exact head using the existing appropriate RMQ
    update the bounded worst-first seed heap with score/ID order
convert retained seeds in place to best-first order
run the unchanged winner/split enumeration for exactly k' results
publish only the complete staged output
~~~

**Envelope lemma.** Since b>=c, a+b-c>=a and J<=c/a for a>0. This envelope is attained when the target consists of exactly c source features and no additional features. Thus no uniformly tighter bound using only a,c exists under this contract. Additional valid cardinality or ID metadata can help, but acquiring it must be charged. An exact minimum-b RMQ is not a free way to avoid that same RMQ.

**Witness lemma.** Once the heap is full, every retained exact head scores at least tau. If c/a<tau, every target in the new interval is strictly worse than each of those k' targets. Disjoint initial intervals and self removal make them distinct eligible witnesses. The new interval contains no global top-k' target. This remains true when those witnesses are later replaced by better heads.

**Same-seed theorem.** Compare with the original bounded seed scan after each interval. Before its heap fills, both execute the same RMQs and updates. A newly skipped interval's hypothetical head is strictly worse than the current root, so the original scan would discard it without modifying its heap. Otherwise both execute the same exact-head comparison/update. Induction gives the identical retained seeds after every prefix. In-place conversion and descendant enumeration therefore have identical correct ordered output and the same child RMQs. This is not a new enumeration algorithm.

The test is deliberately strict. At equality an unseen smaller original ID can win. An incomplete source-overlap sum is also not an upper bound: skipped feature streams may still increase c. The gate is applied only after constructing the complete exact field. It cannot stop the physical-order run scan just because one interval fails; later intervals may have larger bounds.

## Work, State And Failure Boundaries

Use P for pre-RMQ rejected intervals, Q=R'-P for issued initial RMQs, s=min(k',R') for retained seeds, and D for child RMQs. For k'>0:

~~~text
initial interval visits = R'
initial RMQs           = Q = R' - P
child RMQs             = D <= min(2*(k'-1), N-R')
total RMQs             = Q + D
heap-entry peak        <= min(N, 2*k'-1)
~~~

The review's child refinement counts distinct discovered targets: if V is the union of s retained intervals, D<=|V|-s and |V|<=N-(R'-s). None of the P skipped initial intervals changes the surviving forest or child traversal. The public receipt verifies the initial-RMQ identity, unchanged child calls and identical a/L/E/R fields for every one of its 30 source summaries.

Added bound work is O(R') fixed-width arithmetic/comparisons. Heap CPU is O((Q+k') log(k'+1)), with the more precise update accounting in the independent review. The existing tree uses O((Q+D) log(n+1)) logical node/metadata probes. A skipped logical RMQ saves its actual probes; not every RMQ has the same probe count. The selector's worst-case heap reservation does not decrease further. k'=0 retains the existing early empty-output path.

Endpoint merging still pays two source/directory passes, every one of L posting intervals, all E=2L endpoints, and the complete R-record field spool/read. It retains the same g-cursor admission. Query-independent preparation, index bytes, scratch reservation, output, refresh and failure cleanup are unchanged. Packed integer products require sufficient checked width; the experiment uses exact Python integers and Fraction. It has no fixed-byte process memory enforcement and must run without Python assertion elimination.

**Worst case remains.** Take a=2 and alternate singleton intervals with c=1 and c=2 while every target has b=4. Scores are 1/5 and 1/2; bounds are 1/2 and one. Neither is strictly below any possible seed threshold, so P=0 even when R' is large. Likewise, a heap containing zero-score heads cannot reject zero-score intervals. If R'<k', the seed heap never fills and this rule prunes nothing. One interval's single maximum cannot be counted as several high-scoring witnesses just because the interval is long.

## The Stronger DAAT Control

The new [posting-merge reference](experiments/probe_posting_merge_similarity.py) performs ordinary document-at-a-time set overlap, using the same exact source features and prepared postings. It is a comparator implementation built here, not a claim that a cited paper contains this exact Python adapter or its output protocol.

~~~text
validate/count source features; admit g posting cursors and k' heap slots
lazily expand each feature's sorted posting intervals into positions
merge the position streams; group equal positions
for every positive-overlap nonself target:
    count exact occurrences c; fetch b once; compute exact Jaccard
    retain the best k' score/ID records
if fewer than k' positive records exist:
    scan the original-ID inverse index in ascending ID order
    skip self and the retained positive IDs; append required zero records
sort the bounded result heap and stage/publish exactly k' records
~~~

Let W be the sum of source-posting membership counts, including occurrences at self; C the number of distinct positive-overlap nonself targets; and Z the emitted zero-score rows. Unlike the field route, this control expands long intervals: W can greatly exceed L. It does not build an n-sized overlap array or dictionary. It keeps O(g+k') logical cursor/heap state, with fixed-record I/O, output copies and object overhead separately chargeable. It reads every source-posting interval and every expanded occurrence; source-normalization and the shared fixture are still outside a production RAM-cap claim.

Correctness follows because each deduplicated feature contributes exactly once to a target's grouped position, so the grouped count is the true intersection. The bounded heap has the best positive targets. If it has p<k' entries after all postings, every positive nonself target is in that heap; therefore any other target is a true zero. Ascending original-ID iteration gives the required zero tail. At most k'+1 inverse records need examining for that tail under the fixed eligibility contract: at most p positive IDs and one self can be skipped before k'-p zeros are emitted. This avoids a full population scan for absent-feature or empty-source queries.

A useful CPU/probe model is O(a log(F+1) + L + W log(g+1) + (C+k') log(k'+1)), plus O(C+Z) random metadata reads, O(k') inverse-prefix reads and output. There is no query RMQ-tree access or overlap-field scratch. The helper reused for metadata records has a historical `rmq_meta` counter tag even when called directly by DAAT; that tag is not a claim that DAAT issues RMQs. The actual control never calls `snapshot.rmq`.

The synthetic long-run test demonstrates a real distinction: two posting intervals over 128 targets produce W=256 expanded occurrences and C=128 metadata evaluations. A runwise sufficient-index route can work with two intervals and a few RMQs there. Conversely, on original-ID ca-GrQc postings, intervals are almost all singletons; avoiding expansion saves very little, and the extra field/RMQ machinery is costly. Neither example proves an advantage over every run-aware engine.

## Public-Data Results

Use the same [hashed SNAP input and explicit projection](experiments/data/README.md): 5,242 nodes, 14,484 simple undirected edges, 28,968 memberships. Physical order remains ascending original ID; 28,606 posting intervals encode those memberships. No new permutation, output precomputation, approximate score, or special query-specific index was introduced.

Queries: the previous six degree-order strata plus 24 additional IDs sampled uniformly without replacement from remaining IDs using Python Random seed 920520. These were not selected from their timing results. They are not an independent dataset or a model of customer query frequency. All queries request exact top-10 with self exclusion and full zero tails. Three repetitions rotate mode order; the raw source IDs and every run are in the receipts.

- [Five-mode receipt](experiments/Similarity-Run-Bound-Results.json): 30 sources x 3 repetitions x 5 methods = 450 exact checked executions. It compares original all-head, bounded-sort, bounded-merge, pruned-merge and full scan.
- [Six-mode receipt](experiments/Similarity-Run-Bound-DAAT-Results.json): the same 30 sources, with the new posting-merge control, for 540 exact checked executions. This is the authoritative matched comparison for claims involving DAAT.

These counts are executions of 30 distinct query inputs, not 990 independent graphs or queries. Methods are Python 3.11.15 file-backed prototypes on macOS 15.3.1 arm64, using normal cached filesystem operation and unbuffered fixed-record helpers. Sort capacity is 4,096 records, sort fan-in 16, present-feature cursor cap 128, selector cap 19 for bounded runwise variants, and output-heap cap 10 for scan/DAAT. Record counts are not equal measured byte budgets. No GDS run, physical RAM-cap measurement, cold-device experiment or tail-latency guarantee is present.

### Why The First Positive Result Was Insufficient

In the five-mode receipt, the two high-degree sources improve from unpruned merge 43.880/47.648 ms to pruned merge 24.054/23.764 ms; the corresponding scans take 27.196/27.625 ms. This looks favorable compared with the previous scan crossover, but it only establishes an improvement over those two implementations. The stronger comparator changes the practical conclusion.

Six-mode matched medians, in milliseconds, for the original six sources:

| Source | Degree | Initial RMQs before | Initial RMQs after | Pruned merge | DAAT merge | Full scan |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 12295 | 0 | 2 | 2 | 2.886 | 0.556 | 26.678 |
| 2307 | 2 | 88 | 54 | 3.034 | 1.220 | 27.215 |
| 10910 | 3 | 139 | 79 | 3.873 | 1.776 | 27.389 |
| 5862 | 6 | 53 | 37 | 3.445 | 1.903 | 27.054 |
| 21281 | 79 | 590 | 67 | 23.235 | 21.964 | 27.581 |
| 21012 | 81 | 676 | 73 | 23.846 | 22.616 | 27.585 |

Across all 30 inputs, 20 have some pre-RMQ pruning. Initial RMQs fall from 2,855 to 1,085, a 62.0% reduction; 1,770 are skipped. All original field dimensions and child-RMQ counts are unchanged. These totals count each source once, not three times for repetitions. They measure the intended algorithmic work saving, not total elapsed time or RAM.

**DAAT has the lower measured median on all 30 sources**, with pruned/DAAT duration ratios from 1.054 to 5.189. Both beat the full scan on all 30. This is a finite observation, not a statistical statement of universal dominance; the narrowest 5% gaps especially need more repetitions and controlled environments before a stable performance claim.

The sum of the 30 per-query medians is 147.208 ms for pruned merge, 91.912 ms for DAAT and 816.174 ms for full scan. This sum is a descriptive equal-weight aggregate, not a measured end-to-end batch duration or market workload estimate. For the 24 additional sources alone, the corresponding pruned/DAAT sums are 86.888/41.877 ms. The original six therefore do not solely explain the negative result.

The worst-degree query 21012 illustrates the mechanism: DAAT expands only 2,901 source-posting occurrences and computes 355 nonself target scores. The runwise route handles 2,844 posting intervals, 5,688 endpoints and 677 field runs before its 73 surviving initial RMQs. Eliminating 603 RMQs is useful but does not eliminate that field work. Most posting intervals have length one, so compressed-run arithmetic has little expansion work to avoid.

### Full-Workflow Accounting

The six-mode preparation took 0.0151 seconds for resident normalization, 0.0422 seconds for canonical fixture files, and 0.2674 seconds for the shared index build. Prepared bytes are unchanged at 1,476,856. These are single observed stages, not a bounded-import benchmark or repeated build statistic.

The shared experiment intentionally gives all modes the same source/index universe. A standalone scan does not need the postings/tree; DAAT does not need the tree or canonical full-scan pairs. Charge each independently minimized build, retained footprint and refresh path before an end-to-end deployment comparison. The current query timers include reading source files, query preparation/evaluation, complete staged output, test-sink reread and cleanup. Creation of each source file and the independent oracle are outside the timer; source-file creation was not individually timed. Original data remains an in-memory fixture during the experiment, outside any low-memory production claim.

## Tests, Review And Reproduction

Four missing-pruned-route tests failed before implementation, then the combined suite passed 17 tests. A missing public-source-selection test failed before its helper was added, then 18 tests passed. Four missing-DAAT tests failed before its implementation, then the combined suite passed 22 tests in 6.115 seconds before the six-mode benchmark. The benchmark subsequently completed successfully.

New pruner evidence: 2,048 exhaustive complete-answer comparisons; an R'=256 synthetic query with one initial RMQ and 255 valid rejections; an equal-score smaller-ID counterexample; insufficient-witness, empty-source and self-exclusion cases. The synthetic query still reads the full field and all its endpoints. New DAAT evidence: another 2,048 exhaustive complete answers; a sparse case with only three visited memberships and a two-row zero tail; the long-run expansion fixture; and cursor/heap/output/disk refusal plus injected sink failure cleanup. These extend rather than replace the earlier bounded/merged suites.

The independent review supplied no code approval. It checked 465 feasible (a,b,c) triples, 28,437 exact target/threshold pairs, 9,479 strict rejections and 45 attaining cases with BigInt, without reading the implementation or rerunning these tests. Its proof, counterexamples and inspected primary passages are retained in [Similarity-Run-Bound-Review.md](Similarity-Run-Bound-Review.md). The reviewer completed and was closed. Its scientific conclusion is an ordinary pruning adaptation, not a new envelope.

Reproduction:

~~~sh
/Users/amuldotexe/.local/bin/python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
/Users/amuldotexe/.local/bin/python3.11 research_algorithms_20260920/experiments/bench_bounded_seed_similarity.py --data research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz --output /tmp/similarity-bound-daat-replay.json --sort-capacity 4096 --sort-fanin 16 --merge-cap 128 --include-pruned --include-daat --sample-count 24 --sample-seed 920520
~~~

Omit `--include-daat` for the five-mode methods, retaining its source sampling parameters. Timings and output-file hashes are not expected to reproduce bit-for-bit. Old receipts remain preserved rather than being replaced by faster later runs. Zero-valued Counter entries may be absent from JSON metric dictionaries; the executable treats absent counters as zero.

SHA-256 checkpoints: pruned/bounded module `75fa2d52cbac5bf9148a49b606c506c026294ad4b5ebcfbdf43507cf30b4e593`; DAAT module `e0e8e21478639de56f25d108530e34e658d60fac97ad3efa4f95bedf19785087`; current driver `7bfd5b3cfbba9e97f9efa233f235fd8ab38a1cbf05ed5e4638cb3c0e3748bbe3`; five-mode JSON `8daff8a3a657f60949ed8a4dc038d3836b439ab9fbcbc6856df16d3059e64a3c`; six-mode JSON `9ca09e6cb03d668084cd2682e8ef69254085d9fe42d51711b1828fd51edc9029`.

## Prior Art And Revised Research Target

The reviewer inspected primary passages in [Chakrabarti, Chaudhuri and Ganti, ICDE 2011](https://www.microsoft.com/en-us/research/publication/interval-based-pruning-for-top-k-processing-over-compressed-lists/), [Ding and Suel, SIGIR 2011](https://research.engineering.nyu.edu/~suel/papers/bmw.pdf), [Swamidass and Baldi, JCIM 2007](https://pmc.ncbi.nlm.nih.gov/articles/PMC2527184/), and [Nasr et al., JCIM 2012](https://pmc.ncbi.nlm.nih.gov/articles/PMC3415597/). The first two establish interval/block bounds before expensive evaluation. The 2007 Tanimoto algorithm has a particularly close full-heap, strict-bound test; the 2012 work connects exact Tanimoto search to inverted indexes. Their scopes, arbitrary-tie limitations where applicable, and access restrictions are explained in the review. Their speedup figures are not applied to this implementation.

The lead also inspected [Low and Zheng, Fast Top-K Similarity Queries Via Matrix Compression, CIKM 2012](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/short_topk.pdf), especially its abstract and algorithm setup: compressed group summaries bound inner products and avoid a large candidate heap, in an explicitly in-memory setting. This is another relevant direction for group-level bounds, not a ready-made Jaccard or low-RAM lifecycle result.

Three statements must remain separate:

1. The pre-RMQ gate is a proved, useful repair to this executor's work schedule.
2. The c/a inequality, exact threshold witnesses and DAAT control are established ideas or direct adaptations. Renaming them does not create a paper contribution.
3. The complete query-independent overlap-field/RMQ architecture is not shown by these citations alone to be historically identical. Its scientific value nevertheless needs a stronger theorem or empirical lifecycle separation than the current full-scan wins, and the new DAAT comparison is negative on this source/order.

The next research step should not be another comparison against only full scanning. Two routes remain credible, with different falsifiers:

- **Pre-field certificate route:** define paid query-independent information that can upper-bound a physical region before reading all its source endpoints. Prove correctness under indistinguishable snapshots and measure metadata/build/refresh versus skipped endpoint work. Compare with a Jaccard adaptation of published interval/block pruning on identical information. Merely storing ordinary block maxima or obtaining exact c first is not the new result.
- **Useful compressed-domain route:** establish a natural input and paid ordering/representation where W is substantially larger than L and remains so across held-out queries and refreshes. Compare full workflow costs against this DAAT control and a competent run-aware implementation. Do not manufacture a free favorable permutation or extrapolate the synthetic shared-feature family to real users.

**Rubber-duck conclusion:** the first question was whether every run needs an RMQ; the answer is no. The second was whether that repair produces a better similarity engine; the strongest executed control currently says no on this dataset/order. The genuine innovation target is avoiding work the competent comparator must still do, not repeatedly optimizing overhead introduced by our own representation. Seven-family publication readiness remains unproven.

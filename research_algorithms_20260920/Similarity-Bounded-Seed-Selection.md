# Bounded Seed Selection For Exact Runwise Similarity

Date: 2026-09-20. A05 follow-on to [runwise range selection](Similarity-Runwise-Selection.md). Research goal: remove the run-count term from resident selector state while preserving exact full top-k semantics, paid disk preparation and output. This is not a claim that generic bounded top-k selection is new.

**Later comparison:** [exact run-bound pruning and DAAT control](Similarity-Run-Bound-Pruning.md) now skips some initial RMQs and evaluates 30 public-data sources. The pruning is correct, but ordinary posting merging is faster on all 30 sampled sources. The six-source scan crossover and hashes below preserve this earlier experiment; they are not the latest strongest-comparator result.

## Main Result And Its Limits

The original runwise Jaccard executor stores one heap head for every overlap interval and can reject a top-1 query when R is large. The implemented follow-on proves that this rejection is a property of its schedule, not an intrinsic R-dependent RAM requirement. Every head is an exact upper representative for all candidates in its interval. An interval whose best candidate is worse than k other distinct interval heads cannot contribute to the global top-k.

Implemented schedule: stream all initial heads through a k'-bounded worst-first heap; discard intervals whose heads do not survive. Convert the retained heap in place to best-first order. Reuse the existing exact RMQ winner/split traversal for k' outputs. With s=min(k',R') retained seeds, at most s+k'-1<=2k'-1 active heap entries are needed, independent of R, for k'>0. Self-exclusion is applied before seeding; ties use the complete score/original-ID order. All initial RMQs and endpoint work remain paid.

The [completed independent mathematical/prior-art review](Similarity-Bounded-Seed-Review.md) supports the theorem under explicit disjointness, tie and completion conditions. This is a derived composition of established heap/range selection, not a new primitive merely because it repairs a repository bottleneck. A separate ordered-endpoint merge control removes avoidable event sorting. On six sampled public-graph sources, the merged route wins four complete-query timing comparisons against a simple scan and loses two. These are small Python/file experiments, not a Neo4j/GDS speedup or physical-RAM demonstration.

## Design Alternatives And Scope

1. Keep every run head: simplest existing schedule, O(R+k) selector state.
2. Spill a general priority queue: permits many heads but adds external queue machinery and I/O; not needed just to obtain top-k.
3. Keep only the k best seeds: selected finite experiment, O(k) selector state and the same initial RMQ count. It still scans all R intervals, and endpoint sorting can remain the dominant cost.

The existing source/index builder and overlap spool are reused. The new module replaces the selector and optionally the overlap-field construction schedule; its published-output materialization is test-sink work, separately labeled. There is no production integration, new source ordering, approximate score, precomputed query answer, or external heap hidden in the experiment.

## Verification-First Plan

1. Write `experiments/test_bounded_seed_similarity.py`. Load only the definition prelude of the existing Markdown reference using Python AST, not its old full test driver. Make explicit missing-selector assertions before creating the new implementation.
2. Cover exact results against independent set intersections; self exclusion, zero-score tails, cross-run ties and many outputs from one winning seed. Exercise R=n with k=1 and a one-slot selector cap that the old executor rejects.
3. Implement `experiments/probe_bounded_seed_similarity.py` with a bounded worst-first seed heap, in-place key conversion, ordinary binary heap splitting, capacity checks and staged output cleanup.
4. Run small exhaustive/randomized complete-output comparisons on real files with the shared builder. Keep initial-head scans and child RMQs separate in the receipt. Preserve an adversarial mutation that removes the ID tie key or prevents child expansion.
5. Integrate independent review and distinguish correctness/resource improvement from novelty. Check whether input preparation and random RMQ reads dominate before claiming an end-to-end practical improvement.

## Follow-On Experiment: Ordered Endpoint Merge

The first public-data experiment completed with 54 exact results. It exposed a confound: the reference uses four-record external-sort buffers and two-way merges. The high-degree query penalty cannot be presented as a general property of runwise storage without a larger-buffer control. The retained JSON is historical evidence and will not be overwritten.

Three schedules will be distinguished: external endpoint sorting with the original tiny budget; the same sorter with an explicit 4,096-record/16-way budget; and ordinary multiway merging of already sorted per-feature endpoint streams. The third consumes every source posting interval, uses at most one current endpoint per admitted feature stream plus fixed bookkeeping, and writes only the final run field. It retains no population-sized overlap array and does not expand long intervals. Multiway merging is established, not a new primitive.

Executed implementation plan: five missing-function/configuration tests failed first; the implementation then passed those and the original eight tests. It uses the unchanged exact sweep, RMQ, bounded selector and output contract; caps present-feature cursors before opening them; explicitly closes every cursor on failure; and rejects insufficient cursor/disk budgets. Counts separate source size a, present streams g, intervals L, endpoint events E, overlap runs R and selector heads. The six-source, three-repetition experiment with larger sort buffers and the extra merged route completed with 72 exact answers. All methods use the same frozen source and prepared files. Query timings exclude shared preparation and source-file writing, both separately disclosed.

The scientific question is whether interval compression still gives a useful complete-query advantage after granting ordinary competitors sensible buffers and streaming selection. Neither a low heap count nor a speedup over the four-record sorter would establish novelty. A compressed-posting/WAND comparison and lifecycle accounting remain necessary.

## Exact Contract And Notation

Use the original fixed-snapshot binary-Jaccard contract: unique original IDs, immutable physical target positions, deduplicated feature sets, coherent cardinalities and indexes, and no late eligibility changes. Let N be the eligible nonself population and k'=min(k,N). Return exactly k' distinct records ordered by descending exact Jaccard and then ascending original ID. Empty-empty and every zero-intersection pair have score zero. Output does not expand boundary ties.

The exact overlap field has R maximal constant-count intervals. Removing self before candidate selection creates R' nonempty disjoint intervals, R'<=R+1. They partition the eligible population. For a positive count c, minimizing (target cardinality b, original ID) maximizes c/(a+b-c). For a zero count, minimizing original ID gives the winner. These are the two already-prepared RMQ fields. Physical position is not a valid substitute for the ID tie order.

No floating comparison is used. The executable uses Python Fraction. The original packed-domain arithmetic-width derivation is unchanged; Python object sizes are not packed-slot sizes. The implementation remains a reference research probe whose assertions require normal, non-optimized Python execution, not a hardened production API.

## Algorithm And Proof

~~~text
validate the request and coherent snapshot; resolve self and k'
reserve complete output and min(N,max(0,2*k'-1)) heap slots
if k'=0: publish empty output without constructing the overlap field
construct the full exact overlap field on disk
for every initial nonself interval:
    obtain its exact RMQ head
    retain it only if it belongs to the best k' heads seen so far
    keep the worst retained head at the heap root
convert the same retained storage to best-first keys; heapify in place
repeat exactly k' times:
    pop the best head and stage its complete result record
    unless this is the final result, RMQ and insert its remainders
publish only after all required results have been written
~~~

**Lemma 1, safe seed pruning.** The bounded stream heap retains the best min(k',t) heads after t heads have been examined, by induction on conditional replacement of the worst head. At the end, any discarded head h has k' retained heads preceding it in the strict total order. Because intervals are disjoint, these are k' distinct actual targets. Every target in h's interval is no better than h. It therefore has at least k' better targets and is outside the global top-k'. No retained witness log is required.

**Lemma 2, complete enumeration.** Let V be the union of retained seed intervals. Lemma 1 proves that top-k'(U) is contained in V and |V|>=k'. The best-first heap partitions the unreported part of V, not all of U. Its heads are the best remaining targets in their intervals. Removing a winner and inserting its two remainders preserves that invariant. Thus exactly k' globally correct records are emitted in order. Retained heads are witnesses, not necessarily the final answer: initial lists [1,2],[3],[4], k'=2 retain roots 1 and 3 but must emit 1 and 2.

**Lemma 3, retained heap bound.** With s=min(k',R') initial seeds, each nonfinal pop removes one interval and adds at most two. After t nonfinal outputs, occupancy is at most s+t. Skip all children after the final output, yielding peak <=min(N,s+k'-1)<=2k'-1. Pop before inserting children. k'=0 uses zero heap slots. The independent review supplies a tight generic five-slot witness for k'=3.

This is a bound on heap length, not every simultaneously live Python object. An incoming head, popped winner, key-conversion temporary, list backing capacity, allocator metadata and output buffers remain chargeable. Even top-1 can have a retained head and an incoming head at once. The code converts keys in place; it does not construct a second seed array. Production code should reserve checked packed capacity before execution. The probe's logical admission is conservative and may reject an input that could use a smaller input-specific reservation.

## Ordered-Endpoint Merge

Let g be the number of source features with nonempty prepared posting lists. Each feature's maximal disjoint intervals produce endpoints (lo,+1),(hi,-1) in sorted order. An ordinary g-way merge therefore supplies exactly the same globally ordered endpoint multiset as sorting all E=2L events. Summing all events at each position before advancing the sweep yields the identical full-domain overlap field. The zero region and empty-source case are preserved.

The implementation makes two paid source/directory passes: first count a, g and L and admit g cursors; then create the cursor generators. It does not retain all a source IDs. Each admitted cursor supplies one current endpoint; all E events are still processed. A finally block closes the merge and its underlying file readers on both success and failure. The prepared posting contract requires maximal intervals with hi<next_lo. Truncated explicit-count reads fail, rather than becoming a successful shortened source.

This control is standard sorted merging applied to already ordered postings. [Python's documented merge primitive](https://docs.python.org/3/library/heapq.html#heapq.merge) supplies a streaming merge without collecting all input items. Its use here is an engineering correction to the original event sorter, not a scientific novelty claim.

### Charged Bounds

| Resource | Bounded seeds with external sort | Bounded seeds with endpoint merge |
| --- | --- | --- |
| Initial logical RMQs | R' | R' |
| Child logical RMQs | <=2(k'-1) for k'>0 | Same |
| Heap CPU | O((R'+k') log(k'+1)) | Same |
| RMQ node and metadata probes | O((R'+k') log n) in this disk tree | Same |
| Field construction CPU | External sorting plus sweep of E events | O(a log F + E log(g+1) + R), including directory service |
| Field-construction state | Admitted sort buffer and fan-in | O(g) bounded cursors, generators and merge entries |
| Retained selector state | O(k') plus fixed/transient overhead | Same |
| Query scratch reservation | 2*w_event*E + w_run*min(n,E+1) + w_output*k' | w_run*min(n,E+1) + w_output*k' |
| Query field payload | Event-sort generations, event reread, run spool | L interval records, no event files, run spool |

Field construction finishes before seed selection starts. Their logical working sets need not coexist; whole-process allocator/cache behavior still requires measurement. Query-independent tree, metadata, inverse map and posting preparation remain paid. The merged route refuses when g exceeds its admitted cursor count; no unimplemented bounded external merge is silently assumed. The sorted route remains an alternative, with its own admission and traffic.

The streaming full-target comparator retains O(a+k') source-set/heap state, reads every canonical membership and target metadata row, and streams the final staged output. It is not an O(k')-total-state comparator. Both variants still need output capacity when k' is large. None supplies an arbitrary-data deadline guarantee.

## Public-Data Experiment

Source: [SNAP ca-GrQc](https://snap.stanford.edu/data/ca-GrQc.html), with [local provenance and hash](experiments/data/README.md). The normalized simple undirected projection has 5,242 retained nodes, 14,484 edges and 28,968 directed memberships. We remove 12 self-loop rows and duplicate/reverse undirected rows while preserving their endpoint IDs, including an isolated source. This projection is explicit; we do not silently reuse the source page's 14,496-edge statistic.

Targets are physically ordered by original ID. The six source IDs are selected deterministically at degree-order positions min, quartiles, and the two highest positions. They are not a random or representative workload sample. Each query computes exact neighbor-set Jaccard top-10, excludes self, and is checked against a direct set-intersection/sort oracle. Three repetitions rotate method order. Query timing includes overlap preparation, selection, staging/publication, test-sink reread and cleanup; it excludes resident fixture normalization, source-file creation and oracle computation. Source-file creation was performed but not individually timed in these receipts. Prepared indexes are shared within each experiment.

Environment: Python 3.11.15, macOS 15.3.1 arm64, normal cached filesystem operation, unbuffered fixed-record Python file helpers. No process/cgroup RAM measurement, cold-device test, NVMe tuning, Rust implementation or GDS comparison. Distinct record/cursor budgets are disclosed; they are not equal measured RSS budgets. Three repetitions do not establish tail percentiles or confidence intervals.

### First Receipt: Tiny Sort Budget

[Original JSON](experiments/Similarity-Bounded-Seed-Results.json): four sort records, fan-in two, three modes, 54 exact checked results. Median complete-query times in milliseconds:

| Source | Degree | R | Original head peak | Bounded head peak | Original ms | Bounded ms | Scan ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 12295 | 0 | 1 | 2 | 2 | 3.046 | 2.976 | 25.895 |
| 2307 | 2 | 89 | 88 | 10 | 12.736 | 13.032 | 26.857 |
| 10910 | 3 | 140 | 139 | 10 | 20.792 | 20.586 | 27.103 |
| 5862 | 6 | 54 | 53 | 10 | 10.214 | 10.217 | 26.672 |
| 21281 | 79 | 591 | 590 | 10 | 541.956 | 482.375 | 27.055 |
| 21012 | 81 | 677 | 676 | 10 | 517.698 | 525.318 | 27.041 |

Nontrivial queries reduce heap-entry peaks by 81.1%-98.5%. Those percentages are not reductions in total RAM. The original executor actually refuses five queries under a 19-slot heap cap; bounded seeding accepts all six. High-degree completion is 17.8-19.4 times the scan duration in this configuration, but the extreme sorter is a confound, not a production verdict.

### Revised Receipt: Sensible Sort Control And Merge

[Revised JSON](experiments/Similarity-Merged-Endpoint-Results.json): 4,096 sort records, fan-in 16, merged route cap 128 feature streams, four modes, 72 exact checked results. The same normalized source and original-ID ordering are used. All source, interval, run, RMQ and output obligations remain. Median complete-query times in milliseconds:

| Source | a | L | R | Original + sort | Bounded + sort | Bounded + merge | Streaming scan |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 12295 | 0 | 0 | 1 | 3.045 | 3.034 | 2.966 | 27.879 |
| 2307 | 2 | 46 | 89 | 6.845 | 6.962 | 6.540 | 26.934 |
| 10910 | 3 | 78 | 140 | 9.817 | 9.925 | 9.347 | 27.134 |
| 5862 | 6 | 38 | 54 | 5.786 | 5.706 | 5.415 | 26.889 |
| 21281 | 79 | 2625 | 591 | 64.856 | 65.638 | 44.633 | 28.608 |
| 21012 | 81 | 2844 | 677 | 70.090 | 70.996 | 48.496 | 27.056 |

**Revised conclusion:** bounded seeding supplies a memory-admission improvement, not a consistent speed improvement over retaining all heads. Merging ordered endpoints avoids unnecessary sorting and shortens the high-degree queries by about 32% relative to the larger-buffer bounded sort route. The streaming scan nevertheless remains faster on those two sources: merged durations are 1.56 and 1.79 times scan durations. On the three nonempty lower-degree sampled sources, the scan/merge duration ratios are 2.90-4.97. The empty-source comparison is a weak control because a scan can special-case zero scores with an ID index; its 9.40 ratio is not a meaningful research headline.

For source 21012, both bounded variants perform the same 676 RMQs, 1,439 tree-node probes and 1,439 metadata probes. The merged route writes 16,568 query-arena payload bytes (677 run records plus ten outputs); the larger-buffer sort route writes 198,584, including two endpoint-file generations. Sort/merge comparison changes real logical traffic, but not the required RMQs. These counters do not measure physical SSD traffic or process RAM.

### Preparation And Reuse

Both experiments produce the same 1,476,856 retained prepared-file bytes: inverse 83,872; tree 262,144; canonical membership pairs 463,488; metadata 83,872; directory 125,784; interval postings 457,696. The 28,606 posting intervals encode 28,968 memberships: almost all are singletons in original-ID order. This dataset/order does not demonstrate strong run compression.

Original normalization/canonical writing/index build: 0.0147/0.0406/3.0337 seconds. Revised: 0.0145/0.0413/0.2718 seconds. These are one observed preparation per configuration, not repeated speedup estimates. Larger sort buffers substantially change the observed build, as well as query work. Resident Python graph/set normalization is outside any bounded importer claim.

The scan does not need the posting directory or RMQ tree. Its executed query reads canonical pairs, metadata, source and the self-ID inverse lookup. Charging the entire shared index build/footprint to a standalone scan would be unfair. For a first answer, compare independently minimized preparation pipelines; for reuse, include snapshot lifetime and query mix. No paid build has been amortized across a fictitious number of users or queries here. Temporary index directories were removed after each benchmark; the public gzip and JSON receipts are retained.

## Verification And Independent Challenge

- The original bounded route had six explicit missing-function failures before implementation. Added scan/parser tests also failed before implementation. Eight tests passed in 2.251 seconds in that iteration.
- The merged route and configurable fixture loader produced five explicit missing-function/configuration failures. The combined current suite passed 13 tests in 3.839 seconds before the revised benchmark. The revised benchmark itself subsequently completed successfully.
- Bounded route: 2,048 exhaustive full answers, 288 randomized full answers, three R=n/top-1 fragmentation fixtures, ties/zero/self cases, descendant winners, refusal and sink-failure cleanup. Streaming scan control: 18 small full-answer comparisons. These are not independent physical-memory measurements.
- Merge route: another 2,048 exhaustive full answers; a 1,024-target fixture with a=4, g=3, L=3, E=6, R=1 and no target expansion; cursor/disk refusals; an absent-feature zero-stream query; an injected interval-reader failure with every tracked reader closed and no query directory left behind; explicit 16-record/four-way sort-budget verification.
- Independent mathematical review: 321,254 abstract cases, a tight generic heap-peak example and failures of weakened tie/witness contracts. The reviewer did not approve the new merge implementation or validate its physical memory. Its complete review is retained separately, including inspected primary passages.

The inherited artifact generator produces valid coherent files; this is not a general corrupted-input validator. The current tests do not inject every initial-RMQ/child-RMQ/truncated-field failure proposed by the independent review, nor do they exercise preserving a previously published external result across a crash. Publication occurs in an isolated temporary query arena and is read back by the test sink. Do not upgrade this to durable service publication.

Reproduce the current suite and revised benchmark:

~~~sh
/Users/amuldotexe/.local/bin/python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
/Users/amuldotexe/.local/bin/python3.11 research_algorithms_20260920/experiments/bench_bounded_seed_similarity.py --data research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz --output /tmp/similarity-merge-replay.json --sort-capacity 4096 --sort-fanin 16 --merge-cap 128
~~~

The current driver includes the fourth merged mode. Therefore running it with four-record defaults will not byte-reproduce the old three-mode receipt. The original receipt remains historical output from the preceding driver. The original embedded Markdown Python source is unchanged; the fixture loader executes its definition prelude and injects explicit sort defaults into that isolated namespace, without running its old full test driver.

SHA-256 at this checkpoint: implementation `543853b285bf58c73a3e9c42fcb251aefed84371afe0a69d2b11a86c31cc9ec4`; benchmark driver `ff6cbba5610c24a137e655e98907e965401b15fc3f4390c95ab1b4350d035750`; original JSON `70f7d346fb3560c51d4a93c17001a21d9eaa4f43eb775252a57c28875aa05598`; revised JSON `bfb729ea27729310431b0e095b2d60754370d6e9d8cc1d85b2cc44a9cfb2856b`.

## Prior Art And What Could Become A Paper

The completed review inspects [Sedgewick/Wayne bounded TopM](https://algs4.cs.princeton.edu/24pq/TopM.java.html), [Akram/Saxena sorted range reporting](https://arxiv.org/pdf/2104.02461v4), [Kaplan et al. heap/list selection](https://arxiv.org/pdf/1802.07041v1), and a published Guava bounded stream selector. They establish the relevant stream-selection, implicit heap and RMQ primitives. They do not, by that fact alone, prove the complete disk-Jaccard composition already appeared elsewhere. Conversely, failure to find that exact combination is not positive novelty evidence.

The candidate-specific research subject is query-independent sufficient indexes for complete exact similarity over compressed overlap fields, with both preparation and execution admitted. This iteration removes an avoidable R-sized head set and an avoidable sort schedule. It has a proved state reduction and an actual small public-data crossover. That is stronger engineering evidence, but still not a defensible standalone new selection algorithm or seven-family paper-readiness certificate.

The next decisive work should target the costs that survive these corrections:

1. Compare paid original-ID, locality-based and workload-based target orders using the same selected source IDs and independently selected holdouts. Measure L, R, full preparation/refresh and output; no free permutation. Report ordinary ordering baselines as existing techniques.
2. Compare a competent run-aware compressed-posting or block-pruning executor with the same semantics, byte budgets and cold/warm conditions. The simple full scan is now a necessary floor, not the strongest competitor.
3. Seek a new sufficient statistic or certificate that reduces mandatory seed RMQs/overlap construction without missing low-ID ties or unseen winners. A prediction of which source is fast is not such a certificate. Require a separating input family and an adversary before implementation.
4. Consider a hybrid planner only after pricing statistics acquisition and each route's admission. Use held-out queries and a regret/work bound if claimed. Selecting the fastest mode after seeing these benchmark times is an oracle comparison, not an implemented planner.

**Rubber-duck correction:** less retained heap state did not imply faster similarity; slower tiny-buffer execution did not prove the format intrinsically slow; removing sort traffic still did not beat the scan on every source. Preserve all three findings. They identify the remaining innovation target instead of disguising a sequence of ordinary implementation repairs as a new graph algorithm.

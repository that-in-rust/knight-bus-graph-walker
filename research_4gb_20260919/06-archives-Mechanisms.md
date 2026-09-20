# Archive-Derived Mechanism Hypotheses

Status: research proposals, NOT implementations or measured results. 2026-09-19. This lane's corpus reading is partial; these combinations are new proposals relative to the spans inspected here, not claims of repository-wide or global originality. See [evidence and coverage](06-archives-Evidence.md).

## Chosen Thesis

The promising target is a repeated, well-specified graph job on a fixed snapshot with explicit output obligations. Not a universal Neo4j replacement, not arbitrary Cypher, and not a promise that disk-backed computation always beats compressed resident GDS.

Three workload families need different plans: sparse impact traversal, iterative rank propagation, and exact neighborhood similarity. The strongest new mechanism lead here is **source-batched exact similarity with certified zero-score completion**. The strongest shared-storage lead is **coverage-certified, byte-bounded epoch construction**: an absent index entry only permits skipping data if the index is complete for that snapshot and predicate domain.

Commercial evidence is mixed. Private discovery in PMF003 1373-1431 values latency and familiar tooling as much as RAM; raw 3471 cautions that many enterprise customers can rent larger machines. Target constrained deployments with repeatable work, and test willingness to replace custom code. Do not equate a refusal-only runtime with a useful product.

## Already Known

- GDS uses compressed CSR and already estimates memory; compare against that real baseline, including supported packed adjacency where applicable. [GDS feature toggles](https://neo4j.com/docs/graph-data-science/current/production-deployment/feature-toggles/)
- Ladybug already executes common analytics while scanning projected data from disk. A storage buffer setting alone does not establish whole-process algorithm memory. [Ladybug algo extension](https://docs.ladybugdb.com/extensions/algo/)
- GridGraph predates this study's tiling/window/active-block ideas. [GridGraph, ATC 2015](https://www.usenix.org/conference/atc15/technical-session/presentation/zhu)
- Exact indexed similarity search is longstanding prior art. The All-Pairs paper's abstract was inspected; full-paper novelty comparison remains pending. [Bayardo, Ma and Srikant, WWW 2007](https://research.google.com/pubs/archive/32781.pdf)
- Repository precedents already read: PMF003 2549-2694 has synchronized epoch watermarks; 4241-4354 has sparse-index modes; 16747-17049 has segmented PageRank/BFS state; 18341-18529 has blocked similarity vectors and source-owned queues. Renaming these would not be an innovation.

## Shared Storage Hypothesis

### H0: Coverage-Certified Epoch Blocks

Inspiration: raw 10745's search index produced false absence conclusions because entire record classes and bodies were missing. Capture journal 4127-4201 similarly warns that a property stream or graph count can omit real topology. Translate that lesson into a planner invariant: **unknown coverage must never be interpreted as no matching edge**.

Proposed mechanism:

1. Ingest nodes, endpoints, relevant properties, and deletions as separately validated streams. Assign stable external-to-dense IDs with external sort/merge or a bounded disk dictionary. Preserve isolated nodes and record duplicate/multiedge policy.
2. Bound decoder bytes and individual-record handling, not just the number of Arrow batches. Split large groups into disk runs. A finite snapshot has an explicit end marker per stream; a live stream needs an actual watermark contract.
3. Seal an epoch only after all required streams are complete and consistent. Persist a manifest containing generation, ID-map version, covered row/key intervals, required lanes, counts, checksums, and tombstone frontier.
4. Build sparse summaries and profile indexes against that manifest. An index miss means absent only when the covered interval and predicate domain are complete. Missing/partial/stale summaries fall back to the base scan. Bloom-like false positives may cost I/O; false negatives are forbidden.
5. Publish the manifest atomically. Old readers pin the old manifest. Reclaim obsolete files only after readers finish; reserve both generations before starting refresh.

Correctness obligation: every edge included by the requested snapshot/filter is either inspected or excluded by a sound complete-domain certificate. A checksum detects changed bytes; it does not prove the producer included all source records. Require source sequence/range reconciliation and source-count validation where available. Independent per-column summaries must not be treated as proof of arbitrary correlated predicates unless the exclusion rule is sound.

Resource shape: `RAM = decoder + bounded stream windows + sort/merge buffers + directory cache + verifier + output buffers`. Unfinished epoch backlog is disk state with a quota, not unbounded RAM. With a stalled source, consistency may force waiting forever; an explicit finite-snapshot deadline can end in a reported failure, not a falsely complete graph. A huge single epoch is split physically without weakening logical completeness.

Potential gain: reuse unchanged immutable blocks across requests and refreshes while safely skipping proven irrelevant tiles. The proposed integration of completeness evidence with profile-index admission is the hypothesis; sorted runs, manifests, watermarks and conservative pruning are established techniques.

Adversarial case: one delayed deletion stream, mixed ID-map generations, a source omitting all propertyless edges, or a corrupt summary. Expect fallback or visible failure, never a smaller silently incorrect graph. Randomly spread updates can invalidate every block and eliminate refresh savings.

## Sparse Traversal Hypothesis

### H1: Output-Obligation Frontier Scheduling

Buyer/job: repeated dependency impact queries over a mostly stable industrial or code graph. Query contracts separate reached set, distances/parents, ordered traversal, and path materialization. A reached-set result must not be advertised as GDS BFS compatibility.

Mechanism: use coverage-certified outgoing blocks and a bounded page cache; batch frontier requests by physical block for reads, but preserve logical discovery ordinals in a spill stream when order is required. Deduplicate candidate destinations using a resident bitset when it fits, or external partitioned visited state when it does not. Construct only the requested result representation. Keep original-ID translation and result export out of the graph-read cache budget.

The ordering challenge is real: locality sorting changes visitation order, target truncation and possibly a traversal path. Proposed ordered mode must reorder discovered candidates by a version-pinned logical key and reproduce the baseline's duplicate/target rules. Start with a single-thread deterministic oracle; concurrency equivalence is a separate gate. Do not describe a visitation path as a shortest source-to-each-node path or as enumeration of all simple paths. GDS exposes visit order and a path over visited nodes. [GDS BFS](https://neo4j.com/docs/graph-data-science/current/algorithms/bfs/)

For an unweighted reached-set profile with 32-bit internal IDs, a resident visited bitmap costs `ceil(n/8)` bytes; a distinct frontier is at most `4n`, but raw duplicate candidate records can be proportional to edges traversed, and therefore require a separate bound/spool. Parents/depth, where requested, add explicit per-node lanes. If the external visited path is selected, charge sorting/anti-join I/O at every affected level. Weighted shortest paths are a different plan; an unweighted BFS is not an admissible substitute.

Potential gain: repeated shallow impact queries avoid scanning the full graph and avoid ordered/path output state when the user does not need it. This is an output-aware combination, not a new BFS algorithm. It must beat straightforward indexed CSR custom code, not only a database query with mismatched semantics.

Adversarial cases: star with a frontier wider than RAM; chain with many sequential I/O-dependent levels; diamond with duplicate discoveries and competing target order; graph requiring a huge output. A complete all-path query can have exponential result size; no memory layout removes the output lower bound. Tiny random reads or external ordering may erase any latency win.

## Iterative Hypothesis

### H2: Epoch-Pinned Rank Lanes

Buyer/job: repeated fixed-profile ranking on a sealed graph, with a slower first computation acceptable. This is a capacity/refresh offering, not the same deadline promise as a shallow impact query.

Mechanism: topology and degree/weight-normalization lanes remain immutable by graph generation. Keep rank/message arrays resident when their entire envelope fits; otherwise partition them into source and destination windows with durable message runs and iteration barriers. A full pass reads tiled topology and reduces contributions into next-iteration state. After the initial computation, reuse the exact output artifact only when snapshot, filters, weights, damping, source personalization, tolerance, iteration cap and scaling match.

Correctness obligations: preserve the chosen recurrence, dangling-node behavior, weight handling, stopping rule, score scaling, and output schema. The archive describes GDS's delta/Pregel calculation, so silently substituting textbook normalized PageRank is not compatibility. Preserve iterations/convergence metadata. Floating-point accumulation order may change low bits; declare numeric tolerance and test it. Bitwise equivalence requires a fixed reduction order with its extra I/O costs.

For the inspected GDS-like state shape, rank plus send/receive doubles imply about `24n` payload bytes before votes, degrees, topology, headers and worker buffers. That is a baseline shape, not an exact universal GDS estimate. A truly bounded window plan replaces resident `O(n)` arrays with `O(v_window)` resident state, but adds persistent arrays, message writes/reduction passes and potentially repeated reads. A full scan per iteration reads at least `I * adjacency_bytes`; write and read traffic can be much larger.

Refresh: cached scores belong to the old epoch until a new valid computation completes. A topology or degree change can influence the whole graph. Warm starting, residual repair, or freezing locally unchanged ranks requires a convergence/error argument; H2 initially permits full recomputation instead of pretending refresh is local. New and old score generations coexist during publication.

Adversarial cases: high damping/slow convergence; nearly all vertices active each round; a weight or dangling-node change affecting global normalization; a request for all scores whose export dominates query time. Active-block skipping and vector windowing are prior art; the proposal is explicit generation/config reuse with an enforced total envelope and comparable cold/refresh accounting.

## Join/Intersection Hypothesis

### H3: Source-Batched Similarity With Certified Zero Tails

Initial scope: exact unweighted Jaccard over nonempty outgoing-neighbor sets, explicit source and target eligibility, positive `topK`, and a version-pinned tie policy. No silent approximation. Weighted modes, bottomK, global topN interactions and arbitrary component policies need their own proofs/oracles before using this specialization.

Mechanism:

1. Store sorted neighbor lists and an inverted list from neighbor to eligible source/target nodes. H0 certifies that the postings cover the whole projected graph generation and filter domain.
2. Process a bounded batch of source nodes. Expand shared-neighbor postings, externally aggregate repeated `(source,target)` intersections, and compute exact Jaccard from full eligible neighbor-set cardinalities. Apply configured degree/filter/self-pair rules; do not filter away neighbors merely because they are not output targets.
3. Maintain topK for only the active source batch. Seal its output and release the queues. A symmetric score can be computed twice to preserve bounded one-owner queues rather than keeping all source queues live.
4. With strictly positive cutoff, disjoint pairs cannot qualify. With cutoff zero, a source that has fewer than K positive matches may require zero-score rows. Merge the sorted eligible-target stream with that source's complete positive-candidate stream to select eligible disjoint targets under the pinned tie order. Do not treat an incomplete posting miss as a zero.
5. Stream rows or an explicit paged result artifact. A request for a fully materialized similarity graph pays a separate build/storage budget.

Why exact in this initial scope: nonempty-set Jaccard is positive exactly when intersection is nonempty. Complete postings enumerate all positive pairs; exact aggregation computes their scores. Remaining eligible pairs have score zero. Adding the required zero ties therefore produces the same score-ranked answer under an agreed tie rule. Degree and component policies, missing values, duplicates and directed eligibility must match the contract. Native GDS tie behavior may not equal ascending target ID; no strict compatibility claim until this is pinned and tested.

GDS already bounds results with topK/topN and explicitly warns about zero-cutoff expansion; those ideas are not novel. The hypothesis adds a byte-bounded source ownership schedule plus completeness-certified complement handling, rather than paying all-source queue memory or silently dropping zeros. [GDS Node Similarity](https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/)

Let batch size be `b`, result cap `k`, and a packed queue entry budget be `q` bytes. Queue payload is `q*b*k`, not `q*n*k`. This does NOT bound candidate work: a popular neighbor can generate `sum_x |S_x|*|T_x|` posting products, and the candidate spool can dwarf the graph. Admit/split based on both RAM and scratch, and fall back to a blocked exact scan if postings are worse. That fallback preserves completion for some adversarial shapes; an impossible disk/time request still needs explicit failure.

Example arithmetic, not measurement: at `n=50,000,000`, `k=10`, `q=16`, all-source queue payload alone is 8,000,000,000 bytes. With `b=100,000`, it is 16,000,000 bytes. Vectors, postings, candidate aggregation and output are additional. A 24-byte exported row gives 24,000,000 bytes for one such batch, but 12,000,000,000 bytes for `n*k` rows. A smaller heap does not make a full export small.

Adversarial cases: all nodes share one neighbor; large disjoint neighborhoods with zero cutoff; asymmetric source/target filters; zero ties crossing a batch boundary; a deletion removing the final common neighbor; extremely popular targets; missing postings falsely interpreted as disjointness. Kill the specialization if exact recall or tie/schema parity cannot be established, or if total build+refresh+query cost loses to a simpler blocked implementation for the target workload.

## Lifecycle Accounting

All quantities are bytes, not GiB. Let `Bphys=4,000,000,000`, provisional reserve `Bos=1,000,000,000`, worker envelope `Bw=3,000,000,000`. The OS reserve is an assumption to measure, not a proof. Account file-backed resident pages and dirty writeback as well as anonymous heap, native buffers, stack/runtime and verifier; mmap address space is not resident RAM.

Global invariant: `runtime + managed resident caches + active algorithm state + decode/ingest buffers + sort/spill buffers + output + verifier + margin <= Bw`. Sharing cache pages must not hide uncharged memory. Initially serialize build/refresh and large jobs; concurrent admission sums all live reservations and shared resident state once. A per-job limit alone is insufficient.

| Phase | Required RAM terms | Disk and time obligations |
| --- | --- | --- |
| Extraction | SDK/network buffers, bounded decoded rows, largest retained record | Source snapshot/replay token, input retention, extraction bandwidth and remote cost |
| Validation/ID mapping | Parser, bounded dictionary/cache, merge buffers | External-ID dictionary, duplicate/orphan checks, node list preserving isolates, sort runs |
| Build | Bounded sort, compression blocks, ID translation, selected property lanes | Endpoint sort/merge, CSR/CSC/postings generation, checksums, full build time; index construction is not free |
| Query | Selected algorithm state, cache, bounded frontier/messages/candidates | Page reads, decompression, spill/merge passes, exact configured semantics |
| Export | Fixed output buffers, original-ID lookup, serializer | Result bytes, consumer backpressure, temp/final coexistence, schema and count validation |
| Refresh | Delta decoder, tombstones, compaction buffers | Old/new generations, changed ID/posting/degree lanes, nonlocal algorithm recomputation, pinned readers |
| Recovery | Manifest reader, bounded replay and validation | Durable source offsets, partial runs, retry quotas, cleanup after crash; never delete the only replay source early |
| Verification | Oracle state bounded or on smaller fixtures, output comparison stream | Sorted comparisons/hashes, mismatch evidence, verifier I/O and scratch included |

`peak_disk = retained_source + old_pinned_artifacts + new_artifacts + peak_live_sort_or_candidate_runs + WAL/deltas + query_state + pending/final_outputs + verification_scratch`.

Do not count peak as the sum of every temporary file ever produced, but do not assume files can be removed before all dependent outputs are durable either. A conservative external-sort plan may need both old and new run generations at a merge step. Multi-layout builds must be scheduled explicitly.

### Worked Storage Envelope

Illustrative unweighted graph: `n=50,000,000`, `m=1,000,000,000`, dense 32-bit endpoints, 64-bit offsets, 64-bit external node IDs. All calculations are design estimates with no compression gain assumed.

- Endpoint source tuples: `8m = 8.0 GB`.
- One CSR orientation: `4m + 8(n+1) = 4,400,000,008 bytes`.
- Illustrative reverse-ID array plus sorted external-ID mapping: `8n + 16n = 1.2 GB`, assuming 16-byte mapping records including padding. A different ID type changes this.
- Two simultaneously live 8 GB sort-run generations: `16 GB`.
- Source 8 GB + sort 16 GB + old 5.6 GB + new 5.6 GB + 2 GB WAL/delta reserve + 2 GB output/state/verification reserve is about **39.2 GB**. The remaining 10.8 GB of a 50 GB scenario is not a general safety guarantee: directories, properties, crash debris and larger outputs still need measured headroom.
- Retaining both orientations in both old/new generations adds about 8.8 GB, leaving about 2 GB headroom. H3's postings may make that choice necessary. A full 12 GB similarity export exceeds the illustrative 2 GB output reservation and invalidates this plan.
- Adding 64-bit weights adds 8 GB to each billion-edge orientation and changes source/sort sizes as well. The unweighted plan must not be reused as a weighted promise.
- If the raw retained input is itself 50 GB on a disk capped at 50 GB, this workflow cannot fit. Streaming from a separately budgeted source, early durable source-chunk release, more disk, or a different retention contract is required and must be explicit.

For H2 at this n, three double state lanes are 1.2 GB and an 8-byte degree/normalizer lane is 0.4 GB. An illustrative allocation of 0.4 GB cache, 0.2 GB runtime, 0.2 GB I/O buffers, 0.1 GB output, and 0.1 GB verifier totals 2.6 GB before vote bits/headers/margin. It is a feasibility target, not an RSS claim. Doubling n breaks that arrangement; state must spill or a different schedule must be chosen.

### Latency And Amortization

`Ttotal = Textract + Tvalidate + Tidmap + Tsort + Tbuild + Tquery + Texport + Tverify + Trefresh_attributable`.

For any compulsory scan, `T >= bytes_read / achievable_bandwidth`; decompression, seeks, CPU, writes and stalls add cost. A 50 GB scan at an assumed 200,000,000 bytes/s already takes 250 seconds. Therefore a 30-second full-ingest promise on that setup is impossible even before graph work. Cold end-to-end and prebuilt/warm query results must be reported separately.

For Q repeated jobs, a more expensive layout pays back only if `Q * (baseline_query - candidate_query) > extra_build + cumulative_extra_refresh`, with comparable outputs and hardware. If the per-query saving is nonpositive there is no positive amortization point. Do not omit a slow rebuild merely because most warm queries look fast.

## Verification-First Experiments

No experiments below were implemented or run in this lane.

| ID | WHEN / THEN / SHALL contract | Oracle and kill criterion |
| --- | --- | --- |
| ARC-01 | WHEN an index lacks a required range, lane or generation, THEN planning SHALL fall back or fail, never certify absence. | Compare against an exhaustive small-graph scan; any false-negative edge kills H0. |
| ARC-02 | WHEN one batch exceeds the cap or a stream stalls, THEN ingest SHALL keep resident bytes bounded and report incomplete progress. | Oversized strings/Arrow rows, skewed streams, crash/replay; no successful partial epoch, no unlimited spool. |
| ARC-03 | WHEN H1 returns a reached set, ordered traversal or path, THEN it SHALL satisfy that exact selected contract, including filters/targets/depth and ID mapping. | Independent queue BFS plus pinned GDS ordered-output fixtures; same set with wrong order fails ordered mode. |
| ARC-04 | WHEN H2 spills rank/message state, THEN scores, recurrence, stopping metadata and weighting SHALL meet the pinned contract. | Tiny reference recurrence, pinned GDS fixtures, mass/residual checks where applicable, slow-convergence and dangling-node cases. Max-iterations exhaustion is not convergence. |
| ARC-05 | WHEN H3 is run with zero and positive cutoffs, THEN all eligible topK positives and required zero ties SHALL match exhaustive Jaccard. | Exhaustive set oracle for small graphs, all-disjoint and all-common-neighbor adversaries, asymmetric filters and deletions; any missed eligible candidate kills it. |
| ARC-06 | WHEN n grows while b is fixed, THEN H3 queue payload SHALL scale with b*k; total process/VM use SHALL remain within the measured envelope. | Include neighbor/posting caches, spool aggregation, output and verifier; queue-only success is insufficient. |
| ARC-07 | WHEN refresh publishes a new epoch or crashes, THEN every answer SHALL use exactly one complete generation. | Update one edge/weight/property, delete a bridge, rotate ID map, crash at each publication step; old/new coexistence charged. |
| ARC-08 | WHEN two accepted jobs or a refresh overlap, THEN summed live use SHALL respect the machine envelope and measured scratch cap. | 4,000,000,000-byte VM/cgroup plan with documented OS/page-cache accounting and no hidden swap. Queuing is allowed; latency impact reported. |
| ARC-09 | WHEN evaluating usefulness, THEN at least one exact target workload beyond the resident-topology baseline SHALL complete including ingestion/build/export/verification. | Compare compressed GDS, Ladybug, and simple CSR/custom code on same semantics. Only refusing hard jobs fails the product experiment. |

Benchmark ladder: small oracle fixtures first; moderate graphs where every comparator completes second; disk-resident graphs with skew, chains, hubs, disconnected components and wide frontiers third; repeated-query/update mixes last. Record cold and warm timings, CPU, bytes read/written, random I/Os, peak resident/process/whole-machine use, peak disk, output cardinality, completed/refused/failed jobs and refresh staleness. Do not average failures out of the latency distribution.

Suggested decision rule, not a measured claim: retain H3 only if it completes a buyer-relevant exact workload under the full envelope and improves either end-to-end cost or the feasible graph size without exceeding the buyer's deadline. Retain H1 only if layout/ordering overhead amortizes at the buyer's actual request/update frequency. H2 can be useful when slower but completing is explicitly valuable, not when a deadline-bound buyer cannot use the output.

## Remaining Questions

1. Does GDS's exact zero-score tie behavior admit a stable efficient completion order, or must H3 expose a separately named deterministic contract?
2. How much candidate spool does the target industrial/recommendation graph produce under its actual degree/filter distribution?
3. Can buyers reuse one snapshot/configuration long enough to amortize ID mapping, sorting, index builds and refresh?
4. Does a byte-bounded epoch builder still meet useful freshness targets under real source skew and large records?
5. Which resource phases actually exceed the 4 GB envelope in Ladybug and compressed GDS on matched jobs? Do not assume topology is the dominant cost.

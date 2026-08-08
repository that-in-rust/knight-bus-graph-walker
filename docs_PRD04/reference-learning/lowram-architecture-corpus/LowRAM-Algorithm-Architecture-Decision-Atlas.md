# Low-RAM Algorithm Architecture Decision Atlas

## Binding Decision

This atlas is subordinate to `A007-spc-founder-interview-prep-v7.md`. Knight Bus is an immutable artifact-to-answer graph analytical runner, not a transactional database. Neo4j, GDS, openCypher, TestKit, and official drivers are compatibility or verification oracles. They do not require copying Neo4j's kernel, record store, MVCC, WAL, routing, or cluster architecture.

Every accepted request resolves to exactly one versioned `fit`, `spill`, `approximate`, or `refuse` plan. `fit` and `spill` preserve the declared exact semantics. `approximate` is opt-in and carries a quality contract. `refuse` is a successful planning outcome when correctness, RAM, temporary storage, time, or determinism cannot be honored.

The shared artifact is intentionally small: dense ID correspondence, selected property columns, manifest/checksums/statistics, and only the topology views justified by named profiles. Forward CSR, reverse CSC, edge blocks, destination tiles, degree-oriented adjacency, postings, quotient levels, and embedding stripes are content-addressed derived views whose preparation cost is never hidden.

## Common Cost Contract

The notation and audit rules are defined in `architecture-synthesis-rubric.md`. All equations below are upper-bound design models to calibrate, not measured product deltas. Mapped pages, decoder buffers, allocator slack, worker replicas, output, conversion high water, and supervisor headroom are charged. Persistent disk bytes and temporary spill bytes are reported separately from resident memory.

The cross-family portfolio and working-set discipline are independently supported by the PRD04 innovation atlas and algorithm-storage decision analysis (`A05-000008`, `A05-000014`), while the binding product boundary is the founder brief (`A05-000009`). These are design inputs, not benchmark proof.

```text
B_total_upper = B_os + B_topology_resident + B_properties_resident
              + B_algorithm_state + B_workers(t) + B_io + B_out
              + B_allocator_slack

admit only when B_total_upper <= B_ram
```

## Selection Summary

| Family | Default fit | Default exact spill | Experimental alternate | Product order |
|---|---|---|---|---|
| Paths/BFS | `ARCH-PATH-001` | `ARCH-PATH-002` | `ARCH-PATH-003` | First founder slice |
| WCC | `ARCH-WCC-001` | `ARCH-WCC-002` | `ARCH-WCC-003` | Second kernel |
| PageRank | `ARCH-PAGERANK-001` | `ARCH-PAGERANK-002` | `ARCH-PAGERANK-003` | First iterative proof |
| NodeSimilarity | `ARCH-NODESIM-001` | `ARCH-NODESIM-002` | `ARCH-NODESIM-003` | Earn with buyer evidence |
| kNN | `ARCH-KNN-001` | `ARCH-KNN-002` | `ARCH-KNN-003` | Defer unless paid |
| Louvain | `ARCH-LOUVAIN-001` | `ARCH-LOUVAIN-002` | `ARCH-LOUVAIN-003` | Defer unless paid |
| Leiden | `ARCH-LEIDEN-001` | `ARCH-LEIDEN-002` | `ARCH-LEIDEN-003` | After Louvain proof |
| Triangles | `ARCH-TRIANGLE-001` | `ARCH-TRIANGLE-002` | `ARCH-TRIANGLE-003` | After WCC/PageRank |
| FastRP | `ARCH-FASTRP-001` | `ARCH-FASTRP-002` | `ARCH-FASTRP-003` | Defer unless paid |

## Paths And BFS

### ARCH-PATH-001: Dense-ID CSR With Bounded Double Frontier

**Algorithms:** PATH
**Plan classes:** fit
**Decision:** choose
**Storage layout:** One selected forward or reverse CSR view with sorted dense-ID adjacency; a dense visited bitset; two pre-reserved frontier vectors; optional distance and predecessor sidecars only when the requested result requires them.
**Working-set model:** `B_path_fit = B_os + B_io + B_out + ceil(n/8) + 2*f*b_e + n*b_distance_if_requested + reached*b_predecessor_if_requested + t*B_frontier_local <= B_ram`.
**Latency/I/O tradeoff:** Expected work is proportional to examined adjacency plus emitted rows; warm fit mode minimizes indirection, while cold latency remains page-fault and topology-layout dependent.
**Predictability mechanism:** Admit from hop cap, output cap, degree histogram, maximum-degree bound, frontier upper bound, orientation availability, and explicit predecessor policy; reserve both frontier generations before traversal.
**Applicability:** Default exact plan for access paths, dependency reachability, blast radius, bounded BFS distance, and security/IAM/SBOM workflows whose state fits.
**Refusal condition:** Refuse before traversal when visited state plus the minimum double frontier and required result lower bound exceed `B_ram`, when path enumeration lacks an output cap, or when the requested orientation cannot be prepared within its charged conversion peak.
**Verification:** Compare reachable sets and distances with a tiny mathematical oracle and GDS BFS; test empty, cycle, diamond, hub, chain, duplicate-edge, one-byte-below budget, cancellation, and slow-consumer cases; require frontier and output receipt terms to match measured high water.
**Evidence:** GDS BFS kernel and state `A02-007002`, `A02-007004`; bounded-artifact and frontier analysis `A04-000021`, `A06-000026`, `A06-000035`; current canonical walk seam `KW-CURRENT-001`.

### ARCH-PATH-002: Partitioned External Frontier Runs

**Algorithms:** PATH
**Plan classes:** spill
**Decision:** choose
**Storage layout:** CSR or source-partitioned edge blocks; level-tagged sorted frontier runs; partitioned visited bitmaps or Roaring containers; deterministic merge/deduplicate; append-only predecessor records only when requested.
**Working-set model:** `B_path_spill = B_os + B_io + B_out + B_visited_window + 2*B_frontier_run + B_merge(t) + B_predecessor_writer <= B_ram`, with `B_tmp >= frontier_run_upper + predecessor_log_upper`.
**Latency/I/O tradeoff:** Exactness is preserved at the cost of run generation, merge, and potentially repeated adjacency reads per level; sequential I/O is preferred and bytes/pass are receipt-visible.
**Predictability mechanism:** Fixed partition count and recursion rule, bounded merge fan-in, maximum run count, hop cap, temporary-space reservation, and deterministic `(level,node_id)` ordering make the trade explicit.
**Applicability:** Exact low-RAM reachability when a dense frontier or predecessor plane cannot remain resident but a bounded level/run schedule and temporary quota are legal.
**Refusal condition:** Refuse when the conservative frontier/predecessor temporary bound exceeds `B_tmp`, one partition remains larger than the minimum legal buffer after recursive partitioning, disk-full safety cannot be reserved, or the time/pass ceiling is incompatible with the hop cap.
**Verification:** Force spill with tiny buffers and compare exact sets/distances against fit mode and GDS; inject disk-full, corrupt run, crash during merge, cancellation, and skewed hub partitions; prove idempotent cleanup and stable output checksum.
**Evidence:** GDS BFS state `A02-007002`, `A02-007004`; low-RAM spill precedent `A04-000021`; delta buckets and bounded partitions `A06-000030`; Knight Bus external-run primitive `KW-CURRENT-003`.

### ARCH-PATH-003: Bidirectional Sparse-Dense Hysteresis

**Algorithms:** PATH
**Plan classes:** fit, hybrid
**Decision:** experiment
**Storage layout:** Dual forward/reverse CSR only for repeated point-to-point profiles; each side selects sorted-vector, Roaring, or dense-bitset frontier containers using deterministic density thresholds and hysteresis; stable meet and parent tie rules.
**Working-set model:** `B_path_bidir = B_os + B_io + B_out + B_dual_resident + B_seen_forward + B_seen_reverse + 2*f*b_e + B_parent_policy <= B_ram`.
**Latency/I/O tradeoff:** Can reduce explored edges dramatically on point-to-point graphs but pays a second orientation, two visited domains, switching overhead, and unpredictable benefit on directed or highly asymmetric graphs.
**Predictability mechanism:** Profile eligibility requires point-to-point semantics, measured direction selectivity, a dual-view reuse threshold, fixed container-switch thresholds, and a worst-case fallback charged as two bounded frontiers.
**Applicability:** Repeated exact source-target shortest-path workloads where both orientations already exist or their amortized preparation cost is justified.
**Refusal condition:** Refuse this option when reverse topology is absent and reuse cannot amortize it, when legal directed semantics prevent reverse expansion, or when the dual visited/frontier lower bound exceeds `B_ram`; fall back to `ARCH-PATH-001` or `002`.
**Verification:** Differentially compare distance and path validity, not arbitrary tied path identity; sweep frontier density and graph directionality; verify switching hysteresis, dual-view preparation accounting, and identical answers across fallback choices.
**Evidence:** Bounded traversal surface `A02-000312`; dual/topology decision evidence `A04-000021`; push/pull switching and Roaring layouts `A06-000035`, `A06-000076`.

## Weakly Connected Components

### ARCH-WCC-001: Packed Deterministic Union-Find Stream

**Algorithms:** WCC
**Plan classes:** fit
**Decision:** choose
**Storage layout:** One logical undirected edge stream or CSR orientation; packed parent IDs at the narrowest proven width; optional rank/size plane; stable smaller-root union rule; deterministic final compression and component-ID canonicalization.
**Working-set model:** `B_wcc_fit = B_os + B_io + B_out + n*b_parent + n*b_rank + B_edge_block + t*B_union_proposals <= B_ram`.
**Latency/I/O tradeoff:** Usually one edge scan plus bounded compression/canonicalization passes; packed arrays improve locality, while deterministic parallel proposal merging may be slower than unconstrained atomics.
**Predictability mechanism:** Parent/rank bytes are exact from `n` and widths; edge blocks and worker proposal buffers are pre-reserved; final labels use canonical minimum member rather than schedule-dependent roots.
**Applicability:** Default exact WCC plan and second end-to-end proof for dependency islands, IAM/account clusters, and SBOM components.
**Refusal condition:** Refuse fit mode when parent/rank plus required streamed or materialized component output and edge buffers exceed `B_ram`, or when the artifact cannot provide stable undirected semantics.
**Verification:** Compare partition isomorphism with GDS WCC and a reference union-find; relabel nodes metamorphically; test isolates, duplicate edges, disconnected stars, one giant component, and deterministic component checksums across worker schedules.
**Evidence:** GDS WCC kernel/state `A02-007155`, `A02-007156`; compact connectivity analysis `A04-000021`, `A04-000135`; hooking/component oracle `A06-000018`.

### ARCH-WCC-002: Mapped Labels With Partitioned Edge Passes

**Algorithms:** WCC
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Edge blocks grouped by stable node ranges; disk-backed current/next label planes or mapped parent pages; bounded hot-page cache; external minimum-label update runs; deterministic reduce and compression passes.
**Working-set model:** `B_wcc_spill = B_os + B_io + B_out + 2*B_label_window + B_edge_block + 2*B_update_run + t*B_partition_local <= B_ram`, with `B_tmp >= 2*n*b_parent + update_runs_upper`.
**Latency/I/O tradeoff:** Exact but requires repeated sequential edge/label passes until no changes; random parent faults are avoided or measured by partition ownership, trading elapsed time for a much smaller resident state.
**Predictability mechanism:** Declare maximum passes, page/cache window, update-run fan-in, partition skew thresholds, and temporary quota; progress receipts expose changed-label count per pass.
**Applicability:** Whole-graph exact WCC when the irreducible parent/label plane does not fit but stable external passes fit the RAM, disk, and time contract.
**Refusal condition:** Refuse when two minimum label windows and merge buffers cannot fit `B_ram`, mapped-page charging policy is unavailable, temporary bytes exceed `B_tmp`, or the allowed pass/time ceiling cannot cover the declared worst case.
**Verification:** Force one-page label windows; compare partition isomorphism with fit mode/GDS; measure page faults and pass counts; inject corrupt update runs, disk exhaustion, restart, cancellation, and adversarial long-chain convergence.
**Evidence:** GDS WCC state `A02-007155`, `A02-007156`; strict spill/accounting prior `A04-000021`, `A04-000023`; hooking/shortcut pass model `A06-000018`.

### ARCH-WCC-003: Hook-Shortcut With Exact Residual Scan

**Algorithms:** WCC
**Plan classes:** fit, hybrid
**Decision:** experiment
**Storage layout:** Two or three packed parent/hook planes, active-node bitset or Roaring set, Shiloach-Vishkin-style hooking and pointer-jumping passes, optional Afforest sampling followed by a mandatory complete residual edge scan.
**Working-set model:** `B_wcc_hook = B_os + B_io + B_out + 3*n*b_parent + B_active(a) + B_edge_block + t*B_hook_proposals <= B_ram`.
**Latency/I/O tradeoff:** Uses more node state than union-find but may reduce diameter-sensitive passes and contention; sampling accelerates only when the exact residual scan remains mandatory.
**Predictability mechanism:** Stable hook direction, bounded pointer-jump rounds, active-set density thresholds, maximum pass count, and mandatory residual validation prevent a heuristic from silently changing exactness.
**Applicability:** Large skewed graphs where measured pass savings justify extra node planes and where exact WCC remains required.
**Refusal condition:** Refuse this option when the additional parent planes do not fit, active-set or proposal skew exceeds its bound, or the residual scan cannot complete within the time/I/O contract; select `001` or `002` instead.
**Verification:** Compare exact partition isomorphism across union-find, hook-shortcut, and GDS; adversarially defeat the sample; require residual scan to find planted cross-component edges; test deterministic roots and pass receipts.
**Evidence:** GDS WCC implementation `A02-007155`; WCC exact-plan prior `A04-000021`; component hooking/shortcutting and metamorphic oracle `A06-000018`, `A06-000019`.

## PageRank

### ARCH-PAGERANK-001: Pull CSC With Two Rank Planes

**Algorithms:** PAGERANK
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Reverse CSC/destination-owned adjacency, outdegree sidecar, current and next rank planes at declared precision, fixed destination partitions, stable dangling-mass and reduction order.
**Working-set model:** `B_pagerank_fit = B_os + B_io + B_out + B_csc_resident + n*b_degree + 2*n*b_s + t*B_reduction <= B_ram`.
**Latency/I/O tradeoff:** Fastest retained repeat-run candidate because each iteration performs locality-friendly pull scans without generic message queues; preparing CSC can dominate a cold single use.
**Predictability mechanism:** Pin precision, iteration cap, tolerance, destination partitioning, worker count, floating mode, and CSC preparation/reuse; rank/output lower bounds are known from `n`.
**Applicability:** Default exact-to-declared-tolerance PageRank plan when two rank planes and the admitted topology envelope fit; first iterative architecture proof.
**Refusal condition:** Refuse fit mode when CSC preparation peak is unbudgeted, two rank planes plus output exceed `B_ram`, or the requested tolerance/strict reduction cannot be honored within the iteration/time cap.
**Verification:** Compare score epsilon, mass, non-negativity, top-K overlap, iterations, and convergence status with GDS and a small reference; test dangling nodes, disconnected graph, equal scores, weighted edges, and strict repeatability.
**Evidence:** GDS PageRank/Pregel state `A02-006976`, `A02-006978`; algorithm-specific tiled prior `A04-000021`, `A04-000031`; convergence and pull evidence `A06-000061`, `A06-000026`.

### ARCH-PAGERANK-002: Destination-Tiled Streaming Pull

**Algorithms:** PAGERANK
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Destination-partitioned edge tiles; disk-backed current rank plane; one bounded destination accumulator tile; sequential tile schedule; external output rank plane swapped atomically after each complete iteration.
**Working-set model:** `B_pagerank_spill = B_os + B_io + B_out + B_rank_source_window + B_destination_tile + B_edge_tile + t*B_reduction <= B_ram`, with `B_tmp >= 2*n*b_s + tile_metadata`.
**Latency/I/O tradeoff:** Preserves exact declared iteration semantics while multiplying edge/rank I/O; each iteration has deterministic bytes/pass and bounded accumulators, making runtime bandwidth-dominated and explainable.
**Predictability mechanism:** Fix destination ownership, tile byte ceiling, source-rank window policy, iteration cap, stable reduction, prefetch depth, and temporary planes; receipt reports bytes and faults per iteration.
**Applicability:** Exact low-RAM PageRank when full rank/topology residency is illegal but repeated sequential scans fit disk and elapsed-time contracts.
**Refusal condition:** Refuse when even one destination accumulator and source window exceed `B_ram`, two external rank planes exceed `B_tmp`, tile skew defeats recursive partitioning, or required iterations exceed the admitted I/O/time envelope.
**Verification:** Force tiny destination tiles and compare every iteration or final epsilon with fit/GDS; test tile skew, crash before plane swap, disk-full, strict checksum replay, cold/warm separation, and estimator error by phase.
**Evidence:** GDS PageRank state `A02-006976`, `A02-006978`; strict tiled plan prior `A04-000021`, `A04-000023`; iteration and spill evidence `A06-000061`, `A06-000062`.

### ARCH-PAGERANK-003: Deterministic Residual Active-Set Propagation

**Algorithms:** PAGERANK
**Plan classes:** fit, hybrid, approximate
**Decision:** experiment
**Storage layout:** Rank and residual planes, active-node sorted vector/Roaring/bitset chosen by density, forward adjacency for sparse push and reverse CSC for dense pull, deterministic hysteresis and residual scheduling.
**Working-set model:** `B_pagerank_residual = B_os + B_io + B_out + n*b_s + n*b_residual + B_active(a) + B_selected_topology_resident + t*B_delta <= B_ram`.
**Latency/I/O tradeoff:** Can avoid full scans when residual activity is sparse but may degrade to more state and switching overhead than pull on dense or slowly converging graphs.
**Predictability mechanism:** Declare residual norm, maximum activations/iterations, sparse/dense thresholds, stable node-ID processing, and a worst-case fallback estimate equal to the admitted dense plan.
**Applicability:** Experiment for localized personalization/source-weight updates or top-K outcomes; may be exact-to-tolerance when the residual bound proves it, otherwise explicitly approximate.
**Refusal condition:** Refuse this option when no residual-to-error bound is implemented, activity can exceed the admitted dense fallback, dual orientations are not justified, or strict compatibility demands a different pinned iteration policy.
**Verification:** Check residual/error inequality, mass and non-negativity, compare against full pull/GDS on held-out shapes, sweep active density, replay exact seed/order, and ensure approximate receipts never claim strict GDS identity.
**Evidence:** GDS PageRank/Pregel behavior `A02-006976`; PageRank experiment prior `A04-000021`; convergence and push/pull hysteresis `A06-000061`, `A06-000035`.

## NodeSimilarity

### ARCH-NODESIM-001: Incidence Postings With Bounded Top-K

**Algorithms:** NODESIM
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Sorted neighborhood or feature-incidence postings; degree/component filters; candidate counts reduced by stable pair key; packed per-node top-K arrays or heaps with deterministic score and node-ID tie order.
**Working-set model:** `B_nodesim_fit = B_os + B_io + B_out + B_postings_resident + B_candidates(C) + n*k*b_pair + t*B_score_local <= B_ram`.
**Latency/I/O tradeoff:** Avoids scoring pairs that share no indexed feature/neighbor, but runtime and memory still depend on posting skew and candidate count rather than only `n` and `m`.
**Predictability mechanism:** Manifest stores posting-degree histograms and pair-count upper bounds; planner applies degree/component/cutoff filters, caps `C`, reserves top-K state, and refuses an unbounded full result.
**Applicability:** Exact Jaccard/overlap/cosine neighborhood similarity with positive-overlap candidate semantics, bounded `k` or `topN`, and buyer-approved filters.
**Refusal condition:** Refuse fit mode when any posting or total candidate upper bound defeats the admitted partition/candidate cap, when `n*k*b_pair` plus output exceeds `B_ram`, or when requested all-pairs zero-overlap semantics require quadratic materialization.
**Verification:** Compare supplied-pair scores and deterministic top-K with GDS/independent set math; test hubs, identical neighborhoods, no overlap, ties, components, weights, cutoff boundaries, and candidate-count estimator calibration.
**Evidence:** GDS NodeSimilarity and top-K state `A02-007086`, `A02-007087`; hard-family/candidate analysis `A04-000022`; incidence and compressed-set evidence `A06-000041`, `A06-000076`.

### ARCH-NODESIM-002: External Pair Runs And Top-K Merge

**Algorithms:** NODESIM
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Stream incidence postings; emit bounded `(node_a,node_b,partial)` pair runs; range/hash partition, external sort/reduce exact intersection statistics, and merge per-partition top-K runs without retaining `n*k` objects.
**Working-set model:** `B_nodesim_spill = B_os + B_io + B_out + B_posting_block + 2*B_pair_run + B_reduce + B_topk_partition <= B_ram`, with `B_tmp >= C*b_candidate + n*k*b_pair`.
**Latency/I/O tradeoff:** Exact but can generate and sort many pair records; I/O is sequential and bounded by the precomputed candidate/wedge upper bound, which may itself be prohibitive on hubs.
**Predictability mechanism:** Compute `sum choose(posting_degree,2)` with checked arithmetic, recursively partition hot postings, fix merge fan-in, reserve temporary bytes, and expose candidates generated/pruned/retained.
**Applicability:** Exact similarity under tight RAM when candidate and temporary bounds are large but finite and the elapsed-time contract accepts external sort/reduce.
**Refusal condition:** Refuse when the candidate/wedge upper bound overflows or exceeds `B_tmp`/time, a hot posting cannot be split without changing semantics, required output is unbounded, or minimum sort/merge buffers exceed `B_ram`.
**Verification:** Compare pair scores/top-K with fit/GDS, force multi-run merges, test a single enormous posting, inject duplicate partials/corruption/disk-full/cancel, and verify deterministic pair ordering and cleanup.
**Evidence:** GDS NodeSimilarity estimator `A02-007086`, `A02-007087`; exact spill/candidate prior `A04-000022`, `A04-000025`; posting and partition synthesis `A06-000041`, `A06-000066`.

### ARCH-NODESIM-003: MinHash-LSH Candidate Gate With Exact Rescore

**Algorithms:** NODESIM
**Plan classes:** approximate, hybrid
**Decision:** experiment
**Storage layout:** Fixed-width MinHash signatures, deterministic LSH buckets, bounded candidate set, then exact set intersection for retained candidates and bounded top-K output.
**Working-set model:** `B_nodesim_lsh = B_os + B_io + B_out + n*b_signature + B_lsh_buckets + B_candidates(C) + B_exact_rescore + n*k*b_pair <= B_ram`.
**Latency/I/O tradeoff:** Substantially reduces candidate work when signatures separate dissimilar nodes, but consumes signature/index bytes and introduces recall risk sensitive to graph shape and tuning.
**Predictability mechanism:** Pin hash family/seed/bands, cap bucket and candidate sizes, spill oversized buckets, and admit only with a declared recall@K or false-negative contract on a versioned audit corpus.
**Applicability:** Approximate discovery workloads where exact scoring of retained candidates is useful and the buyer accepts measured recall rather than all-pairs completeness.
**Refusal condition:** Refuse when approximation is not authorized, no held-out recall audit exists, bucket skew exceeds the legal spill plan, or quality falls below the declared threshold; never relabel a cap as exact.
**Verification:** Compare recall@K and score error against exact plans on held-out graph families, stress adversarial identical signatures/hubs, replay seeds, and require exact rescoring plus receipt-visible candidate and quality counts.
**Evidence:** GDS similarity state baseline `A02-007086`; approximation risk prior `A04-000022`; compressed sets and ANN/candidate lessons `A06-000076`, `A06-000049`.

## K-Nearest Neighbors

### ARCH-KNN-001: Exact Feature-Tiled All-Pairs Top-K

**Algorithms:** KNN
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Columnar feature planes; two fixed node-feature tiles; packed `n*k` neighbor/score arrays; deterministic tile-pair schedule, metric, score order, and tie rule.
**Working-set model:** `B_knn_fit = B_os + B_io + B_out + 2*B_tile*d*b_s + n*k*b_neighbor + t*B_distance_local <= B_ram`.
**Latency/I/O tradeoff:** Produces mathematically exact top-K but requires quadratic distance work; RAM is bounded and regular while elapsed time can become the disqualifying resource.
**Predictability mechanism:** Exact `n`, `d`, `k`, tile size, metric cost, and output lower bound feed both RAM and operation/time admission; no random candidate structure is hidden.
**Applicability:** Small or moderate exact vector/property kNN jobs, gold-oracle generation, and paid workloads where exactness outweighs `O(n^2*d)` compute.
**Refusal condition:** Refuse when packed `n*k` state plus two minimum feature tiles exceeds `B_ram`, feature/output bytes violate policy, or the operation/time upper bound exceeds the job contract even though RAM fits.
**Verification:** Compare exact neighbors/distances with brute-force independent math and GDS-compatible fixtures; test ties, NaN/null policy, metric variants, tile boundaries, deterministic ordering, and one-byte budget thresholds.
**Evidence:** GDS kNN neighbor/sampler state `A02-007053`, `A02-007056`; hard-family state analysis `A04-000022`; graph/vector workload synthesis `A06-000041`, `A06-000090`.

### ARCH-KNN-002: Query-Block Exact Scan With External Top-K Output

**Algorithms:** KNN
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Columnar feature artifact scanned reference-tile by reference-tile for one bounded query block; only `B_query*k` heaps resident; completed top-K blocks written as immutable sorted output runs.
**Working-set model:** `B_knn_spill = B_os + B_io + B_out + 2*B_tile*d*b_s + B_query*k*b_neighbor + t*B_distance_local <= B_ram`, with `B_tmp >= n*k*b_neighbor`.
**Latency/I/O tradeoff:** Exact and strongly RAM bounded but repeatedly reads the reference feature plane for query blocks; wall time and I/O can be very large even though state no longer scales as resident `n*k`.
**Predictability mechanism:** Fixed query/reference tile sizes, scan order, distance-operation count, bytes/pass, output-run quota, and time estimate; planner can refuse on compute or I/O before allocation.
**Applicability:** Exact offline kNN under a hard RAM cap when temporary output fits and the user explicitly accepts long batch execution.
**Refusal condition:** Refuse when two minimum feature tiles exceed `B_ram`, output lower bound exceeds `B_tmp`, feature scans exceed the time/I/O contract, or the metric cannot be streamed without unbounded auxiliary state.
**Verification:** Force single-query blocks, compare with fit/brute-force, validate every output run and merge, inject restart/disk-full/cancel, and report distance evaluations, bytes read, output bytes, and estimate error.
**Evidence:** GDS kNN state `A02-007053`, `A02-007056`; bounded candidate/output prior `A04-000022`; partitioned probe and quantization evidence `A06-000049`, `A06-000068`.

### ARCH-KNN-003: DiskANN-IVF-PQ Bounded Probe Profile

**Algorithms:** KNN
**Plan classes:** approximate, hybrid
**Decision:** experiment
**Storage layout:** Versioned IVF or Vamana/DiskANN sidecar, optional product-quantized vectors, bounded probe/beam, and exact full-precision rescore of a capped candidate set.
**Working-set model:** `B_knn_ann = B_os + B_io + B_out + B_centroids + B_probe*B_sector + B_candidates(C) + B_exact_rescore + k*b_neighbor <= B_ram`.
**Latency/I/O tradeoff:** Replaces quadratic scans with bounded probes and compact scoring, but index build/reuse economics and recall vary with vector distribution; random reads must be sector-shaped and counted.
**Predictability mechanism:** Pin index hash, training sample, seed, probe/beam/candidate caps, sector cache, quantizer, and recall@K contract; preparation and warm query metrics remain separate.
**Applicability:** Future approximate vector/property kNN after a paid workflow exists and after exact `ARCH-KNN-001/002` provide the quality oracle.
**Refusal condition:** Refuse when approximation is forbidden, index provenance or recall corpus is missing, probe/candidate caps cannot fit `B_ram`, or the view build cost has no measured reuse justification.
**Verification:** Measure recall@K, distance ratio, tail reads, and exact-rescore correctness against exact tiled scans across held-out distributions; test corrupted index, cold cache, seed replay, and capped-beam degradation.
**Evidence:** GDS kNN compatibility baseline `A02-007053`; hard-family defer evidence `A04-000022`; DiskANN, IVF, and PQ layouts `A06-000032`, `A06-000049`, `A06-000068`.

## Louvain

### ARCH-LOUVAIN-001: Packed Level-At-A-Time Local Move

**Algorithms:** LOUVAIN
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Weighted logical-undirected CSR; packed community labels, node/community volumes and gains; bounded worker influence maps; one current quotient level plus requested hierarchy planes.
**Working-set model:** `B_louvain_fit = B_os + B_io + B_out + B_level_resident(m) + n*b_label + n*b_volume + B_community(c) + t*B_influence_cap + B_hierarchy <= B_ram`.
**Latency/I/O tradeoff:** Keeps local moves and current level locality-friendly, but heuristic convergence, influence-map skew, and quotient size make elapsed time less predictable than WCC/PageRank.
**Predictability mechanism:** Pin seed, node order, tie rule, worker count, maximum levels/iterations, influence-map cap, hierarchy retention, and per-level quotient upper bound.
**Applicability:** Pinned-policy Louvain compatibility or buyer workflow whose current level, workspace, and requested hierarchy fit; `choose` means default within that earned profile, not initial roadmap priority.
**Refusal condition:** Refuse fit mode when any level's topology plus minimum label/volume/influence state exceeds `B_ram`, skew defeats the influence cap, or strict reproducibility is requested for an unsupported parallel schedule.
**Verification:** Compare modularity and partition relation or pinned-policy output with GDS/tiny oracle; test disconnected cliques, rings, weighted ties, seeds, level transitions, worker schedules, and hierarchy/output accounting.
**Evidence:** GDS Louvain estimator/kernel `A02-006940`, `A02-006942`; contracted-community evidence `A04-000022`, `A04-000160`; graph-family synthesis `A06-000041`.

### ARCH-LOUVAIN-002: Sealed Quotient Levels With External Contraction

**Algorithms:** LOUVAIN
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Disk-backed labels/volumes; partition-local move proposals; deterministic external sort/reduce of `(community_u,community_v,weight)` into a content-addressed quotient artifact; release prior-level scratch after atomic seal.
**Working-set model:** `B_louvain_spill = B_os + B_io + B_out + B_label_window + B_volume_window + B_move_partition + 2*B_edge_run + t*B_influence_cap <= B_ram`, with `B_tmp >= quotient_runs_upper + hierarchy_output`.
**Latency/I/O tradeoff:** Exact execution of the pinned heuristic policy is preserved through extra proposal/contraction passes and temporary quotient I/O; sealing levels improves auditability and restart behavior.
**Predictability mechanism:** Stable partition/node order, proposal merge rule, bounded influence maps, run fan-in, level/pass caps, quotient size estimate, and temporary reservation per level.
**Applicability:** Low-RAM Louvain when full current/next level state cannot remain resident but deterministic level artifacts and elapsed-time budget are acceptable.
**Refusal condition:** Refuse when a minimum move partition or contraction merge cannot fit `B_ram`, quotient temporary upper bound exceeds `B_tmp`, influence skew lacks a legal fallback, or maximum levels/passes exceed time policy.
**Verification:** Compare modularity/partition with fit and GDS, force many quotient runs, crash before/after level seal, inject disk-full/corrupt edge aggregate, and verify stable restart, cleanup, and per-level receipt hashes.
**Evidence:** GDS aggregate-graph and hierarchy state `A02-006940`, `A02-006942`; hard-family quotient prior `A04-000022`, `A04-000024`; content-addressed view synthesis `A06-000041`, `A06-000020`.

### ARCH-LOUVAIN-003: Active-Community Hot-Cold Delta Plane

**Algorithms:** LOUVAIN
**Plan classes:** fit, hybrid, approximate
**Decision:** experiment
**Storage layout:** Stable label plane; Roaring active-node set; bounded hot community-volume/influence cache with cold mapped backing; only dirty edge partitions revisited; optional reduced-level/early-stop quality profile.
**Working-set model:** `B_louvain_delta = B_os + B_io + B_out + n*b_label + B_active(a) + B_hot_community + B_dirty_partitions + t*B_influence_cap <= B_ram`.
**Latency/I/O tradeoff:** May avoid full passes after moves become sparse, but worst-case activity returns to full scans and cache churn; early stopping lowers work only as an explicit approximate policy.
**Predictability mechanism:** Deterministic active-set threshold, hot-cache cap, dirty-partition upper bound, stable move ordering, full-scan fallback reservation, and modularity-delta stopping contract.
**Applicability:** Experiment on graphs with demonstrably sparse late-level activity or a buyer-approved early-stop quality target.
**Refusal condition:** Refuse this option when the full-scan fallback is not admissible, hot-community skew breaks the cache bound, approximate stopping is unauthorized, or deterministic scheduling cannot be reproduced.
**Verification:** Compare exact completion with full level-at-a-time plan, measure active density/cache faults, adversarially trigger global moves, and evaluate modularity/stability distributions for early-stop mode across pinned seeds.
**Evidence:** GDS Louvain state `A02-006940`; contracted-community risk `A04-000022`; graph view and compressed active-set synthesis `A06-000041`, `A06-000076`.

## Leiden

### ARCH-LEIDEN-001: Refine-One-Community Fit Arena

**Algorithms:** LEIDEN
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Weighted undirected CSR; packed current/refined/parent community planes, weights and dendrogram; one pre-reserved refinement arena sized for the largest admitted community; stable seed/order policy.
**Working-set model:** `B_leiden_fit = B_os + B_io + B_out + B_level_resident(m) + n*(b_label+b_refined+b_parent+b_weight) + B_refine(max_community) + B_hierarchy <= B_ram`.
**Latency/I/O tradeoff:** Avoids retaining refinement scratch for all communities simultaneously, but a giant community can make the single arena nearly graph-sized and refinement remains heuristic/schedule sensitive.
**Predictability mechanism:** Precompute community size/edge histograms at each level, reserve the largest refinement arena, pin gamma/theta/seed/order/levels, and reject a level before entering refinement if its phase bound fails.
**Applicability:** Earned Leiden profile where all level arrays and the largest-community refinement workspace fit; default only after Louvain and a paid community use case.
**Refusal condition:** Refuse fit mode when any mandatory per-node plane, largest-community refinement arena, quotient level, or hierarchy output exceeds `B_ram`, or when the requested reproducibility mode is unsupported.
**Verification:** Compare quality, connectivity refinement invariants, hierarchy shape, and pinned-seed stability with GDS/tiny fixtures; test one giant community, many tiny communities, weighted ties, and phase-specific estimates.
**Evidence:** GDS Leiden estimator/kernel `A02-006928`, `A02-006930`; hard community/refinement evidence `A04-000022`, `A04-000103`; graph-family synthesis `A06-000041`.

### ARCH-LEIDEN-002: Community-Bucketed External Refinement

**Algorithms:** LEIDEN
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Membership runs keyed by current community; edge shards keyed by community; one bounded refinement bucket active at a time; partition-local moves; externally sorted quotient edges and atomically sealed level artifacts.
**Working-set model:** `B_leiden_spill = B_os + B_io + B_out + B_membership_run + B_edge_bucket + B_refine_bucket + 2*B_quotient_run + t*B_move_local <= B_ram`, with `B_tmp >= memberships + edge_shards + quotient_runs`.
**Latency/I/O tradeoff:** Exact execution of the pinned refinement policy trades repeated bucketing, sort/merge, and cross-community scans for a hard resident cap and restartable level boundaries.
**Predictability mechanism:** Bound bucket bytes and cross-community spill, recursively split oversized membership/edge buckets without changing membership semantics, fix merge order, and reserve all temporary classes before level execution.
**Applicability:** Low-RAM Leiden when global arrays can be mapped/windowed and each refinement bucket or recursively legal sub-bucket satisfies RAM/disk/time policy.
**Refusal condition:** Refuse when a community's exact refinement dependency cannot be partitioned into the minimum legal arena, temporary upper bound exceeds `B_tmp`, cross-bucket passes exceed time, or level publication cannot be made atomic.
**Verification:** Compare quality/connectivity/partition relation with fit/GDS, force giant-community spill, inject failures during membership bucketing/refinement/quotient seal, and verify deterministic restart and complete per-phase receipt.
**Evidence:** GDS Leiden refinement/aggregation state `A02-006928`, `A02-006930`; contracted-level evidence `A04-000022`, `A04-000024`; immutable view-foundry synthesis `A06-000041`, `A06-000020`.

### ARCH-LEIDEN-003: Coarsened Seed Then Exact Pinned Refinement

**Algorithms:** LEIDEN
**Plan classes:** hybrid, approximate
**Decision:** experiment
**Storage layout:** Sampled or coarsened quotient produces an initial partition; full graph refinement then runs under bounded fit/spill arenas when exact pinned-policy completion is requested; approximate mode may stop after declared refinement budget.
**Working-set model:** `B_leiden_coarse = B_os + B_io + B_out + B_coarse_graph + n*b_seed_label + B_exact_refine_or_budgeted_refine + B_quality_audit <= B_ram`.
**Latency/I/O tradeoff:** A good seed may reduce moves and levels, but coarsening can hide bridges or create unstable basins; exact completion still pays full validation/refinement, while bounded early stop changes quality semantics.
**Predictability mechanism:** Pin sampling/coarsening seed and rate, cap coarse graph, reserve exact fallback when claimed exact, and define modularity/connectivity/stability thresholds for approximate termination.
**Applicability:** Experiment for very large community workloads after exact fit/spill baselines exist; useful only when repeated evidence shows fewer full-level passes.
**Refusal condition:** Refuse when exact mode lacks a full-graph residual/refinement proof, approximate quality is unauthorized or unaudited, seed instability exceeds policy, or full fallback is required but not admitted.
**Verification:** Plant small bridge communities that coarsening can miss, compare exact-completion output with non-coarsened policy, measure modularity/stability distributions for bounded mode, and replay all seeds/view hashes.
**Evidence:** GDS Leiden baseline `A02-006928`; hard-family defer/quality evidence `A04-000022`, `A04-000025`; oracle and graph-family synthesis `A06-000041`, `A06-000086`.

## Triangle Counting

### ARCH-TRIANGLE-001: Degree-Oriented Sorted Forward Graph

**Algorithms:** TRIANGLE
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Canonical simple-undirected degree order with each edge stored once from lower order to higher; sorted/delta-coded forward lists; merge/galloping intersections; scalar or packed per-node counters.
**Working-set model:** `B_triangle_fit = B_os + B_io + B_out + B_oriented_resident(m_u) + n*b_count_if_local + t*B_intersection <= B_ram`.
**Latency/I/O tradeoff:** Converts topology once, then avoids duplicate edge work and keeps intersection scratch tiny; high-degree forward lists still dominate tail latency and cold decode cost.
**Predictability mechanism:** Manifest records orientation rule, forward-degree histogram, maximum decoded block, expected intersections, local/global output mode, and conversion/reuse cost.
**Applicability:** Default exact triangle/global clustering plan after the degree-oriented view is justified; strong proof that algorithm-shaped topology can reduce work and state.
**Refusal condition:** Refuse fit mode when oriented-view preparation peak is unbudgeted, largest decoded adjacency/intersection buffer or local count output exceeds `B_ram`, or multigraph/self-loop semantics cannot be canonicalized as declared.
**Verification:** Compare exact global/local counts with GDS/reference, enforce `sum(local)=3*global` for simple undirected graphs, permute node/edge order, and stress clique, bipartite, hub, duplicates, and cold conversion.
**Evidence:** GDS sorted-intersection kernel/state `A02-007125`, `A02-007127`; triangle plan/oracle prior `A04-000021`, `A04-000025`; CSR/orientation evidence `A06-000026`.

### ARCH-TRIANGLE-002: Partitioned External Adjacency Intersections

**Algorithms:** TRIANGLE
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Degree-oriented adjacency partitioned by lower endpoint and degree bands; bounded decoded adjacency blocks; deterministic block-pair schedule; external per-node count runs reduced at completion; no materialized wedge set.
**Working-set model:** `B_triangle_spill = B_os + B_io + B_out + 2*B_adjacency_block + t*B_intersection + 2*B_count_run <= B_ram`, with `B_tmp >= per_node_count_runs + partition_index`.
**Latency/I/O tradeoff:** Exact with bounded RAM but may reread hub adjacency blocks across partitions; avoids the potentially explosive temporary volume of explicit wedge enumeration.
**Predictability mechanism:** Degree bands, maximum decoded block, hub replication/read upper bound, fixed block-pair order, output-run quota, and recursive partitioning are computed before execution.
**Applicability:** Exact triangle counts under tight RAM when sorted/oriented topology exists and block reread/time bounds are acceptable.
**Refusal condition:** Refuse when one adjacency list cannot be streamed within the minimum two-block arena, hub reread exceeds I/O/time policy, count runs exceed `B_tmp`, or local output lower bound has no legal stream/spill form.
**Verification:** Force one-block partitions and compare with fit/GDS, test extreme hubs and dense cliques, count decoded/reread bytes, inject run corruption/disk-full/cancel, and verify count identities and cleanup.
**Evidence:** GDS triangle cursor/intersection state `A02-007125`, `A02-007127`; low-state triangle evidence `A04-000021`; orientation/block synthesis `A06-000026`, `A06-000041`.

### ARCH-TRIANGLE-003: Roaring Hubs With Sorted Sparse Lists

**Algorithms:** TRIANGLE
**Plan classes:** fit, hybrid
**Decision:** experiment
**Storage layout:** Degree-oriented sorted lists remain canonical; only nodes above a calibrated density/degree threshold gain Roaring array/bitmap/run containers; intersection dispatch is list-list, list-bitmap, or bitmap-bitmap.
**Working-set model:** `B_triangle_hybrid = B_os + B_io + B_out + B_oriented_resident(m_u) + B_hub_sidecars + n*b_count_if_local + t*B_intersection <= B_ram`.
**Latency/I/O tradeoff:** Bitmap operations can tame hub intersections but sidecars add persistent bytes, conversion, and cache pressure; sparse nodes remain cheaper as lists.
**Predictability mechanism:** Choose thresholds from measured per-container bytes and operation timings, cap total hub-sidecar residency, publish conversion/reuse economics, and keep list-only fallback admitted.
**Applicability:** Power-law graphs with repeated triangle/motif work where a small hub set accounts for a large fraction of intersection cost.
**Refusal condition:** Refuse this option when hub sidecars exceed their format-proliferation or RAM budget, measured reuse cannot amortize build cost, or list-only fallback is not legal; select `001` or `002`.
**Verification:** Sweep degree/density distributions, compare exact counts among all dispatch combinations, test container threshold boundaries and cold/warm costs, and retain a no-sidecar control receipt.
**Evidence:** GDS triangle baseline `A02-007125`; cellular/shape experiment discipline `A04-000003`, `A04-000021`; Roaring hub evidence `A06-000076`, `A06-000077`.

## FastRP

### ARCH-FASTRP-001: Compatibility Three-Plane Fit Baseline

**Algorithms:** FASTRP
**Plan classes:** fit
**Decision:** choose
**Storage layout:** Selected CSR/property columns; GDS-compatible three full f32 embedding planes where required; fixed seeded initialization, iteration weights, normalization, partition order, and bounded vector output writer.
**Working-set model:** `B_fastrp_fit = B_os + B_io + B_out + B_topology_resident + B_properties + 3*n*d*4 + t*d*b_s <= B_ram`.
**Latency/I/O tradeoff:** Highest retained RAM but the fewest topology passes and clearest compatibility baseline; dense sequential matrices can be fast when they truly fit.
**Predictability mechanism:** `n`, `d`, precision, plane count, selected features, iterations, workers, and output bytes determine the lower bound exactly; no object-per-vector representation is allowed.
**Applicability:** Reference/compatibility plan and moderate-size paid FastRP jobs where all three planes and output fit; `choose` is within the earned profile, not founder-slice priority.
**Refusal condition:** Refuse fit mode when three planes plus properties/output exceed `B_ram`, dimension/precision overflow checked arithmetic, or strict seeded floating policy cannot be reproduced.
**Verification:** Compare seeded vectors within declared tolerance or downstream distance/quality with GDS fixtures; test dimensions, selected features, zero-degree nodes, worker schedules, output backpressure, and exact byte accounting.
**Evidence:** GDS FastRP three-plane estimator/kernel `A02-006780`, `A02-006782`; model-artifact evidence `A04-000022`, `A04-000146`; embedding-family synthesis `A06-000041`.

### ARCH-FASTRP-002: Dimension-Striped External Ping-Pong

**Algorithms:** FASTRP
**Plan classes:** spill
**Decision:** choose
**Storage layout:** Embedding dimensions split into fixed stripes; current/next stripe planes mmap or direct-I/O backed; deterministic edge scans per stripe/iteration; completed output stripes sealed as an immutable embedding artifact.
**Working-set model:** `B_fastrp_spill = B_os + B_io + B_out + B_topology_window + B_properties_window + planes*n*stripe_d*b_s + t*stripe_d*b_s <= B_ram`, with `B_tmp >= planes*n*d*b_s`.
**Latency/I/O tradeoff:** Hard-bounds resident embedding dimensions but multiplies topology/property passes by `ceil(d/stripe_d)` and can be much slower than fit mode; output lower bound remains on disk.
**Predictability mechanism:** Fix stripe width, plane count, iteration/scan order, bytes per pass, prefetch depth, output/temp quota, and atomic plane/stripe publication; include cold and warm view costs.
**Applicability:** Exact declared-precision FastRP under low RAM when stripe independence preserves the pinned kernel semantics and disk/time budgets accept repeated passes.
**Refusal condition:** Refuse when the minimum one-stripe arena exceeds `B_ram`, the kernel has cross-dimension dependencies incompatible with striping, external planes exceed `B_tmp`, or pass count exceeds time/I/O policy.
**Verification:** Compare striped versus fit/GDS outputs under the declared equality relation, test stripe widths 1 and non-divisors of `d`, crash between stripes/iterations, inject disk-full, and report scans/bytes/quality drift.
**Evidence:** GDS FastRP state `A02-006780`, `A02-006782`; dimension-gated spill prior `A04-000022`; embedding stripe and resource contract `A06-000041`, `A06-000003`.

### ARCH-FASTRP-003: Hash-Regenerated Seed With Mixed-Precision Planes

**Algorithms:** FASTRP
**Plan classes:** fit, hybrid, approximate
**Decision:** experiment
**Storage layout:** Counter-based deterministic PRNG regenerates immutable initial features from `(artifact,node,dimension,seed)` instead of storing a seed plane; current/next planes use f32, f16, or quantized blocks with f32 accumulation and explicit scales.
**Working-set model:** `B_fastrp_mixed = B_os + B_io + B_out + B_topology_resident + B_properties + 2*n*d*b_quant + B_accumulator + t*d*b_s <= B_ram`.
**Latency/I/O tradeoff:** Can remove one persistent plane and halve state width, paying regeneration, conversion, and possible numeric drift; benefits depend on compute-versus-bandwidth balance.
**Predictability mechanism:** Pin PRNG algorithm/seed, quantizer/scales/block width, accumulator precision, overflow policy, quality corpus, and exact f32 fallback; receipt exposes saved bytes and added compute.
**Applicability:** Experiment for lower-RAM embeddings when byte-identical GDS coordinates are not required and downstream distance/ranking quality is the real contract.
**Refusal condition:** Refuse when compatibility requires stored GDS initialization, approximation is unauthorized, quantization quality misses threshold, overflow/saturation is unbounded, or exact fallback does not fit.
**Verification:** Bit-test PRNG regeneration, compare f32 two-plane feasibility first, evaluate distance preservation/downstream recall and coordinate error across held-out graphs, stress saturation, and ensure approximate receipts never claim exact identity.
**Evidence:** GDS FastRP baseline `A02-006780`, `A02-006782`; hard-family/model evidence `A04-000022`, `A04-000146`; quantization and oracle evidence `A06-000068`, `A06-000086`.

## Cross-Family Decisions

### Choose

1. Choose the A007 artifact-to-answer boundary, immutable view foundry, closed working-set equation, supervised hard ceiling, bounded output, and terminal receipt before adding algorithm breadth.
2. Choose paths first, WCC second, and PageRank as the first iterative proof. The remaining six families are architecture-ready profiles, not immediate roadmap commitments.
3. Choose exact fit and exact spill as separately versioned kernels. Never let ordinary allocation pressure trigger an unreceipted physical-plan change.
4. Choose algorithm-specific equality: set/distance for paths, partition isomorphism for WCC, numeric invariants for PageRank, score/top-K relations for similarity, quality/partition relations for communities, integer identities for triangles, and seeded tolerance/downstream quality for FastRP.

### Experiment

1. Experiment with compressed sets only where density/skew measurements select them.
2. Experiment with residual/delta execution only when a full-scan fallback or error bound is admitted.
3. Experiment with approximation only after an exact oracle and paid workflow establish the quality metric.
4. Experiment with new persistent views only when first-run, repeat-run, and amortized receipts prove reuse value.

### Reject

1. Reject one universal in-memory graph representation, object-heavy adjacency, and full topology duplication by default.
2. Reject unbounded frontier, all-pairs candidate, output, influence-map, wedge, or embedding materialization.
3. Reject mmap or OS page cache as an uncharged hard-RAM solution.
4. Reject Rust, `io_uring`, compression, or JVM removal as a standalone performance proof.
5. Reject WAL, MVCC, record chains, replication, routing, and live mutable catalog scope from the analytical kernel.

### Defer

1. Defer broad Bolt/Cypher/APOC/driver parity until one direct proof-carrying slice passes.
2. Defer distributed supersteps, standing incremental dataflow, GNN training, RDF permutations, and hybrid full-text/vector search as separate product regimes.
3. Defer community, kNN, and embedding implementation until a design partner provides recurring pain, a budget owner, a paid next step, and a verification artifact.

## Falsification Gates

The architecture thesis is weakened or rejected for a family when any retained exact plan repeatedly underpredicts peak memory on held-out graph shapes, cannot be enforced before ceiling breach, cannot cleanly survive forced spill failures, cannot match its declared oracle relation, or loses to a generic mmap engine after matched preparation/output/memory accounting. A low-RAM plan that requires commercially unacceptable time or temporary storage is a refusal, not a win.

The product thesis is weakened when founder evidence shows ingestion, schema governance, visualization, permissions, online mutation, or database operations dominate the buyer's pain. In that case, do not answer by quietly expanding Knight Bus into Neo4j.

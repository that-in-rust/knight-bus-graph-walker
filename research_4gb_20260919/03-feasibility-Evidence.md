# Feasibility Evidence: PRD02 And PRD05

Status: all nine assigned documents fully read; synthesis and final verification recorded below, 2026-09-19. This lane owns only this evidence file and `03-feasibility-Journal.md`. No source edits, implementation, commits, or new agents.

## Evidence Rules

- Physical budget: 4,000,000,000 bytes, not 4 GiB. Provisional worker budget: 3,000,000,000 bytes including charged file residency; OS reserve requires measurement.
- Source estimates are not measurements. Local benchmark assertions below remain source assertions unless independently inspected.
- Consumed spans mean actual bounded text reads, not search results or hashes. External facts require primary references. Proposed mechanisms are hypotheses, not global novelty claims.
- Lenses: storage lifecycle, graph semantics, resource accounting, buyer utility, adversarial falsification.

## Reading Ledger

| Source | Actual spans consumed | Status | Distinct observation |
| --- | --- | --- | --- |
| `docs_PRD02/Low-RAM-OLAP-Format-Variants.md` | 1-250, 251-500 | Full, 500/500 | Lines 83-106 explicitly document GDS CSR/compressed adjacency and disprove a pointer-everywhere baseline. Lines 293-310 budget an 8 GB machine, not 4 GB. Lines 318-345 mandate dual adjacency, but a query-specific single orientation may save more than compression; reverse-property alignment and external ID maps must be added to byte equations. Lines 429-444 ask for lifecycle measurements but provide no new 50 GB measurements. |
| `docs_PRD02/User-Journey-50GB-OLTP-OLAP-Lag.md` | 1-220, 221-440 | Full, 440/440 | Lines 21-24 assume import already finished, so freshness UX is not evidence of feasible ingestion. Lines 13-15 and 412-418 promise never blocking truth on refresh; finite disk, CPU, and WAL backlog can still require admission/backpressure. An overlay alone does not guarantee zero staleness without a common transaction watermark and complete ordered updates (124-138). The memory-bandwidth-bound inference (304-309) applies to warm fitting working sets, not arbitrary cold 50 GB random walks. |
| `docs_PRD02/V003-All-GDS-Surface-Requirements.md` | 1-350, 351-700, 701-1050, 1051-1390 | Full, 1390/1390; appendix names read, not implementation-verified | Lines 96-117 separate 562 unique names from executable support. Global atomic writeback (496-500) may overpromise the incumbent transaction contract and consume undo/WAL space; must verify procedure-specific batching. Seed equality (327-331) does not ensure identical RNG assignment, traversal ordering, or floating-point reduction across backends. Null/type/parallel-edge/orientation rules (374-440) determine physical bytes. |
| `docs_PRD02/V003-All-GDS-Surface-Supportability-Matrix.md` | 1-170, 171-324 | Full, 324/324; all rows consumed as architecture classifications, not proven support | Lines 10-24 and 307-315 are representability claims, not resource or completion evidence. KNN candidate sampling (182-188) is distinct from exact top-k. Filter pushdown cannot change the neighbor universe used in similarity denominators. Degree from offsets (122) is only constant-time for unfiltered physical adjacency; type/property filters require indexes or scans. |
| `docs_PRD02/V003-Diligence-CSR-Tiles-GDS-Surface.md` | 1-270, 271-540, 541-800, 801-1049 | Full, 1049/1049 | Lines 156-176 count 567 procedures versus 562 in requirements; exclusions/revision must be pinned. Lines 97-113 enumerate overlapping memory categories, so a corrected total must not add heap + RSS + mmap + page cache. Explicit buffered I/O alone still uses page cache (210-240). Chunked PageRank vectors (812-814) need a dependency schedule; naming spill does not bound random state I/O or iteration time. Promotion gates (995-1010) need completed resource-limited scale jobs in addition to rejection tests. |
| `docs_PRD05/Neo4j-Rust-Prior-Art-Feasibility-Risk-Assessment.md` | 1-280, 281-560, 561-840, 841-1120, 1121-1400, 1401-1680, 1681-1880, 1881-2044 | Full, 2044/2044; bibliography read, linked sources not all independently read | Lines 403-428 recognize GART as prior art; 627-726 establish external-memory lineage without promising general GDS parity. Warm-start PageRank (1002-1014) can change finite-iteration outputs and reported convergence even with the same tolerance; not automatically a transparent physical optimization. The total-memory expression (154-163) double-counts stacks/buffers if anonymous memory already includes them. The 'not found' conjunction (34-49) is not evidence of global originality; receipts (1142-1158) are audit records, not formal correctness proofs. |
| `docs_PRD05/Neo4j-Rust-Rewrite-Feasibility.md` | 1-280, 281-560, 561-840, 841-1107 | Full, 1107/1107 | Measured corpus counts (56-69) are not measured rewrite effort. LOC/day and token/calendar bands (682-794) are uncalibrated scenarios. Version mismatch 5.26.0 versus GDS dependency 5.26.26 (137-155) undermines an unpinned oracle. TCK is not full Neo4j compatibility (431-449, 971-988). Upstream tests were inventoried, not run (1096-1107). |
| `docs_PRD05/Neo4j-Rust-Two-Scenario-Estimation.md` | 1-300, 301-590, 591-880, 881-1170, 1171-1440 | Full, 1440/1440 | Lines 48-74 correctly distinguish resident-fast and strict-streaming, but latency bands lack measured calibration. Planning graph is 50 GiB and strict lane 8 GiB, unlike this 4GB task. Equation 8E+36V+16+K (502-529) excludes typed properties/edge identities. Source arrays (591-599, 978-990) are implementation-plan sizes, not universal mathematical RAM lower bounds. Break-even claim (890-902) misses finite early wins when preparation is cheaper but repeat execution slower. Capacity bands (964-976) compare a resident OLTP incumbent to deliberately nonresident challenger; useful scenario comparison, not kernel advantage. |
| `docs_PRD05/README.md` | 1-41 | Full, 41/41 | Lines 34-41 define compatibility, oracle, fixtures, resource gates, and traceability as the verification spine. The listed Sol-01/Sol-02 links do not expand this lane's frozen nine-document assignment. |

## Provisional Audit

1. Fair analytical comparison is Rust execution versus GDS projected graph plus equivalent algorithm semantics, with database/extraction/projection charges separately reported. Cypher path queries remain a different baseline.
2. Durable mmap is a lifetime/storage choice, not a RAM bound or automatic speedup. Process RSS and file cache overlap must not be summed as disjoint bytes.
3. The source already contains base topology + columnar properties + scratch + optional hot artifacts. Repeating that design is not a new research direction.
4. A lifecycle model must include retained old generations, build scratch, external-ID mapping, orientation-specific edge-property indexing, result size, WAL/delta backlog, crash recovery, and concurrent jobs.

## Coverage Checkpoint

Coverage: 9/9 documents complete; 8,335/8,335 assigned lines consumed. No unread assigned spans. Tables were read in the listed bounded chunks, with architecture classifications distinguished from implementation evidence. External verification is targeted, not an exhaustive reading of every cited paper or every GDS implementation.

## Primary Verification Checkpoint

Checked 2026-09-19. These are targeted reads, not full external-paper coverage.

- Local GDS checkout is `dc4417b3c1feffbdf9eb6293fbbd1ffb1b7233b9`. Read `gitrefrepo/Neo4j family/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java` lines 1-155: primitive byte pages, degree and offset arrays (101-110), delta-based estimates (58-86), and decompression cursors (119-143). This directly falsifies the pointer-object-everywhere premise.
- Read `.../core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java` lines 1-120 and 117-190. Projection estimates include node ID maps and properties (103-115), and the maximum of loading/loaded relationship structures (117-130), plus adjacency loading buffers (174-190). Lifecycle estimation is not absent from GDS.
- [GDS memory estimation](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/) documents automatic heap checks but explicitly does not guarantee completion after admission. It also says legacy Cypher estimates execute the node and relationship queries to obtain counts. Correct PRD02 Diligence lines 435-442: no algorithm execution is a reasonable estimate contract; metadata-only projection estimation is not universally incumbent-compatible.
- [GDS system requirements](https://neo4j.com/docs/graph-data-science/current/installation/System-requirements/) distinguish on-heap projection/state, native Arrow staging, and database page cache; analytical-only deployments may reduce page cache. CE concurrency is capped at four; the cap follows the GDS license, not the Neo4j database edition. A 64-core laboratory under an 8GiB cgroup is not evidence for a typical 4GB machine.
- [Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html) defines hierarchical current/peak accounting and overlapping memory.stat detail counters. Use measured totals, not RSS plus every subcounter. A cap is enforcement, not a completion guarantee.
- [GDS BFS](https://neo4j.com/docs/graph-data-science/current/algorithms/bfs/) returns visited node IDs and a traversal path value. Neither a reached set nor a parent tree alone reproduces every observable GDS return value; general Cypher relationship-unique path enumeration is another contract entirely.
- [GDS PageRank](https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/) documents finite maximum iterations and a per-score-change stopping rule. A warm start, altered damping, or different reduction schedule needs semantic validation, not only close final scores.
- [GDS KNN](https://neo4j.com/docs/graph-data-science/current/algorithms/knn/) samples neighbors and exposes accuracy/runtime controls; deterministic examples require seed and concurrency one. Exact nearest-neighbor search and exact reproduction of this sampled procedure are different promises.
- [GDS Node Similarity](https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/) separates per-node topK/bottomK from global topN/bottomN, and produces directed result relationships. A small result limit does not by itself eliminate candidate computation.

Novelty checks found direct antecedents for possible new mechanisms: [incremental property-graph view maintenance](https://arxiv.org/abs/1806.07344), [PageRank equitable partitions and quotient computation](https://scholar.afit.edu/etd/2632/), and [exact, disk-resident AllPairs similarity search](https://www.bayardo.org/ps/www2007.pdf). Accordingly the directions below must concern new scoped combinations and measurable engineering hypotheses, not claim these algorithms were invented here.

## Decision Summary

The defensible opportunity is a small number of semantically explicit, disk-backed graph products, not a claim that Rust makes arbitrary Neo4j/GDS workloads fit in 4GB. Avoiding unnecessary preparation, reducing algorithm state when structure permits, and avoiding provably irrelevant candidate work are separate research levers. Three hypotheses below exercise those levers without duplicating the lead's answer-inheritance, identical-source-row factoring, or identical-neighborhood-class mechanisms.

The old scenario described a large GDS projection in RAM. The revised scenario caps the replacement's **prepared persisted artifacts at 50,000,000,000 bytes**. Neither a 50GB projected graph nor a 50GB source file establishes the replacement's vertex/edge count, index size, build feasibility, or total disk peak. Every comparison must record all three sizes separately.

Snapshot-first is a product/freshness choice, not a theorem that live graph algorithms cannot work. [GART](https://www.usenix.org/conference/atc23/presentation/shen) is direct prior art for dynamic graph analytics with mutable CSR and versioning. Its in-memory design does not establish this 4GB envelope. The relevant choice is the consistency/freshness contract and its update-maintenance cost, not a blanket live-versus-snapshot verdict.

## Baseline Corrections

### Pinned GDS Implementation

Additional source consumed at commit `dc4417b3c1feffbdf9eb6293fbbd1ffb1b7233b9`, all under `gitrefrepo/Neo4j family/neo4j-gds-src/`:

| File | Actual consumed lines | Consequence |
| --- | --- | --- |
| `algo/src/main/java/org/neo4j/gds/pagerank/PageRankComputation.java` | 1-115 | Initial rank is `1-damping` for all nodes or selected sources (60-75). Later steps accumulate damped incoming deltas; propagation depends on per-node tolerance and initial-step status (78-99). The implementation multiplies messages by relationship weights (106-109). A generic normalized textbook PageRank recurrence is not automatically this contract. |
| `algo/src/main/java/org/neo4j/gds/pagerank/PageRankMemoryEstimateDefinition.java` | 1-38 | Delegates a DOUBLE rank property to the non-queue Pregel estimate. |
| `pregel/src/main/java/org/neo4j/gds/beta/pregel/Pregel.java` | 109-145 | Includes vote bits, per-thread compute state, node values, and message representation. |
| `pregel/src/main/java/org/neo4j/gds/beta/pregel/ReducingMessenger.java` | 60-88 | Two huge double message arrays, not an object for every graph edge. |
| `pregel/src/main/java/org/neo4j/gds/beta/pregel/NodeValue.java` | 75-100 | DOUBLE properties use a huge double array. |
| `algo/src/main/java/org/neo4j/gds/pagerank/DegreeFunctions.java` | 1-93 | PageRank obtains a degree function from degree centrality with weight awareness. Its construction and retention must be charged, not assumed free from the three-array subtotal. |

Thus `24V` bytes is a useful subtotal for one rank array and two message arrays, not the full GDS PageRank allocation and not a lower bound on all possible algorithms. Exact weighted-degree validation, negative/default-weight handling, scheduling, convergence counters, and writeback need further source/oracle checks before implementation compatibility can be claimed.

The historical [Neo4j GDS configuration guide](https://go.neo4j.com/rs/710-RRC-335/images/Neo4j-Graph-Data-Science-Configuration-Guide-EN-A4.pdf), PDF pages 7-9, reports GDS 1.5/Neo4j 4.0.6 Enterprise on a 512GB, 64-logical-core machine. LDBC100 has about 317M nodes/2.15B relationships; its projection is listed as 7.7GB, while PageRank is listed at 110GB and 8.28 minutes. Algorithm times exclude stream/write. These historical vendor observations demonstrate the projection/state distinction; they are neither a modern 4GB comparison nor a mathematical per-algorithm minimum. The table does not establish whether every memory entry is incremental or total.

The [current operations manual](https://neo4j.com/docs/operations-manual/current/performance/disks-ram-and-other-tips/) labels io_uring support as Enterprise, introduced in 2026.04, opt-in, initially used for background page-cache writes. Do not backport a general async-read advantage/disadvantage to the 5.26 reference or all editions.

### Fair Compatibility And Comparison

| Contract | Required comparison | Invalid shortcut |
| --- | --- | --- |
| Bounded-hop reached nodes | Same directed/undirected view, source set, depth, labels, property predicates, treatment of missing nodes | Compare it with enumerating every relationship-unique Cypher path |
| GDS BFS | Visited IDs plus traversal path/order observables actually required by caller | Return a set and call the whole procedure compatible |
| Weighted shortest path | Same weight domain, defaults, tie policy, parallel edges, path reconstruction | Replace weighted distance with hop count or skip negative-weight checks |
| PageRank | Same pinned initialization, weighting, per-step propagation/halting, maximum iterations, result tolerance, modes | Warm start or use f32 and report the same finite-iteration algorithm |
| Node similarity | Same neighbor-set definition, degree filters, candidate filters, cutoff, directed topK, global limit and tie behavior | Filter away neighbors in the denominator, replace exact comparison by sampling, or silently symmetrize output |
| KNN / embeddings | Same sampling/seed/concurrency or explicitly a different approximation contract; output dimension included | Exact nearest neighbors mislabeled as reproduction of sampled KNN; embeddings treated as a small scalar array |
| Bolt/Cypher/GDS compatibility | Pin server/procedure versions, protocol modes, errors, value types, transaction behavior, and supported subset | Count 562/567 registered names or passing TCK as full product parity |

Use two baseline tracks: equal 4GB resources for completion/admission evidence, and a disclosed larger-memory incumbent for an answer oracle and performance context. An incumbent refusal/OOM is a capacity outcome, not an infinite speedup. Charge extraction, projection, execution, serialization, export, and refresh to both systems. Tune the incumbent for analytical use rather than reserving an unnecessarily huge OLTP page cache. Keep CE and licensed Enterprise comparisons separate. A remote incumbent serving extraction remains a resource dependency, not a free component of a standalone 4GB replacement.

## Parameterized Feasibility Model

### Definitions And Units

All GB below are decimal. Physical `P = 4e9` bytes is about 3.725 GiB. Provisional OS/unrelated-process reserve `O = 1e9`; worker cap `W = P-O = 3e9`, about 2.794 GiB. Prepared-disk cap `D_limit = 50e9`, about 46.566 GiB. These are accounting targets, not measured safe settings.

| Symbol | Meaning |
| --- | --- |
| `V, E` | Vertices and stored adjacency entries in the semantic view, after filters, deduplication/aggregation, and orientation decisions; report logical relationships separately |
| `q` | Independently materialized orientations; normally 1 or 2, not automatically 2 |
| `i, o` | Neighbor-ID and offset widths; illustrative values 4 and 8 bytes |
| `K, N_p, E_p` | External-key bytes, node-property bytes, relationship-property bytes including offsets, validity and schema |
| `A, I, H` | Prepared topology/properties/ID maps; auxiliary indexes/certificates; retained version count or explicit shared extents |
| `L_j, S_j, f_j` | Bytes sorted by build stage j, its run-generation workspace, merge fan-in |
| `Q, R, r` | Concurrent query count, exported record count, actual encoded bytes per record |
| `lambda, mu` | Change-log arrival and sustainable application rates in comparable bytes/sec or work units/sec |
| `Delta, tau` | Retained change bytes and refresh/publication interval |

Use 32-bit neighbor IDs only when the dense-ID space and sentinel policy fit. A billion adjacency entries may fit 32-bit entry offsets, but byte offsets or larger graphs can require 64 bits. Relationship IDs and external IDs are separate domains. String keys, typed lists, nulls, temporal values, edge identities and type tables must not disappear inside a topology-only estimate.

### RAM: Phase Peak, Not File Length

At time t, plan disjoint allocations and verify a measured aggregate:

`M_worker(t) = runtime + metadata/ID-cache + explicit-I/O/codec-buffers + algorithm-state + output-buffer + charged-file-residency + charged-kernel + unassigned-guard`.

Require `max_t M_worker(t) <= W` and total-machine use `<= P`. Include child processes. Do not add process RSS to page-cache totals, or add mapped-file bytes again to the file category. A 50GB mapping is not 50GB resident, but page tables, faults, dirty pages, codec buffers and cache occupancy still count. Buffered read/write can duplicate data between user buffers and file cache. Direct I/O has alignment/buffering costs and is not a universal remedy.

An illustrative single-query allocation, in millions of bytes, sums to 3,000: runtime 160; metadata/ID cache 80; I/O and codec 128; output 64; charged kernel 96; fragmentation/unassigned guard 272; algorithm state 1,400; charged file residency 800. These are tunable reservations, not empirical measurements. Large node maps or thread pools can invalidate them immediately. Build may use a different split, but a query and build running together must share one cap, not each claim 3GB.

In a Linux experiment use a parent cgroup with swap disabled, monitor `memory.current`, `memory.peak`, memory events and pressure, and separately observe host use. The OS reserve is tested, not subtracted and forgotten. The current research machine is not evidence of the target device's SSD, core count, or OS overhead. A container limit on a large host is useful but does not by itself reproduce all 4GB-machine behavior. Bounded memory plus finite input does not promise acceptable completion time, sufficient disk, or convergence.

### Prepared Bytes And State Bytes

Uncompressed topology for q orientations is `A_top = q * (iE + o(V+1))`. This assumes E entries per orientation; union/undirected projections must count the actual duplicated entries and self-loop policy. Compression replaces each term with measured encoded bytes plus block directories/restart tables; no fixed compression ratio is assumed.

For one scalar edge weight of width w, copying it into every orientation costs `qwE`. One canonical column plus reverse permutations costs `wE + (q-1)pE` for permutation width p, before edge identity and null bits. This can be larger than copying a small weight. Node scalar columns cost `wV` plus validity/metadata; variable-size properties add offsets and payload. A reversible external-ID dictionary is required for real outputs even if algorithms use dense IDs.

The inspected prototype's formula is `8E + 36V + 16 + K`, before general properties. With average 16-byte keys, `K=16V`. The following calculations are arithmetic, not measured file sizes; totals shown in GB omit only the 8/16-byte terminal offsets for readability.

| V / E | Single CSR | Dual CSR | Prototype plus 16-byte keys | Three f64 state arrays |
| --- | ---: | ---: | ---: | ---: |
| 10M / 100M | 0.48 | 0.96 | 1.32 | 0.24 |
| 50M / 500M | 2.40 | 4.80 | 6.60 | 1.20 |
| 200M / 1B | 5.60 | 11.20 | 18.40 | 4.80 |

At 200M vertices: a u32 vector is 0.8GB; f64 is 1.6GB; one visited bitset is 25MB. A simple BFS design with a u32 parent array and two full-size u32 frontier buffers consumes 2.425GB before topology, ID translation, runtime or filtering. That design fails the illustrative 1.4GB state reservation. External frontiers or fewer arrays are different schedules to analyze, not free savings.

Other state/output constraints:

- WCC u32 union-find parents alone cost 0.8GB at 200M; ranks, labels, relabel passes and concurrent state add bytes. External connectivity algorithms exist, so failure of one resident design is not impossibility.
- Dijkstra requires distance/predecessor state and a queue. A lazy heap can contain many stale entries, not merely one per vertex. A decrease-key heap adds position metadata. The queue must have its own bound or external-memory plan.
- At 200M nodes, topK=10 can export 2B directed records. Packed `(u32 source, u32 target, f64 score)` is 32GB; external IDs/CSV can be larger. Even just `(target,score)` state for all sources is at least 24GB packed, often 32GB aligned. Streaming output does not remove computation or sink cost.
- A single 200M-by-128 f32 embedding matrix is 102.4GB. It exceeds the entire prepared-disk cap before topology or training state. Smaller graphs/dimensions, external export, or a different output contract must be explicit.

### Import And External Build

The constrained workflow starts at raw acquisition, not at an already imported Neo4j database. Record source size `S_source`, schema discovery/validation, decompression expansion, edge/node counts, bad-record handling, duplicate semantics and input consistency watermark. A malformed late record can force rollback/restart; charge rejected bytes and cleanup.

Do not construct a giant external-key hash map and call only the later CSR builder bounded. A conservative alternative uses externally sorted keys, deduplication under the contract, endpoint merge joins, and sorted adjacency streams. Node ordering, source/destination joins, each orientation, properties and uniqueness validation can require separate passes. A streaming source is permissible only when replay/retention and remote-resource dependencies are disclosed.

For a materialized sort stage of L bytes, run workspace S and merge fan-in f: initial runs `n=ceil(L/S)`; merge levels `p=ceil(log_f(n))` for n>1, otherwise zero. A conventional schedule reads and writes roughly `2L(1+p)` bytes across run generation and merges. Final-merge fusion or streaming changes that schedule; count the actual eliminated pass, not all passes. `f * per-input-buffer + output-buffer + merge-heap + codec/runtime` must fit that phase's allocation. Record expansion and variable-length keys can make L much larger than the final compressed artifact.

Before reclamation, two generations of sort runs can approach `2L` working bytes. This is not automatically additive with every other stage's peak, nor automatically hidden inside the final graph. Draw a timeline of overlapping files and release a run only after durable consumers no longer need it. Recovery metadata, checksums, manifests, fsync and replay validation belong in build time.

### Disk: Finished Product Versus Working Peak

Mandatory revised check: `D_prepared = live prepared extents + retained-version extents + dictionaries/properties + indexes + persisted results/certificates <= 50e9`. Shared immutable extents count once; logically distinct copies count separately. Report single-current-generation size as an additional number, never as a substitute for all retained bytes.

Separately require actual available local storage to cover:

`D_peak = max_t(source-staging + durable-truth + prepared-extents + run-files/build-IR + checkpoints + export-spool + WAL/delta-backlog + orphan-recovery-reserve)`.

The user specified a prepared-artifact limit, not an unlimited scratch device. Every candidate must report `D_peak` and the required scratch headroom. A stricter prototype profile should additionally test all local working files within a 50GB arena. Source data stored elsewhere must be labeled elsewhere, with input transfer/replay time charged; do not silently move scratch to a larger machine. A literal 50GB local source file on an otherwise full 50GB device leaves no room for the replacement.

Concrete failure: the 200M/1B prototype with 16-byte keys is 18,400,000,016 bytes. Old plus new full artifacts are 36,800,000,032 bytes and fit the prepared cap without properties/results. If one concurrent edge-sort stage needs two 16GB runs, the working peak is already 68,800,000,032 bytes, before source, properties, WAL or export. Therefore a fitting finished artifact does not establish a feasible refresh on a 50GB working device. Conversely, bucket-at-a-time replacement with shared unchanged extents can reduce this peak; its slower pass schedule and durable reclamation must be demonstrated.

A 32GB similarity export retained beside that 18.4GB artifact already exceeds 50GB. External streaming can remove local result retention, but then network throughput, resumability, receiver capacity and failure behavior remain product requirements.

### First Answer, Repeats And Refresh

Distinguish time to first emitted row from time to the first **complete correct answer**. Ranking, exact top-k and convergence-based results may not have a useful final row until global work ends.

`T_first_complete = acquisition + validation + required-ID/property work + mandatory preparation + recovery-safe publication + cold query + full export` along the critical path. Concurrent phases use their actual overlap and resource contention, not a sum of ideal independent runtimes. Report separately the cost from raw input, from an already available durable source, and from a warm prepared artifact.

For each phase, useful lower bounds are bytes read/write divided by measured service bandwidth, random I/O count divided by measured IOPS, work divided by measured compute throughput, and encoded results divided by sink throughput. Shared storage does not get independent full read and write bandwidth at once. These are optimistic lower bounds, not latency predictions. Degree skew, decompression, cache misses, queue depth, thermal behavior and errors can dominate.

Example: twenty complete reads of the uncompressed single-orientation 200M/1B CSR total 112,000,000,160 bytes. This schedule-specific count excludes state, weights, reverse lookup and export. It is not a universal PageRank I/O floor; offset caching/compression/active-edge skipping can reduce it, while spilling and repeated gathers can increase it. Destination blocking that rescans a source-state vector per block may read `number_of_blocks * state_bytes` per iteration. A schedule must specify where old state lives, when next state becomes visible, and how messages are reduced without changing finite-step semantics.

For N equivalent repeated jobs, compare `DeltaP + sum(DeltaRefresh) + N*DeltaR`, including export in either R or an explicit separate term. Delta means challenger minus baseline. If DeltaR<0, enough repeats can amortize a larger preparation. If DeltaR>0 but DeltaP<0, the challenger can win for a finite early interval and lose later. Neither 'always faster after preparation' nor 'no break-even when repeats are slower' is generally correct.

Refresh must account for `lambda*T_build` incoming changes during a rebuild, recovery/replay amplification, old readers, new writes and result export overlapping. Sustainable maintenance needs mu>lambda over the relevant workload; average stability does not bound bursts. With reader lifetime L and publication every tau, retained generations can approach `ceil(L/tau)+1`, not necessarily two. Backpressure, admission, expiring reader leases, or added capacity may be needed; 'never block writers' is not unconditional on finite storage.

A consistency contract names the source transaction cutoff, transaction ordering, completeness watermark, deletion/property changes, and atomic visibility of multi-edge updates. Live algorithms can incrementally maintain results at such cutoffs. A snapshot system instead exposes lag and schedules rebuilds. Neither an overlay nor incremental maintenance removes the need to bound update work. Measure query p95/p99 during refresh, refresh lag, backlog growth and export competition, not just isolated query time. Full atomic writeback versus batched writeback is a separate observable and resource contract.

## Three Distinct Research Directions

These are scoped architecture hypotheses derived from the audit, not claims of globally new algorithms. None has been implemented or benchmarked in this lane. Their distinction from the lead's three mechanisms is deliberate: change preparation granularity, reduce state dimension, or bound candidate materialization, rather than inherit old answers, factor identical source rows, or score identical neighborhood classes.

### H1: Sealed-Bucket Progressive Adjacency

**Useful product:** dependency/blast-radius exploration over imported directed graphs, where users need a correct first reached-set answer before a complete dual-CSR build. Initial contract: stable numeric external IDs, set-valued edges, bounded-hop reached sets, explicit cutoff. Multigraph relationship paths, arbitrary Cypher, and general property types are not included by implication.

**Mechanism:** partition incoming edges by a stable hash of the full source ID into disk buckets with bounded buffers. Keep exact IDs, not hash-only identity. A receipt records complete acquisition through cutoff t, node/property streams included where needed. A bucket may remain unsorted: querying it scans all its relevant records. Hot buckets are gradually sorted/indexed under an admitted maintenance budget; cold buckets stay scan-readable. Frontier processing groups requested sources by bucket and scans each required bucket once per level, with external visited/frontier state if necessary. A bucket is safe to query only when its source coverage is complete at t; merely having seen a source once is insufficient. Unless upstream supplies trustworthy per-partition completion, the first exact answer still waits for a complete input pass.

This defers optional indexing, not correctness work. [Database cracking](https://stratos.seas.harvard.edu/publications/database-cracking), [its update handling](https://stratos.seas.harvard.edu/publications/updating-cracked-database), and [progressive indexing](https://ir.cwi.nl/pub/29163) are direct antecedents. The research question is whether graph frontier batching plus completion receipts and bucket-local durable refinement improve the **raw-input-to-answer and refresh-overlap envelope** on this small machine. Do not call adaptive indexing itself new.

**Complete lifecycle:**

| Stage | Required accounting and contract |
| --- | --- |
| Import/build | One complete parse/partition pass, bounded open-bucket buffers, decompression and input validation; repeated partitions if fan-out exceeds RAM/file-handle limits. No free global dense-ID map. Node existence/property validation may require additional external joins before publication. |
| Prepared disk | For the narrow numeric set-edge contract, raw endpoint payload alone is `16E`; add node records, properties, bucket directories, framing and version/deletion metadata. At 1B edges this is 16GB, not a compressed CSR claim. Relationship identities for multigraph deletion would add another domain/column. Retained indexed/raw copies count toward 50GB until reclaimed. |
| Working disk | Active buckets plus uncommitted replacement buckets, local sort runs, WAL, frontiers/visited spill, input staging and export. Replacing one bucket avoids mandatory duplication of the whole graph, but the largest/skewed bucket determines scratch; recursively repartition or reject before exhausting space. |
| First/query answer | `input_complete + mandatory_validation + touched_bucket_reads + visited/dedup work + export`; no full-sort cost unless demanded by the actual query. Sparse roots may benefit; a frontier covering every bucket devolves to full scans each level. Correctness requires reading unindexed portions too. |
| Export | Stream exact external IDs with bounded buffers and backpressure. Reached-set ordering is unspecified or explicitly sorted at extra cost. A GDS traversal path is not claimed. Restartable export pins a version and prolongs retention. |
| Refresh/overlap | Append transaction-tagged bucket deltas; queries merge base and complete deltas at a common cutoff. Bucket-local compaction publishes atomically and keeps old extents until readers finish. Deletes, duplicate events and failed/replayed transactions need idempotent semantics. Query, compaction and ingestion share I/O and the 3GB worker cap. |

**Counterexamples:** adversarial IDs concentrate a bucket; very long keys make the numeric model inapplicable; a high-degree source requires a huge scan/output; every frontier touches every bucket; global node-property filters force expensive joins; a late rejected input invalidates publication. Query-driven refinement can make an early answer slower than a plain scan and later answers slower than a one-time bulk sort.

**Verification-first experiment:** compare plain edge scans, a complete single-CSR external build, and this progressive design from the same raw source. Use clustered, uniform and adversarial root sequences, skewed sources, missing node records, hash collisions, duplicate deliveries, deletions, and a transaction straddling multiple buckets. Oracle-check every reached set. Record first complete export, total time/bytes over N queries, refinement bytes, peak disk, memory and refresh lag. Kill the hypothesis for the intended workload if deferred indexing only moves more work onto queries without a first-answer or lifecycle advantage. Do not claim success merely because the unsorted store answers something.

### H2: Contract-Certified State Quotients

**Useful product:** repeated ranking over graphs with strongly repeated structure, such as generated/template dependency graphs. The optimization target is the number of rank/message state entries, not merely repeated edge scans. This differs from identical-source-row factoring, which can retain all V rank entries.

**Mechanism and restricted correctness argument:** let `P_uv` be the pinned procedure's effective normalized contribution from u to v, and partition vertices into cells. For every pair of cells B,C require that `sum(u in B) P_uv = a_BC` is the same for every v in C. Require equal initialization and relevant per-node parameters within each cell. In exact arithmetic, if cell members currently have equal rank and delta, their next incoming deltas are equal; equal tolerance decisions preserve that equality. A quotient can propagate one delta per cell and retain one accumulated rank per cell. Initialization, personalized sources and changed properties may force splits. The initial-superstep exception and halt/reactivation rules must be reproduced, not replaced by a generic convergence test.

[Equitable partitions and PageRank quotient computation](https://scholar.afit.edu/etd/2632/) are established prior art. Proposed differentiation is a resource-aware, reusable certificate for the **specific finite-step/delta-propagation contract**, including construction, refresh splitting and result expansion. This is not a claim to invent lumping or compressed matrix multiplication. Equality in real arithmetic does not ensure bit-identical floating reduction. An oracle-approved numerical tolerance is a separate contract; bit-for-bit compatibility can disqualify the optimization.

**Complete lifecycle:**

| Stage | Required accounting and contract |
| --- | --- |
| Import/build | Constrained endpoint mapping and normalized-weight validation precede external partition refinement. Group signatures propose cells; exact structural/weight checks establish them, not hashes alone. Every refinement round sorts/joins membership with edges; cap rounds/work or fall back to singleton cells. Full validation may cost more than several ordinary rank runs. |
| Prepared disk | Membership map, quotient adjacency and coefficients, original-ID map, certificate/version metadata and any retained base graph. Three f64 vectors use `24C` bytes for C cells before flags/degrees/runtime. At C=20M instead of V=200M that subtotal is 480MB rather than 4.8GB; this is conditional arithmetic, not an observed compression ratio. A u32 V-to-cell map is another 800MB on disk. |
| Working disk | Refinement signatures, sort runs, old/new memberships and quotient coefficients, checkpoint vectors and export. Count the base graph if retained for future split/validation; omitting it requires a charged replay source. Check 50GB prepared and the actual peak independently. |
| First/query answer | First answer includes partition construction and certification; repeats may use C-state propagation and quotient-edge scans. Dense quotient edges can erase the benefit. Any convergence aggregate must account for multiplicity: cell max-norm and vertex max-norm agree, while L1 sums require cell sizes. |
| Export | Expand scores through membership to V external-ID rows. Output remains O(V), even if propagation is tiny. Per-node properties, ties and score normalization must match the declared contract. Export state is not C rows unless the user explicitly requests cell-level output. |
| Refresh/overlap | A single weight/seed change can split a cell and propagate refinement. No bound proportional only to changed edge count is promised. Revalidate or rebuild before using a quotient for the new cutoff, while old readers retain old maps; charge overlap and accumulated changes. A fully live version would need a maintained invariant and update-latency evidence, not just a snapshot certificate. |

**Counterexamples:** a random asymmetric graph yields C=V; personalized sources split every useful cell; nearly equal but unequal weights cannot be merged as exact; quotient edge density or membership joins dominate; a tiny update causes global refinement. Floating-point reassociation can change tolerance crossings, iterations and scores even when final differences are small. Keeping only approximate cells would be a different algorithm and must not be smuggled into this hypothesis.

**Verification-first experiment:** build exhaustive small symmetric/asymmetric directed fixtures, dangling nodes, self-loops, parallel-edge aggregation cases, weighted splits and personalized starts. Compare every iteration's rank, message and activity state, not only final ordering, at 1/2/20 steps and around tolerance boundaries. Force signature collisions. Compare against both pinned GDS and an exact-arithmetic small oracle under explicitly different equivalence checks. Then measure construction/revalidation plus N runs/export. Kill if the accepted numerical contract cannot hold, useful C reduction is absent, or build/refresh never amortizes within the customer's expected N. No such experiments were run in this lane.

### H3: Hub-Bounded Exact Similarity

**Useful product:** exact binary-neighbor Jaccard candidate lists for entity/fraud investigation, where high-degree shared features create enormous candidate tapes. This is not sampled KNN, and does not assume that entire node neighborhoods are identical.

**Mechanism:** build neighbor sets and inverted feature postings externally. Process candidate pair tiles or bounded source blocks. Light postings may enumerate pairs; heavy postings must not materialize every wedge into a global intermediate. Instead refine block/pair overlap bounds, reading heavy postings only where unresolved candidates can still qualify. With exact counted overlap c and remaining unprocessed set sizes rA,rB, an upper intersection bound is `u=min(a,b,c+min(rA,rB))`; Jaccard is at most `u/(a+b-u)` for nonempty union. Prune only below the cutoff or the relevant source's current kth score. Equality needs the actual tie rule. Exact integer cross-products can avoid an unsafe floating-bound comparison.

The unseen candidate universe also needs coverage: pairs sharing only heavy features cannot be dropped because no light feature generated them. A coarse upper bound must justify skipping an entire pair block; otherwise enumerate it. Exhaustive blocked intersections are the correctness fallback. [AllPairs](https://www.bayardo.org/ps/www2007.pdf), particularly its exact filtering and disk-resident extension, is prior art; this hypothesis concerns a GDS-shaped directed-result contract and a hard intermediate-space budget, not the invention of exact similarity search or pruning.

**Complete lifecycle:**

| Stage | Required accounting and contract |
| --- | --- |
| Import/build | External sorting/deduplication constructs the contract's binary neighbor sets, degrees and inverted postings. Separate candidate filters from the neighbor universe. Count both orientations if both are materialized; a transpose is a real sort/write stage. |
| Prepared disk | Neighbor sets plus posting lists, degree arrays, block summaries and external-ID map; compressed postings must be measured, with a raw upper case. No promise that 'bitmap' means small. Persisted topK outputs and old index versions count toward 50GB. |
| Working disk | Sort runs, bounded pair tiles, per-block heaps, checkpoints and export spool. For b active sources, topK state is approximately `b*k*entry_bytes`, not `V*k`. If a b-by-b counter tile is used, charge its quadratic size explicitly. Never hide a global wedge/pair file outside the budget. |
| First/query answer | Include posting construction and all verification needed to finalize each source's topK. Global topN cannot be finalized by returning early local hits. Worst-case pair work remains quadratic; `sum(feature f) choose(degree(f),2)` explains why a shared hub is dangerous but is not a required implementation intermediate. |
| Export | Directed per-source topK, self exclusion, cutoff and tie semantics first; global limits applied in the specified order. Chunked output may be huge despite bounded RAM. Use the 32GB example above as a result-budget counterexample, not an expected dataset size. |
| Refresh/overlap | Changing one feature posting affects pairs involving its members, potentially quadratic in posting degree. Rebuild affected blocks only with sound dependencies; otherwise recompute. Retain old postings/results for pinned readers, count changed-degree summaries and log backlog, and share RAM/I/O with query/export. Live maintenance is possible in principle but not cheap by assertion. |

**Counterexamples:** all vertices share a hub and scores lie near the cutoff; all bounds stay loose; zero cutoff admits disjoint candidates; huge tied scores prevent aggressive pruning; weighted/signed vectors invalidate these binary-set bounds. Small topK limits stored output but do not prove small search work. A nonlocal topK change can invalidate previously emitted rows.

**Verification-first experiment:** enumerate small set collections, compare exhaustive rational Jaccard and the pinned procedure under an explicit tie policy, then adversarially test hub-only overlap, disjoint sets, identical/tied scores, empty sets, candidate-only filters and topK followed by topN. Validate every prune against exhaustive candidates. Measure raw import, transpose/index build, candidate verifications, peak intermediate disk, complete export and a high-degree feature update during queries. Kill if safe bounds rarely prune and added index/build cost exceeds the plain blocked exact baseline at intended cadence. Do not relabel a timeout as successful bounded-memory completion.

## Experiment Acceptance And Handoff

The research output is complete; empirical feasibility is not established. Proposed experiments have not been executed. The lead reported its own restricted exhaustive checks for three different mechanisms; those results are not independent verification of H1-H3 here.

| Requirement | WHEN / THEN / SHALL contract for a future experiment |
| --- | --- |
| FEAS-01 Semantic oracle | WHEN a candidate runs on a declared fixture/configuration, THEN all required outputs and errors SHALL match the pinned contract; separately state exact, tolerance-based, order-insensitive or approximate comparisons. |
| FEAS-02 Full import | WHEN started from the declared raw source, THEN acquisition, ID mapping, validation, every sort and artifact publication SHALL occur inside the measured resource boundary; no prebuilt artifacts from a larger machine. |
| FEAS-03 Memory | WHEN import, query, export and refresh are exercised alone and in supported overlap, THEN parent-cgroup and whole-machine peaks SHALL be reported, with no swap-dependent success. Admission/rejection SHALL be distinguished from completion. |
| FEAS-04 Disk | WHEN running steady state and refresh, THEN all prepared extents SHALL remain at or below 50e9 bytes; scratch/source/output/backlog peak SHALL also be reported and fit the declared local disk. Test the stricter 50GB working-arena profile separately. |
| FEAS-05 First answer | WHEN the clock starts at raw acquisition, THEN time to first row and first complete correct export SHALL both be recorded, together with bytes and time per stage. |
| FEAS-06 Refresh | WHEN representative and adversarial updates arrive during queries, THEN lag, backlog, query tail latency, retained versions and maintenance throughput SHALL be measured; no publication of incomplete cutoffs. |
| FEAS-07 Recovery | WHEN interrupted during run generation, manifest publication, index refinement or export, THEN restart SHALL preserve the advertised cutoff and reclaim/count orphan files without exceeding the envelope. |
| FEAS-08 Product value | WHEN compared with a tuned baseline over the customer's actual query/update cadence, THEN report the full preparation/repeat/export/refresh trade-off and cost of failures; claim no speedup until repeated controlled measurements support it. |

Required benchmark record: version/commit/license, hardware/OS/SSD and filesystem, source and projected counts/bytes, semantic parameters, final/peak prepared and scratch bytes, peak memory and swap, input/output byte counts, first-answer and full-export time, cold/warm condition, refresh arrival schedule, successful/failed/rejected/timed-out status, and correctness evidence. Report multiple trials and variation; do not present source planning ranges or historical vendor numbers as measurements of this system.

Priority for experiments: H1 tests whether preparation can be usefully deferred; H2 tests whether state can shrink enough to change the capacity class; H3 tests whether useful exact results can avoid explosive intermediates. Each must beat its simplest semantically equivalent disk-backed baseline on a relevant lifecycle metric without hiding losses elsewhere. None is a full Neo4j replacement by itself.

## Final Verification Record

- Assigned reading complete: 9/9 documents, 8,335/8,335 lines. Exact chunks and unique observations are preserved above. No next unread assigned file/span.
- Supporting local GDS source reads and targeted primary-source checks are explicitly scoped. External bibliography and complete upstream source/test suites were not exhaustively read or executed.
- Arithmetic was checked with direct integer calculations: CSR/state sizes, 3e9 reservation sum, 18.4GB artifact, 68.8GB example build peak, 32GB topK export, 102.4GB embedding, and 480MB quotient-state example. These are formula checks, not benchmarks.
- Open implementation questions remain: exact weighted GDS preprocessing/halting/writeback parity, useful real-world partition/pruning distributions, target-device performance, safe OS reserve and crash-consistent constrained builds. They are experiment work, not unread assigned prose.
- User's subsequent PRD04 transfer is a separate assignment with separate artifacts and denominator; it does not alter this completed nine-document ledger.

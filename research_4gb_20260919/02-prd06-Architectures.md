# PRD06: Four Bounded-State, Low-I/O Hypotheses

Research deliverable, 2026-09-19 UTC. No implementation, performance experiment or customer validation was performed. These are proposed combinations, not claims of global originality. Primary-source checks and the subsequent four-source PRD04 transfer narrow the novelty claims; they are not an exhaustive literature review.

Transfer addendum: all4 PRD04 architecture sources (6,609 lines) were subsequently read. Their stronger existing mechanisms include triangle orientation/hub caching, exact page pruning,2D PageRank windows/residual skips, community tally spill/differential fallback and FastRP dimension blocking. Only the narrower algorithm-specific certificates, revalidation and norm/replay combinations remain candidates. Read the superseding [PRD04 reconciliation and factor-native WCC/BFS build contract](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/02-prd04-architectures-Evidence.md) before attributing novelty to H1-H4. The existing-ideas table below describes PRD06 itself, not absence of those mechanisms in PRD04.

Reading basis: 89/89 assigned prose documents, including every line of the 4,638-line atlas; 9/9 support tables structurally inspected with selected rows. Exact consumed spans and individual observations are in [Evidence](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/02-prd06-Evidence.md). Progress is in [Journal](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/02-prd06-Journal.md). This lane does not certify the other eight source folders or the parent goal.

## Executive Judgment

The defensible product is initially a snapshot analytics worker over existing graph exports, not a replacement transactional database. Its differentiation must be completed useful jobs under a physical 4,000,000,000-byte limit, with low enough total I/O to meet the buyer's deadline. A heap cap, a resource receipt, or a job rejected before execution is not that result.

The atlas is a useful topic index, not 213 completed architectures. Its 71 canonical rows each receive three options. Concrete source pairs explain CSR construction, frontier switching, compression, exact WAND bounds, version publication and several iterative algorithms. Community refinement, embeddings, motif ownership, large candidate sets and external-memory state traffic remain notably underdeveloped. Repetition transfers incorrect state across algorithm families; examples and counterexamples are recorded in Evidence.

The incumbent premise needs correction: GDS documents compressed CSR, not universal pointer-per-edge objects. The opportunity is projection/build/state/output economics and end-to-end resource enforcement, not merely switching from objects to arrays. See the primary [GDS representation documentation](https://neo4j.com/docs/graph-data-science/current/production-deployment/feature-toggles/).

Prioritize H1 and H4 for small falsification experiments. H2 has stronger mathematical certification but much existing prior art. H3 addresses a genuine missing family, yet its locality and quality risks are highest. None is a universal fast graph engine.

| Hypothesis | Intended low-I/O regime | Cost paid for the benefit |
| --- | --- | --- |
| H1 shared certified joins | Many repeated exact motif/similarity jobs with selective bins and shared hot adjacency | Summary/oriented-view build, metadata reads, maintenance; loose bounds may save nothing |
| H2 residual block PageRank | Localized residual after refresh or personalization, preferably scalar state in RAM | Update consolidation, bound bookkeeping and a complete final residual audit |
| H3 revalidated community histograms | Scalar state fits, repeated local moves reuse a bounded set of histograms | Neighbor-delta traffic, serialized current-state revalidation and occasional rebuilds |
| H4 norm-separated replay | Feature layers are much larger than topology; scalar norms and one stripe fit | Repeated topology scans and possibly a full output transpose |

## Existing-Ideas Map

The following mapping covers all 71 atlas canonical IDs, grouped by actual algorithmic obligations rather than inheriting its category mistakes. Source pointers refer to documents consumed in full; source-code paths mentioned inside them were not independently inspected in this lane.

| Family and canonical IDs | Already present and genuinely developed | Missing obligation or costly regime |
| --- | --- | --- |
| Reachability/BFS/DFS:007,019,024,053,058 | `frontier-pushpull-switching` and `csr-adjacency-layout` explain sparse/bitmap frontiers, reverse adjacency and first-hit pull. | Reached set, one parent, all shortest parents, DFS order and arbitrary paths differ. Reverse-index build, frontier conversion and paged visited-state traffic count. Long-diameter sparse frontiers can cause repeated whole-file scans. |
| Weighted and k-paths:001,005,018,020,071 | `delta-stepping-buckets` explains light-edge bucket closure, stale relaxations and thread-local bins. | Bound all bins and duplicates, not just active frontier. Dijkstra/delta require nonnegative weights; Bellman-Ford needs negative-cycle policy. A* heuristic admissibility/consistency and Yen spur/exclusion/path-uniqueness work are absent. |
| Components:003,013,023,031,056,064 | `component-hooking-shortcutting` explains CAS parents, shortcutting and Afforest's exact finishing pass. | Paged random parent chasing is not sequential I/O. SCC is directed mutual reachability, not WCC. All-edge label agreement accepts an incorrect single-component partition; verify independent partition equivalence. |
| Density:036,062 | `semiring-matrix-traversal` explains masks and lower-triangle products; atlas names peeling and intersections. | k-core needs maximality, not only minimum retained degree. Triangle orientation/ownership, hub scheduling and scalar versus enumeration output are missing. Spilling does not bound wedge/intersection work. |
| Similarity:002,035,038,046 | Sorted postings, Roaring kernels and WAND supply useful building blocks. Corpus already proposes transferring safe top-k bounds. | Complete candidate generation, certified score bounds, ties, zero similarities, filters and potentially quadratic output are unresolved. Exact rescoring of an approximate candidate set is not exact top-k. |
| PageRank and other centralities:006,010,011,016,021,049 | `pagerank-iteration-convergence` specifies iterations, dangling choices and numerical comparison. | State gathers may fault per edge. Degree is not power iteration. Closeness requires distances; betweenness requires shortest-path counts/dependencies. Katz needs its own convergence condition; eigenvector/HITS are not PageRank with renamed fields. |
| Communities:012,039,040,042 | Generic local moves and modularity are named; validation notes acknowledge partition variation. Label propagation is listed under Components but has different semantics. | Community totals, neighbor-community histograms, accepted-move interactions, Leiden refinement, coarsening and convergence are not worked out. LPA may oscillate; equal labels across every edge is not its oracle. |
| Embeddings:022,026,028,029,045 | Atlas names feature tiles/sampling; original FastRP paper is in its ledger. | FastRP linear propagation, GraphSAGE nonlinear learned aggregation, HashGNN hashing and node2vec walks/training cannot share one optimizer template. Layer state, sampling/alias build, RNG, optimizer state and final `n*d` output are unbounded in the templates. |
| Sparse algebra:057,059,069 | `semiring-matrix-traversal` develops masks and operator choices. | SpGEMM fill-in can be quadratic. Endpoint products lose intermediate bindings/path multiplicity. Floating reassociation changes rounding; the semiring must preserve the requested result, not just connectivity. |
| Partitioned/out-of-core execution:027,048 | Atlas names GraphChi, GridGraph and X-Stream; source synthesis admits external-memory gaps. | Edge tiling is established. Partition cuts may be huge; state rereads, preprocessing, delta expansion and output remain. Low RAM does not imply low I/O. |
| Incremental/dataflow/joins:033,060,070 | `incremental-delta-iteration`, `superstep-message-convergence` and pull pipelines explain signed updates, arrangements, barriers and breakers. | Small input delta can cause global recomputation; trace compaction needs progress/reader rules. Join output may be enormous. WCOJ output caps are a changed result contract unless explicitly requested. |
| Graph layouts:015,032,054,063 | CSR count/prefix/scatter, base-plus-delta, record chains, permutation indexes are explained. | External-ID remapping, isolates, dual directions, parallel relationship identities, sort peak disk and snapshot publication are additional obligations. A record-store layout is not GDS's projected layout. |
| Storage primitives:004,008,014,017,044,047,055,067 | LSM selection, COW publication, Bloom negatives, bounded merge fan-in, container intersections and group commit are substantive. | ARIES is not an LSM variant. Pending writer queues, pinned roots, tombstone safety, conversions and filter residency all add state/I/O. Compression ratios and amplification examples are not guarantees. |
| Text storage/retrieval:009,025,041,050,052,061,068 | Posting skip structures, FST construction and WAND conservative bounds are developed. | FST registry build memory, fuzzy traversal work, positions/analyzers and global fusion candidate completeness matter. A document set does not validate scores/positions; exact independent top-k lists need not contain fused top-k. |
| ANN/vector storage:030,034,037,043,051,065,066 | HNSW queues/pruning, IVF lists, PQ/ADC and DiskANN packing/build sketches are concrete. | Build graph/PQ training and visited scratch are additional RAM. KD/LSH/RP trees are not IVF. Quantization and candidate omission require recall contracts; exact rescoring does not restore recall. Downstream ANN construction is not free after embeddings. |

Evidence grade: the paired notes are explanatory mechanisms with source witnesses, not executed validations. The original corpus SPEC prohibits benchmarking, and the rewrite thesis explicitly says its five-minute sample was uncompiled/unrun. Do not promote its estimates into measurements.

## Shared Physical Accounting

All numeric quantities below are decimal bytes. `n` is projected vertices including isolates; `m` is stored adjacency entries after the declared direction/multiedge policy; `d` is embedding dimension; `f` is bytes per scalar. State-width choices require checked cardinality bounds. Input bytes, projected RAM, sealed disk bytes and peak disk are four separate quantities.

### Memory Contract

Provisional worker budget `Mw=3,000,000,000`, OS/other reserve `1,000,000,000`; neither reserve has been validated on the target machine. Count unique physical pages across workers, anonymous/shared mappings and file cache, plus kernel/pinned I/O overhead. Do not sum shared RSS twice or ignore resident mmap pages. Swapping is not a permitted way to meet the physical-memory target.

`Mw >= state + codec + sort/merge + I/O buffers + ID/property caches + result buffers + runtime/threads/allocator + overlapping build/refresh state`.

One process-wide reservation authority must cover every worker and asynchronous writer. Initially serialize build, refresh and heavy queries. Concurrent small queries divide the same budget; a thread-local cap is not a machine cap. A dedicated 4 GB deployment needs measurements of the OS reserve, not just a 3 GB process RSS limit.

### Build And Disk Contract

| Phase | Work and live resources that must be charged |
| --- | --- |
| Extraction | Consistent source snapshot or explicitly reconciled watermark; bounded parser/network/decompression buffers; transfer bytes and source-side work; retained input `I` if required for replay. Streaming from another machine must be disclosed, not treated as free local disk. |
| Validation | Types/nulls, endpoint existence, relationship identity, weights, direction, overflow and chosen duplicate policy. Error/quarantine output is bounded and counted. Node input preserves isolated vertices. Filters are evaluated at the declared snapshot. |
| ID mapping | External dedup/sort of all node IDs; stable forward/inverse mapping; two endpoint joins or an equivalent measured lookup method. Random hash probes are not free. Degree reordering needs an extra permutation and mapping back to stable IDs. |
| Topology construction | External edge sort, degree/prefix construction, block compression and optional reverse sort/index. Property sidecars remain aligned with relationship identities. Dual directions and extra join permutations each cost bytes, build I/O and update work. |
| Seal/publication | Checksums, index/offset bounds, durable files, durable manifest/directory publication and snapshot identity. Atomic rename alone is not a complete persistence protocol. Readers never see half a graph. |
| Query/export | Algorithm state and intermediate files, complete result cardinality, ID translation, serialization and destination backpressure. Slow consumers pin generations; a bounded output buffer does not bound total output or completion time. |
| Refresh | Validate/resolve additions and deletions, maintain degree/weight changes, rebuild affected summaries, merge base/delta, and retain old snapshots until readers finish. Tiny updates may invalidate global answers. Stable IDs and physical order are separate namespaces. |
| Recovery | Versioned checkpoints, input offsets, immutable partition outputs and idempotent retries. Charge checkpoint writes and coexistence with earlier checkpoints. Disk-full/corruption must not publish an apparently exact partial answer. |
| Concurrency/GC | Explicit reader lifetime and generation-retention policy. Admission or documented cancellation before disk exhaustion; never reclaim pinned data silently. Query, build, refresh, output and retries share both budgets. |

For `S` bytes externally sorted with initial run capacity `K`, merge fan-in `F>=2`, `R=ceil(S/K)`, `p=ceil(log_F R)`, a simple materialized design transfers roughly `2*S*(1+p)` bytes, before parsing/ID joins and final consumer reads. `F` follows actual input/output-buffer and codec allocations. Avoided writes by streaming the final merge must be established by the concrete pipeline, not subtracted by wishful accounting.

`Dpeak = max_over_time(retained input + ID maps + old pinned sealed artifacts + new artifacts + live sort runs + algorithm spill + checkpoints + deltas + result/export staging)`.

This is a liveness sum: do not count the same physical file twice, and do not assume disjoint phases overlap. Conversely, old and new merge runs really can coexist. Bounded scratch can force more passes; declare that tradeoff. Reserve a worst-case uncompressed/escape encoding, not an average compression ratio.

One unweighted u32-neighbor/u64-offset CSR costs `4*m+8*(n+1)` before headers/indexes. Out+in is twice that when `m` denotes one direction. Properties, weights and relationship IDs are additional. A u32 sentinel can reduce usable ID range; choose widths against the actual format. For larger ID spaces use u64 or an explicitly charged shard mapping.

Illustrative counterexample: `n=100M,m=1B` yields 4.8 GB per direction and 9.6 GB dual topology. A one-way inverse mapping of 8-byte external IDs adds 0.8 GB; a searchable forward mapping costs more. Thus a roughly 10.4 GB minimal sealed artifact could look comfortable below 50 GB. But a retained 24 GB input plus old 10.4 GB snapshot plus two 24 GB sort generations already totals 82.4 GB, before the new sealed copy. This is a possible naive schedule, not a lower bound; it demonstrates why a 50 GB prepared-storage scenario grants no unlimited scratch.

### Result Semantics

Reached IDs and one parent tree are `O(n)` outputs. All paths can be exponential; all-pairs similarity can be quadratic; embeddings require at least `f*n*d` raw result bytes. Triangle scalar counting has constant-size output, per-node counts `O(n)`, enumeration `O(t)` records. Streaming bounds resident memory, not these totals. All hypotheses below pin filters, weights, relationship multiplicity, snapshot, order/ties and cancellation behavior before execution.

## H1: Certified Motif And Similarity Block Joins

**Hypothesis:** one adaptive, immutable neighbor-summary layout can avoid enough adjacency reads across both exact similarity and triangle jobs to repay its construction and refresh costs, while keeping candidate and cache state explicitly bounded. The advance is not intersections, bitmap compression, WAND transfer or tiling individually; it is a shared certified row/block hierarchy plus a measured investment rule for refining it.

### Mechanism And Correctness

1. Materialize the selected simple neighbor-set view, or explicitly choose another multiplicity contract. Freeze a total vertex order. For triangles orient every undirected edge from lower to higher order; process edge `(u,v)` and intersect forward sets `N+(u),N+(v)`. A triangle ordered `u<v<w` appears only on its `(u,v)` edge. Directed/weighted motifs require a separate definition and are not silently coerced into this job.
2. Partition the ID universe into nonoverlapping bins. Store sparse exact counts `a_b` for each row, plus offsets for its actual compressed adjacency segments. Two sets have intersection at most `U=sum_b min(a_b,b_b)`. Every bin contribution is bounded by its smaller population, proving the inequality without a sketch. Missing summaries mean unknown, never zero.
3. For nonempty binary sets of sizes `a,b`, safe score bounds are `J<=U/(a+b-U)`, overlap `<=U/min(a,b)` and cosine `<=U/sqrt(a*b)`. Define empty-set behavior explicitly. Clamp numerical bounds outward; prune only when the bound cannot beat the full score-and-ID tie order. For nonnegative weighted cosine use per-bin L2 norms and Cauchy-Schwarz, not these binary counts. Adamic-Adar needs a bound using its actual shared-neighbor weights and degree-one convention.
4. Add candidate-row block envelopes: per bin use `M_b=max_{row in block} count(row,b)` and a conservative degree range. For query row A, `sum min(a_b,M_b)` bounds intersection against every row in that block. A coarse Jaccard bound uses denominator `a+max(b_min,U)-U`, with `U<=a` and an impossible/empty block handled separately; this intentionally overbounds when no row realizes all maxima. Alternatively maximize the score bound over the recorded valid size interval. Reject an entire candidate block only from such a safe bound. Hierarchical traversal avoids first materializing all candidate pairs.
5. Stream surviving block pairs. Keep at most `r` query rows and their `k`-element heaps, or threshold-output buffers. The complete-search baseline visits all row-block pairs. A metric-specific proven prefix/inverted candidate scheme may replace that traversal only after verifying its completeness. Positive-overlap indexes need a separate zero-score/tie completion step for top-k contracts that include zero similarities.
6. Refine bins only for repeatedly ambiguous, expensive blocks: record observed reads, estimate saved future bytes, cap persistent summary bytes, and charge refinement build/refresh. Hold a bounded shared hub-block cache for a batch of edge-owned intersections. A hierarchy lets disjoint bins skip list segments; it does not imply one read per edge or one full scan per triangle job.

Triangle scalar mode accumulates a checked wide integer; per-vertex mode emits/reduces bounded count deltas or reserves the full counter vector. Enumeration streams owned triples and charges their bytes. No top-k heap belongs in scalar counting. Filters producing new neighbor sets also change set sizes; unfiltered counts alone may bound intersection but cannot be substituted into filtered Jaccard denominators.

### Resources, Maintenance And Failure

Memory: `r*k*(ID bytes+score bytes+heap overhead) + r-row bounded decode windows + fixed summary-page cache + bounded hub cache + merge buffers + runtime <= Mw`. No single high-degree row must be fully materialized. Unbounded priority queues are replaced by bounded batches or external runs. A deterministic block traversal can avoid storing a quadratic pending-pair list.

Persistent counts are bounded by nonempty row/bin incidences at one level (at most `m`), not by a magically small constant. Multiple levels add factors unless explicitly capped. Build counts in the adjacency sort/encode stream where possible; block envelopes require reductions. Finer bins save reads only when their metadata, cache misses and rebuilds cost less than the avoided list pages. Disk also includes any separate oriented view, candidate-run spill and result output. Sharing encoding between forward and full sets needs a concrete layout; do not count both as one copy by default.

Query I/O is the actual sum of summary pages, all fetched/re-fetched neighbor segments, candidate merge traffic and output. A fixed snapshot safely serves all jobs in the batch. Refresh of changed rows rebuilds their bin counts and ancestor envelopes. After deletions, old counts remain upper bounds on intersection only: row sizes, norms and block minimum sizes must be refreshed or conservatively invalidated, and U must be clamped against current valid sizes before deriving a score bound. Insertions require updating/invalidating count bounds too. Degree-based orientation may change widely, so keep an epoch-fixed order or charge a reorder rebuild.

Concrete stale-denominator counterexample: A={1,2}, old B={1,2,3,4}, new B={1,2}. Old intersection upper bound U=2 remains safe, but using old size 4 reports a Jaccard bound of 0.5 while the true new score is 1. A threshold 0.75 would falsely omit the pair. The snapshot identity must cover both topology summaries and score normalizers.

Counterexamples: interleaved disjoint neighbors share every bin; cliques share everything; hubs can evict each other; candidate output may be quadratic. Bounds then save little. Fall back to complete blocked merge/external-memory triangle or all-pairs plans and report the measured overhead. No universal near-linear triangle I/O claim is justified.

Prior-art boundary: [All-Pairs](https://www.bayardo.org/ps/www2007.pdf), especially section 4.6, already provides exact candidate pruning and a bounded-index out-of-core repeated-scan scheme. [Pagh and Silvestri](https://arxiv.org/abs/1312.0723) establish strong external-memory triangle-enumeration algorithms and model-specific bounds. Their enumeration lower bound is not a lower bound for returning only a scalar count. The proposed contribution to test is adaptive shared summaries and cache scheduling across these jobs, not invention of exact pruning or external joins.

Falsifier: after including extra indexes/build/refresh, no predeclared motif/similarity workload beats its strongest exact external-memory baseline on total I/O at equal semantics. Also kill immediately on a missed tie/threshold result, overflow, stale-bound false negative or 4 GB violation.

## H2: Residual-Certified PageRank Block Scheduling

**Hypothesis:** schedule compressed source blocks by certified residual mass per estimated byte, coalescing state updates under a fixed budget, to reduce scans after localized refresh or for localized personalization. This is not a novel residual-push algorithm; the candidate improvement is a storage-aware scheduler with explicit outstanding-update accounting and a charged final audit.

### Operator And Certificate

Let `P` be a nonnegative row-stochastic transition matrix, including the declared dangling-node redistribution; `v` is a probability vector; `0<alpha<1`; `b=(1-alpha)*v`. Target `x*=b+alpha*P^T*x*`. For candidate x, `r=b+alpha*P^T*x-x`, hence

`||x*-x||1 <= ||r||1/(1-alpha)`.

Reason: `x*-x=(I-alpha*P^T)^(-1)r`; the Neumann series has induced 1-norm at most `sum alpha^j=1/(1-alpha)`. This reasoning is for this operator and norm, not a GDS stop-rule compatibility claim. Numerical implementation must conservatively include rounding/reduction error `eta`, yielding certificate `(U_residual+eta)/(1-alpha)<=epsilon`.

A block push chooses `d=r` on selected vertices and zero elsewhere, then updates `x<-x+d`, `r<-r-d+alpha*P^T*d`. This retains the residual invariant, including self-loops; it must not simply set each pushed residual to zero and forget incoming/self contributions. Signed residuals are needed after graph refresh. Schedule fairly so low-priority blocks cannot starve indefinitely.

### Storage And Scheduling

In the scalar-state-fit regime retain x/r and outgoing weight totals; fetch only source edge blocks needing propagation. Destination updates then touch RAM, not disk per edge. Page/block bounds use sums of absolute outstanding residuals. A cost estimate may influence scheduling, but never stopping correctness.

In the scalar-state-spill regime, source blocks append `(destination,delta)` records into fixed-capacity sorted runs. Apply a destination tile only after accounting for all updates needed for its selected coordinates. Unapplied logs count in the residual upper bound by the triangle inequality; cancellation may loosen that bound but never reduce it unsafely. Merge runs with bounded fan-in, and force consolidation before the run-count/disk quota is exhausted. A macro-epoch can freeze source increments, stream all affected edge blocks, then apply grouped updates, avoiding per-edge random writes at the expense of more work per epoch.

Dangling pushes may distribute to every vertex. Batch them into an explicitly charged dense state pass before using the affected residuals again; do not pretend a lazy scalar has been applied to every coordinate for free. Uniform v is implicit; arbitrary v requires storage and reads. A global residual audit recomputes `b+alpha*P^T*x-x` at the end, charging full topology and any state joins. If conservative floating error cannot be certified tightly, return an uncertified numerical result rather than assert the bound.

Core vectors cost `16*n` with f64 x/r. Outgoing totals add `8*n`; arbitrary personalization another `8*n`; scheduler directories, residual audit tiles, logs, mappings and runtime are additional. Thus even 32*n is not the entire job. At n=50M these four arrays consume 1.6 GB; at n=100M they consume 3.2 GB before any buffers and require a different regime. There is no assertion that every 50 GB graph has scalar state fitting RAM.

I/O charge: touched topology pages plus rereads, destination run writes/merges, state-tile reads/writes, checkpoints, final audit and output. A naive 16-byte log per active edge generates 16 GB per billion edges before merge amplification, easily losing to a 4 GB u32 neighbor scan. Early in-memory combination and low active-block fraction are therefore necessary measured conditions, not optional optimizations.

Refresh: a changed outgoing weight renormalizes the entire source row, not merely the changed edge. Recompute its contribution difference; dangling transitions and changed personalization can affect all nodes. Warm-start at old x, compute the true new residual, and invalidate the old certificate. Manifest the graph/operator version together with state and update logs. Retry applies each logged batch once.

Prior art: the [Gleich/Kloster seeded-PageRank paper](https://www.cs.purdue.edu/homes/dgleich/publications/Kloster%202016%20-%20seeded%20pagerank.pdf), sections3.1-4.1, explicitly describes residual coordinate push. [GridGraph](https://www.usenix.org/conference/atc15/technical-session/presentation/zhu) already provides selective edge-block scheduling. Neither idea is claimed new here. The residual inequality above is a mathematical derivation, not a reported benchmark.

Failure regime: a uniformly active expander, high alpha, strict tolerance, high-churn refresh or dense personalization can activate almost everything. Final audit may dominate a cheap query. Fixed-iteration Jacobi compatibility cannot silently be replaced by an asynchronous epsilon solution. Baseline must include a state-fit streaming PageRank plan and an out-of-core blocked plan, not only a failed in-memory projection.

## H3: Community Histograms With Revalidated Moves

**Hypothesis:** bounded, selectively retained neighbor-community histograms plus versioned move revalidation and a dirty-volume rebuild threshold can reduce repeated adjacency reads during Louvain-like community optimization. Incremental affected-vertex scheduling already exists; the proposed part is the coupled cache/spill/revalidation policy under the physical limit.

### Correctness Contract

Start with nonnegative weighted undirected graphs and an explicit self-loop/parallel-edge convention. For symmetric adjacency A, `s_i=sum_j A_ij`, `W=sum_i s_i`, define `Q=(1/W)*sum_ij (A_ij-gamma*s_i*s_j/W)*[c_i=c_j]`. Zero W is handled separately. Return a partition, its recomputed Q, parameters, seed/schedule and termination reason. This is a heuristic quality contract, not a global modularity optimum or identical GDS partition.

For each candidate vertex, neighbor-community weights and current community totals give move gains. Generate proposals against a frozen label epoch, but serialize acceptance and recompute gain using current labels/totals before every accepted move. Nonadjacent vertices may still share source/target communities, so independence based only on graph adjacency is invalid. Accept only gains above the defined numerical threshold. Stopping after a work cap is budget termination, not proof of local optimality.

### Bounded State And Avoided Reads

Keep scalar labels, degrees, label versions and community totals when they fit. Allocate a capped histogram cache only to rows with demonstrated reuse. A histogram has at most the row's number of distinct neighboring communities, which can equal its degree; do not allocate an unbounded map for a hub. Construct oversize rows by external reduction or multiple passes over bounded community-ID ranges. The latter trades capped scratch for known repeated adjacency scans.

An accepted vertex move creates histogram differences for its neighbors. Coalesce these into bounded sorted runs keyed by `(neighbor,community)` rather than perform disk random writes. Before evaluating a cached histogram, merge all relevant deltas or rebuild it from the current labels; version checks on only the vertex itself are insufficient because its neighbors may have moved. Cache membership and log retention are explicit, and a cache eviction discards derived state, not unapplied semantic changes.

Community-total changes affect gain calculations even when a row's adjacency and histogram did not change. Read current totals during revalidation. A frozen histogram batch is not safe for simultaneous unchecked moves. The baseline acceptance lane is serial; parallel acceptance is deferred until a proven conflict rule includes community totals and neighbor-label changes.

When the byte estimate for incident-edge scans of accepted moves plus sorting/merge traffic exceeds a measured full-scan rebuild budget, discard derived histograms and rebuild affected batches by a complete scan. Proposal records are also batched under a fixed allocation; scheduling them by locality cannot require an unbounded pending heap. If a data structure's worst-case scratch is too large, use the multi-pass bounded-range plan. Neither rebuild nor restart loses accepted labels. This policy bounds derived memory but does not guarantee fewer scans in every graph.

For labels u32, degrees f64, vertex versions u64 and community totals f64, scalar state is approximately `20*n+8*C` bytes, with `C<=n`; use wider IDs when necessary. At n=50M, C=50M that is 1.4 GB, leaving 1.6 GB for runtime, caches, I/O and logs. At n=100M it leaves only 0.2 GB and likely requires a spill plan. An all-vertex histogram file could require `O(m)` multiword records and is not implicitly affordable; selective caching is central to the design.

H3's intended low-I/O claim is explicitly the scalar-state-fit regime. When labels/totals spill, rebuilding a histogram requires paged lookups or external joins with the current labels; revalidating each serial move can make those joins prohibitively expensive. An external-memory full-scan community baseline remains available, but this dossier does not assert H3 accelerates that regime. The experiment must report the crossover rather than silently extend the claim.

Coarsening externally sorts/reduces weighted community edges and preserves self-loop weights. Charge coarsened graph, old level, original-to-level mappings, sort generations and output. Histograms use the graph/label epoch in their identity. Refresh invalidates changed-edge neighborhoods and weighted totals, but a small change can trigger a global partition change; quality is compared to full recomputation, not assumed equivalent.

Leiden is a separate extension, not a name for this plan. Its prescribed refinement must be executed and verified, including how refinement subgraphs fit/spill. A connectivity check after a Louvain run does not recreate all Leiden guarantees. The [original Leiden paper](https://arxiv.org/abs/1810.08473) distinguishes connectedness and stronger iterative local-optimality properties from Louvain.

Prior-art boundary: [Delta-screening](https://arxiv.org/abs/1904.08553) and [DF Louvain](https://arxiv.org/abs/2404.19634) already select affected vertices and maintain changed graph/community information. Their abstracts were checked, not their entire implementations; their reported large-server speedups are not evidence for this machine. Benchmark against them where practicable, alongside sequential full recomputation.

Failure regime: one giant community creates contention; expander-like boundaries and oscillating candidates generate degree-proportional delta traffic; rejected proposals still cost revalidation reads. A cold graph with no histogram reuse is worse than direct scanning. Kill the hypothesis if cache/log bookkeeping outweighs saved adjacency I/O or if quality distributions degrade beyond the buyer's agreed limit.

## H4: Norm-Separated FastRP Replay

**Hypothesis:** separate global row norms from dimension-independent propagation, replaying deterministic dimension stripes to avoid full embedding-layer files. This may lower scratch and total I/O when feature matrices are much larger than topology. It may instead be worse when topology is large, stripes are narrow, or consumers require an expensive transpose.

### Restricted Mathematical Contract

Use a pinned linear propagation `E_l=Ahat*E_(l-1)` with deterministic initial R and final `Y_i=sum_l w_l*E_l[i,:]/||E_l[i,:]||2`, including l0 if requested. Zero-norm rows follow a defined zero-vector rule. Ahat contains the declared direction, finite weights and averaging/degree rules. Arbitrary nonlinear normalization inside propagation is outside this particular derivation.

The [GDS FastRP documentation](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/fastrp/) describes neighborhood averaging of intermediate vectors followed by a weighted combination of their row-normalized values. Its property initialization is also described. This supplies a useful candidate contract, not verified source-level parity. The [original FastRP paper](https://arxiv.org/abs/1908.11512) distinguishes sparse random projection from optimization-based embedding training.

1. Choose stripe width c to fit two propagation buffers and one output accumulator across all n vertices. For each stripe, generate the corresponding initial random/property dimensions and propagate L layers. Accumulate f64 sum-of-squares per vertex and participating layer; discard stripe intermediates after their norm contributions are recorded.
2. Once all stripes have contributed, finalize row norms. Replay each stripe deterministically through the same L layers, now accumulating `w_l*E_l/known_norm_l` into its output stripe. Emit that stripe; no complete `n*d` intermediate layer must be retained.
3. In exact arithmetic, matrix multiplication acts independently on each output dimension, sum-of-squares is additive over stripes, and the final row normalization uses the same full norm. These facts establish the restricted equivalence. Floating reduction order can change norms; require a numerical error contract and compare each layer, not only downstream recall.
4. A counter-based RNG keyed by stable vertex ID, dimension and seed makes replay independent of stripe order. That changes GDS parity unless its initialization and RNG mapping are matched. For a new FastRP-like contract, compare against a full-matrix oracle using the identical RNG; for GDS compatibility, source-level RNG/operator validation remains a blocking prerequisite.

### Accounting And Tradeoff

State approximately `3*f*n*c + 8*n*(L+1)` for current/next/output stripe and all participating norm accumulators, plus degree/weight data, property initialization, topology windows, maps and runtime. If l0 is unused its norm can be omitted. The scalar norm sidecars must fit or spill with their I/O charged; this is not dimension striping as a universal solution for arbitrary n and L.

Logical topology traffic without reuse is `2*L*ceil(d/c)*T`, T being bytes scanned for one propagation. Physical reads may be lower with a genuinely budgeted cache. Final raw output is `f*n*d`. Checkpointing can preserve norms and completed output stripes, then replay an interrupted stripe from its beginning; persisting all stripe states is optional extra I/O/disk, not free recovery.

Worked arithmetic, not a benchmark: n=10M, m=100M, d=128, L=3, f=4, c=16 gives 1.92 GB stripe buffers, 0.32 GB norms and 0.08 GB degree scalars: 2.32 GB before runtime and I/O. At most 0.68 GB remains inside the provisional worker budget. Unweighted one-direction topology T is about 0.48 GB, so 48 logical passes total 23.04 GB. Output alone is 5.12 GB. A materialized-layer implementation instead writes/reads multi-GB feature files, but a competent fused or tiled baseline can reduce that traffic; do not claim a win from an intentionally wasteful baseline. Measure both including initial projection, normalization, final output and checkpoint costs.

This regime may permit topology caching but no full feature layer. At n=100M, d=128, f=4, output is 51.2 GB before topology or metadata, already beyond a 50 GB total-persistence cap. No replay strategy changes that arithmetic.

Stripe-major output is a real format choice. If downstream consumers require row-major vectors, an external transpose adds at least a full output read and write, plus its scratch and page amplification; count it. A downstream ANN index adds training/build graph/codebook/ID/result storage. A narrow output contract that never emits full vectors must be separately justified by the buyer, not silently substituted.

Refresh changes the operator and therefore potentially every L-hop neighborhood; no claim of local cost without a bounded dirty closure. Recompute both norm and replay passes on the immutable new snapshot unless an exact incremental norm proof is implemented. Property projection bytes and feature normalization rules belong in the manifest too.

Failure regime: large topology, tiny feasible c, deep L, expensive property regeneration or output transpose erase savings. GraphSAGE's learned dimension mixing/nonlinearities and node2vec's walk/training state do not inherit this proof. HashGNN needs its own hash/aggregation semantics. This is an embedding-family-specific proposal, deliberately not a universal embedding engine.

Prior-art boundary: random projection, dimension blocking and recomputation/checkpointing are established ideas. The corpus does not develop the two-pass norm/replay coupling above. A broader literature and implementation search is still necessary before any global novelty statement. The useful result would be a verified I/O crossover and complete 4 GB workflow, even if the combination is independently known.

## BFS And Other Baseline Regimes

Do not force every family through the four hypotheses. A plain compressed adjacency implementation with proper state accounting may be best for BFS. At n=100M, a visited bitmap plus two frontier bitmaps costs 37.5 MB; u32 distances and one u32 parent add 800 MB. The 837.5 MB state subtotal leaves room for bounded topology windows and mappings. This is a possible state-fit regime, not a full memory bill. At n=500M the same arrays alone need 4.1875 GB, so this layout fails.

Sparse push should read indexed adjacency of frontier vertices; pull can save edge probes when frontiers are dense, but incurs reverse-layout cost. On n=100M, m=1B, scanning a 4.8 GB topology for 50 levels is 240 GB logical reads before outputs. Selective access can avoid that, or become random-I/O dominated. Preserve level barriers when spilling; perform membership/dedup by bounded external partitions if necessary. Returned paths require valid predecessors; all shortest paths require more than one predecessor and potentially huge output.

For SSSP spill sorted bucket relaxations with stale-distance rejection, minimum-bucket selection and repeated light-edge closure; charge re-sorting and duplicates. For WCC, compare blocked hooking against external sorting approaches when parent state does not fit. For SCC, use a genuinely directed algorithm and validate mutual reachability. For all-source centrality and SpGEMM, admit the large work/output honestly instead of claiming compressed topology makes them cheap. These are known baselines, not a fifth innovation claim.

## Buyer And Product Tests

The domain maps and two PMF analyses are demand hypotheses, not validated sales evidence. They identify recurring analytics and integration friction; they do not establish that every Neo4j workload wants a 4 GB replacement. Preserve the source database and offer a snapshot export/job/result integration first.

| Buyer workflow | Useful result and paid problem to test | Alternative and switching cost |
| --- | --- | --- |
| Fraud/risk analyst | Repeatable triangle/local-clustering and exact high-similarity candidate jobs, with witness IDs for investigation. Ask for a currently failed/too-costly job and its deadline. Similarity/community membership is not proof of fraud or identity. | Native GDS, columnar SQL joins and existing offline pipelines. Cost: extracting the correct filtered snapshot and returning IDs/evidence into the analyst workflow. |
| Recommendation/data engineer | Scheduled FastRP-like features or exact neighbor sets with agreed numerical/recall contract and freshness. Include downstream model/ANN usefulness and full output handling. | Native GDS or established embedding tooling. Cost: seed/operator compatibility, retraining and vector/index transfer, not merely graph execution. |
| Community/cohort analyst | Stable-enough exploratory cohorts with reported modularity/connectivity, budget/seed and refresh history. Determine whether quality variation is acceptable. | Existing Louvain/Leiden tooling. Cost: explaining changed partitions and maintaining interpretable comparisons. Strict algorithm parity may rule out H3. |
| Dependency/SBOM engineer | Reverse reachability with type/version/time filters and one valid witness path where requested. Meet repeated queries without projecting irrelevant properties. | Recursive SQL, in-memory indexes and existing dependency tools. Cost: semantic ingestion and workflow integration; a generic graph engine may be unnecessary. |
| Ranking owner | Nightly PageRank with pinned operator, tolerance and complete result export after small updates. | Compressed GDS and streaming out-of-core iterations. Cost: reconciling normalization and tolerance semantics with existing rankings. |

Three customer tests before broad product work: obtain real input/snapshot/query/result contracts for three recurring failed jobs; run the strongest incumbent configurations on the same total-machine budget; ask whether the completed result, freshness and integration savings justify paying or switching. Failure to fit their result export, or no costly recurring job, invalidates the proposed wedge even if a kernel benchmark wins.

Full Cypher/transaction/APOC/driver parity is out of the initial scope. The corpus's compatibility tests are useful scaffolding but not proof of behavioral completeness. Authentication, transaction semantics and online write availability remain source-system responsibilities unless separately implemented and verified.

## Verification-First Experiments

No tests below have been executed. These are executable-specification targets for a subsequent implementation task; this lane intentionally writes no algorithm code. First establish semantic truth and exact resource measurements, then optimize.

| ID | WHEN / SHALL acceptance target | Oracle, measurement and decisive failure |
| --- | --- | --- |
| 02-V01 Semantics | WHEN ingest includes isolates, parallel edges, loops, reversed edges, filtered properties, huge IDs and invalid weights, THEN each job SHALL use its declared projected multiset/set and reject unsupported values without partial publication. | Tiny independent enumerator and projection checks; include IDs that actually collide when truncated. Verify edge IDs/weights, not just neighbor sets. |
| 02-V02 H1 exactness | WHEN every small graph/set collection is enumerated within a declared finite domain, THEN triangle scalar/per-node/triple results and similarity threshold/top-k SHALL match the independent oracle including ties and zero scores. | Check each bound against exhaustive true maxima, including weighted cosine and filtered denominators. Clique, bipartite, star, interleaved-disjoint, identical-row and empty-set fixtures. One false-negative bound kills the design. |
| 02-V03 H2 certificate | WHEN weighted directed/dangling graphs, personalization and signed refresh residuals are processed, THEN reported L1 error bound SHALL contain the true error to a high-precision solve. | Exercise self-loops, disconnected graphs, alpha near 1, log spill, cancellation and rounding. Separately compare fixed-iteration mode; never treat its mismatch as permitted epsilon parity. |
| 02-V04 H3 moves | WHEN proposals are delayed across conflicting accepted moves, THEN every accepted move SHALL have recomputed positive gain under the current partition and exact maintained totals. | Recompute Q independently after each move on small graphs. Nonadjacent vertices sharing communities, self-loops and coarsening fixtures; audit stale histogram updates. Separate quality distributions over seeds from partition identity. |
| 02-V05 Leiden boundary | WHEN the product advertises Leiden, THEN its actual refinement/coarsening rules and stated guarantees SHALL be independently verified, not inferred from a post-hoc connectedness check. | Until then label H3 Louvain-like only. Use original-method oracle and disconnected-Louvain counterexamples. |
| 02-V06 H4 replay | WHEN stripe widths, traversal order and restart points vary, THEN each raw layer, full norm and final vector SHALL match the pinned full-matrix oracle within declared error, with no missing output stripe. | Same RNG/operator, zero norms, property dimensions, weighted rows, deterministic recovery. For GDS parity validate its actual source/version/RNG first. Include transpose and ANN consumer where required. |
| 02-V07 Other families | WHEN testing traversal/components/density, THEN exact results SHALL match their own independent contracts. | Distances plus valid one-parent witnesses; all-path contract separately. SCC on a->b; WCC on disconnected components; maximal k-core rather than empty-set-vacuous invariant; zero-weight SSSP and negative-cycle policy. |
| 02-V08 Resource cap | WHEN extraction, build, query, refresh, export and retry execute on the same 4,000,000,000-byte machine, THEN measured whole-machine memory SHALL stay within the cap without swap and all admitted exact jobs SHALL complete correctly. | Dedicated bounded environment, validated OS reserve, physical page accounting, allocator peaks, major faults, cache and pinned I/O. A process limit alone or high refusal rate fails the usefulness claim. |
| 02-V09 Disk/crash | WHEN disk-full, torn output, kill-before/after-publication, slow pinned readers and retries occur, THEN recovery SHALL expose only complete generations and exactly-once result partitions. | Failpoints at data/manifest/checkpoint boundaries; record peak live disk including old/new/scratch/output. No partial exact-result label. Do not test on source production data. |
| 02-V10 I/O crossover | WHEN comparing matched end-to-end jobs cold and warm, THEN the proposed improvement SHALL repay its added build/refresh cost over the declared number of uses. | Logical/physical bytes read/written, page faults, random IOPS, decode CPU, p50/p99 latency, output size, refresh expansion and disk high-water. Include projection/export; disclose all cached/prebuilt assets. |

Experiment order: V01 and tiny family oracles; H1/H4 semantic prototypes; hard memory/disk harness; adversarial cold-cache I/O; real workloads; then H2 and H3. Parent research may reprioritize based on independently read GDS code or customer evidence. Do not implement sophisticated scheduling before a correct bounded baseline exists.

Predeclare a practical success target rather than invent a result afterward: for each hypothesis seek at least 20% lower end-to-end bytes transferred on two relevant workload shapes, no correctness/resource failures, and a buyer-accepted latency/quality outcome. Treat 20% as an experimental decision threshold, not a theorem or measured claim. Publish losses on the adversarial shapes as well. A well-measured narrower win is more useful than a universal claim.

Use graph families with the same n/m but different topology: bounded-degree road-like graphs, power-law hubs, planted communities with controlled mixing, uniform expanders, cliques, bipartite graphs and interleaved neighbor sets. Vary active fraction, output threshold/k, update locations/degree, dimensions/layers, filters and available scratch independently. Uniform random updates are insufficient for worst-case refresh.

For amortization, if extra preparation costs `B_extra` bytes and each matched query saves `Q_saved>0`, the break-even count before refresh is at least `ceil(B_extra/Q_saved)`, increased by maintenance cost. If Q_saved is nonpositive there is no I/O break-even. Physical bytes alone do not establish latency: random IOPS, CPU/decompression and output stalls may dominate.

## Primary Verification Scope And Open Questions

External checks were performed 2026-09-19 UTC. GDS representation and FastRP are official documentation reads; GridGraph's overview establishes prior tiling. All-Pairs methods and disk-resident section 4.6 were inspected. The seeded-PageRank paper's operator/push sections were inspected. Triangle-I/O, FastRP-original, Leiden and dynamic-community abstracts were inspected. This is not a claim to have read every external paper fully, rerun their experiments, or inspected local GDS source.

Unresolved before implementation: exact target GDS version/operator/RNG; physical runtime reserve on the target OS; real input sizes and ID/property distributions; chosen 50 GB sealed versus peak-disk allowance; customer output/latency/freshness contracts; state-fit crossover; complete primary-code/literature novelty audit. These do not prevent delivering this research, but they prevent claiming the hypotheses already work.

## Handoff

All assigned prose reading is complete; no unread file/span remains. All nine support tables have explicitly limited structural/selected-row coverage. Four hypotheses, the family map and verification contracts are ready for parent-lane integration. No source documents or other lanes were edited, no agents were launched, no code was implemented, and no commits were made.

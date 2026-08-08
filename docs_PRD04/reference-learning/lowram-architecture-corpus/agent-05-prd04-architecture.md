# Agent 05: PRD04 Low-RAM Architecture Corpus

Status: complete evidence synthesis
Binding source: `docs_PRD04/A007-spc-founder-interview-prep-v7.md`  
Frozen denominator: `docs_PRD04/reference-learning/lowram-architecture-corpus/evidence/all-documents-denominator.tsv`  
Corpus assignment: exactly 68 rows where `assigned_agent=agent-05`

## Executive Decision

Knight Walker SHALL be designed as low-RAM deterministic OLAP graph compute, not as a database. Neo4j compatibility is an adoption adapter and verification oracle. It SHALL NOT pull transactions, mutable database storage, administration, general Cypher breadth, or server-platform ceremony into the product core.

The architecture target is an artifact-to-answer bounded runner:

```text
portable artifact
  + declared hard budget
  + complete working-set estimate
  + explicit fit | spill | approximate | refuse decision
  + enforced execution plan
  + deterministic result and resource receipt
```

Every conclusion below is constrained by the 68-row frozen denominator. Source
code cross-checks are separately identified and do not silently become corpus
files.

## Evidence Method

1. Read A007 completely before all other corpus work.
2. Preserve the frozen denominator's `path`, `sha256`, `bytes`, and `extension` fields exactly.
3. Semantically read architecture, founder, and algorithm-storage Markdown.
4. Structurally query the 36 MB raw research dump and generated TSV ledgers; do not pretend every generated line is equal-value prose.
5. Inspect the XLSX evidence matrix with a structured workbook parser.
6. Cross-check high-impact architecture claims with `@sdsrs/code-graph` 0.114.1 against the relevant Neo4j/GDS repositories.
7. Assign exactly one evidence row and one unique `A05-######` identifier to every one of the 68 frozen files.
8. Recompute filesystem SHA-256 and byte size after writing the outputs and require zero denominator gaps or mismatches.

## Founder Contract Extracted From A007

### Product boundary

- Build an **artifact-to-answer bounded graph runner**.
- Do not build a general graph database, AI memory platform, or every graph algorithm.
- Treat compatibility as an adoption mechanism for unchanged useful queries and familiar result shapes, never as the engine architecture.
- Lead with the first ICP's job: security, IAM, dependency, SBOM, and access-path analysis.
- Treat codebase intelligence as a fast demo and founder-advantage wedge, not automatically the highest-budget market.

### Enforceable systems contract

- The estimate SHALL include representation, fixed, per-node, per-edge, frontier/queue, output, conversion/projection, runtime, and safety-reserve state.
- Admission SHALL choose `fit`, `spill`, `approximate`, or `refuse` before execution.
- Execution SHALL enforce a hard RSS or cgroup ceiling; an estimate without enforcement is not the product.
- Approximation SHALL be explicit, opt-in, quality-bounded, and identified in the receipt.
- The post-run receipt SHALL expose actual peak RSS, mapped/retained memory where measurable, I/O and spill, wall/CPU time, output cardinality/checksum, estimator error, and engine/artifact identity.
- Determinism SHALL cover result identity, stable tie-breaking, seeded randomness, and reproducible execution metadata. It SHALL NOT falsely promise identical wall time or physical page-fault schedules.

### Product falsifiers

- The customer's binding pain is ingestion, schema design, permissions, UI, or a product-specific workflow rather than graph execution.
- A tuned existing system already meets the hard-budget job with acceptable certainty and ceremony.
- Conservative refusal does not improve planning or trust.
- Receipt-grade evidence does not change repeat-use or willingness to pay.
- The low-RAM plan only relocates memory into uncontrolled page cache or unmeasured build/conversion peaks.
- The 50 GB on 16 GB milestone works technically but does not correspond to a real artifact and decision-changing job.

## Founder Thesis Evolution And Superseded Claims

| Stage | Useful contribution | Superseded or narrowed by A007 |
|---|---|---|
| A000/A001 | Storage-shaped traversal, scoped 4.5x walk-path evidence, explicit rejection of a broad Neo4j replacement. | Broad market list, unverified revenue claims, and “top seven cover 80-90%” are not decision-grade evidence. |
| A002 | Memory honesty, deterministic paths, and pre-run admission become product concepts. | Graph universality does not prove a company or a database category. |
| A003/A004 | Embedded OLAP rather than an Oracle-of-graphs; quote/receipt as commercial surface; Kuzu and GraphChi acknowledged. | Agent memory as first wedge and “quote-before-run” as empty whitespace were later invalidated. |
| A005 | Full `fit/spill/approximate/refuse` vocabulary; output receipts; working-set statistics; out-of-core lineage. | “Zipf pays the subsidy” is a workload hypothesis, not a universal capacity theorem. Scarcity does not automatically yield an accurate estimator. |
| A006 | Clear artifact-to-job GTM, fast-learning code/security wedge, and receipt-as-behavior framing. | Pre-run certainty alone is competitively incomplete because Neo4j and other systems already estimate. |
| A007 | Estimate becomes an enforceable portable contract; security/dependency/access paths become the best evidenced ICP. | Binding current thesis. |

## Architecture Option Registry

The registry will be completed from the architecture corpus. IDs are stable labels for the per-file evidence ledger.

| ID | Architecture option | Current status |
|---|---|---|
| `OPT-CONTRACT-01` | Full-working-set estimator plus hard admission/enforcement/receipt | Choose; binding product invariant |
| `OPT-COMPAT-01` | Narrow read-only Bolt/Cypher/GDS compatibility facade | Choose as adoption adapter; scope by first ICP |
| `OPT-DB-REWRITE-00` | Full Neo4j/database rewrite | Reject |
| `OPT-GENERIC-CSR-01` | One universal CSR/CSC representation for all algorithms | Defer as baseline only; reject as sole architecture |
| `OPT-HOT-COLD-01` | Degree/traffic-aware hot, warm, and streamed adjacency strata | Experiment; workload-shape dependent |
| `OPT-MMAP-01` | Memory-mapped immutable artifact pages | Choose as one substrate, not as a RAM guarantee |
| `OPT-SPILL-01` | Runtime-owned bounded spill with explicit I/O accounting | Choose; implementation varies by algorithm |
| `OPT-APPROX-01` | Explicit bounded approximation plans | Defer until exact first slice is proven; never silent |
| `OPT-ANSWER-01` | Precomputed answer or query-shaped artifact | Experiment for repeated immutable workloads; reject as universal compute replacement |
| `OPT-CANONICAL-01` | Small immutable canonical generation as rebuild truth | Choose; it is compiler input and verification control, not the universal serving layout |
| `OPT-PORTFOLIO-01` | Per-family portfolio of algorithm-shaped artifacts | Choose, but add variants only when reuse pays build, disk, invalidation, and operational cost |
| `OPT-FAST-01` | Resident algorithm-native topology and maximum admitted lanes | Experiment per algorithm; warm-latency profile, not a low-RAM claim |
| `OPT-STRICT-01` | Fixed-buffer external-memory execution under an external memory ceiling | Choose as the hard-budget profile; accept additional I/O and passes |
| `OPT-SELECTIVE-TRANSPOSE-01` | Build reverse topology only for algorithms/queries that need it | Choose; reject unconditional dual adjacency |
| `OPT-STATE-CAPSULE-01` | Pre-reserved per-run mutable state with no hidden growth path | Choose; required for enforceability |
| `OPT-ADAPTIVE-ROW-01` | Degree/density-selected inline, packed-list, bitmap, or hub-page rows | Experiment after plain controls; quote actual artifact bytes rather than a universal ratio |
| `OPT-RECOMPUTE-01` | Recompute deterministic intermediates instead of retaining them | Experiment where extra scans have a closed bound and preserve exact semantics |
| `OPT-QUOTIENT-01` | SCC/component/community quotient or certificate artifact | Experiment for repeated high-stakes questions; generation-bound |
| `OPT-PRECISION-01` | Reduced or staged numeric precision | Defer from the exact first slice; expose as a distinct numerical contract |
| `OPT-PRUNE-01` | Sound bound-based candidate/block pruning | Experiment; require a proof that skipped work cannot change the declared answer |
| `OPT-BUILD-BUDGET-01` | Builder admission, spill, retained-generation, and publication budget | Choose; low run RAM is invalid if construction is unbounded |
| `OPT-OUTPUT-BOUND-01` | Bounded/streamed result sink with admission-visible cardinality policy | Choose; output is part of the working set |
| `OPT-ADAPTER-01` | BloodHound/OpenGraph, Neo4j export, Parquet/Arrow, or codegraph adapter | Choose exactly the first-customer adapter; defer broad ingestion surface |

## Corpus Accounting

At initialization, all 68 assigned files existed and exactly matched the frozen
denominator's SHA-256 and byte count. The corpus contains 62 Markdown files,
four generated TSVs, one XLSX workbook, and one raw research dump. Their frozen
total is 50,757,703 bytes.

Coverage was intentionally unequal by evidence type:

| Coverage status | Files | Treatment |
|---|---:|---|
| `semantic_read` | 56 | Founder, architecture, algorithm-storage, market-evidence, current spec, compact reference-learning prose, and the complete GitHub repo longlist were read for claims, caveats, contradictions, and decisions. |
| `superseded_classified` | 6 | Earlier executable plans and the older simulation were classified against later binding documents; useful requirements were retained, but their product boundary was not inherited. |
| `structured_queried` | 6 | The raw dump, three generated evidence ledgers, their denominator, and the XLSX workbook were counted, grouped, and queried as structured evidence rather than represented as line-by-line semantic reading. The workbook's sheets and row contents were parsed through its ZIP/XML structure. |

The 36,341,645-byte raw dump has 31,856 lines, 15 top-level headings, 10,173
URL occurrences, 382 memory-estimation matches, 1,385
fit/spill/approximate/refuse matches, and 724 algorithm-name matches. It is a
research substrate, not 31,856 independent corroborating claims.

The workbook contains five sheets: `Read Me` (23 rows), `Evidence Matrix` (66
rows), `Top 15` (16 rows), `Claim Audit` (15 rows), and `Summary` (41 rows). Its
65 evidence records grade as A=26, B=16, C=21, D=2; their declared directions
are Supports=46, Mixed=11, Counter=8. Those are the workbook author's
classifications, not independent replication by Agent 05.

The three generated source ledgers contain 12,847, 12,213, and 7,202 data rows.
Their 32,262-row denominator resolves to 5,014 direct reads, 19,186 graph-indexed
files, 7,103 non-code classifications, 845 binary classifications, and 114
generated classifications. A graph-indexed row proves inventory/searchability;
it does not prove semantic comprehension of that file.

## Architecture Findings

### F-001: The product architecture is a contract wrapped around a portfolio

The corpus does not support one universal low-RAM graph format. It supports a
small canonical generation, algorithm-shaped derived artifacts, bounded mutable
state, and one admission/enforcement/receipt spine. Physical layout and runtime
policy are independent axes: a small topology does not bound scratch, and a
receipt does not improve locality.

### F-002: Representation elimination is the first optimization

The highest-confidence savings remove structures entirely: unused properties,
unneeded edge orientation, eager projections, pairwise clique expansion,
unrequested full output, retained old generations, stored random walks, and
concurrent algorithm lanes. Compression is a second-order choice to benchmark
after the required representations are known.

### F-003: Per-algorithm materialization narrows uncertainty; it does not abolish it

The exact built artifact length and fixed state slabs are arithmetic. Dynamic
frontiers, shortest-path queues, similarity candidates, Louvain tally maps,
iterations, spill amplification, and output cardinality remain data- or
execution-dependent. A valid quote therefore separates hard controlled bytes,
modeled intervals, calibrated expected bytes, and the conservative admission
bound. Claims that the manifest makes all memory exact are rejected.

### F-004: Fast and strict are different products profiles

The fast profile may use resident or memory-mapped views and optimize warm
latency. The strict profile uses fixed readers, bounded buffers, controlled
concurrency, spill limits, and an external RSS/cgroup ceiling. `mmap` is useful
addressing and cache policy; it is not enforcement. Strict plans can be much
slower than a warm resident GDS run while still enabling a job that otherwise
cannot run on the machine.

### F-005: Algorithm state often dominates topology

Reachability can use bitmaps; WCC has a clear dense-label floor; PageRank needs
dense rank state; NodeSimilarity is governed by candidate generation; Louvain
by dynamic neighboring-community tallies and level overlap; FastRP and
GraphSAGE by `V * D` matrices/features; Node2Vec by walks unless regenerated or
streamed. No topology codec removes these terms.

### F-006: Query shape, result mode, and cadence belong in physical identity

The optimization key is `(generation, logical view, algorithm, parameters,
result mode, exactness, cadence, budget)`, not merely the algorithm name.
Source-target BFS, bounded reachability, all-reachable output, and proof-path
output have materially different state and output bills. The same is true of
PageRank top-k versus stream-all and similarity top-k versus exact all-pairs.

### F-007: Construction and publication must obey the same promise

Every artifact build must quote build peak, temporary runs, retained bytes,
old/new generation overlap, invalidation/freshness, and recovery publication.
Moving an unbounded projection or compaction peak outside the measured kernel
does not create a low-RAM system.

### F-008: The first slice should maximize customer and architecture learning

Bounded security/dependency reachability should be first because it matches the
best evidenced job. WCC should be second because partition equivalence is a
clean oracle and its `O(V)` state makes memory accounting legible. PageRank is a
recognizable third benchmark. NodeSimilarity is the first state-heavy
falsifier: it tests whether admission remains honest when candidate volume, not
input edges, is the dangerous dimension.

## Algorithm-Specific Decision Matrix

| Family | Preferred custom artifacts | Hard resident/state terms | Exact low-RAM action | Explicit alternative | Principal falsifier |
|---|---|---|---|---|---|
| Reachability/BFS | Typed source-range outgoing blocks; optional selective reverse tiles; optional SCC condensation | visited, sparse/dense frontier, optional distance/predecessor, bounded result | adaptive sparse/bitmap frontier; spill distance/predecessor and results | depth/result-cap plan only when changed semantics are declared | reachable set or output explodes; reverse/build cost exceeds reuse |
| WCC | canonical pair-once undirected edge runs; optional symmetric speed view; component/forest answer artifact | parent/label array and bounded edge buffers | stream edges over resident labels; external range merge only when proven | materialized labels for repeated immutable generation | label state itself cannot fit; deletion freshness requires rebuild |
| SCC/low-link | trimmed cyclic core, bounded DFS-frame tape, condensation/block-cut artifact | index/lowlink/component arrays, DFS frames/events | spill frames/events and process bounded core partitions | materialized condensation or block-cut answers | partition boundary loses low-link semantics |
| PageRank/HITS | destination-ordered pull blocks, outdegree/reciprocal column, dangling bitmap, optional prior vector | two rank vectors plus optional residual and output | resident vectors with streamed topology; 2-D strict tiles only with full traffic quote | reduced precision/early convergence as separate numeric plan; materialized ranks | strict tile rereads dominate; reduction order violates tolerance/determinism |
| Weighted paths | typed weight-band adjacency, delta buckets, optional landmarks/CH/DAG tape | distance/predecessor plus data-dependent priority/bucket state | bounded/spillable buckets and state slabs | landmarks/CH for stable road-like workloads | queue/output bound is not credible or index freshness dominates |
| Louvain/Leiden | weighted undirected blocks, external community-edge sort/reduce runs, one sealed quotient level at a time | community/degree vectors, dynamic tally arena, current/next level overlap | bounded tally arena with exact overflow spill and streamed contraction | capped/sample/early-stop only as approximate plan | exact spill schedule is too expensive; partition/numeric contract unstable |
| NodeSimilarity/kNN | exact entity-feature postings, rare-first prefix blocks, degree bands, fixed top-k output; optional sketches/ANN | candidate accumulators, postings, top-k heaps, sketches/index | sound upper-bound pruning, partitioned exact intersections, candidate spill | MinHash/LSH/ANN candidates plus exact rerank and recall receipt | dense/adversarial features defeat pruning; output cap does not bound candidates |
| Triangles/LCC | degree/degeneracy-oriented forward lists; adaptive array/bitmap hub intersections | intersection buffers and optional per-node counters | bounded intersections over one-count orientation; spill result counters | sampling as separately named estimator | hub bit pages or orientation build cost exceed saved work |
| Random walk/Node2Vec | counter-based RNG transducer and degree-adaptive exact sampler | in-flight batch and model matrices, not a completed walk matrix | regenerate deterministic walks and stream bounded batches | approximate/biased sampler only with declared distribution contract | trainer/model matrix dominates; sampler changes walk distribution |
| FastRP/HashGNN | normalized operator blocks, rolling/dimension slabs, bit-sliced hashes | one or more `V*D` matrices and output | dimension/vertex blocking with repeated topology passes and disk output | quantization/dimension reduction as approximate plan | cross-block dependencies make flushing invalid; output remains larger than budget |
| GraphSAGE | sampled feature pages and bounded batches | input/result features, model, batch concurrency | cap batch/concurrency and page features | quantized/sampled model contract | training semantics and feature state overwhelm first-product scope |
| Betweenness/closeness | source-lane schedule and deterministic recomputation tape | per-source predecessors/order/sigma/delta | budget source lanes; recompute predecessors to exchange RAM for scans | source sampling with error receipt | weighted ties/recomputation alter semantics or runtime becomes prohibitive |
| CELF/influence | streamed live-edge simulations; optional reverse-reachable sets | active sets, queues, simulations/RR sets | bounded simulation batches and spilled artifacts | RR-set sampling with confidence contract | probabilistic error or stored RR set exceeds quote |
| K-means | columnar feature batches and streaming sufficient statistics | centroids plus bounded feature batch and assignments/output | multiple deterministic scans with fixed batch | quantized features or approximate convergence | this is not graph-specific differentiation |

## Working-Set Model Audit

For every plan the admissible peak is:

```text
M_peak = M_resident_artifacts
       + M_algorithm_state
       + M_worker_scratch(concurrency)
       + M_frontier_or_dynamic_state
       + M_output_window
       + M_io_buffers
       + M_runtime_and_stacks
       + M_build_or_generation_overlap
       + M_safety_reserve
```

Disk and time are co-equal contracts:

```text
D_peak = retained_generations + build_runs + spill + output
T_run  = decode + edge/state work + reduce/sync + I/O + publication
T_e2e  = missing_artifact_build + queue + T_run
```

The quote SHALL label hard bytes, modeled ranges, calibrated expectation, and
the conservative refusal bound separately. It SHALL name cold versus warm
cache, topology residency, concurrency, result mode, exactness, snapshot,
artifact build inclusion, and safety margin.

## Contradictions And Missing Terms

| Corpus claim or temptation | Correction |
|---|---|
| “Top seven algorithms cover 80-90%.” | No representative public procedure-frequency telemetry in the corpus supports the percentage. Keep an ordinal hypothesis and collect workload manifests. |
| “Per-algorithm artifact makes the estimator exact.” | It makes artifact and fixed-slab bytes exact. Dynamic queues, candidates, tallies, iterations, cache residency, and output still need bounds/calibration. |
| “`mmap` means the graph uses almost no RAM.” | Mapped pages can become resident and evict other workloads. Strict mode needs fixed buffers/direct I/O and an external ceiling. |
| “Rust plus `io_uring` makes the same algorithm faster.” | Resident graph kernels are often memory-bandwidth, synchronization, state, or output bound. `io_uring` helps only when useful independent I/O exists. |
| “Compressed topology solves graph OOM.” | State-heavy families can be dominated by `V*D`, candidate, tally, predecessor, or result memory. |
| “O(V) state fits.” | Big-O omits width, multiplicity, concurrency, and output. `V*D` is O(V) for fixed D and can still be hundreds of GB. |
| “Bitmap frontier is always smaller.” | A dense bitmap is good for dense frontiers; sparse lists/runs win for sparse frontiers. The representation should switch without changing semantics. |
| “Lower precision preserves the same algorithm.” | `f32`, `f16`, `int8`, capped tallies, sampling, and early stop require an explicit equivalence or approximate contract. |
| “Top-k bounds NodeSimilarity.” | It bounds retained output, not the candidates considered to discover that output. |
| “Bloom/sketch rejection is exact.” | Only a one-sided, proven bound may skip exact work. Otherwise the plan is approximate. |
| “Streaming FastRP strata removes matrix memory.” | A stratum can be flushed only if future operator blocks no longer depend on it; cross-stratum propagation must be proved or accumulated. |
| “WCC incrementally handles updates.” | Inserts are easy to merge; arbitrary deletions require rebuild or a substantially more complex dynamic-connectivity structure. |
| “Receipt proves the budget.” | Reservations, bounded allocators, spill limits, cancellation, and a cgroup/RSS ceiling enforce it; the receipt records the outcome. |
| “50 GB on 16 GB proves the company.” | It proves a capacity milestone only. The job, workflow, repeat use, and willingness to pay remain founder falsifiers. |

## Code-Graph Cross-Checks

Agent 05 independently ran `@sdsrs/code-graph` version `0.114.1` against the
existing indexes in `neo4j-gds-src` and `neo4j-src`. No index was rebuilt. Both
deep health checks passed SQLite integrity checks with zero FTS drift and no
stale index schema:

| Repository | Indexed files | Symbols | Edges | Index size | Limitation |
|---|---:|---:|---:|---:|---|
| Neo4j GDS | 4,921 | 38,262 | 521,221 | 177,352,704 bytes | FTS-only; 15,756 unresolved calls |
| Neo4j core | 8,002 | 113,831 | 1,632,830 | 526,471,168 bytes | FTS-only; 36,076 unresolved calls |

The unresolved-call counts matter. Common names such as `add`, `of`, and
`memoryEstimation` produced implausible cross-family callers in some graph
results. Consequently, the findings below rely on the targeted source body
returned by `show`; call-graph fan-in numbers are not treated as proof.

### CG-001: GDS already estimates and guards execution

`DefaultMemoryGuard.assertAlgorithmCanRun` builds a `MemoryRequirement`, chooses
the configured maximum estimate, and calls `memoryTracker.tryToTrack`. It throws
before execution when required bytes exceed available bytes. A `sudo` branch can
bypass the guard, and an unimplemented estimator can cause the guard to be
skipped. Source:
`applications/algorithms/machinery/.../DefaultMemoryGuard.java:62-103`.

Decision impact: reject “Neo4j has no estimates” and “Neo4j has no admission
guard.” Knight Walker must differentiate through complete process-level
accounting, controlled spill/approximation choices, hard external enforcement,
and post-run reconciliation. It must also have no privileged path that silently
violates the declared budget.

### CG-002: PageRank delegates its state bill to Pregel

`PageRankMemoryEstimateDefinition.memoryEstimation` calls
`Pregel.memoryEstimation` with one `DOUBLE` PageRank property. Pregel then bills
vote bits per node, compute steps per thread, node values, and either message
queues or reducing message arrays. Sources:
`algo/.../PageRankMemoryEstimateDefinition.java:30-37` and
`pregel/.../Pregel.java:117-139`.

Decision impact: a PageRank comparison that counts only rank vectors or only
topology is invalid. The custom pull plan can remove Pregel-generic vote/message
machinery, but it still owes two dense vectors, reduction/order semantics,
dangling state, output, and topology traffic.

### CG-003: GDS BFS exposes why query scope is part of the format

`BfsMemoryEstimateDefinition` bills a visited bitset, traversed-node array,
weights, minimum-chunk state, data-dependent per-thread local nodes, chunks, and
result nodes. Its local-node upper bound depends on the smaller of the edge
upper bound and `concurrency * (V - 1)`. Source:
`algo/.../paths/traverse/BfsMemoryEstimateDefinition.java:34-70`.

Decision impact: Knight Walker should not clone this broad state contract for a
bounded access-path query. A source-target boolean, a bounded proof path, and a
stream-all traversal need separate manifests and output bills. This is the
strongest source-backed reason to make the first slice query-shaped.

### CG-004: WCC has a legible state floor

`WccMemoryEstimateDefinition` consists of a `HugeAtomicDisjointSetStruct`, with
an incremental variant selected by configuration. Source:
`algo/.../wcc/WccMemoryEstimateDefinition.java:35-41`.

Decision impact: WCC is a good second slice because the dense disjoint-set state
is explicit, partition equivalence is easy to oracle, and a streamed edge pass
can test whether strict-mode accounting is honest. The state does not disappear;
arbitrary deletions still require rebuild or a separate dynamic-connectivity
contract.

### CG-005: Path memory is not “just a queue”

`DijkstraMemoryEstimateDefinition` bills a huge priority queue, reverse-path
map, optional relationship-ID map, optional targets bitset, and visited bitset.
Source: `algo/.../paths/dijkstra/DijkstraMemoryEstimateDefinition.java:37-54`.

Decision impact: target count, relationship-path output, and proof-path mode are
material plan dimensions. An honest bounded path engine must cap or spill its
queue and predecessor state, not merely stream adjacency.

### CG-006: Top-k similarity does not cap candidate work

The NodeSimilarity estimator bills node filters, materialized vectors and
optional weights, optional WCC/component state, and a similarity graph, top-k
map, or top-n list according to result mode. More importantly,
`NodeSimilarity.computeTopKMap` iterates source/target pairs; its parallel path
comments that it deliberately computes the full matrix except the diagonal.
Sources: `algo/.../nodesim/NodeSimilarityMemoryEstimateDefinition.java:45-108`
and `algo/.../nodesim/NodeSimilarity.java:295-321`.

Decision impact: a fixed top-k heap bounds retained answers, not comparisons.
The exact low-RAM experiment must use proven upper-bound pruning and spillable
candidate partitions; the approximate experiment must report recall and exact
rerank behavior.

### CG-007: Louvain materializes changing graph and hierarchy state

`LouvainMemoryEstimateDefinition` includes modularity-optimization state, a
range for a newly created aggregate graph, and one or multiple dense dendrogram
arrays depending on `includeIntermediateCommunities` and `maxLevels`. The source
calls the aggregate graph size a “rough estimate.” Source:
`algo/.../louvain/LouvainMemoryEstimateDefinition.java:48-90`.

Decision impact: streamed contraction must explicitly budget current/next
levels, external sort/reduce runs, tally overflow, and dendrogram/result mode.
“One graph at a time” is not enough if publication or hierarchy output keeps
both generations resident.

### CG-008: FastRP is a state-heavy falsifier

`FastRPMemoryEstimateDefinition` bills fixed property vectors plus three
per-node embedding matrices: `embeddings`, `embeddingsA`, and `embeddingsB`,
each sized by the configured embedding dimension. Source:
`algo/.../embeddings/fastrp/FastRPMemoryEstimateDefinition.java:36-50`.

Decision impact: FastRP is dominated by `V * D` state and output, not merely
graph topology. Dimension blocking or reduced precision must prove dependency
and numerical semantics. It should not be promised in the first product slice.

### CG-009: Neo4j core reinforces the process-scope distinction

`TransactionMemoryPool` reserves and releases heap/native memory against a
delegate pool, throws on a configured transaction limit, creates local/execution
trackers, and records a high-water mark. Separately,
`ConfiguringPageCacheFactory.getPageCacheMaxMemory` reads an explicit page-cache
setting or computes a heuristic. Sources:
`community/kernel/.../TransactionMemoryPool.java:42-180` and
`community/kernel/.../ConfiguringPageCacheFactory.java:136-148`.

Decision impact: Neo4j already contains meaningful internal memory controls,
but a transaction tracker is not a complete analytical process ceiling. Knight
Walker should measure and enforce RSS/cgroup usage including allocator, native
buffers, page residency, output, and build overlap. It should not claim that
moving from JVM to Rust alone creates this property.

## Founder Falsifier Register

| Falsifier | Fastest decisive test | Kill or pivot condition |
|---|---|---|
| The pain is ingestion, schema, UI, or permissions rather than compute | Ten artifact-owning security/dependency interviews; request the exact failed job and workaround | Fewer than three can supply an artifact plus a compute decision changed by memory uncertainty |
| Existing systems already solve the job | Run tuned GDS, Ladybug/Kuzu successor, Slater/Grafeo where applicable, and Knight Walker under the same machine and output contract | Incumbent meets the ceiling and total time-to-answer with no meaningful ceremony or trust deficit |
| Refusal is not useful | Present a conservative refusal before the customer's current run | Users prefer an attempted late failure and do not change provisioning, schedule, or workflow |
| The receipt is theatre | Show estimate and actual-error receipts over repeated runs | Receipt does not change rerun behavior, auditability, or willingness to adopt/pay |
| Memory is merely displaced | Run under cgroup/RSS ceiling with cold-cache accounting, builder overlap, page residency, output, and retained generations | The engine exceeds the ceiling or depends on uncontrolled cache growth |
| Strict mode is operationally useless | Force spill on the real access-path job and compare end-to-end completion deadline | It fits but misses the decision deadline by enough that buying RAM is clearly preferable |
| The first ICP is too narrow | Repeat artifact/job qualification across security, SBOM, IAM, and dependency teams | No repeatable job/schema/adapter pattern appears after ten qualified conversations |
| Per-algorithm formats do not repay complexity | Compare canonical-only control with one custom artifact including build, disk, invalidation, and run costs | Savings do not repay build/retention complexity at observed rerun cadence |
| The 50 GB on 16 GB demonstration is synthetic | Require a named customer's artifact and output contract | Benchmark cannot be mapped to a real blocked or overprovisioned decision |
| Exactness blocks useful low-RAM plans | Compare exact strict plan with explicit approximate plan and user tolerance | Exact is unusable and customers reject the approximation/error receipt |

## Choose / Experiment / Reject / Defer

### Choose now

| Decision | Why it survives the corpus |
|---|---|
| `OPT-CONTRACT-01` | The only defensible differentiation is estimate plus admission choice plus enforcement plus reconciliation. |
| `OPT-CANONICAL-01` + `OPT-PORTFOLIO-01` | A small canonical truth makes rebuild and differential verification possible; derived artifacts earn residency per algorithm/query family. |
| `OPT-STRICT-01` and `OPT-FAST-01` as separate profiles | A warm resident run and a hard-ceiling external-memory run have different goals and must never share an ambiguous performance claim. |
| `OPT-STATE-CAPSULE-01` + `OPT-OUTPUT-BOUND-01` | Algorithm state and output are often larger or less predictable than topology. Both need reserved/capped paths. |
| `OPT-BUILD-BUDGET-01` | Unbounded conversion, sort, publication, or old/new generation overlap would falsify the low-RAM claim. |
| `OPT-SELECTIVE-TRANSPOSE-01` | Direction-specific layouts should exist only when the selected algorithm/query reuses them enough to repay build and disk. |
| First slice: bounded access/dependency path | It matches the binding ICP and permits narrow boolean/proof-path/output contracts. |
| Second slice: WCC | It supplies a clean partition oracle and a visible dense-state floor. |
| One `OPT-ADAPTER-01` | Choose the first design partner's real artifact format; avoid a broad ingestion platform. |
| Narrow `OPT-COMPAT-01` | Support only the read-only query/procedure shape needed to adopt the first slice; preserve Neo4j/GDS as an oracle. |

### Experiment before promising

| Option | Required comparison |
|---|---|
| `OPT-ADAPTIVE-ROW-01` | Inline/list/bitmap/hub rows versus a plain CSR control, including build bytes, branch/decode cost, skew, and cold I/O. |
| `OPT-HOT-COLD-01` | Stable hotness or degree bands versus uniform pages under representative query sources; no Zipf assumption without measurement. |
| `OPT-RECOMPUTE-01` | Saved resident bytes versus extra deterministic scans and deadline impact. |
| `OPT-QUOTIENT-01` / `OPT-ANSWER-01` | Repeated-query savings versus build, freshness, invalidation, and artifact proliferation. |
| PageRank destination-pull strict tiles | Full vector/topology/output traffic, deterministic reduction tolerance, and cold/warm time under a hard ceiling. |
| Exact NodeSimilarity rare-first/pruned plan | Adversarial dense features, candidate spill, proof that pruning is sound, and top-k parity. |
| Streamed Louvain contraction | Current/next graph overlap, exact tally overflow, hierarchy output, and deterministic/numerical contract. |
| Counter-based Node2Vec | Distribution parity with the chosen sampler and proof that completed walks are never required downstream. |

### Reject

| Rejected option or claim | Reason |
|---|---|
| `OPT-DB-REWRITE-00` | A full Neo4j rewrite consumes years of surface-area work and contradicts the binding OLAP product boundary. |
| `OPT-GENERIC-CSR-01` as the sole format | Useful control and interchange substrate, but it cannot optimize all direction, state, precision, output, and cadence shapes. |
| “`mmap` is the memory ceiling” | Page residency is not a hard budget. |
| “Rust/`io_uring` makes unchanged kernels faster” | Many graph kernels are bandwidth, state, synchronization, or output bound; evidence must be measured per plan. |
| Silent sampling, precision reduction, caps, or early stop | These change the result contract and undermine verification-first adoption. |
| “Top seven are 80-90% of use” | No representative procedure-frequency telemetry in the corpus supports this number. |
| “The manifest makes every estimate exact” | Data-dependent queues, candidates, tallies, iterations, cache effects, and output remain modeled or bounded. |
| Artifact-per-parameter sweep | Build, disk, invalidation, and operational state explode faster than demonstrated reuse. |
| In-process Neo4j plugin as the core | It reintroduces OLTP blast radius, licensing/integration risk, and a database-shaped roadmap; at most retain a thin exporter/adapter if demanded. |

### Defer

| Deferred scope | Trigger to revisit |
|---|---|
| Broad Bolt/Cypher/APOC and all GDS modes | A paid/design-partner workflow requires a captured unsupported query after the first slice works. |
| `OPT-APPROX-01` and `OPT-PRECISION-01` | Exact traversal/WCC proof exists and a customer supplies an explicit quality tolerance. |
| PageRank product support | The first two slices pass correctness, ceiling, builder, receipt, and customer-value gates. |
| NodeSimilarity, Louvain/Leiden, triangles, FastRP | Each predecessor family earns the next experiment; NodeSimilarity remains the first dangerous-state falsifier. |
| Distributed execution and custom OS work | A single-node SSD runner proves product demand and profiling shows an OS/distribution bottleneck. |
| Dynamic low-link/connectivity maintenance | Rebuild cadence becomes a measured customer blocker. |
| General GraphRAG/agent-memory platform | A real workload exceeds RAM and values this contract rather than generic memory/database features. |

## Final Validation Receipt

The companion TSV was generated from the frozen denominator in denominator
order. The post-generation validator returned:

| Check | Result |
|---|---|
| Frozen expected rows | 68 |
| Agent 05 evidence rows | 68 |
| Current source files rehashed/recounted | 68/68 match |
| Preserved identity fields | `lane`, `path`, `sha256`, `bytes`, `extension`: exact |
| Missing / extra / reordered paths | 0 / 0 / 0 |
| Unique evidence IDs | 68/68, each matching `A05-######` |
| Illegal coverage statuses | 0 |
| Undefined architecture option IDs | 0 |
| Output paths present in denominator | 0 |
| Denominator gaps | 0 |

Coverage totals are `semantic_read=56`, `structured_queried=6`, and
`superseded_classified=6`. The permitted but unused statuses
`generated_classified` and `binary_inspected` were not applied merely to fill a
category.

Final authority is the machine check, not the prose: **PASS, zero gaps**.

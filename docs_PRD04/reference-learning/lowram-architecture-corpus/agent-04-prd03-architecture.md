# Agent 04: PRD03 Low-RAM Architecture Evidence Dossier

## Executive decision

The PRD03 corpus supports a narrower and stronger architecture than a Neo4j rewrite:

> Build an immutable graph artifact runner whose physical plan is selected per workload, whose total working set is declared before admission, and whose execution ends with a receipt comparing the declared and measured RAM, I/O, latency, and result.

This is an OLAP compute product, not a database. Neo4j and GDS are the compatibility oracle, source of behavioral fixtures, and incumbent baseline. They are not the architecture to translate.

The durable substrate should be small and shared:

1. one canonical immutable topology artifact, normally forward and reverse CSR;
2. typed property sidecars loaded only when a plan requires them;
3. immutable result and model artifacts;
4. an atomic generation catalog for publication, rollback, and reader pinning;
5. a strict executor with explicit memory pools, bounded concurrency, spill paths, and deterministic refusal;
6. algorithm-specific physical plans and workspaces, not a different durable database for every algorithm.

The first founder wedge should be bounded security, IAM, dependency, SBOM, and access-path analysis. BFS/shortest path and WCC are the safest first kernels. PageRank and triangle counting are the next proof slices. Similarity, Louvain/Leiden, and embeddings are valuable differentiators only after the budget contract is proven on simpler families.

The strongest differentiation is not "uses less RAM." It is:

> For a named artifact, algorithm, exactness profile, and hard budget, the runner says `fit`, `spill`, `approximate`, or `refuse`; enforces that decision; and returns a proof receipt.

This conclusion follows the binding founder brief in `docs_PRD04/A007-spc-founder-interview-prep-v7.md`. All PRD03 claims were re-evaluated against that boundary.

## Scope and evidence method

The authoritative denominator was:

`docs_PRD04/reference-learning/lowram-architecture-corpus/evidence/all-documents-denominator.tsv`

Only rows with `assigned_agent=agent-04` were used. The denominator contributed exactly 222 files under `docs_PRD03/`. The companion ledger assigns one evidence ID and one honest coverage status to every row.

| Evidence class | Files | Treatment |
| --- | ---: | --- |
| Hand-authored Markdown outside generated dossiers | 37 | Read completely and marked `semantic_read`; obsolete goal meaning remains explicit in `file_class` |
| Generated GDS dossiers, including `ROLLUP.md` | 112 | All schema-classified; 52 high-relevance files individually queried and marked `structured_queried`; 60 lower-relevance files remain `generated_classified` |
| TSV artifacts | 29 | Header, row count, field count, structural consistency, and decision-relevant rows queried |
| Clarity DOT graphs | 20 | Node, edge, and cycle counts queried; treated as generated file-dependency evidence |
| Clarity stderr files | 20 | Byte/line state inspected; 19 empty and one openCypher unsupported-language diagnostic |
| Python evidence generators | 3 | Imports, constants, functions, inputs, outputs, and generation behavior inspected structurally |
| SQLite graph | 1 | Schema, indexes, views, counts, attack lanes, and selected neighborhoods queried |
| **Total** | **222** | **Zero denominator gaps** |

Final coverage statuses after the targeted parent-validator correction are exact: 37 `semantic_read`, 84 `structured_queried`, 100 `generated_classified`, and 1 `binary_inspected`. There are no `superseded_classified` rows. The eight fully read obsolete research/rewrite contracts remain identifiable through `file_class=superseded_rewrite_or_research_contract`.

The 52 promoted generated dossiers were not promoted by relevance alone. Each was individually queried for document length, heading structure, source/evidence anchors, architecture or verification sections, oracle markers, and tabular content. All 52 passed the structural-evidence gate. This supports `structured_queried`, not `semantic_read`: their generated provenance remains material.

This corpus contains evidence at different strengths. A generated table does not become direct source evidence merely because it is structured. A source dossier does not make a benchmark claim measured. A memory estimate does not establish a hard RSS bound. Those distinctions are preserved below.

## Corpus facts that survive A007

### 1. The artifact is the product boundary

PRD03 repeatedly converges on immutable topology, typed sidecars, named generations, validation before publication, and reader pinning. Those concepts survive A007 because they are properties of a bounded analytical artifact, not requirements for an OLTP database.

The minimum artifact identity should include:

- `artifact_id`
- `generation_id`
- source watermark or snapshot identity
- node and relationship counts
- ID width and offset width
- orientation and relationship-type semantics
- property-sidecar manifest
- sorted-neighborhood guarantee
- checksums for every payload
- build receipt and format version

Relevant corpus anchors include the projection/build contract, publication state machine, Batch 03, Batch 04, and the architecture scorecard (A04-000008, A04-000010, A04-000017, A04-000018, A04-000014).

### 2. A small substrate supports many graph families

The evidence does not justify a durable topology format per algorithm. It supports a canonical adjacency representation plus per-plan logical views and temporary workspaces:

- forward CSR for source-oriented scans and traversals;
- reverse CSR where pull-style scans or inbound traversal justify its bytes;
- sorted adjacency for intersection and deterministic iteration;
- logical undirected orientation when semantics permit it;
- typed node/relationship columns for weights, seeds, filters, and features;
- bounded result/model artifacts for outputs;
- runtime contracted graphs for Louvain/Leiden, never silently retained as another serving graph.

This conclusion is supported by the low-RAM priors, hard-family study, support-tier matrix, and procedure-to-kernel ledger (A04-000021, A04-000022, A04-000030, A04-000038). It is also source-cross-checked below.

### 3. Memory is a compositional contract

The useful GDS idea is the estimator tree: graph terms, algorithm state, result terms, model terms, write terms, and build high water can be composed. Knight Bus should make this stricter than GDS:

```text
declared_peak =
    resident_control_bytes
  + topology_window_bytes
  + property_window_bytes
  + algorithm_workspace_bytes
  + result_writer_bytes
  + spill_buffer_bytes
  + allocator_and_runtime_margin_bytes
```

Admission must compare the worst simultaneous phase, not the sum of unrelated lifetime allocations and not just final graph size. Every term must name an owner, lifetime, accounting mechanism, and failure behavior.

The current GDS guard is evidence for both the pattern and the gap: `DefaultMemoryGuard.assertAlgorithmCanRun` computes a requirement and reserves it, but catches `MemoryEstimationNotImplementedException` and skips the guard. A007 requires the opposite default: an unestimated plan is unsupported and must refuse before execution.

### 4. Build and publication are separate from serving

The Projection Build Store is a foundry, not a query engine. It may use an append-only receipt log, transactional metadata, bounded external sort, merge runs, dictionaries, and validation scratch. Published execution artifacts remain immutable. The build plane can be restarted and replayed without exposing mutable intermediate state to analytical readers.

The strongest PRD03 option is an append log plus a small transactional metadata/fact store, followed by atomic generation publication. A full LSM/delta serving layer is explicitly counter to the founder brief because it recreates a database and makes query-time memory less predictable.

### 5. Compatibility is an adapter and oracle

The product does not need to implement Bolt, Cypher, transactions, drivers, locks, recovery, or Neo4j storage. A narrow adapter may accept a useful read-only subset of Neo4j/GDS-shaped calls, resolve a named artifact, compile a bounded plan, and stream a Neo4j-shaped result.

The initial compatibility boundary should be limited to:

- selected `CALL gds.<family>.<mode>` names;
- graph/artifact name and supported configuration keys;
- `YIELD` column names and deterministic ordering rules where promised;
- estimate/explain output for supported plans;
- stream results and immutable result-artifact references;
- explicit unsupported errors for mutate/write/transactional behavior not implemented.

The canary matrix and Batch 05 remain valuable verification sources (A04-000006, A04-000019). The OLTP Rust record-store contract (A04-000007) is superseded as a product objective.

## Architecture option catalog

These option IDs are used in the evidence ledger.

| ID | Option | Role | A007 disposition |
| --- | --- | --- | --- |
| A04-OPT-01 | Proof-carrying graph artifact | Manifest, checksums, scale facts, generation identity, build receipt | Choose |
| A04-OPT-02 | Canonical dual CSR topology | Shared immutable forward/reverse adjacency with sorted neighbors | Choose, with reverse adjacency optional by plan |
| A04-OPT-03 | Typed sidecar property plane | Fixed-width hot columns and compressed cold columns | Choose |
| A04-OPT-04 | Strict budget admission | Compositional worst-phase estimate and `fit/spill/approximate/refuse` result | Choose |
| A04-OPT-05 | Explicit-I/O window executor | Fixed buffer pool, deterministic partitions, bounded concurrency, explicit eviction | Choose for strict mode |
| A04-OPT-06 | Frontier traversal plan | Bounded frontier spool, visited bitmap, optional parent/depth sidecars | Choose first |
| A04-OPT-07 | Compact connectivity plan | Packed parent/rank or component arrays with deterministic union scheduling | Choose first |
| A04-OPT-08 | Tiled PageRank plan | Pull scan over reverse CSR, fixed rank vectors, deterministic reductions | Experiment after traversal |
| A04-OPT-09 | Degree-oriented triangle plan | Sorted intersections, degree ordering, max-degree gate, optional global-only result | Experiment after PageRank |
| A04-OPT-10 | Candidate-bounded similarity plan | Property-first blocks, candidate cap, top-K heaps, spill/approximation profiles | Experiment |
| A04-OPT-11 | Contracted community plan | Runtime level artifacts, bounded contraction, per-level receipt, spill/refusal | Experiment |
| A04-OPT-12 | Dimension-gated embedding plan | Streamed feature/walk batches, explicit embedding precision, model artifact | Defer/experiment |
| A04-OPT-13 | Atomic generation publication | Stage, validate, seal, atomically activate, pin, retire | Choose |
| A04-OPT-14 | Read-only Neo4j/GDS adapter | Narrow procedure-shaped compatibility without database semantics | Choose after one native slice |
| A04-OPT-15 | Cellular locality packaging | Optional partition/cell layout only after measured locality wins | Experiment, not default |
| A04-OPT-16 | Bounded build foundry | External sort, merge, dictionaries, validation, reproducible publication | Choose |
| A04-OPT-17 | Result/model artifact plane | Immutable outputs with lifecycle metadata and generation pin | Choose minimally; expand as needed |
| A04-OPT-18 | Selective GraphBLAS kernels | Sparse algebra only where it beats native traversal under the same contract | Experiment |

## Proposed physical architecture

```text
Neo4j export / edge files / property files
                    |
                    v
      +-----------------------------+
      | A04-OPT-16 Build foundry    |
      | bounded sort + merge        |
      | ID map + validation         |
      +-------------+---------------+
                    |
                    v
      +-----------------------------+
      | A04-OPT-01 Artifact         |
      | manifest + checksums        |
      | CSR + sidecars + statistics |
      +-------------+---------------+
                    |
             atomic publish
                    |
                    v
      +-----------------------------+
      | A04-OPT-13 Catalog          |
      | active generation + pins    |
      +-------------+---------------+
                    |
        estimate -> admit -> execute
                    |
                    v
      +-----------------------------+
      | A04-OPT-04/05 Executor      |
      | fixed pools + phase ledger  |
      | fast / strict / approximate |
      +-------------+---------------+
                    |
                    v
      result stream + artifact + proof receipt
```

### Topology bytes

For `V` nodes and `E` directed relationships, an illustrative u64-offset/u32-target CSR uses approximately:

```text
one_direction ~= 8 * (V + 1) + 4 * E + metadata
dual_direction ~= 16 * (V + 1) + 8 * E + metadata
```

If node IDs require u64 targets, the target terms double. Properties, labels, type indexes, alignment, checksums, and build scratch are separate. These formulas are design accounting identities, not measured compression claims.

Reverse CSR should be an artifact capability selected from workload evidence, not a universal tax. The manifest can declare forward-only, reverse-only, or dual topology. A security access-path artifact may value both directions; a one-way batch may not.

### Sidecar policy

Each sidecar declares:

- entity domain: node or relationship;
- logical type and physical width;
- null/default encoding;
- dense, sparse, dictionary, run-length, or bit-packed representation;
- page/block index;
- checksum and generation;
- decompression scratch upper bound;
- whether random access is guaranteed.

Hot fixed-width columns should be directly addressable. Cold or scan-oriented columns may be Parquet-like/compressed, but decoding buffers must appear in the plan. A columnar file is not low-RAM if the operator materializes it into an unbounded heap array.

### Fast and strict execution modes

`fast` mode may use mmap and the OS page cache for low latency. It reports RSS and page-cache observations but does not promise a hard resident bound.

`strict` mode uses an explicit buffer pool, bounded queues, fixed concurrency, and a declared spill directory. On Linux, direct I/O or explicit read windows can reduce hidden page-cache residency; `io_uring` may improve submission overhead and overlap, but it does not reduce algorithmic state. Every strict-mode allocation is charged to an owner pool.

Both modes execute the same semantic plan and oracle. They differ only in physical scheduling, I/O, precision profile, and admitted resource envelope.

## Algorithm-by-algorithm plans

The byte formulas below describe candidate Knight Bus plans. They are not benchmark results. `W` is a configured window/frontier bound, `K` is top-K, `D` is embedding dimension, and `C` is a candidate cap.

| Workload | Artifact/view | Dominant workspace | Candidate low-RAM plan | Latency and predictability | Founder fit | Confidence |
| --- | --- | --- | --- | --- | --- | --- |
| BFS / reachability | Forward CSR; reverse optional | visited `V/8`; frontier `W*id`; optional parent/depth `4V-12V` | A04-OPT-06 spools overflow frontier partitions and omits parent/depth unless requested | Usually one or few edge scans; strict cap is straightforward; spill adds extra passes | Excellent for access paths, dependency impact, attack paths | High |
| Unweighted shortest path | Forward CSR | visited, frontier, distance, optional parent | Same frontier engine with depth barriers and deterministic parent tie-break | Predictable for bounded depth; worst-case full scan is declared | Excellent | High |
| Weighted SSSP | Weighted sidecar + CSR | distance array, visited state, bounded bucket/heap | Delta buckets or external priority runs selected by weight domain | More I/O-sensitive; exact bounded mode may be materially slower | Strong for risk/cost paths | Medium |
| WCC | Logical undirected view | parent/component `4V-8V`; optional rank/seed | A04-OPT-07 packed union-find or label-min iterations with deterministic partitions | Near-linear; no alternate durable topology required | Strong for identity islands and dependency groups | High |
| PageRank | Reverse CSR preferred; weight/outdegree sidecars optional | two rank vectors `16V` for f64 plus degree/state | A04-OPT-08 tiled pull scan; fixed reduction order; f32 is an explicit approximate profile | In-memory fast path can be fast; strict path is bandwidth-bound and repeatable | Medium as first wedge, strong proof of iterative scans | High for shape, medium for delta |
| Triangle count | Sorted logical undirected adjacency | per-node count `8V` only if requested; small intersection state | A04-OPT-09 degree-oriented two-pointer intersections; global-only output removes `8V` | Skew drives tail latency; max-degree gate makes risk explicit | Useful for fraud/cohesion, not first wedge | High |
| NodeSimilarity | CSR plus optional weights/components | vectors plus top-K/candidates, potentially superlinear | A04-OPT-10 block candidates, cap `C`, spill buckets, preserve top-K only | Exact broad comparisons can be slow; cap creates predictable profile | Potentially valuable, but risky first proof | High for risk, medium for plan |
| KNN / filtered KNN | Typed property plane; topology secondary | property vectors and roughly `O(V*K)` neighbors | Property-block scan, bounded candidate generator, filter pushdown, spillable top-K | Strongly data- and dimension-dependent; exactness must be explicit | Valuable after property plane exists | High for risk, medium for plan |
| Louvain | Logical undirected CSR plus weights/seeds | communities, volumes, dendrogram, contracted graph per level | A04-OPT-11 seals each level as a temporary artifact; bound one current and one next level | More passes and I/O, but phase peaks become measurable | High-value algorithm, poor first kernel | High for workspace risk |
| Leiden | Undirected CSR plus weights/seeds | Louvain state plus refinement and post-aggregation arrays | Same level artifact model with stricter per-phase admission | Highest community-family complexity; refuse when phase proof is absent | Defer until Louvain proof | High |
| FastRP | CSR plus feature/weight sidecars | GDS evidence shows three `V*D` float embedding arrays | A04-OPT-12 two-buffer ping-pong where semantics allow; mmap/spill model; f16 only as approximate | Memory scales directly with `V*D`; dimension gate is highly predictable | Good later demonstration | High for incumbent shape, medium for optimization |
| Node2Vec | CSR plus optional weights | walks, RNG state, center/context embeddings | Generate bounded walk batches into training; never retain complete corpus unless budgeted | Extra I/O/training time can be large; deterministic seed/profile required | Defer | Medium |
| GraphSAGE / ML pipelines | Feature sidecars and model plane | sampled neighborhoods, batches, weights, optimizer state | Bounded batches and immutable model artifacts | Large scope beyond graph artifact proof | Defer | Medium-low |

### Source-grounded deltas worth testing

The source cross-check gives concrete incumbent state shapes:

- GDS BFS estimates a visited bitset, traversed-node array, weights array, minimum-chunk array, local per-thread nodes, chunks, and result nodes. A result-specific Knight Bus plan can omit or compress structures that the requested output does not need.
- GDS WCC estimates a `HugeAtomicDisjointSetStruct`. A u32 ID artifact permits a packed parent representation and a separately bounded rank/size representation.
- GDS PageRank delegates to Pregel memory estimation with a per-node double value. The Knight Bus experiment should compare pull-style fixed vectors against that oracle, not assume Rust itself creates a win.
- GDS NodeSimilarity explicitly accounts for vectors, optional weights, component arrays/WCC, a similarity graph, top-K map, and top-N list. The risk is candidate/result state, not CSR alone.
- GDS Louvain estimates modularity state, a newly built undirected aggregated CSR graph, and one or more dendrogram arrays. The temporary contracted graph is the memory architecture problem.
- GDS Leiden adds four major per-node arrays, seeded communities, local move, modularity, dendrogram, refinement, aggregation, and four post-aggregation arrays. It should not be admitted under a generic "community" estimate.
- GDS FastRP estimates a property vector and three per-node float embedding arrays. Dimension and precision must be first-class plan inputs.
- GDS triangle counting's estimator exposes the per-node result array; degree-skew and adjacency-read behavior still require measured phase telemetry.

## The admission and receipt contract

### Planner outcome

Every supported invocation returns one of:

| Outcome | Meaning |
| --- | --- |
| `fit` | Exact plan fits within the hard budget with the declared concurrency |
| `spill` | Exact plan fits only with named spill phases and an explicit disk/I/O bound |
| `approximate` | Only a named error-bounded or precision-reduced plan fits; caller must opt in |
| `refuse` | No implemented plan can satisfy the requested semantics and resource envelope |

There is no "run and hope" outcome. An absent estimator is `refuse`, not a guard bypass.

### Pre-run explain record

```text
artifact_id, generation_id, algorithm, mode, exactness_profile
V, E, property_bytes, id_width, orientation, sorted_neighbors
hard_ram_bytes, spill_bytes, concurrency, partition_count
phase_estimates[] {name, resident, scratch, io_read, io_write}
peak_phase, estimated_peak_bytes, safety_margin_bytes
decision, refusal_code, plan_hash
```

### Post-run proof receipt

```text
plan_hash, artifact_checksums, implementation_version
started_at, completed_at, outcome
measured_peak_rss, measured_pool_high_water, measured_spill_bytes
bytes_read, bytes_written, major_faults, wall_time, cpu_time
phase_measurements[], result_checksum, oracle_status
estimate_error_bytes, estimate_error_percent
```

OS RSS alone is not enough. The receipt should distinguish explicit pools, allocator overhead, file mappings, and observed page-cache effects where the platform exposes them.

## Verification spine

Verification should be built before breadth:

1. **Semantic oracle:** run tiny and adversarial fixtures against the first-party GDS tests/procedures and compare normalized outputs.
2. **Artifact oracle:** validate counts, sortedness, orientation, checksums, ID mapping, and round-trip generation identity.
3. **Estimate oracle:** compare the plan formula with instrumented pool high water and process metrics; fail on underestimation beyond policy.
4. **Budget oracle:** test one byte below and one byte above admission thresholds; verify no phase exceeds the admitted pool.
5. **Failure oracle:** corrupt artifacts, missing sidecars, insufficient disk, cancellation, and unsupported modes must return stable errors.
6. **Determinism oracle:** fixed artifact, seed, plan hash, concurrency, and precision profile must produce the promised parity class.
7. **Baseline oracle:** run the same workload against Neo4j/GDS and report correctness, peak memory scope, latency distribution, and preparation time separately.

Parity classes from Batch 11 remain useful:

- exact set/value parity for BFS, WCC normalization, and triangle fixtures;
- numeric tolerance parity for PageRank and floating-point reductions;
- partition parity for Louvain/Leiden because raw community IDs may differ;
- estimate parity as a shape check, not a requirement to copy GDS byte counts.

The first proof-carrying slice should be:

```text
Neo4j-shaped request
  -> read-only adapter
  -> artifact selection
  -> BFS/shortest-path plan
  -> strict 5 GB and 10 GB budget trials
  -> GDS/tiny-fixture parity
  -> proof receipt
```

WCC should reuse the same artifact and executor next. PageRank then proves iterative full-edge scans without changing the product boundary.

## Code-graph 0.114.1 cross-check

The required tool version was invoked through `npx` because no global `code-graph` executable was installed:

```text
npx --yes @sdsrs/code-graph@0.114.1 --version
```

Result: `code-graph-mcp 0.114.1`.

### Index health

Commands:

```text
cd "gitrefrepo/Neo4j family/neo4j-gds-src"
npx --yes @sdsrs/code-graph@0.114.1 health-check --json

cd "gitrefrepo/Neo4j family/neo4j-src"
npx --yes @sdsrs/code-graph@0.114.1 health-check --json
```

Findings:

- GDS index: healthy, 4,921 files, 38,262 nodes, 521,221 edges, no FTS drift, schema version 10.
- Neo4j index: healthy, 8,002 parser-supported files, 113,831 nodes, 1,632,830 edges, no FTS drift, schema version 10.
- Both searches were FTS-only because embeddings were pending. Findings below are structural/source evidence, not semantic-vector results.

### Memory enforcement

Commands:

```text
npx --yes @sdsrs/code-graph@0.114.1 search "MemoryEstimations memory estimate guard" --limit 20 --json
npx --yes @sdsrs/code-graph@0.114.1 show "DefaultMemoryGuard.assertAlgorithmCanRun" --context-lines 8 --refs --json
```

Finding: the GDS guard builds `MemoryRequirement`, reserves via `MemoryTracker`, and rejects over-availability requests, but catches `MemoryEstimationNotImplementedException` and logs that the guard is skipped. This directly validates A04-OPT-04 while distinguishing it from incumbent best-effort behavior.

### Canonical topology and build high water

Commands:

```text
npx --yes @sdsrs/code-graph@0.114.1 search "CSRGraphStoreFactory adjacency list build" --limit 20 --json
npx --yes @sdsrs/code-graph@0.114.1 show "CSRGraphStoreFactory.getMemoryEstimation" --context-lines 5 --refs --json
```

Finding: GDS composes node-ID, node-property, relationship, loading, and after-loading estimates. During load it chooses a maximum over loading and after-loading relationship phases. The same factory estimate is called by native/cypher projection and by Louvain/Leiden contracted-graph work. This supports phase high-water accounting and shows why community contraction cannot be hidden inside a generic algorithm term.

### Algorithm state

Commands:

```text
npx --yes @sdsrs/code-graph@0.114.1 show "BfsMemoryEstimateDefinition.memoryEstimation" --context-lines 4 --json
npx --yes @sdsrs/code-graph@0.114.1 show "WccMemoryEstimateDefinition.memoryEstimation" --context-lines 4 --json
npx --yes @sdsrs/code-graph@0.114.1 show "PageRankMemoryEstimateDefinition.memoryEstimation" --context-lines 6 --refs --json
npx --yes @sdsrs/code-graph@0.114.1 show "NodeSimilarityMemoryEstimateDefinition.memoryEstimation" --context-lines 4 --json
npx --yes @sdsrs/code-graph@0.114.1 show "LouvainMemoryEstimateDefinition.memoryEstimation" --context-lines 4 --json
npx --yes @sdsrs/code-graph@0.114.1 show "LeidenMemoryEstimateDefinition.memoryEstimation" --context-lines 4 --json
npx --yes @sdsrs/code-graph@0.114.1 show "FastRPMemoryEstimateDefinition" --context-lines 3 --json
npx --yes @sdsrs/code-graph@0.114.1 show "IntersectingTriangleCountMemoryEstimateDefinition.memoryEstimation" --context-lines 4 --json
```

Findings are summarized in the algorithm table and source-grounded delta list. They validate different workspace shapes per family while preserving a common topology substrate.

### Why full Neo4j compatibility is not one seam

Commands:

```text
cd "gitrefrepo/Neo4j family/neo4j-src"
npx --yes @sdsrs/code-graph@0.114.1 search "Bolt protocol state machine request message reader" --limit 20 --json
npx --yes @sdsrs/code-graph@0.114.1 search "CALL procedure Cypher procedure invocation runtime" --limit 25 --json
```

Finding: Bolt request/state-machine types, Cypher runtime `DbAccess`, kernel execution context, and global procedure registration are distinct subsystems. Reimplementing them would turn the artifact runner into a database rewrite. A04-OPT-14 should terminate at a narrow invocation/result adapter.

## Contradictions, corrections, and superseded claims

| Earlier corpus claim or direction | Evidence judgment under A007 | Resolution |
| --- | --- | --- |
| Rewrite Neo4j OLTP record storage in Rust | Product-scope contradiction | Reject; keep Neo4j/external systems as sources and baselines |
| Preserve all Bolt, Cypher, driver, and procedure behavior | Adoption scope confused with product scope | Implement only a measured read-only procedure adapter; register the rest as unsupported |
| Complete GDS source reading directly unlocks a complete Rust rewrite | Too broad and output-oriented | Reuse dossiers as fixture, estimator, and behavior evidence for selected kernels |
| Low explicit heap from mmap means hard low RAM | False accounting boundary | Fast mode may mmap; strict mode must explicitly control pools and report page-cache/RSS behavior |
| An estimate proves the job fits | Estimate is a prediction, not enforcement | Couple estimate to admission, allocator/pool limits, spill, and post-run error measurement |
| Flat CSR alone solves GDS | Contradicted by sidecars, model/result artifacts, and contracted workspaces | Choose CSR plus support planes and per-family runtime plans |
| A durable format should be invented for every algorithm | Not forced by current evidence | Share topology; specialize views, workspaces, precision, spill, and result artifacts |
| Cells are the default low-RAM answer | Evidence remains hypothetical | Keep A04-OPT-15 behind the falsifier thresholds in A04-000003 |
| Full mutable LSM/delta serving improves freshness | Violates immutable bounded-runner boundary | Reject as a serving architecture |
| GDS lacks memory estimation | False | GDS has rich estimators; the opportunity is enforceable closed-world plans and receipts |
| The progress dashboard shows only 60 of 111 dossiers complete | Stale generated progress claim | Current filesystem has all 111 queue dossier paths plus `ROLLUP.md` |
| Procedure-to-kernel TSV is fully rectangular | False | One Leiden row has 15 fields against a 16-field header; treat that row cautiously |

## Evidence confidence

| Level | Meaning | Examples |
| --- | --- | --- |
| High | Direct source cross-check or mechanically validated artifact fact | code-graph method body, SQLite counts, denominator hashes, GDS estimator terms |
| Medium-high | Repeated independent corpus evidence with a clear falsifier | canonical CSR plus sidecars, atomic generation catalog, bounded build foundry |
| Medium | Plausible physical plan grounded in known state shape but not benchmarked here | tiled PageRank, packed WCC, level artifacts for Louvain |
| Low/experimental | Architecture could help but current corpus does not establish a win | cells, selective GraphBLAS, precision-reduced embeddings |

No numerical speedup or RAM-reduction multiplier is claimed by this dossier. Such deltas must come from the benchmark proof plan with matched datasets, semantics, preparation scope, concurrency, and memory scope.

## Founder-oriented experiment sequence

### Experiment 1: Artifact and admission contract

- Build one immutable CSR artifact with checksums and generation metadata.
- Implement explicit topology, sidecar, workspace, writer, and margin accounting.
- Prove `fit/refuse` at adjacent budget thresholds.
- Deliberately corrupt counts/checksums and verify refusal.

### Experiment 2: Access-path slice

- Implement BFS and unweighted shortest path using A04-OPT-06.
- Support bounded depth, source/target filters, optional parent output, cancellation, and result checksum.
- Compare with GDS tiny fixtures and one realistic security graph.
- Run fast and strict modes at 5 GB and 10 GB caps.

### Experiment 3: WCC reuse proof

- Reuse the artifact, catalog, planner, pools, and receipt.
- Add only A04-OPT-07 workspace and partition parity oracle.
- This tests whether the architecture is a product substrate rather than a one-algorithm demo.

### Experiment 4: Iterative scan proof

- Implement A04-OPT-08 PageRank with deterministic reductions.
- Compare f64 exact/tolerance profile with an explicit f32 approximate profile.
- Measure topology I/O, vector high water, iteration tail, and preparation separately.

### Experiment 5: Hard-family falsifiers

- Triangle count: skewed-degree graphs and global-only versus per-node results.
- Similarity: candidate-cap and top-K spill thresholds.
- Louvain: one-level and multi-level contracted graph high water.
- Cells: adopt only if repeated workloads beat flat artifacts after metadata, duplicate bytes, rebuild, and cache costs.

## Denominator closure

At final corpus preflight:

- assigned rows: 222
- unique paths: 222
- denominator bytes: 173,196,880
- missing files: 0
- path/hash/bytes/extension drift: 0

The exact per-file coverage, classification, relevance, option mapping, and evidence ID are in:

`docs_PRD04/reference-learning/lowram-architecture-corpus/evidence/agent-04-prd03-files.tsv`

## Final decision table

| Decision | Options or scope | Why |
| --- | --- | --- |
| **Choose** | A04-OPT-01, 02, 03, 04, 05, 06, 07, 13, 16 | Small common substrate, founder-aligned wedge, and enforceable bounded execution |
| **Choose minimally** | A04-OPT-14 and 17 | Needed for adoption/results, but must not expand into database or ML-platform scope |
| **Experiment** | A04-OPT-08, 09, 10, 11, 15, 18 | Strong hypotheses whose RAM/latency deltas require matched benchmarks |
| **Defer** | A04-OPT-12, Node2Vec, GraphSAGE, broad ML pipelines, full model catalog | Large state and scope; not required to prove the company thesis |
| **Reject** | Rust OLTP rewrite, full Bolt/Cypher/driver clone, one durable topology per algorithm, live mutable OLAP serving, unbounded mmap-as-hard-budget, estimator bypass | These dilute A007, recreate incumbent complexity, or break the deterministic memory promise |

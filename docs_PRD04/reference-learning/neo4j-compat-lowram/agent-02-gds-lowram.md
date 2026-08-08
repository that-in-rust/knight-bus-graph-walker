# Agent 02: Neo4j GDS/APOC Evidence and Algorithm-Shaped Low-RAM Architecture

## Executive conclusion

The useful thing to inherit from Neo4j GDS is not its implementation architecture. It is the **observable contract surface**: graph projection, algorithm configuration, `stream`/`stats`/`mutate`/`write`/`estimate` modes, result schemas, termination behavior, and a large body of tests.

The A007 product should use that surface as an adoption adapter around a different core:

> An immutable graph artifact plus an algorithm-specific physical plan, admitted against a hard memory budget, executed as `fit`, `spill`, `approximate`, or `refuse`, and closed with a deterministic receipt.

The GDS source demonstrates mature component-level memory estimation. It does **not**, in the evidence examined here, establish the complete A007 contract: a conservative full-working-set bound covering graph representation, projection/conversion, algorithm state, concurrency, output buffering, spill buffers, runtime overhead, and safety margin, followed by process/container-level enforcement and calibrated receipts. This is the product gap. It must be stated without claiming that Neo4j lacks memory estimates.

For the first proof-carrying slice, the best order remains the A007 order:

1. BFS/access path
2. WCC
3. PageRank
4. Node similarity and kNN
5. Louvain and Leiden
6. Triangle count
7. FastRP

BFS is the right first implementation because it is directly useful for security/dependency/access-path artifacts, can be exact with bounded frontier and predecessor state, and exposes every part of the product contract: projection, estimation, admission, spill, cancellation, output streaming, and receipt verification.

---

## 1. Scope and governing product contract

This dossier is subordinate to `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.

The governing contract is:

```text
artifact + requested algorithm + hard budget
                     |
                     v
       full-working-set estimate
                     |
            +--------+---------+-------------+
            |                  |             |
           fit               spill      approximate
            |                  |             |
            +------------------+-------------+
                               |
                         enforced run
                               |
                  content-addressed receipt

If no valid plan exists under the declared contract: refuse before execution.
```

Neo4j compatibility is an adoption surface. It is not the core product and it does not justify reimplementing the Neo4j transactional database, Cypher runtime, or all APOC procedures.

### Assigned repositories

| Repository | Commit read | Product role |
|---|---:|---|
| `neo4j-gds-src` | `dc4417b3c1fe` | Primary behavioral oracle: graph catalog, estimators, algorithms, modes, tests |
| `neo4j-gds-client-src` | `e96f90669e75` | Python contract surface and result schemas across API/Cypher/Arrow transports |
| `gds-agent-src` | `65d1894a1fb0` | Tool/agent invocation adapter; useful for parameter and endpoint mapping |
| `graph-data-science-src` | `1da2b3d85b74` | Small examples/data repository; context, not an engine implementation |
| `neo4j-apoc-procedures-src` | `940033f21e82` | APOC procedure/function boundary and traversal utilities |
| `neo4j-apoc-src` | `11dbf56b0d23` | Second APOC lineage/snapshot; overlapping boundary evidence |

### License constraint

The GDS and APOC repositories include GPL-family licensing evidence. This dossier treats them as **behavioral oracles only**. A Rust implementation should be independently designed from public behavior, configuration/result contracts, differential tests, and measurements. No Java implementation should be translated line by line. Keep a provenance ledger for every compatibility test and independently derived algorithm.

---

## 2. Evidence method and quantitative coverage

### 2.1 Canonical denominator

The authoritative denominator is:

`docs_PRD04/reference-learning/neo4j-compat-lowram/evidence/all-files-denominator.tsv`

This agent's auditable ledger is:

`docs_PRD04/reference-learning/neo4j-compat-lowram/evidence/agent-02-files.tsv`

The ledger contains one row for every `assigned_agent=agent-02` denominator row and preserves `repo`, `path`, `git_blob`, `bytes`, and `extension` exactly. Evidence IDs are globally unique in the form `A02-######`.

### 2.2 Coverage totals

| Repository | Tracked rows | Direct read | Graph indexed | Generated | Non-code | Binary | Direct-read bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| `gds-agent-src` | 57 | 42 | 0 | 1 | 12 | 2 | 508,739 |
| `graph-data-science-src` | 18 | 6 | 0 | 0 | 5 | 7 | 8,632 |
| `neo4j-apoc-procedures-src` | 4,898 | 12 | 834 | 25 | 3,585 | 442 | 110,971 |
| `neo4j-apoc-src` | 612 | 11 | 462 | 0 | 122 | 17 | 126,291 |
| `neo4j-gds-client-src` | 994 | 72 | 742 | 2 | 151 | 27 | 510,231 |
| `neo4j-gds-src` | 5,634 | 843 | 4,072 | 40 | 551 | 128 | 4,357,785 |
| **Total** | **12,213** | **986** | **6,110** | **68** | **4,426** | **623** | **5,622,649** |

The immutable direct-read pass consumed 145,140 lines. Every direct-read blob was fetched by Git object ID; all 986 object hashes and byte counts matched the denominator.

### 2.3 Code-graph health and use

The required `code-graph-mcp` sequence was executed for every repository: deep health check, map, tour, then targeted search/show/deps/impact. All six indexes passed SQLite quick checks with zero FTS drift and zero orphan vectors. The indexes are FTS-only; no vector embeddings were present.

| Repository | Index files | Nodes | Edges | Pending unresolved calls | Result |
|---|---:|---:|---:|---:|---|
| `neo4j-gds-src` | 4,921 | 38,262 | 521,221 | 15,756 | Healthy |
| `neo4j-gds-client-src` | 823 | 7,066 | 70,056 | 3,286 | Healthy |
| `gds-agent-src` | 44 | 644 | 1,572 | 717 | Healthy |
| `graph-data-science-src` | 6 | 24 | 14 | 52 | Healthy |
| `neo4j-apoc-procedures-src` | 865 | 11,407 | 84,434 | 11,802 | Healthy |
| `neo4j-apoc-src` | 516 | 7,299 | 44,420 | 8,489 | Healthy |

“Pending unresolved calls” is a call-edge resolution limitation, not an unresolved file in the evidence ledger. File coverage has zero missing and zero unexpected rows.

### 2.4 Direct-read evidence spine

The TSV is the complete path-to-evidence map. These are the most consequential direct reads:

| Evidence | Repository path | What it establishes |
|---|---|---|
| `A02-009747` | `neo4j-gds-src/memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java` | Lower/upper memory ranges |
| `A02-009746` | `neo4j-gds-src/memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java` | Composite fixed/per-node/per-thread/per-dimension estimates |
| `A02-007613` | `neo4j-gds-src/applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/DefaultMemoryGuard.java` | Reservation/admission behavior and bypass conditions |
| `A02-009428` | `neo4j-gds-src/executor/src/main/java/org/neo4j/gds/executor/MemoryEstimationExecutor.java` | Projection-plus-algorithm estimate construction |
| `A02-012049` | `neo4j-gds-src/progress-tracking/src/main/java/org/neo4j/gds/mem/MemoryTracker.java` | Estimate tracking in task/progress machinery |
| `A02-007612` | `neo4j-gds-src/applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/DefaultAlgorithmProcessingTemplate.java` | Load, validate, compute, side-effect, render lifecycle |
| `A02-007611` | `neo4j-gds-src/applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ComputationService.java` | Guarded compute orchestration |
| `A02-010472` | `neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java` | Projection and projection-estimate procedure signatures |
| `A02-009737` | `neo4j-gds-src/memory-estimation/src/main/java/org/neo4j/gds/memest/GraphMemoryEstimation.java` | Graph dimensions plus post-load estimate |
| `A02-008263` | `neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java` | Projection memory components and CSR graph construction |
| `A02-008104` | `neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java` | Cursor-oriented adjacency interface and memory reporting |
| `A02-008363` | `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/huge/HugeGraph.java` | ID map, adjacency/inverse adjacency, properties, cursor caches |
| `A02-012025` | `neo4j-gds-src/progress-tracking/src/main/java/org/neo4j/gds/core/utils/progress/tasks/ProgressTracker.java` | Progress/task lifecycle |
| `A02-012100` | `neo4j-gds-src/termination/src/main/java/org/neo4j/gds/termination/TerminationFlag.java` | Cooperative termination contract |
| `A02-007002` / `A02-007004` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/paths/traverse/BFS.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/paths/traverse/BfsMemoryEstimateDefinition.java` | BFS kernel and memory state |
| `A02-007155` / `A02-007156` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/wcc/Wcc.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/wcc/WccMemoryEstimateDefinition.java` | WCC strategy and disjoint-set state |
| `A02-006976` / `A02-006978` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankAlgorithm.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankMemoryEstimateDefinition.java` | Pregel-style PageRank and memory definition |
| `A02-007086` / `A02-007087` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarity.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityMemoryEstimateDefinition.java` | Candidate/top-K/full-output memory behavior |
| `A02-007053` / `A02-007056` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/similarity/knn/Knn.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/similarity/knn/KnnMemoryEstimateDefinition.java` | Iterative neighbor structures and sampler state |
| `A02-006940` / `A02-006942` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/louvain/Louvain.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/louvain/LouvainMemoryEstimateDefinition.java` | Multilevel modularity and contracted graphs |
| `A02-006928` / `A02-006930` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/leiden/Leiden.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/leiden/LeidenMemoryEstimateDefinition.java` | Local move, refinement, aggregation state |
| `A02-007125` / `A02-007127` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/triangle/IntersectingTriangleCount.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/triangle/IntersectingTriangleCountMemoryEstimateDefinition.java` | Sorted-adjacency intersection and counters |
| `A02-006780` / `A02-006782` | `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRP.java`; `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRPMemoryEstimateDefinition.java` | Three embedding arrays and iteration ping-pong |
| `A02-005926` | `neo4j-gds-client-src/src/graphdatascience/procedure_surface/api/pathfinding/bfs_endpoints.py` | BFS public modes/results |
| `A02-005968` | `neo4j-gds-client-src/src/graphdatascience/procedure_surface/api/similarity/knn_endpoints.py` | kNN public modes/results |
| `A02-000017` / `A02-000032` | `gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/algorithm_handler.py`; `gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/registry.py` | Tool-to-GDS operation mapping |
| `A02-000312` / `A02-005290` | `neo4j-apoc-procedures-src/core/src/main/java/apoc/path/PathExplorer.java`; `neo4j-apoc-src/core/src/main/java/apoc/path/PathExplorer.java` | Bounded traversal compatibility surface |
| `A02-000161` / `A02-005217` | `neo4j-apoc-procedures-src/core/src/main/java/apoc/cypher/Timeboxed.java`; `neo4j-apoc-src/core/src/main/java/apoc/cypher/Timeboxed.java` | Time-boxed execution, not memory bounding |

---

## 3. What the repository family actually contains

### 3.1 Architecture map

```text
Python client / gds-agent / Cypher procedure calls
                       |
                       v
             procedure + config surface
                       |
          +------------+-------------+
          |                          |
          v                          v
 graph catalog/projection      algorithm machinery
 native/cypher/filter          estimate/guard/compute
          |                          |
          v                          v
 CSR/HugeGraph + ID map       algorithm-specific kernel
 adjacency + inverse          + progress/termination
 properties + schema                  |
          |                          v
          +-----------------> stream/stats/mutate/write
```

The most useful architectural separation is already visible: the catalog/projection layer is distinct from algorithm computation and procedure rendering. The rewrite should preserve the separation but change ownership. A graph **artifact** should be immutable and portable; a projection should usually be a view/manifest over that artifact, not a second materialized graph hidden from the budget.

### 3.2 Algorithm-family inventory

The code map exposes a much broader family than the first seven A007 targets:

| Family | Observed members | A007 interpretation |
|---|---|---|
| Traversal/path | BFS, DFS, Dijkstra, A*, delta stepping, Bellman-Ford, Yen, all-pairs shortest path, random walk, spanning/Steiner/PCST, DAG longest path/topological sort | Start with bounded BFS/access path; add weighted paths only after the receipt loop works |
| Components/community | WCC, SCC, Louvain, Leiden, label propagation, SLLPA, k-core, modularity, max-k-cut, coloring, k-means | WCC is first exact streaming/multi-pass proof; Louvain/Leiden test level-wise storage |
| Centrality | PageRank, ArticleRank, eigenvector, degree, betweenness, closeness/harmonic, HITS, bridges, articulation points, influence maximization | PageRank is the first iterative vector workload; degree can be a cheap control |
| Similarity | Node similarity, filtered node similarity, kNN, filtered kNN | Best proof that algorithm-shaped candidate generation matters more than generic CSR alone |
| Embeddings | FastRP, GraphSAGE, Node2Vec, HashGNN | FastRP is the first dense-vector pressure test; defer trained models |
| Triangle/cluster statistics | Triangle count, local clustering coefficient | Exact sorted/intersection format is a strong low-RAM target |
| ML/pipeline | Node classification/regression, link prediction, model/pipeline catalogs | Out of initial product core; later adapter territory |
| Graph operations | Projection, filtering, sampling, generation, mutate/write/export | Projection and filtering are core; database export is an adapter |

The commercial-looking surface area is large, but the portable bounded runner does not need to reproduce it all. The product should be famous for a small number of workloads with unusually credible memory contracts.

---

## 4. GDS compatibility surface

### 4.1 Graph catalog/projection

`GraphProjectProc` exposes these important shapes:

```text
gds.graph.project(graphName, nodeProjection, relationshipProjection, configuration)
gds.graph.project.estimate(nodeProjection, relationshipProjection, configuration)
gds.graph.project.cypher(graphName, nodeQuery, relationshipQuery, configuration) [deprecated]
gds.graph.project.cypher.estimate(nodeQuery, relationshipQuery, configuration)     [deprecated]
gds.beta.graph.project.subgraph(...)                                                [deprecated]
```

Compatibility should accept a useful subset of projection declarations, then canonicalize them into an artifact manifest:

```text
ArtifactManifest
  artifact_hash
  node_id_domain
  node_count
  edge_count_by_type_and_orientation
  property_columns
  adjacency_variants_available
  projection_predicate_hash
  statistics_version
  checksums
```

Projection estimation must include both final representation and peak conversion/loading state. `CSRGraphStoreFactory` explicitly distinguishes memory during loading from memory after loading; that distinction belongs in A007's receipt.

### 4.2 Mode matrix

| Algorithm | Estimate | Stream | Stats | Mutate | Write | Compatibility note |
|---|---:|---:|---:|---:|---:|---|
| BFS | Yes | Yes | Yes | Yes | No in examined client surface | Stream returns path-shaped output; output can dominate memory unless backpressured |
| WCC | Yes | Yes | Yes | Yes | Yes | `componentId` per node plus component statistics |
| PageRank | Yes | Yes | Yes | Yes | Yes | Score per node; convergence and iteration metadata |
| Node similarity | Yes | Yes | Yes | Yes | Yes | Pair output may be quadratic without `topK`/`topN` constraints |
| kNN | Yes | Yes | Yes | Yes | Yes | Pair output and convergence/iteration metadata |
| Louvain | Yes | Yes | Yes | Yes | Yes | Community IDs, modularity, levels, optional intermediate communities |
| Leiden | Yes | Yes | Yes | Yes | Yes | Similar to Louvain plus convergence/refinement behavior |
| Triangle count | Yes | Yes | Yes | Yes | Yes | Per-node triangle count; separate triangle enumeration surface also exists |
| FastRP | Yes | Yes | Yes | Yes | Yes | Embedding vector per node; output is `N * D` values |

Mode semantics for the bounded runner:

- `estimate`: emit a plan preview and upper-bound receipt without running.
- `stream`: bounded producer/consumer stream. Client backpressure is part of the memory contract.
- `stats`: aggregate only; must not materialize node/edge rows.
- `mutate`: create a new immutable artifact layer or sidecar column, never an unaccounted in-memory catalog mutation.
- `write`: adapter-controlled export with bounded batches and separate I/O/output accounting.

### 4.3 Result-schema compatibility

Compatibility should preserve names and types where they are valuable, not internal class hierarchies:

| Algorithm | Stream essentials | Stats/mutate/write essentials |
|---|---|---|
| BFS | `sourceNode`, path/node sequence | timings, configuration, relationships/properties written where applicable |
| WCC | `nodeId`, `componentId` | component count/distribution, timings, properties written |
| PageRank | `nodeId`, `score` | `ranIterations`, `didConverge`, score distribution, timings |
| Node similarity | `node1`, `node2`, `similarity` | nodes compared, pair counts, distribution, timings |
| kNN | `node1`, `node2`, `similarity` | iterations, convergence, pairs considered, timings |
| Louvain | `nodeId`, `communityId`, optional intermediate IDs | modularity/modularities, levels, community count/distribution |
| Leiden | `nodeId`, `communityId`, intermediate IDs | convergence, levels, community count/distribution |
| Triangle count | `nodeId`, `triangleCount` | global count/distribution, timings |
| FastRP | `nodeId`, `embedding` | embedding dimension, node properties written, timings |

### 4.4 Priority signature/configuration spine

The direct-read Python endpoint interfaces expose the following compatibility vocabulary. Every mode takes a graph or projection configuration; mutate/write modes add their target property or relationship type. Common operational arguments include `relationship_types`, `node_labels`, `concurrency`, `job_id`, `log_progress`, `username`, and, on executable modes, `sudo`. A007 should parse useful compatibility arguments but must not let `sudo` disable its hard budget.

| Algorithm | Algorithm-specific arguments observed in the client surface |
|---|---|
| BFS | `source_node`, `target_nodes`, `max_depth`; mutate adds `mutate_relationship_type` |
| WCC | `threshold`, `seed_property`, `consecutive_ids`, `relationship_weight_property`, `min_component_size` |
| PageRank | `damping_factor`, `tolerance`, `max_iterations`, `scaler`, `relationship_weight_property`, `source_nodes` |
| Node similarity | `top_k`, `bottom_k`, `top_n`, `bottom_n`, `similarity_cutoff`, lower/upper degree cutoffs, `similarity_metric`, `use_components`, weight property |
| kNN | `node_properties`, `top_k`, `similarity_cutoff`, `delta_threshold`, `max_iterations`, `sample_rate`, `perturbation_rate`, `random_joins`, `random_seed`, `initial_sampler` |
| Louvain | `tolerance`, `max_levels`, `max_iterations`, `include_intermediate_communities`, `seed_property`, `consecutive_ids`, weight property, `min_community_size` |
| Leiden | `gamma`, `theta`, `tolerance`, `max_levels`, `random_seed`, intermediate-community flag, seed/weight properties, minimum community size |
| Triangle count | `label_filter`, `max_degree` |
| FastRP | `embedding_dimension`, `iteration_weights`, `normalization_strength`, `node_self_influence`, `property_ratio`, `feature_properties`, weight property, `random_seed` |

These arguments are not all equally important for the first release. The planner must nevertheless bind every accepted argument into the canonical configuration hash because degree caps, output limits, dimensions, iteration limits, seed, concurrency, and projection filters can materially change memory, latency, determinism, or quality.

---

## 5. What GDS memory estimation proves, and what A007 must add

### 5.1 Evidence from GDS

The memory framework can compose:

- fixed estimates;
- per-node estimates;
- per-node-vector estimates;
- per-thread estimates;
- graph-dimension-dependent estimates;
- ranges, maxima, and composite memory trees.

`DefaultMemoryGuard` builds a memory requirement and reserves a lower or upper estimate depending on configuration. The examined implementation also contains two product-significant escape paths: administrator/sudo bypass, and skip-on-`MemoryEstimationNotImplementedException`. Those may be reasonable in that product, but an A007 hard-budget profile must never silently bypass admission.

The progress `MemoryTracker` records estimate/reservation state. It is not evidence of a process-level RSS or cgroup hard limiter. Therefore the bounded runner needs both an estimator and an external/enforced meter.

### 5.2 Full-working-set model

Use variables, not transplanted Java constants:

| Variable | Meaning |
|---|---|
| `N` | projected node count |
| `E` | projected adjacency entries after orientation/materialization |
| `P` | projected property payload bytes |
| `C` | worker concurrency |
| `F_peak` | peak active frontier/candidate count |
| `Q` | produced result rows or vectors |
| `D` | embedding/property dimension |
| `K` | retained neighbors/results per node |
| `L` | hierarchy levels |
| `B` | execution block/tile size |
| `S` | number and size of spill buffers |
| `R_out` | bytes per encoded output row |

The admission estimate should be:

```text
W_peak_upper =
    W_process_base
  + W_runtime_and_executor(C)
  + W_artifact_mappings
  + W_resident_graph_upper(N, E, P, representation)
  + W_projection_conversion_peak
  + W_algorithm_fixed
  + W_algorithm_node(N)
  + W_algorithm_edge(E)
  + W_frontier_or_candidates(F_peak)
  + W_thread_local(C)
  + W_output_buffers(Q, R_out, output_policy)
  + W_spill_buffers(S, B)
  + W_safety_margin(calibration_class)
```

This deliberately separates:

1. virtual mapped bytes;
2. estimated resident bytes;
3. process RSS;
4. cgroup/container charged memory;
5. retained output/artifact bytes;
6. transient conversion/loading peak.

Conflating them makes a “10 GB” promise meaningless.

### 5.3 Admission and enforcement

```text
usable_budget = hard_budget - process_reserve - emergency_cancel_reserve

FIT         if fit_upper <= usable_budget
SPILL       if spill_upper <= usable_budget and temp/I/O quotas are valid
APPROXIMATE if user opted in and quality contract has a measurable bound
REFUSE      otherwise
```

Enforcement requirements:

- launch each run inside a cgroup/container/job object or equivalent OS memory envelope;
- use allocator/accounting arenas for algorithm, output, and spill buffers;
- preallocate bounded queues; do not permit unbounded result collectors;
- sample process RSS and charged memory independently of allocator estimates;
- cancel cooperatively before the hard boundary using an emergency reserve;
- treat cancellation latency as a tested quantity;
- record actual peak, page faults, spill bytes, I/O, and estimate error;
- never downgrade exact to approximate silently;
- never bypass admission because the caller is privileged.

---

## 6. Algorithm-shaped storage and plan families

### 6.1 Common artifact spine

Do not build one physical graph format and force every algorithm through it. Build a common manifest with derived, content-addressed physical views:

```text
canonical edge/property artifact
        |
        +-- CSR-out: traversal, WCC
        +-- CSC-in / destination blocks: PageRank pull
        +-- degree-oriented sorted adjacency: triangles
        +-- postings by feature/neighbor: node similarity
        +-- columnar vector tiles: kNN/FastRP
        +-- community-contracted edge runs: Louvain/Leiden
```

Derived views are reusable artifacts with their own hashes and receipts. Conversion cost is charged once when created and always shown in the plan.

### 6.2 Per-algorithm architecture table

| Algorithm | GDS oracle state observed | Proposed exact physical plan | Symbolic peak state beyond artifact | Spill plan | Approximation plan | Refuse condition |
|---|---|---|---|---|---|---|
| BFS/access path | visited bitset; node and weight arrays; local-node/chunk state; copied result nodes | mmap/block CSR-out, compressed visited bitmap, bounded two-level frontier, optional predecessor sidecar | `W_fixed + W_visited(N) + W_frontier(F_peak) + W_predecessor(N_or_reached) + W_output` | partition frontier into sorted node-ID runs; merge/deduplicate between levels; predecessor log append-only | landmark reachability or sampled paths only under explicit non-exact mode; not a substitute for security proof | exact predecessor/output plus minimum frontier buffers exceed budget |
| WCC | disjoint set; larger incremental variant; sampled and unsampled strategies | stream edge blocks over mmap CSR/edge runs; paged union-find parent/rank; deterministic union policy | `W_fixed + W_parent(N) + W_rank(N) + W_block(B) + W_output` | mmap parent/rank; multi-pass label propagation or external union logs with deterministic compression passes | sampled component sketch only with explicit false-merge/false-split semantics | no exact multi-pass plan fits budget/temp quota or input is mutating during run |
| PageRank | Pregel node value plus message/queue machinery; iterative convergence | destination-sorted CSC blocks; two rank vectors, or bounded residual frontier; sequential pull scans | `W_fixed + 2*W_rank(N) + W_dangling + W_thread(C) + W_output` | mmap rank vectors and scan edge blocks each iteration; block Gauss-Seidel only if semantics declared | residual/active-set, early stop, quantized vectors, or top-K-only with error/residual receipt | requested tolerance cannot be reached within iteration/I/O/time budget |
| Node similarity | per-node bitsets/vectors; optional weights/components; top-K map; top-N list or full result graph | inverted postings/wedge enumeration, degree ordering, bounded per-node top-K heaps, partitioned candidate reducer | `W_fixed + W_posting_block(B) + W_candidates(F_peak) + N*K*W_pair + W_output` | hash/range partition candidate pairs; external reduce; bounded heap merge | MinHash/LSH/signature filters with recall audit sample | unconstrained all-pairs output or candidate upper bound exceeds all quotas |
| kNN | top-K per node plus old/new/reverse temporary neighbor structures; per-thread sampler | columnar feature tiles; bounded top-K heap per node; deterministic tile-pair schedule | `W_fixed + W_feature_tiles(B,D) + N*K*W_neighbor + W_thread(C) + W_output` | external tile schedule and per-partition top-K runs, then deterministic merge | HNSW/LSH/PQ or sampled neighbor-descent with measured `recall@K` | exact all-pairs distance work violates declared time/I/O budget, or approximate not authorized |
| Louvain | modularity state; hierarchy arrays; new CSR contracted graph at each level | edge runs grouped by community; assignment/volume arrays; partition-local move deltas; content-addressed contracted graph per level | `W_fixed + W_assignment(N) + W_volume(N) + W_delta(N_or_active) + W_level_graph(E_l) + W_output` | external sort/reduce to construct contracted levels; mmap prior level | early stop, restricted candidates, or coarser resolution with modularity delta receipt | deterministic/exact policy cannot fit one move block plus assignment/volume state |
| Leiden | local move, refinement, aggregation, post-aggregation, dendrogram and many node arrays | Louvain spine plus refinement partitions and bounded subcommunity workspaces | `W_louvain + W_refinement(N_or_partition) + W_aggregation(E_l) + W_dendrogram(N,L)` | spill level graphs and refinement partitions; process partitions in stable order | early stop or bounded refinement under explicit quality contract | refinement workspace minimum exceeds budget or reproducibility policy cannot be met |
| Triangle count | per-node atomic triangle counts; intersections assume sorted adjacency; degree filters | degree-oriented forward graph with sorted/delta-coded adjacency; small reusable intersection buffers | `W_fixed + W_count(N) + W_oriented_index(N,E_f) + C*W_intersection(max_degree_block) + W_output` | partition oriented wedges/edges; external intersection/count reduction | degree cap, wedge sampling, or sparsification with excluded-node/sample error receipt | exact oriented representation plus counters cannot fit and no valid spill volume exists |
| FastRP | property vectors plus output/A/B float embedding arrays; ping-pong iterations | columnar property tiles; row-blocked embeddings; mmap ping-pong matrices; fused degree scaling/accumulation | `W_fixed + W_property_tile(B,Dp) + W_embedding_tiles(B,D) + W_accumulator(C,D) + W_output` | mmap matrices and stream edge blocks; checkpoint each iteration | FP16/int8 storage, randomized dimension reduction, fewer iterations; report drift against sample oracle | requested precision/dimension requires a minimum tile larger than budget |

### 6.3 Why these formats reduce RAM without pretending I/O is free

The gains come from four mechanisms:

1. **Do not duplicate topology.** Select the direction/index needed by the algorithm; charge any inverse index explicitly.
2. **Bound active state.** Frontiers, candidates, heaps, output queues, and vector tiles have declared maxima.
3. **Use sequential external passes.** Spill plans trade elapsed time and storage I/O for a lower resident peak.
4. **Avoid generic object graphs.** Dense IDs, bitmaps, packed arrays, sorted runs, and columnar vectors make bytes predictable.

None guarantees lower latency. Fit-mode can be faster through locality and less allocation; spill-mode is expected to be slower but predictable. The receipt must report which regime ran.

### 6.4 Per-algorithm receipt additions

| Algorithm | Required receipt fields beyond common envelope |
|---|---|
| BFS | reached nodes, levels, peak frontier, predecessor policy, path checksum |
| WCC | component count/distribution hash, union passes, largest component, incremental flag |
| PageRank | iterations, convergence, residual norm, dangling mass policy, score checksum/top-K hash |
| Similarity | candidate pairs considered, retained pairs, degree filters, exact/approx, recall audit |
| kNN | distance evaluations, iterations, convergence, `K`, recall@K sample, index/seed hash |
| Louvain/Leiden | levels, modularity per level, community counts, move/refinement passes, seed/order policy |
| Triangles | orientation rule, intersections, excluded nodes/degree cap, global count and node-count checksum |
| FastRP | dimension, iterations, seed, numeric representation, quantization scale, vector checksum/error sample |

---

## 7. Determinism and predictability contract

Determinism is not “same wall-clock time.” It is a declared repeatability envelope.

Every receipt should bind:

```text
artifact_hash
derived_view_hashes
canonical_algorithm_config_hash
engine_build_hash
plan_id_and_version
exact_or_approximate
random_seed
worker_count
partition_and_reduction_order
floating_point_mode
hard_memory_budget
estimated_peak_lower_and_upper
actual_peak_rss_and_charged_memory
spill_bytes_and_io
runtime_and_progress_counters
result_schema_hash
result_checksum_or_quality_metrics
termination_status
```

For floating-point algorithms, reproducibility may require stable partition/reduction order and a deterministic mode slower than the fastest mode. Make that a visible plan choice:

- `deterministic-strict`: stable ordering and checksum target;
- `deterministic-tolerant`: bounded numeric delta with stated tolerance;
- `throughput`: nondeterministic reduction permitted, still budget-enforced.

---

## 8. APOC boundary

APOC is not one compatibility feature. It is a large collection of procedures and functions crossing very different risk boundaries. Main-source annotation searches found 448 `@Procedure`, 266 `@UserFunction`, and 16 `@UserAggregationFunction` occurrences in `neo4j-apoc-procedures-src`; the second `neo4j-apoc-src` snapshot has 223, 286, and 13 respectively. These are annotation occurrences, not deduplicated public API names: version aliases and overlapping snapshots can represent the same behavior more than once.

The examined high-value boundary is graph traversal:

- `PathExplorer.expandConfig`: relationship/label filters, min/max depth, uniqueness, BFS/DFS choice, start filtering, limits, end/terminator nodes, sequence, optional result.
- `Neighbors`: hop-oriented expansion/count.
- `PathFinding`: A*, Dijkstra, and simple-path utilities delegated into Neo4j traversal/path facilities.
- `Timeboxed`: queue- and time-bounded execution with transaction termination; this is not a RAM bound.
- `PeriodicUtils`: batch transaction machinery; useful only for adapter/export work.

Recommended capability tiers:

| Tier | Surface | Initial policy |
|---|---|---|
| 0 | Pure deterministic functions | Allow selectively; no graph or external side effects |
| 1 | Read-only bounded traversal/path | First APOC compatibility target; translate to artifact traversal plans |
| 2 | Import/export with explicit file/network capability | Adapter only; sandboxed, quota-bound, off by default |
| 3 | Mutating and periodic/batched procedures | Defer; use immutable artifact transforms instead |
| 4 | Dynamic Cypher, custom procedures, external/network/system access | Refuse in portable bounded profile unless separately sandboxed |

The correct user experience is an explicit unsupported-capability error, not partial execution with altered semantics.

---

## 9. Verification loop

### 9.1 Oracle ladder

```text
Level 1: hand-constructed tiny graphs with mathematical expected answers
Level 2: independent Rust property/metamorphic tests
Level 3: differential result comparison against GDS/APOC behavior
Level 4: randomized graph corpus across topology/degree/pathology classes
Level 5: budget enforcement under cgroup/container pressure
Level 6: estimator calibration and held-out error bounds
Level 7: fault injection for spill, disk-full, cancellation, and consumer backpressure
```

### 9.2 Behavioral oracles to extract

- procedure/function names, arguments, defaults, validation failures;
- stream/stats/mutate/write/estimate result columns and types;
- directed/undirected and inverse-index semantics;
- empty graph, singleton, disconnected, multigraph, self-loop, weighted, and filtered cases;
- convergence and iteration fields;
- seeded algorithm behavior and accepted numeric tolerance;
- projection and algorithm memory-estimate tree shape;
- termination and cleanup behavior;
- client API parity across API, Cypher, and Arrow adapters.

### 9.3 Calibration corpus

For every algorithm and physical plan, retain a corpus stratified by:

- `N` and `E`;
- degree distribution and maximum degree;
- directedness and inverse-index availability;
- property/vector dimensions;
- connected-component distribution;
- frontier/candidate selectivity;
- output mode and consumer speed;
- concurrency;
- exact/spill/approximate plan.

Fit an upper-bound correction by **plan class**, not one global multiplier. Hold out graph families during calibration. A plan graduates only when underprediction frequency and magnitude stay within the declared safety policy.

---

## 10. Requirement candidates

### REQ-A02-001: Canonical estimate

**WHEN** a user submits an artifact, algorithm configuration, output mode, and hard memory budget  
**THEN** the planner SHALL emit lower and conservative upper estimates for every full-working-set term  
**AND** SHALL identify the graph statistics and calibration version used  
**AND** SHALL refuse to report an estimate if a required term is unknown.

### REQ-A02-002: Four-way plan decision

**WHEN** estimation completes  
**THEN** the planner SHALL select exactly one of `fit`, `spill`, `approximate`, or `refuse`  
**AND** SHALL never select `approximate` without explicit caller authorization and a quality contract.

### REQ-A02-003: Hard enforcement

**WHEN** a run starts with budget `M`  
**THEN** the executor SHALL run inside an enforceable OS memory envelope no larger than `M`  
**AND** SHALL reserve cancellation headroom  
**AND** SHALL terminate before uncontrolled allocation can violate the envelope.

### REQ-A02-004: No privilege bypass

**WHEN** an administrator or privileged caller submits a bounded run  
**THEN** the same admission and enforcement policy SHALL apply  
**AND** unknown estimator terms SHALL cause refusal, not a skipped guard.

### REQ-A02-005: Bounded stream

**WHEN** stream output is selected and the consumer slows or stops  
**THEN** the producer SHALL respect a configured row/byte buffer bound  
**AND** SHALL backpressure, spill, or cancel according to the selected plan  
**AND** SHALL include the output-buffer peak in the receipt.

### REQ-A02-006: Projection accounting

**WHEN** an algorithm requires a missing physical view  
**THEN** the estimate SHALL include conversion peak, final view size, temporary storage, and conversion time class  
**AND** the resulting view SHALL be content-addressed and reusable.

### REQ-A02-007: Deterministic receipt

**WHEN** a run terminates for success, refusal, cancellation, or failure  
**THEN** the system SHALL persist a receipt binding artifact, plan, configuration, budget, estimates, actual peak, I/O, progress, and result checksum/quality.

### REQ-A02-008: Differential correctness

**WHEN** an implemented compatibility operation has a GDS/APOC oracle  
**THEN** the verification suite SHALL compare results and error behavior on the approved corpus  
**AND** SHALL document any intentional semantic deviation.

### REQ-A02-009: Spill integrity

**WHEN** a spill plan is interrupted, disk-full, or checksum-corrupted  
**THEN** it SHALL fail without publishing a successful result artifact  
**AND** SHALL clean or quarantine partial spill state  
**AND** SHALL record the fault in the receipt.

### REQ-A02-010: Estimator calibration

**WHEN** measured peak memory differs from the estimate  
**THEN** the receipt SHALL persist estimate error by plan term and graph class  
**AND** future estimator versions SHALL be evaluated on held-out graphs before becoming admissible.

### REQ-A02-011: APOC capability refusal

**WHEN** a submitted APOC call requires a disabled capability tier  
**THEN** the adapter SHALL refuse before graph execution  
**AND** SHALL return the unsupported capability and the nearest supported operation, if one exists.

### REQ-A02-012: BFS proof slice

**WHEN** an exact BFS/access-path request is run against the reference artifact under a hard budget  
**THEN** the runner SHALL produce the same reachable/path result as the approved oracle  
**AND** SHALL demonstrate fit and spill plans  
**AND** SHALL survive a slow consumer and cancellation probe  
**AND** SHALL emit a complete receipt.

---

## 11. Falsifiers and decision gates

The architecture is falsified or materially weakened if any of these occur:

1. **Estimator falsifier:** held-out runs regularly exceed the conservative upper estimate after calibration.
2. **Enforcement falsifier:** the process can cross the hard envelope before cooperative cancellation completes.
3. **Output falsifier:** stream/mutate/write output dominates memory and cannot be backpressured or spilled predictably.
4. **Conversion falsifier:** creating algorithm-shaped views costs more peak memory or time than the target workload can tolerate.
5. **Spill falsifier:** exact external plans cause unacceptable I/O amplification for the ICP's real graphs.
6. **Correctness falsifier:** deterministic exact plans cannot match mathematical and differential oracles.
7. **Approximation falsifier:** quality cannot be measured cheaply enough to make an approximation receipt credible.
8. **Compatibility falsifier:** most prospective users require unsupported Cypher/APOC/database semantics before they value the bounded runner.
9. **ICP falsifier:** founder interviews reveal that ingestion, schema governance, visualization, or transactionality is the dominant pain rather than algorithm RAM/predictability.
10. **Differentiation falsifier:** a generic mmap graph engine plus container memory limit matches the same peak, correctness, and receipt quality without algorithm-shaped storage.

---

## 12. Recommended implementation sequence

### Phase 1: Contract before kernels

Build artifact manifest, planner term model, hard-budget executor, receipt schema, bounded output channel, and refusal semantics. Use a no-op/counting kernel first. The proof is that the envelope and receipt are real.

### Phase 2: BFS end to end

Implement exact BFS over mmap CSR with a bounded frontier and optional predecessor sidecar. Verify tiny graphs, differential behavior, a 2 GB artifact, fit/spill paths, slow consumer, cancellation, and estimate calibration.

### Phase 3: WCC and PageRank

WCC proves exact multi-pass external state. PageRank proves iterative vectors, convergence, deterministic floating-point policy, and destination-oriented physical views.

### Phase 4: Similarity and communities

Node similarity/kNN prove candidate bounding and measurable approximation. Louvain/Leiden prove hierarchical derived artifacts and level-wise spill.

### Phase 5: Triangles and FastRP

Triangles prove orientation-specific topology. FastRP proves vector tiling, numerical representations, and high-output-pressure receipts.

At every phase, the deliverable is not “algorithm implemented.” It is:

```text
compatibility contract
+ mathematical oracle
+ estimator
+ admission decision
+ enforced fit/spill/approx/refuse execution
+ receipt
+ calibration evidence
```

That is the smallest coherent architecture that serves A007 and remains meaningfully different from “Neo4j rewritten in Rust.”

---

## 13. Audit status

- Assigned denominator rows: **12,213**
- Evidence rows: **12,213**
- Missing repo/path identities: **0**
- Unexpected repo/path identities: **0**
- Duplicate repo/path identities: **0**
- Git blob mismatches: **0**
- Byte-count mismatches: **0**
- Empty evidence IDs: **0**
- Duplicate Agent-02 evidence IDs: **0**
- Founder-critical source paths not direct-read: **0**
- Relevance `>=80` rows not direct-read: **0**
- Illegal coverage statuses: **0**
- Direct-read immutable blob verification errors: **0**

The repository-wide validator cannot complete until the peer `agent-01-files.tsv` and `agent-03-files.tsv` ledgers exist. Agent 02's subset passes the same identity, field, status, critical-read, and uniqueness checks independently.

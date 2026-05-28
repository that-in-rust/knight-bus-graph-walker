# v003 Requirements: Full GDS Surface On A CSR-Centered OLAP Architecture

> Purpose: capture the complete first-pass requirements for aiming at the full
> Neo4j Graph Data Science surface while keeping the storage decision honest.
>
> Core correction: **Tilehouse is not a requirement.** A full-GDS target needs a
> CSR-centered multi-plane architecture. Tilehouse is one possible topology
> backend for update locality and bounded compaction, not the foundation of GDS
> compatibility.

## Requirement Thesis

The architecture SHALL be:

```text
Neo4j-compatible procedure surface
-> GDS inventory and compatibility registry
-> named projection catalog
-> topology backend abstraction
   -> flat generationed dual CSR first
   -> optional Tilehouse backend later
   -> optional Graph-LSM backend later if freshness demands it
-> columnar property plane
-> bounded scratch plane
-> result sidecar plane
-> model and pipeline artifact plane
-> OLTP writeback/freshness bridge
```

The architecture SHALL NOT assume:

```text
CSR alone can represent all GDS semantics.
Tilehouse is mandatory for all GDS.
Every GDS procedure can run under an 8 GB budget.
Unsupported procedures can look unknown.
Memory estimates can ignore page cache, scratch, deltas, or output state.
```

## Completeness Definition

"ALL GDS requirements" means more than procedure names. The requirements are
complete only when they cover every visible contract layer a Neo4j GDS user can
observe or depend on:

| layer | requirement scope |
| --- | --- |
| procedure discovery | all local `gds.*` procedures, alpha/beta/deprecated aliases, modes, and estimates |
| invocation ABI | procedure names, parameters, defaults, configs, output columns, error behavior |
| catalog semantics | graph projection, list, exists, drop, size, schema, mutation, export, sample, and filter |
| graph data model | dense IDs, Neo4j IDs, labels, relationship types, orientations, properties, defaults, nulls |
| topology execution | forward/reverse/undirected adjacency, global streams, inverse indexes, edge filters |
| property execution | scalar columns, vector columns, weights, labels, feature matrices, graph properties |
| algorithm execution | exactness, convergence, deterministic seeds, workspaces, scratch, result modes |
| memory accounting | heap, RSS, page cache, direct buffers, scratch, deltas, sidecars, output, model bytes |
| freshness | snapshot generation, delta inclusion, write visibility, stale-read policy, refresh policy |
| mutate/writeback | projected graph sidecars, OLTP writeback, counts, property names, partial failure handling |
| ML artifacts | model metadata, pipeline metadata, training configs, feature schemas, metrics, persistence |
| operations | progress, cancellation, cleanup, telemetry, sysinfo, version/license state |
| testing | inventory, ABI goldens, tiny oracles, parity, memory budgets, crash/restart, compatibility |

Therefore, this document treats the scanned GDS procedure bases as **surface
coverage**, and the requirement IDs as **implementation contracts**. The final
implementation must satisfy both.

## Source Baseline

Local source facts used by these requirements:

| fact | evidence |
| --- | --- |
| Current Knight Bus topology is flat immutable dual CSR plus mmap. | `README.md:13-17`, `src/snapshot.rs:14-21`, `src/runtime.rs:41-53` |
| Current runtime is walk-focused, not GDS-focused. | `src/runtime.rs:22-39`, `src/types.rs:145-183` |
| Current normalized graph data is topology-only. | `src/types.rs:265-280` |
| GDS graph store includes graph, node, relationship, property, label, inverse index, mutation, and graph-view APIs. | `gitrefrepo/neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/GraphStore.java:48-238` |
| GDS builds CSR graph stores from projected nodes and relationships. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-87` |
| GDS compressed adjacency estimates pages, degrees, and offsets. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java:44-112` |
| GDS facade includes catalog, algorithms, model catalog, operations, pipelines, and deprecated metrics. | `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-44` |

Local GDS scan result used as a working inventory baseline:

| module | procedure bases | annotation rows |
| --- | ---: | ---: |
| `catalog` | 35 | 54 |
| `centrality` | 15 | 96 |
| `community` | 26 | 154 |
| `embeddings` | 6 | 36 |
| `machine-learning` | 33 | 46 |
| `misc` | 28 | 39 |
| `path-finding` | 20 | 92 |
| `pipeline-catalog` | 2 | 6 |
| `similarity` | 6 | 44 |
| `sysinfo` | 3 | 3 |
| **total** | **174 bases** | **570 rows** |

The scan found `562` unique procedure names. The final inventory SHALL be a
checked artifact and SHALL be treated as more authoritative than this summary.

## Requirement Levels

Every GDS procedure SHALL have exactly one support level:

| level | meaning |
| --- | --- |
| `P0Registered` | Procedure is known, classified, config-shaped, schema-shaped, and returns deterministic unsupported behavior if not implemented. |
| `P1ExactLowRam` | Procedure is implemented exactly and can run when its memory estimate fits the requested budget. |
| `P2Later` | Procedure is intentionally deferred but known and registered. |
| `UnsupportedButRegistered` | Procedure is known to exist in GDS but not supported in this release. |

Requirement:

```text
WHEN a user calls any known GDS procedure
THEN Knight Bus SHALL either execute it or return a deterministic
UnsupportedButRegistered response
AND SHALL NOT report it as an unknown procedure.
```

## Architecture Plane Requirements

### REQ-PLANE-001: Procedure ABI Plane

**WHEN** a GDS procedure call enters the system
**THEN** the system SHALL resolve it through a procedure registry
**AND** SHALL classify family, mode, config schema, output schema, support level,
and estimate behavior before execution.

Verification:

| test | assertion |
| --- | --- |
| `gds_registry_knows_all_inventory_rows` | every inventory row has a registry entry |
| `gds_registry_rejects_duplicates` | duplicate procedure names fail registry validation |
| `gds_unknown_differs_from_registered_unsupported` | unknown and known-unsupported errors differ |

### REQ-PLANE-002: Catalog Plane

**WHEN** a user creates, lists, filters, mutates, writes, samples, exports, or
drops a GDS graph
**THEN** the catalog plane SHALL manage named projection state independently of
the physical topology backend.

Verification:

| test | assertion |
| --- | --- |
| `graph_catalog_lifecycle_matches_gds_shape` | project/list/exists/drop works for named graphs |
| `graph_catalog_size_estimate_includes_all_planes` | size includes topology, properties, sidecars, scratch, and outputs |
| `graph_catalog_does_not_duplicate_topology_by_default` | projection reuses topology where possible |

### REQ-PLANE-003: Topology Backend Plane

**WHEN** an algorithm needs graph adjacency
**THEN** it SHALL depend on a topology backend trait, not on flat CSR or
Tilehouse directly.

Minimum topology backend operations:

```text
node_count
relationship_count
node labels by dense id
relationship types
forward neighbors
reverse neighbors
undirected logical neighbors
global forward edge stream
global reverse edge stream
relationship type filtered stream
degree and weighted degree
optional inverse index
```

Backend requirements:

| backend | requirement |
| --- | --- |
| `FlatDualCsrBackend` | SHALL be first implementation and correctness oracle. |
| `TilehouseBackend` | MAY be added for local updates, local compaction, and bounded page-window planning. |
| `GraphLsmBackend` | MAY be added only if measured freshness pressure makes snapshots plus deltas insufficient. |

### REQ-PLANE-004: Columnar Property Plane

**WHEN** a procedure requires labels, relationship types, weights, features,
node properties, relationship properties, graph properties, defaults, or nulls
**THEN** the property plane SHALL serve typed columnar values without requiring a
new topology layout.

Supported value requirements:

| value area | requirement |
| --- | --- |
| node labels | label membership SHALL be queryable as bitsets or equivalent compressed columns |
| relationship types | type filters SHALL apply to adjacency streams |
| weights | numeric relationship weights SHALL support default and missing-property behavior |
| node properties | scalar and vector values SHALL support typed reads |
| relationship properties | edge-aligned properties SHALL support relationship type filters |
| graph properties | graph-level values SHALL be stored in catalog metadata or graph property sidecars |
| feature vectors | ML and KNN features SHALL be columnar and estimateable |
| embeddings | output embeddings SHALL be result sidecars, not topology |

### REQ-PLANE-005: Scratch Plane

**WHEN** an algorithm requires vectors, frontiers, queues, heaps, candidate pairs,
walk corpora, matrices, or contracted graphs
**THEN** the scratch plane SHALL allocate or spill that state under an explicit
execution budget.

Scratch classes:

```text
ScalarPerNode
VectorPerNode
FrontierBitset
DistanceVector
PriorityQueue
CandidateTopK
PairStream
EmbeddingMatrix
WalkCorpus
ContractedGraph
ModelTrainingBatch
```

### REQ-PLANE-006: Result Sidecar Plane

**WHEN** a procedure runs in `mutate` mode
**THEN** its output SHALL be stored as a projected graph sidecar
**AND** the catalog SHALL expose the new property or relationship type.

**WHEN** a procedure runs in `write` mode
**THEN** its output SHALL be written through the OLTP-facing writeback bridge
**AND** write counts, property names, and failure behavior SHALL match the GDS
procedure contract.

### REQ-PLANE-007: Model And Pipeline Artifact Plane

**WHEN** a procedure creates, trains, lists, predicts with, or drops a model or
pipeline
**THEN** model and pipeline metadata SHALL live in an artifact plane separate
from topology and property columns.

This plane SHALL support:

```text
model name
pipeline name
owner/database identity
feature schema
training configuration
metrics
artifact bytes
creation and modification metadata
versioning
drop/list/exists behavior
```

### REQ-PLANE-008: Freshness Bridge

**WHEN** OLTP data changes after an OLAP projection is built
**THEN** OLAP SHALL expose explicit freshness semantics:

```text
snapshot_only
snapshot_plus_bounded_delta
force_refresh_before_run
reject_until_compacted
```

The freshness bridge SHALL NOT require Tilehouse in v1. It MAY use:

| approach | requirement |
| --- | --- |
| generationed flat CSR rebuild | baseline correctness path |
| bounded global delta overlay | small update bridge |
| Tilehouse cell-local deltas | optional if local update/compaction wins are measured |
| Graph-LSM | later if near-real-time OLAP becomes mandatory |

## Cross-Cutting GDS Requirements

### REQ-GDS-INV-001: Full Procedure Inventory

**WHEN** the local GDS reference changes
**THEN** the inventory generator SHALL detect added, removed, or renamed
procedures
**AND** SHALL fail CI until support levels and schema metadata are updated.

### REQ-GDS-ABI-001: Procedure Modes

Every algorithm procedure SHALL classify modes:

| mode | requirement |
| --- | --- |
| `stream` | return rows without mutating catalog or OLTP |
| `stats` | return aggregate metrics without writing result sidecars |
| `mutate` | write result into projected graph sidecars |
| `write` | write result back through OLTP-facing bridge |
| `estimate` | return memory contract without executing algorithm work |
| `train` | create model artifact and metrics |
| `predict` | use model/pipeline artifacts and return or write predictions |

### REQ-GDS-ABI-002: Config Parsing

**WHEN** a procedure receives configuration
**THEN** the parser SHALL validate defaults, aliases, bad types, missing keys,
unknown keys, and deprecated config keys before execution.

### REQ-GDS-ABI-003: Output Schemas

**WHEN** a procedure executes or estimates
**THEN** output column names and value types SHALL match the checked GDS
inventory for that procedure and mode.

### REQ-GDS-ABI-004: Deprecated, Alpha, And Beta Procedures

**WHEN** a procedure is alpha, beta, or deprecated in GDS
**THEN** the registry SHALL record that status
**AND** SHALL expose a deterministic compatibility policy:

```text
support as alias
support as deprecated alias
register but unsupported
exclude only with explicit compatibility note
```

### REQ-GDS-ABI-005: Determinism

**WHEN** a procedure has stochastic behavior
**THEN** seeded execution SHALL be reproducible across flat CSR and any future
topology backend.

### REQ-GDS-ABI-006: Procedure Discovery And Metadata

**WHEN** procedure metadata is requested or inspected
**THEN** the system SHALL expose procedure names, modes, deprecation status,
descriptions where available, parameter names, default values, and output
columns consistently with the checked inventory.

### REQ-GDS-ABI-007: Error Semantics

**WHEN** a procedure fails before execution
**THEN** the error SHALL identify whether the cause is unknown procedure,
registered unsupported procedure, bad config, missing graph, missing property,
unsupported mode, insufficient memory budget, stale projection, or internal
failure.

**WHEN** a procedure fails during execution
**THEN** partial scratch, result sidecars, model artifacts, and catalog mutations
SHALL be cleaned up or marked failed deterministically.

### REQ-GDS-ABI-008: User, Database, And Ownership Context

**WHEN** a procedure reads or writes catalog/model/pipeline state
**THEN** graph names, model names, and pipeline names SHALL be scoped by database
identity and owner semantics compatible with Neo4j GDS expectations.

### REQ-GDS-ABI-009: Concurrency Config

**WHEN** a procedure accepts concurrency or read-concurrency configuration
**THEN** the planner SHALL translate that value into bounded worker, I/O, and
scratch budgets
**AND** SHALL reject settings that would violate the selected memory contract.

### REQ-GDS-ABI-010: Cancellation And Cleanup

**WHEN** a user cancels a running procedure or the server shuts down
**THEN** the procedure SHALL stop at a safe checkpoint, release scratch, preserve
durable catalog/model state, and report a deterministic cancelled or interrupted
status.

## Graph Data Model Requirements

### REQ-DATA-001: ID Mapping

**WHEN** data enters an OLAP projection
**THEN** the system SHALL maintain a stable mapping between Neo4j-facing IDs,
external keys where present, and internal dense IDs.

The mapping SHALL support:

```text
node id to dense id
dense id to node id
relationship id or synthetic id to edge position
edge position to source and target dense ids
generation identity
projection-local filtered ids
```

### REQ-DATA-002: Labels

**WHEN** a projection selects node labels
**THEN** label membership SHALL be represented as columnar or bitmap sidecars
that support union, intersection, exclusion, and empty-label behavior.

### REQ-DATA-003: Relationship Types

**WHEN** a projection selects relationship types
**THEN** type selection SHALL apply to adjacency streams, property streams,
counts, estimates, inverse indexes, and writeback targets.

### REQ-DATA-004: Orientation

**WHEN** a projection or algorithm requests `NATURAL`, `REVERSE`, or
`UNDIRECTED` orientation
**THEN** the logical graph view SHALL produce the correct edge direction without
rewriting base topology unless the physical plan explicitly chooses a derived
sidecar.

### REQ-DATA-005: Relationship Aggregation

**WHEN** projection config aggregates parallel relationships
**THEN** aggregation semantics SHALL be represented in the projection catalog,
topology view, relationship counts, and property values.

### REQ-DATA-006: Property Defaults And Nulls

**WHEN** a property is missing, null, or has a configured default
**THEN** the property plane SHALL resolve the value according to procedure config
before execution begins.

### REQ-DATA-007: Numeric Types And Coercion

**WHEN** a procedure requires numeric weights, features, labels, or outputs
**THEN** type coercion, overflow, NaN, infinity, and invalid-value behavior SHALL
be validated before execution.

### REQ-DATA-008: Vector Values

**WHEN** a procedure requires vector properties or embeddings
**THEN** dimensionality, physical value type, null behavior, and output layout
SHALL be explicit in the property or result sidecar manifest.

### REQ-DATA-009: Schema Reporting

**WHEN** graph schema is listed or streamed
**THEN** node labels, relationship types, graph properties, node properties,
relationship properties, and result sidecars SHALL be reported from catalog
metadata without scanning the full graph.

## Execution Semantics Requirements

### REQ-EXEC-001: Physical Plan Explanation

**WHEN** a procedure is estimated or executed
**THEN** the selected physical plan SHALL be explainable as one of:

```text
metadata_only
local_adjacency
global_mmap_scan
global_explicit_stream
frontier_traversal
priority_queue_traversal
iterative_vector_scan
intersection_scan
candidate_generation
matrix_or_embedding_job
pipeline_training_job
model_prediction_job
writeback_job
```

### REQ-EXEC-002: Row Ordering

**WHEN** a procedure returns rows
**THEN** row ordering SHALL be deterministic where GDS requires or implies it
**AND** unspecified ordering SHALL be documented and stable enough for tests.

### REQ-EXEC-003: Streaming And Backpressure

**WHEN** result rows are streamed
**THEN** the system SHALL avoid materializing all rows in heap unless the
procedure semantics require global sorting or aggregation.

### REQ-EXEC-004: Progress Reporting

**WHEN** a long-running procedure executes
**THEN** progress state SHALL report procedure name, graph name, phase, work
completed, work remaining where knowable, memory plan, and cancellation state.

### REQ-EXEC-005: Resource Isolation

**WHEN** multiple procedures run concurrently
**THEN** each procedure SHALL have independent budget accounting for heap,
scratch, spill, direct buffers, and result sidecars.

### REQ-EXEC-006: Temporary Artifact Lifecycle

**WHEN** a procedure creates scratch, spill files, candidate tapes, walk corpora,
or temporary contracted graphs
**THEN** those artifacts SHALL be namespaced by job/generation and cleaned up on
success, cancellation, and recoverable failure.

### REQ-EXEC-007: Result Atomicity

**WHEN** a mutate, write, train, or pipeline operation completes
**THEN** the catalog/model/writeback state SHALL become visible atomically at the
procedure boundary.

## Catalog Surface Requirements

Catalog module scope from local GDS scan:

```text
35 procedure bases
54 annotation rows
```

### REQ-CAT-001: Native Projection

**WHEN** a user calls `gds.graph.project`
**THEN** the system SHALL create a named graph projection from labels,
relationship types, orientation, and property selectors
**AND** SHALL not copy full topology unless the physical plan requires it.

### REQ-CAT-002: Cypher Projection

**WHEN** a user calls `gds.graph.project.cypher`
**THEN** the system SHALL support a compatibility path for node and relationship
queries
**AND** SHALL either execute the projection or return
`UnsupportedButRegistered` with a Cypher-projection reason.

### REQ-CAT-003: Graph Lifecycle

**WHEN** a user calls graph list, exists, drop, or size procedures
**THEN** the catalog SHALL reflect named projection state, owner/database
identity, schema, counts, memory estimates, and modification timestamps.

### REQ-CAT-004: Graph Filtering And Subgraphs

**WHEN** a user creates a filtered/subgraph projection
**THEN** the system SHALL represent filters as logical graph views over topology
and property planes where possible.

### REQ-CAT-005: Property Streaming

**WHEN** a user streams node, relationship, or graph properties
**THEN** the property plane SHALL stream typed values without materializing the
whole property set in heap.

### REQ-CAT-006: Property Mutation And Drop

**WHEN** a user mutates or drops graph, node, or relationship properties
**THEN** the catalog and sidecar metadata SHALL update atomically.

### REQ-CAT-007: Relationship Transformations

**WHEN** a user calls relationship transforms such as to-undirected,
index-inverse, relationship delete, or derived relationship procedures
**THEN** the system SHALL create logical or sidecar topology artifacts without
rewriting base CSR unless explicitly required.

### REQ-CAT-008: Export And Sampling

**WHEN** a user exports, samples, or generates graphs
**THEN** the system SHALL route through bounded streaming plans and report memory
usage before execution.

## Centrality Requirements

Centrality scope from local scan:

```text
15 procedure bases
96 annotation rows
```

### REQ-CENT-001: Degree Family

Degree centrality SHALL read offsets and optional weights from topology/property
planes and SHALL support stream/stats/mutate/write/estimate modes where present
in the inventory.

### REQ-CENT-002: PageRank Family

PageRank, ArticleRank, and Eigenvector SHALL run over global edge streams with
explicit vector estimates.

Requirements:

```text
score vector bytes SHALL be explicit
previous/current vectors SHALL be explicit
degree or weight arrays SHALL be explicit
mmap plans SHALL NOT claim deterministic RAM
strict-RAM plans SHALL use explicit stream/spill/reject behavior
```

### REQ-CENT-003: HITS

HITS SHALL support forward and reverse scans and estimate authority/hub vectors
separately.

### REQ-CENT-004: Closeness And Harmonic

Closeness and harmonic centrality SHALL treat all-pairs or many-source traversal
as high-risk and SHALL estimate source batching, frontier, and distance state.

### REQ-CENT-005: Betweenness

Betweenness SHALL estimate source sweeps, sigma/delta vectors, stacks,
predecessor state, and source batching before execution.

### REQ-CENT-006: Articulation Points And Bridges

Articulation point and bridge detection SHALL support DFS low-link state and
SHALL preserve deterministic traversal order for reproducible output.

### REQ-CENT-007: Influence Maximization

Influence/CELF procedures SHALL be registered and SHALL require explicit
simulation/sample/candidate state estimates before moving beyond unsupported
status.

## Pathfinding Requirements

Pathfinding scope from local scan:

```text
20 procedure bases
92 annotation rows
```

### REQ-PATH-001: BFS And DFS

BFS and DFS SHALL run over topology backend cursors with bounded visited,
frontier, path, and parent state.

### REQ-PATH-002: Weighted Shortest Path

Dijkstra, A*, Delta-Stepping, and Bellman-Ford SHALL require a weight sidecar
contract and SHALL fail deterministically if configured weights are absent or
invalid.

### REQ-PATH-003: All Shortest Paths

All-shortest-path procedures SHALL estimate repeated source sweeps and SHALL
reject unsafe exact plans under strict memory budgets.

### REQ-PATH-004: Yen's K Shortest Paths

Yen's procedures SHALL estimate candidate path heaps, suppressed path state, and
underlying shortest-path work.

### REQ-PATH-005: Random Walk

Random walk procedures SHALL use deterministic seeded RNG behavior and SHALL
store walk buffers or corpora in the scratch/result sidecar plane.

### REQ-PATH-006: Spanning And Steiner Trees

Spanning tree and Steiner procedures SHALL estimate parent arrays, candidate
heaps, prizes/weights, and writeback relationship counts.

### REQ-PATH-007: DAG Algorithms

Topological sort and longest path SHALL validate DAG assumptions and SHALL
return deterministic errors for cyclic input where required.

## Community And Structure Requirements

Community scope from local scan:

```text
26 procedure bases
154 annotation rows
```

### REQ-COMM-001: WCC And SCC

WCC and SCC SHALL run over topology backend cursors and SHALL estimate component
arrays, stacks, and frontier/union state.

### REQ-COMM-002: Triangle Count And Local Clustering

Triangle count and local clustering coefficient SHALL require sorted adjacency
or intersection-capable cursors and SHALL avoid double-counting across filters,
relationship types, and future Tilehouse boundaries.

### REQ-COMM-003: K-Core

K-core SHALL estimate degree arrays, peel queues, and result sidecars.

### REQ-COMM-004: Coloring

K-1 coloring SHALL estimate color arrays and conflict frontiers and SHALL define
deterministic tie-breaking.

### REQ-COMM-005: Label Propagation And SLPA

Label propagation and SLPA SHALL estimate label state, distributions, seed
behavior, and convergence iterations.

### REQ-COMM-006: Louvain And Leiden

Louvain and Leiden SHALL require a contracted-graph scratch artifact and SHALL
estimate community arrays, modularity state, and every contraction level.

### REQ-COMM-007: Modularity And Conductance

Modularity, modularity optimization, and conductance SHALL read community
assignments from property/result sidecars and SHALL stream cut/internal edge
counts.

### REQ-COMM-008: Max-K-Cut And K-Means

Max-k-cut and k-means SHALL be treated as property/state-heavy algorithms and
SHALL require explicit vector/assignment/centroid estimates.

## Similarity Requirements

Similarity scope from local scan:

```text
6 procedure bases
44 annotation rows
```

### REQ-SIM-001: Node Similarity

Node similarity SHALL use adjacency/property overlap without materializing all
`O(n^2)` pairs unless a configured budget explicitly allows it.

### REQ-SIM-002: KNN

KNN SHALL operate on feature/property vectors from the columnar property plane
and SHALL require candidate generation, topK heap, and cutoff estimates.

### REQ-SIM-003: Filtered Similarity

Filtered similarity procedures SHALL push label/property filters before
candidate expansion wherever possible.

## Embeddings Requirements

Embeddings scope from local scan:

```text
6 procedure bases
36 annotation rows
```

### REQ-EMB-001: FastRP

FastRP SHALL estimate `node_count * dimension * bytes_per_value` output and
intermediate propagation state before execution.

### REQ-EMB-002: Node2Vec

Node2Vec SHALL estimate walk corpus, context windows, embedding matrices, RNG
state, and training batches.

### REQ-EMB-003: GraphSAGE

GraphSAGE SHALL require model artifact support, sampled neighbor batches, feature
columns, and train/infer mode contracts.

### REQ-EMB-004: HashGNN

HashGNN SHALL define deterministic hash behavior and estimate feature hashes plus
embedding outputs.

## Machine Learning And Pipeline Requirements

Machine-learning module scope from local scan:

```text
33 procedure bases
46 annotation rows
```

Pipeline catalog scope:

```text
2 procedure bases
6 annotation rows
```

### REQ-ML-001: Pipeline Lifecycle

Pipeline create, list, exists, drop, configure, add-step, and train procedures
SHALL be backed by the model/pipeline artifact plane.

### REQ-ML-002: Node Classification And Regression

Node classification and regression SHALL require:

```text
feature schema
target property validation
train/test split metadata
model candidate metadata
metrics
prediction sidecars
writeback behavior
```

### REQ-ML-003: Link Prediction

Link prediction SHALL require relationship split metadata, negative sampling
state, feature extraction, model artifacts, and prediction output contracts.

### REQ-ML-004: KGE

Knowledge graph embedding procedures SHALL require typed relationship sidecars,
negative sampling, embedding matrices, model artifacts, and strict memory
estimates.

### REQ-ML-005: Split Relationships

Relationship splitting SHALL be deterministic with seed configuration and SHALL
write split membership as sidecars or relationship properties.

## Model Catalog Requirements

Model catalog scope from local scan:

```text
3 procedure bases
drop/list/exists behavior
```

### REQ-MODEL-001: Model Metadata

Models SHALL persist:

```text
model name
model type
creator
database identity
creation time
feature schema
training config
metrics
artifact location
version
```

### REQ-MODEL-002: Model Lifecycle

Model list, exists, and drop SHALL survive process restart and SHALL be
independent from CSR topology generation.

## Miscellaneous, Operations, And Sysinfo Requirements

Misc scope from local scan:

```text
28 procedure bases
39 annotation rows
```

Sysinfo scope:

```text
3 procedure bases
3 annotation rows
```

### REQ-MISC-001: Scale Properties

Scale properties SHALL operate on the columnar property plane and SHALL support
stream/mutate/write/estimate behavior where present.

### REQ-MISC-002: Collapse Path

Collapse path SHALL create derived relationship sidecars or topology artifacts
without rewriting base CSR unless required by config.

### REQ-MISC-003: To Undirected

To-undirected SHALL prefer logical projection or derived relationship sidecar
over base topology rewrite.

### REQ-MISC-004: Index Inverse

Index inverse SHALL create or validate reverse adjacency/inverse indexes for
relationship types.

### REQ-MISC-005: Progress And Memory Procedures

Progress and memory procedures SHALL expose running task state, estimates,
actual measurements, and historical summary where compatible.

### REQ-MISC-006: Feature Toggles

Feature procedures SHALL be registered and SHALL map to explicit Knight Bus
settings or deterministic unsupported responses.

### REQ-SYS-001: Version, License, And Debug Info

Version, license state, and sysinfo procedures SHALL be server/admin surface
requirements, not topology requirements.

## Memory Requirements

### REQ-MEM-001: Holistic Estimate Object

Every estimate SHALL include:

```text
required_bytes
heap_bytes
rss_budget_bytes
page_cache_expected_bytes
page_cache_unbounded_risk
direct_io_buffer_bytes
topology_bytes
property_sidecar_bytes
algorithm_state_bytes
scratch_bytes
delta_overlay_bytes
result_sidecar_bytes
model_artifact_bytes
writeback_bytes
spill_bytes
```

### REQ-MEM-002: Mmap Honesty

**WHEN** a plan uses mmap
**THEN** the estimate SHALL state that page-cache residency is OS-mediated
**AND** SHALL NOT claim deterministic RAM.

### REQ-MEM-003: Strict-RAM Execution

**WHEN** a user selects a strict memory budget
**THEN** the planner SHALL choose explicit-stream/spill execution or reject the
procedure before execution.

### REQ-MEM-004: 50 GB On 8 GB Decision

**WHEN** the graph is 50 GB-class and the machine budget is 8 GB-class
**THEN** each procedure SHALL produce:

```text
can_run
required_budget_bytes
dominant_state
execution_profile
freshness_mode
reason_if_rejected
```

## Freshness And Update Requirements

### REQ-FRESH-001: Generation Identity

Every projection SHALL record source generation, freshness watermark, and
whether deltas are included.

### REQ-FRESH-002: Small Update Behavior

**WHEN** 10 OLTP records change
**THEN** OLAP SHALL NOT be required to rebuild the entire projection unless the
selected freshness mode requires exact regenerated snapshots.

Allowed behavior:

```text
serve snapshot-only stale analytics
serve snapshot plus bounded delta
force refresh before run
reject until refresh if delta budget is exceeded
```

### REQ-FRESH-003: Tilehouse Optionality

Tilehouse SHALL be introduced only if one of these measured triggers occurs:

| trigger | measurement |
| --- | --- |
| flat rebuild lag violates freshness SLO | rebuild time and update rate |
| global delta overlay exceeds memory budget | delta bytes and query merge cost |
| local traversals churn page cache badly | major faults and resident set |
| dirty-region compaction beats generation rebuild | compaction time and scratch bytes |

## Operational Requirements

### REQ-OPS-001: Restart Durability

**WHEN** the process restarts
**THEN** catalog metadata, projection manifests, result sidecars, model
artifacts, pipeline artifacts, and durable freshness receipts SHALL either load
successfully or fail with a recoverable corruption report.

### REQ-OPS-002: Manifest Versioning

**WHEN** a stored artifact is opened
**THEN** the system SHALL validate format version, graph generation, checksum or
length metadata, feature flags, and required sidecar presence before serving it.

### REQ-OPS-003: Compatibility Versioning

**WHEN** GDS inventory is generated from a reference checkout
**THEN** the inventory SHALL record reference repo path, branch or tag, commit
hash where available, scan command, excluded paths, and generation timestamp.

### REQ-OPS-004: Telemetry

**WHEN** a procedure runs
**THEN** telemetry SHALL record estimate, selected plan, actual peak RSS where
available, scratch bytes, spill bytes, page-fault counters where available,
duration, row count, write count, and failure reason.

### REQ-OPS-005: Security And Access Control Boundary

**WHEN** a procedure accesses graph, model, pipeline, property, or writeback
state
**THEN** the architecture SHALL carry user/database context through the call even
if full Neo4j-compatible authorization is implemented later.

### REQ-OPS-006: Export And Import Boundaries

**WHEN** graph export, CSV export, database export, Arrow import, or equivalent
I/O-heavy procedures are called
**THEN** the system SHALL treat them as bounded streaming jobs with explicit
disk, memory, and cancellation behavior.

### REQ-OPS-007: Admin Surface

**WHEN** version, sysinfo, license, memory, debug, or feature procedures are
called
**THEN** the system SHALL route them through an admin/procedure plane independent
from topology storage.

### REQ-OPS-008: Documentation Traceability

**WHEN** a requirement is implemented
**THEN** the implementation PR SHALL cite the requirement ID, inventory rows,
and tests that prove the requirement.

## Testing Requirements

### REQ-TEST-001: Inventory Tests

Tests SHALL prove the local GDS inventory is complete and deterministic.

### REQ-TEST-002: ABI Golden Tests

Tests SHALL compare procedure names, modes, config argument names, and result
columns against checked inventory fixtures.

### REQ-TEST-003: Tiny Oracle Tests

Each implemented algorithm SHALL have tiny hand-computed graph fixtures.

### REQ-TEST-004: Flat CSR Oracle Tests

Each topology backend SHALL match flat CSR adjacency and global edge streams for
small generated graphs.

### REQ-TEST-005: Property Plane Tests

Tests SHALL cover labels, relationship types, numeric weights, missing values,
defaults, vector features, and null handling.

### REQ-TEST-006: Memory Contract Tests

Every implemented procedure SHALL have:

```text
estimate test
budget accept test
budget reject test
mmap honesty test if mmap is used
spill accounting test if spill is used
```

### REQ-TEST-007: Mode Tests

Every implemented mode SHALL have schema and side-effect tests:

| mode | test requirement |
| --- | --- |
| `stream` | no catalog mutation |
| `stats` | aggregate rows only |
| `mutate` | result sidecar exists and catalog reflects it |
| `write` | OLTP writeback count and property name validation |
| `estimate` | no algorithm execution |
| `train` | model artifact created |
| `predict` | prediction rows or writeback match schema |

### REQ-TEST-008: Crash And Cleanup Tests

Tests SHALL cover temp scratch cleanup, partial result failure, receipt replay,
and model/catalog durability.

## TDD Rollout Requirements

### REQ-ROLL-001: First Implementation PR

The first code-bearing PR SHALL be:

```text
GDS inventory and registry only.
```

It SHALL NOT implement Tilehouse or algorithm kernels.

### REQ-ROLL-002: Second Implementation PR

The second code-bearing PR SHALL be:

```text
Projection catalog skeleton and estimate shape.
```

### REQ-ROLL-003: Third Implementation PR

The third code-bearing PR SHALL be:

```text
GraphTopologyBackend over existing flat dual CSR.
```

### REQ-ROLL-004: Tilehouse Gate

Tilehouse SHALL NOT begin until flat CSR backend, property plane skeleton,
catalog skeleton, and memory estimate object exist.

### REQ-ROLL-005: Support Promotion Gate

No procedure SHALL move to `P1ExactLowRam` until:

```text
registry row exists
config parser exists
estimate exists
tiny oracle passes
flat CSR backend passes
mode schema test passes
budget reject test passes
determinism policy is documented
unsupported modes are deterministic
```

## Appendix A: Scanned Procedure Base Coverage

This appendix records the local procedure bases found in the GDS reference shelf
after excluding test fixtures and `proc/test`. It is not a substitute for the
future checked inventory artifact, but it is the current requirements coverage
map.

### `catalog`

| procedure base |
| --- |
| `gds.alpha.graph.graphProperty` |
| `gds.alpha.graph.nodeLabel` |
| `gds.alpha.graph.sample.rwr` |
| `gds.beta.graph.export.csv` |
| `gds.beta.graph.generate` |
| `gds.beta.graph.project.subgraph` |
| `gds.beta.graph.relationships` |
| `gds.beta.model` |
| `gds.graph` |
| `gds.graph.deleteRelationships` |
| `gds.graph.export` |
| `gds.graph.export.csv` |
| `gds.graph.filter` |
| `gds.graph.generate` |
| `gds.graph.graphProperty` |
| `gds.graph.nodeLabel` |
| `gds.graph.nodeProperties` |
| `gds.graph.nodeProperty` |
| `gds.graph.project` |
| `gds.graph.project.cypher` |
| `gds.graph.relationship` |
| `gds.graph.relationshipProperties` |
| `gds.graph.relationshipProperty` |
| `gds.graph.relationships` |
| `gds.graph.removeNodeProperties` |
| `gds.graph.sample.cnarw` |
| `gds.graph.sample.rwr` |
| `gds.graph.streamNodeProperties` |
| `gds.graph.streamNodeProperty` |
| `gds.graph.streamRelationshipProperties` |
| `gds.graph.streamRelationshipProperty` |
| `gds.graph.writeNodeProperties` |
| `gds.graph.writeRelationship` |
| `gds.internal.graph.sizeOf` |
| `gds.model` |

### `centrality`

| procedure base |
| --- |
| `gds.alpha.closeness.harmonic` |
| `gds.alpha.hits` |
| `gds.articleRank` |
| `gds.articulationPoints` |
| `gds.beta.closeness` |
| `gds.beta.influenceMaximization.celf` |
| `gds.betweenness` |
| `gds.bridges` |
| `gds.closeness` |
| `gds.closeness.harmonic` |
| `gds.degree` |
| `gds.eigenvector` |
| `gds.hits` |
| `gds.influenceMaximization.celf` |
| `gds.pageRank` |

### `community`

| procedure base |
| --- |
| `gds.alpha.conductance` |
| `gds.alpha.maxkcut` |
| `gds.alpha.modularity` |
| `gds.alpha.scc` |
| `gds.alpha.sllpa` |
| `gds.alpha.triangles` |
| `gds.beta.k1coloring` |
| `gds.beta.kmeans` |
| `gds.beta.leiden` |
| `gds.beta.modularityOptimization` |
| `gds.conductance` |
| `gds.k1coloring` |
| `gds.kcore` |
| `gds.kmeans` |
| `gds.labelPropagation` |
| `gds.leiden` |
| `gds.localClusteringCoefficient` |
| `gds.louvain` |
| `gds.maxkcut` |
| `gds.modularity` |
| `gds.modularityOptimization` |
| `gds.scc` |
| `gds.sllpa` |
| `gds.triangleCount` |
| `gds.triangles` |
| `gds.wcc` |

### `embeddings`

| procedure base |
| --- |
| `gds.beta.graphSage` |
| `gds.beta.hashgnn` |
| `gds.beta.node2vec` |
| `gds.fastRP` |
| `gds.hashgnn` |
| `gds.node2vec` |

### `machine-learning`

| procedure base |
| --- |
| `gds.alpha.ml.splitRelationships` |
| `gds.alpha.pipeline.linkPrediction.addMLP` |
| `gds.alpha.pipeline.linkPrediction.addRandomForest` |
| `gds.alpha.pipeline.linkPrediction.configureAutoTuning` |
| `gds.alpha.pipeline.nodeClassification.addMLP` |
| `gds.alpha.pipeline.nodeClassification.addRandomForest` |
| `gds.alpha.pipeline.nodeClassification.configureAutoTuning` |
| `gds.alpha.pipeline.nodeRegression` |
| `gds.alpha.pipeline.nodeRegression.addLinearRegression` |
| `gds.alpha.pipeline.nodeRegression.addNodeProperty` |
| `gds.alpha.pipeline.nodeRegression.addRandomForest` |
| `gds.alpha.pipeline.nodeRegression.configureAutoTuning` |
| `gds.alpha.pipeline.nodeRegression.configureSplit` |
| `gds.alpha.pipeline.nodeRegression.create` |
| `gds.alpha.pipeline.nodeRegression.predict` |
| `gds.alpha.pipeline.nodeRegression.selectFeatures` |
| `gds.beta.pipeline.linkPrediction` |
| `gds.beta.pipeline.linkPrediction.addFeature` |
| `gds.beta.pipeline.linkPrediction.addLogisticRegression` |
| `gds.beta.pipeline.linkPrediction.addNodeProperty` |
| `gds.beta.pipeline.linkPrediction.addRandomForest` |
| `gds.beta.pipeline.linkPrediction.configureSplit` |
| `gds.beta.pipeline.linkPrediction.create` |
| `gds.beta.pipeline.linkPrediction.predict` |
| `gds.beta.pipeline.nodeClassification` |
| `gds.beta.pipeline.nodeClassification.addLogisticRegression` |
| `gds.beta.pipeline.nodeClassification.addNodeProperty` |
| `gds.beta.pipeline.nodeClassification.addRandomForest` |
| `gds.beta.pipeline.nodeClassification.configureSplit` |
| `gds.beta.pipeline.nodeClassification.create` |
| `gds.beta.pipeline.nodeClassification.predict` |
| `gds.beta.pipeline.nodeClassification.selectFeatures` |
| `gds.ml.kge.predict` |

### `misc`

| procedure base |
| --- |
| `gds` |
| `gds.alpha.scaleProperties` |
| `gds.beta.collapsePath` |
| `gds.beta.graph.relationships.toUndirected` |
| `gds.beta.listProgress` |
| `gds.collapsePath` |
| `gds.features.adjacencyPackingStrategy` |
| `gds.features.adjacencyPackingStrategy.reset` |
| `gds.features.enableAdjacencyCompressionMemoryTracking` |
| `gds.features.enableAdjacencyCompressionMemoryTracking.reset` |
| `gds.features.enableArrowDatabaseImport` |
| `gds.features.enableArrowDatabaseImport.reset` |
| `gds.features.pagesPerThread` |
| `gds.features.pagesPerThread.reset` |
| `gds.features.useMixedAdjacencyList` |
| `gds.features.useMixedAdjacencyList.reset` |
| `gds.features.usePackedAdjacencyList` |
| `gds.features.usePackedAdjacencyList.reset` |
| `gds.features.useReorderedAdjacencyList` |
| `gds.features.useReorderedAdjacencyList.reset` |
| `gds.features.useUncompressedAdjacencyList` |
| `gds.features.useUncompressedAdjacencyList.reset` |
| `gds.graph.relationships.indexInverse` |
| `gds.graph.relationships.toUndirected` |
| `gds.listProgress` |
| `gds.memory` |
| `gds.memory.summary` |
| `gds.scaleProperties` |

### `path-finding`

| procedure base |
| --- |
| `gds.allShortestPaths` |
| `gds.allShortestPaths.delta` |
| `gds.allShortestPaths.dijkstra` |
| `gds.alpha.allShortestPaths` |
| `gds.alpha.kSpanningTree` |
| `gds.bellmanFord` |
| `gds.beta.spanningTree` |
| `gds.beta.steinerTree` |
| `gds.bfs` |
| `gds.dag.longestPath` |
| `gds.dag.topologicalSort` |
| `gds.dfs` |
| `gds.kSpanningTree` |
| `gds.prizeSteinerTree` |
| `gds.randomWalk` |
| `gds.shortestPath.astar` |
| `gds.shortestPath.dijkstra` |
| `gds.shortestPath.yens` |
| `gds.spanningTree` |
| `gds.steinerTree` |

### `pipeline-catalog`

| procedure base |
| --- |
| `gds.beta.pipeline` |
| `gds.pipeline` |

### `similarity`

| procedure base |
| --- |
| `gds.alpha.knn.filtered` |
| `gds.alpha.nodeSimilarity.filtered` |
| `gds.knn` |
| `gds.knn.filtered` |
| `gds.nodeSimilarity` |
| `gds.nodeSimilarity.filtered` |

### `sysinfo`

| procedure base |
| --- |
| `gds.debug.sysInfo` |
| `gds.license.state` |
| `gds.version` |

## Acceptance Checklist

This requirements document is complete enough for the next TDD phase when:

| item | status |
| --- | --- |
| It states Tilehouse is optional, not mandatory. | done |
| It defines the multi-plane architecture beyond CSR. | done |
| It covers catalog, algorithms, similarity, embeddings, ML, pipelines, model catalog, misc, operations, and sysinfo. | done |
| It defines procedure support levels. | done |
| It defines memory estimate requirements. | done |
| It defines freshness and update requirements. | done |
| It defines data model semantics beyond topology. | done |
| It defines execution, cancellation, progress, cleanup, and atomicity requirements. | done |
| It defines operational durability, telemetry, admin, and versioning requirements. | done |
| It lists every scanned procedure base by module as current surface coverage. | done |
| It defines test requirements before implementation. | done |
| It identifies inventory/registry as the first implementation PR. | done |

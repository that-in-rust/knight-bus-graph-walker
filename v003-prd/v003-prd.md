

# Structure

``` text
We will follow a minto pyramid

L1 will be small
L2 will be longer
L3 will be even longer and detailed

```

# L1 PRD

```
Neo4j rewritten in Rust

- exact same APIs or surface area with ZERO changes so that the same code can be used
- identical architecture for OLTP queries
- lowest RAM custom storage formats for OLAP queries
  → REAL RAM: 50 GB data processed comfortably on 8 GB systems
- community edition hence single node
- OLAP will have some lag as compared to OLTP in terms of freshness AND THAT IS OK


to be specific
- OLTP data storage remains Neo4j-shaped.
- OLAP must account for direct and indirect RAM: heap, RSS, page cache, mmap residency, duplicate layouts, compaction buffers, snapshot build scratch, optional tail overlays, indexes, sidecars, model artifacts, spill, and algorithm intermediates.

- O_DIRECT-style explicit streaming gives the strictest RAM control for file-data reads
  - O_DIRECT bypasses file-data page cache for the eligible read path
  - We allocate explicit buffers: 64 KB, 1 MB, 64 MB — our choice
  - RSS still includes heap, stacks, direct buffers, scratch, sidecars, model artifacts, result buffers, kernel effects, and non-O_DIRECT paths
  - mmap remains useful for throughput but page-cache residency is OS-mediated
  - We CAN promise "PageRank requires X bytes of declared budget" only when the selected physical plan accounts for every plane and rejects if it cannot fit


```


# Comprehensive Surface Area OLAP in Requirement format for Neo4j rewrite

## Premise and conclusion

Premise is sound: if the goal is a Neo4j rewrite with unchanged external APIs,
the OLAP requirement is not "build CSR." The requirement is to support the
Neo4j-facing GDS/product surface while using lower-RAM OLAP storage and execution
plans than a general-purpose graph database.

Conclusion:

```text
Knight Bus v003 SHALL preserve Neo4j-shaped OLTP semantics and expose a
GDS-compatible OLAP surface through a multi-plane analytical architecture:

OLTP truth
  -> verified Projection Build Store / analytical pre-dataset
  -> immutable CSR snapshot backend
  -> columnar property/result/model sidecars
  -> bounded scratch/spill execution
  -> optional tail overlay only when freshness requirements justify the RAM cost
```

The current architecture choice is:

```text
FlatDualCsrBackend first.
Tilehouse later only when measurements prove it is needed.
Tail overlay optional, not foundational.
```

## Evidence basis used for these requirements

These requirements are grounded in local project docs and reference code:

| source | evidence used |
| --- | --- |
| `README.md` | Knight Bus already proves low-RAM, low-latency CSR-style walks on tracked datasets, but only for fixed walk workloads. |
| `v003-prd/Arch-01-CSR-multiple-options.md` | The preferred architecture is OLTP truth -> Projection Build Store -> immutable CSR/cell snapshots -> snapshot-as-of queries, with optional tail overlay. |
| `v003-diligence-01/diligence-codex-notes01.md` | Defines support levels, GDS inventory counts, architecture plane requirements, data/exec/memory/freshness requirements, and algorithm-family supportability. |
| `v003-diligence-01/tasks-diligence.md` | Converts the requirement framework into TDD phases: inventory, registry, Projection Build Store, catalog, flat backend, sidecars, memory planner, algorithms, freshness, optional Tilehouse. |
| `gitrefrepo/neo4j-src` | Confirms Neo4j OLTP is record/page-cache oriented; v003 must preserve OLTP semantics but should not force OLAP through the OLTP record path. |
| `gitrefrepo/neo4j-gds-src` | Confirms GDS already has a projected graph plane, graph catalog, CSR-like graph stores, compressed adjacency, facades, modes, and memory estimation. |
| `gitrefrepo/neo4j-*-driver-src` and `neo4j-docs-bolt-src` | Reinforce that application compatibility is an API/protocol/surface contract, not only storage layout compatibility. |
| `gitrefrepo/age-src`, `opencypher-src`, `antlr-grammars-v4-src`, `libcypher-parser-src` | Reinforce Cypher/parser/type-system compatibility concerns for the OLTP/query-facing layer. |
| `gitrefrepo/duckdb-src`, `apache-arrow-*`, `apache-parquet-format-src`, `clickhouse-src`, `datafusion-*` | Reinforce columnar/streaming/spill patterns for analytical facts, property columns, and bounded execution. |
| `gitrefrepo/cugraph-src`, `graphblas-src`, `lagraph-src`, `gapbs-src`, `gunrock-src`, `petgraph-src` | Reinforce that graph algorithms require topology plus algorithm state; CSR alone is not the whole execution model. |

Important evidence from `neo4j-gds-src`:

- `GraphDataScienceProcedures` exposes algorithms, graph catalog, model catalog,
  operations, and pipelines.
- `AlgorithmsProcedureFacade` splits algorithms into centrality, community,
  machine learning, miscellaneous, node embeddings, path finding, and similarity.
- Procedure annotations expose many `gds.*` variants; the local scan in the
  diligence notes found `174` procedure bases, `570` annotation rows, and `562`
  unique procedure names.
- `CSRGraphStoreFactory`, `CSRGraphStore`, `GraphStoreCatalog`,
  `GraphImporter`, `CompressedAdjacencyList`, `PackedAdjacencyList`, and
  `MemoryEstimationExecutor` show that GDS is already a projection/catalog/memory
  estimation system with CSR-like compressed adjacency.

Therefore:

```text
The fair target is not "can Knight Bus traverse a CSR graph?"
The fair target is "can Knight Bus expose the GDS-like OLAP product surface
with lower holistic RAM and explicit freshness semantics?"
```

## Requirement levels

Every known GDS-compatible procedure SHALL have exactly one support level.

| level | meaning |
| --- | --- |
| `P0Registered` | Known, classified, config-shaped, schema-shaped, and returns deterministic unsupported behavior if not implemented. |
| `P1ExactLowRam` | Implemented exactly and allowed to run when its memory estimate fits the requested budget. |
| `P2Later` | Intentionally deferred but known and registered. |
| `UnsupportedButRegistered` | Known to exist in GDS but not supported in this release. |

Requirement:

```text
WHEN a user calls any known GDS procedure
THEN Knight Bus SHALL either execute it or return deterministic
UnsupportedButRegistered behavior
AND SHALL NOT report it as an unknown procedure.
```

## Architecture plane requirements

### REQ-OLAP-PLANE-001: Procedure ABI plane

WHEN a GDS or GDS-like procedure call enters the system,
THEN Knight Bus SHALL resolve it through a procedure registry
AND classify:

```text
procedure name
family
mode
alpha/beta/deprecated status
config schema
output schema
support level
estimate behavior
error behavior
```

Acceptance:

- every checked inventory row has one registry entry;
- duplicate procedure names fail registry validation;
- unknown procedures and `UnsupportedButRegistered` procedures return distinct
  errors.

### REQ-OLAP-PLANE-002: Catalog plane

WHEN a user creates, lists, filters, samples, mutates, exports, writes, or drops
a graph projection,
THEN the catalog plane SHALL manage named projection state independently of the
physical topology backend.

The catalog SHALL record:

```text
user identity
database identity
graph name
projection generation
source transaction/generation watermark
freshness mode
node count
relationship count
schema
topology backend
property sidecars
result sidecars
model/pipeline links
memory estimate
creation/modification timestamps
```

### REQ-OLAP-PLANE-003: Projection Build Store / analytical pre-dataset

WHEN OLTP data is made available to OLAP,
THEN it SHALL first be normalized into a durable Projection Build Store before
being compiled into immutable CSR snapshots.

The Projection Build Store SHALL include:

```text
source generation
source tx watermark
dense node id mapping
relationship id / edge position mapping
node facts
relationship facts
label facts
relationship type facts
property facts
tombstones
schema facts
verification metadata
```

The Projection Build Store is:

```text
not the OLTP source of truth
not the final read-optimized CSR topology
the verified analytical IR used to build snapshots
```

### REQ-OLAP-PLANE-004: Topology backend plane

WHEN an algorithm needs graph adjacency,
THEN it SHALL depend on a topology backend trait, not directly on flat CSR,
Tilehouse, or any future physical layout.

Minimum operations:

```text
node_count
relationship_count
labels(node)
relationship_types()
neighbors(node, direction)
global_edges(direction)
typed_edges(direction, relationship_type_filter)
degree(node, direction, relationship_type_filter)
weighted_degree where configured
```

Backend policy:

| backend | requirement |
| --- | --- |
| `FlatDualCsrBackend` | SHALL be first implementation and correctness oracle. |
| `TilehouseBackend` | MAY be added for measured local compaction, page-window, or rebuild-lag wins. |
| `GraphLsmBackend` | MAY be explored only if snapshots plus bounded tail cannot satisfy a measured freshness requirement. |

### REQ-OLAP-PLANE-005: Columnar property plane

WHEN a projection or procedure requires labels, relationship types, weights,
features, node properties, relationship properties, graph properties, defaults,
or nulls,
THEN typed values SHALL be served from columnar sidecars without requiring a new
topology layout.

Required sidecar kinds:

```text
node labels
relationship types
node scalar properties
relationship scalar properties
graph properties
relationship weights
feature vectors
embedding outputs
algorithm result properties
derived relationship sidecars
```

### REQ-OLAP-PLANE-006: Bounded scratch/spill plane

WHEN an algorithm requires working state,
THEN it SHALL allocate or spill that state under an explicit execution budget.

Scratch classes include:

```text
scalar per-node arrays
vector per-node arrays
frontier bitsets
visited bitsets
distance vectors
priority queues
candidate top-k heaps
pair streams
embedding matrices
walk corpora
contracted graphs
model training batches
```

### REQ-OLAP-PLANE-007: Result sidecar and writeback plane

WHEN a procedure runs in `mutate` mode,
THEN its output SHALL be stored as a projected graph sidecar
AND catalog metadata SHALL expose the new property or relationship type.

WHEN a procedure runs in `write` mode,
THEN output SHALL be written through an OLTP-facing writeback bridge
AND write counts, property names, and failure semantics SHALL match the procedure
contract.

### REQ-OLAP-PLANE-008: Model and pipeline artifact plane

WHEN a procedure creates, trains, lists, predicts with, or drops a model or
pipeline,
THEN model and pipeline metadata SHALL live in an artifact plane separate from
topology and property columns.

The artifact plane SHALL support:

```text
model name
pipeline name
owner/database identity
feature schema
training configuration
metrics
artifact bytes
creation/modification metadata
versioning
list/drop/exists behavior
```

### REQ-OLAP-PLANE-009: Admin and operations plane

WHEN version, sysinfo, license, memory, progress, debug, feature, cancellation,
or job-status procedures are called,
THEN they SHALL route through an admin/procedure plane independent of topology.

## Data model requirements

### REQ-OLAP-DATA-001: ID mapping

WHEN data enters an OLAP projection,
THEN the system SHALL maintain stable mappings among:

```text
Neo4j-facing node ids
external keys where present
internal dense node ids
Neo4j-facing relationship ids or synthetic relationship ids
edge positions
source/target dense ids
projection-local filtered ids
generation identity
```

### REQ-OLAP-DATA-002: Labels

WHEN a projection selects node labels,
THEN label membership SHALL support union, intersection, exclusion, and
empty-label behavior through columnar or bitmap sidecars.

### REQ-OLAP-DATA-003: Relationship types

WHEN a projection selects relationship types,
THEN type selection SHALL apply consistently to adjacency streams, property
streams, counts, estimates, inverse indexes, and writeback targets.

### REQ-OLAP-DATA-004: Orientation

WHEN a projection or algorithm requests `NATURAL`, `REVERSE`, or `UNDIRECTED`
orientation,
THEN the logical graph view SHALL produce the correct edge direction without
rewriting base topology unless the physical plan explicitly chooses a derived
sidecar.

### REQ-OLAP-DATA-005: Relationship aggregation

WHEN projection config aggregates parallel relationships,
THEN aggregation semantics SHALL be represented in catalog metadata, logical
topology view, relationship counts, and property values.

### REQ-OLAP-DATA-006: Property defaults, nulls, and coercion

WHEN a property is missing, null, has a configured default, or requires numeric
coercion,
THEN the property plane SHALL resolve missing/default/null/coercion behavior
before execution begins
AND SHALL reject overflow, NaN, infinity, invalid dimensions, and invalid value
types where the procedure contract requires.

### REQ-OLAP-DATA-007: Schema reporting

WHEN graph schema is listed or streamed,
THEN node labels, relationship types, graph properties, node properties,
relationship properties, and result sidecars SHALL be reported from catalog and
sidecar metadata without scanning the full graph.

## Execution semantics requirements

### REQ-OLAP-EXEC-001: Physical plan explanation

WHEN a procedure is estimated or executed,
THEN the selected physical plan SHALL be explainable as one of:

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

### REQ-OLAP-EXEC-002: Streaming and backpressure

WHEN result rows are streamed,
THEN the system SHALL avoid materializing all rows in heap unless semantics
require global sorting, aggregation, or top-k selection.

### REQ-OLAP-EXEC-003: Determinism

WHEN row ordering is required or implied by the GDS surface,
THEN output order SHALL be deterministic.

WHEN ordering is not specified,
THEN the implementation SHALL document the order and keep tests stable.

WHEN a procedure has stochastic behavior,
THEN seeded execution SHALL be reproducible across flat CSR and any future
topology backend.

### REQ-OLAP-EXEC-004: Progress, cancellation, and cleanup

WHEN a long-running procedure executes,
THEN progress SHALL report procedure name, graph name, phase, work completed,
work remaining where knowable, memory plan, and cancellation state.

WHEN a user cancels a procedure or the server shuts down,
THEN the procedure SHALL stop at a safe checkpoint, release scratch, preserve
durable catalog consistency, and report cancelled/failed state deterministically.

### REQ-OLAP-EXEC-005: Atomicity

WHEN a mutate, write, train, pipeline, projection publish, or model operation
completes,
THEN the catalog/model/writeback state SHALL become visible atomically at the
procedure boundary.

Partial scratch, result sidecars, model artifacts, and catalog mutations SHALL
be cleaned up or marked failed deterministically.

### REQ-OLAP-EXEC-006: Concurrency and resource isolation

WHEN multiple procedures run concurrently,
THEN each procedure SHALL have independent budget accounting for heap, scratch,
spill, direct buffers, result sidecars, and model artifacts.

WHEN a procedure accepts `concurrency` or read-concurrency configuration,
THEN the planner SHALL translate that setting into bounded worker, I/O, and
scratch budgets
AND reject settings that violate the selected memory contract.

## Memory requirements

### REQ-OLAP-MEM-001: Holistic estimate object

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
tail_overlay_bytes
result_sidecar_bytes
model_artifact_bytes
writeback_bytes
spill_bytes
dominant_state
can_run
reason_if_rejected
```

### REQ-OLAP-MEM-002: Mmap honesty

WHEN a plan uses mmap,
THEN the estimate SHALL state that page-cache residency is OS-mediated
AND SHALL NOT claim deterministic RAM.

### REQ-OLAP-MEM-003: Strict-RAM execution

WHEN a user selects a strict memory budget,
THEN the planner SHALL choose explicit-stream/spill execution or reject the
procedure before execution.

### REQ-OLAP-MEM-004: 50 GB on 8 GB decision

WHEN the graph is 50 GB-class and the machine budget is 8 GB-class,
THEN every procedure estimate SHALL return:

```text
can_run
required_budget_bytes
dominant_state
execution_profile
freshness_mode
reason_if_rejected
```

The acceptance target is not "every algorithm must run on 8 GB." The acceptance
target is:

```text
Every algorithm must honestly say whether it can run on 8 GB,
why,
with which physical plan,
and with which freshness semantics.
```

## Freshness requirements

### REQ-OLAP-FRESH-001: Snapshot-as-of default

WHEN an OLAP query runs by default,
THEN it SHALL run against an immutable published snapshot
AND report the exact snapshot/source watermark used.

OLAP freshness lag versus OLTP is acceptable when explicitly reported.

### REQ-OLAP-FRESH-002: Projection Build Store sync

WHEN OLTP commits graph-relevant changes,
THEN the Projection Build Store SHALL ingest verifiable receipts and advance its
watermark monotonically.

WHEN a CSR snapshot is built,
THEN its manifest SHALL record the Projection Build Store generation and source
watermark used.

### REQ-OLAP-FRESH-003: Optional tail overlay

WHEN a query requires visibility beyond the published snapshot watermark,
THEN the system MAY choose a bounded tail overlay only if:

```text
tail facts are verified
tail bytes are estimated
merge buffers are estimated
conflict/tombstone handling is deterministic
the full plan fits the memory budget
```

Tail overlay SHALL NOT be required for durability or next-snapshot correctness.

### REQ-OLAP-FRESH-004: Tilehouse optionality

Tilehouse SHALL be introduced only if one of these measured triggers occurs:

| trigger | measurement |
| --- | --- |
| flat rebuild lag violates freshness SLO | rebuild time and update rate |
| bounded tail overlay exceeds memory budget | tail bytes and query merge cost |
| local traversals churn page cache badly | major faults, RSS, and latency |
| dirty-region compaction beats generation rebuild | compaction time and scratch bytes |

## GDS surface requirements

### REQ-OLAP-GDS-001: Full procedure inventory

WHEN the local GDS reference changes,
THEN an inventory generator SHALL detect added, removed, or renamed procedures
AND fail CI until support levels, config schemas, output schemas, and estimates
are updated.

The current working baseline from the diligence scan is:

| module | procedure bases | annotation rows | requirement |
| --- | ---: | ---: | --- |
| `catalog` | 35 | 54 | support graph projection lifecycle, size, schema, property streaming, mutation, export, and sampling |
| `centrality` | 15 | 96 | support degree/PageRank first; register high-risk centralities with explicit estimates |
| `community` | 26 | 154 | support WCC/SCC/triangle/k-core first; contraction-heavy algorithms require scratch artifacts |
| `embeddings` | 6 | 36 | require embedding/result/model artifact planes |
| `machine-learning` | 33 | 46 | require model/pipeline artifacts, feature schemas, metrics, and training budgets |
| `misc` | 28 | 39 | support property transforms, derived topology, graph generation, and utility procedures through appropriate planes |
| `path-finding` | 20 | 92 | require topology cursors, weight sidecars, path/frontier/priority state |
| `pipeline-catalog` | 2 | 6 | require pipeline artifact lifecycle |
| `similarity` | 6 | 44 | require candidate generation, filter pushdown, top-k budget, and feature/property planes |
| `sysinfo` | 3 | 3 | require admin/procedure plane |
| **total** | **174** | **570** | **all known rows registered; implementation support may vary by support level** |

### REQ-OLAP-GDS-002: Procedure modes

Every algorithm procedure SHALL classify supported modes:

| mode | requirement |
| --- | --- |
| `stream` | return rows without mutating catalog or OLTP |
| `stats` | return aggregate metrics without writing result sidecars |
| `mutate` | write result into projected graph sidecars |
| `write` | write result back through OLTP-facing bridge |
| `estimate` | return memory contract without executing algorithm work |
| `train` | create model artifact and metrics |
| `predict` | use model/pipeline artifacts and return or write predictions |

### REQ-OLAP-GDS-003: Catalog procedures

WHEN users call graph catalog procedures,
THEN Knight Bus SHALL support or register:

```text
native projection
Cypher projection compatibility path
list / exists / drop / size
schema listing
subgraph / filter projection
property stream
property mutate/drop
relationship transforms such as to-undirected and inverse index
graph export
graph sampling
graph generation
```

### REQ-OLAP-GDS-004: Centrality

Centrality requirements:

```text
degree: offsets + optional weights
PageRank / ArticleRank / Eigenvector: global edge stream + explicit vectors
HITS: forward/reverse scans + authority/hub vectors
closeness/harmonic: source batching + frontier/distance state
betweenness: sigma/delta/stack/predecessor/source batching
articulation/bridges: DFS low-link state
influence/CELF: simulation/sample/candidate estimates before support
```

PageRank SHALL NOT claim deterministic RAM under mmap. Strict-RAM PageRank SHALL
use explicit stream/spill/reject behavior.

### REQ-OLAP-GDS-005: Pathfinding

Pathfinding requirements:

```text
BFS / DFS: bounded visited/frontier/path/parent state
Dijkstra / A* / Delta-Stepping / Bellman-Ford: weight sidecar contract
all shortest paths: repeated source-sweep estimates
Yen's k-shortest paths: candidate path heaps and suppressed path state
random walk: deterministic seed behavior and walk buffers/corpora
spanning / Steiner trees: parent arrays, heaps, prizes/weights, writeback counts
DAG algorithms: cycle validation and deterministic cyclic-input errors
```

### REQ-OLAP-GDS-006: Community and structure

Community/structure requirements:

```text
WCC / SCC: component arrays, stacks, frontier/union state
triangle count / local clustering: sorted or intersection-capable cursors
k-core: degree arrays, peel queues, result sidecars
coloring: color arrays, conflict frontiers, tie-breaking
label propagation / SLPA: label state, distributions, seed, convergence
Louvain / Leiden: contracted graph scratch artifacts and modularity state
modularity / conductance: community sidecars and cut/internal edge streams
max-k-cut / k-means: vector/assignment/centroid estimates
```

### REQ-OLAP-GDS-007: Similarity

Similarity requirements:

```text
nodeSimilarity: adjacency/property overlap without unbudgeted O(n^2) materialization
KNN: feature/property vectors + candidate generation + top-k heap estimates
filtered similarity: push label/property filters before candidate expansion
```

### REQ-OLAP-GDS-008: Embeddings

Embedding requirements:

```text
FastRP: node_count * dimension * bytes_per_value output estimate
Node2Vec: walk corpus, context windows, RNG state, training batches
GraphSAGE: model artifact, sampled neighbor batches, feature columns
HashGNN: deterministic hash behavior and embedding output estimates
```

### REQ-OLAP-GDS-009: Machine learning and pipelines

ML/pipeline requirements:

```text
pipeline lifecycle: create/list/exists/drop/configure/add-step/train
node classification/regression: feature schema, target validation, splits, model candidates, metrics, predictions
link prediction: relationship splits, negative sampling, features, model artifacts
KGE: typed relationship sidecars, negative sampling, embedding matrices, model artifacts
splitRelationships: deterministic seed and split sidecars/properties
```

### REQ-OLAP-GDS-010: Model catalog

Model catalog procedures SHALL persist and expose:

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
list / exists / drop behavior
```

### REQ-OLAP-GDS-011: Miscellaneous, operations, and sysinfo

Miscellaneous and operations procedures SHALL route to the correct plane:

```text
scaleProperties -> columnar property plane
collapsePath -> derived relationship sidecar/topology artifact
toUndirected -> logical orientation or derived sidecar
graph generation -> bounded streaming build job
version/sysinfo/license/memory/features -> admin/procedure plane
progress/list/kill -> operations plane
```

## Operational requirements

### REQ-OLAP-OPS-001: Restart durability

WHEN the process restarts,
THEN catalog metadata, projection manifests, sidecars, result artifacts, model
artifacts, pipeline artifacts, and freshness receipts SHALL either load
successfully or fail with a recoverable corruption report.

### REQ-OLAP-OPS-002: Manifest versioning

WHEN a stored artifact is opened,
THEN the system SHALL validate format version, graph generation, checksum or
length metadata, feature flags, and required sidecar presence before serving it.

### REQ-OLAP-OPS-003: Compatibility versioning

WHEN GDS inventory is generated from a reference checkout,
THEN the inventory SHALL record reference repo path, branch or tag, commit hash
where available, scan command, excluded paths, and generation timestamp.

### REQ-OLAP-OPS-004: Telemetry

WHEN a procedure runs,
THEN telemetry SHALL record:

```text
estimate
selected plan
actual peak RSS where available
scratch bytes
spill bytes
page-fault counters where available
duration
row count
write count
failure reason
freshness watermark
```

### REQ-OLAP-OPS-005: Security and ownership boundary

WHEN a procedure accesses graph, model, pipeline, property, or writeback state,
THEN user/database context SHALL be carried through the call even if full
Neo4j-compatible authorization is implemented later.

### REQ-OLAP-OPS-006: Import/export boundaries

WHEN graph export, CSV export, database export, Arrow import, or equivalent
I/O-heavy procedures are called,
THEN the system SHALL treat them as bounded streaming jobs with explicit disk,
memory, progress, cancellation, and failure behavior.

## Non-goals and anti-requirements

The following are explicitly not v003 requirements:

```text
Do not require Tilehouse before the flat backend proves ABI/catalog/memory/kernel contracts.
Do not claim mmap gives deterministic RAM.
Do not require zero-lag OLAP by default.
Do not store every algorithm's intermediate state as a persistent topology format.
Do not treat "CSR exists" as equivalent to "GDS surface is supported."
Do not materialize all result rows, all candidate pairs, or all embeddings in heap unless the estimate explicitly permits it.
```

## Acceptance criteria

This PRD section is satisfied when:

| acceptance item | requirement |
| --- | --- |
| Inventory | every scanned GDS procedure row is known and has a support level |
| ABI | procedure registry validates modes, config schema, output schema, support level, and estimate behavior |
| Catalog | named graph projections are scoped by user/database/name and expose freshness watermarks |
| Build store | Projection Build Store manifests source generation, watermarks, facts, dense IDs, and verification metadata |
| Topology | FlatDualCsrBackend implements the topology trait and acts as correctness oracle |
| Properties | labels, types, weights, scalar/vector properties, graph properties, and result sidecars are columnar |
| Memory | every estimate includes heap/RSS/page-cache/direct/topology/property/scratch/tail/result/model/writeback/spill bytes |
| Freshness | snapshot-only is default; bounded tail is optional and budgeted |
| Algorithms | first-tier degree, BFS/DFS, WCC/SCC, triangle/k-core, and PageRank pass oracle and memory-budget tests |
| Operations | progress, cancellation, cleanup, atomic publish, restart recovery, telemetry, and manifest version checks exist |

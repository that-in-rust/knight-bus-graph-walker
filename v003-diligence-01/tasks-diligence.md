# v003 Diligence 01: TDD Task List for Low-RAM OLAP + GDS Surface

This diligence note turns the v003 architecture direction into a test-first task
list.

The target is not "add a faster PageRank." The target is:

```text
Neo4j-shaped OLTP stays fixed.
OLAP compiles verified projection facts into low-RAM immutable snapshots.
The external GDS-style surface is preserved.
Every claim is backed by tests before implementation.
```

## 0. Codex-note correction

`diligence-codex-notes01.md` changes the implementation order. The diligence
target is no longer "build Cellular CSR first." The target is a low-RAM OLAP
plane that can prove GDS compatibility and 8 GB behavior before any exotic
physical layout becomes mandatory.

Core facts from the Codex notes:

| fact | consequence for this task list |
| --- | --- |
| "Lowest RAM" means heap, RSS, page cache, mmap residency, build/compaction peaks, scratch, result sidecars, and operator-visible machine pressure. | Every estimate and acceptance test must account for holistic memory, not just Rust heap. |
| Neo4j GDS already uses a projected graph plane with CSR-like compressed adjacency, catalog identity, schema, properties, and memory estimation. | Knight Bus must compete against GDS projection semantics, not Cypher-over-record-store semantics. |
| The original "13 persistent layout families" idea is overbuilt. | Keep a tiny number of durable byte families; move algorithm-specific state into scratch/result/model sidecars. |
| Flat compressed dual CSR is closest to the current proof and should remain the first backend and oracle. | Do not require Tilehouse before the flat backend proves the procedure ABI, catalog, estimates, and algorithm contracts. |
| Tilehouse is useful only if measured triggers appear: rebuild lag, tail-overlay cost, page-cache churn, or local compaction wins. | Tilehouse becomes an optional physical backend phase, not the foundational premise. |
| `compio`/`io_uring` is more relevant to append/publish pipelines than to the mmap-heavy OLAP scan hot path. | Borrow journal/publish discipline; do not rewrite the OLAP read path around async I/O without evidence. |

Revised thesis:

```text
GDS-compatible procedure ABI
  -> named projection catalog
  -> Projection Build Store / analytical facts
  -> topology backend trait
  -> FlatDualCsrBackend first
  -> property/result/model sidecars
  -> bounded algorithm scratch
  -> optional TilehouseBackend only when measurements justify it
```

The implementation rule is:

```text
First prove the ABI, projection catalog, flat-CSR backend, memory contracts,
and first GDS kernels. Then add Tilehouse only if the flat snapshot pipeline
cannot satisfy freshness, rebuild, or page-cache requirements.
```

## 1. Core facts

### PRD facts

- v003 wants a Neo4j rewrite in Rust with the same API/surface area and zero
  application-code changes.
- OLTP storage remains Neo4j-shaped.
- OLAP storage is the optimization target.
- The RAM promise is holistic: heap, RSS, page cache, mmap residency, duplicate
  layouts, compaction scratch, snapshot build scratch, optional tail overlays,
  indexes, result sidecars, model artifacts, and algorithm intermediates all
  count.
- The scale target is 50 GB-class data on 8 GB systems.
- O_DIRECT-style explicit streaming is preferred for deterministic strict-RAM
  plans because it bypasses page cache and makes buffer sizes explicit.
- `mmap` remains valid for throughput-oriented plans, but it cannot honestly
  claim deterministic RAM because page-cache residency is OS-mediated.
- OLAP freshness lag versus OLTP is acceptable when queries clearly report their
  snapshot/projection watermark.

### Current Knight Bus facts

Current Knight Bus is a compact flat immutable dual-CSR runtime:

```text
snapshot/
  manifest.json
  node_table.bin
  strings.bin
  forward.offsets.bin
  forward.peers.bin
  reverse.offsets.bin
  reverse.peers.bin
  key_index.bin
```

Current snapshot writing is centered on:

```rust
pub trait SnapshotArtifactWriter {
    fn write_snapshot_artifacts(
        &self,
        graph_data: &NormalizedGraphData,
        output_dir: &Path,
    ) -> Result<SnapshotBuildSummary, KnightBusError>;
}
```

The current manifest declares:

```rust
pub struct SnapshotManifest {
    pub version: u32,
    pub node_id_width: u32,
    pub adjacency_offset_width: u32,
    pub node_count: u32,
    pub edge_count: u64,
    pub key_mode: String,
    pub storage_mode: String,
    pub forward_offsets: String,
    pub forward_peers: String,
    pub reverse_offsets: String,
    pub reverse_peers: String,
    pub node_table: String,
    pub strings: String,
    pub key_index: String,
}
```

The current runtime is walk-focused:

```rust
pub trait WalkQueryRuntime {
    fn query_entity_neighbors(
        &self,
        entity_key: &NodeKey,
        direction: WalkDirection,
        hops: HopCount,
    ) -> Result<QueryResult, KnightBusError>;

    fn query_keys_for_family(
        &self,
        entity_key: &NodeKey,
        family: QueryFamily,
    ) -> Result<Vec<String>, KnightBusError>;

    fn all_node_keys(&self) -> Result<Vec<NodeKey>, KnightBusError>;

    fn snapshot_size_bytes(&self) -> u64;
}
```

The current query families are:

```rust
pub enum QueryFamily {
    ForwardOne,
    BackwardOne,
    ForwardTwo,
    BackwardTwo,
}
```

This proves the flat CSR seed works for static 1-hop/2-hop walks, but it does
not yet prove the v003 GDS surface.

### GDS reference facts

The local Neo4j GDS reference has a facade shaped like:

```java
public interface GraphDataScienceProcedures {
    Log log();
    AlgorithmsProcedureFacade algorithms();
    GraphCatalogProcedureFacade graphCatalog();
    ModelCatalogProcedureFacade modelCatalog();
    OperationsProcedureFacade operations();
    PipelinesProcedureFacade pipelines();
    DeprecatedProceduresMetricService deprecatedProcedures();
}
```

Algorithms are grouped by facades:

```java
public class AlgorithmsProcedureFacade {
    public CentralityProcedureFacade centrality();
    public CommunityProcedureFacade community();
    public MachineLearningProcedureFacade machineLearning();
    public MiscellaneousProcedureFacade miscellaneous();
    public NodeEmbeddingsProcedureFacade nodeEmbeddings();
    public PathFindingProcedureFacade pathFinding();
    public SimilarityProcedureFacade similarity();
}
```

The graph catalog surface includes named projections, list/drop, native/cypher
project, subgraph project, memory sizing, property streaming, and property
mutation/writeback.

Individual procedures expose mode variants. PageRank stream is representative:

```java
@Procedure(value = "gds.pageRank.stream", mode = READ)
public Stream<CentralityStreamResult> stream(
    @Name(value = "graphName") String graphName,
    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
)

@Procedure(value = "gds.pageRank.stream.estimate", mode = READ)
public Stream<MemoryEstimateResult> estimate(
    @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
    @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
)
```

Therefore, "full GDS surface" means at least:

```text
catalog + algorithms + modes + configs + output schemas + memory estimates
```

not merely graph algorithm kernels.

Additional Codex-note facts from the local GDS scan:

- GDS builds a `CSRGraphStore` from projected nodes, relationships, schema, and
  import results.
- GDS projected stores include ID maps, relationship stores, node properties,
  graph properties, schema, capabilities, and timestamps.
- GDS publishes projected graphs through a user/database/name catalog.
- GDS compressed adjacency uses page/degree/offset-style structures that are
  close to a compressed CSR family.
- The working scan found `174` procedure bases, `570` annotation rows, and
  `562` unique procedure names; the checked inventory remains the authority.
- Every known GDS procedure must be registered as executable, later, or
  deterministic `UnsupportedButRegistered`; unknown and known-unsupported must
  not collapse into the same error.

### Revised architecture-plane facts

The durable OLAP plane should be small:

```text
1. Projection Build Store / analytical fact plane
   - source tx/generation watermarks
   - dense ID map
   - normalized node/edge/property facts
   - projection schema inputs

2. CompressedDualCsrBaseV1 topology plane
   - forward/reverse offsets and peers
   - optional compression/packing metadata
   - immutable snapshot generation

3. Columnar sidecar planes
   - labels, relationship types, weights, scalar/vector properties
   - result sidecars, writeback sidecars, model/pipeline artifacts

4. Bounded scratch/spill plane
   - vectors, frontiers, candidate heaps, walk corpora, contracted graphs
   - lifecycle scoped to job/generation
```

Tilehouse is a candidate topology backend, not a required architecture root:

```text
FlatDualCsrBackend = first implementation and correctness oracle.
TilehouseBackend = add only for measured local compaction/page-window wins.
GraphLsmBackend = later only if snapshot + bounded tail is insufficient.
```

## 2. Rubber-duck diligence

### Duck: Are CSR tiles enough to claim GDS compatibility?

No. CSR tiles are the storage substrate. GDS compatibility additionally needs
procedure names, config parsing, graph catalog semantics, output schemas, modes,
memory estimates, and write/mutate behavior.

### Duck: Did the Codex notes change the physical-storage priority?

Yes. Flat compressed dual CSR remains the first backend because it is closest to
the current proof and to GDS compressed adjacency. Tilehouse should be developed
after the ABI/catalog/flat backend/memory contract is proven, unless a measured
freshness or page-cache failure forces it earlier.

### Duck: How many durable OLAP formats should v003 create?

As few as possible:

```text
base topology + property sidecars + bounded scratch/result/model artifacts
```

Do not create one persistent graph layout per algorithm family. Each new durable
format must prove a RAM-saving reason beyond this base split.

### Duck: Should implementation start with tile partitioning?

No. Start with a procedure inventory and compatibility registry. Otherwise we
could build beautiful tiles and still miss the public surface.

### Duck: Does test-first mean unit tests only?

No. This needs a ladder:

1. registry golden tests;
2. snapshot format tests;
3. tile equivalence tests against flat CSR;
4. algorithm correctness tests against tiny oracle graphs;
5. GDS procedure shape tests against the reference inventory;
6. memory-contract tests;
7. update/freshness tests;
8. 50GB/8GB diligence simulations.

### Duck: What is the first scary failure?

The scary failure is not PageRank being slow. The scary failure is silently
reducing the GDS surface while believing we are compatible. That must be caught
by generated procedure inventory tests before storage work dominates the plan.

### Duck: What is the second scary failure?

The second scary failure is "low Rust heap" while OS page cache, mmap residency,
optional tail overlays, result sidecars, model artifacts, or algorithm vectors
exceed the 8 GB machine. The memory tests must account for holistic RSS and
page-cache policy, not just allocator bytes.

### Duck: What is the creative move?

Treat GDS as an ABI and low-RAM topology backends as physical plan targets:

```text
GDS procedure call
  -> compatibility registry
  -> graph projection catalog
  -> Projection Build Store / analytical facts
  -> logical graph view
  -> memory contract
  -> physical plan:
       flat CSR mmap scan
       global O_DIRECT stream
       tile-local window, if Tilehouse is selected
       multi-tile wavefront, if Tilehouse is selected
       spillable vector/tape
  -> stream/stats/mutate/write result mode
```

This makes every physical layout prove itself against the whole API surface
instead of becoming a disconnected storage experiment.

## 3. TDD strategy

### Rule 1: every implementation task starts with a failing test

Each task below has a test-first acceptance criterion. If a task cannot name its
test, it is not ready for implementation.

### Rule 2: compatibility tests precede performance tests

The v003 PRD says exact same surface area. A fast non-compatible system fails.

### Rule 3: flat CSR remains the oracle

Flat CSR is not a throwaway seed. It is the first backend, the snapshot oracle,
and the baseline every later topology backend must match.

### Rule 4: Neo4j/GDS reference is the API oracle

The local `gitrefrepo/neo4j-gds-src` tree is the diligence source for procedure
names, modes, facade categories, config shape, and output shape.

### Rule 5: memory estimates are executable contracts

`estimate` should not be marketing text. It should be derived from:

```text
topology bytes + sidecar bytes + frontier/vector bytes + spill buffers
+ result/model/writeback bytes + optional tail-overlay bytes
+ compaction/build scratch + planned page-cache policy
```

### Rule 6: Tilehouse is measurement-gated

Tilehouse tasks remain in this plan because they are a serious candidate for
bounded local compaction and page-window planning. They should not be treated as
required for MVP unless one of these tests fails for the flat snapshot pipeline:

| trigger | measurement |
| --- | --- |
| flat rebuild lag violates freshness SLO | rebuild time and update rate |
| global tail overlay exceeds memory budget | overlay bytes and query merge cost |
| local traversals churn page cache badly | major faults, RSS, and latency |
| dirty-region compaction beats full generation rebuild | compaction time and scratch bytes |

## 4. Task list

### Phase 0: preserve current behavior

#### T0.1 Baseline test snapshot contracts

Write tests that lock down current flat CSR files, manifest fields, and current
walk outputs.

Acceptance tests:

- `tests/snapshot_manifest_contract.rs`
  - builds a fixture snapshot;
  - asserts required files exist;
  - asserts `storage_mode == "immutable_dual_csr"`;
  - asserts forward/reverse offsets and peer file sizes.
- Existing `tests/library_contract.rs` continues to pass.

Why first:

```text
Cellular CSR must be additive. We must not break the current proven seed.
```

#### T0.2 Baseline RSS test harness

Create a test helper that can run a command/function and report peak RSS in a
stable JSON shape.

Acceptance tests:

- `tests/rss_contract.rs`
  - runs a small build/query;
  - asserts `peak_rss_bytes > 0`;
  - asserts report includes RSS source and phase.

Why:

```text
v003 is a RAM promise. RAM reporting must become a first-class test target.
```

### Phase 0B: projection-plane requirements from Codex notes

#### T0B.1 Freeze support levels and procedure inventory contract

Promote the Codex-note support-level model into a checked contract before
building more storage.

Support levels:

```rust
pub enum GdsSupportLevel {
    P0Registered,
    P1ExactLowRam,
    P2Later,
    UnsupportedButRegistered,
}
// Every known GDS procedure receives exactly one support level.
```

Acceptance tests:

- inventory summary records the reference source, scan command, and scan time;
- inventory includes the current working counts as metadata:
  - `174` procedure bases;
  - `570` annotation rows;
  - `562` unique procedure names;
- every inventory row maps to exactly one support level;
- unknown procedure errors differ from known `UnsupportedButRegistered` errors.

#### T0B.2 Define the Projection Build Store contract

The Projection Build Store is the analytical pre-dataset used to compile
immutable CSR snapshots. It is not the OLTP truth and not the final read
optimized topology.

Symbols to add:

```rust
pub struct ProjectionBuildStoreManifest {
    pub version: u32,
    pub source_generation: u64,
    pub source_tx_watermark: u64,
    pub node_fact_count: u64,
    pub relationship_fact_count: u64,
    pub property_fact_count: u64,
    pub dense_id_map_path: String,
    pub schema_path: String,
}
// Durable analytical-fact manifest used to build CSR snapshots.

pub struct ProjectionFactReceipt {
    pub tx_id: u64,
    pub op_kind: ProjectionFactOpKind,
    pub entity_id: u64,
}
// Verifiable receipt connecting OLTP changes to the analytical build plane.
```

Acceptance tests:

- manifest round-trips and validates source generation/watermark fields;
- fixture OLTP facts build the same dense IDs on repeated runs;
- CSR snapshot manifest records the Projection Build Store generation used;
- a stale or mismatched build-store watermark refuses to publish a snapshot.

#### T0B.3 Add atomic projection publish semantics

GDS projection import publishes to a catalog after building. Knight Bus needs
the same atomic boundary so queries never observe half-built topology/sidecars.

Acceptance tests:

- building a projection writes to a staging generation first;
- successful publish atomically updates catalog metadata;
- failed publish leaves the previous projection generation visible;
- restart after partial publish reports recoverable staging garbage rather than
  serving it.

### Phase 1: GDS surface inventory before implementation

#### T1.1 Generate GDS procedure inventory

Create a script that scans Neo4j GDS reference procedure annotations and facade
interfaces into a checked-in inventory.

Proposed artifact:

```text
v003-diligence-01/gds-procedure-inventory.tsv
```

Columns:

```text
procedure_name
category
mode
estimate_name
source_file
config_args
result_type
status
```

Acceptance tests:

- `tests/gds_inventory_contract.rs`
  - reads the inventory;
  - asserts every row starts with `gds.`;
  - asserts `gds.pageRank.stream` exists;
  - asserts `gds.pageRank.stream.estimate` exists;
  - asserts graph catalog rows exist;
  - asserts categories include catalog, centrality, community, pathfinding,
    similarity, embeddings, ml, miscellaneous, operations, pipelines.

Implementation note:

```text
The initial inventory may be generated by a script, but the test reads the
checked-in result so CI is deterministic.
```

#### T1.2 Add a GDS procedure registry

Symbols to add:

```rust
pub enum GdsProcedureMode {
    Stream,
    Stats,
    Mutate,
    Write,
    Estimate,
}
// Represents GDS mode variants consistently across categories.

pub enum GdsProcedureFamily {
    Catalog,
    Centrality,
    Community,
    PathFinding,
    Similarity,
    Embedding,
    MachineLearning,
    Miscellaneous,
    Operations,
    Pipelines,
    ModelCatalog,
}
// Groups procedure compatibility by GDS facade family.

pub struct GdsProcedureSpec {
    pub name: &'static str,
    pub family: GdsProcedureFamily,
    pub mode: GdsProcedureMode,
    pub estimate_name: Option<&'static str>,
    pub result_shape: &'static [&'static str],
    pub support_level: GdsSupportLevel,
}
// Static compatibility declaration for one public procedure variant.

pub fn gds_procedure_specs() -> &'static [GdsProcedureSpec];
// Returns the checked compatibility registry.
```

Acceptance tests:

- registry has a matching spec for every inventory row marked in-scope;
- no duplicate procedure names;
- each non-estimate algorithm mode links to the expected estimate procedure;
- missing kernels return `UnsupportedButRegistered`, not unknown procedure;
- each spec has exactly one support level.

### Phase 2: graph projection/catalog semantics

#### T2.1 Add projection spec types

Symbols to add:

```rust
pub struct GraphProjectionSpec {
    pub graph_name: String,
    pub node_projection: ProjectionSelector,
    pub relationship_projection: ProjectionSelector,
    pub orientation: RelationshipOrientation,
    pub node_properties: Vec<PropertyKey>,
    pub relationship_properties: Vec<PropertyKey>,
}
// Parsed representation of gds.graph.project / cypher project intent.

pub enum RelationshipOrientation {
    Natural,
    Reverse,
    Undirected,
}
// Controls whether CSR uses forward, reverse, or symmetric adjacency.

pub struct GraphProjectionHandle {
    pub graph_name: String,
    pub user: String,
    pub database: String,
    pub snapshot_generation: u64,
    pub source_tx_watermark: u64,
    pub freshness_mode: FreshnessMode,
    pub node_count: u64,
    pub relationship_count: u64,
    pub memory_estimate: MemoryEstimate,
}
// Catalog handle returned to algorithms.

pub enum FreshnessMode {
    SnapshotOnly,
    SnapshotPlusBoundedTail,
    ForceRefreshBeforeRun,
    RejectUntilRefresh,
}
// Explicitly records whether a query is snapshot-as-of or includes a tail.
```

Acceptance tests:

- `gds.graph.project` fixture maps to `GraphProjectionSpec`;
- `orientation: "UNDIRECTED"` doubles or symmetrizes relationships in the
  logical view without changing base CSR files;
- catalog list/drop/existence behavior matches GDS expectations for named
  user/database/graph handles;
- list/stream output exposes source generation, source tx watermark, and
  freshness mode.

#### T2.2 Add projection memory estimation

Symbols to add:

```rust
pub struct MemoryEstimate {
    pub required_bytes: u64,
    pub heap_bytes: u64,
    pub rss_budget_bytes: u64,
    pub page_cache_bytes: u64,
    pub page_cache_unbounded_risk: bool,
    pub direct_io_buffer_bytes: u64,
    pub topology_bytes: u64,
    pub property_sidecar_bytes: u64,
    pub algorithm_state_bytes: u64,
    pub scratch_bytes: u64,
    pub tail_overlay_bytes: u64,
    pub result_sidecar_bytes: u64,
    pub model_artifact_bytes: u64,
    pub writeback_bytes: u64,
    pub spill_bytes: u64,
}
// Holistic memory contract for projection and algorithm execution.
```

Acceptance tests:

- estimates include all fields;
- estimate for flat CSR projection does not include duplicate full topology;
- mmap plans set `page_cache_unbounded_risk == true` unless bounded by measured
  residency policy;
- strict-RAM plans select explicit stream/spill execution or reject before
  execution;
- estimate for Tilehouse projection includes tile metadata and sidecar indexes
  only when Tilehouse is selected;
- 50GB/8GB scenario has an explicit pass/fail decision with dominant state,
  execution profile, freshness mode, and rejection reason.

#### T2.3 Add columnar property-plane contracts

Symbols to add:

```rust
pub enum SidecarKind {
    NodeLabel,
    RelationshipType,
    NodeProperty,
    RelationshipProperty,
    GraphProperty,
    Weight,
    VectorFeature,
}
// All non-topology values are sidecars, not new topology layouts.

pub struct SidecarColumnManifest {
    pub name: String,
    pub kind: SidecarKind,
    pub value_type: PropertyValueType,
    pub null_bitmap_path: Option<String>,
    pub data_path: String,
    pub generation: u64,
}
// Typed columnar sidecar required by GDS projections and algorithms.
```

Acceptance tests:

- label membership supports union, intersection, exclusion, and empty-label
  behavior;
- relationship type filters apply to adjacency streams and property streams;
- missing/null/default property behavior is resolved before execution begins;
- numeric weight/feature coercion validates overflow, NaN, infinity, and invalid
  values;
- schema reporting reads catalog/sidecar metadata without scanning the full
  graph.

### Phase 3: abstract graph access before tilehouse

#### T3.1 Introduce algorithm-facing adjacency trait

Symbols to add:

```rust
pub trait GraphAdjacencyRuntime {
    fn node_count(&self) -> u64;

    fn relationship_count(&self) -> u64;

    fn labels(&self, node: DenseNodeId) -> Result<LabelSet<'_>, KnightBusError>;

    fn relationship_types(&self) -> Result<RelationshipTypeSet<'_>, KnightBusError>;

    fn neighbors(
        &self,
        node: DenseNodeId,
        direction: WalkDirection,
    ) -> Result<NeighborCursor<'_>, KnightBusError>;

    fn global_edges(
        &self,
        direction: WalkDirection,
    ) -> Result<EdgeCursor<'_>, KnightBusError>;

    fn typed_edges(
        &self,
        direction: WalkDirection,
        relationship_types: RelationshipTypeFilter<'_>,
    ) -> Result<EdgeCursor<'_>, KnightBusError>;

    fn degree(
        &self,
        node: DenseNodeId,
        direction: WalkDirection,
        relationship_types: RelationshipTypeFilter<'_>,
    ) -> Result<u64, KnightBusError>;
}
// Minimal substrate needed by GDS algorithms without exposing file layout.
```

Acceptance tests:

- `MmapWalkRuntime` implements `GraphAdjacencyRuntime`;
- neighbor results match `WalkQueryRuntime`;
- global edge cursor returns exactly the flat CSR edge set;
- reverse global cursor matches reverse CSR;
- relationship-type filtered stream matches fixture oracle;
- undirected logical neighbors do not rewrite base topology.

Why before tiles:

```text
Algorithms should target a logical graph, not the storage format.
```

#### T3.2 Add graph oracle tests

Acceptance tests:

- fixture graph produces identical adjacency via:
  - normalized in-memory graph;
  - current flat CSR runtime;
  - future tilehouse runtime.
- property-based small graphs compare all implementations.

#### T3.3 Add flat compressed dual CSR backend as first physical backend

Symbols to add:

```rust
pub struct FlatDualCsrBackend {
    pub manifest: SnapshotManifest,
    pub io_policy: GraphIoPolicy,
}
// First GDS topology backend over the existing immutable dual CSR files.

pub enum GraphIoPolicy {
    MmapThroughput,
    ExplicitDirectStream,
}
// Makes page-cache honesty visible in planning.
```

Acceptance tests:

- flat backend can serve the projection/catalog handle without Tilehouse;
- `MmapThroughput` estimates include page-cache risk;
- `ExplicitDirectStream` estimates include explicit direct buffer bytes;
- algorithms can run through the backend trait without knowing the storage mode.

### Phase 4: Cellular CSR Tilehouse storage

Phase status: measurement-gated. Do not start this phase before Phases 0B-3
unless a flat-backend test proves that rebuild lag, page-cache churn, or update
freshness cannot meet the v003 target.

#### T4.1 Add tile metadata types

Symbols to add:

```rust
pub struct TileId(pub u32);
// Stable identifier for a CSR tile/cell.

pub struct CsrTilePassport {
    pub tile_id: TileId,
    pub local_node_count: u32,
    pub local_edge_count: u64,
    pub boundary_edge_count: u64,
    pub dense_id_start: u32,
    pub dense_id_end_exclusive: u32,
    pub dirty_tx_watermark: Option<u64>,
}
// Small metadata record used for planning, freshness, and compaction.

pub struct TilehouseManifest {
    pub version: u32,
    pub generation: u64,
    pub tile_count: u32,
    pub node_count: u64,
    pub edge_count: u64,
    pub boundary_edge_count: u64,
    pub passports: Vec<CsrTilePassport>,
}
// Durable top-level description of the Cellular CSR snapshot.
```

Acceptance tests:

- manifest round-trips through JSON;
- passports cover every dense id exactly once;
- tile edge counts sum to total internal + boundary edge count;
- invalid overlapping tile ranges fail validation.

#### T4.2 Add tile partitioner

Symbols to add:

```rust
pub trait TilePartitioner {
    fn partition(
        &self,
        graph: &NormalizedGraphData,
        budget: TileBudget,
    ) -> Result<TileAssignment, KnightBusError>;
}
// Chooses cell boundaries before physical write.

pub struct TileBudget {
    pub max_nodes_per_tile: u32,
    pub max_edges_per_tile: u64,
    pub max_tile_bytes: u64,
}
// Keeps each tile under a planning/RAM budget.
```

Acceptance tests:

- every node receives exactly one tile;
- no tile exceeds configured node/edge budget;
- boundary ratio is reported;
- deterministic partitioner returns stable output for stable input.

#### T4.3 Add tilehouse writer and reader

Symbols to add:

```rust
pub struct CellularCsrSnapshotWriter<P: TilePartitioner> {
    pub partitioner: P,
    pub tile_budget: TileBudget,
}
// Writes tilehouse artifacts instead of one flat CSR file set.

impl<P: TilePartitioner> SnapshotArtifactWriter for CellularCsrSnapshotWriter<P> {
    fn write_snapshot_artifacts(
        &self,
        graph_data: &NormalizedGraphData,
        output_dir: &Path,
    ) -> Result<SnapshotBuildSummary, KnightBusError>;
}

pub struct TilehouseRuntime {
    tilehouse: CellularCsrTilehouse,
}
// Runtime adapter implementing GraphAdjacencyRuntime over tiles.
```

Acceptance tests:

- tilehouse writer creates manifest, tile CSR files, and boundary files;
- tilehouse reader validates file sizes and passport coverage;
- tilehouse adjacency matches flat CSR for all fixture queries;
- snapshot size overhead is reported.

### Phase 5: sidecars for labels, types, weights, properties

#### T5.1 Add columnar sidecar manifest

Symbols to add:

```rust
pub enum SidecarKind {
    NodeLabel,
    RelationshipType,
    NodeProperty,
    RelationshipProperty,
    Weight,
}
// Declares sidecar semantic role.

pub struct SidecarColumnManifest {
    pub name: String,
    pub kind: SidecarKind,
    pub physical_type: PhysicalValueType,
    pub path: PathBuf,
    pub nulls_path: Option<PathBuf>,
}
// Describes one tile-local column.
```

Acceptance tests:

- node labels from CSV are queryable without heap materializing all labels;
- relationship type filters select the correct edge subset;
- missing property behavior matches GDS config expectations;
- sidecar bytes are included in memory estimates.

### Phase 6: algorithm execution contract

#### T6.1 Add procedure execution trait

Symbols to add:

```rust
pub trait GdsProcedure {
    fn spec(&self) -> &'static GdsProcedureSpec;

    fn estimate(
        &self,
        graph: &GraphProjectionHandle,
        config: &GdsConfig,
    ) -> Result<MemoryEstimate, KnightBusError>;

    fn execute(
        &self,
        graph: &GraphProjectionHandle,
        config: &GdsConfig,
        mode: GdsProcedureMode,
    ) -> Result<GdsResultStream, KnightBusError>;
}
// One testable contract for stream/stats/mutate/write/estimate.
```

Acceptance tests:

- every registered procedure has an implementation object or an explicit
  registered unsupported stub;
- every implementation supports `estimate`;
- `execute(..., Estimate)` never performs algorithm work;
- unsupported algorithms preserve procedure shape and produce deterministic
  errors.

#### T6.2 Add execution physical plan

Symbols to add:

```rust
pub enum PhysicalGraphPlan {
    TileLocal { tiles: Vec<TileId> },
    TileWavefront { start_tiles: Vec<TileId> },
    GlobalDirectStream,
    SpillableVectorTape,
}
// Planner choice for bounded-memory execution.

pub struct ExecutionBudget {
    pub max_rss_bytes: u64,
    pub direct_io_buffer_bytes: u64,
    pub spill_bytes: u64,
}
// Runtime budget enforced by algorithms.
```

Acceptance tests:

- local BFS chooses `TileLocal` or `TileWavefront`;
- PageRank chooses `GlobalDirectStream` or `SpillableVectorTape`;
- planner rejects plans above budget before execution;
- plan explains each memory component.

### Phase 7: first correctness kernels

#### T7.1 Degree centrality

Why first:

```text
It proves projection orientation, sidecar filtering, stream/stats/write modes,
and memory estimates with minimal algorithm complexity.
```

Acceptance tests:

- degree on fixture graph matches hand-calculated oracle;
- directed/reverse/undirected modes differ correctly;
- stream output row shape matches GDS expectation;
- estimate includes one output value per node and no unnecessary topology copy.

#### T7.2 BFS and WCC

Why second:

```text
BFS proves frontier scheduling. WCC proves global iterative traversal.
```

Acceptance tests:

- BFS paths match oracle on small graphs;
- WCC component ids are stable under flat CSR and tilehouse;
- memory budget caps frontier buffers;
- tile boundary traversal is covered by tests.

#### T7.3 PageRank

Why third:

```text
PageRank proves the 50GB/8GB promise because vectors dominate memory.
```

Acceptance tests:

- tiny graph PageRank converges to known values within tolerance;
- estimate for 200M nodes reports vector bytes explicitly;
- Level 2 plan uses bounded vectors and direct stream;
- Level 3 plan spills or streams under strict budget;
- no mmap page-cache plan is allowed to claim deterministic RAM.

### Phase 7B: algorithm-family diligence matrix

The current Phase 7 kernels are enough to start architecture convergence, but
not enough to plan implementation across the whole GDS surface. Add a separate
algorithm diligence matrix before writing many kernels.

The matrix should classify every GDS algorithm by:

```text
graph access pattern
state shape
tile execution strategy
direct-I/O strategy
spill strategy
exactness target
GDS modes
oracle test
50GB/8GB risk
```

Codex-note guardrail:

```text
Algorithms do not get new durable topology layouts by default.
They get a topology cursor, typed property sidecars, bounded scratch/spill,
and result/model artifacts where required.
```

#### T7B.1 Add shared algorithm state types

Symbols to add:

```rust
pub enum AlgorithmAccessPattern {
    PerNodeDegree,
    FrontierTraversal,
    GlobalIterativeScan,
    PriorityQueueTraversal,
    TriangleIntersection,
    CommunityContraction,
    PairwiseSimilarity,
    RandomWalkSampling,
    DenseEmbedding,
    PropertyColumnTransform,
}
// Describes how an algorithm touches CSR tiles.

pub enum AlgorithmStateShape {
    ScalarPerNode { bytes_per_node: u64 },
    VectorPerNode { vectors: u32, bytes_per_value: u64 },
    FrontierBitset,
    PriorityQueue,
    CandidateTopK { k: u32 },
    DenseMatrix { dimensions: u32, bytes_per_value: u64 },
    ModelArtifact,
}
// Drives memory estimates and spill decisions.

pub struct AlgorithmDiligenceSpec {
    pub procedure_prefix: &'static str,
    pub family: GdsProcedureFamily,
    pub access_pattern: AlgorithmAccessPattern,
    pub state_shapes: &'static [AlgorithmStateShape],
    pub supported_modes: &'static [GdsProcedureMode],
    pub exactness: ExactnessTarget,
    pub tile_strategy: PhysicalGraphPlanKind,
    pub first_oracle: &'static str,
}
// One row per algorithm family, independent of implementation status.
```

Acceptance tests:

- every algorithm prefix in the GDS inventory maps to one diligence spec;
- every diligence spec names at least one oracle fixture;
- every vector/matrix state reports bytes as a function of node count,
  edge count, dimension, and configured `topK` where applicable;
- high-risk families cannot be marked implementation-ready without an explicit
  spill strategy.

#### T7B.2 Centrality algorithms

Reference procedures include PageRank/ArticleRank/Eigenvector, degree,
betweenness, harmonic/closeness, HITS, articulation points, and CELF-style
influence procedures.

| Algorithm group | CSR/tile execution | Dominant state | TDD oracle | RAM risk |
| --- | --- | --- | --- | --- |
| Degree | read offsets only, tile-local | output scalar per node | hand degree fixture | low |
| PageRank / ArticleRank / Eigenvector | global edge stream per iteration | 2-4 vectors × node_count | tiny numerical convergence | medium/high at 200M nodes |
| HITS | forward + reverse global scans | authority/hub vectors | bipartite toy graph | high |
| Harmonic / closeness | repeated BFS/SSSP from selected/all nodes | frontier + distance arrays | small unweighted graph | high if all-pairs |
| Betweenness | Brandes-style source sweeps | stack, sigma, delta, predecessor frontier | diamond graph | very high |
| Articulation points | DFS low-link | discovery/low/parent arrays | bridge/articulation fixture | medium |
| CELF/influence | repeated marginal-gain simulations | candidate heap + sampled reachability | tiny influence graph | high |

Implementation tasks:

- implement degree first because it validates projection orientation cheaply;
- implement PageRank next because it proves deterministic vector estimates;
- mark all-pairs centralities as exact-but-expensive unless they have sampling
  configs;
- require estimates to reject all-pairs plans that cannot fit the configured
  budget.

Acceptance tests:

- degree stream/stat/write outputs match expected schema;
- PageRank estimate for 200M nodes shows each vector explicitly;
- betweenness/closeness estimates scale with `source_count × frontier_state`;
- unsupported centrality procedures still appear in registry and return
  deterministic `UnsupportedButRegistered`.

#### T7B.3 Path-finding algorithms

Reference procedures include all-shortest-paths, BFS, DFS, Bellman-Ford,
Delta-Stepping, Dijkstra single-source/single-pair, A*, Yen's k-shortest paths,
random walk, spanning tree, Steiner tree, and longest path variants.

| Algorithm group | CSR/tile execution | Dominant state | TDD oracle | RAM risk |
| --- | --- | --- | --- | --- |
| BFS / DFS | tile wavefront frontier | visited bitset + parent/distance | small tree/cycle graph | low/medium |
| Dijkstra / A* | tile wavefront + weight sidecar | priority queue + distance + predecessor | weighted diamond graph | medium |
| Delta-Stepping | bucketed frontier over weight sidecars | buckets + distance vector | weighted multi-path graph | medium/high |
| Bellman-Ford | repeated global edge stream | distance vector | negative-edge no-cycle graph | medium |
| All shortest paths | repeated BFS/SSSP or blocked dynamic plan | many distance/frontier states | tiny all-pairs oracle | very high |
| Yen's k-shortest | repeated Dijkstra with path suppression | heap of candidate paths | graph with 3 known paths | high |
| Random walk | tile-local sampling with boundary handoff | RNG state + walk buffers | deterministic seeded walk | medium |
| Spanning/Steiner tree | frontier/priority plan | parent + candidate heap | weighted tree oracle | medium |

Implementation tasks:

- make unweighted BFS/DFS the first pathfinding kernels;
- add weight sidecar requirements before weighted shortest paths;
- support `sourceNode` / `targetNode` configs without loading all nodes;
- add deterministic RNG seed tests for random walk;
- make all-pairs procedures budget-aware and able to reject unsafe exact plans.

Acceptance tests:

- BFS output has stable path order for fixture graphs;
- Dijkstra and Delta-Stepping agree on non-negative weighted fixtures;
- Bellman-Ford detects negative cycles where GDS-compatible behavior requires it;
- random walk with a fixed seed is reproducible across flat CSR and tilehouse;
- every pathfinding write mode reports relationship write counts.

#### T7B.4 Community and structure algorithms

Reference procedures include WCC, SCC, triangle count, local clustering
coefficient, k-core, k-1 coloring, label propagation, Louvain, Leiden,
modularity/modularity optimization, conductance, approximate max-k-cut, SLPA,
and k-means-style procedures.

| Algorithm group | CSR/tile execution | Dominant state | TDD oracle | RAM risk |
| --- | --- | --- | --- | --- |
| WCC | iterative union/frontier | component id per node | two-components fixture | low/medium |
| SCC | DFS/Kosaraju/Tarjan over forward+reverse | stack + component arrays | directed SCC fixture | medium |
| Triangle count / LCC | sorted neighbor intersections | intersection buffers | triangle/square fixture | medium/high for high degree |
| k-core | degree peeling | degree + queue | k-core toy graph | medium |
| Coloring | iterative color assignment | color per node + conflict frontier | odd/even cycle fixture | medium |
| Label propagation / SLPA | iterative neighbor label scans | label distributions | two-cluster fixture | medium/high |
| Louvain / Leiden | multi-level contraction | community ids + aggregate graph | modularity toy graph | high |
| Modularity / conductance | scan cut/internal edges | community/property sidecars | known partition fixture | medium |
| K-means | property/embedding vectors | centroid matrix + assignment | tiny points fixture | high if dense vectors |

Implementation tasks:

- implement WCC and SCC before modularity algorithms;
- implement triangle/LCC only after neighbor intersections are proven over tile
  boundaries;
- implement Louvain/Leiden only after a compact "contracted graph" sidecar
  format exists;
- require each mutate/write mode to store community ids as projected graph
  sidecars before OLTP writeback.

Acceptance tests:

- WCC/SCC are stable between flat CSR and tilehouse;
- triangle count does not double-count across tile boundaries;
- Louvain/Leiden estimates include contracted graph scratch;
- label propagation has deterministic tie-breaking when configured;
- community write mode validates property names and output counts.

#### T7B.5 Similarity algorithms

Reference procedures include KNN, filtered KNN, nodeSimilarity, and filtered
nodeSimilarity.

| Algorithm group | CSR/tile execution | Dominant state | TDD oracle | RAM risk |
| --- | --- | --- | --- | --- |
| Node similarity | adjacency/property overlap | candidate pairs + topK heaps | small bipartite overlap graph | very high |
| Filtered node similarity | overlap with label/property filters | filtered candidate pairs | filtered toy graph | high |
| KNN | vector/property sidecar scan | per-node topK + candidate sampler | small vector set | very high |
| Filtered KNN | KNN with node filters | filtered topK heaps | filtered vector set | high |

Implementation tasks:

- never materialize all pairwise similarities;
- require candidate generation strategy: tile-local blocking, degree filters,
  LSH/random-projection, or GraphBLAS-style sparse product;
- make `topK`, `similarityCutoff`, and filter selectivity part of estimates;
- stream mode should be spillable and sorted only as much as required by GDS
  output semantics.

Acceptance tests:

- topK results match hand oracle on tiny graphs;
- `similarityCutoff` prunes expected rows;
- filtered and unfiltered procedures differ only by filter semantics;
- estimate rejects `O(n^2)` exact plans without a configured candidate strategy.

#### T7B.6 Embeddings algorithms

Reference procedures include FastRP, GraphSAGE stream/train/write, HashGNN, and
Node2Vec.

| Algorithm group | CSR/tile execution | Dominant state | TDD oracle | RAM risk |
| --- | --- | --- | --- | --- |
| FastRP | repeated sparse propagation | embedding matrix | deterministic tiny embedding with seed | very high |
| Node2Vec | random walks + skip-gram/training | walk corpus + embedding matrix | seeded tiny walk corpus | very high |
| GraphSAGE | neighbor sampling + model | sampled batches + model weights | tiny train/infer fixture | very high |
| HashGNN | hashed features + neighborhood aggregation | feature hashes + embeddings | deterministic hash fixture | high |

Implementation tasks:

- separate "embedding output sidecar" from algorithm scratch;
- require dimension, batch size, concurrency, and walk length in estimates;
- make seeded determinism a test requirement for all stochastic algorithms;
- support streaming embeddings without requiring writeback;
- defer training-heavy variants until model catalog semantics exist.

Acceptance tests:

- output vector dimensions match config;
- seeded runs are reproducible;
- estimate for `node_count × dimension × bytes_per_value` is explicit;
- write mode stores embedding sidecars without duplicating base topology.

#### T7B.7 ML, pipelines, model catalog, and miscellaneous algorithms

Reference ML/misc includes KGE, splitRelationships, scaleProperties,
toUndirected, collapsePath, indexInverse, model catalog, operations, and
pipelines.

| Algorithm group | CSR/tile execution | Dominant state | TDD oracle | RAM risk |
| --- | --- | --- | --- | --- |
| KGE | relationship-type/property sidecars | embeddings + negative samples | tiny typed KG fixture | very high |
| Split relationships | sidecar/filter mutation | split tags/properties | deterministic split with seed | medium |
| Scale properties | property column scan | min/max/std stats + output column | numeric property fixture | low |
| To undirected | logical projection or sidecar relationship writes | boundary-aware doubled edges | asymmetric fixture | medium |
| Collapse path / index inverse | topology transform | derived relationship sidecar | path fixture | medium/high |
| Pipelines/model catalog | metadata + trained artifacts | model bytes + feature schema | train/register/list/drop fixture | high |

Implementation tasks:

- implement `scaleProperties` early as the first property-only algorithm;
- treat `toUndirected` primarily as a projection operation, not a base topology
  rewrite;
- require model catalog persistence and versioning before training APIs are
  marked supported;
- keep operations/progress APIs wired into execution budget telemetry.

Acceptance tests:

- scaleProperties stream/write matches numeric oracle;
- splitRelationships is deterministic with a seed;
- model catalog list/drop survives process restart;
- pipeline train estimates include feature extraction, model memory, and output
  writeback.

#### T7B.8 Algorithm-mode contract

Every algorithm family must implement or explicitly stub each mode:

| Mode | Required behavior | Test requirement |
| --- | --- | --- |
| `stream` | return result rows without catalog mutation | output schema and row-order tests |
| `stats` | aggregate timings/counts/distribution summaries | no per-node sidecar created |
| `mutate` | write result into projected graph sidecar | catalog reflects new property |
| `write` | write result back to OLTP-facing store | write count and property name validation |
| `estimate` | return memory contract without execution | no graph scan beyond metadata |

Cross-cutting acceptance tests:

- same algorithm config produces compatible `stream`, `stats`, `mutate`, and
  `write` metadata;
- mode-specific unsupported cases are explicit;
- write/mutate never run if `estimate.required_bytes > budget.max_rss_bytes`;
- output schemas are checked against the inventory.

#### T7B.9 Algorithm rollout gates

Do not mark an algorithm "supported" until all gates pass:

```text
G1 registry row exists
G2 config parser validates defaults and bad inputs
G3 estimate accounts for topology, sidecars, state, scratch, tail overlays
G4 tiny oracle correctness test passes
G5 flat CSR parity test passes; Tilehouse parity is required only if that
   backend is selected
G6 each supported mode has schema tests
G7 budget rejection test passes
G8 deterministic ordering/seed behavior is documented
```

### Phase 8: freshness and update bridge

#### T8.1 Add projection fact receipts

Symbols to add:

```rust
pub enum ProjectionFactOpKind {
    CreateNode,
    DeleteNode,
    CreateRelationship,
    DeleteRelationship,
    SetNodeProperty,
    SetRelationshipProperty,
    AddLabel,
    RemoveLabel,
}
// Graph-relevant OLTP facts normalized into the Projection Build Store.

pub struct ProjectionFactReceipt {
    pub tx_id: u64,
    pub op: ProjectionFactOpKind,
    pub affected_node_ids: Vec<NodeId>,
    pub affected_relationship_ids: Vec<RelationshipId>,
}
// Minimal receipt that can advance analytical watermarks without loading the
// whole graph.
```

Acceptance tests:

- create relationship receipt maps to source/target dense IDs;
- property update receipt maps to sidecar dirty region;
- delete receipt creates a tombstone fact;
- receipt stream advances Projection Build Store watermark monotonically;
- receipt replay is idempotent for the same tx/entity/op identity;
- out-of-order receipts are rejected or buffered deterministically.

#### T8.2 Add snapshot freshness modes

The default serving contract is snapshot-as-of. Query-time tail merge is
optional and must be selected by freshness mode and memory budget.

```rust
pub struct SnapshotFreshness {
    pub snapshot_generation: u64,
    pub snapshot_tx_watermark: u64,
    pub build_store_tx_watermark: u64,
    pub mode: FreshnessMode,
}
// Reports query freshness explicitly.
```

Acceptance tests:

- snapshot-only query reports exact-as-of snapshot watermark;
- when build-store watermark is newer than snapshot watermark, lag is reported;
- force-refresh mode refuses to run stale snapshots;
- bounded-tail mode includes tail bytes in `MemoryEstimate`;
- bounded-tail mode refuses to run if tail merge exceeds configured budget.

#### T8.3 Add optional tail overlay

Acceptance tests:

- query view applies added/deleted tail facts over base snapshot;
- property tail facts resolve before algorithm execution begins;
- tail bytes, merge buffers, and conflict-resolution state appear in estimates;
- tail overlay is not required for durability or next-snapshot correctness.

#### T8.4 Add snapshot rebuild and optional Tilehouse compaction budget tests

Acceptance tests:

- rebuilding from Projection Build Store produces a new flat CSR generation;
- build peak RSS stays under configured budget on fixture;
- crash during build leaves previous generation readable;
- manifest publish is atomic;
- if Tilehouse is selected, compacting dirty cells produces a new cell
  generation under budget.

### Phase 9: full GDS family rollout

Implement in dependency order, with one golden fixture per family:

| Tier | Family | Representative procedures | Why this order |
| --- | --- | --- | --- |
| 1 | catalog | graph.project/list/drop/exists/sizeOf | unlocks named graph execution |
| 1 | centrality | degree, PageRank | proves vector memory and stream/write |
| 1 | pathfinding | BFS, DFS, SSSP | proves frontier/priority memory |
| 1 | community | WCC, SCC, triangle count, k-core | proves global iterative and structural algorithms |
| 2 | community | Louvain, Leiden, label propagation | requires mutation/workspace layers |
| 2 | similarity | nodeSimilarity, KNN | requires candidate pruning and top-k memory contracts |
| 3 | embeddings | FastRP, Node2Vec, GraphSAGE | requires vector sidecars/model output |
| 3 | ML/pipelines/model catalog | train/predict/write/model catalog | depends on embeddings and stored model semantics |
| 3 | miscellaneous/operations | scaleProperties, progress, feature flags | depends on catalog/procedure infrastructure |

Every procedure must have:

```text
registry row
config parser test
estimate test
unsupported-or-implemented execution test
output schema test
memory budget test
```

## 5. Proposed implementation PR sequence

### PR 1: GDS inventory and registry only

No storage change.

Tests first:

- inventory parser fixture;
- registry duplicate detection;
- `gds.pageRank.stream` and `.estimate` known.

Value:

```text
Prevents API-surface drift before deep storage work.
```

### PR 2: support levels + Projection Build Store manifest

No algorithm kernels.

Tests first:

- every inventory row has a support level;
- unknown differs from `UnsupportedButRegistered`;
- build-store manifest records source generation and watermark;
- stale build-store watermark refuses publish.

Value:

```text
Turns the Codex-note analytical pre-dataset into a testable contract.
```

### PR 3: graph projection catalog skeleton

No algorithm kernels.

Tests first:

- project/list/drop/exists;
- orientation parsing;
- user/database/graph identity;
- source tx watermark/freshness mode;
- memory estimate shape.

Value:

```text
Turns snapshots into named GDS graph handles.
```

### PR 4: `GraphAdjacencyRuntime` over existing flat CSR

No tilehouse yet.

Tests first:

- flat CSR neighbor cursor parity;
- flat CSR global edge cursor parity;
- type-filtered and undirected logical view parity;
- old walk tests still pass.

Value:

```text
Algorithms can be written once and later run on tiles.
```

### PR 5: columnar property sidecars

Tests first:

- labels/types/properties filter correctly;
- defaults/nulls/coercion are deterministic;
- sidecar bytes show up in estimates;
- schema reporting reads metadata without full graph scan.

Value:

```text
Unlocks real GDS projections without multiplying topology layouts.
```

### PR 6: holistic memory planner

Tests first:

- estimate includes heap/RSS/page-cache/direct/topology/property/result/model/spill;
- mmap plans do not claim deterministic RAM;
- strict-RAM plan streams/spills or rejects;
- 50GB/8GB decision reports dominant state and reason.

Value:

```text
Makes the 8 GB promise executable before expensive kernels exist.
```

### PR 7: degree + BFS + WCC

Tests first:

- hand oracle correctness;
- stream/stats/write modes where applicable;
- memory plan below budget.

Value:

```text
First end-to-end GDS algorithms over projection/catalog/runtime.
```

### PR 8: PageRank deterministic RAM

Tests first:

- tiny numerical oracle;
- 200M-node estimate contract;
- O_DIRECT/global stream plan;
- no false deterministic claim for mmap.

Value:

```text
Directly validates the PRD's "PageRank uses exactly X MB" claim.
```

### PR 9: freshness watermarks and snapshot rebuild

Tests first:

- receipt -> Projection Build Store watermark;
- snapshot exact-as-of reporting;
- force-refresh and stale-snapshot behavior;
- rebuild/publish under scratch budget;
- recovery test.

Value:

```text
Connects Neo4j-shaped OLTP updates to OLAP freshness without mandatory
query-time overlays.
```

### PR 10: optional bounded tail overlay

Only if freshness SLA needs query visibility beyond the published snapshot.

Tests first:

- snapshot + bounded tail query correctness;
- tail bytes and merge buffers in estimates;
- rejection when tail exceeds memory budget;
- no durability dependency on tail overlay.

Value:

```text
Adds near-real-time freshness only when it can stay inside the RAM contract.
```

### PR 11: Tilehouse manifest and partitioner

Tests first:

- tile passport validation;
- deterministic partition;
- boundary ratio accounting.

Value:

```text
Proves the storage unit before writing full tile files.
```

### PR 12: Tilehouse writer/reader parity

Tests first:

- fixture tilehouse files exist;
- all flat CSR walk outputs equal tilehouse outputs;
- snapshot overhead measured.

Value:

```text
Makes Cellular CSR real without changing GDS algorithms.
```

### PR 13+: remaining GDS families

Roll out by dependency order with compatibility matrix tracking.

## 6. Symbols to modify or add, in control/data-flow order

```rust
// src/types.rs
pub enum SnapshotStorageMode {
    ImmutableDualCsr,
    CellularCsrTilehouse,
}
// Replace stringly storage-mode handling while preserving JSON compatibility.
// ImmutableDualCsr is the first v003 backend and correctness oracle.

pub struct SnapshotManifest {
    // existing fields remain
    // add Projection Build Store source generation and source tx watermark
    // add optional tilehouse manifest path once v3 snapshots exist
}
// Keeps v2 flat CSR readable while making v3 snapshot freshness explicit.
```

```rust
// src/gds/procedure.rs
pub enum GdsSupportLevel { P0Registered, P1ExactLowRam, P2Later, UnsupportedButRegistered }
pub enum GdsProcedureMode { Stream, Stats, Mutate, Write, Estimate }
pub enum GdsProcedureFamily { Catalog, Centrality, Community, PathFinding, Similarity, Embedding, MachineLearning, Miscellaneous, Operations, Pipelines, ModelCatalog }
pub struct GdsProcedureSpec { /* name, family, mode, estimate, result shape, support level */ }
pub fn gds_procedure_specs() -> &'static [GdsProcedureSpec];
// Public surface registry generated/validated from the GDS reference inventory.
```

```rust
// src/projection_build_store.rs
pub struct ProjectionBuildStoreManifest { /* version, source generation, tx watermark, fact counts, dense id map */ }
pub enum ProjectionFactOpKind { CreateNode, DeleteNode, CreateRelationship, DeleteRelationship, SetNodeProperty, SetRelationshipProperty, AddLabel, RemoveLabel }
pub struct ProjectionFactReceipt { /* tx id, op, affected nodes/relationships */ }
// Analytical pre-dataset between OLTP truth and immutable CSR snapshots.
```

```rust
// src/gds/catalog.rs
pub struct GraphProjectionSpec { /* graph name, selectors, orientation, properties */ }
pub enum RelationshipOrientation { Natural, Reverse, Undirected }
pub struct GraphProjectionCatalog { /* named projected graphs */ }
pub enum FreshnessMode { SnapshotOnly, SnapshotPlusBoundedTail, ForceRefreshBeforeRun, RejectUntilRefresh }
pub struct GraphProjectionHandle { /* user, database, graph name, generation, tx watermark, freshness, runtime, estimate */ }
// Mirrors GDS graph catalog semantics over Knight Bus snapshots/backends.
```

```rust
// src/memory.rs
pub struct MemoryEstimate {
    pub required_bytes: u64,
    pub heap_bytes: u64,
    pub rss_budget_bytes: u64,
    pub page_cache_bytes: u64,
    pub page_cache_unbounded_risk: bool,
    pub direct_io_buffer_bytes: u64,
    pub topology_bytes: u64,
    pub property_sidecar_bytes: u64,
    pub algorithm_state_bytes: u64,
    pub scratch_bytes: u64,
    pub tail_overlay_bytes: u64,
    pub result_sidecar_bytes: u64,
    pub model_artifact_bytes: u64,
    pub writeback_bytes: u64,
    pub spill_bytes: u64,
}
// Holistic memory accounting object used by every estimate procedure.
```

```rust
// src/runtime.rs
pub trait GraphAdjacencyRuntime {
    fn node_count(&self) -> u64;
    fn relationship_count(&self) -> u64;
    fn labels(&self, node: DenseNodeId) -> Result<LabelSet<'_>, KnightBusError>;
    fn relationship_types(&self) -> Result<RelationshipTypeSet<'_>, KnightBusError>;
    fn neighbors(&self, node: DenseNodeId, direction: WalkDirection) -> Result<NeighborCursor<'_>, KnightBusError>;
    fn global_edges(&self, direction: WalkDirection) -> Result<EdgeCursor<'_>, KnightBusError>;
    fn typed_edges(&self, direction: WalkDirection, relationship_types: RelationshipTypeFilter<'_>) -> Result<EdgeCursor<'_>, KnightBusError>;
}
// Algorithm-facing graph access abstraction.

pub struct FlatDualCsrBackend { /* manifest + mmap/direct-stream policy */ }
impl GraphAdjacencyRuntime for FlatDualCsrBackend { /* flat CSR adapter */ }
// Lets current flat CSR run future GDS tests before tiles exist.
```

```rust
// src/tilehouse.rs
pub struct TileId(pub u32);
pub struct CsrTilePassport { /* counts, dense range, boundary count, dirty watermark */ }
pub struct TilehouseManifest { /* generation, counts, passports, sidecar refs */ }
pub struct CellularCsrTilehouse { /* opened tile metadata/readers */ }
pub struct TilehouseRuntime { /* tilehouse + IO policy */ }
// New Cellular CSR storage/runtime layer.
```

```rust
// src/tilehouse/partition.rs
pub trait TilePartitioner {
    fn partition(&self, graph: &NormalizedGraphData, budget: TileBudget) -> Result<TileAssignment, KnightBusError>;
}
pub struct TileBudget { pub max_nodes_per_tile: u32, pub max_edges_per_tile: u64, pub max_tile_bytes: u64 }
// Testable partitioning policy and RAM-bounded cell sizing.
```

```rust
// src/snapshot.rs
pub struct CellularCsrSnapshotWriter<P: TilePartitioner> {
    pub partitioner: P,
    pub tile_budget: TileBudget,
}
impl<P: TilePartitioner> SnapshotArtifactWriter for CellularCsrSnapshotWriter<P> { /* write tilehouse */ }
// Emits tilehouse artifacts while preserving the existing writer.
```

```rust
// src/sidecar.rs
pub enum SidecarKind {
    NodeLabel,
    RelationshipType,
    NodeProperty,
    RelationshipProperty,
    GraphProperty,
    Weight,
    VectorFeature,
}
pub struct SidecarColumnManifest { /* name, kind, type, path, null bitmap */ }
// Columnar labels/types/properties required by GDS projections.
```

```rust
// src/gds/execution.rs
pub trait GdsProcedure {
    fn spec(&self) -> &'static GdsProcedureSpec;
    fn estimate(&self, graph: &GraphProjectionHandle, config: &GdsConfig) -> Result<MemoryEstimate, KnightBusError>;
    fn execute(&self, graph: &GraphProjectionHandle, config: &GdsConfig, mode: GdsProcedureMode) -> Result<GdsResultStream, KnightBusError>;
}
pub enum PhysicalGraphPlan {
    FlatMmapScan,
    GlobalDirectStream,
    TileLocal { tiles: Vec<TileId> },
    TileWavefront { start_tiles: Vec<TileId> },
    SpillableVectorTape,
}
// Converts API calls into bounded-memory physical execution.
```

```rust
// src/oltp_bridge.rs
pub struct SnapshotFreshness { /* snapshot generation, snapshot watermark, build-store watermark, mode */ }
pub struct TailOverlayPlanner;
impl TailOverlayPlanner {
    pub fn estimate_tail(&self, freshness: SnapshotFreshness, budget: MemoryBudget) -> Result<MemoryEstimate, KnightBusError>;
}
// Optional freshness layer for tx > snapshot watermark.
```

## 7. Done definition

This diligence sequence is complete when:

- every GDS procedure is inventoried and classified;
- every in-scope procedure is registered;
- every registered procedure has estimate behavior;
- flat CSR backend is proven against fixture/property-based graph oracles;
- Tilehouse adjacency is proven equivalent only if the Tilehouse backend is
  selected;
- first-tier algorithms pass oracle tests;
- every memory estimate names heap, RSS budget, page cache, direct I/O buffers,
  topology, property sidecars, algorithm state, tail overlays, results, models,
  writeback, spill, and scratch;
- Projection Build Store receipts advance freshness watermarks and snapshots
  rebuild/publish under budget;
- 50GB/8GB tests demonstrate deterministic pass/fail decisions.

## 8. First concrete task to start

Start with:

```text
PR 1: GDS inventory and registry only
```

because it is the cheapest uncertainty reducer:

- it tests whether "full surface area" is known;
- it prevents accidental API narrowing;
- it does not depend on tile partitioning choices;
- it gives every later algorithm/storage PR a compatibility checklist.

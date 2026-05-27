# v003 Diligence 01: TDD Task List for CSR Tiles + GDS Surface

This diligence note turns the v003 architecture direction into a test-first task
list.

The target is not "add a faster PageRank." The target is:

```text
Neo4j-shaped OLTP stays fixed.
OLAP uses Cellular CSR Tilehouse.
The external GDS-style surface is preserved.
Every claim is backed by tests before implementation.
```

## 1. Core facts

### PRD facts

- v003 wants a Neo4j rewrite in Rust with the same API/surface area and zero
  application-code changes.
- OLTP storage remains Neo4j-shaped.
- OLAP storage is the optimization target.
- The RAM promise is holistic: heap, page cache, duplicate layouts, compaction
  scratch, snapshot build scratch, delta overlays, indexes, and algorithm
  intermediates all count.
- The scale target is 50 GB-class data on 8 GB systems.
- O_DIRECT + compio is preferred for deterministic RAM because it bypasses page
  cache and makes buffer sizes explicit.

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

## 2. Rubber-duck diligence

### Duck: Are CSR tiles enough to claim GDS compatibility?

No. CSR tiles are the storage substrate. GDS compatibility additionally needs
procedure names, config parsing, graph catalog semantics, output schemas, modes,
memory estimates, and write/mutate behavior.

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
delta overlays, or algorithm vectors exceed the 8 GB machine. The memory tests
must account for holistic RSS and page-cache policy, not just allocator bytes.

### Duck: What is the creative move?

Treat GDS as an ABI and CSR tiles as a physical plan target:

```text
GDS procedure call
  -> compatibility registry
  -> graph projection catalog
  -> logical graph view
  -> memory contract
  -> physical plan:
       tile-local window
       multi-tile wavefront
       global O_DIRECT stream
       spillable vector/tape
  -> stream/stats/mutate/write result mode
```

This makes tile storage prove itself against the whole API surface instead of
becoming a disconnected storage experiment.

## 3. TDD strategy

### Rule 1: every implementation task starts with a failing test

Each task below has a test-first acceptance criterion. If a task cannot name its
test, it is not ready for implementation.

### Rule 2: compatibility tests precede performance tests

The v003 PRD says exact same surface area. A fast non-compatible system fails.

### Rule 3: flat CSR remains the oracle

Before Cellular CSR is trusted, every topology query over tiles must match the
current flat CSR runtime.

### Rule 4: Neo4j/GDS reference is the API oracle

The local `gitrefrepo/neo4j-gds-src` tree is the diligence source for procedure
names, modes, facade categories, config shape, and output shape.

### Rule 5: memory estimates are executable contracts

`estimate` should not be marketing text. It should be derived from:

```text
topology bytes + sidecar bytes + frontier/vector bytes + spill buffers
+ delta overlay bytes + compaction scratch + planned page-cache policy
```

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
}
// Static compatibility declaration for one public procedure variant.

pub fn gds_procedure_specs() -> &'static [GdsProcedureSpec];
// Returns the checked compatibility registry.
```

Acceptance tests:

- registry has a matching spec for every inventory row marked in-scope;
- no duplicate procedure names;
- each non-estimate algorithm mode links to the expected estimate procedure;
- missing kernels return `UnsupportedButRegistered`, not unknown procedure.

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
    pub snapshot_generation: u64,
    pub node_count: u64,
    pub relationship_count: u64,
    pub memory_estimate: MemoryEstimate,
}
// Catalog handle returned to algorithms.
```

Acceptance tests:

- `gds.graph.project` fixture maps to `GraphProjectionSpec`;
- `orientation: "UNDIRECTED"` doubles or symmetrizes relationships in the
  logical view without changing base CSR files;
- catalog list/drop/existence behavior matches GDS expectations for named graphs.

#### T2.2 Add projection memory estimation

Symbols to add:

```rust
pub struct MemoryEstimate {
    pub required_bytes: u64,
    pub heap_bytes: u64,
    pub page_cache_bytes: u64,
    pub direct_io_buffer_bytes: u64,
    pub algorithm_state_bytes: u64,
    pub delta_overlay_bytes: u64,
    pub scratch_bytes: u64,
}
// Holistic memory contract for projection and algorithm execution.
```

Acceptance tests:

- estimates include all fields;
- estimate for flat CSR projection does not include duplicate full topology;
- estimate for tilehouse projection includes tile metadata and sidecar indexes;
- 50GB/8GB scenario has an explicit pass/fail decision.

### Phase 3: abstract graph access before tilehouse

#### T3.1 Introduce algorithm-facing adjacency trait

Symbols to add:

```rust
pub trait GraphAdjacencyRuntime {
    fn node_count(&self) -> u64;

    fn relationship_count(&self) -> u64;

    fn neighbors(
        &self,
        node: DenseNodeId,
        direction: WalkDirection,
    ) -> Result<NeighborCursor<'_>, KnightBusError>;

    fn global_edges(
        &self,
        direction: WalkDirection,
    ) -> Result<EdgeCursor<'_>, KnightBusError>;
}
// Minimal substrate needed by GDS algorithms without exposing file layout.
```

Acceptance tests:

- `MmapWalkRuntime` implements `GraphAdjacencyRuntime`;
- neighbor results match `WalkQueryRuntime`;
- global edge cursor returns exactly the flat CSR edge set;
- reverse global cursor matches reverse CSR.

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

### Phase 4: Cellular CSR Tilehouse storage

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

### Phase 8: update/freshness bridge

#### T8.1 Add WAL receipt types

Symbols to add:

```rust
pub enum WalGraphOp {
    CreateNode,
    DeleteNode,
    CreateRelationship,
    DeleteRelationship,
    SetNodeProperty,
    SetRelationshipProperty,
    AddLabel,
    RemoveLabel,
}
// OLTP-to-OLAP operation vocabulary.

pub struct WalReceipt {
    pub tx_id: u64,
    pub op: WalGraphOp,
    pub source_node: Option<NodeKey>,
    pub target_node: Option<NodeKey>,
    pub relationship_type: Option<String>,
    pub property_key: Option<String>,
}
// Small durable fact emitted after OLTP commit.
```

Acceptance tests:

- receipt maps to exactly the affected tile(s);
- relationship updates dirty source and target tiles when needed;
- property updates dirty only sidecar/passport metadata;
- out-of-order receipts are rejected or buffered deterministically.

#### T8.2 Add delta overlay and compaction contract

Symbols to add:

```rust
pub struct DeltaApplier;

impl DeltaApplier {
    pub fn append_receipt(&self, receipt: WalReceipt) -> Result<(), KnightBusError>;

    pub fn compact_dirty_cells(
        &self,
        budget: BuildMemoryBudget,
    ) -> Result<CompactionReport, KnightBusError>;
}
// Freshness path from Neo4j-shaped OLTP commits to tile-local OLAP updates.
```

Acceptance tests:

- query after receipt sees fresh edge via overlay;
- compaction removes overlay entry and updates tile CSR;
- compaction never exceeds configured scratch budget;
- crash between append and compaction recovers from durable receipts.

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

### PR 2: graph projection catalog skeleton

No algorithm kernels.

Tests first:

- project/list/drop/exists;
- orientation parsing;
- memory estimate shape.

Value:

```text
Turns snapshots into named GDS graph handles.
```

### PR 3: `GraphAdjacencyRuntime` over existing flat CSR

No tilehouse yet.

Tests first:

- flat CSR neighbor cursor parity;
- flat CSR global edge cursor parity;
- old walk tests still pass.

Value:

```text
Algorithms can be written once and later run on tiles.
```

### PR 4: Tilehouse manifest and partitioner

Tests first:

- tile passport validation;
- deterministic partition;
- boundary ratio accounting.

Value:

```text
Proves the storage unit before writing full tile files.
```

### PR 5: Tilehouse writer/reader parity

Tests first:

- fixture tilehouse files exist;
- all flat CSR walk outputs equal tilehouse outputs;
- snapshot overhead measured.

Value:

```text
Makes Cellular CSR real without changing GDS algorithms.
```

### PR 6: sidecar columns

Tests first:

- labels/types/properties filter correctly;
- sidecar bytes show up in estimates.

Value:

```text
Unlocks full GDS projections instead of topology-only demos.
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

### PR 9: WAL receipts and cell-local deltas

Tests first:

- receipt -> dirty tile mapping;
- overlay query freshness;
- compaction under scratch budget;
- recovery test.

Value:

```text
Connects Neo4j-shaped OLTP updates to OLAP freshness.
```

### PR 10+: remaining GDS families

Roll out by dependency order with compatibility matrix tracking.

## 6. Symbols to modify or add, in control/data-flow order

```rust
// src/types.rs
pub enum SnapshotStorageMode {
    ImmutableDualCsr,
    CellularCsrTilehouse,
}
// Replace stringly storage-mode handling while preserving JSON compatibility.

pub struct SnapshotManifest {
    // existing fields remain
    // add optional tilehouse manifest path once v3 snapshots exist
}
// Keeps v2 flat CSR readable and points v3 snapshots to tile metadata.
```

```rust
// src/gds/procedure.rs
pub enum GdsProcedureMode { Stream, Stats, Mutate, Write, Estimate }
pub enum GdsProcedureFamily { Catalog, Centrality, Community, PathFinding, Similarity, Embedding, MachineLearning, Miscellaneous, Operations, Pipelines, ModelCatalog }
pub struct GdsProcedureSpec { /* name, family, mode, estimate, result shape */ }
pub fn gds_procedure_specs() -> &'static [GdsProcedureSpec];
// Public surface registry generated/validated from the GDS reference inventory.
```

```rust
// src/gds/catalog.rs
pub struct GraphProjectionSpec { /* graph name, selectors, orientation, properties */ }
pub enum RelationshipOrientation { Natural, Reverse, Undirected }
pub struct GraphProjectionCatalog { /* named projected graphs */ }
pub struct GraphProjectionHandle { /* graph name, generation, runtime, estimate */ }
// Mirrors GDS graph catalog semantics over Knight Bus snapshots/tilehouse.
```

```rust
// src/memory.rs
pub struct MemoryEstimate {
    pub required_bytes: u64,
    pub heap_bytes: u64,
    pub page_cache_bytes: u64,
    pub direct_io_buffer_bytes: u64,
    pub algorithm_state_bytes: u64,
    pub delta_overlay_bytes: u64,
    pub scratch_bytes: u64,
}
// Holistic memory accounting object used by every estimate procedure.
```

```rust
// src/runtime.rs
pub trait GraphAdjacencyRuntime {
    fn node_count(&self) -> u64;
    fn relationship_count(&self) -> u64;
    fn neighbors(&self, node: DenseNodeId, direction: WalkDirection) -> Result<NeighborCursor<'_>, KnightBusError>;
    fn global_edges(&self, direction: WalkDirection) -> Result<EdgeCursor<'_>, KnightBusError>;
}
// Algorithm-facing graph access abstraction.

impl GraphAdjacencyRuntime for MmapWalkRuntime { /* flat CSR adapter */ }
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
pub enum SidecarKind { NodeLabel, RelationshipType, NodeProperty, RelationshipProperty, Weight }
pub struct SidecarColumnManifest { /* name, kind, type, path, null bitmap */ }
// Columnar tile-local labels/types/properties required by GDS projections.
```

```rust
// src/gds/execution.rs
pub trait GdsProcedure {
    fn spec(&self) -> &'static GdsProcedureSpec;
    fn estimate(&self, graph: &GraphProjectionHandle, config: &GdsConfig) -> Result<MemoryEstimate, KnightBusError>;
    fn execute(&self, graph: &GraphProjectionHandle, config: &GdsConfig, mode: GdsProcedureMode) -> Result<GdsResultStream, KnightBusError>;
}
pub enum PhysicalGraphPlan { TileLocal { tiles: Vec<TileId> }, TileWavefront { start_tiles: Vec<TileId> }, GlobalDirectStream, SpillableVectorTape }
// Converts API calls into bounded-memory physical execution.
```

```rust
// src/oltp_bridge.rs
pub enum WalGraphOp { CreateNode, DeleteNode, CreateRelationship, DeleteRelationship, SetNodeProperty, SetRelationshipProperty, AddLabel, RemoveLabel }
pub struct WalReceipt { /* tx id, op, source/target, type/property */ }
pub struct DeltaApplier;
impl DeltaApplier {
    pub fn append_receipt(&self, receipt: WalReceipt) -> Result<(), KnightBusError>;
    pub fn compact_dirty_cells(&self, budget: BuildMemoryBudget) -> Result<CompactionReport, KnightBusError>;
}
// WAL-to-cell delta and compaction bridge.
```

## 7. Done definition

This diligence sequence is complete when:

- every GDS procedure is inventoried and classified;
- every in-scope procedure is registered;
- every registered procedure has estimate behavior;
- flat CSR and tilehouse adjacency are proven equivalent on fixtures and
  property-based graphs;
- first-tier algorithms pass oracle tests;
- every memory estimate names heap, page cache, direct I/O buffers, algorithm
  state, deltas, and scratch;
- WAL receipts update tile-local deltas and compact under budget;
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

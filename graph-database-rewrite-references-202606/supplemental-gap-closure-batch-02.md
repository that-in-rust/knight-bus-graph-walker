# Supplemental Gap Closure Batch 02

Date: 2026-07-07

This supplement closes a high-value Neo4j compatibility gap left by the first
supermeta corpus pass. It focuses on the public and internal surfaces most
likely to constrain a Rust rewrite:

- Neo4j OLTP record storage and dense-node traversal.
- Official driver routing, retry, bookmark, and fetch-size semantics.
- Bolt handshake and PackStream byte-level compatibility.
- Neo4j GDS projection and algorithm memory estimation.
- GDS pipeline algorithm stubs as a surface-management pattern.
- APOC procedure modes, virtual graph values, and bitmap-frontier traversal.

Evidence policy: CodeGraphContext is used as navigational evidence only. Every
pattern claim below is backed by direct local source reads.

## Evidence Tools And Runs

### CodeGraphContext Runs

Neo4j Python driver synchronous package:

```text
run_dir: /tmp/codex-code-intel/codegraphcontext/neo4j-python-driver-sync-20260706-235310
indexed path: gitrefrepo/Neo4j family/neo4j-python-driver-src/src/neo4j/_sync
stats: 22 files, 504 functions, 67 classes, 118 modules
note: a larger driver scan exited early; this smaller focused scan produced usable navigation stats
```

Neo4j GDS pipeline stubs:

```text
run_dir: /tmp/codex-code-intel/codegraphcontext/neo4j-gds-stubs-20260707-063956
indexed path: gitrefrepo/Neo4j family/neo4j-gds-src/pipeline/src/main/java/org/neo4j/gds/ml/pipeline/stubs
stats: 50 files, 52 functions, 50 classes, 88 modules
finding: `find name Stub` surfaced repeated `stub` methods across algorithm-family adapters
```

## Pattern: OLTP Record Store Is Pointer-Rich By Design

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-src`
- Language: Java
- Files:
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java:30-117`
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java:30-170`
  - `community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java:684-688`

Evidence:

- `NodeRecordFormat` defines a 15-byte fixed record containing in-use state,
  next relationship ID, next property ID, labels, and an extra byte.
- `RelationshipRecordFormat` defines a 34-byte fixed record with first node,
  second node, relationship type, first/second previous and next relationship
  links, next property ID, and first-in-chain markers.
- `dense_node_threshold` is a setting named
  `db.relationship_grouping_threshold` with default value `50`.

Transferable idea:

Neo4j's OLTP shape is not accidentally inefficient. It is a transactional
record-store shape optimized around stable references, in-place updates,
relationship chains, property chains, and dense-node grouping. It is correct for
OLTP mutation semantics, but it is not the lowest-RAM physical shape for OLAP.

Rust translation:

```rust
#[repr(C)]
struct NodeRecordV1 {
    header: u8,
    next_relationship_low: u32,
    next_property_low: u32,
    labels_low: u32,
    labels_high: u8,
    flags: u8,
}

#[repr(C)]
struct RelationshipRecordV1 {
    header: u8,
    first_node_low: u32,
    second_node_low: u32,
    type_and_modifiers: u32,
    first_prev_rel_low: u32,
    first_next_rel_low: u32,
    second_prev_rel_low: u32,
    second_next_rel_low: u32,
    next_property_low: u32,
    chain_flags: u8,
}
```

Why it matters for Neo4j-in-Rust:

- The Rust rewrite should preserve the record-store contract where OLTP
  compatibility requires it.
- OLAP should not pretend the OLTP store is already a graph-algorithm store.
- The RAM-first architecture needs a derived analytical plane: CSR snapshots,
  typed property columns, and algorithm scratch separated from mutable records.

When to use it:

- For transactionally mutable node, relationship, and property state.
- For exact Neo4j-like semantics around node references and relationship
  traversal from the OLTP API.

When not to use it:

- As the primary global PageRank or GDS graph-projection layout.
- As the sole source shape for 50GB-on-8GB analytical execution.

Memory implications:

- Pointer-rich records can be compact per record, but traversal causes
  scattered page access.
- A derived OLAP plane can trade update locality for contiguous scans and
  deterministic buffer ownership.

Testing implications:

- Add binary round-trip tests for record header modifiers and dense flags.
- Add compatibility tests that prove OLTP traversal over records agrees with the
  derived OLAP snapshot for committed generations.

Agentic guidance:

Future agents should not "optimize away" OLTP record links merely because CSR is
better for analytics. Treat record storage and analytical storage as distinct
physical plans for distinct promises.

## Pattern: Dense Nodes Use A Relationship-Group State Machine

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-src`
- Language: Java
- File: `community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:72-270`

Evidence:

- The traversal cursor initializes from a node cursor by reading node reference,
  next relationship reference, dense flag, and relationship selection.
- Non-dense nodes traverse a relationship chain.
- Dense nodes treat the reference as a relationship-group record and iterate a
  small state machine over incoming, outgoing, and loop chains.
- The source comment explains the dense-node flow: fetch group, incoming chain,
  outgoing chain, loop chain, then the next group.
- Ordered groups allow early return when the group type exceeds the selected
  highest type.

Transferable idea:

High-degree nodes need a separate traversal representation. Neo4j's OLTP answer
is relationship-group records; a Rust OLAP answer may be CSR partitions, hub
sidecars, or degree-aware adjacency windows. The shared principle is the same:
supernodes cannot be treated as ordinary linked-list nodes.

Rust translation:

```rust
enum DenseTraversalState {
    Incoming,
    Outgoing,
    Loop,
}

struct DenseNodeTraversal<'a> {
    origin: NodeId,
    group_cursor: RelationshipGroupCursor<'a>,
    state: DenseTraversalState,
    selection: RelationshipSelection,
}
```

Why it matters for Neo4j-in-Rust:

- OLTP needs dense-node semantics to match Neo4j.
- OLAP needs dense-node planning to avoid RAM and latency cliffs.
- GDS projections should preserve direction/type filtering without walking
  irrelevant dense-node chains.

When to use it:

- When a node crosses a relationship-count threshold.
- When type/direction filtering can skip whole groups or adjacency slices.

When not to use it:

- For every node; the metadata and indirection are wasted on sparse nodes.

Memory implications:

- Dense grouping increases metadata but prevents huge irrelevant scans.
- A low-RAM OLAP plane can map dense groups into type-partitioned CSR windows
  and only load windows selected by the algorithm/projection.

Testing implications:

- Test sparse and dense nodes separately.
- Add fixtures with incoming, outgoing, and loop relationships of multiple
  types.
- Verify early-skip behavior for relationship-type selections.

Agentic guidance:

When generating graph traversal code, include "dense node" as a first-class
branch in the design, not as an afterthought in performance tuning.

## Pattern: Driver Routing Tables Are Compatibility State

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-python-driver-src`
- Language: Python
- File: `src/neo4j/_routing.py:81-203`

Evidence:

- Routing info from the server is parsed into routers, readers, and writers.
- Freshness requires the table not to be expired, at least one router, and a
  server for the requested read/write mode.
- Stale routing tables are purged only after TTL plus a configured purge delay.
- Updates replace routers, readers, writers, refresh `last_updated_time`, and
  update TTL.

Transferable idea:

Neo4j compatibility includes cluster/routing behavior even when the first Rust
target is single-node. Official drivers encode routing expectations in their
connection pools and session execution paths.

Rust translation:

```rust
struct RoutingTable {
    database: DatabaseName,
    routers: OrderedSet<SocketAddress>,
    readers: OrderedSet<SocketAddress>,
    writers: OrderedSet<SocketAddress>,
    last_updated: Instant,
    ttl: Duration,
}

impl RoutingTable {
    fn is_fresh(&self, mode: AccessMode) -> bool {
        !self.expired()
            && !self.routers.is_empty()
            && match mode {
                AccessMode::Read => !self.readers.is_empty(),
                AccessMode::Write => !self.writers.is_empty(),
            }
    }
}
```

Why it matters for Neo4j-in-Rust:

- A single-node CE-compatible server can still answer routing metadata
  deterministically.
- Multi-node semantics can be stubbed in a compatible way before real
  clustering exists.
- Driver errors should distinguish "no writers," "routing table expired," and
  "unknown routing context" rather than collapsing them into generic connection
  failure.

Memory implications:

- Routing state is tiny compared with graph storage; keep it in protocol/session
  state, not graph runtime state.

Testing implications:

- Add driver-facing tests for `neo4j://` and `bolt://` URI behavior.
- Test routing-table TTL, no-writer, no-reader, and purge cases.
- Ensure official drivers can request routing info without triggering graph
  projection or OLAP allocation.

Agentic guidance:

Do not model Bolt compatibility as a raw TCP parser only. Include driver routing
state machines in the compatibility spec.

## Pattern: Bookmarks Are Cross-Session Consistency Tokens

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-python-driver-src`
- Language: Python
- Files:
  - `src/neo4j/api.py:321-383`
  - `src/neo4j/_conf.py:359-386`

Evidence:

- The Python driver documents Neo4j clusters as eventually consistent and uses
  bookmarks to make later queries observe earlier database states.
- Bookmark managers keep sessions automatically consistent.
- All bookmark-manager methods must be concurrency safe.
- Driver config includes `max_transaction_retry_time`, retry delay, jitter,
  fetch size, database name, impersonated user, and bookmark manager.

Transferable idea:

Bookmarks are a protocol-level consistency contract. They are not an incidental
driver convenience. A Rust rewrite must propagate transaction watermarks through
Bolt sessions, write acknowledgements, retry loops, and eventually OLAP snapshot
freshness metadata.

Rust translation:

```rust
trait BookmarkManager: Send + Sync {
    fn update_bookmarks(
        &self,
        previous: &[Bookmark],
        new_bookmarks: &[Bookmark],
    ) -> Result<(), BookmarkError>;

    fn get_bookmarks(&self) -> Result<Vec<Bookmark>, BookmarkError>;
}

struct Bookmark {
    tx_id: TxId,
    database: Option<DatabaseName>,
    generation: Option<OlapSnapshotGeneration>,
}
```

Why it matters for Neo4j-in-Rust:

- OLTP compatibility must track committed transaction IDs.
- OLAP queries can expose snapshot generation separately, but the bridge from
  bookmarks to snapshot freshness should be explicit.
- A user asking "why did GDS not see my write?" needs a bookmark/snapshot answer,
  not a vague eventual-consistency answer.

When to use it:

- For official driver sessions and managed transactions.
- For cross-session read-your-writes semantics.
- For OLTP-to-OLAP freshness contracts.

When not to use it:

- As a replacement for full transaction isolation inside a single transaction.

Memory implications:

- Bookmark state is small. The risk is not RAM; the risk is correctness drift
  between OLTP commit IDs and OLAP generation IDs.

Testing implications:

- Add concurrency tests for bookmark manager implementations.
- Add integration tests where a write session returns a bookmark and a later
  read session supplies it.
- Add OLAP tests that report whether the requested bookmark is newer than the
  available snapshot generation.

Agentic guidance:

When generating transaction/session code, always thread bookmark state through
the API even if the first backend is single-node.

## Pattern: Bolt Compatibility Is Byte-Level And Versioned

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-docs-bolt-src`
- Language: AsciiDoc specification
- Files:
  - `modules/ROOT/pages/bolt/handshake.adoc:35-166`
  - `modules/ROOT/pages/packstream/index.adoc:1-90`

Evidence:

- A Bolt client must initiate handshake immediately after connection.
- The magic identification bytes are `60 60 B0 17`.
- The client sends exactly four protocol versions, each a big-endian 32-bit
  unsigned integer.
- Bolt 5.7 introduces manifest v1 using special version `00 00 01 FF`,
  VarInts, supported-version lists, and capability bitmasks.
- PackStream is Bolt's binary presentation format, uses Cypher-compatible
  values, starts each value with a marker byte, omits unsigned integers and
  32-bit floats by design, and uses big-endian representations.

Transferable idea:

Protocol compatibility is a byte contract before it is an API contract. A Rust
rewrite should have golden byte tests for handshake negotiation, PackStream
markers, size encodings, endianness, and unsupported reserved markers.

Rust translation:

```rust
const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const MANIFEST_V1_REQUEST: [u8; 4] = [0x00, 0x00, 0x01, 0xFF];

enum BoltHandshake {
    LegacyFourVersions([BoltVersionOffer; 4]),
    ManifestV1 {
        offered_versions: Vec<BoltVersion>,
        capabilities: u64,
    },
}
```

Why it matters for Neo4j-in-Rust:

- Official drivers will fail before Cypher parsing if the handshake is wrong.
- PackStream type choices affect value modeling across the whole database
  server, including virtual nodes, GDS results, APOC maps, and temporal/spatial
  values.
- Big-endian network encoding must be tested independent of host endianness.

Memory implications:

- PackStream can be decoded streaming; avoid materializing large lists/maps
  unless the procedure/result contract requires it.
- Use bounded decode buffers and incremental result streaming for low-RAM
  server behavior.

Testing implications:

- Golden tests for all handshake examples.
- Fuzz tests for reserved marker bytes and truncated sized values.
- Driver tests for pipelining after final manifest handshake part.

Agentic guidance:

When implementing Bolt, generate tests from the spec first. Do not trust a
manual parser without byte-level golden vectors.

## Pattern: GDS Memory Estimate Separates Projection From Algorithm

Where found:

- Repositories:
  - `gitrefrepo/Neo4j family/graph-data-science-src`
  - `gitrefrepo/Neo4j family/neo4j-gds-src`
- Languages: AsciiDoc, Java
- Files:
  - `documentation/graph-data-science.adoc:542-604`
  - `core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:103-265`

Evidence:

- GDS documentation says algorithms run on an in-memory, heap-allocated
  projection outside the main database.
- Graph projection memory is estimated with `gds.graph.project.estimate`.
- Algorithm memory is estimated separately, and algorithm estimates consider
  only execution when the graph is already in memory.
- `CSRGraphStoreFactory` estimates graph projection as a tree with node ID map,
  node properties, relationship projection loading/after-loading components,
  adjacency loading buffers, offsets, degrees, relationship properties, inverse
  indexes, and after-loading adjacency lists.

Transferable idea:

Memory estimation must be compositional. "This algorithm needs X" is incomplete
unless the estimate also states whether the graph projection is already
resident, mmap-backed, direct-I/O streamed, or scratch-built.

Rust translation:

```rust
struct MemoryEstimateTree {
    name: String,
    components: Vec<MemoryEstimateTree>,
    heap_bytes: u64,
    page_cache_bytes: u64,
    direct_io_bytes: u64,
    algorithm_state_bytes: u64,
    projection_bytes: u64,
    scratch_bytes: u64,
    delta_overlay_bytes: u64,
}
```

Why it matters for Neo4j-in-Rust:

- Neo4j GDS is the fair OLAP baseline, not Cypher over OLTP records.
- Knight Bus/Rust can differentiate itself by making the projection plane
  mmap/direct-I/O/sidecar based instead of heap-projected by default.
- GDS-compatible `estimate` procedures should report projection and algorithm
  components separately, even when the execution path is lower-RAM than Neo4j's.

When to use it:

- For every GDS projection and algorithm mode.
- For `stream`, `stats`, `mutate`, and `write` modes, because result storage
  changes memory.

When not to use it:

- Do not use a single flat required-memory string as the internal model.

Memory implications:

- Required estimate dimensions for a Rust rewrite: topology bytes, property
  sidecar bytes, vector/frontier bytes, output sidecar bytes, page-cache policy,
  direct buffers, scratch, deltas, and compaction memory.

Testing implications:

- Add exact estimate tests for tiny fixture graphs.
- Add symbolic large-graph tests such as 200M nodes/1B edges.
- Add tests proving mmap plans do not claim deterministic RSS unless page-cache
  behavior is explicitly bounded or bypassed.

Agentic guidance:

When writing any GDS-compatible API, implement `estimate` before `execute`.
Execution without memory contracts undermines the RAM-first product thesis.

## Pattern: Algorithm Stubs Preserve A Large Surface Without False Support

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-gds-src`
- Language: Java
- Files:
  - `algo-common/src/main/java/org/neo4j/gds/AlgorithmFactory.java:119-137`
  - `pipeline/src/main/java/org/neo4j/gds/ml/pipeline/NodePropertyStepExecutor.java:73-118`
  - `pipeline/src/main/java/org/neo4j/gds/ml/pipeline/stubs/AbstractStub.java:30-54`
  - `pipeline/src/main/java/org/neo4j/gds/ml/pipeline/stubs/*.java`

Evidence:

- `AlgorithmFactory.memoryEstimation` is a default method that throws
  `MemoryEstimationNotImplementedException`.
- `NodePropertyStepExecutor` maps executable node-property steps to stubs and
  takes the max estimation, while source comments call out approximation
  caveats around mutate-properties and cleanup.
- `AbstractStub` delegates memory estimation and execution to an algorithm
  facade mutate stub.
- CGC indexed 50 GDS pipeline stub files and found repeated `stub` methods
  across PageRank, FastRP, WCC, KCore, TriangleCount, NodeSimilarity,
  GraphSage, Dijkstra, Bellman-Ford, Louvain, Leiden, and more.

Transferable idea:

When the public surface is huge, compatibility needs a registry of known
procedures and explicit support states. Unknown procedure, registered but
unsupported, registered with estimate only, and fully implemented are different
states.

Rust translation:

```rust
enum ProcedureSupportState {
    RegisteredUnsupported { reason: &'static str },
    EstimateOnly,
    StreamImplemented,
    StatsImplemented,
    MutateImplemented,
    WriteImplemented,
}

trait GdsProcedureStub {
    fn spec(&self) -> &'static GdsProcedureSpec;
    fn estimate(&self, ctx: EstimateContext) -> Result<MemoryEstimateTree, GdsError>;
    fn execute(&self, ctx: ExecuteContext) -> Result<GdsResultStream, GdsError>;
}
```

Why it matters for Neo4j-in-Rust:

- "All of GDS surface" does not mean all algorithms must be optimized on day
  one. It means all names, modes, config parsers, estimate behavior, and error
  contracts are known.
- Stubs prevent accidental narrowing of the API while keeping unsupported
  behavior deterministic.
- The estimate caveat comments are a warning: if a mode estimates by
  approximation, the result must expose that approximation boundary.

Memory implications:

- Registered stubs are cheap.
- False implementation claims are expensive: they can trigger unexpected graph
  projection, materialization, or OOM.

Testing implications:

- Inventory tests should assert every GDS procedure maps to a support state.
- Unsupported registered procedures should return deterministic errors, not
  "unknown procedure."
- Estimate-only procedures should never run algorithm work.

Agentic guidance:

Future agents should build the GDS ABI registry before algorithm kernels. The
kernel work is downstream of procedure-surface truth.

## Pattern: APOC Procedures Are A Second Compatibility Surface

Where found:

- Repositories:
  - `gitrefrepo/Neo4j family/neo4j-apoc-procedures-src`
  - `gitrefrepo/Neo4j family/neo4j-apoc-src`
- Languages: Kotlin, Java
- Files:
  - `full/src/main/kotlin/apoc/nlp/aws/AWSProcedures.kt:45-95`
  - `core/src/main/java/apoc/create/Create.java:47-90`

Evidence:

- APOC procedures use `@Context` fields such as `Log` and `Transaction`.
- `apoc.nlp.aws.entities.stream` is READ mode; it verifies source/property,
  creates a client, converts input, partitions into batches of 25, and streams
  results.
- `apoc.nlp.aws.entities.graph` is WRITE mode; it accepts config keys including
  relationship type/property, `write`, and `scoreCutoff`; it can create virtual
  graph results or store them in the transaction.
- `apoc.create.node` has Cypher-version-scoped definitions and a Cypher 25
  deprecation path in favor of dynamic labels.

Transferable idea:

Neo4j-compatible applications frequently depend on APOC semantics. Even if APOC
is not part of the first storage engine, the Rust rewrite should know the
procedure ABI, mode, config, and result-shape classes it intends to support,
stub, or omit.

Rust translation:

```rust
struct ProcedureSpec {
    name: &'static str,
    mode: ProcedureMode,
    cypher_scope: CypherScope,
    config_schema: ConfigSchema,
    result_shape: ResultShape,
    support_state: ProcedureSupportState,
}
```

Why it matters for Neo4j-in-Rust:

- GDS is not the only extension surface. APOC can dominate real application
  query usage.
- READ/WRITE mode controls transaction access, locks, and whether an OLAP
  procedure is allowed to mutate projected or OLTP state.
- Version-scoped deprecations imply that Cypher 5 and Cypher 25 compatibility
  may require different procedure behavior.

Memory implications:

- Stream procedures should preserve streaming behavior.
- Graph/write procedures may create virtual or persisted graphs; these need
  budget checks before materializing node/relationship sets.

Testing implications:

- Procedure registry tests should include APOC as a separate family from GDS.
- Mode tests should verify READ procedures cannot write transaction state.
- Config parser tests should include default values and invalid type errors.

Agentic guidance:

When implementing "Neo4j compatibility," do not stop at Cypher and GDS. Create
an APOC inventory and classify each procedure.

## Pattern: Virtual Graphs Are Result Values, Not Always Stored Graphs

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-apoc-src`
- Language: Java
- Files:
  - `common/src/main/java/apoc/result/VirtualGraph.java:35-65`
  - `core/src/main/java/apoc/graph/Graphs.java:52-130`
  - `core/src/main/java/apoc/create/Create.java:390-510`

Evidence:

- `VirtualGraph` stores a result map containing name, nodes, relationships, and
  properties.
- The constructor converts iterables to sets when they are not already sets.
- `apoc.graph.from`, `fromPath`, `fromPaths`, `fromDB`, and `fromCypher` build
  virtual graph results from nodes, relationships, paths, database scans, or
  Cypher output.
- `apoc.create.vNode`, `vNodes`, `vRelationship`, and virtual-from-node
  functions create virtual entities without necessarily writing to storage.
- Cypher 25 virtual-from-node config adds validation for additional labels and
  properties.

Transferable idea:

Virtual graph/entity values are user-visible objects. They are neither purely
stored records nor purely display wrappers. A low-RAM rewrite needs a lazy or
handle-backed representation that preserves result semantics without
unnecessarily materializing the whole graph.

Rust translation:

```rust
enum VirtualEntityRef {
    ExistingNode(NodeId),
    ExistingRelationship(RelationshipId),
    SyntheticNode(Arc<VirtualNodePayload>),
    SyntheticRelationship(Arc<VirtualRelationshipPayload>),
}

struct VirtualGraphValue {
    name: String,
    nodes: VirtualEntitySet,
    relationships: VirtualEntitySet,
    properties: SmallMap<PropertyKey, Value>,
}
```

Why it matters for Neo4j-in-Rust:

- Result materialization can dominate memory even when storage is compact.
- APOC `fromDB` can enumerate the entire database as a virtual graph; that must
  be bounded, streamed, or explicitly rejected under a memory budget.
- Virtual values share serialization requirements with Bolt/PackStream.

When to use it:

- For APOC graph/entity results.
- For GDS stream/mutate/write mode result rows that reference projected graph
  values.

When not to use it:

- Do not internally convert every virtual graph to owned `HashSet<Node>` if the
  result can be cursor-backed.

Memory implications:

- The Java implementation's set conversion is behaviorally simple but can be
  heavy.
- Rust can preserve semantics with lazy ID sets, roaring bitmaps, or bounded
  result cursors.

Testing implications:

- Test virtual values over existing nodes, synthetic nodes, paths, and full DB
  scans.
- Test PackStream serialization of virtual-ish result shapes.
- Test memory-budget rejection for huge virtual graph construction.

Agentic guidance:

If future agents generate result structs, make them streaming/lazy by default
unless the API contract explicitly requires a materialized collection.

## Pattern: APOC Neighbor Traversal Uses Bitmap Frontiers

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-apoc-src`
- Language: Java
- File: `core/src/main/java/apoc/neighbors/Neighbors.java:61-100` and `244-285`

Evidence:

- `apoc.neighbors.tohop` returns empty streams when distance is less than one or
  relationship types are absent.
- It initializes `Roaring64NavigableMap` bitmaps for seen and alternating next
  frontiers.
- It parses relationship-type/direction patterns and adds neighbor internal IDs
  to bitmap frontiers.
- `apoc.neighbors.byhop` uses an array of `Roaring64NavigableMap` values, one
  per hop.

Transferable idea:

Frontier traversal does not always need heap-heavy `HashSet<Node>` structures.
Compressed bitmaps can represent visited/frontier sets compactly, especially
when node IDs are dense or mostly ordered.

Rust translation:

```rust
struct HopFrontiers {
    seen: RoaringBitmap64,
    current: RoaringBitmap64,
    next: RoaringBitmap64,
}

struct ByHopFrontiers {
    hops: Vec<RoaringBitmap64>,
}
```

Why it matters for Neo4j-in-Rust:

- BFS, WCC, k-hop APOC procedures, and GDS traversals all need frontier memory
  discipline.
- Dense node IDs from CSR snapshots map naturally to bitmap frontiers.
- Relationship type/direction selection can be pushed into adjacency cursors or
  sidecar filters.

When to use it:

- For deduplicated k-hop traversals and connected-component frontiers.
- For large graph traversals where dense IDs make compressed bitmaps efficient.

When not to use it:

- For very tiny traversals where a small vector or stack is cheaper.
- For arbitrary sparse external IDs without dense remapping.

Memory implications:

- Bitmap frontiers can be significantly smaller than hash sets.
- Per-hop arrays of bitmaps can still grow; algorithms should bound hop count
  and estimate frontier memory.

Testing implications:

- Test invalid distance/type inputs.
- Test direction/type parser behavior.
- Compare bitmap frontier results against a simple set-based oracle.

Agentic guidance:

When generating traversal algorithms for the Rust rewrite, default to dense-ID
bitsets/roaring bitmaps for dedupe-heavy frontiers before reaching for hash
sets.

## Pattern: Dynamic Create And Virtual Create Need Versioned Semantics

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-apoc-src`
- Language: Java
- File: `core/src/main/java/apoc/create/Create.java:47-90` and `390-510`

Evidence:

- `apoc.create.node` exists in WRITE mode for Cypher 5.
- The Cypher 25 scoped version is deprecated in favor of Cypher dynamic label
  syntax.
- `apoc.create.addLabels` has similar Cypher-version-scoped behavior.
- Virtual node and relationship procedures/functions create synthetic entities
  with labels, properties, relationship type, and endpoints.
- Cypher 25 virtual-from-node config validates `additionalLabels` as a
  `LIST<STRING>` and `additionalProperties` as a `MAP`.

Transferable idea:

Compatibility is versioned. A Rust rewrite should represent procedure semantics
as a matrix over procedure name, Cypher version, mode, deprecation metadata,
config schema, and result type.

Rust translation:

```rust
struct VersionedProcedureSpec {
    name: &'static str,
    mode: ProcedureMode,
    cypher_scope: &'static [CypherVersion],
    deprecated_by: Option<&'static str>,
    config: ConfigSchema,
    result_shape: ResultShape,
}
```

Why it matters for Neo4j-in-Rust:

- "Same Neo4j API" changes meaning across Neo4j/Cypher versions.
- The Rust rewrite should avoid hard-coding one global procedure definition when
  the upstream surface itself is version-scoped.
- This matters for driver metadata, warnings, deprecation notices, and query
  planning.

Memory implications:

- Versioned metadata is small.
- The real RAM risk is virtual entity creation over large property maps or
  result batches; validate and stream where possible.

Testing implications:

- Add tests for Cypher 5 vs Cypher 25 procedure availability/deprecation.
- Add config type-validation tests.
- Add virtual relationship serialization tests.

Agentic guidance:

When generating compatibility registries, include version scope and deprecation
fields from the beginning. Retrofitting them later will create ambiguous API
behavior.

## Batch 02 Synthesis For The Rust Rewrite

The central conclusion from this batch:

```text
Neo4j compatibility is a stack of contracts, not a single storage format.
```

The contracts found here are:

- OLTP record contracts: fixed records, relationship chains, dense-node groups.
- Protocol contracts: Bolt magic bytes, version negotiation, PackStream markers.
- Driver contracts: routing tables, bookmarks, retry timings, fetch sizes.
- GDS contracts: projected graph memory, algorithm memory, estimate modes,
  algorithm stubs, result/mutate/write semantics.
- APOC contracts: procedure modes, virtual graphs, version-scoped procedures,
  bitmap-frontier traversal helpers.

For a RAM-first Rust rewrite, the architecture implication is:

```text
Keep OLTP record compatibility and driver/procedure ABI compatibility explicit,
but do not force OLAP to execute directly over OLTP records. Derive a bounded,
generationed analytical plane whose estimates separate projection, algorithm
state, page-cache/direct-I/O policy, deltas, and result materialization.
```

The strongest next research targets after this batch are:

1. Remaining Neo4j official drivers for cross-language differences in routing,
   bookmarks, retry, fetch-size, and notification behavior.
2. GDS procedure inventory generation from `@Procedure` annotations and facade
   classes.
3. APOC procedure inventory generation with mode, Cypher scope, and virtual
   result classification.
4. Neo4j transaction log/checkpoint source paths to connect bookmarks and OLAP
   snapshot generations to durable recovery semantics.

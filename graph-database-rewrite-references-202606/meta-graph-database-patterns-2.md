# Graph Database Patterns 2: Engines, RDF Stores, Property Graphs, and Query Runtimes

Date: 2026-07-06

Role: Agent 2 in the five-agent corpus build.

Scope: graph database engines, RDF/property-graph systems, and query-language engines, with a bias toward patterns useful when rewriting a Neo4j-like graph database in Rust with lower RAM usage.

This file is intentionally repo-grounded. Graph-tool output was used only as navigation evidence. Important engineering claims below are tied to direct source reads where available. Some repositories received only path-level or grep-level inspection in this timebox; those gaps are called out at the end.

## Design Bias for a Rust Rewrite

The common lesson across these systems is not "copy Neo4j." It is: split the graph database into small storage facts, a narrow transactional storage API, streaming traversal operators, and a catalog/query layer that can evolve without forcing every node, edge, property, path, or schema object into memory.

For a lower-RAM Rust rewrite, prefer:

- fixed-width node and edge cores stored in pages or ordered key-value ranges;
- overflow stores for labels, long properties, and high-degree adjacency;
- typed IDs and tokenized label/property/type names;
- iterator/cursor APIs that borrow from snapshots instead of cloning graph values;
- path expansion operators that can stream node/edge IDs and materialize full paths only when the query needs them;
- query-planning boundaries that separate syntax, semantic binding, logical planning, physical planning, and execution;
- WAL/checkpoint/snapshot boundaries that are testable in isolation;
- feature compatibility layers for Cypher/Gremlin/SPARQL/GSQL-style imports without letting compatibility dictate the internal storage layout.

## Pattern Index

| Pattern | Best evidence repos | Main Rust rewrite use |
|---|---|---|
| Fixed core records plus overflow stores | `ongdb-src`, `arcadedb-src`, `orientdb-src` | Keep hot node/edge fields compact and page-cacheable. |
| Prefix adjacency and reverse adjacency keyspaces | `indradb-src`, `typedb-src`, `arangodb-src` | Make edge scans ordered, bounded, and mmap/RocksDB-friendly. |
| CSR/columnar relation tables | `kuzu-src` | Reduce RAM for analytical and repeated traversal workloads. |
| Sparse-matrix graph topology | `redisgraph-src`, `falkordb-src` | Fast set-oriented traversal, but beware path materialization and matrix memory. |
| Store API narrower than graph API | `indradb-src`, `cayley-src`, `apache-jena-src`, `eclipse-rdf4j-src` | Keep storage embeddable and testable. |
| Parser/binder/planner/executor/storage layering | `kuzu-src`, `memgraph-src`, `arangodb-src`, `redisgraph-src`, `falkordb-src`, `age-src`, `orientdb-src` | Prevent Cypher syntax from contaminating storage. |
| Traversal as resumable cursors | `arangodb-src`, `ongdb-src`, `memgraph-src`, `cayley-src`, `kuzu-src` | Stream expansions with bounded allocations. |
| Chunked graph scans for GDS | `kuzudb__kuzu` | Run graph algorithms over batches of IDs/properties instead of materializing adjacency. |
| VM-backed buffer manager and checkpointed free lists | `kuzudb__kuzu` | Bound physical RAM while mapping logical graph pages into stable virtual regions. |
| Factorized query plans and semi-mask pruning | `kuzudb__kuzu` | Delay flattening and push node masks into scans/recursive expansion. |
| Index substitution and late scan replacement | `falkordb-src`, `kuzu-src`, `janusgraph-src`, `indradb-src`, `apache-hugegraph-src` | Use indexes where selective, not everywhere. |
| Snapshot, WAL, checkpoint, and MVCC separation | `typedb-src`, `dgraph-src`, `arcadedb-src`, `ongdb-src`, `arangodb-src` | Make durability a small subsystem, not a global side effect. |
| Catalog/schema/token versioning | `surrealdb-src`, `kuzu-src`, `age-src`, `falkordb-src`, `janusgraph-src`, `apache-hugegraph-src` | Decouple user names from storage IDs and plan-cache invalidation. |
| Embedded/server split | `arcadedb-src`, `indradb-src`, `typedb-src`, `eclipse-rdf4j-src`, `apache-tinkerpop-src`, `helix-db-src` | One engine, multiple facades. |
| Observability as an engine surface | `falkordb-src`, `arangodb-src`, `memgraph-src`, `ongdb-src`, `nebula-src` | Debug plans, memory, page cache, and WAL before optimizing blindly. |
| Migration and compatibility adapters | `age-src`, `apache-tinkerpop-src`, `apache-jena-src`, `eclipse-rdf4j-src`, `orientdb-src`, `surrealdb-src`, `tigergraph-ecosys-src` | Preserve user workflows while replacing internals. |

## Pattern 1: Fixed Core Records Plus Overflow Stores

### Where Found

- `gitrefrepo/ongdb-src/community/kernel/src/main/java/org/neo4j/kernel/impl/store/record/NodeRecord.java`
  - Lines 49-56 define a compact node record core with `nextRel`, `labels`, `dynamicLabelRecords`, `isLight`, and `dense`.
  - Lines 62-70 initialize the hot fields without immediately loading dynamic labels.
  - Lines 113-129 distinguish inline label fields from dynamic label records.
- `gitrefrepo/ongdb-src/community/kernel/src/main/java/org/neo4j/kernel/impl/store/record/RelationshipRecord.java`
  - Lines 46-56 store endpoints, type, prev/next links for both endpoint chains, and first-in-chain flags.
  - Lines 88-102 initialize the relationship record in one fixed-core step.
- `gitrefrepo/ongdb-src/community/kernel/src/main/java/org/neo4j/kernel/impl/store/NodeStore.java`
  - Lines 94-108 wire `NodeStore` to a `DynamicArrayStore` for overflow labels.
  - Lines 117-135 load heavy dynamic labels only when the label field points to dynamic records.
- `gitrefrepo/ongdb-src/community/io/src/main/java/org/neo4j/io/pagecache/PageCache.java`
  - Lines 50-57 describe page caching as the RAM boundary.
  - Lines 63-86 map files by page size.
- `gitrefrepo/arcadedb-src/engine/src/main/java/com/arcadedb/graph/MutableVertex.java`
  - Lines 42-45 keep out-edge and in-edge head chunks as RIDs.
  - Lines 124-141 expose head chunk getters/setters and mark records dirty.
- `gitrefrepo/arcadedb-src/engine/src/main/java/com/arcadedb/graph/MutableEdge.java`
  - Lines 43-50 store edge endpoints as RIDs.
  - Lines 94-119 expose endpoints.
- `gitrefrepo/orientdb-src/core/src/main/java/com/orientechnologies/orient/core/record/impl/OVertexDocument.java`
  - Grep evidence showed `out_` and `in_` edge fields, `ORidBag`, and conversion paths around lines 39-40, 151-159, 352-357, 635-717.
- `gitrefrepo/orientdb-src/core/src/main/java/com/orientechnologies/orient/core/storage/ridbag/sbtree/OSBTreeRidBag.java`
  - Grep evidence showed tree-backed RidBag support and deferred changes around lines 77, 417-423, 520-525, 749, 848.

### Short Shape

```text
NodeCore {
  id
  first_relationship_or_group
  first_property
  inline_label_bits_or_overflow_id
  flags: dense, in_use
}

RelationshipCore {
  id
  source
  target
  type_token
  prev_next_for_source
  prev_next_for_target
  first_property
}

Overflow stores:
  dynamic labels
  property chains
  high-degree adjacency chunks
```

### Why It Matters for Neo4j-in-Rust

Neo4j-style stores are memory efficient because the hot graph topology is not a heap of objects. A node can be traversed by reading a small fixed-width record and following pointers/chunks only when needed. This is directly relevant for a Rust rewrite: do not represent the engine as `Vec<Node { labels: Vec<_>, props: HashMap<_, _>, edges: Vec<_> }>` except maybe in tests. That shape duplicates names, allocates per entity, and cannot page cold data out.

### When to Use

Use this when:

- the engine must store billions of nodes/edges;
- traversal over topology is more common than full property hydration;
- labels/types can be tokenized;
- updates can tolerate overflow allocation and pointer repair.

### When Not to Use

Do not use this as the only format when:

- the workload is mostly columnar analytics over a few properties;
- relation tables are naturally append-only and dense;
- the engine is a pure RDF triple store where subject-predicate-object permutations are the main access path.

### Rust Translation

Use fixed-layout structs for storage records, but avoid relying on Rust struct layout unless explicitly controlled.

```rust
#[repr(C)]
struct NodeCoreRecord {
    next_rel: u64,
    next_prop: u64,
    label_field: u64,
    flags: u8,
}

#[repr(C)]
struct RelationshipCoreRecord {
    source: u64,
    target: u64,
    type_id: u32,
    source_prev: u64,
    source_next: u64,
    target_prev: u64,
    target_next: u64,
    next_prop: u64,
}
```

Prefer explicit codec functions over transmuting structs:

```rust
fn encode_node_core_record(record: &NodeCoreRecord, dst: &mut [u8]) { /* endian checked */ }
fn decode_node_core_record(src: &[u8]) -> Result<NodeCoreRecord, StoreError> { /* bounds checked */ }
```

Keep property values out of the topology record. A good first Rust split is:

- `node_store`: fixed records and dense-node flags;
- `rel_store`: fixed records and endpoint chains;
- `token_store`: label/type/property name tokens;
- `property_store`: property record chains or column families;
- `overflow_store`: dynamic label arrays and long strings;
- `adjacency_chunk_store`: optional high-degree adjacency chunks.

### Risks

- Relationship chain repair is hard. Every edge insert/delete touches both endpoint chains.
- Dense-node transition must be atomic and testable.
- Dynamic-label or overflow records can leak if transaction rollback misses one pointer.
- Fixed record formats become compatibility promises. Version the page format immediately.

### Memory, Concurrency, and Testing Implications

Memory:

- Good: hot topology can be page-cache backed.
- Bad: random relationship chains can become IO-heavy without clustering.

Concurrency:

- Lock endpoints in canonical ID order for edge updates.
- Test deadlock scenarios for concurrent edge create/delete touching the same two vertices.

Testing:

- Fuzz record encode/decode round trips.
- Crash after each pointer update and verify recovery either replays or rolls back.
- Test dense-node threshold transitions with concurrent inserts.

### Future-Agent Application

If a future agent designs the Rust store, start with this record split but do not import Neo4j's exact on-disk layout. The goal is lower RAM and clean Rust ownership, not binary compatibility with ONGDB.

## Pattern 2: Prefix Adjacency and Reverse Adjacency Keyspaces

### Where Found

- `gitrefrepo/indradb-src/lib/src/rdb/datastore.rs`
  - Lines 12-21 declare RocksDB column families for vertices, edge ranges, reversed edge ranges, vertex properties, edge properties, and property-value indexes.
- `gitrefrepo/indradb-src/lib/src/rdb/managers.rs`
  - Lines 134-155 write edges into both normal and reversed edge range managers.
  - Lines 180-205 define edge-range keys as outbound ID plus type plus inbound ID.
  - Lines 226-250 perform prefix scans by root/type.
  - Lines 502-575 and 594-681 build property-value index keys with property name, serialized value/hash, and entity ID.
- `gitrefrepo/typedb-src/encoding/graph/thing/vertex_object.rs`
  - Lines 23-32 define fixed-width object vertex keys: prefix, type ID, object ID.
  - Lines 82-89 serialize to a compact byte array.
  - Lines 106-128 decode only when the prefix and length match.
- `gitrefrepo/typedb-src/encoding/graph/thing/edge.rs`
  - Lines 36-45 define `ThingEdgeHas` as owner plus attribute.
  - Lines 52-57 define prefix lengths for different scan modes.
  - Lines 70-107 construct prefixes from type/object/object+type.
- `gitrefrepo/arangodb-src/arangod/RocksDBEngine/RocksDBKey.cpp`
  - Lines 250-276 build edge-index keys from index ID, vertex ID, separator, document ID, and high-byte prefix-extractor marker.

### Short Shape

```text
edge_cf:
  key = [src_id][type_id][dst_id]
  val = edge_payload_or_empty

reverse_edge_cf:
  key = [dst_id][type_id][src_id]
  val = edge_payload_or_empty

property_value_cf:
  key = [property_token][encoded_value_or_hash][entity_id]
  val = empty_or_property_pointer
```

### Why It Matters for Neo4j-in-Rust

Prefix keyspaces are the simplest path to low RAM if the engine uses RocksDB, sled-like trees, redb, fjall, LMDB, or custom B+trees. You can scan a node's outbound or inbound adjacency without building a heap-resident adjacency list. IndraDB is small and clean enough to use as a teaching model: normal and reversed edge ranges are explicit, property-value indexes are separate, and edge payloads can be empty if all important topology is in the key.

Typedb adds the more advanced lesson: design byte prefixes for every common access pattern. Do not just serialize a Rust struct. Think in sorted bytes.

### When to Use

Use this when:

- adjacency scans dominate;
- you can tolerate two writes per edge for forward and reverse access;
- edge IDs are not required for every traversal;
- the storage engine has efficient prefix iteration.

### When Not to Use

Avoid as the only adjacency model when:

- queries often need "all relationships by global insertion order";
- edge updates require large property payload rewrites in the key-value value;
- high-degree nodes cause huge contiguous prefix scans that need CSR or matrix compression.

### Rust Translation

Build a key codec module with no graph logic:

```rust
fn encode_outbound_edge_key(src: NodeId, ty: RelTypeId, dst: NodeId, dst_key: &mut Vec<u8>);
fn encode_inbound_edge_key(dst: NodeId, ty: RelTypeId, src: NodeId, dst_key: &mut Vec<u8>);
fn scan_outbound_edges_by_prefix(snapshot: &ReadSnapshot, src: NodeId, ty: Option<RelTypeId>) -> EdgeIter;
```

Use big-endian numeric encoding so lexical order matches numeric order. Reserve one byte for key-kind/version at the front. Keep value payload small:

- empty value for unqualified edges;
- edge ID or property pointer for property-carrying edges;
- tombstone or MVCC metadata only if required by the transaction model.

### Risks

- Prefix format mistakes are permanent storage-format bugs.
- Value-hash indexes need collision handling. IndraDB hashes larger JSON property values for index keys, which is reasonable, but the Rust version must verify equality after lookup.
- Reverse keyspaces double write amplification.
- Range deletes and compaction can surprise latency.

### Memory, Concurrency, and Testing Implications

Memory:

- Excellent for cold adjacency. Iterators can stream from block cache.
- Keep per-iterator buffers reusable and avoid `Vec<Edge>` materialization.

Concurrency:

- Edge writes must update forward, reverse, and indexes in one transaction.
- Snapshot iterators must see a stable view while compaction proceeds.

Testing:

- Property-test key ordering.
- Round-trip every key codec.
- Compare prefix-scan results against a simple in-memory oracle for random graphs.
- Crash/recovery test partial forward/reverse/index updates.

### Future-Agent Application

Use IndraDB and TypeDB as the first reference pair for any Rust key-value store design. IndraDB shows understandable range managers; TypeDB shows production-grade byte-key discipline.

## Pattern 3: CSR and Columnar Relation Tables

### Where Found

- `gitrefrepo/kuzu-src/src/include/storage/table/node_table.h`
  - Lines 23-40 define `NodeTableScanState`.
  - Lines 44-55 define insert state with node ID, primary key vector, and index insert states.
  - Lines 110-180 expose scan, lookup, insert, update, delete, visibility, index, commit, checkpoint, and rollback operations.
- `gitrefrepo/kuzu-src/src/include/storage/table/rel_table.h`
  - Lines 17-39 define `RelTableScanState` with direction, bound node, CSR offset column, CSR length column, random lookup flag, and local scan state.
  - Lines 94-106 define relation insert state with source and destination vectors.
  - Lines 141-180 expose relation scans, insert/update/delete, detach delete, and node-has-rels checks.
- `gitrefrepo/kuzu-src/src/include/storage/table/column.h`
  - Lines 19-32 define a typed `Column` over file handle, memory manager, shadow file, and compression metadata.
  - Lines 43-67 expose lookup/scan APIs.
  - Lines 98-144 expose checkpoint hooks.
- `gitrefrepo/kuzu-src/src/include/processor/result/factorized_table.h`
  - Lines 26-52 define `DataBlock`.
  - Lines 89-178 define `FactorizedTable`, block append/scan/lookup, overflow buffer, and memory manager usage.

### Short Shape

```text
RelTable {
  src_table_id
  dst_table_id
  csr_offset_column
  csr_length_column
  property_columns
}

Scan(bound_node, direction):
  offset = offset_column[bound_node]
  len = length_column[bound_node]
  stream neighbors from contiguous relation vectors
```

### Why It Matters for Neo4j-in-Rust

Kuzu is a strong counterweight to Neo4j's relationship-record chains. For read-heavy graph analytics and repeated pattern matching, CSR-style relation tables can reduce pointer chasing and compress adjacency. This is especially relevant if the Rust rewrite wants lower RAM and high traversal throughput for relatively stable graphs.

Neo4j-like record chains are good for transactional point updates. CSR is good for compact scans. A serious Rust design can use both:

- write-optimized delta adjacency for recent updates;
- checkpointed CSR segments for stable committed graph partitions;
- merge iterators at query time.

### When to Use

Use CSR/columnar relations when:

- graph updates are batchy or can be checkpointed;
- relationship types are known and stable;
- queries often expand many nodes of the same relation type;
- property projection should read only selected columns.

### When Not to Use

Do not make CSR the first storage for:

- high-frequency single-edge writes;
- arbitrary edge property maps with many sparse keys;
- workloads that require immediate cheap delete without tombstone accumulation.

### Rust Translation

Use typed relation tables:

```rust
struct RelationTable {
    from_table: TableId,
    to_table: TableId,
    offsets: Column<u64>,
    lengths: Column<u32>,
    dst_nodes: Column<NodeId>,
    edge_ids: Option<Column<EdgeId>>,
    properties: PropertyColumnSet,
}
```

Keep mutable deltas separate:

```rust
struct RelationSnapshot {
    base_csr: Arc<CsrSegment>,
    write_delta: Arc<AdjacencyDelta>,
}
```

The executor should read base and delta through one cursor, not concatenate them into a vector.

### Risks

- Deletes create tombstones unless compaction is designed early.
- Converting from write-delta to CSR checkpoint is a complex background job.
- Query planner must know whether a relation is in CSR, key-value ranges, or both.

### Memory, Concurrency, and Testing Implications

Memory:

- Excellent for stable graph topology and column projection.
- Avoid factorized-result tables becoming the new RAM sink. Kuzu's factorized table pattern is powerful, but a Rust rewrite should impose memory budgets on every operator.

Concurrency:

- Readers should hold immutable CSR snapshots.
- Writers append to deltas and publish new snapshots atomically after checkpoint.

Testing:

- Differential-test CSR expansion against the key-value adjacency store.
- Check snapshot visibility across checkpoint/rollback.
- Benchmark high-degree and low-degree nodes separately.

### Future-Agent Application

If later agents design storage tiers, propose "KV delta plus CSR checkpoint" as a concrete hybrid. This gives Neo4j-like transactional freshness without forcing all committed adjacency into linked records.

## Pattern 4: Sparse Matrix Topology With Optional Path Materialization

### Where Found

- `gitrefrepo/redisgraph-src/src/graph/graph.h`
  - Lines 49-62 define graph data blocks for nodes/edges plus adjacency matrix, label matrices, relation matrices, zero matrix, graph lock, sync matrix function, and stats.
  - Lines 64-90 expose locking and synchronization APIs.
- `gitrefrepo/redisgraph-src/src/graph/entities/node.h`
  - Lines 30-34 define a node as attributes plus entity ID.
- `gitrefrepo/redisgraph-src/src/graph/entities/edge.h`
  - Lines 32-39 define an edge as attributes, ID, relationship name/ID, source ID, and destination ID.
- `gitrefrepo/redisgraph-src/src/execution_plan/ops/op_cond_var_len_traverse.c`
  - Lines 97-136 construct conditional variable-length traversal.
  - Lines 139-185 optimize traversal by disabling path collection when the edge is not referenced and filters do not require it.
- `gitrefrepo/falkordb-src/src/graph/graph.h`
  - Lines 49-62 mirror the matrix-backed graph structure with `Delta_Matrix` and tensors for relations.
  - Lines 79-102 expose write-lock timeout and write-lock status APIs.

### Short Shape

```text
Graph {
  nodes: DataBlock
  edges: DataBlock
  adjacency: SparseMatrix
  labels: SparseMatrix[]
  relations: SparseMatrix[] or Tensor
}

VariableLengthTraverse:
  if path is not projected and no filter needs edge objects:
    stream reachable endpoints only
  else:
    collect path state
```

### Why It Matters for Neo4j-in-Rust

RedisGraph/FalkorDB show that a graph engine can treat topology as algebra. Sparse matrix operations are attractive for set-oriented Cypher patterns and batched traversal. The most transferable idea for a lower-RAM Rust rewrite is not necessarily GraphBLAS itself; it is the executor rule: do not materialize path objects unless the query asks for them.

### When to Use

Use matrix topology when:

- graph is mostly in memory;
- query workload is set-oriented and relation-type filtering is common;
- the engine benefits from vectorized sparse operations;
- writes can synchronize matrix deltas safely.

### When Not to Use

Do not use matrix-first storage when:

- the primary goal is out-of-core low-RAM storage;
- edge properties are large and frequently read with topology;
- transaction isolation must be MVCC across matrix mutations and property writes.

### Rust Translation

Even if the Rust rewrite does not adopt sparse matrices, adopt the "path laziness" rule:

```rust
enum TraversalOutputMode {
    EndpointsOnly,
    EdgesOnly,
    FullPath,
}

fn choose_path_output_mode(plan: &ExpandPlan) -> TraversalOutputMode {
    if plan.projects_path || plan.filters_on_edge_properties {
        TraversalOutputMode::FullPath
    } else {
        TraversalOutputMode::EndpointsOnly
    }
}
```

### Risks

- Matrix deltas and transaction rollback can become hard to reason about.
- High-cardinality relationship types can create many sparse matrices.
- Path queries can explode if the optimized no-path path is accidentally bypassed.

### Memory, Concurrency, and Testing Implications

Memory:

- Matrices may be compact for topology but poor for sparse per-edge property maps.
- Always measure memory by relation type and degree distribution.

Concurrency:

- Matrix sync must be observable and lock-bounded.
- FalkorDB's explicit write-lock status and timeout APIs are a reminder to expose lock health.

Testing:

- Verify that queries not projecting paths do not allocate path vectors.
- Test variable-length traversals with filters that do and do not reference edges.

### Future-Agent Application

Use RedisGraph/FalkorDB primarily for executor rules and observability, not as the default storage model for a low-RAM Rust rewrite.

## Pattern 5: Store API Narrower Than Graph API

### Where Found

- `gitrefrepo/indradb-src/lib/src/database.rs`
  - Lines 10-24 define `DynIter` and the object-safe `Transaction<'a>` trait.
  - Lines 24-196 list narrow storage methods for vertices, edge ranges, reversed edge ranges, properties, indexes, sync, bulk insert, and index creation.
  - Lines 198-207 define `Datastore` with an associated transaction type.
  - Lines 221-287 show `Database` methods wrapping transactions.
  - Lines 380-420 recursively execute `Query` over the storage trait.
- `gitrefrepo/cayley-src/graph/quadstore.go`
  - Lines 17-23 describe `QuadStore` as the only interface needed by the rest of the stack.
  - Lines 55-82 define `QuadIndexer`.
  - Lines 90-111 define the `QuadStore` interface.
- `gitrefrepo/cayley-src/graph/iterator/iterator.go`
  - Lines 39-77 define base iterator behavior.
  - Lines 113-148 define `Shape` with iterate, lookup, stats, optimize, and sub-iterator methods.
- `gitrefrepo/apache-jena-src` and `gitrefrepo/eclipse-rdf4j-src`
  - Direct source inspection in this pass was limited, but both repos are relevant because RDF stacks traditionally split model/repository/sail APIs from storage engines and SPARQL evaluation.

### Short Shape

```text
Storage transaction trait:
  get_vertex(id)
  scan_vertices(range)
  scan_edges(prefix)
  get_property(entity, key)
  scan_property_index(key, value)
  commit/sync

Graph API:
  create node
  match pattern
  path query
  Gremlin/Cypher/SPARQL facade
```

### Why It Matters for Neo4j-in-Rust

The storage layer should not know Cypher AST nodes, path variables, or HTTP requests. IndraDB's trait is a clean reference because it can support memory and RocksDB backends through one transaction surface. Cayley makes the same point for RDF quads: if the store exposes lookup and iterator shapes, query layers can optimize without owning storage internals.

### When to Use

Use this always. It is the main guardrail against a graph rewrite becoming a monolith.

### When Not to Use

The only exception is a prototype, and even then the prototype should name the boundary it is violating.

### Rust Translation

Prefer a storage trait with associated iterator types when performance matters, and boxed trait objects when plugin ergonomics matter. IndraDB intentionally avoids generics in `Transaction` for plugin compatibility. A Rust rewrite may need both:

```rust
trait GraphSnapshot {
    type EdgeIter<'a>: Iterator<Item = Result<EdgeRef, StoreError>>
    where
        Self: 'a;

    fn scan_edges_from_node<'a>(&'a self, node: NodeId, dir: Direction) -> Self::EdgeIter<'a>;
}

trait DynGraphSnapshot {
    fn scan_edges_boxed(&self, node: NodeId, dir: Direction) -> Box<dyn Iterator<Item = Result<EdgeRef, StoreError>> + '_>;
}
```

### Risks

- Too narrow a trait forces inefficient query execution.
- Too wide a trait becomes an engine API dump.
- Object-safe boxed iterators can allocate per operator if not pooled.

### Memory, Concurrency, and Testing Implications

Memory:

- Streaming iterators avoid relation materialization.
- Trait boundaries must expose borrowed values or arena-backed values where possible.

Concurrency:

- The trait should distinguish read snapshot, write transaction, and admin/catalog transaction.

Testing:

- Run the same graph API tests against memory and persistent backends.
- Build a fake store that records method calls to test planner access paths.

### Future-Agent Application

Before adding any query-language feature, ask: "What storage primitive does this need?" If the answer is "pass the AST into storage," stop and design the primitive.

## Pattern 6: Query Pipeline Boundaries: Parser, Binder, Planner, Executor, Storage

### Where Found

- `gitrefrepo/kuzu-src`
  - Path inspection showed distinct `src/parser`, `src/binder`, `src/planner`, `src/processor`, `src/storage`, `src/catalog`, and `src/transaction` layers.
- `gitrefrepo/memgraph-src/src/query/plan/operator.hpp`
  - Lines 73-102 define `Cursor` with virtual `Pull`, `Reset`, and `Shutdown`.
  - Lines 104-116 define custom allocator ownership for cursors.
  - Lines 118-180 list logical/physical operators including scans, expand, variable expand, filters, deletes, joins, aggregates, and parallel operators.
- `gitrefrepo/arangodb-src/arangod/Aql/TypedAstNodes.h`
  - Lines 328-356 define `TraversalNode` with internal traversal fields and output variables.
  - Lines 358-386 define `ShortestPathNode`.
- `gitrefrepo/redisgraph-src/src/execution_plan/ops/op_cond_var_len_traverse.c`
  - Lines 97-136 construct a physical traversal op from graph, algebraic expression, flags, and record indices.
- `gitrefrepo/falkordb-src/src/execution_plan/optimizations/utilize_indices.c`
  - Lines 540-563 replace scan ops with index scans.
  - Lines 580-690 reduce conditional traversal and filters into edge index scans.
- `gitrefrepo/age-src/src/backend/parser/cypher_clause.c`
  - Lines 6482-6487 enforce directed relationships in `CREATE`.
  - Lines 6492-6512 require/create relationship labels.
  - Lines 6514-6529 open label relations and build default ID expressions.
- `gitrefrepo/age-src/src/backend/executor/cypher_create.c`
  - Lines 360-385 set edge endpoint IDs based on direction.
  - Lines 401-421 insert edge tuple fields.
  - Lines 427-453 construct edge return values only if the query target needs them.
- `gitrefrepo/orientdb-src/core/src/main/java/com/orientechnologies/orient/core/sql/executor`
  - Path/grep evidence found create/delete/move edge planners, edge traversers, pointer update steps, and cast-to-edge/vertex steps.

### Short Shape

```text
parse: text -> AST
bind: AST -> symbols, labels, property tokens, relation tables
logical plan: graph operators independent of storage
physical plan: access paths, indexes, join order, traversal direction
executor: streaming cursors/operators
storage: snapshot and transaction primitives
```

### Why It Matters for Neo4j-in-Rust

Cypher compatibility is a trap if the parser directly mutates storage. AGE, RedisGraph, FalkorDB, Memgraph, ArangoDB, and Kuzu all show named query layers. The rewrite should keep the parser boring and make the physical planner decide whether a pattern uses:

- node label scan;
- property index seek;
- edge prefix scan;
- CSR relation scan;
- matrix operation;
- path expansion cursor.

### When to Use

Use this for any engine that wants more than a toy query language.

### When Not to Use

For a tiny embedded graph API, parser/planner layers can be skipped, but execution should still be streaming and storage-independent.

### Rust Translation

```rust
enum LogicalOp {
    NodeScan { label: Option<LabelId> },
    Expand { dir: Direction, rel_type: Option<RelTypeId> },
    Filter { expr: ExprId },
    Project { columns: Vec<ColumnId> },
}

enum PhysicalOp {
    NodeLabelIndexScan(NodeLabelIndexPlan),
    EdgePrefixExpand(EdgePrefixExpandPlan),
    CsrExpand(CsrExpandPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
}
```

The parser should not allocate engine objects. The binder should intern names into tokens. The physical planner should own cost/selectivity estimates.

### Risks

- Too many layers too early can slow initial delivery.
- Planner/executor contracts become stale if storage evolves faster than query planning.
- Query ASTs can grow large; intern strings and keep spans compact.

### Memory, Concurrency, and Testing Implications

Memory:

- Plans should be small and cacheable.
- Executors should reuse row batches and arenas.

Concurrency:

- Plan cache invalidation must use schema/catalog versioning.

Testing:

- Golden tests for parse/bind.
- Plan-shape tests for common queries.
- Execution differential tests against a simple interpreter.

### Future-Agent Application

Design the Rust query layer around "plan shapes" rather than "execute AST recursively." Use Memgraph's cursor/operator model and Kuzu's layer names as a guide.

## Pattern 7: Traversal as Resumable Cursors, Not Vectors

### Where Found

- `gitrefrepo/arangodb-src/arangod/Graph/Cursors/EdgeCursor.h`
  - Lines 41-71 define an abstract traversal edge cursor with callback-based reads, batch API, `rearm` by vertex/depth, `hasMore`, HTTP request count, current vertex, and current depth.
- `gitrefrepo/arangodb-src/arangod/Graph/Cursors/SingleServerEdgeCursor.h`
  - Lines 60-91 define `LookupInfo` with index accessor, index iterator, expression, covering index positions, and rearm-by-vertex.
  - Lines 95-141 wire resource monitor, transaction, expression context, and cache usage.
- `gitrefrepo/ongdb-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/RelationshipTraversalCursor.java`
  - Lines 41-64 define a cursor for relationships of a single node with origin and neighbor references.
- `gitrefrepo/memgraph-src/src/query/path.hpp`
  - Lines 24-39 describe path as a vertex/edge alternating value with allocator.
  - Lines 111-126 enforce path expansion invariants.
  - Lines 135-150 expose shrink and path vectors.
- `gitrefrepo/cayley-src/query/path/path.go`
  - Lines 69-75 define `Path` as a stack of morphisms over a quad store.
  - Lines 113-145 clone/reverse path morphism state.
- `gitrefrepo/cayley-src/query/path/morphism_apply_functions.go`
  - Lines 302-335 apply recursive morphisms and annotate depth tags.
- `gitrefrepo/kuzu-src/src/include/storage/table/rel_table.h`
  - Lines 17-39 define scan state with bound node, direction, CSR columns, and local scan state.

### Short Shape

```text
cursor.rearm(vertex, depth)
while cursor.has_more():
  cursor.read_next(batch, callback)
```

Or in Rust:

```rust
trait ExpandCursor {
    fn rearm_expand_cursor(&mut self, origin: NodeId, depth: u16);
    fn next_edge_reference(&mut self) -> Result<Option<EdgeRef>, QueryError>;
}
```

### Why It Matters for Neo4j-in-Rust

Every `MATCH (a)-[:T*1..5]->(b)` can become a memory disaster. Traversal must be represented as resumable cursors and small frontier structures, not vectors of all possible paths. ArangoDB's `rearm` concept is particularly transferable: one cursor object can be reused for many start vertices and depths. Memgraph's explicit path value shows when full paths are actually needed.

### When to Use

Use for every expand, shortest path, variable-length traversal, and neighbor lookup.

### When Not to Use

Only materialize vectors at API boundaries or when the query explicitly projects `path`, `relationships(path)`, or `nodes(path)`.

### Rust Translation

Use a frontier abstraction with memory budget:

```rust
struct ExpansionFrontier {
    current: Vec<NodeId>,
    next: Vec<NodeId>,
    visited: Option<RoaringBitmap>,
    budget: OperatorBudget,
}
```

Do not store `Vec<Path>` by default. Store predecessor links only for shortest path or when path reconstruction is required:

```rust
struct PathBackpointer {
    node: NodeId,
    edge: EdgeId,
    previous: Option<BackpointerId>,
}
```

### Risks

- Path semantics differ between Cypher, Gremlin, SPARQL property paths, and custom APIs.
- BFS shortest path and DFS variable expansion need different memory profiles.
- Dedup/visited semantics must be query-language specific.

### Memory, Concurrency, and Testing Implications

Memory:

- Add explicit per-operator memory budgets.
- Track frontier sizes and path materialization counts.

Concurrency:

- Cursors should borrow a read snapshot and not outlive it.
- Avoid holding write locks during traversal.

Testing:

- Tests for zero-length paths, self-loops, multi-edges, direction changes, and max-depth.
- Allocation-count tests proving endpoint-only expansion does not allocate path objects.

### Future-Agent Application

When implementing Cypher `MATCH`, build endpoint-only expansion first. Add path materialization as a separate mode, not as the default representation.

## Pattern 8: Index Substitution and Late Scan Replacement

### Where Found

- `gitrefrepo/falkordb-src/src/execution_plan/optimizations/utilize_indices.c`
  - Lines 540-563 replace a scan op with a new index scan op.
  - Lines 580-690 reduce conditional traversals and filters into edge index scans, including removal of an all-node scan when an edge index can drive the plan.
- `gitrefrepo/indradb-src/lib/src/rdb/datastore.rs`
  - Lines 79-97 query an indexed property only when the property name is present in `indexed_properties`; otherwise return no indexed result.
- `gitrefrepo/indradb-src/lib/src/rdb/managers.rs`
  - Lines 502-575 implement vertex property value index keys.
  - Lines 594-681 implement edge property value index keys.
- `gitrefrepo/kuzu-src/src/include/storage/table/node_table.h`
  - Lines 44-55 and 80-92 show node insert and index scan helper state.
  - Lines 140-180 expose primary key lookup and index add/drop.
- `gitrefrepo/janusgraph-src/janusgraph-core/src/main/java/org/janusgraph/diskstorage/keycolumnvalue/KeyColumnValueStore.java`
  - Lines 27-40 describe BigTable-like row/column storage.
  - Lines 45-66 expose slice reads.
  - Lines 103-129 expose mutations.
  - Lines 131-160 expose expected-value locking support.
- `gitrefrepo/apache-hugegraph-src`
  - Direct detail was limited in this timebox, but HugeGraph is relevant for schema/index builder and backend-store separation.

### Short Shape

```text
logical:
  NodeScan(label) -> Filter(prop = value) -> Expand

physical after optimization:
  NodePropertyIndexSeek(label, prop, value) -> Expand

edge filter variant:
  ConditionalTraverse(edge_prop = value)
  -> EdgePropertyIndexScan(edge_prop, value) -> endpoint join/filter
```

### Why It Matters for Neo4j-in-Rust

Index selection should be a planner rewrite, not an API special case. FalkorDB's optimizer is useful because it rewrites graph-specific traversal/filter patterns, not just node scans. IndraDB is useful because it makes the property index optional and explicit: an unindexed property query should not silently scan unless the planner chooses to.

### When to Use

Use indexes when:

- selectivity is high enough;
- index covers the needed label/type/property predicate;
- index maintenance cost is acceptable;
- the query can stream index hits into traversal.

### When Not to Use

Avoid index substitution when:

- selectivity is poor and full scan is cheaper;
- index is stale or being rebuilt;
- edge traversal from a small bound set is cheaper than global edge index lookup.

### Rust Translation

Represent access paths explicitly:

```rust
enum AccessPath {
    NodeById(NodeId),
    NodeLabelScan(LabelId),
    NodePropertySeek(PropertyIndexId, EncodedValue),
    EdgePrefixScan { src: NodeId, rel: Option<RelTypeId> },
    EdgePropertySeek(PropertyIndexId, EncodedValue),
}
```

The physical planner should expose why an index was or was not chosen:

```rust
struct AccessPathDecision {
    chosen: AccessPath,
    alternatives: Vec<RejectedAccessPath>,
    estimated_rows: u64,
    estimated_bytes: u64,
}
```

### Risks

- Index maintenance can dominate writes.
- Property-value encoding can make range queries difficult.
- Edge indexes can produce duplicate endpoint work if not joined carefully.
- Plan cache must invalidate on index creation/drop and schema version change.

### Memory, Concurrency, and Testing Implications

Memory:

- Index scans should return compact entity IDs, not hydrated nodes.
- Keep index-hit buffers bounded.

Concurrency:

- Index updates must commit atomically with entity/property updates.
- Online index rebuild needs a visibility model.

Testing:

- Run every indexed query with index enabled and disabled against the same oracle.
- Test stale plan cache after index creation/drop.
- Test hash-collision equality checks for hashed values.

### Future-Agent Application

Add "why this access path" diagnostics early. Without them, index bugs look like query bugs.

## Pattern 9: Snapshot, WAL, Checkpoint, and MVCC Boundaries

### Where Found

- `gitrefrepo/typedb-src/database/transaction.rs`
  - Lines 38-46 define commit intent.
  - Lines 48-58 define `TransactionRead` with `Arc<ReadSnapshot>`, managers, query manager, and profile.
  - Lines 60-98 open a read transaction with schema cache and read snapshot.
  - Lines 113-158 open a write transaction with a reserved write transaction and write snapshot.
- `gitrefrepo/typedb-src/database/database.rs`
  - Lines 63-90 define database fields: path, MVCC storage, ID generators, schema cache, query cache, transaction exclusivity, statistics updater, and checkpointer.
  - Lines 105-130 reserve write/schema transactions.
  - Lines 261-313 create WAL/MVCC/checkpointer.
  - Lines 331-432 load WAL/checkpoint and create a checkpoint when the WAL is beyond checkpoint.
  - Lines 523-548 perform checkpointing.
- `gitrefrepo/typedb-src/durability/durability.rs`
  - Lines 27-51 expose sequenced/unsequenced/read/truncate durability service methods.
- `gitrefrepo/dgraph-src/posting/mvcc.go`
  - Lines 40-53 define rollup batching fields.
  - Lines 91-135 roll up a key by taking a timestamp, waiting for in-flight transactions, reading at timestamp, rolling up, removing cache, and writing KVs.
- `gitrefrepo/dgraph-src/posting/list.go`
  - Lines 850-893 choose conflict keys for schema/no-conflict/upsert/list/index/count cases.
  - Lines 902-930 set mutation layers and fingerprints.
- `gitrefrepo/arcadedb-src/engine/src/main/java/com/arcadedb/database/TransactionContext.java`
  - Lines 52-61 describe WAL format.
  - Lines 62-92 define caches, modified pages, new pages, WAL settings, transaction status, and updated records.
  - Lines 112-127 initialize transaction maps.
  - Lines 130-147 commit in two phases.
  - Lines 163-183 provide read-your-writes cache.
- `gitrefrepo/ongdb-src/community/kernel/src/main/java/org/neo4j/kernel/impl/transaction/log/BatchingTransactionAppender.java`
  - Lines 69-72 describe concurrent transaction append and batched forcing.
  - Lines 115-163 append a batch, force to disk, optionally rotate logs, and publish committed.
  - Lines 191-209 write checkpoints.
- `gitrefrepo/arangodb-src/arangod/StorageEngine/TransactionState.h`
  - Lines 92-170 define transaction state fields including role, options, status, actor, stats, lock timeout, and wait-for-sync.
- `gitrefrepo/arangodb-src/arangod/Transaction/IndexesSnapshot.h`
  - Lines 35-55 hold read lock plus shared index vector snapshot.
- `gitrefrepo/nebula-src`
  - Path-level evidence found `src/kvstore/wal/*`, `src/kvstore/raftex/*`, `src/kvstore/NebulaSnapshotManager.*`, and storage transaction processors. Direct source opening remains a gap.

### Short Shape

```text
ReadTransaction:
  snapshot: Arc<ReadSnapshot>
  schema_version

WriteTransaction:
  write_snapshot
  commit_intent
  wal_sequence

Commit:
  write WAL
  apply storage changes
  publish visibility
  maybe checkpoint
```

### Why It Matters for Neo4j-in-Rust

Durability and visibility must be explicit subsystems. TypeDB is the cleanest Rust-native reference here: read and write transactions hold distinct snapshot types, database owns MVCC storage and checkpointer, and schema write exclusivity is visible in the database object. Dgraph adds a useful LSM/MVCC lesson: old versions must roll up, and conflict keys are a deliberate design.

### When to Use

Use full MVCC/WAL when:

- concurrent readers must not block writers;
- long traversals need stable snapshots;
- crash recovery is a product requirement;
- storage compaction/checkpointing must be online.

### When Not to Use

For an embedded toy graph, a single-writer append-only log plus coarse read lock may be enough. But the format should still leave room for snapshots.

### Rust Translation

```rust
struct ReadGraphTransaction {
    snapshot: Arc<StorageSnapshot>,
    catalog_version: CatalogVersion,
}

struct WriteGraphTransaction {
    base_snapshot: Arc<StorageSnapshot>,
    delta: WriteDelta,
    intent: CommitIntent,
}

struct CommitRecord {
    tx_id: TxId,
    catalog_version: CatalogVersion,
    changed_key_ranges: Vec<KeyRange>,
    wal_lsn: Lsn,
}
```

Separate durability API:

```rust
trait DurabilityLog {
    fn append_commit_record(&self, record: &CommitRecord) -> Result<Lsn, WalError>;
    fn sync_log_to_lsn(&self, lsn: Lsn) -> Result<(), WalError>;
    fn replay_log_from_lsn(&self, lsn: Lsn) -> Result<ReplayIter, WalError>;
}
```

### Risks

- MVCC can retain too many old versions under long-running traversals.
- WAL record format becomes a compatibility surface.
- Checkpoint jobs can contend with compaction and query IO.
- Schema writes may need stricter isolation than data writes.

### Memory, Concurrency, and Testing Implications

Memory:

- Version retention must be bounded by oldest reader and configured limits.
- Write deltas should spill or abort when memory budgets are exceeded.

Concurrency:

- Single schema writer is often simpler and safer than fully concurrent schema mutation.
- Read snapshots should be cheap `Arc` handles.

Testing:

- Crash after WAL append before publish.
- Crash after publish before checkpoint.
- Long reader while writer updates and checkpoint runs.
- Conflict-key tests for concurrent edge/property writes.

### Future-Agent Application

Use TypeDB as the primary Rust reference for transaction architecture. Use Dgraph for rollup/conflict-key ideas. Use ONGDB's appender as a reminder that log force and publish are different events.

## Pattern 10: Catalog, Schema, Token, and Version Models

### Where Found

- `gitrefrepo/surrealdb-src/surrealdb/core/src/catalog/record.rs`
  - Lines 14-38 define `Record` with optional metadata and data value.
  - Lines 52-61 test whether a record is an edge by metadata.
  - Lines 84-97 define `RecordType::Table` and `RecordType::Edge`.
- `gitrefrepo/surrealdb-src/surrealdb/core/src/catalog/table.rs`
  - Lines 148-156 define `TableType::{Any, Normal, Relation(Relation)}`.
  - Lines 158-188 print relation definitions with `IN`, `OUT`, and `ENFORCED`.
  - Lines 212-228 define relation endpoints and enforcement.
  - Lines 230-255 migrate older relation type forms.
- `gitrefrepo/kuzu-src/src/include/catalog/catalog.h`
  - Lines 52-159 expose catalog functions for tables, indexes, schemas, create/drop, and transaction-scoped reads.
- `gitrefrepo/falkordb-src/src/graph/graphcontext_struct.h`
  - Lines 8-17 describe graph-context version signatures for schema mappings that clients may cache.
  - Lines 18-38 store graph, attributes, schema lock, name, string mapping, node/relationship schemas, index count, slowlog, query log, plan cache, telemetry, and write queue.
- `gitrefrepo/age-src/src/backend/commands/graph_commands.c`
  - Lines 36-40 define graph schema name conventions.
  - Lines 76-110 create a graph schema, insert graph metadata, and create default vertex/edge labels.
  - Lines 140-188 generate PostgreSQL schema and sequence statements for label IDs.
- `gitrefrepo/janusgraph-src/janusgraph-core/src/main/java/org/janusgraph/graphdb/database/management/ManagementSystem.java`
  - Lines 135-160 define fields for graph, system log, management logger, transactional config, schema cache, transaction, updated types, triggers, start time, shutdown flag, and open flag.
- `gitrefrepo/helix-db-src/helix-cli/src/config.rs`
  - Lines 160-164 define graph secondary indices in config.
  - Lines 166-185 group vector and graph config into database config.

### Short Shape

```text
Catalog {
  labels: token table
  relationship_types: token table
  property_keys: token table
  tables_or_labels: schema entries
  indexes: index definitions
  version: monotonically increasing
}

PlanCacheKey = query_hash + catalog_version + settings
```

### Why It Matters for Neo4j-in-Rust

Names are user-facing; IDs are storage-facing. The catalog must map between them, version them, and invalidate plan caches. SurrealDB's relation table type is useful because it makes edge-ness a catalog property. FalkorDB's graph-context version is useful because clients cache name-to-ID mappings and need a cheap invalidation signal.

### When to Use

Always use a catalog if the engine has labels/types/properties/indexes.

### When Not to Use

For a pure key-value adjacency library with no query language, token maps can be delegated to the caller.

### Rust Translation

```rust
struct GraphCatalog {
    version: CatalogVersion,
    labels: TokenMap<LabelId>,
    rel_types: TokenMap<RelTypeId>,
    property_keys: TokenMap<PropertyKeyId>,
    indexes: IndexCatalog,
    relation_constraints: Vec<RelationConstraint>,
}
```

Make catalog reads snapshot-scoped:

```rust
fn load_catalog_for_snapshot(snapshot: &ReadSnapshot) -> Arc<GraphCatalog>;
```

### Risks

- Token ID reuse can corrupt old plans or snapshots.
- Online schema changes can break long-running queries.
- Compatibility layers may require multiple names for the same internal object.

### Memory, Concurrency, and Testing Implications

Memory:

- Intern strings once in catalog; store IDs in records.
- Catalog can be `Arc` shared across snapshots.

Concurrency:

- Use single-writer catalog updates unless there is a strong reason not to.
- Readers hold an immutable catalog version.

Testing:

- Plan-cache invalidation on every schema/index mutation.
- Migration tests from old relation schema forms.
- Token stability tests across backup/restore.

### Future-Agent Application

Do not design node records with label names or property names in them. Every future feature should go through token IDs and catalog versions.

## Pattern 11: Embedded Engine and Server Facade Split

### Where Found

- `gitrefrepo/arcadedb-src/engine/src/main/java/com/arcadedb/database/Database.java`
  - Lines 41-49 define isolation and consistency enums.
  - Lines 63-72 expose select/batch builders.
  - Lines 74-86 route commands by language.
  - Lines 91-101 run work in auto transaction with rollback behavior.
  - Lines 103-160 scan types/buckets.
  - Lines 162-170 lookup by index.
- `gitrefrepo/arcadedb-src/engine/src/main/java/com/arcadedb/database/LocalDatabase.java`
  - Lines 129-138 describe local file implementation and edge-list chunk sizes.
  - Lines 154-179 define schema, transaction manager, and statement/execution/Cypher plan caches.
- `gitrefrepo/indradb-src/lib/src/database.rs`
  - Lines 198-218 define `Datastore` and `Database`.
- `gitrefrepo/typedb-src/server/state/transaction_operator.rs`
  - Lines 27-54 define server-side transaction info/operator state.
  - Lines 59-72 run cleanup.
  - Lines 87-105 open transactions and record them in the operator map.
- `gitrefrepo/helix-db-src/helix-cli/src/config.rs`
  - Lines 111-145 define local instance config and memory/disk storage mode.
  - Lines 148-158 define vector config.
  - Lines 160-185 define graph/db config.
- `gitrefrepo/apache-tinkerpop-src`
  - Direct detail was limited, but TinkerPop is relevant as a Gremlin API and traversal facade over multiple graph providers.
- `gitrefrepo/eclipse-rdf4j-src`
  - Direct detail was limited, but RDF4J is relevant as a repository/Sail facade around storage and query evaluation.

### Short Shape

```text
core engine:
  storage
  transactions
  catalog
  planner/executor

embedded facade:
  Rust API
  in-process transactions

server facade:
  connection/session
  transaction registry
  protocol codecs
  cleanup/timeout
```

### Why It Matters for Neo4j-in-Rust

Lower RAM is easier if the core engine can run embedded. Server features like sessions, auth, HTTP, Bolt, metrics, and query logs should wrap the engine, not be required by it. ArcadeDB and TypeDB show two sides of this: local database API and server transaction operator.

### When to Use

Use the split when:

- the engine needs both library and server modes;
- tests should run without a network server;
- compatibility protocols are optional features.

### When Not to Use

If the product is only a hosted distributed database, embedded API may be less important. Still, keep core storage protocol-independent.

### Rust Translation

```rust
struct GraphEngine {
    storage: Arc<dyn StorageEngine>,
    catalog: CatalogManager,
    planner: QueryPlanner,
}

struct GraphServer {
    engine: Arc<GraphEngine>,
    transactions: DashMap<TransactionId, TransactionHandle>,
}
```

### Risks

- Embedded API may bypass server enforcement if auth/limits live only in the server.
- Server transaction cleanup can leak snapshots if handles are not dropped.
- Feature flags can create divergent behavior.

### Memory, Concurrency, and Testing Implications

Memory:

- Server sessions should hold transaction handles, not copied graphs.
- Query result streaming should backpressure clients.

Concurrency:

- Server transaction registry needs timeout cleanup.
- Embedded transactions should use same core transaction types as server transactions.

Testing:

- Same conformance tests through embedded API and server API.
- Leak tests for abandoned server transactions.

### Future-Agent Application

Implement embedded engine first. Add server protocols as adapters, not storage dependencies.

## Pattern 12: Observability and Failure Modes as First-Class Engine Surfaces

### Where Found

- `gitrefrepo/falkordb-src/src/graph/graphcontext_struct.h`
  - Lines 18-38 store slowlog, query log, plan cache, telemetry, `write_in_progress`, and pending write queue in graph context.
- `gitrefrepo/arangodb-src/arangod/Graph/Cursors/EdgeCursor.h`
  - Lines 41-71 expose `httpRequests`, current vertex, current depth, and batch boundaries on traversal cursor.
- `gitrefrepo/memgraph-src/src/query/plan/operator.hpp`
  - Lines 73-102 expose cursor lifecycle methods.
  - Repository path inspection also found query memory tracker and mode-specific exceptions in `src/query`.
- `gitrefrepo/ongdb-src/community/io/src/main/java/org/neo4j/io/pagecache/PageCache.java`
  - Lines 121-131 expose flush/force with optional IO limiter.
  - Lines 163-167 report thread-local events to global page-cache tracer.
- `gitrefrepo/ongdb-src/community/kernel/src/main/java/org/neo4j/kernel/impl/transaction/log/BatchingTransactionAppender.java`
  - Lines 122-123 assert database health before log append.
  - Lines 172-177 panic database health on impossible transaction ID mismatch.
  - Lines 202-205 panic database health on checkpoint failure.
- `gitrefrepo/nebula-src`
  - Path-level evidence found `src/storage/http/StorageHttpStatsHandler.*`, `StorageHttpAdminHandler.*`, and `StorageHttpPropertyHandler.*`.
- `gitrefrepo/arcadedb-src/engine/src/main/java/com/arcadedb/database/LocalDatabase.java`
  - Lines 154-179 include plan caches and index compaction state.

### Short Shape

```text
Every operator records:
  rows_in
  rows_out
  bytes_allocated
  storage_reads
  page_cache_hits/misses
  elapsed

Every storage subsystem records:
  WAL lag
  checkpoint age
  oldest reader
  compaction backlog
  lock wait time
```

### Why It Matters for Neo4j-in-Rust

Graph databases fail in nonlinear ways: a harmless variable-length path turns into path explosion; one high-degree node destroys a query budget; one old reader pins MVCC versions; one plan cache entry survives schema change. Observability is not garnish. It is how the engine stays debuggable.

### When to Use

Use always, starting with simple counters.

### When Not to Use

Do not let observability allocate per row or require global locks in hot loops.

### Rust Translation

```rust
struct OperatorStats {
    rows_in: u64,
    rows_out: u64,
    bytes_allocated: u64,
    storage_reads: u64,
    elapsed_nanos: u64,
}

trait ProfiledOperator {
    fn operator_stats_snapshot(&self) -> OperatorStats;
}
```

Use feature flags for detailed tracing, but keep cheap counters always on.

### Risks

- Metrics can hide allocations if labels are dynamic strings.
- Profiling can perturb concurrency.
- Slowlog without plan snapshots is hard to act on.

### Memory, Concurrency, and Testing Implications

Memory:

- Use numeric IDs for metric labels in hot code.
- Attach string labels at reporting boundary.

Concurrency:

- Use per-thread/operator counters and aggregate later.

Testing:

- Assert memory-budget errors for path explosion.
- Add tests that page-cache/WAL/checkpoint counters move under known operations.
- Include slow-query plan snapshots in integration tests.

### Future-Agent Application

Every new storage or executor pattern should include "what will we measure?" before it is merged.

## Pattern 13: Migration and Compatibility Adapters

### Where Found

- `gitrefrepo/age-src/src/backend/commands/graph_commands.c`
  - Lines 76-110 create graph schemas and default labels inside PostgreSQL.
- `gitrefrepo/age-src/src/backend/parser/cypher_clause.c`
  - Lines 6482-6529 adapt Cypher create semantics into PostgreSQL label relations and IDs.
- `gitrefrepo/age-src/src/backend/executor/cypher_create.c`
  - Lines 401-421 insert edge tuple fields.
- `gitrefrepo/surrealdb-src/surrealdb/core/src/sql/lookup.rs`
  - Lines 9-24 define lookup as a unified way of handling graph edges and record references.
  - Lines 129-148 represent graph lookup versus reference lookup.
- `gitrefrepo/surrealdb-src/surrealdb/core/src/sql/part.rs`
  - Lines 245-261 define recursive/path/shortest path instructions.
  - Lines 263-343 convert those instructions into SQL/expression forms.
- `gitrefrepo/apache-tinkerpop-src`
  - Gremlin compatibility surface; direct file details remain a gap for this first version.
- `gitrefrepo/apache-jena-src`, `gitrefrepo/eclipse-rdf4j-src`, `gitrefrepo/blazegraph-src`
  - SPARQL/RDF compatibility surface; Blazegraph path inspection found generated SPARQL AST nodes under `sparql-grammar/src/main/java/com/bigdata/rdf/sail/sparql/ast/*`, including property path and update/query AST classes.
- `gitrefrepo/tigergraph-ecosys-src`
  - Not source-inspected in this timebox. Treat as future compatibility corpus for GSQL/import/export patterns, not storage internals until verified.

### Short Shape

```text
Compatibility adapter:
  parse external language
  bind external semantics to internal graph IR
  preserve user-visible errors where possible
  execute through core planner/storage
```

### Why It Matters for Neo4j-in-Rust

Users care about Cypher, Gremlin, SPARQL, imports, drivers, and tooling. The engine cares about compact IDs, snapshots, and streaming cursors. Compatibility adapters bridge those worlds. AGE is the cleanest compatibility warning: Cypher-on-Postgres is useful, but its storage decisions are PostgreSQL-shaped. A Rust rewrite should borrow compatibility ideas without inheriting a row-store architecture accidentally.

### When to Use

Use adapters when:

- migrating users from Neo4j, RDF stores, or Gremlin stacks;
- importing/exporting graph data;
- offering language compatibility without matching internals.

### When Not to Use

Do not put compatibility semantics inside the storage format. A storage record should not know whether a query arrived as Cypher, Gremlin, SPARQL, or a Rust API call.

### Rust Translation

Create an internal graph IR:

```rust
enum GraphPattern {
    Node(NodePattern),
    Edge(EdgePattern),
    Path(PathPattern),
}

trait LanguageAdapter {
    fn parse_query_text(&self, text: &str) -> Result<ExternalAst, ParseError>;
    fn lower_ast_to_graph_ir(&self, ast: ExternalAst, catalog: &GraphCatalog) -> Result<GraphIr, BindError>;
}
```

### Risks

- Compatibility can force odd semantics, especially around nulls, path uniqueness, multi-edges, and typed values.
- Error-message compatibility can become a maintenance burden.
- SPARQL property paths, Gremlin traversals, and Cypher variable-length paths are similar but not identical.

### Memory, Concurrency, and Testing Implications

Memory:

- Adapters should lower into compact IR, not retain full parser trees during execution.

Concurrency:

- Language adapters should be stateless or share immutable grammar tables.

Testing:

- Maintain conformance suites per language.
- Differential-test small queries against reference systems where feasible.

### Future-Agent Application

Treat compatibility as a product layer. Keep the Rust core graph engine small enough that multiple adapters can target it.

## Repository-Specific Engineering Notes

### `indradb-src`

IndraDB is the most immediately transferable Rust reference for a small embeddable property graph. Its strengths are:

- simple `Vertex` and `Edge` models (`models/vertices.rs`, `models/edges.rs`);
- object-safe transaction API (`lib/src/database.rs`);
- RocksDB column-family split (`lib/src/rdb/datastore.rs`);
- forward and reversed edge ranges (`lib/src/rdb/managers.rs`);
- optional property-value indexes.

Taste judgment: use it for API shape and keyspace intuition, but do not stop at its recursive query execution model if building a Cypher runtime. A production Neo4j-like rewrite needs an explicit planner/executor.

### `typedb-src`

TypeDB is the strongest Rust reference for:

- byte-level graph key encoding;
- read/write transaction separation;
- MVCC storage ownership;
- WAL/checkpoint lifecycle;
- schema transaction exclusivity.

Taste judgment: use TypeDB for storage correctness and transaction boundaries. Its domain model is not Neo4j's property graph model, so translate storage patterns, not semantics.

### `kuzu-src`

Kuzu is the best source here for low-RAM columnar graph execution:

- node and relation tables;
- CSR offset/length columns;
- column-level checkpoint hooks;
- factorized result tables.

Taste judgment: use Kuzu to challenge every linked-list adjacency design. If the workload is analytical or relation-type constrained, CSR segments can beat record chains.

### `redisgraph-src` and `falkordb-src`

These repos show matrix-backed topology and physical traversal operators. FalkorDB's graph context also bundles schema versioning, logs, telemetry, plan cache, and write queue.

Taste judgment: borrow executor laziness and observability. Be cautious about adopting a fully in-memory matrix model if the central goal is lower RAM.

### `ongdb-src`

ONGDB is the closest Neo4j-family baseline:

- fixed node and relationship records;
- page-cache boundary;
- transaction log append/force/publish separation;
- relationship traversal cursor;
- record storage engine as a large composition root.

Taste judgment: learn from the record/page-cache architecture, but avoid recreating the Java object graph and storage-engine complexity in Rust all at once.

### `arangodb-src`

ArangoDB contributes:

- rearmable graph edge cursors;
- traversal nodes in typed AQL AST;
- RocksDB edge-index key construction;
- transaction state and index snapshots.

Taste judgment: its traversal cursor API is very relevant to a Rust executor. The multi-model database architecture is larger than needed for a focused graph rewrite.

### `arcadedb-src` and `orientdb-src`

ArcadeDB and OrientDB show document/graph hybrids:

- vertices/edges as records/documents with endpoint RIDs;
- edge head chunks or `in_`/`out_` fields;
- RidBag/tree-backed adjacency for high-degree collections;
- SQL-ish command and execution planners;
- transaction contexts with page/WAL state.

Taste judgment: useful for migration and mixed document/graph APIs. For lower RAM, do not copy document-level property maps into the hot topology path.

### `dgraph-src`

Dgraph is useful for:

- LSM-style posting lists;
- MVCC rollup;
- conflict-key selection;
- Raft snapshot/WAL ideas.

Taste judgment: use Dgraph when designing distributed or LSM-backed property/index storage. Its GraphQL/RDF-like model is not the same as Neo4j, but its version retention and rollup patterns matter. See `supplemental-gap-closure-batch-05.md` for the direct-source pass on posting lists, delta-only mutation paths, segmented durable list keys, compressed UID operators, schema/index rebuild contracts, query expansion limits, and Raft/WAL backpressure.

### `cayley-src`

Cayley is useful for:

- tiny `QuadStore` contract;
- iterator/shape optimizer;
- path morphism stack;
- SQL-backed quad store with caches.

Taste judgment: good reference for RDF-style query composition and store abstraction. Not enough by itself for a Cypher property-graph executor.

### `janusgraph-src`

JanusGraph is useful for:

- key-column-value backend abstraction;
- slice queries over rows;
- expected-value locks;
- management/schema transaction system;
- ID assignment and partitioning.

Taste judgment: use it for pluggable backend and distributed ID/schema lessons. Its dependency stack is intentionally broad and not a good low-RAM embedded baseline.

### `age-src`

AGE is useful for:

- Cypher compatibility on top of PostgreSQL;
- graph catalog/label creation;
- parser/executor split for Cypher create;
- migration and compatibility design.

Taste judgment: good compatibility reference, not a storage model for low-RAM Rust.

### `surrealdb-src`

SurrealDB contributes:

- relation table catalog type;
- edge metadata on records;
- graph lookup/reference unification;
- recursive/path/shortest path instruction representation.

Taste judgment: useful for flexible graph/document semantics and catalog modeling. Validate performance before copying any dynamic `Value`-heavy path into a low-RAM engine.

### `helix-db-src`

This checkout appeared to contain CLI/SDK/config rather than deep engine internals. The useful inspected piece is that local config explicitly distinguishes memory versus disk storage and graph secondary indexes.

Taste judgment: good reminder to make storage mode and index config explicit in deployment config.

### `nebula-src`

Path-level inspection found graph/storage/meta daemons, storage query processors, WAL, Raft snapshot, index nodes, transaction processors, and HTTP stats handlers. Direct source reads remain a gap.

Taste judgment: likely important for distributed graph storage, WAL, and snapshot designs, but do not rely on it until future agents open the exact files.

### `apache-hugegraph-src`

Only partially inspected in this pass. HugeGraph should be used by future agents for:

- schema/index builders;
- backend-store abstraction;
- Gremlin/TinkerPop integration;
- OLTP server architecture.

Taste judgment: likely strong for API/schema/backend separation, but direct file evidence should be added.

### `apache-tinkerpop-src`

Only lightly inspected in this pass. TinkerPop should be used as:

- Gremlin traversal semantics reference;
- provider API compatibility reference;
- test-suite/conformance reference.

Taste judgment: do not use TinkerPop as storage inspiration; use it as an adapter and traversal-language contract.

### `apache-jena-src`, `eclipse-rdf4j-src`, and `blazegraph-src`

Jena/RDF4J/Blazegraph are RDF/SPARQL references. Blazegraph path inspection found extensive generated SPARQL AST files and transaction tests. Direct source reads for Jena/RDF4J remain thin in this first version.

Taste judgment: use these for SPARQL/property-path compatibility, RDF indexing permutations, and repository/Sail/API boundary design. Be careful translating RDF triples/quads into property-graph storage.

### `nornicdb-src`, `terminusdb-src`, and `tigergraph-ecosys-src`

These were not meaningfully source-inspected in the timebox.

Taste judgment:

- `terminusdb-src` is likely relevant for immutable/document/RDF-like graph and datalog-ish query ideas.
- `tigergraph-ecosys-src` is likely more ecosystem/examples/connectors than core engine, useful for GSQL/import/export compatibility.
- `nornicdb-src` needs initial orientation before any claims.

## Cross-Cutting Failure Modes to Design Against

### Path Explosion

Evidence: RedisGraph/FalkorDB variable-length traversal optimization; Memgraph path materialization; Surreal recursive/path instruction forms.

Rust design:

- endpoint-only by default;
- explicit path-output mode;
- per-operator memory budget;
- max-depth and max-path checks;
- query profile exposes path materialization count.

### High-Degree Nodes

Evidence: ONGDB dense flag and relationship chains; ArcadeDB edge chunks; OrientDB RidBag; Kuzu CSR; prefix adjacency ranges.

Rust design:

- detect high-degree nodes;
- switch from inline/small adjacency to chunked or CSR storage;
- test threshold transitions;
- expose degree histograms.

### Index Staleness

Evidence: IndraDB indexed property set; Kuzu index add/drop/checkpoint; FalkorDB plan rewrite; JanusGraph management/schema.

Rust design:

- index catalog has state: building, online, dropping, failed;
- queries only use online indexes;
- plan cache key includes catalog version;
- rebuild uses snapshot plus catch-up delta.

### Long Readers Pinning Versions

Evidence: TypeDB `Arc<ReadSnapshot>`; Dgraph rollup and ongoing transaction wait; Arango index snapshots.

Rust design:

- track oldest reader;
- refuse or warn on readers exceeding retention budget;
- let checkpoint/compaction report pinned bytes.

### Partial Edge Update

Evidence: IndraDB writes both edge ranges; ONGDB relationship records update both endpoint chains; OrientDB/Arcade update vertex edge pointers.

Rust design:

- edge insert/delete is one write transaction;
- crash tests after every sub-write;
- invariant checker verifies forward/reverse/index consistency.

### Schema/Token Drift

Evidence: FalkorDB graph-context version for cached mappings; Surreal table relation migration; AGE graph/label catalog.

Rust design:

- catalog version monotonic;
- token IDs never reused inside a database lifetime;
- clients receive schema version and refresh mappings on mismatch.

## Suggested Rust Module Map

```text
crates/
  graph_ids/
    NodeId, EdgeId, LabelId, RelTypeId, PropertyKeyId
  graph_keycodecs/
    prefix encoders, value encoders, format versions
  graph_storage/
    StorageEngine, ReadSnapshot, WriteTransaction
  graph_records/
    NodeCoreRecord, RelationshipCoreRecord, PropertyRecord
  graph_catalog/
    token maps, schema, indexes, catalog versions
  graph_indexes/
    property indexes, label indexes, edge property indexes
  graph_traversal/
    ExpandCursor, frontier, path backpointers
  graph_query_ir/
    AST-independent graph IR
  graph_planner/
    logical plan, physical plan, access-path decisions
  graph_executor/
    cursors, operators, row batches, operator stats
  graph_durability/
    WAL, checkpoint, replay
  graph_server/
    optional protocol/session facade
```

Four-word-ish names that fit the project style:

- `encode_outbound_edge_key`
- `scan_outbound_edges_prefix`
- `load_node_core_record`
- `hydrate_property_values_lazy`
- `choose_index_access_path`
- `publish_catalog_version_after`
- `checkpoint_storage_snapshot_async`
- `materialize_path_values_only`

## Testing Strategy to Lift From the Corpus

1. Storage codec tests

   - Inspired by TypeDB byte keys and IndraDB managers.
   - Property-test every key codec, record codec, and prefix ordering rule.

2. Store-backend conformance

   - Inspired by IndraDB/Cayley store interfaces.
   - Run the same graph mutation/query tests against memory, RocksDB-like, and future mmap/page stores.

3. Query plan tests

   - Inspired by FalkorDB index rewrites and Memgraph operators.
   - Assert plan shapes for label scans, property seeks, edge expands, and path expands.

4. Traversal memory tests

   - Inspired by RedisGraph path-collection optimization and Memgraph path values.
   - Assert endpoint-only traversal does not allocate path objects.

5. Crash recovery tests

   - Inspired by TypeDB/ONGDB/ArcadeDB/Dgraph durability boundaries.
   - Inject crashes after WAL append, after forward edge write, after reverse edge write, after index write, and before checkpoint publish.

6. Catalog migration tests

   - Inspired by Surreal relation migration and FalkorDB schema versioning.
   - Load old catalog versions and verify plan cache invalidation.

7. High-degree node tests

   - Inspired by ONGDB dense nodes, OrientDB RidBag, ArcadeDB edge chunks, and Kuzu CSR.
   - Verify threshold transitions and expansion correctness across storage representations.

## Coverage and Gaps

### Graph Tools Attempted

Both requested evidence-reader skills were used.

- `codebase-memory-evidence-reader`
  - Wrapper completed successfully.
  - Output directory: `/tmp/codex-code-intel/codebase-memory/knight-bus-graph-walker-20260706-230528`
  - Important limitation: smoke output verified that indexed query outputs did not mention `gitrefrepo/`.
- `codegraphcontext-evidence-reader`
  - Wrapper completed successfully.
  - Output directory: `/tmp/codex-code-intel/codegraphcontext/knight-bus-graph-walker-20260706-230528`
  - Important limitation: smoke output verified that indexed query outputs did not mention `gitrefrepo/`.

Because the smoke index did not include `gitrefrepo`, direct source reads and `rg`/path navigation were the primary evidence for reference repositories.

### Direct Source Reads With Strong Evidence

- `age-src`
  - `src/backend/commands/graph_commands.c`
  - `src/backend/parser/cypher_clause.c`
  - `src/backend/executor/cypher_create.c`
- `arangodb-src`
  - `arangod/Graph/Cursors/EdgeCursor.h`
  - `arangod/Graph/Cursors/SingleServerEdgeCursor.h`
  - `arangod/Graph/BaseOptions.h`
  - `arangod/Aql/TypedAstNodes.h`
  - `arangod/RocksDBEngine/RocksDBKey.cpp`
  - `arangod/StorageEngine/StorageEngine.h`
  - `arangod/StorageEngine/TransactionState.h`
  - `arangod/Transaction/IndexesSnapshot.h`
- `arcadedb-src`
  - `engine/src/main/java/com/arcadedb/graph/MutableVertex.java`
  - `engine/src/main/java/com/arcadedb/graph/MutableEdge.java`
  - `engine/src/main/java/com/arcadedb/database/Database.java`
  - `engine/src/main/java/com/arcadedb/database/LocalDatabase.java`
  - `engine/src/main/java/com/arcadedb/database/TransactionContext.java`
  - `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`
- `cayley-src`
  - `graph/quadstore.go`
  - `graph/iterator/iterator.go`
  - `query/path/path.go`
  - `query/path/morphism_apply_functions.go`
  - `graph/sql/quadstore.go`
- `dgraph-src`
  - `posting/mvcc.go`
  - `posting/list.go`
  - `conn/node.go`
  - `worker/draft.go`
- `falkordb-src`
  - `src/graph/graph.h`
  - `src/graph/graphcontext_struct.h`
  - `src/execution_plan/optimizations/utilize_indices.c`
- `helix-db-src`
  - `helix-cli/src/config.rs`
- `indradb-src`
  - `lib/src/models/vertices.rs`
  - `lib/src/models/edges.rs`
  - `lib/src/database.rs`
  - `lib/src/rdb/datastore.rs`
  - `lib/src/rdb/managers.rs`
  - `lib/src/memory/datastore.rs`
- `janusgraph-src`
  - `janusgraph-core/src/main/java/org/janusgraph/diskstorage/keycolumnvalue/KeyColumnValueStore.java`
  - `janusgraph-core/src/main/java/org/janusgraph/graphdb/database/management/ManagementSystem.java`
  - `janusgraph-core/src/main/java/org/janusgraph/graphdb/database/idassigner/VertexIDAssigner.java`
- `kuzu-src`
  - `src/include/storage/table/node_table.h`
  - `src/include/storage/table/rel_table.h`
  - `src/include/storage/table/column.h`
  - `src/include/catalog/catalog.h`
  - `src/include/processor/result/factorized_table.h`
- `memgraph-src`
  - `src/query/vertex_accessor.hpp`
  - `src/query/path.hpp`
  - `src/query/plan/operator.hpp`
- `ongdb-src`
  - `community/kernel/src/main/java/org/neo4j/kernel/impl/store/NodeStore.java`
  - `community/kernel/src/main/java/org/neo4j/kernel/impl/store/RelationshipStore.java`
  - `community/kernel/src/main/java/org/neo4j/kernel/impl/store/record/NodeRecord.java`
  - `community/kernel/src/main/java/org/neo4j/kernel/impl/store/record/RelationshipRecord.java`
  - `community/kernel/src/main/java/org/neo4j/kernel/impl/storageengine/impl/recordstorage/RecordStorageEngine.java`
  - `community/io/src/main/java/org/neo4j/io/pagecache/PageCache.java`
  - `community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/RelationshipTraversalCursor.java`
  - `community/kernel/src/main/java/org/neo4j/kernel/impl/transaction/log/BatchingTransactionAppender.java`
- `redisgraph-src`
  - `src/graph/graph.h`
  - `src/graph/entities/node.h`
  - `src/graph/entities/edge.h`
  - `src/execution_plan/ops/op_cond_var_len_traverse.c`
- `surrealdb-src`
  - `surrealdb/core/src/catalog/record.rs`
  - `surrealdb/core/src/catalog/table.rs`
  - `surrealdb/core/src/sql/lookup.rs`
  - `surrealdb/core/src/sql/part.rs`
- `typedb-src`
  - `encoding/graph/thing/vertex_object.rs`
  - `encoding/graph/thing/edge.rs`
  - `database/transaction.rs`
  - `database/database.rs`
  - `durability/durability.rs`
  - `storage/record.rs`
  - `server/state/transaction_operator.rs`
  - `ir/pattern/mod.rs`

### Grep or Path-Level Evidence Only

- `apache-hugegraph-src`
  - Needs direct source reads for backend store, schema/index, traversal, and Gremlin server layers.
- `apache-jena-src`
  - Needs direct source reads for graph/dataset storage, transaction, SPARQL algebra, and TDB/TDB2 indexes.
- `apache-tinkerpop-src`
  - Needs direct source reads for traversal strategies, graph provider API, and test/conformance modules.
- `blazegraph-src`
  - Path inspection found SPARQL AST files and transaction tests. Needs direct reads for journal, BTree, triple-store indexes, and query engine.
- `eclipse-rdf4j-src`
  - Needs direct source reads for Sail, MemoryStore, NativeStore, query algebra/evaluation, and transaction isolation.
- `nebula-src`
  - Path inspection found WAL, Raft, snapshot, storage query, index nodes, transaction processors, and HTTP stats handlers. Needs direct reads before strong design claims.
- `orientdb-src`
  - Grep evidence captured vertex/edge document, RidBag, SBTree, transaction, and SQL executor files. Needs fuller direct reads for exact edge update and RidBag persistence semantics.

### Not Meaningfully Inspected Yet

- `nornicdb-src`
- `terminusdb-src`
- `tigergraph-ecosys-src`

### Highest-Value Next Reads

1. `apache-jena-src` TDB/TDB2 node table, triple/quad indexes, transaction coordinator.
2. `eclipse-rdf4j-src` NativeStore and Sail connection transaction model.
3. `nebula-src/src/common/utils/IndexKeyUtils.*`, `src/storage/query/GetNeighborsProcessor.*`, `src/kvstore/wal/FileBasedWal.*`, `src/kvstore/raftex/SnapshotManager.*`.
4. `orientdb-src/core/src/main/java/com/orientechnologies/orient/core/record/impl/OVertexDocument.java` and `OSBTreeRidBag.java` with full line-by-line reads.
5. `apache-tinkerpop-src` traversal strategies and provider interfaces.
6. `terminusdb-src` storage/query modules for immutable graph/document ideas.
7. `tigergraph-ecosys-src` only after confirming whether it contains engine code or mostly ecosystem examples.

## Bottom Line

The strongest low-RAM Rust direction from this corpus is a hybrid:

- ONGDB-style fixed core node/relationship records for transactional topology;
- IndraDB/TypeDB-style prefix keyspaces for adjacency, reverse adjacency, and property indexes;
- Kuzu-style CSR/columnar checkpoint segments for stable relation scans;
- Memgraph/Arango/Cayley-style streaming traversal cursors;
- FalkorDB/RedisGraph-style path laziness and physical traversal optimization;
- TypeDB/Dgraph/ONGDB-style explicit WAL, snapshot, and checkpoint boundaries;
- Surreal/Falkor/AGE-style catalog versioning and compatibility adapters.

Do not make Rust objects mirror user-visible graph objects. Make compact storage facts, then let query APIs assemble user-facing nodes, edges, paths, and records only when the query actually asks for them.

### Pattern: Kuzu Chunked Graph Scans For Algorithm Memory Control

Source:

- `kuzudb__kuzu/src/include/graph/graph.h:1-280`
- `kuzudb__kuzu/src/include/graph/on_disk_graph.h:1-180`
- `kuzudb__kuzu/src/graph/on_disk_graph.cpp:1-430`
- `kuzudb__kuzu/src/include/main/client_config.h:1-60`
- `kuzudb__kuzu/src/main/settings.cpp:45-80`
- `kuzudb__kuzu/src/include/function/gds/gds_frontier.h:1-320`
- `kuzudb__kuzu/src/function/gds/gds_task.cpp:1-180`
- `kuzudb__kuzu/extension/algo/src/function/page_rank.cpp:1-380`
- `kuzudb__kuzu/extension/algo/src/function/strongly_connected_components.cpp:1-340`
- `kuzudb__kuzu/extension/algo/src/function/weakly_connected_components.cpp:1-130`
- `kuzudb__kuzu/extension/algo/src/function/k_core_decomposition.cpp:1-260`

Graph-tool evidence:

- codebase-memory:
  `/tmp/codex-code-intel/codebase-memory/kuzudb__kuzu-20260707-070427`
  indexed 50,232 nodes and 158,544 edges. Schema evidence included 14,812
  methods, 8,106 functions, 4,987 classes, 3,536 files, 42,525 `CALLS` edges,
  and 836 `TESTS` edges. Symbol search surfaced `Graph`, `OnDiskGraph`,
  `OnDiskGraphNbrScanState`, `OnDiskGraphVertexScanState`, `BFSGraphManager`,
  `BaseBFSGraph`, `SemiMask`, `DenseFrontier`, and planner semi-mask classes.
- CodeGraphContext:
  `/tmp/codex-code-intel/codegraphcontext/kuzudb__kuzu-20260707-070427`
  was attempted, but after interrupting the long writer the readable DB
  reported 1 repository and 0 files/functions/classes. It is recorded as an
  attempted non-evidentiary pass for this repo; all claims below are confirmed
  by direct source reads and codebase-memory.

Observed design:

- `Graph` exposes `prepareRelScan`, `scanFwd`, `scanBwd`, `prepareVertexScan`,
  and `scanVertices`. The API returns iterators whose values are chunks, not
  individual heap-allocated edge objects.
- `NbrScanState::Chunk` holds spans of neighbor node IDs, a mutable selection
  vector, and spans of property vectors. Algorithms can iterate selected rows
  with `forEach` or `forEachBreakWhenFalse`.
- `Graph` instances are explicitly not expected to be thread-safe. Parallel GDS
  code must make a lightweight `copy()` and give each worker its own graph
  state, avoiding shared mutable scan cursors.
- `OnDiskGraphNbrScanState` constructs value vectors and relation table scan
  states once during `prepareRelScan`; `scanFwd` and `scanBwd` set a single
  source node value, reset selection state, and iterate relation table chunks.
- `OnDiskGraphNbrScanState::InnerIterator::next` restores a saved selection
  vector, scans the relation table, applies optional edge predicates, applies
  an optional neighbor node mask, and loops until it has at least one selected
  value.
- `OnDiskGraphVertexScanState::next` scans node tables up to
  `DEFAULT_VECTOR_CAPACITY` at a time and respects node-group boundaries,
  committed/uncommitted scan sources, and property vector resets.
- Client config exposes `sparseFrontierThreshold`, `enableSemiMask`, and
  `recursivePatternSemantic`. GDS extensions use dense frontier pairs for
  PageRank, strongly connected components, weakly connected components, and
  k-core style algorithms.

Rust rewrite transfer:

- Do not model algorithm traversal as `Vec<Edge>` or `Vec<Node>` by default.
  Use a `GraphScanState` that yields borrowed chunks:

```rust
struct NeighborChunk<'a> {
    node_ids: &'a [NodeId],
    selected: &'a SelectionVector,
    property_columns: &'a [ColumnView<'a>],
}
```

- Keep scan state separate from graph metadata. A graph handle can be shared,
  but each worker should own its own scan cursors, vectors, masks, and frontier
  state.
- Make selection vectors the first-class pruning mechanism. Predicate filters,
  neighbor masks, and frontier restrictions should narrow a chunk before
  algorithm code touches it.
- Expose frontier representation policy as a measured setting, not a hidden
  constant. Sparse frontiers work when the active set is small; dense frontiers
  work when active nodes approach table scale.

Executable-spec candidates:

```text
REQ-GDS-SCAN-001
WHEN a graph algorithm scans neighbors for one source node
THEN the graph layer SHALL yield neighbor chunks backed by stable column/vector
storage
AND SHALL NOT require materializing all neighbor node IDs before computation.

REQ-GDS-SCAN-002
WHEN a predicate or semi-mask applies to a neighbor scan
THEN the scan state SHALL update a selection vector for the current chunk
AND SHALL expose only selected rows to the algorithm callback.

REQ-GDS-SCAN-003
WHEN GDS execution runs in parallel
THEN each worker SHALL own independent scan state
AND shared graph metadata SHALL remain immutable or explicitly synchronized.
```

Why it matters for the Neo4j rewrite:

This pattern is one of the clearest answers to the "5x lower RAM" question.
Graph algorithms should run over relation-table chunks, selected row masks, and
frontier-local state. They should not force the OLTP store to project a giant
in-memory object graph before useful work can begin.

### Pattern: Kuzu VM-Backed Buffer Manager Plus Checkpointed Free Space

Source:

- `kuzudb__kuzu/src/include/storage/buffer_manager/buffer_manager.h:1-430`
- `kuzudb__kuzu/src/include/storage/free_space_manager.h:1-100`
- `kuzudb__kuzu/src/storage/free_space_manager.cpp:1-260`
- `kuzudb__kuzu/src/include/storage/page_range.h:1-20`
- `kuzudb__kuzu/src/main/db_config.cpp:1-80`

Graph-tool evidence:

- codebase-memory:
  `/tmp/codex-code-intel/codebase-memory/kuzudb__kuzu-20260707-070427`
  found `BufferManager`, `FreeSpaceManager`, `PageRange`,
  `MemoryManager`, `EvictionQueue`, and storage test classes. Its graph schema
  reported 42,525 `CALLS` edges and 7,409 `WRITES` edges, which made this
  storage subsystem stand out as a high-value direct-read target.
- CodeGraphContext was attempted for the same repo but produced an empty
  readable DB after interruption; it is not used as supporting evidence here.

Observed design:

- `BufferManager` is documented as the central manager of database memory. It
  pins/unpins pages for storage structures such as columns, lists, and hash
  indexes, provides optimistic reads of unlocked or marked pages, and also
  backs `MemoryManager` allocations through temp in-memory file handles.
- The unit split is explicit: page is the storage/file unit; frame is the
  virtual memory unit; each file handle owns page state; the buffer manager
  manages physical memory pressure and frame eviction.
- The buffer manager relies on virtual memory regions mapped through `mmap`.
  Disk pages map into a large virtual region, while temp in-memory file handles
  get regions sized by the buffer pool limit. Actual physical memory is bounded
  by pin/unpin, eviction, and `MADV_DONTNEED`-style release.
- Page states are `EVICTED`, `LOCKED`, `UNLOCKED`, and `MARKED`, with versioned
  transitions that make stale optimistic reads detectable.
- `EvictionQueue` is a circular buffer of atomic eviction candidates. Eviction
  candidates have second-chance behavior: recently optimistically-read pages
  move from `UNLOCKED` back to `MARKED` instead of being immediately evicted.
- `FreeSpaceManager` stores free page ranges in size-class-like levels using
  the largest power of two less than or equal to the range size. `popFreePages`
  searches from the requested level upward and splits larger ranges.
- Freed pages are first collected as `uncheckpointedFreePageRanges`; they are
  not reusable until `finalizeCheckpoint`, which evicts frames, merges adjacent
  ranges, truncates tail ranges when possible, and then promotes safe free
  space.

Rust rewrite transfer:

- Use explicit page/frame state machines rather than ad hoc cache maps. In
  Rust, page state can be an atomic tagged/versioned word guarded by typed
  pin/unpin APIs.
- Make free-space lifecycle checkpoint-aware. A page deleted by a transaction
  should not be reusable until the checkpoint/recovery boundary makes reuse
  crash-safe.
- Represent free space as coalesced ranges, not individual pages. Split ranges
  for allocation and merge them during checkpoint finalization.
- Make the memory budget global and visible: page cache, temp operator buffers,
  list/vector overflow buffers, and spill buffers should all report against the
  same memory manager.

Executable-spec candidates:

```text
REQ-BUFFER-001
WHEN storage code pins a page
THEN the buffer manager SHALL transition that page into an exclusive mutable
state
AND SHALL account the page against the configured memory budget.

REQ-BUFFER-002
WHEN a page is freed by a transaction
THEN the free-space manager SHALL retain it as uncheckpointed free space
AND SHALL NOT return it for reuse until checkpoint finalization.

REQ-BUFFER-003
WHEN checkpoint finalization runs
THEN adjacent free page ranges SHALL be merged
AND tail free ranges SHALL truncate the file when they reach the current file
end.

REQ-BUFFER-004
WHEN physical memory must be reclaimed
THEN evictable marked pages SHALL be reclaimed before pinned pages
AND recently optimistically-read pages SHALL receive a second chance.
```

Why it matters for the Neo4j rewrite:

The rewrite should not merely store graph data in compact structs; it needs a
budgeted runtime that prevents query operators, projections, and temp buffers
from bypassing the page cache. Kuzu's design makes storage pages, temp buffers,
free ranges, checkpoint timing, and eviction all part of one memory story.

### Pattern: Kuzu Factorized Plans, List Slices, And Semi-Mask Pruning

Source:

- `kuzudb__kuzu/src/optimizer/factorization_rewriter.cpp:1-220`
- `kuzudb__kuzu/src/optimizer/remove_factorization_rewriter.cpp:1-80`
- `kuzudb__kuzu/src/include/expression_evaluator/list_slice_info.h:1-120`
- `kuzudb__kuzu/src/processor/operator/semi_masker.cpp:1-220`
- `kuzudb__kuzu/src/planner/plan/append_join.cpp`
- `kuzudb__kuzu/src/planner/plan/append_extend.cpp`
- `kuzudb__kuzu/src/optimizer/acc_hash_join_optimizer.cpp`

Graph-tool evidence:

- codebase-memory:
  `/tmp/codex-code-intel/codebase-memory/kuzudb__kuzu-20260707-070427`
  surfaced `FactorizationRewriter`, `RemoveFactorizationRewriter`,
  `LogicalFlatten`, `LogicalSemiMasker`, `BoundJoinHintNode`,
  `LogicalScanNodeTable`, `LogicalIndexScanNodeCollector`, and list/vector
  evaluator classes.
- CodeGraphContext was attempted but did not produce positive symbol evidence
  for this repo.

Observed design:

- `FactorizationRewriter` runs bottom-up over the logical plan. It appends
  `LogicalFlatten` operators only where a downstream operator demands flat
  groups, then recomputes the factorized schema.
- Projection treats random functions specially by flattening all input groups,
  because random evaluation must be tuple-at-a-time. Non-random expressions use
  `FlattenAllButOne`, preserving factorization where possible.
- Hash joins, intersects, aggregates, order-by, limits, distinct, unwind,
  filters, updates, copy, inserts, deletes, and merge each ask for the precise
  group positions that must be flattened.
- `RemoveFactorizationRewriter` can remove flatten operators and recompute a
  flat schema, then asserts no flatten operators remain. This gives tests and
  agents a verification hook for factorization rewrites.
- `ListSliceInfo` documents a critical vectorization edge case: list data
  vectors can exceed `DEFAULT_VECTOR_CAPACITY`, so lambda evaluators process
  list data in slices and operate on individual data-vector entries instead of
  whole lists.
- `SemiMaskerSharedState` creates per-thread local masks and merges them into a
  shared global roaring bitmap using fast union. The code switches between
  32-bit and 64-bit roaring representations based on max offset.
- Semi-mask operators collect node IDs from simple node values, path relation
  lists, source/destination pairs, and multi-table node IDs. Planner settings
  expose `enableSemiMask` and recursive/frontier thresholds.

Rust rewrite transfer:

- Do not flatten graph-path/query results unless the next operator demands it.
  Carry factorized groups through the logical and physical plan, then insert
  explicit `Flatten` operators as costed boundaries.
- Give every operator a method like `required_flatten_groups(&Schema) ->
  GroupSet`. That makes materialization visible and testable.
- Treat list/path values as potentially larger than one vector batch. Vectorized
  Rust evaluators should include `ListSliceInfo`-style iterators that map each
  slice row back to the original list entry.
- Use local masks per worker and merge them at pipeline barriers. This avoids
  contention while preserving a compact global representation for pruning
  later scans.

Executable-spec candidates:

```text
REQ-FACTOR-001
WHEN planning a query that carries path/list/factorized groups
THEN the optimizer SHALL insert flatten operators only for groups required by
the consuming operator.

REQ-FACTOR-002
WHEN a list value contains more elements than the vector capacity
THEN lambda/list evaluators SHALL process it in slices
AND SHALL preserve the mapping from each slice row to its original list entry.

REQ-SEMI-MASK-001
WHEN a parallel pipeline builds node masks
THEN each worker SHALL build a local mask
AND a barrier SHALL merge local masks into one shared mask before consumers use
it for scan pruning.
```

Why it matters for the Neo4j rewrite:

Graph queries become RAM-heavy when every path, list, or joined row is flattened
too early. Kuzu's factorization and semi-mask machinery shows a practical
alternative: preserve grouped/factorized shape until an operator proves it
needs flat tuples, and push compact node masks into later scans. This is a
direct design lever for lower-memory Cypher execution.

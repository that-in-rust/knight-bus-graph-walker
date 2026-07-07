# Supplemental Gap Closure Batch 04: Adjacent Graph Stores And Pushdown Plans

Date: 2026-07-07

This supplement closes the next high-value `neo4j_gds_compat` gap batch in the
Desktop-wide graph database rewrite corpus. It focuses on adjacent graph systems
that are not Neo4j/GDS itself but teach useful implementation taste for a
Neo4j-style Rust rewrite with lower practical RAM:

- Apache HugeGraph: separated graph tables and store-local query pushdown.
- Blazegraph: access-path selection across multiple statement index orders.
- Eclipse RDF4J: storage/inference API boundaries and closeable statement
  iterators.
- IndraDB: small Rust graph API, object-safe transaction trait, dual edge ranges,
  property-value indexes, and backend equivalence fuzzing.
- NebulaGraph: expression pushdown visitors, partitioned storage plans, memory
  guards, and index-only vs base-data scans.
- SurrealDB: Rust multi-model datastore, typed KV graph keys, transaction
  cleanup, and graph lookup range planning.

The intent is not to claim that these systems are directly better than Neo4j.
The intent is to harvest transferable implementation patterns for agents that
will later design and implement Knight Bus / Neo4j-compatible Rust storage,
query, and OLAP layers.

## Evidence Tooling

CodeGraphContext was used as a focused navigation lens for the small Rust graph
repo `gitrefrepo/indradb-src`.

```text
run directory: /tmp/codex-code-intel/codegraphcontext/indradb-batch04
indexed repo:  /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/indradb-src
stats:         1 repository, 70 files, 529 functions, 6 traits, 74 structs, 19 enums, 109 modules
find results:  Transaction trait at lib/src/database.rs:24
               query function at lib/src/database.rs:380
               RocksdbTransaction struct at lib/src/rdb/datastore.rs:23
```

The CGC results were used only to locate structure. Claims below are backed by
direct source reads.

## Pattern 01: Split Graph Tables Before Optimizing Algorithms

Where found:

- Repository: `gitrefrepo/apache-hugegraph-src`
- Language: Java
- Paths:
  - `hugegraph-server/hugegraph-hstore/src/main/java/org/apache/hugegraph/backend/store/hstore/HstoreTables.java:35-150`
  - `hugegraph-server/hugegraph-hstore/src/main/java/org/apache/hugegraph/backend/store/hstore/HstoreStore.java:725-769`
  - `hugegraph-store/docs/query-engine.md:17-52`
  - `hugegraph-store/docs/query-engine.md:199-235`

Source evidence:

- `HstoreTables.Vertex` binds the vertex table to `HugeTableType.VERTEX` and
  resolves ID queries through `getById` (`HstoreTables.java:37-48`).
- `HstoreTables.Edge` chooses `OUT_EDGE` or `IN_EDGE` table names depending on
  direction (`HstoreTables.java:82-103`).
- `HstoreTables.IndexTable` uses an all-index table and treats ordinary index
  elimination differently from label-driven index deletion
  (`HstoreTables.java:106-130`).
- `HstoreGraphStore` registers separate managers for `VERTEX`, `OUT_EDGE`,
  `IN_EDGE`, `ALL_INDEX_TABLE`, `OLAP_TABLE`, task info, and server info
  (`HstoreStore.java:749-769`).
- The query-engine docs describe server-to-store planning where the server
  determines tables, extracts filters, identifies partitions, sends parallel
  store requests, applies filters at RocksDB scan level, computes aggregations,
  streams results, and then merges/deduplicates/orders/limits at the server
  (`query-engine.md:17-52`).
- HugeGraph models indexes as separate store tables; the docs show index table
  scans with keys shaped like `<index_value>:<vertex_id>` (`query-engine.md:199-235`).

Transferable idea:

Graph systems should make physical table families explicit before inventing
algorithm-specific storage. HugeGraph's shape says: do not store "the graph" as
one undifferentiated blob. Separate vertex records, outbound edges, inbound
edges, index rows, OLAP rows, and task metadata so planners can route work to
the cheapest physical family.

Rust translation:

```text
GraphStore
  table(VertexTable)
  table(OutEdgeTable)
  table(InEdgeTable)
  table(PropertyIndexTable)
  table(OlapSidecarTable)
  table(TaskStateTable)
```

For Knight Bus, this argues for:

- keeping OLTP record tables separate from OLAP sidecars;
- avoiding one universal "property graph row" encoding for every path;
- giving `gds.*` and Cypher procedures a table-family-aware physical planner;
- treating index scans as first-class table scans, not as helper methods hidden
  inside property lookup code.

Memory implication:

The low-RAM benefit is not that extra tables use less disk. They often use more
disk. The benefit is that a query can touch only `OUT_EDGE`, only `IN_EDGE`, or
only a property index instead of dragging unrelated vertex/property/edge state
into heap, page cache, or direct-I/O buffers.

When to use:

- Use for GDS projections with label/type/property filters.
- Use for traversal APIs that need one direction only.
- Use when property indexes can reduce scan width before relationship expansion.
- Use when OLAP sidecars need their own lifecycle and compaction cadence.

When not to use:

- Do not create a new physical table for every algorithm output.
- Do not duplicate topology just because one benchmark becomes faster.
- Do not hide write amplification; every additional table has an update cost.

Agent guidance:

When future agents implement graph storage, they should first ask which physical
family an operation needs: node, forward edge, reverse edge, property column,
index row, OLAP sidecar, or task state. If the answer is "all of them," require a
memory estimate before execution.

## Pattern 02: Access Path Is Predicate Plus Index Plus Residual Filter

Where found:

- Repository: `gitrefrepo/blazegraph-src`
- Language: Java
- Paths:
  - `bigdata-core/bigdata/src/java/com/bigdata/relation/IRelation.java:54-216`
  - `bigdata-core/bigdata/src/java/com/bigdata/relation/AbstractRelation.java:155-365`
  - `bigdata-core/bigdata-rdf/src/java/com/bigdata/rdf/spo/SPORelation.java:114-156`
  - `bigdata-core/bigdata-rdf/src/java/com/bigdata/rdf/spo/SPOKeyOrder.java:52-220`
  - `bigdata-core/bigdata-rdf/src/java/com/bigdata/rdf/store/AbstractTripleStore.java:740-790`
  - `bigdata-core/bigdata-rdf/src/java/com/bigdata/rdf/store/AbstractTripleStore.java:3208-3258`

Source evidence:

- `IRelation` describes a relation as backed by one or more indices and says the
  relation knows how to return the most efficient `IAccessPath` for a predicate
  (`IRelation.java:54-59`).
- `IRelation#getKeyOrder` defines a perfect access path as one where bound
  predicate values form a prefix in the index key space (`IRelation.java:155-179`).
- `IRelation#getAccessPath` says a non-perfect index must incorporate filters so
  irrelevant tuples are filtered during the scan, local to data, rather than
  materialized and filtered later (`IRelation.java:181-207`).
- `AbstractRelation` retrieves timestamped named indexes and wraps unisolated
  `Journal` or `TemporaryStore` indexes in `UnisolatedReadWriteIndex` to impose
  concurrency controls (`AbstractRelation.java:195-265`).
- `SPORelation` states that triples are converted to term IDs and inserted into
  statement indices for the three possible triple access paths, with all
  statement state replicated in each index (`SPORelation.java:114-124`).
- `SPOKeyOrder` enumerates natural triple and quad key orders: `SPO`, `OSP`,
  `POS`, and six quad permutations (`SPOKeyOrder.java:89-198`).
- `AbstractTripleStore` includes a `ONE_ACCESS_PATH` option that disables all
  but one statement index for special bulk-load purposes, while warning that it
  is not compatible with inference or high-level query (`AbstractTripleStore.java:740-761`).
- The same options block warns that a bloom filter can cost about 1 MB per small
  commit on the SPO/SPOC index and has scale-up limits (`AbstractTripleStore.java:763-790`).

Transferable idea:

An access path should be an explicit object:

```text
predicate shape
  + selected index order
  + residual filters
  + range/prefix bound
  + concurrency/snapshot view
  + memory estimate
```

This is better than a planner that only chooses "scan vertex table" or "scan
relationship table." Blazegraph's relation API makes query locality a contract:
if the index is imperfect, residual filters still move as close to the data as
possible.

Rust translation:

```rust
struct AccessPathPlan {
    family: PhysicalFamily,
    key_order: KeyOrder,
    prefix_bound: Option<KeyPrefix>,
    range_bound: Option<KeyRange>,
    residual_filter: Option<FilterExpr>,
    snapshot: SnapshotView,
    memory_estimate: MemoryEstimate,
}
```

For Knight Bus:

- A GDS projection filter should compile to an access-path plan before it
  creates any projected graph artifact.
- Label/type/property filters should be pushed into index/sidecar scans when
  they form useful prefixes.
- If an access path is imperfect, the residual filter should travel with the
  scan cursor to avoid materializing giant candidate sets.
- A "single access path for lowest RAM" mode must be benchmarked carefully:
  Blazegraph's own comments warn that deleting duplicate access paths can break
  high-level query capability.

Memory implication:

Duplicate indexes increase disk and write amplification. But the absence of the
right access path can force full scans, candidate materialization, or more page
cache churn. Lowest RAM is therefore not "fewest files." It is "fewest resident
bytes for the workload under a correctness-preserving access-path set."

Anti-pattern:

Do not optimize OLAP by deleting every auxiliary index. That can make PageRank's
topology smaller while making filtered projections, path expansion, and property
lookups explode.

## Pattern 03: Storage And Inference Layer Is A Compatibility Boundary

Where found:

- Repository: `gitrefrepo/eclipse-rdf4j-src`
- Language: Java
- Paths:
  - `core/sail/api/src/main/java/org/eclipse/rdf4j/sail/Sail.java:22-110`
  - `core/sail/api/src/main/java/org/eclipse/rdf4j/sail/SailConnection.java:35-430`
  - `core/sail/base/src/main/java/org/eclipse/rdf4j/sail/base/SailDataset.java:28-133`
  - `core/sail/base/src/main/java/org/eclipse/rdf4j/sail/base/SailDatasetTripleSource.java:33-120`

Source evidence:

- RDF4J defines `Sail` as "Storage And Inference Layer," an interface for RDF
  storage that can store statements and evaluate queries (`Sail.java:22-30`).
- `Sail` owns lifecycle, writability, `getConnection`, value factory, supported
  isolation levels, and default isolation level (`Sail.java:32-110`).
- `SailConnection` warns that active connections can block concurrent queries or
  updates depending on implementation (`SailConnection.java:35-38`).
- `SailConnection` allows a store to bypass the standard parser by preparing a
  sail-specific `TupleExpr` (`SailConnection.java:60-76`).
- `SailConnection` evaluates tuple expressions and exposes wildcard
  subject/predicate/object/context statement retrieval through closeable
  iterations (`SailConnection.java:78-122`).
- `SailConnection` defines transaction lifecycle operations: begin, prepare,
  commit, rollback, active-state checks, statement add/remove, update contexts,
  and namespace operations (`SailConnection.java:196-430`).
- `SailDataset` represents a point-in-time state for reads and says snapshot
  isolation keeps the dataset unchanged until release/close (`SailDataset.java:28-43`).
- `SailDatasetTripleSource` adapts a `SailDataset` into a query-evaluation
  triple source, wrapping closeable statement iterations and translating storage
  exceptions into query exceptions (`SailDatasetTripleSource.java:33-120`).

Transferable idea:

RDF4J treats storage/inference as a boundary below repository/query APIs. That
boundary is small enough to swap stores but rich enough to express:

- snapshot reads;
- wildcard statement access;
- query evaluation;
- parser bypass hooks;
- update contexts;
- isolation levels;
- closeable streaming iterators.

Rust translation:

```rust
trait GraphStorageLayer {
    type ReadSnapshot<'a>: GraphReadSnapshot<'a>;
    type WriteTxn<'a>: GraphWriteTxn<'a>;

    fn open_read(&self, level: IsolationLevel) -> Result<Self::ReadSnapshot<'_>>;
    fn open_write(&self, level: IsolationLevel) -> Result<Self::WriteTxn<'_>>;
}

trait GraphReadSnapshot<'a> {
    fn statements(&'a self, pattern: StatementPattern)
        -> Result<Box<dyn Iterator<Item = Result<StatementRef<'a>>> + 'a>>;
}
```

For Knight Bus:

- Neo4j compatibility should sit above storage through traits that preserve
  transaction and cursor semantics.
- OLAP snapshots should be read snapshots, not mutable graph objects.
- GDS procedures should consume a logical graph view backed by closeable cursors
  or direct-I/O streams.
- Parser bypass is a useful design hook: a Rust implementation can route
  `CALL gds.*`, known internal procedures, or compiled Cypher fragments to
  specialized logical plans before the general planner handles them.

Memory implication:

Closeable iterators matter. They make resource ownership explicit and keep large
statement/result streams from becoming `Vec` materializations by accident.

Concurrency implication:

Expose connection/transaction lifecycle in the API shape. If a connection can
block others, tests must prove close/rollback behavior under errors.

## Pattern 04: Object-Safe Transaction Trait Keeps Rust Graph Backends Swappable

Where found:

- Repository: `gitrefrepo/indradb-src`
- Language: Rust
- CodeGraphContext: `/tmp/codex-code-intel/codegraphcontext/indradb-batch04`
- Paths:
  - `lib/src/database.rs:1-207`
  - `lib/src/database.rs:260-620`
  - `lib/src/models/queries.rs:1-280`
  - `lib/src/rdb/datastore.rs:1-460`
  - `lib/src/rdb/managers.rs:1-240`
  - `lib/fuzz/fuzz_targets/compare.rs:1-220`

Source evidence:

- `Transaction<'a>` deliberately avoids generic arguments to preserve object
  safety for plugins (`database.rs:14-24`).
- The transaction trait separates vertex scans, edge scans, reversed edge scans,
  property lookups, property-value index lookups, deletes, sync, creation,
  bulk-insert, property indexing, and property updates (`database.rs:24-196`).
- `Datastore` has an associated transaction type and a `transaction()` factory
  (`database.rs:198-207`).
- `Database<D>` stores a generic datastore and keeps shared query handling above
  implementation-specific storage (`database.rs:209-228`).
- Query execution uses `q.output_len()` to pre-size output storage
  (`database.rs:275-286`).
- The internal query enum includes all vertices, vertex ranges, property
  presence/value filters, all edges, specific edges, pipe traversal, pipe
  property, include, and count (`queries.rs:64-153`).
- Query extension traits provide fluent traversal and property-filter methods
  such as outbound, inbound, property presence/value, properties, and include
  (`queries.rs:155-225`).
- RocksDB storage uses column families for vertices, edge ranges, reversed edge
  ranges, vertex properties, edge properties, property-value indexes, and
  metadata (`rdb/datastore.rs:12-21`).
- `RocksdbTransaction` stores separate managers for vertex, edge, forward edge
  range, reverse edge range, property, property-value, and metadata handling
  (`rdb/datastore.rs:23-35`).
- Forward and reverse edge range managers are both maintained when setting an
  edge (`managers.rs:134-156`).
- Property indexing scans existing vertices and edges and writes property-value
  index rows (`rdb/datastore.rs:309-337`).
- The fuzz target generates operations and query shapes to compare memory and
  RocksDB behavior through the same public model (`compare.rs:10-220`).

Transferable idea:

IndraDB is not a full Neo4j replacement. Its value here is the minimal Rust
shape:

```text
Database<D: Datastore>
  -> Transaction<'a>
     -> vertex/edge/property/index primitives
  -> shared query interpreter
  -> backend-specific managers
  -> fuzz equivalence between backends
```

Rust translation for Knight Bus:

- Define storage traits before implementing file formats.
- Keep graph query/procedure semantics above the backend-specific store.
- Use associated transaction types for zero-cost static dispatch where possible.
- Use object-safe trait objects only at plugin/procedure boundaries where dynamic
  loading or heterogeneous backends are required.
- Maintain forward and reverse adjacency families as a correctness requirement,
  not a convenience.

Memory implication:

Pre-sizing output with `output_len()` is small but instructive. A lower-RAM
rewrite should make output cardinality estimates part of every logical plan and
every `gds.*.estimate` path. When a query cannot estimate output, it should
stream or spill rather than allocate a guessed `Vec`.

Testing implication:

Backend equivalence fuzzing is a strong pattern for the rewrite:

```text
same operation stream
  -> memory backend
  -> mmap/record backend
  -> OLAP snapshot backend
  -> compare visible query results
```

This is particularly useful for WAL-to-OLAP freshness: the same generated
transaction stream should be replayed against OLTP records and OLAP sidecars.

Risk:

IndraDB's query interpreter collects many intermediate results into vectors. For
Knight Bus, the same trait shape should be paired with streaming cursors and
budget-aware materialization.

## Pattern 05: Pushdown Visitor Plus Storage Mini-Plan

Where found:

- Repository: `gitrefrepo/nebula-src`
- Language: C++
- Paths:
  - `src/graph/visitor/ExtractFilterExprVisitor.cpp:1-180`
  - `src/graph/visitor/ExtractFilterExprVisitor.cpp:250-520`
  - `src/storage/query/GetNeighborsProcessor.h:1-130`
  - `src/storage/query/GetNeighborsProcessor.cpp:1-420`
  - `src/storage/exec/GetNeighborsNode.h:1-220`
  - `src/storage/exec/IndexScanNode.h:1-170`
  - `src/storage/exec/IndexScanNode.cpp:376-565`

Source evidence:

- `ExtractFilterExprVisitor` decides whether expression variants can be pushed
  based on expression type, schema availability, and push target such as
  get-neighbors, get-vertices, or get-edges (`ExtractFilterExprVisitor.cpp:13-180`).
- The visitor can split logical `AND`/`OR` expressions into pushed and remaining
  fragments, preserving residual expressions when only part of a predicate is
  storage-local (`ExtractFilterExprVisitor.cpp:250-520`).
- `GetNeighborsProcessor` defines a storage query processor with counters,
  context building, plan building, single-thread and multi-thread execution, and
  executor-based partition work (`GetNeighborsProcessor.h:20-89`).
- The processor wraps execution with memory check scopes and catches
  `std::bad_alloc` as storage memory exceeded (`GetNeighborsProcessor.cpp:23-30`,
  `116-205`).
- `GetNeighborsProcessor::buildPlan` constructs a storage mini-plan: tag nodes,
  edge nodes, hash join or multi-tag node, optional filter node, optional
  aggregate node, and output `GetNeighborsNode` (`GetNeighborsProcessor.cpp:208-310`).
- The storage processor builds tag/edge context, yields, filter, and result
  column names before executing (`GetNeighborsProcessor.cpp:313-420`).
- `GetNeighborsNode` assembles rows by taking tag results from join nodes,
  iterating edge results, collecting edge properties, enforcing per-vertex
  limits, deduplicating self-reflective edges, and writing into the result
  dataset (`GetNeighborsNode.h:18-168`).
- `IndexScanNode` is an access-to-disk node that scans index data and fetches
  base data only when necessary (`IndexScanNode.h:25-35`).
- During `IndexScanNode::init`, required columns are compared against indexed
  fields to decide whether base data access is needed (`IndexScanNode.cpp:376-417`).
- `IndexScanNode::doNext` can return rows directly from the index when compatible
  and base access is unnecessary; otherwise it fetches base data, rechecks
  qualification, and decodes required columns (`IndexScanNode.cpp:426-470`).

Transferable idea:

Storage pushdown is not a flag. It is a pipeline:

```text
logical predicate
  -> pushdown visitor
  -> pushed fragment + residual fragment
  -> storage-local mini-plan
  -> memory-guarded partition execution
  -> result merge
```

Rust translation:

```rust
struct PushdownSplit {
    pushed: Option<Expr>,
    residual: Option<Expr>,
}

enum StoragePlanNode {
    TagScan,
    EdgeScan,
    HashJoin,
    Filter,
    Aggregate,
    GetNeighbors,
    IndexScan { need_base_data: bool },
}
```

For Knight Bus:

- GDS filtered projections should have a pushdown visitor that splits filters
  into sidecar/index-local and residual pieces.
- `gds.graph.project`, `MATCH` path expansion, and neighborhood queries should
  not share a single naive "neighbors then filter" code path.
- Per-partition or per-cell execution should have memory guards and explicit
  result-buffer ownership.
- Index scans should distinguish "index-only" from "must fetch base record" in
  memory estimates.

Memory implication:

The line between index-only and base-data fetch is critical. Index-only scans can
avoid touching record/property pages. Base fetches can explode page-cache or
direct-I/O buffers. Future `estimate` procedures should report this difference.

Concurrency implication:

Parallel partition execution creates separate contexts and result datasets before
merging. That is safer than sharing one mutable result buffer across workers, but
it requires budget enforcement because each partition can add local buffers.

## Pattern 06: Graph Traversal As Typed KV Range, Not In-Memory Object Walk

Where found:

- Repository: `gitrefrepo/surrealdb-src`
- Language: Rust
- Paths:
  - `surrealdb/core/src/kvs/ds.rs:90-230`
  - `surrealdb/core/src/kvs/ds.rs:1830-1945`
  - `surrealdb/core/src/kvs/tx.rs:45-235`
  - `surrealdb/core/src/key/graph/mod.rs:110-260`
  - `surrealdb/core/src/dbs/processor.rs:120-210`
  - `surrealdb/core/src/dbs/processor.rs:1040-1085`
  - `surrealdb/core/src/dbs/plan.rs:110-145`
  - `surrealdb/ast/src/lib.rs:220-390`
  - `surrealdb/ast/src/lib.rs:960-1040`

Source evidence:

- `Datastore` owns transaction factory, datastore ID, auth state, dynamic
  configuration, slow log, transaction timeout, capabilities, notification
  channel, index stores, cross-transaction cache, index builder, buckets,
  sequences, and async event trigger (`ds.rs:102-148`).
- `TransactionFactory::transaction` converts read/write and pessimistic/optimistic
  modes into backend booleans, then asks the backend builder for a transaction
  and wraps it in a higher-level `Transaction` (`ds.rs:168-219`).
- `Datastore::transaction` exposes the transaction factory (`ds.rs:1837-1839`).
- `Datastore::execute` parses SQL text with capabilities and then processes the
  AST (`ds.rs:1874-1902`).
- `Transaction` carries local/remote state, underlying transactor, transaction
  cache, sequences, changefeed writer, async event trigger, pending index-batch
  cleanup queue, and close checks (`tx.rs:45-126`).
- `Transaction::cancel` clears buffered changefeed entries and enqueues pending
  index batches for cleanup before canceling the underlying transaction
  (`tx.rs:128-140`).
- `Transaction::commit` stores buffered changes, cancels on store-change failure,
  cleans pending index batches on commit failure, commits the underlying
  transaction, and triggers async events after commit (`tx.rs:142-165`).
- Graph keys encode namespace, database, table, record ID, graph direction,
  foreign table, and foreign key in a typed store key (`key/graph/mod.rs:122-156`,
  `236-260`).
- Graph key helper functions produce direction-aware prefix and suffix ranges for
  all edges, in edges, out edges, and foreign-table-constrained edges
  (`key/graph/mod.rs:158-228`).
- The processor treats graph lookup as a special collectable case that decodes a
  graph key and may rebuild downstream table context if the foreign table differs
  from the originating table (`processor.rs:120-210`).
- Graph lookup planning chooses prefix/suffix ranges for both directions or a
  single direction, and expands specific edge types into separate ranges
  (`processor.rs:1040-1085`).
- `dbs/plan.rs` names graph lookup as "Iterate Edges" in plan descriptions
  (`plan.rs:123-145`).
- The AST includes query, transaction, `WITH INDEX`, and index definition nodes,
  including unique, count, full-text, and HNSW index kinds (`ast/src/lib.rs:220-390`,
  `960-1040`).

Transferable idea:

SurrealDB shows a Rust-native multi-model approach where graph traversal is a
typed key-range lookup, not an object graph pointer chase. The storage key itself
encodes enough structure to support directional graph scans.

Rust translation for Knight Bus:

```text
/graph/ns/db/table/record-id/direction/foreign-table/foreign-key

prefix(record)      -> all graph edges for record
egprefix(record,in) -> inbound graph edges for record
egprefix(record,out)-> outbound graph edges for record
ftprefix(..., type) -> constrained edge table/type scan
```

For Neo4j compatibility, this does not replace Neo4j's record-shaped OLTP
requirements. It does suggest a low-RAM secondary index / OLAP freshness path:

- encode WAL-derived edge receipts as typed key ranges;
- query recent delta edges by directional prefix;
- merge typed KV delta ranges with immutable CSR snapshots;
- use graph-key ranges for local freshness before full OLAP compaction.

Memory implication:

Typed KV ranges can make small fresh-delta queries cheap. They do not solve
global algorithms alone. PageRank and WCC still need global edge streams and
bounded algorithm state, but graph-key deltas can avoid rebuilding a 50 GB OLAP
snapshot for a tiny edit batch.

Testing implication:

Graph key encoding requires golden tests. Direction, table, record ID, and
foreign key order are compatibility-critical because one byte-order mistake
silently turns local traversal into a scan miss.

## Pattern 07: Duplicate Reference Shelf Rows Need Explicit Resolution

Where found:

- Repository family:
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/indradb__indradb`
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/kuzudb__kuzu`
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/neo4j__neo4j`
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/dgraph-io__dgraph`
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/PrestonKnopp__tree-sitter-gdscript`
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/surrealdb__surrealql-tree-sitter`
  - `parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/zxfgds__mcp-code-indexer`

Source evidence:

- The directories exist and contain source or top-level project files.
- Some are duplicate upstreams already represented by primary `gitrefrepo/`
  clones:
  - `indradb__indradb` duplicates `gitrefrepo/indradb-src`, now covered above.
  - `kuzudb__kuzu` was already direct-source cited in earlier Kuzu patterns.
  - `neo4j__neo4j` duplicates `gitrefrepo/Neo4j family/neo4j-src`, already
    covered in batch 02.
- Some are adjacent parser/code-indexing tools rather than graph DB systems:
  - `tree-sitter-gdscript` is useful for parser tooling taste, but low-signal
    for Neo4j/GDS compatibility.
  - `surrealql-tree-sitter` is useful for query grammar extraction, but not as
    strong as `surrealdb-src` for storage/query execution.
  - `mcp-code-indexer` is useful for code-intelligence workflows, not graph
    database execution.
- `dgraph-io__dgraph` is not fully closed by this batch. The primary
  `gitrefrepo/dgraph-src` row was only name-cited before this pass; Dgraph still
  deserves a future direct-source batch for posting-list, Raft, Badger, schema,
  and GraphQL+-style query patterns.

Transferable idea:

A Desktop-wide corpus needs a duplicate-resolution policy. Otherwise agents will
keep spending research budget rediscovering the same upstream clone under
multiple shelves while genuinely uncovered systems remain untouched.

Policy:

```text
if duplicate_upstream_has_primary_direct_source:
    mark duplicate as low_signal_duplicate_or_primary_covered
elif repo_is_parser_or_code_tool_in_wrong_slice:
    mark low_signal_for_neo4j_gds_compat and optionally reassign mentally to parser/tooling
else:
    keep as metadata_browsed_gap and schedule direct-source read
```

Rust rewrite implication:

This is not merely bookkeeping. It affects future agent behavior. If duplicate
repos remain "gaps," agents will thrash. If they are marked as low-signal or
covered-by-primary, the next research worker can spend time on uncited high-value
systems such as Dgraph, remaining parser/code-intelligence representatives, and
storage engines with direct memory-layout relevance.

## Cross-System Synthesis

| system | pattern extracted | Rust rewrite lesson |
| --- | --- | --- |
| HugeGraph | physical graph table families plus store-local pushdown | separate OLTP, index, edge-direction, and OLAP families before algorithm work |
| Blazegraph | access path = predicate + key order + residual filters | maintain enough access paths to avoid materializing huge candidates |
| RDF4J | Sail storage/inference boundary with closeable iterators | preserve API semantics above swappable read/write storage traits |
| IndraDB | object-safe transaction trait and backend equivalence fuzzing | keep graph API testable across memory, record, and snapshot backends |
| NebulaGraph | pushdown visitor and storage-local mini-plan | compile filters/projections into storage plans with residual fragments |
| SurrealDB | graph traversal as typed KV range | use directional key ranges for fresh deltas and local graph lookups |

## Source / Inference / Speculation Table

| claim | classification | evidence |
| --- | --- | --- |
| HugeGraph separates vertex, outbound edge, inbound edge, index, OLAP, task, and server-info table managers. | sourced fact | `HstoreStore.java:749-769` |
| HugeGraph docs describe store-side filters, aggregation, index scans, streaming, and parallel partition execution. | sourced fact | `query-engine.md:17-52`, `199-235` |
| Blazegraph statement indices replicate state across multiple SPO/POS/OSP and quad key orders. | sourced fact | `SPORelation.java:114-124`, `SPOKeyOrder.java:89-198` |
| Disabling duplicate Blazegraph access paths is not compatible with high-level query/inference. | sourced fact | `AbstractTripleStore.java:740-761` |
| RDF4J Sail is a storage/inference compatibility boundary with transactional closeable iterators. | sourced fact | `Sail.java:22-110`, `SailConnection.java:35-430`, `SailDataset.java:28-133` |
| IndraDB's transaction trait is object-safe by design for plugins. | sourced fact | `database.rs:14-24` |
| Nebula splits pushed and residual filters and builds a storage-local `GetNeighbors` plan. | sourced fact | `ExtractFilterExprVisitor.cpp:250-520`, `GetNeighborsProcessor.cpp:208-310` |
| SurrealDB encodes graph traversal as directional typed KV key ranges. | sourced fact | `key/graph/mod.rs:122-228`, `processor.rs:1040-1085` |
| Knight Bus should model GDS projection filters as access-path plans before materializing projected graphs. | inference | Derived from HugeGraph, Blazegraph, and Nebula source patterns |
| Typed KV edge deltas could reduce small-update OLAP freshness cost. | inference | Derived from SurrealDB graph key ranges and Knight Bus WAL-to-OLAP design goals |
| Dgraph remains a future high-value direct-source gap. | sourced bookkeeping plus inference | Ledger row for `gitrefrepo/dgraph-src` is only `metadata_browsed_name_cited`; duplicate row remains a gap |
| The exact best Knight Bus physical layout is settled by this batch. | speculation rejected | This batch extracts patterns; it does not benchmark Knight Bus layouts |

## Requirements For Future Agents

When implementing or extending the Rust rewrite research from this batch:

1. Treat "lowest RAM" as access-path and working-set minimization, not as "fewest
   files."
2. Add a `StorageFamily` or equivalent physical-family concept before deep
   algorithm implementation.
3. Make pushdown decisions visible in explain/estimate output:
   - which predicates were pushed;
   - which predicates remained;
   - whether an index-only scan is possible;
   - whether base records/properties must be fetched.
4. Preserve a closeable streaming cursor model for large results.
5. Add backend-equivalence tests that compare memory backend, OLTP record
   backend, and OLAP snapshot/delta backend behavior over the same operation
   stream.
6. Keep duplicate-reference-shelf policy in the ledger so later agents do not
   rediscover already covered upstreams.

## Follow-Up Gaps After Batch 04

High-value gaps that remain:

- Dgraph direct-source read: posting lists, Badger/Raft storage, schema/index
  planning, query execution, and GraphQL compatibility.
- Parser/code-intelligence representatives: many rows remain unclosed and should
  be triaged by whether they teach grammar extraction, source indexing, or
  agentic navigation.
- Rust systems/tooling rows: prioritize repos with memory ownership, async
  runtime, WAL/log, snapshot, mmap, or benchmark harness patterns.
- Remaining graph algorithms/sparse rows: prioritize exact-memory or spillable
  algorithm implementations before broad algorithm catalog summaries.

This batch materially improves the adjacent graph-system evidence base, but it
does not complete the Desktop-wide objective.

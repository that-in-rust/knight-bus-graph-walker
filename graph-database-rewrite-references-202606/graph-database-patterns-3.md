# Graph Database Patterns 3: Storage Engines, Query Execution, Columnar Systems, and Systems Infrastructure

Agent 3 first-version corpus note for the Neo4j-like graph database rewrite effort.

This section focuses on storage engines, query execution engines, columnar/vector systems, indexes, logs, persistence, and low-level systems infrastructure. It is written for a Rust implementation whose central goal is lower RAM usage without giving up durability, traversal performance, or operator-level observability.

Important evidence rule: graph-tool output in this file is used only as navigation evidence. Engineering claims below are grounded in direct source reads where file paths and line ranges are listed. Repositories not directly read in this pass are named in the coverage/gaps section rather than silently folded into claims.

## Design Bias for a Lower-RAM Neo4j Rewrite

A Neo4j-like graph store has several conflicting memory pressures:

- topology wants fast adjacency expansion;
- properties want compact typed storage and predicate pushdown;
- labels and secondary indexes want mutable lookup structures;
- transactions want WAL and crash safety;
- queries want streaming pipelines rather than materialized intermediate graphs;
- long traversals want scheduler fairness and memory reservations;
- observability wants per-operator counters without allocating strings in hot paths.

The recurring pattern across Arrow, DataFusion, fjall, redb, sled, RocksDB, rust-rocksdb, and TiKV is: keep durable bytes and logical objects separate, make ownership explicit, reserve memory before large allocations, stream batches through plans, and make crash recovery a deliberately small state machine.

## Pattern Index

1. Arc-backed immutable buffers with pointer-stable slicing
2. Typed scalar buffers as zero-copy views
3. ArrayData and RecordBatch as the physical row-to-column boundary
4. Streaming physical plans that return lazy RecordBatch streams
5. Lazy in-memory execution nodes for tests and bounded memory
6. Statistics pruning as a graph-segment skip layer
7. Parquet page-level pruning as the model for property-page skipping
8. RAII memory reservations and spill-aware memory pools
9. Cooperative scheduling around long-running streams
10. Metrics objects with stable labels and pruning counters
11. Checksum-terminated WAL batches with persist modes
12. Journal recovery by truncating partial batches
13. Sealed-journal recovery with per-keyspace watermarks
14. Packed page identifiers and region-aware page allocation
15. Dual commit slots with an atomic selector byte
16. Scoped page ownership and cache eviction safety
17. Buddy allocation for variable-size pages
18. Prefix-compressed leaves with tunable fanout
19. Flush epochs and cooperative serialization
20. Write buffer managers and explicit write stalls
21. Fragmented WAL records over fixed-size log blocks
22. Flush and compaction as staged jobs
23. Lifetime-safe FFI iterators and pinned engine-owned slices
24. Engine abstraction around RocksDB-like storage
25. Priority read pools and admission-controlled scheduling
26. Memory-lock conflict checks as a graph transaction pattern
27. Applied storage blueprint for Neo4j-in-Rust
28. Testing, benchmark, and observability implications

## 1. Arc-Backed Immutable Buffers with Pointer-Stable Slicing

**Where found:** Apache Arrow Rust.

**Language/framework:** Rust, Arrow memory/buffer layer.

**Source paths:**

- `gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/immutable.rs`, especially lines 33-37, 70-85, 102-103, 154-175, 205-237.

**Pattern name:** Shared immutable byte storage with O(1) slicing.

Arrow's `Buffer` is a contiguous immutable memory region. It is backed by `Arc<Bytes>`, keeps a raw pointer and length, and is marked `Send` and `Sync`. The source comments explicitly call out slice/clone without copying and sharing memory from non-Rust sources. One especially useful implementation detail is that `Buffer` stores a pointer directly rather than an offset. The comments note that offset arithmetic can hurt LLVM vectorization.

Short structural sketch:

```rust
struct Buffer {
    data: Arc<Bytes>,
    ptr: *const u8,
    length: usize,
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}
```

**Why it matters for Neo4j-in-Rust:** graph stores often copy property values, adjacency lists, and transaction buffers too aggressively. A lower-RAM graph store should be able to represent a slice of a property column, relationship id array, label bitmap, or mmap-backed page as a cheap immutable view. This lets traversal and filtering operators share the same physical bytes without materializing per-operator vectors.

**Use when:**

- properties are immutable within a snapshot;
- query operators need cheap projection/slicing;
- memory may come from mmap, FFI, page cache, or decompressed blocks;
- the store can guarantee lifetime through reference-counted ownership.

**Do not use when:**

- the data must be mutated in place;
- aliasing rules are unclear;
- the backing memory can disappear outside Rust's ownership model;
- slicing hides expensive decompression or IO.

**Rust translation for the graph rewrite:**

- Define a `GraphBuffer` or `PageBytes` wrapper around `Arc<[u8]>`, `Bytes`, or a custom allocation owner.
- Store pointer and length for hot vectorized loops only after proving alignment and lifetime.
- Keep mutation in append-only WAL/memtable/page-builder layers; expose immutable buffers to query execution.
- Allow external ownership only through an unsafe constructor that records a custom owner object.

**Risks:**

- Unsafe `Send`/`Sync` becomes a global correctness claim.
- A pointer-plus-length view can outlive the intended page pin if ownership is not represented.
- Custom allocations need clear drop semantics.

**Memory implications:** this is one of the highest-leverage RAM reducers. Slicing a 1 GB property column into query batches should allocate view metadata, not copy bytes.

**Concurrency implications:** immutable buffers are ideal for snapshot reads and parallel operators. Writers must publish new buffers rather than modifying existing ones.

**Testing implications:**

- Test slicing at non-zero offsets.
- Test clone/drop ordering under concurrent reads.
- Test FFI/custom allocations with Miri where possible.
- Add property tests for `slice(a).slice(b)` equivalence.

**How future agents should apply it:** use Arrow's buffer structure as the model for all immutable value vectors, relationship-id arrays, dictionary pages, and decoded posting lists.

## 2. Typed Scalar Buffers as Zero-Copy Views

**Where found:** Apache Arrow Rust.

**Language/framework:** Rust, Arrow typed buffer layer.

**Source paths:**

- `gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/scalar.rs`, especially lines 26-33, 65-70, 89-94, 114-117, 156-168, 184-199, 222-228.

**Pattern name:** Typed view over an immutable byte buffer.

`ScalarBuffer<T>` wraps `Buffer` and exposes a typed slice. The source says it behaves like `Arc<Vec<T>>`, but with O(1) slicing and external-memory support. Construction checks byte offset, length, alignment, and element size. Deref to `[T]` uses raw parts after those checks.

Sketch:

```rust
struct ScalarBuffer<T> {
    buffer: Buffer,
    len: usize,
    _marker: PhantomData<T>,
}

impl<T: ArrowNativeType> Deref for ScalarBuffer<T> {
    type Target = [T];
}
```

**Why it matters for Neo4j-in-Rust:** Neo4j-style stores have many typed primitive arrays: node ids, relationship ids, label ids, property keys, offsets, degrees, timestamps, transaction ids, and vector dimensions. Storing them as typed zero-copy views avoids deserializing into `Vec<T>` at every operator boundary.

**Use when:**

- the graph store has fixed-width values;
- property columns or adjacency offsets can be read as typed arrays;
- the operator only needs immutable read access.

**Do not use when:**

- the data may be unaligned;
- endian conversion is needed per element and has not been centralized;
- values need validation beyond width/alignment.

**Rust translation:**

- Build `NodeIdBuffer`, `RelIdBuffer`, `OffsetBuffer`, and `LabelIdBuffer` as typed wrappers around a shared byte buffer.
- Perform alignment and endian validation once at construction.
- Expose `&[NodeId]` only for native-endian internal pages; expose iterator conversion for portable on-disk formats.

**Risks:** typed views can accidentally bless corrupt bytes as valid domain ids. Add validation for reserved id ranges, monotonic offsets, and page-local bounds.

**Memory/concurrency/testing implications:**

- Memory: avoids per-query vector materialization.
- Concurrency: immutable typed slices can be shared across threads.
- Testing: fuzz construct-from-bytes; assert panics/errors on misalignment and overflow; test zero-copy conversion only when buffer is uniquely owned.

**How future agents should apply it:** treat every hot numeric graph array as a typed buffer candidate before designing a heap-allocated object model.

## 3. ArrayData and RecordBatch as the Physical Row-to-Column Boundary

**Where found:** Apache Arrow Rust.

**Language/framework:** Rust, Arrow arrays and batches.

**Source paths:**

- `gitrefrepo/apache-arrow-rs-src/arrow-data/src/data.rs`, especially lines 64-157 and 159-251.
- `gitrefrepo/apache-arrow-rs-src/arrow-array/src/record_batch.rs`, especially lines 223-232, 263-393, 454-480, 675-695.

**Pattern name:** Separate logical schema from physical buffers and children.

`ArrayData` describes physical buffers, child arrays, nulls, length, and offset. The constructor logic knows that booleans are bit-packed, primitives are fixed-width, Utf8/Binary use offset buffers plus value buffers, lists use offsets plus child data, dictionaries use keys plus values, and unions use type ids. `RecordBatch` then groups schema and columns with a row count, validates column count, data types, nullability, and row counts, and implements projection and slicing without copying column data.

Sketch:

```rust
struct ArrayData {
    data_type: DataType,
    len: usize,
    offset: usize,
    buffers: Vec<Buffer>,
    child_data: Vec<ArrayData>,
    nulls: Option<NullBuffer>,
}

struct RecordBatch {
    schema: SchemaRef,
    columns: Vec<Arc<dyn Array>>,
    row_count: usize,
}
```

**Why it matters for Neo4j-in-Rust:** a graph DB should not represent query rows as boxed `Node` and `Relationship` objects. It should represent query batches as columns: node id column, relationship id column, property value column, label bitmap column, path state column. This makes property scans, label filters, degree filters, and path expansions work with cache-friendly vectors.

**Use when:**

- query execution crosses from storage to operators;
- intermediate results can be batched;
- schema and physical layout must be validated once.

**Do not use when:**

- the result is genuinely single-record transactional state;
- the query needs recursive variable-length path objects that cannot be columnized without a path arena;
- validation overhead dominates tiny operations.

**Rust translation:**

- Define a graph `Batch` abstraction compatible with Arrow `RecordBatch` or directly use Arrow where dependencies are acceptable.
- Use columns for ids, offsets, labels, property keys, and property values.
- Keep path expansion state in compact side buffers: parent row id, frontier offset, depth, and last relationship id.
- Use projection and slicing for subplans instead of rebuilding rows.

**Risks:**

- Null semantics must be explicit. Graph "property absent" and SQL null are not always the same.
- Nested paths may tempt heap allocation; prefer offset-buffer encodings.
- Schema evolution must not invalidate old page layouts.

**Memory/concurrency/testing implications:**

- Memory: batch slicing avoids copying intermediate rows.
- Concurrency: immutable batches can be shared across parallel operators.
- Testing: validate all generated batches against schema; test projection preserves row count; test offsets with non-zero batch slices.

**How future agents should apply it:** model graph query execution on columnar batches even if the public API exposes graph entities.

## 4. Streaming Physical Plans that Return Lazy RecordBatch Streams

**Where found:** Apache DataFusion.

**Language/framework:** Rust, async physical query execution.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs`, especially lines 66-94, 118-128, 140-196, 198-209, 236-331.

**Pattern name:** Physical plan nodes as `Send + Sync` stream factories.

DataFusion's `ExecutionPlan` represents physical plan nodes. A plan has schema and plan properties for the optimizer, declares required distribution and ordering, exposes children and child replacement, can be repartitioned, and executes by returning a `SendableRecordBatchStream`. The comments are particularly valuable: `execute` should be lazy until first poll, errors should be yielded in the stream, stream drop should release resources, raw spawning is discouraged in favor of the runtime abstraction, and long-running work must yield regularly.

**Why it matters for Neo4j-in-Rust:** a graph query engine can leak RAM by materializing every traversal frontier, optional match, sort, or aggregation. A streaming physical plan keeps each operator bounded by batch size and reservation size.

**Use when:**

- operators can process batches incrementally;
- joins, traversals, expansions, and projections need optimizer-visible properties;
- the engine must support cancellation.

**Do not use when:**

- a transaction requires all-or-nothing mutation before a result stream;
- the operator is truly point lookup only and async stream overhead is unnecessary.

**Rust translation:**

```rust
trait GraphExecutionPlan: Send + Sync + Debug {
    fn schema(&self) -> GraphSchemaRef;
    fn properties(&self) -> PlanProperties;
    fn children(&self) -> Vec<&Arc<dyn GraphExecutionPlan>>;
    fn execute(&self, partition: usize, ctx: Arc<TaskContext>)
        -> Result<SendableGraphBatchStream>;
}
```

For graph-specific operators, add properties such as:

- preserves node id ordering;
- preserves path depth ordering;
- requires partitioning by node id or relationship type;
- can spill frontier state;
- benefits from input partitioning.

**Risks:**

- It is easy to create an async stream that buffers too much before first poll.
- Cancellation is only reliable if drop paths release page pins and reservations.
- Raw task spawning can bypass memory accounting and tracing.

**Memory/concurrency/testing implications:**

- Memory: each operator should reserve before constructing output batches.
- Concurrency: plan nodes are shared; per-execution mutable state belongs in streams.
- Testing: test that dropping a stream frees reservations and page pins; test lazy execution with a source that panics if polled too early.

**How future agents should apply it:** build graph operators as stream-producing plan nodes, not as recursive functions that return `Vec<Row>`.

## 5. Lazy In-Memory Execution Nodes for Tests and Bounded Memory

**Where found:** Apache DataFusion.

**Language/framework:** Rust, physical plan testing utilities.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/memory.rs`, especially lines 44-58, 90-122, 136-156, 197-204.

**Pattern name:** Lazy batch generation for tests and synthetic inputs.

`MemoryStream` streams through a vector of `RecordBatch` values, optionally projects and slices on poll, and may hold a `MemoryReservation`. `LazyMemoryExec` wraps a `LazyBatchGenerator` so output batches are generated lazily and constant memory can be preserved.

**Why it matters for Neo4j-in-Rust:** graph engines need many deterministic tests: path expansion, fanout, cycles, deletes, property filters, and snapshot isolation. Lazy synthetic sources let tests describe very large graph frontiers without allocating all rows at once.

**Use when:**

- testing query operators;
- benchmarking batch sizes;
- simulating large graph frontiers.

**Do not use when:**

- it masks real storage latency;
- it hides page pin or WAL interaction.

**Rust translation:** implement `LazyGraphSourceExec` for tests. It should generate `GraphBatch` values on demand, respect projection, and optionally hold a memory reservation.

**Risks:** synthetic sources can make the query engine look more streaming than real storage sources. Pair them with storage-backed tests.

**Memory/concurrency/testing implications:** make "does not preallocate all rows" a testable property. Use counters in the generator to prove only the requested batches were materialized.

**How future agents should apply it:** every graph physical operator should have both storage-backed tests and lazy synthetic tests.

## 6. Statistics Pruning as a Graph-Segment Skip Layer

**Where found:** Apache DataFusion.

**Language/framework:** Rust, predicate pruning.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/pruning/src/pruning_predicate.rs`, especially lines 56-80, 169-257, 361-378, 434-503, 519-545.

**Pattern name:** Evaluate query predicates against container statistics before reading data.

`PruningPredicate` rewrites filter expressions to evaluate against min/max/null count/row count statistics. It works over many containers at once and returns a boolean per container. `true` means "may match"; `false` means safe to prune. The source is careful about SQL null semantics and literal guarantees.

**Why it matters for Neo4j-in-Rust:** graph stores should skip entire property pages, adjacency segments, label partitions, and relationship-type partitions before touching bytes. For example:

- node property `age > 50`: skip property pages whose max age <= 50;
- relationship type filter: skip adjacency segment with no matching type id;
- label filter: skip node id range whose label bitmap summary cannot match;
- temporal relationship query: skip edge pages outside timestamp range.

**Use when:**

- pages/segments have cheap min/max/count/bloom metadata;
- predicates can be compiled into a stats expression;
- false negatives are impossible.

**Do not use when:**

- statistics are stale or not tied to the same snapshot;
- graph semantics require existence checks not represented by stats;
- the predicate contains user functions with side effects.

**Rust translation:**

```rust
trait SegmentStatistics {
    fn min_value(&self, column: PropertyId) -> Option<ArrayRef>;
    fn max_value(&self, column: PropertyId) -> Option<ArrayRef>;
    fn null_count(&self, column: PropertyId) -> Option<ArrayRef>;
    fn row_count(&self) -> ArrayRef;
}

fn prune_segments(predicate: &GraphPredicate, stats: &dyn SegmentStatistics) -> Vec<bool> {
    // true = may match, false = skip
}
```

**Risks:**

- Graph "missing property" semantics are not SQL null semantics by default.
- Stats must be atomically associated with immutable segment data.
- A bad pruning bug silently loses query results.

**Memory/concurrency/testing implications:**

- Memory: pruning saves page-cache pressure and decoded vector allocations.
- Concurrency: immutable stats can be shared; stats publication must align with snapshot publication.
- Testing: use exhaustive small-domain tests comparing pruned execution with full scan; inject null/missing-property cases.

**How future agents should apply it:** build segment metadata before building a scan operator, not as an afterthought.

## 7. Parquet Page-Level Pruning as the Model for Property-Page Skipping

**Where found:** Apache DataFusion Parquet datasource.

**Language/framework:** Rust, Parquet page index pruning.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/page_filter.rs`, especially lines 112-171 and 414-570.

**Pattern name:** Convert per-page prune results into row selections.

DataFusion builds a `PagePruningAccessPlanFilter` from physical expressions. It splits conjunctions, ignores predicates it cannot safely page-prune, evaluates page statistics through `PruningPredicate`, and converts the resulting per-page booleans into a `RowSelection` of selected and skipped ranges.

**Why it matters for Neo4j-in-Rust:** property pages and adjacency pages should have row/edge ranges. A graph scan should turn page-level decisions into "skip N rows/edges, read M rows/edges" instructions rather than allocating one boolean per entity in the hot path.

**Use when:**

- pages have offset indexes or row ranges;
- predicates can be evaluated against page stats;
- storage can efficiently skip ranges.

**Do not use when:**

- pages are too small and metadata overhead dominates;
- range selection would fragment IO more than it saves.

**Rust translation:**

```rust
enum GraphRowSelection {
    Select { start: u64, len: u64 },
    Skip { start: u64, len: u64 },
}
```

For graph data, replace "row" with:

- node id ranges in a node property segment;
- relationship ordinal ranges in an adjacency segment;
- posting-list ranges in a label or index segment.

**Risks:** too-fine page pruning can turn sequential IO into random IO. Add a coalescing policy.

**Memory/concurrency/testing implications:**

- Memory: avoid decoding skipped pages entirely.
- Concurrency: row selections are immutable plan inputs.
- Testing: compare row selections against full scan for random predicates and page boundaries.

**How future agents should apply it:** every property-store page format should include the metadata needed for this pattern.

## 8. RAII Memory Reservations and Spill-Aware Memory Pools

**Where found:** Apache DataFusion.

**Language/framework:** Rust, execution memory accounting.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/execution/src/memory_pool/mod.rs`, especially lines 41-83, 113-130, 176-226, 248-265.

**Pattern name:** Reserve before allocating; release on drop.

DataFusion's memory pool tracks large consumers across operators. The comments are honest: it is not a general-purpose allocator and does not track every allocation. Instead, operators that may consume substantial memory reserve capacity before allocating. If reservation fails, they spill, free, or error. `MemoryConsumer`, `SharedRegistration`, and `MemoryReservation` encode that relationship.

**Why it matters for Neo4j-in-Rust:** graph workloads can explode memory during variable-length path expansion, hash joins, distinct, aggregation, sorting, and index intersection. A lower-RAM rewrite needs memory accounting at those operator boundaries.

**Use when:**

- an operator may buffer frontier/path/join state;
- an index builder may batch postings;
- a storage layer may decompress large pages;
- a transaction may hold write sets.

**Do not use when:**

- attempting to track every tiny allocation;
- the code cannot recover by spilling or reducing batch size.

**Rust translation:**

```rust
struct GraphMemoryReservation {
    pool: Arc<dyn GraphMemoryPool>,
    bytes: usize,
}

impl Drop for GraphMemoryReservation {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}
```

Use named consumers such as `ExpandFrontier`, `HashNodeJoin`, `PropertySort`, `VectorKnnScratch`, and `PageDecode`.

**Risks:**

- Reservations can drift from actual allocations.
- Missing a single large allocation path undermines the limit.
- Spill paths must be tested as first-class behavior.

**Memory/concurrency/testing implications:**

- Memory: central mechanism for RAM caps.
- Concurrency: pool must handle concurrent plans; fairness policy matters.
- Testing: force tiny limits to exercise spill/error paths; assert reservations return to zero after stream drop and errors.

**How future agents should apply it:** do not write a path-expansion or join operator without a named memory consumer.

## 9. Cooperative Scheduling Around Long-Running Streams

**Where found:** Apache DataFusion.

**Language/framework:** Rust, Tokio cooperative scheduling.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/coop.rs`, especially lines 20-68, 101-160, 213-299, 365-386.

**Pattern name:** Wrap streams so they yield to the async runtime.

DataFusion identifies that long-running streams can starve other tasks. `CooperativeStream` consumes Tokio cooperative budget, and `CooperativeExec` wraps a child execution plan to make returned streams cooperative.

**Why it matters for Neo4j-in-Rust:** graph traversal can monopolize CPU: expand millions of relationships, evaluate predicates, deduplicate paths, repeat. If the engine runs in an async server, it must yield fairly.

**Use when:**

- an operator can run for many batches;
- traversal or index intersection is CPU-heavy;
- multiple queries share one runtime.

**Do not use when:**

- the code is synchronous and isolated;
- yield points break transaction locks or page pins in unsafe ways.

**Rust translation:** create a `CooperativeGraphStream` wrapper and an optimizer rule that inserts it above expensive operators: expand, var-length expand, hash join, sort, aggregate, vector search.

**Risks:** yield points inside critical sections can create latency cliffs or deadlocks. Keep locks out of await/yield regions.

**Memory/concurrency/testing implications:** test two concurrent heavy traversals and assert neither starves. Add tracing spans around yield-heavy operators.

**How future agents should apply it:** build fairness into the execution engine early; retrofitting yield points into recursive traversal code is painful.

## 10. Metrics Objects with Stable Labels and Pruning Counters

**Where found:** Apache DataFusion Parquet datasource.

**Language/framework:** Rust, execution metrics.

**Source paths:**

- `gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/metrics.rs`, especially lines 32-96 and 98-220.

**Pattern name:** Per-scan metrics object with shared labels and explicit pruning counters.

`ParquetFileMetrics` tracks files/ranges pruned, row groups pruned by bloom/statistics/limit, bytes scanned, pushdown rows pruned/matched, page index timing, metadata timing, scan efficiency ratio, and predicate cache metrics. The implementation shares filename labels as `Arc<str>` to avoid repeated string allocation.

**Why it matters for Neo4j-in-Rust:** lower RAM claims are not believable without counters. A graph scan should report:

- pages skipped by label stats;
- relationship segments skipped by type;
- property pages skipped by min/max/bloom;
- decoded bytes;
- rows/entities emitted;
- frontier rows spilled;
- page-cache hits/misses;
- scan efficiency ratio.

**Use when:**

- building any storage-backed operator;
- debugging query plans;
- proving RAM reductions.

**Do not use when:**

- metrics allocation appears in the per-row hot path;
- labels include unbounded query strings or property values.

**Rust translation:** allocate metrics once per operator instance, use stable labels, and update atomic or local counters in batches.

**Risks:** high-cardinality labels can make observability itself a memory leak.

**Memory/concurrency/testing implications:** test that metric label creation is outside the hot loop; use benchmarks with metrics enabled.

**How future agents should apply it:** every pattern in this document needs a counter or benchmark that proves it is active.

## 11. Checksum-Terminated WAL Batches with Persist Modes

**Where found:** fjall.

**Language/framework:** Rust, LSM-oriented key-value storage.

**Source paths:**

- `gitrefrepo/fjall-src/src/journal/entry.rs`, especially lines 13-22, 23-39, 55-83, 124-157, 159-243.
- `gitrefrepo/fjall-src/src/journal/writer.rs`, especially lines 18-50, 66-150, 258-297, 326-378.

**Pattern name:** Start/items/end journal batch with checksum and trailer magic.

fjall's journal batch is encoded as `Start`, N item records, and `End`. The start record carries item count. The end record carries checksum and trailer magic. A start inside an existing batch is treated as broken. The writer supports persist modes: buffer only, `sync_data`, and `sync_all`. Journal creation uses `create_new`, preallocation, and directory fsync on rotation.

Sketch:

```text
Start { item_count }
Item { keyspace, key, value/tombstone }
Item { ... }
End { checksum, magic }
```

**Why it matters for Neo4j-in-Rust:** graph transactions often update multiple structures: node property page, adjacency page, label index, relationship type index, and counts. A transaction journal should make partial-batch detection trivial.

**Use when:**

- one logical transaction maps to multiple physical writes;
- recovery must distinguish complete from partial batches;
- durability mode is configurable.

**Do not use when:**

- every operation is idempotent and independently replayable;
- storage requires external consensus/raft log as the sole journal.

**Rust translation:**

- Encode graph transaction WAL as `BeginTxn`, repeated physical mutations, `CommitTxn { checksum, magic }`.
- Hash the encoded item bytes, not reconstructed structs.
- Include logical ids: transaction id, keyspace/column-family id, segment id, page id.
- Provide persist modes for tests and deployments: buffered, data sync, full sync.

**Risks:**

- Checksumming decoded structs instead of encoded bytes can miss serialization bugs.
- Directory fsync omissions can make rotated logs disappear after crash.
- Compression inside WAL complicates recovery; keep compression thresholds explicit.

**Memory/concurrency/testing implications:**

- Memory: batch serialization should stream into a fixed buffer.
- Concurrency: writers need a single serialization point or sequence-number assignment.
- Testing: crash at every byte boundary; assert recovery truncates partial transactions and replays complete ones exactly once.

**How future agents should apply it:** use the fjall pattern for local graph transaction logs before inventing a more elaborate log format.

## 12. Journal Recovery by Truncating Partial Batches

**Where found:** fjall.

**Language/framework:** Rust, journal reader/recovery.

**Source paths:**

- `gitrefrepo/fjall-src/src/journal/batch_reader.rs`, especially lines 25-35, 51-70, 93-145, 146-212.

**Pattern name:** Reader state machine with last-valid-position truncation.

`JournalBatchReader` tracks whether it is inside a batch, item count, sequence number, last valid position, and checksum builder. When a batch closes while incomplete, it truncates the file to `last_valid_pos` and syncs. It validates nested starts, item count, checksum, and trailer magic.

**Why it matters for Neo4j-in-Rust:** crash recovery should not require clever reasoning. A small state machine with a last-known-good offset is easier to audit and fuzz.

**Use when:**

- WAL is append-only;
- partial tail records are expected after crash;
- truncation is safe after validation.

**Do not use when:**

- log segments are immutable and repaired by manifest switching;
- truncation can race with another writer.

**Rust translation:** implement graph WAL recovery as an iterator over validated batches. It should return either a committed transaction or truncate/stop at the first invalid tail.

**Risks:** truncating too aggressively can discard a valid transaction if checksum or count logic is wrong.

**Memory/concurrency/testing implications:** recovery should allocate only a small vector for one transaction's operations. Crash tests should include nested begin markers, wrong counts, bad checksum, and missing end marker.

**How future agents should apply it:** define recovery states before defining transaction APIs.

## 13. Sealed-Journal Recovery with Per-Keyspace Watermarks

**Where found:** fjall.

**Language/framework:** Rust, LSM recovery.

**Source paths:**

- `gitrefrepo/fjall-src/src/db.rs`, especially lines 570-590, 599-644, 667-710.
- `gitrefrepo/fjall-src/src/recovery.rs`, especially lines 120-168, 170-206, 212-260.

**Pattern name:** Replay only what has not already reached durable lower layers.

fjall recovery checks version and lock state, recovers journals, initializes meta tree and supervisor components, recovers keyspaces and sealed memtables, and only then replays active journals. The recovery code builds watermarks per keyspace, applies value/tombstone/weak tombstone operations, handles cleared keyspaces, and skips sealed memtables whose persisted sequence number is already high enough.

**Why it matters for Neo4j-in-Rust:** graph stores will likely have multiple durable structures. Replaying every journal into every structure wastes IO and can duplicate work. Per-structure watermarks make recovery proportional to missing data.

**Use when:**

- there are multiple keyspaces/column families: topology, properties, labels, indexes;
- memtables or segments can be sealed and later flushed;
- sequence numbers are globally ordered.

**Do not use when:**

- the system lacks reliable per-structure persisted sequence numbers.

**Rust translation:**

- Maintain watermarks for topology store, property store, label index, relationship index, and count store.
- During recovery, replay only operations newer than the persisted watermark for that structure.
- Model clears/drop-label/drop-index operations explicitly.

**Risks:** a wrong watermark is a data-loss bug. Watermarks must be persisted atomically with flushed data.

**Memory/concurrency/testing implications:** recovery should not load every segment; it should stream journals and update bounded memtables. Add crash tests around "flushed data but not watermark" and "watermark but not flushed data".

**How future agents should apply it:** design sequence-number and watermark persistence before implementing compaction.

## 14. Packed Page Identifiers and Region-Aware Page Allocation

**Where found:** redb.

**Language/framework:** Rust, embedded B-tree storage.

**Source paths:**

- `gitrefrepo/redb-src/src/tree_store/page_store/base.rs`, especially lines 23-39, 91-109, 143-162, 175-191, 200-250.
- `gitrefrepo/redb-src/src/tree_store/page_store/layout.rs`, especially lines 11-19, 31-46, 74-80, 114-200, 222-240.

**Pattern name:** Encode page address, region, and order in a compact page number.

redb's `PageNumber` packs page index, region, reserved bits, and order into on-disk bytes. The comments describe a 4 PiB-ish address range with 4 KiB pages. Layout code separates allocator header from page area and calculates full regions, trailing regions, usable bytes, and region base.

**Why it matters for Neo4j-in-Rust:** a graph DB needs durable references to adjacency pages, property pages, and index pages. A compact page id with embedded order/region can reduce metadata RAM and simplify allocator operations.

**Use when:**

- storage is page-oriented;
- pages can have multiple orders/sizes;
- region-local allocation matters for shrinking or locality.

**Do not use when:**

- logical ids need to stay stable across compaction without indirection;
- data is primarily log-structured segments rather than mutable pages.

**Rust translation:**

```rust
struct GraphPageId(u64);

// fields: page_index, region_id, order, flags/reserved
```

Keep domain ids (`NodeId`, `RelationshipId`) separate from physical page ids. Store physical ids only in indexes/manifests that can be updated by compaction.

**Risks:** packed formats are hard to change. Reserve bits intentionally and write versioned decoders.

**Memory/concurrency/testing implications:** compact page ids reduce pointer-heavy metadata. Test round-trip encoding, address range calculation, and layout recalculation for many file lengths.

**How future agents should apply it:** use packed physical ids for page stores, but keep graph entity ids logical.

## 15. Dual Commit Slots with an Atomic Selector Byte

**Where found:** redb.

**Language/framework:** Rust, embedded database header and commit protocol.

**Source paths:**

- `gitrefrepo/redb-src/src/tree_store/page_store/header.rs`, especially lines 10-41, 57-60, 87-148, 154-192, 195-230.
- `gitrefrepo/redb-src/src/tree_store/page_store/page_manager.rs`, especially lines 767-865.

**Pattern name:** Two root slots plus a selector byte for crash-safe commits.

redb's database header has a super-header plus two commit slots containing roots, checksums, transaction id, region data, and slot checksum. A "god byte" records primary slot, recovery-required, and two-phase commit flags. On finalize, the code reconciles file length/layout and selects the valid primary slot. Commit writes secondary state, optionally performs a two-phase flush, swaps primary, writes header, flushes, and clears unpersisted state. Non-durable commit writes a secondary slot and reads from secondary.

**Why it matters for Neo4j-in-Rust:** a graph store can publish a new consistent root set for topology, property, label, and index manifests without rewriting the whole database. Two slots reduce crash recovery complexity.

**Use when:**

- database state can be summarized by root pointers/manifests;
- commit metadata is small;
- local durability is required without external consensus.

**Do not use when:**

- there are many independently committed shards with separate logs;
- distributed consensus determines the committed root.

**Rust translation:**

```text
Header:
  magic/version
  primary_slot: 0 or 1
  flags
Slot[0]:
  txn_id
  topology_root
  property_root
  label_index_root
  rel_index_root
  allocator_root
  checksum
Slot[1]: same
```

**Risks:**

- Commit protocol bugs are catastrophic and may pass ordinary tests.
- Filesystem flush semantics differ by platform.
- Recovery-required flags need clear operator tooling.

**Memory/concurrency/testing implications:** the in-memory manifest can be small and atomically swapped. Test crash at every commit step with both durable and non-durable modes.

**How future agents should apply it:** if building a page-store graph engine, use dual slots for root publication and WAL for transaction replay.

## 16. Scoped Page Ownership and Cache Eviction Safety

**Where found:** redb.

**Language/framework:** Rust, page cache and B-tree storage.

**Source paths:**

- `gitrefrepo/redb-src/src/tree_store/btree_base.rs`, especially lines 20-99 and 145-170.
- `gitrefrepo/redb-src/src/tree_store/page_store/cached_file.rs`, especially lines 18-40, 57-116, 118-198, 200-239.

**Pattern name:** Page access guards and caches that know when a page is borrowed.

redb's B-tree base validates checksums over valid page ranges and exposes an `AccessGuard` that can hold immutable, mutable, owned, or `Arc` memory with offset and length. The cached file layer has `WritablePage` values that return to the write cache on drop. Its LRU write cache stores values as `Option<Arc<[u8]>>`, so borrowed pages cannot be evicted. The checked backend records global IO failure/closed flags so future operations fail consistently.

**Why it matters for Neo4j-in-Rust:** page caches become unsound if a page can be evicted while an operator still holds a slice into it. Rust should encode page pin lifetimes or owned bytes.

**Use when:**

- operators borrow page bytes directly;
- cache eviction and IO writes run concurrently;
- corruption checks happen at page boundary.

**Do not use when:**

- every page read is copied into owned memory;
- mmap lifetime is already process-wide and eviction is OS-managed.

**Rust translation:**

- Return `PageGuard` from page cache reads.
- Expose typed views only through the guard.
- Make eviction require absence of active guards.
- Store global IO error state and poison the database after unrecoverable IO failure.

**Risks:** guards can be held across await points and stall eviction. Add linting/review rules.

**Memory/concurrency/testing implications:** page pins bound memory. Test eviction under active read guards and write guards. Test IO failure poisoning.

**How future agents should apply it:** no borrowed page slice should exist without an owner/guard visible in the type.

## 17. Buddy Allocation for Variable-Size Pages

**Where found:** redb.

**Language/framework:** Rust, page allocator.

**Source paths:**

- `gitrefrepo/redb-src/src/tree_store/page_store/buddy_allocator.rs`, especially lines 27-34, 37-74, 85-144, 147-192, 220-250.
- `gitrefrepo/redb-src/src/tree_store/page_store/page_manager.rs`, especially lines 70-82, 990-1026, 1043-1108.

**Pattern name:** Bitmap-backed buddy allocator with shrink-aware policy.

redb's allocator marks free pages at the largest order possible. It serializes allocator state as max order, page counts, free-end offsets, and bitmaps. It can report trailing free pages for file shrinking. Page manager policies include a `Lowest` allocation policy to keep trailing pages free for shrink.

**Why it matters for Neo4j-in-Rust:** adjacency lists are not uniform. Dense nodes may need larger pages, while sparse nodes fit small pages. A buddy allocator can reduce fragmentation while keeping page ids compact.

**Use when:**

- page sizes vary by adjacency/property/index block;
- file shrinking matters;
- allocator metadata must be persisted compactly.

**Do not use when:**

- append-only immutable segments with compaction are simpler;
- allocation churn is too high and LSM would fit better.

**Rust translation:** pair a small fixed page size with larger orders for dense adjacency blocks, large property dictionaries, or vector chunks.

**Risks:** buddy allocators can fragment under adversarial update patterns. Measure dense-node churn.

**Memory/concurrency/testing implications:** allocator bitmaps should be memory-mapped or compactly loaded. Add model tests against a simple reference allocator.

**How future agents should apply it:** use buddy pages for mutable page stores; use LSM/segment files for append-heavy indexes.

## 18. Prefix-Compressed Leaves with Tunable Fanout

**Where found:** sled.

**Language/framework:** Rust, concurrent embedded key-value store.

**Source paths:**

- `gitrefrepo/sled-src/src/config.rs`, especially lines 70-106, 135-153.
- `gitrefrepo/sled-src/src/db.rs`, especially lines 9-21, 32-42, 80-150, 236-258.
- `gitrefrepo/sled-src/src/leaf.rs`, especially lines 3-19, 43-83, 85-136, 150-181, 184-240.

**Pattern name:** Configurable leaf fanout with prefix stripping and compressed serialization.

sled exposes cache capacity, entry-cache percent, periodic flush interval, zstd level, heap fill ratio, and inline-value threshold. `LEAF_FANOUT` is described as the one major performance/efficiency knob: lower values can help random larger-than-memory workloads and reduce contention. Leaves store low/high keys, prefix length, a stack map, in-memory size, mutation count, dirty/pageout/deleted epochs, and max unflushed epoch. They strip prefixes from keys, merge by recomputing common prefix, serialize with zstd and bincode, and split with attention to edge workloads.

**Why it matters for Neo4j-in-Rust:** graph keys often share prefixes:

- `(label_id, node_id)`;
- `(relationship_type, source_node_id, ordinal)`;
- `(property_key, value, node_id)`;
- `(source_node_id, relationship_type, target_node_id)`.

Prefix compression reduces RAM and disk bytes for leaves and posting lists.

**Use when:**

- keys are sorted and prefix-heavy;
- leaves are immutable enough to serialize/compress;
- fanout can be tuned per workload.

**Do not use when:**

- keys are random hashes with poor prefix sharing;
- compression CPU is unacceptable.

**Rust translation:**

- Encode index keys with high-cardinality suffixes last.
- Use prefix-compressed leaf pages for label and property indexes.
- Expose fanout as a storage-profile knob, not a hidden constant.

**Risks:** bad key layout ruins prefix compression. Test with realistic graph id distributions, not random strings only.

**Memory/concurrency/testing implications:** in-memory size should be exact or conservatively estimated. Test split/merge under monotonic inserts and random inserts.

**How future agents should apply it:** design key encodings and leaf formats together.

## 19. Flush Epochs and Cooperative Serialization

**Where found:** sled.

**Language/framework:** Rust, object cache and background flushing.

**Source paths:**

- `gitrefrepo/sled-src/src/object_cache.rs`, especially lines 16-42, 93-111, 141-181, 192-212.
- `gitrefrepo/sled-src/src/tree.rs`, especially lines 67-103, 106-155, 182-197.

**Pattern name:** Dirty objects move through explicit flush states and epochs.

sled tracks cache hits/misses, latency, heap, flush phases, compacted slots, and leaves merged. Dirty state includes `NotYetSerialized`, `CooperativelySerialized`, and `MergedAndDeleted`. Leaf guards release locks before marking access and eviction. Flush APIs promise fsync behavior and warn about performance.

**Why it matters for Neo4j-in-Rust:** a graph store needs background flushing without retaining the whole mutable graph in memory. Epochs let readers/writers coordinate with flushers while keeping dirty object sets bounded.

**Use when:**

- mutable pages/objects are cached;
- background flushes must not block all readers;
- dirty state must be observable.

**Do not use when:**

- immutable segment writes and manifest swaps are sufficient;
- the system cannot tolerate background flush complexity.

**Rust translation:** create flush epochs for mutable topology pages, property pages, and index leaves. Move dirty pages through serialized/merged/flushed states.

**Risks:** flush state machines are bug-prone. Background panics must poison the database or surface clearly.

**Memory/concurrency/testing implications:** force small cache capacity to test eviction and flush. Track dirty bytes by epoch. Use failpoints around serialization and write batch.

**How future agents should apply it:** give every dirty object an epoch and final state; do not rely on vague "dirty bool" semantics for crash-safe storage.

## 20. Write Buffer Managers and Explicit Write Stalls

**Where found:** RocksDB.

**Language/framework:** C++, LSM storage engine.

**Source paths:**

- `gitrefrepo/rocksdb-src/memtable/write_buffer_manager.cc`, especially lines 20-39, 56-100, 118-150.

**Pattern name:** Global mutable-memory budget with active memory and stall queue.

RocksDB's `WriteBufferManager` tracks total buffer size, a mutable limit, memory used, active memory, and optional cache reservation integration. It reserves memory, schedules/free memory, and begins/ends write stalls through a queue when limits are crossed.

**Why it matters for Neo4j-in-Rust:** write-heavy graph imports can fill memtables, adjacency deltas, property deltas, and index updates faster than flush/compaction can drain them. Stalling writers is better than unbounded RAM growth.

**Use when:**

- writes first land in memory before flush;
- multiple column families/keyspaces share memory;
- page cache and write buffers compete.

**Do not use when:**

- all writes are synchronously applied to fixed pages;
- import mode can use explicit external batching.

**Rust translation:** one `GraphWriteBufferManager` should reserve memory for topology delta memtables, property delta memtables, label index memtables, and full-text/vector index staging.

**Risks:** stalls can look like deadlocks if not observable. Provide metrics and logs.

**Memory/concurrency/testing implications:** test sustained writes with tiny memory limits; assert stalls start/end and memory returns after flush.

**How future agents should apply it:** put the mutable graph write path behind a shared memory budget before implementing import throughput optimizations.

## 21. Fragmented WAL Records over Fixed-Size Log Blocks

**Where found:** RocksDB.

**Language/framework:** C++, WAL/log format.

**Source paths:**

- `gitrefrepo/rocksdb-src/db/log_format.h`, especially lines 22-61.
- `gitrefrepo/rocksdb-src/db/log_writer.cc`, especially lines 23-41, 54-65, 87-189.

**Pattern name:** Logical records split across fixed-size physical blocks.

RocksDB log records include full, first, middle, last, recyclable, compression, user-defined timestamp, and WAL verification record types. The block size is 32 KiB. Headers include checksum, length, and type. `AddRecord` fragments a logical record across blocks, fills trailers, optionally compresses, emits record fragments, flushes depending on mode, and tracks last sequence number.

**Why it matters for Neo4j-in-Rust:** graph transactions can be larger than one log block, especially imports or dense-node updates. Fixed physical blocks simplify preallocation and recovery while logical fragmentation keeps large transactions possible.

**Use when:**

- WAL records vary in size;
- IO wants block alignment;
- recovery should resynchronize by record headers.

**Do not use when:**

- transactions are always tiny and fjall-style whole-batch records are simpler.

**Rust translation:** if graph WAL batches may exceed page/log-block size, support `First/Middle/Last` fragments under a transaction checksum.

**Risks:** fragmented logs add recovery complexity. Start with batch records unless large transactions force fragmentation.

**Memory/concurrency/testing implications:** writer should not require a contiguous allocation for huge transactions. Test fragmentation at every boundary: exactly block size, one byte before, one byte after.

**How future agents should apply it:** choose between fjall-style batches and RocksDB-style fragmented records based on max graph transaction size.

## 22. Flush and Compaction as Staged Jobs

**Where found:** RocksDB.

**Language/framework:** C++, LSM storage engine.

**Source paths:**

- `gitrefrepo/rocksdb-src/db/flush_job.h`, especially lines 57-91, 138-158, 211-220.
- `gitrefrepo/rocksdb-src/db/compaction/compaction_job.h`, especially lines 63-105, 178-202.

**Pattern name:** Flush and compaction are explicit job lifecycles, not helper functions.

RocksDB's flush job has a pick/run/cancel shape and includes directory sync requirements before manifest updates. The header also documents memtable purge for filtering outdated bytes from immutable memtables before writing to SSD. Compaction jobs have prepare, run, and install phases, aggregate subcompaction stats, set boundaries, resume progress, run subcompactions in parallel, and install the result.

**Why it matters for Neo4j-in-Rust:** graph storage will need background jobs: flush topology deltas, compact property segments, rebuild label indexes, merge relationship adjacency segments, purge deleted nodes, and update counts. These should be modeled as observable jobs with phases.

**Use when:**

- background work changes durable state;
- a manifest install step publishes new files/pages;
- job stats are needed for tuning.

**Do not use when:**

- the operation is a tiny synchronous write.

**Rust translation:** define `FlushJob`, `CompactionJob`, `IndexBuildJob`, and `CheckpointJob` with phases: prepare, run, validate, install, cleanup.

**Risks:** background job cancellation can leave orphaned files. Install must be atomic.

**Memory/concurrency/testing implications:** each job must reserve memory, expose stats, and support failpoint crash tests at phase boundaries.

**How future agents should apply it:** never hide durable state transitions inside an ad hoc background task.

## 23. Lifetime-Safe FFI Iterators and Pinned Engine-Owned Slices

**Where found:** rust-rocksdb.

**Language/framework:** Rust wrapper over RocksDB C++.

**Source paths:**

- `gitrefrepo/rust-rocksdb-src/src/db_iterator.rs`, especially lines 76-90, 120-155, 581-657.
- `gitrefrepo/rust-rocksdb-src/tests/test_pinnable_slice.rs`, especially lines 22-64.

**Pattern name:** Rust wrapper owns the options that C++ iterator borrows.

`DBRawIterator` stores the inner RocksDB iterator pointer and the full `ReadOptions` because the C++ iterator keeps pointers to bounds inside options/vectors. The Rust wrapper ties lifetime to the DB via `PhantomData`. WAL iteration yields sequence numbers and write batches. `get_pinned` tests show pinned slices can be read without copying, and a snapshot pinned slice remains the old value after a later put.

**Why it matters for Neo4j-in-Rust:** if the graph store wraps mmap, C, C++, or custom page-cache APIs, Rust must own any buffers/options that the lower layer borrows. Pinned slices are valuable for zero-copy property reads and index values.

**Use when:**

- exposing zero-copy values from storage;
- wrapping external engines;
- snapshots must keep old bytes alive.

**Do not use when:**

- the lower layer cannot guarantee pin lifetime;
- users may hold pinned values across long async waits without accounting.

**Rust translation:** a `PinnedGraphValue<'db>` should carry the owner guard/snapshot/options necessary to keep bytes alive.

**Risks:** FFI lifetime mistakes often compile and fail later. Keep wrappers small and heavily tested.

**Memory/concurrency/testing implications:** pinned slices reduce copying but can retain cache memory. Test snapshot isolation and drop order.

**How future agents should apply it:** any zero-copy storage API must expose ownership in the return type, not in documentation only.

## 24. Engine Abstraction Around RocksDB-Like Storage

**Where found:** TiKV.

**Language/framework:** Rust, RocksDB-backed distributed KV engine.

**Source paths:**

- `gitrefrepo/tikv-src/components/engine_rocks/src/engine.rs`, especially lines 147-155, 187-196, 209-270.

**Pattern name:** Wrap storage engine behind explicit traits and snapshots.

TiKV's `RocksEngine` wraps `Arc<DB>`, tracks multi-batch-write support and ingest latches, exposes snapshots, syncs WAL, creates iterators per column family with read options, peeks values, and exposes mutable operations such as put/delete/range delete.

**Why it matters for Neo4j-in-Rust:** a graph rewrite should isolate graph logic from its first storage backend. It may start with fjall/redb/RocksDB-like storage and later replace it. The engine boundary should expose graph needs without leaking one backend's API everywhere.

**Use when:**

- storage backends may change;
- tests need an in-memory or fault-injecting engine;
- graph layers need snapshots and iterators.

**Do not use when:**

- over-abstracting hides critical storage semantics such as snapshot pinning or range tombstones.

**Rust translation:**

```rust
trait GraphStorageEngine {
    type Snapshot: GraphSnapshot;
    type Iterator: GraphIterator;

    fn snapshot(&self) -> Result<Self::Snapshot>;
    fn sync_wal(&self) -> Result<()>;
    fn put(&self, cf: GraphColumn, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete_range(&self, cf: GraphColumn, from: &[u8], to: &[u8]) -> Result<()>;
}
```

**Risks:** trait design can erase useful batch and range APIs. Keep traits close to actual graph access patterns.

**Memory/concurrency/testing implications:** snapshots pin memory/files; make snapshot lifetimes visible. Test each backend against the same conformance suite.

**How future agents should apply it:** define a storage trait only after listing graph-specific access patterns: adjacency lookup, property scan, label index scan, transaction replay.

## 25. Priority Read Pools and Admission-Controlled Scheduling

**Where found:** TiKV.

**Language/framework:** Rust, async read pools.

**Source paths:**

- `gitrefrepo/tikv-src/src/read_pool.rs`, especially lines 191-317.

**Pattern name:** Route tasks by priority and account for resource control.

TiKV routes futures into high, normal, and low priority pools. In the Yatp path, levels are mapped explicitly, resource control can admit/enqueue tasks, gauges are incremented/decremented around running tasks, and `spawn_handle` wraps a future with a oneshot result.

**Why it matters for Neo4j-in-Rust:** graph DBs mix latency-sensitive point lookups with heavy analytical traversals. Without priority scheduling, an all-graph scan can starve OLTP reads.

**Use when:**

- serving concurrent transactional and analytical graph queries;
- background compaction/checkpoint jobs share runtime with reads;
- resource groups exist.

**Do not use when:**

- single-user embedded mode does not need scheduling overhead.

**Rust translation:** add query priorities: transactional lookup, short traversal, analytical traversal, background maintenance. Route them through separate queues or executor lanes.

**Risks:** priority inversion and starvation. Low-priority analytical work still needs progress.

**Memory/concurrency/testing implications:** combine priority with memory pools. Test high-priority lookup latency during low-priority scan.

**How future agents should apply it:** scheduling is part of the query engine, not an operations-only concern.

## 26. Memory-Lock Conflict Checks as a Graph Transaction Pattern

**Where found:** TiKV.

**Language/framework:** Rust, transactional concurrency checks.

**Source paths:**

- `gitrefrepo/tikv-src/src/coprocessor/endpoint.rs`, especially lines 996-1031.

**Pattern name:** Check in-memory locks over ranges before serving a snapshot-sensitive read.

TiKV's endpoint checks memory locks for ranges, updates max timestamp unless stale, scans the concurrency manager for each range, checks timestamp conflicts, and records locked/unlocked histograms.

**Why it matters for Neo4j-in-Rust:** graph queries often scan ranges: all relationships for node, all nodes with label, all properties in a key range. Snapshot reads should know whether in-memory uncommitted writes conflict with the read timestamp.

**Use when:**

- MVCC or timestamped transactions are used;
- in-memory locks are not yet reflected in durable index pages;
- read queries span ranges.

**Do not use when:**

- strict single-writer transactions make concurrent conflicts impossible;
- all reads are serialized behind writes.

**Rust translation:** before a range scan, ask a concurrency manager for locks overlapping node id ranges, relationship key ranges, or property index ranges.

**Risks:** lock checks can become a scalability bottleneck if every small range does a global scan.

**Memory/concurrency/testing implications:** lock tables need memory bounds and observability. Test stale reads, conflicting writes, and no-conflict fast paths.

**How future agents should apply it:** if graph queries use MVCC, build lock-range checks into scan planning.

## 27. Applied Storage Blueprint for Neo4j-in-Rust

This is the concrete architecture implied by the verified patterns above.

### Storage Planes

**Topology plane:** stores node records, relationship records, and adjacency pages. Use packed physical page ids for mutable page storage or append-only segment ids for immutable storage. Dense nodes should spill to larger adjacency blocks or segmented adjacency lists.

**Property plane:** stores property values in columnar pages. Use Arrow-like buffers and typed scalar buffers for fixed-width values. Use offset buffers for strings, arrays, and variable-size values. Store min/max/null/missing counts per page.

**Index plane:** stores label indexes, relationship-type indexes, property indexes, and optionally full-text/vector index references. Use prefix-compressed leaves or LSM segments. Key layout should maximize prefix sharing.

**Transaction plane:** uses WAL batches with start/items/end, checksum, trailer magic, and persist modes. Keep sequence numbers and watermarks per storage plane.

**Manifest/root plane:** use dual commit slots if building a page store, or manifest file rotation if building immutable segments. Commit metadata should publish roots for topology, property, indexes, allocator, and watermarks.

**Execution plane:** query operators exchange columnar `GraphBatch` values. Operators are streaming, cooperative, memory-reserved, and metrics-bearing.

### Recommended Physical Encodings

Node id column:

```text
Buffer<u64 NodeId>
```

Adjacency page:

```text
header:
  source_node_id
  relationship_type_summary
  min_target_node_id
  max_target_node_id
  edge_count
buffers:
  target_node_ids: u64[]
  relationship_ids: u64[]
  type_ids: u32[] or dictionary ids
  property_ref_offsets: u32[]
```

Property page:

```text
header:
  property_key_id
  value_type
  row_count
  min/max stats
  missing_count
buffers:
  entity_ids: u64[]
  validity/missing bitmap
  fixed_values or offsets + bytes
```

WAL transaction:

```text
Begin { txn_id, item_count, first_seqno }
Mutation { plane, keyspace, logical_key, physical_delta }
Mutation { ... }
Commit { checksum_over_mutations, trailer_magic }
```

### RAM Reduction Checklist

- Prefer immutable shared buffers to per-row objects.
- Keep graph entities as ids in query batches, not heap objects.
- Store properties column-wise with page stats.
- Use row/page selections instead of boolean vectors when skipping pages.
- Reserve memory for expand/join/sort/aggregate/vector-search operators.
- Make page pins visible in types and metrics.
- Use prefix compression for sorted index keys.
- Bound write buffers globally and stall writers under pressure.
- Use lazy streams and cooperative scheduling for long traversals.
- Persist watermarks so recovery does not replay already-flushed data.

### When to Choose LSM vs B-Tree/Page Store

Use an LSM-like design when:

- ingest/write rate is high;
- updates can be append-only deltas;
- compaction can merge property/index segments;
- range scans can use segment metadata and bloom filters.

Use a B-tree/page-store design when:

- point lookup and small updates dominate;
- predictable page ownership matters;
- root swapping and page checksums are simpler than compaction;
- file size and page-level mutation control matter.

For a graph DB, a hybrid is plausible:

- topology adjacency pages: page store for hot mutable adjacency, segment store for cold compacted adjacency;
- property store: columnar immutable segments plus delta memtables;
- secondary indexes: LSM or prefix-compressed B-tree leaves;
- transaction log: checksum-framed WAL;
- query engine: DataFusion-like streaming plan.

## 28. Testing, Benchmark, and Observability Implications

### Crash Safety Tests

Use failpoints or process-kill tests at:

- after WAL begin;
- after each WAL mutation;
- after WAL commit checksum but before fsync;
- after memtable flush before watermark update;
- after watermark update before manifest/root slot update;
- during dual-slot commit before selector swap;
- during selector swap before fsync;
- during compaction install.

Expected invariant: recovery returns either the old committed graph or the new committed graph, never a mixed graph.

### Memory Tests

Run with tiny limits:

- tiny operator memory pool;
- tiny write buffer manager;
- tiny page cache;
- tiny flush threshold;
- dense-node adjacency stress;
- variable-length path expansion with dedup.

Expected invariant: memory reservations return to zero after success, error, cancellation, and stream drop.

### Query Correctness Tests

For pruning:

- generate random property pages and predicates;
- run full scan and pruned scan;
- assert identical entity ids;
- include missing property/null semantics explicitly.

For streaming:

- drop streams early;
- cancel tasks mid-batch;
- interleave high-priority point reads with low-priority scans.

For page ownership:

- hold page guards while forcing eviction;
- assert guarded page bytes remain valid;
- test no guard is held across await in critical code paths.

### Benchmark Harness Recommendations

The directly read sources point to what should be benchmarked even where benchmark files were not opened in this pass:

- buffer slicing versus copying;
- typed property scan over `u64`, `f64`, dictionary string, and boolean bitmaps;
- page pruning selectivity;
- adjacency expansion by degree distribution;
- dense-node update and flush;
- write stall behavior under import;
- recovery time by WAL size and watermark position;
- stream fairness under concurrent traversals.

Expose counters for:

- page-cache hit/miss/evict;
- bytes decoded;
- bytes skipped by pruning;
- WAL bytes written and fsync latency;
- flush/compaction job phase times;
- memory reserved by operator;
- spilled bytes;
- cooperative yields;
- high/normal/low priority queue times.

## Coverage and Gaps

### Skills and Graph Tools Attempted

Both requested skills were used as feasible:

- `codebase-memory-evidence-reader`: read the skill instructions and used the local `codebase-memory-mcp` CLI.
- `codegraphcontext-evidence-reader`: read the skill instructions and used the local `cgc` CLI.

Graph-tool evidence:

- Codebase Memory successfully indexed `gitrefrepo/fjall-src`.
  - Result recorded during the run: 176 files, 1646 nodes, 5681 edges.
  - Project name: `Users-amuldotexe-Desktop-personal-repos-lane-knight-bus-graph-walker-gitrefrepo-fjall-src`.
  - Querying journal/recovery terms helped navigate to `src/journal/*`, `src/recovery.rs`, `src/snapshot_tracker.rs`, and `src/write_buffer_manager.rs`.
  - Important fjall findings in this file were verified by direct source reads.
- CodeGraphContext indexing was attempted on `gitrefrepo/redb-src`.
  - It failed with `NoneType object has no attribute split`.
  - No redb claims rely on this failed graph output.
- CodeGraphContext indexing was attempted on `gitrefrepo/sled-src`.
  - It completed after reporting unresolved ambiguous call relationships.
  - Sled claims in this file rely on direct source reads, not graph output.

### Direct Source Reads Used for Claims

Directly source-read and used in this first version:

- `gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/immutable.rs`
- `gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/scalar.rs`
- `gitrefrepo/apache-arrow-rs-src/arrow-data/src/data.rs`
- `gitrefrepo/apache-arrow-rs-src/arrow-array/src/record_batch.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/memory.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/pruning/src/pruning_predicate.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/page_filter.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/execution/src/memory_pool/mod.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/coop.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/metrics.rs`
- `gitrefrepo/fjall-src/src/journal/entry.rs`
- `gitrefrepo/fjall-src/src/journal/writer.rs`
- `gitrefrepo/fjall-src/src/journal/batch_reader.rs`
- `gitrefrepo/fjall-src/src/db.rs`
- `gitrefrepo/fjall-src/src/recovery.rs`
- `gitrefrepo/redb-src/src/tree_store/page_store/base.rs`
- `gitrefrepo/redb-src/src/tree_store/page_store/page_manager.rs`
- `gitrefrepo/redb-src/src/tree_store/page_store/buddy_allocator.rs`
- `gitrefrepo/redb-src/src/tree_store/page_store/layout.rs`
- `gitrefrepo/redb-src/src/tree_store/page_store/header.rs`
- `gitrefrepo/redb-src/src/tree_store/btree_base.rs`
- `gitrefrepo/redb-src/src/tree_store/btree_mutator.rs`
- `gitrefrepo/redb-src/src/tree_store/page_store/cached_file.rs`
- `gitrefrepo/sled-src/src/lib.rs`
- `gitrefrepo/sled-src/src/config.rs`
- `gitrefrepo/sled-src/src/db.rs`
- `gitrefrepo/sled-src/src/tree.rs`
- `gitrefrepo/sled-src/src/leaf.rs`
- `gitrefrepo/sled-src/src/object_cache.rs`
- `gitrefrepo/rocksdb-src/memtable/write_buffer_manager.cc`
- `gitrefrepo/rocksdb-src/db/log_format.h`
- `gitrefrepo/rocksdb-src/db/log_writer.cc`
- `gitrefrepo/rocksdb-src/db/flush_job.h`
- `gitrefrepo/rocksdb-src/db/compaction/compaction_job.h`
- `gitrefrepo/rust-rocksdb-src/src/db_iterator.rs`
- `gitrefrepo/rust-rocksdb-src/tests/test_pinnable_slice.rs`
- `gitrefrepo/tikv-src/components/engine_rocks/src/engine.rs`
- `gitrefrepo/tikv-src/src/read_pool.rs`
- `gitrefrepo/tikv-src/src/coprocessor/endpoint.rs`

Adjacent corpus file consulted only for alignment of scope/shape:

- `graph-database-rewrite-references-202606/meta-graph-database-patterns-3.md`

I read its repository/heading summary but did not rely on it for unverified code claims in this file.

### Repositories Covered by Direct Evidence in This First Version

- `apache-arrow-rs-src`
- `apache-datafusion-src`
- `fjall-src`
- `redb-src`
- `sled-src`
- `rocksdb-src`
- `rust-rocksdb-src`
- `tikv-src`

### Repositories Listed in the Assignment but Not Directly Source-Read Before the Timebox Stop

These remain gaps for future agents. They should be inspected directly before adding repo-specific claims:

- `apache-arrow-ballista-src`
  - Follow-up focus: scheduler, distributed execution graph, shuffle, task state, executor memory.
- `apache-arrow-src`
  - Follow-up focus: C++ Arrow `Buffer`, `MemoryPool`, IO interfaces, IPC zero-copy boundaries.
- `apache-iggy-src`
  - Follow-up focus: append-only stream segments, message batch persistence, partition recovery, retention.
- `apache-parquet-format-src`
  - Follow-up focus: Thrift definitions for row groups, column chunks, pages, encodings, statistics.
- `clickhouse-src`
  - Follow-up focus: MergeTree parts, marks/granules, primary key pruning, query pipeline processors, memory tracker.
- `datafusion-comet-src`
  - Follow-up focus: Spark-native columnar execution, native operator boundaries, Parquet scan pushdown.
- `datafusion-python-src`
  - Follow-up focus: Python/Arrow FFI, pyarrow conversion, RecordBatch ownership across Python/Rust.
- `duckdb-src`
  - Follow-up focus: `DataChunk`, `Vector`, buffer manager, WAL, checkpoint manager, vectorized operators.
- `materialize-src`
  - Follow-up focus: persist, timely/differential arrangements, trace compaction, durable collection metadata.
- `qdrant-src`
  - Follow-up focus: mmap vector storage, segment lifecycle, HNSW, payload indexes, WAL and snapshots.
- `risingwave-src`
  - Follow-up focus: Hummock LSM state store, compactor scheduling, streaming executors, batch execution.
- `snap-src`
  - Follow-up focus: adjacency memory layout, graph serialization, compact node/edge tables.
- `tantivy-src`
  - Follow-up focus: mmap directory, segment readers/writers, postings, fast fields/columnar storage.

### Gaps Inside Repositories That Were Partially Read

- `apache-arrow-rs-src`: IPC, compute kernels, dictionary arrays, boolean bitmaps, and FFI were not inspected.
- `apache-datafusion-src`: optimizer rules, hash join, sort spill, aggregate spill, and scheduler internals were not inspected.
- `fjall-src`: lower-level LSM tree internals, compaction, bloom filter implementation, and snapshot tracker were not inspected directly.
- `redb-src`: full transaction API, savepoints, repair logic, and mmap backend variants need deeper reading.
- `sled-src`: full heap/recovery path, page cache internals, and crash test harness need deeper reading.
- `rocksdb-src`: MANIFEST/version set, table formats, bloom/ribbon filters, block cache, and snapshots need direct reads.
- `rust-rocksdb-src`: write batches, snapshots, column family APIs, and option lifetimes need direct reads.
- `tikv-src`: Raft log, MVCC reader/writer, Titan/blob storage, lock manager, and full coprocessor execution need direct reads.

### Codebase-Memory Tool Evidence: ClickHouse Focused Fallback

The full `gitrefrepo/clickhouse-src` codebase-memory run timed out twice:

- first after 120 seconds;
- then after 600 seconds;
- then after a final 1800-second single-repo retry.

The long retry log showed the tool was making real progress through the full
26,426-file repository, but it still did not finish. To preserve codebase-memory
evidence for the repo without pretending the full index succeeded, I ran a
focused fallback over the high-signal ClickHouse slices most relevant to a
Neo4j-in-Rust rewrite:

- `clickhouse-src/src/Storages`
- `clickhouse-src/src/Processors`
- `clickhouse-src/src/Interpreters`
- `clickhouse-src/src/Parsers`
- `clickhouse-src/src/Disks`
- `clickhouse-src/base`

The focused codebase-memory pass indexed all six slices successfully:

- status file:
  `graph-database-rewrite-references-202606/clickhouse-focused-codebase-memory-status.tsv`
- logs:
  `/tmp/codex-code-intel/codebase-memory/clickhouse-focused-20260707/logs`
- aggregate focused evidence: 70,100 nodes, 225,998 edges, 4,724 files.

Interpretation for future agents:

- Treat the full ClickHouse repo as too large for the current codebase-memory
  runtime budget.
- Treat the six focused slices as valid codebase-memory evidence for storage
  layout, query pipelines, interpreter/planner boundaries, parser surfaces,
  disk/IO abstractions, and common systems infrastructure.
- Do not claim full ClickHouse graph coverage unless a later run completes the
  full repository index.

### Next-Agent Recommendations

1. Start with the uninspected repos that are most relevant to the missing themes:
   - DuckDB for vectorized execution and buffer/checkpoint design.
   - ClickHouse for granule pruning, sparse primary indexes, query pipelines, and memory tracking.
   - Qdrant for vector/mmap payload index design.
   - Tantivy for mmap segment search, postings, and fast fields.
   - Materialize/RisingWave for streaming state, persist, and compaction.
2. Keep using graph tools only for navigation. Open source files before adding claims.
3. Add a second section specifically for binary formats after reading `apache-parquet-format-src`, DuckDB storage headers, ClickHouse part formats, and Qdrant segment files.
4. Add benchmark tables only after opening benchmark harness source paths.
5. Preserve the distinction between logical graph ids and physical storage/page ids throughout the rewrite design.

# Supplemental Gap Closure Batch 05: Dgraph Posting Lists, DQL, And Raft WAL

Date: 2026-07-07

Purpose: close the primary `gitrefrepo/dgraph-src` direct-source gap in the
Desktop-wide graph-database rewrite pattern corpus. This batch treats Dgraph as
an adjacent graph/database system whose implementation can inform a Rust
Neo4j-style rewrite, especially where flat CSR is insufficient for fresh
updates, filtered access paths, query execution, and operational background
work.

Source repo:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/dgraph-src`

Evidence tools:

- CodeGraphContext batch05 artifact:
  `/tmp/codex-code-intel/codegraphcontext/dgraph-batch05/dgraph.sqlite`.
  It reports 1 repository, 480 files, 5,998 functions, 54 interfaces, 585
  structs, and 8 modules.
- CodeGraphContext was rerun for this continuation at:
  `/tmp/codex-code-intel/codegraphcontext/dgraph-src-20260707-072213`.
  The long index process was interrupted after a silent period, but the
  resulting database remained readable and reported 1 repository, 829 files,
  8,379 functions, 79 interfaces, 858 structs, and 13 modules.
- The rerun confirmed the key navigation anchors: `List` at
  `posting/list.go:66`, `Rollup` at `posting/list.go:1416`, `rollup` at
  `posting/list.go:1655`, `SubGraph` at `query/query.go:249`, and
  `proposeAndWait` at `worker/mutation.go:930` and
  `worker/proposal.go:125`.
- codebase-memory was also run at:
  `/tmp/codex-code-intel/codebase-memory/dgraph-src-20260707-072213`.
  It indexed 18,748 nodes and 91,942 edges, and found the same important
  anchors including `posting.List`, `query.SubGraph`,
  `posting.AddMutationWithIndex`, and worker `proposeAndWait` methods.
- Both graph tools were used for navigation only. Direct source reads are the
  authority for all claims below.

## Why Dgraph Matters For The Rust Rewrite

Dgraph is not Neo4j and does not implement Cypher/GDS compatibility, but its
storage/query architecture is a valuable counterpoint to a pure CSR mental
model:

- It stores predicate-keyed posting lists with immutable base state and mutable
  deltas.
- It uses compressed UID packs and specialized intersection operators as first
  class physical operators.
- It encodes access path families directly into durable keys: data, reverse,
  index, count, schema/type, and split-list keys.
- It treats schema/index changes as background operational work with interim
  query schemas, prefix drops, rebuilds, throttles, and task conflict rules.
- It exposes graph query execution through a recursive `SubGraph` tree with UID,
  value, facet, and language matrices, not through ad hoc object walking.
- It places explicit limits on query edge expansion and shortest-path frontier
  size to avoid OOM, accepting that some limits can change optimality.

For Knight Bus v003, this supports a hybrid thesis:

```text
Flat dual CSR should remain the static OLAP oracle and global-stream primitive,
but a full Neo4j/GDS-compatible low-RAM engine needs additional access-path
families: compressed posting-list sidecars, typed key ranges, mutable delta
overlays, schema/index rebuild contracts, and explicit query/algorithm memory
limits.
```

## Pattern 1: Predicate-Keyed Posting Lists With Mutable Deltas

### Source Evidence

Dgraph's `List` contains immutable posting-list state plus a `MutableLayer`,
along with timestamp bounds for the immutable layer:

- `posting/list.go:65-75` defines `List` with `plist`, `mutationMap`,
  `minTs`, `maxTs`, and a cache buffer.
- `posting/list.go:77-87` explains that every posting list has an immutable
  layer and a mutable layer; new postings are added as deltas and later
  converted into a complete posting list by rollup.
- `posting/list.go:87-107` defines `MutableLayer` with committed entries,
  current transaction entries, read timestamp, delete marker, UID caches, and
  calculated UID cache.
- `posting/list.go:129-147` clones a mutable layer by sharing committed entries
  and cached committed state by reference while ignoring current transaction
  entries.
- `posting/list.go:150-168` resets current transaction entries, read timestamp,
  current UID cache, calculated UID cache, delete marker, and UID map.

### Transferable Pattern

Do not make every graph update rewrite the durable adjacency structure. Use:

```text
base immutable adjacency/list
  + committed delta map
  + transaction-local current entries
  + timestamp-aware read facade
  + later rollup/compaction
```

### Rust Translation

For Knight Bus:

```rust
pub struct PostingListView<'a> {
    base: &'a PackedPostingList,
    committed_delta: &'a DeltaRun,
    current_delta: Option<&'a DeltaRun>,
    read_ts: ReadTimestamp,
}
```

The important rule is not the exact shape. The important rule is that the base
adjacency/list remains immutable and cheap to share, while recent updates are
small, timestamped, independently inspectable, and rollup-ready.

### Why This Matters For Neo4j-In-Rust

The active architecture question is whether a 50GB-class graph requires a full
OLAP rebuild after 10 record edits. Dgraph's design is evidence that the answer
should be "no" for many update classes. A Rust Neo4j rewrite can keep CSR as the
OLAP base, but WAL-derived updates should first land in a bounded delta overlay
or posting-list sidecar, not trigger a full snapshot rebuild.

### When To Use

- Fresh OLAP reads need to see small recent edge/property changes.
- Updates are small relative to the full graph.
- Base graph files are immutable, compressed, or mmap/direct-I/O optimized.
- Rollup can run in the background under explicit memory and I/O budgets.

### When Not To Use

- If the graph is fully rebuilt by large import batches where deltas approach
  the size of the base graph.
- If query semantics cannot tolerate reading base plus deltas.
- If delta depth is unbounded; then the overlay becomes a hidden LSM problem.

### Risks

- Mutable delta maps need strict timestamp correctness.
- Delete-all markers and tombstones can pollute query paths.
- Shared committed state must avoid accidental mutation. In Rust, this should
  be encoded with immutable references, `Arc`, copy-on-write buffers, or
  append-only delta files rather than shared mutable maps.

## Pattern 2: Rollup Splits Large Lists Into Multipart Durable Keys

### Source Evidence

- `posting/list.go:1346-1364` documents rollup: immutable and mutable layers
  are merged and may be split into multiple lists. The base key stores split
  start UIDs, and split parts are stored under keys formed from the base key and
  part start UID.
- `posting/list.go:1366-1415` describes timestamp rules for rollup writes,
  including writing at max timestamp plus one to avoid WAL replay overwrite and
  stale-read hazards.
- `posting/list.go:1416-1435` writes the marshaled rollup output and adjusts
  version when a reserved read timestamp is used.
- `posting/list.go:2260-2262` splits when serialized proto size exceeds
  `maxListSize` and UID pack has more than one block.
- `posting/list.go:2271-2288` recursively splits until no part needs splitting.
- `posting/list.go:2327-2355` splits UID blocks and postings at a midpoint UID.
- `x/keys.go:510-525` creates split keys by changing the key prefix to
  `ByteSplit` and appending the start UID.

### Transferable Pattern

Large adjacency or posting-list records need an internal page/tile split format.
The split boundary should be represented in the durable keyspace, not only in
an in-memory index.

### Rust Translation

For Knight Bus:

```text
node-adj-key -> split directory [start_dense_id...]
node-adj-key/start=1 -> first packed adjacency segment
node-adj-key/start=N -> next packed adjacency segment
```

This can complement Cellular CSR without forcing every partition to be a
geographic graph tile. A supernode can be split by UID range even inside a flat
snapshot.

### Why This Matters For Neo4j-In-Rust

Neo4j-style graphs have supernodes. A low-RAM OLAP layer cannot let one
high-degree node become a monolithic mmap fault, direct-I/O read, or compaction
unit. Multipart adjacency lets the system read or compact only the needed
segment.

### Design Implication

Cells are not the only option. Dgraph suggests an alternative or companion:

```text
segmented posting-list / segmented adjacency families
```

This can be cheaper than full Tilehouse for supernode-heavy workloads because
the split unit is an access path segment, not a whole graph partition.

## Pattern 3: Delta-Only Mutation Fast Path Avoids Base Reads

### Source Evidence

- `posting/lists.go:258-299` implements `LocalCache.getInternal`: when
  `readFromDisk` is false, it creates an empty `List` facade with a new mutable
  layer instead of reading Badger, then applies any existing delta.
- `posting/lists.go:397-402` defines `GetFromDelta`, documented as retrieving
  a cached version without reading disk, used when a posting list will only be
  modified and not read.
- `worker/mutation.go:82-88` explains the performance optimization: if a
  mutation path does not use the read posting list, Dgraph avoids retrieving
  it; rollup later consolidates deltas into a posting list.
- `worker/mutation.go:89-106` selects between scalar-list, full read, and
  `GetFromDelta` depending on schema, language, count index, delete operation,
  and list/scalar shape.

### Transferable Pattern

An update bridge should classify operations by read requirement:

```text
needs_base_read
needs_scalar_latest_value
needs_count_or_delete_context
delta_only_write
```

Then choose the cheapest physical path.

### Rust Translation

For Knight Bus:

```rust
pub enum OlapReceiptReadMode {
    DeltaOnly,
    ScalarLatest,
    FullAdjacencyRead,
    CountOrDeleteRead,
}
```

The WAL-to-OLAP bridge should compute this mode before touching CSR, sidecars,
or posting lists.

### Why This Matters For Neo4j-In-Rust

If 10 OLTP records are edited in a 50GB graph, the RAM-first OLAP layer should
not read 50GB, rebuild CSR, or fault large mmap regions merely to append
well-typed receipts. Dgraph proves a mature graph database can avoid base reads
for many mutation classes and defer consolidation.

### Agentic Guidance

When future agents implement WAL-derived OLAP updates, they should not start by
opening base CSR files. They should first write tests that prove a property or
index-only receipt can be appended without touching base adjacency bytes.

## Pattern 4: Compressed UID Lists Need Multiple Intersection Operators

### Source Evidence

- `algo/uidlist.go:17-18` defines jump and ratio heuristics.
- `algo/uidlist.go:31-60` intersects a compressed UID pack with a normal UID
  list and chooses linear-jump or binary strategy based on approximate length
  ratio.
- `algo/uidlist.go:83-87` cites "Fast Intersection Algorithms for Sorted
  Sequences" for compressed binary intersection.
- `algo/uidlist.go:140-167` intersects two sorted uncompressed UID lists and
  chooses linear, jump, or binary strategy with ratio thresholds.
- `algo/packed.go:16-32` applies a filter directly to packed UID blocks by
  decoding, testing, and re-encoding.
- `algo/packed.go:34-93` performs linear intersection between compressed UID
  lists block by block.
- `algo/packed.go:101-137` sorts multiple packed lists by exact length before
  intersecting from smallest to largest.

### Transferable Pattern

Filtered graph workloads need a physical operator library, not only an adjacency
format. At minimum:

```text
packed ∩ packed
packed ∩ unpacked
unpacked ∩ unpacked
packed filter -> packed
multi-list intersection ordered by estimated/exact length
```

### Rust Translation

For Knight Bus:

```rust
pub trait UidSetOperator {
    fn intersect_packed_with_packed(&self, left: PackedIds, right: PackedIds) -> PackedIds;
    fn intersect_packed_with_slice(&self, packed: PackedIds, slice: &[DenseNodeId]) -> SmallVecIds;
    fn filter_packed(&self, packed: PackedIds, filter: impl Fn(DenseNodeId) -> bool) -> PackedIds;
}
```

The concrete implementation should start with scalar portable Rust and then
permit SIMD or specialized decoding behind an identical oracle-tested API.

### Why This Matters For Neo4j-In-Rust

Full GDS surface includes filtered projections, label/type filters, property
filters, path predicates, candidate generation for similarity, and community
subgraphs. CSR gives fast neighbor iteration, but compressed set operators make
filtered graph execution memory-friendly.

### Memory Implication

These operators should stream/decode into bounded buffers rather than expand
all candidates into heap vectors. The low-RAM contract must include temporary
decode buffers and output buffers.

## Pattern 5: Durable Key Families Encode Physical Access Paths

### Source Evidence

- `x/keys.go:153-164` builds keys with type byte, namespace, attribute length,
  and attribute bytes.
- `x/keys.go:166-176` defines schema keys with a unique prefix.
- `x/keys.go:190-210` defines data keys as predicate plus UID, with optional
  split start UID for multipart lists.
- `x/keys.go:212-232` defines reverse keys.
- `x/keys.go:247-268` defines index keys as predicate plus index term.
- `x/keys.go:270-292` defines count keys, including reverse-count variants.
- `x/keys.go:294-304` defines parsed key fields for attr, UID, start UID,
  term, count, byte type, and prefix.
- `x/keys.go:527-590` parses durable keys back into structured access-path
  fields.

### Transferable Pattern

A graph database should make access path family explicit in the durable key:

```text
schema(attr)
data(attr, uid)
reverse(attr, uid)
index(attr, token)
count(attr, count)
split(base_key, start_uid)
```

### Rust Translation

For Knight Bus:

```rust
pub enum GraphKeyFamily {
    Schema,
    NodeProperty,
    ForwardAdjacency,
    ReverseAdjacency,
    LabelIndex,
    RelationshipTypeIndex,
    PropertyTokenIndex,
    CountIndex,
    SplitAdjacencySegment,
}
```

Typed key encoders should be round-trip tested and fuzzed. They should not be
ad hoc strings.

### Why This Matters For Neo4j-In-Rust

Neo4j's record store has pointer-rich records. A Rust rewrite preserving the
frontend semantics can still choose a backend that encodes physical access paths
as compact typed keys for sidecars, indexes, deltas, and OLAP receipts. That
gives a middle path between record pointers and all-in CSR.

## Pattern 6: Schema Parsing Produces Executable Index Contracts

### Source Evidence

- `schema/parse.go:168-184` documents and starts parsing `@index(...)`
  directives.
- `schema/parse.go:183-251` parses tokenizer and vector index specs, rejects
  invalid indexed types, tracks duplicate tokenizers, and restricts sortable
  tokenizers.
- `schema/parse.go:406-411` recognizes tokenizer or vector index specs.
- `schema/parse.go:413-468` resolves default tokenizers and validates index
  constraints, including invalid types, tokenizers without indexing, duplicate
  tokenizers, and multiple sortable indexes.
- `posting/index.go:52-78` builds index tokens only after checking schema type,
  scalar compatibility, index availability, type conversion, and tokenizer
  language.
- `posting/index.go:1114-1120` defines `IndexRebuild` with attr, start
  timestamp, old schema, and current schema.
- `posting/index.go:1130-1160` computes query schema that can be served while
  indexes are being rebuilt.
- `posting/index.go:1162-1184` drops tokenizer, vector, reverse, and count
  index prefixes before rebuild.
- `posting/index.go:1191-1208` decides whether rebuild is needed and builds
  token, reverse-edge, and count indexes.

### Transferable Pattern

Schema is not just metadata. It is a contract that drives:

```text
tokenizer selection
vector index shape
reverse edge materialization
count index materialization
query-time allowed schema during rebuild
drop/rebuild prefixes
background index task scheduling
```

### Rust Translation

For Knight Bus:

```rust
pub struct IndexContract {
    pub predicate: PropertyKey,
    pub value_type: PhysicalValueType,
    pub tokenizers: Vec<TokenizerSpec>,
    pub vector_indexes: Vec<VectorIndexSpec>,
    pub reverse_materialized: bool,
    pub count_materialized: bool,
    pub rebuild_plan: Option<IndexRebuildPlan>,
}
```

### Why This Matters For Neo4j/GDS Compatibility

Full GDS/Cypher compatibility requires the engine to know whether a property,
label, type, vector field, or relationship direction can be used as an access
path. A low-RAM architecture should not materialize every possible projection.
It should compile index/sidecar contracts from schema and configuration, and it
should expose memory estimates that account for rebuilding or scanning those
access paths.

## Pattern 7: Query Execution Uses Recursive SubGraphs And Matrices

### Source Evidence

- `query/query.go:35-86` explains lowering a nested graph query into recursive
  `SubGraph` objects and processing children concurrently.
- `query/query.go:99-202` defines `params`, including alias, count, offset,
  ordering, language preference, facets, variables, recursion, cascade,
  shortest-path inputs, `MaxFrontierSize`, and allowed predicates.
- `query/query.go:170-173` explicitly states that `MaxFrontierSize` prevents
  OOM during shortest-path computation but may affect optimality if too low.
- `query/query.go:225-298` defines `SubGraph` with count, value matrix, UID
  matrix, facets matrix, source UIDs, destination UIDs, filters, math
  expression, children, and vector metrics.
- `protos/pb.proto:37-66` models query requests with attr, language list,
  after UID, count flag, UID list, source function, reverse flag, facet filter,
  cache/read timestamp, limit/offset, and ordering.
- `protos/pb.proto:76-85` models results as UID matrix, value matrix, counts,
  facets matrix, language matrix, list flag, and vector metrics.
- `query/query.go:2920-3025` schedules query blocks when their variables are
  available and runs shortest path, recurse, or normal `ProcessGraph` in
  goroutines.

### Transferable Pattern

Represent graph query execution as:

```text
logical query tree
  -> recursive subgraph plan
  -> source UID list
  -> per-source UID/value/facet matrices
  -> child plans
  -> variable dependency scheduler
```

### Rust Translation

For Knight Bus:

```rust
pub struct LogicalGraphPlan {
    pub source: SourceSelector,
    pub expansions: Vec<ExpansionPlan>,
    pub filters: Vec<FilterPlan>,
    pub outputs: Vec<ResultColumnPlan>,
    pub memory_contract: MemoryEstimate,
}
```

The result surface should be matrix-aware because both Cypher path expansion
and GDS stream/mutate/write modes need stable row/column schemas.

### Why This Matters For Neo4j-In-Rust

CSR adjacency is a physical primitive, not a public query engine. Dgraph's
`SubGraph` evidence reinforces that a Neo4j rewrite needs an intermediate
representation that accounts for source lists, destination lists, filters,
facets/properties, result materialization, variable dependencies, and memory
limits before touching storage.

## Pattern 8: Recursive And Shortest-Path Expansion Must Enforce Edge Limits

### Source Evidence

- `query/recurse.go:55-164` iterates depth by depth, processes subgraphs
  concurrently, filters already-seen edges when loops are not allowed, merges
  destination UIDs, expands children, and checks `x.Config.LimitQueryEdge`.
- `query/shortest.go:141-236` expands outgoing adjacency for shortest path,
  processes child subgraphs concurrently, extracts facet costs, stores an
  adjacency map, and checks `x.Config.LimitQueryEdge`.
- `query/query.go:170-173` adds `MaxFrontierSize` to shortest-path parameters
  and warns that it prevents OOM but can affect optimality.

### Transferable Pattern

Every graph expansion operator needs:

```text
edge visit counter
frontier/candidate limit
loop/revisit policy
cost extraction policy
memory estimate before execution
runtime abort with deterministic error
```

### Rust Translation

For Knight Bus:

```rust
pub struct ExpansionBudget {
    pub max_edges_seen: u64,
    pub max_frontier_items: u64,
    pub max_heap_bytes: u64,
    pub may_sacrifice_optimality: bool,
}
```

### Why This Matters For Full GDS

Algorithms such as BFS, DFS, Dijkstra, Yen, all-shortest-paths, betweenness,
closeness, random walk, and influence-style procedures can explode memory if
frontiers are unbounded. A GDS-compatible surface should expose estimates and
budget rejections rather than silently OOMing.

### Verification Note

`query/shortest.go:231-235` compares against `x.Config.LimitQueryEdge` but the
error string mentions `x.Config.LimitMutationsNquad`. Future verification specs
should assert the limit variable and the user-facing error agree.

## Pattern 9: Background Work Needs An Explicit Conflict Matrix

### Source Evidence

- `worker/mutation.go:184-216` throttles concurrent predicate indexing because
  Badger opens roughly 8 files per predicate and unbounded indexing can hit
  file descriptor limits.
- `worker/mutation.go:218-278` checks tablet ownership, schema validity,
  rebuild requirements, starts an indexing task when necessary, installs an
  interim query schema, drops/rebuilds data, and either builds indexes in the
  background or updates schema immediately.
- `worker/draft.go:110-116` documents task-start semantics: rollup is canceled
  when other operations start; restore cancels all other operations; only
  `Done` should be called on the returned closer.
- `worker/draft.go:135-204` implements operation conflict rules for rollup,
  restore, backup, indexing, snapshot, and predicate move.

### Transferable Pattern

Background operations must be modeled as mutually constrained operations:

```text
rollup
snapshot
index rebuild
backup
restore
predicate move / partition move
compaction
```

They need a conflict matrix, cancellation semantics, resume behavior, and
resource budgets.

### Rust Translation

For Knight Bus:

```rust
pub enum BackgroundOperation {
    OlapDeltaRollup,
    SnapshotBuild,
    SidecarIndexRebuild,
    Backup,
    Restore,
    CellOrSegmentCompaction,
}
```

The operation scheduler should be testable without real storage by asserting
which operations can start, block, cancel, or resume.

### Why This Matters For 50GB-On-8GB

The biggest hidden RAM risks are often not query heaps. They are snapshot build
scratch, compaction buffers, index rebuilds, and multiple background jobs
running together. Dgraph's explicit task conflict rules are directly
transferable to a RAM-first OLAP architecture.

## Pattern 10: Raft WAL Uses Fixed Metadata, Zeroed Slots, Rotation, And Retry Backpressure

### Source Evidence

- `raftwal/storage.go:21-60` documents the disk format: a small `wal.meta`
  file, 32-byte entry metadata for term/index/type/data offset, first 30k
  entries fitting in the first 1MB, preallocated/zeroed entry area, cleanup
  relative to snapshot index, mmap behavior, and optional `msync`.
- `raftwal/storage.go:76-116` initializes metadata/WAL files, checks snapshot
  index consistency, and inserts delete entries for missing ranges after
  improper close.
- `raftwal/wal.go:132-223` overwrites same-index entries, zeroes later entries
  or deletes later files, rotates at max entries or 1GB, writes data slices,
  and writes 32-byte entry metadata.
- `worker/proposal.go:42-99` implements a feedback-only pending-proposal
  limiter with exponential retry weight and metrics.
- `worker/proposal.go:217-309` stores proposal contexts by unique key, proposes
  through Raft, retries timeouts, exempts deltas from the limiter, and releases
  limiter slots before retry to avoid a dining-philosopher-style deadlock.

### Transferable Pattern

Durability for a graph update bridge needs:

```text
fixed-size metadata
append-only entry slots
explicit zeroing/truncation on overwrite
snapshot-index-aware cleanup
proposal/apply correlation key
retry policy
pending-operation backpressure
metrics
```

### Rust Translation

For Knight Bus single-node CE, this does not require Raft in v003, but the shape
still applies:

```rust
pub struct OlapReceiptLogEntry {
    pub tx_id: u64,
    pub generation: u64,
    pub entry_type: ReceiptEntryType,
    pub payload_offset: u64,
}
```

The receipt log should support idempotent replay, truncation after partial
apply, and bounded pending receipts.

### Why This Matters For Neo4j-In-Rust

If OLTP remains Neo4j-shaped and OLAP is fed by WAL-derived receipts, then the
bridge is a durable log system, not a best-effort queue. The Dgraph WAL/proposal
shape gives concrete implementation taste for crash safety and backpressure.

## Pattern 11: Timestamp-Aware Cache Correctness Beats Raw Cache Hit Rate

### Source Evidence

- `posting/mvcc.go:510-531` initializes a Ristretto cache with max cost set to
  95 percent of cache size, cost based on posting-list approximate size, and an
  update rule that rejects older `maxTs`.
- `posting/mvcc.go:533-551` records cache metrics periodically.
- `posting/mvcc.go:564-571` sends lists with high delta counts to a high
  priority rollup queue.
- `posting/mvcc.go:764-777` reads from cache only if `minTs <= readTs` and
  `maxTs >= readTs`; otherwise it returns cache miss to avoid stale data.
- `posting/mvcc.go:813-830` reads latest disk state, saves it in cache, and
  only serves it if its timestamp range covers the requested read timestamp.

### Transferable Pattern

Cache validity in a graph database should include logical time:

```text
cache hit is valid iff cached_min_ts <= read_ts <= cached_max_ts
```

### Rust Translation

For Knight Bus:

```rust
pub struct CachedAdjacencyWindow {
    pub min_generation: SnapshotGeneration,
    pub max_generation: SnapshotGeneration,
    pub bytes: Arc<[u8]>,
}
```

### Why This Matters For Low RAM

A RAM-first system cannot rely on large warm caches. But the caches it does
allow must be correct under snapshot generations and delta overlays. Otherwise
low-RAM OLAP can return stale algorithm inputs while claiming snapshot
consistency.

## Decision Impact: Do We Need Cells?

Dgraph does not prove that Cellular CSR Tilehouse is mandatory. It proves CSR
alone is not enough for all update/filter/query/index cases. The stronger
design space after this batch is:

| option | what Dgraph evidence supports | low-RAM tradeoff |
| --- | --- | --- |
| Flat CSR only | Good as static global oracle and stream primitive | Weak for fresh updates, filters, supernodes, and index-maintained sidecars |
| Cellular CSR Tilehouse | Good if locality/compaction units need graph partitions | More metadata and boundary complexity |
| Segmented posting-list sidecars | Strongly supported by Dgraph's split lists and key families | Better for predicate/filter/supernode access; less natural for whole-graph scans |
| Graph-LSM delta overlay | Supported by immutable-plus-delta-plus-rollup pattern | Powerful but tombstone/version complexity can grow |
| Hybrid CSR + posting-list sidecars + receipt log | Best supported by current evidence | More formats than CSR, but fewer than 13 algorithm-specific layouts |

Recommended interpretation:

```text
Cells are one possible compaction/locality unit. They are not the only possible
unit. Dgraph suggests a lower-risk intermediate design: keep flat dual CSR as
the global OLAP base, add typed segmented posting-list sidecars for labels,
relationship types, property/index access paths, supernode segments, and
WAL-derived deltas, then graduate to cells only if measured locality/compaction
pressure proves that key-segment units are insufficient.
```

## Requirements Added To The V003 Research Spec

The Dgraph pass adds these requirements to future executable specs:

1. The OLAP update bridge SHALL classify receipts into delta-only, scalar-latest,
   count/delete, and full-adjacency-read paths before reading base topology.
2. The OLAP layer SHALL support bounded delta overlays and explicit rollup
   thresholds.
3. Supernode adjacency SHALL be splittable into durable segments independent of
   global CSR layout.
4. Filtered projection execution SHALL include compressed set/posting-list
   operators, not only CSR neighbor scans.
5. Durable sidecar/index keys SHALL be typed and round-trip validated.
6. Schema/index configuration SHALL compile into executable contracts and memory
   estimates.
7. Query and algorithm plans SHALL carry explicit edge/frontier/vector budgets.
8. Background work SHALL use a tested conflict matrix covering rollup, snapshot
   build, sidecar index rebuild, backup/restore, and compaction.
9. Cache validity SHALL include snapshot generation or timestamp range.
10. WAL-to-OLAP receipts SHALL be replayable, idempotent, bounded, and observable.

## Source / Inference / Speculation Table

| claim | type | evidence |
| --- | --- | --- |
| Dgraph stores posting lists as immutable base plus mutable deltas. | sourced fact | `posting/list.go:65-87` |
| Dgraph rolls deltas into complete posting lists and may split large lists. | sourced fact | `posting/list.go:1346-1364`, `posting/list.go:2260-2355` |
| Dgraph has a delta-only mutation path that avoids disk reads when only modifying. | sourced fact | `posting/lists.go:397-402`, `worker/mutation.go:82-106` |
| Dgraph specializes compressed UID intersections based on length ratios. | sourced fact | `algo/uidlist.go:31-60`, `algo/uidlist.go:140-167` |
| Dgraph key families encode data, reverse, index, count, schema, and split-list access paths. | sourced fact | `x/keys.go:153-304`, `x/keys.go:510-590` |
| Dgraph index rebuild has interim query schema and prefix-drop/rebuild steps. | sourced fact | `posting/index.go:1114-1208` |
| Dgraph query execution uses recursive subgraphs and UID/value/facet matrices. | sourced fact | `query/query.go:35-86`, `query/query.go:225-298`, `protos/pb.proto:37-85` |
| A Knight Bus OLAP bridge should avoid full CSR rebuild after small edits. | inference | Based on Dgraph delta/rollup patterns plus Knight Bus low-RAM objective |
| Segmented posting-list sidecars may be a lower-risk alternative or precursor to Tilehouse cells. | inference | Based on Dgraph split-list keys and earlier flat CSR evidence |
| Hybrid CSR plus typed sidecars is preferable to 13 per-algorithm persistent layouts. | inference | Consistent with previous Atlas critique plus Dgraph sidecar/index evidence |
| Cells can be deferred until measured locality/compaction pressure proves key-segment units are insufficient. | speculation | Requires benchmark evidence on Knight Bus workloads |

## Follow-Up Questions

1. Can Knight Bus implement segmented adjacency/posting sidecars with less
   complexity than Cellular CSR while still supporting fresh OLAP deltas?
2. Which GDS algorithm families benefit most from compressed posting-list set
   operators: similarity, label/type filtered projection, path filters, or
   community subgraph extraction?
3. What is the exact memory budget for base CSR plus delta overlay plus
   compressed posting sidecars on a 50GB graph / 8GB machine?
4. Can WAL-derived OLAP receipts be made idempotent without importing the full
   complexity of Raft proposal handling?
5. Should supernode split segments be keyed by dense ID range, edge type,
   property partition, or physical byte budget?

## Agentic Guidance

When future agents work on the Rust rewrite:

- Do not frame the storage choice as `CSR vs cells` only.
- Preserve flat CSR as the global correctness oracle.
- Add a typed access-path vocabulary before building more physical formats.
- Implement receipt classification and delta-only append tests before any full
  snapshot refresh optimization.
- Treat compressed UID/posting-list operators as algorithm infrastructure.
- Build memory estimates from physical operator state: decoded block buffers,
  frontier heaps, UID matrices, delta overlays, cache windows, rebuild scratch,
  and direct I/O buffers.
- Require every background task to declare conflicts and memory budget before
  it can run.

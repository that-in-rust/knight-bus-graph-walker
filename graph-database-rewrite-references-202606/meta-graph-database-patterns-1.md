# Meta Graph Database Patterns 1

Source-backed encyclopedia slice for Neo4j-family storage, graph database
engines, graph libraries, graph algorithms, and graph benchmark patterns relevant
to rewriting Neo4j in Rust with lower RAM.

Worker: 1
Canonical write target: `graph-database-rewrite-references-202606/meta-graph-database-patterns-1.md`

## Premise And Deconstruction

A lower-RAM Rust rewrite of Neo4j should not copy one implementation shape into
all workloads. The inspected systems keep making the same architectural split:

1. The online graph store wants stable IDs, cheap mutation, compact records,
   overflow stores for rare large values, and predictable concurrency.
2. The traversal and graph algorithm layer wants compact dense integer IDs,
   contiguous adjacency, cursor reuse, batch scans, and memory estimates that
   distinguish peak loading memory from steady-state memory.
3. The benchmark layer wants independent oracles: canonical output formats,
   ID remapping, exact rules for integral algorithms, epsilon rules for floats,
   and small validation graphs.

Neo4j's record store is not "bad CSR." It is a mutable, page-cache-oriented
graph store with fixed-width records and linked chains. Neo4j GDS, Kuzu,
petgraph CSR, graph-csr-openmp, RedisGraph/FalkorDB, and GraphBLAS-style
platforms show the other side: projection or matrix forms that are excellent for
scan-heavy algorithms but awkward for property graph updates. The design target
for Rust should be a two-form engine:

- Primary store: stable external IDs, compact records, append/overflow pools,
  small inline payloads, and MVCC-friendly mutation.
- Projected store: per-label/type selected graph snapshots with remapped dense
  IDs, compressed CSR adjacency, cursor APIs, and typed property side arrays.

The strongest recurring taste is "make the hot path structurally boring." Hot
records are fixed-width. Hot adjacency is offsets plus packed targets. Hot graph
algorithm state is one or two arrays indexed by dense node offset. Rare cases
move to side stores: dynamic labels, dynamic property values, big counts,
compressed buffers, delta matrices, transient CSR append lists, or external
degree caches.

## Evidence Tools And Commands

Graph tools were used for navigation only. Important claims below are verified
with direct source reads.

### Required skill tools

- Read and followed:
  `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- Read and followed:
  `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

### codebase-memory-mcp / local wrapper

Used as workspace orientation, not as proof for reference repositories:

```bash
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh \
  /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
```

Output directory:

```text
/tmp/codex-code-intel/codebase-memory/knight-bus-graph-walker-20260706-230144
```

The scan was constrained to the current workspace and did not index
`gitrefrepo/`.

### CodeGraphContext

Attempted to index `gitrefrepo/petgraph-src`; this was too expensive for the
turn and was stopped. CodeGraphContext was then used successfully on a compact
high-value CSR reference:

```bash
HOME="$run/home" \
/Users/amuldotexe/.codex/tooling/code-intelligence/.venvs/codegraphcontext/bin/cgc \
  --database ladybugdb --path "$run/ladybugdb.sqlite" \
  index /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src \
  --force

HOME="$run/home" cgc --database ladybugdb --path "$run/ladybugdb.sqlite" stats
HOME="$run/home" cgc --database ladybugdb --path "$run/ladybugdb.sqlite" list
```

Observed stats: 1 repo, 4 files, 7 functions, 3 modules. Direct source reads
verified the CSR layout and parallel builder claims.

### Direct source evidence

Primary commands were `rg --files`, `rg -n`, and `nl -ba ... | sed -n ...`.
Representative command shapes:

```bash
rg --files gitrefrepo | rg 'neo4j|kuzu|memgraph|petgraph|graph-csr|graphblas|ldbc'
nl -ba <source-file> | sed -n '<start>,<end>p'
rg -n 'struct Graph|CSR|Adjacency|PropertyStore|RelationshipGroup|GraphBLAS' <repo>
```

## Repositories Inspected

| Repository | Depth | Highest value evidence |
| --- | --- | --- |
| `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j` | Deep direct source reads | Record storage, relationship chains, dense node groups, node labels, property blocks, importer cache |
| `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03` | Deep direct source reads | Compressed adjacency, packed adjacency, cursor APIs, HugeGraph, projection memory estimates, catalog, canonical oracle |
| `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science` | Light sibling orientation | Same Neo4j GDS family; used mainly to confirm local availability |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src` | CodeGraphContext plus direct source reads | CSR layout and OpenMP edge-list-to-CSR builder |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/petgraph-src` | Direct source reads | Rust Vec-backed graph, Rust CSR, generic algorithms |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/memgraph-src` | Direct source reads | Mutable vertex/edge store, packed delta pointers, encoded PropertyStore |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src` | Direct source reads | Table+offset IDs, graph scan interface, CSR node groups, dense/sparse frontiers |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redisgraph-src` | Direct source reads | GraphBLAS matrix graph, label/relation matrices, delta matrices |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/falkordb-src` | Search-level orientation | Same RedisGraph lineage with delta matrix files and graph memory files; not deeply read |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/lagraph-src` | Direct source read | Cached out-degree as derived sparse vector |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_graphblas-src` | Direct source reads | GraphBLAS benchmark algorithms, MatrixMarket loading, output remapping |
| `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics-src` | Direct source reads | Benchmark validation rules and sample graph tests |

## Pattern Index

- Graph storage:
  fixed records with overflow stores; two-endpoint relationship chains; mutable
  small-vector primary store; GraphBLAS matrix graph with deltas.
- IDs:
  packed references; table+offset IDs; compact algorithm IDs; ID mapping at
  benchmark boundaries.
- Adjacency layout:
  sparse chains; dense relationship groups; CSR offsets; compressed pages;
  chunked CSR with transient append areas.
- Properties:
  inline primitive/short values; dynamic overflow; encoded flat buffers; typed
  side arrays for projections.
- Labels and types:
  inline label fields; dynamic label overflow; per-label and per-relation
  matrices.
- Dense/sparse nodes and algorithm state:
  dense-node thresholds; importer count caches; sparse/dense frontier switching.
- Traversal:
  cursor state machines; reusable cursors; vectorized neighbor chunks.
- Graph algorithms:
  cursor-over-adjacency APIs; GraphBLAS kernels; cached derived vectors.
- Projection/catalog:
  peak versus steady-state memory estimates; named in-memory graph catalog.
- Benchmark oracles:
  canonical adjacency; Graphalytics exact/epsilon/equivalence rules.

## Graph Storage Patterns

### Pattern: Fixed-Width Record Kernel With Overflow Stores

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
- Files:
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/record/NodeRecord.java`
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java`
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/record/RelationshipRecord.java`
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java`
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java`

Source taste:

- `NodeRecord` carries only next relationship reference, next property reference,
  label field, dynamic label records, light/heavy state, and a dense bit
  (`NodeRecord.java:32-67`, `:95-127`).
- Standard node records are 15 bytes: in-use/header, next-rel, next-prop,
  5-byte labels, and extra dense flags (`NodeRecordFormat.java:30-32`,
  `:55-112`).
- Standard relationship records are 34 bytes and pack endpoints, type,
  four chain references, property reference, and first-in-chain flags
  (`RelationshipRecordFormat.java:31-35`, `:63-113`, `:121-182`).
- Property records are 41 bytes: modifier byte, prev/next property references,
  and 32 bytes of payload blocks (`PropertyRecordFormat.java:33-39`,
  `:64-136`).

Pseudocode:

```text
node_record = {
  next_rel_ref,
  next_prop_ref,
  label_field_40_bits,
  dense_flag
}

relationship_record = {
  first_node, second_node, type,
  first_prev, first_next,
  second_prev, second_next,
  next_prop,
  first_in_chain_flags
}
```

Why it matters:

Fixed records give predictable page-cache density. Large and uncommon material
is pushed into dynamic records. The cost is pointer chasing; the benefit is that
the primary store does not allocate a heap object per conceptual edge/property.

Rust translation:

- Use packed `#[repr(C)]` or carefully serialized record structs for disk/page
  format, not Rust heap structs as the durable format.
- Split logical references from physical storage. Use newtypes like
  `NodeRecordId`, `RelRecordId`, `PropRecordId`.
- Keep optional overflow pointers as integer references. Avoid `Option<Box<T>>`
  in hot records.

Memory implications:

- Excellent steady-state RAM if pages can be mapped/cached and decoded lazily.
- Poor scan locality for whole-graph algorithms unless projected into CSR.

Concurrency implications:

- Fixed records make page/record latching tractable.
- Overflow chains need update discipline: allocating a new dynamic chain can be
  cheaper than in-place rewrite.

Testing implications:

- Golden tests should assert byte-level encode/decode round trips for high bits,
  dense flags, label fields, and property terminators.
- Fuzz references near high-bit boundaries.

Agent guidance:

- Do not model the primary Neo4j replacement as a vector of rich Rust structs.
  First define the durable record bytes and overflow tables, then wrap them in
  typed APIs.

### Pattern: Relationship Belongs To Both Endpoint Chains

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
- File: `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/record/RelationshipRecord.java`
- Also echoed in Rust library form:
  `gitrefrepo/petgraph-src/crates/petgraph/src/graph_impl/mod.rs`

Source taste:

- Neo4j relationship records store `firstNode`, `secondNode`, `type`, and four
  linked-list references: prev/next for the first endpoint and prev/next for the
  second endpoint (`RelationshipRecord.java:29-39`, `:58-80`).
- Accessors choose the correct chain side by endpoint node ID
  (`RelationshipRecord.java:133-171`).
- petgraph's mutable `Graph` keeps `Vec<Node>` and `Vec<Edge>`; each node has
  first outgoing/incoming edge IDs and each edge has next outgoing/incoming edge
  IDs plus endpoint IDs (`graph_impl/mod.rs:230-260`, `:392-396`).

Pseudocode:

```text
rel.r_next(node):
  if node == rel.first_node: return rel.first_next
  if node == rel.second_node: return rel.second_next
  error
```

Why it matters:

An edge is a first-class record, but traversal from either endpoint still starts
from a node-local chain. This is mutation-friendly and keeps edge properties
attached to one relationship record.

Rust translation:

- Represent each relationship record once.
- Store two chain slots per relationship:
  `endpoint[0].prev_next`, `endpoint[1].prev_next`.
- Use endpoint-side enums instead of repeatedly comparing raw IDs after lookup.

Memory implications:

- Four relationship references per relationship are expensive compared with CSR,
  but cheaper than duplicating property-bearing edges.

Concurrency implications:

- Creating/deleting a relationship touches both endpoint chains and the
  relationship record. A Rust implementation needs an ordering rule for latches
  to avoid deadlock.

Testing implications:

- Test deletion from head, middle, and tail for both endpoint sides.
- Test self-loops separately because both endpoints are the same logical node.

Agent guidance:

- Preserve the distinction between relationship identity and adjacency entry.
  CSR entries are adjacency entries; Neo4j relationships are identity-bearing
  records.

### Pattern: Mutable Small-Vector Primary Store With MVCC Delta Chains

Where found:

- Language: C++
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/memgraph-src`
- Files:
  - `src/storage/v2/vertex.hpp`
  - `src/storage/v2/edge.hpp`
  - `src/storage/v2/edge_ref.hpp`
  - `src/storage/v2/delta.hpp`
  - `src/utils/pointer_pack.hpp`

Source taste:

- `Vertex` has a stable `gid`, small-vector labels, small-vector in/out edge
  triples, a compact `PropertyStore`, lock, and packed delta pointer
  (`vertex.hpp:32-66`). The file asserts `sizeof(Vertex) == 80`
  (`vertex.hpp:72-73`).
- An edge triple is `(EdgeTypeId, Vertex*, EdgeRef)` (`vertex.hpp:29-30`).
- `EdgeRef` is a union of `Gid` or `Edge*` (`edge_ref.hpp:22-34`).
- `PointerPack<T, NumFlagBits>` stores low-bit flags in aligned pointers
  (`pointer_pack.hpp:18-62`).
- `Delta` uses a union for actions: labels, properties, edge add/remove, and
  prev/next delta pointers (`delta.hpp:244-391`). It has a size assertion at
  most 56 bytes (`delta.hpp:402-408`).

Pseudocode:

```text
vertex = {
  gid,
  labels: small_vec<label_id>,
  in_edges: small_vec<(type, other_vertex, edge_ref)>,
  out_edges: small_vec<(type, other_vertex, edge_ref)>,
  properties: encoded_property_store,
  lock,
  delta_ptr_with_flags
}
```

Why it matters:

Memgraph optimizes for an in-memory mutable graph rather than disk fixed
records. The notable part is not "use vectors"; it is the disciplined compactness
around mutation: small vectors, pointer tagging, packed deltas, and encoded
properties.

Rust translation:

- Use `SmallVec` only if benchmarks show most node degrees/labels fit inline.
- Encapsulate pointer tagging behind a safe newtype. Prefer integer arena IDs if
  pointer provenance or compaction matters.
- Model deltas as compact enums stored in an arena, not boxed trait objects.

Memory implications:

- Excellent for low-degree graphs and hot mutable workloads.
- Higher per-vertex base cost than Neo4j's fixed 15-byte record model.

Concurrency implications:

- Per-vertex/edge locks and delta chain iteration must be designed together.
- Memgraph explicitly allows non-sequential deltas for edge-write-heavy imports,
  trading faster writes for more expensive delta-chain iteration and GC
  (`delta.hpp:180-186`).

Testing implications:

- Size tests are valuable. Treat struct size as a public performance contract.
- Add MVCC visibility tests for sequential and non-sequential edge deltas.

Agent guidance:

- If the rewrite is page-cache/disk-first, borrow Memgraph's compact mutation
  ideas selectively. Do not blindly adopt an 80-byte in-memory vertex as the
  primary durable node.

### Pattern: Matrix Graph With Delta Plus/Minus

Where found:

- Language: C
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redisgraph-src`
- Files:
  - `src/graph/graph.h`
  - `src/graph/graph.c`
  - `src/graph/rg_matrix/rg_matrix.h`
  - `src/graph/rg_matrix/rg_wait.c`
- Related orientation: `gitrefrepo/falkordb-src/src/graph/delta_matrix/*`

Source taste:

- RedisGraph's `Graph` stores nodes and edges in datablocks plus GraphBLAS
  matrices: one adjacency matrix, label matrices, a node-label matrix, relation
  matrices, and locks (`graph.h:49-62`).
- `RG_Matrix` wraps a materialized matrix, `delta_plus`, `delta_minus`, optional
  transpose, and mutex (`rg_matrix.h:124-131`).
- Matrix comments enumerate valid and impossible states for base/delta matrices
  (`rg_matrix.h:42-123`).
- Synchronization applies deletions and additions when forced or when pending
  changes exceed a threshold (`rg_wait.c:28-126`).
- Graph matrix synchronization resizes to graph-required dimension and flushes
  dirty matrices under matrix lock (`graph.c:171-233`).

Pseudocode:

```text
visible_matrix = base - delta_minus + delta_plus

wait(force):
  if force or deleted_pending >= threshold: apply delta_minus
  if force or added_pending >= threshold: apply delta_plus
  materialize base and delta matrices
```

Why it matters:

This is the cleanest algebraic design for typed traversal and label/type scans,
but it is not a property-record store. It is best viewed as a projection/index
strategy for selected labels/types, not as a full Neo4j replacement by itself.

Rust translation:

- Consider matrix-backed indexes for labels/relationship types if algebraic
  query operators are a goal.
- Keep matrix deltas separate from primary relationship records.
- Provide a `MatrixProjection` trait rather than entangling GraphBLAS with
  storage records.

Memory implications:

- Sparse matrices can be extremely compact for relation/type adjacency but may
  duplicate the primary graph.
- Delta matrices add peak memory but batch updates and reduce immediate rewrite.

Concurrency implications:

- RedisGraph uses graph read/write locks plus per-matrix locks.
- Rust should avoid exposing mutable matrix internals without lock/context
  ownership.

Testing implications:

- Test impossible base/delta states.
- Test sync thresholds and force-sync behavior.
- Test transpose consistency if maintained.

Agent guidance:

- Use this pattern for "graph algorithm/query acceleration projection," not for
  the canonical property graph record layer.

## ID Patterns

### Pattern: Stable Logical ID Versus Compact Dense Offset

Where found:

- Language: C++
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src`
- File: `src/include/common/types/types.h`
- Related Rust library:
  `gitrefrepo/petgraph-src/crates/petgraph/src/graph_impl/mod.rs`
- Related benchmark platform:
  `gitrefrepo/ldbc_graphalytics_platforms_graphblas-src/src/main/c/src/graphio.cpp`

Source taste:

- Kuzu's `internalID_t` is `(offset, tableID)`; `nodeID_t` and `relID_t` are
  aliases (`types.h:71-95`).
- petgraph's `Graph` uses compact node/edge indices and warns that removals can
  shift indices; default index type is `u32`, with `usize` for very large graphs
  (`graph_impl/mod.rs:362-383`).
- GraphBLAS Graphalytics loads a separate mapping vector from compact matrix row
  IDs to original vertex IDs (`graphio.cpp:40-65`).

Pseudocode:

```text
external_node_id = { table_id, offset }
projection_node_id = dense_u32_or_u64
mapping[projection_node_id] -> external_node_id
```

Why it matters:

Stable user-visible IDs and compact algorithm IDs solve different problems.
Neo4j-style storage wants stable references. CSR/matrix algorithms want dense
integer offsets.

Rust translation:

- Define separate newtypes:
  `StoreNodeId`, `StoreRelId`, `ProjectionNodeId`, `TableId`, `Offset`.
- Require explicit mapping when crossing from store to projection.
- Let projection choose `u32` when node count fits; this matters for RAM.

Memory implications:

- `u32` adjacency targets cut target-array memory in half versus `u64`.
- Mapping arrays are a cost, but they prevent the whole store from paying dense
  ID assumptions.

Concurrency implications:

- Dense projection IDs can be immutable snapshot-local, avoiding locks during
  algorithms.
- Store IDs remain stable across concurrent transactions.

Testing implications:

- Test projection mapping under deleted/missing nodes.
- Test ID overflow and `u32` to `u64` promotion paths.

Agent guidance:

- Never pass raw `usize` between storage and algorithm layers. The compiler
  should make accidental ID mixing hard.

### Pattern: Reference High Bits And Flag Bits Are Worth Budgeting

Where found:

- Language: Java and C++
- Repos:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/memgraph-src`
- Files:
  - `NodeRecordFormat.java`
  - `RelationshipRecordFormat.java`
  - `src/utils/pointer_pack.hpp`
  - `src/storage/v2/delta.hpp`

Source taste:

- Neo4j record formats pack high reference bits into header/modifier bytes
  (`NodeRecordFormat.java:55-77`, `RelationshipRecordFormat.java:63-113`).
- Memgraph uses low pointer bits for flags when alignment guarantees those bits
  are zero (`pointer_pack.hpp:18-27`).
- Memgraph's `PreviousPtr` stores pointer type in low bits for Delta/Vertex/Edge
  pointers (`delta.hpp:51-133`).

Pseudocode:

```text
encoded_ref = low_32_bits_in_field + high_bits_in_header
tagged_ptr = aligned_ptr | low_bit_flags
```

Why it matters:

The cheapest byte is the one never allocated. High-bit and tag-bit packing is
unglamorous but central to RAM reduction.

Rust translation:

- Use bit-packed integer references for durable records.
- Keep unsafe pointer tagging isolated. Consider `NonNull<T>` plus explicit
  alignment assertions, or avoid pointer tagging in favor of packed arena IDs.

Memory implications:

- Saves per-record bytes multiplied by billions of records.
- Can impose maximum ID ranges and migration work.

Concurrency implications:

- Atomic pointer tagging needs memory-order reasoning.
- Durable bit packing needs endian and compatibility tests.

Testing implications:

- Property-based encode/decode tests for reference boundaries.
- Static size tests for all hot records.

Agent guidance:

- Treat bit layout as a versioned public contract. Do not "simplify" packed
  references during refactors without a measured memory budget.

## Adjacency Layout Patterns

### Pattern: Sparse Chains Become Dense Relationship Groups

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
- Files:
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/record/RelationshipGroupRecord.java`
  - `community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java`

Source taste:

- `RelationshipGroupRecord` stores relationship type, next group, first outgoing,
  first incoming, first loop, owning node, and external-degree flags
  (`RelationshipGroupRecord.java:28-74`, `:158-180`).
- Its prev pointer is explicitly not persisted; it is set while reading group
  chains (`RelationshipGroupRecord.java:130-147`).
- `RecordRelationshipTraversalCursor` decodes a relationship reference as either
  sparse relationship-chain reference or dense relationship-group reference
  (`RecordRelationshipTraversalCursor.java:83-101`).
- Dense traversal is a state machine over incoming, outgoing, and loop chains
  per relationship type group (`RecordRelationshipTraversalCursor.java:171-244`).

Pseudocode:

```text
if node.is_dense:
  for group in relationship_groups(node):
    if type_selected(group.type):
      scan(group.first_in)
      scan(group.first_out)
      scan(group.first_loop)
else:
  scan(node.next_rel_chain)
```

Why it matters:

Low-degree nodes should not pay per-type index overhead. High-degree nodes need
type/direction heads to avoid scanning thousands of unrelated relationships.

Rust translation:

- Use a `NodeAdjacencyRef` enum:
  `Sparse(RelId)` or `Dense(RelGroupId)`.
- Store relationship groups as fixed records separate from node records.
- Keep group prev links transient unless a measured update path requires them.

Memory implications:

- Dense groups add records only for high-degree nodes.
- External degree flags avoid bloating every group with large counters.

Concurrency implications:

- Promotion from sparse to dense is a structural rewrite. It needs transaction
  logging and rollback tests.

Testing implications:

- Test sparse/dense equivalence for all direction/type filters.
- Test ordered type selection early exit when groups are sorted.
- Test self-loop handling.

Agent guidance:

- Dense node handling is not an optimization afterthought. It is a separate
  traversal representation with its own cursor states.

### Pattern: CSR As Immutable Traversal Projection

Where found:

- Languages: Java, C++, Rust
- Repos:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/petgraph-src`
- Files:
  - `core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java`
  - `gitrefrepo/graph-csr-openmp-src/inc/Graph.hxx`
  - `gitrefrepo/petgraph-src/crates/petgraph/src/csr.rs`

Source taste:

- GDS exposes adjacency through `degree(node)` and cursor constructors, not
  storage internals (`AdjacencyList.java:25-62`, `:94-104`).
- graph-csr-openmp's `DiGraphCsr` stores offsets, degrees, node values,
  edge keys, and edge values (`Graph.hxx:376-409`). Edge iteration slices from
  `offsets[u]` for `degrees[u]` entries (`Graph.hxx:489-507`).
- petgraph CSR stores `column`, `edges`, `row`, `node_weights`, and edge count
  (`csr.rs:68-79`). It requires sorted unique edges for linear-time construction
  (`csr.rs:164-254`).

Pseudocode:

```text
neighbors(u):
  start = row[u]
  end = row[u + 1]
  return columns[start..end]
```

Why it matters:

CSR is the workhorse layout for memory-efficient traversal. It should be a
projection target from the mutable store, not the only storage model.

Rust translation:

- Define `CsrGraph<Target = u32, Offset = u64>`.
- Keep edge properties in optional parallel arrays or typed side arrays.
- Use sorted targets when algorithms need `advance/skip_until`.

Memory implications:

- `row` is `nodes + 1`; `columns` is `edges`. This is the baseline memory model
  to beat.
- `u32` targets and compressed deltas can cut RAM further.

Concurrency implications:

- Immutable CSR snapshots are easy to share across threads.
- Building CSR from a live store needs snapshot isolation.

Testing implications:

- Compare CSR traversal against primary-store traversal on sampled graphs.
- Test sortedness and duplicate rejection.

Agent guidance:

- First build correct uncompressed CSR. Add compression after cursor tests and
  memory estimates are in place.

### Pattern: Parallel Edge-List To CSR Builder Has Separate Peak Memory

Where found:

- Language: C++ with OpenMP
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src`
- Files:
  - `inc/io.hxx`
  - `main.cxx`

Source taste:

- The builder computes degrees, exclusive-scans offsets, and uses atomic capture
  on offsets to place edges for the one-partition path (`io.hxx:318-394`).
- The multi-partition path computes global degrees, per-partition offsets, fills
  per-partition CSR, then combines per vertex into global CSR (`io.hxx:318-394`).
- `main.cxx` allocates per-thread source/target/weight arrays and per-partition
  degrees/offsets/edge arrays before conversion (`main.cxx:60-92`).

Pseudocode:

```text
degree[u] += 1
offset = exclusive_scan(degree)
write_pos = atomic_fetch_add(temp_offset[u], 1)
columns[write_pos] = v
```

Why it matters:

Projection builders often use far more memory than the final CSR. A lower-RAM
rewrite must track peak memory, not just steady-state graph size.

Rust translation:

- Use a streaming or partitioned builder when input cannot fit twice.
- Separate `BuildMemoryEstimate` from `ProjectedMemoryEstimate`.
- Prefer per-thread degree histograms plus reduction over atomics if contention
  is high.

Memory implications:

- Temporary edge lists and per-partition arrays can dominate peak RSS.
- Streaming sort/spill may be necessary for large imports.

Concurrency implications:

- Atomics in builders are acceptable if isolated from query runtime.
- Parallel fill should avoid false sharing on hot degree/offset counters.

Testing implications:

- Benchmark skewed graphs with high-degree nodes.
- Assert final CSR equivalence across one-partition and multi-partition builds.

Agent guidance:

- When promising "lower RAM," always quote both loading peak and steady-state
  projection memory.

### Pattern: Compressed Paged Adjacency With Cursor Reuse

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- Files:
  - `core/src/main/java/org/neo4j/gds/core/compression/common/CompressedAdjacencyList.java`
  - `core/src/main/java/org/neo4j/gds/core/compression/common/AdjacencyDecompressingReader.java`
  - `compression/src/main/java/org/neo4j/gds/compression/packed/PackedAdjacencyList.java`
  - `core-api/src/main/java/org/neo4j/gds/api/AdjacencyCursor.java`

Source taste:

- `CompressedAdjacencyList` stores `byte[][] pages`, `HugeIntArray degrees`,
  and `HugeLongArray offsets` (`CompressedAdjacencyList.java:107-110`).
- It estimates best/worst compression from delta sizes and degree/offset arrays
  (`CompressedAdjacencyList.java:64-105`).
- A zero degree returns an empty cursor; existing cursors are reused when they
  are of the right decompression type (`CompressedAdjacencyList.java:132-155`).
- `AdjacencyDecompressingReader` decodes chunks of 64 values and supports
  `skipUntil`/`advance` by checking block boundaries and binary searching inside
  decoded blocks (`AdjacencyDecompressingReader.java:31-40`, `:69-163`).
- `PackedAdjacencyList` hides multiple compression strategies behind the same
  cursor interface (`PackedAdjacencyList.java:57-237`).
- `AdjacencyCursor` exposes `next`, `peek`, `skipUntil`, `advance`, and
  `advanceBy` (`AdjacencyCursor.java:47-90`).

Pseudocode:

```text
cursor.init(offset, degree)
while cursor.has_next:
  target = cursor.next()

skip_until(target):
  skip compressed blocks while block_last < target
  binary_search(decoded_block, target)
```

Why it matters:

Compression only helps if algorithms can traverse without allocating decoded
neighbor vectors. Cursor reuse keeps decompression scratch memory out of hot
loops.

Rust translation:

- Define `AdjacencyCursor` as a trait or enum over concrete cursor strategies.
- Prefer concrete enums in hot paths to avoid dynamic dispatch if needed.
- Keep a reusable `[u64; 64]` decode block or arena-owned scratch.

Memory implications:

- Degrees and offsets are still major arrays. Compressing only target lists is
  not enough.
- Page arrays avoid one allocation per node.

Concurrency implications:

- Immutable pages can be shared; cursors are per-thread/per-task scratch.

Testing implications:

- Build scalar decoder golden tests.
- Test `next`, `peek`, `skip_until`, `advance`, and `advance_by` against
  uncompressed sorted arrays.
- Test cursor reuse does not leak state between nodes.

Agent guidance:

- Do not expose decompressed adjacency as `Vec<NodeId>` in algorithm APIs unless
  the algorithm explicitly needs materialization.

### Pattern: Generated Bit Packers For Fixed Blocks

Where found:

- Language: Java generated by Rust
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- File:
  `compression/src/main/java/org/neo4j/gds/compression/packed/AdjacencyPacking.java`

Source taste:

- The file says it is generated by Rust `src/main.rs` and should not be edited
  manually (`AdjacencyPacking.java:24-28`).
- It uses `BLOCK_SIZE = 64` and dispatches packers by bit width
  (`AdjacencyPacking.java:33-41`).
- The generated table covers bit widths 0 through 64 (`AdjacencyPacking.java:50-116`).

Pseudocode:

```text
pack_64_values(values, bit_width):
  dispatch bit_width -> specialized_packer(values)
```

Why it matters:

Generated pack/unpack code removes branches from the inner compression loop and
keeps source maintainability.

Rust translation:

- Use `build.rs` or a generator crate for bit-width-specialized packers.
- Keep scalar fallback for clarity and fuzzing.
- Consider const generics for `BitWidth` if compiler output is acceptable.

Memory implications:

- Block packing turns sorted delta targets into near-information-density storage.

Concurrency implications:

- Generated packers are pure functions and easy to parallelize.

Testing implications:

- Exhaustively test small bit widths and random round trips for all widths.
- Differential-test generated packers against scalar packers.

Agent guidance:

- Generated compression code is acceptable, but the generator is the source of
  truth. Review the generator, not only the generated file.

### Pattern: Chunked CSR With Persistent Data And Transient Append Area

Where found:

- Language: C++
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src`
- Files:
  - `src/include/storage/table/csr_node_group.h`
  - `src/include/storage/table/csr_chunked_node_group.h`
  - `src/storage/table/csr_node_group.cpp`
  - `src/storage/table/csr_chunked_node_group.cpp`

Source taste:

- `CSRNodeGroup` stores persistent data in CSR format and transient data in
  normal chunked groups, with an extra CSR index tracking row indices per bound
  node (`csr_node_group.h:165-172`).
- `InMemChunkedCSRHeader` and `ChunkedCSRHeader` store offset and length columns
  (`csr_chunked_node_group.h:59-95`, `:97-130`).
- Header code computes start/end offsets, gap sizes, and region offsets
  (`csr_chunked_node_group.cpp:64-207`).
- In-memory CSR lists remain sequential when appending at the end and expand to
  explicit sorted row indices when non-sequential (`csr_node_group.cpp:362-397`).

Pseudocode:

```text
node_csr_index:
  if sequential: [start_row, length]
  else: sorted_row_indices[]

append(bound_node, start_row, length):
  if start_row == old_start + old_length: extend sequential
  else: expand to explicit rows and sort
```

Why it matters:

This is a middle ground between immutable CSR and fully mutable adjacency lists.
It preserves compact scans for committed data while tolerating new writes.

Rust translation:

- Use enum:
  `NodeRows::Sequential { start, len } | NodeRows::Explicit(Vec<RowId>)`.
- Keep persistent CSR immutable and append/transient rows separate until
  checkpoint/compaction.

Memory implications:

- Sequential append is very cheap.
- Random updates pay explicit row-index memory only where needed.

Concurrency implications:

- Persistent chunks can be read concurrently; transient chunk groups need locks.
- Checkpointing/compaction is a natural write barrier.

Testing implications:

- Test sequential-to-explicit transition.
- Test persistent plus transient scan order.
- Test checkpoint preserves visible rows and gaps.

Agent guidance:

- This is a strong candidate for lower-RAM relationship storage if the rewrite
  can tolerate checkpointed adjacency compaction.

## Property Patterns

### Pattern: Inline Primitive Values, Overflow Rare Values

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
- Files:
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/record/PropertyBlock.java`
  - `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/PropertyStore.java`

Source taste:

- `PropertyBlock` comments describe the first 8-byte block as carrying data,
  type, and key; following blocks carry additional data; long strings and big
  arrays point to dynamic record chains (`PropertyBlock.java:45-53`).
- Primitive values are encoded into one block when possible
  (`PropertyStore.java:463-480`).
- `long` is inlined if it fits the available bits, else uses two blocks
  (`PropertyStore.java:483-491`).
- Strings try short-string encoding first, else allocate dynamic string records
  and store a pointer block (`PropertyStore.java:510-529`).
- Key/type/value bit packing uses token bits and value shift
  (`PropertyStore.java:593-600`).

Pseudocode:

```text
encode_property(key, value):
  if primitive_fits: return one_8_byte_block
  if long_fits: return one_8_byte_block
  if short_string_fits: return one_or_more_inline_blocks
  return pointer_to_dynamic_records
```

Why it matters:

Most property values are small. A lower-RAM store should make the common case
inline and make the rare case pay indirection.

Rust translation:

- Define a compact `PropertyBlock([u64; N])` or variable-length block slice.
- Use a dynamic value store for long strings, arrays, maps, and large values.
- Keep property keys tokenized to small integers.

Memory implications:

- Avoids allocating a `Value` enum object for every property.
- Dynamic chains fragment if not managed carefully.

Concurrency implications:

- Updating a property may rewrite a block or allocate/free overflow chains.

Testing implications:

- Golden tests for each type's inline/dynamic boundary.
- Fuzz property key/type/value bit layouts.

Agent guidance:

- Do not start with `HashMap<String, Value>` on each node. That is a query-layer
  convenience, not a low-RAM storage representation.

### Pattern: Encoded Flat Property Buffer With Small Buffer Optimization

Where found:

- Language: C++
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/memgraph-src`
- Files:
  - `src/storage/v2/property_store.hpp`
  - `src/storage/v2/property_store.cpp`

Source taste:

- Memgraph comments state `PropertyStore` uses about 10x less memory than a
  map, trading O(log n) operations for O(n) scans (`property_store.cpp:63-118`).
- Each mapping begins with one metadata byte: type bits, property ID size bits,
  and payload-size bits (`property_store.cpp:100-118`).
- Integers are encoded into 1, 2, 4, or 8 bytes depending on value
  (`property_store.cpp:112-113`).
- The object stores a 12-byte buffer field (`property_store.hpp:181-189`).
- Small buffer optimization uses the pointer+size memory as local data and
  stores mode markers in low bits/bytes (`property_store.cpp:2227-2326`).
- Optional compression is skipped if compressed data plus metadata is not
  smaller (`property_store.cpp:2359-2397`).

Pseudocode:

```text
property_entry = metadata_byte + encoded_property_id + encoded_payload

if encoded_size < local_capacity:
  store in object-local buffer
else:
  allocate external byte buffer
  optionally compress if smaller
```

Why it matters:

This is a direct answer to property RAM bloat. The price is linear property
access, which may be acceptable if property counts per entity are small and
indexes cover selective lookups.

Rust translation:

- Use `SmallVec<[u8; 11]>` or a custom 12/16-byte inline buffer.
- Store sorted property IDs if binary search becomes worthwhile.
- Make compression a policy, not the default.

Memory implications:

- Excellent for low property counts and primitive-heavy data.
- Bad for entities with many frequently-read properties unless indexed/cached.

Concurrency implications:

- Whole-buffer rewrite on set/remove is simple but copy-heavy under MVCC.

Testing implications:

- Round-trip every property type.
- Test endian assumptions if serialized.
- Test compression threshold behavior.

Agent guidance:

- This is one of the highest-value patterns for lower RAM. Use it before
  inventing exotic graph compression.

## Label And Type Patterns

### Pattern: Inline Label Field With Dynamic Overflow

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
- File:
  `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/NodeLabelsField.java`

Source taste:

- Each node has a 5-byte label field. Labels are inlined if they fit; otherwise
  the field points to dynamic label records (`NodeLabelsField.java:30-45`).
- Parsing distinguishes inline and dynamic fields (`NodeLabelsField.java:49-53`).
- The high bit marks a dynamic pointer and the remaining body is a 40-bit mask
  (`NodeLabelsField.java:97-113`).
- `getNoEnsureHeavy` can load dynamic labels without making the node record
  heavy (`NodeLabelsField.java:56-80`).

Pseudocode:

```text
if label_field.high_bit == 0:
  labels = decode_inline(label_field)
else:
  labels = read_dynamic_label_records(label_field.body)
```

Why it matters:

Most nodes have few labels. The label field should be near-free in the common
case and pay dynamic storage only for unusual nodes.

Rust translation:

- Use `InlineLabels(u64)` with one dynamic marker bit and a body mask/reference.
- Keep label token IDs small and sorted for inline encoding.

Memory implications:

- Saves a side allocation per node.

Concurrency implications:

- Adding a label can switch inline to dynamic. This transition needs atomicity
  in the storage transaction.

Testing implications:

- Test every label-count boundary.
- Test dynamic labels can be read lazily without loading the whole node.

Agent guidance:

- Treat labels like hot storage metadata, not as a general property list.

### Pattern: Labels And Relationship Types As Sparse Matrices

Where found:

- Language: C
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redisgraph-src`
- File: `src/graph/graph.h`

Source taste:

- The graph stores label matrices, a node-label matrix, relation matrices, and a
  global adjacency matrix (`graph.h:49-62`).
- Procedures and bulk insert paths explicitly access label/relation matrices
  (`bulk_insert.c` search hits around label/relation matrix synchronization).

Pseudocode:

```text
label_matrix[label_id][node_id, node_id] = true
relation_matrix[type_id][src_id, dst_id] = edge_id_or_true
node_labels[node_id, label_id] = true
```

Why it matters:

Sparse matrices make label/type scans and algebraic joins efficient. They are
also a natural projection for graph pattern matching.

Rust translation:

- Store label/type matrix indexes outside the canonical record store.
- Consider roaring bitmaps for labels before full sparse matrices if operations
  are mostly set membership.

Memory implications:

- Duplicates adjacency/type information if primary records are retained.
- Can be compact for sparse labels and relation types.

Concurrency implications:

- Needs delta or snapshot discipline to stay consistent with primary writes.

Testing implications:

- Validate matrix indexes against record-store truth after every mutation class.

Agent guidance:

- Useful as an index/projection layer; risky as the only source of truth for a
  property graph with relationship identity and properties.

## Dense/Sparse State Patterns

### Pattern: Importer Count Cache With Small Counts And Rare Big Counts

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
- File:
  `community/record-storage-engine/src/main/java/org/neo4j/internal/batchimport/cache/NodeRelationshipCache.java`

Source taste:

- Count fields use bit masks and group-entry sizes (`NodeRelationshipCache.java:80-86`).
- The constructor takes a dense-node threshold and keeps a big-count side array
  (`NodeRelationshipCache.java:104-119`).
- Comments describe a 29-bit small count and a big-count bit that points into a
  shared big-count array (`NodeRelationshipCache.java:141-168`).
- Counts above the small maximum use the side array (`NodeRelationshipCache.java:173-190`).
- The cache avoids clearing huge arrays by toggling scan direction/sense and
  reuses relationship group cache for dense nodes (`NodeRelationshipCache.java:438-481`).

Pseudocode:

```text
if count <= MAX_SMALL:
  store count in node_slot
else:
  store BIG_FLAG | big_counts_index
  big_counts[index] = count
```

Why it matters:

Batch import must handle billions of relationships without allocating worst-case
degree counters for every node.

Rust translation:

- Use packed counters with overflow side arrays.
- Avoid clearing large vectors during multi-pass import; use epoch/sense bits.

Memory implications:

- Huge savings when almost all nodes have small degree.

Concurrency implications:

- Parallel import needs atomic or partitioned updates to counts.

Testing implications:

- Test threshold around `MAX_SMALL`.
- Test dense-node detection from counts.
- Test repeated passes without clearing arrays.

Agent guidance:

- Lower-RAM import is a separate subsystem. Do not judge memory only after data
  has landed in final records.

### Pattern: Sparse/Dense Frontier Switching

Where found:

- Language: C++
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src`
- Files:
  - `src/include/function/gds/gds_frontier.h`
  - `src/function/gds/gds_frontier.cpp`

Source taste:

- A frontier stores iteration number, not just a boolean; unvisited is
  `UINT16_MAX` (`gds_frontier.h:16-23`).
- `SparseFrontier` uses a table-scoped hash map of offset to iteration
  (`gds_frontier.h:42-70`).
- `DenseFrontier` allocates arrays of atomic iteration values to max offset per
  table (`gds_frontier.h:97-140`, `gds_frontier.cpp:105-142`).
- `DenseSparseDynamicFrontierPair` switches to dense when sparse size exceeds a
  threshold (`gds_frontier.h:251-265`).
- Switching initializes dense arrays then copies sparse entries
  (`gds_frontier.cpp:378-388`).

Pseudocode:

```text
if frontier_state == sparse and sparse_size > threshold:
  allocate dense arrays
  fill UNVISITED
  copy sparse iteration marks
  state = dense
```

Why it matters:

The right algorithm state layout depends on active set density. Sparse hash maps
save RAM early; dense arrays win when most nodes are touched.

Rust translation:

- Use `Frontier::Sparse(HashMap<Offset, Iter>)` and
  `Frontier::Dense(Vec<AtomicU16>)`.
- Make switching explicit and measured per algorithm.

Memory implications:

- Sparse avoids `O(nodes)` state for tiny active sets.
- Dense arrays are predictable and cache-friendly once active sets grow.

Concurrency implications:

- Dense uses relaxed atomic stores/loads in Kuzu. Rust can mirror this only when
  algorithm correctness does not require stronger ordering.

Testing implications:

- Test switching preserves active nodes and iteration numbers.
- Test sparse and dense produce identical algorithm output.

Agent guidance:

- Algorithm memory is often larger than adjacency for some workloads. Budget it
  explicitly.

## Traversal Patterns

### Pattern: Cursor State Machine Over Storage Shape

Where found:

- Languages: Java, C++
- Repos:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/neo4j`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src`
- Files:
  - `RecordRelationshipTraversalCursor.java`
  - `AdjacencyCursor.java`
  - `src/include/graph/graph.h`
  - `src/include/graph/on_disk_graph.h`

Source taste:

- Neo4j relationship traversal switches between sparse chain traversal and dense
  group traversal (`RecordRelationshipTraversalCursor.java:146-244`).
- GDS cursor methods include `next`, `peek`, `remaining`, `skipUntil`,
  `advance`, and `advanceBy` (`AdjacencyCursor.java:47-90`).
- Kuzu `NbrScanState` returns chunks with neighbor node spans, selection
  vectors, and relationship property vectors (`graph.h:35-83`).
- Kuzu `OnDiskGraphNbrScanState` wraps relationship table scan iterators and
  exposes current chunks without materializing all neighbors (`on_disk_graph.h:25-90`).

Pseudocode:

```text
scan_state.next() -> fills reusable vectors
chunk = scan_state.get_chunk()
for selected row in chunk.selection:
  visit(chunk.neighbor_nodes[row], chunk.properties[row])
```

Why it matters:

Traversal APIs are where storage complexity is hidden from algorithms. Good
cursors allow compression, batching, dense/sparse switching, and property
projection without changing algorithms.

Rust translation:

- Use lending-style iterators carefully; simple explicit cursors may be easier
  than trying to encode all lifetimes in `Iterator`.
- Keep batch cursors for property scans and single-target cursors for graph
  algorithms.

Memory implications:

- Reusable cursor buffers avoid per-node allocations.
- Chunked scans amortize IO/decompression.

Concurrency implications:

- Cursors should be thread-local. Shared graph snapshots can be immutable.

Testing implications:

- Differential test cursor traversal against a materialized adjacency oracle.
- Test early termination and filter interactions.

Agent guidance:

- Design cursor APIs before optimizing storage. The cursor is the compatibility
  layer that lets storage evolve.

### Pattern: Lightweight Graph Copies For Parallel Algorithms

Where found:

- Language: C++
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src`
- File: `src/include/graph/graph.h`

Source taste:

- Kuzu's `Graph` interface says instances are not expected to be thread-safe; if
  used in parallel, each thread should receive a separate lightweight copy
  (`graph.h:120-127`).

Pseudocode:

```text
shared_snapshot = Arc<GraphData>
thread_handle = GraphView { shared_snapshot, cursor_cache, scratch }
```

Why it matters:

Parallel algorithms need independent cursor state but should not copy adjacency
or properties.

Rust translation:

- Use `Arc<ProjectedGraphData>` plus per-thread `GraphView`.
- Store cursor caches and scratch buffers in the view.

Memory implications:

- One adjacency copy, many small cursor/scratch copies.

Concurrency implications:

- Immutability of the shared snapshot is the simplest safety story.

Testing implications:

- Run algorithms with 1 thread and N threads and compare deterministic outputs.

Agent guidance:

- Avoid `&mut Graph` global state in algorithm APIs. Put mutable traversal state
  in per-worker handles.

## Graph Algorithm Patterns

### Pattern: Algorithm API Targets Traits, Not Concrete Storage

Where found:

- Language: Rust and Java
- Repos:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/petgraph-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- Files:
  - `crates/petgraph/src/algo/dijkstra.rs`
  - `crates/petgraph/src/algo/page_rank.rs`
  - `core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java`

Source taste:

- petgraph Dijkstra is generic over `IntoEdges + Visitable`; it uses a visit
  map, binary heap, and caller-supplied edge-cost closure (`dijkstra.rs:92-217`).
- petgraph PageRank is generic over `NodeCount + IntoEdges + NodeIndexable`, but
  the read implementation scans edges repeatedly (`page_rank.rs:64-108`).
- GDS adjacency interfaces decouple algorithms from compressed adjacency
  implementations (`AdjacencyList.java:25-104`).

Pseudocode:

```text
fn dijkstra<G: IntoEdges + Visitable>(graph, start, edge_cost)
```

Why it matters:

Algorithm traits let one implementation run on multiple graph representations.
But generic APIs can hide poor complexity if the required primitive is too weak.

Rust translation:

- Define separate traits by algorithm need:
  `OutNeighbors`, `InNeighbors`, `Degree`, `SortedNeighbors`,
  `WeightedNeighbors`.
- Do not force PageRank onto an API that only answers "does edge w->v exist" by
  scanning all outgoing edges.

Memory implications:

- Trait APIs permit zero-copy algorithms over compressed adjacency.
- Accidental materialization in trait adapters can erase memory wins.

Concurrency implications:

- Traits should distinguish immutable graph access from mutable algorithm state.

Testing implications:

- Run each algorithm against both primary-store adapter and CSR projection on
  small graphs.

Agent guidance:

- Make algorithm traits as strong as the algorithm needs. Weak graph traits
  create hidden quadratic scans.

### Pattern: GraphBLAS Kernels With Cached Derived Vectors

Where found:

- Languages: C and C++
- Repos:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/lagraph-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_graphblas-src`
- Files:
  - `src/utility/LAGraph_Cached_OutDegree.c`
  - `src/main/c/src/algorithms/bfs.cpp`
  - `src/main/c/src/algorithms/pr.cpp`

Source taste:

- `LAGraph_Cached_OutDegree` returns early if cached, otherwise computes a
  sparse out-degree vector by multiplying adjacency by a zero vector with a
  plus-one semiring (`LAGraph_Cached_OutDegree.c:18-21`, `:51-79`).
- Graphalytics BFS wraps `LAGr_BreadthFirstSearch` on a `LAGraph_Graph`
  (`bfs.cpp:70-82`).
- Graphalytics PageRank caches out-degree and transpose before calling
  `LAGr_PageRankGX` (`pr.cpp:47-65`).

Pseudocode:

```text
if graph.out_degree missing:
  out_degree = A mxv ones using plus_one semiring

pagerank:
  cache(out_degree)
  cache(transpose)
  run kernel
```

Why it matters:

Graph algorithms repeatedly need derived structures: degree vectors, transpose,
symmetry flags, self-edge flags. Cache them with invalidation rather than
recomputing inside every algorithm.

Rust translation:

- Add a projection metadata cache:
  `out_degree`, `in_degree`, `transpose`, `has_self_loops`, `is_symmetric`.
- Tie cache lifetime to immutable projection version.

Memory implications:

- Derived caches consume memory but prevent repeated expensive scans.
- Make caches lazy and visible in memory estimates.

Concurrency implications:

- Lazy initialization can use `OnceLock` per projection.

Testing implications:

- Test cache correctness after projection rebuilds.
- Compare cached and uncached algorithm results.

Agent guidance:

- Do not hide caches. Expose them in projection metadata and memory accounting.

## Projection And Catalog Patterns

### Pattern: Projection Memory Estimates Separate Loading Peak From Loaded Graph

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- Files:
  - `core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStoreFactory.java`
  - `core/src/main/java/org/neo4j/gds/core/huge/HugeGraph.java`

Source taste:

- `HugeGraph` documentation describes adjacency in byte pages addressable up to
  64 TiB, sorted target IDs delta-encoded as vlongs, offset arrays, sparse
  adjacency, and thread-local import chunks similar to TLABs
  (`HugeGraph.java:63-96`).
- `CSRGraphStoreFactory` estimates graph projection memory during loading,
  including loading buffers, offsets/degrees, and property arrays
  (`CSRGraphStoreFactory.java:143-217`).
- It separately estimates after-loading adjacency list and compressed adjacency
  properties (`CSRGraphStoreFactory.java:219-270`).

Pseudocode:

```text
memory_during_loading =
  builder_buffers + offsets + degrees + property_import_arrays + adjacency_pages

memory_after_loading =
  compressed_adjacency + degrees + offsets + projected_properties
```

Why it matters:

Users experience peak RSS failures during projection, not just steady-state
graph size.

Rust translation:

- Every projection builder should return:
  `estimate_peak_loading`, `estimate_steady_state`, and `estimate_scratch_per_thread`.
- Make memory estimates part of acceptance tests for lower-RAM claims.

Memory implications:

- Prevents "compact final graph, OOM during build" surprises.

Concurrency implications:

- Per-thread import buffers scale with configured concurrency.

Testing implications:

- Snapshot memory estimates for representative schemas.
- Assert peak estimate changes in PRs when builders change.

Agent guidance:

- Never claim lower RAM from final structure alone. Include construction memory.

### Pattern: Named In-Memory Projection Catalog

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- File:
  `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`

Source taste:

- The catalog is a static concurrent per-user map with event listeners
  (`GraphStoreCatalog.java:50-60`).
- Lookup checks the user's catalog, then other users if there is exactly one
  match; it errors on missing or ambiguous graph names (`GraphStoreCatalog.java:84-125`).
- Setting a graph store fires an added event with memory size
  (`GraphStoreCatalog.java:187-219`).

Pseudocode:

```text
catalog[user][graph_name] = GraphStoreWithConfig
on_add(graph): emit(memory_usage)
lookup(name): own_user || unique_other_user || error
```

Why it matters:

Projection lifecycle is a product feature and a memory safety mechanism.
Long-lived projections need names, ownership, memory accounting, and drop paths.

Rust translation:

- Use an `Arc<ProjectedGraph>` catalog keyed by user/database/name.
- Store memory estimates and actual allocation counters with each projection.

Memory implications:

- Catalog visibility prevents orphaned large projections.

Concurrency implications:

- Concurrent readers need snapshot handles; drops should wait for references.

Testing implications:

- Test ambiguity, ownership, memory accounting, and drop behavior.

Agent guidance:

- Treat projected graphs like database resources, not temporary algorithm
  objects.

### Pattern: Composite Adjacency As View, Not Merge

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- File:
  `core/src/main/java/org/neo4j/gds/core/compression/common/CompositeAdjacencyList.java`

Source taste:

- The composite stores a list of adjacency lists plus cursor factory
  (`CompositeAdjacencyList.java:34-39`).
- Degree sums across child lists (`CompositeAdjacencyList.java:96-103`).
- Cursor construction creates a cursor per child list and wraps them
  (`CompositeAdjacencyList.java:111-117`).
- Cursor reuse updates child cursors and notes old cursors may be ahead
  (`CompositeAdjacencyList.java:125-144`).
- Memory info sums pages, heap, and off-heap across children
  (`CompositeAdjacencyList.java:157-180`).

Pseudocode:

```text
degree(u) = sum(child.degree(u) for child in lists)
cursor(u) = composite(child.cursor(u) for child in lists)
```

Why it matters:

Multi-type projections need not physically merge adjacency. Views save memory
and preserve per-type compression.

Rust translation:

- Implement `CompositeAdjacency<'a>` as small vector of child adjacency handles.
- Only materialize merged/sorted neighbors when an algorithm requires global
  ordering or duplicate removal.

Memory implications:

- Avoids duplicate adjacency arrays.
- Composite cursor has per-child cursor scratch.

Concurrency implications:

- Child projections remain immutable and shareable.

Testing implications:

- Test degree sums, cursor order expectations, duplicate semantics, and memory
  accounting.

Agent guidance:

- Do not merge relationship types by default. Compose first, materialize only
  when an algorithm proves it needs merged adjacency.

## Benchmark And Oracle Patterns

### Pattern: Canonical Graph Equality Independent Of Internal IDs

Where found:

- Language: Java
- Repo: `/Users/amuldotexe/Desktop/personal-repos-lane/graph-data-science-2026.03`
- File:
  `test-utils/src/main/java/org/neo4j/gds/compat/CanonicalAdjacencyMatrix.java`

Source taste:

- Labels and properties are canonicalized and sorted (`CanonicalAdjacencyMatrix.java:42-103`).
- Graph canonicalization collects out/in adjacencies for each relationship type
  (`CanonicalAdjacencyMatrix.java:106-130`).
- Relationship strings include type and property, and final representations are
  sorted deterministically (`CanonicalAdjacencyMatrix.java:132-230`).

Pseudocode:

```text
canonical_node = sorted(labels) + sorted(properties)
canonical_edges = sorted(type, src_canonical, dst_canonical, property)
```

Why it matters:

Rewrites need an oracle that ignores incidental record IDs and adjacency order.

Rust translation:

- Build a `CanonicalGraph` test helper for primary store and projection store.
- Include labels, relationship types, and selected properties.

Memory implications:

- Test-only materialization is acceptable.

Concurrency implications:

- Canonicalization should run against a stable snapshot.

Testing implications:

- Use after import, mutation replay, projection, compaction, and recovery tests.

Agent guidance:

- Before optimizing memory, build canonical equality. It prevents invisible
  semantic drift.

### Pattern: Benchmark Output Maps Dense IDs Back To Original IDs

Where found:

- Language: C++
- Repo:
  `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_graphblas-src`
- Files:
  - `src/main/c/src/graphio.cpp`
  - `src/main/c/src/algorithms/bfs.cpp`
  - `src/main/c/src/algorithms/pr.cpp`

Source taste:

- GraphBLAS platform reads MatrixMarket or binary graph matrix and materializes
  it (`graphio.cpp:8-37`).
- It reads a vertex mapping from `.vtb` or `.vtx` files (`graphio.cpp:40-65`).
- BFS finds the compact source vertex by searching the mapping for the original
  source ID (`bfs.cpp:94-103`).
- BFS serialization writes every original vertex ID and uses infinity for
  unreachable vertices (`bfs.cpp:47-64`).
- PageRank serialization writes original IDs with scientific precision
  (`pr.cpp:17-45`).

Pseudocode:

```text
matrix_id = position(mapping, original_source_id)
result = run_algorithm(matrix_id)
for dense_id in 0..n:
  write(mapping[dense_id], result[dense_id] or infinity)
```

Why it matters:

Algorithm projections can reorder/remap IDs internally, but benchmark outputs
must be stable and user-visible.

Rust translation:

- Every projection should retain an ID mapper.
- Algorithm result serializers should operate at the boundary, not inside hot
  loops.

Memory implications:

- Mapping vector is required projection overhead.

Concurrency implications:

- Immutable mapping can be shared with algorithm results.

Testing implications:

- Test missing source ID.
- Test unreachable output conventions.
- Test round-trip mapping after compaction.

Agent guidance:

- Never let benchmark code accidentally validate dense internal IDs as external
  IDs.

### Pattern: Validation Rules Are Algorithm-Specific

Where found:

- Languages: Java and Python
- Repo:
  `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics-src`
- Files:
  - `graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/MatchLongValidationRule.java`
  - `graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EpsilonValidationRule.java`
  - `graphalytics-validation/src/main/java/science/atlarge/graphalytics/validation/algorithms/bfs/BreadthFirstSearchValidationTest.java`
  - `bin/utils/compare-output/check.py`

Source taste:

- Integral outputs use exact matching (`MatchLongValidationRule.java:20-35`).
- Floating outputs use a 0.01 percent epsilon and special infinity handling
  (`EpsilonValidationRule.java:20-40`).
- BFS validation runs directed and undirected sample graphs and compares result
  vertices and per-vertex path lengths (`BreadthFirstSearchValidationTest.java:70-151`).
- The Python compare utility uses exact checks for BFS/CDLP, threshold checks
  for PR/LCC/SSSP, and equivalence-class checks for WCC (`check.py:73-139`).

Pseudocode:

```text
validator(algorithm):
  bfs, cdlp -> exact integer
  pr, lcc, sssp -> floating threshold
  wcc -> component equivalence mapping
```

Why it matters:

One oracle shape does not fit all graph algorithms. WCC component IDs can differ
while partitions are equivalent; PageRank floats need tolerance; BFS levels need
exactness.

Rust translation:

- Create algorithm-specific result validators.
- Include Graphalytics-style tiny fixtures in CI.

Memory implications:

- Validators can materialize outputs; they are test tools.

Concurrency implications:

- Parallel algorithms may produce nondeterministic but equivalent labels. Use
  equivalence validators where appropriate.

Testing implications:

- Add exact, epsilon, and equivalence validators before optimizing algorithms.

Agent guidance:

- Do not use "matches expected file" as a single generic benchmark oracle. The
  comparison rule is part of the algorithm contract.

## Cross-Cutting Rust Rewrite Guidance

1. Separate primary store and projection store.
   Neo4j records, Memgraph vertices, Kuzu CSR node groups, and RedisGraph
   matrices are not interchangeable. Use each where its tradeoff fits.

2. Make memory a contract.
   Track record byte sizes, projection steady-state memory, projection peak
   loading memory, and algorithm scratch memory. Add tests for estimates.

3. Use typed IDs everywhere.
   Store IDs, table offsets, projection IDs, relationship IDs, property keys,
   label tokens, and row IDs should be distinct Rust types.

4. Inline common small values.
   Labels, primitive properties, short strings, small counts, and low-degree
   adjacency all deserve compact inline forms.

5. Push rare cases sideways.
   Dynamic labels, dynamic properties, big counts, explicit CSR row indices,
   compressed property buffers, and delta matrices should be side structures.

6. Prefer cursor APIs over materialization.
   Traversal should be expressed through reusable cursors and chunks. Algorithms
   should not force `Vec` allocation per node.

7. Keep algorithm state density-aware.
   Sparse and dense frontiers are both first-class. Switch only with explicit
   thresholds and tests.

8. Treat generated compression code as normal infrastructure.
   Generated bit packers are appropriate when paired with scalar oracles and
   fuzz tests.

9. Build independent oracles first.
   Canonical graph equality and Graphalytics-style validators are prerequisites
   for safe memory-oriented rewrites.

## Explicit Gaps And Uninspected Repos

The following repositories or areas were not deeply inspected in this slice:

- `/Users/amuldotexe/Desktop/oss-read-only/omnigraph`
- `gitrefrepo/dgraph-src`
- `gitrefrepo/janusgraph-src`
- `gitrefrepo/apache-tinkerpop-src`
- `gitrefrepo/apache-jena-src`
- `gitrefrepo/age-src`
- `gitrefrepo/arangodb-src`
- `gitrefrepo/arcadedb-src`
- `gitrefrepo/cayley-src`
- `gitrefrepo/raphtory-src`
- `gitrefrepo/typedb-src`
- `gitrefrepo/orientdb-src`
- `gitrefrepo/terminusdb-src`
- `gitrefrepo/igraph-src`
- `gitrefrepo/networkit-src`
- `gitrefrepo/ligra-src`
- `gitrefrepo/gunrock-src`
- `gitrefrepo/graphchi-cpp-src`
- `gitrefrepo/graphscope-src`
- Most of `gitrefrepo/graphblas-src` beyond LAGraph and Graphalytics platform use.
- Neo4j kernel areas outside record storage and GDS projection/catalog utilities.
- Full FalkorDB delta-matrix implementation beyond search-level orientation and
  RedisGraph lineage reads.

Tool limitation:

- CodeGraphContext indexing of `petgraph-src` was attempted but stopped due
  runtime cost. Direct source reads were used instead.

Recommended next slices:

- Dgraph/Badger/Raft storage and posting-list patterns.
- JanusGraph/TinkerPop/Jena/TypeDB schema and query-planning patterns.
- GraphChi/Ligra/Gunrock/GraphScope out-of-core and accelerator algorithm
  patterns.
- OmniGraph-specific architecture if it is intended as a local reference for the
  rewrite.

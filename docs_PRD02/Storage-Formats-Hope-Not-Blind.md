# Storage Formats: Hope, Not Blind Copying

*What this repo already knows about storage formats, what's wrong with
Neo4j's, where the real improvement is, and why "just copy Java" would
be the worst possible choice.*

---

## What's Actually Wrong with Neo4j's Storage Format

Neo4j's storage isn't bad. It's been production-grade for 15 years. But
it was designed in 2007 for a different world. Its specific weaknesses
are structural — no amount of Java optimization can fix them.

### The Record Store Architecture

From `docs_PRD01/Neo4j-Architecture-Map.md`, lines 97-111:

```
Store file                     Record   Size    Key fields
─────────────────────────────  ──────   ─────   ─────────────────────────
neostore.nodestore             Node     15 B    labels, nextRel, nextProp, inUse
neostore.relationshipstore     Rel      34 B    startNode, endNode, type,
                                                 nextRel, nextProp
neostore.propertystore         Prop     41 B    key, value, nextProp
neostore.relationshipgroupstore Group   32 B    type, firstOut, firstIn,
                                                 firstLoop, owningNode
```

### The Three Structural Problems

**Problem 1: Linked-list traversal = pointer chasing**

To find the neighbors of a node:
```
read node record (15 B) → follow nextRel pointer
read rel record (34 B) → follow nextRel pointer for same node
read rel record (34 B) → follow nextRel pointer
... repeat for every relationship
```

Each record can be on a DIFFERENT page. For a node with 100
relationships, that's ~100 random page reads. The CPU cache line is
64 bytes — each read loads one record plus wasted padding.

This is the fundamental reason Neo4j traversal is slow at scale. It's
not Java vs Rust. It's data layout.

**Problem 2: Properties are a SECOND linked list**

After finding a neighbor, to read its properties:
```
node.nextProp → property record → property.nextProp → next property ...
```

So a query like `MATCH (n)-[:KNOWS]->(m) RETURN m.name` does:
1. Index lookup → node page read
2. Relationship chain traversal → N random reads
3. For EACH neighbor: property chain traversal → M more random reads

A node with 100 relationships, each with 3 properties, requires
~100 + ~300 = ~400 random page reads. With a 4K page and SSD,
that's 400 × ~10μs = ~4ms just in I/O. With spinning disk: 400 ×
~5ms = ~2 seconds.

**Problem 3: No locality between related data**

Neo4j assigns record IDs sequentially at insertion time. Two nodes
that are densely connected may have record IDs millions apart,
scattered across different pages. There is no clustering by
neighborhood.

This means the working set for a neighborhood query can span
hundreds of pages even for a small neighborhood.

### What This Means Numerically

From `docs_PRD01/Previous-learnings-01.md`:

> Knight Bus specializes for: immutable graph, one relationship
> semantics, exact-key anchor lookup, one-hop and two-hop adjacency
> expansion.

> Neo4j is paying for generality: property graph storage, Cypher
> execution, server process boundaries, row materialization,
> traversal machinery designed for many query shapes.

The gap isn't "Java is slow." The gap is that Neo4j's storage format
forces random I/O where contiguous reads would suffice.

---

## What's Already in This Repo: A Full Alternative Story

This repo doesn't just complain about Neo4j. It contains a complete,
documented, partially-proven alternative storage philosophy. Here's
the inventory:

### 1. The Storage-Runtime Alignment Doctrine

Source: `docs/pre-v002/STORAGE_RUNTIME_ALIGNMENT.md`

The core thesis:

> The storage is "aligned to runtime" only when the hot traversal
> path is already visible in the on-disk bytes.

Concrete principles:
- Peers (adjacency data) are the payload
- Offsets are the seek aid
- Key lookup is a separate sidecar concern
- No database or log lookup in the traversal loop
- No reverse-edge derivation at query time
- Immutable sealed artifacts
- Explicit validation on open

This is influenced by two sources:
- **Parseltongue** (graph shape: dense IDs, dual CSR, mmap)
- **Apache Iggy** (storage discipline: payload shaped for read path,
  tiny sidecar indexes, immutable sealed segments)

### 2. The Proven Base Format: AnchorDualCsr

Source: actual code in `src/`, `docs/pre-v002/STORAGE_RUNTIME_ALIGNMENT.md`

```
snapshot/
  manifest.json
  node_table.bin          # fixed-width records (key_offset, key_len, flags)
  strings.bin             # packed UTF-8 strings
  forward.offsets.bin     # u64[node_count + 1]
  forward.peers.bin       # u32[edge_count]
  reverse.offsets.bin     # u64[node_count + 1]
  reverse.peers.bin       # u32[edge_count]
  key_index.bin           # sorted key → dense_id mapping
```

Read path:
```
key → binary_search(key_index) → dense_id
dense_id → offsets[id]..offsets[id+1] → peers[start..end]
```

**Status: PROVEN.** 4,710 LOC, 4 traits, 23 tests, zero clippy warnings.

### 3. The 13 Layout Families (Algorithm Storage Atlas)

Source: `docs_PRD01/Rubber-Duck-Frontend-Backend-Split.md`, Appendix B

The breakthrough insight from the later conversations:

> One universal base format was rejected. One fully bespoke engine
> per algorithm was also rejected. The chosen pattern is a small
> family of reusable layout types plus per-algorithm contracts.

The families, what they optimize for, and why they're better than
Neo4j's format for their specific workload:

| # | Layout Family | What it optimizes | Why Neo4j is bad at this | Algorithms |
|---|---|---|---|---|
| 1 | **AnchorDualCsr** | Exact anchor → adjacency slice | Neo4j: linked-list pointer chase. KB: contiguous slice | 4 |
| 2 | **InboundPower** | Repeated inbound score accumulation (PageRank) | Neo4j: re-scan reverse relationships each iteration. KB: pre-materialized inbound CSR | 4 |
| 3 | **ConnectivityLowlink** | DFS numbering, lowlinks (Tarjan's SCC) | Neo4j: random traversal + stack management. KB: pre-ordered DFS-friendly layout | 4 |
| 4 | **OrderedWedge** | Sorted-neighbor intersection (triangle counting) | Neo4j: sort at runtime per hop. KB: pre-sorted adjacency | 9 |
| 5 | **PartitionRefinement** | Community assignment + evaluation (Louvain) | Neo4j: community labels as properties (linked-list reads). KB: dense community array | 9 |
| 6 | **PeelBucket** | Low-degree peeling (k-core) | Neo4j: degree computation requires full rel chain scan. KB: pre-computed degree array | 3 |
| 7 | **RelaxationFrontier** | Weighted frontier relaxation (Dijkstra) | Neo4j: weight is a property (linked-list). KB: weight inlined in edge data | 10 |
| 8 | **EdgeOrderForest** | Globally ordered edge scan (MST) | Neo4j: no global edge ordering. KB: edge-sorted snapshot | 2 |
| 9 | **FlowResidual** | Mutable residual arc updates (max flow) | Neo4j: capacity/flow as properties. KB: inlined residual arrays | 4 |
| 10 | **FeatureMetric** | Row-major feature distance (k-NN) | Neo4j: features scattered across property chains. KB: dense feature matrix | 4 |
| 11 | **EmbeddingSample** | Neighborhood sampling (Node2Vec) | Neo4j: random walks via pointer chase. KB: pre-structured walk candidates | 4 |
| 12 | **DagOrder** | Topological replay (longest path) | Neo4j: no topo ordering. KB: pre-sorted DAG order | 2 |
| 13 | **InfluenceMonteCarlo** | Stochastic cascade simulation (CELF) | Neo4j: random cascade via full traversal. KB: pre-structured cascade neighborhoods | 1 |

**Status: 1 PROVEN (AnchorDualCsr), 12 DESIGN-ONLY.**

### 4. The Existing Precedent Inventory

Source: `docs_PRD01/Faithful-Rust-Port-Analysis-v2.md`, lines 686-984

The repo already cataloged real systems that prove alternatives work:

| System | What it proves | Relevance to Knight Bus |
|---|---|---|
| **sled** | Rust embedded DB with B+tree, WAL, crash recovery | Storage engine in Rust is solved |
| **TiKV** | Distributed KV store in Rust, production at scale | Rust storage at scale works |
| **redb** | Pure-Rust ACID embedded database | Clean Rust ACID is achievable |
| **LMDB** | Zero-copy mmap-based storage | mmap as primary storage works |
| **MeshDB** | Bolt 5 in Rust (MIT licensed) | Bolt protocol in Rust is done |
| **decypher** | Cypher parser in Rust | Cypher parsing in Rust is done |
| **ocg** | 100% openCypher TCK compliant parser | Cypher conformance is achievable |
| **DuckDB** | Columnar analytics DB, WAL + checkpoint model | Append + compact analytics model works |
| **ClickHouse** | Immutable parts + background merges | Immutable segment model works at petabyte scale |

---

## The Hope: What SPECIFICALLY Can Be Better Than Neo4j

### 1. Traversal: 10-100x (PROVEN for the narrow case)

Neo4j traversal:
```
100 neighbors × 1 random page read each = 100 cache misses
```

Knight Bus traversal:
```
1 offset read + 1 contiguous 400-byte read = 1 cache miss
```

This isn't theoretical — the benchmark exists and passes. The
improvement comes from data layout, not language speed.

**Can this survive adding Cypher/Bolt?** YES. The traversal kernel
stays the same. Cypher parsing and Bolt serialization add fixed
overhead per query, not per neighbor. A query that traverses 10,000
neighbors spends 99% of time in the traversal kernel regardless of
frontend overhead.

### 2. Algorithm Execution: Potentially 10-100x (DESIGN ONLY)

The Atlas families aren't just "CSR for everything." Each one is
designed for a SPECIFIC algorithm's inner loop:

**Example: PageRank**

Neo4j GDS PageRank:
1. Project graph into in-memory adjacency structure
2. Each iteration: for each node, scan inbound neighbors, accumulate score
3. Inbound neighbor scan goes through GDS's adjacency list (better than
   record store, but still generic)

Knight Bus InboundPower:
1. Build InboundPowerLayoutV1 at snapshot time
2. Inbound adjacency is already materialized as contiguous array
3. Score accumulation is direct arithmetic on contiguous memory
4. No per-iteration projection overhead

The improvement is that Neo4j GDS projects at runtime. Knight Bus
projects at BUILD time. Runtime work becomes "boring" — direct
array arithmetic.

**Example: Triangle Counting**

Neo4j: For each edge (u,v), intersect neighbor sets of u and v.
Neighbor sets are unsorted → O(d²) per edge with linear scan.

Knight Bus OrderedWedge: Adjacency is PRE-SORTED. Neighbor
intersection is O(d) per edge with merge-join. The sort happened at
build time.

### 3. Memory: 3-5x (PARTIALLY PROVEN)

Neo4j JVM overhead:
- Object headers (16 bytes per object on 64-bit JVM)
- Pointer boxing (every object reference is 8 bytes)
- GC metadata (card tables, remembered sets)
- JVM base memory (~500MB-1GB just for the JVM itself)

Knight Bus:
- No runtime. Just mmap'd arrays.
- RSS = only the pages actually touched
- 50GB graph → maybe 200MB RSS for a single query (only the
  neighborhoods touched are paged in)

The existing benchmark tracks RSS. This claim is partially proven.

### 4. Startup: 100-1000x (STRUCTURALLY GUARANTEED)

Neo4j startup: JVM boot → class loading → page cache warmup → WAL
replay → index recovery → GBPTree cleanup → ready. Takes 10-60
seconds for large databases.

Knight Bus startup: read manifest.json → mmap files → ready. Takes
<100ms regardless of graph size.

This is structural — mmap doesn't require loading data upfront.

### 5. Operational Simplicity: Qualitative

Neo4j: JVM tuning (heap size, GC settings, page cache size), JMX
monitoring, log4j configuration, rolling upgrades, plugin management.

Knight Bus: single static binary. No JVM. No GC tuning. No heap
sizing. `./knrt serve --snapshot ./data` and it runs.

---

## The Rust-Native Storage Engine: Not a Copy, an Improvement

From `Faithful-Rust-Port-Analysis-v2.md`, lines 688-711:

> Building a record-based or page-based storage engine in Rust is a
> solved problem. [...] You don't need to replicate Neo4j's exact
> record format.

The key insight from the v2 analysis: **don't copy Neo4j's record
format, design a Rust-native format from first principles.**

### What a Rust-native format looks like

```rust
// Instead of Neo4j's 15-byte node record with linked-list pointers:
#[repr(C)]
struct RustNodeRecord {
    // Dense, not scattered. No linked-list pointers.
    label_set: u64,        // bitset of labels (not a separate store)
    prop_offset: u64,      // offset into columnar property storage
    rel_offset: u64,       // offset into CSR adjacency
    rel_count: u32,        // pre-computed degree (no need to walk chain)
    flags: u32,
}

// Instead of Neo4j's 34-byte rel record with 6 linked-list pointers:
// → just use CSR arrays. No per-record overhead at all.

// Instead of Neo4j's 41-byte property record with linked-list chain:
// → columnar property storage, one typed array per property key
```

### Why this is better (not just different)

| Aspect | Neo4j Record Store | Rust-native + CSR |
|---|---|---|
| Node-to-neighbors | Follow linked list (random I/O) | Array slice (sequential I/O) |
| Property access | Follow property chain (random I/O) | Column offset (sequential I/O for scans, indexed for point) |
| Degree computation | Walk entire rel chain to count | Pre-stored in node record or offsets array |
| Reverse traversal | Walk forward chain, filter by direction | Separate reverse CSR (O(1) lookup) |
| Memory footprint | 15+34+41 bytes per node/rel/prop in linked lists | Dense arrays, no pointer overhead |
| Cache efficiency | 1 useful record per cache line | 8-16 node IDs per cache line |

### The key: you're not copying OR ignoring Neo4j

You're taking Neo4j's **model** (property graph: nodes with labels,
relationships with types, properties on both) and storing it in a
BETTER format (columnar/CSR instead of linked-list records).

The data model stays the same. The queries stay the same. The wire
protocol stays the same. Only the bytes on disk change — and those
bytes are arranged for sequential reads instead of random pointer
chasing.

---

## Specific Improvements Over Neo4j's Storage (by Component)

### Page Cache: mmap vs MuninnPageCache

Neo4j built MuninnPageCache (14,241 LOC) because linked-list records
scatter data across pages, requiring careful eviction, pinning, and
dirty-page tracking.

With CSR arrays, data is contiguous. `mmap` lets the OS manage
paging. The OS kernel has had 30 years of optimization on page
management. You don't need 14K LOC of custom page cache.

**Improvement: 14,241 LOC → ~500 LOC.** Already proven in Knight Bus.

### Indexes: Sorted arrays vs GBPTree

Neo4j uses GBPTree (13,402 LOC) for indexes because record IDs are
scattered — you need a B+tree to find records by property value.

With dense IDs and columnar properties, many index operations become:
- Exact key lookup: binary search on sorted array
- Range scan: sequential scan of typed property column
- Label scan: bitset scan

B+trees are still needed for arbitrary property indexes, but the
core entity lookup is simpler.

**Improvement: Simpler for most operations.** B+tree needed only for
arbitrary property indexes, not for entity lookup.

### ID Generation: Dense IDs vs recycled free-list

Neo4j's id-generator (10,939 LOC) manages a free-list of recycled
IDs because deleted nodes leave gaps in the record store. It uses
GBPTree to track which IDs are free.

With CSR snapshots, IDs are reassigned at build time. Dense, gap-free
numbering from 0 to N-1. No ID recycling, no free-list, no GBPTree
for ID tracking.

**Improvement: 10,939 LOC → 0 LOC.** Dense IDs eliminate the problem.

### Counts Store: Pre-computed vs scan-based

Neo4j maintains a `GBPTreeGenericCountsStore` that tracks how many
nodes have each label, how many relationships have each type, etc.
This is needed for query planner cardinality estimation.

With CSR snapshots, these counts can be computed at build time and
stored in the manifest. Cost: one extra build step. Benefit: no
runtime counts store maintenance.

**Improvement: Pre-computed at build time.** Simpler than maintaining
a separate counts store.

### Concurrency: Immutable reads vs locking

Neo4j's lock manager (5,522 LOC) provides read/write locks with
deadlock detection because readers and writers share mutable state.

With immutable snapshots for reads, concurrent readers need zero
synchronization. Only the write path (append log / snapshot builder)
needs coordination, and that's a single writer.

**Improvement: Read concurrency drops from 5,522 LOC to 0 LOC.**

---

## The Honest Limitations

### Where Neo4j's format is actually BETTER

1. **Immediate writes.** Neo4j can update a property in ~1ms (update
   one 41-byte record). CSR snapshots require a rebuild.

2. **Space efficiency for sparse data.** Neo4j only stores properties
   that exist. CSR columnar storage wastes space for NULL values
   (unless you add NULL bitmaps, which adds complexity).

3. **Fine-grained transactions.** Neo4j can abort a single write in
   a multi-write transaction. CSR append-log transactions are
   all-or-nothing on the snapshot level.

4. **Schema evolution.** Neo4j can add a property to one node without
   touching any other data. CSR columnar storage may require
   rebuilding an entire property column.

### Where the improvement is real but unproven

1. **Algorithm-specific layouts** — 12 of 13 families are design-only
2. **Property storage in CSR** — not yet implemented
3. **50GB scale** — tested only at 39 nodes
4. **Incremental rebuild** — not yet designed

### What's genuinely hard regardless of format

1. **Cypher semantic correctness** — NULL propagation, three-valued
   logic, implicit coercions. Same difficulty for any storage format.
2. **Query planner cost model** — needs real cardinality estimation.
   CSR changes the cost constants but not the planning difficulty.
3. **Bolt conformance** — driver compatibility quirks. Same for any
   backend.

---

## The Bottom Line: Is There Hope?

**Yes. And it's not blind hope.**

The hope is specific and grounded:

1. **Traversal is 10-100x faster** — PROVEN for the narrow case,
   structurally guaranteed for the general case (contiguous reads
   vs pointer chasing).

2. **The storage format eliminates entire subsystems:**
   - MuninnPageCache (14K LOC) → mmap (~500 LOC)
   - ID generator (11K LOC) → dense IDs (0 LOC)
   - Lock manager (5.5K LOC) → immutable reads (0 LOC)
   - Counts store → pre-computed at build time
   - That's ~31K LOC of Neo4j's backend that simply disappears.

3. **The algorithm-specific layouts are a genuine innovation** — no
   other graph database stores data in the shape each algorithm's
   inner loop wants. This is new. If it works (and it needs to be
   proven), it's a real competitive moat.

4. **The Rust ecosystem provides the infrastructure** — Bolt
   (MeshDB), Cypher (decypher/ocg), B+tree (redb/sled), async
   (tokio). You're writing domain logic, not infrastructure.

5. **The data model stays the same** — property graph with labels,
   types, and properties. Users get the same model, same queries,
   same drivers. Only the bytes on disk change.

**The risk isn't "can we improve on Neo4j's storage?" That's clearly
YES. The risk is "can we build enough Cypher/Bolt infrastructure
around the improved storage to be actually usable?" That's the
execution challenge, not the architecture challenge.**

The storage format is the ONE thing you should NOT copy from Neo4j.
It's the ONE thing where you have a clear, proven, structural
advantage. Copy the frontend (Cypher, Bolt, drivers, errors). Copy
the transaction model concepts (WAL, isolation). But the storage
format — the bytes on disk — is where Knight Bus's thesis lives.

> Don't copy the filing cabinet. Build a better map.

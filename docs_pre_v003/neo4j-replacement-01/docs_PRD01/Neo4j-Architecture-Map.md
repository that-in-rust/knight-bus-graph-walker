# Neo4j Architecture Map

Focused reference for the Rust replacement project. Covers the subsystems
relevant to graph storage, traversal, and query execution.

Wiki: https://app.devin.ai/wiki/neo4j/neo4j

---

## High-Level Stack

```
┌─────────────────────────────────────────────────┐
│  Network & API Layer                            │
│  ├── Bolt Protocol  (community/bolt)            │
│  ├── HTTP API       (community/server)          │
│  └── Cypher Shell   (community/cypher-shell)    │
├─────────────────────────────────────────────────┤
│  Query Engine — Cypher  (community/cypher/)      │
│  ├── Front-End  (Parser → AST → Semantic Check) │
│  ├── Planner    (Logical Plan → Cost-Based Opt) │
│  └── Runtime    (Interpreted / Slotted pipes)   │
├─────────────────────────────────────────────────┤
│  Database Management                             │
│  ├── DatabaseManagementService                   │
│  ├── GlobalModule     (PageCache, JobScheduler)  │
│  └── CommunityEditionModule                      │
├─────────────────────────────────────────────────┤
│  Kernel & Transaction Layer (community/kernel)   │
│  ├── KernelTransaction                           │
│  ├── Cursors (NodeCursor, RelCursor, etc.)       │
│  └── Operations (Write, SchemaWrite)             │
├─────────────────────────────────────────────────┤
│  Storage Engine  (community/record-storage-engine)│
│  ├── RecordStorageEngine                          │
│  ├── NeoStores (NodeStore, RelStore, PropStore)   │
│  └── MetaDataStore                                │
├─────────────────────────────────────────────────┤
│  Support Subsystems                              │
│  ├── Page Cache — Muninn  (community/io)         │
│  ├── ID Generation        (community/id-generator)│
│  ├── Indexing — GBPTree   (community/index)      │
│  ├── WAL / Transaction Log (community/wal)       │
│  └── Counts Store         (community/storage-engine-util)│
└─────────────────────────────────────────────────┘
```

---

## Module Inventory (community/)

### Core — must understand for replacement

| Module | What it does |
|---|---|
| `kernel` | Core database engine: transactions, cursors, operations, recovery |
| `kernel-api` | Public SPI: `StorageEngine`, `StorageReader`, `CommandCreationContext` |
| `record-storage-engine` | Default storage: fixed-size records for nodes/rels/props, batch import |
| `storage-engine-util` | Counts store (`GBPTreeGenericCountsStore`), shared storage utilities |
| `io` | Page cache (Muninn), `PageCursor`, file I/O abstraction |
| `index` | `GBPTree` — B+tree used for indexes, counts, ID tracking |
| `wal` | Write-ahead log for durability and recovery |
| `lock` | Lock manager for transaction concurrency |

### Query — understand the overhead Knight Bus avoids

| Module | What it does |
|---|---|
| `cypher` | Full Cypher engine assembly |
| `cypher/front-end` | Parser, AST, semantic analysis, rewriting |
| `cypher/cypher-planner` | Logical planning, cost-based optimization, IDP solver |
| `cypher/cypher-logical-plans` | Plan tree representation, eagerness analysis |
| `cypher/runtime-spec-suite` | Interpreted + slotted pipe execution, slot allocation |
| `bolt` | Bolt wire protocol — serialization, session state machine |

### Supporting — lower priority for replacement

| Module | What it does |
|---|---|
| `id-generator` | Recycles freed entity IDs via GBPTree or scan |
| `lucene-index` | Lucene-backed full-text indexes |
| `fulltext-index` | Full-text search integration |
| `spatial-index` | Spatial indexing (R-tree style) |
| `graph-algo` | Classic graph algorithms (shortest path, Dijkstra) |
| `consistency-check` | Validates structural integrity of record stores |
| `import-tool` / `import-util` | CSV/batch import pipeline |
| `fabric` | Federated query routing across databases |

---

## Record Storage Engine — Deep Dive

This is the heart of what Knight Bus replaces for traversal workloads.

### Physical Layout

Neo4j stores graph data in **fixed-size records** across multiple store files:

| Store file | Record type | Record size | Key fields |
|---|---|---|---|
| `neostore.nodestore` | Node | 15 bytes | `labels`, `nextRel`, `nextProp`, `inUse` |
| `neostore.relationshipstore` | Relationship | 34 bytes | `startNode`, `endNode`, `type`, `nextRel`, `nextProp` |
| `neostore.propertystore` | Property | 41 bytes | `key`, `value`, `nextProp` |
| `neostore.relationshipgroupstore` | Rel Group | 32 bytes | `type`, `firstOut`, `firstIn`, `firstLoop`, `owningNode` |

Nodes point to a **doubly-linked list of relationships**. Relationship records
chain to the next relationship for both the start and end node. This means:

- **Traversal = pointer chasing** through scattered records.
- Dense nodes use RelationshipGroupStore to partition by type.
- Properties are another linked list off each node/relationship.

### How Traversal Works in Neo4j

```
query: MATCH (n {node_id: $id})-[:DEPENDS_ON]->(m)

1. Property index lookup → find node record by node_id property
2. Read node record → get firstRelationship pointer
3. Follow relationship chain:
   for each rel in doubly-linked list:
     if rel.type == DEPENDS_ON and rel.startNode == n:
       collect rel.endNode
4. For each collected endNode:
   read node record → extract node_id property
```

This involves:
- Index B-tree seek (GBPTree)
- Node record read via PageCursor
- N relationship record reads (pointer chasing through linked list)
- Property reads for each target node (another linked list)
- All through the Page Cache with locking

### What Knight Bus Does Instead

```
query: forward neighbors of key K

1. Binary search sorted key_index → dense_id         O(log n)
2. Read offsets[dense_id] and offsets[dense_id + 1]   O(1)
3. Slice peers[start..end]                            O(degree)
4. Map dense_ids back to keys via node table           O(degree)
```

No linked lists. No pointer chasing. No property decoding. No page cache
locking. Just array arithmetic over contiguous memory.

---

## Page Cache — Muninn

Neo4j's custom off-heap memory manager (`MuninnPageCache`):

- Maps store files into fixed-size pages (typically 8 KiB)
- Uses `PageCursor` abstraction for concurrent read/write
- Implements its own eviction (clock sweep)
- Handles faulting, flushing, and file growth
- **All record access goes through this layer**

Knight Bus replaces this with direct `mmap` — the OS page cache does
the work, with zero overhead from a managed page abstraction.

---

## Indexing — GBPTree

Neo4j's `GBPTree` is a general-purpose B+tree used for:

- Schema indexes (node/rel property lookups)
- ID generators (tracking free IDs)
- Counts store (graph statistics)
- Relationship group degrees

It is a **page-cache-backed, crash-safe B+tree** with:

- Structural changes via generation-based concurrency
- Checkpoint integration for durability
- Cleanup on crash recovery

Knight Bus replaces index lookups with a sorted key array + binary search,
which is simpler but sufficient for exact-key lookups.

---

## Transaction & WAL Layer

Neo4j provides full ACID transactions:

1. `KernelTransaction` manages the lifecycle
2. Changes accumulate in `TransactionState` (in-memory)
3. On commit: generate `StorageCommand` objects
4. Write commands to WAL (write-ahead log)
5. Apply commands to stores via `RecordStorageEngine.apply()`
6. Checkpoint: flush dirty pages to disk

Knight Bus operates on **immutable snapshots** — no transactions, no WAL,
no undo/redo. This eliminates the entire transaction engine from the hot path.

---

## Cypher Execution Pipeline

The overhead Knight Bus avoids:

```
Query text
  → Parser (ANTLR-based, produces AST)
  → Semantic analysis (type checking, scope resolution)
  → AST rewriting (normalization, deprecation handling)
  → Logical planning (IDP solver, cost-based optimization)
  → Physical planning (slot allocation, pipe mapping)
  → Runtime execution (interpreted or slotted pipes)
  → Result materialization (rows → Bolt values)
  → Bolt serialization → network
```

For a simple `MATCH (n {node_id: $id})-[:DEPENDS_ON]->(m)`:
- The planner must consider index strategies, join orders, eager barriers
- The runtime creates operator pipelines with state machines
- Results are materialized into rows, serialized over Bolt

Knight Bus skips all of this — the "query" is a direct function call.

---

## Replacement Strategy Summary

| Neo4j component | Knight Bus equivalent | Notes |
|---|---|---|
| Record stores (Node/Rel/Prop) | Dual CSR arrays | Forward + reverse adjacency |
| Property index | Sorted key_index | Binary search for exact-key |
| Page Cache (Muninn) | `mmap` | OS page cache, zero overhead |
| GBPTree indexes | Not needed | Only exact-key lookups |
| Transaction engine | Not needed | Immutable snapshots |
| WAL | Not needed | No mutations |
| Cypher parser/planner/runtime | Direct function calls | No query language |
| Bolt protocol | Not needed | In-process queries |
| Linked list traversal | Contiguous array slice | No pointer chasing |

---

## Variant Design Implications

### variant_low_RAM
- Aggressive `mmap` with minimal resident pages
- External merge sort during build (already in `src/low_ram.rs`)
- Smaller node table format, lazy key resolution
- Trade: slightly higher latency for much lower RSS

### variant_low_latency
- Pin hot pages / prefetch adjacency arrays
- Inline short keys in node table to avoid indirection
- Potentially pre-sort queries by locality for cache friendliness
- Trade: higher RSS for lower p99

---

## Key Source Paths in Neo4j Reference Clone

```
neo4j-reference/neo4j/community/
├── kernel/                    # Core engine
├── kernel-api/                # StorageEngine SPI
├── record-storage-engine/     # NeoStores, record format
│   └── src/main/java/org/neo4j/internal/recordstorage/
│       ├── RecordStorageEngine.java
│       ├── RecordStorageEngineFactory.java
│       └── RelationshipCreator.java
├── io/                        # Page Cache (Muninn)
│   └── src/main/java/org/neo4j/io/pagecache/
├── index/                     # GBPTree
├── cypher/                    # Full query engine
├── bolt/                      # Wire protocol
└── storage-engine-util/       # Counts store
```

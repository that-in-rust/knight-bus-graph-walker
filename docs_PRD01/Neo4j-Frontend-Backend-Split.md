# Neo4j: Frontend vs Backend Split

*Frontend = what the user touches. Must be flawlessly like Neo4j.*
*Backend = what processes data. Gets the Knight Bus treatment.*

---

## The Split

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  FRONTEND / USER EXPERIENCE LAYER                          │
│  ═══════════════════════════════════                        │
│  "The user should not know they left Neo4j"                │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Cypher Language Surface        325,311 LOC         │   │
│  │  ├── front-end/                 322,897 LOC         │   │
│  │  │   ├── Parser (ANTLR → AST)                      │   │
│  │  │   ├── Semantic analysis (types, scopes)          │   │
│  │  │   ├── AST rewriting (normalization)              │   │
│  │  │   └── Name resolution                            │   │
│  │  ├── cypher-config/               1,991 LOC         │   │
│  │  └── cypher-rendering/               81 LOC         │   │
│  │       └── EXPLAIN / PROFILE output                  │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Wire Protocol (Bolt)            42,064 LOC         │   │
│  │  └── bolt/                       42,064 LOC         │   │
│  │      ├── Protocol versions (v3, v4, v5)             │   │
│  │      ├── PackStream serialization                   │   │
│  │      ├── Session state machine                      │   │
│  │      ├── HELLO → READY → STREAMING → …              │   │
│  │      └── Auth handshake                             │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  HTTP / Admin / Server           34,012 LOC         │   │
│  │  ├── server/                     19,960 LOC         │   │
│  │  │   ├── REST endpoints                             │   │
│  │  │   ├── Neo4j Browser serving                      │   │
│  │  │   └── Query submission via HTTP                  │   │
│  │  ├── server-api/                  1,377 LOC         │   │
│  │  ├── configuration/              12,295 LOC         │   │
│  │  │   └── neo4j.conf settings (~400 settings)        │   │
│  │  └── monitoring/                    570 LOC         │   │
│  │      └── Metrics, JMX                               │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  CLI Tools                       23,889 LOC         │   │
│  │  ├── cypher-shell/               18,777 LOC         │   │
│  │  │   └── Interactive Cypher REPL                    │   │
│  │  ├── import-tool/                 4,231 LOC         │   │
│  │  │   └── neo4j-admin import CLI                     │   │
│  │  └── command-line/                  881 LOC         │   │
│  │      └── CLI framework                              │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  User-Facing Types & Errors      44,896 LOC         │   │
│  │  ├── values/                     24,076 LOC         │   │
│  │  │   └── CypherValue types users see in results     │   │
│  │  ├── neo4j-gql-status/            8,191 LOC         │   │
│  │  │   └── GQL standard status codes                  │   │
│  │  ├── neo4j-notifications/         3,508 LOC         │   │
│  │  │   └── Deprecation/perf warnings                  │   │
│  │  ├── neo4j-exceptions/            2,809 LOC         │   │
│  │  │   └── Error types users catch                    │   │
│  │  ├── graphdb-api/                 4,600 LOC         │   │
│  │  │   └── Public Graph interface (Node, Rel, Path)   │   │
│  │  └── token-api/                   1,439 LOC         │   │
│  │      └── Label/property name tokens                 │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Security & Auth                  3,620 LOC         │   │
│  │  └── security/                    3,620 LOC         │   │
│  │      ├── Username/password auth                     │   │
│  │      ├── Role-based access control                  │   │
│  │      └── User management                            │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Procedures (user-callable)      19,909 LOC         │   │
│  │  ├── procedure/                  15,327 LOC         │   │
│  │  │   └── Built-in CALL procedures                   │   │
│  │  ├── procedure-compiler/          4,309 LOC         │   │
│  │  └── procedure-api/                 273 LOC         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  FRONTEND TOTAL:                   493,701 LOC             │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  BACKEND / ENGINE LAYER                                    │
│  ═══════════════════════                                   │
│  "This is where Knight Bus wins"                           │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Storage Engine                  90,438 LOC         │   │
│  │  ├── record-storage-engine/      69,646 LOC         │   │
│  │  │   ├── NodeStore (15-byte linked records)         │   │
│  │  │   ├── RelationshipStore (34-byte records)        │   │
│  │  │   ├── PropertyStore (41-byte records)            │   │
│  │  │   ├── RelGroupStore (dense node partitioning)    │   │
│  │  │   ├── Batch import pipeline                      │   │
│  │  │   └── Record format encoding/decoding            │   │
│  │  ├── storage-engine-util/         7,898 LOC         │   │
│  │  │   └── Counts store, degree cache                 │   │
│  │  ├── id-generator/               10,939 LOC         │   │
│  │  │   └── ID recycling (freed entity IDs)            │   │
│  │  ├── layout/                        723 LOC         │   │
│  │  └── consistency-check/           1,331 LOC         │   │
│  │      └── Store integrity validation                 │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Kernel (Transaction Engine)    101,839 LOC         │   │
│  │  ├── kernel/                     83,297 LOC         │   │
│  │  │   ├── KernelTransaction lifecycle                │   │
│  │  │   ├── NodeCursor, RelCursor, PropCursor          │   │
│  │  │   ├── Write operations                           │   │
│  │  │   ├── Recovery on crash                          │   │
│  │  │   └── Database lifecycle management              │   │
│  │  └── kernel-api/                 18,542 LOC         │   │
│  │      ├── StorageEngine trait                        │   │
│  │      ├── StorageReader trait                        │   │
│  │      └── CommandCreationContext                      │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Page Cache + I/O                14,241 LOC         │   │
│  │  └── io/                         14,241 LOC         │   │
│  │      ├── MuninnPageCache (custom off-heap manager)  │   │
│  │      ├── PageCursor abstraction                     │   │
│  │      ├── Clock sweep eviction                       │   │
│  │      └── File I/O abstraction                       │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Indexing                        13,402 LOC         │   │
│  │  └── index/                      13,402 LOC         │   │
│  │      ├── GBPTree (B+tree)                           │   │
│  │      ├── Schema indexes                             │   │
│  │      ├── Counts store index                         │   │
│  │      └── Crash-safe checkpoint integration          │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Durability                       8,888 LOC         │   │
│  │  └── wal/                         8,888 LOC         │   │
│  │      ├── Write-ahead log entries                    │   │
│  │      ├── Checkpointing                              │   │
│  │      └── Log rotation                               │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Concurrency                      5,522 LOC         │   │
│  │  └── lock/                        5,522 LOC         │   │
│  │      ├── Read/write locks                           │   │
│  │      └── Deadlock detection                         │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Query Execution Engine         376,530 LOC         │   │
│  │  ├── cypher-planner/            181,802 LOC         │   │
│  │  │   ├── IDP solver (join ordering)                 │   │
│  │  │   ├── Cost-based optimization                    │   │
│  │  │   ├── Cardinality estimation                     │   │
│  │  │   └── Eager barrier analysis                     │   │
│  │  ├── interpreted-runtime/        59,267 LOC         │   │
│  │  │   └── Interpreted pipe execution                 │   │
│  │  ├── cypher/ (assembly)          45,591 LOC         │   │
│  │  │   └── CypherQueryEngine glue                     │   │
│  │  ├── runtime-util/               35,381 LOC         │   │
│  │  │   └── Row handling, shared runtime code          │   │
│  │  ├── slotted-runtime/            19,674 LOC         │   │
│  │  │   └── Slot-array based execution (faster)        │   │
│  │  ├── cypher-logical-plans/       14,605 LOC         │   │
│  │  │   └── Plan node types (Scan, Expand, Filter…)    │   │
│  │  ├── physical-planning/          10,735 LOC         │   │
│  │  │   └── Slot allocation, pipe mapping              │   │
│  │  ├── ir/                          9,740 LOC         │   │
│  │  │   └── Intermediate representation                │   │
│  │  ├── logical-plan-builder/        7,847 LOC         │   │
│  │  ├── cypher-cache/                1,597 LOC         │   │
│  │  │   └── Query plan cache                           │   │
│  │  ├── planner-spi/                 1,412 LOC         │   │
│  │  ├── expression-evaluator/        1,369 LOC         │   │
│  │  ├── graph-counts/                  809 LOC         │   │
│  │  └── codegen/                    13,279 LOC         │   │
│  │      └── Runtime code generation (JIT substitute)   │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Search & Spatial                18,191 LOC         │   │
│  │  ├── lucene-index/               12,896 LOC         │   │
│  │  │   └── Lucene-backed full-text                    │   │
│  │  ├── fulltext-index/              3,029 LOC         │   │
│  │  │   └── Full-text integration                      │   │
│  │  └── spatial-index/               2,266 LOC         │   │
│  │      └── R-tree spatial indexing                    │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Graph Algorithms                 4,321 LOC         │   │
│  │  └── graph-algo/                  4,321 LOC         │   │
│  │      ├── Shortest path (Dijkstra)                   │   │
│  │      ├── BFS / DFS                                  │   │
│  │      └── A*                                         │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Import Infrastructure           27,090 LOC         │   │
│  │  ├── import-util/                21,981 LOC         │   │
│  │  │   └── Batch import pipeline                      │   │
│  │  └── csv/                         5,109 LOC         │   │
│  │      └── CSV parsing                                │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Shared Backend Infrastructure   30,329 LOC         │   │
│  │  ├── common/                     12,324 LOC         │   │
│  │  ├── collections/                11,356 LOC         │   │
│  │  ├── schema/                      6,669 LOC         │   │
│  │  ├── concurrent/                  1,813 LOC         │   │
│  │  └── unsafe/                      1,443 LOC         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  BACKEND TOTAL:                    690,791 LOC             │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  NOT PORTED                                                │
│  ══════════                                                │
│                                                             │
│  Tests (rewrite in Rust)           314,872 LOC             │
│  ├── community-it/                 207,135 LOC             │
│  ├── kernel-test/                   58,323 LOC             │
│  ├── testing/                       20,277 LOC             │
│  ├── gbptree-tests/                 17,603 LOC             │
│  ├── kernel-test-utils/              7,421 LOC             │
│  ├── server-test-utils/              1,982 LOC             │
│  ├── neo4j-harness/                  2,131 LOC             │
│  ├── cypher/runtime-spec-suite/    122,221 LOC  (in #2)    │
│  ├── cypher/cypher-testing/          1,855 LOC  (in #2)    │
│  ├── cypher/logical-plan-generator/  1,715 LOC  (in #2)    │
│  ├── cypher/spec-suite-tools/        1,546 LOC  (in #2)    │
│  └── cypher/compatibility-spec-suite/  487 LOC  (in #2)    │
│                                                             │
│  Cloud / Enterprise / Misc          26,826 LOC             │
│  ├── fabric/                        14,958 LOC             │
│  ├── cloud/                          4,115 LOC             │
│  ├── genai-plugin/                   4,230 LOC             │
│  ├── push-to-cloud/                  3,986 LOC             │
│  ├── dbms/                          10,560 LOC             │
│  ├── neo4j/ (bootstrap)             11,443 LOC             │
│  ├── data-collector/                 2,043 LOC             │
│  ├── logging/                        4,080 LOC             │
│  ├── ssl/                            1,851 LOC             │
│  ├── neo4j-slf4j-provider/           1,122 LOC             │
│  ├── capabilities/                   1,133 LOC             │
│  ├── native/                           481 LOC             │
│  ├── udc/                              289 LOC             │
│  ├── diagnostics/                      109 LOC             │
│  ├── bootcheck/                        107 LOC             │
│  ├── resource/                         116 LOC             │
│  ├── arrow-bom/                        101 LOC             │
│  ├── zstd-proxy/                        69 LOC             │
│  ├── neo4j-community/                   36 LOC             │
│  └── import-api/                       987 LOC             │
│                                                             │
│  NOT PORTED TOTAL:                 ~397K LOC               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## The Numbers

| Layer | LOC | % of Neo4j | KNRT strategy |
|---|---|---|---|
| **Frontend / UX** | 493,701 | 31% | **Flawlessly replicate** |
| **Backend / Engine** | 690,791 | 44% | **Replace with Knight Bus architecture** |
| **Tests** | 314,872 | 20% | Rewrite in Rust |
| **Cloud / Enterprise / Misc** | ~83K | 5% | Skip for v1 |
| **Total** | 1,582,052 | 100% | |

---

## FRONTEND: What Must Be Flawlessly Like Neo4j

The user should not know they switched. Every Cypher query, every
driver connection, every error message, every CLI command should
behave identically.

### What KNRT replicates (from openCypher + Bolt specs, NOT from Neo4j code)

| Frontend piece | Neo4j LOC | KNRT approach | KNRT LOC est. |
|---|---|---|---|
| Cypher parser + semantic | 325K | New from openCypher spec + ANTLR grammar | 8-15K |
| Bolt protocol | 42K | From Bolt v5 spec; MeshDB proves doable | 10-15K |
| HTTP admin | 34K | Minimal `axum` endpoints for v1 | 2-3K |
| cypher-shell | 19K | `clap` + `rustyline` CLI | 3-5K |
| Values/types | 24K | Rust enum-based CypherValue | 5-8K |
| Error types + status | 14.5K | `thiserror` + GQL status codes | 2-3K |
| Security | 3.6K | `argon2` + config file auth | 1-2K |
| Procedures (built-in subset) | 20K | Most common `db.*` procedures | 3-5K |
| **Frontend total** | **493K** | | **34-56K** |

**Compression: 9-14x.** Java/Scala verbosity → Rust conciseness.
Same behavior, fraction of the code.

**The contract:** A Python/Java/JavaScript Neo4j driver connects to
KNRT on `bolt://host:7687`, runs `MATCH (n:Person)-[:KNOWS]->(m)
WHERE n.age > 30 RETURN m.name`, and gets back the same result types,
same error codes, same session lifecycle as Neo4j. The user edits
one connection string.

---

## BACKEND: Where Knight Bus Wins

This is where the architecture changes. Every folder below gets
replaced with Knight Bus principles:

> - contiguous memory over linked lists
> - array slicing over pointer chasing
> - mmap over managed page cache
> - precomputed reverse adjacency
> - dense integer IDs over scattered records
> - immutable snapshots over transactional mutation
> - algorithm-specific layouts over one-size-fits-all records

### Storage Engine (90,438 LOC → Knight Bus CSR)

**Neo4j's problem (from Previous Learnings):**
```
Node record → pointer → first Relationship
Relationship → pointer → next Relationship (LINKED LIST)
Relationship → pointer → first Property
Property → pointer → next Property (LINKED LIST)
```

Every traversal = pointer chasing through scattered 15/34/41 byte
records across multiple store files. Page cache fault on every hop.

**Knight Bus already proved the fix:**
```
dense_id → offsets[dense_id] .. offsets[dense_id + 1]
peer slice = contiguous array[start..end]
```

No linked lists. No pointer chasing. Array arithmetic over contiguous
memory. 100x faster.

**KNRT backend design:**

| Neo4j folder | LOC | KNRT replacement | Benefit |
|---|---|---|---|
| `record-storage-engine/` | 69,646 | Dual-CSR snapshots (from Knight Bus) + lightweight mutable store for writes | 100x read, 2-3x write |
| `storage-engine-util/` | 7,898 | Precomputed degree arrays (already in Atlas) | No runtime counting |
| `id-generator/` | 10,939 | Dense u32 IDs (Knight Bus model) | No ID recycling needed — rebuild snapshot |
| `layout/` | 723 | Snapshot manifest.json | Already exists in Knight Bus |
| `consistency-check/` | 1,331 | Knight Bus `verify` command | Already exists |

**From Algorithm Storage Atlas — the storage shapes:**

The Atlas defines 13 layout families. The base `AnchorDualCsrLayoutV1`
IS Knight Bus's current snapshot format. The other 12 families extend
it for specific algorithm workloads:

```
Storage Engine Architecture:

┌──────────────────────────────────────────────────────────┐
│  MUTABLE LAYER (for writes)                              │
│  ├── Lightweight append-only records                     │
│  ├── WAL for durability                                  │
│  ├── Periodic CSR snapshot rebuild                       │
│  └── ~30-50K LOC (sled/redb for KV, custom for graph)   │
├──────────────────────────────────────────────────────────┤
│  IMMUTABLE CSR LAYER (for reads — Knight Bus)            │
│  ├── AnchorDualCsrLayoutV1  ← current Knight Bus        │
│  ├── + PropertyPlane (typed numeric/categorical data)    │
│  ├── + Multi-relationship-type support                   │
│  ├── + Per-algorithm layouts from Atlas:                 │
│  │   ├── InboundPowerLayoutV1 (PageRank, HITS)          │
│  │   ├── OrderedWedgeLayoutV1 (TriCount, Similarity)    │
│  │   ├── RelaxationFrontierLayoutV1 (Dijkstra, SSSP)   │
│  │   ├── PartitionRefinementLayoutV1 (Louvain, LPA)    │
│  │   ├── ConnectivityLowlinkLayoutV1 (SCC, WCC)        │
│  │   └── ... (13 families total)                        │
│  └── FormatSelectionProfile chooses layout per algorithm │
├──────────────────────────────────────────────────────────┤
│  MMAP RUNTIME (Knight Bus MmapWalkRuntime)               │
│  ├── OS page cache handles working set                   │
│  ├── Zero overhead vs Muninn's managed eviction          │
│  └── Already proven in Knight Bus                        │
└──────────────────────────────────────────────────────────┘
```

### Page Cache (14,241 LOC → mmap)

**Neo4j's approach** (`io/` — MuninnPageCache):
- Custom off-heap memory manager
- 8 KiB fixed pages
- Clock sweep eviction
- PageCursor abstraction
- Faulting, flushing, file growth

**Knight Bus's approach** (already proven):
- Direct `mmap` via `memmap2` crate
- OS page cache does eviction
- Zero abstraction overhead
- No PageCursor objects, no managed eviction

**From Previous Learnings:**
> "There is no Bolt/network round-trip in the hot path"
> "There is no query planner in the hot path"
> "There is no transaction engine in the hot path"

Muninn IS in Neo4j's hot path. mmap removes it.

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `io/` | 14,241 | `memmap2` + thin wrapper | ~500 |

### Indexing (13,402 LOC → sorted arrays + optional B+tree)

**Neo4j's approach** (`index/` — GBPTree):
- General-purpose crash-safe B+tree
- Used for: schema indexes, counts, IDs, degree tracking

**Knight Bus's approach:**
- Sorted key_index.bin + binary search for exact-key lookup
- degree.u32.bin arrays for degree lookups (from Atlas)
- counts are precomputed in snapshot manifest

**KNRT approach:**
- Keep sorted key index for exact lookups (Knight Bus)
- Use `redb` crate for property index when schema indexes are needed
- Precompute counts/degrees in snapshot build

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `index/` | 13,402 | Sorted arrays (KB) + `redb` for property indexes | 3-5K |

### WAL + Lock (14,410 LOC → lightweight alternatives)

**Neo4j's approach:**
- Full WAL with checkpointing, recovery, log rotation
- Read/write lock manager with deadlock detection

**KNRT approach:**
- Append-only WAL for the mutable layer (much simpler — CSR is immutable)
- `parking_lot` RwLock for concurrency (no deadlock detection needed —
  immutable snapshots can't deadlock readers)

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `wal/` | 8,888 | Simple append log + fsync | 2-3K |
| `lock/` | 5,522 | `parking_lot` RwLock | ~500 |

### Query Execution Engine (376,530 LOC → CSR shortcuts + Volcano)

This is where the Atlas and Previous Learnings converge.

**Neo4j's problem (from Previous Learnings):**
```
Query text
  → Parser → AST                      (frontend — keep)
  → Semantic analysis                  (frontend — keep)
  → AST rewriting                      (frontend — keep)
  → Logical planning (IDP solver)      ← 181,802 LOC of backend
  → Physical planning (slot mapping)   ← 10,735 LOC of backend
  → Runtime execution (pipes)          ← 114,322 LOC of backend
  → Result materialization             ← backend overhead
  → Bolt serialization → network       (frontend — keep)
```

**KNRT's two-track execution:**

```
Track 1: CSR SHORTCUT (for traversal-heavy patterns)
══════════════════════════════════════════════════════
Detected patterns:
  MATCH (n {id: $id})-[:REL]->(m) RETURN m       → 1-hop forward
  MATCH (n {id: $id})<-[:REL]-(m) RETURN m       → 1-hop reverse
  MATCH (n)-[:REL*1..N]->(m)                      → N-hop expansion
  MATCH (n)-[:REL]->(m) RETURN count(m)           → degree lookup

Execution:
  key → binary search → dense_id
  dense_id → offsets[id]..offsets[id+1]
  peer_slice → result

No planner. No pipe operators. No row materialization.
This is what Previous Learnings proved at 100x.

Track 2: VOLCANO OPERATORS (for general Cypher patterns)
══════════════════════════════════════════════════════
For queries that can't shortcut to CSR:
  MATCH (a:Person)-[:KNOWS]->(b)-[:WORKS_AT]->(c:Company)
  WHERE a.age > 30 AND c.name = 'Acme'
  RETURN b.name, count(c)
  ORDER BY count(c) DESC

Execution:
  LabelScan(a, Person) → Filter(a.age > 30)
    → Expand(a, KNOWS, b) → Expand(b, WORKS_AT, c)
      → Filter(c:Company AND c.name = 'Acme')
        → Aggregate(b.name, count(c))
          → Sort(count DESC) → Produce

But even Track 2 reads from CSR arrays, not linked-list records.
So the per-operator I/O is still faster than Neo4j.
```

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `cypher-planner/` | 181,802 | Rule-based planner (v1) + CSR shortcut detection | 15-25K |
| `interpreted-runtime/` | 59,267 | Track 2: Volcano operator pipeline | 15-20K |
| `slotted-runtime/` | 19,674 | (merged into single runtime) | — |
| `runtime-util/` | 35,381 | Shared operator utilities | 5-8K |
| `cypher/` (assembly) | 45,591 | Query engine glue | 3-5K |
| `cypher-logical-plans/` | 14,605 | Plan node enums | 2-3K |
| `physical-planning/` | 10,735 | (simpler — no slot/pipe mapping) | 2-3K |
| `ir/` | 9,740 | AST → plan IR | 2-3K |
| `codegen/` | 13,279 | Not needed — Rust monomorphization replaces JIT | 0 |
| `cypher-cache/` | 1,597 | `lru` crate + plan hash | ~500 |
| Other small folders | ~10K | Misc | 1-2K |
| **Execution total** | **376,530** | | **45-70K** |

**Compression: 5-8x.** And the result is faster because the operators
read from CSR arrays, not linked-list records.

### Graph Algorithms (4,321 LOC → Algorithm Storage Atlas)

**Neo4j's approach** (`graph-algo/`):
- Generic algorithms over property graph cursors
- Dijkstra, BFS, DFS, A*, shortest path
- All pay property-graph traversal overhead

**KNRT approach (from Algorithm Storage Atlas):**

Each algorithm gets its optimal layout. The Atlas maps 60 GDS
algorithms to 13 byte-level layout families. The backend builds
the right snapshot shape per algorithm:

```
User runs: CALL gds.pageRank.stream('myGraph', {})

KNRT backend:
  1. FormatSelectionProfile → InboundPowerLayoutV1
  2. Build or use cached snapshot:
     in.offsets.u64.bin       ← inbound adjacency
     in.peers.u32.bin
     in.weight.f32.bin        ← edge weights
     out.mass.f32.bin         ← precomputed out-degree mass
     dangling.bitset.bin      ← dangling node bitset
  3. Run PageRank:
     for node in partition:
       score_next[node] = base + sum(score[src] * weight / mass[src])
     swap(score_curr, score_next)
  4. Write ResultSidecar: pagerank.scores.f32.bin

Versus Neo4j:
  - Projects graph into GDS in-memory model (already expensive)
  - Iterates through Java object graph
  - GC pressure from millions of score objects
  - No precomputed out-mass, derived per iteration

Why KNRT is faster:
  - Storage "vibes with the algorithm" (Atlas thesis)
  - Pure numeric gather over flat f32 arrays
  - No object headers (Java: 12-16 bytes per object)
  - No GC pauses during iteration
  - Precomputed out.mass eliminates per-iteration derivation
```

**P0 algorithms (build first, from Atlas ROI ranking):**

| Algorithm | Layout family | What it proves |
|---|---|---|
| Degree Centrality | `AnchorDualCsrLayoutV1` | Knight Bus already has this |
| BFS / DFS | `AnchorDualCsrLayoutV1` | Extend KB for frontier replay |
| PageRank | `InboundPowerLayoutV1` | Pure numeric gather wins |
| Dijkstra | `RelaxationFrontierLayoutV1` | Weighted shortest path |
| Triangle Count | `OrderedWedgeLayoutV1` | Sorted intersection wins |

These 5 proofs cover:
- Adjacency replay (what KB does today)
- Iterative score propagation
- Weighted frontier relaxation
- Sorted-neighbor intersection
- Together they prove: "storage should vibe with the algorithm"

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `graph-algo/` | 4,321 | Atlas layout families + native Rust algorithms | 10-20K |

### Search & Spatial (18,191 LOC → Rust crates)

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `lucene-index/` | 12,896 | `tantivy` crate | ~2K glue |
| `fulltext-index/` | 3,029 | `tantivy` integration | ~1K |
| `spatial-index/` | 2,266 | `rstar` crate (v2) | defer |

### Import Infrastructure (27,090 LOC → extend Knight Bus)

Knight Bus already has the hard part: external merge sort build
pipeline for large graphs (`low_ram.rs`, 1,703 LOC).

| Neo4j folder | LOC | KNRT replacement | LOC est. |
|---|---|---|---|
| `import-util/` | 21,981 | Extend KB build pipeline + Neo4j format reader | 5-8K |
| `csv/` | 5,109 | `csv` crate (already in KB) | ~500 |

---

## Backend Summary: What Knight Bus Learnings Buy

| Backend subsystem | Neo4j LOC | KNRT LOC | Compression | Speed gain |
|---|---|---|---|---|
| Storage engine | 90,438 | 15-25K | 4-6x | **100x reads** (CSR vs linked lists) |
| Kernel/transactions | 101,839 | 10-15K | 7-10x | 2-3x (no GC) |
| Page cache | 14,241 | ~500 | 28x | **mmap vs Muninn** |
| Indexing | 13,402 | 3-5K | 3-4x | Binary search on sorted arrays |
| WAL + lock | 14,410 | 2.5-3.5K | 4-6x | Simpler (immutable snapshots) |
| Query execution | 376,530 | 45-70K | 5-8x | **CSR shortcut for traversal** |
| Search/spatial | 18,191 | 2-3K | 6-9x | `tantivy` replaces Lucene |
| Graph algorithms | 4,321 | 10-20K | 0.2-0.5x* | **Atlas layout families** |
| Import | 27,090 | 5.5-8.5K | 3-5x | KB low-RAM build exists |
| Shared infra | 30,329 | 5-8K | 4-6x | Rust stdlib + crates |
| **Backend total** | **690,791** | **~100-160K** | **4-7x** | |

*Graph algorithms LOC increases because Atlas adds 13 layout families
that Neo4j doesn't have. This is new capability, not compression.

---

## The Full Picture

```
KNRT = Neo4j-identical frontend + Knight Bus-powered backend

Frontend (flawless Neo4j):          34-56K LOC Rust
Backend (Knight Bus architecture): 100-160K LOC Rust
─────────────────────────────────────────────────
Total KNRT:                        134-216K LOC Rust

vs Neo4j:                        1,582,052 LOC Java/Scala

Compression: 7-12x
```

**The user experience:**
- Same Cypher syntax
- Same Bolt protocol
- Same drivers (Python, Java, JS, .NET, Go)
- Same neo4j.conf-style configuration
- Same cypher-shell CLI
- Same error messages and status codes

**The engine underneath:**
- 100x faster traversal reads (CSR vs linked lists)
- No GC pauses (Rust vs JVM)
- 3-10x less memory (dense arrays vs object headers)
- Algorithm-specific storage layouts (Atlas)
- mmap instead of Muninn page cache
- Single binary, <1s startup

**From Previous Learnings — why the backend gap is real:**

> "The hot path is mostly array slicing over contiguous memory."
> "There is no query planner in the hot path."
> "There is no transaction engine in the hot path."
> "There is no property decoding in the hot path."
> "There is no Bolt/network round-trip in the hot path."

Every one of these "no X in the hot path" statements maps to a
backend folder that KNRT replaces or eliminates:

| "No X in hot path" | Neo4j backend folder eliminated | How |
|---|---|---|
| No query planner | `cypher-planner/` (181K LOC) | CSR shortcut detection |
| No transaction engine | `kernel/` (83K LOC) | Immutable snapshots |
| No property decoding | `record-storage-engine/` (70K LOC) | Dense typed arrays |
| No Bolt round-trip | `bolt/` (42K LOC) | In-process for CSR path |
| No page cache locking | `io/` (14K LOC) | Direct mmap |
| No pointer chasing | `record-storage-engine/` | CSR contiguous arrays |

**From Algorithm Storage Atlas — why per-algorithm wins:**

> "The cleanest way to make storage vibe with the algorithm is to
> stop treating every algorithm as 'graph query plus math' and instead
> treat it as 'one dominant inner loop plus the smallest byte shape
> that feeds that loop well.'"

This is a backend-only insight. The frontend doesn't care what
layout family the snapshot uses. The user writes the same Cypher.
The `FormatSelectionProfile` in the backend picks the right layout.

```
User sees:     CALL gds.pageRank.stream(...)       ← same as Neo4j
Backend does:  InboundPowerLayoutV1                 ← Atlas-optimized
User sees:     CALL gds.triangleCount.stream(...)   ← same as Neo4j
Backend does:  OrderedWedgeLayoutV1                 ← Atlas-optimized
User sees:     MATCH (n)-[:X]->(m) RETURN m         ← same as Neo4j
Backend does:  AnchorDualCsrLayoutV1                ← Knight Bus CSR
```

Frontend: flawlessly Neo4j.
Backend: radically different.

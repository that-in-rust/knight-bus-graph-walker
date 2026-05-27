# Complete Understanding: Knight Bus — Neo4j Rewritten in Rust

*Everything I know, from code analysis to architectural decisions, consolidated.*

---

## I. The Mission (L1 PRD)

```
Neo4j rewritten in Rust:
1. Exact same APIs or surface area with ZERO changes
   → same code can be used (drivers, Cypher, Bolt protocol)
2. Identical architecture for OLTP queries
   → same record format: 15B node / 34B relationship / 41B property
3. Lowest RAM custom storage formats for OLAP queries
   → REAL RAM: 50 GB graph comfortably processed on 8 GB system
4. Community edition, hence single node
   → no Raft, no clustering, no enterprise features
```

---

## II. What We're Replacing (Source Code Analysis)

### Neo4j Community Edition

**Location:** `neo4j-reference/neo4j/community/`  
**Size:** 67 folders, ~1.58M LOC Java + 674K LOC Scala = **~2.25M LOC total**  
**Architecture:** Single-node OLTP graph database

Confirmed from actual source code:
- **Node records:** 15 bytes (`NodeRecordFormat.java:32` → `RECORD_SIZE = 15`)
- **Relationship records:** 34 bytes (`RelationshipRecordFormat.java:35` → `RECORD_SIZE = 34`)
- **Property records:** 41 bytes (`PropertyRecordFormat.java:37-38` → `1 + 4 + 4 + 32 = 41`)
- **Relationship traversal:** Pointer-chase linked list (`RecordRelationshipTraversalCursor.java:259-267`)
- **Dense nodes:** Extra group indirection (group → incoming → outgoing → loop chains)
- **Storage engine:** Pluggable via service loading (`StorageEngine` interface)
- **Page cache:** Custom implementation called Muninn (not OS page cache)
- **Cypher:** 450K LOC total — parser (ANTLR4/JavaCC), planner (182K LOC Scala — LARGEST module), 2 runtimes

### 9 Functional Groups in Neo4j Community

| Group | LOC | What It Does |
|---|---|---|
| Cypher Engine | ~450K | Parser, planner, 2 runtimes (interpreted + slotted) |
| Storage Engine | ~228K | Record stores, page cache, cursors |
| Kernel | ~122K | Transactions, locks, ID allocation |
| Bolt Protocol | ~73K | PackStream binary protocol, session state machine |
| Values/Types | ~50K | Type system (50+ Value subclasses) |
| Indexing | ~49K | Lucene, fulltext, spatial indexes |
| Server | ~47K | HTTP, management, DBMS lifecycle |
| Procedures | ~29K | Built-in procedures, procedure framework |
| Import/Export | ~47K | Batch import, CSV, tools |

### Neo4j GDS (Graph Data Science)

**Location:** `/home/ubuntu/repos/neo4j-gds/`  
**Size:** 4,898 Java files, **~530K LOC**  
**Architecture:** Neo4j plugin — registers CALL procedures, projects graph into heap, runs analytics

**Critical findings from reading the actual code:**

1. **PageRank uses PREGEL, not direct CSR iteration.** The `PageRankComputation` class implements `PregelComputation` — it sends messages to neighbors via BSP supersteps, not direct array access. This means our direct CSR iteration will be faster (no message queue overhead).

2. **The Graph interface** (the OLAP API boundary):
```java
interface Graph extends IdMap, NodePropertyContainer, Degrees,
                        RelationshipIterator, RelationshipProperties {
    long nodeCount();
    long relationshipCount();
    int degree(long nodeId);
    void forEachRelationship(long nodeId, RelationshipConsumer consumer);
    void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer);
    Graph concurrentCopy();
}
```
Every algorithm programs against this interface. In GDS it's backed by in-memory CSR. In Knight Bus it's backed by mmap'd or O_DIRECT-streamed CSR.

3. **40 stable algorithm families, ~200 procedures:**

| Category | Algorithms | Count |
|---|---|---|
| Centrality | pageRank, articleRank, eigenvector, betweenness, degree, closeness, harmonic, hits | 8 |
| Community | louvain, leiden, labelPropagation, wcc, scc, modularity, modularityOptimization, k1coloring, localClusteringCoefficient, conductance, triangleCount, kcore, maxkcut, articulationPoints, bridges, sllpa | 16 |
| Path Finding | dijkstra, astar, yens, allDijkstra, delta, bellmanFord, bfs, dfs, topoSort, longestPath, randomWalk | 11 |
| Similarity | nodeSimilarity, knn, filtered variants | 4 |
| Embeddings | fastRP, node2vec, hashgnn | 3 |
| Misc | spanningTree, kSpanningTree, steinerTree, prizeSteiner, celf, collapsePath, scaleProperties, kmeans | 8 |

4. **Procedure dispatch chain:** `CALL gds.pageRank.stream('graph', {config})` → `PageRankStreamProc` → `facade.algorithms().centrality().pageRankStream()` → `PageRankAlgorithm.compute()` → `Pregel.run()` → `Graph.forEachRelationship()`

5. **The projection (graph loading into heap) is the RAM problem.** GDS reads Neo4j's record stores → builds in-memory adjacency lists → runs algorithm. This requires 30-60 GB heap for a 50 GB graph. Knight Bus skips steps 1-2 because our storage IS the CSR.

### BoltR (Pure Rust Bolt v5.x)

**Location:** `/home/ubuntu/repos/boltr/`  
**Size:** 5,328 LOC Rust  
**License:** MIT OR Apache-2.0

BoltR is a spec-faithful Bolt v5.x wire protocol library with a `BoltBackend` trait:
```rust
#[async_trait]
pub trait BoltBackend: Send + Sync + 'static {
    async fn create_session(&self, config: &SessionConfig) -> Result<SessionHandle, BoltError>;
    async fn close_session(&self, session: &SessionHandle) -> Result<(), BoltError>;
    async fn execute(&self, session: &SessionHandle, query: &str,
                     parameters: &HashMap<String, BoltValue>, extra: &BoltDict,
                     transaction: Option<&TransactionHandle>) -> Result<ResultStream, BoltError>;
    async fn begin_transaction(&self, session: &SessionHandle, extra: &BoltDict) -> Result<TransactionHandle, BoltError>;
    async fn commit(&self, session: &SessionHandle, transaction: &TransactionHandle) -> Result<BoltDict, BoltError>;
    async fn rollback(&self, session: &SessionHandle, transaction: &TransactionHandle) -> Result<(), BoltError>;
    async fn get_server_info(&self) -> Result<BoltDict, BoltError>;
}
```

We implement this trait → Neo4j drivers connect to our server. Saves ~2 weeks of protocol work.

### Neo4j Python Driver

**Location:** `/home/ubuntu/repos/neo4j-python-driver/`  
**Size:** 112 Python files, ~38K LOC  
**Purpose:** Compatibility test target — verify our Bolt server works with existing Neo4j drivers

---

## III. Architecture Decisions (Proven by Analysis)

### Decision 1: Dual-Engine HTAP Architecture

```
Bolt Server (PackStream v5, tokio) — from BoltR
    ↓
Cypher Engine (hand-rolled parser, cost-based planner)
    ↓
Query Router
    ↙           ↘
OLTP Engine       OLAP Engine
(identical)       (lowest RAM)
    ↘           ↙
  Sync Bridge (WAL → CSR rebuild)
```

- **OLTP path:** Identical record format (15B/34B/41B), mmap replaces Muninn page cache, zero-copy replaces PageCursor, no GC. Expected: 1.5-3× faster than Neo4j.
- **OLAP path:** 3-level RAM control system (see below).

### Decision 2: 3-Level OLAP RAM Control

For a 50 GB graph (200M nodes, 1B edges) on an 8 GB system:

| Level | How It Works | RAM | Speed | When |
|---|---|---|---|---|
| Level 1: mmap | OS manages page cache | Variable | 8-22 sec | Default if RAM is abundant |
| **Level 2: O_DIRECT** | **Scores in RAM, CSR streamed from disk** | **3.2 GB exact** | **10-25 sec** | **Default on 8 GB system** |
| Level 3: Edge-centric | Everything partitioned + streamed | ~1.6 GB | 300-600 sec | Fallback for >400M node graphs |

**The headline:** Neo4j GDS needs 30-60 GB heap → OOM on 8 GB. Knight Bus needs 3.2 GB → works with 4.8 GB headroom.

### Decision 3: ONE Storage Format, NOT 13

**We analyzed 13 custom storage format families and killed 12 of them.**

The "13 layouts" thesis was tested against:
- 25+ academic papers (X-Stream, Cagra, GraphBLAS, GraphChi, etc.)
- Neo4j GDS source code
- Hard math for 50 GB baseline

Result: 8 of 13 layouts are actively harmful (increase page cache pressure), 3 are mixed, 1 is negligible, 1 is the base CSR itself.

**Corrected architecture:**
```
ONE on-disk format: CSR (sorted adjacency) + typed property columns
  +
PER-ALGORITHM runtime optimizations (segmenting, streaming, etc.)
  +
OPTIONAL in-memory cached views (GraphBLAS model, v0.0.5+)
```

**Why single format wins for "lowest RAM":**
- 13 layouts → 40-65 GB on disk → page cache thrashing → unpredictable RAM
- 1 CSR → 26 GB on disk → predictable, streamable, O_DIRECT compatible

### Decision 4: Use BoltR as Dependency (Not Build from Scratch)

BoltR gives us: PackStream encoding, chunk framing, session state machine, version negotiation, auth, TLS, WebSocket. We write ~450 LOC of integration instead of ~2,000 LOC from scratch.

### Decision 5: Build Order — Timeline C (Vertical Slice)

| Phase | What Ships | LOC | Time |
|---|---|---|---|
| **v0.0.3** | Bolt server + CALL procedures + PageRank | +2,250 | 2-3 weeks |
| v0.0.5 | More algorithms (wcc, louvain, dijkstra, bfs) + Cypher subset | +6,000 | 6 weeks |
| v0.0.7 | OLTP record stores + write path (WAL, transactions) | +3,700 | 6 weeks |
| v0.1.0 | Cost-based planner, indexes, config, production | +8,800 | 8 weeks |

v0.0.3 demo: `neo4j-driver → CALL gds.pageRank.stream → 3.2 GB, 15 sec` (Neo4j: OOM)

---

## IV. The 14 Rust Crates (100% Surface Area)

```
knight-bus/                          # Workspace root
├── crates/
│   ├── kb-bolt/          (~450 LOC)    # BoltR wrapper — wire protocol
│   ├── kb-cypher/        (~25,000 LOC) # Parser, planner, runtime
│   ├── kb-values/        (~3,000 LOC)  # Type system, errors
│   ├── kb-graph-api/     (~1,000 LOC)  # Graph trait (OLAP API boundary)
│   ├── kb-algo/          (~10,000 LOC) # All 40+ algorithm families
│   ├── kb-algo-procedures/ (~5,000 LOC) # CALL gds.* dispatch
│   ├── kb-store-oltp/    (~8,000 LOC)  # 15B/34B/41B record stores
│   ├── kb-store-olap/    (~5,000 LOC)  # CSR storage + compression
│   ├── kb-kernel/        (~10,000 LOC) # Transaction engine
│   ├── kb-index/         (~5,000 LOC)  # Indexes (tantivy for fulltext)
│   ├── kb-wal/           (~2,000 LOC)  # Write-ahead log
│   ├── kb-config/        (~1,500 LOC)  # Configuration
│   ├── kb-server/        (~3,000 LOC)  # HTTP + management
│   └── kb-import/        (~3,000 LOC)  # Bulk import, CSV, export
└── Cargo.toml

Total: ~70,000 LOC Rust replaces ~2.78M LOC Java/Scala (40:1 ratio)
```

### Crate Dependency Graph

```
                    kb-server (binary)
                   /    |    \
          kb-bolt   kb-cypher   kb-config
              \       |    \       /
            kb-algo-procedures
                   |        \
               kb-algo      kb-graph-api
              /    |    \        |
     kb-store-olap   kb-store-oltp   kb-kernel
           \            |            /
            kb-wal    kb-index   kb-values
                  \       |       /
                    kb-import
```

---

## V. Two Algorithm Paths in Neo4j (Key Insight)

### Path 1: Cypher Built-in (OLTP)

```
MATCH shortestPath((a)-[*]-(b))
  → Cypher parser → planner → runtime
  → kernel-api cursors → graph-algo/ (7K LOC)
  → record-storage-engine (linked-list traversal)
```

Algorithms: shortestPath, allShortestPaths, BFS expand, DFS expand, variable-length paths, weighted Dijkstra.
These operate on the live store, single-pair, cursor-based.

### Path 2: GDS CALL Procedures (OLAP)

```
CALL gds.pageRank.stream('myGraph', {dampingFactor: 0.85})
  → procedure registry → PageRankStreamProc
  → facade → Graph catalog → algorithm
  → Graph.forEachRelationship() (CSR iteration)
  → results → Bolt → driver
```

Algorithms: 40 families, ~200 procedures. Whole-graph analytics on projected CSR.

**Knight Bus advantage:** GDS must project (copy) the graph from Neo4j stores into heap (30-60 GB). Knight Bus storage IS the CSR — no projection step, no heap copy.

---

## VI. The OLAP API (The Graph Trait)

```rust
/// Every OLAP algorithm programs against this trait.
/// Backed by: mmap'd CSR (Level 1), O_DIRECT streamed CSR (Level 2),
/// or partitioned streaming (Level 3).
pub trait Graph: Send + Sync {
    fn node_count(&self) -> u64;
    fn relationship_count(&self) -> u64;
    fn degree(&self, node_id: u64) -> u32;
    fn for_each_relationship(&self, node_id: u64, consumer: &mut dyn FnMut(u64, u64) -> bool);
    fn for_each_relationship_weighted(&self, node_id: u64, fallback: f64,
                                       consumer: &mut dyn FnMut(u64, u64, f64) -> bool);
    fn for_each_inverse_relationship(&self, node_id: u64, consumer: &mut dyn FnMut(u64, u64) -> bool);
    fn is_multi_graph(&self) -> bool;
    fn concurrent_copy(&self) -> Box<dyn Graph>;
}

pub trait IdMap {
    fn to_mapped_id(&self, original: u64) -> Option<u64>;
    fn to_original_id(&self, mapped: u64) -> u64;
}
```

---

## VII. Performance Estimates (50 GB Graph on 8 GB System)

### PageRank (200M nodes, 1B edges, 20 iterations)

| | Neo4j GDS | Knight Bus Level 2 |
|---|---|---|
| **Can it run?** | **NO — OOM** | **YES** |
| **RAM** | 30-60 GB | **3.2 GB** |
| **Time** | N/A (can't start) | **~15 sec** |
| **How** | Heap projection | O_DIRECT streaming + rayon |

### All Algorithms on 8 GB

| Algorithm | RAM (Level 2) | Speed | Notes |
|---|---|---|---|
| PageRank | 3.2 GB | ~15 sec | 2× f64 arrays (200M × 8B × 2) |
| WCC | 1.6 GB | ~5 sec | 1× u64 union-find array |
| BFS/DFS | 0.2 GB | ~2 sec | Visited bitset only |
| Dijkstra | 1.6 GB | ~3 sec | Distance array + priority queue |
| Louvain | 2.4 GB | ~20 sec | Community + modularity arrays |
| Triangle Count | 0.8 GB | ~30 sec | Degree + intersection counters |
| SCC (Tarjan) | 2.4 GB | ~10 sec | Stack + index + lowlink arrays |
| k-Core | 1.6 GB | ~5 sec | Degree array + peeling |

All fit within 6.5 GB (8 GB − OS − server overhead).

---

## VIII. Current Codebase State

**Repository:** `/home/ubuntu/repos/knight-bus-graph-walker`  
**Branch:** `neo4j-replacement-01`  
**Existing Rust code:** ~4,710 LOC — read-only CLI, dual CSR + mmap

### What Exists
- CSR snapshot builder (from CSV)
- Memory-mapped graph traversal runtime
- CLI for forward/reverse walks
- Benchmark suite (Python)
- ~60 analysis/architecture documents

### What's Next (v0.0.3)
1. Add `boltr = "0.2"` as dependency
2. Implement `BoltBackend` trait (~200 LOC)
3. Build PageRank on Level 2 O_DIRECT (~350 LOC)
4. Wire CALL dispatch: `CALL gds.pageRank.stream(...)` → engine → results
5. Test with `neo4j-driver-python`

---

## IX. Key Corrections Made During Analysis

| What I Originally Said | What's Actually True | Source |
|---|---|---|
| "13 custom layouts = lowest RAM" | 13 layouts INCREASE RAM (page cache pressure) | Math + 25 papers |
| "Level 3 = 41 MB for any scale" | Level 3 = ~1.6 GB for 200M nodes (score arrays) | X-Stream paper math |
| "Build Bolt from scratch (~2K LOC)" | BoltR exists: 5.3K LOC Rust, ready to use | GrafeoDB/boltr repo |
| "GDS uses plain CSR iteration" | GDS PageRank uses PREGEL (message-passing BSP) | `PageRankComputation.java` |
| "No paper proposes multiple layouts" | GraphBLAS has 5 in-memory format variants | SuiteSparse docs |
| "ConnectivityLowlink is conceptually wrong" | Wrong as on-disk layout, but node relabeling helps cache | Corrected |
| Didn't recommend sorted adjacency | Sorting peers in CSR at build time = free, helps triangle counting | Corrected |

---

## X. Repos We Have

| Repo | Location | Size | Purpose |
|---|---|---|---|
| **Knight Bus** | `/home/ubuntu/repos/knight-bus-graph-walker/` | ~4.7K LOC Rust + 60 docs | Our project |
| **Neo4j Community** | `neo4j-reference/neo4j/community/` (inside Knight Bus) | ~2.25M LOC Java+Scala | OLTP reference |
| **Neo4j GDS** | `/home/ubuntu/repos/neo4j-gds/` | ~530K LOC Java | OLAP reference |
| **BoltR** | `/home/ubuntu/repos/boltr/` | ~5.3K LOC Rust | Bolt v5.x (USE DIRECTLY) |
| **Neo4j Python Driver** | `/home/ubuntu/repos/neo4j-python-driver/` | ~38K LOC Python | Compat testing |

---

## XI. The Moat (Why This Works)

The fundamental advantage isn't "Rust is faster than Java." It's structural:

1. **No projection tax.** GDS copies the entire graph from Neo4j stores into heap (30-60 GB, 60-90 seconds). Knight Bus storage IS the CSR. Zero copy, zero wait.

2. **No GC.** Java's off-heap collections (`HugeDoubleArray`, etc.) exist BECAUSE of GC. Rust doesn't need them — `Vec<f64>` is already off-heap with zero overhead.

3. **O_DIRECT streaming.** Bypass OS page cache entirely. Read CSR directly from disk into user-space buffers. Guaranteed 3.2 GB for PageRank on 200M nodes. Java can't do this without JNI.

4. **mmap is free.** Rust `memmap2` maps files directly into address space. No `PageCursor` abstraction, no bounds checking per read, no `ByteBuffer` allocation.

5. **12:1 LOC ratio.** 2.78M LOC Java/Scala → ~70K LOC Rust. Less code = fewer bugs = faster iteration.

---

## XII. Document Index

All analysis documents in `docs_PRD02/`:

| Document | Lines | Key Conclusion |
|---|---|---|
| `100-Percent-Surface-Area-Crate-Map.md` | 545 | 14 Rust crates, complete folder mapping, ~70K LOC |
| `All-Repos-Folder-Inventory.md` | 291 | All 144 folders across 4 repos with relevance ratings |
| `Neo4j-Ecosystem-Repos-Study.md` | 238 | GDS uses Pregel, BoltR exists, self-audit of gap |
| `1000IQ-OLAP-Architecture-Deep-Think.md` | 1023 | 13 formats killed → 1 CSR, web-research validated |
| `Deep-Research-Custom-Formats-Per-Family.md` | — | 25 papers: not a single one proposes per-algorithm layouts |
| `Rubber-Duck-13-Families-vs-Neo4j-Source.md` | — | Verified all claims vs actual Neo4j Java source code |
| `OLAP-RAM-8GB-Constraint-Analysis.md` | 715 | Level 2 = 3.2 GB for PageRank on 200M nodes |
| `Architecture-Dual-Engine.md` | 615 | HTAP: identical OLTP + lowest-RAM OLAP |
| `Choices-Timeline-Traverser.md` | 424 | 8 choices, 3 timelines, recommend Timeline C |
| `PMF-RAM-vs-Latency-Doshi.md` | — | RAM complaints 2:1 vs latency in Neo4j community |
| `PMF-Viral-The-Obvious-Mistake.md` | — | "50 GB graph on a laptop" = viral headline |
| Plus 24 more documents | ~19K total | Various analysis, timelines, rubber-duck debugs |

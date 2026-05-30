# 100% Surface Area Replacement: Neo4j → Rust Multi-Crate Architecture

*Complete folder-by-folder mapping from Neo4j Java/Scala to Knight Bus Rust crates.*

---

## Core Facts

1. **Neo4j Community:** 67 folders, ~1.58M LOC Java + 674K LOC Scala = **~2.25M LOC total**
2. **Neo4j GDS:** 67 folders, ~530K LOC Java
3. **BoltR:** 10 modules, ~5.3K LOC Rust (USE AS-IS)
4. **Combined source we're replacing:** ~2.78M LOC
5. **Estimated Rust equivalent:** ~50-70K LOC (40:1 ratio — Rust has no GC overhead, no test infra, enums replace class hierarchies)

---

## The Key Bottleneck: How Cypher Calls Algorithms

There are TWO separate algorithm paths in Neo4j:

### Path 1: Cypher Built-in (OLTP — from `graph-algo/`)

```
Cypher query: MATCH shortestPath((a)-[*]-(b))
    ↓
Cypher Parser (front-end/parser/ — ANTLR4/JavaCC, 148K LOC Java + 175K Scala)
    ↓
Cypher Planner (cypher-planner/ — 182K LOC Scala, LARGEST module)
    ↓
Physical Planning (physical-planning/ — 10K LOC Scala)
    ↓
Runtime (interpreted-runtime/ or slotted-runtime/ — 59K + 20K LOC Scala)
    ↓
Kernel API (kernel-api/ — cursor-based traversal, 35K LOC)
    ↓
graph-algo/ (Dijkstra, A*, BFS, AllPaths — 7K LOC)
    ↓
Record Storage Engine (record-storage-engine/ — 96K LOC)
```

**Algorithms in this path:** shortestPath, allShortestPaths, BFS expand, DFS expand, 
pattern matching (variable-length paths), Dijkstra (weighted shortest path).

These are OLTP algorithms — single-pair, traversal-oriented, operate on the live store.

### Path 2: GDS Procedures (OLAP — from GDS `algo/`)

```
Cypher query: CALL gds.pageRank.stream('myGraph', {dampingFactor: 0.85})
    ↓
Cypher Parser → recognizes CALL statement
    ↓
Procedure Registry → looks up "gds.pageRank.stream"
    ↓
PageRankStreamProc.stream() (proc/centrality/)
    ↓
facade.algorithms().centrality().pageRankStream()
    ↓
GraphDataScienceProcedures → CentralityProcedureFacade (procedures/)
    ↓
Graph Catalog → finds projected graph "myGraph"
    ↓
PageRankAlgorithm.compute() (algo/pagerank/)
    ↓
Pregel.run() → BSP supersteps over Graph interface (pregel/)
    ↓
Graph.forEachRelationship() → CSR adjacency iteration (core/)
    ↓
Results → Stream<CentralityStreamResult> → Bolt → driver
```

**Algorithms in this path:** 40 families (PageRank, WCC, Louvain, Dijkstra, BFS, 
triangle counting, betweenness centrality, etc.)

These are OLAP algorithms — whole-graph analytics, operate on projected graph.

### The OLAP API Boundary (The Key Interface)

Every GDS algorithm programs against this interface:

```java
interface Graph extends IdMap, NodePropertyContainer, Degrees, 
                        RelationshipIterator, RelationshipProperties {
    
    // From IdMap
    long toMappedNodeId(long originalNodeId);
    long toOriginalNodeId(long mappedNodeId);
    long nodeCount();
    
    // From Degrees  
    int degree(long nodeId);
    
    // From RelationshipIterator
    void forEachRelationship(long nodeId, RelationshipConsumer consumer);
    void forEachRelationship(long nodeId, double fallbackValue, 
                             RelationshipWithPropertyConsumer consumer);
    void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer);
    
    // From RelationshipProperties
    double relationshipProperty(long sourceNodeId, long targetNodeId);
    
    // Own methods
    long relationshipCount();
    boolean isMultiGraph();
    Graph concurrentCopy();
}
```

**This is the trait we implement in Rust.** Every algorithm sees this interface.
In GDS, it's backed by in-memory CSR. In Knight Bus, it's backed by 
mmap'd CSR (Level 1) or O_DIRECT streamed CSR (Level 2).

---

## Rust Multi-Crate Workspace Architecture

```
knight-bus/                          # Workspace root (Cargo.toml)
├── crates/
│   ├── kb-bolt/                     # Wire protocol (BoltR wrapper)
│   ├── kb-cypher/                   # Cypher parser + planner
│   ├── kb-values/                   # Type system
│   ├── kb-graph-api/                # Graph trait (the OLAP API boundary)
│   ├── kb-algo/                     # OLAP algorithms
│   ├── kb-algo-procedures/          # CALL gds.* dispatch
│   ├── kb-store-oltp/               # Record storage (15B/34B/41B)
│   ├── kb-store-olap/               # CSR storage + mmap/O_DIRECT
│   ├── kb-kernel/                   # Transaction engine
│   ├── kb-index/                    # Indexing framework
│   ├── kb-wal/                      # Write-ahead log
│   ├── kb-config/                   # Configuration
│   ├── kb-server/                   # Server (main binary)
│   └── kb-import/                   # Bulk import tool
└── Cargo.toml                       # Workspace definition
```

---

## Complete Folder Mapping

### Neo4j Community → Knight Bus Crates

| # | Neo4j Folder | LOC | Language | → Rust Crate | Est. Rust LOC | Notes |
|---|---|---|---|---|---|---|
| | **═══ WIRE PROTOCOL ═══** | | | | | |
| 1 | `bolt/` | 72,982 | Java | **`kb-bolt/`** | ~450 | BoltR does 95% — we add integration glue |
| | **═══ CYPHER ENGINE ═══** | | | | | |
| 2 | `cypher/front-end/parser/` | ~40,000 | Java | **`kb-cypher/`** (parser module) | ~3,000 | ANTLR4 grammar → hand-rolled Rust parser |
| 3 | `cypher/front-end/ast/` | ~30,000 | Scala | **`kb-cypher/`** (ast module) | ~2,000 | AST enums — Rust enums are perfect |
| 4 | `cypher/front-end/frontend/` | ~20,000 | Scala | **`kb-cypher/`** (semantic) | ~1,500 | Semantic analysis |
| 5 | `cypher/front-end/rewriting/` | ~15,000 | Scala | **`kb-cypher/`** (rewriting) | ~1,000 | AST rewrites/optimizations |
| 6 | `cypher/front-end/expressions/` | ~15,000 | Scala | **`kb-cypher/`** (expressions) | ~1,000 | Expression evaluation |
| 7 | `cypher/front-end/util/` | ~5,000 | Scala | **`kb-cypher/`** | ~300 | Utilities |
| 8 | `cypher/cypher-planner/` | 181,802 | Scala | **`kb-cypher/`** (planner) | ~8,000 | Cost-based query planner — HARDEST part |
| 9 | `cypher/cypher-logical-plans/` | 14,605 | Scala | **`kb-cypher/`** (logical plans) | ~1,000 | Logical plan data types |
| 10 | `cypher/ir/` | 9,740 | Scala | **`kb-cypher/`** (IR) | ~700 | Intermediate representation |
| 11 | `cypher/physical-planning/` | 10,493 | Scala | **`kb-cypher/`** (physical) | ~800 | Physical plan generation |
| 12 | `cypher/interpreted-runtime/` | 59,267 | Scala | **`kb-cypher/`** (runtime) | ~3,000 | Interpreted query execution |
| 13 | `cypher/slotted-runtime/` | 19,674 | Scala | (merged into runtime) | — | Slotted = optimized interpreted |
| 14 | `cypher/runtime-util/` | 35,381 | Java+Scala | **`kb-cypher/`** (runtime util) | ~1,500 | BFS cursors, traversal helpers |
| 15 | `cypher/cypher-config/` | 1,991 | Scala | **`kb-config/`** | ~100 | Cypher-specific config |
| 16 | `cypher/cypher-cache/` | 1,597 | Scala | **`kb-cypher/`** | ~200 | Prepared statement cache |
| 17 | `cypher/planner-spi/` | 1,412 | Scala | **`kb-cypher/`** | ~100 | Planner SPI |
| 18 | `cypher/graph-counts/` | 809 | Scala | **`kb-cypher/`** | ~100 | Statistics for cost model |
| 19 | `cypher/logical-plan-builder/` | 7,847 | Scala | **`kb-cypher/`** | ~500 | Test utility (skip for now) |
| 20 | `cypher/expression-evaluator/` | 1,369 | Java+Scala | **`kb-cypher/`** | ~100 | Expression eval |
| 21 | `cypher/cypher-rendering/` | 81 | Scala | **`kb-cypher/`** | ~20 | Plan rendering |
| | **CYPHER SUBTOTAL** | **~450K** | | | **~25,000** | |
| | **═══ STORAGE ENGINE ═══** | | | | | |
| 22 | `record-storage-engine/` | 96,165 | Java | **`kb-store-oltp/`** | ~8,000 | 15B node / 34B rel / 41B property records |
| 23 | `kernel/` | 121,729 | Java | **`kb-kernel/`** | ~10,000 | Transaction engine, cursors, store access |
| 24 | `kernel-api/` | 35,297 | Java | **`kb-graph-api/`** (OLTP part) | ~2,000 | StorageEngine, cursor interfaces |
| 25 | `io/` | 24,123 | Java | **`kb-store-oltp/`** (io module) | ~2,000 | Page cache (Muninn) → mmap in Rust |
| 26 | `wal/` | 13,823 | Java | **`kb-wal/`** | ~2,000 | Write-ahead log |
| 27 | `storage-engine-util/` | 11,206 | Java | **`kb-store-oltp/`** | ~500 | Format utilities |
| 28 | `id-generator/` | 15,406 | Java | **`kb-kernel/`** | ~1,000 | Dense ID allocation |
| | **═══ TYPE SYSTEM ═══** | | | | | |
| 29 | `values/` | 33,192 | Java | **`kb-values/`** | ~2,000 | All Neo4j value types |
| 30 | `token-api/` | 2,378 | Java | **`kb-values/`** | ~200 | Label/RelType/PropertyKey tokens |
| 31 | `neo4j-gql-status/` | 9,346 | Java | **`kb-values/`** (errors) | ~500 | GQL error codes |
| 32 | `neo4j-exceptions/` | 4,165 | Java | **`kb-values/`** (errors) | ~300 | Exception hierarchy → Rust error types |
| | **═══ INDEXING ═══** | | | | | |
| 33 | `index/` | 21,735 | Java | **`kb-index/`** | ~2,000 | Index framework |
| 34 | `lucene-index/` | 19,204 | Java | **`kb-index/`** (fulltext) | ~2,000 | Lucene → tantivy in Rust |
| 35 | `fulltext-index/` | 5,176 | Java | **`kb-index/`** (fulltext) | ~500 | Fulltext provider |
| 36 | `spatial-index/` | 3,087 | Java | **`kb-index/`** (spatial) | ~500 | Spatial indexes |
| 37 | `schema/` | 10,131 | Java | **`kb-index/`** (schema) | ~800 | Schema objects |
| | **═══ PROCEDURES ═══** | | | | | |
| 38 | `procedure/` | 21,112 | Java | **`kb-algo-procedures/`** (builtins) | ~1,000 | Built-in procedures (db.*, dbms.*) |
| 39 | `procedure-api/` | 1,121 | Java | **`kb-algo-procedures/`** | ~100 | Procedure API |
| 40 | `procedure-compiler/` | 6,881 | Java | SKIP | — | Annotation processor (not needed in Rust) |
| | **═══ GRAPH ALGORITHMS ═══** | | | | | |
| 41 | `graph-algo/` | 7,159 | Java | **`kb-algo/`** (oltp module) | ~1,000 | Dijkstra, A*, BFS (OLTP, cursor-based) |
| | **═══ SERVER ═══** | | | | | |
| 42 | `server/` | 29,702 | Java | **`kb-server/`** | ~2,000 | HTTP server, management endpoints |
| 43 | `server-api/` | 1,953 | Java | **`kb-server/`** | ~200 | Server API |
| 44 | `dbms/` | 15,139 | Java | **`kb-server/`** (dbms) | ~1,000 | Database management, lifecycle |
| 45 | `graphdb-api/` | 13,041 | Java | **`kb-graph-api/`** | ~1,000 | Top-level GraphDatabaseService |
| | **═══ CONFIGURATION ═══** | | | | | |
| 46 | `configuration/` | 16,221 | Java | **`kb-config/`** | ~1,000 | neo4j.conf parser |
| 47 | `capabilities/` | 1,859 | Java | **`kb-config/`** | ~100 | Feature capabilities |
| | **═══ INFRASTRUCTURE ═══** | | | | | |
| 48 | `collections/` | 16,762 | Java | SKIP | — | Off-heap collections (Rust doesn't need) |
| 49 | `concurrent/` | 2,832 | Java | SKIP | — | Thread utils (Rust has std::sync) |
| 50 | `common/` | 21,824 | Java | (spread across crates) | ~500 | Shared utilities |
| 51 | `logging/` | 6,528 | Java | SKIP | — | We use tracing crate |
| 52 | `ssl/` | 2,643 | Java | SKIP | — | BoltR has TLS support |
| 53 | `security/` | 5,542 | Java | **`kb-server/`** (auth) | ~500 | Auth providers |
| 54 | `native/` | 856 | Java | SKIP | — | JNI (not needed) |
| 55 | `unsafe/` | 2,130 | Java | SKIP | — | Unsafe memory (Rust handles this) |
| 56 | `lock/` | 8,386 | Java | **`kb-kernel/`** (locks) | ~600 | Lock manager |
| | **═══ IMPORT/EXPORT ═══** | | | | | |
| 57 | `import-util/` | 32,198 | Java | **`kb-import/`** | ~2,000 | Batch import framework |
| 58 | `import-tool/` | 4,862 | Java | **`kb-import/`** | ~500 | neo4j-admin import CLI |
| 59 | `import-api/` | 1,930 | Java | **`kb-import/`** | ~200 | Import SPI |
| 60 | `csv/` | 7,673 | Java | **`kb-import/`** (csv) | ~500 | CSV parsing (LOAD CSV) |
| | **═══ SKIP (testing/CI/enterprise) ═══** | | | | | |
| 61 | `community-it/` | 240,762 | — | SKIP | — | Integration tests |
| 62 | `kernel-test/` | 78,066 | — | SKIP | — | Kernel tests |
| 63 | `testing/` | 28,876 | — | SKIP | — | Test framework |
| 64 | `kernel-test-utils/` | 10,887 | — | SKIP | — | Test utils |
| 65 | `gbptree-tests/` | 23,880 | — | SKIP | — | GBP-tree tests |
| 66 | `cypher-shell/` | 25,088 | — | SKIP | — | CLI client (use existing) |
| 67+ | remaining 8 folders | ~40,000 | — | SKIP | — | codegen, fabric, cloud, etc. |

**Neo4j Community → Rust subtotal: ~50,000 LOC**

---

### Neo4j GDS → Knight Bus Crates

| # | GDS Folder | LOC | → Rust Crate | Est. Rust LOC | Notes |
|---|---|---|---|---|---|
| | **═══ ALGORITHMS ═══** | | | | |
| 1 | `algo/` | 87,393 | **`kb-algo/`** | ~10,000 | 40 algorithm families → direct CSR |
| | • `algo/pagerank/` | ~800 | `kb-algo/centrality/pagerank.rs` | ~150 | Pregel→direct iteration |
| | • `algo/wcc/` | ~500 | `kb-algo/community/wcc.rs` | ~100 | Union-Find |
| | • `algo/louvain/` | ~800 | `kb-algo/community/louvain.rs` | ~200 | Community detection |
| | • `algo/paths/dijkstra/` | ~1,000 | `kb-algo/pathfinding/dijkstra.rs` | ~200 | Weighted shortest path |
| | • `algo/traversal/` | ~500 | `kb-algo/pathfinding/bfs_dfs.rs` | ~100 | BFS/DFS |
| | • `algo/betweenness/` | ~600 | `kb-algo/centrality/betweenness.rs` | ~150 | Betweenness centrality |
| | • `algo/triangle/` | ~500 | `kb-algo/community/triangle.rs` | ~100 | Triangle counting |
| | • `algo/scc/` | ~500 | `kb-algo/community/scc.rs` | ~100 | Strongly connected components |
| | • `algo/kcore/` | ~300 | `kb-algo/community/kcore.rs` | ~80 | K-core decomposition |
| | • `algo/leiden/` | ~600 | `kb-algo/community/leiden.rs` | ~150 | Leiden algorithm |
| | • `algo/similarity/knn/` | ~800 | `kb-algo/similarity/knn.rs` | ~200 | K-nearest neighbors |
| | • `algo/similarity/nodesim/` | ~500 | `kb-algo/similarity/nodesim.rs` | ~150 | Node similarity |
| | • `algo/embeddings/fastrp/` | ~600 | `kb-algo/embeddings/fastrp.rs` | ~200 | FastRP embeddings |
| | • `algo/embeddings/node2vec/` | ~800 | `kb-algo/embeddings/node2vec.rs` | ~200 | Node2Vec |
| | • `algo/degree/` | ~300 | `kb-algo/centrality/degree.rs` | ~50 | Degree centrality |
| | • `algo/closeness/` | ~400 | `kb-algo/centrality/closeness.rs` | ~100 | Closeness centrality |
| | • `algo/harmonic/` | ~300 | `kb-algo/centrality/harmonic.rs` | ~80 | Harmonic centrality |
| | • `algo/hits/` | ~400 | `kb-algo/centrality/hits.rs` | ~100 | HITS (hubs/authorities) |
| | • `algo/labelpropagation/` | ~400 | `kb-algo/community/labelprop.rs` | ~100 | Label propagation |
| | • `algo/modularity*/` | ~500 | `kb-algo/community/modularity.rs` | ~120 | Modularity optimization |
| | • `algo/k1coloring/` | ~300 | `kb-algo/community/coloring.rs` | ~80 | Graph coloring |
| | • `algo/spanningtree/` | ~400 | `kb-algo/misc/spanning.rs` | ~100 | Spanning tree |
| | • `algo/steiner/` | ~500 | `kb-algo/misc/steiner.rs` | ~120 | Steiner tree |
| | • `algo/influenceMaximization/` | ~500 | `kb-algo/misc/influence.rs` | ~120 | CELF |
| | • `algo/kmeans/` | ~500 | `kb-algo/misc/kmeans.rs` | ~120 | K-means clustering |
| | • `algo/paths/astar/` | ~400 | `kb-algo/pathfinding/astar.rs` | ~100 | A* shortest path |
| | • `algo/paths/bellmanford/` | ~400 | `kb-algo/pathfinding/bellmanford.rs` | ~100 | Bellman-Ford |
| | • `algo/paths/yens/` | ~400 | `kb-algo/pathfinding/yens.rs` | ~100 | Yen's K shortest |
| | • `algo/paths/delta/` | ~300 | `kb-algo/pathfinding/delta.rs` | ~80 | Delta-stepping SSSP |
| | • `algo/dag/` | ~400 | `kb-algo/misc/dag.rs` | ~100 | Topo sort, longest path |
| | • `algo/scaleproperties/` | ~300 | `kb-algo/misc/scale.rs` | ~80 | Property scaling |
| | • `algo/articulationpoints/` | ~300 | `kb-algo/community/articulation.rs` | ~80 | Articulation points |
| | • `algo/bridges/` | ~300 | `kb-algo/community/bridges.rs` | ~80 | Bridge detection |
| | • `algo/conductance/` | ~200 | `kb-algo/community/conductance.rs` | ~50 | Conductance metric |
| | • `algo/sllpa/` | ~300 | `kb-algo/community/sllpa.rs` | ~80 | Speaker-Listener LPA |
| | • `algo/approxmaxkcut/` | ~400 | `kb-algo/misc/maxkcut.rs` | ~100 | Max K-Cut |
| | • `algo/walking/` | ~300 | `kb-algo/misc/randomwalk.rs` | ~80 | Random walk |
| | • remaining minor algos | ~2,000 | various | ~500 | |
| | **═══ GRAPH CORE ═══** | | | | |
| 2 | `core/` | 75,900 | **`kb-store-olap/`** | ~5,000 | CSR construction, compression, loading |
| | • `core/compression/` | 30,594 | `kb-store-olap/compression/` | ~2,000 | Packed adjacency compression |
| | • `core/loading/` | ~10,000 | `kb-store-olap/build/` | ~1,000 | CSR builder |
| | • `core/graph/` | ~15,000 | `kb-store-olap/graph/` | ~1,000 | HugeGraph, CSR implementation |
| | • `core/utils/` | ~10,000 | spread | ~500 | Partitioning, concurrency |
| 3 | `core-api/` | 4,430 | **`kb-graph-api/`** | ~500 | Graph trait (THE OLAP boundary) |
| 4 | `graph-projection-api/` | 3,448 | **`kb-graph-api/`** | ~200 | Projection SPI (simplified) |
| 5 | `graph-schema-api/` | 1,948 | **`kb-graph-api/`** | ~200 | Schema for projected graphs |
| 6 | `graph-dimensions/` | 319 | **`kb-graph-api/`** | ~50 | Graph size estimation |
| | **═══ PREGEL FRAMEWORK ═══** | | | | |
| 7 | `pregel/` | 5,888 | **`kb-algo/`** (pregel module) | ~500 | BSP framework (optional, most algos go direct) |
| | **═══ PROCEDURE DISPATCH ═══** | | | | |
| 8 | `proc/` | 71,416 | **`kb-algo-procedures/`** | ~3,000 | @Procedure entry points for all 40 algos |
| | • `proc/centrality/` | ~15,000 | procedures/centrality/ | ~600 | PageRank, betweenness, degree, etc. |
| | • `proc/community/` | ~20,000 | procedures/community/ | ~800 | WCC, Louvain, Leiden, etc. |
| | • `proc/path-finding/` | ~12,000 | procedures/pathfinding/ | ~500 | Dijkstra, BFS, A*, etc. |
| | • `proc/similarity/` | ~8,000 | procedures/similarity/ | ~300 | KNN, NodeSim |
| | • `proc/embeddings/` | ~6,000 | procedures/embeddings/ | ~300 | FastRP, Node2Vec |
| | • `proc/misc/` | ~5,000 | procedures/misc/ | ~200 | Spanning, Steiner, etc. |
| 9 | `procedures/` | 61,485 | **`kb-algo-procedures/`** | ~2,000 | Facade layer (business logic routing) |
| 10 | `native-projection/` | 5,379 | SKIP | — | Neo4j store scanner (we ARE the store) |
| | **═══ PROJECTION/IO ═══** | | | | |
| 11 | `io/` | 13,946 | **`kb-import/`** (graph-io) | ~1,000 | CSV, Arrow, Parquet export |
| 12 | `core-write/` | 4,246 | **`kb-store-olap/`** (writeback) | ~400 | Write results to store |
| 13 | `graph-construction/` | ~1,000 | **`kb-store-olap/`** (construct) | ~200 | Programmatic graph building |
| 14 | `graph-sampling/` | ~2,000 | **`kb-algo/`** (sampling) | ~300 | Graph sampling |
| | **═══ ML PIPELINE ═══** | | | | |
| 15 | `ml/` | 31,871 | **`kb-ml/`** (future) | ~3,000 | GraphSAGE, link prediction, etc. |
| 16 | `pipeline/` | 18,366 | **`kb-ml/`** (future) | ~1,500 | ML pipeline framework |
| | **═══ SUPPORT ═══** | | | | |
| 17 | `collections/` | 10,626 | SKIP | — | HugeArray → Rust Vec |
| 18 | `concurrency/` | 2,805 | SKIP | — | We use rayon |
| 19 | `executor/` | 2,059 | SKIP | — | We use rayon |
| 20 | `memory-estimation/` | ~2,000 | **`kb-algo/`** (memory) | ~200 | --ram-budget estimation |
| 21 | `config-api/` | ~3,000 | **`kb-config/`** | ~300 | Algorithm config interfaces |
| 22 | `algorithm-specifications/` | ~2,000 | **`kb-algo-procedures/`** | ~200 | Algorithm metadata |
| 23 | `progress-tracking/` | ~1,000 | **`kb-server/`** (progress) | ~100 | Progress bars |
| 24 | `subgraph-filtering/` | ~1,000 | **`kb-store-olap/`** | ~100 | Graph filtering |
| 25 | `cypher-aggregation/` | ~1,500 | **`kb-cypher/`** | ~200 | Custom aggregation |
| | **═══ SKIP ═══** | | | | |
| 26-67 | 42 folders | ~163K | SKIP | — | Tests, adapters, codegen, compat, licensing, etc. |

**GDS → Rust subtotal: ~20,000 LOC**

---

### BoltR → Knight Bus Crates

| # | BoltR Module | LOC | → Rust Crate | Notes |
|---|---|---|---|---|
| 1 | `server/` | 1,825 | **`kb-bolt/`** (re-export) | Wrap BoltR as dependency |
| 2 | `packstream/` | 1,145 | (transparent) | Used internally by kb-bolt |
| 3 | `message/` | 571 | (transparent) | Message types |
| 4 | `types/` | 330 | **`kb-values/`** (bridge) | BoltValue ↔ KbValue conversion |
| 5 | `chunk/` | 213 | (transparent) | Chunk framing |
| 6 | `client/` | 566 | **tests only** | Integration testing |
| 7 | `ws/` | 376 | **`kb-bolt/`** (feature flag) | WebSocket transport |
| 8 | `error.rs` | 112 | **`kb-bolt/`** | Error mapping |
| 9 | `version.rs` | 108 | (transparent) | Protocol negotiation |
| 10 | `lib.rs` | 82 | (transparent) | Crate root |

**BoltR: 0 LOC to rewrite, ~450 LOC to integrate**

---

## The Rust Crate Dependency Graph

```
                    kb-server (binary, ~3,000 LOC)
                   /    |    \
                  /     |     \
          kb-bolt   kb-cypher   kb-config
          (~450)    (~25,000)    (~1,500)
              \       |    \       /
               \      |     \     /
            kb-algo-procedures (~5,000)
                   |        \
                   |         \
               kb-algo      kb-graph-api
              (~10,000)      (~1,000)
              /    |    \        |
             /     |     \       |
            /      |      \      |
     kb-store-olap   kb-store-oltp   kb-kernel
      (~5,000)        (~8,000)       (~10,000)
           \            |            /
            \           |           /
             \          |          /
              kb-wal    kb-index   kb-values
             (~2,000)  (~5,000)   (~3,000)
                  \       |       /
                   \      |      /
                    kb-import
                    (~3,000)
```

**Total: 14 crates, ~70,000 LOC Rust**

---

## The OLAP APIs: What Cypher Sees

### Built-in OLTP Algorithms (from Cypher syntax)

These are called by the Cypher runtime directly, not via CALL:

```rust
// kb-algo/src/oltp.rs — built-in path algorithms

/// Called by: MATCH shortestPath((a)-[*]-(b))
pub fn shortest_path(
    store: &dyn OltpStore,
    start: NodeId,
    end: NodeId,
    max_depth: usize,
    expander: &dyn RelationshipExpander,
) -> Option<Path>;

/// Called by: MATCH allShortestPaths((a)-[*]-(b))
pub fn all_shortest_paths(
    store: &dyn OltpStore,
    start: NodeId,
    end: NodeId,
    max_depth: usize,
    expander: &dyn RelationshipExpander,
) -> Vec<Path>;

/// Called by: MATCH (a)-[*1..5]-(b) (variable-length path)
pub fn var_length_expand(
    store: &dyn OltpStore,
    start: NodeId,
    min_hops: usize,
    max_hops: usize,
    expander: &dyn RelationshipExpander,
) -> Vec<Path>;
```

### GDS OLAP Algorithms (from CALL procedures)

These are called via the procedure dispatch system:

```rust
// kb-graph-api/src/lib.rs — THE OLAP API BOUNDARY
// Every OLAP algorithm programs against this trait.

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

pub trait NodeProperties {
    fn double_value(&self, node_id: u64, key: &str) -> Option<f64>;
    fn long_value(&self, node_id: u64, key: &str) -> Option<i64>;
}
```

### Procedure Dispatch (the CALL routing)

```rust
// kb-algo-procedures/src/dispatch.rs — CALL gds.* routing

pub struct ProcedureRegistry {
    procedures: HashMap<String, Box<dyn Procedure>>,
}

impl ProcedureRegistry {
    pub fn register_all(&mut self) {
        // Centrality
        self.register("gds.pageRank.stream", PageRankStreamProc);
        self.register("gds.pageRank.stats", PageRankStatsProc);
        self.register("gds.articleRank.stream", ArticleRankStreamProc);
        self.register("gds.eigenvector.stream", EigenvectorStreamProc);
        self.register("gds.betweenness.stream", BetweennessStreamProc);
        self.register("gds.degree.stream", DegreeStreamProc);
        self.register("gds.closeness.stream", ClosenessStreamProc);
        self.register("gds.closeness.harmonic.stream", HarmonicStreamProc);
        self.register("gds.hits.stream", HitsStreamProc);
        
        // Community
        self.register("gds.wcc.stream", WccStreamProc);
        self.register("gds.louvain.stream", LouvainStreamProc);
        self.register("gds.leiden.stream", LeidenStreamProc);
        self.register("gds.labelPropagation.stream", LabelPropStreamProc);
        self.register("gds.scc.stream", SccStreamProc);
        self.register("gds.triangleCount.stream", TriangleCountStreamProc);
        self.register("gds.localClusteringCoefficient.stream", LccStreamProc);
        self.register("gds.kcore.stream", KcoreStreamProc);
        self.register("gds.k1coloring.stream", K1ColoringStreamProc);
        self.register("gds.modularity.stream", ModularityStreamProc);
        self.register("gds.modularityOptimization.stream", ModOptStreamProc);
        self.register("gds.conductance.stream", ConductanceStreamProc);
        self.register("gds.articulationPoints.stream", ArticulationStreamProc);
        self.register("gds.bridges.stream", BridgesStreamProc);
        self.register("gds.sllpa.stream", SllpaStreamProc);
        
        // Path Finding
        self.register("gds.shortestPath.dijkstra.stream", DijkstraStreamProc);
        self.register("gds.shortestPath.astar.stream", AstarStreamProc);
        self.register("gds.shortestPath.yens.stream", YensStreamProc);
        self.register("gds.allShortestPaths.dijkstra.stream", AllDijkstraStreamProc);
        self.register("gds.allShortestPaths.delta.stream", DeltaSteppingStreamProc);
        self.register("gds.bellmanFord.stream", BellmanFordStreamProc);
        self.register("gds.bfs.stream", BfsStreamProc);
        self.register("gds.dfs.stream", DfsStreamProc);
        self.register("gds.dag.topologicalSort.stream", TopoSortStreamProc);
        self.register("gds.dag.longestPath.stream", LongestPathStreamProc);
        self.register("gds.randomWalk.stream", RandomWalkStreamProc);
        
        // Similarity
        self.register("gds.nodeSimilarity.stream", NodeSimStreamProc);
        self.register("gds.knn.stream", KnnStreamProc);
        self.register("gds.knn.filtered.stream", KnnFilteredStreamProc);
        self.register("gds.nodeSimilarity.filtered.stream", NodeSimFilteredStreamProc);
        
        // Embeddings
        self.register("gds.fastRP.stream", FastRPStreamProc);
        self.register("gds.node2vec.stream", Node2VecStreamProc);
        self.register("gds.hashgnn.stream", HashGnnStreamProc);
        
        // Misc
        self.register("gds.spanningTree.stream", SpanningTreeStreamProc);
        self.register("gds.kSpanningTree.stream", KSpanningTreeStreamProc);
        self.register("gds.steinerTree.stream", SteinerTreeStreamProc);
        self.register("gds.prizeSteinerTree.stream", PrizeSteinerStreamProc);
        self.register("gds.influenceMaximization.celf.stream", CelfStreamProc);
        self.register("gds.collapsePath.mutate", CollapsePathMutateProc);
        self.register("gds.scaleProperties.stream", ScalePropertiesStreamProc);
        self.register("gds.kmeans.stream", KmeansStreamProc);
        self.register("gds.maxkcut.stream", MaxKCutStreamProc);
        
        // + .mutate, .write, .stats, .estimate variants for each
    }
}
```

---

## Summary Table

| Rust Crate | Source | Java/Scala LOC | Rust LOC | Purpose |
|---|---|---|---|---|
| **kb-bolt** | Neo4j `bolt/` + BoltR | 73K + 5.3K | ~450 | Wire protocol (BoltR dependency) |
| **kb-cypher** | Neo4j `cypher/` | ~450K | ~25,000 | Parser, planner, runtime |
| **kb-values** | Neo4j `values/`, `token-api/`, exceptions | ~50K | ~3,000 | Type system, errors |
| **kb-graph-api** | GDS `core-api/`, Neo4j `kernel-api/`, `graphdb-api/` | ~53K | ~1,000 | Graph trait (OLAP+OLTP boundary) |
| **kb-algo** | GDS `algo/`, Neo4j `graph-algo/` | ~95K | ~10,000 | All 40+ algorithm families |
| **kb-algo-procedures** | GDS `proc/`, `procedures/` | ~133K | ~5,000 | CALL dispatch + procedure facades |
| **kb-store-oltp** | Neo4j `record-storage-engine/`, `io/` | ~130K | ~8,000 | OLTP record stores |
| **kb-store-olap** | GDS `core/` | ~76K | ~5,000 | CSR storage + compression |
| **kb-kernel** | Neo4j `kernel/`, `lock/`, `id-generator/` | ~145K | ~10,000 | Transaction engine |
| **kb-wal** | Neo4j `wal/` | ~14K | ~2,000 | Write-ahead log |
| **kb-index** | Neo4j `index/`, `lucene-index/`, etc. | ~49K | ~5,000 | Indexes (tantivy for fulltext) |
| **kb-config** | Neo4j `configuration/`, GDS `config-api/` | ~21K | ~1,500 | Configuration |
| **kb-server** | Neo4j `server/`, `dbms/` | ~47K | ~3,000 | HTTP server, management |
| **kb-import** | Neo4j `import-*/`, `csv/`, GDS `io/` | ~60K | ~3,000 | Bulk import, CSV, export |
| **kb-ml** (future) | GDS `ml/`, `pipeline/` | ~50K | ~4,500 | ML pipelines (v1.0+) |
| **TOTAL** | | **~2.78M LOC** | **~70,000 LOC** | **40:1 ratio** |

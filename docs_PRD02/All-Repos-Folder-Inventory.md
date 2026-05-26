# Complete Folder Inventory: All Repos We Must Leverage

*Every top-level folder across Neo4j Community, Neo4j GDS, BoltR, and Neo4j Python Driver,
rated by relevance to the Knight Bus Rust rewrite.*

---

## Summary

| Repo | Location | Java/Rust LOC | Folders | What It Is |
|---|---|---|---|---|
| **Neo4j Community** | `neo4j-reference/neo4j/community/` | ~1.58M LOC Java | 67 folders | OLTP database engine |
| **Neo4j GDS** | `/home/ubuntu/repos/neo4j-gds/` | ~530K LOC Java | 67 folders | OLAP analytics plugin |
| **BoltR** | `/home/ubuntu/repos/boltr/` | ~5.3K LOC Rust | 10 modules | Bolt v5.x wire protocol |
| **Neo4j Python Driver** | `/home/ubuntu/repos/neo4j-python-driver/` | ~38K LOC Python | 112 files | Compatibility test target |

---

## Relevance Key

```
★★★ MUST STUDY  — We are directly reimplementing this in Rust
★★☆ SHOULD STUDY — Informs our design or provides reference
★☆☆ REFERENCE    — Useful for edge cases or compatibility
☆☆☆ SKIP         — Not relevant (testing infra, CI, enterprise-only)
```

---

## 1. Neo4j Community Edition (67 folders, ~1.58M LOC)

### ★★★ MUST STUDY (Core Engine — We Rewrite These)

| Folder | LOC | What It Does | Knight Bus Equivalent |
|---|---|---|---|
| **bolt** | 72,982 | Bolt protocol: PackStream, session FSM, connection handling | **BoltR crate** (already done in Rust!) |
| **cypher** | 168,426 | Cypher parser (ANTLR4), planner (181K!), 2 runtimes | Hand-rolled subset parser (v0.0.5+) |
| **kernel** | 121,729 | Transaction engine, store access, cursors, locks, ID allocation | Core engine (v0.0.7+) |
| **record-storage-engine** | 96,165 | 15B node / 34B rel / 41B property record stores | OLTP record stores (v0.0.7+ if "identical architecture") |
| **values** | 33,192 | Type system: Value, AnyValue, TextValue, NumberValue, etc. | Rust enums + BoltR types |
| **io** | 24,123 | Page cache (Muninn), file I/O, page cursors | mmap (Level 1) + O_DIRECT (Level 2) |
| **wal** | 13,823 | Write-ahead log: transaction log, checkpointing | WAL engine (v0.0.7+ for writes) |
| **index** | 21,735 | Index framework: provider, reader, updater, schema rules | CSR-based indexing (v0.0.7+) |
| **graph-algo** | 7,159 | Dijkstra, A*, BFS, Floyd-Warshall (built-in, NOT GDS) | Part of our algo engine |

### ★★☆ SHOULD STUDY (Design Reference)

| Folder | LOC | What It Does | Why Study |
|---|---|---|---|
| **kernel-api** | 35,297 | Kernel interfaces: StorageEngine, StoreReadLayer, cursors | Understand the API surface our OLTP must match |
| **configuration** | 16,221 | neo4j.conf parser, settings framework | Reference for our config system |
| **schema** | 10,131 | Schema objects: constraints, indexes, labels, rel types | Must understand for Cypher compatibility |
| **lock** | 8,386 | Lock manager: ForsetiLockManager, read/write locks | Reference for transaction isolation |
| **procedure** | 21,112 | Procedure framework: @Procedure annotation, context injection | Reference for CALL dispatch |
| **procedure-api** | 1,121 | Procedure API interfaces | API contract we must match |
| **server** | 29,702 | HTTP server, REST API, management endpoints | Reference (we might skip HTTP, Bolt-only) |
| **id-generator** | 15,406 | ID generation: FreelistIdGenerator, markAsDeleted | Reference for dense ID allocation |
| **collections** | 16,762 | Off-heap collections: HugeLongArray, LongLongMap | We use Rust Vec (no GC = no need for off-heap) |
| **storage-engine-util** | 11,206 | Storage engine utilities, format compatibility | Reference for record format versioning |
| **graphdb-api** | 13,041 | GraphDatabaseService, Transaction, Result interfaces | The top-level API surface |

### ★☆☆ REFERENCE (Edge Cases)

| Folder | LOC | What It Does | When Needed |
|---|---|---|---|
| **lucene-index** | 19,204 | Lucene-based indexes (text search) | If we support fulltext search |
| **fulltext-index** | 5,176 | Full-text index provider | If we support fulltext search |
| **spatial-index** | 3,087 | Spatial/geometric indexes | If we support spatial types |
| **csv** | 7,673 | CSV parsing for LOAD CSV | If we support LOAD CSV |
| **import-util** | 32,198 | Batch import framework | Reference for our build pipeline |
| **import-tool** | 4,862 | neo4j-admin import CLI | Reference for bulk import |
| **concurrent** | 2,832 | Thread utilities, latches | Rust has std::sync |
| **common** | 21,824 | Shared utilities | Cherry-pick as needed |
| **dbms** | 15,139 | Database management, lifecycle | Reference for multi-DB (v1.0+) |
| **security** | 5,542 | Auth providers, security manager | If we support auth |
| **token-api** | 2,378 | Label/RelType/PropertyKey token IDs | Must match for Cypher compat |
| **logging** | 6,528 | Log framework | We use tracing crate |
| **ssl** | 2,643 | TLS configuration | BoltR has TLS support |
| **neo4j-gql-status** | 9,346 | GQL error status codes | Compatibility (error messages) |
| **neo4j-exceptions** | 4,165 | Exception hierarchy | Map to Rust error types |

### ☆☆☆ SKIP (Not Relevant)

| Folder | LOC | Why Skip |
|---|---|---|
| **community-it** | 240,762 | Integration tests (useful later, not for architecture) |
| **kernel-test** | 78,066 | Kernel unit tests |
| **testing** | 28,876 | Test framework utilities |
| **kernel-test-utils** | 10,887 | Test utilities |
| **gbptree-tests** | 23,880 | GBP-tree specific tests |
| **cypher-shell** | 25,088 | CLI Cypher client (we'd use existing cypher-shell) |
| **codegen** | 12,884 | Code generation for compiled Cypher runtime |
| **fabric** | 13,629 | Enterprise multi-database queries (not Community) |
| **cloud** | 5,940 | Cloud deployment |
| **push-to-cloud** | 5,165 | Aura cloud push |
| **genai-plugin** | 5,790 | GenAI integration |
| **data-collector** | 2,707 | Telemetry |
| **consistency-check** | 1,584 | Store consistency checker |
| **neo4j-harness** | 3,420 | Embedded test harness |
| **neo4j-notifications** | 3,445 | Query notifications |
| **neo4j-slf4j-provider** | 1,409 | SLF4J adapter |
| **neo4j** | 16,166 | Top-level Neo4j assembly |
| **neo4j-community** | — | Assembly module |
| **server-test-utils** | 2,735 | Server test utilities |
| **server-api** | 1,953 | Server API |
| **monitoring** | 840 | JMX monitoring |
| **capabilities** | 1,859 | Feature capability negotiation |
| **procedure-compiler** | 6,881 | Annotation processor for procedures |
| **command-line** | 1,315 | CLI argument parsing |
| **layout** | 1,154 | Store file layout |
| **native** | 856 | JNI native utilities |
| **unsafe** | 2,130 | Unsafe memory operations |
| **bootcheck** | 135 | Boot environment checks |
| **diagnostics** | 179 | System diagnostics |
| **resource** | 259 | Resource tracking |
| **udc** | 334 | Usage data collection |
| **import-api** | 1,930 | Import SPI |
| **arrow-bom** | — | Apache Arrow BOM |
| **zstd-proxy** | — | Zstd compression wrapper |

---

## 2. Neo4j GDS (67 folders, ~530K LOC)

### ★★★ MUST STUDY (Algorithm Engine — We Rewrite These)

| Folder | LOC | What It Does | Knight Bus Equivalent |
|---|---|---|---|
| **algo** | 87,393 | ALL algorithm implementations: 40 families | Our OLAP algorithm engine |
| **core** | 75,900 | Graph representation, CSR, compression (30K LOC!), loading | Our CSR engine + compression |
| **pregel** | 5,888 | Pregel BSP framework (PageRank, HITS, etc. built on this) | Direct CSR iteration (faster) |
| **native-projection** | 5,379 | Scans Neo4j stores → builds in-memory CSR | We skip this (our storage IS CSR) |
| **proc** | 71,416 | Procedure entry points (@Procedure annotations) | Our CALL dispatch |
| **procedures** | 61,485 | Procedure facade layer (business logic routing) | Our procedure registry |

### ★★☆ SHOULD STUDY (Design Reference)

| Folder | LOC | What It Does | Why Study |
|---|---|---|---|
| **core-api** | 4,430 | Graph interface: `Graph`, `RelationshipIterator`, `NodeProperties` | API contract algorithms program against |
| **graph-projection-api** | 3,448 | Graph projection SPI | How GDS abstracts graph access |
| **graph-schema-api** | 1,948 | Schema for projected graphs (labels, rel types, properties) | Reference for our schema model |
| **core-write** | 4,246 | Writing results back to Neo4j | Reference for .write mode |
| **collections** | 10,626 | HugeArray, HugeLongArray, off-heap collections | We use Rust Vec (simpler) |
| **concurrency** | 2,805 | Concurrency utilities for algorithms | We use rayon |
| **executor** | 2,059 | Task execution framework | We use rayon thread pool |
| **io** | 13,946 | Import/export (CSV, Arrow, Parquet) | Reference for data exchange |
| **memory-estimation** | — | Memory estimation for algorithms | Reference for --ram-budget |
| **memory-usage** | — | Memory tracking | Reference |
| **algorithm-specifications** | — | Algorithm metadata (modes, params) | Reference for procedure registration |
| **config-api** | — | Algorithm configuration interfaces | Reference for PageRank config, etc. |
| **graph-dimensions** | 319 | Graph size estimation | Reference |
| **graph-sampling** | — | Graph sampling algorithms | Later (v0.1.0+) |
| **graph-construction** | — | Programmatic graph construction | Later |

### ★☆☆ REFERENCE

| Folder | LOC | What It Does | When Needed |
|---|---|---|---|
| **ml** | 31,871 | Machine learning pipelines (GraphSAGE, KNN, etc.) | Much later (v1.0+) |
| **pipeline** | 18,366 | ML pipeline framework | Much later |
| **subgraph-filtering** | — | Filter projected graphs | Later |
| **cypher-aggregation** | — | Custom Cypher aggregation | Later |
| **legacy-cypher-projection** | — | Old-style Cypher projection | Skip (legacy) |
| **progress-tracking** | — | Progress bars for long algorithms | Nice-to-have |
| **logging** | — | GDS logging | We use tracing |

### ☆☆☆ SKIP

| Folder | Why Skip |
|---|---|
| **algo-test**, **algo-common**, **algo-params** | Test infrastructure |
| **annotations**, **config-generator**, **pregel-proc-generator** | Java annotation processing (not needed in Rust) |
| **compatibility**, **neo4j-adapter**, **neo4j-api**, **neo4j-settings** | Neo4j version compat shims |
| **doc**, **doc-test**, **doc-test-tools** | Documentation |
| **edition-api**, **licensing**, **open-licensing** | License management |
| **examples** | Example code |
| **etc**, **gradle** | Build system |
| **gds-values**, **neo4j-values**, **neo4j-log-adapter** | Type adapters |
| **test-utils**, **test-graph-loaders**, **core-test-utils** | Test utilities |
| **collections-generator**, **collections-memory-estimation** | Code generation |
| **string-formatting** | String utilities |
| **termination** | Cancellation support |
| **transaction** | Transaction helpers |
| **metrics-api** | Metrics |
| **model-catalog-api**, **open-model-catalog** | ML model storage |
| **procedure-collector** | Procedure discovery |
| **open-packaging**, **open-write-services** | Packaging |
| **defaults-and-limits-configuration** | Config defaults |
| **snowgraph** | Internal tool |
| **triplet-graph-builder** | Internal |
| **alpha**, **applications**, **dependencies** | Various |

---

## 3. BoltR (10 modules, ~5.3K LOC Rust)

### ★★★ USE DIRECTLY (Add as Dependency)

| Module | LOC | What It Does | Our Usage |
|---|---|---|---|
| **server/** | 1,825 | Bolt server: backend trait, connection handler, state machine, handshake, auth | **Implement `BoltBackend` trait** |
| **packstream/** | 1,145 | PackStream binary encoding/decoding | Transparent (used by server) |
| **message/** | 571 | Bolt message types (HELLO, RUN, PULL, etc.) | Transparent |
| **types/** | 330 | BoltValue enum (NULL, INT, FLOAT, STRING, NODE, etc.) | Convert our types ↔ BoltValue |
| **chunk/** | 213 | Chunk framing (2-byte length-prefix) | Transparent |
| **error.rs** | 112 | BoltError type | Map to our errors |
| **version.rs** | 108 | Protocol version negotiation | Transparent |
| **lib.rs** | 82 | Crate root, re-exports | Import |
| **client/** | 566 | Bolt client (for testing) | **Use for integration tests** |
| **ws/** | 376 | WebSocket transport (optional) | Later (v0.1.0+) |

---

## 4. Neo4j Python Driver (~38K LOC Python)

### ★★☆ USE FOR TESTING

| What | Purpose |
|---|---|
| `src/neo4j/` | The driver we test against — verify our Bolt server works |
| `tests/` | Test patterns we can adapt |
| `testkitbackend/` | Test kit for protocol compliance |

---

## The Build Order (Which Folders When)

### v0.0.3 (Next 2 Weeks) — Bolt + PageRank

| Source Folder | What We Take | Knight Bus Location |
|---|---|---|
| **BoltR** (entire crate) | `boltr = "0.2"` dependency | `Cargo.toml` |
| **GDS algo/pagerank/** | PageRank algorithm logic (~115 LOC Java → ~150 LOC Rust) | `src/algo/pagerank.rs` |
| **GDS proc/centrality/** | CALL gds.pageRank.stream API surface | `src/procedures/` |
| **GDS core-api/** | Graph trait (forEachRelationship, nodeCount) | `src/algo/graph.rs` |
| **Neo4j bolt/** | Reference for driver compatibility edge cases | — |

### v0.0.5 (Month 2) — More Algorithms + Cypher Subset

| Source Folder | What We Take |
|---|---|
| **GDS algo/wcc/** | Weakly Connected Components |
| **GDS algo/louvain/** | Louvain community detection |
| **GDS algo/paths/dijkstra/** | Dijkstra shortest path |
| **GDS algo/traversal/** | BFS/DFS |
| **Neo4j cypher/** | ANTLR4 grammar reference for subset parser |
| **Neo4j values/** | Type system reference |

### v0.0.7 (Month 3) — OLTP + Write Path

| Source Folder | What We Take |
|---|---|
| **Neo4j record-storage-engine/** | Record format reference (if "identical architecture") |
| **Neo4j kernel/** | Transaction engine, cursors, store access patterns |
| **Neo4j wal/** | WAL design reference |
| **Neo4j io/** | Page cache design reference (we use mmap instead) |
| **Neo4j index/** | Index framework reference |

### v0.1.0 (Quarter 2) — Production Ready

| Source Folder | What We Take |
|---|---|
| **Neo4j configuration/** | Config system reference |
| **Neo4j schema/** | Schema/constraint reference |
| **Neo4j lock/** | Lock manager reference |
| **GDS algo/** (remaining) | Triangle counting, k-core, betweenness, etc. |
| **GDS io/** | Import/export reference |

---

## The Numbers

### Total LOC We Must Understand

| Category | Source LOC | What We Build | Est. Rust LOC |
|---|---|---|---|
| Bolt protocol | 73K Java | **Use BoltR** (~450 LOC integration) | ~450 |
| OLAP algorithms (top 5) | ~15K Java (subset of 87K) | Direct CSR implementations | ~2,000 |
| OLAP algorithms (all 40) | 87K Java | Direct CSR implementations | ~10,000 |
| Cypher (subset) | ~30K Java (of 168K) | Hand-rolled subset parser | ~5,000 |
| OLTP record stores | 96K Java | Rust record stores | ~15,000 |
| Kernel (transactions) | 122K Java | Transaction engine | ~20,000 |
| **Total for v0.1.0** | **~340K Java** | **Rust equivalent** | **~25-30K** |

The 12:1 ratio (Java → Rust) comes from:
- No GC overhead code (off-heap collections, unsafe memory)
- No annotation processing / code generation
- No plugin framework / service loading
- Rust enums replace 50+ Value subclasses
- BoltR replaces 73K LOC of Bolt protocol

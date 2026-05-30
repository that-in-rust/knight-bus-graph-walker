# Docs Readme — Cloned Competitor Repositories

> This file lists every competitor repository that was cloned and read during the
> Knight Bus replacement analysis. For each repo, we list the clone URL, the
> branch/tag at time of cloning, the language, and the key source files that were
> inspected. All repos were cloned to `/home/ubuntu/` on the analysis machine.

---

## Repositories

### 1. Neo4j (Core Database)

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/neo4j-src` |
| Origin | `https://github.com/neo4j/neo4j.git` |
| Branch | `release/5.26.0` (tag `5.26.1`) |
| Language | Java |

**Key files read:**

- `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java`
  — 15-byte fixed node records with bit-packed fields
- `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java`
  — 34-byte relationship records with four linked-list pointers (doubly-linked chain per endpoint)
- `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipGroupRecordFormat.java`
  — 25-byte relationship group records (per-type first-relationship pointers)
- `community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java`
  — 41-byte property records with prev/next pointers (linked-list property chain)

**Used in:** `docs/competitor-algorithm-innovations.md` innovations #1-#4

---

### 2. Neo4j Graph Data Science (GDS)

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/neo4j-gds-src` |
| Origin | `https://github.com/neo4j/graph-data-science.git` |
| Branch | `2.13` |
| Language | Java |

**Key files read:**

- `algo/src/main/java/org/neo4j/gds/pagerank/PageRankComputation.java`
  — Pregel-based iterative PageRank (message-passing framework)
- `algo/src/main/java/org/neo4j/gds/louvain/Louvain.java`
  — Multi-level community detection with graph summarization
- `algo/src/main/java/org/neo4j/gds/labelpropagation/LabelPropagation.java`
  — Iterative label propagation with HugeLongArray
- `core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStore.java`
  — Neo4j GDS's own CSR representation (validates that CSR is the right format)

**Used in:** `docs/competitor-algorithm-innovations.md` innovations #5-#8

---

### 3. Neo4j APOC

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/neo4j-apoc-src` |
| Origin | `https://github.com/neo4j/apoc.git` |
| Branch | `dev` |
| Language | Java |

**Key files read:**

- Explored for extended procedure patterns and utility functions
- Less directly used in innovations document (APOC is a procedure library, not a storage engine)

**Used in:** Background context for Neo4j ecosystem understanding

---

### 4. DuckDB

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/duckdb-src` |
| Origin | `https://github.com/duckdb/duckdb.git` |
| Branch | `main` (commit `932e831`) |
| Language | C++ |

**Key files read:**

- `src/execution/operator/join/physical_hash_join.cpp`
  — Hash join construction with radix partitioning
- `src/execution/operator/join/perfect_hash_join_executor.cpp`
  — Perfect hash join: direct array indexing for small integer key ranges (`MAX_BUILD_SIZE = 1048576`)
- `src/execution/operator/aggregate/physical_perfecthash_aggregate.cpp`
  — Perfect hash aggregate: array-indexed GROUP BY for small key ranges
- `src/include/duckdb/common/types/vector.hpp` (and related)
  — Vectorized execution model (2048-row batches)

**Used in:** `docs/competitor-algorithm-innovations.md` innovations #9-#12, `docs/olap-innovations.md`

---

### 5. ClickHouse

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/clickhouse-src` |
| Origin | `https://github.com/ClickHouse/ClickHouse.git` |
| Branch | `master` (commit `170229cf`) |
| Language | C++ |

**Key files read:**

- `src/Storages/StorageMergeTree.cpp`
  — MergeTree engine: immutable sorted parts with background merge
- `src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp`
  — Granule-based writing with mark files (index granularity, typically 8192 rows per granule)
- `src/Storages/MergeTree/MergeTreeDataMergerMutator.cpp`
  — Background merge/compaction logic

**Used in:** `docs/competitor-algorithm-innovations.md` innovations #13-#15, `docs/mutation-strategy-analysis.md` (ClickHouse's immutable-parts-with-delta pattern directly inspired the Delta Layer proposal)

---

### 6. Apache AGE

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/age-src` |
| Origin | `https://github.com/apache/age.git` |
| Branch | `master` (commit `9960e9c`) |
| Language | C (PostgreSQL extension) |

**Key files read:**

- `src/backend/utils/adt/age_vle.c`
  — Variable-length edge (VLE) traversal: DFS with hash table tracking edge state (`edge_state_entry` struct at ~40 bytes per entry, initialized at 100K entries)
- `src/backend/utils/adt/age_global_graph.c`
  — Global graph context: vertex and edge hash tables for in-memory graph representation (`vertex_entry` with `ListGraphId` adjacency lists)

**Used in:** `docs/competitor-algorithm-innovations.md` innovations #16-#18

---

### 7. Memgraph

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/memgraph-src` |
| Origin | `https://github.com/memgraph/memgraph.git` |
| Branch | `master` (commit `674b3ee`) |
| Language | C++ |

**Key files read:**

- `src/storage/v2/vertex.hpp`
  — 80-byte vertex struct with `small_vector<EdgeTriple>` for in/out edges (pointer-based adjacency)
- `src/storage/v2/edge.hpp`
  — Edge struct with MVCC delta chains for version control

**Used in:** `docs/competitor-algorithm-innovations.md` innovations #19-#20

---

### 8. ArangoDB

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/arangodb-src` |
| Origin | `https://github.com/arangodb/arangodb.git` |
| Branch | `devel` (commit `35aa2e414`) |
| Language | C++ |

**Key files read:**

- `arangod/Graph/Traverser.cpp` and related graph traversal code
  — RocksDB-backed key-value graph traversal (bloom filter → binary search → decompress per edge lookup)
- `arangod/RocksDBEngine/` directory
  — RocksDB storage engine integration

**Used in:** `docs/competitor-algorithm-innovations.md` innovation #21

---

### 9. JanusGraph

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/janusgraph-src` |
| Origin | `https://github.com/JanusGraph/janusgraph.git` |
| Branch | `master` (commit `3ed2758`) |
| Language | Java |

**Key files read:**

- `janusgraph-core/src/main/java/org/janusgraph/graphdb/database/EdgeSerializer.java`
  — Wide-row edge serialization with variable-length encoding (direction, type, vertex ID, properties encoded as byte blobs in Cassandra/HBase)
- `janusgraph-core/src/main/java/org/janusgraph/diskstorage/keycolumnvalue/`
  — KeyColumnValueStore abstraction over pluggable backends

**Used in:** `docs/competitor-algorithm-innovations.md` innovation #22

---

### 10. TigerGraph Ecosystem

| Field | Value |
|---|---|
| Local path | `/home/ubuntu/tigergraph-ecosys-src` |
| Origin | `https://github.com/tigergraph/ecosys.git` |
| Branch | `master` (commit `248e6fa`) |
| Language | Mixed |

**Key files read:**

- Explored for GSQL examples and ecosystem tooling
- TigerGraph's core engine is proprietary (not in this repo)
- This repo contains sample code, connectors, and Kubernetes operator configs

**Used in:** Not directly cited in innovations (proprietary core engine not available)

---

## Documents Produced from This Research

| Document | Description |
|---|---|
| `docs/neo4j-pain-points.md` | 18 categories of Neo4j pain points with 44 references (web research) |
| `docs/olap-innovations.md` | 3 OLAP innovations reasoned from Knight Bus source code |
| `docs/competitor-algorithm-innovations.md` | 24 innovations derived from reading all competitor source code |
| `docs/mutation-strategy-analysis.md` | Mutation strategy analysis: Paged CSR vs Delta Layer, with 50 GB simulation and rubber-duck corrections |

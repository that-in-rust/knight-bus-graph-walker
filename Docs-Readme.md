# Git Reference Repository Readme

This document accounts for the external source repositories cloned for Knight
Bus v003 architecture study. The actual clones live under `gitrefrepo/` at the
repo root and are intentionally ignored by git via `.gitignore`.

The goal of this corpus is not to vendor code. It is a local research shelf for
studying proven implementations of graph storage, graph query languages,
analytics engines, columnar execution, low-memory formats, and Rust log/stream
systems that may inform the Knight Bus v003 design.

## Local Layout

```text
knight-bus-graph-walker/
  Docs-Readme.md
  gitrefrepo/                 # ignored local clone shelf
    neo4j-src/
    neo4j-gds-src/
    apache-iggy-src/
    apache-datafusion-src/
    ...
```

## Clone Policy

Most repos were cloned shallowly and bloblessly where possible:

```text
git clone --filter=blob:none --depth=1 --no-tags <url> gitrefrepo/<name>
```

That keeps this folder usable as a research cache rather than a vendored source
tree. Some repos were migrated from the older `ref-repo-folder/` shelf and may
not be shallow. Commit SHAs below are the actual local checkouts verified on
2026-05-27.

## Study Priorities

1. **Neo4j compatibility baseline**: Bolt, Cypher, record storage, procedures,
   GDS, APOC behavior, and official driver expectations.
2. **OLAP / columnar execution**: Arrow memory layout, DataFusion planning,
   Parquet format choices, vectorized execution, and distributed execution.
3. **Graph database competitors**: query surface, storage model, indexes,
   traversal runtime, analytics procedures, and operational tradeoffs.
4. **Rust infrastructure patterns**: Apache Iggy and Rust graph/database repos
   for durable logs, memory ownership, async boundaries, and storage APIs.

## Current Local Inventory

| local path | upstream | branch / checkout | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/neo4j-src` | `https://github.com/neo4j/neo4j.git` | `release/5.26.0` | `c68156edf24` | Canonical Neo4j community implementation: record store, Bolt, Cypher, transactions, locks, WAL/checkpointing, and kernel APIs. |
| `gitrefrepo/neo4j-gds-src` | `https://github.com/neo4j/graph-data-science.git` | `2.13` | `dc4417b3c1` | Neo4j graph analytics architecture: projected graph stores, CSR-like layouts, algorithm APIs, and procedure integration. |
| `gitrefrepo/neo4j-gds-client-src` | `https://github.com/neo4j/graph-data-science-client.git` | `main` | `e96f9066` | GDS client ergonomics and user-facing workflow expectations. |
| `gitrefrepo/neo4j-apoc-src` | `https://github.com/neo4j/apoc.git` | `dev` | `11dbf56` | APOC procedure surface and extension patterns expected by Neo4j users. |
| `gitrefrepo/neo4j-testkit-src` | `https://github.com/neo4j-drivers/testkit.git` | `6.x` | `ec46b65` | Compatibility oracle for official Neo4j drivers. |
| `gitrefrepo/neo4j-docs-bolt-src` | `https://github.com/neo4j/docs-bolt.git` | `main` | `1714723` | Bolt protocol documentation source. |
| `gitrefrepo/neo4j-python-driver-src` | `https://github.com/neo4j/neo4j-python-driver.git` | `6.x` | `9e23c904` | Python driver behavior, Bolt handshake expectations, routing/session semantics, errors, and transaction APIs. |
| `gitrefrepo/neo4j-java-driver-src` | `https://github.com/neo4j/neo4j-java-driver.git` | `6.x` | `7652d3c3f` | Java driver behavior for enterprise clients, sessions, streaming, retries, and type mapping. |
| `gitrefrepo/neo4j-javascript-driver-src` | `https://github.com/neo4j/neo4j-javascript-driver.git` | `6.x` | `d8841712` | JavaScript and TypeScript client behavior, async results, PackStream values, and browser/server ergonomics. |
| `gitrefrepo/neo4j-go-driver-src` | `https://github.com/neo4j/neo4j-go-driver.git` | `6.x` | `c872010` | Go driver concurrency and session expectations. |
| `gitrefrepo/neo4j-dotnet-driver-src` | `https://github.com/neo4j/neo4j-dotnet-driver.git` | `6.x` | `261a8250` | .NET driver behavior and type-system mapping. |
| `gitrefrepo/opencypher-src` | `https://github.com/opencypher/openCypher.git` | `main` | `677cbaf` | Cypher grammar, TCK/spec history, and language compatibility reference. |
| `gitrefrepo/antlr-grammars-v4-src` | `https://github.com/antlr/grammars-v4.git` | `master` | `284602b3` | Parser/grammar references, including Cypher-related grammar patterns. |
| `gitrefrepo/libcypher-parser-src` | `https://github.com/cleishm/libcypher-parser.git` | `main` | `8ef7b22` | Compact C Cypher parser architecture reference. |
| `gitrefrepo/apache-iggy-src` | `https://github.com/apache/iggy` | `master` | `115ac2146` | Rust persistent streaming/log system for WAL, append-only topics, segment layout, retention, and async I/O boundaries. |
| `gitrefrepo/apache-datafusion-src` | `https://github.com/apache/datafusion.git` | `main` | `f220077` | Rust query engine: logical/physical plans, optimizer rules, Arrow execution, and table providers. |
| `gitrefrepo/apache-arrow-rs-src` | `https://github.com/apache/arrow-rs.git` | `main` | `2eeb805` | Rust Arrow arrays, buffers, IPC, Parquet integration, and memory-safe columnar primitives. |
| `gitrefrepo/apache-arrow-src` | `https://github.com/apache/arrow.git` | `main` | `4e04d46` | Cross-language Arrow specifications and implementations. |
| `gitrefrepo/datafusion-python-src` | `https://github.com/apache/datafusion-python.git` | `main` | `56b1cea` | Python binding patterns for future notebook-style graph analytics. |
| `gitrefrepo/datafusion-comet-src` | `https://github.com/apache/datafusion-comet.git` | `main` | `81df72d` | DataFusion acceleration for Spark workloads and interoperability patterns. |
| `gitrefrepo/apache-parquet-format-src` | `https://github.com/apache/parquet-format.git` | `master` | `662cdac` | Parquet format specification for low-RAM column storage and interchange decisions. |
| `gitrefrepo/apache-arrow-ballista-src` | `https://github.com/apache/arrow-ballista.git` | `main` | `7a96c94` | Distributed scheduler/executor around DataFusion for later distributed OLAP study. |
| `gitrefrepo/duckdb-src` | `https://github.com/duckdb/duckdb.git` | `main` | `811109f` | Compact OLAP engine, vectorized execution, buffer manager, optimizer, and single-process analytical design. |
| `gitrefrepo/clickhouse-src` | `https://github.com/ClickHouse/ClickHouse.git` | `master` | `df057801` | Columnar storage, MergeTree, compression, query execution, and production OLAP tradeoffs. |
| `gitrefrepo/age-src` | `https://github.com/apache/age.git` | `master` | `9960e9c` | Cypher on PostgreSQL extension model and graph query planning over relational storage. |
| `gitrefrepo/memgraph-src` | `https://github.com/memgraph/memgraph.git` | `master` | `8ea82dc` | In-memory Cypher-compatible graph database: storage, procedures, execution engine, and replication choices. |
| `gitrefrepo/arangodb-src` | `https://github.com/arangodb/arangodb.git` | `devel` | `3e50ab7fc` | Multi-model graph/document database with AQL traversal and graph indexing choices. |
| `gitrefrepo/janusgraph-src` | `https://github.com/JanusGraph/janusgraph.git` | `master` | `3ed2758` | Distributed graph database over pluggable backends and Gremlin integration. |
| `gitrefrepo/tigergraph-ecosys-src` | `https://github.com/tigergraph/ecosys.git` | `master` | `248e6fa` | TigerGraph ecosystem examples, connectors, and graph workload patterns. |
| `gitrefrepo/apache-tinkerpop-src` | `https://github.com/apache/tinkerpop.git` | `master` | `cf0118a` | Gremlin traversal language and graph provider APIs. |
| `gitrefrepo/apache-hugegraph-src` | `https://github.com/apache/incubator-hugegraph.git` | `master` | `b9a3dd9` | Apache graph database with Gremlin support and pluggable storage. |
| `gitrefrepo/falkordb-src` | `https://github.com/FalkorDB/FalkorDB.git` | `master` | `1a57217` | RedisGraph successor and sparse-matrix/GraphBLAS-inspired execution. |
| `gitrefrepo/kuzu-src` | `https://github.com/kuzudb/kuzu.git` | `master` | `89f0263` | Embedded graph database with columnar graph storage and Cypher-like execution. |
| `gitrefrepo/nebula-src` | `https://github.com/vesoft-inc/nebula.git` | `master` | `cdef57e` | Distributed graph database architecture, graph partitioning, query service, and storage service. |
| `gitrefrepo/graphscope-src` | `https://github.com/alibaba/GraphScope.git` | `main` | `2ac5ac8` | Graph analytics stack with interactive/query/learning components. |
| `gitrefrepo/dgraph-src` | `https://github.com/dgraph-io/dgraph.git` | `main` | `153d9ee` | Distributed graph database with indexing, posting lists, and log/storage design. |
| `gitrefrepo/terminusdb-src` | `https://github.com/terminusdb/terminusdb.git` | `main` | `f1b101b` | Document/graph database with immutable/versioned data model. |
| `gitrefrepo/arcadedb-src` | `https://github.com/ArcadeData/arcadedb.git` | `main` | `4e117d0` | Multi-model graph/document database, traversal APIs, SQL extensions, and embeddable server design. |
| `gitrefrepo/typedb-src` | `https://github.com/typedb/typedb.git` | `master` | `15fcd28` | Strongly typed knowledge graph database for schema, inference, and query planning tradeoffs. |
| `gitrefrepo/indradb-src` | `https://github.com/indradb/indradb.git` | `master` | `11c1c04` | Rust graph database for idiomatic storage and API boundaries. |
| `gitrefrepo/surrealdb-src` | `https://github.com/surrealdb/surrealdb.git` | `main` | `a97d3af` | Rust multi-model database with graph edges and embedded/server modes. |
| `gitrefrepo/cayley-src` | `https://github.com/cayleygraph/cayley.git` | `master` | `81dcd7d` | Quad-store graph database and query abstraction. |
| `gitrefrepo/redisgraph-src` | `https://github.com/RedisGraph/RedisGraph.git` | `master` | `5784cb8` | Redis module graph database, sparse matrix execution, and Cypher subset. |
| `gitrefrepo/orientdb-src` | `https://github.com/orientechnologies/orientdb.git` | `develop` | `670cfb4` | Mature multi-model graph/document database and SQL-like graph extensions. |
| `gitrefrepo/blazegraph-src` | `https://github.com/blazegraph/database.git` | `master` | `829ce82` | RDF/SPARQL graph database, triple-store indexes, and query optimization. |
| `gitrefrepo/apache-jena-src` | `https://github.com/apache/jena.git` | `main` | `852f2d7e` | RDF/SPARQL toolkit and database stack. |
| `gitrefrepo/eclipse-rdf4j-src` | `https://github.com/eclipse-rdf4j/rdf4j.git` | `main` | `3407d5e` | RDF framework and stores, repository APIs, and SPARQL evaluation. |
| `gitrefrepo/rocksdb-src` | `https://github.com/facebook/rocksdb.git` | `main` | `364eb88` | Production LSM storage engine: compaction, WAL, block cache, bloom filters, and write amplification. |
| `gitrefrepo/tikv-src` | `https://github.com/tikv/tikv.git` | `master` | `6cdd896` | Rust transactional key-value store, MVCC, and production-grade persistence patterns. |
| `gitrefrepo/redb-src` | `https://github.com/cberner/redb.git` | `master` | `76e0e07` | Embedded Rust copy-on-write B-tree database. |
| `gitrefrepo/fjall-src` | `https://github.com/fjall-rs/fjall.git` | `main` | `fb57152` | Rust LSM key-value store and compaction patterns. |
| `gitrefrepo/sled-src` | `https://github.com/spacejam/sled.git` | `main` | `e449d17` | Rust embedded database and historical lessons in crash safety. |
| `gitrefrepo/tantivy-src` | `https://github.com/quickwit-oss/tantivy.git` | `main` | `46b3fb9` | Rust search/index engine for full-text and property index architecture. |
| `gitrefrepo/snap-src` | `https://github.com/snap-stanford/snap.git` | `master` | `6924a03` | Stanford graph analytics library and algorithm baselines. |
| `gitrefrepo/gapbs-src` | `https://github.com/sbeamer/gapbs.git` | `master` | `b5e3e19` | Graph Algorithm Platform Benchmark Suite for BFS/PageRank/CC/SSSP benchmark structure. |
| `gitrefrepo/lagraph-src` | `https://github.com/GraphBLAS/LAGraph.git` | `stable` | `e2539e2` | Graph algorithms over GraphBLAS. |
| `gitrefrepo/graphblas-src` | `https://github.com/DrTimothyAldenDavis/GraphBLAS.git` | `stable` | `1fd5475` | SuiteSparse GraphBLAS implementation and sparse matrix kernels. |
| `gitrefrepo/gunrock-src` | `https://github.com/gunrock/gunrock.git` | `main` | `748f79e` | GPU graph analytics and frontier-based algorithm design. |
| `gitrefrepo/cugraph-src` | `https://github.com/rapidsai/cugraph.git` | `main` | `e792dc6` | GPU/distributed graph analytics stack. |
| `gitrefrepo/risingwave-src` | `https://github.com/risingwavelabs/risingwave.git` | `main` | `be0b87c` | Rust streaming database for stateful streaming, scheduling, and sync architecture. |
| `gitrefrepo/petgraph-src` | `https://github.com/petgraph/petgraph.git` | `master` | `ed71465` | Idiomatic Rust graph API and internal abstraction ideas. |

## Practical Reading Order

For v003, prioritize depth over breadth:

1. `neo4j-src`: record store, Bolt, Cypher, transaction log, procedure API.
2. `neo4j-testkit-src`, official drivers, and `neo4j-docs-bolt-src`: driver
   compatibility and protocol behavior.
3. `neo4j-gds-src`: CSR/projected graph representation and algorithm API.
4. `neo4j-apoc-src`: procedure compatibility expectations.
5. `apache-iggy-src`: append-only log/segment architecture for the OLTP to
   OLAP sync bridge.
6. `apache-datafusion-src` + `apache-arrow-rs-src`: query planning and
   columnar memory patterns for OLAP property columns.
7. `kuzu-src`, `falkordb-src`, `memgraph-src`: compact graph-specific execution
   engines closest to the Knight Bus shape.
8. `rocksdb-src`, `tikv-src`, `redb-src`, `fjall-src`: storage, WAL,
   compaction, and crash-safety patterns.
9. `gapbs-src`, `lagraph-src`, `graphblas-src`, `snap-src`: algorithm
   baselines after the CSR snapshot layer exists.
10. `apache-tinkerpop-src`, `apache-jena-src`, `eclipse-rdf4j-src`: standards
    and long-term compatibility semantics.

## Notes

- `gitrefrepo/` is a working research cache, not a dependency directory.
- Do not commit cloned repositories or copy their code without a deliberate
  licensing and design review.
- Some clones are shallow and track current branch heads rather than the exact
  historical SHAs listed in older research docs.
- Use the repo checkouts for architecture study, source navigation, and
  benchmark baselines, not for direct vendoring.

# Git Reference Repository Readme

This document accounts for the external source repositories cloned for Knight Bus
v003 architecture study. The actual clones live under `gitrefrepo/` at the repo
root and are intentionally ignored by git via `.gitignore`.

The goal of this corpus is not to vendor code. It is a local research shelf for
studying proven implementations of graph storage, graph query languages,
analytics engines, columnar execution, low-memory formats, and Rust log/stream
systems that may inform the Knight Bus v003 design.

## Local layout

```text
knight-bus-graph-walker/
  Docs-Readme.md
  gitrefrepo/                 # ignored local clone shelf
    neo4j-src/
    apache-iggy-src/
    apache-datafusion-src/
    ...
```

## Study priorities

1. **Neo4j compatibility baseline**: Bolt, Cypher, record storage, procedures,
   GDS, and APOC behavior.
2. **OLAP / columnar execution**: Arrow memory layout, DataFusion planning,
   Parquet format choices, vectorized execution, and distributed execution.
3. **Graph database competitors**: query surface, storage model, indexes,
   traversal runtime, analytics procedures, and operational tradeoffs.
4. **Rust infrastructure patterns**: Apache Iggy and Rust graph/database repos
   for durable logs, memory ownership, async boundaries, and storage APIs.

## Original Neo4j replacement references

These were requested from the pre-v003 research document
`docs_pre_v003/Neo4j-replacement-02/docs/Docs-Readme.md`.

| local path | upstream | branch / checkout | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/neo4j-src` | `https://github.com/neo4j/neo4j.git` | `5.26.1` detached | `c68156e` | Canonical Neo4j community implementation: storage records, Bolt, Cypher runtime, transactions, locks, WAL/checkpointing, kernel APIs. |
| `gitrefrepo/neo4j-gds-src` | `https://github.com/neo4j/graph-data-science.git` | `2.13` | `dc4417b` | Neo4j graph analytics architecture: projected graph stores, CSR-like layouts, algorithm APIs, procedure integration. |
| `gitrefrepo/neo4j-apoc-src` | `https://github.com/neo4j/apoc.git` | `dev` | `11dbf56` | APOC procedure surface and extension patterns expected by Neo4j users. |
| `gitrefrepo/duckdb-src` | `https://github.com/duckdb/duckdb.git` | `main` | `a93109a` | Compact OLAP engine: vectorized execution, buffer manager, optimizer, single-process analytical database design. |
| `gitrefrepo/clickhouse-src` | `https://github.com/ClickHouse/ClickHouse.git` | `master` | `835ad2e8` | High-performance columnar storage, MergeTree design, compression, query execution, production OLAP tradeoffs. |
| `gitrefrepo/age-src` | `https://github.com/apache/age.git` | `master` | `9960e9c` | Cypher on PostgreSQL extension model; graph query parsing/planning over a relational substrate. |
| `gitrefrepo/memgraph-src` | `https://github.com/memgraph/memgraph.git` | `master` | `8ea82dc` | In-memory Cypher-compatible graph database; execution engine, storage, procedures, replication choices. |
| `gitrefrepo/arangodb-src` | `https://github.com/arangodb/arangodb.git` | `devel` | `fab5089f6` | Multi-model graph/document database; AQL traversal and graph storage/indexing choices. |
| `gitrefrepo/janusgraph-src` | `https://github.com/JanusGraph/janusgraph.git` | `master` | `3ed2758` | Distributed graph database over pluggable storage backends; Gremlin traversal integration. |
| `gitrefrepo/tigergraph-ecosys-src` | `https://github.com/tigergraph/ecosys.git` | `master` | `248e6fa` | TigerGraph ecosystem examples, connectors, and graph workload patterns. |

## Apache Iggy and DataFusion ecosystem

These repos are useful for the non-graph parts of the v003 architecture: WAL/log
design, columnar data interchange, vectorized execution, and distributed query
planning.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/apache-iggy-src` | `https://github.com/apache/iggy.git` | `master` | `c5fde76` | Rust persistent streaming/log system; useful for WAL, append-only topics, segment layout, retention, and async I/O boundaries. |
| `gitrefrepo/apache-datafusion-src` | `https://github.com/apache/datafusion.git` | `main` | `04c01bb` | Rust query engine: logical plans, physical plans, optimizer rules, Arrow-native execution, extensible table providers. |
| `gitrefrepo/apache-arrow-rs-src` | `https://github.com/apache/arrow-rs.git` | `main` | `bbbe8a6` | Rust Arrow arrays, buffers, IPC, Parquet integration, and memory-safe columnar primitives. |
| `gitrefrepo/apache-arrow-src` | `https://github.com/apache/arrow.git` | `main` | `b5ece7e` | Cross-language Arrow specifications and implementations; canonical memory layout reference. |
| `gitrefrepo/datafusion-python-src` | `https://github.com/apache/datafusion-python.git` | `main` | `081325a` | Python binding patterns for DataFusion; useful for future developer ergonomics and notebook-style graph analytics. |
| `gitrefrepo/datafusion-comet-src` | `https://github.com/apache/datafusion-comet.git` | `main` | `a384427` | DataFusion acceleration for Spark workloads; useful for adapter and interoperability patterns. |
| `gitrefrepo/apache-parquet-format-src` | `https://github.com/apache/parquet-format.git` | `master` | `662cdac` | Parquet format specification; useful for low-RAM column storage and interchange decisions. |
| `gitrefrepo/apache-arrow-ballista-src` | `https://github.com/apache/arrow-ballista.git` | `main` | `65ec1c4` | Distributed scheduler/executor around DataFusion; useful if Knight Bus later studies distributed OLAP. |

## Graph competitors and adjacent graph systems

This set is intentionally broader than direct Neo4j replacements. It covers
Cypher, Gremlin, SPARQL/RDF, Datalog-like, document-graph hybrids, Rust graph
stores, and multi-model databases.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/apache-tinkerpop-src` | `https://github.com/apache/tinkerpop.git` | `master` | `cf0118a` | Gremlin traversal language and graph abstraction layer; useful for traversal semantics and provider APIs. |
| `gitrefrepo/apache-hugegraph-src` | `https://github.com/apache/incubator-hugegraph.git` | `master` | `f56462a` | Apache graph database with Gremlin support and pluggable storage; useful for server/storage separation. |
| `gitrefrepo/falkordb-src` | `https://github.com/FalkorDB/FalkorDB.git` | `master` | `1a57217` | RedisGraph successor; Cypher execution over sparse matrix/GraphBLAS-inspired representation. |
| `gitrefrepo/kuzu-src` | `https://github.com/kuzudb/kuzu.git` | `master` | `89f0263` | Embedded graph database; columnar graph storage, Cypher-like query execution, vectorized runtime. |
| `gitrefrepo/nebula-src` | `https://github.com/vesoft-inc/nebula.git` | `master` | `cdef57e` | Distributed graph database; query service, storage service, graph partitioning, and operational architecture. |
| `gitrefrepo/graphscope-src` | `https://github.com/alibaba/GraphScope.git` | `main` | `2ac5ac8` | Graph analytics stack with interactive/query/learning components; useful for OLAP algorithms and distributed graph abstractions. |
| `gitrefrepo/dgraph-src` | `https://github.com/dgraph-io/dgraph.git` | `main` | `153d9ee` | Distributed graph database with GraphQL+-style query layer; useful for indexing, posting lists, and storage/log design. |
| `gitrefrepo/terminusdb-src` | `https://github.com/terminusdb/terminusdb.git` | `main` | `f1b101b` | Document/graph database with immutable/versioned data model; useful for append-only semantics and schema reasoning. |
| `gitrefrepo/arcadedb-src` | `https://github.com/ArcadeData/arcadedb.git` | `main` | `61177a8` | Multi-model graph/document database; useful for traversal APIs, SQL extensions, and embeddable server design. |
| `gitrefrepo/typedb-src` | `https://github.com/typedb/typedb.git` | `master` | `1a745de` | Strongly typed knowledge graph database; useful for schema, inference, and query planning tradeoffs. |
| `gitrefrepo/indradb-src` | `https://github.com/indradb/indradb.git` | `master` | `11c1c04` | Rust graph database; useful for simple idiomatic Rust graph storage and API boundaries. |
| `gitrefrepo/surrealdb-src` | `https://github.com/surrealdb/surrealdb.git` | `main` | `a97d3af` | Rust multi-model database with graph edges; useful for storage abstraction, query surface, and embedded/server modes. |
| `gitrefrepo/cayley-src` | `https://github.com/cayleygraph/cayley.git` | `master` | `81dcd7d` | Graph database inspired by Freebase/Knowledge Graph use cases; useful for quad stores and query abstraction. |
| `gitrefrepo/redisgraph-src` | `https://github.com/RedisGraph/RedisGraph.git` | `master` | `5784cb8` | Redis module graph database; useful for sparse matrix execution, Cypher subset, and module embedding model. |
| `gitrefrepo/orientdb-src` | `https://github.com/orientechnologies/orientdb.git` | `develop` | `670cfb4` | Mature multi-model graph/document database; useful for storage/indexing and SQL-like graph extensions. |
| `gitrefrepo/blazegraph-src` | `https://github.com/blazegraph/database.git` | `master` | `829ce82` | RDF/SPARQL graph database; useful for triple-store indexes, query optimization, and semantic graph workloads. |
| `gitrefrepo/apache-jena-src` | `https://github.com/apache/jena.git` | `main` | `b8babed5` | RDF/SPARQL toolkit and database stack; useful for query algebra, triple indexes, and standards compatibility. |
| `gitrefrepo/eclipse-rdf4j-src` | `https://github.com/eclipse-rdf4j/rdf4j.git` | `main` | `3407d5e` | RDF framework and stores; useful for repository APIs, SPARQL evaluation, and semantic graph data management. |

## Practical reading order

For v003, prioritize depth over breadth:

1. `neo4j-src`: record store, Bolt, Cypher, transaction log, procedure API.
2. `neo4j-gds-src`: CSR/projected graph representation and algorithm API.
3. `apache-iggy-src`: append-only log/segment architecture for the OLTP to OLAP
   sync bridge.
4. `apache-datafusion-src` + `apache-arrow-rs-src`: query planning and
   columnar memory patterns for OLAP property columns.
5. `kuzu-src`, `falkordb-src`, `memgraph-src`: compact graph-specific execution
   engines closest to the Knight Bus shape.
6. `apache-tinkerpop-src`, `apache-jena-src`, `eclipse-rdf4j-src`: standards and
   query semantics that may affect long-term compatibility decisions.

## Newly added compatibility, storage, and algorithm references

This pass fills the largest remaining gaps in the research shelf. The v003 PRD
has one especially unforgiving promise: existing Neo4j applications should work
with zero client-side changes. That makes protocol and driver compatibility the
highest-leverage addition, followed by Cypher conformance, OLTP storage/WAL
design, and OLAP graph-algorithm baselines.

### Tier 1: Neo4j driver and Bolt compatibility

Use these before designing the public server surface.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/neo4j-testkit-src` | `https://github.com/neo4j-drivers/testkit.git` | `6.x` | `ec46b65` | Compatibility oracle for official Neo4j drivers. This is the most direct way to test whether Knight Bus behaves like Neo4j from client code. |
| `gitrefrepo/neo4j-python-driver-src` | `https://github.com/neo4j/neo4j-python-driver.git` | `6.x` | `9e23c90` | Python driver behavior, Bolt handshake expectations, routing/session semantics, error handling, and transaction API behavior. |
| `gitrefrepo/neo4j-java-driver-src` | `https://github.com/neo4j/neo4j-java-driver.git` | `6.x` | `7652d3c` | Java driver is a primary enterprise client; useful for compatibility expectations around sessions, result streaming, retries, and type mapping. |
| `gitrefrepo/neo4j-javascript-driver-src` | `https://github.com/neo4j/neo4j-javascript-driver.git` | `6.x` | `a98f9c5` | JavaScript/TypeScript client behavior, async result consumption, PackStream value mapping, and browser/server ergonomics. |
| `gitrefrepo/neo4j-go-driver-src` | `https://github.com/neo4j/neo4j-go-driver.git` | `6.x` | `c872010` | Go driver behavior and concurrency expectations. |
| `gitrefrepo/neo4j-dotnet-driver-src` | `https://github.com/neo4j/neo4j-dotnet-driver.git` | `6.x` | `261a825` | .NET driver behavior and type-system mapping. |
| `gitrefrepo/neo4j-docs-bolt-src` | `https://github.com/neo4j/docs-bolt.git` | `main` | `1714723` | Bolt protocol documentation source; use with `neo4j-src` to keep implementation and docs aligned. |

### Tier 2: Cypher grammar, parser, and conformance

Use these before committing to a Cypher parser strategy.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/opencypher-src` | `https://github.com/opencypher/openCypher.git` | `main` | `677cbaf` | Cypher grammar, TCK/spec history, and language-level compatibility reference. |
| `gitrefrepo/antlr-grammars-v4-src` | `https://github.com/antlr/grammars-v4.git` | `master` | `284602b3` | Large grammar corpus including Cypher-related grammars and parser design references. |
| `gitrefrepo/libcypher-parser-src` | `https://github.com/cleishm/libcypher-parser.git` | `main` | `8ef7b22` | C implementation of a Cypher parser; useful as a compact parser architecture reference. |

### Tier 3: OLTP storage, WAL, and indexing

Use these when designing the Rust record store, WAL, page cache, and indexes.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/rocksdb-src` | `https://github.com/facebook/rocksdb.git` | `main` | `ae30c71` | Production LSM storage engine; useful for compaction, WAL, block cache, bloom filters, and write amplification tradeoffs. |
| `gitrefrepo/tikv-src` | `https://github.com/tikv/tikv.git` | `master` | `6cdd896` | Rust transactional key-value store; useful for MVCC, Raft-separated storage ideas, and production-grade Rust persistence patterns. |
| `gitrefrepo/redb-src` | `https://github.com/cberner/redb.git` | `master` | `76e0e07` | Embedded Rust database with copy-on-write B-tree design; useful for single-node durable storage alternatives. |
| `gitrefrepo/fjall-src` | `https://github.com/fjall-rs/fjall.git` | `main` | `9971cc8` | Rust LSM key-value store; useful for simpler embedded persistence and compaction implementation patterns. |
| `gitrefrepo/sled-src` | `https://github.com/spacejam/sled.git` | `main` | `e449d17` | Rust embedded database; useful for historical lessons in lock-free/tree storage and crash safety. |
| `gitrefrepo/tantivy-src` | `https://github.com/quickwit-oss/tantivy.git` | `main` | `46b3fb9` | Rust search/index engine; useful for full-text and property index architecture. |

### Tier 4: OLAP graph algorithms and sparse linear algebra

Use these when designing the low-RAM algorithm layer beyond the first CSR
vertical slice.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/snap-src` | `https://github.com/snap-stanford/snap.git` | `master` | `6924a03` | Stanford graph analytics library; useful for algorithm baselines and graph workload patterns. |
| `gitrefrepo/gapbs-src` | `https://github.com/sbeamer/gapbs.git` | `master` | `b5e3e19` | Graph Algorithm Platform Benchmark Suite; useful for BFS/PageRank/CC/SSSP benchmark structure. |
| `gitrefrepo/lagraph-src` | `https://github.com/GraphBLAS/LAGraph.git` | `stable` | `e2539e2` | Graph algorithms over GraphBLAS; useful for sparse-linear-algebra expression of graph analytics. |
| `gitrefrepo/graphblas-src` | `https://github.com/DrTimothyAldenDavis/GraphBLAS.git` | `stable` | `1fd5475` | SuiteSparse GraphBLAS implementation; useful for sparse matrix kernels and memory layout ideas. |
| `gitrefrepo/gunrock-src` | `https://github.com/gunrock/gunrock.git` | `main` | `748f79e` | GPU graph analytics reference; not a v003 dependency, but useful for frontier-based algorithm design. |
| `gitrefrepo/cugraph-src` | `https://github.com/rapidsai/cugraph.git` | `main` | `e792dc6` | GPU/distributed graph analytics stack; useful for algorithm API and large-scale analytics patterns. |

### Tier 5: Rust streaming/query architecture and ergonomics

Use these only after the compatibility and storage questions are covered.

| local path | upstream | branch | commit | why study it |
| --- | --- | --- | --- | --- |
| `gitrefrepo/risingwave-src` | `https://github.com/risingwavelabs/risingwave.git` | `main` | `1b2354c` | Rust streaming database; useful for stateful streaming, query scheduling, and OLTP-to-OLAP sync architecture thinking. |
| `gitrefrepo/petgraph-src` | `https://github.com/petgraph/petgraph.git` | `master` | `ed71465` | Idiomatic Rust graph API; not storage-scale, but useful for API ergonomics and internal graph abstractions. |

### Recommendation

Read Tier 1 and Tier 2 first. Those repos directly de-risk the hardest v003
claim: Neo4j applications and drivers should connect without client changes.
Tier 3 comes next for the single-node OLTP record store. Tier 4 is valuable
after the CSR snapshot layer exists and needs algorithmic coverage beyond the
first PageRank/BFS-style procedures.

## Notes

- `gitrefrepo/` is a working research cache, not a dependency directory.
- Do not commit cloned repositories or copy their code without a deliberate
  licensing and design review.
- Some clones are shallow and track current branch heads rather than the exact
  historical SHAs listed in older research docs.
- Pre-commit hooks were installed in cloned repos that provided
  `.pre-commit-config.yaml` during setup.

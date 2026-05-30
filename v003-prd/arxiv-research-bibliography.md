# ArXiv Research Bibliography for v003

This bibliography is scoped to `v003-prd/v003-prd.md`: a Rust rewrite of
Neo4j with the same public surface, Neo4j-like OLTP behavior, low-RAM OLAP
storage, and single-node community-edition constraints.

## Phase 0: Deconstruct and clarify

Premise is sound. Proceeding with optimized protocol.

The problem is not “find every graph paper on arXiv.” That would create noise.
The useful problem is to identify the arXiv papers that should shape v003
architecture decisions:

- **Neo4j compatibility**: Cypher, GQL/SQL-PGQ, path semantics, and driver-facing
  behavior.
- **OLTP graph storage**: adjacency scans, transactions, dynamic graph updates,
  WAL/log structure, MVCC, and cache locality.
- **Low-RAM OLAP**: CSR, semi-external memory, mmap-friendly formats,
  compression, graph algorithms, and sparse linear algebra.
- **Query execution**: vectorized execution, push/pull pipelines, Arrow-native
  columnar layout, and path-query optimization.
- **Single-node operational realism**: 50 GB processed on 8 GB systems means the
  design must optimize bytes moved, not just algorithmic big-O.

Assumptions:

- Papers are chosen for design relevance, not only citation count.
- ArXiv is the required source. Some canonical systems papers may have stronger
  official conference versions outside arXiv; this document still points to the
  arXiv record when available.
- New 2025-2026 arXiv papers are useful as design signals but should be treated
  as less battle-tested than older systems papers.

## Phase 1: Cognitive staging

Expert council used for this bibliography:

1. **Graph database architect**: prioritizes Neo4j-compatible semantics,
   transactions, and query/runtime architecture.
2. **Storage-systems engineer**: prioritizes WAL, page/cache behavior, LSM/B-tree
   tradeoffs, and crash recovery.
3. **Graph analytics / sparse linear algebra researcher**: prioritizes CSR,
   GraphBLAS, PageRank/BFS/SSSP, and semi-external algorithms.
4. **Columnar query-engine engineer**: prioritizes Arrow/DataFusion/DuckDB-style
   execution, vectorization, and optimizer boundaries.
5. **Skeptical engineer**: rejects anything that does not reduce risk for the
   v003 PRD.

Knowledge scaffolding:

- Labeled property graph model, Cypher, GQL, SQL/PGQ, path queries.
- Neo4j-style OLTP: node/relationship records, property stores, locks,
  transaction logs, page cache, and indexes.
- Immutable analytical snapshots: dense IDs, sorted CSR, reverse CSR, property
  columns, mmap, external sort, and atomic snapshot swap.
- HTAP bridge: append-only log, delta layer, snapshot builder, replay, and query
  router.
- Low-memory analytics: semi-external memory, compressed graph representation,
  GraphBLAS/SpMV, frontier scheduling, and selective I/O.

## Phase 2: Strategy synthesis

### Conventional approach

Read Neo4j implementation papers/docs, then implement the storage and query
engine directly.

Problem: this underweights the PRD's low-RAM OLAP promise and risks reproducing
Neo4j's general-purpose memory profile.

### Divergent approach A: Graph database as a railway switchyard

Blend graph databases with railway signaling. OLTP is the live track; OLAP is a
parallel express track; the WAL is the signaling system that safely switches
state between them.

Implication: prioritize papers on transactional graph storage, dynamic graph
snapshots, and log-coupled representations.

### Divergent approach B: Graph engine as a biological circulatory system

Blend graph storage with physiology. Hot mutable state is blood flow; cold
analytical state is tissue structure; WAL/checkpoints are circulation and
repair.

Implication: do not make every structure mutable. Keep a compact stable body
and use small active deltas.

### Divergent approach C: Graph runtime as a compressed musical score

Blend CSR with music notation. The graph is a compact score; algorithms are
performers that stream through notes without loading the full orchestra into
memory.

Implication: prioritize graph compression, CSR loading, semi-external memory,
and GraphBLAS/SpMV papers.

Selected hybrid: **log-coupled dual engine**. Use OLTP papers to preserve
Neo4j-like behavior and OLAP papers to preserve the Knight Bus low-RAM advantage.
Use query-language papers to define compatibility boundaries and query-engine
papers to keep execution extensible.

## Tier 0: Must-read papers before committing architecture

These are the highest-leverage papers for v003. Read them before locking the
storage/query design.

| arXiv | paper | why it matters for v003 |
| --- | --- | --- |
| [1910.05773](https://arxiv.org/abs/1910.05773) | **LiveGraph: A Transactional Graph Storage System with Purely Sequential Adjacency List Scans** | Directly targets the OLTP/OLAP tension: transactional updates plus sequential adjacency scans. The Transactional Edge Log is highly relevant to a WAL/delta-to-CSR bridge. |
| [2502.10959](https://arxiv.org/abs/2502.10959) | **Revisiting the Design of In-Memory Dynamic Graph Storage** | Systematic comparison of dynamic graph storage. Important warning: many mutable graph stores use several times more memory than CSR. |
| [2411.06392](https://arxiv.org/abs/2411.06392) | **LSMGraph: A High-Performance Dynamic Graph Storage System with Multi-Level CSR** | The closest arXiv match to “write-friendly log/LSM plus read-friendly CSR.” Strong input for the OLTP-to-OLAP bridge. |
| [1802.09984](https://arxiv.org/abs/1802.09984) | **Formal Semantics of the Language Cypher** | Necessary for “same API/surface area.” Defines the read-only core semantics Knight Bus must not accidentally reinterpret. |
| [1705.02844](https://arxiv.org/abs/1705.02844) | **Formalising openCypher Graph Queries in Relational Algebra** | Useful for compiling Cypher-like patterns into relational/graph algebra and for deciding how DataFusion-like planning could fit. |
| [2112.06217](https://arxiv.org/abs/2112.06217) | **Graph Pattern Matching in GQL and SQL/PGQ** | Frames modern standard graph pattern matching; useful for not painting v003 into a Neo4j-only corner. |
| [2010.15879](https://arxiv.org/abs/2010.15879) | **Log(Graph): A Near-Optimal High-Performance Graph Representation** | Directly relevant to “50 GB on 8 GB”: compressed representation with low-overhead traversal. |
| [1602.02864](https://arxiv.org/abs/1602.02864) | **Semi-External Memory Sparse Matrix Multiplication for Billion-Node Graphs** | Strong reference for keeping graph structure on SSD/mmap while keeping dense vectors in memory; maps to PageRank and iterative analytics. |
| [1504.01039](https://arxiv.org/abs/1504.01039) | **Graphs, Matrices, and the GraphBLAS: Seven Good Reasons** | Design basis for expressing graph algorithms as sparse matrix/vector operations. |
| [1606.05790](https://arxiv.org/abs/1606.05790) | **Mathematical Foundations of the GraphBLAS** | Formalizes the algebraic basis for a compact algorithm layer over CSR/sparse matrices. |

## Query language and compatibility

Read these to avoid subtly incompatible Cypher behavior.

| arXiv | paper | design use |
| --- | --- | --- |
| [1802.09984](https://arxiv.org/abs/1802.09984) | **Formal Semantics of the Language Cypher** | Core read-only semantics: pattern matching, filtering, relational table behavior. |
| [1705.02844](https://arxiv.org/abs/1705.02844) | **Formalising openCypher Graph Queries in Relational Algebra** | Compilation model from openCypher to algebra. Good bridge to DataFusion-style logical plans. |
| [2112.06217](https://arxiv.org/abs/2112.06217) | **Graph Pattern Matching in GQL and SQL/PGQ** | Standard graph pattern matching; helps future-proof against GQL/SQL-PGQ. |
| [2409.01102](https://arxiv.org/abs/2409.01102) | **GQL and SQL/PGQ: Theoretical Models and Expressive Power** | Explains model differences and expressive limitations in new standards. |
| [2306.02194](https://arxiv.org/abs/2306.02194) | **PathFinder: A unified approach for handling paths in graph query languages** | Useful for path-returning semantics, which are central to Cypher/GQL behavior. |
| [2604.02553](https://arxiv.org/abs/2604.02553) | **Efficient Path Query Processing in Relational Database Systems** | Newer path-query optimization work; relevant if Knight Bus compiles path queries into a relational/vectorized backend. |
| [2504.04584](https://arxiv.org/abs/2504.04584) | **BARQ: A Vectorized SPARQL Query Execution Engine** | SPARQL, not Cypher, but useful for vectorized graph-query execution patterns. |

## OLTP graph storage, updates, and concurrency

These papers inform the mutable side of the dual-engine design.

| arXiv | paper | design use |
| --- | --- | --- |
| [1910.05773](https://arxiv.org/abs/1910.05773) | **LiveGraph** | Transactional Edge Log, sequential adjacency scans, and concurrency control tuned for graph workloads. |
| [2502.10959](https://arxiv.org/abs/2502.10959) | **Revisiting the Design of In-Memory Dynamic Graph Storage** | Comparative evidence on memory overhead, concurrency, and scan costs of dynamic graph structures. |
| [2411.06392](https://arxiv.org/abs/2411.06392) | **LSMGraph** | Multi-level CSR inside an LSM-like design; highly relevant if deltas become more than a simple overlay. |
| [1904.08380](https://arxiv.org/abs/1904.08380) | **Low-Latency Graph Streaming Using Compressed Purely-Functional Trees** | Aspen/C-trees: snapshots, parallelism, low-latency dynamic graph updates with compressed structures. |
| [2507.00839](https://arxiv.org/abs/2507.00839) | **RapidStore: An Efficient Dynamic Graph Storage System for Concurrent Queries** | Newer read/write decoupling approach for read-intensive dynamic graph storage. |
| [2601.01444](https://arxiv.org/abs/2601.01444) | **RadixGraph: A Fast, Space-Optimized Data Structure for Dynamic Graph Storage** | Newer space-optimized dynamic graph storage; useful as a warning/benchmark against overbuilding mutable CSR. |
| [2312.14396](https://arxiv.org/abs/2312.14396) | **GastCoCo: Graph Storage and Coroutine-Based Prefetch Co-Design for Dynamic Graph Processing** | Cache misses dominate dynamic graph processing; relevant to Rust prefetch/iterator design. |
| [1912.12740](https://arxiv.org/abs/1912.12740) | **Practice of Streaming Processing of Dynamic Graphs: Concepts, Models, and Systems** | Survey/taxonomy for dynamic/streaming graph models and concurrency choices. |
| [2305.11162](https://arxiv.org/abs/2305.11162) | **The Graph Database Interface** | Large-scale transactional/analytical graph workload interface. Not single-node, but useful for API/workload framing. |
| [2510.11166](https://arxiv.org/abs/2510.11166) | **Poseidon: A OneGraph Engine** | Recent production-adjacent graph engine description; useful for openCypher plus algorithms over dynamic graphs. |

## Low-RAM OLAP, CSR, and semi-external graph analytics

These papers inform the custom low-RAM analytical storage layer.

| arXiv | paper | design use |
| --- | --- | --- |
| [2010.15879](https://arxiv.org/abs/2010.15879) | **Log(Graph)** | Compressed graph representation with low decompression overhead. Strong reference for reducing bytes per edge without killing traversal speed. |
| [1812.10977](https://arxiv.org/abs/1812.10977) | **Compact and Efficient Representation of General Graph Databases** | Compact labeled/attributed graph databases, relevant to property graph compression beyond raw adjacency. |
| [1006.0809](https://arxiv.org/abs/1006.0809) | **Tight and simple Web graph compression** | Classic compression tradeoffs for adjacency lists and random access. |
| [1602.08820](https://arxiv.org/abs/1602.08820) | **Compressing Graphs and Indexes with Recursive Graph Bisection** | Graph reordering/compression for locality; relevant to dense ID assignment. |
| [1602.02864](https://arxiv.org/abs/1602.02864) | **Semi-External Memory Sparse Matrix Multiplication for Billion-Node Graphs** | SSD-backed sparse matrix operations; directly supports the 50 GB / 8 GB aspiration. |
| [1707.02557](https://arxiv.org/abs/1707.02557) | **GraphMP: An Efficient Semi-External-Memory Big Graph Processing System on a Single Machine** | Single-machine semi-external graph processing; useful for scheduling and edge-cache decisions. |
| [1810.04334](https://arxiv.org/abs/1810.04334) | **GraphMP: I/O-Efficient Big Graph Analytics on a Single Commodity Machine** | Later GraphMP framing emphasizing I/O-efficient analytics. |
| [1905.04264](https://arxiv.org/abs/1905.04264) | **PartitionedVC: Partitioned External Memory Graph Analytics Framework for SSDs** | CSR plus multi-log update mechanism for active-vertex workloads. |
| [1710.07736](https://arxiv.org/abs/1710.07736) | **BigSparse: High-performance external graph analytics** | Fully external analytics where both vertices and edges can exceed memory. |
| [1907.03335](https://arxiv.org/abs/1907.03335) | **Graphyti: A Semi-External Memory Graph Library for FlashGraph** | Practical principles for writing semi-external graph algorithms. |
| [2101.06911](https://arxiv.org/abs/2101.06911) | **DFOGraph: An I/O- and Communication-Efficient System for Distributed Fully-out-of-Core Graph Processing** | Mostly distributed, but useful for partitioning/compression ideas; lower priority because v003 is single-node. |
| [2311.14650](https://arxiv.org/abs/2311.14650) | **GVEL: Fast Graph Loading in Edgelist and Compressed Sparse Row (CSR) formats** | Useful for the builder path: fast edgelist-to-CSR loading and mmap-friendly CSR ingestion. |

## Graph algorithms and sparse linear algebra

These papers should shape algorithm APIs and execution kernels.

| arXiv | paper | design use |
| --- | --- | --- |
| [1504.01039](https://arxiv.org/abs/1504.01039) | **Graphs, Matrices, and the GraphBLAS: Seven Good Reasons** | Rationale for graph algorithms over sparse matrix primitives. |
| [1606.05790](https://arxiv.org/abs/1606.05790) | **Mathematical Foundations of the GraphBLAS** | Formal semiring/linear-algebra foundation for PageRank, BFS, centrality, etc. |
| [1602.02864](https://arxiv.org/abs/1602.02864) | **Semi-External Memory Sparse Matrix Multiplication for Billion-Node Graphs** | SpMM/SpMV patterns for PageRank/eigensolvers/NMF with sparse graph on SSD. |

## Storage engines, WAL, and LSM tradeoffs

These are not graph-specific, but they matter if Knight Bus uses an embedded
storage engine or borrows LSM/WAL design.

| arXiv | paper | design use |
| --- | --- | --- |
| [2407.15581](https://arxiv.org/abs/2407.15581) | **vLSM: Low tail latency and I/O amplification in LSM-based KV stores** | Tail-latency and amplification tradeoffs in RocksDB-like systems. |
| [2506.04678](https://arxiv.org/abs/2506.04678) | **BVLSM: Write-Efficient LSM-Tree Storage via WAL-Time Key-Value Separation** | Interesting WAL-time separation idea. Relevant if properties/large values are stored separately from graph topology. |
| [2005.14213](https://arxiv.org/abs/2005.14213) | **From WiscKey to Bourbon: A Learned Index for Log-Structured Merge Trees** | Learned indexing over LSM; lower priority for v003, but useful for later property/key index work. |

## Columnar/vectorized execution and Arrow/DataFusion relevance

These papers help if the OLAP side uses Arrow/DataFusion-style columnar
execution for properties or path-query fragments.

| arXiv | paper | design use |
| --- | --- | --- |
| [2105.09894](https://arxiv.org/abs/2105.09894) | **Towards an Arrow-native Storage System** | Arrow-native storage and compute-offload framing; relevant to property columns and zero-copy analytics. |
| [1610.09166](https://arxiv.org/abs/1610.09166) | **Push vs. Pull-Based Loop Fusion in Query Engines** | Execution-engine design: pull vs push pipelines and fusion. Useful before choosing iterator APIs. |
| [2601.12456](https://arxiv.org/abs/2601.12456) | **Bringing Data Transformations Near-Memory for Low-Latency Analytics in HTAP Environments** | Newer HTAP/near-memory transformation idea; useful but speculative. |
| [2602.17335](https://arxiv.org/abs/2602.17335) | **Do GPUs Really Need New Tabular File Formats?** | Mostly GPU/file-format focused, but useful for Parquet/Arrow default-format skepticism. |
| [2508.04701](https://arxiv.org/abs/2508.04701) | **Rethinking Analytical Processing in the GPU Era** | Useful later if Knight Bus explores GPU acceleration; not needed for the first single-node CPU path. |

## Recommended reading order

1. **Compatibility semantics first**
   - `1802.09984`
   - `1705.02844`
   - `2112.06217`
   - `2306.02194`

2. **Mutable graph storage second**
   - `1910.05773`
   - `2502.10959`
   - `2411.06392`
   - `1904.08380`

3. **Low-RAM analytical storage third**
   - `2010.15879`
   - `1602.02864`
   - `1707.02557`
   - `1812.10977`
   - `2311.14650`

4. **Algorithm substrate fourth**
   - `1504.01039`
   - `1606.05790`

5. **Storage/query-engine internals fifth**
   - `2407.15581`
   - `2506.04678`
   - `2105.09894`
   - `1610.09166`

## Decision filter

- **Strongest normal path**: implement a log-coupled dual engine:
  Neo4j-like OLTP store plus immutable/mmap CSR OLAP snapshots.
- **Safest bad-case path**: keep OLTP and OLAP independently shippable. If Bolt
  compatibility stalls, the CSR analytics engine remains useful. If CSR rebuilds
  are slower than expected, the OLTP engine still moves toward Neo4j-compatible
  CRUD.
- **Fastest uncertainty reducers**:
  1. Prototype a transactional edge log inspired by LiveGraph.
  2. Replay that log into sorted forward/reverse CSR snapshots.
  3. Compile a small Cypher read-only subset using the formal Cypher papers as
     the semantic oracle.
  4. Run PageRank/BFS over mmap CSR using GraphBLAS/SpMV papers as design
     references.

## Verification notes

I verified the listed arXiv IDs and titles using the arXiv API or arXiv search
results. The bibliography intentionally excludes broad graph neural network,
knowledge-graph embedding, and distributed-cluster-only papers unless they
directly inform the v003 PRD.

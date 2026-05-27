# v003 ArXiv Paper Corpus

This folder copies the arXiv papers relevant to the `v003-prd` scope and the follow-up storage/architecture discussion: Neo4j-compatible APIs, OLTP record/WAL storage, low-RAM CSR/OLAP snapshots, property/index sidecars, query routing, GraphBLAS-style algorithms, mmap vs explicit I/O, and vectorized/columnar execution.

## Workflow articulation

### Phase 0: Deconstruct and clarify

Premise is sound. Proceeding with optimized protocol. The request is to gather arXiv source material that informs the v003 architecture, not to implement code or replace the existing benchmark docs. “All” is interpreted as a broad, curated corpus of directly relevant arXiv papers; broad GNN/ML papers and distributed-only systems are excluded unless they affect storage/query architecture.

### Phase 1: Cognitive staging

Expert council: graph database architect, storage-systems engineer, graph analytics/GraphBLAS researcher, columnar query-engine engineer, and skeptical engineer. Knowledge domains: Cypher/GQL semantics, Neo4j-like record stores, WAL/LSM/storage engines, CSR/mmap/external-memory OLAP, GraphBLAS, property indexes, and explicit I/O.

### Phase 2: Synthesis

Conventional approach: read Neo4j and implement a compatible server. Better hybrid: treat Knight Bus as a log-coupled dual engine—mutable OLTP record/WAL path plus immutable low-RAM CSR OLAP snapshots. The copied papers are grouped around that architecture.

### Phase 3: Verification

Metadata for all 57 papers was verified through the arXiv API before download. The reproducible metadata table is `arxiv-papers.tsv`. PDFs are stored under `arxiv-papers/`.

## Decision filter

- Keep current `snapshot-v2`/dual CSR as the OLAP kernel.
- Add `snapshot-v3` sidecars for labels, relationship types, typed properties, and dictionaries.
- Add a separate OLTP record store plus WAL/delta overlay instead of making CSR the primary mutable store.
- Rebuild/atomic-swap CSR snapshots from WAL/delta for analytical freshness.
- Use explicit I/O only where deterministic RAM is more important than mmap convenience.

## Paper index

### Tier 0 must-read architecture papers

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [1910.05773](https://arxiv.org/abs/1910.05773) | [`1910.05773-livegraph-a-transactional-graph-storage-system-with-purely-sequential-adjacency-.pdf`](./arxiv-papers/1910.05773-livegraph-a-transactional-graph-storage-system-with-purely-sequential-adjacency-.pdf) | **LiveGraph: A Transactional Graph Storage System with Purely Sequential Adjacency List Scans** (2019; Xiaowei Zhu, Guanyu Feng, Marco Serafini, et al.) | `cs.DB` | LiveGraph: transactional graph storage; edge log + sequential scans for WAL/delta bridge. |
| [2502.10959](https://arxiv.org/abs/2502.10959) | [`2502.10959-revisiting-the-design-of-in-memory-dynamic-graph-storage.pdf`](./arxiv-papers/2502.10959-revisiting-the-design-of-in-memory-dynamic-graph-storage.pdf) | **Revisiting the Design of In-Memory Dynamic Graph Storage** (2025; Jixian Su, Chiyu Hao, Shixuan Sun, et al.) | `cs.DB` | Survey/analysis of in-memory dynamic graph storage; memory-overhead warning vs CSR. |
| [2411.06392](https://arxiv.org/abs/2411.06392) | [`2411.06392-lsmgraph-a-high-performance-dynamic-graph-storage-system-with-multi-level-csr.pdf`](./arxiv-papers/2411.06392-lsmgraph-a-high-performance-dynamic-graph-storage-system-with-multi-level-csr.pdf) | **LSMGraph: A High-Performance Dynamic Graph Storage System with Multi-Level CSR** (2024; Song Yu, Shufeng Gong, Qian Tao, et al.) | `cs.DB` | LSMGraph: multi-level CSR; closest fit to write-friendly deltas plus read-friendly CSR. |
| [1802.09984](https://arxiv.org/abs/1802.09984) | [`1802.09984-formal-semantics-of-the-language-cypher.pdf`](./arxiv-papers/1802.09984-formal-semantics-of-the-language-cypher.pdf) | **Formal Semantics of the Language Cypher** (2018; Nadime Francis, Alastair Green, Paolo Guagliardo, et al.) | `cs.DB, cs.PL` | Formal Cypher semantics for compatibility oracle. |
| [1705.02844](https://arxiv.org/abs/1705.02844) | [`1705.02844-formalising-opencypher-graph-queries-in-relational-algebra.pdf`](./arxiv-papers/1705.02844-formalising-opencypher-graph-queries-in-relational-algebra.pdf) | **Formalising opencypher Graph Queries in Relational Algebra** (2017; József Marton, Gábor Szárnyas, Dániel Varró) | `cs.DB` | openCypher to relational algebra; bridge to DataFusion-style plans. |
| [2112.06217](https://arxiv.org/abs/2112.06217) | [`2112.06217-graph-pattern-matching-in-gql-and-sql-pgq.pdf`](./arxiv-papers/2112.06217-graph-pattern-matching-in-gql-and-sql-pgq.pdf) | **Graph Pattern Matching in GQL and SQL/PGQ** (2021; Alin Deutsch, Nadime Francis, Alastair Green, et al.) | `cs.DB` | Graph pattern matching standards for future-proof API semantics. |
| [2010.15879](https://arxiv.org/abs/2010.15879) | [`2010.15879-log-graph-a-near-optimal-high-performance-graph-representation.pdf`](./arxiv-papers/2010.15879-log-graph-a-near-optimal-high-performance-graph-representation.pdf) | **Log(Graph): A Near-Optimal High-Performance Graph Representation** (2020; Maciej Besta, Dimitri Stanojevic, Tijana Zivic, et al.) | `cs.DS, cs.DB, cs.DC, cs.IR` | Log(Graph): low-overhead graph compression for 50GB-on-8GB goal. |
| [1602.02864](https://arxiv.org/abs/1602.02864) | [`1602.02864-semi-external-memory-sparse-matrix-multiplication-for-billion-node-graphs.pdf`](./arxiv-papers/1602.02864-semi-external-memory-sparse-matrix-multiplication-for-billion-node-graphs.pdf) | **Semi-External Memory Sparse Matrix Multiplication for Billion-Node Graphs** (2016; Da Zheng, Disa Mhembere, Vince Lyzinski, et al.) | `cs.DC` | Sparse matrix operations with graph on disk and vectors in RAM. |
| [1504.01039](https://arxiv.org/abs/1504.01039) | [`1504.01039-graphs-matrices-and-the-graphblas-seven-good-reasons.pdf`](./arxiv-papers/1504.01039-graphs-matrices-and-the-graphblas-seven-good-reasons.pdf) | **Graphs, Matrices, and the GraphBLAS: Seven Good Reasons** (2015; Jeremy Kepner, David Bader, Aydın Buluc, et al.) | `cs.DC` | Graph algorithms as sparse linear algebra. |
| [1606.05790](https://arxiv.org/abs/1606.05790) | [`1606.05790-mathematical-foundations-of-the-graphblas.pdf`](./arxiv-papers/1606.05790-mathematical-foundations-of-the-graphblas.pdf) | **Mathematical Foundations of the GraphBLAS** (2016; Jeremy Kepner, Peter Aaltonen, David Bader, et al.) | `cs.MS, astro-ph.IM, cs.DC, cs.DS` | Formal semiring foundations for algorithm substrate. |

### Cypher, GQL, SQL-PGQ, path queries

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2409.01102](https://arxiv.org/abs/2409.01102) | [`2409.01102-gql-and-sql-pgq-theoretical-models-and-expressive-power.pdf`](./arxiv-papers/2409.01102-gql-and-sql-pgq-theoretical-models-and-expressive-power.pdf) | **GQL and SQL/PGQ: Theoretical Models and Expressive Power** (2024; Amélie Gheerbrant, Leonid Libkin, Liat Peterfreund, et al.) | `cs.DB` | Theoretical model and expressive power of current graph query standards. |
| [2306.02194](https://arxiv.org/abs/2306.02194) | [`2306.02194-pathfinder-a-unified-approach-for-handling-paths-in-graph-query-languages.pdf`](./arxiv-papers/2306.02194-pathfinder-a-unified-approach-for-handling-paths-in-graph-query-languages.pdf) | **PathFinder: A unified approach for handling paths in graph query languages** (2023; Benjamín Farías, Wim Martens, Carlos Rojas, et al.) | `cs.DB` | Path handling in graph query languages. |
| [2604.02553](https://arxiv.org/abs/2604.02553) | [`2604.02553-efficient-path-query-processing-in-relational-database-systems.pdf`](./arxiv-papers/2604.02553-efficient-path-query-processing-in-relational-database-systems.pdf) | **Efficient Path Query Processing in Relational Database Systems** (2026; Diego Rivera Correa, Mirek Riedewald) | `cs.DB` | Relational processing of path queries. |
| [2504.04584](https://arxiv.org/abs/2504.04584) | [`2504.04584-barq-a-vectorized-sparql-query-execution-engine.pdf`](./arxiv-papers/2504.04584-barq-a-vectorized-sparql-query-execution-engine.pdf) | **BARQ: A Vectorized SPARQL Query Execution Engine** (2025; Simon Grätzer, Lars Heling, Pavel Klinov) | `cs.DB` | BARQ: vectorized SPARQL engine; useful for graph-query execution design. |

### Query indexes and property/path constraints

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2003.03079](https://arxiv.org/abs/2003.03079) | [`2003.03079-language-aware-indexing-for-conjunctive-path-queries.pdf`](./arxiv-papers/2003.03079-language-aware-indexing-for-conjunctive-path-queries.pdf) | **Language-aware Indexing for Conjunctive Path Queries** (2020; Yuya Sasaki, George Fletcher, Makoto Onizuka) | `cs.DB` | Language-aware/structural indexing for conjunctive path queries. |
| [2512.01733](https://arxiv.org/abs/2512.01733) | [`2512.01733-answering-constraint-path-queries-over-graphs.pdf`](./arxiv-papers/2512.01733-answering-constraint-path-queries-over-graphs.pdf) | **Answering Constraint Path Queries over Graphs** (2025; Heyang Li, Anthony Widjaja Lin, Domagoj Vrgoč) | `cs.DB` | Constraint path queries over property graphs; future advanced Cypher/GQL planning input. |

### OLTP/dynamic graph storage

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [1904.08380](https://arxiv.org/abs/1904.08380) | [`1904.08380-low-latency-graph-streaming-using-compressed-purely-functional-trees.pdf`](./arxiv-papers/1904.08380-low-latency-graph-streaming-using-compressed-purely-functional-trees.pdf) | **Low-Latency Graph Streaming Using Compressed Purely-Functional Trees** (2019; Laxman Dhulipala, Julian Shun, Guy Blelloch) | `cs.DC, cs.DS, cs.PL` | Aspen/C-trees: compressed purely-functional trees for low-latency graph streaming. |
| [2507.00839](https://arxiv.org/abs/2507.00839) | [`2507.00839-rapidstore-an-efficient-dynamic-graph-storage-system-for-concurrent-queries.pdf`](./arxiv-papers/2507.00839-rapidstore-an-efficient-dynamic-graph-storage-system-for-concurrent-queries.pdf) | **RapidStore: An Efficient Dynamic Graph Storage System for Concurrent Queries** (2025; Chiyu Hao, Jixian Su, Shixuan Sun, et al.) | `cs.DB` | RapidStore: read-heavy concurrent dynamic graph storage. |
| [2601.01444](https://arxiv.org/abs/2601.01444) | [`2601.01444-radixgraph-a-fast-space-optimized-data-structure-for-dynamic-graph-storage-exten.pdf`](./arxiv-papers/2601.01444-radixgraph-a-fast-space-optimized-data-structure-for-dynamic-graph-storage-exten.pdf) | **RadixGraph: A Fast, Space-Optimized Data Structure for Dynamic Graph Storage (Extended Version)** (2026; Haoxuan Xie, Junfeng Liu, Siqiang Luo, et al.) | `cs.DB` | RadixGraph: newer space-optimized dynamic graph storage. |
| [2312.14396](https://arxiv.org/abs/2312.14396) | [`2312.14396-gastcoco-graph-storage-and-coroutine-based-prefetch-co-design-for-dynamic-graph-.pdf`](./arxiv-papers/2312.14396-gastcoco-graph-storage-and-coroutine-based-prefetch-co-design-for-dynamic-graph-.pdf) | **GastCoCo: Graph Storage and Coroutine-Based Prefetch Co-Design for Dynamic Graph Processing** (2023; Hongfu Li, Qian Tao, Song Yu, et al.) | `cs.DB` | GastCoCo: graph storage + coroutine prefetch co-design. |
| [1912.12740](https://arxiv.org/abs/1912.12740) | [`1912.12740-practice-of-streaming-processing-of-dynamic-graphs-concepts-models-and-systems.pdf`](./arxiv-papers/1912.12740-practice-of-streaming-processing-of-dynamic-graphs-concepts-models-and-systems.pdf) | **Practice of Streaming Processing of Dynamic Graphs: Concepts, Models, and Systems** (2019; Maciej Besta, Marc Fischer, Vasiliki Kalavri, et al.) | `cs.DC, cs.DB, cs.DS, cs.PF` | Taxonomy for streaming/dynamic graph systems. |

### OLTP/OLAP graph engine interface

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2305.11162](https://arxiv.org/abs/2305.11162) | [`2305.11162-the-graph-database-interface-scaling-online-transactional-and-analytical-graph-w.pdf`](./arxiv-papers/2305.11162-the-graph-database-interface-scaling-online-transactional-and-analytical-graph-w.pdf) | **The Graph Database Interface: Scaling Online Transactional and Analytical Graph Workloads to Hundreds of Thousands of Cores** (2023; Maciej Besta, Robert Gerstenberger, Marc Fischer, et al.) | `cs.DB, cs.DC` | Interface for scaling OLTP and OLAP graph workloads. |
| [2510.11166](https://arxiv.org/abs/2510.11166) | [`2510.11166-poseidon-a-onegraph-engine.pdf`](./arxiv-papers/2510.11166-poseidon-a-onegraph-engine.pdf) | **Poseidon: A OneGraph Engine** (2025; Brad Bebee, Ümit V. Çatalyürek, Olaf Hartig, et al.) | `cs.DB` | Recent graph engine architecture; useful for unified API + algorithms. |

### Persistent graph database architecture

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2111.01540](https://arxiv.org/abs/2111.01540) | [`2111.01540-millenniumdb-a-persistent-open-source-graph-database.pdf`](./arxiv-papers/2111.01540-millenniumdb-a-persistent-open-source-graph-database.pdf) | **MillenniumDB: A Persistent, Open-Source, Graph Database** (2021; Domagoj Vrgoc, Carlos Rojas, Renzo Angles, et al.) | `cs.DB` | MillenniumDB: storage, indexing, planning, path-query architecture. |

### Low-RAM graph representation and compression

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [1812.10977](https://arxiv.org/abs/1812.10977) | [`1812.10977-compact-and-efficient-representation-of-general-graph-databases.pdf`](./arxiv-papers/1812.10977-compact-and-efficient-representation-of-general-graph-databases.pdf) | **Compact and Efficient Representation of General Graph Databases** (2018; Sandra Álvarez-García, Borja Freire, Susana Ladra, et al.) | `cs.DS, cs.DB` | Compact general graph databases; property/labeled graph compression. |
| [1006.0809](https://arxiv.org/abs/1006.0809) | [`1006.0809-tight-and-simple-web-graph-compression.pdf`](./arxiv-papers/1006.0809-tight-and-simple-web-graph-compression.pdf) | **Tight and simple Web graph compression** (2010; Szymon Grabowski, Wojciech Bieniecki) | `cs.DS` | Classic adjacency compression baseline. |
| [1602.08820](https://arxiv.org/abs/1602.08820) | [`1602.08820-compressing-graphs-and-indexes-with-recursive-graph-bisection.pdf`](./arxiv-papers/1602.08820-compressing-graphs-and-indexes-with-recursive-graph-bisection.pdf) | **Compressing Graphs and Indexes with Recursive Graph Bisection** (2016; Laxman Dhulipala, Igor Kabiljo, Brian Karrer, et al.) | `cs.DS, cs.SI` | Recursive bisection for graph/index compression and locality. |

### Persistent-memory/dynamic CSR alternatives

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2403.02665](https://arxiv.org/abs/2403.02665) | [`2403.02665-dgap-efficient-dynamic-graph-analysis-on-persistent-memory.pdf`](./arxiv-papers/2403.02665-dgap-efficient-dynamic-graph-analysis-on-persistent-memory.pdf) | **DGAP: Efficient Dynamic Graph Analysis on Persistent Memory** (2024; Abdullah Al Raqibul Islam, Dong Dai) | `cs.DS, cs.DC, cs.PF` | DGAP: mutable CSR on persistent memory; useful as alternative/anti-pattern for v003. |

### Semi-external and out-of-core graph analytics

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [1707.02557](https://arxiv.org/abs/1707.02557) | [`1707.02557-graphmp-an-efficient-semi-external-memory-big-graph-processing-system-on-a-singl.pdf`](./arxiv-papers/1707.02557-graphmp-an-efficient-semi-external-memory-big-graph-processing-system-on-a-singl.pdf) | **GraphMP: An Efficient Semi-External-Memory Big Graph Processing System on a Single Machine** (2017; Peng Sun, Yonggang Wen, Ta Nguyen Binh Duong, et al.) | `cs.DC` | GraphMP single-machine semi-external graph processing. |
| [1810.04334](https://arxiv.org/abs/1810.04334) | [`1810.04334-graphmp-i-o-efficient-big-graph-analytics-on-a-single-commodity-machine.pdf`](./arxiv-papers/1810.04334-graphmp-i-o-efficient-big-graph-analytics-on-a-single-commodity-machine.pdf) | **GraphMP: I/O-Efficient Big Graph Analytics on a Single Commodity Machine** (2018; Peng Sun, Yonggang Wen, Ta Nguyen Binh Duong, et al.) | `cs.DC` | GraphMP I/O-efficient commodity-machine analytics. |
| [1905.04264](https://arxiv.org/abs/1905.04264) | [`1905.04264-partitionedvc-partitioned-external-memory-graph-analytics-framework-for-ssds.pdf`](./arxiv-papers/1905.04264-partitionedvc-partitioned-external-memory-graph-analytics-framework-for-ssds.pdf) | **PartitionedVC: Partitioned External Memory Graph Analytics Framework for SSDs** (2019; Kiran Kumar Matam, Hanieh Hashemi, Murali Annavaram) | `cs.DC` | PartitionedVC for SSD-backed active-vertex workloads. |
| [1710.07736](https://arxiv.org/abs/1710.07736) | [`1710.07736-bigsparse-high-performance-external-graph-analytics.pdf`](./arxiv-papers/1710.07736-bigsparse-high-performance-external-graph-analytics.pdf) | **BigSparse: High-performance external graph analytics** (2017; Sang-Woo Jun, Andy Wright, Sizhuo Zhang, et al.) | `cs.DB` | BigSparse external graph analytics. |
| [1907.03335](https://arxiv.org/abs/1907.03335) | [`1907.03335-graphyti-a-semi-external-memory-graph-library-for-flashgraph.pdf`](./arxiv-papers/1907.03335-graphyti-a-semi-external-memory-graph-library-for-flashgraph.pdf) | **Graphyti: A Semi-External Memory Graph Library for FlashGraph** (2019; Disa Mhembere, Da Zheng, Carey E. Priebe, et al.) | `cs.DC, cs.DB` | Graphyti/FlashGraph semi-external library patterns. |
| [2101.06911](https://arxiv.org/abs/2101.06911) | [`2101.06911-dfograph-an-i-o-and-communication-efficient-system-for-distributed-fully-out-of-.pdf`](./arxiv-papers/2101.06911-dfograph-an-i-o-and-communication-efficient-system-for-distributed-fully-out-of-.pdf) | **DFOGraph: An I/O- and Communication-Efficient System for Distributed Fully-out-of-Core Graph Processing** (2021; Jiping Yu, Wei Qin, Xiaowei Zhu, et al.) | `cs.DC` | DFOGraph; lower priority because distributed, but useful for partitioning. |

### CSR builder/loading path

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2311.14650](https://arxiv.org/abs/2311.14650) | [`2311.14650-gvel-fast-graph-loading-in-edgelist-and-compressed-sparse-row-csr-formats.pdf`](./arxiv-papers/2311.14650-gvel-fast-graph-loading-in-edgelist-and-compressed-sparse-row-csr-formats.pdf) | **GVEL: Fast Graph Loading in Edgelist and Compressed Sparse Row (CSR) formats** (2023; Subhajit Sahu) | `cs.PF` | GVEL: fast edgelist and CSR loading. |

### WAL, LSM, KV separation, compaction

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2407.15581](https://arxiv.org/abs/2407.15581) | [`2407.15581-vlsm-low-tail-latency-and-i-o-amplification-in-lsm-based-kv-stores.pdf`](./arxiv-papers/2407.15581-vlsm-low-tail-latency-and-i-o-amplification-in-lsm-based-kv-stores.pdf) | **vLSM: Low tail latency and I/O amplification in LSM-based KV stores** (2024; Giorgos Xanthakis, Antonios Katsarakis, Giorgos Saloustros, et al.) | `cs.DB` | vLSM: tail latency and amplification tradeoffs. |
| [2506.04678](https://arxiv.org/abs/2506.04678) | [`2506.04678-bvlsm-write-efficient-lsm-tree-storage-via-wal-time-key-value-separation.pdf`](./arxiv-papers/2506.04678-bvlsm-write-efficient-lsm-tree-storage-via-wal-time-key-value-separation.pdf) | **BVLSM: Write-Efficient LSM-Tree Storage via WAL-Time Key-Value Separation** (2025; Ming Li, Wendi Cheng, Jiahe Wei, et al.) | `cs.DB` | BVLSM: WAL-time key-value separation; relevant for property sidecars. |
| [2005.14213](https://arxiv.org/abs/2005.14213) | [`2005.14213-from-wisckey-to-bourbon-a-learned-index-for-log-structured-merge-trees.pdf`](./arxiv-papers/2005.14213-from-wisckey-to-bourbon-a-learned-index-for-log-structured-merge-trees.pdf) | **From WiscKey to Bourbon: A Learned Index for Log-Structured Merge Trees** (2020; Yifan Dai, Yien Xu, Aishwarya Ganesan, et al.) | `cs.DB, cs.LG` | Bourbon: learned index for LSM; future property/key index ideas. |
| [2307.16693](https://arxiv.org/abs/2307.16693) | [`2307.16693-aislsm-revolutionizing-the-compaction-with-asynchronous-i-os-for-lsm-tree.pdf`](./arxiv-papers/2307.16693-aislsm-revolutionizing-the-compaction-with-asynchronous-i-os-for-lsm-tree.pdf) | **AisLSM: Revolutionizing the Compaction with Asynchronous I/Os for LSM-tree** (2023; Yanpeng Hu, Li Zhu, Lei Jia, et al.) | `cs.DB` | AisLSM: asynchronous I/O for LSM compaction. |
| [2308.07013](https://arxiv.org/abs/2308.07013) | [`2308.07013-learning-to-optimize-lsm-trees-towards-a-reinforcement-learning-based-key-value-.pdf`](./arxiv-papers/2308.07013-learning-to-optimize-lsm-trees-towards-a-reinforcement-learning-based-key-value-.pdf) | **Learning to Optimize LSM-trees: Towards A Reinforcement Learning based Key-Value Store for Dynamic Workloads** (2023; Dingheng Mo, Fanchao Chen, Siqiang Luo, et al.) | `cs.DB, cs.LG` | RL-based dynamic LSM optimization. |
| [2504.17178](https://arxiv.org/abs/2504.17178) | [`2504.17178-how-to-grow-an-lsm-tree-towards-bridging-the-gap-between-theory-and-practice.pdf`](./arxiv-papers/2504.17178-how-to-grow-an-lsm-tree-towards-bridging-the-gap-between-theory-and-practice.pdf) | **How to Grow an LSM-tree? Towards Bridging the Gap Between Theory and Practice** (2025; Dingheng Mo, Siqiang Luo, Stratos Idreos) | `cs.DB` | Vertiorizon: LSM growth strategy tradeoffs. |
| [2406.18892](https://arxiv.org/abs/2406.18892) | [`2406.18892-learnedkv-integrating-lsm-and-learned-index-for-superior-performance-on-storage.pdf`](./arxiv-papers/2406.18892-learnedkv-integrating-lsm-and-learned-index-for-superior-performance-on-storage.pdf) | **LearnedKV: Integrating LSM and Learned Index for Superior Performance on Storage** (2024; Wenlong Wang, David Hung-Chang Du) | `cs.DB, cs.LG` | LearnedKV: LSM + learned index tiering. |
| [2508.13909](https://arxiv.org/abs/2508.13909) | [`2508.13909-scavenger-better-space-time-trade-offs-for-key-value-separated-lsm-trees.pdf`](./arxiv-papers/2508.13909-scavenger-better-space-time-trade-offs-for-key-value-separated-lsm-trees.pdf) | **Scavenger: Better Space-Time Trade-Offs for Key-Value Separated LSM-trees** (2025; Jianshun Zhang, Fang Wang, Sheng Qiu, et al.) | `cs.DB` | Scavenger: space-time tradeoffs for KV-separated LSM. |
| [2508.13935](https://arxiv.org/abs/2508.13935) | [`2508.13935-scavenger-revisiting-space-time-tradeoffs-in-key-value-separated-lsm-trees.pdf`](./arxiv-papers/2508.13935-scavenger-revisiting-space-time-tradeoffs-in-key-value-separated-lsm-trees.pdf) | **Scavenger+: Revisiting Space-Time Tradeoffs in Key-Value Separated LSM-trees** (2025; Jianshun Zhang, Fang Wang, Jiaxin Ou, et al.) | `cs.DB` | Scavenger+: revisited key-value separated LSM tradeoffs. |
| [2603.05162](https://arxiv.org/abs/2603.05162) | [`2603.05162-resystance-unleashing-hidden-performance-of-compaction-in-lsm-trees-via-ebpf.pdf`](./arxiv-papers/2603.05162-resystance-unleashing-hidden-performance-of-compaction-in-lsm-trees-via-ebpf.pdf) | **RESYSTANCE: Unleashing Hidden Performance of Compaction in LSM-trees via eBPF** (2026; Hongsu Byun, Seungjae Lee, Honghyeon Yoo, et al.) | `cs.DB` | Resystance: kernel/offload path for LSM compaction. |

### Memory-mapped storage structures

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2005.13762](https://arxiv.org/abs/2005.13762) | [`2005.13762-cedrusdb-persistent-key-value-store-with-memory-mapped-lazy-trie.pdf`](./arxiv-papers/2005.13762-cedrusdb-persistent-key-value-store-with-memory-mapped-lazy-trie.pdf) | **CedrusDB: Persistent Key-Value Store with Memory-Mapped Lazy-Trie** (2020; Maofan Yin, Hongbo Zhang, Robbert van Renesse, et al.) | `cs.DB` | CedrusDB: memory-mapped lazy-trie KV store. |

### Explicit async/direct I/O

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2512.04859](https://arxiv.org/abs/2512.04859) | [`2512.04859-high-performance-dbmss-with-io-uring-when-and-how-to-use-it.pdf`](./arxiv-papers/2512.04859-high-performance-dbmss-with-io-uring-when-and-how-to-use-it.pdf) | **High-Performance DBMSs with io_uring: When and How to use it** (2025; Matthias Jasny, Muhammad El-Hindi, Tobias Ziegler, et al.) | `cs.DB` | When/how io_uring helps DBMS storage and shuffle workloads. |

### Persistent memory and NVM storage

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2112.00425](https://arxiv.org/abs/2112.00425) | [`2112.00425-how-to-use-persistent-memory-in-your-database.pdf`](./arxiv-papers/2112.00425-how-to-use-persistent-memory-in-your-database.pdf) | **How to use Persistent Memory in your Database** (2021; Dimitrios Koutsoukos, Raghav Bhartia, Ana Klimovic, et al.) | `cs.DB` | Empirical PMEM database guidance. |
| [2502.09431](https://arxiv.org/abs/2502.09431) | [`2502.09431-on-usage-of-non-volatile-memory-as-primary-storage-for-database-management-syste.pdf`](./arxiv-papers/2502.09431-on-usage-of-non-volatile-memory-as-primary-storage-for-database-management-syste.pdf) | **On Usage of Non-Volatile Memory as Primary Storage for Database Management Systems** (2025; Naveed Ul Mustafa, Adri`a Armejach, Ozcan Ozturk, et al.) | `cs.DB` | NVM as primary storage for DBMS. |
| [2506.14630](https://arxiv.org/abs/2506.14630) | [`2506.14630-keigo-co-designing-log-structured-merge-key-value-stores-with-a-non-volatile-con.pdf`](./arxiv-papers/2506.14630-keigo-co-designing-log-structured-merge-key-value-stores-with-a-non-volatile-con.pdf) | **Keigo: Co-designing Log-Structured Merge Key-Value Stores with a Non-Volatile, Concurrency-aware Storage Hierarchy (Extended Version)** (2025; Rúben Adão, Zhongjie Wu, Changjun Zhou, et al.) | `cs.DC, cs.DB` | Keigo: LSM with non-volatile concurrency-aware hierarchy. |
| [2108.07223](https://arxiv.org/abs/2108.07223) | [`2108.07223-metall-a-persistent-memory-allocator-for-data-centric-analytics.pdf`](./arxiv-papers/2108.07223-metall-a-persistent-memory-allocator-for-data-centric-analytics.pdf) | **Metall: A Persistent Memory Allocator For Data-Centric Analytics** (2021; Keita Iwabuchi, Karim Youssef, Kaushik Velusamy, et al.) | `cs.DC` | Metall: persistent memory allocator for data-centric analytics. |
| [2305.09034](https://arxiv.org/abs/2305.09034) | [`2305.09034-blizzard-adding-true-persistence-to-main-memory-data-structures.pdf`](./arxiv-papers/2305.09034-blizzard-adding-true-persistence-to-main-memory-data-structures.pdf) | **Blizzard: Adding True Persistence to Main Memory Data Structures** (2023; Pradeep Fernando, Daniel Zahka, Ada Gavrilovska, et al.) | `cs.DC` | Blizzard: fault-tolerant PMEM persistent programming runtime. |

### Arrow/columnar property storage

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2105.09894](https://arxiv.org/abs/2105.09894) | [`2105.09894-towards-an-arrow-native-storage-system.pdf`](./arxiv-papers/2105.09894-towards-an-arrow-native-storage-system.pdf) | **Towards an Arrow-native Storage System** (2021; Jayjeet Chakraborty, Ivo Jimenez, Sebastiaan Alvarez Rodriguez, et al.) | `cs.DC` | Arrow-native storage for zero-copy property columns. |

### Vectorized query-engine execution model

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [1610.09166](https://arxiv.org/abs/1610.09166) | [`1610.09166-push-vs-pull-based-loop-fusion-in-query-engines.pdf`](./arxiv-papers/1610.09166-push-vs-pull-based-loop-fusion-in-query-engines.pdf) | **Push vs. Pull-Based Loop Fusion in Query Engines** (2016; Amir Shaikhha, Mohammad Dashti, Christoph Koch) | `cs.DB, cs.PL` | Loop fusion and execution model choice. |

### HTAP and near-memory execution

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2601.12456](https://arxiv.org/abs/2601.12456) | [`2601.12456-bringing-data-transformations-near-memory-for-low-latency-analytics-in-htap-envi.pdf`](./arxiv-papers/2601.12456-bringing-data-transformations-near-memory-for-low-latency-analytics-in-htap-envi.pdf) | **Bringing Data Transformations Near-Memory for Low-Latency Analytics in HTAP Environments** (2026; Arthur Bernhardt, David Volz, Sajjad Tamimi, et al.) | `cs.DB` | Near-memory transformations for HTAP; speculative later input. |

### Future GPU/accelerated analytical execution

| arXiv | local PDF | paper | categories | why it matters |
| --- | --- | --- | --- | --- |
| [2602.17335](https://arxiv.org/abs/2602.17335) | [`2602.17335-do-gpus-really-need-new-tabular-file-formats.pdf`](./arxiv-papers/2602.17335-do-gpus-really-need-new-tabular-file-formats.pdf) | **Do GPUs Really Need New Tabular File Formats?** (2026; Jigao Luo, Qi Chen, Carsten Binnig) | `cs.DB, cs.DC` | Skepticism about new tabular formats; future GPU path only. |
| [2508.04701](https://arxiv.org/abs/2508.04701) | [`2508.04701-rethinking-analytical-processing-in-the-gpu-era.pdf`](./arxiv-papers/2508.04701-rethinking-analytical-processing-in-the-gpu-era.pdf) | **Rethinking Analytical Processing in the GPU Era** (2025; Bobbi Yogatama, Yifei Yang, Kevin Kristensen, et al.) | `cs.DB` | GPU analytics execution trends; later acceleration only. |

## Exclusions

- Generic graph neural network papers are excluded unless they affect storage/layout/query execution.
- Cluster-only systems are mostly excluded because v003 is explicitly single-node community edition; a few are included only when they provide useful storage or partitioning ideas.
- Non-arXiv-only canonical systems papers are not copied here because the user explicitly asked for arXiv papers.

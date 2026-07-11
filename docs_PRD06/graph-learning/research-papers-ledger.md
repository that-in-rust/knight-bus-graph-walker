# Research Papers Ledger

The literature spine behind the corpus — the papers that the corpus repos
*implement*. Two tiers of confidence:

- **Verified**: arXiv ID resolved live against the arXiv API during this
  research round (title confirmed).
- **Venue**: published at SIGMOD/VLDB/OSDI/etc. and not (reliably) on arXiv;
  cited from knowledge — verify independently before quoting details.

Reading order suggestion per category is roughly the row order.

## 1. Graph processing & analytics

| Paper | Where | Status | Why it's canonical |
| --- | --- | --- | --- |
| Pregel: A System for Large-Scale Graph Processing (Malewicz et al., 2010) | SIGMOD'10 | Venue | Origin of vertex-centric / BSP "think like a vertex"; ancestor of Giraph (in corpus) |
| Ligra: A Lightweight Graph Processing Framework for Shared Memory (Shun & Blelloch, 2013) | PPoPP'13 | Venue | edgeMap/vertexMap + push/pull direction switching; corpus: `ligra-src` |
| Theoretically Efficient Parallel Graph Algorithms Can Be Fast and Scalable | arXiv:1805.05208 | Verified | The GBBS paper; corpus: `gbbs-src` |
| GraphChi: Large-Scale Graph Computation on Just a PC (Kyrola et al., 2012) | OSDI'12 | Venue | Parallel sliding windows — out-of-core graphs on one machine; the low-RAM ancestor of this repo's thesis; corpus: `graphchi-cpp-src` |
| X-Stream: Edge-centric Graph Processing using Streaming Partitions (2013) | SOSP'13 | Venue | Edge-centric scatter-gather; sequential I/O beats random |
| GridGraph: Large-Scale Graph Processing on a Single Machine Using 2-Level Hierarchical Partitioning (2015) | USENIX ATC'15 | Venue | 2D-partitioned out-of-core processing; corpus round-2 addition |
| PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs (2012) | OSDI'12 | Venue | Vertex-cut partitioning + GAS model for power-law graphs |
| Scalability! But at what COST? (McSherry et al., 2015) | HotOS'15 | Venue | Single thread beats clusters; the skeptic's benchmark lens — core to this repo's 8GB-beats-cluster positioning |
| The GAP Benchmark Suite | arXiv:1508.03619 | Verified | Standard 6-kernel graph benchmark + direction-optimizing BFS reference code |
| Low-Latency Graph Streaming Using Compressed Purely-Functional Trees (Aspen) | arXiv:1904.08380 | Verified | Immutable functional graph snapshots (C-trees) — structurally what kb-graph sampled |
| Fast unfolding of communities in large networks (Louvain) | arXiv:0803.0476 | Verified | THE community-detection algorithm in GDS |
| From Louvain to Leiden: guaranteeing well-connected communities | arXiv:1810.08473 | Verified | Fixes Louvain's disconnected-community defect |
| SNAP: A General Purpose Network Analysis and Graph Mining Library | arXiv:1606.07550 | Verified | Corpus: `snap-src` |
| The Anatomy of a Large-Scale Hypertextual Web Search Engine (Brin & Page, 1998) | WWW7 | Venue | PageRank's original statement |

## 2. Graph embeddings (GDS surface)

| Paper | Where | Status | Why |
| --- | --- | --- | --- |
| DeepWalk: Online Learning of Social Representations | arXiv:1403.6652 | Verified | Random-walk embeddings; ancestor of node2vec |
| node2vec: Scalable Feature Learning for Networks | arXiv:1607.00653 | Verified | GDS `gds.node2vec` |
| Fast and Accurate Network Embeddings via Very Sparse Random Projection (FastRP) | arXiv:1908.11512 | Verified | GDS's default embedding `gds.fastRP` — directly in the parity surface |
| Inductive Representation Learning on Large Graphs (GraphSAGE) | arXiv:1706.02216 | Verified | GDS `gds.beta.graphSage` |

## 3. Vector / ANN

| Paper | Where | Status | Why |
| --- | --- | --- | --- |
| Efficient and robust approximate nearest neighbor search using HNSW | arXiv:1603.09320 | Verified | THE vector-index paper; a graph algorithm wearing a search costume; corpus: hnswlib et al. |
| DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node (2019) | NeurIPS'19 | Venue | Vamana graph on SSD under a RAM budget — the closest published cousin of this repo's thesis |
| FreshDiskANN: A Fast and Accurate Graph-Based ANN Index for Streaming Similarity Search | arXiv:2105.09613 | Verified | Updatable disk ANN — the OLTP/OLAP lag problem in vector form |
| Product Quantization for Nearest Neighbor Search (Jégou et al., 2011) | TPAMI'11 | Venue | PQ compression; basis of IVF-PQ everywhere |
| Billion-scale similarity search with GPUs | arXiv:1702.08734 | Verified | The Faiss GPU paper |
| The Faiss library | arXiv:2401.08281 | Verified | The definitive Faiss retrospective |
| A Comprehensive Survey and Experimental Comparison of Graph-Based Approximate Nearest Neighbor Search | arXiv:2101.12631 | Verified | Best map of the graph-ANN design space |
| Survey of Vector Database Management Systems | arXiv:2310.14021 | Verified | Systems-level (not algorithm-level) survey |
| Accelerating Large-Scale Inference with Anisotropic Vector Quantization (ScaNN, 2020) | ICML'20 | Venue | Google's quantization behind Vertex AI Vector Search |

## 4. Query languages & graph DB systems

| Paper | Where | Status | Why |
| --- | --- | --- | --- |
| Formal Semantics of the Language Cypher | arXiv:1802.09984 | Verified | The formal spec of the language this repo must be parity-true to |
| Cypher: An Evolving Query Language for Property Graphs (2018) | SIGMOD'18 | Venue | Cypher's design rationale from the Neo4j team |
| The LDBC Social Network Benchmark | arXiv:2001.02299 | Verified | The graph-DB benchmark standard; corpus: ldbc impls |
| Kùzu: A Database Management System For "Beyond Relational" Workloads (2023) | CIDR'23 | Venue | Columnar graph storage + factorized execution; corpus: `kuzu-src` |
| The Ubiquity of Large Graphs and Surprising Challenges of Graph Processing (Sahu et al., 2017) | VLDB'18 | Venue | The user-survey paper: what practitioners actually struggle with (memory!) |
| TAO: Facebook's Distributed Data Store for the Social Graph (2013) | USENIX ATC'13 | Venue | Graph served from a cache hierarchy, not a graph DB |

## 5. Storage engines

| Paper | Where | Status | Why |
| --- | --- | --- | --- |
| The Log-Structured Merge-Tree (O'Neil et al., 1996) | Acta Informatica | Venue | The LSM paper under RocksDB/LevelDB/Pebble/sled/fjall |
| ARIES: A Transaction Recovery Method... (Mohan et al., 1992) | TODS'92 | Venue | WAL + redo/undo recovery — the vocabulary every engine uses |
| Bigtable: A Distributed Storage System for Structured Data (2006) | OSDI'06 | Venue | SSTables + memtables as deployed at scale |
| RocksDB: Evolution of Development Priorities in a Key-Value Store... (Dong et al., 2021) | FAST'21 | Venue | What a decade of production LSM taught Facebook |
| LeanStore: In-Memory Data Management Beyond Main Memory (2018) | ICDE'18 | Venue | Pointer swizzling — the anti-mmap buffer manager |
| Umbra: A Disk-Based System with In-Memory Performance (2020) | CIDR'20 | Venue | Variable-size pages over SSD |
| Are You Sure You Want to Use MMAP in Your DBMS? (Crotty et al., 2022) | CIDR'22 | Venue | **The prosecution's case against this repo's mmap architecture — must-read adversarial literature** |

## 6. Full-text search

| Paper | Where | Status | Why |
| --- | --- | --- | --- |
| Okapi at TREC-3 (Robertson et al., 1994) | TREC-3 | Venue | BM25's origin |
| Efficient Query Evaluation using a Two-Level Retrieval Process (WAND, 2003) | CIKM'03 | Venue | Dynamic pruning of posting lists |
| Faster Top-k Document Retrieval Using Block-Max Indexes (2011) | SIGIR'11 | Venue | Block-max WAND — in Lucene/Tantivy today |
| Direct Construction of Minimal Acyclic Subsequential Transducers (2001) | — | Venue | The FST construction behind Lucene/Tantivy term dictionaries |

## Cross-links to the corpus

- Every Verified row can be fetched as `https://arxiv.org/abs/<ID>`.
- Papers ↔ repos: Ligra/GBBS/GraphChi/SNAP/Kuzu/hnswlib/Faiss/DiskANN rows
  point at corpus clones; pattern docs should cite paper + code together.
- The two adversarial reads for this repo's own architecture: *COST*
  (against cluster-scale claims — supports us) and *mmap-CIDR22* (against
  mmap-based engines — challenges us; the receipts/verification story is
  partly an answer to it).

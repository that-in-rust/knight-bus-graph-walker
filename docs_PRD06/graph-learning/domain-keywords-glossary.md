# Domain Keywords Glossary

The vocabulary of the graph / vector-search / full-text-search / storage
space, organized by category. Two uses: (1) search keywords — what to type
into GitHub/papers/docs to find things in this area; (2) concept keywords —
the terms the pattern docs in this folder will keep using. Terms marked ★
are the ones most central to this repo's own thesis (low-RAM graph engine,
Neo4j/GDS parity).

## 1. Graph databases (OLTP)

**Systems**: Neo4j, Memgraph, Kuzu (archived), FalkorDB, Dgraph, NebulaGraph,
ArangoDB, JanusGraph, TuGraph, Apache AGE, DuckPGQ, OrientDB, HugeGraph,
Cayley, TypeDB, TerminusDB, IndraDB, SurrealDB, HelixDB, CozoDB, Amazon
Neptune (closed; Blazegraph ancestor), TigerGraph (closed).

**Query languages**: Cypher ★, openCypher ★, GQL (ISO standard), Gremlin
(TinkerPop), SPARQL (RDF), AQL (Arango), nGQL (Nebula), Datalog (Cozo),
SQL/PGQ (property graph queries in SQL — DuckPGQ).

**Protocol / wire**: Bolt protocol ★, PackStream ★, driver handshake,
routing table, causal consistency / bookmarks.

**Data models**: property graph ★, labeled property graph (LPG), RDF triple
store, quad store, hypergraph, multi-model.

**Core concepts**: node/relationship/label/property, index-free adjacency ★,
traversal, expand ★, variable-length path, shortest path, pattern matching,
query planner, eager vs lazy evaluation, transaction (ACID), MVCC ★,
write-ahead log (WAL) ★, page cache ★, record store, schema constraints,
full-text + vector indexes inside graph DBs.

## 2. Graph analytics / algorithms (OLAP)

**Systems**: Neo4j GDS ★ / OpenGDS ★, GraphBLAS, LAGraph, Ligra, GBBS,
GraphChi, FlashX, Gunrock (GPU), Galois, GraphIt, Giraph, cuGraph,
NetworkX, igraph, NetworKit, SNAP, GAPBS (benchmark), METIS/KaHIP
(partitioners), WebGraph, petgraph, JGraphT, parlaylib.

**Algorithm families** ★ (the seven from PRD04): community detection
(Louvain, Leiden, label propagation, WCC ★ / connected components, SCC),
centrality (PageRank ★, betweenness, degree, closeness, eigenvector,
ArticleRank), pathfinding (BFS/DFS, Dijkstra, A*, Yen's k-shortest,
delta-stepping), similarity (node similarity ★, Jaccard, overlap, cosine,
KNN), link prediction (Adamic-Adar, common neighbors), embeddings (FastRP ★,
node2vec, GraphSAGE, HashGNN), topological (triangle counting ★, local
clustering coefficient, k-core).

**Execution models**: vertex-centric ("think like a vertex", Pregel), BSP
(bulk synchronous parallel), push vs pull ★ (direction-optimizing BFS),
frontier ★, edgeMap/vertexMap (Ligra), linear algebra on sparse matrices
(GraphBLAS: SpMV, SpGEMM, semirings), asynchronous execution, delta-based
incremental computation, GAS (gather-apply-scatter).

**Out-of-core / low-RAM** ★ (this repo's home turf): external-memory graph
processing, GraphChi parallel sliding windows, GridGraph 2D partitioning,
X-Stream edge-centric streaming, semi-external algorithms, mmap-based
processing ★, memory budget / reservation ★, scratch space ★.

## 3. Graph storage & representation

CSR (compressed sparse row) ★, CSC, dual-CSR ★ (forward+reverse), adjacency
list, adjacency matrix, COO (edge list), dense ID mapping ★ (u32 internal
IDs), ID compaction, delta encoding / varint / v-byte, gap compression,
WebGraph compression (referentialization), immutable snapshot ★, sealed
segment, structural sharing (Aspen C-trees), degree-ordered layout,
Hilbert/Z-order edge ordering, partitioning (edge-cut vs vertex-cut),
columnar property sidecars ★, null bitmaps, dictionary encoding.

## 4. Vector search / ANN

**Systems**: FAISS, Milvus, Qdrant, Weaviate, Vespa, Chroma, LanceDB/Lance,
pgvector, pgvecto.rs, hnswlib, Annoy, DiskANN ★, NMSLIB, usearch, voyager,
ScaNN, cuVS, jvector, zvec, Vald, Pinecone (closed).

**Index structures**: HNSW ★ (hierarchical navigable small world — a
*designed graph*: greedy search, layer hierarchy, efConstruction/efSearch,
M neighbors), IVF (inverted file), IVF-PQ, product quantization (PQ),
scalar quantization, binary quantization, OPQ, Vamana graph ★ (DiskANN),
NSG, KD-tree, random projection trees (Annoy), LSH.

**Concepts**: ANN (approximate nearest neighbor), recall@k, embedding,
distance metrics (cosine, dot product, L2/Euclidean, Hamming), curse of
dimensionality, filtered vector search, hybrid search (vector + keyword,
RRF reciprocal rank fusion), reranking, memory-mapped indexes ★, disk-based
ANN ★ (DiskANN's SSD-resident graph — the low-RAM thesis in production),
segment/shard, quantization-aware training.

## 5. Full-text search

**Systems**: Lucene ★ (the ancestor of everything), Elasticsearch,
OpenSearch, Solr, Tantivy, Meilisearch, Typesense, Quickwit, Sonic, Bleve,
Xapian, Manticore, RediSearch, ParadeDB, lnx.

**Index structures**: inverted index ★, posting list ★, term dictionary,
FST (finite state transducer) ★, skip list, block-max WAND, doc values
(columnar), stored fields, term frequencies/positions/offsets, n-gram /
edge-gram indexes.

**Concepts**: tokenization, analyzer/normalizer, stemming, BM25 ★, TF-IDF,
relevance scoring, segment ★ (immutable — same idea as graph snapshots ★),
segment merge / merge policy ★, refresh vs flush vs commit, near-real-time
search, faceting/aggregations, typo tolerance (Levenshtein automata),
sharding and replicas, translog (ES's WAL).

## 6. Storage engines (underneath everything)

**Systems**: RocksDB ★, LevelDB, LMDB ★, WiredTiger, sled, redb, fjall,
Pebble, Badger, FoundationDB, TiKV, heed, SQLite, bbolt, Speedb, FASTER,
Turso (SQLite rewritten in Rust — a known-endpoint rewrite ★), DuckDB,
Redis/Valkey/KeyDB/Dragonfly.

**Structures**: LSM tree ★ (memtable, SSTable, compaction — leveled vs
tiered, write amplification, bloom filters, block cache), B-tree / B+tree ★
(page splits, copy-on-write B-trees — LMDB), skiplist, ART (adaptive radix
tree), hash index (FASTER), append-only log, WAL ★, group commit, fsync
discipline ★, mmap vs buffered I/O ★ (the LMDB-vs-RocksDB religious war;
this repo is on team mmap), direct I/O, zero-copy reads ★, page cache,
checksums, snapshot isolation ★, MVCC ★, copy-on-write, crash recovery /
redo-undo, ARIES.

## 7. Benchmarks, testing, verification ★

LDBC (SNB Interactive/BI, Graphalytics), GAPBS, openCypher TCK ★, Neo4j
testkit ★, differential testing ★ (this repo's core method: stock Neo4j/GDS
as executable oracle), parity verification ★, canonicalization (component
relabeling for WCC ★), tolerance bands (float algorithms), property-based
testing, fuzzing, failpoint injection, ANN-benchmarks, BEIR/MS MARCO (IR),
recall/latency Pareto frontier, RSS measurement ★, cgroup memory limits ★.

## 8. This repo's own vocabulary ★

GRAIN snapshot, dual-CSR, dense u32 IDs, property sidecars, projection
build store, projection wall, scratch plane, budget reservation, atomic
publication, generation catalog, execution receipt, proof-carrying OLAP,
read-shape architecture, OLTP/OLAP separation, visibility tiers
(journal/overlay/base), cold-open path, weak-model contract, truth graph,
verification spine, known-endpoint convergence (PRD06).

## GitHub search topics that actually work

`graph-database`, `graph-algorithms`, `graph-processing`, `graph-analytics`,
`vector-database`, `vector-search`, `approximate-nearest-neighbor-search`,
`similarity-search`, `search-engine`, `full-text-search`,
`information-retrieval`, `key-value-store`, `lsm-tree`, `embedded-database`,
`storage-engine`. Caveat from Phase A: these return ~95% noise (RAG apps,
note tools, awesome-lists); always verify liveness + engine-substance via
the API before trusting a hit.

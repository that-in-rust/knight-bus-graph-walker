# Pattern Index

Running index of all published pattern pairs (REQ-GLC-043.1: updated in the
same commit as any pair). Kind ∈ {algorithm, storage, execution}.

| # | Pattern | Kind | Repos cited | Pair | Date |
| --- | --- | --- | --- | --- | --- |
| 1 | LSM compaction tradeoff (leveled vs tiered) | storage | rocksdb, pebble, mini-lsm, fjall/lsm-tree, slatedb | lsm-compaction-tradeoff-{ascii,mermaid}.md | 2026-07-08 |
| 2 | WAL group commit (fsync amortization) | storage | rocksdb, pebble, sqlite, slatedb, turso | wal-group-commit-{ascii,mermaid}.md | 2026-07-08 |
| 3 | Roaring bitmap ID sets (per-chunk containers) | storage | CRoaring, RoaringBitmap(Java), roaring-rs | roaring-bitmap-idsets-{ascii,mermaid}.md | 2026-07-08 |
| 4 | MVCC snapshot visibility | storage | toydb, badger, rocksdb, tikv, memgraph | mvcc-snapshot-visibility-{ascii,mermaid}.md | 2026-07-08 |
| 5 | COW tree snapshot (path copy + root flip) | storage | lmdb, bbolt, redb, sled | cow-tree-snapshot-{ascii,mermaid}.md | 2026-07-08 |
| 6 | Bloom filter read shortcut | storage | mini-lsm, badger, rocksdb, fjall/lsm-tree | bloom-filter-shortcut-{ascii,mermaid}.md | 2026-07-08 |
| 7 | CSR adjacency layout | storage | gapbs, gunrock, ligra, kuzu, webgraph-rs, parlaylib | csr-adjacency-layout-{ascii,mermaid}.md | 2026-07-08 |
| 8 | Frontier push/pull switching | execution | gapbs, ligra, graphit, gunrock, gbbs | frontier-pushpull-switching-{ascii,mermaid}.md | 2026-07-08 |
| 9 | Semiring matrix traversal | algorithm | LAGraph, GraphBLAS, falkordb | semiring-matrix-traversal-{ascii,mermaid}.md | 2026-07-08 |
| 10 | Component hooking + shortcutting | algorithm | gapbs, LAGraph, gbbs, ligra | component-hooking-shortcutting-{ascii,mermaid}.md | 2026-07-08 |
| 11 | PageRank iteration + convergence | algorithm | gapbs, LAGraph, networkit, graphit | pagerank-iteration-convergence-{ascii,mermaid}.md | 2026-07-08 |
| 12 | Delta-stepping buckets (parallel SSSP) | algorithm | gapbs, gbbs, LAGraph, graphit | delta-stepping-buckets-{ascii,mermaid}.md | 2026-07-08 |
| 13 | HNSW layered greedy search | algorithm | hnswlib, qdrant, faiss, knowhere | hnsw-layered-greedy-{ascii,mermaid}.md | 2026-07-08 |
| 14 | Product quantization ladder | storage | faiss, qdrant, pgvectorscale | product-quantization-ladder-{ascii,mermaid}.md | 2026-07-08 |
| 15 | DiskANN Vamana disk layout | storage | DiskANN, pgvectorscale, knowhere | diskann-vamana-layout-{ascii,mermaid}.md | 2026-07-08 |
| 16 | IVF partitioned probe | execution | faiss, knowhere, cuvs | ivf-partitioned-probe-{ascii,mermaid}.md | 2026-07-08 |
| S3 | Vector ANN category synthesis | execution | hnswlib, qdrant, faiss, DiskANN, pgvectorscale, knowhere, cuvs | vector-ann-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |
| 17 | Posting block compression | storage | lucene, tantivy, quickwit | posting-block-compression-{ascii,mermaid}.md | 2026-07-08 |
| 18 | BM25 + WAND pruning | algorithm | tantivy, lucene, quickwit | bm25-wand-pruning-{ascii,mermaid}.md | 2026-07-08 |
| 19 | FST term dictionary | storage | lucene, tantivy | fst-term-dictionary-{ascii,mermaid}.md | 2026-07-08 |
| S4 | Full-text search category synthesis | execution | lucene, tantivy, quickwit, elasticsearch, OpenSearch, paradedb | full-text-search-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |
| 20 | Record chain adjacency | storage | neo4j, kuzu | record-chain-adjacency-{ascii,mermaid}.md | 2026-07-08 |
| 21 | Pull operator pipeline | execution | memgraph, kuzu, neo4j | pull-operator-pipeline-{ascii,mermaid}.md | 2026-07-08 |
| 22 | Triple permutation indexing | storage | oxigraph, qlever | triple-permutation-indexing-{ascii,mermaid}.md | 2026-07-08 |
| S5 | Graph DB category synthesis | execution | neo4j, kuzu, memgraph, janusgraph, oxigraph, qlever | graph-db-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |
| 23 | PackStream wire encoding | storage | neo4rs, neo4j-python-driver | packstream-wire-encoding-{ascii,mermaid}.md | 2026-07-08 |
| 24 | Stub script conformance | execution | testkit, neo4j-python-driver, neo4j-go-driver | stub-script-conformance-{ascii,mermaid}.md | 2026-07-08 |
| S6 | Neo4j ecosystem category synthesis | execution | neo4rs, neo4j-python-driver, testkit, neo4j-go-driver | neo4j-ecosystem-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |
| 25 | Incremental delta iteration | execution | timely-dataflow, differential-dataflow, datafrog | incremental-delta-iteration-{ascii,mermaid}.md | 2026-07-08 |
| 26 | Superstep message convergence | execution | spark, giraph | superstep-message-convergence-{ascii,mermaid}.md | 2026-07-08 |
| S7 | Dataflow compute category synthesis | execution | timely-dataflow, differential-dataflow, datafrog, spark, giraph | dataflow-compute-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |

## Category syntheses

| Category | Synthesis pair | Date |
| --- | --- | --- |
| storage-engine | storage-engine-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |
| graph-analytics | graph-analytics-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |

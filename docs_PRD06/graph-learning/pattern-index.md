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

## Category syntheses

| Category | Synthesis pair | Date |
| --- | --- | --- |
| storage-engine | storage-engine-pattern-synthesis-{ascii,mermaid}.md | 2026-07-08 |

# CSR Adjacency Layout — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `csr-adjacency-layout-ascii.md` / `csr-adjacency-layout-mermaid.md` |
| One-line job | Store a graph's adjacency as two flat arrays — offsets and neighbors — so that "give me v's neighbors" is two array reads and a contiguous scan, the fastest thing hardware can do |

## 1. The job

Every graph analytics engine's inner loop is the same: for vertex v,
iterate its neighbors. Pointer-based adjacency (linked lists, per-node
vectors) pays a cache miss per hop. CSR (Compressed Sparse Row) packs
ALL adjacency into two arrays so the neighbors of any vertex are one
contiguous slice:

```text
offsets:   [0, 3, 5, 5, 8, 9]        length = |V| + 1
neighbors: [1, 3, 4, 0, 2, 0, 1, 4, 3]   length = |E|

neighbors(v) = neighbors[ offsets[v] .. offsets[v+1] ]
degree(v)    = offsets[v+1] - offsets[v]

v=0: neighbors[0..3] = {1,3,4}    v=2: neighbors[5..5] = {} (deg 0)
v=3: neighbors[5..8] = {0,1,4}
```

Two loads and a linear scan. No pointers, no per-node allocation, and
the whole structure is position-addressed — which is why it mmaps.

## 2. Raw data shape across the witnesses

The same two arrays under different names:

```text
gapbs    CSRGraph: out_index_ / out_neighbors_
         (gapbs-src/src/graph.h:99,135-142; num_edges recovered
          as (out_index_[n]-out_index_[0]) — offsets ARE pointers
          into the neighbor array in this variant)
gunrock  row_offsets / column_indices, GPU-resident
         (gunrock-src/include/gunrock/graph/csr.hxx:182-186,223-224)
ligra    packed vertex arrays + optional byte/byteRLE compressed
         neighbor lists (ligra-src/ligra/byte.h, byteRLE.h) —
         CSR plus delta compression per list
kuzu     CSR *inside a disk-backed table*: csr_node_group /
         csr_chunked_node_group — CSR chunked into node groups so
         it can be updated and paged
         (kuzu-src/src/include/storage/table/csr_node_group.h)
```

Directed graphs store two CSRs (out + in, i.e. CSR + CSC); weighted
graphs either widen neighbors to (target, weight) pairs or keep a
parallel weights array of length |E|.

## 3. Step-by-step: building CSR from an edge list

```text
input: E = [(0,1),(0,3),(0,4),(1,0),(1,2),(3,0),(3,1),(3,4),(4,3)]

1. count pass:   deg = [3,2,0,3,1]
2. prefix sum:   offsets = [0,3,5,5,8,9]      (exclusive scan)
3. scatter pass: cursor = copy of offsets;
                 for (u,v) in E: neighbors[cursor[u]++] = v
4. (optional) sort each slice -> binary-searchable adjacency,
   required for O(log d) edge-existence queries

cost: two linear passes + a scan — trivially parallel (parlaylib's
scan primitives exist for exactly this; gapbs builder does the
count/scan/scatter dance in parallel).
```

The prefix sum is the load-bearing trick: it converts per-vertex
counts into positions, turning "where does v's list start?" from a
data structure question into arithmetic.

## 4. Why analytics engines refuse anything else

```text
BFS inner loop over CSR:
    for u in frontier:
        for v in neighbors[offsets[u]..offsets[u+1]]:   <- streaming
            if !visited[v]: next.push(v)

memory behavior: offsets[u], offsets[u+1] — 1 cache line;
neighbor slice — sequential prefetchable reads at ~memory bandwidth.
A pointer-chasing adjacency does 1 random miss (~100ns) PER EDGE;
CSR amortizes to ~1 miss per cache line = per 16 edges (u32 ids).
=> the ~10-16x gap between "graph library" and "analytics engine"
   is mostly this layout, before any algorithmic cleverness.
```

GPU engines are even stricter: gunrock's load-balanced kernels assign
threads to slices of `column_indices` — only possible because the
slice boundaries are plain integers (`csr.hxx` merge-path search over
`row_offsets`).

## 5. Worked example 1 — memory arithmetic at a billion edges

Social graph: |V| = 100M, |E| = 1B (directed), u64 offsets, u32 ids:

```text
offsets:    (100M+1) x 8 B  =   0.8 GB
neighbors:  1B x 4 B        =   4.0 GB
out+in CSR: 2 x 4.8         =   9.6 GB     <- fits an mmap budget
adjacency-list equivalent:  1B x (8B ptr + 4B id + alloc overhead
                            ~16B) + 100M vec headers ~ 30+ GB
                            AND random-placed -> unprefetchable
```

Add ligra-style byte-RLE delta compression on sorted lists (deltas
between consecutive neighbor ids varint-encoded): social-graph lists
compress ~2.5-4x, bringing neighbors to ~1.2 GB — the technique
webgraph-rs industrializes for web-scale graphs.

## 6. Worked example 2 — the price: updates

CSR's weakness is symmetrical to its strength: inserting one edge
(u,v) means shifting EVERYTHING after u's slice.

```text
naive insert into 1B-edge CSR: memmove of ~2 GB on average. Absurd.

engine answers, all in the corpus:
kuzu:    chunk CSR into node groups (~100k vertices each) with slack;
         an insert rewrites one chunk, not the file
         (csr_chunked_node_group.cpp) — COW at chunk granularity,
         exactly pattern 5's move
ligra+:  (graphbolt) keep CSR immutable + a delta log; rebuild
         merged snapshots periodically — the LSM move (pattern 1)
gapbs:   doesn't: analytics engines rebuild the whole CSR per run,
         because build cost (2 passes) is minutes even at 1B edges
```

The storage-engine synthesis predicted this: immutable artifact +
garbage-collected rebuild is the recurring answer to mutation.

## 7. Where graph systems inherit this

- Every analytics engine in the corpus (gapbs, ligra, gunrock, gbbs,
  networkit) is CSR-first; GraphBLAS generalizes CSR to sparse
  matrices where adjacency IS the matrix and BFS is SpMV.
- Kuzu shows CSR surviving contact with a transactional disk store —
  the chunked variant is the bridge between this category and the
  storage-engine category.
- This repo's mmap-walk design is CSR + the storage patterns: build
  immutable CSR segments (pattern 5 flip), compress lists
  (ligra-style), bloom-guard external IDs (pattern 6). Offsets being
  plain integers is what makes the file directly walkable.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/graph.h` | reference CSRGraph (lines 99-142), parallel builder |
| gunrock | `reference-repos-corpus/gunrock-src/include/gunrock/graph/csr.hxx` | GPU CSR: row_offsets/column_indices (182-186) |
| ligra | `reference-repos-competitors/ligra-src/ligra/byte.h` | byte-coded delta-compressed neighbor lists |
| ligra | `reference-repos-competitors/ligra-src/ligra/byteRLE.h` | RLE variant of list compression |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/csr_node_group.h` | disk-backed, chunked, updatable CSR |
| webgraph-rs | `reference-repos-corpus/webgraph-rs-src` | industrial compressed-CSR for web graphs |
| parlaylib | `reference-repos-corpus/parlaylib-src` | the parallel scan/scatter primitives CSR builds on |

## 9. Cross-references

- Sibling patterns: `cow-tree-snapshot` (kuzu's chunk rewrite is the
  same move); `lsm-compaction-tradeoff` (graphbolt's delta-log is the
  LSM move); `roaring-bitmap-idsets` (the other flat set layout —
  bitmaps for membership, CSR for iteration).
- Next pairs in this category: frontier push/pull switching (the
  algorithm that exploits CSR+CSC both being present), and
  rank/select succinct offsets (sdsl-lite deferred from storage).
- 202606 digest overlap: the prior digests named CSR as "the layout";
  this pair adds the build algorithm, the update workarounds, and
  cross-corpus numbers.

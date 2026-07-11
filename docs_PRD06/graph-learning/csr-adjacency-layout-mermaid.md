# CSR Adjacency Layout — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `csr-adjacency-layout-ascii.md` / `csr-adjacency-layout-mermaid.md` |
| One-line job | Store a graph's adjacency as two flat arrays — offsets and neighbors — so that "give me v's neighbors" is two array reads and a contiguous scan |

## 1. The two arrays

```mermaid
flowchart TD
    OFF["offsets (len |V|+1):<br/>[0, 3, 5, 5, 8, 9]"]
    NBR["neighbors (len |E|):<br/>[1, 3, 4, 0, 2, 0, 1, 4, 3]"]
    OFF -- "neighbors(v) = neighbors[offsets(v) .. offsets(v+1)]<br/>degree(v) = offsets(v+1) - offsets(v)" --> NBR
    NBR --> EX["v=0 -> {1,3,4}; v=2 -> {} (deg 0); v=3 -> {0,1,4}"]
```

Two loads and a linear scan; no pointers, no per-node allocation;
position-addressed — which is why it mmaps.

## 2. The same arrays across the witnesses

```mermaid
flowchart LR
    CSR[CSR] --> GA["gapbs: out_index_ / out_neighbors_<br/>(src/graph.h:99,135-142 —<br/>offsets ARE pointers in this variant)"]
    CSR --> GU["gunrock: row_offsets / column_indices,<br/>GPU-resident (graph/csr.hxx:182-186)"]
    CSR --> LI["ligra: CSR + byte / byteRLE<br/>delta-compressed neighbor lists"]
    CSR --> KZ["kuzu: csr_node_group — CSR chunked<br/>into node groups, disk-backed and updatable"]
```

Directed graphs keep two CSRs (out + in = CSR + CSC); weights ride in
a parallel |E|-length array or widen entries to (target, weight).

## 3. Building CSR from an edge list

```mermaid
flowchart TD
    E["edge list E"] --> C["1. count pass: deg[u]++ per edge<br/>deg = [3,2,0,3,1]"]
    C --> P["2. exclusive prefix sum:<br/>offsets = [0,3,5,5,8,9]"]
    P --> S["3. scatter: cursor = offsets copy;<br/>neighbors[cursor[u]++] = v"]
    S --> O["4. optional: sort each slice -><br/>O(log d) edge-existence queries"]
    P -.-> KEY["the load-bearing trick: prefix sum turns<br/>counts into positions — 'where does v start?'<br/>becomes arithmetic, not a data structure"]
```

Two linear passes + a scan — trivially parallel (parlaylib's scan
primitives; gapbs's builder does count/scan/scatter in parallel).

## 4. Why analytics engines refuse anything else

```mermaid
flowchart LR
    subgraph PTR [pointer adjacency]
        P1["1 random cache miss (~100ns)<br/>PER EDGE"]
    end
    subgraph CSRL [CSR]
        C1["offsets: 1 cache line;<br/>neighbor slice: sequential,<br/>prefetched at memory bandwidth"] --> C2["~1 miss per cache line<br/>= per 16 edges (u32 ids)"]
    end
    PTR --> GAP["the ~10-16x gap between 'graph library'<br/>and 'analytics engine' is mostly<br/>this layout"]
    CSRL --> GAP
```

GPU engines are stricter still: gunrock's load-balanced kernels
assign threads to slices of `column_indices` via merge-path search
over `row_offsets` — only possible because slice boundaries are plain
integers.

## 5. Worked example — memory arithmetic at a billion edges

|V| = 100M, |E| = 1B directed, u64 offsets, u32 ids:

```mermaid
flowchart TD
    A["offsets: 100M x 8B = 0.8 GB"] --> T["out CSR = 4.8 GB<br/>out+in = 9.6 GB — fits an mmap budget"]
    B["neighbors: 1B x 4B = 4.0 GB"] --> T
    T --> VS["adjacency-list equivalent: ~30+ GB,<br/>randomly placed, unprefetchable"]
    T --> CMP["+ ligra-style byteRLE deltas on sorted lists:<br/>social graphs compress 2.5-4x -><br/>neighbors ~1.2 GB (webgraph-rs industrializes this)"]
```

## 6. Worked example — the price: updates

Insert one edge into a 1B-edge CSR = memmove ~2 GB on average. The
corpus answers:

```mermaid
flowchart TD
    U["edge insert"] --> K["kuzu: chunked node groups (~100k vertices)<br/>with slack — rewrite ONE chunk<br/>(csr_chunked_node_group.cpp)<br/>= COW at chunk granularity (pattern 5's move)"]
    U --> G["graphbolt: immutable CSR + delta log,<br/>periodic merged rebuild = the LSM move (pattern 1)"]
    U --> R["gapbs: don't — rebuild whole CSR per run;<br/>2 passes is minutes even at 1B edges"]
    K & G & R --> TH["storage-engine synthesis predicted it:<br/>immutable artifact + GC'd rebuild is<br/>THE recurring answer to mutation"]
```

## 7. Inheritance map

```mermaid
flowchart LR
    CSR2[CSR] --> AN["gapbs, ligra, gunrock, gbbs, networkit:<br/>CSR-first engines"]
    CSR2 --> GB["GraphBLAS: CSR generalized to sparse<br/>matrices — adjacency IS the matrix,<br/>BFS is SpMV"]
    CSR2 --> KZ2["kuzu: CSR surviving a transactional<br/>disk store — bridge to storage-engine category"]
    CSR2 --> ME["this repo: immutable CSR segments (p5 flip)<br/>+ compressed lists + bloom-guarded IDs (p6) —<br/>integer offsets make the file directly walkable"]
```

## 8. CSR vs CSC vs COO — the three sparse siblings

```mermaid
flowchart TD
    COO["COO: raw (src, dst) pairs —<br/>the ingest/interchange format,<br/>|E| x 8B, no structure"]
    CSRX["CSR: grouped by source —<br/>out-neighbors fast (push, BFS out)"]
    CSC["CSC: grouped by destination —<br/>in-neighbors fast (pull, PageRank gather)"]
    COO -- "count + prefix sum + scatter" --> CSRX
    COO -- "same, keyed by dst" --> CSC
    CSRX <-. "transpose = rebuild<br/>(gunrock ships coo/csr/csc<br/>converters: graph/conversions)" .-> CSC
```

Engines that switch traversal direction at runtime (next pattern:
push/pull) must hold BOTH — doubling memory but unlocking the
direction-optimized BFS that made ligra famous. GraphBLAS hides the
choice behind the matrix abstraction and transposes lazily.

```mermaid
sequenceDiagram
    participant L as loader
    participant C as COO buffer
    participant B as builder
    L->>C: stream edges (src, dst) as ingested
    C->>B: count pass -> degrees
    B->>B: exclusive prefix sum -> offsets
    C->>B: scatter pass -> neighbors filled
    B-->>L: CSR ready (+ CSC if direction switching planned)
    Note over B: total: two linear passes over |E| —<br/>minutes even at 1B edges, which is why<br/>gapbs simply rebuilds per run
```

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/graph.h` | reference CSRGraph (99-142), parallel builder |
| gunrock | `reference-repos-corpus/gunrock-src/include/gunrock/graph/csr.hxx` | GPU CSR (182-186) |
| ligra | `reference-repos-competitors/ligra-src/ligra/byte.h` | byte-coded delta-compressed lists |
| ligra | `reference-repos-competitors/ligra-src/ligra/byteRLE.h` | RLE list compression |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/csr_node_group.h` | disk-backed chunked updatable CSR |
| webgraph-rs | `reference-repos-corpus/webgraph-rs-src` | industrial compressed CSR for web graphs |
| parlaylib | `reference-repos-corpus/parlaylib-src` | parallel scan/scatter primitives |

## 10. Cross-references

- Sibling patterns: `cow-tree-snapshot` (kuzu's chunk rewrite);
  `lsm-compaction-tradeoff` (graphbolt's delta log);
  `roaring-bitmap-idsets` (bitmaps for membership, CSR for iteration).
- Next in category: frontier push/pull switching (exploits having
  both CSR and CSC); succinct rank/select offsets (sdsl-lite).
- 202606 digest overlap: digests named CSR as "the layout"; this pair
  adds build algorithm, update workarounds, cross-corpus numbers.

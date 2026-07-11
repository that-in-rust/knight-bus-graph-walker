# HNSW Layered Greedy — Mermaid

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `hnsw-layered-greedy-ascii.md` / `hnsw-layered-greedy-mermaid.md` |
| One-line job | Approximate nearest-neighbor search as a greedy walk over a hierarchy of proximity graphs: coarse upper layers teleport near the target, layer 0 runs a beam search — recall is bought with a queue-width knob (ef) |

## 1. The hierarchy

```mermaid
flowchart TD
    L2["layer 2: ~N/M² nodes, LONG edges (highways)"]
    L1["layer 1: ~N/M nodes, medium edges"]
    L0["layer 0: ALL N nodes, short edges (local streets)"]
    L2 -->|"greedy descent, beam 1"| L1
    L1 -->|"greedy descent, beam 1"| L0
    L0 -->|"beam search, width ef"| K["top k results"]
```

```mermaid
flowchart LR
    LV["node level ~ floor(-ln(U) x 1/ln(M))"] --> GEO["geometric distribution:<br/>layer i holds ~N/M^i nodes"]
    GEO --> W["hnswlib hnswalg.h:142,207-211 and qdrant<br/>graph_layers_builder.rs:317,385 —<br/>the IDENTICAL formula, independently"]
```

## 2. Search, end to end

```mermaid
sequenceDiagram
    participant Q as query
    participant U as upper layers
    participant B as layer-0 beam
    Q->>U: start at global entry point (hnswalg.h:45)
    loop each layer maxlevel..1
        U->>U: move to any closer neighbor;<br/>stop at local minimum (beam width 1 —<br/>qdrant search_entry_on_level,<br/>graph_layers.rs:15-17)
    end
    U->>B: descend with best-found entry
    loop until closest unexpanded candidate ><br/>worst kept result (hnswalg.h:248)
        B->>B: pop nearest candidate (min-heap),<br/>expand links, push improvements<br/>into results (max-heap, capped ef)
    end
    B-->>Q: top k of the ef kept
```

The termination rule is the economy: prune exactly when no frontier
node can provably improve the result set.

## 3. The diversity heuristic — why not just kNN edges

```mermaid
flowchart TD
    KNN["naive: connect each node to its<br/>M nearest neighbors"] --> CL["creates tight clusters greedy<br/>search cannot escape"]
    CL --> H["heuristic (hnswalg.h:443-475; qdrant's<br/>'not closer than base',<br/>graph_layers_builder.rs:41-42):<br/>accept c only if dist(c, you) <<br/>dist(c, every accepted neighbor)"]
    H --> RNG["edges forced into DIVERSE directions<br/>(relative-neighborhood-graph flavor):<br/>this loop is what makes the graph<br/>NAVIGABLE — both witnesses implement<br/>the same rule independently"]
```

## 4. Memory arithmetic at 100M vectors (768-d f32, M=16)

```mermaid
flowchart TD
    V["vectors: 100M x 768 x 4B = 307 GB"]
    G0["layer-0 links: 100M x 32 x 4B = 12.8 GB"]
    GU["upper layers: ~0.9 GB"]
    V & G0 & GU --> L["graph overhead ~4.5% of vector bytes:<br/>the GRAPH is cheap, the VECTORS dominate"]
    L --> SO["hence: quantization (PQ/SQ) and disk<br/>placement (DiskANN) matter more than graph<br/>compression; qdrant keeps links in RAM,<br/>lets vectors go to mmap/disk"]
```

## 5. The ef dial (k=10, 100M vectors)

```mermaid
flowchart LR
    E1["ef=10: ~300-600 dist evals,<br/>recall ~0.6-0.8"] --> E2["ef=100: ~2-5k evals,<br/>recall ~0.95-0.99"]
    E2 --> E3["ef=500: ~15-25k evals,<br/>recall ~0.999"]
    E3 --> INV["invariant: recall rises with ef at ~log cost,<br/>latency linearly — the ann-benchmarks<br/>Pareto curve; still ~4000x fewer evals<br/>than the linear scan"]
```

## 6. Inheritance map

```mermaid
flowchart LR
    H[HNSW] --> HL["hnswlib: the reference"]
    H --> FS["faiss HNSW.cpp: one index type<br/>in the IVF/PQ toolbox"]
    H --> QD["qdrant: per-segment HNSW +<br/>graph_layers_healer.rs (link repair under<br/>deletes — mutability strikes again) +<br/>ACORN for filtered search"]
    H --> ALL["Milvus/knowhere, Weaviate, pgvector,<br/>Lucene/ES/Vespa: default graph index —<br/>the corpus's most reimplemented algorithm"]
    H --> N4["Neo4j: vector indexes via Lucene HNSW —<br/>graph DB and vector search converge"]
```

## 7. Kinship with the analytics category

```mermaid
flowchart TD
    K1["pattern 8 kinship: HNSW search IS a<br/>frontier walk — with a distance-ordered<br/>priority queue instead of FIFO"]
    K2["pattern 12 kinship: both relax strict<br/>priority order into a bounded-slack<br/>structure (delta buckets / ef beam)"]
    K3["pattern 7 kinship: per-layer links are<br/>CSR-shaped arrays; qdrant's graph_links.rs<br/>is a compressed links format"]
    K1 --> META["the analytics meta-move again:<br/>exactness traded for speed,<br/>bounded by a knob (ef),<br/>guarded by the lowerBound check"]
    K2 --> META
    K3 --> META
```

## 8. The verification angle

```mermaid
flowchart TD
    AP["ANN is approximate BY CONTRACT:<br/>the endpoint is a recall distribution,<br/>not a result set"] --> EQ["equivalence = recall@k over a fixed<br/>query set, at pinned ef, seeds,<br/>and build parameters"]
    EQ --> RND["build is randomized (level draws) —<br/>two builds of the SAME library differ;<br/>per-query equality is the wrong oracle"]
    RND --> TH["docs_PRD06 thesis condition 3 at its<br/>most extreme: the cloud IS the spec —<br/>pin the distribution, not the points"]
```

## 8b. Insertion — building while searching

```mermaid
sequenceDiagram
    participant N as new vector
    participant U as upper layers
    participant L as target layers
    N->>N: draw level L ~ geometric (mult = 1/ln M)
    N->>U: greedy descent from entry point to layer L
    loop each layer L..0
        N->>L: beam search width ef_construction (~200)
        L->>L: shrink candidates to M via the<br/>diversity heuristic (hnswalg.h:513)
        L->>L: connect BOTH directions;<br/>if a neighbor now exceeds Mcurmax,<br/>re-run the heuristic on ITS list<br/>(hnswalg.h:603)
    end
    Note over N,U: if L > maxlevel, the new node becomes<br/>the global entry point (hnswalg.h:135-136) —<br/>insertion IS a search plus bidirectional<br/>pruned wiring; build and query share<br/>the same kernel
```

The bidirectional-connect-then-prune step is why concurrent inserts
need per-node locks (hnswlib) or why qdrant builds per-segment
immutable graphs and heals links on merges instead — the storage
category's mutation dilemma, replayed in the index layer.

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| hnswlib | `reference-repos-corpus/hnswlib-src/hnswlib/hnswalg.h` | level draw (142, 207-211), beam search (226-311), diversity heuristic (443-475), knobs (31-32, 93-115) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/segment/src/index/hnsw_index/graph_layers.rs` | search-on-level family, beam-1 upper layers (7-23) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/segment/src/index/hnsw_index/graph_layers_builder.rs` | level_factor (317, 385), m/m0 (239, 305), heuristic flag (41-42) |
| faiss | `reference-repos-corpus/faiss-src/faiss/impl/HNSW.cpp` | independent third implementation |
| milvus/knowhere | `reference-repos-corpus/knowhere-src` | HNSW among FAISS/DiskANN backends |

## 10. Cross-references

- Sibling patterns: `frontier-pushpull-switching`,
  `csr-adjacency-layout`, `delta-stepping-buckets` (see §7).
- Next in category: quantization ladders (PQ/SQ/binary), then
  DiskANN's on-disk Vamana graph as the counter-design.
- Paper trail: Malkov & Yashunin's HNSW paper (verified in
  `research-papers-ledger.md`), the NSW predecessor, and the
  ACORN filtered-search paper qdrant implements.
- 202606 digest overlap: digests named HNSW as the standard vector
  index; this pair adds level math, beam mechanics, the diversity
  heuristic with dual-witness cites, and memory/ef arithmetic.

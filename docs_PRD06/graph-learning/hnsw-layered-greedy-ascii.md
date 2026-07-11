# HNSW Layered Greedy — ASCII

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `hnsw-layered-greedy-ascii.md` / `hnsw-layered-greedy-mermaid.md` |
| One-line job | Approximate nearest-neighbor search as a greedy walk over a hierarchy of proximity graphs: coarse upper layers teleport you near the target, layer 0 does a beam search — recall is bought with a queue-width knob (ef) |

## 1. The job

Exact nearest-neighbor in high dimensions degenerates to a linear
scan (the curse of dimensionality kills trees). HNSW (Hierarchical
Navigable Small World) answers "give me the ~k nearest vectors,
probably" in sub-millisecond time by building a graph you can
greedily descend:

```text
layer 2:   o----------o              few nodes, LONG edges
             \                       (highways)
layer 1:   o--o----o---o             more nodes, medium edges
                \
layer 0:   o-o-o-o-o-o-o-o-o         ALL nodes, short edges
                                     (local streets)

search: start at the top entry point; greedy-descend each layer
        (beam width 1) until layer 0; there, widen the beam to ef
        and collect the best k.
```

It is pattern 8's frontier walk with a distance-ordered priority
queue instead of a FIFO — graph traversal machinery applied to
geometry.

## 2. Raw data shape

```text
per node: level  ~ floor(-ln(U(0,1)) * mult), mult = 1/ln(M)
          (hnswlib hnswalg.h:142,207-211; qdrant
           graph_layers_builder.rs:317,385 — identical formula,
           level_factor = 1/ln(M))
          -> geometric distribution: layer i has ~N/M^i nodes

per node per layer: adjacency list, capped at M (upper layers)
          and M0 = 2M (layer 0) — qdrant stores m and m0
          separately (graph_layers_builder.rs:239,305)

knobs:    M                fanout: memory & graph quality (~16)
          ef_construction  build-time beam width (~200,
                           hnswalg.h:31,93-115)
          ef               query-time beam width — THE
                           recall/latency dial (hnswalg.h:32,174)
```

The whole index is CSR-shaped adjacency per layer plus the raw
vectors; qdrant's `graph_links.rs` is literally a compressed
links-array format.

## 3. Step-by-step search

```text
1. ep = global entry point (highest-level node; hnswalg.h:45,135)
2. for layer L = maxlevel .. 1:            # beam width 1
     repeat: move ep to any neighbor closer to query
     until no neighbor improves            # greedy local minimum
     (qdrant calls this search_entry_on_level: "beam size of 1,
      used on all levels above 0" — graph_layers.rs:15-17)
3. at layer 0: beam search with width ef   # searchBaseLayer(ST)
     candidates  = min-heap by distance (to expand)
     results     = max-heap of best ef found (to keep)
     stop when the closest unexpanded candidate is farther than
     the worst kept result (hnswalg.h:248 — the lowerBound check)
4. return top k of results
```

Step 3's termination rule is the pattern's core economy: the beam
prunes exactly when it can prove no frontier node can improve the
result set — same shape as best-first search with an admissible
bound.

## 4. The neighbor-selection heuristic (why it's not just kNN edges)

Connecting each node to its M nearest neighbors creates clusters
that greedy search cannot escape. The fix
(getNeighborsByHeuristic2, hnswalg.h:443-475; qdrant's
"not closer than base" heuristic, graph_layers_builder.rs:41-42):

```text
accept candidate c into the neighbor list only if
    dist(c, query_node) < dist(c, every already-accepted neighbor)
-> neighbors must be closer to YOU than to each other:
   forces edges to point in DIVERSE directions (relative
   neighborhood graph flavor), preserving navigability.
```

This one loop is what makes the graph "navigable small world"
rather than a clumpy kNN graph — and both witnesses implement the
same rule independently.

## 5. Worked example 1 — memory at 100M vectors

768-dim float32, M = 16:

```text
vectors:  100M x 768 x 4 B                    = 307 GB
layer 0:  100M x 32 links x 4 B               = 12.8 GB
layers 1+: sum N/16^i x 16 links x 4 B        ~ 0.9 GB
total graph overhead: ~4.5% of vector bytes.
lesson: the GRAPH is cheap; the VECTORS dominate — which is why
quantization (PQ/SQ) and disk placement (DiskANN) matter more than
graph compression, and why qdrant keeps links in RAM but lets
vectors go to mmap/disk.
```

## 6. Worked example 2 — the ef dial

100M vectors, k = 10:

```text
ef = 10    ~ 300-600 distance evals    recall@10 ~ 0.6-0.8
ef = 100   ~ 2-5k distance evals       recall@10 ~ 0.95-0.99
ef = 500   ~ 15-25k distance evals     recall@10 ~ 0.999
(shape of the ann-benchmarks Pareto curve; exact numbers are
dataset-dependent — the INVARIANT is: recall rises with ef at
roughly log cost, latency rises linearly.)
even ef = 500 is ~4000x fewer distance evals than the 100M linear
scan. That ratio is the entire reason vector DBs exist.
```

## 7. Where systems inherit this

- hnswlib is the reference implementation; faiss wraps its own
  (faiss/impl/HNSW.cpp) as one index type among IVF/PQ hybrids.
- qdrant productionizes it in Rust: per-segment HNSW +
  `graph_layers_healer.rs` for repairing links under deletes —
  mutability strikes again (the storage category's problem, in
  vector clothing); plus ACORN variant for filtered search
  (graph_layers.rs:11-12).
- Milvus/knowhere, Weaviate, pgvector, Lucene/Elasticsearch/Vespa
  all ship HNSW as the default graph index — the corpus's most
  reimplemented single algorithm.
- Neo4j itself ships vector indexes (Lucene HNSW) — graph DB and
  vector search converge in one system.
- This repo: HNSW layers over immutable segments fit the
  storage-category publication discipline directly; the ef dial is
  a per-query knob a differential harness must PIN (recall is
  stochastic — equivalence must be "recall over a query set", not
  per-query equality).

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| hnswlib | `reference-repos-corpus/hnswlib-src/hnswlib/hnswalg.h` | reference: level draw (142, 207-211), beam search (226-311), diversity heuristic (443-475), knobs (31-32, 93-115) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/segment/src/index/hnsw_index/graph_layers.rs` | production search-on-level family, beam-1 upper layers (7-23) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/segment/src/index/hnsw_index/graph_layers_builder.rs` | level_factor = 1/ln(M) (317, 385), m/m0 split (239, 305), heuristic flag (41-42) |
| faiss | `reference-repos-corpus/faiss-src/faiss/impl/HNSW.cpp` | independent third implementation inside the IVF/PQ toolbox |
| milvus/knowhere | `reference-repos-corpus/knowhere-src` | HNSW as one engine among FAISS/DiskANN backends |

## 9. Cross-references

- Sibling patterns: `frontier-pushpull-switching` (HNSW = frontier
  walk + distance-ordered queue); `csr-adjacency-layout` (the links
  arrays); `delta-stepping-buckets` (both replace strict priority
  order with a bounded-slack structure).
- Next in category: quantization ladders (PQ/SQ/binary), then
  DiskANN's on-disk Vamana graph as the counter-design.
- Verification note (docs_PRD06 thesis): ANN is APPROXIMATE by
  contract — the endpoint is a recall distribution, not a result
  set. Differential testing must compare recall@k over a fixed
  query set at pinned ef, seeds, and build parameters.
- 202606 digest overlap: digests named HNSW as the standard vector
  index; this pair adds the level math, the beam mechanics, the
  diversity heuristic with dual-witness line cites, and the
  memory/ef arithmetic.

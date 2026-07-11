# Vector ANN Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `vector-ann-pattern-synthesis-ascii.md` / `vector-ann-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 13-16: vector search as the meeting point of the graph-analytics and storage categories — navigate or partition, compress everywhere, rescore at the end, and accept that the contract itself is probabilistic |

## 1. The category in one paragraph

Approximate nearest-neighbor search has exactly two structural
ideas — NAVIGATE a proximity graph (HNSW in RAM, Vamana on disk)
or PARTITION space into buckets (IVF) — and one universal
amplifier: quantize the vectors and score in the compressed domain
(the PQ/SQ/BQ ladder). Everything else is placement (RAM vs NVMe
vs Postgres pages) and maintenance (wiring vs re-clustering). The
category's defining novelty versus the rest of the corpus is the
CONTRACT: results are correct-with-probability, so every design
exposes a recall/latency dial and every verification strategy must
be distributional.

## 2. The four patterns and their one-line lessons

```text
13 hnsw-layered-greedy         navigate: hierarchy teleports,
                               layer-0 beam (ef) buys recall;
                               diversity pruning keeps the graph
                               navigable
14 product-quantization-ladder compress: learned codebooks, score
                               by table lookup (ADC) or popcount
                               (BQ); never decompress on the hot
                               path; always rescore
15 diskann-vamana-layout       place: vector + links co-located in
                               one 4 KB sector; alpha>1 buys
                               shorter paths = fewer reads; PQ in
                               RAM steers the walk
16 ivf-partitioned-probe       partition: k-means buckets, probe
                               the nprobe nearest; contiguous
                               scans; the text inverted index
                               transplanted into geometry
```

## 3. The category's organizing axes

```text
AXIS 1 — navigate vs partition
  graph (13,15): ~log-hops to the answer; best online latency
                 at high recall; pays for mutation per write
  buckets (16):  scan a fixed fraction; best for GPU/batch and
                 streaming appends; pays for drift per epoch
AXIS 2 — where the exact truth lives
  compressed working set (14) gates access to full-precision
  vectors (RAM -> mmap -> NVMe -> object storage); EVERY modern
  deployment is two-stage: approximate candidates, exact rescore
AXIS 3 — the knob surface
  ef / W / nprobe / alpha / M / nlist / ladder rung — all are
  points on ONE recall/latency/memory Pareto surface; systems
  differ in which knobs are per-query (nprobe, ef) vs per-build
  (M, nlist, alpha, codebooks)
```

## 4. Old patterns, re-priced

The category invents almost nothing structurally; it re-prices
known patterns under a geometric cost model:

```text
from graph-analytics:
  HNSW/Vamana search = pattern 8's frontier walk with a
    distance-ordered queue; ef/W = beam width
  ef/nprobe dials  = the exactness-for-parallelism meta-move,
    per-query edition
from storage:
  two-stage rescoring = pattern 6's bloom-filter gate
  ladder rung choice  = pattern 3's per-shape encoding choice
  sector co-location  = pattern 7's inline neighbors, disk atom
  FreshDiskANN merge  = patterns 1/5: delta + compaction + flip
  IVF re-clustering   = compaction at the clustering level
genuinely new here:
  learned structure (k-means codebooks/centroids ARE the index),
  and the probabilistic correctness contract.
```

## 5. Worked roll-up: one workload, four designs

10M vectors, 768-d f32 (29 GB raw), recall@10 >= 0.95, one box:

```text
flat scan          29 GB RAM   ~50 ms/query    exact — the oracle
HNSW (13)          31 GB RAM   ~0.3 ms         M=16, ef~100
HNSW+SQ (13+14)    9 GB RAM    ~0.4 ms         u8 + rescore
IVF-PQ (16+14)     1.5 GB RAM  ~2 ms           nlist 4k, nprobe 32
                                               + rescore from disk
DiskANN (15+14)    2 GB RAM +  ~1.5 ms         W=8, alpha 1.2
                   40 GB NVMe
same data, same recall target, ~20x memory spread and ~150x
latency spread: the DESIGN is the resource-allocation decision.
```

## 6. What the category exports

```text
to graph-db      vector indexes are now table stakes (Neo4j ships
                 Lucene HNSW; every corpus graph DB has or plans
                 one); per-segment immutable HNSW + healer is the
                 emerging pattern (qdrant)
to full-text     IVF is the inverted index's child; hybrid search
                 (BM25 + vector fusion) is where the two
                 categories are converging — Lucene/ES/Vespa
                 carry both in one segment format
to verification  the strongest lesson in the corpus: when the
                 contract is probabilistic, the ENDPOINT is a
                 distribution. Equivalence = recall@k over a
                 pinned query set at pinned knobs and seeds;
                 flat scan is the ground truth; ann-benchmarks
                 institutionalized exactly this method
```

## 7. Honest gaps (not yet covered by pairs)

```text
- filtered search (metadata predicates during traversal): qdrant's
  ACORN, Milvus's bitmap prefilters — pattern 3's bitmaps meeting
  pattern 13's graphs; strong candidate for a later pair
- learned/OPQ rotations and anisotropic quantization (ScaNN)
- multi-vector/late-interaction retrieval (ColBERT-style)
- GPU CAGRA graphs (cuVS) — graph ANN rebuilt for GPU memory
- distributed sharding of ANN (Milvus segments, Vald agents)
```

## 8. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| hnswlib | `reference-repos-corpus/hnswlib-src/hnswlib/hnswalg.h` | reference HNSW (pattern 13) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/segment/src/index/hnsw_index/` + `lib/quantization/src/` | production HNSW + the full quantization ladder (13, 14) |
| faiss | `reference-repos-corpus/faiss-src/faiss/` | PQ/ADC, IVF family, HNSW — the toolbox (14, 16) |
| DiskANN | `reference-repos-corpus/DiskANN-src/` | sector layout + alpha pruning (15) |
| pgvectorscale | `reference-repos-corpus/pgvectorscale-src/pgvectorscale/src/access_method/` | Vamana+SBQ inside Postgres (15) |
| knowhere | `reference-repos-corpus/knowhere-src` | HNSW/IVF/DiskANN behind one contract (13, 15, 16) |
| cuvs | `reference-repos-corpus/cuvs-src` | GPU IVF + CAGRA (16, gap) |

## 9. Cross-references

- Category syntheses so far: `storage-engine-pattern-synthesis-*`
  (patterns 1-6), `graph-analytics-pattern-synthesis-*` (7-12) —
  this category consumes both, as §4 itemizes.
- Individual pairs: patterns 13-16 in `pattern-index.md`.
- Next category per spec order: full-text-search — the ancestor
  category: inverted indexes, posting-list compression, BM25, and
  the segment architecture that Lucene gave to everyone.

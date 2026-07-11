# Vector ANN Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `vector-ann-pattern-synthesis-ascii.md` / `vector-ann-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 13-16: vector search as the meeting point of the graph-analytics and storage categories — navigate or partition, compress everywhere, rescore at the end, and accept that the contract itself is probabilistic |

## 1. The category in one map

```mermaid
flowchart TD
    ANN["ANN search"] --> NAV["NAVIGATE a proximity graph:<br/>HNSW in RAM (13),<br/>Vamana on disk (15)"]
    ANN --> PART["PARTITION space into buckets:<br/>IVF (16)"]
    NAV & PART --> AMP["universal amplifier: quantize and score<br/>in the compressed domain —<br/>the PQ/SQ/BQ ladder (14)"]
    AMP --> RS["universal closer: exact rescoring<br/>of the candidate pool"]
    RS --> CONTRACT["the category's novelty: correctness is<br/>PROBABILISTIC — every design exposes a<br/>recall/latency dial, every verification<br/>must be distributional"]
```

## 2. The four patterns

```mermaid
flowchart LR
    P13["13 hnsw-layered-greedy:<br/>hierarchy teleports, ef buys recall,<br/>diversity pruning keeps navigability"]
    P14["14 product-quantization-ladder:<br/>learned codebooks, ADC lookups /<br/>BQ popcount, never decompress"]
    P15["15 diskann-vamana-layout:<br/>vector+links in one 4 KB sector,<br/>alpha>1 -> fewer reads, PQ steers"]
    P16["16 ivf-partitioned-probe:<br/>k-means buckets, probe nprobe nearest,<br/>contiguous scans"]
    P13 --> P15
    P14 --> P15 & P16
```

## 3. The organizing axes

```mermaid
flowchart TD
    A1{"AXIS 1: navigate vs partition"}
    A1 -->|graph| G["~log hops, best online latency;<br/>mutation paid per write (wiring)"]
    A1 -->|buckets| B["fixed-fraction scans, best GPU/batch;<br/>mutation paid per epoch (re-cluster)"]
    A2["AXIS 2: compressed working set gates the<br/>full-precision truth (RAM -> NVMe -> object<br/>store); every deployment is two-stage"]
    A3["AXIS 3: ef / W / nprobe / alpha / M / nlist /<br/>rung — one Pareto surface; per-query knobs<br/>(ef, nprobe) vs per-build knobs (M, nlist,<br/>alpha, codebooks)"]
    G & B --> A2 --> A3
```

## 4. Old patterns, re-priced

```mermaid
flowchart LR
    subgraph from graph-analytics
        F8["pattern 8 frontier walk -><br/>beam search with ef/W"]
        FM["exactness-for-parallelism meta-move -><br/>per-query recall dials"]
    end
    subgraph from storage
        F6["pattern 6 bloom gate -><br/>two-stage rescoring"]
        F3["pattern 3 per-shape encoding -><br/>ladder rung choice"]
        F7["pattern 7 inline neighbors -><br/>sector co-location"]
        F15["patterns 1/5 delta+compaction+flip -><br/>FreshDiskANN merge, IVF re-clustering"]
    end
    F8 & FM & F6 & F3 & F7 & F15 --> NEW["genuinely new: LEARNED structure<br/>(codebooks/centroids ARE the index) +<br/>the probabilistic contract"]
```

## 5. One workload, four designs

10M x 768-d f32 (29 GB raw), recall@10 >= 0.95, one box:

```mermaid
flowchart TD
    FL["flat scan: 29 GB RAM, ~50 ms — exact,<br/>the ORACLE"]
    H["HNSW: 31 GB RAM, ~0.3 ms"]
    HS["HNSW+SQ: 9 GB RAM, ~0.4 ms"]
    IP["IVF-PQ: 1.5 GB RAM, ~2 ms + disk rescore"]
    DK["DiskANN: 2 GB RAM + 40 GB NVMe, ~1.5 ms"]
    FL --> H --> HS --> IP
    HS --> DK
    IP & DK --> CON["same data, same recall target:<br/>~20x memory spread, ~150x latency spread —<br/>the DESIGN is the resource allocation"]
```

## 6. What the category exports

```mermaid
flowchart TD
    V[vector-ann] --> GDB["to graph-db: vector indexes are table<br/>stakes (Neo4j ships Lucene HNSW);<br/>per-segment immutable HNSW + healer<br/>is the emerging shape (qdrant)"]
    V --> FTS["to full-text: IVF is the inverted index's<br/>child; hybrid BM25+vector fusion is the<br/>convergence point (Lucene/ES/Vespa carry<br/>both in one segment format)"]
    V --> VER["to verification: when the contract is<br/>probabilistic, the ENDPOINT is a<br/>distribution — recall@k over pinned<br/>queries/knobs/seeds; flat scan is ground<br/>truth (ann-benchmarks method)"]
```

## 7. The verification pipeline, end to end

```mermaid
sequenceDiagram
    participant O as oracle (flat scan)
    participant S as system under test
    participant H as harness
    H->>O: pinned query set -> exact top-k per query
    H->>S: same queries at pinned (ef | nprobe | W),<br/>pinned build params + seeds
    S-->>H: approximate top-k per query
    H->>H: recall@k per query -> DISTRIBUTION,<br/>not a boolean
    H->>H: also collect counters: distance evals,<br/>hops, sectors read (pattern 15 §8)
    Note over H: pass = distribution dominates the<br/>agreed Pareto point; a single bad query<br/>is signal for triage, not failure —<br/>docs_PRD06 thesis condition 3 made concrete:<br/>EQUIVALENCE IS DEFINED, not assumed
```

## 8. Honest gaps

```mermaid
flowchart LR
    GAP["not yet covered by pairs"] --> FS["filtered search: qdrant ACORN, Milvus<br/>bitmap prefilters — pattern 3 meets 13;<br/>strongest candidate for a later pair"]
    GAP --> OPQ["learned rotations / anisotropic<br/>quantization (OPQ, ScaNN)"]
    GAP --> MV["multi-vector late interaction<br/>(ColBERT-style)"]
    GAP --> CAGRA["GPU CAGRA graphs (cuVS)"]
    GAP --> DIST["distributed sharding (Milvus<br/>segments, Vald agents)"]
```

## 8b. Choosing a design — the integrated decision walk

```mermaid
flowchart TD
    START["N vectors, d dims, recall target,<br/>latency budget, box size"]
    START --> Q1{"fits in RAM<br/>uncompressed + links?"}
    Q1 -->|yes| H2["HNSW (13); add SQ u8 (14)<br/>anyway — free 4x headroom"]
    Q1 -->|no| Q2{"compressed codes<br/>fit in RAM?"}
    Q2 -->|"yes, and NVMe available"| D2["DiskANN (15): PQ in RAM steers,<br/>sectors hold the truth"]
    Q2 -->|"batch/GPU workload"| I2["IVF-PQ (16+14): bucket scans<br/>are dense kernels"]
    Q2 -->|"no (extreme scale)"| S2["shard first (Milvus segments),<br/>then re-enter this walk per shard"]
    H2 & D2 & I2 --> T["then TUNE on the pinned query set:<br/>walk ef / W / nprobe until the recall<br/>distribution clears the target —<br/>never trust defaults across datasets"]
```

Every production engine in the corpus is a packaging of this walk:
qdrant/weaviate pin the top branch, Milvus/knowhere expose all
three, pgvector grew from IVF to HNSW as RAM got cheap, and
pgvectorscale exists because Postgres pages made the DiskANN
branch natural. Reading the four pairs equips you to predict each
vendor's next move from their storage substrate alone.

## 9. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| hnswlib | `reference-repos-corpus/hnswlib-src/hnswlib/hnswalg.h` | reference HNSW (13) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/segment/src/index/hnsw_index/` + `lib/quantization/src/` | production HNSW + full ladder (13, 14) |
| faiss | `reference-repos-corpus/faiss-src/faiss/` | PQ/ADC, IVF family, HNSW toolbox (14, 16) |
| DiskANN | `reference-repos-corpus/DiskANN-src/` | sector layout + alpha pruning (15) |
| pgvectorscale | `reference-repos-corpus/pgvectorscale-src/pgvectorscale/src/access_method/` | Vamana+SBQ in Postgres (15) |
| knowhere | `reference-repos-corpus/knowhere-src` | HNSW/IVF/DiskANN behind one contract |
| cuvs | `reference-repos-corpus/cuvs-src` | GPU IVF + CAGRA |

## 10. Cross-references

- Prior syntheses: `storage-engine-pattern-synthesis-*` (1-6),
  `graph-analytics-pattern-synthesis-*` (7-12) — this category
  consumes both, as §4 shows.
- Individual pairs: patterns 13-16 in `pattern-index.md`.
- Next category per spec order: full-text-search — the ancestor:
  inverted indexes, posting-list compression, BM25, and the
  Lucene segment architecture everyone inherited.

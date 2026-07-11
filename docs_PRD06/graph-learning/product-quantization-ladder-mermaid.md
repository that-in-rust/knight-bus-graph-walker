# Product Quantization Ladder — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `product-quantization-ladder-ascii.md` / `product-quantization-ladder-mermaid.md` |
| One-line job | Compress vectors 4x-192x by encoding them against small learned codebooks, and score compressed codes against a query with table lookups instead of float arithmetic — accuracy traded for memory on an explicit ladder |

## 1. The ladder

```mermaid
flowchart TD
    F32["float32: 4 B/dim, exact, 1x"]
    SQ["scalar u8: 1 B/dim, per-dim min/max, 4x"]
    TQ["turbo/tq: 2-4 bit scalar, 8-16x"]
    PQ["product PQ: learned codebooks, 8-64x"]
    BQ["binary: sign bit only, 32x"]
    F32 --> SQ --> TQ --> PQ --> BQ
    BQ --> Q["qdrant ships every rung in one directory:<br/>encoded_vectors_{u8,tq,pq,binary}.rs"]
```

Distances are computed IN the compressed domain — decompression
never happens on the hot path. That is the pattern.

## 2. PQ encoding

```mermaid
flowchart LR
    V["768-d vector"] --> SP["split into M=96 subvectors<br/>of dsub = 8 dims"]
    SP --> KM["per subspace: k-means learns<br/>ksub = 2^nbits = 256 centroids<br/>(faiss ProductQuantizer.h:30-35;<br/>qdrant CENTROIDS_COUNT=256,<br/>encoded_vectors_pq.rs:30)"]
    KM --> C["code = M bytes [c1..cM],<br/>each the nearest-centroid index"]
    C --> R["3072 B -> 96 B = 32x,<br/>plus one shared (M,ksub,dsub)<br/>codebook table ~0.8 MB<br/>(ProductQuantizer.h:54-55)"]
```

## 3. ADC scoring — the lookup-table trick

```mermaid
sequenceDiagram
    participant Q as query (exact floats)
    participant T as dis_table (M x ksub)
    participant DB as encoded codes
    Q->>T: ONCE per query: dis_table[m][j] =<br/>||query_sub_m - centroid[m][j]||²<br/>(ProductQuantizer.h:118-126;<br/>qdrant lut, encoded_vectors_pq.rs:38-42)
    loop each candidate code
        DB->>T: dist ~ SUM_m dis_table[m][code[m]]
        Note over T: M lookups + M adds,<br/>ZERO multiplies per candidate
    end
    Note over Q,DB: ASYMMETRIC: query stays exact, only the<br/>database side is quantized — half the error<br/>of symmetric SDC (whose table faiss also<br/>keeps, ProductQuantizer.h:167)
```

## 4. Binary quantization — the extreme rung

```mermaid
flowchart TD
    E["encode: bit_i = sign(v_i);<br/>768 dims -> 96 bytes"]
    E --> S["score: hamming = popcount(xor(a,b)) —<br/>qdrant xor_popcnt with SSE lanes for<br/>u128/u64/u32 (encoded_vectors_binary.rs:144-177):<br/>two CPU instructions per word"]
    S --> W["works because high-dim normalized<br/>embeddings: angle ~ hamming on sign bits"]
    W --> USE["recall drops -> used as a PREFILTER,<br/>~10-50x faster scoring"]
```

## 5. The two-stage rescoring architecture

```mermaid
flowchart LR
    Q[query] --> S1["stage 1: HNSW/IVF over<br/>COMPRESSED codes in RAM,<br/>ADC/hamming scoring"]
    S1 --> C["top ~4k candidates"]
    C --> S2["stage 2: rescore with FULL float<br/>vectors from disk/mmap"]
    S2 --> K["top k, exact order"]
    K --> BL["the bloom-filter shape (pattern 6):<br/>cheap approximate structure gates the<br/>expensive exact one; stage-1 errors cost<br/>latency, never correctness — as long as<br/>the true neighbors survive into the pool"]
```

## 6. Budget arithmetic — 100M x 768-d

```mermaid
flowchart TD
    A["raw f32: 307 GB (RAM-hostile)"]
    B["SQ u8: 77 GB"]
    Cc["PQ M=96: 9.6 GB + codebooks"]
    D["BQ: 96 B/vec = 9.6 GB —<br/>same size as PQ M=96 here;<br/>BQ wins on SPEED at this dim"]
    A --> B --> Cc
    B --> D
    Cc --> T["PQ + HNSW links (12.8 GB) ~ 22 GB:<br/>one 32 GB box replaces a 384 GB box;<br/>stage-2 reads ~12 MB of exact vectors<br/>per query from NVMe"]
```

## 7. ADC cost per query

```mermaid
flowchart LR
    TB["build table: 96 x 256 = 24,576<br/>8-dim distances ~ 200k flops, once"]
    TB --> SC["scan 10k candidates:<br/>10k x 96 lookups+adds ~ 1M<br/>cache-friendly ops"]
    SC --> VS["vs full precision: 15.4M flops +<br/>30 GB/s memory traffic for raw vectors"]
    VS --> AM["table amortizes after ~15 candidates —<br/>every real candidate list is far past that"]
```

## 8. The verification angle

```mermaid
flowchart TD
    DT["given trained codebooks, quantization is<br/>a DETERMINISTIC transform"] --> P["pin the codebook ARTIFACT in differential<br/>tests: same codebook -> exact code and<br/>ADC-score equality is checkable"]
    P --> KM["but training is k-means from random<br/>seeds: independently trained PQ indexes<br/>only compare at the recall@k level"]
    KM --> TH["docs_PRD06 thesis: split the surface into<br/>the deterministic part (test exactly) and<br/>the stochastic part (test distributionally) —<br/>same split as pattern 13's build vs search"]
```

## 9. Kinship map

```mermaid
flowchart LR
    PQL[quantization ladder] --> B6["pattern 6 bloom filters:<br/>cheap gate before expensive truth"]
    PQL --> R3["pattern 3 roaring bitmaps:<br/>pick the encoding per data shape —<br/>containers there, ladder rungs here"]
    PQL --> H13["pattern 13 HNSW:<br/>stage 1 searches the compressed codes"]
    PQL --> DA["DiskANN/pgvectorscale (SBQ):<br/>the ladder grafted onto on-disk graphs —<br/>next pattern candidate"]
```

## 9b. Choosing a rung — the decision procedure

```mermaid
flowchart TD
    START["what is the binding constraint?"]
    START -->|"RAM is fine,<br/>latency matters"| U8["SQ u8: 4x smaller, SIMD-friendly,<br/>~0.99 recall retained — the default<br/>first step everywhere"]
    START -->|"RAM is the wall"| DIM{"embedding family?"}
    DIM -->|"high-dim normalized<br/>(OpenAI/Cohere-style)"| BQ2["BQ prefilter + rescore:<br/>angle survives sign-bit projection"]
    DIM -->|"lower-dim or<br/>anisotropic"| PQ2["PQ: codebooks adapt to the<br/>actual data distribution —<br/>k-means IS the compression model"]
    U8 & BQ2 & PQ2 --> RS["ALWAYS pair with full-precision<br/>rescoring; tune stage-1 pool size until<br/>end-to-end recall target holds"]
    RS --> OB["observable knobs for a harness:<br/>rung, pool size, rescore on/off —<br/>each moves the recall/latency Pareto<br/>point measurably (ann-benchmarks method)"]
```

The rung choice is pattern 8's direction switch at architecture
scale: measure the workload's shape, pick the representation whose
cost model fits, keep a guard (rescoring) so the wrong choice
degrades gracefully instead of failing.

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| faiss | `reference-repos-corpus/faiss-src/faiss/impl/ProductQuantizer.h` | canonical PQ: M/nbits/dsub/ksub (30-35), centroid layout (54-63), ADC tables (118-138), SDC (167) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/quantization/src/encoded_vectors_pq.rs` | production PQ: 256 centroids (30), LUT (38-42), chunk division (63-88, 160) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/quantization/src/encoded_vectors_binary.rs` | BQ with SIMD xor_popcnt (100, 144-177) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/quantization/src/` | the full ladder: u8, tq, pq, binary + kmeans.rs |
| pgvectorscale | `reference-repos-corpus/pgvectorscale-src` | the ladder inside Postgres (SBQ) over a DiskANN graph |

## 11. Cross-references

- Sibling patterns: `hnsw-layered-greedy`, `bloom-filter-shortcuts`,
  `roaring-bitmap-sets` (see §9).
- Next in category: DiskANN/Vamana on-disk graphs (where stage-2
  vectors live), then IVF partitioning as the HNSW alternative.
- Paper trail: Jégou et al.'s PQ paper and the faiss library paper
  (verified in `research-papers-ledger.md`).
- 202606 digest overlap: digests mentioned PQ as Milvus/faiss
  compression; this pair adds ADC table mechanics, the BQ popcount
  kernel with line cites, the rescoring architecture, and the
  budget arithmetic.

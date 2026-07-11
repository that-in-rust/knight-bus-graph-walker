# Product Quantization Ladder — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `product-quantization-ladder-ascii.md` / `product-quantization-ladder-mermaid.md` |
| One-line job | Compress vectors 4x-192x by encoding them against small learned codebooks, and score compressed codes against a query with table lookups instead of float arithmetic — accuracy traded for memory on an explicit ladder |

## 1. The job

Pattern 13 showed the vectors dominate memory (307 GB of vectors vs
~14 GB of graph at 100M x 768-d). Quantization attacks exactly that
term. The ladder, roughly ordered by compression:

```text
float32          4 B/dim      exact              1x
scalar (SQ/u8)   1 B/dim      per-dim min/max    4x
turbo/tq         ~0.5 B/dim   2-4 bit scalar     8-16x
product (PQ)     ~1 B / 2 dims  learned codebooks  8-64x
binary (BQ)      1 bit/dim    sign only          32x
(qdrant ships every rung: encoded_vectors_{u8,tq,pq,binary}.rs)
```

The pattern's second half is the real trick: distances are computed
IN the compressed domain — decompression never happens on the hot
path.

## 2. Raw data shape — PQ

```text
split each d-dim vector into M subvectors of dsub = d/M dims;
learn ksub = 2^nbits centroids PER subspace via k-means:

faiss ProductQuantizer.h:30-35,54-55
    M      number of subquantizers
    nbits  bits per code (typically 8 -> ksub = 256)
    centroid table: (M, ksub, dsub) floats

qdrant encoded_vectors_pq.rs:30
    pub const CENTROIDS_COUNT: usize = 256;   # nbits=8, fixed

encoded vector = M bytes: [c1, c2, ..., cM]
    each byte = index of nearest centroid in that subspace

768-d f32 -> 3072 B raw; M=96 -> 96 B codes = 32x compression
(+ one shared 96 x 256 x 8-dim codebook table, ~0.8 MB total)
```

## 3. Scoring without decompression — the ADC table

Per query, precompute once (faiss ProductQuantizer.h:118-126):

```text
dis_table[m][j] = || query_sub_m - centroid[m][j] ||^2
    size M x ksub floats (96 x 256 = 24k floats, fits L2 cache)

then per candidate code:
    dist ~ SUM_m dis_table[m][ code[m] ]
    = M table lookups + M adds. ZERO multiplies per candidate.
```

qdrant names the same structure lut ("distance from each query
chunk to each centroid related to this chunk",
encoded_vectors_pq.rs:38-42). This is Asymmetric Distance
Computation: the query stays exact, only the database side is
quantized — half the quantization error of symmetric (SDC, whose
table faiss also keeps: ProductQuantizer.h:167).

## 4. Binary quantization — the extreme rung

```text
encode: bit_i = sign(v_i)      768 dims -> 96 bytes -> 12 bytes/u128
score:  hamming = popcount(xor(a, b))
qdrant's xor_popcnt with SSE specializations for u128/u64/u32
lanes (encoded_vectors_binary.rs:144-177) — the entire distance
function is two CPU instructions per word.
works because for high-dim normalized embeddings, angle ~ hamming
on sign bits; ~32x compression and ~10-50x faster scoring, but
recall drops enough that it is used as a PREFILTER.
```

## 5. The two-stage rescoring architecture

Every production system uses the ladder end-to-end, not one rung:

```text
stage 1: search HNSW/IVF over COMPRESSED codes (fits in RAM,
         fast ADC/hamming scoring) -> top ~4k candidates
stage 2: rescore candidates with FULL float vectors (from disk
         or mmap) -> top k
error analysis: stage 1 only needs enough fidelity to not DROP
true neighbors from the 4k pool; stage 2 restores exact order.
this is the storage category's bloom-filter shape (pattern 6):
a cheap approximate structure gates access to the expensive
exact one — false positives cost latency, never correctness
(as long as stage-1 recall holds).
```

## 6. Worked example — 100M x 768-d budget

```text
raw floats:               307 GB   (RAM-hostile)
SQ u8:                     77 GB
PQ M=96:                  9.6 GB   (+ codebooks, negligible)
BQ:                       9.6 GB at 1 bit/dim = /8 more: 1.2 GB... 
    (768 bits = 96 B: same as PQ M=96 — BQ wins on SPEED not size
     at this dim; at 1536-d BQ is 192 B vs PQ's chosen M)
PQ + HNSW links (12.8 GB) ~ 22 GB total: a single 32 GB box
serves what raw floats needed a 384 GB box for — with stage-2
rescoring reading exact vectors from NVMe only for ~4k
candidates/query (~12 MB of reads).
```

## 7. Worked example — ADC cost per query

```text
build table: M x ksub = 96 x 256 = 24,576 subvector distances,
             each 8-dim: ~200k flops, ONCE per query.
scan 10k candidates: 10k x 96 lookups+adds ~ 1M cache-friendly ops
vs full-precision: 10k x 768 x 2 = 15.4M flops + 30 GB/s of
    memory traffic for the raw vectors.
the table amortizes after ~15 candidates — every ANN candidate
list is far past that.
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| faiss | `reference-repos-corpus/faiss-src/faiss/impl/ProductQuantizer.h` | canonical PQ: M/nbits/dsub/ksub (30-35), centroid layout (54-63), ADC tables (118-138), SDC (167) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/quantization/src/encoded_vectors_pq.rs` | production Rust PQ: fixed 256 centroids (30), LUT (38-42), chunk division (63-88, 160) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/quantization/src/encoded_vectors_binary.rs` | BQ with SIMD xor_popcnt (100, 144-177) |
| qdrant | `reference-repos-corpus/qdrant-src/lib/quantization/src/` | the full ladder in one directory: u8, tq (turbo), pq, binary + kmeans.rs |
| pgvectorscale | `reference-repos-corpus/pgvectorscale-src` | the same ladder grafted into Postgres (SBQ) over a DiskANN graph |

## 9. Cross-references

- Sibling patterns: `hnsw-layered-greedy` (what stage 1 searches);
  `bloom-filter-shortcuts` (same cheap-gate-before-expensive-truth
  shape); `roaring-bitmap-sets` (both are "pick the encoding per
  data shape" designs — containers there, ladder rungs here).
- Next in category: DiskANN/Vamana on-disk graphs (where stage-2
  vectors live), then IVF partitioning as the HNSW alternative.
- Verification note (docs_PRD06 thesis): quantization is a
  DETERMINISTIC transform given trained codebooks — but training
  is k-means from random seeds. Pin the codebook artifact itself
  in differential tests; comparing independently trained PQ
  indexes only makes sense at the recall@k level.
- 202606 digest overlap: digests mentioned PQ as Milvus/faiss
  compression; this pair adds the ADC table mechanics, the BQ
  popcount kernel with line cites, the rescoring architecture, and
  the budget arithmetic.

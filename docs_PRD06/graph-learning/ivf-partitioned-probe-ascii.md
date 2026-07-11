# IVF Partitioned Probe — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `ivf-partitioned-probe-ascii.md` / `ivf-partitioned-probe-mermaid.md` |
| One-line job | Skip most of the dataset by clustering vectors into nlist buckets at build time and scanning only the nprobe closest buckets at query time — the inverted-index idea from text search, applied to geometry |

## 1. The job

The graph indexes (patterns 13, 15) navigate; IVF (Inverted File)
PARTITIONS. Train k-means over the dataset, assign every vector to
its nearest centroid's bucket, and at query time:

```text
1. score the query against all nlist centroids   (tiny)
2. pick the nprobe closest buckets
3. linearly scan ONLY those buckets              (the work)

scanned fraction ~ nprobe/nlist; recall lost = the true
neighbors that live in unprobed buckets near cell boundaries.
```

Two dials, one Pareto curve: nlist (build-time granularity) and
nprobe (query-time breadth) — pattern 13's ef in partition form.

## 2. Raw data shape

faiss's canonical structures:

```text
Level1Quantizer (IndexIVF.h:25-43)
    quantizer   an Index that maps vector -> bucket
                ("quantizer that maps vectors to inverted
                 lists", :31-32) — usually IndexFlat over the
                nlist centroids, but ANY index works (even HNSW
                over the centroids, for huge nlist)
    nlist       number of buckets (:35)

nprobe          "number of probes at query time" (IndexIVF.h:69,
                105; knowhere re-exports the same two knobs,
                index_param.h:136-137)

InvertedLists (invlists/InvertedLists.h:58-89)
    per bucket: ids[]   vector IDs
                codes[] the stored encodings, list_size x
                        code_size bytes (:77-84)
    concurrency contract: "for resize and add_entries, only
    concurrent access to DIFFERENT lists" is safe (:55-56) —
    bucket = natural lock/shard granularity.
```

The bucket contents are OPAQUE codes: flat floats, PQ codes
(IndexIVFPQ), or SQ bytes — IVF composes with the whole
quantization ladder (pattern 14).

## 3. The residual trick (IndexIVFPQ.h:31, 156-172)

```text
instead of PQ-encoding the raw vector, encode
    residual = vector - its_centroid
residuals are centered near zero with much smaller variance than
raw vectors -> the SAME 256-centroid-per-subspace codebook covers
them far more precisely. Costs one table recomputation per probed
bucket (query residual differs per centroid), which faiss
mitigates with precomputed tables (:156).
partition + residual encoding = coarse-to-fine: the centroid says
WHERE the vector roughly is, the PQ code says where WITHIN the
cell.
```

## 4. Step-by-step: one query

```text
setup:  nlist = 4096 buckets over 10M vectors (~2.4k/bucket),
        IVF+PQ codes, nprobe = 16
1. query vs 4096 centroids: 4096 distances        ~ 0.1 ms
2. take 16 nearest buckets  -> ~39k candidates (0.4% of data)
3. per bucket: compute/fetch the residual-adjusted ADC table,
   scan codes with table lookups (pattern 14 §3)  ~ 39k x M adds
4. keep a k-heap across all probed buckets; return top k
total: ~1-2M cheap ops vs 10M full float distances — and the
buckets are CONTIGUOUS arrays: pure sequential scans (pattern 7's
argument again), SIMD-friendly, prefetchable.
```

## 5. Worked example — the two dials

```text
10M vectors, recall@10 targets (representative shape):
  nlist 4096, nprobe  1:  scan 0.02%   recall ~0.4   ~0.1 ms
  nlist 4096, nprobe 16:  scan 0.4%    recall ~0.9   ~0.5 ms
  nlist 4096, nprobe 128: scan 3%      recall ~0.99  ~3 ms
rule-of-thumb nlist ~ sqrt(N) (here ~3162) balances the two
costs: centroid scoring (grows with nlist) vs bucket scanning
(shrinks with nlist). nprobe then walks the recall/latency curve
at query time WITHOUT rebuilding — the cheap dial.
```

## 6. IVF vs graph — when partitioning wins

```text
IVF wins when:  memory is tight (no per-node links: zero graph
                overhead); batch/GPU workloads (bucket scans are
                dense matrix ops — cuVS/raft's home turf);
                streaming inserts (append to a bucket, no wiring)
graph wins:     single-query latency at high recall (navigates
                to the answer in ~log hops instead of scanning
                fixed fractions); skewed/clustered data where
                cell boundaries cut neighborhoods
production splits accordingly: faiss/cuVS default to IVF-PQ for
GPU and billion-scale batch; qdrant/weaviate default HNSW for
online serving; Milvus ships both and lets the collection choose.
```

## 7. Where systems inherit this

- faiss: IndexIVF is the family root — IVFFlat, IVFPQ, IVFSQ; the
  coarse quantizer is pluggable (IndexIVF.h:38-42 even enumerates
  how it trains).
- knowhere/Milvus: exposes the same nlist/nprobe contract
  (index_param.h:136-137) across CPU and GPU backends.
- cuVS: IVF-Flat/IVF-PQ as the GPU-native structures.
- pgvector: `ivfflat` was its FIRST index type (HNSW came later)
  — buckets map cleanly onto Postgres pages.
- Lucene/Elasticsearch text search: IVF IS the inverted index
  pattern itself — term -> posting list becomes centroid ->
  vector list; the FTS category studies the ancestor directly.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| faiss | `reference-repos-corpus/faiss-src/faiss/IndexIVF.h` | Level1Quantizer (25-43), nprobe contract (69, 105), pluggable coarse quantizer |
| faiss | `reference-repos-corpus/faiss-src/faiss/invlists/InvertedLists.h` | bucket storage: ids/codes per list (58-89), per-list concurrency contract (55-56) |
| faiss | `reference-repos-corpus/faiss-src/faiss/IndexIVFPQ.h` | residual encoding (31), precomputed tables (156-172) |
| knowhere | `reference-repos-corpus/knowhere-src/include/knowhere/comp/index_param.h` | NPROBE/NLIST as the cross-backend contract (136-137) |
| cuvs | `reference-repos-corpus/cuvs-src` | GPU-native IVF-Flat/IVF-PQ |

## 9. Cross-references

- Sibling patterns: `hnsw-layered-greedy` and
  `diskann-vamana-layout` (the navigate pole vs this scan pole);
  `product-quantization-ladder` (what fills the buckets; residual
  trick); `csr-adjacency-layout` (contiguous-scan economics).
- The FTS category will meet this pattern's ancestor: the text
  inverted index with term posting lists.
- Verification note (docs_PRD06 thesis): IVF assignment is
  deterministic given centroids — pin the trained centroids and
  bucket contents are exactly reproducible; recall then depends
  only on (nlist, nprobe), making IVF the EASIEST ANN structure
  to differential-test. Graph indexes should be tested against an
  IVF or flat oracle for exactly this reason.
- 202606 digest overlap: digests mentioned IVF as faiss's other
  index; this pair adds the residual arithmetic, the concurrency
  contract, the sqrt(N) sizing rule, and the IVF-vs-graph
  decision table.

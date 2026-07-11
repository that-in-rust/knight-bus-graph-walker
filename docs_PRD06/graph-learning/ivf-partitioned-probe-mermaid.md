# IVF Partitioned Probe — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `ivf-partitioned-probe-ascii.md` / `ivf-partitioned-probe-mermaid.md` |
| One-line job | Skip most of the dataset by clustering vectors into nlist buckets at build time and scanning only the nprobe closest buckets at query time — the inverted-index idea from text search, applied to geometry |

## 1. Partition, don't navigate

```mermaid
flowchart TD
    T["build: k-means -> nlist centroids;<br/>every vector joins its nearest<br/>centroid's bucket"]
    T --> Q["query: score ALL nlist centroids (tiny)"]
    Q --> P["pick nprobe closest buckets"]
    P --> S["linearly scan ONLY those buckets"]
    S --> F["scanned fraction ~ nprobe/nlist;<br/>recall lost = true neighbors in<br/>unprobed boundary cells"]
```

```mermaid
flowchart LR
    D1["nlist: build-time granularity"] --> PC["one Pareto curve —<br/>pattern 13's ef,<br/>in partition form"]
    D2["nprobe: query-time breadth<br/>(walk the curve WITHOUT rebuilding)"] --> PC
```

## 2. The data structures (faiss)

```mermaid
flowchart TD
    L1["Level1Quantizer (IndexIVF.h:25-43):<br/>quantizer = an Index mapping vector -> bucket;<br/>usually IndexFlat over centroids, but ANY<br/>index works — even HNSW over centroids"]
    L1 --> IL["InvertedLists (invlists/InvertedLists.h:58-89):<br/>per bucket: ids[] + codes[]<br/>(list_size x code_size bytes)"]
    IL --> CC["concurrency contract (:55-56):<br/>'for resize and add_entries, only concurrent<br/>access to DIFFERENT lists' is safe —<br/>bucket = natural lock/shard granularity"]
    IL --> OP["bucket contents are OPAQUE codes:<br/>flat / PQ / SQ — IVF composes with the<br/>whole quantization ladder (pattern 14)"]
```

## 3. The residual trick (IndexIVFPQ.h:31, 156-172)

```mermaid
flowchart LR
    R["encode residual = vector - its_centroid,<br/>not the raw vector"]
    R --> V["residuals center near zero with small<br/>variance -> same 256-centroid codebooks<br/>cover them far more precisely"]
    V --> C["cost: query residual differs per probed<br/>bucket -> per-bucket table recompute,<br/>mitigated by precomputed tables (:156)"]
    C --> CF["coarse-to-fine: centroid says WHERE<br/>roughly, PQ code says where WITHIN"]
```

## 4. One query, end to end

```mermaid
sequenceDiagram
    participant Q as query
    participant C as centroids (4096)
    participant B as probed buckets (16)
    participant H as k-heap
    Q->>C: 4096 distances (~0.1 ms)
    C-->>Q: 16 nearest buckets (~39k of 10M vectors)
    loop each probed bucket
        Q->>B: residual-adjusted ADC table
        B->>H: scan contiguous codes with table<br/>lookups; push improvements
    end
    H-->>Q: top k
    Note over B: buckets are CONTIGUOUS arrays —<br/>pure sequential scans, SIMD-friendly<br/>(pattern 7's argument again)
```

## 5. The two dials (10M vectors, recall@10 shape)

```mermaid
flowchart LR
    A["nprobe 1: scan 0.02%,<br/>recall ~0.4, ~0.1 ms"] --> B2["nprobe 16: scan 0.4%,<br/>recall ~0.9, ~0.5 ms"]
    B2 --> C2["nprobe 128: scan 3%,<br/>recall ~0.99, ~3 ms"]
    C2 --> RL["rule of thumb nlist ~ sqrt(N):<br/>balances centroid scoring (grows with<br/>nlist) vs bucket scanning (shrinks)"]
```

## 6. IVF vs graph — the decision table

```mermaid
flowchart TD
    W{"workload?"}
    W -->|"tight memory, batch/GPU,<br/>streaming inserts"| IVF["IVF: zero graph overhead;<br/>bucket scans = dense matrix ops<br/>(cuVS home turf); insert = append"]
    W -->|"online single-query latency<br/>at high recall"| G["graph (HNSW/Vamana): navigates in<br/>~log hops instead of scanning<br/>fixed fractions"]
    IVF --> PR["production split: faiss/cuVS default IVF-PQ<br/>for GPU + billion-scale batch;<br/>qdrant/weaviate default HNSW for serving;<br/>Milvus ships both per collection"]
    G --> PR
```

## 7. Inheritance map

```mermaid
flowchart LR
    I[IVF] --> FA["faiss: IndexIVF family root —<br/>IVFFlat/IVFPQ/IVFSQ, pluggable<br/>coarse quantizer (IndexIVF.h:38-42)"]
    I --> KN["knowhere/Milvus: same nlist/nprobe<br/>contract across CPU+GPU backends<br/>(index_param.h:136-137)"]
    I --> CU["cuVS: GPU-native IVF-Flat/IVF-PQ"]
    I --> PG["pgvector: ivfflat was its FIRST index —<br/>buckets map onto Postgres pages"]
    I --> FTS["Lucene/ES: IVF IS the inverted index —<br/>term -> posting list becomes<br/>centroid -> vector list"]
```

## 8. The verification angle

```mermaid
flowchart TD
    DET["assignment is deterministic given<br/>centroids: pin the trained centroids -><br/>bucket contents exactly reproducible"] --> EZ["recall then depends only on (nlist,<br/>nprobe): IVF is the EASIEST ANN<br/>structure to differential-test"]
    EZ --> OR["so: use IVF or flat scan as the ORACLE<br/>when testing graph indexes — a slow exact<br/>or semi-exact endpoint against which<br/>HNSW/Vamana recall is measured"]
    OR --> TH["docs_PRD06 thesis condition 1: the oracle<br/>itself must be trustworthy — flat scan is<br/>the ground truth, IVF the fast approximation<br/>of the ground truth"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K13["patterns 13/15: navigate vs scan —<br/>the two poles of ANN"]
    K14["pattern 14: what fills the buckets;<br/>the residual trick sharpens it"]
    K7["pattern 7: contiguous-scan economics"]
    K8["pattern 8: nprobe is a breadth knob like<br/>ef — but chosen per query, statelessly"]
    K13 & K14 & K7 & K8 --> CAT["and the FTS category will study this<br/>pattern's ANCESTOR: the text inverted<br/>index with term posting lists"]
```

## 9b. The mutation story — why streaming favors IVF

```mermaid
sequenceDiagram
    participant W as writer
    participant B as bucket b
    participant G as graph index (contrast)
    W->>B: insert v: one centroid scoring pass,<br/>then append (id, code) to bucket b
    Note over B: per-list concurrency contract<br/>(InvertedLists.h:55-56): appends to<br/>DIFFERENT buckets need no coordination
    W->>B: delete v: tombstone the id, or<br/>compact the bucket offline
    Note over B: no rewiring — the structure is<br/>a partition, not a topology
    W->>G: contrast: graph insert = beam search +<br/>bidirectional wiring + neighbor re-pruning<br/>(patterns 13 §8b, 15 §9b); delete = link<br/>healing (qdrant graph_layers_healer.rs)
    Note over W,G: IVF degrades differently: as data drifts,<br/>centroids go STALE — buckets bloat unevenly<br/>and recall decays silently; the fix is<br/>periodic re-training + re-assignment,<br/>i.e. compaction (pattern 1) at the<br/>clustering level
```

Both families thus reinvent the storage category's maintenance
loop: graphs pay per-write (wiring), partitions pay per-epoch
(re-clustering). There is no mutation-free index — only a choice
of when to pay.

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| faiss | `reference-repos-corpus/faiss-src/faiss/IndexIVF.h` | Level1Quantizer (25-43), nprobe (69, 105) |
| faiss | `reference-repos-corpus/faiss-src/faiss/invlists/InvertedLists.h` | bucket storage (58-89), concurrency contract (55-56) |
| faiss | `reference-repos-corpus/faiss-src/faiss/IndexIVFPQ.h` | residual encoding (31), precomputed tables (156-172) |
| knowhere | `reference-repos-corpus/knowhere-src/include/knowhere/comp/index_param.h` | NPROBE/NLIST cross-backend contract (136-137) |
| cuvs | `reference-repos-corpus/cuvs-src` | GPU-native IVF variants |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Next in category: the vector-ann synthesis pair rolling up
  patterns 13-16.
- Paper trail: Jégou et al.'s PQ paper introduced IVFADC (IVF +
  PQ + ADC in one system); the faiss library paper documents the
  family — see `research-papers-ledger.md`.
- 202606 digest overlap: digests mentioned IVF as faiss's other
  index; this pair adds residual arithmetic, the concurrency
  contract, sqrt(N) sizing, and the IVF-vs-graph decision table.

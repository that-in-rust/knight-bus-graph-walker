# DiskANN Vamana Layout — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `diskann-vamana-layout-ascii.md` / `diskann-vamana-layout-mermaid.md` |
| One-line job | Billion-scale ANN on one machine by co-locating each vector WITH its neighbor list in a 4 KB disk sector: one flat graph (Vamana), alpha-relaxed pruning for short paths, PQ codes in RAM steering which sectors to read |

## 1. The memory split

```mermaid
flowchart TD
    RAM["RAM: PQ codes for ALL vectors<br/>(pattern 14) — tens of GB at 1B scale"]
    NVME["NVMe: sector[i] = full vector i +<br/>neighbor IDs of i — one 4 KB read<br/>= one node's data AND adjacency"]
    RAM -->|"cheap ADC distances<br/>steer the walk"| NVME
    NVME -->|"exact vectors rescore in-loop;<br/>neighbor IDs feed the beam"| RAM
    NVME --> F["pattern 14's two-stage architecture<br/>FUSED into the traversal loop"]
```

## 2. The sector format (disk_index_writer.rs:362-390)

```mermaid
flowchart LR
    B1["block #1: metadata —<br/>node_len, num_nodes_per_sector,<br/>vamana_frozen_point (:366)"]
    B2["blocks #2..n: packed nodes"]
    B1 --> B2
    B2 --> P1["node_len <= 4096: pack floor(4096/len)<br/>nodes/sector, never split —<br/>'600B node: pack 6, leave 496B unused'<br/>(:385-386)"]
    B2 --> P2["node_len > 4096: span consecutive<br/>sectors, pad to the boundary (:389-390)"]
    P1 & P2 --> CSR["CSR (pattern 7) re-cut for the disk's atom:<br/>locality unit = 4 KB sector, and padding<br/>is cheaper than a second read"]
```

## 3. Why one flat graph — Vamana vs HNSW

```mermaid
flowchart TD
    H["HNSW hierarchy shortens the ENTRY path —<br/>but on disk every layer hop is a random read:<br/>layers MULTIPLY I/O"]
    V["Vamana: ONE flat graph, entered at a fixed<br/>frozen medoid; long-range edges baked in<br/>by alpha-pruning"]
    H -->|"disk flips the tradeoff"| V
    V --> A["alpha occlude rule (index.rs:2625-2637):<br/>candidate i rejected if a kept neighbor j<br/>occludes it beyond alpha; alpha=1.0 IS<br/>HNSW's diversity heuristic (pattern 13 §4)"]
    A --> A2["alpha > 1 (typ 1.2) KEEPS some occluded<br/>edges: redundant long edges -> shorter<br/>paths -> fewer disk reads; multi-pass<br/>schedule from 1.0 up (index.rs:2598-2605)"]
    A2 --> K["the knob's meaning flips per medium:<br/>RAM wants sparse (cache), disk wants<br/>dense-but-short (100 us per hop)"]
```

## 4. Search, end to end

```mermaid
sequenceDiagram
    participant Q as query
    participant R as RAM (PQ codes)
    participant D as NVMe sectors
    Q->>R: build ADC table once (pattern 14 §3)
    Q->>D: beam = {medoid}, width W
    loop until best unexpanded > worst kept
        R->>R: pick unexpanded candidate<br/>with best PQ distance
        R->>D: READ its sector (batched W-wide)
        D-->>Q: exact vector -> rescore this node
        D-->>R: neighbor IDs -> PQ-score in RAM,<br/>push into beam
    end
    Q-->>Q: top k by EXACT distance<br/>(already rescored in-loop)
```

## 5. Budget at 1B x 768-d

```mermaid
flowchart TD
    RB["RAM: PQ M=64 -> 64 GB<br/>(SBQ/OPQ can halve)"]
    DB["NVMe: 3072 B vector + 256 B links<br/>= 3328 B -> 1 node/sector -> 4 TB"]
    RB & DB --> S["search at W=8, ~4 hops avg:<br/>~32 sector reads x 100 us, batched<br/>-> 1-2 ms latency, recall ~0.95"]
    S --> INV["the invariant: latency is COUNTED IN<br/>SECTOR READS — everything optimizes<br/>hop count, hence alpha > 1"]
```

## 6. Co-location arithmetic

```mermaid
flowchart LR
    SPL["split layout: adjacency file +<br/>vector file = 2 reads per hop"]
    CO["co-located sector:<br/>1 read per hop"]
    SPL -->|"the entire disk format exists<br/>for this one fact"| CO
    CO --> KIN["same move as pattern 7 storing<br/>neighbors inline vs pointer-chasing,<br/>one level down the hierarchy"]
```

## 7. Inheritance map

```mermaid
flowchart LR
    DA[DiskANN/Vamana] --> PG["pgvectorscale: Vamana on 8 KB Postgres<br/>pages — access_method/graph/, SBQ,<br/>planner cost_estimate.rs"]
    DA --> KH["Milvus/knowhere: DiskANN as the<br/>on-disk engine beside HNSW/FAISS"]
    DA --> JV["JVector (Cassandra): Vamana-style flat<br/>graph with inline vectors — same<br/>read-amplification argument"]
    DA --> FR["FreshDiskANN: inserts via in-RAM delta<br/>index + periodic merge — the LSM answer<br/>(patterns 1/5), re-derived for ANN"]
```

## 8. The verification angle

```mermaid
flowchart TD
    FZ["the disk format is a FROZEN artifact"] --> BD["pin build seeds + thread count -><br/>byte-level index diffing is possible"]
    BD --> CT["otherwise compare cheap observable<br/>counters: recall@k, mean hops,<br/>sectors per query"]
    CT --> TH["docs_PRD06 thesis condition 1:<br/>hop and sector counts make the I/O<br/>behavior OBSERVABLE — the part of ANN<br/>that query-diffing alone cannot see"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K13["pattern 13: alpha=1.0 recovers HNSW's<br/>heuristic — Vamana generalizes it"]
    K14["pattern 14: PQ is the RAM half<br/>of this design"]
    K7["pattern 7: the locality argument,<br/>disk edition"]
    K15["patterns 1/5: FreshDiskANN's update story<br/>is compaction + snapshot flip"]
    K13 & K14 & K7 & K15 --> C["the vector-ann category is the storage and<br/>analytics categories meeting geometry:<br/>every design here is an old pattern<br/>re-priced for a new cost model"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| DiskANN | `reference-repos-corpus/DiskANN-src/diskann-disk/src/storage/disk_index_writer.rs` | sector format: metadata (366), packing/padding (385-390) |
| DiskANN | `reference-repos-corpus/DiskANN-src/diskann/src/graph/index.rs` | occlude_list alpha-pruning (2598-2605, 2625-2637, 2675) |
| DiskANN | `reference-repos-corpus/DiskANN-src/diskann-disk/src/search/` | PQ-steered beam over sectors (pq/, search_mode.rs) |
| pgvectorscale | `reference-repos-corpus/pgvectorscale-src/pgvectorscale/src/access_method/` | Vamana on Postgres pages |
| knowhere | `reference-repos-corpus/knowhere-src` | DiskANN inside Milvus |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Next in category: IVF partitioning (the clustering alternative),
  then the vector-ann synthesis pair.
- Paper trail: DiskANN (NeurIPS'19), FreshDiskANN, and the
  original Vamana description — see `research-papers-ledger.md`.
- 202606 digest overlap: digests named DiskANN as the
  billion-scale option; this pair adds sector packing rules,
  occlude arithmetic with line cites, and the I/O cost models.

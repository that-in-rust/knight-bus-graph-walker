# DiskANN Vamana Layout — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `diskann-vamana-layout-ascii.md` / `diskann-vamana-layout-mermaid.md` |
| One-line job | Billion-scale ANN on one machine by co-locating each vector WITH its neighbor list in a 4 KB disk sector: one flat graph (Vamana), alpha-relaxed pruning for short search paths, PQ codes in RAM steering which sectors to read |

## 1. The job

HNSW (pattern 13) assumes the graph AND vectors live in RAM. At
1B x 768-d that is 3+ TB. DiskANN's answer: keep only compressed
codes in RAM and put the full-precision vectors plus the graph on
NVMe — but lay them out so every graph hop costs exactly ONE disk
read:

```text
RAM:   PQ codes (pattern 14) for ALL vectors        ~ tens of GB
NVMe:  sector[i] = [ full vector i | neighbor IDs of i ]
       one 4 KB read = one node's data AND its adjacency

search: beam walk steered by cheap PQ distances (RAM),
        sector reads fetch exact vectors for rescoring +
        the next hop's candidate IDs — pattern 14's two-stage
        architecture fused INTO the traversal loop.
```

## 2. Raw data shape — the sector layout

The disk format (Microsoft's Rust DiskANN,
diskann-disk/src/storage/disk_index_writer.rs:362-390):

```text
block #1:   metadata: node_len, num_nodes_per_sector,
            vamana_frozen_point... (:366)
block #2..: num_nodes_per_block nodes per 4 KB block

packing rule (:385-390):
  node_len <= 4096: pack floor(4096/node_len) nodes per sector,
      never split a node ("if node_len is 600B, we can pack 6...
      and leave 496B unused")
  node_len >  4096: node spans consecutive sectors, padded to
      the next boundary
```

This is CSR (pattern 7) re-cut for the disk's atom: the unit of
locality is not the cache line but the 4 KB sector, and padding is
cheaper than a second read. Same logic as slotted pages in B-trees.

## 3. Why one flat graph — Vamana vs HNSW

```text
HNSW's hierarchy exists to shorten the ENTRY path; on disk each
layer hop is a random read, so layers multiply I/O. Vamana
instead builds ONE flat graph with LONG-RANGE edges baked in by
alpha-pruning, entered from a fixed "frozen" medoid point
(vamana_frozen_point in the metadata, disk_index_writer.rs:366).

alpha-pruning (the occlude rule, diskann/src/graph/index.rs:2625-2637):
  candidates sorted by distance; candidate i is REJECTED if some
  already-kept neighbor j has
      occlude_factor(i,j) = dist(i, query_node)/dist(i, j) ... > alpha
  i.e. j "occludes" i — you can reach i's region via j.
  alpha = 1.0 is exactly HNSW's diversity heuristic (pattern 13 §4).
  alpha > 1 (typ. 1.2) KEEPS some occluded edges — deliberately
  redundant long edges that cut average path length, so fewer
  hops = fewer disk reads. The implementation runs multiple
  passes with increasing alpha (current_alpha=1.0, increment
  toward configured alpha — index.rs:2598-2605, 2675).
```

The knob's meaning flips per medium: in RAM (HNSW) sparser graphs
win (cache); on disk denser-but-shorter wins (each hop is 100 us).

## 4. Step-by-step search

```text
1. load PQ codes for all N vectors into RAM; build the query's
   ADC table (pattern 14 §3)
2. beam = {medoid}; W = beam width (like ef)
3. loop:
     pick the unexpanded candidate with best PQ distance
     READ its sector (1 NVMe read, often batched W-wide)
     -> exact vector: rescore this node exactly
     -> neighbor IDs: score them with PQ (RAM), push into beam
   until beam's best unexpanded > worst kept result
4. return top k by EXACT distance (already rescored in-loop)
```

## 5. Worked example — 1B vectors, 768-d f32

```text
RAM:  PQ M=64 codes: 64 GB (fits a big box; SBQ/OPQ can halve it)
NVMe: node = 3072 B vector + 64 x 4 B neighbors = 3328 B
      -> 1 node/sector, padded: 4 KB x 1B = 4 TB on disk
search at W=8, ~4 hops average (alpha=1.2 keeps paths short):
      ~32 sector reads x 100 us, batched -> ~1-2 ms latency
      vs in-RAM HNSW: impossible (3+ TB), vs brute scan: minutes
recall ~0.95 at these settings — the published DiskANN operating
point family (dataset-dependent; the INVARIANT is: latency is
counted in sector reads, so everything optimizes hop count).
```

## 6. Worked example — why co-location beats split layouts

```text
split layout (graph file + vector file):
    per hop: 1 read for adjacency + 1 read for the vector = 2 I/O
co-located sector: 1 I/O per hop -> HALF the latency at identical
    recall. The entire disk format exists for this one arithmetic
    fact. (Same move as pattern 7 storing neighbors inline vs
    pointer-chasing, one level down the hierarchy.)
```

## 7. Where systems inherit this

- pgvectorscale re-implements the pattern inside Postgres: on-page
  graph (access_method/graph/), SBQ compression, plus cost
  estimation for the planner (cost_estimate.rs) — Vamana adapted
  to 8 KB Postgres pages instead of raw sectors.
- Milvus/knowhere ships DiskANN as its on-disk engine option next
  to HNSW/FAISS.
- JVector (Cassandra's vector index) uses Vamana-style flat graphs
  with in-line vectors for the same read-amplification reason.
- The fresh-update problem (inserts into a packed disk graph) is
  the LSM problem: DiskANN's answer (FreshDiskANN) is an in-RAM
  delta index + periodic merge — patterns 1/5's compaction and
  snapshot-flip discipline, re-derived for ANN.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| DiskANN | `reference-repos-corpus/DiskANN-src/diskann-disk/src/storage/disk_index_writer.rs` | the sector format: metadata block (366), packing/padding rules (385-390) |
| DiskANN | `reference-repos-corpus/DiskANN-src/diskann/src/graph/index.rs` | occlude_list alpha-pruning: rule (2625-2637), multi-pass alpha schedule (2598-2605, 2675) |
| DiskANN | `reference-repos-corpus/DiskANN-src/diskann-disk/src/search/` | PQ-steered beam search over sectors (pq/, search_mode.rs) |
| pgvectorscale | `reference-repos-corpus/pgvectorscale-src/pgvectorscale/src/access_method/` | Vamana on Postgres pages: graph/, cost_estimate.rs, meta_page.rs |
| knowhere | `reference-repos-corpus/knowhere-src` | DiskANN as Milvus's on-disk engine beside HNSW/FAISS |

## 9. Cross-references

- Sibling patterns: `hnsw-layered-greedy` (the in-RAM pole;
  alpha=1.0 recovers its heuristic); `product-quantization-ladder`
  (the RAM half of this design); `csr-adjacency-layout` (same
  locality argument, disk edition); `lsm-compaction-tradeoffs` +
  `cow-tree-snapshots` (FreshDiskANN's update story).
- Next in category: IVF partitioning (the clustering alternative),
  then the vector-ann synthesis pair.
- Verification note (docs_PRD06 thesis): the disk format is a
  FROZEN artifact — byte-level diffing of built indexes is
  possible if build seeds and thread counts are pinned; otherwise
  compare recall@k + mean hops + sectors/query, all cheaply
  observable counters.
- 202606 digest overlap: digests named DiskANN as the billion-scale
  option; this pair adds the sector packing rules, the occlude
  arithmetic with line cites, and the I/O cost models.

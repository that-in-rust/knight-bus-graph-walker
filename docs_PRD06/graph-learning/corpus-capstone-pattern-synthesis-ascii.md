# Corpus Capstone Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `corpus-capstone-pattern-synthesis-ascii.md` / `corpus-capstone-pattern-synthesis-mermaid.md` |
| One-line job | Distill all 8 categories (28 patterns) into one narrative: every serious data engine is the same five-layer machine — an immutable-artifact storage core, a compressed adjacency/posting layout, a pull-or-frontier execution loop, an approximation dial, and an oracle stack — and the corpus shows the same ~7 ideas reinvented in every category |

## 1. What this document rolls up

```text
category          patterns   synthesis pair
storage-engine    1-6        storage-engine-pattern-synthesis
graph-analytics   7-12       graph-analytics-pattern-synthesis
vector-ann        13-16      vector-ann-pattern-synthesis
full-text-search  17-19      full-text-search-pattern-synthesis
graph-db          20-22      graph-db-pattern-synthesis
neo4j-ecosystem   23-24      neo4j-ecosystem-pattern-synthesis
dataflow-compute  25-26      dataflow-compute-pattern-synthesis
bench-testing     27-28      bench-testing-pattern-synthesis
28 patterns, 8 syntheses, 172-repo corpus, every claim
file-cited. This capstone is the cross-category layer.
```

## 2. The five-layer machine

```text
every engine in the corpus decomposes the same way:
 L1 DURABILITY   immutable artifacts + a mutation log
    LSM SSTables (1), WAL group commit (2), COW roots (5),
    Lucene segments (17), HNSW-per-segment, differential
    traces (25), graph snapshots (this repo's own design)
 L2 LAYOUT       offsets-into-a-sorted-array, compressed
    CSR (7), posting blocks (17), FSTs (19), triple
    permutations (22), Vamana sectors (15), roaring (3)
 L3 EXECUTION    a loop that pulls or pushes small batches
    pull operators (21), frontier push/pull (8), supersteps
    (26), differential frontiers (25), WAND cursors (18)
 L4 APPROXIMATION a quality dial traded for speed/space
    bloom filters (6), PQ codes (14), HNSW ef (13), IVF
    nprobe (16), BM25 saturation (18), epsilon bands (28)
 L5 ORACLES      how the engine knows it is right
    metamorphic identities, history models, tolerant
    validation, recall (27-28), stub scripts (24)
the layers are not metaphor: in most corpus repos they are
literal module boundaries (rocksdb table/ vs db/, lucene
codecs/ vs search/, kuzu storage/ vs processor/).
```

## 3. The seven ideas every category reinvents

```text
idea                     appearances across categories
1 sorted-array + offset  CSR (7), postings (17), SSTable
  indirection            blocks (1), triple relations (22),
                         IVF lists (16)
2 immutable + merge      LSM compaction (1), Lucene merges
                         (17), differential trace compaction
                         (25), snapshot generations (5)
3 smallest-encoding      delta+bitpack postings (17),
  compression            PackStream markers (23), roaring
                         containers (3), quantization (14)
4 skip/prune during scan bloom (6), skip lists + block-max
                         WAND (18), HNSW upper layers (13),
                         frontier direction switch (8)
5 signed deltas as the   MVCC versions (4), differential
  unit of change         (data,time,diff) (25), tombstones
                         (1,17), graph tx streams
6 the partition IS the   vertex-cut 2*sqrt(P) (26), IVF
  performance decision   nlist (16), LSM levels (1), node
                         groups / CSR slack (20)
7 equality is a design   canonicalization, epsilon,
  decision               isomorphism, recall (27-28) —
                         the bench-testing keystone
memorize these seven and every repo in the corpus becomes
"which combination, with what constants?".
```

## 4. Worked example — one query through all layers

```text
"friends-of-friends of node 42, ranked by a text+vector score"
 L2: node 42 -> dense id via dictionary (FST-like);
     neighbors = CSR slice offsets[42..43] (7,20)
 L3: expand x2 as a pull pipeline: Scan->Expand->Expand->
     Dedup (21); dedup set = roaring bitmap (3)
 L4: text score via BM25+WAND over candidates' posting
     lists (18); vector score via HNSW ef=64 beam (13);
     fuse; exact rescoring of top-100 (vector synthesis)
 L1: all reads against one immutable snapshot (5) — no
     locks; concurrent writers append to the log (2,4)
 L5: parity gates: result-set diff vs stock engine with
     ORDER-BY canonicalization; recall>=0.95 on the vector
     leg; TLP identity on the WHERE clause (27,28)
counting: 42 has 100 friends, each 100 friends -> 10,000
candidate rows; roaring dedup -> ~8,000; WAND prunes scoring
to ~1,200 full evaluations; HNSW visits ~600 nodes of 10M.
every reduction above is one of the seven ideas.
```

## 5. Worked example — sizing the same engine's write path

```text
ingest 10k edges/sec while serving reads:
 - L1: group commit at 5ms windows -> 50 edges/fsync batch
   amortized (2); WAL bytes ~ 34B/edge record (20) -> 340KB/s
 - L2: edges land in a memtable/overlay; CSR base rebuilt
   by compaction when overlay > threshold (1's tradeoff:
   leveled = read-optimized, tiered = write-optimized)
 - L3: readers never block — they hold the old root (5) or
   old snapshot; visibility = three tiers (journal/overlay/
   base), which is exactly LSM read-path logic applied to
   graph topology
 - L4: bloom filters keep negative neighbor probes O(1) (6)
 - L5: incremental-vs-rebuild equality (25's self-oracle):
   CSR(base+overlay) must equal CSR(rebuilt-from-scratch)
this is this repo's own architecture (mmap snapshots +
visibility tiers) located inside the corpus's pattern space.
```

## 6. The convergence thesis, restated in corpus terms

```text
docs_PRD06 thesis: a known endpoint turns a rewrite into a
search problem. The corpus supplies both halves:
 - the ENDPOINT's anatomy: layers L1-L4 as they exist in
   neo4j/kuzu/memgraph source (patterns 20-22), the wire
   contract (23), the ecosystem harness (24)
 - the SEARCH's fitness function: L5 — four oracle families
   (27-28) + free assets (TCK, boltstub, Graphalytics
   rules, ann-benchmarks ground truth)
condition 1 (observability): the expensive residue is crash/
concurrency behavior — history models, not query diffs.
condition 2 (coverage): the harness, not the generated code,
is the durable asset. condition 3 (equality): pattern 28's
relation taxonomy IS the acceptance spec, per procedure.
```

## 7. Honest gaps of the whole corpus study

```text
- breadth over depth: each pattern cites 2-6 repos; 100+
  corpus repos are cloned but not yet pattern-mined
- concurrency internals (latching, lock-free indexes, io_uring
  paths) under-covered relative to their production weight
- query OPTIMIZERS (cost models, join ordering, WCOJ) have no
  dedicated pattern yet — the largest single omission
- distributed transactions/consensus intentionally out of
  scope (single-node focus per the repo's mission)
- GNN/embedding training (PyG/DGL rows) unmined
```

## 8. Citing repos (capstone cross-section)

| Repo | Path | Role |
| --- | --- | --- |
| rocksdb | `reference-repos-corpus/rocksdb-src/db/` | L1 exemplar (LSM, WAL, MVCC) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/processor/operator/physical_operator.h` | L3 pull pipeline |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/record-storage-engine/` | endpoint anatomy (records) |
| tantivy | `reference-repos-corpus/tantivy-src/src/` | L2/L4 (postings, FST, WAND kin) |
| faiss | `reference-repos-corpus/faiss-src/faiss/` | L4 (PQ/IVF/HNSW dials) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/collection.rs` | signed deltas (idea 5) |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/TLPWhereOracle.java` | L5 identity oracle |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EquivalenceValidationRule.java` | L5 equality taxonomy |

## 9. Cross-references

- Members: all 8 category syntheses; via them, patterns 1-28.
- The repo's own docs: `docs_PRD06/Rewrite-Sampling-And-
  Convergence-Thesis.md` (the why), `docs_PRD02` (the L1/L2
  design this study contextualizes).
- Suggested reading path for a newcomer: this capstone ->
  storage-engine synthesis -> graph-db synthesis ->
  bench-testing synthesis -> then any pattern that a real
  task touches.
- The capstone's one-line moral: engines differ in constants
  and combinations, not in ideas — learn the seven ideas and
  the five layers, and every new system is a diff, not a
  textbook.

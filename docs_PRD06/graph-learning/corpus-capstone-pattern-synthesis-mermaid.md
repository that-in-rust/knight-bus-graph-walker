# Corpus Capstone Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `corpus-capstone-pattern-synthesis-ascii.md` / `corpus-capstone-pattern-synthesis-mermaid.md` |
| One-line job | Distill all 8 categories (28 patterns) into one narrative: every serious data engine is the same five-layer machine — an immutable-artifact storage core, a compressed adjacency/posting layout, a pull-or-frontier execution loop, an approximation dial, and an oracle stack — and the corpus shows the same ~7 ideas reinvented in every category |

## 1. What this rolls up

```mermaid
flowchart TD
    CAP["corpus capstone<br/>(28 patterns, 172 repos)"]
    CAP --> C1["storage-engine (1-6)"]
    CAP --> C2["graph-analytics (7-12)"]
    CAP --> C3["vector-ann (13-16)"]
    CAP --> C4["full-text-search (17-19)"]
    CAP --> C5["graph-db (20-22)"]
    CAP --> C6["neo4j-ecosystem (23-24)"]
    CAP --> C7["dataflow-compute (25-26)"]
    CAP --> C8["bench-testing (27-28)"]
```

## 2. The five-layer machine

```mermaid
flowchart TD
    L1["L1 DURABILITY: immutable artifacts +<br/>mutation log — SSTables (1), WAL (2),<br/>COW roots (5), Lucene segments (17),<br/>differential traces (25)"]
    L2["L2 LAYOUT: offsets into sorted,<br/>compressed arrays — CSR (7), posting<br/>blocks (17), FSTs (19), triple<br/>permutations (22), Vamana sectors (15)"]
    L3["L3 EXECUTION: a loop pulling/pushing<br/>small batches — pull operators (21),<br/>frontier switch (8), supersteps (26),<br/>WAND cursors (18)"]
    L4["L4 APPROXIMATION: a quality dial —<br/>bloom (6), PQ (14), ef (13),<br/>nprobe (16), epsilon bands (28)"]
    L5["L5 ORACLES: identities, history<br/>models, tolerant validation, recall<br/>(27-28), stub scripts (24)"]
    L1 --> L2 --> L3 --> L4 --> L5
    L5 --> LIT["not metaphor: literal module<br/>boundaries — rocksdb table/ vs db/,<br/>lucene codecs/ vs search/,<br/>kuzu storage/ vs processor/"]
```

## 3. The seven ideas every category reinvents

```mermaid
flowchart TD
    I1["1 sorted-array + offset indirection:<br/>CSR, postings, SSTable blocks,<br/>triple relations, IVF lists"]
    I2["2 immutable + merge:<br/>LSM compaction, Lucene merges,<br/>trace compaction, snapshot generations"]
    I3["3 smallest-encoding compression:<br/>delta+bitpack, PackStream markers,<br/>roaring containers, quantization"]
    I4["4 skip/prune during scan:<br/>bloom, block-max WAND, HNSW upper<br/>layers, direction switching"]
    I5["5 signed deltas as the unit of change:<br/>MVCC versions, (data,time,diff),<br/>tombstones, tx streams"]
    I6["6 the partition IS the performance<br/>decision: vertex-cut 2*sqrt(P), nlist,<br/>LSM levels, CSR node groups"]
    I7["7 equality is a design decision:<br/>canonicalization, epsilon,<br/>isomorphism, recall"]
    I1 & I2 & I3 & I4 & I5 & I6 & I7 --> KEY["every corpus repo = 'which combination,<br/>with what constants?'"]
```

## 4. One query through all layers

```mermaid
flowchart TD
    Q["friends-of-friends of node 42,<br/>ranked by text+vector score"]
    Q --> S2["L2: id 42 -> CSR slice<br/>offsets[42..43] (7,20)"]
    S2 --> S3["L3: Scan->Expand->Expand->Dedup<br/>pull pipeline (21);<br/>dedup = roaring bitmap (3)"]
    S3 --> S4["L4: BM25+WAND prunes scoring (18);<br/>HNSW ef=64 beam (13);<br/>exact rescore of top-100"]
    S4 --> S1["L1: all reads on one immutable<br/>snapshot (5); writers append<br/>to the log (2,4)"]
    S1 --> S5["L5: result diff vs stock engine +<br/>ORDER-BY canonicalization;<br/>recall >= 0.95; TLP on WHERE (27,28)"]
    S5 --> NUM["counting: 100x100 = 10,000 candidates<br/>-> roaring dedup ~8,000 -> WAND ~1,200<br/>full scores -> HNSW visits ~600 of 10M —<br/>each reduction is one of the seven ideas"]
```

## 5. The same engine's write path

```mermaid
flowchart TD
    W["ingest 10k edges/sec<br/>while serving reads"]
    W --> W1["L1: group commit, 5ms windows -><br/>~50 edges/fsync (2);<br/>~34B/edge record (20) -> 340KB/s WAL"]
    W --> W2["L2: edges land in overlay/memtable;<br/>CSR base rebuilt by compaction (1's<br/>leveled-vs-tiered tradeoff)"]
    W --> W3["L3: readers hold the old root (5) —<br/>never block; visibility tiers<br/>journal/overlay/base = LSM read path<br/>applied to topology"]
    W --> W4["L4: bloom keeps negative neighbor<br/>probes O(1) (6)"]
    W --> W5["L5: self-oracle (25):<br/>CSR(base+overlay) ==<br/>CSR(rebuilt-from-scratch)"]
    W5 --> OWN["this is this repo's own mmap-snapshot<br/>architecture, located inside the<br/>corpus's pattern space"]
```

## 6. The convergence thesis in corpus terms

```mermaid
flowchart LR
    EP["ENDPOINT anatomy:<br/>L1-L4 in neo4j/kuzu/memgraph<br/>source (20-22), wire contract (23),<br/>ecosystem harness (24)"]
    FF["FITNESS FUNCTION:<br/>L5 — four oracle families (27-28)<br/>+ free assets: TCK, boltstub,<br/>Graphalytics rules, ann-benchmarks"]
    EP --> LOOP["generate -> diff -> regenerate"]
    FF --> LOOP
    LOOP --> COND["condition 1: crash/concurrency needs<br/>history models, not query diffs;<br/>condition 2: the harness is the durable<br/>asset; condition 3: pattern 28's relation<br/>taxonomy IS the acceptance spec"]
```

## 7. Honest gaps of the whole study

```mermaid
flowchart TD
    G["corpus-wide gaps"]
    G --> G1["breadth over depth: 100+ cloned<br/>repos not yet pattern-mined"]
    G --> G2["concurrency internals (latching,<br/>lock-free, io_uring) under-covered"]
    G --> G3["query OPTIMIZERS (cost models, join<br/>ordering, WCOJ): largest omission —<br/>no dedicated pattern yet"]
    G --> G4["distributed txn/consensus: out of<br/>scope (single-node mission)"]
    G --> G5["GNN/embedding training<br/>(PyG/DGL rows) unmined"]
```

## 8. Reading path

```mermaid
flowchart LR
    R0["this capstone"]
    R0 --> R1["storage-engine synthesis"]
    R1 --> R2["graph-db synthesis"]
    R2 --> R3["bench-testing synthesis"]
    R3 --> R4["then any pattern a real<br/>task touches"]
    R4 --> MORAL["moral: engines differ in constants and<br/>combinations, not ideas — learn 7 ideas<br/>+ 5 layers, and every new system is a<br/>diff, not a textbook"]
```

## 8b. Where each category earns its keep

```mermaid
flowchart TD
    ROLE["what each category contributes<br/>to the five-layer machine"]
    ROLE --> A["storage-engine: L1 wholesale —<br/>durability, versions, merges;<br/>every other category builds on it"]
    ROLE --> B["graph-analytics + vector-ann +<br/>full-text-search: L2-L4 for their<br/>data shape — adjacency, vectors,<br/>terms; same seven ideas, different<br/>constants"]
    ROLE --> C["graph-db: the INTEGRATION layer —<br/>where L1-L4 meet a query language<br/>and a transaction model"]
    ROLE --> D["neo4j-ecosystem: the CONTRACT layer —<br/>bytes and conversations that turn N<br/>implementations into one ecosystem"]
    ROLE --> E["dataflow-compute: L3 generalized —<br/>iteration and incrementality as<br/>first-class mechanisms"]
    ROLE --> F["bench-testing: L5 — the keystone;<br/>without it the other layers are<br/>claims, not knowledge"]
```

## 9. Citing repos (capstone cross-section)

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

## 10. Cross-references

- Members: all 8 category syntheses; via them, patterns 1-28.
- Repo docs: `docs_PRD06/Rewrite-Sampling-And-Convergence-
  Thesis.md` (the why); `docs_PRD02` (the L1/L2 design this
  study contextualizes).
- The ASCII twin carries the same two worked examples with
  full arithmetic and the idea-by-category appearance table.
- Capstone exam: pick any repo in the 172-row ledger and name
  (a) its five layers by module path, (b) which of the seven
  ideas it uses at each layer and with what constants, and
  (c) the oracle family that would gate a rewrite of it —
  if the knowledgebase did its job, all three are answerable
  from the syntheses without opening the repo.

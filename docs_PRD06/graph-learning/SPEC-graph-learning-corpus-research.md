# SPEC: Graph Learning Corpus Research

| Field | Value |
| --- | --- |
| Version | 2 — rebuilt on the completed Phase A research (three rounds, 172 verified repos) |
| Status | Active — Phase A DONE; Phase B (clone + map) next |
| Owner | That in Rust |
| Executor | Devin sessions attached to this repo |
| Home | `docs_PRD06/graph-learning/` |
| Kind | Executable research spec (codexplorer-style): REQ contracts, measurable exit gates, traceable output ledger |

## 1. Purpose

Build an in-depth, evidence-grounded education corpus about graph systems —
and their neighbors in vector search, full-text search, storage engines, and
dataflow compute — by:

1. holding a **verified 172-repository corpus** (done: `corpus-ledger.tsv`);
2. **shallow-cloning every corpus repo** and mapping each with the installed
   code-exploration tools;
3. extracting the **fundamental algorithm patterns**, **storage patterns**,
   and **execution patterns** (how the algorithms are actually run);
4. publishing findings as **paired documents** — one ASCII document and one
   Mermaid document per pattern — with **four-word file names**, accumulating
   in this folder as the research proceeds.

End state: a pattern library that teaches graphs in depth from real source
code, not textbook prose — cross-referenced to the papers ledger and the
proprietary-endpoint landscape.

## 2. Phase A — DONE (evidence base)

Three research rounds are complete and committed:

| Artifact | Content |
| --- | --- |
| `corpus-ledger.tsv` | 172 repos, 8 categories, all GitHub-API-verified (stars, last push, language, archived flag, local-clone path) |
| `corpus-research-findings.md` | The three rounds: topic-search round (117), glossary-seeded round (154), gap-focused round (172: dataflow-compute, bench-testing, RDF/SPARQL, roaring/succinct) |
| `domain-keywords-glossary.md` | The domain vocabulary, 8 sections, ★-marked terms central to this repo's thesis |
| `research-papers-ledger.md` | 18 arXiv-verified papers + canonical venue papers, cross-linked to corpus repos |
| `proprietary-tools-landscape.md` | Closed systems (Neptune, Pinecone, Turbopuffer, ScaNN…) treated as behavior-endpoints, not source |

Verified corpus composition (REQ-GLC-001/002 satisfied):

| Category | Repos | Notes |
| --- | ---: | --- |
| graph-db | 36 | incl. RDF/SPARQL (QLever, Virtuoso, gStore) and Datalog (cozo, datascript) |
| graph-analytics | 33 | incl. out-of-core classics, GraphBLAS family, GNN frameworks (PyG, DGL) |
| storage-engine | 33 | incl. roaring/succinct kernels (CRoaring, sdsl-lite) |
| vector-ann | 25 | incl. Rust HNSW impls, knowhere, pgvectorscale |
| neo4j-ecosystem | 18 | drivers, APOC, openCypher spec, testkit, Cypher parsers |
| full-text-search | 17 | Lucene family + Tantivy/Meilisearch/Typesense peers |
| dataflow-compute | 6 | timely/differential-dataflow, Flink, Spark, Velox, datafrog |
| bench-testing | 4 | SQLancer, ann-benchmarks, Jepsen, LDBC Graphalytics |
| **Total** | **172** | 40 already cloned locally |

Corpus is declared **saturated**: further additions are collection, not
learning. New repos enter only by replacing a dead/renamed row.

## 3. Scope

### In scope

- Everything in the 8 ledger categories; reading-only research.
- **Shallow clones (`--depth 1`) of ALL 172 corpus repos** — including the
  40 already on disk, which are reused in place and never re-cloned.
- Pattern extraction with file-path citations into local clones.

### Out of scope

- Running or benchmarking the systems.
- Modifying any cloned reference repo.
- Full-history clones (`--depth 1` only; disk budget §8).
- Proprietary systems as *source* (they appear only as behavior-endpoints
  per `proprietary-tools-landscape.md`).

## 4. Prior Assets (do not redo)

| Asset | Location | Reuse |
| --- | --- | --- |
| 40 reference repos already shallow-cloned (~3.4 GB) | `reference-repos-neo4j-family/` (20), `reference-repos-competitors/` (20) | Count toward the 172; ledger `local_clone` column records each path; never re-clone |
| 5-layer pattern digests from the 202606 study | `graph-database-rewrite-references-202606/` | Seed vocabulary; new docs must extend, not repeat |
| Installed exploration tools + skills | `.agents/skills/` (11 tools) | The mapping pipeline of §7 |
| Algorithm explainers | `docs_PRD04/AlgoExplainers-ASCII.md` | Style baseline for ASCII/Mermaid pairs |

## 5. Requirements

Format: `REQ-GLC-<NNN>` (Graph Learning Corpus), WHEN/THEN/SHALL.

### Phase A — Corpus selection (SATISFIED)

- **REQ-GLC-001** ✅ WHEN the candidate list is built THEN it SHALL contain
  at least 100 repositories, each with GitHub URL, star count, last-push
  date, primary language, and category in `corpus-ledger.tsv`.
  *Met: 172 rows, all API-verified.*
- **REQ-GLC-002** ✅ WHEN a repo is proposed THEN it SHALL meet liveness and
  substance gates: a push within 24 months OR historical-classic status
  (flagged in the `flags` column), and real engine/library substance.
  *Met: classics and archived repos flagged; 95%-noise topic hits rejected.*
- **REQ-GLC-003** WHEN the list is complete THEN it SHALL be presented to
  the owner for approval before mass cloning. *Presented at 117, 154, and
  172; owner directed spec creation — cloning starts on explicit go.*

### Phase B — Acquisition and mapping

- **REQ-GLC-010** WHEN a corpus repo is acquired THEN it SHALL be cloned
  `--depth 1` into `reference-repos-corpus/<name>-src/` and its ledger row
  updated with clone date and on-disk size. Every one of the 172 repos SHALL
  end up with a local shallow clone: the 40 pre-existing clones satisfy this
  in place; the ~132 new ones are cloned in category batches. Total volume
  SHALL respect §8.
- **REQ-GLC-011** WHEN a repo is mapped THEN at minimum one structural index
  SHALL be built (mcp-codebase-index or cocoindex-code for breadth;
  GitNexus/tessera/Serena where precision pays) and three artifacts captured
  per ledger row: top-level module map, core entry points, storage-layer
  directory.
- **REQ-GLC-012** WHEN mapping fails (tool/parser limits — expected for the
  giant JVM/C++ repos) THEN the failure SHALL be recorded, a manual skim
  SHALL substitute, and the repo SHALL NOT be silently dropped.
- **REQ-GLC-013** WHEN any repo is acquired — including the giants (Spark,
  Flink, Elasticsearch, Milvus, Velox) — THEN it SHALL be a full-tree
  `git clone --depth 1` (owner directive: everything is a plain shallow
  clone; no sparse-checkout, no blob filters). If a shallow clone still
  exceeds the per-repo cap of §8, the size SHALL be recorded in the ledger
  and the owner informed before proceeding with further batches.

### Phase C — Pattern extraction

- **REQ-GLC-020** WHEN a pattern is extracted THEN it SHALL be one of three
  kinds — `algorithm`, `storage`, or `execution` — and SHALL cite at least
  **two** corpus repos implementing it, with file paths into local clones.
- **REQ-GLC-021** WHEN a pattern claim is written THEN every factual claim
  about a repo SHALL be traceable to a file path (weak-model contract: no
  claims from memory, only from inspected source). Paper citations SHALL use
  the verified IDs in `research-papers-ledger.md`; proprietary systems may
  be cited descriptively only.
- **REQ-GLC-022** WHEN a pattern duplicates one already documented in
  `graph-database-rewrite-references-202606/` THEN the new doc SHALL link
  the old one and add only cross-corpus deltas.

### Phase D — Publication contract

- **REQ-GLC-030** WHEN a pattern is published THEN it SHALL produce exactly
  two MD files in this folder, four-word names
  `<noun>-<noun>-<noun>-<form>.md`, form ∈ {ascii, mermaid}: e.g.
  `csr-adjacency-layout-ascii.md` + `csr-adjacency-layout-mermaid.md`.
- **REQ-GLC-031** WHEN an ASCII doc is written THEN it SHALL contain: the
  pattern's job, raw data shape, memory/disk layout in ASCII, step-by-step
  walkthrough, two worked numeric examples, and the citing-repos table
  (repo, path, one-line role).
- **REQ-GLC-032** WHEN a Mermaid doc is written THEN it SHALL render the
  same pattern as Mermaid diagrams plus the same citing-repos table; each
  of the pair SHALL stand alone.
- **REQ-GLC-033** WHEN any doc is added THEN `pattern-index.md` SHALL be
  updated in the same commit: pattern name, kind, repos cited, pair, date.

### Phase E — Cadence and integrity

- **REQ-GLC-040** WHEN a session works this spec THEN it SHALL commit
  completed doc pairs incrementally and push to the working branch.
- **REQ-GLC-041** WHEN a category completes THEN a synthesis pair
  (`<category>-pattern-synthesis-{ascii,mermaid}.md`) SHALL summarize the
  dominant patterns of that category.

## 6. Pattern backlog (seeded from Phase A evidence)

Initial extraction targets, ordered by cross-category reach (each already
has ≥2 known implementations in the corpus; final citations at write time):

| # | Candidate pattern | Kind | Likely witnesses |
| --- | --- | --- | --- |
| 1 | CSR adjacency layout | storage | ligra, gbbs, kuzu, cugraph, this repo |
| 2 | HNSW greedy descent | algorithm | hnswlib, qdrant, instant-distance, jvector |
| 3 | Graph-on-disk memory budget | execution | DiskANN, pgvectorscale, graphchi, GridGraph |
| 4 | LSM compaction tradeoff | storage | rocksdb, pebble, fjall, mini-lsm, slatedb |
| 5 | Push-pull frontier switching | execution | ligra, gapbs, gbbs, networkit |
| 6 | Posting-list skip compression | storage | lucene, tantivy, RediSearch, RUM |
| 7 | Immutable segment merging | execution | lucene, tantivy, quickwit + graph snapshots here |
| 8 | Sparse-matrix graph algebra | algorithm | GraphBLAS, LAGraph, falkordb, graphblast |
| 9 | Roaring bitmap ID sets | storage | CRoaring, roaring-rs, lucene, pilosa-descendants |
| 10 | WAL group commit discipline | storage | rocksdb, sqlite, tikv, neo4j |
| 11 | MVCC snapshot visibility | execution | memgraph, tikv, lmdb, datascript |
| 12 | Copy-on-write tree snapshots | storage | lmdb, sled, aspen(parlaylib), datascript |
| 13 | Product quantization compression | algorithm | faiss, knowhere, cuvs, qdrant |
| 14 | Vertex-centric superstep scheduling | execution | giraph, graphscope, flink-gelly, pregel-descendants |
| 15 | Differential incremental computation | execution | differential-dataflow, feldera, materialize-lineage |
| 16 | Two-phase query planning | execution | neo4j, kuzu, memgraph, cozo |
| 17 | Louvain-Leiden community refinement | algorithm | gds, networkit, igraph, cugraph |
| 18 | Delta-varint edge encoding | storage | webgraph(+rs), lucene, tantivy, snap |
| 19 | Bloom-filter read shortcut | storage | rocksdb, pebble, badger, quickwit |
| 20 | Differential oracle testing | execution | sqlancer, testkit, ann-benchmarks, ldbc impls |

Count floats: fundamental patterns only; expected 30–50 pairs at completion.

## 7. Pipeline (per repo)

```text
select (ledger row, category batch)
   |
   v
clone --depth 1  ->  reference-repos-corpus/<name>-src/      [REQ-GLC-010/013]
   |                 (40 legacy clones reused in place)
   v
map: mcp-codebase-index / cocoindex-code (breadth)           [REQ-GLC-011]
     GitNexus / tessera / serena (precision when needed)
   |
   v
locate: engine entry points, storage layer, algorithm kernels
   |
   v
extract patterns (>=2-repo rule)                             [REQ-GLC-020..022]
   |
   v
publish four-word ASCII + Mermaid pair, update index         [REQ-GLC-030..033]
   |
   v
commit + push incrementally                                  [REQ-GLC-040]
```

Batch order (interleaved: map a category → write its patterns → next):

1. storage-engine (grounds every later "X sits on RocksDB" sentence)
2. graph-analytics (algorithm kernels; most patterns per repo-hour)
3. vector-ann (HNSW/DiskANN — the graph algorithms in disguise)
4. full-text-search (segment/posting patterns)
5. graph-db (largest repos; mapped last, informed by all prior vocabulary)
6. neo4j-ecosystem + dataflow-compute + bench-testing (surface + harness)

## 8. Budgets

| Budget | Value | Rationale |
| --- | --- | --- |
| Disk for new clones | ≤ 50 GB total; check `df` before each batch | ~132 new shallow clones; existing 40 ≈ 3.4 GB |
| Per-repo size cap | none — all plain `--depth 1` (REQ-GLC-013); sizes recorded, owner informed if a clone is unusually large | spark/flink/elasticsearch shallow-clone fine (~1-2 GB each) |
| Repos per session batch | 10–15 mapped per session | incremental, reviewable commits |
| Pattern doc size | 150–400 lines per file | deep enough to teach, short enough to read |
| Evidence per pattern | ≥ 2 repos, file-path cited | REQ-GLC-020 |

## 9. Verification Matrix

| req_id | check | how verified |
| --- | --- | --- |
| REQ-GLC-001/002 | ledger completeness + gates | every row has url/stars/date/lang/category; API-sourced ✅ |
| REQ-GLC-003 | owner approval | recorded in ledger header / session log |
| REQ-GLC-010/013 | clone discipline | `du -sh` per clone in ledger; sum under budget; partial flags recorded |
| REQ-GLC-011/012 | mapping artifacts | ledger row has module map + entry points + storage dir, or recorded failure+skim |
| REQ-GLC-020/021 | citation integrity | spot-check: every cited path exists in the local clone |
| REQ-GLC-030..033 | publication contract | docs in pairs, four-word names, index updated same commit |
| REQ-GLC-041 | category syntheses | one pair per completed category |

## 10. Deliverable Shape Of This Folder (end state)

```text
docs_PRD06/graph-learning/
  SPEC-graph-learning-corpus-research.md   (this file)
  corpus-ledger.tsv                        (172 approved rows + clone/map columns)
  corpus-research-findings.md              (Phase A record)
  domain-keywords-glossary.md
  research-papers-ledger.md
  proprietary-tools-landscape.md
  pattern-index.md                         (running index of all pairs)
  csr-adjacency-layout-ascii.md            (pattern pairs, four-word names)
  csr-adjacency-layout-mermaid.md
  hnsw-greedy-descent-ascii.md
  hnsw-greedy-descent-mermaid.md
  ...
  storage-engine-pattern-synthesis-ascii.md  (category syntheses)
  storage-engine-pattern-synthesis-mermaid.md
```

## 11. Decisions (formerly open questions — resolved defaults)

1. **Corpus mix**: fixed by evidence — the verified 8-category split of §2
   (owner may still trim rows).
2. **Classics policy**: archived/classic repos stay, flagged in the ledger
   (`classic`, `archived`) — they carry the cleanest teaching code.
3. **Clone location**: single `reference-repos-corpus/` sibling folder for
   all new clones; the two legacy folders remain untouched; all three are
   gitignored working material.
4. **Pattern granularity**: count floats; §6 backlog is the seed; expected
   30–50 fundamental pairs.
5. **Sequencing**: interleave per §7 batch order.
6. **Everything shallow**: all 172 repos get a local `--depth 1` clone
   (owner directive) — nothing is studied API-only.

# SPEC: Graph Learning Corpus Research

| Field | Value |
| --- | --- |
| Status | Draft for approval — open questions in §10 |
| Owner | That in Rust |
| Executor | Devin sessions attached to this repo |
| Home | `docs_PRD06/graph-learning/` |
| Kind | Executable research spec (codexplorer-style): every phase has REQ contracts, measurable exit gates, and a traceable output ledger |

## 1. Purpose

Build an in-depth, evidence-grounded education corpus about graph systems —
and their neighbors in vector search and full-text search — by:

1. identifying the **top ~100 repositories** across graph databases, graph
   analytics engines, vector search, and the Elasticsearch/OpenSearch/Lucene
   family;
2. shallow-cloning and **mapping** each with the installed code-exploration
   tools;
3. extracting the **fundamental algorithm patterns**, **storage patterns**,
   and **execution patterns** (how the algorithms are actually run);
4. publishing findings as **paired documents** — one ASCII document and one
   Mermaid document per pattern — with **four-word file names**, accumulating
   in this folder as the research proceeds.

The end state is a pattern library that teaches graphs in depth from real
source code, not textbook prose.

## 2. Scope

### In scope

- Graph databases (OLTP + OLAP), graph analytics/compute engines, graph
  libraries.
- Vector search engines and ANN libraries.
- Full-text search: the Lucene family (Elasticsearch, OpenSearch, Solr) and
  non-JVM peers (Tantivy, Meilisearch, Typesense, Quickwit).
- The storage engines these systems sit on (LSM/B-tree/KV: RocksDB, LMDB,
  sled, etc.) — because storage patterns are half the assignment.

### Out of scope

- Running or benchmarking the systems (reading-only research).
- Modifying any cloned reference repo.
- Deep-cloning full history (`--depth 1` only; disk is a budget, §7).

## 3. Prior Assets (do not redo)

| Asset | Location | Reuse |
| --- | --- | --- |
| 40 reference repos already cloned | `reference-repos-neo4j-family/` (20), `reference-repos-competitors/` (20) | Count toward the 100; never re-clone |
| 5-layer pattern digests from the 202606 study | `graph-database-rewrite-references-202606/` (patterns 1-5, meta, supermeta, coverage ledgers) | Seed vocabulary; new docs must extend, not repeat, these |
| Installed exploration tools + skills | `.agents/skills/` (11 tools) | The mapping pipeline of §6 |
| Algorithm explainers | `docs_PRD04/AlgoExplainers-ASCII.md` | Style baseline for ASCII/Mermaid pairs |

## 4. Requirements

Format: `REQ-GLC-<NNN>` (Graph Learning Corpus), WHEN/THEN/SHALL.

### Phase A — Corpus selection

- **REQ-GLC-001** WHEN the candidate list is built THEN it SHALL contain at
  least 100 repositories spanning all five categories of §5, each with a
  recorded GitHub URL, star count, last-commit date, primary language, and
  category — captured in `corpus-ledger.tsv` in this folder.
- **REQ-GLC-002** WHEN a repo is proposed THEN it SHALL meet liveness and
  substance gates: a commit within 24 months OR historical-classic status
  (explicitly flagged, e.g. GraphChi, Ligra), and it must contain a real
  engine/library (no awesome-lists, no tutorials, no wrappers).
- **REQ-GLC-003** WHEN the list is complete THEN it SHALL be presented to the
  owner for approval before mass cloning begins.

### Phase B — Acquisition and mapping

- **REQ-GLC-010** WHEN a corpus repo is acquired THEN it SHALL be cloned
  `--depth 1` into `reference-repos-corpus/<name>-src/` and recorded in the
  ledger with clone date and on-disk size; total new clone volume SHALL stay
  under the disk budget of §7.
- **REQ-GLC-011** WHEN a repo is mapped THEN at minimum one structural index
  SHALL be built over it (mcp-codebase-index or cocoindex-code for breadth;
  GitNexus/tessera where call-graph precision pays) and three artifacts SHALL
  be captured in the ledger row: top-level module map, entry points of the
  core engine, and the storage-layer directory.
- **REQ-GLC-012** WHEN mapping fails on a repo (parser/tool limits) THEN the
  failure SHALL be recorded in the ledger, a manual skim SHALL substitute,
  and the repo SHALL NOT be silently dropped.

### Phase C — Pattern extraction

- **REQ-GLC-020** WHEN a pattern is extracted THEN it SHALL be one of three
  kinds — `algorithm` (e.g. BFS frontier compaction, HNSW greedy descent),
  `storage` (e.g. CSR layout, LSM compaction, posting-list compression), or
  `execution` (e.g. vertex-centric scheduling, pull/push switching, segment
  merging) — and SHALL cite at least **two** corpus repos implementing it,
  with file paths into the local clones.
- **REQ-GLC-021** WHEN a pattern claim is written THEN every factual claim
  about a repo SHALL be traceable to a file path (weak-model contract: no
  claims from memory of a codebase, only from inspected source).
- **REQ-GLC-022** WHEN a pattern duplicates one already documented in
  `graph-database-rewrite-references-202606/` THEN the new doc SHALL link the
  old one and add only cross-corpus deltas (e.g. how vector engines reuse a
  graph-storage trick).

### Phase D — Publication contract

- **REQ-GLC-030** WHEN a pattern is published THEN it SHALL produce exactly
  two MD files in this folder, named with the **four-word convention**
  `<noun>-<noun>-<noun>-<form>.md` where form is `ascii` or `mermaid`:
  e.g. `csr-adjacency-layout-ascii.md` + `csr-adjacency-layout-mermaid.md`.
- **REQ-GLC-031** WHEN an ASCII doc is written THEN it SHALL contain: the
  pattern's job, the raw data shape, the memory/disk layout drawn in ASCII,
  a step-by-step walkthrough, two worked examples with real numbers, and the
  citing-repos table (repo, path, one-line role).
- **REQ-GLC-032** WHEN a Mermaid doc is written THEN it SHALL contain the
  same pattern rendered as Mermaid diagrams (flowchart/sequence/state as
  fits) plus the same citing-repos table; the pair SHALL agree — any reader
  can use either alone.
- **REQ-GLC-033** WHEN any doc is added THEN `pattern-index.md` in this
  folder SHALL be updated in the same commit: pattern name, kind, repos
  cited, doc pair, date.

### Phase E — Cadence and integrity

- **REQ-GLC-040** WHEN a session works this spec THEN it SHALL commit
  completed doc pairs incrementally (not one giant end dump) and push to the
  working branch.
- **REQ-GLC-041** WHEN the corpus study completes a category THEN a category
  synthesis pair (`<category>-pattern-synthesis-{ascii,mermaid}.md`) SHALL
  summarize which patterns dominate that category and why.

## 5. The Corpus: Categories and Candidate Slate

Target ≈100 repos. The 40 local repos count. The slate below is the working
proposal — **approval gate REQ-GLC-003 applies**. Star counts to be verified
via GitHub API at execution time (REQ-GLC-001), not trusted from memory.

### Category 1 — Graph databases (~30; 24 already local)

Local: neo4j, kuzu, memgraph, falkordb, dgraph, nebula, arangodb, janusgraph,
tugraph, apache-age, duckpgq, graphscope + the neo4j-family satellite repos.
Add: OrientDB, HugeGraph, Cayley, TypeDB, TerminusDB, IndraDB (Rust),
Grafeo/other young Rust engines, AWS-style openCypher tooling.

### Category 2 — Graph analytics and compute engines (~20; 8 local)

Local: ligra, gbbs, graphchi-cpp, flashx, networkit, networkx, igraph, snap,
cugraph. Add: GraphBLAS (SuiteSparse), LAGraph, Gunrock, Galois, GraphIt,
Giraph, GraphX (spark subtree), Pregel-descendants, KaHIP/METIS partitioners,
WebGraph (+ Rust port), Aspen.

### Category 3 — Vector search and ANN (~20)

Milvus, Qdrant, Weaviate, Vespa, Chroma, LanceDB + lance format, pgvector,
FAISS, hnswlib, ScaNN, Annoy, DiskANN, NMSLIB, usearch, voyager, RAFT/cuVS.
Rationale: HNSW **is** a graph algorithm on a designed graph; DiskANN is the
low-RAM graph-on-disk thesis in production — directly relevant to PRD05.

### Category 4 — Full-text search (~15)

Lucene, Elasticsearch, OpenSearch, Solr, Tantivy, Meilisearch, Typesense,
Quickwit, Sonic, Bleve, Xapian, Manticore. Rationale: posting lists, skip
lists, segment merge, and doc-value columnar layouts are the storage patterns
the assignment asks for, in their most battle-tested form.

### Category 5 — Storage engines underneath (~15)

RocksDB, LevelDB, LMDB, WiredTiger, sled, redb, fjall, Pebble, BadgerDB,
FoundationDB, TiKV, heed, SQLite (btree.c as the canonical B-tree). These are
cited constantly by categories 1-4; reading them once grounds every
"X stores it in RocksDB" sentence.

## 6. Pipeline (per repo)

```text
select (ledger row exists, category assigned)
   |
   v
clone --depth 1  ->  reference-repos-corpus/<name>-src/     [REQ-GLC-010]
   |
   v
map: mcp-codebase-index / cocoindex-code (breadth)          [REQ-GLC-011]
     GitNexus / tessera (call-graph precision, when needed)
     serena (symbol-precise reading of hot files)
   |
   v
locate: core engine entry points, storage layer, algorithm kernels
   |
   v
extract patterns (>=2-repo rule)                            [REQ-GLC-020..022]
   |
   v
publish four-word ASCII + Mermaid pair, update index        [REQ-GLC-030..033]
   |
   v
commit + push incrementally                                 [REQ-GLC-040]
```

Tool notes (from the installed skills): mcp-codebase-index is regex-based for
Rust/C++ — fine for breadth mapping; use Serena/GitNexus when precision on a
specific kernel matters. CodeGraphContext indexes code dirs individually.
Add `reference-repos-corpus/` to the same gitignore treatment as the existing
reference folders (clones are local working material, never committed).

## 7. Budgets

| Budget | Value | Rationale |
| --- | --- | --- |
| Disk for new clones | ≤ 40 GB total, checked before each clone batch | ~60 new repos, most < 500 MB shallow |
| Repos per session batch | 10-15 mapped per session | Keeps commits incremental and reviewable |
| Pattern doc size | 150-400 lines per file | Deep enough to teach; short enough to read |
| Minimum corpus evidence per pattern | 2 repos, file-path cited | REQ-GLC-020 |

## 8. Verification Matrix

| req_id | check | how verified |
| --- | --- | --- |
| REQ-GLC-001/002 | ledger completeness + gates | script: every row has url/stars/date/lang/category; stars from API not memory |
| REQ-GLC-003 | owner approval | recorded decision in ledger header |
| REQ-GLC-010 | clone discipline | `du -sh` per clone in ledger; sum under budget |
| REQ-GLC-011/012 | mapping artifacts | ledger row has module map + entry points + storage dir, or a recorded failure+skim |
| REQ-GLC-020/021 | citation integrity | spot-check: every cited path exists in the local clone |
| REQ-GLC-030..033 | publication contract | CI-style check: docs come in pairs, four-word names, index updated same commit |
| REQ-GLC-041 | category syntheses | one pair per completed category |

## 9. Deliverable Shape Of This Folder (end state)

```text
docs_PRD06/graph-learning/
  SPEC-graph-learning-corpus-research.md   (this file)
  corpus-ledger.tsv                        (100+ rows, the approved corpus)
  pattern-index.md                         (running index of all pairs)
  csr-adjacency-layout-ascii.md            (example pair)
  csr-adjacency-layout-mermaid.md
  hnsw-greedy-descent-ascii.md
  hnsw-greedy-descent-mermaid.md
  lsm-compaction-tradeoff-ascii.md
  lsm-compaction-tradeoff-mermaid.md
  ...
  vector-search-pattern-synthesis-ascii.md (category syntheses)
  vector-search-pattern-synthesis-mermaid.md
```

## 10. Open Questions (owner decisions before Phase B)

1. **Corpus mix**: is the §5 category split (30/20/20/15/15) the right
   weighting, or should vector search / FTS get more slots at the expense of
   more graph DBs?
2. **Classics policy**: include archived-but-canonical repos (GraphChi,
   X-Stream, Annoy) under the historical-classic flag — yes/no?
3. **Clone location**: `reference-repos-corpus/` as a new sibling folder
   (proposed), or split into per-category folders matching the existing
   naming (`reference-repos-vector/`, `reference-repos-fts/`, ...)?
4. **Pattern granularity**: aim for ~30-50 pattern pairs total (fundamental
   patterns only), or let the count float as extraction proceeds?
5. **Depth-first or breadth-first**: map all 100 shallowly before writing any
   pattern docs, or interleave (map a category → write its patterns → next
   category)? Spec default: interleave, per REQ-GLC-040.

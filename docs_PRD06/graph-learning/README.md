# Graph-Learning Knowledgebase

A source-grounded study of how graph systems actually work, extracted from a
frozen corpus of 172 shallow-cloned repositories (`corpus-ledger.tsv`) and
published as 28 pattern pairs + 8 category syntheses. Every factual claim
cites a local source file; every pattern ships as two standalone documents
(`*-ascii.md` prose/diagrams, `*-mermaid.md` diagrams) with worked numerical
examples. Gates enforced by `verify-corpus-spec.sh`.

## Start here

1. `pattern-index.md` — the complete map (28 patterns, 8 syntheses).
2. Read a category synthesis first, then its member patterns:

| Category | Synthesis | Members |
| --- | --- | --- |
| storage-engine | `storage-engine-pattern-synthesis-*` | 1-6 (LSM, WAL, roaring, MVCC, COW, bloom) |
| graph-analytics | `graph-analytics-pattern-synthesis-*` | 7-12 (CSR, push/pull, semirings, hooking, PageRank, delta-stepping) |
| vector-ann | `vector-ann-pattern-synthesis-*` | 13-16 (HNSW, PQ, DiskANN, IVF) |
| full-text-search | `full-text-search-pattern-synthesis-*` | 17-19 (postings, BM25+WAND, FST) |
| graph-db | `graph-db-pattern-synthesis-*` | 20-22 (record chains, pull pipelines, triple permutations) |
| neo4j-ecosystem | `neo4j-ecosystem-pattern-synthesis-*` | 23-24 (PackStream, testkit/boltstub) |
| dataflow-compute | `dataflow-compute-pattern-synthesis-*` | 25-26 (differential frontiers, Pregel supersteps) |
| bench-testing | `bench-testing-pattern-synthesis-*` | 27-28 (metamorphic oracles, tolerant validation) |

## Reference material

- `corpus-ledger.tsv` — the authoritative 172-repo corpus (all plain
  `--depth 1` clones under `reference-repos-*`).
- `SPEC-graph-learning-corpus-research.md` — the executable spec the work
  follows (REQ contracts + CHK gates).
- `research-papers-ledger.md` — the literature spine, cross-linked to repos.
- `proprietary-tools-landscape.md` — closed systems as behavior endpoints.
- `domain-keywords-glossary.md` — the vocabulary, by category.
- `corpus-research-findings.md` — how the corpus was researched and frozen.

## The through-line

The knowledgebase serves `docs_PRD06/Rewrite-Sampling-And-Convergence-Thesis.md`:
a known endpoint (stock Neo4j/GDS) turns a rewrite into a search problem whose
bottleneck is verification, not code. Seven categories describe what graph
systems build; the eighth (bench-testing) describes how to know a rebuild is
right — the oracles that make convergence loops converge.

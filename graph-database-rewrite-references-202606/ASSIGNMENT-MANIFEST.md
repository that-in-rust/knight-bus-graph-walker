# Assignment Manifest: Desktop Graph Database Pattern Corpus

Date: 2026-07-07

Scope:

`/Users/amuldotexe/Desktop/`

Output directory:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/graph-database-rewrite-references-202606`

The active objective requires exactly these five canonical files:

| file | slice | thematic lens |
| --- | --- | --- |
| `meta-graph-database-patterns-1.md` | Neo4j family architecture and compatibility surface | Neo4j kernel/procedure boundaries, Cypher/Bolt contracts, GDS graph catalog, drivers, testkit |
| `meta-graph-database-patterns-2.md` | Storage engines and memory layout | WAL, checkpoints, page cache, mmap/direct I/O, Arrow/Parquet, durable file formats |
| `meta-graph-database-patterns-3.md` | Query execution and parser/compiler infrastructure | grammar/AST, binder, planner, optimizer, physical operators, vectorized execution |
| `meta-graph-database-patterns-4.md` | Graph algorithms, CSR, GraphBLAS, sparse traversal | algorithm state shape, traversal frontiers, external-memory processing, low-RAM proof |
| `meta-graph-database-patterns-5.md` | Testing, observability, protocols, developer tooling | metrics, tracing, failure injection, benchmark design, driver fixtures, operational ergonomics |

## Coverage Rule

Every Git repo root discovered under `/Users/amuldotexe/Desktop/` must have one
row in:

`graph-database-rewrite-references-202606/repo-coverage-ledger.tsv`

Allowed inspection levels:

- `direct_source_cited`: repo has direct source or doc citations in a canonical
  file or named supplement.
- `metadata_browsed_name_cited`: repo was browsed at metadata/name level and
  cited as relevant context, but not deeply source-extracted.
- `metadata_browsed_low_signal`: repo was browsed and judged low signal for the
  Neo4j-in-Rust rewrite unless a later requirement makes it relevant.
- `metadata_browsed_gap`: repo is inventoried but still needs source evidence,
  explicit low-signal rationale, or user-accepted scoping.

## Pattern Record Shape

For every meaningful pattern, capture as much as practical:

- pattern name
- repo path
- file path and line range
- language or stack
- what the code does
- why it matters for a Neo4j-in-Rust rewrite
- Rust translation
- memory, concurrency, testing, and performance implications
- risks and caveats
- agentic guidance for future code generation

## Evidence Tool Policy

- Use `codegraphcontext-evidence-reader` as an accelerator, not as a truth
  oracle.
- Confirm important graph-tool findings with direct source reads.
- Record graph-tool paths, stats, and failed/hanging attempts in the audit or
  journal.
- Use `tdd-task-progress-context-retainer` to preserve resumable state after
  meaningful batches.

## Current Desktop Coverage Count

The active ledger currently covers 911 repo roots under `/Users/amuldotexe/Desktop/`.

As of supplemental gap-closure batch 05:

- 89 repos are `direct_source_cited`.
- 69 repos are `metadata_browsed_name_cited`.
- 129 repos are `metadata_browsed_low_signal`.
- 624 repos are `metadata_browsed_gap`.

## Non-Canonical Files

The directory also contains older or narrower artifacts such as
`graph-database-patterns-*`, `supermeta-*`, supplemental files, and legacy
worker outputs. Keep them for historical context, but treat `meta-*` as the
required five-file corpus for the active objective.

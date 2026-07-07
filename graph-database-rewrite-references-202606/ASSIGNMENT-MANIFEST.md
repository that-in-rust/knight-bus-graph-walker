# Assignment Manifest: Gitrefrepo Graph Database Pattern Corpus

Date: 2026-07-07

Scope:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo`

Output directory:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/graph-database-rewrite-references-202606`

The active objective requires exactly these five canonical files:

| file | slice | thematic lens |
| --- | --- | --- |
| `graph-database-patterns-1.md` | Neo4j family architecture and compatibility surface | Neo4j kernel/procedure boundaries, Cypher/Bolt contracts, GDS graph catalog, drivers, testkit |
| `graph-database-patterns-2.md` | Graph engines and query runtimes | graph storage, parser/planner/executor boundaries, cursor traversal, index substitution |
| `graph-database-patterns-3.md` | Storage engines and query execution infrastructure | memory layout, zero-copy IO, WAL, indexes, columnar/vector systems, Rust ownership and unsafe boundaries |
| `graph-database-patterns-4.md` | Graph algorithms, CSR, GraphBLAS, sparse traversal | algorithm state shape, traversal frontiers, external-memory processing, low-RAM proof |
| `graph-database-patterns-5.md` | Parsers, testing, observability, allocators, developer tooling | grammar/ASTs, metrics, tracing, failure injection, benchmark design, operational ergonomics |

## Coverage Rule

Every repo root under `gitrefrepo` must have one row in:

`graph-database-rewrite-references-202606/gitrefrepo-coverage-ledger.tsv`

Every repo root under `gitrefrepo` must also have a codebase-memory tool-status
row in:

`graph-database-rewrite-references-202606/gitrefrepo-codebase-memory-status.tsv`

Allowed inspection levels in the corpus coverage ledger:

- `canonical_file_mentioned`: repo appears in at least one canonical
  `graph-database-patterns-*.md` file.
- `direct_source_cited`: repo has direct source or doc citations in a
  canonical file.
- `assigned_inventory_only`: repo is assigned but has not yet received enough
  evidence. This should be zero before considering the corpus done.

Codebase-memory status interpretation:

- `indexed`: full repo path indexed by codebase-memory.
- `timeout`: full repo path did not complete within the recorded timeout.
- For `clickhouse-src`, full-repo indexing timed out after 1800 seconds, but
  focused high-signal slices are indexed in
  `clickhouse-focused-codebase-memory-status.tsv`.

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

- Use `codebase-memory-evidence-reader` and `codegraphcontext-evidence-reader`
  as accelerators, not as truth oracles.
- Confirm important graph-tool findings with direct source reads.
- Record graph-tool paths, stats, and failed/hanging attempts in the audit or
  journal.
- Use `tdd-task-progress-context-retainer` to preserve resumable state after
  meaningful batches.

## Current Assignment Count

The active coverage ledger covers 106 repo roots under `gitrefrepo`.

Current corpus coverage ledger:

- 91 repos are `canonical_file_mentioned`.
- 15 repos are `direct_source_cited`.
- 0 repos are `assigned_inventory_only`.

Current codebase-memory tool ledger:

- 105 repos are full-repo `indexed`.
- 1 repo, `clickhouse-src`, is full-repo `timeout` after 1800 seconds.
- 6 focused ClickHouse slices are `indexed` as fallback evidence.

## Non-Canonical Files

The directory also contains older or broader artifacts such as `meta-*`,
`supermeta-*`, supplemental files, Desktop-wide ledgers, and legacy worker
outputs. Keep them for historical context, but do not treat them as the
required five outputs for this active goal.

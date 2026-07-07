# Coverage Spine: Desktop Graph Database Pattern Corpus

Date: 2026-07-07

This file is the navigation spine for the active Desktop-wide `meta-*`
corpus.

## Active Scope

Read pattern evidence from:

`/Users/amuldotexe/Desktop/`

Write canonical notes to:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/graph-database-rewrite-references-202606`

## Canonical Files

| file | role |
| --- | --- |
| `meta-graph-database-patterns-1.md` | Neo4j family architecture, GDS, Bolt/Cypher/procedure/driver compatibility |
| `meta-graph-database-patterns-2.md` | storage engines, WAL/checkpoint/index/memory layout, Arrow/Parquet |
| `meta-graph-database-patterns-3.md` | parser/planner/executor boundaries and query runtime design |
| `meta-graph-database-patterns-4.md` | graph algorithms, sparse data structures, CSR/GraphBLAS/HPC traversal |
| `meta-graph-database-patterns-5.md` | benchmark/validation loops, observability, protocols, driver fixtures, developer tooling |

## Ledgers

Authoritative Desktop-wide repo coverage ledger:

`graph-database-rewrite-references-202606/repo-coverage-ledger.tsv`

Metadata browse ledger:

`graph-database-rewrite-references-202606/repo-metadata-browse-ledger.tsv`

Coverage invariant:

- The ledgers SHALL contain exactly one row per discovered Git repo root under
  `/Users/amuldotexe/Desktop/`.
- `metadata_browsed_gap` SHALL be treated as an open gap.
- `metadata_browsed_low_signal` proves explicit triage, not deep extraction.
- `direct_source_cited` proves direct source/doc citation in a canonical file or
  named supplement.

Current post-batch05 state:

| inspection level | repositories |
| --- | ---: |
| `direct_source_cited` | 89 |
| `metadata_browsed_name_cited` | 69 |
| `metadata_browsed_low_signal` | 129 |
| `metadata_browsed_gap` | 624 |

## Reading Strategy For Future Agents

1. Start from `repo-coverage-ledger.tsv` to choose the weakest high-signal rows.
2. Use `rg` for exact terms and file discovery.
3. Use CodeGraphContext when symbol graph evidence helps and the repo indexes
   cleanly.
4. Confirm graph-tool findings with direct `nl -ba` or source reads.
5. Add one deep pattern at a time to the correct canonical meta file or a
   named supplement.
6. Update the ledger, audit, and TDD journal after each meaningful batch.

## Strongest Current Navigation Points

- Neo4j compatibility and GDS architecture:
  `meta-graph-database-patterns-1.md`
- Storage/query execution and memory layout:
  `meta-graph-database-patterns-2.md`
- Query compiler and runtime design:
  `meta-graph-database-patterns-3.md`
- Low-RAM traversal and algorithm state:
  `meta-graph-database-patterns-4.md`
- Verification loop, protocol compatibility, observability, tooling:
  `meta-graph-database-patterns-5.md`

## Remaining Strictness Question

The active objective literally says to browse each repo under
`/Users/amuldotexe/Desktop/` with `codegraphcontext-evidence-reader`. The corpus
now has ledger coverage for all 911 repos and focused graph-tool use, but it
does not yet prove successful CodeGraphContext indexing for every repo. If the
next goal insists on strict tool-per-repo proof, add explicit
`codegraphcontext_status` columns to the ledger and populate them repo by repo.

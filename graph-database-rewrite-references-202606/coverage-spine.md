# Coverage Spine: Gitrefrepo Graph Database Pattern Corpus

Date: 2026-07-07

This file is the navigation spine for the active `gitrefrepo` corpus. It does
not describe the older Desktop-wide or `meta-*` / `supermeta-*` corpora.

## Active Scope

Read pattern evidence from:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo`

Write canonical notes to:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/graph-database-rewrite-references-202606`

## Canonical Files

| file | role |
| --- | --- |
| `graph-database-patterns-1.md` | Neo4j family architecture, GDS, Bolt/Cypher/procedure/driver compatibility |
| `graph-database-patterns-2.md` | graph databases, RDF/property graph engines, graph query runtimes |
| `graph-database-patterns-3.md` | storage engines, query execution engines, WAL/checkpoint/index/memory layout |
| `graph-database-patterns-4.md` | graph algorithms, sparse data structures, CSR/GraphBLAS/HPC traversal |
| `graph-database-patterns-5.md` | parsers, benchmark/validation loops, observability, allocators, developer tooling |

## Ledgers

Authoritative repo coverage ledger:

`graph-database-rewrite-references-202606/gitrefrepo-coverage-ledger.tsv`

Codebase-memory tool-status ledger:

`graph-database-rewrite-references-202606/gitrefrepo-codebase-memory-status.tsv`

ClickHouse focused fallback ledger:

`graph-database-rewrite-references-202606/clickhouse-focused-codebase-memory-status.tsv`

Coverage invariant:

- The repo coverage ledger SHALL contain exactly one row per repo root under
  `gitrefrepo`.
- `assigned_inventory_only` SHALL be treated as an open gap.
- `canonical_file_mentioned` proves corpus mention, not uniform depth.
- `direct_source_cited` proves direct source/doc citation in a canonical file.
- Codebase-memory evidence SHALL be read from the tool-status ledgers.

Current repo coverage state:

| inspection level | repositories |
| --- | ---: |
| `canonical_file_mentioned` | 91 |
| `direct_source_cited` | 15 |
| `assigned_inventory_only` | 0 |

Current codebase-memory state:

| status | count |
| --- | ---: |
| full repo `indexed` | 105 |
| full repo `timeout` | 1 |
| focused ClickHouse slices `indexed` | 6 |

## Reading Strategy For Future Agents

1. Start from the ledger to choose the weakest rows.
2. Use `rg` for exact terms and file discovery.
3. Use codebase-memory and CodeGraphContext when symbol graph evidence helps.
4. Confirm graph-tool findings with direct `nl -ba` or source reads.
5. Add one deep pattern at a time to the correct canonical file.
6. Update the ledger, audit, and TDD journal after each meaningful batch.

## Strongest Current Navigation Points

- Neo4j compatibility and GDS architecture: `graph-database-patterns-1.md`
- External graph engines and query runtimes: `graph-database-patterns-2.md`
- Storage/query execution and memory layout: `graph-database-patterns-3.md`
- Low-RAM traversal and algorithm state: `graph-database-patterns-4.md`
- Verification loop, parser compatibility, allocator/page-cache taste:
  `graph-database-patterns-5.md`

## Remaining Strictness Question

The corpus now proves codebase-memory activity for every gitrefrepo repo:
105 repos full-indexed and one repo, ClickHouse, covered through focused
high-signal source-slice indexes after full-repo timeout. A future stricter
audit could still require a completed full ClickHouse index, but that is now a
tooling-scale limitation rather than an unattempted repo.

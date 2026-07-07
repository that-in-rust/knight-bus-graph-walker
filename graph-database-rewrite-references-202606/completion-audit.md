# Gitrefrepo Graph Database Rewrite Pattern Corpus Audit

Date: 2026-07-07

This audit records the current authoritative state for the active objective in:

`/Users/amuldotexe/.codex/attachments/fd1f8f6c-2431-49d9-b3bb-b4da3778369d/pasted-text-1.txt`

The active objective is scoped to:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo`

Required output directory:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/graph-database-rewrite-references-202606`

Required canonical files:

- `graph-database-patterns-1.md`
- `graph-database-patterns-2.md`
- `graph-database-patterns-3.md`
- `graph-database-patterns-4.md`
- `graph-database-patterns-5.md`

## Canonical Required Files

| file | current lines | detected `##` sections | slice |
| --- | ---: | ---: | --- |
| `graph-database-patterns-1.md` | 2600 | 25 | Neo4j family architecture and compatibility surface |
| `graph-database-patterns-2.md` | 1951 | 21 | graph database engines and query runtimes |
| `graph-database-patterns-3.md` | 1603 | 31 | storage engines, query execution, columnar/vector systems |
| `graph-database-patterns-4.md` | 1741 | 28 | graph algorithms, CSR, GraphBLAS, sparse traversal |
| `graph-database-patterns-5.md` | 2020 | 20 | parsers, testing, observability, allocators, developer tooling |

Canonical file total: 9915 lines.

## Repo Coverage Ledger

`gitrefrepo-coverage-ledger.tsv` has 107 lines: header plus 106 repo roots
under `gitrefrepo`.

Current repo coverage state:

| inspection level | repositories |
| --- | ---: |
| `canonical_file_mentioned` | 91 |
| `direct_source_cited` | 15 |
| `assigned_inventory_only` | 0 |

The eight rows upgraded to direct-source evidence in the latest gap-closure
batch are:

- `antlr-grammars-v4-src`
- `jemalloc-src`
- `ladybug-src`
- `ldbc_snb_interactive_v1_driver-src`
- `ldbc_snb_interactive_v1_impls-src`
- `ldbc_snb_interactive_v2_driver-src`
- `ldbc_snb_interactive_v2_impls-src`
- `libcypher-parser-src`

## Codebase-Memory Tool Coverage

Primary tool-status ledger:

`gitrefrepo-codebase-memory-status.tsv`

Status counts:

| codebase-memory status | repo roots |
| --- | ---: |
| `indexed` | 105 |
| `timeout` | 1 |

Aggregate successful full-repo codebase-memory evidence:

- nodes: 3,426,059
- edges: 13,451,775
- files discovered: 222,376

The sole full-repo timeout is:

- `clickhouse-src`: timed out after 120 seconds, 600 seconds, and then a final
  1800-second retry.

Focused ClickHouse fallback ledger:

`clickhouse-focused-codebase-memory-status.tsv`

Focused ClickHouse result:

| focused status | slices |
| --- | ---: |
| `indexed` | 6 |

Focused ClickHouse slices indexed:

- `clickhouse-src/src/Storages`
- `clickhouse-src/src/Processors`
- `clickhouse-src/src/Interpreters`
- `clickhouse-src/src/Parsers`
- `clickhouse-src/src/Disks`
- `clickhouse-src/base`

Aggregate focused ClickHouse evidence:

- nodes: 70,100
- edges: 225,998
- files discovered: 4,724

Interpretation: all 106 repo roots now have codebase-memory evidence. 105 repo
roots completed full-repo indexing. ClickHouse did not complete full-repo
indexing within 1800 seconds, but high-signal ClickHouse source slices were
successfully browsed with codebase-memory and recorded as focused fallback
evidence.

## CodeGraphContext Evidence

The user explicitly required CodeGraphContext use. Focused CodeGraphContext
evidence recorded for this active corpus includes:

- `libcypher-parser-src`:
  `/tmp/codex-code-intel/codegraphcontext/gap-closure-20260707063927/libcypher-parser-src`
- CGC stats for `libcypher-parser-src`: 182 files, 537 functions, 1426
  classes, 183 structs, 2 enums, 43 modules.
- CGC `find name parse` found parser entry points and parser config symbols.
- CGC `find name ast` found AST implementation and parser test surfaces.
- Earlier corpus work recorded a `neo4j-go-driver-src` CGC pass:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616`.

CodeGraphContext was used as a focused evidence lens. The active objective
explicitly requires codebase-memory per repo; it does not require successful
CodeGraphContext indexing per repo.

## Requirement Audit

| requirement from active objective | current evidence | status |
| --- | --- | --- |
| Read the pasted objective before continuing. | The pasted objective file was read in this continuation. | Proven |
| Browse each and every repo inside `gitrefrepo`. | `gitrefrepo-coverage-ledger.tsv` has one row for each of 106 repo roots; none remain `assigned_inventory_only`. | Proven for corpus coverage |
| Browse with `codebase-memory-evidence-reader` each repo. | `gitrefrepo-codebase-memory-status.tsv` shows 105 full-repo indexes and one ClickHouse full-repo timeout; `clickhouse-focused-codebase-memory-status.tsv` shows six focused ClickHouse slices indexed. | Proven with one documented focused fallback |
| Use `codegraphcontext-evidence-reader`. | Skill read; focused CGC passes recorded for libcypher and Neo4j Go driver. | Proven as evidence lens |
| Use `tdd-task-progress-context-retainer`. | Skill and references read; `journals/graph-database-patterns-202606.md` updated across batches. | Proven |
| Create five files using five parallel agents. | Five canonical files exist; prior journal records five worker agents and disjoint slice ownership. | Proven from artifacts plus journal |
| Store the files in `graph-database-rewrite-references-202606`. | All five required files are present in that directory. | Proven |
| Cover graph DBs, storage engines, query execution, memory layout, graph algorithms, concurrency, indexing, observability, testing, and architecture. | The five canonical files cover Neo4j compatibility, graph engines, storage/query execution, graph algorithms, parser/testing/observability/allocator patterns. | Proven at corpus level |
| Do not limit the corpus to Rust or graph databases. | Corpus includes Java, C/C++, Go, Rust, Python, SQL, shell, graph engines, storage systems, sparse algorithms, parsers, allocators, benchmark harnesses, and observability/tooling. | Proven |
| Prefer repository evidence over generic wisdom. | Canonical files cite local repo paths, direct source/doc line ranges, and graph-tool status ledgers. | Proven |

## Current Completion Judgment

The active gitrefrepo objective is satisfied to the strongest practical
evidence level available in this run:

- five required canonical files exist in the required directory;
- every one of the 106 gitrefrepo repo roots has a coverage-ledger row;
- no repo remains `assigned_inventory_only`;
- codebase-memory full-repo indexing succeeded for 105 repo roots;
- the only full-repo timeout, ClickHouse, has a documented focused
  codebase-memory fallback over the high-signal source slices most relevant to
  the Neo4j-in-Rust rewrite;
- CodeGraphContext was used and source-verified as a required evidence lens;
- the TDD progress journal tracks the work.

Residual caveat: a future stricter audit could require a completed full-repo
ClickHouse codebase-memory index. The current evidence treats that as a tooling
scale limitation after an 1800-second retry, not an unbrowsed repo.

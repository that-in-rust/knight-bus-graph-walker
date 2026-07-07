# Graph Database Rewrite Pattern Corpus Audit

Date: 2026-07-07

This audit records the current authoritative state for the active goal whose
objective text was read from:

`/Users/amuldotexe/.codex/attachments/b68153b8-8b0f-4ab2-bce2-26a80de55092/pasted-text-1.txt`

The active objective is Desktop-wide: browse repositories under
`/Users/amuldotexe/Desktop/`, use `codebase-memory-evidence-reader`, preserve
progress with `tdd-task-progress-context-retainer`, and maintain five canonical
files named `meta-graph-database-patterns-1.md` through
`meta-graph-database-patterns-5.md` in:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/graph-database-rewrite-references-202606`

The user also explicitly reinforced in this continuation that
`codegraphcontext-evidence-reader` must be used. The Dgraph closure below uses
CodeGraphContext, codebase-memory, and direct source reads.

## Canonical Required Files

| file | lines | detected pattern sections | slice |
| --- | ---: | ---: | --- |
| `meta-graph-database-patterns-1.md` | 2015 | 29 | Neo4j/GDS, graph databases, graph storage, graph projections |
| `meta-graph-database-patterns-2.md` | 2246 | 17 | storage engines, WAL, checkpoints, memory layout, Rust systems |
| `meta-graph-database-patterns-3.md` | 2309 | 34 | query engines, parser/planner/executor boundaries, vectorized execution |
| `meta-graph-database-patterns-4.md` | 2192 | 33 | graph algorithms, CSR, GraphBLAS, sparse execution, benchmarks |
| `meta-graph-database-patterns-5.md` | 2042 | 38 | observability, testing, protocol fixtures, parser/tooling, agent workflows |

Canonical total: 10,804 lines and 151 detected pattern sections.

Pattern sections are counted with both canonical heading shapes currently found
in the files: `### Pattern:` and `## Pattern N:`.

## Supplemental Files

| file family | status |
| --- | --- |
| `supplemental-gap-closure-batch-01.md` | 718 lines; direct-source patterns from Neo4j Testkit, ClickHouse, Memgraph, Polars, GraphScope, TiKV, and Quickwit |
| `supplemental-gap-closure-batch-02.md` | 930 lines; direct-source patterns from Neo4j record storage, Python driver, Bolt docs, GDS, and APOC |
| `supplemental-gap-closure-batch-03.md` | 820 lines; direct-source patterns from official drivers, GDS client, Browser, OGM, and GDS Agent |
| `supplemental-gap-closure-batch-04.md` | 726 lines; direct-source patterns from HugeGraph, Blazegraph, RDF4J, IndraDB, NebulaGraph, SurrealDB, plus duplicate reference-shelf policy |
| `supplemental-gap-closure-batch-05.md` | 810 lines; direct-source patterns from Dgraph posting lists, DQL/SubGraph query execution, schema/index rebuild, Raft WAL, compressed UID operators, and task conflict rules |
| `supplemental-parser-code-intelligence-patterns.md` | parser/tree-sitter/code-intelligence patterns |
| `supplemental-storage-rust-systems-patterns.md` | storage/Rust systems patterns |
| `graph-database-patterns-*.md` and `supermeta-graph-database-patterns-*.md` | earlier or alternate corpus artifacts retained for comparison |

## Inventory And Ledgers

| file | evidence |
| --- | --- |
| `desktop-repository-inventory.txt` | 911 Git roots discovered under `/Users/amuldotexe/Desktop` |
| `repo-metadata-browse-ledger.tsv` | header plus one metadata-browse row per discovered repo |
| `repo-coverage-ledger.tsv` | header plus one coverage/evidence row per discovered repo |
| `repository-slice-counts.tsv` | coarse slice counts used during assignment |
| `coverage-spine.md` | Desktop-wide coverage policy and corpus spine |
| `ASSIGNMENT-MANIFEST.md` | worker/file ownership and pattern record policy |
| `progress-journal.md` | corpus-local progress journal |
| `journals/graph-database-pattern-corpus-202606.md` | broader TDD progress journal |

Current `repo-coverage-ledger.tsv` inspection-level counts:

| inspection level | repositories |
| --- | ---: |
| `direct_source_cited` | 89 |
| `metadata_browsed_name_cited` | 69 |
| `metadata_browsed_low_signal` | 129 |
| `metadata_browsed_gap` | 624 |

Current remaining `metadata_browsed_gap` rows by slice:

| slice | gap repositories |
| --- | ---: |
| `parser_code_intelligence` | 596 |
| `rust_systems_tooling` | 17 |
| `query_compiler_execution` | 5 |
| `storage_columnar_memory` | 4 |
| `graph_algorithms_sparse` | 2 |

There are now zero `neo4j_gds_compat` rows left at `metadata_browsed_gap`.

## Latest Direct Evidence Upgrade

`supplemental-gap-closure-batch-05.md` contains direct-source patterns for the
primary Dgraph clone:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/dgraph-src`

It upgrades this repo to `direct_source_cited` with patterns for:

- predicate-keyed posting lists with immutable base state and mutable deltas;
- rollup and multipart split-list storage for large posting lists;
- delta-only mutation fast paths that avoid reading base posting lists;
- compressed UID/posting-list intersection operators;
- typed durable key families for data, reverse, index, count, schema, and split
  records;
- executable schema/index contracts, interim query schema, and rebuild plans;
- DQL/SubGraph execution with UID/value/facet matrices and variable scheduling;
- edge/frontier limits for recursive and shortest-path execution;
- background task conflict rules for rollup, restore, backup, indexing,
  snapshots, and predicate moves;
- Raft WAL layout, zeroed log slots, retry backpressure, and timestamp-aware
  cache validity.

The source pass also records a verification warning:
`query/shortest.go:231-235` compares against `LimitQueryEdge` but the error
text mentions `LimitMutationsNquad`.

## Evidence Tool Use

Required skills loaded in this continuation:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/tdd-task-progress-context-retainer/SKILL.md`
- `/Users/amuldotexe/.codex/skills/tdd-task-progress-context-retainer/references/progress-journal-schema.md`
- `/Users/amuldotexe/.codex/skills/tdd-task-progress-context-retainer/references/tdd-checkpoint-cadence-playbook.md`
- `/Users/amuldotexe/.codex/skills/tdd-task-progress-context-retainer/references/resume-handoff-prompts.md`

Focused graph-tool evidence currently recorded:

- `dgraph-src` CodeGraphContext batch05 artifact:
  `/tmp/codex-code-intel/codegraphcontext/dgraph-batch05/dgraph.sqlite`
  reports 1 repository, 480 files, 5,998 functions, 54 interfaces, 585 structs,
  and 8 modules.
- `dgraph-src` CodeGraphContext rerun for this continuation:
  `/tmp/codex-code-intel/codegraphcontext/dgraph-src-20260707-072213`
  was interrupted after a long silent index run, but the readable database
  reports 1 repository, 829 files, 8,379 functions, 79 interfaces, 858 structs,
  and 13 modules. CGC found `posting.List`, `List.Rollup`, `query.SubGraph`,
  and worker `proposeAndWait` anchors.
- `dgraph-src` codebase-memory pass:
  `/tmp/codex-code-intel/codebase-memory/dgraph-src-20260707-072213`
  indexed 18,748 nodes and 91,942 edges. It found the same important anchors,
  including `posting.List`, `query.SubGraph`, `posting.AddMutationWithIndex`,
  `Rollup`, and worker `proposeAndWait` methods.
- `kuzudb__kuzu` codebase-memory pass:
  `/tmp/codex-code-intel/codebase-memory/kuzudb__kuzu-20260707-070427`
  indexed 50,232 nodes and 158,544 edges.
- `kuzudb__kuzu` CodeGraphContext pass:
  `/tmp/codex-code-intel/codegraphcontext/kuzudb__kuzu-20260707-070427`
  was attempted, but after interrupting the long writer the readable database
  reported 1 repository and 0 files/functions/classes. It is recorded as an
  attempted non-evidentiary CGC pass for this repo.
- `indradb-src` CodeGraphContext pass:
  `/tmp/codex-code-intel/codegraphcontext/indradb-batch04`
  indexed 1 repository, 70 files, 529 functions, 6 traits, 74 structs, 19
  enums, and 109 modules.
- `neo4j-go-driver-src`, `neo4j-python-driver-src`, `neo4j-gds-src`,
  JavaScript driver routing, and `gds-agent-src` have focused CodeGraphContext
  attempts recorded in prior supplemental batches.

Important caveat: CodeGraphContext has been used as a required focused evidence
lens, but this audit does not prove successful CGC indexing for every one of
the 911 discovered Desktop repositories. Direct conclusions are source-read
backed.

## Requirement Audit

| requirement from active objective / current user instruction | current evidence | status |
| --- | --- | --- |
| Read the active objective before continuing. | `/Users/amuldotexe/.codex/attachments/b68153b8-8b0f-4ab2-bce2-26a80de55092/pasted-text-1.txt` was read in this continuation. | Proven |
| Browse each and every repo inside `/Users/amuldotexe/Desktop/`. | 911 Git roots are inventoried; metadata and coverage ledgers have one row per repo. | Incomplete under strict reading because 624 rows remain `metadata_browsed_gap` |
| Use `codebase-memory-evidence-reader`. | Skill loaded; Dgraph and Kuzu passes produced positive graph evidence and guided direct reads. | Proven as evidence lens, not exhaustive per repo |
| Use `codegraphcontext-evidence-reader`. | Skill loaded; Dgraph CGC was rerun in this continuation and produced queryable evidence after interruption. | Proven as evidence lens, not exhaustive per repo |
| Use `tdd-task-progress-context-retainer`. | Skill and references loaded; progress journals are updated during corpus work. | Proven |
| Create five `meta-graph-database-patterns-*.md` files. | All five files exist in the requested directory and total 10,804 lines. | Proven |
| Store all five files in `graph-database-rewrite-references-202606/`. | All five canonical `meta-*` files are present in the target directory. | Proven |
| Cover graph DBs, storage engines, query execution, memory layout, graph algorithms, concurrency, indexing, observability, testing, and architecture. | The five meta files and supplements cover these categories with repo paths and source-backed patterns. | Broadly proven for cited repos |
| Do not limit the corpus to Rust or graph databases. | Corpus includes Java, Kotlin, C, C++, Rust, Go, Python, SQL/planner, GraphBLAS, parser tooling, benchmark, observability, and workflow evidence. | Proven for cited repos |

## Current Completion Judgment

The five required `meta-*` files exist and are useful. The Desktop-wide
inventory is also materially better than a raw file dump: every discovered Git
root has a ledger row and top-level metadata was recorded.

The full objective is not complete under a strict reading. The explicit phrase
"each and every repo inside `/Users/amuldotexe/Desktop/`" requires stronger
evidence than current ledgers provide. The current measurable gap is:

- 624 repositories remain `metadata_browsed_gap`;
- 89 repositories are currently `direct_source_cited`;
- CodeGraphContext use is proven for focused/high-value passes, not for every
  discovered Desktop repository.

Do not mark the active goal complete until either:

1. the remaining `metadata_browsed_gap` rows are upgraded to direct evidence,
   explicit low-signal/skipped rationale, or user-accepted scoped coverage; or
2. the user explicitly accepts the high-signal corpus plus coverage ledgers as
   satisfying the intended scope.

## Next Work To Close The Gap

1. Continue upgrading high-value `metadata_browsed_gap` rows, starting with
   high-signal parser/code-intelligence representatives, Rust systems tooling,
   query/compiler engines, storage/columnar systems, and graph algorithm repos.
2. Prefer CodeGraphContext when the repo shape gives usable graph evidence; if
   CGC fails or indexes empty, record that explicitly and rely on
   codebase-memory plus direct source reads.
3. Add each resulting pattern to a canonical `meta-*` file or named supplement.
4. Update `repo-coverage-ledger.tsv`, `repo-metadata-browse-ledger.tsv`, this
   audit, and the TDD progress journals after each meaningful coverage upgrade.
5. Run whitespace and artifact checks after edits.

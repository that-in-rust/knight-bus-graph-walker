# Reference Shelf Subpath Coverage Audit

Date: 2026-06-24

This audit implements the folder and sub-repo coverage part of
[`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/V003-Reference-Folder-Learning-Spec.md`](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/V003-Reference-Folder-Learning-Spec.md)
by proving that the spec's named paths resolve to real local folders and to
repo-root graph-evidence runs already recorded in the shelf-wide truthcheck.

It does not replace the repo-wide graph ledger. It makes the spec's
"which folders exactly?" requirement mechanically traceable.

## Verdict

- The canonical reference shelf is still `gitrefrepo/`; `ref-repo-folder/` is
  still empty and remains a legacy alias only.
- The current learning spec's concrete repo scope is still fully covered by the
  repo-root truthcheck:
  `71` concrete repo targets, `71` TSV rows, `0` missing, `0` extra.
- The spec's named folder and sub-repo clauses are supportable through:
  - repo-root graph indexing with the two evidence-reader skills; and
  - direct filesystem and source-path verification inside the named folders.
- Two exceptions remain explicit and honest:
  - `gitrefrepo/neo4j-graph-examples` is optional in the spec and is currently
    absent locally.
  - `clickhouse-src` remains `NeedsRerun` at the graph-tool level, although the
    repo itself exists and is still usable for direct source reads.
- Two Neo4j shorthand clauses need path normalization by future agents:
  - `REQ-LEARN-004.0` names `cypher-planner`, `physical-planning`,
    `runtime-spec-suite`, and related modules without repeating the
    `community/cypher/` prefix after the first token.
  - `REQ-LEARN-005.0` names `procedure-api`, `procedure-compiler`, and
    `values` without repeating the `community/` prefix.
  These are real folders, but they are not top-level repo paths.

## Evidence Inputs

- Spec:
  [`docs_PRD03/V003-Reference-Folder-Learning-Spec.md`](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/V003-Reference-Folder-Learning-Spec.md)
- Repo-wide control ledger:
  [`docs_PRD03/reference-learning/Reference-Shelf-Graph-Evidence-Ledger.md`](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/Reference-Shelf-Graph-Evidence-Ledger.md)
- Machine-readable truthcheck:
  [`docs_PRD03/reference-learning/Reference-Shelf-Graph-Tool-Truthcheck.tsv`](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/Reference-Shelf-Graph-Tool-Truthcheck.tsv)
- Skills used:
  - `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
  - `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

## Fresh Control Runs

These runs were executed during this audit to verify that the two graph-tool
entrypoints still work on the current workspace and the two first-party roots
that dominate the spec.

| repo | tool | run id | result |
| --- | --- | --- | --- |
| current Knight Bus repo | `codebase-memory-mcp` | `knight-bus-graph-walker-20260624-162809` | complete; wrapper verified indexed query outputs do not mention `gitrefrepo/` |
| current Knight Bus repo | CodeGraphContext | `knight-bus-graph-walker-20260624-162809` | complete; `stats.txt` reports `90` files, `439` functions, `57` structs, `23` enums, `72` modules |
| `gitrefrepo/neo4j-src` | `codebase-memory-mcp` | `neo4j-src-20260624-162809` | complete; wrapper verified indexed query outputs do not mention `gitrefrepo/` |
| `gitrefrepo/neo4j-src` | CodeGraphContext | `neo4j-src-20260624-162809` | partial only; `index.txt` exists but no semantic `stats.txt` yet, matching the shelf ledger's CGC low-yield story |
| `gitrefrepo/neo4j-gds-src` | `codebase-memory-mcp` | `neo4j-gds-src-20260624-162809` | complete; wrapper verified indexed query outputs do not mention `gitrefrepo/` |
| `gitrefrepo/neo4j-gds-src` | CodeGraphContext | `neo4j-gds-src-20260624-162809` | partial only; `index.txt` exists but no semantic `stats.txt` yet, again matching the shelf ledger |

## Coverage Rule

For this learning program, folder coverage means:

1. The enclosing repo root is present on disk.
2. The enclosing repo root already has a graph-tool truthcheck row, or the repo
   is explicitly classified as `GraphToolLowYield`.
3. The named folder, module path, or document path exists locally, or the spec
   explicitly marks it as a pattern or optional input.

That is the contract future agents should use. They should not create a second,
separate graph database per subfolder when the repo-root run already exists.

## Requirement Coverage Matrix

| requirement | named roots | repo-root graph status | folder or subpath result |
| --- | --- | --- | --- |
| `REQ-LEARN-002.0` | `neo4j-src` | `CbmSemanticReadyCgcLowYield` | `community/record-storage-engine`, `community/kernel`, `community/kernel-api`, `community/storage-engine-util`, `community/io`, and `community/collections` all exist |
| `REQ-LEARN-003.0` | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, driver wildcard | CBM on `neo4j-src`, `GraphToolLowYield` on docs-bolt, `DualSemanticReady` on testkit, CBM on driver repos | `community/bolt` exists; wildcard expands locally to `neo4j-dotnet-driver-src`, `neo4j-go-driver-src`, `neo4j-java-driver-src`, `neo4j-javascript-driver-src`, and `neo4j-python-driver-src` |
| `REQ-LEARN-004.0` | `neo4j-src` | `CbmSemanticReadyCgcLowYield` | shorthand clause normalizes to `community/cypher/{front-end,cypher-planner,cypher-logical-plans,physical-planning,slotted-runtime,interpreted-runtime,runtime-spec-suite,compatibility-spec-suite}`; all exist under `community/cypher/` |
| `REQ-LEARN-005.0` | `neo4j-src` | `CbmSemanticReadyCgcLowYield` | shorthand clause normalizes to `community/{procedure,procedure-api,procedure-compiler,values}`; all exist |
| `REQ-LEARN-006.0` | `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | `proc`, `procedures`, `applications/algorithms`, and `applications/graph-store-catalog` exist; `procedures/*-facade*` is a valid pattern clause |
| `REQ-LEARN-007.0` | `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | `core`, `core-api`, `native-projection`, `legacy-cypher-projection`, `graph-projection-api`, `triplet-graph-builder`, `graph-schema-api`, and `graph-dimensions` all exist |
| `REQ-LEARN-008.0` | `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | `executor`, `memory-estimation`, `memory-usage`, and `collections-memory-estimation` all exist |
| `REQ-LEARN-009.0` | `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | `algo`, `algorithm-specifications`, and `algo-params` exist; `proc/{centrality,community,path-finding,similarity,embeddings,machine-learning}` is a valid pattern clause |
| `REQ-LEARN-010.0` | `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | `core-write`, `model-catalog-api`, `open-model-catalog`, `pipeline`, and `ml` all exist |
| `REQ-LEARN-011.0` | `apache-iggy-src`, `rocksdb-src`, `fjall-src`, `redb-src`, `tikv-src` | CBM on `apache-iggy-src`, `rocksdb-src`, `tikv-src`; Dual on `fjall-src`, `redb-src` | all repo roots exist locally |
| `REQ-LEARN-012.0` | `apache-arrow-rs-src`, `apache-parquet-format-src`, `apache-datafusion-src` | CBM on `arrow-rs` and `datafusion`; `GraphToolLowYield` on parquet-format | `arrow-array`, `arrow-buffer`, `arrow-ipc`, `parquet`, and `datafusion/datasource-parquet` all exist |
| `REQ-LEARN-013.0` | `apache-datafusion-src` | `CbmSemanticReadyCgcLowYield` | `datafusion/{catalog,expr,sql,optimizer,physical-plan,execution,session}` is a valid existing pattern scope |
| `REQ-LEARN-014.0` | `kuzu-src`, `ladybug-src`, `falkordb-src`, `memgraph-src`, `age-src` | all `CbmSemanticReadyCgcLowYield` | all repo roots exist locally |
| `REQ-LEARN-015.0` | `gapbs-src`, `snap-src`, `lagraph-src`, `graphblas-src` | `DualSemanticReady` on `gapbs-src`; CBM on the others | all repo roots exist locally |
| `REQ-LEARN-019.0` | `ladybug-src` | `CbmSemanticReadyCgcLowYield` | `docs/icebug-disk.md`, `docs/index_build_recovery.md`, `docs/morsel_parallelism.md`, `src/storage`, `src/transaction`, `src/planner`, `src/optimizer`, `src/processor`, and `src/graph` all exist |
| `REQ-LEARN-020.0` | `neo4j-gds-client-src`, `graph-data-science-src`, `gds-agent-src`, `neo4j-graph-examples` | CBM on `neo4j-gds-client-src` and `gds-agent-src`; Dual on `graph-data-science-src`; optional missing on `neo4j-graph-examples` | three concrete local roots exist; `neo4j-graph-examples` is absent and stays optional |
| `REQ-LEARN-021.0` | `neo4j-apoc-src`, `neo4j-apoc-procedures-src` | both `CbmSemanticReadyCgcLowYield` | both repo roots exist locally |
| `REQ-LEARN-022.0` | `cypher-shell-src`, `cypher-dsl-src`, `neo4rs-src`, `neo4j-ogm-src`, `neo4j-browser-src` | Dual on `cypher-shell-src` and `neo4rs-src`; CBM on the others | all repo roots exist locally |
| `REQ-LEARN-023.0` | `ldbc_graphalytics-src`, `ldbc_graphalytics_docs-src`, `ldbc_graphalytics_platforms_graphblas-src`, `ldbc_snb_interactive_v1_driver-src`, `ldbc_snb_interactive_v1_impls-src`, `ldbc_snb_interactive_v2_driver-src`, `ldbc_snb_interactive_v2_impls-src` | docs repo is `GraphToolLowYield`; GraphBLAS platform repo is Dual; others are CBM-strong | all concrete local roots exist |
| `REQ-LEARN-024.0` | `graphchi-cpp-src`, `gridgraph-src`, `minigraph-src`, `thunderrw-src` | CBM on `graphchi-cpp-src`; Dual on the other three | all repo roots exist locally |
| `REQ-LEARN-025.0` | `graphblas-src`, `lagraph-src`, `graphblas-pointers-src`, `ldbc_graphalytics_platforms_graphblas-src`, `python-graphblas-src`, `graphblas_sparse_linear_algebra-src`, `falkordb-src`, `redisgraph-src` | Dual on `graphblas-pointers-src` and `ldbc_graphalytics_platforms_graphblas-src`; CBM on the others | all repo roots exist locally |
| `REQ-LEARN-026.0` | `petgraph-src`, `rustworkx-src`, `sprs-src`, `sparsetools-src`, `networkit-src`, `igraph-src` | Dual on `petgraph-src`, `sprs-src`, `sparsetools-src`; CBM on `rustworkx-src`, `networkit-src`, `igraph-src` | all repo roots exist locally |
| `REQ-LEARN-027.0` | `tracing-src`, `jemalloc-src`, `neo4j-gds-src`, `duckdb-src`, `clickhouse-src`, `ladybug-src` | Dual on `tracing-src`; CBM on `jemalloc-src`, `neo4j-gds-src`, `duckdb-src`, `ladybug-src`; `clickhouse-src` is `NeedsRerun` | all concrete repo roots exist; only `clickhouse-src` remains graph-tool-stale |
| `REQ-LEARN-028.0` | `differential-dataflow-src`, `timely-dataflow-src`, `materialize-src`, `risingwave-src` | Dual on `timely-dataflow-src`; CBM on the others | all repo roots exist locally |
| `REQ-LEARN-029.0` | `helix-db-src`, `nornicdb-src`, `qdrant-src`, `tantivy-src`, plus any cloned Neo4j vector-index paths | all named concrete roots are `CbmSemanticReadyCgcLowYield` | all named concrete roots exist; no extra Neo4j vector-index clone is currently called out in the audit input |
| `REQ-LEARN-031.0` | current Knight Bus snapshots plus `ladybug-src`, `duckdb-src`, `clickhouse-src`, `redb-src`, `rocksdb-src`, `fjall-src` | workspace plus mixed shelf statuses; `clickhouse-src` still `NeedsRerun` | all concrete repo roots exist; current repo is freshly re-indexed in both tools |
| `REQ-LEARN-032.0` | current Knight Bus repo | fresh CBM plus fresh CGC run completed in this audit | workspace-level graph indexing plus direct reads is the correct coverage model for `docs_PRD03/`, `src/`, `tests/`, README, and snapshot/runtime files |

## What This Proves

- The spec's "all the folders and sub repos mentioned" instruction is
  implementable without inventing another indexing layer.
- Repo-root graph indexing plus direct file verification is enough to cover the
  named subpaths, including the current Knight Bus repo.
- The current blocker set is small and explicit:
  - optional local absence of `neo4j-graph-examples`;
  - graph-tool rerun debt on `clickhouse-src`;
  - future agents must normalize the two Neo4j shorthand clauses before
    claiming that a top-level folder is missing.

## What This Does Not Prove

- It does not prove the architecture is correct.
- It does not prove every graph-tool result is semantically useful for every
  repo. The shelf truthcheck still says `codebase-memory-mcp` is the stronger
  default substrate and CodeGraphContext is selective.
- It does not prove that the batch artifacts extracted the right facts from
  every covered folder. It proves the local evidence substrate and folder
  resolution are in place.

## Verification Commands

```bash
ls -1 gitrefrepo | sed -n '1,120p'
ls -1 ref-repo-folder
sed -n '1,260p' docs_PRD03/reference-learning/Reference-Shelf-Graph-Evidence-Ledger.md
sed -n '1,120p' docs_PRD03/reference-learning/Reference-Shelf-Graph-Tool-Truthcheck.tsv

/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src

find gitrefrepo/neo4j-src/community -maxdepth 3 -type d | rg 'cypher-planner|cypher-logical-plans|physical-planning|slotted-runtime|interpreted-runtime|runtime-spec-suite|compatibility-spec-suite|procedure-api|procedure-compiler|values'
test -d gitrefrepo/neo4j-graph-examples && echo present || echo absent
ls -1 gitrefrepo | rg '^neo4j-.*-driver-src$'
```

## Skeptical Note

This audit is deliberately narrower than "the spec is done." It only closes the
folder-resolution gap. A future agent could still read the wrong symbols inside
a correctly covered repo. The strongest falsifier would be a counterexample
where a batch artifact cites a spec-named folder that either does not exist on
disk after normalization or does not have a matching repo-root truthcheck row.

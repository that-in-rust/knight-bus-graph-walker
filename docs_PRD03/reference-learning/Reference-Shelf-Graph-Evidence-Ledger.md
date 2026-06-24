# Reference Shelf Graph Evidence Ledger

Date: 2026-06-24

This control artifact records the full graph-evidence sweep required by
`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/V003-Reference-Folder-Learning-Spec.md`.
It turns the spec's repo-family graph-tool contract into a concrete run-state
ledger for every currently named concrete repo target.

## Verdict

- The canonical reference shelf is
  `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo`;
  the legacy alias `ref-repo-folder/` is still not the live clone root.
- The spec currently resolves to `71` concrete local repo targets after
  expanding wildcard driver families and collapsing duplicate mentions.
- Of those `71`, `67` are code-bearing repos and `4` are docs/spec-first repos
  that are intentionally tracked as `GraphToolLowYield`.
- The current full-shelf sweep produced `32` `DualToolReady` repos, `35`
  `CbmReadyCgcTimeout` repos, and `4` `GraphToolLowYield` repos.
- `codebase-memory-mcp` proved broadly tractable across the full code-bearing
  shelf in this pass. CodeGraphContext was valuable but materially more
  timeout-heavy on larger support repos under a safe `90s` cap.
- The sweep completed without leaving long-lived local indexer workers, and the
  transient repo-root `.cgcignore` artifact created earlier was removed after
  the pass.

## Method

Skills used:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

Wrapper commands used:

```bash
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh <ABSOLUTE_REPO_PATH>
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh <ABSOLUTE_REPO_PATH>
```

Execution rules for this full-shelf pass:

1. Resolve every concrete repo named by the current spec into
   `gitrefrepo/<repo>`.
2. Expand `neo4j-*-driver-src` to the five concrete official driver repos
   present locally.
3. Reuse existing successful smoke outputs where the wrapper had produced the
   expected discovery artifacts.
4. Re-run missing or partial repos with a safe process-group timeout of `90s`
   and kill the full process group on expiry so orphaned indexers do not
   accumulate.
5. Record docs/spec shelves as `GraphToolLowYield` instead of pretending a code
   graph adds signal where direct file reading is the true source of truth.

Readiness criteria used in this ledger:

- `codebase-memory ready`: latest run directory contains `index_repository.json`.
- `CodeGraphContext ready`: latest run directory contains `files_query.txt` and
  `functions_find.txt`, proving that the follow-up discovery query stage
  completed after indexing.
- `CbmReadyCgcTimeout`: the repo has a reusable `codebase-memory` run, but the
  safe CodeGraphContext pass timed out before producing the full discovery
  artifacts.

Machine-generated run report for this pass:

- `/tmp/codex-code-intel/spec_repo_sweep_safe_20260624.tsv`

## Scope Summary

| status | count | meaning |
| --- | ---: | --- |
| `DualToolReady` | 32 | Both graph tools have reusable smoke-plus-discovery outputs for the repo. |
| `CbmReadyCgcTimeout` | 35 | `codebase-memory` is reusable, while CodeGraphContext timed out under the safe 90s cap. |
| `GraphToolLowYield` | 4 | The repo is docs/spec-first; direct text reading is the right primary path. |

## Coverage Semantics

- Repo-root scans are sufficient for the spec's named subpaths because each
  wrapper indexes the full repo root and therefore covers the nested folders
  called out elsewhere in the learning plan.
- This ledger is about graph-evidence substrate coverage, not architectural
  approval. A `DualToolReady` repo is easier to study; it is not thereby a
  design endorsement.
- A `CbmReadyCgcTimeout` repo is still materially usable for the learning
  program because one graph substrate is ready and direct `rg` plus file reads
  remain available. It simply means the stricter dual-tool contract is
  operationally expensive on that repo under the safe cap used here.

## DualToolReady

| repo | cbm_status | cbm_run | cgc_status | cgc_run | notes | repo_path |
| --- | --- | --- | --- | --- | --- | --- |
| `age-src` | `ready` | `age-src-20260624-121827` | `ready` | `age-src-20260624-121828` | ready | `gitrefrepo/age-src` |
| `apache-iggy-src` | `ready` | `apache-iggy-src-20260624-120656` | `ready` | `apache-iggy-src-20260624-120703` | ready | `gitrefrepo/apache-iggy-src` |
| `cypher-dsl-src` | `ready` | `cypher-dsl-src-20260624-120654` | `ready` | `cypher-dsl-src-20260624-120655` | ready | `gitrefrepo/cypher-dsl-src` |
| `fjall-src` | `ready` | `fjall-src-20260624-121114` | `ready` | `fjall-src-20260624-121114` | ready | `gitrefrepo/fjall-src` |
| `gapbs-src` | `ready` | `gapbs-src-20260624-125024` | `ready` | `gapbs-src-20260624-125024` | ready | `gitrefrepo/gapbs-src` |
| `gds-agent-src` | `ready` | `gds-agent-src-20260624-120625` | `ready` | `gds-agent-src-20260624-120625` | ready | `gitrefrepo/gds-agent-src` |
| `graph-data-science-src` | `ready` | `graph-data-science-src-20260624-120618` | `ready` | `graph-data-science-src-20260624-120618` | ready | `gitrefrepo/graph-data-science-src` |
| `graphblas-pointers-src` | `ready` | `graphblas-pointers-src-20260624-125038` | `ready` | `graphblas-pointers-src-20260624-125038` | ready | `gitrefrepo/graphblas-pointers-src` |
| `gridgraph-src` | `ready` | `gridgraph-src-20260624-125335` | `ready` | `gridgraph-src-20260624-125335` | ready | `gitrefrepo/gridgraph-src` |
| `ladybug-src` | `ready` | `ladybug-src-20260624-121259` | `ready` | `ladybug-src-20260624-121311` | ready | `gitrefrepo/ladybug-src` |
| `ldbc_graphalytics_platforms_graphblas-src` | `ready` | `ldbc_graphalytics_platforms_graphblas-src-20260624-125824` | `ready` | `ldbc_graphalytics_platforms_graphblas-src-20260624-125825` | ready | `gitrefrepo/ldbc_graphalytics_platforms_graphblas-src` |
| `ligra-src` | `ready` | `ligra-src-20260624-130144` | `ready` | `ligra-src-20260624-130145` | ready | `gitrefrepo/ligra-src` |
| `minigraph-src` | `ready` | `minigraph-src-20260624-130356` | `ready` | `minigraph-src-20260624-130402` | ready | `gitrefrepo/minigraph-src` |
| `neo4j-apoc-procedures-src` | `ready` | `neo4j-apoc-procedures-src-20260624-120631` | `ready` | `neo4j-apoc-procedures-src-20260624-120636` | ready | `gitrefrepo/neo4j-apoc-procedures-src` |
| `neo4j-apoc-src` | `ready` | `neo4j-apoc-src-20260624-120631` | `ready` | `neo4j-apoc-src-20260624-120635` | ready | `gitrefrepo/neo4j-apoc-src` |
| `neo4j-dotnet-driver-src` | `ready` | `neo4j-dotnet-driver-src-20260624-120607` | `ready` | `neo4j-dotnet-driver-src-20260624-120609` | ready | `gitrefrepo/neo4j-dotnet-driver-src` |
| `neo4j-gds-client-src` | `ready` | `neo4j-gds-client-src-20260624-120618` | `ready` | `neo4j-gds-client-src-20260624-120631` | ready | `gitrefrepo/neo4j-gds-client-src` |
| `neo4j-gds-src` | `ready` | `neo4j-gds-src-20260624-120618` | `ready` | `neo4j-gds-src-20260624-120627` | ready | `gitrefrepo/neo4j-gds-src` |
| `neo4j-go-driver-src` | `ready` | `neo4j-go-driver-src-20260624-120607` | `ready` | `neo4j-go-driver-src-20260624-120608` | ready | `gitrefrepo/neo4j-go-driver-src` |
| `neo4j-java-driver-src` | `ready` | `neo4j-java-driver-src-20260624-120602` | `ready` | `neo4j-java-driver-src-20260624-120604` | ready | `gitrefrepo/neo4j-java-driver-src` |
| `neo4j-javascript-driver-src` | `ready` | `neo4j-javascript-driver-src-20260624-120607` | `ready` | `neo4j-javascript-driver-src-20260624-120608` | ready | `gitrefrepo/neo4j-javascript-driver-src` |
| `neo4j-ogm-src` | `ready` | `neo4j-ogm-src-20260624-120655` | `ready` | `neo4j-ogm-src-20260624-120656` | ready | `gitrefrepo/neo4j-ogm-src` |
| `neo4j-python-driver-src` | `ready` | `neo4j-python-driver-src-20260624-120541` | `ready` | `neo4j-python-driver-src-20260624-120546` | ready | `gitrefrepo/neo4j-python-driver-src` |
| `neo4j-src` | `ready` | `neo4j-src-20260624-120354` | `ready` | `neo4j-src-20260624-120536` | ready | `gitrefrepo/neo4j-src` |
| `neo4j-testkit-src` | `ready` | `neo4j-testkit-src-20260624-120354` | `ready` | `neo4j-testkit-src-20260624-120358` | ready | `gitrefrepo/neo4j-testkit-src` |
| `petgraph-src` | `ready` | `petgraph-src-20260624-121656` | `ready` | `petgraph-src-20260624-121657` | ready | `gitrefrepo/petgraph-src` |
| `redb-src` | `ready` | `redb-src-20260624-121207` | `ready` | `redb-src-20260624-121208` | ready | `gitrefrepo/redb-src` |
| `sparsetools-src` | `ready` | `sparsetools-src-20260624-131248` | `ready` | `sparsetools-src-20260624-131249` | ready | `gitrefrepo/sparsetools-src` |
| `sprs-src` | `ready` | `sprs-src-20260624-131311` | `ready` | `sprs-src-20260624-131311` | ready | `gitrefrepo/sprs-src` |
| `thunderrw-src` | `ready` | `thunderrw-src-20260624-131400` | `ready` | `thunderrw-src-20260624-131400` | ready | `gitrefrepo/thunderrw-src` |
| `timely-dataflow-src` | `ready` | `timely-dataflow-src-20260624-131516` | `ready` | `timely-dataflow-src-20260624-131516` | ready | `gitrefrepo/timely-dataflow-src` |
| `tracing-src` | `ready` | `tracing-src-20260624-121526` | `ready` | `tracing-src-20260624-121526` | ready | `gitrefrepo/tracing-src` |

## CbmReadyCgcTimeout

| repo | cbm_status | cbm_run | cgc_status | cgc_run | notes | repo_path |
| --- | --- | --- | --- | --- | --- | --- |
| `apache-arrow-rs-src` | `ready` | `apache-arrow-rs-src-20260624-123607` | `timeout` | `apache-arrow-rs-src-20260624-124554` | cgc timeout 90.0s | `gitrefrepo/apache-arrow-rs-src` |
| `apache-datafusion-src` | `ready` | `apache-datafusion-src-20260624-123607` | `timeout` | `apache-datafusion-src-20260624-124554` | cgc timeout 90.0s | `gitrefrepo/apache-datafusion-src` |
| `clickhouse-src` | `ready` | `clickhouse-src-20260624-123607` | `timeout` | `clickhouse-src-20260624-124724` | cgc timeout 90.0s | `gitrefrepo/clickhouse-src` |
| `cypher-shell-src` | `ready` | `cypher-shell-src-20260624-120654` | `timeout` | `cypher-shell-src-20260624-124724` | cgc timeout 90.0s | `gitrefrepo/cypher-shell-src` |
| `differential-dataflow-src` | `ready` | `differential-dataflow-src-20260624-124115` | `timeout` | `differential-dataflow-src-20260624-124854` | cgc timeout 90.0s | `gitrefrepo/differential-dataflow-src` |
| `duckdb-src` | `ready` | `duckdb-src-20260624-124119` | `timeout` | `duckdb-src-20260624-124854` | cgc timeout 90.0s | `gitrefrepo/duckdb-src` |
| `falkordb-src` | `ready` | `falkordb-src-20260624-125024` | `timeout` | `falkordb-src-20260624-125035` | cgc timeout 90.0s | `gitrefrepo/falkordb-src` |
| `graphblas-src` | `ready` | `graphblas-src-20260624-125040` | `timeout` | `graphblas-src-20260624-125049` | cgc timeout 90.0s | `gitrefrepo/graphblas-src` |
| `graphblas_sparse_linear_algebra-src` | `ready` | `graphblas_sparse_linear_algebra-src-20260624-125205` | `timeout` | `graphblas_sparse_linear_algebra-src-20260624-125205` | cgc timeout 90.0s | `gitrefrepo/graphblas_sparse_linear_algebra-src` |
| `graphchi-cpp-src` | `ready` | `graphchi-cpp-src-20260624-125219` | `timeout` | `graphchi-cpp-src-20260624-125221` | cgc timeout 90.0s | `gitrefrepo/graphchi-cpp-src` |
| `helix-db-src` | `ready` | `helix-db-src-20260624-125346` | `timeout` | `helix-db-src-20260624-125424` | cgc timeout 90.0s | `gitrefrepo/helix-db-src` |
| `igraph-src` | `ready` | `igraph-src-20260624-125351` | `timeout` | `igraph-src-20260624-125352` | cgc timeout 90.0s | `gitrefrepo/igraph-src` |
| `jemalloc-src` | `ready` | `jemalloc-src-20260624-125522` | `timeout` | `jemalloc-src-20260624-125523` | cgc timeout 90.0s | `gitrefrepo/jemalloc-src` |
| `kuzu-src` | `ready` | `kuzu-src-20260624-125554` | `timeout` | `kuzu-src-20260624-125612` | cgc timeout 90.0s | `gitrefrepo/kuzu-src` |
| `lagraph-src` | `ready` | `lagraph-src-20260624-125653` | `timeout` | `lagraph-src-20260624-125654` | cgc timeout 90.0s | `gitrefrepo/lagraph-src` |
| `ldbc_graphalytics-src` | `ready` | `ldbc_graphalytics-src-20260624-125742` | `timeout` | `ldbc_graphalytics-src-20260624-125742` | cgc timeout 90.0s | `gitrefrepo/ldbc_graphalytics-src` |
| `ldbc_snb_interactive_v1_driver-src` | `ready` | `ldbc_snb_interactive_v1_driver-src-20260624-125843` | `timeout` | `ldbc_snb_interactive_v1_driver-src-20260624-125844` | cgc timeout 90.0s | `gitrefrepo/ldbc_snb_interactive_v1_driver-src` |
| `ldbc_snb_interactive_v1_impls-src` | `ready` | `ldbc_snb_interactive_v1_impls-src-20260624-125912` | `timeout` | `ldbc_snb_interactive_v1_impls-src-20260624-125913` | cgc timeout 90.0s | `gitrefrepo/ldbc_snb_interactive_v1_impls-src` |
| `ldbc_snb_interactive_v2_driver-src` | `ready` | `ldbc_snb_interactive_v2_driver-src-20260624-130014` | `timeout` | `ldbc_snb_interactive_v2_driver-src-20260624-130014` | cgc timeout 90.0s | `gitrefrepo/ldbc_snb_interactive_v2_driver-src` |
| `ldbc_snb_interactive_v2_impls-src` | `ready` | `ldbc_snb_interactive_v2_impls-src-20260624-130043` | `timeout` | `ldbc_snb_interactive_v2_impls-src-20260624-130043` | cgc timeout 90.0s | `gitrefrepo/ldbc_snb_interactive_v2_impls-src` |
| `materialize-src` | `ready` | `materialize-src-20260624-130213` | `timeout` | `materialize-src-20260624-130226` | cgc timeout 90.0s | `gitrefrepo/materialize-src` |
| `memgraph-src` | `ready` | `memgraph-src-20260624-130236` | `timeout` | `memgraph-src-20260624-130250` | cgc timeout 90.0s | `gitrefrepo/memgraph-src` |
| `neo4j-browser-src` | `ready` | `neo4j-browser-src-20260624-120655` | `timeout` | `neo4j-browser-src-20260624-130420` | cgc timeout 90.0s | `gitrefrepo/neo4j-browser-src` |
| `neo4rs-src` | `ready` | `neo4rs-src-20260624-120654` | `timeout` | `neo4rs-src-20260624-130500` | cgc timeout 90.0s | `gitrefrepo/neo4rs-src` |
| `networkit-src` | `ready` | `networkit-src-20260624-130550` | `timeout` | `networkit-src-20260624-130552` | cgc timeout 90.0s | `gitrefrepo/networkit-src` |
| `nornicdb-src` | `ready` | `nornicdb-src-20260624-130630` | `timeout` | `nornicdb-src-20260624-130636` | cgc timeout 90.0s | `gitrefrepo/nornicdb-src` |
| `python-graphblas-src` | `ready` | `python-graphblas-src-20260624-130722` | `timeout` | `python-graphblas-src-20260624-130723` | cgc timeout 90.0s | `gitrefrepo/python-graphblas-src` |
| `qdrant-src` | `ready` | `qdrant-src-20260624-130806` | `timeout` | `qdrant-src-20260624-130810` | cgc timeout 90.0s | `gitrefrepo/qdrant-src` |
| `redisgraph-src` | `ready` | `redisgraph-src-20260624-130853` | `timeout` | `redisgraph-src-20260624-130901` | cgc timeout 90.0s | `gitrefrepo/redisgraph-src` |
| `risingwave-src` | `ready` | `risingwave-src-20260624-130940` | `timeout` | `risingwave-src-20260624-130947` | cgc timeout 90.0s | `gitrefrepo/risingwave-src` |
| `rocksdb-src` | `ready` | `rocksdb-src-20260624-120656` | `timeout` | `rocksdb-src-20260624-131031` | cgc timeout 90.0s | `gitrefrepo/rocksdb-src` |
| `rustworkx-src` | `ready` | `rustworkx-src-20260624-131118` | `timeout` | `rustworkx-src-20260624-131118` | cgc timeout 90.0s | `gitrefrepo/rustworkx-src` |
| `snap-src` | `ready` | `snap-src-20260624-131201` | `timeout` | `snap-src-20260624-131212` | cgc timeout 90.0s | `gitrefrepo/snap-src` |
| `tantivy-src` | `ready` | `tantivy-src-20260624-131342` | `timeout` | `tantivy-src-20260624-131344` | cgc timeout 90.0s | `gitrefrepo/tantivy-src` |
| `tikv-src` | `ready` | `tikv-src-20260624-131514` | `timeout` | `tikv-src-20260624-131521` | cgc timeout 90.0s | `gitrefrepo/tikv-src` |

## GraphToolLowYield

| repo | cbm_status | cbm_run | cgc_status | cgc_run | notes | repo_path |
| --- | --- | --- | --- | --- | --- | --- |
| `apache-parquet-format-src` | `skipped` | `` | `skipped` | `` | docs/spec-first repo | `gitrefrepo/apache-parquet-format-src` |
| `ldbc_graphalytics_docs-src` | `skipped` | `` | `skipped` | `` | docs/spec-first repo | `gitrefrepo/ldbc_graphalytics_docs-src` |
| `neo4j-docs-bolt-src` | `skipped` | `` | `skipped` | `` | docs/spec-first repo | `gitrefrepo/neo4j-docs-bolt-src` |
| `opencypher-src` | `skipped` | `` | `skipped` | `` | docs/spec-first repo | `gitrefrepo/opencypher-src` |

## What This Changes For Future Study Passes

- Future agents can safely assume `codebase-memory` coverage exists for every
  currently named code-bearing repo in the spec.
- Future agents should prefer CodeGraphContext first on the `DualToolReady`
  subset and treat the `CbmReadyCgcTimeout` subset as repos where a longer
  custom CGC run is a conscious choice, not an invisible default.
- The four docs/spec shelves should continue to be handled text-first unless
  the learning question specifically requires grammar or protocol-wide
  structural indexing.

## Requirement Impact

| requirement | effect of this artifact |
| --- | --- |
| `REQ-LEARN-040.0` | fully grounded with a full spec-resolved run-state ledger instead of a partial tranche. |
| `REQ-LEARN-036.0` | strengthened because clone coverage is now tied to an explicit graph-evidence status for all currently named concrete repos. |
| `REQ-LEARN-053.0` | reinforced because the ledger continues to canonicalize the live shelf as `gitrefrepo/` while treating `ref-repo-folder/` as a legacy alias only. |

## Skeptical Note

This ledger still should not be overclaimed. It does **not** mean every repo
here has already produced architecture-grade source evidence; it means the
graph-navigation substrate has now been exercised across the full current repo
set named by the spec. The next quality bottleneck is not shelf discovery, but
disciplined evidence extraction and architectural synthesis from the
highest-value repos.

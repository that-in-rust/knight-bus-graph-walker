# GDS Complete Read Plan v1

This plan reduces the Neo4j-family SQLite map from 24,234 files to a complete-read queue for the Rust rewrite research loop.

Inputs:

```text
docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite
docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv
```

Output dossier folder:

```text
docs_PRD03/reference-learning/gds-v2-dossiers
```

## Selection

Complete-read set:

```text
implementation files: 81 files, 20,579 lines
verification oracle files: 30 files, 16,922 lines
total queue: 111 files, 37,501 lines
```

This is intentionally small enough for HQ LLM passes but broad enough to cover the PRD's hard areas: Projection Build Store, immutable OLAP snapshots, graph catalog lifecycle, strict RAM estimation, procedure compatibility, and algorithm execution modes.

## Tier Counts

| tier | files |
| --- | --- |
| T0_IMPLEMENTATION_COMPLETE_READ | 57 |
| T1_IMPLEMENTATION_COMPLETE_READ | 24 |
| T2_VERIFICATION_ORACLE_COMPLETE_READ | 30 |

## Lane Counts

| lane | files |
| --- | --- |
| catalog_lifecycle | 9 |
| projection_build | 13 |
| memory_estimator | 9 |
| procedure_surface | 20 |
| olap_algorithm | 26 |
| write_import_export | 4 |
| verification_oracle | 30 |

## Folder Reading Zones

Read folders as zones, not as commit boundaries. The queue is file-first, but these folders explain why the files matter.

| folder | files | source_files | lines | fan_in | fan_out | degree |
| --- | --- | --- | --- | --- | --- | --- |
| algo | 726 | 459 | 87393 | 2010 | 5323 | 7333 |
| procedures/facade-api | 443 | 437 | 23808 | 2326 | 1793 | 4119 |
| applications/algorithms | 198 | 188 | 21243 | 1629 | 2370 | 3999 |
| core | 380 | 244 | 77464 | 1819 | 2004 | 3823 |
| procedures/algorithms-facade | 246 | 240 | 20919 | 426 | 2276 | 2702 |
| core-api | 61 | 60 | 4430 | 2488 | 137 | 2625 |
| pipeline | 168 | 127 | 18366 | 685 | 1585 | 2270 |
| test-utils | 92 | 75 | 11996 | 1782 | 431 | 2213 |
| ml/ml-algo | 178 | 116 | 19371 | 737 | 1263 | 2000 |
| graph-projection-api | 24 | 17 | 3448 | 1517 | 87 | 1604 |
| ml/ml-core | 137 | 78 | 12082 | 839 | 640 | 1479 |
| progress-tracking | 69 | 51 | 5938 | 1177 | 288 | 1465 |
| memory-usage | 17 | 12 | 2735 | 1150 | 41 | 1191 |
| procedures/pipelines-facade | 83 | 75 | 9637 | 206 | 942 | 1148 |
| annotations | 18 | 15 | 2120 | 1034 | 10 | 1044 |
| proc/community | 123 | 67 | 15420 | 183 | 861 | 1044 |
| applications/graph-store-catalog | 85 | 62 | 11167 | 179 | 807 | 986 |
| collections | 68 | 52 | 10626 | 845 | 138 | 983 |

## The First 35 Files

| priority | lane | folder | file | lines | fan_in | fan_out |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog_lifecycle | core-api | core-api/src/main/java/org/neo4j/gds/api/GraphStore.java | 238 | 453 | 18 |
| 2 | memory_estimator | memory-usage | memory-usage/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MemoryEstimateResult.java | 98 | 307 | 4 |
| 3 | memory_estimator | memory-usage | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java | 50 | 292 | 4 |
| 4 | projection_build | graph-projection-api | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java | 52 | 294 | 1 |
| 5 | projection_build | graph-projection-api | graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java | 83 | 273 | 0 |
| 6 | projection_build | graph-projection-api | graph-projection-api/src/main/java/org/neo4j/gds/NodeLabel.java | 48 | 256 | 1 |
| 7 | procedure_surface | procedures/procedures-facade-api | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java | 44 | 246 | 7 |
| 8 | projection_build | proc/catalog | proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java | 124 | 244 | 7 |
| 9 | olap_algorithm | applications/algorithms | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimings.java | 35 | 211 | 0 |
| 10 | projection_build | graph-projection-api | graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java | 257 | 168 | 3 |
| 11 | procedure_surface | procedures/algorithms-facade | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/community/LocalCommunityProcedureFacade.java | 1290 | 4 | 163 |
| 12 | catalog_lifecycle | core | core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java | 463 | 145 | 17 |
| 13 | procedure_surface | procedures/procedures-facade-api | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/ProcedureConstants.java | 26 | 161 | 0 |
| 14 | memory_estimator | memory-usage | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java | 860 | 139 | 7 |
| 15 | olap_algorithm | config-api | config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java | 104 | 137 | 9 |
| 16 | memory_estimator | memory-usage | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java | 161 | 131 | 1 |
| 17 | olap_algorithm | applications/algorithms | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/NodePropertiesWritten.java | 25 | 129 | 0 |
| 18 | memory_estimator | memory-usage | memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java | 343 | 128 | 1 |
| 19 | procedure_surface | procedures/algorithms-facade | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/LocalCentralityProcedureFacade.java | 1323 | 2 | 114 |
| 20 | projection_build | graph-projection-api | graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java | 152 | 111 | 1 |
| 21 | olap_algorithm | applications/algorithms | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ResultBuilder.java | 49 | 109 | 2 |
| 22 | projection_build | graph-projection-api | graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java | 272 | 107 | 4 |
| 23 | procedure_surface | procedures/algorithms-facade | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/pathfinding/LocalPathFindingProcedureFacade.java | 1103 | 1 | 107 |
| 24 | olap_algorithm | algo-common | algo-common/src/main/java/org/neo4j/gds/Algorithm.java | 47 | 100 | 2 |
| 25 | procedure_surface | procedures/facade-api | procedures/facade-api/algorithms-facade-common/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/MutateStub.java | 55 | 99 | 2 |
| 26 | procedure_surface | procedures/pipelines-facade | procedures/pipelines-facade/src/main/java/org/neo4j/gds/procedures/pipelines/PipelineApplications.java | 964 | 5 | 96 |
| 27 | procedure_surface | procedures/algorithms-facade-api | procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java | 88 | 83 | 7 |
| 28 | catalog_lifecycle | model-catalog-api | model-catalog-api/src/main/java/org/neo4j/gds/core/model/ModelCatalog.java | 162 | 81 | 3 |
| 29 | procedure_surface | neo4j-api | neo4j-api/src/main/java/org/neo4j/gds/api/ProcedureReturnColumns.java | 27 | 82 | 0 |
| 30 | catalog_lifecycle | applications/graph-store-catalog | applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/DefaultGraphCatalogApplications.java | 1160 | 2 | 74 |
| 31 | olap_algorithm | applications/algorithms | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmLabel.java | 170 | 73 | 2 |
| 32 | olap_algorithm | applications/algorithms | applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithms.java | 528 | 9 | 65 |
| 33 | catalog_lifecycle | model-catalog-api | model-catalog-api/src/main/java/org/neo4j/gds/core/model/Model.java | 131 | 65 | 6 |
| 34 | olap_algorithm | applications/algorithms | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/RequestScopedDependencies.java | 50 | 62 | 9 |
| 35 | memory_estimator | memory-usage | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimateDefinition.java | 25 | 69 | 1 |

## Reading Prompt

Use this prompt for each row in `gds-complete-read-queue.tsv`.

```text
You are reading one complete source/test file from the Neo4j GDS reference corpus for the Knight Bus Rust rewrite.

PRD constraint: OLTP reads/writes remain Neo4j-shaped. OLAP/GDS reads must use published immutable OLAP snapshots. Projection Build Store is build/control-plane only. Strict-RAM plans must account for memory and reject before execution if budget cannot fit.

Read the entire file, not just snippets.

Queue row:
- tier: {tier}
- lane: {lane}
- repo: {repo}
- file: {file}
- folder: {folder}
- prompt: {read_prompt}

Also use the SQLite DB to fetch direct dependencies and direct dependents:

```sql
SELECT target_file FROM edges WHERE repo = '{repo}' AND source_file = '{file}' ORDER BY target_file;
SELECT source_file FROM edges WHERE repo = '{repo}' AND target_file = '{file}' ORDER BY source_file;
```

Save a dossier at `{dossier_path}` using the exact schema below. Do not write prose-only summaries. Every claim should cite line ranges or named symbols where possible.
```

## Dossier Schema

Save one Markdown file per queue row in `docs_PRD03/reference-learning/gds-v2-dossiers/`.

```markdown
# <priority> <lane> <file basename>

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | <relative path> |
| lane | <lane> |
| tier | <tier> |
| line_count | <line_count> |
| fan_in / fan_out | <fan_in> / <fan_out> |

## Why This File Matters

One paragraph tied to the PRD. Say whether this is projection, catalog, memory, procedure, algorithm, write/export, or verification evidence.

## Public Contract

Bullets for public types, methods, configs, procedure names, modes, columns, result objects, and visible errors.

## Internal Mechanics

The control flow, state transitions, important collaborators, and what data is read/written/transformed.

## Memory And Storage Implications

Explicit memory terms, allocations, duplicate structures, high-water states, mmap/page-cache implications if visible, and strict-RAM rejection implications.

## Snapshot And Catalog Implications

What this says about graph identity, generation/watermark, immutability, lifecycle, publication, drop/delete, model/result artifacts, or projection build state.

## Verification Oracles

Concrete tests/specs to write in Rust. Use WHEN/THEN/SHALL. Include fixture graph shape and expected output/error.

## Rust Rewrite Notes

Recommended Rust modules, traits, structs, enums, error types, and ownership boundaries. Include what should be no_std/L1 core vs std/L2 vs external/L3.

## Dependencies Read Next

Table of direct dependencies that look important, with reason.

## Dependents As Tests

Table of direct dependents that look like tests/docs/procedure callers, with reason.

## Open Questions

Only questions that block architecture or verification.

## Coding Prompt Unlocked

A short prompt that could be given to an LLM to implement the next smallest Rust artifact test-first.
```

## Save A Rollup Too

After every 10 dossiers, update:

```text
docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md
```

Rollup schema:

```markdown
# GDS v2 Evidence Rollup

## Decisions Strengthened

## New Rust Module Candidates

## Verification Specs Discovered

## Memory Accounting Terms

## Projection/Catalog Invariants

## Unsupported Behavior Registry Candidates

## Next 10 Files
```

## Goal Prompt To Start

```text
/goal Use docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv as the source of truth. Read the first 10 not-yet-done rows completely from gitrefrepo/Neo4j family/neo4j-gds-src. For each row, use the row-specific read_prompt, query docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite for direct dependencies and dependents, and save one Markdown dossier in docs_PRD03/reference-learning/gds-v2-dossiers/ using the schema in docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-plan-v1.md. After the 10 dossiers, update docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md with decisions, Rust module candidates, verification specs, memory terms, projection/catalog invariants, unsupported behavior candidates, and the next 10 recommended files. Do not summarize whole folders. Do not skip line-level evidence. Optimize for verification-first Rust rewrite work.
```

## My Recommendation

Start with exactly 10 files per goal turn. Ten is enough to see cross-file patterns without drowning the context. After 30 implementation dossiers and 10 oracle dossiers, pause and rewrite the PRD L2/L3 architecture around actual evidence.

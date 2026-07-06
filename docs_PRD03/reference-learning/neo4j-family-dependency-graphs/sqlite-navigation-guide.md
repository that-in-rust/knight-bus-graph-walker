# SQLite Navigation Guide For Neo4j Family Graphs

This database is the map room for using Clarity output in LLM coding.

Database:

```text
docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite
```

Builder:

```text
docs_PRD03/reference-learning/build_neo4j_family_graph_db.py
```

## Mental Model

The PRD says OLTP surface area remains Neo4j-shaped, while OLAP/GDS is where the low-RAM rewrite needs new architecture. So use this DB in two modes:

1. **Verification mode**: Bolt, Cypher, procedure ABI, and driver behavior define tests.
2. **Attack mode**: GDS projection, graph catalog, memory estimation, algorithms, and write/export paths define the first code-reading targets.

The raw Clarity graph is file-level. It is not a call graph. Use it to choose what to read next and to avoid losing yourself in the repo forest.

## Current Scale

| Table | Rows |
| --- | ---: |
| `repos` | 20 |
| `files` | 24234 |
| `edges` | 147310 |
| `file_tags` | 69252 |
| `folder_metrics` | 523 |
| `attack_candidates` | 150 |
| `query_recipes` | 9 |

## Largest Repositories

| repo | role | nodes | edges | cycles |
| --- | --- | --- | --- | --- |
| neo4j-src | oltp_kernel | 10738 | 85273 | 215 |
| neo4j-gds-src | olap_gds | 4921 | 30609 | 74 |
| neo4j-ogm-src | application_compatibility | 1127 | 4261 | 64 |
| neo4j-dotnet-driver-src | official_driver | 886 | 3341 | 18 |
| neo4j-java-driver-src | official_driver | 884 | 4378 | 24 |
| neo4j-apoc-procedures-src | procedure_ecosystem | 828 | 3205 | 20 |
| neo4j-gds-client-src | gds_client | 818 | 3873 | 2 |
| neo4j-browser-src | application_compatibility | 695 | 1565 | 8 |
| cypher-dsl-src | cypher_client | 669 | 3173 | 33 |
| neo4j-javascript-driver-src | official_driver | 603 | 1561 | 4 |
| neo4j-apoc-src | procedure_ecosystem | 466 | 1564 | 5 |
| neo4j-python-driver-src | official_driver | 443 | 1621 | 5 |

## Core Tables

| Table | Purpose |
| --- | --- |
| `repos` | One row per Neo4j-family repo, with role and graph size. |
| `files` | One row per file node, with folder bucket, kind, extension, path, and line count when available. |
| `edges` | File-to-file dependency edges from Clarity plus openCypher fallback reference edges. |
| `file_tags` | Heuristic tags such as `plane:olap_gds`, `olap:projection`, `olap:memory`, `surface:bolt_driver`. |
| `file_metrics` | Fan-in, fan-out, total degree. |
| `folder_metrics` | Aggregated file count, line count, and graph degree by folder bucket. |
| `attack_candidates` | Ranked OLAP/GDS files to read first. |
| `query_recipes` | Copy-paste SQL for common navigation tasks. |

## GDS Folder Map

These are not final architecture boundaries. They are reading zones: start with a folder, then pull the ranked files plus direct dependencies/dependents.

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
| concurrency | 15 | 11 | 2805 | 935 | 29 | 964 |
| proc/machine-learning | 83 | 43 | 10390 | 150 | 707 | 857 |
| config-api | 41 | 40 | 2445 | 647 | 170 | 817 |
| proc/path-finding | 104 | 52 | 9578 | 153 | 601 | 754 |
| proc/catalog | 59 | 29 | 8146 | 390 | 335 | 725 |
| pregel | 43 | 34 | 5888 | 416 | 296 | 712 |
| algorithm-specifications | 56 | 55 | 2938 | 68 | 599 | 667 |

## First GDS Attack Candidates

Each candidate should become a small evidence dossier: contract, invariants, verification oracle, and the smallest Rust coding prompt it unlocks.

| rank | lane | file | fan_in | fan_out | degree |
| --- | --- | --- | --- | --- | --- |
| 1 | catalog_lifecycle | core-api/src/main/java/org/neo4j/gds/api/GraphStore.java | 453 | 18 | 471 |
| 2 | memory_estimator | memory-usage/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MemoryEstimateResult.java | 307 | 4 | 311 |
| 3 | memory_estimator | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java | 292 | 4 | 296 |
| 4 | projection_build | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java | 294 | 1 | 295 |
| 5 | projection_build | graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java | 273 | 0 | 273 |
| 6 | projection_build | graph-projection-api/src/main/java/org/neo4j/gds/NodeLabel.java | 256 | 1 | 257 |
| 7 | procedure_surface | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java | 246 | 7 | 253 |
| 8 | projection_build | proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java | 244 | 7 | 251 |
| 9 | olap_algorithm | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimings.java | 211 | 0 | 211 |
| 10 | projection_build | graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java | 168 | 3 | 171 |
| 11 | procedure_surface | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/community/LocalCommunityProcedureFacade.java | 4 | 163 | 167 |
| 12 | catalog_lifecycle | core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java | 145 | 17 | 162 |
| 13 | procedure_surface | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/ProcedureConstants.java | 161 | 0 | 161 |
| 14 | memory_estimator | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java | 139 | 7 | 146 |
| 15 | olap_algorithm | config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java | 137 | 9 | 146 |
| 16 | memory_estimator | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java | 131 | 1 | 132 |
| 17 | olap_algorithm | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/NodePropertiesWritten.java | 129 | 0 | 129 |
| 18 | memory_estimator | memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java | 128 | 1 | 129 |
| 19 | procedure_surface | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/LocalCentralityProcedureFacade.java | 2 | 114 | 116 |
| 20 | projection_build | graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java | 111 | 1 | 112 |
| 21 | olap_algorithm | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ResultBuilder.java | 109 | 2 | 111 |
| 22 | projection_build | graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java | 107 | 4 | 111 |
| 23 | procedure_surface | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/pathfinding/LocalPathFindingProcedureFacade.java | 1 | 107 | 108 |
| 24 | olap_algorithm | algo-common/src/main/java/org/neo4j/gds/Algorithm.java | 100 | 2 | 102 |
| 25 | procedure_surface | procedures/facade-api/algorithms-facade-common/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/MutateStub.java | 99 | 2 | 101 |

## Verification Surface Hubs

Use these for compatibility tests and unsupported-behavior registration. They should not drive the low-RAM storage design directly.

| repo | folder | file | fan_in | fan_out | degree |
| --- | --- | --- | --- | --- | --- |
| neo4j-src | community/cypher | community/cypher/front-end/test-util/src/main/scala/org/neo4j/cypher/internal/util/test_helpers/CypherFunSuite.scala | 684 | 1 | 685 |
| neo4j-src | community/cypher | community/cypher/front-end/expressions/src/main/scala/org/neo4j/cypher/internal/expressions/Expression.scala | 427 | 12 | 439 |
| neo4j-src | community/cypher | community/cypher/cypher-logical-plans/src/main/scala/org/neo4j/cypher/internal/logical/plans/LogicalPlan.scala | 374 | 40 | 414 |
| neo4j-src | community/cypher | community/cypher/interpreted-runtime/src/main/scala/org/neo4j/cypher/internal/runtime/interpreted/pipes/QueryState.scala | 358 | 17 | 375 |
| neo4j-src | community/cypher | community/cypher/front-end/util/src/main/scala/org/neo4j/cypher/internal/util/InputPosition.scala | 359 | 0 | 359 |
| neo4j-gds-src | annotations | annotations/src/main/java/org/neo4j/gds/core/CypherMapWrapper.java | 343 | 1 | 344 |
| neo4j-src | community/cypher | community/cypher/front-end/util/src/main/scala/org/neo4j/cypher/internal/util/attribution/Ids.scala | 340 | 1 | 341 |
| neo4j-src | community/cypher | community/cypher/runtime-util/src/main/scala/org/neo4j/cypher/internal/runtime/CypherRow.scala | 283 | 4 | 287 |
| neo4j-src | community/cypher | community/cypher/front-end/expressions/src/main/scala/org/neo4j/cypher/internal/expressions/LogicalVariable.scala | 278 | 2 | 280 |
| neo4j-src | community/cypher | community/cypher/front-end/ast/src/test/scala/org/neo4j/cypher/internal/ast/AstConstructionTestSupport.scala | 188 | 72 | 260 |
| neo4j-gds-src | procedures/procedures-facade-api | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java | 246 | 7 | 253 |
| neo4j-src | community/cypher | community/cypher/front-end/util/src/main/scala/org/neo4j/cypher/internal/util/Rewritable.scala | 249 | 3 | 252 |
| neo4j-gds-src | proc/catalog | proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java | 244 | 7 | 251 |
| neo4j-apoc-procedures-src | core | core/src/main/java/apoc/util/Util.java | 233 | 16 | 249 |
| neo4j-src | community/cypher | community/cypher/interpreted-runtime/src/main/scala/org/neo4j/cypher/internal/runtime/interpreted/commands/expressions/Expression.scala | 233 | 8 | 241 |
| neo4j-src | community/cypher | community/cypher/front-end/expressions/src/main/scala/org/neo4j/cypher/internal/expressions/SymbolicName.scala | 234 | 4 | 238 |
| neo4j-src | community/cypher | community/cypher/front-end/ast/src/main/scala/org/neo4j/cypher/internal/ast/Clause.scala | 172 | 53 | 225 |
| neo4j-gds-src | test-utils | test-utils/src/main/java/org/neo4j/gds/GdsCypher.java | 205 | 16 | 221 |
| neo4j-src | community/cypher | community/cypher/runtime-util/src/main/scala/org/neo4j/cypher/internal/runtime/ClosingIterator.scala | 214 | 1 | 215 |
| neo4j-apoc-procedures-src | test-utils | test-utils/src/main/java/apoc/util/TestUtil.java | 214 | 0 | 214 |

## Map Levels

Use the DB at four levels:

1. **Repo level**: separate compatibility repos from implementation repos.
2. **Folder level**: choose a reading zone such as graph catalog, projection, memory estimation, or procedure facade.
3. **File level**: read high-degree files first, because they encode shared vocabulary and stable contracts.
4. **Neighborhood level**: read direct dependencies and dependents around a chosen file before writing any summary.

## First Queries

Top OLAP/GDS files to read:

```sql
SELECT rank, repo, file, attack_lane, fan_in, fan_out, total_degree, suggested_read_question
FROM attack_candidates
ORDER BY rank
LIMIT 50;
```

Top GDS folders:

```sql
SELECT folder, file_count, source_file_count, total_lines, fan_in, fan_out, total_degree
FROM v_folder_hubs
WHERE repo = 'neo4j-gds-src'
LIMIT 50;
```

Top GDS hubs:

```sql
SELECT folder, file, kind, line_count, fan_in, fan_out, total_degree
FROM v_file_hubs
WHERE repo = 'neo4j-gds-src'
LIMIT 50;
```

Projection Build Store candidates:

```sql
SELECT rank, file, fan_in, fan_out, suggested_read_question
FROM attack_candidates
WHERE attack_lane = 'projection_build'
ORDER BY rank
LIMIT 50;
```

Strict RAM candidates:

```sql
SELECT rank, file, fan_in, fan_out, suggested_read_question
FROM attack_candidates
WHERE attack_lane = 'memory_estimator'
ORDER BY rank
LIMIT 50;
```

What depends on a file:

```sql
SELECT source_file
FROM edges
WHERE repo = 'neo4j-gds-src'
  AND target_file = 'applications/services/src/main/java/org/neo4j/gds/applications/services/GraphDimensionFactory.java'
ORDER BY source_file;
```

What a file depends on:

```sql
SELECT target_file
FROM edges
WHERE repo = 'neo4j-gds-src'
  AND source_file = 'applications/services/src/main/java/org/neo4j/gds/applications/services/GraphDimensionFactory.java'
ORDER BY target_file;
```

## How To Use This With LLMs

For each attack candidate, ask the LLM to produce exactly four outputs:

```text
1. Contract: what behavior this file defines.
2. Invariants: what a Rust rewrite must preserve.
3. Verification oracle: what test proves it.
4. Coding prompt: the smallest implementation task this unlocks.
```

Do not ask the LLM to summarize whole repos. Ask it to summarize **one ranked candidate plus its direct dependencies and dependents**.

## Practical Next Step

Start with the first 25 rows of `attack_candidates`, but filter to:

```sql
WHERE repo = 'neo4j-gds-src'
  AND attack_lane IN ('projection_build', 'memory_estimator', 'catalog_lifecycle', 'procedure_surface')
```

That gives the shortest path from the current PRD to implementation-grade verification.

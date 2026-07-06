# Neo4j Family Rewrite Read Progress Dashboard (GDS v2 Corpus)

**Generated:** 2026-07-06T10:38:19Z
**Scope:** `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/*` + `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/*`

## Executive intent
This dashboard is the hard continuation artifact for the Rust-rewrite planning phase.
Its purpose is to make the remaining work measurable: **exactly how many files are read, how many remain, what should be read next, and why.**

We are not reading randomly. We read files in dependency-informed priority order using `gds-complete-read-queue.tsv` and preserve every decision in docs + journal checkpoints.

## Current coverage (as of this checkpoint)

- **Total queue rows:** `111`
- **Fully read and documented files:** `60`
- **Remaining files to read:** `51`

| Lane | Total | Completed | Remaining |
|---|---:|---:|---:|
| catalog_lifecycle | 9 | 7 | 2 |
| memory_estimator | 9 | 7 | 2 |
| olap_algorithm | 26 | 17 | 9 |
| procedure_surface | 20 | 15 | 5 |
| projection_build | 13 | 12 | 1 |
| verification_oracle | 30 | 0 | 30 |
| write_import_export | 4 | 2 | 2 |

### Completed files (60)
`1..49`, `53..58`, `59`, `60`, `61`, `62`, `64`

- `001-catalog_lifecycle-GraphStore.md`
- `002-memory_estimator-MemoryEstimateResult.md`
- `003-memory_estimator-MemoryEstimation.md`
- `004-projection_build-RelationshipType.md`
- `005-projection_build-Orientation.md`
- `006-projection_build-NodeLabel.md`
- `007-procedure_surface-GraphDataScienceProcedures.md`
- `008-projection_build-GraphProjectProc.md`
- `009-olap_algorithm-AlgorithmProcessingTimings.md`
- `010-projection_build-ValueType.md`
- `011-procedure_surface-LocalCommunityProcedureFacade.md`
- `012-catalog_lifecycle-GraphStoreCatalog.md`
- `013-procedure_surface-ProcedureConstants.md`
- `014-memory_estimator-MemoryEstimations.md`
- `015-olap_algorithm-AlgoBaseConfig.md`
- `016-memory_estimator-MemoryRange.md`
- `017-olap_algorithm-NodePropertiesWritten.md`
- `018-memory_estimator-Estimate.md`
- `019-procedure_surface-LocalCentralityProcedureFacade.md`
- `020-projection_build-Aggregation.md`
- `021-olap_algorithm-ResultBuilder.md`
- `022-projection_build-DefaultValue.md`
- `023-procedure_surface-LocalPathFindingProcedureFacade.md`
- `024-olap_algorithm-Algorithm.md`
- `025-procedure_surface-MutateStub.md`
- `026-procedure_surface-PipelineApplications.md`
- `027-procedure_surface-AlgorithmsProcedureFacade.md`
- `028-catalog_lifecycle-ModelCatalog.md`
- `029-procedure_surface-ProcedureReturnColumns.md`
- `030-catalog_lifecycle-DefaultGraphCatalogApplications.md`
- `031-olap_algorithm-AlgorithmLabel.md`
- `032-olap_algorithm-CommunityAlgorithms.md`
- `033-catalog_lifecycle-Model.md`
- `034-olap_algorithm-RequestScopedDependencies.md`
- `035-memory_estimator-MemoryEstimateDefinition.md`
- `036-olap_algorithm-AlgorithmSpec.md`
- `037-procedure_surface-AsNodeFunc.md`
- `038-procedure_surface-NewConfigFunction.md`
- `039-procedure_surface-GenericStub.md`
- `040-olap_algorithm-PathFindingAlgorithms.md`
- `041-projection_build-PropertyMapping.md`
- `042-projection_build-GraphProjectConfig.md`
- `043-projection_build-RelationshipProjection.md`
- `044-olap_algorithm-StreamResultBuilder.md`
- `045-olap_algorithm-CommunityAlgorithmsMutateModeBusinessFacade.md`
- `046-olap_algorithm-CentralityAlgorithms.md`
- `047-olap_algorithm-RelationshipsWritten.md`
- `048-procedure_surface-GdsCallable.md`
- `049-procedure_surface-CommunityProcedureFacade.md`
- `053-memory_estimator-ProgressTrackerCreator.md`
- `054-memory_estimator-CommunityAlgorithmsEstimationModeBusinessFacade.md`
- `055-write_import_export-CommunityAlgorithmsWriteModeBusinessFacade.md`
- `056-olap_algorithm-MutateStep.md`
- `057-projection_build-PropertyMappings.md`
- `058-olap_algorithm-AlgorithmProcessingTemplateConvenience.md`
- `059-catalog_lifecycle-GraphStoreCatalogService.md`
- `060-catalog_lifecycle-CSRGraphStore.md`
- `061-projection_build-ElementProjection.md`
- `062-procedure_surface-LocalSimilarityProcedureFacade.md`
- `064-write_import_export-WritePropertyConfig.md`

### Why this is not optional for rewrite safety
1. The GDS rewrite target is OLAP-heavy and currently has **>100k lines** of dependency graph context.
2. We cannot safely preserve compatibility until the top-centrality API seams are read in priority order.
3. Memory rewrite correctness depends on exact estimator grammar and failure semantics, which live in a handful of high-fan-in files already prioritized.
4. Procedure surface behavior is a compatibility boundary: if these are missed, end-to-end behavior drifts and verification becomes unreliable.

## Shreyas-style framing for next actions

### 1) Highest leverage first
Prioritize by **dependency centrality + contract relevance**:
- catalog lifecycle boundary, then projection DSL, then memory and procedure ABI.
- This reduces “surprise contracts” that usually break rewrites (identity, schema, and error handling).

### 2) Preserve behavior via executable contracts before abstraction
For each file: read full file, extract public contract and implied invariants, then map to Rust candidate modules.

### 3) Use verification as a forcing function
Every completed file should emit at least one `WHEN/THEN/SHALL` oracle and a `Rust Rewrite Notes` section with explicit L1/L2/L3 mapping.

### 4) Keep anti-regression focus narrow
Read in batches that keep the same seam alive:
- batch2 begins with catalog lifecycle and procedure/IO edges before algorithm breadth.

### 5) Track spend, not just output
The useful metric is not lines skimmed; it is **files fully read + dossiers complete + next 20 prioritized rows**.

## Next 20 files to read next (priority order)
These are the remaining highest-priority targets from queue (after `64`) in exact order.

| Priority | Lane | File | fan_in/fan_out | Planned dossier |
|---|---|---|---:|---|
| 65 | olap_algorithm | `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MutateNodeProperty.java` | 40 / 8 | `065-olap_algorithm-MutateNodeProperty.md` |
| 66 | olap_algorithm | `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/GraphSageModelTrainer.java` | 19 / 29 | `066-olap_algorithm-GraphSageModelTrainer.md` |
| 67 | procedure_surface | `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/embeddings/LocalNodeEmbeddingsProcedureFacade.java` | 1 / 47 | `067-procedure_surface-LocalNodeEmbeddingsProcedureFacade.md` |
| 68 | memory_estimator | `applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithmsEstimationModeBusinessFacade.java` | 17 / 30 | `068-memory_estimator-PathFindingAlgorithmsEstimationModeBusinessFacade.md` |
| 69 | olap_algorithm | `applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithmsMutateModeBusinessFacade.java` | 9 / 38 | `069-olap_algorithm-CentralityAlgorithmsMutateModeBusinessFacade.md` |
| 70 | write_import_export | `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteStep.java` | 42 / 4 | `070-write_import_export-WriteStep.md` |
| 71 | procedure_surface | `proc/common/src/main/java/org/neo4j/gds/NullComputationResultConsumer.java` | 41 / 5 | `071-procedure_surface-NullComputationResultConsumer.md` |
| 72 | olap_algorithm | `applications/algorithms/node-embeddings/src/main/java/org/neo4j/gds/applications/algorithms/embeddings/NodeEmbeddingAlgorithms.java` | 9 / 37 | `072-olap_algorithm-NodeEmbeddingAlgorithms.md` |
| 73 | procedure_surface | `procedures/facade/src/main/java/org/neo4j/gds/procedures/LocalGraphDataScienceProcedures.java` | 4 / 42 | `073-procedure_surface-LocalGraphDataScienceProcedures.md` |
| 75 | olap_algorithm | `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/StatsResultBuilder.java` | 43 / 2 | `075-olap_algorithm-StatsResultBuilder.md` |
| 76 | olap_algorithm | `applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithmsMutateModeBusinessFacade.java` | 12 / 33 | `076-olap_algorithm-PathFindingAlgorithmsMutateModeBusinessFacade.md` |
| 77 | catalog_lifecycle | `pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java` | 39 / 5 | `077-catalog_lifecycle-PipelineCatalog.md` |
| 78 | procedure_surface | `pregel/src/main/java/org/neo4j/gds/beta/pregel/PregelProcedureConfig.java` | 36 / 8 | `078-procedure_surface-PregelProcedureConfig.md` |
| 79 | write_import_export | `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteToDatabase.java` | 30 / 13 | `079-write_import_export-WriteToDatabase.md` |
| 80 | olap_algorithm | `algo/src/main/java/org/neo4j/gds/algorithms/community/CommunityCompanion.java` | 29 / 14 | `080-olap_algorithm-CommunityCompanion.md` |
| 81 | memory_estimator | `memory-usage/src/main/java/org/neo4j/gds/mem/BitUtil.java` | 42 / 0 | `081-memory_estimator-BitUtil.md` |
| 83 | procedure_surface | `procedures/facade-api/configs/node-embeddings-configs/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageTrainConfig.java` | 27 / 15 | `083-procedure_surface-GraphSageTrainConfig.md` |
| 86 | projection_build | `native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java` | 32 / 9 | `086-projection_build-GraphProjectFromStoreConfig.md` |
| 87 | catalog_lifecycle | `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphCatalogApplications.java` | 9 / 31 | `087-catalog_lifecycle-GraphCatalogApplications.md` |
| 88 | olap_algorithm | `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsStreamModeBusinessFacade.java` | 2 / 38 | `088-olap_algorithm-CommunityAlgorithmsStreamModeBusinessFacade.md` |

## Evidence anchors for this dashboard

- Queue data source: `docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv`
- Execution progress source: dossier paths + file existence under `docs_PRD03/reference-learning/gds-v2-dossiers/`
- Structural source for neighboring files: `docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite`

## Exact commands to recalc progress

```bash
cd /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
python3 - <<'PY'
import csv
from pathlib import Path
from collections import Counter
rows = list(csv.DictReader(open('docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv'), delimiter='\t'))
complete = [r for r in rows if Path(r['dossier_path']).exists() and Path(r['dossier_path']).stat().st_size > 0]
missing = [r for r in rows if r not in complete]
print('TOTAL', len(rows))
print('COMPLETED', len(complete))
print('REMAINING', len(missing))
print('BY_LANE_REMAINING', Counter(r['lane'] for r in missing))
print('NEXT20', [r['priority'] for r in missing[:20]])
PY
```

## Concrete next-step target for this branch

Write all files for priorities **65..88** as the next queued block (20 files; missing priorities are `63`, `74`, `82`, `84`, `85`), each as:

1. full-file read of the source in `gitrefrepo/Neo4j family`
2. dependency neighborhood capture (incoming + outgoing if available)
3. dossier creation with required headings (`Evidence`, `Inference`, `Blocked` labels)
4. oracle + Rust mapping in `L1/L2/L3` terms
5. journal checkpoint after each 5 files

This keeps progress auditable and prevents us from overfitting the rewrite to guesses.

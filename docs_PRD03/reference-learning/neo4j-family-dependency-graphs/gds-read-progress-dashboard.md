# Neo4j Family Rewrite Read Progress Dashboard (GDS v2 Corpus)

**Generated:** 2026-07-06T13:12:00Z  
**Scope:** `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/*` + `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/*`

## Executive intent
This dashboard is the hard continuation artifact for the Rust-rewrite planning phase.
Its purpose is to make the remaining work measurable: **exactly how many files are read, how many remain, what should be read next, and why.**

We are not reading randomly. We read files in dependency-informed priority order using `gds-complete-read-queue.tsv` and preserve every decision in docs + journal checkpoints.

## Current coverage (as of this checkpoint)

- **Total queue rows:** `111`
- **Fully read and documented files:** `10`
- **Remaining files to read:** `101`

| Lane | Total | Completed | Remaining |
|---|---:|---:|---:|
| catalog_lifecycle | 9 | 1 | 8 |
| memory_estimator | 9 | 2 | 7 |
| olap_algorithm | 26 | 1 | 25 |
| procedure_surface | 20 | 1 | 19 |
| projection_build | 13 | 5 | 8 |
| verification_oracle | 30 | 0 | 30 |
| write_import_export | 4 | 0 | 4 |

### Completed files (10)
`1..10`

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
The useful metric is not lines skimmed; it is **files fully read + dossiers complete + next 10 recommended rows**.

## Next 20 files to read next (priority order)
These are the remaining highest-priority targets from queue (11–30) in exact order.

| Priority | Lane | File | fan_in/fan_out | Planned dossier |
|---|---|---|---:|---|
| 11 | procedure_surface | `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/community/LocalCommunityProcedureFacade.java` | 4 / 163 | `011-procedure_surface-LocalCommunityProcedureFacade.md` |
| 12 | catalog_lifecycle | `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java` | 145 / 17 | `012-catalog_lifecycle-GraphStoreCatalog.md` |
| 13 | procedure_surface | `procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/ProcedureConstants.java` | 161 / 0 | `013-procedure_surface-ProcedureConstants.md` |
| 14 | memory_estimator | `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java` | 139 / 7 | `014-memory_estimator-MemoryEstimations.md` |
| 15 | olap_algorithm | `config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java` | 137 / 9 | `015-olap_algorithm-AlgoBaseConfig.md` |
| 16 | memory_estimator | `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java` | 131 / 1 | `016-memory_estimator-MemoryRange.md` |
| 17 | olap_algorithm | `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/NodePropertiesWritten.java` | 129 / 0 | `017-olap_algorithm-NodePropertiesWritten.md` |
| 18 | memory_estimator | `memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java` | 128 / 1 | `018-memory_estimator-Estimate.md` |
| 19 | procedure_surface | `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/LocalCentralityProcedureFacade.java` | 2 / 114 | `019-procedure_surface-LocalCentralityProcedureFacade.md` |
| 20 | projection_build | `graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java` | 111 / 1 | `020-projection_build-Aggregation.md` |
| 21 | olap_algorithm | `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ResultBuilder.java` | 109 / 2 | `021-olap_algorithm-ResultBuilder.md` |
| 22 | projection_build | `graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java` | 107 / 4 | `022-projection_build-DefaultValue.md` |
| 23 | procedure_surface | `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/pathfinding/LocalPathFindingProcedureFacade.java` | 1 / 107 | `023-procedure_surface-LocalPathFindingProcedureFacade.md` |
| 24 | olap_algorithm | `algo-common/src/main/java/org/neo4j/gds/Algorithm.java` | 100 / 2 | `024-olap_algorithm-Algorithm.md` |
| 25 | procedure_surface | `procedures/facade-api/algorithms-facade-common/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/MutateStub.java` | 99 / 2 | `025-procedure_surface-MutateStub.md` |
| 26 | procedure_surface | `procedures/pipelines-facade/src/main/java/org/neo4j/gds/procedures/pipelines/PipelineApplications.java` | 5 / 96 | `026-procedure_surface-PipelineApplications.md` |
| 27 | procedure_surface | `procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java` | 83 / 7 | `027-procedure_surface-AlgorithmsProcedureFacade.md` |
| 28 | catalog_lifecycle | `model-catalog-api/src/main/java/org/neo4j/gds/core/model/ModelCatalog.java` | 81 / 3 | `028-catalog_lifecycle-ModelCatalog.md` |
| 29 | procedure_surface | `neo4j-api/src/main/java/org/neo4j/gds/api/ProcedureReturnColumns.java` | 82 / 0 | `029-procedure_surface-ProcedureReturnColumns.md` |
| 30 | catalog_lifecycle | `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/DefaultGraphCatalogApplications.java` | 2 / 74 | `030-catalog_lifecycle-DefaultGraphCatalogApplications.md` |

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

Write all files for priorities **11..20** as Batch-2 next step, each as:

1. full-file read of the source in `gitrefrepo/Neo4j family`
2. dependency neighborhood capture (incoming + outgoing if available)
3. dossier creation with required headings (`Evidence`, `Inference`, `Blocked` labels)
4. oracle + Rust mapping in `L1/L2/L3` terms
5. journal checkpoint after each 5 files

This keeps progress auditable and prevents us from overfitting the rewrite to guesses.

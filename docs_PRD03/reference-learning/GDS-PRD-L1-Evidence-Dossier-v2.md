# GDS PRD L1 Evidence Dossier v2

Generated: 2026-06-24 22:01:14

Source scope: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src`

This is the second-pass evidence dossier for rewriting `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/prd-l1.md`. It does not overwrite the v1 dossier. The purpose is to make the PRD rewrite easier by turning the Neo4j GDS source into decision tables: procedure surface, projection variants, memory formulas, behavior modes, artifact lifecycles, test oracles, and PRD patch instructions.

## Executive Take

Take the stricter call: the rewrite should not promise "GDS compatible" from CSR/topology alone. It should promise a generated compatibility ledger, strict estimator coverage, deterministic unsupported responses, and a catalog lifecycle model that separates OLTP write-back, OLAP snapshots, GDS graph mutations, model artifacts, pipeline artifacts, and result/file artifacts.

The most important correction to the PRD is mode semantics. In GDS source, a user-visible `*.mutate` procedure can be annotated as Neo4j `READ` while the `@GdsCallable` execution mode mutates the in-memory GDS graph catalog. A `*.write` procedure is the OLTP/write-back danger zone. v003 must model those as different side-effect planes.

## Method

The graph tools were used as candidate finders only. Every architecture-critical claim in this dossier is grounded by direct source paths or by a rerunnable local query plus a direct source pointer.

Confidence labels used:

- `DirectSource`: directly read source or source-derived annotation row from the scoped GDS repo.
- `GraphToolAssisted`: graph tool found the candidate and source was read or source path is provided.
- `CandidateOnly`: useful test/source candidate that must be opened or run before becoming implementation proof.
- `Inference`: PRD implication derived from multiple direct source facts.
- `LowYield`: tool/source pass did not provide enough usable evidence.

## Graph Tool Readiness

- `codebase-memory`: usable after explicit project targeting. Indexed `64,762` nodes and `280,144` edges for the scoped GDS repo. Useful candidate outputs were saved under `/tmp/codex-code-intel/codebase-memory/neo4j-gds-src-20260624-214458/`, especially `search_projection_explicit.json`, `search_memory_explicit.json`, and `search_catalog_explicit.json`.
- `CodeGraphContext`: wrapper was bounded and interrupted after a long run, but the resulting SQLite database answered `stats`: `1` repository, `834` files, `3,532` functions, `832` classes, `87` interfaces, `17` enums, and `711` modules. Treat this as usable for coarse second-opinion stats, not as a complete implementation proof.
- `rg` and direct file reads remain the verification source of truth.

## Coverage Audit

All mandatory thin folders were inspected and are represented in `GDS-V2-Coverage-Audit.tsv`.

- `procedure-collector`: 5 files, primary evidence `procedure-collector/processor/src/main/java/org/neo4j/gds/procedure/ProcedureCollector.java:42`
- `native-projection`: 47 files, primary evidence `native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java:40`
- `legacy-cypher-projection`: 19 files, primary evidence `legacy-cypher-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromCypherConfig.java:35`
- `collections-memory-estimation`: 3 files, primary evidence `collections-memory-estimation/src/main/java/org/neo4j/gds/mem/estimation/HugeSparseCollections.java:30`
- `applications/services`: 3 files, primary evidence `applications/services/src/main/java/org/neo4j/gds/applications/services/GraphDimensionFactory.java:40`
- `open-write-services`: 2 files, primary evidence `open-write-services/src/main/java/org/neo4j/gds/core/write/OpenGdsExportBuildersExtension.java:20`
- `applications/operations`: 4 files, primary evidence `applications/operations/src/main/java/org/neo4j/gds/applications/operations/OperationsApplications.java:35`
- `applications/graph-store-catalog-results`: 18 files, primary evidence `applications/graph-store-catalog-results/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphMemoryUsage.java:40`
- `neo4j-api`: 4 files, primary evidence `neo4j-api/src/main/java/org/neo4j/gds/compat/ProcedureReturnColumns.java:20`
- `neo4j-adapter`: 7 files, primary evidence `neo4j-adapter/src/main/java/org/neo4j/gds/compat/neo4j/InternalReadOps.java:25`
- `neo4j-values`: 10 files, primary evidence `neo4j-values/src/main/java/org/neo4j/gds/values/Neo4jNodePropertyValuesUtil.java:20`
- `gds-values`: 28 files, primary evidence `gds-values/src/main/java/org/neo4j/gds/values/PrimitiveValues.java:20`
- `io`: 143 files, primary evidence `io/core/src/main/java/org/neo4j/gds/core/io/GraphStoreExporter.java:20`
- `proc/machine-learning`: 84 files, primary evidence `proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictWriteProc.java:39`
- `proc/pipeline-catalog`: 7 files, primary evidence `proc/pipeline-catalog/src/main/java/org/neo4j/gds/pipeline/catalog/PipelineDropProc.java:40`

The coverage audit closes the v1 gap where several thin folders were present only in reading logs. The remaining gap is intentionally narrower: per-procedure implementation tracing is still required before claiming runtime support.

## Procedure Surface Join

The procedure join is generated from scoped `src/main/java` annotations: `@Procedure` and `@GdsCallable`. It contains `305` rows.

Mode distribution:

- `READ`: 205
- `WRITE`: 27
- `READ_DEFAULT`: 26
- `STREAM`: 15
- `MUTATE_NODE_PROPERTY`: 11
- `WRITE_NODE_PROPERTY`: 9
- `STATS`: 7
- `MUTATE_RELATIONSHIP`: 3
- `TRAIN`: 1
- `WRITE_RELATIONSHIP`: 1

Top family distribution:

- `gds_public_surface`: 135
- `graph_catalog`: 48
- `machine_learning_pipeline`: 48
- `path_finding`: 34
- `similarity`: 16
- `node_embedding`: 8
- `model_catalog`: 6
- `machine_learning_kge`: 6
- `machine_learning_split_relationships`: 2
- `community`: 2

Sample rows:

| procedure | mode | family | source |
|---|---:|---|---|
| gds.allShortestPaths.delta.mutate | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaMutateProc.java:41 |
| gds.allShortestPaths.delta.mutate.estimate | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaMutateProc.java:50 |
| gds.allShortestPaths.delta.stats | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaStatsProc.java:41 |
| gds.allShortestPaths.delta.stats.estimate | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaStatsProc.java:50 |
| gds.allShortestPaths.delta.stream | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaStreamProc.java:41 |
| gds.allShortestPaths.delta.stream.estimate | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaStreamProc.java:50 |
| gds.allShortestPaths.delta.write | WRITE | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaWriteProc.java:42 |
| gds.allShortestPaths.delta.write.estimate | READ | path_finding | proc/path-finding/src/main/java/org/neo4j/gds/paths/singlesource/delta/AllShortestPathsDeltaWriteProc.java:51 |

Decision: the PRD should require a generated public-surface ledger at build/test time. The procedure collector writes service metadata for `@GdsCallable` classes, so a hand-written support list is likely to drift.

Relevant companion: `GDS-Procedure-Surface-Join-v2.tsv`.

## Projection Variant Matrix

Projection compatibility is not one feature. Native projection, legacy Cypher projection, file import/export, generated graphs, and catalog mutation paths have different grammar and estimator constraints.

Key source-backed facts:

- `GraphProjectFromStoreConfig` normalizes top-level node and relationship properties into projections and validates empty projections and aggregation/property conflicts.
- `GraphDimensionsReader` pulls label/type/property tokens, estimated node counts, highest possible node count, relationship counts, and highest relationship id from Neo4j APIs.
- `GraphProjectFromCypherConfig` rejects native projection keys and uses query strings plus params.
- `CypherRecordLoader` validates mandatory columns and wraps authorization/write attempts as read-only projection failures.
- `CypherQueryEstimator` uses `EXPLAIN` and `EstimatedRows`.

Relevant companion: `GDS-Projection-Variant-Matrix-v2.tsv`.

## Memory Formula Book

Strict RAM compatibility should be expressed as formula coverage, not a single cap. The minimum useful formula taxonomy is:

1. graph-load resident terms,
2. graph-load build scratch terms,
3. algorithm terms,
4. result stream or artifact terms,
5. model/pipeline artifact terms,
6. write-back/export terms,
7. high-water risks,
8. reject condition for absent or incomplete estimators.

Source-backed correction: `AlgorithmFactory.memoryEstimation` has a default implementation that throws `MemoryEstimationNotImplementedException`. Therefore, no supported procedure can rely on "we will estimate later." The PRD must say that absent estimators produce deterministic unsupported responses before execution.

Relevant companion: `GDS-Memory-Formula-Book-v2.tsv`.

## Behavior Mode Semantics

The behavior matrix distinguishes:

- stream/read: row output with no catalog or OLTP mutation,
- mutate: in-memory graph catalog side effect, often exposed as Neo4j `READ`,
- write: OLTP/write-back or export side effect,
- estimate: control-plane memory result,
- train: model artifact creation,
- pipeline catalog: user-scoped catalog mutation that can also be exposed through `READ` procedures,
- operations: progress/log/toggle control plane.

This should become a PRD section because side-effect classification is the line between safe OLAP snapshot reads and writes into the OLTP plane.

Relevant companion: `GDS-Behavior-Mode-Semantics-v2.tsv`.

## Artifact Lifecycle State Machine

The rewrite needs separate state machines for:

- named graphs,
- pipelines,
- models,
- result stores,
- memory reports,
- import/export files.

GDS source does not give v003 the exact generation/watermark identity model. That is a deliberate rewrite addition for immutable published OLAP snapshots, and the PRD should call it an extension rather than an upstream GDS behavior.

Relevant companion: `GDS-Artifact-Lifecycle-State-Machine-v2.tsv`.

## Oracle Extraction Appendix

The best acceptance tests should be extracted from upstream tests and source oracles, not invented from prose. The appendix prioritizes ML estimate tests, memory guard tests, sparse collection tests, projection validation, model/pipeline catalog tests, value adapters, and IO estimator tests.

Relevant companion: `GDS-Oracle-Extraction-Appendix-v2.tsv`.

## PRD Rewrite Patch Plan

The PRD should be patched in these areas:

1. replace broad procedure compatibility with generated support ledger,
2. split projection variants,
3. turn RAM claims into estimator formula/reject contracts,
4. separate Neo4j procedure mode from GDS execution mode,
5. document graph/model/pipeline/result/file artifact lifecycles,
6. add writer/exporter adapter requirements,
7. add property value compatibility tests,
8. extract upstream test oracles before implementation claims.

Relevant companion: `GDS-PRD-Rewrite-Patch-Plan-v2.tsv`.

## Architecture Decisions For `prd-l1.md`

### AD-001: Compatibility Ledger Before Support Claims

The PRD shall require a generated support ledger from source annotations and procedure-collector service metadata. A procedure is not supported until the ledger row has facade/config/result/estimator/side-effect classification and a deterministic unsupported fallback.

### AD-002: Projection Is A Family, Not A Single Store

Projection Build Store remains a build/control plane. Native projection, Cypher projection, file import, and generated graph creation must each have separate acceptance criteria.

### AD-003: Strict RAM Means Estimator Or Reject

Every supported procedure must have estimator coverage for graph load, algorithm state, output artifacts, model/pipeline artifacts, write-back/export buffers, and build scratch. If any major term is absent, the procedure is unsupported for strict-RAM mode.

### AD-004: Mutate Is Not Write

Catalog mutation and Neo4j write-back must be separate modes. `*.mutate` updates GDS graph artifacts; `*.write` writes through OLTP/export adapters.

### AD-005: Generation Identity Is A v003 Extension

Upstream GDS catalog source is user/name-oriented. v003's user/database/name/generation identity is necessary for immutable snapshot publication, but it is not a direct upstream behavior.

## Open Questions

- Which GDS procedure families are in the v003 MVP support tier versus deterministic unsupported tier?
- Will Cypher projection be supported in v003, or rejected with a Neo4j-compatible error shape?
- Which memory budget source is authoritative: heap cap, RSS cap, per-query cap, per-graph cap, or tenant cap?
- Does v003 support model/pipeline catalogs, or are they deterministic unsupported for MVP?
- What is the local equivalent of Neo4j transaction/security behavior for read-only Cypher projection and write-back procedures?

## Verification

Generated companion files:

- `GDS-Procedure-Surface-Join-v2.tsv`
- `GDS-Projection-Variant-Matrix-v2.tsv`
- `GDS-Memory-Formula-Book-v2.tsv`
- `GDS-Behavior-Mode-Semantics-v2.tsv`
- `GDS-Artifact-Lifecycle-State-Machine-v2.tsv`
- `GDS-Oracle-Extraction-Appendix-v2.tsv`
- `GDS-PRD-Rewrite-Patch-Plan-v2.tsv`
- `GDS-V2-Coverage-Audit.tsv`

Verification commands used or intended:

```bash
python3 docs_PRD03/reference-learning/generate_gds_v2_evidence.py
python3 - <<'PY'
from pathlib import Path
files = [
  'GDS-Procedure-Surface-Join-v2.tsv',
  'GDS-Projection-Variant-Matrix-v2.tsv',
  'GDS-Memory-Formula-Book-v2.tsv',
  'GDS-Behavior-Mode-Semantics-v2.tsv',
  'GDS-Artifact-Lifecycle-State-Machine-v2.tsv',
  'GDS-Oracle-Extraction-Appendix-v2.tsv',
  'GDS-PRD-Rewrite-Patch-Plan-v2.tsv',
  'GDS-V2-Coverage-Audit.tsv',
]
base = Path('docs_PRD03/reference-learning')
for name in files:
    p = base / name
    assert p.exists(), name
    assert p.read_text().splitlines()[0], name
PY
git diff --check
```

## Companion File Index

Use the TSV files as the machine-readable substrate and this Markdown file as the synthesis layer. For the PRD rewrite, start from `GDS-PRD-Rewrite-Patch-Plan-v2.tsv`, then use the other tables as evidence backing for each acceptance criterion.

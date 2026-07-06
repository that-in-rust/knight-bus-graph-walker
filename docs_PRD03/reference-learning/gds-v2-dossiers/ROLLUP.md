# GDS v2 Evidence Rollup

This file accumulates the cross-file conclusions from the complete-read queue.

Source queue:

```text
docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv
```

## Decisions Strengthened

- Projection primitives in `graph-projection-api` (`RelationshipType`, `NodeLabel`, `Orientation`) form a coherent immutable identity model: wildcard constants (`__ALL__`) are explicit sentinels and inversion/identity behavior is method-level and deterministic.
- `MemoryEstimation` is not a storage implementation; it is a behavioral contract for estimate composition and therefore should be rewritten as composable traits with safe arithmetic and explicit failure channels in Rust.
- `GraphProjectProc` confirms procedure surfaces are strict dispatch wrappers; catalog control logic remains in façade implementations, while procedure classes carry deprecation/compatibility behavior and delegation discipline.
- `GraphDataScienceProcedures` is the public façade seam; splitting it cleanly in Rust should be a first-order architecture boundary for testability and protocol stability.
- `PipelineApplications` is the orchestration spine for pipeline workflows: it centralizes config parse, mode execution (estimate/stream/mutate/write/train), model-catalog checks, and estimator wiring into one dependency-heavy façade.
- `MutateStub` is the shared mutate contract across procedures and pipelines (`parseConfiguration`, `getMemoryEstimation`, `estimate`, `execute`) and should be a strong trait boundary in Rust.
- `AlgorithmsProcedureFacade` is a pure domain seam: its only purpose is stable family facade routing (`centrality`, `community`, `machineLearning`, `miscellaneous`, `nodeEmbeddings`, `pathFinding`, `similarity`).
- `ModelCatalog` combines strict and permissive APIs (`dropOrThrow`/`drop`, `getUntypedOrThrow`/`getUntyped`) and includes the `EMPTY` no-op sentinel with permissive defaults.
- `ProcedureReturnColumns` acts as a hot-path boolean feature gate for output field inclusion; `EMPTY` returning false for all fields is compatibility-sensitive.
- Community/community algorithm orchestration (`CommunityAlgorithms`) shows a stable execution pattern: algorithm-specific config/task/progress creation then one machinery call path, which argues for one reusable Rust execution abstraction to preserve consistency across 10+ community procedures.
- `Model` is an immutable metadata contract with derived flags (`loaded`, `stored`, `isPublished`) and builder-based factory style; this is a low-risk but high-fidelity seam to model as a typed value object in Rust.
- `RequestScopedDependencies` is a constructor-churn reducer via an immutable record + builder boundary; this is a strong candidate for a dedicated request-context struct in Rust.
- `MemoryEstimateDefinition` is a narrow single-method marker and can be promoted cleanly to a trait object boundary in the estimator graph without adding accidental behavior.
- `AlgorithmSpec` captures the full algorithm seam and default behavior (`validationConfig`, `newConfigFunction`, `releaseProgressTask`) in one place; preserving these defaults reduces registration drift.
- `GenericStub` and `NewConfigFunction` together encode the parser-and-executor contract and reinforce the need to preserve distinct parse modes for estimate vs execute in the Rust executor layer.
- `PathFindingAlgorithms` is the canonical path-finding execution façade: it owns algorithm selection, validation guards, task/progress assembly, and dispatch to machinery.
- `PropertyMapping`, `GraphProjectConfig`, and `RelationshipProjection` together define projection value semantics, parser branches, and invariant validation that must remain tightly aligned for compatibility.
- `StreamResultBuilder` is a high-leverage rendering seam; stream-based output strategy is likely more important than any single concrete DTO here.
- `CommunityAlgorithmsMutateModeBusinessFacade`, `CentralityAlgorithms`, and `MutateNodeProperty` form a reusable execution-template pattern across mutate/write paths.
- `RelationshipsWritten` and related typed count holders indicate a deliberate pattern: metadata is carried as immutable value objects, not generic maps.
- `GdsCallable` and related callable metadata (`ExecutionMode`) imply runtime discoverability requirements that should map to Rust registry metadata in registration-time code.
- `CommunityProcedureFacade` is primarily an API-compatibility contract surface: stream + estimate method families should be generated or mechanically validated to prevent drift.
- `GraphStoreCatalogService` is the mutable-adapter gateway that preserves graph loading/validation order and error-safe graph materialization paths.
- `CSRGraphStore` is a stateful store implementation with synchronized mutation paths, schema mutation in-place, and materialization branches that dominate RAM hot paths.
- `ElementProjection` is the shared projection-normalization seam (`properties`, defaults, toObject) feeding many projection entrypoints.
- `LocalSimilarityProcedureFacade` is a strict procedure-routing glue where parse strategy, return-column gating, and façade delegation must stay stable.
- `WritePropertyConfig` is a tiny but user-visible validation contract where whitespace/empty key handling directly affects write compatibility.

## New Rust Module Candidates

- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
  - `trait MemoryEstimation` with default `components()` and `times(...)`.
- `graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java`
  - `enum Orientation` + `parse` helper with explicit error enum.
- `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java`
  - `enum`/`newtype RelationshipType` + wildcard constant + `project_all`.
- `graph-projection-api/src/main/java/org/neo4j/gds/NodeLabel.java`
  - `enum`/`newtype NodeLabel` + list parser helpers.
- `procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java`
  - `trait GraphDataScienceProcedures`.
- `proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`
  - facade-backed procedure command handlers with deprecation telemetry.
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimings.java`
  - immutable `AlgorithmProcessingTimings` value type.
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalogService.java`
  - `trait GraphStoreCatalogService` for graph existence, guard checks, resource retrieval, and orchestrated validation hooks.
- `core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStore.java`
  - concrete graph store implementation with synchronized schema mutation, graph materialization paths, and controlled validation failures.
- `graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java`
  - `enum ValueType`, visitor trait, and parser returning explicit parse error type.
- `procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java`
  - `struct AlgorithmsProcedureFacade` and pure getters for each algorithm family seam.
- `procedures/facade-api/algorithms-facade-common/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/MutateStub.java`
  - `trait MutateStub<Configuration, Result>` for parse/estimate/execute contract across pipeline/procedure mutate modes.
- `model-catalog-api/src/main/java/org/neo4j/gds/core/model/ModelCatalog.java`
  - `trait ModelCatalog` with typed get/drop/verify/store surfaces and explicit `EMPTY` behavior.
- `neo4j-api/src/main/java/org/neo4j/gds/api/ProcedureReturnColumns.java`
  - `trait ProcedureReturnColumns` + constant empty impl (`EMPTY`).
- `procedures/pipelines-facade/src/main/java/org/neo4j/gds/procedures/pipelines/PipelineApplications.java`
  - orchestration service for pipeline estimate/train/stream/mutate/write flows, model persistence constraints, and estimator dispatch.
- `applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithms.java`
  - `struct PathFindingAlgorithms` with per-algorithm façade methods and shared progress-managed execution.
- `graph-projection-api/src/main/java/org/neo4j/gds/PropertyMapping.java`
  - `struct PropertyMapping` with controlled parser branches and strict validation.
- `graph-projection-api/src/main/java/org/neo4j/gds/ElementProjection.java`
  - `trait ElementProjection` with default `PropertyMappings`, parser helper, and inline-builder conflict checks.
- `config-api/src/main/java/org/neo4j/gds/config/GraphProjectConfig.java`
  - `trait GraphProjectConfig` with default config values, concurrency validation hooks, and `cleansed` output filtering.
- `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipProjection.java`
  - `struct RelationshipProjection` with static construction helpers, sentinel handling, and orientation/aggregation/index invariants.
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/StreamResultBuilder.java`
  - stream result boundary with renderer specialization by result/metadata type.
- `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsMutateModeBusinessFacade.java`
  - shared mutate template for estimation, execution, and result-builder wiring.
- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/similarity/LocalSimilarityProcedureFacade.java`
  - procedure facade module with parse→builder→delegate consistency and return-column gate propagation.
- `config-api/src/main/java/org/neo4j/gds/config/WritePropertyConfig.java`
  - config trait exposing `writeProperty` with converter for `empty_to_null` and whitespace validation.
- `executor/src/main/java/org/neo4j/gds/executor/GdsCallable.java`
  - `struct GdsCallableMeta` + static registry index replacement for procedure discoverability.
- `procedures/facade-api/community-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/community/CommunityProcedureFacade.java`
  - macro-generated trait surface for algorithm mode-family compatibility.

## Verification Specs Discovered

- Add focused tests for `times(...)` behavior of `MemoryEstimation` under neutral and scaled factors.
- Add parser tests for `Orientation.parse(...)` covering valid case-insensitive input, enum passthrough, and unsupported token diagnostics.
- Add façade contract tests for `GraphDataScienceProcedures` accessor wiring and `GraphProjectProc` delegation paths.
- Add `ValueType::from_csv_name` negative tests for unsupported types and unsupported `UNKNOWN`/`UNTYPED_ARRAY` csv conversions.
- Add projection parser/property mapping tests for wildcard plus aggregation edge cases and invalid input shape errors.
- Add callable registration metadata tests to ensure `GdsCallable`-style name/alias/mode parity and deterministic scan behavior.
- Add `GraphStoreCatalogService` tests around `getGraphResources` sequencing, hook execution order, and `removeGraph` callback semantics.
- Add `CSRGraphStore` tests for duplicate-add failures, schema-consistent remove/add flows, and `getGraph`/`getCompositeRelationshipIterator` error branches.
- Add `ElementProjection` tests for default property mapping, non-empty key validation, and mixed-builder invalid states.
- Add `LocalSimilarityProcedureFacade` matrix tests for every route/mode with return-column gate coverage.
- Add `WritePropertyConfig` validation tests for blank values and whitespace rejection.

## Memory Accounting Terms

- `description`: human-facing estimator label.
- `components`: estimated memory subtree members.
- `estimate(...)`: returns `MemoryTree` from `GraphDimensions` + `Concurrency`.
- `factor`: multiplier from `times(long factor)`.
- `fallbackValue`: typed default for a value kind (`DefaultValue`).
- `cypherName` / `csvName`: external serialization names used by procedure/config interfaces.

## Projection/Catalog Invariants

- Procedure classes (`GraphProjectProc`) are intentionally thin dispatchers; catalog state transitions remain inside façade-backed `GraphDataScienceProcedures.graphCatalog()`.
- Estimate calls should be available before catalog mutation and should use the same projection input schema as write calls.
- Wildcard identifiers (`__ALL__`) in `RelationshipType` / `NodeLabel` imply broad reads that must be scoped by query context and permission policy before materialization.
- Deprecated projection APIs should preserve forward-compatible warning paths while being kept as explicit migration points.

## Unsupported Behavior Registry Candidates

- `Orientation.parse(null)` currently triggers `NullPointerException` through `input.getClass()` in unsupported-type branch.
- `ValueType.fromCsvName` rejects unknown CSV names with formatted exception, but has unsupported-csv behavior by design for `UNKNOWN` and some arrays.
- `ValueType.UNKNOWN.csvName()` and `ValueType.UNTYPED_ARRAY.csvName()` behavior are intentionally non-CSV-compatible and should be modeled explicitly in Rust to avoid silent coercion.
- `GraphProjectProc` deprecations still execute successfully with warnings; this migration behavior is an integration contract, not legacy noise to delete immediately.
- `PipelineApplications::nodeRegressionPredictMemoryEstimation` and `nodeRegressionTrainEstimation` throw `MemoryEstimationNotImplementedException`; keep this explicit compatibility gap in the rewrite.
- `ModelCatalog.EMPTY` returns permissive defaults/nulls for missing operations; if absent catalogs are hit in Rust, behavior must be intentionally mirrored or intentionally hardened with migration notes.
- `ProcedureReturnColumns.EMPTY` returning false for all field names can alter output shape assumptions if changed in either direction.
- `PropertyMapping.fromObject` supports only string and map inputs; all other types should remain hard errors in Rust.
- `RelationshipProjection.checkAggregation()` and `check()` enforce strict projection invariants that should stay explicit in rewritten validation paths.
- `GraphProjectConfig` `cleansed(...)` key filtering affects result shape and should be preserved rather than normalized away.
- `GraphStoreCatalogService` guard methods (`ensureGraphExists`/`ensureGraphDoesNotExist`) currently emit message-shaped `IllegalArgumentException`s that should be preserved intentionally or migrated with explicit contract notes.
- `CSRGraphStore` error shape for duplicate properties/types and unknown labels/types is user-visible and must remain compatible with procedure-level expectation.
- `WritePropertyConfig` blank-input normalization and whitespace checks are intentionally opinionated and must remain behaviorally equivalent (`""` to null + reject spaces).

## Next 20 Files

- 65 / `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MutateNodeProperty.java` (olap_algorithm)
- 66 / `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/GraphSageModelTrainer.java` (olap_algorithm)
- 67 / `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/embeddings/LocalNodeEmbeddingsProcedureFacade.java` (procedure_surface)
- 68 / `applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithmsEstimationModeBusinessFacade.java` (memory_estimator)
- 69 / `applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithmsMutateModeBusinessFacade.java` (olap_algorithm)
- 70 / `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteStep.java` (write_import_export)
- 71 / `proc/common/src/main/java/org/neo4j/gds/NullComputationResultConsumer.java` (procedure_surface)
- 72 / `applications/algorithms/node-embeddings/src/main/java/org/neo4j/gds/applications/algorithms/embeddings/NodeEmbeddingAlgorithms.java` (olap_algorithm)
- 73 / `procedures/facade/src/main/java/org/neo4j/gds/procedures/LocalGraphDataScienceProcedures.java` (procedure_surface)
- 75 / `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/StatsResultBuilder.java` (olap_algorithm)
- 76 / `applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithmsMutateModeBusinessFacade.java` (olap_algorithm)
- 77 / `pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java` (catalog_lifecycle)
- 78 / `pregel/src/main/java/org/neo4j/gds/beta/pregel/PregelProcedureConfig.java` (procedure_surface)
- 79 / `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteToDatabase.java` (write_import_export)
- 80 / `algo/src/main/java/org/neo4j/gds/algorithms/community/CommunityCompanion.java` (olap_algorithm)
- 81 / `memory-usage/src/main/java/org/neo4j/gds/mem/BitUtil.java` (memory_estimator)
- 83 / `procedures/facade-api/configs/node-embeddings-configs/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageTrainConfig.java` (procedure_surface)
- 86 / `native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java` (projection_build)
- 87 / `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphCatalogApplications.java` (catalog_lifecycle)
- 88 / `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsStreamModeBusinessFacade.java` (olap_algorithm)

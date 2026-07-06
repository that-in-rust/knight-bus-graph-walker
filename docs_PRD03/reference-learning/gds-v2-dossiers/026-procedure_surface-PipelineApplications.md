# 26 procedure_surface PipelineApplications

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/pipelines-facade/src/main/java/org/neo4j/gds/procedures/pipelines/PipelineApplications.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 26 |
| line_count | 964 |
| fan_in / fan_out | 5 / 96 |
| purpose | Capture procedure ABI: names, modes, columns, config parsing, errors, and unsupported behavior. |
| read_prompt | Read this entire file as a public GDS procedure surface contract. Extract procedure names, modes, columns, config parsing, errors, unsupported behavior, and verification fixtures. |

## Why This File Matters

- This is a high-leverage orchestration class for pipeline-based ML workflows (link prediction, node classification, node regression).
- It hosts both compatibility-critical procedure boundaries and orchestration semantics: mode dispatch, config parsing, estimator wiring, side-effect boundaries, and model lifecycle guards.
- It is a central risk area for a Rust rewrite because it combines catalog/model dependencies, algorithm factories, and execution templates in one object.

## Public Contract

- Constructed via:
  - package-private constructor with many injected collaborators.
  - `static PipelineApplications create(...)` factory that initializes:
    - `GraphStoreService`
    - `ModelPersister`
    - estimators: `LinkPredictionPipelineEstimator`, `NodeClassificationPredictPipelineEstimator`
    - `NodePropertyWriter`
    - trained-model helpers: `TrainedLPPipelineModel`, `TrainedNCPipelineModel`
- Request-scoped collaborators:
  - `GraphStoreCatalogService`, `GraphStoreService`, `ModelCatalog`, `PipelineRepository`, `CloseableResourceRegistry`, `User`, `DatabaseId`, parse/metrics/termination utilities, exporters, return columns, and progress/tracking factories.
- Command methods (package-private):
  - Pipeline editing:
    - `addFeature`, `addNodePropertyToLinkPredictionPipeline`, `addNodePropertyToNodeClassificationPipeline`, `addNodePropertyToNodeRegressionPipeline`
    - `create*TrainingPipeline` (link prediction/classification/regression)
    - `selectFeaturesForClassification`, `selectFeaturesForRegression`
    - `dropAcceptingFailure`, `dropSilencingFailure`, `exists`, `getAll`, `getSingle`
  - Link-prediction flows:
    - `linkPredictionEstimate`, `linkPredictionMutate`, `linkPredictionStream`, `linkPredictionTrain`, `linkPredictionTrainEstimate`
  - Node-classification flows:
    - `nodeClassificationPredictEstimate`, `nodeClassificationPredictMutate`, `nodeClassificationPredictStream`, `nodeClassificationPredictWrite`
    - `nodeClassificationTrain`, `nodeClassificationTrainEstimate`
  - Node-regression flows:
    - `nodeRegressionPredictStream`, `nodeRegressionPredictMutate`, `nodeRegressionTrain`
- Validation and support functions:
  - `ensureTrainingModelCanBeStored(ModelConfig)` before model-writing train modes
  - `validateRelationshipProperty(...)` to prevent conflicting relationship weight properties in a pipeline
- Estimation helpers (private):
  - `linkPredictionMemoryEstimation`, `linkPredictionTrainMemoryEstimation`, `nodeClassificationPredictMemoryEstimation`
  - `nodeClassificationTrainEstimation`
  - `nodeRegressionPredictMemoryEstimation` and `nodeRegressionTrainEstimation` currently throw `MemoryEstimationNotImplementedException`
- Computation builder helpers:
  - `construct*Computation(...)` methods for each flow creating configured computation contexts.

## Internal Mechanics

- Dependency graph in constructor is explicit and broad; almost all orchestration is dependency-driven.
- Most execution methods use shared execution templates:
  - `AlgorithmEstimationTemplate.estimate(...)`
  - `AlgorithmProcessingTemplate.processAlgorithmForMutate(...)`
  - `processAlgorithmForStream(...)`
  - `processAlgorithmForWrite(...)`
  - `processAlgorithmAndAnySideEffects(...)`
- Execution pattern is highly regular:
  1. parse config from raw map using `pipelineConfigurationParser`.
  2. derive `StandardLabel`.
  3. construct computation (`construct*Computation`).
  4. pick result builder and optional side effects/validators.
  5. delegate to appropriate template with memory estimation function.
- `ModelCatalog.verifyModelCanBeStored(...)` is called before train operations that persist a model.
- Validation methods guard incompatible pipeline metadata changes (e.g., relationship weight property conflicts).
- Memory estimations are built via `MemoryEstimations.builder(...)` around domain estimators to produce final `MemoryEstimateResult` in estimate mode.

## Storage and Runtime Behavior

- The class holds references to heavy collaborators (catalogs, repositories, exporters), but keeps execution state mostly local per call.
- Mode operations produce streams or single result objects; this minimizes retained heap per call while allowing lazy iteration.
- Memory estimation for node-regression methods currently fails fast (`MemoryEstimationNotImplementedException`), and that failure path must be preserved in rewrite if API remains same.
- The file couples I/O heavy components (`NodePropertyWriter`, exporters, model persister) into execution paths; these are hot spots for RAM/control boundary modeling.

## Failure / Incompatibility Surfaces

- `nodeRegressionPredictMemoryEstimation` and `nodeRegressionTrainEstimation` throw explicit `MemoryEstimationNotImplementedException`.
- `validateRelationshipProperty(...)` throws `IllegalArgumentException` when multiple non-null `relationshipWeightProperty` values are attempted.
- If `ensureTrainingModelCanBeStored(...)` fails, train-mode persistence behavior must block execution before heavy algorithm steps.
- Any mismatch in graph-name/config parsing or pipeline type lookups will surface as runtime exceptions from repository/config/deps; contract behavior is not wrapped into a custom failure enum.

## Verification Oracles

1. **WHEN** `create(...)` is invoked  
   **THEN** **SHALL** initialize all heavy collaborators in a deterministic request-capable object and return a ready façade.
2. **WHEN** `linkPredictionEstimate`/`nodeClassificationPredictEstimate`/`nodeClassificationTrainEstimate` are called  
   **THEN** **SHALL** route to `algorithmEstimationTemplate` with an appropriately assembled `MemoryEstimations` wrapper.
3. **WHEN** `linkPredictionMutate` is called with valid configuration and graph  
   **THEN** **SHALL** parse mutate config, create computation, and execute through mutate template producing `MutateResult` stream.
4. **WHEN** node-classification prediction stream/write/mutate is called  
   **THEN** **SHALL** set up column-aware builders using `ProcedureReturnColumns` and run through corresponding stream/mutate/write processor.
5. **WHEN** training mode is called for LP or NC with model name and user context  
   **THEN** **SHALL** enforce storage preconditions via `modelCatalog.verifyModelCanBeStored(...)` before persistence side effects.
6. **WHEN** adding node property with conflicting relationship-weight property to existing LP pipeline  
   **THEN** **SHALL** reject with `IllegalArgumentException` detailing existing tasks and property value.
7. **WHEN** regression pipeline estimate methods are still unimplemented  
   **THEN** **SHALL** throw `MemoryEstimationNotImplementedException`.

## Rust Rewrite Notes

- **L1:** create a `PipelineApplications` orchestration module with explicit pure functions for parse/estimate/execute path construction; keep object construction and dependencies explicit and deterministic.
- **L2:** represent operation modes with enums and dedicated handlers for estimate/stream/mutate/write/train to avoid cross-mode state bleed.
- **L3:** preserve template-driven delegation (`AlgorithmEstimationTemplate`/`AlgorithmProcessingTemplate`) as composable abstractions for consistent limits, memory checks, and side effects.
- Keep `MemoryEstimationNotImplemented` explicit to avoid silent behavior changes.

## Dependencies Read Next

- `org.neo4j.gds.procedures.pipelines.*` estimators:
  - `TrainedLPPipelineModel`, `LinkPredictionPipelineEstimator`, `NodeClassificationPredictPipelineEstimator`
- `Application` machinery:
  - `AlgorithmEstimationTemplate`, `AlgorithmProcessingTemplate`, `DimensionTransformer`, `GraphStoreService`, `ProgressTrackerCreator`
- `Procedure facade` family under:
  - `procedures/algorithms/*` + `procedures/facade/*`
- `model-catalog-api` for model persistence behavior.

## Dependents As Tests

- ML pipeline procedures and tests for LP/NC/NR predict/train/mutate/stream/write behavior.
- Tests that assert memory estimation outputs for predict/train endpoints.
- Regression tests around `validateRelationshipProperty` conflict behavior.

## Open Questions

- Should unimplemented regression estimation paths remain hard errors in Rust MVP, or should explicit fallback strategies be introduced?
- Should mutable pipeline mutation methods be transactionally guarded by repository-level locks or by optimistic fail-fast checks?

# 1004 verification_oracle NodeClassificationPredictPipelineExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/machine-learning/src/test/java/org/neo4j/gds/ml/pipeline/node/classification/predict/NodeClassificationPredictPipelineExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 551 |
| fan_in / fan_out | 0 / 49 |

## Why This File Matters

This is a verification oracle for node classification prediction pipeline execution, including estimator validation, progress telemetry, and failure behavior on invalid feature vectors.

## Public Contract

- Lifecycle (`setup`, `tearDown`) manages model catalog/fixtures for repeatable runs (`110`, `128`).
- Prediction contract:
  - `shouldPredict` (`132`)
  - `shouldPredictWithRandomForest` (`187`)
  - `shouldPredictWithNodePropertySteps` (`241`)
  - `progressTracking` (`297`)
  - `validateFeaturesExistOnGraph` (`373`)
  - `failOnInvalidFeatureDimensions` (`517`)
- Memory:
  - `shouldEstimateMemory` (`403`)
  - `shouldEstimateMemoryWithRandomForest` (`469`)

## Internal Mechanics

- Uses `TestProcedureRunner` and pipeline components (`NodePropertyStep`, `NodeClassificationTrainingPipeline`, `NodeClassificationPredictPipeline`) to orchestrate predict execution.
- Asserts output prediction payload integrity (`predictedClasses`, `predictedProbabilities`), graph relationship/type shape, and graph label/state invariants.
- Uses explicit progress logging assertions for INFO-level order in prediction execution.

## Memory And Storage Implications

- Memory estimation tests exercise memory budget checks for base prediction and random-forest model modes.
- Failure mode for invalid feature dimensions indicates an early validation boundary before expensive execution.
- Progress telemetry validates lightweight tracking overhead expectations in production-like execution.

## Snapshot And Catalog Implications

- Implied model and graph-catalog interactions through `ModelCatalog` and `OpenModelCatalog` imports.
- Verifies that prediction does not mutate unexpected graph properties (`hasNodeProperty(..., "degree") is false` in successful paths).

## Verification Oracles

1. **WHEN** `shouldPredict` executes, **THEN** predictions SHALL exist for all input nodes and probability payload SHALL be present.
2. **WHEN** `shouldPredictWithNodePropertySteps` executes, **THEN** predicted links/classes MUST remain graph-shape correct and keep relation graph types stable.
3. **WHEN** `progressTracking` executes, **THEN** INFO logs SHALL contain the expected message sequence exactly.
4. **WHEN** `failOnInvalidFeatureDimensions` executes, **THEN** compute SHALL throw with dimensionality validation error before model execution completes.

## Rust Rewrite Notes

- Keep a dedicated prediction executor test surface for both default and RandomForest model families.
- Preserve memory estimation checks separately from compute path and ensure both share equivalent configuration normalization.
- Represent progress messages as deterministic output IDs for Rust-side compatibility tests.

## Dependencies Read Next

- `executor/ExecutionContext`
- `applications/algorithms/machinery/RequestScopedDependencies`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
- `core-test-utils/.../TestTaskStore` (for progress validation)
- `core/model` model catalog APIs

## Dependents As Tests

- `procedures/pipelines-facade/src/test/java/org/neo4j/gds/procedures/pipelines/LinkPredictionPredictPipelineExecutorTest.java`
- `pipeline/src/test/java/.../LinkPredictionTrainPipelineExecutorTest.java`

## Open Questions

- Should prediction progress logging be asserted as explicit output contract in Rust (current suite implies it is expected)?

## Coding Prompt Unlocked

Create a Rust predictor oracle module with dual-mode (single model / RandomForest) prediction execution and explicit memory-estimation + invalid-feature guard tests.

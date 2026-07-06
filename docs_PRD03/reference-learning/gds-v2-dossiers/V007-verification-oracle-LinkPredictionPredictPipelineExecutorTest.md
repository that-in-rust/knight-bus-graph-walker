# 1007 verification_oracle LinkPredictionPredictPipelineExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/pipelines-facade/src/test/java/org/neo4j/gds/procedures/pipelines/LinkPredictionPredictPipelineExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 648 |
| fan_in / fan_out | 0 / 42 |

## Why This File Matters

This is a core verification oracle for link prediction **predict** executors at the procedures layer, covering default/random-forest prediction outcomes, memory estimation, filtered labels, and feature validation.

## Public Contract

- Core prediction tests:
  - `shouldPredict` (`120`)
  - `shouldPredictWithRandomForest` (`182`)
  - `shouldPredictWithNodePropertySteps` (`229`)
  - `shouldPredictFilteredWithNodePropertySteps` (`276`)
- Progress and memory:
  - `progressTracking` (`334`)
  - `shouldEstimateMemoryWithLogisticRegression` (`428`)
  - `shouldEstimateMemoryWithRandomForest` (`460`)
- Validation:
  - `failOnInvalidFeatureDimension` (`489`)

## Internal Mechanics

- Uses `LinkPredictionPredictPipeline` and model data carriers (`ImmutableLogisticRegressionData`, `ImmutableRandomForestClassifierData`) to build deterministic predict outputs.
- Asserts prediction size and expected graph relationship shape after execution.
- Checks error and estimate paths directly in the same test layer, making it a useful cross-compatibility boundary.

## Memory And Storage Implications

- Memory estimation is algorithm-specific by model family (logistic regression vs random forest) and expected to vary in range.
- Filtered prediction (`shouldPredictFilteredWithNodePropertySteps`) indicates projection/filter overhead and result scope constraints.

## Snapshot And Catalog Implications

- Uses procedure-layer catalog/model surfaces; ties directly to procedure-level contracts for predict executors.
- Confirms model metadata consistency for prediction pipelines and graph scope/label filters.

## Verification Oracles

1. **WHEN** baseline `shouldPredict` runs, **THEN** predicted links count SHALL be stable (`hasSize(3)` style expectation) with consistent label/type outputs.
2. **WHEN** RandomForest path is used, **THEN** prediction and memory behavior SHALL differ predictably from logistic regression baseline.
3. **WHEN** filtered node-property steps are applied, **THEN** only expected labels/nodes should participate.
4. **WHEN** feature dimension is invalid, **THEN** execution SHALL fail with clear message around expected dimension.

## Rust Rewrite Notes

- Implement a shared prediction test harness with explicit model-typed estimators and filter-aware execution.
- Keep memory estimation assertions per model family in dedicated tests rather than a merged path.
- Preserve progress logs as deterministic expected output for procedure-level validation.

## Dependencies Read Next

- `exceptions/MemoryEstimationNotImplementedException`
- `procedures/pipelines-facade` model catalog/pipeline classes
- `executor/ExecutionContext`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`

## Dependents As Tests

- `pipeline/src/test/java/org/neo4j/gds/ml/pipeline/linkPipeline/train/LinkPredictionTrainPipelineExecutorTest.java`
- `proc/machine-learning/src/test/java/.../NodeClassificationPredictPipelineExecutorTest.java`

## Open Questions

- Do we treat `MemoryEstimationNotImplementedException` as fatal contract mismatch or partial-mode unsupported-state in Rust?

## Coding Prompt Unlocked

Build link prediction predict executor validation that differentiates logistic-regression and RandomForest estimation/memory behavior, with strict filtered-node and dimension-error assertions.

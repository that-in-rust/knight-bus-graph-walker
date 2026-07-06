# 1005 verification_oracle LinkPredictionTrainPipelineExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/test/java/org/neo4j/gds/ml/pipeline/linkPipeline/train/LinkPredictionTrainPipelineExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 883 |
| fan_in / fan_out | 0 / 48 |

## Why This File Matters

This is a high-value oracle for link prediction training pipeline execution, especially parameter-space branching, split config behavior, OOB error handling, and progress/logging expectations.

## Public Contract

- Nested suites define topologies:
  - `MonoPartiteTest` (`129`) with methods like `runWithOnlyOOBError`, `validateLinkFeatureSteps`, and `failOnEmptySplitGraph`.
  - `BiPartiteTest` (`607`) with `withAdvancedFiltering`, `splitsRespectTrainConfigFiltering`, and `validateNodePropertiesExistOnNodesInScope`.
- Shared validation and estimation points:
  - `estimateWithDifferentNodePropertySteps` (`476`)
  - `failEstimateOnEmptyParameterSpace` (`518`)
  - `shouldValidNodePropertyStepsContextConfigs` (`542`)
- Progress + lifecycle behavior:
  - `shouldLogProgress` (`354`)
  - split filtering behavior in `validateLinkFeatureSteps` / `splitsRespectTrainConfigFiltering`.

## Internal Mechanics

- Uses `InspectableTestProgressTracker`, `TestNodePropertyStep`, and generated fixtures to validate pipeline graph splitting logic and metrics.
- Executes and inspects training model outputs (`actualModel.customInfo()`, metrics map assertions).
- Verifies invalid split behavior and edge cases by catching expected exceptions and checking exact error strings.

## Memory And Storage Implications

- Multiple memory-estimation parameterized tests exercise node-property-step-dependent memory composition and expected min/max ranges.
- Protects against empty parameter space and incompatible split-configuration memory behavior before execution.

## Snapshot And Catalog Implications

- Involves train pipeline configuration objects and model catalog persistence paths via node-property and split configuration.
- Confirms train/predict flow boundaries are controlled by split config and graph scope.

## Verification Oracles

1. **WHEN** `runWithOnlyOOBError` executes, **THEN** OOB-only failure messaging and model path behavior SHALL be explicit and deterministic.
2. **WHEN** `failOnEmptySplitGraph` executes, **THEN** execution SHALL reject and return expected split-graph error text.
3. **WHEN** `estimateWithDifferentNodePropertySteps` executes, **THEN** estimated memory ranges SHALL track step composition.
4. **WHEN** `withAdvancedFiltering` executes, **THEN** training scopes SHALL honor filtering constraints from split config.

## Rust Rewrite Notes

- Treat this as a verification backbone for link prediction training: test both MonoPartite and BiPartite pipelines.
- Keep split-config validation as a distinct error layer before heavy execution.
- Model custom info/metrics should be asserted structurally (keys like `OUT_OF_BAG_ERROR`) in Rust too.

## Dependencies Read Next

- `config-api/src/main/java/org/neo4j/gds/config/MutateNodePropertyConfig.java`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
- `executor/ExecutionContext`
- `memory/MemoryEstimation`
- `ml.pipeline.train` pipeline modules

## Dependents As Tests

- `pipeline/src/test/java/org/neo4j/gds/ml/pipeline/linkPipeline/LinkPredictionTrainingPipelineTest.java`
- `procedures/pipelines-facade/src/test/java/org/neo4j/gds/procedures/pipelines/LinkPredictionPredictPipelineExecutorTest.java`

## Open Questions

- Do we preserve split-config validation as compile-time config validation or runtime check in the Rust equivalent?

## Coding Prompt Unlocked

Implement link-prediction training executor tests for mono/bi-partite modes with split-graph and parameter-space validation first, then memory-estimation parity, then progress/error regression checks.

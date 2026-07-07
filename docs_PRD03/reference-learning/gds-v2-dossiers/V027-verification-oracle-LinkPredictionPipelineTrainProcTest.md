# 1027 verification_oracle LinkPredictionPipelineTrainProcTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/machine-learning/src/test/java/org/neo4j/gds/ml/linkmodels/pipeline/train/LinkPredictionPipelineTrainProcTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 394 |
| fan_in / fan_out | 0 / 25 |
| seed anchor | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java |

## Why This File Matters

This is end-to-end procedure verification for `gds.beta.pipeline.linkPrediction.train`, covering successful model training, validation failures, deterministic retraining, relationship-weight sensitivity, memory estimation, and `OUT_OF_BAG_ERROR` metric rules.

## Public Contract

- Training returns `modelInfo`, `modelSelectionStats`, `trainMillis`, and `configuration`.
- Successful rows require model metadata, best parameters, metrics, pipeline, node/feature steps, non-negative train time, and config map size `14`.
- `.estimate` yields `bytesMin`, `bytesMax`, `nodeCount`, and `relationshipCount`.
- `MemoryRange` is a non-negative byte range with fallible construction and exact arithmetic operations.

## Fixture Graph Shape

- Sixteen nodes total:
  - Fifteen `:N` nodes `a..o`
  - One `:Ignore` node `p`
- Five cliques of sizes `4`, `3`, `2`, `3`, and `3`.
- One `a -> p` `REL`.
- Four `IGNORED` relationships.
- Node properties include `noise`, `z`, and `array`.
- Relationship property `weight` is mixed explicit/missing.
- Setup projects `g` and `weighted_graph` with labels `N`, `Ignore`, undirected `REL`/`IGNORED`, relationship property `weight`, and node properties.

## Public Contract Evidence

- `setUp` (`110`) registers procedures and projects graphs.
- `tearDown` (`144`) removes pipelines.
- `trainAModel` (`149`) asserts successful procedure output.
- `failsWhenMissingFeatures` (`198`) checks missing feature-step validation.
- `failsWhenMissingNodeProperty` (`212`) checks missing property validation.
- `trainOnNodeLabelFilteredGraph` (`225`) checks label-filtered metrics.
- `trainIsDeterministic` (`270`) checks seeded retraining.
- `trainUsesRelationshipWeight` (`295`) checks weight sensitivity.
- `estimate` (`319`) checks exact memory estimate and counts.
- `cannotUseOOBAsMainMetricWithLR` (`339`) and `canUseOOBAsMainMetricWithRF` (`365`) define OOB metric compatibility.
- `modelData` (`389`) reads model data from `ModelCatalog`.

## Asserted Outputs And Errors

- Successful training returns LinkPrediction model metadata, model-selection stats, non-negative `trainMillis`, and configuration map size `14`.
- Temporary node-property step output `pr` must not remain in graph storage for labels `N` or `Ignore`.
- Missing features error says training requires at least one feature and points to `gds.beta.pipeline.linkPrediction.addFeature`.
- Missing node property error names `[missingNodeProperty]` and says it does not exist in graph or pipeline.
- Label-filtered training expects AUCPR `outerTrain=1.0`, `test=1.0`, validation/train min `0.0`, avg `0.5`, max `1.0`.
- Deterministic retrain compares model data recursively while ignoring `LocalIdMap` and checks class count.
- Weighted graph retrain must produce different model data.
- OOB as first metric rejects LogisticRegression with exact incompatibility message.
- RandomForest-only OOB returns `min_oob=0.6`.
- Estimate asserts `MemoryRange.of(16_392, 502_472)`, `nodeCount=16`, and `relationshipCount=34`.

## Memory And Storage Implications

- `MemoryRange` represents non-negative byte ranges, permits exact zero, rejects negative min/max and `max < min`, and supports exact arithmetic.
- Procedure-level training must clean temporary node properties from the graph catalog.
- Exact min/max memory estimate is part of this procedure oracle.
- OOB metric compatibility changes model-selection memory/execution paths indirectly through trainer choice.

## Snapshot And Catalog Implications

- Procedures are registered in setup.
- Graphs are projected into `GraphStoreCatalog`.
- `PipelineCatalog.removeAll()` runs after each test.
- Trained model data is pulled from injected `ModelCatalog`.
- One test explicitly drops `trainedModel6` before retrain.

## Verification Oracles

1. **WHEN** a pipeline has node property, feature, and logistic-regression candidates, **THEN** `train` SHALL return LinkPrediction model metadata, model-selection stats, non-negative train millis, and 14 config entries.
2. **WHEN** training adds temporary `pr`, **THEN** graph catalog storage SHALL NOT retain `pr` on `N` or `Ignore`.
3. **WHEN** no feature step exists, **THEN** training SHALL fail with the exact addFeature guidance message.
4. **WHEN** feature steps reference absent graph/pipeline properties, **THEN** training SHALL fail naming the missing property.
5. **WHEN** the same seeded train query is rerun after model drop, **THEN** learned logistic data SHALL be equal except local ID mapping.
6. **WHEN** relationship weights differ through `weighted_graph`, **THEN** learned model data SHALL differ.

## Rust Rewrite Notes

- Model `MemoryRange { min, max }` with a fallible constructor.
- Keep temporary node properties under an RAII cleanup guard.
- Make pipeline validation explicit before training.
- Preserve deterministic RNG seed behavior.
- Encode OOB metric compatibility as a typed validation rule.
- Keep golden memory estimates as exact min/max assertions where Java compatibility is desired.

## Dependencies Read Next

- `LinkPredictionPipelineTrainProc`
- `LinkPredictionTrainPipelineExecutor`
- `LinkPredictionTrainConfig`
- `LinkPredictionPipelineAddStepProcs`
- `LinkPredictionPipelineAddTrainerMethodProcs`
- `LinkPredictionPipelineConfigureSplitProc`
- `PipelineCatalog`
- `GraphStoreCatalog`
- `ModelCatalog`
- `MemoryEstimations`

## Open Questions

- Why is estimate relationship count `34` while the projected graph also includes `IGNORED`?
- Is config map size `14` intended public compatibility?
- How stable are the exact AUCPR/OOB constants across algorithm changes?
- Is ignoring only `LocalIdMap` sufficient for deterministic model equivalence?

## Coding Prompt Unlocked

Build Rust link-prediction train procedure tests around successful training output, temporary property cleanup, validation errors, deterministic retraining, relationship-weight sensitivity, OOB compatibility, and exact memory estimate output.

# 1011 verification_oracle LinkPredictionTrainTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/test/java/org/neo4j/gds/ml/pipeline/linkPipeline/train/LinkPredictionTrainTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 924 |
| fan_in / fan_out | 0 / 35 |
| seed anchor | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java |

## Why This File Matters

This is the end-to-end oracle for link-prediction model training: feature extraction, model selection, seeded reproducibility, metric routing, memory estimates, and progress logging.

## Public Contract

- `LinkPredictionTrainResult` exposes `classifier()` and `trainingStatistics()`.
- `LinkPredictionTrain.compute()` performs train feature extraction, random-search/cross-validation, best-model training, train metric computation, test feature extraction, and test metric computation.
- `MemoryRange` is a validated immutable byte range: negative bounds and `max < min` throw, empty range is canonical `(0,0)`, and arithmetic uses checked add/multiply/subtract.
- Split dimensions create synthetic relationship types for `_TEST_`, `_TEST_COMPLEMENT_`, `_TRAIN_`, and `_FEATURE_INPUT_`.

## Fixture Graph Shape

- Fifteen `:N` nodes named `a..o`, original IDs offset from `42`.
- Every node has `scalar` and 5D `array` properties.
- There are 23 directed `:REL` relationships with `label`.
- The fixture contains both positive and negative examples.
- `a -> i` appears twice with opposite labels, so duplicate endpoint pairs are part of the stress fixture.

## Public Contract Evidence

- `trainsAModel` (`222`) asserts model training output, classifier shape, validation score differences, and best parameters.
- `shouldProduceCorrectTrainingStatistics` (`248`) parameterizes OOB behavior and metric maps.
- `shouldProduceCorrectTrainingStatisticsForWinningRF` (`281`) checks RandomForest OOB routing.
- `seededTrain` (`325`) checks recursively equal classifier data across repeated seeded runs.
- `estimateWithDifferentGraphSizes` (`344`) checks memory range sensitivity to relationship count.
- `estimateWithDifferentSplits` (`375`) checks memory range sensitivity to train/test/validation split config.
- `estimateWithParameterSpace` (`401`) checks memory range sensitivity to trainer parameter search space.
- `estimateWithConcurrency` (`431`) checks memory range sensitivity to concurrency.
- `logProgressRF` (`461`), `logProgressLR` (`560`), and `logProgressLRWithRange` (`622`) assert progress-log and debug-log behavior.

## Asserted Outputs And Errors

- Default helper pipeline trains logistic regression with classifier weight size `6`.
- Best parameters are `LogisticRegressionTrainConfig.of({penalty=1, patience=5, tolerance=0.00001})`.
- Train AUCPR statistic count is `MAX_TRIALS + 4`.
- AUCPR train and validation stats are non-null and have expected count.
- Best trial index is between `0` and `10`.
- Winning model outer-train metrics contain AUCPR; winning model test metrics contain only AUCPR unless OOB is used.
- OOB validation/test metrics populate for RandomForest, while OOB train metrics remain null or empty as asserted.
- Repeated seeded training runs produce recursively equal classifier data, with `LocalIdMap` equality handled explicitly.
- Progress logs include exact phase names and rounded AUCPR/loss strings for RandomForest, LogisticRegression, and range-search LogisticRegression.
- Debug logs for the range-search case include exact fold-start, loss, convergence, and fold-finish messages.

## Memory And Storage Implications

- `LinkPredictionTrain.estimate()` uses a compatibility fudge for link feature dimension: `MemoryRange.of(10, 500)`.
- Estimation treats train and test feature lifetimes as a high-water `max`, not a sum.
- Memory ranges include stats maps and best-model stats.
- Graph size estimation shows relationship count drives the range more than node count in the tested cases.
- Parameter-space choices can expand memory dramatically, especially batch-size ranges.

## Snapshot And Catalog Implications

- The GDL fixture is injected and not cataloged by default.
- Non-catalog tests use ephemeral result storage.
- Real model-catalog persistence is outside this file and should be taken from procedure/pipeline executor tests.
- Progress logs form a snapshot surface even though training itself is in-memory here.

## Verification Oracles

1. **WHEN** the default helper pipeline trains on the 15-node graph with seed `1337`, **THEN** the result SHALL be logistic regression with 6 weights and best params `{penalty=1, patience=5, tolerance=0.00001}`.
2. **WHEN** AUCPR is requested, **THEN** train and validation stats SHALL have `MAX_TRIALS + concreteTrainerConfigs` non-null entries.
3. **WHEN** OOB is requested with RandomForest, **THEN** validation/test OOB SHALL populate while train OOB SHALL remain null or empty as asserted.
4. **WHEN** identical seeded training runs execute twice, **THEN** classifier data SHALL be recursively equal.
5. **WHEN** memory estimation varies relationship count, split config, parameter space, or concurrency, **THEN** exact `MemoryRange` min/max values SHALL match the test tables.
6. **WHEN** progress is logged, **THEN** phase names and rounded metric/loss strings SHALL match the RandomForest, LogisticRegression, and range-search expectations.

## Rust Rewrite Notes

- Use a checked `MemoryRange { min, max }`.
- Model training as explicit staged state: extract train features, create folds, select best model, train best model, compute train metrics, evaluate test data.
- Preserve deterministic RNG and class map behavior for `NEGATIVE`/`POSITIVE`.
- Represent relationship counts by `RelationshipType` newtype.
- Keep train/test feature lifetimes as peak `max`, not sum, in estimator parity.
- Make progress events structured and testable before rendering strings.

## Dependencies Read Next

- `LinkFeaturesAndLabelsExtractor`
- `LinkPredictionRelationshipSampler`
- `CrossValidation`
- `TrainingStatistics`
- `RandomSearch`
- `ClassifierTrainerFactory`
- `LinkPredictionTrainPipelineExecutor`
- `LinkPredictionModelInfo`
- `LinkPredictionSplitConfig`
- `MemoryRange`

## Open Questions

- Should Rust match Java allocator-specific byte ranges exactly or maintain platform-adjusted golden ranges?
- Is the duplicate `a -> i` mixed-label edge pair intentional product behavior or only a stress fixture?
- Which procedure layer is the authoritative model-catalog persistence contract?

## Coding Prompt Unlocked

Build Rust link-prediction training oracle tests around deterministic model training, metric routing, memory-estimation tables, split/parameter/concurrency sensitivity, and progress-log parity.

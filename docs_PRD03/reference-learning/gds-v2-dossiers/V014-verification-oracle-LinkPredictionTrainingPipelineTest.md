# 1014 verification_oracle LinkPredictionTrainingPipelineTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/test/java/org/neo4j/gds/ml/pipeline/linkPipeline/LinkPredictionTrainingPipelineTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 492 |
| fan_in / fan_out | 0 / 33 |
| seed anchor | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java |

## Why This File Matters

This verifies the in-memory definition and serializable snapshot of a link-prediction training pipeline: feature steps, node-property steps, split config, trainer candidates, auto-tuning defaults, and relationship-weight derivation.

## Public Contract

- `LinkPredictionTrainingPipeline` extends `TrainingPipeline<LinkFeatureStep>`.
- The constructor installs default `LinkPredictionSplitConfig`.
- The pipeline exposes ordered feature steps, ordered node-property steps, a method-keyed training parameter space, split config, and `toMap()` snapshot.
- Before execution, link prediction requires at least one feature step.
- Relationship weight can be derived from a node-property step config or from a trained model referenced by a node-property step.
- The seed `MemoryEstimation` contract is composable: it has a description, optional components, multiplicative scaling, and `estimate(dimensions, concurrency)`.

## Fixture Shape

- No concrete graph fixture is used; this is a pipeline-state oracle.
- Empty pipeline fixture: no feature steps, no node-property steps, default split config, default auto-tuning, empty training parameter space.
- Feature-step fixture: `HadamardFeatureStep(["a"])`, then `CosineFeatureStep(["b", "c"])`.
- Node-property-step fixture: `gds.testProc.mutate` with `mutateProperty=pr`, then `mutateProperty=pr2`.
- Split config fixture: train/test fractions are set and then overwritten.
- Model catalog fixtures: weighted trained model with `relationshipWeightProperty=derivedWeight`, and unweighted trained model without such a property.

## Public Contract Evidence

- `canCreateEmptyPipeline` (`72`) asserts default empty pipeline state.
- `canAddFeatureSteps` (`84`) asserts ordered feature-step insertion.
- `canAddNodePropertySteps` (`100`) asserts ordered node-property-step insertion.
- `canSetParameterSpace` (`120`) asserts training configs grouped by training method.
- `addMultipleCandidates` (`141`) asserts ordered multiple candidates for logistic regression.
- `canSetSplitConfig` (`161`) and `overridesTheSplitConfig` (`171`) assert split config replacement.
- `deriveRelationshipWeightProperty` (`184`) asserts direct config-based weight derivation.
- `deriveRelationshipWeightPropertyFromTrainedModel` (`224`) asserts model-catalog-based derivation.
- `notDerivePropertyFromUnweightedTrainedModel` (`278`) asserts unweighted model does not provide a weight property.
- Nested `ToMapTest.returnsCorrectDefaultsMap` (`386`) and `returnsCorrectMapWithFullConfiguration` (`416`) define the serialization shape.

## Asserted Outputs And Errors

- New pipeline returns empty feature steps, empty node-property steps, default split config, and empty logistic-regression parameter space.
- Feature and node-property adders preserve insertion order.
- Trainer configs are grouped under `TrainingMethod.LogisticRegression`, `RandomForestClassification`, and `MLPClassification`; candidate order is preserved within each method.
- Setting split config more than once replaces the previous config.
- A node-property step config containing `relationshipWeightProperty` yields that property (`myWeight`).
- A node-property step config containing `modelName` yields `derivedWeight` only when the catalog model train config implements `RelationshipWeightConfig`.
- Default `toMap()` contains only `featurePipeline`, `splitConfig`, `trainingParameterSpace`, and `autoTuningConfig`.
- Full `toMap()` contains node-property-step maps, feature-step maps, split config map, and all trainer config maps.
- No thrown error path is asserted by this file.

## Memory And Storage Implications

- The pipeline itself is in-memory configuration state.
- Production node-property steps must expose memory estimation; the local test step returns `null`, which is a test stub, not a rewrite model.
- `NodePropertyStep.estimate(...)` parses labels/types and returns algorithm memory estimation, falling back to a zero range when estimation is not implemented.
- Trainer candidate space and split config affect future memory estimation through the training executor, even though this file does not assert exact memory ranges.

## Snapshot And Catalog Implications

- `toMap()` is the pipeline snapshot surface; base implementation intentionally excludes type and creation time.
- Model catalog affects relationship-weight inference through `modelName`, username, and train config type.
- Weighted model catalog entries can implicitly supply relationship weight properties; unweighted entries do not.
- The rewrite needs a deterministic representation of method-keyed trainer parameter spaces for snapshot parity.

## Verification Oracles

1. **WHEN** a new link-prediction training pipeline is created, **THEN** it SHALL have empty feature steps, empty node-property steps, default split config, default auto-tuning, and empty supported classification parameter spaces.
2. **WHEN** feature or node-property steps are added, **THEN** accessors and `toMap()` SHALL preserve insertion order.
3. **WHEN** trainer configs are added for multiple methods or multiple candidates, **THEN** the system SHALL group them by training method and preserve candidate order per method.
4. **WHEN** split config is set more than once, **THEN** the latest config SHALL replace the prior config.
5. **WHEN** a node-property step config contains `relationshipWeightProperty`, **THEN** `relationshipWeightProperty(...)` SHALL return that property.
6. **WHEN** a node-property step references a model name, **THEN** the pipeline SHALL derive a relationship weight only if the catalog model's train config implements `RelationshipWeightConfig`.

## Rust Rewrite Notes

- Represent pipeline state as ordered feature steps, ordered executable node-property steps, split config, auto-tuning config, and a deterministic method-to-candidates map.
- Prefer `IndexMap`-style deterministic serialization over plain hash-map output for snapshot tests.
- Replace nullable memory-estimation stubs with `Option<MemoryEstimation>` or an explicit no-op estimator.
- Decide whether multiple node-property steps that imply different relationship weights should preserve Java "first encountered" behavior or become a typed conflict.

## Dependencies Read Next

- `LinkPredictionTrainingPipeline`
- `TrainingPipeline`
- `LinkPredictionSplitConfig`
- `NodePropertyStep`
- `ExecutableNodePropertyStep`
- `HadamardFeatureStep`
- `CosineFeatureStep`
- `TunableTrainerConfig`
- `LogisticRegressionTrainConfig`
- `RandomForestClassifierTrainerConfigImpl`
- `MLPClassifierTrainConfigImpl`
- `ModelCatalog`

## Open Questions

- What should happen if multiple node-property steps imply different relationship weight properties?
- Should duplicate mutate-property validation from `TrainingPipeline` be included in the Rust compatibility suite even though this file does not assert it?
- Should `toMap()` key order be stable in Rust or only key/content equivalent?

## Coding Prompt Unlocked

Build Rust link-prediction pipeline snapshot tests around default state, ordered step insertion, trainer candidate grouping, split config replacement, relationship-weight derivation, and deterministic `to_map` output.

# 083 procedure_surface GraphSageTrainConfig

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/facade-api/configs/node-embeddings-configs/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageTrainConfig.java |
| lane | procedure_surface |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 83 |
| line_count | 166 |
| fan_in / fan_out | 27 / 15 |

## Why This File Matters

This config object defines exactly what a GraphSage training procedure accepts and how it validates defaults and compatibility before algorithm execution.

## Public Contract

- Config mixins:
  - `TrainBaseConfig`
  - `BatchSizeConfig`
  - `IterationsConfig`
  - `ToleranceConfig`
  - `EmbeddingDimensionConfig`
  - `RelationshipWeightConfig`
  - `FeaturePropertiesConfig`
  - `RandomSeedConfig`
- Key defaults:
  - `embeddingDimension = 64`
  - `sampleSizes = [25, 10]`
  - `aggregator = MEAN`
  - `activationFunction = SIGMOID`
  - `tolerance = 1e-4`
  - `learningRate = 0.1`
  - `epochs = 1`
  - `maxIterations = 10`
  - `penaltyL2 = 0`
  - `searchDepth = 5`
  - `negativeSampleWeight = 20`
- `convertToIntSamples` validates all sample entries fit into int range.
- `isMultiLabel()` derived from `projectedFeatureDimension().isPresent()`.
- `propertiesMustExistForEachNodeLabel()` disabled when multi-label mode.
- Validation:
  - `validate()` requires non-empty `featureProperties`.
  - `validateNonEmptyGraph(...)` rejects relationship-less effective graph.
- `of(username,userInput)` factory returns `GraphSageTrainConfigImpl`.

## Internal Mechanics

- Uses annotation-driven config metadata:
  - `@Configuration.IntegerRange`, `@Configuration.DoubleRange`
  - `@Configuration.Key`, `@Configuration.ConvertWith`, `@Configuration.ToMapValue`
- Converters enforce bounded ranges and human-friendly parsing of enum types.
- Validation is split between static checks and runtime graph checks.

## Memory and Storage Implications

- This file is a read-only contract layer with direct implications for downstream memory estimates:
  - layer count (`sampleSizes`)
  - embedding and sample dimensions (`embeddingDimension`, `batchSizes`)
  - relationship requirements and property extraction expectations.

## Snapshot And Catalog Implications

- Procedure-level compatibility checks should ensure this config object exists before model generation.
- In Rust, this becomes a strict typed config schema with derived default values and validation hooks.

## Verification Oracles

1. **WHEN** `featureProperties` is empty, **THEN** validation SHALL throw an `IllegalArgumentException`.
2. **WHEN** no relationship types survive selection, **THEN** graph-store validation SHALL throw `"There should be at least one relationship in the graph."`
3. **WHEN** `sampleSizes` includes values outside int range, **THEN** conversion SHALL fail via `IllegalArgumentException`.
4. **WHEN** `projectedFeatureDimension` is present, **THEN** config shall behave as multi-label mode.

## Rust Rewrite Notes

- Keep validation at config construction boundaries before expensive allocator setup.
- Keep `@Configuration` metadata-equivalent tags in docs or derive-like parser layer.

## Dependencies Read Next

- `GraphSageModelTrainer` / GraphSage algorithm classes
- `GraphSageTrainMemoryEstimateParameters`
- Graph store/relationship availability checks

## Dependents As Tests

- Unit tests for defaults and serialization round-trip.
- Validation tests for empty features and no-relationship graphs.

## Open Questions

- Should `sampleSizes` and `projectedFeatureDimension` be schema-versioned to support backward-compatibility in config migrations?


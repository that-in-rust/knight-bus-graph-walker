# 1017 verification_oracle GraphSageAlgorithmFactoryTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/test/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageAlgorithmFactoryTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 569 |
| fan_in / fan_out | 0 / 30 |
| seed anchor | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java |

## Why This File Matters

This is the oracle for GraphSage inference memory estimation, memory tree shape, mutate resident memory, and training factory dispatch.

## Public Contract

- `GraphSageAlgorithmFactory.memoryEstimation(config)` resolves the model from `ModelCatalog` and estimates memory using the model train config.
- Mutate configs put `resultFeatures` under `residentMemory`.
- Stream configs keep `resultFeatures` under `temporaryMemory`.
- Train factory dispatch uses projected feature dimension to choose multi-label versus single-label training implementation.
- The seed `MemoryEstimations` contract names `residentMemory` and `temporaryMemory` tree sections that are asserted directly.

## Fixture Graph Shape

- Most tests use virtual `GraphDimensions.of(nodeCount)`, not a material graph.
- Node counts include `1`, `10`, `100`, `10_000`, `11_000_000_000`, and `100_000_000_000`.
- Models use `GraphSchema.empty()` and empty `Layer[]`.
- Train-factory selection uses `GdlGraphs.EMPTY`.
- Model catalog entries are set before stream/mutate estimates.

## Public Contract Evidence

- `memoryEstimation` (`84`) independently computes expected min/max memory and compares exact values.
- Computed components include initial features, result features, subgraphs, MEAN aggregator memory, POOL aggregator memory, normalize rows, first-layer memory, and concurrency multiplier.
- `memoryEstimationTreeStructure` (`271`) asserts exact stream tree labels.
- `memoryEstimationMutateTreeStructure` (`321`) asserts resident `resultFeatures` precedes temporary memory.
- `shouldCreateCorrectAlgorithmInstance` (`373`) asserts `MultiLabelGraphSageTrain` versus `SingleLabelGraphSageTrain`.
- `mutateHasPersistentPart` (`528`) asserts exact mutate total and resident memory values.

## Asserted Outputs And Errors

- Stream GraphSage memory estimation across the generated parameter matrix must match the test's independent `MemoryRange` sum.
- Stream memory tree labels must match the exact flattened tree asserted by the test for sample sizes `[1, 2]` and MEAN aggregator.
- Mutate memory tree labels must include `residentMemory/resultFeatures` and exclude temporary `resultFeatures`.
- Mutating 10,000 nodes at concurrency `4` yields total memory `6,861,864..18,593,064` and resident memory `5,320,064`.
- If `projectedFeatureDimension` is present, train factory creates `MultiLabelGraphSageTrain`.
- If `projectedFeatureDimension` is absent, train factory creates `SingleLabelGraphSageTrain`.
- No error path is asserted by this file.

## Memory And Storage Implications

- Estimates are byte-exact and JVM-layout-sensitive.
- `HugeObjectArray.memoryEstimation` is central for initial and result features.
- Mutate stores result embeddings as resident memory; stream treats output as temporary.
- Large node-count cases require checked arithmetic in a Rust rewrite.
- Tree label ordering matters, not just total byte ranges.

## Snapshot And Catalog Implications

- Model catalog presence is mandatory; the test sets models before stream/mutate estimates.
- This file does not exercise graph catalog snapshots.
- The memory tree is itself a snapshot surface; label nesting and order should be preserved for compatibility checks.

## Verification Oracles

1. **WHEN** stream GraphSage is estimated across the generated parameter matrix, **THEN** min/max memory SHALL equal the independent `MemoryRange` sum.
2. **WHEN** sample sizes are `[1, 2]` and aggregator is MEAN, **THEN** stream memory tree labels SHALL match the asserted flattened tree exactly.
3. **WHEN** mutate config is estimated, **THEN** memory tree labels SHALL include `residentMemory/resultFeatures` and temporary memory SHALL omit temporary `resultFeatures`.
4. **WHEN** mutating 10,000 nodes at concurrency `4`, **THEN** total memory SHALL be `6,861,864..18,593,064` and resident memory SHALL be `5,320,064`.
5. **WHEN** `projectedFeatureDimension` is present, **THEN** train factory SHALL build `MultiLabelGraphSageTrain`; otherwise it SHALL build `SingleLabelGraphSageTrain`.
6. **WHEN** batch expansion crosses layers, **THEN** max next-node count SHALL be capped by `nodeCount`.

## Rust Rewrite Notes

- Model `MemoryRange { min, max }` and ordered `MemoryTree` explicitly.
- Preserve tree labels and ordering, not only total bytes.
- Use checked arithmetic for large node-count estimates.
- Recreate Java object/array sizing or pin a compatibility estimator for GDS parity.
- Represent catalog lookup as a trait and stream/mutate memory behavior as an explicit mode.

## Dependencies Read Next

- `GraphSageMemoryEstimateDefinition`
- `GraphSageHelper.embeddingsEstimation`
- `MeanAggregatorMemoryEstimator`
- `PoolAggregatorMemoryEstimator`
- `HugeObjectArray`
- `Estimate`
- `MemoryRange`
- `MemoryTree`
- `GraphSageModelResolver`
- `TrainConfigTransformer`

## Open Questions

- `degreesAsProperty` is iterated but unused in the parameter matrix; is this an accidental dead axis or future compatibility hook?
- The test hard-codes `40L` for `GraphSage.class`; how stable is this across JVM/object-layout changes?
- Mutate numeric constants may be JVM-layout dependent; should Rust compare exact Java-parity bytes or separate structural estimates from byte rendering?

## Coding Prompt Unlocked

Build Rust GraphSage memory oracle tests around exact memory ranges, ordered tree labels, mutate resident memory, catalog model resolution, and single-label versus multi-label train factory dispatch.

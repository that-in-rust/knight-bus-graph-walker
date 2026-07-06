# 1010 verification_oracle GraphSageTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/test/java/org/neo4j/gds/embeddings/graphsage/GraphSageTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 326 |
| fan_in / fan_out | 0 / 39 |

## Why This File Matters

This verifies GraphSage algorithm behavior at a user-visible API level: prediction numerics, train/predict dataset partitioning expectations, logging, and termination behavior.

## Public Contract

- `setUp` fixture setup (`105`).
- Numerical/algorithmic invariants:
  - `shouldNotMakeNanEmbeddings` parameterized across aggregator type (`140`)
  - `differentTrainAndPredictionGraph` (`181`)
  - `testLogging` (`228`)
  - `testTermination` (`291`)
- Ensures output shape/quality through vector checks and node count alignment.

## Internal Mechanics

- Uses GraphSage entry points from `GraphSage`, `GraphSageAlgorithmFactory`, and `GraphSageTrainAlgorithmFactory`.
- Verifies compute output embeddings for non-NaN values and expected size.
- Uses graph-generation helpers (`RandomGraphGenerator`, `RandomGraphGeneratorConfig`) to validate behavior under generated graph input.
- Includes deterministic behavior checks when original IDs and termination are involved.

## Memory And Storage Implications

- Random generator + concurrency paths imply non-trivial memory initialization; logs and termination tests indicate expected performance envelope and safe early-stop handling.
- Confirms no uncontrolled allocations that introduce NaNs under common aggregation modes.

## Snapshot And Catalog Implications

- Validates train/predict separation and graph-scope expectations when different train/predict graphs are used.
- Confirms logging does not alter graph mutation semantics and is safe for deterministic comparison.

## Verification Oracles

1. **WHEN** aggregate variant runs in `shouldNotMakeNanEmbeddings`, **THEN** every embedding value SHALL be non-NaN.
2. **WHEN** train and prediction graphs differ, **THEN** GraphSage SHALL still produce valid size-compatible embeddings.
3. **WHEN** `testTermination` runs, **THEN** termination path shall halt correctly without corrupting output lifecycle.
4. **WHEN** logging is enabled, **THEN** expected message stream SHALL be present and deterministic.

## Rust Rewrite Notes

- Preserve GraphSage embedding semantics by porting non-NaN guarantees and graph-shape validation first.
- Keep train/predict graph boundary explicit in API design (`GraphSageTrainConfigImpl` vs `GraphSageConfig` style shape).
- Add progress/logging assertions to prevent silent regression.

## Dependencies Read Next

- `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSage.java`
- `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageTrainAlgorithmFactory.java`
- `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/TrainConfigTransformer.java`
- `core/concurrency/DefaultPool`
- `core/core-loading/CSRGraphStoreUtil`

## Dependents As Tests

- `algo/src/test/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageAlgorithmFactoryTest.java`
- `algo/src/test/java/org/neo4j/gds/embeddings/fastrp/FastRPTest.java` (related embedding algorithm expectations)

## Open Questions

- Should GraphSage termination behavior be configurable via generic termination flags or algorithm-specific config in Rust?

## Coding Prompt Unlocked

Build Rust GraphSage oracle tests around non-NaN embeddings, train-vs-predict graph separation, and termination-safe execution with deterministic logging assertions.

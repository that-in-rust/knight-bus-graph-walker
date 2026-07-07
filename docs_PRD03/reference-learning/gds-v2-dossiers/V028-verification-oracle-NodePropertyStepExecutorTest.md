# 1028 verification_oracle NodePropertyStepExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/test/java/org/neo4j/gds/ml/pipeline/NodePropertyStepExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 284 |
| fan_in / fan_out | 0 / 25 |
| seed anchor | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java |

## Why This File Matters

This isolates `NodePropertyStepExecutor` behavior for executing multiple node-property steps, validating context labels/types, cleaning temporary properties, progress logging, and estimating peak memory for step pipelines.

## Public Contract

- Executor mutates `GraphStore` with step outputs via `executeNodePropertySteps`.
- It removes step outputs via `cleanupIntermediateProperties`.
- It validates context configs through `validNodePropertyStepsContextConfigs`.
- It emits progress tasks.
- It estimates memory with `estimateNodePropertySteps`.
- The seed `MemoryEstimation` contract includes `description()`, default empty `components()`, `times(long)`, and `estimate(GraphDimensions, Concurrency)`.

## Fixture Graph Shape

- Injected undirected GDL graph:
  - Nodes: `a:A`, `b1:B1`, `b2:B2`
  - `R1` edges: `a-b1`, `a-b2`, `b1-b2`
  - `R2` edge: `b1-b2`
- Local two-node `age` graph is used for mutation/cleanup and progress tests.

## Public Contract Evidence

- `executeSeveralSteps` (`73`) checks multiple step execution and cleanup.
- `executeWithContext` (`103`) checks context-restricted computation.
- `failWithInvalidContextConfigs` (`168`) checks missing label/type validation.
- `progressLogging` (`215`) checks exact progress sequence.
- `memoryEstimation` (`250`) checks peak memory behavior.
- Inner config methods `graphName` (`274`) and `usernameOverride` (`279`) define minimal config behavior.

## Asserted Outputs And Errors

- Initial property keys are exactly `age`.
- After two steps, property keys are exactly `age`, `prop1`, and `prop2`.
- After cleanup, property keys return to exactly `age`.
- Context execution creates `r1_degree`, `r2_sum`, and `r2_b1_sum` with expected graph values and undirected reverse edges.
- Invalid relationship type error names `INVALID` and available `['R1', 'R2']`.
- Invalid label error names `INVALID` and available `['A', 'B1', 'B2']`.
- Progress log sequence is exactly start, each proc start/100%/finished, and final finished.
- Memory estimation over fixed ranges `42` and `1337` yields `MemoryRange.of(1337)`.

## Memory And Storage Implications

- Node-property steps write directly into `GraphStore`.
- Cleanup restores the original property-key set for the simple case.
- Context filtering can produce sparse per-node properties.
- Memory behavior appears peak/largest-step based rather than additive; the test expects `1337`, not `42 + 1337`.

## Snapshot And Catalog Implications

- Uses injected in-memory `GraphStore`, not graph catalog procedures.
- Memory estimation passes a fresh `OpenModelCatalog` and does not assert model/catalog writes.
- Graph equality uses union graph snapshots.
- Sparse node properties must be represented distinctly from zero-valued properties.

## Verification Oracles

1. **WHEN** two node-property steps run, **THEN** graph storage SHALL contain both new mutate properties.
2. **WHEN** cleanup runs for those steps, **THEN** graph storage SHALL return to the original property-key set.
3. **WHEN** step context restricts relationship types and node labels, **THEN** computed properties SHALL match the expected sparse undirected graph snapshot.
4. **WHEN** a context relationship type is missing, **THEN** validation SHALL throw `IllegalArgumentException` with missing and available relationship types.
5. **WHEN** a context node label is missing, **THEN** validation SHALL throw `IllegalArgumentException` with missing and available labels.
6. **WHEN** estimating multiple node-property steps, **THEN** executor SHALL report peak memory equal to the largest step estimate.

## Rust Rewrite Notes

- Define an `ExecutableNodePropertyStep` trait with typed context labels/types, mutate property, execute, and estimate methods.
- Make cleanup guard-based.
- Keep context validation deterministic in error ordering.
- Represent sparse node properties explicitly.
- Implement progress as nested task events.
- Compose memory estimates as peak `MemoryRange`, not sum, unless additional implementation evidence says otherwise.

## Dependencies Read Next

- `NodePropertyStepExecutor`
- `ExecutableNodePropertyStep`
- `ExecutableNodePropertyStepTestUtil`
- `SumNodePropertyStepConfig`
- Generated `SumNodePropertyStepConfigImpl`
- `MemoryEstimations`
- `MemoryTree`
- `GraphDimensions`
- Progress tracker task classes
- `OpenModelCatalog`

## Open Questions

- Does `cleanupIntermediateProperties` remove all step outputs or only outputs marked intermediate in production pipelines?
- Does `MemoryTree` record child components while reporting peak usage?
- Do missing sparse properties default to absent or zero in downstream algorithms?
- Does constructor argument ordering encode source graph labels versus context labels in a way Rust should make type-safe?

## Coding Prompt Unlocked

Build Rust node-property step executor tests around multi-step mutation, cleanup, sparse context output, deterministic validation errors, nested progress logs, and peak memory estimation.

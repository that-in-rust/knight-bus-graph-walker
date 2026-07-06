# 1002 verification_oracle ModularityOptimizationMutateProcTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/community/src/integrationTest/java/org/neo4j/gds/modularityoptimization/ModularityOptimizationMutateProcTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 570 |
| fan_in / fan_out | 0 / 55 |

## Why This File Matters

This test file is the modularity-optimization mutate verification oracle, especially around seeded execution, convergence metadata, and write-back behavior. It is high-signal for algorithm result contracts and mutable-write consistency.

## Public Contract

- Setup/teardown methods (`setup`, `tearDown`) enforce graph fixture isolation (`133`, `145`).
- Mutate behavior is covered by:
  - `testMutate` (`150`)
  - `testMutateWeighted` (`172`)
  - `testMutateSeeded` (`194`)
  - `testMutateTolerance` (`216`)
  - `testMutateIterations` (`235`)
- Estimation and write paths:
  - `testMutateEstimate` (`252`)
  - `testWriteBackGraphMutationOnFilteredGraph` (`269`)
- Graph state/mutation and errors:
  - `testGraphMutation` (`320`)
  - `testGraphMutationOnFilteredGraph` (`335`)
  - `testMutateFailsOnExistingToken` (`364`)
  - `testRunOnEmptyGraph` (`394`)

## Internal Mechanics

- Uses GDS procedure call plumbing and `GdsCypher` to exercise mutate flows.
- Asserts on output row fields (`didConverge`, `modularity`, `communityCount`, `ranIterations`, byte estimate fields where applicable).
- Writes back mutation results to graph and asserts expected resulting graph topology and labels.
- Applies seeding and tolerance checks that influence algorithm path selection and iteration behavior.

## Memory And Storage Implications

- Verifies memory estimate path (`testMutateEstimate`) and byte estimate bounds assertions (`bytesMin`, `bytesMax`) in row results.
- Explicitly validates that mutation writes preserve node-property ownership by label (expected vs non-target labels).
- Failure and empty-graph tests reduce accidental memory churn by enforcing early rejection paths.

## Snapshot And Catalog Implications

- Reinforces graph catalog expectations around graph mutation after projection and filtered graph mutation behavior.
- Confirms deterministic cleanup of temporary/loaded graphs across tests to avoid contamination.

## Verification Oracles

1. **WHEN** `testMutateSeeded` runs, **THEN** output SHALL remain deterministic with matching community partitions under seed and include expected convergence metadata.
2. **WHEN** tolerance or iteration knobs are changed (`testMutateTolerance`, `testMutateIterations`), **THEN** algorithm outputs SHALL reflect those controls while preserving schema.
3. **WHEN** estimated memory is requested (`testMutateEstimate`), **THEN** returned byte estimate fields SHALL satisfy asserted bounds.
4. **WHEN** an existing token collision is encountered (`testMutateFailsOnExistingToken`), **THEN** it SHALL fail with `IllegalArgumentException` and preserve mutation safety.

## Rust Rewrite Notes

- Model modularity optimization mutate mode as separate estimate/compute/estimate+write pathways with shared config shape.
- Keep convergence metadata as explicit first-class fields (`didConverge`, `ranIterations`) for cross-language contract checks.
- Preserve seed/tolerance/iteration configuration paths through the same config parsing entrypoints.

## Dependencies Read Next

- `applications/algorithms/machinery/DefaultAlgorithmProcessingTemplate`
- `applications/algorithms/machinery/MemoryGuard`
- `applications/algorithms/machinery/RequestScopedDependencies`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalogService.java`

## Dependents As Tests

- `proc/community/src/integrationTest/.../WccMutateProcTest.java` (shared mutate semantics)
- `algo/.../ModularityOptimizationWithoutOrientationTest.java` (adjacent algorithm semantics)

## Open Questions

- Should modularity/tolerance failure states be surfaced as structured typed outcomes to simplify cross-language verification harnesses?

## Coding Prompt Unlocked

Rebuild this oracle as a Rust verification suite for mutate algorithms that validates seeded determinism, convergence metadata, estimation bounds, and write-back graph-label consistency.

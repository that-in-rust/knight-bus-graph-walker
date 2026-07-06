# 1001 verification_oracle WccMutateProcTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/community/src/integrationTest/java/org/neo4j/gds/wcc/WccMutateProcTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 642 |
| fan_in / fan_out | 0 / 57 |

## Why This File Matters

This is a concrete verification oracle for WCC mutate + write behavior. It locks in mutate mode contracts for component extraction, mutation token checks, filtered graph handling, and result metadata that the rewrite must preserve for procedure compatibility.

## Public Contract

- **Setup/teardown contracts:** `setupGraph()` creates graph fixtures and `removeAllLoadedGraphs()` clears state after each test (`setupGraph` at line 164, `removeAllLoadedGraphs` at line 173).
- **Mutate write with seed path:** `testMutateAndWriteWithSeeding` (`178`) drives full `gds.wcc.mutate` flow with seeding and validates output columns (`didConverge`, timings, counts).
- **Mutate result invariants:** `testMutateYields` (`217`) and `zeroCommunitiesInEmptyGraph` (`297`) verify component accounting semantics in normal and empty cases.
- **Filtered graph semantics:** `testWriteBackGraphMutationOnFilteredGraph` (`322`) and `testGraphMutationOnFilteredGraph` (`390`) define behavior when filtering is involved.
- **Failure surfaces:** `testMutateFailsOnExistingToken` (`428`) and `testMutateTwiceWithComputedSeed` (`523`) define error and idempotency boundaries.

## Internal Mechanics

- Uses shared procedure and catalog helpers from `GdsCypher`, `GraphProjectProc`, and `GraphWriteNodePropertiesProc`.
- Builds fixtures via `StoreLoaderBuilder`, `TestNativeGraphLoader`, and `GraphStoreCatalog`.
- Reads procedure results as rows and validates both graph mutation shape and metrics (`componentCount`, `nodePropertiesWritten`, timing fields).
- Explicitly checks graph property visibility (`graphStore.getUnion()`, mutated node property keys on labels A/B).

## Memory And Storage Implications

- Reads and validates timing fields (`preProcessingMillis`, `computeMillis`, `postProcessingMillis`, `mutateMillis`) as part of expected result payload.
- Confirms node property write behavior for WCC mutation via `nodePropertiesWritten`.
- Exposes duplicate-seeding behavior constraints (`testMutateTwiceWithComputedSeed`) relevant to idempotent RAM safety in repeated mutate flows.
- Covers empty-token constraints that can influence memory/CPU work skipped by early failure.

## Snapshot And Catalog Implications

- Verifies lifecycle from projected graph to mutated graph projection with local fixture graphs.
- Confirms graph mutation is executed only for expected labels and not leaked to non-target labels in filtered scenarios.
- Enforces graph store cleanup on each test (`removeAllLoadedGraphs`) which signals rewrite-safe test isolation expectations.

## Verification Oracles

1. **WHEN** `testMutateAndWriteWithSeeding` runs, **THEN** it SHALL return a non-empty `gds.wcc.mutate` result and assert consistent `componentCount`, `didConverge`, and timing fields from the row schema.
2. **WHEN** `zeroCommunitiesInEmptyGraph` runs, **THEN** it SHALL handle zero-component input gracefully without failing the mutate pipeline.
3. **WHEN** `testMutateFailsOnExistingToken` runs, **THEN** it SHALL throw `IllegalArgumentException` for existing mutation token conflicts.
4. **WHEN** `testMutateTwiceWithComputedSeed` runs, **THEN** it SHALL keep deterministic component counts under repeated seeded mutation without corrupting graph mutation outputs.

## Rust Rewrite Notes

- Keep WCC mutate executor as a strict two-path contract: estimation/compute plus optional write-back.
- Preserve filtered-graph execution and error behavior around existing token/name collisions.
- Model timing/result telemetry as typed fields so downstream rewrite validation can compare payload shape.

## Dependencies Read Next

- `applications/algorithms/machinery/DefaultAlgorithmProcessingTemplate`
- `applications/algorithms/machinery/MemoryGuard`
- `applications/algorithms/machinery/ProgressTrackerCreator`
- `applications/algorithms/machinery/RequestScopedDependencies`
- `applications/algorithms/machinery/WriteContext`

## Dependents As Tests

- `proc/community/src/integrationTest/.../ModularityOptimizationMutateProcTest.java` (same mutation/test harness pattern)
- `proc/community/src/integrationTest/.../LabelPropagationMutateProcTest.java` (mutate/write contract pattern)

## Open Questions

- Should WCC mutate conflict behavior be represented as typed error variants in Rust instead of exception-matching strings?
- Can timing fields be preserved as optional for release profile parity while still retaining observability?

## Coding Prompt Unlocked

Implement WCC mutate verification artifact parity by re-creating mutate result telemetry + filtered graph mutation checks first, then layer in seeded rerun/token-collision failure behavior as explicit typed outcomes.

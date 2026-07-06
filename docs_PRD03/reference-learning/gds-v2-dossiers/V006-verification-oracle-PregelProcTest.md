# 1006 verification_oracle PregelProcTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/pregel/src/test/java/org/neo4j/gds/pregel/proc/PregelProcTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 497 |
| fan_in / fan_out | 0 / 46 |

## Why This File Matters

This file is a compact but strict oracle for Pregel procedure modes (`stream`, `write`, `mutate`) plus task cleanup and failure behavior under thrown compute errors.

## Public Contract

- Procedure execution modes:
  - `stream` (`93`)
  - `streamWithPartitioning` parameterized across `Partitioning` (`111`)
  - `streamWithInvalidPartitioning` (`133`)
  - `write` (`149`)
  - `mutate` (`182`)
- Failure safety cleanup contracts:
  - `cleanupTaskRegistryWhenTheAlgorithmFailsInStreamMode` (`204`)
  - `cleanupTaskRegistryWhenTheAlgorithmFailsInWriteMode` (`233`)
  - `cleanupTaskRegistryWhenTheAlgorithmFailsInMutateMode` (`260`)
- Internal nested specs provide concrete factories/consumers for each mode (`WriteSpecification`, `MutateSpecification`, `StreamSpecification`).

## Internal Mechanics

- Defines procedure-level test doubles (`MutateProc`, `WriteProc`, `StreamProc`) and `TestPregelComputation`.
- Asserts that task registry cleanup is complete when algorithm throws inside compute path.
- Verifies partitioning argument handling, including invalid partitioning rejection.

## Memory And Storage Implications

- Exercises algorithm creation paths with explicit `MemoryEstimation` and memory estimate definitions through test factories.
- Confirms that failing execution paths do not leak task registry state, preventing long-running task buildup in compute-heavy runs.

## Snapshot And Catalog Implications

- Confirms catalog-level correctness by validating procedure mode dispatch across stream/write/mutate for the same config family.
- Uses explicit procedure stubs as a model for mode-specific registration boundaries in rewrite.

## Verification Oracles

1. **WHEN** `stream` executes with default config, **THEN** expected result shape and completion semantics SHALL hold.
2. **WHEN** invalid partitioning is used in stream mode, **THEN** compute SHALL fail validation before deep execution.
3. **WHEN** any mode throws during compute, **THEN** task registry SHALL be fully cleaned in the matching cleanup test.
4. **WHEN** `mutate` mode is invoked, **THEN** mutate procedure path SHALL be isolated from write/stream paths while preserving configuration semantics.

## Rust Rewrite Notes

- Keep mode dispatch as explicit enum or trait boundary (`StreamProc`, `WriteProc`, `MutateProc`) rather than dynamic reflection.
- Bake task-registry cleanup checks into regression tests because this is easy to miss in rewrite.
- Keep partitioning validation deterministic and visible in public API.

## Dependencies Read Next

- `algo-common/GraphAlgorithmFactory`
- `executor/AlgorithmSpec`
- `executor/ComputationResultConsumer`
- `core.write.NodePropertyExporterBuilder`
- `executor/ProcedureExecutor`

## Dependents As Tests

- `pregel/src/test/java/org/neo4j/gds/beta/pregel/PregelTest.java`
- `procedures/algorithms/*` (Pregel compute mode compatibility)

## Open Questions

- Should registry cleanup be modeled as explicit RAII guard in Rust to guarantee failure cleanup by construction?

## Coding Prompt Unlocked

Implement Pregel procedure mode tests in Rust that explicitly cover stream/write/mutate dispatch plus guaranteed cleanup on any compute-path exception.

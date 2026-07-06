# 1003 verification_oracle LabelPropagationMutateProcTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/community/src/integrationTest/java/org/neo4j/gds/labelpropagation/LabelPropagationMutateProcTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 558 |
| fan_in / fan_out | 0 / 55 |

## Why This File Matters

This is a direct compatibility oracle for label propagation mutate/write behavior and is especially valuable because it includes nested filtered-graph coverage and explicit deterministic result assertions.

## Public Contract

- `setup`/`tearDown` lifecycle at lines `131` and `151`.
- Core mutating flows:
  - `testMutateAndWriteWithSeeding` (`172`)
  - `testMutateYields` (`212`)
  - `testGraphMutation` (`343`)
  - `testMutateTwice...` equivalent behavior is validated through repeated execution expectations.
- Empty and failure semantics:
  - `zeroCommunitiesInEmptyGraph` (`268`)
  - `testMutateFailsOnExistingToken` (`400`)
- Nested filtered suite:
  - `FilteredGraph.testGraphMutationFiltered` (`466`) validates filtered-mode behavior.

## Internal Mechanics

- Reads/writes graph projection fixtures and captures both count/metadata and actual mutation outputs.
- Validates row-level numeric timing + algorithm-specific payload fields (`preProcessingMillis`, `computeMillis`, `postProcessingMillis`, `mutateMillis`, `communityCount`, `didConverge`).
- Verifies community assignment consistency (`mutatedGraph.nodeProperties("communityId")` checks near `489`).

## Memory And Storage Implications

- Mutation writes and seeding tests confirm property write side-effects and target labels.
- Empty-graph and filtered-path checks represent RAM/compute guardrails for invalid or narrow graph spaces.

## Snapshot And Catalog Implications

- Validates graph mutation property materialization boundaries and filtered execution behavior under separate nested test contexts.
- Asserts explicit node-property outcomes for source vs target labels post-mutation.

## Verification Oracles

1. **WHEN** `testMutateYields` executes on standard fixture graph, **THEN** component/community output SHALL include positive convergence and timing fields with deterministic counts.
2. **WHEN** `FilteredGraph.testGraphMutationFiltered` executes, **THEN** only filtered graph scope shall be mutated, with property keys constrained to expected labels.
3. **WHEN** `zeroCommunitiesInEmptyGraph` executes, **THEN** algorithm SHALL complete without component mutation errors.
4. **WHEN** token conflict occurs (`testMutateFailsOnExistingToken`), **THEN** execution SHALL surface validation error with message pattern consistency.

## Rust Rewrite Notes

- Keep filtered-mutation tests as a first-class mode in the rewrite test harness, not a secondary optimization.
- Treat seeding and empty-graph behavior as mode-level invariants for mutate execution.
- Preserve nested-test behavior as explicit sub-suite to ensure filtered/multi-graph fixtures remain deterministic.

## Dependencies Read Next

- `applications/algorithms/machinery/DefaultAlgorithmProcessingTemplate`
- `applications/algorithms/machinery/MemoryGuard`
- `applications/algorithms/machinery/ProgressTrackerCreator`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
- `proc/community/src/...` (mutate procedure suite peers)

## Dependents As Tests

- `proc/community/src/integrationTest/.../ModularityOptimizationMutateProcTest.java`
- `proc/community/src/integrationTest/.../WccMutateProcTest.java`

## Open Questions

- Should filtered nested behavior be preserved as a separate API mode in Rust (e.g., separate execution entrypoint) or via fixture-level query scoping?

## Coding Prompt Unlocked

Implement label-propagation mutate parity focusing on (1) seeded mutate/write paths, (2) filtered graph mutability, and (3) deterministic node-property assertions over `communityId`.

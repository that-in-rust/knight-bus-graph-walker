# 1009 verification_oracle PregelTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pregel/src/test/java/org/neo4j/gds/beta/pregel/PregelTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 933 |
| fan_in / fan_out | 0 / 39 |

## Why This File Matters

This is an extensive beta Pregel behavior oracle covering message passing semantics, termination flags, partitioning modes, progress logging, concurrency rules, and memory estimation.

## Public Contract

- Message-path correctness:
  - `sendsMessages` parameterized across partitioning (`102`)
  - `sendMessageToSpecificTarget` (`294`)
  - `compositeNodeValueTest` (`317`)
  - `messagesInInitialSuperStepShouldBeEmpty` (`674`)
- Progress and lifecycle:
  - `stopsEarlyWhenTransactionHasBeenTerminated` (`123`)
  - `logProgress` (`142`)
  - `cleanupProgressLogging` (`200`)
- Master-compute:
  - `testMasterComputeStep` (`363`)
  - `testMasterComputeStepWithConvergence` (`378`)
- Error bounds and validation:
  - `memoryEstimation` (`584`)
  - `preventIllegalConcurrencyConfiguration` (`652`)
  - `throwIfBidirectionalWithoutInverseIndex` (`903`)

## Internal Mechanics

- Defines custom `PregelComputation` implementations (`TestPregelComputation`, `TestReduciblePregelComputation`, `TestWeightComputation`, `TestSendTo`, etc.) to exercise algorithm mechanics.
- Verifies node value updates, termination behavior (`didConverge`, `ranIterations`), and ID mapping correctness.
- Asserts progress and task-tracking side effects across both synchronous/asynchronous behavior.

## Memory And Storage Implications

- Direct memory estimation coverage via `memoryEstimation` and explicit `MemoryRange` assertions.
- Concurrency validation guards against unsupported/unsafe partitioning/coexistence settings.
- Large graph correctness tests (`testCorrectnessForLargeGraph`) indicate memory and scale expectations for realistic workloads.

## Snapshot And Catalog Implications

- This test exercises core beta Pregel primitives that are expected to map to procedure-layer execution in rewrite.
- Validates that execution invariants are preserved for mapping from partitioning mode to algorithm behavior.

## Verification Oracles

1. **WHEN** `sendsMessages` runs under each partitioning, **THEN** expected messages/values SHALL match fixed expectations.
2. **WHEN** transaction termination is flagged, **THEN** execution SHALL stop early and return expected termination-safe behavior.
3. **WHEN** concurrency is illegal, **THEN** executor SHALL throw/guard with validation error in `preventIllegalConcurrencyConfiguration`.
4. **WHEN** bidirectional mode lacks inverse index, **THEN** execution SHALL fail with expected rejection semantics.

## Rust Rewrite Notes

- Preserve computation-family abstractions (init/send/compute/master-compute) as trait-style state machines.
- Keep progress logging and task registry semantics visible in tests since this suite heavily exercises lifecycle behavior.
- Add typed safeguards for partitioning+concurrency compatibility before execution.

## Dependencies Read Next

- `annotations/Configuration`
- `beta/pregel` computation interfaces and config types
- `core/concurrency/DefaultPool`
- `termination/TerminationFlag`
- `collections/ha/HugeDoubleArray`

## Dependents As Tests

- `proc/pregel/src/test/java/org/neo4j/gds/pregel/proc/PregelProcTest.java`
- broader beta pregel integration suites that depend on partitioning semantics

## Open Questions

- Which portion of beta pregel behavior should remain stable in the first Rust rewrite tranche, and which can be intentionally simplified?

## Coding Prompt Unlocked

Port Pregel core behavior tests by first implementing message-passing + termination paths, then master-compute and concurrency validation, then memory-estimation guards.

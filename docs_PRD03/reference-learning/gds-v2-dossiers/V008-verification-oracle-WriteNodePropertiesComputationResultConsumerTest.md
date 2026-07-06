# 1008 verification_oracle WriteNodePropertiesComputationResultConsumerTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/common/src/test/java/org/neo4j/gds/WriteNodePropertiesComputationResultConsumerTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 232 |
| fan_in / fan_out | 0 / 42 |

## Why This File Matters

This is a focused validation of write-mode compatibility for node property consumers, especially where `PropertyState` and `WriteMode` mismatch should fail early with informative errors.

## Public Contract

- `shouldThrowWhenWriteModeDoesNotMatchPropertyState` (`113`): validates strict compatibility checks.
- `shouldThrowWhenWriteModeIsNone` (`122`): validates forbidden write-mode path.
- `executeWrite(...)` helper methods enforce fixture graph + capability setup and call into `NativeNodePropertiesExporterBuilder` style consumer paths.

## Internal Mechanics

- Uses lightweight test graph builder (`HugeGraphBuilder`) and `GraphCharacteristics` to simulate realistic node-property write state.
- Focuses on compute-result consumption path and how result consumers reject invalid write mode/property state combinations.
- Uses `GraphStoreAdapter` test subclass to isolate capability behavior.

## Memory And Storage Implications

- Write-path validation here prevents write attempts into unsupported `PropertyState` and avoids unnecessary graph mutation.
- Emphasizes correctness on capability checks before large-scale exporter setup.

## Snapshot And Catalog Implications

- Confirms consumer contracts with `GraphStore` capabilities and catalog-managed graph properties.
- Reinforces that write consumers should fail closed when write mode or property state assumptions are invalid.

## Verification Oracles

1. **WHEN** property state and write mode mismatch, **THEN** write consumer SHALL throw (via `assertThatThrownBy`).
2. **WHEN** write mode is `NONE`, **THEN** write invocation SHALL be rejected.
3. **WHEN** mock graph state is correctly configured, **THEN** write consumer SHALL only proceed under valid capability combinations.

## Rust Rewrite Notes

- Implement this as a small contract test in the Rust write-consumer layer to enforce write-mode and property-state invariants.
- Treat invalid combinations as explicit error returns (typed, not generic panic path).
- Keep write-capability checks close to result-consumption layer, before mutation side-effects.

## Dependencies Read Next

- `core/src/main/java/org/neo4j/gds/core/write/NodePropertyExporterBuilder`
- `core/src/main/java/org/neo4j/gds/core/write/NativeNodePropertiesExporterBuilder`
- `core/src/main/java/org/neo4j/gds/core/loading/Capabilities`
- `executor/ComputationResult`

## Dependents As Tests

- `proc/pregel/src/test/java/org/neo4j/gds/pregel/proc/PregelProcTest.java` (mutation/write result flow)
- broader write-path suites that consume computation results with `WriteContext`

## Open Questions

- Should this rejection be represented as typed enum (`InvalidWriteMode`, `InvalidPropertyState`) in rewritten API for better diagnostics?

## Coding Prompt Unlocked

Add a Rust write-consumer verification layer that enforces property-state/write-mode contracts before attempting write execution, mirroring both thrown-path and guard-path tests.

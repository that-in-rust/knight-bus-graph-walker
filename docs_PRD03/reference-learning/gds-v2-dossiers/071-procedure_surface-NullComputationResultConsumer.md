# 071 procedure_surface NullComputationResultConsumer

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/common/src/main/java/org/neo4j/gds/NullComputationResultConsumer.java |
| lane | procedure_surface |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 71 |
| line_count | 38 |
| fan_in / fan_out | 41 / 5 |

## Why This File Matters

This is the canonical no-output consumer used where a computation result stream is intentionally suppressed.

## Public Contract

- Class is generic over algorithm type, configuration, and return type (`29-30`).
- Implements `ComputationResultConsumer<..., Stream<RESULT>>` and returns `Stream.empty()` from `consume(...)` (`31-37`).
- This enforces a strict “compute-only” path with no result materialization.

## Internal Mechanics

- No branching or state is present; method body is a pure no-op stream emission (`32-37`).
- The type bounds preserve compile-time compatibility with existing computation pipeline signatures.

## Memory and Storage Implications

- Minimal allocator footprint by design; no temporary output buffering in the consumer.
- Good for heavy compute modes that already emit side effects and do not need stream output.

## Snapshot And Catalog Implications

- Keeps procedure-level API consistent while avoiding protocol drift where some modes return empty streams.

## Verification Oracles

1. **WHEN** used in a compute pipeline, **THEN** consume SHALL always return an empty stream.
2. **WHEN** `executionContext` is provided, **THEN** no exception path is introduced by the consumer.
3. **WHEN** all outputs are suppressed, **THEN** no downstream stream assumptions should fail (finite empty stream).

## Rust Rewrite Notes

- Represent as a generic sink struct implementing the same compute-consumer trait.
- Keep it generic and side-effect-free.

## Dependencies Read Next

- `ComputationResultConsumer`
- `ComputationResult`
- `ExecutionContext`

## Dependents As Tests

- Pipeline tests asserting empty-stream output for configured no-output modes.
- Type-compatibility tests against compute pipeline generic parameters.

## Open Questions

- Should the Rust equivalent expose both stream and non-stream variants for ergonomics?

# 17 olap_algorithm NodePropertiesWritten

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/NodePropertiesWritten.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 17 |
| line_count | 25 |
| fan_in / fan_out | 129 / 0 |
| purpose | Microtype record for node-property write-count metadata. |
| read_prompt | Read this entire file as a microtype contract for mutation accounting metadata; identify expected representation guarantees. |

## Why This File Matters

- Compact return type used as metadata for mutation outcomes; microtypes improve semantic clarity in algorithms/machinery contracts.
- Being immutable record-like value avoids accidental mutation bugs in Rust rewrites if mapped to tuple-like struct.

## Public Contract

- `NodePropertiesWritten` is:
  - a public record
  - single field `long value`
- The type expresses how many node properties were written by an algorithm flow.
- No explicit methods beyond record auto-accessors.

## Internal Mechanics

- No business logic in this file.
- This is intended as a value-level domain wrapper, not a behavior container.

## Memory And Storage Implications

- Allocation footprint is minimal (single primitive field record).
- Value semantics support low-overhead pass-through in APIs that need explicit "count" typing.

## Snapshot And Catalog Invariants

- No direct catalog behavior.
- Useful when mapping operation results into typed result summaries.

## Verification Oracles

1. **WHEN** instantiated with `3`
   **THEN** **SHALL** expose `value() == 3`.
2. **WHEN** compared against another `NodePropertiesWritten(3)`
   **THEN** **SHALL** behave as value equality (record semantics).
3. **WHEN** serialized/deserialized as a result DTO
   **THEN** **SHALL** preserve the single long payload exactly.

## Rust Rewrite Notes

- **L1:** define a newtype struct `NodePropertiesWritten(pub u64)` or `i64` depending on signedness policy.
- **L2:** keep serialization support for this single-field payload.
- **L3:** prefer strong-typed API signatures over bare integers for write-result accounting.

## Dependencies Read Next

- Algorithm/metadata modules that return or consume mutation-write counts.
- Procedure result adapters that expose node-property write metrics.

## Dependents As Tests

- Mutation procedure tests that verify reported `nodes/props written` counters.

## Open Questions

- Signed vs unsigned count type in Rust (`i64` to mirror Java `long`).
- Whether this microtype should be combined with additional metadata for future algorithms.

## Coding Prompt Unlocked

Introduce a Rust newtype for mutation write counts and replace bare count primitives in algorithm metadata/result structs where only property-write count is currently represented.

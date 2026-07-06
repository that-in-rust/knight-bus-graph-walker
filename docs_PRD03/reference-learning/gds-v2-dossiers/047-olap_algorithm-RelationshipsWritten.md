# 47 olap_algorithm RelationshipsWritten

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/RelationshipsWritten.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 47 |
| line_count | 25 |
| fan_in / fan_out | 59 / 0 |

## Why This File Matters

This is a microtype value object used to carry the count of written relationships across mutate pipelines and API responses.

## Public Contract

- **Evidence:** Declares `public record RelationshipsWritten(long value){}` (`25`), making this a transparent, immutable value wrapper.
- **Inference:** Its contract is serialization- and type-safe transport of a single numeric metric.

## Internal Mechanics

- **Evidence:** As a Java `record`, it gets canonical constructor, accessor, `equals`, and `toString` for free (`25`).
- **Inference:** No custom behavior means this is intentionally a pure domain metric type, not a mutable counter.

## Memory And Storage Implications

- **Evidence:** Zero fields beyond one `long` plus object header, so low overhead per instance (`25`).
- **Inference:** In hot paths with repeated emits, this may still be allocative if boxed frequently; Rust can keep it as value copy.

## Snapshot And Catalog Implications

- **Inference:** Used in mutate result metadata contracts; consistency matters for output fields and API compatibility.
- **Blocked:** No direct catalog interactions in this file.

## Verification Oracles

1. **WHEN** constructing `new RelationshipsWritten(x)`, **THEN** `value()` **SHALL** return `x`.
2. **WHEN** values are compared with same `value`, **THEN** record equality semantics **SHALL** return true.
3. **WHEN** serialized in downstream DTOs, **THEN** relationship-write accounting **SHALL** remain exact and not coerced.

## Rust Rewrite Notes

- **L1:** Newtype wrapper `RelationshipsWritten(u64)` (or `i64`) for explicit metric intent.
- **L2:** Derive copy/equality/hash traits for cheap movement.
- **L3:** Keep as value object in mutate metadata pipelines rather than mutable counters.

## Dependencies Read Next

- `applications/algorithms/machinery/MutateNodeProperty.java`
- Community mutate/result builder paths that return this type.
- Any result contract serializers expecting relationship-write metadata.

## Dependents As Tests

- Mutate pipeline tests that assert write-metadata correctness.
- DTO serialization tests for mutate response fields.

## Open Questions

- Should Rust use signed (`i64`) or unsigned (`u64`) for this metric to align with existing Java `long` behavior?

## Coding Prompt Unlocked

Replace relationship-write metric as a typed Rust value object (`RelationshipsWritten`) with:
1) non-optional numeric access,
2) cheap copy/compare semantics,
3) explicit tests validating pass-through into mutate metadata contracts.

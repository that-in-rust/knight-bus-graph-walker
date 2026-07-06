# 57 projection_build PropertyMappings

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/PropertyMappings.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 57 |
| line_count | 197 |
| fan_in / fan_out | 46 / 4 |

## Why This File Matters

This file defines how projection property mappings are parsed, merged, validated, and serialized. It is the normalization boundary between user-configured projection input and runtime map representation.

## Public Contract

- **Evidence:** `PropertyMappings` is immutable (`@Value.Immutable`) and enumerable via `mappings()` and `Iterator` (`41-46`, `104-111`).
- **Evidence:** Parsing supports both `String`, `List`, and `Map` inputs, while only unsupported classes fail fast with `IllegalArgumentException` and input class name (`54-93`).
- **Evidence:** Duplicate property keys are rejected:
  - during merge of list entries in builder (`71-77`),
  - and during `toObject` map collection (`128-133`).
- **Evidence:** Aggregate mixing rule is validated by `checkForAggregationMixing()`: mixing `Aggregation.NONE` with other strategies is rejected (`148-157`).
- **Inference:** The class guarantees semantic normalization before projection work proceeds.

## Internal Mechanics

- **Evidence:** `of(...)` returns empty immutable mappings if input is null (`47-52`).
- **Evidence:** List input iterates nested mappings and flattens by recursively calling `fromObject(...)` (`66-80`).
- **Evidence:** `withDefaultAggregation` is captured in builder and post-processed in `build()` to apply non-default aggregation across all mappings (`160-170`, `186-195`).
- **Evidence:** `mergeWith(...)` prefers existing mapping when one side is empty and deduplicates by `distinct()` when merged (`136-146`).
- **Inference:** This class is both parser and validator; downstream code should receive canonicalized mappings.

## Memory And Storage Implications

- **Evidence:** Parsing from string/list/map creates intermediate `Builder` and potentially `ArrayList`-like mapping collections (`47-80`, `81-88`, `136-146`).
- **Inference:** For large mapping payloads, memory is mostly dominated by materialized `List<PropertyMapping>` and temporary merge buffers; this is unavoidable for stable validation.
- **Inference:** The explicit duplicate checks are cheaper fail-fast protections versus letting ambiguous mappings flow into projection pipeline.

## Snapshot And Catalog Implications

- **Evidence:** No direct catalog mutation; it only normalizes user/procedure input into in-memory typed mappings (`41-52` and entire class).
- **Inference:** Rewrite should preserve strict `Unsupported input -> explicit exception` behavior to avoid silent projection drift.
- **Blocked:** Exact allocation profile depends on `PropertyMapping` internals and downstream projection structures.

## Verification Oracles

1. **WHEN** input is `null`, **THEN** `of(...)` **SHALL** return `ImmutablePropertyMappings.of()`.
2. **WHEN** input is `String`, **THEN** parser **SHALL** treat key and mapping as same property name (`63-66`).
3. **WHEN** duplicate mappings are seen, **THEN** either list build (`71-77`) or `toObject` materialization (`128-133`) **SHALL** throw `IllegalStateException`/`IllegalArgumentException`.
4. **WHEN** mappings contain mixed `Aggregation.NONE` plus other strategies, **THEN** `checkForAggregationMixing` **SHALL** throw `IllegalArgumentException`.
5. **WHEN** builder receives non-default default aggregation, **THEN** `build()` **SHALL** apply it consistently to each mapping (`178-195`).

## Rust Rewrite Notes

- **L1:** Model `PropertyMappings` as an immutable value type with validated construction.
- **L2:** Implement parser functions for `String`, `List`, and `Map` inputs with explicit typed failure for unsupported input shapes.
- **L2:** Encode aggregation invariants (`NONE` exclusivity) at construction/build time.
- **L3:** Return canonical ordered mappings and deterministic serialization via `toObject`-style converter for API compatibility.

## Dependencies Read Next

- `graph-projection-api/src/main/java/org/neo4j/gds/PropertyMapping.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/ElementProjection.java`
- all projection tests using `PropertyMappings.fromObject(...)`

## Dependents As Tests

- `PropertyMappingsTest` should assert null, string, list-flatten, map, unsupported input, duplicate key, and mixed aggregation behavior.
- `NodeProjectionsTest`/projection integration tests to ensure mapping normalization affects topology projection consistently.
- Contract tests that `mergeWith` is idempotent when one side is empty and deduplicates when both non-empty.

## Open Questions

- Should default aggregation be made explicit at `PropertyMapping` level in Rust to avoid hidden mutation semantics in builder internals?
- Should duplicate detection preserve first-wins or reject always? Current behavior rejects duplicates in at least two paths.
- Should property mapping serialization preserve insertion order or allow canonical reordering?

## Coding Prompt Unlocked

Implement `PropertyMappings` in Rust as a validated immutable projection-input boundary:
1) parse string/list/map inputs with explicit errors for unsupported types,
2) enforce duplicate-key checks during parse and serialization,
3) enforce aggregation mixing invariants centrally,
4) add tests mirroring the exact failure and empty-input semantics.

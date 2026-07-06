# 41 projection_build PropertyMapping

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/PropertyMapping.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 41 |
| line_count | 229 |
| fan_in / fan_out | 60 / 6 |

## Why This File Matters

`PropertyMapping` is the core projection property descriptor. It encodes how a projected property key maps between Cypher input and graph schema and how default/aggregation semantics are represented.

## Public Contract

- **Evidence:** Abstract property contract: `propertyKey()` may be nullable and `neoPropertyKey()` defaults to `propertyKey()` (`45–53`).
- **Evidence:** Defaults for `defaultValue()` and `aggregation()` are explicit (`56–63`).
- **Evidence:** Validation constraints are enforced via `validateProperties()` and `validatePropertyKey()` (`66–77`), rejecting `"*"` with non-count aggregation and empty keys.
- **Evidence:** `fromObject(propertyKey, Object)` supports:
  1. single-string input (`79–89`),
  2. map input with case-insensitive normalization (`90–93`, `124–128`),
  3. explicit validation and conversion of aggregation key (`102–113`),
  4. explicit error on unsupported shape (`123–128`).
- **Inference:** The parser accepts exactly two input forms (string/map), and all other shapes are treated as programmer/config errors.
- **Evidence:** Mutation helpers (`setNonDefaultAggregation`, static `of(...)` overloads) provide controlled constructors and avoid accidental state mutation (`150–156`, `157–229`).

## Internal Mechanics

- **Evidence:** `@ValueClass` plus generated immutable implementations implies builder-based construction and copy-on-write semantics (`36`, `157+`).
- **Evidence:** `validateProperties()` checks `neoPropertyKey()` against special wildcard and aggregation constraints (`67–70`).
- **Inference:** The default `defaultValue` is likely serialized as a stable `DefaultValue` wrapper, which keeps schema-level defaults uniform across projection builders.
- **Blocked:** Actual runtime cost of `DefaultValue.of(...)` for large nested values cannot be inferred from this file alone.

## Memory And Storage Implications

- **Evidence:** `fromObject(..., Object)` creates new `TreeMap` (case-insensitive) for map inputs (`90–93`) and constructs immutable mapped value objects (`117–122`, `158+`).
- **Inference:** For bulk projection parsing, object-to-map normalization can cause temporary allocation spikes, so Rust should batch or stream validation/normalization where possible.
- **Evidence:** Value conversion never mutates source object and always creates a new `PropertyMapping` value object (`82–89`, `117–122`).

## Snapshot And Catalog Implications

- **Inference:** No direct catalog writes in this file; it is a transport/value layer between parser and projection builders.
- **Evidence:** It contributes deterministic shape to graph projection schema by storing `propertyKey` and `neoPropertyKey`.

## Verification Oracles

1. **WHEN** `neoPropertyKey == "*" && aggregation != COUNT` **THEN** `validateProperties()` **SHALL** throw `IllegalArgumentException` (`67–70`).
2. **WHEN** `propertyKey` is empty **THEN** `validatePropertyKey` **SHALL** throw `IllegalArgumentException` (`74–77`).
3. **WHEN** `fromObject` receives `String`, **THEN** it **SHALL** return object with default value and mapping equal to key (`79–89`).
4. **WHEN** `fromObject` receives non-string/non-map input, **THEN** it **SHALL** throw `IllegalStateException` with the type name (`123–128`).

## Rust Rewrite Notes

- **L1:** Value object/DTO `PropertyMapping` with constructor defaults and validation.
- **L2:** Strict parser from either string or map (`from_object`) returning typed parse result or error enum.
- **L2:** `set_non_default_aggregation` as pure functional transform returning cloned mapping.
- **L3:** `DefaultValue` integration point with projection serializer/deserializer boundaries.

## Dependencies Read Next

- `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipProjection.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/ElementProjection.java`
- `core/src/main/java/org/neo4j/gds/collections` helpers (`DefaultValue`, `Aggregation`)
- `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java`

## Dependents As Tests

- Projection-builder tests that parse property mapping objects from procedure/config payloads.
- Fuzz/property-key parser tests for wildcard and aggregation combos.

## Open Questions

- Should Rust parser preserve case-insensitivity exactly as Java `TreeMap(String.CASE_INSENSITIVE_ORDER)`?
- Can empty map or missing default/aggregation fields be represented in one unified "unset" enum for clearer RAM accounting?

## Coding Prompt Unlocked

Create `PropertyMapping` in Rust as:
1) immutable struct/record with defaults,
2) two input forms (`String` and map),
3) invariant validation (`*` + aggregation),
4) explicit parse errors for unsupported types,
5) tests for each constructor branch and failure mode.

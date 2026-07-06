# 43 projection_build RelationshipProjection

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipProjection.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 43 |
| line_count | 248 |
| fan_in / fan_out | 56 / 8 |

## Why This File Matters

This file defines the relationship projection contract used by graph projection APIs: shape, parsing, validation, serialization keys, and immutable merge/composition behavior.

## Public Contract

- **Evidence:** `type()`, `orientation()`, `aggregation()`, `indexInverse()` accessors and static sentinels (`ALL`, `ALL_UNDIRECTED`) are first-class projection state (`39–41`, `42–57`).
- **Evidence:** Builder factories and static convenience constructors (`fromMap`, `fromString`, `fromObject`, multiple `of(...)`) centralize construction (`106–130`, `151–167`).
- **Evidence:** `fromMap` applies config-key validation (`213–221`) and key coercion for orientation/aggregation/indexInverse (`113–123`).
- **Evidence:** `check()` enforces `orientation != UNDIRECTED` when `indexInverse=true` (`67–73`).
- **Evidence:** `checkAggregation()` enforces non-empty property mappings when global MIN/SUM/MAX/COUNT are requested (`81–93`).
- **Evidence:** `projectAll()` identifies wildcard relationship sentinel by exact comparison with `PROJECT_ALL` (`97–99`).
- **Inference:** The constructor path intentionally prevents silently invalid projections before algorithm execution.

## Internal Mechanics

- **Evidence:** `fromObject` dispatches on `null`, `String`, or `Map`, returning `ALL` for null (`132–135`) and throwing on unsupported input (`145–149`).
- **Evidence:** `withAdditionalPropertyMappings(...)` merges new mappings using default aggregation propagation and returns same value if no change (`184–197`).
- **Evidence:** `includeAggregation()` is always true in this subclass (`171–174`), and write behavior is explicit via `writeToObject(...)` (`176–183`).
- **Inference:** This is a value object with controlled validation at parse and merge boundaries.

## Memory And Storage Implications

- **Evidence:** Parsing creates temporary `TreeMap` for case-insensitive access and `PropertyMappings` conversion (`140–143`, `203–211`).
- **Inference:** Frequent map normalization can be allocation-heavy in high-frequency projection calls; Rust rewrite can optimize by caching/canonicalizing keys before parse.
- **Evidence:** Merge path copies-on-write with `ImmutableRelationshipProjection` builder (`194–197`), which can allocate replacement objects on change only.

## Snapshot And Catalog Implications

- **Inference:** This descriptor directly controls cataloged projection shape; any change to default values or key semantics can alter stored graph schema.
- **Evidence:** `TYPE_KEY`, `ORIENTATION_KEY`, `AGGREGATION_KEY`, `INDEX_INVERSE_KEY` keys define serialized projection fields (`101–105`).

## Verification Oracles

1. **WHEN** `orientation == UNDIRECTED` and `indexInverse == true`, **THEN** `check()` **SHALL** throw `IllegalArgumentException` (`67–73`).
2. **WHEN** global aggregation requires properties but `properties()` is empty, **THEN** `checkAggregation()` **SHALL** throw (`81–93`).
3. **WHEN** `fromObject(null)` is called, **THEN** result **SHALL** be `RelationshipProjection.ALL` (`132–135`).
4. **WHEN** map includes only recognized keys, **THEN** parse **SHALL** create a valid projection; unrecognized keys **SHALL** fail key validation (`106–113`, `213–221`).

## Rust Rewrite Notes

- **L1:** `RelationshipProjection` immutable struct/enumerated projection with parser/validator boundary.
- **L2:** `from_object` parser accepting null/string/map with case-insensitive key handling.
- **L2:** Dedicated validation functions for orientation/indexInvariant and aggregation/property cardinality rules.
- **L2:** Efficient copy-on-write merge path for property mappings.
- **L3:** Serialization key module (`to_object`, `write_to_object`) as projection persistence boundary.

## Dependencies Read Next

- `graph-projection-api/src/main/java/org/neo4j/gds/ElementProjection.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/PropertyMappings.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java`

## Dependents As Tests

- Projection parser tests for `relationshipProjection` fields.
- Serialization tests for map round-trip (`fromMap` → `writeToObject`).
- Invalid-key rejection tests.

## Open Questions

- Should Rust enforce config key allowlist at compile time (`enum`-style) or at runtime map parse boundary?
- Do we need to preserve case-insensitive map semantics globally or only for relationship projection input?

## Coding Prompt Unlocked

Build `RelationshipProjection` in Rust with:
1) strict constructor + parse pipeline,
2) key allowlist validation,
3) invariants (undirected/index inverse + property-aggregation),
4) merge-with-defaults behavior,
5) roundtrip projection serialization tests with explicit key set.

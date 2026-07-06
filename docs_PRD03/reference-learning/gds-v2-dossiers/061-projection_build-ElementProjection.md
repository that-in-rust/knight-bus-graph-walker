# 61 projection_build ElementProjection

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/ElementProjection.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 61 |
| line_count | 166 |
| fan_in / fan_out | 44 / 5 |

## Why This File Matters

`ElementProjection` is the shared abstraction for node/relationship projection elements. It captures defaults, parse wiring, property mapping parsing, and serialization contract reused by projection APIs.

## Public Contract

- **Evidence:** It is abstract with `projectAll()` and `withAdditionalPropertyMappings(...)` as core projection semantics (`36-50`, `47-50`).
- **Evidence:** `toObject()` always writes `properties` output by delegating to `properties().toObject(includeAggregation())` (`51-55`).
- **Evidence:** static `create(Map<String, Object>, Function<PropertyMappings, T>)` reads `PROPERTIES_KEY` and uses `PropertyMappings.fromObject(...)` for parsing (`58-65`).
- **Evidence:** `nonEmptyString` enforces that requested config keys must be non-empty strings and throws `IllegalArgumentException` otherwise (`67-76`).

## Internal Mechanics

- **Evidence:** Optional projection defaults (`@Value.Default @Value.Parameter`) expose `PropertyMappings.of()` when absent (`41-45`).
- **Evidence:** Builder-facing `InlineProperties` trait supports in-code property accumulation and validation through `propertiesBuilder()` and `build()` guardrails (`82-165`).
- **Evidence:** `build()` prevents simultaneous explicit `properties(...)` and `addProperty` usage unless `properties` are intentionally layered (`139-151`).
- **Inference:** The class is a reusable normalization point for both parse and serialization code paths.

## Memory And Storage Implications

- **Evidence:** `toObject` creates a linked insertion-ordered map for deterministic projection output (`51-56`).
- **Inference:** Allocation cost is bounded: one map plus mapped conversion from `PropertyMappings` serialization.
- **Inference:** Inline builder path can allocate temporary property-builder state when fluent property composition is used.

## Snapshot And Catalog Implications

- **Evidence:** No catalog writes are performed; this is pure projection metadata normalization (`36-66`, `78-80`).
- **Inference:** Any rewrite can keep this as immutable + builder-style value type and still preserve call semantics.

## Verification Oracles

1. **WHEN** `properties` is absent in input config, **THEN** projection SHALL default to `PropertyMappings.of()`.
2. **WHEN** config contains `properties`, **THEN** `create(...)` SHALL parse through `PropertyMappings.fromObject`.
3. **WHEN** an input config key expects string but receives empty/invalid input, **THEN** `nonEmptyString` SHALL throw `IllegalArgumentException`.
4. **WHEN** inline builder is used with both existing complete `properties` and additional adders, **THEN** `build()` SHALL throw `IllegalStateException`.
5. **WHEN** serializing, **THEN** returned object SHALL include a `properties` key whose values depend on `includeAggregation()`.

## Rust Rewrite Notes

- **L1:** Model `ElementProjection` as abstract projection trait with immutable defaults.
- **L2:** Keep parse contract (`properties` map parse + non-empty string helper) as shared validation utilities.
- **L3:** Preserve inline builder safety check: reject invalid mixed state between full `properties` assignment and incremental add operations.

## Dependencies Read Next

- `graph-projection-api/src/main/java/org/neo4j/gds/PropertyMappings.java`
- `graph-projection-api/src/main/java/org/neo4j/gds/PropertyMapping.java`
- projection DTOs that call `ElementProjection.create(...)` in node/relationship projection builders

## Dependents As Tests

- Add tests for:
  - default property mapping behavior,
  - parsing from map/list/string through delegated parser,
  - inline builder conflict detection,
  - serialization ordering and `includeAggregation` behavior.
- Contract tests for all projection entrypoints that rely on `create(...)` for config parsing.

## Open Questions

- Should `toObject` include empty `properties` when defaulted, or keep map minimal by omitting? Current code includes it through shared method.
- Should include-aggregation behavior remain abstract or become an explicit enum flag in Rust?

## Coding Prompt Unlocked

Implement `ElementProjection` in Rust as a core projection-semantics module:
1) abstract trait + concrete projection types with immutable defaults,
2) shared parser helper for `properties`,
3) inline builder with conflict detection, and
4) stable serialization contract with deterministic map ordering.

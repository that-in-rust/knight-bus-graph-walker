# 086 projection_build GraphProjectFromStoreConfig

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java |
| lane | projection_build |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 86 |
| line_count | 199 |
| fan_in / fan_out | 32 / 9 |

## Why This File Matters

This file defines projection config normalization for implicit graph loading from store and encodes strict property/projection compatibility checks.

## Public Contract

- Interface extending `GraphProjectConfig` with keys:
  - `nodeProjection`, `relationshipProjection`, `nodeProperties`, `relationshipProperties`
- Default property maps:
  - `nodeProperties()` and `relationshipProperties()` return empty `PropertyMappings`
- Static validation:
  - both node and relationship projections must be non-empty
- `withNormalizedPropertyMappings()`:
  - validates projection/property key disjointness
  - expands normalized node/relationship projections with property mappings
  - validates relationship aggregation after normalization
- Helper `verifyProperties` throws on property overlap between projection and property mappings.
- Factories:
  - `of(userName, graphName, nodeProjections, relationshipProjections, config)`
  - `all(userName, graphName)`
  - `fromProcedureConfig(username, config)`
- `asProcedureResultConfigurationField()` strips output fields from result exposure.

## Internal Mechanics

- Uses `NodeProjections#fromObject`, `RelationshipProjections#fromObject` and property mapping converters.
- Input defaults:
  - if node projections absent, add `NodeProjections.all()`
  - if relationship projections absent, add `RelationshipProjections.ALL`
- `withNormalizedPropertyMappings` may throw if conflicting configuration appears, before builder construction.

## Memory and Storage Implications

- This file drives topology shape prior to projection build.
- It enforces memory/compute safety by limiting property mapping combinations:
  - prevents loading multiple relationship properties for a single relationship type in implicit loading path.

## Snapshot And Catalog Implications

- This is a config normalization contract before projection execution.
- For Rust rewrite, preserve this as a deterministic preflight transformation stage before planner touches storage.

## Verification Oracles

1. **WHEN** node projections are empty, **THEN** validation SHALL reject with explicit guidance to use `*`.
2. **WHEN** relationship projections are empty, **THEN** validation SHALL reject with explicit guidance to use `*`.
3. **WHEN** property overlap exists, **THEN** an `IllegalArgumentException` SHALL include both overlapping keys.
4. **WHEN** multiple relationship properties are attached per relationship type, **THEN** normalize-check SHALL throw.

## Rust Rewrite Notes

- Model normalization as a pure transform returning a fresh validated config object.
- Keep converters (`*fromObject`, `*toObject`) explicit and composable.

## Dependencies Read Next

- `GraphProjectConfig`, `NodeProjections`, `RelationshipProjections`, `PropertyMappings`
- `RelationshipProjection#checkAggregation`
- Estimation/projection execution layers

## Dependents As Tests

- Unit tests for empty projection rejection.
- Normalize path tests with overlapping and non-overlapping property mappings.

## Open Questions

- Should the overlap error return structured structured error data to support localized client messaging in Rust?


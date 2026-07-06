# 080 olap_algorithm CommunityCompanion

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/main/java/org/neo4j/gds/algorithms/community/CommunityCompanion.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 80 |
| line_count | 213 |
| fan_in / fan_out | 29 / 14 |

## Why This File Matters

`CommunityCompanion` is a pure helper for community algorithm node-property preparation, especially around property normalization, filtering, and extraction.

## Public Contract

- Static normalization helpers:
  - `nodePropertyValues(...)` overloads for:
    - consecutive IDs conversion
    - incremental writes with seed property merge behavior
    - optional minimum community-size filtering
    - convenience composition of the above
- `createIntermediateCommunitiesNodePropertyValues(...)` builds anonymous `LongArrayNodePropertyValues`.
- `extractSeedingNodePropertyValues(...)` validates seeding property exists and is `ValueType.LONG`.
- Internal `CommunitySizeFilter` supports:
  - `nodeCount()`
  - `longValue(nodeId)`
  - `hasValue(nodeId)` with minimum size checks

## Internal Mechanics

- If `consecutiveIds` is true, long property values are wrapped by `ConsecutiveLongNodePropertyValues`.
- Incremental branch (`isIncremental && resultProperty.equals(seedProperty)`) injects `LongIfChangedNodePropertyValues`.
- Community-size filtering computes community counts via `CommunityStatistics.communitySizes(...)` and applies:
  - `Long.MIN_VALUE` skip sentinel semantics from wrapped property values
  - `hasValue` checks to keep filtering consistent with wrapped node property behavior
- `extractSeedingNodePropertyValues` checks for existence and type, and throws on non-long seeding fields.

## Memory and Storage Implications

- Most methods are views over existing property arrays; memory overhead is mostly wrapper object creation.
- `CommunitySizeFilter` stores `HugeSparseLongArray` counts and a wrapped source, so memory impact is proportional to community count and sparse cardinality.
- This file contributes to write correctness by marking “filtered out” values with `Long.MIN_VALUE` rather than allocating separate sparse masks.

## Snapshot And Catalog Implications

- No catalog writes here; pure data-shaping layer for algorithm outputs before write or stream.
- In Rust, keep filtering + seeding extraction semantics explicit to maintain exact graph mutation outputs.

## Verification Oracles

1. **WHEN** `consecutiveIds` is true, **THEN** output must be wrapped with consecutive-ID semantics.
2. **WHEN** `isIncremental` and `resultProperty == seedProperty`, **THEN** input supplier should be wrapped into `LongIfChangedNodePropertyValues`.
3. **WHEN** `extractSeedingNodePropertyValues` sees missing property, **THEN** return `null`.
4. **WHEN** min-size filter is set, **THEN** `nodeProperties.hasValue` and `longValue` MUST agree on skip semantics.

## Rust Rewrite Notes

- Implement as pure utility module with function-level transformations.
- Preserve sentinel-based omission semantics (`Long.MIN_VALUE`) for values below filter threshold.
- Keep min-size filter in shared helper reused by community/training write paths.

## Dependencies Read Next

- `CommunityStatistics.communitySizes`
- `ConsecutiveLongNodePropertyValues`
- `LongIfChangedNodePropertyValues`
- `FilteredNodePropertyValuesMarker`

## Dependents As Tests

- Unit tests for each `nodePropertyValues` overload combination.
- Filter contract test asserting sentinel value consistency between `longValue` and `hasValue`.
- Multi-label/min-size filter edge-case tests.

## Open Questions

- Could filtering be moved into a single reusable property adapter shared across algorithms beyond community?
- Should community-size threshold be validated early with graph stats to avoid late sentinel errors?


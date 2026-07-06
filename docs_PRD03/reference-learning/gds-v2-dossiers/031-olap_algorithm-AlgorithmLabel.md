# 31 olap_algorithm AlgorithmLabel

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmLabel.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 31 |
| line_count | 170 |
| fan_in / fan_out | 73 / 2 |

## Why This File Matters

This is the central algorithm-label taxonomy used by algorithm orchestration, progress task naming, and cross-file classification. It is small but extremely high-leverage because it normalizes algorithm identity for task display and execution routing.

## Public Contract

- Enum values represent algorithm families (e.g., `K1Coloring`, `Louvain`, `WCC`, `PageRank`) with display labels (`lines 25-85`).
- Implements `Label` with:
  - `String asString()` (`lines 158-161`)
  - `String toString()` (`lines 166-168`)
- Static `from(Algorithm algorithm)` maps metadata enum to this display enum (`lines 94-155`).
- Includes aliases/mappings that can look surprising (e.g., `SingleSourceDijkstra` maps to `"All Shortest Paths"` label at line 75).

## Internal Mechanics

- Immutable mapping:
  - no mutable fields except the stored human label string.
  - simple constructor stores display string (`line 89`).
- Mapping is declarative and exhaustive over `Algorithm` values with `switch` expression (`lines 94-155`), so compile-time coverage is centralized.

## Memory And Storage Implications

- Very low runtime overhead: this is an enum + string mapping.
- Memory impact mostly in code/static constants; no per-call allocations except temporary objects returned by mapping.
- Useful in rewrite for reducing duplicated string-literal bugs and centralizing telemetry labels.

## Snapshot And Catalog Implications

- This file is not a catalog store itself, but it indirectly constrains what can be surfaced as algorithm identifiers across progress and facade layers.
- `SingleSourceDijkstra` and `AllShortestPaths` both share the same user-facing label value, which may be an intentional aliasing artifact; preserve this exact mapping in Rust to avoid progress/reporting regressions.

## Verification Oracles

1. **WHEN** `AlgorithmLabel.from(Algorithm.AllShortestPaths)` is called **THEN** it **SHALL** return `AllShortestPaths`.
2. **WHEN** `AlgorithmLabel.from(Algorithm.SingleSourceDijkstra)` is called **THEN** it **SHALL** return the `SingleSourceDijkstra` enum member and `asString()` **SHALL** be `"All Shortest Paths"`.
3. **WHEN** any known `Algorithm` case is added without `from(...)` update (in tests/compilation) **THEN** Java compile should fail in exhaustive switch migration.

## Rust Rewrite Notes

- `L1`: internal `enum AlgorithmLabel` with string view type and conversion traits.
- `L2`: explicit mapping table from `Algorithm` discriminant to `AlgorithmLabel`.
- `L3`: maintain `Display`/`as_string` parity for progress output.
- Keep alias mappings explicit (especially where UI label differs from enum discriminant).

## Dependencies Read Next

| File | Why |
| --- | --- |
| `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/Algorithm.java` | source enum mapped by `from(...)` |
| `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/Label.java` | interface contract |

## Dependents As Tests

| Caller | Why |
| --- | --- |
| `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithms.java` | constructs progress tasks using labels |
| `applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithms.java` | task naming and algorithm family routing |
| `applications/algorithms/path-finding/.../PathFindingAlgorithms.java` | path-finding label coverage |

## Open Questions

- Should the rewrite split `AlgorithmLabel` into static labels and display labels to allow stable machine identifiers while preserving user-visible strings?

## Coding Prompt Unlocked

Implement `enum AlgorithmLabel` and compile-time exhaustive `from(Algorithm)` conversion in Rust with one unit test asserting `SingleSourceDijkstra -> "All Shortest Paths"` and one test asserting round-trip for `Label -> display string`.

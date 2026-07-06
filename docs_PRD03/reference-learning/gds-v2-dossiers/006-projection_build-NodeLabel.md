# 6 projection_build NodeLabel

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/NodeLabel.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 6 |
| line_count | 48 |
| fan_in / fan_out | 256 / 1 |
| purpose | Define node label identity and wildcard behavior used by projection and algorithm filtering. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

**Evidence:** This is the node-label equivalent to `RelationshipType`, providing canonical labels and wildcard projection semantics via `__ALL__`. [Lines 28-47]

## Public Contract

- `NodeLabel` extends `ElementIdentifier` [Line 28].
- `ALL_NODES` sentinel equals `NodeLabel.of("__ALL__")` [Lines 30-31].
- `projectAll()` returns `ALL_NODES` [Lines 36-39].
- `of(@NotNull String name)` constructs typed objects [Lines 41-43].
- `listOf(@NotNull String...)` maps names to label objects [Lines 45-47].

## Internal Mechanics

- **Evidence:** No mutable fields or side effects beyond constructor and factory methods [Lines 28-47].
- **Evidence:** Inherits nullability and base identifier normalization from `ElementIdentifier` [Line 22; superclass reference].
- **Inference:** This file intentionally mirrors `RelationshipType` for consistency and to reduce type handling divergence between node and relationship projections.

## Memory And Storage Implications

- **Evidence:** Same low-memory pattern as `RelationshipType`: all constant and reference operations [Lines 28-47].
- **Inference:** Most memory behavior comes from external string lists built by `listOf` and downstream projection expansion.

## Snapshot And Catalog Implications

- **Inference:** A wildcard label must be preserved consistently to query “all labels” in cataloged graph builds without accidentally dropping filters.
- **Evidence:** No snapshot IDs live here; identity is a pure model primitive consumed by catalog/procedure layers [Lines 28-47].

## Verification Oracles

1. **WHEN** `NodeLabel.of("Person")` is created and `toString` is observed via inherited behavior  
   **THEN** **SHALL** retain and expose `"Person"` label.

2. **WHEN** `NodeLabel.projectAll()` is called  
   **THEN** **SHALL** return `NodeLabel` with name `__ALL__`.

3. **WHEN** `NodeLabel.listOf("A", "B", "A")` is used as fixture input  
   **THEN** **SHALL** return a collection preserving source order.

## Rust Rewrite Notes

- **L1:** Introduce `NodeLabel` as typed newtype/enum companion to `RelationshipType` in Rust projection API.
- **L1:** Preserve sentinel `ALL_NODES` (`"__ALL__"`) and `project_all()` mapping.
- **L2:** Add list parser with deterministic allocation strategy (`Vec<NodeLabel>`) and explicit `Clone`.
- **L3:** Reuse a shared parser/validation utility with `RelationshipType` to avoid semantic drift.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| graph-projection-api/src/main/java/org/neo4j/gds/ElementIdentifier.java | base behavior for names and compatibility checks |
| graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java | adjacent projection primitive |
| graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java | paired identifier model |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| algo/src/main/java/org/neo4j/gds/embeddings/graphsage/GraphSageHelper.java | consumes node-label identity |
| algo/src/main/java/org/neo4j/gds/algorithms/community/CommunityAlgorithms.java | projection and filter setup |
| algo/src/test/java/org/neo4j/gds/embeddings/fastrp/FastRPTest.java | likely exercises label parsing paths |

## Open Questions

- Should wildcard token `__ALL__` remain a reserved value, or should configuration-level escape handling be introduced in Rust?
- Are null/empty string names tolerated by `ElementIdentifier`, or should Rust reject them explicitly earlier than Java callsites?

## Coding Prompt Unlocked

Implement a Rust `NodeLabel` module including sentinel behavior, list parser, and cross-check tests against the Java `NodeLabel` invariants.


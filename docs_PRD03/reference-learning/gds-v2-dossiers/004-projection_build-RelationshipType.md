# 4 projection_build RelationshipType

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 4 |
| line_count | 52 |
| fan_in / fan_out | 294 / 1 |
| purpose | Define relationship-type identity, all-type sentinel, and parsing/list helpers used across projection and algorithms. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

**Evidence:** This is the primitive type contract for relationship projection filters and matching; it exports a typed identity object used across algorithm entrypoints and projection builders. [Lines 28-52]

## Public Contract

- `RelationshipType` extends `ElementIdentifier` [Line 28].
- `ALL_RELATIONSHIPS` is the sentinel value for wildcard relationship matching via `__ALL__` [Line 30].
- `projectAll()` is overridden to return `ALL_RELATIONSHIPS` [Lines 37-39].
- `of(@NotNull String name)` constructs typed instances from caller-provided names [Lines 41-43].
- `toString(RelationshipType)` maps to `name()` [Lines 45-47].
- `listOf(@NotNull String...)` maps each string through `of(...)` and returns a `Collection` [Lines 49-51].

## Internal Mechanics

- **Evidence:** The entire type is immutable and constructor-only: no mutating methods, no setters [Lines 28-52].
- **Evidence:** Wildcard behavior is centralized in one constant + override pair (`ALL_RELATIONSHIPS`, `projectAll`) [Lines 30,37-39].
- **Inference:** Type parsing/validation is delegated to `ElementIdentifier`; this file constrains only identity semantics.
- **Evidence:** `projectAll()` is polymorphic through base type contract, enabling generic wildcard handling in callsites.

## Memory And Storage Implications

- **Evidence:** No memory-heavy operations; methods are constant-time constructors and list mapping [Lines 32-51].
- **Inference:** The key memory/perf impact is callsite-side list allocation in `listOf`; this can be controlled by reusing parsed identifiers or interned strings in rewrite.

## Snapshot And Catalog Implications

- **Inference:** Wildcard `__ALL__` is important for projection breadth and must be represented as exact sentinel behavior in catalog queries and validation to avoid accidentally broadening scans.
- **Evidence:** No embedded projection metadata is stored here; this is a pure identifier primitive used by catalog/projection layers [Lines 28-52].

## Verification Oracles

1. **WHEN** `RelationshipType.of("FRIEND")` is used and then `toString(...)` called  
   **THEN** **SHALL** return `"FRIEND"` and preserve identity type.

2. **WHEN** `RelationshipType.listOf("A", "B", "A")` is called  
   **THEN** **SHALL** return a collection containing one object per input token (including duplicates) and maintain order.

3. **WHEN** `projectAll()` is called on any `RelationshipType` instance  
   **THEN** **SHALL** return a `RelationshipType` whose sentinel name is `__ALL__`.

## Rust Rewrite Notes

- **L1:** `enum RelationshipType { AllRelationships, Typed(String) }` or `struct RelationshipType { name: Arc<str> }` with constant `ALL_RELATIONSHIPS`.
- **L1:** Preserve wildcard contract as `project_all` and map to `__ALL__` constant to maintain compatibility.
- **L2:** Add `list_of` parser utility for `&[&str] -> Vec<RelationshipType>`.
- **L3:** Keep this in projection API crate (no heavy external deps).

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| graph-projection-api/src/main/java/org/neo4j/gds/ElementIdentifier.java | base type behavior for validation and nullability |
| graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java | complementary relation orientation logic |
| graph-projection-api/src/main/java/org/neo4j/gds/NodeLabel.java | sibling primitive pattern for node identity |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| algo/src/main/java/org/neo4j/gds/algorithms/machinelearning/KGEPredictParameters.java | uses typed relation identifiers |
| algo/src/main/java/org/neo4j/gds/algorithms/similarity/SimilaritySingleTypeRelationshipsHandler.java | filters by relationship type |
| algo/src/main/java/org/neo4j/gds/indexInverse/InverseRelationships.java | uses relationship type constants and semantics |

## Open Questions

- Should duplicates in `listOf` be deduplicated at this layer or left to higher layers?
- Should `ALL_RELATIONSHIPS` include normalization (trim/upper-case) or preserve raw names?

## Coding Prompt Unlocked

Implement Rust equivalents and add unit tests for wildcard, identity round-trip, and duplicate-preserving list parsing to match the Java contract.


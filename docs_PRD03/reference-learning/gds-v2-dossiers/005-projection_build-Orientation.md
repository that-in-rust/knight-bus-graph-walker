# 5 projection_build Orientation

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 5 |
| line_count | 83 |
| fan_in / fan_out | 273 / 0 |
| purpose | Capture orientation parsing and default semantics for edge direction in projection paths. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

**Evidence:** This enum defines how projection and algorithms interpret directional traversal expectations, including inversion semantics and strict parse-time validation. [Lines 27-83]

## Public Contract

- Enum values: `NATURAL`, `REVERSE`, `UNDIRECTED`, each with explicit `inverse()` behavior [Lines 29-46].
- Static `VALUES` caches legal names as strings to make parsing O(1)-ish lookups by membership check [Lines 50-53].
- `parse(Object input)` accepts `String` (case-insensitive) and `Orientation` directly [Lines 55-71].
- `parse` throws `IllegalArgumentException` for unsupported string values and for unsupported input object types [Lines 62-77].
- `toString(Orientation)` delegates to enum `toString` [Lines 80-82].

## Internal Mechanics

- **Evidence:** String inputs are canonicalized with `toUpperCase(Locale.ENGLISH)` before validation [Lines 57].
- **Evidence:** Error path includes supported values and input class in message formatting [Lines 62-67,73-77].
- **Inference:** Because there is no null guard, `parse(null)` will throw a `NullPointerException` at `input.getClass().getSimpleName()` in the final branch [Lines 73-77], which is externally visible behavior.
- **Inference:** `parse` is intentionally strict and defensive about value domain.

## Memory And Storage Implications

- **Evidence:** Constant-time enum and small list, no heap allocations beyond list creation at class initialization [Lines 50-53].
- **Inference:** The primary memory consideration is avoiding repeated creation of direction strings in caller code; centralizing parse reduces duplicate parsing state.

## Snapshot And Catalog Implications

- **Inference:** Projection direction impacts graph traversal shape, so preserving `inverse()` correctness is critical before algorithm execution over projected snapshots.
- **Evidence:** No graph IDs are modified or stored in this file; it is a control parameter type consumed by upstream projection/catalog logic.

## Verification Oracles

1. **WHEN** `Orientation.parse("natural")` is called  
   **THEN** **SHALL** return `Orientation.NATURAL` and not mutate global state.

2. **WHEN** `Orientation.parse("ReVersE")` is called  
   **THEN** **SHALL** return `Orientation.REVERSE`.

3. **WHEN** `Orientation.parse("sideways")` is called  
   **THEN** **SHALL** throw `IllegalArgumentException` whose message lists supported values.

4. **WHEN** `Orientation.parse(Orientation.UNDIRECTED)` is called  
   **THEN** **SHALL** return the same enum constant.

5. **WHEN** `Orientation.parse(null)` is called  
   **THEN** **SHALL** fail with `NullPointerException` (current behavior); **SHALL** capture this as an explicit compatibility edge case in Rust migration.

## Rust Rewrite Notes

- **L1:** `enum Orientation { Natural, Reverse, Undirected }` with `inverse(&self)` returning mapped variant.
- **L2:** `parse(input: impl Into<OrientationInput>) -> Result<Orientation, ParseError>` where a `NullInput` case becomes explicit error instead of panic.
- **L3:** Keep user-facing parser in projection API crate; avoid duplicating parse semantics across algorithm and catalog modules.
- **Open compatibility decision:** preserve legacy Java error text where tests depend on exact messages, or document incompatibility.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| applications/algorithms/similarity/src/main/java/org/neo4j/gds/algorithms/similarity/SimilaritySingleTypeRelationshipsHandler.java | orientation used for traversal behavior |
| algo/src/main/java/org/neo4j/gds/indexInverse/InverseRelationships.java | orientation controls edge interpretation |
| graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java | sibling identifier semantics |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| algo/src/main/java/org/neo4j/gds/degree/DegreeCentrality.java | direction-sensitive algorithm behavior |
| algo/src/main/java/org/neo4j/gds/louvain/LouvainMemoryEstimateDefinition.java | algorithm estimation depends on orientation inputs |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimings.java | downstream timing/diagnostics for orientation-dependent execution |

## Open Questions

- Should Rust API preserve legacy null-parse crash semantics or switch to explicit `InvalidInput` error for safety?
- Should unknown orientation parsing remain case-insensitive in all external-facing APIs?

## Coding Prompt Unlocked

Write a Rust parser for `Orientation` with explicit error enum variants for unknown token, wrong type, and null-like missing input; include tests for canonicalized token parsing and inverse mapping.


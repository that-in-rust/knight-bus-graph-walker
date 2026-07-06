# 20 Aggregation

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 20 |
| line_count | 152 |
| fan_in / fan_out | 111 / 1 |
| purpose | Map how Neo4j-shaped inputs become projected graph topology, labels, relationship types, orientation, and properties. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

- `Aggregation` controls how multiple projected relationships are merged when duplicate relationships are encountered during projection/import.
- This is central to graph build correctness and memory usage since it decides whether repeated inputs collapse, accumulate, or throw.
- It is a high fan-in contract type and propagates behavior to load-time schema semantics.

## Public Contract

- Enum constants:
  - `DEFAULT` (placeholder only; not intended for direct use, `merge` throws)
  - `NONE`
  - `SINGLE`
  - `SUM`
  - `MIN`
  - `MAX`
  - `COUNT`
- Abstract/overridable contract:
  - `public abstract double merge(double runningTotal, double value)` (required by all variants)
  - `public double normalizePropertyValue(double value)` (default passthrough)
  - `public double emptyValue(double mappingDefaultValue)` (default passthrough)
- `parse(Object input)` behavior:
  - accepts `String`: upper-cases via locale helper, checks against all enum names, throws `IllegalArgumentException` on unsupported value with supported-list text.
  - accepts `Aggregation`: returns as-is.
  - rejects other inputs with `IllegalArgumentException` that reports input class.
- `resolve(Aggregation aggregation)` maps `DEFAULT -> NONE`, otherwise passthrough.
- `equivalentToNone(Aggregation aggregation)` checks `resolve(...) == NONE`.

## Internal Mechanics

- `VALUES` is a precomputed immutable list of all enum names used by parse validation.
- Merge behavior by variant:
  - `DEFAULT`: always unsupported; explicit guard path.
  - `NONE`: unsupported for repeated entries in load context; tells user to use `SINGLE` or another strategy.
  - `SINGLE`: returns running total unchanged.
  - `SUM`: additive.
  - `MIN`: min fold with NaN fallback to `POSITIVE_INFINITY`.
  - `MAX`: max fold with NaN fallback to `NEGATIVE_INFINITY`.
  - `COUNT`: additive running totals; `normalizePropertyValue` always returns `1.0`.
- `emptyValue(mappingDefaultValue)` controls identity/initial accumulator semantics per aggregator:
  - `SUM`: returns provided default unless NaN, then 0.
  - `MIN`: default or `POSITIVE_INFINITY`.
  - `MAX`: default or `NEGATIVE_INFINITY`.
  - `COUNT`: always `0.0`.

## Memory And Storage Implications

- This class is compute-only and stateless; no mutable shared state or caches.
- In Rust, keep behavior value-based and side-effect-free to avoid projection races under parallel loaders.
- `SINGLE`/`NONE` branches alter accumulation assumptions and thus shape memory cardinality downstream.

## Failure / Incompatibility Surfaces

- `parse("unsupported")` throws `IllegalArgumentException` with supported list.
- `parse(non-string/non-enum)` throws `IllegalArgumentException` with runtime class name.
- Using `DEFAULT` as a live aggregation value is a contract bug and currently explicit `UnsupportedOperationException`.
- `NONE` signals multi-edge policy mismatch and should be handled at config translation layer (if projection API allows).

## Verification Oracles

1. **WHEN** `Aggregation.parse("sum")` is called  
   **THEN** **SHALL** return `Aggregation.SUM`.
2. **WHEN** `Aggregation.parse("unsupported")` is called  
   **THEN** **SHALL** throw `IllegalArgumentException` with a supported values list.
3. **WHEN** `Aggregation.parse(42)` is called  
   **THEN** **SHALL** throw `IllegalArgumentException` with actual type mention.
4. **WHEN** `resolve(DEFAULT)` is called  
   **THEN** **SHALL** return `NONE`.
5. **WHEN** `equivalentToNone(aggregation)` is called  
   **THEN** **SHALL** be true for `NONE` and `DEFAULT`, false otherwise.

## Rust Rewrite Notes

- **L1:** represent aggregation as an enum with total match coverage.
- **L2:** keep parse as explicit total parse function returning `Result<Aggregation, String>`.
- **L3:** preserve identity semantics:
  - `sum`, `min`, `max`, `count`, `single`.
  - treat `default` as validation-time alias-to-none only if compatibility demands.
- Keep list-backed parse diagnostics for migration debugging and reproducibility.

## Dependencies Read Next

- `graph-projection-api/src/main/java/org/neo4j/gds/core/Projection` loaders using `Aggregation`.
- Any relationship loader pipeline calling `Aggregation.parse(...)`.
- `NodeLabel`, `RelationshipType`, `Orientation` contracts in projection API.

## Dependents As Tests

- Projection loader tests that validate duplicate relationship handling.
- Config round-trip tests covering string parsing and fallback behavior.
- Backward-compatibility tests that keep `DEFAULT` unsupported behavior stable.

## Open Questions

- Should `Aggregation.DEFAULT` remain a hard runtime guard or be normalized into `NONE` at parse time in Rust?
- Should unsupported values use richer typed errors (`UnknownValue`) instead of generic `IllegalArgumentException`?

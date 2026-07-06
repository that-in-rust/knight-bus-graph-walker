# 10 projection_build ValueType

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 10 |
| line_count | 257 |
| fan_in / fan_out | 168 / 3 |
| purpose | Define supported node property value types, serialization names, and compatibility behavior for projection/algorithm metadata. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

**Evidence:** `ValueType` encodes the canonical property-type model used across projections and algorithms: name mapping to Cypher/CSV, default values, visitor dispatch, array compatibility, and parse-time validation errors. [Lines 30-256]

## Public Contract

### Evidence-backed contracts
- Supported primitive/map variants include `LONG`, `DOUBLE`, `STRING`, `DOUBLE_ARRAY`, `FLOAT_ARRAY`, `LONG_ARRAY`, `UNTYPED_ARRAY`, `UNKNOWN` [Lines 186-206].
- All variants expose `cypherName()`, `csvName()`, `fallbackValue()`, and `accept(Visitor)` [Lines 208-215].
- `isCompatibleWith(ValueType other)` defaults to exact match and is overridden for `UNTYPED_ARRAY` [Lines 216-218, 179-184].
- `fromCsvName(String csvName)`:
  - iterates all enum values and skips `UNKNOWN` [Lines 221-227];
  - throws `IllegalArgumentException` with supported values list on miss [Lines 231-237].
- `Visitor` supports typed hooks (`visitLong`, `visitDouble`, `visitString`, `visitLongArray`, `visitDoubleArray`, `visitFloatArray`) and default `visitUnknown() -> null` [Lines 240-256].

### Inference
- This is effectively a closed set of supported projection value types with runtime pattern matching via visitor.

## Internal Mechanics

- **Evidence:** Enum variants map to output names via methods, sometimes returning the same string for multiple variants (`DOUBLE` and `FLOAT_ARRAY` both return list variants of float, `STRING` returns `"String"` etc.) [Lines 54-61, 118-124].
- **Evidence:** Unknown and untyped arrays intentionally throw `UnsupportedOperationException` for CSV conversion [Lines 175-176,194-195].
- **Inference:** Distinct fallback values can be used to provide stable defaults even when algorithm input type conversion fails upstream.
- **Inference:** The method `fromCsvName` relies on `StringJoining.join` over all `csvName()` values, meaning unsupported values are surfaced with runtime string list.

## Memory And Storage Implications

- **Evidence:** `DefaultValue` fallback is returned per variant to produce typed default placeholders [Lines 43-45,64-66,86-87,106-108,128-129,169-171,198-200].
- **Inference:** This enum shape itself is zero-allocation at runtime aside from occasional exception path allocations.
- **Inference:** Type compatibility checks (`UNTYPED_ARRAY`) allow certain assignments without full conversion and should be explicit in Rust rewrite to avoid unsafe casting.

## Snapshot And Catalog Implications

- **Evidence:** This file is pure type metadata used to interpret projection and property values, not store graph snapshots directly [Lines 30-256].
- **Inference:** In snapshoted OLAP workflows, using stable `ValueType` identifiers ensures deterministic export/import and cross-language compatibility for property serialization.

## Verification Oracles

1. **WHEN** `ValueType.fromCsvName("long")` is called  
   **THEN** **SHALL** return `ValueType.LONG` and call-site `accept` on `ValueType::visitLong`.

2. **WHEN** `ValueType.fromCsvName("Any[]")` is called  
   **THEN** **SHALL** return `ValueType.UNTYPED_ARRAY` or fail depending on current behavior if `csvName` matching excludes `UNKNOWN` and includes `UNTYPED_ARRAY`.

3. **WHEN** `ValueType.fromCsvName("weird")` is called  
   **THEN** **SHALL** throw `IllegalArgumentException` and include supported csv values in the message.

4. **WHEN** `UNTYPED_ARRAY.isCompatibleWith(LONG_ARRAY)` is queried  
   **THEN** **SHALL** return true; **WHEN** `UNTYPED_ARRAY.isCompatibleWith(DOUBLE)` is queried **SHALL** return false.

5. **WHEN** `UNKNOWN.csvName()` is called  
   **THEN** **SHALL** throw `UnsupportedOperationException`, preserving unsupported input signaling.

## Rust Rewrite Notes

- **L1:** Implement `enum ValueType` with variants for each Java case.
- **L2:** Provide `cypher_name`, `csv_name`, `fallback_value`, and visitor methods.
- **L2:** Represent visitor as trait object or enum pattern matching (`match self`) for zero-alloc dispatch.
- **L3:** Implement `from_csv_name` parser with explicit `UnsupportedValueType` error returning all supported keys.
- **Compatibility:** Keep unsupported paths (`Unknown` csv name / untyped CSV) as explicit error returns to mirror Java exceptions.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java | default placeholder payload contract [Lines 23,44-47] |
| string-formatting/src/main/java/org/neo4j/gds/utils/StringFormatting.java | error message formatter in `fromCsvName` [Line 28] |
| string-formatting/src/main/java/org/neo4j/gds/utils/StringJoining.java | supported values rendering [Line 235] |
| graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/NodePropertyValues.java | consumer of typed value behavior |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| algo-params/similarity-params/src/main/java/org/neo4j/gds/similarity/knn/metrics/SimilarityMetric.java | metric parsing likely validates value types |
| algo/src/main/java/org/neo4j/gds/algorithms/community/CommunityCompanion.java | property/type handling in algorithm setup |
| algo/src/main/java/org/neo4j/gds/algorithms/similarity/SimilaritySingleTypeRelationshipsHandler.java | value type driven dispatch |
| algo/src/main/java/org/neo4j/gds/hits/HitsMemoryEstimateDefinition.java | memory estimation for typed results |

## Open Questions

- Are `Double[]` and `Float[]` intended to remain distinct semantically in Rust despite identical `cypherName()` output?
- Should parser accept aliases (e.g., `"long[]"`) as forward-compatible CSV names?
- Should `visitUnknown` return a typed explicit enum instead of `Option::None` for clarity?

## Coding Prompt Unlocked

Implement Rust `ValueType` with parser + compatibility matrix tests:
- verify round-trip for supported csv names,
- verify unsupported csv names throw explicit parser errors,
- verify visitor dispatch and `UNTYPED_ARRAY` compatibility behavior.


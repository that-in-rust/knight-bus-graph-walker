# 22 projection_build DefaultValue

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 22 |
| line_count | 272 |
| fan_in / fan_out | 107 / 4 |
| purpose | Map how Neo4j-shaped inputs become projected graph topology, labels, relationship types, orientation, and properties. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

- This class defines fallback/default behavior for projected property values at the API boundary.
- It is reused when schema/value defaults are passed across graph projection and procedure layers; wrong semantics here affect all loaders and algorithm defaults.
- It has non-trivial conversion logic between primitive wrappers/arrays and numeric semantics.

## Public Contract

- Factory methods:
  - `of(Object)`, `of(Object, boolean)`, `of(Object, ValueType, boolean)`
  - typed convenience factories: `forInt()`, `forLong()`, `forDouble()`, `forFloat()`, `forLongArray()`, `forFloatArray()`, `forDoubleArray()`
- Core accessors:
  - `boolean isUserDefined()`
  - `long longValue()`
  - `double doubleValue()`
  - `float[] floatArrayValue()`
  - `double[] doubleArrayValue()`
  - `long[] longArrayValue()`
  - `Object getObject()`
- Equality and representation:
  - `toString()`, `equals(Object)`, `hashCode()`
- Invalid conversion behavior:
  - `getInvalidTypeException(Class<?>)` throws `IllegalArgumentException` with expected and actual class names.

## Internal Mechanics

- Internal storage is one field: `defaultValue` (nullable `Object`) + `isUserDefined` flag.
- Constants:
  - numeric fallbacks (`Integer.MIN_VALUE`, `Long.MIN_VALUE`, `Float.NaN`, `Double.NaN`) and typed cached array fallbacks.
- `of(Object)` delegates to `of(Object, true)` and preserves existing `DefaultValue` instances.
- If input is `List`, converts list to primitive array via `transformObjectToPrimitiveArray`.
- `of(Object, ValueType, boolean)` handles `LONG`, `DOUBLE`, `DOUBLE_ARRAY`, `LONG_ARRAY`, `FLOAT_ARRAY`; otherwise fallback behavior via `DefaultValue.of`.
- `longValue()`:
  - null -> `LONG_DEFAULT_FALLBACK`
  - NaN wrappers -> fallback
  - converts Double/Float with exact conversion
  - allows general `Number`
- `doubleValue()`:
  - explicit conversion from `Long/Integer`
  - NaN fallback semantics for special long fallback sentinel
- `floatValue/doubleValue arrays` convert between typed arrays with overflow-safe or exact conversion helpers.
- `equals` uses `Objects.deepEquals` to compare array-backed values correctly.

## Storage And Storage Implications

- Primary RAM holder is the `defaultValue` object and possibly converted array copies in typed accessors.
- Repeated `float/double/long` conversions can allocate new arrays when input is scalar or incompatible typed array.
- For hot projection paths, keep this abstraction lazy and typed to avoid unnecessary boxing/copying.

## Failure / Incompatibility Surfaces

- `longValue()` / `doubleValue()` / array methods throw `IllegalArgumentException` if `defaultValue` is wrong runtime type.
- Parsing `Object` into `ValueType` is strict and uses `Long.parseLong/Double.parseDouble` for scalar values; parse exceptions may surface from Java wrappers.
- Unknown/null `ValueType` string-like values in external callers fail before this class via parser layers; this class expects typed enum/objects.

## Verification Oracles

1. **WHEN** `DefaultValue.forLong()` is used  
   **THEN** **SHALL** return a fallback equivalent to `Long.MIN_VALUE` and report as non-user-defined.
2. **WHEN** `DefaultValue.of(Arrays.asList(1,2))` (or mixed numeric list) is used  
   **THEN** **SHALL** convert to a primitive array default container with `isUserDefined == true`.
3. **WHEN** `doubleValue()` is read from `Long.MIN_VALUE` sentinel  
   **THEN** **SHALL** return `Double.NaN` fallback.
4. **WHEN** `longValue()` reads `Double.NaN` or `Float.NaN`  
   **SHALL** return `Long.MIN_VALUE` fallback.
5. **WHEN** array accessors are called on mismatched types  
   **THEN** **SHALL** throw `IllegalArgumentException` with concrete type mismatch message.
6. **WHEN** two defaults with equal primitive arrays are compared  
   **THEN** **SHALL** be deep-equal via `Objects.deepEquals`.

## Rust Rewrite Notes

- **L1:** represent as enum-backed `DefaultValue` (`Int`, `Long`, `Float`, `Double`, `LongArray`, `FloatArray`, `DoubleArray`, `UserValue`).
- **L2:** enforce typed conversions with explicit error channels for overflow/type mismatch.
- **L3:** preserve user-defined tracking to keep provenance from config parser vs fallback values.
- **Memory optimization:** cache common singleton defaults (`forInt`, `forLong`, etc.) as `static`/`const` values.

## Dependencies Read Next

- `org.neo4j.gds.api.nodeproperties.ValueType`
- `DefaultValueUtil` helper conversion functions.
- `ValueConversion` conversion utilities.
- Projection schema builders that rely on typed fallback constants.

## Dependents As Tests

- Projection property schema tests for numeric defaults and array defaults.
- Migration tests that feed both null and explicit null-equivalent values.
- Equality tests for deep-equal arrays and fallback sentinels across primitive types.

## Open Questions

- Should Rust carry sentinel constants (`MIN_VALUE`, `NaN`) as explicit enum variants or metadata on `DefaultValue`?
- Should list-to-primitive conversion fail-fast with detailed index diagnostics for mixed types?

# 18 memory_estimator Estimate

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 18 |
| line_count | 343 |
| fan_in / fan_out | 128 / 1 |
| purpose | JVM object/primitive/runtime-size calculators for memory estimation semantics and human-readable formatting. |
| read_prompt | Read this entire file as the sizing engine contract. Extract size formulas, JVM assumptions, reflection behavior, overflow handling, and display contracts for rewrite parity. |

## Why This File Matters

- This is the memory calibration bedrock for many estimation computations.
- It computes object headers, field sizes, and array alignment assumptions based on runtime JVM options.
- Many estimate formulas ultimately rely on these constants and helpers.

## Public Contract

- Static sizing APIs:
  - primitives/arrays: `sizeOfByteArray`, `sizeOfCharArray`, `sizeOfShortArray`, `sizeOfIntArray`, `sizeOfFloatArray`, `sizeOfLongArray`, `sizeOfDoubleArray`, `sizeOfObjectArray`, `sizeOfObjectArrayElements`, `sizeOfBitset`, `sizeOfOpenHashContainer`, `sizeOfLongDoubleHashMap`, `sizeOfLongHashSet`, `sizeOf*List`, etc.
  - class instance size: `sizeOfInstance(Class<?>)`
  - human display: `humanReadable(long bytes)`
- Static constants:
  - `MAX_ARRAY_SIZE`, `BYTES_ARRAY_HEADER`, `BYTES_OBJECT_HEADER`, `BYTES_OBJECT_REF` etc.

## Internal Mechanics

- Runtime calibration runs in static init:
  - detects 32/64-bit architecture,
  - attempts HotSpot diagnostic bean reads for compressed oops and object alignment.
- Field size adjustment:
  - primitives use explicit primitive size map,
  - refs resolve to `1L << SHIFT_OBJECT_REF` (4 or 8 depending on compression).
- `alignObjectSize` and `sizeOfInstance` compute alignment and reflectively walk declared fields across class hierarchy.
- `humanReadable` progressively shifts for units (`Bytes`, `KiB`, `MiB`, ...) with a right-shift threshold.

## Memory And Storage Implications

- This class directly impacts estimate correctness for all memory trees and should be aligned with platform/runtime assumptions.
- Reflection-based size calc means estimates are architecture/JVM dependent.
- Constants can change by runtime environment and are intentionally discovered at startup.

## Snapshot And Catalog Invariants

- The estimate engine assumes HotSpot-like JVM behavior; absent bean access falls back to best-effort defaults.
- Estimation is advisory/diagnostic; it is not an allocator and should not alter algorithm semantics.

## Verification Oracles

1. **WHEN** `sizeOfInstance` is called for arrays
   **THEN** **SHALL** reject with `IllegalArgumentException`.
2. **WHEN** array length is negative in sizing methods
   **THEN** **SHALL** throw runtime exception via underlying array math/align logic.
3. **WHEN** `humanReadable` receives large sizes
   **THEN** **SHALL** switch through units and never return unsupported overflow output (throws `UnsupportedOperationException` only beyond expected range).
4. **WHEN** `sizeOfOpenHashContainer` is called with negative count
   **THEN** **SHALL** throw `IllegalArgumentException`.
5. **WHEN** primitive vs reference fields are mixed in class
   **THEN** **SHALL** aggregate field sizes and align final object size.

## Rust Rewrite Notes

- **L1:** create a platform-aware sizing module with conservative defaults.
- **L2:** implement JVM-like constants/heuristics explicitly in config.
- **L3:** isolate reflection-like behavior behind derive/macro input (Rust cannot read Java class structure directly).
- Provide `#[cfg(test)]` fixtures to mimic HotSpot defaults for portability.

## Dependencies Read Next

- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java`
- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java`
- hppc types used by sizing helpers.

## Dependents As Tests

- Algorithm memory estimator unit tests that indirectly validate computed ranges for known structures.
- Regression tests around `MemoryRange` and estimate totals.

## Open Questions

- In Rust, should sizing use target platform constants or configured constants from deployment profile?
- Is it acceptable to approximate HotSpot-specific details under non-HotSpot environments?

## Coding Prompt Unlocked

Implement Rust `Estimate` sizing utilities with runtime-detected or configurable alignment constants, and keep a compatibility matrix for HotSpot defaults vs explicit-config mode.

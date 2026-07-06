# 081 memory_estimator BitUtil

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/mem/BitUtil.java |
| lane | memory_estimator |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 81 |
| line_count | 114 |
| fan_in / fan_out | 42 / 0 |

## Why This File Matters

This is the low-level bit-level utility crate used by memory estimators and alignment helpers.

## Public Contract

- Power-of-two checks:
  - `isPowerOfTwo(int)`
  - `isPowerOfTwo(long)`
- Rounding helpers:
  - `previousPowerOfTwo(int/long)`
  - `nearbyPowerOfTwo(int/long)` chooses nearest power-of-two
  - `nextHighestPowerOfTwo(int/long)`
- Memory-alignment and division:
  - `align(long value, int alignment)` asserts power-of-two alignment
  - `ceilDiv(long, long)` and `ceilDiv(int, int)`

## Internal Mechanics

- Standard bit-hack methods are used for fast O(1) arithmetic.
- `align` performs `(value + alignment - 1) & ~(alignment - 1)` after asserting alignment validity.
- `nearbyPowerOfTwo` chooses next vs previous using absolute distance.
- `previous/next` methods intentionally saturate on already-pow2 inputs by early return semantics.

## Memory and Storage Implications

- These functions directly affect allocator behavior in estimator calculations and buffer sizing.
- They should be stable and side-effect free because many estimators assume deterministic rounding behavior.

## Snapshot And Catalog Implications

- No catalog/state ownership.
- In Rust rewrite, this maps to a small utility module with strict unit tests for integer edge cases.

## Verification Oracles

1. **WHEN** `align` is called with non-power-of-two alignment, **THEN** an assertion/error shall be raised.
2. **WHEN** input already equals a power-of-two, **THEN** previous/next helpers SHALL return same value.
3. **WHEN** value is between two powers, **THEN** `nearbyPowerOfTwo` SHALL return nearest according to distance rule.

## Rust Rewrite Notes

- Implement as `const fn`-friendly helpers where possible.
- Keep integer-width-specific variants explicit (`i32`/`i64`) for clear overflow control.

## Dependencies Read Next

- `MemoryEstimations`, `Estimate` sizing paths that consume align and power helpers.

## Dependents As Tests

- Unit tests for boundaries: `0`, `1`, powers, non-powers, and largest representable values.

## Open Questions

- Should integer rounding behavior remain Java-compatible bit-hack exactness or adopt saturating-safe alternatives in Rust for clarity?


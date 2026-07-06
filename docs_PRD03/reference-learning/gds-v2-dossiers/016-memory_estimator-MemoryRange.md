# 16 memory_estimator MemoryRange

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 16 |
| line_count | 161 |
| fan_in / fan_out | 131 / 1 |
| purpose | Immutable byte-range value with arithmetic helpers and invariants used by all estimate trees. |
| read_prompt | Read this entire file as the memory range algebra contract. Extract range validation, arithmetic semantics, and zero/empty representation. |

## Why This File Matters

- `MemoryRange` is the primitive unit of estimate values (min/max bytes).
- Encodes safe construction and arithmetic invariants that are easy to regress in a rewrite.
- Empty range is canonicalized to a single shared `NULL_RANGE` (`0..0`).

## Public Contract

- Factories:
  - `of(long value)`
  - `of(long min, long max)` (validates non-negative and `max >= min`)
  - `empty()`
- Value fields:
  - `min`, `max` are public final.
- Arithmetic:
  - `add(long)`
  - `add(MemoryRange)`
  - `times(long)`
  - `subtract(long)`
  - `elementWiseSubtract(MemoryRange)`
  - `union(MemoryRange)`
  - `max(MemoryRange)`
  - `apply(LongUnaryOperator)`
- Predicates:
  - `isEmpty()`

## Internal Mechanics

- Invalid construction checks:
  - negative inputs reject
  - max < min rejects
- Optimizations:
  - operations may return `this` when unchanged (identity/cheap path).
- `times(0)` returns canonical empty range.
- `subtract`/`elementWiseSubtract` use `Math.subtractExact` and funnel through `of(...)` for validation.
- `toString()`:
  - exact range prints `"x Bytes"` style if min==max
  - range prints `"[min ... max]"` via `Estimate.humanReadable`.

## Memory And Storage Implications

- Arithmetic uses checked operations (`Math.addExact`, `Math.multiplyExact`) meaning overflow produces fail-fast exceptions.
- As range is immutable final fields, copies are cheap and alias-safe.

## Snapshot And Catalog Invariants

- Empty representation is a shared singleton, so equality and zero checks are deterministic.
- Estimate logic often depends on empty-range behavior being idempotent for no-op scaling.

## Verification Oracles

1. **WHEN** `MemoryRange.of(-1)` is called
   **THEN** **SHALL** throw `IllegalArgumentException`.
2. **WHEN** `MemoryRange.of(5, 3)` is called
   **THEN** **SHALL** throw `IllegalArgumentException` ("max range < min").
3. **WHEN** `MemoryRange.empty().isEmpty()`
   **THEN** **SHALL** return true and `toString()` should show zero bytes.
4. **WHEN** `times(0)` is called on non-empty
   **THEN** **SHALL** return canonical empty range singleton.
5. **WHEN** `add` between two ranges causes negative overflow
   **THEN** **SHALL** fail via arithmetic overflow (`ArithmeticException`).

## Rust Rewrite Notes

- **L1:** implement an immutable `MemoryRange { min, max }` with constructor validation.
- **L2:** implement same overflow behavior (`checked_*` and explicit panic/err) as Java's exact arithmetic to avoid silent wraparound.
- **L2:** represent empty range as zero constant and canonicalize where possible.

## Dependencies Read Next

- `memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java` for human-readable output expectations.
- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java` for algebra integration.

## Dependents As Tests

- Memory estimation tests verifying range math behavior and overflow safety.
- Procedure estimate tests that assert exact range outputs.

## Open Questions

- Should Rust version return `Result<MemoryRange, Error>` or panic on invalid arithmetic for parity?
- How to preserve object-equality identity optimization (`this` return) semantics in Rust APIs?

## Coding Prompt Unlocked

Build `MemoryRange` in Rust with checked arithmetic, canonical empty singleton, and `union/max` semantics; test constructor + overflow + `times(0)` edge cases.

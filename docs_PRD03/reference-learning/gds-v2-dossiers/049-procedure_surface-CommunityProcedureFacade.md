# 49 procedure_surface CommunityProcedureFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/facade-api/community-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/community/CommunityProcedureFacade.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 49 |
| line_count | 441 |
| fan_in / fan_out | 2 / 56 |

## Why This File Matters

This is the community procedure façade surface for 40+ operations. It is the contract boundary for callable/stub exposure across stats/stream/write/estimate combinations.

## Public Contract

- **Evidence:** Interface exposes:
  - mutate-stub accessors (`approximateMaximumKCutMutateStub`, `k1ColoringMutateStub`, etc.),
  - per-algorithm methods for each mode (stats/stream/write) returning `Stream<...>`,
  - and corresponding `...Estimate` methods returning `Stream<MemoryEstimateResult>`.
- **Evidence:** Method families repeat consistent names across algorithm families (`43–84`, `87–147`, `148–207`, ... up to `419–437`).
- **Evidence:** All result methods are stream-based interfaces even for scalar-like payloads (`43–48`, `63–68`, `70–74`).
- **Inference:** The explicit duplication encodes compatibility shape and stable public API contract more than business logic.

## Internal Mechanics

- **Evidence:** No implementations here; this is a pure API declaration used by call-site wrappers/facades.
- **Evidence:** Naming convention (`Stats`, `Stream`, `Write`, `Estimate`) is machine-readable and likely drives reflection/dispatch patterns.
- **Inference:** This interface is effectively a compile-time and design-time compatibility lock; small refactors here risk broad API drift.

## Memory And Storage Implications

- **Evidence:** Return type choice is uniformly `Stream`, which keeps output materialization policy at the caller/adapter boundary.
- **Inference:** For estimates, stream semantics may be intentionally one-element, preserving a uniform transport layer and avoiding array/list special-casing.
- **Blocked:** Runtime cardinality and retention are decided by implementers.

## Snapshot And Catalog Implications

- **Evidence:** Many methods accept `String graphName` and some accept `Object graphNameOrConfiguration` for estimation mode, making this a compatibility bridge for old/new input forms (`49`, `48–51`, `65–67`, etc.).
- **Inference:** This indicates API stability expectations across procedure signatures and parser evolution.

## Verification Oracles

1. **WHEN** a community mutate operation is requested, **THEN** the facade **SHALL** expose corresponding mutate stub accessor.
2. **WHEN** algorithm mode is requested, **THEN** facade **SHALL** expose stats, stream, and write methods following naming conventions (`...Stats`, `...Stream`, `...Write`).
3. **WHEN** estimate is requested, **THEN** method **SHALL** return `Stream<MemoryEstimateResult>` and accept graph-or-config form where applicable.
4. **WHEN** output/result methods are consumed, **THEN** they **SHALL** be streamable, not directly list-returning.

## Rust Rewrite Notes

- **L1:** Trait `CommunityProcedureFacade` with algorithm families grouped by mode and result channel.
- **L2:** Strongly typed result enums for `Stats/Stream/Write/Estimate` outputs per algorithm.
- **L2:** `graphNameOrConfiguration` overload shape represented via explicit enum to avoid raw `Object`.
- **L3:** Generate repetitive façade methods from a DSL or macro to preserve API consistency without hand-written drift.

## Dependencies Read Next

- `procedures/facade-api/community-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/community/stubs/*`
- `procedures/facade-api/community-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/memory/MemoryEstimateResult` (path from import)
- concrete procedure adapter implementations for community algorithms.

## Dependents As Tests

- Procedure contract tests asserting each algorithm/mode has estimate + execution method.
- Stub-injection tests verifying mutate accessors are wired and non-null.
- Stream shape tests for one-element and multi-element result behavior.

## Open Questions

- Should Rust flatten method explosion via generic `execute(mode, algorithm, config)` APIs with generated wrappers, or keep explicit methods for compatibility parity?
- How to preserve deprecation/compatibility behavior for older method naming while introducing enum-based dispatch?

## Coding Prompt Unlocked

Implement the Rust community procedure facade as an explicit trait plus generated wrappers:
1) `mutate_stub` accessors per algorithm,
2) `stats/stream/write` per algorithm methods,
3) `estimate` variants with explicit graph-or-config input enum,
4) macro-based generation tests for signature parity and naming conventions.

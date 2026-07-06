# 3 memory_estimator MemoryEstimation

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 3 |
| line_count | 50 |
| fan_in / fan_out | 292 / 4 |
| purpose | Capture the generic memory-estimation contract used by algorithm and projection estimation flows. |
| read_prompt | Read this entire file as a strict RAM contract. Extract every memory term, range, estimator composition rule, unit, high-water state, rejection condition, and testable invariant. |

## Why This File Matters

**Evidence:** This file defines the single memory-estimation contract that many estimation-mode components rely on: any estimation component must expose a human-readable contract, nested components, and an `estimate(...)` operation. [Lines 31-50]

**Inference:** For the Rust rewrite, this likely represents the first interface to implement as a strict algebra (composition + query) before algorithm-specific estimators.

## Public Contract

### Evidence

- `description()` returns a textual label for a component [Lines 36].
- `components()` defaults to an immutable empty collection and represents nested child estimators [Lines 41-43].
- `times(long factor)` is a default combinator returning `MemoryEstimations.andThen(this, memoryRange -> memoryRange.times(factor))` [Line 46].
- `estimate(GraphDimensions dimensions, Concurrency concurrency)` returns a `MemoryTree` for concrete dimension/concurrency pairs [Line 49].

### Inference
- The interface is designed to support decomposition trees where complex estimators are composed from smaller ones.

## Internal Mechanics

- **Evidence:** The contract is intentionally minimal and side-effect free: there is no stateful field in this interface [Lines 20-50].
- **Evidence:** `components()` and `times(...)` are default methods, so implementers may opt into defaults and only implement a leaf estimate function [Lines 41-47].
- **Inference:** The design suggests two implementation classes should be expected later: leaves (direct formula over dimensions/concurrency) and internal combinators that stitch leaves.

## Memory And Storage Implications

- **Evidence:** Only one method returns `MemoryTree`, so this contract does not encode byte units directly [Lines 49].
- **Inference:** Allocations and units are likely deferred to `MemoryTree`/`MemoryRange`, meaning this boundary is a **shape contract** and not a storage allocator itself.
- **Inference:** The `times(long factor)` combinator can be used to amplify memory estimates for parallel phases or repeated structures; strict overflow handling may be required in Rust tests because `times` takes a signed `long`.

## Snapshot And Catalog Implications

- **Inference:** Since estimator interfaces consume `GraphDimensions` and `Concurrency`, these estimators can be run before heavy projection writes and therefore are likely candidates for preflight RAM rejection checks in snapshot workflows.
- **Evidence:** No graph identifiers are threaded through this interface directly, so callsites must inject catalog metadata before/after estimation [Lines 49].

## Verification Oracles

1. **WHEN** a concrete implementation returns `Collections.singleton("leaf")` from `components()` and its `estimate(...)` returns a known `MemoryTree`  
   **THEN** **SHALL** `components()` still reflect the explicit override and **SHALL** `times(2)` return a composed estimator whose result is computed by `MemoryEstimations.andThen(...)` in a wrapper test.

2. **WHEN** a mock `MemoryEstimation` does not override `components()`  
   **THEN** **SHALL** calling `components()` return an empty collection with size `0` and no side effects.

3. **WHEN** implementing a fixture estimator for `times(0)`  
   **THEN** **SHALL** assertion verify either a zero-memory or neutral estimate depending on `MemoryEstimations` semantics, and the test shall document whether negative and zero factors are rejected.

## Rust Rewrite Notes

- **L1:** `trait MemoryEstimation { fn description(&self) -> &str; fn components(&self) -> &[Arc<dyn MemoryEstimation>]; fn estimate(&self, dimensions:&GraphDimensions, concurrency:&Concurrency) -> MemoryTree; fn times(&self, factor:i64) -> Box<dyn MemoryEstimation>; }`
- **L2:** Keep `times` and empty-components behavior as default methods on a trait extension helper (or default trait methods).
- **L2/L3:** Define explicit error type for invalid factors before arithmetic scaling in combinator helpers.
- **Compatibility note:** Preserve `description`, `components`, and `estimate` naming in Rust-facing public APIs for ported estimator tests.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java | entrypoint for combinator helpers |
| memory-usage/src/main/java/org/neo4j/gds/mem/MemoryTree.java | return type of estimate contract |
| memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java | likely scalar unit carrier used by tree calculations |
| graph-dimensions/src/main/java/org/neo4j/gds/core/GraphDimensions.java | estimation input contract |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| algo-common/src/main/java/org/neo4j/gds/AlgorithmFactory.java | likely validates algorithm families through estimation contract |
| applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithms.java | consumer of memory estimators |
| applications/algorithms/centrality/src/main/java/org/neo4j/gds/algorithms/BetweennessCentralityMemoryEstimateDefinition.java | concrete estimator definition |
| applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithmsEstimationModeBusinessFacade.java | direct estimation caller |

## Open Questions

- Should `times(long factor)` reject negative factors at this interface boundary, or is that deferred to `MemoryTree` arithmetic?
- Should the trait return `Arc<[impl MemoryEstimation]>` to avoid repeated allocation of components vectors?
- What is the intended algebraic identity for composing multiple estimators with zero/overflow scale factors?

## Coding Prompt Unlocked

Create a Rust unit-test-first module implementing a `MemoryEstimation` trait and a `TimesAdapter`. Write tests that:
1) verify default empty components,
2) verify composition order of `times`,
3) verify error behavior on factor overflow or negative factors.

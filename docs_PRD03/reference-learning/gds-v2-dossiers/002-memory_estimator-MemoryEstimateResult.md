# 2 memory_estimator MemoryEstimateResult

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MemoryEstimateResult.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 2 |
| line_count | 98 |
| fan_in / fan_out | 307 / 4 |
| read_prompt | Read this entire file as a strict RAM contract. Extract every memory term, range, estimator composition rule, unit, high-water state, rejection condition, and testable invariant. |

## Why This File Matters

**Evidence:** This file is the canonical payload for estimate procedures and contains both presentation fields (`requiredMemory`, `treeView`, `mapView`) and hard numeric constraints (`bytesMin`, `bytesMax`, percentages, counts). [Lines 31-39,56-65].

**Inference:** It anchors preflight memory rejection policy for algorithm and projection entrypoints.

## Public Contract

- Immutable result object with fields:
  - `requiredMemory`, `treeView`, `mapView`, `bytesMin`, `bytesMax`, `nodeCount`, `relationshipCount`, `heapPercentageMin`, `heapPercentageMax`. [Lines 31-39].
- Public constructor from `MemoryTreeWithDimensions` and explicit constructor variant for scalars. [Lines 40-45,76-85].
- `getPercentage` computes ratio against process memory via `Runtime.getRuntime().maxMemory()`. [Lines 67-74].
- TODO debt in implementation comment: `// FIXME: pass the heap size from the outside?` [Line 54].

## Internal Mechanics

- **Evidence:** Constructor path 1 transforms a `MemoryTreeWithDimensions` into display and tree form [Lines 40-46].
- **Evidence:** Constructor 2 accepts explicit scalar inputs and bypasses source object [Lines 76-85].
- **Inference:** Heap coupling is process-level and not host-policy level; runtime caps should be externalized for deterministic testing and strict pre-run rejection.

## Memory And Storage Implications

- **Evidence:** `bytesMin` / `bytesMax` carry strict memory range bounds [Lines 59-61].
- **Evidence:** `heapPercentageMin/Max` are derived telemetry against process heap [Lines 61-74].
- **Inference:** In Rust, estimate consumers should compare against an explicit budget channel (CLI/config) rather than only runtime max, to make rejection deterministic.

## Snapshot And Catalog Implications

- **Inference:** This estimate output should gate catalog mutation/algorithm start; if limits are exceeded, caller should block project/stream/write transitions before mutation.
- **Evidence:** Since this DTO is not tied to specific projection ID, callers should pair with graph identifiers from catalog entries.

## Verification Oracles

1. **WHEN** constructor with explicit values is used (`requiredMemory="128 MB"`, `bytesMin=1_000_000`, `bytesMax=2_000_000`, `nodeCount=100`, `relationshipCount=50`)
   **THEN** **SHALL** preserve exact fields and expose `heapPercentageMin/Max` inputs unchanged.

2. **WHEN** `getPercentage(1_000_000, 2_000_000_000)` is called
   **THEN** **SHALL** produce `0.5` (rounded up to one decimal place).

3. **WHEN** denominator is `0`
   **THEN** **SHALL** return `Double.NaN` in percentage fields.

## Rust Rewrite Notes

- **L1:** `MemoryEstimateResult` struct with typed scalar fields (`u64`) and derived percentage fields.
- **L2:** `MemoryRange` and `MemoryTree` composition module producing estimate views.
- **L3:** procedure output mapper converting result into API rows with deterministic JSON rendering.
- **Refinement:** pass heap budget as dependency (`MemoryBudgetSource`) instead of reading runtime global directly.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java | source of low/high estimate values |
| memory-usage/src/main/java/org/neo4j/gds/mem/MemoryTreeWithDimensions.java | constructor input type |
| graph-dimensions/src/main/java/org/neo4j/gds/core/GraphDimensions.java | input metrics for dimensions |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java | estimate consumption path |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithmsEstimationModeBusinessFacade.java | estimate output handoff |
| applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsEstimationModeBusinessFacade.java | same |
| applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/CypherProjectApplication.java | memory estimate used by catalog flow |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmSpec.java | estimate template integration |

## Open Questions

- Should percentage fields use raw ratio, percentage points, or formatted strings in Rust contracts?
- Should `heapSize` be measured per execution queue/budget instead of process global?

## Coding Prompt Unlocked

Create a Rust estimator result module with deterministic constructors and tests:
- `MemoryEstimateResult::from_tree_with_dimensions` using injected budget,
- `MemoryEstimateResult::from_scalars`,
- property tests for `bytes_min <= bytes_max`, and
- NaN-safe behavior when budget is unknown.

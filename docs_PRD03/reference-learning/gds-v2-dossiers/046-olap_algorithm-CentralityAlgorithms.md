# 46 olap_algorithm CentralityAlgorithms

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithms.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 46 |
| line_count | 441 |
| fan_in / fan_out | 7 / 53 |

## Why This File Matters

This façade hosts all centrality algorithm entry points and is the canonical routing layer for algorithm-specific compute selection, progress/task construction, and execution through `AlgorithmMachinery`.

## Public Contract

- **Evidence:** Constructor wires `ProgressTrackerCreator` and `TerminationFlag` once for all algorithm methods (`90–96`).
- **Evidence:** Methods map configs to execution style and tasks:
  - streaming algorithms (`articleRank`, `eigenVector`, `pageRank` variants),
  - traverser-based algorithms (`betweennessCentrality`),
  - Pregel-driven algorithms with helper builders (`pageRankComputation`, etc.).
- **Evidence:** Several methods return typed result wrappers (e.g. `PageRankResult`, `BetwennessCentralityResult`, `ClosenessCentralityResult`) and consistently delegate execution to `algorithmMachinery.runAlgorithmsAndManageProgressTracker(...)` with an explicit `trackProgressAfterCompletion` flag (`128–134`, `176–181`, `191–196`, etc.).
- **Evidence:** Overloads separate core algorithm wiring from externally supplied progress tracker (`98–103` and `105–119`, `280–298`).
- **Inference:** This is the central contract for centrality API parity across modes and variants.

## Internal Mechanics

- **Evidence:** Each method builds task graph through `Tasks.leaf/task` before execution (`101–114`, `123–136`, `143–148`, `200–230`).
- **Evidence:** `betweennessCentrality` decides sampling strategy based on provided count and graph size (`156–159`).
- **Evidence:** Internal helper methods convert input into computation parameter objects to keep public method contracts clean (`389–425`).
- **Evidence:** `runAlgorithmsAndManageProgressTracker` is the universal execution edge, preserving consistent lifecycle handling (`128–137`, `176–181`, `191–197`, ...).
- **Inference:** `TerminationFlag` + `ProgressTrackerCreator` means cancellation and progress are first-class and should remain unmerged in rewrite.

## Memory And Storage Implications

- **Evidence:** `Huge` outputs are not in this layer, but this layer allocates per-run task trees and temporary structures (`Tasks...`, `LongScatterSet`, degree arrays helpers, mapped source nodes) (`252–263`, `396–410`, `434–438`).
- **Inference:** Memory hotspots are around temporary `LongScatterSet`/`Long` mapping and algorithm-specific internal structures; this façade coordinates these allocations but does not store persistent state.
- **Evidence:** `compute` calls are per-request and stateless; no object-level caches are present.

## Snapshot And Catalog Implications

- **Inference:** Snapshot/caching is external: methods accept a concrete `Graph` and configuration only.
- **Evidence:** No catalog APIs are used in this class, confirming this is a pure execution layer.

## Verification Oracles

1. **WHEN** betweenness is called without/with sampling size, **THEN** strategy selection **SHALL** reflect sample threshold (`156–159`).
2. **WHEN** `articleRank`/`eigenVector`/`pageRank` are invoked, **THEN** they **SHALL** route through dedicated helper builders and then `runAlgorithmsAndManageProgressTracker(...)`.
3. **WHEN** `pageRank(...)` has an existing progress tracker overload, **THEN** it **SHALL** reuse the same algorithm builder with provided tracker (`373–387`).
4. **WHEN** algorithm method executes, **THEN** cancellation flag should remain provided via injected `terminationFlag` in all algorithm constructors that require it (`108–116`, `166–174`, `276–294`, `376–384`).

## Rust Rewrite Notes

- **L1:** Centrality façade trait/class with per-algorithm entry methods keyed by typed config/result.
- **L2:** Shared execution helper (`run_algorithm`) that takes progress task, algorithm constructor, and cancellation signal.
- **L2:** Config transformer helpers (`to_parameters`, compute helpers) for heavy mapping before dispatch.
- **L3:** Strategy split (`RandomDegreeSelection` vs `FullSelection`) as explicit enum branch, not implicit branch in algorithm internals.

## Dependencies Read Next

- `applications/algorithms/machinery/AlgorithmMachinery.java`
- `applications/algorithms/machinery/ProgressTrackerCreator.java`
- Centrality algorithm classes (`betweenness`, `pagerank`, `closeness`, etc.)
- `core-utils` task/progress modules.

## Dependents As Tests

- Centrality procedure integration tests for articleRank, pageRank, betweenness variants.
- Cancellation/progress tests verifying termination flag wiring.
- Sampling strategy tests for betweenness with/without explicit `samplingSize`.

## Open Questions

- Should the strategy-selection function in betweenness be made a pluggable strategy module in Rust for cleaner test isolation?
- Do all centrality algorithms need both single and overloaded progress paths, or can a single path reduce duplication safely?

## Coding Prompt Unlocked

Implement centrality execution façade in Rust:
1) method-per-algorithm dispatch with config typing,
2) shared progress/task builder,
3) strategy-based betweenness selection,
4) helper functions for computation param preparation,
5) tests for overload reuse and termination flag propagation.

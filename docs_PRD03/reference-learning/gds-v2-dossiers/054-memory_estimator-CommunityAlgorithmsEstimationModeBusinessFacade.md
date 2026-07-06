# 54 memory_estimator CommunityAlgorithmsEstimationModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsEstimationModeBusinessFacade.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 54 |
| line_count | 279 |
| fan_in / fan_out | 19 / 33 |

## Why This File Matters

This class is the per-API estimation seam for community algorithms. It centralizes how every community algorithm’s estimator is built and how that estimator is fed into `AlgorithmEstimationTemplate`.

## Public Contract

- **Evidence:** Class holds one dependency, `AlgorithmEstimationTemplate algorithmEstimationTemplate`, injected via constructor (`56-61`).
- **Evidence:** For each supported algorithm, it exposes a `MemoryEstimation` builder method and a matching `MemoryEstimateResult` method that calls `algorithmEstimationTemplate.estimate(...)` with `(configuration, graphNameOrConfiguration, memoryEstimation)` (`63-84`, `88-109`, `112-133`, `126-147`, `143-165`, `160-176`, `174-191`, `188-205`, `202-220`, `219-245`, `233-250`, `251-263`, `265-277`).
- **Evidence:** Unsupported branches fail fast via `MemoryEstimationNotImplementedException` in `conductance()` and `triangles()` (`80-82`, `247-249`).
- **Inference:** The file enforces estimator shape consistency across modes by making every algorithm’s estimate path use the same `algorithmEstimationTemplate.estimate` handoff.

## Internal Mechanics

- **Evidence:** All methods are thin: they translate config into the right `MemoryEstimationDefinition` and delegate to shared template; there is no graph mutation or storage I/O here (`63-84`, `88-123`, etc.).
- **Evidence:** Different algorithms use different config projections when creating memory estimators (`configuration.toParameters()`, `.toMemoryEstimationParameters()`, `.seedProperty()`, `.isIncremental()`) before `.memoryEstimation()` (`112-117`, `144-145`, `251-253`).
- **Evidence:** The two unsupported methods are the only places that throw directly instead of delegating (`80-82`, `247-249`).
- **Inference:** This is a deterministic “policy + factory” layer for memory contracts, not execution runtime.

## Memory And Storage Implications

- **Evidence:** Memory estimation does not alter catalog/graph state; it is purely computational contract assembly (`63-277`).
- **Evidence:** The heavy-memory path is all downstream in `algorithmEstimationTemplate.estimate` and the estimator definitions this class instantiates (`64-85`, `98-99`, `233-234`, etc.).
- **Inference:** For rewrite safety, preserving this boundary is important: estimator wiring in one place avoids drift across ~11 community algorithms.
- **Blocked:** File-level RAM profile of each estimator body lives in imported `*MemoryEstimateDefinition` classes and `AlgorithmEstimationTemplate`.

## Snapshot And Catalog Implications

- **Evidence:** This file receives only Java config objects and emits memory outputs; no catalog service is invoked (`56-277`).
- **Inference:** Estimation can be treated as a catalog-safe read path and should remain independent from projection/catalog mutation semantics in the rewrite.
- **Inference:** The unsupported methods define explicit compatibility gaps that need to be mirrored or documented if Rust paths do not implement those algorithms yet (`80-82`, `247-249`).

## Verification Oracles

1. **WHEN** `approximateMaximumKCut(ApproxMaxKCutBaseConfig)` is called, **THEN** it **SHALL** return `new ApproxMaxKCutMemoryEstimateDefinition(...).memoryEstimation()` (`63-65`).
2. **WHEN** `k1Coloring(K1ColoringBaseConfig, Object)` is called, **THEN** it **SHALL** call `algorithmEstimationTemplate.estimate` with `k1Coloring()` as estimator source (`88-95`).
3. **WHEN** `conductance()` or `triangles()` are called, **THEN** method **SHALL** throw `MemoryEstimationNotImplementedException` (`80-82`, `247-249`).
4. **WHEN** a new community estimate mode is added, **THEN** implementation **SHALL** follow existing pattern: create estimator, then one template `estimate` call with config + graphNameOrConfiguration + estimator (`63-77` style pattern for each algorithm).

## Rust Rewrite Notes

- **L1:** Preserve this as a pure Rust estimation facade with one injected estimation template dependency and no side effects.
- **L2:** Model each algorithm method as returning either `MemoryEstimation` or `MemoryEstimateResult` via a consistent generic `estimate` helper function.
- **L2:** Keep unsupported algorithms explicit as `NotImplemented` branches instead of silent defaults.
- **L3:** Centralize estimator constructors in one module boundary to keep algorithm growth linear and predictable for rewrite validation.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java`
- `memory-usage/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MemoryEstimateResult.java`
- `org.neo4j.gds.mem.MemoryEstimation`
- all community `*MemoryEstimateDefinition` classes listed as dependencies

## Dependents As Tests

- Add estimation tests for each algorithm method (k1Coloring, kCore, kMeans, labelPropagation, lcc, leiden, louvain, modularity, modularityOptimization, scc, triangleCount, wcc, speakerListenerLPA).
- Add compatibility tests asserting `conductance` and `triangles` still throw as not-implemented.
- Add regression checks that config-specific factories (`toParameters`, `toMemoryEstimationParameters`, `seedProperty`, `isIncremental`) are forwarded unchanged.

## Open Questions

- Should Rust surface `MemoryEstimation` builders as a fixed trait set or use a map-based algorithm registry?
- Should unsupported estimators return explicit typed errors in the Rust API to preserve caller intent?
- Should `MemoryEstimationNotImplementedException` include algorithm identifiers for observability?

## Coding Prompt Unlocked

Implement `CommunityAlgorithmsEstimationModeBusinessFacade` in Rust as a side-effect-free estimation boundary:
1) Keep one injected template dependency, 2) create per-algorithm estimator constructors with the same config-to-parameters mapping, 3) route every supported algorithm through one `estimate` helper, 4) mark unsupported algorithms explicitly with fail-fast typed errors, and 5) preserve all method-level signatures in the rewrite contract tests.

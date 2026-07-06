# 32 olap_algorithm CommunityAlgorithms

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithms.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 32 |
| line_count | 528 |
| fan_in / fan_out | 9 / 65 |

## Why This File Matters

This file is the algorithm execution hub for community-style workloads (e.g., Leiden, Louvain, KMeans, triangles, modularity, WCC).  
**Evidence:** lines `93-102` define the class and inject only two dependencies (`ProgressTrackerCreator`, `TerminationFlag`) and then expose per-algorithm entry points in package scope/public methods.

## Public Contract

- **Evidence (104-128):** `approximateMaximumKCut` builds a dynamic iterative task graph, parses parameters via the algorithm config, instantiates `ApproxMaxKCut`, and runs it through `algorithmMachinery.runAlgorithmsAndManageProgressTracker(...)`.
- **Evidence (131-157):** `conductance` materializes transformed config parameters and passes them into `Conductance` with a composed progress task.
- **Evidence (160-174):** `k1Coloring` delegates to `K1ColoringStub`, passing `progressTracker`, `terminationFlag`, and `configuration.concurrency()` for run strategy.
- **Evidence (177-188):** `kCore` constructs `KCoreDecomposition` directly and executes with the same machinery pattern as conductance/KMeans.
- **Evidence (191-215):** `kMeans` performs explicit client-visible validation before execution:
  - rejects multiple runs with provided seed centroids (`lines 193-195`),
  - rejects mismatched seed counts (`196-198`).
- **Evidence (217-243):** `labelPropagation` builds iterative tasks for initialization and iteration, then constructs algorithm with `DefaultPool`, `progressTracker`, and termination.
- **Evidence (273-276):** `leiden` enforces undirected graph requirement and fails fast with `IllegalArgumentException` when violated.
- **Evidence (309-335):** `louvain` builds a specific Louvain progress task and passes max levels/iterations and concurrency into the algorithm.
- **Evidence (337-345):** `modularity` is pure compute; it does not run through the machinery wrapper and returns `modularityCalculator.compute()`.
- **Evidence (347-383):** `modularityOptimization` resolves optional seed property values, builds task + tracker, and executes through machinery.
- **Evidence (385-403) / (405-426):** `scc` has both a generic overload and a `ConcurrencyConfig` overload, with machinery-managed execution.
- **Evidence (428-427):** `triangles` returns a `Stream<TriangleResult>` directly from `TriangleStream.compute()`.
- **Evidence (439-451):** `wcc` logs warning if `relationshipWeightProperty` is set without threshold, then delegates to `WccStub`.
- **Evidence (453-507):** private helpers (`constructKMeansProgressTask`, `kMeansTask`, `searchTask`, `localSearchTask`) shape progress semantics and expose branch-specific task trees.
- **Evidence (509-527):** `speakerListenerLPA` constructs a Pregel progress task and executes through machinery.

## Internal Mechanics

- **Inference:** This file centralizes “algorithm contract -> progress/task wiring -> machinery execution” for community algorithms, while leaving heavy logic in algorithm classes and stubs.
- **Evidence:** The class has a single `AlgorithmMachinery` field created at declaration (`lines 94-94`) and reuses it across all algorithm methods.
- **Evidence:** Every main algorithm execution method follows a shared pattern:
  1. Build `Task` descriptor using `Tasks.*` utilities.
  2. Create tracker via `progressTrackerCreator.createProgressTracker(configuration, task)`.
  3. Build concrete algorithm/stub from config and graph.
  4. Call `runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true, configuration.concurrency())` (or equivalent stub call).
- **Inference:** The shared pattern implies we should preserve this as a common abstraction boundary in Rust to avoid drift in progress tracking behavior across algorithms.
- **Blocked:** There are no inline TODO/FIXME markers in this file.

## Memory And Storage Implications

- **Evidence:** No mutable state is retained per algorithm call beyond local variables; object-level state is constant shared dependencies.
- **Inference:** RAM behavior is dominated by algorithm-specific graph/model objects, not by this coordinator.
- **Evidence:** Warnings and validation happen before long allocations (`kMeans`, `leiden`, `wcc`) so failing paths avoid unnecessary compute.
- **Evidence:** `triangles` returning `Stream<TriangleResult>` is significant for output streaming and may cap intermediate buffering.

## Snapshot And Catalog Implications

- **Inference:** This class is not a catalog owner; it is a façade over algorithm invocation, so it must stay pure orchestration for compatibility.
- **Evidence (27-31):** Uses `Graph` and algorithm-specific parameter objects from projections; no `GraphStoreCatalog` calls are present in this file.
- **Evidence (273-276, 443-446):** Validation and warning behavior is part of runtime contract and should be preserved in rewrite tests.

## Verification Oracles

1. **WHEN** `kMeans` receives seeded centroids and `numberOfRestarts > 1` **THEN** it **SHALL** throw `IllegalArgumentException("K-Means cannot be run multiple time when seeded")`.
2. **WHEN** `kMeans` receives non-empty seed centroids with size different from `k()` **THEN** it **SHALL** throw `IllegalArgumentException("Incorrect number of seeded centroids given for running K-Means")`.
3. **WHEN** `leiden` receives a directed graph **THEN** it **SHALL** throw `IllegalArgumentException("The Leiden algorithm works only with undirected graphs...")`.
4. **WHEN** `wcc` receives `hasRelationshipWeightProperty()==true` and `threshold()==0` **THEN** it **SHALL** emit a warning but still attempt normal execution.

## Rust Rewrite Notes

- **L1:** `CommunityAlgorithms` as orchestrator module with internal immutable config + cancellation token.
- **L2:** Common helper trait for constructing and executing `(Task -> progressTracker -> algorithm -> runManagedExecution)` paths with default options.
- **L2:** Represent algorithm-specific preflight checks (`kMeans` and `leiden`) as explicit validation functions before execution branch.
- **L2:** Stream-based outputs (`triangles`) return iterator-like API that avoids materializing all rows.
- **L3:** Integrate a `ProgressTaskDsl` + `TaskRegistry` service and algorithm-specific config translators.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmMachinery.java` (execution orchestration utility)
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ProgressTrackerCreator.java` (progress lifecycle)
- `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsStatsModeBusinessFacade.java` (mode façade)
- `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsWriteModeBusinessFacade.java` (mode mapping)
- `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsMutateModeBusinessFacade.java` (mode mapping)
- `applications/algorithms/community/src/test/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsTest.java` (behavioral expectations)
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmLabel.java` (label contract)
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java` (estimation pipeline for neighboring specs)

## Dependents As Tests

- **Caller:** `CommunityAlgorithmsMutateModeBusinessFacade` — invokes community algorithm entry points by mode.
- **Caller:** `CommunityAlgorithmsStreamModeBusinessFacade` — exercises streaming/compute mode routing.
- **Caller:** `CommunityAlgorithmsStatsModeBusinessFacade` — exercises stats-oriented result construction.
- **Caller:** `proc/community/src/integrationTest/...` classes (triangles/LCC/Leiden/WCC suites) — exercise public output semantics.

## Open Questions

- **Inference:** Which of these algorithms should remain “public API” in Rust vs. internal-only wrappers? Only a subset is directly called by procedure surfaces.
- **Question:** Should `modularity` stay thinly outside `AlgorithmMachinery`, or should rewrite enforce a unified execution abstraction for all algorithm functions to simplify tracing?

## Coding Prompt Unlocked

Implement `CommunityAlgorithms` in Rust with:
1. one orchestrator module using a shared execution helper;
2. algorithm methods for `leiden`, `louvain`, `kMeans`, `wcc`, `modularity`, and `triangles`;
3. validation checks mirroring current exception paths;
4. stream/result polymorphism so triangles-style outputs can be consumed lazily;
5. tests for the 4 invariants in section "Verification Oracles".

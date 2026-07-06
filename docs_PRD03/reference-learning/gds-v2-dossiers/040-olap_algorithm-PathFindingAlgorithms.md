# 40 olap_algorithm PathFindingAlgorithms

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithms.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 40 |
| line_count | 553 |
| fan_in / fan_out | 14 / 54 |

## Why This File Matters

This class is the path-finding business façade: algorithm-specific orchestration, task shaping, and execution lifecycle. It sits one layer above concrete algorithm types and below procedure/flow layers, so it defines behavior shared across many path-finding APIs in a low-variance way.

## Public Contract

- **Evidence:** Class-level docs (lines 83–90) describe this as a “bottom business facade” where callers pass Graph/config and this layer dispatches algorithm execution.
- **Evidence:** Constructor injects `RequestScopedDependencies` and `ProgressTrackerCreator` (`98–104`), establishing per-request context and progress factory control.
- **Evidence:** Each algorithm method creates a `ProgressTracker` from configuration/task, instantiates the domain algorithm, and executes via `algorithmMachinery.runAlgorithmsAndManageProgressTracker` (`107–113`, `120–137`, `158–175`, `221–227`, `328–327` etc.).
- **Evidence:** Directional API split:
  - Graph-returning methods (`allShortestPaths`, `breadthFirstSearch`, etc.) and numeric-stream results are explicit (`106–110`, `145–156`, `297–328`).
  - `singlePairShortestPathYens` has a shared overload with a pre-built `ProgressTracker` (`391–413`).
- **Evidence:** There are mode-specific validation shortcuts for topology assumptions, e.g., `kSpanningTree` and `spanningTree` reject directed inputs (`196–199`, `439–442`).
- **Evidence:** `selectAlgorithm` returns either `WeightedAllShortestPaths` or `MSBFSAllShortestPaths` based on whether relationship weights are present (`529–544`).

## Internal Mechanics

- **Evidence:** The constructor keeps orchestration stateless except for injected request context and progress tracker creator (`92–104`).
- **Evidence:** Each execution path typically:
  1. Build config-aware task (`113–136`, etc.),
  2. Build algorithm + progress tracker (`122–134`, `167–173`, `203–218`),
  3. Delegate to `AlgorithmMachinery` for managed execution (`131–137`, `169–175`, `220–227`, `322–327`, etc.).
- **Evidence:** Optional validation and source mapping pass-through is pushed up/down boundary: path finder only maps source nodes once and passes through `requestScopedDependencies.terminationFlag()` into long-running algorithms (`154–155`, `190–191`, `318–319`).
- **Inference:** This design prevents algorithm duplication by enforcing a common compute-and-manage wrapper around each algorithm while allowing local algorithm selection and task specialization.

## Memory And Storage Implications

- **Evidence:** Algorithms returning large collections (`HugeLongArray`, `HugeAtomicLongArray`, and node-ID arrays) are streamed through strongly typed result objects (`145–156`, `297–308`, `466–475`, `485–494`).
- **Inference:** The most expensive memory behavior is in algorithm-specific implementations, not in this façade, but this file controls whether execution uses stream semantics and whether reroute subtasks are created (`473–478`).
- **Evidence:** This layer allocates per-call temporary task structures (`Tasks.leaf`, `Tasks.task`) and progress objects before entering execution (`113–119`, `145–157`, `202–208`, `504–510`).
- **Blocked:** Exact heap profiles and retained-size deltas are not available in this file; profiling must be captured at algorithm/computation module tests.

## Snapshot And Catalog Implications

- **Evidence:** Methods accept `Graph` directly, not graph names, indicating name resolution and catalog lookup occur in an upper layer (`106–110`, `112–119`).
- **Inference:** Any cache invalidation or graph snapshot behavior is orthogonal and controlled by callers; rewriting should preserve this seam.

## Verification Oracles

1. **WHEN** `kSpanningTree(...)` is called on a directed graph, **THEN** it **SHALL** throw `IllegalArgumentException` with a message indicating spanning tree requires undirected edges (`196–199`).
2. **WHEN** `singlePairShortestPathYens(...)` is called through both overloads, **THEN** the two overloads **SHALL** share the same execution path via the overload that accepts a prebuilt `ProgressTracker` (`381–392`).
3. **WHEN** `singleSourceShortestPathDijkstra(...)` is called, **THEN** it **SHALL** call `algorithmMachinery.runAlgorithmsAndManageProgressTracker(...)` with `releaseProgressTracker=false` equivalent behavior (`430–434`).
4. **WHEN** a path-finding config has weighted edges, **THEN** `selectAlgorithm(...)` **SHALL** return `WeightedAllShortestPaths`; otherwise `MSBFSAllShortestPaths` (`529–544`).

## Rust Rewrite Notes

- **L1:** Introduce `PathFindingAlgorithms` as a small façade with deterministic method signatures per supported algorithm and request-scoped dependencies.
- **L2:** Standardize path-finding result wrappers (`StreamResult`, `PathFindingError`, `NodeArrayResult`) and helper `run_with_progress`.
- **L2:** Preserve topology preconditions as explicit guards before delegating to algorithms (`spanningTree`/`kSpanningTree`).
- **L2:** Keep shared `create_progress_tracker` boundary to centralize task-shape construction and cancellation flags.
- **L3:** Bind concrete algorithm adapters for path families (`Dijkstra`, `Yens`, `AStar`, `TopologicalSort`) to avoid monolithically huge Rust enums.

## Dependencies Read Next

- `executor/.../AlgorithmMachinery.java`
- `applications/algorithms/machinery/ProgressTrackerCreator.java`
- `applications/algorithms/machinery/RequestScopedDependencies.java`
- `applications/algorithms/machinery/AlgorithmLabel.java`
- Concrete algorithm classes imported in this file (`MSBFSAllShortestPaths`, `DagLongestPath`, `RandomWalk`, etc.)

## Dependents As Tests

- Procedure-layer path-finding procedure tests that assert each `...Result` path/estimate contract.
- Mode-specific tests (streaming vs mutate/write equivalence in outer layers).
- Graph-oriented tests that inject undirected and directed graphs into `kSpanningTree` / `spanningTree`.

## Open Questions

- **Inference:** Some methods return package-private types for layer-local composition (`106–110`, `145–156`, `195 etc.`); we should decide whether Rust keeps them crate-private or fully public for cross-crate calls.
- **Inference:** Should `algorithmMachinery.runAlgorithmsAndManageProgressTracker(...)` remain synchronous in Rust for deterministic tests, or be moved to async with bounded scheduler?

## Coding Prompt Unlocked

Implement a Rust equivalent `PathFindingAlgorithms` module with:
1) explicit config-specific result mapping for each path algorithm,
2) shared progress creation helpers (including override path for prebuilt trackers),
3) uniform managed execution adapter,
4) topology precondition checks as typed errors,
5) tests covering the weighted/unweighted algorithm split and directed-graph spanning-tree rejection.

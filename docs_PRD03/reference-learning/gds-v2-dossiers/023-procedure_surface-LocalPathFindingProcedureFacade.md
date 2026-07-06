# 23 procedure_surface LocalPathFindingProcedureFacade

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/pathfinding/LocalPathFindingProcedureFacade.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 23 |
| line_count | 1103 |
| fan_in / fan_out | 1 / 107 |
| purpose | Capture procedure ABI: names, modes, columns, config parsing, errors, unsupported behavior, and verification fixtures. |
| read_prompt | Read this entire file as a public GDS procedure surface contract. Extract procedure names, modes, columns, config parsing, result shapes, side effects, errors, unsupported behavior, and verification fixtures. |

## Why This File Matters

- This is one of the largest compatibility surfaces for procedure calls in pathfinding.
- It maps more than 30 public operations across stats/stream/write/estimate into business facades and result builders.
- It is extremely high-risk for rewrite parity because signature shape, parsing path, and mutation stubs must remain stable.

## Public Contract

- Implements `PathFindingProcedureFacade` and owns a full request-scoped constructor + static `create(...)` factory.
- Primary constructor dependencies:
  - request services: `CloseableResourceRegistry`, `NodeLookup`, `ProcedureReturnColumns`
  - delegated facades: estimate/stats/stream/write path business interfaces
  - mutate stubs for each domain family
  - `UserSpecificConfigurationParser`
- `create(...)` does full adapter assembly:
  - extracts applications from `ApplicationsFacade.pathFinding()`
  - creates local mutate stubs (`bellmanFord`, `bfs`, `delta`, `dfs`, `randomWalk`, `shortest path` variants, `spanning`, `steiner`)
  - returns configured facade instance.
- Supported operation families:
  - all shortest paths stream
  - bellman-ford stream/stats/write (+ estimates + stub getter)
  - BFS stats/stream (+ estimate + stub getter)
  - delta stepping stats/stream/write (+ estimate + stub getter)
  - DFS stream (+ estimate + stub getter)
  - K-spanning-tree write (+ estimate + stub getter)
  - longest path stream
  - prize-collecting Steiner path stream (+ estimate + stub getter)
  - random walk stats/stream (+ estimate + stub getter)
  - single-pair shortest path A* stream/write (+ estimate + stub getter)
  - single-pair shortest path Dijkstra stream/write (+ estimate + stub getter)
  - single-pair shortest path Yens stream/write (+ estimate + stub getter)
  - single-source shortest path Dijkstra stream/write (+ estimate + stub getter)
  - spanning tree stats/stream/write (+ estimate + stub getter)
  - Steiner tree stats/stream/write (+ estimate + stub getter)
  - topological sort stream
- Each estimate endpoint returns `Stream<MemoryEstimateResult>` and delegates to estimation facade with parsed config and graph-or-config input.
- Stream/stats/write endpoints parse config, then delegate to relevant business mode with dedicated result builder.

## Internal Mechanics

- Parse-and-dispatch pattern repeated across methods:
  1. parse config using `configurationParser.parseConfiguration(configuration, <Config>::of)`,
  2. parse graph with `GraphName.parse(graphName)` when method accepts graph name,
  3. construct method-specific result builders (often carrying `closeableResourceRegistry`, `nodeLookup`, and `procedureReturnColumns` state),
  4. call business facade method.
- `procedureReturnColumns` drives path inclusion and sometimes output shape:
  - `"route"` in Bellman-Ford stream
  - `"path"` for several stream modes.
- Builder creation patterns include stream-specific `ResultBuilderForStreamMode` and stats-specific builders that require parsed config for headers etc.
- `@Override` coverage indicates this file satisfies a stable interface and is likely validated via interface contract tests.

## Storage and Runtime Behavior

- Class itself stores only references to injected dependencies; no algorithm state.
- Per-request behavior constructs new result builders and parsed configs; this avoids sharing mutable algorithm render state.
- The heaviest memory usage is likely in business result objects and result builders produced for stream path results.
- For Rust, keep this as request-scoped façade struct to avoid cross-request contamination.

## Failure / Incompatibility Surfaces

- `GraphName.parse(graphName)` failure behavior is preserved for all graph-scoped endpoints.
- Parser failures (`parseConfiguration`) propagate for invalid parameter sets.
- Unsupported combinations are avoided by missing/miswired builder wiring: this file itself is strict but not a policy checker beyond parser+facade errors.
- `getters` for mutate stubs must always return configured stubs; null here is a hard mismatch.

## Verification Oracles

1. **WHEN** `create(...)` is called with valid dependencies  
   **THEN** **SHALL** return non-null `PathFindingProcedureFacade` with all mutate stubs set.
2. **WHEN** `allShortestPathStream` is called  
   **THEN** **SHALL** parse config with `AllShortestPathsConfig::of` and return stream mode output.
3. **WHEN** `bellmanFordStream` is called with graphName + config  
   **THEN** **SHALL** pass `routeRequested = procedureReturnColumns.contains("route")` into stream result builder.
4. **WHEN** any `<algo>Estimate(...)` endpoint is called  
   **THEN** **SHALL** return a singleton `Stream` with one estimate result from corresponding estimation facade method.
5. **WHEN** a stream endpoint that needs path output (e.g., shortest path, random walk, spanning, steiner, etc.) is called with `procedureReturnColumns.contains("path") = false`  
   **THEN** **SHALL** omit or suppress path-dependent projection behavior accordingly.
6. **WHEN** write endpoint is called  
   **THEN** **SHALL** return a `Stream` wrapper around write facade result and use write-mode result builders.

## Rust Rewrite Notes

- **L1:** model this as `LocalPathFindingProcedureFacade` struct with explicit mode facades and stub fields.
- **L2:** group endpoint methods into nested modules by mode:
  - `stream_mode`, `stats_mode`, `write_mode`, `estimate_mode`.
- **L3:** preserve one configuration parse boundary:
  - strongly typed parser with method-specific config factories and error propagation.
- Keep `create(...)` centralized so stub wiring is deterministic and easy to test.
- For performance, use streaming result iterator abstraction that can emit empty streams without allocation of wrapper objects.

## Dependencies Read Next

- `applications.algorithms.pathfinding` facade interfaces used for estimation/stats/stream/write.
- `procedures.algorithms.pathfinding` stubs (`Local...MutateStub`, `...MutateStub`).
- Pathfinding result builder classes (`BellmanFord...`, `Bfs...`, `PathFinding...`, etc.).
- `ProcedureReturnColumns` contract and `GraphName.parse`.

## Dependents As Tests

- Pathfinding procedure integration tests across stats/stream/write/estimate for each algorithm family.
- Config parser compatibility tests with path-related return columns (`route`, `path`).
- Mutate stub getter tests ensuring each getter returns live, initialized stub.

## Open Questions

- Should Rust expose one `path_requested` flag globally per request or per-method to avoid repeated column lookups?
- Can one stream-builder factory produce all path-related builders without regressions in column-dependent payload shape?

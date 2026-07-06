# 076 olap_algorithm PathFindingAlgorithmsMutateModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithmsMutateModeBusinessFacade.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 76 |
| line_count | 281 |
| fan_in / fan_out | 12 / 33 |

## Why This File Matters

This class is the mutate entrypoint façade for path-finding algorithms and the critical seam where mutation mode behavior is unified across algorithm families.

## Public Contract

- Constructor wires:
  - `PathFindingAlgorithmsEstimationModeBusinessFacade`
  - `PathFindingAlgorithms`
  - `AlgorithmProcessingTemplateConvenience`
  - `MutateNodeProperty`
- Each public method has the shared shape:
  - inputs: `GraphName`, strongly typed *mutate* config, `ResultBuilder`
  - output: generic `RESULT`
  - processing: `processRegularAlgorithmInMutateMode(...)`
- Mutate methods are:
  - `bellmanFord`
  - `breadthFirstSearch`
  - `deltaStepping`
  - `depthFirstSearch`
  - `randomWalk`
  - `singlePairShortestPathAStar`
  - `singlePairShortestPathDijkstra`
  - `singlePairShortestPathYens`
  - `singleSourceShortestPathDijkstra`
  - `spanningTree`
  - `steinerTree`
- `ResultBuilder` result metadata is algorithm-specific:
  - `NodePropertiesWritten` for random walk
  - `RelationshipsWritten` for the remaining path-building methods
- Mutate steps are instantiated per method (`new ...MutateStep`), except:
  - BFS/DFS share `SearchMutateStep`
  - shortest-path methods use `ShortestPathMutateStep` / `ShortestPathAStar...`

## Internal Mechanics

- The façade does not execute traversal logic directly; it delegates to:
  1. estimator facade (`estimationFacade::...`)
  2. algorithm executor (`pathFindingAlgorithms...`)
  3. mutate step construction (algorithm-specific)
  4. template convenience (`algorithmProcessingTemplateConvenience`)
- Generic orchestration preserves a strict contract boundary per method:
  - one estimator path
  - one execution path
  - one mutate path
  - one result builder path
- The TODO comment on `randomWalk` notes a likely future split in memory estimate strategy, implying estimator and algorithm contract are coupled.

## Memory and Storage Implications

- Mutating APIs produce `RelationshipsWritten` or `NodePropertiesWritten` payloads, so writes are expected to include explicit output metadata for downstream layers.
- Shared mutable state is minimized (constructor DI only); method-local mutate step creation should allow Rust implementations to keep per-call allocation localized.
- RAM-heavy logic (search frontier/queues, sampled structures) belongs in delegated algorithm implementations, while this class contributes mostly orchestration overhead.

## Snapshot And Catalog Implications

- This layer assumes a bound graph context exists in upstream procedure/catalog logic.
- Rewrite impact: retain generic `RESULT` plus result-builder abstraction to avoid special-casing each algorithm at this boundary.

## Verification Oracles

1. **WHEN** any supported mutate config is invoked, **THEN** the call path SHALL use `processRegularAlgorithmInMutateMode`.
2. **WHEN** breadth-first/dfs mutate configs are used, **THEN** `mutateRelationshipType` SHALL be mapped into `SearchMutateStep`.
3. **WHEN** `randomWalk` runs, **THEN** a `RandomWalkCountingNodeVisitsMutateStep` SHALL be used and return `NodePropertiesWritten`.
4. **WHEN** shortest-path configurations are used, **THEN** their `mutate` metadata SHALL be `RelationshipsWritten`.

## Rust Rewrite Notes

- Keep a single generic helper for regular mutate-path execution.
- Model mutate step creation as a small adapter layer keyed by config/label.
- Preserve one method-per-algorithm for ABI readability in phase-1 Rust rewrite.

## Dependencies Read Next

- `PathFindingAlgorithms.java`
- `PathFindingAlgorithmsEstimationModeBusinessFacade.java`
- `MutateNodeProperty` / `SearchMutateStep` / `ShortestPathMutateStep` / `ShortestPathAStarMutateStep`
- `AlgorithmLabel` enum members used: `AStar`, `BFS`, `BellmanFord`, `DFS`, `DeltaStepping`, `Dijkstra`, `RandomWalk`, `SingleSourceDijkstra`, `SteinerTree`, `SpanningTree`, `Yens`

## Dependents As Tests

- Contract test per method:
  - estimate function tied to execution function
  - mutate metadata type expected (`NodePropertiesWritten` vs `RelationshipsWritten`)
- Regression test for TODO-noted path (`randomWalk`) if estimator function changes.

## Open Questions

- Should `randomWalk` share exactly the same memory-estimation contract as graph algorithms with path outputs, or remain specialized?
- Should algorithm labels be validated before selection, or can they be assumed by call site selection?


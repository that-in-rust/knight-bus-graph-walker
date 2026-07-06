# 068 memory_estimator PathFindingAlgorithmsEstimationModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/path-finding/src/main/java/org/neo4j/gds/applications/algorithms/pathfinding/PathFindingAlgorithmsEstimationModeBusinessFacade.java |
| lane | memory_estimator |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 68 |
| line_count | 257 |
| fan_in / fan_out | 17 / 30 |

## Why This File Matters

This file is the top-level memory-estimation façade for path-finding algorithms. It defines which algorithms are currently estimated and where estimation is still unimplemented.

## Public Contract

- **Evidence:** It exposes one public estimation constructor path: `new PathFindingAlgorithmsEstimationModeBusinessFacade(AlgorithmEstimationTemplate)` and holds a final `algorithmEstimationTemplate` (`57-62`).
- **Evidence:** Pattern is uniform across implemented methods: build `MemoryEstimation` through an overload then call generic `runEstimation(configuration, graphNameOrConfiguration, memoryEstimation)` (`68-85`, `91-110`, `126-136`, etc.).
- **Evidence:** Unimplemented algorithms currently throw `MemoryEstimationNotImplementedException` in `allShortestPaths()`, `kSpanningTree()`, `longestPath()`, and `topologicalSort()` (`64-66`, `117-123`, `242`).
- **Evidence:** `runEstimation(...)` is a generic helper bound by `CONFIGURATION extends AlgoBaseConfig`, delegating to template estimate (`246-255`).
- **Inference:** Any rewrite must preserve the exact surfaced subset: these throws are compatibility-critical and indicate unsupported modes.

## Internal Mechanics

- **Evidence:** Each supported path has two methods: full configuration-specific `MemoryEstimateResult` and a pure `MemoryEstimation` builder (`68-75`, `78-89`, `91-102`, `104-115`, etc.).
- **Evidence:** Most algorithms delegate to dedicated `...MemoryEstimateDefinition` classes and return `.memoryEstimation()` (`75`, `88`, `101`, `114`, `136`, `149`, `165`, `181`, `183`, `196`, `198`, `211`, `213`, `225`, `239`).
- **Evidence:** Random-walk estimation includes both generic and mutate-config overloads; mutate uses `RandomWalkCountingVisitsMemoryEstimateDefinition(configuration.toMemoryEstimateParameters())` (`152-154`).
- **Evidence:** Several methods pass config to helper conversions before constructing definitions, e.g., `toMemoryEstimateParameters()` (`149`, `152`).
- **Blocked:** The actual estimation formulas live in memory-definition classes, so memory contract must merge file-level API with these definitions.

## Memory and Storage Implications

- **Inference:** This layer primarily orchestrates estimator construction and invocation; it is lightweight aside from object graph creation.
- **Inference:** Memory RAM surface for each algorithm lives in its dedicated `MemoryEstimationDefinition`; this file is the routing policy not the accounting formula.

## Snapshot And Catalog Implications

- **Inference:** No direct graph/catalog access happens here; runtime graphName/config route is delegated to `algorithmEstimationTemplate`.
- **Inference:** Unsupported operations should produce explicit, typed runtime exceptions rather than silent defaults.

## Verification Oracles

1. **WHEN** a supported estimate method receives `(configuration, graphNameOrConfiguration)`, **THEN** it SHALL call the generic `runEstimation(...)` with the corresponding memory definition.
2. **WHEN** an unsupported algorithm is called (`allShortestPaths`, `kSpanningPath`, `longestPath`, `topologicalSort`), **THEN** `MemoryEstimationNotImplementedException` SHALL be thrown.
3. **WHEN** random walk mutate config is used, **THEN** code SHALL use `toMemoryEstimateParameters()` before constructing `RandomWalkCountingVisitsMemoryEstimateDefinition`.
4. **WHEN** estimate template is invoked, **THEN** it SHALL receive a config constrained to `AlgoBaseConfig`.

## Rust Rewrite Notes

- **L1:** Implement façade methods per algorithm with explicit match between configuration and definition constructors.
- **L1:** Keep a single generic `run_estimation` helper that forwards `(config, graph_name_or_config, estimation)` to template.
- **L2:** Represent unsupported algorithms as explicit `Err(NotImplemented)` to preserve behavior, not hidden placeholder values.
- **L3:** Preserve the method naming/signature symmetry for stats-like contract extraction by higher-level callers.

## Dependencies Read Next

- `applications/algorithms/machinery/AlgorithmEstimationTemplate`
- Memory definition classes referenced:
  - `BellmanFordMemoryEstimateDefinition`, `BfsMemoryEstimateDefinition`, `DfsMemoryEstimateDefinition`, etc.
- Config classes:
  - `DijkstraSourceTargetsBaseConfig`, `ShortestPathYensBaseConfig`, `RandomWalkMutateConfig`, etc.

## Dependents As Tests

- Unit tests for supported methods ensure exact definition class instantiation.
- Regression test verifying unsupported throws with stable message semantics.
- Contract test that every public estimate method delegates into `runEstimation` path exactly once.

## Open Questions

- Should unsupported methods remain throw-only even if no caller reaches them, or can they return a sentinel estimate?
- Can this façade eventually own a dynamic registration map to remove manual duplication, or is manual expansion preferred for explicitness?
- Blocker: method-level call coverage is needed if these estimation APIs are exposed to user-visible procedures.


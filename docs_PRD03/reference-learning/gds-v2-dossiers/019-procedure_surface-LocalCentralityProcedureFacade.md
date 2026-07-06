# 19 procedure_surface LocalCentralityProcedureFacade

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/LocalCentralityProcedureFacade.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 19 |
| line_count | 1323 |
| fan_in / fan_out | 2 / 114 |
| purpose | Concrete procedure façade for centrality algorithms; delegates mode-specific entry points to algorithm facades and result builders. |
| read_prompt | Read this entire file as the centrality procedure surface contract. Extract procedure names, modes, columns, config parsing, result shapes, side effects, unsupported behavior, and verification fixtures. |

## Why This File Matters

- This file is the execution seam between procedure API contracts and algorithm implementations for many centrality entry points.
- It standardizes the call pattern across algorithm families (stream/stats/write/estimate + mutate stubs), which is critical when rewriting procedure surfaces in Rust without behavior drift.
- It owns the "shape discipline": every entrypoint receives `graphName` + raw config map and delegates using parsed config and mode-specific builder strategy.

## Public Contract

- `LocalCentralityProcedureFacade` is `public final` and implements `CentralityProcedureFacade`.
- Constructed by `create(...)`, injected with:
  - `ApplicationsFacade` (`centrality` application surfaces),
  - user-specific config parser,
  - and graph-procedure return column policy.
- Centralize operation families exposed through override methods:
  - alpha/harmonic (stream/write),
  - article rank (stats/stream/write + estimate + mutate-stub getter),
  - beta closeness (write + estimate + mutate-stub getter),
  - betweenness (stats/stream/write + estimate + mutate-stub getter),
  - articulation points (stream/stats/write + all estimate variants + mutate-stub getter),
  - bridges (stream/estimate),
  - CELF (stats/stream/write + estimate + mutate-stub getter),
  - closeness (stats/stream/write + estimate + mutate-stub getter),
  - degree (stats/stream/write + estimate + mutate-stub getter),
  - eigenvector (stats/stream/write + estimate + mutate-stub getter),
  - harmonic (stats/stream/write + estimate + mutate-stub getter),
  - pagerank (stats/stream/write + estimate + mutate-stub getter),
  - hits (stream/stats/write + estimate + mutate-stub getter).
- Every estimate endpoint returns `Stream<MemoryEstimateResult>`.
- Mutate getters return strongly typed stubs for callers in procedure layer (e.g., `BetweennessCentralityMutateStub`, `PageRankMutateStub<...>`).

## Internal Mechanics

### Construction and dependency graph

1. `create(...)` pulls `centralityApplications = applicationsFacade.centrality()`.
2. From it, obtains:
   - mutate façade (`centralityApplications.mutate()`),
   - estimate façade (`centralityApplications.estimate()`),
   - plus explicit `stats`, `stream`, and `write` facades in constructor parameters.
3. Instantiates local mutate wrappers once:
   - local page-rank based stubs and one-off stubs for all remaining centrality operations.
4. Returns a fully injected façade object containing these dependencies.

### Execution pattern per operation

Most procedure methods follow:

1. Parse user config (`configurationParser.parseConfiguration`).
2. Determine optional columns from `ProcedureReturnColumns.contains("centralityDistribution")` or `"similarityDistribution"`.
3. Create algorithm-specific result builder.
4. Delegate to corresponding application facade method (`statsModeBusinessFacade`, `streamModeBusinessFacade`, or `writeModeBusinessFacade`).
5. Estimate methods parse config and return `Stream.of(estimationModeBusinessFacade.<algo>(parsedConfiguration, graphNameOrConfiguration))`.

### Notable details

- `GraphName.parse(graphName)` is consistently applied before non-estimate delegations.
- `AlphaHarmonicCentrality` write path intentionally uses deprecated write config (`DeprecatedTieredHarmonicCentralityWriteConfig`).
- Some result builders are constructed with parsed config; others are default constructors plus parsed distribution flags.
- No procedure annotations are declared in this class; it is a pure façade adapter, not the procedure registration class.

## Inferred Storage / Runtime Behaviors

- No persistent mutable storage here; state is effectively the injected collaboration graph:
  - business facades,
  - parser,
  - return-column policy,
  - local stubs.
- Per call memory is mostly temporary:
  - parsed configuration instances,
  - result builders,
  - short-lived stream objects.
- RAM behavior to replicate in Rust: avoid repeated parser/facade reconstruction by hoisting to façade fields and reusing immutable dependencies.

## Failure / Incompatibility Surfaces

- The class itself does not `throw` domain errors directly; parsing and downstream façade calls can fail and should preserve existing exception behavior.
- Bad graph names fail through `GraphName.parse`.
- Unsupported/invalid configurations fail through parser or downstream algorithm config factories.
- Deprecated config handling (`DeprecatedTieredHarmonicCentralityWriteConfig`) is an intentional compatibility seam.

## Verification Oracles

1. **WHEN** `create(...)` is called with valid `applicationsFacade` and parser
   **THEN** **SHALL** return a non-null `CentralityProcedureFacade` with all expected local stubs injected (non-null on getter access).

2. **WHEN** any non-estimate entrypoint is called with malformed config
   **THEN** **SHALL** propagate parser/config validation failure in the same failure style as existing `parseConfiguration(...)` behavior.

3. **WHEN** an estimate path is called (`<algo>Estimate(...)`)
   **THEN** **SHALL** return a `Stream<MemoryEstimateResult>` containing estimate output generated from the estimation business facade.

4. **WHEN** `graphName` is invalid for an enabled facade call
   **THEN** **SHALL** fail through `GraphName.parse` with existing invalid-name semantics.

5. **WHEN** return columns include `"centralityDistribution"` and write-mode result builder is requested
   **THEN** **SHALL** pass the distribution flag into write builders that consume it.

6. **WHEN** `alphaHarmonicCentralityWrite` is executed
   **THEN** **SHALL** use `DeprecatedTieredHarmonicCentralityWriteConfig` as the parser config source.

## Rust Rewrite Notes

- **L1 (structure):** define a `LocalCentralityProcedureFacade` struct that stores facade handles + parser + return-column policy.
- **L2 (type-level parity):**
  - split APIs by mode modules (`stats_mode`, `stream_mode`, `write_mode`, `estimate_mode`),
  - expose typed stub getters per algorithm family.
- **L3 (behavioral parity):**
  - keep one config-parser abstraction boundary and one "builder factory" boundary,
  - preserve method fanout exactly (`stats/stream/write/estimate` matrix) to avoid compatibility gaps.
- **Important for memory safety:** avoid allocating estimate stream containers beyond one output per estimate call unless required by call-site interfaces.

## Dependencies Read Next

- `procedures/facade-api/centrality-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/CentralityProcedureFacade.java`
- `procedures/facade-api/centrality-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/stubs/*MutateStub`
- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/*ResultBuilder*`
- `applications` facades under `org.neo4j.gds.applications.algorithms.centrality`
- `org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser`

## Dependents As Tests

- `CentralityProcedureFacade` tests in verification modules covering stream/stats/write/estimate dispatch.
- Procedure integration tests that exercise all centrality procedures in both execution and estimate mode.
- Error-path tests around config parse failures for each supported centrality family.

## Open Questions

- Should deprecated `DeprecatedTieredHarmonicCentralityWriteConfig` usage remain behind a compatibility flag in Rust, or be modeled as explicit compatibility module?
- Can return-column checks be normalized across all centrality families in one shared parser policy to reduce per-method branching in rewritten Rust?

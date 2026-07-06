# 11 procedure_surface LocalCommunityProcedureFacade

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/community/LocalCommunityProcedureFacade.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 11 |
| line_count | 1290 |
| fan_in / fan_out | 4 / 163 |
| purpose | Route community-procedure calls (stats/stream/write + estimates + mutate stubs) from procedure facade surface into dedicated business facades. |
| read_prompt | Read this entire file as the community procedure facade seam. Extract parser wiring, per-procedure dispatch behavior, estimate vs compute split, and mutating-mode stub exposure needed for rewrite compatibility. |

## Why This File Matters

- **Surface seam:** This is the central dispatch surface for all community algorithms and therefore the ABI boundary for procedure-facing behavior.
- **Contract shape:** It exposes one method per algorithm mode (`stats`, `stream`, `write`) plus estimate overloads and mutate-stub accessors for many community algorithms.
- **Separation pattern:** Procedure input parsing and facade delegation are centralized here while operational behavior remains in `communityApplications.*` business facades and generated result builders.
- **Fan-out pressure:** High fan-out indicates this file mediates a large public surface.

## Public Contract

- `create(ApplicationsFacade, GenericStub, CloseableResourceRegistry, ProcedureReturnColumns, UserSpecificConfigurationParser) -> CommunityProcedureFacade`:
  - constructs algorithm-specific mutate stubs and facade delegates; injects them into facade state.
- Community procedures covered:
  - `approxMaxKCutStream`, `conductanceStream`
  - `k1ColoringStats/Stream/Write` and `k1Coloring*Estimate`
  - `kCore*` `stats/stream/write` and estimate methods
  - `kmeans*`, `labelPropagation*`, `localClusteringCoefficient*`
  - `leiden*`, `louvain*`, `modularity*`, `scc*`, `triangleCount*`, `trianglesStream`
  - `wcc*` and `sllpa*` endpoints
- Mutate stubs are exposed via:
  - `approximateMaximumKCutMutateStub`, `k1ColoringMutateStub`, `kCoreMutateStub`, `kMeansMutateStub`, `labelPropagationMutateStub`, `lccMutateStub`, `leidenMutateStub`, `louvainMutateStub`, `modularityOptimizationMutateStub`, `sccMutateStub`, `triangleCountMutateStub`, `wccMutateStub`, `speakerListenerLPAMutateStub`.
- **Parsing contract:** every compute/mutate/estimate path begins with `configurationParser.parseConfiguration(...)` and `GraphName.parse(graphName)` for runtime-facing operations.
- **Estimate contract:** each `*Estimate` method returns a singleton `Stream<MemoryEstimateResult>` produced by `estimationModeBusinessFacade`.

## Internal Mechanics

- **Field-driven constructor pattern:** class fields hold:
  - `CloseableResourceRegistry`
  - `ProcedureReturnColumns`
  - four mode facades (`estimation/stats/stream/write`)
  - many algorithm-specific mutate stubs
  - one `UserSpecificConfigurationParser`.
- **Mode split by convention:**
  - `statsModeBusinessFacade.*` for `*Stats` methods
  - `streamModeBusinessFacade.*` for `*Stream` methods
  - `writeModeBusinessFacade.*` for `*Write` methods
  - `estimationModeBusinessFacade.*` for `*Estimate` methods
- **Common request path (observed repeatedly):**
  1. parse config
  2. build mode-specific result builder (when needed)
  3. delegate to relevant business facade.
- **Return-shape behavior:** methods are `Stream<...>` for streaming/procedure paths; estimate paths are `Stream<MemoryEstimateResult>` with single-element stream.
- **Result column influence:** some builders depend on `procedureReturnColumns.contains(...)`, e.g.:
  - `k1Coloring...` checks `colorCount`
  - `kmeans...`, `leiden...`, `louvain...` checks list-of-centroids / component-specific flags
  - `article-like` ones check `centralityDistribution` style flags in this file analogous to other facades.

## Memory And Storage Implications

- `CloseableResourceRegistry` is threaded in for stream procedures (`trianglesStream` in particular uses `TriasResultBuilderForStreamMode`).
- No direct allocation policy exists in this file; memory ownership is delegated to builders/facades.
- `MemoryEstimationResult` is produced only in estimate branches; no mutation-side memory sizing happens directly.
- Mutate stubs are pre-built once in `create(...)`, so rewrite should retain cheap facade construction and avoid heavy per-call object graphs.

## Dependencies Read Next

- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/community/CommunityProcedureFacade.java`
- `applications/algorithms/centrality` and `applications/algorithms/community` business facade classes.
- Result-builder classes used by community results:
  - `.../community/result/` (various local builder types referenced here)
- Config classes such as `KmeansStreamConfig`, `LouvainWriteConfig`, etc.
- `proc/algorithms-facade-api` generic interfaces referenced for parser/stub contracts.

## Dependents As Tests

- Algorithm-facing procedure tests that execute `gds.community.*` proc paths and assert stream/stats/write/estimate parity for parse and delegation consistency.
- Graph surface contract suites that verify defaulted result columns and unsupported config handling in community procedures.

## Verification Oracles

1. **WHEN** a `kmeansStream` call is made with valid config
   **THEN** **SHALL** parse with `KmeansStreamConfig::of`, instantiate a stream result builder, and delegate to `streamModeBusinessFacade.kMeans(...)`.
2. **WHEN** `k1ColoringStatsEstimate` is invoked
   **THEN** **SHALL** parse with `K1ColoringStatsConfig::of` and return `Stream.of(estimationModeBusinessFacade.k1Coloring(...))`.
3. **WHEN** `trianglesStream` is invoked with valid `TriangleCountBaseConfig`
   **THEN** **SHALL** build `TrianglesResultBuilderForStreamMode(closeableResourceRegistry)` and delegate to `streamModeBusinessFacade.triangles(...)`.
4. **WHEN** `sllpaWrite` path is called
   **THEN** **SHALL** parse `SpeakerListenerLPAConfig`, build write builder, and delegate to `writeModeBusinessFacade.sllpa(...)`.
5. **WHEN** `create(...)` is called twice with same registry/cols/parser
   **THEN** **SHALL** return a fresh facade containing independently constructed stub wiring (no shared mutable cross-call state leakage).

## Rust Rewrite Notes

- **L1:** model this as a Rust `CommunityProcedureFacade` trait with:
  - `create(...) -> impl CommunityProcedureFacade`
  - explicit associated context for each delegate mode.
- **L2:** isolate parser and builder concerns:
  - parser trait/service returns typed config for each algorithm
  - builders are pure functions from config + return-column flags.
- **L3:** keep method-level split by endpoint family; use enum-variant match for algorithms and mode.
- **L2/L3:** expose mutate stubs as separate fields/handles; keep estimates as single result stream objects to preserve caller contract.
- **Error behavior:** parse failures should preserve existing Java exception semantics as explicit parse/invalid-config errors with algorithm-scoped messages.

## Open Questions

- Which builders are required to be lazy-created per-call vs pre-built at construction for memory/performance in Rust?
- Are all builder-result columns deterministically computed from `ProcedureReturnColumns`, or does server context inject implicit defaults?
- Can estimate and write methods share a common typed error path, or should they preserve Java-specific estimation exception categories?

## Coding Prompt Unlocked

Implement `LocalCommunityProcedureFacade` equivalent in Rust:
- parse endpoint-specific config types,
- route each algorithm+mode request to the correct mode facade,
- ensure `*Estimate` methods return a deterministic single-item estimate stream,
- and expose all mutate stub getters with stable facade naming.

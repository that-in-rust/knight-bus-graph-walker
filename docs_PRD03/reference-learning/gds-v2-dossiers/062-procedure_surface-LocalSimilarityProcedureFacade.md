# 62 procedure_surface LocalSimilarityProcedureFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/similarity/LocalSimilarityProcedureFacade.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 62 |
| line_count | 523 |
| fan_in / fan_out | 1 / 48 |

## Why This File Matters

This façade is the procedure-surface entrypoint for similarity algorithms. It wires parse, mode-specific business facades, mutate stubs, and return-column gating for knn / node-similarity flows in stats/stream/write paths.

## Public Contract

- **Evidence:** The class is `final` and stores one `procedureReturnColumns` plus four business facades and four mutate stubs (`56-83`).
- **Evidence:** `create(...)` is the composition point that instantiates four mutate stubs (`102-142`).
- **Evidence:** Each public method follows a pattern:
  - parse config,
  - build mode-specific result builder,
  - delegate to `statsModeBusinessFacade` / `streamModeBusinessFacade` / `writeModeBusinessFacade` / `estimationModeBusinessFacade` (`149-523`).
- **Evidence:** Mutate stub getters (`filteredKnnMutateStub`, `knnMutateStub`, etc.) provide compatibility handles for mutation procedures.
- **Inference:** This class does not implement algorithm logic; it enforces method-level routing and result-shape configuration.

## Internal Mechanics

- **Evidence:** `procedureReturnColumns.contains("similarityDistribution")` gates whether result builders emit distribution columns in stats/write methods (`151-160`, `208-217`, `243-252`, `308-315`, `399-406`, `490-502`).
- **Evidence:** Three distinct config parsing flows are used:
  - direct `Config::of` methods (e.g., `FilteredKnnStatsConfig::of`, `KnnWriteConfig::of`) for lightweight config types,
  - `CypherMapWrapper` function wrapping `GraphSage*Config.of` for user-scoped embedding-specific parsing patterns (`240-243` etc.),
  - raw map parse via `configurationParser`.
- **Evidence:** All estimation methods return `Stream.of(result)` wrapping a single `MemoryEstimateResult` (`170-180`, `195-205`, etc.), matching existing result contract.
- **Inference:** The class is a large but regular delegator with strong consistency across algorithm families.

## Memory And Storage Implications

- **Evidence:** Memory footprint is mostly transient parse/build/delegate objects; no local caching of graphs or heavy state (`56-69`, `96-142`).
- **Inference:** Allocation pattern should remain thin, especially around repeated request handling.
- **Inference:** `procedureReturnColumns` read occurs per call, enabling cheap output-shape branching without global branch state.

## Snapshot And Catalog Implications

- **Evidence:** This file uses only parameter maps and procedure-level config parsing; catalog interactions are implicit through downstream business facades (`59-63`).
- **Inference:** In rewrite, this should remain procedure-layer glue with stable method signature count and delegate behavior.

## Verification Oracles

1. **WHEN** a similarity stats method is invoked, **THEN** configuration SHALL be parsed with the correct config type and results routed through the corresponding stats facade.
2. **WHEN** a call path checks `shouldComputeSimilarityDistribution`, **THEN** result builders SHALL receive this flag in methods that support it.
3. **WHEN** estimation methods are invoked, **THEN** they SHALL return singleton stream payloads via `Stream.of(...)`.
4. **WHEN** any stream or write estimate method is invoked, **THEN** it SHALL parse config first and pass both graph and config into correct business façade method.
5. **WHEN** mutate stub getter is requested (`knnMutateStub`, `filteredNodeSimilarityMutateStub`), **THEN** the specific prebuilt stub instance shall be returned.

## Rust Rewrite Notes

- **L1:** Model as a pure procedure façade module with no internal algorithm decisions.
- **L2:** Preserve one parse-delegate-return pattern per method for consistent compatibility and easier test mapping.
- **L2:** Keep return-column gating explicit (option + builder wiring).
- **L3:** Preserve the exact set/order of methods to reduce API surface drift in node-embedding/similarity compatibility tests.

## Dependencies Read Next

- `applications/algorithms/similarity/*.java` business facades
- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/similarity/SimilarityProcedureFacade.java`
- mutate stub implementations in `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/similarity/stubs/`

## Dependents As Tests

- Procedure-level matrix tests across:
  - `filteredKnnStats` + `filteredKnnStream` + `filteredKnnWrite` + estimate variants,
  - `knn` variants,
  - `nodeSimilarity` variants,
  - stub getter methods.
- Contract tests ensuring `shouldComputeSimilarityDistribution` toggles output columns consistently.

## Open Questions

- Should return-column checks be memoized per call or stay recalculated for each request?
- Should estimation methods retain singleton stream typing or return result vectors for simpler Rust ergonomics?
- Should user-based config wrapper (`CypherMapWrapper` path) be generalized into a shared parser strategy?

## Coding Prompt Unlocked

Implement `LocalSimilarityProcedureFacade` in Rust as a strict delegating façade:
1) constructor/factory wires four business facades + four mutate stubs,
2) each public method mirrors parse → builder → delegate → return stream pattern,
3) ensure return-column flags propagate only where contract requires them.

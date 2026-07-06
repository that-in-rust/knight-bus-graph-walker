# 088 olap_algorithm CommunityAlgorithmsStreamModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsStreamModeBusinessFacade.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 88 |
| line_count | 331 |
| fan_in / fan_out | 2 / 38 |

## Why This File Matters

This is the stream-mode façade for community algorithms, mapping many different community-related algorithm configs into one execution template.

## Public Contract

- Injected collaborators:
  - `CommunityAlgorithmsEstimationModeBusinessFacade`
  - `CommunityAlgorithms`
  - `AlgorithmProcessingTemplateConvenience`
- Stream API methods (all `public <RESULT> Stream<RESULT>`) include:
  - `approximateMaximumKCut`
  - `conductance`
  - `k1Coloring`
  - `kCore`
  - `kMeans`
  - `labelPropagation`
  - `lcc`
  - `leiden`
  - `louvain`
  - `modularity`
  - `modularityOptimization`
  - `scc`
  - `triangleCount`
  - `triangles`
  - `wcc`
  - `sllpa`
- Each method delegates to `processRegularAlgorithmInStreamMode(...)` with:
  - graph name
  - config
  - matching `AlgorithmLabel`
  - estimate function
  - algorithm execution lambda
  - stream result builder

## Internal Mechanics

- Uniform orchestration style across all methods with algorithm-specific type pairs:
  - config type
  - result type
  - result-builder type (`StreamResultBuilder`)
- `sllpa` returns `Stream<RESULT>` over `PregelResult`, showing cross-stack behavior through same template.
- No mutable per-method state; this is deterministic delegating dispatch logic.

## Memory and Storage Implications

- Stream mode avoids single-batch materialization but still requires graph and algorithm execution memory controls in lower layers.
- Minimal overhead at façade level except generic dispatch object allocation and streaming result wrapping.

## Snapshot And Catalog Implications

- This class indicates which algorithms support stream mode and therefore informs rewrite API surface decisions for procedure mode availability.
- Keep mode-specific output contracts consistent with catalog and procedure return columns.

## Verification Oracles

1. **WHEN** stream mode is requested for a supported config, **THEN** `processRegularAlgorithmInStreamMode` SHALL be invoked.
2. **WHEN** estimator and algorithm branch mismatch, **THEN** runtime behavior may expose wrong complexity assumptions.
3. **WHEN** `StreamResultBuilder` is supplied, **THEN** method SHALL return lazily consumed stream output.
4. **WHEN** `sllpa` config is used, **THEN** result type SHALL be `PregelResult`.

## Rust Rewrite Notes

- Treat this as macro-style or generated dispatch layer over config + algorithm label table.
- Keep exact stream API for parity with existing user-facing procedures.
- Preserve all result/result-builder associations from current mapping.

## Dependencies Read Next

- `AlgorithmLabel` enum values used by community algorithms
- `StreamResultBuilder`
- `CommunityAlgorithms` and estimation façade

## Dependents As Tests

- Parameterized tests iterating all stream methods and confirming template call shape.
- Sanity test for result builder type mapping on each method.

## Open Questions

- Could a table-driven dispatcher reduce duplication while preserving test traceability?
- Are all current community algorithms still valid for stream mode after migration to OLAP-only constraints?


# 089 olap_algorithm GraphSageHelper

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/main/java/org/neo4j/gds/embeddings/graphsage/GraphSageHelper.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 89 |
| line_count | 338 |
| fan_in / fan_out | 10 / 29 |

## Why This File Matters

This helper centralizes GraphSage-specific feature extraction, memory estimation, and layer configuration math used in train workflows.

## Public Contract

- Static methods for core GraphSage mechanics:
  - `embeddingsComputationGraph(...)`
  - `subGraphsPerLayer(...)`
  - `embeddingsEstimation(...)`
  - `initializeSingleLabelFeatures(...)`
  - `multiLabelFeatureExtractors(...)`
  - `initializeMultiLabelFeatures(...)`
  - `layerConfigs(...)`
- Private helpers:
  - `propertyKeysPerNodeLabel`
  - `filteredPropertyKeysPerNodeLabel`
  - `labelOf`
- `embeddingsEstimation` accepts:
  - `GraphSageTrainMemoryEstimateParameters`
  - batch size
  - node count
  - label count
  - gradient-descent inclusion flag

## Internal Mechanics

- `embeddingsComputationGraph` builds reverse-layer aggregation chain and applies `NormalizeRows`.
- `subGraphsPerLayer` creates one `NeighborhoodSampler` per layer and reverses them to match expected sampler order.
- `embeddingsEstimation` builds two memory estimation subtrees:
  - `subgraphs` size by layer
  - `forward` and optional `backward` aggregators
- Aggregator memory uses `MeanAggregatorMemoryEstimator` or `PoolAggregatorMemoryEstimator` depending on config.
- Multi-label mode introduces feature-function and label lookup overhead (`sizeOfObjectArray`).
- Initialization methods create and fill `HugeObjectArray<double[]>` for single and multi-label feature matrices.
- `layerConfigs` builds deterministic configuration records with random seeds and per-layer sample sizes.

## Memory and Storage Implications

- One of the more RAM-relevant helpers:
  - subgraph object construction
  - per-node feature arrays
  - aggregator matrices for forward/backward
- Memory estimation is explicit and range-based; this file controls both estimator shape and likely runtime buffer sizing assumptions.
- Label extraction is strict (`labelOf`) and throws when a node has != 1 label.

## Snapshot And Catalog Implications

- No direct catalog writes, but this file directly constrains feasible training shapes through memory estimation and layer config behavior.
- In Rust, keep estimate + execution helpers colocated to avoid divergence.

## Verification Oracles

1. **WHEN** multi-label mode is active, **THEN** memory estimation SHALL add label-related auxiliary allocations.
2. **WHEN** `embeddingsComputationGraph` is called, **THEN** layers SHALL be consumed in reverse order through their aggregators.
3. **WHEN** node has multiple labels, **THEN** `labelOf` SHALL throw an argument error.
4. **WHEN** `initializeSingleLabelFeatures`/`initializeMultiLabelFeatures` are called, **THEN** arrays SHALL be pre-sized to `graph.nodeCount()`.

## Rust Rewrite Notes

- Keep estimator and execution helpers strongly correlated in one module.
- Encode layer-specific memory formula as typed structs to avoid accidental estimate drift.
- Keep label strictness behavior explicit to prevent silent multi-label mis-handling.

## Dependencies Read Next

- `MeanAggregatorMemoryEstimator`, `PoolAggregatorMemoryEstimator`
- `Layer`, `LayerConfig`, `GraphSageTrainMemoryEstimateParameters`
- `FeatureExtraction`, `MultiLabelFeatureExtractors`, `SubGraph`, `NeighborhoodSampler`

## Dependents As Tests

- Deterministic tests for `layerConfigs` and `embeddingsEstimation` under varying batch sizes.
- Label enforcement test for 0/1/>1 labels.
- Round-trip memory estimation tests for with/without gradient descent and single/multi-label modes.

## Open Questions

- Should layer-level random seeds be exposed for deterministic replay in Rust by design?
- Can forward/backward memory builders be unified without losing clarity for instrumentation?


# 072 olap_algorithm NodeEmbeddingAlgorithms

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/node-embeddings/src/main/java/org/neo4j/gds/applications/algorithms/embeddings/NodeEmbeddingAlgorithms.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 72 |
| line_count | 273 |
| fan_in / fan_out | 9 / 37 |

## Why This File Matters

Central execution coordinator for node-embedding algorithms.

## Public Contract

- Holds shared dependencies (`AlgorithmMachinery`, `GraphSageModelCatalog`, `ProgressTrackerCreator`, `TerminationFlag`) (`62-69`).
- Exposes entrypoints for FastRP, GraphSage, GraphSage training, HashGNN, and Node2Vec.
- FastRP path: `createFastRPTask(...)` + `runAlgorithmsAndManageProgressTracker(...)` (`80-109`, `242-253`).
- GraphSage path resolves model from catalog and runs algorithm through shared machinery (`111-134`).
- GraphSage training uses `GraphSageModelTrainer.progressTasks(...)` and a constructor dispatch (`136-166`, `168-197`).
- HashGNN and Node2Vec build dedicated task shapes and share machinery lifecycle (`199-240`, `242-270`).

## Internal Mechanics

- `constructGraphSageTrainAlgorithm(...)` is a strict branch point for multi-label vs single-label train implementations (`177-196`).
- Node2Vec adds `DegreeCentralityTask` when relationship properties are present (`258-259`).

## Memory and Storage Implications

- Heavy memory behavior is mainly inside algorithm implementations and feature extraction, while this class orchestrates shared lifecycles.
- Progress tracker + task metadata allocation is per-run and likely high-frequency; reuse patterns should be preserved.

## Snapshot And Catalog Implications

- `GraphSageModelCatalog` integration makes model lifecycle part of execution semantics (`65-66`, `115-116`).
- Shared termination path across modes (`68`, `100`, `125`, `157`, `194`, `231`).

## Verification Oracles

1. **WHEN** multi-label config is set, **THEN** `MultiLabelGraphSageTrain` SHALL be created.
2. **WHEN** single-label config is set, **THEN** `SingleLabelGraphSageTrain` SHALL be created.
3. **WHEN** FastRP `selfInfluence` is zero, optional task block SHALL be skipped.
4. **WHEN** Node2Vec graph has relationship properties, **THEN** degree task SHALL be added before random-walk tasks.

## Rust Rewrite Notes

- Preserve mode-specific helper methods (`fastRP`, `graphSage`, `graphSageTrain`, `hashGnn`, `node2Vec`).
- Keep progress construction in helper functions for transparent observability.
- Keep a single `AlgorithmMachinery`-style execution facade and one `ProgressTrackerCreator` path.

## Dependencies Read Next

- `AlgorithmMachinery`, `ProgressTrackerCreator`
- `GraphSageModelCatalog`
- `GraphSageModelTrainer`, `GraphSage`, `HashGNN`, `Node2Vec`

## Dependents As Tests

- Per-mode tests for task selection and branch behavior.
- Termination and cancellation coverage in long-running training.

## Open Questions

- Could progress-task labels be shared across node-embedding modes without losing compatibility?

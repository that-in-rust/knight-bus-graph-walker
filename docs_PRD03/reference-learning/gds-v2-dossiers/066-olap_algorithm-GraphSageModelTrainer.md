# 066 olap_algorithm GraphSageModelTrainer

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/main/java/org/neo4j/gds/embeddings/graphsage/GraphSageModelTrainer.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 66 |
| line_count | 405 |
| fan_in / fan_out | 19 / 29 |

## Why This File Matters

This is the hot execution loop for GraphSage training. It orchestrates batching, forward/backward passes, convergence checks, and training metrics.

## Public Contract

- **Evidence:** Constructors accept `GraphSageTrainParameters`, executor, `ProgressTracker`, `TerminationFlag`, optional feature function, and initial projection weights (`72-96`).
- **Evidence:** Static `progressTasks(...)` declares the task shape: `Prepare batches` then iterative `Train model -> Epoch -> Iteration` (`98-110`).
- **Evidence:** `train(graph, features)` returns `ModelTrainResult` containing iteration loss history, convergence flag, and layer array (`113-189`).
- **Evidence:** Convergence uses `Math.abs(prevLoss - avgLossPerNode) < parameters.tolerance()` and can terminate early (`276-279`).
- **Evidence:** Metrics are surfaced in immutable value objects (`GraphSageTrainMetrics`, `ModelTrainResult`) with derived fields and map serialization (`344-404`).

## Internal Mechanics

- **Evidence:** Batching is precomputed from `BatchSampler.extendedBatches(...)` using `batchSize`, `searchDepth`, and `randomSeed` (`121-124`).
- **Evidence:** Adaptive batching strategy: when `batchesPerIteration * maxIterations > extendedBatches.size()` it caches batch tasks per epoch; otherwise it computes lazily (`137-176`).
- **Evidence:** Each epoch delegates to `trainEpoch`, which builds sampled tasks, runs them via `RunWithConcurrency`, aggregates losses, updates gradients with `AdamOptimizer`, and tracks iterations (`247-295`).
- **Evidence:** Individual tasks are `BatchTask implements Runnable` that builds local computation context, computes forward+backward, stores gradients, and reports progress (`305-333`).
- **Blocked:** The task graph and tensor types are deep ML-core constructs; exact numeric behavior in helper classes (`GraphSageLayer`, losses) needs additional read files for fidelity.

## Memory and Storage Implications

- **Inference:** Main memory pressure sits in `weights` collection and `features`/subgraph construction for sampled batches (`114-117`, `205-213`, `267-291`).
- **Inference:** `localGraph = graph.concurrentCopy()` per batch creates duplicate graph views for each sampled task (`203`), relevant for RAM budgeting and concurrency tuning.
- **Inference:** Creating `l2penalty` tensors per batch when `penaltyL2 > 0` adds temporary allocations (`224-239`).
- **Evidence:** Metrics serialization uses compact lists/maps (`350-383`) and should remain lightweight.

## Snapshot And Catalog Implications

- **Inference:** This class mutates model weights in-place via optimizer updates, but does not touch graph-store catalog. State transitions are algorithm-internal only.
- **Inference:** `model train result` is the handoff artifact for higher layers (result builders, stats/logging).

## Verification Oracles

1. **WHEN** `train(...)` starts, **THEN** batch preparation and training subtasks are sequenced exactly (`Prepare batches` then `Train model`).
2. **WHEN** `createBatchTasksEagerly` is true, **THEN** per-epoch cached task list MUST be used.
3. **WHEN** tolerance threshold is met, **THEN** training SHALL set `converged=true` and stop the epoch loop early.
4. **WHEN** `terminationFlag.assertRunning()` is called in epoch/iteration, **THEN** termination should abort before launching extra iterations.
5. **WHEN** results are returned, **THEN** `GraphSageTrainResult.metrics.iterationLossPerEpoch` MUST contain monotonic run-history for observability.

## Rust Rewrite Notes

- **L1:** Represent immutable config and mutable trainer state with explicit ownership:
  - `GraphSageTrainer { parameters, feature_fn, layers, weights, executor, termination_flag, progress_tracker }`.
- **L1:** Preserve dual task path (eager vs lazy batch task generation) based on estimated repetition ratio.
- **L2:** Implement `EpochResult` and `GraphSageTrainMetrics` as compact serializable domain structs.
- **L2:** Keep per-iteration loss stream and early-stop semantics as first-class behavior, not just logging.
- **L3:** Keep `BatchTask` side-effect-free on shared state except controlled gradient output to avoid data races.

## Dependencies Read Next

- `org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainParameters`
- `org.neo4j.gds.ml.core.optimizer.AdamOptimizer`
- `org.neo4j.gds.ml.core.subgraph.GraphSageHelper` / `BatchSampler`
- `org.neo4j.gds.embeddings.graphsage.GraphSageLoss`
- `org.neo4j.gds.embeddings.graphsage.GraphSageLayer` and aggregator implementations

## Dependents As Tests

- Unit tests for task-shape contract:
  - verify sequence and names in `progressTasks`.
- Deterministic unit test for convergence stop condition with synthetic tensors and fixed tolerance.
- Concurrency test using fake executor to ensure `RunWithConcurrency` receives expected task count.
- Snapshot tests for metrics `toMap()` fields and names.

## Open Questions

- Should local graph copying happen per task in the Rust version, or can tasks borrow an immutable graph token safely?
- Can we stream aggregate losses instead of full history to reduce memory for very long epochs?
- Is there an established timeout policy layered on top of `TerminationFlag` that should be preserved as error types?


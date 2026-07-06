# 53 olap_algorithm ProgressTrackerCreator

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ProgressTrackerCreator.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 53 |
| line_count | 63 |
| fan_in / fan_out | 46 / 7 |

## Why This File Matters

This class is the central progress-tracker factory for algorithm execution. It centralizes the one decision in Java code: whether to use a verbose `TaskProgressTracker` or lightweight `TaskTreeProgressTracker` based on algorithm config.

## Public Contract

- **Evidence:** Constructor requires `Log` and `RequestScopedDependencies` and stores both as final dependencies (`33–39`).
- **Evidence:** Public method `createProgressTracker(AlgoBaseConfig, Task)` returns either `TaskProgressTracker` when `configuration.logProgress()` is true or `TaskTreeProgressTracker` otherwise (`41–60`).
- **Evidence:** Both branches pass the same shared collaborators: `task`, `log`, concurrency, job id, task registry factory, and user log registry factory (`43–60`).
- **Inference:** Configuration’s `logProgress`, `concurrency`, and `jobId` are the policy surface for tracker selection and execution telemetry.

## Internal Mechanics

- **Evidence:** This is a tiny stateless façade with no mutable fields except injected dependencies (`32–39`).
- **Evidence:** Branching is binary and explicit in one method; there is no plugin, queue, or deferred factory indirection (`41–60`).
- **Evidence:** `TaskProgressTracker` is chosen when progress logging is enabled to provide explicit progress reporting (`42–47`).
- **Evidence:** `TaskTreeProgressTracker` is chosen otherwise for non-logged execution paths (`52–60`).
- **Inference:** This object can be treated as a stable seam to isolate all progress policy from algorithm façade implementations.

## Memory And Storage Implications

- **Evidence:** Allocation of exactly one `ProgressTracker` instance per algorithm invocation happens in one branch (`43–60`).
- **Inference:** The major RAM impact is from chosen tracker class behavior, not from this factory’s own logic.
- **Inference:** `TaskProgressTracker` can be heavier than `TaskTreeProgressTracker` due to richer logging; this may matter when hot loops execute many short-lived jobs.
- **Blocked:** Exact per-path allocation size is in tracker constructors and cannot be proven from this file alone.

## Snapshot And Catalog Implications

- **Inference:** Snapshot/caching logic is external; this factory only receives `Task` and `AlgoBaseConfig` and returns an execution tracker.
- **Evidence:** No catalog reads/writes (`createProgressTracker` has only config + task + injected factories).
- **Inference:** This file likely sits below catalog-backed execution orchestration and should remain stable if procedure surface changes.

## Verification Oracles

1. **WHEN** `configuration.logProgress()` is true, **THEN** `createProgressTracker(...)` SHALL return `TaskProgressTracker` configured with `configuration.concurrency()` and `configuration.jobId()` (`41–50`).
2. **WHEN** `configuration.logProgress()` is false, **THEN** `createProgressTracker(...)` SHALL return `TaskTreeProgressTracker` with same concurrency/job factory context (`52–60`).
3. **WHEN** called repeatedly with same config shape, **THEN** method SHALL not retain hidden mutable state in the creator instance.

## Rust Rewrite Notes

- **L1:** Introduce trait-driven seam (`ProgressTrackerFactory`) that owns `log` and request-scoped registries.
- **L2:** Make selection explicit: `create_progress_tracker(configuration, task, task_id)` returns enum-like tracker with one allocation per call.
- **L3:** Preserve constructor dependency boundary (`log + registries`) as stateful struct fields and keep method side-effect free.

## Dependencies Read Next

- `applications/algorithms/machinery/RequestScopedDependencies.java`
- `config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java`
- `core-utils/progress` tracker classes and execution templates that call this factory

## Dependents As Tests

- Algorithm façade tests that assert tracker type/metadata for logging-enabled and disabled runs.
- Progress instrumentation tests that assert task/job registration and cancellation behavior under both tracker branches.

## Open Questions

- Should Rust make factory dispatch explicit in config parsing (return enum) or keep imperative branch here?
- Should `TaskProgressTracker` creation trigger a structured metric (`logProgress=true`) for all runs or remain per-call config only?

## Coding Prompt Unlocked

Implement `ProgressTrackerCreator` in Rust as a small factory module:
1) inject logger and request-context factories, 2) choose tracker impl by `logProgress`, 3) preserve concurrency/job metadata plumbing, and 4) add tests that assert tracker constructor selection and registry wiring.

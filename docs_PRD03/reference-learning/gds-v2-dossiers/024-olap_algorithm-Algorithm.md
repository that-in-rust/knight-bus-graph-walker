# 24 olap_algorithm Algorithm

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo-common/src/main/java/org/neo4j/gds/Algorithm.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 24 |
| line_count | 47 |
| fan_in / fan_out | 100 / 2 |
| purpose | Capture algorithm execution skeleton: config, estimation, execution modes, result/write/mutate/stream artifacts. |
| read_prompt | Read this entire file as an algorithm execution contract. Extract lifecycle stages, required graph views, algorithm state, result artifacts, memory estimation hooks, mutate/write/stream behavior, and verification cases. |

## Why This File Matters

- This is the common base for algorithm implementations and therefore one of the highest leverage compatibility seams.
- It defines the lifecycle shape (`compute`) and cross-cutting execution telemetry (`ProgressTracker`, termination flag).
- Any Rust port of algorithm infrastructure should preserve the same state model to avoid cancellation and progress regressions.

## Public Contract

- Generic abstract class: `public abstract class Algorithm<RESULT>`.
- Fields:
  - `protected final ProgressTracker progressTracker`
  - `protected TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE`
- Required method:
  - `public abstract RESULT compute();`
- Lifecycle APIs:
  - `setTerminationFlag(TerminationFlag)`
  - `getTerminationFlag()`
  - `getProgressTracker()`

## Internal Mechanics

- `progressTracker` is constructor-injected and immutable (`final`).
- `terminationFlag` is mutable and starts in `RUNNING_TRUE`.
- Subclasses are forced to implement `compute()` as the algorithm entrypoint.
- No default cancellation logic exists in this base type; it only stores and exposes execution control state.

## Storage / Runtime Implications

- Lightweight class with two fields; no container allocations beyond standard object header.
- Rust equivalent should avoid lock-based overhead for the termination flag unless cancellation concurrency requires it.
- `ProgressTracker` object references should remain immutable for the algorithm instance lifetime to mirror Java constructor contract.

## Failure / Incompatibility Surfaces

- If subclasses return nulls or ignore compute contract, those are subclass-level failures not enforced here.
- If `getProgressTracker()` consumers expect non-null, base class relies on constructor providing non-null input.
- There is no hard override for cancellation: termination behavior depends on consumers and subclass checks.

## Verification Oracles

1. **WHEN** an algorithm subclass is instantiated with a `ProgressTracker`  
   **THEN** **SHALL** expose the same object via `getProgressTracker()`.
2. **WHEN** no one sets the termination flag  
   **THEN** **SHALL** remain at `TerminationFlag.RUNNING_TRUE`.
3. **WHEN** `setTerminationFlag` is called  
   **THEN** **SHALL** update value returned by `getTerminationFlag()`.
4. **WHEN** `compute()` is invoked through base-class reference  
   **THEN** **SHALL** dispatch to subclass implementation.

## Rust Rewrite Notes

- **L1:** define abstract base trait/struct with required method `compute()`.
- **L2:** use owned `termination_flag` field with atomic or regular enum storage depending on thread model.
- **L3:** preserve `progress_tracker` immutably and pass into compute pathways.
- Keep lifecycle minimal and opinionated: this class should not own scheduling, only contract + state access.

## Dependencies Read Next

- `org.neo4j.gds.termination.TerminationFlag`
- `org.neo4j.gds.core.utils.progress.tasks.ProgressTracker`
- Subclasses in `algo-common` and all algorithm folders implementing `Algorithm<...>`.

## Dependents As Tests

- Algorithm interface contract tests validating progress/termination exposure.
- Cancellation tests (if subclass checks flag) and progress tracking integration.

## Open Questions

- Should Rust use `AtomicTerminationFlag` for all algorithms or keep flag purely local to each algorithm thread?
- Should `setTerminationFlag` be infallible or return error when called post-start in Rust safety model?

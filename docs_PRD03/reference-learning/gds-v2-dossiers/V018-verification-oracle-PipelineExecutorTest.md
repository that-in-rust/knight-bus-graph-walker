# 1018 verification_oracle PipelineExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/test/java/org/neo4j/gds/ml/pipeline/PipelineExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 328 |
| fan_in / fan_out | 0 / 30 |
| seed anchor | config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java |

## Why This File Matters

This is the lifecycle oracle for ML pipeline execution: validate before execution, run intermediate node-property steps, execute the pipeline, always clean graph-store mutations, and emit stable progress logs.

## Public Contract

- `PipelineExecutor.compute()` begins progress, generates dataset filters, validates feature input, creates `NodePropertyStepExecutor`, validates node-step context, splits datasets, executes node-property steps, validates feature properties, executes the pipeline, and finally cleans intermediate properties plus subclass cleanup.
- Config must satisfy both `AlgoBaseConfig` and `GraphNameConfig`.
- `AlgoBaseConfig` owns core defaults and graph-store validation semantics.
- Intermediate properties are stored in the graph store and removed after success or failure.

## Fixture Graph Shape

- Valid fixture: exactly one node `(n:N)` with label `N`.
- Invalid fixture: `(), ()`, two unlabeled nodes with no feature property `a`.
- `PipelineExecutorTestConfig` returns graph name `test`, node label `N`, and no username override.
- `SucceedingPipelineExecutor` returns dataset split `FEATURE_INPUT` with node label `N`.
- `FailingPipelineExecutor` throws from `execute(...)`.
- `BogusNodePropertyPipeline` adds one node-property step.
- `FailingNodePropertyPipeline` adds a successful step and then a failing step.

## Public Contract Evidence

- `shouldCleanGraphStoreWhenComputationIsComplete` (`68`) asserts success cleanup.
- `shouldCleanGraphStoreOnFailureWhenExecuting` (`81`) asserts execute-failure cleanup.
- `shouldHaveCorrectProgressLoggingOnSuccessfulComputation` (`94`) asserts exact success logs.
- `shouldCleanGraphStoreWhenNodePropertyStepIsFailing` (`119`) asserts node-step-failure cleanup.
- `shouldHaveCorrectProgressLoggingOnFailure` (`133`) asserts failure logs omit root finish.
- `failOnInvalidGraphBeforeExecution` (`157`) asserts validation happens before execution.

## Asserted Outputs And Errors

- Successful computation does not throw and removes `someBogusProperty`.
- If pipeline `execute(...)` throws, the original `PipelineExecutionTestFailure` propagates and intermediate properties are removed.
- If a later node-property step throws, both the already-created property and the failed step's declared property are absent afterward.
- Successful progress logs exactly include root start, node-property-step start/progress/finish, node-step container finish, and root finish.
- Failure after node-property steps logs node-step completion but not root executor `Finished`.
- Missing feature property `a` fails before execution with a message containing `Missing node properties for the following node labels`.

## Memory And Storage Implications

- No memory estimate is asserted here.
- Storage impact is graph-store mutation safety: temporary node properties added by node-property steps must be removed on all paths.
- Cleanup removes step `mutateNodeProperty` names from the graph store.
- A Rust rewrite should use a cleanup guard so errors do not leave temporary algorithm features attached to graph snapshots.

## Snapshot And Catalog Implications

- `PipelineExecutor` captures `schemaBeforeSteps` from selected labels/types before mutating the graph.
- No model catalog is used in this test.
- Progress logs are part of the public lifecycle surface.
- Cleanup behavior is graph-store local, not procedure catalog global.

## Verification Oracles

1. **WHEN** a node-property step succeeds and pipeline execution succeeds, **THEN** `compute()` SHALL not throw and SHALL remove `someBogusProperty`.
2. **WHEN** pipeline `execute(...)` throws after node-property steps, **THEN** the original exception SHALL propagate and intermediate properties SHALL be removed.
3. **WHEN** a later node-property step throws, **THEN** already-created and declared failed-step properties SHALL both be absent.
4. **WHEN** success progress is logged, **THEN** messages SHALL exactly match the start, node-step, and finish sequence asserted by the test.
5. **WHEN** execution fails after node-property steps, **THEN** progress SHALL include node-step completion but SHALL NOT include root executor `Finished`.
6. **WHEN** feature steps reference missing property `a`, **THEN** `compute()` SHALL fail before execution with the missing-property message.

## Rust Rewrite Notes

- Use an RAII/defer cleanup guard around intermediate graph properties so success and error paths both clean up.
- Preserve propagation of the original execution error.
- Preserve progress nesting semantics, including no root `Finished` on execute failure if matching Java behavior.
- Capture `schema_before_steps` before mutation.
- Decide whether cleanup errors can mask original execution errors or should be joined/suppressed.

## Dependencies Read Next

- `PipelineExecutor`
- `NodePropertyStepExecutor`
- `ExecutableNodePropertyStep`
- `TrainingPipeline`
- `PipelineGraphFilter`
- `LinkPredictionPredictPipeline`
- `L2FeatureStep`
- `GraphStore.addNodeProperty`
- `GraphStore.removeNodeProperty`
- `ProgressTracker`

## Open Questions

- The progress root is named `FailingPipelineExecutor` even in the successful test; should Rust preserve class-derived task names or use operation names?
- Cleanup removes by property name globally, not visibly label-scoped; should the rewrite constrain cleanup to labels touched by the pipeline?
- Java `finally` cleanup errors could mask original exceptions; Rust should make this behavior explicit.

## Coding Prompt Unlocked

Build Rust pipeline executor lifecycle tests around validation-before-execution, temporary node-property cleanup, original error propagation, schema snapshotting, and progress log parity.

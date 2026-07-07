# 1022 verification_oracle PredictPipelineExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pipeline/src/test/java/org/neo4j/gds/ml/pipeline/PredictPipelineExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 306 |
| fan_in / fan_out | 0 / 27 |
| seed anchor | config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java |

## Why This File Matters

This locks the abstract predict-pipeline executor lifecycle: validate before mutation, execute node-property steps, run prediction, and always clean temporary properties through `finally`.

## Public Contract

- `PredictPipelineExecutor.compute()` validates feature input before executing prediction.
- It executes node-property steps when a pipeline needs temporary features.
- It runs prediction-specific `execute(...)`.
- It cleans temporary node properties on success, executor failure, and node-step failure.
- Test config implements both `AlgoBaseConfig` and `GraphNameConfig`.
- `AlgoBaseConfig` supplies wildcard relationship/node selector defaults, selector resolution, graph-store validation hooks, and inherited concurrency defaults.

## Fixture Graph Shape

- Primary GDL graph: one `N` node, no relationships and no properties.
- Invalid graph: two isolated unlabeled/propertyless nodes.
- `NodeIdPropertyStep` adds a long node-id property to all graph labels.
- `FailingNodePropertyStep` declares `failingStepProperty` and throws before adding it.
- Invalid link-prediction case creates `L2FeatureStep(["a"])`; the graph and pipeline do not provide property `a`.

## Public Contract Evidence

- `shouldCleanGraphStoreWhenComputationIsComplete` (`65`) asserts success cleanup.
- `shouldCleanGraphStoreOnFailureWhenExecuting` (`78`) asserts executor-failure cleanup.
- `shouldHaveCorrectProgressLoggingOnSuccessfulComputation` (`91`) asserts exact progress logs.
- `shouldCleanGraphStoreWhenNodePropertyStepIsFailing` (`116`) asserts node-step-failure cleanup.
- `shouldHaveCorrectProgressLoggingOnFailure` (`130`) asserts failure progress omits outer finish.
- `failOnInvalidGraphBeforeExecution` (`154`) asserts missing feature-property validation.
- `taskTree` (`174`) defines the progress task root.
- `SucceedingPipelineExecutor.execute` (`217`) returns a string result.
- `FailingPipelineExecutor.execute` (`243`) throws `PipelineExecutionTestFailure`.

## Asserted Outputs And Errors

- Success path: no exception and `hasNodeProperty(N, "someBogusProperty") == false`.
- Executor failure: exact `PipelineExecutionTestFailure` propagates and temporary property is absent.
- Node-property-step failure: exact node-step failure propagates; both `someBogusProperty` and `failingStepProperty` are absent afterward.
- Success progress logs include start, node-step container start, `AddBogusNodePropertyStep` start/100%/finished, node-step container finish, and outer finish.
- Failure progress logs include node-step completion but omit outer `Finished`.
- Invalid graph error contains `Node properties [a] defined in the feature steps do not exist in the graph or part of the pipeline`.

## Memory And Storage Implications

- Temporary node properties are in-memory `GraphStore` mutations.
- Cleanup removes every step's `mutateNodeProperty` key globally, including keys for failed steps.
- Memory estimation for node-property steps uses maximum estimation and explicitly does not account for feature-dataset cleanup.
- Runtime cleanup and estimator assumptions should remain separate in a rewrite.

## Snapshot And Catalog Implications

- No `GraphStoreCatalog` mutation occurs.
- The injected GDL graph's property set returns to baseline after success, executor failure, or step failure.
- Invalid feature-property validation occurs before node-property execution, preventing partial mutation.
- Progress-log message order is part of the compatibility surface.

## Verification Oracles

1. **WHEN** predict compute succeeds after a node-property step, **THEN** the temporary mutate property SHALL be absent afterward.
2. **WHEN** executor `execute()` throws after node-property steps, **THEN** the original exception class SHALL propagate and temporary properties SHALL be cleaned.
3. **WHEN** a later node-property step throws, **THEN** prior successful mutate properties and the failing step's declared mutate key SHALL both be absent afterward.
4. **WHEN** progress is tracked on success, **THEN** messages SHALL match the exact seven-message sequence ending in outer `Finished`.
5. **WHEN** executor failure occurs after step execution, **THEN** progress SHALL include step completion but SHALL NOT include outer `Finished`.
6. **WHEN** a feature step references missing property `a` and no node-property step produces it, **THEN** compute SHALL fail before prediction with the documented missing-property message.

## Rust Rewrite Notes

- Use an RAII cleanup guard around intermediate properties.
- Make `remove_node_property` idempotent and decide whether cleanup errors can mask compute errors.
- Represent config inheritance as traits: graph name, username, concurrency, selectors, validation hooks.
- Preserve progress message order as a compatibility surface.
- Model unused feature steps as empty vectors rather than Java-style `null`.

## Dependencies Read Next

- `PredictPipelineExecutor`
- `NodePropertyStepExecutor`
- `Pipeline`
- `ExecutableNodePropertyStep`
- `PipelineGraphFilter`
- `ExecutableNodePropertyStepTestUtil`
- `GdlExtension`
- `GdlFactory`
- `LinkPredictionPredictPipeline`
- `L2FeatureStep`
- `AlgoBaseConfig`
- `ElementTypeValidator`
- `ConcurrencyConfig`

## Open Questions

- Can cleanup of a temporary property accidentally delete a pre-existing user property with the same key?
- Should cleanup failure preserve the original execution exception?
- Is missing outer `Finished` on executor failure intentional API behavior or current progress-tracker behavior?

## Coding Prompt Unlocked

Build Rust predict-pipeline executor tests around validation-before-mutation, temporary node-property cleanup, original exception propagation, exact progress logs, and feature-property validation.

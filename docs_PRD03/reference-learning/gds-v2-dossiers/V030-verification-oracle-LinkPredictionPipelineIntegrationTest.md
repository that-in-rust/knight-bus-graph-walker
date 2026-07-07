# V030 Verification Oracle: LinkPredictionPipelineIntegrationTest

Source: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-gds-src/proc/machine-learning/src/test/java/org/neo4j/gds/ml/linkmodels/pipeline/LinkPredictionPipelineIntegrationTest.java`

Seed: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-gds-src/graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java`

## Why This File Matters

`LinkPredictionPipelineIntegrationTest` is the procedure-level integration oracle for link-prediction pipelines across projection, pipeline creation, split config, node-property mutate steps, feature steps, trainer selection, model training, mutate prediction, filtered multi-label prediction, and GraphSage feature integration.

For the Rust rewrite, this is a useful end-to-end slice because it shows how public procedure calls compose into durable pipeline state and graph mutation side effects.

## Public Contract

- Setup registers graph projection, GraphSage train/mutate, model list, link prediction train/stream/mutate/create/add-step/add-trainer/configure-split procedures.
- Projection `g` uses label `N`, undirected `REL`, properties `noise`, `z`, `array`, and `DefaultValue.DEFAULT`.
- Projection `g_multi` uses labels `N`, `X`, `Z`, undirected `REL_2`, natural `CONTEXT`, the same properties, and the same default.
- `PipelineCatalog.removeAll()` is the explicit teardown cleanup.
- Link prediction training exposes model info with `modelType = "LinkPrediction"`.
- Mutate prediction writes predicted relationships into the in-memory `GraphStore`.

## Fixture Graph Shape

- Nodes include one `Ignored`, sixteen `:N` nodes `a..p`, eight `:X` nodes `x1..x8`, and three `:Z` context nodes.
- `:N` nodes carry `noise`, `z`, and 5D or 1D `array`.
- `:Z` nodes have no listed feature properties, exercising `DefaultValue.DEFAULT`.
- Relationships include `REL` among `:N` nodes, one `IGNORED`, `REL_2` from `:N` to `:X`, and `CONTEXT` through `:Z`.

## Important Assertions

- `trainAndPredictLR` creates `pipe`, configures two validation folds, adds PageRank property `pr`, adds a COSINE feature, adds two logistic-regression trainers, trains `trainedModel`, then mutates top four predictions.
- `trainAndPredictRF` uses RandomForest with `numberOfDecisionTrees = 2`, `maxDepth = 5`, and `minSplitSize = 2`.
- `trainAndPredictFiltered` trains on `g_multi` with target type `REL_2`, source label `N`, target label `X`, PageRank context label `Z`, context relationship `CONTEXT`, and prediction restricted to `relationshipTypes: ['REL_2']`.
- `runWithGraphSage` projects `g_2`, trains GraphSage model `exampleTrainModel`, adds `beta.graphSage` node property `embedding`, trains a link prediction model, and streams `topN: 2`.

## Asserted Outputs And Errors

- All train calls return `modelInfo.modelType = "LinkPrediction"`.
- Mutate predictions assert `preProcessingMillis`, `computeMillis`, and `mutateMillis` are greater than `-1`.
- Mutate predictions assert `postProcessingMillis = 0`.
- For an undirected graph and `topN = 4`, `relationshipsWritten = 2 * topN = 8`.
- `configuration`, `samplingStats`, and `probabilityDistribution` are maps.
- Mutated graph stores must have relationship type `PREDICTED` with property `probability`.
- GraphSage stream prediction returns exactly two rows for `topN: 2`.

## Memory And Storage Implications

Graph projection stores two named in-memory graph stores before each test. `predict.mutate` writes predicted relationships into the in-memory `GraphStore`, not into the Neo4j database.

Undirected mutate doubles `topN`, so `topN = 4` yields eight written relationships. `DefaultValue.DEFAULT` is a null fallback wrapper, with typed scalar/array conversion behavior delegated to `DefaultValue`.

## Snapshot And Catalog Implications

The test obtains `graphStore` and `multiGraphStore` from `GraphStoreCatalog` immediately after projection. Pipeline catalog cleanup is explicit, but the file does not explicitly remove graph catalog entries.

User context is fixed to `"myUser"` for GraphSage and pipeline calls. That implies model ownership and catalog visibility should be treated as part of the integration contract, even if this file mostly tests the happy path.

## Verification Oracles

1. WHEN the pipeline trains with logistic regression on graph `g`, THEN `modelInfo.modelType` SHALL equal `LinkPrediction`.
2. WHEN mutate prediction runs with `topN = 4` on an undirected graph, THEN `relationshipsWritten` SHALL equal eight.
3. WHEN mutate prediction completes, THEN `PREDICTED.probability` SHALL exist in the relevant in-memory graph store.
4. WHEN RandomForest is the trainer, THEN the same mutate output contract SHALL hold: non-negative timings, zero post-processing, and maps for config, sampling, and probability distribution.
5. WHEN training on `g_multi` uses source label `N`, target label `X`, target type `REL_2`, and context `Z`/`CONTEXT`, THEN prediction SHALL mutate `g_multi` with `PREDICTED.probability`.
6. WHEN GraphSage embeddings are added as link-prediction features and streamed with `topN = 2`, THEN exactly two prediction rows SHALL be returned.

## Rust Rewrite Notes

Represent pipeline state explicitly:

- split config
- node-property steps
- feature steps
- trainer configs
- trained model
- prediction mode

Preserve seeded randomness, especially `1337` in train calls. Keep mutate output as a structured record with timings, written relationship count, sampling stats, probability distribution, and graph mutation side effects.

Model default feature values using a typed Rust equivalent of `DefaultValue`, especially for missing properties on `Z` nodes and mixed array lengths.

## Dependencies To Read Next

- `LinkPredictionPipelineTrainProc`
- `LinkPredictionPipelineMutateProc`
- `LinkPredictionPipelineStreamProc`
- `LinkPredictionPipelineCreateProc`
- `LinkPredictionPipelineAddStepProcs`
- `LinkPredictionPipelineAddTrainerMethodProcs`
- `LinkPredictionPipelineConfigureSplitProc`
- `PipelineCatalog`
- `GraphSageTrainProc`
- `GraphSageMutateProc`
- `GraphStoreCatalog`
- `DefaultValue`

## Open Questions

- Should Rust tests assert exact sampling/probability distribution contents rather than only map shape?
- Is graph catalog cleanup handled by the base test harness, or should Rust mirror explicit RAII cleanup for graph stores as well as pipelines?
- Should the GraphSage path assert model ownership/user isolation beyond the hard-coded `"myUser"`?

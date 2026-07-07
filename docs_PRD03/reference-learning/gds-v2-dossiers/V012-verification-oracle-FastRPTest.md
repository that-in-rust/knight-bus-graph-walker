# 1012 verification_oracle FastRPTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/test/java/org/neo4j/gds/embeddings/fastrp/FastRPTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 831 |
| fan_in / fan_out | 0 / 35 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java |

## Why This File Matters

This is the algorithmic oracle for FastRP embedding math: random/property vector initialization, propagation, weighting, determinism, progress logs, and missing-data failures.

## Public Contract

- `FastRPParameters` carries feature properties, iteration weights, embedding dimension, property dimension, optional weight property, normalization strength, and node self influence.
- `FastRP.compute()` runs degree partitioning, property vectors, random vectors, self influence, propagation, and returns `FastRPResult(embeddings)`.
- `RelationshipType.of(name)` wraps a typed relationship identifier; `ALL_RELATIONSHIPS` is `__ALL__`.

## Fixture Graph Shape

- Two equivalent five-node graphs:
  - `arrayGraph` has array property `f`.
  - `scalarGraph` has scalar properties `f1`, `f2`, and `f3`.
- Nodes `a` and `b` are `Node1`, node `c` is `Node2`, and nodes `d` and `e` are `Isolated`.
- Six directed weighted `REL` edges connect `a`, `b`, and `c`.
- Nested missing-property fixture:
  - `b:N` is missing node property `prop`.
  - `c -> d` is missing relationship property `weight`.

## Public Contract Evidence

- `shouldYieldSameResultsForScalarAndArrayProperties` (`111`) checks equivalent scalar and array feature projections.
- `shouldSwapInitialRandomVectors` (`123`) checks propagation with filtered `Node1` graph and random vector swapping.
- `shouldAverageNeighbors` (`168`) checks unweighted neighbor averaging.
- `shouldAddInitialVectors` (`214`) checks self-influence scaling.
- `shouldInitialisePropertyEmbeddingsCorrectly` (`287`) pins property vectors and projected node-vector slices for seed `42`.
- `shouldBeDeterministicInParallel` (`354`) checks concurrency `4` versus `1`.
- `shouldAverageNeighborsWeighted` (`407`) checks weighted propagation semantics.
- `shouldDistributeValuesCorrectly` (`456`) checks random vector sign distribution.
- `shouldYieldEmptyEmbeddingForIsolatedNodes` (`520`) checks constructor-zeroed embeddings for isolated nodes.
- `shouldLogProgress` (`551`) checks progress-log phases.
- `shouldFailWhenNodePropertiesAreMissing` (`616`) checks missing node-property error.
- `shouldFailWhenRelationshipWeightIsMissing` (`650`) checks missing relationship-weight error.
- `shouldBeDeterministicGivenSameOriginalIds` (`691`) checks original-id-stable embeddings under shuffled mapped IDs.

## Asserted Outputs And Errors

- Scalar and array feature projections that encode the same values produce matching embeddings node-by-node.
- Unweighted propagation yields L2-normalized average of neighbor random vectors.
- Weighted propagation with `weight` uses weighted sums but still divides by degree before normalization.
- Self influence scales initial vectors by `nodeSelfInfluence / l2_norm` within `1e-6`.
- Property vector initialization for seed `42` is byte-level stable enough to assert exact float arrays/slices.
- Concurrency `4` and `1` produce exactly equal embeddings for the same seed.
- Same original IDs under shuffled internal mapped IDs produce average cosine `1 +/- 0.000001`.
- Missing node property failure includes the configured property name and original node ID.
- Missing relationship weight failure includes the configured weight property and original source/target IDs.

## Memory And Storage Implications

- FastRP allocates `propertyVectors`, result `embeddings`, and two work buffers (`embeddingA`/`embeddingB`) as per-node float arrays.
- The estimator mirrors this with fixed property-vector storage plus three per-node `HugeObjectArray<float[]>` estimates.
- This file does not assert exact memory ranges, but it exposes the algorithm's storage layout through direct lifecycle calls.

## Snapshot And Catalog Implications

- The test uses injected graph stores and filtered `getGraph(NodeLabel, RelationshipType, Optional<weight>)` reads.
- No mutate/write/catalog operation is asserted here.
- Fixture graph names would be `arrayGraph` and `scalarGraph`, but default `addToCatalog=false` keeps storage ephemeral.
- Progress logs are a user-visible algorithm lifecycle snapshot.

## Verification Oracles

1. **WHEN** scalar and array feature projections encode the same values, **THEN** embeddings SHALL match node-by-node.
2. **WHEN** unweighted propagation runs, **THEN** node embedding SHALL be the L2-normalized average of neighbor random vectors.
3. **WHEN** weighted propagation runs with `weight`, **THEN** weighted sums SHALL still be divided by degree before normalization.
4. **WHEN** self influence is nonzero and iteration weight is zero, **THEN** result embeddings SHALL equal normalized initial vectors scaled by self influence.
5. **WHEN** the same seed runs with concurrency `4` and `1`, **THEN** embeddings SHALL be exactly equal; same original IDs under shuffled mapped IDs SHALL average cosine `1 +/- 0.000001`.
6. **WHEN** feature values or relationship weight values are missing, **THEN** failures SHALL include the configured property name and original node IDs.

## Rust Rewrite Notes

- Seed random vectors from original node IDs, not mapped IDs.
- Keep two alternating propagation buffers plus one result buffer.
- Model missing relationship weight as an error on first weighted propagation.
- Make degree-normalization semantics explicit; weighted sums are not divided by total weight in this test.
- Test scalar and array feature extraction through one shared feature API.

## Dependencies Read Next

- `FeatureExtraction`
- `FeatureExtractor`
- `HugeObjectArray`
- `NodeEmbeddingAlgorithms`
- `FastRPConfigTransformer`
- `FastRPBaseConfig`
- FastRP stream/mutate/write facades
- `ElementIdentifier`
- `FastRPMemoryEstimateDefinition`

## Open Questions

- Should Rust preserve Java's exact high-quality RNG sequence byte-for-byte?
- Should `shouldYieldEmptyEmbeddingForIsolatedNodes` be renamed or expanded, since it currently checks constructor-zeroed embeddings before `compute()`?
- How should mutate/write modes persist embeddings relative to this pure algorithm result?

## Coding Prompt Unlocked

Build Rust FastRP oracle tests around scalar/array feature parity, random/property vector initialization, weighted and unweighted propagation, self influence, deterministic concurrency, missing-data errors, and original-id-stable embeddings.

# 1015 verification_oracle HashGNNTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/test/java/org/neo4j/gds/embeddings/hashgnn/HashGNNTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 406 |
| fan_in / fan_out | 0 / 33 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java |

## Why This File Matters

This is the compact semantic oracle for HashGNN embeddings: binary feature propagation, dense-feature binarization, output dimensionality, progress-log snapshots, concurrency repeatability, and original-id-based determinism.

## Public Contract

- `HashGNNParameters` carries concurrency, iterations, embedding density, neighbor influence, feature properties, heterogeneous flag, optional output dimension, optional binarize/generate feature configs, and random seed.
- `HashGNNConfigTransformer.toParameters(...)` is a field-for-field config bridge.
- `HashGNNResult` exposes embeddings as `NodePropertyValues`.
- `RelationshipType.of("REL")` and `Orientation.UNDIRECTED` anchor synthetic graph construction.

## Fixture Graph Shape

- Binary graph: three `:N` nodes with scalar `f1` plus 2-vector `f2`.
  - `a`: `f1=1`, `f2=[0.0, 0.0]`
  - `b`: `f1=0`, `f2=[1.0, 0.0]`
  - `c`: `f1=0`, `f2=[0.0, 1.0]`
  - Edges: `b-[:R]->a`, `b-[:R]->c`.
- Double graph: same topology, non-binary numeric features.
- Synthetic determinism graph: 1,000 nodes, label `hello`, undirected `REL` edges, degree `4` random targets, identical original IDs, and shuffled second mapped-node order.

## Public Contract Evidence

- `binaryLowNeighborInfluence` (`99`) expects exact binary embeddings.
- `binaryHighEmbeddingDensityHighNeighborInfluence` (`121`) expects neighbor influence to change only node `b`.
- `shouldBeDeterministic` (`142`) runs Cartesian concurrency/binarize/dim-reduce cases and requires exact vector equality across two runs.
- `shouldRunOnDoublesAndBeDeterministicEqualNeighborInfluence` (`169`) checks dense-feature binarization and equal neighbor/self influence.
- `shouldRunOnDoublesAndBeDeterministicHighNeighborInfluence` (`217`) checks high neighbor influence.
- `outputDimensionIsApplied` (`254`) checks explicit output dimension.
- `shouldLogProgress` (`275`) snapshots binary and dense progress logs from resource files.
- `shouldBeDeterministicGivenSameOriginalIds` (`320`) checks original-id-aligned cosine similarity under shuffled mapped IDs.

## Asserted Outputs And Errors

- Low neighbor influence with density `4` and seed `42` yields:
  - `a=[1.0, 0.0, 0.0]`
  - `b=[0.0, 1.0, 1.0]`
  - `c=[0.0, 0.0, 1.0]`
- High embedding density and high neighbor influence yields `b=[1.0, 0.0, 1.0]`, while `a` and `c` remain as asserted.
- Repeated runs are exact-match deterministic across concurrency `1` and `4`, binarize on/off, and dimensionality reduction on/off.
- Dense-feature binarization to dimension `8` yields `b >= max(a, c)` at influence `1.0`, with at least one unique component.
- Dense-feature binarization to dimension `8` yields `b == max(a, c)` at influence `1000.0`.
- `outputDimension=42` makes all returned embedding vectors length `42`.
- Two graphs with the same original IDs but different mapped IDs produce original-id-aligned average cosine close to `1` within `0.000001`.
- No invalid-input error path is asserted by this file.

## Memory And Storage Implications

- This test does not assert memory estimates.
- Production HashGNN memory includes two per-node bitset embedding caches, embedding-density-scaled hash cache, and per-node dense double arrays or sparse-to-dense range depending on `outputDimension`.
- The test is stream/in-memory only; it reads `doubleArrayValue` and does not mutate graph catalog storage.

## Snapshot And Catalog Implications

- Progress logs are exact snapshots through `expected-test-logs/hashgnn-binary` and `expected-test-logs/hashgnn-dense`.
- Binary logs include feature extraction, density `1.0000`, two min-hash iterations, and finish.
- Dense logs include binarization statistics, density `8.3333`, densify output, and finish.
- Original ID stability matters more than mapped ID stability for generated features and randomization.

## Verification Oracles

1. **WHEN** the binary graph uses density `4`, neighbor influence `0.01`, and seed `42`, **THEN** embeddings SHALL be `a=[1,0,0]`, `b=[0,1,1]`, and `c=[0,0,1]`.
2. **WHEN** the binary graph uses density `200` and neighbor influence `100`, **THEN** `b` SHALL become `[1,0,1]` while `a` and `c` remain as asserted.
3. **WHEN** the same parameters are run twice across concurrency `{1,4}`, binarize `{true,false}`, and dim-reduce `{true,false}`, **THEN** every node vector SHALL match exactly.
4. **WHEN** double features are binarized to dimension `8`, **THEN** neighbor influence `1.0` SHALL make `b >= max(a,c)` with at least one unique component, and influence `1000.0` SHALL make `b == max(a,c)`.
5. **WHEN** `outputDimension=42`, **THEN** every returned embedding SHALL have length `42`.
6. **WHEN** two graphs share original IDs but differ in mapped-node order, **THEN** original-id-aligned average cosine SHALL be `1 +/- 0.000001`.

## Rust Rewrite Notes

- Preserve original-id-based RNG/input determinism rather than mapped-id determinism.
- Flatten scalar plus array feature properties into a single feature vector before hashing.
- Make concurrency deterministic; exact vector equality is asserted across thread counts.
- Snapshot progress-log strings if Rust keeps compatible logging.
- Represent relationship type as a value object with `__ALL__`, `of`, `to_string`, and `list_of` semantics.

## Dependencies Read Next

- `HashGNN.java`
- `HashTask`
- `HashGNNTask`
- `HashGNNCompanion`
- `HashGNNConfigTest`
- `HashGNNMemoryEstimateDefinitionTest`
- `HashGNNStreamProcTest`
- `HashGNNMutateProcTest`

## Open Questions

- Invalid feature-property errors are not asserted here; which config/procedure tests own them?
- Heterogeneous behavior, relationship-type-specific hashing, and mutation/write storage are not covered by this file.
- Should Rust compatibility include the large progress-log resources as snapshots or reduce them to phase names and key metrics?

## Coding Prompt Unlocked

Build Rust HashGNN oracle tests around exact binary embeddings, dense binarization behavior, output dimension, concurrency determinism, progress logs, and original-id-stable generated embeddings.

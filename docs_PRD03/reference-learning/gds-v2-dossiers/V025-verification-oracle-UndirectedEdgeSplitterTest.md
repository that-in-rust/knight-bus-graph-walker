# 1025 verification_oracle UndirectedEdgeSplitterTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | ml/ml-algo/src/test/java/org/neo4j/gds/ml/splitting/UndirectedEdgeSplitterTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 469 |
| fan_in / fan_out | 0 / 25 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java |

## Why This File Matters

This verifies undirected link-prediction split semantics: holdout relationships become directed positive examples, remaining relationships preserve undirected schema/properties, label filters remove invalid pairs, random seeds control repeatability, and dense-graph negative examples must not collide with the master graph.

## Public Contract

- `EdgeSplitter.splitPositiveExamples(graph, holdoutFraction, remainingRelPropertyKey)` builds selected relationships as `Direction.DIRECTED` with property key `label`.
- Remaining relationships retain original graph direction and optional property key.
- Split output is returned as relationship builders/counts in `SplitResult`.
- Builders force `Aggregation.SINGLE` for topology/property configs.
- `Aggregation.SINGLE.merge(...)` keeps the running value.

## Fixture Graph Shape

- Base undirected chain: `n1:A - n2:A - n3:A - n4:A - n5:B - n6:A`, all `:T {foo: 5}`.
- Multi-label chain: `A - C - A - A - B - D`.
- Two-node multigraph fixture has four parallel directed declarations under undirected projection.
- Unused negative fixture adds `:NEGATIVE` edges to `n3`, `n5`, and `n7`.

## Public Contract Evidence

- `split` (`95`) expects remaining topology count `8`, undirected direction, non-multigraph output, and copied properties.
- `splitMultiGraph` (`126`) asserts aggregated non-multigraph output with element count `2`.
- Determinism tests assert same seed gives equal remaining/holdout and different seeds differ (`190`, `236`).
- Label filtering tests expect remaining counts `4` for `A -> A` and `2` for `A/B -> C/D`, with selected counts `1` and `2`.
- Weight preservation asserts every copied `foo` value remains `5.0`.

## Asserted Outputs And Errors

- Base split with holdout `0.2` and property `foo` yields remaining undirected non-multigraph topology count `8`.
- Selected holdout graph is directed, non-undirected, non-multigraph, and has one positive edge.
- Label filters drop invalid pairs from both selected and remaining graphs.
- Same seed reproduces remaining and holdout relationships exactly; different seeds differ.
- Remaining relationship properties preserve `foo=5.0`.
- Multigraph input collapses to non-multigraph output under current `Aggregation.SINGLE` behavior.
- No explicit thrown error path is asserted by this file.

## Memory And Storage Implications

- Tests allocate in-memory `RelationshipsBuilder`s and `HugeGraph`s only.
- `createGraph` reuses `GraphStore` nodes/schema and attaches topology/properties from built relationships.
- No catalog mutation or snapshot file write occurs.
- Counting valid undirected candidates is parallel via degree partition and `LongAdder`; sampling uses mutable counters and Java RNG.

## Snapshot And Catalog Implications

- The split output itself is an in-memory graph snapshot.
- Remaining graph must preserve the original graph direction and relationship properties.
- Selected graph must be directed positive examples with `label=1.0`.
- `Aggregation.SINGLE` deduplication is part of the observable output shape.

## Verification Oracles

1. **WHEN** splitting the base undirected chain with holdout `0.2` and property `foo`, **THEN** remaining SHALL be undirected, non-multigraph, property-bearing, with topology element count `8`, and selected SHALL be directed/non-undirected with one positive edge.
2. **WHEN** splitting with source/target labels both `A`, **THEN** invalid non-`A -> A` pairs SHALL be dropped from both selected and remaining, leaving remaining count `4` and selected count `1`.
3. **WHEN** splitting with source labels `A,B` and target labels `C,D`, **THEN** selected positive examples SHALL only target `C,D` nodes and remaining SHALL contain only original relationships with count `2`.
4. **WHEN** the same seeded splitter runs twice on the same random undirected graph, **THEN** remaining and holdout relationships SHALL be equal; with different seeds they SHALL differ.
5. **WHEN** preserving remaining relationship properties, **THEN** every copied `foo` value SHALL remain `5.0`.
6. **WHEN** aggregating split output from a multigraph, **THEN** output topology SHALL not remain a multigraph under current `Aggregation.SINGLE` behavior.

## Rust Rewrite Notes

- Preserve `source < target` undirected canonicalization.
- Count valid undirected candidates as `+2` and decrement sampling counters by `2`.
- Skip invalid/self-loop pairs.
- Build selected edges as directed `(root_source, root_target, 1.0)`.
- Preserve remaining weights.
- Implement `SINGLE` deduplication.
- Keep deterministic iteration/RNG behavior explicit; exact Java `Random` compatibility is only required if future tests assert exact sampled edges.

## Dependencies Read Next

- `SplitRelationships`
- `RandomNegativeSampler`
- `UserInputNegativeSampler`
- `DirectedEdgeSplitterTest`
- `RelationshipsBuilder`
- `GraphFactory`
- Aggregation semantics in relationship builders

## Open Questions

- `negativeSamplingRatio` in `split` is unused; should it be covered by a different splitter test?
- `negativeGraph` is injected but unused; is this dead fixture or future negative-sampling coverage?
- `negativeEdgesShouldNotOverlapMasterGraph` checks existing negative edges, while `splitPositiveExamples` creates positives only.
- `splitMultiGraph` uses `DirectedEdgeSplitter` inside the undirected test class; is that intentional cross-parity coverage?

## Coding Prompt Unlocked

Build Rust undirected edge-splitter oracle tests around canonical undirected sampling, label filters, deterministic seeds, property preservation, directed positive holdouts, and `Aggregation.SINGLE` output shape.

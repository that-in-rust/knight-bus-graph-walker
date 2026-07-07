# 1026 verification_oracle LouvainTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/test/java/org/neo4j/gds/louvain/LouvainTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 399 |
| fan_in / fan_out | 0 / 25 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java |

## Why This File Matters

This is the algorithm-level oracle for Louvain community detection: unweighted versus weighted partitions, seeded exact labels, tolerance/max-level stopping, cancellation, negative seed rejection, and modularity consistency against an independent calculator.

## Public Contract

- Louvain constructor accepts graph, concurrency, max iterations, tolerance, max levels, intermediate tracking, optional seed property, progress tracker, executor, and termination flag.
- `compute()` runs modularity optimization level-by-level.
- It stores modularities and dendrograms, summarizes the graph, and stops on unchanged node count, single-node graph, convergence, or max levels.
- `LouvainResult` exposes final communities, ran levels, dendrogram manager, modularities, modularity, `community(node)`, and size.
- Summarized graph relationships use synthetic type `IGNORED`, inherit root orientation, and aggregate property weights with `Aggregation.SUM`.

## Fixture Graph Shape

- Undirected GDL with `idOffset=0`.
- Fifteen `:Node` vertices `a..n,x` with `seed` values `1`, `2`, and `42`.
- Unselected nodes `u:Some`, `v:Other`, and `w:Label` are present but excluded by label selection.
- `TYPE_OUT` relationships all have `weight`, mostly `1.0`, with one weak edge `e-f` at `0.01`.
- `seed2` has `a=-1` and `b=10`, used for the negative seed error.

## Public Contract Evidence

- Unweighted test asserts level-0 and level-1 communities, ran levels, and modularity.
- Weighted test asserts altered communities due to weak edge and weighted projection.
- Seeded test asserts exact seed labels and one-level stop.
- Tolerance and max-level tests assert stop conditions.
- Negative seed test asserts `non-negative` error.
- Random graph oracle compares Louvain modularity to `ModularityCalculator`.

## Asserted Outputs And Errors

- Unweighted level 0 communities:
  - `{a,b,d}`
  - `{c,e,f,x}`
  - `{g,h,i}`
  - `{j,k,l,m,n}`
- Unweighted level 1 merges first two into `{a,b,c,d,e,f,x}` and yields `ranLevels=2`, modularity about `0.38`.
- Weighted level 0 communities:
  - `{a,b,d}`
  - `{c,e,x}`
  - `{f,g}`
  - `{h,i}`
  - `{j,k,l,m,n}`
- Weighted level 1 communities are `{a,b,c,d,e,f,g,x}` and `{h,i,j,k,l,m,n}`, with `ranLevels=2`, modularity about `0.37`.
- Seeded mode preserves exact community labels `1`, `2`, and `42`, stops after one level, and modularity is about `0.38`.
- Tolerance `2.0` and `maxLevels=1` each stop at one level.
- Negative `seed2` throws an error containing `non-negative`.

## Memory And Storage Implications

- No catalog writes occur; tests call `graphStore.getGraph(...)`.
- Louvain stores `HugeLongArray` dendrograms.
- Tracking intermediate communities allocates `maxLevels` arrays.
- Non-tracking mode stores only `min(maxLevels, 2)` ring-buffer arrays.
- Summarization uses `Aggregation.SUM` for internal graph weights.
- Random graph oracle uses `Aggregation.SINGLE` to collapse duplicate relationships.

## Snapshot And Catalog Implications

- Dendrogram levels are the core algorithm snapshot surface.
- Seeded mode requires exact community IDs; unseeded tests assert groups rather than arbitrary labels.
- Unselected labels are deliberately excluded by `NodeLabel.listOf("Node")`.
- Modularity consistency against `ModularityCalculator` is an independent verification surface.

## Verification Oracles

1. **WHEN** Louvain runs unweighted on the `:Node`/`:TYPE_OUT` fixture, **THEN** dendrogram levels SHALL match the asserted four level-0 groups and three level-1 groups, with `ranLevels=2` and final modularity about `0.38`.
2. **WHEN** Louvain runs weighted with `weight`, **THEN** the weak `e-f` edge SHALL change level-0 and level-1 grouping as asserted, with final modularity about `0.37`.
3. **WHEN** `seedProperty="seed"` is provided, **THEN** first-level communities SHALL retain exact seed labels `1`, `2`, and `42`, and the algorithm SHALL stop after one level.
4. **WHEN** tolerance is larger than modularity improvement, **THEN** computation SHALL stop after one level; **WHEN** `maxLevels=1`, **THEN** it SHALL also stop after one level.
5. **WHEN** a seed property contains a non-missing negative value, **THEN** compute SHALL throw an error containing `non-negative`.
6. **WHEN** Louvain computes on the seeded random graph oracle, **THEN** result modularity SHALL equal `ModularityCalculator` total modularity within `1e-5`, and `ranLevels` SHALL be greater than `1`.

## Rust Rewrite Notes

- Implement deterministic node/relationship iteration and tie-breaking.
- Community IDs may be arbitrary for unseeded grouped assertions, but seeded mode requires exact labels.
- Preserve non-negative seed validation from modularity optimization.
- Use a ring buffer for dendrograms when intermediate tracking is off.
- Use `SUM` aggregation for summarized graph weights.
- Make termination checks explicit at level boundaries and parallel loops.

## Dependencies Read Next

- `ModularityOptimization`
- `ModularityColorArray`
- `RelationshipCountCollector`
- `LouvainMemoryEstimateDefinition`
- Louvain stream/stats/write/mutate procedure adapters
- `RandomGraphGenerator` internals for exact random graph fixture parity
- `Aggregation`

## Open Questions

- Unseeded Louvain partition tests are sensitive to modularity tie-breaking even though labels are not exact.
- `LouvainResult.intermediateCommunities` always sees a non-null manager here, but non-tracking mode stores a two-slot ring.
- Fixture includes excluded `Some`/`Other`/`Label` nodes; should Rust tests explicitly assert exclusion?

## Coding Prompt Unlocked

Build Rust Louvain oracle tests around weighted/unweighted dendrogram groups, seeded exact labels, stop conditions, negative seed validation, summarized graph aggregation, and modularity-calculator parity.

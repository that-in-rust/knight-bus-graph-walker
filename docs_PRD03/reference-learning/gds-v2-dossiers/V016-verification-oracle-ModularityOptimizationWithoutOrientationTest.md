# 1016 verification_oracle ModularityOptimizationWithoutOrientationTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | algo/src/test/java/org/neo4j/gds/modularityoptimization/ModularityOptimizationWithoutOrientationTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 303 |
| fan_in / fan_out | 0 / 33 |
| seed anchor | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java |

## Why This File Matters

This is the oracle for modularity optimization on a graph projected without an explicit orientation override, covering weighted versus unweighted behavior, seed-property handling, progress logging, and byte-exact memory estimation.

## Public Contract

- `ModularityOptimizationResult` exposes `communityId(nodeId)`, `modularity`, `ranIterations`, `didConverge`, `nodeCount`, and `asNodeProperties`.
- The test constructs `ModularityOptimization(graph, maxIterations, tolerance, seedProperties, concurrency, minBatchSize, pool, progressTracker, terminationFlag).compute()`.
- The seed `MemoryEstimations` contract defines named resident/temporary memory and composable per-node/per-thread memory trees.

## Fixture Graph Shape

- Six `:Node` nodes with `seed1`; all except `b` also have `seed2`.
- Expected seeded communities are `{a,b}`, `{c,e}`, `{d,f}`.
- Fourteen weighted `TYPE_OUT` directed relationships form symmetric pairs.
- Strong weights are `5.0`; weak weights are `0.01`.
- `unweightedGraph()` retrieves label `Node`, relationship type `TYPE_OUT`, and no relationship weight property.

## Public Contract Evidence

- `testUnweighted` (`114`) checks unweighted modularity, communities, and iteration bound.
- `testWeighted` (`129`) checks weighted modularity, communities, and iteration bound.
- `testSeedingWithBiggerSeedValues` (`142`) checks `seed2`, missing seed handling, large seed values, and remapped community IDs.
- `testSeeding` (`168`) checks `seed1` community grouping.
- `testLogging` (`194`) checks key progress phases.
- `testMemoryEstimation` (`214`) checks exact min/max memory ranges for three concurrency values.
- `memoryEstimationTuples` (`248`) defines byte-exact expected ranges.

## Asserted Outputs And Errors

- Unweighted graph with max iterations `3` yields modularity `0.12244 +/- 0.001`, communities `{a,b,c,e}` and `{d,f}`, and `ranIterations <= 3`.
- Weighted graph with concurrency `3` and min batch size `2` yields modularity `0.4985 +/- 0.001`, communities `{a,e,f}` and `{b,c,d}`, and `ranIterations <= 3`.
- Seeding from `seed2` yields modularity `0.0816 +/- 0.001`, communities `{a,b}`, `{c,e}`, `{d,f}`, with `a/b` mapped to new community `5580`, `c/e` to `4242`, and `d/f` to `3333`.
- Seeding from `seed1` yields the same community grouping and modularity `0.0816 +/- 0.001`.
- Logging contains start, K1Coloring color/validate start-finish messages, optimizeForColor start-finish messages, and finished message.
- Memory estimate for 100,000 nodes:
  - concurrency `1`: `5,614,032..8,413,064`
  - concurrency `4`: `5,617,320..14,413,328`
  - concurrency `42`: `5,658,968..90,416,672`
- No error path is asserted by this file.

## Memory And Storage Implications

- Memory estimation is byte-exact and tree-component-sensitive.
- Components include `currentCommunities`, `nextCommunities`, `cumulativeNodeWeights`, `nodeCommunityInfluences`, `communityWeights`, color bitsets, colors, optional reversed seed-community mapping, community-weight updates, and per-thread `communityInfluences`.
- The file computes in memory and does not write or mutate catalog properties.

## Snapshot And Catalog Implications

- `GraphStore.getGraph(...)` plus `RelationshipType.listOf("TYPE_OUT")` is the catalog boundary.
- Weighted and unweighted projections are separate graph views.
- Logging is a partial snapshot: the test asserts contained lifecycle messages rather than the full ordered list.
- Seed property handling is part of the algorithm contract, not only procedure-layer validation.

## Verification Oracles

1. **WHEN** the unweighted projection is computed with max iterations `3`, **THEN** modularity SHALL be `0.12244 +/- 0.001`, communities SHALL be `{a,b,c,e}` and `{d,f}`, and iterations SHALL be `<= 3`.
2. **WHEN** the weighted graph is computed with concurrency `3` and min batch size `2`, **THEN** modularity SHALL be `0.4985 +/- 0.001`, communities SHALL be `{a,e,f}` and `{b,c,d}`, and iterations SHALL be `<= 3`.
3. **WHEN** seeding from `seed2` with missing `b.seed2`, **THEN** communities SHALL be `{a,b}`, `{c,e}`, and `{d,f}`, with `a/b` remapped to `5580`, `c/e` to `4242`, and `d/f` to `3333`.
4. **WHEN** seeding from `seed1`, **THEN** modularity SHALL be `0.0816 +/- 0.001` and seeded community grouping SHALL match `{a,b}`, `{c,e}`, and `{d,f}`.
5. **WHEN** logging a run at `K1COLORING_MAX_ITERATIONS`, **THEN** logs SHALL contain start, K1Coloring color/validate start-finish, optimizeForColor start-finish, and finished messages.
6. **WHEN** estimating memory for 100,000 nodes, **THEN** byte ranges SHALL match the three concurrency tuples exactly.

## Rust Rewrite Notes

- Keep weighted and unweighted projections as distinct graph views.
- Preserve seed-community handling for large seed values and missing seed values.
- Expose result as both direct lookup and node-property values.
- Make memory estimation tree compositional with min/max ranges and concurrency scaling.
- Keep progress phases named compatibly if logs remain an oracle.

## Dependencies Read Next

- `ModularityOptimization.java`
- `ModularityOptimizationTask.java`
- `K1Coloring`
- `ModularityOptimizationMemoryEstimateDefinitionTest`
- `ModularityOptimizationTest`
- Procedure stream/stats/write/mutate tests for modularity optimization
- `MemoryRange`
- `MemoryTree`

## Open Questions

- `testSeeding` has a loose boolean assertion with Java precedence: `(actual[0] == 4 && actual[2] == 2) || actual[3] == 3`; confirm the intended exact ID contract before porting.
- This file does not cover disconnected nodes, negative weights, directed-only asymmetric input, write/mutate modes, or invalid seed-property types.

## Coding Prompt Unlocked

Build Rust modularity optimization oracle tests around weighted/unweighted community results, seed-property behavior, progress phases, and exact memory-estimation ranges for 100,000-node graphs.

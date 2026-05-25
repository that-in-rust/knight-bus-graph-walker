# Neo4j Backend Strategy Timeline Comparison

This note compares three plausible paths for building a Neo4j-compatible Rust system:

- a near-literal Rust port of Neo4j-style backend architecture
- a Rust-native backend rewrite using Knight Bus and bespoke storage families
- a hybrid path that treats Neo4j as the behavioral oracle and Rust as the implementation freedom

The goal is:

> preserve a flawless Neo4j user experience while choosing the backend strategy that gives the best long-term adoption and performance story.

These are directional estimates, not measured benchmark claims.

## Decision Frame

The real fork is not "port or rewrite" in the abstract.

It is:

- do we optimize for implementation familiarity
- or do we optimize for long-term product differentiation
- or do we preserve the Neo4j contract while rewriting only the backend layers that create real advantage

Assumptions:

- the user-facing surface should remain Neo4j-like
- backend specialization is allowed
- compatibility includes Bolt, drivers, Cypher behavior, transaction semantics, and operational expectations
- algorithm improvements should be estimated against the most relevant Neo4j baseline, not against the weakest possible baseline

## Timeline A

### Near-Literal Rust Port

Starting move:

- rebuild Neo4j-style kernel, storage, transactional, protocol, and operational behavior as directly as possible in Rust

Likely sequence:

1. first `2-4` months feel productive because the structure is inherited
2. months `4-10` become dominated by hidden behavioral coupling
3. months `10-18` are spent on compatibility archaeology rather than obvious innovation
4. months `18-30` are spent hardening correctness, import, recovery, procedures, error behavior, and edge cases

Daily reality:

- lower fear at the beginning
- increasing frustration later because much of the work is rebuilding complexity users do not directly reward
- stronger confidence in semantic parity than in differentiated performance

Estimates:

- first meaningful beta: `12-18 months`
- adoption-grade v1: `24-36 months`
- probability of feeling "Neo4j enough" early: `medium-high`
- probability of feeling strategically exciting: `medium`

## Timeline B

### Rust-Native Bespoke Rewrite

Starting move:

- define the compatibility contract
- build a Neo4j-shaped shell
- rewrite the backend around immutable artifacts, dense IDs, `mmap`, and workload-specific layout families

Likely sequence:

1. first `1-3` months go into compatibility definition and oracle testing
2. months `3-6` produce the first narrow but impressive backend proofs
3. months `6-12` unlock several algorithm families with strong backend differentiation
4. months `12-24` are spent expanding compatibility breadth and operational maturity

Daily reality:

- more energizing and strategically sharp
- more scope pressure because performance success can arrive before product trust does
- requires constant discipline to avoid building a fast but behaviorally incomplete substitute

Estimates:

- first meaningful beta: `6-12 months` for narrow compatibility
- broader developer beta: `12-18 months`
- adoption-grade v1: `18-30 months`
- probability of strong backend differentiation: `high`
- probability of user trust lagging backend speed: `high`

## Timeline C

### Recommended Hybrid

Starting move:

- treat Neo4j as the behavioral oracle
- build compatibility tests first
- rewrite the backend only where Rust-native storage and execution produce durable gains

Likely sequence:

1. months `0-2` define the compatibility matrix and conformance harness
2. months `2-6` implement the Neo4j-shaped shell plus the first high-ROI hot paths
3. months `6-12` expand into PageRank-style, SCC/WCC, shortest path, and wedge-intersection families
4. months `12-24` widen compatibility while preserving backend freedom

Daily reality:

- best balance of confidence and momentum
- less architectural romance than a pure greenfield rewrite
- much lower regret than a full internal port

Estimates:

- first meaningful beta: `6-9 months`
- adoption-grade v1: `18-24 months`
- probability of preserving product trust while building a moat: `high`

## Algorithm Improvement Estimates

These are directional ranges.

The left column estimates what a near-literal port might plausibly deliver over a comparable Neo4j baseline. The right column estimates what a Rust-native bespoke backend might plausibly deliver if the layout family truly matches the dominant inner loop.

| algorithm family | near-literal port | bespoke rewrite | confidence | main caveat |
| --- | ---: | ---: | --- | --- |
| Exact-key fixed-hop traversal over Cypher/Bolt | `1.1x-2.0x` | `5x-30x` | high | strongest only on narrow replay-style workloads |
| BFS / DFS / random walk | `1.0x-1.5x` | `1.3x-3.0x` | medium | fair comparison should include GDS-style projected execution |
| PageRank / HITS / Eigenvector | `1.0x-1.5x` | `1.5x-5.0x` | medium | wins depend on dense inbound planes and projection cost |
| SCC / WCC / bridges / articulation points | `1.0x-1.6x` | `1.5x-4.0x` | medium | reverse-pass and twin-edge identity matter |
| Dijkstra / A* / Delta-stepping | `1.0x-1.6x` | `1.5x-4.0x` | medium | gains come from queue-friendly IDs and flat weight planes |
| Triangle Count / Node Similarity / common-neighbor family | `1.0x-1.3x` | `2.0x-10.0x` | medium-high | strongest when sorted intersections dominate runtime |
| Louvain / Leiden / label propagation | `0.9x-1.3x` | `1.2x-3.0x` | low-medium | graph coarsening and refinement complexity can eat gains |
| Maximum Flow / Min-Cost Max-Flow | `1.0x-1.4x` | `1.5x-4.0x` | low-medium | mutable residual state is harder to harden cleanly |
| KNN / K-Means / HDBSCAN | `0.9x-1.2x` | `1.1x-2.5x` | low | many cases are compute-bound rather than graph-fetch-bound |
| FastRP / Node2Vec / GraphSAGE / CELF | `0.9x-1.2x` | `1.1x-2.5x` | low | training or simulation often dominates storage wins |

## Risk Comparison

| path | biggest risk | schedule risk | semantics risk | strategic upside |
| --- | --- | ---: | ---: | ---: |
| near-literal port | spend years preserving internals users do not value | high | medium | medium |
| bespoke rewrite | build a fast engine that is not yet trusted as Neo4j-like | high | high | very high |
| hybrid | let parity work and backend work blur into one expanding scope | medium | medium | very high |

## Cross-Timeline Analysis

The strongest pattern is:

- a literal port is better for reducing conceptual fear than for creating a moat
- a bespoke rewrite is better for creating a moat than for reducing adoption anxiety
- the hybrid path gives the best risk-adjusted story because it ports the contract instead of the internals

The highest-ROI backend proof order remains:

1. `AnchorDualCsrLayoutV1`
2. `InboundPowerLayoutV1`
3. `ConnectivityLowlinkLayoutV1`
4. `RelaxationFrontierLayoutV1`
5. `OrderedWedgeLayoutV1`

And the best first concrete proof snapshots remain:

1. `DegreeCentralityAnchorDualCsrSnapshotV1`
2. `BfsTraversalFrontierSnapshotV1`
3. `PageRankInboundPowerSnapshotV1`
4. `DijkstraSingleSourceHeapRelaxationSnapshotV1`
5. `TriangleCountOrderedWedgeSnapshotV1`

That is the path that most cleanly compounds backend differentiation without forcing the full algorithm universe into scope on day one.

## Decision Filter

If the real priority is:

- "make it feel exactly like Neo4j as fast as possible, even if the backend story is less interesting"

then the near-literal port is defensible.

If the real priority is:

- "build a real moat, even if compatibility takes longer to mature"

then the bespoke rewrite is stronger.

If the real priority is:

- "maximize adoption while still earning a durable backend advantage"

then the hybrid path is the most robust choice.

Short version:

> port the user contract, not the JVM internals.

## Basis

This note is grounded in:

- [A-20260525164835-faithful-rust-port-dossier.md](./A-20260525164835-faithful-rust-port-dossier.md)
- [A-20260525171232-knight-bus-storage-format-story-summary.md](./A-20260525171232-knight-bus-storage-format-story-summary.md)
- [KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md](../KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md)
- official Neo4j documentation on Cypher, Bolt, drivers, and import behavior

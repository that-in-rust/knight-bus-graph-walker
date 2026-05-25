# OLTP/OLAP E2E Benchmark Estimates

This note captures a modeled end-to-end benchmark view for a proposed split architecture:

- `OLTP truth plane`: Neo4j-like transactional graph core
- `OLAP RAM-optimized plane`: generic CSR-style projection with minimal prebuilt sidecars
- `OLAP latency-optimized plane`: same base projection plus hot specialized views for known algorithm families

Important caveat:

- These are **modeled estimates**, not measured end-to-end benchmark results yet.
- What is actually measured today is only the repo's current narrow benchmark: Knight Bus fixed-hop traversal versus Neo4j over Bolt in [README.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/README.md:1).
- The ranges below extrapolate from that measured traversal win, the local storage/atlas notes, and Neo4j's official GDS projection model:
  - [Neo4j GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/)
  - [Neo4j native projection](https://neo4j.com/docs/graph-data-science/current/management-ops/graph-creation/graph-project/)
  - [Neo4j Cypher projection](https://neo4j.com/docs/graph-data-science/current/management-ops/graph-creation/graph-project-cypher-projection/)

## Benchmark Model

Assumptions:

- `50 GB` logical graph
- low write rate
- Neo4j-like OLTP truth layer
- two OLAP variants:
  - `RAM-optimized`: one generic CSR-style base projection, minimal prebuilt sidecars
  - `Latency-optimized`: same base projection plus hot specialized views for known algorithm families
- known algorithm calls go to OLAP
- transactional writes stay in OLTP
- read-only traversal routes to OLAP when supported

## E2E Comparison

### 1. Transactional Behavior

| workload | Neo4j-only | OLTP + RAM-OLAP | OLTP + Latency-OLAP | confidence |
| --- | ---: | ---: | ---: | --- |
| write commit latency | `1.0x` | `0.95x-1.05x` | `0.95x-1.05x` | medium |
| transactional point reads | `1.0x` | `0.95x-1.05x` | `0.95x-1.05x` | medium |

Why:

- If projections refresh asynchronously, OLTP stays near parity.
- If projection updates become part of the commit path, this gets worse fast.

### 2. Read-Only Ad-Hoc Graph Queries

| workload | Neo4j-only | OLTP + RAM-OLAP | OLTP + Latency-OLAP | confidence |
| --- | ---: | ---: | ---: | --- |
| exact-key 1-2 hop traversal | `1.0x` | `5x-20x` | `5x-20x` | medium-high |
| read-heavy dependency/blast-radius queries | `1.0x` | `3x-12x` | `3x-12x` | medium |
| arbitrary Cypher not mappable to base OLAP | `1.0x` | `0.9x-1.0x` | `0.9x-1.0x` | medium |

Why:

- This is where the current measured Knight Bus proof matters most.
- The local benchmark shows huge wins for narrow fixed-hop traversal.
- These ranges intentionally discount those microbenchmark wins into more believable end-to-end estimates.

### 3. First Algorithm Call After Fresh Data

This is where the two OLAP variants separate.

| workload | Neo4j-only | OLTP + RAM-OLAP | OLTP + Latency-OLAP | confidence |
| --- | ---: | ---: | ---: | --- |
| first `PageRank` after refresh | `1.0x` | `1.3x-3.0x` | `0.8x-2.0x` | low-medium |
| first shortest-path family run | `1.0x` | `1.5x-3.5x` | `0.9x-2.0x` | low-medium |
| first triangle/similarity run | `1.0x` | `1.2x-2.5x` | `0.7x-1.8x` | low |

Why:

- `RAM-optimized` usually wins the cold path because it builds less upfront and leans on the generic base.
- `Latency-optimized` can lose cold if the hot specialized view does not exist yet and must be built.

### 4. Repeated Known Algorithm Calls on a Stable Graph

| workload | Neo4j-only | OLTP + RAM-OLAP | OLTP + Latency-OLAP | confidence |
| --- | ---: | ---: | ---: | --- |
| repeated `PageRank` / HITS / Eigenvector | `1.0x` | `1.8x-4.0x` | `3x-8x` | medium |
| repeated shortest path / delta-stepping | `1.0x` | `2x-5x` | `3x-10x` | medium |
| repeated triangle / node similarity | `1.0x` | `2x-6x` | `4x-15x` | medium-low |
| repeated Louvain / Leiden / label propagation | `1.0x` | `1.3x-2.5x` | `1.5x-3.5x` | low-medium |

Interpretation:

- the generic base helps a lot
- specialized hot layouts help most for known, repeated workloads

These estimates line up with:

- [Knight Bus Algorithm Storage Atlas](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs/KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md:1)
- [faithful Rust port dossier](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs/strategic-research/A-20260525164835-faithful-rust-port-dossier.md:1)
- [storage format story summary](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs/strategic-research/A-20260525171232-knight-bus-storage-format-story-summary.md:1)

### 5. Memory and Infrastructure

Normalized against `Neo4j-only analytics setup = 1.0x`.

| metric | Neo4j-only | OLTP + RAM-OLAP | OLTP + Latency-OLAP | confidence |
| --- | ---: | ---: | ---: | --- |
| peak analytics RAM | `1.0x` | `0.25x-0.50x` | `0.40x-0.80x` | medium |
| total disk footprint | `1.0x` | `1.2x-2.0x` | `2.0x-6.0x` | medium |
| combined server cost | `1.0x` | `0.6x-0.9x` | `0.9x-1.3x` | low-medium |

Interpretation:

- `RAM-optimized` is the likely PMF wedge
- `Latency-optimized` is the power-user or premium tier
- latency mode often spends disk and precompute budget to reduce user-visible runtime

## Cold, Warm, and Steady-State Winners

| scenario | likely winner |
| --- | --- |
| cheapest analytics footprint | `OLTP + RAM-OLAP` |
| best first result after import | `OLTP + RAM-OLAP` |
| best repeated hot algorithm latency | `OLTP + Latency-OLAP` |
| safest simplest architecture | `Neo4j-only` or `OLTP + RAM-OLAP` |
| best overall risk-adjusted product story | `OLTP + RAM-OLAP` first, then add latency OLAP |

## My Best E2E Read

If this is compressed into one recommendation:

- **Phase 1**
  - build `OLTP + RAM-optimized OLAP`
  - use one generic CSR base projection
  - route read-only traversal and most analytics there
  - build specialized views lazily

- **Phase 2**
  - add `Latency-optimized OLAP`
  - only for the top `3-5` repeated algorithms
  - treat it like a hot materialized-view tier

This gives the most believable sequence:

1. save memory and machine cost first
2. keep unknown-query coverage strong
3. add turbo modes only where usage proves demand

## What Would Make These Estimates Wrong

These ranges move a lot if any of these turn out false:

- read-only Cypher-to-base-OLAP routing covers much less than expected
- projection refresh is more expensive than assumed
- specialized views are hard to keep fresh incrementally
- real customer workloads are mostly unusual ad-hoc Cypher, not known algorithms
- OLTP compatibility work dominates so heavily that OLAP gains arrive too late to matter

## The 3 Real Benchmark Suites To Run Next

### 1. Cold Path

- import `50 GB`
- publish base OLAP projection
- run first `PageRank`, first shortest-path, first similarity query

### 2. Warm Path

- run `100` repeated calls each of:
  - read-only `1-2` hop traversal
  - `PageRank`
  - shortest path
  - node similarity

### 3. Steady-State Mixed Day

- low write stream into OLTP
- periodic projection refresh
- mixed OLTP reads plus OLAP queries
- measure:
  - write ack latency
  - projection lag
  - peak RAM
  - `p50` and `p95` OLAP latency
  - total box cost

## Final Synthesis

The strongest near-term story is:

- **first** ship `OLTP + RAM-optimized OLAP`
- **then** add a latency tier for repeated known workloads

That sequence matches both the local research trail and the practical benchmark logic:

- one generic projection keeps unknown-query fear manageable
- memory savings are the sharper early PMF
- specialized formats are most valuable once actual usage identifies the hot algorithm set

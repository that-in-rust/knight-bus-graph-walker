# Main Code-Graph Architecture Evidence

## Purpose

This note records the main-agent structural evidence used to connect the A007 product contract, the Neo4j compatibility adapter, and the low-RAM algorithm architecture atlas. It does not claim that the algorithm kernels already exist.

## Tool Runs

The requested `code-graph-mcp` package was invoked through `npx` because no global `code-graph` binary was installed.

```text
npx --yes @sdsrs/code-graph reindex
Full index: 447 files, 12,864 nodes, 2,907 edges
```

The index is FTS5-only because no embedding model was present. Searches therefore provide lexical and structural evidence, not semantic-vector completeness.

Searches executed:

1. `memory budget admission spill graph algorithm`
2. `cypher canonical plan bolt gds execution`
3. `pagerank wcc louvain triangle fastrp similarity knn`

The same repository was also queried through `codebase-memory-mcp`, whose current graph reports 10,093 nodes, 14,600 edges, 428 functions, 240 methods, and 300 previously indexed execution flows in the repository metadata. The two tools have different extractors and denominators, so their counts are not expected to match.

## Structural Findings

### 1. The compatibility adapter already has a narrow canonical-plan seam

`src/cypher.rs::compile_neighborhood_walk_plan` parses one deliberately bounded Cypher profile and produces `CompiledNeighborhoodWalkPlan`. The codebase-memory graph reports 13 direct callers, including `KnightBusBoltBackend.execute` and the Cypher differential/contract tests. The compiler explicitly rejects writes, unbounded patterns, procedures, predicates, unsupported clauses, and unsupported hop ranges.

Architecture consequence: new algorithm calls SHALL compile into bounded, versioned algorithm-plan values adjacent to this seam. They SHALL not make the Cypher parser or Bolt session layer own algorithm storage.

### 2. GDS currently exposes catalog and estimate foundations, not the nine kernels

`src/gds/execution.rs::execute_registered_gds_entry` dispatches catalog projection, projection estimate, existence, listing, property streaming, dropping, and size operations. The graph reaches `memory_estimate_detail_map_now`, which already names topology references, duplicated topology, sidecars, catalog metadata, heap, page cache, direct-I/O buffers, algorithm state, overlays, and scratch bytes.

Architecture consequence: the atlas working-set equations can map to an existing estimate vocabulary, but algorithm implementations and their admission/enforcement paths remain future work. A specification must not mislabel catalog-surface completion as PageRank, WCC, similarity, community, triangle, or FastRP execution.

### 3. External sorting is proven for artifact construction

`src/low_ram.rs::spill_sorted_records_now` sorts a bounded in-memory run, serializes it, clears the buffer, and records the temporary run path. Six construction/verification functions call it, including node-key run construction, edge-source run construction, key resolution, snapshot emission, and snapshot verification.

Architecture consequence: run generation and merge machinery is a credible reusable primitive for deterministic spill plans. This is evidence for the mechanism, not evidence that a graph algorithm automatically becomes efficient when expressed as external sorting. Each algorithm still needs an access-order and I/O-amplification proof.

### 4. Runtime and algorithm storage must remain separate layers

The current graph clusters place the memory-mapped walk runtime and low-RAM builder in one core cluster, while GDS execution/catalog behavior forms a separate cluster. The canonical Cypher plan is called from Bolt and tests but does not own topology.

Architecture consequence: use a five-stage boundary:

```text
Neo4j/Bolt/GDS request
        |
        v
bounded canonical algorithm plan
        |
        v
artifact statistics + budget admission
        |
        v
algorithm-shaped fit/spill/approximate/refuse kernel
        |
        v
bounded stream/stats/sidecar output + receipt
```

The adapter, planner, admission model, kernel, and receipt SHALL have separately testable contracts.

## Atlas Implications

Every retained architecture option needs to answer all of the following:

1. Which immutable topology/property planes it reads and in what order.
2. Which state vectors, frontiers, candidate sets, queues, partitions, or accumulators are resident.
3. Which bytes are reserved before execution and which bytes may be spilled.
4. Which temporary run/partition format permits deterministic restart and bounded cleanup.
5. Which exactness, tie-breaking, convergence, and ordering rules are externally observable.
6. Which graph shape, skew, density, or budget condition forces a different plan or refusal.
7. Which Neo4j/GDS oracle, small independent oracle, and metamorphic property verify it.

## Current Implementation Gap

The current repository gives the final design three useful footholds:

- an immutable memory-mapped adjacency runtime;
- a bounded external-run builder;
- a narrow Cypher/Bolt/GDS compatibility and estimate surface.

It does not yet provide the generalized per-algorithm admission selector, hard runtime memory allocator, forced-spill algorithm kernels, deterministic receipts for those kernels, or differential implementations of the nine required algorithm families. Those are the executable-specification target.


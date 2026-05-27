# Previous Learnings 01

Knight Bus Graph Walker is faster than Neo4j in this benchmark because it turns
the problem into immutable snapshot replay over contiguous adjacency arrays,
while Neo4j is executing general-purpose property-graph traversal queries.

## What Knight Bus Does

- Precompiles the graph into a dual-CSR layout:
  - `forward_offsets + forward_peers`
  - `reverse_offsets + reverse_peers`
- Assigns dense integer node IDs (`u32`) to node keys.
- Stores a sorted key index for exact-key lookup.
- Memory-maps the snapshot, so the OS page cache handles the working set.
- Answers queries by:
  1. exact key -> dense ID via binary search
  2. dense ID -> adjacency slice via offset lookup
  3. optional one extra hop for 2-hop queries
  4. dense IDs -> node keys for output

## Why That Is Fast

- The hot path is mostly array slicing over contiguous memory.
- Reverse traversal is cheap because reverse adjacency is precomputed.
- There is minimal pointer chasing.
- There is no query planner in the hot path.
- There is no transaction engine in the hot path.
- There is no property decoding in the hot path.
- There is no Bolt/network round-trip in the hot path.
- There is no general variable-length path engine in the hot path.
- The workload is fixed to exact `DEPENDS_ON` neighborhood expansion.

## Important Benchmark Detail

- This is not Knight Bus vs Neo4j GDS BFS.
- The Neo4j harness uses Cypher queries over Bolt:
  - `MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m)`
  - `MATCH (n {node_id: $node_id})<-[:DEPENDS_ON]-(m)`
  - `MATCH (n {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(m)`
- So the comparison is against general Cypher traversal on a property graph,
  not against Neo4j GDS on a preprojected in-memory graph.

## Why The Gap Can Be Enormous

- Knight Bus specializes for:
  - immutable graph
  - one relationship semantics
  - exact-key anchor lookup
  - one-hop and two-hop adjacency expansion
- Neo4j is paying for generality:
  - property graph storage
  - Cypher execution
  - server process boundaries
  - row materialization
  - traversal machinery designed for many query shapes, not just this one

## The Fairest Claim

- Knight Bus is dramatically faster for exact-key, fixed-hop `DEPENDS_ON`
  neighborhood replay on a static graph snapshot.
- It is not yet proof that Knight Bus beats every Neo4j algorithm or every
  Neo4j configuration.
- A fairer next benchmark would compare Knight Bus against:
  - Neo4j Cypher with an indexed labeled anchor lookup
  - Neo4j GDS BFS or traversal on a projected in-memory graph

## Best Keywords

- immutable graph snapshot
- dual CSR
- compressed sparse row
- forward adjacency
- reverse adjacency
- memory-mapped snapshot
- exact-key lookup
- dense integer node IDs
- sorted key index
- contiguous memory access
- precomputed reverse edges
- hot path optimization
- low pointer chasing
- OS page cache
- fixed-hop neighborhood expansion
- purpose-built traversal engine
- general-purpose property graph overhead
- Cypher traversal
- Bolt round-trip
- query planner overhead
- materialization overhead
- workload specialization

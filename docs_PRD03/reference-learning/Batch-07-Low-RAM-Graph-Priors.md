# Batch 07: Low-RAM Graph Priors And Architectures

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Low-RAM graph priors`
- `Out-of-core graph execution`
- `CSR / sparse adjacency storage`
- `Memory-budgeted graph projection`

Requirement IDs touched in this batch:

- `REQ-LEARN-015.0`
- `REQ-LEARN-024.0`
- `REQ-LEARN-025.0`
- `REQ-LEARN-026.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-047.0`
- `REQ-LEARN-048.0`

## Scope

This note answers a narrow question:

**Which mirrored GitHub repos actually do less work in RAM for graph-related workloads, and what architectural moves make that possible?**

I treated a repo as a real low-RAM precedent only if it had explicit evidence for one of these:

- disk-backed or out-of-core execution
- CSR or sparse adjacency as the canonical graph shape
- sparse linear algebra as the algorithm substrate
- explicit memory budgets or memory estimators
- deferred materialization that avoids duplicate in-memory copies

I did **not** count repos that merely:

- benchmark graph workloads without a storage story
- run graphs on GPUs or clusters without changing the RAM model
- provide an API wrapper without a distinct memory design

## Quick Read

The strongest direct precedents, in order, are:

1. `GraphChi` - disk-first graph execution on a single machine.
2. `MiniGraph` - out-of-core graph processing with pipelined disk/CPU overlap.
3. `Kuzu` - columnar disk-based storage with CSR adjacency/join indices.
4. `RedisGraph` and `FalkorDB` - sparse adjacency matrices plus GraphBLAS.
5. `GraphBLAS`, `LAGraph`, `python-graphblas`, `sprs` - sparse linear algebra and CSR/CSC graph kernels.
6. `Ligra` and `graph-csr-openmp` - compressed / CSR graph representations and fast conversion.
7. `Neo4j GDS` - not a RAM reducer, but a useful memory-budgeting guardrail.

## Ranked Shortlist

| repo | URL | relevance | commentary | why it matters for the PRD |
| --- | --- | ---: | --- | --- |
| `graphchi-cpp-src` | https://github.com/GraphChi/graphchi-cpp | 97 | Best direct fit. It treats disk as the primary graph store and makes a laptop-sized machine feel much larger. | Strong precedent for single-machine graph work that intentionally avoids full in-memory residency. |
| `minigraph-src` | https://github.com/Vinawx/MiniGraph | 95 | The architecture is blunt in a good way: overlap I/O and compute, then keep the memory manager out of the hot path. | Very close to the PRD's "strict holistic RAM" instinct. |
| `kuzu-src` | https://github.com/kuzudb/kuzu | 92 | Disk-native graph database design with CSR adjacency structures; this is the modern embedded version of the same instinct. | Useful if we want storage-first graph shape without turning the whole system into a distributed service. |
| `redisgraph-src` | https://github.com/RedisGraph/RedisGraph | 90 | Sparse adjacency plus algebraic traversal is the whole game here. | Strong proof that graph traversal can be expressed as sparse matrix operations instead of pointer-chasing. |
| `falkordb-src` | https://github.com/FalkorDB/FalkorDB | 88 | Same sparse-matrix family, but with a more productized property-graph posture. | Good evidence that sparse adjacency is not just an academic trick. |
| `graphblas-src` | https://github.com/DrTimothyAldenDavis/GraphBLAS | 86 | The backend story is sparse linear algebra, not graph-specific plumbing. That is the point. | Valuable as a substrate choice if the PRD wants graph math to stay compact. |
| `lagraph-src` | https://github.com/GraphBLAS/LAGraph | 84 | Algorithm library on top of GraphBLAS, so it inherits the sparse representation advantage. | Good algorithm precedent once the storage substrate is decided. |
| `python-graphblas-src` | https://github.com/python-graphblas/python-graphblas | 82 | The delayed-object API avoids needless intermediate allocations if used carefully. | Shows how to keep high-level syntax without paying for every temporary. |
| `sprs-src` | https://github.com/sparsemat/sprs | 80 | Plain sparse matrix structures in Rust, with CSR/CSC front and center. | Useful if we need a compact Rust-side representation or test fixture oracle. |
| `ligra-src` | https://github.com/jshun/ligra | 79 | Shared-memory graph framework with compressed graph support and CSR-like layout. | Good for compressed representation ideas, though not as aggressive as disk-first systems. |
| `graph-csr-openmp-src` | https://github.com/puzzlef/graph-csr-openmp | 76 | Fast edgelist-to-CSR loader. More loader than runtime engine, but still relevant. | Helps if the PRD wants a compact canonical graph shape before execution begins. |
| `neo4j-gds-src` | https://github.com/neo4j/graph-data-science | 74 | Not a RAM reducer. It is the opposite: a heap-based system with explicit memory estimation. | Useful as a guardrail and an anti-example for the PRD's low-RAM story. |
| `graphblas_sparse_linear_algebra-src` | https://github.com/code-sam/graphblas_sparse_linear_algebra | 72 | Wrapper, not substrate. Still relevant because it exposes GraphBLAS to Rust users. | Good if the PRD wants sparse-linear-algebra access from Rust without bespoke bindings. |

## What The Repos Actually Do

### 1) Disk-first or out-of-core execution

These repos reduce RAM by making the graph live on disk and only pulling in the active working set.

#### GraphChi

Source:

- [`graphchi-cpp-src/README.md:30-34`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphchi-cpp-src/README.md:30>)
- [`graphchi-cpp-src/README.md:107-115`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphchi-cpp-src/README.md:107>)

What it does:

- processes large graphs from disk instead of requiring the whole graph to fit in RAM
- runs vertex-centric programs asynchronously and in parallel
- uses the Parallel Sliding Windows method so the working set is streamed from disk
- explicitly mentions a run on a Mac Mini with 8 GB RAM and SSD

Short evidence snippet:

```text
processing the graph from disk
Parallel Sliding Windows
8 gigabytes of RAM
```

Why this matters:

- this is the clearest single-machine precedent for "less RAM, more disk"
- it is a good fit for PRD thinking that wants graph state to be staged, not globally resident

#### MiniGraph

Source:

- [`minigraph-src/README.md:4-18`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/minigraph-src/README.md:4>)
- [`minigraph-src/README.md:85-108`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/minigraph-src/README.md:85>)

What it does:

- calls itself an out-of-core graph system for a single machine
- pipelines disk reads/writes with CPU work
- says this decouples computation from memory management and scheduling
- stores graphs in binary CSR
- uses `buffer_size` to limit how many fragments can remain resident in memory

Short evidence snippet:

```text
out-of-core system
binary CSR format
buffer_size ... residented in memory
```

Why this matters:

- this is almost a design pattern catalog for the PRD's build-store layer
- the `buffer_size` idea is especially useful because it makes memory a first-class operating parameter

#### Kuzu

Source:

- [`kuzu-src/README.md:22-29`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src/README.md:22>)

What it does:

- uses columnar disk-based storage
- uses columnar sparse row-based (CSR) adjacency list / join indices
- couples that with a vectorized and factorized query processor

Short evidence snippet:

```text
Columnar disk-based storage
Columnar sparse row-based (CSR) adjacency list/join indices
```

Why this matters:

- this is the most directly relevant modern embedded graph-store pattern in the mirror
- it suggests a clean separation between durable storage shape and execution shape

### 2) Sparse adjacency and GraphBLAS-style traversal

These repos reduce RAM by making graph structure sparse by construction and using matrix algebra instead of object-heavy adjacency structures.

#### RedisGraph

Source:

- [`redisgraph-src/docs/docs/design/_index.md:122-165`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redisgraph-src/docs/docs/design/_index.md:122>)

What it does:

- represents graphs as sparse adjacency matrices
- uses one matrix for the general adjacency structure and more matrices for typed relationships / labels
- translates traversal patterns into matrix multiplication
- prefers multiplying sparse intermediates first
- uses GraphBLAS
- current implementation uses CSC format

Short evidence snippet:

```text
sparse adjacency matrices
GraphBLAS
CSC sparse matrix format
```

Why this matters:

- sparse matrices are the RAM story here, not just the query language
- the "multiply sparse intermediates first" rule is an execution-time memory saver as well as a speed trick

#### FalkorDB

Source:

- [`falkordb-src/README.md:39-46`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/falkordb-src/README.md:39>)

What it does:

- positions sparse matrices as the adjacency representation
- uses linear algebra for querying

Why this matters:

- it is a productized proof that the RedisGraph idea survived into a newer graph database line
- useful if the PRD wants a graph-native store whose internal representation is already sparse

#### GraphBLAS

Source:

- [`graphblas-src/README.md:1-18`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphblas-src/README.md:1>)
- [`graphblas-src/README.md:208-216`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphblas-src/README.md:208>)

What it does:

- defines graph algorithms in the language of sparse linear algebra
- applies sparse matrix operations to sparse adjacency matrices
- can build kernels at run time in `GRAPHBLAS_COMPACT` mode to reduce library size

Short evidence snippet:

```text
sparse matrix operations on semirings
applied to sparse adjacency matrices
```

Why this matters:

- this is the most important substrate-level precedent in the set
- the RAM win is mostly implicit: sparse structures are the representation, not an afterthought

#### LAGraph

Source:

- [`lagraph-src/README.md:6-16`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/lagraph-src/README.md:6>)

What it does:

- collects algorithms that use GraphBLAS
- therefore inherits the sparse representation and sparse-operator model

Why this matters:

- use this as the algorithm-layer precedent after GraphBLAS is chosen
- it is less about storage and more about keeping graph kernels in the sparse domain

#### python-graphblas

Source:

- [`python-graphblas-src/README.md:89-96`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/python-graphblas-src/README.md:89>)
- [`python-graphblas-src/README.md:111-115`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/python-graphblas-src/README.md:111>)

What it does:

- creates delayed objects that do no work until the GraphBLAS call is formed
- warns that `.new()` inside a loop creates unnecessary objects
- encourages object reuse outside the loop

Short evidence snippet:

```text
creates a delayed object which does no computation
will create many unnecessary objects if used in a loop
```

Why this matters:

- this is a small but real RAM story: fewer temporaries, fewer live objects
- it is the kind of "don't materialize early" lesson we should steal directly

#### sprs

Source:

- [`sprs-src/README.md:1-34`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/sprs-src/README.md:1>)

What it does:

- provides CSR and CSC matrices plus sparse vectors in pure Rust
- includes sparse matrix/vector products and sparse iterators

Why this matters:

- this is the compact Rust-side data shape we want if the PRD leans into a native implementation
- it is a better fit than dense adjacency lists for low-RAM graph work

### 3) Compressed graph loading and canonical CSR shapes

These repos do not always run out-of-core, but they still reduce RAM pressure by compressing the graph representation before algorithm execution.

#### Ligra

Source:

- [`ligra-src/README.md:1-18`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ligra-src/README.md:1>)
- [`ligra-src/README.md:37-40`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ligra-src/README.md:37>)
- [`ligra-src/README.md:105-129`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ligra-src/README.md:105>)
- [`ligra-src/README.md:167-171`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ligra-src/README.md:167>)

What it does:

- supports compressed graphs and hypergraphs
- offers byte codes, run-length encoding, and nibble codes
- stores CSR offsets in `.idx` and adjacency arrays in `.adj`
- can run the same algorithms on compressed inputs with a `-c` flag

Short evidence snippet:

```text
compressed graphs and hypergraphs
CSR format
byte codes with run-length encoding
```

Why this matters:

- this is a strong precedent for precomputing a compact structural representation before the actual graph kernel starts

#### graph-csr-openmp

Source:

- [`graph-csr-openmp-src/README.md:1-5`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src/README.md:1>)
- [`graph-csr-openmp-src/README.md:95-132`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src/README.md:95>)

What it does:

- optimizes edgelist reading and conversion into CSR
- explicitly discusses memory mapping and out-of-core I/O references

Why this matters:

- if the PRD needs a fast ingestion path into a compact canonical layout, this is the right kind of precedent

### 4) Memory budgeting and guardrails rather than memory reduction

#### Neo4j GDS

Source:

- [`neo4j-gds-src/doc/modules/ROOT/pages/common-usage/memory-estimation.adoc:1-58`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src/doc/modules/ROOT/pages/common-usage/memory-estimation.adoc:1>)
- [`neo4j-gds-src/doc/modules/ROOT/pages/management-ops/graph-creation/graph-project.adoc:1-22`](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src/doc/modules/ROOT/pages/management-ops/graph-creation/graph-project.adoc:1>)

What it does:

- says the graph algorithms library operates completely on the heap
- exposes `.estimate` mode so users can check `requiredMemory` before running
- keeps projected graphs in memory until dropped

Short evidence snippet:

```text
operates completely on the heap
requiredMemory
projected graphs reside in memory
```

Why this matters:

- this is not the low-RAM answer, but it is the honest guardrail answer
- useful as a counterexample: memory estimation is not the same as memory reduction

## Supporting Local Analog

This one is not from the longlist, but it is too relevant to ignore because it shows the same pattern in a different domain.

### NornicDB

Source:

- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/nornicdb-src/docs/architecture/indexing-memory-large-datasets.md`

What it does:

- writes vectors to an append-only `.vec` file
- keeps only `id -> offset` in RAM
- builds HNSW from the file in chunks
- avoids a duplicate vector copy during search
- bounds peak RAM by chunk size plus the offset map

Why it matters:

- this is the cleanest file-backed / lookup-backed memory reduction pattern in the local mirror
- even though it is not a graph kernel repo, the storage lesson maps cleanly to graph sidecars and embeddings

## Excluded From The Shortlist

These are present in the mirror, but I did not count them as direct low-RAM graph precedents:

| repo | reason excluded |
| --- | --- |
| `gapbs-src` | benchmark suite and reference kernels, not a storage architecture. |
| `graphscope-src` | distributed graph platform with an in-memory manager; it scales out, but it does not primarily minimize RAM per graph. |
| `cugraph-src` | GPU-first graph analytics; different memory model. |
| `timely-dataflow-src` | valuable for backpressure and resource control, but not a graph storage precedent. |
| `graphblas-pointers-src` | pointer/index of papers and implementations, not an implementation path by itself. |

## Pattern Extraction

The repeatable low-RAM moves across the shortlist are:

1. **Push the graph onto disk early.**
   - GraphChi, MiniGraph, Kuzu, and the NornicDB analog all do this.
   - The active working set becomes much smaller than the graph itself.

2. **Make CSR or sparse adjacency the canonical shape.**
   - Ligra, sprs, graph-csr-openmp, RedisGraph, FalkorDB, and Kuzu all converge here.
   - This avoids dense representation blowups and pointer-heavy adjacency storage.

3. **Move graph math into sparse linear algebra.**
   - GraphBLAS, LAGraph, RedisGraph, and FalkorDB all show this path.
   - Traversal becomes matrix multiplication or masked sparse operations instead of object churn.

4. **Budget memory explicitly.**
   - MiniGraph's `buffer_size` and Neo4j GDS `requiredMemory` are the clearest examples.
   - Even when a system is still in-memory, the budget becomes observable.

5. **Delay materialization.**
   - python-graphblas demonstrates that delayed objects and object reuse matter.
   - NornicDB shows the same principle with file-backed vector lookup.

6. **Separate ingestion shape from execution shape.**
   - graph-csr-openmp, Ligra compression, and Kuzu's columnar disk layout all do this.
   - That separation is where the RAM savings usually appear.

## What I Think We Should Steal For Knight Bus

If the PRD wants a real low-RAM outcome, the strongest borrowable pattern stack is:

- **Disk-backed graph facts**
  - do not keep the entire graph in live heap state

- **CSR or sparse adjacency as the canonical structural layout**
  - use one compact structural form for both build and query

- **Sparse-linear-algebra kernels for traversal**
  - avoid custom pointer-chasing if the same traversal can be expressed as sparse ops

- **Explicit memory budgets and estimates**
  - make "fit" something we can ask before execution, not something we discover by OOM

- **Delayed materialization for intermediate objects**
  - do not create transient graph objects unless a kernel needs them

- **Chunked file-backed sidecars for heavyweight auxiliary state**
  - embeddings, labels, or history should not force a second full resident copy

## Bottom Line

The repos that really reduce RAM are the ones that treat graph state as:

- a disk-backed working set,
- a sparse structural representation,
- or a sparse algebra problem.

The ones that only estimate memory are still useful, but they are not the same class.

For the PRD, the highest-confidence path is:

1. pick a compact canonical graph shape, ideally CSR-like,
2. keep the working set bounded or file-backed,
3. run graph kernels through sparse algebra or frontier-based access,
4. and make memory budgets explicit enough that the system can refuse work before it OOMs.

## References

- [GraphChi README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphchi-cpp-src/README.md:30>)
- [MiniGraph README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/minigraph-src/README.md:4>)
- [Kuzu README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/kuzu-src/README.md:22>)
- [RedisGraph design](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/redisgraph-src/docs/docs/design/_index.md:122>)
- [FalkorDB README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/falkordb-src/README.md:41>)
- [GraphBLAS README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphblas-src/README.md:1>)
- [LAGraph README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/lagraph-src/README.md:6>)
- [python-graphblas README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/python-graphblas-src/README.md:89>)
- [sprs README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/sprs-src/README.md:1>)
- [Ligra README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ligra-src/README.md:1>)
- [graph-csr-openmp README](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graph-csr-openmp-src/README.md:1>)
- [Neo4j GDS memory estimation](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src/doc/modules/ROOT/pages/common-usage/memory-estimation.adoc:1>)
- [Neo4j GDS graph projection lifecycle](</Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src/doc/modules/ROOT/pages/management-ops/graph-creation/graph-project.adoc:1>)

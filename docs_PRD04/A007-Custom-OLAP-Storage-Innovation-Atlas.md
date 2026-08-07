# A007 Custom OLAP Storage Innovation Atlas

Date started: 2026-08-06
Status: architecture synthesis complete; benchmark validation pending
North star: `A007-spc-founder-interview-prep-v7.md`
Companion: `Algorithm-Storage-Decision-Analysis.md`

## Purpose

Design a portfolio of custom OLAP storage formats and execution-state formats
for graph algorithms. The objective is not one universal graph layout. The
objective is a set of logically exact or explicitly approximate physical plans
that expose useful Pareto choices among:

- peak whole-process RAM;
- latency and time to first result;
- temporary I/O and retained disk;
- build and freshness cost;
- result materialization;
- and predictability under a hard resource budget.

Every architecture in this atlas must explain which bytes or operations it
removes, which state variable it bounds, what new cost it creates, and which
measurement would falsify it.

## Executive Answer

The recommended design is an **artifact portfolio compiler**:

```text
canonical snapshot
      |
      v
profile the graph + read the workload contract
      |
      v
compile only the algorithm-shaped physical views that earn their cost
      |
      v
run inside a fixed state capsule
      |
      v
answer + estimate-versus-actual receipt
```

The storage innovation is not one exotic replacement for CSR. It is five
forms of specialization used together:

1. **semantic compression:** persist quotients and answers, such as a WCC map,
   condensation DAG, level tape, core vector, or route index;
2. **state deletion:** stream random walks, recompute Brandes predecessors,
   and stream Louvain contractions instead of retaining large intermediates;
3. **local shape adaptation:** choose inline, bitpacked, Roaring, hub-page, or
   weight-lane encodings per row/block;
4. **safe pruning:** attach exact upper bounds and membership summaries so
   blocks that cannot affect an answer stay unread;
5. **resource virtualization:** make concurrency, frontier size, bucket size,
   feature batches, and page residency explicit budget dials.

The largest plausible reductions are in selected algorithm state, not a
blanket whole-system multiplier:

- reachability can remove most of the roughly `32*V` named GDS BFS dense-array
  recipe when the contract asks only for reach or parent/distance;
- specialized PageRank can replace Pregel message state with two score vectors,
  roughly `1.5x` less named payload in f64 or `3x` in an approved f32 mode;
- streamed Node2Vec deletes the `8*V*walks*length` walk matrix entirely;
- recomputed Brandes predecessors deletes an `O(E)` term per admitted source
  lane at the cost of another edge scan;
- strict community contraction removes simultaneous resident old/new CSR
  levels at the cost of sorted-run I/O.

These are architecture estimates awaiting benchmarks. Some `RACE` plans may
also beat GDS latency by touching fewer bytes. `STRICT` plans will often be
slower but will fit a hard RAM envelope. That visible Pareto choice is the
product.

The first implementation should remain narrow: security/dependency reach,
WCC, optional shortest path, four resource profiles, GDS parity, and receipts.
PageRank and Node Similarity are the next architecture proofs; embeddings and
training come later.

This atlas makes no patent or first-in-literature claim. Most primitives are
established. The proposed innovation is their algorithm-specific composition
behind an enforceable, portable workload contract.

## A007 Constraint

Creative storage is useful only when it strengthens the A007 product:

```text
graph artifact + high-stakes question + declared hard budget
  -> enumerate custom physical plans
  -> quote the nondominated RAM/latency choices
  -> FIT | SPILL | APPROXIMATE | REFUSE
  -> enforce the selected plan
  -> return answer + estimate-versus-actual receipt
```

## Research Inputs

1. All relevant artifacts under `docs_PRD06/graph-learning/`.
2. Neo4j GDS source under
   `gitrefrepo/Neo4j family/neo4j-gds-src/`.
3. `code-graph-mcp` structural search over GDS algorithms, estimators, kernels,
   and execution state.
4. The evidence and constraints already reconciled in
   `docs_PRD04/Algorithm-Storage-Decision-Analysis.md`.

## Epistemic Labels

- `Source observed`: directly supported by local source or a cited primary
  artifact.
- `Derived`: arithmetic from explicit `V`, `E`, dimension, precision, or block
  assumptions.
- `Modeled`: plausible performance/resource estimate awaiting a benchmark.
- `Speculative`: creative mechanism whose correctness or economics need a
  prototype.
- `Rejected`: fails first-principles or rubber-duck review.

## Research Ledger

| Input | Status | Intended contribution |
| --- | --- | --- |
| PRD06 graph-learning corpus | Read, high-value subset | Mechanism library spanning graph kernels, compression, search, ANN, storage engines, dataflow, and verification |
| Neo4j GDS code graph | Indexed twice | 4,921 files and 38,262 nodes in `code-graph`; 54,265 nodes and 284,022 edges in `codebase-memory-mcp` |
| A007 companion analysis | Read | Customer order, four architecture profiles, evidence corrections, and verification contract |
| GDS memory estimators | Read, representative kernels | 48 concrete `MemoryEstimateDefinition` implementations; state ingredients extracted for every major physical signature |
| First-principles arithmetic | Complete as model | Converts physical layouts into RAM, I/O, build, output, and generation-overlap terms |
| Rubber-duck review | Complete as design review | Hidden state, invalid exactness assumptions, and latency costs are called out with falsifiers |
| ASCII validation | Passed | Editorial checker passed with the long-form document width profile |

## Governing Design Rules

1. Canonical data is rebuild truth, not the universal serving layout.
2. Every supported algorithm family gets at least one custom OLAP artifact.
3. Multiple physical plans are expected when RAM and latency objectives differ.
4. Approximation is a public capability, never a silent implementation detail.
5. `mmap` bytes, page cache, state, output, spill, build overlap, and retained
   generations all count.
6. A compressed topology does not solve unbounded algorithm state.
7. A precomputed answer is a legitimate storage format for a stable repeated
   question.
8. Build cost and freshness lag belong beside query latency.
9. No architecture earns a number without a formula or benchmark receipt.
10. The first proof remains A007's security/dependency traversal plus WCC.

## Running Observations

### Observation 1: GDS already proves that algorithms do not share one state shape

The GDS source does not estimate "graph algorithm memory" as one generic
quantity. It contains 48 concrete estimator implementations. Representative
source-observed state includes:

- BFS: visited bits, several `long[V]` and `double[V]` arrays, result nodes,
  and thread-local node lists whose upper bound depends on `min(E, C*V)`;
- WCC: an atomic disjoint-set structure;
- PageRank and HITS: Pregel node values plus vote bits and send/receive
  message arrays;
- delta-stepping: distance and predecessor arrays plus shared and local bins
  with a loose upper bound that can reach `E` or `C*V`;
- Node Similarity: vector copies, optional weights, component state, and
  top-k or top-N structures;
- Louvain: modularity state, a newly contracted CSR graph at each level, and
  one or more dendrogram arrays;
- Betweenness: centrality plus per-thread predecessor lists, traversal order,
  deltas, sigmas, distances or a priority queue;
- Node2Vec: all random walks, sampling distributions, and two embedding
  matrices;
- FastRP: three full embedding matrices;
- SCC and low-link algorithms: multiple node arrays plus traversal stacks
  whose loose upper bound can scale with `E`;
- k-core and coloring: node arrays plus work structures replicated by
  concurrency;
- KNN: top-k lists and four temporary neighbor-list families;
- GraphSAGE: full feature matrices plus concurrent sampled batches.

That is direct code evidence for the thesis in this document: the important
optimization target is not Java object overhead alone. It is the mathematical
shape and lifetime of each algorithm's state.

### Observation 2: the corpus repeatedly uses seven physical moves

The PRD06 corpus found the same moves in storage, graph analytics, full-text
search, ANN, dataflow, and databases:

1. sorted arrays plus offsets;
2. immutable generations plus merge;
3. the smallest encoding that matches local data shape;
4. safe skip or prune bounds;
5. signed deltas as the unit of change;
6. partitioning as a performance decision;
7. an equality relation designed per algorithm.

The creative opportunity is not to invent a mysterious eighth primitive. It
is to compose these seven differently for each graph kernel and expose the
composition as a resource contract.

### Observation 3: compressed topology and bounded execution are different

GDS already has compressed and packed adjacency implementations. Therefore,
"compress CSR" is not a sufficient differentiation claim. Knight Walker must
also remove or bound state terms such as predecessor lists, random-walk
materialization, per-thread `V`-sized maps, full contracted graphs, and
multiple feature matrices.

### Observation 4: predictable RAM requires ownership of paging

An mmap file may have tiny heap usage and still produce large resident memory
through page cache. Strict mode therefore cannot define RAM as allocator bytes.
It needs a controlled resident window, fixed buffers, bounded readahead, and
measurement of RSS, mapped resident pages, page faults, and kernel I/O. The
strictest plan may use direct or explicitly evicted I/O; the balanced plan may
let the OS cache help but must quote a larger residency envelope.

### Observation 5: the most valuable custom format may be an answer

For stable, repeated questions, a component map, condensation DAG, BFS level
tape, core-number vector, triangle-count vector, or community assignment is a
better serving artifact than a faster graph representation. Build latency and
freshness become the price; query RAM and latency collapse.

## Algorithm Surface

### Code-backed inventory

The following table groups the 48 source-observed estimators by the state
transition they price. The grouping is a proposed Knight Walker physical-plan
boundary, not a claim about GDS module ownership.

| Physical signature | GDS estimator-backed algorithms | Dominant state risk |
| --- | --- | --- |
| Frontier traversal | BFS, DFS | visited, result, frontier and per-thread replication |
| Components | WCC, SCC | parent/component arrays; SCC stacks |
| Low-link structure | Bridges, Articulation Points | discovery/low arrays and potentially edge-sized event stack |
| Iterative rank | PageRank, HITS | multiple dense vectors and message arrays |
| Weighted paths | Dijkstra, Delta Stepping, Bellman-Ford, A*, Yen's | distances, predecessors, queues/buckets, duplicate entries |
| Tree and route construction | Spanning Tree, Steiner Tree, Prize Steiner Tree | parent/cost arrays plus path frontier |
| Community optimization | Louvain, Leiden, Modularity Optimization, Modularity Calculator | labels, aggregates, contracted graph, dendrograms |
| Local voting and coloring | Label Propagation, Speaker-Listener LPA, K1 Coloring, ApproxMaxKCut | labels plus per-thread vote/color state |
| Peeling | K-Core Decomposition | degree/core arrays and rebuild queues |
| Set similarity | Node Similarity, Filtered Node Similarity | copied neighbor vectors, candidate pairs, top-k output |
| Vector neighborhood | KNN, Filtered KNN | multiple temporary neighbor lists and persistent top-k lists |
| Motifs | Intersecting Triangle Count, Local Clustering Coefficient | sorted adjacency intersections and per-node counts |
| Random walks | Random Walk, Random Walk Counting Visits, Node2Vec | walk materialization, sampling tables, embedding matrices |
| Embeddings and aggregation | FastRP, HashGNN, GraphSAGE, GraphSAGE Train | `V*D` feature matrices and sampled batches |
| Dense feature clustering | KMeans, Scale Properties | feature scans, assignments, centroids, distances |
| Influence | CELF | repeated active sets, spread arrays, priority queue, simulations |
| Topology transformation | To Undirected, Inverse Relationships | a second graph-sized topology |
| Exposure | Indirect Exposure | propagation state and result vectors |
| ML pipeline support | Approximate Link Prediction, Split Relationships | candidate/index state and train/test split materialization |

Algorithms visible elsewhere in GDS specifications and procedures, including
degree/eigenvector or ArticleRank variants, topological/longest-path forms,
all-pairs paths, and graph transforms, map to the same signatures. They do not
require a new storage religion; they require a declared artifact/state recipe.

### Why signatures, not 48 independent engines

The product should expose an algorithm-specific plan, but implementation reuse
should happen below that API:

```text
algorithm request
      |
      v
+----------------------------+
| semantic contract          |
| exact / approximate / k    |
+-------------+--------------+
              |
              v
+----------------------------+
| physical signature         |
| frontier / rank / join ... |
+-------------+--------------+
              |
        +-----+-----+
        |           |
        v           v
 sealed artifact   state capsule
 exact byte size   fixed byte limit
        |           |
        +-----+-----+
              |
              v
    receipt-producing kernel
```

This lets BFS and random walk share adjacency decoding while still giving BFS
a bitmap frontier and random walk a sampling table. "Custom" means the whole
recipe is custom; it does not mean duplicating every codec.

## Innovation Families

### I1. Artifact portfolio compiler

Do not make one stored graph answer every question. Compile the canonical edge
snapshot into a small portfolio selected by the workload declaration:

```text
                        CANONICAL SNAPSHOT
                      rebuild and audit truth
                               |
                   +-----------+-----------+
                   | workload declarations |
                   +-----------+-----------+
                               |
                               v
                    +----------------------+
                    | portfolio compiler   |
                    | measure, cost, prune |
                    +----------+-----------+
                               |
          +--------------------+--------------------+
          |                    |                    |
          v                    v                    v
   reach artifact       rank artifact       similarity artifact
   push rows             pull tiles          rare-first postings
   selective transpose  normalized weights  prefix bounds
```

The compiler is similar to a database index advisor, a tensor compiler's
layout selection, and partial evaluation. It knows the algorithm in advance,
so it is allowed to pay build cost to remove runtime generality.

**Why RAM falls:** unused orientations, properties, widths, indexes, and
general-purpose metadata do not enter the runtime artifact.

**Why predictability rises:** every sealed artifact has an exact file length,
block directory, codec histogram, checksum, and measured decode bound.

**Limit:** many declared workloads can recreate a universal database by
accident. The compiler needs a storage budget and must show the marginal cost
of each extra artifact.

### I2. Shape-shifting adjacency

Roaring bitmaps choose an array, bitmap, or run container per 65,536-value
chunk. Apply that principle to adjacency per degree/locality class:

| Row shape | Encoding | Best operation |
| --- | --- | --- |
| degree 0-7 | inline varints in row directory | tiny traversal, no extra pointer |
| sparse sorted row | delta-bitpacked blocks | sequential iteration |
| dense local row | roaring/run containers | intersection and membership |
| very high-degree hub | paged bitmap plus sparse exceptions | pull tests, set algebra |
| weighted row | target deltas plus parallel weight lane | optional weight decode |

The artifact stores the chosen codec tag with each row or row block. The
decision is made from measured encoded bytes and the declared operations, not
from one global degree threshold.

**Transfer:** Roaring's per-container choice plus Lucene's per-block bit width
plus WebGraph/Ligra adjacency compression.

**Limit:** branchy codec dispatch can hurt a full scan. Rows should be grouped
by codec into runs so the hot loop remains monomorphic for thousands of rows.

### I3. Selective transpose instead of universal CSC

Direction-optimized traversal normally stores both out-CSR and in-CSR, nearly
doubling topology. But pull is useful only for dense frontiers and only where
incoming probes have a good chance of early success.

Build inbound tiles only for partitions whose degree/frontier model predicts
benefit. Other partitions remain push-only. A third mode streams a
destination-sorted edge tile when the frontier is dense but not worth a
permanent transpose.

```text
sparse phase             dense phase                cold partition
push out-rows            pull hot in-tiles          scan edge tile
    |                         |                           |
    +----------- runtime density chooser ----------------+
                              |
                              v
                       next frontier bitmap
```

**Derived storage effect:** if a fraction `h` of edges belongs to pull-worthy
tiles, the second orientation costs approximately `h*E*w_id + tile_index`, not
`E*w_id + (V+1)*w_offset`. At `h=0.2`, the neighbor-ID part of the transpose is
about 80% smaller.

**Limit:** a graph or source set that makes every partition dense drives
`h -> 1`; the compiler must then choose full CSC or admit slower scans.

### I4. Proof-carrying blocks

Borrow block-max WAND's exact pruning form. Every adjacency/posting block can
carry cheap summaries:

- minimum and maximum target ID;
- cardinality and degree range;
- relationship-type mask;
- weight minimum/maximum;
- component or partition range where valid;
- Bloom/Ribbon membership filter;
- algorithm-specific upper bound, such as maximum possible similarity
  contribution.

A block is skipped only if the summary proves it cannot affect the answer.
False positives cost reads; false negatives are forbidden. This is exact
pruning, not approximation.

**Why RAM can fall:** the runtime avoids building large candidate sets and can
leave skipped blocks nonresident.

**Limit:** summaries with weak bounds add bytes and branches without pruning.
The receipt must report blocks considered, blocks skipped, and bound hit rate;
the compiler should omit summaries that fail a measured benefit threshold.

### I5. Fixed state capsules

Each physical plan declares every mutable region before execution:

```text
+--------------------------------------------------------------+
| STATE CAPSULE: immutable allocation after admission          |
+----------------------+---------------------------------------+
| node state           | exact-width arrays / mmap windows     |
| frontier or buckets  | fixed ring + bounded spill channels  |
| worker scratch       | one slab per admitted worker          |
| output               | bounded top-k or streaming sink       |
| I/O buffers          | registered, block-aligned buffers     |
| counters             | receipt and cancellation telemetry    |
+----------------------+---------------------------------------+
```

No hot-path `Vec` growth, hash-table rehash, or worker multiplication is
allowed outside the capsule. When a bounded region fills, the plan follows a
declared action: spill a run, compact a frontier, reduce concurrency,
approximate, or refuse.

**Transfer:** real-time arenas, database work memory, GPU tensor planning, and
io_uring registered buffers.

**Limit:** arena reservation is not the same as physical residency. Receipts
must separately report virtual reservation and resident peak.

### I6. Width calculus

Use the narrowest exact integer representation proved by artifact metadata:

- node IDs: `ceil(log2(V))/8`, practically u24 or u32 until `V > 2^32`;
- component/community IDs: based on count, often u8/u16/u24;
- degree/core/color: based on measured maximum, not `V`;
- BFS level: u16/u24 if diameter bound allows, with overflow side table;
- weights and scores: f32 where the public equivalence policy permits it,
  otherwise f64;
- exceptions: a sparse side table for values that overflow the common width.

This is the Frame-of-Reference idea from column stores: encode the common
case tightly and keep rare outliers out of the hot lane.

**Limit:** narrowing is exact only with checked overflow and a side lane.
Floating-point narrowing is approximate unless an exact refinement phase
restores the declared equivalence relation.

### I7. Frontier and bucket virtualization

Queues are often the least predictable term. Replace an unbounded object
queue with one of four declared representations:

1. sparse sorted IDs in a fixed vector;
2. dense Roaring/bitset frontier;
3. circular delta-bucket window plus immutable spill runs;
4. partitioned mailboxes with fixed credit per partition.

The runtime converts only at density boundaries with hysteresis. A full
frontier no longer means `E` accidental queue entries; duplicates are reduced
through a bitmap, generation tag, or stale-entry guard before entering the
next representation.

### I8. Streaming contraction

Louvain-like algorithms create a smaller graph after each level. Building the
new graph beside the old one produces a peak-overlap bill. Instead:

1. stream old edges as `(community_u, community_v, weight)` tuples;
2. sort fixed-memory runs;
3. merge and sum duplicates into a new sealed artifact;
4. atomically publish the new level;
5. retire the old level before admitting the next optimization pass.

This is external sort plus LSM compaction applied to graph contraction.

**Why RAM becomes predictable:** the only variable memory is the declared run
buffer and merge fan-in. Disk and build latency rise.

### I9. Precision staircase with exact landing

Use low precision to get near a fixed point, then finish under the public
precision policy:

```text
early iterations       middle iterations       acceptance iterations
int/f16 or bf16   ---> f32 deterministic  ---> f64 fixed order
cheap bandwidth        tighter residual         endpoint equivalence
```

This borrows mixed-precision iterative refinement from numerical linear
algebra. It is attractive for PageRank, HITS, FastRP, and GNN inference.

**Important:** low-precision convergence alone is approximate. The claim of
endpoint equivalence is earned only if final high-precision iterations pass a
residual and differential oracle. Some ill-conditioned problems will not;
those must stay on the full-precision plan.

### I10. Recompute instead of remember

Memory is sometimes cheaper to remove than to compress. Brandes betweenness
stores predecessor DAGs so the backward pass is fast. A strict-RAM plan can
store distances and traversal order, then rescan incoming edges during the
backward pass to rediscover exact predecessors.

This is checkpointing/recomputation from automatic differentiation: trade
extra compute and I/O for a smaller live state.

**Limit:** weighted floating-point predecessor equality needs a pinned
tolerance and summation policy. Exact integer weights are the clean first
implementation.

### I11. Deterministic regeneration instead of materialization

Node2Vec's GDS estimator explicitly prices all random walks. A counter-based
RNG can derive every random choice from `(snapshot, seed, node, walk, step)`.
Walks can then flow directly into the trainer and be regenerated exactly on
retry; no `V*walks*length` walk matrix is needed.

This borrows counter-based simulation and stateless procedural generation.

**Limit:** training order changes floating-point results. A reproducible mode
must pin walk order, worker assignment, and reduction order; a throughput mode
may instead promise distributional quality.

### I12. Quotients, skeletons, and answer artifacts

Many graph questions do not need the original graph after preprocessing:

- WCC -> default giant component plus sparse exceptions;
- SCC -> component map plus condensation DAG;
- bridges/articulation -> block-cut tree;
- k-core -> core-number vector plus shell offsets;
- repeated BFS root -> level tape and parent tape;
- stable PageRank -> score vector plus sorted top-k;
- repeated shortest path -> contraction hierarchy, landmarks, or hub labels;
- influence maximization -> reverse-reachable-set coverage index.

The mathematical quotient is smaller because it removes distinctions the
question cannot observe. This is not generic compression; it is semantic
compression.

### I13. Stateful freshness as a separate artifact family

Not every custom artifact should be incrementally maintained. Declare one of:

| Freshness mode | Mechanism | Best fit |
| --- | --- | --- |
| immutable epoch | rebuild and root flip | stable or batch inputs |
| base plus delta | read base, then signed overlay | moderate updates |
| standing arrangement | differential `(data,time,diff)` traces | repeated query, high update rate |
| answer invalidation | mark stale and rebuild | expensive derived answers |

Insert-only WCC can update a DSU cheaply; deleting a bridge can alter a large
region. The freshness policy must be algorithm-specific too.

### I14. Receipts as feedback control

Every run records predicted and actual bytes by category, pages read, decode
bytes, spill bytes, frontier maxima, candidate counts, iterations, and output
size. The next quote uses the same artifact's historical error distribution.
This turns estimation into a calibrated control system rather than a static
formula.

## Custom Algorithm Architectures

The ranges below are architecture hypotheses, not benchmark results. A ratio
such as `2-4x less state` compares the explicit state ingredients in the GDS
estimators with the proposed state recipe, excluding the graph projection and
unbounded output unless stated otherwise. Each claim is tagged `Derived` when
it follows directly from widths and `Modeled` when workload behavior matters.

### A1. BFS, DFS, reachability, and bounded blast radius

#### Source-observed GDS state

The BFS estimator includes a visited bitset, `traversedNodes`, `weights`,
`minimumChunk`, `resultNodes`, chunk metadata, and local node lists. Before
locals, these named arrays are approximately `32.125*V` bytes if the huge
long/double arrays behave as eight-byte payloads. The local-list upper bound
is `min(E, C*(V-1))` entries.

#### Custom artifacts

**A1-SPEED: dual direction artifact**

- shape-shifting out adjacency;
- full or selective inbound tiles;
- sparse ID frontier plus dense bitmap frontier;
- u32 parent or distance vector only when requested;
- optional landmark/component prefilter.

State for boolean reachability can be `visited bitset + two frontier bitmaps`,
approximately `0.375*V` bytes, plus sparse-frontier capacity. With a u32
distance/parent result it becomes about `4.375*V`. At `V=100M`, those terms
are about 37.5 MB and 437.5 MB respectively. `Derived`.

**A1-STRICT: blocked frontier walk**

- out rows grouped into fixed disk blocks;
- visited as mmap-able Roaring chunks with a bounded hot-container cache;
- frontier partitioned by target block and emitted as sorted immutable runs;
- only `R` run buffers and `P` page buffers resident;
- no permanent transpose.

Peak RAM becomes `M_visited_hot + R*run_buffer + P*page_size + output_window`,
independent of `E`. Latency may be `2-20x` warm-RAM traversal when the graph
does not fit and each hop rereads cold blocks. `Modeled`; topology and frontier
locality dominate.

**A1-ANSWER: level tape**

For a stable repeated root or root set, persist:

```text
level_offsets: [0, count(L0), count(L0)+count(L1), ...]
level_nodes:   node IDs grouped by BFS level
parent_lane:   optional parent delta per level
membership:    optional compressed reached set
```

Reachability, distance, and progressive result delivery become scans over an
answer artifact. Query state is a few pages; freshness is rebuild-bound.

#### Creative exact pruning

For a directed graph, first map source and target to SCCs and ask reachability
on the condensation DAG. If topological intervals or landmark labels prove
"no path," no base edge is read. A positive filter result falls back to exact
traversal unless a complete reachability label is stored.

#### Expected tradeoff versus GDS

| Plan | Peak algorithm state | Latency expectation | Exactness |
| --- | --- | --- | --- |
| A1-SPEED | `4-12x` lower for common reach/distance contracts | `0.7-1.5x` GDS after artifact build | exact |
| A1-STRICT | fixed by page/frontier credits | `2-20x` slower if I/O-bound | exact |
| A1-ANSWER | page-window only | `10-1000x` faster repeated query | exact at snapshot |

Falsifier: if BFS semantics require all GDS result arrays simultaneously, the
state reduction narrows. The first implementation should expose separate
`exists`, `nodes`, `distance`, and `path` contracts rather than carry every
possible result.

### A2. WCC and connected-component serving

#### Custom artifacts

**A2-SPEED: narrow DSU**

- u32 parent array when `V < 2^32`;
- u8 rank/size class or union-by-min without a rank lane;
- path compression in fixed partitions;
- Afforest-style first-neighbor sample and giant-component skip.

State is approximately `4-5*V` bytes instead of a general atomic-long DSU
family. Expected state reduction is `1.5-3x`, depending on GDS DSU layout.
`Modeled` because the exact huge-DSU implementation overhead was not reduced
to one payload formula here.

**A2-STRICT: shard, quotient, merge**

1. partition vertices into `P` ranges;
2. run a local DSU for one partition at a time;
3. emit only boundary component edges;
4. solve WCC on the much smaller boundary quotient;
5. stream final labels into the result artifact.

Peak state is `O(V/P + E_boundary)` instead of `O(V)`. Extra edge passes and
boundary skew cost latency. A bad partition on a power-law graph can make
`E_boundary` approach `E`; quote it from an exact partition scan before run.

**A2-ANSWER: giant-default component map**

If one component contains fraction `g` of vertices, encode its ID as the
default and store only exceptions:

```text
component(node):
  if exception_bitmap[node] == 0 -> GIANT_ID
  else lookup exception rank -> exception_component_ids[rank]
```

For `g=0.999`, the exception IDs are roughly `0.001*V*w_component` plus a
compressed bitmap, instead of `V*w_component`. This can be orders of magnitude
smaller than a dense result vector. `Derived`; result distribution controls it.

#### Incremental policy

Insert-only updates can union components in an overlay. Deletes cannot in
general split a DSU cheaply. A deletion either uses a dynamic-connectivity
structure with greater complexity or marks affected components stale and
rebuilds. The honest MVP is insert-fast, delete-rebuild.

### A3. SCC, bridges, and articulation points

These algorithms share a hard problem: stack depth or event count can be much
larger than the friendly `V`-sized arrays. The GDS SCC estimator gives its
`todo` stack a loose bound up to `max(V,E)`; articulation points price stack
events directly from relationship count.

#### A3.1 SCC: trim-core plus condensation artifact

**Plan SCC-TRIM**

1. repeatedly peel nodes with zero in-degree or zero out-degree;
2. record the peel order as a compressed tape;
3. run SCC only on the remaining cyclic core;
4. reconstruct component labels from the tape;
5. emit the condensation DAG immediately.

Many dependency, citation, and workflow graphs are nearly DAGs. If cyclic-core
fraction is `q`, expensive SCC state and random work apply to `q*V` and its
induced edges. On a fully strongly connected graph `q=1` and the trim is pure
overhead. The artifact compiler can measure `q` cheaply and omit the plan.

**Plan SCC-STRICT**

- u32 discovery/component arrays where legal;
- compact DFS frames `(node,next_edge_offset)` in an append-only stack file;
- a fixed hot-stack window;
- boundary checkpoints at adjacency-block edges;
- result emitted as default component plus exceptions where skew permits.

The edge-sized stack no longer becomes resident. Expected peak-state reduction
is `1.5-10x` on cases where GDS's event stack grows far beyond `V`; sequential
spill can make latency `1.2-4x` slower. `Modeled`.

#### A3.2 Bridges: spanning-tree chord coverage

Build a spanning forest, assign Euler intervals, and treat every non-tree edge
as a chord that covers the tree path between its endpoints. A tree edge is a
bridge exactly when no chord covers it. With an LCA structure and difference
accumulation, the algorithm can use:

- parent and depth lanes;
- Euler order;
- compressed non-tree chord endpoint stream;
- one coverage accumulator per tree node.

This replaces a general object event stack with flat arrays and sequential
chord blocks. The output can be a compressed list because bridges are often
sparse.

#### A3.3 Articulation points: block-cut artifact

Use the same forest and low-link evidence to emit a bipartite block-cut tree:
one node kind for articulation vertices, one for biconnected blocks. Repeated
resilience queries then run on the smaller tree. The build still needs exact
low-link semantics; the win is predictable flat state and a reusable answer
artifact, not magical avoidance of the first analysis.

Falsifier: verify bridge and articulation outputs against GDS under adversarial
deep paths, parallel edges, self-loops, disconnected graphs, and vertex IDs
near width boundaries.

### A4. PageRank, HITS, eigenvector, ArticleRank, and indirect exposure

#### Source-observed GDS state

PageRank calls Pregel with one double node value. The shared Pregel estimator
adds vote bits and a reducing messenger with send and receive double arrays.
The named payload is therefore approximately `24.125*V` bytes before object
and paging overhead. HITS has two double node values and the same two message
arrays, approximately `32.125*V`.

#### A4-SPEED: normalized pull tape

Compile inbound edges into destination-major blocks and store each source's
normalized contribution beside or derivable from the edge:

```text
rank_old[source] ---> [target-sorted contribution blocks] ---> rank_new[target]
                          sequential read                    private tile sums
```

- no general Pregel messenger;
- two dense f64 vectors for strict numeric parity: `16*V` bytes;
- or two f32 vectors for a tolerance-approved mode: `8*V`;
- one tile-local reduction buffer per worker, not one full vector per worker;
- fixed reduction order for reproducibility.

Against the named GDS state, the f64 PageRank plan is about `1.5x` smaller and
the f32 plan about `3x` smaller. HITS uses four rolling vectors in the simple
form (`16*V` f32 or `32*V` f64); fusion can keep authority and hub lanes in one
pair of blocks. `Derived` for payload, not whole-process RSS.

Latency can improve because the kernel moves fewer bytes and avoids atomic
messages. A reasonable hypothesis is `0.5-1.0x` GDS compute time for a warm,
fully resident specialized artifact, not because Rust is inherently faster
but because the physical plan is narrower. This requires measurement.

#### A4-BALANCED: precision staircase

Run early iterations in f16/bf16 or quantized block format, switch to f32 as
the residual shrinks, and finish in deterministic f64 until the same residual
policy as the reference passes. This may reduce early-iteration bandwidth by
`2-4x` while preserving the final contract when iterative refinement succeeds.

Refuse the staircase when:

- the residual stalls after promotion;
- rank order near the requested top-k is unstable;
- a condition proxy exceeds its calibrated region;
- the differential oracle exceeds tolerance.

#### A4-STRICT: 2D destination tiles

Partition the adjacency matrix by `(source_block,destination_block)`. Keep one
destination score tile and one source-rank window resident, stream edge tiles,
and append partial sums. A deterministic merge produces the next rank vector.

Peak RAM is `O(B_source + B_destination + C*scratch)` rather than `O(V)`, but
each iteration reads the graph and score tiles. On NVMe this can be `3-30x`
slower than all-RAM execution for 60-100 iterations. It is still useful because
it fits and its peak is enforceable.

#### A4-ANSWER: score vector and top-k ladder

Persist the full score vector in node-ID order plus top-k lists at several
cutoffs. Common "top central nodes" queries read only a tiny answer artifact.
Personalized PageRank can optionally use a low-rank/Krylov basis as an
approximate candidate generator followed by exact local refinement; this is a
research plan, not an MVP exactness claim.

### A5. Dijkstra, A*, delta-stepping, Bellman-Ford, Yen's, and route trees

#### A5.1 Bounded delta buckets

The GDS delta-stepping estimator has dense distance and predecessor arrays plus
shared/local bins whose upper bounds can reach `E` and `C*V`. Replace bins with:

- a circular window of `W` bucket slots;
- one dedup generation lane per node or partition;
- sorted spill runs for buckets outside the window;
- stale-entry guards on dequeue;
- worker-local append buffers with fixed credits.

Peak bucket RAM is `W*slot_headers + B_bucket_entries*w_id`, chosen by the
budget, not by surprise duplicates. Distances remain `8*V` for f64; predecessor
IDs can be u32 (`4*V`) when legal. This does not necessarily reduce the dense
distance term, but it removes the dangerous unbounded bin term.

#### A5.2 Weight-band adjacency

For a pinned delta policy, store light and heavy edges in separate lanes, or
store edges in logarithmic weight bands. Delta-stepping avoids decoding heavy
weights during repeated light closure. A multi-band artifact supports several
delta choices without duplicating all targets.

Limit: a workload that changes delta arbitrarily or has uniform weights may
gain little and pay extra metadata.

#### A5.3 Exact repeated-query indexes

| Workload | Artifact | Query effect | Build/update cost |
| --- | --- | --- | --- |
| geographic/metric paths | ALT landmarks | admissible A* lower bounds reduce search | distance vectors per landmark |
| stable road-like topology | contraction hierarchy | exact shortcut search | expensive customization/rebuild |
| very high query rate | hub labels / 2-hop cover | intersection of labels | potentially huge disk artifact |
| DAG paths | topological order tape | one ordered pass | invalidated by cycle changes |
| k shortest paths | sidetrack/Eppstein artifact | shares alternatives across k | more complex than repeated Yen |

These formats trade storage and freshness for large repeated-query latency
wins. Contraction hierarchy or ALT can plausibly reduce visited edges by
`10-1000x` on road-like graphs; there is no such guarantee on arbitrary dense
social graphs.

#### A5.4 Bellman-Ford exact strict plan

Negative weights invalidate Dijkstra/ALT/CH assumptions. Store edges in flat
source/destination tiles, keep distances in a paged vector, and use a changed
vertex bitmap to skip inactive source tiles. The plan is exact and bounded but
retains worst-case `O(VE)` work and must detect negative cycles explicitly.

#### A5.5 Steiner and spanning variants

Use terminal bitmaps plus the same bounded multi-source path engine. Stable
terminal sets may justify a terminal-distance cache. Prize Steiner and
rerouting still require their objective-specific state; they share only the
path substrate, not one universal output format.

### A6. Louvain, Leiden, modularity, LPA, coloring, max-cut, and k-core

#### A6.1 Community optimization with streamed levels

**SPEED:** keep the current contracted level in RAM, with narrow community IDs
and fixed worker tally slabs. Publish only the next level after its build
completes.

**STRICT:** perform contraction as fixed-memory sorted runs (Innovation I8),
then mmap the next level. Peak excludes simultaneous full in-RAM old and new
CSRs. Expected peak reduction is `1.5-4x`; latency may be `1.2-4x` slower from
sort/merge I/O. `Modeled`.

**ANSWER:** persist node-to-community maps per selected level and a community
quotient graph. Most product queries use these answers, not rerun Louvain.

#### A6.2 Degree-bounded vote slab

The GDS Label Propagation estimator permits a per-thread vote container whose
upper range scales with `V`. A node can receive at most `distinct_neighbor_labels`
votes, bounded by its degree. Use two worker structures:

- small-degree stack array, linearly reduced;
- high-degree open-addressed slab sized to the largest admitted degree block,
  reset with generation tags instead of clearing `V` entries.

Worker memory becomes `O(max_degree_in_admitted_block)` rather than `O(V)`.
Sort nodes into degree classes so the scheduler can quote the slab before each
class. Hubs may run serially or spill vote pairs.

#### A6.3 Speaker-Listener LPA label sketches

SLLPA's label history can grow per node. Offer explicit plans:

- exact bounded-iterations: compressed `(label,count)` histories with a
  derived upper bound from iteration count;
- top-r label history: approximate, fixed `r` counters per node;
- materialized communities: persist only final memberships.

Approximation is visible because truncating label history can change overlap
communities.

#### A6.4 K1 coloring with degree-sized forbidden colors

The GDS estimator prices a `V`-sized forbidden-color bitset per thread. Greedy
coloring of a node needs to distinguish at most `degree(v)+1` colors at that
step. Use a generation-tagged u32 array sized to the maximum degree in the
current degree class, plus a sparse overflow structure for exceptional hubs.

Colors use u8/u16/u24 when the observed color count permits. On graphs where
`max_degree << V`, per-thread memory drops dramatically, plausibly `2-20x` for
the coloring state. On a star, the central degree is `V-1`; the hub fallback is
required.

#### A6.5 K-core peel tape

- encode current degree with the narrowest width proved by max degree;
- maintain degree buckets as a circular/radix structure with fixed chunks;
- emit `(node,core)` in peel order;
- materialize shell offsets for instant `core >= k` filters.

This removes concurrency-multiplied rebuild queues. Expected state is roughly
`w_degree*V + w_core*V + bounded_bucket_chunks`, commonly `4-8*V` bytes. The
GDS source names two int arrays plus per-thread tasks and rebuild structures;
the expected reduction is `1.5-4x`, configuration dependent.

#### A6.6 ApproxMaxKCut

Partition IDs need only `ceil(log2(k))` bits. Store them packed, and tally cut
gain in fixed degree-class slabs. Multiple random restarts can run sequentially
under strict RAM or in parallel under speed mode. Because the algorithm is
approximate, the receipt reports objective value, seed, restarts, and bound or
baseline comparison rather than equality of assignments.

### A7. Node Similarity, filtered similarity, KNN, and candidate joins

This family is where a storage format must attack work creation, not just byte
width. Exact all-pairs similarity can produce `O(V^2)` candidates and output;
no codec rescues an unconstrained contract.

#### A7.1 Rare-first prefix join for set similarity

For Jaccard, overlap, or cosine over neighbor sets:

1. order feature/neighbor IDs by global frequency, rare first;
2. store each node's sorted vector in posting blocks;
3. derive a prefix length from the threshold or current top-k lower bound;
4. generate candidates only from shared prefix postings;
5. use degree and remaining-overlap upper bounds to prune;
6. verify surviving pairs by exact compressed-list intersection.

This is an exact set-similarity join blended with Lucene's WAND frame: cheap
upper bounds must beat a monotone result floor before full scoring.

```text
node vector
   |
   +--> rare prefix --> posting lookup --> candidate IDs
   |                                      |
   +--> suffix length / norm bound --------+--> exact intersection
                                                    |
                                                    v
                                             fixed top-k slots
```

The artifact contains rare-first vectors, feature-to-node postings, per-block
max contribution, vector cardinality/norm, and optional component filters.

On skewed real graphs, candidate work may fall `5-100x`; on a complete or
nearly regular graph, bounds become useless and the plan approaches exhaustive
work. The quote must estimate candidate volume from posting-frequency
histograms and refuse if the upper confidence bound exceeds the budget.

#### A7.2 Fixed top-k output

When the public contract asks for top-k per node, allocate exactly `k` slots:
u32 target plus f32 score is `8*k*V` bytes, or wider where required. Never
build a general similarity graph first. Stream evicted candidates away.

The GDS Node Similarity estimator prices copied vectors and weights in addition
to optional top-k/top-N structures. Reusing the algorithm artifact directly
and keeping only fixed top-k state can plausibly lower resident state `2-10x`,
before candidate pruning. `Modeled`.

#### A7.3 Component and type partitioning

Pairs from disjoint WCCs cannot share graph-neighbor evidence. Relationship
type, label, time window, and component can define independent posting shards.
This is exact when the similarity definition has the same filter. It lowers
both candidate count and working-set size.

#### A7.4 Approximate candidate gates

- MinHash/LSH for set similarity;
- IVF-PQ for dense property vectors;
- HNSW/Vamana for online high-recall vector KNN;
- binary sketches or SimHash for a first gate;
- exact rescoring from the source artifact.

The result is approximate if the candidate gate can drop true neighbors,
despite exact rescoring. Receipts report recall on a pinned sample, not "exact."

#### A7.5 KNN artifact instead of iterative neighbor lists

The GDS KNN estimator names one top-k neighbor list plus old/new and reverse
old/new temporary lists. A persisted IVF/HNSW/Vamana artifact moves candidate
organization to build time. Query state becomes beam/probe buffers plus top-k,
not five `k*V`-like list families.

| Plan | RAM | Latency | Contract |
| --- | --- | --- | --- |
| HNSW + scalar quantization | high-medium | lowest online | approximate recall |
| IVF-PQ + exact rescore | low | medium, batch-friendly | approximate recall |
| Disk Vamana + PQ guide | low RAM + NVMe | medium | approximate recall |
| flat block scan | bounded window | highest | exact oracle |

The ANN formats are relevant to vector KNN, not a universal replacement for
topological Node Similarity.

### A8. Triangle count and local clustering coefficient

#### A8.1 Degree-oriented forward graph

Orient each undirected edge from lower `(degree,node_id)` order to higher.
Triangles are found by intersecting forward lists only; each triangle is
visited once. The forward artifact contains no reverse duplicate and has lower
maximum out-degree than arbitrary orientation.

#### A8.2 Hybrid intersection rows

- tiny rows: inline sorted u32/u24 arrays;
- medium rows: delta-bitpacked blocks with skip pointers;
- dense hub rows: Roaring/bitmap containers;
- each block: min/max target plus a small membership signature.

Array-array uses galloping intersection, array-bitmap probes the small side,
bitmap-bitmap uses SIMD AND/popcount. Block signatures may skip only proven
non-overlaps; final counts remain exact.

#### A8.3 Materialized motif lane

Triangle counts and local clustering coefficients are compact per-node answer
vectors. For stable graphs, serve them directly and retain the forward graph
only for rebuild or drill-down. GDS's intersecting triangle estimator already
shows algorithm state can be just one atomic count array; the larger win is
fewer adjacency bytes touched and fewer intersections.

Expected topology/state reduction versus a dual general adjacency projection
is `2-8x`, while latency improvement can range from none to `20x` depending on
degree skew and signature pruning. A complete graph is the adversarial case:
nothing prunes and output arithmetic dominates.

### A9. Random Walk, visit counting, and Node2Vec

#### A9.1 Streamed walk transducer

The GDS Node2Vec estimator prices:

```text
random walks = V * walks_per_node * walk_length * sizeof(long)
model        = two V * embedding_dimension float matrices
probability  = three V-sized arrays
```

At `walks_per_node=10` and `walk_length=80`, the raw walk IDs alone are
`6,400*V` bytes: 640 GB at `V=100M`, before array wrappers. Do not store them.

Use a counter-based RNG and pipe bounded walk batches directly into skip-gram
training or visit aggregation:

```text
(snapshot,seed,node,walk,step)
              |
              v
        deterministic RNG ---> next-hop sampler ---> trainer/count reducer
                                                        |
                                                        v
                                                 discard walk batch
```

With batch size `B_w` and length `L`, walk residency is `B_w*L*w_id`, chosen by
the budget rather than `V`. The walk-state reduction can be `10-1000x` for
large graphs. End-to-end RAM may still be dominated by embeddings.

#### A9.2 Degree-adaptive sampling

- degree <= small threshold: direct linear or binary-weight scan;
- high degree, static first-order walk: alias table only for those hubs;
- Node2Vec second-order walk: rejection sampling using exact adjacency
  membership, with a bounded retry count and exact local-table fallback;
- stable repeated walks: optional compressed alias blocks on disk.

A Bloom filter alone cannot determine Node2Vec transition class because a
false positive changes the probability distribution. It may prefilter, but an
exact membership check must resolve positives in an exact mode.

#### A9.3 Visit-count mode

If only visit counts are requested, aggregate each streamed step into a u32/u64
count vector or partitioned counter runs. There is never a reason to retain
walk sequences unless the user asks for them.

#### A9.4 Reproducibility policy

Strict mode pins RNG, walk order, training order, worker partition, and float
reduction. Fast mode pins only the distributional configuration and validates
embedding quality downstream. The two modes must not share one claim of exact
result equality.

### A10. FastRP, HashGNN, GraphSAGE, GraphSAGE Train, and KMeans

#### A10.1 FastRP rolling matrices

The GDS estimator names `embeddings`, `embeddingsA`, and `embeddingsB`, three
full f32 `V*D` matrices, approximately `12*V*D` payload bytes. The recurrence
needs previous/current buffers plus an output policy, not three permanently
resident general object arrays.

- speed: two f32 rolling matrices, `8*V*D` bytes;
- balanced: f16/bf16 early buffer plus f32 current/output, roughly
  `4-6*V*D` bytes depending schedule;
- strict: block rows, stream adjacency tiles, mmap output; resident
  `O(B*D)` rather than `O(V*D)`;
- answer: persist only the final quantized embedding plus exact-rescore source
  if downstream KNN needs it.

Payload reduction is `1.5x` for simple rolling f32 and up to `2-6x` with mixed
precision/streaming. Numerical quality must be measured on downstream tasks.

#### A10.2 HashGNN bit-sliced state

If the algorithm's semantic state is binary hashes, store dimensions as
bit-sliced words and aggregate with XOR/popcount/SIMD. A `D`-dimensional
boolean vector needs `D/8` bytes, not `4D` or `8D`. This can be `32-64x`
smaller than float matrices, but only for the hash algorithm's own contract;
it is not a free GraphSAGE replacement.

#### A10.3 GraphSAGE sampled feature pages

The GDS estimator distinguishes resident result features, temporary initial
features, and per-thread concurrent batches. Build a feature-page store that
co-locates:

- a row block of quantized/source features;
- sampling metadata for that node block;
- adjacency references to likely neighbor blocks;
- exact scale/zero-point or f32 escape lane.

Execution admits `C` batches whose induced subgraph and feature pages fit the
capsule. Full source features stay mmap/NVMe; result pages stream out.

The RAM benefit can be `5-20x` versus all feature matrices resident, while
latency depends heavily on neighbor locality. Reorder nodes by community or
partition to increase feature-page reuse. This is a `Modeled` research target.

#### A10.4 Training-specific caution

GraphSAGE Train has optimizer state, gradients, negative samples, and model
checkpoints beyond inference features. The quote must include them explicitly.
Quantized optimizer state changes training dynamics; treat it as approximate
model quality, not numerical equivalence.

#### A10.5 KMeans streaming columns

For Lloyd KMeans, keep centroids in RAM and stream feature blocks. Persist:

- cluster assignment in u8/u16/u32 based on `k`;
- nearest distance in f32/f64;
- optional lower bounds for Elkan/Hamerly pruning;
- feature columns in compression chosen per dimension.

Peak RAM is `centroids + assignments_window + C*partial_sums + feature_pages`,
not the full feature matrix. Exact Lloyd semantics are possible with fixed
order; mini-batch KMeans is a separate approximate plan.

### A11. Betweenness and closeness-family multi-source centrality

#### A11.1 Source-lane budgeting

GDS Betweenness stores global centrality plus a full compute task per worker,
including predecessor vectors sized from average degree. Concurrency therefore
multiplies a potentially edge-sized term.

Knight Walker should make source parallelism a budgeted lane count `S`:

```text
M_state = M_global_score + S * M_single_source_state + M_shared_topology
S       = floor((budget - fixed_terms) / M_single_source_state)
```

More RAM buys more sources in parallel; the same kernel scales down to one
source without changing correctness. Predictability is immediate.

#### A11.2 Recompute-predecessor Brandes

For unweighted graphs:

1. forward BFS stores distance, sigma, and node order;
2. backward pass scans in-neighbors of each node;
3. `u` is an exact predecessor of `v` iff `dist[u] + 1 = dist[v]`;
4. accumulate dependency without storing predecessor lists.

Per lane becomes roughly:

```text
distance u32 + sigma f64 + delta f64 + order u32 = 24*V bytes
```

instead of `O(E)` predecessor IDs plus the dense arrays. On average degree 10,
removing an 8-byte predecessor entry per edge can save around `80*V` bytes per
lane before wrappers. The price is an extra inbound-edge scan during the
backward pass, plausibly `1.5-3x` per-source work. `Derived` payload,
`Modeled` latency.

For weighted paths, predecessor rediscovery needs a declared equality rule
for `dist[u] + w == dist[v]`; integer or fixed-point weights are the safest
first exact target.

#### A11.3 Approximate source sampling

Sample sources with a pinned strategy and produce confidence intervals for
centrality. This is often the only practical plan at very large `V`. Report
sample count, seed, confidence method, and held-out error. Never label it exact.

#### A11.4 Closeness and harmonic centrality

They reuse the source-lane BFS/SSSP engine but aggregate distances instead of
retaining predecessor DAGs. Strict mode streams one lane; speed mode admits as
many lanes as the budget allows. A disconnected-graph policy and infinity
handling are part of the equality contract.

### A12. CELF and influence maximization

CELF repeatedly estimates marginal spread and maintains active bitsets and a
priority queue. The core algorithm is already approximate through Monte Carlo.

#### A12-MC: streamed live-edge simulations

- counter-based RNG defines each live-edge realization without storing it;
- one or a few active bitmaps reused across candidates;
- simulations batched to the memory budget;
- CELF's lazy upper-bound priority queue reduces recomputation.

#### A12-RIS: reverse-reachable set artifact

Generate many reverse-reachable sets, store them as compressed posting lists
or Roaring containers, and turn seed selection into maximum coverage:

```text
node ---> posting list of RR-set IDs containing node
seed choice = node covering most uncovered RR sets
```

This moves repeated traversal to build time. RAM mode keeps RR bitmaps hot;
strict mode streams posting blocks and keeps only the uncovered-set bitmap.
Quality depends on sample count, so the artifact manifest includes confidence
parameters and empirical validation.

### A13. Topology transforms and utility kernels

#### Inverse relationships and `ToUndirected`

Do not eagerly duplicate topology for every graph. Use the selective-transpose
and orientation policies:

- full second orientation only when repeated pull/reverse queries justify it;
- destination-sorted edge tiles for strict mode;
- virtual undirected iteration over two ordered lanes when acceptable;
- materialized symmetrized CSR for speed mode.

#### Degree centrality

Degree is already in row offsets for unweighted adjacency. Serve it as
arithmetic or a narrow materialized vector; do not run a general algorithm.
Weighted degree gets a precomputed row-sum lane when repeated.

#### Scale Properties

Use columnar property blocks with streaming reductions. Keep only aggregate
statistics and one output block resident. This is a column-store operation,
not a reason to load graph topology.

#### Indirect Exposure

Map it to iterative rank or bounded frontier propagation depending on the
formula. The custom artifact includes only relevant relationship types and
weights, and the state capsule declares dense vectors versus sparse frontier
before run.

### Algorithm-to-artifact decision table

| Algorithm | Preferred speed artifact | Preferred low-RAM artifact | Materialized answer |
| --- | --- | --- | --- |
| BFS / DFS | adaptive out-CSR + selective CSC | blocked rows + run frontier | level/parent tape |
| WCC | narrow DSU + Afforest | shard quotient | giant-default component map |
| SCC | trim-core bidirectional graph | disk-backed DFS frames | condensation DAG |
| Bridges | forward/cotree lanes | chord stream + fixed forest state | bridge list/tree |
| Articulation Points | low-link flat arrays | fixed hot stack + spill | block-cut tree |
| PageRank / HITS | normalized pull tiles | 2D streamed tiles | score/top-k vectors |
| Dijkstra / A* | CSR + ALT landmarks | bounded priority runs | route index |
| Delta Stepping | weight-band CSR + ring buckets | spillable bucket window | distance tree |
| Bellman-Ford | active source edge tiles | streamed tile rounds | distances/cycle witness |
| Yen's | sidetrack-aware path store | bounded candidate runs | cached k paths |
| Spanning / Steiner | path artifact + terminal bitmap | streamed multi-source engine | parent/cost tree |
| Louvain / Leiden | in-RAM current quotient | streamed contraction runs | community maps/quotient |
| Modularity | community-sorted edge tiles | streamed aggregation | modularity scalar/vector |
| Label Propagation | degree-class vote slabs | spilled hub votes | labels |
| SLLPA | bounded label histories | compressed histories | overlapping memberships |
| K1 Coloring | degree-sized color epochs | serial hub fallback | color vector |
| ApproxMaxKCut | packed partitions | sequential restarts | best partition/objective |
| K-Core | narrow bucket peel | chunked peel tape | core vector/shell offsets |
| Node Similarity | rare-first postings | partitioned exact join | fixed top-k graph |
| KNN | HNSW/SQ | IVF-PQ or Disk Vamana | neighbor lists |
| Triangle / LCC | oriented hybrid rows | block intersections | count/coefficient vector |
| Random Walk | hub alias rows | direct/rejection sampler | optional walk tape |
| Node2Vec | streamed walks + in-RAM model | streamed walks + paged model | embeddings |
| FastRP | rolling f32 matrices | tiled/mixed-precision recurrence | embeddings |
| HashGNN | bit-sliced vectors | mmap bit blocks | hash embeddings |
| GraphSAGE | locality-ordered feature pages | sampled batch paging | embeddings/model |
| KMeans | resident centroids + streamed columns | one feature window | assignments/centroids |
| Betweenness | many admitted source lanes | one recompute-predecessor lane | score vector |
| CELF | parallel live-edge batches | RIS posting blocks | seed set + confidence |
| Inverse / Undirected | full second orientation | selective/virtual orientation | transformed snapshot |
| Link prediction pipeline | ANN/similarity candidate index | sampled candidates and fixed splits | scored top-k links |
| Split Relationships | deterministic split bitmap | streamed partition writer | reusable split manifest |

## Rubber-Duck Audit

### Duck 1: "Does custom storage automatically use less RAM?"

No. If topology is already compact and the algorithm uses one small `V`-sized
array, another encoding may add metadata and decode cost without changing the
dominant term. Degree centrality and triangle-count state are examples where
the biggest win may be materialization or fewer edge touches, not algorithm
state.

**Rule:** identify the dominant term from `M_artifact`, `M_state`, and
`M_output` before proposing a codec. Reject any design that attacks less than
20% of the quoted peak unless it also materially improves latency or
predictability.

### Duck 2: "Can mmap make a 100 GB graph a 1 GB RAM process?"

Only if the access pattern and residency control keep most pages cold. A full
PageRank sweep eventually touches every edge page each iteration; the OS may
retain many pages. Heap metrics can lie while RSS and page cache grow.

**Rule:** strict mode counts resident mapped pages and uses explicit windowing,
readahead limits, and eviction/direct-I/O experiments. Quote warm-cache and
cold-cache latency separately.

### Duck 3: "Is the selective transpose always better than full CSC?"

No. It wins only if pull-worthy edges are a minority or if edge-tile scans are
acceptable. A low-diameter social graph may enter dense pull across most
partitions; a full CSC can then be both simpler and faster.

**Falsifier:** for each corpus graph, compare `(bytes of selected in-tiles +
extra edge scans)` with full CSC bytes and runtime. If selected fraction exceeds
a calibrated threshold, compile full CSC.

### Duck 4: "Can the precision staircase claim exact PageRank?"

Not by analogy alone. Mixed-precision iterative refinement has rigorous results
for particular numerical problems under conditioning assumptions. PageRank
power iteration is related but not automatically covered by those theorems.

**Correction:** classify the staircase as a research plan. It earns the exact
endpoint label only through a high-precision residual gate and differential
equivalence under the declared PageRank policy. Otherwise it is approximate.

### Duck 5: "Does u24 always beat u32?"

No. Packed 24-bit loads require unpacking and can inhibit SIMD/alignment. They
win on disk and bandwidth when decode is amortized; they may lose in hot random
access.

**Rule:** support u24 as a storage codec that expands into u32 vectors in speed
mode, or benchmark native packed access before using it in a hot state array.

### Duck 6: "Can a fixed state capsule guarantee P100 latency?"

It can enforce memory and I/O issuance, not control hardware faults, OS
scheduling, SSD tail latency, thermal throttling, or power loss. "P100" is also
an unstable statistical phrase unless every run in a finite set is meant.

**Correction:** promise a hard memory admission/enforcement contract and a
measured latency envelope under a named machine, cache state, data artifact,
and run count. Return outliers in the receipt; do not promise universal P100.

### Duck 7: "Can fixed top-k make Node Similarity bounded?"

It bounds output state, not candidate generation. A dense graph may still
create quadratic candidate work. Exact top-k bounds can prune only after a
competitive floor exists and only when score upper bounds are informative.

**Rule:** quote candidate-volume confidence bounds from feature posting
histograms. Refuse, spill, or require approximation when the bound exceeds the
contract.

### Duck 8: "Can we eliminate all Node2Vec sampling tables?"

Direct/rejection sampling may be slow or have poor retry behavior for extreme
`p/q` biases and clustered hubs. A bounded fallback exact table is necessary.
The best design is degree-adaptive, not table-free dogma.

**Falsifier:** measure attempts per accepted transition by degree and `p/q`.
Compile alias blocks for classes whose expected retry cost crosses the table
memory/latency break-even point.

### Duck 9: "Does predecessor recomputation make betweenness strictly better?"

It removes memory but adds an inbound-edge scan and needs CSC or equivalent.
If full predecessor lists fit and many source lanes run in parallel, the GDS
shape may be faster. On sparse graphs, predecessor payload may also be modest.

**Rule:** expose both plans. Let the quote choose lane count and remember-vs-
recompute based on `E/V`, budget, orientation availability, and I/O cost.

### Duck 10: "Can streamed Louvain contraction avoid all overlap?"

The old level must remain readable while new sorted runs are produced, so disk
overlap remains. Some in-memory metadata for both generations also remains.
The plan removes simultaneous resident CSRs, not simultaneous retained bytes.

**Rule:** quote resident peak and retained disk peak separately, including
temporary runs and merge fan-in.

### Duck 11: "Can semantic answer artifacts replace the graph?"

Only for their exact snapshot and question. A WCC map cannot answer paths; a
PageRank vector with damping 0.85 cannot answer damping 0.9; a shortest-path
index may depend on one weight property. Answer artifacts multiply if the
semantic key is not controlled.

**Rule:** every artifact key includes snapshot, projection, direction, types,
weights, algorithm version, parameters, numeric policy, and build seed where
relevant. Garbage-collect by observed reuse and rebuild cost.

### Duck 12: "Will io_uring make the RAM-first kernels faster?"

Not necessarily. io_uring helps when the runtime issues real asynchronous I/O.
A fully resident PageRank is memory-bandwidth-bound; syscall reduction does
little. io_uring belongs in strict/out-of-core plans, artifact build pipelines,
and spill/merge paths, not as a blanket speed claim.

### Duck 13: "Does Rust itself create the improvement?"

Rust removes JVM object/header/GC risks and makes fixed layouts and allocation
discipline easier to enforce. But GDS already uses huge primitive arrays and
compressed adjacency. The large gains in this atlas come from fewer arrays,
no materialized walks/predecessors, bounded concurrency, custom artifacts, and
safe pruning. A same-layout language port should expect modest and
workload-dependent changes, not an automatic order-of-magnitude win.

### Duck 14: "What if output is larger than RAM?"

Then output must stream or spill. All-pairs paths, unbounded Node Similarity,
walk streams, and mutation results can dominate the algorithm. The quote must
price output from the requested mode and reject an unbounded materialization
under a hard RAM contract.

### Duck 15: "Does building more artifacts increase build peak?"

Yes. A naive compiler can hold canonical input, sort buffers, old generation,
and several outputs simultaneously. Build is itself a bounded job with a
capsule, run files, publication protocol, and receipt. Runtime predictability
cannot be purchased with an unbounded builder.

### Rejected or demoted ideas

| Idea | Verdict | Reason |
| --- | --- | --- |
| one compressed CSR for every algorithm | rejected | ignores direction, state, candidate, and feature shapes |
| mmap means bytes do not count as RAM | rejected | resident pages are real memory |
| all f64 -> f32 with no oracle | rejected | changes numerical contract |
| Bloom-only Node2Vec adjacency test | rejected for exact mode | false positives alter transition probabilities |
| maintain every answer incrementally | rejected | deletion impact and maintenance state can exceed rebuild cost |
| native u24 everywhere | demoted | decode/alignment can erase bandwidth win |
| full six-order edge permutation index | demoted | useful only if query mix justifies write/disk amplification |
| full reachability transitive closure | demoted | quadratic worst-case storage |
| universal HNSW for graph similarity | rejected | vector proximity is not topological set similarity |
| io_uring as the main compute speedup | rejected | resident graph kernels are usually memory-bandwidth-bound |

### Minimum falsification suite

Every proposed plan must run against graph shapes designed to break it:

1. long road chain: thin frontiers and high diameter;
2. low-diameter power-law graph: dense frontiers and hubs;
3. complete/dense graph: no similarity or motif pruning;
4. giant component plus tiny islands: tests skew compression;
5. many equal-size components: defeats giant-default encoding;
6. DAG with tiny cyclic core and fully strongly connected graph: brackets SCC
   trim value;
7. uniform and heavy-tailed weights: brackets bucket bands;
8. high-clustering and bipartite graphs: brackets triangle intersections;
9. adversarial high-degree star: tests color/vote hub fallback;
10. random and highly redundant feature vectors: brackets similarity prefix
    bounds;
11. cold-cache and warm-cache runs;
12. concurrent build plus query to expose generation-overlap peaks.

## ASCII Mechanism Gallery

### Gallery 1: the product is a compiler plus an enforcer

```text
USER ORDER
artifact + algorithm + semantics + hard RAM + latency preference
    |
    v
+------------------------+
| portfolio planner      |
| exact byte ledger      |
+-----------+------------+
            |
            v
+------------------------+
| nondominated plans     |
| race / balance / strict|
+-----------+------------+
            |
            v
+------------------------+
| artifact compiler      |
| build if missing       |
+-----------+------------+
            |
            v
+------------------------+
| sealed physical view   |
+-----------+------------+
            |
            v
+------------------------+
| state capsule          |
| enforce or abort       |
+-----------+------------+
            |
            v
 answer + resource receipt
```

### Gallery 2: adaptive adjacency without unconditional duplication

```text
OUTGOING BASE
+----------+----------+----------+----------+
| inline   | bitpack  | roaring  | hub page |
| deg 0-7  | sparse   | dense    | very hot |
+-----+----+-----+----+-----+----+-----+----+
      |          |          |          |
      +----------+----------+----------+
                         traversal decoder
                               |
            +------------------+------------------+
            |                                     |
            v                                     v
      sparse frontier                       dense frontier
      push out rows                         pull selected tiles
                                                   |
                                      no tile? scan edge block
```

### Gallery 3: strict-RAM PageRank as destination tiles

```text
iteration i

rank block A ----+      edge tile A->X ----+
rank block B ----+      edge tile B->X ----+--> sum block X --> disk
rank block C ----+      edge tile C->X ----+

resident at one moment:
  one source-rank window
  one destination accumulator
  fixed worker scratch
  fixed I/O buffers

next destination X+1 repeats; final merge publishes rank_(i+1)
```

### Gallery 4: exact similarity without a quadratic resident set

```text
rare-first node vector
          |
          v
  prefix posting blocks
          |
          v
candidate stream --> degree/norm upper bound --> can beat top-k floor?
                                                   | no
                                                   +----> skip
                                                   |
                                                   | yes
                                                   v
                                      exact list intersection
                                                   |
                                                   v
                                         fixed k-slot heap
```

### Gallery 5: streamed Node2Vec

```text
counter tuple       adjacency sampler        bounded walk batch
(seed,n,w,step) ---> exact next hop --------> [walks in flight]
      ^                                             |
      | deterministic regeneration                 v
      +-------------------------------------- trainer/reducer
                                                    |
                                                    v
                                               output page

No completed-walk matrix exists.
```

### Gallery 6: streamed community contraction

```text
old sealed level
      |
      v
(community_u, community_v, weight)
      |
      v
+------------+  +------------+  +------------+
| sorted run |  | sorted run |  | sorted run |
+------+-----+  +------+-----+  +------+-----+
       +---------------+---------------+
                       v
                 bounded merge
                       |
                       v
               new sealed quotient
                       |
                 atomic root flip
                       |
                       v
                  retire old level
```

### Gallery 7: RAM is a concurrency dial, not a failure surprise

```text
budget after fixed terms
          |
          v
+---------+---------+
| bytes per source  |
| or worker lane    |
+---------+---------+
          |
          v
lanes = floor(available / bytes_per_lane)

more RAM  -> more lanes -> lower wall time, same answer
less RAM  -> fewer lanes -> higher wall time, same answer
too small -> spill plan, approximate plan, or REFUSE
```

## Estimation Model

### Symbols

| Symbol | Meaning |
| --- | --- |
| `V` | node count |
| `E` | stored directed relationship count for the selected projection |
| `C` | admitted concurrency |
| `D` | feature/embedding dimension |
| `k` | requested top-k or cluster count, by context |
| `L` | walk length or hierarchy levels, by context |
| `B` | admitted block/batch entries |
| `P` | partition count or resident page count, by context |
| `w_id` | stored node-ID bytes |
| `w_off` | offset bytes |
| `q` | active/core fraction |
| `h` | fraction of edges receiving a selective transpose |

### Full peak equation

```text
M_peak = M_resident_artifacts
       + M_algorithm_state
       + M_worker_scratch(C)
       + M_output_window
       + M_io_buffers
       + M_runtime
       + M_build_or_generation_overlap
       + safety_margin
```

The estimate is not complete if it omits any line. `M_runtime` includes the
allocator, stacks, code, libraries, telemetry, and control-plane overhead.
The first release should calibrate it conservatively from measured high-water
marks rather than pretend it is zero.

### Disk and latency equations

```text
D_peak = retained generations + temporary runs + spill + output

T_run  = T_decode + T_edges + T_state + T_reduce + T_io + T_sync
T_e2e  = T_artifact_build_if_missing + T_queue + T_run + T_publish
```

A speed-mode number that excludes first-build time is reported as warm-artifact
latency, never as end-to-end latency.

### Common artifact formulas

```text
raw one-way CSR       = (V+1)*w_off + E*w_id
raw dual CSR          = 2*((V+1)*w_off + E*w_id)
selective transpose   = base_CSR + h*E*w_id + tile_directory
bitset                = ceil(V/8)
u32/f32 vector        = 4*V
u64/f64 vector        = 8*V
embedding f32         = 4*V*D
fixed top-k u32+f32   = 8*k*V
walk matrix u64       = 8*V*walks_per_node*walk_length
```

Compressed adjacency is quoted from the actual built byte length. Degree and
ID entropy make a universal compression ratio dishonest.

### Worked scale: 100M nodes, 1B directed edges

Assume u64 offsets and u32 IDs. Decimal units are used for readability.

| Item | Derived payload |
| --- | ---: |
| one-way raw CSR | 4.8 GB |
| dual raw CSR | 9.6 GB |
| one bit per node | 12.5 MB |
| one u32/f32 vector | 400 MB |
| one u64/f64 vector | 800 MB |
| PageRank GDS named state recipe | about 2.41 GB plus overhead |
| custom PageRank two f64 vectors | 1.60 GB plus tile scratch |
| custom PageRank two f32 vectors | 0.80 GB plus tile scratch |
| BFS GDS named dense arrays before locals | about 3.21 GB plus overhead |
| custom reachability, three bitmaps | 37.5 MB plus sparse/run capacity |
| custom reachability plus u32 parent | 437.5 MB plus frontier capacity |
| FastRP GDS three f32 matrices at D=128 | 153.6 GB payload |
| FastRP two rolling f32 matrices at D=128 | 102.4 GB payload |
| Node2Vec walk IDs at 10 walks x length 80 | 640 GB payload |
| streamed Node2Vec walk batch, 1M x 80 u32 | 320 MB payload |

The table intentionally says "named state recipe," not measured GDS RSS. Java
array headers, huge-array paging, graph projection, results, and runtime add to
the former; packed GDS topology can be smaller than raw CSR.

### Prediction intervals, not point estimates

Each quote should include:

```text
hard bytes       exact preallocated/resident maximum controlled by runtime
modeled bytes    paging/candidate/frontier interval from artifact statistics
expected bytes   calibrated median or p95 from prior receipts
refusal bound    conservative upper confidence limit used for admission
```

For exact strict plans, the admission decision uses hard bytes plus a bounded
I/O window. For opportunistic mmap plans, it uses a measured resident envelope
and may downgrade concurrency when pressure rises.

### Estimator calibration

For category `x`:

```text
error_x       = actual_peak_x - estimated_peak_x
relative_x    = actual_peak_x / max(estimated_peak_x, 1)
next_margin_x = quantile(relative_x, target_confidence) * estimate_x
```

Calibration keys include algorithm plan, artifact codec, degree histogram
class, machine, kernel, concurrency, and cache mode. A single global safety
factor would hide the very algorithm specificity the product is built around.

## Pareto Recommendations

### Core thesis

The strongest architecture is not "Neo4j in Rust with compressed CSR." It is:

```text
A proof-carrying portfolio of algorithm-shaped artifacts, each paired with a
fixed state capsule and an equality oracle, selected by an admission planner
that turns RAM into an explicit concurrency, I/O, freshness, or quality dial.
```

That is meaningfully different from GDS while preserving Neo4j compatibility
where it matters. GDS is the semantic/reference oracle and evidence source;
Knight Walker's differentiator is physically compiling the question.

### Proposed runtime architecture

| Layer | Responsibility | Non-negotiable artifact |
| --- | --- | --- |
| canonical log/snapshot | durable rebuild and audit truth | immutable snapshot ID and checksum |
| artifact compiler | profile graph and build selected views | exact byte/block manifest |
| artifact registry | key by full semantics and generation | lineage, freshness, reuse counters |
| plan enumerator | generate speed/balanced/strict/answer choices | RAM/I/O/build/output formulas |
| admission controller | FIT/SPILL/APPROXIMATE/REFUSE | hard envelope and safety margin |
| state-capsule allocator | reserve all mutable state | slab map by category |
| algorithm kernels | operate directly on custom artifacts | no hidden growth path |
| verification spine | compare by algorithm equality relation | GDS/reference/metamorphic oracle |
| receipt service | estimate versus actual | peak RSS, faults, I/O, spill, time, checksum |

### The five public plan profiles

| Profile | Topology | State | Intended promise |
| --- | --- | --- | --- |
| `RACE` | all useful views resident | maximum admitted lanes | minimum warm latency |
| `BALANCED` | mmap/adaptive views | compact state, bounded cache | practical default |
| `STRICT` | streamed blocks | fixed capsule and spill | hard RAM envelope |
| `APPROX` | sketches/quantized/candidates | quality-bounded | lower RAM/latency with explicit error |
| `ANSWER` | materialized quotient/result | tiny query window | repeated stable question |

Do not force one profile to pretend it dominates. The product should show the
nondominated choices and make the trade visible.

### Recommended bets, ranked

| Rank | Architecture bet | Product value | Technical leverage | Main risk |
| ---: | --- | --- | --- | --- |
| 1 | adaptive reach artifact + state capsule | directly serves A007 security/dependency ICP | simple exact oracle, clear RAM math | cold-I/O latency |
| 2 | WCC quotient + giant-default answer | proves semantic compression | tiny result serving, easy parity | deletions require rebuild |
| 3 | PageRank normalized pull tiles | famous benchmark and bandwidth story | removes Pregel message arrays | numeric/reduction policy |
| 4 | rare-first exact Node Similarity | strongest differentiated algorithm-shaped index | attacks candidate creation | adversarial dense graphs |
| 5 | streamed Node2Vec walks | spectacular state-term deletion | deterministic generation is clean | model matrices still large |
| 6 | streamed Louvain contraction | bounds a known graph-overlap spike | external sort is mature | I/O and build complexity |
| 7 | recompute-predecessor betweenness | elegant memory/compute dial | removes edge-sized per-lane state | extra scans, weighted ties |
| 8 | GraphSAGE feature pages | large future RAM wedge | local AI relevance | highest verification complexity |

### The first product slice

Stay aligned with A007. Build only these first:

1. canonical directed snapshot with relationship-type filtering;
2. adaptive outgoing reach artifact;
3. WCC component artifact;
4. optional selective inbound tiles after graph profiling;
5. `RACE`, `BALANCED`, `STRICT`, and `ANSWER` plans for reach/WCC;
6. hard state capsules and cancellation;
7. exact differential parity against GDS/Neo4j fixtures;
8. cold/warm memory and latency receipts.

This slice proves the business claim: the same high-stakes question receives a
quote, a plan, enforcement, an answer, and an estimator-error receipt. It also
proves the architectural thesis without waiting for embeddings or every GDS
procedure.

### What follows after the proof

**Second wave:** PageRank/HITS pull tiles, because they expose a clean
bandwidth-vs-precision-vs-I/O frontier and are recognizable benchmarks.

**Third wave:** Node Similarity and shortest paths. They demonstrate safe
pruning and query-specific indexes, the deepest algorithm-storage
differentiation.

**Fourth wave:** streamed Louvain and Node2Vec. They show that Knight Walker
can delete large transient state shapes, not just compress topology.

**Later research:** GraphSAGE Train, full Cypher surface, dynamic low-link,
distributed execution, and every estimator-backed GDS algorithm. They are not
needed to validate the product wedge.

### Acceptance tests for every new algorithm plan

1. **Semantic:** differential result under the declared equality relation.
2. **Metamorphic:** relabeling, edge-order, partition, or scaling invariants.
3. **Memory:** process high-water mark stays below admitted hard envelope.
4. **Paging:** mapped residency and page-fault counters remain inside quote.
5. **Spill:** actual spill bytes do not exceed the declared maximum.
6. **Latency:** cold and warm distributions are reported separately.
7. **Estimator:** per-category estimate-versus-actual error is persisted.
8. **Adversarial:** the falsification graph suite completes or refuses before
   execution as specified.
9. **Build:** artifact construction obeys its own memory and retained-disk
   contract.
10. **Freshness:** answer identifies exact snapshot and parameter key.

### Quantitative claim policy

The architecture supports large theoretical payload reductions in selected
state terms: roughly `8x+` for reachability versus the source-observed BFS
dense arrays, `1.5-3x` for rank vectors versus Pregel message state,
`10-1000x` for eliminating stored walk matrices, and potentially `2-20x` for
edge-sized or concurrency-multiplied work structures. Those are design ranges,
not product claims.

No public performance claim should ship until one benchmark matrix records:

- Neo4j/GDS version and configuration;
- Knight Walker plan and artifact build time;
- exact dataset checksum and graph statistics;
- machine, kernel, storage, and cache state;
- projection, algorithm, output, and whole-process peaks;
- p50/p95/p99 plus all-run maximum over a named run count;
- correctness/recall relation;
- and estimate-versus-actual receipt.

### Final decision

Pursue custom OLAP storage, but implement it as a disciplined plan portfolio.
The most defensible innovation is the combination of:

1. semantic compression through quotients and answer artifacts;
2. state-shape deletion through streaming and recomputation;
3. local shape adaptation through row/block codecs;
4. exact bound-based pruning;
5. explicit approximate alternatives;
6. fixed state capsules;
7. calibrated receipts.

That combination can lower RAM substantially and make it more predictable.
Some plans will also be faster because they touch fewer bytes or candidates;
strict out-of-core plans will often be slower. The product advantage is not
that every point beats Neo4j on every axis. It is that the user can choose a
point on the Pareto frontier and trust that the runtime will honor it.

## Local GDS Evidence Appendix

Two independent structural indexes were used:

```text
code-graph-mcp
  4,921 files
  38,262 nodes
  521,221 edges
  AST search: 45 MemoryEstimateDefinition classes

codebase-memory-mcp project neo4j-gds-local
  4,859 indexed files
  54,265 nodes
  284,022 edges
  inheritance query: 48 MemoryEstimateDefinition implementations
```

The count difference comes from extraction/query behavior and additional
estimator forms; the architecture did not infer algorithm coverage from one
count alone.

Representative source evidence used in the state analysis:

| Algorithm/state | Local source path | Observed ingredients |
| --- | --- | --- |
| BFS | `algo/src/main/java/org/neo4j/gds/paths/traverse/BfsMemoryEstimateDefinition.java` | visited, traversed nodes, weights, minimum chunk, local nodes, result nodes |
| WCC | `algo/src/main/java/org/neo4j/gds/wcc/WccMemoryEstimateDefinition.java` | atomic disjoint set |
| PageRank | `algo/src/main/java/org/neo4j/gds/pagerank/PageRankMemoryEstimateDefinition.java` | Pregel with one double property |
| Pregel | `pregel/src/main/java/org/neo4j/gds/beta/pregel/Pregel.java` | vote bits, node values, message arrays/queues |
| Node Similarity | `algo/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityMemoryEstimateDefinition.java` | copied vectors/weights, components, top-k/top-N |
| KNN | `algo/src/main/java/org/neo4j/gds/similarity/knn/KnnMemoryEstimateDefinition.java` | top-k plus four temporary neighbor-list families |
| Louvain | `algo/src/main/java/org/neo4j/gds/louvain/LouvainMemoryEstimateDefinition.java` | modularity state, contracted CSR graph, dendrograms |
| Delta Stepping | `algo/src/main/java/org/neo4j/gds/paths/delta/DeltaSteppingMemoryEstimateDefinition.java` | distance, predecessor, shared bin, local bins |
| Betweenness | `algo/src/main/java/org/neo4j/gds/betweenness/BetweennessCentralityMemoryEstimateDefinition.java` | per-thread predecessors, order, delta, sigma, traversal state |
| Node2Vec | `algo/src/main/java/org/neo4j/gds/embeddings/node2vec/Node2VecMemoryEstimateDefinition.java` | all random walks, probability arrays, two embedding matrices |
| FastRP | `algo/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRPMemoryEstimateDefinition.java` | three embedding matrices |
| SCC | `algo/src/main/java/org/neo4j/gds/scc/SccMemoryEstimateDefinition.java` | index/components/visited plus multiple stacks |
| Articulation | `algo/src/main/java/org/neo4j/gds/articulationpoints/ArticulationPointsMemoryEstimateDefinition.java` | tin/low/children, bitsets, relationship-sized events |
| K-Core | `algo/src/main/java/org/neo4j/gds/kcore/KCoreDecompositionMemoryEstimateDefinition.java` | degree/core arrays, per-thread tasks, rebuild state |
| Label Propagation | `algo/src/main/java/org/neo4j/gds/labelpropagation/LabelPropagationMemoryEstimateDefinition.java` | labels plus per-thread vote maps |
| K1 Coloring | `algo/src/main/java/org/neo4j/gds/k1coloring/K1ColoringMemoryEstimateDefinition.java` | colors plus per-thread forbidden-color bitsets |
| GraphSAGE | `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/GraphSageMemoryEstimateDefinition.java` | initial/result features and concurrent batches |
| CELF | `algo/src/main/java/org/neo4j/gds/influenceMaximization/CELFMemoryEstimateDefinition.java` | spread arrays, active sets, queue, simulations |

These files are evidence for state ingredients, not proof that GDS realizes the
loose estimator upper bound in ordinary runs. Benchmark comparisons must use
actual whole-process measurements.

## External Primary Sources Used To Check Borrowed Ideas

These sources were checked after the local corpus pass. They support the
borrowed mechanism, not the Knight Walker performance estimates:

- [Direction-Optimizing Breadth-First Search](https://www.scottbeamer.net/pubs/beamer-sc2012.pdf): push/pull BFS and its topology-dependent benefit.
- [Scaling Up All Pairs Similarity Search](https://www.bayardo.org/ps/www2007.pdf): exact sparse-vector candidate generation and pruning rather than approximation.
- [Computing the Shortest Path: A* Search Meets Graph Theory](https://www.microsoft.com/en-us/research/publication/computing-the-shortest-path-a-search-meets-graph-theory-2/): ALT landmarks and triangle-inequality lower bounds for optimal paths.
- [Exact Routing in Large Road Networks Using Contraction Hierarchies](https://pubsonline.informs.org/doi/10.1287/trsc.1110.0401): exact preprocessing/query tradeoff for road-like shortest paths.
- [Optimizing Parallel Graph Connectivity Computation via Subgraph Sampling](https://cris.huji.ac.il/en/publications/optimizing-parallel-graph-connectivity-computation-via-subgraph-s/): Afforest's subgraph-sampling approach to connectivity.
- [Parallel Random Numbers: As Easy as 1,2,3](https://random123.com/): stateless counter-based random generation used to justify deterministic walk regeneration.
- [Accelerating the Solution of Linear Systems by Iterative Refinement in Three Precisions](https://epubs.siam.org/doi/10.1137/17M1140819): the numerical inspiration, and the reason this atlas does not transfer mixed-precision guarantees to PageRank without a new proof/oracle.
- [Influence Maximization Revisited](https://www.weizhewei.com/papers/sigmod2020-subsim.pdf): reverse-reachable-set sampling as an influence-maximization architecture.

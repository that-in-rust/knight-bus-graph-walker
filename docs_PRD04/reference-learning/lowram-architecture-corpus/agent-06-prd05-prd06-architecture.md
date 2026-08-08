# Agent 06: PRD05 and PRD06 Low-RAM Architecture Dossier

## 0. Mandate and conclusion

This dossier is governed by `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
The product is a low-RAM, deterministic graph analytical runner. It is not a
database rewrite.

The recommended architecture is:

> Compile a sealed input artifact into a small menu of content-addressed,
> algorithm-shaped views. Before every run, choose exactly one bounded plan
> from `fit | spill | approximate | refuse`. Execute it in a supervised
> process with pre-reserved memory, bounded output, and a proof-carrying
> receipt. Keep Neo4j compatibility outside this kernel as a narrow adoption
> adapter and oracle.

The core differentiation is not Rust, mmap, compression, or disk-backed
execution by themselves. Prior art covers all of those. The differentiated
contract is that the selected representation, full working set, quality
policy, and hard resource limit are explicit before execution and reconciled
with measurements afterward.

The first product proof should remain:

1. A security, IAM, dependency, or SBOM artifact.
2. A bounded path or reachability answer.
3. A hard RAM ceiling.
4. Exact comparison with a trusted oracle.
5. A receipt showing estimate, plan, observed peak, elapsed time, I/O, and
   output identity.

WCC should follow as the first whole-graph exact kernel. PageRank should be the
first iterative proof. Similarity, communities, triangles, and embeddings are
earned later only when founder evidence justifies them.

## 1. Evidence accounting

The authoritative denominator is:

`docs_PRD04/reference-learning/lowram-architecture-corpus/evidence/all-documents-denominator.tsv`

Only rows assigned to `agent-06` were used for corpus accounting.

| Measure | Result |
| --- | ---: |
| Frozen assigned files | 94 |
| Markdown files semantically read | 92 |
| Structured TSV files queried | 1 |
| Shell verifier semantically read | 1 |
| Output ledger rows | 94 |
| Missing paths | 0 |
| Duplicate paths | 0 |
| Hash or byte mismatches at intake | 0 |

The file-by-file record is:

`docs_PRD04/reference-learning/lowram-architecture-corpus/evidence/agent-06-prd05-prd06-files.tsv`

The most load-bearing evidence groups are:

| Evidence IDs | Contribution |
| --- | --- |
| `A06-000001..000003` | Rewrite feasibility, prior art, and quantitative working-set estimates |
| `A06-000009..000011` | Graph workload and buyer-domain maps |
| `A06-000018..000019` | Components, hooking, shortcutting, and WCC oracles |
| `A06-000026..000027` | CSR layout, orientation, and byte arithmetic |
| `A06-000030..000031` | Delta-stepping bucket state and weighted path tradeoffs |
| `A06-000035..000036` | Sparse push versus dense pull frontiers |
| `A06-000041..000042` | Graph analytics synthesis |
| `A06-000061..000062` | PageRank state, convergence, and iteration policy |
| `A06-000064..000065` | Product and PMF lenses |
| `A06-000076..000079` | Compressed sets and semiring oracles |
| `A06-000086..000087` | Algorithm-specific equality relations |

The corpus contains useful numerical examples, but most are analytical models
or representative operating points, not fresh Knight Walker benchmarks.
Accordingly, this dossier uses them to choose experiments and define formulas.
It does not promote them to product performance claims.

## 2. A007 architecture laws

The following are treated as hard architecture laws:

1. **Artifact to answer, not database to transaction.** Input is a sealed,
   versioned artifact. The product does not own OLTP, replication, recovery,
   mutable indexes, or a general catalog.
2. **Admission precedes material allocation.** A run is rejected before
   algorithm state or conversion state is allocated when its conservative
   upper bound cannot fit.
3. **Every material byte has an owner.** Topology, properties, conversion,
   state, frontiers, output, I/O buffers, worker replicas, allocator slack,
   and operating-system margin are all charged.
4. **The plan is explicit.** The plan class is exactly one of `fit`,
   `spill`, `approximate`, or `refuse`.
5. **Spill is an algorithm, not an emergency.** Spill layout, maximum buffer,
   pass count, temporary-space bound, and cleanup behavior are known before
   execution.
6. **Approximation is a quality contract.** A seed, quality metric, threshold,
   and oracle corpus are part of the admitted plan.
7. **Output is part of the working set.** A correct algorithm that cannot
   return its answer under the cap is not an admissible plan.
8. **Receipts are the differentiated surface.** The before, during, and after
   evidence must make the estimate and enforcement auditable.
9. **Compatibility is subordinate.** Bolt, PackStream, Cypher, GDS procedure
   names, and official drivers may reduce migration ceremony. They do not
   define the kernel.
10. **Market evidence controls breadth.** A technically elegant second or
    seventh algorithm does not outrank a painful, recurring, paid workflow.

## 3. Architecture option registry

These IDs are also used in the evidence ledger.

| Option ID | Meaning | Default disposition |
| --- | --- | --- |
| `ARCH-BOUNDARY` | Artifact-to-answer product boundary | Choose |
| `ARCH-STORAGE-SEAL` | Immutable generation, manifest, checksum, atomic publication | Choose |
| `ARCH-VIEW-FOUNDRY` | Content-addressed compiler for algorithm-shaped views | Choose |
| `ARCH-COMPRESSED-SETS` | Delta blocks, Roaring containers, bitsets, and skip metadata selected by shape | Choose selectively |
| `ARCH-PATH` | Bounded reachability, BFS, and weighted shortest-path capsule | Choose first |
| `ARCH-WCC` | Exact components capsule | Choose second |
| `ARCH-PAGERANK` | Iterative centrality capsule | Experiment third |
| `ARCH-SIMILARITY` | Exact or approximate neighborhood similarity capsule | Defer pending demand |
| `ARCH-COMMUNITY` | Louvain or Leiden capsule | Defer |
| `ARCH-TRIANGLE` | Intersection-oriented triangle and clustering capsule | Experiment after WCC/PageRank |
| `ARCH-EMBEDDING` | FastRP and embedding capsule | Defer |
| `ARCH-SPILL` | Family-specific bounded external-memory execution | Choose as a contract |
| `ARCH-PARTITION` | Skew-aware, recursively bounded partitions or tiles | Choose as shared machinery |
| `ARCH-ORACLE` | Exact, tolerant, isomorphic, recall, and metamorphic validation | Choose |
| `ARCH-RECEIPT` | Versioned estimate, execution, and terminal evidence | Choose |
| `ARCH-PMF` | Founder and buyer learning gate | Choose |
| `ARCH-ADAPTER` | Narrow official-driver or GDS-shaped adapter | Experiment after kernel proof |
| `ARCH-QUERY-ADAPTER` | Bounded query/profile compiler, not general Cypher | Experiment narrowly |
| `ARCH-ANN` | HNSW, IVF, DiskANN, and vector quantization | Defer except as similarity evidence |
| `ARCH-INCREMENTAL` | Standing differential computation over updates | Defer as a distinct product regime |
| `ARCH-DEFER` | Explicitly useful but outside the first product proof | Defer |

## 4. Recommended system shape

```text
                        OPTIONAL ADOPTION EDGE
  official driver -> bounded Bolt/Cypher/GDS profile compiler
                              |
                              v
  +-----------------------------------------------------------+
  |                  BOUNDED PLAN COMPILER                    |
  | artifact + workload profile + hard RAM/temp/time/quality  |
  | -> complete working-set formula -> eligible plan set      |
  | -> exactly one of fit | spill | approximate | refuse      |
  +-----------------------------+-----------------------------+
                                |
                                v
  +-----------------------------------------------------------+
  |                  IMMUTABLE VIEW FOUNDRY                   |
  | canonical dense IDs | properties | labels | source hashes |
  | forward CSR | reverse CSC | edge blocks | degree order     |
  | Roaring hubs | destination tiles | similarity postings     |
  | quotient levels | embedding stripes                        |
  +-----------------------------+-----------------------------+
                                |
                                v
  +-----------------------------------------------------------+
  |                SUPERVISED ALGORITHM CAPSULE               |
  | pre-reserved state | bounded buffers | cancellation        |
  | deterministic reduction policy | progress counters         |
  | direct answer stream or bounded spill output               |
  +-----------------------------+-----------------------------+
                                |
                                v
  +-----------------------------------------------------------+
  |                 PROOF-CARRYING RECEIPT                    |
  | estimate terms | selected view | plan hash | quality policy |
  | peak RSS | mapped/read/written/spilled bytes | passes       |
  | elapsed by phase | checksum | oracle verdict | estimator err|
  +-----------------------------------------------------------+
```

### 4.1 Canonical artifact spine

The generic artifact should contain only what nearly every approved capsule
needs:

- Manifest and schema version.
- Content hash and source provenance.
- Dense internal node-ID mapping plus optional external-ID correspondence.
- Node and edge counts by declared type and direction.
- Selective property columns.
- Optional labels or filters as bitsets or Roaring sets.
- Checksums and byte counts for every view.
- Build provenance, including tool version, width decisions, ordering policy,
  and deterministic seed.

The canonical spine must not automatically contain both edge orientations,
every property, an OLTP record store, a transaction log, or all profile
sidecars. Those are paid for only when an admitted workload needs them.

### 4.2 View foundry

The foundry compiles one canonical artifact into immutable views. A view key is
the hash of:

```text
source artifact identity
+ view type and version
+ orientation
+ sort/order policy
+ numeric widths
+ compression policy
+ selected properties
+ build seed and deterministic mode
```

Views are reusable across runs and independently disposable. Their build time
and disk bytes are reported separately from algorithm execution. A view is
never silently built inside a supposedly warm benchmark.

Candidate view types:

| View | Best users | Why it exists |
| --- | --- | --- |
| Forward CSR | Outbound paths, push frontiers, WCC scans | One offset lookup plus contiguous neighbors |
| Reverse CSC | Pull PageRank, inbound paths | Destination-owned accumulation without random writes |
| Dual CSR/CSC | Repeated mixed-direction workload | Pays disk once to avoid repeated conversion |
| Edge block stream | WCC, strict PageRank, bulk transforms | Sequential external scan with tiny topology residency |
| Destination tiles | Strict PageRank and reductions | Bounds active accumulators and makes writes local |
| Degree-oriented forward graph | Triangles and k-core | Stores each undirected edge once in intersection order |
| Roaring hub sidecar | Dense frontiers, labels, high-degree intersections | Shape-adaptive compressed set operations |
| Incidence postings | NodeSimilarity and filters | Candidate generation by shared features/neighbors |
| Quotient graph generation | Louvain/Leiden levels | Seals each induced graph and frees prior scratch |
| Embedding stripe files | FastRP | Caps resident dimensions and supports streaming output |

### 4.3 Supervisor and memory ledger

Each run should execute in a supervised worker process. The parent:

1. Parses the artifact and workload profile.
2. Uses checked arithmetic to compute the complete working-set range.
3. Reserves memory and temporary storage.
4. Starts a worker with an enforceable ceiling.
5. Samples RSS, mapped pages, I/O, spill, and progress.
6. Cancels the worker when a declared bound is crossed.
7. Persists a terminal receipt even after worker failure.

In-process allocator checks are useful but not sufficient as the only hard
limit. The receipt should distinguish algorithm-managed bytes, process RSS,
mapped clean pages, mapped dirty pages, page cache charged by policy, and
supervisor overhead.

## 5. Notation and common cost model

| Symbol | Meaning |
| --- | --- |
| `V` | Node count |
| `E` | Stored directed edge count for the selected orientation |
| `w_id` | Node-ID width, normally 4 or 8 bytes |
| `w_off` | Offset width, normally 4 or 8 bytes |
| `w_val` | Weight/property width |
| `w_rank` | Rank/state width, normally 4 or 8 bytes |
| `F_max` | Admitted maximum frontier cardinality |
| `K` | Requested top-K |
| `C` | Generated candidate-pair count |
| `d` | Embedding dimension |
| `L` | Louvain/Leiden levels retained |
| `T` | Worker count |
| `I` | Iteration or pass count |
| `B` | Admitted memory ceiling |
| `O_max` | Admitted maximum output bytes |
| `S_os` | Operating-system and allocator safety margin |

Base topology formulas:

```text
plain one-orientation CSR = (V + 1) * w_off + E * w_id
dual plain CSR/CSC       = 2 * ((V + 1) * w_off + E * w_id)
bitset(V)                = ceil(V / 8)
node vector              = V * element_width
bounded output           <= O_max
admission upper bound    = persistent residency
                         + conversion peak
                         + algorithm state
                         + frontier/candidates
                         + worker replicas
                         + I/O and spill buffers
                         + output
                         + S_os
```

Compressed CSR must be estimated from measured block statistics, not a single
marketing ratio. Its manifest needs compressed bytes, block index bytes,
decoder scratch, maximum decoded block, and cold-read amplification.

## 6. Exhaustive corpus taxonomy

The graph-learning corpus has 28 named patterns in eight categories. Every
pattern is accounted for below.

| Patterns | Category | Architectural lesson | A007 disposition |
| --- | --- | --- | --- |
| 1 LSM compaction | Storage | Immutable runs plus merge move write cost out of reads | Reuse only for view lifecycle; do not build an OLTP LSM |
| 2 WAL group commit | Storage | Batching amortizes durability | Reject from kernel; input artifacts are already sealed |
| 3 Roaring ID sets | Storage | Select array, bitmap, or run container per local density | Choose for filters, dense frontiers, and hubs |
| 4 MVCC visibility | Storage | Readers pin a stable generation | Replace with whole-artifact identity, not row MVCC |
| 5 COW snapshots | Storage | Publish a new root atomically | Choose for manifest/view publication |
| 6 Bloom shortcut | Storage | Cheap negative gates avoid expensive reads | Choose only when negative probes exist and filter RAM is charged |
| 7 CSR adjacency | Graph analytics | Offsets plus contiguous neighbors minimize bytes touched | Choose as a family of oriented views |
| 8 Push/pull frontier | Graph analytics | Sparse push and dense pull need different orientations and hysteresis | Choose for traversal and iterative kernels |
| 9 Semiring traversal | Graph analytics | Algebraic formulation yields reusable reference oracles | Choose mainly as oracle/backend, not universal engine |
| 10 Hook/shortcut components | Graph analytics | WCC can converge with one or a few node arrays plus scans | Choose |
| 11 PageRank iteration | Graph analytics | Rank state is cheap; repeated topology traffic dominates | Choose as iterative proof |
| 12 Delta stepping | Graph analytics | Bucket slack trades ordering for parallelism | Choose for weighted path profile |
| 13 HNSW | Vector ANN | Graph navigation exposes recall versus latency | Defer |
| 14 Quantization ladder | Vector ANN | Compressed-domain scoring plus exact rescore saves RAM | Defer, reuse for embeddings/similarity experiments |
| 15 DiskANN/Vamana | Vector ANN | Co-locate data and links to minimize disk reads per hop | Defer, retain sector-shaping lesson |
| 16 IVF | Vector ANN | Partition and scan a declared fraction | Defer, candidate for bounded approximate similarity |
| 17 Posting blocks | Full text | Sorted IDs, deltas, fixed blocks, and skips are a universal set format | Choose as inspiration for compressed adjacency and incidence |
| 18 BM25/WAND | Full text | Rising floor plus safe upper bounds avoids touching losers | Reuse in bounded top-K similarity where true bounds exist |
| 19 FST dictionary | Full text | Sorted immutable input supports compact navigation | Defer except external-ID or schema dictionaries |
| 20 Record-chain adjacency | Graph DB | Neo4j chains optimize mutation, not analytical scans | Reject as analytical storage |
| 21 Pull operator pipeline | Graph DB | Demand-driven output and explicit breakers bound rows | Reuse in optional query adapter and output stream |
| 22 Triple permutations | Graph DB | Redundant orders remove runtime sorting at high disk/write cost | Reject generally; permit only justified orientation sidecars |
| 23 PackStream | Neo4j ecosystem | Byte-exact wire contract is independently testable | Optional adapter only |
| 24 Stub conformance | Neo4j ecosystem | Conversation scripts are executable protocol evidence | Choose for adapter verification, not kernel |
| 25 Incremental deltas | Dataflow | Standing queries pay for arrangements and process changes | Defer as a separate product mode |
| 26 Supersteps | Dataflow | Barriers expose partitioning and recovery costs | Reject cluster-first architecture; retain oracle lessons |
| 27 Metamorphic oracles | Testing | Identities find bugs without trusting a second engine | Choose |
| 28 Tolerant equivalence | Testing | Equality is algorithm-specific | Choose and encode in every profile |

## 7. Workload taxonomy and product priority

| Workload family | Examples | Natural data shape | Founder relevance | Default |
| --- | --- | --- | ---: | --- |
| Bounded reachability | Access path, dependency path, blast radius | Forward CSR plus bounded frontier | 100 | Build first |
| Unweighted shortest path | BFS distance, provenance chain | Forward or bidirectional CSR | 96 | Build first |
| Weighted shortest path | Risk-weighted route, supply path | Weighted CSR plus delta buckets | 82 | Add after unweighted proof |
| Components | WCC, account islands, SBOM partitions | Edge stream plus parent/label plane | 92 | Build second |
| Iterative centrality | PageRank, ArticleRank | Pull CSC plus degree/rank planes | 78 | First iterative proof |
| Simple centrality | Degree, in/out counts | Degree sidecar only | 70 | Cheap supporting profile |
| Neighborhood similarity | Jaccard, overlap, NodeSimilarity | Incidence postings or Roaring sets | 72 | Experiment after proof |
| Vector kNN | Similar embeddings | Quantized vectors plus IVF/HNSW/DiskANN | 45 | Defer |
| Communities | Louvain, Leiden, label propagation | Undirected CSR plus sealed quotient levels | 56 | Defer |
| Motifs | Triangles, clustering coefficient | Degree-oriented sorted adjacency | 63 | Experiment after WCC/PageRank |
| Core structure | k-core | Degree-ordered CSR plus peel buckets | 58 | Defer |
| Embeddings | FastRP | CSR plus striped dense matrices | 42 | Defer |
| Expensive all-source centrality | Betweenness, closeness | Repeated BFS/SSSP state | 30 | Refuse or batch only |
| Dynamic graph answers | Standing BFS/WCC/PageRank | Arranged signed deltas | 38 | Separate future regime |
| Distributed graph compute | Pregel/GraphX/Giraph | Partitioned edges plus shuffle messages | 20 | Reject cluster-first |
| Query-language graph plans | Cypher patterns | Pull pipeline over approved capsules | 55 | Narrow adapter |
| RDF/SPARQL analytics | Triple patterns and permutations | Multiple sorted permutations | 18 | Outside first ICP |
| Hybrid text/vector ranking | BM25 plus ANN | Posting blocks plus vector index | 22 | Outside first ICP |
| GNN training | GraphSAGE, PyG, DGL | Sampled neighborhoods plus tensors | 15 | Defer |

The domain maps support security/IAM, dependency/SBOM, lineage, and
observability as strong near-term candidates because the artifacts are
graph-shaped, the answers are checkable, and uncontrolled memory is costly.
The maps do not prove buyer urgency or willingness to pay.

## 8. Algorithm capsule contracts

### 8.1 Bounded paths, BFS, and SSSP

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `PTH-FIT-FWD` | Forward plain or compressed CSR | Outbound reachability and BFS |
| `PTH-FIT-BIDIR` | Forward CSR plus reverse CSC | Bidirectional point-to-point search |
| `PTH-SPILL-FRONTIER` | CSR or edge blocks plus external frontier runs | Exact bounded search when frontier state exceeds RAM |
| `PTH-WEIGHT-DELTA` | Weighted CSR plus bounded delta buckets | Exact weighted SSSP |
| `PTH-APPROX-SKETCH` | Landmark or reachability labels | Approximate/filtered precheck only |

#### Working set

```text
topology              = selected orientation view
visited               = bitset(V) or measured Roaring bytes
frontier_fit          = <= F_max * w_id
frontier_spill_buffer = <= admitted_run_bytes
distance              = V * distance_width, if requested
predecessor           = reachable_or_V * w_id, only if requested
path/output           = <= O_max
weighted buckets      = active bucket IDs + per-thread buffers
```

The GDS delta-stepping source corroborates that distance and predecessor arrays
are per-node, while shared and thread-local bins range from node-scale toward
edge- or concurrency-amplified worst cases. That is exactly the state that must
be bounded rather than left to a generic queue.

#### Plan policy

- **Fit:** Use CSR, bitset/Roaring visited state, and a pre-reserved in-memory
  frontier. Prefer one orientation unless bidirectional search has a proven
  repeated benefit.
- **Spill:** Write sorted frontier runs or partition-local queues. Bound the
  number and size of merge buffers. Charge predecessor and output separately.
- **Approximate:** Only for explicitly named reachability-label, landmark, or
  hop-capped semantics. Never call a capped search an exact unreachable result.
- **Refuse:** Path enumeration without an output cap; a hub/frontier above the
  admitted spill design; negative weights for a non-negative SSSP profile; or
  a predecessor requirement that cannot fit.

#### Oracle and receipt

- Exact distance per node for BFS/SSSP.
- Set equality for bounded reachability.
- Do not compare one chosen path when several shortest paths are legal unless
  a tie policy is part of the profile.
- Receipt: visited nodes, edges examined, frontier peak, spill runs, passes,
  output count, maximum hop/distance, and canonical answer checksum.

#### Founder relevance

This is the highest-relevance family. Access paths, dependency reachability,
SBOM exposure, and blast radius are legible outcomes and can be demonstrated
with a hard cap and a small oracle.

### 8.2 WCC and components

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `WCC-UF-STREAM` | One parent array plus sequential edge scan | Lowest-state exact baseline |
| `WCC-SV-BLOCK` | Hook/shortcut arrays plus edge blocks | Parallel exact passes |
| `WCC-AFFOREST` | Sample then exact residual verification | Exact acceleration on skewed graphs |
| `WCC-MMAP-PARENT` | Mapped parent/label plane plus bounded hot-page cache | Exact low-RAM mode |

#### Working set

```text
union_find_min        = V * w_parent + atomic/page overhead
incremental_seeded    = union_find_min + V * w_parent
SV_family             = 2..3 * V * w_id + edge-block buffers
topology              = edge stream or one CSR orientation
output                = V * output_label_width, unless streamed
```

`@sdsrs/code-graph 0.114.1` found
`WccMemoryEstimateDefinition` delegating to
`HugeAtomicDisjointSetStruct.memoryEstimation`. The non-incremental form
allocates one atomic long array per node; incremental mode adds a second
seeding array. This supports WCC as the cleanest whole-graph low-RAM proof.

#### Plan policy

- **Fit:** Union-find when its parent plane and output fit. Use deterministic
  root canonicalization after convergence.
- **Spill:** Mmap the parent plane or process partitioned edge blocks with a
  bounded cache. Receipt must count passes and random page faults.
- **Approximate:** Afforest remains exact only when a final residual scan and
  canonical validation are performed. Sampling without that scan is a
  separately named approximate profile.
- **Refuse:** Parent/label lower bound plus output cannot fit or be mapped
  under policy; pass/time ceiling is too small for the admitted edge scans.

#### Oracle and receipt

- Partition isomorphism, not raw component-label equality.
- Metamorphic oracle: node relabeling must preserve the partition.
- Receipt: unions attempted/succeeded, compression passes, edge passes,
  parent-page faults, largest component, component count, and partition
  checksum based on canonical minimum member.

#### Founder relevance

High. Components summarize blast-radius regions, dependency islands, and
account clusters. WCC is also a strong estimator-calibration kernel because
its irreducible state is simple.

### 8.3 PageRank and iterative centrality

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `PR-FIT-PULL` | Reverse CSC plus outdegree sidecar | Destination-owned exact iterations |
| `PR-FIT-COMPRESSED` | Block-compressed CSC plus degree/rank planes | Lower topology bytes, decode per block |
| `PR-SPILL-EDGE` | Sequential edge blocks each iteration | Minimum topology residency |
| `PR-SPILL-TILES` | Destination-partitioned tiles | Bounded accumulator range |
| `PR-RESIDUAL` | Residual frontier and sparse propagation | Exact-to-tolerance or approximate early stop |

#### Working set

```text
pull topology         = CSC bytes
outdegree             = V * w_degree
rank current          = V * w_rank
rank next/accumulator = V * w_rank, or one destination tile
active/residual set   = bitset/Roaring/frontier bytes
dangling/reduction    = T * partial_sum_width + fixed state
output                = V * output_width, unless streamed
total I/O             = I * topology_bytes_read for streamed plans
```

The GDS source confirms a Pregel node value of type double, vote bits, compute
step state, and message arrays or queues. Its PageRank computation sums
messages, applies damping, sends `delta / degree`, and votes to halt when the
delta is below tolerance. A custom pull capsule can reduce generic message
machinery, but it cannot remove the rank vector or repeated edge traffic.

#### Plan policy

- **Fit:** Pull over CSC with precomputed outdegree. Use deterministic
  partition order and stable reductions when strict reproducibility is
  requested.
- **Spill:** Stream edge blocks or destination tiles every iteration. The plan
  declares `I_max`, bytes per pass, accumulator tile size, and temporary
  bytes.
- **Approximate:** Residual threshold, fewer iterations, f32 state, or sampled
  edges only with an explicit rank-error or top-K quality contract.
- **Refuse:** Required tolerance cannot be reached within admitted iterations;
  even one rank/output plane violates the ceiling; or conversion to the needed
  orientation cannot fit its charged peak.

#### Oracle and receipt

- Relative epsilon per node, plus mass/conservation and non-negativity checks.
- Compare top-K overlap separately from score epsilon when that is the buyer
  outcome.
- Receipt: preparation time, iterations, residual by iteration, edge bytes per
  pass, dangling mass, reduction mode, convergence status, and score checksum.

#### Founder relevance

Medium-high as the first iterative systems proof, not necessarily the first
buyer workflow. It demonstrates that bounded execution survives repeated
passes and floating-point determinism.

### 8.4 NodeSimilarity and kNN

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `SIM-SET-CSR` | Sorted neighborhood lists | Exact pair scoring for supplied pairs |
| `SIM-ROARING-HYBRID` | Array containers for sparse nodes, bitmaps for hubs | Fast exact intersections |
| `SIM-INCIDENCE` | Neighbor-to-source postings | Candidate generation by shared neighbor |
| `SIM-SPILL-TOPK` | Partitioned pair accumulators and external top-K merge | Exact bounded candidate processing |
| `SIM-MINHASH-LSH` | Signatures and LSH buckets | Approximate Jaccard |
| `SIM-IVF-PQ` | Vectorized feature representation with bounded probes | Approximate kNN |

#### Working set

```text
neighborhood storage = E * encoded_id_cost + offsets/indexes
candidates           = C * candidate_record_width
exact topK           = V * K * entry_width + queue/object overhead
signatures           = V * signature_width
partition buffer     = <= admitted_pair_run_bytes
output               = min(C, V*K, topN) * result_width
```

The GDS code graph found that NodeSimilarity estimates node filters, per-node
vectors, optional weights, component mappings, similarity-graph output, and
top-K or top-N structures. `TopKMap.memoryEstimation` allocates a bounded
priority queue per node, making `O(V*K)` retained state explicit. This family
cannot be treated as a small extension of CSR.

#### Plan policy

- **Fit:** Exact scoring only when the supplied or generated candidate set and
  per-node top-K state fit.
- **Spill:** Partition candidates by source or hash, sort/reduce overlap
  counts, and externally merge bounded top-K lists.
- **Approximate:** MinHash/LSH, IVF, HNSW, or quantized vectors with recall or
  Jaccard-error thresholds and a fixed query corpus.
- **Refuse:** Unbounded all-pairs generation; unbounded high-degree postings;
  or a requested exact top-K whose lower-bound state/output exceeds budget.

#### Oracle and receipt

- Exact pair scores for supplied pairs.
- Exact top-K with deterministic tie rules for exact plans.
- Recall@K, distance-equivalent ties, and error distribution for approximate
  plans.
- Receipt: generated candidates, pruned candidates, set-container mix,
  intersections, top-K memory, spills, recall corpus identity, and quality.

#### Founder relevance

Potentially high for identity correlation, fraud, and recommendations, but the
candidate-generation problem can dominate both RAM and product complexity.
Require buyer evidence before implementation.

### 8.5 Louvain and Leiden

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `COMM-FIT-LEVEL` | Undirected CSR plus per-node community planes | In-memory level optimization |
| `COMM-SEALED-QUOTIENT` | Content-addressed induced graph per level | Release old scratch and make levels auditable |
| `COMM-PARTITION-MOVE` | Partition-local move proposals plus bounded merge | Lower resident influence maps |
| `COMM-SKETCH` | Sampled/coarsened graph | Approximate exploratory communities |

#### Working set

```text
base topology          = undirected CSR and optional weights
community arrays       = several V * 8 planes
thread influence maps  = T * skew-dependent map bytes
dendrogram             = V * w_id * retained_levels
induced graph          = E_level topology + one weight per edge
output                 = V * label width, plus optional hierarchy
```

The GDS estimator directly lists current and next communities, cumulative
weights, node-community influences, community weights, colors, updates, and
per-thread influence maps. Louvain additionally estimates a new aggregate
graph every level and one to `L` dendrogram arrays. A generic `O(V+E)`
statement is materially incomplete.

#### Plan policy

- **Fit:** One level at a time, bounded influence maps, and only the requested
  hierarchy retained.
- **Spill:** Seal each quotient graph, partition move proposals, and merge in
  deterministic order. Charge cross-partition passes.
- **Approximate:** Coarsening, sampling, reduced levels, or lower precision
  with a modularity/quality distribution contract.
- **Refuse:** A level's graph plus minimum state cannot fit any legal plan;
  thread-local influence maps have no enforceable skew cap; or strict
  reproducibility is requested for an unsupported schedule.

#### Oracle and receipt

- Community-ID values are arbitrary. Compare partition relations when a
  deterministic reference is meaningful.
- For stochastic/heuristic modes, compare modularity and stability
  distributions across pinned seeds, not byte-identical labels.
- Receipt: levels, moves, modularity by level, quotient sizes, influence-map
  peaks, seeds, scheduling mode, and hierarchy bytes.

#### Founder relevance

Lower than paths, WCC, and PageRank for the named first ICP. Defer until a
design partner has a recurring segmentation or fraud-ring workflow.

### 8.6 Triangles, clustering, and k-core

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `TRI-DEG-FWD` | Degree-ordered forward adjacency | Store each undirected edge once and intersect sorted lists |
| `TRI-HUB-ROARING` | Roaring bitmap sidecars for high-degree nodes | Bitmap intersections for hubs |
| `TRI-SPILL-BLOCK` | Degree partitions and bounded adjacency blocks | Exact external intersections |
| `TRI-WEDGE-SAMPLE` | Sampled wedges | Approximate global/local clustering |

#### Working set

```text
oriented topology     = (V + 1) * w_off + E_forward * w_id
per-node count        = V * count_width, if local counts requested
thread cursors        = T * bounded decoder/intersection scratch
hub bitmap cache      = <= admitted_hub_bytes
wedge sample          = sample_count * sample_record_width
output                = scalar or V * count_width
```

The GDS estimator for intersecting triangle count has one atomic long count per
node, while the kernel consumes adjacency cursors. The source also contains
sorted-array intersection helpers. The main architectural cost is therefore
building and reading intersection-ready topology, not a large generic
algorithm heap.

#### Plan policy

- **Fit:** Degree-oriented sorted adjacency, merge intersections, and
  shape-adaptive hub bitmaps.
- **Spill:** Partition by lower-degree endpoint and read bounded adjacency
  blocks. Avoid materializing wedges.
- **Approximate:** Wedge sampling with confidence intervals.
- **Refuse:** Required conversion to sorted/oriented topology violates the
  budget; a hub bitmap has no bounded alternative; or local output cannot fit.

#### Oracle and receipt

- Exact integer counts and the identity `sum(local triangles) = 3 * global`
  for undirected simple graphs.
- Metamorphic node relabeling and edge-order permutation.
- Approximate profile reports confidence interval and sample seed.
- Receipt: oriented edges, intersections, hub strategy, decoded bytes, wedge
  samples, and count checksum.

#### Founder relevance

Useful in fraud and dense dependency clusters, but less direct than access
paths. It is an attractive low-state architecture proof after the first three
families.

### 8.7 FastRP and embeddings

#### Storage alternatives

| Plan ID | Representation | Use |
| --- | --- | --- |
| `EMB-GDS-3PLANE` | Three full f32 embedding planes | Compatibility baseline |
| `EMB-2PLANE` | Current and next planes | Reduced exact/compatible state if kernel permits |
| `EMB-STRIPED` | Dimension stripes and repeated topology passes | Hard RAM cap |
| `EMB-MAPPED` | Mapped output/intermediate planes | Low retained heap |
| `EMB-QUANT` | f16, scalar, product, or binary quantization | Approximate lower-RAM output |

#### Working set

```text
GDS-style embedding state = 3 * V * d * 4 + object/page overhead
two-plane state           = 2 * V * d * precision
striped state             = planes * V * stripe_d * precision
property projection       = selected feature columns + transform state
output lower bound        = V * d * output_precision
topology I/O              = passes * topology_bytes
```

The GDS `FastRPMemoryEstimateDefinition` explicitly estimates
`embeddings`, `embeddingsA`, and `embeddingsB`, each a per-node f32
array of `embeddingDimension`. That three-plane term must be a first-class
baseline. Rust does not make it disappear.

#### Plan policy

- **Fit:** Use the compatibility number of planes and requested precision.
- **Spill:** Stripe dimensions or map intermediate/output planes. Declare the
  extra topology passes.
- **Approximate:** Lower precision or quantized output with downstream quality
  metrics and exact rescore where applicable.
- **Refuse:** The output lower bound itself violates RAM plus temporary-storage
  policy, or the requested deterministic mode cannot be reproduced.

#### Oracle and receipt

- Fixed seed, worker count, partition/reduction order, and float policy.
- Relative tolerance for coordinates only when the implementation contract
  supports coordinate comparison.
- Prefer downstream quality or distance preservation for quantized/stochastic
  modes.
- Receipt: planes, dimensions, precision, passes, seed, output bytes,
  quantization artifact, and quality metric.

#### Founder relevance

Low for the first security/access-path proof. Defer until embeddings are tied
to a recurring paid answer rather than broad AI positioning.

## 9. RAM, latency, and predictability matrix

| Family and plan | RAM direction | Latency direction | Predictability | Main risk |
| --- | --- | --- | --- | --- |
| Path forward CSR fit | Low to moderate | Fast | High with hop/output caps | Hub frontier |
| Path external frontier | Hard bounded | Slower by run/merge I/O | High if run count bounded | Exploding predecessor/output |
| WCC union-find stream | Very low state | Multiple sequential scans possible | High | Parent random access under mmap |
| WCC hook/shortcut | More node planes | Fewer passes on large diameter | Medium-high | Contention and pass variance |
| PageRank pull CSC fit | Moderate | Fastest repeat mode | High at fixed iterations | Preparation cost |
| PageRank streamed edges | Low topology RAM | Roughly one full scan per iteration | High I/O, medium wall time | Storage bandwidth and faults |
| PageRank destination tiles | Bounded accumulators | Extra partition passes | High | Skewed tiles |
| Similarity exact top-K | Often high | Candidate dependent | Low until `C` is bounded | Candidate explosion |
| Similarity MinHash/IVF | Low to moderate | Faster | High resource, probabilistic quality | Recall drift |
| Louvain level-at-a-time | Moderate to high | Data/schedule dependent | Medium | Influence-map and quotient skew |
| Triangles degree-forward | Low state | Fast on ordered topology | High after conversion | Hubs |
| Triangle wedge sample | Very low | Fast | High resource, statistical answer | Confidence width |
| FastRP three-plane | High `V*d` | Fastest compatible | High at pinned policy | Output dominates |
| FastRP striped | Hard bounded | More topology passes | High | I/O and long elapsed time |

The strict low-RAM lane is not automatically faster than Neo4j GDS. It can be
faster only when the prepared view removes enough bytes, indirection, generic
message state, or passes to compensate for decompression and I/O. The product
claim should be predictable fit and enforced bounds first. Speedup is an
empirical per-profile result.

## 10. Verification architecture

### 10.1 Equality registry

Every workload profile must declare an equality relation before code exists:

| Output | Acceptance relation |
| --- | --- |
| Reachable nodes | Exact set equality |
| BFS/SSSP distance | Exact integer or declared numeric tolerance |
| Chosen path | Path validity plus distance; exact identity only with tie policy |
| WCC | Partition isomorphism |
| PageRank | Relative epsilon plus invariants |
| Exact NodeSimilarity | Score tolerance and deterministic top-K tie policy |
| Approximate kNN | Recall@K or distance-threshold recall |
| Louvain/Leiden | Quality and stability distribution; optional partition relation |
| Triangles/k-core | Exact integers and graph identities |
| FastRP | Seeded tolerance or downstream quality |

### 10.2 Oracle ladder

1. Tiny hand-checkable fixtures.
2. Metamorphic identities that do not trust Neo4j.
3. Differential comparison with stock Neo4j/GDS where legally and
   operationally available.
4. Independent GraphBLAS, Graphalytics, NetworkX, or simple reference kernels.
5. Production-shaped artifacts with retained raw receipts.

Every failure must carry a reproducer artifact, profile, seed, command,
expected/actual outputs, and counterexample rows.

### 10.3 Resource verification

Correctness tests are insufficient for the A007 promise. Each profile also
needs:

- Budget one byte or one page below admission threshold.
- Forced spill with tiny buffers.
- Hub, skew, and path-explosion fixtures.
- Slow consumer and canceled output.
- Corrupt view and checksum mismatch.
- Cold versus warm execution.
- Worker termination at the ceiling.
- Estimator calibration across held-out graph shapes.
- Idempotent cleanup after crash during every spill phase.

## 11. Code-graph cross-check

### 11.1 Commands

The exact requested version was used:

```bash
npx -y @sdsrs/code-graph@0.114.1 --version
npx -y @sdsrs/code-graph@0.114.1 health-check --json
npx -y @sdsrs/code-graph@0.114.1 search 'PageRank memory estimation' --json --compact --limit 8
npx -y @sdsrs/code-graph@0.114.1 search 'WCC union find memory estimation' --json --compact --limit 10
npx -y @sdsrs/code-graph@0.114.1 search 'NodeSimilarity TopK memory estimation' --json --compact --limit 10
npx -y @sdsrs/code-graph@0.114.1 search 'Louvain dendrogram memory estimation' --json --compact --limit 10
npx -y @sdsrs/code-graph@0.114.1 search 'triangle intersection memory estimation' --json --compact --limit 10
npx -y @sdsrs/code-graph@0.114.1 search 'FastRP embedding dimension memory estimation' --json --compact --limit 10
npx -y @sdsrs/code-graph@0.114.1 search 'delta stepping bucket' --json --compact --limit 12
npx -y @sdsrs/code-graph@0.114.1 search 'graph orientation adjacency compression' --json --compact --limit 10
npx -y @sdsrs/code-graph@0.114.1 show 'WccMemoryEstimateDefinition.memoryEstimation' --json --context-lines 8
npx -y @sdsrs/code-graph@0.114.1 show 'Pregel.memoryEstimation' --json --context-lines 10
npx -y @sdsrs/code-graph@0.114.1 show 'NodeSimilarityMemoryEstimateDefinition.memoryEstimation' --json --context-lines 8
npx -y @sdsrs/code-graph@0.114.1 show 'TopKMap.memoryEstimation' --json --context-lines 8
npx -y @sdsrs/code-graph@0.114.1 show 'LouvainMemoryEstimateDefinition.memoryEstimation' --json --context-lines 8
npx -y @sdsrs/code-graph@0.114.1 show 'ModularityOptimizationMemoryEstimateDefinition.memoryEstimation' --json --context-lines 10
npx -y @sdsrs/code-graph@0.114.1 show 'IntersectingTriangleCountMemoryEstimateDefinition.memoryEstimation' --json --context-lines 8
npx -y @sdsrs/code-graph@0.114.1 show 'FastRPMemoryEstimateDefinition' --json --context-lines 8
npx -y @sdsrs/code-graph@0.114.1 show 'DeltaSteppingMemoryEstimateDefinition' --json --context-lines 10
```

The same version was used against existing indexes in
`neo4j-testkit-src`, `opencypher-src`, and
`neo4j-gds-client-src`.

### 11.2 Confirmed high-impact claims

| Claim | Code-graph evidence |
| --- | --- |
| WCC minimum retained state is unusually simple | GDS estimates one `HugeAtomicLongArray` parent plane; incremental mode adds one seeding plane |
| PageRank's generic GDS path includes execution overhead beyond ranks | Pregel estimates vote bits, node values, compute-step state, and message arrays or queues |
| NodeSimilarity can retain `O(V*K)` state | `TopKMap` creates a bounded priority queue per node; the broader estimator also includes vectors, weights, filters, and candidates/output |
| Louvain cannot be modeled as only one label array | Estimator includes multiple community/weight/color arrays, per-thread influence maps, dendrograms, and an aggregate graph per level |
| Triangle counting state is small relative to topology preparation | Estimator has one atomic per-node count; kernel consumes adjacency cursors and sorted intersections |
| FastRP has a concrete three-plane baseline | Estimator allocates `embeddings`, `embeddingsA`, and `embeddingsB` per node at the chosen dimension |
| Delta-stepping queue state has broad shape-dependent bounds | Estimator includes distance, predecessor, shared bins from node to edge scale, and concurrency-amplified local bins |
| GDS client compatibility is a separate surface | Client index exposes PageRank API, Arrow, and Cypher endpoint classes independently from kernel code |
| TestKit can verify a narrow adapter | TestKit index exposes script parsing and protocol verification functions |

### 11.3 Tool limitations

- The GDS index was healthy and contained 4,921 files, 38,262 nodes, and
  521,221 edges.
- Search was FTS-only. Embedding coverage was zero, so no semantic-vector
  result is represented as evidence.
- The index reported 15,756 unresolved calls. Call graphs are useful but not
  complete.
- The project map output was extremely large and truncated. No conclusion
  depends on the omitted tail.
- Existing indexes were used. No reindex was run because the assignment
  permits writing only the two Agent-06 outputs.
- The openCypher index contained only three indexed files and returned no TCK
  search result. Therefore openCypher/TCK claims come from the assigned corpus,
  not from a successful structural code-graph cross-check.
- TestKit had a healthy 255-file index and GDS client had a healthy 823-file
  index, but those checks validate discoverability of adapter surfaces, not
  algorithm behavior.
- Source-level memory estimators are strong evidence of retained structures,
  not proof that estimates are perfectly calibrated to process RSS.

## 12. Three concrete architecture scenarios

### Scenario A: Founder slice runner

Scope:

- One sealed security/IAM/dependency artifact.
- Forward CSR and selective labels/properties.
- Bounded reachability and one shortest-path profile.
- `fit | spill | refuse`; approximation initially forbidden.
- Tiny oracle plus Neo4j differential oracle.
- Hard 5 GB and 10 GB profiles.

Why:

- Fastest route to a buyer-legible proof.
- Smallest adapter and algorithm surface.
- Exposes all differentiated machinery: admission, enforcement, output bounds,
  cancellation, and receipts.

### Scenario B: Whole-graph deterministic kernel

Scope:

- Scenario A plus WCC and PageRank.
- Edge stream, forward CSR, reverse CSC, and destination-tile views.
- Exact low-RAM plans and deterministic/tolerant float modes.
- Calibration across power-law, road, disconnected, and dense-community
  shapes.

Why:

- Proves the architecture generalizes across frontier, scan, and iterative
  workloads without pretending to support all GDS.

### Scenario C: Algorithm foundry

Scope:

- Scenario B plus similarity, communities, triangles, and embeddings.
- Candidate postings, quotient generations, degree-oriented adjacency,
  quantization, and quality-contract registry.
- Optional narrow Bolt/Cypher/GDS profile.

Why:

- This is the long-term differentiated platform, but only after partner demand
  proves that profile proliferation is worth its operational cost.

## 13. Experiments that decide the architecture

| Experiment | Competing plans | Required outputs |
| --- | --- | --- |
| Path artifact | Plain CSR vs block-compressed CSR vs edge blocks | Build bytes/time, cold/warm latency, RSS, frontier peak |
| WCC state | In-RAM union-find vs mmap parent vs SV block passes | RSS, page faults, passes, elapsed, partition oracle |
| PageRank strict | Pull CSC vs streamed edges vs destination tiles | RSS, I/O/iteration, total time, epsilon, estimator error |
| Frontier shape | Bitset vs Roaring vs sorted vector with hysteresis | Bytes and set-operation time over density/skew sweep |
| Triangle hubs | Merge intersection vs Roaring hub sidecars | Conversion cost, RSS, intersections/sec, exact counts |
| Similarity candidates | Incidence postings vs MinHash/LSH vs IVF | Candidate count, top-K memory, recall, time |
| FastRP state | Three-plane vs two-plane vs striped mmap | RSS, passes, output quality, deterministic drift |
| Output pipeline | Direct stream vs bounded run files | Slow-consumer RSS, cancellation latency, total I/O |

Every experiment stores raw receipts. A median alone is insufficient. Report
shape, cold/warm state, p50/p95/p99 or maximum as appropriate, variance, and
failure/cancellation behavior.

## 14. Decisions

### Choose

1. Choose the A007 artifact-to-answer boundary.
2. Choose immutable content-addressed generations and a view foundry.
3. Choose a generic manifest spine plus algorithm-specific sidecars.
4. Choose bounded paths as the founder slice, WCC as the first whole-graph
   proof, and PageRank as the first iterative proof.
5. Choose per-profile complete working-set formulas and upper-bound admission.
6. Choose supervised hard enforcement, bounded output, cancellation, and
   terminal receipts.
7. Choose algorithm-specific equality relations and reproducer artifacts.
8. Choose shape-adaptive compressed sets rather than one universal encoding.
9. Choose explicit cold, warm, preparation, execution, and amortized metrics.

### Experiment

1. Experiment with plain versus compressed CSR by graph shape.
2. Experiment with WCC union-find mmap versus SV block scans.
3. Experiment with PageRank pull CSC, streamed edges, and destination tiles.
4. Experiment with Roaring only for dense frontiers, labels, and hubs.
5. Experiment with a narrow official-driver adapter after the direct runner is
   proof-carrying.
6. Experiment with exact candidate postings and approximate MinHash/IVF only
   after a similarity buyer workflow exists.
7. Experiment with deterministic reduction cost versus throughput mode.

### Reject

1. Reject a whole Neo4j rewrite as the product plan.
2. Reject OLTP ownership, general MVCC, WAL, replication, and a mutable graph
   catalog in the first product.
3. Reject one universal in-memory graph representation.
4. Reject always storing both orientations.
5. Reject `io_uring`, Rust, mmap, or compression as standalone product
   differentiation.
6. Reject unbounded all-pairs similarity, path enumeration, and output
   materialization.
7. Reject a speed claim derived only from JVM-to-Rust translation.
8. Reject cluster-first Pregel or Spark architecture while a bounded
   single-machine plan is viable.
9. Reject byte-level equality for internal storage layouts.

### Defer

1. Defer broad Bolt, Cypher, APOC, Browser, OGM, routing, and transaction
   compatibility.
2. Defer Louvain/Leiden until a paid community workflow exists.
3. Defer FastRP, HNSW, IVF, DiskANN, and general vector search.
4. Defer standing differential/incremental graph computation as a separate
   product regime.
5. Defer distributed graph execution, GNN training, RDF permutations, and
   hybrid full-text/vector search.
6. Defer persistent sidecars that have no measured reuse and buyer value.

## 15. Options missing or underspecified in the current mega spec

The current mega spec has the correct boundary and strong generic requirements.
The following options are absent or not yet concrete enough to drive an
implementation decision:

1. **A versioned representation registry.** Add named view contracts for
   forward CSR, reverse CSC, edge blocks, destination tiles, degree-oriented
   adjacency, hub bitmaps, incidence postings, quotient levels, and embedding
   stripes.
2. **A view-selection formula.** Each profile should state why one orientation
   or sidecar is admitted, including expected reuse, build amortization, disk
   bytes, conversion peak, and cold-page behavior.
3. **Content-addressed conversion caching.** The spec permits sidecars but does
   not fully define deterministic view keys, reuse, garbage collection, and
   how cached build cost appears in first/repeat/amortized receipts.
4. **Per-family spill invariants.** Generic bounded partitions are not enough.
   Paths need frontier-run bounds, WCC needs parent-page/pass bounds, PageRank
   needs bytes-per-iteration and accumulator-tile bounds, similarity needs
   candidate-run/top-K merge bounds, and triangles need adjacency-block bounds.
5. **Irreducible output lower bounds.** Add an explicit admission rule that
   rejects or changes output mode when `V * output_width`, `V*K`, or
   `V*d*precision` cannot satisfy RAM and temporary-storage policy.
6. **Shape-adaptive set policy.** Add density and skew thresholds for choosing
   bitset, sorted vector, Roaring array/bitmap/run container, or spill run.
7. **WCC plan alternatives.** Name union-find stream, SV hook/shortcut,
   exact Afforest with residual verification, and mapped-parent plans as
   independently estimated options.
8. **PageRank destination tiling.** The open question lists edge streaming,
   partitioned push, and blocked tiles, but the executable requirements should
   define tile ownership, accumulator bounds, pass accounting, and deterministic
   reduction.
9. **Heuristic exactness vocabulary.** Louvain is not an exact optimizer in the
   mathematical sense. Distinguish exact execution of a pinned policy from
   globally optimal communities, and specify distributional quality gates.
10. **Approximation registry.** Define allowed quality metrics and required
    corpus artifacts for MinHash/LSH, ANN, wedge sampling, residual PageRank,
    coarsened communities, and quantized embeddings.
11. **Preparation reuse economics.** Require first-run, repeat-run, and
    amortized time-to-answer for every persistent sidecar and prohibit warm
    speed claims that hide build cost.
12. **Mapped-memory charging policy as a requirement.** It currently remains
    an open question. Choose a user-visible policy for clean pages, dirty
    pages, shared pages, page cache, and supervisor overhead before claiming a
    hard 5 GB or 10 GB ceiling.
13. **Format-proliferation budget.** Set a maximum supported view count per
    artifact/profile version and require measured reuse before a new persistent
    format is promoted.
14. **Algorithm receipt extensions.** Define required counters per family:
    frontier peaks, edge passes, residuals, candidate counts, quotient sizes,
    intersections, embedding planes, and quality metrics.
15. **A kernel-versus-adapter milestone gate.** The direct artifact runner must
    pass correctness, RAM, latency, and receipt gates before Bolt/Cypher work
    can consume the core roadmap.
16. **An explicit defer list.** Mark incremental dataflow, distributed
    supersteps, ANN, GNN, RDF, and full-text work as separate future regimes so
    they cannot silently expand the first product.

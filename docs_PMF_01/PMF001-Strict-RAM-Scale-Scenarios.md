# PMF001 Strict RAM Scale Scenarios

## Executive Answer

**Yes, but specifically resident RAM, not total data footprint.**

This is not a claim that a fundamentally terabyte-scale computation magically
becomes a fast 24 GB computation. The claim is that Knight Bus can keep only a
bounded working window in RAM while storing the remaining topology, vectors,
intermediates, and output on NVMe.

```text
Terabytes of logical state
        |
        v
+---------------------------+
| NVMe artifacts and slabs  |
+-------------+-------------+
              | bounded reads/writes
              v
+---------------------------+
| 8-64 GB resident capsule  |
| current tiles + buffers   |
+---------------------------+
```

## Strict-RAM Scenarios

The estimates below assume a graph whose already-projected Neo4j/GDS topology
occupies approximately 50 GB in RAM. They describe peak resident RAM after the
required custom artifacts have been built. They do not include the complete
on-disk footprint or claim equal latency.

| Algorithm | Logical data/state outside RAM | Plausible strict resident RAM | Consequence | Confidence |
| --- | ---: | ---: | --- | ---: |
| BFS/reachability | 30-60 GB graph artifact | **4-8 GB** | Exact; potentially `2-20x` slower | 80% |
| WCC | Graph plus component-label slabs | **6-12 GB** | Exact external merge; `2-8x` slower | 70% |
| PageRank | 50+ GB topology and 8-17 GB score vectors | **8-16 GB** | Exact tiled execution; `3-30x` slower | 70% |
| Node Similarity, top-k=10 | Graph/postings plus approximately 83 GB output | **8-24 GB** | Exact external candidate reduction; potentially very slow | 50% |
| FastRP, `D=128` | Approximately 533 GB final embeddings plus temporary slabs | **16-64 GB** | Repeated sparse-dense streaming; `3-20x` slower | 65% |
| GraphSAGE inference | Hundreds of GB of features and output pages | **16-64 GB** | Feasible for sampled inference; locality determines performance | 45% |
| Node2Vec | Approximately 1.1 TB model state on disk; walks never materialized | **24-72 GB** | Technically feasible, but random model updates are difficult | 40% |
| Betweenness | Dense state slabs and graph on disk | **12-30 GB** | Exact recomputation; at least `1.5-3x` more work | 60% |

The confidence percentage is a subjective confidence that a correctly
implemented benchmark will land inside the stated broad resident-RAM range
under these assumptions. It is not statistical confidence from existing
measurements.

## Why This Is Possible

Neo4j/GDS frequently estimates multiple complete structures concurrently:

```text
graph
+ input vectors
+ output vectors
+ messages
+ worker copies
+ candidate lists
+ walks or feature matrices
```

The strict architecture changes execution to:

```text
graph block
+ state window
+ fixed worker buffers
+ bounded output window
+ spill files
```

The reduction comes from four mechanisms:

1. Stream topology and state in bounded blocks instead of retaining every
   array simultaneously.
2. Spill queues, frontiers, contractions, candidates, and output into fixed
   memory runs.
3. Recompute selected information when another scan is cheaper than retaining
   an edge-sized intermediate.
4. Materialize narrow answers or semantic quotients for repeated queries.

## Concrete FastRP Example

FastRP over approximately one billion nodes at 128 dimensions produces roughly
a **533 GB final f32 embedding matrix**. That result cannot fit into 32 GB RAM.
The strict plan can nevertheless:

1. Compute approximately 4-16 million rows at a time.
2. Keep only their feature and operator blocks resident.
3. Write completed embedding slabs to NVMe.
4. Reuse a fixed 16-64 GB memory capsule.

The final output remains 533 GB. It simply never becomes resident all at once.
The likely cost is repeated sparse-dense streaming and a runtime approximately
`3-20x` slower than an all-resident implementation.

## Important Confidence Correction

The Node2Vec estimate contains two claims with very different confidence:

- **85% confidence:** the multi-terabyte stored walk matrix can be eliminated
  by generating walks deterministically in bounded batches.
- **40% confidence:** the complete Node2Vec training job can run usefully in
  24-72 GB, because paged random embedding updates may make it painfully slow.

Similarly, GraphSAGE inference can be paged, but this does not yet support a
claim that full GraphSAGE training fits efficiently in 16-64 GB.

## Enforcement Requirement

`mmap` alone does not guarantee a resident-RAM limit. The implementation needs:

- a controlled buffer pool;
- fixed state capsules;
- bounded worker concurrency;
- explicit spill behavior;
- streamed or bounded output;
- cgroup and whole-process RSS enforcement;
- page-fault and resident-page receipts;
- and preferably direct or carefully managed I/O.

Without these controls, the operating-system page cache can quietly consume
the missing hundreds of gigabytes and invalidate the strict-RAM claim.

## Precise Product Claim

> Some terabyte-scale graph jobs can run correctly with tens of gigabytes of
> resident RAM by becoming external-memory algorithms. They retain
> terabyte-scale disk footprints and usually exchange RAM savings for
> additional I/O and latency.

This should be presented as a Pareto choice, not a universal performance win:

| Plan | Resident RAM | Disk and I/O | Expected latency | Appropriate claim |
| --- | ---: | --- | --- | --- |
| Resident speed | Highest | Lowest runtime I/O | Lowest warm latency | Compete with GDS on speed |
| Balanced | Medium | Compressed and selectively paged | Near-memory when locality is favorable | Lower RAM with acceptable latency |
| Strict RAM | Fixed low envelope | Highest paging, spill, and temporary storage | Commonly much slower | Finish exactly within a hard budget |
| Materialized answer | Tiny query footprint | Pay build and freshness cost earlier | Lookup or sequential scan | Extremely cheap repeated queries |

The verification loop must report both the gain and the displaced cost:

```text
peak resident RAM
+ retained disk
+ temporary disk
+ bytes read and written
+ cold and warm latency
+ result correctness
+ artifact build time
+ freshness lag
```

Only the complete receipt can establish whether a strict-RAM scenario is a
useful product improvement rather than merely a technically possible run.

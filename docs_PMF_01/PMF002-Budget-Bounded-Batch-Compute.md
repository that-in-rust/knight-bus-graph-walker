# PMF002 Budget Bounded Batch Compute

## Governing Thesis

Knight Bus should begin as a graph-compute product but be architected as a
**budget-addressed batch execution system**.

Its fundamental product contract is not merely "use less RAM." It is:

> Given a data snapshot, computation, correctness requirement, RAM ceiling,
> storage allowance, and deadline, compile a physical plan that finishes
> inside the resource envelope, explicitly degrades, or refuses before wasting
> the customer's time and money.

Graphs are the best initial wedge because graph algorithms have radically
different state shapes and Neo4j/GDS strongly exposes the cost of assuming that
every useful computation belongs in an all-resident projection. The longer-term
platform opportunity is broader: recurring analytical and machine-learning
jobs whose users will trade elapsed time for a smaller and more predictable
machine.

The correct expansion is therefore:

```text
custom graph OLAP
       |
       | proves hard-RAM execution, receipts, and algorithm-shaped storage
       v
resource-contract runtime
       |
       | adds row/column batch operators without rebuilding SQL
       v
recurring batch ML
       |
       | k-means, scoring, linear training, selected external-memory kernels
       v
budget-addressed compute portfolio
```

This is not a recommendation to replace Spark, DuckDB, Parquet, or XGBoost.
Those systems should be inputs, baselines, collaborators, and distribution
surfaces. Knight Bus should own the narrow layer they do not consistently
provide: a workload-specific plan bound to a whole-process resource contract,
with a machine-verifiable estimate-versus-actual receipt.

---

## Situation, Complication, Question, Answer

### Situation

Many large analytical and ML jobs are not interactive. They run hourly, daily,
weekly, or after a new data generation arrives. For these jobs, the buyer may
care more about completion before a business deadline than about the lowest
possible wall-clock latency.

Spark, DuckDB, XGBoost, and other engines already partition, spill, stream,
cache, and process data larger than memory. Parquet already provides columnar
storage, row groups, compression, and page-skipping metadata. External-memory
execution is established computer science, not new whitespace.

### Complication

The user experience is still commonly machine-first:

```text
choose cluster or executor sizes
        -> submit job
        -> discover skew, spill, memory pressure, or OOM at runtime
        -> tune partitions and caches
        -> rerun
```

Resource settings exist, but they are not always a complete per-job promise.
Heap, native allocations, Python workers, mapped pages, page cache, output,
temporary files, concurrency, and algorithm-specific state can escape a simple
memory-limit setting. A job may technically spill yet become economically
useless because the execution plan crosses an I/O cliff.

### Question

Can Knight Bus let the user specify the machine budget and deadline first, then
compile a correct plan around those constraints, initially for graphs and later
for recurring data-engineering and ML workloads?

### Answer

Yes, for a meaningful but bounded class of workloads. The system can make RAM
an explicit input to physical planning by combining fixed state capsules,
bounded concurrency, external-memory algorithms, workload-shaped artifacts,
streamed output, pre-run admission, and calibrated receipts.

It cannot abolish information-theoretic state, storage traffic, output size,
or compute. When logical state exceeds RAM, the displaced cost reappears as
NVMe capacity, bytes moved, extra passes, recomputation, lower concurrency,
approximation, or freshness lag. The product succeeds only when that displaced
cost is cheaper than the cluster capacity it replaces.

---

## The New Product Primitive

### A Job Is A Resource Contract

A request should look conceptually like this:

```text
data:             snapshot-2026-08-07
operation:        exact-kmeans(k=100, iterations<=20)
correctness:      deterministic-f32
ram_limit:        5 GiB whole-process peak
deadline:         06:00 local time
temporary_disk:   2 TiB
output:           streamed parquet
failure_policy:   refuse-before-run
objective:        minimize estimated infrastructure cost
```

For a graph job, `operation` might instead be exact reachability, PageRank,
WCC, Node Similarity top-k, or a materialized answer refresh. The contract is
the same even though the physical state is different.

### The Planner Solves A Constrained Optimization Problem

```text
minimize:
    expected infrastructure cost
or:
    expected completion time

subject to:
    peak_whole_process_rss <= ram_limit
    temporary_disk         <= disk_limit
    completion_time        <= deadline
    result_quality         >= correctness_contract
    output_behavior         = bounded_or_streamed
```

The planner enumerates only implementations whose hard terms fit. It estimates
modeled terms from artifact statistics and historical receipts. It removes
dominated plans, presents the remaining Pareto choices, and admits one only
when its upper confidence bound is acceptable.

### Five User-Visible Plans

| Plan | Physical strategy | RAM | Time | Appropriate promise |
| --- | --- | ---: | ---: | --- |
| `RACE` | Resident algorithm-native artifact and maximum admitted lanes | Highest | Lowest warm latency | Compete on speed |
| `BALANCED` | Compressed blocks, important state resident, bounded cache | Medium | Moderate | Lower RAM without severe slowdown |
| `STRICT` | Fixed windows, external runs, paged state, streamed output | Hard low ceiling | Usually slowest | Complete exactly inside a machine budget |
| `APPROX` | Sampling, sketches, quantization, early stopping | Configurable | Potentially low | Explicit quality/cost trade |
| `ANSWER` | Generation-bound result or sufficient statistic | Tiny query RAM | Lookup or scan | Pay build and freshness cost earlier |

The names are less important than the product behavior. A customer must see
that more RAM purchases concurrency and locality, while less RAM purchases a
smaller machine at the cost of passes and I/O.

---

## A Hard RAM Limit Is Possible But Conditional

### The Whole-Process Equation

```text
M_peak = M_runtime_floor
       + M_resident_artifacts
       + M_algorithm_state
       + M_worker_scratch(admitted_workers)
       + M_output_window
       + M_io_buffers
       + M_page_cache_allowance
       + M_build_overlap
       + safety_margin
```

The system must either control or conservatively reserve every term. A heap
limit is insufficient. `mmap` is insufficient. A spillable operator is
insufficient when another part of the process can grow without bound.

At admission time, the runtime should answer:

1. What bytes are immutable and exactly known from artifact lengths?
2. What state is proportional to vertices, dimensions, clusters, or top-k?
3. What state is multiplied by concurrency?
4. Which structures have hard capacity and which have workload-dependent
   occupancy?
5. What happens when a bounded structure fills?
6. Can output exceed RAM, and where will it stream?
7. Does the build phase overlap old and new generations?

### Five GiB Means Less Than Five GiB Of Useful State

A 5 GiB process might reserve its budget as follows:

| Category | Example reservation |
| --- | ---: |
| Runtime, stacks, libraries, allocator | 512 MiB |
| Control plane and telemetry | 128 MiB |
| Registered I/O buffers | 512 MiB |
| Output window | 256 MiB |
| Worker scratch | 1 GiB |
| Algorithm state and hot artifact pages | 2.1 GiB |
| Safety margin | 512 MiB |

The exact split is plan-specific. The point is that a 5 GiB product promise
cannot allocate 5 GiB to the main array and hope the rest is negligible.

### The System Must Own Pressure Behavior

When a bounded region fills, the action is declared before execution:

```text
frontier full        -> compact or emit sorted run
bucket window full   -> spill immutable bucket run
candidate bound high -> partition, approximate explicitly, or refuse
worker state too big -> reduce concurrency
output window full   -> backpressure and flush
page budget reached  -> evict through controlled buffer policy
deadline endangered  -> surface forecast; never silently violate correctness
```

Linux cgroups or an equivalent process-level mechanism provide the final
enforcement boundary. The runtime should measure RSS, mapped residency,
major/minor faults, direct allocations, temporary bytes, and output bytes.

### Time Does Not Increase Smoothly As RAM Falls

The user may say, "I will wait one additional hour if RAM stays under 10 GB."
That is a valid contract, but not every workload has a plan satisfying it.

Reducing RAM can cause phase transitions:

- a hash table becomes a partitioned external aggregate;
- one pass becomes several radix or sort passes;
- a hot vector becomes repeated NVMe reads and writes;
- source-parallel execution becomes serial;
- random access loses locality and collapses device throughput.

Therefore the planner quotes a range, not a linear conversion:

```text
10 GiB plan: expected 75-95 min, p95 <= 120 min
 5 GiB plan: expected 140-240 min, p95 <= 330 min
 3 GiB plan: infeasible under exactness and deadline
```

If no plan fits, the system asks the user to relax RAM, deadline, exactness,
temporary disk, or output requirements. Refusal is a product feature because
it prevents expensive late failure.

---

## Existing Systems Validate The Mechanism But Narrow The Whitespace

### Spark Already Spills And Adapts

Spark divides memory into execution and storage regions, serializes cached
data, spills shuffle state, and recommends increasing parallelism when a
reduce task's grouping state is too large. Adaptive Query Execution can
coalesce shuffle partitions and split skewed work.

Primary references:

- [Spark memory tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Spark SQL Adaptive Query Execution](https://spark.apache.org/docs/3.5.5/sql-performance-tuning.html)
- [Spark executor memory configuration](https://spark.apache.org/docs/3.5.6/configuration.html)

Spark primarily allocates executors and tasks across a distributed
application. Knight Bus should not claim that spill, memory configuration, or
adaptive partitioning is novel. Its narrower claim is pre-run compilation of
an algorithm-specific plan into a whole-job envelope, followed by a receipt
that reconciles the estimate with the observed peak and displaced costs.

### DuckDB Already Runs SQL Larger Than Memory

DuckDB supports larger-than-memory grouping, joining, sorting, and windowing
through disk spill. It exposes memory and temporary-directory settings. Its
limits documentation notes that `memory_limit` applies to the buffer manager,
which illustrates why a whole-process contract is a distinct and stricter
claim.

Primary references:

- [DuckDB larger-than-memory workloads](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads)
- [DuckDB resource limits](https://duckdb.org/docs/current/operations_manual/limits)
- [DuckDB configuration](https://duckdb.org/docs/stable/configuration/overview)

Generic SQL aggregation is therefore an unattractive first expansion. It is
well served and operationally subtle. Knight Bus should use DuckDB or another
established engine for relational preparation where possible.

### Parquet Is A Substrate, Not The Runtime

Parquet stores columns in row groups and column chunks, with compression,
encoding, metadata, and optional indexes that allow readers to avoid irrelevant
pages. It is an excellent source and sink for bounded execution, but it does
not choose an algorithm's live state, spill schedule, convergence policy, or
whole-process budget.

Primary references:

- [Apache Parquet file format](https://parquet.apache.org/docs/file-format/)
- [Apache Parquet column chunks](https://parquet.apache.org/docs/file-format/data-pages/columnchunks/)
- [Apache Parquet row-group configuration](https://parquet.apache.org/docs/file-format/configurations/)

The architecture should reuse Parquet and Arrow-compatible batches rather than
invent another universal tabular storage format.

### XGBoost Already Proves External-Memory ML

XGBoost provides external-memory training through `ExtMemQuantileDMatrix` for
histogram-based tree methods. Its documentation explicitly warns that CPU
external-memory training becomes disk-I/O limited and recommends choosing
batches from available memory.

Primary reference:

- [XGBoost external-memory training](https://xgboost.readthedocs.io/en/release_3.2.0/tutorials/external_memory.html)

This validates the market behavior but weakens a generic "low-RAM boosted
trees" wedge. Knight Bus would need meaningfully stronger budget enforcement,
receipts, cost planning, portability, or recurring-job compilation to earn a
place beside XGBoost.

---

## Graphs Remain The Best Initial Wedge

Graph algorithms reward physical specialization more than generic relational
operators because each family has a different dominant state:

| Family | State problem | Useful custom action |
| --- | --- | --- |
| Reachability | Dense result arrays and variable frontiers | Delete unused result lanes; virtualize frontier |
| WCC | Dense parent structure | Narrow DSU; shard and merge quotient |
| PageRank | Message vectors and repeated scans | Destination-major pull tape; tiled rank slabs |
| Paths | Unbounded queues and predecessor state | Bounded buckets; spill runs; route indexes |
| Louvain | Old/new graph overlap and worker tallies | Stream contraction; narrow community state |
| Similarity | Candidate explosion and top-k output | Rare-first postings; exact bounds; fixed output |
| Node2Vec | Materialized walks and large models | Deterministic walk regeneration; paged model shards |
| Embeddings | Multiple `V x D` matrices | Rolling matrices; streamed feature/output slabs |

A generic engine can spill many of these structures, but an algorithm-shaped
artifact can avoid creating them, change their representation, or replace the
base graph with a semantic quotient or answer. That is where the graph product
can create defensible differentiation.

The first proof remains:

```text
exact bounded reachability
        +
hard 5/10 GiB envelopes
        +
GDS differential oracle
        +
cold/warm latency and I/O receipt
        +
refuse-before-run behavior
```

---

## Recurring Batch ML Is The Best Adjacency

### Workload Selection

| Workload | 5-10 GiB feasibility | Existing competition | Knight Bus opportunity | Priority |
| --- | ---: | --- | --- | ---: |
| Daily propensity scoring | Very high | Spark, SQL engines, model servers | Hard budget, single-node simplicity, streamed receipt | 1 |
| Exact/mini-batch k-means | High | Spark MLlib, scikit-learn variants | Clear state math and time/RAM curve | 1 |
| Logistic/linear training | High for moderate model width | Spark, Vowpal Wabbit, many libraries | Deterministic bounded epochs over Parquet | 2 |
| Batch embedding inference | Medium-high | Framework/data-loader stacks | Artifact-shaped feature pages and bounded output | 2 |
| Histogram GBDT | High | XGBoost, LightGBM ecosystems | Weak unless envelope/receipt is materially better | 3 |
| Huge `GROUP BY` | High | DuckDB, Spark, ClickHouse and others | Little differentiated whitespace | Avoid initially |
| General joins and sorts | High | Mature database and Spark engines | Infrastructure burden, weak wedge | Avoid initially |
| Deep-model training | Low-medium | Mature accelerator/offload systems | Model/optimizer floor often dominates | Avoid initially |

The adjacency is not "data engineering" as a whole. It is **recurring batch
algorithms with small sufficient state, streamable input, streamable output,
and meaningful deadline slack**.

---

## K-Means Is The Cleanest Non-Graph Proof

### Why Its State Can Be Bounded

For Lloyd k-means with `N` rows, `D` dimensions, and `K` clusters, the dataset
need not be resident. Each iteration needs:

```text
centroids               = K * D * value_width
cluster partial sums    = workers * K * D * accumulator_width
cluster counts          = workers * K * count_width
feature batch           = batch_rows * D * input_width
assignment/output       = bounded window or streamed column
```

The central live state depends primarily on `K`, `D`, workers, and batch size,
not directly on `N`.

For `K=100`, `D=128`, and f32 centroids:

```text
100 * 128 * 4 bytes = 51,200 bytes
```

The centroids are tiny. The dominant choices are input batch size, worker
partial sums, decoding, and whether assignments are retained. A 5 GiB process
can scan a multi-terabyte feature table if each row group is decoded into a
bounded batch and assignments stream to disk.

Spark's KMeans documentation recommends caching the input for performance
because the algorithm is iterative. Knight Bus deliberately explores the
opposite Pareto point: reread or retain a compact algorithm-shaped feature
artifact when RAM is more expensive than additional scans.

Primary references:

- [Spark MLlib k-means](https://spark.apache.org/docs/latest/ml-clustering.html)
- [Spark KMeans API caching note](https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/clustering/KMeans.html)

### K-Means Physical Plans

| Plan | Input representation | Resident state | Main cost |
| --- | --- | --- | --- |
| `RACE` | Feature blocks cached or memory-mapped hot | Centroids, all hot blocks, wide worker partials | RAM |
| `BALANCED` | Quantized/encoded row groups with bounded cache | Centroids, several batches, worker partials | Decode plus moderate rereads |
| `STRICT` | Sequential feature slabs and streamed assignments | Centroids, one/two batches, fixed workers | One full scan per iteration |
| `APPROX` | Coreset or mini-batch sample | Centroids and sample | Statistical error |
| `ANSWER` | Persisted assignments and centroids | Query pages | Refresh on new generation |

### K-Means Verification

An exact plan needs:

- pinned initialization, including k-means|| behavior where applicable;
- pinned row order and floating-point accumulation order;
- empty-cluster policy;
- convergence and iteration-limit semantics;
- centroid and assignment differential checks;
- within-cluster sum-of-squares reconciliation;
- peak RSS, input bytes, temporary bytes, and scan count;
- and separate artifact-build and iterative-run time.

The first non-graph benchmark should compare one bounded single-machine plan
against Spark MLlib and a strong single-node baseline on the same Parquet
snapshot. It should not compare an inexpensive local machine with a poorly
tuned cluster and declare victory.

---

## Propensity Workloads Split Into Three Different Products

### Scoring Is The Strongest Candidate

Daily propensity scoring commonly applies a trained model to a newly prepared
feature generation. When the model fits in memory, execution is naturally:

```text
Parquet row groups
      -> bounded decode and feature transform
      -> resident model inference
      -> streamed id, score, and model-version output
```

RAM becomes approximately:

```text
model
+ transform dictionaries
+ one or two feature batches
+ worker scratch
+ bounded output buffers
+ runtime margin
```

The source table may be terabytes while resident RAM remains 2-10 GiB. The job
is primarily limited by storage bandwidth, feature decoding, and model compute.
This is the best adjacency because it is one-pass, recurring, easy to verify,
and naturally attached to a deadline.

### Linear Training Is Also Compatible

Logistic regression and related linear models can use streamed mini-batches or
full-batch gradient passes. The model and optimizer state scale with feature
width rather than row count. Exact equivalence depends on optimizer, order,
regularization, convergence, and floating-point policy; stochastic plans need
a quality contract rather than bitwise equality.

The RAM win is strongest for sparse or moderately wide features where the
parameter vector fits easily. Extremely wide categorical models can move the
parameter state itself beyond the target envelope and require partitioned or
feature-sharded optimization.

### Tree Training Is Feasible But Not Distinctive Yet

Histogram-based gradient boosting can quantize features and process external
pages, as XGBoost already demonstrates. Knight Bus should not prioritize this
until the generic runtime has proven a meaningfully better hard-envelope and
receipt experience. Scoring an existing tree model is much simpler and should
come first.

### Feature Engineering Should Be Delegated

Propensity pipelines often spend more time joining, filtering, windowing, and
aggregating features than running model inference. Those operators belong to
Spark, DuckDB, warehouses, or other mature engines initially.

The clean boundary is:

```text
existing data engine
        |
        | produces versioned Parquet/Arrow feature generation
        v
Knight Bus bounded algorithm runner
        |
        | produces predictions, model metrics, and resource receipt
        v
warehouse, lake, or downstream application
```

---

## The Economic Thesis Is Conditional, Not Automatic

### The Break-Even Equation

Low RAM saves money only when the reduced machine price outweighs the longer
runtime and added storage traffic:

```text
cluster_cost = cluster_hourly_cost * cluster_runtime

bounded_cost = small_machine_hourly_cost * bounded_runtime
             + temporary_storage_cost
             + read_write_io_cost
             + artifact_build_amortization

bounded plan wins when bounded_cost < cluster_cost
```

Ignoring secondary costs for intuition:

```text
maximum tolerable slowdown
    approximately
cluster hourly cost / bounded-runner hourly cost
```

If a cluster costs ten times as much per hour, a five-times-slower bounded job
can be materially cheaper. A twenty-times-slower job may be more expensive.
The actual comparison must include cloud pricing, utilization, retries,
egress, attached storage, operations, and whether the cluster is already paid
for and busy.

### The Best Buyer Has Slack But Values Predictability

The strongest initial buyer profile is not merely "cannot afford RAM." It is:

- runs a large job repeatedly;
- has a deadline measured in hours rather than milliseconds;
- overprovisions a cluster to avoid uncertain peaks;
- can provide stable or versioned inputs;
- can stream outputs;
- owns fast local or attached NVMe;
- and values a guaranteed machine envelope more than the fastest possible run.

Potential jobs include nightly graph risk analysis, daily segmentation,
propensity scoring, recommendation candidate refresh, embedding generation,
and periodic model evaluation.

### The Receipt Is The Economic Proof

Every run should emit:

```text
resource-contract.json
estimate-receipt.json
correctness-receipt.json
output-manifest.json
```

The estimate receipt records:

- admitted and observed peak RSS;
- peak mapped residency and page-cache accounting;
- concurrency and maximum queue/frontier/batch occupancy;
- bytes read, written, spilled, and retained;
- cold and warm wall time;
- CPU time, I/O wait, and device throughput;
- artifact build and reuse count;
- output bytes and backpressure time;
- and estimated versus actual infrastructure cost.

The customer should not have to trust a benchmark slide. Their own repeated
receipts should show whether the slower, smaller plan is economically superior.

---

## Product Strategy

### Do Not Build A Spark Replacement

The broad data platform contains mature execution engines, catalogs,
schedulers, connectors, security models, SQL semantics, observability,
autoscaling, fault tolerance, and years of operational edge cases. Rebuilding
that surface would bury the differentiated kernel.

Knight Bus should instead expose three integration modes:

1. **Local CLI/library:** run against local Parquet, Arrow, graph snapshots, or
   object-store ranges.
2. **Sidecar/operator:** accept jobs produced by Spark, Airflow, Dagster, or a
   warehouse pipeline and return outputs plus receipts.
3. **Kernel library:** let another engine call a budget-bound algorithm plan
   inside an executor or native extension.

### Product Horizons

| Horizon | Product | What must be proven | Expansion gate |
| --- | --- | --- | --- |
| H1 | Graph resource-contract runner | Exactness, 5/10 GiB completion, honest latency, GDS comparison | Real users value the envelope |
| H2 | Generic capsule/spill/receipt substrate | Reusable bounded execution without graph assumptions | Second algorithm family reuses most infrastructure |
| H3 | K-means and propensity scoring | Parquet input, deterministic results, cost advantage | Beats strong single-node and Spark baselines for a real recurring job |
| H4 | Selected batch algorithm portfolio | Planner selects plans by deadline and budget | Repeatable demand across multiple jobs |
| H5 | Ecosystem integrations | Operational value without owning the data platform | Partners and workflows pull adoption |

The project should earn each horizon. A technically reusable allocator is not
evidence that a broad platform has product-market fit.

---

## Verification-First Development Program

### Contract Tests

Every physical plan needs executable requirements:

```text
WHEN a job is admitted with a 5 GiB whole-process limit
THEN peak measured RSS SHALL remain below the declared envelope
AND output SHALL stream without unbounded buffering
AND correctness SHALL satisfy the named oracle
AND spill SHALL remain below its admitted maximum
AND the receipt SHALL reconcile estimated and observed categories
AND the job SHALL refuse before execution when no feasible plan exists
```

### Test Families

1. **Semantic:** compare against GDS, Spark MLlib, XGBoost, or another named
   oracle under an explicit equality relation.
2. **Metamorphic:** vary row/edge order, partition count, batch size, worker
   count, and block boundaries without changing the answer contract.
3. **Resource:** run under 5, 10, and 20 GiB cgroups and verify whole-process
   peaks rather than allocator counters alone.
4. **I/O:** capture cold/warm reads, writes, faults, spill amplification, and
   device saturation.
5. **Deadline:** compare predicted and observed completion distributions.
6. **Failure:** inject full disks, slow disks, cancellation, corrupt blocks,
   worker failure, and output backpressure.
7. **Adversarial:** use skew, giant rows, high-cardinality groups, dense graph
   regions, empty clusters, and outputs larger than RAM.
8. **Economics:** calculate actual machine-hour and storage cost against the
   incumbent plan.

### Benchmark Matrix

| Axis | Required values |
| --- | --- |
| RAM envelope | 5, 10, 20 GiB and resident baseline |
| Cache state | cold and warm |
| Storage | local NVMe and representative attached volume |
| Data scale | fits RAM, 2x RAM, 10x RAM, and output-larger-than-RAM |
| Distribution | uniform, skewed, adversarial |
| Correctness | exact, tolerance-bound, approximate where declared |
| Concurrency | one worker through saturation |
| Output | scalar, top-k, full streamed result |
| Comparison | incumbent default and carefully tuned incumbent |

No product claim should use only the friendly cell of this matrix.

---

## PMF Hypotheses

### Hypothesis 1: Buyers Will Trade Time For A Hard Machine Envelope

Evidence required:

- users can name jobs where a two-to-ten-times slowdown is acceptable;
- those jobs are currently overprovisioned or unreliable;
- the smaller plan materially reduces actual cost;
- and refusal/receipts change operational behavior rather than merely looking
  reassuring.

### Hypothesis 2: A Single-Machine Plan Can Replace Some Cluster Jobs

Evidence required:

- data locality or object-store access does not erase savings;
- NVMe endurance and temporary space remain operationally acceptable;
- artifact build cost amortizes across repeated runs;
- and one machine's longer failure domain is mitigated by checkpoints.

### Hypothesis 3: Graph Specialization Produces A Better First Wedge

Evidence required:

- custom storage removes state or work that generic spill cannot;
- users need the supported graph jobs;
- GDS parity is achieved;
- and the product wins on a buyer-visible RAM, cost, or predictability metric.

### Hypothesis 4: The Runtime Generalizes Without Becoming A Platform Trap

Evidence required:

- k-means or scoring reuses admission, capsules, I/O, receipts, and artifact
  machinery;
- less than half of the new implementation is workload-specific runtime code;
- and integration through Parquet/Arrow is enough without building a catalog,
  scheduler, SQL optimizer, or distributed control plane.

---

## Falsifiers

The thesis should be rejected or narrowed if any of the following persists:

1. The 5/10 GiB graph plans are so slow that users prefer renting more RAM.
2. Page cache, runtime, or output repeatedly breaks the hard envelope.
3. Artifact builds cost more RAM or time than the jobs they are meant to save.
4. Customers cannot identify recurring jobs with meaningful deadline slack.
5. Strong single-node engines already match the envelope and cost without new
   artifacts or operational complexity.
6. Spark cluster costs are sunk and incremental jobs are effectively free.
7. Attached-storage I/O pricing or throttling erases compute savings.
8. Exactness requirements force full state residency for the target workloads.
9. Approximate plans are acceptable but existing tools already dominate them.
10. The generic runtime requires rebuilding SQL, orchestration, or distributed
    fault tolerance before delivering customer value.

---

## Recommended Sequence

### Phase 1: Prove The Primitive In Graphs

Build one exact reachability slice with 5, 10, and resident-RAM profiles.
Measure GDS parity, build cost, peak RSS, I/O, cold/warm latency, output, and
refusal. This validates whether a hard resource contract is real.

### Phase 2: Prove A Global Iterative Graph Algorithm

Add PageRank. It tests dense state vectors, repeated scans, deterministic
reduction, tiling, and the boundary between near-memory and strict execution.

### Phase 3: Extract The Generic Runtime

Separate:

- admission and Pareto planning;
- state capsules and bounded workers;
- block readers and writers;
- external runs and merges;
- cgroup/RSS enforcement;
- receipts and calibration;
- snapshot and artifact identity.

Do not generalize graph-specific layouts prematurely.

### Phase 4: Add Streaming K-Means

Use Parquet/Arrow feature generations, exact deterministic Lloyd semantics,
bounded batches, streamed assignments, and 5/10/20 GiB profiles. Compare with
Spark MLlib and a strong single-node baseline.

### Phase 5: Add Daily Propensity Scoring

Start with an existing model format and one-pass scoring. Prove that a
versioned feature generation larger than RAM can be scored cheaply and
predictably before considering training.

### Phase 6: Decide The Company, Not Merely The Architecture

Choose among three evidence-backed outcomes:

| Outcome | Meaning | Strategic response |
| --- | --- | --- |
| Graph wedge is strong, adjacency weak | Graph users pay; batch ML is crowded | Remain the resource-predictable graph engine |
| Graph and batch ML both pull | Runtime generalizes and buyers value it | Build budget-addressed compute portfolio |
| Technical proof works, willingness to pay does not | Impressive system without urgent job | Open-source, reposition, or stop before platform expansion |

---

## Final Position

The improved thesis is not:

> Rust and NVMe make every large algorithm cheaper than Spark or Neo4j.

It is:

> Many recurring analytical jobs have more deadline slack than memory slack.
> By compiling algorithm-specific external-memory plans into hard whole-process
> envelopes, Knight Bus can let users spend time, storage, or controlled
> approximation instead of automatically buying peak RAM. Graph computation
> is the differentiated proving ground; k-means and propensity scoring are the
> best adjacent tests; mature SQL and ML engines remain collaborators and
> baselines rather than targets for wholesale replacement.

The product becomes valuable when it can say, before execution:

```text
This exact job will finish in 75-95 minutes,
remain below 10 GiB resident RAM,
use at most 180 GiB temporary NVMe,
stream a 42 GiB result,
and refuse safely if the contract cannot be honored.
```

That is a stronger promise than low RAM alone. It turns resource trade-offs
into a user-visible, verifiable product surface.


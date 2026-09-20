# End To End Resource Bounded Graph Analytics

Date: 2026-09-19. Status: proposed product and engineering contract, not an implemented capability or completed research synthesis.

## The Product Thesis

The user does not purchase a smaller graph representation. They purchase a useful, trustworthy answer from their data, within a machine budget and before their decision deadline, without having to operate a complicated analytical system.

Our current hypothesis is therefore:

> Bring a graph snapshot, select a supported analytical job, and choose a resource envelope. Knight Bus prepares and executes it within that envelope, reports the snapshot and semantics used, and makes the next scheduled run manageable too.

This is an interpretation using product judgment inspired by Shreyas Doshi, not a statement attributed to him. The existing strategic anchor is [A007](../docs_PRD04/A007-spc-founder-interview-prep-v7.md). It points toward security, dependency, and access-path workloads; it does not yet prove willingness to pay.

The main question is not whether some algorithm can use little memory. It is whether a particular customer can finish their entire job more affordably and reliably than their best available alternative.

## Corrections To The Starting Assumptions

### The Input Is Simple Only After We Define Its Meaning

An edge list is a good first interface. Neo4j's property graph also carries node labels, relationship types and direction, and properties. These affect the computation. Our first importer can deliberately support a restricted projection, but must not silently throw away facts that change the answer. See [Neo4j graph concepts](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/).

For example, these are different jobs:

- Can account A reach resource B following only permitted relationship types?
- Is B reachable when edge direction is ignored?
- What is the cheapest path using a numerical edge weight?
- What is the shortest path in number of hops?
- What paths existed at a historical timestamp?

The same three-column CSV cannot communicate all those semantics unless the contract supplies them separately. Security reachability must also represent the customer's permission semantics correctly; ordinary graph reachability is not automatically an authorization evaluator.

### Analytical Does Not Mean Static

Live and incremental graph analytics already exist. For a concrete counterexample, [Memgraph Online PageRank](https://memgraph.com/docs/advanced-algorithms/available-algorithms/pagerank_online) updates an approximate PageRank computation as a graph changes. Its approximation and current Enterprise licensing are explicit; this is not proof of exact GDS parity or cheap updates for every algorithm.

Our recommendation to start with snapshots is a scope decision. It gives each answer a stable input, makes preparation reusable, and isolates the first resource-contract problem. It is not a claim that analytics cannot operate on changing data.

Source data may keep changing while we analyze a consistent older snapshot. The answer must say which snapshot it represents. Later versions can shorten publication intervals or introduce algorithm-specific incremental maintenance.

### Existing GDS Already Has A Preparation Stage

[Neo4j GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/) describes compressed in-memory projections and their reuse across analytical workflows. Its catalog also supports backup and restore. Compare our preparation and reuse against the corresponding incumbent workflow, not against a deliberately cold incumbent on every query.

## Three Sizes, Plus One Machine Limit

Use decimal units here: GB = 1,000,000,000 bytes.

| Quantity | Meaning | Current scenario |
| --- | --- | --- |
| Source size | CSV, source database, or export bytes | Unknown; measure independently |
| Prepared persistent size | Sealed topology, property planes, ID mappings, indexes and retained answer artifacts | Up to 50 GB, if that is the chosen product allowance |
| Peak live disk usage | All simultaneously live prepared files, old generations, staging, scratch, output and recovery files | Must have a separate limit; not automatically 50 GB |
| Physical RAM | Whole machine, including OS and every local process | 4 GB for this research goal |

The older PMF001 scenario used 50 GB to mean a GDS projected-memory footprint. That is a different benchmark. Neither meaning allows us to infer node count, edge count, compression ratio or execution time from 50 GB alone.

There is also a retention ambiguity to keep explicit. The conservative interpretation is a 50 GB cap on the union of unique retained prepared blocks, including pinned generations, selected indexes and saved answers. A 50 GB allowance for each active generation with additional old-generation storage is a looser alternative, not an assumed entitlement. Temporary workspace needs its own admitted cap in either case. The overlap examples below expose these tradeoffs; they do not authorize extra unbounded disk.

A provisional worker budget is 3 GB, reserving 1 GB for the OS and other overhead. These are design assumptions requiring measurement, not an established configuration. Running Neo4j on the same machine consumes part of the same physical budget. A separate 4 GB worker does not make the source database's memory disappear from whole-system economics.

## The Customer Journey

| Step | Customer action or expectation | Work we perform | What must be visible or bounded |
| --- | --- | --- | --- |
| 1. Define the question | Choose an analytical template and required result | Check algorithm, projection, parameters and output semantics | Supported/unsupported distinction, exactness, expected decision value |
| 2. Select data | Supply files or a Neo4j source projection | Establish credentials, source identity and snapshot boundary | Permissions, consistency, export burden and source-system impact |
| 3. Validate | Understand whether the data is usable | Stream validation of IDs, edges, types and required properties | Invalid-row policy, bounded error log, no silent semantic changes |
| 4. Preview choices | Choose capacity versus response-time tradeoff | Estimate candidate plans; distinguish sample-based predictions from proven allocation bounds | Build time range, peak RAM, peak disk, freshness and limitations |
| 5. Prepare | Wait for the first usable artifact | External ID mapping, sorting, encoding, optional indexing or precomputation | Build RAM, disk writes, source throttling, progress, cancellation and restart |
| 6. Publish | Begin querying one consistent version | Validate files and atomically publish an immutable generation | Generation identity, supported operations, checksums, retention policy |
| 7. Submit a query | Change allowed parameters, seeds or filters | Plan and admit a supported job against available resources | Queue delay, admitted budget, deadline policy and cancellation behavior |
| 8. Execute | Receive a useful result before a deadline | Run kernels, bounded external state, spills and merges | Memory high-water mark, bytes read/written, work completed and remaining uncertainty |
| 9. Consume results | Inspect, export, join or write back | Stream original IDs and requested fields under backpressure | Time to first useful answer, total output time, output-size limit and result completeness |
| 10. Refresh | Run again with newer data | Capture a new version, rebuild or safely reuse artifacts, publish | Answer age, overlap storage, effect on running queries and failure recovery |
| 11. Operate | Trust unattended jobs | Enforce quotas, recover, retain or delete generations | Cost per useful answer, failed jobs, stale answers and operator effort |

Preparation cannot be an invisible prerequisite. First-answer latency starts before import. A build progress indicator also must not imply an accurate completion estimate when skew or compression work is still unknown.

## What Must Be Declared At Submission

### Data And Semantics

| Parameter | Why it matters |
| --- | --- |
| V, E and stored arc count | Undirected expansion and duplicate preservation can change stored arc count from logical relationship count |
| Directed, undirected or mixed projection | Determines legal traversal and adjacency orientation |
| Node labels and relationship-type selection | A generic graph projection may be semantically wrong for the job |
| Stable external node IDs | Results must map back without identity corruption across snapshots |
| Duplicate-edge policy | Deduplication changes degrees, weights and sometimes the answer |
| Self-loop policy | Affects rank, degree, motifs and normalization |
| Isolated and missing nodes | An edge-only file omits isolated nodes unless separately declared |
| Edge weights and missing-value rules | Negative, nonfinite and missing weights require algorithm-specific handling |
| Property selection and types | Large strings and vectors can dominate storage even when topology is small |
| Input sort order, encoding and compression | Changes parsing work and external-sort requirements |
| Source snapshot or transaction boundary | Prevents mixing nodes and edges from incompatible times |
| Graph shape | Degree skew, components, repeated rows, overlap and diameter change actual work |

Counts and degree distributions are useful but insufficient to predict every graph query. The existing evidence ledger demonstrates two graphs with identical V, E and degrees but different reachability and triangles.

### Computation And Answer

| Parameter | Why it matters |
| --- | --- |
| Algorithm and version | PageRank, reachability and similarity have different state and work |
| Parameters allowed after build | Determines how reusable a specialized artifact is |
| Exactness contract | Mathematical equivalence, bounded numerical error, approximate recall and bitwise equality are different promises |
| Initial state and stopping rule | Fixed iterations are not identical to the incumbent's convergence rule |
| Seed and deterministic ordering | Tie breaks and floating-point reduction order can affect repeatability |
| Query-local filters | Can invalidate a precomputed answer or a shared-equivalence representation |
| Output mode | Scalar, path, top-K and all-node/all-pair output have very different costs |
| Result limit and semantics | Truncating the output is not permission to return an incorrect top-K |
| Reuse count per generation | Determines whether extra preparation earns back its cost |
| Allowed answer age | Determines feasible refresh and publication schedules |

### Resource And Operational Contract

| Parameter | Why it matters |
| --- | --- |
| Whole-machine and worker RAM limits | A per-job allocator alone misses OS and concurrent-process usage |
| CPU allocation and concurrency | Parallelism multiplies buffers and can saturate shared bandwidth |
| Peak disk, prepared disk and output quota | A compact final artifact can still need large temporary files |
| Storage throughput and random-I/O behavior | Sequential bandwidth is not a random-access or tail-latency guarantee |
| Source egress and extraction limits | Remote export can dominate the first run and affect production |
| Job deadline and queue policy | Execution time alone is not customer wait time |
| Foreground versus background priority | Refresh and build can interfere with interactive reads |
| Cancellation and recovery budgets | Cancelled work must release resources; retries must not double the footprint |
| Retention and reader pinning | Long-running readers can keep old generations alive |
| Durability and availability | Rebuild-after-crash and uninterrupted serving need different extra storage |
| Security and tenant boundaries | An answer cache must not bypass authorization or mix tenants |
| Telemetry retention | Logs, receipts and diagnostic samples also need bounded storage |

## Known Queries: Three Very Different Products

| Product shape | What is known at build time | What can change later | Consequence |
| --- | --- | --- | --- |
| Algorithm-ready artifact | Graph projection and an algorithm family | Supported parameters and seeds; filters only where implemented | Broadest useful reuse, still runs real computation per job |
| Workload-specialized artifact | Family plus workload distribution or selected query templates | Declared subset of seeds, filters and parameters | More indexes or summaries, greater build and invalidation burden |
| Answer artifact | Exact projection, snapshot, algorithm, parameters and output contract | Usually result lookup, pagination or safe derived views | Very fast delivery after preparation; computation was moved earlier |

For example, storing an inbound PageRank layout need not fix the damping factor. Storing completed scores does. Changing a relationship filter may change the transition matrix and require a new computation. Similarly, a reachability index for one projection is not automatically correct for a query that excludes some relationship types.

Recommendation: start with an algorithm-ready artifact for one valuable question family. Add answer artifacts where repeated identical jobs are observed. Do not require every customer to predict every query in advance.

## Full Lifecycle Resource Accounting

### Memory

For one execution stage:

```text
worker charged memory = runtime + I/O buffers + decoded blocks
                      + algorithm state + sort/merge workspace
                      + output buffers + metadata + headroom

whole-machine memory = worker and other process consumption
                     + the relevant kernel/OS consumption
```

Use one authoritative accounting view and non-overlapping explanatory categories. RSS, mapped-file residency and page cache overlap; summing them naively double counts. A container cap can contain a failure without ensuring the job completes. Internal resource reservations and disk-space planning are still needed.

The lifecycle peak is the maximum of simultaneously live resource use, not the sum of the independent maximum of every serial stage. Conversely, serving queries while building creates real overlap that a stage-by-stage maximum misses.

On a first 4 GB deployment, one heavy job at a time is a defensible baseline. Background preparation can be paused or time-sliced. More concurrency must be earned by explicit shared-pool accounting and measurements, rather than multiplying a per-job memory limit by an arbitrary number of workers.

### Disk

```text
peak local disk = max over time of (
  retained input + canonical intermediate data + active generation
  + pinned old generations + incomplete new generation
  + live scratch + retained output + recovery overhead
)
```

Only count physically shared immutable blocks once. Compression and copy-on-write can reduce overlap but cannot guarantee a small delta after arbitrary graph changes or global ID reordering.

Illustrative capacity check, not a requirement for every implementation:

| Simultaneously live item | GB |
| --- | ---: |
| Active prepared generation | 50 |
| New prepared generation | 50 |
| Staging | 20 |
| Live scratch | 30 |
| Retained results | 2 |
| Total before safety reserve | 152 |

If the total physical storage limit is 50 GB, this schedule is impossible. Options are a smaller artifact, streamed input, aggressive file lifetime management, sharing unchanged blocks, a maintenance window that relinquishes old availability, or additional local/remote capacity. They are different product contracts. We must ask which 50 GB the customer means.

The example also violates a 50 GB retained-prepared cap because its two distinct prepared generations alone total 100 GB, even if the disk has room for the 152 GB peak. Calling the replacement a temporary file cannot remove its physical charge or authorize a publication that exceeds the retained-prepared allowance. A successful schedule must satisfy both caps and its chosen availability/retention policy.

Do not remove the only valid old generation before a replacement is safe unless the customer explicitly selected a destructive/rebuildable workflow. Atomic publication requires either retained old data or an accepted availability tradeoff.

### Time

```text
first completed answer = extraction + validation + preparation
                       + publication + queue + execution + delivery

later completed answer = queue + execution + delivery

fresh-answer delay = source capture delay + refresh queue
                   + refresh/build + publication + query/delivery
```

Pipelined stages can overlap; their sum is a conservative serial schedule, not an assertion that all implementations must serialize. Measure end-to-end wall time separately from CPU and I/O service demand.

An output stream's first row is not always a useful answer. A partial rank list is not a final top-K unless the remaining work is proved unable to displace it. A positive reachability witness can be useful early; a negative answer needs a complete search or a valid exclusion certificate.

## When More Build Time Makes Sense

For the same graph version and answer semantics, ignoring overlap for clarity:

```text
our total time      = B_ours + N * Q_ours
baseline total time = B_base + N * Q_base

time break-even N > (B_ours - B_base) / (Q_base - Q_ours)
```

This threshold applies only when query savings are positive and the artifact remains valid. If our build is 1,800 seconds more expensive but an equivalent query drops from 60 seconds to 5 seconds, time breaks even after 33 queries. If the user runs once before every refresh, extra build time has not paid back in latency. Lower machine cost might still justify it, but that is a separate calculation.

Apply the same logic to paid compute, storage, I/O, extraction and operator effort. Customer value can justify a slower job if it finishes on available hardware before a daily deadline. That is a capacity/economics win, not a speed win.

Avoid comparing our answer cache to an incumbent that is forbidden to cache the same answer.

## Three Product Scenarios

### A. Scheduled Analytical Job

CSV or a consistent export arrives daily. A declared algorithm produces a complete result artifact before a deadline. Preparation and execution can share the entire worker budget serially. No always-on interactive serving is promised.

Best initial fit: a team whose existing scheduled graph job exceeds available RAM or requires an expensive machine but tolerates batch completion. The unit of value is a completed daily analysis, not a query per second.

Tradeoffs: slow first answer may be acceptable; snapshot acquisition, output usefulness and failed-job recovery remain essential. A low-memory kernel alone does not solve messy exports or permission modeling.

### B. Prepared Snapshot Serving

A version is prepared and reused for many supported queries. Users explore seeds, bounded neighborhoods, selected filters, or prepared scores. Refreshes publish new immutable generations.

Best fit: repeated investigation on a stable security or dependency snapshot. The unit of value is an investigator's completed question with traceable evidence.

Tradeoffs: query latency matters, but so do build delay, disk for extra indexes, authorization, reader-pinned generations and predictable refresh interference. A hot cache and a cold start are separate benchmarks.

### C. Continuously Maintained Analytics

A change feed updates algorithm-specific state or sealed micro-batches. Different algorithms can use different maintenance strategies. The output declares a watermark and may use explicitly approximate semantics where chosen.

Best fit: customers with demonstrated minute/second freshness requirements and enough recurring value to justify operational complexity.

Tradeoffs: inserts, deletes, replay ordering, recovery, state invalidation and worst-case recomputation. One deleted bridge can affect an enormous component. Small input delta does not imply small computational delta. Real-time ingestion and real-time exact answers are distinct promises.

Recommendation: earn A or B first based on one customer's workflow. Preserve a version/refresh interface that can support C later, but do not build a general streaming database to validate the initial product.

## Algorithm And Storage Implications

| Customer question | Baseline specialized representation | Additional option | Main failure mode to test |
| --- | --- | --- | --- |
| Can this identity reach this resource? | Typed adjacency plus bounded visited/frontier state | Reuse validated witnesses; certify some unchanged answers across snapshots | Wrong projection semantics; explosive frontier; false negative from invalid pruning |
| What is reachable from a selected set? | Direction-aware adjacency and external frontier runs | Batch shared-seed work where answers remain separable | Per-seed output/state multiplies; all-path enumeration is not ordinary reachability |
| Which entities are connected? | Streamable edges plus bounded component state | Component certificate inheritance under verified safe changes | Deletions split a component; high-vertex-count state exceeds RAM |
| Which nodes are influential? | Compressed scan layout with resident or external rank blocks | Shared-row class-mass PageRank where exact row equivalence exists | Low repetition; reconstruction/output cost; incompatible stopping semantics |
| Which entities have similar neighborhoods? | Sorted postings and exact candidate bounds | Shared exact-vector classes and reusable class-level top-K tables | U nearly N; dense candidate work; output explosion; query-specific filters |

The candidate mechanisms and restricted mathematical checks are in [Architecture Candidates V1](Architecture-Candidates-v1.md). No RAM reduction or latency estimate there is a measured claim for an arbitrary 50 GB graph.

## The Metrics That Decide Whether This Is A Product

| Metric | Required interpretation |
| --- | --- |
| Completed useful jobs under budget | Include admitted completions and rejected jobs; blanket refusal is not success |
| Time to first correct useful answer | Begins at the source input, not after an undocumented preprocessing job |
| Preparation elapsed time and peak memory | Include parsing, ID mapping, sort, compression and optional answers |
| Peak live disk and total bytes written | Include refresh, scratch, retries and output |
| Cold and warm query p50/p95/p99 | Declare sample count, workload distribution, concurrency and hardware |
| Deadline miss rate | More actionable than claiming a universal p100 bound from finite samples |
| Freshness | Measure from the source watermark, not only the latest file timestamp |
| Source-system impact | Production CPU, memory, I/O and tail latency during extraction |
| Recovery and cancellation | Time, rework bytes, released allocations and cleanup correctness |
| Total cost per useful answer | Include source, worker, storage, egress and human operation |
| Reuse per generation | Shows whether specialization/precomputation earns its complexity |
| Switching effort | Steps from current query/data source to validated comparable output |

RAM can be controlled by a carefully implemented schedule. Wall-clock time also depends on work, device stalls, scheduling and failures. A hard deadline can promise timely cancellation; it cannot promise a completed exact answer for every arbitrary graph. Distinguish a proven bound, a conditional bound, an estimate and a measured percentile on every quote.

## First Verification Package

1. One representative customer question, one documented graph projection and one independently checked answer contract.
2. A complete CSV workflow with string IDs, duplicates, missing endpoints, isolated nodes and bounded diagnostics; then a consistent Neo4j extraction path with measured source impact.
3. A source-to-answer run on a 4 GB physical machine or explicitly equivalent controlled environment, including the build. No larger hidden builder.
4. One 50 GB prepared-storage scenario described by V, E, properties, shape and artifact composition, not a size label alone.
5. Correct answers for representative and adversarial graphs, including high-degree nodes, long paths, many isolates, repeated topology and low-repetition counterexamples.
6. Disk-full, mid-build crash, cancelled query, slow consumer and refresh-with-pinned-reader cases. Previous valid data must remain usable under the selected availability contract.
7. A fair incumbent comparison with matching projection, numerical semantics, output and reuse. Separately show same-machine capacity and larger-machine economics.
8. A second run on changed data. Measure full refresh, safe reuse, freshness, peak overlap and query interference. First-run success alone is insufficient.

## Product Decision

The differentiator should be a useful workflow that reliably completes within an understandable resource envelope. Algorithm-shaped storage is the technical mechanism; preparation, freshness and operability determine whether the mechanism becomes a product.

The next customer question is: **How old may the graph be when this answer is used, and when must the first complete answer arrive?** That choice determines whether we optimize a scheduled job, a prepared investigative service, or incremental analytics. Until answered, use scheduled immutable snapshots as an explicit working assumption, not a universal truth about analytical workloads.

## What The Customer Should Actually Have To Decide

The long parameter inventory above is an engineering responsibility, not a forty-field onboarding form. Progressive disclosure matters: accept a simple dataset and question, infer what can be inferred, then ask only about unresolved semantics and tradeoffs that affect the answer.

The initial interaction should establish five things:

1. **Data:** which files or source projection, which ID columns, and what the relationships mean. Offer a preview of interpreted rows and rejected rows before a costly build. An edge-only file needs an explicit decision about isolated nodes.
2. **Question:** the supported analytical question, allowed parameters, and required output. "Show one shortest dependency path" and "enumerate all paths" must never be treated as the same request.
3. **Machine budget:** available physical RAM, total writable disk, and whether the source database shares that machine. Keep the prepared-artifact allowance separate from total workspace capacity.
4. **Waiting tolerance:** deadline for the first answer and acceptable delay for later queries. A one-off investigation and a nightly scheduled job need different plans.
5. **Freshness:** the newest source change that must be reflected, or the maximum acceptable age. Asking about freshness is more useful than asking the customer to choose "batch versus streaming architecture."

Do not require customers to select CSR, tiles, incidence layouts, compression codecs or rank-vector placement. Those are implementation options we must justify through the quoted plan and measured result.

## Three Plans For The Same Supported Question

These are proposed choices, not current functionality or measured speed estimates. All preserve the selected answer semantics unless the user separately chooses an explicitly approximate contract.

| Plan | Preparation | Subsequent queries | Resource tradeoff | Appropriate customer |
| --- | --- | --- | --- | --- |
| Minimal preparation | Build the smallest sufficient validated layout; avoid optional indexes | More scanning or external work | Lower upfront work and retained disk; may read more per query | One-off or infrequent analysis |
| Prepared investigation | Add only measured-beneficial orientation, filter or locality indexes | Faster supported parameterized questions when indexes help | More build/refresh cost and disk within the same RAM budget | Repeated exploration of one snapshot |
| Scheduled answer production | Build layout and compute the declared result before the user needs it | Retrieve or safely derive from the completed result | Computation moves before the serving window; result retention and invalidation cost remain | Recurring reports with stable parameters |

The preview must show an eligible plan or a specific incompatibility, not an unjustified precise quote. State separately: controlled-memory allowance; conservative disk requirement; estimated build/query time range; first-answer deadline risk; supported parameter changes; snapshot age; and additional storage or downtime required for refresh. Profiling itself needs bounded resources and elapsed-time accounting. Sample-derived compression or selectivity estimates must not masquerade as worst-case bounds.

If the customer supplies arbitrary Cypher outside the supported analytical subset, return a clear unsupported-query explanation. Do not silently send it back to a larger Neo4j instance and claim it ran within our worker budget. Any optional delegation must be visible, separately authorized where necessary, and included in cost and latency accounting.

## Parameters Need An Invalidation Map

A stored graph should advertise what can change without rebuilding, what requires recomputing a result, and what changes the graph itself. This avoids conflating a reusable layout with a cached answer.

| Customer change | Typical effect under the stated example contract |
| --- | --- |
| New starting node for traversal | Reuse typed adjacency; run a new traversal |
| New traversal depth limit | Reuse adjacency; recompute or use a separately proved reusable result |
| Select another already-supported relationship type | Reuse a type-aware layout if that filter is implemented; invalidate incompatible cached answers |
| Add a property filter whose column was omitted | Read a consistent compatible sidecar if available, or prepare a new projection; never ignore the filter |
| Change PageRank damping or personalization | The same complete topology may be reusable; completed scores generally are not |
| Increase top-k beyond the stored table's certified coverage | Recompute or extend exactly; do not present the old smaller table as sufficient |
| Receive newer data | Capture a new consistent generation; rebuild or apply a separately verified reuse/update rule |
| Lose permission to access some records | Invalidate affected access/result contracts before serving; an old snapshot is not an authorization bypass |

These examples are not universal compatibility guarantees. Each implementation profile needs its own exact dependency and invalidation declaration.

## Make The Second Run Part Of The Product Demo

The first demonstration should finish an actual customer question, then repeat it on changed data. This exposes costs that a polished one-shot kernel demo hides:

- Can a failed build resume without re-exporting the entire production source?
- Can an old valid snapshot still answer queries while a new one is being prepared within the chosen combined budget?
- What happens when a slow reader prevents deletion of the old generation?
- Can the customer see that a result is stale rather than mistaking its recent download time for fresh source data?
- Does a changed query reuse useful storage without making the customer rebuild everything?
- Are output rows usable with the customer's original IDs and downstream tools?

The primary product metric is **correct, useful jobs completed before their deadline within the declared lifecycle budget**. Pair it with admission/completion rates, first-answer time and operational effort. Lower RAM is the mechanism and capacity advantage; fewer expensive machines, fewer failed runs and less waiting or intervention are the customer outcomes to validate.

## A Quote Before Preparation Is Not A Blank Cheque

There is a circularity in an apparently simple onboarding promise: to know which representation fits and which query will be fast, we may first need to examine the data. That examination itself consumes time and resources. Solve this with a bounded preparation process, not a confident estimate derived from a small sample.

| Point in the workflow | What we can responsibly tell the customer | What remains conditional |
| --- | --- | --- |
| Before reading | Supported input/answer semantics, configured worker ceiling, initial workspace reservation, maximum profiling effort | Graph counts, compression, skew, useful reuse and completion time |
| After a bounded preview | Estimated sizes and times with declared sampling coverage; obvious semantic problems | Rare hubs, long strings, disconnected regions and other unsampled cases |
| After full validation and canonical counting | Actual input counts and widths; a conservative size calculation for specified uncompressed layouts; available fallback plans | Compression gains, query-specific reachable subgraphs, convergence and hardware stalls |
| After preparation | Actual retained bytes, artifact capabilities, generation identity, measured preparation work | Later query mix, cache state, concurrency and result sizes |
| After a query or refresh | Actual outcome, resource high-water marks and elapsed times for that run | Universal future guarantees or a population p100 inferred from finite observations |

The memory schedule can be bounded before knowing whether the whole job will finish. Completion additionally requires a finite supported computation, enough workspace/output capacity, working storage and a suitable execution window. Keep those two promises separate.

When the data invalidates a tentative disk or time estimate, the permitted actions are explicit: finish within a previously authorized fallback envelope; pause at a durable checkpoint; request a changed budget or narrower question; or terminate with a clear incomplete status. Do not silently increase RAM, upload data, borrow a larger worker, discard graph semantics, or present a truncated computation as a complete answer. Checkpoint retention and eventual cleanup also consume the quoted disk allowance.

For disk estimates, use a safe encoding fallback where possible. A promise based solely on expected compression fails on incompressible input. For variable-length IDs and properties, either charge and stream their real length or declare an input limit before accepting them. The user need not learn these implementation details, but our planner must.

## What Fast Means For Each Question

The same prepared graph can support several questions, but one response-time promise cannot cover them all. These are qualitative execution implications, not new benchmark claims.

| Question | What may make it fast | What cannot be hidden |
| --- | --- | --- |
| Return the direct dependencies of one item | One indexed adjacency row and selected property/ID lookups | A hub may have millions of dependencies; reading and transmitting them takes work |
| Is there a permitted path from A to B? | Early positive witness, good bidirectional search where legal, or a valid prepared certificate | A negative answer may need much more exploration; a cancelled search is not a negative answer |
| Return all affected items from a seed set | Shared traversal and streamed distinct endpoints | Whole-component traversal, sorting if required, and potentially enormous output |
| Return all PageRank scores | Reused scan layout, reduced mathematical state, or a valid completed score artifact | Iterations, numerical accuracy, full output and changes to damping/personalization |
| Return exact nearest neighborhoods | Complete candidate pruning and a bounded top-k structure | Broad overlaps can defeat pruning; a small k does not make candidate discovery free |

It is legitimate to offer a fast bounded-neighborhood service and a slower full-component job on the same snapshot. Label them as different supported operations. A user choosing the slower exact job is expressing a useful product preference, not causing an error.

## Freshness Is Part Of Latency

For recurring snapshots, a result has two clocks: when the user receives it and the source watermark it describes. A one-millisecond lookup of yesterday's scores is not a one-millisecond answer about the current source.

For a simple periodic schedule with snapshot interval H and a bounded post-snapshot capture/build/publication delay L, a source change just after a snapshot can wait approximately H+L before it appears in a published generation. Query/delivery time and any additional missed schedule must be added. This is conditional schedule arithmetic, not a universal freshness guarantee. A source that supplies only an inconsistent export or an unknown watermark prevents a trustworthy freshness quote.

Three independent choices should be visible:

1. Does the source change continuously? It may do so even for our first snapshot-only product.
2. How often do we publish a usable analytical version? Daily, hourly and micro-batched are different preparation loads.
3. Do we maintain algorithm results incrementally, or recompute them for a new version? That choice is algorithm-specific and does not follow automatically from receiving a change stream.

Snapshot-first therefore means **stable inputs for each computation**, not a requirement that the customer's business stop changing. Live/incremental analytics is established prior art; the cited Memgraph example was rechecked on 2026-09-20 local and explicitly describes approximate online PageRank. It does not establish a low-RAM or exact-parity guarantee for our product.

## Product Priorities Through An LNO Lens

This is our application of the product-prioritization lens associated with Shreyas Doshi, not his assessment of this project.

| Category | Work worth doing now | Why |
| --- | --- | --- |
| Leverage | One painful recurring question, a real representative dataset, an independent answer oracle, and the entire first/second run under budget | Establishes whether a smaller machine actually changes a customer's ability or cost to finish the job |
| Neutral but necessary | Source validation, stable IDs, supported query interface, useful output, cancellation and truthful freshness | Missing any of these can make an excellent kernel unusable; implement the minimum complete version |
| Overhead at this stage | Full Neo4j replacement, every Cypher feature, universal live maintenance, or a large menu of user-selected storage formats | Expands work before proving the specific customer outcome; these may become valuable later with evidence |

The most useful first adoption test is not "Would you like a lower-RAM graph engine?" It is: **Give us the last job you could not finish economically, its input/projection, the answer you needed, the deadline, and what you tried instead.** Then reproduce that job and run it a second time after a meaningful data change.

A proposed first product sentence is: **Run a supported dependency investigation or scheduled graph job on your available machine, from export through usable results, with explicit memory, disk and freshness limits.** Keep the algorithm-specific structural innovations as ways to fulfill that promise, not obligations the buyer must understand or accept.

## A Concrete Meaning Of Predictable Compute

Separate three levels: a bounded allocation schedule; a bounded amount of algorithmic work for a specified input/accuracy contract; and a wall-clock commitment conditional on the execution environment. We can sometimes establish the first two much more strongly than the third.

For example, the restricted [feature-state PageRank proposal](Feature-State-PageRank.md) now has an exact-arithmetic contraction bound in the norm used by its error certificate. It can derive an update-count allowance from graph counts, damping and numerical accuracy before solving. In its favorable billion-entity constructed example, 113 updates suffice for the stated 1e-8 L1 target, moving 2.26 TB of row payload before initial preparation and final output. That is about 4,520 seconds of row service at an assumed 500 MB/s, not a measured completion time or one-hour guarantee. Production floating error must also be bounded before publishing this as a certified plan.

This illustrates the proposed product distinction: **the memory and work envelope can be explicit even when the time estimate is conditional.** It also demonstrates why "fits in 4 GB" and "meets this user's deadline" are independent admission tests. Other algorithms need their own work model; this PageRank proof cannot be attached to an arbitrary Cypher query.

## A Concrete Initial Worker Allocation Plan

The following is a proposed set of allocation ceilings, not measured consumption or proof the current program enforces them. It turns the provisional 3 GB worker allowance into a testable design. Use decimal MB and a dedicated 4 GB environment; validate the remaining 1 GB OS/other-process allowance rather than assuming an arbitrary desktop already has that headroom.

| Simultaneously live category | Proposed ceiling, MB | Ownership rule |
| --- | ---: | --- |
| Runtime, stacks, parser/control state and bounded diagnostics | 350 | Includes native/FFI allocations; untracked allocations are a failed admission model |
| Current stage's principal arena | 1500 | External-sort state during build, algorithm state during execution, or refresh aggregation; not three simultaneous arenas |
| Decoded blocks and wide-row chunks | 256 | Sum of all retained decoded buffers, not a per-worker limit |
| Pending I/O and resident file-cache allowance | 256 | Account for the actual I/O mode and unique charged pages; user buffers and kernel cache may both exist |
| Output and protocol buffers | 64 | Byte backpressure through the final owned buffer; row count alone is insufficient |
| Directories, dictionaries and cached metadata | 128 | Overflow uses a specified disk lookup/merge path, not a growing hash map |
| Additional allocator/kernel/error-accounting headroom | 300 | Reserved against measured escaped overhead; not a substitute for observing it |
| **Total planned worker envelope** | **2854** | Leaves 146 MB inside the 3000 MB worker allowance |

Categories must be non-overlapping in the implementation's accounting view. A retained allocator pool is still physical consumption; freeing a logical object does not automatically release the backing pages. Memory mapping a small window does not by itself bound total retained cache pages. A hard process/container cap can stop an overrun but does not prove that the job completes. Both an internal progress-preserving allocation plan and whole-environment measurement are necessary.

The first scheduler runs one heavy build, query or refresh stage at a time. This is an explicit latency/concurrency tradeoff, not a permanent prohibition. A resident lightweight service must fit the common allowance; a local Neo4j server or client that materializes a large result consumes the same physical machine budget. A remote source remains visible in system cost even when its RAM is outside this worker's measurement.

The arena has materially different uses by algorithm:

- **Traversal example, N=200M:** a visited bitmap is 25 MB, a u32 parent plane is 800 MB, and a 256 MB bounded frontier pool brings those selected components to 1081 MB. Larger frontiers spill; the complete graph and ID map stay in separately charged storage. Adding another 800 MB distance plane would exceed this selected arena, so choose a separately justified streamed/external representation or a different allocation plan. These are payload calculations, not a measured fit.
- **D16 example, F=1M:** two f64 feature vectors use 16 MB of the principal arena; a per-feature numerical-error representation, counts and other arrays add their actual widths. The small vector payload does not remove the build arena or cache accounting.
- **D17 refresh example, F=1M:** three f64 arrays use 24 MB before numerical-error state. The affected-row union uses bounded external runs and row chunks; no limit on RAM may secretly become an unlimited group-member vector.
- **Exact similarity:** reserve queues only for the admitted source batch. Candidate intersection aggregation and result sorting share the remaining arena or spill under a workspace quota. Lower queue memory does not bound the number of candidates that must be processed.

Every phase must reserve enough buffers to make forward progress, including a final output/spill buffer. A design where all memory is pinned while the spill writer needs another allocation can deadlock inside its nominal cap. Cancellation and recovery need the same reserved path. These obligations belong in the first scale experiment, not in a later reliability project.

## The Client Is Part Of The Output Contract

For a streamed `(nodeId, score)` result, an appropriately bounded consumer can process rows without retaining all scores. A single result row containing a huge nested list or path object is different: the protocol decoder or client may materialize that object. The server cannot declare an entire same-machine workflow bounded while excluding that allocation.

Offer a native bounded export/pagination mode where semantics permit it, and keep the exact compatibility profile explicit. A streamed row set is not automatically the same API as a giant list-valued GDS result. On small-output profiles the existing Bolt route is useful; on large output, demonstrate bounded end-to-end encoding and consumption or declare the unsupported size/shape. Returning fewer results is not a silent solution to this incompatibility.

## One Concrete Customer Walkthrough

This is a proposed customer experience, not an observed customer or a claim that the current implementation supports it end to end. It uses the dependency-investigation direction in A007 and the narrow existing Bolt proof as a starting point.

The job is: **Given a package or service, identify all services that transitively depend on it in the selected snapshot, and return original IDs with enough evidence to support remediation.** Whether evidence means one witness per service, one selected shortest path, or a full subgraph must be agreed; those are materially different outputs.

| Moment | What the customer sees | What the system is responsible for |
| --- | --- | --- |
| Connect | Supply node/relationship CSVs or select a Neo4j export; preview the interpreted dependency direction | Preserve IDs, relationship types, selected properties and a consistent source boundary; charge extraction and validation |
| Specify | Choose the dependency question, result shape, machine allowance, first-answer deadline and maximum answer age | Infer counts and encoding details; ask about ambiguous semantics rather than exposing storage internals |
| Assess | See eligible minimal-build or prepared-investigation plans, with explicitly provisional time ranges | Bound profiling; explain missing information and any unsupported query clauses; do not promise arbitrary Cypher |
| Build | See durable progress, bytes retained, current resource use and a cancellation option | Stream and externally sort as necessary; keep hubs and long IDs from creating unbounded allocations; preserve a restart point within disk quota |
| Publish | Receive a named, validated snapshot with its source watermark and supported operations | Publish atomically under the selected retention/availability contract; never label a partial graph complete |
| Investigate | Change the package/service seed without rebuilding the graph; select only supported filters | Admit the query, bound frontier and output state, report queue time, and distinguish a complete negative answer from an unfinished search |
| Act | Export or inspect the affected service IDs and requested evidence | Preserve completeness and identity through the client; measure delivery time and downstream usability, not just the kernel |
| Repeat | Select newer data and see whether an old snapshot can remain available during refresh | Account for old/new overlap; safely reuse only valid artifacts; otherwise rebuild or negotiate an explicit maintenance window |

There are at least three people to understand even when one engineer plays all roles: the source owner who worries about production export load, the operator who owns failures and budgets, and the answer consumer who needs to decide what to fix. Saving the operator RAM while making the answer unusable, or forcing a risky production export, is not a successful workflow.

### Fifty Gigabytes Is A Portfolio Budget

Custom storage for each algorithm should mean **a choice of useful physical representations**, not an obligation to retain every representation simultaneously. Charge shared physical blocks once and all distinct indexes, property planes, ID maps and answer artifacts against the selected retained-storage allowance.

For example, if a customer's active portfolio already uses 46 GB, adding a distinct 8 GB similarity index cannot fit a 50 GB cap. Possible plans are to omit it and use a slower eligible algorithm, release an unneeded artifact, choose another representation, or obtain a different explicitly approved budget. Building the index temporarily still needs admitted workspace, and replacing an index pinned by an active reader cannot assume immediate reclamation. These numbers illustrate capacity arithmetic, not an estimated footprint for a real graph.

Do not introduce a mandatory expanded canonical graph that destroys the savings of an incidence-native representation. The stable abstraction should be graph meaning, identity and snapshot provenance; the physical representation can remain algorithm-specific where an equivalence argument permits it.

### Preparation Is An Investment With An Expiry Date

An optional index or precomputed result pays back only through eligible use before it becomes invalid or is evicted. A daily snapshot with one daily query is not a high-reuse workload merely because the customer repeats it every day. Conversely, thousands of different seeds on one stable typed adjacency can amortize its preparation without sharing identical answers.

For each optional artifact, record its extra build time and disk, the questions it actually accelerates, expected eligible uses per generation, refresh cost, and an alternative when it is absent. The planner can recommend a representation; the customer chooses the resource and waiting tradeoff. Initially this can be a small explicit rule set rather than a speculative general-purpose optimizer.

The essential product tests are therefore:

1. **Activation:** Can a new user get one independently verified useful answer from their own input without hidden larger-machine preparation?
2. **Repeated value:** Can they finish another meaningful question or tomorrow's job with tolerable refresh and operational effort?
3. **Budget integrity:** Do build, execution, output, failure and refresh respect the selected physical resource envelope?
4. **Decision timeliness:** Is the answer both delivered before its deadline and fresh enough for the decision?
5. **Adoption advantage:** Is the complete experience better enough than their current workable alternative to justify switching or adding a tool?

The first release should offer one coherent journey with one valuable question family. Multiple internal storage plans can differentiate that journey without multiplying the customer's onboarding decisions. A demonstration should include first preparation, a second seed/query, and a changed-data refresh. Report the three separately so preparation, reuse and maintenance cannot conceal one another's cost.

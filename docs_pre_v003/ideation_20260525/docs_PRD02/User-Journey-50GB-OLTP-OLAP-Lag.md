# User Journey: 50 GB Dataset, CRUD, Queries, and OLTP/OLAP Lag

This local version folds the existing `50 GB` user-journey note into a deeper
Iggy-informed architecture addendum.

The short answer remains:

> **Do not stop OLTP queries or writes while OLAP catches up.**

But after studying [Apache Iggy](https://github.com/apache/iggy),
`compio`, and recent `io_uring` guidance, the more precise answer is:

> **Never block truth-plane reads or writes on OLAP refresh.**
> **Allow analytics to be stale by default, merged when cheap, and optionally
> wait only when the user explicitly asks for fresh analytics.**

---

## The Setup

Assume:

- `50 GB` graph dataset
- initial import already completed
- users keep doing CRUD
- users also run traversals and graph algorithms
- the system has:
  - `OLTP truth plane`
  - `OLAP projection plane`

The core UX question is:

> after a write lands in OLTP, what happens if the user immediately runs
> a traversal or `PageRank`?

---

## Baseline User Journey

### Phase 1: Initial Import

1. User imports `50 GB` of graph data.
2. OLTP truth is built first.
3. OLAP projection is built from that truth.
4. Once the first projection is ready:
   - OLTP reads and writes are available
   - OLAP traversals and algorithms are fast

At this point, both planes reflect the same data.

### Phase 2: User Starts Editing

1. User creates a node, edge, or property.
2. The write lands in OLTP immediately.
3. The OLAP projection is now older than truth.
4. Queries continue to work, but not all of them should have the same contract.

The clean contract is:

| query type | default source | freshness |
| --- | --- | --- |
| point lookup / CRUD readback | OLTP | immediate |
| transactional Cypher | OLTP | immediate |
| heavy traversal on compatible projection | OLAP | latest projection |
| graph algorithms | OLAP | latest projection |
| explicit "fresh analytics" request | OLAP after catch-up or merged path | user-selected |

This avoids the worst product sin:

- **silently returning stale analytics as if they were latest truth**

---

## Industry Baseline

The user journey above matches what serious mixed-workload systems already do.

- **Oracle Database In-Memory** keeps row and column representations together.
  Oracle documents a dual-format architecture and transactional consistency
  between row and in-memory column copies:
  [Oracle Database In-Memory](https://www.oracle.com/database/in-memory/),
  [In-Memory Column Store Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/18/inmem/in-memory-column-store-architecture.html?source=%3Aow%3Alp%3Acpo%3A%3A)
- **TiDB + TiFlash** uses asynchronous columnar replicas while preserving
  consistency on reads, and also exposes stale-read semantics explicitly:
  [TiFlash Overview](https://docs.pingcap.com/tidb/stable/tiflash-overview/),
  [Stale Read](https://docs.pingcap.com/tidb/v6.1/dev-guide-use-stale-read/)
- **AlloyDB columnar engine** marks invalid columnar content after row updates,
  runs mixed row/column execution, and refreshes in the background:
  [About the AlloyDB columnar engine](https://docs.cloud.google.com/alloydb/docs/columnar-engine/about),
  [Maintain freshness of in-memory column store data](https://docs.cloud.google.com/alloydb/docs/columnar-engine/maintain-content-freshness)
- **Neo4j GDS** already separates transactional graph truth from projected
  analytics graphs:
  [GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/),
  [Understand the GDS workflow](https://graphacademy.neo4j.com/courses/gds-fundamentals/2-gds-basic-concepts/1-understand-gds-workflow/)

The repeated pattern is:

- writes do not wait for analytics refresh
- analytics may lag
- the system either merges tiers or exposes freshness semantics

---

## Candidate Timelines

### A. Snapshot Versioning

- Writes land in OLTP.
- OLAP serves the latest completed snapshot.
- Background rebuild creates the next snapshot.
- Staleness exists between rebuilds.

Good at:

- simplicity
- correctness
- easy rollback to the last good snapshot

Bad at:

- showing new data in analytics immediately
- full rebuild rewrite cost

### B. Overlay Model

- OLAP has immutable base CSR plus mutable overlay.
- Queries merge the base and overlay.
- Periodic recompact merges overlay into a new base.

Good at:

- zero logical staleness
- no "where is my node?" confusion

Bad at:

- more complex read logic
- whole-graph algorithms get slower as overlay grows

### C. Query Router

- OLTP and OLAP remain separate.
- Query type decides where execution runs.
- Analytics can be slightly stale while truth stays immediate.

Good at:

- clean product separation
- strong truth semantics

Bad at:

- query-classification complexity
- user confusion if freshness is not shown clearly

### D. Incremental CSR

- Keep CSR base mostly immutable.
- Append new edges or nodes into overflow structures.
- Compact later.

Good at:

- near-zero staleness
- often lower steady-state penalty than a fully generic overlay

Bad at:

- higher implementation complexity
- trickier algorithm invariants

---

## Current Best Path

For Knight Bus, the strongest near-term progression is still:

1. `v0.0.3`: manual snapshot rebuild
2. `v0.0.4`: scheduled/background rebuild
3. `v0.0.5`: explicit overlay or three-tier visibility
4. `v0.1.0`: incremental CSR only if the simpler overlay proves insufficient

That remains the recommended path even after studying Iggy.

What changed is **why**.

---

## Deep Exploration: What Apache Iggy Changes

### Premise Check

There are two different ideas hiding inside the question
"can we use `compio` or something?"

1. **Can Knight Bus borrow a faster write-path architecture from Iggy?**
   - `Yes.`
2. **Should Knight Bus replace its current OLAP mmap walker with `compio`
   and expect big speedups?**
   - `Probably no.`

The important correction is:

- `compio` and `io_uring` help most when your hot path is dominated by
  **many concurrent network and file I/O operations**
- Knight Bus OLAP today is dominated by:
  - `mmap`
  - page cache
  - dense array walking
  - memory bandwidth
  - algorithm inner loops

That means the hottest Knight Bus OLAP path is not waiting on the same thing
Iggy is optimizing.

### Expert Lenses

- `Storage-engine lens`: which parts of Iggy’s persistence design transfer to
  graph storage?
- `Async runtime lens`: where does `compio` materially outperform a simpler
  runtime choice?
- `Graph algorithm lens`: what helps traversals and PageRank versus what only
  helps append-heavy message logs?
- `Operator lens`: what new deployment and debugging burden comes with
  `io_uring`?
- `Skeptical lens`: are we chasing runtime novelty instead of the real
  bottleneck?

### Candidate Approaches

| approach | upside | downside | verdict |
| --- | --- | --- | --- |
| Replace Knight Bus wholesale with `compio`/`io_uring` | appealing performance story | likely little gain for mmap-heavy OLAP hot path; high complexity | reject |
| Ignore Iggy entirely | preserves simplicity | misses a strong write-path precedent | reject |
| Borrow Iggy’s persistence shape, not its full runtime ideology | targets the real problem: visibility, journaling, persistence tiers | still requires careful design | choose |

### Chosen Thesis

**Apache Iggy is highly relevant to Knight Bus, but mainly as a model for the
mutable plane, not as a reason to rewrite the immutable OLAP walker around
`compio`.**

The most transferable lessons are:

1. **Three-tier visibility**
   - persisted base
   - in-flight persisted-but-not-fully-published data
   - newest mutable journal

2. **Flush thresholds and durability knobs**
   - save after `N` messages
   - save after `N` bytes
   - periodic saver
   - optional `fsync` enforcement

3. **Append-first persistence**
   - journal first
   - publish optimized shape later

4. **Strong benchmark discipline**
   - benchmark the architecture transition itself, not just the end state

What is **not** the first thing to borrow:

1. full thread-per-core sharding
2. `compio` as a mandatory baseline for all code
3. Linux-specific `io_uring` operational assumptions in the OLAP read path

---

## Evidence and Verification

### Sourced Facts

- Apache Iggy explicitly describes itself as a persistent append-only log using
  `thread-per-core`, `shared nothing`, `io_uring`, and `compio`:
  [Apache Iggy README](https://github.com/apache/iggy)
- The Iggy architecture docs show a request flow where messages are first
  buffered in `MemoryMessageJournal` and then flushed to `.log` files:
  [Iggy architecture](https://iggy.apache.org/docs/introduction/architecture/)
- Iggy’s current code merges reads across **disk -> in-flight -> journal**,
  which is directly analogous to the visibility problem in base+overlay graph
  designs:
  [partition ops read path](https://github.com/apache/iggy/blob/master/core/server/src/streaming/partitions/ops.rs)
- Iggy exposes configurable persistence behavior such as periodic saving,
  `enforce_fsync`, buffered thresholds, and segment sizing:
  [Iggy server config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- Iggy’s migration write-up explains why they chose `compio`: the driver is
  disaggregated from the executor, but it also calls out non-zero complexity and
  even notes that request boxing introduces heap allocations:
  [Iggy migration to thread-per-core and io_uring](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- Iggy’s deployment docs make the operational cost concrete: the server needs
  working `io_uring` support and may require extra capabilities such as
  `IPC_LOCK` and relaxed seccomp settings in containerized environments:
  [Iggy Helm README](https://github.com/apache/iggy/blob/master/helm/charts/iggy/README.md)
- Recent DBMS research says `io_uring` is **not a panacea**: simply swapping
  interfaces yields only modest gains, while bigger wins come when the system is
  redesigned around batching and other capabilities:
  [High-Performance DBMSs with io_uring: When and How to use it](https://www.informatik.tu-darmstadt.de/media/systems/pdf_publications/iouring_vldb.pdf)
- `compio` is thread-local and centered on completion-based I/O rather than
  generic cross-thread async ergonomics:
  [compio docs](https://docs.rs/compio/latest/compio/runtime/struct.Runtime.html)

### Reasoned Inference

- Knight Bus OLAP queries are more likely to be **memory-bandwidth bound**
  after `mmap` than **syscall bound**.
- Therefore, moving the OLAP read path to `compio` is unlikely to unlock the
  kind of gains Iggy gets from network-plus-disk message streaming.
- The Knight Bus write and refresh path, by contrast, can benefit from Iggy’s
  style of:
  - journal
  - in-flight visibility
  - segmented persistence
  - explicit flush and `fsync` policy

### Verification Questions

1. **Does Iggy already solve a multi-tier visibility problem?**
   - `Yes.` Its read path explicitly merges disk, in-flight, and journal tiers.
2. **Is Iggy’s performance story mainly about immutable mmap scans?**
   - `No.` It is mainly about append-heavy, concurrent network-plus-disk I/O.
3. **Does the `io_uring` literature support "just swap the runtime"?**
   - `No.` The stronger gains come when the architecture is redesigned around
     batching and completion-based I/O.
4. **Would Knight Bus still need overlay or visibility logic even with compio?**
   - `Yes.` `compio` does not remove the need for a mutable truth layer.

---

## Rubber Duck Debug

If I tell a rubber duck:

> "We should use `compio` because Iggy is fast."

the duck should ask:

1. **What exactly is slow in Knight Bus today?**
   - not the steady-state mmap adjacency walk
   - the unsolved part is mutable visibility and refresh

2. **What does `compio` make faster?**
   - lots of concurrent completion-based I/O
   - network + file pipelines
   - shard-local async services

3. **What does `compio` not magically fix?**
   - overlay merge semantics
   - CSR mutation cost
   - algorithm cache invalidation
   - whole-graph recomputation policy

4. **So where should it go, if anywhere?**
   - the future OLTP daemon
   - the WAL / journal / flush worker
   - the background projection builder
   - possibly the ingestion pipeline

5. **Where should it not be the first move?**
   - replacing the current mmap OLAP read path just for fashion

If the duck is satisfied, the recommendation survives.

---

## Iggy-Informed Recommendation

### Borrow First

1. **Three-tier graph visibility**
   - `sealed CSR base`
   - `in-flight persisted delta`
   - `mutable overlay journal`

2. **Configurable freshness and durability policy**
   - rebuild interval
   - overlay-size threshold
   - delta-bytes threshold
   - optional stronger durability mode

3. **Segmented, append-friendly mutable plane**
   - not one giant mutable graph structure
   - a journal or delta segment stream that can later compact into CSR

4. **Measurement discipline**
   - benchmark rebuild latency
   - benchmark overlay growth impact
   - benchmark fresh readback latency

### Delay Until Later

1. **Full `compio` migration**
2. **Thread-per-core everywhere**
3. **Linux-only `io_uring` assumptions in the OLAP reader**

### Use `compio` If These Become True

- Knight Bus becomes a long-running Linux-first server
- the mutable plane is doing lots of concurrent file and socket I/O
- query routing and refresh work become I/O-driven bottlenecks
- the team is comfortable owning `io_uring` operational complexity

### Do Not Use `compio` First If These Remain True

- the core win is still mmap + dense arrays
- the next bottleneck is overlay semantics, not syscalls
- portability and simple developer setup matter more than shaving write-path tail latency

---

## Final Synthesis

The user journey does **not** change in its top-level contract:

- truth-plane queries never stop
- writes never wait for OLAP rebuild
- analytics may be stale unless merged or explicitly refreshed

What the Iggy study changes is the implementation opinion:

- **Do not chase `compio` as the first optimization for the OLAP walker.**
- **Do borrow Iggy’s journal, in-flight, and segmented persistence ideas for the mutable plane.**

In one sentence:

> **Iggy is a better template for Knight Bus OLTP and refresh mechanics than for Knight Bus OLAP execution mechanics.**

---

## Open Questions

1. What is the measured rebuild time for a synthetic `500M`-edge graph on the target hardware?
2. How large can the graph overlay become before traversal and PageRank slowdowns become user-visible?
3. Can the first overlay design be generic enough for traversal plus a few core algorithms without forcing a full incremental-CSR implementation?
4. Should the first user-facing freshness contract be:
   - `latest_truth`
   - `latest_analytics_snapshot`
   - `wait_for_fresh_analytics`
5. At what point does the mutable plane become I/O-bound enough that `compio` is worth the operational complexity?

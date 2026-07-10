# Neo4j Rust Rewrite: Two-Scenario Quantitative Estimation

<!-- markdownlint-disable MD013 MD060 -->

Date: 2026-07-10

Status: decision-grade estimate, not a performance promise

Revision: 2 - rubber-duck audit of PageRank, preparation cost, and the
difference between reducing bytes and externalizing bytes

Scope: Neo4j Community plus the locally available OpenGDS surface. Closed
Neo4j Enterprise and closed GDS components are excluded because their source,
tests, and complete behavior are not available locally.

## Executive Answer

The two scenarios optimize for different things:

1. **Scenario A - resident faithful rewrite:** reproduce Neo4j Community plus
   broad OpenGDS behavior in Rust, preserve broadly similar internal shapes,
   keep active OLTP and OLAP data resident, use tuned parallelism, and use
   `io_uring` for Linux storage paths.
2. **Scenario B - read-shape architecture:** preserve the same claimed external
   surface, but separate Neo4j-shaped OLTP truth from a Projection Build Store
   and exact, immutable, algorithm-shaped OLAP snapshots. It contains two
   materially different runtime lanes: a prepared, mostly resident fast lane
   and a hard-budget streaming/direct-I/O lane.

The governing conclusion is:

> Scenario A is the larger compatibility program with the weaker performance
> thesis. Scenario B is the stronger product architecture because it attacks
> representation copies, working-set size, and algorithm access shape, but it
> does not make every algorithm simultaneously faster and 8 GiB-capable.

The current repository already proves one important part of Scenario B. The
v002 immutable mmap plus dual-CSR walker was correct on a fixed corpus and used
less runtime RSS with much lower fixed-hop traversal latency than Neo4j on the
three recorded datasets. It does **not** yet prove Bolt parity, Cypher parity,
GDS algorithm parity, all-machine RAM, or the 50 GiB-on-8 GiB claim.

### Corrected Execution Model

The two architectural scenarios produce three Rust execution configurations.
They must not be collapsed into one "Rust" number:

| Configuration | What remains resident | Primary objective | PageRank expectation versus tuned resident GDS EE |
|---|---|---|---:|
| Neo4j plus tuned GDS EE | projected topology and algorithm state | incumbent baseline | 1.0x |
| Scenario A resident speed-first | Neo4j-shaped projection and state | compatibility with the lowest Scenario A runtime | 0.70x-1.30x |
| Scenario B fast read-shape | compact prepared topology and state, with enough page cache to avoid repeated storage scans | reduce bytes touched and runtime | 0.40x-1.00x after preparation, hypothesis only |
| Scenario B strict 8 GiB | compact state plus bounded topology windows; topology is reread or decoded as needed | fit a hard RAM budget | 1.50x-5.00x; normally slower |

Lower RAM can improve speed only when the engine **eliminates bytes** through
narrower IDs, one required orientation, compact state, better ordering, fewer
copies, or fewer passes. Lower RAM obtained by **externalizing bytes** to NVMe
normally increases latency. Scenario B fast uses the first mechanism; Scenario
B strict relies partly on the second. The strict lane is a capacity product,
not the project's fastest PageRank mode.

The final planning estimates are:

| Dimension | Scenario A: resident faithful rewrite | Scenario B: read-shape architecture |
|---|---:|---:|
| First commercially useful proof from current state | 8-16 weeks | 8-16 weeks for direct WCC/GDS proof |
| Seven-family OLAP product | 9-24 months, but blocked by compatibility shell decisions | 9-24 months with 4-8 engineers |
| Broad Community plus OpenGDS external compatibility | 3-6 years with 12-25 engineers | 4-7 years with 12-30 engineers if full parity remains mandatory |
| Likely implementation code | 1.0M-2.0M lines plus comparable tests | 1.2M-2.4M lines plus comparable tests at full parity; much less for the OLAP wedge |
| LLM token planning band | 3B-15B | 4B-18B at full parity; 0.3B-2B for seven OLAP families |
| Machine class for the 50 GiB planning graph | roughly 96-192 GiB; 256 GiB is the prudent lab size | fast lane roughly 16-64+ GiB for selected compact views; strict 8 GiB only for approved plans |
| Algorithm latency versus tuned resident GDS EE | usually 0.65x-1.4x elapsed time | fast lane 0.4x-1.5x; strict PageRank 1.5x-5x and other strict jobs algorithm-dependent |
| Preparation accounting | projection cost must be reported separately | snapshot compilation/publication can dominate the first request; compare first-run and amortized repeat-run totals |
| Large, durable speed advantage | unlikely from Rust or `io_uring` alone | plausible only when fewer bytes, fewer passes, or less state are touched |
| Main product risk | years spent reproducing behavior before differentiated value | architecture and surface breadth outrun verification |

In latency ratios throughout this document, `0.5x` means half the Neo4j
elapsed time and therefore twice as fast. Lower is better.

---

## Phase 0: Deconstruct And Clarify

### The Actual Decision

The decision is not simply "Rust versus Java." It is:

```text
Do we first reproduce the incumbent's resident architecture and hope that
native execution creates enough advantage?

or

Do we preserve observable compatibility while changing the physical unit of
storage, planning, and execution around graph-algorithm read shapes?
```

### Premise Check

The objective is sound after six corrections.

#### Correction 1: "Everything" is not locally specifiable

The local source supports estimating Neo4j Community and OpenGDS. It does not
support pricing exact Enterprise behavior, clustering, every plugin ABI, or
closed GDS functionality. "Whole Neo4j" in this document therefore means:

```text
official-driver-compatible Bolt and PackStream
+ broad Community Cypher and transaction behavior
+ single-node lifecycle, storage, recovery, indexes, and server behavior
+ broad locally visible OpenGDS procedures, catalog, algorithms, and modes
```

It excludes exact internal Java APIs and exact store-file compatibility unless
they are later made explicit requirements.

#### Correction 2: `io_uring` and all-resident OLAP solve different paths

`io_uring` can reduce submission overhead and overlap storage latency during
loading, WAL/checkpoints, spill, publication, cold reads, and export. Once
topology and algorithm state are resident, graph kernels are limited by memory
bandwidth, cache misses, atomics, synchronization, skew, and arithmetic.

The Linux interface uses shared submission and completion queues, and fixed
resources can reduce per-I/O overhead. Its own manual warns that polling modes
must be evaluated case by case. Registered buffers also pin memory, which is a
cost under a strict-RAM goal.

Primary sources:

- [io_uring setup](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
- [io_uring resource registration](https://www.man7.org/linux/man-pages/man2/io_uring_register.2.html)
- [io_uring SQPOLL](https://man7.org/linux/man-pages/man7/io_uring_sqpoll.7.html)

#### Correction 3: Maximum parallelism is not maximum speed

The useful target is **optimal measured concurrency**, not all available
threads. More workers eventually saturate memory bandwidth, increase atomic
contention, disturb locality, and worsen tails. Neo4j's own historical GDS
configuration guide notes that some algorithms, including Louvain, may stop
scaling well beyond 16-32 CPUs.

Current GDS Community is limited to four concurrent algorithm threads while
GDS Enterprise is not. A Rust result must therefore report two comparisons:

1. product comparison against GDS Community; and
2. architecture comparison against tuned GDS Enterprise.

Primary source: [GDS system requirements](https://neo4j.com/docs/graph-data-science/current/installation/System-requirements/).

#### Correction 4: P100 is not a stable population percentile

For an unbounded production lifetime, a single pause, page fault, scheduler
stall, hardware fault, or overload event becomes the maximum. "Minimum P100"
is therefore not a defensible universal promise.

This dossier uses:

```text
p50 / p95 / p99 / p99.9
observed_max over an explicit sample count and duration
timeout or SLO violation rate
```

The v002 reports preserve p99 but do not preserve raw samples or observed max,
so a historical P100 cannot be reconstructed.

#### Correction 5: Low process RSS is not low total machine RAM

An mmap-backed engine can show a small heap while the operating system holds
mapped pages in the filesystem cache. The PRD correctly requires holistic
accounting across RSS, page cache, direct buffers, algorithm state, results,
and build scratch.

Neo4j likewise divides memory among heap, native allocations, page cache,
transactions, network buffers, JVM overhead, and OS memory. Primary source:
[Neo4j memory configuration](https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/).

#### Correction 6: "Lower RAM" names two opposite latency mechanisms

There are two ways to reduce resident memory:

1. **Represent fewer bytes:** remove duplicate orientations and copies, narrow
   IDs where valid, compact state, improve locality, or avoid materializing
   unused properties. This can reduce both RAM and latency because fewer bytes
   cross the memory hierarchy.
2. **Externalize bytes:** leave topology or state on NVMe and stream, fault, or
   spill it through a bounded window. This reduces resident RAM but introduces
   I/O, decoding, and repeated-pass cost.

Any estimate that says "lower RAM and faster" SHALL identify which bytes were
eliminated rather than merely moved. PageRank in strict mode repeatedly scans
topology, so it should be presumed slower than resident GDS until a paired
benchmark disproves that presumption.

### Optimized Execution Protocol

This estimate uses four reasoning modules without exposing hidden internal
reasoning:

1. **Evidence hierarchy:** measured local facts outrank modeled extrapolation.
2. **Component arithmetic:** topology, properties, state, results, and runtime
   overhead are estimated separately.
3. **Scenario traversal:** each architecture is followed from build through
   query, recovery, and compatibility delivery.
4. **Adversarial verification:** a skeptical pass attacks every headline
   number before Draft 2 is accepted.

---

## Phase 1: Expert Lenses And Knowledge Scaffold

### Expert Lenses

| Lens | Primary question |
|---|---|
| Neo4j compatibility | Which external behaviors must remain identical, and which internals may change? |
| Graph algorithms and representations | Which bytes and access orders dominate each algorithm family? |
| Linux storage and concurrency | Where can async I/O and parallelism help, and where do they add overhead? |
| Verification and benchmarking | Are compared inputs, outputs, process scopes, cache states, and result materialization equivalent? |
| Skeptical engineering | Which conclusion disappears when the strongest benchmark is normalized fairly? |

### Knowledge Scaffold

The estimate depends on these domains:

- Bolt, PackStream, Cypher semantics, transaction isolation, WAL, recovery,
  page cache, indexes, and procedure execution;
- projected graph catalogs, orientations, properties, result modes, memory
  estimation, and graph algorithm semantics;
- CSR and compressed adjacency, dense IDs, sidecars, working-set planning,
  mmap, direct I/O, and snapshot publication;
- memory bandwidth, NUMA placement, atomics, work partitioning, queue depth,
  and result backpressure;
- language-neutral differential testing and fault-injection verification;
- codebase size, verified engineering throughput, and LLM context economics.

### Council Opening Statements

**Compatibility lens:** A full rewrite must be estimated from observable
contracts, not Java line translation. The external surface is already much
larger than the algorithm kernels.

**Graph systems lens:** The only credible path to an order-of-magnitude RAM
change is changing bytes retained or touched. Rust alone does not change the
asymptotic state of PageRank, Louvain, or similarity.

**Linux systems lens:** `io_uring` belongs behind an I/O trait. It can matter
for strict streaming and publication, but it is not the resident kernel.

**Verification lens:** v002 is valid evidence for its exact contract. It must
not be relabeled as full Neo4j or GDS parity.

**Skeptical lens:** The current Rust benchmark avoids Bolt, Cypher planning,
transactions, and GDS algorithms. Any estimate that directly extrapolates its
245,000x p50 result to the rewrite should be rejected.

---

## Phase 2: Candidate Approaches

### Conventional Approach: Faithful Resident Port

Port the server subsystem by subsystem, retain a page-cache/record-store OLTP
architecture and a resident projected graph, replace JVM abstractions with
Rust, then optimize with async I/O and broad parallelism.

Advantages:

- easiest mapping from upstream source ownership;
- most direct reuse of architecture-specific tests after translation;
- fewer novel storage semantics during early compatibility work.

Disadvantages:

- differentiation arrives late;
- Rust removes GC but not duplicate representations or algorithm state;
- a similar architecture creates a similar memory-bandwidth ceiling.

### Blend 1: Compiler Backend

Blend graph storage with compiler design:

```text
OLTP records = source language
Projection Build Store = typed intermediate representation
snapshot compiler = optimizing backend
algorithm read shape = machine code for one workload family
execution receipt = compiler and runtime proof artifact
```

This leads to a stable logical `GraphView` with multiple exact physical access
plans. It avoids forcing one representation to serve every algorithm.

### Blend 2: Virtual-Memory Working-Set Contracts

Blend database planning with operating-system working-set control. A plan does
not merely estimate heap; it declares:

```text
resident metadata
topology window
state arrays
result buffers
I/O queue and registered buffers
page-cache policy
spill budget
```

The runtime reserves that budget before starting and rejects plans that do not
fit. Fast mmap and strict direct/streamed modes become explicit products rather
than accidental cache states.

### Blend 3: Manufacturing Cells And Generations

Blend snapshot storage with manufacturing cells. The build plane refines facts
once, compiles bounded cells or runs, validates them independently, and
publishes a complete generation atomically. Specialized outputs are derived
products with lineage, not hidden caches.

This improves rebuild locality and failure isolation, but can slow global scans
if cell boundaries and routing are poor.

### Selected Hybrid

The strongest architecture is a hybrid:

```text
compatibility shell
  -> Neo4j-shaped OLTP source of truth
  -> typed Projection Build Store
  -> generation catalog
  -> generic exact topology
  -> per-family exact access plans and state layouts
  -> fast mmap lane OR strict-RAM streaming lane
  -> shared oracle and benchmark harness
```

This is Scenario B. Scenario A remains useful as a control architecture and as
a possible compatibility implementation for OLTP, not as the primary
performance thesis.

### Structured Debate And Resolution

The skeptical lens raised four objections:

1. **"Neo4j GDS already uses compressed in-memory graph structures."**
   Response: correct. Scenario B must beat the current GDS estimate for the
   exact graph and cannot claim victory merely by using CSR.
2. **"Algorithm-shaped copies may consume more disk and build time."**
   Response: correct. Disk, publication lag, and generation count are explicit
   costs in Draft 2.
3. **"Strict RAM may turn every iteration into an NVMe scan."**
   Response: correct. The strict lane is modeled as 1.5-5x slower for PageRank
   and can be slower still for harder iterative algorithms. A first request
   that also compiles the view can be materially slower; no stable ratio is
   accepted until both engines' preparation is measured symmetrically. Plans
   whose state cannot fit are rejected.
4. **"Full compatibility plus specialized storage is more work than a port."**
   Response: correct at final parity. Scenario B wins through earlier useful
   vertical slices and differentiated economics, not lower total full-parity
   engineering effort.

Core thesis after debate:

> Build compatibility as a shell and verification contract, but earn the
> product advantage through fewer retained bytes, fewer bytes touched, fewer
> passes, and explicit working-set control. Do not attribute those gains to the
> implementation language or I/O API.

---

## Evidence Hierarchy

Every quantitative statement is labeled conceptually by one of these classes:

| Class | Meaning | Example |
|---|---|---|
| M | Measured locally | v002 RSS and latency, current line counts, current tests |
| D | Derived arithmetically | dual-CSR byte formula, per-node state arrays |
| P | Primary external evidence | Neo4j manuals, Linux manuals, official GDS benchmark |
| R | Reasoned estimate | engineering and performance ranges |
| H | Hypothesis requiring experiment | per-algorithm speedups from specialized layouts |

Confidence labels mean:

| Confidence | Interpretation |
|---|---|
| High | direct measurement or deterministic formula with known inputs |
| Medium | source-backed model with important workload sensitivity |
| Low | wide planning range intended to prevent a false point estimate |

---

## Local Baseline

### Source And Verification Surface

The local core corpus is not weekend-sized:

| Measured scope | Source-like lines | Raw source tokens |
|---|---:|---:|
| Neo4j Community | 2,127,716 | 18,353,485 |
| OpenGDS | 531,701 | 4,498,494 |
| Combined core | 2,659,417 | 22,851,979 |
| All 20 Neo4j-family repositories | 3,982,335 | 33,694,228 |

Core implementation and test lines are approximately evenly split. Local
evidence includes about 14,895 Neo4j JUnit-like annotations, 18,606 Scala test
declarations, 1,407 Neo4j Gherkin scenarios, 4,518 GDS JUnit-like annotations,
and 1,615 openCypher TCK scenarios. Most Java and Scala tests cannot execute
unchanged against Rust because they inspect JVM internals.

Pinned local reference revisions:

| Repository | Revision |
|---|---|
| Neo4j Community | `c68156e`, release `5.26.0`, tagged `5.26.0`/`5.26.1`, 2024-12-03 |
| OpenGDS | `dc4417`, branch `2.13`, 2026-05-12; examples declare GDS `2.13.10` and Neo4j `5.26.26` |

The two checkouts are therefore useful but not an exact matched release pair.

### Current Knight Bus Implementation

Direct counts on this branch:

| Item | Count |
|---|---:|
| Rust production lines under `src/` | 7,423 |
| Rust integration/support test lines | 1,785 |
| Total Rust source/test lines | 9,208 |
| Passing Rust tests from `cargo test --all-targets` | 51 |
| GDS inventory entries registered | 575 |
| Built-in GDS entries marked implemented now | 15 |
| Implemented GDS algorithm kernels | 0 |

The 15 implemented entries are catalog, projection estimate/lifecycle,
property streaming, graph existence, and size operations. PageRank is present
in the registry but remains unsupported.

Two independent code graphs were built for the current workspace:

| Tool | Files/nodes | Structural result |
|---|---:|---|
| codebase-memory-mcp | 6,775 nodes, 9,996 edges | `runtime`, `types`, `gds`, and `error` are core fan-in areas; `low_ram` is a large orchestration entry |
| CodeGraphContext | 334 files, 570 functions, 66 structs, 6 traits | low-RAM build fans into run creation, key resolution, forward/reverse emission, manifest, and size accounting |

Direct source reads confirm:

- `MmapWalkRuntime` maps forward/reverse offsets and peers, the node table,
  strings, and key index;
- open-time validation scans offsets, node records, and the key index;
- the low-RAM builder uses external sorted runs with a default 64 MiB logical
  budget and a one-quarter spill buffer;
- query execution is currently sequential and allocation-bearing;
- no Rust async runtime or `io_uring` dependency exists in `Cargo.toml`;
- current GDS execution dispatch reaches 11 catalog/property handler families,
  not WCC, PageRank, or Louvain kernels.

### v002 Measured Proof

The v002 benchmark ran on macOS 14.6 ARM64. The exact CPU model, core count,
RAM capacity, raw samples, and tracked report bundles are absent from this
checkout, so cross-machine replication is not currently possible.

| Dataset | Nodes | Edges | Snapshot | Rust runtime RSS | Neo4j server RSS | Rust p99 | Neo4j p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 MB | 1,949 | 17,722 | 251,390 B | 6,668,288 B | 525,926,400 B | 0.025550 ms | 12.611896 ms |
| 50 MB | 97,606 | 886,085 | 12,555,088 B | 14,499,840 B | 616,054,784 B | 0.036300 ms | 52.235973 ms |
| 2 GB | 3,997,988 | 36,294,270 | 514,241,964 B | 234,340,352 B | 1,065,615,360 B | 0.044948 ms | 1,514.533206 ms |

Measured phase peaks for the 2 GB dataset were 235,143,168 B for build and
409,452,544 B for verification. Rust open time was 189.979 ms versus 90.446 ms
for Neo4j, so Neo4j won that cold-open measurement.

The benchmark proves:

- parity for the fixed forward-one, reverse-one, and reverse-two-hop corpus;
- a compact snapshot at about 23.51% of raw generated CSV bytes;
- substantially lower runtime process RSS for that corpus;
- an extremely fast direct Rust traversal path.

It does not prove:

- Bolt or Cypher implementation parity in Rust;
- GDS algorithm parity;
- equal process boundaries, because Rust runs in-process while Neo4j is called
  through a Python driver and Bolt;
- equal service functionality, because Neo4j carries the full server;
- holistic page-cache memory;
- P100;
- behavior on a customer graph distribution;
- 50 GiB-on-8 GiB.

The current Python harness also has an environment reproducibility issue in
this checkout: 16 tests pass, one is skipped, and one fails under system Python
3.9 because the renderer uses `zip(..., strict=True)`, introduced in Python
3.10. The Rust suite is green. This does not invalidate the archived numbers,
but it means the benchmark environment is not one-command reproducible here.

### Deterministic Storage Arithmetic

For current Knight Bus dual CSR with `V` nodes, `E` directed relationships,
and `K` total key-string bytes:

```text
forward and reverse peers      = 2 * E * 4
forward and reverse offsets    = 2 * (V + 1) * 8
node table                     = V * 16
sorted key index               = V * 4
strings                        = K

total bytes = 8E + 36V + 16 + K
```

For the PRD planning graph of `V = 200M`, `E = 1B`:

| Average key bytes | Current exact dual-CSR snapshot |
|---:|---:|
| 8 | 15.65 GiB |
| 16 | 17.14 GiB |
| 24 | 18.63 GiB |

A single topology orientation without key structures is about 5.22 GiB:

```text
4E + 8(V + 1)
```

This is why "mmap" and "fits in 8 GiB" must not be conflated. The current
snapshot can be larger than RAM while a subset remains resident.

Linear extrapolation of the 2 GB generated dataset to 50 GiB gives a useful
but intentionally naive diagnostic:

| Extrapolated quantity | Linear result |
|---|---:|
| Snapshot | 11.75 GiB |
| Runtime RSS | 5.36 GiB |
| Build peak RSS | 5.37 GiB |
| Verify peak RSS | 9.36 GiB |

This is not a forecast. Dataset shape, page residency, current external-sort
behavior, and open validation all break simple linearity.

### Neo4j And GDS Primary Baseline

Current Neo4j documentation establishes that:

- Neo4j performance memory spans heap, native memory, page cache, transaction
  memory, network buffers, JVM overhead, and OS memory;
- GDS projections and algorithm state live on the heap;
- GDS supports `.estimate` procedures and can reject executions likely to
  exceed memory;
- current GDS Community generally limits concurrency to four, while Enterprise
  removes that cap;
- current Neo4j has Linux asynchronous page-cache I/O support for a subset of
  operations, so async I/O is not a permanent differentiator by itself.

Primary sources:

- [Neo4j memory configuration](https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/)
- [GDS memory estimation](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/)
- [GDS system requirements](https://neo4j.com/docs/graph-data-science/current/installation/System-requirements/)
- [Neo4j configuration settings](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/)

An official but historical GDS 1.5 benchmark provides an order-of-magnitude
calibration, not a current competitive result. On an AWS R5d.16xlarge with 64
logical cores, 512 GB RAM, 400 GB heap, and NVMe, an LDBC100 projection with
317,216,062 nodes and 2,154,861,632 relationships was reported as:

| Operation | Reported elapsed time | Reported memory |
|---|---:|---:|
| Graph create | 91 seconds | 7.7 GB |
| WCC | 36 seconds | 58 GB |
| PageRank | 8.28 minutes | 110 GB |
| Louvain | 12.27 minutes | 119 GB |
| FastRP | 5.85 minutes | 254 GB |

Source: [Neo4j GDS Configuration Guide, pages 7-8](https://go.neo4j.com/rs/710-RRC-335/images/Neo4j-Graph-Data-Science-Configuration-Guide-EN-A4.pdf).

The result is old, Enterprise-only, and based on a larger graph and older
algorithms. It is retained only to prevent estimates that are off by orders of
magnitude.

### Source-Derived Algorithm State Floors

Current local OpenGDS source provides better state-shape evidence:

| Algorithm | Local GDS state evidence | 200M-node deterministic floor |
|---|---|---:|
| WCC | non-incremental `HugeAtomicDisjointSetStruct` uses one atomic `long` parent per node | about 1.49 GiB before overhead |
| PageRank | Pregel uses one double node value, two atomic double message arrays, and vote bits | about 4.47 GiB plus bits and overhead |
| Louvain | modularity optimization has multiple long/double/atomic arrays, colors, per-thread maps, dendrograms, and subgraphs | greater than 10.4 GiB before maps/subgraphs in the simple seven-array count |

A Rust WCC implementation using a dense `u32` parent has a 0.745 GiB parent
floor. Three `f32` vectors for PageRank are 2.235 GiB; three `f64` vectors are
4.470 GiB. Using `f32` is a semantic decision, not a free optimization.

---

## Scenario Definitions

### Scenario A: Resident Faithful Rewrite

Architecture:

```text
official drivers
  -> Rust Bolt/PackStream
  -> Rust Cypher parser/planner/runtime
  -> Rust transaction/kernel/index/recovery layer
  -> Rust Neo4j-shaped record/page store
  -> resident projected graph catalog
  -> broadly faithful GDS algorithms and result modes
```

Performance posture:

- active database and indexes are preloaded or strongly resident;
- projected graph and algorithm state are resident;
- tuned worker counts, NUMA placement, SIMD where valid, and bounded allocation;
- `io_uring` for Linux WAL, checkpoints, import, backup, cold reads, and spill;
- portable I/O backend elsewhere.

This scenario spends RAM to suppress I/O variability. It is not a low-RAM
architecture.

### Scenario B: Read-Shape Architecture

Architecture:

```text
official drivers
  -> compatibility shell
  -> Neo4j-shaped OLTP truth store
  -> Projection Build Store / analytical IR
  -> atomic generation catalog
  -> generic exact topology plus typed sidecars
  -> per-family exact read plans and state layouts
  -> fast mmap lane OR strict-RAM direct/streamed lane
```

Scenario B contains two modes with different promises:

| Lane | Residency contract | Promise | Non-promise |
|---|---|---|---|
| Fast read-shape | prepared compact topology and state are resident or have a deliberately large warm page cache | minimize execution time by touching fewer, better-ordered bytes | no hard 8 GiB machine cap |
| Strict RAM | total cgroup usage is reserved and bounded; topology is windowed, streamed, or decoded under that reservation | complete approved jobs inside the RAM cap | not expected to beat resident PageRank latency |

Shared performance posture:

- no query-time reconciliation with newer writes;
- OLAP executes at a declared source watermark;
- topology, properties, and state are late-materialized by plan;
- full scans use sequential, compressed, or orientation-specific datasets;
- local workloads use cells/page indexes and bounded residency;
- output and intermediate state can stream or spill;
- execution starts only after a holistic reservation succeeds.

This scenario spends disk, publication work, and architectural complexity to
control RAM and reduce bytes touched.

The build and publication lag is part of the product. Neo4j/GDS also pays a
projection cost, so fair measurements report both engines in two states:

```text
first request = projection_or_snapshot_build + open + execute + materialize
repeat request = open + execute + materialize
```

Fast-lane speed claims apply to repeat execution after a compatible view is
published. Strict-lane value is primarily capacity and predictability.

### Shared Planning Workload

Unless another row says otherwise, modeled absolute capacities use:

| Parameter | Planning value |
|---|---:|
| Logical graph | 50 GiB class |
| Nodes | 200 million |
| Directed relationships | 1 billion |
| Machine for resident experiments | Linux, 32 physical / 64 logical cores, 256 GiB RAM, fast NVMe |
| Strict experiment | same CPU/storage class under an 8 GiB cgroup limit |
| Cache states | cold, warm, and constrained |
| Result modes | stream, mutate, write, and stats measured separately |

The hardware is a normalization assumption, not a claim about the v002 Mac.

---

## Draft 1: Optimistic First Estimate

Draft 1 deliberately records the first-pass extrapolation before adversarial
correction.

### Draft 1 Assumptions

1. Rust removes most JVM overhead.
2. `io_uring` materially improves the database as a whole.
3. all cores can be used profitably;
4. v002 traversal improvements partially generalize to GDS;
5. mmap-backed snapshots make the 50 GiB graph an 8 GiB workload;
6. source and tests define most behavior;
7. full Rust code can be materially smaller than Java/Scala.

### Draft 1 Quantitative Table

| Metric | Scenario A first estimate | Scenario B first estimate |
|---|---:|---:|
| Broad external parity | 2-4 years, 10-20 engineers | 2-5 years, 10-24 engineers |
| Final implementation | 0.8M-1.5M lines | 0.7M-1.5M lines |
| LLM tokens | 2B-8B | 2B-10B |
| 50 GiB graph machine RAM | 55-90 GiB | 4-8 GiB |
| Fixed-hop end-to-end latency | 2x-10x faster than Neo4j | 10x-100x faster |
| WCC/PageRank elapsed time | 1.5x-3x faster | 2x-10x faster |
| Tail / "P100" | 3x-10x lower | 10x-100x lower |
| Cold load / projection | 1.5x-3x faster | 1x-3x faster |
| Disk footprint versus Neo4j only | 1.0x-1.5x | 1.5x-4x |

### Why Draft 1 Initially Looked Plausible

- v002 showed 4.5x-78.9x lower runtime RSS and enormous traversal latency
  deltas;
- Rust has no GC pauses and exposes compact value layouts;
- the current snapshot is only about 23.5% of the generated CSV input;
- full source and unusually dense tests are locally available;
- asynchronous I/O and parallel algorithms are real capabilities.

Draft 1 is not accepted. The next section explains why.

---

## Rubber-Duck And Adversarial Verification

### Duck 1: Is v002 an end-to-end Neo4j-compatible benchmark?

No. It is end to end for its fixed traversal contract, but the Rust side does
not implement Bolt, Cypher parsing/planning, transactions, or a general server.
The Neo4j side crosses Python and Bolt. The result is powerful read-path
evidence, not a whole-product speed ratio.

**Revision:** reduce projected full-surface traversal gains from 10x-100x to a
wide 5x-50x only for reads routed directly to the optimized snapshot, and use
0.6x-1.3x elapsed-time ratios for generic compatible OLTP paths.

### Duck 2: Does v002 benchmark GDS?

No. The corpus contains `forward_one`, `reverse_one`, and `reverse_two` Cypher
patterns. No `CALL gds.*` algorithm is compared.

**Revision:** remove all GDS speedups derived directly from v002.

### Duck 3: If everything is resident, what does `io_uring` accelerate?

Not the graph-kernel hot loop. It can improve the edges of the lifecycle:
loading, prefetch, WAL, checkpoint, spill, and publication.

**Revision:** resident algorithm gains are attributed to representation,
scheduling, SIMD, and allocation, with `io_uring` modeled as 0-5% on the hot
resident job and potentially more on I/O phases only.

### Duck 4: Is GDS a naive object graph?

No. GDS uses primitive, paged, compressed, and algorithm-aware structures and
already performs memory estimation. Historical GDS Enterprise reported a 7.7
GB projection for more than two billion relationships.

**Revision:** same-layout Rust memory gains narrow to roughly 10-35% for
algorithm-heavy paths unless 64-bit state can safely become 32-bit or layouts
are changed.

### Duck 5: Is current dual CSR itself smaller than GDS?

Not necessarily. Current Knight Bus stores both orientations with 32-bit peers,
64-bit offsets, node records, keys, and an index. At 200M/1B it is 15.65-18.63
GiB for plausible key lengths. An algorithm that needs only one orientation
has a 5.22 GiB uncompressed topology floor before properties.

**Revision:** Scenario B's large RAM claim depends on orientation-specific,
compressed, windowed, or streamed datasets, not merely formalizing current
dual CSR.

### Duck 6: Does mmap enforce an 8 GiB machine limit?

No. It delegates residency to the operating system. Page-cache growth and
faults must be measured at cgroup or machine level.

**Revision:** split Scenario B into fast mmap and strict-RAM lanes. Only the
strict lane may advertise a hard budget.

### Duck 7: Can every algorithm fit in 8 GiB?

No. WCC and selected traversal/path families can. PageRank may fit with compact
state and a streamed/windowed or compressed topology. At the planning shape,
one uncompressed orientation plus three `f64[V]` arrays is already about 9.69
GiB before runtime overhead, so that all-resident plan cannot fit. Three `f32`
arrays reduce state, but precision is then an explicit semantic contract.
Louvain, embeddings, NodeSimilarity, all-pairs/path enumeration, and large
outputs can exceed 8 GiB even when topology is streamed.

**Revision:** strict mode rejects unsupported plans before execution and never
turns one machine-size claim into a universal algorithm claim.

### Duck 8: Does maximum concurrency minimize latency?

No. Bandwidth-bound algorithms plateau; atomic and NUMA-heavy algorithms can
regress.

**Revision:** report a concurrency sweep and retain the best stable setting,
including 1, 2, 4, 8, 16, 32, and 64 workers where available.

### Duck 9: Can we estimate P100 from p99?

No. The raw samples are absent and p100 depends on sample count.

**Revision:** retract numeric P100 claims and specify observed-max and timeout
contracts.

### Duck 10: Do all upstream tests transfer?

No. External fixtures, openCypher TCK, drivers, and wire behavior transfer
well. Internal Java/Scala unit tests need contract extraction or Rust-native
replacement.

**Revision:** restore the full rewrite estimate to 3-6 years and 3B-15B tokens
for Scenario A.

### Duck 11: Is Scenario B less engineering at full parity?

No. It adds a build plane, generations, planners, sidecars, two execution
lanes, and more failure modes.

**Revision:** Scenario B is estimated as 10-30% more full-parity implementation
than Scenario A, while reaching differentiated OLAP value much earlier.

### Duck 12: Is the existing benchmark reproducible from this checkout?

Not fully. The reports and large fixed datasets are absent, Neo4j is not
installed, the macOS Java shim has no runtime, and the Python harness has a
3.9 compatibility failure. Rust tests are green.

**Revision:** retain v002 as archived evidence but lower confidence on any
extrapolation until a containerized or Nix-like benchmark bundle reruns it.

### Duck 13: When RAM falls, were bytes removed or moved?

The first draft did not force that distinction. A compact orientation-specific
layout can remove bytes from every PageRank iteration; a small topology window
merely moves the remaining bytes across NVMe on demand.

**Revision:** every low-RAM result must report physical bytes in the view,
peak resident bytes, bytes read from storage, bytes decoded, and bytes written.
A lower RSS alone is not evidence of less work.

### Duck 14: Can strict 8 GiB PageRank be faster merely because it is Rust?

No defensible model says so. One uncompressed orientation at the planning
shape is about 5.22 GiB. A conventional 20-iteration PageRank reads at least
about 104.4 GiB of topology before state traffic:

```text
20 * 5.22 GiB = 104.4 GiB topology traffic

illustrative DRAM floor at 50 GiB/s = about 2.1 seconds
illustrative NVMe floor at 5 GiB/s  = about 20.9 seconds
```

Those are topology-only lower bounds, not runtime forecasts. Atomics, state
reads/writes, decoding, convergence checks, skew, and synchronization add to
both. They nevertheless show why externalization cannot itself be the speed
mechanism.

**Revision:** strict PageRank is estimated at 1.5x-5x tuned resident GDS
elapsed time. Scenario B fast is the only low-byte Scenario B lane currently
hypothesized to beat resident GDS PageRank.

### Duck 15: Did the comparison charge preparation to both engines?

Not consistently in Draft 1. GDS pays graph projection; Knight Bus pays view
compilation, validation, and atomic publication, potentially for several
algorithm-shaped datasets.

**Revision:** publish cold first-request, warm repeat-request, and amortized
`N`-run totals. Never add Knight Bus preparation while silently starting GDS
from an already projected graph, or vice versa.

### Duck 16: When does a prepared fast view amortize its build?

Let `P_k` and `P_g` be Knight Bus and GDS preparation time, and `R_k` and
`R_g` their repeat execution time. If and only if `R_k < R_g`, Knight Bus wins
after more than:

```text
N_break_even = (P_k - P_g) / (R_g - R_k)
```

runs, when the numerator is positive. If strict mode has `R_k >= R_g`, it has
no speed break-even; its justification is fitting hardware on which resident
GDS cannot run.

### Duck 17: Are two PageRank results semantically comparable?

Only when orientation, relationship weights, damping factor, tolerance,
maximum iterations, convergence behavior, precision, concurrency, dangling
node handling, and result mode match. Switching from `f64` to `f32` saves about
2.235 GiB across three 200M-entry arrays, but it is a contract change unless
the differential oracle accepts its error envelope.

**Revision:** performance tables are conditional on matched semantics. Any
compact-precision result is reported as a separate mode, not silent parity.

---

## Draft 2: Revised Decision Estimate

Draft 2 is the accepted planning model.

### What Changed From Draft 1

| Estimate | Draft 1 | Draft 2 | Reason |
|---|---:|---:|---|
| Scenario A calendar | 2-4 years | 3-6 years | internal tests and hidden product behavior do not mechanically transfer |
| Scenario B full-parity calendar | 2-5 years | 4-7 years | specialized storage adds architecture even while shipping earlier slices |
| Scenario A GDS speed | 1.5x-3x | 0.65x-1.4x elapsed time vs tuned GDS EE | GDS is already primitive, compressed, and parallel |
| Scenario B fast GDS speed | 2x-10x | 0.4x-1.5x after preparation | fewer bytes and passes may beat the resident baseline; this remains a hypothesis |
| Scenario B strict PageRank speed | implicitly faster | 1.5x-5x elapsed time, normally slower | bounded residency turns repeated topology passes into storage/decode traffic |
| Scenario B universal RAM | 4-8 GiB | 3-8 GiB for selected families; 8-30+ GiB or reject for hard families | algorithm state and outputs remain |
| Preparation | mostly omitted | first, repeat, and amortized totals reported separately | GDS projection and Knight Bus compilation must be charged symmetrically |
| P100 | numeric reduction | retracted | no stable population P100 and no raw v002 samples |
| `io_uring` contribution | system-wide | lifecycle/I/O paths only | resident kernels issue no disk I/O |
| Current CSR advantage | assumed smaller | may be larger on disk than compressed GDS | dual orientation and keys cost 15.65-18.63 GiB at target shape |

### Final Engineering Estimate

| Deliverable | Scenario A | Scenario B |
|---|---:|---:|
| One direct GDS WCC differential slice | 8-16 weeks, 2-4 engineers/agents | 8-16 weeks, 2-4 engineers/agents |
| Stable compatibility shell plus one GDS family | 4-9 months | 4-9 months |
| Seven-family GDS product, limited Cypher | 9-24 months, 4-8 engineers | 9-24 months, 4-8 engineers |
| Broad OpenGDS procedures/catalog/ML | 18-36 months, 8-15 engineers | 18-42 months, 8-18 engineers |
| Broad Community plus OpenGDS external parity | 3-6 years, 12-25 engineers | 4-7 years, 12-30 engineers |
| Full-parity implementation scale | 1.0M-2.0M lines plus 0.8M-2.0M test/fixture lines | 1.2M-2.4M lines plus 1.0M-2.2M test/fixture lines |
| Engineer-year envelope | roughly 36-150 before perpetual compatibility maintenance | roughly 48-210 before perpetual maintenance |
| LLM token envelope | 3B-15B | 4B-18B full parity; 0.3B-2B for the seven-family wedge |

Confidence is medium for code/token order of magnitude and low for calendar
because team quality, upstream drift, accepted surface, and oracle automation
dominate.

### Final Memory Model

#### Whole-Machine Capacity And Active Working Sets

Only Scenario B strict is a hard whole-machine/cgroup cap. Scenario B fast
figures describe the active analytical plan; its filesystem cache may expand
to available machine memory. A fast-lane RSS number therefore SHALL NOT be
marketed as a whole-machine peak.

For the 50 GiB-class planning graph:

| Workload state | Neo4j/GDS baseline | Scenario A | Scenario B fast mmap | Scenario B strict |
|---|---:|---:|---:|---:|
| OLTP-only, mostly resident | 64-96 GiB | 56-88 GiB | 8-32 GiB if OLTP cache is intentionally bounded | 6-8 GiB with lower cache hit rate |
| OLTP plus resident projection, no heavy algorithm | 72-120 GiB | 64-112 GiB | 8-32 GiB process plus elastic OS cache | 6-8 GiB, topology streamed/windowed |
| WCC active | 80-128 GiB | 68-120 GiB | 6-16 GiB active plan; 8-24 GiB machine is prudent | 3-7 GiB |
| PageRank active | 88-144 GiB | 72-128 GiB | 8-24 GiB active plan; 12-32 GiB machine is prudent | 5-8 GiB only with streamed/windowed topology and approved compact state; otherwise reject |
| Louvain active | 96-192 GiB | 80-176 GiB | 10-40 GiB | generally reject on 8 GiB until a proven spill/partition plan exists |
| FastRP / large embeddings | 128-512+ GiB | 96-400+ GiB | dimension times node count dominates | reject, partition, quantize, or externalize |
| NodeSimilarity / large outputs | configuration-dependent and potentially explosive | same asymptotic risk | bounded by topK/topN and streaming/spill | reject unless output and candidates are bounded |

These are capacity-planning ranges, not measurements. They include an OLTP
resident/cached component for baseline and Scenario A. Scenario B intentionally
does not keep the complete OLTP store and every OLAP representation resident.

#### Algorithm-State Arithmetic At 200M Nodes

| State | Bytes | GiB |
|---|---:|---:|
| one `u32[V]` | 800,000,000 | 0.745 |
| one `u64[V]` or `f64[V]` | 1,600,000,000 | 1.490 |
| one bitset | 25,000,000 | 0.023 |
| three `f32[V]` arrays | 2,400,000,000 | 2.235 |
| three `f64[V]` arrays | 4,800,000,000 | 4.470 |
| seven 8-byte arrays | 11,200,000,000 | 10.431 |

The strict lane cannot optimize away these lower bounds without changing
precision, semantics, output, or algorithm.

### Final Latency And Throughput Model

The table uses `estimated elapsed time / tuned Neo4j or GDS elapsed time`.
Lower is better.

| Workload | Scenario A | Scenario B fast | Scenario B strict | Confidence |
|---|---:|---:|---:|---|
| Bolt point read, same OLTP semantics | 0.6x-1.2x | 0.6x-1.3x on OLTP path | 0.7x-1.5x | Low-medium |
| durable small write transaction | 0.7x-1.3x | 0.8x-1.4x; publication is async | 0.9x-1.6x | Low |
| routed 1-2 hop snapshot traversal | 0.2x-0.8x | 0.02x-0.20x | 0.05x-0.50x cold-page sensitive | Medium for direct kernel, low end to end |
| projection build | 0.7x-1.3x | 0.5x-2.0x | 0.8x-3.0x | Low |
| WCC versus tuned GDS EE | 0.65x-1.20x | 0.40x-1.10x | 0.80x-3.00x | Low-medium |
| PageRank versus tuned GDS EE | 0.70x-1.30x | 0.40x-1.00x after preparation | 1.50x-5.00x | Low |
| Louvain versus tuned GDS EE | 0.80x-1.40x | 0.50x-1.30x after preparation | 1.50x-6.00x or reject | Low |
| NodeSimilarity versus tuned GDS EE | 0.70x-1.50x | 0.30x-2.00x after preparation | 1.50x-10.00x or reject | Very low |
| result write-back | 0.7x-1.3x | 0.7x-1.5x | 1.0x-4.0x if spill/replay is required | Low |

Against GDS Community's four-thread cap, Rust may appear 2x-6x faster on
algorithms that scale to 16-32 cores. That is a valid product comparison but
not proof that the Rust kernel is better than tuned GDS Enterprise.

### Historical Absolute-Time Calibration

Scaling the old official LDBC100 results to a different graph and modern
software is not rigorous enough for a point estimate. The following wide bands
exist only to make "seconds versus days" concrete on the 64-logical-core,
large-memory reference class:

| Algorithm | Historical larger-graph GDS result | Modeled Neo4j/GDS target band | Scenario A | Scenario B fast | Scenario B strict |
|---|---:|---:|---:|---:|---:|
| WCC | 36 s | 15-45 s | 10-55 s | 8-50 s | 12-135 s |
| PageRank | 8.28 min | 3-9 min | 2-12 min | 1.5-9 min after preparation | 5-45 min |
| Louvain | 12.27 min | 5-14 min | 4-20 min | 3-18 min after preparation | 8-85 min or reject |

Confidence is very low. The required next benchmark replaces this table.
Range endpoints must not be cross-multiplied as though they came from one
matched run; measured ratios will be calculated pairwise on identical jobs.

### Physical Lower Bounds

For one 5.22 GiB single-orientation topology pass:

```text
resident effective bandwidth 5-40 GiB/s -> 0.13-1.04 s scan floor
strict NVMe effective 1-5 GiB/s       -> 1.04-5.22 s I/O floor
```

Irregular access, atomics, decoding, state writes, scheduling, and convergence
can multiply those floors by 2-20 or more. Iterative algorithms multiply them
again by iteration count. This is why storage specialization can win by
reducing bytes and passes, while `io_uring` alone cannot rescue an algorithm
that repeatedly scans too much state.

For the planning graph, a conventional 20-iteration PageRank illustrates the
direction of the effect:

```text
topology bytes alone = 20 * 5.22 GiB = 104.4 GiB
at 50 GiB/s effective DRAM = at least about 2.1 seconds
at 5 GiB/s effective NVMe  = at least about 20.9 seconds
```

Real elapsed time is much larger because this omits state traffic and compute.
The calculation does not predict a tenfold end-to-end slowdown; it invalidates
the assumption that moving topology to NVMe is itself a speedup.

### Preparation And Amortization Model

Every PageRank benchmark SHALL report:

```text
T_first  = T_prepare + T_open + T_execute + T_materialize
T_repeat =             T_open + T_execute + T_materialize
T_total(N) = T_prepare + N * T_repeat
```

`T_prepare` means GDS projection for Neo4j and snapshot compilation,
validation, and publication for Knight Bus. It is valid to compare hot
execution with preparation excluded only when both sides start with a ready
compatible graph. It is also valid to compare first requests when both sides
start from the same source facts. Mixing those states is not valid.

Scenario B fast can win total time after its extra preparation amortizes over
enough repeat jobs. Scenario B strict may never win on elapsed time and can
still win economically by completing under a memory limit where resident GDS
cannot start.

### `io_uring` Contribution Estimate

| Path | Scenario A planning effect | Scenario B planning effect | Interpretation |
|---|---:|---:|---|
| fully resident WCC/PageRank/Louvain loop | 0-5% | 0-5% | normally no storage I/O in hot loop |
| cold sequential topology scan | 0-20% | 0-30% | queue depth, batching, device, and kernel dependent |
| random cold page fetch | -10% to +20% | -10% to +30% | overhead can exceed benefit at low queue depth |
| snapshot build/spill/merge | 0-20% | 0-25% | useful if compute and I/O overlap |
| WAL/checkpoint | 0-15% | 0-15% | durability ordering and fsync dominate |
| backup/export | 0-25% | 0-25% | naturally batched and asynchronous |

These are experiment ranges, not sourced universal speedups. Neo4j now exposes
asynchronous page-cache I/O for supported Linux operations, so all tests must
compare against that feature enabled and disabled.

### Tail-Latency Contract Replacing P100

The product should publish this tuple for every benchmark:

```text
sample_count
run_duration
p50 / p95 / p99 / p99.9
observed_max
timeout_threshold
timeout_count
warm/cold/constrained cache state
result row and byte count
```

Initial design targets, not estimates:

| Operation | Candidate target on local same-host client |
|---|---:|
| small Bolt point read p99 | less than 10 ms |
| small Bolt point read observed max over 1 hour | less than 100 ms absent injected faults |
| durable small write p99 | less than 25 ms on declared fsync/storage policy |
| routed fixed-hop read p99 | less than 5 ms plus result streaming time |
| OLAP job | algorithm-, graph-, and result-specific deadline; never one global P100 |

### Disk, Build, And Freshness

| Dimension | Scenario A | Scenario B |
|---|---:|---:|
| Base OLTP store and indexes | roughly Neo4j-like | roughly Neo4j-like |
| OLAP topology | one resident projection, 6-20+ GiB modeled | 16-20 GiB current dual CSR, or 3-12 GiB per compressed/orientation-specific view |
| Generations retained | normally catalog-defined resident graphs | at least old and new during atomic publication |
| Projection Build Store | optional staging | 10-50+ GiB depending retained IR and compaction |
| Total disk versus Neo4j-only | 1.0x-1.6x | 2x-5x typical planning band; higher with many hot views |
| 50 GiB snapshot publication | 1-20 min after source is readable | 5-45 min with external sort, validation, and multiple views |
| OLAP freshness | projection refresh | generation watermark; seconds to tens of minutes |
| Small update cost | update OLTP and later rebuild projection | update OLTP, compile affected cells/runs, publish complete generation |

Scenario B trades storage capacity and freshness for controlled runtime RAM. A
marketing claim that omits this trade is incomplete.

### Operational Comparison

| Property | Scenario A | Scenario B |
|---|---|---|
| Crash surface | WAL, page cache, indexes, projection catalog | same plus build IR, generation publication, lineage, and spill |
| Portability | Linux optimization plus portable backend | same; strict direct I/O varies by filesystem/device |
| Observability | conventional DB and GDS metrics | adds per-plan memory receipt, page faults, bytes read, generation watermark |
| Failure behavior | memory pressure can terminate resident jobs | strict lane rejects before start; fast lane can still face page-cache pressure |
| Upgrade complexity | schema/store/protocol compatibility | same plus snapshot format and compiler compatibility |
| Reversibility | difficult after a broad internal port | higher per vertical slice because stock Neo4j/GDS remains an oracle |

---

## Verification Chain

### Fact-Check Questions

| Question | Answer | Evidence status |
|---|---|---|
| Is v002 correct for its fixed corpus? | Yes, according to archived verify and Neo4j parity records. | M, but large artifacts are absent here |
| Does v002 call a GDS algorithm? | No. | M, benchmark source |
| Does the current Rust code implement Bolt or Cypher? | No. | M, current source/Cargo inventory |
| Does current Rust implement any GDS algorithm kernel? | No; it implements 15 catalog/property surface entries. | M, dispatch source and tests |
| Is current dual CSR guaranteed to fit in 8 GiB at 200M/1B? | No; physical snapshot is 15.65-18.63 GiB for plausible keys. | D, exact format formula |
| Can WCC state fit within 8 GiB? | Yes in principle; parent/state is roughly 0.75-2 GiB depending width and design. | D plus source evidence |
| Can PageRank fit within 8 GiB? | Selected plans may: one uncompressed orientation plus three f64 arrays is already about 9.69 GiB, so strict mode must stream/window/compress topology and possibly use separately verified compact state. | D/R |
| Is strict 8 GiB PageRank expected to beat resident GDS? | No; the accepted planning ratio is 1.5x-5x slower. | D/R, pending experiment |
| Can a lower-RAM fast view beat GDS? | Possibly, only when it removes bytes/passes rather than externalizing them. | H |
| Are preparation costs included symmetrically? | They must be reported as first, repeat, and amortized totals for both engines. | Verification contract |
| Can Louvain universally fit within 8 GiB? | No evidence supports that. | D, local estimator structures |
| Does `io_uring` speed an all-resident kernel? | No direct mechanism beyond incidental runtime integration. | P/D |
| Is GDS already parallel and memory-estimated? | Yes. | P and local source |
| Can upstream tests all run unchanged against Rust? | No. | M, language/internal coupling inventory |
| Can full Enterprise parity be estimated locally? | No. | M, missing closed source/tests |

### Draft 2 Consistency Checks

The revised model passes these checks:

1. Every large speedup is tied to a changed read path, not language identity.
2. Every 8 GiB claim names an algorithm family and a strict execution mode.
3. mmap process RSS is not presented as total machine RAM.
4. GDS Community and Enterprise concurrency baselines are separated.
5. P100 is replaced by a reproducible observed-max contract.
6. full-parity Scenario B is not falsely priced below Scenario A.
7. old Neo4j benchmark numbers are labeled historical.
8. absent reports, datasets, Java runtime, and Python mismatch are disclosed.
9. strict PageRank is never presented as the fastest mode.
10. preparation is separated from execution and charged to both engines.
11. compact precision is treated as a semantic mode, not a free optimization.

---

## Experiments That Collapse The Estimate Ranges

### Experiment 1: Reproduce And Harden v002

Deliverables:

- pinned Neo4j image/version;
- pinned Python 3.13 environment;
- regenerated checksummed datasets or tracked manifests and generators;
- raw latency samples and observed max;
- cgroup or machine-level memory, page cache, major/minor faults, and I/O;
- Rust direct-runtime and Rust-over-Bolt variants;
- Neo4j warm/cold/constrained variants.

This tells us how much of the traversal win survives equal protocol and
service boundaries.

### Experiment 2: Ask Current GDS For Real Memory Estimates

On the same Neo4j/GDS release pair, call projection and algorithm `.estimate`
for synthetic dimensions and materialized representative graphs:

```text
200M nodes / 1B relationships
NATURAL and UNDIRECTED
zero, one, and several properties
WCC, PageRank, Louvain, NodeSimilarity, FastRP
stream, mutate, and write modes
```

Store full `treeView`, `mapView`, `bytesMin`, and `bytesMax`. This replaces the
historical memory calibration.

### Experiment 3: Proof Two - Direct WCC Parity

Run identical graph and configuration through:

```text
CALL gds.wcc.stream(...)
Knight Bus WCC over GraphAdjacencyRuntime
```

Normalize component partitions rather than comparing arbitrary component IDs.
Measure projection, open, compute, stream, total elapsed time, RSS, page cache,
faults, bytes read, and CPU.

### Experiment 4: Fast Versus Strict Storage

Run the same WCC and PageRank jobs with:

```text
resident/mmap warm
mmap cold
mmap under memory.max=8GiB
buffered pread
O_DIRECT where supported
io_uring buffered
io_uring direct/fixed-buffer where supported
```

The strict lane is promoted only if total cgroup peak stays within budget and
result parity remains exact under forced eviction and restart.

For PageRank, record both ready-view execution and first-request totals. The
fast lane must identify the byte/pass reduction behind any speedup. The strict
lane is judged against a declared slowdown budget, not required to beat the
resident engine.

### Experiment 5: Parallelism Sweep

For each engine and algorithm, sweep worker counts and record:

```text
elapsed time
CPU time
instructions and cycles
LLC misses
memory bandwidth if available
atomics/contention proxies
NUMA remote access
p99 task time and straggler distribution
```

Do not select the highest worker count by default.

### Experiment 6: One Hard Falsifier

Use NodeSimilarity or Louvain after WCC. If the architecture handles only WCC,
it proves one kernel, not a general substrate.

---

## Decision Gates

### Scenario A Promotion Gate

Continue a broad faithful port only if a compatibility-complete vertical slice
shows at least one of:

- 30% lower p99 at equivalent throughput and durability;
- 30% lower whole-machine peak memory;
- 2x throughput at the same p99;
- a major operational simplification worth the rewrite cost.

If a complete slice lands within 20% of Neo4j/GDS on all dimensions, the
language-only rewrite has not earned expansion.

### Scenario B Promotion Gate

Promote the read-shape architecture if:

- WCC result parity is complete across semantic and adversarial fixtures;
- strict WCC stays below 8 GiB total cgroup peak on the planning graph;
- strict WCC is no worse than 3x tuned GDS EE elapsed time;
- fast WCC is at or better than tuned GDS EE;
- PageRank demonstrates a second, iterative family without violating declared
  precision and convergence contracts;
- fast PageRank is at or better than tuned GDS EE after preparation, or its
  measured break-even run count is acceptable for the target workload;
- strict PageRank stays below 8 GiB total cgroup peak and is no worse than 5x
  tuned GDS EE; it is not required or advertised to be faster;
- snapshot publication is atomic and restart-safe;
- every run produces a memory receipt and execution receipt.

### Kill Or Narrow Gate

Narrow the product if:

- required property/result surfaces force full materialization for most jobs;
- strict mode repeatedly exceeds estimates;
- specialized views multiply disk beyond the customer's economic tolerance;
- publication lag makes the target use case invalid;
- compatibility work consumes the majority of time without unlocking a
  customer-visible workflow.

---

## Recommended Decision

Do not choose Scenario A as the primary strategy.

Retain it as:

1. a control estimate;
2. a possible OLTP implementation direction;
3. a way to port selected external compatibility surfaces;
4. a reminder that exact behavior still requires a large program.

Choose Scenario B as the product architecture, with two important disciplines:

1. **one compatibility shell, many physical plans;**
2. **one verification spine shared by fast and strict lanes.**

Within Scenario B, use the fast read-shape lane for the latency product and
the strict lane for the capacity product. Do not ask one lane to prove both
claims.

The next tangible milestone is not another architecture document. It is a
reproducible WCC evidence bundle that replaces the widest Draft 2 ranges with
measurements.

---

## Final Synthesis

The project is no longer asking whether compact Rust graph traversal can beat
a general Neo4j query path. v002 already answered that narrow question.

The unresolved question is harder and more valuable:

> Can Knight Bus preserve the Neo4j/GDS contract while selecting exact
> algorithm-shaped storage and state plans that use materially less total
> machine RAM, without making important jobs unacceptably slower?

Scenario A is technically possible for Community plus OpenGDS, but it is a
3-6-year, multi-team compatibility effort whose resident hot loops should be
assumed near parity with tuned GDS until measured otherwise. Rust can improve
tails, allocation, footprint, and control, but `io_uring` does not transform an
all-resident graph algorithm.

Scenario B is also a large program at complete parity, and may contain more
code. Its advantage is sequencing: it can produce a useful low-RAM OLAP
product years before broad server parity, and its gains come from explicit,
testable causes - fewer representations, narrower orientations, compressed or
windowed topology, compact state, streaming results, and bounded working sets.

The corrected PageRank thesis is deliberately narrower:

```text
Scenario A resident:
  likely near tuned GDS, with a planning range of 0.70x-1.30x elapsed time.

Scenario B fast:
  can be faster and use less active memory only by eliminating bytes/passes;
  planning hypothesis 0.40x-1.00x after preparation.

Scenario B strict 8 GiB:
  minimizes bounded residency by streaming/windowing topology;
  planning estimate 1.50x-5.00x elapsed time, normally slower.
```

Preparation can make the first Scenario B request slower even when its repeat
kernel is faster. The benchmark must therefore preserve both first-request
and amortized results. That distinction is now part of the estimate, not a
footnote.

The honest product claim today is:

```text
Proven:
  exact fixed-hop snapshot traversal on three generated datasets
  with lower recorded runtime RSS and latency than the tested Neo4j path.

Implemented but not yet algorithm-proven:
  low-RAM snapshot build/verify substrate, generation metadata,
  GDS registry, projection catalog, memory estimate shape,
  and selected graph/property procedures.

Next proof:
  direct WCC parity and same-machine RAM/latency against current GDS,
  followed by PageRank and one hard falsifier.
```

That is a strong position, provided estimates remain visibly separate from
measurements.

---

## Local Evidence Index

- [README v002 benchmark](../README.md)
- [Final v002 testing journal](../Final-Testing-Journal-v002.md)
- [PRD L1](../docs_PRD04/prd-l1.md)
- [Sol-01 architecture and 90-day plan](Sol-01.md)
- [Full rewrite feasibility study](Neo4j-Rust-Rewrite-Feasibility.md)
- [Architecture options](../docs_PRD04/Arch-options.md)
- [GRAIN architecture](../docs_PRD04/Arch05.md)
- [Algorithm/storage lever analysis](../docs_PRD04/Arch06.md)
- [SQLite navigation guide](../docs_PRD03/reference-learning/neo4j-family-dependency-graphs/sqlite-navigation-guide.md)

## External Primary Sources

- [Neo4j memory configuration](https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/)
- [Neo4j page-cache and disk guidance](https://neo4j.com/docs/operations-manual/current/performance/disks-ram-and-other-tips/)
- [GDS memory estimation](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/)
- [GDS system requirements](https://neo4j.com/docs/graph-data-science/current/installation/System-requirements/)
- [GDS WCC](https://neo4j.com/docs/graph-data-science/current/algorithms/wcc/)
- [GDS PageRank](https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/)
- [GDS Louvain](https://neo4j.com/docs/graph-data-science/current/algorithms/louvain/)
- [GDS Node Similarity](https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/)
- [Historical GDS Configuration Guide](https://go.neo4j.com/rs/710-RRC-335/images/Neo4j-Graph-Data-Science-Configuration-Guide-EN-A4.pdf)
- [Linux io_uring setup](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
- [Linux io_uring registration](https://www.man7.org/linux/man-pages/man2/io_uring_register.2.html)

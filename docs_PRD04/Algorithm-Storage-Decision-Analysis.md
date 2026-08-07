# A007 Algorithm-to-Storage Decision Analysis

Date started: 2026-08-06
Last reconciled: 2026-08-06
Scope: every artifact directly under `docs_PRD04/`
North star: `A007-spc-founder-interview-prep-v7.md`
Method: verification-first synthesis through a Shreyas Doshi product lens and
a Jeff Dean systems lens.

## Executive Answer

Knight Walker should not begin as a Neo4j rewrite, a generic graph database, a
new universal graph format, or a seven-algorithm catalog. A007 defines a much
more useful first product:

> A portable artifact-to-answer graph runner that accepts a hard resource
> budget, accounts for the complete working set, chooses `FIT`, `SPILL`,
> `APPROXIMATE`, or `REFUSE` before execution, enforces that choice, and emits a
> post-run estimate-versus-actual receipt.

The first customer job is a high-stakes security, IAM, dependency, SBOM, or
access-path question. The first engine proof should therefore pair:

1. Bounded reachability or shortest/access-path traversal, because it is the
   directly evidenced customer job.
2. WCC, because it is the simplest global algorithm with a deterministic
   correctness oracle and an intelligible `O(V)` state bill.

The storage answer is deliberately **custom OLAP storage per algorithm**, and
often more than one physical storage plan per algorithm. That is the intended
differentiation. It is a four-layer design:

```text
portable source artifact
  -> immutable canonical rebuild truth
  -> custom algorithm-shaped OLAP artifacts
  -> budget-selected storage + scratch + execution plan
```

The canonical generation should remain small and boring: stable dense IDs,
normalized edge facts or sorted runs, typed property sidecars, checksums,
statistics, and immutable generation identity. It is the compiler input and
recovery truth, not the universal serving layout. The serving layer is an
algorithm-artifact portfolio: path pages, WCC edge streams, PageRank pull
blocks, similarity incidence lists, community aggregation runs, oriented
triangle lists, and embedding operator slabs. Each family may expose multiple
artifacts that trade build cost, RAM, I/O, and latency differently.

The planner's product value is not merely selecting an algorithm. It selects a
point on an explicit frontier:

```text
SPEED        high RAM, lowest repeated-run latency
BALANCED     custom compressed topology, hot state resident
STRICT-RAM   bounded topology and state windows, more I/O/passes
MATERIALIZED generation-bound answers, lowest query latency, highest freshness cost
```

A specialized artifact is built eagerly when it is part of the chosen product
profile, lazily when the first run can pay for it, or not at all when admission
shows the trade is poor. The canonical truth keeps this portfolio reversible.

The recommended first-quarter strategy is:

```text
primary product: A007 portable bounded runner
first job:       security/dependency traversal
first oracle:    traversal plus WCC differential parity
first substrate: one deliberately custom path artifact and one custom WCC
                 artifact, initially encoded plainly before codec tuning
first proof:     enforced cgroup/RSS ceiling plus reconciled receipt
secondary path:  thin Neo4j/Bolt/export adapter if a design partner needs it
explicitly defer: full Neo4j rewrite, broad Cypher, full GDS surface,
                  universal GRAIN format, cells, and seven-family breadth
```

This is the highest-leverage path because it simultaneously tests the customer,
the enforcement contract, the graph representation, and the verification loop.
It avoids spending a quarter proving that Rust can reimplement interfaces while
leaving the paid behavior hypothesis untouched.

## 1. Governing Product Contract

### 1.1 A007 Is Authoritative

`A007-spc-founder-interview-prep-v7.md` is the governing product objective.
Everything else in this folder is classified as one of:

- supporting evidence;
- an implementation mechanism;
- a useful falsifier;
- historical reasoning;
- or a superseded product proposal.

The chronological corpus is valuable because it shows how the thesis changed.
It is not a set of co-equal requirements.

### 1.2 The Contract

For every admitted run, the system must make this state transition explicit:

```text
request
  = artifact generation
  + logical graph view
  + algorithm and parameters
  + result mode
  + exactness policy
  + memory, disk, and time budgets

request
  -> validate artifact and capabilities
  -> estimate complete working set
  -> select FIT | SPILL | APPROXIMATE | REFUSE
  -> reserve and enforce resources
  -> execute a named physical plan
  -> verify result semantics
  -> reconcile estimate with actuals
  -> emit answer plus receipt
```

The receipt is evidence of enforcement; it is not the enforcement mechanism.
Enforcement comes from reservations, bounded buffers, bounded result sinks,
spill policy, concurrency control, cancellation, and an external process or
cgroup ceiling.

### 1.3 First ICP And Job

The first ICP is a team that already has a graph-shaped artifact and a costly
question but does not want to adopt or provision a graph platform. The strongest
publicly supported examples are:

- security and IAM attack/access paths;
- dependency and SBOM blast radius;
- code or service dependency reachability;
- topology incident investigation;
- entity grouping through connected components.

Codebase intelligence is a founder-advantaged demo because the repository
already contains dependency graphs and verification tooling. Existing Neo4j or
GDS users remain useful design partners and comparison subjects, but they do not
define the first product.

### 1.4 Algorithm Order

A007's customer-driven order supersedes the older modeled-adoption order:

1. BFS, bounded reachability, and shortest/access paths.
2. WCC and connected components.
3. PageRank and centrality.
4. NodeSimilarity and kNN.
5. Louvain and Leiden.
6. Triangle counting and local clustering coefficient.
7. FastRP and embeddings.

The percentages previously attached to these families are prioritization
hypotheses, not telemetry. No public representative GDS procedure-frequency
dataset in this corpus supports the claimed `~85%` cumulative share.

### 1.5 Non-Goals For The First Proof

The first product does not need:

- Neo4j OLTP record-store parity;
- general Bolt or Cypher compatibility;
- the complete `gds.*` procedure catalog;
- mutate/write/model/pipeline parity;
- a database server;
- GraphBLAS as a universal substrate;
- an immutable public graph-format standard;
- cells, Hilbert ordering, graph grammar, or a schedule DSL;
- all-pairs shortest paths;
- or a claim that every exact algorithm can finish under every budget.

Refusal is a valid product outcome when it happens before expensive work and
includes a precise explanation.

## 2. Evidence Discipline

### 2.1 Evidence Labels

- `Observed`: measured in this repository or explicitly reported by a cited
  primary source in the corpus.
- `Derived`: arithmetic or a systems conclusion derived from stated inputs.
- `Modeled`: an estimate that has not been benchmarked here.
- `Hypothesis`: a product, adoption, or architecture claim requiring a test.
- `Superseded`: useful history whose conclusion is replaced by later evidence.

### 2.2 Decision Rules

1. Current customer evidence outranks an elegant historical architecture.
2. A later document wins only when it identifies the evidence that changed.
3. A graph representation, algorithm view, scratch layout, admission policy,
   result mode, and product API are separate decisions.
4. Exact and approximate modes must have different plan identities and receipt
   fields.
5. Whole-process memory matters. Heap-only or allocator-only measurements are
   insufficient.
6. `mmap` makes addressing convenient; it does not create a strict RSS ceiling.
7. A faster language does not remove the topology, state, I/O, synchronization,
   or output lower bounds.
8. Every performance claim must name graph shape, query shape, hardware, cache
   state, output semantics, and build cost.

### 2.3 Evidence Hierarchy

For product claims, use this order:

```text
paid behavior or design-partner artifact
  > current primary customer evidence
  > current vendor documentation and source
  > reproducible benchmark in this repository
  > peer-reviewed mechanism precedent
  > code-reading inference
  > modeled architecture estimate
  > intuition
```

## 3. Complete Corpus Coverage Ledger

All 54 pre-existing artifacts directly under `docs_PRD04/` were inventoried.
The large raw dump is treated as uncurated substrate because its own header says
the synthesized findings live in `evidence01.md` and
`Everything-We-Know-Now.md`. The workbook was inspected sheet by sheet.

### 3.1 Founder And Product Lineage

| Artifact | Status | A007 contribution or disposition |
| --- | --- | --- |
| `A000-spc-founder-interview-prep.md` | Read, superseded | Establishes the original low-RAM runtime story; useful lineage, not current positioning. |
| `A001-spc-submission-source-draft.md` | Read, source draft | Application-source narrative and founder context; not an architecture authority. |
| `A002-spc-founder-interview-prep-v2.md` | Read, superseded | Expands graph universality and use cases; later evidence narrows the customer. |
| `A003-spc-founder-interview-prep-v3.md` | Read, superseded | Introduces quote/business framing; estimate novelty is later rejected. |
| `A004-spc-founder-interview-prep-v4.md` | Read, superseded | Adds embedded/DuckDB framing; useful distribution intuition, not unique whitespace. |
| `A005-spc-founder-interview-prep-v5.md` | Read, superseded | Adds agents, graph literacy, and scarcity thesis; agent-market assertions remain unproven. |
| `A006-spc-founder-interview-prep-v6.md` | Read, predecessor | Narrows toward security/code/dependency and enforcement; direct precursor to A007. |
| `A007-spc-founder-interview-prep-v7.md` | Read, governing | Defines portable bounded runner, first ICP, evidence corrections, six-week test, and falsifiers. |
| `spc-interview-prep-critique-202607311413.md` | Read, supporting | Exposes missing receipts, algorithm clarity, and wedge discipline; its concerns are resolved more fully by A007. |

### 3.2 Architecture And Algorithm Lineage

| Artifact | Status | A007 contribution or disposition |
| --- | --- | --- |
| `Arch-options.md` | Read, supporting | Separates OLTP truth, Build Store, immutable OLAP generation, and catalog. Keep generation discipline; drop full-rewrite requirement. |
| `Arch01.md` | Read, supporting | Compares flat, tile, Budget Machine, Foundry, and algebra timelines. Budget Machine is the surviving common spine. |
| `Arch02.md` | Read, supporting | Defines access signatures and scan-heavy versus state-heavy split. Adoption weights remain modeled. |
| `Arch03.md` | Read, supporting | Corrects novelty using DataFusion, GraphBLAS, and out-of-core precedents; supports substrate-first execution. |
| `Arch04.md` | Read, supporting | Brownfield correction: grow a bounded executor before a rewrite. Plugin conclusion is now optional, not the north star. |
| `Arch05.md` | Read, experimental | GRAIN, degree-ranked IDs, hot/warm/cold encodings, and manifest pricing. Useful hypothesis; polynomial state formulas are not evidence. |
| `Arch06.md` | Read, high-value | Best family-by-family state and access-plan analysis. Supplies the core storage matrix, but numerical improvements remain modeled. |
| `A01-202607260102.md` | Read, high-value | Makes `algorithm x query shape x cadence` the physical-design unit and argues for derived materializations over universal layout. |
| `mega-arch-20260726v1.md` | Read, supporting | Consolidates byte formulas and RAM patterns. Useful arithmetic baseline. |
| `innovation-mega-arch-20260726v1.md` | Read, experimental | Adds precomputed answers, precision ladders, peeling, indexes, and quotients. Its serialized-random-access latency argument is superseded. |
| `innovation-storage-timelines-20260726v2.md` | Read, experimental | Execution tape, freeze ordering, Hilbert layout, residual quotient, and generation braid; all require cheap falsifiers. |
| `innovation-storage-timelines-20260726v3.md` | Read, experimental | Answer lattice, Hot Boot, Grammar Graph, and Sketch Deck. Answer reuse and hot starts survive; grammar remains high variance. |
| `Conclusion-01-Spend-Disk-Buy-RAM.md` | Read, superseded | Early PageRank verdict and very large RAM/speed claims; query-shape framing survives, baseline does not. |
| `Conclusion-01-v2-First-Principles-Derivation.md` | Read, authoritative arithmetic | Corrects topology/state floors, memory-level parallelism, and realistic RAM/latency ranges. |
| `AlgoExplainers-ASCII.md` | Read, supporting | Clear family use cases and access patterns. One WCC label-size statement conflicts with later `u32` arithmetic. |
| `Arch-Summary-ASCII-v1.md` | Read, visual companion | Visual recap of architecture options; no independent authority. |
| `ASCII-Conclusion-01-v2-First-Principles.md` | Read, visual companion | Visual form of the corrected first-principles derivation. |
| `ASCII-innovation-mega-arch-20260726v1.md` | Read, superseded in part | Visualizes the innovation set but repeats the refuted serialized 80 ns lookup premise. |
| `Sol-01.md` | Read, major predecessor | Rich proof-carrying/read-shape architecture and 90-day plan. A007 replaces its Neo4j-plugin/WCC-first product framing but retains many engine seams. |
| `Sol-02.md` | Read, supporting | Correctly downgrades the seven-family Pareto percentages to a hypothesis. |
| `prd-l1.md` | Read, superseded product scope | Full Neo4j rewrite PRD. Its immutable snapshots, watermarks, and memory rules are mechanisms, not current product scope. |

### 3.3 PMF, GTM, And Strategic Lineage

| Artifact | Status | A007 contribution or disposition |
| --- | --- | --- |
| `PMF01.md` | Read, superseded | Low-RAM, compatibility, and receipt positioning. Estimation and whitespace claims are later corrected. |
| `PMF02.md` | Read, supporting | Narrows differentiation from estimate to enforcement and favors benchmark plus interviews. |
| `PMF03.md` | Read, superseded in part | Adds Aura/Kuzu/GraphRAG competitor movement; later evidence corrects Kuzu's status and GraphRAG confidence. |
| `PMF04.md` | Read, superseded | Compares format-first, engine-first, and receipt product. Format-first and receipt-only are rejected by later evidence. |
| `Everything-We-Know-Now.md` | Read, high-value predecessor | Corrects moat, market, and competitor assumptions; argues for a plugin. A007 keeps corrections but chooses a portable runner. |
| `Real-Pain-Wrong-Product.md` | Read, high-value predecessor | Strong warning that pain is real but the product was wrong. Its tests and competitor facts directly inform A007. |
| `The-One-Question-Left.md` | Read, high-value predecessor | Defines the enterprise analytics-incident question and plugin gates. Retained as an integration scenario. |
| `Not-First-Still-Different.md` | Read, supporting | Correctly accepts convergent competitors and narrows residual differentiation. |
| `Win-The-Whales-Vision.md` | Read, supporting | Production incident as trigger, cost as justifier, peak-shaving story. Enterprise whale path is not the first A007 wedge. |
| `gtm-POC-01.md` | Read, implementation precedent | Concrete Neo4j WCC plugin, ID mapping, FFI, and parity design. Keep as an adapter option. |
| `graph-adoption-wave-thesis-202608011557.md` | Read, hypothesis | "Graph compute DuckDB moment" framing is useful; agent-driven demand is not established. |
| `simulation01.md` | Read, historical evidence synthesis | Useful pain/use-case corpus, but estimate novelty, empty turf, Kuzu status, sharding, and GraphRAG claims are corrected later. |

### 3.4 Current Evidence And Research Substrate

| Artifact | Status | A007 contribution or disposition |
| --- | --- | --- |
| `evidence01.md` | Read, authoritative correction | Vendor-primary correction: estimation exists, turf is occupied, Ladybug/Slater/Grafeo matter, and paying low-RAM segment may be small. |
| `graph-compute-customer-evidence-dossier.md` | Read, governing evidence | 65-row current evidence set; establishes enforcement contract and security/access-path wedge. |
| `graph-compute-evidence-matrix.xlsx` | Binary-inspected | Sheets `Read Me`, `Evidence Matrix`, `Top 15`, `Claim Audit`, and `Summary`; 65 rows: A=26, B=16, C=21, D=2; support=46, mixed=11, counter=8. |
| `raw-research-evidence-dump-2026-07-26.txt` | Reference-only | 31,856-line machine dump of agent transcripts and two research runs; uncurated and explicitly superseded by synthesized dossiers. |
| `github-repo-longlist.md` | Read, reference shelf | Broad repository map. Scores target the old full-rewrite PRD and must be recalibrated for A007. |
| `SUM01.md` | Read, index/summary | Folder map and prior synthesis; no independent evidence authority. |

### 3.5 Historical Research And Executable-Spec Artifacts

| Artifact | Status | A007 contribution or disposition |
| --- | --- | --- |
| `Reference-Learning-Critique-Gaps.md` | Read, supporting | Separates research coverage from implementation readiness and demands executable memory/publication contracts. |
| `GDS-PRD-L1-Evidence-Dossier-Executable-Spec.md` | Read, historical spec | Defines a source-backed GDS reading pass. Useful oracle research; too broad for the first product. |
| `GDS-PRD-L1-Evidence-Dossier-v2-Executable-Spec.md` | Read, historical spec | Deepens procedure-to-kernel, formula, lifecycle, and oracle requirements. Reuse selectively. |
| `Gap-Closure-Executable-Spec.md` | Read, historical spec | Defines registry, compatibility, memory, publication, and storage artifacts for the old PRD. |
| `Gap-Closure-Implementation-Plan.md` | Read, historical plan | Good status semantics and verification discipline; scope exceeds A007. |
| `V003-Reference-Folder-Learning-Spec.md` | Read, reference process | Excellent decision-first and graph-tool study method. Its full-rewrite outcome spine is superseded. |

## 4. What The Corpus Actually Teaches

### O-001: The Corpus Contains Several Product Generations

The progression is roughly:

```text
low-RAM Neo4j rewrite
  -> graph-compute platform
  -> pre-run quote
  -> embedded/DuckDB graph engine
  -> agent-era graph literacy
  -> Neo4j plugin and production safety valve
  -> portable enforceable artifact runner for a specific high-stakes job
```

Reading every proposal as current would create an impossible roadmap. A007 is
the correction layer that turns accumulated insight into a falsifiable product.

### O-002: Four Technical Decisions Were Repeatedly Collapsed

These must remain separate:

1. The canonical persisted graph artifact.
2. An algorithm/query-specific derived view.
3. Runtime scratch and result state.
4. Admission, spill, approximation, and publication policy.

For example, Louvain's tally explosion is not fixed by a smaller CSR; PageRank's
pull locality is not fixed by a receipt; a path query's result explosion is not
fixed by streaming topology. A format-only architecture misses three-fourths of
the contract.

### O-003: Enforcement Is The Wedge, Not Estimation

Neo4j GDS, Aura Graph Analytics, Neptune Analytics, and other systems already
estimate or control capacity. A007 survives because its decision is stronger:

```text
incumbent: estimate -> provision an in-memory tier -> run may still deplete
A007:      estimate -> bind plan to hard budget -> finish, degrade, or refuse
```

The estimator must include graph representation, conversion, algorithm state,
frontier/queue, result materialization, buffers, spill metadata, page-cache
policy, retained generations, and concurrency margin.

### O-004: Query Shape Outranks Algorithm Name

The correct optimization unit is:

```text
(generation, logical view, algorithm, parameters, result mode, cadence, budget)
```

Examples:

- source-target BFS is not all-pairs shortest path;
- bounded reachability is not full predecessor materialization;
- global PageRank is not personalized PageRank;
- top-K ranks are not stream-all ranks;
- exact all-pairs similarity is not sketch candidate generation plus exact
  reranking;
- one Louvain run is not a warm-started repeated generation.

### O-005: The First Proof Is Traversal Plus WCC

Traversal proves the customer's job. WCC proves a global scan, deterministic
partition semantics, memory admission, and result reconciliation. PageRank is a
strong third proof because it stresses repeated pull scans and dense vectors,
but PageRank alone would optimize a benchmark before proving the A007 job.

### O-006: Scan-Heavy And State-Heavy Families Need Different Remedies

Storage/read-shape improvements have high leverage for WCC, BFS, PageRank,
triangles, and parts of FastRP. Louvain and NodeSimilarity can be dominated by
dynamic algorithm state. For them, the primary controls are bounded state,
partitioned spill, candidate control, approximation, or refusal.

### O-007: A Receipt Must Be Machine-Verifiable

At minimum, a completed receipt should bind:

- artifact and generation checksum;
- normalized logical-view checksum;
- algorithm, parameters, seed, and engine version;
- exactness class and output mode;
- declared memory, disk, and time budgets;
- estimated fixed, per-vertex, per-edge, queue/frontier, result, conversion,
  and retained-state bytes;
- selected plan and capability requirements;
- actual cgroup/RSS high-water mark and engine reservation peak;
- bytes read/written, spill bytes, faults, and cache mode;
- CPU and wall time;
- output cardinality and checksum;
- approximation bound or quality measurement;
- oracle result and tolerance;
- estimator absolute and percentage error.

### O-008: The Highest-Risk Unknown Is Behavioral

The evidence establishes difficult graph working sets. It does not establish
that buyers will pay for a receipt or that the low-RAM segment is large. One
Neo4j staff statement explicitly says enterprise customers rent very large
machines and insufficient demand prevented out-of-core prioritization.

The six-week proof must therefore test whether the bounded contract changes a
real decision, not merely whether it produces an attractive benchmark.

### O-009: A007 Is A Learning Product Before A Platform

The first product has one artifact, one high-stakes job, one enforced plan, one
receipt, one competitor comparison, and one paid-pilot decision. General graph
database work is downstream option value.

### O-010: Every Supported Algorithm Gets Custom Storage; Extra Variants Are Earned

The first supported plan for each family should already be algorithm-shaped.
The generic canonical representation is a compiler input and debugging control,
not the differentiated execution target. A second or third custom variant
should be persisted when:

```text
expected_reuses * direct_plan_penalty
  > build_time + build_IO + retained_disk_cost
    + invalidation_cost + operational_complexity
```

Otherwise, compile only the family's baseline custom artifact. Direct execution
on canonical facts remains a verification fallback, not the desired steady
state.

### O-011: The Best RAM Wins Often Come From Eliminating Representations

The highest-confidence reductions are not exotic compression ratios. They are:

- do not load unused properties;
- do not materialize a projection that can remain a logical view;
- do not keep both edge orientations when only one is needed;
- do not build an entity-entity clique when incidence is exact;
- do not retain full output when the caller requested top-K or a digest;
- do not keep multiple graph generations resident;
- do not keep all state resident when an exact partitioned plan exists.

### O-012: Rust And `io_uring` Are Mechanisms, Not The Thesis

Rust can remove JVM object overhead, reduce GC variance, make ownership and
buffer budgets explicit, and support efficient static distribution. `io_uring`
can reduce syscall and scheduling overhead when the execution plan has enough
independent queued I/O. Neither changes the number of edges, the amount of
algorithm state, SSD bandwidth, synchronization, or output volume. The planner
must first eliminate bytes and expose queue depth; only then can asynchronous I/O
matter.

## 5. Recommended Physical Architecture

### 5.1 The Four-Layer Model

```text
Layer A: source adapter
  CSV | Parquet | Arrow | BloodHound/OpenGraph | Neo4j export | code graph

Layer B: immutable canonical generation
  manifest + dense-id map + topology + typed columns + checksums + statistics

Layer C: exact or declared-approximate physical views
  reverse adjacency | undirected stream | incidence | oriented motif lists
  rank pull view | landmarks | sketches | prior-generation warm state

Layer D: per-run plan and bounded state
  fixed buffers + state slabs + frontier/queue + result sink + spill + receipt
```

The source adapter is replaceable. The canonical generation is rebuild truth.
Physical views are compiled OLAP serving artifacts and the primary execution
surface; they remain evictable because canonical truth can reproduce them.
Per-run state is disposable.

### 5.2 Canonical Generation V1

Do not invent a public standard in quarter one. Use an internal generation with
the smallest fields needed to prove A007:

```text
generation/
  manifest.json
  nodes.idmap
  topology.forward.offsets
  topology.forward.neighbors
  topology.reverse.*              # optional capability
  edges.sorted-runs/*             # optional strict scan source
  node-properties/*               # typed, late materialized
  edge-properties/*               # typed, late materialized
  statistics.json
  checksums.json
  views/*                         # evictable derived views
  receipts/*
```

Required manifest facts:

- source identity and source watermark;
- generation hash and format version;
- `V`, directed `E`, self-loop and parallel-edge semantics;
- ID width and offset width;
- directedness and available orientations;
- sorted-neighbor and uniqueness guarantees;
- property types, null/default semantics, and aggregation rules;
- degree histogram and high-degree quantiles;
- block byte sizes, checksums, and codecs;
- available derived-view capabilities;
- build-time and retained-byte receipt.

### 5.3 View Promotion States

```text
ABSENT
  -> EPHEMERAL_BUILDING
  -> EPHEMERAL_READY
  -> PROMOTION_CANDIDATE
  -> PERSISTED
  -> STALE
  -> EVICTED
```

Every supported family must have at least one `EPHEMERAL_READY` or `PERSISTED`
algorithm-native artifact before it is called implemented. Promotion of
additional speed, strict-RAM, or materialized variants requires evidence.
Eviction must never lose logical truth. A view is invalid if its generation,
projection hash, property semantics, or algorithm-relevant configuration does
not match the request.

### 5.4 Fast And Strict Lanes

`FAST`:

- `mmap` and OS page cache are allowed;
- useful for latency and warm-cache throughput;
- reports mapped and resident memory separately;
- makes no hard-RSS claim unless externally constrained and measured.

`STRICT`:

- explicit fixed buffer pool;
- bounded readers and result chunks;
- declared spill directory and byte limit;
- controlled concurrency;
- external cgroup/process ceiling;
- refusal when the plan cannot honor the ceiling.

The same algorithm semantics should run through both lanes. Their receipts and
performance claims must remain separate.

### 5.5 Custom OLAP Artifacts Are The Product Differentiator

The canonical generation exists so the system can reproducibly compile several
different physical graphs from one logical truth. It should not force every
kernel through a generic adjacency API when a custom representation can remove
bytes, state, branches, passes, or uncertainty.

The intended compiler output looks like this:

```text
canonical generation G
  |
  +-- path/<view-hash>/<variant>/
  |     typed forward/reverse blocks, frontier indexes, weights, landmarks
  |
  +-- wcc/<view-hash>/<variant>/
  |     canonical undirected edge runs, union blocks, label checkpoints
  |
  +-- pagerank/<view-hash>/<variant>/
  |     pull-sorted reverse blocks, degree reciprocals, dangling bitmap
  |
  +-- similarity/<view-hash>/<variant>/
  |     incidence postings, degree bands, sketch deck, candidate buckets
  |
  +-- community/<view-hash>/<variant>/
  |     weighted undirected blocks, aggregation runs, hierarchy levels
  |
  +-- triangles/<view-hash>/<variant>/
  |     degree/degeneracy-oriented lists, hub bit pages
  |
  +-- fastrp/<view-hash>/<variant>/
        normalized operator blocks, feature handles, embedding slabs
```

Every directory has its own manifest and memory formula. An artifact manifest
must declare:

- logical generation and projection hash;
- algorithm family and supported query/configuration class;
- exactness and numeric precision;
- physical ordering, codec, ID range, and block size;
- required companion state and scratch formula;
- supported execution profiles;
- build time, build peak, retained bytes, and invalidation rule;
- canonical decoded-stream checksum;
- expected sequential/random byte movements per pass;
- and the benchmark evidence that justified this variant.

This is how custom storage improves predictability: the estimator prices a
closed physical plan rather than an abstract graph plus an algorithm name.

### 5.6 Four Architecture Profiles

These profiles are not four separate products. They are four selectable
physical architectures exposed by one planner. A deployment can compile only
the profiles it values.

| Profile | Physical strategy | RAM shape | Latency shape | Build/freshness cost | Best customer choice |
| --- | --- | --- | --- | --- | --- |
| `SPEED` | Fully resident, uncompressed or lightly encoded, algorithm-native topology and hot state | Highest; often `O(E) + O(V) + algorithm state` | Lowest repeated-run latency; warm-cache optimized | High retained RAM and often duplicate topology | Dedicated analytics host, frequent repeated jobs, latency valued over footprint |
| `BALANCED` | Algorithm-native compressed blocks; indexes and dense state resident; topology mapped/read in predictable ranges | Medium; topology residency elastic, important state fixed | Near-memory speed when decode is cheaper than misses; moderate cold penalty | Moderate custom build and retained disk | Default server/container profile |
| `STRICT-RAM` | External-memory topology plus state windows/capsules, fixed buffer pool, spill runs, explicit I/O schedule | Lowest and most enforceable; commonly `O(V)` or `O(window)` rather than `O(E)` resident | Slower through extra passes, writes, and device bandwidth | Highest temporary I/O; stable machine requirement | Laptop, constrained container, air-gapped or fixed-budget job |
| `MATERIALIZED` | Store generation-bound final answers or reusable certified intermediates | Tiny query RAM; build may use any admitted profile | Lowest query latency, potentially just indexed lookup/stream | Highest staleness and recomputation burden | Repeated fixed query/configuration over slowly changing generations |

The quote must expose the chosen profile and alternatives:

```text
SPEED:        28-36 GiB estimated, 42-58 s, no spill
BALANCED:      9-13 GiB estimated, 55-80 s, bounded topology cache
STRICT-RAM:  3.5-4.0 GiB cap, 2.5-5.0 min, <= 40 GiB temporary I/O
MATERIALIZED: 180 MiB query cap, < 2 s lookup, 11-18 min rebuild
```

Those numbers are illustrative schema examples, not current performance claims.
The product distinction is that every option is explicit before execution and
reconciled afterward.

### 5.7 Portfolio Policy

For each supported family:

1. Ship at least one genuinely algorithm-native artifact.
2. Keep one simple plain encoding as the semantic and performance control.
3. Add a RAM-minimized variant when a real target budget requires it.
4. Add a latency-maximized resident variant when repeated demand repays RAM.
5. Add a materialized answer only for a stable query/configuration class.
6. Keep approximate storage and state in a separately named capability.
7. Let receipts decide which variant becomes the default for each workload
   cohort; do not select by elegance.

### 5.8 Pareto Plan Selection

For a request, the planner estimates every compiled variant and removes
dominated plans. Plan A dominates Plan B only when A is no worse on all hard
constraints and is strictly better on at least one user objective:

```text
hard constraints
  exactness, output semantics, memory cap, temporary-disk cap, deadline,
  generation freshness, allowed build work

objectives
  minimize peak RAM
  minimize time to first result
  minimize total wall time
  minimize temporary I/O
  minimize retained disk
  minimize rebuild/freshness lag
```

The system then returns the remaining Pareto choices rather than pretending
there is one universal optimum:

| User policy | Selection rule |
| --- | --- |
| `--minimize-ram` | Choose the lowest enforced peak that meets deadline and exactness. |
| `--minimize-latency` | Choose the lowest end-to-end wall time including any missing-view build. |
| `--reuse-count N` | Amortize build and retained-disk cost across `N` expected runs. |
| `--freshness-max T` | Reject materialized plans whose source generation age exceeds `T`. |
| `--no-approx` | Remove every sketch, quantized, capped, sampled, or early-stop plan. |
| `--deadline T` | Remove plans whose conservative runtime range crosses `T`. |
| `--disk-cap D` | Remove spill/materialization plans whose temporary plus retained disk exceeds `D`. |

A quote should include at least two nondominated choices when they exist. This
turns the RAM/latency tradeoff into a product surface and creates the data needed
to learn which point customers actually choose.

## 6. Algorithm-to-Storage Decision Matrix

| A007 order | Family and first query shape | Canonical input | Preferred derived view | Dominant scratch | Exact budget action | Approximate action | First verification oracle |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | Bounded BFS/reachability | Forward CSR or source-range channels | Reverse CSR for bidirectional search; filtered channel/page index | visited bitmap, frontier, optional distance/predecessor or priority queue | fit, demand-page, spill distance/predecessor, or refuse explosive output | bounded depth/result cap only when declared as changed semantics | exact reachable set/path against GDS, NetworkX/igraph tiny oracle, and hand fixtures |
| 2 | WCC | Undirected edge stream or symmetric CSR | Canonical pair-once stream; optional precomputed component labels for repeated fixed generations | `u32[V]` parent/label plus frontier/union buffers | stream edges with resident labels; shard labels if needed; refuse if state cannot fit | none needed for first proof | partition equivalence after component-ID canonicalization; GAPBS/GDS |
| 3 | Global PageRank top-K or stream-all | Forward topology | Reverse CSR/CSC plus exact out-degree/reciprocal column | two rank vectors, optional residual/delta vector, top-K heap/output | pull scan; stream vectors by slab only with a proven exact schedule; spill output; refuse impossible state | reduced precision or early convergence only as explicit approximate plan | seeded/tolerance parity, rank sum, residual, top-K ordering against GDS/GAPBS |
| 4 | NodeSimilarity top-K | Source-aligned adjacency or exact entity-feature incidence | Feature postings, degree bands, LSH/sketch buckets | sketches, candidate accumulators, bounded top-K heaps | exact bucketed candidates and exact rerank; spill candidate runs; refuse exact all-pairs explosion | MinHash/LSH candidate generation plus exact rerank, with recall receipt | pair scores and tie rules on fixtures; recall@K and exact rerank checks |
| 5 | Louvain/Leiden | Weighted undirected adjacency | Per-level compacted graph generations or sorted community-edge runs | community IDs, weighted degree, modularity totals, dynamic neighbor-community tallies | bounded/spilled tally map and one level at a time; refuse if exact state bound is unavailable | capped tallies, sampled edges, or early stop only under an approximate plan | seeded invariants, modularity, partition validity, and tolerance; IDs alone are not an oracle |
| 6 | Triangle count/LCC | Sorted adjacency | Degree/degeneracy-oriented forward-only lists; dense-hub bit pages if measured | intersection buffers, global or per-node counters | exact intersections with bounded buffers; spill per-node results | sampling only as separately named estimate mode | exact global/per-node counts on adversarial motifs and independent library |
| 7 | FastRP/embedding | Sparse topology plus declared features | Normalized sparse operator blocks and output slabs | one or two embedding matrices, random seed state, streamed output | block/slab output, declared precision, spill result; refuse dimensions that cannot be bounded | quantized state, fewer dimensions, sampled propagation only with quality receipt | seed/config parity where possible, vector tolerance, checksum, downstream quality probe |

### 6.0 Per-Algorithm Architecture Portfolio

This table is the direct answer to "which custom storage for which algorithm?"
Each row is a different legitimate product mode. Relative latency is against
the other Knight Walker plans for the same semantics, not a measured Neo4j
speedup.

| Family | Profile | Custom OLAP storage | RAM behavior | Relative latency | Predictability |
| --- | --- | --- | --- | --- | --- |
| Paths | `SPEED` | Type-partitioned dual CSR, inline narrow weights, resident landmark/index data | High `O(E)` residency plus frontier/queue | Best | High after build |
| Paths | `BALANCED` | Source-range forward/reverse blocks, type bitmap, property zone maps, compressed neighbors | Bounded cache plus state | Good; selective queries can beat resident generic CSR by skipping blocks | High |
| Paths | `STRICT-RAM` | Frontier-addressable edge channels, disk distance/predecessor slabs, spillable priority queue | Fixed buffers plus bitsets/window state | Slowest; proportional to touched blocks and spill | Very high if output is bounded |
| Paths | `MATERIALIZED` | SCC/condensation DAG, access labels, landmark tables, or cached source-target answers | Tiny query RAM | Best for covered queries | Very high, but narrow and freshness-sensitive |
| WCC | `SPEED` | Symmetric CSR with Afforest/label-propagation-friendly ordering | High topology residency plus labels | Best first-run | High |
| WCC | `BALANCED` | Canonical pair-once undirected edge runs plus resident union-find labels | `O(V)` labels plus bounded edge buffers | One/few sequential scans; often excellent | Very high |
| WCC | `STRICT-RAM` | Range-sharded edge runs and parent/label state capsules with checkpointed unions | `O(window)` state possible with more external merging | Slowest through repeated state I/O | Very high |
| WCC | `MATERIALIZED` | Component label column plus spanning forest/certificate | Tiny query RAM | Lookup/stream speed | Very high per generation |
| PageRank | `SPEED` | Resident reverse CSR, reciprocal out-degree, dangling bitmap, NUMA-partitioned rank arrays | Reverse topology plus 2-3 dense vectors | Best iterative latency | High |
| PageRank | `BALANCED` | Compressed pull blocks ordered by destination range; degree column resident; hot hubs pinned | Dense vectors resident, topology cache bounded | Near-speed if decode is hidden by bandwidth savings | High |
| PageRank | `STRICT-RAM` | 2D source-by-destination pull tiles with SSD rank slabs, bounded destination accumulators, and fixed double buffers | `O(source_window + destination_window)` rank/state plus buffers | Extra tile and rank-slab reads/writes; much slower | Very high |
| PageRank | `MATERIALIZED` | Exact rank vector, top-K index, optional previous-vector warm-start certificate | Tiny query RAM | Lookup/stream speed | Very high for fixed damping/tolerance/view |
| NodeSimilarity | `SPEED` | Resident sorted adjacency/incidence, dense feature buckets, in-memory candidate tables | Potentially enormous and distribution-dependent | Best when it fits | Medium unless candidate bounds are strong |
| NodeSimilarity | `BALANCED` | Feature-to-entity postings, degree bands, compact sketches/LSH buckets, exact rerank lists | Bounded sketches and top-K; candidates partitioned | Good for top-K; avoids clique construction | High with histogram-based quote |
| NodeSimilarity | `STRICT-RAM` | External feature buckets, pair-contribution sort runs, merge-reduce exact scores, disk top-K partitions | Fixed buffers and bounded merge fan-in | Slowest but exact for admitted candidate volume | Very high |
| NodeSimilarity | `MATERIALIZED` | Per-node top-K neighbor lists and scores keyed by generation/config | Tiny query RAM | Lookup speed | Very high, expensive to refresh |
| Louvain/Leiden | `SPEED` | Resident weighted symmetric CSR, resident tally arenas, all current level state | Highest and most volatile | Best | Medium because tally/state depends on evolution |
| Louvain/Leiden | `BALANCED` | Community-range edge blocks, bounded tally arenas, one compacted level at a time | Fixed vectors resident; tally overflow spills | Moderate | High if overflow is priced |
| Louvain/Leiden | `STRICT-RAM` | External community-edge sort/reduce runs, disk level graphs, windowed community state | Fixed buffers/window state | Slow through repeated external aggregation | Very high |
| Louvain/Leiden | `MATERIALIZED` | Hierarchy, membership column, modularity/quality metadata, optional warm-start mapping | Tiny query RAM | Lookup speed | High for fixed seed/config; rebuild sensitive |
| Triangles/LCC | `SPEED` | Resident degree-oriented sorted adjacency plus dense hub bitsets | High topology/hub residency | Best intersections | High |
| Triangles/LCC | `BALANCED` | Compressed oriented lists, hub cache, block-local SIMD intersection metadata | Bounded cache plus counters | Good; often dual RAM/latency win | High |
| Triangles/LCC | `STRICT-RAM` | 2D edge tiles or wedge runs, external sort/intersection, spilled per-node counters | Fixed buffers | Slowest through extra generated records | Very high |
| Triangles/LCC | `MATERIALIZED` | Global/per-node triangle counts and LCC columns | Tiny query RAM | Lookup speed | Very high per generation |
| FastRP | `SPEED` | Resident normalized sparse operator plus full `f32` ping-pong matrices | Extremely high `O(V * dimensions)` | Best | High once dimensions are fixed |
| FastRP | `BALANCED` | Operator blocks plus dimension-blocked exact propagation and disk-backed output slabs | `O(V * dimension_block)` plus topology cache | More topology passes; moderate | Very high |
| FastRP | `STRICT-RAM` | Vertex-range and dimension-range tapes, fixed operator buffers, external matrix slabs | Fixed by block dimensions and buffers | Slowest due repeated sparse-dense passes | Very high |
| FastRP | `MATERIALIZED` | Generation/config/seed-bound embedding matrix, optional vector index | Query RAM set by result/index access | Lookup speed | Very high; expensive storage and refresh |

The `BALANCED` and `STRICT-RAM` variants are where maximum differentiation is
most likely. `SPEED` proves that custom layout can also compete on latency.
`MATERIALIZED` demonstrates that repeated graph questions should sometimes be
served as data, not recomputed as algorithms.

### 6.1 BFS, Reachability, And Shortest Paths

#### Product shape

Start with a bounded access or dependency question, not a generic path catalog:

```text
from principal/service/package X
through allowed relationship types
within depth or cost bound B
return reachable targets and one proof path per target
```

#### Storage plan

- Forward CSR is the baseline for one-direction expansion.
- Reverse CSR is valuable for bidirectional source-target search and reverse
  blast radius; make it an optional capability, not an unconditional duplicate.
- Relationship type and weight live in late-materialized sidecars or typed
  channels.
- Exact block metadata can skip pages whose source IDs, destination IDs,
  relationship types, or property ranges cannot contribute.
- A landmark-distance view can accelerate A* only after representative repeat
  workloads justify build and invalidation cost.

The custom path artifact family should look approximately like:

```text
path/<projection>/<variant>/
  manifest.json
  forward.blocks
  forward.source-index
  reverse.blocks                 # SPEED/bidirectional capability
  reverse.destination-index
  relationship-type.bitmap
  weight.column                  # Dijkstra/A* capability
  block-membership.index         # conservative frontier pruning
  landmarks.distances            # optional MATERIALIZED accelerator
```

The block ordering should be chosen for the first ICP's typed, bounded
traversals. A generic all-relationship CSR is the control, not necessarily the
shipping representation.

#### State plan

- Reachability: visited bitset plus sparse or dense frontier.
- BFS distance: `u32[V]` only if the result needs distances.
- Proof path: predecessor state can dominate; allow bounded target sets or spill
  predecessor slabs.
- Dijkstra: priority queue is data- and exploration-dependent; admission needs a
  conservative bound or a spillable queue.
- Result enumeration can exceed compute state. Result cardinality and chunking
  are part of admission.

#### Modes

- `FIT`: state and bounded result fit reserved memory.
- `SPILL`: distance/predecessor/queue slabs or result chunks spill under a disk
  budget.
- `APPROXIMATE`: only if semantics explicitly change, such as landmark-only
  candidate paths or a declared result cap.
- `REFUSE`: all-pairs, unbounded path enumeration, or an output bound larger
  than the declared memory/disk envelope.

### 6.2 WCC

#### Why it is second

WCC is the best global verification kernel. Its output is a partition, so
implementation-specific component IDs can be canonicalized before comparison.
It also exposes the distinction between topology bytes and `O(V)` state cleanly.

#### Storage plan

- Prefer an undirected edge stream that stores each canonical pair once for a
  union-find or streaming-union plan.
- Retain symmetric CSR only when repeated adjacency-based execution or other
  kernels repay the second orientation.
- A precomputed component-label artifact can make repeated queries tiny, but it
  cannot replace the first-run proof and must be generation-bound.

The custom WCC artifact family should look approximately like:

```text
wcc/<projection>/<variant>/
  manifest.json
  undirected-pairs.runs          # each normalized pair once
  source-range.index
  range-boundary.edges
  union-order.schedule           # optional ordering chosen from degree stats
  label.checkpoint               # STRICT-RAM restart state
  components.column              # MATERIALIZED answer
  spanning-forest.edges          # reusable certificate
```

The speed variant may additionally compile symmetric adjacency for Afforest or
label propagation. The RAM-first variant should avoid paying for that duplicate
topology.

#### State plan

- `u32[V]` parent or label is the basic dense state.
- Frontier bitmaps or changed-label sets control repeated passes.
- If the label array itself exceeds the budget, use range-aligned state capsules
  only after an exact external-union design is demonstrated; otherwise refuse.

#### Verification

- adversarial fixtures: isolates, self-loops, parallel edges, two giant
  components, high-degree stars, non-dense source IDs;
- randomized differential tests;
- canonicalized partition equality;
- estimator error and cgroup peak under fast and strict lanes.

### 6.3 PageRank

#### Storage plan

- Pull PageRank prefers reverse adjacency/CSC and an exact out-degree or
  reciprocal-degree vector.
- A top-K-only result reduces output but not the iterative rank-state floor.
- A prior exact vector can be a warm start for the same semantics and a nearby
  generation; convergence still decides correctness.
- Personalized PageRank is a different workload and may prefer sparse residual
  methods. Do not route it through the global plan by name alone.

The custom PageRank artifact family should look approximately like:

```text
pagerank/<projection>/<variant>/
  manifest.json
  reverse-by-destination.blocks
  destination-range.index
  outdegree.u32
  reciprocal-degree.f32
  dangling.bitmap
  hot-hub.block-list
  previous-rank.vector           # optional exact warm-start seed
  ranks.f32                      # MATERIALIZED answer
  topk.index
```

The strict artifact should add a 2D source-by-destination tile index. For each
destination window, it loads bounded source-rank slabs and matching edge tiles,
accumulates a bounded destination slab, and writes the next rank slab. This
costs extra rank and tile traffic but makes exact state residency explicit.

#### State plan

- two `f32[V]` vectors are the basic ping-pong state;
- some variants add residual, dangling mass, personalization, or active-set
  state;
- stream-all output has an unavoidable serialization floor;
- strict plans must not count mapped topology as free memory.

#### Latency truth

PageRank can be faster than GDS if Knight Walker reads fewer bytes, avoids a
projection build, uses a pull-contiguous representation, reduces synchronization,
or reuses a valid warm start. It is not faster merely because Rust replaces
Java. An out-of-core strict plan may be slower than a warm in-memory GDS run and
still be the correct product when GDS cannot run on the available machine.

### 6.4 NodeSimilarity And kNN

#### Storage plan

- Prefer exact entity-feature incidence when the logical graph originates from
  a bipartite relation. Do not eagerly expand a feature bucket into a clique.
- Use sorted postings and degree bands for exact intersections.
- Store compact sketches or LSH buckets as candidate-generation views, never as
  silent replacements for exact scores.

The custom similarity artifact family should look approximately like:

```text
similarity/<projection>/<variant>/
  manifest.json
  feature-to-entity.postings
  entity-to-feature.postings
  feature-degree.histogram
  heavy-feature.partitions
  degree-band.index
  sketch.deck                    # approximate candidate capability
  lsh.bucket-index               # approximate candidate capability
  exact-rerank.requests
  topk-neighbors.column          # MATERIALIZED answer
```

For user-item, account-device, package-symbol, and principal-permission graphs,
the incidence representation is often the exact graph-shaped storage. Building
a pair graph first would destroy the primary RAM advantage.

#### State plan

- candidate count, not input edges alone, is the dangerous dimension;
- degree histograms and heavy-feature statistics must enter the estimate;
- exact candidates can be partitioned into sorted spill runs;
- maintain only bounded top-K heaps per active node or partition;
- exact all-pairs similarity may be correctly refused.

#### Approximation contract

The approximate receipt should state sketch type, seed, width, candidate count,
exact rerank count, measured recall@K on a calibration sample, and any nodes
excluded by policy.

### 6.5 Louvain And Leiden

#### Storage plan

- weighted undirected adjacency with stable aggregation semantics;
- process one coarsened level at a time;
- publish or spill level graphs as temporary generation-bound runs instead of
  retaining every level;
- warm-start community IDs only when generation/config identity permits it.

The custom community artifact family should look approximately like:

```text
community/<projection>/<variant>/
  manifest.json
  weighted-undirected.blocks
  vertex-weighted-degree.column
  community-range.index
  community-edge.runs            # exact external aggregation
  level-000.graph
  level-001.graph
  hierarchy.manifest
  membership.column              # MATERIALIZED answer
  modularity.receipt
```

Only the current level needs to be active. Old level graphs are evidence or
restart artifacts, not automatically resident state. A strict plan may
repeatedly sort/reduce community-edge runs to cap tally memory.

#### State plan

The dynamic map from neighboring community to accumulated weight can dominate
both topology and fixed vectors. A smaller edge encoding cannot guarantee a
small tally map. The executor must choose among:

- bounded hash/table arena with overflow spill;
- sort-and-reduce community-edge runs;
- exact refusal when a safe bound cannot be established;
- explicitly approximate capped/sampled state.

#### Verification

Community IDs and even partitions can vary across valid implementations. The
oracle packet should include seed behavior, modularity, partition validity,
coverage, deterministic fixtures with known optima, and accepted tolerance. A
raw array equality test is not sufficient.

### 6.6 Triangle Count And LCC

#### Storage plan

- sort adjacency;
- orient each undirected edge by `(degree, node_id)` or degeneracy order;
- store only forward oriented lists for the motif view;
- intersect forward lists so each triangle is counted once;
- add dense-hub bit pages only when intersection benchmarks justify them.

The custom triangle artifact family should look approximately like:

```text
triangles/<projection>/<variant>/
  manifest.json
  orientation.rank               # degree or degeneracy order
  forward-oriented.offsets
  forward-oriented.neighbors
  hub-bitpages/
  intersection.block-index
  triangle-count.column          # MATERIALIZED per-node answer
  lcc.column
```

This is intentionally not the same adjacency as PageRank or paths. Direction is
an exact counting schedule: it removes duplicate work while preserving every
triangle.

#### State plan

Global count needs tiny state. Per-node triangle/LCC results need a counter per
node and possibly degree data. Output choice therefore changes admission even
when compute topology is identical.

### 6.7 FastRP And Embeddings

#### Storage plan

- sparse normalized operator blocks over the canonical topology;
- late-materialized numeric features;
- dimension-major or row-slab output according to the kernel;
- result sidecar keyed by generation, config, seed, precision, and dimension.

The custom FastRP artifact family should look approximately like:

```text
fastrp/<projection>/<variant>/
  manifest.json
  normalized-operator.blocks
  operator.range-index
  numeric-feature.handles
  random-seed.manifest
  dimension-block.schedule
  matrix-input.slabs
  matrix-output.slabs
  embeddings.matrix              # MATERIALIZED answer
  vector-index/                  # optional serving accelerator
```

Dimension blocking is a major custom-storage lever. It trades repeated topology
passes for a deterministic matrix-state cap. The quote should show that curve,
for example 8, 16, 32, or 128 dimensions per state block.

#### State plan

Embedding matrices are the bill. At `V=200M`, one 128-dimensional `f32` matrix
is `102.4 GB` decimal before topology or a second buffer. Even one `int8` matrix
is `25.6 GB`. Therefore an 8 GB promise requires exact blockwise semantics,
output spill, lower dimension/precision, or refusal. Quantization is an
approximation unless the public procedure's semantics already permit it.

## 7. Jeff Dean Lens: Count Bytes And Movements First

### 7.1 A Concrete Scale Model

Use `V = 200,000,000` vertices and `E = 1,000,000,000` directed edges as a
shared arithmetic example. Decimal GB is used first; GiB is shown where useful.

| Structure | Formula | Decimal bytes | Approx GiB |
| --- | ---: | ---: | ---: |
| One `u32` or `f32` vertex vector | `4V` | 0.8 GB | 0.745 GiB |
| One `u64` vertex vector | `8V` | 1.6 GB | 1.49 GiB |
| Visited bitset | `V/8` | 25 MB | 0.023 GiB |
| One CSR neighbor array | `4E` | 4.0 GB | 3.73 GiB |
| One CSR offset array (`u64`) | `8(V+1)` | 1.6 GB | 1.49 GiB |
| One full CSR orientation | `4E + 8(V+1)` | 5.6 GB | 5.22 GiB |
| Dual CSR orientations | `8E + 16(V+1)` | 11.2 GB | 10.43 GiB |
| PageRank two `f32` ranks + `u32` degree | `12V` | 2.4 GB | 2.24 GiB |
| WCC `u32` labels | `4V` | 0.8 GB | 0.745 GiB |
| Per-node triangle `u64` counter | `8V` | 1.6 GB | 1.49 GiB |
| FastRP 128-dimension `f32` matrix | `512V` | 102.4 GB | 95.37 GiB |

These are lower-order representation terms, not whole-process estimates. Add
ID maps, properties, alignment, buffers, queues, result materialization,
allocator overhead, threads, code, spill metadata, retained generations, and a
safety margin.

### 7.2 Per-Family Working-Set Consequences

| Family | Important lower bound at the example scale | Systems consequence |
| --- | --- | --- |
| Bounded reachability | bitsets can be tens of MB; distance/predecessor each add 0.8 GB | query result and proof-path requirements can dominate a tiny frontier |
| WCC | labels alone about 0.8 GB | a strict edge stream plus labels can fit far below dual-CSR residency |
| PageRank | rank/degree state about 2.4 GB plus one useful topology orientation | 8 GB is plausible only with disciplined topology residency and buffers; it is not automatic |
| NodeSimilarity | `sketch_bytes * V` plus candidate state | even 64 bytes/node is 12.8 GB; sketch and candidate partitioning are mandatory at this scale |
| Louvain | fixed vectors plus unbounded-looking local tally working set and level graphs | admission depends on degree/community statistics and spill design, not only `V` and `E` |
| Triangles | oriented topology plus intersections and optional 1.6 GB per-node output | orientation saves work; result mode changes the bill |
| FastRP | matrix bytes scale as `V * dimension * precision` | dimensions/precision/output slabs are first-class budget parameters |

### 7.3 Latency Floors

1. **Storage bandwidth:** a 5.6 GB sequential pass cannot be faster than bytes
   divided by achieved device or memory bandwidth.
2. **Iteration count:** PageRank and community algorithms multiply the pass
   bill by iterations/levels.
3. **Output:** returning 200M rows has a serialization and transport floor even
   if compute were free.
4. **Random access:** individual DRAM latency is not multiplied serially when
   hardware exposes memory-level parallelism. Use measured bandwidth and queue
   depth, not `lookups x 80 ns` folklore.
5. **Decode:** compression wins latency only when avoided I/O/cache misses exceed
   decode and dispatch cost.
6. **Coordination:** atomics, barriers, and high-degree contention can dominate
   a bandwidth-efficient layout.
7. **Preparation:** a specialized view may accelerate the kernel while making
   first answer slower. Build cost belongs in the comparison.

### 7.4 Hardware-Aware Rules

- Use large sequential extents for global scan families.
- Separate one-pass traffic from reusable hot blocks.
- Double-buffer predictable reads in strict mode.
- Pin small degree/offset metadata only when every iteration uses it.
- Avoid duplicate decompression by assigning block ownership.
- Keep state slabs aligned to topology ranges for NUMA and spill locality.
- Use `io_uring` only after the block schedule exposes useful queue depth.
- Measure NVMe bandwidth, page faults, cache state, NUMA placement, and CPU
  utilization; do not infer them from language choice.

## 8. Shreyas Doshi Lens: Solve The Right Job With LNO Discipline

### 8.1 Strategy Kernel

**Diagnosis:** Teams with graph-shaped security/dependency questions can face
memory uncertainty, late failure, result explosion, or excessive platform
ceremony. Existing systems validate the need with estimates and controls, so
estimation alone is not differentiated. The size and willingness-to-pay of the
bounded local segment remain unproven.

**Guiding policy:** Win one high-stakes artifact-to-answer job by making resource
behavior enforceable and auditable on hardware the user already controls.

**Coherent actions:** Build traversal plus WCC through one bounded execution
spine, compare against incumbents, put receipts in a design partner's hands,
and let measured demand choose the next algorithm and integration.

### 8.2 LNO Allocation

| Class | Work | Why |
| --- | --- | --- |
| Leverage | One exact security/dependency traversal | Directly tests the first customer job. |
| Leverage | Hard-budget planner and enforcement | The surviving differentiation. |
| Leverage | WCC differential oracle | Makes correctness and memory claims falsifiable. |
| Leverage | Reconciled machine-verifiable receipt | Builds trust and estimator calibration. |
| Leverage | Five real artifacts/interviews and one paid-pilot ask | Tests behavior and dollars, the weakest evidence legs. |
| Leverage | Competitor shootout with Neo4j GDS, Ladybug, Slater/Grafeo where runnable | Prevents an obsolete whitespace story. |
| Neutral | Simple CSV/Parquet/Arrow/OpenGraph adapters | Necessary adoption work, but not the product moat. |
| Neutral | Immutable generation/checksum catalog | Required reliability substrate. |
| Neutral | CLI, progress, cancellation, bounded output | Product hygiene that supports the core job. |
| Overhead | Full Neo4j OLTP/Bolt/Cypher rewrite | Does not test the first paid behavior. |
| Overhead | Registering hundreds of GDS procedures | Breadth before the execution spine is proven. |
| Overhead | New universal graph format | Standardization before repeat workload evidence. |
| Overhead | Cells, graph grammar, general schedule language | High-complexity architecture without a measured trigger. |
| Overhead | Seven algorithms before customer signal | Converts learning into catalog production. |

### 8.3 What The User Must Feel

The winning experience is not "a smaller graph engine." It is:

```text
I gave it the artifact and the machine limit.
It told me what would happen before doing expensive work.
It did not exceed the limit.
It either finished, degraded exactly as approved, or refused early.
I can prove which data, plan, and answer produced the result.
I trust the next run more than the previous one.
```

### 8.4 Product Falsifiers

Stop or materially change the strategy if:

- five target users cannot provide a job they avoid, overprovision, or distrust;
- the artifact/integration step, not execution, is consistently the dominant
  pain and Knight Walker cannot own it cheaply;
- users treat a safe refusal as no better than the current product;
- the receipt does not change a run, machine, incident, or purchasing decision;
- Ladybug, Slater, Grafeo, or another maintained product already satisfies the
  same job with comparable enforcement and ergonomics;
- strict execution cannot stay within its declared cgroup ceiling;
- first-quarter view/build complexity exceeds the value of direct execution;
- or the paid signal is for a different job, such as ingestion or interactive
  path serving rather than batch algorithms.

## 9. Contradiction Ledger

| Topic | Earlier claim | Later evidence/correction | Resolution for A007 |
| --- | --- | --- | --- |
| Product scope | Rewrite all of Neo4j in Rust | A007 defines a portable artifact runner | Full rewrite is not quarter-one or first-product scope. |
| Novelty | Nobody estimates before run | GDS, Aura, Neptune and others estimate | Differentiate on enforceable full-working-set contract and reconciliation. |
| Competitive turf | Out-of-core field is empty | Ladybug, Slater, Grafeo, Onager and ancestors exist | Compare directly; claim a narrower job and contract. |
| Kuzu | Archived/dead and in-memory analytics | Ladybug is the live successor and scans projected data from disk, with state caveats | Treat Ladybug as a current competitor and benchmark target. |
| Distribution | Neo4j plugin is the only uncontested wedge | A007 evidence favors graph-artifact security/dependency users first | Plugin is an adapter/design-partner scenario, not product identity. |
| First algorithm | PageRank proves the engine | Security/access-path evidence points to traversal; WCC is easiest oracle | Traversal first, WCC second, PageRank third. |
| Layout | GRAIN hot/warm/cold is the architecture | State-heavy algorithms and query shapes need different plans | Compile custom OLAP storage for every supported family; earn additional speed/RAM/materialized variants experimentally. |
| RAM improvement | 150x-900x versus Neo4j | v2 first-principles work separates whole-machine baseline from necessary analytics bytes | Publish workload-specific measured ratios only. |
| Random access | `N x 80 ns` serialized makes a huge latency floor | Memory-level parallelism makes many scans bandwidth-bound | Use measured bandwidth/queue depth, not serialized arithmetic. |
| Strict RAM | Low heap or `mmap` equals low RAM | Page cache, RSS, direct buffers, output, and retained generations matter | Strict lane requires bounded I/O plus cgroup evidence. |
| Pareto | Seven families are about 85% of use | No representative public telemetry | Use ordinal family hypothesis and collect workload manifests. |
| Coverage | Seven algorithms cover graph use cases | DAG, filtered retrieval, temporal generations, catalogs and pipelines are outside them | Add workload gates later; do not add algorithms reflexively. |
| Speed | Rust plus `io_uring` should make everything faster | Many kernels are bandwidth/state/output bound | Win by eliminating movement/work; use language/runtime improvements as secondary gains. |
| Receipt | Receipt alone is a product | Existing estimates and guards are common; willingness to pay is unproven | Receipt is part of an enforced outcome and must be tested behaviorally. |
| Market size | Low-RAM segment is structurally large/growing | Neo4j staff says few paying customers requested out-of-core; public dollar evidence is thin | Treat segment size as a six-week falsifiable hypothesis. |
| Incumbent response | Neo4j cannot move toward cost efficiency | InfiniGraph shows movement on independent storage/compute and scale-out | Avoid static-moat claims; win speed of learning and a sharper local job. |
| Sharding | Neo4j cannot shard | InfiniGraph makes old complaint stale | Remove from current positioning. |
| GraphRAG | Leiden discards about 10% of entities | Later audit found a query-layer NaN filtering bug with different magnitude | Do not use old claim; GraphRAG remains a secondary hypothesis. |
| OLTP complaints | Neo4j query slowness is the opportunity | A007 is OLAP/artifact compute and cannot fix OLTP | State the exclusion clearly. |

## 10. Candidate Product/Architecture Scenarios

Scores are current strategic judgments from 1 (weak) to 5 (strong).

| Scenario | Customer learning | Technical proof | Time to evidence | Reversibility | Competitive clarity | Score |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A. Portable A007 bounded runner | 5 | 5 | 5 | 5 | 5 | 25 |
| B. Neo4j plugin/sidecar first | 4 | 4 | 3 | 4 | 3 | 18 |
| C. Extend/contribute to Slater, Grafeo, or Ladybug | 3 | 4 | 4 | 3 | 2 | 16 |
| D. Full Neo4j-compatible Rust rewrite | 1 | 2 | 1 | 2 | 1 | 7 |
| E. GRAIN/read-shape format foundry first | 2 | 4 | 2 | 3 | 2 | 13 |

### Scenario A: Portable A007 Runner - Recommended

Accept a simple artifact, run one security/dependency traversal plus WCC under
a hard budget, and emit a receipt. This maximizes learning per week and leaves
Neo4j, BloodHound/OpenGraph, code graphs, Parquet, and Arrow as adapters.

Primary risk: the target customer may value ingestion or interactive serving
more than bounded batch compute. That is exactly what the first six weeks test.

### Scenario B: Neo4j Plugin Or Sidecar

Use `gtm-POC-01.md` as the design: Java procedure shim, chunked FFI or process
boundary, stable ID mapping, Rust-owned planning and memory. This puts the proof
near a known pain trigger and uses GDS as an oracle.

Use when a real design partner's graph is already trapped inside Neo4j. Do not
make JNI, full GDS naming, or plugin deployment the universal architecture.

### Scenario C: Existing Engine Collaboration

Benchmark and possibly contribute admission, total-working-set estimation, and
receipts to Slater, Grafeo, or Ladybug. This can validate whether the contract is
valuable independent of Knight Walker's storage code.

Use as a strategic probe. The risk is that Knight Walker becomes a feature or
contribution without a distinct customer relationship.

### Scenario D: Full Rewrite

Reimplement Bolt, Cypher, OLTP records, WAL, locks, indexes, procedures, GDS,
models, and operations. This is a multi-year platform program with late product
feedback. Rust may improve footprint and tail variance, but architectural parity
also imports Neo4j's major representation choices.

Keep as a long-term option only after the bounded runner has demand, repeated
artifacts, and a reason to own OLTP.

### Scenario E: Format Foundry

Build relationship channels, GRAIN codecs, cells, content-addressed generations,
and a schedule compiler before the first customer job. This has high research
upside and low immediate product learning.

Keep these mechanisms behind benchmark gates. Do not let a clever format become
the product definition.

## 11. Verification-First Development Loop

### 11.1 The Reusable Loop

```text
1. Freeze request semantics and output mode.
2. Build tiny adversarial fixtures and an independent oracle.
3. Normalize graph identity and output comparison.
4. Write the memory formula and rejection cases.
5. Observe the RED failure in correctness and/or enforcement.
6. Implement the minimum plan that can pass.
7. Run randomized differential and metamorphic tests.
8. Run Linux cgroup cold/warm memory and latency trials.
9. Reconcile estimate, reservation, RSS/cgroup peak, I/O, and result checksum.
10. Publish the immutable evidence bundle or preserve the failure.
```

### 11.2 Oracle Rules By Family

| Family | Correctness equivalence |
| --- | --- |
| Reachability/BFS | Same reachable node set under the same filters and depth; path edges valid; shortest unweighted distance equal. |
| Weighted shortest path | Same optimal cost within numeric tolerance; every path edge exists and sums to reported cost. |
| WCC | Same partition after canonicalizing component IDs; isolate and self-loop semantics explicit. |
| PageRank | Same configuration/seed, convergence and normalization invariants, per-node tolerance, and top-K tie policy. |
| NodeSimilarity | Same candidate semantics, score formula, cutoff, top-K and tie handling; approximate mode reports recall. |
| Louvain/Leiden | Valid full partition, modularity/tolerance, seed behavior, hierarchy and coverage; do not require arbitrary community IDs to match. |
| Triangles/LCC | Exact global/per-node counts and denominator semantics. |
| FastRP | Same seed/config and numeric tolerance where implementation permits; otherwise declare cross-engine quality invariants rather than false bit parity. |

### 11.3 Required Test Families

- semantic fixtures;
- randomized differential tests;
- metamorphic tests such as ID permutation and edge-order permutation;
- corrupted artifact and checksum tests;
- insufficient-memory and insufficient-disk tests;
- bounded-output and result-explosion tests;
- cancellation and timeout tests;
- estimator overflow and arithmetic-boundary tests;
- strict cgroup peak tests;
- cold-cache and warm-cache performance tests;
- repeated-generation and stale-view tests;
- crash-safe receipt/publication tests;
- competitor reproduction tests.

### 11.4 Evidence Bundle

```text
evidence/<run-id>/
  request.json
  artifact-manifest.json
  plan.json
  estimate-receipt.json
  completed-receipt.json
  oracle-result.json
  normalized-output.digest
  cgroup-memory.events
  process-metrics.jsonl
  io-metrics.json
  environment.json
  stdout.log
  stderr.log
```

Summaries and charts should be generated from these files. Failed and refused
runs remain in the corpus.

## 12. Ninety-Day Plan

### Month 1: Prove The Customer Job And Execution Contract

#### Week 1: Freeze A007's Smallest Product

- Obtain or construct one security/dependency artifact and one codegraph demo.
- Define the bounded reachability request and result schema.
- Define artifact identity, exactness, memory/disk/time budgets, and result caps.
- Run five problem interviews using one real job, not a concept pitch.
- Write the traversal and WCC verification packets before kernel work.

Exit gate: one target user can state the current failure/overprovision decision
and supplies an artifact or a structurally faithful fixture.

#### Week 2: Build The Canonical Generation And Oracle Harness

- Ingest one simple source format.
- Publish stable dense IDs, checksums, forward topology, minimal properties, and
  statistics.
- Implement independent tiny-graph oracles and output normalization.
- Record RED tests for insufficient budgets and corrupted artifacts.

Exit gate: the same logical graph produces a stable checksum and reproducible
oracle output across edge order and source-ID permutations.

#### Week 3: Build Admission, Reservations, And Receipts

- Implement total-working-set formula schema.
- Add fixed memory reservations, bounded result sink, cancellation, and refusal.
- Separate fast `mmap` and strict fixed-buffer plans.
- Persist estimate receipt before topology execution begins.

Exit gate: a fake kernel can fit, spill, refuse, cancel, and reconcile without
allocating outside its declared large-memory owners.

#### Week 4: Make Bounded Traversal Exact

- Compile a custom `BALANCED` path artifact with source-range blocks, type
  indexes, and conservative block-membership metadata.
- Compile a custom `STRICT-RAM` path plan with fixed readers and spillable
  distance/predecessor state.
- Implement forward expansion with sparse/dense frontier choice.
- Add relationship filters and optional weighted sidecar.
- Add demand-paged execution and bounded proof-path output.
- Differential-test on fixtures and randomized graphs.

Month 1 gate: a real high-stakes query runs through the common plan and stays
within its strict budget, or produces an honest early refusal.

### Month 2: Prove Global Execution And Buyer Value

#### Week 5: Make WCC Exact

- Compile the custom pair-once undirected edge-run artifact.
- Implement the `BALANCED` edge-stream/union plan behind the same executor.
- Quote a `SPEED` symmetric-CSR alternative and a `STRICT-RAM` state-capsule
  alternative, even if only one is implemented in this week.
- Canonicalize partitions in the verifier.
- Add formula, refusal, concurrency, and result-stream tests.
- Compare against GDS and GAPBS or another independent implementation.

Exit gate: exact partition parity and reconciled memory receipt.

#### Week 6: Run The First Competitive And Customer Test

- Compare with Neo4j GDS, Ladybug, Slater/Grafeo where runnable, and a local
  library baseline.
- Separate import/build, projection/view build, algorithm, and result time.
- Demonstrate both a finish and a refusal.
- Ask for a paid pilot or explicit rejection reason.

Kill/continue gate: continue only if the contract changes a user's run,
machine, operational, or purchasing decision, or the rejection identifies a
specific adjacent job worth owning.

#### Week 7: Harden Generations And View Identity

- Pin artifact/view identity to generation and projection hash.
- Add stale-view rejection, retention, and crash-safe publication.
- Ensure receipts cannot be attached to the wrong graph or configuration.

Exit gate: crashes expose old or new generation, never mixed files.

#### Week 8: Add The Narrowest Needed Adapter

Choose from evidence:

- BloodHound/OpenGraph artifact adapter;
- Neo4j export/plugin adapter;
- Parquet/Arrow adapter;
- codegraph bundle adapter.

Do not build all four. Build the one blocking the strongest design partner.

Month 2 gate: one externally meaningful artifact-to-answer workflow is
repeatable by someone other than the author.

### Month 3: Test Generality Without Building A Catalog

#### Week 9: Add Global PageRank

- Compile a custom pull artifact: reverse-by-destination blocks, reciprocal
  degree column, dangling bitmap, and destination-range index.
- Implement two-vector pull plan and bounded output modes.
- Quote resident, compressed-topology, 2D strict-RAM, and materialized-rank
  choices from the same generation.
- Measure first-run versus warm-start and top-K versus stream-all.
- Publish first-principles floors beside measured results.

Exit gate: no speed claim excludes projection/view build, output, or cache state.

#### Week 10: Attack The Architecture With NodeSimilarity

- Measure exact candidate explosion from real degree/feature histograms.
- Compile exact feature-to-entity postings and degree-band metadata rather than
  reusing the PageRank/path topology.
- Compare exact bucketed spill with sketch candidate generation plus exact
  rerank.
- Verify top-K ties and approximation recall.
- Refuse all-pairs when the bound is not credible.

Exit gate: the common admission contract survives a state-heavy family, even if
the correct outcome is refusal.

#### Week 11: Run Cross-Profile Storage Experiments

- Compare `SPEED`, `BALANCED`, `STRICT-RAM`, and `MATERIALIZED` quotes wherever
  the family has enough compiled variants.
- Compare generic canonical execution only as the control.
- Compare forward-only versus custom reverse/pull artifacts.
- Test exact page-index skipping on the first filtered path workload.
- Test plain pages versus one succinct codec only if physical I/O is proven
  dominant.

Promotion gate for a dual win:

- at least 20% lower whole-process/cgroup peak;
- at least 10% lower median and p95 wall time;
- same semantics/tolerance;
- build time, disk amplification, and retained bytes included;
- reproduction on at least two graph distributions.

#### Week 12: Run The Honest Scale Ladder

- tiny, generated, medium, and target-scale artifacts;
- cold and warm cache;
- fast and strict modes;
- finish, spill, approximate, and refuse cases;
- competitor baselines;
- estimator calibration plots generated from receipts.

#### Week 13: Close The Quarter

Choose exactly one next bet from evidence:

- deepen security traversal and interactive serving;
- add Louvain if GraphRAG/fraud demand is real;
- add Neo4j/Bolt integration if export is the blocking pain;
- promote a read-shaped view if repeated-run economics are proven;
- contribute the contract to an existing engine if storage differentiation is
  weak;
- or stop if the paid behavior hypothesis failed.

Day-90 success is not "seven algorithms" or "Neo4j rewritten." It is:

```text
one externally meaningful graph job
+ exact or honestly approximate answer
+ enforced resource outcome
+ reproducible competitor comparison
+ calibrated receipt corpus
+ paid-pilot signal or a decisive falsifier
```

## 13. Architecture Decisions To Record

| ADR | Decision | Revisit trigger |
| --- | --- | --- |
| A007-ADR-001 | Portable artifact runner is the first product | Repeated interviews prove integration inside a database is the actual job. |
| A007-ADR-002 | Traversal plus WCC is the first proof pair | First design partner has a different high-stakes job with a stronger oracle. |
| A007-ADR-003 | Simple canonical generation before new codecs | Measured topology I/O, not state/output/integration, dominates two target workloads. |
| A007-ADR-004 | Derived views are evictable and generation-bound | Never; this protects logical truth from cache architecture. |
| A007-ADR-005 | Fast and strict lanes have separate claims | A single lane proves both latency and hard-RSS behavior across targets. |
| A007-ADR-006 | Exact, approximate, and refuse are distinct public outcomes | Never; silently degrading violates the product contract. |
| A007-ADR-007 | Whole-process cgroup/RSS is the strict memory oracle | A more reliable platform enforcement primitive replaces it. |
| A007-ADR-008 | Plugin/Bolt is an adapter, not the core | Paid demand specifically requires a server-compatible replacement. |
| A007-ADR-009 | Every release claim is backed by an evidence bundle | Never; the product's central promise is auditability. |
| A007-ADR-010 | Do not add the next algorithm until the execution spine is green | A customer-paid requirement justifies an explicit exception. |

## 14. Final Recommendation

The correct architecture is a **proof-carrying bounded graph runner with a
small canonical rebuild substrate and a portfolio of custom OLAP storage plans
for every supported algorithm family**.

The correct first product is a **security/dependency artifact-to-answer job**,
not a Neo4j replacement.

The correct first technical sequence is:

```text
artifact identity
  -> traversal oracle
  -> full-working-set admission
  -> strict enforcement
  -> bounded traversal
  -> WCC parity
  -> reconciled receipt
  -> customer/competitor test
  -> PageRank
  -> state-heavy falsifier
```

The product earns the right to become a broader graph engine only after this
loop changes real behavior. Until then, every additional protocol, algorithm,
codec, view, or compatibility surface must answer one question:

> Does this make A007's first high-stakes graph job more trustworthy, more
> usable, or faster to validate with a real customer?

If the answer is no, it is not quarter-one work.

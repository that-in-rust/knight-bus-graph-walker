# Sol-01: Verification-First Architecture And A 90-Day Build Plan

<!-- markdownlint-disable MD013 -->

Date: 2026-07-10

Status: proposed architecture and execution strategy

Source scope: the architecture and PRD notes in `docs_PRD04/`, the evidence
subfolders that remain under `docs_PRD03/`, the current Rust implementation,
and the local reference repositories cited by those notes. The three research
questions added on 2026-07-10 include targeted internet verification using
current official documentation and primary systems/production papers. Their
URLs and the limits of what each source establishes are recorded inline.

This document records evidence, conclusions, assumptions, tradeoffs, and
verification gates. It does not expose private chain-of-thought.

## Answer First

The next three months should not be spent trying to implement Neo4j, Bolt,
Cypher, OLTP storage, 575 GDS entries, a novel compressed format, and seven
algorithm families at once.

The strongest next move is a **proof-carrying OLAP vertical slice**:

1. Keep the current flat immutable dual-CSR snapshot as the topology oracle.
2. Keep stock Neo4j temporarily as the OLTP host and Neo4j GDS as the semantic
   oracle. This is a delivery bridge, not the final product architecture.
3. Implement one exact algorithm, WCC, through a common plan, budget,
   reservation, execution, streaming, and receipt path.
4. Make every support claim executable: same input generation, normalized
   result parity, deterministic memory admission, measured peak reconciliation,
   and a persisted evidence bundle.
5. Add atomic generation publication before adding a second serious algorithm.
6. Expose the slice through a stock-Neo4j `grain.*` plugin for side-by-side
   testing, while keeping the Rust core independent of JNI and Neo4j.
7. Use the last month to prove the result under an 8 GB Linux cgroup and to run
   one architecture-breaking spike, preferably NodeSimilarity candidate state.

The 90-day product is therefore not "Neo4j rewritten." It is something more
useful and more honest:

```text
An outsider can run one graph through Neo4j GDS and Knight Bus,
observe the same WCC partition, inspect the exact snapshot watermark,
see the estimated and measured memory bill, and reproduce the evidence.
```

That artifact becomes the verification spine for the eventual rewrite. Bolt,
Cypher, a Rust OLTP record store, more GDS procedures, new snapshot codecs, and
cells can attach to it later without changing what "correct and low-RAM" means.

Two evidence envelopes remain deliberately separate:

```text
semantic/integration proof = stock Neo4j + GDS + grain plugin, with the whole
                             combined process reported

strict-memory proof        = standalone Rust engine under an 8 GB Linux
                             cgroup, with Neo4j absent from the measured run
```

The first proves the user seam and parity. The second proves the engine's RAM
behavior. Neither number may be presented as the other.

```text
                         TEMPORARY 90-DAY BRIDGE

  stock Neo4j OLTP ------------------------------------+
      | committed export / receipt                     |
      v                                                |
  Projection Build Adapter                             | semantic oracle
      |                                                v
      v                                          stock Neo4j GDS
  immutable snapshot generation W                      |
      |                                                | normalized result
      v                                                v
  plan -> estimate -> reserve -> WCC -> stream -> compare
      |                                      |
      +-------------- execution receipt -----+
                         |
                         v
              reproducible evidence bundle

                         DURABLE DESTINATION

  Rust OLTP -> Build Store -> immutable snapshots -> budgeted kernels
       ^                                                |
       +---------- Bolt / Cypher / gds.* adapters ------+
```

## Premise Check

The premise is sound: `docs_PRD04/` plus the evidence subfolders under
`docs_PRD03/` contain enough architecture research, source evidence,
public-surface inventory, oracle planning, and current-code knowledge to begin
implementation.

Five corrections are necessary before acting on it.

### Correction 1: Documentation completion is not implementation readiness

The reference-learning program is complete for its declared study scope, but
the implementation packet explicitly says that production Rust was not added.
Its tracker currently contains six `Verified` documentation artifacts and six
`Stubbed` artifacts. The distinction must remain visible in code and CI.

Source:

- [reference-learning/README.md](../docs_PRD03/reference-learning/README.md)
- [implementation-readiness/README.md](../docs_PRD03/implementation-readiness/README.md)
- [V003-Implementation-Readiness-Tracker.tsv](../docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv)

### Correction 2: A support tier is not an implementation state

The 575-row readiness registry labels 62 rows
`P1-ImplementedExactLowRam`, but the current Rust dispatcher implements only a
small catalog/property subset and rejects `gds.pageRank.stream`. This is not a
small naming issue. It can cause agents, dashboards, or generated tests to
mistake a target tier for observed reality.

The replacement schema must have independent fields:

```text
target_support_tier: P0 | P1 | P2 | out_of_scope
implementation_state: inventoried | specified | red | green | verified | shipped
verification_state: none | fixture | oracle | memory | integration | benchmark
evidence_revision: git commit or immutable artifact id
```

No procedure is "implemented" because a research TSV says so. Runtime code and
passing evidence gates promote the implementation state.

Sources:

- [GDS-Procedure-Support-Registry.tsv](../docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv)
- [src/gds.rs](../src/gds.rs)
- [src/gds/execution.rs](../src/gds/execution.rs)
- [tests/gds_registry_contract.rs](../tests/gds_registry_contract.rs)

### Correction 3: Low heap is not strict RAM

The current mmap runtime is an excellent fast path and a proven topology
oracle. It is not, by itself, a deterministic 8 GB guarantee because page-cache
residency is controlled partly by the operating system. Strict mode needs:

- an explicit execution budget;
- engine-owned reservations for all controlled allocations;
- bounded read buffers or a platform-specific direct-I/O policy;
- spill accounting;
- result-stream accounting;
- Linux cgroup measurement of the whole process;
- rejection before execution when the plan cannot fit.

Sources:

- [prd-l1.md](../docs_PRD04/prd-l1.md)
- [Current-Codebase-Low-RAM-Patterns.md](../docs_PRD03/reference-learning/Current-Codebase-Low-RAM-Patterns.md)
- [Memory-Estimate-Formula-Book.tsv](../docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv)

### Correction 4: GRAIN is a promising hypothesis, not the first milestone

The predictability-first GRAIN idea is valuable, especially the manifest,
capability, and access-plan concepts. Its central claim, that state-heavy
families such as NodeSimilarity and Louvain can be priced accurately from
manifest polynomials, remains explicitly unproven. Three codecs, degree
ranking, Elias-Fano, and 2D blocks are too much uncertainty to place before the
first end-to-end algorithm receipt.

For 90 days, keep the manifest extensible but use the current flat CSR payload.
Let GRAIN codecs earn adoption through benchmark results later.

Sources:

- [Arch05.md](../docs_PRD04/Arch05.md)
- [Arch06.md](../docs_PRD04/Arch06.md)
- [Cells-Adoption-Falsifier-Plan.md](../docs_PRD03/implementation-readiness/Cells-Adoption-Falsifier-Plan.md)

### Correction 5: A sidecar proof is not yet a Neo4j rewrite

A `grain.wcc.stream` plugin proves the OLAP thesis and creates an unusually
strong differential harness. It does not prove Bolt compatibility, Cypher
compatibility, transactional correctness, WAL recovery, or zero-change
`gds.*` compatibility. The project should call it what it is:

```text
OLAP replacement proof: yes
Neo4j-compatible Rust database: not yet
```

That narrower claim is a strength. It keeps the first proof falsifiable.

Source: [gtm-POC-01.md](../docs_PRD04/gtm-POC-01.md)

## Evidence Baseline

### What exists now

The current branch is not an empty greenfield project.

| Current asset | Direct observation | Architectural implication |
| --- | --- | --- |
| Rust implementation | About 7,423 source lines and 1,785 test lines under `src/` and `tests/` | Extend the existing proof unless a bounded audit demonstrates structural failure. |
| Test baseline | `cargo test --all-targets` passes 51 tests on 2026-07-10 | Preserve this suite as the topology and catalog regression floor. |
| Low-RAM builder | Bounded external sort, spill runs, and k-way merge exist in `src/low_ram.rs` | Build-budget plumbing is prior art inside this repo, not a new invention. |
| Snapshot runtime | Immutable dual CSR, forward/reverse adjacency, sidecars, and mmap cursors exist | This is the canonical topology oracle and the initial fast execution lane. |
| Manifest | Snapshot v3 carries generation metadata, source transaction range, orientations, and sidecar catalog | Extend this contract; do not replace it before evidence requires a new format. |
| GDS inventory | 575 entries are loaded into the Rust registry from a source-derived TSV | Surface breadth is known and deterministic unsupported behavior is possible. |
| Executable GDS subset | Fifteen entry keys are explicitly promoted by `built_in_gds_support_status_now`; these are catalog/property operations, not graph algorithms | The next meaningful promotion is one real algorithm family, not more metadata aliases. |
| Algorithm kernels | No WCC, PageRank, Louvain, NodeSimilarity, Dijkstra, FastRP, or triangle kernel is present in `src/` | Documents describing these as ready are planning evidence, not implementation evidence. |
| Publication protocol | A written state machine exists; production publication and crash tests do not | Atomic generations are a Month 2 implementation gate. |
| Compatibility | Canary matrix exists; Bolt/Cypher/driver canaries are not implemented | Full Neo4j compatibility must remain outside the 90-day product claim. |

### What the research establishes strongly

| Claim | Evidence strength | Consequence |
| --- | --- | --- |
| OLTP, build/control, and OLAP serving must remain separate | Direct PRD constraint | No query-time reconciliation and no user reads from the Build Store. |
| Flat CSR alone does not constitute GDS support | Direct GDS source study | Catalog, typed properties, artifacts, modes, estimates, and result semantics are first-class. |
| One canonical topology plus support planes covers more of GDS than many durable per-algorithm formats | Strong source-backed inference | Keep durable topology simple; make execution plans bespoke. |
| WCC has a clean partition-parity oracle and bounded per-node state | Direct GDS test and estimator evidence | WCC is the best first verification slice. |
| Louvain and similarity families pressure runtime workspace more than topology | Direct source study plus inference | Budgeting and spill are architectural, not optional optimization. |
| Cells can help locality/publication but are not proven as the default | Explicit falsifier plan | Do not build cells during the core 90-day path. |
| GDS estimates memory already | Direct source evidence | Differentiation is enforceable completion/rejection and reconciliation, not merely estimation. |

### What remains uncertain

| Uncertainty | Why it matters | Fastest falsifier |
| --- | --- | --- |
| Can the current runtime accept a budget token without broad surgery? | Decides brownfield extension versus a contained new execution core | Thread a budget through one traversal in Week 1; kill extension if it implies more than one month of rework. |
| Can strict mode stay inside an 8 GB whole-process cap? | This is the product thesis | Run under Linux cgroup v2 with cold and warm cache states; measure `memory.current`, peak, spill, and engine reservations. |
| Is JNI/plugin transport operationally boring? | Determines whether the proof can run inside stock Neo4j | `grain.ping` plus a direct-buffer degree stream in Week 1. |
| Does WCC finish on the target scale with only O(V) resident state? | First exact low-RAM algorithm claim | Scale ladder ending in a 50 GB-class logical graph under an 8 GB cgroup. |
| Can NodeSimilarity candidate state be bounded without destroying exactness or usefulness? | Prices the hardest architecture class | Month 3 bucketed exact spill spike; sketches are a separate approximate mode and cannot silently substitute. |

## Expert Lenses

### Graph storage and algorithm lens

The current flat dual CSR is already the right initial substrate. WCC needs
global edge scans and per-node component state; it does not need a new durable
topology format. The first architecture work belongs above the bytes: graph
capabilities, workspaces, budget reservations, cancellation, progress, result
streaming, and receipts.

### Neo4j compatibility lens

Compatibility is several contracts, not one label: Bolt, PackStream/values,
Cypher, transactions, procedures/GDS, and client behavior. A stock-Neo4j
plugin isolates the OLAP experiment from those contracts while preserving a
real user-facing procedure path. It must not be advertised as completion of
the compatibility objective.

### Verification and reliability lens

Tests should not merely assert an algorithm result. The unit of evidence is an
execution envelope containing input identity, plan identity, budget verdict,
result normalization, oracle comparison, and measured resource use. Snapshot
publication needs crash-point tests before the engine creates artifacts that
users may depend on.

### Product and sequencing lens

The fastest route to a meaningful artifact is not the fastest route to more
code. One exact, externally reproducible WCC swap validates more of the thesis
than ten unintegrated kernels or a Bolt handshake over toy storage. It proves
the user path, graph extraction, ID mapping, storage read path, algorithm,
result stream, parity rule, estimate, enforcement, and benchmark story.

### Skeptical engineering lens

The leading proposal has four obvious failure modes:

1. It may benchmark only a friendly graph whose vertex state happens to fit.
2. mmap may move memory outside the heap while the cgroup still OOMs.
3. WCC may be too easy to generalize to Louvain or NodeSimilarity.
4. A `grain.*` procedure may create the illusion of zero-change GDS support.

These objections materially change the plan:

- target-scale cgroup evidence is a release gate, not a stretch chart;
- strict and fast execution lanes are explicit and separately named;
- Month 3 includes an architecture-breaking family spike;
- the release is named an OLAP replacement proof, not a Neo4j rewrite;
- every public table separates observed implementation state from intended
  support tier.

## Candidate Architectures

The conventional approach and four alternatives are compared below. The
non-obvious alternatives deliberately blend graph-database engineering with
other disciplines because the documents show that the hard problem is not
only data layout.

### Architecture A: Breadth-First Neo4j Rewrite

Conventional approach: create Rust subsystems for record storage, WAL, locks,
indexes, Bolt, PackStream, Cypher, procedures, snapshots, and algorithms in
parallel, then integrate them.

Why it is attractive:

- it resembles the final product diagram;
- every week produces visible subsystem progress;
- it avoids a temporary Java bridge.

Why it is wrong for the next 90 days:

- each subsystem has a different oracle and failure model;
- integration arrives late;
- the RAM thesis receives little attention;
- a solo or small team can produce broad stubs while proving no user outcome;
- transaction/storage work can consume the quarter before one OLAP run exists.

Keep as: long-term program map, not first-quarter execution strategy.

### Architecture B: Compatibility Shell

Blend: database rewrite with protocol conformance testing.

Build Bolt, PackStream, basic Cypher, and generated GDS signatures first. Use
an in-memory or toy store behind them until storage and algorithms arrive.

Advantage: official drivers connect early and canaries are satisfying.

Failure mode: a polished shell around an engine that cannot finish the
workload motivating the rewrite. The passing compatibility suite can become a
local optimization that delays the low-RAM substrate.

Keep as: a bounded side spike and later parallel track. A `HELLO/RESET/RETURN
1` canary is useful; a quarter of compatibility work is not.

### Architecture C: GRAIN Format Foundry

Blend: graph storage with compiler IR design and self-describing executable
formats.

Design a manifest whose fields price each access plan, then add hot flat CSR,
warm Elias-Fano, and cold 2D blocks. The Projection Build Store compiles one
watermark into those artifacts.

Advantage: powerful long-term shape, portfolio-ready manifest, and a possible
format-level differentiation.

Failure mode: codec and estimator research delays the first externally
verified algorithm. The state-heavy formula thesis may fail after the format
has already consumed a quarter.

Keep as: manifest vocabulary and experimental branch. Delay new codecs until
the current flat payload produces one reconciled receipt.

### Architecture D: Algorithm Factory

Blend: graph engine development with a product-line factory.

Implement WCC, BFS, PageRank, Dijkstra, triangles, FastRP, and Louvain rapidly
against current adjacency. Add common budgeting after the family set exists.

Advantage: fast demos and broad algorithm learning.

Failure mode: every family grows a bespoke workspace and error path. The
budget abstraction is retrofitted exactly when the first difficult benchmark
arrives. This creates two execution models: easy algorithms and honest ones.

Keep as: an explicit anti-pattern. One kernel must prove the common execution
contract before a second kernel uses it.

### Architecture E: Proof-Carrying OLAP Strangler

Blend 1, safety engineering: every supported operation ships with a small
"safety case" made of executable evidence.

Blend 2, scientific instruments: Neo4j GDS is the calibration reference;
Knight Bus results are normalized and compared before claims are promoted.

Blend 3, logistics: execution has three lanes selected before departure:

```text
FAST    -> mmap + resident workspace when the complete plan fits
STRICT  -> bounded buffers + spill + whole-process budget enforcement
REJECT  -> deterministic bill and reason before graph bytes are touched
```

Blend 4, compiler architecture: the public procedure request is compiled into
an explicit access plan over a generation manifest. The kernel executes the
plan; it does not choose storage policy ad hoc.

Advantage: it directly proves the differentiating claim, reuses current code,
and creates stable seams for future adapters and formats.

Risk: it may look narrower than a rewrite. The remedy is not broader scope; it
is a clear evolution map showing which temporary adapters are replaced later.

### Architecture F: Read-Shape Foundry

Blend 1, streaming storage: borrow Apache Iggy's separation between sealed
immutable segments, sidecar indexes, configurable index residency, and a fixed
memory pool. Translate a stream partition into a graph **relationship channel**
partitioned by relationship type, orientation, and source-ID range.

Blend 2, database views: a named GDS graph begins as a logical projection, not
as another copied CSR. Its durable identity is:

```text
projection = generation
           + node selector
           + relationship selector
           + orientation
           + property column handles
           + semantic/configuration hash
```

Blend 3, compiler architecture: compile `(projection, procedure, graph stats,
budget, hardware class)` into one physical read shape:

```text
direct masked channel scan
or ephemeral compact CSR
or factorized incidence execution
or generation-published algorithm artifact
```

Blend 4, real-time scheduling: the plan owns a fixed buffer pool and declares
its future block order. It can bypass cache for one-pass blocks, pin blocks that
will be revisited, prefetch the next block, and co-locate the matching algorithm
state range with each edge block.

The resulting durable shape is:

```text
Neo4j-shaped OLTP
        |
Projection Build Store
        |
sealed relationship channels + exact block metadata
        |
immutable generation manifest
        |
logical projection algebra
        |
read-shape compiler
   |             |                  |
direct view   ephemeral CSR   factorized/artifact view
        \          |          /
       plan-owned buffers + co-sharded state
                       |
             exact result + receipt + lineage
```

Why it could be better than GRAIN alone:

- GRAIN optimizes the encoding of a graph after materialization; F first asks
  whether the projected graph needs materialization at all.
- A filtered projection can avoid reading unrelated relationship channels and
  properties instead of copying selected edges into another graph.
- Factorized incidence can preserve `Account -> Device <- Account` without
  expanding it into an account-account clique.
- Exact block metadata can prove that an inactive block cannot contribute to
  the current frontier, allowing it to be skipped without approximation.
- Algorithm state can be opened in the same ranges as topology rather than
  keeping every `O(V)` vector resident.

Why it can fail:

- channel fragmentation can create tiny files and file-descriptor pressure;
- a schedule compiler can become a research project before one workload wins;
- compression and metadata can slow the flat whole-graph scan that CSR already
  serves well;
- differential or factorized execution is not valid for every procedure;
- extra physical artifacts can quietly become an unbounded disk portfolio.

Keep as: the durable post-proof storage and planning destination. Architecture
E remains the 90-day delivery strategy. E's `GraphView`, `AccessPlan`, budget,
receipt, and parity seams must be capable of admitting F later, but the first
WCC proof must not wait for relationship channels or a schedule compiler.

### Comparison

Scores are strategic judgments, not measured facts. `5` is strongest.

| Architecture | 90-day end-to-end proof | Directly tests RAM thesis | Reuses current proof | Reversible | Scope control | Main regret risk |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| A. Breadth-first rewrite | 1 | 2 | 2 | 2 | 1 | A quarter of components with no falsifiable product result |
| B. Compatibility shell | 3 | 1 | 3 | 3 | 2 | Drivers connect to a hollow engine |
| C. GRAIN foundry | 2 | 3 | 3 | 4 | 2 | Elegant format before proven execution |
| D. Algorithm factory | 3 | 3 | 4 | 3 | 2 | Budget and spill retrofit after family-specific debt |
| E. Proof-carrying strangler | 5 | 5 | 5 | 5 | 5 | Narrow proof mistaken for full compatibility |
| F. Read-shape foundry | 3 | 5 | 4 | 4 | 2 | Planner/index complexity before representative workloads exist |

## Chosen Thesis

Choose Architecture E for the first 90 days and keep its durable seams
compatible with Architecture F. Use four pieces from the other paths:

- B contributes a tiny protocol/plugin canary, not the quarter's main work.
- C contributes an extensible generation manifest and declared capabilities,
  not new codecs yet.
- D contributes WCC as the first real kernel and NodeSimilarity as the first
  architecture-breaking spike, not seven-family breadth.
- F contributes logical projections, future channel-backed `GraphView`
  implementations, and plan-owned buffers, but no new serving format before
  the flat-CSR WCC proof establishes the measurement baseline.

The key distinction is between the **durable product architecture** and the
**temporary delivery architecture**.

### Durable product architecture

```text
Neo4j-compatible adapters
  Bolt | Cypher | procedure ABI | official-driver behavior
                         |
                         v
Contract Registry -> Planner -> Budget Admission -> Executor -> Result Stream
                         |              |               |
                         v              v               v
                 Generation Catalog  Reservations   Evidence Receipt
                         |              |               |
                         +------ GraphView -------------+
                                   |
                      published immutable snapshot W
                                   ^
                                   |
Rust OLTP truth -> receipts -> Projection Build Store -> validate/publish
```

### Temporary 90-day delivery architecture

```text
stock Neo4j = OLTP host
stock GDS   = semantic and estimate oracle
grain plugin = procedure adapter
CSV/export bridge = temporary committed-fact adapter

Everything below SourceGraphAdapter and ProcedureAdapter is durable Rust.
```

The temporary components must be behind explicit ports so replacing them does
not disturb algorithm correctness or memory evidence.

## Research Question 1: Are The Seven Algorithm Families Enough?

Research date: 2026-07-10. Method: compare the claims in `Arch02.md`,
`Arch04.md`, `simulation01.md`, and `AlgoExplainers-ASCII.md` with current
official Neo4j documentation, another graph vendor's use-case documentation,
Microsoft GraphRAG documentation, and primary production papers.

### Answer first

The seven are useful, but the current wording overstates what is known.

They are seven **algorithm families**, not seven graph shapes and not seven
business use cases. They are a good initial workload portfolio because they
exercise global scans, iterative state, pair-candidate explosion, paths,
embeddings, and set intersections. They are not enough to claim either the
complete Neo4j GDS surface or the complete market for graph workloads.

The `~20%`, `~15%`, and cumulative `~85%` figures are not confirmed usage
telemetry. `Arch02.md` correctly says Neo4j publishes no procedure-invocation
ranking. These percentages must remain explicitly labeled **modeled adoption
weights**. They may guide experiment ordering, but they cannot support a market
share, compatibility, or product-completeness claim.

### Claim audit

| Existing claim | Truth-seeking verdict | Confidence | Required correction |
| --- | --- | --- | --- |
| "These are the seven top graph use cases" | Incorrect category: they are algorithm families | High | Separate business use cases, query workloads, and algorithm families |
| "About 85% of GDS adoption uses them" | Plausible prioritization hypothesis; not publicly verifiable | Low-medium | Keep ordinal priority; remove any implication of measured telemetry |
| "They are enough to stress the storage architecture" | Mostly true for static batch analytics | Medium-high | Add filtered retrieval, temporal change, DAG, and artifact-lifecycle probes |
| "They cover the current GDS algorithm surface" | False | High | Current GDS separately exposes DAG and topological link-prediction categories, Pregel, and ML pipelines |
| "They cover the major graph business uses" | False | High | Major uses also depend on pattern matching, k-hop retrieval, reasoning, dynamic updates, and operational traversals |
| "One physical graph layout can serve all seven well" | Unsupported and unlikely | High | Treat them as a portfolio of access signatures compiled from one logical generation |

### Primary and official source ledger

These links verify categories and mechanisms, not the unobservable adoption
percentages:

| Source | What it establishes | What it does not establish |
| --- | --- | --- |
| [Neo4j GDS graph algorithms](https://neo4j.com/docs/graph-data-science/current/algorithms/) | Current categories include centrality, community detection, similarity, path finding, DAG algorithms, node embeddings, topological link prediction, and Pregel | Relative usage or revenue by category |
| [Neo4j GDS machine learning](https://neo4j.com/docs/graph-data-science/current/machine-learning/machine-learning/) | Node classification, link prediction, and node regression are end-to-end pipeline surfaces with model artifacts | That these pipelines fit inside the seven static kernel families |
| [Neo4j DAG algorithms](https://neo4j.com/docs/graph-data-science/current/algorithms/dag/dag-algorithms/) | Topological sort and longest path are explicit dependency-graph workloads | Their adoption share |
| [Neo4j topological link prediction](https://neo4j.com/docs/graph-data-science/current/algorithms/linkprediction/) | Common Neighbors, Adamic Adar, Resource Allocation, Preferential Attachment, and related scores are a separate category | That NodeSimilarity is a complete substitute |
| [Neo4j use cases](https://neo4j.com/use-cases/) | Neo4j markets AI systems, GenAI, fraud, supply chain, recommendations, IAM, compliance, network/IT operations, knowledge graphs, pattern matching, digital twins, and metadata management | Which algorithms dominate those uses |
| [Amazon Neptune graph applications](https://docs.aws.amazon.com/neptune/latest/userguide/graph-get-started.html) | Independent vendor documentation names knowledge, identity, fraud, social, routing, logistics, diagnostics, science, regulation, and network/security graphs | That one vendor's taxonomy is complete |
| [Microsoft GraphRAG indexing overview](https://microsoft.github.io/graphrag/index/overview/) | GraphRAG extracts entities and relationships, detects communities, generates summaries, and embeds text | That Leiden alone is the GraphRAG workload |
| [Twitter Who-To-Follow paper](https://web.stanford.edu/~rezab/papers/wtf_overview.pdf) | A production recommendation system used personalized graph ranking and SALSA-style computation | General market share |
| [Pinterest Pixie paper](https://arxiv.org/abs/1711.07601) | Production recommendation used random-walk-style retrieval over a multi-billion-scale graph | That batch PageRank alone serves online recommendation |
| [PinSage paper](https://arxiv.org/abs/1806.01973) | Production-scale recommendation combines graph sampling, learned embeddings, and downstream serving | That FastRP covers graph ML generally |

### Business use cases versus computational workloads

The market/use-case taxonomy and the engine workload taxonomy intersect, but
they are not interchangeable:

| Business use case | Seven-family coverage | Important work outside the seven |
| --- | --- | --- |
| Fraud, AML, and entity resolution | Strong: WCC, community, centrality, similarity, paths, triangles | Time-windowed motifs, circular-flow patterns, streaming updates, investigator k-hop queries, merge/split correction |
| Recommendations and personalization | Strong: similarity, embeddings, ranking | Random walks, candidate sampling, link-prediction pipelines, low-latency neighborhood retrieval, feature/model lifecycle |
| Knowledge graphs and GraphRAG | Partial: community, centrality, embeddings | Typed pattern joins, k-hop retrieval, provenance, source citations, hybrid vector+graph retrieval, continuous re-indexing |
| Supply chain and dependency analysis | Partial: paths and components | DAG topological order, SCC/cycle detection, impact reachability, longest path, scenario views, flow/capacity constraints |
| Network, IT operations, IAM, and security | Partial: paths, components, centrality | Temporal event correlation, authorization path proofs, pattern matching, attack-path enumeration, incremental topology |
| Customer 360 and master data | Strong initial WCC/similarity fit | Explainable evidence paths, incremental entity merge/split, typed property joins, survivorship rules |
| Life sciences and drug discovery | Partial: similarity, community, paths, embeddings | Heterogeneous typed subgraphs, motif/substructure search, knowledge reasoning, provenance-heavy joins |
| Social and influence graphs | Strong static coverage | Temporal diffusion, feed retrieval, online random walks, evolving-community analysis |

### What is actually missing

Do not respond by adding twenty more algorithms to the first release. Add four
**workload gates** that expose architectural blind spots:

1. **Filtered pattern/retrieval gate.** A typed, property-filtered two-to-four
   hop query with high selectivity. This tests relationship channels, property
   late materialization, and whether an OLAP projection can avoid a full copy.
2. **DAG/dependency gate.** Topological sort plus cycle/SCC detection over a
   directed dependency graph. This tests directionality and ordered execution
   that WCC cannot represent.
3. **Temporal-generation gate.** Apply a small exact edge delta, publish W+1,
   and decide whether to reuse or recompute. This tests the freshness problem
   already identified by `Arch04.md`.
4. **Artifact-pipeline gate.** Produce a feature, train or register a model-like
   artifact, and bind it to graph generation/configuration identity. This tests
   the catalog and lifecycle surface that a kernel-only roadmap misses.

The revised verification portfolio is therefore **seven algorithm families
plus four cross-cutting gates**, not "eleven top algorithms":

```text
seven families
  = static analytical access and workspace signatures

four gates
  = selective query + directed dependency + change over time + artifact life
```

### Product and architecture consequence

- Retain WCC, Louvain/Leiden, PageRank, NodeSimilarity/KNN, paths, FastRP, and
  triangles as the first algorithm workload portfolio.
- Stop displaying the percentages as facts. Label them `modeled_priority` and
  attach source/confidence/falsifier fields.
- Do not use the seven to claim Neo4j surface parity. Surface parity also needs
  catalogs, modes, projections, DAG/link prediction, pipelines, and result
  semantics.
- Design storage around computational signatures, not around a presumed fixed
  market ranking.
- Collect opt-in anonymous procedure counters or customer workload manifests
  before replacing modeled priorities with measured ones.

### Falsifier

This conclusion should be revisited if Neo4j publishes representative
procedure telemetry, or if five target users provide workload manifests in
which a different class dominates. Until then the seven are a disciplined MVP
hypothesis, not discovered ground truth.

## Research Question 2: Can Exact Storage Reduce RAM While Preserving The Neo4j Surface?

### Answer first

Yes, because the Neo4j-compatible surface and the OLAP physical representation
do not have to be the same thing. Bolt, Cypher-facing procedure signatures,
catalog identity, modes, and result semantics can remain Neo4j-shaped while the
OLAP engine compiles each named projection into one or more exact physical
datasets.

The largest RAM win is not a cleverer integer codec. It is **representation
elimination**:

- do not copy a projection that can remain a logical view;
- do not create pair edges when an incidence factor is enough;
- do not keep both orientations resident when one can be streamed;
- do not load properties an algorithm never reads;
- do not retain all generations' duplicate blocks;
- do not keep the full algorithm state resident when ranges can be processed
  and spilled exactly.

Some options below trade latency for a smaller hard memory envelope. That is
acceptable in this section; the next research question isolates dual wins.

### Exactness contract

Every physical dataset must prove that it represents the same logical graph
view. "Compressed" or "factorized" is not permission to change semantics.

For projection `P`, publish:

```text
P.identity = hash(
  source_generation,
  node_selector,
  relationship_selector,
  orientation,
  aggregation_rules,
  property_types,
  default/null semantics
)

P.edge_truth  = normalized edge-stream checksum
P.node_truth  = normalized node-id checksum
P.prop_truth  = checksum per typed property column
P.capability  = exact operations this physical dataset supports
```

An alternative representation is accepted only when its decoded canonical
stream matches the oracle projection and its algorithm output matches stock GDS
under the procedure's documented ordering/tolerance rules. Approximate sketches
may estimate cost or guide prefetch; they may never silently justify dropping
an edge from an exact plan.

### Storage option A: Sealed relationship channels

Store immutable edge facts in moderate-sized channels keyed approximately by:

```text
relationship type x orientation x source-ID range
```

Each sealed channel contains contiguous edge payload plus small sidecars:

```text
channel-00421/
  edges.bin
  source.offsets
  source.membership
  destination.membership
  property-zone-map
  checksum
```

Rare relationship types share overflow channels; labels remain masks rather
than exploding into one file per label combination. The generation manifest
publishes only sealed channels. This adapts the ordered sealed-segment, sidecar
index, and configurable-residency ideas documented by the
[Apache Iggy storage engine](https://iggy.apache.org/docs/server/storage-engine/).

RAM mechanism: a filtered graph opens only selected channels and bounded
indexes. Cost: an unfiltered global scan may open more extents than flat CSR.

### Storage option B: Virtual projection manifests

A named graph initially stores handles, masks, and semantics rather than copied
topology:

```text
fraud_graph@W = {
  generation: W,
  node_mask: labels(Account),
  channels: [TRANSFER, USES_DEVICE],
  orientation: NATURAL,
  properties: [amount, timestamp],
  projection_hash: ...
}
```

The planner chooses among:

- execute directly through masks and channels;
- build a bounded ephemeral CSR when selectivity makes dense IDs valuable;
- publish a reusable CSR when repeated execution repays build and disk cost.

RAM mechanism: avoid simultaneous OLTP representation + source projection +
algorithm copy. Cost: masks add branches and broad views may scan irrelevant
records unless channels and page indexes are selective enough.

### Storage option C: Succinct adjacency pages

Encode adjacency pages independently using delta gaps, reference similarity,
Elias-Fano offsets, or run encodings while retaining random access boundaries.
Google's [Zuckerli](https://research.google/pubs/zuckerli-a-new-compressed-representation-for-graphs/)
demonstrates that large graphs can remain directly addressable without full
decompression; the source reports smaller representations than WebGraph on its
tested graphs with comparable decompression resource use.

RAM mechanism: fewer topology bytes enter buffers or page cache. Cost: decode
CPU and graph-order sensitivity. Keep plain `u32` pages as a codec, not as a
separate architecture.

### Storage option D: Factorized incidence and hyperedge datasets

Many "derived graphs" are lossy expansions of a smaller exact relation:

```text
expanded similarity graph        factorized incidence

Account A ---- Account B         Account A -- Device X
Account A ---- Account C                    -- Account B
Account B ---- Account C                    -- Account C
```

For entity resolution, recommendation, and fraud, preserve
`entity -> feature <- entity` and teach kernels to consume the factor. WCC can
union entities through a factor; NodeSimilarity can intersect factor lists;
candidate generation can remain bucketed without ever materializing the full
entity-entity graph.

This is supported as a mechanism by factorized query processing in
[Kuzu's CIDR paper](https://vldb.org/cidrdb/papers/2023/p48-jin.pdf) and by the
set-oriented layouts and joins in
[EmptyHeaded](https://arxiv.org/abs/1503.02368). Neither source proves a Knight
Bus GDS kernel; that remains a parity and benchmark task.

RAM mechanism: avoid potentially quadratic intermediate edges and repeated
factor values. Cost: only applicable when projection semantics can be expressed
through retained factors; arbitrary user-supplied projected edges fall back to
ordinary adjacency.

### Storage option E: Orientation-on-demand exact topology

The canonical generation can retain one orientation plus a compact source
index and build/stream the transpose into bounded runs for pull algorithms.
Alternatively, publish reverse orientation only for projections whose observed
plans justify it.

RAM/disk mechanism: remove the unconditional second edge array. Cost: PageRank
and other pull-heavy workloads pay transpose or additional indirection latency.
This is a strict-mode option, not an assumed default.

### Storage option F: Co-sharded state capsules

Partition algorithm state by exactly the same dense-ID ranges as topology:

```text
range-00042.edges
range-00042.labels
range-00042.frontier
range-00042.result
```

Open a topology range and its state capsule, process it, persist the dirty
state, then release both. GraphChi's
[parallel sliding windows](https://www.usenix.org/conference/osdi12/technical-sessions/presentation/kyrola)
is evidence that organizing external-memory graph computation around aligned
shards can make large exact computations practical on small machines.

RAM mechanism: resident state becomes `O(window)` instead of always `O(V)`.
Cost: extra passes, synchronization, and writes; use only when in-core vectors
fail admission.

### Storage option G: Content-addressed generations

Name immutable blocks by content hash and let generation manifests reference
them:

```text
W   = [a, b, c, d]
W+1 = [a, b, e, d]
```

Only `e` is new. This mainly reduces disk and build pressure, but also reduces
cache churn because unchanged blocks retain stable identities across
generations. Garbage collection is manifest-reachability plus reader pins.

RAM mechanism: fewer duplicated indexes and build buffers during publication.
Cost: reference accounting, compaction, and adversarial small-block metadata.

### Storage option H: Typed late-materialized property plane

Keep topology, node properties, and relationship properties physically
separate. Encode each property with its own null bitmap, dictionary or fixed
width values, and block statistics. An algorithm opens only declared columns.

RAM mechanism: WCC does not touch `amount`, PageRank does not deserialize text,
and a path plan reads only its weight column. Cost: a procedure needing several
properties may perform more coordinated reads. Property defaults, numeric
coercions, missing values, and aggregation semantics must be parity-tested.

### One logical graph, multiple exact OLAP datasets

The foundry should not generate all layouts eagerly. It should publish the
canonical fact representation, then promote exact artifacts under a disk and
build budget:

| Dataset | Physical shape | Primary families | Exact information preserved |
| --- | --- | --- | --- |
| `topology_scan` | Forward channels or flat CSR | WCC, FastRP, global push | Every selected edge and endpoint |
| `rank_pull` | Reverse channels + exact out-degree vector | PageRank-class | Every reverse edge and normalization degree |
| `community_undirected` | Canonical undirected pair once + weight | WCC, Louvain/Leiden | Undirected multiplicity/aggregation semantics |
| `similarity_incidence` | Feature-to-entity inverted lists | NodeSimilarity/KNN, ER | Original bipartite relation; no synthetic clique |
| `path_weighted` | Type-filtered adjacency + exact weight columns | BFS, Dijkstra, A* | Direction and every weight value |
| `motif_intersection` | Degree-oriented sorted lists; optional dense hub pages | Triangles/LCC | Edge existence with an orientation rule that counts once |
| `embedding_operator` | Sparse normalized blocks + feature handles | FastRP/propagation | Exact selected topology and declared numeric inputs |

Every artifact carries `source_generation`, `projection_hash`, `config_hash`,
`canonical_stream_checksum`, byte size, build receipt, and capabilities. It can
be evicted without losing truth because the canonical generation can rebuild
it.

### Low-RAM option scorecard

Scores are hypotheses to prioritize experiments, not measured results. `5` is
best; latency-cost `5` means the largest likely slowdown.

| Option | Peak-RAM potential | Latency cost | Breadth | Complexity | First falsifier |
| --- | ---: | ---: | ---: | ---: | --- |
| Relationship channels | 4 | 2 | 5 | 3 | filtered projection reads materially fewer bytes than flat CSR |
| Virtual projections | 5 | 2 | 5 | 4 | direct view beats projection copy at representative selectivity |
| Succinct pages | 4 | 3 | 4 | 3 | decode does not erase I/O savings |
| Factorized incidence | 5 | 2 | 2 | 4 | exact NodeSimilarity/WCC avoids pair-edge explosion |
| Orientation on demand | 3 | 5 | 3 | 2 | saved reverse bytes justify PageRank penalty |
| State capsules | 5 | 5 | 4 | 5 | strict job completes under cap with tolerable write amplification |
| Content-addressed generations | 3 | 1 | 5 | 3 | small update reuses most blocks without metadata blowup |
| Late property plane | 4 | 1 | 5 | 3 | property-heavy and property-free plans both remain efficient |

### Recommendation for the low-RAM lane

Use virtual projections over sealed relationship channels as the default
research direction. Add a typed property plane and content-addressed immutable
blocks as orthogonal capabilities. Keep succinct codecs and state capsules as
admission-selected strict plans. Add factorized incidence only for a concrete
fraud/recommendation projection that proves expansion is the dominant bill.

Do not replace flat dual CSR yet. It remains the oracle, global-scan baseline,
and fallback when a specialized representation cannot prove a win.

## Research Question 3: Can Exact Storage Reduce Both RAM And Latency?

### Answer first

Sometimes, but only when the design **eliminates work or movement**. Merely
compressing the same work exchanges I/O for CPU and may not reduce latency.
The credible dual-win mechanisms are:

1. avoid building or copying a representation;
2. prove that blocks cannot contribute and skip them;
3. preserve a smaller factor instead of expanding derived edges;
4. make the execution plan control buffers and future reads;
5. choose representations and traversal direction from graph statistics;
6. reuse exact unchanged work across immutable generations.

The target is not "one magic format." It is an exact physical-design compiler
that selects among multiple graph-shaped datasets while keeping one logical
projection contract.

### Dual-win option 1: Projection-free direct execution

Compile a logical projection directly into channel and mask iterators. Delay
dense-ID remapping and property materialization until a kernel demonstrates it
needs them. For one-shot or broad projections this removes both projection RAM
and projection-build latency.

Materialize an ephemeral CSR only when the estimated savings from dense IDs and
contiguous adjacency exceed its build bill:

```text
materialize when
  repeated_edge_reads * direct_view_penalty
    > build_time + build_IO + temporary_bytes_cost
```

This decision must be recorded in the execution receipt, not hidden in a cache.

### Dual-win option 2: Exact graph page indexes

Add conservative metadata to each edge/property page:

```text
source-ID range and exact/encoded membership
destination-ID range and membership
relationship-type bitmap
property min/max and null count
edge count and degree bounds
payload offset, length, codec, checksum
```

At each frontier or filter step, the plan asks whether a page can contribute.
It skips the page only when the answer is provably no. False positives cost a
read; false negatives are forbidden. This adapts the page-pruning mechanism in
the official [Apache Parquet page-index specification](https://parquet.apache.org/docs/file-format/pageindex/)
to graph frontiers and relationship filters.

RAM win: irrelevant pages never become resident. Latency win: storage and
decode work disappear. Full scans can ignore the index, preserving the CSR-like
fallback.

### Dual-win option 3: Codec-adaptive micro-pages

Choose a codec per page from a small closed set:

```text
flat u32          random sparse neighborhoods
delta bit-pack    locally ordered sparse neighborhoods
Elias-Fano        monotone lists with useful universe/list ratio
run container     long consecutive ranges
dense bit tile    dense hub-to-range blocks and intersections
```

Group pages by codec so execution dispatches once per run, not once per edge.
Maintain codec-specialized kernels for scan, membership, intersection, and
frontier expansion. [Zuckerli](https://research.google/pubs/zuckerli-a-new-compressed-representation-for-graphs/)
supports the feasibility of directly accessible compressed adjacency;
[Bit-GraphBLAS](https://arxiv.org/abs/2201.08560) supports dense bit tiles for
locally dense matrix blocks. These are mechanism precedents, not expected
Knight Bus benchmark results.

RAM win: fewer bytes read and retained. Latency win exists only where reduced
I/O/cache misses exceed decode and dispatch cost. Plain pages remain mandatory.

### Dual-win option 4: Plan-owned future-aware buffers

Do not delegate strict execution to an opaque OS page-cache policy. The access
plan already knows much of its future block sequence, so it can:

- bypass cache for a one-pass scan;
- double-buffer the next sequential block;
- pin a small hot index or high-degree block used every iteration;
- evict by declared next-use distance rather than generic recency;
- reserve topology, state, result, and spill buffers before graph open;
- assign one shard owner to avoid duplicate decompression and cache lines.

This combines Iggy's documented fixed memory-pool idea with graph-plan
knowledge. It makes memory deterministic and can improve latency by preventing
useful pages from being displaced by one-pass traffic.

### Dual-win option 5: Graph schedule compiler

Keep algorithm semantics independent from the physical schedule. Compile:

```text
algorithm IR
  + projection statistics
  + available physical datasets
  + memory budget
  + hardware profile
  -> direction, frontier representation, codec kernels,
     block order, fusion, prefetch, state placement, concurrency
```

The schedule choices include sparse push, dense pull, hybrid direction,
bit-vector versus sparse frontier, direct versus materialized view, and state
window size. [GraphIt](https://graphit-lang.org/) demonstrates the value of
separating an algorithm from schedules controlling traversal direction, data
layout, parallelism, and locality.

Cache a winning schedule only under a key containing algorithm/config class,
projection fingerprint, graph-statistics bucket, format version, and hardware
class. Revalidate it after meaningful distribution or hardware changes.

### Dual-win option 6: Factor-native kernels

Execute on the smallest exact relation rather than a convenient expanded
graph. Examples:

- WCC unions accounts encountered under each device factor.
- NodeSimilarity intersects device/product incidence lists and retains bounded
  top-K heaps without constructing every candidate pair.
- Recommendation propagation alternates entity and feature partitions.
- Typed GraphRAG retrieval traverses entity-document-community incidence
  without flattening every relationship into one homogeneous projection.

RAM and latency both fall when avoided expansion dominates factor traversal.
The kernel must declare exactly which projection semantics it supports and
fall back when parallel edges, weights, or aggregation rules would differ.

### Dual-win option 7: Topology-state fusion by range

For strict or NUMA plans, store state capsules in the same ID ranges as edge
pages and fuse operations while both are hot:

```text
read edge page i
read state slab i
decode -> update -> reduce -> emit dirty slab
release both
```

Avoid creating a decoded edge buffer when a codec iterator can feed the kernel
directly. Avoid separate passes for a property when its narrow typed values can
be decoded alongside selected edges. This reduces peak buffers and memory
traffic; whether it reduces wall time depends on write amplification.

### Dual-win option 8: Differential generation lineage

Record which content-addressed input blocks and configuration produced each
result chunk or acceleration artifact. On generation W+1:

1. identify changed input blocks;
2. invalidate dependent result/state chunks;
3. propagate exact changes;
4. stop only at an algorithm-specific fixed-point certificate;
5. fall back to full recomputation when the dirty region or retained state is
   estimated to cost more.

[GraphSurge](https://arxiv.org/abs/2004.05297) demonstrates declarative graph
views, shared differential computation across view collections, and adaptive
choice between differential and from-scratch execution. Knight Bus should copy
the adaptive decision, not assume incremental is always cheaper.

RAM win: avoid rebuilding unchanged state and topology. Latency win: small
deltas can touch a small region. Risk: deletions, high-centrality changes, and
global normalization can invalidate most of the graph; retained arrangements
can themselves consume too much RAM.

### Dual-win option 9: Exact certified skeletons

Publish small, evictable, generation-bound accelerators rather than another
complete topology:

| Family | Exact accelerator | How it helps without changing the answer |
| --- | --- | --- |
| WCC | spanning forest plus component labels | unchanged components can be validated/reused; changed bridges trigger repair/full fallback |
| SCC | condensation DAG | many reachability operations run over a much smaller graph |
| shortest path | landmark lower-bound distances | A* explores fewer nodes; admissible bounds preserve optimality |
| triangles | degeneracy/degree orientation | each triangle is intersected and counted once |
| PageRank | previous exact vector as initial state | fewer iterations when convergence is verified against the same tolerance |

These artifacts require source generation, algorithm configuration, checksum,
and invalidation rules. A warm start is not an exact answer; the normal
convergence or optimality certificate still decides completion.

### Per-family read shapes

This is the concrete interpretation of "storage in the shape of the read":

| Family | Preferred exact dataset | State shape | Main dual-win hypothesis |
| --- | --- | --- | --- |
| WCC | canonical undirected edge channel, each pair once | sharded parent/label slabs | half the orientation bytes plus sequential union; no full copied projection |
| Louvain/Leiden | undirected weighted adjacency grouped by current community range | community/degree slabs; ephemeral coarsened levels | coarsening levels spill and reuse buffers instead of coexisting in RAM |
| PageRank | reverse channels + exact out-degree column | two rank slabs or delta/residual slabs | pull-contiguous reads, pinned degrees, block convergence skips where sound |
| NodeSimilarity/KNN | feature-to-entity incidence | bucketed candidate/top-K state | never materialize the candidate-pair graph |
| Paths | type/weight-filtered channels + optional landmarks | sparse frontier/priority queue + windowed dist | avoid unrelated relationship/property pages; landmarks reduce expansions |
| FastRP/propagation | normalized sparse operator blocks | output matrix row blocks | fuse edge decode and blockwise vector writes; never retain full output if sink streams |
| Triangles/LCC | degree-oriented sorted lists + dense hub bit pages | counters and small intersection buffers | fewer duplicate intersections and SIMD/bitset work on dense local blocks |

### Dual-win scorecard

Scores are hypotheses. `5` means strongest expected effect or highest risk.

| Mechanism | RAM reduction | Latency reduction | Exactness ease | Engineering risk | Recommended phase |
| --- | ---: | ---: | ---: | ---: | --- |
| Projection-free execution | 5 | 4 | 4 | 3 | First |
| Exact page index | 4 | 5 for selective/frontier plans | 5 | 3 | First |
| Future-aware buffer manager | 4 | 4 | 5 | 3 | First |
| Schedule compiler | 3 | 5 | 4 | 5 | Second, with a tiny schedule vocabulary |
| Codec-adaptive pages | 4 | 3-5 by distribution | 5 | 4 | Second |
| Factor-native kernels | 5 | 5 when expansion is large | 3 | 5 | NodeSimilarity spike |
| Topology-state fusion | 4 | 3 | 4 | 4 | Strict-lane second phase |
| Differential lineage | 3-5 | 1-5 by delta | 3 | 5 | After atomic generations |
| Certified skeletons | 2-4 | 3-5 | 3 | 4 | Workload-driven only |

### Recommended composite

The best architecture is not to replace GRAIN with another fixed format. It is
to turn GRAIN into one family of physical artifacts under the Read-Shape
Foundry:

```text
canonical exact facts
  = sealed relationship channels + typed property columns

logical graph
  = generation-bound projection algebra

physical graph portfolio
  = flat CSR | succinct pages | factorized incidence
    | algorithm-shaped exact artifacts

execution
  = schedule compiler + plan-owned fixed buffers + state capsules

time
  = content-addressed generations + adaptive differential lineage

proof
  = normalized graph checksum + stock-GDS parity + memory/latency receipt
```

The first Architecture F experiment, after Architecture E's WCC execution
spine is green, should be only:

1. virtual projection descriptor over the existing flat generation;
2. exact page metadata for one relationship/property filter;
3. fixed plan-owned buffer pool in strict mode;
4. direct-view versus ephemeral-CSR planner decision;
5. WCC and one filtered path query as falsifiers.

That subset tests the central thesis without building channels, five codecs,
differential dataflow, and nine kernels at once.

### Verification experiments and promotion gates

| Experiment | Compared plans | Required evidence | Kill criterion |
| --- | --- | --- | --- |
| E1 projection elimination | copied flat CSR vs direct virtual view vs ephemeral CSR | same normalized edges/results; build bytes; first-result and total time; cgroup peak | direct view loses on both memory and latency at representative selectivity |
| E2 page skipping | scan all pages vs exact page index | skipped-page proof; physical bytes; cold/warm p50/p95 | metadata saves <10% bytes on target filtered/frontier workloads |
| E3 fixed buffers | mmap fast lane vs explicit fixed pool at 512 MiB/2 GiB/8 GiB | cgroup peak, I/O, faults, wall time, rejection behavior | budget is exceeded or slowdown is unacceptable for the strict product promise |
| E4 factorized similarity | expanded candidate graph vs incidence-native top-K | exact top-K parity including ties; candidate count; spill; peak | factor traversal does not materially reduce candidate/state bill |
| E5 generation differential | full W+1 recompute vs lineage-driven update | exact final result, invalidation trace, retained-state bill | planner cannot reliably identify when full recompute is cheaper |

A **dual-win** claim requires, on the same published generation, configuration,
hardware, and output semantics:

- at least 20% lower whole-process/cgroup peak memory;
- at least 10% lower median and p95 wall time across repeated cold and warm
  trials, with confidence intervals reported;
- no output mismatch beyond the stock procedure's documented numeric tolerance;
- build time, disk amplification, and retained artifact bytes reported beside
  execution results;
- flat CSR retained as fallback until the win reproduces on at least two graph
  distributions.

These thresholds are proposed promotion gates, not current performance claims.

### What not to do

- Do not partition physically by every label/property combination.
- Do not publish every algorithm layout for every projection.
- Do not call mmap residency "free RAM."
- Do not use Bloom filters or sketches as proof that an exact page is irrelevant
  unless the direction of error makes false negatives impossible.
- Do not quantize IDs, weights, or properties under an exact compatibility
  claim without proving procedure-level equivalence.
- Do not retain differential state when its memory bill exceeds recomputation.
- Do not let the schedule compiler choose a plan that the admission ledger has
  not reserved and the receipt cannot explain.

## Proposed Internal Architecture

### 1. Contract Registry

One generated registry owns procedure identity, mode, target tier, observed
implementation state, config schema, result schema, estimate function, oracle,
and evidence revision.

Required rule:

```text
runtime promotion is generated from verified implementation evidence;
research intent cannot promote runtime support.
```

The current inventory remains an input, not the runtime source of truth.

### 2. Generation Catalog

The catalog first resolves `(user, database, graph, generation)` to a
metadata-only `GenerationDescriptor`: validated manifest fields, dimensions,
checksums, watermarks, and capabilities. Reading this descriptor must not map
or open topology payloads. The planner uses it to estimate and select a lane.

Only after admission does the catalog pin the generation and open a
`GraphView`. A running query retains its pinned generation even if `CURRENT`
advances from W to W+1.

Minimum capabilities for the first generation format:

```text
forward_adjacency
reverse_adjacency
global_edge_stream
sorted_neighbors
dense_node_ids
source_watermark
topology_checksums
```

Properties, weights, result sidecars, cells, compressed blocks, and external
streams enter as additional capabilities, not assumptions in every kernel.

### 3. GraphView

Algorithms depend on graph semantics, not mmap internals.

Conceptual interface:

```rust
trait GraphView {
    fn dimensions(&self) -> GraphDimensions;
    fn capabilities(&self) -> GraphCapabilities;
    fn neighbors(&self, node: DenseNodeId, direction: Direction)
        -> Result<NeighborStream<'_>, GraphReadError>;
    fn edges(&self, orientation: Orientation)
        -> Result<EdgeStream<'_>, GraphReadError>;
}
```

`NeighborStream` and `EdgeStream` are adapter-neutral cursor facades in this
sketch, not aliases for the existing mmap-only cursor types. They may later be
implemented as enums or another zero-cost dispatch form after both the mmap and
strict readers exist.

The existing `GraphAdjacencyRuntime` and `MmapWalkRuntime` should satisfy the
first adapter. A future strict block reader can satisfy the same contract.

### 4. Access Plan

The planner converts a public request into a fully declared plan before
execution. Its only graph input is the metadata-only `GenerationDescriptor`.
The intended order is mechanically visible:

```text
describe generation -> construct plan -> admit/reserve -> pin/open GraphView
-> execute -> stream -> reconcile receipt -> release pin/reservations
```

For WCC, a plan states:

- graph generation and orientation;
- exact algorithm variant;
- label width and number of label arrays;
- frontier representation;
- resident bytes;
- read-buffer bytes;
- result-stream chunk bytes;
- spill allowance and spill unit;
- progress and cancellation checkpoints;
- fast, strict, or reject lane;
- parity normalizer and oracle id.

The plan, not the procedure name, is the unit of estimation and benchmarking.

### 5. Budget Ledger And Reservations

Every engine-owned allocation is charged to a reservation before it exists.
Dropping the reservation returns the bytes. An operation cannot silently fall
back to an untracked `Vec` for large state.

```text
configured process budget
- fixed runtime baseline
- pinned generation allowance
- result stream allowance
- concurrent job reservations
= bytes available to this plan
```

The budget verdict is one of:

```text
AdmitFast
AdmitStrict
RejectInsufficientBudget
RejectMissingCapability
RejectUnsupportedExactPlan
```

### 6. Kernel Contract

The common kernel boundary exists before kernel number two.

Conceptually:

```rust
trait AlgorithmKernel {
    type Config;
    type Row;

    fn plan(
        generation: &GenerationDescriptor,
        config: &Self::Config,
        budget: &MemoryBudget,
    ) -> Result<ExecutionPlan, PlanError>;

    fn execute(
        graph: &dyn GraphView,
        plan: &ExecutionPlan,
        workspace: &mut Workspace,
        output: &mut dyn ResultSink<Self::Row>,
        control: &ExecutionControl,
    ) -> Result<ExecutionStats, ExecutionError>;
}
```

Cancellation and progress belong here on day one because GDS behavior and the
reference corpus both identify them as mandatory execution hooks.

### 7. Result Sink

Results must not be materialized wholesale merely because the kernel is low
RAM. The same WCC row stream can target:

- CLI JSON/TSV chunks;
- JNI direct buffers;
- a future Bolt record stream;
- a result sidecar;
- a digest-only verifier.

Backpressure and cancellation propagate from the sink to the kernel.

### 8. Execution Receipt

The receipt is an evidence record, not marketing text.

Minimum fields:

```text
receipt_version
engine_commit
procedure_and_mode
normalized_config_hash
graph_name
snapshot_generation
source_watermark
manifest_checksum
plan_id_and_lane
exact_or_approximate
budget_bytes
estimated_controlled_bytes
reserved_peak_bytes
measured_process_peak_rss_bytes
measured_cgroup_peak_bytes
page_cache_policy
bytes_read
bytes_written
spill_bytes
result_row_count
normalized_result_digest
oracle_name_and_version
oracle_verdict
elapsed_by_phase
cancellation_or_failure_class
```

An estimate receipt may exist before execution. A completed receipt appends
measured fields and reconciliation. The estimate is never overwritten.

### 9. Oracle Harness

Correctness has three layers, matching the conclusion in Batch 11:

1. Topology parity: snapshot edges match normalized truth input.
2. Semantic parity: WCC partition matches GDS after canonicalization.
3. Resource honesty: the plan admitted or rejected correctly and reconciled
   controlled and whole-process memory measurements.

WCC canonicalization:

```text
For every component in each result:
  canonical_component_id = minimum original node id in that component

Sort by original node id and compare canonical component ids.
The diff must be empty.
```

### 10. Publication Engine

Implement the written state machine as executable filesystem semantics:

```text
staged -> validating -> published -> retired -> garbage
                    \-> failed
```

The active pointer changes only after payload fsync, validation, manifest
fsync, and parent-directory durability. A query observes W or W+1, never a
partially built generation. Failpoints exercise every transition.

## Module Evolution

Do not split immediately into a dozen crates. The current project is one small
crate, and premature workspace boundaries would slow the first proof. Add
module seams first, then extract only boundaries shared by a second adapter or
algorithm.

| Concern | Existing home | 90-day evolution |
| --- | --- | --- |
| Snapshot topology | `src/runtime.rs`, `src/snapshot.rs`, `src/types.rs` | Add graph capabilities and pinned generation handle without replacing current mmap runtime. |
| Build path | `src/low_ram.rs` | Reuse bounded sort; add private-generation staging and publication handoff. |
| Surface registry | `src/gds.rs` | Separate target tier from observed implementation and evidence state. |
| Catalog/properties | `src/gds/catalog.rs` | Bind catalog entries to real generation handles rather than only in-memory metadata. |
| Procedure dispatch | `src/gds/execution.rs` | Route WCC through planner/executor; leave unsupported rows deterministic. |
| Planning and budget | new `src/plan.rs`, `src/budget.rs` | Own plan types, reservations, lane selection, and rejection. |
| Algorithms | new `src/algorithms/wcc.rs` | First common-kernel implementation. No family-per-crate yet. |
| Evidence | new `src/receipt.rs`, `src/oracle.rs` | Persist receipts, normalize WCC, and compare oracles. |
| Publication | new `src/publication.rs` | Implement state transitions, active pointer, reader pins, retention, and failpoints. |
| Neo4j bridge | separate small Java plugin plus a narrow Rust FFI module | Keep Java/JNI out of algorithm and storage modules. |

Extraction rule after Day 90:

```text
Extract a crate only when at least two independent consumers need the boundary
or when unsafe/FFI isolation materially improves reviewability.
```

Likely first extractions are `knight-bus-kernel-api`,
`knight-bus-snapshot-format`, and `knight-bus-ffi`, but only after the second
kernel or second adapter validates the seam.

## Evidence And Verification

### Verification ladder

| Level | Question | Required evidence |
| --- | --- | --- |
| V0 Build | Does it compile and preserve the current product? | Formatting, linting, all existing tests. |
| V1 Local contract | Does each type enforce its own invariant? | Unit and property tests for plans, reservations, manifests, and errors. |
| V2 Topology | Does the runtime see exactly the published graph? | Existing adjacency parity plus corruption/truncation tests. |
| V3 Algorithm | Is WCC correct independent of Neo4j labels? | Tiny fixtures, randomized oracle, GAPBS verifier, partition normalization. |
| V4 GDS semantics | Does the public call shape and result behavior match the claimed mode? | GDS fixture extraction and stock-GDS differential test. |
| V5 Resource | Does admission prevent over-budget execution? | Tiny-budget rejection tests, reservation accounting, cgroup runs, spill evidence. |
| V6 Publication | Are generations atomic through crashes? | Failpoint matrix, restart recovery, reader pin tests. |
| V7 External workflow | Can a stranger reproduce the claim? | Plugin/CLI runbook and immutable evidence bundle from a clean machine. |

### WCC executable acceptance contract

`REQ-SOL01-WCC-001`: exact partition parity

```text
WHEN the same directed or undirected fixture is projected into GDS and a
published Knight Bus generation
THEN `grain.wcc.stream` SHALL return the same partition as `gds.wcc.stream`
after canonical component normalization
AND SHALL preserve original Neo4j node identity in the returned rows.
```

`REQ-SOL01-WCC-002`: deterministic admission

```text
WHEN the WCC plan's controlled-memory requirement exceeds the configured
available budget
THEN execution SHALL reject before opening topology payloads
AND SHALL return a stable error class and an estimate receipt.
```

`REQ-SOL01-WCC-003`: strict lane

```text
WHEN strict mode is selected
THEN edge access SHALL use bounded read buffers and declared page-cache policy
AND all engine-owned large allocations SHALL be covered by reservations
AND the completed receipt SHALL report controlled, RSS, and cgroup peaks.
```

`REQ-SOL01-WCC-004`: immutable generation

```text
WHEN publication advances from W to W+1 while WCC is running on W
THEN the running query SHALL finish on W
AND a new query SHALL open W+1
AND neither query SHALL observe files from both generations.
```

`REQ-SOL01-WCC-005`: bounded result delivery

```text
WHEN WCC returns more rows than one output chunk
THEN rows SHALL be streamed with bounded buffering
AND cancellation or downstream backpressure SHALL stop further graph work.
```

### Benchmark evidence bundle

Each benchmark run should be a directory, not a screenshot:

```text
run-<id>/
  command.txt
  engine-commit.txt
  machine.json
  cgroup.json
  dataset-manifest.json
  snapshot-manifest.json
  estimate-receipt.json
  completed-receipt.json
  oracle-result.json
  normalized-diff.tsv
  phase-metrics.json
  stdout.txt
  stderr.txt
  checksums.sha256
```

The public claim comes from these artifacts. The prose summary is derived.

### Scale ladder

Do not jump directly from a tiny test to 50 GB. Run the same command and
evidence schema at every scale.

| Rung | Purpose | Required result |
| --- | --- | --- |
| Tiny hand graph | Semantic debugging | Exact partition and explicit edge cases |
| Random property suite | Structural robustness | Hundreds of generated graphs against a simple oracle |
| Existing 2 GB corpus | Regression against current evidence | No loss of current topology/RSS behavior |
| 10 GB-class graph | Spill and runtime rehearsal | Strict lane and cgroup metrics stable |
| 50 GB-class logical graph | PRD thesis gate | Exact WCC finishes under an 8 GB cgroup, or the claim remains unproven and the receipt explains rejection |

### Memory truth rules

1. Engine reservation accounting is a deterministic invariant.
2. RSS and cgroup usage are measurements, not inferred from heap allocations.
3. Fast-mmap and strict-bounded-I/O results are reported separately.
4. Cold-cache and warm-cache runs are reported separately.
5. Snapshot build, publication, verification, and query execution are separate
   phases with separate peaks.
6. A result that fits only because output rows were omitted is not a passing
   stream benchmark.
7. A killed or swapped process is not a completed low-RAM run.
8. Plugin measurements include the whole Neo4j/JVM/native process. Standalone
   Rust measurements exclude Neo4j by construction and are labeled as such.
9. Cross-engine parity and target-scale memory may use different graph scales
   only when both datasets and the reason are explicit in the evidence bundle.

## Timeline Traversal

## Decision Frame

- Fork: which architecture should consume the next 90 days?
- Desired outcome: one externally reproducible, exact, low-RAM GDS algorithm
  path that compounds into the rewrite.
- Hard constraints: small team, current code must remain green, 8 GB target,
  immutable snapshots, deterministic unsupported behavior, no false
  compatibility claim.
- Failure: many new modules but no end-to-end parity and memory receipt.
- Horizon: Week 1, Month 1, Month 2, Month 3.

## Timeline A: Breadth-First Full Rewrite

- Opening move: establish crates for OLTP, WAL, Bolt, Cypher, GDS, Build Store,
  snapshots, and algorithms.
- Week 1: architecture feels comprehensive; most outputs are interfaces.
- Month 1: Bolt may connect and record files may exist, but no production
  transaction semantics or serious GDS execution is complete.
- Month 2: integration cost appears across values, transactions, query
  planning, and procedure dispatch. The low-RAM algorithm path is still one
  workstream among many.
- Month 3: a broad demo is possible only by hiding stubs. No single claim has
  deep evidence.
- Lived experience: constant context switching and a misleading sense of
  motion.
- Kill criterion: if no same-input differential algorithm run exists by Week
  6, stop and collapse into Timeline E.
- Likely quarter outcome: architecture scaffolding, weak product proof.

## Timeline B: Compatibility First

- Opening move: Bolt handshake, PackStream values, basic Cypher, generated
  procedure signatures.
- Week 1: an official driver connects. Morale is high.
- Month 1: `RETURN 1` and deterministic unsupported GDS calls work.
- Month 2: real projection/catalog behavior exposes missing storage and
  generation semantics.
- Month 3: WCC work begins late under a public compatibility narrative.
- Lived experience: measurable progress becomes a trap because each added
  Testkit pass feels safer than starting the hard engine.
- Kill criterion: freeze compatibility after the first driver canary; if it
  consumes more than two weeks, move it out of the critical path.
- Likely quarter outcome: credible shell, unproven differentiator.

## Timeline C: GRAIN Format First

- Opening move: freeze a format spec and implement degree ranking, Elias-Fano,
  strata, and 2D blocks.
- Week 1: strong design artifacts and codec spikes.
- Month 1: builder complexity dominates; the hot flat stratum can run simple
  scans, but the full estimator thesis is not yet tested.
- Month 2: format debugging competes with algorithm semantics and publication.
- Month 3: one or two algorithms run, but state-heavy formula accuracy remains
  uncertain.
- Lived experience: intellectually rich and externally quiet.
- Kill criterion: if WCC cannot run from the hot subset by Week 3, fall back to
  current snapshot v3 and preserve only manifest additions.
- Likely quarter outcome: useful research, delayed end-to-end proof.

## Timeline D: Families First

- Opening move: implement WCC, BFS, PageRank, and Dijkstra directly against
  current adjacency.
- Week 1: WCC/BFS code lands quickly.
- Month 1: several algorithms pass tiny fixtures.
- Month 2: each has its own vectors, queues, errors, and output handling.
- Month 3: Louvain or NodeSimilarity exposes the absent budget/spill substrate;
  a common execution interface must be retrofitted.
- Lived experience: fast and fun until the most important benchmark arrives.
- Kill criterion: no second algorithm begins until the first runs through a
  common plan, reservation, sink, and receipt.
- Likely quarter outcome: algorithm breadth with architectural debt.

## Timeline E: Proof-Carrying WCC Slice

- Opening move: separate support intent from implementation truth, define the
  common execution envelope, and run a plugin/FFI ping spike.
- Week 1: existing tests stay green; the two riskiest seams, budget threading
  and JNI direct-buffer transport, receive explicit verdicts.
- Month 1: WCC passes tiny, randomized, and first-party fixture parity through
  CLI execution. Budget rejection and estimate receipts exist.
- Month 2: strict execution, atomic generation publication, reader pinning,
  cancellation, progress, and completed receipt reconciliation work.
- Month 3: `grain.wcc.stream` runs beside GDS on stock Neo4j, the scale ladder
  produces evidence bundles, and NodeSimilarity prices the next architecture
  decision.
- Lived experience: narrower daily scope, frequent falsifiable milestones,
  and less spectacular code volume. Each month ends in a runnable system.
- Kill criterion: if current code cannot accept the execution envelope by Week
  2, create one contained new core behind existing APIs; do not broaden into a
  full rewrite.
- Likely quarter outcome: one small but defensible replacement path and a
  reusable verification spine.

## Cross-Timeline Analysis

| Path | Upside | Downside | Reversibility | Regret risk | What must cooperate |
| --- | --- | --- | --- | --- | --- |
| A Full rewrite | Resembles final product | Integration arrives after the quarter | Low-medium | High | Every subsystem and oracle at once |
| B Compatibility | Early driver proof | Does not test low-RAM completion | Medium | High if it becomes the main track | Protocol scope discipline |
| C GRAIN first | Potential format moat | Core hypothesis and codecs unproven | High if manifest-first/flat-payload | Medium-high | Codec performance and formula accuracy |
| D Families first | Fast kernel learning | Common budget/runtime debt | Medium | Medium-high | Willingness to stop before family two |
| E Proof-carrying WCC | Strongest evidence per week | Narrower visible scope | High | Low | WCC oracle, cgroup harness, bridge seam |

## Decision Filter

- Strongest under normal conditions: Timeline E.
- Safest if capacity is halved: Timeline E still yields CLI WCC, receipt, and
  publication evidence; the plugin or NodeSimilarity spike can slip without
  corrupting the architecture.
- Best hedge against wrong format decisions: Timeline E, because `GraphView`
  and capability plans allow a later GRAIN or cell reader.
- Best hedge against wrong market assumptions: the stock-Neo4j side-by-side
  proof can be shown to real users before the full rewrite.
- Fastest uncertainty collapse: Week 1 budget-threading and JNI ping, followed
  by Month 1 WCC partition parity.

## The 90-Day Step-By-Step Plan

### North-star deliverable

By Day 90, from a clean Linux machine, one command sequence should:

1. load or reference the same graph in stock Neo4j and Knight Bus;
2. build and atomically publish Knight Bus generation W;
3. print `grain.wcc.estimate` without opening topology payloads;
4. run `grain.wcc.stream` under a configured memory budget;
5. run `gds.wcc.stream` as the oracle when the baseline can execute;
6. canonicalize and compare partitions;
7. persist an evidence bundle containing estimate, execution, result digest,
   oracle verdict, memory peaks, watermarks, and checksums;
8. either finish a 50 GB-class logical graph under an 8 GB cgroup or state
   clearly that the target remains unproven.

This north star contains two linked runs, not one misleading process total:

- a small or medium same-input plugin run establishes stock-GDS semantic
  parity and reports the entire Neo4j/JVM/native process footprint;
- a standalone target-scale Rust run establishes the 8 GB engine claim;
- if stock GDS cannot execute the target-scale graph, its estimate/rejection
  is captured, but it is not required for the exact large-scale Rust run.

### Explicitly in scope

- status-truth separation;
- common graph, plan, budget, kernel, sink, control, and receipt contracts;
- exact WCC stream and estimate modes;
- deterministic rejection;
- atomic snapshot publication and reader pinning;
- Neo4j/GDS differential harness;
- a small stock-Neo4j `grain.*` plugin;
- Linux cgroup benchmark evidence;
- one hard-family architecture spike.

### Explicitly out of scope

- a Rust OLTP record store;
- full Bolt or Cypher implementation;
- shadowing or patching the `gds.*` namespace inside stock Neo4j;
- write/mutate WCC modes;
- all 575 procedure implementations;
- Louvain production support;
- GRAIN Elias-Fano and cold-block codecs;
- cellular CSR adoption;
- GraphBLAS backend;
- managed cloud deployment;
- public "Neo4j replacement" or "50 GB on 8 GB" claims before their gates.

### Week 1: Establish truth and kill the two largest unknowns

Deliverables:

- record the passing 51-test baseline and current artifact checksums;
- replace the overloaded support label in the design with separate target,
  implementation, verification, and evidence fields;
- create a failing registry consistency test proving no research row can
  promote runtime implementation;
- thread a no-op `MemoryBudget` and cancellation token through one current
  adjacency traversal;
- build `grain.ping` or an equivalent throwaway Neo4j-plugin JNI spike that
  returns a bounded direct-buffer stream of `(nodeId, degree)` from an existing
  snapshot;
- write ADRs for the 90-day scope, WCC-first choice, flat-snapshot baseline,
  and strict-versus-fast lanes.

Gate:

```text
GO brownfield if budget/control threading is local and existing tests stay green.
CONTAINED CORE if it requires broad invasive changes.
STOP plugin plan if JNI/classloader/direct-buffer proof fails; use a Unix-socket
sidecar adapter without changing the Rust execution architecture.
```

### Week 2: Write the WCC verification packet before the kernel

Read and extract:

- GDS `WccTest` fixture graphs and expected behavior;
- GDS WCC stream result shape and config defaults;
- GDS WCC memory estimator state classes;
- GAPBS connected-components verifier behavior;
- current Knight Bus topology parity helpers.

Create failing tests for:

- isolated nodes;
- one component;
- multiple components;
- duplicate and reordered edges;
- directed input interpreted with the declared orientation;
- seeded behavior only if included in the first support contract;
- component-id normalization;
- over-budget rejection before graph open;
- bounded output chunks;
- cancellation between edge passes.

Gate: the test matrix names the parity class, memory formula, public result
columns, and unsupported configs. No kernel code begins while those are vague.

### Week 3: Implement the common execution envelope

Deliverables:

- `GraphView` adapter over `MmapWalkRuntime`;
- `ExecutionRequest`, `ExecutionPlan`, and capability validation;
- `MemoryBudget`, `MemoryReservation`, and fixed workspace ownership;
- `ExecutionControl` for cancellation and progress;
- `ResultSink` with digest-only and bounded-vector test sinks;
- estimate and completed receipt schemas;
- an in-memory tiny-graph adapter used only as an oracle.

Gate: a fake kernel can plan, reserve, emit rows, reconcile a receipt, cancel,
and reject without special knowledge of WCC.

### Week 4: Make WCC green on semantic fixtures

Deliverables:

- exact WCC kernel against `GraphView`;
- deterministic component canonicalization in the verifier, not necessarily in
  the production result;
- CLI `estimate` and `stream` paths;
- randomized small-graph comparison against a simple independent oracle;
- all previous tests remain green.

Month 1 exit criterion:

```text
One real algorithm runs through the common plan/budget/sink/receipt path.
Tiny and randomized partitions are exact.
An insufficient budget rejects before topology open.
No Neo4j compatibility claim is made yet.
```

### Week 5: Turn estimates into enforced reservations

Deliverables:

- WCC formula from manifest dimensions and selected variant;
- separation of topology residency policy, controlled workspace, read buffers,
  result chunks, and optional spill;
- reservation underflow/overflow and concurrent-job tests;
- hostile tiny-budget tests forcing every rejection branch;
- estimate receipt persisted before execution.

Gate: engine-owned peak reservation never exceeds the admitted value. Any
untracked large allocation blocks promotion to `verified`.

### Week 6: Implement and measure the strict lane

Deliverables:

- explicit bounded edge-block reader or equivalent strict access adapter;
- fast mmap lane remains available and separately reported;
- Linux cgroup v2 runner with cold/warm cache controls;
- receipt reconciliation for reservation peak, RSS peak, cgroup peak, bytes
  read, and spill bytes;
- repeated-run variance report.

Gate: strict-mode evidence is not inferred from macOS RSS or heap counters. It
must be collected from the Linux target environment. If strict mode exceeds the
budget, preserve the failure evidence and adjust the plan rather than hiding it.

### Week 7: Implement atomic publication with failpoints

Deliverables:

- private generation directory creation;
- payload and manifest checksums;
- staged, validating, published, retired, failed, and garbage states;
- active pointer update with platform-correct durability ordering;
- failpoints after each state transition and durability operation;
- restart recovery rules.

Gate: for every injected crash, restart selects either W or W+1 and never a
mixed generation.

### Week 8: Bind catalog and readers to real generations

Deliverables:

- catalog key includes user, database, graph, and generation;
- reader pin retains W while publication advances to W+1;
- generation capabilities come from the validated manifest;
- retention refuses to collect pinned generations;
- existing in-memory projection tests are upgraded to use fixture generation
  handles where practical.

Month 2 exit criterion:

```text
WCC is exact, budget-admitted, cancellable, streamed, generation-pinned,
crash-safe, and accompanied by a reconciled receipt.
```

### Week 9: Productize the narrow Neo4j bridge

Deliverables:

- `grain.wcc.estimate` and `grain.wcc.stream` procedure shims;
- one chunked direct-buffer FFI boundary, never a callback per result row;
- stable mapping between Neo4j node IDs and snapshot dense IDs;
- deterministic error mapping for missing graph, corrupt generation,
  unsupported config, insufficient budget, and cancellation;
- plugin integration test against a supported stock Neo4j version.

Gate: Java contains procedure adaptation only. It does not own graph topology,
algorithm state, planning, or memory policy.

### Week 10: Make the same-input differential loop boring

Deliverables:

- one-command or one-script snapshot export/build path;
- GDS and Knight Bus execution over the same committed input;
- partition canonicalization and empty diff;
- watermark, ID-map, and manifest checksums in the evidence bundle;
- rerun determinism test.

Gate: an ID mapping bug must cause a hard verification failure, not merely a
different component label.

### Week 11: Run the honest scale ladder

Deliverables:

- tiny, generated, 2 GB, 10 GB-class, and target-scale runs;
- Neo4j GDS estimate captured even when GDS rejects or cannot run;
- Knight Bus estimate and completed receipts;
- cold/warm, fast/strict, build/query phase separation;
- no manually edited benchmark summaries.

Gate: every chart cell links to a complete evidence directory. Missing or
killed runs are shown as failures, not omitted.

### Week 12: Attack the architecture with NodeSimilarity

This is an architecture spike, not a support promise.

Compare:

1. exact in-memory candidate generation;
2. exact bucketed candidate spill;
3. deterministic rejection from degree/candidate bounds;
4. optional sketch-and-rerank as an explicitly approximate mode.

Questions:

- Can exact candidate memory be bounded from manifest statistics tightly
  enough to admit or reject before execution?
- Is bucketed spill operationally viable under 8 GB?
- Which additional snapshot capabilities, if any, are actually required?
- Does this evidence justify cells, degree ranking, or a GRAIN cold-block
  experiment?

Gate: no approximate result is labeled exact, and no new durable format is
adopted from a synthetic speedup alone.

### Week 13: Reproduce, close, and choose Quarter 2

Deliverables:

- clean-machine reproduction of the full WCC workflow;
- immutable release evidence bundle;
- updated procedure registry states derived from passing gates;
- updated benchmark and limitation statement;
- one Quarter 2 architecture decision based on evidence:
  - PageRank next if the common scan/vector plan needs validation;
  - NodeSimilarity exact-spill track if Week 12 succeeds;
  - GRAIN/cells experiment if a measured reader/layout bottleneck exists;
  - compatibility track if real users reject the `grain.*` bridge despite the
    engine proof;
- explicit list of unmet PRD requirements.

Day 90 exit criterion:

```text
The WCC proof is independently runnable and falsifiable.
The 50GB/8GB claim is either demonstrated or still explicitly unproven.
The next architecture bet is selected from receipts and failures, not taste.
```

## Recommended First Ten Work Items

These are ordered vertical dependencies, not ten parallel tasks.

1. Split registry target tier from observed implementation and verification.
2. Add the WCC oracle/acceptance test packet in red state.
3. Add graph capability and generation-handle contracts.
4. Add execution plan, budget reservation, cancellation, and result-sink
   contracts.
5. Implement exact WCC on the existing topology runtime.
6. Persist estimate and completed receipts with reconciliation.
7. Implement atomic publication and reader pins with failpoints.
8. Implement the narrow plugin/FFI bridge.
9. Automate GDS differential parity and evidence bundle generation.
10. Run target-scale WCC and the NodeSimilarity architecture spike.

Each work item should be independently mergeable and should leave all prior
gates green. Avoid a branch that is red for weeks while architecture is built
in private.

## Reading Queue For Implementation

The first implementation goal does not require rereading all 24,000 reference
files. It requires a narrow source packet.

### Current Knight Bus: read completely

1. `src/runtime.rs`
2. `src/low_ram.rs`
3. `src/snapshot.rs`
4. `src/types.rs`
5. `src/gds.rs`
6. `src/gds/catalog.rs`
7. `src/gds/execution.rs`
8. `src/parity.rs`
9. `tests/graph_adjacency_runtime_contract.rs`
10. `tests/gds_registry_contract.rs`
11. `tests/gds_execution_contract.rs`
12. `tests/snapshot_manifest_v3_contract.rs`

### Neo4j GDS: read the WCC path completely

The current clone has moved under `gitrefrepo/Neo4j family/neo4j-gds-src/`;
some older documents still cite its former top-level path.

1. `proc/community/.../WccStreamProc.java`
2. `procedures/algorithms-facade/.../LocalCommunityProcedureFacade.java` around
   `wccStream` and `wccStreamEstimate`
3. `procedures/procedures-facade-api/.../GraphDataScienceProcedures.java`
4. `algo/.../wcc/WccMemoryEstimateDefinition.java`
5. `algo/.../wcc/WccTest.java`
6. WCC stream result builder and result row types
7. WCC config interfaces and default-value tests
8. `mutate` and `write` paths only enough to mark them explicitly out of the
   90-day support scope

### Secondary oracles and runtime precedent: targeted reads

1. `gitrefrepo/gapbs-src/src/cc.cc` for an independent verifier shape.
2. DataFusion memory-pool reservation interfaces and one spillable operator,
   for ownership and accounting patterns rather than a wholesale dependency.
3. Neo4j procedure/plugin examples and registration tests for the narrow Java
   shim.
4. Neo4j testkit only for the later compatibility canary; it is not on the WCC
   kernel critical path.

### Prompt for each source file

```text
Read this file as evidence for the Knight Bus WCC verification slice.

Return:
1. Public behavior or invariant exposed by the file.
2. Inputs, defaults, modes, outputs, errors, cancellation, and progress rules.
3. Memory-owning data structures and their size dimensions.
4. Graph capabilities required: direction, sorted neighbors, weights,
   properties, global stream, random access.
5. Exact parity rule and any nondeterminism.
6. Source symbols and line spans supporting each claim.
7. What belongs in the Rust core, adapter, test oracle, or unsupported registry.
8. One or more executable tests unlocked by the evidence.
9. Open uncertainty and the smallest falsifier.

Do not propose code until behavior, memory, and oracle contracts are explicit.
Mark every statement DirectSource, GraphToolAssisted, Inference, or Unknown.
```

Save each result as a small evidence record linked to a requirement/test ID,
not as another free-form codebase summary. The final unit of reuse is:

```text
source span -> contract -> test -> implementation state -> receipt evidence
```

## Architecture Decisions To Record

The first implementation branch should create or approve these ADRs before
their corresponding work begins:

| ADR | Decision | Revisit trigger |
| --- | --- | --- |
| ADR-001 | Extend current Rust core for the first slice | Week 1 budget-threading audit exceeds one month of predicted rework |
| ADR-002 | WCC stream/estimate is the first algorithm surface | First-party oracle or ID mapping proves materially harder than expected |
| ADR-003 | Snapshot v3 flat dual CSR remains the baseline payload | Measured target-scale reader bottleneck requires a different capability |
| ADR-004 | Fast mmap and strict bounded-I/O lanes are separate | A measured platform policy makes one lane strictly dominate without weakening truthfulness |
| ADR-005 | Registry intent and implementation truth are separate fields | Never; this is a governance invariant |
| ADR-006 | Neo4j/GDS is a temporary host and oracle | Rust OLTP/Bolt path passes its own compatibility gates |
| ADR-007 | `grain.*` is the stock-Neo4j POC namespace | Standalone Rust server is ready to expose true `gds.*` compatibility |
| ADR-008 | No cells, EF, or GraphBLAS on the critical path | A falsifier/benchmark crosses the documented adoption threshold |
| ADR-009 | One crate until two consumers validate a seam | Second algorithm/adapter creates a stable shared boundary |
| ADR-010 | Evidence bundles are release artifacts | Never; without them the central claim is not reviewable |

## Effort Envelope

This is an inference for planning, not a promise. With one strong engineer plus
high-quality coding agents, the selected quarter is roughly:

| Workstream | Approximate engineering share | Risk |
| --- | ---: | --- |
| Contracts, planning, budget, receipts | 20% | Medium; semantics matter more than code volume |
| WCC kernel and oracles | 15% | Low-medium |
| Publication/catalog hardening | 20% | Medium-high due to crash semantics |
| Strict-memory harness and scale benchmarks | 20% | High due to OS and dataset realities |
| Neo4j plugin/FFI and ID mapping | 15% | Medium-high |
| NodeSimilarity architecture spike and closure | 10% | High but deliberately bounded |

An order-of-magnitude code expectation is 6,000-12,000 new or substantially
changed lines across Rust, tests, harness scripts, and the small Java bridge.
That number is not a goal. A smaller implementation with stronger evidence is
better than a larger one.

## Risk Register

| Risk | Early warning | Containment |
| --- | --- | --- |
| Current code resists budget threading | Week 1 changes fan across most modules | Put a contained executor behind existing APIs; preserve runtime as adapter/oracle. |
| Registry state remains ambiguous | Docs and runtime report different support counts | Generate runtime support only from code-backed evidence states; CI checks the join. |
| WCC result IDs silently mismatch Neo4j IDs | Partition differs only after export/reload | Round-trip ID-map checksums and adversarial non-dense Neo4j IDs. |
| Strict lane still OOMs due to page cache | Reservation passes while cgroup peak crosses cap | Bounded reader, cache policy, cgroup evidence, and safety margin; no strict claim from mmap. |
| Publication is only logically atomic | Crash tests reveal mixed or missing generations | Failpoint-first implementation and platform-specific fsync/rename tests. |
| JNI bridge dominates work | Per-row callbacks or graph copies appear | One chunked direct buffer; switch adapter transport if Week 1 spike fails. |
| WCC success overgeneralizes | NodeSimilarity cannot be planned under budget | Month 3 hard-family spike before broad family roadmap. |
| Benchmark becomes marketing theater | Missing failed runs or hand-edited summaries | Artifact bundle is canonical; summaries generated from it. |
| Full rewrite scope leaks back in | Bolt/OLTP tasks enter the WCC critical path | Enforce explicit out-of-scope list until Month 2 exit gate. |
| GRAIN becomes identity rather than hypothesis | Codec work begins before a measured bottleneck | Manifest-compatible flat payload only; ADR revisit requires benchmark evidence. |

## Evidence And Verification Questions

This section performs the deep-exploration verification pass on the central
claims.

### Q1. Does the current repo contain enough reusable substrate to avoid a rewrite now?

Yes. Direct reads show a bounded external-sort builder, immutable dual-CSR
snapshot, mmap adjacency runtime, generation-aware manifest, GDS inventory and
catalog subset, truth/parity code, benchmarks, and 51 passing tests. This does
not prove the design is perfect; the Week 1 budget-threading audit is the
falsifier.

### Q2. Is WCC actually the most verifiable first algorithm?

Yes for the current objective. The GDS source contains WCC tests and memory
estimation, the output can be compared by partition normalization, GAPBS offers
an independent verifier, and the result shape is simple. PageRank introduces
floating-point tolerance; Louvain introduces non-unique optima and contracted
graph workspaces.

### Q3. Does WCC prove the full 50 GB-on-8 GB thesis?

No. It proves one important semi-external scan family. It does not prove
state-heavy similarity, embeddings, or hierarchical community detection. The
claim remains family- and mode-specific, which is why Month 3 attacks
NodeSimilarity.

### Q4. Does a memory receipt itself enforce memory safety?

No. Enforcement comes from reservations, bounded readers, spill policy,
admission, and cgroup limits. The receipt makes those decisions auditable and
reconciles them with measurements.

### Q5. Does the stock-Neo4j plugin preserve the final architecture?

Only if it is an adapter. Java must not own planning, topology, algorithms, or
memory policy. The same Rust core should work from CLI now and Bolt later.

### Q6. Does delaying Bolt/Cypher/OLTP jeopardize the rewrite?

It delays breadth but reduces foundational risk. The quarter produces a stable
kernel and verification contract that those surfaces can call later. The
tradeoff changes if the user's immediate goal is driver compatibility rather
than low-RAM OLAP; that is not the priority stated by `prd-l1.md` and the recent
conversation.

### Q7. Is the architecture novel because of one data structure?

No. Its novelty is the integration of ordinary mechanisms into a stronger
contract:

```text
generation identity + logical projection + selected physical read shape
+ explicit plan + enforced budget + exact/approx flag + streamed result
+ oracle verdict + measured reconciliation
```

Each mechanism has precedent. Their composition makes a graph run independently
auditable.

### Q8. What observation would most damage this recommendation?

If Week 1 shows that the current runtime cannot accept budget/control seams
without pervasive redesign, and the JNI/plugin adapter cannot stream without
copying graph or result state, the brownfield/plugin shape weakens. The response
is a contained new executor plus socket adapter, not a return to breadth-first
rewriting.

## Final Synthesis

`docs_PRD04/` has already done the broad thinking. It has separated OLTP from
OLAP, inventoried GDS, mapped memory and algorithm families, cataloged reference
systems, designed publication semantics, and explored flat, cellular,
compressed, algebraic, and out-of-core architectures.

The internet verification in this revision also corrects one planning premise:
the seven are algorithm families and modeled priorities, not seven complete
graph use cases or measured adoption telemetry. Keep them as the static
analytics workload portfolio, then add the four gates for filtered retrieval,
directed dependencies, temporal generations, and artifact lifecycle.

The project now needs a narrowing move.

The best architecture for the next quarter is a proof-carrying OLAP strangler:
reuse the current snapshot and runtime, compile WCC requests into explicit
budgeted plans, execute through one common kernel contract, publish generations
atomically, stream results through bounded sinks, compare against GDS, and
persist the receipt that ties correctness and memory to an exact watermark.

The strongest durable storage destination is the Read-Shape Foundry: preserve
canonical exact graph facts, represent named graphs logically when possible,
and compile each admitted algorithm into direct channels, an ephemeral CSR, a
factorized view, or an exact generation-bound artifact. Plan-owned buffers,
page-skip proofs, and co-sharded state are more promising than expecting one
universal compressed CSR to win every workload. This destination is a
falsifiable hypothesis, not a reason to delay the first WCC proof.

The decisive sequencing rule is:

```text
Do not add kernel number two until kernel number one proves the execution spine.
Do not add a new serving payload until a measurement proves the current payload
or projection materialization is the bottleneck.
Do not claim compatibility beyond the canaries that pass.
Do not claim strict RAM from heap accounting or mmap alone.
```

At Day 90, success is not measured by how much of Neo4j has been rewritten.
Success is measured by whether a skeptical outsider can falsify the first
replacement claim and fails to do so.

## Open Questions

These questions should be answered by experiments, not another broad research
round:

1. What exact Linux filesystem and cgroup environment defines the first strict
   8 GB benchmark target?
2. Which WCC variant and config subset is the initial public contract: unseeded
   stream only, or seeded stream as well?
3. Does the first plugin consume a prebuilt snapshot, or must one-command
   Neo4j export/build be a Day 90 release gate?
4. What safety margin separates measured cgroup peak from the configured
   budget during strict execution?
5. Is original Neo4j node ID sufficient as the external identity, or must the
   bridge preserve element IDs and database identity as well?
6. Which exact 50 GB-class dataset is legally redistributable and useful for
   public reproduction?
7. Is NodeSimilarity the right Month 3 breaker, or should Louvain be selected
   because it more directly exercises temporary graph generations and spill?
8. Which receipt fields are stable public ABI and which remain experimental?
9. At what point does the second adapter or algorithm justify extracting a
   multi-crate workspace?
10. Which user observation would cause Quarter 2 to prioritize Bolt/Cypher
    canaries over PageRank or NodeSimilarity?
11. Which real filtered projection should falsify relationship channels and
    exact page skipping after the WCC spine is green?
12. What disk/build budget governs promotion and eviction of algorithm-shaped
    exact artifacts?
13. Which generation changes are eligible for exact differential reuse, and
    which force full recomputation immediately?
14. What opt-in customer workload evidence would be sufficient to replace the
    seven modeled priorities with measured ordering?

The first four questions can be fixed during Week 1. Questions 11-13 belong to
the post-WCC Architecture F experiments. The others should remain open until
the WCC evidence makes them cheaper to answer.

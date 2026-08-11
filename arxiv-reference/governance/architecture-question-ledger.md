# G01 Architecture Question Ledger

**Goal:** G01 repository-grounded terminology mining only
**Authority:** `arxiv-reference/Arxiv-Pattern-Foundry-SOP.md` and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`
**Research state:** No external query has been executed. Every question is `OPEN`.
**Decision rule:** A later source is useful only when it can eliminate, specialize, repair, or defer a candidate option below.

## Encoding Contract

- File encoding: UTF-8.
- Line endings: LF.
- TSV field delimiter: TAB.
- Multi-value delimiter inside one TSV field: `|`.
- Literal multi-value delimiter inside a value: percent-encode as `%7C`.
- Tabs, carriage returns, and newlines inside TSV values: prohibited.
- Null sentinel for unexecuted query fields: `NOT_EXECUTED`.
- Architecture-question identifier syntax: `AQ-NNN`, allocated contiguously by G01.
- Taxonomy identifier syntax: `TERM-NNN`, allocated contiguously by G01.
- Query identifier syntax: `QRY-NNN`, allocated contiguously by G01.
- Stable-ID rule: an identifier is never reused; later wording changes preserve the ID and are recorded in the owning goal journal.
- Path rule: repository evidence paths are repository-relative regular files, never machine-absolute paths.
- Planned-query rule: all query rows remain complete but unexecuted; execution timestamps, result counts, and response checksums use `NOT_EXECUTED`.

## Question Set Contract

This batch intentionally uses all 12 allowed question slots. The set covers the algorithm portfolio, the A007 enforceable-runtime contract, compatibility as an adoption adapter, and verification as the product proof. It does not select a winning architecture or imply that a planned query has literature support.

## AQ-001: Algorithm-Specific Physical Layout Portfolio

- family_slug: algorithm-specific-layouts
- decision: Which topology, sidecar, state, scratch, output, and answer artifacts should be shared or specialized for each supported algorithm and query shape?
- product_consequence: Determines whether Knight Bus can quote closed physical plans with differentiated RAM and latency, without becoming an unbounded collection of duplicated formats.
- candidate_options: Execute canonical generation directly as a control; share immutable topology and specialize only state; compile one baseline algorithm-native view; earn separate SPEED, BALANCED, STRICT-RAM, or MATERIALIZED variants through measured reuse; explicitly defer FastRP and embedding layouts until a customer or benchmark trigger exists.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` separates canonical facts, derived views, runtime state, and admission policy; `docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md` defines shared substrate plus custom state; DERIVED_INFERENCE - the canonical artifact is compiler input and a semantic control, not automatically the best execution layout.
- missing_evidence: View-build peak RSS, retained-disk multiplication, invalidation frequency, cross-algorithm block reuse, graph-shape sensitivity, customer reuse distributions, and operating cost of concurrent generations.
- falsifier: After charging build, validation, retained disk, invalidation, cold-cache execution, and operational complexity, no custom variant produces a nondominated RAM or end-to-end latency point over one simple shared representation.
- status: OPEN
- owner_goal: G01

## AQ-002: PageRank State And Pull Layout

- family_slug: pagerank
- decision: Which exact PageRank semantics and physical profile should prove iterative bounded compute: resident destination-major pull, compressed pull blocks, strict two-dimensional tiles, or a generation-bound materialized result?
- product_consequence: Can prove state estimation, convergence, numerical determinism, and strict-RAM execution, but risks optimizing a benchmark before proving the security and dependency customer job.
- candidate_options: Resident reverse CSR or CSC with ping-pong vectors; compressed destination-range pull blocks; strict source-by-destination tiles with rank slabs; precision-reduced declared approximation; warm-start or materialized rank vectors for stable generations.
- known_evidence: SOURCE_CLAIM - `docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md` identifies PageRank as the iterative-state proof; `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` distinguishes global from personalized PageRank and names the reverse-adjacency state floor; `docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md` proposes normalized pull and strict tile plans.
- missing_evidence: GDS comparator semantics, dangling-node policy, deterministic reduction cost, whole-process RSS, page-cache residency, skew sensitivity, exact tiled schedules, warm-start validity, and reduced-precision top-K stability.
- falsifier: A specialized pull plan fails numerical parity or does not reduce measured bytes, RSS, or full projection-to-validation time; strict tiling violates its cap or disk envelope; warm-start validation costs more than saved iterations.
- status: OPEN
- owner_goal: G01

## AQ-003: BFS And Bounded Path State

- family_slug: bfs
- decision: Which first path contract should Knight Bus support - existence, reachable nodes, weighted or unweighted distances, or proof paths - and when should reverse topology, predecessor state, or path indexes be compiled?
- product_consequence: This is the strongest A007 security and dependency wedge, but frontier, predecessor, and output state can defeat the RAM contract even when adjacency access is compact.
- candidate_options: Forward-only resident traversal; optional reverse blocks for bidirectional or reverse reach; adaptive sparse and bitmap frontiers; strict blocked frontier runs; spillable distance or predecessor slabs; generation-bound SCC, landmark, level, or parent artifacts.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/A007-spc-founder-interview-prep-v7.md` ranks bounded path work first; `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` separates topology, frontier, predecessor, queue, and output admission; `src/runtime.rs` and `src/cypher.rs` prove only mmap-backed one-hop and reverse one-to-two-hop execution with materialized endpoints.
- missing_evidence: Real frontier distributions, touched-block locality, output-cardinality predictors, sparse-to-bitmap thresholds, reverse-view reuse, predecessor spill cost, bidirectional hit rate, and exact mapping to GDS and Cypher path semantics.
- falsifier: Representative workloads require unbounded output or predecessor state, strict blocked execution repeatedly scans most cold blocks, selective reverse storage approaches universal transpose cost, or target users do not value bounded traversal.
- status: OPEN
- owner_goal: G01

## AQ-004: WCC Streaming And Component State

- family_slug: wcc
- decision: Should exact WCC use symmetric adjacency, a canonical pair-once edge stream with resident labels, or range-sharded external union state under strict budgets?
- product_consequence: WCC is the cleanest global proof of topology-versus-state accounting and equivalence-class verification, but its best plan depends on whether a dense parent or label array fits.
- candidate_options: Afforest or label propagation over symmetric CSR; pair-once undirected edge runs plus union-find; range-sharded edge runs and state capsules; materialized component labels plus spanning-forest certificates for repeated generations.
- known_evidence: SOURCE_CLAIM - `docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md` defines WCC as the partition-verification proof; `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` prefers a pair-once stream for RAM-first union and requires canonicalized partition equality; `docs_PRD06/LowRAM-All-Algorithm-Architecture-Atlas.md` records WCC, Afforest, hooking, and union-find variants.
- missing_evidence: Exact external-union schedules, high-degree and giant-component behavior, random-versus-sequential label I/O, checkpoint recovery cost, concurrency sensitivity, and first-run comparison against tuned GDS or Graphalytics fixtures.
- falsifier: Pair-once streaming is slower and no smaller end to end than symmetric adjacency, sharded state causes uncontrolled random I/O or merge growth, or partition certificates cannot expose implementation errors independently.
- status: OPEN
- owner_goal: G01

## AQ-005: Triangle Orientation And Intersection Layout

- family_slug: triangles
- decision: Which exact edge orientation, adjacency encoding, intersection kernel, and hub treatment minimize bytes and variance for global triangle count and per-node clustering outputs?
- product_consequence: Can demonstrate an algorithm whose storage layout encodes an exact counting schedule, but may be too specialized for the first customer proof and may shift cost into orientation preprocessing.
- candidate_options: Sorted symmetric adjacency control; degree-oriented forward lists; degeneracy-oriented lists; compressed block-local intersections; measured dense-hub bit pages; external wedge or tile runs under strict RAM; materialized triangle and LCC columns.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` proposes degree or degeneracy orientation so each triangle is counted once; `docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md` separates global counters from per-node output state; `docs_PRD06/LowRAM-All-Algorithm-Architecture-Atlas.md` identifies triangle counting and bitmap layouts.
- missing_evidence: Orientation build peak and lifetime, degree-skew breakpoints, compressed-intersection throughput, hub-bitset promotion thresholds, per-node counter spill cost, and independent motif fixtures with self-loop and parallel-edge semantics.
- falsifier: Orientation and hub metadata cost more build time or retained bytes than saved intersections for the target cadence, or strict wedge expansion creates an output larger than the admitted envelope.
- status: OPEN
- owner_goal: G01

## AQ-006: Community Detection Dynamic State

- family_slug: communities
- decision: How should Louvain, Leiden, and label-propagation plans bound neighbor-community tallies, level graphs, contraction, seed behavior, and partition output without hiding approximation?
- product_consequence: Community algorithms test the hard case where dynamic state can dominate topology, so a smaller CSR alone cannot make an enforceable promise.
- candidate_options: Fully resident weighted adjacency and tally arenas; bounded tally arenas with overflow spill; exact community-edge sort and reduce by level; streamed contraction with one active level; explicit capped, sampled, or early-stop approximation; materialized hierarchy and membership.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` identifies neighbor-community tallies as the volatile state and proposes sort-reduce overflow; `docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md` proposes streamed contraction and one-level-at-a-time artifacts; `docs_PRD06/LowRAM-All-Algorithm-Architecture-Atlas.md` separates Louvain, Leiden, LPA, and community-family plans.
- missing_evidence: Safe tally upper bounds, spill amplification by skew, convergence and seed reproducibility, exact contraction scheduling, modularity tolerance, valid-partition oracles, hierarchy storage, and warm-start correctness across generations.
- falsifier: Exact tally state cannot be conservatively bounded without refusing most useful graphs, spill destroys useful latency, or cross-implementation partition variability prevents a decision-grade oracle.
- status: OPEN
- owner_goal: G01

## AQ-007: Similarity And kNN Candidate Control

- family_slug: similarity-knn
- decision: Which separately admitted exact, spillable, approximate, materialized, or refusal layouts can bound topological NodeSimilarity and property-vector kNN without conflating their state or silently replacing exact semantics?
- product_consequence: Avoiding pair-graph, clique, or full-vector materialization could be a major RAM win, but incidence-candidate count, vector dimension, ANN search breadth, and retained index bytes are different dangerous dimensions that require separate estimates and receipts.
- candidate_options: Exact sorted adjacency intersections; exact entity-feature incidence postings; degree bands and heavy-feature partitions; external pair-contribution sort and reduce; MinHash or LSH candidates plus exact set scoring; exact brute-force vector scan; HNSW with scalar quantization; IVF-PQ partitioned probes; disk-resident ANN graph plus full-vector rescoring; bounded per-node top-K state; generation-bound materialized neighbors; refuse exact or approximate jobs whose declared bounds cannot be met.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` says incidence may be the exact graph-shaped storage and requires candidate-volume admission; `docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md` separates exact all-pairs, sketch candidates, exact landing, and fixed top-K; `docs_PRD06/LowRAM-All-Algorithm-Architecture-Atlas.md` records similarity, kNN, LSH, ANN, and quantization families.
- missing_evidence: Incidence-candidate prediction from degree histograms, heavy-feature skew, vector count and dimension, quantization error, HNSW beam and IVF probe bounds, disk-ANN I/O locality, retained index and full-vector bytes, exact spill volume, tie semantics, all-pairs refusal thresholds, recall calibration, rerank completeness, and whether first-ICP artifacts naturally expose either bipartite incidence or property vectors.
- falsifier: Candidate growth remains effectively quadratic after incidence and degree controls, ANN retained state or random I/O defeats the resource contract, full-vector rescoring exceeds its admitted budget, or approximate candidates cannot state a useful recall or error contract.
- status: OPEN
- owner_goal: G01

## AQ-008: Bounded-RAM External Execution

- family_slug: bounded-ram-external-memory
- decision: What runtime and storage contract can enforce a whole-process RAM ceiling while selecting exact fit, exact spill, declared approximation, or pre-run refusal?
- product_consequence: This is the central A007 differentiation; an internal buffer budget or post-hoc RSS measurement is not the paid promise.
- candidate_options: Dense admitted fit; mmap with elastic page cache and no hard claim; fixed buffer pool plus cgroup ceiling; partitioned or semi-external multi-pass plan; declared sketch or quantized plan; fail-closed refusal with minimum feasible alternatives.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/reference-learning/neo4j-compat-lowram/current-implementation-gap-ledger.md` shows bounded snapshot runs and RSS measurement but no whole-process admission or four-way planner; `docs_PRD04/reference-learning/neo4j-compat-lowram/founder-contract-spine.md` defines full-working-set terms and hard ceilings; `src/low_ram.rs` proves bounded construction phases only.
- missing_evidence: Cgroup or supervised-process overshoot, allocator and mmap accounting, page-cache policy, exact spill schedules, output bounds, temporary-disk admission, cleanup after failure, OS safety margin, and estimator calibration across graph shapes.
- falsifier: Admitted jobs cross the declared ceiling outside a stated overshoot bound, external plans cannot estimate or cap temporary I/O, or refusal and spill do not change a real deployment decision.
- status: OPEN
- owner_goal: G01

## AQ-009: Preparation Versus Repeated Latency

- family_slug: preprocessing-repeated-latency
- decision: Under which reuse, freshness, mutation, and disk conditions should Knight Bus build and retain a derived view, warm state, index, or complete answer rather than execute from canonical facts?
- product_consequence: Determines whether a kernel speedup survives build-to-answer accounting and whether custom artifacts become an economic advantage or a freshness and storage liability.
- candidate_options: Direct canonical execution; ephemeral compilation; one persisted baseline custom view; workload-aware promotion after break-even reuse; immutable-epoch materialization; base-plus-delta maintenance; standing arrangements; explicit invalidate-and-rebuild.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` gives a reuse inequality charging build, I/O, retained disk, invalidation, and operations; `docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md` separates immutable, delta, standing, and rebuild freshness; `docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md` stops the strategy when preparation erases execution benefit.
- missing_evidence: Workload cadence, graph update rate, generation lifetime, stale-answer tolerance, rebuild concurrency, exact break-even reuse, retained-disk budgets, and incremental-maintenance correctness.
- falsifier: The artifact is invalidated before measured break-even reuse or preparation plus validation remains slower or more expensive than a tuned direct baseline for representative workloads.
- status: OPEN
- owner_goal: G01

## AQ-010: Deterministic RAM And Tail Latency

- family_slug: deterministic-ram-tail-latency
- decision: Which scheduler, concurrency, paging, I/O, NUMA, and safety-margin controls are necessary to turn a modeled resource envelope into repeatable peak RAM and useful tail latency?
- product_consequence: Separates deterministic-compute evidence from point benchmarks and determines whether Linux controls are sufficient before any lower-level RustHallows work.
- candidate_options: Statistical envelope on an uncontrolled host; pinned and preallocated Linux profile; bounded worker and queue schedule; explicit page-cache or direct-I/O profile; NUMA-aware partitioning; strict serialized lane; refuse when hardware or workload assumptions are absent.
- known_evidence: SOURCE_CLAIM - `docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md` requires cgroup, allocator, CPU, NUMA, page-fault, mmap, and I/O controls before custom OS work; `docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md` states mmap does not itself bound residency or P100; `src/low_ram.rs` can measure RSS but does not enforce admission.
- missing_evidence: Tail distributions under cold and warm cache, page-fault variance, concurrency scaling, NUMA placement, storage queue depth, thermal and noisy-neighbor effects, overshoot bounds, and attribution of variance to each layer.
- falsifier: Existing Linux controls cannot produce stable envelopes at useful throughput, or the remaining tail variance is not valuable enough to change a customer machine, scheduling, or reliability decision.
- status: OPEN
- owner_goal: G01

## AQ-011: Neo4j Cypher GDS Compatibility Boundary

- family_slug: neo4j-cypher-gds-compatibility
- decision: Which Bolt, Cypher, projection, procedure, mode, and result semantics are must-build adoption adapters, oracle-only references, deferred breadth, or explicit unsupported surface for the first bounded job?
- product_consequence: Enough compatibility can route a production-shaped workload into Knight Bus, while broad parity would turn the wedge into a multi-year Neo4j rewrite.
- candidate_options: File and CLI artifact path only; constrained Bolt plus Cypher profile; GDS procedure-shaped adapter for selected algorithms and modes; translation sidecar; oracle-only GDS comparison; fail-closed unsupported matrix; full parity rejected until customer evidence changes priority.
- known_evidence: SOURCE_CLAIM - `docs_PRD04/reference-learning/neo4j-compat-lowram/founder-contract-spine.md` defines must-build, adapter-only, oracle-only, defer, and reject; `docs_PMF_01/PMF006-Cypher-Bolt-Walk-Spec.md` and `src/bolt.rs` prove one driver-visible neighborhood profile; `src/gds/execution.rs` currently reaches catalog and property operations, not algorithm execution.
- missing_evidence: Real production query corpus, exact parser and planner eligibility boundary, demand-driven PULL and slow-client backpressure, Bolt state-machine coverage, GDS configuration and result-mode semantics, projection ownership, whole-gateway RSS and overhead, clean-room provenance, error codes, writeback behavior, and buyer tolerance for explicit unsupported features.
- falsifier: The target workflow requires broad transactional or Cypher surface before one bounded answer is useful, adapter maintenance dominates the runtime work, or users prefer export-and-run over unchanged-query compatibility.
- status: OPEN
- owner_goal: G01

## AQ-012: Correctness Verification And Receipts

- family_slug: correctness-verification-receipts
- decision: Which independent oracles, equivalence rules, deterministic identities, resource measurements, and before/during/after receipt fields make each admitted algorithm claim machine-verifiable?
- product_consequence: The receipt is the trust primitive and possible product wedge; without independent correctness and measured resource reconciliation, low-RAM claims are benchmark rhetoric.
- candidate_options: GDS differential oracle plus independent mathematical oracle; Graphalytics fixtures; property and metamorphic invariants; exact checksum comparison; partition canonicalization; floating tolerance and convergence contract; approximation quality measurement; same-input resource receipt.
- known_evidence: SOURCE_CLAIM - `docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md` defines four verification lanes; `docs_PRD04/reference-learning/neo4j-compat-lowram/founder-contract-spine.md` defines before, during, and after fields; `docs_PRD04/Algorithm-Storage-Decision-Analysis.md` requires machine-verifiable identities, estimates, high-water marks, I/O, output, oracle, and error.
- missing_evidence: Algorithm-specific equivalence rules, independent implementation selection, receipt schema versioning, measurement-source semantics, oracle disagreement handling, nondeterministic parallel reductions, adversarial fixtures, and whether receipts alter trust or willingness to pay.
- falsifier: Two independent oracles cannot distinguish a plausible wrong implementation, resource fields cannot be reconciled to the enforced boundary, or design partners say the receipt does not affect run, machine, or purchase decisions.
- status: OPEN
- owner_goal: G01

## G01 Decision Yield

G01 does not answer these questions. It converts repository knowledge into a bounded discovery surface whose planned queries can be executed in G02. The next goal should prioritize sources by expected decision change, not by citation count or novelty.

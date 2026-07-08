# Arch05: Greenfield — The Storage Format IS The Estimator

Date: 2026-07-08
Method: Timeline Traverser, fifth iteration. Per instruction, this document
assumes a **GREENFIELD rewrite: all current repo code is POC-only** — it
proved concepts (mmap dual-CSR works, parity vs Neo4j is checkable, RSS can
stay low) and its job is done. Arch04's brownfield conclusion is explicitly
suspended for this exercise. The task here: invent a *logically correct
storage format that suffices OLAP* for the adoption-driving use cases, back
it with published evidence (exact URLs in References), and simulate the
format fork with timelines. Companion: `PMF04.md`.

---

## Phase 0: Deconstruct & Clarify

Premise is sound. Proceeding with optimized protocol. Council: storage-format
designer, external-memory systems researcher (GraphChi/GridGraph lineage),
compression theorist (WebGraph lineage), Skeptical Engineer, GDS-workload
realist.

Core facts carried forward (established in SUM01/Arch02, unchanged):
OLAP snapshots are immutable and generation-sealed; freshness comes from
publishing W+1; ~85% of adoption rides on 7 algorithm families (WCC,
Louvain/Leiden, PageRank, NodeSim/KNN, shortest paths, FastRP, triangles);
the box is 8-16 GB; the differentiator is *honest completion* — knowing the
memory bill before running.

## Part 1: The Creative Leap — What Should A Greenfield Format Optimize?

Conventional answer: optimize bytes/edge (compression) or scan speed (flat
CSR). Both are solved problems with known ceilings: WebGraph reaches ~3
bits/link on web graphs [R4]; flat CSR is the scan-speed baseline every
engine converges to [R6][R7].

The out-of-the-box answer this document proposes: **optimize the format for
PREDICTABILITY — design the file layout so that the memory cost of every
supported algorithm is a closed-form function of a tiny manifest, computable
without touching the data.** Neo4j's estimator runs code to guess [R13]; a
format designed for estimation makes the guess *arithmetic*. Nobody ships
this because every existing format was designed first and estimated later.
Greenfield means we can invert the order.

### The proposed format: GRAIN v0 (Generational Ranked Adjacency, Indexed Nanoshards)

Logical layout (a sealed generation directory):

```text
gen-000042/
  MANIFEST.cbor          # the whole estimator's input: ~KB, never larger
  ranks.dense            # dense u32 node ids, DEGREE-DESCENDING rank order
  strata/
    S0.hot/              # top-k highest-degree vertices ("heads")
      fwd.csr  rev.csr   #   plain uncompressed CSR — scan speed where it matters
    S1.warm/             # mid-degree band
      fwd.ef   rev.ef    #   Elias-Fano compressed adjacency (monotone ids) [R5]
    S2.cold/             # long tail (most vertices, few edges each)
      blocks/PxQ.ef      #   2D edge blocks, GridGraph-style [R2], streamable
  sidecars/
    <prop>.col           # columnar properties, one file per property
  receipts/
    build.json           # provenance: source watermark, build cost, checksums
```

The five design rules that make it "logically sufficient for OLAP":

```text
G1. DEGREE-RANKED DENSE IDS. Vertices are renumbered by descending degree.
    Consequence: the degree array is monotone non-increasing, so it
    compresses to near nothing AND any prefix [0, k) is exactly "the k
    heaviest vertices." Skew-aware layouts are proven (Terrace stores
    neighbors differently by degree class and wins on skewed graphs [R8]);
    GRAIN moves that idea from data structure into the FILE format.
G2. THREE STRATA, THREE ENCODINGS. Hot stratum = flat CSR (it is scanned
    by everything, every iteration); warm = Elias-Fano (2-4x smaller,
    random-accessible without full decompression [R5][R9]); cold = 2D
    blocked for streaming with sliding windows (GridGraph's dual sliding
    windows reduce out-of-core I/O and enable selective block skipping
    when active sets shrink [R2] — exactly WCC/SSSP convergence behavior).
G3. THE MANIFEST IS THE ESTIMATOR. MANIFEST.cbor stores, per stratum:
    vertex count, edge count, exact degree-CDF knots, block occupancy
    histogram, and bytes-on-disk per file. Every supported family's
    workspace formula is a polynomial over these numbers (e.g. PageRank
    scratch = 2 f64 arrays over |V| + hot-stratum pin; NodeSim candidate
    state = f(degree-CDF top band) — the CDF is IN the manifest). Estimate
    cost: read ~KB, do arithmetic. No sampling, no code execution, no
    "production-tier only" carve-outs [R13].
G4. BLOCKS ARE THE SPILL UNIT. The cold stratum's PxQ blocks are the unit
    of admission-controlled execution: an out-of-core plan is "which
    blocks are resident when," i.e., GraphChi/GridGraph scheduling [R1]
    [R2] falls out of the format instead of being bolted on. A 42.5B-edge
    graph has been processed this way on one machine in the literature
    (Gem, 2025, out-of-core monotonic engine over partitioned graphs [R3]).
G5. GENERATIONS, NOT MUTATIONS. No PMA gaps, no in-place inserts (that is
    the mutable-CSR research lane: VCSR/Terrace/Aspen [R8][R10]) — GRAIN
    stays immutable per PRD; freshness = publish gen N+1, and the journal/
    overlay tiers live OUTSIDE the format. One format, one job.
```

Why this suffices for the seven families (the sufficiency argument):
PageRank/FastRP/WCC are whole-graph scans → G2's hot-CSR + streamed cold
blocks; Louvain's coarsening levels are just *new tiny GRAIN generations*
built in-memory (the format recursively describes its own scratch);
NodeSim/KNN's candidate explosion is bounded by the degree-CDF that G3
exposes — the format *knows* its own worst pairs before execution;
shortest paths/BFS get selective block scheduling per G4 [R2][R3];
triangles benefit maximally from G1 (rank-ordered ids are the standard
triangle-counting optimization). No family requires anything the layout
does not already encode.

---

## Decision Frame

- **Fork in the road:** Greenfield storage format for the OLAP engine:
  **A:** GRAIN v0 (predictability-first, stratified, above), **B:**
  maximal-compression monolith (WebGraph-class, ~3 bits/link [R4]),
  **C:** flat-CSR minimalism (POC format, formalized), **D:** mutable
  hybrid (PMA/Terrace-class, freshness inside the format [R8][R10]).
- **Desired outcome:** a format spec'd, frozen at v0, and carrying the
  seven families on 8-16 GB within two quarters, whose estimate story is
  materially stronger than the incumbent's [R13].
- **Hard constraints:** immutable generations (PRD); mmap-friendly (POC's
  one validated lesson); solo capacity; format must be specifiable in a
  document short enough for a third party to implement (the PMF04 play).
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1; kill criteria
  with check dates (Arch04 discipline retained).
- **What counts as failure:** a format so clever it takes a quarter before
  the first algorithm runs; OR estimate formulas that turn out NOT to be
  closed-form for NodeSim/Louvain (the two state-heavy families) — that
  would gut the thesis.

Assumptions stated: Elias-Fano random access from mmap without full-block
decompression is achievable in Rust (strong literature support [R5][R9],
but our implementation is unwritten); degree-ranking's cache benefits
survive the strata boundaries; the manifest-polynomial claim for NodeSim
is the least certain (Experiment X1 targets exactly it).

---

## Timeline A: GRAIN v0 ("the manifest is the estimator")

- **Opening move:** Write the format spec FIRST (20 pages max), including
  the workspace polynomial for each of the seven families as a spec
  appendix — the estimator is reviewable before any Rust exists.
- **Week 1:** Spec + fixture generator. First fight: Elias-Fano select/
  rank primitives in Rust — take an existing crate or write against the
  paper [R5]. The hot/flat stratum means PageRank can run before EF works.
- **Month 1:** Builder seals gen-0 from CSV; PageRank + WCC run hot+cold;
  first real receipt printed from manifest arithmetic and checked against
  measured RSS. The moment estimate≈actual lands within ±15% is the
  project's proof-of-thesis moment.
- **Quarter 1:** NodeSim's polynomial faces reality (X1): if the degree-
  CDF bound holds, the format thesis is validated end-to-end; Louvain's
  levels-as-generations trick either feels elegant or reveals hidden
  copy costs. EF stratum lands; bytes/edge lands between flat CSR and
  WebGraph-class, by design not accident.
- **Long-term shape (Year 1):** a frozen v0 spec + one engine + the
  benchmark ("estimated on a KB, ran on 16 GB, finished") + the spec
  itself as a publishable artifact (PMF04's wedge).
- **Likelihood:** ~65%. **Kill criterion:** if NodeSim/Louvain workspace
  bounds cannot be expressed as manifest polynomials within ±30% by
  month 2, demote G3 from "estimator" to "hint" and continue as a very
  good stratified format. **Check date:** end of month 2.
- **Stress points:** three encodings = three debuggers; the spec-first
  discipline against the urge to just code.
- **Inflection points:** the first estimate-vs-actual receipt; the
  NodeSim polynomial verdict.
- **Lived experience:** the rare joy of designing a thing whose
  correctness argument fits on paper — punctuated by EF bit-twiddling.

## Timeline B: Compression Monolith ("WebGraph in Rust")

- **Opening move:** Chase bytes/edge: gap coding, referentiation, ζ codes
  per Boldi-Vigna [R4]; one compressed structure for everything.
- **Week 1-Month 1:** Deep compression work; nothing runs yet. The
  literature ceiling (~3 bits/link on web graphs [R4]) is real but was
  earned over years by specialists.
- **Quarter 1:** Superb storage numbers, but decompression cost taxes
  every PageRank iteration (WebGraph's own answer is lazy decompression
  [R4], which is engineering-heavy), and — the killing observation —
  compression does NOT reduce ALGORITHM WORKSPACE, which Arch02 showed is
  where Louvain/NodeSim actually die. Best bits/link, unchanged OOM story.
- **Long-term shape:** a great library (a Rust WebGraph port has
  independent value) attached to an engine that still can't price its
  scratch. **Likelihood of being the right lead:** ~15%. **Kill:** month
  1 if PageRank/iteration cost >2x flat CSR. **Check date:** month 1.
- **Stress/inflection:** specialist-depth work; the workspace realization
  usually arrives via a benchmark, painfully.

## Timeline C: Flat Minimalism ("formalize the POC")

- **Opening move:** Freeze the POC's dual flat CSR + sidecars as v0;
  spend the year on algorithms only.
- **Week 1-Quarter 1:** Fastest possible algorithm progress (the format
  is trivial); five families early. But cold-stratum graphs 3-5x larger
  than RAM have no story (no blocks, no EF), and estimates stay code-
  driven guesses like the incumbent's [R13]. By Q1 the "50GB-class graph
  on 8GB box" PRD headline is quietly unmet.
- **Long-term shape:** a fast small-graph engine — the crowded quadrant
  (embedded engines already live there), differentiation thinnest.
- **Likelihood of right-lead:** ~30%. **Kill:** the day a target workload
  exceeds mmap-friendly working set — which the PRD's own sizing predicts
  in Q1. **Check date:** Q1 benchmark day.
- **Stress/inflection:** none early (that is the trap); the wall arrives
  with the first big-graph user.

## Timeline D: Mutable Hybrid ("freshness inside the format")

- **Opening move:** PMA/hierarchical container per VCSR/Terrace [R8][R10]:
  the format itself absorbs updates.
- **Week 1-Quarter 1:** Order-maintenance machinery (PMA rebalances,
  degree-class promotion) consumes the quarter; the research systems this
  copies were multi-person, multi-year efforts. Meanwhile the PRD already
  solved freshness ARCHITECTURALLY (journal/overlay/base + republication)
  — the format is solving a problem the system design already owns.
- **Long-term shape:** a streaming-graph research artifact; OLAP families
  arrive late and unbudgeted. **Likelihood of right-lead:** ~10%. **Kill:**
  immediate on re-reading the PRD's immutability invariant. **Check
  date:** week 1 (this timeline exists to be killed explicitly).
- **Stress/inflection:** complexity without a customer; violates G5.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A GRAIN v0 | estimation-by-arithmetic is a NEW capability class; spill unit native; spec is itself a PMF asset | 3 encodings of complexity; NodeSim polynomial unproven | high (strata degrade gracefully to C) | LOW-MED | EF crate quality; month-2 polynomial verdict |
| B Compression | best bytes/edge; real library value | workspace problem untouched; specialist time-sink | medium | HIGH as lead (fine as later stratum encoding) | years of compression craft |
| C Flat minimal | fastest algorithm progress | no big-graph story; estimator stays a guess; crowded quadrant | high | medium | nothing — that's the trap |
| D Mutable hybrid | freshness in-format | violates PRD immutability; research-grade complexity | low | HIGH | a multi-person team we don't have |

Composability (the quiet win): B and C are not really rivals — they are
**strata encodings inside A**. Flat CSR is GRAIN's hot stratum; WebGraph-
class compression is a future cold-stratum codec upgrade behind the same
manifest. Only D is genuinely excluded (by invariant, not taste). The fork
is thus "A now, with C as its week-1 subset" versus "C now, A maybe later"
— and the difference is whether the manifest/strata/receipts contracts are
in the v0 spec (cheap now, near-impossible to retrofit once files exist in
the wild).

---

## Decision Filter

**Which path is strongest if everything goes normally?**
**A (GRAIN v0), built inside-out:** week 1 ships the manifest + hot flat
stratum only (= Timeline C's engine, but wearing A's contracts), EF warm
stratum in month 2, cold blocks in month 3. Every algorithm lands budgeted
from day one because the manifest exists from day one.

**Which path is safest if things go badly?**
Still A-inside-out: its worst case degrades to Timeline C *with a
manifest* — strictly better than C proper. B's worst case is a beautiful
library and no engine; D's is a rewrite of the rewrite.

**What experiment would collapse uncertainty fastest?**
```text
X1 (week 1, paper only): derive the NodeSim and Louvain workspace
    polynomials over the manifest fields (degree-CDF knots, stratum
    sizes) and test them against the POC's measured runs on fixtures.
    The thesis lives or dies on this BEFORE any format code is written.
X2 (week 1, 2 days): Rust Elias-Fano spike — mmap a fixture adjacency,
    random-access neighbors, measure ns/lookup vs flat CSR. Prices the
    warm stratum with data [R5][R9].
X3 (week 2, 1 day): block-schedule dry run — replay a WCC convergence
    trace from the POC against a simulated PxQ block grid to measure the
    selective-skip win GridGraph promises [R2] on OUR workload shape.
```

---

## Chain of Verification

| # | question | answer | status |
| --- | --- | --- | --- |
| V1 | Do 2D edge blocks + sliding windows really reduce out-of-core I/O and allow block skipping? | Yes — GridGraph, USENIX ATC'15 [R2]; GraphChi established the disk-based lane, OSDI'12 [R1]; Gem (2025) shows the lane is still state-of-the-art at 42.5B edges [R3]. | verified, web |
| V2 | Is ~3 bits/link real for compressed web graphs? | Yes — WebGraph framework, Boldi & Vigna [R4]; framework maintained on GitHub [R4b]. | verified, web |
| V3 | Is Elias-Fano adjacency random-accessible without full decompression, and does it beat CSR on size? | Yes — quasi-succinct/EF lane [R5]; a 2023 IPDPS paper reports 1.55x compression over CSR with GPU-decompressible access [R9]. | verified, web |
| V4 | Is degree-class-stratified storage a validated idea? | Yes — Terrace (SIGMOD'21) stores neighbors in different structures by degree and wins on skewed graphs [R8]; VCSR uses degree-aware gap placement [R10]. | verified, web |
| V5 | Does Neo4j's estimator have the "production-tier only" limitation GRAIN's G3 targets? | Yes — GDS memory-estimation docs: only production-tier algorithms are guaranteed an .estimate mode [R13]. | verified, web |
| V6 | Is the "manifest polynomial" claim proven for the state-heavy families? | NO — it is the document's central creative bet; X1 exists to test it on paper in week 1, before code. | honest-uncertainty (the load-bearing one) |

## One-Sentence Summary

```text
Greenfield lets us invert the industry's order of operations — design the
storage format around ESTIMATION instead of estimating around a format —
so GRAIN v0 stratifies vertices by degree rank (flat-CSR hot, Elias-Fano
warm, GridGraph-blocked cold), makes the manifest a KB-sized closed-form
estimator for all seven adoption families, and the timelines say: build it
inside-out (manifest + hot stratum first, so week 1 already runs), test
the NodeSim polynomial on paper before writing format code, and treat
compression and flat-CSR not as rival formats but as strata codecs the
manifest was born to govern.
```

---

## References

Accessed 2026-07-08. External claims are tagged [R#]; untagged content is
design/judgment. Readers should verify independently.

- **[R1]** GraphChi: Large-Scale Graph Computation on Just a PC (OSDI'12) — disk-based sharded processing on a single machine:
  https://www.usenix.org/system/files/conference/osdi12/osdi12-final-126.pdf
- **[R2]** GridGraph: Large-Scale Graph Processing on a Single Machine Using 2-Level Hierarchical Partitioning (USENIX ATC'15) — 2D edge blocks, dual sliding windows, selective scheduling:
  https://www.usenix.org/system/files/conference/atc15/atc15-paper-zhu.pdf
- **[R3]** Gem: Scalable Monotonic Graph Processing Beyond Billion-Scale on a Single Machine (2025) — out-of-core engine, 42.5B-edge ClueWeb, up to 135x over GridGraph:
  https://doi.org/10.1145/3769795
- **[R4]** The WebGraph Framework: Compression Techniques (Boldi & Vigna) — ~3 bits/link, gap coding, ζ codes, lazy decompression:
  https://vigna.di.unimi.it/algoweb/WebGraph.pdf
- **[R4b]** WebGraph reference implementation (GitHub):
  http://github.com/vigna/webgraph/
- **[R5]** Quasi-succinct indices / Elias-Fano monotone-sequence encoding (Vigna) — random access into compressed monotone id lists:
  https://arxiv.org/abs/1206.4300
- **[R6]** Ligra: A Lightweight Graph Processing Framework for Shared Memory (PPoPP'13) — the shared-memory flat-representation baseline:
  https://www.cs.cmu.edu/~guyb/papers/SB13.pdf
- **[R7]** GraphBLAST: A High-Performance Linear Algebra-based Graph Framework on the GPU (arXiv:1908.01407) — the algebraic-substrate lane GRAIN treats as an optional backend:
  https://arxiv.org/abs/1908.01407
- **[R8]** Terrace: A Hierarchical Graph Container for Skewed Dynamic Graphs (SIGMOD'21) — degree-stratified neighbor storage:
  https://itshelenxu.github.io/files/papers/terrace-sigmod-21.pdf
- **[R9]** Traversing Large Compressed Graphs on GPUs (IPDPS'23) — Elias-Fano-based format, 1.55x smaller than CSR, decompression-free traversal:
  https://doi.org/10.1109/ipdps54959.2023.00013
- **[R10]** VCSR: Mutable CSR Graph Format Using Vertex-Centric Packed Memory Array (CCGRID'22) — the mutable-CSR lane Timeline D copies (and G5 rejects):
  https://webpages.charlotte.edu/ddai/data/dong-ccgrid-22.pdf
- **[R13]** Neo4j GDS Memory Estimation docs — `.estimate` mode; production-tier-only guarantee:
  https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/

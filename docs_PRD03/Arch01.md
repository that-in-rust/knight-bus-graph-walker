# Arch01: Possible Architectures For Rewriting Neo4j In Rust (OLAP-Focused)

Date: 2026-07-08
Method: Timeline Traverser — simulate plausible futures per architecture, then
compare. This is a decision-simulation document, not a decision decree. It
builds on `prd-l1.md`, `Arch-options.md`, `SUM01.md`, the reference-learning
shelf, the implementation-readiness packet, and `docs_PRD02` (format variants,
CSR-tiles diligence, GDS surface matrices).

The main focus is the **OLAP side**: how to think about the snapshot plane in
novel ways, and what each way costs over time.

---

## Part 0: Core Facts Enumeration

Before simulating futures, enumerate what we actually know. Every architecture
below must survive these facts.

### Facts from the PRD (prd-l1.md)

```text
F01. Product = Neo4j-compatible Rust rewrite; zero client-code changes where
     support is claimed.
F02. RAM target = 50 GB-class logical graph practical on 8 GB-class machine.
F03. Three planes: Neo4j-shaped OLTP truth | Projection Build Store (factory,
     never user-queried) | published immutable OLAP snapshots.
F04. OLAP answers are exact as of a published watermark W. Freshness improves
     only by publishing W+1. No query-time write merging. Ever.
F05. Publication is atomic: readers see all of W or all of W+1.
F06. RAM accounting is holistic: heap + RSS + page cache + mmap residency +
     build scratch + sidecars + result/model artifacts + algorithm state.
F07. Strict-RAM plans must reject BEFORE execution if the budget cannot fit.
F08. Single-node community target.
F09. Claims are proven by harnesses/falsifiers, not prose.
```

### Facts from the evidence shelf (reference-learning/, docs_PRD02)

```text
F10. The visible GDS surface is ~575 gds.* procedures. Topology alone covers
     almost none of it; configs, catalogs, estimates, mutate/write/stream/stats
     modes, results, and model artifacts are the real surface.
F11. GDS itself is already "projection-plane" shaped: CSRGraphStore, graph
     catalog keyed by user/db/name, compressed/packed adjacency lists,
     compositional memory estimator (perNode, perNodeVector, perThread, ...).
F12. Neo4j's record store (15 B node, 34 B rel, 41 B property records, chain
     traversal, dense-node groups) is good OLTP engineering and a poor OLAP
     read primitive. A raw CSR peer entry is ~4 bytes.
F13. Knight Bus v002 already proved: immutable dual CSR + mmap gives lower
     runtime RSS than Neo4j on 1 MB / 50 MB / 2 GB fixed-hop traversal tests.
F14. docs_PRD02 verdict on durable formats: NOT one format, NOT thirteen
     per-algorithm formats — three families:
       CompressedDualCsrBaseV1 (topology),
       MmapColumnarPropertyPlaneV1 (typed properties/features),
       BoundedScratchAndSidecarV1 (scratch + results),
     plus opt-in HotMaterializedArtifactV1.
F15. Representative kernel tracing (Batch 07): PageRank/BFS/WCC fit
     "flat CSR + sidecars + bounded scratch". FastRP fits only if embeddings
     are heavyweight sidecar artifacts. NodeSimilarity is the first family
     that pressures toward selective spill / pruning / GraphBLAS-style moves.
F16. Cells (partitioned/tiled CSR) are a falsifiable hypothesis with concrete
     adopt/postpone/reject thresholds (boundary-edge ratio <15% adopt / >35%
     reject; metadata overhead <5%; global-stream slowdown <10%; dirty-region
     rebuild <5% of topology bytes for 10K updates).
F17. mmap does not give deterministic RAM; residency counts. Strict mode needs
     explicit buffers/O_DIRECT-style accounting.
F18. Low-RAM precedents worth stealing from: GraphChi/X-Stream (out-of-core
     shards/streams), Ligra/GAPBS (flat array kernels), WebGraph/BV
     (compressed topology), Kuzu (disk-first CSR as durable storage),
     GraphBLAS (algorithms as sparse linear algebra).
F19. Arch-options.md ledger already ranks: A (flat CSR) MVP baseline,
     B (+sidecars) required, E (generation catalog) required ops layer,
     C (cells) measured evolution, D (hybrid publication) mature direction.
F20. The critique verdict: research shelf is strong; implementation contract
     is thin. The registry (575 rows), memory formula book, publication state
     machine, and canary matrix exist as stubs/contracts to be filled.
```

### What is genuinely still open (the real fork)

The PRD fixes the three-plane boundary. It deliberately does NOT fix:

```text
O1. The physical shape of the OLAP snapshot (flat, compressed, tiled, multi).
O2. The execution model of the OLAP runtime (mmap pull, streamed push,
     linear-algebra kernels, budget-admitted plans).
O3. How the 575-procedure surface maps onto storage (one substrate + planes,
     or per-family artifacts).
O4. How strict-RAM mode is actually enforced (mmap trust vs explicit buffers).
O5. Whether the snapshot is ONE artifact or a PORTFOLIO of artifacts compiled
     from the same Build Store watermark.
```

Those five open questions are what the architectures below explore.

---

## Decision Frame

- **Fork in the road:** Given the fixed pipeline `OLTP -> Build Store ->
  published snapshot -> OLAP runtime`, what should the snapshot + runtime
  actually BE, physically and operationally, to honestly serve 575 GDS
  procedures within 8 GB?
- **Desired outcome:** A credible, testable OLAP plane where every procedure
  either runs within an explicit RAM estimate or rejects deterministically;
  measured wins vs Neo4j+GDS on 8 GB hardware; zero client changes.
- **Hard constraints:** F01-F09 above. Snapshot-only serving. Atomic
  generations. Holistic RAM. Single node.
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1 of implementation
  effort (assume one strong implementer + agents, part-time reviewers).
- **What counts as failure:**
  - an OLAP demo that only does BFS/PageRank and hand-waves the other 570
    procedures;
  - RAM claims that die under holistic accounting (page-cache storms, hidden
    result heaps);
  - a middle layer that quietly becomes a queryable database;
  - a benchmark victory on 2 GB that collapses at 50 GB.

Five timelines are simulated. A and B are the "sanctioned" paths from
Arch-options.md; C, D, and E deliberately explore the novel OLAP framings the
user asked for.

---

## Timeline A: The Monolith Snapshot (Flat Dual CSR + Sidecars + Generations)

> "One canonical topology, planes around it, editions over time."
> This is Arch-options A→B→E executed in order — the sanctioned baseline.

### Shape

```text
Build Store at watermark W
   |
   v
+--------------------------------------------------+
| generation N (immutable directory)               |
|   manifest.json      (watermark, checksums)      |
|   fwd.offsets / fwd.peers   (dual CSR)           |
|   rev.offsets / rev.peers                        |
|   idmap.*            (external <-> dense u32/u64)|
|   sidecars/labels.*  sidecars/reltypes.*         |
|   sidecars/props/<key>.<type>  (columnar, mmap)  |
|   sidecars/results/<algo-run>.*                  |
+--------------------------------------------------+
   |
   v
OLAP runtime: mmap windows + bounded scratch + cursor API
```

- **Opening move:** Freeze the snapshot manifest format v1. Port the v002 dual
  CSR into the generation-directory shape with a watermark field. Implement
  atomic `active_generation` swap (Option E semantics) immediately, not later.
- **Week 1:** Dual CSR + idmap + manifest compile from a Build Store fixture.
  Generation swap test passes (reader sees N or N+1, never both). BFS/degree
  run against generation N. Morale is high because v002 code is being reused.
- **Month 1:** Label/reltype/weight sidecars land. PageRank, WCC, BFS run with
  bounded scratch planes and report `as_of_watermark`. The memory formula book
  gets its first ~20 real rows (perNode scratch formulas verified against RSS
  measurements). First honest 8 GB test with a ~10 GB logical graph.
- **Quarter 1:** The property plane matures (typed columns, null bitmaps,
  vector/feature columns for FastRP). The GDS registry stops being a stub:
  every P0 procedure is Implemented or RejectsWithEstimate; everything else is
  RegisteredUnsupported with deterministic errors. The 50 GB corpus build
  works but the BUILD path becomes the pain: external sorts in the Build
  Store, spill budgets, checkpointed compiles.
- **Long-term shape (Year 1):** A boring, trustworthy engine. Global-scan
  algorithms are excellent. The weak spots calcify: NodeSimilarity-class
  procedures (F15) either reject on 8 GB or need per-procedure spill hacks
  bolted on; page-cache behavior under concurrent OLAP jobs is folklore
  ("run one heavy algo at a time"); strict-RAM mode is half-real because mmap
  residency (F17) was never brought under explicit control.
- **Likelihood this path is available:** ~95%. Everything needed is already
  proven in v002 or specified in implementation-readiness.
- **Stress points:** The moment holistic accounting (F06) meets mmap reality:
  RSS looks fine, the box still swaps because page cache is thrashing between
  fwd.peers and a 40 GB property column. Also the "570 other procedures"
  dread — the monolith gives no leverage on the long tail; each hard family is
  a fresh fight.
- **Inflection points:**
  1. When NodeSimilarity/kNN arrive: bolt on ad-hoc spill (stay in A) or admit
     a budget-first executor is needed (jump toward Timeline C).
  2. When update-locality complaints arrive: cells falsifier (F16) either
     fires or does not — deciding whether D-hybrid ever happens.

### Lived experience

Calm, incremental, demo-friendly. Every week produces something runnable. The
anxiety is deferred, not removed: the engineer knows the long tail and the
strict-RAM promise are both unpaid debts, and quarter reviews keep asking
"so when does gds.nodeSimilarity work on 8 GB?"

---

## Timeline B: The Tilehouse (Cellular / Tiled CSR As The Primary Snapshot)

> "The snapshot is a warehouse of bounded cells, not one big slab."
> Arch-options C promoted to the default, plus docs_PRD02 CSR-tiles diligence.

### Shape

```text
Build Store at W  ->  partitioner (degree-aware / label-aware)
   |
   v
+----------------------------------------------------------+
| generation N                                             |
|   global-manifest (watermark, cell map, boundary index)  |
|   cell_0000/ passport + local dual CSR + local sidecars  |
|   cell_0001/ ...                                         |
|   boundary/  cut-edge index + halo tables                |
|   stream/    logical global edge stream adapter          |
+----------------------------------------------------------+
   |
   v
runtime: opens ONLY the cells a query touches; global algos
run over the stream adapter cell-by-cell (GraphChi-style)
```

- **Opening move:** Build the partition lab inside the Build Store first
  (Arch-options already names this a Build Store use). Choose a first
  partitioning heuristic (e.g. degree-ordered ranges, or label-major).
- **Week 1:** Slow start. Nothing user-visible runs; the week is spent on cell
  passports, boundary-edge bookkeeping, and the manifest schema. The flat-CSR
  oracle from v002 is kept alive purely as the correctness comparator.
- **Month 1:** First two-layout parity test: cellular snapshot equals flat
  oracle at W, edge for edge. BFS restricted to one cell is beautifully cheap.
  Global PageRank over the stream adapter works but measures 12-30% slower
  than flat, depending on partition quality — exactly the falsifier band F16
  warned about.
- **Quarter 1:** Reality bites in three ways. (1) Boundary-edge ratio on
  social-graph-like fixtures lands at 25-45% for naive partitions; getting
  under 15% requires real partitioning science (metis-like passes inside the
  Build Store) which eats the quarter. (2) Metadata overhead creeps toward the
  20% reject threshold. (3) Every sidecar now needs a per-cell story, tripling
  format surface. Meanwhile the GDS registry has barely moved.
- **Long-term shape (Year 1):** Two futures, sharply divided:
  - *Partitioning wins* (graphs with strong community structure, workloads
    dominated by local/egonet/catalog-subgraph queries): bounded rebuilds
    make freshness cadence dramatically better — 10K OLTP changes recompile
    2-3 cells, not 50 GB. Cells also become the natural RAM budget unit
    ("this plan opens ≤ 12 cells ≈ 900 MB"). This is a genuinely novel
    budgeting story flat CSR cannot tell.
  - *Partitioning loses* (power-law graphs, global-scan workloads): the
    project spent two quarters building a warehouse for tenants who never
    arrived, and the falsifier plan forces a humiliating retreat to flat.
- **Likelihood of the winning branch:** ~30-40% as the DEFAULT architecture
  (per Batch 07: traced algorithm semantics do not force cells). Much higher
  (~70%) as a LATER additive layout under Timeline D/E framing.
- **Stress points:** Constant parity anxiety (cells vs flat oracle); the
  partitioner becomes a research project inside an engineering project; every
  new GDS family asks "and how does this work across cell boundaries?"
- **Inflection points:**
  1. First falsifier measurement (F16). If boundary ratio >35% with no
     locality win, the timeline should be aborted — the plan literally says
     so. Ignoring that number is how this timeline becomes a disaster.
  2. If dirty-region rebuild wins big, cells may justify themselves purely as
     a BUILD/publication accelerator even if serving stays flat — an exit
     ramp into Timeline D.

### Lived experience

Intellectually thrilling, operationally heavy. Weeks of invisible work; the
demo gap is real and demoralizing. If the partition numbers come back good,
vindication; if not, sunk-cost gravity becomes the biggest project risk.

---

## Timeline C: The Budget Machine (Admission-Controlled, Out-Of-Core Executor)

> NOVEL FRAMING: the snapshot format is boring; the RUNTIME is the product.
> "Every OLAP query is a loan application against an 8 GB bank."
> Steals from GraphChi/X-Stream (F18) + the GDS estimator (F11) + F07.

### Shape

```text
             every procedure call
                     |
                     v
        +---------------------------+
        | MEMORY PLANNER / ADMITTER |
        | inputs: formula book row, |
        |  graph stats from Build   |
        |  Store, configured budget |
        +------+-------------+------+
               |             |
        fits in budget    does not fit
               |             |
               v             v
        +-------------+  +----------------------------+
        | IN-CORE     |  | OUT-OF-CORE PLAN           |
        | mmap/array  |  | shard-streamed execution:  |
        | kernels     |  |  partition scratch to disk,|
        | (Ligra-ish) |  |  stream edges in passes,   |
        +-------------+  |  spill frontiers/pairs     |
                         +----------------------------+
               |             |
               v             v
          result sidecar + as_of_watermark + memory receipt
```

Physical storage stays deliberately simple: flat dual CSR + property plane
(Timeline A's bytes). The novelty is that EVERY procedure runs through an
admission controller, and every procedure has TWO implementations classes:
an in-core kernel and a degraded-but-honest out-of-core plan (or a
deterministic rejection). The unit of architecture is the *execution plan
under a byte budget*, not the file format.

- **Opening move:** Implement the memory receipt: every algorithm run returns
  `{estimated_bytes, peak_rss_observed, budget, verdict}`. Wire the formula
  book TSV directly into the runtime as the admission table.
- **Week 1:** PageRank runs three ways on purpose: (a) full in-core, (b)
  artificially budget-capped so score vectors spill per-range to disk, (c)
  rejected at admission with a correct estimate. This trichotomy IS the demo.
- **Month 1:** The estimator vocabulary (perNode, perNodeVector, perThread —
  stolen shamelessly from GDS, F11) exists as a Rust combinator library.
  Strict mode replaces "trust mmap" with windowed explicit reads for the
  streaming path (answers F17/O4 head-on). BFS/WCC get frontier-spill plans.
- **Quarter 1:** The first genuinely hard family — NodeSimilarity — becomes
  the flagship instead of the shame: pair-candidate generation runs in
  bucketed passes with bounded heaps and disk spill, slow but HONEST on 8 GB
  where GDS would OOM. That benchmark ("we finish; they die") is the entire
  marketing story of the product (F02). The cost: every procedure needs plan
  classification work, and out-of-core variants are 3-10x slower than
  in-core, which must be communicated, not hidden.
- **Long-term shape (Year 1):** The engine develops a three-tier procedure
  taxonomy that maps beautifully onto the support registry (F20):
  `InCore | OutOfCore(cost-class) | RejectsWithEstimate`. The 575-row registry
  stops being documentation and becomes a runtime dispatch table. Storage
  evolution (cells, compression) can be adopted later WITHOUT disturbing the
  admission model, because plans, not formats, are the architecture.
- **Likelihood:** ~60% this becomes load-bearing regardless of which storage
  timeline wins, because F07 (reject-before-execute) is a non-negotiable PRD
  row that only this timeline treats as the center of gravity.
- **Stress points:** Formula accuracy is a grind — every estimate must be
  falsified against measured RSS (F09), and allocator/page-cache noise makes
  clean measurement hard. Two implementations per hot family is real cost.
  The temptation to let "estimate ≈ vibes" must be resisted forever.
- **Inflection points:**
  1. First formula that lies badly in production (estimate 2 GB, RSS 6 GB):
     either the holistic-accounting harness catches it (timeline survives) or
     users catch it (credibility of the whole RAM promise dies).
  2. Whether out-of-core plans can share machinery (a generic
     shard-stream/spill substrate) or degenerate into 40 bespoke hacks.

### Lived experience

Less visually demo-able than A ("it rejected correctly!" is a weird demo) but
psychologically calm: there is no deferred dread, because the scary families
are confronted first. Engineers describe progress in coverage percentages of
the registry, which management actually understands.

---

## Timeline D: The Snapshot Foundry Portfolio (Multi-Layout Compiler Targets)

> NOVEL FRAMING: stop asking "which format wins" — the Build Store is a
> COMPILER, so let it emit MULTIPLE specialized artifacts per watermark and
> let a planner choose. Arch-options D generalized from {flat, cells} to a
> real portfolio, governed by promotion/eviction economics.

### Shape

```text
Build Store facts at watermark W
   |
   |  one IR, many backends (like LLVM targets)
   +-> flat dual CSR            [always built: canonical + oracle]
   +-> property/feature plane   [always built]
   +-> BV-compressed topology   [built when RAM-starved: WebGraph-style,
   |                             2-4x smaller peers at decode-cost]
   +-> degree-ordered relabeled CSR   [built when triangle/similarity hot]
   +-> cell packages            [built when falsifier F16 passes]
   +-> per-algo hot artifacts   [PROMOTED by usage, e.g. sorted-edge list
   |                             for MST, RR-set sidecars for influence]
   v
generation N manifest lists ALL artifacts + their parity proofs
planner: (procedure, budget, stats) -> cheapest admissible artifact set
```

Key governance rule (this is what keeps it from becoming the rejected
"thirteen persistent layouts" disaster of docs_PRD02/F14): artifacts beyond
the canonical pair are **promoted only by measured usage** and **evicted when
cold**, like a query-plan cache but for bytes on disk. The Atlas's 13 families
become the *promotion menu*, not the default build.

- **Opening move:** Build nothing new physically. Build the MANIFEST that can
  describe a multi-artifact generation, plus the parity-proof slot per
  artifact ("this artifact provably equals the canonical flat stream at W").
- **Week 1:** Flat CSR + property plane inside the portfolio manifest. The
  planner is an if-statement. This is Timeline A wearing a bigger coat —
  deliberately, so the option value is nearly free.
- **Month 1:** Second real target lands: BV-style compressed peers for the
  reverse CSR (biggest file, coldest access). A 50 GB logical graph's
  topology drops toward ~15-25 GB on disk, page-cache pressure drops with it
  — a direct, measurable F02 win. Planner learns its first real rule:
  "PageRank pull-iteration reads rev; use compressed rev when budget-tight,
  decode cost be damned."
- **Quarter 1:** Promotion economics go live: usage telemetry (per-procedure
  run counts, bytes faulted) drives building the degree-ordered relabeled
  variant for triangle counting; it wins ~2-4x on wedge intersection and
  justifies its disk. The eviction side is tested by turning workloads off.
  Cells enter ONLY as a portfolio member behind the falsifier gates. Disk
  footprint grows to 1.5-2.2x canonical — acceptable on disk-rich boxes,
  painful on laptops — so the portfolio gets a disk budget too.
- **Long-term shape (Year 1):** The Build Store fulfills its foundry destiny
  (F03): one verified fact plane, many casts. Hard GDS families each get the
  bytes they actually want (F14's HotMaterializedArtifactV1, made systematic).
  The risk that materialized: planner complexity. Choosing among 4-6 artifact
  types × budgets × procedures needs its own falsifier harness ("planner never
  picks a plan worse than 25% off optimal"), and parity testing every artifact
  against the canonical stream at every watermark is a permanent CI tax.
- **Likelihood:** ~50% as the year-one endpoint; ~80% as the year-three
  endpoint of ANY successful version of this product, because portfolio
  publication is where A, B, and C all converge when they mature.
- **Stress points:** Combinatorics. Every new artifact multiplies build time,
  parity tests, disk, and planner states. Without ruthless promotion/eviction
  discipline this becomes the thirteen-layout graveyard with extra steps.
- **Inflection points:**
  1. The first artifact whose parity test fails in CI — does the culture
     respond "evict it" (healthy) or "special-case it" (rot begins)?
  2. Disk-budget collisions on small machines: the portfolio must degrade to
     canonical-only gracefully, or the 8 GB story breaks on the 256 GB-SSD
     axis instead of the RAM axis.

### Lived experience

Feels like running a small compiler team. Satisfying flywheel once telemetry
drives promotion ("the system tunes its own storage"), but the CI matrix and
manifest bookkeeping generate a steady hum of maintenance anxiety.

---

## Timeline E: The Algebra Engine (GraphBLAS-Style Kernel Substrate)

> NOVEL FRAMING: OLAP procedures are not 575 bespoke programs — most are
> sparse-linear-algebra expressions over a small kernel set. Build the kernels
> once; compile procedures onto them. The snapshot is a sparse MATRIX, the
> runtime is a tiny BLAS, and GDS surface = a standard library.

### Shape

```text
snapshot = dual CSR viewed as sparse matrix A (and A^T)
         + property plane as vectors/masks

kernel substrate (the ~8 verbs):
  SpMV / SpMSpV (masked)       -> PageRank, eigen, HITS, label prop
  frontier expand w/ mask      -> BFS, SSSP (semiring: min-plus)
  union-find / hook-compress   -> WCC, SCC pieces
  masked SpGEMM / wedge join   -> triangles, similarity candidates
  scan/reduce over vectors     -> degree, stats, centralities
  sample/walk stream           -> FastRP, node2vec walks
  bucketed peel                -> k-core, degeneracy
  external sort/merge          -> MST, ordering passes

procedure layer: gds.pageRank = (semiring, damping loop, convergence check)
                 over SpMV kernel + perNode vectors — ~200 lines, not 5000.
```

- **Opening move:** Define the semiring-parameterized SpMV and masked-frontier
  kernels over the existing mmap dual CSR, with the scratch plane as the
  vector arena. Prove PageRank, BFS, and WCC are each expressible in <300
  lines on top.
- **Week 1:** The three flagship algorithms run on two kernels. Code size is
  startlingly small; correctness parity against v002 outputs passes. Everyone
  is briefly euphoric.
- **Month 1:** The honeymoon meets the long tail. Louvain/Leiden (partition
  refinement), betweenness (Brandes accumulation), HDBSCAN, and the ML
  pipelines resist clean algebraic form; they need the escape hatch: kernels
  plus imperative "custom op" procedures. The architecture becomes
  "algebra where possible, bespoke where necessary" — still a win (maybe 60%
  of the surface algebraic), no longer a silver bullet.
- **Quarter 1:** The compounding benefits appear where nobody demoed them:
  - every kernel gets ONE memory formula, so admission control (Timeline C)
    covers whole procedure families at once;
  - every kernel gets ONE out-of-core variant (streamed SpMV over edge
    shards), so budget-degradation is inherited, not rewritten;
  - kernel-level SIMD/parallelism improvements lift dozens of procedures
    simultaneously.
  The cost also appears: debugging "why is Leiden 4x slower than GDS" now
  means reasoning through kernel plans instead of straight-line code, and
  masked-SpGEMM performance work is genuinely hard systems programming.
- **Long-term shape (Year 1):** The registry's 575 rows classify into
  `AlgebraicOnKernels | CustomOp | Rejected`, and coverage grows by kernel,
  not by procedure — the only framing under which "all of GDS" is a tractable
  sentence for a tiny team. Interop bonus: the kernel substrate is exactly
  where suitesparse-style or SIMD crates can be swapped in later.
- **Likelihood:** ~45% as the primary runtime organization; near-100% that at
  least the SpMV/frontier kernels exist in any successful version (they are
  the hot loops regardless of what they are called).
- **Stress points:** Abstraction tax under profiling; the temptation to
  contort naturally-imperative algorithms into ugly algebra (resist: use
  CustomOp); explaining the architecture to contributors raised on
  per-algorithm codebases.
- **Inflection points:**
  1. Masked SpGEMM performance on similarity workloads: if it cannot get
     within ~2x of bespoke wedge code, the algebra layer stays a convenience,
     not the substrate.
  2. Whether the escape hatch stays <40% of procedures. Beyond that, the
     substrate is decoration and Timeline E collapses into Timeline A with
     extra vocabulary.

### Lived experience

The most intellectually satisfying path and the hardest to staff. Progress is
lumpy: weeks of kernel work, then ten procedures light up in a day. Requires
protecting the team from "just write it the normal way" pressure during the
lumps.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A Monolith (flat CSR+sidecars+generations) | Fast start, reuses v002, sanctioned by Arch-options, demo-friendly, lowest complexity | No leverage on hard families or strict RAM; long-tail debt compounds silently | High — its bytes are the canonical core of every other timeline | Low early, MEDIUM late (quarter-3 wall on NodeSimilarity-class + strict mode) | Almost nobody; Build Store fixtures and CI |
| B Tilehouse (cells primary) | Bounded rebuilds, cell-sized RAM budgeting, locality wins IF graphs cooperate | Partitioning research risk; parity tax; metadata overhead; slow demos; F16 may simply say no | Medium — flat oracle survives, but 1-2 quarters may be sunk | HIGH as default; low as later portfolio member | The GRAPHS themselves (community structure), partition quality, falsifier discipline |
| C Budget Machine (admission + out-of-core) | Directly pays PRD rows F02/F06/F07; turns the scariest families into the flagship; registry becomes runtime | Formula-verification grind; dual implementations; weak visual demos | High — admission layer sits ABOVE any storage; portable across A/B/D/E | LOW — hard to imagine a future where reject-before-execute wasn't needed | Measurement harnesses; benchmark hardware; formula discipline |
| D Foundry Portfolio (multi-layout targets) | Foundry destiny of the Build Store; each family gets its bytes; storage self-tunes via promotion/eviction | Planner + parity + disk combinatorics; thirteen-layout graveyard risk without discipline | Medium — individual artifacts evictable, but manifest/planner machinery is sticky | Medium — regret concentrates in over-eager artifact promotion | CI capacity, disk budgets, telemetry, eviction discipline |
| E Algebra Engine (kernel substrate) | Only tractable framing for 575 procedures with a tiny team; formulas/out-of-core/SIMD inherited per-kernel; tiny procedure code | Long tail resists algebra (~40% custom ops); abstraction tax; staffing/explanation cost | Medium-low — kernel-shaped code permeates everything above the bytes | Medium — worst case it decays into a nice internal library | Kernel performance engineering; team buy-in; profiling honesty |

### Variance and inflection summary

```text
Lowest-variance path:      A  (you know what you get, and what you don't)
Highest-variance path:     B  (graph structure decides, not you)
Highest option value:      C and E (both survive ANY storage decision)
Convergence attractor:     D  (mature versions of A/B/C/E all end up
                               publishing a portfolio per watermark)
Cheapest-to-buy insurance: A's bytes + C's admission receipts, from day one
```

The deep observation the simulation surfaces: **A, B, D are answers to
"what are the bytes?" while C, E are answers to "what is the runtime?" — they
are not actually competitors.** The real architecture space is a 2-axis grid:

```text
                     RUNTIME AXIS
                bespoke      kernels(E)
              +-----------+-----------+
   flat (A)   |  A-plain  |   A+E     |
BYTES         +-----------+-----------+     ...and EVERY cell can wear
AXIS  cells(B)|  B-plain  |   B+E     |     C's admission controller,
              +-----------+-----------+     because C is a policy layer,
   portfolio  |  D-plain  |   D+E     |     not a format or a kernel.
      (D)     +-----------+-----------+
```

Arch-options.md already chose the bytes axis (flat -> +sidecars -> portfolio
if measured). What it never chose — and what this document argues is the more
consequential OLAP decision — is the runtime axis and the admission layer.

---

## Decision Filter

**Which path is strongest if everything goes normally?**
The composite: **A's bytes + C's admission layer from week one + E's kernels
grown underneath the flagship algorithms + D's manifest shape (portfolio-ready
even while it contains only two artifacts)**. Concretely:

```text
1. Publish generations of {flat dual CSR + property plane} in a manifest that
   can already list multiple artifacts (D's coat, A's body).        [week 1]
2. Every procedure call goes through the admission controller and returns a
   memory receipt; the formula book is a runtime table, not a doc.  [week 1-2]
3. Implement PageRank/BFS/WCC as semiring kernels, not bespoke loops; keep a
   CustomOp escape hatch and feel zero shame using it.              [month 1]
4. Cells and compressed/reordered variants enter ONLY through the falsifier
   gates and D's promotion/eviction economics.                      [when data says]
```

**Which path is safest if things go badly?**
A-plain. If the team shrinks or deadlines compress, ship A with C's rejection
verdicts (`RejectsWithEstimate` for everything hard). An engine that runs 40
procedures honestly and rejects 535 deterministically is PRD-compliant (F10 +
Support-Status semantics) and credible. An engine that pretends to run 200 and
OOMs on 8 GB is dead on arrival.

**What experiment or conversation would collapse uncertainty fastest?**
Three experiments, ordered by information-per-week:

```text
X1 (1 week): NodeSimilarity spike on the 2 GB corpus under a 1 GB scratch
    budget: in-core vs bucketed-spill vs reject. This single experiment
    prices Timeline C's central bet (out-of-core honesty) on the family
    Batch 07 already identified as the architecture-breaker.
X2 (1 week): BV-compress the reverse CSR of the largest existing snapshot;
    measure PageRank wall-time + page-cache faults vs raw. Prices D's first
    non-canonical artifact and the whole 50-GB-on-8-GB compression story.
X3 (2 weeks): Run the cells falsifier (F16) partition experiment on ONE
    realistic power-law fixture. If boundary-edge ratio lands >35%, Timeline
    B is dead as a default for that graph class and everyone stops arguing
    about it; if <15%, cells earn a portfolio slot. Either result ends the
    longest-running debate in the ledger with a number.
```

**One-sentence recommendation:**

```text
Keep flat CSR + sidecars + generations as the bytes (per Arch-options), but
make the NOVEL OLAP commitments on the runtime side: an admission-controlled,
receipt-emitting, out-of-core-capable executor (C) organized around a small
kernel algebra (E), published through a portfolio-shaped manifest (D) whose
extra artifacts — cells included — must buy their disk with falsifier-passed
measurements (B under F16 gates).
```

---

## Appendix: Where each timeline touches existing artifacts

| timeline | primary existing artifacts it consumes or fills |
| --- | --- |
| A | `Arch-options.md` options A/B/E; `Snapshot-Publication-State-Machine.md`; v002 dual-CSR runtime |
| B | `Cells-Adoption-Falsifier-Plan.md`; `V003-Diligence-CSR-Tiles-GDS-Surface.md` (docs_PRD02); Build Store partition lab |
| C | `Memory-Estimate-Formula-Book.tsv`; `Support-Status-Runtime-Semantics.md`; PRD strict-RAM row; Batch 07 NodeSimilarity finding |
| D | Build Store "multi-target compiler source" + "offline optimizer" uses in `Arch-options.md`; `HotMaterializedArtifactV1` (docs_PRD02); Atlas 13-family menu |
| E | Batch 07 kernel tracing; Batch 10 estimator vocabulary; `GDS-Procedure-Support-Registry.tsv` (as the dispatch table) |

Assumptions stated (per playbook guardrails): team size ~1-3; hardware truth
is an 8 GB box with commodity SSD; workload mix unknown (hence X3); no
external funding pressure forcing zero-lag OLAP; the OLTP plane and Build
Store proceed in parallel and deliver fixtures/watermarks on schedule. If any
of these break, re-run the timelines — especially B, whose fate belongs to the
graphs, not to us.

# Arch02: The Adoption-Driving Algorithms × The Arch01 Architectures

Date: 2026-07-08
Method: internet research first (which algorithms actually drive Neo4j/GDS
adoption), then Timeline Traverser simulation of how each Arch01 architecture
(A Monolith, B Tilehouse, C Budget Machine, D Foundry Portfolio, E Algebra
Engine) lives with exactly those algorithms over week 1 / month 1 / quarter 1 /
year 1. Companion to `Arch01.md` and `SUM01.md`.

---

## Phase 0: Deconstruct & Clarify

- **Core objective:** Identify the small set of algorithms that explains ~80%
  of Neo4j GDS adoption, then stress-test each Arch01 architecture against
  precisely that set, over time.
- **Premise check:** Premise is sound, with one honest caveat: Neo4j publishes
  no telemetry ranking of procedure invocations. The "80% set" below is
  therefore triangulated from Neo4j's own use-case selection guides, product
  briefs, benchmark/configuration guides (which algorithms Neo4j chooses to
  benchmark is itself a usage signal), GraphAcademy curricula, and fraud/
  recommendation solution papers. Sources are cited; percentages are informed
  estimates, marked as such. Proceeding with optimized protocol.
- **Execution plan:** Research synthesis -> algorithm shortlist with per-
  algorithm computational signatures -> council framing -> five timelines
  (one per architecture, each walked through the shortlist) -> cross-timeline
  matrix -> decision filter -> chain of verification.

### Council of Experts (Phase 1)

| persona | contribution |
| --- | --- |
| Graph-algorithms researcher | computational signature of each algorithm (memory shape, access pattern, scratch) |
| Database kernel engineer | how each architecture's bytes and runtime serve those signatures |
| Product/GTM analyst | which algorithms actually close deals (fraud, reco, ER) vs which are brochure filler |
| Skeptical Engineer (Devil's Advocate) | attacks the 80% claim, the benchmark-as-usage proxy, and every architecture's happy path |
| Site operator on an 8 GB box | lived reality: swap storms, page-cache fights, "why did my kNN die" tickets |

Skeptical Engineer's standing objections, answered inline throughout:
(1) "80% is unmeasurable" — true; treated as a triangulated estimate, and the
conclusions below are robust to the exact percentages because the shortlist is
stable across every independent source. (2) "Benchmark presence ≠ usage" —
mitigated by requiring each shortlisted algorithm to appear in at least two
independent signal types (use-case guide + benchmark + curriculum).

---

## Part 1: Research — What Actually Drives Neo4j GDS Adoption

### Signals used (sources)

```text
S1. Neo4j GDS Use Case Selection Guide (go.neo4j.com, PDF): maps fraud
    detection, recommendations, entity resolution / customer 360, churn,
    supply chain to algorithm categories.
S2. Neo4j "Exploring Fraud Detection with GDS" blog series: names WCC,
    Louvain, PageRank/degree, KNN/Node Similarity as the working toolkit.
S3. Neo4j GDS Configuration Guide (PDF): the algorithms Neo4j itself
    benchmarks on LDBC100: PageRank, Louvain, WCC, Label Propagation,
    Triangle Count, Local Clustering Coefficient, FastRP, Node Similarity.
S4. Neo4j GDS Product Brief: five headline categories (centrality, community
    detection, similarity, pathfinding, embeddings + graph-ML).
S5. GraphAcademy GDS workshop: teaches PageRank/Degree/Betweenness, Louvain/
    Leiden, WCC, Node Similarity, FastRP as the canonical tour.
S6. Aura Graph Analytics "Explore" integration: ships ONLY centrality +
    community detection in the simplified UI — Neo4j's own judgment of the
    minimum viable algorithm surface.
S7. Fraud/AML solution briefs: WCC for shared-identifier rings, Louvain for
    fraud rings, PageRank for anomalous accounts, KNN for similar-to-known-
    fraudster profiles.
(Web-sourced facts; the user may want to independently verify the PDFs.)
```

### The 80% set (estimate, triangulated)

Seven families, in estimated order of adoption weight:

```text
+----+---------------------------+----------------------------------+-----------+
| #  | Algorithm family          | Flagship procedures              | Est. share|
+----+---------------------------+----------------------------------+-----------+
| 1  | Connected components      | gds.wcc.*                        |  ~20%     |
|    |  (entity res, fraud rings)|                                  |           |
| 2  | Modularity communities    | gds.louvain.*, gds.leiden.*,     |  ~15%     |
|    |  (fraud rings, segments)  | gds.labelPropagation.*           |           |
| 3  | PageRank-class centrality | gds.pageRank.*, articleRank,     |  ~15%     |
|    |  (influence, anomaly)     | degree, betweenness (lighter use)|           |
| 4  | Similarity / KNN          | gds.nodeSimilarity.*, gds.knn.*  |  ~12%     |
|    |  (reco, ER, fraud lookup) |                                  |           |
| 5  | Shortest path / traversal | gds.shortestPath.dijkstra/astar, |  ~10%     |
|    |  (routing, dependencies)  | BFS/DFS, allShortestPaths        |           |
| 6  | Node embeddings           | gds.fastRP.* (dominant),         |   ~8%     |
|    |  (ML feature pipelines)   | node2vec, graphSage (rarer)      |           |
| 7  | Triangle / clustering coef| gds.triangleCount.*, gds.local-  |   ~5%     |
|    |  (cohesion, fraud scoring)| ClusteringCoefficient.*          |           |
+----+---------------------------+----------------------------------+-----------+
                                              cumulative ≈ 85%
```

Everything else — betweenness at scale, link-prediction pipelines, k-core,
HITS, spanning trees, flow, the ML pipeline surface — is the long tail that
matters for compatibility claims (the 575-procedure registry) but not for
adoption volume.

### Computational signatures (what the architectures must actually serve)

| family | access pattern | scratch shape | 8 GB pressure point |
| --- | --- | --- | --- |
| WCC | full edge scan, iterative or union-find | perNode parent array (u32/u64) | trivial scratch; dominated by topology scan I/O |
| Louvain/Leiden | repeated neighborhood scans + graph coarsening | perNode community + degree sums + COARSENED GRAPH COPIES | coarsening copies are hidden multi-GB allocations |
| PageRank | pull over reverse CSR (or push fwd), N iterations | 2× perNode f64 score vectors | rev-CSR page-cache residency across ~20 sweeps |
| NodeSim/KNN | pairwise candidate generation over shared neighbors | candidate pair heaps, top-K per node | QUADRATIC-ish blowup; the known architecture-breaker (Arch01 F15) |
| Shortest path | localized frontier expansion from source | dist/pred arrays + priority queue | tiny for single-source; cheap wins |
| FastRP | few sparse propagation sweeps | perNode × dim f32 embedding matrix | embedding matrix = N×128×4 B; 50 GB-class graph -> tens of GB output |
| Triangle/LCC | wedge intersection, degree-ordered | sorted adjacency views, counters | intersection cost explodes on high-degree hubs |

Neo4j's own LDBC100 numbers make the pressure vivid (S3): PageRank 45.9-110 GB,
Louvain 45.9-119 GB, FastRP 212-254 GB of memory on a 512 GB machine. That is
the incumbent's honest bill — and the product opening for an 8 GB target.

---

## Decision Frame

- **Fork in the road:** Given that ~85% of adoption rides on seven algorithm
  families, which Arch01 architecture serves THOSE SEVEN best on 8 GB — and
  what does each architecture's first year look like if the seven families are
  the roadmap?
- **Desired outcome:** All seven families run (or degrade honestly) on a 50 GB
  logical graph within an 8 GB envelope, with watermark-exact answers, before
  any long-tail work begins.
- **Hard constraints:** PRD facts F01-F09 (see Arch01 Part 0): snapshot-only
  serving, atomic generations, holistic RAM, reject-before-execute, single
  node.
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1, seven-family
  roadmap ordered by adoption weight (WCC first, triangles last).
- **What counts as failure:** any of the top-4 families (WCC, Louvain,
  PageRank, NodeSim) missing or dishonest on 8 GB at year end; or an engine
  that serves the seven but violated the plane boundaries to do it.

Assumption stated: the roadmap deliberately follows adoption weight, not
implementation ease. That choice is itself simulated — it is what makes
NodeSimilarity arrive in month ~3 in every timeline instead of "someday".

---

## Timeline A: The Monolith Serves The Seven (flat dual CSR + sidecars + generations)

- **Opening move:** Ship WCC over the flat forward CSR with a perNode parent
  array in the bounded scratch plane.
- **Week 1:** WCC and single-source Dijkstra work. Family 1 and family 5 —
  ~30% of adoption weight — land in week one, because both are exactly what
  flat CSR + tiny scratch is for. Fastest visible start of any timeline.
- **Month 1:** PageRank lands (family 3): 2×N f64 vectors fit even for large N
  (400 MB per vector at 25M nodes), but the FIRST holistic-RAM scare appears —
  20 pull-iterations over a rev CSR bigger than RAM turn into page-cache
  thrash; wall-time is I/O-bound and nothing in the architecture names or
  bounds this. Louvain starts and immediately exposes the coarsening problem:
  each coarsening level materializes a smaller graph, but level-0 scratch
  (community sums, sorted edge regroup) is a multi-GB hidden allocation the
  monolith has no vocabulary for. It works on the 2 GB corpus; on 50 GB-class
  it OOMs or swaps, and the fix (external regroup via the Build Store) is
  bespoke.
- **Quarter 1:** NodeSimilarity (family 4) arrives on schedule and, as
  Batch 07 predicted, does not fit: candidate-pair state explodes. The
  monolith's only honest answers are "reject on big graphs" or hand-written
  spill code grafted onto this one algorithm. FastRP (family 6) forces the
  property plane to grow a WRITABLE output column type (embedding matrix as
  mmap'd sidecar, built in slabs) — a good improvement, invented under
  pressure. Triangles get degree-ordered relabeling as a one-off preprocessing
  hack.
- **Long-term shape (Year 1):** Six of seven families run; NodeSim runs with
  a bespoke spill path that only its author understands. The engine is real
  and demo-strong, but every hard family left behind a private mechanism:
  Louvain's external regroup, NodeSim's spill, triangles' relabel, FastRP's
  slab writer. Four private mechanisms = the unpaid generalization that
  Timelines C/D/E charge for up front.
- **Likelihood of reaching year-end shape:** ~85%.
- **Stress points:** month-1 Louvain coarsening OOM (first crisis); quarter-1
  NodeSim wall (second crisis); the creeping realization that "bounded
  scratch plane" was a name, not a mechanism.
- **Inflection points:** the NodeSim crisis is THE branch point — bespoke
  spill keeps you in A; generalizing the spill machinery IS Timeline C, so
  the honest version of A quietly becomes A+C by year end.
- **Lived experience:** exhilarating first month, grinding third. The team
  demos weekly but privately keeps a list titled "things that only work
  because the fixture is small".

---

## Timeline B: The Tilehouse Serves The Seven (cellular/tiled CSR primary)

- **Opening move:** Partition the fixture, then implement WCC — which is
  immediately awkward, because WCC is a GLOBAL algorithm and the very first
  adoption-critical family ignores cell locality entirely.
- **Week 1:** WCC runs cell-by-cell with a cross-cell union pass over the
  boundary index. Correct, but 15-30% slower than flat and the boundary
  bookkeeping consumed the week. Bad omen: family #1 by adoption weight
  gained nothing from the architecture's central bet.
- **Month 1:** PageRank over the cell stream adapter: workable (GraphChi
  proved shard-streamed PageRank in 2012), and cells DO bound the working set
  per pass — the page-cache thrash that hurt Timeline A becomes a scheduled,
  predictable shard rotation. First genuine win. Louvain, however, is
  actively hostile to fixed partitions: communities do not respect cell
  boundaries, and coarsened levels need re-partitioning per level. The team
  runs Louvain over the global stream, i.e., pretends the cells aren't there.
- **Quarter 1:** Scorecard so far: of the top-5 families, only PageRank
  clearly benefited; WCC/Louvain are neutral-to-negative; Dijkstra from a
  single source is nicely cell-local (opens 3-8 cells) — a real but
  low-weight win; NodeSim blows up exactly as in Timeline A, except now the
  candidate-pair state ALSO fragments across cells. The falsifier metrics
  (Arch01 F16) come back mixed: locality wins for egonet/path queries,
  boundary-edge ratio 28-40% on the social-shaped fixtures that fraud
  workloads resemble.
- **Long-term shape (Year 1):** The honest conclusion writes itself: THE
  SEVEN ADOPTION FAMILIES ARE MOSTLY GLOBAL-SCAN OR PAIRWISE ALGORITHMS, and
  cells help mainly with (a) PageRank-style sweep scheduling and (b) bounded
  dirty-region REBUILDS on the publication side. Cells survive as a build/
  publication accelerator and a portfolio member for path-heavy tenants —
  Timeline D's framing — not as the primary serving layout.
- **Likelihood cells win as PRIMARY layout given this workload mix:** ~15%.
  (Materially lower than Arch01's workload-agnostic ~30-40% — this is the
  main new information Arch02's research adds.)
- **Stress points:** perpetual "why are we paying boundary tax for global
  algorithms" reviews; partition-quality research stealing time from the
  family roadmap.
- **Inflection points:** the quarter-1 falsifier readout. With fraud/reco
  workloads dominating adoption, the numbers land in the postpone/reject
  band, and the team either accepts the exit ramp to D or burns quarter 2
  on partitioning science that adoption data says few users need.
- **Lived experience:** the most frustrating timeline. Competent work,
  correct results, and a nagging feeling — confirmed by the adoption
  research — of having optimized for the wrong customers.

---

## Timeline C: The Budget Machine Serves The Seven (admission-controlled, out-of-core executor)

- **Opening move:** Write the memory formula for WCC (trivial: perNode parent
  + flags), wire it into the admission controller, ship WCC with a receipt.
- **Week 1:** WCC and Dijkstra land, same speed as Timeline A, plus receipts
  (`estimated 214 MB, observed 221 MB, budget 2 GB, verdict InCore`). The
  demo is only mildly more impressive than A's — the payoff is deferred, and
  the team knows it.
- **Month 1:** PageRank ships THREE plans: in-core (small graphs), windowed
  rev-CSR streaming (the page-cache thrash from Timeline A becomes an
  explicit, budgeted window — strict mode is real from month one), reject-
  with-estimate. Louvain's coarsening scratch — Timeline A's first crisis —
  is caught at ADMISSION: the estimate includes per-level regroup buffers,
  and the plan spills the edge-regroup to disk via external sort when over
  budget. Slower, honest, no OOM. Crisis converted to a feature.
- **Quarter 1:** NodeSimilarity is the flagship, not the wall: bucketed
  candidate passes with bounded top-K heaps and disk spill. On the 8 GB box
  it takes 4-9x longer than GDS on a 512 GB box — and FINISHES, where GDS on
  the same 8 GB box dies. That single benchmark ("we finish; they can't even
  load") is the product's entire adoption pitch made concrete, aimed at
  family #4 which is precisely the reco/ER algorithm mid-market buyers want.
  FastRP: embedding matrix admitted as a first-class artifact cost, built in
  budget-sized slabs to an mmap sidecar. Triangles: degree-ordered pass with
  hub-splitting under the same spill substrate.
- **Long-term shape (Year 1):** All seven families run on 8 GB in some mode,
  each with a receipt; the registry rows for the seven read
  `InCore | OutOfCore(cost) | RejectsWithEstimate` and are TESTED. The spill
  substrate (external sort, bucketed passes, windowed scans) is ONE shared
  mechanism where Timeline A grew four private ones. Cost: every family took
  ~1.5-2x the engineering of its Timeline-A version, and wall-times in
  degraded mode need constant expectation management.
- **Likelihood:** ~75% to reach year-end shape; ~95% that its admission layer
  exists in whatever architecture actually ships (unchanged from Arch01).
- **Stress points:** formula-vs-reality drift (Louvain's estimate is genuinely
  hard — coarsening depth is data-dependent, so estimates carry safety
  factors that occasionally reject runs that would have fit); "why is kNN
  slow" tickets that are actually "why is honesty slow" tickets.
- **Inflection points:** whether the Louvain estimate can be made tight
  enough (data-dependent coarsening) without either lying or rejecting
  half of feasible runs — the first real test of estimate-quality culture.
- **Lived experience:** the calmest timeline. No crisis months, because the
  crises were scheduled and priced. The anxiety is chronic instead: every
  formula is a small promise that measurement can break.

---

## Timeline D: The Foundry Portfolio Serves The Seven (multi-layout compiler targets)

- **Opening move:** Canonical flat CSR + property plane inside the portfolio
  manifest (Timeline A's bytes in D's coat); WCC and Dijkstra on it, week one.
- **Week 1:** Identical to Timeline A functionally. The manifest carries one
  artifact set; the planner is an if-statement. Cost of the coat so far: ~2
  days of manifest/parity plumbing.
- **Month 1:** The seven families start ORDERING artifacts like customers:
  PageRank wants a BV-compressed rev CSR (smaller sweeps -> less page-cache
  pressure); triangles want the degree-ordered relabeled CSR; FastRP wants
  the embedding sidecar as a managed artifact with its own generation
  lifecycle. Two of the three get built this month (compressed rev, embedding
  sidecar) because telemetry says PageRank and FastRP run daily in the pilot
  workloads. Each new artifact costs a parity proof against the canonical
  stream.
- **Quarter 1:** The portfolio's honest report card on the seven: PageRank
  +30-45% wall-time from compressed rev on cache-starved boxes; triangles
  2-4x from relabeling; Louvain gains nothing from any LAYOUT (its problem is
  scratch, not bytes) and quietly borrows Timeline C's spill idea for the
  regroup — revealing the same lesson as Timeline A, from the other side:
  LAYOUTS FIX SCAN COSTS, NOT STATE COSTS. NodeSim likewise: no artifact
  saves it; only budgeted execution does. Cells enter the portfolio late in
  the quarter for the publication-rebuild win identified in Timeline B.
- **Long-term shape (Year 1):** A two-artifact-to-five-artifact portfolio
  driven by real usage, each member paying rent via telemetry, plus a grafted
  mini-version of C's spill machinery for the state-heavy families. Disk
  footprint 1.6-2x canonical. The planner is small because the seven families
  have obvious artifact preferences; planner complexity was overfeared for
  THIS algorithm mix (it becomes real only in the long tail).
- **Likelihood:** ~55% as year-one endpoint (up slightly from Arch01's 50%:
  the seven families give unusually clear promotion signals).
- **Stress points:** parity-test CI time; disk pressure on laptop-class
  boxes; the standing temptation to solve Louvain/NodeSim with "one more
  artifact" when the evidence says they need budgeted execution instead.
- **Inflection points:** the moment the team notices families 2 and 4 (30%
  of adoption weight) are untouched by layout work — whether that triggers
  adopting C's executor (healthy) or artifact thrashing (rot).
- **Lived experience:** satisfying compiler-team rhythm, with one recurring
  bruise: the two most stubborn families are immune to the architecture's
  favorite move.

---

## Timeline E: The Algebra Engine Serves The Seven (kernel substrate)

- **Opening move:** Define masked-SpMV and frontier kernels; express WCC as
  hook-compress iterations (or label-prop-to-fixpoint over min-semiring SpMV).
- **Week 1:** WCC ~150 lines over kernels; Dijkstra as min-plus frontier
  expansion. Slightly slower start than A (kernel plumbing first), then two
  families light up in three days. Code review is a pleasure.
- **Month 1:** PageRank is the textbook kernel case: damped SpMV loop,
  ~80 lines, and every future SpMV optimization (SIMD, windowed streaming)
  lifts it for free. Label Propagation (family 2's cheap member) is
  max-frequency SpMV — nearly free. But Louvain — the family-2 flagship that
  fraud customers actually name — resists: modularity gain updates and
  coarsening are irreducibly imperative. It ships as a CustomOp using kernel
  primitives only for its scan phases. The escape hatch is used early and
  without shame, exactly as Arch01 prescribed.
- **Quarter 1:** The seven families classify cleanly:
  ```text
  AlgebraicOnKernels : WCC, PageRank, LabelProp, FastRP (sparse propagation
                        sweeps = repeated SpMV over random projections),
                        shortest paths (semiring frontier)
  KernelAssisted     : triangles/LCC (masked wedge-join kernel — hard SpGEMM
                        engineering, the quarter's main performance fight)
  CustomOp           : Louvain/Leiden coarsening loop, NodeSim candidate
                        generation (bucketed passes — which is Timeline C's
                        spill substrate wearing algebra clothes)
  ```
  Five of seven families inherit the kernel's memory formula and its
  out-of-core streaming variant IN ONE PLACE — the leverage Arch01 promised,
  demonstrated on exactly the algorithms that drive adoption.
- **Long-term shape (Year 1):** ~70% of the ADOPTION-WEIGHTED surface is
  algebraic or kernel-assisted (much better than the ~60% Arch01 guessed for
  the full 575, because the popular families skew algebraic — a genuinely
  encouraging research finding). The two CustomOps are the same two problem
  children as every other timeline. Masked-SpGEMM performance for triangles
  ends the year at ~1.7x bespoke code — inside the survival threshold Arch01
  set, barely.
- **Likelihood:** ~50% as primary organization (up from Arch01's 45%; the
  seven-family skew helps it).
- **Stress points:** the SpGEMM fight; onboarding contributors who want to
  "just write the loop"; profiling through kernel indirection.
- **Inflection points:** triangle-kernel performance (the ~2x threshold), and
  whether FastRP's algebraic form stays within memory honesty when the
  embedding matrix outgrows RAM (it must borrow slab-spill from C regardless).
- **Lived experience:** lumpy and cerebral. Weeks of kernel work punctuated
  by days where three procedures appear at once. The team's private joke:
  "Louvain doesn't believe in linear algebra, and neither do our two hardest
  customers' favorite algorithms."

---

## Cross-Timeline Analysis

### Per-family × per-architecture fit (the core matrix)

Legend: ++ architecture actively helps | + fine | 0 neutral | − fights it | X needs machinery the architecture doesn't natively have

| family (est. weight) | A Monolith | B Tilehouse | C Budget Machine | D Portfolio | E Algebra |
| --- | --- | --- | --- | --- | --- |
| WCC (~20%) | ++ | − (global; boundary tax) | ++ (trivial formula) | ++ (canonical suffices) | ++ (clean algebraic) |
| Louvain/Leiden (~15%) | X (coarsening scratch OOM) | − (communities defy cells) | ++ (admitted + spilled regroup) | X (no layout helps) | X→CustomOp |
| PageRank (~15%) | 0 (page-cache thrash unmanaged) | + (shard-scheduled sweeps) | ++ (windowed streaming plan) | ++ (compressed rev artifact) | ++ (SpMV textbook case) |
| NodeSim/KNN (~12%) | X (bespoke spill or reject) | X (worse: fragmented pairs) | ++ (bucketed spill = flagship) | X (no artifact saves it) | X→CustomOp (reuses C's substrate) |
| Shortest path (~10%) | ++ | ++ (best case for cells) | ++ | ++ | ++ (semiring frontier) |
| FastRP (~8%) | + (invents slab sidecar under pressure) | 0 | ++ (slab-budgeted output) | ++ (managed embedding artifact) | ++ (SpMV sweeps + borrowed slabs) |
| Triangles/LCC (~5%) | + (one-off relabel hack) | 0 | + (hub-split plans) | ++ (relabeled artifact) | + (SpGEMM fight, ~1.7x) |

### Read of the matrix

```text
R1. Column B is the research's clearest verdict: the adoption-driving mix is
    global-scan + pairwise + state-heavy. Cells' best cases (path queries,
    dirty rebuilds) sit in the LOW-weight rows or off the serving path.
    Cells-as-primary drops from "maybe" to "almost certainly not".
R2. Rows 2 and 4 (Louvain, NodeSim — 27% of adoption weight) are X in every
    bytes-oriented column and ++ only under C. The two algorithms most likely
    to close a fraud/reco deal are STATE problems, not LAYOUT problems.
R3. Column C is the only column with no X. It is also never the fastest
    column per-row. Honesty is its only superpower — and per the PRD (F02,
    F07), honesty on 8 GB is the product.
R4. Column E's ++ cells cluster on the highest-weight rows (WCC, PageRank,
    paths, FastRP): the popular algorithms skew algebraic. The kernel bet
    pays best precisely where adoption lives.
R5. Every column eventually imports C's spill/budget substrate for rows 2
    and 4. Arch01's conclusion ("C is a policy layer all paths need")
    is upgraded by the research to "C is the layer the ADOPTION CORE needs".
```

### Standard comparison table

| path | upside for the 80% set | downside for the 80% set | reversibility | regret risk | who/what must cooperate |
| --- | --- | --- | --- | --- | --- |
| A Monolith | 3 families ship in a month; fastest credibility | families 2+4 (27% weight) hit walls; four private mechanisms by year end | high | medium — converges to A+C anyway, after two crises | small fixtures' silence about big-graph reality |
| B Tilehouse | PageRank sweep scheduling; path queries; publication rebuilds | wrong bet for 5 of 7 families; boundary tax on the #1 family | medium | HIGH as primary; fine as D-member | graph community structure that fraud-shaped graphs partly lack |
| C Budget Machine | only path where all 7 run honestly on 8 GB; NodeSim becomes the pitch | never the fastest; 1.5-2x engineering per family; estimate-quality grind | high (policy layer) | LOW | measurement discipline; user patience with honest slowness |
| D Portfolio | PageRank/triangles/FastRP get tailored bytes; usage-driven evolution | impotent on Louvain/NodeSim; disk + parity tax | medium | medium — safe if promotion discipline holds | telemetry, CI capacity, disk budgets |
| E Algebra | top-weight families are algebraic; formulas + out-of-core inherited per kernel | Louvain + NodeSim escape the algebra; SpGEMM risk on triangles | medium-low | medium | kernel performance engineering; contributor buy-in |

---

## Decision Filter

**Which path is strongest if everything goes normally?**
The Arch01 composite, now with research-backed sequencing: **A's bytes,
C's admission/spill substrate built BEFORE month 3 (not discovered during the
NodeSim crisis), E's kernels for the five algebra-friendly families, D's
manifest from day one, cells demoted to a publication-side and portfolio
option.** The seven-family roadmap in composite form:

```text
Week 1:    WCC + Dijkstra on flat CSR kernels, with receipts.
Month 1:   PageRank (windowed SpMV plan) + LabelProp. Start Louvain CustomOp
           with admitted, spillable coarsening from the first line.
Quarter 1: NodeSimilarity on the bucketed-spill substrate as the FLAGSHIP
           8 GB benchmark vs GDS. FastRP slab-built embedding sidecar.
Quarter 2: Triangles via relabeled artifact (D's first promoted layout) or
           wedge kernel, whichever X-experiment wins.
```

**Which path is safest if things go badly?**
A + C-lite: flat CSR + sidecars + generations, admission receipts on every
procedure, and `RejectsWithEstimate` as the honest fallback for families 2
and 4 on large graphs. Even that degenerate engine serves ~55-60% of the
adoption weight (WCC, PageRank, paths, FastRP, triangles) honestly on 8 GB —
a shippable wedge product.

**What experiment or conversation would collapse uncertainty fastest?**
Re-ranked from Arch01 by the adoption research:

```text
X1' (unchanged, still first): NodeSimilarity under a 1 GB scratch budget —
     bucketed-spill vs reject. Now known to price 12% of adoption weight
     AND the flagship benchmark story. 1 week.
X2' (promoted): Louvain coarsening memory trace on the 2 GB corpus —
     measure true per-level scratch, then design the admitted/spilled
     regroup. Prices 15% of adoption weight that every bytes-first
     architecture fails. 1 week.
X3' (new): PageRank three ways on an 8 GB box with a 50 GB-class rev CSR:
     naive mmap vs windowed streaming vs BV-compressed rev. Decides both
     strict-mode mechanics and D's first artifact with one experiment. 1 week.
X4' (demoted from Arch01's X3): the cells falsifier — run it only when a
     path-query-heavy tenant actually shows up.
```

---

## Chain of Verification (Phase 3)

| # | verification question | answer | status |
| --- | --- | --- | --- |
| V1 | Does Neo4j publish per-procedure usage telemetry? | No public source found; hence "estimate, triangulated" labeling throughout. | honest-uncertainty |
| V2 | Are WCC/Louvain/PageRank/KNN really named in Neo4j's fraud materials? | Yes — fraud GDS papers and blog series name WCC (shared identifiers), Louvain (fraud rings), PageRank (anomalous accounts), KNN/NodeSim (similar profiles). | verified vs S2/S7 |
| V3 | Does Neo4j's own config guide benchmark exactly the shortlisted families? | Yes: PageRank, Louvain, WCC, LabelProp, TriangleCount, LCC, FastRP, NodeSimilarity on LDBC100. | verified vs S3 |
| V4 | LDBC100 memory figures as quoted? | PageRank 45.9-110 GB, Louvain 45.9-119 GB, FastRP 212-254 GB, NodeSim (PNP subset) ~800 MB-20 min, per the two config-guide editions. Figures vary by edition; both editions support the "incumbent needs 100+ GB-class boxes" claim. | verified vs S3, edition-variance noted |
| V5 | Is Louvain coarsening genuinely a scratch/state problem rather than a layout problem? | Yes — coarsening materializes successively smaller graph copies plus regroup buffers; no static layout of the ORIGINAL graph removes that cost. Consistent with Batch 07/10 evidence. | verified vs local shelf |
| V6 | Is FastRP an SpMV-style propagation (supporting Timeline E's claim)? | Yes — FastRP is iterated sparse matrix-vector/matrix products over random projections (Chen et al. 2019). | verified, external knowledge — user may independently verify |
| V7 | Is GraphChi-style shard-streamed PageRank real precedent for B/C streaming plans? | Yes — GraphChi (OSDI'12) demonstrated out-of-core PageRank et al. on a single PC. | verified, external knowledge |
| V8 | Do the percentage weights change any conclusion if wrong by ±50%? | No — R1-R5 depend only on the shortlist membership and the state-vs-layout split, both stable across sources. | robustness argued |

Weaknesses acknowledged: adoption shares are estimates; LDBC100 numbers vary
between guide editions; Timeline likelihoods are judgment, not measurement.
All three are flagged where used.

---

## One-Sentence Summary

```text
The algorithms that sell Neo4j are seven families, and they split cleanly
into scan problems (WCC, PageRank, paths, FastRP, triangles) that Arch01's
bytes-and-kernels answers serve well, and state problems (Louvain, NodeSim —
27% of adoption weight) that ONLY the admission-controlled, spill-capable
Budget Machine serves honestly on 8 GB — so build C's substrate before
month 3, put E's kernels under the scan families, keep A's bytes in D's
manifest, and let cells wait for a customer who actually walks paths.
```

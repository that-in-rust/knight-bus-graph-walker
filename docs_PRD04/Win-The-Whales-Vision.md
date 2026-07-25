# Win-The-Whales-Vision — Product Strategy For Large, Paying Neo4j Users

Date: 2026-07-25
Method: Doshi-lens strategy analysis — premise audit, council debate, conceptual
blending, parametrized comparison, chain-of-verification.
Status: **judgment.** Facts sourced in `evidence01.md` (URL-cited, verified
2026-07-25) and `simulation01.md`. Anything from general knowledge rather than
those sources is marked inline.
Companions: `Real-Pain-Wrong-Product.md` (the verdict), `Not-First-Still-Different.md`
(priority), `gtm-POC-01.md` (the mechanism this vision rides on).

---

## 0. The premise audit — three flaws, in descending order of consequence

### Flaw 1: the central contradiction (fatal if unaddressed)

The brief asks for a vision that wins **large Neo4j users** for a product whose
differentiation is **low RAM**. These are mutually hostile requirements.

```text
Neo4j staff engineer paul.horn, on the record, 2025-03-17:

  "GDS is a product primarily sold to enterprises, and so far we've not had
   customers who weren't able to rent a big enough machine in the cloud
   (SOME EVEN HAD 12TB RAM)"
```

**Large Neo4j customers are, by definition, the population that solves RAM
scarcity with a purchase order.** Selling frugality to them is selling a cure for
a disease they already vaccinated against with capex.

If the vision is "cheaper RAM," the honest answer to "how do we get whales to
adopt us" is **none of them will**, and no execution quality fixes it. Everything
below is the search for what whales *do* bleed from.

### Flaw 2: "without worrying about time and money"

Tokens are free; engineering is not. This project's own arithmetic:

```text
  Slater      2026-06-10 -> 2026-07-21  (~6 weeks, one person)
              empty repo -> Bolt-speaking, Elias-Fano, bounded-memory engine,
              Wikidata imported, HN launch

  This repo   2026-04-16 -> 2026-07-25  (~14 weeks)
              24 planning documents, 8 scheduled experiments, 0 run
```

Treating time as unconstrained is precisely the assumption that cost the moat.
The strategy below is time-priced per rung.

### Flaw 3: nomenclature

"Shreyas Darshan" -> **Shreyas Doshi**. His frameworks are applied here; this
document does not speak for him.

---

## 1. Council of experts

| Persona | Mandate |
| --- | --- |
| Doshi-lens Product Strategist | Levels of work; insight/feature/execution ladder; trigger moments; strategy-as-refusal |
| Enterprise Graph Platform Owner (bank, self-hosted, 4 TB box) | What pages them at 2 a.m.; what change control will actually approve |
| Database Kernel Engineer | What is physically true about heaps, projections, plugins, JNI |
| **Skeptical Engineer / Devil's Advocate** | Attacks every claim; owns the risk register |
| Compliance Officer (fraud/AML) | Model risk, provenance, auditability |

---

## 2. Divergent exploration

### 2.1 The conventional approach — and why it is now obsolete

PMF01-04's plan: OSS engine -> flagship benchmark (NodeSim/FastRP on 50 GB in
16 GB) -> sidecar -> enterprise honesty tier.

**Reject.** It was designed for a market with no competitors and it targets the
laptop buyer. Grafeo (707 stars) and Slater both occupy the benchmark ground, and
neither the benchmark nor the price story moves a customer with a 12 TB machine.

### 2.2 Blend A — Immunology / inoculation

*Fuse: graph-analytics adoption x vaccination and herd immunity.*

You do not replace the host organism. You introduce a tiny, provably harmless
agent that confers immunity to one specific disease. The disease is documented by
Neo4j's own product lead, advising a customer with hundreds of millions of nodes:

```text
  "the risk is OOMing your database"
```

The plugin is the **inoculation**: ~200 lines of Java plus a Rust cdylib,
namespaced `grain.*`, sitting beside `gds.*`. Zero migration, one jar, deletable.
Immunity conferred: **analytics can no longer kill the system of record.**

Why the blend is generative: it reframes adoption from *displacement* (which
whales structurally refuse) to *co-residency* (which change control can approve).

### 2.3 Blend B — Electrical grid / peak shaving

*Fuse: graph memory budgeting x utility demand-response and grid batteries.*

Utilities learned that customers pay for **peak** capacity, not average draw — so
batteries that absorb peaks let you provision for the mean. Graph analytics has
the identical shape:

```text
   FLEET RAM PROVISIONED  (what the enterprise actually buys)
   |
   |            #### FastRP 212-254 GB   <- the PEAK sets the purchase
   |      ###   ####
   |  ##  ###   ####   ###
   |  ###############################    <- OLTP baseline: the actual mean
   +--------------------------------------------> time

   They buy the tallest bar. Forever. In EVERY environment.
```

Neo4j's own LDBC100 sizing guide supplies the peaks: PageRank 45.9-110 GB,
Louvain 45.9-119 GB, **FastRP 212-254 GB**. The enterprise buys 254 GB of headroom
to run FastRP occasionally, then multiplies by dev/staging/prod/DR and by region.

We are the **battery**: absorb the peak onto disk; let them provision the baseline.

Why generative: it changes the buyer from the data scientist (no budget authority)
to the **capacity planner / platform owner** (has budget authority), and it yields
a directly invoiceable metric — *peak GB retired*.

### 2.4 Blend C — Aviation: pre-flight checklist + black box

*Fuse: memory receipts x flight-safety instrumentation and accident investigation.*

Aviation solved "complex system, catastrophic failure, regulatory scrutiny" with
two artifacts:

```text
  PRE-FLIGHT CHECKLIST  -> refuse to take off unless conditions are met
  FLIGHT RECORDER       -> prove afterwards exactly what happened
```

Map directly:

- `Arch05` G3's manifest-derived estimate **is** the pre-flight checklist — and it
  refuses departure rather than crashing at hour four.
- Immutable watermarked generations **are** the black box: "this fraud score came
  from generation 42, watermark W, checksum X."

For fraud/AML — the deepest-pocketed GDS segment per `simulation01` §13.2 — model
provenance is an obligation, not a nicety. *(Model-risk governance regimes
generally require reproducibility and lineage; specific regulatory citations are
NOT from this session's sources and must be verified independently before any
external use.)*

Why generative: it converts a technical artifact (a receipt) into a **compliance
instrument** — the one category enterprises buy without price sensitivity.

### 2.5 Evaluation and selection

| Approach | Whale fit | Adoption tax | Copyable by rivals? | Verdict |
| --- | --- | --- | --- | --- |
| Conventional (OSS + benchmark) | Poor — laptop buyer | Low | Already copied (Grafeo, Slater) | **Reject** |
| A — Inoculation | **Excellent** — co-residency | **Lowest possible** | **No** — rivals *are* replacement DBs | **Core** |
| B — Peak shaving | **Excellent** — right buyer, right metric | Low | Partially — needs their engine in-situ | **Core** |
| C — Black box | **Excellent** — compliance wedge | Low | Not quickly — needs the estimator contract | **Core, as moat** |

**Selection: hybrid A+B+C.** They compose without friction and each removes a
different blocker: A solves *how it gets installed*, B solves *who signs*, C solves
*why they renew*. Critically, **all three are only reachable from inside a live
Neo4j** — the one position no competitor can take without abandoning its own
architecture.

### 2.6 Structured debate

**Platform Owner:** "I will never migrate the system of record for an analytics
feature. But I have been paged because a data scientist's projection ate the heap.
Give me analytics with *zero* heap contact on my OLTP instance, removable by
deleting a jar, and that clears change control in one cycle. That is a security
review, not a migration project."

**Compliance Officer:** "Deterministic provenance is worth more to me than speed.
If every score names the generation and watermark it came from, and infeasible
jobs *refuse* instead of dying halfway, that removes an audit finding. I have
budget for removing audit findings."

**Kernel Engineer:** "Mechanically sound. Neo4j registers procedures from any
plugin jar; `grain.wcc.stream` coexists with `gds.wcc.stream` because names are
namespaced. Results cross JNI as one direct ByteBuffer, never per-row. The graph
never crosses the boundary — Rust reads its own snapshot."

**Skeptical Engineer — four objections, two serious:**

```text
  (1) Aura FORBIDS custom plugins. If whales migrate to managed, this is
      dead-ended.
  (2) GPL. A plugin links Neo4j's kernel API; Community Edition is GPLv3.
      You may be building a derivative work you cannot license as you wish.
  (3) dbms.security.procedures.unrestricted=grain.* plus a native .so in
      plugins/ is a hostile ask inside a bank.
  (4) You are describing four features and calling it a moat.
```

**Responses:**

- **-> (1), Platform Owner:** *Inverted.* Aura Graph Analytics sessions cap at
  **512 GB**; AuraDS tops out around **1,952 GB**. A customer with **12 TB** of RAM
  **cannot** be on Aura at that scale. The whale segment is structurally
  self-hosted — exactly where plugins are permitted. **Aura's plugin ban excludes
  us from small accounts, not large ones.**
- **-> (2), Kernel Engineer:** Real; resolve with counsel, not argument. Precedent
  exists — APOC ships as a Neo4j plugin under Apache-2.0 *(general knowledge, not
  from this session's sources — verify)*. Mitigation regardless: keep the Rust
  engine a standalone Apache-2.0 binary and make the Java shim a thin, separately
  licensed adapter.
- **-> (3), Kernel Engineer:** Conceded. One documented config line, the same ask
  APOC makes — but it moves first-deal timelines from weeks to a quarter, and the
  plan must price that.
- **-> (4), Doshi Strategist:** **Conceded, and it is the most important
  concession here.** These are features. The moat is not any one of them — it is
  the **install-base position**. Once we are the jar inside a Neo4j instance, we
  own the trigger moment, and every rival must persuade the customer to migrate
  *away from Neo4j* to displace us. **Our moat is Neo4j's own gravity, borrowed.**

### 2.7 Master synthesis — the core thesis

> **Stop selling frugality. Sell blast-radius elimination, peak-capacity
> retirement, and auditable provenance — delivered as a deletable jar inside the
> customer's existing Neo4j.**
>
> Low RAM stops being the product and becomes the *mechanism*. Whales do not buy
> cheaper memory; they buy the removal of a production risk, the retirement of
> fleet-wide peak provisioning, and an answer for the auditor. And the plugin is
> the only architecture in the market that asks a whale for **nothing**.

---

## 3. Table A — Product strategy anatomy (parametrized)

| Player | Strategy archetype | Wedge / entry | Buyer + trigger moment | Graph residency | **Adoption tax** | Compat surface | Monetization | Distribution engine | Diff level | **Whale fit** | Structural ceiling |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Neo4j GDS / Aura** | Incumbent land-and-expand | Cypher familiarity + ecosystem | Enterprise architect; new graph initiative | **100% JVM heap**, no spill | n/a (installed) | Native | RAM-metered (GB-hr / GB-min) + enterprise licence | Brand, GraphAcademy, partners | Insight (ecosystem) | **Native** | Revenue *is* the RAM meter; cannot sell frugality without self-cannibalizing |
| **Neo4j InfiniGraph** (2025-09-03) | Scale-out defence | Property sharding, 100 TB+ | Same buyer, bigger data | Sharded cluster | Upgrade | Native | Same meter, larger tiers | Same | Feature | **Native** | Scale-*out*, not out-of-core; does not remove heap projection |
| **Grafeo** (707*, Rust, Apache-2.0) | Feature-superset assault | "Fastest + low memory", 6 query languages | Greenfield dev; new project | Disk + transparent spilling | **Migration** | GQL/Cypher/Gremlin/SPARQL + `boltr` | TBD | GitHub, benchmarks, LangChain | Feature | **Poor** — requires replacing the SoR | Must win a database bake-off to get in the door |
| **Slater** (Rust, Apache-2.0, 6 wks old) | Precision wedge | "Graphs that don't fit in memory, over Bolt" | Platform eng; graph outgrew the box | Disk image + fixed cache budgets | **Migration + offline compile** | Bolt 5.4/4.4/4.1, Cypher subset | TBD | HN, README | Feature | **Poor** — same reason | Read-replica shaped; not a system of record |
| **LadybugDB** (Kuzu renamed, 1,475*, MIT) | Embedded continuation | Embedded OLAP + disk-scanned algorithms | AI/RAG dev; local knowledge graph | Columnar disk | **Migration** | Own API | TBD | Kuzu lineage | Feature | **Poor** | Post-acquisition governance uncertainty |
| **Onager** (DuckDB extension, MIT) | Ecosystem parasite (the right shape, wrong host for us) | 40+ algorithms as SQL table functions | Analyst already in DuckDB | **In-memory** registry outside DuckDB's buffer pool | **Very low** (extension) | SQL | Free | DuckDB community channel | Execution | Poor (no Neo4j path) | Rebuilds the graph per call; same heap wall, minus the JVM |
| **Memgraph** | Anti-positioning | Latency, streaming | Real-time engineer | **In-memory** (on-disk mode experimental) | Migration | Cypher subset | Enterprise | Benchmarks | Feature | Poor | Its pitch *is* RAM; cannot follow us to disk |
| **cuGraph** | Vertical acceleration | GPU speed | ML platform with GPUs | VRAM | Rewrite | Python | NVIDIA halo | RAPIDS | Feature | Medium | Needs GPU budget; VRAM is scarcer than RAM |
| **igraph / NetworKit** | Academic default | Free, trusted, cited | Researcher | RAM only | Rewrite | Python/R | None | Academia | — | Poor | No storage layer by design |
| **US — `prd-l1` as written** | Full displacement | "Rewrite Neo4j in Rust" | Undefined | Own OLTP + snapshots | **Total (migration + trust)** | Bolt/Cypher/APOC/575 procedures | Undefined | None yet | — | **Fatal** — asks a whale to replace its SoR | A decade of work against solo capacity |
| **US — `gtm-POC-01` plugin** | **Ecosystem parasite + blast-radius removal** | `grain.wcc.stream` beside `gds.wcc.stream` | **Platform owner / SRE; the OOM page or the audit finding** | mmap snapshot, `O(V)` resident | **~Zero: one jar + one config line** | **`gds.*` call shape**, parity by `diff` | Enterprise licence on receipts / audit / SLA | **Neo4j's own install base** | Feature x **position** | **Excellent — uniquely so** | Aura ban (irrelevant at whale scale); GPL review; `unrestricted` config |

> **The single most important column is Adoption Tax.** Every credible rival
> charges a migration. Whales cannot pay it. We are the only entrant whose tax is
> a jar.

---

## 4. Table B — What large, paying Neo4j customers actually bleed from

| # | Evidence (verbatim where the quote carries the weight) | Source · date | Conf. | Enterprise pain proven | Implication for the vision |
| --- | --- | --- | --- | --- | --- |
| W1 | *"GDS is a product primarily sold to enterprises, and so far we've not had customers who weren't able to rent a big enough machine in the cloud (**some even had 12TB ram**)"* | Neo4j staff `paul.horn`, forum · **2025-03-17** | **VERIFIED** | Whales are **not** RAM-poor | **Kill the frugality pitch for whales.** Frugality is mechanism, never promise |
| W2 | *"the risk is **OOMing your database**"* — official guidance to a user with 100Ms of nodes on 32 GB | Neo4j GDS product lead (A. Frame) · 2022 | **VERIFIED** | **Analytics can take down the system of record** | **THE WHALE WEDGE. A SEV, not a cost line.** |
| W3 | *"The graph algorithms library operates completely on the heap"*; remedy set = drop graphs / raise heap / *"sudo mode which allows you to manually skip heap control"* | GDS docs · current | **VERIFIED** | Co-tenancy is architectural, not misconfiguration | "Zero heap contact on your OLTP instance" becomes a *provable* claim |
| W4 | *"For purely analytical workloads … **decrease the configured PageCache in favor of an increased heap**"* | GDS System Requirements | **VERIFIED** | Tuning for analytics degrades transactional performance | Analytics tuning and OLTP tuning are in direct conflict — we end the conflict |
| W5 | PageRank 45.9-110 GB · Louvain 45.9-119 GB · **FastRP 212-254 GB** (LDBC100) | Neo4j's own GDS Configuration Guide | **VERIFIED** (edition variance noted) | **Peak sets the purchase** | **Peak-shaving economics.** Retire 254 GB x every environment |
| W6 | 95 GB RAM / 75 GB heap, ~320M nodes — **still** heap errors. Separately: projection fails at ~300 GB, *"capacity exhausted"* | Neo4j forum (E4, E5) | **VERIFIED** | Big iron does not save you; the wall scales with you | Buying more RAM is not even a *reliable* fix for whales |
| W7 | *"Procedure was blocked since minimum estimated memory (**130 GiB**) exceeds current free memory (24 GiB)"* — user shrank the graph to **2,594 nodes**, still got a **54 GiB** estimate; *"**motivating us to look elsewhere for scale**"* | Neo4j forum · NodeSimilarity | **VERIFIED** | Estimator over-provisioning drives **churn**, in the user's own words | The estimate itself is the loss event |
| W8 | *"When I drop the memory graph, my memory usage does not change"* — `gds.graph.drop` does not return memory to the OS; forced restarts | Neo4j forum | **VERIFIED** | **Restarting production to reclaim RAM** | `munmap` actually releases. Unglamorous; beloved by SREs |
| W9 | Louvain: **5 hours, >70 GB heap** on a 60 GiB store | Stack Overflow | **VERIFIED** | Iteration latency; nightly-batch fragility | Warm-start / delta convergence is a **whale** feature (re-run economics) |
| W10 | *"Neo4j's entire pricing model, even in cloud, is built around the idea that you'll have **one centralized very large graph**"* — does not fit shops with 3-5 pre-production environments | HN · 2021 | **VERIFIED** | **Fleet multiplication:** RAM x environments x regions | Peak shaving compounds across the fleet — that is the real bill |
| W11 | Paused 512 GB AuraDS Enterprise bills 33.28 ACU/hr => **~USD 23,962/month executing nothing**; sessions bill a **10-minute minimum at full declared RAM** (512 GB = USD 34.30 for one second of work) | Neo4j rate card + billing docs · accessed 2026-07-25 | **VERIFIED** (list-price arithmetic, not observed invoices) | Capacity-time billing punishes over-declaration | **Mid-market wedge, NOT a whale argument** — see V7 |
| W12 | Aura **does not permit custom plugins**; AGA sessions cap at 512 GB, AuraDS ~1,952 GB | Neo4j docs; `gtm-POC-01` | **VERIFIED** | Whales at 12 TB **cannot be on Aura** | **The plugin ban excludes us from small accounts, not large ones** |
| W13 | Fraud/AML = deepest-pocketed segment; re-runs WCC + Louvain + PageRank + triangles on a growing graph | `simulation01` §13.2 | Modeled | Re-run-heavy, regulated, wealthy | Warm-start + provenance aim at exactly this buyer |
| W14 | Nine consecutive GDS releases (Nov 2025 -> Jul 2026): **zero** disk/spill/off-heap/mmap features. Staff: *"providing a good implementation of out-of-core GDS has never been prioritized enough"* | Release notes + forum | **VERIFIED** | The gap is **stable and intentional** | Window is open — but W1 explains *why*, which is also the warning |

---

## 5. Table C — Doshi scorecard: who succeeds, and why

| Player | Insight quality | Distribution | **Access to the trigger moment** | Monetization clarity | Structural conflict | **Doshi verdict** | P(wins whale segment) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Neo4j** | Ecosystem is the real moat | Overwhelming | **Owns it** | Total (the meter) | Meter blocks frugality; InfiniGraph shows they defend on scale-*out* | **Wins by default** — not because they solved it, but because switching costs exceed the pain | **~70%** |
| **Grafeo** | Feature superset; low memory as one bullet | 707* and climbing | **None** — must win a bake-off | Unclear | Must beat an incumbent on trust to get in | *"Impressive engineering aimed at greenfield. Whales don't run bake-offs for analytics."* | **~5%** |
| **Slater** | **Sharp and correct** — decouples RAM from graph size | 93*, four days old | Bolt means drivers work, but it still replaces the DB | Unclear | Read-replica shaped, not a system of record | *"Best insight in the field. Wrong buyer for whales; right buyer for mid-market."* | **~5%** |
| **LadybugDB** | Embedded OLAP | 1,475* + lineage | None | Unclear | Governance post-acquisition | *"Strong for local/RAG. Not an enterprise motion."* | **~2%** |
| **US — plugin** | **Position, not feature:** co-residency | **Zero today** | **Only entrant that lives where the pain occurs** | Clear: receipts / audit / SLA licence | Solo capacity; GPL review; `unrestricted` config | *"The only strategy with a credible whale path — IF it exists as code within a quarter. It does not exist yet, and that is the whole risk."* | **~15%**, conditional on shipping |
| **US — `prd-l1`** | — | — | — | — | Asks a whale to replace its SoR | *"Not a strategy. A wish."* | **~0%** |

*Probabilities are calibrated judgment, not measurement.*

**Doshi's reasoning on who wins:** whales are not won by the best engine. They are
won by whoever is **already inside** when the trigger fires. Neo4j is inside.
Grafeo, Slater and Ladybug must all get the customer to *leave* Neo4j — a decision
made by committees over quarters, on a dimension (analytics memory) that no
committee has ever prioritized above transactional stability. Our plugin is the
only design that is inside **without asking permission to replace anything.** That
is the strategic asset, and it is worth more than the four features beneath it.

---

## 6. Table D — The vision, laddered and parametrized

| Rung | Promise, in the buyer's own words | Buyer | Trigger | Proof required | Mechanism | Monetization | Time-priced |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **V0** | *"Same answer as GDS, verified by one `diff`."* | Data scientist | Curiosity | WCC partition parity, canonicalized by min member; empty diff | Plugin + JNI + mmap snapshot | **$0** — this buys trust, not revenue | **2 weeks** |
| **V1** | *"Your analytics can no longer take down your database."* | **Platform owner / SRE** | The OOM page (W2, W8) | Seven families run with **zero heap growth** on the OLTP instance; `munmap` returns memory | `O(V)` resident, edges streamed | Enterprise licence | Q1 |
| **V2** | *"Retire the 254 GB you keep for FastRP — in every environment."* | **Capacity planner / FinOps** | Budget cycle; fleet audit (W5, W10) | Before/after peak-provisioning report across the fleet | Peak shaving; N stateless readers from one image | **Share of retired peak capacity** | Q2 |
| **V3** | *"Every score names the data generation that produced it, and infeasible jobs refuse to start."* | **Compliance / model risk** | Audit finding (W13) | Watermarked immutable generations + pre-run receipt + deterministic refusal | Manifest-as-estimator (`Arch05` G3) + generation catalog | Audit / SLA tier — **the renewal moat** | Q3 |
| **V4** | *"Your five-hour Louvain re-runs in twenty minutes because we remember yesterday."* | Fraud/AML lead | Nightly-batch SLA (W9) | Warm start from the prior generation; delta convergence | `Arch-Summary` AXIS 2 (memoized results) | Solution pricing | Y2 |

### The vision statement

```text
Graph analytics that cannot hurt production, cannot surprise your budget,
and can testify in an audit -- installed as one file inside the Neo4j
you already run.
```

### The north-star metric

Not `completed-runs-that-would-not-have-fit` — that is the laptop metric. For
whales:

```text
   PEAK GB RETIRED
   = the provisioned peak RAM a customer no longer needs to hold for
     analytics, summed across their entire fleet.
```

It is the capacity planner's own language, it is directly invoiceable
(share-of-savings), and every unit of it is a sentence Neo4j's architecture cannot
say.

### What this vision refuses (strategy is refusal; good strategy feels like loss)

```text
  REFUSE: rewriting Neo4j's OLTP, WAL, locks, transactions
  REFUSE: Bolt, Cypher, drivers, APOC, the 575-procedure surface
  REFUSE: greenfield bake-offs against Grafeo and Slater
  REFUSE: "cheaper than Aura" as the headline
  REFUSE: benchmarks whose unit is speed rather than retired capacity
```

---

## 7. Chain of verification

| # | Question | Answer | Status |
| --- | --- | --- | --- |
| V1 | Do whales really run 12 TB machines? | Yes — Neo4j staff, verbatim, 2025-03-17. **This is the fact that invalidates the frugality pitch for whales.** | VERIFIED |
| V2 | Did Neo4j really advise a customer that the workaround risks OOMing their database? | Yes — GDS product lead, verbatim. Load-bearing for rung V1. | VERIFIED |
| V3 | FastRP 212-254 GB on LDBC100? | Yes, Neo4j's own configuration guide; figures vary by edition, both editions support the claim. | VERIFIED, variance noted |
| V4 | Does Aura's plugin ban kill this strategy? | **No — it inverts.** Sessions cap at 512 GB, AuraDS ~1,952 GB, so 12 TB workloads structurally cannot be on Aura. Excludes small accounts, not whales. | VERIFIED fact; **inference** on segmentation |
| V5 | Can the plugin be licensed as we wish? | **UNRESOLVED — needs counsel.** APOC-as-Apache-plugin is precedent but is general knowledge, not from this session's sources. Mitigation: standalone Apache-2.0 Rust binary + thin separately licensed shim. | **OPEN LEGAL RISK** |
| V6 | Is "zero heap contact" actually provable? | Mechanically plausible — the graph never crosses JNI; results cross as one direct ByteBuffer. **Unmeasured.** This is the `grain.ping` spike. | **UNPROVEN — the gate** |
| V7 | Do whales feel the Aura billing pain (W11)? | **Probably not** — W1 says they buy hardware. W11 is real arithmetic aimed at mid-market. **Do not lead with it for enterprise.** | **Corrected in draft** |
| V8 | Is "peak GB retired" measurable pre-product? | No. Requires V1 shipped plus a customer fleet audit. V2's metric only becomes real in Q2. | honest-uncertainty |
| V9 | Are the win probabilities measured? | No — calibrated judgment. The dominant term is *whether code exists within a quarter*, which is under our control and nobody else's. | honest-uncertainty |

**Correction the verification forced:** the initial draft used Aura
over-provisioning arithmetic (W11) as a whale argument. V7 kills that — it
contradicts W1. Cost arguments are a **mid-market** wedge. For whales the argument
is **production risk (W2), fleet peak (W5 + W10), and audit (W13)**. The tables
above reflect the correction.

---

## 8. The one paragraph, if nothing else survives

Whales will never adopt us for using less memory, because a 12 TB purchase order
already solved that for them. They will adopt us for three things their own
vendor's documentation admits it cannot give: analytics that **cannot OOM the
system of record**, peak capacity **retired across the whole fleet** rather than
bought for the worst algorithm in every environment, and a **provenance receipt**
that survives an audit. And they will only ever adopt something that asks them for
**nothing** — which is why the deletable jar inside a running Neo4j is not the
humble version of this strategy. It is the only version that can win, and it is
the one thing three well-built competitors structurally cannot copy, because each
of them *is* the database you would have to leave.

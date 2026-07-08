# PMF01: Differentiation, PMF Offerings, And The Replacement Narrative (Shreyas Doshi POV)

Date: 2026-07-08
Voice: written as Shreyas Doshi would reason about this product — impact over
activity, positioning before features, pre-mortems before roadmaps, and a
ruthless separation of "real differentiation" from "stuff we're proud of."
Builds on `SUM01.md` (what the shelf says), `Arch01.md` (architectures),
`Arch02.md` (the seven adoption families and their memory bills).

---

## Part 0: Core Facts (enumerated before opining)

### Facts we already established locally

```text
K1. Target: Neo4j-compatible Rust rewrite; zero client-code changes where
    support is claimed. 50 GB-class graphs on 8 GB-class machines.
K2. ~85% of GDS adoption rides on 7 algorithm families: WCC, Louvain/Leiden,
    PageRank, NodeSimilarity/KNN, shortest paths, FastRP, triangles (Arch02).
K3. Neo4j's own LDBC100 configuration guide: PageRank needs 45.9-110 GB,
    Louvain 45.9-119 GB, FastRP 212-254 GB of memory on a 512 GB machine.
K4. Louvain and NodeSim (~27% of adoption weight) are state problems that
    only an admission-controlled, spill-capable executor serves honestly on
    small RAM (Arch02 matrix).
K5. v002 already measured lower RSS than Neo4j on traversal fixtures.
K6. The PRD's honesty machinery (memory receipts, reject-before-execute,
    watermark-exact snapshots) is unusual enough to be a FEATURE, not
    plumbing.
```

### Facts from fresh internet research (user may independently verify)

```text
W1. Neo4j PRICES BY RAM. AuraDS is billed in GB-hours of RAM capacity
    (billed even when paused); Aura Graph Analytics ~$0.40/GB-hour;
    AuraDB Professional ~$65/GB/month, Business Critical ~$146/GB/month.
    RAM is not just Neo4j's technical constraint — it is literally the
    billing dimension. (console.neo4j.io/pricing, neo4j.com/pricing,
    Aura billing-dimensions docs.)
W2. Sizing example: the FastRP job from K3 needs a 200+ GB-RAM class
    instance. On RAM-metered pricing, big-graph GDS is structurally
    expensive; a 256 GB AuraDS tier lists at ~$83-166/hour-class rates.
W3. Second graph wave (2024-2026): GraphRAG / LLM agent memory made graph
    DBs standard RAG-stack infrastructure (Microsoft GraphRAG 2024, ISO GQL
    2024, Neptune Analytics launch). New buyers are AI engineers, not DBAs.
W4. Competitive camps: Neo4j/Memgraph/TigerGraph (property-graph servers),
    Kuzu/FalkorDB (embedded/analytics), Apache AGE (Postgres extension).
    Notably: Memgraph is IN-MEMORY (more RAM, not less); Kuzu/FalkorDB are
    fast but NOT Neo4j/GDS-API-compatible; nobody in the market sells
    "your existing Neo4j code, on 1/10th the RAM."
W5. GDS self-managed requires Neo4j + enterprise licensing for production
    features; the free path for serious GDS on big graphs is effectively
    "buy a very large machine."
```

### The one-line synthesis of the facts

```text
The incumbent meters by the exact resource we are 10x better at,
on the exact workloads (7 families) that drive its adoption.
```

That is a rare and precious situation. Most challengers are better at
something the incumbent doesn't charge for. We are better at the billing axis.

---

## Part 1: Shreyas Doshi Framing — What Is The Actual Differentiation?

Shreyas's first move: separate the three kinds of "differentiation" teams
confuse — features (copyable), advantages (durable-ish), and positioning
(what the buyer repeats to their boss). Then check for impact, not elegance.

### 1.1 The differentiation stack, ranked by durability

| rank | differentiation | type | why it's defensible | Shreyas-style caution |
| --- | --- | --- | --- | --- |
| 1 | **RAM-honest analytics**: 50 GB-class GDS workloads finish on 8 GB-class boxes; every run gets a memory receipt; infeasible runs reject BEFORE burning an hour | advantage → positioning | Requires the whole Arch01-C substrate (admission, formulas, spill). Neo4j can't copy without re-architecting the JVM/GDS heap model AND cannibalizing RAM-metered revenue. Structural conflict = durable moat | Don't market "low RAM" (a spec). Market "never buy a 512 GB box to run PageRank again" (a bill) |
| 2 | **Drop-in compatibility**: same Bolt/Cypher/gds.* calls, zero client changes | table stakes + wedge | Compatibility is the adoption wedge, not the differentiation — Memgraph has partial Cypher compat and it alone didn't dethrone anyone | The 575-procedure registry with DETERMINISTIC unsupported behavior is what makes "compatible" a truthful word. Guard it |
| 3 | **Deterministic honesty**: watermark-exact answers, atomic generations, receipts | advantage | Nobody in the graph market sells predictability as a product. Ops people LOVE this once they've been paged for an OOM'd Louvain | Honesty is a retention feature, not an acquisition feature. It closes renewals, not first meetings |
| 4 | Rust single-binary operational simplicity (no JVM, no heap tuning, embedded-friendly) | feature | Kuzu/FalkorDB also have this; alone it's not differentiating | Bundle it into #1's story ("nothing to tune because nothing balloons") |
| 5 | Speed on scans (SpMV kernels etc.) | feature | Someone is always faster somewhere; benchmarks are a treadmill | Never lead with speed. We will LOSE some speed benchmarks to 512 GB machines — by design. Lead with cost-to-finish |

### 1.2 Impact on the key workloads (estimates, from K2-K4 + W1-W2)

The honest impact table — what the seven families' buyers actually feel.
"Incumbent bill" assumes RAM-metered managed pricing (W1) or an equivalently
sized self-managed box; estimates are directional, flagged per Arch02's
method (±50% robustness).

| workload (adoption weight) | who buys it | incumbent reality (K3/W1) | our impact | estimated economic delta |
| --- | --- | --- | --- | --- |
| WCC — entity resolution, fraud rings (~20%) | fraud/AML teams, MDM | needs whole-graph projection in RAM; 100M-edge class → 64-128 GB instances | runs in near-topology-only footprint; trivial scratch (Arch02) | **5-15x smaller instance**; ER batch jobs move from cluster-budget line items to cron jobs on a VM |
| Louvain/Leiden — fraud rings, segmentation (~15%) | fraud, marketing analytics | 119 GB on LDBC100; coarsening OOMs are folklore | admitted + spilled coarsening: slower (est. 2-5x wall) but FINISHES on 8-16 GB, with a receipt | jobs that today require the biggest box in the fleet run on commodity; predictability > speed for weekly batch |
| PageRank-class (~15%) | risk scoring, GraphRAG relevance | 45-110 GB on LDBC100 | windowed/compressed sweeps: est. 1.5-3x wall vs big-box GDS, on 1/10th RAM | **~10x cheaper per run** on RAM-metered math (W1: same GB-hours price × 1/10 the GB, ×2-3 the hours) |
| NodeSim/KNN — reco, ER, fraud lookup (~12%) | reco teams, fraud | the OOM king; GDS's own guide quietly benchmarks it on a reduced graph (W: LDBC100-PNP) | bucketed-spill: the flagship "we finish, they can't load" benchmark (Arch02) | converts "we can't run this at our scale" into "runs overnight" — a CAPABILITY unlock, not a discount |
| Shortest paths (~10%) | logistics, dependency analysis | fine on Neo4j (cheap algo) | parity; nothing to brag about | ~0; keep for compatibility completeness |
| FastRP embeddings (~8%) | ML platform teams, GraphRAG | 212-254 GB on LDBC100 — the single worst bill in Neo4j's own guide | slab-built embedding sidecars under budget; est. 2-4x wall on 1/10-1/20 RAM | **the poster child**: "the job Neo4j sizes at 254 GB runs on your 16 GB dev box overnight" |
| Triangles/LCC (~5%) | cohesion scoring | moderate | relabeled-CSR artifact: parity-to-better | modest; supports fraud-score bundles |

Net impact claim (the one number to defend publicly): **for 5 of the 7
adoption-driving families, total cost per completed run drops ~5-15x on
RAM-metered infrastructure, at 1.5-5x wall-time — and two families
(NodeSim-at-scale, FastRP-at-scale) flip from "infeasible without a special
machine" to "feasible on what you already have."**

### 1.3 The Shreyas pre-mortem (why this fails, written in advance)

```text
PM1. Compatibility debt kills velocity: chasing 575 procedures instead of
     nailing 7 families + honest rejection for the rest.
     Countermeasure: the registry IS the roadmap firewall (K6).
PM2. We win benchmarks nobody buys with: speed charts vs a 512 GB box we
     were never trying to beat. Countermeasure: publish COST-TO-FINISH
     benchmarks only ($/completed-run, box-size-to-finish).
PM3. Neo4j responds with a "GDS-lite low-RAM mode."
     Countermeasure: their pricing (W1) makes this self-cannibalizing;
     the deeper moat is reject-before-execute honesty, which requires
     estimate infrastructure they'd need years to retrofit.
PM4. The wedge buyer (cost-conscious mid-market) churns to Kuzu/FalkorDB
     for greenfield work where compatibility doesn't matter.
     Countermeasure: our buyer HAS Neo4j code/skills already; stay laser
     on brownfield migration, don't fight embedded engines for greenfield.
PM5. "Slower but honest" reads as "slow" in a bake-off run by a DBA.
     Countermeasure: the receipt UX — every run prints estimated vs actual
     vs budget — turns honesty into a visible, demo-able artifact.
```

---

## Part 2: The PMF Offerings Table (the deliverable)

Shreyas lens applied: each offering names the specific buyer, their trigger
moment (PMF lives in trigger moments, not personas), what we uniquely do,
proof required, and the monetization/adoption mechanic. Ordered as a wedge
sequence, not a menu — O1 earns the right to O2, etc.

| # | offering | buyer + trigger moment | what it is | unique because | proof required (falsifiable) | monetization / adoption mechanic |
| --- | --- | --- | --- | --- | --- | --- |
| O1 | **"Runs-on-what-you-have" OSS engine** — Neo4j/GDS-compatible single binary for the 7 families | data scientist / AI engineer whose GDS job OOM'd or whose AuraDS quote made the CFO wince | Free, open-source core: Bolt + Cypher subset + gds.* for the 7 families, memory receipts, honest rejection | Only engine that runs EXISTING gds.* code on laptop-class RAM (W4: Memgraph needs more RAM, Kuzu/FalkorDB aren't compatible) | The Arch02 flagship benchmark: NodeSim + FastRP on 50 GB-class graph, 16 GB box, published & reproducible | **Adoption, deliberately $0.** OSS is the distribution strategy; every "it ran on my laptop" tweet is the GTM |
| O2 | **Batch analytics sidecar** — keep Neo4j OLTP, point our engine at it as the OLAP plane | platform lead at a Neo4j shop whose GDS projections are eating the prod heap; trigger = the next OOM page or license renewal | CDC/dump → Build Store → snapshots → gds.* endpoint; Neo4j untouched (the exact PRD three-plane shape, K1) | Zero-risk coexistence: nobody else offers "keep Neo4j, halve the machine" — competitors all demand migration first (W4) | Watermark-exact parity harness vs GDS outputs on customer's own graph, side-by-side bill | **First revenue.** Support subscription + per-node/instance pricing at ~1/5 the equivalent AuraDS GB-hour bill (W1 anchor). Land here, expand later |
| O3 | **GraphRAG memory engine** — embedded/serverless graph+algorithms for LLM agent memory | AI engineer building agent memory / GraphRAG (W3); trigger = "my knowledge graph outgrew NetworkX but I refuse to run a JVM cluster" | Embedded or single-binary mode: snapshots + WCC/Louvain/PageRank/FastRP for community-summarization RAG loops, tiny footprint | Second-wave buyers (W3) are RAM-poor (they spend on GPUs) and API-pragmatic; the 7 families are EXACTLY the GraphRAG loop (community detection + centrality + embeddings) | GraphRAG pipeline demo: entity graph → Leiden communities → summaries, on a dev box, wired to an agent framework | **Growth vector.** Free embedded; paid managed/serverless per-session (mirrors Aura Graph Analytics' $0.40/GB-hr model at a fraction — our GB count is the moat, W1) |
| O4 | **The honesty SLA** — receipts, reject-before-execute, watermark-exact snapshots as an ops product | data platform / SRE owner; trigger = the third 2 a.m. OOM page or a compliance ask for "which data version produced this score" | Admission control + memory receipts + generation catalog + audit trail (K6) as enterprise features | Nobody sells deterministic analytics ops in this market (research: no competitor markets estimates/receipts at all) | Chaos-test: run a hostile workload mix for a week on a fixed budget with zero OOMs and full audit lineage | **Enterprise tier.** Per-instance enterprise license (RBAC, audit, SLA). This is the renewal-safe revenue O2 lands into |
| O5 | **Fraud/ER solution bundles** — packaged WCC+Louvain+KNN pipelines with fixtures and dashboards | fraud ops / MDM lead at mid-market fintech; trigger = fraud team asked for GDS, IT said no budget for a 128 GB cluster | Opinionated pipelines over O1/O2 for the two highest-weight use cases (K2: fraud/ER ≈ families 1,2,4) | Vertical packaging of the exact algorithms fraud buys (S7 in Arch02), at commodity-hardware cost | One lighthouse case study: fraud-ring detection at a customer, cost-per-run vs prior stack | **Deal-size expander.** Solution pricing (annual), services-light. Do NOT build this before O2 has 5 happy users |

### The sequencing rule (Shreyas: strategy = what you refuse to do now)

```text
Now (0-2 quarters):   O1 only. One benchmark, seven families, receipts.
Next (2-4 quarters):  O2 at Neo4j shops; O3 demos into the GraphRAG wave.
Later (year 2):       O4 enterprise tier; O5 only after lighthouse pull.
Never (as strategy):  speed-first positioning; 575-procedure completionism
                      before the seven families are excellent; greenfield
                      DB-war against Kuzu/FalkorDB on their turf.
```

---

## Part 3: Monetization — Or, If Adoption Is The Goal, The Replacement Narrative

### 3.1 Monetization logic in one paragraph

RAM-metered incumbent pricing (W1) is our pricing umbrella: we sell the same
completed workload at a fraction of the GB-hours, and even a generous margin
leaves the customer 3-10x better off. Monetize the OPERATED and GUARANTEED
versions (O2 sidecar, O3 serverless, O4 honesty SLA), never the engine —
the engine's job is distribution. This is the standard OSS wedge (engine
free, ops paid) with one twist that Shreyas would insist on naming: our COGS
advantage is structural (we need smaller machines to serve the same
workload), so managed-service margin is durable, not promotional.

### 3.2 If only adoption matters: the replacement narrative

Shreyas on narratives: a challenger narrative must (a) concede the
incumbent's greatness honestly, (b) reframe the buying criterion to your
axis, (c) make the first step feel reversible. Never "Neo4j is bad" — Neo4j
is excellent and beloved; attacking it makes its users defend it.

**The narrative, in three sentences (the repeatable version):**

```text
"Neo4j taught the world graphs — and then priced graph analytics by the
gigabyte of RAM, so the algorithms that made it famous need the biggest
machine in your fleet. We rebuilt the same engine contract in Rust so your
existing Cypher and gds.* code runs on the machines you already have — it
tells you the memory bill before it runs, finishes jobs the big-box setup
couldn't, and every answer states exactly which snapshot of your data it
came from. Keep Neo4j for your transactions if you love it; point us at it
and stop renting RAM to run PageRank."
```

**Narrative mechanics (why each clause is load-bearing):**

| clause | job it does |
| --- | --- |
| "Neo4j taught the world graphs" | concedes greatness; recruits Neo4j-skilled users instead of insulting them (their skills transfer 1:1 — that's the compatibility promise) |
| "priced analytics by the gigabyte of RAM" | reframes the axis from features/speed (their turf) to cost-of-RAM (our turf, their billing dimension — W1). The buyer can verify this on Neo4j's own pricing page, which makes the narrative feel discovered, not asserted |
| "runs on machines you already have" | converts a spec (low RAM) into an experienced moment (no procurement ticket). The PMF trigger is the moment a data scientist runs FastRP on a laptop |
| "tells you the memory bill before it runs" | introduces the honesty category (O4) as a user-visible delight, pre-seeding the enterprise story |
| "finishes jobs the big-box setup couldn't" | the capability unlock (NodeSim/FastRP at scale) — challenger narratives need one "impossible → possible" claim, not ten "cheaper" claims |
| "keep Neo4j for transactions" | makes step one reversible (O2 sidecar), which is what actually gets brownfield pilots approved. Full replacement is a year-2 conversation the customer initiates, not us |

**The adoption flywheel this narrative feeds:**

```text
OOM'd / sticker-shocked user
   -> runs O1 on their laptop against a Neo4j dump   ("it just ran")
   -> posts the receipt screenshot (est vs actual vs budget is inherently
      screenshot-able — design the CLI output for this)
   -> team adopts O2 sidecar next renewal cycle       ("keep Neo4j, halve
      the bill")
   -> ops falls for receipts + watermarks             (O4 renewal moat)
   -> GraphRAG teams in the same org pick O3 because it's already approved
```

**One metric to rule the narrative (Shreyas: pick the metric the narrative
implies, or the narrative is decoration):**

```text
North star: completed-runs-that-would-not-have-fit — the count of algorithm
executions whose memory receipt shows estimated requirement > the RAM of the
machine they ran on. Every unit of this metric is a story the incumbent
architecture cannot tell, and the narrative, the benchmark, and the product
all point at it.
```

### 3.3 Chain of verification

| # | claim to verify | verdict |
| --- | --- | --- |
| V1 | Neo4j Aura bills analytics by RAM GB-hours, including paused state (AuraDS)? | Verified vs Aura billing docs + pricing pages (W1). User should re-check current pricing before quoting externally. |
| V2 | FastRP on LDBC100 needs 212-254 GB in Neo4j's own guide? | Verified vs two editions of the GDS Configuration Guide (Arch02 S3). |
| V3 | Memgraph is in-memory (i.e., not a low-RAM alternative)? | Verified (W4 sources); its pitch is speed, not footprint. |
| V4 | Kuzu/FalkorDB are not Neo4j/GDS API-compatible? | Verified directionally (own APIs / partial Cypher; no gds.* surface). "Nobody sells drop-in low-RAM compatibility" claim holds as of research date. |
| V5 | GraphRAG wave makes graph+community-detection standard RAG infra? | Verified (Microsoft GraphRAG 2024, ISO GQL, 2026 landscape write-ups, W3). |
| V6 | Our wall-time/cost multipliers (1.5-5x slower, 5-15x cheaper)? | ESTIMATES from Arch02 signatures + W1 pricing math; not yet measured. Must be replaced by the X1'-X3' experiment results before any external claim. |

Honest weakness: V6 is the entire quantitative spine and it is currently a
model, not a measurement. Shreyas's closing note would be exactly this —
the single highest-leverage act available is running the three Arch02
experiments and turning this document's estimates into receipts.

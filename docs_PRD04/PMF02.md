# PMF02: The Evidence Corpus Meets The Go-To-Market — Wedge-Order Timelines

Date: 2026-07-08
Method: Timeline Traverser, second PMF iteration. PMF01 defined the
differentiation stack, the O1-O5 offering ladder, and the replacement
narrative. This iteration ingests the `graph-database-rewrite-references-202606`
corpus and does two things: (1) re-price PMF01's claims against source-level
evidence, (2) simulate the genuinely open GTM fork — WHICH WEDGE GOES FIRST —
as three timelines. Companion to `Arch03.md` (the build-order twin of this
document; the two decisions are coupled and the coupling is priced below).

---

## Part 0: What The Corpus Changes About The PMF Story

### Claims re-priced (honesty pass on PMF01)

| PMF01 claim | corpus evidence | verdict |
| --- | --- | --- |
| "Memory receipts are a differentiator" | GDS already ships memory estimation as a public API (`.estimate` procedures, estimate objects — patterns-1 Pattern 13) | **NARROWED.** The differentiator is not estimating — Neo4j does that. It is ENFORCING: reject-before-execute, budgeted/spilled plans that FINISH, receipts that reconcile estimate vs actual. Sell the guarantee, not the number. Marketing language must never say "we estimate memory" (they do too); say "we finish or we tell you before we start" |
| "Drop-in compatibility is a huge, diffuse risk" | Compatibility = five separately-testable contracts, each with an existing oracle: Testkit protocol scripts for Bolt/drivers, signature-first metadata for procedures, grammar corpora for Cypher (patterns-1 Patterns 3, 8, 10) | **DE-RISKED.** "Compatible" becomes a checklist with pass/fail evidence per contract — which also becomes marketing collateral ("passes N Testkit scenarios") instead of an unfalsifiable adjective |
| "Honesty SLA (O4) is a plausible enterprise tier" | patterns-5 provides the full operational skeleton: metrics minimums, trace fields, four benchmark tiers, failure-injection families, "no performance claim without benchmark artifacts" | **STRENGTHENED.** O4 is no longer a vibe — it has a bill of materials. The failure-injection families (protocol/storage/transaction) are exactly what a chaos-test-backed SLA demo needs |
| "1.5-5x wall, 5-15x cost" multipliers | Substrate prior art (DataFusion pools, GridGraph shards — patterns-3 §8, patterns-4 T8) lowers the engineering cost of the honest paths, not the wall-time estimates | unchanged, still MODELED not measured; benchmark-artifact discipline from patterns-5 is now the required format for replacing them |
| "Nobody sells low-RAM Neo4j compatibility" (W4) | Corpus confirms the two nearest systems' shapes: Kuzu = CSR/columnar but own API; FalkorDB = sparse-matrix but path-materialization memory traps (patterns-2 Patterns 3, 4) | holds |

### New PMF ammunition found in the corpus

```text
N1. The Testkit story: "our Bolt implementation passes the same conformance
    scripts Neo4j's own drivers are tested with" is a trust-transfer device
    no adjacent competitor can use (they aren't wire-compatible at all).
N2. The benchmark-artifact discipline (patterns-5: command.txt, machine.json,
    metrics.prom, summary.json per run) IS the credibility strategy for a
    challenger whose whole pitch is a benchmark. Publish runs as artifact
    bundles, not blog charts.
N3. The GDS estimate API being public is also an OFFENSIVE tool: run
    gds.*.estimate on the customer's own graph on THEIR Neo4j to print the
    RAM bill Neo4j itself predicts — then run the same workload on ours on
    a small box. The incumbent's own honesty API becomes our demo's opening
    slide.
N4. Five-contract structure lets us ship compatibility CLAIMS incrementally:
    "Bolt: full (Testkit-verified). Cypher: subset (documented). GDS: 7
    families + honest rejection elsewhere." Checklist honesty beats
    adjective marketing and matches the PRD's registry culture.
```

---

## Decision Frame

- **Fork in the road:** Which wedge ships FIRST and defines the company's
  first public year: **O1** (OSS engine + published benchmark), **O2**
  (design-partner sidecar next to a real Neo4j shop), or **O3** (GraphRAG/
  agent-memory embedded engine for the AI wave)?
- **Desired outcome:** Within a year: one repeatable adoption motion with
  real users, evidence (per N2) that the core claim is true, and a funnel
  toward paid O2/O4 without having poisoned pricing or positioning.
- **Hard constraints:** solo/small-team capacity (one wedge at a time);
  engine reality follows Arch03 (substrate-first ⇒ Bolt surface matures
  ~Q3); no external performance claims without artifact bundles (N2);
  PMF01's "never lead with speed" rule stands.
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1.
- **What counts as failure:** a year of GitHub stars with zero workload
  owners; OR a design partner promised compatibility the engine can't
  honor (PMF01 PM5 + Arch03 Timeline B's failure mode leaking into GTM);
  OR pricing anchored so low the O4 tier can never recover margin.

Assumptions stated: (1) Arch03's recommended build order (substrate-first,
Testkit stub early) is adopted — each timeline below notes where it fights
that order. (2) The seven-family engine hits internal flagship-benchmark
quality around end of Q1 (Arch03 Timeline A). (3) No funding event changes
capacity mid-year.

---

## Timeline A: Benchmark-First OSS (O1 leads)

- **Opening move:** Build in quiet; the first public artifact is the
  flagship benchmark — NodeSim + FastRP on a 50 GB-class graph finishing
  on a 16 GB box — published as a patterns-5 artifact bundle (N2) with
  the N3 opener (Neo4j's own gds.*.estimate output on the same graph).
- **Week 1:** Nothing public. Internally: fixture graphs, bench CLI,
  artifact-bundle tooling (cheap now, priceless at publication).
- **Month 1:** Still quiet. The discipline cost is real: every peer is
  shipping GraphRAG demos into the 2024-26 wave and the founder is
  writing spill code. A private "receipt screenshot" starts circulating
  to 5-10 trusted graph people for narrative testing.
- **Quarter 1:** Publication. The N3 demo structure makes it
  self-verifying: readers can run Neo4j's estimate themselves. If the
  claim holds, this is the highest-credibility launch available to a
  solo challenger — HN/graph-community traction, inbound from exactly
  the people whose GDS jobs OOM. Risk realized here if the benchmark is
  even slightly gameable-looking: challenger benchmarks get audited by
  hostile experts within days (mitigation: artifact bundles + reproducer
  script, N2).
- **Long-term shape (Year 1):** OSS engine with a reputation anchored to
  one falsifiable, reproduced claim; O2 design-partner conversations
  start WARM in Q2-Q3 ("we saw the benchmark") with the sidecar as the
  natural paid follow-on; Bolt compat lands Q3 per Arch03, converting
  benchmark-readers into actual users. Monetization begins ~Q4.
- **Likelihood of year-end shape:** ~70%.
- **Stress points:** the silent first quarter (identical emotional shape
  to Arch03 Timeline A — the two quiet paths compound); benchmark-audit
  anxiety at launch.
- **Inflection points:** whether the benchmark survives third-party
  reproduction week. Survive = the year is made; stumble = restart with
  damaged credibility (this is the timeline's one concentrated bet).
- **Lived experience:** monastic then explosive; one quarter of doubt
  traded for a launch that does its own arguing.

## Timeline B: Design-Partner Sidecar First (O2 leads)

- **Opening move:** Recruit 2-3 Neo4j shops with OOMing GDS jobs (fraud/
  ER mid-market per PMF01 O5 profile) as unpaid design partners; build
  the sidecar against THEIR graphs and THEIR seven-family subset.
- **Week 1:** Outreach + discovery calls. First reality check arrives
  immediately: partners' first question is "does it run OUR Cypher?" —
  pulling compat work forward, directly against Arch03's substrate-first
  order. The GTM choice starts fighting the build order in week one.
- **Month 1:** One partner signed; their workload is (say) WCC + Louvain
  on a 40 GB graph. Engine work re-prioritizes to their exact shape —
  which is 80% aligned with the seven-family roadmap (good) and 20%
  bespoke Cypher/loader glue (pure distraction, but it buys the most
  valuable asset in B: a real graph with a real bill).
- **Quarter 1:** The partner's job finishes on their 16 GB VM. This is
  Timeline A's benchmark WITH A WITNESS — nominally stronger evidence.
  But: partner data is confidential (can't publish the graph), the
  result generalizes worse ("works for them" vs "reproduce it yourself"),
  and the team now owes support to 1-3 pilots while the engine is
  pre-Bolt-maturity. Velocity on families 5-7 drops ~30%.
- **Long-term shape (Year 1):** 2-3 lighthouse case studies, first
  revenue in Q3 (earliest of any timeline), deep workload knowledge —
  and an engine whose shape tilted toward its first partners' quirks. A
  public launch still requires doing Timeline A's benchmark work anyway,
  now in year 2 with less novelty.
- **Likelihood:** ~55% (recruiting good partners pre-proof is itself a
  ~50/50 gate for an unknown solo project).
- **Stress points:** support burden pre-product; the partner who asks
  for procedure #8; confidentiality blocking the public proof.
- **Inflection points:** partner selection. A partner whose workload IS
  the seven families = compounding; a partner with exotic needs = the
  engine becomes a consultancy deliverable (PMF01's O5-too-early trap).
- **Lived experience:** warm and grounding (real users, real thanks),
  with a persistent background hum of obligation; the calendar fills
  with other people's urgencies.

## Timeline C: GraphRAG-First (O3 leads)

- **Opening move:** Ship the embedded/single-binary mode into the
  2024-26 GraphRAG wave (PMF01 W3): entity graph -> Leiden communities ->
  community summaries -> agent memory, on a dev box, integrated with one
  popular agent framework.
- **Week 1:** A GraphRAG quickstart demo is buildable almost immediately
  (small graphs, no spill needed) — fastest public start of the three.
  Distribution surface (AI engineers) is 100x larger than graph-DB
  buyers and RAM-poor by constitution (GPU budgets).
- **Month 1:** Real traction signals possible (stars, quickstart clones).
  But the wedge quietly redefines the product: GraphRAG users need
  Louvain/Leiden + PageRank + embeddings on graphs that mostly FIT IN
  RAM — the honest-out-of-core differentiator is invisible at this
  workload size, and the competition is not Neo4j but Kuzu, FalkorDB,
  NetworkX, and every vector-DB adding graph features. We chose the one
  arena where our moat doesn't bind (corpus: Kuzu/FalkorDB are exactly
  strong here, patterns-2 P3/P4).
- **Quarter 1:** Fork in the fork: either GraphRAG demand grows into
  bigger-than-RAM territory (agent memory accumulating for months —
  plausible but unproven in 2026) and the moat re-engages, or the
  project is now a nice embedded graph library in a crowded field,
  drifting from the PRD's Neo4j-replacement thesis and the 575-procedure
  registry culture that justifies it.
- **Long-term shape (Year 1):** Widest funnel, weakest qualification;
  possible breakout if agent-memory-at-scale materializes on schedule;
  otherwise a pivot-shaped year. Neo4j-compat work stalls (its buyers
  aren't in this funnel), making a later return to O1/O2 a restart.
- **Likelihood of a strong year:** ~35%, with the highest variance of
  the three (this is the venture-style bet).
- **Stress points:** competing with well-funded embedded engines on
  their turf; watching the differentiation sit unused.
- **Inflection points:** the first user whose agent memory outgrows RAM —
  if that user appears organically by ~Q2, C converts into the best
  version of A (moat + wave); if not, sunk quarter.
- **Lived experience:** energetic, trend-adjacent, crowd-validated —
  and strategically anxious, because applause is coming from people who
  don't need the thing we're uniquely good at.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A Benchmark-First | self-verifying credibility; aligns perfectly with Arch03 build order; warm O2 funnel | one silent quarter; single concentrated bet on reproduction week | high | low | benchmark integrity; patience |
| B Sidecar-First | earliest revenue; real workloads shape the engine; witnesses | fights the build order from week 1; confidential proof; support tax pre-product | medium | medium-high | 2-3 well-chosen partners existing and behaving |
| C GraphRAG-First | biggest funnel; rides the wave; fastest public start | moat doesn't bind at wave workload sizes; wrong competitors; thesis drift | low-medium (audience switch = restart) | HIGH unless agent-memory-at-scale arrives on schedule | a market timing event outside our control |

Coupling with Arch03 (the decisive observation): **A is the only wedge
whose demands are exactly the substrate-first build order's outputs.** B
pulls Bolt/Cypher forward (Arch03 Timeline B's known failure shape); C
pulls embedded-mode + small-graph polish forward (work the composite
architecture treats as an eventual facade, patterns-2 Pattern 11). GTM
and build order are one decision wearing two hats.

---

## Decision Filter

**Which path is strongest if everything goes normally?**
**A (Benchmark-First OSS), with B's discovery calls run in parallel from
month 1 at zero build cost:** talk to 5-10 OOM-suffering Neo4j shops
during the quiet quarter — not as partners, as informants — so the
benchmark workload mirrors real bills, and the Q2 design-partner list is
pre-qualified the day the benchmark lands. Keep a weekend-grade GraphRAG
quickstart in the repo (C's funnel at 2% of C's cost) to catch the
agent-memory-outgrows-RAM user if they appear early — that user is the
signal that unlocks O3 properly.

**Which path is safest if things go badly?**
Still A: its worst case (benchmark needs another quarter) wastes time
but nothing else; B's worst case burns partner trust and engine shape;
C's worst case relocates the company to a market where its moat is
irrelevant. A is also the only failure that stays private.

**What experiment or conversation would reduce uncertainty fastest?**
```text
Y1. The N3 dry run (1 week, do immediately): on a public 50 GB-class
    graph, run gds.*.estimate for the seven families on stock Neo4j and
    publish nothing — just confirm the incumbent's own API prints the
    RAM bills our narrative depends on. If those numbers are smaller
    than K3/W2 suggest, the entire pitch re-prices NOW, cheaply.
Y2. Ten OOM interviews (3 weeks, parallel): find ten people who have
    actually hit GDS memory failures (forums, GitHub issues, r/Neo4j).
    Ask what they did next. If most say "we sampled the graph and moved
    on," the pain is episodic, not budget-worthy — B and half of A's
    thesis weaken, and the honest wedge may be O3 after all.
Y3. Reproduction-week rehearsal (2 days, before launch): give the
    artifact bundle to two hostile-competent friends with "break this."
    Cheap inoculation against Timeline A's single concentrated risk.
```

---

## Chain of Verification

| # | question | answer | status |
| --- | --- | --- | --- |
| V1 | Does GDS really expose public memory estimation, requiring the PMF01 "receipts" claim to be narrowed? | Yes — patterns-1 Pattern 13; PMF messaging updated to "enforcement, not estimation" throughout. | verified, claim corrected |
| V2 | Is Testkit genuinely usable as third-party conformance evidence? | patterns-1 Pattern 10 documents it as Neo4j's own driver oracle; using it for a non-Neo4j server is novel but mechanically supported (scripted protocol scenarios). | verified with caveat |
| V3 | Are Kuzu/FalkorDB really strongest exactly in the GraphRAG-size arena? | Corpus: Kuzu CSR/columnar analytics (patterns-2 P3), FalkorDB sparse-matrix traversal with path-materialization caveats (P4); both embedded-friendly. Supports Timeline C's competitive read. | verified |
| V4 | Do the artifact-bundle requirements exist as stated? | Yes — patterns-5 required run artifacts list, benchmark tiers, and the "no performance claim without benchmark artifacts" rule. | verified |
| V5 | Are the timeline likelihoods measured? | No — judgment; Y1/Y2 exist precisely to convert the two most load-bearing market assumptions (the RAM bill, the pain depth) into data before launch. | honest-uncertainty |

## One-Sentence Summary

```text
The corpus narrows the differentiation from "we estimate memory" (Neo4j
already does) to "we ENFORCE and FINISH — verified by the incumbent's own
estimate API on one side and Testkit-grade conformance artifacts on the
other," and the wedge-order timelines say: lead with the self-verifying
public benchmark (O1), run informant interviews in its shadow, keep a
cheap GraphRAG quickstart as a listening post, and let design-partner
revenue (O2) arrive warm in Q2 instead of cold in week one.
```

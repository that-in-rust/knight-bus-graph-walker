# PMF04: Selling A Format, Not Just An Engine — The "Parquet Moment" Timelines

Date: 2026-07-08
Method: Timeline Traverser, fourth PMF iteration. Greenfield framing per
instruction (current repo = POC only). Arch05 proposed GRAIN v0, a storage
format whose manifest is a closed-form memory estimator. This document asks
the PMF question that a format-first architecture newly unlocks: **is the
product the engine, the format, or the receipt?** — and simulates the three
answers. All external claims carry [R#] URLs in References.

---

## Part 0: What Arch05 Changes About The PMF Position

Previous PMF iterations sold an ENGINE (run the seven families cheaply,
honestly). A spec-first format adds two genuinely new commercial objects:

```text
N1. THE SPEC AS DISTRIBUTION. Columnar analytics had its "Parquet moment":
    an open FORMAT (spec + permissive license) became the industry's
    neutral ground, and engines competed above it [R1][R2]. Graph OLAP has
    NO equivalent: there is no vendor-neutral, spec-documented, immutable
    graph-snapshot format designed for analytics. (GraphAr, an Apache
    incubating project, is the closest motion and validates the demand
    for one [R3] — but it standardizes archival layout, not workspace
    predictability.) A 20-page GRAIN spec is a wedge that costs nothing
    to copy — which is the point: copies spread the manifest contract.
N2. THE RECEIPT AS THE UNIT OF TRUST. If memory cost is manifest
    arithmetic (Arch05 G3), then a receipt — "this run needs 6.2 GB, here
    is the polynomial and the manifest fields it read" — is auditable by
    a third party WITHOUT our code. The incumbent's estimate is a black-
    box guess, guaranteed only for production-tier algorithms [R4], on a
    platform whose sessions bill by pre-declared RAM size [R5][R6]. An
    auditable estimate attacks the billing axis at its root: you cannot
    overprovision what you can price from a KB of metadata.
```

Carried forward unchanged from PMF03 (all still verified): the RAM meter is
intact — Aura Graph Analytics bills GB-minutes on declared session RAM with
a 10-minute minimum, AuraDS bills RAM even paused [R5], list price
$0.40/GB/hour [R6]; the serverless-convenience ground is taken (GA May 2025
[R7]); the Kuzu vacuum is open (archived Oct 2025 [R8][R9], reported Apple
acquisition Feb 2026 [R10]); GraphRAG demand for Leiden-family workloads is
citable and cost-anxious [R11][R12].

---

## Decision Frame

- **Fork in the road:** What is the PRODUCT the first year sells:
  **A:** the open format + reference engine ("Parquet play"), **B:** the
  engine with the format as internal detail (classic PMF01-03 wedge,
  re-run on greenfield), or **C:** the receipt/estimation layer itself as
  the product (estimator-as-a-service over anyone's graph metadata)?
- **Desired outcome:** in 12 months: one adoption motion where the format-
  first architecture produces a moat an engine-only competitor can't
  copy cheaply.
- **Hard constraints:** solo capacity; Arch05's inside-out build order
  (manifest+hot stratum week 1) fixes what exists when; no performance
  claims without artifact bundles; spec must actually be short enough to
  be adopted (the moment it needs a committee, the play dies).
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1; kill criteria
  with check dates.
- **What counts as failure:** a "standard" nobody implements twice (a
  spec with one implementation is just documentation); OR giving away the
  engine's differentiation inside a spec others out-execute; OR selling
  receipts no procurement process knows how to buy.

Assumptions stated: (1) Arch05's X1 verdict (NodeSim/Louvain manifest
polynomials hold within ±30%) — if it fails, C dies and A weakens; (2) the
Parquet analogy transfers at least partially to graph OLAP (GraphAr's
existence is evidence demand exists [R3], not evidence we'd win it); (3)
spec adoption dynamics run on years, engine adoption on quarters — the
timelines price this clock difference explicitly.

---

## Timeline A: Format-First ("the Parquet play")

- **Opening move:** Publish the GRAIN v0 spec (20 pages, permissive
  license) simultaneously with the reference engine's first receipt —
  never a spec without a running proof, never an engine without the spec.
- **Week 1:** Spec draft public as PR-able markdown; the manifest schema
  and one worked estimate example (PageRank) are the whole launch.
  Audience: engine builders and data-platform people, not end users.
- **Month 1:** First external reactions arrive shaped as spec review
  (better than stars: reviewers are implementers). GraphAr-adjacent
  people notice — the positioning sentence matters: "GraphAr archives
  graphs; GRAIN prices computation over them" [R3]. Risk surfaces: a
  spec invites bikeshedding that a solo maintainer must ration.
- **Quarter 1:** The credibility gate: a SECOND reader — a Python reader,
  a DuckDB extension sketch, or one RAG framework loading GRAIN directly
  — even if we write 80% of it. One spec + two readers = a format; the
  benchmark ("estimated from a KB, finished on 16 GB") now advertises
  the FORMAT, and every future engine that adopts it inherits our
  receipt contract — the moat is the contract's spread, not the code.
- **Long-term shape (Year 1):** the neutral-ground position: our engine
  is "the reference implementation of the honest graph format" — a title
  engine-only competitors cannot take without adopting our spec first.
  Monetization stays PMF03-shaped (engine sidecar, support), but every
  spec adopter is channel, not competition.
- **Likelihood:** ~50% for the two-reader gate in year 1. **Kill
  criterion:** zero external spec engagement (issues, review, reader
  attempts) by month 3 → demote spec to documentation, continue as B.
  **Check date:** end of month 3.
- **Stress points:** bikeshed rationing; the fear of arming competitors;
  writing docs while wanting to write Rust.
- **Inflection points:** the second reader; the first spec change
  REQUESTED by an outsider (that's adoption, wearing a complaint).

## Timeline B: Engine-First, Format Internal ("classic wedge, greenfield")

- **Opening move:** PMF03's corrected sequence unchanged: benchmark-first
  OSS engine aimed at "your hardware, your data, finishes anyway," with
  the embedded GraphRAG quickstart rushed into the Kuzu window [R8];
  GRAIN stays an undocumented internal detail.
- **Week 1-Month 1:** Faster than A (no spec upkeep); the quickstart
  catches the orphan window while it's warm; OOM interviews proceed.
- **Quarter 1:** The re-aimed benchmark lands as in PMF03-A. But a quiet
  cost accrues: every "how do I read your files from Python?" issue gets
  answered with code instead of a spec, and by Q3 an informal, frozen-by-
  usage format exists ANYWAY — undocumented, unversioned, retrofitted
  later at 5x the cost (formats calcify the moment third parties depend
  on bytes).
- **Long-term shape (Year 1):** PMF03's outcome — respectable engine
  adoption, warm sidecar pipeline — minus the neutral-ground moat, plus
  format debt. Competitors who DO publish a spec (or GraphAr expanding
  scope [R3]) can claim the standards ground uncontested.
- **Likelihood:** ~65% of hitting its own (engine-shaped) year-1 goals.
  **Kill criterion:** n/a (this is the default); the check is whether
  format questions in the issue tracker exceed ~1/week by Q2 — the
  signal that A's play was available and is being forfeited. **Check
  date:** end of Q2.
- **Stress points:** the known PMF03 set (silent quarter, audit week).
- **Inflection points:** the first third party that reverse-engineers
  the file layout — proof the spec had an audience.

## Timeline C: Receipt-as-Product ("sell the estimator")

- **Opening move:** Productize G3 alone: a tool that reads graph metadata
  (ours, or degree stats sampled from any Neo4j/CSV source) and prints
  auditable workspace receipts for the seven families — "know your
  session size before you rent it" — aimed squarely at Aura's declare-
  RAM-up-front session model [R5].
- **Week 1:** CLI that ingests degree distributions and emits priced
  plans. Demo: "Aura will ask you to declare a session size [R5]; here
  is the number, derived, with the polynomial shown."
- **Month 1:** The awkward truth surfaces: a receipt without an engine
  that HONORS it is advice, not product. Users who get a 40 GB estimate
  still need somewhere to run the job; we've quantified pain and handed
  the purchase to... the incumbent's bigger session tier.
- **Quarter 1:** Pivot pressure: either bundle the engine (→ becomes A
  or B) or chase the "FinOps for graph analytics" niche — real but tiny,
  and dependent on incumbent pricing opacity persisting.
- **Long-term shape (Year 1):** best case, a beloved free sizing tool
  that funnels into the engine (i.e., C was A/B's marketing all along);
  worst case, a consulting practice.
- **Likelihood as standalone product:** ~15%. **Kill criterion:** if by
  month 2 no user pays or converts, fold the CLI into A/B as the demo
  layer (its true role). **Check date:** end of month 2.
- **Stress points:** watching the tool's insight monetize someone else's
  compute.
- **Inflection points:** the first user who asks "ok, so where do I RUN
  it under this budget?" — the question is the pivot.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A Format-First | neutral-ground moat; adopters become channel; receipt contract spreads beyond our code | slower start; bikeshed tax; arms competitors if we stall | high (degrades to B) | medium | one external second-reader appearing in year 1 |
| B Engine-First | fastest engine traction; PMF03's verified plan; catches the perishable Kuzu window | forfeits the standards ground; format calcifies undocumented | medium (spec retrofit ~5x cost) | MEDIUM-HIGH (the quiet kind) | nothing new — that's its appeal and its trap |
| C Receipt-Only | cheapest build; attacks the billing axis directly [R5][R6] | advice-not-product; funnels demand to incumbent capacity | high (folds into A/B) | low (cheap to try) | buyers existing for sizing-as-a-product (doubtful) |

The clock structure decides it: **B's opportunities are perishable
(Kuzu window [R8], benchmark novelty), A's are durable (standards ground
stays open until someone takes it), C's is a feature wearing a product
costume.** Perishable-first, durable-layered: run B's motions on B's
clock, but wearing A's artifacts — publish the spec early precisely
because it costs A almost nothing extra WHILE the engine work is
happening anyway, and fold C in as the free demo layer it was born to be.

---

## Decision Filter

**Which path is strongest if everything goes normally?**
**B's calendar wearing A's clothes:** ship PMF03's sequence (quickstart
into the Kuzu window now, re-aimed benchmark in Q1) but publish the GRAIN
v0 spec + manifest schema in week 1 as part of the engine's documentation
— a spec-shaped README costs days, reserves the neutral ground, and lets
the two-reader gate be tested by the market at zero campaign cost. C's
CLI ships as the benchmark's opening demo ("here's the receipt; here's
the run"), never as a standalone SKU.

**Which path is safest if things go badly?**
The same hybrid: if the spec finds no audience it quietly remains
documentation (no loss); if the engine stalls, the spec + receipts are
salvageable IP with independent credibility; C-as-demo risks nothing.

**What experiment would reduce uncertainty fastest?**
```text
Z1 (gates everything, week 1, paper): Arch05-X1 — the NodeSim/Louvain
    manifest polynomials. If they fail, the receipt story downgrades to
    "hints" and this document's N2 weakens honestly.
Z2 (week 2, 1 day): GraphAr positioning probe — read the GraphAr spec
    [R3] deeply; write the one-paragraph "GRAIN vs GraphAr" note. If the
    honest answer is "GraphAr + a manifest extension would do," the right
    move may be contributing the estimator contract THERE — standards
    judo instead of standards war. Cheapest possible test of the whole
    Parquet analogy.
Z3 (month 1, interviews): add one question to PMF03's Z3 OOM interviews:
    "if your files were readable by other tools, would that change your
    willingness to adopt a young engine?" — the lock-in-fear signal is
    the format play's demand evidence.
```

---

## Chain of Verification

| # | question | answer | status |
| --- | --- | --- | --- |
| V1 | Is the "Parquet moment" characterization accurate — open columnar format, engines competing above it? | Yes — Apache Parquet is an open, vendor-neutral columnar format spec with many independent implementations [R1][R2]. Analogy transfer to graphs is a judgment. | verified fact, judged analogy |
| V2 | Does a graph-storage standardization effort exist, validating demand? | Yes — Apache GraphAr (incubating) standardizes graph data files for lake/archival interchange [R3]. Its scope differs from GRAIN's (no workspace/estimation contract) — verified by reading its stated goals; readers should verify. | verified with caveat |
| V3 | Do Aura sessions really require declaring RAM size up front, billed GB-minutes, 10-min minimum? | Yes — Aura billing-dimensions docs [R5]; $0.40/GB/hour list price [R6]. | verified, web |
| V4 | Is the incumbent estimator's production-tier-only limitation real? | Yes — GDS memory-estimation docs [R4]. | verified, web |
| V5 | Kuzu window and GraphRAG demand still current? | Archived Oct 2025 [R8][R9]; Apple report Feb 2026 (single outlet, verify) [R10]; microsoft/graphrag active with Leiden-based pipeline and cost warnings [R11][R12]. | verified with caveat |
| V6 | Are the timeline likelihoods measured? | No — judgment with kill dates; Z1/Z2 convert the two load-bearing bets (polynomials, Parquet analogy) into evidence within two weeks. | honest-uncertainty |

## One-Sentence Summary

```text
A format designed around estimation creates two products an engine never
had — a spec that turns adopters into channel and a receipt a third party
can audit without our code — and the timelines say: keep PMF03's engine
calendar (the Kuzu window is perishable), but wear the format's clothes
from week 1 (spec-shaped docs, receipt-led demos), test the whole Parquet
analogy for the price of reading GraphAr's spec, and let the estimator
CLI be the demo it was born to be, never the SKU it can't be.
```

---

## References

Accessed 2026-07-08. External claims tagged [R#]; untagged content is
analysis/judgment. Readers should verify independently.

- **[R1]** Apache Parquet — open columnar storage format spec (vendor-neutral, multi-engine):
  https://parquet.apache.org/
- **[R2]** Apache Parquet format specification repository:
  https://github.com/apache/parquet-format
- **[R3]** Apache GraphAr (incubating) — open standard for graph data file storage/interchange:
  https://graphar.apache.org/
- **[R4]** Neo4j GDS Memory Estimation docs — `.estimate` mode; production-tier-only guarantee:
  https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/
- **[R5]** Neo4j Aura billing dimensions — Graph Analytics sessions billed GB-minutes on declared RAM, 10-minute minimum; AuraDS billed by RAM running or paused:
  https://neo4j.com/docs/aura/billing/billing-dimensions/
- **[R6]** Neo4j pricing page — Aura Graph Analytics at $0.40/GB/hour:
  https://neo4j.com/pricing/
- **[R7]** Neo4j press release, May 7, 2025 — Aura Graph Analytics GA (serverless, zero-ETL, any data source):
  https://neo4j.com/press-releases/aura-graph-analytics/
- **[R8]** The Register, Oct 14, 2025 — KuzuDB abandoned, community weighing forks:
  https://www.theregister.com/software/2025/10/14/kuzudb-graph-database-abandoned-community-mulls-options/
- **[R9]** kuzudb/kuzu README archival commit (Oct 2025):
  https://github.com/kuzudb/kuzu/commit/06890e1ac6bd31216f916526b933afc2a7802ec1
- **[R10]** BetaKit, Feb 2026 — reported Apple acquisition of Kuzu (single-outlet; verify independently):
  https://betakit.com/apple-strikes-deal-to-acquire-canadian-database-software-startup-kuzu/
- **[R11]** microsoft/graphrag — GraphRAG repository (indexing cost warning; 34k+ stars):
  https://github.com/microsoft/GraphRAG
- **[R12]** GraphRAG community-detection docs — hierarchical Leiden + community summaries:
  https://microsoft-graphrag.mintlify.app/concepts/community-detection

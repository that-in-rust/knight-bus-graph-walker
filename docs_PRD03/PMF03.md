# PMF03: Rubber-Duck Debugging PMF01/PMF02 — The World Moved; Re-Simulate

Date: 2026-07-08
Method: Timeline Traverser, third PMF iteration. This pass rubber-duck-debugs
PMF01 and PMF02 against FRESHLY VERIFIED external evidence (every external
claim now carries an [R#] pointing to an exact URL in the References section),
then re-simulates the go-to-market from the corrected world model. Companion:
`Arch04.md` (same debugging pass on the architecture line).

---

## Part 1: The Rubber-Duck Session — Bugs Found In PMF01/PMF02

```text
PMF-BUG-1 (The Incumbent Already Countered) — SEVERITY: HIGH
  PMF01's sidecar pitch leaned on "no persistent instance, no ETL, pay only
  when you run." Neo4j shipped exactly that: Aura Graph Analytics, GA
  May 7, 2025 — serverless, zero-ETL, any data source, 65+ algorithms,
  pay-as-you-go sessions [R7][R8][R9]. Part of our pitch is now their
  press release. What survives (verified): sessions are still billed by
  RAM — GB-minutes, "specify the allocated memory size ... multiplied by
  the RAM capacity used by the session," 10-minute minimum [R3]; Aura
  Graph Analytics lists at $0.40/GB/hour [R2]; and AuraDS instances are
  billed by RAM capacity "both in a running state and in a paused state"
  [R3]. The RAM meter is intact. Our differentiation must retreat from
  "serverless-ish convenience" (lost) to the ground they structurally
  cannot take: LESS RAM NEEDED AT ALL, on hardware the customer already
  owns, with data that never leaves it.

PMF-BUG-2 (The Embedded Competitor Died) — SEVERITY: HIGH, POSITIVE
  PMF02 priced Timeline C (GraphRAG-first) low partly because "Kuzu and
  FalkorDB are exactly strong here." Kuzu is gone: repo archived Oct 2025
  with a "working on something new" note [R11], covered as abandonment by
  The Register (Oct 14, 2025: community "mulling options," a fledgling
  Kineviz fork "bighorn") [R10], with a reported Apple acquisition
  disclosed Feb 2026 [R12]. The leading embedded analytics-oriented graph
  engine vanished mid-wave. PMF02's ~35% on GraphRAG-first was priced
  against a competitor that no longer exists.

PMF-BUG-3 (No Verifiable References) — SEVERITY: MEDIUM
  PMF01/02 asserted pricing, market and product facts without URLs.
  Fixed: References section below; every external claim tagged [R#].

PMF-BUG-4 (Estimate-API Nuance Incomplete) — SEVERITY: LOW
  PMF02 correctly narrowed "receipts" to enforcement (GDS ships a public
  .estimate mode [R4]). New detail found: only PRODUCTION-TIER algorithms
  are guaranteed an estimate mode ("only algorithms in the production-
  ready tier are guaranteed to have an .estimate mode" [R4]) — so
  universal receipts across OUR whole registry is a real, checkable
  differentiator, not just enforcement.

PMF-BUG-5 (GraphRAG Demand Was Hand-Waved) — SEVERITY: MEDIUM
  Now grounded: Microsoft GraphRAG (34k+ GitHub stars, MIT, actively
  released through 2026 [R13]) builds its pipeline on hierarchical LEIDEN
  community detection plus community summaries [R14][R15], and its own
  README warns "GraphRAG indexing can be an expensive operation" [R13].
  The wave's flagship pipeline runs exactly our algorithm families and
  publicly complains about cost. That is a documented, citable demand
  signal — not vibes.
```

What survives debugging: the wedge logic (distribution first, revenue
second), the never-lead-with-speed rule, the artifact-bundle evidence
discipline, and the replacement narrative's tone. What must be re-simulated:
the wedge ORDER, because PMF-BUG-1 weakens the sidecar (O2) and PMF-BUG-2/5
strengthen the embedded/GraphRAG wedge (O3) — the exact opposite of PMF02's
conclusion. Intellectual honesty requires rerunning the timelines.

---

## Decision Frame

- **Fork in the road:** With the incumbent occupying "serverless
  convenience" and the embedded-analytics leader dead: which wedge leads —
  **A:** benchmark-first OSS (PMF02's pick, re-tested), **B:** embedded
  GraphRAG engine into the Kuzu vacuum (re-priced up), or **C:**
  local-first "your hardware, your data" sidecar (the ground serverless
  concedes)?
- **Desired outcome:** one repeatable adoption motion in 12 months, on
  ground the May-2025 incumbent move cannot occupy, with every public
  claim citable ([R#]-grade).
- **Hard constraints:** solo capacity; Arch04's brownfield-substrate build
  order (Timeline A there) supplies the engine reality; no external
  claims without artifact bundles; PMF01's pricing-anchor caution stands.
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1, kill criteria
  and check dates included (imported discipline from Arch04 BUG-5 fix).
- **What counts as failure:** launching a pitch Aura Graph Analytics
  already makes better [R7]; OR inheriting Kuzu's orphans and becoming an
  unfunded general-purpose graph DB (Arch04 Timeline D path (b)); OR a
  year of stars with no workload owners.

Assumptions stated: (1) Aura Graph Analytics keeps RAM-metered session
billing (current docs [R3]); (2) the Kuzu vacuum stays open ≥2 quarters
(Apple absorption [R12] makes a community-facing revival unlikely soon,
but this is a judgment); (3) GraphRAG-style pipelines keep Leiden/community
workloads central [R14].

---

## Timeline A: Benchmark-First, Re-Aimed ("finish what their meter can't")

- **Opening move:** PMF02's plan with one re-aim: the flagship benchmark's
  framing changes from "cheaper than AuraDS" to "completes on YOUR 16 GB
  machine what their own estimator prices at a 128 GB session." Opening
  slide remains the incumbent's own `gds.*.estimate` output [R4] — now
  doubly potent because Aura sessions make you pre-declare the RAM size
  you'll pay for [R3]: their UX literally asks the question our engine
  makes unnecessary.
- **Week 1:** Quiet build (Arch04 Timeline A trunk); artifact-bundle
  tooling; the N3 dry run (estimate the seven families on a public
  50 GB-class graph via stock Neo4j) executes THIS week — it is the load-
  bearing fact and costs nothing [R4].
- **Month 1:** Ten OOM interviews proceed (PMF02 Y2), plus a new question
  added post-BUG-1: "did you evaluate Aura Graph Analytics sessions, and
  why (not)?" — mapping where serverless already mops up the pain.
  Expected split (to be verified): data-egress-restricted and
  hardware-owning shops can't or won't use it; those are our buyers.
- **Quarter 1:** Publication with the re-aimed framing. The incumbent's
  plausible response is now known-shape (serverless price cuts, marketing
  — Arch04 adversary layer): none of it reaches "runs on hardware you
  already own, data never leaves." The claim is positioned where the
  counter-move can't follow.
- **Long-term shape (Year 1):** as PMF02-A — warm O2 pipeline in Q2-Q3 —
  but O2's pitch is rewritten to lead with data locality + completion,
  price arbitrage second.
- **Likelihood:** ~70% (unchanged; re-aim costs nothing, removes the
  known counter). **Kill criterion:** if the N3 dry run shows estimator
  outputs materially below the K3/W2 expectations, re-price everything
  immediately. **Check date:** end of week 1.
- **Stress points:** the silent quarter; reproduction-week audit risk
  (mitigation Y3 rehearsal stands).
- **Inflection points:** N3 numbers; interview split between
  "serverless solved it" and "serverless can't touch our data."

## Timeline B: Embedded GraphRAG Engine ("the vacuum wedge")

- **Opening move:** Ship the embedded/CLI mode (Arch04 X2 facade) as a
  GraphRAG-pipeline backend: hierarchical Leiden + PageRank + k-hop +
  FastRP with memory receipts, quickstart against Microsoft GraphRAG-
  style outputs [R13][R14].
- **Week 1:** Quickstart public. The Kuzu orphans are actively deciding
  right now [R10]; being visibly alive and analytics-focused during
  their decision window is the whole point of moving fast.
- **Month 1:** Inbound arrives pre-sorted by Arch04 X3's community
  question. The discipline: answer analytics-shaped asks, publicly
  decline general-DB asks ("we are the analytics engine in your RAG
  stack, not your system of record") — the narrow promise that keeps
  this from being Kuzu cosplay.
- **Quarter 1:** If the analytics:general ratio holds ≥1:1, the engine
  becomes a listed backend in 1-2 RAG frameworks; GraphRAG's documented
  indexing-cost complaint [R13] gives the receipts feature a native
  audience (cost-anxious pipeline builders). If the ratio collapses,
  kill per Arch04 Timeline D and fold back to A.
- **Long-term shape (Year 1):** a niche-respected RAG-stack component
  with organic pull toward bigger-than-RAM agent memory — the moment
  that demand appears, this timeline converts into A's benchmark story
  with a waiting audience. Neo4j-compat surface develops slower.
- **Likelihood:** ~50% (up from PMF02's 35% — the competitor is gone
  [R10][R11] and demand is now citable [R13]). **Kill criterion:**
  analytics:general inbound ratio < 1:2 at month 2. **Check date:** end
  of month 2.
- **Stress points:** orphan asks pulling toward general-DB scope; Apple
  wildcard [R12]; two audiences (graph people, RAG people) needing
  different words.
- **Inflection points:** first framework listing; first
  agent-memory-outgrew-RAM user (converts B into the best version of A).

## Timeline C: Local-First Sidecar ("the ground serverless concedes")

- **Opening move:** Lead with O2 directly, repositioned: an on-prem
  sidecar for shops that CANNOT ship graphs to Aura — regulated data,
  air-gapped environments, data-egress policies. Zero-ETL against their
  existing Neo4j export path; receipts as the audit story.
- **Week 1:** Discovery outreach to regulated-industry Neo4j shops
  (fraud/AML profile from PMF01 O5). Slow first week — this buyer
  doesn't answer cold email fast.
- **Month 1:** The PMF02 Timeline-B problem recurs on schedule: first
  serious prospect asks for THEIR Cypher surface, pulling compat forward
  against the Arch04 build order; and enterprise security review begins
  before any code runs. Sales cycle length reveals itself.
- **Quarter 1:** Possibly one signed design partner — with confidential
  results (can't publish), support obligations, and an engine still
  pre-Bolt-maturity. The wedge is real (serverless genuinely can't serve
  these buyers) but its clock speed is enterprise, not open-source.
- **Long-term shape (Year 1):** 1-2 lighthouse accounts, first revenue
  earliest of the three, public credibility latest; the Timeline A
  benchmark still has to be built for year-2 scale-out.
- **Likelihood:** ~45%. **Kill criterion:** no signed design partner by
  month 3. **Check date:** end of month 3.
- **Stress points:** enterprise cycle vs solo capacity; confidentiality
  blocking the proof stream; bespoke glue accumulating.
- **Inflection points:** partner selection (workload = the seven
  families, or exotic); whether receipts satisfy a real security review.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A Benchmark-First (re-aimed) | self-verifying proof; positioned where the May-2025 counter-move can't follow; aligns with Arch04 trunk | one silent quarter; single audit-week bet | high | LOW | N3 numbers holding; benchmark integrity |
| B Embedded GraphRAG | competitor vacuum is open NOW [R10]; citable demand [R13]; fastest public start | scope pull toward general DB; two-audience messaging; compat stalls | medium-high (facade stays cheap) | medium | inbound staying analytics-shaped; vacuum staying open |
| C Local-First Sidecar | earliest revenue; ground serverless structurally concedes; real buyers | enterprise clock vs solo capacity; confidential proof; compat pulled forward | medium | medium-high | regulated buyers moving at startup speed (they don't) |

Timing asymmetry (the decisive new fact): A's window is stable (the RAM
meter isn't going away [R2][R3]), C's window is stable (regulation isn't
going away), but **B's window is open only while the Kuzu orphans are
choosing** — weeks-to-months, not quarters [R10]. Perishable opportunity
outranks durable ones in sequencing even when likelihoods are similar.

---

## Decision Filter

**Which path is strongest if everything goes normally?**
**A as trunk, B as the immediate cheap graft** — invert PMF02's "quickstart
as listening post at 2% cost" into "quickstart as an actively announced
wedge at ~10% cost," BECAUSE the vacuum is perishable and the competitor
died after PMF02 was priced. Concretely: week 1 = N3 dry run + publish the
embedded quickstart into the Kuzu-orphan conversation (Arch04 X3 question
attached); quarter 1 = the re-aimed benchmark. C is not led with, but its
buyers are interviewed in month 1 (they're in the OOM interview pool
anyway) so the Q3 sidecar pitch is pre-shaped.

**Which path is safest if things go badly?**
A: failure is private and time-priced only. B's failure mode is public but
cheap if the narrow promise is kept. C's failure mode (a partner promised
compat we can't honor) is the only reputation-damaging one — another
reason it follows proof rather than leads.

**What experiment would reduce uncertainty fastest?**
```text
Z1 (week 1, ~0 cost): N3 dry run — gds.*.estimate for the seven families
    on a public 50 GB-class graph [R4]. The single load-bearing number.
Z2 (week 1, 1 day): post the analytics-vs-general question where Kuzu
    orphans congregate [R10]; the reply ratio is Timeline B's kill data,
    gathered before building anything beyond the Arch04 X2 facade.
Z3 (month 1, interviews): ten OOM interviews + the new Aura-sessions
    question ("why didn't serverless solve this for you?") — separates
    the buyers serverless already saved from the ones it structurally
    can't [R3][R7].
```

---

## Chain of Verification

| # | question | answer | status |
| --- | --- | --- | --- |
| V1 | Is Aura Graph Analytics really serverless/zero-ETL/GA since May 2025? | Yes — Neo4j press release, May 7, 2025 [R7]; launch blog [R8]; product docs [R9]. | verified, web |
| V2 | Is it still RAM-metered? | Yes — billing docs: sessions billed in GB-minutes by allocated RAM size, 10-min minimum; AuraDS billed by RAM even when paused [R3]; $0.40/GB/hour listed [R2]. | verified, web |
| V3 | Is Kuzu really abandoned, and is the Apple claim solid? | Repo archived with note, Oct 2025 [R11]; The Register coverage [R10]. Apple acquisition is reported by BetaKit citing an EU disclosure [R12] — single-outlet reporting; reader should verify independently. | verified with caveat |
| V4 | Does GDS's estimate mode really cover only production-tier algorithms? | Yes — memory-estimation docs: "only algorithms in the production-ready tier are guaranteed to have an .estimate mode" [R4]. | verified, web |
| V5 | Is the GraphRAG/Leiden demand signal citable? | Yes — microsoft/graphrag README (cost warning, 34k+ stars) [R13]; docs describing hierarchical Leiden + community summaries [R14][R15]. | verified, web |
| V6 | Are the wedge likelihoods measured? | No — judgment with kill criteria and check dates; Z1-Z3 convert the three largest assumptions into data within one month. | honest-uncertainty |

## One-Sentence Summary

```text
Debugging PMF01/02 against verified sources shows the world moved twice —
Neo4j took the "serverless convenience" ground in May 2025 (but kept the
RAM meter running, even on paused instances) and Kuzu's death opened the
embedded-analytics vacuum PMF02 had priced as contested — so the corrected
sequence is: re-aim the flagship benchmark at the ground serverless cannot
take ("your hardware, your data, finishes anyway"), rush the cheap embedded
GraphRAG quickstart into the perishable Kuzu-orphan window now, and let the
regulated-industry sidecar follow the proof instead of leading it.
```

---

## References

All external claims in this document cite the URLs below (accessed
2026-07-08). Facts drawn from these sources are marked [R#] in the text;
anything not marked [R#] or "verified locally" is analysis/judgment.

- **[R1]** Neo4j pricing page (AuraDB/AuraDS tiers; Aura Graph Analytics listed at $0.40/GB/hour):
  https://neo4j.com/pricing/
- **[R2]** Neo4j Aura console pricing table (AuraDS + Graph Analytics Serverless line items, ACU rates):
  https://console.neo4j.io/pricing?version=1.4
- **[R3]** Neo4j Aura billing dimensions (AuraDS: GB-hours by RAM capacity, billed running AND paused; Graph Analytics: GB-minutes by allocated session RAM, 10-minute minimum):
  https://neo4j.com/docs/aura/billing/billing-dimensions/
- **[R4]** Neo4j GDS Memory Estimation docs (`.estimate` mode syntax; production-tier-only guarantee):
  https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/
- **[R5]** Neo4j Aura Graph Analytics product docs (session model, size limits, AuraDS comparison):
  https://neo4j.com/docs/aura/graph-analytics/
- **[R6]** Aura Graph Analytics serverless setup docs (native Neo4j integration path):
  https://neo4j.com/docs/graph-data-science/current/installation/aura-graph-analytics-serverless/
- **[R7]** Neo4j press release, May 7, 2025 — "Neo4j Launches Industry's First Graph Analytics Offering For Any Data Platform" (serverless, zero-ETL, 65+ algorithms, pay-as-you-use):
  https://neo4j.com/press-releases/aura-graph-analytics/
- **[R8]** Neo4j launch blog — "Introducing Neo4j Aura Graph Analytics":
  https://neo4j.com/blog/aura-graph-analytics/neo4j-aura-graph-analytics/
- **[R9]** SiliconANGLE-syndicated coverage via Neo4j newsroom — "Neo4j goes serverless, bringing graph analytics to any data source":
  https://neo4j.com/news/neo4j-goes-serverless-bringing-graph-analytics-to-any-data-source/
- **[R10]** The Register, Oct 14, 2025 — "KuzuDB graph database abandoned, community mulls options" (archival, community reaction, Kineviz "bighorn" fork):
  https://www.theregister.com/software/2025/10/14/kuzudb-graph-database-abandoned-community-mulls-options/
- **[R11]** kuzudb/kuzu README archival commit, Oct 2025 — "Kuzu is working on something new ... archiving the KuzuDB project":
  https://github.com/kuzudb/kuzu/commit/06890e1ac6bd31216f916526b933afc2a7802ec1
- **[R12]** BetaKit, Feb 2026 — "Apple strikes deal to acquire Canadian database software startup Kuzu" (EU disclosure; single-outlet report — verify independently):
  https://betakit.com/apple-strikes-deal-to-acquire-canadian-database-software-startup-kuzu/
- **[R13]** microsoft/graphrag GitHub repository (34k+ stars, MIT license; README cost warning on indexing):
  https://github.com/microsoft/GraphRAG
- **[R14]** GraphRAG community-detection docs (hierarchical Leiden clustering, community summaries, global/local search):
  https://microsoft-graphrag.mintlify.app/concepts/community-detection
- **[R15]** GraphRAG project documentation index (knowledge graph -> community hierarchy -> summaries pipeline):
  https://microsoft.github.io/graphrag/

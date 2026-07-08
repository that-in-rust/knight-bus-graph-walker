# Arch04: Rubber-Duck Debugging Arch01-03 — And The Fork They All Missed

Date: 2026-07-08
Method: Timeline Traverser, fourth iteration. Before simulating anything new,
this document rubber-duck-debugs Arch01/Arch02/Arch03 — explains each prior
version out loud until its hidden bugs surface — then re-simulates from the
corrected premises. Companion: `PMF03.md` (same debugging pass applied to the
PMF line, with verifiable References).

---

## Phase 0: Deconstruct & Clarify

- **Premise check:** Premise is sound with one self-correction required: the
  prior Arch docs contain identifiable reasoning bugs (below), so "next
  iteration" here means *debug then re-simulate*, not "add a sixth
  architecture." Proceeding with optimized protocol.
- **Council:** graph-kernel engineer, incumbent strategist (simulates
  Neo4j's moves), brownfield refactoring specialist, Skeptical Engineer
  (owns the bug list below), solo-maintainer realist.

## Part 1: The Rubber-Duck Session — Bugs Found In Arch01-03

Explaining each document aloud to the duck, these defects surfaced:

```text
BUG-1 (Greenfield Fallacy) — SEVERITY: HIGH
  Arch03 simulated build orders as if nothing exists. But the repo already
  contains ~5,500 lines of working Rust: dual-CSR mmap runtime
  (src/runtime.rs), snapshot builder (src/snapshot.rs), a GDS procedure
  registry with support statuses and reject-unsupported semantics
  (src/gds.rs: require_supported_gds_entry, effective_gds_support_status),
  truth-graph parity harness (src/truth.rs, src/parity.rs), and a bench
  lane vs Neo4j (benchmarks/walk_hopper_v1). Arch03's "Week 1: WCC on flat
  CSR" is partly ALREADY DONE. The real fork is what to do with the
  existing v00x asset — extend, strangle, or re-aim it.

BUG-2 (Static-World Fallacy) — SEVERITY: HIGH
  No timeline ever gave the incumbent a move. In reality Neo4j already
  moved: Aura Graph Analytics (GA May 7, 2025) is serverless, zero-ETL,
  works against ANY data source, pay-as-you-go. Part of our imagined
  pitch ("no persistent instance, no ETL") is now the incumbent's pitch.
  What it did NOT change: it still bills by RAM GB-hours and still
  requires the RAM (see PMF03 refs R7-R8). Timelines must include
  adversary reactions.

BUG-3 (Frozen-Workload Fallacy) — SEVERITY: MEDIUM
  Arch02's seven families came from pre-LLM use-case guides. The 2024-26
  GraphRAG wave re-weights the mix toward Leiden community detection,
  k-hop retrieval neighborhoods, and embeddings — and adds a freshness
  demand (agent memory graphs GROW continuously). Static-snapshot-only
  thinking under-serves the fastest-growing workload.

BUG-4 (Freshness Blind Spot) — SEVERITY: MEDIUM
  The PRD's own three-tier visibility model (Journal / Overlay / Base)
  never appeared in ANY timeline. Every simulation assumed sealed static
  snapshots. Publication cadence and OLAP lag are architecture-shaping
  constraints that were silently dropped after SUM01.

BUG-5 (Uncalibrated Likelihoods) — SEVERITY: LOW-MEDIUM
  "~75%", "~55%" etc. had no falsification dates or kill criteria. A
  likelihood without a date is a mood. Fixed below: every timeline gets
  a kill criterion and a check date.

BUG-6 (Infinite-Capacity Maintainer) — SEVERITY: LOW
  No timeline modeled capacity shocks. Fixed: each timeline notes its
  degraded form under half capacity.
```

What survives the debugging (confirmed, carried forward): the composite
architecture (A's planes + C's substrate + E-as-backend + D-as-flags), the
seven-family adoption analysis as a *baseline* (now needing GraphRAG
re-weighting), and the substrate-first instinct — but now applied to a
brownfield, adversarial, freshness-aware world.

---

## Decision Frame

- **Fork in the road:** Given a working v00x engine (BUG-1), an incumbent
  that already went serverless (BUG-2), and a workload mix drifting toward
  GraphRAG-with-freshness (BUG-3/4): what is v004? Four candidate moves:
  extend the existing engine (brownfield substrate), strangle-rewrite it,
  build the freshness tiers first, or re-aim at the GraphRAG/embedded
  vacuum.
- **Desired outcome:** In one year: an engine that keeps v00x's proven
  low-RSS traversal wins, adds honest budgeted execution for the heavy
  families, and holds a defensible position the incumbent's serverless
  move does not already occupy.
- **Hard constraints:** PRD F01-F09; solo capacity; v00x parity harness
  must keep passing (it is the project's accumulated proof); no public
  performance claims without artifact bundles (Arch03/patterns-5 rule).
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1, with kill
  criteria and check dates per BUG-5.
- **What counts as failure:** discarding working v00x code for aesthetic
  reasons; OR spending the year on freshness plumbing while the seven
  families still OOM; OR arriving in Q4 differentiated only on things
  Aura serverless already offers.

Assumptions stated: (1) v00x code quality is extendable, not rotten —
spot-checks of the registry and runtime support this, but a 1-week audit is
Experiment X1. (2) Aura Graph Analytics keeps RAM-metered billing (public
pricing as of research date). (3) GraphRAG freshness demand is real but its
timing is the least certain input (BUG-3's residual risk).

---

## Timeline A: Brownfield Substrate ("v00x grows a Budget Machine")

- **Opening move:** Retrofit the Arch03 workspace trait
  (`estimate_workspace_memory_bytes`) onto the EXISTING runtime; the
  registry's `require_supported_gds_entry` gate becomes the admission
  point — support status + memory verdict in one check.
- **Week 1:** Audit + trait skeleton; existing traversal procedures get
  receipts with near-zero new machinery (their scratch is tiny). The
  parity harness keeps passing throughout — the week costs no proof.
- **Month 1:** DataFusion-style pool + spill file land behind the trait.
  First friction: v00x types weren't designed for budget threading;
  ~2 weeks of refactor tax that the greenfield timelines never priced.
  The tax is real but bounded because the codebase is 5.5k lines, not 50k.
- **Quarter 1:** Louvain + NodeSim arrive as the first NEW families,
  budgeted from their first commit (Arch02's crises stay pre-paid). The
  flagship benchmark runs against the same bench lane that already
  compares v00x to Neo4j — evidence continuity compounds: old RSS wins
  and new completion wins publish as one artifact series.
- **Long-term shape (Year 1):** v004 = v00x + substrate + 7 families,
  one continuous parity/bench history, freshness tiers begun in Q3.
- **Likelihood:** ~80%. **Kill criterion:** if the week-1 audit finds the
  runtime can't thread budgets without >1 month of rework, switch to
  Timeline B. **Check date:** end of week 2.
- **Stress points:** refactor tax morale ("why am I rewriting working
  code"); temptation to skip the trait for "just this one algorithm."
- **Inflection points:** the audit verdict; whether receipts go into the
  registry (one honest surface) or beside it (two sources of truth —
  the rot vector).
- **Half-capacity form:** substrate + Louvain only; NodeSim slips a
  quarter; still coherent.
- **Lived experience:** unglamorous, compounding; the codebase feels
  like a house being rewired while lived in — annoying, never homeless.

## Timeline B: Strangler Rewrite ("v00x becomes the oracle")

- **Opening move:** New crate designed substrate-first per Arch03;
  v00x demoted to reference implementation — its outputs become the
  new engine's correctness oracle alongside the truth graph.
- **Week 1:** Clean pool/trait core, no legacy constraints; nothing runs
  end-to-end. The v00x bench history goes quiet — the public evidence
  stream pauses.
- **Month 1:** New engine reaches WCC/PageRank parity with v00x. Now two
  codebases need maintenance; every v00x bugfix is a decision (backport
  or ignore). The oracle idea works but costs a standing tax.
- **Quarter 1:** New engine passes v00x on the heavy families (which
  v00x never had) but still trails it on cold-open time and walker
  latency — the old engine's mmap paths were more tuned than remembered.
  Closing that gap consumes the quarter's margin.
- **Long-term shape (Year 1):** A cleaner engine that spends the year
  re-earning proofs the old one already had; net position ≈ Timeline A
  minus one quarter, plus better bones whose value pays off in year 2+.
- **Likelihood:** ~55%. **Kill criterion:** if new engine hasn't matched
  v00x's bench suite by month 3, fold its substrate back into v00x
  (i.e., become Timeline A late). **Check date:** end of month 3.
- **Stress points:** double maintenance; the quiet evidence stream;
  sunk-cost gravity in both directions.
- **Inflection points:** month-3 bench parity; the first time a user
  asks "which engine should I run?"
- **Half-capacity form:** collapses — two codebases and one half-person
  is the textbook failure; auto-switch to A.
- **Lived experience:** intellectually satisfying, strategically
  anxious; the duck keeps asking "and the old one was broken HOW?"

## Timeline C: Freshness-First ("the tiers are the moat")

- **Opening move:** Implement the PRD's Journal/Overlay/Base visibility
  tiers on top of v00x snapshots: append journal, periodic overlay
  compaction, atomic base republication (W+1 publishing, never
  query-time merge).
- **Week 1:** Journal format + write path; reads still base-only.
  Feels like OLTP work — because it partly is, which is the first hint
  the timeline has drifted across the PRD's own plane boundary.
- **Month 1:** Overlay reads work; traversal answers now carry a
  watermark tuple (base gen, overlay seq). Genuinely novel surface —
  neither GDS projections nor Aura serverless sessions offer "query the
  graph as of 90 seconds ago with proof."
- **Quarter 1:** The bill arrives: every EXISTING procedure needs
  overlay-awareness (or explicit base-only labeling), the heavy families
  still have no budget substrate, and the seven-family OOM story — the
  actual adoption driver per Arch02 — is unchanged. Freshness impresses
  architects; completion sells to practitioners.
- **Long-term shape (Year 1):** Best-in-class snapshot freshness
  semantics wrapped around an engine that still rejects Louvain on big
  graphs. Differentiated on the axis fewer buyers currently price.
- **Likelihood of being the right lead move:** ~25% now — rising toward
  ~50% IF GraphRAG agent-memory freshness demand materializes (BUG-3).
  **Kill criterion:** if by month 2 no design-partner-grade user wants
  sub-hour freshness, park the tiers at journal-format-only and switch
  to A. **Check date:** end of month 2.
- **Stress points:** scope creep toward OLTP; watching the OOM tickets
  that the substrate would have fixed accumulate.
- **Inflection points:** first real freshness request (converts this
  from speculation to demand); overlay complexity crossing the "second
  executor" threshold.
- **Half-capacity form:** journal format only, documented, shelved.
- **Lived experience:** architecturally thrilling, commercially lonely
  in 2026; the timeline that future-us may thank or curse present-us for.

## Timeline D: GraphRAG Re-Aim ("fill the Kuzu vacuum")

- **Opening move:** Package v00x as an embedded/CLI engine for the
  GraphRAG loop — Leiden + PageRank + k-hop retrieval + FastRP — aimed
  at the community orphaned by Kuzu's abandonment (archived Oct 2025;
  team acquired by Apple per Feb 2026 disclosure — see PMF03 refs
  R10-R12). Embedded analytics graph engines just lost their leader.
- **Week 1:** Embedded API + a GraphRAG quickstart. The vacuum is real:
  Kuzu users are actively "mulling options" (The Register, Oct 2025) and
  the Kineviz "bighorn" fork is embryonic. Fast public start.
- **Month 1:** First orphaned-Kuzu inbound. But their asks are Kuzu's
  surface — Cypher breadth, multi-file format stability, extensions —
  not our GDS-compat/low-RAM thesis. Serving them well means becoming
  Kuzu, which one solo maintainer cannot (The Register: "probably six
  people actually understand the codebase" — and that was KUZU's code).
- **Quarter 1:** Choose: (a) narrow promise — "embedded GRAPH ANALYTICS
  for RAG, not a general graph DB" — which fits our engine and stays
  honest, or (b) chase general-DB parity and dissolve the roadmap. Path
  (a) keeps the seven-family work central; path (b) is the PMF02
  Timeline-C drift with extra steps.
- **Long-term shape (Year 1):** On path (a): a respected niche tool in
  the RAG stack + the same substrate work as Timeline A but sequenced by
  GraphRAG priority (Leiden before NodeSim); Neo4j-compat surface
  stalls. On path (b): unfunded Kuzu cosplay.
- **Likelihood:** ~40% for path (a) being a strong year. **Kill
  criterion:** if month-2 inbound is dominated by general-DB asks rather
  than analytics asks, retreat to Timeline A with the embedded facade
  kept as a listening post. **Check date:** end of month 2.
- **Stress points:** the mismatch between orphan asks and our thesis;
  Apple ambiguity (if Kuzu re-emerges inside Apple tooling, the vacuum
  partially refills).
- **Inflection points:** the analytics-vs-general-DB inbound ratio; the
  first RAG framework that lists us as a supported backend.
- **Half-capacity form:** quickstart + embedded facade only; no active
  courting of the orphan community.
- **Lived experience:** energizing (a real community, in real need, right
  now) and treacherous (their need is shaped like someone else's product).

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A Brownfield Substrate | keeps compounding proof; crises pre-paid; cheapest path to the flagship benchmark | refactor tax; least glamorous | high | LOW | week-1 audit verdict on v00x extendability |
| B Strangler Rewrite | cleanest bones for year 2+ | double maintenance; evidence stream pauses; re-earns old proofs | medium (can fold back into A) | medium-high | month-3 bench parity; solo capacity holding |
| C Freshness-First | genuinely novel surface neither GDS nor Aura serverless has | wrong axis for 2026 buyers; heavy families still OOM; OLTP scope creep | medium | medium (timing bet) | GraphRAG freshness demand arriving on schedule |
| D GraphRAG Re-Aim | real vacuum (Kuzu dead), fast start, wave-aligned | orphan asks ≠ our thesis; Apple wildcard; compat stalls | medium-high (facade is cheap to keep) | medium | inbound being analytics-shaped, not DB-shaped |

Shared inflection: all four timelines eventually need the substrate; they
differ only in what they do FIRST and which proof stream they grow. A grows
the existing one; B pauses it; C and D grow new ones of uncertain value.

Adversary layer (BUG-2 fix): the incumbent's plausible next moves are
(i) price cuts on Graph Analytics sessions, (ii) tighter Snowflake/Databricks
embeddings, (iii) a "small-footprint" marketing push. Moves (i)-(ii)
strengthen the case for differentiation they can't follow (honest completion
on customer-owned small hardware; embedded/local mode) and weaken pure
price-arbitrage positioning. Only Timeline C and D hold ground the
serverless move cannot occupy at all; Timeline A holds the "your hardware,
your data, finishes anyway" ground, which serverless structurally concedes.

---

## Decision Filter

**Which path is strongest if everything goes normally?**
**A (Brownfield Substrate) as the trunk, with D's path-(a) facade grafted
on at ~5% cost** (the embedded quickstart doubles as the Kuzu-orphan
listening post and the GraphRAG demo), and C's journal FORMAT (not the
full tiers) specified on paper in Q2 so freshness can be activated the
week real demand appears. B is retired: the duck's question — "the old
one was broken how?" — has no answer that justifies a quarter of silence.

**Which path is safest if things go badly?**
A again: every failure mode is local (a refactor that takes longer, a
family that slips) and the proof stream never stops. Under half capacity
it degrades to "substrate + Louvain," which is still a publishable year.

**What experiment would collapse uncertainty fastest?**
```text
X1 (do first, 1 week): v00x extendability audit — thread a budget token
    through one existing traversal procedure end-to-end. Decides A vs B
    with code, not opinion. Kill-check: end of week 2.
X2 (parallel, 2 days): GraphRAG loop smoke test — run Leiden + k-hop +
    FastRP shapes on v00x fixtures to see how far the CURRENT engine
    already is from D's demo. Prices the facade graft precisely.
X3 (parallel, 0 code): post one honest "what do orphaned Kuzu users
    actually need?" question in the Graphgeeks/Kuzu-community spaces.
    The reply distribution IS Timeline D's kill-criterion data, gathered
    before spending anything.
```

---

## Chain of Verification

| # | question | answer | status |
| --- | --- | --- | --- |
| V1 | Does the repo really already contain the claimed v00x machinery? | Yes — verified by direct read: src/runtime.rs (576 ln), src/gds.rs (registry + support gates), src/parity.rs, src/truth.rs, benchmarks/walk_hopper_v1. | verified locally |
| V2 | Did Aura Graph Analytics really ship serverless/zero-ETL in May 2025? | Yes — Neo4j press release dated May 7, 2025 (PMF03 ref R7). | verified, web |
| V3 | Is Kuzu really abandoned/acquired? | Repo archived Oct 2025 with "working on something new" note (R11); The Register coverage Oct 14, 2025 (R10); BetaKit reports Apple acquisition disclosure Feb 2026 (R12). User may independently verify. | verified, web |
| V4 | Were the visibility tiers really absent from Arch01-03? | Yes — grep confirms Journal/Overlay/Base appear in SUM01/PRD material but in no Arch01-03 timeline. | verified locally |
| V5 | Are the new likelihoods calibrated? | Better than before (each now has a kill criterion + check date) but still judgment; X1-X3 convert the three largest guesses into evidence within two weeks. | honest-uncertainty |

## One-Sentence Summary

```text
Debugging Arch01-03 exposed four blind spots — the code that already
exists, the incumbent that already moved, the workload that already
shifted, and the freshness tiers we already specified — and the corrected
simulation says: grow the Budget Machine inside the existing v00x engine
(audit first, kill-date week 2), keep a cheap embedded/GraphRAG facade as
a listening post over the Kuzu vacuum, hold freshness as a specified-but-
dormant option, and let the strangler rewrite die for lack of a crime.
```

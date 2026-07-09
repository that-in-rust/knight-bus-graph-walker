# Simulation 01 — Do Real Users Actually Feel This Pain?

*A truth-seeking market-evidence dossier, written in the voice and with the
discipline of Shreyas Doshi: insight before execution, pre-mortems before
pitches, and radical honesty about what the evidence does and does not say.*

---

## 0. Why this document exists (the Shreyas preamble)

Most products fail not because the team executed badly, but because the team
never verified that the pain they imagined was the pain users actually felt.
The single highest-leverage activity before writing a line of production code
is to go find the users in the wild — in their own words, on their own forums,
complaining about their own bills — and check whether the story we tell
ourselves survives contact with their reality.

Our story going in: *"Neo4j GDS demands the whole graph in RAM; we can run the
same algorithms in ~10-25x less RAM by streaming from disk, and — uniquely —
we can print the exact bill before the run starts."*

This document is the audit of that story. It contains six passes of evidence
gathering, each triggered by a harder question than the last:

1. Do users complain about **our three flagship algorithms** (WCC, Louvain,
   PageRank)? — Sections 2-5.
2. Do users complain about the **other four families** (NodeSimilarity,
   shortest paths, FastRP, triangles)? — Section 6.
3. What does the **aggregate complaint record of Neo4j** (2011-2026, all
   themes) look like? — Section 7.
4. Which **differentiating claims ("badges")** does the evidence actually
   support? — Section 8.
5. If this turf is so good, **why is nobody standing on it?** A wide
   competitive sweep. — Section 9.
6. What does an **independent external research pass** add or contradict?
   — Section 10.

Every factual claim carries a numbered citation; all URLs are collected in
the References section at the bottom so that anyone — including a skeptical
future version of ourselves — can check every line.

**Method note.** Searches were run against the Hacker News Algolia API,
community.neo4j.com (Discourse search API), the StackExchange API, and the
GitHub API (issues and repository metadata). Reddit's API blocked
unauthenticated search; this is flagged as a coverage gap rather than papered
over. Quotes are minimally paraphrased; where a quote matters, it is verbatim.

---

## 1. Core facts going in (what we claim, and how honest each claim is)

Before looking at users, state the claims and grade their epistemic status.
A claim that is "modeled" is a hypothesis wearing a suit; it must not be
allowed to dress up as a measurement.

| # | Claim | Epistemic status |
|---|-------|------------------|
| 1 | WCC + Louvain + PageRank ≈ ~50% of GDS algorithm usage; all seven families ≈ ~90% | Modeled from GDS documentation emphasis and conference talks; not from vendor telemetry |
| 2 | ~10-25x less RAM to pay for at fixed dataset size | Modeled; **code-verified against GDS source** (see Arch06) but **not yet measured** on our own engine |
| 3 | The bill (RAM + wall-clock) is computable from ~1 KB of manifest metadata **before** the run | Designed, not yet built |
| 4 | 2-5x slower wall-clock for streaming runs (recovered partially by delta convergence and warm starts) | Modeled from the out-of-core literature (GraphChi lineage) |

The question this entire document answers: **does anyone actually care?**
Not "is this technically impressive" — Shreyas would remind us that impressive
technology solving unfelt pain is the most expensive way to fail.

---

## 2. Evidence catalog: the first pass (WCC / Louvain / PageRank era)

### 2.1 Direct, on-point pain — the Neo4j Community Forum

These are the strongest signals available anywhere: real users hitting the
exact wall our product removes, answered by **Neo4j's own staff confirming
the wall is structural, not configurational**. When the vendor's employees
describe your product's reason to exist, you are no longer speculating.

**E1 — The 16 GB clustering user.** A user asks *"How to efficiently cluster
nodes using GDS with limited memory (16 GB RAM)"* and receives the error:
`Procedure was blocked since minimum estimated memory (52 GiB) exceeds
current free memory (5120 MiB)` [[1]](#references).
This is *literally* our demo scenario: a 16 GB box, a 52 GiB estimate, a job
refused. Our pitch to this exact person: that job finishes, streaming, with
the bill printed first.

**E2 — The vendor admits there is no spill.** A user with hundreds of
millions of nodes on a 32 GB VM asks *"What library to use instead of GDS
when the graph db is too big to project in memory?"* Alicia Frame — then
Neo4j's GDS product lead — replies: *"We don't offer any spill over / out of
core computations right now,"* and offers two workarounds: project a subset,
or set `sudo: True` with the caveat that *"the risk is OOMing your
database"* [[2]](#references). Read that again from a product lens: the
official guidance is **use less data or risk crashing production**.

**E3 — The user who writes our pitch for us.** A user with billions of nodes
and hundreds of billions of edges OOMs at projection and asks whether GDS can
run without one. Neo4j GDS engineer paul.horn: *"GDS also needs to project
all the data... into a single in-memory projection, there is no option to
spill to disk."* The user's reply is the single best sentence in this entire
dossier: *"that surprises me as it defeats the purpose of using a database if
you need to map its data to an in-memory representation to run an
algorithm"* [[3]](#references). An unprompted user independently articulating
the core absurdity we exist to fix — this is what real demand looks like.

**E4 — Big iron does not save you.** A machine with 95 GB RAM and a 75 GB
heap, ~320M nodes, still hits heap errors while clustering
[[4]](#references). The wall scales with you.

**E5 — "Capacity exhausted" at 300 GB.** A projection fails at ~300 GB;
the user reduces concurrency and tunes heap configs; nothing helps
[[5]](#references).

**E6 — The 6.6 TiB import on a 32 GB machine.** The import tool "suggests
203 GB" [[6]](#references). Not GDS-specific, but the same shape of pain:
the tool demands RAM the user does not have.

**E7 — Memory creep under path workloads.** A projection's memory grows
steadily under a 150k-shortest-paths-per-hour workload until restart
[[7]](#references).

### 2.2 Pricing and RAM resentment — Hacker News

The forum gives us the *technical* pain; HN gives us the *emotional and
economic* pain. Shreyas's framing applies: users buy progress on problems
they resent, and resentment compounds.

**E8.** *"When we looked to scale Neo4j, we almost had a heart attack when
seeing the price"* (startup, 2019). The same thread contains the meta-signal:
*"Every time that Neo4j is mentioned here, the pricing issues are raised. No
exception today."* [[8]](#references)

**E9.** *"They wanted to charge us something like 10% of our ARR"* (2021, on
a pricing model built around one very large centralized graph)
[[9]](#references).

**E10.** *"We evaluated umpteen graph dbs this past year and chose vanilla
Postgres instead because Neo4j/RedisGraph have insane licenses"* (2020)
[[10]](#references). Note what this is: **silent churn caught on camera** —
a buyer who left the category without ever filing a complaint ticket.

**E11.** *"If the data fits in memory (Neo4j, in the past at least, has
pretty much required that)..."* (2015) [[11]](#references). The RAM-resident
reputation is a **decade old**. Reputations this durable are moats for
whoever attacks them.

**E12.** GraphRAG cost anxiety (2025): *"GraphRAG preprocessing is insanely
expensive and precisely does not scale linearly with your dataset"*
[[12]](#references), with an explainer reply detailing entity extraction plus
community detection over the whole corpus [[13]](#references).

**E13.** *"...you're told to add Neo4j or a specialized GraphDB to your
stack... you have to deal with Neo4j and who wants that"* — a 2025 Show HN
for a GraphRAG tool whose selling point is *not needing Neo4j*
[[14]](#references). When "avoids the incumbent" is a launch headline,
the incumbent has a resentment problem.

### 2.3 Stack Overflow

A thinner signal — graph-analytics questions mostly flow to the Neo4j forum —
but present and on-theme:

**E14.** *"How can I create graph projections in Neo4j for a very large
graph"* [[15]](#references).

**E15.** *"Keep a projected graph in sync with persisted graph in Neo4j
GDS"* — the staleness/refresh pain arising organically [[16]](#references).

---

## 3. Truth-seeking: what the evidence does NOT say

Shreyas's discipline: the counter-evidence section is not an appendix; it is
the part of the document most likely to save the company. Six honest
corrections to our own pitch:

1. **Nobody complains about WCC, Louvain, or PageRank by name.** The pain is
   almost always at the **projection step** — getting the graph into memory
   at all — not at the algorithm step. Users say "can't project," never
   "Louvain ate my tallies." Implication: the headline must attack the
   projection wall ("no projection step — point us at the files"), with the
   per-algorithm plans as supporting proof, not the lede.

2. **The complaint volume is moderate, not a flood.** Tens of high-quality
   threads over ~5 years, not thousands. Two readings, both partially true:
   (a) most Neo4j users run small graphs that fit, and the big-graph users
   are a minority — in which case *the minority is the market*; (b)
   survivorship bias — people who hit the wall silently leave for igraph,
   Postgres, or cuGraph (E10 and E13 show exactly this), so forums
   structurally under-count the pain.

3. **GraphRAG's "insanely expensive" is mostly LLM token cost, not Leiden
   RAM** (E12's explainer thread is clear on this). Community detection is a
   real but secondary cost there. Our GraphRAG story must stay honest: we cut
   the graph-compute and re-index slice, not the LLM bill.

4. **The pricing rage (E8-E10) is about licenses and cluster pricing, not
   GB-hours specifically.** Aura Graph Analytics ($/GB-hour) is newer; there
   is not yet a public corpus of "my Aura analytics bill" complaints. Our
   dollar comparisons are extrapolations, not quotes from victims — and must
   be labeled as such.

5. **Reddit is unsearched** (API blocked unauthenticated search). Flagged as
   a gap, not silently omitted.

6. **Neo4j's estimate-then-block behavior (E1) is arguably a feature they
   already ship** — the job was refused, not OOM-killed mid-flight. What they
   do not offer is the second half of the sentence: *"...and here is how it
   finishes anyway."* Our differentiation is the FINISH, plus
   estimate-in-one-second-without-a-running-database.

---

## 4. Simulated reactions if we ship what we claim

The simulation: we publish *"the 52-GiB-estimate job from E1 finishes on the
same 16 GB box in 38 minutes; bill printed before start; reproducible
artifact bundle attached."* How does each observed population react?

**P1 — The forum users of E1-E5 (data engineers with too-big graphs).**
Reaction: immediate trial. They already asked for exactly this and were told
"no spill, use less data." The conversion blocker is not desire — it is that
their data lives *inside* Neo4j, and the export step (2-8 hours,
failure-prone) is our onboarding cliff. Simulated quote: *"Worked on the CSV
dump. Now can it read my Neo4j store directly?"*

**P2 — The HN skeptic.** *"You could do this with GraphChi in 2012 / with
NetworKit / with a 750-line Rust program. Out-of-core graph processing is a
solved research problem."* This objection is **true**, and the correct reply
is prepared in advance: yes, the mechanism is 13 years old; what nobody did
is productize it with a pre-run cost receipt aimed at the #1 graph vendor's
workloads. **The receipt, not the streaming, is the news.** Expect this
comment within the first hour of any HN launch; pre-empt it inside the post
itself.

**P3 — The "just buy RAM" crowd.** *"512 GB servers are cheap now."*
Partially true — a rented 512 GB bare-metal box runs ~$200-400/month. But
E1 and E4 show real users on 16-95 GB boxes who did *not* buy bigger
machines — because of procurement, cloud policy, or the fact that the
machine is a laptop. The segment that cannot "just buy RAM" exists, posts
about it, and is answered by the vendor with "use less data."

**P4 — Neo4j's probable competitive response.** Short term: point at their
existing estimate mode and Aura autoscaling. Medium term: ship *some*
disk-backed projection (block-format work already exists in the DB layer).
What they will **not** do is make bill-before-run + finish-anyway the center
of their story, because (a) JVM heap + GC makes byte-exact receipts
technically embarrassing, and (b) per-GB-hour revenue is a direct
disincentive. Our moat holds exactly as long as the receipt stays
byte-honest. (Section 10.3 extends this with an external forecast.)

**P5 — GraphRAG builders (E12, E13).** They want *fewer* moving parts, not
another database. Reaction to "a CLI that runs Leiden on your parquet of
edges with a fixed RAM cap": strong — it **subtracts** a component (Neo4j)
instead of adding one. This audience is acquirable without ever touching the
Neo4j export cliff, because their edges are already in files. Possibly the
true beachhead.

**P6 — The silent majority (small graphs that fit).** No reaction. Our
product is irrelevant below ~10M edges, and that is most users. This is
fine — they were never the market, and pretending otherwise would only blur
the positioning.

---

## 5. Verdict of the first pass

| Question | Answer |
|----------|--------|
| Is the pain real? | **YES** — vendor-confirmed structural wall (E2, E3); users asking for exactly our product (E1, E2, E3) |
| Is it about our three algorithms by name? | **NO** — it is about the projection wall; algorithms are downstream. Lead with "no projection step." |
| Is the market loud? | **MODERATE** — quality over volume; silent churn to igraph/Postgres likely understates it |
| Strongest wedge audiences | (1) forum-class users with too-big-to-project graphs; (2) GraphRAG builders with edges already in files (no export cliff) |
| Biggest adoption risks | The Neo4j export step; and the "solved problem, see GraphChi" rebuttal (answer: the receipt is the product, not the streaming) |

---

## 6. Second pass: the other four families (NodeSimilarity, paths, FastRP, triangles)

The first pass covered only WCC / Louvain / PageRank. After Arch06 gained
worked examples 4-7, the harder question became: do the remaining families —
NodeSimilarity (~12% of usage), shortest paths (~10%), FastRP (~8%),
triangles (~5%) — generate their own pain signal, or were we about to build
proof points nobody asked for?

Method: same APIs (community.neo4j.com Discourse search, HN Algolia,
StackExchange, GitHub issues on neo4j/graph-data-science).

### 6.1 New evidence

**E16 — The money quote, revisited.** The E3 thread, re-read for the
NodeSimilarity-era pass, yields the richest exchange in the corpus: billions
of nodes; the engineer's *"there is no option to spill to disk"*; the user's
*"defeats the purpose of using a database"* [[3]](#references). A real user
independently articulating our pitch's core absurdity, unprompted.

**E17 — The question title that IS the product spec.** Stack Overflow: *"In
Neo4j, is there any way to create a graph projection if your graph is too big
to fit in memory?"* [[17]](#references). Again: projection-level, not
algorithm-level.

**E18 — Shortest paths still OOMs people.** GitHub issues #55/#54 on
neo4j/graph-data-science: `gds.alpha.shortestPath.stream:
java.lang.OutOfMemoryError: Java heap space` [[18]](#references). The family
whose *scratch* is tiny still kills users — because the projection and result
streaming are the bill. This confirms Arch06 example 5's framing: path pain
means paying for the whole graph to answer one question.

**E19 — OOM from thread machinery, not even data.** GitHub #139/#132:
personalized PageRank fails with `OutOfMemoryError: unable to create native
thread` under normal use [[19]](#references). JVM operational fragility is
its own complaint class, distinct from data volume.

**E20 — A 500-node graph generating memory anxiety.** The E7 thread in
detail: ~500 nodes (!), 150k Dijkstra calls/hour, memory climbing until
restart [[7]](#references). Even trivially small graphs create memory dread
when the runtime is a JVM. Our fixed-arena story — "RSS never exceeds the
receipt" — speaks to this buyer too.

**E21 — FastRP pain is usage confusion, not RAM.** Forum FastRP threads are
about zero vectors, type errors, and applying embeddings to new samples —
not memory [[20]](#references). The RAM pain shows up indirectly: users at
the scale where FastRP's 254 GB sizing bites have usually already died at the
projection wall and never reached FastRP at all.

### 6.2 What the second pass did NOT find (honesty)

| Searched for | Found |
|--------------|-------|
| "nodeSimilarity out of memory" complaints | ~Nothing algorithm-specific; similarity pain appears as generic projection failures (but see Section 10.1 — a re-sourcing pass later found a direct hit [[73]](#references)) |
| "triangle count memory" complaints | Essentially zero |
| FastRP RAM complaints | Zero (usage confusion only) |
| HN threads naming these algorithms | Zero relevant |

### 6.3 Interpretation

1. **The funnel hypothesis.** Users die at the projection step before they
   ever reach the algorithm whose scratch would have killed them.
   NodeSimilarity's whole-graph-copy scratch (Arch06 example 4) generates few
   complaints because the population that would hit it already OOM'd at
   `gds.graph.project`. **The wall hides behind the wall.** This is the kind
   of non-obvious causal structure Shreyas would insist on naming explicitly,
   because it changes what the messaging must attack.
2. **Messaging consequence (reinforces the first pass).** Per-algorithm RAM
   numbers are our *internal engineering truth*; the *user-facing truth* is
   one sentence — "no projection step." The seven bespoke plans are proof
   points, not headlines.
3. **Small-graph JVM anxiety (E20) is a real secondary segment.** Even users
   whose graphs fit complain about creep and restarts. "Fixed arena, RSS
   never exceeds the receipt" resonates beyond the big-graph market.
4. **Where evidence is thin, claims must be too.** For triangles and FastRP
   the demand evidence is inferential (funnel-blocked), not observed. The
   docs must not claim "users are crying out for low-RAM FastRP" — they are
   not, visibly. The honest claim: the same projection wall blocks them, and
   the same fix frees them.

---

## 7. Aggregate anecdotal log: everywhere Neo4j gets complained about

Third pass, widest aperture: not GDS, not our algorithms — **every recurring
complaint theme about Neo4j** across Hacker News (2011-2026), the community
forum, Stack Overflow, and GitHub. Purpose: an honest anecdotal map of where
Neo4j fails its users, so we know precisely which failures our product
addresses — and, just as importantly, which it does not.

### 7.1 Memory / resource consumption (our home turf)

**A1.** *"I've had bad experience with Neo4j's memory consumption so I'm
wary... we actively chose to go against it because of past issues with
resource usage."* — posted, of all places, on the thread celebrating Neo4j's
$325M Series F [[21]](#references). Resentment showing up at the victory
parade is a strong signal.

**A2.** An ex-Neo4j insider on why the product abandoned OS memory mapping:
*"50% driven by Java memory mapping having insane issues on Windows, 20%
Java memory mapping having insane issues on every platform..."*
[[22]](#references). Notable for us: **mmap-hostility is a JVM problem, not
an mmap problem** — a Rust engine does not inherit it.

**A3.** The ID-space ceiling: *"we reached the limit of 32 billions of unique
IDs (Neo 3.2)... had to wait for the next version so we could add more
data."* [[23]](#references)

(Plus the entire GDS catalog above — E1-E3, E16-E20: projection OOM, no
spill to disk, "defeats the purpose of using a database.")

### 7.2 Performance / speed

**B1.** *"I still have nightmares with Cypher and neo4j's slowness."*
[[24]](#references)

**B2.** *"right now neo4j is slower for graphs than postgres, just with a
nicer UI."* [[25]](#references)

**B3.** *"Neo4j is dead slow."* — from **2013** [[26]](#references).

**B4.** *"Neo4j was freaking slow when I tried to use it."* — from **2025**
[[27]](#references). Twelve years, same sentence. A complaint this stable is
not a bug backlog; it is an architecture.

**B5.** On vector search: *"the #2 committer of the project may agree...
Neo4j is slower than a 'normal vector db'."* [[28]](#references)

### 7.3 Reliability / operations

**C1.** The classic 2015 rant: *"the least reliable and most buggy database
solution I've ever worked with"* — six months of production experience,
posted on the ArangoDB benchmark thread [[29]](#references).

**C2.** The ConceptNet author (rspeer): *"I lost months of work to Neo4j
back in 2011... The write speed was awful, the stability was awful."*
[[30]](#references)

**C3.** *"Neo4j was a terrible experience as a developer. It crashed
constantly, local dev required me to finagle around with Java SDK
versions... Their managed offering was equally as shitty."*
[[31]](#references). Note the JVM-toolchain complaint specifically: a single
static Rust binary is itself a feature against this.

### 7.4 Pricing / licensing (the loudest theme by volume)

**D1.** *"Every time that Neo4j is mentioned here, the pricing issues are
raised. No exception today."* — from a user who hit the scaling wall on the
free version, then *"almost had a heart attack"* at enterprise pricing
[[8]](#references).

**D2.** *"Neo4j's entire pricing model, even in cloud, is built around the
idea that you'll have one centralized very large graph"* — which does not
fit many-small-graphs shops running 3-5 pre-production environments
[[9]](#references).

**D3.** Scaling is enterprise-gated: *"Neo4j is also very expensive if you
want to use it in a cluster."* [[32]](#references)

**D4.** License bait-and-switch resentment: *"they did a bait and switch
with the license model, I was not happy about that."* [[33]](#references)
Context: the Neo4j v. PureThink litigation over AGPL + Commons Clause
("open-washing") ran for years on HN's front page
[[34]](#references), [[35]](#references).

**D5.** Confirmation from the sales side: *"The company was totally
inflexible with their very outdated licensing model and it constantly lost
them potential customers."* [[36]](#references)

### 7.5 Scaling architecture (the structural critique)

**F1.** *"always thought Neo4j was a joke, it was based on an execution
model which is not scalable at all... story after story from people who
tried it for projects which were just too big"* — from the 2022 "Ask HN:
What Is Going on with Neo4j?" layoffs thread [[37]](#references).

**F2.** *"You can't shard it... you could only scale vertically, not
horizontally."* [[38]](#references)

**F3.** The "Trillion Relationship Graph" marketing claim earned its own
debunking thread [[39]](#references).

### 7.6 Churn stories (who left, and where they went)

**G1.** *"We're in the process of migrating off Neo4j/OngDB to Postgres.
Happy with how it's going so far."* [[40]](#references)

**G2.** ConceptNet moved off entirely (see C2) — to purpose-built storage.

**G3.** Whole HN threads exist of people soliciting and comparing
alternatives (FalkorDB, Memgraph, ArangoDB, Apache AGE, KuzuDB...)
[[41]](#references), [[42]](#references).

### 7.7 Honesty: the counter-log

Neo4j also has real defenders, and an honest log must show them — otherwise
this document becomes a pitch deck wearing a lab coat:

- *"Had the exact opposite experience with N4j. Easy to operate, scale and
  run... large scale enterprise rollout"* [[43]](#references).
- GDS itself is praised: *"The built-in Graph Data Science package has a lot
  of nice graph algos that are easy to [use]"* [[44]](#references).
- Cypher's ergonomics and the browser UI are consistently liked even by
  critics (B2: "...just with a nicer UI").
- Several of the harshest complaints (C1, C2) date to 2011-2015; the product
  has improved materially since. **Age of evidence must be weighed**, and
  each item above carries its date for exactly that reason.

### 7.8 What the aggregate log says

| Complaint theme | Volume | Age span | Do WE fix it? |
|-----------------|--------|----------|----------------|
| Pricing / licensing | **Loudest** | 2011-2026 | Indirectly — own-hardware, open engine, no meter |
| Memory / RAM / OOM | High | 2015-2026 | **YES — the core product** |
| Slow (OLTP queries) | High | 2013-2026 | **NO** — we are OLAP-only, and must say so loudly |
| Reliability / JVM ops | Medium | 2011-2022 | Partly — static binary, fixed arena, no GC |
| Can't scale out | Medium | 2015-2026 | Sidestepped — scale-UP via disk, not scale-OUT |
| Vendor trust | Medium | 2018-2026 | YES, by being boring: open format, receipt, no meter |

Two product lessons the wide log adds beyond the GDS-specific passes:

1. **Pricing/licensing resentment is the emotional carrier wave.** The RAM
   receipt lands harder because the audience already distrusts the meter.
   The pitch order should therefore be: *no meter → no projection → receipt
   → algorithms.*
2. **We must explicitly NOT claim to fix the top-volume complaint we don't
   address** (OLTP query slowness / Cypher performance). Being loudly
   OLAP-only converts a scope limitation into credibility with an audience
   primed to smell overclaiming. Positioning by exclusion is still
   positioning — often the strongest kind.

---

## 8. The badges (Shreyas Doshi lens): what we get to wear that they can't

Shreyas distinguishes three levels of differentiation: **execution-level**
(you do the same thing better — copyable in quarters), **feature-level**
(you have something they lack — copyable in a year), and **insight-level**
(your advantage flows from something the competitor *cannot* adopt without
damaging themselves — durable). The badges below are ranked by that ladder,
weighted by the evidence of Sections 2-7.

| # | Badge | Why it's ours | Differentiation level | Evidence base |
|---|-------|---------------|----------------------|---------------|
| 1 | **"THE BILL BEFORE THE RUN"** — exact RAM + wall-clock computed from ~1 KB of metadata, before a byte is read | Neo4j cannot sell certainty without breaking the GB-hour meter; their own Louvain estimator source comments "rough estimate of graph size" | **Insight-level** — uncopyable for *business* reasons, not technical ones | E1 [[1]](#references), E5-E7 [[5]](#references)[[6]](#references)[[7]](#references); GDS Louvain source (Arch06); Arch06 ex. 7 (time receipt) |
| 2 | **"NO PROJECTION STEP"** — the wall every evidence pass converged on | Vendor-confirmed structural: *"no option to spill to disk"* (their engineer); *"defeats the purpose of using a database"* (their user) | Feature-level today; hard to retrofit into a JVM heap architecture | E2 [[2]](#references), E3/E16 [[3]](#references), E17 [[17]](#references); funnel hypothesis (§6.3) |
| 3 | **"FINISHES ON THE MACHINE YOU OWN"** — the money badge | Rides the loudest emotional theme (meter resentment, 2011-2026) — but is *derivative* of #1+#2: streaming alone earns the "solved in 2012" rebuttal; the receipt is what makes it a product | Feature-level | 110-254 GB sessions → 8-16 GB box; §7.4; GraphChi rebuttal (§4, P2) |
| 4 | **"LOUDLY OLAP-ONLY"** — a negative badge: we do NOT fix their biggest complaint (OLTP/Cypher slowness) | Positioning by exclusion — refusing to overclaim buys credibility with an audience primed to smell overclaiming | Positioning-level | §7.8: OLTP slowness is high-volume and explicitly not ours |
| 5 | **"BORING AND TRUSTWORTHY"** — open format, static Rust binary, no GC, no license bait-and-switch | Speaks to the vendor-trust wound (license litigation, bait-and-switch resentment) | Table-stakes hygiene, not a moat | §7.4 (D4 [[33]](#references), D5 [[36]](#references)); §7.3 (C3 [[31]](#references): JVM toolchain pain) |

The pitch order the evidence supports:

> **no meter** (why care) → **no projection** (what's gone) → **the receipt**
> (the proof) → **algorithms** (the proof points)

**Badge discipline.** Badge #1 is the only one that is uncopyable for
*business* rather than technical reasons — everything else a well-funded
competitor could ship within quarters. Therefore every product decision
should be scored first by a single question: *does this strengthen the
receipt* (byte-honest, cgroup-verifiable, time-quoted)? A feature that makes
the receipt fuzzier is a strategic loss regardless of how much it helps
elsewhere.

---

## 9. Competitive landscape: why is the low-RAM turf empty?

The Shreyas move here is to treat an empty market as a **question, not a
gift**. Empty turf means one of two things: nobody wants it (a graveyard),
or everyone who could occupy it has a structural reason not to (an
opportunity). Distinguishing the two requires going far and wide — so this
pass swept every notable graph engine, library, and platform, using the
GitHub API for stars and liveness (checked 2026-07). The question asked of
each: *does it occupy our turf — bounded-RAM, disk-streaming analytics with
a cost receipt?*

### 9.1 The table

#### Graph databases (OLTP-first: query languages, transactions)

| Tool (stars, status) | Primarily used for | On our turf? |
|----------------------|--------------------|--------------|
| Neo4j [[45]](#references) + GDS [[46]](#references) | Property-graph OLTP + in-heap analytics | **NO** — the incumbent whose wall we fix |
| Dgraph (21.7k, active) [[47]](#references) | Distributed OLTP, GraphQL-native | NO — scale-OUT answer, not low-RAM |
| NebulaGraph (12.3k, active) [[48]](#references) | Distributed OLTP at billion-edge scale | NO — same |
| ArangoDB (14.2k, active) [[49]](#references) | Multi-model (document + graph) | NO |
| JanusGraph (5.8k, active) [[50]](#references) | Distributed OLTP over Cassandra/HBase | NO — RAM-heavy, ops-heavy |
| Memgraph (4.2k, active) [[51]](#references) | **In-memory** OLTP + streams (pitched for GraphRAG) | **OPPOSITE turf** — doubles down on RAM |
| FalkorDB (4.7k, active) [[52]](#references) | Sparse-matrix Cypher (GraphBLAS), RAG focus | NO — in-memory, latency-first |
| Kuzu (4.0k, **ARCHIVED**) [[53]](#references) | Embedded OLAP graph DB, columnar, out-of-core joins | **CLOSEST DB** — but the company died Oct 2025 (Apple acqui-hire [[54]](#references)); repo frozen |
| Apache AGE (4.7k, active) [[55]](#references) | Cypher inside Postgres | NO — convenience, not scale; algorithms basic |
| DuckPGQ (0.4k, active) [[56]](#references) | SQL/PGQ graph queries in DuckDB | **ADJACENT** — DuckDB ethos = ours, but pattern matching, not iterative algorithms |
| TuGraph (1.7k, active) [[57]](#references) | Ant Group's graph DB | NO |

#### Analytics libraries (bring your own RAM; no storage story)

| Tool (stars, status) | Primarily used for | On our turf? |
|----------------------|--------------------|--------------|
| igraph (2.0k, active) [[58]](#references) | C library with R/Python bindings; academia's workhorse | NO — in-RAM only; **dies where we start** |
| NetworKit (0.9k, active) [[59]](#references) | Parallel in-RAM network science (C++/Python) | NO — same |
| NetworkX (huge, active) [[60]](#references) | Pure-Python teaching/prototyping standard | NO — 10-100x slower than igraph even in-RAM |
| SNAP (2.3k, dormant) [[61]](#references) | Stanford's C++ library | NO — in-RAM |
| GBBS [[62]](#references) / Ligra [[63]](#references) (academic) | Shared-memory parallel algorithm suites | NO — assumes the graph fits |
| cuGraph (2.2k, active) [[64]](#references) | GPU graph analytics (RAPIDS) | NO — needs GPU + VRAM budget; the vertical-scaling answer on a card |

#### Out-of-core engines (our technical ancestors)

| Tool (status) | Primarily used for | On our turf? |
|---------------|--------------------|--------------|
| GraphChi (0.8k, **DEAD** since 2019) [[65]](#references) | The OSDI'12 proof [[66]](#references) that a laptop can process billion-edge graphs via disk | **YES technically** — but research code: no product, no receipt, JVM/C++ |
| X-Stream [[67]](#references), GridGraph [[68]](#references), FlashGraph [[69]](#references), Mosaic [[70]](#references) | 2013-2017 academic out-of-core systems | **YES technically** — all unmaintained paper artifacts |
| GraphScope (3.6k, active) [[71]](#references) | Alibaba's one-stop distributed graph computing | NO — cluster-scale answer (scale-OUT) |

#### Paid platforms

| Tool | Primarily used for | On our turf? |
|------|--------------------|--------------|
| TigerGraph [[72]](#references) | Enterprise distributed analytics | NO — scale-out MPP, enterprise $$ |
| AWS Neptune + Neptune Analytics [[74]](#references) | Managed OLTP + RAM-provisioned analytics | NO — **the SAME meter**: m-NCU = memory-metered billing |
| Neo4j Aura Graph Analytics [[75]](#references) | Neo4j's metered GDS | The thing itself |

### 9.2 The Shreyas answer: WHY the turf is empty

Not because it is impossible — GraphChi proved the physics in **2012**
[[66]](#references), and then died. The turf is empty because **every class
of player faces a structural, self-interested reason not to stand on it**.
This is the pattern Shreyas calls out when he distinguishes "nobody has done
it" (a warning) from "everybody is incentivized not to do it" (an
invitation):

| Player class | Why they won't build low-RAM + receipt |
|--------------|----------------------------------------|
| Incumbents (Neo4j, Neptune) | Their **revenue IS the RAM meter** (Aura GB-hours, Neptune m-NCUs). Certainty cannibalizes the meter. Asking them to ship the receipt is asking them to un-invent their own billing model. |
| In-memory challengers (Memgraph, FalkorDB) | Their **pitch is latency**. Admitting that disk is fine for analytics undermines their one differentiator against Neo4j. |
| Libraries (igraph, NetworKit, cuGraph) | **No storage layer exists at all** — "bring your own RAM" is the design. Cost estimation is out of scope by construction. |
| Academia (the GraphChi line) | Papers reward novel algorithms, not receipts, packaging, or decade-long maintenance. **Code dies at tenure.** |
| The scale-out camp (Dgraph, Nebula, GraphScope) | The 2015-2020 zeitgeist said the answer to big graphs is MORE MACHINES. An entire funding generation went to horizontal scaling; none went to frugality. |
| Kuzu — the one that got close | Validated the adjacent turf (embedded, columnar, out-of-core JOINS) — then was acqui-hired [[54]](#references) before ever reaching iterative-analytics-with-a-receipt. The near-miss that proves the demand and vacates the seat simultaneously. |

The five-forces reading: the *technique* is public and thirteen years old;
the GAP is a **product gap** (receipt, bounded arena, per-algorithm access
plans) compounded by a **business-model gap** (nobody with distribution is
*incentivized* to sell RAM-frugality). That second gap is the moat — the
same conclusion Section 8 reached from the badge side, now confirmed from
the competitor side. When two independent analyses converge on the same
moat, Shreyas would say you have found your strategy kernel.

### 9.3 Differentiation one-liners per near-neighbor

- **vs igraph/NetworKit:** "We start where they OOM; they have no disk story."
- **vs cuGraph:** "No GPU required; our budget is the receipt, not VRAM."
- **vs Kuzu (RIP):** "They proved embedded-OLAP demand; we add iterative
  algorithms plus the receipt, in maintained form."
- **vs DuckPGQ:** "Same ethos, different layer: they query patterns, we run
  iterative analytics. Potential ALLY (export sink), not rival."
- **vs the GraphChi lineage:** "They are our physics citation, not a rival:
  dead code, no estimation, no product."
- **vs Neo4j/Neptune:** "They cannot copy the receipt without breaking the
  meter."

---

## 10. Notes from an external LLM deep-research pass (user-supplied)

A parallel deep-research report (independent LLM with web search, 2026-07)
was reviewed against our catalog. Items below extend Sections 2-7. The
report's citation links were not exported with its text, so a re-sourcing
pass (2026-07) was run by us: items we could independently locate now carry
URLs and are marked **VERIFIED**; the rest remain **[ext-unverified]** and
must not be used in public material until sourced.

### 10.1 New pain anecdotes worth keeping

| Status | Anecdote |
|--------|----------|
| **VERIFIED** | An Aura **free-tier** user OOM'd on `gds.graph.project()` while following Neo4j's *own* Graph Academy GDS-fundamentals course: *"it is stated that it is possible to use AuraDB free tier for the course. However, I have found this not to be true."* [[76]](#references) The vendor's teaching material does not run on the vendor's entry-level product. |
| **VERIFIED** | NodeSimilarity blocked outright: *"Procedure was blocked since minimum estimated memory (130 GiB) exceeds current free memory (24 GiB)"* — on a graph the user then shrank to 2,594 nodes and **still** received a 54 GiB estimate; *"motivating us to look elsewhere for scale."* [[73]](#references) A direct, algorithm-named hit that partially fills the §6.2 evidence gap. |
| **VERIFIED** | `gds.graph.drop` does **not** return memory to the OS (JVM GC behavior) — the user is forced into stop/start restarts to reclaim RAM [[77]](#references). A new complaint class for us: our munmap actually releases. |
| **VERIFIED** (adjacent find) | A Louvain run taking 5 hours and >70 GB of heap on a 60 GiB store (community edition 3.5) [[78]](#references). |
| [ext-unverified] | K8s sidecar with 120 GB provisioned still OOM-crashed at ~40 GB used — manual JVM threshold management fails even with headroom. (Could not locate the original post.) |
| [ext-unverified] | Delta-stepping OOM'd with a 12 GB heap on a 63k-node graph. (Could not locate; nearest verified path-OOM evidence remains E18/E19 [[18]](#references)[[19]](#references).) |
| [ext-unverified] | Louvain on the Yelp dataset: DB corruption + ~23 GB consumption. (Could not locate; the SO post above [[78]](#references) is the closest verified analog.) |
| [ext-unverified] | Aura pricing quotes: *"$70k a year isn't even nearly competitive"*; Neptune claimed ~1/6th the price at similar provisioning (flagged by the report itself as workload-dependent). |
| [ext-unverified] | Workaround culture: projecting node IDs only and re-MATCHing attributes — trading the memory wall for an I/O latency wall. |

### 10.2 GraphRAG cost-split confirmation

The external report independently reaches our Section 3 correction: LLM
token cost dominates GraphRAG, BUT the Leiden/community-detection phase is
the CPU/RAM-bound slice, unstable on dense graphs, and re-runs from scratch
on every re-index (stochastic and seed-dependent — one cited team proposed
k-core decomposition just to obtain determinism). Two additions worth
keeping:

- Microsoft GraphRAG reportedly drops **~10% of entities** because Leiden
  discards weakly-connected/isolated nodes — an *accuracy* complaint against
  the incumbent pipeline, directly useful for our exactness-flags story.
  [ext-unverified]
- "GraphRAG is 20-100x more expensive than vector RAG" — the indexing phase
  is the adoption blocker we relieve (the graph-compute slice).
  [ext-unverified]

### 10.3 Incumbent-response scenarios (adopted into our planning)

The report's forecast of Neo4j counter-moves, kept as planning input and
mapped to our pre-prepared answers:

1. **FUD via latency benchmarks** conflating OLTP with batch analytics —
   pre-answered by the "loudly OLAP-only" badge (§7.8, §8).
2. **"Serverless" pricing tiers that HIDE the RAM meter** rather than remove
   it — obfuscation, not architecture; the receipt is the counter-story.
3. **An Apache Arrow off-ramp**: position Neo4j as the storage hub and push
   compute out to Polars/DuckDB/cuGraph — which would *validate* the export
   sidecar as our critical wedge (§4, P1 journey risk).
4. If genuinely disruptive: an **acquisition attempt** ("cold-storage
   analytics tier under GDS").

### 10.4 Segments (matches ours, one addition)

The report's top-3 segments match ours exactly — GraphRAG builders;
mid-market data engineers with nightly ETL; academics/bioinformaticians.
One addition worth keeping: **local AI agents on consumer hardware** — an
embedded, 16 GB-class buyer for whom Aura is disqualified outright.

### 10.5 Methodological flags (theirs and ours)

The report itself flags that the Neptune-1/6th-price and "custom Rust DB =
1000x" claims are workload-dependent folklore. We additionally flag: the
report's citation links were not exported, so our re-sourcing pass verified
four §10.1 items with URLs; the remainder stay [ext-unverified] and are
quarantined from public use. The core memory-wall thesis, however, is now
**triangulated three independent ways**: our own API sweeps (§2, §6, §7),
the GDS source code itself (Arch06), and this external pass. Triangulation
of this kind is the closest a pre-build product can get to proof.

---

## 11. Reading the competitors' own code: the shallow-clone audit

Section 9 judged competitors from the outside (stars, liveness, positioning).
This pass goes one level deeper — the Shreyas move of *auditing the artifact,
not the press release*. All ~20 competitor repos were shallow-cloned into
`reference-repos-competitors/` in this repository (gitignored, `*-src`
naming, alongside the existing `reference-repos-neo4j-family/`), and the
near-neighbors were read.

### 11.1 The Kuzu finding: the closest competitor had OUR wall inside it

Kuzu [[53]](#references) was Section 9's "CLOSEST DB" — embedded, columnar,
disk-based, out-of-core joins. Reading the archived source (final commit
2025-10-10, one month before the repo froze) yields the single most
strategically comforting discovery of this entire dossier:

**Kuzu's graph algorithms run on a full in-memory CSR copy of the graph.**

The `algo` extension (`extension/algo/` in `kuzu-src`) implements WCC,
SCC (x2), PageRank, Louvain, k-core decomposition, spanning forest, and
component ids — and every one of them is fed by `InMemGraph`
(`extension/algo/src/common/in_mem_graph.cpp`): a `csrOffsets` +
`csrEdges` pair of in-memory vectors, rebuilt per run, with the header
comment "CSR-like in-memory representation of an undirected weighted
graph... Undirected edges should be explicitly inserted twice."

Read that against their own storage layer: Kuzu's core is genuinely
disk-based and columnar (`src/storage/`: buffer manager, compression,
disk arrays, WAL). The *queries* stream from disk — but the moment you ask
for Louvain or PageRank, the graph is projected into RAM, doubled for
undirectedness, exactly like `gds.graph.project`. **The projection wall
lived inside our closest competitor too.** And `grep -ri estimate
extension/algo/` returns nothing: no memory estimation, no receipt, no
pre-run bill of any kind.

| What the Kuzu clone shows | Measurement |
|---------------------------|-------------|
| Total graph-algorithm code | ~3,400 LOC (7 algorithm files) |
| Algorithm families covered | WCC, SCC, PageRank, Louvain, k-core, spanning forest — **no** NodeSimilarity, no embeddings/FastRP, no triangle counting |
| Algorithm storage model | Full in-memory CSR (`InMemGraph`), rebuilt per run, 2x for undirected |
| Pre-run cost estimation | None (zero hits for "estimate" in the extension) |
| Where the engineering actually went | Extension list in `extension_config.cmake`: azure, delta, duckdb, fts, httpfs, iceberg, json, **llm**, postgres, sqlite, unity_catalog, **vector**, **neo4j** (a migration tool), algo |

### 11.2 Why they were acquired so quickly — the code answers it

The question was: why did Kuzu get acqui-hired (Apple, Oct 2025
[[54]](#references)) so fast, seemingly mid-product? The shallow clone
suggests a three-part answer:

1. **The crown jewels were the query core, not the analytics.** The
   valuable, deeply-engineered code is in `src/processor/`,
   `src/storage/`, and the factorized/vectorized join machinery — years of
   world-class database-systems work by a small team. That is exactly what
   an acquirer of *talent and engine* pays for. The graph-analytics story —
   our turf — was a ~3.4k-LOC extension bolted on late, still in-memory,
   still receipt-less. Apple bought a database team, not a graph-analytics
   product, because the graph-analytics product did not exist yet.
2. **The extension list documents a breadth pivot.** llm, vector, fts,
   iceberg, delta, unity_catalog, a neo4j-migration extension — in the final
   year the energy went to RAG/lakehouse connectivity (where the 2024-2025
   money was), not to deepening the analytics engine. Classic Shreyas
   pattern: when a startup's roadmap becomes a mirror of the current hype
   cycle, it is fundraising with features — and an acqui-hire is often the
   next event.
3. **Therefore the turf was never actually occupied.** Section 9 called Kuzu
   the closest competitor "acqui-hired before completing the product." The
   code sharpens this: even uncompleted, they were building toward *embedded
   OLTP+OLAP with RAG trimmings*, not bounded-RAM analytics with a receipt.
   Had they never been acquired, they would still have hit our wall —
   `InMemGraph` — and would have had to rebuild their algorithm layer to
   escape it.

### 11.3 Spot-checks on the rest of the shelf

| Repo (local clone) | What the code confirms |
|--------------------|------------------------|
| `graphchi-cpp-src` | Last commit 2019-01-02; 58 header files of research C++ — the out-of-core physics proof [[65]](#references)[[66]](#references), frozen. No estimation layer, no product surface. Our citation, not our rival. |
| `memgraph-src` | The storage engine is literally named `InMemoryStorage` (`src/storage/v2/inmemory/`) — in-memory is the architecture, not a configuration. They structurally cannot follow us to disk. |
| `falkordb-src` | README leads with "Powering Generative AI, Agent Memory..." — latency/RAG positioning; GraphBLAS sparse matrices resident in RAM. |
| `ligra-src` / `gbbs-src` | Shared-memory academic suites; assume the graph fits by design. GBBS still active (2025) but is a library of algorithms, not a storage product. |
| `flashx-src` | Semi-external-memory (SSD) research line — the closest ancestral DNA to our plan — dead since 2017. |

### 11.4 What this pass changes

Nothing in our strategy — and that is the point. This is the fourth
independent line of evidence (after our API sweeps, the GDS source, and the
external research pass) converging on the same conclusion: the bounded-RAM +
receipt turf is empty, and even the competitor that came closest carried the
projection wall inside its own `extension/algo/` directory. When you can
read the incumbent's *and* the near-neighbor's code and find your product
missing from both, you are no longer guessing about whitespace.

---

## 12. Closing synthesis: why is nobody building lower-RAM graph databases?

The final question this dossier must answer in one place. The answer is not
one reason but **five interlocking mental models** — each rational in
isolation — that together leave the turf empty. This synthesizes Sections
9 and 11 with the wider evidence.

### 12.1 The five reasons

**1. The industry's dominant belief: "RAM got cheap, so disk-frugality is a
solved non-problem."** From ~2012 onward, the systems community internalized
"just buy a bigger box or a cluster." It is *mostly true* — which is exactly
what makes it dangerous. Beliefs that are 90% true stop being questioned,
and the 10% (procurement limits, laptops, cloud RAM meters, GraphRAG on
consumer hardware — E1 [[1]](#references), E4 [[4]](#references), §10.4)
becomes invisible whitespace. This is an industry-wide blind spot created by
a mostly-correct heuristic.

**2. Low-RAM is a *ceiling on revenue* for anyone who sells hosting.** The
moment you monetize by GB-hour (Aura [[75]](#references), Neptune m-NCUs
[[74]](#references)), building frugality is building your own pay cut. This
is not hypothetical — it is why "estimate then BLOCK" exists in GDS
[[1]](#references) while "estimate then FINISH ANYWAY" does not. The feature
that would help users most is the one that shrinks the bill.

**3. Databases are judged by benchmarks, and benchmarks measure latency, not
certainty.** Every graph DB competes on queries/second because that is what
wins the HN launch and the analyst slot. "Runs on 8 GB instead of 256 GB,
3x slower" *loses every published benchmark* while winning the actual user.
Nobody builds what the scoreboard punishes. The receipt is a new scoreboard
— which is precisely why it is uncomfortable for incumbents to adopt.

**4. The engineering is miserable in a specific, unglamorous way.** The
cloned repos (§11) tell this story directly. Out-of-core *iterative*
analytics needs a bespoke storage plan per algorithm (Arch06), careful I/O
scheduling, and honest accounting. GraphChi / X-Stream / FlashX did the hard
part, got the paper, and died [[65]](#references)–[[70]](#references) —
because the last 80% (packaging, estimation, maintenance) earns no citations
and no VC story. Meanwhile Kuzu — the one funded team with the right
disk-based bones — took the easy path for its analytics layer: `InMemGraph`,
a full in-RAM CSR (§11.1), because in-memory algorithms are a weekend and
out-of-core ones are a year. Then it pivoted to llm/vector/iceberg
extensions chasing the 2024-25 hype cycle and was acqui-hired
[[54]](#references). The code is a confession: even the closest competitor
chose speed-to-demo over the wall.

**5. The buyer who feels this pain has no lobby.** The person OOMing on a
16 GB box is a mid-market data engineer or a solo GraphRAG builder — not an
enterprise account with a CIO budget. Vendors build for whoever signs the
big checks, and the big checks come from people who can afford the RAM.
Classic underserved-segment dynamics: the pain is real, distributed, and
unmonetized *by anyone whose business model permits monetizing it*.

### 12.2 The one-liner

> **The turf is empty not because nobody can build it, but because everyone
> who could is either paid not to (vendors), scored not to (benchmarks),
> promoted not to (academia), or funded not to (startups chasing the hype
> cycle).**

That is the best kind of empty — structural, not accidental.

### 12.3 The honest warning

The only genuine warning in the data is reason #1: if the market segment
that cannot "just buy RAM" turns out to be too small, the incumbents were
right to ignore it, and this whole strategy is a well-documented mistake.
That is the bet. It is also why the GraphRAG and local-AI-agent wave
(§10.4) matters strategically: it is minting new members of exactly that
segment — analytics-hungry users on fixed consumer hardware — every month.
The bet is not that the segment exists (it demonstrably does — §2, §7); the
bet is that it is growing.

---

## 13. Who actually uses these seven algorithms — practical use cases, with public evidence

A strategy document that names seven algorithm families owes the reader an
answer to the blunt question: *who actually runs these, for what, and where
is the public proof?* Every company attribution below is backed by a
published paper or engineering blog on the open web — no private telemetry,
no internal-discussion claims. Where the usage pattern is industry-standard
but not publicly attributed to a named company (e.g., specific banks' fraud
stacks), that is said explicitly.

> ASCII walkthroughs of each family — use case, raw data, storage, and how
> the algorithm works, with two worked examples each — live in the companion
> document `AlgoExplainers-ASCII.md`.

### 13.1 The seven families and their documented uses

**WCC — Weakly Connected Components (~20% of modeled adoption).** The
workhorse of *entity resolution*: build a similarity graph over records
(shared email, device, address) and every connected component is one
real-world entity. Neo4j's own fraud-detection materials lead with WCC as
the first-ring-finding step [[79]](#references), and the GDS manual
describes it as a canonical early step in analytics pipelines
[[80]](#references). The same primitive drives master-data-management and
dedup pipelines across retail, insurance, and AML — a documented pattern,
though individual banks rarely publish their stacks by name.

**Louvain / Leiden — community detection (~15%).** Finding "clumps": fraud
rings, customer segments from co-purchase graphs, citation and
protein-interaction modules. The Leiden algorithm itself is published in
Nature Scientific Reports by CWTS Leiden [[81]](#references). Its newest and
fastest-growing consumer is **Microsoft GraphRAG**, which runs Leiden over
an LLM-extracted entity graph to build hierarchical community summaries —
stated directly in the GraphRAG paper [[82]](#references) and documentation
[[83]](#references). Every GraphRAG deployment is a Leiden user.

**PageRank (~15%).** Influence and importance scoring far beyond web search.
Twitter's "Who To Follow" service ran personalized PageRank / SALSA variants
over the full follower graph — published at WWW 2013 [[84]](#references).
Pinterest's Pixie recommender is random-walk-with-restart (PageRank's
query-time cousin) over a 3-billion-item graph [[85]](#references). In
fraud, PageRank scores the centrality of accounts in money-flow networks —
a use Neo4j documents in its fraud materials [[79]](#references).

**NodeSimilarity / kNN (~12%).** "Which nodes behave alike" over bipartite
graphs (user-product, account-device, patient-drug): recommendation
candidate generation and fraud pattern-matching (accounts sharing many
devices/IPs). Alibaba published billion-scale item-similarity-and-embedding
recommendation over its product graph [[86]](#references); the
insurance/claims-similarity variant is an industry-documented pattern.

**Shortest paths / BFS / Dijkstra (~10%).** Routing and logistics, plus the
bigger enterprise uses: supply-chain impact analysis ("which products break
if this supplier fails" is a BFS), network dependency tracing, and
degrees-of-separation features. LinkedIn's LIquid engineering series
describes its graph serving the connection-distance and network features at
member scale [[87]](#references). Data-lineage tools express column-level
impact analysis as path queries over dependency graphs.

**FastRP — graph embeddings (~8%).** Turning nodes into vectors for
downstream ML: fraud-model features, churn prediction, recommendation
embeddings. The FastRP algorithm is published at CIKM 2019
[[88]](#references) and is Neo4j's flagship CPU embedding
[[89]](#references). The wider graph-embeddings-feed-ML pattern is
documented at production scale by Pinterest's PinSage (KDD 2018)
[[90]](#references), Uber Eats' graph learning for food recommendation
[[91]](#references), and Alibaba [[86]](#references). FastRP is the
cheap-and-cheerful CPU member of that same family.

**Triangle counting / clustering coefficient (~5%).** Mostly a *feature
factory*: triangle counts and clustering coefficients are strong features in
fraud and fake-account models (fabricated networks have abnormal
clustering), social-capital scoring, and community-quality metrics. The
canonical web-scale study (Suri & Vassilvitskii, WWW 2011) counts triangles
on the Twitter follower graph precisely because clustering signals matter at
social-network scale [[92]](#references). Bot detection uses the same
signal: bot follower networks show near-zero clustering.

### 13.2 The cross-cutting pattern (the Shreyas read)

The paying use cases cluster into three buyers:

| Buyer | Families used | Character of the workload |
|---|---|---|
| **Fraud / AML teams** (banks, payments, insurance) | WCC + Louvain + PageRank + triangles, together | Re-run repeatedly on the same growing graph — the warm-start story |
| **Recommendation / ML platform teams** | NodeSimilarity + FastRP (embedding feature pipelines) | Batch feature generation feeding downstream models |
| **GraphRAG builders** | Leiden (community summaries) | Run on hardware whose size they do not control |

Fraud is the deepest-pocketed and most re-run-heavy segment; GraphRAG is the
fastest-growing and most RAM-constrained. This is the same beachhead
conclusion the complaint evidence reached (§2-§7) — the use-case side of the
market now independently agrees with the pain side.

*Honesty note:* the Uber Eats blog URL [[91]](#references) returns a
bot-block (HTTP 406) to non-browser clients but loads normally in a browser;
all other URLs in this section were verified reachable (HTTP 200) at the
time of writing.

---

## 14. References

Every URL cited in this document, numbered in order of first appearance.

### Neo4j Community Forum (first pass)

1. "How to efficiently cluster nodes using GDS with limited memory (16 GB RAM)" — https://community.neo4j.com/t/how-to-efficiently-cluster-nodes-using-gds-with-limited-memory-16-gb-ram/71073
2. "What library to use instead GDS when graph db is too big to project in memory?" — https://community.neo4j.com/t/what-library-to-use-instead-gds-when-graph-db-is-too-big-to-project-in-memory/55821
3. "GDS algorithms without a projection" — https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039
4. "What is the ideal heap memory size for GDS in Neo4j" — https://community.neo4j.com/t/what-is-the-ideal-heap-memory-size-for-gds-in-neo4j/76311
5. "Memory Limit on graph projection" — https://community.neo4j.com/t/memory-limit-on-graph-projection/61567
6. "How can I load a very large dataset with limited memory?" — https://community.neo4j.com/t/how-can-i-load-a-very-large-dataset-with-limited-memory/59189
7. "GDS ShortestPath memory consumption" — https://community.neo4j.com/t/gds-shortestpath-memory-consumption/58340

### Hacker News (pricing / RAM resentment)

8. Pricing "heart attack" thread (2019) — https://news.ycombinator.com/item?id=18797980
9. "10% of our ARR" / centralized-graph pricing (2021) — https://news.ycombinator.com/item?id=27544889
10. "chose vanilla Postgres instead... insane licenses" (2020) — https://news.ycombinator.com/item?id=22485576
11. "pretty much required that data fits in memory" (2015) — https://news.ycombinator.com/item?id=8899483
12. "GraphRAG preprocessing is insanely expensive" (2025) — https://news.ycombinator.com/item?id=45063386
13. GraphRAG cost explainer reply — https://news.ycombinator.com/item?id=45068902
14. "you have to deal with Neo4j and who wants that" (2025) — https://news.ycombinator.com/item?id=46347143

### Stack Overflow (first pass)

15. "How can I create graph Projections in Neo4J for a very large graph" — https://stackoverflow.com/questions/79650281/how-can-i-create-graph-projections-in-neo4j-for-a-very-large-graph
16. "Keep a projected graph in sync with persisted graph in Neo4j GDS" — https://stackoverflow.com/questions/73258583/keep-a-projected-graph-in-synch-with-persisted-graph-in-neo4j-gds

### Second pass (other four families)

17. "Is there any way to create a graph projection if your graph is too big to fit in memory?" — https://stackoverflow.com/questions/69092539
18. GDS GitHub issue #55: shortestPath OutOfMemoryError — https://github.com/neo4j/graph-data-science/issues/55
19. GDS GitHub issue #139: personalized PageRank "unable to create native thread" — https://github.com/neo4j/graph-data-science/issues/139
20. FastRP usage-confusion thread — https://community.neo4j.com/t/dgs-fastrp-write-returns-failed-to-invoke-procedure-gds-fastrp-write-caused-by-java-l/51294

### Aggregate log — Hacker News (2011-2026)

21. A1: memory-consumption wariness, Series F thread — https://news.ycombinator.com/item?id=27543721
22. A2: ex-insider on Java memory-mapping issues — https://news.ycombinator.com/item?id=31509341
23. A3: 32-billion-ID ceiling — https://news.ycombinator.com/item?id=33918730
24. B1: "nightmares with Cypher and neo4j's slowness" — https://news.ycombinator.com/item?id=48647551
25. B2: "slower for graphs than postgres, just with a nicer UI" — https://news.ycombinator.com/item?id=27544079
26. B3: "Neo4j is dead slow" (2013) — https://news.ycombinator.com/item?id=6693003
27. B4: "freaking slow when I tried to use it" (2025) — https://news.ycombinator.com/item?id=48581477
28. B5: slower than a normal vector DB — https://news.ycombinator.com/item?id=37871190
29. C1: "least reliable and most buggy database" (2015) — https://news.ycombinator.com/item?id=9699964
30. C2: ConceptNet author, "lost months of work" (on 2011 Neo4j) — https://news.ycombinator.com/item?id=9700558
31. C3: "crashed constantly... Java SDK versions" — https://news.ycombinator.com/item?id=33916804
32. D3: "very expensive if you want to use it in a cluster" — https://news.ycombinator.com/item?id=7804908
33. D4: "bait and switch with the license model" — https://news.ycombinator.com/item?id=33916759
34. Neo4j v PureThink / AGPL litigation thread (2022) — https://news.ycombinator.com/item?id=30726286
35. Neo4j v PureThink follow-up thread (2023) — https://news.ycombinator.com/item?id=34763955
36. D5: sales-side, "totally inflexible... outdated licensing model" — https://news.ycombinator.com/item?id=33918651
37. F1: "execution model which is not scalable at all" (2022 layoffs thread) — https://news.ycombinator.com/item?id=33916259
38. F2: "You can't shard it" — https://news.ycombinator.com/item?id=33919132
39. F3: "Trillion Relationship Graph" debunking thread — https://news.ycombinator.com/item?id=28707310
40. G1: "migrating off Neo4j/OngDB to Postgres" — https://news.ycombinator.com/item?id=33916848
41. G3a: alternatives-comparison thread — https://news.ycombinator.com/item?id=43202780
42. G3b: alternatives-comparison thread — https://news.ycombinator.com/item?id=48358865
43. Counter-log: "exact opposite experience with N4j" — https://news.ycombinator.com/item?id=33917206
44. Counter-log: GDS algorithms praised — https://news.ycombinator.com/item?id=41269987

### Competitive landscape (Section 9)

45. Neo4j — https://github.com/neo4j/neo4j
46. Neo4j Graph Data Science — https://github.com/neo4j/graph-data-science
47. Dgraph — https://github.com/hypermodeinc/dgraph
48. NebulaGraph — https://github.com/vesoft-inc/nebula
49. ArangoDB — https://github.com/arangodb/arangodb
50. JanusGraph — https://github.com/JanusGraph/janusgraph
51. Memgraph — https://github.com/memgraph/memgraph
52. FalkorDB — https://github.com/FalkorDB/FalkorDB
53. Kuzu (archived) — https://github.com/kuzudb/kuzu
54. Kuzu shutdown / acqui-hire discussion (HN) — https://news.ycombinator.com/item?id=44383243
55. Apache AGE — https://github.com/apache/age
56. DuckPGQ — https://github.com/cwida/duckpgq-extension
57. TuGraph — https://github.com/TuGraph-family/tugraph-db
58. igraph — https://github.com/igraph/igraph
59. NetworKit — https://github.com/networkit/networkit
60. NetworkX — https://github.com/networkx/networkx
61. SNAP — https://github.com/snap-stanford/snap
62. GBBS — https://github.com/ParAlg/gbbs
63. Ligra — https://github.com/jshun/ligra
64. cuGraph — https://github.com/rapidsai/cugraph
65. GraphChi — https://github.com/GraphChi/graphchi-cpp
66. GraphChi paper (OSDI 2012) — https://www.usenix.org/system/files/conference/osdi12/osdi12-final-126.pdf
67. X-Stream paper (SOSP 2013) — https://dl.acm.org/doi/10.1145/2517349.2522740
68. GridGraph paper (USENIX ATC 2015) — https://www.usenix.org/system/files/conference/atc15/atc15-paper-zhu.pdf
69. FlashGraph / FlashX — https://github.com/flashxio/FlashX
70. Mosaic paper (EuroSys 2017) — https://dl.acm.org/doi/10.1145/3064176.3064191
71. GraphScope — https://github.com/alibaba/GraphScope
72. TigerGraph — https://www.tigergraph.com/
73. NodeSimilarity blocked at 130 GiB vs 24 GiB free ("motivating us to look elsewhere for scale") — https://community.neo4j.com/t/comparing-jaccard-similarity-neo4j-3-4-to-node-similarity-on-neo4j-3-5-and-gds-1-1-1/37205
74. AWS Neptune pricing (m-NCU memory-metered) — https://aws.amazon.com/neptune/pricing/
75. Neo4j Aura pricing — https://neo4j.com/pricing/

### External-pass re-sourcing (Section 10)

76. Aura free tier fails Neo4j's own GDS course — https://community.neo4j.com/t/using-gds-graph-project-on-auradb-free-tier/76520
77. `gds.graph.drop` does not release memory — https://community.neo4j.com/t/when-i-drop-the-memory-graph-my-memory-usage-does-not-change/67604
78. Louvain: 5 hours, >70 GB heap on a 60 GiB store — https://stackoverflow.com/questions/60050083/how-to-reduce-the-running-time-and-memory-utilization-of-the-louvain-algorithm-i

### Use-case evidence (Section 13)

79. Neo4j fraud-detection use case (WCC / community detection / centrality) — https://neo4j.com/use-cases/fraud-detection/
80. Neo4j GDS manual: WCC algorithm — https://neo4j.com/docs/graph-data-science/current/algorithms/wcc/
81. Traag, Waltman & van Eck, "From Louvain to Leiden" (Nature Scientific Reports, 2019) — https://www.nature.com/articles/s41598-019-41695-z
82. Edge et al., "From Local to Global: A Graph RAG Approach to Query-Focused Summarization" (arXiv 2404.16130) — https://arxiv.org/abs/2404.16130
83. Microsoft GraphRAG documentation — https://microsoft.github.io/graphrag/
84. Gupta et al., "WTF: The Who to Follow Service at Twitter" (WWW 2013) — https://web.stanford.edu/~rezab/papers/wtf_overview.pdf
85. Eksombatchai et al., "Pixie: A System for Recommending 3+ Billion Items to 200+ Million Users in Real-Time" (arXiv 1711.07601) — https://arxiv.org/abs/1711.07601
86. Wang et al., "Billion-scale Commodity Embedding for E-commerce Recommendation in Alibaba" (arXiv 1803.02349) — https://arxiv.org/abs/1803.02349
87. LinkedIn Engineering, "LIquid: The soul of a new graph database, Part 1" — https://engineering.linkedin.com/blog/2020/liquid-the-soul-of-a-new-graph-database-part-1
88. Chen et al., "Fast and Accurate Network Embeddings via Very Sparse Random Projection" (FastRP, arXiv 1908.11512) — https://arxiv.org/abs/1908.11512
89. Neo4j GDS manual: FastRP node embeddings — https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/fastrp/
90. Ying et al., "Graph Convolutional Neural Networks for Web-Scale Recommender Systems" (PinSage, arXiv 1806.01973) — https://arxiv.org/abs/1806.01973
91. Uber Engineering, "Food Discovery with Uber Eats: Using Graph Learning to Power Recommendations" — https://www.uber.com/blog/uber-eats-graph-learning/
92. Suri & Vassilvitskii, "Counting Triangles and the Curse of the Last Reducer" (WWW 2011) — https://theory.stanford.edu/~sergei/papers/www11-triangles.pdf

# Simulation 01 — Do Real Users Actually Feel This Pain?

Truth-seeking exercise: before believing our own pitch ("~20x less RAM
for WCC / Louvain / PageRank, bill known before the run"), go read what
real users say on forums — Neo4j Community, Hacker News, Stack Overflow —
and simulate honestly how they would react if we delivered it.

Method: searched HN (Algolia API), community.neo4j.com (Discourse API),
Stack Overflow (StackExchange API). Reddit's API blocked unauthenticated
search; noted as a gap. All quotes paraphrased-minimally; URLs given so
every claim is independently checkable.

---

## 1. Core facts going in (what we claim)

```
  claim                                    status
  ---------------------------------------  --------------------------
  WCC/Louvain/PageRank = ~50% of GDS use   modeled from GDS docs/talks
  ~10-25x less RAM to pay for              modeled, code-verified vs
                                           GDS source, NOT measured
  bill known before run (manifest math)    designed, not built
  2-5x slower wall-clock (streaming)       modeled from literature
```

The question this doc answers: **does anyone actually care?**

---

## 2. Evidence catalog (with URLs)

### 2.1 Direct, on-point pain — Neo4j Community Forum

These are the strongest signals: real users hitting the exact wall our
product removes, answered by Neo4j staff confirming the wall is
structural.

**E1. "How to efficiently cluster nodes using GDS with limited memory
(16 GB RAM)"** — user tries to project a graph for clustering, gets:
`Procedure was blocked since minimum estimated memory (52 GiB) exceeds
current free memory (5120 MiB)`.
https://community.neo4j.com/t/how-to-efficiently-cluster-nodes-using-gds-with-limited-memory-16-gb-ram/71073
- This is LITERALLY our demo scenario: 16 GB box, 52 GiB estimate,
  job refused. Our pitch: that job finishes, streaming, bill printed.

**E2. "What library to use instead GDS when graph db is too big to
project in memory?"** — user with hundreds of millions of nodes on a
32 GB VM. Neo4j (Alicia Frame, then-GDS PM) replies: *"We don't offer
any spill over / out of core computations right now"*, suggests
projecting a subset or `sudo: True` ("the risk is OOMing your
database").
https://community.neo4j.com/t/what-library-to-use-instead-gds-when-graph-db-is-too-big-to-project-in-memory/55821
- Vendor-confirmed: no spill exists; official workarounds are "use
  less data" or "risk crashing the database."

**E3. "GDS algorithms without a projection"** — user with billions of
nodes / hundreds of billions of edges OOMs at projection. Neo4j staff
(paul.horn, GDS engineer): *"GDS also needs to project all the data...
into a single in-memory projection, there is no option to spill to
disk."* User's reply: *"that surprises me as it defeats the purpose of
using a database if you need to map its data to an in-memory
representation to run an algorithm."*
https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039
- The user articulates our pitch FOR us, unprompted.

**E4. "What is the ideal heap memory size for GDS in Neo4j"** — 95 GB
RAM machine, 75 GB heap, ~320M nodes, still heap errors while
clustering.
https://community.neo4j.com/t/what-is-the-ideal-heap-memory-size-for-gds-in-neo4j/76311

**E5. "Memory Limit on graph projection"** — "Capacity exhausted" at
~300 GB during projection; user reduced concurrency, tinkered with
heap configs, nothing helped.
https://community.neo4j.com/t/memory-limit-on-graph-projection/61567

**E6. "How can I load a very large dataset with limited memory?"** —
6.6 TiB import on a 32 GB machine; import tool "suggests 203 GB."
https://community.neo4j.com/t/how-can-i-load-a-very-large-dataset-with-limited-memory/59189

**E7. "GDS ShortestPath memory consumption"** — memory of a projection
grows steadily under a 150k-paths/hour workload.
https://community.neo4j.com/t/gds-shortestpath-memory-consumption/58340

### 2.2 Pricing/RAM resentment — Hacker News

**E8.** *"When we looked to scale Neo4j, we almost had a heart attack
when seeing the price"* (startup, 2019). Same thread: *"Every time that
Neo4j is mentioned here, the pricing issues are raised."*
https://news.ycombinator.com/item?id=18797980

**E9.** *"They wanted to charge us something like 10% of our ARR"*
(2021, on pricing model built around one very large centralized graph).
https://news.ycombinator.com/item?id=27544889

**E10.** *"We evaluated umpteen graph dbs this past year and chose
vanilla Postgres instead because Neo4j/RedisGraph have insane
licenses"* (2020).
https://news.ycombinator.com/item?id=22485576

**E11.** *"If the data fits in memory (Neo4j, in the past at least, has
pretty much required that)..."* (2015 — the RAM-resident reputation is
a decade old).
https://news.ycombinator.com/item?id=8899483

**E12.** GraphRAG cost anxiety (2025): *"GraphRag preprocessing is
insanely expensive and precisely does not scale linearly with your
dataset"* and the explainer reply detailing entity extraction +
community detection over everything.
https://news.ycombinator.com/item?id=45063386 and
https://news.ycombinator.com/item?id=45068902

**E13.** *"...you're told to add Neo4j or a specialized GraphDB to your
stack... you have to deal with Neo4j and who wants that"* (2025, Show
HN for a GraphRAG-without-Neo4j tool — builders are routing AROUND
Neo4j).
https://news.ycombinator.com/item?id=46347143

### 2.3 Stack Overflow

Thinner signal (graph-analytics questions mostly go to the Neo4j forum
instead), but present:

**E14.** "How can I create graph Projections in Neo4J for a very large
graph"
https://stackoverflow.com/questions/79650281/how-can-i-create-graph-projections-in-neo4j-for-a-very-large-graph

**E15.** "Keep a projected graph in sync with persisted graph in Neo4j
GDS" — the staleness/refresh pain, organic.
https://stackoverflow.com/questions/73258583/keep-a-projected-graph-in-synch-with-persisted-graph-in-neo4j-gds

---

## 3. Truth-seeking: what the evidence does NOT say

Honesty section — the counter-signals matter as much as the signals.

1. **Nobody complains about WCC/Louvain/PageRank BY NAME.** The pain is
   almost always at the PROJECTION step (get the graph into memory at
   all), not the algorithm step. Users say "can't project," not "Louvain
   ate my tallies." Implication: our messaging should attack the
   projection wall ("no projection step — point us at the files"), with
   per-algorithm plans as supporting detail, not the headline.

2. **The complaint volume is moderate, not a flood.** Tens of
   high-quality forum threads over ~5 years, not thousands. Two honest
   readings: (a) most Neo4j users run small graphs that fit, and the
   big-graph users are a minority — the market is the minority; (b)
   survivorship bias — people who hit the wall silently leave for
   igraph/NetworkX/cuGraph or Postgres (E10, E13 show exactly this),
   so forums under-count the pain. Both are partially true.

3. **GraphRAG's "insanely expensive" is mostly LLM token cost, not
   Leiden RAM** (E12's explainer: entity extraction + narrative
   generation dominate). Leiden/community detection is a real but
   secondary cost there. Our GraphRAG story must be honest: we cut the
   graph-compute and re-index slice, not the LLM bill.

4. **The pricing rage (E8-E10) is about licenses and cluster pricing,
   not GB-hours specifically.** Aura Graph Analytics ($/GB-hour) is
   newer; there isn't yet a public corpus of "my Aura analytics bill"
   complaints. Our $-comparisons are extrapolated, not quoted from
   victims.

5. **Reddit unsearched** (API blocked). Gap, flagged.

6. **Neo4j's estimate-then-block behavior (E1) is arguably a FEATURE
   they already ship** — the job was refused, not OOM-killed. What they
   don't offer is the second half: "...and here's how it finishes
   anyway." Our differentiation is the FINISH, plus estimate-in-1-second
   -without-a-running-database.

---

## 4. Simulated reactions if we ship what we claim

Simulation: we publish "the 52-GiB-estimate job from E1 finishes on the
same 16 GB box in 38 minutes; bill printed before start; artifact
bundle reproducible." How do the observed populations react?

**P1. The forum users in E1-E5 (data engineers with too-big graphs):**
Reaction: immediate trial. They already asked for exactly this and were
told "no spill, use less data." Conversion blocker: their data lives
inside Neo4j — the export step (2-8 hrs, failure-prone) is OUR onboarding
cliff. Simulated quote: *"Worked on the CSV dump. Now can it read my
Neo4j store directly?"*

**P2. The HN skeptic:** *"You can do this with GraphChi in 2012 /
networkit / a 750-line Rust program. Out-of-core graph processing is a
solved research problem."* — TRUE, and the correct reply is: yes, the
mechanism is 13 years old; nobody productized it with a pre-run cost
receipt against the #1 graph vendor's workloads. The receipt, not the
streaming, is the news. Expect this comment in the first hour of any
HN launch; pre-empt it in the post itself.

**P3. The "just buy RAM" crowd:** *"512 GB servers are cheap now."* —
Partially true (a 512 GB bare-metal box is ~$200-400/mo rented). But
E1/E4 show real users on 16-95 GB boxes NOT buying bigger machines —
because procurement, cloud policy, or laptops. The segment that can't
just buy RAM exists and posts about it.

**P4. Neo4j's probable response (simulate the competitor):** short
term, point at their estimate mode + Aura autoscaling; medium term,
ship *some* disk-backed projection (they already have block format work
in the DB layer). What they will NOT do is make the bill-before-run +
finish-anyway promise central, because (a) Java heap + GC makes honest
byte-exact receipts hard, (b) per-GB-hour revenue disincentive. Our
moat holds only while we keep the receipt byte-honest.

**P5. GraphRAG builders (E12, E13):** they want FEWER moving parts, not
another database. Reaction to "a CLI that runs Leiden on your parquet
of edges with a fixed RAM cap": strong — it subtracts a component
(Neo4j) instead of adding one. This audience is acquirable without
touching the Neo4j export cliff at all, because their edges are already
in files. Possibly the true beachhead.

**P6. The silent majority (small graphs that fit):** no reaction. Our
product is irrelevant below ~10M edges, and that's most users. Fine —
they were never the market.

---

## 5. Verdict

```
  question                              answer
  ------------------------------------  --------------------------------
  Is the pain real?                     YES — vendor-confirmed structural
                                        wall (E2, E3), users asking for
                                        exactly our product (E1, E2, E3)
  Is it about our 3 algorithms          NO — it's about the PROJECTION
  by name?                              wall; algorithms are downstream.
                                        Lead with "no projection step."
  Is the market loud?                   MODERATE — quality over volume;
                                        silent churn to igraph/Postgres
                                        likely understates it
  Strongest wedge audience              (1) forum-class users with
                                        too-big-to-project graphs;
                                        (2) GraphRAG builders with edges
                                        already in files (no export cliff)
  Biggest adoption risk                 the Neo4j EXPORT step, and the
                                        "solved problem, see GraphChi"
                                        rebuttal (answer: the receipt is
                                        the product, not the streaming)
```

---

## 6. Second pass: the other four families (NodeSim, paths, FastRP, triangles)

The first pass covered only WCC / Louvain / PageRank. After Arch06
gained worked examples 4-7, this second search pass asked: do the
remaining families (NodeSimilarity ~12%, shortest paths ~10%, FastRP
~8%, triangles ~5%) generate their own pain signal?

Method: same APIs (community.neo4j.com Discourse search, HN Algolia,
StackExchange, GitHub issues on neo4j/graph-data-science).

### 6.1 New evidence

**E16. "GDS algorithms without a projection" (revisit of E3, richer
quote)** — billions of nodes, user asks to run centrality WITHOUT
projecting. Neo4j engineer paul.horn: *"GDS also needs to project all
the data... into a single in-memory projection, there is no option to
spill to disk."* User's reply is the money quote: *"that surprises me
as it defeats the purpose of using a database if you need to map its
data to an in-memory representation to run an algorithm."*
https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039
- A real user independently articulating our pitch's core absurdity.

**E17. Stack Overflow: "In Neo4j, is there anyway to create a graph
projection if your graph is too big to fit in memory?"** — the question
title IS the product spec.
https://stackoverflow.com/questions/69092539
- Again projection-level, not algorithm-level.

**E18. GitHub neo4j/graph-data-science #55 / #54:
"gds.alpha.shortestPath.stream: java.lang.OutOfMemoryError: Java heap
space"** — shortest path, the family whose scratch is tiny, still OOMs
users (result streaming + projection).
https://github.com/neo4j/graph-data-science/issues/55
- Confirms Arch06 example 5's frame: paths pain = paying for the whole
  graph to answer one question.

**E19. GitHub #139 / #132: personalized PageRank
"OutOfMemoryError: unable to create native thread" under normal use.**
https://github.com/neo4j/graph-data-science/issues/139
- OOMs even from thread machinery, not just data — JVM operational
  fragility is its own complaint class.

**E20. Forum: "GDS ShortestPath memory consumption"** — a ~500-node (!)
graph, 150k Dijkstra calls/hour, memory climbing until restart.
https://community.neo4j.com/t/gds-shortestpath-memory-consumption/58340
- Even trivially small graphs generate memory anxiety when the runtime
  is a JVM; our fixed-arena story speaks to this buyer too.

**E21. Forum: fastRP threads are about USAGE confusion (zero vectors,
type errors, applying to new samples), not RAM.**
https://community.neo4j.com/t/dgs-fastrp-write-returns-failed-to-invoke-procedure-gds-fastrp-write-caused-by-java-l/51294
- FastRP RAM pain shows up indirectly: users at the scale where the
  254 GB sizing bites have usually already hit the projection wall
  earlier and never reached fastRP.

### 6.2 What the second pass did NOT find (honesty)

```
  searched for                          found
  ------------------------------------  -----------------------------
  "nodeSimilarity out of memory"        ~nothing algorithm-specific;
                                        similarity pain appears as
                                        generic projection failures
  "triangle count memory" complaints    essentially zero
  fastRP RAM complaints                 zero (usage confusion only)
  HN threads naming these algorithms    zero relevant
```

### 6.3 Interpretation

1. **The funnel hypothesis:** users die at the PROJECTION step before
   they ever reach the algorithm whose scratch would have killed them.
   NodeSimilarity's whole-graph-copy scratch (Arch06 ex.4) generates
   few complaints because the population that would hit it already
   OOM'd at `gds.graph.project`. The wall hides behind the wall.
2. **Messaging consequence (reinforces first pass):** per-algorithm
   RAM numbers are OUR internal engineering truth; the USER-facing
   truth is one sentence — "no projection step." The seven bespoke
   plans are proof points, not headlines.
3. **Small-graph JVM anxiety (E20) is a real secondary segment:** even
   users whose graphs fit complain about creep/restarts. "Fixed arena,
   RSS never exceeds the receipt" resonates beyond the big-graph
   market.
4. **Where evidence is thin, claims must be too:** for triangles and
   fastRP the demand evidence is inferential (funnel-blocked), not
   observed. The docs should not claim "users are crying out for
   low-RAM fastRP" — they are not, visibly. The claim is: the same
   projection wall blocks them, and the same fix frees them.

---

## 7. Aggregate anecdotal log: everywhere Neo4j gets complained about

Third pass, widest aperture: not GDS, not our algorithms — EVERY
recurring complaint theme about Neo4j across Hacker News (2011-2026),
the community forum, Stack Overflow, and GitHub. Purpose: an honest
anecdotal map of where Neo4j fails its users, so we know which failures
our product actually addresses (and which it doesn't).

URL convention: HN comment ids link as
https://news.ycombinator.com/item?id=<id>

### 7.1 Memory / resource consumption (our home turf)

**A1.** *"I've had bad experience with Neo4J's memory consumption so
I'm wary... we actively chose to go against it because of past issues
with resource usage."* — HN, on the $325M Series F thread of all
places. https://news.ycombinator.com/item?id=27543721

**A2.** Ex-Neo4j insider on why they abandoned OS memory mapping:
*"50% driven by Java memory mapping having insane issues on Windows,
20% Java memory mapping having insane issues on every platform..."*
https://news.ycombinator.com/item?id=31509341
- Notable for us: mmap-hostility is a JVM problem, not an mmap
  problem. Rust doesn't inherit it.

**A3.** ID-space ceiling: *"we reached the limit of 32 billions of
unique IDs (Neo 3.2)... had to wait for the next version so we could
add more data."* https://news.ycombinator.com/item?id=33918730

(Plus the entire GDS catalog above: E1-E3, E16-E20 — projection OOM,
no spill to disk, "defeats the purpose of using a database.")

### 7.2 Performance / speed

**B1.** *"I still have nightmares with Cypher and neo4j's slowness."*
https://news.ycombinator.com/item?id=48647551

**B2.** *"right now neo4j is slower for graphs than postgres, just
with a nicer UI."* https://news.ycombinator.com/item?id=27544079

**B3.** *"Neo4j is dead slow."* (2013 — the complaint is over a decade
stable) https://news.ycombinator.com/item?id=6693003

**B4.** *"Neo4j was freaking slow when I tried to use it."* (2025)
https://news.ycombinator.com/item?id=48581477

**B5.** On vector search: *"the #2 committer of the project may
agree... Neo4j is slower than a 'normal vector db'."*
https://news.ycombinator.com/item?id=37871190

### 7.3 Reliability / operations

**C1.** The classic 2015 rant: *"the least reliable and most buggy
database solution I've ever worked with"* — six months of production
experience, on the ArangoDB benchmark thread.
https://news.ycombinator.com/item?id=9699964

**C2.** ConceptNet author (rspeer): *"I lost months of work to Neo4J
back in 2011... The write speed was awful, the stability was awful."*
https://news.ycombinator.com/item?id=9700558

**C3.** *"Neo4j was a terrible experience as a developer. It crashed
constantly, local dev required me to finagle around with Java SDK
versions... Their managed offering was equally as shitty."*
https://news.ycombinator.com/item?id=33916804
- Note the JVM-toolchain complaint: a single static Rust binary is
  itself a feature against this.

### 7.4 Pricing / licensing (the loudest theme by volume)

**D1.** *"Every time that Neo4J is mentioned here, the pricing issues
are raised. No exception today."* — user who hit the scaling wall on
the free version, then *"almost had a heart attack"* at enterprise
pricing. https://news.ycombinator.com/item?id=18797980

**D2.** *"Neo4j's entire pricing model, even in cloud, is built around
the idea that you'll have one centralized very large graph"* — doesn't
fit many-small-graphs shops with 3-5 pre-prod environments.
https://news.ycombinator.com/item?id=27544889

**D3.** Scaling is enterprise-gated: *"Neo4j is also very expensive if
you want to use it in a cluster."*
https://news.ycombinator.com/item?id=7804908

**D4.** License bait-and-switch resentment: *"they did a bait and
switch with the license model, I was not happy about that."*
https://news.ycombinator.com/item?id=33916759
- Context: the Neo4j v PureThink litigation over AGPL+Commons Clause
  ("open-washing") ran for years on HN's front page:
  https://news.ycombinator.com/item?id=30726286 ,
  https://news.ycombinator.com/item?id=34763955

**D5.** Sales-side confirmation: *"The company was totally inflexible
with their very outdated licensing model and it constantly lost them
potential customers."* https://news.ycombinator.com/item?id=33918651

### 7.5 Scaling architecture (the structural critique)

**F1.** *"always thought Neo4J was a joke, it was based on an
execution model which is not scalable at all... story after story from
people who tried it for projects which were just too big."* — "Ask HN:
What Is Going on with Neo4j?" (2022, layoffs thread).
https://news.ycombinator.com/item?id=33916259

**F2.** *"You can't shard it... you could only scale vertically, not
horizontally."* https://news.ycombinator.com/item?id=33919132

**F3.** The "Trillion Relationship Graph" marketing claim got its own
debunking thread: https://news.ycombinator.com/item?id=28707310

### 7.6 Churn stories (who left, and where they went)

**G1.** *"We're in the process of migrating off Neo4j/OngDB to
Postgres. Happy with how it's going so far."*
https://news.ycombinator.com/item?id=33916848

**G2.** ConceptNet moved off entirely (C2) — to purpose-built storage.

**G3.** Whole HN threads exist of people asking for and comparing
alternatives (FalkorDB, Memgraph, ArangoDB, Apache AGE, KuzuDB...):
https://news.ycombinator.com/item?id=43202780 ,
https://news.ycombinator.com/item?id=48358865

### 7.7 Honesty: the counter-log

Neo4j also has real defenders, and the log must show them:

- *"Had the exact opposite experience with N4j. Easy to operate, scale
  and run... large scale enterprise rollout"* —
  https://news.ycombinator.com/item?id=33917206
- GDS itself is praised: *"The built-in Graph Data Science package has
  a lot of nice graph algos that are easy to [use]"* —
  https://news.ycombinator.com/item?id=41269987
- Cypher's ergonomics and the browser UI are consistently liked even
  by critics (B2: "...just with a nicer UI").
- Several complaints (C1, C2) date to 2011-2015; the product has
  improved materially since. Age of evidence must be weighed.

### 7.8 What the aggregate log says

```
  complaint theme        volume   age span     do WE fix it?
  ---------------------  -------  -----------  -------------------------
  pricing / licensing    LOUDEST  2011-2026    indirectly (own-hardware,
                                               open engine = no meter)
  memory / RAM / OOM     high     2015-2026    YES — the core product
  slow (OLTP queries)    high     2013-2026    NO (we are OLAP-only;
                                               must say so loudly)
  reliability / JVM ops  medium   2011-2022    partly (static binary,
                                               fixed arena, no GC)
  can't scale out        medium   2015-2026    sidestepped (scale-UP via
                                               disk, not scale-OUT)
  vendor trust           medium   2018-2026    YES by being boring: open
                                               format, receipt, no meter
```

Two product lessons the wide log adds beyond the GDS-specific passes:

1. **Pricing/licensing resentment is the emotional carrier wave.** The
   RAM receipt lands harder because the audience already distrusts the
   meter. The pitch order should be: no meter -> no projection ->
   receipt -> algorithms.
2. **We must explicitly NOT claim to fix the top-volume complaint we
   don't address** (OLTP query slowness / Cypher performance). Being
   loudly OLAP-only converts a scope limit into credibility with an
   audience primed to smell overclaiming.

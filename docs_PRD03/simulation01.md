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

---

## 8. The badges (Shreyas Doshi lens): what we get to wear that they can't

Ranked by defensibility x evidence weight from sections 2-7.

```
  #  badge                        why it's ours          evidence base
  -- ---------------------------  ---------------------  ---------------
  1  "THE BILL BEFORE THE RUN"    insight-level moat:    E1, E5-E7;
     exact RAM + wall-clock from  Neo4j can't sell it    Louvain code
     1 KB of metadata, before a   without breaking the   comment "rough
     byte is read                 GB-hour meter; their   estimate";
                                  own estimator admits   Arch06 ex.7
                                  imprecision            (time receipt)
  2  "NO PROJECTION STEP"         vendor-confirmed       E2, E3, E16,
     the wall every evidence      structural: "no        E17; funnel
     pass converged on            option to spill to     hypothesis
                                  disk" (their eng.);    (sec. 6.3)
                                  "defeats the purpose
                                  of using a database"
                                  (their user)
  3  "FINISHES ON THE MACHINE     rides the loudest      110-254 GB
     YOU OWN"                     emotional theme        sessions ->
     the money badge              (meter resentment,     8-16 GB box;
                                  2011-2026) — but is    sec. 7.4;
                                  DERIVATIVE of #1+#2:   GraphChi
                                  streaming alone gets   rebuttal noted
                                  "solved in 2012";      in sec. 4
                                  the receipt makes it
                                  a product
  4  "LOUDLY OLAP-ONLY"           positioning by         sec. 7.8:
     a negative badge:            exclusion — refusing   OLTP slowness
     we do NOT fix their          to overclaim buys      is high-volume
     biggest complaint            credibility with an    and NOT ours
     (OLTP/Cypher slowness)       audience primed to
                                  smell overclaiming
  5  "BORING AND TRUSTWORTHY"     speaks to the vendor-  sec. 7.4 (D4,
     open format, static Rust     trust wound (license   D5), 7.3 (C3:
     binary, no GC, no license    litigation, bait-and-  JVM toolchain
     bait-and-switch              switch resentment) —   pain)
                                  real but table-stakes
                                  hygiene, not a moat
```

Pitch order the evidence supports:

```
  no meter  ->  no projection  ->  the receipt  ->  algorithms
  (why care)    (what's gone)      (the proof)      (the proof points)
```

Badge discipline: #1 is the only badge that is uncopyable for BUSINESS
reasons rather than technical ones — everything else a well-funded
competitor could ship in quarters. Product decisions should be scored
by whether they strengthen the receipt (byte-honest, cgroup-verified,
time-quoted) before anything else.

---

## 9. Competitive landscape: why is the low-RAM turf empty?

Wide sweep (GitHub API for stars/liveness, checked 2026-07) of every
notable graph engine, grouped by what it is FOR. Question asked of
each: does it occupy our turf — bounded-RAM, disk-streaming analytics
with a cost receipt?

### 9.1 The table

```
  tool (stars, last push)     primarily used for       on our turf?
  --------------------------  -----------------------  ------------------
  GRAPH DATABASES (OLTP-first, query languages, transactions)
  Neo4j                       property-graph OLTP +    NO — the incumbent
                              GDS in-heap analytics    whose wall we fix
  Dgraph (21.7k, active)      distributed OLTP,        NO — scale-OUT
                              GraphQL-native           answer, not low-RAM
  NebulaGraph (12.3k, act.)   distributed OLTP at      NO — same
                              billion-edge scale
  ArangoDB (14.2k, active)    multi-model (doc+graph)  NO
  JanusGraph (5.8k, active)   distributed OLTP over    NO — RAM-heavy,
                              Cassandra/HBase          ops-heavy
  Memgraph (4.2k, active)     IN-memory OLTP+streams   OPPOSITE turf —
                              (pitched for GraphRAG)   doubles down on RAM
  FalkorDB (4.7k, active)     sparse-matrix Cypher     NO — in-memory,
                              (GraphBLAS), RAG focus   latency-first
  Kuzu (4.0k, ARCHIVED)       embedded OLAP graph DB,  CLOSEST DB — but
                              columnar, out-of-core    company died Oct
                              joins                    2025 (Apple acqui-
                                                       hire); repo frozen
  Apache AGE (4.7k, active)   Cypher inside Postgres   NO — convenience,
                                                       not scale; algos
                                                       are basic
  DuckPGQ (0.4k, active)      SQL/PGQ graph queries    ADJACENT — DuckDB
                              in DuckDB                ethos = ours, but
                                                       pattern matching,
                                                       not iterative algos
  TuGraph (1.7k, active)      Ant Group's graph DB     NO

  ANALYTICS LIBRARIES (bring your own RAM, no storage story)
  igraph (2.0k, active)       C library w/ R/Python;   NO — in-RAM only;
                              academia's workhorse     dies where we start
  NetworKit (0.9k, active)    parallel in-RAM network  NO — same
                              science (C++/Python)
  NetworkX (huge, active)     pure-Python teaching/    NO — 10-100x slower
                              prototyping standard     than igraph even
                                                       in-RAM
  SNAP (2.3k, dormant)        Stanford's C++ library   NO — in-RAM
  GBBS/Ligra (academic)       shared-memory parallel   NO — assumes the
                              algorithm suites         graph fits
  cuGraph (2.2k, active)      GPU graph analytics      NO — needs GPU +
                              (RAPIDS)                 VRAM budget; the
                                                       vertical-scaling
                                                       answer on a card

  OUT-OF-CORE ENGINES (our technical ancestors)
  GraphChi (0.8k, DEAD 2019)  the OSDI'12 proof that   YES technically —
                              a laptop can do 1B+      but research code,
                              edges via disk           no product, no
                                                       receipt, JVM/C++
  X-Stream, GridGraph,        2013-2015 academic       YES technically —
  FlashGraph, Mosaic          out-of-core systems      all unmaintained
                                                       paper artifacts
  GraphScope (3.6k, active)   Alibaba's one-stop       NO — cluster-scale
                              distributed graph        answer (scale-OUT)
                              computing

  PAID PLATFORMS
  TigerGraph                  enterprise distributed   NO — scale-out MPP,
                              analytics               enterprise $$
  AWS Neptune (+Analytics)    managed OLTP + RAM-      NO — SAME meter:
                              provisioned analytics    m-NCU = memory-
                                                       metered billing
  Aura Graph Analytics        Neo4j's metered GDS      the thing itself
```

#### 9.1.1 Reference URLs (repo/product pages for every row above)

- Neo4j: https://github.com/neo4j/neo4j and GDS: https://github.com/neo4j/graph-data-science
- Dgraph: https://github.com/hypermodeinc/dgraph
- NebulaGraph: https://github.com/vesoft-inc/nebula
- ArangoDB: https://github.com/arangodb/arangodb
- JanusGraph: https://github.com/JanusGraph/janusgraph
- Memgraph: https://github.com/memgraph/memgraph
- FalkorDB: https://github.com/FalkorDB/FalkorDB
- Kuzu (archived): https://github.com/kuzudb/kuzu
- Apache AGE: https://github.com/apache/age
- DuckPGQ: https://github.com/cwida/duckpgq-extension
- TuGraph: https://github.com/TuGraph-family/tugraph-db
- igraph: https://github.com/igraph/igraph
- NetworKit: https://github.com/networkit/networkit
- NetworkX: https://github.com/networkx/networkx
- SNAP: https://github.com/snap-stanford/snap
- GBBS: https://github.com/ParAlg/gbbs ; Ligra: https://github.com/jshun/ligra
- cuGraph: https://github.com/rapidsai/cugraph
- GraphChi: https://github.com/GraphChi/graphchi-cpp
  (paper: https://www.usenix.org/system/files/conference/osdi12/osdi12-final-126.pdf)
- GridGraph paper: https://www.usenix.org/system/files/conference/atc15/atc15-paper-zhu.pdf
- X-Stream paper: https://dl.acm.org/doi/10.1145/2517349.2522740
- FlashGraph: https://github.com/flashxio/FlashX
- Mosaic paper: https://dl.acm.org/doi/10.1145/3064176.3064191
- GraphScope: https://github.com/alibaba/GraphScope
- TigerGraph: https://www.tigergraph.com/
- AWS Neptune Analytics (m-NCU pricing): https://aws.amazon.com/neptune/pricing/
- Neo4j Aura Graph Analytics pricing: https://neo4j.com/pricing/
- Kuzu shutdown/acqui-hire discussion: https://news.ycombinator.com/item?id=44383243

### 9.2 The Shreyas answer: WHY the turf is empty

Not because it's impossible — GraphChi proved the physics in 2012 and
then DIED. The turf is empty because every player faces a structural
reason not to stand on it:

```
  player class      why they won't build low-RAM + receipt
  ----------------  --------------------------------------------------
  incumbents        their REVENUE is the RAM meter (Aura GB-hours,
  (Neo4j, Neptune)  Neptune m-NCUs). certainty cannibalizes the meter.
  in-memory         their PITCH is latency; admitting disk is fine for
  challengers       analytics undermines their one differentiator.
  (Memgraph etc.)
  libraries         no storage layer at all — "bring your own RAM" is
  (igraph, cuGraph) the design, cost estimation is out of scope.
  academia          papers reward novel algorithms, not receipts,
  (GraphChi line)   packaging, or maintenance. code dies at tenure.
  scale-out camp    the 2015-2020 zeitgeist said the answer to big
  (Dgraph, Nebula,  graphs is MORE MACHINES. an entire generation of
  GraphScope)       funding went to horizontal, none to frugal.
  Kuzu (the one     validated the adjacent turf (embedded, columnar,
  that got close)   out-of-core JOINS) — then got acqui-hired before
                    reaching iterative-analytics-with-receipt.
```

The five-forces reading: the technique is public and 13 years old; the
GAP is a product gap (receipt, bounded arena, algorithm plans) plus a
business-model gap (nobody with distribution is INCENTIVIZED to sell
RAM-frugality). That second gap is the moat — same conclusion as
section 8, reached from the competitor side.

Differentiation one-liner per near-neighbor:
- vs igraph/NetworKit: "we start where they OOM; they have no disk story."
- vs cuGraph: "no GPU required; our budget is the receipt, not VRAM."
- vs Kuzu (RIP): "they proved embedded-OLAP demand; we add iterative
  algorithms + the receipt, in maintained form."
- vs DuckPGQ: "same ethos, different layer: they query patterns, we run
  iterative analytics. potential ALLY (export sink), not rival."
- vs GraphChi lineage: "they are our physics citation, not a rival:
  dead code, no estimation, no product."
- vs Neo4j/Neptune: "they cannot copy the receipt without breaking the
  meter."

---

## 10. Notes from external LLM deep-research pass (user-supplied)

A parallel deep-research report (independent LLM with web search,
2026-07) was reviewed against our catalog. Items below extend sections
2-7; its citation links were not exported with the text, so items are
marked [ext-unverified] until re-sourced — directionally consistent
with our verified base.

### 10.1 New pain anecdotes worth keeping

Re-sourcing pass (2026-07): items we could independently locate now
carry URLs and are VERIFIED; the rest stay [ext-unverified].

- [VERIFIED] Aura FREE-TIER user OOM'd on `gds.graph.project()` while
  following Neo4j's own Graph Academy GDS-fundamentals course:
  https://community.neo4j.com/t/using-gds-graph-project-on-auradb-free-tier/76520
  ("it is stated that it is possible to use AuraDB free tier for the
  course. However, I have found this not to be true.")
- [VERIFIED] NodeSimilarity blocked: "Procedure was blocked since
  minimum estimated memory (130 GiB) exceeds current free memory
  (24 GiB)" — on a graph they then shrank to 2,594 nodes and STILL
  hit a 54 GiB estimate; "motivating us to look elsewhere for scale":
  https://community.neo4j.com/t/comparing-jaccard-similarity-neo4j-3-4-to-node-similarity-on-neo4j-3-5-and-gds-1-1-1/37205
- [VERIFIED] `gds.graph.drop` does NOT return memory to the OS (JVM GC
  behavior) — user forced into stop/start restarts to reclaim RAM:
  https://community.neo4j.com/t/when-i-drop-the-memory-graph-my-memory-usage-does-not-change/67604
  New complaint class for us: our munmap actually releases.
- [VERIFIED, adjacent] Louvain run taking 5 hours and >70 GB heap on a
  60 GiB store (community 3.5, algo.louvain):
  https://stackoverflow.com/questions/60050083/how-to-reduce-the-running-time-and-memory-utilization-of-the-louvain-algorithm-i
- [ext-unverified] K8s sidecar with 120 GB provisioned still
  OOM-crashed at ~40 GB used — manual JVM threshold management fails
  even with headroom. (Could not locate the original post.)
- [ext-unverified] Delta-stepping OOM'd with 12 GB heap on a 63k-node
  graph. (Could not locate; nearest verified path-OOM evidence remains
  E18/E19, GDS GitHub issues #55/#54.)
- [ext-unverified] Louvain on Yelp dataset: DB corruption + ~23 GB
  consumption. (Could not locate; the SO post above is the closest
  verified analog.)
- [ext-unverified] Aura pricing quotes: "$70k a year isn't even nearly
  competitive"; Neptune claimed ~1/6th the price at similar
  provisioning (flagged by the report itself as workload-dependent).
- [ext-unverified] Workaround culture: projecting node IDs only and
  re-MATCHing attributes — trading the memory wall for an I/O latency
  wall.

### 10.2 GraphRAG cost-split confirmation

The report independently reaches our section 3 correction: LLM token
cost dominates GraphRAG, BUT the Leiden/community phase is the
CPU/RAM-bound slice, unstable on dense graphs, and re-runs from
scratch on every re-index (stochastic, seed-dependent — one cited team
proposed k-core decomposition just to get determinism). Two additions:
- Microsoft GraphRAG reportedly drops ~10% of entities because Leiden
  discards weakly-connected/isolated nodes — an ACCURACY complaint
  against the incumbent pipeline, useful for our exactness-flags story.
- "GraphRAG is 20-100x more expensive than vector RAG" — the indexing
  phase is the adoption blocker we relieve (the graph-compute slice).

### 10.3 Incumbent-response scenarios (adopted into our planning)

The report's Neo4j counter-move forecast, kept as planning input:
1. FUD via latency benchmarks conflating OLTP with batch analytics —
   pre-answered by our "loudly OLAP-only" badge (sec. 7.8/8).
2. "Serverless" pricing tiers that HIDE the RAM meter rather than
   remove it — obfuscation, not architecture; the receipt is the
   counter-story.
3. Apache Arrow off-ramp: position Neo4j as the storage hub, push
   compute to Polars/DuckDB/cuGraph — this validates the EXPORT
   sidecar as our critical wedge (sec. 5 journey risk).
4. If disruptive: acquisition attempt ("cold-storage analytics tier
   under GDS").

### 10.4 Segments (matches ours, one addition)

Report's top-3 segments = ours (GraphRAG builders; mid-market data
engineers with nightly ETL; academics/bioinformaticians). Addition
worth keeping: LOCAL AI agents on consumer hardware — an embedded,
16 GB-class buyer where Aura is disqualified outright.

### 10.5 Methodological flags (theirs and ours)

The report itself flags: the Neptune 1/6th-price and "custom Rust DB =
1000x" claims are workload-dependent folklore. We additionally flag:
the report's citation links were not exported, so a re-sourcing pass
(2026-07) was run: four 10.1 items are now independently VERIFIED with
URLs; the remainder stay [ext-unverified] and must not be used in
public material until sourced. The core memory-wall thesis, however,
is now triangulated three independent ways: our API sweeps (sec. 2, 6,
7), GDS source code (Arch06), and this external pass.

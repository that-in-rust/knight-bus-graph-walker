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

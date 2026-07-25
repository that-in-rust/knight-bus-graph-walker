# Everything-We-Know-Now — Consolidated State Of Knowledge

Date: 2026-07-26
Status: **the single document to read if you read only one.** Consolidates the
verified evidence, the competitive reality, the cost question, the strategy, and
— explicitly — everything still unknown.
Supersedes nothing; it compresses `evidence01.md`, `Real-Pain-Wrong-Product.md`,
`Not-First-Still-Different.md`, `Win-The-Whales-Vision.md` and corrects two claims
made in them.

Evidence base: 24 files of `docs_PRD04/`; a 110-agent research pass (27 sources,
25 claims 3-vote verified, 13 confirmed); a hand sweep of open APIs after web
search failed; a second cost-focused research run **stopped early at 73/81 agents**
because the marginal value had gone to zero. Facts are graded. Judgment is labelled.

---

## 0. The bottom line

```text
  THE WALL IS REAL AND VENDOR-DOCUMENTED.          <- proven, saturated, stop researching
  THE PRODUCT SPEC IS WRONG.                        <- prd-l1 builds a database; the
                                                       pain needs a component
  THE MOAT IS CONTESTED, NOT EMPTY.                 <- 4 live entrants, 2 of them
                                                       shipping our exact thesis
  COST IS A REAL PAIN -- BUT IT IS LICENSING-SHAPED,
    NOT RAM-SHAPED.                                 <- corrected 2026-07-26
  THE MARKET SIZE IS STILL UNMEASURED, AND THE ONLY
    FIRST-PARTY DATAPOINT CUTS AGAINST US.          <- the only question left
  ONE POSITION REMAINS UNCONTESTED: live inside a
    running Neo4j.                                  <- ~2 weeks to test
```

---

## 1. What is PROVEN (stop spending time here)

### 1.1 GDS is heap-only by design

Vendor docs, current channel (Manual v2026.06), accessed 2026-07-25:

```text
"The graph algorithms library operates completely on the heap, which means we'll
 need to configure our Neo4j Server with a much larger heap size than we would
 for transactional workloads."

"The amount of free memory can be increased by either dropping unused graphs from
 the catalog, or by increasing the maximum heap size prior to starting the Neo4j
 instance."

"we have sudo mode which allows you to manually skip heap control and run your
 procedure regardless."

"For purely analytical workloads including native projections, it is recommended
 to decrease the configured PageCache in favor of an increased heap size."

"Data received through Arrow is temporarily stored in direct memory before being
 converted and loaded into an on-heap graph."
```

The complete documented remedy set is **drop graphs / buy more RAM / bypass the
safety check.** There is no disk option. Neo4j actively tells analytics users to
shrink the page cache — the architectural opposite of out-of-core. The Arrow line
closes the "but Arrow ingest" objection: it lands on-heap regardless.

### 1.2 The absence is deliberate, and the same sentence undercuts our market

Neo4j staff engineer `paul.horn`, forum post #6, **2025-03-17 16:04 UTC**
(profile title "Neo4j Staff"):

```text
"Yes, there is certainly room for that, at the end, though, it's a prioritization
 issue. In practice, GDS is a product primarily sold to enterprises, and so far
 we've not had customers who weren't able to rent a big enough machine in the
 cloud (some even had 12TB ram) to cover the memory requirements. As a result,
 providing a *good* implementation of out-of-core GDS has never been prioritized
 enough, compared to the effort it takes to implement it. It's not like we've
 never thought about it, we just never had enough people ask about it to justify
 working on it."
```

Post #2, same thread, same day: *"GDS requires graph projections for its
algorithms, there is no option to run directly on the database."*
Earlier corroboration, GDS product lead Alicia Frame (2022): *"We don't offer any
spill over/out of core computations right now"* — with the workaround caveat
*"the risk is OOMing your database."*

> **This is the most important paragraph in the entire corpus, and it says two
> things at once.** It proves the wall is intentional and unlikely to close
> (good for us). It also says enterprise customers rent 12 TB machines and
> **"we just never had enough people ask"** (bad for us). Never quote one half.

### 1.3 Nothing has changed through 2026-07-08

Nine consecutive GDS releases — 2.23 (3 Nov 2025) through 2026.06.0 (8 Jul 2026,
current) — read in full, cross-checked against the GitHub releases API. Regex over
all nine bodies for `disk|off.heap|out.of.core|spill|mmap`: **zero hits.** The only
memory-adjacent item in the window (2.27, 6 Mar 2026): *"Minor improvements in
allocated memory by gds.nodeSimilarity.* under certain situations."*

**Scope caveat:** verified for *release notes only*. The blog channel was not swept
by the run — and §3.4 below is a concrete example of something real that lives
there and was missed.

### 1.4 The Aura meter, exactly

```text
"GB-hours usage is calculated by multiplying the number of hours a database is
 running (WHETHER ACTIVELY USED OR NOT) by the memory size in gigabytes."

Graph Analytics sessions: GB-MINUTES on DECLARED RAM.
  14 purchasable rungs: 2,4,8,16,24,32,48,64,96,128,192,256,384,512 GB
  (a 1GB SKU exists but is marked "not sellable")
  No autoscaling: grep resize|autoscal|elastic across session config -> 0 hits
  "The minimum billed duration of a session is ten minutes."
  worked example: 8 minutes at 4GB -> billed 10 x 4 = 40 GB-minutes
  512GB session = USD 34.30 for one second of work (256GB = USD 17.15)

Paused instances bill 20%, not 0%:
  512GB AuraDS Enterprise (aws) paused = 33.28 ACU/hr = ~USD 23,962 / 30-day month
  executing nothing.  ("1 ACU = 1 USD", printed on the console pricing page)
  Pausing auto-expires after 30 days, back to full rate.
```

**Scope discipline — the most likely downstream error:** AuraDB/AuraDS instances
are **GB-hours** and pausable at 20%. Graph Analytics sessions are **GB-minutes**,
**not pausable**, 10-minute floor. Never cross-cite.

**Time fuse:** catalogue v1.5 and v2.0 both take effect **2026-08-01**. v2.0
collapses Graph Analytics Serverless to three rows and drops the "not sellable"
note. The load-bearing economics survive verbatim. *Snapshot the JSON before then.*

### 1.5 The pain, in users' words

| Evidence | Detail |
| --- | --- |
| The canonical refusal | `Procedure was blocked since minimum estimated memory (52 GiB) exceeds current free memory (5120 MiB)` — a 16 GB box |
| NodeSimilarity, the worst case | Blocked at **130 GiB vs 24 GiB free**; user shrank the graph to **2,594 nodes** and *still* got a **54 GiB** estimate; *"motivating us to look elsewhere for scale"* |
| Big iron doesn't save you | 95 GB RAM / 75 GB heap, ~320M nodes — still heap errors. Separately: projection fails at ~300 GB, *"capacity exhausted"* |
| Memory never returns | *"When I drop the memory graph, my memory usage does not change"* — `gds.graph.drop` doesn't release to the OS; forced restarts |
| Wall-clock | Louvain: **5 hours, >70 GB heap** on a 60 GiB store |
| Vendor's own courseware | Aura free tier cannot run Neo4j's own GraphAcademy GDS course |
| Their own sizing guide (LDBC100) | PageRank 45.9–110 GB · Louvain 45.9–119 GB · **FastRP 212–254 GB** |
| Fresh demand voice (2026-07-21) | *"I've wanted to analyze all published scientific papers and authors and their citation relationships in Neo4j but hit resource limits"* |

---

## 2. The COST question — resolved as far as public evidence permits

This section exists because a claim in `Win-The-Whales-Vision.md` §0 was
challenged and **the challenge was correct.**

### 2.1 What I got wrong

I wrote that whales "do not want to reduce their bill," inferring it from
paul.horn's *"able to rent a big enough machine."* That sentence proves **ability**
to pay, not **willingness**. It also suffers survivorship bias (it describes
customers who stayed) and wrong-channel bias (enterprises complain to account
managers, not Discourse). And `simulation01.md` §7.8 already recorded
**pricing/licensing as the LOUDEST complaint theme, 2011–2026 — louder than
memory.** I wrote a strategy on top of a corpus whose loudest signal contradicted it.

### 2.2 What the cost evidence actually says

```text
"we evaluated umpteen graph dbs ... chose vanilla Postgres instead because
 Neo4j/RedisGraph have INSANE LICENSES"
"they did a BAIT AND SWITCH with the license model"
"The company was totally inflexible with their very OUTDATED LICENSING MODEL and
 it constantly lost them potential customers"        <- from the sales side
"They wanted to charge us something like 10% of our ARR"
"we almost had a HEART ATTACK when seeing the price"
"Neo4j is also very expensive if you want to use it IN A CLUSTER"
"Neo4j's entire pricing model, even in cloud, is built around the idea that
 you'll have ONE CENTRALIZED VERY LARGE GRAPH"       <- doesn't fit 3-5 environments
+ years of AGPL / Commons Clause / PureThink "open-washing" litigation on HN
```

PeerSpot (segments reviewers by company size), sampled 2026-07-26:

| Reviewer | Size | Verbatim |
| --- | --- | --- |
| Consultant, Cognitive Atlas · Aug 2024 | Small | *"The tool is not expensive."* |
| Dir. Digital Transformation, Innodigital · Feb 2024 | Mid-market | *"the Neo4j license fee is more expensive than a local solution"* |
| Principal SWE, Tech Services (501–1,000) · Apr 2016 | **Enterprise** | *"you need to contact them to establish pricing"* |

**The finding:** almost none of the anger is *"our RAM bill is too high."* Nearly
all of it is ***"your licensing model is predatory, opaque, or inflexible."***

### 2.3 The corrected claim

| # | Claim | Confidence |
| --- | --- | --- |
| 1 | Cost is a real, loud, durable pain — the loudest theme in fifteen years of record | **High** |
| 2 | It is **licensing-model-shaped**, not RAM-consumption-shaped | **Moderate-high** |
| 3 | For whales it is a **procurement grievance** (opacity, inflexibility, lock-in), not an existential budget threat | **Moderate** |
| 4 | Wanting a lower bill ≠ migrating databases to get one. Cost is a **justifier**, not a **trigger** | **Moderate** |
| 5 | A frugality product mostly **misses** a licensing-shaped pain | **Moderate-high** |

**We have zero observed enterprise invoices, zero procurement documents, and zero
conversations with a large Neo4j customer.** No Aura cost-complaint corpus exists —
Discourse search for `"aura credits exhausted"` returns **0 topics**; the closest
Aura billing thread (75748, 2025-10-22) is billing-data *confusion*, not bill shock.

### 2.4 The strategic payoff of being wrong

`simulation01.md` §8 rates badge #5 — *"BORING AND TRUSTWORTHY: open format,
static Rust binary, no GC, no license bait-and-switch"* — as **"table-stakes
hygiene, not a moat."**

Given that licensing resentment is the loudest and most durable theme in the
record, **that badge is underrated and should be promoted.** "Permissively
licensed, no meter, no seat count, no audit, no bait-and-switch" speaks to the
wound people actually describe. It is a weaker *technical* claim than low-RAM and
a stronger *emotional* one — and emotional carrier waves are what get pilots
approved.

---

## 3. The competitive reality

### 3.1 The turf is contested, not empty

| Project | Created | Stars | Licence | What it ships | Gap it leaves |
| --- | ---: | ---: | --- | --- | --- |
| **LadybugDB** (Kuzu, renamed) | 2025-10-07 | 1,475 | MIT | *"Ladybug does not materialize projected graphs in memory, and the corresponding data is scanned from disk on the fly."* K-Core, Louvain, PageRank, SCC, WCC | Spill is `COPY FROM`-scoped only; **no memory budget for algorithm state, no estimate/receipt** |
| **Grafeo** | 2026-01-26 | **707** | Apache-2.0 | Rust, *"from the ground up for speed and low memory use"*, *"Transparent spilling for out-of-core processing"*, mmap storage + LRU, Cypher/GQL/Gremlin/GraphQL/SPARQL/SQL-PGQ, full algorithm suite, **`boltr` = Bolt v5.x for Neo4j driver compatibility** | "Resource limits" are timeouts / property caps / HNSW bound — **not** admission control; no pre-run estimate |
| **Slater** | 2026-06-10 | 93 | Apache-2.0 | Rust. *"graphs that don't fit in memory ... over standard Bolt, so any neo4j driver just works"*, *"Resident memory is set by a cache budget you choose, NOT by the size of the graph"*, `slater-build` → *"immutable, content-hashed generation directory"*, Bolt 5.4/4.4/4.1, Elias-Fano, ~14 B/edge, PageRank/BFS/betweenness/WCC | Cache budget is an **LRU knob**, not an admission decision with a printed bill; requires offline compile + replaces Neo4j on the read path |
| **Onager** | — | — | MIT/Apache | DuckDB extension, 40+ algorithms as SQL table functions | Graph held **in-memory** in a registry *outside* DuckDB's buffer manager; issue #27: *"~1.3 seconds just to construct the graph in memory"* on 10M edges. Same wall, no JVM |
| **Memgraph** | — | — | — | On-disk *storage* mode | *"all the graph objects used in the transactions still need to be able to fit in the RAM, or Memgraph will throw an exception"* — storage ≠ execution. Thesis survives here |

### 3.2 Slater is this folder's architecture, shipped

| `docs_PRD04` design decision | Slater, in production |
| --- | --- |
| GRAIN immutable generation-sealed dirs (`Arch05` G5) | *"immutable, content-hashed generation directory"* |
| Content-addressed generations (`Arch-Summary` §5 AXIS 1) | content-addressed on-disk image |
| RAM = O(V) by construction (`Arch06` L2) | *"cache budget you choose, not the size of the graph"* |
| Elias-Fano warm stratum (`Arch05` G2) | Elias-Fano succinct structures |
| Degree-rank / superhub handling (`Arch05` G1) | *"heuristics to avoid superhubs"* |
| Zero client change, Bolt (`prd-l1`) | Bolt 5.4/4.4/4.1, `cypher-shell`, browsers unchanged |
| Snapshot compiler / Build Store | `slater-build`, offline Cypher-dump compiler |

**We were not first, and nobody piled on.** The idea is public since GraphChi
(OSDI 2012); `simulation01.md` §4 P2 already conceded *"This objection is TRUE"*
and §12.1 called the technique *"public and thirteen years old."* Two of three
rivals had public code **before this repo's first commit (2026-04-16)**. This is
convergent discovery.

### 3.3 The judgment error that produced the "empty turf" belief

`simulation01.md` §12 argued the turf stays empty because incumbents are *paid*
not to, benchmarks *score* against it, academia is *promoted* away, startups are
*funded* elsewhere. **Every clause is about institutions; the conclusion was about
everybody.** Two solo builders in Rust were subject to none of those incentives.

### 3.4 Neo4j is moving on the cost axis

Neo4j blog, **2025-09-03**, Dan McGrath (VP Product Management, Cloud) —
"InfiniGraph": *"Property sharding allows the graph itself to remain logically
whole"*, *"100TB+ horizontal scale"*, and *"scaling storage independently of memory
and compute ... avoid over-provisioning and reduce cloud costs."*

Scale-**out**, not single-node out-of-core — so the architecture leg survives. But
it retires the *"you can't shard Neo4j"* complaint and shows the incumbent working
the over-provisioning axis. **Found by hand; the 110-agent run fetched it and never
verified it.**

### 3.5 Estimation is not our turf either

`.estimate` ships on every production-tier GDS algorithm (2.21 Clique Counting,
2.23 Maximum Flow, 2.25 Min-Cost Max Flow all shipped with it). Aura ships
`gds.session.estimate` returning `estimatedMemory` **plus `recommendedSize`** —
*"the smallest available tier that covers the estimated memory."*

They estimate **and name the tier to buy.** Residual asymmetry, stated narrowly:
*"only algorithms in the production-ready tier are guaranteed to have an .estimate
mode"* — universal coverage is still claimable.

---

## 4. What is genuinely ours

| Ours | Where | Status elsewhere |
| --- | --- | --- |
| **Manifest as closed-form estimator** — memory cost as arithmetic over ~1 KB of metadata, before reading a graph byte | `Arch05` G3 | Unclaimed by all four. Slater has knobs; nobody *derives* a bill |
| **Reject-before-execute as a product surface** — admission control on algorithm state, printed receipt, deterministic refusal | `Arch01`-C, `Arch02` col. C | Grafeo has timeouts/caps. Not admission control |
| **`gds.*`-shaped coexistence inside a running Neo4j** | `gtm-POC-01` | All four **replace** Neo4j and pay the export cost |
| **Holistic RAM accounting** — heap + page cache + scratch + retained generations as one budget | `prd-l1`, `Arch-options` | Undocumented anywhere else |

Four features. Narrow. Copyable in a quarter by anyone with a working engine.
**Not a moat — a head start on a specific slice.**

---

## 5. The strategy that survives everything above

### 5.1 Kill the scope

`prd-l1.md` specifies Neo4j-shaped OLTP, WAL, locks, 575 procedures, Bolt, Cypher,
APOC. `simulation01.md` §7.8 simultaneously declares OLTP slowness **explicitly not
our problem** and being loudly OLAP-only a *credibility asset*. The PRD contains
the entire hard half of the problem that nobody asked for.

> **You are not rewriting Neo4j. You are building the spill-to-disk executor
> Neo4j's own staff engineer said, on the record, they never prioritized.**

### 5.2 The one uncontested position

```text
   LIVE INSIDE A RUNNING NEO4J.

     grain.wcc.stream  registered BESIDE  gds.wcc.stream  (namespaced, no conflict)
     one jar + one config line     no export, no migration, no new DB to trust
     parity provable by ONE DIFF   (canonicalize components by min member)
     delete the jar -> nothing changed

   Slater and Grafeo CANNOT follow us here.
   They ARE the replacement database. That is their entire architecture.
```

**And the trigger-moment argument, which is the one that decides it.** The pain
happens *inside a Neo4j session, at a prompt, data already loaded, one line from
giving up.* Ask what each option demands **in that moment**: rewrite Neo4j →
"migrate your database" (no). Export + sidecar → "run an 8-hour risky export"
(mostly no). Plugin → **type the next line.**

### 5.3 What to sell (corrected)

| Rung | Promise | Buyer | Trigger | Weeks |
| --- | --- | --- | --- | --- |
| **V0** | *"Same answer as GDS, verified by one `diff`."* | Data scientist | Curiosity | **2** |
| **V1** | *"Your analytics can no longer take down your database."* | Platform owner / SRE | The OOM page | Q1 |
| **V2** | *"Retire the 254 GB you keep for FastRP — in every environment."* | Capacity planner | Budget cycle | Q2 |
| **V3** | *"Every score names the generation that produced it; infeasible jobs refuse to start."* | Compliance / model risk | Audit finding | Q3 |
| **V4** | *"Your 5-hour Louvain re-runs in 20 minutes."* | Fraud/AML lead | Batch SLA | Y2 |

Plus, promoted after the cost correction: **licence honesty as a first-class
pitch** — permissive, no meter, no seat count, no audit, no bait-and-switch.

**Never lead with:** "less RAM" (commodity), "we estimate memory" (they do),
"cheaper than Aura" (weak trigger; whales budgeted it).

### 5.4 Refuse

```text
  REFUSE: OLTP, WAL, locks, transactions
  REFUSE: Bolt, Cypher, drivers, APOC, the 575-procedure surface
  REFUSE: greenfield bake-offs against Grafeo and Slater
  REFUSE: benchmarks whose unit is speed rather than retired capacity
  REFUSE: another architecture document
```

---

## 6. What to do in the next four weeks

```text
  DAY 1     Estimator dry run: gds.*.estimate for the seven families on a public
            50 GB-class graph, stock Neo4j. Zero code. Every document leans on
            this number and nobody has produced it.

  WEEK 1    grain.ping -- the JNI round trip. Java shim -> Rust cdylib -> mmap a
            snapshot -> stream 1,000 (nodeId, degree) pairs back. De-risks
            classloader + transport + ID mapping at once. This is the gate on
            the ONLY uncontested position we have.

  WEEK 2    Benchmark Slater + Grafeo + LadybugDB on a graph 5-10x RAM.
            Do they bound ALGORITHM STATE, or only avoid the projection?
            Arch02 R2 says Louvain/NodeSim die on state -- if they hold, the
            remaining gap is very small and "contribute rather than build"
            becomes the rational move. That sentence must be sayable out loud.

  WEEKS 2-4 Five interviews with platform owners at self-hosted 100GB+ shops:
            (1) "What do you spend on Neo4j, and who owns that number?"
            (2) "Has an analytics job ever affected production? What happened?"
            Q1 tests the cost thesis. Q2 tests the blast-radius thesis.
            Nobody in this project has ever spoken to one of these people.

  ALSO      Legal read on plugin licensing (see §7). It gates everything.
  ALSO      Snapshot the Neo4j rate-card JSON before 2026-08-01.
```

---

## 7. Open risks and unknowns — the honest register

| # | Unknown | Severity | How to close |
| --- | --- | --- | --- |
| U1 | **Plugin licensing.** Linking Neo4j's kernel API; Community Edition is GPLv3. APOC-as-Apache-plugin is precedent *(general knowledge, NOT from this session's sources — verify)*. Mitigation: standalone Apache-2.0 Rust binary + thin separately-licensed shim | **Blocking** | Counsel |
| U2 | **"Zero heap contact" is unmeasured.** Mechanically plausible — graph never crosses JNI, results cross as one direct ByteBuffer — but never demonstrated | **Blocking** | `grain.ping`, 1 week |
| U3 | **Do Slater/Grafeo/Ladybug bound algorithm state?** If yes, our gap is ~nothing | **Blocking** | Benchmark, 1 week |
| U4 | **Market size.** Zero interviews, zero invoices. The one first-party datapoint says *"never had enough people ask"* | **Existential** | 5 interviews |
| U5 | **Whether cost is trigger or justifier.** §2 is inference from public text by people who mostly aren't whales | High | Interview Q1 |
| U6 | `dbms.security.procedures.unrestricted=grain.*` + a native `.so` in `plugins/` is a hostile ask in a regulated shop | Medium | Prices the sales cycle, doesn't kill it |
| U7 | **Blog/roadmap/NODES channels never swept.** The release-note negative is solid; "Neo4j has announced nothing anywhere" is NOT established. §3.4 proves this channel yields real material | Medium | Sweep before any public claim |
| U8 | Reddit remains unreachable (`.json` blocked to curl). Needs an authenticated app | Low | — |
| U9 | Five of six quarantined `[ext-unverified]` claims remain UNFOUND and must not be used externally | Low | — |

---

## 8. Corrections made during this session

| # | Claim | Correction |
| --- | --- | --- |
| C1 | "Nobody ships memory estimation" | **False.** `.estimate` per algorithm + Aura `recommendedSize` |
| C2 | "The out-of-core turf is structurally empty" | **False.** Four live entrants; two predate this repo |
| C3 | "Nobody sells drop-in low-RAM Neo4j compatibility" (`PMF01` W4) | **False.** Slater ships Bolt 5.4/4.4/4.1; Grafeo ships `boltr` |
| C4 | "The moat is narrower than we wrote" (my own, same day) | Too generous. **Contested**, not narrower |
| C5 | "Whales don't want to reduce their bill" | **Overreach.** Cost is the loudest theme in the record. Corrected to licensing-shaped, procurement-grievance, justifier-not-trigger |
| C6 | GraphRAG drops ~10% of entities via Leiden (`simulation01` §10.2 f) | **Wrong mechanism.** It is a query-layer NaN filter bug (graphrag#2348), and 93.4% in the reporter's case |
| C7 | `simulation01` §7.5 F1/F2 ("can't shard / vertical only") | Retired — dated pre-InfiniGraph |

---

## 9. The pattern that must not repeat

```text
  Arch04 X1 "1 week"                        -> not run
  Arch05 X1 "week 1, paper only"            -> not run
  Arch06 L1.c "one week ... gates the
              biggest claim in this doc"    -> not run
  Arch02 X1' "1 week"                       -> not run
  PMF02  Y1  "1 week, do immediately"       -> not run
  PMF02  Y2  ten OOM interviews             -> not run
  PMF03  Z1  "week 1, ~0 cost"              -> not run
  gtm-POC grain.ping "one week"             -> not run

  Eight scheduled falsification tests. Zero executed. Six more documents instead.
```

And the arithmetic that should end the argument:

```text
  110 agents, 6.09M tokens, 1,952 tool calls, 97 min
    -> hardened the leg that was already strongest; MISSED both live competitors

  4 curl calls to HN Algolia + gh, ~20 min, ~$0
    -> found Slater (4 days old) and Grafeo (707 stars)
    -> i.e. found that the moat was gone

  Slater      2026-06-10 -> 2026-07-21 (~6 weeks): working engine, HN launch
  This repo   2026-04-16 -> 2026-07-26 (~14 weeks): 27 documents, 0 experiments
```

**The difference was never insight. On paper our architecture is the more
sophisticated one. The difference was that they were compiling.**

---

## 10. If one paragraph survives

The wall is real, vendor-documented, and deliberately unfixed — that part held up
under adversarial verification and is not in doubt. But the idea was never ours
(GraphChi, 2012), the turf emptied and refilled while this folder was being
written, memory estimation ships on both sides, and the cost anger everyone points
to is about **licensing, not gigabytes**. What remains is one genuinely uncontested
position — a deletable jar inside a running Neo4j, where the pain actually happens,
that asks the customer for nothing — plus four narrow features nobody else has
built. That is enough to be worth two weeks of code and five phone calls. It is not
enough to be worth another document.

---

## 11. Cross-references

```text
evidence01.md ................. every fact above, URL-cited, confidence-graded,
                               plus §5.1 = the tooling that actually works
Real-Pain-Wrong-Product.md .... full verdict; §3.2b = the competitor discovery
Not-First-Still-Different.md .. priority/originality; timeline; what is ours
Win-The-Whales-Vision.md ...... enterprise strategy; NOTE: §0 and §7-V7 are
                               superseded by §2 of THIS file
gtm-POC-01.md ................. the plan. Already correct. Execute it.
Arch05.md G3 .................. manifest-as-estimator -- the one idea that is ours
Arch06.md L2 .................. RAM = O(V) by construction -- the real engineering
Arch02.md R2 .................. Louvain/NodeSim die on STATE -- why §6 week 2 matters
prd-l1.md ..................... scope must be rewritten; downstream re-prices free
```

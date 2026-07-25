# evidence01 — Vendor-Primary Evidence Dossier (URL-cited)

Date: 2026-07-25
Method: 110-agent deep-research fan-out (6 angles, 27 sources fetched, 134 claims
extracted, 25 adversarially verified by 3-vote panels → 13 confirmed, 12 killed),
plus 3 direct verifications by hand afterwards.
Companion to: `simulation01.md` (the practitioner-anecdote corpus, ~92 refs).

**This document deliberately does NOT repeat simulation01.** It was scoped to that
dossier's own declared gaps. What it adds is a *documentary spine*: the RAM-metering
and no-spill-to-disk legs of the thesis are now nailed to Neo4j's own live rate card,
billing docs, product docs, and nine consecutive release notes — read directly and
quoted verbatim.

---

## 0. Verdict first: what actually changed

```text
CONFIRMED AND HARDENED
  The architectural leg of the thesis is now vendor-documented, not inferred.
  GDS is heap-only BY DESIGN, the vendor's own remedy list contains no disk
  option, and a Neo4j staff engineer stated on the record (17 Mar 2025) that
  out-of-core GDS is absent by DELIBERATE DEPRIORITIZATION.

THREE THINGS WE MUST STOP CLAIMING
  1. "Nobody ships memory estimation" — FALSE. It ships in GDS per-algorithm
     AND in Aura as gds.session.estimate returning a recommendedSize.
  2. "The out-of-core graph-analytics turf is empty" — FALSE, NOT MERELY
     NARROWED. Four entrants, all live, all in the last nine months:
       LadybugDB  (Kuzu renamed, MIT, 1,475*) — no in-memory projection
       Grafeo     (Rust, Apache-2.0, 707*)    — "low memory use",
                                                "transparent spilling for
                                                out-of-core processing",
                                                + boltr = Bolt v5 for Neo4j
                                                  driver compatibility
       Slater     (Rust, Apache-2.0, 93*)     — "graphs that don't fit in
                                                memory ... over standard Bolt";
                                                RAM set by a cache budget,
                                                NOT by graph size
       Onager     (DuckDB ext, MIT)           — 40+ algorithms as SQL
  3. "Nobody sells drop-in low-RAM Neo4j compatibility" (PMF01 W4) — FALSE.
     Slater and Grafeo BOTH ship Bolt wire compatibility in Rust.

ONE THING THAT CUTS BOTH WAYS
  The same staff post that confirms the wall also says enterprise customers
  rent 12TB machines and "we just never had enough people ask about it."
  That is the strongest evidence yet that the low-RAM segment is SMALL among
  the people who pay Neo4j.

WHAT WE STILL HAVE NO EVIDENCE FOR
  Real dollar pain. Market size. Practitioner voice. This pass produced
  ZERO of it, because web search was broken throughout (see §5).
  All cost claims remain list-price arithmetic.
```

---

## 1. The memory wall — now vendor-documented

### 1.1 GDS is heap-only by design, and the documented escape hatches are "buy RAM" or "gamble"

**VERIFIED** · vendor technical docs, "current" channel = Manual v2026.06 · accessed 2026-07-25

| source | URL |
| --- | --- |
| GDS Memory Estimation | https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/ |
| GDS System Requirements | https://neo4j.com/docs/graph-data-science/current/installation/System-requirements/ |
| GDS Management Ops | https://neo4j.com/docs/graph-data-science/current/management-ops/ |

Verbatim:

```text
"The graph algorithms library operates completely on the heap, which means
 we'll need to configure our Neo4j Server with a much larger heap size than
 we would for transactional workloads."

"The amount of free memory can be increased by either dropping unused graphs
 from the catalog, or by increasing the maximum heap size prior to starting
 the Neo4j instance."

"we have sudo mode which allows you to manually skip heap control and run
 your procedure regardless."

"For purely analytical workloads including native projections, it is
 recommended to decrease the configured PageCache in favor of an increased
 heap size."

"Data received through Arrow is temporarily stored in direct memory before
 being converted and loaded into an on-heap graph."
```

Why it matters: the wall is **not** a tuning accident. The vendor's complete
enumerated remedy set is *drop graphs / raise heap / bypass the safety check*.
And Neo4j actively tells analytics users to **shrink the disk page cache in
favour of heap** — the architectural opposite of out-of-core. The `sudo mode`
line is the single strongest quote available for a bounded-RAM pitch. The Arrow
line closes the obvious "but Arrow ingest" objection: it lands on-heap anyway.

### 1.2 The vendor admission: out-of-core is absent by choice — and the same post says the segment is small

**VERIFIED** · Neo4j staff statement on Neo4j's own forum · post #6, 2025-03-17 16:04:04 UTC ·
author `paul.horn`, profile title "Neo4j Staff", primary group `Neo4j_Staff`
https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039

Full post, verbatim:

```text
"Yes, there is certainly room for that, at the end, though, it's a
 prioritization issue. In practice, GDS is a product primarily sold to
 enterprises, and so far we've not had customers who weren't able to rent a
 big enough machine in the cloud (some even had 12TB ram) to cover the memory
 requirements. As a result, providing a *good* implementation of out-of-core
 GDS has never been prioritized enough, compared to the effort it takes to
 implement it. It's not like we've never thought about it, we just never had
 enough people ask about it to justify working on it."
```

Post #2, same thread, same day 08:43 UTC:

```text
"GDS requires graph projections for its algorithms, there is no option to run
 directly on the database."
```

This is first-party, dated, and against commercial interest. It is materially
newer than the Alicia Frame (May 2022) and older paul.horn quotes already in
`simulation01.md`.

> **DO NOT QUOTE THE FIRST HALF WITHOUT THE SECOND.**
> `"some even had 12TB ram"` and `"we just never had enough people ask about it"`
> are the best evidence in this entire corpus that the bounded-RAM segment is
> **small among Neo4j's paying buyers**. This single post simultaneously
> validates the architecture thesis and attacks the market thesis. Any deck
> that uses one half and hides the other is lying by selection.

**Quarantined from this same thread** (failed 3-vote verification, do not use):
that Neo4j shipped and abandoned a disk-resident execution mode ~5 years ago
because it was ~100x slower. If true it is both a precedent and a performance
bar we must beat — worth re-sourcing properly, currently unusable.

### 1.3 Post-2025 negative check: nine releases, zero disk features

**VERIFIED** · all nine full release-note bodies fetched and read (not index teasers),
cross-checked against the GitHub releases API for completeness · accessed 2026-07-25
https://neo4j.com/release-notes/gds/

```text
2.23        3 Nov 2025      2026.03.0    8 Apr 2026
2.24       20 Dec 2025      2026.04.0   29 Apr 2026
2.25       20 Dec 2025      2026.05.0    1 Jun 2026
2.26        5 Feb 2026      2026.06.0    8 Jul 2026   <- current
2.27        6 Mar 2026
```

Regex over all nine bodies for `disk|off.heap|out.of.core|spill|mmap` → **zero hits.**
The only memory-adjacent item in the entire window (2.27, 6 Mar 2026, Improvements):

```text
"Minor improvements in allocated memory by gds.nodeSimilarity.* under certain
 situations."
```

Non-refutations worth noting: 2026.05.0's "Support for vector-type properties in
graph projections" *increases* in-heap footprint rather than relieving it.

> **SCOPE CAVEAT, IMPORTANT.** This is verified for **GDS release notes only.**
> Blog, roadmap, NODES/GraphConnect talks, Aura release channels and job postings
> were NOT swept. "Neo4j has announced nothing anywhere" is **NOT** established —
> and §3.5 below is a concrete example of something real that lives in the blog
> channel and that this sweep missed.

---

## 2. Pricing and billing mechanics — exact, from the rate card

### 2.1 Aura meters provisioned capacity × wall-clock, "whether actively used or not"

**VERIFIED** · vendor billing docs · accessed 2026-07-25
https://neo4j.com/docs/aura/billing/cost-explorer/ · https://neo4j.com/docs/aura/billing/billing-dimensions/

```text
"GB-hours usage is calculated by multiplying the number of hours a database is
 running (whether actively used or not) by the memory size in gigabytes (GB)."

"You specify the allocated memory size when the session is launched."

"The billing dimension for Graph Analytics sessions is compute and it is
 measured in GB-minutes. The duration of a session is calculated in minutes
 and multiplied by the RAM capacity used by the session."

"Compute is billed both in a running state and a paused state."
```

Marketing corroborates against interest — https://neo4j.com/pricing/ calls it
`"Capacity-based consumption pricing"`. The parenthetical *"(whether actively
used or not)"* is **Neo4j's own phrasing**, not our inference.

### 2.2 The over-provisioning penalty, in dollars

**VERIFIED** · vendor doc + machine-readable rate-card API (unauthenticated, 584 rows) ·
catalogue v1.4, validFrom 2026-03-01 · accessed 2026-07-25
https://neo4j.com/docs/aura/graph-analytics/aga/ · https://console.neo4j.io/api/product-catalogue/v1/versions/1.4/products

```text
"When creating a session (either explicitly or via a remote graph projection),
 you must set the memory parameter to specify the amount of memory allocated to
 the session. The supported values are 2GB, 4GB, 8GB, 16GB, 24GB, 32GB, 48GB,
 64GB, 96GB, 128GB, 192GB, 256GB, 384GB, and 512GB."

"The minimum billed duration of a session is ten minutes."
  worked example: "if the session ran for 8 minutes and had a 4GB size, you are
  billed for 10 minutes x 4GB = 40 GB-minutes."
```

From the rate card itself:

```text
{"consumptionSKU":"aws.gds_session.1gb","metricUnit":"GB-minutes",
 "baseUnitPrice":"0.0067","description":"not sellable"}

512GB session: 3.4304 ACU/min  -> 10-minute floor = USD 34.30
256GB session: 1.7152 ACU/min  -> 10-minute floor = USD 17.15
```

14 purchasable rungs (the 1GB tier is marked *not sellable*, so the floor is 2GB).
Grep for `resize|autoscal|elastic` across all session-configuration pages: **zero hits.**
There is no auto-sizing. **You pay for the rung you guessed, on a 10-minute floor.**

That sentence is the precise economic inefficiency a bounded-RAM engine removes.

### 2.3 "Billed while paused" is documented policy, not user confusion

**VERIFIED** · rate card + docs + vendor support KB (created 2024-03-15, modified 2025-09-12)

```text
running:  {"serviceName":"AuraDS","tierName":"Enterprise","cloudProviderName":"aws",
           "incrementValue":512,"incrementUnitPrice":"166.4"}
paused:   {"serviceName":"AuraDS paused",...,"incrementUnitPrice":"33.28"}

ratio exactly 0.200 (0.065/0.325 AuraDS; 0.025/0.125 GDS); paused SKUs on aws/azure/gcp
"1 ACU (Aura Consumption Unit) = 1 USD"  [printed on the console pricing page]

=> paused 512GB AuraDS Enterprise = 33.28 ACU/hr = USD 798.72/day
                                  ~ USD 23,962 per 30-day month, executing NOTHING
```

Docs: `"If a database is paused, its charge is reduced to 20% of the standard
hourly rate."` Support KB: `"A paused Aura Instance incurs only 20% of the cost
of a running one."` And pausing auto-expires: `"You can pause an instance for up
to 30 days, after which point Aura automatically resumes the instance."`

> **SCOPE DISCIPLINE — the most likely downstream error.**
> `AuraDB/AuraDS instances` = **GB-hours**, pausable at 20%.
> `Aura Graph Analytics sessions` = **GB-minutes**, **cannot be paused at all**,
> 10-minute floor. **Never cross-cite these.** Do not present the paused-billing
> figure as evidence about analytics sessions.

---

## 3. COUNTER-EVIDENCE (read this section first, not last)

### 3.1 Memory estimation is NOT unoccupied turf — it already ships, on both sides

**VERIFIED** · GDS release notes + GDS manual + Aura docs · accessed 2026-07-25

Every production-tier algorithm has a paired `.estimate` mode, and new algorithms
ship with it as a matter of course — 2.25 (20 Dec 2025) Min-Cost Maximum Flow:
`gds.maxFlow.minCost.stream.estimate` plus mutate/write/stats variants; 2.23
(3 Nov 2025) Maximum Flow; 2.21 (28 Aug 2025) Clique Counting. Aura-side:

```text
"In order to help determine a good session size for a given workload, there is
 the estimate() function."

gds.session.estimate -> yields "estimatedMemory" (e.g. "1.6GB") plus
 "recommendedSize" = "the smallest available tier that covers the estimated memory"
```

> **This kills "the receipt is the product" as a standalone claim.** Neo4j not only
> estimates, it estimates *and tells you which RAM tier to buy*. The residual
> differentiator is narrow and must be stated narrowly: their estimate's only
> actionable output is **which rung of the ladder to purchase**; ours must be
> **a hard budget the engine respects by spilling**. Sell the *enforced ceiling
> and the finish*, never the number.
>
> The one surviving asymmetry: `"only algorithms in the production-ready tier are
> guaranteed to have an .estimate mode"` — universal coverage is still ours to claim.

### 3.2 LadybugDB — Kuzu did not die, it was renamed, and it ships out-of-core algorithms

**VERIFIED** (failed 3-vote 1-2 upstream; re-fetched and confirmed by hand 2026-07-25,
HTTP 200, 102,733 bytes) · https://docs.ladybugdb.com/extensions/algo/ · https://github.com/LadybugDB/ladybug

```text
"A projected graph is evaluated only when an algorithm is executed. Ladybug does
 not materialize projected graphs in memory, and the corresponding data is
 scanned from disk on the fly."

"The algo extension allows you to run common graph algorithms such as PageRank,
 Connected Components, and Louvain on the graph stored in Ladybug."

algorithms: K-Core Decomposition / Louvain / PageRank / SCC / WCC

README: "The database was formerly known as Kuzu
         (https://github.com/kuzudb/kuzu)."

GitHub API: created 2025-10-07, pushed 2026-07-25 (today), 1,475 stars, MIT, not a fork
```

**This is the single most thesis-threatening fact in the corpus.** It directly
overturns `simulation01.md` §11.1, whose central competitive comfort was that
Kuzu's `InMemGraph` carried our wall inside it. The successor project appears to
have **fixed exactly that**, is permissively licensed, embedded, and active today.
It also answers "where did the Kuzu orphans go": nowhere — the project was renamed.

**Honest limit on the threat (also verified).** The only documented spill knob is
ingest-scoped: `"SPILL_TO_DISK spill data to disk if there is not enough memory
when running COPY FROM"`. There is **no** documented spill for *algorithm state*,
**no** memory-budget parameter for `algo` runs, and **no** pre-run estimate
anywhere in the algo docs. So Ladybug removes the *projection materialization*
wall but publishes no bounded-memory guarantee for algorithm working state — which
is precisely where Louvain and NodeSimilarity actually die (per `Arch02.md` R2).

Residual differentiation, stated honestly: an **enforced** memory ceiling, a
pre-run receipt, and Neo4j/GDS API compatibility. Not "we're the only ones off-heap."

### 3.3 Memgraph — storage beyond RAM ≠ execution beyond RAM (thesis survives here)

**VERIFIED** · vendor docs, rendered + docs-repo source; release notes current to v3.12.0 (15 Jul 2026)
https://memgraph.com/docs/fundamentals/storage-memory-usage

```text
"Keep in mind that while executing queries, all the graph objects used in the
 transactions still need to be able to fit in the RAM, or Memgraph will throw
 an exception."
"A single transaction must fit into the memory"
"The on-disk transactional storage mode is still in the experimental phase"
"We recommend our users use in-memory storage modes whenever possible."
```

And recent releases move toward *footprint compression*, not out-of-core:
v3.12.0 `--storage-light-edge=true` saving `"roughly 24B of memory overhead per
relationship"`; v3.9.0 `--storage-floating-point-resolution-bits`.

The technical half is documented fact. *"Therefore Memgraph doesn't occupy the turf"*
is **inference** and must be labelled as such. A third-party sweep for anyone
claiming Memgraph does out-of-core execution was not performed.

### 3.4 Onager (DuckDB extension) — competes on breadth and price, not on the wall

**VERIFIED** · repo shallow-cloned at HEAD `49ad15b5` (2026-07-22), docs + Rust source read
https://github.com/CogitatorTech/onager

40+ graph algorithms as SQL table functions, MIT/Apache-2.0, DuckDB community
extension. No out-of-core, spill, larger-than-memory or estimation claim anywhere
in its docs or 11-section roadmap. Its compute layer is Graphina, wrapping
`petgraph::StableGraph`, held in a process-global registry **outside DuckDB's
spillable buffer manager**:

```rust
pub struct UndirectedGraphWrapper { graph: Graph<i64, f64>, node_mapping: HashMap<i64, NodeId> }
static GRAPH_REGISTRY: Lazy<Arc<RwLock<HashMap<String, GraphType>>>>
```

Practitioner corroboration, open issue #27 (2026-07-16): `"on a 10M edge dataset,
it takes ~1.3 seconds just to construct the graph in memory."` Same architectural
wall as a GDS heap projection, minus the JVM.

Erodes the "no free GDS-algorithm substitute" claim. Does **not** erode the
bounded-RAM claim. *(The upstream claim that Onager has meaningful uptake —
852 weekly downloads, 149 stars — FAILED verification 1-2 and must not be cited.)*

### 3.5 InfiniGraph — found by hand, missed by the sweep, partially threatening

**VERIFIED BY HAND** (fetched by the workflow but never verified; surfaced by
reading the source list. `neo4j.com` 403s WebFetch — retrieved via curl with a
browser UA) · Neo4j blog, **2025-09-03**, Dan McGrath, VP Product Management, Cloud
https://neo4j.com/blog/graph-database/infinigraph-scalable-architecture/

```text
"Infinigraph architecture solves this challenge by distributing a graph's
 property data across the servers in a cluster. Property sharding allows the
 graph itself to remain logically whole; queries behave as expected, and
 applications scale without code changes or manual workarounds."

"100TB+ horizontal scale with zero application rewrites"
"It enables organizations to run both analytical and transactional workloads in
 the same system"

"Adjustable storage — Lower your infrastructure costs and simplify capacity
 planning by scaling storage independently of memory and compute within the Aura
 console and API. You'll be able to avoid over-provisioning and reduce cloud
 costs, which is especially valuable for AI training, analytics, and
 high-volume log data."
```

Assessment, carefully:

| what it does | effect on our thesis |
| --- | --- |
| Property sharding across a **cluster** — scale-OUT | Does **not** deliver single-node out-of-core GDS. Architecture leg survives. |
| Kills "you can't shard Neo4j / vertical scaling only" | **Retires** `simulation01.md` §7.5 F1/F2 as live complaints. Stop using them. |
| "scaling storage independently of memory and compute", "avoid over-provisioning and reduce cloud costs" | Neo4j is **actively working the over-provisioning cost axis.** This is the direction of travel we assumed they were structurally blocked from taking. |
| Unifies transactional + analytical "in the same system" | Unclear whether GDS *algorithms* run sharded. **Open question, high stakes.** |

Note the date: **September 2025 — before** the release-note window swept in §1.3,
and in the blog channel that sweep explicitly did not cover. This is direct proof
that the §1.3 negative, while solid, is narrower than it looks.

### 3.6 Slater — the v003 thesis, shipped, announced four days ago

**VERIFIED** · found via HN Algolia API + `gh api` (channels the workflow could not
reach) · https://github.com/Hikari-Systems/slater · Apache-2.0 · Rust ·
created **2026-06-10** · pushed 2026-07-23 · 93 stars ·
announced on HN **2026-07-21**, 43 points: *"Slater – Low-memory graphdb designed
for read-heavy graphs"* — https://news.ycombinator.com/item?id=48996325

README, verbatim:

```text
"Slater serves graphs that don't fit in memory -- hundreds of millions of nodes
 and billions of edges in low hundreds of MB of RAM -- over standard Bolt, so any
 neo4j driver just works ... Resident memory is set by a cache budget you choose,
 NOT by the size of the graph."

"The most common complaint about graph databases is that they don't scale past
 what you can hold in RAM. Many of them (eg neo4j, Memgraph, FalkorDB, etc) keep
 the whole graph resident: a 40 GB graph wants 40 GB of memory -- per instance."

"the 90 million node / 1.5B-edge Wikidata graph needs ~64-128 GiB resident, so
 the in-memory engines can't open it at all."

"A 4 GB graph and a 400 GB graph cost the same RAM to serve"

"You compile the graph once, offline, into a content-addressed on-disk image with
 slater-build; then any number of Slater servers serve it over Bolt"

"Bounded, predictable memory | Resident memory is capped by three cache budgets
 *you* set -- it does not grow with graph size"

"Speaks Bolt 5.4 / 4.4 / 4.1 -- use the standard neo4j drivers (JS, Python, Go,
 Java...), cypher-shell, or graph browsers unchanged."

"graph algorithms (PageRank, BFS, betweenness, WCC...) -- bounded memory"

"slater-build | The offline compiler: turns a primitive-Cypher dump into an
 immutable, content-hashed generation directory."
```

Author, in the HN thread (2026-07-22), on encoding and density:

```text
"So you're also using succinct datastructures with Elias-Fano" [commenter j-pb]
"...in bytes/edge, that's probably about 14."                  [author rickkjp]
"it will try to pack neighbours into the same blocks ... with some additional
 heuristics to avoid superhubs causing over[flow]"             [author rickkjp]
```

> **Read this against our own corpus and the overlap is near-total:**
>
> | `docs_PRD04` design decision | Slater, shipped |
> | --- | --- |
> | GRAIN: immutable **generation-sealed** snapshot dirs (`Arch05` G5) | "immutable, **content-hashed generation directory**" |
> | Arch-Summary §5 AXIS 1: content-addressed generations | content-addressed on-disk image |
> | `Arch06` L2: RAM = O(V) by construction | "cache budget you choose, not the size of the graph" |
> | `Arch05` G2: Elias-Fano warm stratum | Elias-Fano succinct structures |
> | `Arch05` G1: degree-rank / superhub handling | "heuristics to avoid superhubs" |
> | `prd-l1`: zero client-code change, Bolt-compatible | Bolt 5.4/4.4/4.1, cypher-shell, browsers unchanged |
> | Seven families | PageRank, BFS, betweenness, WCC |
> | Snapshot compiler / Projection Build Store | `slater-build`, offline Cypher-dump compiler |
>
> This is not an adjacent competitor. **This is the architecture in this folder,
> built and published while the folder was being written.** Created six weeks ago.

**Where it does NOT reach (the honest remaining gap):** no pre-run memory receipt
or estimate is documented; the budget is a *cache* budget (LRU tuning knob), not a
per-algorithm admission decision with a printed bill and a reject path; and it is
a Bolt *server* replacing Neo4j on the read path, not a plugin coexisting inside
a live Neo4j. It also accepts the export/compile cost that `gtm-POC-01` was
designed to eliminate.

### 3.7 Grafeo — same thesis, more traction, Bolt compatibility in progress

**VERIFIED** · https://github.com/GrafeoDB/grafeo · Apache-2.0 · Rust ·
created **2026-01-26** · pushed 2026-07-20 · **707 stars**, 31 forks, 41 open issues

```text
"Grafeo is a graph database built in Rust from the ground up for speed and
 LOW MEMORY USE."

"Transparent spilling for out-of-core processing"
"Memory-mapped storage: Disk-backed vectors with LRU cache for large datasets"
"Streaming execution for large result sets without buffering"
"Resource limits: query timeouts, property size caps, HNSW max_elements bound"

query languages: GQL (ISO/IEC 39075), Cypher (openCypher 9.0), Gremlin,
                 GraphQL, SPARQL, SQL/PGQ
```

Published benchmark table (vendor's own, unverified by us):

```text
                  SNB Interactive   Graph Analytics
  Grafeo Server         730 ms            15 ms
  Memgraph            4,113 ms            19 ms
  Neo4j               6,788 ms           253 ms
  ArangoDB           40,043 ms        22,739 ms
   "...while using a fraction of the memory of some of the alternatives."
```

Algorithm surface, from the source tree (`crates/grafeo-adapters/src/plugins/algorithms/`):
`centrality.rs`, `clustering.rs`, `community.rs`, `components.rs`, `flow.rs`,
`isomorphism.rs`, `metrics.rs`, `mst.rs`, `shortest_path.rs`, `structure.rs`,
`traversal.rs` — i.e. the seven families and then some.

**And the Neo4j-compatibility piece:** https://github.com/GrafeoDB/boltr —
*"A standalone, pure Rust implementation of the Bolt v5.x wire protocol"*,
described in the parent README as *"Bolt Wire Protocol: pure Rust Bolt v5.x
implementation for Neo4j driver compatibility"* (created 2026-02-20, 4 stars —
early, but the intent is explicit).

Ecosystem already built out: `grafeo-server`, `grafeo-memory` (*"AI memory layer
for LLM applications"*), `grafeo-langchain` (*"graph store, vector store, Graph
RAG retrieval"*).

**Where it does NOT reach:** its "Resource limits" are timeouts, property-size
caps and an HNSW element bound — **not** an enforced memory ceiling on algorithm
state, and no pre-run estimate/receipt is documented anywhere. Spilling is
described as "transparent", i.e. automatic, not budgeted-and-quoted.

### 3.8 Second outlet confirms the Apple/Kuzu acquisition

**VERIFIED** · 9to5Mac, Chance Miller, **2026-02-11** ·
https://9to5mac.com/2026/02/11/kuzu-database-company-joins-apple/

```text
"Apple also recently acquired a database company called Kuzu."
"The Kuzu acquisition, which was first spotted by AppleInsider, occurred in
 October. Kuzu has scrubbed most of its online presence, which is common when a
 company is acquired by Apple."
```

Upgrades `simulation01.md`'s single-outlet BetaKit citation to **two independent
outlets plus AppleInsider**. Note the detail: the deal *occurred in October* —
i.e. simultaneous with the archival, and LadybugDB's creation on 2025-10-07.

### 3.9 Demand-side voice (thin, but real, and newly found)

**VERIFIED** · HN thread on Slater, 2026-07-21 · https://news.ycombinator.com/item?id=48996325

```text
[UltraSane] "I've wanted to analyze all published scientific papers and authors
 and their citation relationships in Neo4j but hit resource limits so this is
 very interesting to me."

[FrustratedMonky] "Big weakness of Neo4j, etc...? Since it is a much in demand
 feature. Why do you think they have not done it themselves already"

[rickkjp, Slater author] "'Low memory' and 'written in Java' are at least in my
 experience not terribly compatible goals."
```

First genuine practitioner demand quote obtained in either research pass — the
academic/bibliometric segment, hitting resource limits in Neo4j. One data point.

### 3.10 A negative finding worth keeping

**VERIFIED** · Neo4j Discourse full-text search, `search.json` · accessed 2026-07-25

Searching the **entire** Neo4j community forum for `"spill to disk"` returns
**exactly one topic** — 73039, the paul.horn thread quoted in §1.2. Related
queries (`"aura credits exhausted"` → 0 topics; `"cheaper alternative"` → 1;
`"cannot afford"` → 3, all irrelevant) are similarly empty.

This is independent corroboration of *"we just never had enough people ask about
it"*. Either the demand is not being voiced, or it is not being voiced in these
words. **It supports Leg 1 and undercuts Leg 3, exactly as §1.2 does.**

### 3.11 The Aura cost corpus still does not exist

**Closest thing found**, Neo4j Discourse topic 75748, 2025-10-22:

```text
"I am looking at the cost data on the aura console and different 'credits used'
 values shows up twice for the same instance with same usage though. Is that
 normal? eg: instance = instance_test, usage = 1190, credits used = 310 /
 instance = instance test, usage = 1190, credits used = 190"
```

That is billing-data **confusion**, not bill shock. After sweeping Discourse, HN
Algolia (22 hits on "neo4j pricing", all 2011–2021 and already in
`simulation01.md`), and StackExchange, **no new Aura cost-pain evidence exists.**
Leg 2's commercial half remains list-price arithmetic. State this plainly in any
deck.

---

## 4. Quarantined claims — status after this pass

Six `[ext-unverified]` items from `simulation01.md` §10.1 were targeted. **Five remain
UNFOUND.** One was found and it **corrects rather than confirms** the original claim.

| # | claim | status |
| --- | --- | --- |
| a | K8s sidecar, 120GB provisioned, OOM at ~40GB used | **UNFOUND** |
| b | delta-stepping OOM, 12GB heap, 63k-node graph | **UNFOUND** |
| c | Louvain on Yelp → corruption + ~23GB | **UNFOUND** |
| d | "$70k a year isn't even nearly competitive"; Neptune ~1/6 price | **UNFOUND** |
| e | node-ID-only projection workaround culture | **UNFOUND** |
| f | GraphRAG drops ~10% of entities because Leiden discards isolated nodes | **FOUND — AND WRONG AS STATED** |

### 4.1 Claim (f), corrected

**VERIFIED** · https://github.com/microsoft/graphrag/issues/2348 · opened 2026-05-10 ·
**closed as not planned**
Title: `_filter_under_community_level` silently drops all entities without community
assignments due to NaN comparison

It is **not** Leiden discarding weakly-connected nodes. It is a **filtering bug in
the query layer**: entities Leiden left unassigned get `level = NaN` after a left
join, and `df[df.level <= community_level]` silently drops them because
`NaN <= n` is False.

```text
reporter: "entities that were not placed into any community by Leiden get
           level = NaN after the left join. The subsequent filter...drops them."
test case: "Total: 151, Assigned: 10, Orphaned: 141"   -> 93.4%, not ~10%
```

Rewrite the claim in `simulation01.md` §10.2 accordingly. The magnitude is far
larger than "~10%" in this reporter's case, the mechanism is different, and the
issue is closed as not planned — which is itself the interesting part.

---

## 5. What the first pass did NOT find, and exactly why

> **SUPERSEDED IN PART — see §5.1.** After this section was written, the failed
> channels were re-run through different tooling (open APIs instead of WebSearch)
> and produced §§3.6–3.11, including the two most consequential findings in the
> dossier. Read §5 as a record of *why the first attempt failed*, then §5.1 for
> what worked. The Aura cost corpus and four of six quarantined claims remain
> genuinely absent.

**Read the original failure as a tooling failure, not as evidence of absence.**

The four named priority gaps of the research question produced **nothing verifiable**:

```text
(1) Reddit / Stack Overflow 2024-26 / dev.to / Medium / LinkedIn / X / Lobsters
    / Discord  ->  NOTHING surfaced
(2) A real "my Aura analytics bill" complaint corpus  ->  NOT LOCATED
(3) Five of six quarantined claims  ->  UNFOUND
(4) Demand-side segment sizing (procurement, air-gapped, egress policy,
    laptop workflows, survey/download/job-posting counts)  ->  NOTHING
```

Cause, reported independently by every verifier in the run:

```text
WebSearch          -> "API Error: 400 output_config.effort 'xhigh' is not
                       supported when thinking is disabled"  on ALL attempts
DuckDuckGo         -> bot CAPTCHA on html/lite endpoints, not bypassed
Bing               -> no parseable results
MCP web_search     -> rate-limited
2 verifier sessions-> search budget exhausted (200/200)
```

**Independently reproduced:** attempting `WebSearch` by hand during this write-up
returned the identical `API Error: 400 ... 'xhigh' is not supported` message. The
tooling explanation is genuine, not an agent excuse.

Direct API probes that *did* run found nothing: Neo4j Discourse `search.json` for
`"paused charged"`, `"pause billing"`, `"billed while paused"` returned no thread
complaining about the rate. `https://neo4j.com/docs/aura/changelog/` returns 404.

Consequence, stated plainly:

> The thesis's **architectural** leg is now heavily documented and close to
> saturated — more work here has diminishing returns.
> The thesis's **commercial-pain and market-size** legs are **not evidenced at
> all.** Every dollar figure in this dossier is list-price arithmetic from public
> rate cards, not an observed invoice. And the one first-party datapoint we have
> about segment size (§1.2) **cuts against us.**

Note also: 27 sources *were* fetched, including four Reddit threads
(r/Neo4j on Neptune comparison, "no one uses neo4j for actual large scale live",
enterprise pricing; r/Database on cheaper alternatives) and a new on-point forum
thread (`community.neo4j.com/t/.../71262`, GDS heap defaulting to 25% of system
memory → fast OOM). Their claims did not survive into the top-25 verification
budget. **These are live leads for a follow-up pass, not findings.**

### 5.1 The tooling that actually works — use this, not WebSearch

Replacing the broken search layer with **open APIs queried directly** recovered
the sweep in roughly 20 minutes and produced §§3.6–3.11. Reusable recipe:

| channel | status | how |
| --- | --- | --- |
| **Neo4j Discourse** | ✅ **works, unauthenticated** | `community.neo4j.com/search.json?q=<urlenc>` for topics; `community.neo4j.com/raw/<topic_id>` for full raw post text; `/u/<user>.json` for staff verification |
| **HN Algolia** | ✅ **works, no auth** — must be **HTTPS** (`http://` returns non-JSON) | `hn.algolia.com/api/v1/search?query=…&tags=comment`; `…/api/v1/items/<id>` returns the full comment tree |
| **StackExchange** | ✅ works, 300/day quota | `api.stackexchange.com/2.3/search/advanced?q=…&site=stackoverflow&filter=withbody` — prefer `tagged=` over free-text `q=`, which is very noisy |
| **GitHub** | ✅ **works, authenticated** (`gh` 2.82.1 logged in) | `gh search repos`, `gh api repos/<o>/<r>`, `gh api repos/<o>/<r>/readme --jq .content \| base64 -d`, `gh api "repos/<o>/<r>/git/trees/HEAD?recursive=1"` — **quote the URL**, `?` globs in zsh |
| **neo4j.com** | ✅ via `curl` with a browser User-Agent | 403s WebFetch; 200s curl. Strip tags in Python and grep keywords with context windows |
| **arbitrary pages** | ✅ WebFetch works fine | it was only `WebSearch` that was broken |
| **Reddit** | ❌ **blocked** | `.json` endpoints return a 190 KB HTML shell even with a browser UA. Needs an authenticated Reddit app (OAuth client credentials) — the one genuinely unresolved channel |
| **WebSearch tool** | ❌ broken | `API Error: 400 output_config.effort 'xhigh' is not supported when thinking is disabled` — reproduced by hand, not an agent excuse |

**Lesson for future passes:** general web search was the single point of failure,
and it was avoidable. Every high-value channel here has a documented JSON API.
Query the APIs first; use search only for discovery of *unknown* domains. The two
biggest findings in this entire dossier (Slater, Grafeo) came from **HN Algolia +
`gh`** — not from 110 agents.

---

## 6. Net effect on the thesis

```text
LEG                          BEFORE THIS PASS        AFTER THIS PASS
---------------------------  ----------------------  -----------------------------
GDS is heap-only, no spill   forum quotes (2022)     VENDOR DOCS + 2025 staff post
                                                     + 9 release notes  = SOLID
Neo4j meters by RAM          pricing pages           RATE CARD API, exact SKUs,
                                                     exact 10-min floor  = SOLID
Users feel dollar pain       thin, extrapolated      STILL THIN. Nothing added.
Segment is big enough        assumed / growing bet    EVIDENCE NOW CUTS AGAINST
                                                     ("never had enough people ask")
Estimation is our turf       believed differentiator  FALSE — ships on both sides
Out-of-core turf is empty    Kuzu InMemGraph comfort  FALSE — 4 live entrants
Nobody sells drop-in         PMF01 W4 keystone claim  FALSE — Slater ships Bolt
  low-RAM Neo4j compat                                5.4/4.4/4.1; Grafeo ships boltr
Neo4j can't/won't respond    structural argument      InfiniGraph shows movement on
                                                     the cost/over-provisioning axis
```

**The competitive timeline, which is the part that should worry us:**

```text
  2026-01-26   Grafeo created        (707* today — "low memory use",
                                     transparent spilling, out-of-core)
  2026-02-20   boltr created         (Bolt v5 for Neo4j driver compat)
  2026-06-10   Slater created        (bounded cache budget, Bolt, EF, ~14 B/edge)
  2026-07-21   Slater announced on HN, 43 points
  2026-07-25   this dossier written
  ^
  |  simulation01.md's competitive sweep (§9, §11) was accurate WHEN RUN.
  |  The turf was empty. It is no longer empty. It emptied and refilled
  |  inside the window this folder was being written.
```

**Three concrete edits this forces on the existing corpus:**

1. **`simulation01.md` §8 badge #1 ("THE BILL BEFORE THE RUN") must be demoted.**
   Neo4j estimates *and* recommends a tier. Promote the badge that survives:
   **the finish** — an enforced ceiling with spill. §3.6 of that document already
   reached this conclusion; §8 contradicts it. Resolve in favour of §3.6.
2. **`simulation01.md` §11.1's Kuzu comfort is obsolete.** Replace with LadybugDB
   as a live, MIT-licensed, partially-overlapping incumbent. The differentiation
   narrows to enforced budget + receipt + GDS compatibility.
3. **`simulation01.md` §7.5 F1/F2 ("can't shard", "vertical only") should be retired**
   as current complaints, dated pre-InfiniGraph.

**And the sequencing implication is unchanged from what the corpus already knew:**
the architectural evidence is saturated. Further research on *whether GDS has a
memory wall* is now waste. The unevidenced legs are demand and dollars, and those
are answered by talking to people and by shipping something they can run — not by
another dossier.

---

## 7. Open questions (ranked by stakes)

1. **Does LadybugDB's `algo` extension bound algorithm STATE, or only avoid
   materializing the projection?** Nothing in its docs states a memory ceiling,
   a spill path for algorithm state, or a pre-run estimate. **Benchmark it on a
   graph 5–10× available RAM before conceding the turf.** Also: does the reported
   Apple acquisition of Kuzu leave Ladybug's governance independent?
2. **Does InfiniGraph run GDS algorithms sharded, or only queries?** If GDS
   algorithms become distributed-capable, the "one big machine" framing weakens
   considerably. Check GDS-on-InfiniGraph docs, NODES 2026 (12 Nov) abstracts.
3. **Does a real Aura cost-complaint corpus exist anywhere reachable** —
   authenticated Reddit API, Discourse full-text, G2/TrustRadius/PeerSpot,
   procurement RFPs? And conversely, is anyone on record saying AGA serverless
   *solved* their memory problem? Until one or the other is found, the pricing-pain
   leg is arithmetic only.
4. **How large is the "cannot just buy more RAM" segment in dollars**, given a
   Neo4j staffer reports customers renting 12TB machines and "never had enough
   people ask"? This needs demand-side quantification, not more architecture.
5. **Has Neo4j signalled out-of-core work outside release notes** — roadmap,
   NODES/GraphConnect talks, the GraphAware acquisition, job postings mentioning
   spill/off-heap/external-memory? §3.5 proves this channel yields real material.

---

## 8. Citation hygiene (will bite you)

```text
- console.neo4j.io/pricing is an SPA; anonymous curl returns a 17KB shell.
  Cite the product-catalogue API instead.
- neo4j.com returns 403 to WebFetch but 200 to curl with a browser UA.
- /docs/aura/billing/usage-report 301-redirects to /cost-explorer/.
- Neo4j docs pages carry NO publication date. "Exact date" can only be an
  access date (2026-07-25) or a Wayback snapshot; docs-repo commit history is
  the closest thing to a revision date.
- ACU->USD rests on ONE printed line ("1 ACU = 1 USD"). All derived dollar
  figures inherit that assumption.
- TIME FUSE: catalogue v1.5 and v2.0 both take effect 2026-08-01 (7 days after
  access). v2.0 collapses Graph Analytics Serverless to 3 rows and drops the
  "not sellable" note on 1GB, so the "42 rows / 2GB floor" detail expires
  imminently. The load-bearing economics (0.0067 ACU per GB-minute on declared
  RAM + 10-minute minimum) survive into v2.0 verbatim. SNAPSHOT THE JSON NOW.
- Ladybug pushed commits the day of access; Onager gained DuckDB 1.5.5 support
  three days before. Re-verify both before any publication.
```

---

## 9. References

**Neo4j — memory architecture (vendor docs)**
1. https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/
2. https://neo4j.com/docs/graph-data-science/current/installation/System-requirements/
3. https://neo4j.com/docs/graph-data-science/current/management-ops/
4. https://neo4j.com/docs/graph-data-science-client/current/aura-graph-analytics/
5. https://neo4j.com/docs/graph-data-science/current/aura-graph-analytics/cypher

**Neo4j — staff statement**
6. https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039 (posts #2, #6, 2025-03-17)
7. https://community.neo4j.com/raw/73039/6

**Neo4j — release notes (nine, Nov 2025 – Jul 2026)**
8. https://neo4j.com/release-notes/gds/
9. https://neo4j.com/release-notes/gds/graph-data-science-2-23/
10. https://neo4j.com/release-notes/gds/graph-data-science-2-25-0/
11. https://neo4j.com/release-notes/gds/graph-data-science-2-27-0/
12. https://neo4j.com/release-notes/gds/graph-data-science-2026-06-0/
13. https://api.github.com/repos/neo4j/graph-data-science/releases

**Neo4j — billing and pricing**
14. https://neo4j.com/docs/aura/billing/cost-explorer/
15. https://neo4j.com/docs/aura/billing/billing-dimensions/
16. https://neo4j.com/docs/aura/graph-analytics/aga/
17. https://console.neo4j.io/api/product-catalogue/v1/versions/1.4/products
18. https://console.neo4j.io/api/product-catalogue/v1/versions
19. https://neo4j.com/pricing/
20. https://support.neo4j.com/s/article/5964769284883-How-much-do-I-pay-when-my-Aura-Instance-is-paused
21. https://neo4j.com/docs/aura/managing-instances/instance-actions/

**Neo4j — InfiniGraph**
22. https://neo4j.com/blog/graph-database/infinigraph-scalable-architecture/ (2025-09-03)

**Competitors — the four live entrants**
23a. https://github.com/Hikari-Systems/slater (Apache-2.0, Rust, created 2026-06-10)
23b. https://news.ycombinator.com/item?id=48996325 (Slater HN launch, 2026-07-21, 43 pts)
23c. https://news.ycombinator.com/item?id=48996326 (author's positioning comment)
23d. https://github.com/GrafeoDB/grafeo (Apache-2.0, Rust, 707*, created 2026-01-26)
23e. https://github.com/GrafeoDB/boltr (Bolt v5.x for Neo4j driver compatibility)
23f. https://github.com/GrafeoDB/graph-bench (their published benchmark suite)
23g. https://github.com/GrafeoDB/grafeo-langchain · https://github.com/GrafeoDB/grafeo-memory
23h. https://9to5mac.com/2026/02/11/kuzu-database-company-joins-apple/ (2nd outlet, 2026-02-11)
23. https://docs.ladybugdb.com/extensions/algo/
24. https://github.com/LadybugDB/ladybug
25. https://raw.githubusercontent.com/LadybugDB/ladybug/main/README.md
26. https://docs.ladybugdb.com/cypher/configuration/
27. https://memgraph.com/docs/fundamentals/storage-memory-usage
28. https://memgraph.com/docs/release-notes
29. https://github.com/CogitatorTech/onager
30. https://github.com/CogitatorTech/onager/issues/27
31. https://github.com/habedi/graphina

**GraphRAG**
32. https://github.com/microsoft/graphrag/issues/2348 (2026-05-10, closed not planned)

**Live leads, fetched but NOT verified — for a follow-up pass**
33. https://community.neo4j.com/t/gds-runs-in-heap-and-heap-is-by-default-set-to-25-of-system-memory-leading-to-oom-quickly-way-to-make-it-dynamically-more/71262
34. https://www.reddit.com/r/Neo4j/comments/18ygbwd/no_one_uses_neo4j_for_actual_large_scale_live/
35. https://www.reddit.com/r/Neo4j/comments/1eyuu73/anyone_have_experience_with_both_neo4j_and_aws/
36. https://www.reddit.com/r/Neo4j/comments/1b72mzf/how_much_is_enterprise_edition_typically/
37. https://www.reddit.com/r/Database/comments/jpmnxp/freecheaper_alternatives_to_neo4j_for_a/
38. https://github.com/neo4j/docs-aura/issues/774
39. https://github.com/neo4j/graph-data-science/issues/69
40. https://arxiv.org/pdf/2508.20637

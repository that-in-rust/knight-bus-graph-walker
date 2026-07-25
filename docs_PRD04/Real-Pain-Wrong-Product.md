# Real-Pain-Wrong-Product — Assimilation And Verdict

Date: 2026-07-25
Status: **judgment, not evidence.** Every fact here is sourced in `evidence01.md`
(vendor-primary, verified 2026-07-25) or `simulation01.md` (practitioner corpus).
This file is the layer those two do not contain: what it all means, and what I
would actually do.

Inputs assimilated: all 24 files of `docs_PRD04/` (prd-l1, SUM01, Arch01–06,
Arch-options, Arch-Summary, PMF01–04, simulation01, gtm-POC-01,
Reference-Learning-Critique-Gaps, Gap-Closure ×2, learning specs, dossier specs),
plus a 110-agent web-research pass (`evidence01.md`).

---

## 0. The whole thing in one box

```text
  THE PAIN IS REAL.            <- now vendor-documented, not inferred
  THE PRODUCT IS WRONG.        <- prd-l1 specifies a database; the pain needs a component
  THE MOAT IS GONE AS WRITTEN. <- FOUR live entrants, two of them shipping the
                                  exact thesis (bounded RAM + Bolt + Rust).
                                  Not narrowed. Contested.
  THE MARKET IS UNMEASURED,    <- and the only first-party datapoint cuts against us
    AND THAT IS NOW THE
    ONLY QUESTION THAT MATTERS.
```

Four words, and the whole argument: **real pain, wrong product.**

> **REVISION, 2026-07-25 (same day).** §3 of this file originally said the moat was
> "narrower than we wrote." A 20-minute sweep of open APIs — after the 110-agent
> research pass failed on broken search — found **Slater** (created 2026-06-10,
> announced on HN four days ago) and **Grafeo** (707 stars, created 2026-01-26).
> Both are Rust. Both lead with low memory. Both ship or are shipping **Bolt wire
> compatibility**. Slater's README is, clause for clause, this folder's PRD.
> I was too generous this morning. The correct word is not *narrower* — it is
> **contested**. §3 is rewritten below; §9 is re-scored.

---

## 1. The three legs, graded

Every strategic claim in this folder rests on three independent legs. They are in
radically different condition, and the corpus has been treating them as if they
were equally strong.

```text
LEG 1: THE ARCHITECTURAL CLAIM  ("GDS is heap-only, no spill, by design")
  ############################################  SATURATED. Stop researching this.
  Vendor docs say GDS "operates completely on the heap."
  Vendor's own remedy list = drop graphs / raise heap / sudo-bypass. No disk option.
  Vendor tells analytics users to SHRINK page cache in favour of heap.
  Neo4j staff engineer, 2025-03-17, on the record:
    "providing a *good* implementation of out-of-core GDS has never been
     prioritized enough"
  Nine consecutive GDS releases (Nov 2025 -> Jul 2026): zero disk/spill/off-heap.
  => This leg is as strong as a pre-code project's leg can be. FURTHER WORK IS WASTE.

LEG 2: THE PRICING CLAIM  ("Neo4j meters the exact thing we're better at")
  ##################################......  MECHANICALLY PROVEN, COMMERCIALLY UNPROVEN
  Rate card, exact: GB-minutes on DECLARED session RAM, 14-rung ladder 2GB-512GB,
    no autoscaling, 10-minute minimum billed at FULL declared size.
    A 512GB session costs USD 34.30 even if it finishes in one second.
  GB-hours "whether actively used or not". Paused instances bill 20%, not zero.
  BUT: zero located complaints from anyone who actually paid one of these bills.
  => The mechanism is documented. The PAIN is arithmetic. Nobody is on record hurting.

LEG 3: THE MARKET CLAIM  ("the segment that can't just buy RAM is big enough")
  ###...................................  UNEVIDENCED, AND EVIDENCE POINTS THE WRONG WAY
  Complaint volume: tens of good threads over five years. Thin.
  Loudest resentment is licenses/clusters, NOT GB-hours.
  And the one first-party datapoint we have is the SAME staff post as Leg 1:
    "we've not had customers who weren't able to rent a big enough machine in
     the cloud (some even had 12TB ram)"
    "we just never had enough people ask about it to justify working on it"
  => THE SENTENCE THAT PROVES THE WALL EXISTS ALSO SAYS NOBODY IS ASKING.
```

**This is the single most important thing I understood.** The corpus has spent
three months and 12,000 lines hardening Leg 1 — which was already the strongest —
while Leg 3, which is the one that decides whether any of this is a business,
has never been touched. And the best new evidence for Leg 1 is simultaneously the
best evidence *against* Leg 3. Those are the same forty words.

---

## 2. The scope error

`prd-l1.md` commits to rewriting Neo4j: Neo4j-shaped OLTP storage as source of
truth, records/WAL/tx/locks, 575 procedures, Bolt handshake versions, driver retry
semantics, Cypher temporal/spatial/path value edge cases, APOC tiers.

`simulation01.md` §7.8 commits to the opposite: OLTP query slowness is a
**high-volume complaint we explicitly do not fix**, and being loudly OLAP-only is
a **credibility asset**.

```text
  THE PRD's SCOPE                        THE EVIDENCE'S SCOPE
  ------------------------------------   ------------------------------------
  OLTP records, WAL, locks, tx           (nobody asked)
  575 GDS procedures                     7 families = ~85% of adoption
  Bolt + drivers + Cypher + APOC         (nobody asked)
  Transactional correctness              explicitly declared NOT OUR PROBLEM
  ------------------------------------   ------------------------------------
  = a decade, for a large team           = a component, for one person
```

The folder already caught this. `Reference-Learning-Critique-Gaps.md` §3–4 says it
plainly: *"zero application-code changes will fail at the client behavior layer
even if the storage architecture is good."* That critique was written, filed, and
then not acted on — the top of the pyramid still says "rewrite."

**The reframe, in one sentence:**

> You are not rewriting Neo4j. You are building the spill-to-disk executor that
> Neo4j's own staff engineer said, on the record, they never prioritized.

That is a component. Components ship. Databases do not — not solo.

And the tell that the folder already knows: **the positioning documents and the
product document describe different products.** PMF03/04 land on "your hardware,
your data, finishes anyway." `gtm-POC-01.md` lands on one jar, one procedure, one
diff. Neither is a database. When positioning, GTM, and PRD disagree, the strategy
hasn't been chosen — it's been deferred, and deferral is being experienced as
breadth.

---

## 3. The moat, re-priced honestly (this is where I changed my mind)

Before the research pass I believed the corpus's competitive read: the turf is
structurally empty, and Kuzu's `InMemGraph` proved even the closest competitor
carried our wall inside it. **Two verified findings force me to downgrade that.**

### 3.1 Estimation is not our turf. It ships on both sides.

```text
  WHAT WE THOUGHT WE OWNED          WHAT NEO4J ACTUALLY SHIPS
  ------------------------------    ----------------------------------------
  "we tell you the bill before      .estimate on every production-tier algo
   the run"                         (new algos ship with it as routine)
                                    gds.session.estimate -> estimatedMemory
                                    PLUS "recommendedSize" = the smallest
                                    tier that covers it
```

They don't just estimate. They estimate **and name the RAM tier to purchase.**

`simulation01.md` §8 makes "THE BILL BEFORE THE RUN" its badge #1 and calls it
insight-level and uncopyable. That is now false as written. But §3.6 of the same
document already got it right: *what they don't offer is the second half of the
sentence — "and here is how it finishes anyway."* The corpus contradicts itself
and the resolution must go to §3.6.

**What survives, stated narrowly enough to be true:**

```text
  DEAD:      "we estimate memory"          (they do)
  DEAD:      "we reject before running"    (they block too — E1 was a refusal)
  ALIVE:     an ENFORCED CEILING the engine honours by SPILLING
  ALIVE:     universal coverage (their .estimate is production-tier only)
  ALIVE:     THE FINISH. The job completes on hardware that cannot hold it.
```

Sell the finish. The receipt is the credibility wrapper, never the hero. A receipt
without an engine that honours it is what PMF04's Timeline C correctly called
**advice, not product.**

### 3.2 Kuzu did not die. It was renamed, and it shipped part of our product.

**LadybugDB** — MIT, 1,475 stars, created 2025-10-07 (immediately after Kuzu's
archival), commits pushed the day I checked. Its `algo` extension documentation:

```text
  "Ladybug does not materialize projected graphs in memory, and the
   corresponding data is scanned from disk on the fly."

  algorithms: K-Core / Louvain / PageRank / SCC / WCC
```

This directly overturns `simulation01.md` §11.1, whose central competitive comfort
was that the closest competitor carried the projection wall inside its own
`extension/algo/`. **The successor project fixed exactly that**, is permissively
licensed, embedded, and active today. It also answers "where did the Kuzu orphans
go": nowhere. The project was renamed.

**The honest limit, also verified — and it is the gap we live in:**

```text
  Ladybug HAS:      no in-memory projection; disk-streamed algorithm execution
  Ladybug HAS NOT:  any memory-budget parameter for algo runs
                    any spill path for ALGORITHM STATE (its only spill knob is
                      COPY FROM / ingest-scoped)
                    any pre-run estimate or receipt anywhere in the algo docs
                    Neo4j/GDS API compatibility
```

Which matters because of `Arch02.md`'s own R2 finding: **Louvain and NodeSimilarity
— 27% of adoption weight — die on algorithm STATE, not on projection bytes.**
Ladybug removed the projection wall. It publishes no guarantee about the wall that
actually kills the two highest-value families.

So the differentiation is real but **much narrower than the corpus claims**:

```text
  NOT:  "nobody runs graph algorithms off disk"        <- false as of Oct 2025
  BUT:  "nobody enforces a memory CEILING on algorithm
         state, proves it with a pre-run receipt, and
         answers to gds.* call shapes"
```

That is a defensible position. It is also a much smaller sentence than "we rebuilt
Neo4j," and it needs to be benchmarked against Ladybug before it is asserted.

### 3.2b Slater and Grafeo — the thesis is being executed by other people, right now

This is the finding that changes the decision, and it was four days old when found.

**Slater** — `Hikari-Systems/slater`, Apache-2.0, Rust, created **2026-06-10**,
announced on HN **2026-07-21**. Its own README:

```text
"Slater serves graphs that don't fit in memory -- hundreds of millions of nodes
 and billions of edges in low hundreds of MB of RAM -- over standard Bolt, so any
 neo4j driver just works ... Resident memory is set by a cache budget you choose,
 NOT by the size of the graph."

"A 4 GB graph and a 400 GB graph cost the same RAM to serve"

"You compile the graph once, offline, into a content-addressed on-disk image with
 slater-build" ... "an immutable, content-hashed generation directory"

"Speaks Bolt 5.4 / 4.4 / 4.1 -- use the standard neo4j drivers ... cypher-shell,
 or graph browsers unchanged."

"graph algorithms (PageRank, BFS, betweenness, WCC...) -- bounded memory"
```

Plus, from the author in-thread: Elias-Fano succinct structures, ~14 bytes/edge,
superhub-aware block packing.

Now put that beside our own files:

```text
  OURS (docs_PRD04)                          THEIRS (shipped)
  ---------------------------------------    -------------------------------------
  GRAIN: immutable generation-sealed dirs    "immutable, content-hashed
    (Arch05 G5)                                generation directory"
  AXIS 1: content-addressed generations      content-addressed on-disk image
    (Arch-Summary §5)
  L2: RAM = O(V) by construction (Arch06)    "cache budget you choose, not the
                                               size of the graph"
  G2: Elias-Fano warm stratum (Arch05)       Elias-Fano succinct structures
  G1: degree-rank / superhub (Arch05)        "heuristics to avoid superhubs"
  Snapshot compiler / Build Store (prd-l1)   slater-build, offline compiler
  Zero client change, Bolt (prd-l1)          Bolt 5.4/4.4/4.1, drivers unchanged
  Seven families (Arch02)                    PageRank, BFS, betweenness, WCC
```

**That is not an adjacent competitor. That is this folder's architecture, built
and published while this folder was being written.** By one person, in Rust, in
about six weeks of repo history.

**Grafeo** — `GrafeoDB/grafeo`, Apache-2.0, Rust, created **2026-01-26**,
**707 stars**, pushed five days before this was written:

```text
"built in Rust from the ground up for speed and LOW MEMORY USE"
"Transparent spilling for out-of-core processing"
"Memory-mapped storage: Disk-backed vectors with LRU cache for large datasets"
Cypher (openCypher 9.0) + GQL + Gremlin + GraphQL + SPARQL + SQL/PGQ
algorithms: centrality, community, components, clustering, flow, mst,
            shortest_path, traversal, metrics, isomorphism, structure
+ boltr: "pure Rust Bolt v5.x implementation for Neo4j driver compatibility"
+ grafeo-langchain, grafeo-memory ("AI memory layer for LLM applications")
```

**What this kills outright:** `PMF01.md` W4 — *"nobody in the market sells 'your
existing Neo4j code, on 1/10th the RAM'"* — was the keystone of the entire
positioning. It is now false. Two projects sell exactly that, in our language,
under permissive licenses.

**What still isn't claimed by anyone (the last defensible ground, and it is small):**

```text
  ✗ out-of-core execution          -> Ladybug, Slater, Grafeo all have it
  ✗ bounded//budgeted memory       -> Slater: three cache budgets you set
  ✗ Bolt / driver compatibility    -> Slater ships it; Grafeo building it
  ✗ Rust, no JVM, single binary    -> all three
  ✗ immutable generations          -> Slater has it
  ✓ a PRE-RUN RECEIPT: estimate-before-execute over a KB of manifest, an
    ENFORCED per-algorithm admission decision, and a REJECT path with a
    printed bill. Slater's budget is an LRU cache knob, not an admission
    controller. Grafeo's "resource limits" are timeouts and property caps.
    Neither documents a pre-run estimate for an algorithm run.
  ✓ gds.* procedure-shape compatibility (Bolt/Cypher ≠ gds.* call surface)
  ✓ coexistence INSIDE a live Neo4j (the gtm-POC-01 plugin) — everyone else
    replaces Neo4j on the read path and pays the export/compile cost
```

Three of those are real. They are also **three features, not a moat** — and
`Arch02.md` R2 says the one that matters most (bounded *algorithm state* for
Louvain/NodeSim) is the hardest of the three to build.

### 3.3 Neo4j is moving on the cost axis (InfiniGraph)

Neo4j blog, 2025-09-03 — a channel the research sweep never covered and found only
by accident:

```text
  "Property sharding allows the graph itself to remain logically whole"
  "100TB+ horizontal scale with zero application rewrites"
  "Adjustable storage - ... scaling storage independently of memory and compute
   ... You'll be able to avoid over-provisioning and reduce cloud costs"
```

| effect | verdict |
| --- | --- |
| Scale-OUT via property sharding, not single-node out-of-core | Architecture leg **survives** |
| Kills "you can't shard Neo4j / vertical only" | **Retire** `simulation01.md` §7.5 F1/F2 |
| "avoid over-provisioning and reduce cloud costs" | Neo4j is **working our axis**. The structural-blindness argument weakens. |
| Unifies transactional + analytical "in the same system" | Does GDS run sharded? **Open, high stakes.** |

The corpus's most comfortable belief was that the incumbent is *structurally*
prevented from following. InfiniGraph shows a company willing to decouple storage
from RAM and say the words "avoid over-provisioning" out loud. Not a refutation.
A warning that the moat is time-limited, not permanent.

---

## 4. What the product actually is

The folder already designed the right thing. It is in `gtm-POC-01.md` and it is
not a rewrite — it is a parasite.

```text
        CALL gds.wcc.stream('g')          CALL grain.wcc.stream('snap1')
   +---------|-----------------------------------|---------------------+
   |  NEO4J (stock, unmodified)                  |                     |
   |    gds-plugin.jar                    grain-plugin.jar (~200 ln)   |
   |    FULL GRAPH -> JVM HEAP            graph NEVER enters heap      |
   |         |  <= the wall                      | JNI                 |
   +---------|-----------------------------------|---------------------+
             v                                    v
      Java union-find                     libgrain_ffi.so (Rust)
      often BLOCKED by                    mmap snapshot on disk
      gds's own estimator                 labels[V] resident only
             |                                    |
             +--------> SAME PARTITION <----------+
                  canonicalize by min member; diff MUST be empty
```

Three properties make this correct rather than merely cheap:

1. **A hostile skeptic verifies you in one line.** Canonicalize components by min
   member, `diff`, empty. No float-parity argument, no "why is row 47 different in
   the 7th decimal." WCC is the only family where this is true — which is why WCC
   first, not because it is impressive.
2. **It inverts the export cliff.** The corpus names the Neo4j export (2–8 hrs,
   30–40% failure) as the #1 adoption blocker in three separate places.
   `CALL grain.snapshot.build(...)` runs *inside* the database that already holds
   the data. The blocker becomes the funnel.
3. **Total reversibility.** One jar. Delete it, nothing changed.

**And the PMF argument, which is the one that actually decides it.** The trigger
moment is exact and we have the transcript:

```text
  "Procedure was blocked since minimum estimated memory (52 GiB) exceeds
   current free memory (5120 MiB)"
```

Where is that person at that instant? **Inside a Neo4j session, at a prompt, data
already loaded, one line from giving up.** Ask what each candidate demands of them
*in that moment*:

```text
  rewrite Neo4j     -> "migrate your database"        they will not
  export + sidecar  -> "run an 8-hour risky export"   they mostly will not
  plugin            -> type the next line              <- reachable from the pain
```

A product must be reachable from the trigger moment. Only one of these is.

---

## 5. What to retire from the corpus

| # | retire | because |
| --- | --- | --- |
| 1 | prd-l1's OLTP/Bolt/Cypher/APOC/575 scope | Cost against a complaint you declared out of scope. Fatal for one person. |
| 2 | `simulation01.md` §8 badge #1 ("the bill before the run") as insight-level | Neo4j estimates *and* recommends a tier. Promote **the finish** instead. §3.6 already says this. |
| 3 | `simulation01.md` §11.1's Kuzu-InMemGraph comfort | Obsolete. Replace with LadybugDB as a live partial incumbent. |
| 4 | `simulation01.md` §7.5 F1/F2 ("can't shard", "vertical only") | Dated pre-InfiniGraph. |
| 5 | `simulation01.md` §10.2 claim (f) as written | It is a query-layer NaN filter bug, not Leiden discarding nodes — and 93.4% in the reporter's case, not ~10%. Rewrite. |
| 6 | GRAIN-as-open-format / the Parquet play | PMF04 prices it honestly itself. A spec with one implementation is documentation. Defer until something runs. |
| 7 | The Arch01→06 series | Oscillating between brownfield and greenfield premises (Arch04 catches the greenfield fallacy; Arch05 suspends the correction; Arch06 goes greenfield again). Premises are free; measurements aren't. |

---

## 6. The pattern I want named, because it will recur

Every document in this folder contains its own kill criterion and check date.

```text
  Arch04  X1  "1 week"   kill-check: end of week 2   -> not run
  Arch05  X1  "week 1, paper only"                   -> not run
  Arch06  L1.c prototype "one week ... gates the
              biggest claim in this doc"             -> not run
  Arch02  X1' "1 week"                               -> not run
  PMF02   Y1  "1 week, do immediately"               -> not run
  PMF02   Y2  "ten OOM interviews"                   -> not run
  PMF03   Z1  "week 1, ~0 cost"                      -> not run
  gtm-POC grain.ping "one week"                      -> not run
```

Eight specified falsification tests. Zero executed. Six more documents instead.

This is not sloppiness. It is a precise thing: **the appearance of accountability
without the exposure.** Writing "check date: end of week 2" and then writing the
next document is how a rigorous person avoids a verdict while feeling rigorous.

And the reason is legible and human: **the estimator dry run is one day of work and
it could end the project.** Right now the *idea* is the most valuable asset here,
and a running binary might be worth less than the idea. Documents never say no.
Code does. Users really do.

Note the inversion this produced: **the quality of the writing rose as the
proximity to reality fell.** The best documents in this folder are the ones
furthest from a compiler.

Evidence it is fixable: the *one* time reality was touched — reading GDS's
`CompressedAdjacencyList` — the headline multiplier dropped honestly from
50–100× to 10–30× (`Arch06.md`). That is what contact looks like. It happened
once, on paper.

---

## 7. The four weeks that would settle this

```text
  DAY 1     Estimator dry run. gds.*.estimate for the seven families on a public
            50 GB-class graph, stock Neo4j. Zero code. It is the number EVERY
            document leans on. If it comes back small, you have saved a year --
            cheaply, privately, today.

  WEEK 1    grain.ping. JNI round trip: Java shim -> Rust cdylib -> mmap an
            existing snapshot -> stream 1,000 (nodeId, degree) pairs back.
            De-risks classloader + transport + ID mapping simultaneously.
            Nothing else in this corpus has that ratio.

  WEEK 2    Benchmark LadybugDB's algo extension on a graph 5-10x available RAM.
            Does it hold algorithm state within bounds, or OOM once the
            projection wall is gone? THIS DECIDES HOW BIG THE REMAINING GAP IS.
            Do not write another positioning doc before this number exists.

  WEEKS 2-4 Ten interviews with people who actually hit GDS memory failures.
            Add one question: "why didn't Aura Graph Analytics sessions solve
            this for you?" That question separates the buyers serverless already
            saved from the ones it structurally cannot. It is the cheapest
            possible probe of LEG 3 -- the only leg that decides the business.
```

Four weeks converts all three load-bearing unknowns into facts. Three months have
produced the best analysis in the field of a question a single day could answer.

---

## 8. Pre-mortem: how this actually dies

The corpus has pre-mortems (PMF01 PM1–PM5), and they are all about external
forces — competitive response, benchmark audits, buyer churn. Here is the one
that is missing, which is the one with the highest probability mass:

> **It is mid-2027. The project is dead. What happened?**
>
> Nobody outran us. Neo4j never responded, because there was nothing to respond
> to. LadybugDB kept shipping and quietly became the default answer for
> "graph algorithms without a big machine." The GraphRAG wave arrived and passed.
>
> There was no crisis. Every individual month was productive. The corpus reached
> forty documents and was, honestly, the best analysis of the graph-storage
> market anyone had written.
>
> **Cause of death: the load-bearing number was never measured, because measuring
> it risked ending the project, and writing about it did not.**

---

## 9. My actual position, with the uncertainty left in

**What I believe (re-scored after §3.2b):**

- The pain is real, the wall is real, and both are now vendor-documented. That
  part held up under adversarial verification and is not in doubt.
- **The window closed while the analysis was being written.** Not "is closing."
  Grafeo has been public for six months and has 707 stars. Slater launched four
  days ago with the exact architecture in `Arch05`/`Arch06`. Both are Rust, both
  permissive, both on Bolt. This is the cost of eight unrun experiments.
- The right artifact is still a Neo4j **plugin**, WCC first, parity by `diff` —
  and it is now *more* right, not less, because it is the only position none of
  the four entrants occupy: living inside a running Neo4j, no export, no
  migration, `gds.*` call shape, verifiable in one line.
- The pitch is **the finish plus the receipt** — and the receipt now has to carry
  real weight, because "off disk" and "low RAM" are no longer distinguishing.
  Enforced admission, printed bill, reject path. That is the last unclaimed ground.
- **This is a features position, not a moat position.** Anyone can add a receipt
  in a quarter. Plan accordingly: speed and narrowness, not defensibility.

**Where my advice is weak, and I will not pretend otherwise:**

- **Shipping the jar may produce indifference, not a verdict.** Forty stars, three
  issues, no workload owners — and indifference is unfalsifiable and demoralizing
  in a way a clean disproof is not. I am recommending exposure, not certainty.
- **Leg 3 might simply be false.** If the segment that cannot buy RAM is too small,
  the incumbent was right to deprioritize, and `simulation01.md` §12.3 already says
  so honestly. The staff quote — *"never had enough people ask"* — is real evidence
  in that direction and I am not going to explain it away.
- **The bet is on segment growth** (GraphRAG, local agents, regulated/air-gapped),
  which is a timing bet on someone else's wave arriving. Legitimate. But it is a
  bet, and it should be named as one rather than assumed as a premise.

**What would change my mind fastest — reordered, because the competitive question
now outranks everything else:**

```text
  -> Run Slater + Grafeo + Ladybug on a graph 5-10x RAM, THIS WEEK.
     If they hold algorithm state within bounds and answer Bolt correctly, the
     honest conclusion may be "contribute to one of them" rather than
     "build a fifth." That sentence should be sayable out loud.
  -> The estimator numbers come back small        => re-price the whole pitch
  -> Ten interviews say "we sampled and moved on" => pain is episodic, not budget-worthy
  -> Neo4j announces out-of-core GDS anywhere     => done
```

**And the option this corpus has never once considered, which now deserves a seat:**
Slater is Apache-2.0 and six weeks old. Its author is answering questions on HN
about Elias-Fano and superhub packing — the exact problems `Arch05` X2 was
scheduled to spike. There is a real possibility that the highest-leverage move
available is not a fifth engine but **the receipt/admission layer, contributed
where the engine already exists.** That is a smaller, less romantic project. It is
also the one with a live user base and no zero-to-one risk. It should be evaluated
on the merits, not dismissed because it isn't ours.

**The one-line verdict:**

> The thesis deserves a binary, not another document. The capability on display in
> this folder is real — it is just all pointed at the half of the problem that has
> no feedback loop.

**And the receipt on that claim, delivered the same day it was written:**

```text
  110 agents, 6.09M tokens, 1,952 tool calls, 97 minutes
    -> hardened the leg that was already strongest
    -> MISSED both live competitors

  4 curl calls to HN Algolia + gh, ~20 minutes, ~$0
    -> found Slater (4 days old) and Grafeo (707 stars, 6 months old)
    -> i.e. found that the moat was gone

  The corpus's §12 conclusion -- "the turf is empty because everyone who could
  build it is paid/scored/promoted/funded not to" -- was TRUE WHEN WRITTEN and
  is FALSE NOW. Two solo builders in Rust did not care about any of those
  incentives. That was always the hole in the argument: it enumerated why
  INSTITUTIONS wouldn't, and concluded no ONE would.
```

This is the pattern named in §6, arriving on schedule. Eight scheduled experiments
went unrun for three months; the market moved inside that window; and the thing
that finally detected it was twenty minutes of API calls, not another document.

**If one line survives from this whole folder, make it this one:** the cheapest
available act was always the one being avoided.

---

## 10. Cross-references

```text
evidence01.md ....... the verified facts behind every claim above (URL-cited)
simulation01.md ..... practitioner corpus; §§3, 6.3, 7.8, 12.3 are its best parts
                      §8 badge #1, §11.1, §7.5 F1/F2, §10.2(f) now need edits
gtm-POC-01.md ....... THE PLAN. The one document here that is already correct.
Arch02.md ........... R2 (Louvain+NodeSim = STATE problems) = why Ladybug's gap matters
Arch06.md ........... L2 (RAM = O(V) by construction) = the real engineering idea,
                      buried as one lever among four when it is the thesis
Arch04.md ........... BUG-1 (greenfield fallacy) + Timeline A = the soundest
                      operational conclusion in the architecture line; never executed
Reference-Learning-Critique-Gaps.md .. already diagnosed the scope error in §3-4
prd-l1.md ........... needs its scope rewritten; everything downstream re-prices free
```

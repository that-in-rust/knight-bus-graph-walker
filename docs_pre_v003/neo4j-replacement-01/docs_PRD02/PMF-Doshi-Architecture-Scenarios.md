# Shreyas Doshi PMF Lens: The "Just Rewrite in Rust" Architecture

*Timeline Traverser scenarios for Knight Bus's product-market fit,
analyzed through Doshi's Customer Problem Stack Rank, LNO
framework, and the three PMF archetypes (Hair on Fire, Hard Fact,
Future Vision). Grounded in real-world adoption stories: DuckDB,
Turso/Limbo, TETRA, Memgraph.*

---

## The Doshi Foundation: Customer Problem Stack Rank

Shreyas Doshi's central PMF insight (developed at Stripe, Twitter,
Google) is: **most products fail not because they don't solve a
problem, but because they solve a problem that isn't in the
customer's TOP THREE priority list.**

The "destined to fail" pattern:
1. PM talks to customers: "Does this solve your problem?" → "Yes!"
2. PM builds the product → launches
3. No adoption
4. Why? The problem was real, but it was #7 on the customer's list.
   They'll never switch for problem #7.

**The test:** Would a Neo4j user SWITCH to Knight Bus? Not "would
they like it?" but "would they actually DO the work of switching?"

Switching a database means:
- Rewriting connection strings (easy: minutes)
- Validating Cypher compatibility (hard: days to weeks)
- Re-testing all queries (painful: weeks)
- Retraining team (expensive: weeks)
- Migrating data (risky: days)
- Trusting a new system in production (terrifying: months)

**A customer will only pay this switching cost if you solve a
problem that is #1 or #2 on their list.** Not #5, not "nice to
have," not "interesting."

---

## What Is the Customer's Problem Stack Rank?

From our research (PMF-RAM-vs-Latency-Doshi.md), the Neo4j
analytics user's stack rank is:

```
#1: "I can't run my workload — OOM or too expensive" (RAM/Cost)
    Evidence: NASA switched. TETRA leads with "10x less RAM."
    Multiple SO/forum posts begging for help with OOM.
    
#2: "Algorithms take too long" (Latency)
    Evidence: GDS projection time (60-100 sec), PageRank slow.
    But users don't SWITCH for this — they optimize queries.
    
#3: "Migration is painful / lock-in" (Switching cost)
    Evidence: AWS Neptune migration guide = multi-week effort.
    Cypher compatibility is the #1 migration concern.
    
#4: "I need more algorithms / features" (Features)
    Evidence: GDS has 60+ algorithms. Users want coverage.
    
#5: "I need multi-node / clustering" (Scale)
    Evidence: Enterprise-only feature. Most users are single-node.
```

**Knight Bus's "just rewrite in Rust" solves #1 and #2
simultaneously.** But does it solve #3 (switching cost)?

---

## The Three PMF Archetypes Applied

### Archetype A: Hair on Fire 🔥

**"The house is burning. They'll use a garden hose if that's
what's available."**

> The customer is actively looking for a solution. The market
> is crowded. You must be DIFFERENT, not just better.
> — Sequoia Arc Framework

**Does Knight Bus fit Hair on Fire?**

YES, for this segment:
- Users who are RIGHT NOW hitting OOM on Neo4j GDS projections
- Users who just got their Neo4j Aura bill and it's $4,000/mo
- Users who were told "upgrade to a bigger instance" and can't

These users are searching Google for "Neo4j alternative cheaper"
right now. They'll find TETRA, Memgraph, FalkorDB, KuzuDB.

**The problem: Knight Bus is the 5th entrant in a crowded lane.**

TETRA claims 10x less RAM. Memgraph won NASA. FalkorDB has
GraphBLAS speed. KuzuDB (via Vela fork) claims 374x faster.
Another "less RAM" claim won't turn heads.

**Doshi's guidance for Hair on Fire:** You can't just be faster
or cheaper. You need a *truly differentiated customer experience*
to have a durable advantage.

**What would make Knight Bus DIFFERENT, not just better?**
→ Algorithm-specific layouts (the Atlas). Nobody else has this.
→ Auto-adaptive (OLAP-RAM on laptop, OLAP-Latency on server).
→ Zero-projection architecture (CSR IS the store, no copy step).

### Archetype B: Hard Fact 📐

**"Everyone accepts this pain as normal. You show them it doesn't
have to be."**

> The customer isn't looking for a solution because they don't
> know one is possible. When you show them, they're incredulous:
> "Why hasn't anyone done this before?"

**Does Knight Bus fit Hard Fact?**

YES, for this realization:
- "Wait — you're telling me I don't NEED a projection step?"
- "The database stores data in the shape the algorithm wants?"
- "PageRank runs in 3 seconds, not 3 minutes, on the SAME data?"

Users have ACCEPTED that GDS projections take 60-100 seconds.
They've accepted that running PageRank requires 4 GB of heap.
They've accepted that you need a separate analytics engine.
They don't know it can be different.

**This is where "just rewrite in Rust" creates a PMF moment.**

Not "Rust is faster" (boring, incremental).
Not "we use less RAM" (TETRA already says this).
But: "we eliminated the entire projection step. The storage
format IS the analytics format. Every algorithm runs directly
on the stored data with zero copy."

**That's a Hard Fact moment.** The customer's reaction:
"Why did Neo4j ever store data in linked lists?"

### Archetype C: Future Vision 🔮

**"A world that doesn't exist yet. You need the customer to
believe in a different future."**

> You're creating demand, not capturing it. This requires
> vision and patience. The market may not exist yet.

**Does Knight Bus fit Future Vision?**

PARTIALLY, for the full vision:
- OLTP + OLAP split with automatic WAL sync
- 13 algorithm-specific storage layouts
- Auto-adaptive engine selection based on hardware
- Full Cypher/Bolt compatibility with Neo4j drivers
- "The graph database for the post-Neo4j era"

But Future Vision is DANGEROUS for a v0.0.3. It requires
massive investment before any validation. DuckDB started as
a research project and took 5 years to reach production.
Turso/Limbo's SQLite rewrite is 1 year in and still pre-1.0.

**Future Vision is the v1.0 story, not the v0.0.3 story.**

---

## Decision Frame

- **Fork in the road:** How should Knight Bus POSITION itself
  for PMF? Three positioning strategies lead to three different
  timelines.

- **Desired outcome:** First 100 users who actually run Knight
  Bus on their data. Not stars, not likes — actual usage.

- **Hard constraints:**
  - Single developer
  - 4,710 LOC existing code
  - No Cypher parser yet (key lookup + traversal only)
  - No Bolt protocol yet
  - v0.0.3 scope: PageRank + benchmark

- **Time horizon:** 6 months to first real users.

- **What would count as failure:**
  - Building for 6 months with zero external users
  - Building the wrong thing (great tech, no adoption)
  - Spending effort on positioning that attracts the wrong
    segment (tire-kickers, not power users)

---

## Timeline A: "The DuckDB Play" — Embedded Analytics Accelerator

### The Analogy

DuckDB's PMF story is the closest precedent:

```
DuckDB:                          Knight Bus:
─────────────────────────────    ─────────────────────────────
"SQLite for analytics"           "SQLite for graph analytics"
Embedded, in-process             Embedded, in-process
Columnar (vs SQLite row-store)   CSR (vs Neo4j linked-list)
No server needed                 No server needed
pip install duckdb               cargo add knight-bus
Sub-second on laptop             Sub-second on laptop (goal)
```

DuckDB's adoption path:
1. **Year 1-2:** Research project. "Why DuckDB?" page. No users.
2. **Year 3:** pip install duckdb. Data scientists try it.
3. **Year 4:** ClickBench #1. "Holy shit it's fast." Viral.
4. **Year 5:** 30K GitHub stars. Definite migrates from Snowflake.
   MotherDuck raises $100M+. Stack Overflow: 3.3% adoption.

**DuckDB's PMF was Hard Fact:** "You don't need Snowflake for
most analytics queries. Your laptop is enough." People didn't
know this was possible. When shown, they were incredulous.

### Knight Bus as "DuckDB for Graphs"

**Opening move:** Position as an embedded graph analytics engine.
Not a database replacement — a LIBRARY.

```rust
// v0.0.3 usage:
let runtime = MmapWalkRuntime::open("/path/to/snapshot")?;
let scores = page_rank(&runtime, &PageRankConfig::default())?;
println!("Top node: {} (score: {:.4})", scores.top(1)[0].key, scores.top(1)[0].score);
```

**Week 1:** Ship `knight-bus pagerank` CLI + Rust library API.
Publish benchmark: "PageRank on 100M edges: 3 sec, 165 MB."

**Month 1:** Write "Why Knight Bus?" page (modeled on DuckDB's).
Core message: "Graph analytics shouldn't require a running
database server. Your graph, your laptop, your results."

**Quarter 1:** Add 3-5 more algorithms (Dijkstra, BFS, connected
components, triangle count). Each one inherits the CSR advantage.
Users discover it via: "Neo4j alternative embedded" searches.

**Quarter 2:** Python bindings (`pip install knight-bus`). This
is where DuckDB's adoption exploded. Data scientists don't
use Rust — they use Python. `import knight_bus` is the gateway.

**Long-term shape:** The go-to embedded graph analytics library.
Not competing with Neo4j as a database — competing with Neo4j
GDS as an analytics engine. Lower switching cost because you're
not replacing Neo4j, you're supplementing it.

### PMF Archetype: Hard Fact

"You don't need Neo4j GDS running in a JVM heap. You can run
PageRank on your laptop in 3 seconds from a binary file."

### Doshi Customer Problem Stack Rank Position: #2

Solves: "Algorithms take too long" + partially solves #1
("too expensive" because you don't need a Neo4j Aura instance
for analytics).

### Likelihood: 70%

DuckDB proves the pattern works for analytics databases. But
graph analytics is a smaller market than SQL analytics.

### Stress Points

- Month 2: "But I need to keep my data in Neo4j for OLTP. How
  do I get it into Knight Bus?" → Need an import pipeline
  (CSV export → KB snapshot build). This is friction.
- Month 3: "Can I run this on a graph that's being updated?"
  → No, snapshots are immutable. This limits the use case to
  batch analytics, not real-time.
- Quarter 2: Python bindings are table stakes but significant
  engineering (PyO3 + maturin + packaging).

### Inflection Points

- **If Python bindings ship:** Data scientist adoption unlocks.
  This is where DuckDB's hockey stick started.
- **If 5+ algorithms ship:** Users stop thinking "PageRank tool"
  and start thinking "graph analytics engine."
- **If someone writes a blog post:** "I replaced Neo4j GDS with
  Knight Bus and it's 20x faster" → viral moment.

---

## Timeline B: "The TETRA Play" — Direct Neo4j Replacement

### The Analogy

TETRA's PMF story:
```
TETRA:                           Knight Bus:
─────────────────────────────    ─────────────────────────────
Full openCypher compliance       No Cypher parser yet
Bolt protocol                    No Bolt yet
"10x less RAM, $299/mo"          "10-30x less RAM, open source"
Neo4j drop-in replacement        Not drop-in (no Cypher/Bolt)
Go + Rust + WASM                 Pure Rust
Shipping product                 Research prototype
```

### Knight Bus as Neo4j Replacement

**Opening move:** Build toward full Cypher/Bolt compatibility.
v0.0.3 adds PageRank, but the REAL work is Cypher parser + Bolt
server. Position as: "Same Cypher, same drivers, 10x less RAM,
open source."

**Week 1-2:** Ship PageRank + benchmark (same as Timeline A).

**Month 1-3:** Build Cypher parser (MASSIVE effort, 50-100K LOC
based on Neo4j's parser being 182K LOC). Subset first: MATCH,
WHERE, RETURN, CREATE, SET, DELETE.

**Quarter 1-2:** Bolt protocol server. Neo4j drivers connect to
Knight Bus. First "change one connection string" moment.

**Quarter 3-4:** GDS algorithm equivalents (PageRank, Dijkstra,
community detection). Users can run `CALL gds.pageRank(...)`.

**Long-term shape:** Open-source Neo4j replacement. Competes
directly with TETRA, Memgraph, FalkorDB on features + cost.

### PMF Archetype: Hair on Fire

"Neo4j is too expensive. Switch to Knight Bus — same Cypher,
same drivers, 10x less RAM, open source."

### Doshi Customer Problem Stack Rank Position: #1

Solves: "OOM / too expensive" (the #1 pain). But ALSO needs to
solve #3 ("migration is painful") by being Cypher-compatible.

### Likelihood: 35%

The Cypher parser alone is a multi-month, multi-person effort.
openCypher spec is complex (CTEs, aggregations, path patterns,
subqueries, CASE expressions, list comprehensions...). Building
this as a single developer is extremely ambitious.

### Stress Points

- Month 2: "The Cypher parser is 10x harder than expected."
  Neo4j's Cypher parser is 182K LOC. Even a subset (MATCH +
  WHERE + RETURN) is 10-20K LOC with proper error handling.
- Month 4: "TETRA already does this. Why would anyone choose
  us over a shipping product?" → Open source is the differentiator,
  but it's a weak one for enterprise buyers who want support.
- Quarter 2: "We're 6 months in and still can't run most Cypher
  queries." → Risk of the Doshi "destined to fail" pattern:
  "We just need features X, Y, Z."

### Inflection Points

- **If Cypher subset works:** "I ran my Neo4j queries on Knight
  Bus without changing them" → magic moment.
- **If Cypher subset doesn't work:** Stuck in "almost compatible"
  purgatory. Users try it, hit an unsupported query, leave.
  This is the #1 risk.
- **Bolt compatibility:** The moment Neo4j drivers connect to
  Knight Bus is a massive milestone. But Bolt v5 is complex
  (chunked messages, handshake negotiation, transaction state
  machine).

---

## Timeline C: "The Research Paper Play" — Algorithm Innovation

### The Analogy

No direct analogy — this is creating a new category.

Closest: GraphBLAS (graph algorithms as linear algebra),
Gunrock (GPU graph analytics), Ligra (shared-memory parallel
graph framework). All are academic projects with niche but
passionate user bases.

### Knight Bus as Algorithm Innovation Platform

**Opening move:** Position on the Algorithm Storage Atlas.
The thesis: "Store the graph in the shape the algorithm wants
to walk." This is a genuinely novel idea that no commercial
graph database implements.

**Week 1-2:** Ship PageRank with `InboundPower` layout (reverse
CSR with pre-computed in-degrees). Show that layout-aware
storage gives measurable speedup over generic CSR.

**Month 1:** Ship 3 layouts: InboundPower (PageRank),
RelaxationFrontier (Dijkstra), OrderedWedge (Triangle Count).
Benchmark each against generic CSR.

**Quarter 1:** Publish a technical blog or paper: "Algorithm-
Specific Graph Storage: Why Your Graph Database Is 100x Slower
Than It Should Be." Demonstrate that layout selection gives
10-100x speedup across different algorithm families.

**Quarter 2:** Build the auto-layout-selector: given a query
workload, Knight Bus automatically builds the optimal layout.
"You tell us what you want to run, we store the graph in the
right shape."

**Long-term shape:** The citation-worthy graph analytics engine.
Adopted by researchers, data scientists, and algorithm engineers
who care about performance. Not a database replacement — a
performance research platform that becomes production-grade.

### PMF Archetype: Future Vision / Hard Fact hybrid

"Graph databases store data in one format and then transform it
for every algorithm. What if the storage format WAS the
algorithm's optimal input?"

### Doshi Customer Problem Stack Rank Position: #2-#4

Solves: "Algorithms take too long" (#2). But most users don't
know they have this problem — they've accepted slow algorithms.

### Likelihood: 50%

Academic credibility is achievable. Production adoption is
harder — researchers use frameworks, not databases.

### Stress Points

- Month 1: "Layout-specific storage means multiple copies of
  the graph on disk." For 13 layouts × 10 GB graph = 130 GB
  disk. Users with large graphs won't want this.
- Month 2: "Which layout do I use?" Decision fatigue. The
  auto-selector is the answer but it's the hardest part.
- Quarter 1: "This is cool research but I still can't use it
  in production." → Need Cypher/Bolt eventually anyway.

### Inflection Points

- **If auto-selector works:** "It just picks the right layout"
  → DuckDB-style "it just works" moment.
- **If benchmarks show 100x:** Viral on HN/Reddit. Research
  communities adopt quickly.
- **If only 2-5x:** "Interesting but not worth the complexity"
  → dead end for the layout thesis.

---

## Timeline D: "The Composable Play" — Library + CLI + Eventually Server

### The Synthesis

What if you don't choose one archetype? What if you BUILD for
Hard Fact but MARKET for Hair on Fire?

```
v0.0.3: Library + CLI                    (Hard Fact — "look what's possible")
v0.0.4: Python bindings                  (Hard Fact — "use it from your notebook")
v0.0.5: Import from Neo4j (Bolt stream)  (Hair on Fire — "switch is easy")
v0.1.0: Basic Cypher subset + Bolt       (Hair on Fire — "drop-in analytics sidecar")
v0.2.0: Algorithm-specific layouts       (Future Vision — "algorithm-aware storage")
v1.0.0: Full OLTP/OLAP split            (Future Vision — "the Neo4j replacement")
```

**This is the DuckDB path.** DuckDB started as a library
(`import duckdb`), grew into a CLI, then added a server mode
(Quack protocol), then MotherDuck added cloud. Each step
validated PMF before the next step was built.

### Opening move

Same as Timeline A: ship `knight-bus pagerank` with measured
benchmark. Position as library/CLI.

### Month 1-2

Validate: do people actually download it and run it on their
data? If yes → continue. If no → something is wrong with the
positioning, investigate before building more.

### Month 3-6

Based on usage signals:
- If data scientists use it → Python bindings (Timeline A path)
- If Neo4j users use it → Cypher subset (Timeline B path)
- If researchers use it → More algorithms (Timeline C path)

**Let the users tell you which timeline is right.**

### PMF Archetype: Start Hard Fact, pivot based on signal

### Doshi CPSR: Validate the stack rank with actual behavior

**This is the Doshi-correct approach.** Don't guess the
customer's problem stack rank — OBSERVE it through usage
patterns. Ship the minimum viable product, watch who shows up,
then build for those people.

### Likelihood: 75%

Highest likelihood because it's adaptive. But requires
discipline to NOT build ahead of signal.

### Stress Points

- Month 1: "Nobody downloaded it." → Was the positioning wrong
  or the distribution wrong? Need to distinguish.
- Month 3: "Three different user segments want three different
  things." → Choice paralysis. Must pick one segment and go
  deep.
- Month 6: "We've been in 'library mode' for 6 months and
  haven't started the Cypher parser." → Pressure to build
  the "real product" before validating.

### Inflection Points

- **First external user who reports a benchmark:** This is the
  signal. WHO they are and WHAT they benchmarked tells you
  which timeline to pursue.
- **First GitHub issue that ISN'T a bug:** "Can you add
  Dijkstra?" → they want more algorithms (Timeline C). "Can
  I connect with my Neo4j driver?" → they want compatibility
  (Timeline B). "Can I use this from Python?" → they want
  accessibility (Timeline A).

---

## Cross-Timeline Analysis

| | A: DuckDB Play | B: TETRA Play | C: Research Play | D: Composable Play |
|---|---|---|---|---|
| **PMF archetype** | Hard Fact | Hair on Fire | Future Vision | Adaptive |
| **First user value** | 2 weeks | 3-6 months | 1-2 months | 2 weeks |
| **Switching cost for user** | **Low** (additive) | **High** (replacement) | **Medium** (new tool) | **Low → Medium** |
| **Competition** | Light (no "DuckDB for graphs") | Heavy (TETRA, Memgraph) | Light (academic niche) | Depends on path |
| **Engineering effort to v0.1** | **Low** (~2K LOC) | **Very high** (~50K LOC) | **Medium** (~5K LOC) | **Low** (adaptive) |
| **Revenue potential** | Medium (library → SaaS) | High (enterprise DB) | Low (research) | Medium → High |
| **Moat depth** | Medium (CSR is replicable) | Low (feature parity) | **High** (13 layouts) | Grows over time |
| **Risk of "destined to fail"** | **Low** | **High** | Medium | **Lowest** |

| Path | Upside | Downside | Reversibility | Regret risk |
|---|---|---|---|---|
| **A: DuckDB** | First mover in "embedded graph analytics" | Small market? | **High** (can always add server later) | "Should've built the database" |
| **B: TETRA** | Large market (Neo4j replacement) | Years of work, may never reach parity | Low (Cypher parser is a one-way door) | "Spent 2 years on parser, nobody cared" |
| **C: Research** | Novel, defensible, publishable | Niche audience, hard to monetize | Medium (algorithms are reusable) | "Interesting but irrelevant" |
| **D: Composable** | Can't be wrong (adaptive) | Slow (no commitment = no momentum) | **Highest** | "We tried everything, mastered nothing" |

---

## The Doshi Verdict: LNO Analysis

Shreyas Doshi's LNO (Leverage, Neutral, Overhead) framework
asks: for each unit of effort, what creates the most value?

### Leverage work (do MORE of this):

1. **The benchmark.** One compelling "PageRank: 3 sec, 165 MB vs
   Neo4j: 120 sec, 4 GB" number is worth more than 10K LOC of
   features. This is the highest-leverage artifact you can create.

2. **The "Why Knight Bus?" page.** DuckDB's "Why DuckDB?" page is
   their most-linked document. It explains the thesis simply.
   Knight Bus needs this.

3. **Python bindings.** Every data scientist is a potential user.
   `pip install knight-bus` unlocks an audience 100x larger than
   `cargo add knight-bus`.

### Neutral work (do this but don't over-invest):

1. More algorithms (after PageRank). Each one is incrementally
   valuable but not transformative.
2. CLI polish. Important but not a PMF driver.

### Overhead work (do LESS of this):

1. **Cypher parser.** Months of work before any user value.
   High risk of "almost compatible" purgatory. This is the
   single biggest overhead risk in the project.

2. **Bolt protocol.** Same issue — weeks of work for protocol
   compliance before any user benefit.

3. **Algorithm-specific layouts.** Cool research but not what
   gets first users. Save for v0.2+.

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline D: Composable Play** (starting with A's opening move).

Ship the library + CLI with PageRank. Measure who shows up.
Build for them. This is the DuckDB path — it worked for the
most successful analytics database of the decade.

But with a SPECIFIC starting position: **Timeline A's opening
move.** "Embedded graph analytics engine" is the positioning.
Not a database replacement (too ambitious). Not a research
project (too niche). An analytics engine.

### Which path is safest if things go badly?

**Timeline A: DuckDB Play.**

If nobody adopts Knight Bus as an embedded analytics engine,
you still have a useful Rust library. It cost 2 weeks to build.
You can always pivot to Timeline B (add Cypher) or C (research).

If nobody adopts Knight Bus as a Neo4j replacement (Timeline B),
you've spent 6 months on a Cypher parser and Bolt server that
nobody wants. That's a much more expensive failure.

### What would reduce uncertainty fastest?

**Ship v0.0.3 with PageRank + benchmark. Then watch.**

The FASTEST uncertainty reducer is not more analysis — it's
shipping. One week of coding + one week of measuring + one
blog post = you learn whether ANYONE cares.

Specifically, watch for:
1. **Who downloads it?** (data scientists, DBAs, researchers?)
2. **What do they benchmark?** (their own data or the synthetic?)
3. **What do they ask for?** (algorithms, Cypher, Python, speed?)

These signals tell you which timeline to commit to.

---

## The One-Liner

### Doshi's Customer Problem Stack Rank applied:

> **Don't build a database replacement. Build an analytics
> accelerator. Let the users pull you toward the database
> replacement if that's where the demand is.**

### In Doshi's language:

> "Knight Bus v0.0.3 should solve problem #2 (algorithms are
> slow) for a segment that ALSO has problem #1 (Neo4j is too
> expensive). The 'just rewrite in Rust' architecture solves
> both simultaneously: CSR eliminates the projection step
> (faster algorithms) AND eliminates the JVM heap overhead
> (less RAM = less cost). Ship the benchmark. Watch who comes."

### In DuckDB's language:

> "We're not building a graph database. We're building an
> embeddable graph analytics engine. If it turns out the world
> wants a graph database, we'll build that too. But we'll
> know because users will tell us."

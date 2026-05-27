# Timeline Traverser: Storage Architecture Risk

*Generated using the Timeline Traverser playbook.*
*Thinking like Shreyas Doshi: LNO framework (Leverage, Neutral, Overhead),
pre-mortem thinking, opportunity cost over ROI, and "most execution problems
are actually strategy problems."*

---

## Decision Frame

- **Fork in the road:** You have a proven read-only CSR engine (4,710 LOC).
  You want to make it a Neo4j replacement. The open question: how do you
  handle writes, and how much database machinery do you actually need?

- **Desired outcome:** A system that Neo4j analytics users can switch to
  with zero learning curve, that is measurably faster for graph algorithms,
  and that doesn't lose committed data.

- **Hard constraints:**
  - Frontend = flawlessly like Neo4j (Cypher, Bolt, drivers, errors)
  - Must handle 50GB graphs
  - Single-node v1 (no clustering)
  - Team has deep Rust skills, limited database internals experience
  - Low write rate target (<1000 mutations/minute)
  - Read-heavy analytics workload (PageRank, shortest paths, community detection)

- **Time horizon:** Week 1 → Month 1 → Quarter 1 → Year 1

- **What would count as failure:**
  - Losing committed data (data corruption)
  - Serving inconsistent reads (partial writes visible)
  - Taking 2+ years and still not usable
  - Building something nobody switches to

### Shreyas Doshi Lens: What KIND of Work Is This?

Using the LNO framework:

- **Leverage work** (10x impact): Getting the storage architecture right.
  This one decision cascades through everything. Getting it wrong wastes
  months. Getting it right unlocks the 10-100x read speedup for users.

- **Neutral work** (must be done, expected quality): Bolt protocol, Cypher
  parser, error codes, CLI, configuration. Copy-work from Neo4j. Must be
  done well but doesn't differentiate.

- **Overhead work** (minimize): Build system, CI/CD, packaging, docs
  website. Necessary but not where value is created.

The storage architecture decision is **pure Leverage work.** Everything
else follows from it. Shreyas would say: "Spend 80% of your creative
energy here. Do not distribute effort evenly."

---

## Timeline A: "Read-Only Accelerator"

*Don't build a database at all. Ship a read-only sidecar that makes
Neo4j analytics faster. Avoid every database complication entirely.*

### Opening Move

Accept that Knight Bus is NOT a database and stop trying to make it one.
Position it as: "Keep Neo4j for your data. Use Knight Bus for your
algorithms. 100x faster PageRank, shortest paths, community detection."

Users export from Neo4j (CSV or Bolt streaming), build a Knight Bus
snapshot, query it through Bolt with Cypher. Read-only. No writes. If
the data changes, re-export and rebuild.

### Week 1

- Stub Bolt v4 server (read-only, reject all write queries)
- Stub Cypher parser for 5 read patterns:
  `MATCH (n) RETURN n`, `MATCH (n {id:$id})`, `MATCH (n)-[:R]->(m)`,
  `MATCH (n)-[:R*1..2]->(m)`, `RETURN count(*)`
- `knrt import --from-csv export/` → builds AnchorDualCsr snapshot
- LOC: ~3-5K new (on top of existing 4,710)

### Month 1

- Bolt v4 working: Neo4j Python/Java/JS drivers connect successfully
- 10-15 Cypher read patterns supported
- Import from Neo4j CSV export working
- First benchmark: same traversal query, Neo4j vs Knight Bus
- LOC: ~12-18K total
- **Stress:** Low. Clear scope. No database complications.
- **Daily reality:** Writing a protocol implementation and a parser.
  Familiar Rust work. No need to learn database internals.

### Quarter 1

- Cypher read coverage: ~40-50% of common patterns
- 2-3 Atlas layout families beyond AnchorDualCsr
- PageRank benchmark published (InboundPowerLayout)
- `knrt import --from-bolt bolt://neo4j:7687` (stream import)
- LOC: ~30-40K total
- **Stress:** Medium. Users start asking "can I write to it?" and
  you have to say no. Some users love it anyway (analytics teams).
  Some bounce ("I need a full database").

### Year 1

- Cypher read coverage: ~60-70%
- 5-7 Atlas layout families
- Proven 10-100x on 5+ algorithm benchmarks
- Incremental import (watch Neo4j changes, rebuild affected snapshot
  regions)
- LOC: ~60-80K total
- **What you have:** A genuinely useful analytics accelerator that some
  teams adopt alongside Neo4j
- **What you DON'T have:** Any write support. It's an accelerator,
  not a replacement.

### Long-term Shape

A niche product for graph analytics teams who already use Neo4j.
Modest adoption. Strong in benchmarks. But the ceiling is limited
because it's a companion, not a replacement. You never solved the
"database problems" because you never needed to.

### Likelihood: 85% chance of shipping something useful by Month 3

### Stress Points

- Month 3: "Users want writes. Are we building a product or a demo?"
- Month 6: "We're a nice-to-have, not a must-have. Hard to grow."
- Year 1: "We proved the thesis but we're still a sidecar."

### Inflection Points

- If analytics teams adopt it without needing writes, the product
  succeeds as a category (graph analytics accelerator)
- If everyone asks for writes, you're forced to add them anyway — but
  now you've delayed by 6-12 months

---

## Timeline B: "Append-Log Analytics Engine"

*Accept writes, but through an append-log + periodic recompile model.
Like ClickHouse's MergeTree: immutable parts, background merges,
eventually consistent reads.*

### The Key Insight: This IS a Real Architecture

Research confirms: this is not a made-up pattern. Real systems use it:

- **ClickHouse MergeTree:** Inserts create sorted, immutable data parts.
  Background merges combine parts. No in-place updates. Queries scan
  all relevant parts. This architecture handles petabytes at Yandex,
  Cloudflare, and Uber.

- **DuckDB:** WAL for crash safety, periodic checkpoint flushes WAL
  into columnar row groups. Single-file database. Full ACID via MVCC.
  Aimed at analytics, not OLTP.

- **Apache Kudu:** Immutable base rowsets + delta stores for mutations.
  Background compaction merges deltas into base. Designed explicitly
  for analytics workloads with modest write rates.

The pattern: **writes go to a fast append-only structure, reads go
to optimized immutable structures, background process merges them.**

This is exactly what Knight Bus could do with CSR snapshots.

### Opening Move

Design a two-layer storage model:

```text
Layer 1: Append Log (mutation journal)
  - CREATE, SET, DELETE operations appended to a WAL-like log
  - Fast writes (sequential I/O only)
  - Crash-safe (fsync on commit)

Layer 2: CSR Snapshot (optimized read layer)
  - Built from Layer 1 + previous snapshot
  - Immutable once built
  - mmap'd for reads (proven fast)
  - Background rebuild when log reaches threshold
```

Query path:

```text
Query arrives via Bolt
  → Check: is this a write? → append to log, return success
  → Check: is this a read? → query latest CSR snapshot
  → Snapshot might be slightly stale (seconds, not minutes)
```

### Week 1

- Same as Timeline A Week 1 (Bolt + Cypher parser + import)
- PLUS: Design the append log format (what does a CREATE entry look like?)
- PLUS: Stub `MutationLog` trait

### Month 1

- Bolt v4 working (reads AND writes accepted)
- Append log: CREATE, SET, DELETE mutations persisted to disk
- Background snapshot rebuilder: every 30 seconds, rebuild CSR from
  (previous snapshot + new mutations)
- Reads served from latest complete snapshot
- LOC: ~18-25K total
- **Stress:** Medium. The append log is simple. The tricky part is:
  what happens when a user writes and then immediately reads? They
  might not see their own write for up to 30 seconds. This is
  "eventual consistency" and some users won't like it.
- **Daily reality:** You're writing a WAL (well-understood), a
  CSR snapshot builder (already exists), and a merge scheduler
  (new but straightforward).

### Quarter 1

- Append log with fsync-on-commit (crash-safe)
- Snapshot rebuild time: <10 seconds for graphs <5M edges
- Read-after-write guarantee: option to force snapshot rebuild on
  demand (slow but correct)
- Basic transaction support: BEGIN/COMMIT/ROLLBACK on the append log
- Property storage in snapshots (PropertyPlane concept)
- LOC: ~40-55K total
- **Stress:** Medium-High. The "staleness window" (time between write
  and read-visibility) is the main UX problem. Some users accept it.
  Some don't. You're explaining a new mental model.

### Year 1

- Staleness window: <5 seconds for graphs <10M edges
- Incremental rebuild: only recompile affected CSR regions, not whole graph
- Full Cypher read/write coverage: ~60%
- 5-7 Atlas layout families
- Proven benchmarks: 10-100x reads, "normal" write speed
- LOC: ~80-110K total
- **What you have:** A working analytics database with a novel write model
- **What you DON'T have:** Sub-second write-to-read latency. Real-time
  OLTP capability. Multi-node.

### Long-term Shape

An honest graph analytics engine. Not trying to be Neo4j for everything.
Killer fast reads. Acceptable writes for analytics workloads. The write
model is "ClickHouse-like" — familiar to analytics engineers, unfamiliar
to OLTP engineers.

### Likelihood: 65% chance of shipping something useful by Month 3

### Stress Points

- Month 2: "The staleness window confuses users. They write a node and
  can't query it for 10 seconds. How do we explain this?"
- Month 4: "Snapshot rebuild time grows with graph size. At 50GB, rebuild
  takes 30-60 seconds. Is that acceptable?"
- Month 6: "Some users want immediate consistency. Do we add a mutable
  overlay (moving toward Timeline C)?"

### Inflection Points

- If incremental rebuild gets fast enough (<5 seconds for 50GB), the
  staleness window becomes invisible and this architecture wins
- If rebuild stays slow (>30 seconds for 50GB), users push for a mutable
  overlay and you end up in Timeline C anyway
- If you can prove that "slightly stale reads" are fine for analytics
  (and they are — PageRank doesn't need up-to-the-second data), the
  model is very strong

---

## Timeline C: "Dual-Engine Hybrid"

*Mutable store for writes (immediate visibility) + CSR snapshots for
analytics (bulk speed). Like Apache Kudu: base rowsets + delta stores.*

### Opening Move

Accept the complexity of two storage engines. Build a simple mutable
store (not Neo4j's full record store — something much lighter) for
writes. Periodically compile the mutable store into CSR snapshots for
reads. Queries check both: recent writes from mutable store, bulk data
from CSR.

```text
Write path:   Bolt → Cypher → MutableStore (immediate)
Read path:    Bolt → Cypher → MutableStore ∪ CsrSnapshot (merged)
Background:   MutableStore → CsrSnapshot (periodic compaction)
```

### Week 1

- Same Bolt + Cypher stubs
- Design MutableStore: what's the simplest data structure that can
  store nodes, relationships, and properties with immediate read-back?
- Option: in-memory `HashMap<NodeId, Node>` + WAL for durability

### Month 1

- Bolt v4 working (reads and writes)
- MutableStore: in-memory hash maps + WAL persistence
- Reads check MutableStore first, then CSR snapshot
- No staleness — writes are immediately visible
- LOC: ~20-28K total
- **Stress:** Medium-High. You're maintaining two data structures and
  merging their results. Every query needs to check both. Edge cases
  emerge: what if a node exists in the mutable store but its
  relationships are in the CSR? What about deletes?

### Quarter 1

- MutableStore with MVCC (concurrent readers/writers)
- Background compaction: merge MutableStore into new CSR snapshot
- Transaction support (BEGIN/COMMIT/ROLLBACK)
- Consistency: reads see a consistent snapshot (MVCC timestamp)
- LOC: ~50-65K total
- **Stress:** HIGH. MVCC is genuinely hard. Concurrent access to the
  mutable store needs careful locking or lock-free structures. The
  merge logic (MutableStore ∪ CsrSnapshot) has subtle correctness
  requirements.
- **Daily reality:** You're debugging concurrency bugs, which is
  the hardest kind of systems programming.

### Year 1

- Full dual-engine working
- MVCC with snapshot isolation
- Background compaction every N seconds
- Write-after-read consistency (readers see their own writes)
- LOC: ~100-140K total
- **What you have:** A real database. Immediate writes. Fast reads.
- **What you DON'T have:** Simplicity. The dual-engine model is
  permanently complex. Every feature needs to work on both paths.

### Long-term Shape

The most capable system but also the most complex. You've essentially
built a small database engine. The CSR snapshots give you the read
speed advantage. The mutable store gives you write compatibility.
But the complexity tax is permanent — you're maintaining two storage
engines forever.

### Likelihood: 45% chance of shipping something useful by Month 3

### Stress Points

- Month 2: "MVCC is harder than I thought. Concurrency bugs everywhere."
- Month 4: "The merge logic between MutableStore and CsrSnapshot has
  edge cases we didn't anticipate."
- Month 8: "Every new feature needs to work on both storage paths.
  Development velocity is halved."

### Inflection Points

- If the team has database internals experience (or acquires it), the
  MVCC + dual-engine model is manageable
- If the team doesn't, this path becomes a multi-year slog through
  concurrency bugs and consistency edge cases
- If write rates stay low (<1000/minute), this complexity may be
  unnecessary (Timeline B would suffice)

---

## Pre-Mortem: "It's 12 Months From Now and We Failed"

*Shreyas Doshi's pre-mortem: assume the project failed. Why?*

### Pre-Mortem for Timeline A (Read-Only Accelerator)

**Why it failed:** We built a fast demo but nobody adopted it. Analytics
teams already have Neo4j and don't want to manage a second system. The
"export → import → query" workflow is too much friction. We're a
benchmark, not a product.

**The deeper cause:** We confused "technically impressive" with "useful."
The 100x speedup is real but the switching cost (even just adding a
sidecar) is higher than we thought. Users don't switch tools for speed
alone — they switch for workflow improvements.

### Pre-Mortem for Timeline B (Append-Log Analytics Engine)

**Why it failed:** The staleness window killed us. Users wrote data and
then couldn't query it for 30 seconds. They filed bugs. We explained
"it's eventual consistency" but they didn't care about our architecture
— they wanted their data to show up immediately. We spent 6 months
trying to make incremental rebuild faster and never got below 10 seconds
for 50GB graphs.

**The deeper cause:** We underestimated how much users hate eventual
consistency, even for analytics. "My write disappeared" is a visceral
negative experience that no benchmark can overcome.

### Pre-Mortem for Timeline C (Dual-Engine Hybrid)

**Why it failed:** MVCC and dual-engine merge logic consumed all our
engineering bandwidth. We spent 9 months on database plumbing and only
shipped 30% Cypher coverage. Users tried it, hit "unsupported Cypher
feature" errors on basic queries, and went back to Neo4j. We built a
correct database that nobody could use.

**The deeper cause:** We tried to solve the hardest problem (database
internals) when we should have solved the most valuable problem (Cypher
coverage and user experience). We optimized for architectural correctness
at the expense of user value.

---

## The "You Don't Know What You Don't Know" List

*Database complications that aren't in any of the timelines above but
will bite you:*

### 1. Query Planning Is the Real Monster

The Cypher query planner is 182K LOC of Scala in Neo4j. It's not the
parser (that's "just" a grammar). It's the optimizer: join ordering,
cardinality estimation, eager barrier analysis, subquery planning.

**Why it matters:** Without a good planner, even simple queries like
`MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a.name = "Alice" RETURN c`
could take seconds instead of milliseconds because the engine picks the
wrong join order.

**Risk level:** HIGH. This is the single biggest "you don't know what
you don't know" item. You can start with a rule-based planner (no
optimization, just execute left-to-right), but you'll hit performance
cliffs on moderate queries.

**Mitigation:** Start with a trivial planner. Accept that complex queries
will be slow. Add cost-based optimization incrementally. The CSR storage
format actually helps here — traversal is so cheap that a bad plan is
still fast for most queries.

### 2. Cypher's Semantic Edge Cases Are a Tarpit

Cypher has:
- Three-valued logic (TRUE, FALSE, NULL)
- Implicit type coercions
- Complex NULL propagation rules
- OPTIONAL MATCH semantics (outer joins)
- Path uniqueness constraints
- Aggregation scoping rules

Each of these is a week of debugging when you get it wrong. The
openCypher spec is incomplete in places. Neo4j's behavior is the de
facto standard, and some behaviors are undocumented.

**Risk level:** MEDIUM-HIGH. Death by a thousand paper cuts. Not a
single hard problem — a hundred small ones.

**Mitigation:** Build a conformance test suite early. Run Neo4j and your
engine against the same queries. Automate the comparison.

### 3. Memory Management at 50GB

A 50GB graph has ~500M-2B edges. The CSR arrays alone are ~8-16GB for
offsets + peers. Property data adds more. At this scale:

- mmap works but you need to be careful about virtual address space
- Building the snapshot requires sorting ~16GB of edge data
- The low-RAM build path (external merge sort) becomes critical
- Memory-mapped files can cause I/O storms if the working set doesn't
  fit in RAM

**Risk level:** MEDIUM. Knight Bus's low-RAM builder already handles
this, but it's only tested at 39 nodes. Testing at 50GB is mandatory
before claiming it works.

**Mitigation:** Build a 50GB test graph generator. Test the full
pipeline: import → build → query. Measure RSS, build time, query latency.

### 4. Driver Compatibility Is Surprisingly Hard

Neo4j's official drivers (Python, Java, JavaScript, Go, .NET) test
against specific Bolt protocol behaviors. Subtle differences cause
failures:
- Connection lifecycle management
- Error code formats
- Result streaming semantics (PULL/DISCARD)
- Bookmark handling for causal consistency
- Routing table (even in single-node mode, drivers expect a routing response)

**Risk level:** MEDIUM. Each driver has its own quirks. The Python driver
is the most forgiving. The Java driver is the strictest.

**Mitigation:** Test with all 3 major drivers (Python, Java, JavaScript)
from Day 1. Don't wait until Month 6.

### 5. Schema and Constraints

Users expect:
- Unique property constraints (`CREATE CONSTRAINT ... IS UNIQUE`)
- Property existence constraints
- Node labels and relationship types
- Indexes that accelerate WHERE clauses

Without constraints, users can corrupt their own data. Without indexes,
WHERE clauses on properties are O(n) scans.

**Risk level:** MEDIUM for analytics (most analytics workflows don't
create constraints). LOW-MEDIUM for v1 (can defer).

### 6. Backup, Monitoring, and Operational Tooling

Production users need:
- Online backup (`neo4j-admin backup`)
- Metrics (query count, latency histogram, memory usage)
- Logging (query logs, slow query detection)
- Health checks (is the database up?)

These are boring but mandatory for production adoption.

**Risk level:** LOW individually, but collectively they add 10-20K LOC
and weeks of work.

---

## Cross-Timeline Analysis

| Path | Time to first user value | Read speed | Write model | Database complexity | LOC Year 1 | Regret risk |
|---|---|---|---|---|---|---|
| **A: Read-Only** | ~1 month | 10-100x | None | None | 60-80K | "We built a demo" |
| **B: Append-Log** | ~2 months | 10-100x | Eventual (seconds lag) | LOW-MEDIUM | 80-110K | "Staleness confused users" |
| **C: Dual-Engine** | ~3 months | 10-100x reads, 1x writes | Immediate | HIGH | 100-140K | "MVCC consumed all bandwidth" |

| Path | Upside | Downside | Reversibility | Who has to cooperate |
|---|---|---|---|---|
| **A** | Ship fast, no database risk | Limited ceiling, can't grow beyond sidecar | HIGH — can always add writes later | Users accept sidecar model |
| **B** | Real product, novel but proven model (ClickHouse-like) | Staleness window UX, rebuild time at scale | MEDIUM — can add mutable overlay later | Users accept eventual consistency |
| **C** | Full database, immediate writes | 2x development cost forever, MVCC bugs | LOW — hard to simplify once built | Team learns database internals |

### Shreyas Doshi "Opportunity Cost" Lens

The question isn't "what's the ROI of each path?" It's "what's the
opportunity cost?"

- **Timeline A** has the lowest opportunity cost. You learn fast, ship
  fast, and preserve the option to add writes later. The cost of being
  wrong is low — you've built a useful accelerator either way.

- **Timeline B** has moderate opportunity cost. The append-log model is
  the highest-leverage architectural decision. If it works, you have a
  novel analytics engine. If it doesn't (staleness is unacceptable),
  you've still built the read path and the Bolt/Cypher layer.

- **Timeline C** has the highest opportunity cost. The MVCC and
  dual-engine work is months of engineering that could have gone into
  Cypher coverage, Atlas families, or user experience. If users don't
  need immediate writes (and analytics users usually don't), this
  complexity was wasted.

### Shreyas Doshi "Three Levels of Product Work" Lens

- **Level 1 (Impact):** What moves the needle for users? → Read speed
  for algorithms. This is where Knight Bus already wins.

- **Level 2 (Execution):** What needs to be built well? → Bolt protocol,
  Cypher parser, import pipeline. Copy-work from Neo4j.

- **Level 3 (Optics):** What looks impressive but doesn't drive adoption?
  → Full ACID transactions, MVCC, distributed consensus. These are
  impressive on a features list but analytics users rarely need them.

**The trap:** Spending 80% of effort on Level 3 (database machinery)
when Level 1 (read speed) is already proven and Level 2 (Bolt/Cypher)
is where the actual adoption barrier lives.

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline B (Append-Log Analytics Engine).**

Here's why, through the Doshi lens:

1. **It's the highest-leverage decision.** The append-log + CSR
   recompile model is a PROVEN architecture (ClickHouse, DuckDB, Kudu).
   You're not inventing it. You're applying it to graphs. The risk is
   lower than you think.

2. **It ships user value in 2 months** (reads + writes via Bolt). Not
   as fast as Timeline A, but with a much higher ceiling.

3. **The staleness window is manageable for the target audience.**
   Analytics users run PageRank on yesterday's data. They don't need
   sub-second write-to-read latency. "Your write will be queryable in
   5-30 seconds" is acceptable for analytics.

4. **It preserves optionality.** If staleness IS a problem, you can
   add a mutable overlay later (evolving toward C). If it's NOT a
   problem, you've avoided months of unnecessary MVCC work.

### Which path is safest if things go badly?

**Timeline A (Read-Only Accelerator).**

If the project fails or loses momentum:
- You have a working fast-read engine (useful as a benchmark tool)
- No wasted time on database machinery
- The codebase is the smallest and most focused
- Every line of code is reusable in Timelines B or C

### What experiment would reduce uncertainty fastest?

**Two experiments, run in parallel:**

**Experiment 1: "Can we serve Bolt?" (1 week)**
Build a minimal Bolt v4 server that accepts connections from the Neo4j
Python driver, runs one hardcoded query against the existing CSR
snapshot, and returns results. This proves the Bolt implementation is
tractable and measures how much LOC it takes.

**Experiment 2: "How fast is snapshot rebuild?" (1 week)**
Generate a synthetic graph with 10M, 50M, and 100M edges. Measure:
- Full CSR snapshot build time
- Incremental rebuild time (add 1000 edges, rebuild)
- Memory usage during rebuild

This answers the critical Timeline B question: "Is the staleness window
5 seconds or 5 minutes for a 50GB graph?" If incremental rebuild is
<10 seconds, Timeline B is clearly viable. If it's >60 seconds, you
need to reconsider.

### The Doshi Verdict: Don't Solve Problems You Don't Have Yet

> "Most execution problems are actually strategy problems."

The strategy problem here is: **you're thinking about database
complications before you have users.** The write model, MVCC,
transactions — these are real problems, but they're problems for Month 6,
not Month 1.

Month 1's problem is: **Can a Neo4j user connect to your engine with
their existing driver and get a faster answer to their query?**

That's it. That's the only thing that matters right now. Everything else
is premature optimization of the architecture.

**Recommended sequence:**

```text
Week 1-2:    Run both experiments (Bolt server + rebuild benchmark)
Week 3-4:    Based on results, commit to Timeline A or B
Month 1-2:   Ship MVP (Bolt + Cypher reads + import)
Month 2-3:   Add append-log writes (if Timeline B)
Month 3-6:   Expand Cypher coverage (THIS is the real work)
Month 6-12:  Add Atlas families, incremental rebuild, property storage
Year 1+:     Evaluate whether mutable overlay is needed based on user feedback
```

**What NOT to do:**
- Do NOT start with MVCC or dual-engine (Timeline C) unless users
  demand immediate writes — and they almost certainly won't for
  analytics workloads
- Do NOT spend months on the "perfect write model" before you have
  a single user connected via Bolt
- Do NOT build the query planner before you have basic Cypher working
  (a trivial rule-based planner is fine for Month 1-6)

### The Real Wins

| Win | When | Path |
|---|---|---|
| "I connected my Neo4j Python driver and got results" | Month 1 | A or B |
| "PageRank ran 50x faster than Neo4j GDS" | Month 3 | A or B |
| "I imported my 50GB graph in 10 minutes" | Month 4 | A or B |
| "I wrote 1000 nodes and they appeared in 5 seconds" | Month 3 | B |
| "My team switched from Neo4j and didn't notice" | Year 1+ | B → C |

### The Real Risks

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Cypher semantic edge cases consume months | HIGH | HIGH | Conformance test suite from Day 1 |
| Bolt driver compat is harder than expected | MEDIUM | MEDIUM | Test with real drivers from Day 1 |
| 50GB rebuild time is too slow | MEDIUM | HIGH | Experiment 2 answers this in 1 week |
| Users demand immediate writes | LOW (for analytics) | MEDIUM | Add mutable overlay when needed, not before |
| Query planner becomes bottleneck | MEDIUM | HIGH | Defer; CSR makes bad plans tolerable |
| Memory management at 50GB | MEDIUM | MEDIUM | Test with realistic data sizes early |
| Team needs database internals knowledge | HIGH (for C), LOW (for A/B) | HIGH | Timeline A/B avoids most database complexity |

### One Sentence

> The highest-leverage move is to prove Bolt + Cypher + CSR reads work
> together (1 month), THEN add append-log writes (1 more month), THEN
> let user feedback — not architecture anxiety — drive whether you need
> more database machinery.

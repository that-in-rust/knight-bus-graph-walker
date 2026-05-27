# Timeline Traverser: The OLTP/OLAP Split — 1000 IQ Ideation

*What if you stop trying to solve both problems with one storage
engine? Copy Neo4j's transactional store in Rust (OLTP). Build
Knight Bus's CSR layouts for analytics (OLAP). Connect them with
a log-based sync — exactly like the industry's biggest databases
already do.*

---

## Research: Who Already Does This?

Before inventing anything, let's see who already solved the
OLTP/OLAP split — and how.

### The HTAP Pattern (Hybrid Transactional/Analytical Processing)

The database industry converged on a clear architecture for
combining transactions with analytics. From a 2024 survey of HTAP
databases (arXiv:2404.15670), the four main patterns are:

| Architecture | How it works | Examples |
|---|---|---|
| **Primary Row Store + In-Memory Column Store** | Row store handles OLTP. Hot tables mirrored into columnar format in memory for analytics. Same process. | **Oracle Database In-Memory**, **SQL Server Columnstore** |
| **Distributed Row Store + Column Store Replica** | Row store (Raft-replicated) handles OLTP. Asynchronous column-format replicas serve OLAP. Separate processes. | **TiDB + TiFlash**, **PolarDB-IMCI** (Alibaba) |
| **Primary Row Store + Distributed Column Store** | Similar to above but column store is distributed across nodes. | **Google AlloyDB** (PostgreSQL + columnar engine) |
| **Primary Column Store + Delta Row Store** | Column store is primary. Small row-format delta store absorbs writes, then merges into columns. | **SAP HANA**, **ClickHouse** (MergeTree) |

**The key insight from every one of these systems:**

> You don't use ONE format for everything. You use a row/record
> format for writes and a columnar/analytical format for reads.
> A synchronization mechanism keeps them consistent.

### Graph Databases That Do This

| System | OLTP Storage | OLAP Storage | Sync Mechanism |
|---|---|---|---|
| **Neo4j + GDS** | Record store (linked lists) | In-memory CSR projection | `gds.graph.project()` — manual, on-demand, ephemeral |
| **GART** (Shanghai Jiao Tong + Alibaba, USENIX ATC'23) | MySQL/PostgreSQL (relational) | **Mutable CSR** with coarse-grained MVCC | WAL log replay — automatic, continuous |
| **Galaxybase** (VLDB 2024) | Log-Structured Adjacency List + Edge Pages | Same store, but with log-structured design for both | WAL + cache + batch flush |
| **FalkorDB** | Sparse adjacency matrix (GraphBLAS) | Same matrix — vectorized traversal | Unified (matrix supports both patterns) |

**The revelation from GART specifically:**

GART is an academic system (2023) that does EXACTLY what you're
proposing — but starting from a relational database:

1. MySQL handles OLTP (row store, full ACID)
2. GART reads MySQL's WAL (binlog)
3. GART converts relational data into **mutable CSR** format
4. GraphScope runs analytics on the CSR

GART's key innovation: an **efficient mutable CSR** that maintains
good locality for edge scans while supporting updates. They use
coarse-grained MVCC (per-epoch versioning, not per-transaction)
to reduce overhead.

**What this means for Knight Bus:**

You're not inventing a new architecture. You're applying a PROVEN
architecture to a graph database. The difference from GART: your
OLTP layer IS a graph database (not relational), and your OLAP
layer uses ALGORITHM-SPECIFIC CSR layouts (not generic CSR).

---

## Decision Frame

- **Fork in the road:** Should you build one unified storage engine
  that handles both writes and analytics? Or split: Rust port of
  Neo4j's record store for OLTP, Knight Bus CSR layouts for OLAP?

- **Desired outcome:** Users connect with Neo4j drivers, run
  `CREATE`, `MERGE`, `SET`, `DELETE` (OLTP) with immediate
  consistency, AND run `gds.pageRank()`, `MATCH` analytics (OLAP)
  with 10-100x speed.

- **Hard constraints:**
  - Frontend must be Neo4j-compatible (Cypher, Bolt, drivers)
  - OLTP must be correct (don't lose committed data, don't serve
    garbage)
  - OLAP must be fast (this is the whole point of Knight Bus)
  - Single-node v1
  - Small team, limited database internals experience

- **Time horizon:** Week 1 → Month 1 → Quarter 1 → Year 1

- **What would count as failure:**
  - "Your writes are slower than Neo4j" (OLTP regression)
  - "Your analytics aren't faster than Neo4j GDS" (OLAP regression)
  - "My write disappeared" (sync lag causes data loss perception)
  - Team drowns in two storage engines instead of shipping one

---

## The 1000 IQ Idea: Log-Coupled Dual Engine

Before the timelines, here's the architectural insight that makes
this work, drawn from how TiDB, Oracle, SQL Server, AlloyDB, and
GART all solved it:

```
┌─────────────────────────────────────────────────────┐
│                    Bolt / Cypher                     │
│              (single query interface)                │
├──────────────────────┬──────────────────────────────┤
│                      │                              │
│    OLTP Engine        │    OLAP Engine               │
│    (Row/Record Store) │    (CSR Layouts)             │
│                      │                              │
│  ┌────────────────┐  │  ┌────────────────────────┐  │
│  │ Rust port of   │  │  │ Knight Bus CSR:        │  │
│  │ Neo4j-style    │  │  │  · AnchorDualCsr       │  │
│  │ records:       │  │  │  · InboundPower        │  │
│  │  · Node store  │  │  │  · RelaxationFrontier  │  │
│  │  · Rel store   │──┼──│  · OrderedWedge        │  │
│  │  · Prop store  │  │  │  · ...                 │  │
│  │  · WAL         │  │  │                        │  │
│  │  · B+tree idx  │  │  │ Built from OLTP data   │  │
│  │  · MVCC / Locks│  │  │ via log-based sync     │  │
│  └────────────────┘  │  └────────────────────────┘  │
│         │            │            ▲                  │
│         │            │            │                  │
│         └────────────┼── WAL ─────┘                  │
│           (write-    │  replay                       │
│            ahead     │                               │
│            log)      │                               │
├──────────────────────┴──────────────────────────────┤
│                   Query Router                       │
│                                                      │
│  Mutation (CREATE/SET/DELETE) → OLTP Engine          │
│  Read-only analytics          → OLAP Engine          │
│  Ad-hoc reads (MATCH...WHERE) → OLTP or OLAP        │
│                                 (cost-based choice)  │
└─────────────────────────────────────────────────────┘
```

### Why This Is a 1000 IQ Move

**1. You stop fighting yourself.**

The previous timelines tried to make ONE format handle both writes
AND analytics. That's like asking a filing cabinet to also be a
road atlas. The industry learned this lesson: Oracle, SQL Server,
TiDB, AlloyDB, and SingleStore all maintain SEPARATE storage
formats for OLTP and OLAP. You should too.

**2. The OLTP engine can be a straightforward Rust port.**

You said: "OLTP we will do as-is replication in Rust — as close as
possible." This is the EASIEST path for the OLTP side because:
- Neo4j's record store is well-documented (15-byte nodes, 34-byte
  rels, 41-byte props)
- It's a solved problem in Rust (sled, redb, TiKV prove it)
- You're not trying to innovate here — you're copying. Copy-work
  is LOW RISK
- Full ACID, immediate reads-after-writes, familiar behavior

**3. The OLAP engine IS Knight Bus.**

This is where you innovate. CSR layouts, algorithm-specific formats,
the Atlas families — all of this applies to the OLAP side. You're
not shoehorning writes into CSR. You're not compromising CSR for
write support. CSR does what it's proven to do: blazing-fast reads.

**4. The sync mechanism is a well-understood pattern.**

WAL replay (OLTP → OLAP) is exactly how TiDB/TiFlash works. The
OLTP engine writes to a WAL. The OLAP engine reads the WAL and
rebuilds CSR snapshots. This is:
- Asynchronous (OLTP writes aren't slowed by OLAP rebuilds)
- Eventually consistent (OLAP may lag by seconds/minutes)
- Crash-safe (WAL is the source of truth)
- Well-documented (TiDB, Oracle, SQL Server all publish how they
  do this)

**5. The query router is simple for v1.**

```rust
fn route_query(ast: &CypherAst) -> Engine {
    if ast.is_mutation() {
        // CREATE, SET, DELETE, MERGE → OLTP engine
        Engine::Oltp
    } else if ast.is_gds_algorithm() {
        // gds.pageRank(), gds.shortestPath() → OLAP engine
        Engine::Olap
    } else {
        // MATCH ... WHERE ... RETURN → either
        // For v1: just use OLTP (correct, slower)
        // For v2: cost-based routing
        Engine::Oltp
    }
}
```

For v1, route all mutations to OLTP and all explicit algorithm
calls to OLAP. Ad-hoc reads go to OLTP (correct, maybe slower).
In v2, a cost-based router can decide whether an ad-hoc read
benefits from CSR (traversal-heavy → OLAP) or not (property-heavy
→ OLTP).

---

## Timeline A: "Unified Engine" (Previous Best — Baseline)

The path from the last analysis: one storage engine tries to do
everything.

### Opening Move

Build an extended CSR format with property columns. Handle writes
via append-log + periodic recompile.

### Week 1-Month 1

Build base CSR + property columns. No ACID writes yet. Import-only.
Same as before.

### Quarter 1

Add append-log writes. Staleness window (5-30 seconds). Some users
confused by write lag. 80-110K LOC by Year 1.

### Stress Points

- Writes and analytics share one format — compromises both
- "Where's my write?" confusion during recompile window
- Can't offer immediate read-after-write for transactional use
- Property updates require rebuilding CSR sections

### Likelihood: 65% ships, 45% commercial success

(Same as Timeline B from previous analysis)

---

## Timeline B: "OLTP/OLAP Split — Log-Coupled Dual Engine"

### The Bet

Two engines. OLTP = Rust port of Neo4j's record store (faithful,
well-understood). OLAP = Knight Bus CSR layouts (innovative,
proven for reads). WAL replay connects them.

### Opening Move

**Don't start with both engines.** Start with the OLTP engine only.
The OLAP engine is additive.

### Week 1

- Scaffold the OLTP record store in Rust:
  - `#[repr(C)]` node records (label_set, first_rel_offset, first_prop_offset)
  - `#[repr(C)]` rel records (start_node, end_node, type, next_rel_start, next_rel_end, first_prop)
  - `#[repr(C)]` prop records (key_token, value, next_prop)
- Page-backed file storage (mmap initially, custom buffer pool later)
- WAL: append-only log of committed changes
- This is copy-work from Neo4j's well-documented format

**Lived experience:** This feels productive. You're building a
real database. No format debates — you know exactly what Neo4j's
record store looks like. The team is writing Rust structs and
tests, not arguing about CSR vs append-log.

### Month 1

- OLTP engine handles basic CRUD:
  - CREATE node/relationship
  - SET property
  - DELETE node/relationship
  - Read by ID (traverse linked lists, like Neo4j)
- WAL provides crash recovery
- Bolt stub accepts connections from Neo4j drivers
- 5-8 Cypher patterns work (MATCH, CREATE, SET, DELETE, RETURN)
- Performance: approximately NEO4J SPEED on writes (same format)
  and 1.5-3x faster on reads (Rust vs Java)

**Lived experience:** Users can connect their existing Neo4j
drivers. Writes work. Reads work. Nothing is spectacular yet.
But nothing is broken either. You have a FUNCTIONAL database.

### Month 2

- **Turn on the OLAP engine.** Now.
- WAL replay: background thread reads committed WAL entries,
  builds AnchorDualCsr snapshot
- First OLAP query works: `MATCH (n)-[:KNOWS]->(m) RETURN m`
  routes to CSR engine → 10-100x faster than OLTP engine
- Users notice: "Why is this read so fast?"
- Query router: mutations → OLTP, reads → OLAP (if snapshot exists)

**Lived experience:** The "aha" moment. Same database, two speeds.
Writes at Neo4j speed. Reads at Knight Bus speed. Users don't
know there are two engines — they just see fast reads.

### Quarter 1

- OLAP engine has 2-3 specialized layouts (InboundPower,
  RelaxationFrontier, maybe OrderedWedge)
- `gds.pageRank()` runs against InboundPower layout → 50-100x
  faster than Neo4j GDS
- WAL replay lag: configurable, typically 1-10 seconds
- OLTP handles all writes with full ACID
- OLAP handles all algorithm calls with CSR speed
- Ad-hoc reads: OLTP by default, OLAP for traversal-heavy queries

**LOC at Quarter 1:**
- OLTP engine: ~30-40K (record store, WAL, page cache, locks)
- OLAP engine: ~15-20K (base CSR + 2-3 specialized layouts)
- Cypher/Bolt/Router: ~25-35K
- Total: ~70-95K

**Lived experience:** You're telling TWO compelling stories:
1. "Drop-in Neo4j replacement in Rust" (OLTP)
2. "100x analytics when you need it" (OLAP)

### Year 1

- OLTP engine is mature: full ACID, constraints, auth, monitoring
- OLAP engine has 5-8 specialized layouts (P0 + P1 algorithms)
- WAL replay is battle-tested, configurable staleness
- Query router does cost-based selection for ad-hoc reads
- LOC: ~100-140K total

### Long-term Shape

A database with TWO competitive advantages:
1. Neo4j-compatible OLTP (the adoption story)
2. 10-100x OLAP via specialized CSR (the performance story)

Neither advantage exists without the other. Together, they create
a product no one else offers: a graph database that's both
compatible AND dramatically faster for analytics.

### Likelihood: 70% ships, 60% commercial success

Higher commercial success than Timeline A because:
- OLTP compatibility removes adoption barrier completely
- OLAP speed creates a genuine competitive moat
- The "two engines" story is compelling (Oracle does this, TiDB
  does this — it's not weird, it's state-of-the-art)

### Stress Points

- **Month 2:** Sync lag. OLAP reads may not see the latest write.
  "I just created a node, where is it?"
  → Fix: Query router checks OLAP snapshot freshness. If too stale,
  falls back to OLTP engine. Always correct, sometimes slower.
  → Fix: `CALL db.sync()` command that forces WAL replay (like
  `CHECKPOINT` in PostgreSQL)
- **Month 3:** Two storage engines = two sets of bugs. Record store
  bug in OLTP, CSR bug in OLAP, sync bug in WAL replay.
  → Mitigation: OLTP engine is copy-work (lower bug density).
  OLAP engine is proven Knight Bus code (lower bug density).
  Sync is the genuinely NEW code — focus testing there.
- **Month 6:** Disk usage. Record store + CSR snapshots = more disk
  than either alone. ~2x disk for the same graph.
  → Mitigation: CSR snapshots are compressed. For a 50GB record
  store, CSR snapshots might add 30-60GB. Total: 80-110GB.
  Acceptable for analytics workloads.
- **Month 9:** Maintaining two engines. Feature X (e.g., new property
  type) needs changes in BOTH engines.
  → Mitigation: OLTP owns the data model (record format, property
  types, constraints). OLAP engine reads whatever OLTP produces.
  New property type → change OLTP record + WAL format → OLAP
  replay handles it automatically. Most features are OLTP-only
  changes.

### Inflection Points

- **Month 2:** If WAL replay builds CSR in <5 seconds for 50GB,
  the lag is imperceptible. If >60 seconds, users will complain.
  This determines whether the split feels seamless or janky.
- **Month 6:** If users spend 80% time in OLTP (browsing, editing)
  and 20% in OLAP (running algorithms), the split is natural.
  If users constantly switch between write-then-read-analytics,
  the sync lag accumulates frustration.

---

## Timeline C: "OLAP-First with OLTP Bolted On"

### The Bet

Start with Knight Bus CSR for reads (OLAP). Add a minimal write
layer later. Don't build a full OLTP record store — just enough
to accept writes and feed them into CSR rebuilds.

This is the previous "append-log" approach, but framed as
"OLAP-first with a thin OLTP shim."

### Opening Move

Ship read-only Knight Bus with Bolt/Cypher. Add append-log writes
later.

### Month 1

- CSR-only reads work. Import via CSV. 10-100x traversal.
- No writes at all. "Read-only analytics accelerator."

### Quarter 1

- Append log for writes. Periodic CSR rebuild.
- Staleness window: 5-30 seconds.
- No immediate read-after-write.
- No ACID transactions (append is all-or-nothing per snapshot).

### Year 1

- Functional but limited. Users who need real writes switch back
  to Neo4j for OLTP and use this for analytics only.
- You've built a sidecar, not a replacement.

### Likelihood: 80% ships, 35% commercial success

Easy to ship (it's just Knight Bus + Bolt). Low commercial success
because "read-only analytics sidecar" is a niche product. Users
want ONE database, not a sidecar.

### Stress Points

- Month 3: "Can I write to it?" "Not really." Adoption stalls.
- Month 6: Users maintain Neo4j (for writes) + Knight Bus (for
  reads). They wanted to replace Neo4j, not add another system.

---

## Timeline D: "Full Rust Port of Neo4j" (No CSR at all)

### The Bet

Forget Knight Bus. Port Neo4j's entire architecture to Rust.
Same linked-list record store, same page cache, same everything.
Just in Rust.

### Opening Move

Port the record store, WAL, page cache, transaction engine, and
Bolt protocol to Rust. Same format as Neo4j.

### Month 1-3

- Record store works. WAL works. Bolt works.
- Speed: 1.5-3x faster than Neo4j (Rust vs Java, same architecture).

### Quarter 1-Year 1

- Full Cypher support progresses slowly (250K LOC Scala planner).
- Speed advantage is thin: 1.5-3x. Not enough to switch databases.
- No CSR, no specialized layouts, no algorithm speedup.
- LOC: 120-180K (you're building all of Neo4j's complexity in Rust).

### Likelihood: 55% ships, 20% commercial success

Hardest to ship (most LOC) and weakest pitch ("switch databases
for a 2x speedup"). The Knight Bus thesis is abandoned.

### Stress Points

- Month 6: "Why should I switch from Neo4j for a 2x speedup?"
  No good answer.
- Year 1: Neo4j ships GraalVM native-image. Your 2x advantage
  drops to 1.2x. Game over.

---

## Cross-Timeline Analysis

| | A: Unified Engine | B: OLTP/OLAP Split | C: OLAP-First | D: Full Rust Port |
|---|---|---|---|---|
| **OLTP correctness** | Medium (append-log, not ACID) | **High** (real record store, WAL, ACID) | Low (no real OLTP) | **High** (same as Neo4j) |
| **OLAP speed** | 5-20x (compromised format) | **10-100x** (dedicated CSR) | **10-100x** (native CSR) | 1.5-3x (same architecture) |
| **Algorithm speed** | 5-20x | **50-100x** (specialized layouts) | **50-100x** (specialized layouts) | 1.5-3x (like Neo4j GDS) |
| **Neo4j compat** | Partial (no immediate writes) | **Full** (OLTP is faithful port) | Partial (read-only) | **Full** |
| **Adoption barrier** | Medium (staleness) | **Low** (same Bolt/Cypher, same write behavior) | High (read-only) | Low (same everything) |
| **Competitive moat** | Medium | **Thick** (two advantages) | Thin (sidecar only) | Thin (just speed) |
| **LOC Year 1** | 80-110K | 100-140K | 60-80K | 120-180K |
| **Disk usage** | 1x | ~2x | 1x | 1x |
| **Write latency** | 5-30s lag | **<1ms** (OLTP) | 5-30s lag | **<1ms** |

### The Decisive Table

| | Upside | Downside | Reversibility | Regret risk | What must cooperate |
|---|---|---|---|---|---|
| **A: Unified** | Simpler. One engine. | Compromises both OLTP and OLAP | Medium — can split later but painful | "We built a mediocre database" | Users accepting staleness |
| **B: OLTP/OLAP Split** | Best of both worlds. Industry-proven. | Two engines to maintain. Sync complexity. | **High** — can drop OLAP and keep OLTP, or vice versa | "Sync bugs cost us users" | WAL replay working reliably |
| **C: OLAP-First** | Ships fastest. Pure Knight Bus. | "It's a sidecar, not a database" | High — can add OLTP later | "We built a demo, not a product" | Users not needing writes |
| **D: Full Port** | Maximum Neo4j compatibility | No CSR advantage. Thin moat. Huge LOC. | Low — committed to Neo4j's architecture | "We wasted 180K LOC replicating a worse version" | Neo4j not closing the gap |

---

## What Real Systems Teach Us

### The Oracle Lesson

Oracle Database In-Memory (2014) added columnar storage ALONGSIDE
the existing row store. Same database, same SQL, same transactions.
Analytical queries automatically use the columnar format. Oracle
didn't replace its row store — it added a second format and let
the optimizer choose.

**Lesson for Knight Bus:** Don't replace the record store. Add CSR
alongside it.

### The TiDB/TiFlash Lesson

TiDB (2020) keeps its row store (TiKV, Raft-replicated) for OLTP.
TiFlash is a separate process that receives Raft logs and converts
row data to columnar format. Analytical queries are routed to
TiFlash. The key: TiFlash provides **snapshot isolation** — it
reads a consistent point-in-time view, even while TiKV continues
processing writes.

**Lesson for Knight Bus:** WAL replay from OLTP → OLAP can
provide snapshot isolation. The OLAP engine reads a consistent
snapshot. It may be slightly stale, but it's never inconsistent.

### The GART Lesson

GART (2023, USENIX ATC) does exactly the OLTP/OLAP split for
GRAPHS. MySQL handles OLTP. GART reads MySQL's binlog, converts
relational data to **mutable CSR**, and runs GraphScope analytics.

Key innovations from GART:
1. **Mutable CSR** — not fully immutable. Uses a two-level
   structure: base CSR + delta logs. Deltas are merged periodically.
2. **Coarse-grained MVCC** — epoch-based versioning (not per-
   transaction). Cheaper than Neo4j's per-transaction MVCC.
3. **Automatic sync** — no manual `gds.graph.project()`. Data flows
   from OLTP to OLAP continuously.

**Lesson for Knight Bus:** You don't need immutable CSR for OLAP.
A mutable CSR with delta logs is viable. AND you don't need
per-transaction MVCC on the OLAP side — epoch-based is enough.

### The Neo4j GDS Lesson

Neo4j GDS is a MANUAL version of this split:
1. User calls `gds.graph.project('myGraph', ...)` — builds
   in-memory CSR from record store
2. User runs algorithms against the projection
3. User drops the projection when done

This works but has problems:
- Manual: user must project explicitly
- Ephemeral: projection lost on restart
- Slow projection: builds CSR from linked-list records (slow)
- Memory-only: graph must fit in heap

**Lesson for Knight Bus:** Automate the projection. Persist it.
Build from WAL (fast sequential reads, not linked-list pointer
chasing). Don't require the user to manage projections.

### The FalkorDB Lesson

FalkorDB uses sparse adjacency matrices (GraphBLAS) instead of
adjacency lists. The matrix representation supports BOTH OLTP
(point updates to matrix cells) AND OLAP (vectorized matrix
traversal). This is a unified format that avoids the split.

**Lesson for Knight Bus:** A unified format IS possible if the
format supports both patterns natively. But FalkorDB chose
matrices over CSR — a different tradeoff. CSR is faster for
sequential traversal; matrices are better for algebraic operations
(PageRank as matrix multiply). Worth studying, but a different
architectural bet.

---

## The 1000 IQ Synthesis

Here's what no one has built yet:

**A graph database with:**
1. **OLTP layer**: Rust port of Neo4j's record store (faithful,
   compatible, immediate ACID writes)
2. **OLAP layer**: Algorithm-specific CSR layouts (Knight Bus
   thesis, 10-100x reads, Atlas families)
3. **Automatic log-based sync**: WAL replay from OLTP → OLAP
   (like TiDB/TiFlash, not manual like Neo4j GDS)
4. **Transparent query routing**: mutations → OLTP, analytics →
   OLAP, ad-hoc reads → cost-based choice

**Why no one has built this:**
- Neo4j tried (GDS), but their OLTP format makes projection slow
  and projection is manual/ephemeral
- GART tried, but their OLTP is relational (MySQL), not graph-
  native
- Galaxybase tried, but their OLAP format is generic (log-
  structured adjacency), not algorithm-specific
- FalkorDB tried, but chose matrices over CSR (different tradeoff)

**What you'd have that no one else does:**
- OLTP that's Neo4j-compatible (adoption story)
- OLAP that's algorithm-specific (performance story)
- Automatic sync (operational story)
- All in one process (simplicity story)

### The Specific Win: Why OLTP Record Store + CSR OLAP Is Better Than Either Alone

| Scenario | OLTP-only (Timeline D) | OLAP-only (Timeline C) | Split (Timeline B) |
|---|---|---|---|
| User creates 1000 nodes | <1ms per node | Not supported | <1ms per node (OLTP) |
| User reads 1000 neighbors | ~400 random page reads | 1 contiguous read | 1 contiguous read (OLAP) |
| User runs PageRank | ~10 min (GDS projection + compute) | ~5 sec (CSR compute) | ~5 sec (OLAP engine) |
| User creates node, then reads it | Immediate | Wait for rebuild | Immediate (OLTP), OLAP catches up |
| User imports 1M edges, then runs analysis | Import: fast. Analysis: slow (projection). | Import: slow (CSR build). Analysis: fast. | Import: fast (OLTP). Analysis: fast (OLAP builds async). |

The split gives you the best of EVERY scenario.

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline B: OLTP/OLAP Split.**

It's the only path that delivers:
- Full Neo4j compatibility on writes (immediate, ACID, familiar)
- 10-100x on reads and algorithms (CSR, proven)
- An architecture validated by Oracle, TiDB, AlloyDB, SQL Server,
  and GART

### Which path is safest if things go badly?

**Timeline B is ALSO the safest.**

Why? Because the two engines are INDEPENDENT:
- If OLAP engine is delayed → you still have a working Neo4j-
  compatible OLTP database in Rust. Ship it.
- If OLTP engine is delayed → you still have Knight Bus as a
  read-only analytics sidecar. Ship it.
- If sync breaks → route everything to OLTP. Users get Neo4j-
  speed (not great) but nothing breaks.

Timeline A has no fallback — if the unified format doesn't work,
you have nothing shippable. Timeline B always has something
shippable.

### What experiment would reduce uncertainty fastest?

**Three experiments, one day each:**

**Experiment 1: Can you build a minimal Rust record store?**
```
Build #[repr(C)] NodeRecord (24 bytes) + RelRecord (40 bytes)
Write 1M nodes + 10M rels to a page-backed file
Read back with linked-list traversal
Measure: write speed, read speed, LOC to build
Expected: <1 day, <500 LOC, confirms record store is copy-work
```

**Experiment 2: Can you replay a WAL into CSR?**
```
Write 1M node-create + 10M rel-create events to a WAL file
Read WAL → build AnchorDualCsr snapshot
Measure: replay time, CSR correctness
Expected: <1 day, reuses existing Knight Bus build code
This proves the OLTP→OLAP sync path works
```

**Experiment 3: Can you route queries between two engines?**
```
Minimal Bolt server. Two backends: record store + CSR.
Router: CREATE → record store, MATCH → CSR
Run 100 queries, verify correctness
Expected: <1 day, <300 LOC for router
This proves the dual-engine architecture is viable
```

If all three experiments succeed in 3 days, you know Timeline B
is the path. If any fails, you know exactly which part needs
more work.

---

## Recommended Build Sequence

```
Week 1-2:   Run Experiments 1-3 (validate dual-engine architecture)
            Deliverable: proof that record store, WAL replay,
            and query routing all work

Week 3-4:   OLTP engine v1 (record store + WAL + basic CRUD)
            Bolt stub (accept connections, parse Cypher)
            Deliverable: users can CREATE and MATCH nodes

Month 2:    WAL replay → CSR build (OLTP → OLAP sync)
            OLAP engine serves traversal queries
            Deliverable: writes at Neo4j speed, reads at 10-100x

Month 3:    First specialized OLAP layout (InboundPower for PageRank)
            gds.pageRank() routes to OLAP → 50-100x
            Deliverable: first "wow" benchmark

Month 4-6:  Expand Cypher coverage (MERGE, SET, DELETE, WHERE, WITH)
            Add 2-3 more specialized layouts (Dijkstra, Triangle Count)
            Add constraints, auth, configuration

Month 6-12: Polish, openCypher TCK, driver compatibility testing
            Add P1 layouts based on user demand
            Cost-based query router for ad-hoc reads
```

**The key insight in this sequence:** OLTP ships first (Month 1).
OLAP is additive (Month 2+). At every point you have something
shippable. The OLTP engine alone is a "Rust Neo4j" — valuable
even without CSR. The OLAP engine adds the 100x speedup on top.

---

## Why This Is the 1000 IQ Answer

The "copy Neo4j's record store" instinct is CORRECT for OLTP.

The "innovate with CSR" instinct is CORRECT for OLAP.

The mistake was trying to make ONE format do both. Every major
database vendor learned this lesson and built dual-format systems:

| Vendor | OLTP Format | OLAP Format | Year Shipped |
|---|---|---|---|
| Oracle | Row store | In-Memory Column Store | 2014 |
| SQL Server | B+tree rows | Columnstore indexes | 2012 |
| TiDB | TiKV row store | TiFlash columnar | 2020 |
| Google AlloyDB | PostgreSQL row store | Columnar engine | 2022 |
| SingleStore | Row store | Column store | 2013 |
| SAP HANA | Delta row store | Main columnar store | 2010 |

You're not inventing this pattern. You're applying it to graph
databases with Knight Bus's specific innovation: algorithm-specific
OLAP layouts instead of generic columnar.

> Copy the record store. Invent the analytics engine. Connect them
> with a log. This is how the industry does it. Do it for graphs.

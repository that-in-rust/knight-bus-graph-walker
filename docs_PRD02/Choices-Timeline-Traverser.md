# Knight Bus: All the Choices We Need to Make

*Using the Timeline Traverser playbook to map every fork in the road,
then simulate the 3 most consequential ones.*

---

## Core Facts Enumerated

```
FACT 1: We have 4,710 LOC Rust (v0.0.2)
  - Dual CSR with mmap (read-only)
  - CLI-only (no server, no Bolt, no Cypher)
  - External merge-sort build (low_ram.rs, 1,703 LOC)
  - Benchmarked at 2 GB: 33,695× faster than Neo4j Cypher, 4.5× less RAM

FACT 2: L1 PRD (the customer's requirements)
  1. Exact same APIs — ZERO changes to client code
  2. Identical OLTP architecture
  3. Lowest RAM OLAP — 50 GB on 8 GB system
  4. Community edition, single node

FACT 3: Neo4j Community = 2.09M LOC Java
  - Bolt server (73K LOC)
  - Cypher engine (670K LOC, planner alone = 181K LOC)
  - Storage engine (228K LOC, record format: 15B/34B/41B)
  - GDS (separate repo, in-memory CSR projection)

FACT 4: OLAP RAM budget decided
  - Level 2 default: 3.2 GB for PageRank on 200M nodes
  - Level 3 fallback: 196 MB for any scale
  - Neo4j: OOM on 8 GB system

FACT 5: What we DON'T have yet
  - No server (no TCP listener)
  - No Bolt protocol
  - No Cypher parser
  - No graph algorithms (PageRank, BFS, etc.)
  - No write path (CREATE, SET, DELETE)
  - No OLTP record store
  - No property storage beyond node keys
  - No relationship types in CSR
  - No transaction manager
```

---

## The 8 Choices

### Choice 1: BUILD ORDER ★★★ (most consequential)

What do we build first?

| Option | What ships | Time | Risk |
|---|---|---|---|
| **A: OLAP-First** | PageRank CLI with Level 2 RAM control | 2 weeks | No API compat yet |
| **B: Bolt-First** | Neo4j drivers connect, `RETURN 1` works | 3 weeks | No analytics |
| **C: Vertical Slice** | Bolt + `CALL gds.pageRank()` | 4 weeks | More scope, more risk |
| **D: OLTP-First** | Neo4j record stores in Rust, then Bolt on top | 8+ weeks | Huge scope, delayed demo |

### Choice 2: API SURFACE ★★☆

How much Neo4j compatibility do we aim for?

| Option | What it means | LOC | Risk |
|---|---|---|---|
| **A: Exact** | Full Bolt v5.3 + full Cypher 25 grammar | ~30K | Years of work |
| **B: Compatible subset** | Bolt v5.3 + Cypher subset (MATCH/WHERE/RETURN + CALL) | ~8K | Covers 80% of users |
| **C: Procedures only** | Bolt v5.3 + CALL procedures + RETURN literal only | ~3K | Fastest, but limited |

### Choice 3: OLTP STORAGE ★★☆

How do we store graph data for OLTP queries?

| Option | What it means | Identical to Neo4j? | Write perf |
|---|---|---|---|
| **A: Neo4j record format** | 15B node + 34B rel + 41B property, linked lists | YES | Same (pointer chase) |
| **B: CSR + WAL** | CSR for reads, write-ahead log for mutations | NO (faster reads) | Rebuild cost |
| **C: Hybrid** | Record format for writes, CSR view for reads | Externally yes | Complex sync |

### Choice 4: OLAP I/O MODEL ★☆☆ (largely decided)

Already decided Level 2 (O_DIRECT) default. Remaining question:

| Option | What it means |
|---|---|
| **A: Level 2 only** | No mmap for OLAP. O_DIRECT always. |
| **B: Level 1 + Level 2** | mmap for speed when RAM is plentiful, O_DIRECT for low-RAM |
| **C: All three levels** | mmap + O_DIRECT + edge-centric streaming |

**Recommendation: C (all three).** Level 1 = fast default for dev machines.
Level 2 = production default. Level 3 = extreme scale. User picks via `--ram-budget`.

### Choice 5: CYPHER IMPLEMENTATION ★★☆

| Option | What it means | LOC | Risk |
|---|---|---|---|
| **A: Port ANTLR4 grammar** | Translate Neo4j's Cypher25Parser.g4 to Rust | ~15K parser + 50K+ planner | Multi-month |
| **B: Use existing crate** | `cypher-parser` or `opencypher-parser` crate | ~2K integration | Crate maturity? |
| **C: Hand-rolled subset** | Parse MATCH/WHERE/RETURN/CALL by hand | ~3K | Limited but shippable |

### Choice 6: WRITE PATH TIMING ★☆☆

| Option | When writes ship |
|---|---|
| **A: Read-only first** | v0.0.3-v0.0.5 read-only. Writes in v0.0.7+ |
| **B: Writes from day 1** | CREATE/SET/DELETE in v0.0.3 |

**Recommendation: A.** Read-only demos "50 GB on 8 GB." Writes are important but
not the differentiator.

### Choice 7: ASYNC RUNTIME ★☆☆

| Option | Server | OLAP | Compatibility |
|---|---|---|---|
| **A: tokio + rayon** | tokio (Bolt server) | rayon (parallel compute) | Most ecosystem support |
| **B: compio + rayon** | compio (Bolt + O_DIRECT) | rayon (parallel compute) | Best for O_DIRECT |
| **C: tokio + tokio-uring** | tokio (Bolt server) | tokio-uring (O_DIRECT) | Simpler runtime |

### Choice 8: PROPERTY STORAGE ★☆☆

| Option | What it means | For OLAP |
|---|---|---|
| **A: Neo4j-style** | 41B records, linked-list chains | Identical but cache-unfriendly |
| **B: Columnar** | Typed property columns (DuckDB-style) | Cache-friendly for scans |
| **C: Embedded store** | Properties in a key-value store (RocksDB, sled) | Simple, proven |

---

## Which Choices Are Actually Consequential?

The 8 choices reduce to **3 real forks:**

```
FORK 1: Build Order (Choice 1)
  → Determines what we can demo, to whom, and when.
  → Everything else (API, Cypher, writes) flows from this.

FORK 2: Storage Architecture (Choice 3)
  → "Identical OLTP architecture" = Neo4j record format?
  → Or is "identical external behavior" sufficient?
  → Determines 30-60% of total codebase.

FORK 3: Cypher Scope (Choice 5)
  → Full Cypher = years. Subset = months. Procedures-only = weeks.
  → "Exact same APIs with ZERO changes" pushes toward full.
  → But GDS users mostly use CALL procedures.
```

Everything else (OLAP I/O model, write timing, async runtime, properties)
has a clear best answer or can be deferred.

---

## Timeline Traverser: The 3 Consequential Forks

### Decision Frame

- **Fork in the road:** What to build first AND how deep to go on compatibility
- **Desired outcome:** Working system where Neo4j clients connect and run analytics on 50 GB graph using 3.2 GB RAM
- **Hard constraints:**
  - "Exact same APIs with ZERO changes" (L1 PRD requirement 1)
  - "50 GB on 8 GB" (L1 PRD requirement 3)
  - Single developer (or very small team)
  - Current codebase: 4,710 LOC, read-only CLI
- **Time horizon:** v0.0.3 (next 4 weeks), v0.0.5 (8 weeks), v0.1.0 (6 months)
- **What would count as failure:**
  - Can't demo "50 GB on 8 GB" → loses the viral moment
  - Neo4j drivers can't connect → "exact same APIs" is a lie
  - Takes 12+ months before anything usable → momentum dies

---

### Timeline A: OLAP Engine First (PageRank → Bolt → Cypher)

**Thesis:** Ship the hardest, most differentiated thing first. The "50 GB on 8 GB" 
story IS the product. Bolt/Cypher is plumbing — add it after the engine works.

**Opening move:** Add `rayon` + `O_DIRECT` PageRank to existing CSR codebase.

**Week 1-2 (v0.0.3):**
- Add `StreamingCsrReader` (O_DIRECT, chunk-based reads) — ~200 LOC
- Add `PageRankLevel2` (vertex state in RAM, stream CSR) — ~150 LOC
- Add `--ram-budget` flag to CLI — ~80 LOC
- Test: 2 GB corpus → PageRank in ~1 sec, RSS < 200 MB
- Ship: `knrt pagerank --snapshot ./data --ram-budget 4G`

**Week 3-4 (v0.0.4):**
- Add `PageRankLevel3` (edge-centric scatter-gather) — ~400 LOC
- Add 2-3 more algorithms (BFS, Connected Components) — ~300 LOC
- Test: Synthetic 50 GB graph → PageRank in 3.2 GB RAM
- Ship: Blog post "50 GB PageRank in 3.2 GB RAM" with benchmarks

**Month 2-3 (v0.0.5-v0.0.6):**
- Bolt server (PackStream codec, session state machine) — ~2,000 LOC
- `CALL gds.pageRank()` procedure dispatch — ~500 LOC
- Neo4j drivers connect and run PageRank

**Quarter 1 (v0.0.7-v0.1.0):**
- Cypher parser (subset: MATCH, WHERE, RETURN, WITH, CALL)
- OLTP record stores (read path only)
- Full "drop-in replacement for read + analytics" story

**Long-term shape:** Analytics-first graph database. Users adopt for OLAP,
stay for OLTP once it's built. "Neo4j but 10× less RAM."

**Likelihood:** HIGH for the OLAP part. Medium for "add Bolt later."

**Stress points:**
- **Week 3:** Need a 50 GB test graph. Where does it come from?
  Generate synthetic? Download LDBC? Convert a real Neo4j export?
- **Month 2:** Bolt protocol is complex. PackStream binary encoding + 
  session state machine + error handling. Could take longer than 2 weeks.
- **Month 3:** "Exact same APIs" claim becomes testable. If drivers don't
  connect cleanly, the "ZERO changes" promise is broken.

**Inflection points:**
- If the 50 GB benchmark looks incredible → viral potential is real, press forward
- If Bolt implementation hits deep compatibility issues → might need a
  compatibility shim or proxy instead of native implementation
- If no one cares about CLI PageRank → Bolt becomes urgent, move it up

---

### Timeline B: Bolt Server First (Protocol → Cypher → OLAP)

**Thesis:** "Exact same APIs with ZERO changes" IS requirement #1 in the L1 PRD.
It's listed FIRST for a reason. Without Bolt, we're a CLI tool, not a database.
Ship the protocol layer first, then add capabilities behind it.

**Opening move:** Implement Bolt v5.3 + PackStream codec in Rust.

**Week 1-2:**
- Bolt handshake + version negotiation — ~300 LOC
- PackStream codec (serialize/deserialize) — ~600 LOC
- Session state machine (NEGOTIATION → AUTHENTICATION → READY) — ~400 LOC
- `RETURN 1` works from `neo4j-driver-python`
- Ship: `knrt serve --port 7687` → drivers connect

**Week 3-4 (v0.0.3):**
- Cypher: parse RETURN literals and CALL procedures — ~500 LOC
- `CALL dbms.components()` → returns version info
- `CALL db.labels()` → returns node labels from CSR snapshot
- Neo4j Browser connects and shows something

**Month 2 (v0.0.4):**
- MATCH (p:Person) RETURN p.name — basic pattern matching — ~2,000 LOC
- Read from snapshot CSR + node_table + strings
- First query that touches real graph data

**Month 3 (v0.0.5):**
- NOW add PageRank as CALL gds.pageRank()
- Level 2 streaming from disk, results returned via Bolt
- The full demo: driver → Bolt → PageRank → 3.2 GB RAM

**Quarter 1-2 (v0.0.7-v0.1.0):**
- Expand Cypher coverage (WHERE, WITH, OPTIONAL MATCH, aggregations)
- Write path (CREATE, SET, DELETE)
- OLTP record stores

**Long-term shape:** Protocol-first database. Every feature is accessible via
standard Neo4j drivers from day 1. No CLI-only phase.

**Likelihood:** HIGH for Bolt basics. MEDIUM for Cypher (parsing is hard).

**Stress points:**
- **Week 1:** Bolt protocol documentation is sparse. Neo4j's own Java
  implementation is 73K LOC. How much is essential vs optional?
- **Week 3:** The moment Cypher parsing starts, scope explodes.
  "Just parse RETURN" becomes "but what about expressions? strings?
  numbers? lists? maps? function calls?" Each adds LOC.
- **Month 2:** `MATCH (p:Person) RETURN p.name` requires:
  label-to-node mapping, property access, pattern matching. This is
  NOT a 2,000 LOC job if done properly. Could be 5,000-10,000.
- **Month 3:** By the time PageRank ships, it's been 3 months.
  Timeline A has it in 2 weeks. That's a 2.5-month delay on the
  core differentiator.

**Inflection points:**
- If Bolt handshake works on first try with official drivers → massive confidence boost
- If Cypher parsing turns into a rabbit hole → may need to
  limit to CALL-only for v0.0.3 (converges with Timeline A)
- If Neo4j Browser connects and shows a graph → emotionally satisfying,
  great for demos, even if only 100 nodes

---

### Timeline C: Vertical Slice (Bolt + PageRank Together)

**Thesis:** Don't choose between OLAP and API. Ship a THIN vertical slice:
Bolt server + CALL gds.pageRank() + Level 2 streaming. No Cypher parser
beyond CALL dispatch. This proves both "exact same API" AND "50 GB on 8 GB"
in one release.

**Opening move:** Build Bolt server AND PageRank engine in parallel
(they don't depend on each other until integration).

**Week 1-2:**
- **Track A (Protocol):** Bolt handshake + PackStream + CALL dispatch — ~1,400 LOC
- **Track B (Engine):** StreamingCsrReader + PageRankLevel2 — ~430 LOC
- Both tracks independent — can be done by different people OR sequentially
- Integration: wire CALL gds.pageRank → engine → Bolt response

**Week 3-4 (v0.0.3):**
- `neo4j-driver-python` connects, runs:
  ```python
  result = session.run("CALL gds.pageRank.stream('myGraph', {})")
  for record in result:
      print(record["nodeId"], record["score"])
  ```
- Level 2 RAM: 3.2 GB for 200M nodes
- Add `CALL gds.bfs.stream()` and `CALL gds.wcc.stream()`
- Ship: blog post "50 GB PageRank via Neo4j driver, 3.2 GB RAM"

**Month 2 (v0.0.4-v0.0.5):**
- Add Cypher parser for MATCH/WHERE/RETURN (subset)
- Add more GDS procedures (Dijkstra, Louvain, triangle counting)
- Level 3 (edge-centric streaming) for extreme scale
- compio integration for O_DIRECT pipelining

**Month 3 (v0.0.6):**
- `MATCH (p:Person)-[:KNOWS]->(q) RETURN p.name, q.name` works
- Property access from snapshot data
- OLTP read path (first real queries, not just procedures)

**Quarter 2 (v0.0.7-v0.1.0):**
- Write path (CREATE, SET, DELETE → rebuild CSR or use WAL)
- Full GDS algorithm coverage (all 13 families → implemented as functions, not layouts)
- OLTP record stores (if "identical architecture" requires them)

**Long-term shape:** "Works like Neo4j, runs like DuckDB." Protocol-compatible
from the start, but with a clear RAM advantage for analytics.

**Likelihood:** HIGH. This is the most balanced path.

**Stress points:**
- **Week 2:** Bolt + PageRank integration. How does CALL dispatch
  know which procedure to call? Need a simple registry pattern.
  Not hard, but must be designed properly.
- **Week 4:** The "50 GB on 8 GB" demo requires a 50 GB dataset.
  If we can't generate or acquire one, the claim is unverified.
- **Month 2:** Cypher parser is still the same scope risk as Timeline B.
  "Subset" is well-defined on paper but scope-creepy in practice.
- **Month 3:** If OLTP read path requires Neo4j record format stores,
  that's a big architectural decision (Choice 3) that must be made.

**Inflection points:**
- If the v0.0.3 demo (driver → PageRank → 3.2 GB) impresses early users
  → double down on GDS coverage before Cypher
- If users say "I need MATCH queries, not procedures" → accelerate Cypher
- If 50 GB demo shows 15 sec on NVMe → the viral story writes itself

---

### Cross-Timeline Analysis

| Path | Upside | Downside | Reversibility | Regret Risk | What Has to Cooperate |
|---|---|---|---|---|---|
| **A: OLAP-First** | Fastest to "50 GB on 8 GB" demo (2 weeks). Pure Rust, no protocol complexity. | No API compat for months. CLI-only = not a "database." | HIGH — Bolt can be added later without changing the engine. | LOW if OLAP is the differentiator. HIGH if users need drivers NOW. | NVMe perf, test dataset |
| **B: Bolt-First** | "Exact same APIs" proven early. Neo4j Browser connects. | 3-month delay to the OLAP story. Cypher parser is a rabbit hole. | MEDIUM — Bolt code is reusable, but Cypher scope is hard to reverse. | HIGH — if you spend 3 months on protocol and have nothing to show for RAM advantage. | Bolt docs, driver compatibility |
| **C: Vertical Slice** | Both stories in one release. Best demo: "driver → PageRank → 3.2 GB." | More scope than A. Two tracks must integrate cleanly. | HIGH — each track is independent, can be shipped separately. | LOWEST — covers both requirements simultaneously. | Both tracks finishing on time |

---

### Decision Filter

**Which path is strongest if everything goes normally?**

→ **Timeline C (Vertical Slice).** You get the best demo: a standard Neo4j
driver connects and runs PageRank on a 50 GB graph in 3.2 GB RAM.
This proves BOTH "exact same APIs" AND "lowest RAM" in one shot.

**Which path is safest if things go badly?**

→ **Timeline A (OLAP-First).** If Bolt turns out to be harder than expected,
you still have a working PageRank engine with incredible benchmarks.
The CLI demo alone is compelling: "50 GB in 3.2 GB, 15 seconds."
Bolt can always be added later.

**What experiment would reduce uncertainty fastest?**

→ **2-day Bolt spike.** Before committing to any timeline:
1. Implement minimal Bolt handshake (version negotiation + HELLO + LOGON)
2. Test: can `neo4j-driver-python` connect and receive a response?
3. If YES in 2 days → Timeline C is feasible, commit to it.
4. If NO (protocol dragons) → Fall back to Timeline A, add Bolt later.

---

## The Choices Within Each Fork

Once you pick a timeline (likely C), these sub-choices follow:

### If Timeline C:

| Sub-Choice | Recommended | Why |
|---|---|---|
| **API surface** | C → Procedures only for v0.0.3 | CALL dispatch is simple. MATCH parsing is scope-creepy. |
| **OLTP storage** | DEFER to v0.0.7 | Read-only CSR for now. Record stores aren't needed until writes. |
| **OLAP I/O** | All 3 levels | Level 1 (mmap) for dev, Level 2 (O_DIRECT) for production, Level 3 for extreme. |
| **Cypher scope** | C → Hand-rolled subset in v0.0.5 | CALL for v0.0.3, MATCH/RETURN for v0.0.5, WHERE for v0.0.6. |
| **Write path** | A → Read-only until v0.0.7 | Writes are important but NOT the differentiator. |
| **Async runtime** | A → tokio + rayon | tokio has the ecosystem (tonic, tower, bytes). rayon for compute. |
| **Properties** | B → Columnar | CSR-adjacent typed columns. Cache-friendly for OLAP scans. |

---

## The One Page Summary

```
8 choices. 3 forks. 1 recommendation.

FORK 1 (Build Order):    Timeline C — Vertical Slice (Bolt + PageRank together)
FORK 2 (Storage):        DEFER — CSR for now, decide Neo4j format at v0.0.7
FORK 3 (Cypher Scope):   Procedures only → subset → full (incremental)

First milestone (v0.0.3, 4 weeks):
  neo4j-driver-python → Bolt → CALL gds.pageRank.stream() → 3.2 GB RAM on 50 GB graph

Next experiment (2 days):
  Can a Neo4j driver handshake with our Rust Bolt server?
  YES → commit to Timeline C
  NO  → fall back to Timeline A (OLAP CLI first)
```

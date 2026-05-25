

# Structure

``` text
We will follow a minto pyramid

L1 will be small
L2 will be longer
L3 will be even longer and detailed

```

# L1 PRD

```
Neo4j rewritten in Rust

- exact same APIs or surface area with ZERO changes so that the same code can be used
- identical architecture for OLTP queries
- lowest RAM custom storage formats for OLAP queries
- community edition hence single node

```

## Supporting Documents (in docs_PRD02/)

- `Architecture-Dual-Engine.md` — Full dual-engine architecture (OLTP identical + OLAP lowest RAM)
- `1000IQ-OLAP-Architecture-Deep-Think.md` — Deep analysis: why 13 custom formats hurt "lowest RAM"

---

# L2 PRD: Neo4j Community Edition Breakdown + v0.0.3 Architecture Options

## FACT 1: The Neo4j Repo Is SINGLE NODE

**Confirmed by reading the actual code.** The repo at `neo4j-reference/neo4j/` contains
ONLY the `community/` directory. There is **no `enterprise/` directory** at all.

What's missing (enterprise-only, not in this repo):
- Raft consensus (causal clustering)
- Leader election
- Read replicas
- Backup/restore clustering
- Role-based access control (advanced)
- Multi-database with server routing

What IS present:
- **Fabric** — multi-database query routing on a SINGLE server (not clustering)
- All single-node database machinery
- Single-server Bolt connections

**Conclusion:** We are building a single-node replacement. Confirmed by both the
user's L1 PRD ("community edition hence single node") AND the source code.

---

## FACT 2: Neo4j Community = 2.09M LOC (10,686 source files)

Broken down by reading every module in `community/`:

### The 9 Functional Groups

```
GROUP 1: STORAGE ENGINE                          ~228K LOC
  kernel/                 121,729 LOC    Core database kernel
  record-storage-engine/   96,165 LOC    15B node / 34B rel record stores
  storage-engine-util/     11,206 LOC    Shared storage utilities

GROUP 2: QUERY LANGUAGE (CYPHER)                 ~670K LOC (32% of codebase!)
  front-end/parser         162,567 LOC    ANTLR4 (v25) + JavaCC (v5) Cypher grammar
  cypher-planner           181,802 LOC    Query planning + optimization (LARGEST module)
  interpreted-runtime       59,267 LOC    Interpreted query execution
  slotted-runtime           19,674 LOC    Slotted query execution
  cypher (other)            45,591 LOC    Core Cypher module
  runtime-util              35,381 LOC    Runtime utilities
  ir/                        9,740 LOC    Intermediate representation
  physical-planning         10,735 LOC    Physical plan generation
  logical-plan-builder       7,847 LOC    Logical plan construction
  cypher-logical-plans      14,605 LOC    Plan data structures
  spec-suite-tools          ~2,000 LOC    TCK compliance
  other cypher modules      ~8,000 LOC    Config, cache, rendering

GROUP 3: WIRE PROTOCOL (BOLT)                    ~73K LOC
  bolt/                    72,982 LOC    PackStream, protocol v3-v5, session state machine

GROUP 4: I/O LAYER                               ~46K LOC
  io/                      24,123 LOC    Page cache (Muninn), page cursors, prefetch
  wal/                     13,823 LOC    Write-ahead log, transaction log
  lock/                     8,386 LOC    Lock manager, deadlock detection

GROUP 5: DATA TYPES & API                        ~68K LOC
  values/                  33,192 LOC    Neo4j value types (AnyValue, TextValue, etc.)
  kernel-api/              35,297 LOC    Public kernel API (StorageEngine interface, etc.)

GROUP 6: INDEXING                                ~49K LOC
  index/                   21,735 LOC    Generic index infrastructure
  lucene-index/            19,204 LOC    Lucene-backed indexes
  fulltext-index/           5,176 LOC    Fulltext search
  spatial-index/            3,087 LOC    Spatial index (Point, WGS-84)

GROUP 7: SERVER & INFRASTRUCTURE                 ~110K LOC
  server/                  29,702 LOC    HTTP REST endpoints, browser serving
  configuration/           16,221 LOC    neo4j.conf (~400 settings)
  dbms/                    15,139 LOC    Database management
  neo4j/                   16,166 LOC    Server bootstrap
  import-util/             32,198 LOC    CSV/data import utilities
  common/                  21,824 LOC    Shared utilities
  concurrent/               2,832 LOC    Concurrency primitives

GROUP 8: ALGORITHMS                              ~7K LOC
  graph-algo/               7,159 LOC    Dijkstra, A*, BFS, Floyd-Warshall, shortest paths
  NOTE: PageRank, Louvain, etc. are in neo4j-gds (SEPARATE repo, not included)

GROUP 9: TOOLING                                 ~31K LOC
  cypher-shell/            25,088 LOC    Interactive Cypher REPL
  import-tool/              4,862 LOC    neo4j-admin import CLI
  command-line/             1,315 LOC    CLI framework
```

---

## FACT 3: Key On-Disk Formats (Confirmed from Source)

**Node Record: 15 bytes** (`NodeRecordFormat.java`)
```
in_use(1B) + next_rel_id(4B) + next_prop_id(4B) + labels(5B) + extra(1B)
```
Fields: `nextRel` (pointer to first relationship), `labels`, `dense` flag

**Relationship Record: 34 bytes** (`RelationshipRecordFormat.java`)
```
header(1B) + firstNode(4B) + secondNode(4B) + rel_type(4B)
+ firstPrevRel(4B) + firstNextRel(4B) + secondPrevRel(4B) + secondNextRel(4B)
+ nextProp(4B) + chain_markers(1B)
```
**FOUR linked-list pointers** per relationship: doubly-linked from both source and target node.
This is the structural reason traversal is slow — every hop follows scattered pointers.

**StorageEngine is PLUGGABLE** (`StorageEngineFactory.java`):
- Uses Java service loading (`allAvailableStorageEngines()`)
- `StorageEngine` interface: `name()`, `id()`, `apply()`, `createCommands()`, `checkpoint()`
- In theory, a CSR-based StorageEngine could plug into Neo4j's existing Cypher/Bolt stack

**Cypher Grammar:**
- ANTLR4: `Cypher25Parser.g4` (2,024 lines) — the grammar definition
- JavaCC: `cypher.jj` (7,898 lines) — older v5 grammar
- Total parser module: 162K LOC (mostly generated + test infrastructure)

**Algorithms in community:**
- Dijkstra, A*, BFS, Floyd-Warshall, shortest paths only
- NO PageRank, Louvain, triangle count, etc. (those are in neo4j-gds, separate product)

---

## FACT 4: What Knight Bus Has Today (v0.0.2)

Source: `WIKI.md` Section I, `docs_PRD01/Knight-Bus-Inventory-and-Gap-Analysis.md`

- 4,710 LOC Rust + 1,703 LOC low_ram.rs + 532 LOC tests
- 4 traits: `WalkQueryRuntime`, `GraphTruth`, `SnapshotBuilder`, `BenchmarkRunner`
- Dual CSR storage (forward + reverse), mmap, binary search key lookup
- Proven benchmark: p99 33,695x faster, 4.5x less RAM on 2 GB dataset
- External merge-sort for bounded-memory builds
- NO Cypher, NO Bolt, NO properties, NO variable-length paths, NO writes

---

## FACT 5: What the L1 PRD Demands

```
1. "similar performance for OLTP queries"    → Need Bolt + Cypher + OLTP path
2. "lowest RAM for OLAP queries"             → Need PageRank etc. on CSR
3. "exact same APIs with ZERO changes"       → Need Bolt protocol + Cypher parser
4. "community edition hence single node"     → Confirmed: no clustering needed
```

---

# Timeline Traverser: v0.0.3 Architecture Options

## Decision Frame

- **Fork in the road:** Given 2.09M LOC of Neo4j to replace and 4,710 LOC today,
  what does v0.0.3 deliver? Which SLICE of the full system do we build first?

- **Desired outcome:** v0.0.3 is a credible step toward "Neo4j Community rewritten
  in Rust" that:
  (a) proves a real technical advantage
  (b) is shippable in 2-4 weeks
  (c) doesn't paint us into a corner for the full vision

- **Hard constraints:**
  - Single-node (confirmed by source code — no clustering in community/)
  - Must eventually reach "exact same API" (L1 PRD)
  - Today: read-only, CLI-only, no Cypher, no Bolt
  - 1-person team
  - Wiki says: "PageRank first" / "~800 LOC, 7-10 days"
  - But L1 PRD says: "exact same APIs with ZERO changes"

- **Time horizon:** v0.0.3 → v0.0.5 → v0.1.0 → v1.0

- **What would count as failure:**
  - Building analytics-only and never reaching API compatibility
  - Building Cypher parser for 6 months with nothing to show
  - Shipping a demo nobody can connect to with existing tools
  - Architecture choices that block the OLAP or OLTP path later

---

## Timeline A: "Analytics Sidecar" (What the wiki recommends)

**Opening move:** PageRank on existing CSR. CLI-only. No Bolt, no Cypher.

### Week 1-2 (v0.0.3)
- `page_rank.rs` — Jacobi PageRank with rayon (~120 LOC)
- `synthetic.rs` — random graph generator (~100 LOC)
- Benchmark: 10M nodes, 100M edges. Record wall time + RSS.
- **Deliverable:** `knight-bus pagerank --snapshot /path/to/snapshot`
- **Headline:** "PageRank in 10 sec, 720 MB. Neo4j GDS: 90 sec, 12 GB."
- Total new LOC: ~800

### Month 1 (v0.0.4)
- Add Dijkstra + BFS (~400 LOC)
- Python bindings via PyO3 (~400 LOC)
- Users: `import knight_bus; kb.pagerank("graph.snapshot")`

### Quarter 1 (v0.0.5-v0.0.7)
- Overlay model for mutable writes (~400 LOC)
- Basic Bolt server (~2,000 LOC) — first time Neo4j drivers can connect
- First Cypher subset: `CALL knight_bus.pagerank.stream()` (~500 LOC)

### Year 1 (v0.1.0+)
- Full Cypher subset for analytics queries
- OLTP record store + WAL
- Full Bolt protocol compliance

### Likelihood: 70%

### Stress points:
- "Exact same APIs" is deferred to Q2+. User might lose patience.
- Python bindings are an API DIFFERENT from Neo4j's.
- Going viral requires Neo4j driver compatibility, which is months away.

### Inflection points:
- **Week 3:** If benchmark numbers are strong, momentum carries forward.
- **Month 2:** If nobody downloads it, "analytics sidecar" positioning may be wrong.
- **Month 3:** When Bolt work starts, discover hidden complexity in PackStream.

---

## Timeline B: "Bolt-First Compatibility" (What the L1 PRD demands)

**Opening move:** Minimal Bolt server. Neo4j drivers connect from Day 1.

### Week 1-2 (v0.0.3)
- Bolt v5 handshake + PackStream serialization (~1,500 LOC)
- Session state machine (HELLO → READY → STREAMING) (~500 LOC)
- 3 hardcoded queries on existing CSR:
  - `MATCH (n) RETURN n LIMIT 100` → scan key_index
  - `MATCH (n {id: $id})-[r]->(m) RETURN m` → binary search + forward CSR
  - `MATCH (n)-[r]->(m) RETURN count(*)` → count from offsets
- **Deliverable:** `neo4j-driver` connects, runs 3 queries, gets results
- Total new LOC: ~2,500

### Month 1 (v0.0.4)
- Cypher subset parser using ANTLR4 grammar (port `Cypher25Parser.g4`) (~2,000 LOC)
- Parse MATCH/RETURN/WHERE for simple patterns
- PageRank as a procedure (`CALL knight_bus.pagerank.stream()`)

### Quarter 1 (v0.0.5-v0.0.7)
- Expand Cypher coverage: CREATE, SET, DELETE for writes
- Property storage + property indexes
- Transaction isolation (snapshot reads)

### Year 1 (v0.1.0+)
- Full Cypher 25 compliance
- WAL + crash recovery
- Lucene-compatible fulltext search

### Likelihood: 40%

### Stress points:
- **Bolt is well-documented** but the state machine has edge cases (auth, routing,
  bookmarks, multi-database). Easy to get 80% right, hard to get 100%.
- **Cypher parser in Month 1** is very aggressive. The ANTLR grammar is 2,024 lines
  but the planner is 181K LOC. Parsing without planning is useless — you get an AST
  but can't execute it.
- **No benchmark headline.** "Our Bolt server returns 3 hardcoded queries" doesn't go viral.
- **PackStream is a custom binary format.** Not JSON, not protobuf. Must implement exactly.

### Inflection points:
- **Week 1:** If PackStream serialization takes >3 days, timeline slips significantly.
- **Month 1:** If Cypher parser works but planner is missing, users get "not supported"
  errors on most real queries.
- **Month 2:** If we have Bolt + basic Cypher + PageRank, this is genuinely compelling.

---

## Timeline C: "Vertical Slice" (Bolt + PageRank + CSR in one release)

**Opening move:** Ship a narrow but COMPLETE vertical slice — a Bolt server that
runs PageRank over CSR, connectable with standard Neo4j drivers.

### Week 1-2 (v0.0.3)
- Minimal Bolt server: handshake, HELLO, RUN, PULL (~1,200 LOC)
- PackStream encoder/decoder for basic types (~400 LOC)
- PageRank on existing CSR (~150 LOC)
- Wire it together: `CALL knight_bus.pagerank.stream({graph: 'default'})` → Bolt result stream
- Synthetic graph generator (~100 LOC)
- **Deliverable:** `neo4j-driver` connects, runs PageRank, streams ranked nodes
- **Headline:** "Connect with your existing Neo4j driver. Run PageRank in 10 seconds."
- Total new LOC: ~2,000-2,500

### Month 1 (v0.0.4)
- Expand Bolt to handle more message types (RESET, DISCARD, ROUTE)
- Add 3-5 more procedures: Dijkstra, BFS, degree centrality
- Basic `MATCH` pattern execution for 1-hop queries (reuse existing `query_entity_neighbors`)
- Benchmark suite comparing procedure calls: KB vs Neo4j GDS

### Quarter 1 (v0.0.5-v0.0.7)
- Cypher subset parser for read queries
- Property storage (initially read-only from CSV-imported properties)
- Overlay model for mutable writes
- Multi-query Bolt sessions

### Year 1 (v0.1.0+)
- Full Cypher analytics subset
- OLTP write path
- Schema constraints
- Full Bolt protocol compliance

### Likelihood: 55%

### Stress points:
- **2 weeks is tight** for Bolt + PackStream + PageRank. More realistic: 3 weeks.
- **"CALL procedure" is the EASIEST Cypher path** — no parser needed.
  Bolt RUN message contains the query string. We pattern-match
  `CALL knight_bus.*` and dispatch directly. No ANTLR needed.
- **Correctness risk:** Bolt protocol edge cases (chunked messages, error handling,
  transaction state) could consume debugging time.

### Inflection points:
- **Day 3:** If PackStream works and `neo4j-driver` connects, momentum is huge.
- **Week 2:** If PageRank numbers over Bolt are good (10 sec, 720 MB), we have both
  compatibility AND performance in one release.
- **Month 1:** Real users try it. Do they ask for more algorithms or more Cypher?
  This determines whether we go deeper on analytics or wider on query language.

---

## Timeline D: "Hybrid JNI Bridge" (Use Neo4j's Cypher, Replace Storage)

**Opening move:** Since `StorageEngine` is pluggable (confirmed in source code),
implement Knight Bus as a Rust storage engine that plugs into the Java Neo4j server
via JNI/FFI.

### Week 1-4 (v0.0.3)
- JNI bridge: Java `StorageEngine` → Rust CSR reader (~2,000 LOC Java + 1,000 LOC Rust)
- Implement `StorageEngine` interface: `name()`, `apply()`, `createCommands()`,
  `StorageReader` for reading nodes/relationships from CSR
- Run existing Neo4j server with Knight Bus storage backend
- All Cypher, all Bolt, all tooling works DAY ONE
- **Deliverable:** Standard Neo4j server, but 10-100x faster traversal
- Total new LOC: ~3,000 (split Java/Rust)

### Month 1-2 (v0.0.4)
- Write path: intercept `apply()` → append to overlay → periodic CSR rebuild
- Property storage mapping
- Handle all record types (nodes, rels, properties, labels, rel types)

### Quarter 1-2 (v0.0.5-v0.1.0)
- Optimize JNI overhead (batch operations, memory-mapped shared buffers)
- WAL integration
- Performance tuning: JNI boundary crossing latency

### Year 1 (v0.1.0+)
- Gradually port Java layers to Rust (Bolt, then Cypher runtime)
- Eventually eliminate JNI bridge
- Pure Rust when ready

### Likelihood: 25%

### Stress points:
- **JNI is a PAIN.** Every call crosses a language boundary. For traversal-heavy
  workloads, JNI overhead could eat the CSR performance advantage.
- **StorageEngine interface is HUGE:** 248 lines of interface, ~30 methods,
  depends on PageCursor, CursorContext, MemoryTracker, LockService, etc.
  Implementing this correctly is likely 10K+ LOC, not 3K.
- **GC interaction:** Rust CSR data is off-heap from JVM's perspective.
  JNI pinned memory can interfere with G1GC.
- **Deployment:** Users must install JVM + Rust binary. Loses the "single binary" story.
- **License:** Neo4j Community is GPL v3. Using it as a library forces GPL on our code
  (which may be fine, but it's a strategic constraint).

### Inflection points:
- **Week 1:** If StorageEngine mock works in JNI, the architecture is viable.
- **Week 3:** If JNI overhead >1ms per traversal call, the performance advantage vanishes.
- **Month 2:** If write path through JNI is stable, this is the fastest path to full
  Neo4j compatibility by LOC — we inherit 2M LOC of working code.

---

## Cross-Timeline Analysis

| Path | Upside | Downside | Reversibility | Regret Risk | Who/What Must Cooperate |
|---|---|---|---|---|---|
| **A: Analytics Sidecar** | Ships in 7-10 days. Proves OLAP thesis. Viral benchmark numbers. | No API compatibility for months. "Exact same APIs" deferred. | **HIGH** — nothing in A blocks B/C/D later | Low: worst case, we have a fast analytics tool | Just us (no external dependencies) |
| **B: Bolt-First** | Neo4j drivers connect from Day 1. Matches L1 PRD "exact same APIs." | No performance headline for v0.0.3. Bolt + Cypher parser = months of plumbing. | **HIGH** — Bolt server is reusable in all paths | Medium: 2-3 weeks on protocol work with no benchmark to show | PackStream spec accuracy, Neo4j driver behavior |
| **C: Vertical Slice** | Best of both: compatibility + performance in one release. "Connect with Neo4j driver, run PageRank in 10 sec." | Tight timeline (3 weeks realistic). Bolt subset might have edge cases. | **HIGH** — all components reuse in later versions | Low: even partial success is valuable | PackStream spec, Bolt handshake behavior |
| **D: JNI Bridge** | Full Neo4j compatibility immediately. All Cypher, all Bolt, all tooling. | JNI overhead may kill performance. GPL constraint. Complex deployment. 10K+ LOC to implement StorageEngine. | **LOW** — JNI bridge is throwaway work | High: if JNI kills perf, months wasted on bridge nobody wants | JVM, JNI stability, StorageEngine interface completeness |

---

## Decision Filter

### Which path is strongest if everything goes normally?
**Timeline C (Vertical Slice).** It proves BOTH theses in one release: "exact same API"
(Neo4j drivers connect) AND "10x faster analytics" (PageRank benchmark). The CALL
procedure path cleverly avoids the Cypher parser entirely — pattern-match on the query
string, dispatch to Rust functions. No ANTLR needed for v0.0.3.

### Which path is safest if things go badly?
**Timeline A (Analytics Sidecar).** If Bolt/PackStream turns out harder than expected,
Timeline A still ships in 7-10 days with a concrete benchmark. Worst case: we have a
fast CLI analytics tool while we figure out protocol compatibility.

### What experiment would reduce uncertainty fastest?
**2-day Bolt spike (before committing to any timeline):**
1. Day 1: Implement Bolt v5 handshake + HELLO/LOGON in Rust. Can `neo4j-driver` connect?
2. Day 2: Implement RUN + PULL for one hardcoded query. Does the driver parse the result?

If this works, Timeline C is viable → commit to it.
If this fails (PackStream edge cases, auth requirements), fall back to Timeline A.

---

## The Recommended Build Order (Combining A + C)

```
PHASE 0: Bolt Spike (2 days)
  → Answers: "Can we speak Bolt at all?"
  → If YES: proceed to Phase 1
  → If NO: fall back to pure Analytics Sidecar (Timeline A)

PHASE 1: v0.0.3 — Vertical Slice (2-3 weeks)
  Bolt server (minimal) + PageRank over CSR + Benchmark
  → Neo4j drivers connect and run CALL knight_bus.pagerank.stream()
  → Headline: "10 sec, 720 MB. Connect with your existing driver."

PHASE 2: v0.0.4 — Expand Procedures (2-3 weeks)
  More algorithms (Dijkstra, BFS, degree centrality)
  Basic MATCH pattern execution for 1-hop queries
  Python bindings via PyO3

PHASE 3: v0.0.5 — Cypher Subset Parser (3-4 weeks)
  Port Cypher25Parser.g4 ANTLR grammar to Rust
  Parse MATCH/RETURN/WHERE for read queries
  Property storage (read-only)

PHASE 4: v0.1.0 — Write Path (4-6 weeks)
  OLTP record store + overlay model
  WAL + crash recovery
  CREATE/SET/DELETE support
```

### What NOT to build for v0.0.3:
- Full Cypher parser (Group 2 is 670K LOC — DEFER)
- Property storage (DEFER to v0.0.5)
- Write path / transactions (DEFER to v0.1.0)
- Lucene indexes (DEFER to v0.1.0+)
- HTTP/REST server (DEFER — Bolt is the primary API)
- cypher-shell (DEFER — use Neo4j's existing cypher-shell if Bolt works)
- JNI bridge (Option D rejected — too much throwaway work)

---

# L3 PRD: Detailed Specification

*To be written after Timeline selection is confirmed.*

*Will contain: exact Bolt message types for v0.0.3, PackStream type mapping,*
*procedure signatures, benchmark methodology, acceptance criteria.*

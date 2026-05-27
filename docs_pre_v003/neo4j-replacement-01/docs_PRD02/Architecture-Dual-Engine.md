# Knight Bus Architecture: Dual-Engine Neo4j Replacement

*Generated using the Timeline Traverser playbook.*
*Every claim grounded in reading the actual Neo4j community source code (2.09M LOC).*

---

## Core Facts Enumerated

### What I know:

1. **Neo4j Community = 2.09M LOC, SINGLE NODE** (confirmed: no `enterprise/` directory)
2. **Neo4j record format** (confirmed from source):
   - NodeRecord: 15B (`in_use:1 + next_rel:4 + next_prop:4 + labels:5 + extra:1`)
   - RelationshipRecord: 34B (`header:1 + first_node:4 + second_node:4 + rel_type:4 + 4_linked_list_ptrs:16 + next_prop:4 + chain_markers:1`)
   - PropertyRecord: 41B (`high_bits:1 + next:4 + prev:4 + payload:32`)
   - RelationshipGroupRecord: 25B (dense node degree groups)
3. **NeoStores has 15 store types**: NODE, NODE_LABEL, RELATIONSHIP, RELATIONSHIP_GROUP, RELATIONSHIP_TYPE_TOKEN, RELATIONSHIP_TYPE_TOKEN_NAME, PROPERTY, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_NAME, PROPERTY_STRING, PROPERTY_ARRAY, LABEL_TOKEN, LABEL_TOKEN_NAME, SCHEMA, META_DATA
4. **StorageEngine is pluggable** (interface with service loading — `StorageEngineFactory`)
5. **Page cache: Muninn** — custom off-heap page cache with clock-sweep eviction, page locking, prefetching
6. **Cypher grammar**: ANTLR4 (`Cypher25Parser.g4`, 2,024 lines) + JavaCC (`cypher.jj`, 7,898 lines)
7. **Bolt versions**: v4.0, v4.1, v4.2, v4.4, v5.0, v5.3 with PackStream binary serialization
8. **graph-algo/**: Only Dijkstra, A*, BFS, Floyd-Warshall. NO GDS (PageRank, Louvain, etc.)
9. **Knight Bus today**: 4,710 LOC Rust, dual CSR, mmap, read-only, CLI-only
10. **Wiki says**: 13 layout families are the moat. OLAP-RAM and OLAP-Latency are the same engine with different mmap hints.
11. **HTAP precedent**: TiDB+TiFlash, Oracle In-Memory, AlloyDB, GART (USENIX ATC'23)

### What I was missing (and went to get):

1. **Property record layout**: 41B — 32B payload holds up to 4 property blocks inline. Overflow goes to DynamicStringStore/DynamicArrayStore. Linked list: `next + prev` pointers.
2. **Dense node pattern**: When a node has >50 relationships, Neo4j switches from a flat linked list to RelationshipGroups (25B each, one per relationship type). Groups contain pointers to OUTGOING, INCOMING, and LOOP chains.
3. **Bolt state machine**: NEGOTIATION → AUTHENTICATION → READY → STREAMING → TX_READY → TX_STREAMING → FAILED. FSM (Finite State Machine) pattern — each state handles specific message types.
4. **Cypher planner is 181K LOC** — the LARGEST single module. This is where cost-based optimization, join strategies, and index selection happen. Cannot be trivially ported.

---

## The L1 Requirements (Updated)

```
Neo4j rewritten in Rust

1. exact same APIs or surface area with ZERO changes
2. identical architecture for OLTP queries
3. lowest RAM custom storage formats for OLAP queries
4. community edition hence single node
```

---

## Architecture: The Dual-Engine Design

### High-Level Diagram

```
                    ┌──────────────────────────────────┐
                    │         CLIENT LAYER              │
                    │    Neo4j Drivers (unchanged)      │
                    │    cypher-shell, Browser, etc.    │
                    └──────────────┬───────────────────┘
                                   │ Bolt v4.0-v5.3
                                   │ PackStream binary
                    ┌──────────────▼───────────────────┐
                    │         BOLT SERVER               │
          L1        │  PackStream codec                 │
                    │  Session state machine            │
                    │  Connection pool (tokio)          │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │       CYPHER ENGINE                │
          L2        │  Parser (ANTLR4 grammar port)     │
                    │  Planner (cost-based)              │
                    │  Execution engine                  │
                    └────────┬─────────────┬────────────┘
                             │             │
                    ┌────────▼──┐    ┌─────▼────────────┐
                    │  QUERY    │    │  PROCEDURE        │
          L3        │  ROUTER   │    │  DISPATCHER       │
                    │           │    │  CALL gds.*       │
                    └──┬────┬──┘    └──────┬────────────┘
                       │    │              │
            ┌──────────▼┐  ┌▼──────────┐  ┌▼──────────────┐
            │   OLTP    │  │   OLAP    │  │   OLAP        │
            │   ENGINE  │  │   ENGINE  │  │   ENGINE      │
   L4       │(identical │  │(lowest    │  │  (algorithm   │
            │ arch)     │  │ RAM CSR)  │  │   procedures) │
            └─────┬─────┘  └─────┬─────┘  └──────┬───────┘
                  │              │                │
            ┌─────▼──────────────▼────────────────▼───────┐
            │              SYNC BRIDGE                     │
   L5       │  WAL consumer → CSR snapshot rebuilder       │
            │  Snapshot versioning (old serves during build)│
            └──────────────────────────────────────────────┘
```

---

### L4a: OLTP Engine — "Identical Architecture"

The OLTP engine replicates Neo4j's record store architecture in Rust.
Same concepts, same record types, same traversal semantics.

#### On-Disk Format (Matching Neo4j)

```
data/
├── neostore.nodestore.db            # NodeRecord (15B fixed)
│   ├── in_use: u8                   # 1B
│   ├── next_rel: u32               # 4B (pointer to first relationship)
│   ├── next_prop: u32              # 4B (pointer to first property)
│   ├── labels: [u8; 5]             # 5B (inline or pointer to dynamic)
│   └── extra: u8                   # 1B (dense flag)
│
├── neostore.relationshipstore.db    # RelationshipRecord (34B fixed)
│   ├── header: u8                   # 1B (in_use + direction)
│   ├── first_node: u32             # 4B
│   ├── second_node: u32            # 4B
│   ├── rel_type: u32               # 4B
│   ├── first_prev_rel: u32         # 4B ─┐
│   ├── first_next_rel: u32         # 4B  │ doubly-linked from
│   ├── second_prev_rel: u32        # 4B  │ both endpoints
│   ├── second_next_rel: u32        # 4B ─┘
│   ├── next_prop: u32              # 4B
│   └── chain_markers: u8           # 1B
│
├── neostore.propertystore.db        # PropertyRecord (41B fixed)
│   ├── high_bits: u8               # 1B
│   ├── next: u32                   # 4B (→ next property record)
│   ├── prev: u32                   # 4B (→ prev property record)
│   └── payload: [u8; 32]           # 32B (up to 4 PropertyBlocks)
│
├── neostore.relationshipgroupstore.db  # RelGroupRecord (25B fixed)
│   # For dense nodes (>50 rels): one group per rel type
│
├── neostore.labeltokenstore.db      # Label tokens
├── neostore.relationshiptypestore.db # Relationship type tokens
├── neostore.propertykeytokenstore.db # Property key tokens
├── neostore.schemastore.db          # Schema rules (indexes, constraints)
└── neostore                         # Metadata store
```

#### Where Rust Replaces Java (Same Architecture, Better Runtime)

| Component | Neo4j (Java) | Knight Bus (Rust) | Why Better |
|---|---|---|---|
| **Page Cache** | Muninn (custom, 24K LOC, off-heap, clock-sweep) | `mmap` (OS page cache) | Zero custom code. OS is better at page eviction. No GC interference. |
| **Record I/O** | PageCursor → off-heap buffer → copy to Java objects | `mmap` → zero-copy `&[u8]` slice | No object allocation per read. No GC pressure. |
| **WAL** | Java FileChannel + fsync | Rust File + fsync (or io_uring at v0.1.0) | No GC pauses during fsync. Direct syscall. |
| **Lock Manager** | Java ReentrantLock + deadlock detection | Rust parking_lot::RwLock | 2-3x faster uncontended. No monitor overhead. |
| **ID Generator** | FreeList + ID files | AtomicU64 + freelist | Lock-free for common case. |
| **Memory** | JVM heap (G1GC, 8-16B object headers) | Direct allocation (zero overhead) | Node record: 15B on disk = 15B in memory (vs 15B + 16B object header in Java) |

#### What This Means for OLTP Performance

Neo4j's OLTP architecture is sound for point operations:
- `MATCH (n {id: 123}) RETURN n` → single record read → O(1)
- `MATCH (n)-[r]->(m)` → follow next_rel pointer → O(degree)
- `CREATE (n:Person {name: "Alice"})` → allocate node record + property record + WAL entry

**We keep this architecture IDENTICAL.** The speed improvement comes from:
1. No GC pauses (Rust has no garbage collector)
2. No object header overhead (15B node record = 15B, not 15B + 16B)
3. mmap vs Muninn (simpler, let OS optimize)
4. Zero-copy reads (mmap slice vs PageCursor copy)

**Expected OLTP improvement: 1.5-3x** (same architecture, better runtime).
This is NOT the main selling point — OLAP is.

---

### L4b: OLAP Engine — "Lowest RAM Custom Storage Formats"

The OLAP engine is a COMPLETELY DIFFERENT storage architecture optimized
for graph algorithms. This is where the 10-100x advantage lives.

#### On-Disk Format (CSR — Already Proven in v0.0.2)

```
olap/
├── manifest.json
├── forward.offsets.bin     # u64[node_count + 1]   (CSR offsets)
├── forward.peers.bin       # u32[edge_count]       (CSR adjacency)
├── reverse.offsets.bin     # u64[node_count + 1]
├── reverse.peers.bin       # u32[edge_count]
├── node_table.bin          # Fixed-width node records
├── key_index.bin           # Sorted key → dense_id mapping
├── strings.bin             # Concatenated UTF-8 keys
└── layouts/                # Algorithm-specific formats (v0.0.5+)
    ├── inbound_power/      # Family #2: PageRank-optimized
    ├── relaxation_frontier/ # Family #7: Dijkstra-optimized
    └── ...                 # 11 more families
```

#### The 3 Levels of RAM Optimization

Source: `docs_PRD02/Why-Compio-IS-Right-For-OLAP-RAM.md`,
        `docs_PRD02/1000IQ-Rubber-Duck-Lowest-RAM-Wins.md`

| Level | I/O Model | RAM (500M edges) | Speed | When |
|---|---|---|---|---|
| **Level 1: mmap** | mmap + rayon | 160 MB heap + variable OS | 8-22 sec | v0.0.3 |
| **Level 2: O_DIRECT** | compio + rayon | **161 MB exact** | 10-25 sec | v0.0.5 |
| **Level 3: Edge-centric** | compio + sort | **41 MB exact** | 150-260 sec | v0.1.0 |

Level 3 is the headline: **PageRank on 1 BILLION nodes in 41 MB of RAM.**

#### Why Neo4j Can't Do This

Source: `docs_PRD02/1000IQ-The-Deeper-Insight.md`

Neo4j GDS ALREADY uses CSR internally. But:
1. It rebuilds CSR from linked-list records every session (60-120 sec, 2-4 GB heap)
2. JVM `MappedByteBuffer` has 2 GB limit
3. G1GC interferes with mmap'd regions
4. `sun.misc.Unsafe` (used for off-heap) is being deprecated

**The JVM makes the fix IMPOSSIBLE.** Rust makes it trivial: `memmap2::Mmap` is a `&[u8]`.

---

### L3: Query Router

The query router decides which engine handles each query:

```
OLTP path (record store):
  MATCH (n {id: X}) RETURN n              → point lookup
  MATCH (n)-[r]->(m) RETURN m             → linked-list traversal
  CREATE (n:Person {name: "Alice"})       → record allocation + WAL
  SET n.age = 30                          → property update + WAL
  DELETE r                                → record deallocation + WAL
  Any query with WHERE on indexed property → index lookup → record read

OLAP path (CSR engine):
  CALL gds.pageRank.stream()              → CSR + rayon PageRank
  CALL gds.shortestPath.dijkstra.stream() → CSR + priority queue
  CALL gds.triangleCount.stream()         → CSR + sorted intersection
  CALL gds.louvain.stream()               → CSR + modularity optimization
  Any CALL gds.* or CALL knight_bus.*     → procedure dispatch → OLAP

HYBRID (both engines):
  MATCH (n) WHERE gds.pageRank(n) > 0.5  → OLAP computes scores → OLTP filters
  → v0.1.0+ feature
```

**The routing rule is simple at v0.0.3:**
- `CALL *` → procedure dispatcher → OLAP engine
- Everything else → OLTP engine

---

### L2: Cypher Engine

#### Parser Strategy

The Cypher25Parser.g4 grammar is 2,024 lines of ANTLR4.
Three options:

| Option | Tool | LOC | Risk |
|---|---|---|---|
| **A: Port ANTLR grammar to Rust** | `antlr4-rust` or manual port | ~3,000-5,000 | Medium — ANTLR4 Rust target exists but is less mature |
| **B: Use tree-sitter-cypher** | tree-sitter | ~500 setup | Low — incremental parsing, good error recovery |
| **C: Write Cypher parser from scratch** | Pratt parser / nom | ~5,000-8,000 | High — Cypher grammar has many edge cases |

**Recommendation: Option A** — port the ANTLR grammar. It's the authoritative
definition. We can validate against Neo4j's TCK (Technology Compatibility Kit)
spec suite (122K LOC of tests).

#### Planner Strategy

The planner (181K LOC) is the hardest part. Phased approach:

```
v0.0.3: No planner. Pattern-match CALL procedures directly.
v0.0.5: Simple planner for single-pattern MATCH queries.
v0.0.7: Cost-based planner for multi-pattern queries.
v0.1.0: Index-aware planner.
v1.0:   Full planner parity with Neo4j.
```

---

### L1: Bolt Server

#### Protocol Layers

```
TCP connection
  └─ Bolt handshake (4 magic bytes + version negotiation)
       └─ PackStream codec (binary serialization)
            └─ Message types:
                 HELLO / LOGON / GOODBYE       (auth)
                 RUN / PULL / DISCARD          (queries)
                 BEGIN / COMMIT / ROLLBACK     (transactions)
                 RESET                         (error recovery)
                 ROUTE                         (server routing)
```

#### PackStream Types We Must Support

```
Null, Boolean, Integer (i64), Float (f64), String, Bytes,
List, Map, Node, Relationship, Path, UnboundRelationship,
Date, Time, LocalTime, DateTime, LocalDateTime, Duration, Point2D, Point3D
```

**Node encoding:** `(id: i64, labels: [String], properties: {String: Any})`
**Relationship encoding:** `(id: i64, start_node_id: i64, end_node_id: i64, type: String, properties: {String: Any})`

---

### L5: Sync Bridge (OLTP → OLAP)

Source: `docs_PRD02/User-Journey-50GB-OLTP-OLAP-Lag.md`,
        `docs_PRD02/Timeline-OLTP-OLAP-Split.md`

```
OLTP writes → WAL → Sync Bridge reads WAL → Rebuilds CSR snapshot
                                          → Old snapshot serves queries during rebuild
                                          → Atomic swap when new snapshot ready

Staleness: 2-5 min (configurable rebuild interval)
Rule: NEVER block OLAP queries. Stale results > blocked results.
```

Phased implementation:
```
v0.0.3:  Manual rebuild (CLI: knight-bus rebuild)
v0.0.5:  Auto rebuild every N minutes
v0.0.7:  Overlay model (zero staleness, Grafeo-style)
v0.1.0:  Incremental CSR update from WAL deltas
```

---

## The 15 Store Files — Rust Module Map

| Neo4j Store File | Record Size | Rust Module | OLTP? | OLAP? |
|---|---|---|---|---|
| `neostore.nodestore.db` | 15B | `oltp/node_store.rs` | YES | Feeds CSR build |
| `neostore.relationshipstore.db` | 34B | `oltp/rel_store.rs` | YES | Feeds CSR build |
| `neostore.propertystore.db` | 41B | `oltp/prop_store.rs` | YES | Properties in OLAP (v0.0.5) |
| `neostore.propertystore.db.strings` | dynamic | `oltp/dynamic_string.rs` | YES | NO |
| `neostore.propertystore.db.arrays` | dynamic | `oltp/dynamic_array.rs` | YES | NO |
| `neostore.relationshipgroupstore.db` | 25B | `oltp/rel_group_store.rs` | YES | Feeds CSR build |
| `neostore.labeltokenstore.db` | token | `oltp/token_store.rs` | YES | Label mapping |
| `neostore.labeltokenstore.db.names` | dynamic | `oltp/token_store.rs` | YES | Label mapping |
| `neostore.relationshiptypestore.db` | token | `oltp/token_store.rs` | YES | Type mapping |
| `neostore.relationshiptypestore.db.names` | dynamic | `oltp/token_store.rs` | YES | Type mapping |
| `neostore.propertykeytokenstore.db` | token | `oltp/token_store.rs` | YES | NO |
| `neostore.propertykeytokenstore.db.names` | dynamic | `oltp/token_store.rs` | YES | NO |
| `neostore.schemastore.db` | variable | `oltp/schema_store.rs` | YES | NO |
| `neostore` (metadata) | 16B | `oltp/meta_store.rs` | YES | NO |
| `neostore.counts.db` | variable | `oltp/counts_store.rs` | YES | NO |

---

## Timeline Traverser: Build Order Options

### Decision Frame

- **Fork in the road:** Given the dual-engine architecture, what do we build FIRST?
  OLTP (identical architecture), OLAP (lowest RAM), or Bolt (exact same API)?

- **Desired outcome:** Each version is shippable, demonstrable, and moves toward
  full Neo4j compatibility.

- **Hard constraints:**
  - Must eventually have ALL layers (Bolt + Cypher + OLTP + OLAP + Sync)
  - 1-person team
  - Current codebase: 4,710 LOC (OLAP engine only)

- **Time horizon:** v0.0.3 (2-3 weeks) → v0.0.5 (2 months) → v0.1.0 (4 months)

- **What would count as failure:**
  - Building OLTP for 3 months without anyone being able to connect
  - Building Bolt for 2 months without any performance story
  - Building OLAP without any compatibility story

---

### Timeline A: "Bottom-Up" (OLTP First, Then Bolt)

**Opening move:** Build the record store engine in Rust. Identical to Neo4j's architecture.

- **Week 1-2:** `oltp/node_store.rs` + `oltp/rel_store.rs` — mmap'd 15B/34B records
- **Week 3-4:** `oltp/prop_store.rs` + token stores — property CRUD
- **Month 2:** WAL + transaction manager — crash recovery, isolation
- **Month 3:** Bolt server + minimal Cypher — first time drivers connect
- **Month 4:** OLAP engine integration + sync bridge

**Likelihood:** 50%
**Stress:** 2 months before any external user can connect. Building database internals
with no way to test against real queries.
**Inflection:** If record store works at 50M nodes (Month 1), OLTP performance story is real.

---

### Timeline B: "Top-Down" (Bolt First, Then Engines)

**Opening move:** Build the Bolt server + PackStream + minimal query handling.

- **Week 1-2:** Bolt handshake + HELLO + RUN/PULL + PackStream codec
- **Week 3-4:** CALL procedure dispatcher + PageRank (OLAP)
- **Month 2:** Cypher parser (ANTLR4 port) for MATCH/RETURN
- **Month 3:** OLTP record store behind Cypher
- **Month 4:** Sync bridge + full integration

**Likelihood:** 55%
**Stress:** Week 1-2 is the critical experiment — does PackStream work? If yes, users
connect from Day 14. If no, stalls.
**Inflection:** If `neo4j-driver` connects and runs CALL procedures (Week 4), we have
the "exact same API" story early.

---

### Timeline C: "Dual Track" (OLTP + Bolt in Parallel)

**Opening move:** Split work: Bolt server in one track, OLTP record store in another.

- **Week 1-2 Track 1:** Bolt server + PackStream
- **Week 1-2 Track 2:** Node/Relationship record stores (mmap'd)
- **Week 3-4:** Wire Bolt to record stores. Basic MATCH queries work.
- **Month 2:** Add OLAP engine (existing CSR + PageRank) + CALL procedures
- **Month 3:** Cypher parser, property stores, token stores
- **Month 4:** WAL + transactions + sync bridge

**Likelihood:** 45% (1-person team makes true parallelism hard)
**Stress:** Context switching between protocol work and storage work.
**Inflection:** If both tracks converge at Week 4 (Bolt serving OLTP reads), massive
momentum.

---

### Timeline D: "Vertical Slice + Expand" (Recommended)

**Opening move:** Bolt server + CALL procedures (OLAP) → OLTP record stores → Cypher.

This builds the most VISIBLE value first (drivers connect, algorithms run),
then adds the FOUNDATIONAL value (OLTP record store), then the COMPLEX value (Cypher).

```
PHASE 1: v0.0.3 — "Drivers Connect, Algorithms Run" (3 weeks)
  ├── Bolt v5 server (tokio + PackStream)           ~1,500 LOC
  ├── CALL procedure dispatcher                      ~300 LOC
  ├── PageRank on existing CSR                       ~150 LOC
  ├── Synthetic graph generator                      ~100 LOC
  └── Benchmark suite                                ~200 LOC
  TOTAL: ~2,250 LOC
  HEADLINE: "Connect with neo4j-driver. CALL knight_bus.pagerank.stream().
             10 sec, 720 MB. Neo4j GDS: 90 sec, 12 GB."

PHASE 2: v0.0.5 — "OLTP Record Store" (4-6 weeks)
  ├── oltp/node_store.rs (15B mmap'd records)        ~400 LOC
  ├── oltp/rel_store.rs (34B mmap'd records)         ~500 LOC
  ├── oltp/prop_store.rs (41B records)               ~600 LOC
  ├── oltp/token_store.rs (labels, rel types)        ~300 LOC
  ├── Cypher parser (ANTLR4 grammar port)            ~3,000 LOC
  ├── Basic MATCH execution engine                   ~1,000 LOC
  └── ID generator + freelist                        ~200 LOC
  TOTAL: ~6,000 LOC
  HEADLINE: "MATCH queries work. Same architecture as Neo4j. Faster runtime."

PHASE 3: v0.0.7 — "Write Path" (4-6 weeks)
  ├── WAL (append-only log + fsync)                  ~800 LOC
  ├── Transaction manager (begin/commit/rollback)    ~600 LOC
  ├── Lock manager (record-level)                    ~400 LOC
  ├── CREATE/SET/DELETE in Cypher                    ~1,000 LOC
  ├── Sync bridge (WAL → CSR rebuild)                ~500 LOC
  └── Schema constraints (uniqueness, existence)     ~400 LOC
  TOTAL: ~3,700 LOC
  HEADLINE: "Full read-write. CREATE, SET, DELETE work. Analytics auto-sync."

PHASE 4: v0.1.0 — "Production Ready" (6-8 weeks)
  ├── Cost-based query planner                       ~3,000 LOC
  ├── Index infrastructure (B+tree)                  ~1,500 LOC
  ├── More algorithms (Dijkstra, BFS, Louvain, etc.) ~2,000 LOC
  ├── Overlay model (zero-stale OLAP)                ~400 LOC
  ├── compio O_DIRECT for Level 2 OLAP-RAM           ~500 LOC
  ├── Configuration (neo4j.conf compatible)          ~600 LOC
  └── Import tool (neo4j-admin import compatible)    ~800 LOC
  TOTAL: ~8,800 LOC
  HEADLINE: "Drop-in Neo4j replacement. Same config. Same import. Faster everything."
```

**Likelihood:** 60%
**Why this order:**
1. **Phase 1** proves "exact same API" (Bolt) + "lowest RAM OLAP" in one release
2. **Phase 2** adds "identical architecture" (record stores) — now both selling points proven
3. **Phase 3** makes it a real database (writes) — no longer read-only
4. **Phase 4** makes it production-ready (planner, indexes, config)

---

### Cross-Timeline Analysis

| Path | Upside | Downside | Reversibility | Regret Risk |
|---|---|---|---|---|
| A: Bottom-Up | Strongest OLTP foundation | 2 months before users connect | HIGH | Medium — building in the dark |
| B: Top-Down | Users connect fastest | May discover OLTP is harder than expected | HIGH | Low — Bolt is reusable |
| **D: Vertical Slice** | **Best demo at every phase** | **Slightly scattered scope** | **HIGH** | **Lowest — each phase delivers value** |

### Decision Filter

**Strongest if everything goes normally:** Timeline D (Vertical Slice + Expand).
Each phase ships a demo. Phase 1 alone proves two of four L1 requirements.

**Safest if things go badly:** Timeline B (Top-Down). If OLTP record stores prove
harder than expected, we still have Bolt + OLAP. If Cypher parsing takes longer,
we still have CALL procedures.

**Experiment to reduce uncertainty:** 2-day Bolt spike (same as before).
If `neo4j-driver` connects → commit to Timeline D.

---

## LOC Estimate Summary

```
Current codebase:                          4,710 LOC
v0.0.3 (Bolt + OLAP procedures):         +2,250 LOC  →  ~6,960 LOC
v0.0.5 (OLTP record store + Cypher):      +6,000 LOC  → ~12,960 LOC
v0.0.7 (Write path + sync):              +3,700 LOC  → ~16,660 LOC
v0.1.0 (Planner + indexes + production):  +8,800 LOC  → ~25,460 LOC

Neo4j Community (for comparison):        2,090,000 LOC

Target: ~25K LOC of Rust replaces ~2M LOC of Java.
Compression ratio: ~80:1 (vs the earlier-debunked 5:1 to 8:1 claim)

Honest ratio explanation:
- We DON'T port Cypher's full TCK spec suite (122K LOC of tests)
- We DON'T port the import-util (32K LOC)
- We DON'T port Muninn page cache (24K LOC — replaced by mmap)
- We DON'T port monitoring, diagnostics, cloud (15K LOC)
- Cypher planner: 181K LOC in Scala/Java → ~3-5K LOC in Rust (simpler planner, fewer
  optimization strategies initially)
- Record store: 96K LOC → ~2K LOC (mmap replaces PageCursor + page management)

The real comparison: we replace ~300K LOC of CORE functionality with ~25K LOC of Rust.
That's a 12:1 ratio — plausible for Rust vs Java with mmap replacing custom infrastructure.
```

---

## What This Architecture Does NOT Include (And Why)

| Feature | Why Not | When |
|---|---|---|
| Multi-node clustering | Community edition is single-node. Neo4j GDS is single-server even in clusters. | v1.0+ |
| HTTP REST API | Bolt is the primary API. REST is legacy. | v0.1.0+ |
| Neo4j Browser serving | Use existing Neo4j Browser, connect via Bolt. | Never (external tool) |
| Lucene fulltext search | Not needed for graph algorithms. | v0.1.0+ |
| Spatial indexes (Point) | Not needed for core graph analytics. | v0.1.0+ |
| Cypher shell | Use Neo4j's existing cypher-shell if Bolt works. | Never (external tool) |
| GDS enterprise features | Community edition scope. | Never |

---

## Folder Structure (Proposed)

```
src/
├── main.rs                    # CLI + server startup
├── lib.rs                     # Library root
├── bolt/                      # L1: Wire protocol
│   ├── mod.rs
│   ├── server.rs              # TCP listener (tokio)
│   ├── packstream.rs          # PackStream codec
│   ├── messages.rs            # Bolt message types
│   └── session.rs             # Session state machine
├── cypher/                    # L2: Query language
│   ├── mod.rs
│   ├── parser.rs              # Cypher parser (ANTLR4 port)
│   ├── ast.rs                 # Abstract syntax tree
│   ├── planner.rs             # Query planner
│   └── executor.rs            # Execution engine
├── router/                    # L3: Query routing
│   ├── mod.rs
│   └── dispatcher.rs          # OLTP vs OLAP routing
├── oltp/                      # L4a: Record store engine
│   ├── mod.rs
│   ├── node_store.rs          # 15B node records
│   ├── rel_store.rs           # 34B relationship records
│   ├── prop_store.rs          # 41B property records
│   ├── rel_group_store.rs     # 25B dense node groups
│   ├── token_store.rs         # Labels, rel types, property keys
│   ├── schema_store.rs        # Schema rules
│   ├── meta_store.rs          # Database metadata
│   ├── id_gen.rs              # ID generation + freelist
│   ├── wal.rs                 # Write-ahead log
│   ├── tx.rs                  # Transaction manager
│   └── lock.rs                # Lock manager
├── olap/                      # L4b: CSR analytics engine
│   ├── mod.rs
│   ├── runtime.rs             # MmapWalkRuntime (existing)
│   ├── page_rank.rs           # PageRank (rayon)
│   ├── dijkstra.rs            # Dijkstra shortest path
│   ├── bfs.rs                 # Breadth-first search
│   ├── community.rs           # Louvain community detection
│   ├── synthetic.rs           # Random graph generator
│   └── layouts/               # Algorithm-specific CSR variants
│       ├── inbound_power.rs   # Family #2
│       └── ...
├── sync/                      # L5: OLTP ↔ OLAP bridge
│   ├── mod.rs
│   ├── wal_consumer.rs        # Read WAL → update CSR
│   └── snapshot.rs            # Snapshot versioning
├── build/                     # Snapshot builder (existing)
│   ├── mod.rs
│   └── low_ram.rs             # External merge-sort
├── types.rs                   # Shared types
├── error.rs                   # Error types
└── config.rs                  # Configuration (neo4j.conf compat)
```

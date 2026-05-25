# What Neo4j Actually Has — Component Breakdown

*Answering: Is there a database? A query language? A runtime?
What are the parts? Which move to Rust/KNRT?*

---

## Neo4j Is Six Things

When someone says "Neo4j," they're actually using six distinct products
bundled into one Java process:

```
┌─────────────────────────────────────────────────┐
│ 1. DATABASE (storage engine)                    │
│    — Where your 50GB of data lives              │
│    — 69,646 LOC (record-storage-engine)         │
│    — 83,297 LOC (kernel — transactions, ACID)   │
│    — 14,241 LOC (io — page cache "Muninn")      │
│    — 13,402 LOC (index — GBPTree)               │
│    — 8,888 LOC (WAL — write-ahead log)          │
│    — 5,522 LOC (lock — concurrency)             │
│    Total: ~195K LOC                             │
├─────────────────────────────────────────────────┤
│ 2. QUERY LANGUAGE (Cypher)                      │
│    — The language you write queries in           │
│    — 701,841 LOC (cypher module — 512K Scala)   │
│    Total: ~702K LOC (44% of entire codebase!)   │
├─────────────────────────────────────────────────┤
│ 3. RUNTIME (query execution engine)             │
│    — Lives INSIDE the cypher module              │
│    — Parser → Planner → Runtime pipeline         │
│    — Interpreted + Slotted pipe execution         │
│    — Already counted in the 702K above           │
├─────────────────────────────────────────────────┤
│ 4. WIRE PROTOCOL (Bolt)                         │
│    — How drivers talk to Neo4j over TCP          │
│    — 42,064 LOC (bolt module)                   │
│    — Binary protocol, session state machine      │
├─────────────────────────────────────────────────┤
│ 5. SERVER (HTTP + management)                   │
│    — HTTP API, admin, monitoring, browser UI     │
│    — 19,960 LOC (server)                        │
│    — 10,560 LOC (dbms)                          │
│    — 3,620 LOC (security)                       │
│    Total: ~34K LOC                              │
├─────────────────────────────────────────────────┤
│ 6. TOOLING (import, shell, extensions)          │
│    — CLI tools, import pipeline, procedures     │
│    — 18,777 LOC (cypher-shell)                  │
│    — 26,212 LOC (import-util + import-tool)     │
│    — 19,909 LOC (procedures)                    │
│    — 12,896 LOC (lucene-index — full-text)      │
│    Total: ~78K LOC                              │
└─────────────────────────────────────────────────┘
```

---

## Let's Go Through Each One

### 1. THE DATABASE — Yes, There Is an Actual Database

Neo4j has a real, physical database — files on disk that store your
graph data. It's not an in-memory toy.

**What it stores:**

| What | How | File on disk |
|---|---|---|
| Nodes | 15-byte fixed records | `neostore.nodestore.db` |
| Relationships | 34-byte fixed records | `neostore.relationshipstore.db` |
| Properties | 41-byte fixed records | `neostore.propertystore.db` |
| Property keys | Strings → tokens | `neostore.propertystore.db.index.keys` |
| Labels | Label sets per node | `neostore.labelscanstore.db` |
| Relationship groups | Type-partitioned rels for dense nodes | `neostore.relationshipgroupstore.db` |
| Indexes | B+tree (GBPTree) per schema index | `schema/index/...` |
| Transaction log | Append-only WAL | `neostore.transaction.db.*` |

**How a 50GB database is structured:**

A 50GB Neo4j `data/databases/neo4j/` folder contains:
- Node store: ~2-5GB (depending on node count)
- Relationship store: ~10-20GB (relationships are 34 bytes each)
- Property store: ~10-25GB (properties are the bulk)
- Indexes: ~2-10GB
- Transaction logs: ~1-5GB
- Everything else: <1GB

The storage is a **linked-list architecture**:
```
Node record → points to → first Relationship
Relationship → points to → next Relationship (linked list)
Relationship → points to → first Property
Property → points to → next Property (linked list)
```

This means reading one node's relationships = chasing pointers through
scattered records. This is the fundamental performance bottleneck.

**What KNRT does with this:**

Two options:

**Option A — Keep Neo4j's record layout, write it in Rust**
- Same files, same 15-byte nodes, same 34-byte rels
- Just faster I/O (mmap vs Muninn), no GC, faster iteration
- Gain: 2-3x (honest — same architecture, different language)
- Advantage: could theoretically read existing Neo4j data files
- Disadvantage: inherits the linked-list bottleneck

**Option B — Knight Bus CSR layout**
- Convert to dual-CSR (forward + reverse contiguous arrays)
- One-time migration: read Neo4j records → write CSR snapshot
- Gain: 100x for traversal queries (contiguous memory, no pointer chasing)
- Disadvantage: requires data migration, immutable (no in-place writes)

**Practical answer: do both.** Use a mutable record store for writes
(Option A) and periodically build CSR snapshots for reads (Option B).
Writes are 2-3x faster. Reads are 100x faster.

---

### 2. THE QUERY LANGUAGE — Yes, There Is Cypher

Cypher is Neo4j's query language. It is 701,841 lines of code — 44%
of the entire Neo4j codebase. Most of it is Scala (512K LOC).

**What Cypher does:**

```cypher
-- This is a Cypher query:
MATCH (person:Person)-[:KNOWS]->(friend:Person)
WHERE person.name = 'Alice' AND friend.age > 30
RETURN friend.name, friend.age
ORDER BY friend.age DESC
LIMIT 10
```

**The parts of Cypher:**

| Part | What it does | LOC (approx) | Difficulty |
|---|---|---|---|
| **Parser** | Text → AST (abstract syntax tree) | ~40K Scala + ANTLR | Medium |
| **Semantic analyzer** | Type checking, scope resolution | ~30K Scala | Medium |
| **AST rewriter** | Normalize, desugar, deprecation handling | ~20K Scala | Medium |
| **Planner** | AST → logical plan → physical plan | ~250K Scala | **Very Hard** |
| **Runtime** | Execute physical plan against storage | ~200K Scala/Java | Hard |
| **Supporting** | Types, values, utilities, tests | ~160K | Medium |

The planner is the monster. It uses IDP (Iterative Dynamic Programming)
for join ordering, cost-based optimization with cardinality estimation,
and eager barrier analysis. This is the hardest piece of Neo4j to
replicate.

**What KNRT does with this:**

You don't rewrite 702K LOC. You build a NEW Cypher implementation
from the **openCypher specification** (which is Apache-2.0 licensed).

- **Parser**: Use existing Rust crate `decypher` or `ocg` — or write
  a recursive descent parser. ~5-10K LOC Rust. The openCypher grammar
  is well-documented.
- **Planner**: Start with a **rule-based planner** (not cost-based IDP).
  Handles 80% of real queries correctly and fast. ~15-25K LOC.
  The full IDP planner is a v2 feature.
- **Runtime**: Build a Volcano-model operator pipeline (Scan, Expand,
  Filter, Project, Sort, Limit, Aggregate). Textbook DB engineering.
  ~25-40K LOC.

**Total: ~50-75K LOC Rust** to support 80% of Cypher.

---

### 3. THE RUNTIME — Yes, It's Inside Cypher

The "runtime" is not a separate module — it's part of the cypher module.
When Neo4j executes a query, this is what happens:

```
Query text: "MATCH (n:Person)-[:KNOWS]->(m) WHERE n.age > 30 RETURN m.name"
    │
    ▼
PARSER ──→ AST (abstract syntax tree)
    │
    ▼
PLANNER ──→ Logical Plan:
    │         AllNodesScan(n)
    │           → Filter(n:Person AND n.age > 30)
    │             → Expand(n, KNOWS, m)
    │               → Projection(m.name)
    │
    ▼
RUNTIME ──→ Physical execution:
    │         SlottedPipe[AllNodesScan]
    │           .pull() → SlottedPipe[Filter]
    │             .pull() → SlottedPipe[Expand]
    │               .pull() → SlottedPipe[Projection]
    │                 .pull() → Row{m.name: "Bob"}
    │
    ▼
RESULT ──→ Bolt serialization → network → driver → application
```

Neo4j has TWO runtimes:
- **Interpreted runtime** — each operator is a function call. Slower.
- **Slotted runtime** — operators work over fixed-size slot arrays.
  Faster, less allocation.

Enterprise also has a **Pipelined runtime** (parallel, vectorized)
and a **Parallel runtime** for read-only queries.

**What KNRT does with this:**

Build ONE runtime — slotted/vectorized from the start:
- Operators implement a `trait Operator { fn next(&mut self) -> Option<Row>; }`
- Rows are stack-allocated fixed-size slot arrays
- No Java object allocation per row
- Monomorphize hot operators (Rust generics eliminate virtual dispatch)

For queries that hit the CSR engine (traversal-heavy), bypass the
operator pipeline entirely — go straight to array slice:
```
MATCH (n {id: $id})-[:DEPENDS_ON]->(m) RETURN m.id
  → detect: single-hop forward expansion
  → shortcut: CSR offset lookup → peer array slice → done
  → skip the entire operator pipeline
```

This is where the 100x comes from. The operator pipeline handles
general queries. The CSR shortcut handles the hot traversal patterns.

---

### 4. THE WIRE PROTOCOL — Bolt

Bolt is the binary protocol that Neo4j drivers use to communicate.
Every language driver (Python, Java, JavaScript, .NET, Go) speaks Bolt.

**What Bolt does:**

```
Client                          Server
  │                               │
  │──── HELLO {agent, auth} ────→│  Authentication
  │←── SUCCESS {server_info} ────│
  │                               │
  │──── RUN "MATCH..." {$p} ───→│  Send query
  │←── SUCCESS {fields} ────────│
  │                               │
  │──── PULL {n: 1000} ────────→│  Fetch results
  │←── RECORD [row1] ──────────│
  │←── RECORD [row2] ──────────│
  │←── SUCCESS {has_more} ─────│
  │                               │
  │──── BEGIN ─────────────────→│  Transactions
  │←── SUCCESS ────────────────│
  │──── RUN ... ───────────────→│
  │──── COMMIT ────────────────→│
  │←── SUCCESS ────────────────│
```

**Bolt uses PackStream** — a binary serialization format similar to
MessagePack. It encodes:
- Null, Boolean, Integer, Float, String, Bytes
- List, Map
- Struct (for Node, Relationship, Path, DateTime, etc.)

**42,064 LOC of Java.** But the protocol itself is simple — MeshDB
already implements Bolt 5 in Rust (MIT licensed).

**What KNRT does with this:**

Build a Bolt server. This is the KEY adoption piece — if Bolt works,
every existing Neo4j driver works. The user changes one connection
string and their application talks to KNRT.

```rust
// Rough architecture
async fn handle_connection(stream: TcpStream) {
    let mut session = BoltSession::new();
    loop {
        let message = read_bolt_message(&mut stream).await;
        match message {
            BoltMessage::Hello { .. } => session.authenticate(...),
            BoltMessage::Run { query, params } => {
                let plan = planner.plan(&query, &params);
                let result = runtime.execute(plan);
                session.set_result(result);
                send_success(&mut stream, result.fields()).await;
            }
            BoltMessage::Pull { n } => {
                for row in session.pull(n) {
                    send_record(&mut stream, &row).await;
                }
                send_success(&mut stream, summary).await;
            }
            // ... BEGIN, COMMIT, ROLLBACK, RESET, GOODBYE
        }
    }
}
```

**~10-15K LOC Rust.** Well-specified, bounded scope.

---

### 5. THE SERVER — HTTP + Management

Beyond Bolt, Neo4j has:

- **HTTP API** — REST endpoints for management, monitoring, ad-hoc queries
- **Browser** — the Neo4j Browser web UI (talks to HTTP API)
- **DBMS** — database management (create/drop databases, start/stop)
- **Security** — authentication, authorization, user management
- **Monitoring** — metrics, logging, JMX

**~34K LOC total** across server, dbms, security, monitoring.

**What KNRT does with this:**

Not needed for MVP. For v1:
- Basic HTTP admin endpoint (`/status`, `/metrics`) — ~1K LOC with `axum`
- Auth from config file (username/password) — ~500 LOC
- Logging via `tracing` — ~500 LOC

The Neo4j Browser is a separate open-source project. If KNRT speaks
Bolt correctly, the Browser just works against KNRT too.

---

### 6. THE TOOLING — Import, Shell, Extensions

| Tool | What it does | LOC | KNRT equivalent |
|---|---|---|---|
| `cypher-shell` | CLI for running Cypher | 18,777 | `clap` CLI, ~3K LOC |
| `import-tool` | Bulk CSV import | 4,231 | Already have CSV import in Knight Bus |
| `import-util` | Import infrastructure | 21,981 | Extend Knight Bus build pipeline |
| `procedures` | User-defined procedures | 19,909 | Defer to v2 |
| `lucene-index` | Full-text search | 12,896 | Use `tantivy` crate when needed |
| `spatial-index` | Geospatial queries | 2,266 | Defer |
| `graph-algo` | BFS, Dijkstra, etc. | 4,321 | Use `petgraph` crate when needed |

---

## Which Parts Move to Rust/KNRT?

Here's the mapping — every Neo4j piece and what happens to it:

### MOVE (rewrite in Rust, clean-room)

| Neo4j component | LOC | KNRT approach | Priority |
|---|---|---|---|
| Record storage engine | 69,646 | Mutable record store + CSR snapshots | **P0** |
| Kernel (transactions) | 83,297 | Lightweight transaction manager | P1 |
| Page cache (io) | 14,241 | mmap + optional buffer pool | **P0** |
| GBPTree (index) | 13,402 | B+tree or use `redb`/`sled` | P1 |
| WAL | 8,888 | Append-only log with `fsync` | P1 |
| Lock manager | 5,522 | `parking_lot` + deadlock detection | P1 |
| Cypher parser | ~40K (of 702K) | From openCypher spec, use `decypher` crate | **P0** |
| Cypher planner | ~250K (of 702K) | Rule-based planner (~15-25K LOC) for v1 | **P0** |
| Cypher runtime | ~200K (of 702K) | Volcano operators + CSR shortcut | **P0** |
| Bolt protocol | 42,064 | From Bolt spec, study MeshDB | **P0** |
| Values/types | 24,076 | Rust enum-based CypherValue | **P0** |

### KEEP (reuse from Knight Bus)

| Knight Bus component | LOC | What it gives KNRT |
|---|---|---|
| Dual-CSR builder | ~1,700 | CSR snapshot creation from graph data |
| MmapWalkRuntime | ~400 | Memory-mapped read engine for CSR |
| Snapshot format | ~220 | Binary snapshot file format |
| Verification | ~440 | Correctness checking infrastructure |
| Benchmark suite | ~460 | Performance regression tracking |
| Low-RAM build | ~1,700 | External merge sort for large graphs |

### SKIP (not needed)

| Neo4j component | LOC | Why skip |
|---|---|---|
| community-it (Java tests) | 207,135 | Write Rust tests instead |
| kernel-test (Java tests) | 65,744 | Write Rust tests instead |
| testing utils | 20,277 | Write Rust tests instead |
| gbptree-tests | 17,603 | Write Rust tests instead |
| fabric (federated) | 14,958 | Enterprise-grade, defer |
| codegen | 13,279 | Rust monomorphization replaces this |
| cloud/push-to-cloud | 8,101 | Cloud features, defer |
| genai-plugin | 4,230 | AI plugin, defer |
| neo4j-slf4j-provider | 1,122 | Java logging bridge, N/A |
| bootcheck | 107 | JVM check, N/A |
| native (JNI) | 481 | N/A in Rust |

### REPLACE WITH CRATE (Rust ecosystem)

| Neo4j component | Rust crate |
|---|---|
| Lucene full-text | `tantivy` |
| Graph algorithms | `petgraph` |
| Logging framework | `tracing` |
| Config parsing | `serde` + `toml` |
| TLS/SSL | `rustls` |
| Password hashing | `argon2` |
| CLI | `clap` |
| CSV | `csv` |
| HTTP server | `axum` |
| Async I/O | `tokio` |
| Compression | `zstd` |

---

## The 50GB Neo4j User — Shreyas Doshi Onboarding

### What does their 50GB look like?

Typical 50GB production Neo4j:

```
Nodes:       50-500 million
Rels:        200M-2B
Rel types:   10-100 different types
Properties:  5-20 per node, 2-5 per relationship
Labels:      10-50 label types, 2-5 per node
Indexes:     10-50 schema indexes
Queries:     50-500 distinct Cypher patterns in application code
Drivers:     3-20 services connecting via Bolt (Python, Java, JS)
Instance:    32-128GB RAM (Neo4j wants 2-3x data size in heap + page cache)
Cost:        $36K+/year license (Enterprise) or self-managed Community
```

### The Shreyas question: "What's the switching cost?"

**For the user to try KNRT, they need to:**

1. Export their Neo4j data
2. Import into KNRT
3. Start KNRT
4. Change connection string
5. See if their queries work

**Steps 1-4 should take under 30 minutes for a 50GB database.**

Here's what that looks like:

```bash
# Step 1: Export from Neo4j (user already knows this)
# Option A: neo4j-admin dump
neo4j-admin database dump neo4j --to-path=/tmp/neo4j-export

# Option B: APOC CSV export
echo "CALL apoc.export.csv.all('export', {})" | cypher-shell

# Step 2: Import into KNRT
knrt import \
  --from neo4j-dump \        # or --from neo4j-csv
  --source /tmp/neo4j-export \
  --output /var/knrt/mydb \
  --memory-budget 8GB        # Knight Bus low-RAM mode

# For 50GB: ~15-30 minutes (depends on disk I/O)
# Knight Bus's external merge sort handles this without needing
# 50GB of RAM — 8GB budget is enough.

# Step 3: Start KNRT
knrt serve \
  --data /var/knrt/mydb \
  --bolt-port 7687 \
  --http-port 7474           # optional admin

# Step 4: Change connection string in application
# Before: bolt://neo4j-server:7687
# After:  bolt://knrt-server:7687
# (or run KNRT on the same host, different port)
```

### Step 5: "Do my queries work?"

This is the make-or-break moment. The user runs their app, and:

**Queries that WILL work (v1):**
```cypher
-- Simple node lookups
MATCH (n:Person {name: 'Alice'}) RETURN n

-- Traversals (any depth, any relationship type)
MATCH (n:Person)-[:KNOWS]->(friend) RETURN friend.name

-- Multi-hop
MATCH (n)-[:DEPENDS_ON*1..3]->(m) RETURN m

-- Filters
MATCH (n:Person) WHERE n.age > 30 AND n.city = 'NYC' RETURN n

-- Aggregation
MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name, count(m)

-- Sorting + pagination
MATCH (n:Person) RETURN n ORDER BY n.age DESC LIMIT 20

-- WITH chaining
MATCH (n:Person)-[:KNOWS]->(m)
WITH n, count(m) AS friendCount
WHERE friendCount > 5
RETURN n.name, friendCount

-- UNWIND
UNWIND $ids AS id MATCH (n {id: id}) RETURN n

-- Create / update / delete
CREATE (n:Person {name: 'Bob', age: 25})
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)
MATCH (n:Person {name: 'Bob'}) SET n.age = 26
MATCH (n:Person {name: 'Bob'}) DELETE n
```

**Queries that WON'T work (v1):**
```cypher
-- APOC procedures
CALL apoc.path.expandConfig(...)
CALL apoc.periodic.iterate(...)

-- Pattern comprehensions
RETURN [(n)-[:KNOWS]->(m) | m.name]

-- Existential subqueries
MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->() }

-- CALL within query
CALL db.labels() YIELD label

-- Full-text search
CALL db.index.fulltext.queryNodes('index', 'search term')

-- Shortest path (v1 — use petgraph later)
MATCH p = shortestPath((a)-[*]->(b)) RETURN p
```

### The compatibility report KNRT shows them:

```
╔═══════════════════════════════════════════════╗
║  KNRT Compatibility Report                    ║
╠═══════════════════════════════════════════════╣
║                                               ║
║  Queries analyzed:     127                    ║
║  Fully compatible:     108 (85%)              ║
║  Partially compatible:   9 (7%)               ║
║  Not supported:         10 (8%)               ║
║                                               ║
║  Not supported breakdown:                     ║
║  ├── APOC procedures:         6               ║
║  ├── Pattern comprehension:   2               ║
║  ├── Existential subquery:    1               ║
║  └── shortestPath:            1               ║
║                                               ║
║  Performance comparison (sampled 50 queries): ║
║  ├── Average latency:  2.8x faster            ║
║  ├── p99 latency:     18x faster              ║
║  ├── Memory:           4x less                ║
║  └── Traversal queries: 95x faster (CSR mode) ║
║                                               ║
╚═══════════════════════════════════════════════╝
```

### The Shreyas pitch for the 50GB user:

> **You have a 50GB Neo4j database.**
>
> Right now it runs on a 64GB RAM instance ($500/month) with GC pauses
> spiking your p99 to 200ms.
>
> With KNRT:
> - Same 50GB of data runs on a 16GB instance ($125/month)
> - p99 drops from 200ms to 10ms
> - 85% of your queries work without changes
> - Your traversal-heavy queries get 100x faster via CSR mode
> - Import takes 30 minutes. Connection string change takes 30 seconds.
>
> Try it as a read replica first. Zero risk. If it works, switch over.
> If not, you've lost 30 minutes.

That's a Shreyas pitch. Low switching cost. Measurable value.
Bounded risk. The first 5 minutes are free.

---

## Exact Folder Mapping — Where Each Part Lives

All paths relative to `neo4j-reference/neo4j/community/`.

### Part 1: DATABASE (storage engine) — 195K LOC

```
community/
├── record-storage-engine/    69,646 LOC  ← THE data store
│   └── RecordStorageEngine, NeoStores, NodeStore,
│       RelStore, PropStore, batch import, record formats
│
├── kernel/                   83,297 LOC  ← transactions, cursors, ACID
│   └── KernelTransaction, NodeCursor, RelCursor,
│       Operations, recovery, database lifecycle
│
├── kernel-api/               18,542 LOC  ← public SPI (traits/interfaces)
│   └── StorageEngine, StorageReader, CommandCreationContext
│
├── io/                       14,241 LOC  ← page cache "Muninn"
│   └── MuninnPageCache, PageCursor, file I/O,
│       page eviction (clock sweep), faulting, flushing
│
├── index/                    13,402 LOC  ← GBPTree (B+tree)
│   └── schema indexes, counts store index,
│       ID tracking, crash-safe, checkpoint integration
│
├── wal/                       8,888 LOC  ← write-ahead log
│   └── log entries, checkpointing, recovery,
│       log rotation, durability
│
├── lock/                      5,522 LOC  ← lock manager
│   └── read/write locks, deadlock detection,
│       transaction concurrency
│
├── storage-engine-util/       7,898 LOC  ← counts store, shared utils
│   └── GBPTreeGenericCountsStore, degree cache
│
├── id-generator/             10,939 LOC  ← ID recycling
│   └── recycles freed entity IDs via GBPTree or scan
│
├── layout/                      723 LOC  ← store file layout
├── concurrent/                1,813 LOC  ← concurrent data structures
├── unsafe/                    1,443 LOC  ← low-level memory access
└── consistency-check/         1,331 LOC  ← store integrity validation
```

### Part 2: QUERY LANGUAGE (Cypher) — 702K LOC

```
community/cypher/             701,841 LOC total (512K Scala + 155K Java)
│
├── front-end/               322,897 LOC  ← THE BIGGEST subfolder
│   └── Parser (ANTLR grammar → AST)
│       Semantic analysis (type checking, scope)
│       AST rewriting (normalization, desugaring)
│       Name resolution, pattern expression handling
│
├── cypher-planner/          181,802 LOC  ← THE HARDEST piece
│   └── IDP solver (join ordering)
│       Cost-based optimization
│       Cardinality estimation (histograms, selectivity)
│       Eager barrier analysis
│       Plan caching, parameter sensitivity
│
├── runtime-spec-suite/      122,221 LOC  ← test specs for runtime
│   └── Cucumber/Gherkin test scenarios
│       Defines expected behavior for all operators
│
├── interpreted-runtime/      59,267 LOC  ← runtime option 1
│   └── Interpreted pipe execution
│       Each operator = function call
│
├── cypher/                   45,591 LOC  ← assembly/glue
│   └── Wires parser → planner → runtime together
│       CypherQueryEngine entry point
│
├── runtime-util/             35,381 LOC  ← shared runtime utilities
│   └── Row handling, argument processing
│       Shared between interpreted + slotted runtimes
│
├── slotted-runtime/          19,674 LOC  ← runtime option 2 (faster)
│   └── Fixed-size slot arrays instead of Row objects
│       Less allocation, better cache behavior
│
├── cypher-logical-plans/     14,605 LOC  ← plan tree types
│   └── LogicalPlan node types (AllNodesScan, Expand,
│       Filter, Projection, Sort, Limit, etc.)
│
├── physical-planning/        10,735 LOC  ← logical → physical plan
│   └── Slot allocation, pipe mapping
│
├── ir/                        9,740 LOC  ← intermediate representation
│   └── Between AST and logical plan
│
├── logical-plan-builder/      7,847 LOC  ← plan construction
│
├── expression-evaluator/      1,369 LOC  ← expression eval
├── cypher-config/             1,991 LOC  ← Cypher config
├── cypher-cache/              1,597 LOC  ← query plan cache
├── cypher-testing/            1,855 LOC  ← test helpers
├── planner-spi/               1,412 LOC  ← planner interfaces
├── graph-counts/                809 LOC  ← graph statistics
├── logical-plan-generator/    1,715 LOC  ← plan generation
├── spec-suite-tools/          1,546 LOC  ← test tooling
├── compatibility-spec-suite/    487 LOC  ← compat tests
└── cypher-rendering/             81 LOC  ← plan rendering
```

### Part 3: RUNTIME (inside Cypher)

The runtime is NOT a separate folder — it's distributed across:
```
community/cypher/interpreted-runtime/   59,267 LOC  ← runtime v1
community/cypher/slotted-runtime/       19,674 LOC  ← runtime v2 (faster)
community/cypher/runtime-util/          35,381 LOC  ← shared runtime code
community/cypher/runtime-spec-suite/   122,221 LOC  ← runtime tests/specs
community/cypher/physical-planning/     10,735 LOC  ← physical plan gen
community/cypher/expression-evaluator/   1,369 LOC  ← expr eval
                                       ────────
                                      ~249K LOC total runtime
```

### Part 4: WIRE PROTOCOL (Bolt) — 42K LOC

```
community/bolt/               42,064 LOC (all Java)
│
└── src/main/java/org/neo4j/bolt/
    ├── protocol/       ← Bolt protocol versions (v3, v4, v5)
    │   ├── common/     ← shared message types
    │   └── v*/         ← version-specific handling
    ├── transport/       ← TCP transport, connection handling
    ├── runtime/         ← session state machine
    │   └── statemachine/  HELLO → READY → STREAMING → etc.
    ├── packstream/      ← PackStream binary serialization
    │   └── encode/decode for Neo4j types
    └── security/        ← auth over Bolt
```

### Part 5: SERVER (HTTP + management) — 34K LOC

```
community/server/             19,960 LOC  ← HTTP API
│   └── REST endpoints, Neo4j Browser serving
│       Admin endpoints, query submission via HTTP
│
community/server-api/          1,377 LOC  ← server interfaces
│
community/dbms/               10,560 LOC  ← database management
│   └── DatabaseManagementService
│       Create/drop/start/stop databases
│       System database management
│
community/security/            3,620 LOC  ← auth & authz
│   └── Authentication (password), authorization (roles)
│       User management, basic RBAC
│
community/configuration/      12,295 LOC  ← config system
│   └── Setting definitions, validation, parsing
│       (Neo4j has ~400 config settings)
│
community/logging/             4,080 LOC  ← logging framework
community/monitoring/            570 LOC  ← metrics
community/ssl/                 1,851 LOC  ← TLS
```

### Part 6: TOOLING — 78K LOC

```
community/cypher-shell/       18,777 LOC  ← CLI for running Cypher
│
community/import-tool/         4,231 LOC  ← bulk import CLI
community/import-util/        21,981 LOC  ← import infrastructure
community/import-api/            987 LOC  ← import interfaces
community/csv/                 5,109 LOC  ← CSV parsing
│
community/procedure/          15,327 LOC  ← built-in procedures
community/procedure-api/         273 LOC  ← procedure interfaces
community/procedure-compiler/  4,309 LOC  ← procedure annotation proc
│
community/lucene-index/       12,896 LOC  ← full-text search (Lucene)
community/fulltext-index/      3,029 LOC  ← full-text integration
community/spatial-index/       2,266 LOC  ← spatial indexing
community/graph-algo/          4,321 LOC  ← graph algorithms
│
community/genai-plugin/        4,230 LOC  ← AI/ML plugin
community/cloud/               4,115 LOC  ← cloud features
community/push-to-cloud/       3,986 LOC  ← cloud push
community/fabric/             14,958 LOC  ← federated queries
```

### Supporting (shared infrastructure)

```
community/common/             12,324 LOC  ← shared utilities
community/collections/        11,356 LOC  ← custom collections
community/values/             24,076 LOC  ← type system (CypherValue)
community/graphdb-api/         4,600 LOC  ← public Graph API
community/token-api/           1,439 LOC  ← label/property tokens
community/neo4j-exceptions/    2,809 LOC  ← exception types
community/neo4j-gql-status/    8,191 LOC  ← GQL status codes
community/neo4j-notifications/ 3,508 LOC  ← notification system
community/capabilities/        1,133 LOC  ← feature flags
community/command-line/          881 LOC  ← CLI framework
community/codegen/            13,279 LOC  ← runtime code generation
community/data-collector/      2,043 LOC  ← telemetry
community/neo4j/              11,443 LOC  ← assembly/bootstrap
```

### Tests (SKIP — rewrite in Rust)

```
community/community-it/      207,135 LOC  ← integration tests
community/kernel-test/         58,323 LOC  ← kernel unit tests
community/kernel-test-utils/    7,421 LOC  ← test utilities
community/testing/             20,277 LOC  ← test framework
community/gbptree-tests/       17,603 LOC  ← B+tree tests
community/server-test-utils/    1,982 LOC  ← server test utils
community/neo4j-harness/        2,131 LOC  ← test harness
```

---

## Summary: Neo4j's 6 Parts → KNRT

| Neo4j Part | LOC | Move to KNRT? | How |
|---|---|---|---|
| **1. Database** (storage) | 195K | YES | Clean-room Rust storage + Knight Bus CSR |
| **2. Query Language** (Cypher) | 702K | YES (80%) | New parser from openCypher spec |
| **3. Runtime** (execution) | (in #2) | YES | Volcano operators + CSR shortcut |
| **4. Wire Protocol** (Bolt) | 42K | YES | From Bolt spec |
| **5. Server** (HTTP/admin) | 34K | MINIMAL | Basic admin only for v1 |
| **6. Tooling** (import etc.) | 78K | PARTIAL | Import tool + CLI |
| **Tests** | 310K | SKIP | Write new Rust tests |
| **Total** | 1.58M | ~100-165K LOC Rust | |

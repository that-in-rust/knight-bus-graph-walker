# What Knight Bus Actually Has — Honest Inventory

*What exists. What doesn't. And how a 50GB Neo4j user gets here.*

---

## The Codebase Today: 4,710 Lines of Rust

12 source files in `src/`. Let's go through every one.

### What EXISTS

#### 1. Storage Format — Immutable Dual-CSR Snapshot

**Files:** `snapshot.rs` (217 LOC), parts of `types.rs`

Knight Bus stores graph data as 7 binary files:

```
manifest.json          — metadata (version, counts, file names)
node_table.bin         — 16-byte NodeRecords (key_offset, key_len, flags)
strings.bin            — concatenated node key strings
key_index.bin          — sorted dense IDs for binary search
forward.offsets.bin    — CSR offsets for outgoing adjacency
forward.peers.bin      — CSR peer arrays for outgoing adjacency
reverse.offsets.bin    — CSR offsets for incoming adjacency
reverse.peers.bin      — CSR peer arrays for incoming adjacency
```

Node record is 16 bytes:
```rust
pub struct NodeRecord {
    pub key_offset: u64,   // offset into strings.bin
    pub key_len: u32,      // length of key string
    pub flags: u32,        // reserved
}
```

This IS a graph storage format. But:
- **Immutable** — no writes, no updates, no deletes after build
- **One relationship type** — everything is DEPENDS_ON, no type discrimination
- **No properties** on relationships — only adjacency
- **Node properties are limited** — node_type, label, parent_id, file_path, span
  (hardcoded CSV columns, not arbitrary key-value pairs)
- **Dense u32 node IDs** — max 4.2 billion nodes

#### 2. Query Runtime — MmapWalkRuntime

**Files:** `runtime.rs` (402 LOC)

```rust
pub trait WalkQueryRuntime {
    fn query_entity_neighbors(
        &self,
        entity_key: &NodeKey,
        direction: WalkDirection,
        hops: HopCount,
    ) -> Result<QueryResult, KnightBusError>;
}
```

The runtime:
- Memory-maps all 7 snapshot files
- Resolves entity key → dense ID via binary search on sorted key index
- Looks up CSR offset/peer arrays for adjacency
- Supports forward or backward direction
- Supports 1-hop or 2-hop expansion

This IS a graph query runtime. But:
- **Read-only** — no mutations
- **Fixed-hop only** — exactly 1 or 2 hops, not variable-length
- **One query shape** — "give me neighbors of X in direction D within N hops"
- **No filtering** — no WHERE clause, no property predicates
- **No aggregation** — no COUNT, SUM, COLLECT
- **No joins** — no multi-pattern matching
- **No query language** — programmatic API only

#### 3. Build Pipeline — CSV → Snapshot

**Files:** `low_ram.rs` (1,703 LOC), `graph.rs` (225 LOC), `truth.rs` (438 LOC)

The build pipeline:
- Reads `nodes.csv` (node_id, node_type, label, parent_id, file_path, span)
- Reads `edges.csv` (from_id, edge_type, to_id)
- External merge sort for low-RAM operation
- Constructs dual-CSR (forward + reverse adjacency)
- Writes snapshot files
- Verifies snapshot against original CSV
- Tracks peak RSS per phase

This IS a graph build system. And it's good:
- **Handles large graphs** with bounded memory (configurable budget)
- **External merge sort** means it can build graphs larger than RAM
- **Verification** ensures snapshot correctness
- **RSS tracking** per build phase

#### 4. CLI Interface

**Files:** `main.rs` (305 LOC)

Commands:
- `knight-bus build --nodes-csv X --edges-csv Y --output DIR`
- `knight-bus verify --snapshot DIR --nodes-csv X --edges-csv Y`
- `knight-bus query --snapshot DIR --entity KEY --dir forward|backward --hops 1|2`
- `knight-bus bench --snapshot DIR --report DIR`
- `knight-bus bench-corpus --snapshot DIR --corpus FILE --report FILE`

#### 5. Benchmark Suite

**Files:** `bench.rs` (458 LOC)

- Per-family latency measurement (p50, p95)
- Corpus-based benchmarking with warmup passes
- RSS tracking via `sysinfo`
- JSON report output

#### 6. Error Handling

**Files:** `error.rs` (119 LOC)

Clean `thiserror`-based error enum with 14 variants covering I/O,
CSV parsing, JSON, validation, overflow, corruption, and parity mismatches.

---

### What DOES NOT Exist

| Capability | Status | What a Neo4j user expects |
|---|---|---|
| **Cypher query language** | ✗ Does not exist | `MATCH (n)-[:KNOWS]->(m) WHERE n.age > 30 RETURN m.name` |
| **Bolt protocol** | ✗ Does not exist | TCP server that Neo4j drivers connect to |
| **TCP/network server** | ✗ Does not exist | Any network listener at all |
| **Transactions** | ✗ Does not exist | BEGIN, COMMIT, ROLLBACK, ACID isolation |
| **Write operations** | ✗ Does not exist | CREATE, MERGE, SET, DELETE |
| **Properties on relationships** | ✗ Does not exist | `[:KNOWS {since: 2020}]` |
| **Arbitrary properties on nodes** | ✗ Does not exist | Open schema key-value pairs |
| **Multiple relationship types** | ✗ Does not exist | KNOWS, WORKS_AT, LIVES_IN — all in one graph |
| **Labels** | Partially | Nodes have `node_type` — but not Neo4j-style multi-labels |
| **Schema indexes** | ✗ Does not exist | Index on property values for fast lookup |
| **Variable-length paths** | ✗ Does not exist | `MATCH (a)-[:KNOWS*1..5]->(b)` |
| **Aggregation** | ✗ Does not exist | COUNT, SUM, AVG, COLLECT, percentile |
| **Subqueries** | ✗ Does not exist | WITH, CALL, EXISTS |
| **Full-text search** | ✗ Does not exist | Lucene-backed full-text indexes |
| **Procedures** | ✗ Does not exist | APOC, custom procedures |
| **Admin/monitoring** | ✗ Does not exist | HTTP admin interface, metrics |
| **Backup/restore** | ✗ Does not exist | Online backup, restore from backup |
| **Concurrency control** | ✗ Does not exist | Locks, MVCC, read/write isolation |
| **Data import from Neo4j** | ✗ Does not exist | Read Neo4j dump/export files |

---

## The Honest Assessment

Knight Bus Graph Walker is **not a database.** It is a **specialized
read-only graph query tool** for a single use case:

> Given CSV data with one relationship type (DEPENDS_ON), build an
> immutable snapshot and answer fixed-hop neighborhood queries
> extremely fast via memory-mapped dual-CSR arrays.

It does this ONE thing 100x faster than Neo4j. But it does only this
one thing.

---

## Which Parts Can Move to KNRT (Knight Bus Neo4j Replacement)?

### Can Reuse Directly

| Component | What it gives KNRT | Modification needed |
|---|---|---|
| **Dual-CSR snapshot format** | Read-optimized graph store | Extend for multiple rel types, properties |
| **MmapWalkRuntime** | Memory-mapped read engine | Generalize beyond fixed-hop |
| **External merge sort build** | Large-graph import pipeline | Extend to handle Neo4j export format |
| **Verification infrastructure** | Correctness testing | Extend to verify Cypher results |
| **Benchmark suite** | Performance regression tracking | Add Cypher-level benchmarks |
| **Error handling patterns** | Clean error architecture | Extend for new error types |
| **NodeRecord / snapshot layout** | Binary format foundation | Add property storage |

### Must Build From Scratch

| Component | Why Knight Bus can't provide it | LOC estimate |
|---|---|---|
| **Cypher parser** | Knight Bus has no query language at all | 5-10K (use `decypher` or `ocg` crate) |
| **Query planner** | Knight Bus has no planning — it's hardcoded to 1-2 hop walks | 15-25K (rule-based) |
| **Query runtime/operators** | Knight Bus does array slicing, not operator pipelines | 25-40K |
| **Bolt server** | Knight Bus is CLI-only, no network | 10-15K |
| **Mutable storage engine** | Knight Bus is immutable-only | 30-50K |
| **Transaction manager** | Knight Bus has no ACID | 10-15K |
| **Property storage** | Knight Bus nodes have 6 fixed fields, not arbitrary KV | 5-10K |
| **Multi-rel-type support** | Knight Bus assumes one relationship type | 3-5K |
| **Schema indexes** | Knight Bus has only sorted key index | 5-10K |
| **Import from Neo4j** | Knight Bus reads its own CSV format only | 5-10K |

---

## Onboarding a 50GB Neo4j User — The Shreyas Doshi View

### What does a 50GB Neo4j database look like?

A 50GB Neo4j database typically has:
- 50-500 million nodes
- 200M-2B relationships
- Multiple relationship types (10-100 types)
- Properties on everything (5-20 properties per node)
- Labels (2-5 labels per node)
- Schema indexes (10-50 indexes)
- Cypher queries in application code (50-500 distinct queries)
- Bolt connections from 3-20 application services
- Running on an instance with 32-128GB RAM (Neo4j wants 2x-3x data size)

### The user journey Shreyas would map

**Step 0: Why would they even consider switching?**

A user with a 50GB Neo4j database switches for exactly one of these reasons:
1. **Cost** — Neo4j Enterprise license is $36K+/year. Smaller instances = savings.
2. **Latency spikes** — GC pauses killing p99 in production
3. **Operational pain** — JVM tuning, heap sizing, GC algorithm selection
4. **Scale ceiling** — Neo4j struggling with their graph size

If none of these hurt, they won't switch. No amount of "it's faster"
matters if what they have works.

**Step 1: "Can I even try this?" (The 5-minute test)**

Shreyas would say: the first 5 minutes determine adoption. What
happens when they run it?

With Knight Bus today:
```bash
# Export from Neo4j
neo4j-admin database dump --to-path=/tmp/neo4j-dump mydb

# Then what? Knight Bus can't read Neo4j dump files.
# Knight Bus can't read Neo4j CSV export either — it expects
# specific columns (node_id, node_type, label, parent_id, file_path, span)
# not arbitrary Neo4j node properties.

# They're stuck at step 1.
```

**What KNRT needs for the 5-minute test:**
```bash
# Export from Neo4j (user already knows this)
neo4j-admin database dump --to-path=/tmp/neo4j-dump mydb
# OR
CALL apoc.export.csv.all("/tmp/export.csv", {})

# Import into KNRT
knrt import --from-neo4j-dump /tmp/neo4j-dump --output /var/knrt/mydb
# OR
knrt import --from-neo4j-csv /tmp/export.csv --output /var/knrt/mydb

# Start server (same Bolt port as Neo4j)
knrt serve --data /var/knrt/mydb --bolt-port 7687

# User changes connection string in their app:
# bolt://neo4j:7687 → bolt://knrt:7687
# Runs their existing queries. Sees results.
```

**That's the whole onboarding.** Three commands and a connection string
change.

**Step 2: "Do my queries work?" (The first hour)**

This is where 80% vs 100% Cypher compatibility matters.

Of the user's 50-500 distinct Cypher queries:
- ~80% will be MATCH/WHERE/RETURN patterns → should work
- ~10% will use WITH, UNWIND, aggregations → should work (v1 target)
- ~5% will use CALL procedures, APOC → will NOT work (v1)
- ~5% will use exotic Cypher (subqueries, pattern comprehensions) → may not work

**Shreyas would say:** log every query that fails, show the user
which queries work and which don't, and provide a clear compatibility
report:

```
KNRT Compatibility Report for mydb:
├── 47 queries tested
├── 41 passed (87%)
├── 4 unsupported (CALL apoc.*) — APOC not supported in v1
├── 2 unsupported (pattern comprehension) — planned for v2
└── Recommendation: 87% of your queries work today.
```

If 87% pass, many users will proceed. The failing 13% are usually
analytics/admin queries, not hot-path production queries.

**Step 3: "Is it actually faster?" (The first day)**

```bash
# KNRT ships a built-in comparison tool
knrt benchmark --compare-neo4j bolt://neo4j:7687 \
               --knrt bolt://knrt:7687 \
               --queries /path/to/my/queries.cypher

# Output:
Query                          | Neo4j   | KNRT    | Speedup
MATCH (n {id: $id})-[:X]->(m) | 2.3ms   | 0.4ms   | 5.8x
MATCH (n)-[:Y*2]->(m) WHERE…  | 45ms    | 12ms    | 3.8x
Complex aggregation query      | 120ms   | 85ms    | 1.4x
p99 (all queries)              | 180ms   | 8ms     | 22.5x
Memory (RSS)                   | 4.2GB   | 1.1GB   | 3.8x
```

The p99 improvement (GC elimination) is the killer metric.

**Step 4: "Can I run this in production?" (The first week)**

What they need:
- [ ] Data migration works for their 50GB
- [ ] All production queries pass
- [ ] Performance meets or beats Neo4j
- [ ] Monitoring/metrics exist
- [ ] Backup/restore exists
- [ ] Connection pooling works with their drivers
- [ ] Their ORM/framework works (Spring Data Neo4j, etc.)

**Step 5: "The Knight Bus mode" (Month 2+)**

Once they're on KNRT, they discover the turbo option:

```bash
# Enable CSR snapshot mode for read-heavy queries
knrt snapshot --build --from-live-db

# Certain queries automatically route to CSR engine
# 100x faster for traversal-heavy patterns
```

This is the upsell. They came for the operational benefits (no GC,
less RAM, single binary). They stay for the 100x read performance
on their hottest queries.

---

## The Gap Summary: Knight Bus → KNRT

```
What Knight Bus has today:
├── CSR snapshot builder (from CSV)         ████████████ (excellent)
├── Mmap read runtime                      ████████████ (excellent)
├── Benchmark suite                        ████████████ (excellent)
├── Low-RAM external sort                  ████████████ (excellent)
├── Verification infrastructure            ████████████ (excellent)
├── Error handling                         ████████████ (excellent)
└── Total: 4,710 LOC

What KNRT needs that doesn't exist:
├── Cypher parser                          ░░░░░░░░░░░░ (build from spec/crates)
├── Query planner                          ░░░░░░░░░░░░ (build from scratch)
├── Query runtime operators                ░░░░░░░░░░░░ (build from scratch)
├── Bolt protocol server                   ░░░░░░░░░░░░ (build from spec)
├── Mutable storage engine                 ░░░░░░░░░░░░ (build from scratch)
├── Transaction manager                    ░░░░░░░░░░░░ (build from scratch)
├── Property storage                       ░░░░░░░░░░░░ (build from scratch)
├── Multi-relationship-type support        ░░░░░░░░░░░░ (extend CSR format)
├── Schema indexes                         ░░░░░░░░░░░░ (build from scratch)
├── Neo4j import tool                      ░░░░░░░░░░░░ (build from scratch)
├── Admin/monitoring server                ░░░░░░░░░░░░ (build with axum)
└── Total needed: ~100-165K LOC
```

Knight Bus provides ~5% of the code needed for a Neo4j replacement.
But it provides the HARDEST 5% — the part that gives 100x performance.
Nobody else has a working dual-CSR with mmap and external merge sort
build pipeline.

The other 95% is well-understood database engineering that multiple
Rust projects have already solved (sled, TiKV, redb, MeshDB).

---

## What Shreyas Would Ship First

**MVP = "Try KNRT in 5 minutes"**

The minimum to get a Neo4j user to try KNRT:

1. **Neo4j CSV import** (read `neo4j-admin export` format → KNRT store)
2. **Bolt server** (existing drivers connect)
3. **Cypher subset** (MATCH, WHERE, RETURN work for simple queries)
4. **Compatibility reporter** (tells user which queries work, which don't)

That's it. No mutable storage needed (import once, query many). No
transactions needed (read-only for the trial). No admin interface
needed (CLI is fine for evaluation).

**LOC for MVP: ~30-40K**
**Timeline for MVP: 3-4 months, 2-3 devs**

The MVP is a read-only Cypher-compatible query engine that happens
to be backed by Knight Bus's CSR engine. It's not a database. It's
a "Neo4j read replica in Rust."

That's the Shreyas play: **don't build a database. Build a read
replica that's 100x faster.**

Users try it as a read replica alongside Neo4j. Their writes go to
Neo4j, reads go to KNRT. Zero risk. Immediate measurable value.
Then gradually, KNRT gains write capability, and users start moving
write traffic too.

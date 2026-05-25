# Storage Risk, Meta Pattern

*Why Knight Bus's storage format is superior, what changes downstream
if you choose it, and how risky that actually is.*

---

## The Meta Pattern (No Database Knowledge Required)

Forget databases for a second. Think about cookbooks.

### The General-Purpose Cookbook

A library has one massive cookbook with every recipe. When you want
to make pasta, you:

1. Go to the library
2. Find the cookbook
3. Open the index
4. Find "pasta" in the index
5. Turn to page 847
6. Read the recipe
7. Cross-reference the sauce on page 212
8. Cross-reference the timing chart on page 1,104

You're jumping around a 2,000-page book. Every cross-reference is
a page flip. The book is organized for maximum generality — it can
answer ANY cooking question — but no single question is fast.

**This is Neo4j.** The data is stored in a general-purpose format
(linked-list records) that can answer any query. But every query
requires jumping between records, following pointers, and
reconstructing the answer from scattered pieces.

### The Mise en Place Kitchen

A professional kitchen doesn't use the cookbook at service time.
Before service, the chef does **mise en place** — everything is
prepped, measured, and arranged in the exact order it will be used:

1. Pasta is portioned in containers
2. Sauce is pre-made in squeeze bottles
3. Garnish is pre-cut in reach containers
4. The cook reaches, grabs, plates — no thinking, no searching

The prep work is expensive (hours of cutting, cooking, organizing).
But service is trivially fast because everything is already in the
shape the cook needs it.

**This is Knight Bus.** The data is compiled (at build time) into
the exact shape the runtime needs. When a query comes in, the
runtime just reaches into contiguous memory and grabs the answer.
No reconstruction, no pointer-following, no searching.

### The Meta Pattern

```text
GENERAL-PURPOSE STORAGE          vs.    OPERATION-ALIGNED STORAGE
─────────────────────────                ─────────────────────────
Store once, serve any question           Prep once, serve one kind of question instantly
Flexible but slow per query              Rigid but fast per query
Work happens at query time               Work happens at build time
```

This pattern shows up everywhere in computing:

| Domain | General-purpose | Operation-aligned |
|---|---|---|
| Databases | Row store (PostgreSQL) | Column store (ClickHouse) |
| Web | Server-side rendering | Static site generation (Next.js SSG) |
| Search | Scan all documents | Pre-built inverted index (Elasticsearch) |
| Maps | Vector tiles on demand | Pre-rendered tile cache (Google Maps) |
| ML | Raw data + training | Pre-computed embeddings |
| Cooking | Cookbook at service | Mise en place |
| Knight Bus | Neo4j record store | CSR snapshot |

**The meta pattern is: if you know the access pattern at build time,
you can eliminate all query-time work by pre-arranging the data.**

Knight Bus isn't inventing anything new. It's applying a pattern
that's been proven in column stores, search engines, static site
generators, and professional kitchens. The only novelty is applying
it to graph traversal specifically.

---

## Why It's Superior for Graph Traversal

### What Neo4j Does at Query Time

When you ask Neo4j "find all neighbors of node X":

```text
Step 1: Look up node X in the node store           → random page read
Step 2: Follow pointer to first relationship        → random page read
Step 3: Decode 34-byte relationship record          → CPU work
Step 4: Follow next-relationship pointer            → random page read
Step 5: Decode next relationship record             → CPU work
Step 6: Repeat steps 4-5 for every relationship     → N random reads
Step 7: For each neighbor, look up the node record  → N more random reads
```

For a node with 100 neighbors, that's ~200 random page reads.
Each random read can be a cache miss. On spinning disk, each miss
is ~10ms. On SSD, ~0.1ms. On NVMe, ~0.01ms. But they ADD UP.

### What Knight Bus Does at Query Time

Same question, "find all neighbors of node X":

```text
Step 1: Binary search key_index for X    → ~log₂(N) comparisons in sorted array
Step 2: Read offsets[dense_id]           → 1 memory read (8 bytes)
Step 3: Read offsets[dense_id + 1]       → 1 memory read (8 bytes, same cache line)
Step 4: Slice peers[start..end]          → 1 contiguous memory read (4 bytes × degree)
```

For a node with 100 neighbors, that's 1 binary search + 1 contiguous
read of 400 bytes. The 400 bytes are in a single memory page. One
cache line fetch. Done.

### Why the Difference Is So Large

The speed difference isn't about Rust vs Java. It's about data layout:

```text
Neo4j memory layout (node with 3 neighbors):

  Page 17: [node X record: points to rel @page 42]
  Page 42: [rel record: neighbor A, next @page 89]
  Page 89: [rel record: neighbor B, next @page 3]
  Page 3:  [rel record: neighbor C, next null]

  3 neighbors = 4 random page accesses

Knight Bus memory layout (same node, same 3 neighbors):

  offsets: [..., 7, 10, ...]     ← position 7 and 10 (adjacent u64s)
  peers:   [..., A, B, C, ...]   ← positions 7, 8, 9 (adjacent u32s)

  3 neighbors = 1 contiguous read of 12 bytes
```

The CPU's cache line is 64 bytes. Knight Bus serves 16 neighbors per
cache line. Neo4j serves 0-1 per cache line (because each record is
on a random page).

**This is why the speedup is 10-100x, not 2x.** It's not a constant
factor improvement. It's a change in the memory access pattern from
random to sequential, which changes the relationship between degree
(number of neighbors) and time from O(degree × cache_miss_cost) to
O(degree × 4 bytes / cache_line_size).

---

## Downstream Impact: What Changes If You Choose CSR

### Things That Get SIMPLER

Surprisingly, choosing CSR makes several downstream components
**easier**, not harder:

#### 1. Page Cache → mmap (~500 LOC instead of ~14K LOC)

Neo4j built MuninnPageCache (14,241 LOC) because linked-list records
need careful page management — the database must track which pages
are dirty, evict cold pages, pin pages during traversal.

CSR snapshots are immutable files. You just `mmap` them and let the
OS kernel manage paging. The OS has a highly optimized page cache
built in. You don't need MuninnPageCache at all.

```text
Neo4j: Custom page cache (14,241 LOC)
  ↓
Knight Bus: mmap (already proven, ~500 LOC wrapper)
```

**Risk: NONE.** Knight Bus already does this. 23 tests pass.

#### 2. Concurrency → Simpler (immutable = no locks for reads)

Neo4j needs read/write locks (`lock/` — 5,522 LOC) and deadlock
detection because readers and writers access the same mutable records.

CSR snapshots are immutable. Multiple readers can access them
concurrently with zero synchronization. No locks, no deadlocks,
no lock contention.

```text
Neo4j: Read/write locks + deadlock detection (5,522 LOC)
  ↓
Knight Bus (reads): Zero synchronization needed
  ↓
Knight Bus (writes): Only the build pipeline needs coordination
```

**Risk: LOW.** Immutable data is the easiest concurrency model.

#### 3. Crash Recovery → Simpler (snapshots are atomic)

Neo4j's crash recovery replays the WAL to reconstruct consistent
state after a crash. This is complex because mutations can leave
the store in an intermediate state.

CSR snapshots are built atomically — either the build completes and
the snapshot exists, or it doesn't. There is no intermediate state.
Recovery = "use the last complete snapshot."

```text
Neo4j: WAL replay + checkpoint recovery (8,888 LOC)
  ↓
Knight Bus: Last complete snapshot wins (~100 LOC logic)
```

**Risk: LOW.** Atomic file creation is well-understood.

#### 4. Consistency Checks → Simpler (validate once at build)

Neo4j runs consistency checks to verify the store isn't corrupted
after crashes or bugs.

CSR snapshots are validated at build time (parity checks against
truth source). Once built, they don't degrade — they're immutable
files.

```text
Neo4j: Runtime consistency checks (1,331 LOC)
  ↓
Knight Bus: Build-time parity verification (already proven, 82 LOC)
```

**Risk: NONE.** Knight Bus already does parity checking.

### Things That Stay THE SAME

#### 5. Bolt Protocol → Identical

The wire protocol doesn't know or care about the storage engine.
Bolt serializes query results into PackStream format. Whether those
results came from linked-list records or CSR arrays, the bytes on
the wire are the same.

```text
Neo4j Bolt: [query] → [result rows] → [PackStream bytes]
  ↓
Knight Bus Bolt: [query] → [result rows] → [PackStream bytes]
```

**Risk: NONE.** Bolt is independent of storage.

#### 6. Error Codes → Identical

Error codes are a user-facing contract. Same errors regardless of
storage engine.

#### 7. Configuration → Mostly Identical

Most settings are independent of storage. A few storage-specific
settings change (e.g., `dbms.memory.pagecache.size` becomes
irrelevant because mmap handles it).

### Things That Get HARDER (or at least DIFFERENT)

#### 8. Writes → Different Model

This is the big one. Neo4j writes are:

```text
BEGIN → CREATE (n {name: "Alice"}) → COMMIT
```

Immediate. The node exists after COMMIT returns.

CSR snapshots are immutable. You can't insert a node into a sorted
array without rebuilding the array. So writes need a different model:

**Option A: Append Log + Periodic Recompile**
```text
User: CREATE (n {name: "Alice"})
System: Appends to write log → returns success
Background: Every N seconds, recompiles snapshot from log
Query: Uses latest snapshot (might not include very recent writes)
```

Latency from write to query-visible: seconds to minutes.
Acceptable for: analytics, batch processing, data warehousing.
Not acceptable for: real-time OLTP (e.g., banking, chat).

**Option B: Mutable Overlay + Immutable Base**
```text
User: CREATE (n {name: "Alice"})
System: Writes to small mutable overlay → returns success
Query: Checks overlay first, then CSR snapshot
Background: Periodically merges overlay into new snapshot
```

Latency from write to query-visible: immediate.
Complexity: queries must check two data sources.
This is essentially Timeline B (Conservative Hybrid).

**Option C: Full Rebuild (current Knight Bus model)**
```text
User: Updates CSV truth source
System: Rebuilds entire snapshot
Query: Uses new snapshot
```

Simple but only works for batch workflows.

**Risk: MEDIUM.** The write model is the main design question.
But note: Knight Bus is targeting analytics/algorithm workloads
where Option A (append + recompile) is perfectly acceptable.
For a 50GB graph with 1000 writes/day, recompiling every 60
seconds is fine.

#### 9. Query Planner Cost Model → Different

Neo4j's planner assumes random I/O costs dominate. Its cost model
says "expanding relationships is expensive" because each expansion
is pointer-chasing.

With CSR, expanding relationships is nearly free (contiguous read).
The cost model needs to reflect this:

```text
Neo4j cost model:
  AllNodesScan:     expensive (page scan)
  NodeByLabelScan:  medium (index lookup + page reads)
  Expand:           expensive (pointer chain per hop)
  Filter:           cheap (CPU only)

CSR cost model:
  AllNodesScan:     cheap (sequential array scan)
  NodeByLabelScan:  medium (index lookup)
  Expand:           VERY CHEAP (offset + slice)
  Filter:           cheap (CPU only)
```

The planner still works the same way (enumerate plans, estimate
cost, pick cheapest). Only the cost numbers change.

**Risk: LOW.** The planner structure is copyable from Neo4j. Only
the cost constants change. And for simple queries (most queries),
the planner barely matters — there's only one reasonable plan.

#### 10. Property Storage → Needs Design

Neo4j stores properties as linked-list records (41 bytes each).
CSR snapshots don't include properties today — Knight Bus only
stores topology (nodes + edges).

For a Neo4j replacement, properties need to be accessible. Options:

**Option A: PropertyPlane (column-oriented)**
Store each property as a separate typed array:
```text
property.name.strings.bin       → ["Alice", "Bob", ...]
property.age.i32.bin            → [30, 25, ...]
property.active.bool.bitset.bin → [1, 1, 0, ...]
```

Each property is a contiguous array indexed by dense_id. This is
the column-store approach — great for analytics ("average age of
all users") but requires knowing the schema at build time.

**Option B: Property sidecar (row-oriented)**
Store properties as a separate key-value store alongside the CSR:
```text
snapshot/
  forward.offsets.bin
  forward.peers.bin
  ...
  properties.db         → embedded key-value store (sled, redb)
```

More flexible (arbitrary properties), but slower for bulk scans.

**Risk: MEDIUM.** Property storage needs design, but both options
are well-understood patterns. The Atlas already includes
`PropertyPlane` in its design vocabulary.

---

## Risk Summary: Honest Assessment

| What changes | Risk level | Why |
|---|---|---|
| Page cache (mmap replaces Muninn) | **NONE** | Already proven in Knight Bus |
| Concurrency (immutable = no read locks) | **LOW** | Simpler, not harder |
| Crash recovery (atomic snapshots) | **LOW** | Simpler, not harder |
| Bolt protocol | **NONE** | Independent of storage |
| Cypher parser | **NONE** | Independent of storage |
| Error codes, config, CLI | **NONE** | Independent of storage |
| Transaction state machine | **LOW** | Simpler for reads, different for writes |
| **Write model** | **MEDIUM** | Append + recompile is different from immediate mutation |
| **Query planner cost model** | **LOW** | Same structure, different cost constants |
| **Property storage** | **MEDIUM** | Needs design, well-understood options |
| **Algorithm layout families** | **LOW** | Incremental, only build what you need |

### The Honest Risk Matrix

```text
                    LOW RISK              MEDIUM RISK           HIGH RISK
                    ────────              ───────────           ─────────
Already proven:     mmap, CSR reads,
                    parity, benchmarks

Copy from Neo4j:    Bolt, errors,         Cypher semantics
                    config, CLI,
                    import, WAL

Needs new design:   Cost model             Write model,
                    constants              property storage

Not yet needed:                            Algorithm layout
                                           families (12 of 13)
```

**The highest-risk item is the write model.** Everything else is
either already proven, copyable from Neo4j, or a well-understood
design problem.

And even the write model risk is bounded:

- For **read-heavy analytics** (the target use case): append +
  periodic recompile is fine. Risk is LOW.
- For **write-heavy OLTP**: need a mutable overlay or full mutable
  store. Risk is MEDIUM-HIGH.
- For **mixed workloads**: can start with append + recompile and
  add a mutable overlay later. Risk is bounded because the read
  path (the hard part) is already proven.

---

## The Bottom Line for Someone Who Doesn't Write Databases

### What's actually hard about writing a database?

The four hard problems in databases are:

1. **Concurrency** — multiple readers and writers at the same time
2. **Durability** — data survives crashes and power loss
3. **Consistency** — every reader sees a valid state
4. **Performance** — answering queries fast enough

### How Knight Bus's storage choice affects each one:

| Hard problem | Neo4j record store | Knight Bus CSR |
|---|---|---|
| **Concurrency** | Hard: locks, deadlocks, MVCC | **Easy**: immutable data, no locks for reads |
| **Durability** | Hard: WAL, checkpointing, recovery | **Easier**: atomic snapshot files, last-good-snapshot recovery |
| **Consistency** | Hard: in-flight mutations can be seen | **Easy**: snapshot is always consistent (built atomically) |
| **Performance** | General: can be okay at everything | **Specialized**: incredible at reads, different model for writes |

Knight Bus's storage choice makes 3 of the 4 hard database problems
**easier**. The only thing it makes harder is the write path — and
even that is "different" more than "harder."

### The actual risk in plain language:

**Low risk:** Reading data, serving queries, handling connections,
parsing Cypher, returning results, crash recovery, running multiple
queries at once. All of these are either already proven or made
simpler by choosing CSR.

**Medium risk:** Handling writes (users adding/updating/deleting data).
The current model is "rebuild the snapshot" which is fine for analytics
but too slow for applications that need instant writes. If instant
writes are needed, that's extra design work — but it's bounded and
well-understood (other databases have solved this with overlay layers).

**Not risky at all:** The stuff that scared me in the Timeline
Traverser analysis (LOC estimates, planner complexity, Bolt
implementation). Most of that is copy-work from Neo4j's structure.

### One sentence:

> Choosing CSR storage makes the hard database problems (concurrency,
> durability, consistency) easier and the easy database problem
> (writes) harder — which is a good trade if your workload is
> read-heavy, which analytics and graph algorithms always are.

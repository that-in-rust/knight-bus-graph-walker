# User Journey: 50 GB Dataset — What Happens When You Edit and Query?

*The question that breaks most dual-engine architectures: "Do I have
to wait?" This doc traces the exact user experience second by second,
grounded in how Oracle, TiDB, AlloyDB, and Grafeo solved it.*

---

## The Setup

A user has:
- 50 GB graph dataset (~50M nodes, ~500M edges)
- Initial bulk import done
- Now they're EDITING (adding nodes, relationships, properties)
- And RUNNING QUERIES (PageRank, traversals, lookups)
- Simultaneously or interleaved

**The question:** When they edit a node and then run PageRank,
does PageRank see the edit? If not, how long until it does?
Do they have to stop querying while the OLAP engine rebuilds?

---

## How the Industry Solves This

### Oracle In-Memory Column Store (since 2014)

```
User writes row         →  Row store updated IMMEDIATELY
                            Column store NOT updated yet
                            
User queries column     →  Oracle checks "transaction journal"
                            Journal = list of modified row IDs
                            For modified rows: fetch from row store
                            For unmodified rows: read from column
                            
Background repopulate   →  When journal > threshold (e.g. 20%),
                            Oracle rebuilds affected IMCUs
                            (In-Memory Compression Units)
                            Queries NEVER blocked during rebuild
```

**Key insight:** Oracle NEVER blocks queries. The column store
might be stale, but Oracle's transaction journal knows which
rows are stale and fetches those from the row store instead.
Queries are ALWAYS consistent. Performance degrades gradually
as more rows become stale (more fetches from row store), then
snaps back after repopulation.

**Repopulation time:** ~2-30 seconds for typical workloads.
Queries continue at reduced speed during repopulation.

### TiDB + TiFlash (since 2020)

```
User writes to TiKV     →  TiKV updated IMMEDIATELY
                            TiFlash replication: async via Raft
                            
User queries TiFlash    →  TiFlash checks its replication state
                            If data is fresh: read from columnar
                            If data is stale: read from TiKV
                            (automatic fallback, transparent)
                            
Replication lag          →  Typically 1-10 seconds
                            "Stale Read" feature: user can opt into
                            reading data up to N seconds old
                            for guaranteed no-fallback performance
```

**Key insight:** TiDB provides a CHOICE. Want guaranteed fresh
data? Query routes to TiKV (slower but fresh). Want guaranteed
fast analytics? Accept "stale read" with configurable staleness
window (e.g., "data from ≤5 seconds ago is fine").

### AlloyDB Columnar Engine (since 2022)

```
User writes rows        →  Row store updated IMMEDIATELY
                            Column store marks blocks "invalid"
                            
User queries            →  Query planner checks column freshness
                            Fresh blocks: read from column store
                            Invalid blocks: read from row store
                            Mixed execution, transparent to user
                            
Background refresh      →  When invalid % > threshold (default 50%),
                            background refresh rebuilds columns
                            Refresh is NON-BLOCKING
                            Manual refresh available: SELECT refresh()
```

**Key insight:** AlloyDB lets up to 50% of column data become
stale before auto-refreshing. During staleness, queries still
work — they just fall back to row store for stale blocks.
Performance degrades linearly with staleness, then recovers.

### Grafeo CompactStore (2026)

```
User calls compact()    →  Builds CSR from mutable store
                            Subsequent writes go to overlay
                            
User queries            →  Reads merge CSR base + overlay
                            Always consistent, always available
                            
User calls recompact()  →  Merges overlay into new CSR base
                            Creates fresh compact store
                            Old compact store available until done
```

**Key insight:** Grafeo's overlay model means queries NEVER
block. Writes after compact() are slightly slower (overlay)
but reads are always consistent.

---

## The Universal Pattern

**NONE of these systems block queries.** The pattern is:

```
┌─────────────────────────────────────────────────────┐
│                 THE HTAP RULE                        │
│                                                     │
│  Writes go to the OLTP store IMMEDIATELY.           │
│  OLAP store may be stale by seconds to minutes.     │
│  Queries ALWAYS work:                               │
│    - Fresh data: read from OLAP (fast path)         │
│    - Stale data: fall back to OLTP (slow path)      │
│  Background process syncs OLTP → OLAP continuously. │
│  Queries NEVER block. NEVER.                        │
│                                                     │
│  The user sees: "queries always work, analytics     │
│  get faster after the OLAP store catches up."       │
└─────────────────────────────────────────────────────┘
```

---

## Decision Frame

- **Fork in the road:** How does Knight Bus handle the moment
  between "user edits data" and "OLAP CSR reflects the edit"?

- **Desired outcome:** User NEVER waits. Queries always return.
  Analytics get fast as soon as CSR is ready.

- **Hard constraints:**
  - CSR is immutable (inserting an edge = rebuilding offsets)
  - 50 GB graph = 500M edges
  - CSR rebuild for 500M edges: ~2-8 minutes (estimate)
  - Cannot block writes OR reads during rebuild

- **Time horizon:** Steady-state experience during mixed
  read/write workload.

- **What would count as failure:**
  - "Sorry, please wait 8 minutes while we rebuild"
  - Queries returning stale data WITHOUT telling the user
  - OOM during rebuild (two copies in memory simultaneously)

---

## The User Journey: Second by Second

### Phase 1: Initial Import (happens once)

```
Time 00:00  User: "Import my 50 GB Neo4j export"
            
            Knight Bus reads CSV/dump files
            Builds OLTP record store (or ingestion store)
            Simultaneously builds CSR snapshot
            
Time ~05:00  Import complete.
             OLTP store: 50 GB on disk (mutable, writable)
             OLAP CSR:   ~10 GB on disk (immutable, fast reads)
             Both are consistent (same data, time T₀)
             
Time 05:01  User: "Run PageRank"
            → Routes to OLAP engine
            → mmap the CSR, run PageRank
            → Result in 5-20 seconds (500M edges, streaming)
            → RAM: ~800 MB - 1.6 GB resident
            
            User: "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)"
            → Routes to OLAP engine (traversal query)
            → Result in microseconds
```

**Experience:** Everything is fast. Both stores have same data.
No lag. No staleness. This is the easy case.

### Phase 2: User Starts Editing

```
Time 10:00  User: "CREATE (:Person {name: 'NewPerson'})"
            
            OLTP store: adds record IMMEDIATELY
            OLAP CSR:   DOES NOT reflect this yet
            CSR is snapshot at time T₀, edit happened at T₁
            
Time 10:01  User: "MATCH (a:Person {name: 'NewPerson'}) RETURN a"
            
            QUESTION: What happens?
```

This is the critical moment. Three possible answers:

---

## Timeline A: "Snapshot Versioning" (The Simple Model)

### How it works

CSR snapshots are versioned. Each snapshot is immutable and
represents the graph at a specific point in time.

```
Snapshot v1: graph at time T₀ (initial import)
Snapshot v2: graph at time T₁ (after batch of edits)
Snapshot v3: graph at time T₂ (after next batch)
```

**Writes** go to a mutable staging area (write-ahead log or
append log). Periodically (every N seconds or N mutations),
a background process builds a NEW CSR snapshot from the
current state.

**Reads** always run against the LATEST COMPLETED snapshot.
They never see partial rebuilds.

### The User Journey

```
Time 10:00  User: CREATE (:Person {name: 'NewPerson'})
            → Written to mutable staging area ✓
            → OLAP snapshot still v1 (no new person yet)
            
Time 10:01  User: MATCH (a:Person {name: 'NewPerson'}) RETURN a
            
            IF query routes to OLAP: ❌ NOT FOUND
               (snapshot v1 doesn't have NewPerson)
            IF query routes to OLTP: ✓ FOUND
               (staging area has it)
               
            SOLUTION: Query router checks staging area FIRST
            for lookups, falls back to OLAP for analytics.
            
Time 10:05  Background: build new snapshot v2
            Reads staging area + snapshot v1
            Builds new CSR including NewPerson
            Build takes ~2-8 minutes for 500M edges
            
Time 10:05 to 12:05  DURING REBUILD:
            → Queries still run against snapshot v1
            → New writes continue to staging area
            → NO BLOCKING anywhere
            
Time 12:05  Snapshot v2 complete
            → Atomically swap: queries now use v2
            → Staging area cleared of pre-v2 writes
            → NewPerson now visible in OLAP queries
            
Time 12:06  User: Run PageRank
            → Uses snapshot v2 (includes NewPerson) ✓
```

### Staleness Window

**2-8 minutes** between edit and OLAP visibility.

For analytics workloads, this is usually fine. Nobody runs
PageRank and expects it to reflect a node created 30 seconds
ago. The StackOverflow question is never "my PageRank is 2
minutes stale" — it's "my PageRank takes 2 hours to run."

### What the User Sees

```
Query Type           Where It Runs    Freshness      Speed
─────────────────    ─────────────    ───────────    ──────
Point lookup         OLTP store       IMMEDIATE      ~1 ms
CREATE/SET/DELETE    OLTP store       IMMEDIATE      ~1 ms
1-hop traversal      OLAP snapshot    Up to 8 min    ~50 μs
PageRank             OLAP snapshot    Up to 8 min    ~15 sec
Dijkstra             OLAP snapshot    Up to 8 min    ~2 sec
```

### Stress Points

- **"Why can't I see the node I just created in traversal?"**
  The user creates NewPerson, immediately runs a traversal,
  and NewPerson isn't there. This is confusing.
  
  **Mitigation:** Return a clear message: "Analytics snapshot
  is 3 minutes behind. Point lookups see latest data. Next
  snapshot rebuilds in ~5 minutes."

- **Rebuild cost:** 500M edges = ~2-8 min rebuild. During this
  time, you need memory/disk for BOTH old and new snapshot.
  At 10 GB per snapshot, that's 20 GB of disk temporarily.
  Manageable.

- **Write amplification:** Every rebuild rewrites the entire
  CSR, even if only 1 node was added. For 500M edges, that's
  2-4 GB of writes per rebuild cycle.

### Precedent

DuckDB WAL + checkpoint. ClickHouse MergeTree. These are
batch-rebuild systems. Proven at scale.

---

## Timeline B: "Overlay Model" (Grafeo-style)

### How it works

The CSR snapshot is immutable, but a mutable OVERLAY sits
on top. New writes go to the overlay. Reads merge the
snapshot + overlay at query time.

```
┌───────────────────────────────────────┐
│          Mutable Overlay              │
│  (new nodes, new edges, deletions)    │
│  Stored as: hash maps, small vectors  │
│  Size: proportional to # of edits    │
│  Speed: slower than CSR, but instant │
└──────────────┬────────────────────────┘
               │ merge at query time
┌──────────────▼────────────────────────┐
│       Immutable CSR Base              │
│  (bulk of the graph)                  │
│  mmap'd, fast, cache-friendly         │
│  Size: ~10 GB for 500M edges         │
└───────────────────────────────────────┘
```

### The User Journey

```
Time 10:00  User: CREATE (:Person {name: 'NewPerson'})
            → Written to overlay ✓
            → CSR base unchanged
            
Time 10:01  User: MATCH (a:Person {name: 'NewPerson'}) RETURN a
            → Query checks overlay FIRST → ✓ FOUND
            → ZERO staleness for point lookups
            
Time 10:02  User: Run PageRank
            → PageRank iterates CSR base (fast, sequential)
            → ALSO iterates overlay edges (slower, hash lookup)
            → Result includes NewPerson ✓
            → Slightly slower than pure CSR (overlay scan overhead)
            
            Speed: ~15-20 sec (vs ~15 sec pure CSR)
            The overhead is proportional to overlay size.
            100 new edges in overlay → negligible overhead
            1M new edges in overlay → ~5-10% slower
            50M new edges → overlay dominates, need recompact

Time 15:00  Background: recompact()
            → Merge overlay into new CSR base
            → Atomic swap: new base, empty overlay
            → Back to pure CSR speed
            
            DURING recompact:
            → Queries run against OLD base + overlay
            → NO BLOCKING
            → New writes go to a NEW overlay
```

### Staleness Window

**ZERO.** Every query sees every write immediately.

The tradeoff: queries get slightly slower as the overlay grows.
But there's no moment where the user says "where's my data?"

### What the User Sees

```
Query Type           Freshness      Speed (small overlay)    Speed (large overlay)
─────────────────    ───────────    ────────────────────     ─────────────────────
Point lookup         IMMEDIATE      ~1 ms                    ~1 ms
CREATE/SET/DELETE    IMMEDIATE      ~1 ms                    ~1 ms
1-hop traversal      IMMEDIATE      ~60 μs                   ~200 μs
PageRank             IMMEDIATE      ~16 sec                  ~25 sec
```

### Stress Points

- **Overlay size growth.** If the user creates 10M new edges
  without recompacting, the overlay becomes a significant
  fraction of the graph. PageRank on a 50M-edge overlay +
  500M-edge CSR is ~10% slower. At 100M new edges (20% of
  graph), it's ~20% slower. This is manageable but needs
  monitoring.

- **Recompact during heavy writes.** If the user is continuously
  writing AND the recompact is running, you need careful
  coordination: new writes go to a fresh overlay while
  recompact merges the old overlay into the base.

- **Memory during recompact.** Briefly need old base + new base
  + overlay in memory/disk. ~20 GB disk temporarily.

### Precedent

Grafeo CompactStore. LLAMA graph store (immutable arrays +
delta log). LSM-tree compaction (LevelDB, RocksDB).

---

## Timeline C: "Query Router" (Oracle/TiDB-style)

### How it works

NO overlay. The OLTP store and OLAP store are completely
separate. A query router decides which store answers each
query based on query TYPE, not data freshness.

```
┌─────────────────────────────────────────────┐
│               Query Router                   │
│                                             │
│  IF (analytics algorithm):                  │
│     → Route to OLAP (CSR snapshot)          │
│     → May be up to N minutes stale          │
│     → FAST                                  │
│                                             │
│  IF (point lookup / CRUD / traversal):      │
│     → Route to OLTP (mutable store)         │
│     → Always fresh                          │
│     → SLOWER (no CSR optimization)          │
│                                             │
│  IF (traversal wanting speed + freshness):  │
│     → Run on OLAP + check OLTP for deltas   │
│     → Merge results                         │
│     → COMPLEX but correct                   │
└─────────────────────────────────────────────┘
```

### The User Journey

```
Time 10:00  User: CREATE (:Person {name: 'NewPerson'})
            → Written to OLTP store ✓
            
Time 10:01  User: MATCH (a:Person {name: 'NewPerson'}) RETURN a
            → Router: this is a point lookup → route to OLTP
            → ✓ FOUND immediately
            
Time 10:02  User: CALL gds.pageRank()
            → Router: this is analytics → route to OLAP
            → CSR snapshot is at T₀ (NewPerson NOT included)
            → User gets PageRank WITHOUT NewPerson
            → Is this OK?
            
            FOR MOST USERS: YES.
            PageRank on 500M edges is a batch operation.
            Nobody expects it to reflect a node added 2 min ago.
            The result is: "PageRank as of [timestamp]"
            
Time 12:00  Background sync: OLTP WAL → rebuild OLAP CSR
            → New snapshot includes NewPerson
            
Time 12:01  User: CALL gds.pageRank()
            → Now includes NewPerson ✓
```

### Staleness Window

**Depends on query type:**
- Point lookups / CRUD: ZERO (always OLTP)
- Analytics: 2-8 minutes (snapshot rebuild interval)

### What the User Sees

```
Query Type           Store         Freshness        Speed
─────────────────    ──────────    ──────────       ──────
Point lookup         OLTP          IMMEDIATE         ~1 ms
CREATE/SET/DELETE    OLTP          IMMEDIATE         ~1 ms
1-hop traversal      OLTP or OLAP  Router decides    ~1 ms or ~50 μs
PageRank             OLAP          Up to 8 min       ~15 sec
"PageRank with       OLTP+OLAP     IMMEDIATE         ~20 sec
 freshness guarantee"  (merge)     (complex)
```

### Stress Points

- **Routing logic complexity.** Which queries go where? Simple
  cases are clear (PageRank → OLAP, CREATE → OLTP). But what
  about `MATCH (a)-[:KNOWS*3]->(b) WHERE a.name = 'Alice'`?
  Is that a traversal (OLTP) or analytics (OLAP)?
  
  **Answer:** Traversals from a seed node → OLTP (needs fresh
  data). Full-graph scans → OLAP. The router needs query
  classification, which is non-trivial.

- **User confusion.** "Why does PageRank not include the node
  I just created?" This is the TiDB stale-read problem.
  Requires clear messaging: "Analytics results are as of
  [timestamp]. Next sync in [N] minutes."

### Precedent

TiDB (TiKV for OLTP, TiFlash for OLAP, query optimizer routes).
Oracle (row store for DML, column store for analytics).
AlloyDB (row store + columnar engine, automatic routing).

---

## Timeline D: "Incremental CSR" (The Hard But Best Path)

### How it works

Instead of rebuilding the ENTIRE CSR for each edit, apply
edits INCREMENTALLY to the CSR. This is hard because CSR
offsets are position-dependent — inserting an edge into the
middle shifts all subsequent entries.

But there are tricks:

```
Trick 1: Append-only edges
  Don't insert into the middle of peers[].
  Append new edges to the END of peers[].
  Keep a secondary "overflow" list per node.
  
  Node 42's neighbors:
    CSR base:   peers[offsets[42]..offsets[43]]
    Overflow:   overflow_list[42] → [new_neighbor_1, new_neighbor_2]
    
  Traversal reads CSR contiguous block + overflow.
  
Trick 2: Batch-compact periodically
  When overflow lists get large, do a full rebuild.
  But between rebuilds, queries are STILL FAST because
  99% of edges are in the contiguous CSR array.

Trick 3: Tombstone deletions
  Don't remove edges from CSR. Mark them as deleted.
  Traversals skip tombstoned edges (one branch per edge).
  Compact removes tombstones.
```

### The User Journey

```
Time 10:00  User: CREATE (:Person {name: 'NewPerson'})
            → Node added to node table (append)
            → No edge changes needed yet
            
Time 10:00  User: CREATE (a)-[:KNOWS]->(NewPerson)
            → Edge appended to overflow list for node 'a'
            → CSR base unchanged
            → Visible IMMEDIATELY in traversals
            
Time 10:01  User: MATCH (a)-[:KNOWS]->(b) RETURN b
            → Read CSR base (fast, contiguous)
            → ALSO read overflow for node 'a' (1 extra lookup)
            → NewPerson appears ✓ IMMEDIATELY
            
Time 10:02  User: Run PageRank
            → Iterate CSR base + overflow lists
            → NewPerson and new edge included ✓
            → Speed: ~15-16 sec (overhead from overflow scans)
            → Only ~1% slower if overlay is small
```

### Staleness Window

**ZERO.** And NO overlay performance penalty for small edits.

The CSR base remains contiguous and cache-friendly.
Only the overflow lists add minor random-access cost.

### What the User Sees

```
Query Type           Freshness      Speed                  Notes
─────────────────    ───────────    ─────────────────      ─────────
Point lookup         IMMEDIATE      ~1 ms                  
CREATE/SET/DELETE    IMMEDIATE      ~0.5 ms (append)       
1-hop traversal      IMMEDIATE      ~55 μs (CSR + overflow)  ~10% overhead
PageRank             IMMEDIATE      ~16 sec                 ~5% overhead
DELETE edge          IMMEDIATE      ~0.5 ms (tombstone)    
After compact        IMMEDIATE      ~50 μs / ~15 sec        Back to baseline
```

### Stress Points

- **Implementation complexity.** Overflow lists + tombstones +
  periodic compaction = real engineering. Estimated: ~500-800
  LOC for the overlay logic, ~200 LOC for compaction.

- **Node additions change the offset array.** Adding a NODE
  (not just an edge) requires extending offsets[]. This can
  be handled by appending to the end (new nodes get high
  dense IDs) and keeping a mapping. But it's tricky.

- **Sorted invariants.** If the CSR base has sorted peers
  (for binary search in some algorithms), new edges in
  overflow break the sort. Either maintain sorted overflow
  (expensive) or fall back to linear scan for overflow.

### Precedent

LLAMA (Log-structured, Multi-version, Array-based — Macko et
al., EDBT 2015). CSR++ (mutable CSR with segments).
LSMGraph (LSM-tree + CSR hybrid, Alibaba/Northeastern, SIGMOD
2024).

---

## Cross-Timeline Analysis

| | A: Snapshot | B: Overlay | C: Query Router | D: Incremental CSR |
|---|---|---|---|---|
| **Staleness** | 2-8 minutes | **ZERO** | Depends on query type | **ZERO** |
| **Query blocking?** | **NEVER** | **NEVER** | **NEVER** | **NEVER** |
| **Write blocking?** | **NEVER** | **NEVER** | **NEVER** | **NEVER** |
| **Impl complexity** | **Low** (~200 LOC) | Medium (~400 LOC) | Medium (~500 LOC) | High (~800 LOC) |
| **Steady-state speed** | Baseline CSR | Degrades with overlay size | Baseline CSR | Near-baseline CSR |
| **Rebuild cost** | Full rebuild every N min | Recompact when overlay large | Full rebuild every N min | Partial compaction |
| **Disk during rebuild** | 2× snapshot temp | 2× snapshot temp | 2× snapshot temp | 1.2× snapshot temp |
| **User confusion risk** | **Medium** ("where's my node?") | **Low** | **Medium** ("stale PageRank") | **Low** |
| **Precedent** | DuckDB, ClickHouse | Grafeo, LLAMA | TiDB, Oracle, AlloyDB | LSMGraph, CSR++ |

---

## The 50 GB Reality Check

For a 50 GB dataset (~500M edges):

### CSR rebuild time estimate

```
500M edges × 8 bytes/edge = 4 GB peers array
50M nodes × 4 bytes/offset = 200 MB offsets array
Total CSR data: ~4.2 GB

Write speed (NVMe SSD): ~2 GB/sec
Build time (sort + write): ~2-5 minutes

With compression: ~2.5 GB, build time ~2-4 minutes
```

So the rebuild window is **2-5 minutes**. During this window:

| Strategy | What happens to queries? | What happens to writes? |
|---|---|---|
| A: Snapshot | Run on old snapshot | Go to staging area |
| B: Overlay | Run on CSR + overlay | Go to overlay |
| C: Router | Analytics on old snapshot, CRUD on OLTP | Go to OLTP |
| D: Incremental | Run on CSR + overflow | Go to overflow |

**All four: queries NEVER block. Writes NEVER block.**

### Memory during rebuild

```
Old CSR:     ~4.2 GB (mmap'd, mostly not resident)
New CSR:     ~4.2 GB (being written)
Staging/Overlay: proportional to edits (typically < 100 MB)

Peak disk:   ~8.4 GB temporary (old + new CSR)
Peak RSS:    ~500 MB - 1 GB (mmap handles the rest)
```

Manageable. The mmap model means you don't need 2× the CSR
in RAM — just on disk. The OS pages out the old CSR as
the new one is built.

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline B: Overlay Model.**

Zero staleness. Queries never block. Implementation is proven
(Grafeo already ships it). Performance degrades gracefully
with overlay size. Recompact is non-blocking.

This is the best UX. The user never asks "where's my data?"

### Which path is safest if things go badly?

**Timeline A: Snapshot Versioning.**

Simplest implementation. Easiest to reason about. If something
goes wrong during rebuild, you still have the old snapshot.
The 2-5 minute staleness is acceptable for analytics workloads.

**This is what Knight Bus already does.** The current
`build → snapshot → mmap` pipeline IS snapshot versioning.
You just need to make it re-runnable on demand.

### What experiment would reduce uncertainty fastest?

**Measure CSR rebuild time for 500M edges.**

Generate a synthetic 500M-edge graph. Build the snapshot.
Measure wall time. This ONE number tells you:
- If < 30 seconds: Snapshot versioning is fine (Timeline A)
- If 30 sec - 2 min: Either A or B works
- If > 2 min: Overlay model (B) or incremental (D) needed

### The recommended path for v0.0.3 → v0.1.0:

```
v0.0.3: Snapshot versioning (already built — just add rebuild)
        The "rebuild" command: knight-bus rebuild /path/to/snapshot
        User experience: "run rebuild when you want fresh analytics"
        
        This is MANUAL. Simple. No lag confusion.
        The user explicitly chooses when to rebuild.
        
v0.0.4: Background rebuild (automatic periodic rebuild)
        User sets interval: "rebuild every 5 minutes"
        Analytics are at most 5 minutes stale
        Queries never block during rebuild
        
v0.0.5: Overlay model (zero staleness)
        New writes go to overlay, merged at query time
        Background recompact merges overlay into CSR
        User never sees stale data

v0.1.0: Incremental CSR (best of all worlds)
        Overflow lists + tombstones + periodic compaction
        Zero staleness, near-baseline speed, minimal overhead
```

### The answer to your question:

> **"Do we stop queries till the OLAP version gets ready?"**
>
> **NO. Never. Not in any timeline.**
>
> The OLTP store handles writes and point lookups immediately.
> The OLAP store handles analytics on the latest snapshot.
> If the snapshot is stale, the user can:
> (a) Accept it ("PageRank as of 3 minutes ago"), or
> (b) Trigger a rebuild and wait 2-5 minutes, or
> (c) Use the overlay model where everything is always fresh.
>
> The key insight from Oracle, TiDB, and AlloyDB:
> **Stale analytics are better than blocked analytics.**
> Nobody ever said "I'd rather wait 5 minutes for PageRank
> than get results 3 minutes stale."

---

## The One-Liner for Users

```
"Your writes are instant. Your analytics are fast.
 If they're a few minutes behind, hit 'rebuild.'
 If you can't wait at all, we merge on the fly."
```

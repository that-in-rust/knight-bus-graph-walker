# Rewrite Strategy: Which Folders, What Strategy, What Run Times

*Mapping every file in the current codebase to the three-engine
architecture. What stays, what changes, what gets written new.
Grounded in actual code read, actual byte sizes, actual algorithms.*

*v2 — Corrected after rubber duck debugging. All false claims killed.*

---

## Core Facts Enumerated

### Current Codebase (v0.0.2): 4,710 LOC Rust + 532 LOC tests

```
FILE               LOC   PURPOSE
─────────────────  ────  ──────────────────────────────────────────
src/lib.rs          36   Module declarations + public re-exports
src/types.rs       596   All data types: NodeKey, DenseNodeId, NodeRecord,
                         SnapshotManifest, QueryResult, BenchmarkReport,
                         BuildMemoryBudget, etc.
src/error.rs       119   KnightBusError enum (15 variants)
src/main.rs        305   CLI: Build, Verify, Query, Bench, BenchCorpus
src/app.rs         129   Orchestration: wires low_ram build + runtime queries
src/low_ram.rs    1703   External merge-sort snapshot builder (7 phases)
src/graph.rs       225   In-memory CSR builder + multi-hop traversal
src/runtime.rs     402   MmapWalkRuntime: mmap-based CSR query engine
src/snapshot.rs    217   Snapshot writer: CSR arrays → binary files
src/bench.rs       458   Benchmark: latency measurement + RSS tracking
src/truth.rs       438   CSV loader + TruthGraphIndex for parity checks
src/parity.rs       82   Parity verification: snapshot vs truth

tests/cli.rs       254   Integration tests for CLI contract
tests/library_contract.rs  250   Library API parity tests
tests/support/mod.rs  28   Test fixtures

Cargo.toml              11 dependencies (NO rand, NO rayon)
Test fixtures           39 nodes, 67 edges (tiny graph)
```

### What the Three-Engine Architecture Needs

```
ENGINE 1: OLTP (Neo4j-compatible record store)
  STATUS: Does NOT exist. Zero code today.
  NEEDED: Record store, write-ahead log, transaction manager
  TIMELINE: v0.1.0+ (not v0.0.3)

ENGINE 2: OLAP (CSR analytics engine)
  STATUS: 80% EXISTS. MmapWalkRuntime IS the OLAP engine.
  MISSING: PageRank (parallel), synthetic graph generator, madvise hints
  TIMELINE: v0.0.3 (next release)

ENGINE 3: Query Router / Sync
  STATUS: Does NOT exist.
  NEEDED: Decides OLTP vs OLAP routing, WAL replay, overlay
  TIMELINE: v0.1.0+ (not v0.0.3)
```

---

## File-by-File Rewrite Map

### KEEP AS-IS (0 changes needed for v0.0.3)

```
FILE               LOC   REASON
─────────────────  ────  ──────────────────────────────────────────
src/parity.rs       82   Parity verification for traversals works
                         unchanged. PageRank parity is a NEW check,
                         added separately.

src/truth.rs       438   CSV loading + TruthGraphIndex. Unchanged.
                         Synthetic graph generation is a separate module.

src/graph.rs       225   In-memory CSR builder. Used by the non-low-ram
                         path (tests, small graphs). Unchanged.
                         PageRank does NOT use this — it uses
                         MmapWalkRuntime directly.

src/low_ram.rs    1703   External merge-sort builder. Unchanged.
                         This IS the build pipeline and it works.

src/snapshot.rs    217   Snapshot writer. Unchanged for v0.0.3.

tests/support/     28    Test fixture helpers. Unchanged.
```

**Total: 2,693 LOC unchanged.**

### EXTEND (add code, don't rewrite)

```
FILE               LOC   WHAT CHANGES                           ADDED LOC
─────────────────  ────  ─────────────────────────────────────  ─────────
src/lib.rs          36   Add: pub mod page_rank;                    +3
                         Add: pub mod synthetic;
                         Add: re-exports for new types

src/types.rs       596   Add: PageRankConfig, PageRankResult,      +80
                         SyntheticGraphConfig, AlgorithmReport
                         (existing types untouched)

src/error.rs       119   Add: new error variants for PageRank      +15
                         (e.g., EmptyGraph) and synthetic
                         generation (e.g., InvalidDegree)
                         (existing 15 variants untouched)

src/main.rs        305   Add: PageRank subcommand,                 +60
                         Generate subcommand
                         (existing subcommands untouched)

src/app.rs         129   Add: run_page_rank(),                     +30
                         generate_synthetic_graph()
                         orchestration functions

src/runtime.rs     402   Add: Methods to expose CSR arrays for     +65
                         PageRank iteration:
                         - forward_degree(dense_id) → u32
                         - reverse_neighbor_range(dense_id) → (usize, usize)
                         - forward_offsets_slice() → &[u64]  (unsafe cast)
                         - reverse_offsets_slice() → &[u64]  (unsafe cast)
                         - reverse_peers_slice() → &[u32]    (unsafe cast)
                         - node_count() already exists
                         Add: madvise hints (MADV_SEQUENTIAL)
                         
                         NOTE: unsafe mmap slice casts needed for
                         performance. Per-element read_u32_from_mmap
                         adds ~10 sec overhead at 100M edges due to
                         function call + bounds check + byte copy
                         per peer (2B calls across 20 iterations).
                         mmap pages are 4KB-aligned → u32/u64 aligned.

src/bench.rs       458   Add: PageRank benchmark scenario          +40
                         (existing scenarios untouched)

tests/cli.rs       254   Add: PageRank + Generate CLI tests        +40
tests/library_contract.rs 250  Add: PageRank parity tests          +30
```

**Total: ~363 LOC added to existing files.**

### CHANGED FILES (non-source)

```
FILE               WHAT CHANGES
─────────────────  ──────────────────────────────────────────
Cargo.toml         Add 2 new dependencies:
                     rand = { version = "0.8", features = ["small_rng"] }
                     rayon = "1.10"
                   
                   WHY rand: synthetic graph PRNG (deterministic
                   random graph generation). No rand in current deps.
                   
                   WHY rayon: parallel PageRank inner loop.
                   Without rayon, single-threaded PageRank is
                   30-60 sec for 100M edges — SLOWER than Neo4j
                   GDS algorithm time (5-15 sec multi-threaded).
                   With rayon (4 cores): 8-15 sec.
                   RAYON IS MANDATORY for the demo to work.

tests/fixtures/    Add: tests/fixtures/pagerank/
pagerank/            tiny_nodes.csv (4 nodes)
                     tiny_edges.csv (known edges for hand-computed PR)
                   
                   Required by PageRank correctness tests.
                   Existing fixtures (39 nodes, 67 edges) are for
                   traversal parity, not PageRank verification.
```

### NEW FILES (v0.0.3)

```
FILE                    EST. LOC   PURPOSE
──────────────────────  ────────   ──────────────────────────────────
src/page_rank.rs          170     Parallel Jacobi PageRank algorithm
                                   - Reads reverse CSR from MmapWalkRuntime
                                   - Two f64 score arrays (old, new)
                                   - rayon par_iter for parallel inner loop
                                   - Dangling node mass redistribution
                                   - Iterates until convergence or max_iter
                                   - Returns PageRankResult (scores + stats)
                                   - Optional: write scores to CSV output
                                   - Uses mmap'd CSR directly via unsafe
                                     slice casts (no per-element copy)

src/synthetic.rs          120     Random graph generator
                                   - Generates nodes.csv + edges.csv
                                   - Configurable: node count, avg degree,
                                     power-law vs uniform distribution
                                   - Deterministic seed (rand SmallRng)
                                   - Outputs CSV with EXACT headers matching
                                     low_ram.rs and truth.rs requirements:
                                     nodes: node_id,node_type,label,
                                            parent_id,file_path,span
                                     edges: from_id,edge_type,to_id
                                   - Streaming write (never holds full graph)

tests/page_rank.rs         60     PageRank correctness tests
                                   - Known small graphs with hand-computed PR
                                   - Convergence tests (scores sum to ~1.0)
                                   - Edge cases: disconnected nodes, self-loops
                                   - Dangling node handling verification
```

**Total: ~350 LOC new files.**

---

## The Architecture Map: v0.0.2 → v0.0.3 → v0.0.5 → v0.1.0

### v0.0.2 (TODAY)

```
src/
├── lib.rs              ← module root
├── types.rs            ← all types
├── error.rs            ← all errors
├── main.rs             ← CLI: Build, Verify, Query, Bench
├── app.rs              ← orchestration
├── low_ram.rs          ← CSV → CSR snapshot builder (external sort)
├── graph.rs            ← in-memory CSR builder (tests)
├── runtime.rs          ← MmapWalkRuntime (mmap query engine) ← THIS IS THE OLAP ENGINE
├── snapshot.rs         ← snapshot file writer
├── bench.rs            ← benchmark runner
├── truth.rs            ← CSV truth loader
└── parity.rs           ← parity verification
```

### v0.0.3 (NEXT RELEASE) — "+3 files, ~800 LOC total change"

```
src/
├── lib.rs              ← +3 LOC (new module declarations)
├── types.rs            ← +80 LOC (PageRankConfig, SyntheticGraphConfig)
├── error.rs            ← +15 LOC (new error variants)
├── main.rs             ← +60 LOC (PageRank + Generate subcommands)
├── app.rs              ← +30 LOC (orchestration for new commands)
├── low_ram.rs          ← UNCHANGED
├── graph.rs            ← UNCHANGED
├── runtime.rs          ← +65 LOC (CSR slice casts + madvise + degree helpers)
├── snapshot.rs         ← UNCHANGED
├── bench.rs            ← +40 LOC (PageRank benchmark scenario)
├── truth.rs            ← UNCHANGED
├── parity.rs           ← UNCHANGED
├── page_rank.rs        ← NEW: 170 LOC (parallel Jacobi PageRank + dangling nodes)
└── synthetic.rs        ← NEW: 120 LOC (random graph generator)

tests/
├── cli.rs              ← +40 LOC
├── library_contract.rs ← +30 LOC
├── support/mod.rs      ← UNCHANGED
├── page_rank.rs        ← NEW: 60 LOC
└── fixtures/pagerank/  ← NEW: 2 CSV files (tiny hand-computed graph)

Cargo.toml              ← +2 dependencies (rand, rayon)
```

**v0.0.3 delta: +800 LOC across 14 files (3 new, 11 modified).**
**2 new dependencies. 2 unsafe blocks (mmap slice casts).**
**Total codebase: ~5,542 LOC Rust + ~660 LOC tests = ~6,202 LOC.**

### v0.0.5 (OVERLAY MODEL) — "+1 file, ~400 LOC total change"

```
src/
├── ...existing...
├── overlay.rs          ← NEW: ~200 LOC
│   ├── MutableOverlay struct
│   │   ├── added_edges: Vec<(u32, u32)>
│   │   ├── tombstoned_edges: HashSet<(u32, u32)>
│   │   └── added_nodes: Vec<NodeKey>
│   ├── append_edges(csv_path) → Result
│   ├── merge_with_runtime(MmapWalkRuntime) → OverlayRuntime
│   └── recompact(output_dir) → Result<SnapshotBuildSummary>
│
├── runtime.rs          ← +100 LOC
│   └── OverlayRuntime wraps MmapWalkRuntime + MutableOverlay
│       ├── Traversal: CSR base + overlay edges merged
│       ├── PageRank: iterate CSR then overlay
│       └── Key lookup: check overlay first, then CSR
│
├── main.rs             ← +30 LOC (Append + Recompact subcommands)
├── types.rs            ← +30 LOC (overlay config types)
└── snapshot.rs         ← +40 LOC (overlay manifest fields)
```

**v0.0.5 delta: ~400 LOC. Total codebase: ~6,602 LOC.**

### v0.1.0 (OLTP + QUERY ROUTER) — major restructure

```
src/
├── lib.rs
├── types.rs
├── error.rs
├── main.rs
│
├── build/                      ← low_ram.rs splits here
│   ├── csv_builder.rs          ← current low_ram.rs (~1700 LOC, moved)
│   └── wal_builder.rs          ← NEW: ~500 LOC (WAL → CSR rebuilder)
│
├── oltp/                       ← NEW ENGINE
│   ├── record_store.rs         ← ~800 LOC (15B node + 34B rel records)
│   ├── wal.rs                  ← ~400 LOC (write-ahead log)
│   ├── transaction.rs          ← ~300 LOC (basic ACID on single records)
│   └── mod.rs                  ← ~50 LOC
│
├── olap/                       ← MmapWalkRuntime moves here
│   ├── runtime.rs              ← current runtime.rs (~470 LOC, moved)
│   ├── page_rank.rs            ← current page_rank.rs (~170 LOC, moved)
│   ├── overlay.rs              ← current overlay.rs (~200 LOC, moved)
│   └── mod.rs                  ← ~30 LOC
│
├── router/                     ← NEW: query routing
│   ├── query_classifier.rs     ← ~200 LOC (analytics vs CRUD detection)
│   ├── sync.rs                 ← ~300 LOC (OLTP WAL → OLAP rebuild trigger)
│   └── mod.rs                  ← ~30 LOC
│
├── graph.rs
├── snapshot.rs
├── bench.rs
├── truth.rs
├── parity.rs
└── synthetic.rs
```

**v0.1.0 delta: ~2,600 new LOC + restructure. Total: ~9,200 LOC.**

---

## Estimated Run Times: The Numbers

*Corrected after rubber duck analysis. Key changes:
PageRank per-iteration time now accounts for random access
to score arrays (cache miss at ~50 ns per access), and all
PageRank times assume parallel execution with rayon.*

### Baseline: What v0.0.2 Can Do Today

```
OPERATION                  10K nodes    1M nodes      10M nodes     50M nodes
                           100K edges   10M edges     100M edges    500M edges
───────────────────────    ──────────   ──────────    ──────────    ──────────
BUILD (CSV → snapshot)
  Parse + sort             <1 sec       5-10 sec      1-3 min       5-15 min
  CSR construction         <1 sec       1-3 sec       10-30 sec     1-3 min
  Write to disk            <1 sec       1-2 sec       5-10 sec      20-60 sec
  TOTAL BUILD              <1 sec       8-15 sec      2-5 min       10-25 min
  Build peak RSS           ~50 MB       ~200 MB       ~500 MB       ~2 GB*
  * with --memory-budget-mb 2048: caps at 2 GB regardless of size

SNAPSHOT SIZE ON DISK
  forward_peers            400 KB       40 MB         400 MB        2 GB
  reverse_peers            400 KB       40 MB         400 MB        2 GB
  forward_offsets          80 KB        8 MB          80 MB         400 MB
  reverse_offsets          80 KB        8 MB          80 MB         400 MB
  node_table               160 KB       16 MB         160 MB        800 MB
  key_index                40 KB        4 MB          40 MB         200 MB
  strings                  100 KB       10-30 MB      100-300 MB    0.5-1.5 GB
  TOTAL SNAPSHOT           ~1.3 MB      ~130 MB       ~1.3 GB       ~7-9 GB

QUERY (traversal on MmapWalkRuntime)
  Key lookup (binary)      <1 μs        2-5 μs        5-20 μs       10-50 μs
  1-hop (cold cache)       10-50 μs     50-200 μs     100-500 μs    200 μs-1 ms
  1-hop (warm cache)       1-5 μs       5-20 μs       10-50 μs      20-100 μs
  2-hop (cold, avg deg 10) 0.1-1 ms     0.5-2 ms      1-5 ms        2-10 ms
  2-hop (warm, avg deg 10) 10-50 μs     50-200 μs     100-500 μs    200 μs-1 ms
```

### v0.0.3: What PageRank Adds

#### Why the Per-Iteration Model Changed

The original estimate ignored the dominant cost: random access
to `scores_old[u]`. In pull-based PageRank, the neighbor ID `u`
is essentially random — it can point to any node. The score
array is 160 MB (10M nodes × 16 bytes for old+new), far larger
than L3 cache (~20-40 MB). Each access to `scores_old[u]` is
a ~50 ns cache miss.

```
Per-iteration cost breakdown (10M nodes, 100M edges):

Sequential scans (fast — hardware prefetcher helps):
  reverse_peers (400 MB):    0.04 sec   ← sequential, ~10 GB/s bandwidth
  reverse_offsets (80 MB):   0.01 sec   ← sequential
  forward_offsets (80 MB):   0.01 sec   ← semi-random but small

Random access (slow — cache misses dominate):
  scores_old[u] lookups:     100M × ~50 ns × ~80% miss rate = 4 sec
  forward_offsets[u]:        10M unique lookups × ~50 ns = 0.5 sec

Score_new writes:            sequential, negligible

SINGLE-THREAD per iteration: 4-5 sec
WITH RAYON (4 cores):        1-1.5 sec

Power-law graphs reduce this:
  High-degree hub nodes' scores stay in cache after first access.
  Real-world graphs: ~60-70% miss rate instead of 80%.
  Effective single-thread per iteration: 3-4 sec
  Effective with rayon (4 cores): 0.8-1.2 sec
```

#### Corrected Run Time Tables

```
OPERATION                  10K nodes    1M nodes      10M nodes     50M nodes
                           100K edges   10M edges     100M edges    500M edges
───────────────────────    ──────────   ──────────    ──────────    ──────────
PAGERANK (20 iterations, damping 0.85, PARALLEL with rayon)

Score arrays allocation
  2 × N × 8 bytes          160 KB       16 MB         160 MB        800 MB

Working set (mmap pages touched per iteration)
  reverse_peers (full)     400 KB       40 MB         400 MB        2 GB
  reverse_offsets (full)   80 KB        8 MB          80 MB         400 MB
  forward_offsets (degree) 80 KB        8 MB          80 MB         400 MB
  TOTAL WORKING SET        560 KB       56 MB         560 MB        2.8 GB
  + score arrays           720 KB       72 MB         720 MB        3.6 GB

Per-iteration time (WITH RAYON, 4 cores)
  64 GB server (all cached) <1 ms       0.1-0.3 sec   0.8-1.5 sec   4-8 sec
  16 GB laptop (8 GB free)  <1 ms       0.1-0.3 sec   1-2 sec       5-12 sec
  8 GB laptop (4 GB free)   <1 ms       0.2-0.5 sec   1.5-3 sec     8-20 sec

Per-iteration time (SINGLE THREAD — for reference only)
  64 GB server (all cached) <1 ms       0.3-0.8 sec   3-5 sec       15-30 sec
  16 GB laptop              <1 ms       0.3-0.8 sec   4-6 sec       20-40 sec
  8 GB laptop               <1 ms       0.5-1 sec     5-8 sec       25-50 sec

TOTAL PAGERANK (20 iterations, PARALLEL with rayon, 4 cores)
  64 GB server              <20 ms      2-6 sec       16-30 sec     80-160 sec
  16 GB laptop (8 GB free)  <20 ms      2-6 sec       20-40 sec     100-240 sec
  8 GB laptop (4 GB free)   <20 ms      4-10 sec      30-60 sec     160-400 sec

  With early convergence (typically 10-15 iterations for most graphs):
  64 GB server              <10 ms      1-4 sec       8-22 sec      40-120 sec
  16 GB laptop              <10 ms      1-4 sec       10-30 sec     50-180 sec
  8 GB laptop               <10 ms      2-8 sec       15-45 sec     80-300 sec

  Neo4j GDS (for comparison — MULTI-THREADED):
  Projection alone         <1 sec       5-15 sec      60-120 sec    300-600 sec
  Algorithm                <1 sec       2-5 sec       5-15 sec      20-60 sec
  TOTAL NEO4J              <2 sec       7-20 sec      65-135 sec    320-660 sec

PAGERANK RSS (resident memory)
  64 GB server              ~1 MB       ~72 MB        ~720 MB       ~3.6 GB
  16 GB laptop              ~1 MB       ~72 MB        ~720 MB       ~3.6 GB*
  8 GB laptop               ~1 MB       ~72 MB        ~500 MB**     ~1.5 GB**
  * OS may page out older CSR pages
  ** OS actively pages; RSS capped by available memory

  Neo4j GDS RSS:
  Projection + algorithm   ~200 MB      ~2 GB         ~8-16 GB      ~30-60 GB
```

#### The Honest Speedup Story

```
                           Knight Bus    Neo4j GDS    Speedup
                           (rayon, 4c)   (total)      
10M nodes, 100M edges:
  64 GB server             8-22 sec      65-135 sec   3-17x
  16 GB laptop             10-30 sec     65-135 sec   2-14x
  8 GB laptop              15-45 sec     OOM          ∞ (Neo4j can't run)

50M nodes, 500M edges:
  64 GB server             40-120 sec    320-660 sec  3-17x
  16 GB laptop             50-180 sec    OOM          ∞
  8 GB laptop              80-300 sec    OOM          ∞

WHERE THE SPEEDUP COMES FROM:
  Projection elimination:  60-600 sec saved (depending on graph size)
  Algorithm speed:         ~SAME as Neo4j GDS (both parallel, similar math)
  The win is skipping the step Neo4j can't skip.
  
  On laptops: the win is "runs at all" vs "OOM crash."
  On servers: the win is "skip the 60-600 sec projection tax."
```

### v0.0.3: What Synthetic Graph Generation Adds

```
OPERATION                  10K nodes    1M nodes      10M nodes     50M nodes
                           100K edges   10M edges     100M edges    500M edges
───────────────────────    ──────────   ──────────    ──────────    ──────────
GENERATE (write CSV files)
  nodes.csv size           ~500 KB      ~50 MB        ~500 MB       ~2.5 GB
  edges.csv size           ~3 MB        ~300 MB       ~3 GB         ~15 GB
  Generation time          <1 sec       3-10 sec      30-90 sec     2-8 min
  RSS during generation    ~10 MB       ~50 MB        ~100 MB       ~200 MB
  (streaming write, no full graph in memory)
  
  NOTE: Requires rand crate. Uses SmallRng for speed.
  NOTE: CSV headers must EXACTLY match:
    nodes: node_id,node_type,label,parent_id,file_path,span
    edges: from_id,edge_type,to_id
  These are hardcoded in low_ram.rs and truth.rs.
```

### v0.0.3: Full Pipeline Time (end-to-end, corrected)

```
"Generate 10M-node graph, build snapshot, run PageRank"

Step                         10M nodes / 100M edges
────────────────────────     ──────────────────────────────
1. knight-bus generate       30-90 sec
2. knight-bus build          2-5 min
3. knight-bus pagerank       8-22 sec (rayon, 64 GB server)
                             10-30 sec (rayon, 16 GB laptop)
────────────────────────     ──────────────────────────────
TOTAL (server)               3-7 minutes
TOTAL (laptop)               3-8 minutes

Same workload on Neo4j:
1. LOAD CSV                  5-15 min
2. gds.graph.project()       60-120 sec (projection)
3. gds.pageRank()            5-15 sec (multi-threaded)
────────────────────────     ──────────────────────────────
TOTAL                        7-18 minutes

DISK SPACE NEEDED (during build):
  CSV files:                 ~3.5 GB
  Build scratch (merge sort): ~6 GB
  Snapshot:                  ~1.3 GB
  TOTAL PEAK:               ~10.8 GB
  AFTER CLEANUP:             ~4.8 GB (CSV + snapshot)
```

### v0.0.5: What Overlay Adds

```
OPERATION                  10M nodes    50M nodes
                           100M edges   500M edges
───────────────────────    ──────────   ──────────
APPEND 1% new edges (1M / 5M edges)
  Parse CSV + add to overlay  <1 sec       2-5 sec
  Overlay memory footprint    8 MB         40 MB
  
PAGERANK WITH OVERLAY (1% overlay)
  Time overhead vs pure CSR   +1-3%        +1-3%
  Same total time within noise

APPEND 10% new edges (10M / 50M edges)
  Parse + add to overlay      2-5 sec      10-30 sec
  Overlay memory footprint    80 MB        400 MB
  
PAGERANK WITH 10% OVERLAY
  Time overhead vs pure CSR   +5-10%       +5-10%

RECOMPACT (merge overlay into new CSR)
  Full rebuild time           2-5 min      10-25 min
  During recompact: queries run on old CSR + overlay
  ZERO DOWNTIME
```

### v0.1.0: What OLTP Engine Adds

```
OPERATION                  Estimated
───────────────────────    ──────────
SINGLE WRITE (CREATE node)
  Record store insert        0.1-0.5 ms
  WAL append                 0.01-0.1 ms
  TOTAL                      0.1-0.6 ms
  Neo4j comparison:          0.5-2 ms (same order, no GC spikes)

SINGLE READ (MATCH by ID)
  OLTP path (record lookup)  0.5-2 ms
  OLAP path (CSR + mmap)     5-50 μs
  Router overhead             ~1 μs (query classification)

WRITE + READ CONSISTENCY
  Read-after-write via OLTP:  IMMEDIATE
  Read-after-write via OLAP:  Up to rebuild interval (2-10 min)
  
WAL REPLAY → CSR REBUILD
  Replay rate:                ~500K edges/sec
  10M new edges:              ~20 sec
  Full 500M rebuild:          2-5 min
```

---

## The v0.0.3 Implementation Plan

### What to Build (in order)

```
STEP 1: Cargo.toml changes
  Add: rand = { version = "0.8", features = ["small_rng"] }
  Add: rayon = "1.10"

STEP 2: src/synthetic.rs (~120 LOC)
  PURPOSE: Generate test data without needing external datasets
  
  pub struct SyntheticGraphConfig {
      pub node_count: u32,         // e.g. 10_000_000
      pub avg_degree: u32,         // e.g. 10 → ~100M edges
      pub seed: u64,               // deterministic randomness
      pub distribution: DegreeDistribution,  // Uniform or PowerLaw
  }
  
  pub fn generate_synthetic_graph(
      config: &SyntheticGraphConfig,
      nodes_path: &Path,
      edges_path: &Path,
  ) -> Result<SyntheticGraphSummary, KnightBusError>
  
  STRATEGY: Stream writes. Never hold full graph in memory.
  Use rand SmallRng (fast, deterministic). Write CSV rows
  directly to BufWriter.
  
  CRITICAL: CSV headers must EXACTLY match:
    nodes: node_id,node_type,label,parent_id,file_path,span
    edges: from_id,edge_type,to_id
  Use constants from truth.rs / low_ram.rs headers.
  
  Power-law distribution: for node i, degree ~ i^(-alpha).
  This models real graphs (most nodes have few edges, some have many).
  
  No dedup in generator (probability of duplicate edge is ~0.0001%
  at 10M nodes with avg degree 10). Let the builder handle it.
  No self-loops (cleaner for PageRank).

STEP 3: Extend runtime.rs (~65 LOC)
  PURPOSE: Expose CSR arrays for PageRank without per-element copy.
  
  impl MmapWalkRuntime {
      /// Returns the forward degree (outgoing edge count) for a node.
      pub fn forward_degree(&self, dense_id: u32) -> u32 {
          let offsets = self.forward_offsets_slice();
          (offsets[dense_id as usize + 1] - offsets[dense_id as usize]) as u32
      }
      
      /// Returns (start, end) indices into reverse_peers for a node.
      pub fn reverse_neighbor_range(&self, dense_id: u32) -> (usize, usize) {
          let offsets = self.reverse_offsets_slice();
          (offsets[dense_id as usize] as usize,
           offsets[dense_id as usize + 1] as usize)
      }
      
      /// Cast mmap'd bytes to u64 offset array.
      /// SAFETY: mmap is page-aligned (4 KB), validated at open time.
      fn forward_offsets_slice(&self) -> &[u64] {
          unsafe {
              std::slice::from_raw_parts(
                  self.forward_offsets.as_ptr() as *const u64,
                  self.forward_offsets.len() / 8,
              )
          }
      }
      
      /// Cast mmap'd bytes to u64 offset array.
      fn reverse_offsets_slice(&self) -> &[u64] {
          unsafe {
              std::slice::from_raw_parts(
                  self.reverse_offsets.as_ptr() as *const u64,
                  self.reverse_offsets.len() / 8,
              )
          }
      }
      
      /// Cast mmap'd bytes to u32 peer ID array.
      fn reverse_peers_slice(&self) -> &[u32] {
          unsafe {
              std::slice::from_raw_parts(
                  self.reverse_peers.as_ptr() as *const u32,
                  self.reverse_peers.len() / 4,
              )
          }
      }
      
      /// Advise OS for sequential access (PageRank full-scan pattern).
      #[cfg(unix)]
      pub fn advise_sequential(&self) {
          use libc::{c_void, madvise, MADV_SEQUENTIAL};
          unsafe {
              madvise(
                  self.reverse_peers.as_ptr() as *mut c_void,
                  self.reverse_peers.len(),
                  MADV_SEQUENTIAL,
              );
              madvise(
                  self.reverse_offsets.as_ptr() as *mut c_void,
                  self.reverse_offsets.len(),
                  MADV_SEQUENTIAL,
              );
          }
      }
  }

STEP 4: src/page_rank.rs (~170 LOC)
  PURPOSE: The headline algorithm. Proof of the CSR advantage.
  
  pub struct PageRankConfig {
      pub damping: f64,            // default 0.85
      pub max_iterations: u32,     // default 20
      pub tolerance: f64,          // default 1e-6
  }
  
  pub struct PageRankResult {
      pub scores: Vec<f64>,        // indexed by dense_id
      pub iterations_run: u32,
      pub converged: bool,
      pub wall_time_ms: f64,
      pub peak_rss_bytes: u64,
  }
  
  pub fn page_rank(
      runtime: &MmapWalkRuntime,
      config: &PageRankConfig,
  ) -> Result<PageRankResult, KnightBusError>
  
  ALGORITHM (parallel pull-based Jacobi with dangling node fix):
    N = runtime.node_count()
    scores_old = vec![1.0 / N; N]
    scores_new = vec![0.0; N]
    d = config.damping
    
    for iteration in 0..max_iterations:
      // Phase 1: Compute dangling mass
      dangling_sum = sum of scores_old[v] where forward_degree(v) == 0
      dangling_contribution = d * dangling_sum / N
      
      // Phase 2: Pull-based PageRank (PARALLEL via rayon)
      scores_new.par_iter_mut().enumerate().for_each(|(v, score)| {
        let (start, end) = runtime.reverse_neighbor_range(v as u32)
        let peers = runtime.reverse_peers_slice()
        let mut sum = 0.0
        for idx in start..end:
          let u = peers[idx]
          let degree = runtime.forward_degree(u)
          if degree > 0:
            sum += scores_old[u as usize] / degree as f64
        *score = (1.0 - d) / N + d * sum + dangling_contribution
      })
      
      // Phase 3: Convergence check
      if L1_norm(scores_new - scores_old) < tolerance:
        converged = true; break
      swap(scores_old, scores_new)
  
  MEMORY: 2 × N × 8 bytes for score arrays. Everything else mmap'd.
  
  WHY DANGLING NODES MATTER: Without redistribution, scores don't
  sum to 1.0 and convergence is wrong. Standard PageRank fix.
  
  WHY RAYON IS NEEDED: Without parallel inner loop, 100M edges
  × 20 iterations takes 60-100 sec (single thread). With rayon
  on 4 cores: 16-30 sec. The difference between "barely faster
  than Neo4j" and "3-17x faster."

STEP 5: Extend main.rs + app.rs (~90 LOC)
  PURPOSE: CLI subcommands for new features.
  
  Commands::Generate {
      nodes_csv: PathBuf,
      edges_csv: PathBuf,
      node_count: u32,
      avg_degree: u32,
      seed: Option<u64>,
  }
  
  Commands::PageRank {
      snapshot: PathBuf,
      damping: Option<f64>,        // default 0.85
      max_iterations: Option<u32>, // default 20
      tolerance: Option<f64>,      // default 1e-6
      output: Option<PathBuf>,     // optional: write scores to CSV
      top_k: Option<usize>,        // optional: print top K nodes
  }

STEP 6: Extend types.rs + error.rs (~95 LOC)
  New types and error variants for PageRank and synthetic.

STEP 7: Tests (~130 LOC)
  - PageRank on hand-computed 4-node graph (new fixture)
  - PageRank convergence test (scores sum to ~1.0)
  - PageRank dangling node test
  - Synthetic graph: verify node/edge counts + CSV headers
  - CLI integration: generate → build → pagerank pipeline
  - Benchmark: pagerank scenario added
  
  NOTE: 10M-scale tests marked #[ignore] (too slow for CI).
  Run manually during Day 6.
```

### What NOT to Build for v0.0.3

```
✗ Hash index (costs 132 MB RAM, only 2x speedup for key lookup)
✗ Overlay model (v0.0.5)
✗ OLTP record store (v0.1.0)
✗ Cypher parser (v0.2.0+)
✗ Bolt protocol (v0.2.0+)
✗ Algorithm-specific CSR layouts (v0.0.4, after baseline benchmark)
✗ Multiple algorithms (Dijkstra, BFS, etc.) — PageRank only for v0.0.3
✗ Python bindings (v0.0.4)
```

---

## The Headline Numbers (Honest, Rubber-Duck Verified)

### v0.0.3 Marketing Claim (50M nodes, 500M edges)

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  PageRank on 500M edges (with rayon, 4 cores):               │
│                                                              │
│  Knight Bus v0.0.3          Neo4j GDS 2.x                    │
│  ─────────────────          ──────────────                    │
│  Projection: 0 sec          Projection: 300-600 sec          │
│  Algorithm:  40-120 sec     Algorithm:  20-60 sec            │
│  TOTAL:      40-120 sec     TOTAL:      320-660 sec          │
│  RSS:        3.6 GB *       RSS:        30-60 GB             │
│                                                              │
│  * 800 MB heap + 2.8 GB mmap (OS-managed)                    │
│  On 8 GB laptop: RSS ~1.5 GB, time ~80-300 sec               │
│  On 8 GB laptop: Neo4j → OOM, cannot run                     │
│                                                              │
│  Speedup: 3-17x faster (from skipping projection)            │
│  Memory: 8-17x less                                          │
│                                                              │
│  HONEST NOTE: Algorithm-only time is ~same as Neo4j GDS.     │
│  The win is eliminating the projection step that Neo4j's     │
│  storage format forces. On laptops the win is "runs at all." │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### The Benchmark Story (10M nodes, 100M edges — the demo size)

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  "Generate → Build → PageRank" in one pipeline:              │
│                                                              │
│  Knight Bus v0.0.3          Neo4j GDS 2.x                    │
│  ─────────────────          ──────────────                    │
│  Generate CSV:  60 sec      LOAD CSV:        5-15 min        │
│  Build CSR:     3 min       (already loaded)                  │
│  PageRank:      8-22 sec    Projection:      60-120 sec      │
│  (rayon, 4c)               Algorithm:       5-15 sec         │
│  TOTAL:         ~4 min      TOTAL:           6-16 min         │
│  RSS:           720 MB *    RSS:             8-16 GB          │
│                                                              │
│  * 160 MB heap + 560 MB mmap                                 │
│  On 16 GB laptop: works perfectly                             │
│  On Neo4j with 8 GB heap: OOM during projection              │
│                                                              │
│  Speedup:       3-12x (total pipeline)                        │
│  Memory:        10-20x less                                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## Version Roadmap Summary

```
VERSION   WHAT                              NEW LOC   TOTAL LOC   TIMELINE
────────  ──────────────────────────────     ───────   ─────────   ────────
v0.0.2    Current: Build + Query + Bench    —         5,242       TODAY
v0.0.3    PageRank + Synthetic + Benchmark  ~800      6,042       2 weeks
v0.0.4    Dijkstra + BFS + Python bindings  ~800      6,842       2-3 weeks
v0.0.5    Overlay model (zero-stale writes) ~400      7,242       1-2 weeks
v0.0.6    madvise/mlock adaptive hints      ~100      7,342       3 days
v0.1.0    OLTP record store + query router  ~2,600    9,942       4-6 weeks
v0.2.0    Cypher subset + Bolt protocol     ~5,000    14,942      2-3 months
```

### The Build Order for v0.0.3 (corrected daily plan)

```
Day 1:  Cargo.toml (add rand, rayon) + src/synthetic.rs + Generate CLI
        → "knight-bus generate --node-count 1000 --avg-degree 5" works
        → Verify: generated CSV has correct headers, build succeeds
        
Day 2:  runtime.rs extensions (unsafe slice casts, degree helpers)
        → forward_degree(), reverse_neighbor_range(), reverse_peers_slice()
        → madvise_sequential()
        
Day 3:  src/page_rank.rs (parallel Jacobi + dangling nodes)
        → Test on 4-node hand-computed graph (new fixture)
        → Test convergence: scores sum to ~1.0
        
Day 4:  PageRank CLI (main.rs + app.rs) + benchmark integration
        → "knight-bus pagerank --snapshot X --top-k 10"
        → Benchmark: generate 10K graph → build → pagerank
        
Day 5:  Pipeline test at 1M scale
        → Generate 1M nodes, 10M edges → build → pagerank
        → Measure: wall time, RSS, convergence
        → Verify numbers within estimate ranges
        
Day 6:  Pipeline test at 10M scale (HIGH RISK DAY)
        → Generate 10M nodes, 100M edges
        → Build snapshot (~1.3 GB, needs ~10 GB disk)
        → Run PageRank, measure wall time + RSS
        → Debug any scale issues (build pipeline, convergence)
        → Compare numbers against estimates above
        
Day 7:  Polish: README update, version bump, benchmark report
        → Update Cargo.toml to 0.0.3
        → Write benchmark results to docs/
        → Tag release
        
RISK: Day 6 may expand to 2-3 days if:
  - Build pipeline has issues at 10M scale (never tested beyond 39 nodes)
  - PageRank convergence takes >20 iterations on power-law graph
  - Disk space insufficient for scratch files (~10 GB needed)
  
TOTAL ESTIMATE: 7-10 working days
```

---

## I/O Strategy: Why mmap, Not compio/io_uring

*Added after studying Apache Iggy's I/O architecture (compio-based)
and rubber-duck debugging whether io_uring would improve Knight Bus.
Full analysis: docs_PRD02/Rubber-Duck-Compio-io_uring-Analysis.md*

### v0.0.3: mmap + rayon (Confirmed Correct)

Knight Bus v0.0.3 is a **batch analytics engine**. The bottleneck
is CPU cache misses (L3 misses on PageRank score array), not disk I/O.

```
WHY mmap WINS for v0.0.3:
  - Zero-copy access to CSR arrays (pointer arithmetic, no memcpy)
  - OS manages page cache (better than userspace LRU)
  - madvise(SEQUENTIAL) enables kernel prefetching
  - rayon par_iter parallelizes the COMPUTE bottleneck
  - mmap + rayon are compatible; compio + rayon are NOT
    (compio is single-threaded async, rayon is multi-threaded sync)

WHY compio/io_uring would HURT v0.0.3:
  - Replacing rayon with compio = single-threaded PageRank = 60-100 sec = SLOWER than Neo4j
  - io_uring operates on file descriptors; can't help with Vec<f64> cache misses
  - Async I/O adds ~500 LOC of plumbing for 0% performance gain
  - Would delay v0.0.3 by 2-3 weeks
```

### v0.1.0: compio for OLTP, mmap for OLAP (Planned)

When Knight Bus adds a write path (OLTP engine), compio becomes
the RIGHT tool:

```
OLTP engine (v0.1.0):
  - WAL append: io_uring batch-submit writes + fsync → 50% lower write latency
  - Concurrent reads during writes: async I/O doesn't block query threads
  - This is EXACTLY what Iggy uses compio for (message streaming)
  
  NEW DEPENDENCY at v0.1.0:
    compio = { version = "0.18", features = ["runtime", "fs"] }
  
  DESIGN: Each engine uses the I/O model optimal for its workload:
    OLTP: compio (async, concurrent writes)
    OLAP: mmap + rayon (zero-copy reads, parallel compute)
    Bridge: channel (like Iggy's shard-to-shard communication)
```

### v0.0.6+: io_uring for Out-of-Core Analytics (Research)

For graphs that exceed RAM (500M+ edges on 8 GB machine):

```
  - Batch-prefetch CSR pages via io_uring SQ
  - Inspired by RingSampler (HotStorage 2025)
  - Alternative to mmap's blocking page faults
  - ~400 LOC, separate codepath from in-memory mmap path
  - PREREQUISITE: v0.0.3 benchmarks show I/O bottleneck on tight-RAM machines
```

### Techniques Adopted from Iggy (Low Effort)

```
TECHNIQUE                          WHERE         LOC    WHEN
posix_fadvise / madvise(SEQ)       runtime.rs    3      v0.0.3 (already planned)
Buffer pooling for merge-sort      low_ram.rs    20     v0.0.4
Vectored writes for snapshot       snapshot.rs   30     v0.0.5
```

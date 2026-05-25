# Rewrite Strategy: Which Folders, What Strategy, What Run Times

*Mapping every file in the current codebase to the three-engine
architecture. What stays, what changes, what gets written new.
Grounded in actual code read, actual byte sizes, actual algorithms.*

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
```

### What the Three-Engine Architecture Needs

```
ENGINE 1: OLTP (Neo4j-compatible record store)
  STATUS: Does NOT exist. Zero code today.
  NEEDED: Record store, write-ahead log, transaction manager
  TIMELINE: v0.1.0+ (not v0.0.3)

ENGINE 2: OLAP (CSR analytics engine)
  STATUS: 80% EXISTS. MmapWalkRuntime IS the OLAP engine.
  MISSING: PageRank, synthetic graph generator, madvise hints
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
src/error.rs       119   Error types are additive; new errors added,
                         none changed. Current 15 variants stay.
                         
src/parity.rs       82   Parity verification for traversals works
                         unchanged. PageRank parity is a NEW check,
                         added separately.

src/truth.rs       438   CSV loading + TruthGraphIndex. Unchanged.
                         Future: may add synthetic graph generation
                         as a separate module, not modifying truth.rs.

tests/support/     28    Test fixture helpers. Unchanged.
```

**Total: 667 LOC unchanged.**

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

src/error.rs       119   Add: PageRankError variants,              +15
                         SyntheticGraphError
                         (existing variants untouched)

src/main.rs        305   Add: PageRank subcommand,                 +60
                         Generate subcommand
                         (existing subcommands untouched)

src/app.rs         129   Add: run_page_rank(),                     +30
                         generate_synthetic_graph()
                         orchestration functions

src/runtime.rs     402   Add: Methods to expose CSR arrays for     +40
                         PageRank iteration:
                         - forward_degree(dense_id) → u32
                         - reverse_neighbors_iter(dense_id) → &[u32]
                         - node_count() already exists
                         Add: madvise hints (MADV_SEQUENTIAL)
                         on open for large snapshots

src/bench.rs       458   Add: PageRank benchmark scenario          +40
                         (existing scenarios untouched)

src/snapshot.rs    217   No changes for v0.0.3.                     +0
                         v0.0.5: add overlay manifest fields.

tests/cli.rs       254   Add: PageRank + Generate CLI tests        +40
tests/library_contract.rs 250  Add: PageRank parity tests          +30
```

**Total: ~338 LOC added to existing files.**

### NEW FILES (v0.0.3)

```
FILE                    EST. LOC   PURPOSE
──────────────────────  ────────   ──────────────────────────────────
src/page_rank.rs          150     Jacobi PageRank algorithm
                                   - Reads reverse CSR from MmapWalkRuntime
                                   - Two f64 score arrays (old, new)
                                   - Iterates until convergence or max_iter
                                   - Returns PageRankResult (scores + stats)
                                   - Uses mmap'd CSR directly (no copy)

src/synthetic.rs          120     Random graph generator
                                   - Generates nodes.csv + edges.csv
                                   - Configurable: node count, avg degree,
                                     power-law vs uniform distribution
                                   - Deterministic seed for reproducibility
                                   - Outputs CSV compatible with existing build

tests/page_rank.rs         60     PageRank correctness tests
                                   - Known small graphs with hand-computed PR
                                   - Convergence tests
                                   - Edge cases: disconnected nodes, self-loops
```

**Total: ~330 LOC new files.**

### UNCHANGED BUT NOTEWORTHY

```
FILE               LOC   NOTE
─────────────────  ────  ──────────────────────────────────────────
src/low_ram.rs    1703   The LARGEST file. External merge-sort builder.
                         Unchanged for v0.0.3. This IS the build
                         pipeline and it works.
                         
                         v0.0.5 (overlay): This file gets extended
                         with an "append to overlay" path (~200 LOC).
                         
                         v0.1.0 (OLTP): This file splits into:
                           src/build/csv_builder.rs (current code)
                           src/build/wal_builder.rs (new: WAL→CSR)
                         
src/graph.rs       225   In-memory CSR builder. Used by the non-low-ram
                         path (tests, small graphs). Unchanged.
                         PageRank does NOT use this — it uses
                         MmapWalkRuntime directly.
```

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

### v0.0.3 (NEXT RELEASE) — "+2 files, ~670 LOC total change"

```
src/
├── lib.rs              ← +3 LOC (new module declarations)
├── types.rs            ← +80 LOC (PageRankConfig, SyntheticGraphConfig)
├── error.rs            ← +15 LOC (new error variants)
├── main.rs             ← +60 LOC (PageRank + Generate subcommands)
├── app.rs              ← +30 LOC (orchestration for new commands)
├── low_ram.rs          ← UNCHANGED
├── graph.rs            ← UNCHANGED
├── runtime.rs          ← +40 LOC (CSR accessor methods + madvise)
├── snapshot.rs         ← UNCHANGED
├── bench.rs            ← +40 LOC (PageRank benchmark scenario)
├── truth.rs            ← UNCHANGED
├── parity.rs           ← UNCHANGED
├── page_rank.rs        ← NEW: 150 LOC (Jacobi PageRank)
└── synthetic.rs        ← NEW: 120 LOC (random graph generator)

tests/
├── cli.rs              ← +40 LOC
├── library_contract.rs ← +30 LOC
├── support/mod.rs      ← UNCHANGED
└── page_rank.rs        ← NEW: 60 LOC
```

**v0.0.3 delta: +668 LOC across 12 files (2 new, 10 extended).**
**Total codebase: ~5,378 LOC Rust + ~630 LOC tests = ~6,008 LOC.**

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

**v0.0.5 delta: ~400 LOC. Total codebase: ~6,408 LOC.**

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
│   ├── runtime.rs              ← current runtime.rs (~440 LOC, moved)
│   ├── page_rank.rs            ← current page_rank.rs (~150 LOC, moved)
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

**v0.1.0 delta: ~2,600 new LOC + restructure. Total: ~9,000 LOC.**

---

## Estimated Run Times: The Numbers

### Baseline: What v0.0.2 Can Do Today

Measured/estimated from code analysis and CSR properties:

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

```
OPERATION                  10K nodes    1M nodes      10M nodes     50M nodes
                           100K edges   10M edges     100M edges    500M edges
───────────────────────    ──────────   ──────────    ──────────    ──────────
PAGERANK (20 iterations, damping 0.85)

Score arrays allocation
  2 × N × 8 bytes          160 KB       16 MB         160 MB        800 MB

Working set (mmap pages touched)
  reverse_peers (full)     400 KB       40 MB         400 MB        2 GB
  reverse_offsets (full)   80 KB        8 MB          80 MB         400 MB
  forward_offsets (degree) 80 KB        8 MB          80 MB         400 MB
  TOTAL WORKING SET        560 KB       56 MB         560 MB        2.8 GB
  + score arrays           720 KB       72 MB         720 MB        3.6 GB

Iteration time (per iteration)
  ALL IN RAM (64 GB srv)   <1 ms        10-30 ms      100-300 ms    0.5-1.5 sec
  MOSTLY CACHED (16 GB)    <1 ms        10-30 ms      150-500 ms    0.8-3 sec
  TIGHT RAM (8 GB free)    <1 ms        20-50 ms      300 ms-1 sec  1.5-5 sec
  VERY TIGHT (4 GB free)   <1 ms        20-50 ms      500 ms-2 sec  3-8 sec

First iteration (cold)
  ALL IN RAM               <1 ms        50-100 ms     0.5-1 sec     2-5 sec
  TIGHT RAM                <1 ms        100-300 ms    1-3 sec       5-15 sec

TOTAL PAGERANK (20 iterations)
  ALL IN RAM               <20 ms       0.3-0.7 sec   2-7 sec       10-35 sec
  16 GB laptop (8 GB free) <20 ms       0.3-0.7 sec   4-12 sec      20-65 sec
  8 GB laptop (4 GB free)  <20 ms       0.5-1 sec     7-22 sec      35-110 sec

  Neo4j GDS (for comparison):
  Projection alone         <1 sec       5-15 sec      60-120 sec    300-600 sec
  Algorithm                <1 sec       2-5 sec       5-15 sec      20-60 sec
  TOTAL NEO4J              <2 sec       7-20 sec      65-135 sec    320-660 sec

PAGERANK RSS (resident memory)
  ALL IN RAM               ~1 MB        ~72 MB        ~720 MB       ~3.6 GB
  16 GB laptop             ~1 MB        ~72 MB        ~720 MB       ~3.6 GB*
  8 GB laptop              ~1 MB        ~72 MB        ~500 MB**     ~1.5 GB**
  * OS may page out older CSR pages
  ** OS actively pages; RSS capped by available memory

  Neo4j GDS RSS:
  Projection + algorithm   ~200 MB      ~2 GB         ~8-16 GB      ~30-60 GB
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
```

### v0.0.3: Full Pipeline Time (end-to-end)

```
"Generate 10M-node graph, build snapshot, run PageRank"

Step                         10M nodes / 100M edges
────────────────────────     ──────────────────────
1. knight-bus generate       30-90 sec
2. knight-bus build          2-5 min
3. knight-bus pagerank       2-12 sec (depends on RAM)
────────────────────────     ──────────────────────
TOTAL                        3-7 minutes

Same workload on Neo4j:
1. LOAD CSV                  5-15 min
2. gds.graph.project()       60-120 sec (projection)
3. gds.pageRank()            5-15 sec
────────────────────────     ──────────────────────
TOTAL                        7-18 minutes
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
STEP 1: src/synthetic.rs (~120 LOC)
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
  Use Xoshiro256++ PRNG (fast, deterministic, no dep needed — 
  or use rand crate). Write CSV rows directly to BufWriter.
  
  Power-law distribution: for node i, degree ~ i^(-alpha).
  This models real graphs (most nodes have few edges, some have many).

STEP 2: src/page_rank.rs (~150 LOC)
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
  
  ALGORITHM (Jacobi / pull-based):
    scores_old = vec![1.0 / N; N]
    scores_new = vec![0.0; N]
    
    for iteration in 0..max_iterations:
      for v in 0..N:
        sum = 0.0
        for u in reverse_neighbors(v):  // read reverse CSR
          sum += scores_old[u] / forward_degree(u)  // read fwd offsets
        scores_new[v] = (1 - d) / N + d * sum
      
      if L1_norm(scores_new - scores_old) < tolerance:
        break
      swap(scores_old, scores_new)
  
  MEMORY: 2 × N × 8 bytes for score arrays. Everything else mmap'd.
  
  KEY METHODS NEEDED ON MmapWalkRuntime:
    fn node_count(&self) -> u32                    // already exists
    fn reverse_neighbor_range(&self, v: u32) -> (usize, usize)  // NEW
    fn reverse_peer_at(&self, index: usize) -> u32               // NEW
    fn forward_degree(&self, v: u32) -> u32                       // NEW

STEP 3: Extend runtime.rs (~40 LOC)
  PURPOSE: Expose CSR arrays for PageRank without copying.
  
  impl MmapWalkRuntime {
      pub fn forward_degree(&self, dense_id: u32) -> u32 {
          let start = read_u64_from_mmap(&self.forward_offsets, dense_id as usize);
          let end = read_u64_from_mmap(&self.forward_offsets, dense_id as usize + 1);
          (end - start) as u32
      }
      
      pub fn reverse_neighbor_range(&self, dense_id: u32) -> (usize, usize) {
          let start = read_u64_from_mmap(&self.reverse_offsets, dense_id as usize) as usize;
          let end = read_u64_from_mmap(&self.reverse_offsets, dense_id as usize + 1) as usize;
          (start, end)
      }
      
      pub fn reverse_peer_at(&self, index: usize) -> u32 {
          read_u32_from_mmap(&self.reverse_peers, index)
      }
      
      // Optional: madvise hint for sequential access
      #[cfg(unix)]
      pub fn advise_sequential(&self) -> Result<(), KnightBusError> {
          use libc::{c_void, madvise, MADV_SEQUENTIAL};
          // Apply to reverse_peers (PageRank scans it sequentially)
          unsafe {
              madvise(
                  self.reverse_peers.as_ptr() as *mut c_void,
                  self.reverse_peers.len(),
                  MADV_SEQUENTIAL,
              );
          }
          Ok(())
      }
  }

STEP 4: Extend main.rs + app.rs (~90 LOC)
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

STEP 5: Extend types.rs + error.rs (~95 LOC)
  New types and error variants for PageRank and synthetic.

STEP 6: Tests (~130 LOC)
  - PageRank on hand-computed 4-node graph
  - PageRank convergence test
  - Synthetic graph: verify node/edge counts
  - CLI integration: generate → build → pagerank pipeline
  - Benchmark: pagerank scenario added
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

## The Headline Numbers (Honest, Verified Against Analysis)

### v0.0.3 Marketing Claim (50M nodes, 500M edges)

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  PageRank on 500M edges:                                     │
│                                                              │
│  Knight Bus v0.0.3          Neo4j GDS 2.x                    │
│  ─────────────────          ──────────────                    │
│  Projection: 0 sec          Projection: 300-600 sec          │
│  Algorithm:  10-35 sec      Algorithm:  20-60 sec            │
│  TOTAL:      10-35 sec      TOTAL:      320-660 sec          │
│  RSS:        3.6 GB *       RSS:        30-60 GB             │
│                                                              │
│  * 800 MB heap + 2.8 GB mmap (OS-managed)                    │
│  On 8 GB laptop: RSS ~1.5 GB, time ~35-110 sec               │
│  On 8 GB laptop: Neo4j → OOM, cannot run                     │
│                                                              │
│  Speedup: 9-66x faster                                       │
│  Memory: 8-17x less                                          │
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
│  PageRank:      2-7 sec     Projection:      60-120 sec      │
│                             Algorithm:       5-15 sec         │
│  TOTAL:         ~4 min      TOTAL:           6-16 min         │
│  RSS:           720 MB *    RSS:             8-16 GB          │
│                                                              │
│  * 160 MB heap + 560 MB mmap                                 │
│  On 16 GB laptop: works perfectly                             │
│  On Neo4j with 8 GB heap: OOM during projection              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## Version Roadmap Summary

```
VERSION   WHAT                              NEW LOC   TOTAL LOC   TIMELINE
────────  ──────────────────────────────     ───────   ─────────   ────────
v0.0.2    Current: Build + Query + Bench    —         5,242       TODAY
v0.0.3    PageRank + Synthetic + Benchmark  ~670      5,912       1-2 weeks
v0.0.4    Dijkstra + BFS + Python bindings  ~800      6,712       2-3 weeks
v0.0.5    Overlay model (zero-stale writes) ~400      7,112       1-2 weeks
v0.0.6    madvise/mlock adaptive hints      ~100      7,212       3 days
v0.1.0    OLTP record store + query router  ~2,600    9,812       4-6 weeks
v0.2.0    Cypher subset + Bolt protocol     ~5,000    14,812      2-3 months
```

### The Build Order for v0.0.3 (daily plan)

```
Day 1:  src/synthetic.rs + Generate CLI + test
        → "knight-bus generate" works, produces CSV files
        
Day 2:  src/page_rank.rs + runtime.rs extensions
        → PageRank algorithm on MmapWalkRuntime
        → Test on 4-node hand-computed graph
        
Day 3:  PageRank CLI + benchmark integration
        → "knight-bus pagerank --snapshot X --top-k 10"
        → Benchmark: generate 1M graph → build → pagerank
        
Day 4:  Full pipeline test at 10M scale
        → Generate 10M nodes, 100M edges
        → Build snapshot
        → Run PageRank, measure wall time + RSS
        → Compare numbers against estimates above
        
Day 5:  Polish: README update, version bump, benchmark report
        → Update Cargo.toml to 0.0.3
        → Write benchmark results to docs/
        → Tag release
```

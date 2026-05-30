# Timeline Traverser: Three-Engine Architecture — OLTP + OLAP-RAM + OLAP-Latency

*Your suggestion: keep OLTP the same shape as Neo4j, then offer
TWO OLAP variants — one optimized for RAM, one optimized for
latency. This is how you serve both the 16GB laptop and the
128GB analytics server from the same codebase.*

---

## Decision Frame

- **Fork in the road:** Given the OLTP/OLAP split, do you build
  ONE OLAP engine or TWO? If two, what are the exact design
  differences that make one RAM-efficient and the other latency-
  optimal?

- **Desired outcome:** A user on a 16GB laptop runs PageRank on
  a 50M-edge graph without swapping. A user on a 128GB server
  runs the same PageRank 5x faster. Same query, same Cypher,
  different OLAP engine selected automatically or by config.

- **Hard constraints:**
  - Both OLAP variants read from the same OLTP WAL
  - Both produce correct results (same answers)
  - User doesn't need to know which variant is running
  - Single codebase (shared traits, different implementations)

- **What would count as failure:**
  - OLAP-RAM uses MORE memory than Neo4j GDS projection
  - OLAP-Latency is SLOWER than Neo4j GDS computation
  - Maintaining two OLAP engines doubles the bug surface

---

## What Already Exists in the Codebase

Knight Bus ALREADY has the seeds of both variants:

### OLAP-RAM seed: `MmapWalkRuntime`

```rust
// src/runtime.rs — current implementation
pub struct MmapWalkRuntime {
    forward_offsets: Mmap,   // OS manages pages
    forward_peers: Mmap,     // only hot pages resident
    reverse_offsets: Mmap,   // cold pages evicted by OS
    reverse_peers: Mmap,
    node_table: Mmap,
    strings: Mmap,
    key_index: Mmap,         // binary search, no full load
}
```

This IS an OLAP-RAM engine. The OS decides what stays resident.
On a 16GB machine with a 5GB snapshot, the OS will keep hot
pages (~1-2 GB) and evict cold pages. Traversal works but may
page-fault on cold regions.

### OLAP-RAM seed: `BuildMemoryBudget`

```rust
// src/types.rs — existing budget control
pub struct BuildMemoryBudget {
    bytes: usize,  // default 64 MB
}
```

The build pipeline already supports memory-constrained operation
via external merge sort (`low_ram.rs`, 1,703 LOC). This pattern
extends naturally to query-time memory management.

### What's missing for OLAP-Latency

No existing code pins data in RAM or pre-computes algorithm-
specific auxiliary structures. The current `MmapWalkRuntime`
treats all data the same — mmap'd, OS-managed.

---

## The Three Engines — Concrete Design

### Engine 1: OLTP (Same Shape as Neo4j)

```
┌─────────────────────────────────────────┐
│              OLTP Engine                │
│                                         │
│  Record Store (Rust port of Neo4j):     │
│  ┌─────────────┐ ┌──────────────────┐  │
│  │ Node Store  │ │ Relationship     │  │
│  │ 15B/record  │ │ Store 34B/record │  │
│  └─────────────┘ └──────────────────┘  │
│  ┌─────────────┐ ┌──────────────────┐  │
│  │ Property    │ │ B+tree Indexes   │  │
│  │ Store 41B   │ │                  │  │
│  └─────────────┘ └──────────────────┘  │
│  ┌─────────────┐ ┌──────────────────┐  │
│  │ WAL         │ │ Lock Manager     │  │
│  │ (append)    │ │ (MVCC/2PL)      │  │
│  └─────────────┘ └──────────────────┘  │
│                                         │
│  Handles: CREATE, SET, DELETE, MERGE    │
│  Read-after-write: immediate            │
│  Consistency: full ACID                 │
└─────────────────────────────────────────┘
```

**Not changing.** Same format, same guarantees, same LOC estimate
(~30-40K) as previous analysis. This document focuses on the
two OLAP variants.

### Engine 2: OLAP-RAM ("The Backpacker")

**Design philosophy:** Fit anywhere. Use minimum memory. Accept
slower query speed as the tradeoff. Ideal for laptops, CI
runners, dev machines, small VMs.

```
┌─────────────────────────────────────────────────┐
│              OLAP-RAM Engine                    │
│              "The Backpacker"                   │
│                                                 │
│  Storage: Compressed CSR on disk                │
│  ┌───────────────────────────────────────────┐  │
│  │ forward.offsets.u64   (mmap, OS-paged)   │  │
│  │ forward.peers.u32     (mmap, OS-paged)   │  │
│  │ reverse.offsets.u64   (mmap, OS-paged)   │  │
│  │ reverse.peers.u32     (mmap, OS-paged)   │  │
│  │ props.*.column         (mmap, OS-paged)   │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  Compression: delta-encoded offsets,            │
│    varint peers, dictionary-encoded strings,    │
│    bitpacked booleans                           │
│                                                 │
│  Algorithm execution:                           │
│    · Stream through mmap'd arrays               │
│    · Working set in bounded buffer              │
│    · Spill intermediate results to disk         │
│    · External merge for large aggregations      │
│                                                 │
│  Memory control:                                │
│    · QueryMemoryBudget (like BuildMemoryBudget) │
│    · madvise(MADV_SEQUENTIAL) for scans         │
│    · madvise(MADV_RANDOM) for lookups           │
│    · Explicit page eviction after use           │
│                                                 │
│  Key trait: WalkQueryRuntime (already exists)    │
│  Extends with: AnalyticsRuntime (new)           │
└─────────────────────────────────────────────────┘
```

**What makes OLAP-RAM different from current `MmapWalkRuntime`:**

| Feature | Current MmapWalkRuntime | OLAP-RAM |
|---|---|---|
| Compression | None (raw bytes) | Delta + varint + dictionary |
| Memory hints | None | madvise per access pattern |
| Algorithm support | Traversal only | PageRank, Dijkstra, etc. |
| Working set control | None (OS decides) | Bounded buffer with spill |
| Intermediate results | In-memory Vec | Spill to disk if over budget |
| Score arrays | N/A | Streaming or memory-mapped |

**Compression details for OLAP-RAM:**

```
Uncompressed CSR (Medium: 10M nodes, 100M edges):
  offsets:   80 MB × 2    = 160 MB
  peers:    400 MB × 2    = 800 MB
  key_index:               320 MB
  properties:            2,000 MB
  ────────────────────────────────
  Total:                 3,280 MB

Compressed CSR:
  offsets (delta-encoded):  ~40 MB × 2  =  80 MB  (50% savings)
  peers (varint-encoded):  ~280 MB × 2  = 560 MB  (30% savings)
  key_index (prefix-compressed): ~200 MB           (38% savings)
  properties (dictionary + bitpack): ~1,200 MB     (40% savings)
  ────────────────────────────────────────────────
  Total:                              ~2,040 MB

  Savings: 38% smaller on disk
  Decompression: streaming, no full materialization needed
```

**Algorithm execution in OLAP-RAM (PageRank example):**

```
Standard PageRank needs:
  · Score array: 10M × 8B = 80 MB (f64 per node)
  · Reverse CSR: streaming read (400 MB, not all resident)

OLAP-RAM approach:
  1. Allocate score array: 80 MB (must be resident)
  2. Stream reverse CSR via mmap with MADV_SEQUENTIAL
     → OS prefetches pages sequentially
     → Only ~8-16 MB resident at any time (read window)
  3. Per iteration: scan reverse CSR, accumulate scores
  4. Total resident memory: ~100-120 MB
  5. madvise(MADV_DONTNEED) on reverse CSR after each iteration
     → releases pages back to OS

  Comparison:
    Neo4j GDS: ~4-8 GB (full projection in heap)
    OLAP-RAM:  ~100-120 MB + OS page cache
```

### Engine 3: OLAP-Latency ("The Dragster")

**Design philosophy:** Maximum speed. Pin everything in RAM.
Pre-compute every auxiliary structure. Ideal for analytics
servers, benchmark runs, production algorithm pipelines.

```
┌─────────────────────────────────────────────────┐
│              OLAP-Latency Engine                │
│              "The Dragster"                     │
│                                                 │
│  Storage: Pinned CSR in RAM + specialized       │
│  ┌───────────────────────────────────────────┐  │
│  │ forward.offsets     (mlock'd, no faults)  │  │
│  │ forward.peers       (mlock'd, no faults)  │  │
│  │ reverse.offsets     (mlock'd, no faults)  │  │
│  │ reverse.peers       (mlock'd, no faults)  │  │
│  │ props.*             (mlock'd, all cols)    │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  Pre-computed auxiliary structures:              │
│  ┌───────────────────────────────────────────┐  │
│  │ degree[].u32      (pre-computed per node) │  │
│  │ in_degree[].u32   (for PageRank mass)     │  │
│  │ dangling.bitset   (no-outgoing-edge flag) │  │
│  │ sorted_by_degree  (for triangle count)    │  │
│  │ weight_inline[]   (edge weights co-located│  │
│  │                    with peers array)       │  │
│  │ hash_index        (O(1) key lookup)       │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  Algorithm-specific layouts (the Atlas):         │
│  ┌───────────────────────────────────────────┐  │
│  │ InboundPowerLayout   (PageRank)           │  │
│  │ RelaxationFrontierLayout (Dijkstra)       │  │
│  │ OrderedWedgeLayout   (Triangle Count)     │  │
│  │ ConnectivityLowlinkLayout (SCC/WCC)       │  │
│  │ ...built on demand, cached in RAM         │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  Parallelism:                                    │
│  ┌───────────────────────────────────────────┐  │
│  │ rayon thread pool for parallel iteration  │  │
│  │ SIMD-optimized score accumulation         │  │
│  │ Partitioned CSR for NUMA-aware access     │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  Key trait: AnalyticsRuntime (new)               │
│  + AlgorithmLayout trait per specialized format   │
└─────────────────────────────────────────────────┘
```

**What makes OLAP-Latency different from OLAP-RAM:**

| Feature | OLAP-RAM | OLAP-Latency |
|---|---|---|
| Memory model | mmap, OS-managed pages | mlock'd, ALL data pinned in RAM |
| Page faults | Yes (cold pages fault in) | **Zero** (everything resident) |
| Key lookup | Binary search O(log n) | **Hash index O(1)** |
| Degree lookup | Compute from offsets | **Pre-computed degree[] array** |
| PageRank auxiliary | None (compute on the fly) | **Dangling bitset + mass array** |
| Triangle count | Sort at query time | **Pre-sorted adjacency by degree** |
| Dijkstra weights | Separate column read | **Weights inlined with peers** |
| Parallelism | Single-threaded iteration | **rayon parallel iteration** |
| Compression | Delta + varint + dictionary | **None (raw bytes for speed)** |
| Score arrays | Streaming | **Pre-allocated, pinned** |

---

## Hard Numbers: OLAP-RAM vs OLAP-Latency

### Medium Workload (10M nodes, 100M edges)

#### Disk Usage

| Component | OLAP-RAM | OLAP-Latency |
|---|---|---|
| Base CSR | 2,040 MB (compressed) | 3,280 MB (raw) |
| Key index | 200 MB (prefix-compressed) | 320 MB (raw) + 160 MB hash |
| Properties (5/node, 2/edge) | 1,200 MB (dictionary) | 2,000 MB (raw) |
| Auxiliary arrays | 0 | 240 MB (degree, in_degree, dangling) |
| Specialized layouts (×3) | 0 (not pre-built) | 3-6 GB (pre-built, pinned) |
| **Total OLAP disk** | **≈ 3.4 GB** | **≈ 6-12 GB** |
| **+ OLTP record store** | **+ 16.6 GB** | **+ 16.6 GB** |
| **Grand total** | **≈ 20 GB** | **≈ 23-29 GB** |

#### RAM Usage (Resident / Physical)

| Component | OLAP-RAM | OLAP-Latency |
|---|---|---|
| CSR arrays (resident) | 100-500 MB (OS pages in what's hot) | **3,280 MB** (all pinned) |
| Key index | 20-200 MB (binary search pages in) | **480 MB** (raw + hash, pinned) |
| Properties | 0-500 MB (only accessed cols) | **2,000 MB** (all pinned) |
| Auxiliary arrays | 0 | **240 MB** |
| Specialized layouts | 0 (built on demand, streamed) | **3,000-6,000 MB** (pinned) |
| Algorithm working set | 80-200 MB (score arrays) | **80-200 MB** (score arrays) |
| **Total OLAP RAM** | **≈ 200 MB - 1.4 GB** | **≈ 6-12 GB** |
| **+ OLTP RAM** | **+ 12-20 GB** | **+ 12-20 GB** |
| **Grand total RAM** | **≈ 12-22 GB** | **≈ 18-32 GB** |

**Key insight:** OLAP-RAM adds only 200 MB - 1.4 GB to the
system. On a 16 GB machine, the OLTP engine gets most of the
RAM (page cache for record store), and OLAP-RAM operates in
the margins. On a 64 GB machine, OLAP-Latency pins everything
and delivers maximum speed.

#### Query Latencies

| Query | OLAP-RAM | OLAP-Latency | Ratio | Neo4j |
|---|---|---|---|---|
| **1-hop traversal** (degree 10) | 10-200 μs | **3-10 μs** | 3-20x | 1-5 ms |
| | (page fault possible) | (zero faults) | | |
| **2-hop traversal** (100 nodes) | 100 μs - 2 ms | **30-100 μs** | 3-20x | 2-10 ms |
| **BFS 1000 nodes** | 1-10 ms | **0.3-2 ms** | 3-5x | 10-50 ms |
| **Key lookup** | 5-50 μs | **0.1-1 μs** | 50x | 0.5-2 ms |
| | (binary search, cold) | (hash, pinned) | | |
| **PageRank** (100M edges) | 15-60 sec | **1-5 sec** | 3-12x | 100-200 sec |
| | (streaming mmap, 1 thread) | (pinned, parallel) | | |
| **PageRank w/ InboundPower** | 10-40 sec | **0.5-3 sec** | 4-13x | 100-200 sec |
| | (streaming, no aux) | (pinned + aux arrays) | | |
| **Dijkstra SSSP** (10M nodes) | 5-30 sec | **1-5 sec** | 3-6x | 50-100 sec |
| | (weight in separate col) | (weight inlined) | | |
| **Triangle Count** (100M edges) | 30-120 sec | **3-15 sec** | 4-10x | 50-200 sec |
| | (sort at query time) | (pre-sorted by degree) | | |

#### Why OLAP-Latency Is 3-20x Faster Than OLAP-RAM

Five sources of speedup, each independent:

```
1. Zero page faults (mlock vs mmap)
   Impact: 2-5x for random access (traversal)
           1.2-1.5x for sequential access (PageRank scan)
   Why: Each mmap page fault costs ~2-5 μs on SSD.
        10 faults per query = 20-50 μs overhead.
        mlock eliminates ALL faults.

2. Hash index vs binary search (key lookup)
   Impact: 10-50x for key resolution
   Why: Binary search on mmap'd key_index: O(log n) comparisons,
        each may fault. For 10M keys: ~23 comparisons.
        Hash: O(1), ~100ns, zero faults.

3. Parallel iteration (rayon vs single-thread)
   Impact: 4-8x for compute-heavy algorithms (PageRank, BFS)
   Why: 8-16 core machine. rayon splits score accumulation
        across cores. Each core processes a partition of nodes.
        OLAP-RAM stays single-threaded to avoid amplifying
        page fault cost across threads.

4. Pre-computed auxiliary arrays
   Impact: 1.3-2x per iteration for algorithms that need them
   Why: PageRank with pre-computed dangling bitset: skip
        degree-zero check per node (~1 branch per node eliminated).
        Triangle count with pre-sorted adjacency: O(min(d1,d2))
        intersection instead of O(d1 * log(d2)) sort-then-search.

5. No decompression overhead
   Impact: 1.1-1.3x for sequential scans
   Why: OLAP-RAM uses delta/varint encoding. Each access requires
        decompression. OLAP-Latency uses raw bytes — memcpy speed.
        Only matters for sequential scans (PageRank, BFS).

Combined: 2-5 × 1.2 × 4-8 × 1.3 × 1.1 ≈ 15-70x theoretical
Realistic (not all apply to every query): 3-20x
```

### Large Workload (100M nodes, 1B edges)

| Metric | OLAP-RAM | OLAP-Latency | OLTP |
|---|---|---|---|
| **Disk (OLAP only)** | **~34 GB** | ~60-120 GB | 166 GB |
| **RAM (OLAP only)** | **~1-5 GB** | ~60-120 GB | 48-64 GB |
| **Minimum machine** | **32 GB total** | **128 GB total** | 64 GB |
| **PageRank** | 150-600 sec | **10-50 sec** | 1000-2000 sec |
| **1-hop traversal** | 10-500 μs | **3-10 μs** | 1-5 ms |

**The fork at Large scale:** OLAP-Latency needs 128 GB to pin a
1B-edge graph. OLAP-RAM operates comfortably at 32 GB total.
This is where having BOTH variants matters most.

---

## Timeline A: "Build OLAP-RAM Only" (Conservative)

### Opening Move

Extend current `MmapWalkRuntime` with compression, madvise hints,
and bounded algorithm execution. No OLAP-Latency.

### Week 1

- Add `madvise(MADV_SEQUENTIAL)` to traversal scans
- Add `QueryMemoryBudget` (like `BuildMemoryBudget`)
- Implement streaming PageRank with bounded score buffer

### Month 1

- OLAP-RAM handles all algorithms with memory-bounded execution
- Performance: 15-60 sec for PageRank on Medium (3-13x vs Neo4j)
- RAM: 200 MB - 1.4 GB for OLAP (fits any machine)
- Add delta compression for offsets, varint for peers

### Quarter 1

- 5-8 algorithms working in streaming mode
- Import pipeline produces compressed CSR snapshots
- WAL replay builds compressed CSR incrementally
- OLTP + OLAP-RAM total RAM: 12-22 GB for Medium workload

### Long-term Shape

A system that runs anywhere — laptops, CI, small VMs. Slower
than a pinned engine but always correct and never OOMs.

### Likelihood: 80% ships, 55% succeeds

Easy to build (extends existing code). Moderate commercial
success because competitors with pinned engines will outbenchmark.

### Stress Points

- Month 3: Benchmarks against Neo4j GDS show 3-13x, not 100x.
  "Where's the 100x we were promised?"
- Month 6: Large graph users report acceptable but not exciting
  performance. OLAP-RAM is streaming — it's correct, not fast.

---

## Timeline B: "Build OLAP-Latency Only" (Aggressive)

### Opening Move

Build a new runtime (`PinnedAnalyticsRuntime`) that mlock's
all CSR arrays, pre-computes auxiliary structures, and uses
rayon for parallel execution.

### Week 1

- New struct: `PinnedAnalyticsRuntime` with `mlock` on open
- Hash index for O(1) key lookup
- Pre-compute degree[] and in_degree[] arrays on load
- rayon parallel PageRank

### Month 1

- PageRank: 1-5 sec for Medium (20-200x vs Neo4j)
- RAM: 6-12 GB for OLAP alone
- But: doesn't work on 16 GB machines (OLTP needs 12-20 GB)
- Users with 32 GB machines: can't run OLTP + OLAP-Latency

### Quarter 1

- 5-8 algorithms with pre-computed auxiliary structures
- Specialized layouts (InboundPower, RelaxationFrontier, OrderedWedge)
  built on demand and pinned
- Benchmark numbers are spectacular: 50-200x over Neo4j
- But: requires 64 GB+ for Medium, 128 GB+ for Large

### Long-term Shape

A benchmark champion. Fastest graph analytics engine in existence.
But only runs on beefy servers.

### Likelihood: 70% ships, 45% succeeds

Harder to ship (more complex runtime, mlock edge cases, NUMA).
Lower commercial success because it excludes laptop/small-VM users.

### Stress Points

- Month 1: "I can't run this on my laptop." Immediate adoption
  barrier.
- Month 6: Cloud users complain about instance costs. "I need a
  128 GB VM just for analytics?"

---

## Timeline C: "Build Both, One Trait" (The Suggestion)

### The Bet

Build BOTH OLAP-RAM and OLAP-Latency behind a shared trait.
Select automatically based on available RAM, or let user configure.

### Opening Move

Define the shared trait:

```rust
pub trait AnalyticsRuntime: WalkQueryRuntime {
    fn page_rank(&self, config: PageRankConfig) -> Vec<f64>;
    fn dijkstra(&self, source: DenseNodeId, config: DijkstraConfig) -> Vec<f64>;
    fn triangle_count(&self, config: TriangleConfig) -> u64;
    fn bfs(&self, source: DenseNodeId, config: BfsConfig) -> Vec<DenseNodeId>;
    // ... one method per algorithm family
    
    fn variant(&self) -> OlapVariant;
    fn memory_usage(&self) -> MemoryReport;
}

pub enum OlapVariant {
    Ram,      // "The Backpacker" — minimum memory
    Latency,  // "The Dragster" — maximum speed
}
```

Auto-selection logic:

```rust
fn select_olap_variant(available_ram: u64, snapshot_size: u64) -> OlapVariant {
    // If we can pin the entire snapshot + 50% headroom for
    // working set, use Latency variant
    if available_ram > snapshot_size * 3 {
        OlapVariant::Latency
    } else {
        OlapVariant::Ram
    }
}
```

### Week 1

- Define `AnalyticsRuntime` trait
- Implement OLAP-RAM variant first (extends existing MmapWalkRuntime)
- Add madvise hints + QueryMemoryBudget
- Implement streaming PageRank

**Lived experience:** Productive. You're extending proven code.
The trait boundary forces clean separation. Each algorithm is
a method, not a tangled mess.

### Month 1

- OLAP-RAM: all algorithms work, streaming, memory-bounded
- Start OLAP-Latency: PinnedAnalyticsRuntime with mlock
- OLAP-Latency: PageRank + BFS working (hash index, rayon, pinned)
- Auto-selection: check `sysinfo::System::available_memory()`
  at startup, pick variant

**Lived experience:** Two runtimes, one trait. Tests run against
both. Any algorithm that works on OLAP-RAM automatically has a
correctness baseline for OLAP-Latency. The trait IS the test
contract.

### Quarter 1

- Both variants support 8-10 algorithms
- OLAP-RAM: 3-13x over Neo4j, runs on 16 GB machines
- OLAP-Latency: 20-200x over Neo4j, needs 64 GB+ machines
- User config: `olap_mode = "auto" | "ram" | "latency"`
- WAL replay builds CSR snapshots; OLAP-Latency additionally
  pre-computes aux arrays and pins everything

**LOC estimate at Quarter 1:**
```
OLTP engine:                    30-40K
OLAP-RAM (extends existing):   8-12K
OLAP-Latency (new runtime):   12-18K
Shared trait + algorithms:      5-8K
Cypher/Bolt/Router:            25-35K
──────────────────────────────────────
Total:                         80-113K
```

### Year 1

- Both variants mature and battle-tested
- OLAP-RAM adds compression (30-40% smaller snapshots)
- OLAP-Latency adds SIMD, NUMA-aware partitioning
- Algorithm-specific layouts work with both variants:
  - OLAP-RAM streams through them (slower but fits in RAM)
  - OLAP-Latency pins them (faster, needs more RAM)

### Long-term Shape

A system that scales DOWN to laptops and UP to analytics servers.
The same binary, the same API. The runtime adapts to the hardware.

This is rare. Most databases either assume big servers (Neo4j,
TigerGraph) or small devices (SQLite). Having both in one
system — auto-selected — is a genuine differentiator.

### Likelihood: 65% ships (both variants), 65% succeeds

Harder to ship than either variant alone (two runtimes to
maintain), but higher commercial success because you don't
exclude any user segment. The shared trait reduces the
maintenance burden significantly.

### Stress Points

- **Month 2:** Testing doubles. Every algorithm test runs against
  both OLAP-RAM and OLAP-Latency. But the trait makes this
  automatic: `#[test_case(OlapVariant::Ram; "ram")]
  #[test_case(OlapVariant::Latency; "latency")]`.
  → Mitigation: parametric tests. Write once, run against both.

- **Month 4:** Edge cases where variants produce different
  floating-point results (parallel reduction order differs).
  → Mitigation: tolerance-based assertions for algorithm results.
  PageRank scores match to 6 decimal places, not bit-exact.

- **Month 6:** Performance bug in one variant but not the other.
  → Mitigation: benchmark both variants in CI. Regression alerts
  per variant.

- **Month 9:** Feature request that's easy for OLAP-Latency but
  hard for OLAP-RAM (e.g., "pre-compute all auxiliary arrays").
  → Mitigation: auxiliary arrays are OPTIONAL in OLAP-RAM.
  If they fit in budget, compute them. If not, skip them and
  compute on the fly. Graceful degradation, not feature parity.

### Inflection Points

- **Month 1:** If the `AnalyticsRuntime` trait is clean and both
  variants share >70% of code (algorithms are trait methods, only
  the iteration pattern differs), maintenance cost is manageable.
  If they share <30% of code, you're maintaining two engines.

- **Month 6:** If 80% of users run OLAP-RAM (laptops, small VMs),
  the OLAP-Latency investment is justified only for benchmarks
  and enterprise. If 80% run OLAP-Latency (big servers), OLAP-RAM
  is justified only for dev/CI. Either way, both exist for
  different audiences.

---

## Cross-Timeline Analysis

| | A: OLAP-RAM Only | B: OLAP-Latency Only | C: Both Variants |
|---|---|---|---|
| **RAM (Medium, OLAP only)** | **200 MB - 1.4 GB** | 6-12 GB | 200 MB - 12 GB (auto) |
| **PageRank speed** | 15-60 sec | **1-5 sec** | 1-60 sec (auto) |
| **1-hop latency** | 10-200 μs | **3-10 μs** | 3-200 μs (auto) |
| **Runs on 16 GB machine** | **Yes** | No | **Yes (RAM mode)** |
| **Runs on 128 GB server** | Yes (underutilizes) | **Yes** | **Yes (Latency mode)** |
| **LOC (OLAP only)** | 8-12K | 12-18K | 20-30K |
| **Maintenance effort** | Low | Medium | Medium-High |
| **Benchmark headlines** | 3-13x over Neo4j | **20-200x over Neo4j** | **20-200x over Neo4j** |
| **Adoption breadth** | Wide (any machine) | Narrow (big servers) | **Widest** |

| | Upside | Downside | Reversibility | Regret risk |
|---|---|---|---|---|
| **A: RAM Only** | Runs anywhere. Simple. | "Only 3-13x? That's it?" | High — add Latency later | "We left 10x on the table" |
| **B: Latency Only** | Spectacular benchmarks | Excludes laptops/small VMs | Medium — hard to add RAM mode after designing for pinned | "Half our users can't run it" |
| **C: Both** | Best of both. Auto-scales. | More code to maintain | **High** — can drop either variant | "We over-engineered it" |

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline C: Both variants.**

It's the only path that delivers:
- 20-200x benchmarks (OLAP-Latency) for marketing and enterprise
- Runs on any machine (OLAP-RAM) for adoption and developer love
- Auto-selection means users don't need to know or care

### Which path is safest if things go badly?

**Timeline A: OLAP-RAM Only.**

If things go badly (team is small, deadlines tight), one OLAP
engine is enough. OLAP-RAM runs anywhere, delivers 3-13x over
Neo4j, and extends the existing codebase naturally. You can
always add OLAP-Latency later.

### What experiment would reduce uncertainty fastest?

**One experiment, half a day:**

```
Measure the gap between mmap and mlock for the existing
MmapWalkRuntime on a Medium-sized snapshot:

1. Build a 10M-node, 100M-edge snapshot (existing build code)
2. Run BFS + PageRank with current mmap runtime → measure time
3. Add mlock() to all Mmap regions → re-run same queries
4. Measure the speedup from eliminating page faults

If mlock speedup < 2x → OLAP-RAM is good enough. Skip Latency.
If mlock speedup > 5x → OLAP-Latency is essential for competitive benchmarks.
If mlock speedup 2-5x → Both variants are justified.

Expected: 3-10x for random access (traversal), 1.2-2x for
sequential (PageRank). This confirms both variants are needed.
```

---

## The Full Architecture — One Picture

```
┌──────────────────────────────────────────────────────────┐
│                     Bolt / Cypher                        │
│                 (single connection string)                │
├────────────────────────────┬─────────────────────────────┤
│                            │                             │
│         OLTP Engine         │       Query Router          │
│    (Rust Record Store)     │    ┌───────────────────┐    │
│                            │    │ Mutation → OLTP   │    │
│  · Full ACID transactions  │    │ GDS algo → OLAP   │    │
│  · Immediate read-write    │    │ Ad-hoc  → either  │    │
│  · WAL for durability      │    │                   │    │
│  · Same shape as Neo4j     │    │ OLAP auto-select: │    │
│                            │    │  RAM > 3×snap →   │    │
│         │                  │    │    Latency mode   │    │
│         │ WAL replay       │    │  else →           │    │
│         ▼                  │    │    RAM mode       │    │
│  ┌──────────────────────┐  │    └───────────────────┘    │
│  │   WAL → CSR Sync     │  │              │              │
│  │  (background thread) │  │              ▼              │
│  └──────┬───────────────┘  │  ┌─────────────────────────┤
│         │                  │  │                         │
│         ├──────────────────┼──┤  OLAP-RAM ("Backpacker") │
│         │                  │  │  · Compressed CSR       │
│         │                  │  │  · mmap + madvise       │
│         │                  │  │  · Streaming algorithms │
│         │                  │  │  · 200MB-1.4GB resident │
│         │                  │  │  · Runs on any machine  │
│         │                  │  ├─────────────────────────┤
│         │                  │  │                         │
│         └──────────────────┼──┤  OLAP-Latency("Dragster")│
│                            │  │  · Raw CSR, all pinned  │
│                            │  │  · Hash index, O(1)     │
│                            │  │  · rayon parallel       │
│                            │  │  · Pre-computed aux     │
│                            │  │  · 6-12GB resident      │
│                            │  │  · Needs 64GB+ machine  │
│                            │  │                         │
└────────────────────────────┴──┴─────────────────────────┘
```

**Three engines, one binary, one connection string.** The user
writes Cypher. The system decides where to run it and which OLAP
variant to use. This is the 1000 IQ version of your suggestion:
OLTP stays the same, OLAP splits into two flavors that auto-
select based on available hardware.

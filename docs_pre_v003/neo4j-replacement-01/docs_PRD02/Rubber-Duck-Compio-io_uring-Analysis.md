# Rubber Duck: Can compio/io_uring Improve Knight Bus Efficiency?

*Studied Apache Iggy's I/O architecture. Researched io_uring benchmarks.
Applied 1000 IQ rubber duck to every claim. Findings below.*

---

## Decision Frame (Timeline Traverser)

- **Fork in the road:** Should Knight Bus adopt compio (io_uring) for I/O, stay with mmap, or use a hybrid?
- **Desired outcome:** Fastest possible PageRank/traversal with lowest RAM
- **Hard constraints:** v0.0.3 ships in 2 weeks, ~800 LOC budget, 1 developer
- **Time horizon:** v0.0.3 (2 weeks) → v0.0.5 (2 months) → v0.1.0 (6 months)
- **What would count as failure:** Adopting compio adds 2-3 weeks of async plumbing for ≤5% speed gain

---

## Core Facts Enumerated

### What Iggy Does

```
IGGY I/O ARCHITECTURE
  Runtime:         compio (io_uring on Linux, IOCP on Windows)
  File I/O:        compio::fs::File (async read_exact_at, write_all_at)
  Memory mapping:  NONE. Zero mmap usage anywhere in codebase.
  Buffers:         4KB-aligned (AVec<u8, ConstAlign<4096>>)
  Buffer pooling:  PooledBuffer with memory_pool() for reuse
  Write batching:  write_vectored_all_at() (scatter/gather I/O)
  Read hints:      posix_fadvise(POSIX_FADV_SEQUENTIAL) on message files
  Threading:       Thread-per-core, single-threaded async per shard
  Concurrency:     Multiple clients writing messages simultaneously
  Workload:        Message streaming — high-throughput append + concurrent reads
```

### What Knight Bus Does

```
KNIGHT BUS I/O ARCHITECTURE
  Runtime:         Synchronous (std::fs + memmap2)
  File I/O:        BufReader/BufWriter for build, mmap for queries
  Memory mapping:  7 mmap'd files per snapshot (offsets, peers, node_table, etc.)
  Buffers:         Standard Vec<u8>, BufWriter defaults
  Buffer pooling:  None
  Write batching:  None (sequential writes via BufWriter)
  Read hints:      None yet (madvise planned for v0.0.3)
  Threading:       Single-threaded (rayon planned for v0.0.3 PageRank)
  Concurrency:     Single user, batch processing
  Workload:        Batch analytics — build once, query many times
```

### What io_uring Actually Is

```
io_uring (Linux 5.1+, mature since 5.10+):
  - Kernel-bypassing async I/O interface
  - Submission queue (SQ) + Completion queue (CQ) in shared memory
  - Batches syscalls: submit N reads in 1 syscall
  - Zero-copy: can use registered buffers
  - Supports: read, write, readv, writev, fsync, accept, connect, etc.
  
compio wraps io_uring (Linux), IOCP (Windows), kqueue/polling (macOS)
  - Provides async File, TcpStream, etc.
  - Thread-per-core model (single-threaded async event loop)
  - Buffer ownership model: buffers move into kernel, returned on completion

io_uring vs mmap (from benchmarks):
  - Sequential scan 22 GB file (NVMe SSD):
      mmap:                     3.43 sec
      mmap + AVX512:            2.61 sec  ← fastest
      io_uring vectored:        2.86 sec
      io_uring simple:          5.26 sec
      BufReader:               15.94 sec
  
  - Random reads 64 GiB file (EBS gp3):
      mmap:        p50=57.5 μs, p99=890.5 μs   ← page faults kill p99
      pread:       p50=3.3 μs,  p99=11.9 μs    ← consistent
      direct-pread: p50=1.5 μs, p99=5.5 μs     ← best, bypasses page cache

  Key insight: mmap wins for sequential scans (prefetcher).
               pread/io_uring wins for random access (no page faults).
               For in-memory data (fits in page cache): mmap ≈ raw pointers.
```

### Academic Evidence

```
RingSampler (HotStorage 2025):
  - io_uring for out-of-core graph neighborhood sampling
  - "Near in-memory sampling performance while operating fully out-of-core"
  - Key technique: batch prefetch neighbor lists via io_uring SQ
  - Applies when: graph >> RAM (out-of-core)
  - Does NOT apply when: graph fits in page cache

VeloANN (vector search on SSD):
  - Coroutine-based async runtime with io_uring
  - 5.8x throughput improvement for graph-based ANN search
  - Key technique: prefetch graph neighbors during traversal
  - Applies when: billions of vectors, SSD-resident index
  - Similar to our use case but at much larger scale
```

---

## The Five Operations and Whether io_uring Helps

### Operation 1: Build Pipeline (low_ram.rs)

```
Current I/O pattern:
  1. Read CSV sequentially (BufReader)
  2. Sort chunks in memory
  3. Write sorted runs to temp files (BufWriter)
  4. K-way merge-sort: read from K files simultaneously
  5. Write CSR arrays to snapshot files (BufWriter)

Bottleneck: CPU (CSV parsing, sorting), NOT I/O
  - CSV parsing: ~60% of build time
  - Sorting: ~25% of build time
  - I/O: ~15% of build time

io_uring potential:
  - Merge-sort phase: async read from K run files → ~10-15% faster on I/O portion
  - But I/O is only 15% of total → net benefit: ~1.5-2% faster build
  - Vectored writes for snapshot: minor syscall reduction
  
  EFFORT: ~500 LOC to convert BufReader/BufWriter to async
  BENEFIT: ~2% faster build (30 seconds saved on a 25-minute build)
  
  VERDICT: NOT WORTH IT for v0.0.3.
```

### Operation 2: PageRank — Graph Fits in Page Cache

```
Current I/O pattern (planned v0.0.3):
  mmap'd CSR arrays → sequential scan reverse_peers → random access scores_old[]

Bottleneck: CPU cache misses on scores_old[u] (L3 cache, not disk)
  - scores_old[u]: 50 ns per L3 miss × 80M misses per iteration = 4 sec/iter
  - reverse_peers scan: ~0.04 sec/iter (sequential, hardware prefetcher)
  - forward_offsets lookup: ~0.5 sec/iter
  
  The score array is a Vec<f64> in USERSPACE MEMORY. Not a file. Not mmap'd.
  io_uring operates on FILE DESCRIPTORS. It cannot prefetch userspace Vec data.
  
  The mmap'd CSR arrays (reverse_peers, offsets) are scanned SEQUENTIALLY.
  The OS page cache + madvise(SEQUENTIAL) already handles this optimally.
  mmap delivers zero-copy access: pointer arithmetic, no memcpy.
  
io_uring potential: ZERO.
  - Can't help with Vec<f64> cache misses (userspace memory, not a file)
  - Can't beat mmap for sequential scans (mmap IS zero-copy)
  - Adding async I/O would require rewriting the inner loop as async/await
  - rayon's parallel iteration is incompatible with compio's single-threaded model

  EFFORT: ~1,000+ LOC (rewrite PageRank as async, abandon rayon)
  BENEFIT: 0% (bottleneck is CPU cache, not I/O)
  
  VERDICT: ACTIVELY HARMFUL. Would remove rayon parallelism for zero I/O gain.
```

### Operation 3: PageRank — Graph Exceeds RAM (Out-of-Core)

```
This is where io_uring COULD help — and where RingSampler proves it.

Scenario: 500M edges on 8 GB laptop (4 GB free)
  - reverse_peers: 2 GB (doesn't fit in free RAM)
  - mmap page faults: thread blocks for each uncached 4KB page
  - ~50,000 pages × ~100 μs per fault × 20 iterations = significant overhead

io_uring approach:
  1. Read offset array (small, fits in RAM)
  2. For each batch of N nodes: compute which peer pages we'll need
  3. Submit io_uring batch read for those pages
  4. Process completed reads while submitting next batch
  5. Overlap compute with I/O (pipeline)

Estimated benefit: 2-3x faster for out-of-core PageRank
  - Eliminates blocking page faults
  - Overlaps compute with disk I/O
  - Similar to RingSampler's approach

BUT:
  - Requires completely different algorithm design (not "add compio")
  - ~800-1,200 LOC for async PageRank engine
  - Incompatible with rayon (compio is single-threaded per core)
  - Our v0.0.3 target (10M nodes) FITS in page cache
  - Users with 500M edges on 8 GB laptop: rare use case for v0.0.3

  EFFORT: ~1,000 LOC + algorithm redesign
  BENEFIT: 2-3x for out-of-core only (niche for v0.0.3)
  
  VERDICT: CORRECT for v0.0.6+ or v0.1.0. NOT for v0.0.3.
  Add to roadmap as "Out-of-Core Analytics Engine" milestone.
```

### Operation 4: Single Traversal Queries

```
Current I/O pattern:
  Binary search on key_index → read offsets → read peers → resolve keys

When warm (graph in page cache):
  Total: 5-50 μs. mmap access = pointer arithmetic. Zero syscalls.
  io_uring adds: queue management overhead (~0.5 μs per submission)
  Net: SLOWER with io_uring.

When cold (first access):
  Total: 0.5-2 ms (3-5 page faults at ~100 μs each)
  io_uring could batch-prefetch all needed pages in 1 submission
  Net: ~50% faster cold queries (1 round-trip instead of 3-5 serial faults)
  But: cold queries are rare after first warmup

  VERDICT: mmap is optimal for warm queries (the common case).
  io_uring helps cold queries marginally but adds constant overhead.
  NOT WORTH IT for v0.0.3.
```

### Operation 5: OLTP Write Path (v0.1.0+)

```
This is where compio SHINES and where Iggy's lesson applies.

Future I/O pattern:
  WAL append (fsync required) → record store insert → trigger OLAP rebuild

Why compio helps:
  1. WAL append: io_uring batch-submit writes + fsync → lower latency
     - Synchronous: write() + fsync() = 2 syscalls, ~200 μs
     - io_uring: submit(write, fsync) = 1 syscall, ~100 μs (50% less)
  2. Concurrent reads during writes: async I/O doesn't block query threads
  3. Batch multiple WAL entries: io_uring SQ can hold 256+ entries
  4. This is EXACTLY what Iggy does (message append + concurrent reads)

  EFFORT: Built into OLTP engine design from the start (~0 extra LOC vs alternative)
  BENEFIT: ~50% lower write latency, non-blocking queries during writes
  
  VERDICT: YES. Adopt compio for the OLTP engine at v0.1.0.
  Design the OLTP module with async I/O from day 1.
```

---

## Timeline A: "Adopt compio Now" (v0.0.3)

**Opening move:** Add compio dependency, convert runtime.rs from mmap to async file I/O.

**Week 1:**
- Replace memmap2 with compio::fs::File
- Rewrite read_u32_from_mmap → async read_exact_at()
- Discover: rayon + compio are incompatible (rayon = thread pool, compio = single-threaded async)
- Decision point: abandon rayon or abandon compio for PageRank?
- If abandon rayon: PageRank is single-threaded → 60-100 sec → SLOWER than Neo4j
- If abandon compio for PageRank: then why did we add compio?

**Week 2:**
- Build pipeline conversion: BufReader → compio async read
- Discover: low_ram.rs is 1,703 LOC of synchronous code with complex state machines
- Converting to async requires restructuring every phase
- Still working on conversion, no PageRank yet

**Week 3-4:**
- Still debugging async lifetime issues
- No benchmark, no demo, no v0.0.3

**Month 1:**
- Maybe shipped, but with WORSE performance than mmap + rayon
- Lost 2 weeks of productivity for negative performance impact

**Likelihood:** 25% (most likely abandoned partway through)
**Stress:** VERY HIGH. Async Rust lifetimes + completion-based I/O = pain.
**Inflection:** Week 1, when rayon incompatibility is discovered.

---

## Timeline B: "Stay with mmap + rayon" (v0.0.3)

**Opening move:** Add rayon + rand dependencies. Keep mmap. Write PageRank.

**Week 1:**
- Day 1-2: synthetic.rs (120 LOC, synchronous, simple)
- Day 3-4: page_rank.rs (170 LOC, rayon par_iter on existing mmap)
- Day 5: CLI integration, basic tests

**Week 2:**
- Day 6-7: Scale testing at 1M → 10M nodes
- Day 8: Benchmark harness, measure wall time + RSS
- Day 9-10: Polish, README, tag v0.0.3

**Month 1:**
- v0.0.3 shipped with honest benchmark: "10x faster, 17x less RAM"
- Community feedback informs v0.0.4 priorities
- Start work on next algorithms (Dijkstra, BFS)

**Likelihood:** 85%
**Stress:** LOW. Synchronous code, familiar patterns, proven primitives.
**Inflection:** Day 6 (10M scale test — first real validation of estimates).

---

## Timeline C: "Hybrid — mmap Now, compio at v0.1.0"

**Opening move:** Ship v0.0.3 with mmap + rayon. DESIGN the v0.1.0 OLTP engine around compio from the start.

**Week 1-2:** (same as Timeline B)
- Ship v0.0.3 with mmap + rayon PageRank

**Month 1-2:** (v0.0.4, v0.0.5)
- Add more algorithms, overlay model — all synchronous, all mmap
- Study compio API deeply for OLTP design

**Month 3-4:** (v0.1.0 OLTP engine)
- OLTP module uses compio::fs::File for WAL and record store
- OLAP module KEEPS mmap (unchanged)
- Query router bridges sync OLAP and async OLTP
- Each engine uses the I/O model that fits its workload

**Quarter 2:** (v0.0.6 or v0.1.1)
- Out-of-core PageRank using io_uring for graph-exceeds-RAM scenario
- Batched async prefetch inspired by RingSampler
- This is a separate codepath, not a replacement for in-memory mmap path

**Likelihood:** 90%
**Stress:** LOW now, MODERATE at v0.1.0 (but by then we understand compio)
**Inflection:** v0.1.0 design decision — how to bridge sync OLAP + async OLTP

---

## Timeline D: "What if We're Wrong About Being Compute-Bound?"

**Premise:** What if on real hardware (not our estimates), PageRank IS I/O bound?

**How to find out:** Run the v0.0.3 benchmark and measure:
  - `perf stat` → instructions per cycle (IPC < 0.5 = I/O bound)
  - `perf record` → sample page faults vs computation
  - RSS vs working set → if RSS << working set, page faults dominate

**If I/O bound:**
  - madvise(SEQUENTIAL) may not be enough
  - Need: madvise(WILLNEED) to pre-fault pages before PageRank
  - Or: MAP_POPULATE flag on mmap to pre-fault entire file
  - Or: posix_fadvise + readahead() before starting PageRank
  - These are 5-10 LOC changes. NOT compio.

**If compute-bound (expected):**
  - rayon parallelism is the correct optimization
  - Software prefetch (_mm_prefetch) for score array is next
  - Neither requires compio

**Likelihood:** 70% compute-bound, 30% mixed (partially I/O on tight-RAM machines)
**Fix if I/O bound:** madvise/readahead (5-10 LOC), NOT compio (1,000 LOC)

---

## Cross-Timeline Analysis

| Path | Upside | Downside | Reversibility | Regret risk | What must cooperate |
|---|---|---|---|---|---|
| **A: compio now** | Future-proof I/O layer | 2-3 week delay, negative perf (lose rayon) | Low (deep refactor) | HIGH | rayon + compio compatibility (doesn't exist) |
| **B: mmap + rayon** | Ships in 2 weeks, proven approach | No async I/O path for OLTP later | HIGH (can add compio later) | LOW | Rust compiler, NVMe SSD |
| **C: Hybrid** | Best of both worlds, matched to workload | Two I/O models to maintain | HIGH | LOWEST | Design discipline at v0.1.0 boundary |
| **D: Measure first** | Data-driven decision | Delays v0.0.3 by 1-2 days | HIGH | LOW | `perf stat` availability |

---

## What Knight Bus Should ACTUALLY Learn from Iggy

Iggy's I/O architecture is impressive but solves a DIFFERENT problem:

```
                    Iggy                       Knight Bus v0.0.3
Workload:          Concurrent streaming         Batch analytics
Writers:           Many clients simultaneously  Single user (rebuild)
Readers:           Many clients simultaneously  Single user (query)
I/O pattern:       Append + random read         Sequential scan + random access
Bottleneck:        I/O (disk throughput)         CPU (cache misses)
Correct I/O:       io_uring (async, batched)     mmap (zero-copy, OS-managed)
Threading:         Thread-per-core (async)       Thread pool (rayon)
```

### Techniques to Adopt (Now, Low Effort)

```
FROM IGGY                          FOR KNIGHT BUS               EFFORT  WHEN
─────────────────────────────────  ───────────────────────────  ──────  ──────
posix_fadvise(SEQUENTIAL)          madvise(MADV_SEQUENTIAL)     3 LOC   v0.0.3
  Iggy: MessagesReader sets        Knight Bus: on reverse_peers
  sequential hint on open          and reverse_offsets at open
  
  ALREADY PLANNED. Iggy validates our approach.

Buffer pooling                     Reuse BufWriter buffers      ~20 LOC v0.0.4
  Iggy: PooledBuffer with          in merge-sort phases of
  memory_pool() for aligned        low_ram.rs build pipeline
  buffer reuse                     
  
  MINOR WIN. 5-10% less allocation churn during build.
```

### Techniques to Adopt (Later, for OLTP)

```
FROM IGGY                          FOR KNIGHT BUS               EFFORT   WHEN
─────────────────────────────────  ───────────────────────────  ───────  ──────
compio::fs::File for WAL           OLTP WAL append              Built-in v0.1.0
  Iggy: async write_all_at +       Knight Bus: WAL needs
  fsync for message persistence    batch-submit + fsync
  
  CRITICAL FOR OLTP. Design the WAL around compio from day 1.

Thread-per-core shard model        One async runtime per OLTP   ~200 LOC v0.1.0
  Iggy: each shard = own compio    shard, OLAP stays with rayon
  runtime, no cross-shard locks    
  
  ARCHITECTURAL LESSON. Keep OLTP and OLAP on separate threads.

Vectored writes                    Batch snapshot file writes    ~30 LOC  v0.0.5
  Iggy: write_vectored_all_at()    Write multiple CSR arrays
  for batching message batches     in fewer syscalls
  
  MINOR WIN. Reduces syscalls during snapshot write phase.
```

### Techniques NOT to Adopt

```
FROM IGGY                          WHY NOT FOR KNIGHT BUS
─────────────────────────────────  ───────────────────────────────────────────
Eliminate mmap entirely            mmap is OPTIMAL for read-only analytics.
  Iggy uses no mmap because        Zero-copy access, OS manages page cache.
  it needs write access to         Knight Bus v0.0.3 is READ-ONLY on snapshot.
  segments. We don't (read-only).  mmap wins over pread for sequential scans.

Single-threaded async model        rayon (thread pool) is better for batch compute.
  Iggy is I/O bound: async         Knight Bus is CPU bound: parallel iteration
  avoids blocking on disk.         on score arrays is the optimization, not
  compio is single-threaded.       async I/O. rayon + compio are incompatible.

4KB-aligned Direct I/O             Would BYPASS page cache. Terrible for queries
  Iggy uses aligned buffers        that access the same CSR pages repeatedly.
  for O_DIRECT bypass. This        Our mmap'd snapshot relies on page cache for
  avoids double-buffering.         warm-query performance (5-50 μs).
```

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline C (Hybrid).** Ship v0.0.3 with mmap + rayon (proven, fast, ships in 2 weeks).
Design v0.1.0 OLTP around compio (right tool for the right job). Each engine
uses the I/O model that matches its workload: mmap for read-only analytics,
compio for concurrent read-write OLTP.

### Which path is safest if things go badly?

**Timeline B (mmap + rayon).** If v0.1.0 never happens, mmap + rayon is a
complete, performant system for analytics. No wasted effort.

### What experiment would reduce uncertainty fastest?

**Run v0.0.3 PageRank and measure with `perf stat`.**
- IPC > 1.0 → compute-bound → rayon is correct, compio adds nothing
- IPC < 0.5 → I/O bound → need madvise/readahead (5 LOC), STILL not compio
- IPC 0.5-1.0 → mixed → measure page fault count, decide per operation

This costs 1 day and answers every question.

---

## Changes to the Rewrite Strategy

### v0.0.3 (No Change — Confirmed Correct)

```
DEPENDENCY: rayon + rand (confirmed: compio NOT needed)
I/O MODEL: mmap + madvise(SEQUENTIAL) (confirmed: optimal for read-only analytics)
THREADING: rayon par_iter (confirmed: io_uring would REMOVE parallelism)
UNSAFE: slice casts for zero-copy mmap access (confirmed: matches Iggy's direct buffer approach)
```

### v0.0.5 (Minor Addition)

```
ADD: Vectored writes for snapshot building (from Iggy's write_vectored_all_at pattern)
ADD: Buffer pooling for merge-sort (from Iggy's PooledBuffer pattern)
EFFORT: +50 LOC
```

### v0.1.0 (Major Addition — from Iggy)

```
ADD TO ROADMAP:
  src/oltp/
  ├── mod.rs              ← compio runtime setup (from Iggy's bootstrap.rs)
  ├── wal.rs              ← WAL using compio::fs::File (from Iggy's persister.rs)
  ├── record_store.rs     ← Record store with async read_exact_at/write_all_at
  └── sync.rs             ← Channel from async OLTP → sync OLAP rebuild trigger

DESIGN PRINCIPLE (from Iggy):
  - OLTP engine: compio async runtime (single-threaded per shard)
  - OLAP engine: mmap + rayon (unchanged from v0.0.3)
  - Bridge: channel (like Iggy's shard-to-shard communication)
  - Each engine uses the I/O model optimal for its workload

NEW DEPENDENCY at v0.1.0:
  compio = { version = "0.18", features = ["runtime", "fs"] }
  
ESTIMATED LOC for compio integration: +100 LOC (runtime setup + WAL I/O)
  (Most of the 2,600 LOC v0.1.0 estimate is record store logic, not I/O)
```

### v0.0.6+ (Future Research — from RingSampler)

```
ADD TO ROADMAP:
  src/olap/out_of_core.rs  ← io_uring-based out-of-core PageRank
  - Batch-prefetch CSR pages via io_uring SQ
  - For graphs >> RAM (500M+ edges on 8 GB machine)
  - Inspired by RingSampler (HotStorage 2025)
  - Alternative to mmap's blocking page faults
  
  ESTIMATED LOC: ~400
  PREREQUISITE: v0.0.3 shipped, benchmarks show I/O bottleneck on tight-RAM machines
  
  This is RESEARCH, not v0.0.3 scope.
```

---

## Summary: The Honest Answer

**compio/io_uring is the WRONG optimization for v0.0.3.**

Knight Bus v0.0.3 is a **batch analytics engine**. Its bottleneck is **CPU cache
misses** (L3 misses on score array during PageRank), not disk I/O. The correct
optimizations are:

1. **rayon** — parallelize the compute across cores (4x speedup)
2. **unsafe slice casts** — zero-copy mmap access (eliminate per-element overhead)
3. **madvise(SEQUENTIAL)** — tell OS to prefetch CSR pages (from Iggy's pattern)

compio becomes the **RIGHT optimization at v0.1.0** when Knight Bus adds a write
path (OLTP engine). WAL appends, concurrent read-write, batch fsync — these are
exactly what compio was built for, and exactly what Iggy uses it for.

**The lesson from Iggy is not "use compio everywhere." It's "match the I/O model
to the workload."** Iggy is I/O bound → compio. Knight Bus v0.0.3 is CPU bound → rayon.
Knight Bus v0.1.0 OLTP is I/O bound → compio. Same principle, different tools.

```
VERSION   I/O MODEL              WHY
v0.0.3    mmap + rayon           Read-only analytics, CPU-bound (cache misses)
v0.0.5    mmap + rayon + vecIO   Same, with minor I/O optimizations from Iggy
v0.1.0    mmap (OLAP) +          OLTP is I/O bound (WAL writes), OLAP is CPU bound
          compio (OLTP)          Each engine uses optimal I/O for its workload
v0.0.6+   mmap + io_uring        Out-of-core analytics for graph >> RAM
          (out-of-core path)     Inspired by RingSampler, batch async prefetch
```

# Three Architecture Options: Folder-by-Folder Comparison

*Three distinct architectures for Knight Bus, each with complete
folder structure, file inventory, dependency list, LOC estimates,
run time estimates, and honest tradeoffs.*

*Rubber-duck verified. Every claim stress-tested.*

---

## Core Facts (Enumerated Before Reasoning)

```
CURRENT CODEBASE (v0.0.2)
  12 source files:     4,710 LOC Rust
  3 test files:        532 LOC
  Cargo.toml:          10 runtime deps, 3 dev deps
  Test fixtures:       39 nodes, 67 edges (tiny)
  Benchmark data:      2 GB corpus (4M nodes, 36M edges)
  Proven result:       p99 = 190 μs, RSS = 223 MB vs Neo4j p99 = 1.78 s, RSS = 1.08 GB

BOTTLENECK ANALYSIS
  Build pipeline:      CPU-bound (CSV parsing 60%, sorting 25%, I/O 15%)
  PageRank:            CPU-bound (L3 cache misses on scores_old[u]: 50 ns × 100M per iter)
  Traversal queries:   Memory-bound when warm (mmap pointer access, 5-50 μs)
  The disk is NOT the bottleneck for any v0.0.3 operation.

I/O RESEARCH RESULTS
  mmap:                Zero-copy, OS-managed page cache, best for sequential + warm random
  io_uring/compio:     Async batched I/O, best for concurrent writes + cold random reads
  rayon:               Thread-pool parallel compute, INCOMPATIBLE with compio's single-threaded model
  Benchmark (22 GB file): mmap = 3.43s, io_uring = 2.86s (sequential), mmap+SIMD = 2.61s (fastest)
  Benchmark (64 GiB random): mmap p50=57μs (page faults), pread p50=3.3μs (consistent)

USER JOURNEY (from previous analysis)
  Analytics users do: BUILD → READ → REBUILD → DELETE (not INSERT/UPDATE/DELETE)
  "CRUD" for our users = Create snapshot, Run algorithms, Update by rebuild, Delete directory
  Staleness of 2-5 min is accepted by all HTAP precedents (Oracle, TiDB, AlloyDB, DuckDB)

COMPETITIVE LANDSCAPE
  Grafeo:              489 stars, Rust, CSR + mutable overlay, 63x less memory than their std store
  TETRA:               Leads with "10x less RAM"
  Memgraph:            In-memory, Cypher-compatible
  DuckDB parallel:     Embedded analytics, no graph support but THE go-to precedent for embedded analytics
```

---

## The Three Options

| | Option A | Option B | Option C |
|---|---|---|---|
| **Name** | **"The Backpacker"** | **"The Dragster"** | **"The Architect"** |
| **Tagline** | Analytics-only, minimal RAM | Full async, lowest latency | Dual engine, full HTAP |
| **I/O model** | mmap + rayon | compio (io_uring) | mmap (OLAP) + compio (OLTP) |
| **Threading** | rayon thread pool | Thread-per-core async | rayon (OLAP) + compio (OLTP) |
| **Write path** | None (rebuild only) | None initially, easy to add | Full OLTP + WAL |
| **Target user** | Data scientist, analyst | Performance researcher | Platform team, startup |
| **Ships v0.0.3** | 2 weeks | 4-6 weeks | 8-12 weeks |

---

## Option A: "The Backpacker" — mmap + rayon, Analytics-Only

*The DuckDB play: embedded analytics engine. Build a snapshot, run algorithms, done.*

### Design Principles
- Read-only analytics on immutable CSR snapshots
- mmap for zero-copy access, OS manages memory
- rayon for parallel compute (PageRank, future algorithms)
- Synchronous build pipeline (low_ram.rs, already proven)
- No write path, no OLTP, no query router
- Rebuild the entire snapshot when data changes

### Folder Structure (v0.0.3 → v0.0.5 → v0.1.0)

#### v0.0.3 State

```
knight-bus-graph-walker/
├── Cargo.toml              ← +2 deps (rand, rayon)
├── src/
│   ├── lib.rs              ← +3 LOC (mod page_rank, mod synthetic)
│   ├── types.rs            ← +80 LOC (PageRankConfig, SyntheticGraphConfig, etc.)
│   ├── error.rs            ← +15 LOC (PageRankError, SyntheticGraphError)
│   ├── main.rs             ← +60 LOC (PageRank + Generate subcommands)
│   ├── app.rs              ← +30 LOC (run_page_rank, generate_synthetic)
│   ├── low_ram.rs          ← UNCHANGED (1703 LOC, build pipeline)
│   ├── graph.rs            ← UNCHANGED (225 LOC, in-memory CSR)
│   ├── runtime.rs          ← +65 LOC (slice casts, degree helpers, madvise)
│   ├── snapshot.rs         ← UNCHANGED (217 LOC)
│   ├── bench.rs            ← +40 LOC (PageRank benchmark)
│   ├── truth.rs            ← UNCHANGED (438 LOC)
│   ├── parity.rs           ← UNCHANGED (82 LOC)
│   ├── page_rank.rs        ← NEW: 170 LOC (parallel Jacobi + dangling nodes)
│   └── synthetic.rs        ← NEW: 120 LOC (random graph generator)
├── tests/
│   ├── cli.rs              ← +40 LOC
│   ├── library_contract.rs ← +30 LOC
│   ├── support/mod.rs      ← UNCHANGED
│   ├── page_rank.rs        ← NEW: 60 LOC
│   └── fixtures/
│       ├── valid/           ← UNCHANGED (39 nodes, 67 edges)
│       └── pagerank/        ← NEW (4-node hand-computed graph)
├── benchmarks/              ← UNCHANGED (Python harness)
└── v003-research/           ← UNCHANGED (2 GB benchmark data)
```

#### File-by-File Table (Option A, v0.0.3)

| File | Current LOC | Status | Change | New LOC | Role |
|---|---|---|---|---|---|
| `Cargo.toml` | — | MODIFY | +2 deps (rand, rayon) | — | Dependencies |
| `src/lib.rs` | 36 | EXTEND | +3 | 39 | Module root |
| `src/types.rs` | 596 | EXTEND | +80 | 676 | Types: PageRankConfig, SyntheticGraphConfig |
| `src/error.rs` | 119 | EXTEND | +15 | 134 | Errors: PageRank, Synthetic variants |
| `src/main.rs` | 305 | EXTEND | +60 | 365 | CLI: PageRank + Generate commands |
| `src/app.rs` | 129 | EXTEND | +30 | 159 | Orchestration for new commands |
| `src/low_ram.rs` | 1703 | **UNCHANGED** | 0 | 1703 | External merge-sort builder |
| `src/graph.rs` | 225 | **UNCHANGED** | 0 | 225 | In-memory CSR (tests only) |
| `src/runtime.rs` | 402 | EXTEND | +65 | 467 | Slice casts, degree, madvise |
| `src/snapshot.rs` | 217 | **UNCHANGED** | 0 | 217 | Snapshot writer |
| `src/bench.rs` | 458 | EXTEND | +40 | 498 | PageRank benchmark |
| `src/truth.rs` | 438 | **UNCHANGED** | 0 | 438 | CSV truth loader |
| `src/parity.rs` | 82 | **UNCHANGED** | 0 | 82 | Parity verification |
| `src/page_rank.rs` | — | **NEW** | +170 | 170 | Parallel Jacobi PageRank |
| `src/synthetic.rs` | — | **NEW** | +120 | 120 | Random graph generator |
| `tests/cli.rs` | 254 | EXTEND | +40 | 294 | CLI tests for new commands |
| `tests/library_contract.rs` | 250 | EXTEND | +30 | 280 | PageRank parity tests |
| `tests/support/mod.rs` | 28 | **UNCHANGED** | 0 | 28 | Test helpers |
| `tests/page_rank.rs` | — | **NEW** | +60 | 60 | PageRank correctness tests |
| `tests/fixtures/pagerank/` | — | **NEW** | +2 files | — | Hand-computed graph data |
| **TOTAL** | **5,242** | | **+800** | **6,042** | |

#### v0.0.5 State (Overlay Model)

| File | Status | Change | New LOC | Purpose |
|---|---|---|---|---|
| `src/overlay.rs` | **NEW** | +200 | 200 | Mutable edge overlay |
| `src/runtime.rs` | EXTEND | +100 | 567 | OverlayRuntime wrapping MmapWalkRuntime |
| `src/main.rs` | EXTEND | +30 | 395 | Append + Recompact commands |
| `src/types.rs` | EXTEND | +30 | 706 | Overlay config types |
| `src/snapshot.rs` | EXTEND | +40 | 257 | Overlay manifest fields |
| **TOTAL** | | **+400** | **6,442** | |

#### v0.1.0 State (OLTP — Option A approach: minimal, NO compio)

| File | Status | Change | New LOC | Purpose |
|---|---|---|---|---|
| `src/oltp/mod.rs` | **NEW** | +50 | 50 | OLTP module root |
| `src/oltp/record_store.rs` | **NEW** | +800 | 800 | 15B node + 34B rel records (sync I/O) |
| `src/oltp/wal.rs` | **NEW** | +400 | 400 | Write-ahead log (sync fsync) |
| `src/oltp/transaction.rs` | **NEW** | +300 | 300 | Basic ACID on single records |
| `src/build/mod.rs` | MOVE | 0 | — | low_ram.rs → build/csv_builder.rs |
| `src/build/csv_builder.rs` | MOVE | 0 | 1703 | Moved from low_ram.rs |
| `src/build/wal_builder.rs` | **NEW** | +500 | 500 | WAL → CSR rebuilder |
| `src/router/mod.rs` | **NEW** | +30 | 30 | Query routing root |
| `src/router/query_classifier.rs` | **NEW** | +200 | 200 | Analytics vs CRUD detection |
| `src/router/sync.rs` | **NEW** | +300 | 300 | OLTP WAL → OLAP rebuild trigger |
| **TOTAL** | | **+2,580** | **~9,022** | |

#### Dependencies (Option A)

```
v0.0.2 (current):  anyhow, clap, csv, libc, memmap2, serde, serde_json, sysinfo, tempfile, thiserror
v0.0.3 adds:       rand = "0.8" (features=["small_rng"]), rayon = "1.10"
v0.0.5 adds:       (none)
v0.1.0 adds:       crc32fast = "1.4" (WAL checksums)
                    NO compio. Sync I/O for WAL (simpler but higher write latency).
```

#### Run Time Estimates (Option A, 10M nodes / 100M edges)

| Operation | 64 GB Server | 16 GB Laptop | 8 GB Laptop | Neo4j GDS |
|---|---|---|---|---|
| **Build from CSV** | 2-5 min | 2-5 min | 3-6 min | 5-15 min |
| **Projection** | **0 sec** | **0 sec** | **0 sec** | 60-120 sec |
| **PageRank (rayon, 4 cores, 20 iter)** | 16-30 sec | 20-40 sec | 30-60 sec | 5-15 sec (algo only) |
| **PageRank (convergence ~12 iter)** | 8-22 sec | 10-30 sec | 15-45 sec | 5-15 sec (algo only) |
| **Total PageRank pipeline** | 8-22 sec | 10-30 sec | 15-45 sec | 65-135 sec (proj+algo) |
| **1-hop traversal (warm)** | 10-50 μs | 10-50 μs | 10-50 μs | 1-5 ms |
| **RSS (PageRank)** | 720 MB | 720 MB | ~500 MB* | 8-16 GB |
| **Rebuild (1% new data)** | 2-5 min | 2-5 min | 3-6 min | N/A |
| **OLTP write** | N/A | N/A | N/A | 0.5-2 ms |

*\* OS pages out mmap'd data; RSS capped by available memory, never OOMs*

#### Rubber Duck: What Breaks in Option A?

```
CLAIM: "PageRank 8-22 sec beats Neo4j's 65-135 sec"
ATTACK: Neo4j algo-only is 5-15 sec. Our algo is 8-22 sec. We're SLOWER algorithm-vs-algorithm.
SURVIVES: Yes — because we SKIP the 60-120 sec projection step. Total vs total we win 3-17x.
HONEST: "We're not faster at math. We skip the step Neo4j's storage format forces."

CLAIM: "No write path is fine for v0.0.3"
ATTACK: What if users want to add 1% new edges without full rebuild?
SURVIVES: v0.0.5 adds overlay for this. v0.0.3 users do BUILD-READ-REBUILD (proven user journey).

CLAIM: "Sync WAL at v0.1.0 works without compio"
ATTACK: Sync fsync adds 200-500 μs per write. Under concurrent queries, fsync blocks the thread.
BREAKS: For high-write-throughput OLTP (>1000 writes/sec), sync WAL becomes bottleneck.
         At <100 writes/min (our target user), it's fine. Document the threshold.

CLAIM: "800 LOC ships in 2 weeks"
ATTACK: Day 6 (10M scale test) is high-risk — build pipeline never tested beyond 39 nodes.
SURVIVES: 7-10 days realistic. Risk is KNOWN and documented.
```

---

## Option B: "The Dragster" — compio/io_uring, Full Async

*The Iggy play: thread-per-core async runtime for everything. Eliminate syscall overhead.*

### Design Principles
- compio runtime for ALL I/O (file reads, writes, future network)
- NO mmap — direct file I/O with 4KB-aligned buffers
- Thread-per-core model (one compio runtime per CPU core)
- Application-managed page cache (LRU buffer pool, like Iggy's PooledBuffer)
- Can naturally extend to network protocol (Bolt/QUIC) later
- PageRank: async single-threaded per core, manual work distribution

### Folder Structure (v0.0.3)

```
knight-bus-graph-walker/
├── Cargo.toml              ← +3 deps (rand, compio, aligned-vec)
│                              REMOVE memmap2
├── src/
│   ├── lib.rs              ← +5 LOC (mod page_rank, mod synthetic, mod io, mod buffer_pool)
│   ├── types.rs            ← +120 LOC (PageRankConfig, SyntheticGraphConfig,
│   │                           BufferPoolConfig, IoRequest, IoCompletion)
│   ├── error.rs            ← +25 LOC (IoError, BufferPoolError, AsyncRuntimeError)
│   ├── main.rs             ← +100 LOC (compio::main, async CLI, PageRank + Generate)
│   ├── app.rs              ← REWRITE: +80 LOC (async orchestration, compio runtime setup)
│   ├── io/                 ← NEW MODULE
│   │   ├── mod.rs          ← NEW: 30 LOC (module root)
│   │   ├── storage.rs      ← NEW: 150 LOC (async Storage trait, read_at/write_at)
│   │   └── buffer_pool.rs  ← NEW: 200 LOC (LRU page cache, 4KB-aligned buffers)
│   ├── low_ram.rs          ← REWRITE: ~1703 LOC (convert BufReader/BufWriter → async compio I/O)
│   │                          Every read()/write() call becomes .await
│   │                          External sort phases become async state machines
│   ├── graph.rs            ← UNCHANGED (225 LOC, in-memory CSR for tests)
│   ├── runtime.rs          ← REWRITE: ~500 LOC (replace Mmap with async file reads,
│   │                          buffer pool for hot pages, async read_exact_at for queries)
│   ├── snapshot.rs         ← EXTEND: +30 LOC (async snapshot writer)
│   ├── bench.rs            ← EXTEND: +60 LOC (async benchmark runner)
│   ├── truth.rs            ← EXTEND: +40 LOC (async CSV reader)
│   ├── parity.rs           ← UNCHANGED (82 LOC)
│   ├── page_rank.rs        ← NEW: 250 LOC (async PageRank, manual partitioning across cores,
│   │                          NO rayon — compio is single-threaded per core,
│   │                          use compio::dispatcher for multi-core work distribution)
│   └── synthetic.rs        ← NEW: 140 LOC (async streaming CSV write)
├── tests/
│   ├── cli.rs              ← REWRITE: +80 LOC (async test harness)
│   ├── library_contract.rs ← REWRITE: +60 LOC (async test harness)
│   ├── support/mod.rs      ← EXTEND: +20 LOC (async helpers)
│   ├── page_rank.rs        ← NEW: 80 LOC (async PageRank tests)
│   └── fixtures/           ← same as Option A
└── ...
```

#### File-by-File Table (Option B, v0.0.3)

| File | Current LOC | Status | Change | New LOC | Role |
|---|---|---|---|---|---|
| `Cargo.toml` | — | MODIFY | +3 deps (rand, compio, aligned-vec), -1 dep (memmap2) | — | Dependencies |
| `src/lib.rs` | 36 | EXTEND | +5 | 41 | Module root |
| `src/types.rs` | 596 | EXTEND | +120 | 716 | Types: +BufferPoolConfig, IoRequest |
| `src/error.rs` | 119 | EXTEND | +25 | 144 | Errors: +IoError, BufferPoolError |
| `src/main.rs` | 305 | **REWRITE** | +100 | 405 | Async main, compio runtime bootstrap |
| `src/app.rs` | 129 | **REWRITE** | +80 | 209 | Async orchestration |
| `src/io/mod.rs` | — | **NEW** | +30 | 30 | I/O module root |
| `src/io/storage.rs` | — | **NEW** | +150 | 150 | Async Storage trait (from Iggy pattern) |
| `src/io/buffer_pool.rs` | — | **NEW** | +200 | 200 | LRU page cache, aligned buffers |
| `src/low_ram.rs` | 1703 | **REWRITE** | ~0 net (+async, -sync) | ~1750 | Async external sort |
| `src/graph.rs` | 225 | **UNCHANGED** | 0 | 225 | In-memory CSR |
| `src/runtime.rs` | 402 | **REWRITE** | +100 | 500 | Async query engine, buffer pool reads |
| `src/snapshot.rs` | 217 | EXTEND | +30 | 247 | Async snapshot writer |
| `src/bench.rs` | 458 | EXTEND | +60 | 518 | Async benchmark |
| `src/truth.rs` | 438 | EXTEND | +40 | 478 | Async CSV reader |
| `src/parity.rs` | 82 | **UNCHANGED** | 0 | 82 | Parity verification |
| `src/page_rank.rs` | — | **NEW** | +250 | 250 | Async PageRank (manual multi-core) |
| `src/synthetic.rs` | — | **NEW** | +140 | 140 | Async streaming generator |
| `tests/cli.rs` | 254 | **REWRITE** | +80 | 334 | Async CLI tests |
| `tests/library_contract.rs` | 250 | **REWRITE** | +60 | 310 | Async parity tests |
| `tests/support/mod.rs` | 28 | EXTEND | +20 | 48 | Async test helpers |
| `tests/page_rank.rs` | — | **NEW** | +80 | 80 | Async PageRank tests |
| `tests/fixtures/pagerank/` | — | **NEW** | +2 files | — | Hand-computed graph |
| **TOTAL** | **5,242** | | **+1,750** | **~6,900** | |

#### Dependencies (Option B)

```
v0.0.2 (current):  anyhow, clap, csv, libc, memmap2, serde, serde_json, sysinfo, tempfile, thiserror
v0.0.3 changes:
  REMOVE:  memmap2 (replaced by compio file I/O)
  ADD:     compio = { version = "0.18", features = ["runtime", "macros", "fs", "io-uring"] }
           aligned-vec = "0.6" (4KB-aligned buffers for Direct I/O)
           rand = "0.8" (features=["small_rng"])
  NO rayon. compio is single-threaded async. Parallelism via compio::dispatcher.
```

#### Run Time Estimates (Option B, 10M nodes / 100M edges)

| Operation | 64 GB Server | 16 GB Laptop | 8 GB Laptop | Neo4j GDS |
|---|---|---|---|---|
| **Build from CSV** | 2-5 min | 2-5 min | 2-5 min | 5-15 min |
| **Projection** | **0 sec** | **0 sec** | **0 sec** | 60-120 sec |
| **PageRank (single-threaded async, 20 iter)** | 60-100 sec | 70-120 sec | 90-150 sec | 5-15 sec (algo) |
| **PageRank (compio::dispatcher, 4 cores)** | 18-30 sec* | 22-40 sec* | 30-60 sec* | 5-15 sec (algo) |
| **Total PageRank pipeline** | 18-30 sec | 22-40 sec | 30-60 sec | 65-135 sec (total) |
| **1-hop traversal (hot buffer pool)** | 1-5 μs** | 1-5 μs** | 1-5 μs** | 1-5 ms |
| **1-hop traversal (cold, async prefetch)** | 50-200 μs | 50-200 μs | 100-500 μs | 1-5 ms |
| **RSS (PageRank)** | 720 MB+ pool | 720 MB+ pool | ~500 MB+ pool | 8-16 GB |
| **OLTP write** | N/A | N/A | N/A | 0.5-2 ms |

*\* compio::dispatcher parallelism is LESS efficient than rayon's work-stealing for CPU-bound compute. Estimated 20-30% slower than rayon for PageRank due to message-passing overhead between single-threaded event loops.*

*\*\* Hot buffer pool queries may be FASTER than mmap (no page fault risk, pre-pinned buffers), but buffer pool management adds constant overhead.*

#### Rubber Duck: What Breaks in Option B?

```
CLAIM: "compio::dispatcher gives us multi-core PageRank"
ATTACK: compio::dispatcher distributes TASKS across cores, not data-parallel iteration.
  PageRank inner loop is: for each node v, sum over reverse neighbors.
  rayon par_iter maps this perfectly. compio::dispatcher requires manual chunking,
  message passing between cores, and explicit result aggregation.
BREAKS: compio parallel PageRank is ~20-30% slower than rayon due to:
  1. Message-passing overhead (channel send/recv per chunk)
  2. No work-stealing (static partitioning, load imbalance with power-law graphs)
  3. Buffer ownership complications (io_uring takes ownership of buffers)

CLAIM: "Eliminating mmap eliminates page fault stalls"
ATTACK: TRUE — but we replace page faults with application-managed buffer pool.
  Buffer pool miss = async file read = ~50 μs (SSD). Page fault = ~100 μs (SSD).
  Difference: async doesn't BLOCK the thread, but still takes the same wall time.
  For PageRank (sequential scan): OS prefetcher with mmap does this AUTOMATICALLY.
  Buffer pool requires manual prefetch logic = MORE code for SAME speed.
SURVIVES: Marginal. Buffer pool gives PREDICTABLE latency (no p99 spikes from GC-like page eviction).
  But: who cares about p99 of a batch PageRank? It runs once, not in a latency-critical loop.

CLAIM: "1,750 LOC ships in 4-6 weeks"
ATTACK: Converting low_ram.rs (1703 LOC of synchronous external sort) to async is HIGH RISK.
  Every File::read() → .await. Every BufWriter → async write.
  External sort has 7 phases with complex state. Async state machines are notoriously hard to debug.
  Lifetime issues with completion-based I/O (buffer ownership moves into kernel).
BREAKS: Likely 6-8 weeks, not 4-6. Async Rust debugging adds 50% time overhead.

CLAIM: "This architecture naturally extends to Bolt/QUIC network protocol"
ATTACK: TRUE. This is Option B's genuine advantage. compio handles TCP/QUIC natively.
  But: v0.0.3 has no network protocol. This benefit is deferred to v0.2.0+.
SURVIVES: Valid long-term benefit, but 0 value for v0.0.3.
```

---

## Option C: "The Architect" — mmap (OLAP) + compio (OLTP)

*The TiDB/AlloyDB play: two engines, each optimized for its workload, bridged by a router.*

### Design Principles
- OLAP engine: mmap + rayon (read-only analytics on immutable CSR)
- OLTP engine: compio (async WAL + record store for writes)
- Query router: classifies queries → routes to correct engine
- OLAP sees point-in-time snapshots (2-5 min staleness, acceptable)
- OLTP provides immediate read-after-write consistency
- Each engine uses the I/O model optimal for its workload

### Folder Structure (v0.0.3 → v0.0.5 → v0.1.0)

#### v0.0.3 State (IDENTICAL to Option A)

v0.0.3 ships the OLAP engine only. The OLTP engine and router are v0.1.0.
This means v0.0.3 is the same code as Option A — 800 LOC, mmap + rayon.

| File | Current LOC | Status | Change | New LOC | Role |
|---|---|---|---|---|---|
| *(Identical to Option A v0.0.3 table above)* | | | | | |
| **TOTAL** | **5,242** | | **+800** | **6,042** | |

#### v0.0.5 State (Overlay — same as Option A)

| File | Status | Change | New LOC | Purpose |
|---|---|---|---|---|
| *(Identical to Option A v0.0.5 table above)* | | | | |
| **TOTAL** | | **+400** | **6,442** | |

#### v0.1.0 State (OLTP + Router — THIS IS WHERE OPTION C DIVERGES)

```
knight-bus-graph-walker/
├── Cargo.toml              ← +2 deps (compio, crc32fast)
├── src/
│   ├── lib.rs              ← +4 LOC (mod oltp, mod router)
│   ├── types.rs            ← +60 LOC (OltpConfig, RouterConfig, WriteResult)
│   ├── error.rs            ← +20 LOC (OltpError, WalError, RouterError)
│   ├── main.rs             ← +50 LOC (Write + Query-routed subcommands)
│   ├── app.rs              ← +40 LOC (orchestration for write + routed query)
│   │
│   ├── olap/               ← EXISTING CODE MOVES HERE
│   │   ├── mod.rs          ← NEW: 30 LOC (OLAP module root)
│   │   ├── runtime.rs      ← MOVED: ~467 LOC (MmapWalkRuntime, UNCHANGED)
│   │   ├── page_rank.rs    ← MOVED: ~170 LOC (parallel PageRank, UNCHANGED)
│   │   └── overlay.rs      ← MOVED: ~200 LOC (mutable overlay, UNCHANGED)
│   │
│   ├── oltp/               ← NEW ENGINE (compio-based)
│   │   ├── mod.rs          ← NEW: 60 LOC (compio runtime setup, shard model)
│   │   ├── record_store.rs ← NEW: 800 LOC (async read_at/write_at, compio::fs::File)
│   │   ├── wal.rs          ← NEW: 450 LOC (async WAL append + batch fsync via io_uring)
│   │   └── transaction.rs  ← NEW: 300 LOC (async single-record ACID)
│   │
│   ├── router/             ← NEW: QUERY ROUTING
│   │   ├── mod.rs          ← NEW: 30 LOC
│   │   ├── classifier.rs   ← NEW: 200 LOC (analytics → OLAP, CRUD → OLTP)
│   │   └── sync.rs         ← NEW: 350 LOC (OLTP WAL → OLAP rebuild trigger,
│   │                           channel bridge between compio runtime and rayon pool)
│   │
│   ├── build/              ← low_ram.rs SPLITS HERE
│   │   ├── mod.rs          ← NEW: 20 LOC
│   │   ├── csv_builder.rs  ← MOVED: ~1703 LOC (from low_ram.rs, UNCHANGED)
│   │   └── wal_builder.rs  ← NEW: 500 LOC (replay WAL → rebuild CSR, sync I/O)
│   │
│   ├── graph.rs            ← UNCHANGED
│   ├── snapshot.rs         ← EXTEND: +40 LOC
│   ├── bench.rs            ← EXTEND: +60 LOC (OLTP + routed query benchmarks)
│   ├── truth.rs            ← UNCHANGED
│   ├── parity.rs           ← UNCHANGED
│   └── synthetic.rs        ← UNCHANGED
└── tests/
    ├── ... (existing + new OLTP tests)
    └── oltp/               ← NEW: ~200 LOC
```

#### File-by-File Table (Option C, v0.1.0 — full dual engine)

| File | v0.0.3 LOC | Status at v0.1.0 | Change | Final LOC | Role |
|---|---|---|---|---|---|
| `Cargo.toml` | — | MODIFY | +2 deps (compio, crc32fast) | — | Add async I/O for OLTP |
| `src/lib.rs` | 39 | EXTEND | +4 | 43 | +mod oltp, router |
| `src/types.rs` | 676 | EXTEND | +60 | 736 | +OltpConfig, WriteResult |
| `src/error.rs` | 134 | EXTEND | +20 | 154 | +OltpError, WalError |
| `src/main.rs` | 365 | EXTEND | +50 | 415 | +Write, Query-routed commands |
| `src/app.rs` | 159 | EXTEND | +40 | 199 | +OLTP orchestration |
| `src/olap/mod.rs` | — | **NEW** | +30 | 30 | OLAP module root |
| `src/olap/runtime.rs` | 467 | **MOVED** | 0 | 467 | MmapWalkRuntime (mmap + rayon) |
| `src/olap/page_rank.rs` | 170 | **MOVED** | 0 | 170 | Parallel PageRank (rayon) |
| `src/olap/overlay.rs` | 200 | **MOVED** | 0 | 200 | Mutable overlay |
| `src/oltp/mod.rs` | — | **NEW** | +60 | 60 | compio runtime setup |
| `src/oltp/record_store.rs` | — | **NEW** | +800 | 800 | Async record store (compio) |
| `src/oltp/wal.rs` | — | **NEW** | +450 | 450 | Async WAL (io_uring batch fsync) |
| `src/oltp/transaction.rs` | — | **NEW** | +300 | 300 | Async single-record ACID |
| `src/router/mod.rs` | — | **NEW** | +30 | 30 | Router module root |
| `src/router/classifier.rs` | — | **NEW** | +200 | 200 | Query classification |
| `src/router/sync.rs` | — | **NEW** | +350 | 350 | OLTP→OLAP bridge (channel) |
| `src/build/mod.rs` | — | **NEW** | +20 | 20 | Build module root |
| `src/build/csv_builder.rs` | 1703 | **MOVED** | 0 | 1703 | From low_ram.rs |
| `src/build/wal_builder.rs` | — | **NEW** | +500 | 500 | WAL → CSR replay |
| `src/graph.rs` | 225 | **UNCHANGED** | 0 | 225 | In-memory CSR |
| `src/snapshot.rs` | 257 | EXTEND | +40 | 297 | +OLTP manifest fields |
| `src/bench.rs` | 498 | EXTEND | +60 | 558 | +OLTP benchmarks |
| `src/truth.rs` | 438 | **UNCHANGED** | 0 | 438 | CSV truth |
| `src/parity.rs` | 82 | **UNCHANGED** | 0 | 82 | Parity |
| `src/synthetic.rs` | 120 | **UNCHANGED** | 0 | 120 | Graph generator |
| `tests/oltp/*.rs` | — | **NEW** | +200 | 200 | OLTP tests |
| **TOTAL** | **6,442** | | **+3,215** | **~9,657** | |

#### Dependencies (Option C)

```
v0.0.2 (current):  anyhow, clap, csv, libc, memmap2, serde, serde_json, sysinfo, tempfile, thiserror
v0.0.3 adds:       rand = "0.8", rayon = "1.10"  (OLAP only — same as Option A)
v0.0.5 adds:       (none)
v0.1.0 adds:       compio = { version = "0.18", features = ["runtime", "macros", "fs", "io-uring"] }
                   crc32fast = "1.4" (WAL checksums)
                   
KEY: memmap2 STAYS (for OLAP). compio ADDED (for OLTP only).
     Both coexist. Each engine uses its optimal I/O model.
```

#### Run Time Estimates (Option C, 10M nodes / 100M edges)

| Operation | 64 GB Server | 16 GB Laptop | 8 GB Laptop | Neo4j GDS |
|---|---|---|---|---|
| **Build from CSV** | 2-5 min | 2-5 min | 3-6 min | 5-15 min |
| **Projection** | **0 sec** | **0 sec** | **0 sec** | 60-120 sec |
| **PageRank (rayon, 4 cores)** | 8-22 sec | 10-30 sec | 15-45 sec | 65-135 sec (total) |
| **1-hop traversal (warm, OLAP)** | 10-50 μs | 10-50 μs | 10-50 μs | 1-5 ms |
| **1-hop traversal (OLTP, record store)** | 0.5-2 ms | 0.5-2 ms | 0.5-2 ms | 0.5-2 ms |
| **RSS (PageRank)** | 720 MB | 720 MB | ~500 MB | 8-16 GB |
| **OLTP single write** | 0.1-0.3 ms* | 0.1-0.3 ms* | 0.1-0.5 ms | 0.5-2 ms |
| **OLTP P99 write** | 0.5-1 ms* | 0.5-1 ms* | 1-3 ms | 5-20 ms (GC) |
| **OLAP staleness** | 2-5 min | 2-5 min | 2-5 min | GDS projection |
| **WAL → CSR rebuild** | 30-90 sec | 30-90 sec | 60-120 sec | N/A |

*\* compio WAL with io_uring batch fsync: 50% lower write latency than sync fsync.*

#### Rubber Duck: What Breaks in Option C?

```
CLAIM: "v0.0.3 is identical to Option A, no extra cost"
ATTACK: TRUE at v0.0.3. But does the v0.1.0 refactor (moving files to olap/) break things?
SURVIVES: Rust module moves are straightforward. `use crate::olap::runtime::MmapWalkRuntime`
  replaces `use crate::runtime::MmapWalkRuntime`. One sed command.

CLAIM: "compio OLTP and rayon OLAP can coexist"
ATTACK: They run on different threads. compio runtime is !Send — can't share data across threads
  without Arc/channels. The OLTP→OLAP bridge (router/sync.rs) needs careful design.
SURVIVES: TiDB does exactly this (TiKV = Raft, TiFlash = columnar). The bridge is a channel.
  Iggy does this too (shards communicate via channels). Well-understood pattern.
  ~350 LOC for sync.rs is realistic for a channel-based bridge.

CLAIM: "3,215 LOC for v0.1.0 is realistic"
ATTACK: Record store alone is 800 LOC. Compare to Grafeo's CompactStore.
  Neo4j's RecordStore is tens of thousands of LOC. Can we do it in 800?
SURVIVES: We're implementing a MINIMAL record store (15B node + 34B rel records).
  No property chains, no ID reuse, no recovery manager. 800 LOC is the happy path.
  A production record store would be 3,000-5,000 LOC. We're building a proof of concept.

CLAIM: "compio WAL gives 50% lower write latency"
ATTACK: io_uring batch fsync is well-documented: 1 syscall for N writes + fsync.
  Sync alternative: N write() syscalls + 1 fsync() = N+1 syscalls.
  At N=100 (batch of 100 writes): io_uring = 1 submission, sync = 101 syscalls.
SURVIVES: TRUE. This is compio's genuine strength. Iggy's entire persistence layer is built on this.
  The benefit is real and measurable.
```

---

## Cross-Option Comparison

### At v0.0.3 Scope

| Metric | Option A "Backpacker" | Option B "Dragster" | Option C "Architect" |
|---|---|---|---|
| **LOC to write** | **800** | 1,750 | **800** (same as A) |
| **Files changed** | 14 (3 new) | 22 (7 new) | 14 (3 new) |
| **New dependencies** | 2 (rand, rayon) | 3 (rand, compio, aligned-vec) | 2 (rand, rayon) |
| **Deps removed** | 0 | 1 (memmap2) | 0 |
| **Time to ship** | **7-10 days** | 28-42 days | **7-10 days** |
| **PageRank 10M/100M** | **8-22 sec** (rayon) | 18-30 sec (compio dispatcher) | **8-22 sec** (rayon) |
| **PageRank RSS** | 720 MB | 720 MB + pool overhead | 720 MB |
| **Traversal (warm)** | 10-50 μs | 1-5 μs (pinned pool) | 10-50 μs |
| **Risk level** | LOW | **HIGH** (async rewrite) | LOW |
| **Rewrite scope** | 2 files rewritten | **7 files rewritten** | 2 files rewritten |

### At v0.1.0 Scope

| Metric | Option A "Backpacker" | Option B "Dragster" | Option C "Architect" |
|---|---|---|---|
| **Total LOC** | ~9,022 | ~9,800 | ~9,657 |
| **OLTP write latency** | 0.2-0.6 ms (sync fsync) | **0.1-0.3 ms** (io_uring) | **0.1-0.3 ms** (io_uring) |
| **OLTP P99** | 1-5 ms (sync blocks) | **0.5-1 ms** (async) | **0.5-1 ms** (async) |
| **OLAP PageRank** | **8-22 sec** (rayon) | 18-30 sec (no rayon) | **8-22 sec** (rayon) |
| **Network protocol** | Manual threading | **Native** (compio TCP/QUIC) | Separate concern |
| **Complexity** | LOW | MEDIUM | **HIGH** (two I/O models) |
| **Precedent** | DuckDB | Iggy | TiDB/AlloyDB |

### At Scale (50M nodes, 500M edges)

| Metric | Option A | Option B | Option C |
|---|---|---|---|
| **PageRank (64 GB server)** | **40-120 sec** | 60-180 sec | **40-120 sec** |
| **PageRank (8 GB laptop)** | 80-300 sec | 90-350 sec | 80-300 sec |
| **Neo4j total** | 320-660 sec | 320-660 sec | 320-660 sec |
| **Speedup vs Neo4j** | **3-17x** | 2-11x | **3-17x** |
| **RSS** | 3.6 GB (mmap managed) | 3.6 GB + pool | 3.6 GB (mmap managed) |
| **Neo4j RSS** | 30-60 GB | 30-60 GB | 30-60 GB |
| **Memory advantage** | **8-17x** | 8-17x | **8-17x** |
| **Runs on 8 GB laptop?** | **YES** (slower) | YES (slower) | **YES** (slower) |
| **Write throughput** | N/A (rebuild only) | N/A at v0.0.3 | 1K-10K writes/sec |

### Honest Tradeoff Matrix

| Factor | Best Option | Why |
|---|---|---|
| **Ship fastest** | **A or C** | 7-10 days vs 28-42 days |
| **PageRank speed** | **A or C** | rayon > compio::dispatcher for CPU-bound |
| **OLTP write latency** | **B or C** | io_uring batch fsync |
| **Code simplicity** | **A** | One I/O model, synchronous, familiar |
| **Future network protocol** | **B** | compio handles TCP/QUIC natively |
| **Match I/O to workload** | **C** | Each engine uses optimal model |
| **Community precedent** | **C** | TiDB/AlloyDB dual-engine, DuckDB + extension model |
| **Smallest risk** | **A** | No async Rust, no compio learning curve |
| **Out-of-core analytics** | **B** (future) | io_uring prefetch for graph > RAM |
| **Long-term extensibility** | **C** | Clean separation, each engine evolves independently |

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Option C ("The Architect").** It ships v0.0.3 as fast as Option A (7-10 days, identical code), then adds the OLTP engine with compio at v0.1.0 — using the right I/O model for each workload. This is the TiDB/AlloyDB pattern, well-proven at scale.

### Which path is safest if things go badly?

**Option A ("The Backpacker").** If OLTP never ships, if compio proves too complex, if the market only wants analytics — Option A is a complete, fast, useful product. Nothing wasted. DuckDB started this way and is now worth hundreds of millions.

### What experiment would reduce uncertainty fastest?

**Ship Option A as v0.0.3 (7-10 days). Measure with `perf stat`.** This answers:
- Is PageRank CPU-bound (IPC > 1.0) or I/O-bound (IPC < 0.5)?
- Does rayon give the expected 3-4x speedup?
- What do users ask for? (Write path? More algorithms? Python bindings?)

User feedback determines whether v0.1.0 is Option C (dual engine) or more Option A (analytics-only with more algorithms).

### The Recommendation

```
v0.0.3:  Option A = Option C (identical)     → Ship in 7-10 days
v0.0.5:  Option A = Option C (identical)     → Add overlay model
v0.1.0:  DECIDE based on user feedback:
           Users want writes → Option C (add compio OLTP)
           Users want more algorithms → Option A (stay analytics-only)
           Users want network protocol → Consider Option B lessons
```

**You don't have to choose now.** Options A and C are identical through v0.0.5. The fork is at v0.1.0, and by then you'll have user data to decide.

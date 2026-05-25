# Faithful Rust Port of Neo4j — Full Analysis

*It's just a binary. Same Cypher, same Bolt, same drivers, same everything.
You change the binary, you keep your world.*

---

## The Shreyas Doshi Framing

The user journey is one sentence:

> "Replace the Neo4j binary. Nothing else changes."

No new query language to learn. No driver migration. No schema changes.
No application rewrites. Point your existing `bolt://` connection at the
Rust binary instead of the Java one. Done.

This is the lowest-friction adoption story in databases. The Bun playbook
proved it works: same architecture, same API surface, different language,
measurably better performance. Users don't adopt technologies — they adopt
outcomes. The outcome here is: **same queries, 5x faster, 5x less RAM,
zero GC pauses, 10MB binary.**

### Why This Wins Adoption

| Adoption factor | Faithful Rust port | New architecture |
|---|---|---|
| Switching cost | Zero | High (rewrite queries) |
| Learning curve | Zero | New API/query model |
| Risk | Low (same semantics) | High (new engine, new bugs) |
| Time to value | Immediate | Weeks of migration |
| Driver compatibility | 100% (same Bolt) | Must build new drivers |
| Community migration | "Just swap the binary" | "Rewrite your app" |

---

## Overall Speed Estimates: Java → Rust (Same Architecture)

| Metric | Java Neo4j | Rust Port | Gain | Why |
|---|---|---|---|---|
| Throughput (q/s) | baseline | 3-8x | | No GC, no JIT warmup, smaller objects |
| p99 latency | 50-200ms spikes | <5ms deterministic | **5-50x** | No stop-the-world GC |
| Average latency | baseline | 2-4x | | Less allocation, no boxing |
| Memory (RSS) | 2-8GB | 300MB-2GB | **3-10x less** | No JVM overhead, no object headers |
| Binary size | 50-200MB | 10-30MB | 5-10x | No JVM, no class files |
| Startup | 10-30s | <1s | 10-30x | No class loading, no JIT |
| Traversal | baseline | 1.5-3x | | Same pointer-chasing, better cache |

---

## Folder-by-Folder Porting Plan

Neo4j has 68 modules under `community/`. Not all need porting. Below is
every module, grouped by priority tier, with LOC, what it does, the Rust
approach, expected speedup, and TDD strategy.

### Tier 1 — Core Engine (MUST PORT)

These modules are the database engine. Without them, nothing works.

#### `kernel` — 83,297 LOC (Java)

**What it does:** Core database engine. Transactions, cursors, operations,
recovery, database lifecycle management.

**Rust approach:**
- `KernelTransaction` → Rust struct with lifetime-scoped resources (`Drop`
  handles rollback automatically — this alone eliminates Neo4j's biggest
  bug class: "forgot to rollback on error path")
- Cursors → zero-copy iterators over page-cached records
- Operations → trait-based dispatch, monomorphized at compile time
- Recovery → same WAL replay logic, but with `Result<>` error propagation

**Speed gain:** 2-4x (transaction overhead drops, cursor iteration is tighter)

**TDD approach:**
1. STUB: Define `KernelTransaction` trait with `commit()`, `rollback()`, `cursor()`
2. RED: Test transaction isolation, crash recovery, cursor lifecycle
3. GREEN: Implement against in-memory store first
4. REFACTOR: Add page-cache-backed implementation

---

#### `kernel-api` — 18,542 LOC (Java)

**What it does:** Public SPI — `StorageEngine`, `StorageReader`,
`CommandCreationContext`. The contract between kernel and storage.

**Rust approach:**
- Java interfaces → Rust traits
- `StorageEngine` trait with associated types for readers/writers
- `StorageReader` → lifetime-bounded borrow from engine (Rust enforces
  "reader cannot outlive the transaction" at compile time — Java can't)

**Speed gain:** 1.5-2x (trait dispatch is cheaper than Java interface dispatch)

**TDD approach:**
1. STUB: Define `StorageEngine`, `StorageReader`, `StorageWriter` traits
2. RED: Contract tests — any implementation must pass the same test suite
3. GREEN: Implement with record-storage-engine
4. This becomes the integration test harness for all storage backends

---

#### `record-storage-engine` — 69,646 LOC (Java)

**What it does:** Fixed-size record stores for nodes, relationships,
properties. NeoStores, batch import, record formats.

**Rust approach:**
- Records → `#[repr(C)]` structs with exact byte layouts
- NeoStores → memory-mapped files with typed accessors
- Linked-list traversal → same pointer chasing but with `u64` offsets,
  no Java object wrapping
- Batch import → parallel with `rayon` or manual thread pool
- RecordFormat → compile-time generic over record size

**Speed gain:** 1.5-3x
- Java: read record → deserialize into heap object → access fields → GC later
- Rust: read record → zero-copy reference to mmap'd bytes → access fields directly
- Each Java record object has 12-16 byte header. Node record is 15 bytes
  of data + 16 bytes of overhead = 52% waste. Rust: 15 bytes is 15 bytes.

**TDD approach:**
1. STUB: `NodeRecord`, `RelRecord`, `PropRecord` structs
2. RED: Round-trip tests — write record, read back, verify fields
3. GREEN: Implement with raw file I/O first
4. REFACTOR: Add mmap, add page cache layer
5. RED: Batch import correctness — import CSV, verify all records
6. GREEN: Implement parallel batch importer

---

#### `io` — 14,241 LOC (Java)

**What it does:** Page cache (Muninn), `PageCursor`, file I/O abstraction.
Every record access goes through this layer.

**Rust approach:**
- MuninnPageCache → either direct `mmap` (simplest, OS handles eviction)
  or custom page cache with `unsafe` page pinning
- PageCursor → zero-cost wrapper around `&[u8]` slice into mapped page
- File abstraction → `std::fs::File` + optional `io_uring` for async I/O
- Eviction → if custom: clock sweep over `AtomicU8` reference bits

**Speed gain:** 2-4x
- Java Muninn: PageCursor allocation, bounds checking via method calls,
  managed eviction, fence synchronization
- Rust mmap: OS page cache, zero userspace overhead, no object allocation

**TDD approach:**
1. STUB: `PageCache` trait, `PageCursor` type
2. RED: Test page read/write, concurrent access, eviction under pressure
3. GREEN: Start with mmap (simplest, fastest)
4. REFACTOR: Add optional managed page cache for fine-grained control

---

#### `index` — 13,402 LOC (Java)

**What it does:** `GBPTree` — B+tree for schema indexes, counts store,
ID tracking. Page-cache-backed, crash-safe.

**Rust approach:**
- GBPTree → generic B+tree over `PageCache` trait
- Keys/values → `Copy` types, inline in page (no heap allocation per entry)
- Concurrency → generation-based optimistic reads (same as Java, but with
  `AtomicU64` instead of `java.util.concurrent.atomic`)
- Crash safety → checkpoint integration same as Java

**Speed gain:** 2-5x
- Java: each key/value lookup allocates a cursor object, boxes primitives
- Rust: key comparison is inline, no allocation, generics monomorphize

**TDD approach:**
1. STUB: `BTree<K, V>` with `insert`, `get`, `scan`, `checkpoint`
2. RED: Correctness — insert N keys, verify all retrievable
3. RED: Crash recovery — simulate crash mid-checkpoint, verify recovery
4. GREEN: Implement page-level B+tree operations
5. RED: Concurrent readers + single writer stress test
6. GREEN: Add generation-based optimistic locking

---

#### `wal` — 8,888 LOC (Java)

**What it does:** Write-ahead log for durability. Log entries, checkpointing,
recovery, log rotation.

**Rust approach:**
- Log entries → `#[repr(C)]` structs, written with `write_all`
- Checkpointing → `fsync` + atomic file rename
- Recovery → forward scan of WAL, replay commands
- Rotation → same file-size-based rotation, but with `io_uring` for
  non-blocking writes if available

**Speed gain:** 2-4x
- Java: FileChannel + ByteBuffer allocation per write batch
- Rust: direct `write` syscall, zero allocation for log entries,
  optional `io_uring` for async durability

**TDD approach:**
1. STUB: `WriteAheadLog` with `append`, `checkpoint`, `recover`
2. RED: Write entries, crash, recover, verify all committed entries present
3. GREEN: Implement with synchronous file I/O
4. REFACTOR: Add batched writes, `io_uring` backend

---

#### `lock` — 5,522 LOC (Java)

**What it does:** Lock manager for transaction concurrency. Read/write
locks on nodes and relationships.

**Rust approach:**
- Lock table → `DashMap` or custom striped lock table
- Deadlock detection → same wait-for graph, but with `parking_lot` mutexes
- Lock modes → Rust enum with `#[non_exhaustive]`

**Speed gain:** 2-3x
- `parking_lot::RwLock` is significantly faster than Java's
  `ReentrantReadWriteLock` (no object monitor overhead)

**TDD approach:**
1. STUB: `LockManager` with `acquire_shared`, `acquire_exclusive`, `release`
2. RED: Concurrent locking stress test, deadlock detection
3. GREEN: Implement with `DashMap<EntityId, RwLock>`
4. RED: Deadlock timeout test

---

#### `storage-engine-util` — 7,898 LOC (Java)

**What it does:** Counts store (`GBPTreeGenericCountsStore`), shared
storage utilities. Graph statistics for query planner.

**Rust approach:**
- Counts store → reuse GBPTree implementation from `index`
- Degree cache → `Vec<AtomicU32>` indexed by node ID

**Speed gain:** 2-3x (reuses GBPTree gains)

**TDD approach:**
1. RED: Store count, retrieve count, verify
2. GREEN: Implement over GBPTree

---

### Tier 2 — Query Engine (PORT FOR FULL COMPATIBILITY)

#### `cypher` — 701,841 LOC (512K Scala + 155K Java)

**What it does:** The entire Cypher engine — parser, semantic analysis,
planner, runtime, result materialization.

**This is the elephant.** 44% of Neo4j's codebase. 512K lines of Scala.

**Rust approach — three sub-strategies:**

**a) Parser (ANTLR grammar → Rust parser)**
- Neo4j uses ANTLR-generated parser (~5K LOC grammar)
- Rust: use `pest`, `nom`, or hand-written recursive descent
- Emit Rust AST enums (no heap allocation per node — use arena or stack)
- Existing crate `decypher` already does this for openCypher
- **Estimated Rust LOC: ~8-15K**

**b) Planner (Scala IDP solver → Rust)**
- This is the most complex piece: cost-based optimization, join ordering,
  eager barrier analysis, cardinality estimation
- Faithful port: translate the IDP solver algorithm to Rust
- Key win: Scala creates thousands of intermediate plan objects per query.
  Rust can use arena allocation — allocate all plan nodes in a bump
  allocator, free everything at once when planning is done.
- **Estimated Rust LOC: ~30-50K** (Scala is verbose; Rust is denser)

**c) Runtime (Interpreted/slotted pipes → Rust iterators)**
- Neo4j's runtime creates a pipeline of operator objects (Expand, Filter,
  Projection, etc.) that pull rows through.
- Rust: same pull-based pipeline, but operators are trait objects or
  enum-dispatched. No heap allocation per row — rows are fixed-size
  slot arrays on the stack.
- Monomorphization: for the top 20 query shapes, generate specialized
  code paths (no virtual dispatch in hot loop).
- **Estimated Rust LOC: ~40-60K**

**Total Cypher in Rust: ~80-125K LOC** (vs 701K in Scala/Java — Rust is
denser, and Scala's implicit/trait overhead inflates LOC significantly)

**Speed gain:** 3-10x
- Parser: 5-10x (no ANTLR runtime, no AST object allocation)
- Planner: 2-5x (same algorithm, arena allocation, no GC pressure)
- Runtime: 3-8x (monomorphized operators, no boxing, stack-allocated rows)

**TDD approach:**
1. STUB: Define AST types, PlanNode enum, Operator trait
2. RED: Parse canonical Cypher queries, verify AST
3. GREEN: Implement parser (or adopt `decypher` / `ocg`)
4. RED: Plan simple queries, verify correct logical plans
5. GREEN: Implement core planner (expand, filter, projection)
6. RED: Execute queries against record store, verify results match Neo4j
7. GREEN: Implement runtime operators
8. Regression suite: run Neo4j's Cucumber tests (23K LOC of `.feature` files)

---

#### `bolt` — 42,064 LOC (Java)

**What it does:** Bolt wire protocol — binary serialization, session state
machine, transaction management over the wire.

**Rust approach:**
- Wire format → `#[repr(C)]` message structs + custom serializer
- Session state machine → Rust enum state machine (`match` over states)
- TCP server → `tokio::net::TcpListener` or raw epoll/io_uring
- Connection pooling → handled by the client driver (we just serve)

**Speed gain:** 2-5x
- Java Netty: event loop + ByteBuf allocation + garbage collection
- Rust tokio: zero-copy reads, no allocation per message, no GC

**TDD approach:**
1. STUB: Define Bolt message types, session states
2. RED: Serialize/deserialize each message type round-trip
3. GREEN: Implement PackStream serializer
4. RED: State machine transitions (HELLO → READY → STREAMING → etc.)
5. GREEN: Implement session state machine
6. RED: Integration — connect official Neo4j Python/Java driver, run query
7. GREEN: Full Bolt server

---

### Tier 3 — Supporting Infrastructure (PORT AS NEEDED)

#### `collections` — 11,356 LOC

**What:** Custom collection types (long-to-object maps, primitive arrays).
**Rust:** Replace with `HashMap<u64, T>`, `Vec<T>`. Rust's standard
collections are already optimized for the cases Java needs custom ones.
**Speed gain:** 1-2x (Rust stdlib is already good; Java needed custom to
avoid boxing)

---

#### `common` — 12,324 LOC

**What:** Shared utilities — resource management, dependency injection.
**Rust:** Most of this is Java boilerplate that Rust doesn't need. Resource
management → `Drop`. DI → trait bounds at compile time.
**Estimated Rust LOC:** ~3-5K (much shrinks away)

---

#### `configuration` — 12,295 LOC

**What:** Config parsing, validation, setting definitions.
**Rust:** `serde` + `toml`/`yaml` → ~3K LOC. Java's config framework is
heavyweight; Rust's derive-based deserialization handles it trivially.

---

#### `values` — 24,076 LOC

**What:** Type system — CypherValue, AnyValue, VirtualValue. The runtime
representation of all data types.
**Rust:** Enum-based value type. `CypherValue { Integer(i64), Float(f64),
String(Arc<str>), List(Vec<CypherValue>), Map(...), ... }`.
**Estimated Rust LOC:** ~5-8K. Java's value hierarchy is deep because of
subtyping; Rust's enum is flat and complete.
**Speed gain:** 3-5x (no virtual dispatch, no boxing, enum discriminant is
1 byte vs 8-byte vtable pointer)

---

#### `id-generator` — 10,939 LOC

**What:** Recycles freed entity IDs via GBPTree or scan.
**Rust:** Reuse GBPTree from `index`. Freed IDs → sorted set in B+tree.
**Estimated Rust LOC:** ~3-5K

---

#### `import-util` / `import-tool` — 26,212 LOC combined

**What:** CSV/batch import pipeline. High-throughput parallel importer.
**Rust:** `csv` crate + `rayon` for parallel record writing. This is where
Rust really shines — Bun-style zero-copy parsing of CSV rows directly
into record format.
**Speed gain:** 3-10x (Rust's zero-copy CSV parsing vs Java's String allocation)

---

#### `logging` — 4,080 LOC

**What:** Log framework.
**Rust:** `tracing` crate. ~500 LOC of setup. Done.

---

#### `procedure` / `procedure-api` / `procedure-compiler` — 19,909 LOC combined

**What:** User-defined procedures and functions.
**Rust:** Plugin system via `dylib` loading or WASM sandbox.
**Can defer:** Not needed for initial release.

---

#### `server` / `server-api` — 21,337 LOC combined

**What:** HTTP API, browser interface.
**Rust:** `axum` or `warp` → ~5K LOC. REST endpoints for admin, monitoring.
**Can defer:** Bolt is the primary interface. HTTP is secondary.

---

#### `schema` — 6,669 LOC

**What:** Schema definitions — indexes, constraints.
**Rust:** Thin layer over index module. ~2K LOC.

---

#### `security` — 3,620 LOC

**What:** Auth, user management.
**Rust:** `argon2` for password hashing, simple role-based access.
**Estimated Rust LOC:** ~1.5K

---

### Tier 4 — SKIP (Not Needed for Faithful Port)

| Module | LOC | Why skip |
|---|---|---|
| `community-it` | 207,135 | Integration tests — rewrite as Rust tests |
| `kernel-test` / `kernel-test-utils` | 65,744 | Test infrastructure — rewrite |
| `testing` | 20,277 | Test utilities |
| `gbptree-tests` | 17,603 | B+tree test suite — rewrite as Rust tests |
| `cypher-shell` | 18,777 | CLI tool — rewrite with `clap` (~3K LOC) |
| `fabric` | 14,958 | Federated queries — defer to v2 |
| `lucene-index` | 12,896 | Full-text — use `tantivy` crate when needed |
| `fulltext-index` | 3,029 | Full-text integration — defer |
| `spatial-index` | 2,266 | Spatial — defer |
| `graph-algo` | 4,321 | Algorithms — defer |
| `cloud` | 4,115 | Cloud features — defer |
| `push-to-cloud` | 3,986 | Cloud features — defer |
| `genai-plugin` | 4,230 | AI plugin — defer |
| `codegen` | 13,279 | Code generation — different in Rust |
| `neo4j-harness` | 2,131 | Test harness — rewrite |
| Various BOM/proxy/boot | ~500 | Build infrastructure |

---

## Total Scope Estimate

| Tier | Java/Scala LOC | Estimated Rust LOC | Ratio |
|---|---|---|---|
| Tier 1 — Core Engine | 221,456 | ~60-90K | 3:1 to 4:1 |
| Tier 2 — Query Engine | 743,905 | ~100-150K | 5:1 to 7:1 |
| Tier 3 — Supporting | ~100,000 | ~25-40K | 3:1 to 4:1 |
| Tier 4 — Skip | ~400,000 | 0 (tests rewritten separately) | — |
| **Total** | **~1,465,000** | **~185-280K** | **5:1 to 8:1** |

Rust is denser than Java/Scala. No getters/setters, no boilerplate DI,
no checked exception ceremony, no Scala implicit chains. A 5:1 ratio is
conservative — Bun saw similar ratios in their Zig→Rust port.

---

## The TDD Master Plan

### Philosophy

Every module gets ported following the same cycle:

```
STUB → RED → GREEN → REFACTOR → INTEGRATE
```

The Bun team's key insight: **the test suite is the spec.** Neo4j has
~370K LOC of tests (`community-it` + `kernel-test` + `gbptree-tests` +
Cucumber). These tests define correctness. The Rust port passes when
the Rust binary produces the same results as the Java binary for every
test case.

### Porting Order (Dependencies Flow Down)

```
Phase 1: Foundation (weeks 1-4)
├── io (PageCache)
├── wal (Write-ahead log)
├── index (GBPTree)
├── values (Type system)
├── collections
└── common utilities

Phase 2: Storage (weeks 5-8)
├── kernel-api (traits / SPI)
├── record-storage-engine (NeoStores)
├── storage-engine-util (counts store)
├── id-generator
├── lock (lock manager)
└── import-util (batch import)

Phase 3: Kernel (weeks 9-12)
├── kernel (transactions, cursors, recovery)
├── schema
├── consistency-check
└── configuration

Phase 4: Query (weeks 13-20)
├── cypher/front-end (parser)
├── cypher/planner
├── cypher/runtime
├── bolt (wire protocol)
└── cypher-shell (CLI)

Phase 5: Server & Polish (weeks 21-24)
├── server (HTTP API)
├── security
├── dbms (database management)
├── logging / monitoring
└── procedure (plugin system)
```

### TDD Per Phase

**Phase 1 — Foundation:**
- RED: Page read/write round-trip, WAL crash recovery, B+tree
  insert/get/scan, value serialization
- Gate: all foundation tests green before Phase 2

**Phase 2 — Storage:**
- RED: Create node record, read back. Create relationship, traverse.
  Batch import 1M records, verify.
- Gate: record-level parity with Java Neo4j

**Phase 3 — Kernel:**
- RED: Open transaction, write node, commit, read node in new transaction.
  Concurrent transactions with isolation. Crash recovery.
- Gate: ACID properties verified under stress

**Phase 4 — Query:**
- RED: Parse Cypher, produce correct AST. Plan query, produce correct
  logical plan. Execute query, produce correct result set.
- Gate: **Neo4j's Cucumber test suite passes** (3,874 openCypher TCK
  scenarios)
- This is the big one. The Cucumber tests ARE the compatibility spec.

**Phase 5 — Server:**
- RED: Connect official Neo4j driver, run MATCH query, get correct result.
- Gate: official Neo4j Python/Java/JS drivers work unmodified.

---

## The "It's Just a Binary" Pitch

Shreyas Doshi would frame this as the ultimate low-friction product:

### Before
```
$ neo4j start
# Java process, 2GB heap, 30s startup, GC pauses
```

### After
```
$ rustneo start
# 15MB binary, 200MB RSS, <1s startup, zero GC
```

**Everything else is the same.** Same `bolt://` protocol. Same Cypher
queries. Same drivers. Same application code. Same data files (or a
one-time migration).

The marketing writes itself:

> **"Neo4j, but fast."**
>
> Same Cypher. Same Bolt. Same drivers.
> 5x faster. 5x less RAM. Zero GC pauses.
> One binary. No JVM.

### Why This Is Credible

Bun proved this exact playbook works:
- 705K LOC Zig → Rust in ~1 week (with AI assistance)
- Same architecture, same data structures
- Binary shrinks, benchmarks neutral-to-faster
- Existing test suite passes at 99.8%
- Zero user-facing API changes

Neo4j is ~1.5M LOC, but:
- ~400K is tests (rewrite, don't port)
- ~200K is Tier 4 (skip/defer)
- The actual engine is ~900K LOC
- Scala→Rust compresses at 5-7:1 ratio
- **Effective port: ~150-250K LOC of Rust**

With AI assistance (Claude/Devin for mechanical translation, human for
architecture decisions), this is a 3-6 month project, not a multi-year one.

---

## What Rust Specifically Buys (Best-of-Rust for Speed)

| Rust feature | What it replaces | Speed impact |
|---|---|---|
| `Drop` (RAII) | Java try-finally / finalizers | Eliminates leak-on-error-path bug class |
| `#[repr(C)]` structs | Java objects (12-16 byte headers) | 2-3x more records per cache line |
| `mmap` (zero-cost) | Muninn PageCache | 2-4x page access |
| Monomorphized generics | Java virtual dispatch | 20-50% faster hot loops |
| `enum` (tagged union) | Java class hierarchies | No vtable, 1-byte discriminant |
| Arena allocation | Java GC heap | Zero GC pauses |
| `parking_lot` locks | Java `ReentrantLock` | 2-3x lock/unlock throughput |
| `io_uring` | Java NIO FileChannel | 2-5x async I/O |
| `rayon` / manual threads | Java `ForkJoinPool` | Better work-stealing, no GC interference |
| Stack-allocated rows | Heap-allocated Row objects | Zero allocation per query row |
| `serde` derive | Java serialization boilerplate | 10x less code, same or faster |
| `tantivy` (if needed) | Lucene | 2-5x full-text search |
| Static binary | JVM + classpath | 10-30MB vs 200MB, <1s vs 30s startup |

---

## Risk Assessment

| Risk | Mitigation |
|---|---|
| Cypher compatibility gaps | Run openCypher TCK (3,874 tests) as gate |
| Performance regression in edge cases | Shadow-diff testing against Java Neo4j |
| Data file format compatibility | Support migration tool; new binary format OK |
| Missing Neo4j Enterprise features | Community edition only for v1 |
| Community adoption resistance | Publish benchmarks, offer migration tool |
| Scala planner complexity | Start with subset, expand iteratively |

---

## The Decision

This is not "should we rewrite Neo4j in Rust?"

This is: **"Bun proved that a faithful Rust port of a complex runtime is a
1-week-to-6-month project that produces a measurably better binary with
zero user-facing changes. Neo4j is the same kind of problem. The test
suite exists. The architecture is documented. The tools are ready."**

It's just a binary.

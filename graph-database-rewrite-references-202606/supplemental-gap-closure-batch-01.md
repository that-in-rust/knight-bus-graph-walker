# Supplemental Gap Closure Batch 01

Date: 2026-07-06

This supplement closes several high-value research gaps left by the first
supermeta corpus pass. It focuses on repositories that are unusually relevant
to a Rust rewrite of Neo4j:

- Neo4j Testkit and Bolt stub behavior for driver compatibility.
- ClickHouse query execution and storage snapshot boundaries.
- Memgraph storage and memory-accounting patterns.
- Polars query optimization flags and streaming/in-memory execution choice.
- GraphScope partitioned graph analytics and incremental update ideas.
- TiKV Raft/snapshot/recovery mechanics.
- Quickwit control-plane rebuild and uncertain-transaction observability.

Important evidence policy: CodeGraphContext was used as navigational evidence.
Every design claim below is tied to direct source reads from local repositories.

## Evidence Tools And Runs

### CodeGraphContext Runs

Neo4j Testkit Bolt stub:

```text
run_dir: /tmp/codex-code-intel/codegraphcontext/neo4j-testkit-boltstub-20260706-234021
indexed path: gitrefrepo/Neo4j family/neo4j-testkit-src/boltstub
stats: 47 files, 957 functions, 179 classes, 128 modules
note: re-indexing reported a non-fatal "'NoneType' object has no attribute 'split'" before stats were available
```

Memgraph storage v2:

```text
run_dir: /tmp/codex-code-intel/codegraphcontext/memgraph-storage-v2-20260706-234021
indexed path: gitrefrepo/memgraph-src/src/storage/v2
stats: 227 files, 2799 functions, 153 classes, 493 structs, 56 enums, 342 modules
note: CGC reported unresolved overloaded C++ call relationships, so direct source reads remain authoritative
```

## Pattern: Driver Compatibility As A Protocol Conversation Oracle

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-testkit-src`
- Language: Python
- Files:
  - `nutkit/frontend/driver.py:57-75`
  - `nutkit/frontend/driver.py:81-124`
  - `nutkit/frontend/session.py:35-99`

Evidence:

- The Testkit frontend builds a `NewDriver` protocol request with URI, auth,
  user agent, resolver, pool, retry, certificate, notification, telemetry, and
  timeout options before it receives a `Driver` handle.
- The driver receive loop handles resolver callbacks, domain-name resolution
  callbacks, auth-token managers, bookmark managers, and certificate-provider
  callbacks before returning the final response.
- Session transaction execution is not just "run Cypher." It enters a retryable
  conversation where the backend sends `RetryableTry`, the frontend runs the
  user transaction function, and the frontend replies with positive or negative
  retryable outcomes.

Transferable idea:

Compatibility is a conversation, not an endpoint shape. A Rust Neo4j rewrite
must pass a driver-oracle suite that exercises every callback and transaction
edge case, not only happy-path Bolt messages.

Rust translation:

```rust
enum DriverConversationEvent {
    NewDriver(NewDriverConfig),
    ResolverRequired(Address),
    DomainNameResolutionRequired(String),
    AuthTokenRequired(AuthTokenManagerId),
    BookmarkCallback(BookmarkManagerId),
    RetryableTry(TransactionId),
    RetryablePositive(SessionId),
    RetryableNegative(SessionId, DriverErrorId),
}
```

Why it matters for Neo4j-in-Rust:

- Bolt compatibility must include routing, callbacks, retry semantics,
  bookmarks, auth-manager behavior, and error propagation.
- A low-RAM graph server can still fail the product goal if official drivers
  behave differently.
- The Testkit shape suggests the compatibility test harness should be a first
  class executable spec before OLAP kernels are called "done."

Memory implications:

- Driver compatibility should not force graph data into heap. The server can
  keep graph storage independent while the protocol layer owns small session,
  bookmark, routing, retry, and auth state.

Testing implications:

- Add deterministic scenario scripts for each driver callback.
- Treat "unsupported but registered" GDS procedures differently from unknown
  procedure names; official clients need stable error shapes.
- Include negative transaction callbacks and driver-originated error IDs in the
  compatibility matrix.

Agentic guidance:

When generating Bolt/procedure server code, do not start with "execute Cypher."
Start with "replay the Neo4j Testkit conversation state machine."

## Pattern: Scriptable Bolt Stub As Wire-Level Regression Harness

Where found:

- Repository: `gitrefrepo/Neo4j family/neo4j-testkit-src`
- Language: Python
- File: `boltstub/channel.py:36-130`

Evidence:

- `Channel` is described in source comments as the glue between stub script,
  socket, and Bolt protocol.
- It validates the Bolt magic preamble, negotiates or fixes handshake data, and
  supports manifest-version-specific handshake handlers.
- It can inject handshake delay and verify expected client handshake response
  bytes.

Transferable idea:

Wire protocol compatibility needs a scriptable fake peer that can accept, delay,
reject, or mutate handshake and message flows. This catches compatibility bugs
that unit tests over internal handlers miss.

Rust translation:

```rust
trait BoltScriptStep {
    fn apply(
        &self,
        wire: &mut dyn BoltWire,
        state: &mut BoltScriptState,
    ) -> Result<BoltStepOutcome, BoltStubError>;
}
```

Why it matters for Neo4j-in-Rust:

- The Rust rewrite needs a Bolt fixture harness separate from the production
  server loop.
- Handshake negotiation, PackStream versions, delayed responses, and malformed
  sequences must be testable without a full graph store.

Performance implications:

- Keep the stub out of production hot paths. It is a test oracle, not the
  runtime architecture.

Testing implications:

- Script Bolt handshake versions, manifest versions, delayed handshakes, and
  wrong magic bytes.
- Add golden traces for official Neo4j driver behavior.

Agentic guidance:

If future agents implement Bolt by directly binding sockets to query execution,
stop them. First create a protocol transcript harness that can reproduce
Testkit-like handshakes and failure modes.

## Pattern: Query Context Callback Surface

Where found:

- Repository: `gitrefrepo/clickhouse-src`
- Language: C++
- File: `src/Server/TCPHandler.cpp:626-790`

Evidence:

- ClickHouse attaches callbacks to query context for query-plan
  deserialization, external table initialization, input storage headers, input
  block reading, cluster-function read tasks, MergeTree range announcements,
  and MergeTree parallel read tasks.
- The callbacks take locks, check cancellation, update metrics, reset streams
  for memory accounting, and mark the query as stopped on exceptions.

Transferable idea:

A query runtime should expose a callback surface around remote inputs,
distributed read tasks, plan serialization, and cancellation instead of burying
these in one monolithic executor.

Rust translation:

```rust
struct QueryExecutionCallbacks {
    load_plan: Option<Box<dyn Fn() -> Result<SerializedPlan> + Send + Sync>>,
    init_external_tables: Option<Box<dyn Fn(&QueryContext) -> Result<()> + Send + Sync>>,
    next_input_block: Option<Box<dyn Fn(&QueryContext) -> Result<Option<RecordBatch>> + Send + Sync>>,
    next_read_task: Option<Box<dyn Fn(ReadTaskRequest) -> Result<Option<ReadTask>> + Send + Sync>>,
}
```

Why it matters for Neo4j-in-Rust:

- GDS and Cypher execution need cancellation, memory accounting, external
  inputs, streaming result delivery, and possibly serialized physical plans.
- OLAP global scans can expose "next tile" or "next CSR stream chunk" callbacks
  with explicit memory budgets.

Memory implications:

- The callback boundary can reset or release input buffers at exact points.
- A RAM-first graph engine should attach memory accounting to callback entry and
  exit, especially for projection materialization and algorithm scratch.

Concurrency implications:

- The ClickHouse span is lock-aware and cancellation-aware. The Rust rewrite
  should make cancellation tokens and query-state locking explicit in every
  callback.

Testing implications:

- Inject callback failures and assert the query state is stopped.
- Assert memory-accounting counters decrease after input buffers are reset.

Agentic guidance:

When designing execution APIs, prefer "callback slots on a query context" over a
single opaque `execute()` body. This makes fault injection and memory
instrumentation much easier.

## Pattern: Storage Snapshot Captured At Query-Tree Boundary

Where found:

- Repository: `gitrefrepo/clickhouse-src`
- Language: C++
- File: `src/Analyzer/TableNode.cpp:58-110`

Evidence:

- A `TableNode` owns storage, storage ID, a share lock, and a storage snapshot.
- Construction locks the storage for share and obtains a storage snapshot from
  in-memory metadata and context.
- Updating a table node refreshes storage ID, share lock, and storage snapshot
  together.

Transferable idea:

Logical plan nodes should capture the storage snapshot they rely on. The query
engine should not repeatedly rediscover mutable store metadata while planning or
executing.

Rust translation:

```rust
struct GraphStorePlanNode {
    storage_id: StorageId,
    storage_guard: SharedStorageGuard,
    snapshot: Arc<GraphStorageSnapshot>,
}
```

Why it matters for Neo4j-in-Rust:

- Cypher and GDS procedures need a stable graph view while OLTP keeps mutating.
- OLAP snapshots, cell generations, label/type sidecars, and property columns
  should be pinned by a query handle rather than looked up ad hoc.

Memory implications:

- Snapshot handles should be small metadata guards, not eager copies of graph
  topology.
- This supports "pin generation N" semantics for low-RAM OLAP.

Testing implications:

- Create a query over generation N, mutate OLTP, and assert the query still sees
  generation N unless explicitly configured to include deltas.

Agentic guidance:

Any generated query-plan node that references graph storage should carry a
snapshot handle. Avoid APIs that accept only a mutable global database pointer.

## Pattern: Tagged Edge Iterable Across Multiple Index Shapes

Where found:

- Repository: `gitrefrepo/memgraph-src`
- Language: C++
- File: `src/storage/v2/edges_iterable.cpp:16-140`

Evidence:

- `EdgesIterable` wraps iterable variants for edge-type index, edge-type-property
  index, and edge-property index.
- It uses a type tag, placement construction, move/copy constructors,
  destructor dispatch, and variant-specific begin/end iterators.

Transferable idea:

A graph query planner often needs a single edge-iteration interface that can be
backed by different physical indexes. The interface should hide the physical
index source while preserving no-extra-allocation iteration.

Rust translation:

```rust
enum EdgeIterable<'a> {
    ByType(TypeIndexIter<'a>),
    ByTypeProperty(TypePropertyIndexIter<'a>),
    ByProperty(PropertyIndexIter<'a>),
}

impl<'a> Iterator for EdgeIterable<'a> {
    type Item = EdgeRef;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::ByType(iter) => iter.next(),
            Self::ByTypeProperty(iter) => iter.next(),
            Self::ByProperty(iter) => iter.next(),
        }
    }
}
```

Why it matters for Neo4j-in-Rust:

- Cypher expansion and GDS projections should choose label/type/property indexes
  without changing downstream operator code.
- For RAM-first design, a tagged iterator avoids materializing candidate edge
  lists unless an algorithm truly needs them.

Memory implications:

- In Rust, an enum iterator can replace C++ placement-new dispatch while keeping
  ownership and destruction safe.
- Beware that boxing trait objects may add heap allocations; use enums when the
  number of physical index types is small and known.

Testing implications:

- The same graph query should return identical edge IDs through all equivalent
  index-backed paths.
- Add tests for empty indexes, invalidated indexes, and mixed property/type
  filters.

Agentic guidance:

Do not generate separate query operators for every edge index. Generate one
logical edge stream with physical iterator variants underneath.

## Pattern: Allocator Hooks With Non-Rollback Memory Tracking

Where found:

- Repository: `gitrefrepo/memgraph-src`
- Language: C++
- File: `src/memory/db_arena.cpp:71-145`

Evidence:

- Memgraph wraps arena allocation/deallocation/commit/decommit/purge hooks and
  updates a memory tracker when committed pages are allocated or freed.
- Commit handling uses an out-of-memory exception blocker because the OS commit
  cannot be rolled back at that point; tracking must be guaranteed rather than
  optionally unwound.
- Hook initialization wires custom functions for alloc, dalloc, destroy,
  commit, decommit, forced purge, and pass-through operations.

Transferable idea:

Memory accounting must live at the allocator/page boundary, not only at logical
object construction. Some OS-level memory events are not rollback-friendly, so
the tracker needs a mode for non-throwing guaranteed accounting.

Rust translation:

```rust
struct ArenaMemoryTracker {
    committed_bytes: AtomicI64,
}

impl ArenaMemoryTracker {
    fn account_non_rollback_commit(&self, len: usize) {
        self.committed_bytes.fetch_add(len as i64, Ordering::Relaxed);
    }
}
```

Why it matters for Neo4j-in-Rust:

- A "5x lower RAM" claim cannot only measure Rust heap allocations.
- mmap, page cache, arenas, direct I/O buffers, algorithm vectors, delta
  overlays, and compaction scratch all need explicit accounting classes.

Memory implications:

- Build memory budgets around committed pages and buffers, not only `Vec`
  capacity.
- Separate rollback-safe allocations from non-rollback OS commitments.

Testing implications:

- Add allocation/accounting tests that simulate commit/decommit/purge events.
- Treat accounting mismatch as a correctness bug, not merely observability debt.

Agentic guidance:

When future agents add arenas or buffer pools, require a memory-accounting
contract at the same abstraction layer as allocation and release.

## Pattern: Optimizer Flags As Executable Query Semantics

Where found:

- Repository: `/Users/amuldotexe/Desktop/oss-read-only/polars`
- Language: Rust
- Files:
  - `crates/polars-plan/src/frame/opt_state.rs:1-91`
  - `crates/polars-lazy/src/frame/mod.rs:116-216`
  - `crates/polars-lazy/src/frame/mod.rs:624-676`
  - `crates/polars-plan/src/dsl/builder_dsl.rs:432-478`

Evidence:

- Polars encodes allowed optimizations as bitflags: projection pushdown,
  predicate pushdown, slice pushdown, streaming, common subplan/expression
  elimination, row estimation, eager mode, fast projection, order checks, sort
  collapse, and GPU.
- LazyFrame exposes toggles for these flags.
- `collect_with_engine` selects streaming, GPU, or in-memory execution and then
  builds an optimized IR plan.
- Opaque UDF nodes record whether predicate pushdown, projection pushdown, and
  streaming are allowed.

Transferable idea:

Optimization eligibility should be explicit data in the plan, not hidden in
planner folklore. Operators should declare what pushdowns and execution engines
they can tolerate.

Rust translation:

```rust
bitflags::bitflags! {
    struct GraphPlanFlags: u32 {
        const LABEL_PUSHDOWN = 1;
        const RELTYPE_PUSHDOWN = 1 << 1;
        const PROPERTY_PUSHDOWN = 1 << 2;
        const LIMIT_PUSHDOWN = 1 << 3;
        const DIRECT_STREAMING = 1 << 4;
        const MMAP_INTERACTIVE = 1 << 5;
        const EXACT_RAM_REQUIRED = 1 << 6;
    }
}
```

Why it matters for Neo4j-in-Rust:

- GDS algorithms and Cypher queries should be able to say whether they permit
  projection pushdown, property filter pushdown, tile-local execution, direct
  streaming, or eager materialization.
- This is cleaner than hard-coding "PageRank uses global stream" and "BFS uses
  tile wavefront" into unrelated code.

Memory implications:

- `EXACT_RAM_REQUIRED` should disable mmap/page-cache-dependent plans for global
  algorithms.
- Streaming eligibility should be visible before execution so `estimate` can
  reject unsafe plans.

Testing implications:

- For every query/operator/algorithm, test the flag matrix:
  - pushdown enabled,
  - pushdown disabled,
  - streaming selected,
  - in-memory selected,
  - unsupported engine fallback or rejection.

Agentic guidance:

When adding new procedure implementations, require a plan-flag declaration
before writing the algorithm body. The flags are part of the executable spec.

## Pattern: Fragmented Graph Analytics With Explicit Message And Memory Layers

Where found:

- Repository: `gitrefrepo/graphscope-src`
- Language/docs: C++/Java/Python analytical engine docs
- Files:
  - `docs/analytical_engine/dev_and_test.md:45-72`
  - `docs/analytical_engine/ingress.md:80-87`
  - `docs/frequently_asked_questions.rst:68-75`

Evidence:

- GraphScope documents fragments as graph partitions processed on computing
  nodes.
- A worker loads a fragment, runs the application locally, and communicates with
  other workers through a message manager.
- Ingress uses CSC/CSR optimized storage, batches generated messages, and
  starts incremental computation from affected vertices after graph updates.
- The FAQ separates vineyard metadata memory, vineyard shared data memory, and
  engine memory, noting a large data-memory multiplier in that architecture.

Transferable idea:

Cell/tile/fragment design is not just file organization. A fragment becomes
useful only when it is a unit of local computation, message exchange,
incremental update activation, and memory budgeting.

Rust translation:

```rust
struct GraphFragment {
    fragment_id: FragmentId,
    local_csr: DualCsrSlice,
    boundary_edges: BoundaryEdgeIndex,
    memory_budget: FragmentBudget,
}

trait FragmentProgram {
    fn run_local(&mut self, fragment: &GraphFragment, inbox: MessageBatch) -> MessageBatch;
}
```

Why it matters for Neo4j-in-Rust:

- If "Tilehouse" exists, its cells must be executable units: readable,
  dirtyable, compactable, and budgetable.
- GraphScope/Ingress warns that partitioned analytics can create high memory
  multipliers if the data manager expects fully resident distributed memory.
  Knight Bus should keep the fragment idea but reject the "load 5x dataset in
  shared memory" budget profile for the RAM-first product.

Memory implications:

- Track metadata memory separately from data memory and engine scratch.
- Do not claim low RAM if fragment metadata, boundary messages, and execution
  state duplicate the graph.

Testing implications:

- Add toy graphs with cross-fragment edges and assert exactness across message
  passing.
- Add incremental update tests that activate only affected vertices when the
  algorithm supports that semantics.

Agentic guidance:

When discussing "cells", always ask: "what can a cell do independently?" If the
answer is only "be a folder," the design is not yet a Tilehouse.

## Pattern: Durable Replay Start Points And Tombstone Transitions

Where found:

- Repository: `gitrefrepo/tikv-src`
- Language: Rust
- Files:
  - `src/server/raftkv/mod.rs:652-674`
  - `src/server/debug2.rs:530-670`

Evidence:

- TiKV exposes asynchronous disk snapshots and in-memory hybrid snapshots.
- Debug repair code can drop unapplied Raft logs by updating raft local state,
  apply state, garbage-collecting log ranges, and consuming the log batch.
- Tombstone transition checks region epoch/version validity and writes region
  state at the applied index.
- Region info uses the persisted applied index as the replay start point because
  it is the state TiKV will acquire during startup.

Transferable idea:

Recovery code should make replay boundaries explicit. The system should know
which applied index or snapshot generation is authoritative after restart, and
state transitions such as tombstone must be durable and idempotent.

Rust translation for Neo4j-style WAL to OLAP:

```rust
struct OlapReplayState {
    snapshot_generation: u64,
    durable_tx_id: u64,
    applied_delta_id: u64,
    compacted_cell_generation: CellGeneration,
}
```

Why it matters for Neo4j-in-Rust:

- The OLTP-to-OLAP bridge cannot be "tail the WAL and hope." It needs durable
  applied watermarks, replay start points, cell tombstones, and idempotent
  compaction receipts.
- Deletes and relationship-type/property changes need explicit state
  transitions comparable to tombstone or replacement transitions.

Memory implications:

- Recovery metadata should stay small and durable. Do not force replay to load
  the full graph just to discover the applied watermark.

Testing implications:

- Crash after appending a WAL receipt but before cell compaction.
- Crash after compaction but before watermark update.
- Re-run replay and assert idempotent final graph state.

Agentic guidance:

When adding OLAP delta logic, always design the restart path in the same patch.
No delta structure is acceptable without an applied watermark and replay test.

## Pattern: Control Plane Rebuilds From Metastore After Uncertain Mutation

Where found:

- Repository: `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit`
- Language: Rust
- Files:
  - `quickwit-control-plane/src/model/mod.rs:42-152`
  - `quickwit-control-plane/src/metrics.rs:112-126`

Evidence:

- Quickwit documents that its control-plane model stays consistent with the
  metastore because mutations go through the control plane.
- If a mutation yields an error, the control plane is killed and restarted.
- On start, the control-plane model clears state and reloads indexes, sources,
  and shards from the metastore in batches.
- Metrics distinguish aborted metastore transactions from transactions with an
  uncertain outcome that should trigger restart.

Transferable idea:

For a derived in-memory/control-plane model, restart-and-rebuild can be simpler
and safer than attempting to repair every ambiguous mutation in place.

Rust translation:

```rust
enum DerivedModelMutationOutcome {
    Applied,
    AbortedNoSideEffect,
    MaybeExecutedRequiresRestart,
}
```

Why it matters for Neo4j-in-Rust:

- Graph catalog state, GDS projection catalogs, and OLAP cell manifests are
  derived from authoritative stores.
- If the derived model sees an uncertain mutation outcome, it should rebuild
  from durable metadata rather than guess.

Memory implications:

- Batch reloads prevent rebuilding the derived model with unbounded heap growth.
- Keep the authoritative metastore compact enough to reload on laptop-class
  machines.

Testing implications:

- Inject ambiguous metastore/write outcomes.
- Assert the derived catalog refuses further mutation and rebuilds from the
  authoritative store.
- Track separate metrics for "aborted, safe to continue" and "maybe executed,
  restart required."

Agentic guidance:

If future agents implement GDS catalog or OLAP manifest mutation, require an
explicit ambiguous-outcome policy. "Retry blindly" is not acceptable for derived
state.

## Cross-Batch Synthesis

The gap-closure evidence strengthens five conclusions in the main corpus:

1. Full Neo4j compatibility starts with protocol/test conversations, not graph
   kernels.
2. Query execution needs explicit callbacks, cancellation, and memory-accounting
   seams.
3. A storage/query plan must pin snapshots or generations rather than reading
   mutable global state opportunistically.
4. Physical storage variants should surface as iterators, plan flags, and
   execution budgets, not as many unrelated algorithm-specific code paths.
5. Derived OLAP state needs durable replay boundaries, rebuild-from-authority
   semantics, and ambiguous-outcome handling.

## Repositories Upgraded By This Batch

The following repositories now have additional direct source-backed evidence in
the corpus:

- `gitrefrepo/Neo4j family/neo4j-testkit-src`
- `gitrefrepo/clickhouse-src`
- `gitrefrepo/memgraph-src`
- `/Users/amuldotexe/Desktop/oss-read-only/polars`
- `gitrefrepo/graphscope-src`
- `gitrefrepo/tikv-src`
- `/Users/amuldotexe/Desktop/oss-read-only/alienplatform/quickwit`

This does not complete the full goal. The goal still asks for every Desktop repo
to be browsed with CodeGraphContext. This batch closes priority architecture
gaps and records exactly where the evidence came from.

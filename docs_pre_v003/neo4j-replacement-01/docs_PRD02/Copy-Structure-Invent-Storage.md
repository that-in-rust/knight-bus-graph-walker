# Copy Structure, Invent Storage

*What parts of Neo4j's backend can be directly copied into Rust,
and where is the real architectural fork?*

---

## The Pushback

The Timeline Traverser analysis (docs_PRD01) made the backend port
sound harder than it needs to be. The honest correction:

> A lot of Neo4j's backend is structural scaffolding. You copy the
> structure, translate Java interfaces to Rust traits, and fill them
> in. That's copy-work, not invention.

The REAL question isn't "how hard is the port?" It's:

> Which storage format sits at the center — linked-list records or
> CSR arrays?

Everything else (transactions, WAL, indexes, Bolt, Cypher) can be
copied from Neo4j's structure regardless of the storage engine choice.

---

## What You Can Copy Directly

These are the parts where Neo4j has already done the design work.
You're translating Java → Rust, not inventing anything new.

### Module Boundaries (~0 design risk)

Neo4j's 68-folder split is 15 years of good engineering. The folder
structure IS the architecture. You'd create the same Rust workspace:

```text
knrt/
  kernel-api/          → pub trait StorageEngine { ... }
  kernel/              → struct KernelTransaction { ... }
  bolt/                → struct BoltServer { ... }
  cypher-frontend/     → struct CypherParser { ... }
  values/              → enum CypherValue { ... }
  ...
```

This is mechanical work. Copy the module names. Translate the Java
interfaces to Rust traits. The boundaries are already proven by
Neo4j's decade of evolution.

### Trait / Interface Boundaries (~0 design risk)

Neo4j's `kernel-api/` (18,542 LOC) defines clean interfaces:

- `StorageEngine` — opens readers, creates commands
- `StorageReader` — reads nodes, relationships, properties
- `CommandCreationContext` — write-side command factory

These translate directly to Rust traits:

```rust
pub trait StorageEngine {
    type Reader: StorageReader;
    fn open_reader(&self) -> Result<Self::Reader, Error>;
}

pub trait StorageReader {
    fn node_cursor(&self, id: NodeId) -> Result<NodeCursor, Error>;
    fn rel_cursor(&self, id: RelId) -> Result<RelCursor, Error>;
}
```

The SHAPE of these traits is the same whether the storage is
linked-list records or CSR arrays. Only the implementation changes.

### Transaction State Machine (~low design risk)

Neo4j's `KernelTransaction` follows a standard state machine:

```text
NEW → OPEN → { COMMIT | ROLLBACK } → CLOSED
```

With:
- Read-only vs read-write modes
- Snapshot isolation (MVCC)
- Write operations buffered until commit
- Rollback discards buffered writes

This is textbook database transaction management. You'd write the
same state machine in Rust. The concurrency edge cases are the only
hard part, and even those are well-studied patterns.

### Write-Ahead Log (~low design risk)

Neo4j's WAL (`wal/` — 8,888 LOC):

- Log entries describe mutations (create node, set property, etc.)
- Checkpoint flushes dirty pages and records log position
- Recovery replays log entries after checkpoint on crash
- Log rotation manages file growth

This is standard database durability. The format can be different
but the structure is the same. Rust's type system actually makes
WAL entries safer (enum variants instead of command byte codes).

### B+Tree Indexes (~low design risk)

Neo4j's GBPTree (`index/` — 13,402 LOC):

- Standard B+tree with crash-safe splits
- Schema indexes for property lookups
- Counts store for cardinality estimation

B+trees are textbook. Rust has good primitives (`BTreeMap` for
in-memory, and crates like `sled` or custom implementations for
on-disk). The checkpoint integration is the only Neo4j-specific
part, and it follows from the WAL design.

### Bolt Protocol (~low design risk)

Bolt is a documented wire protocol:

- PackStream binary serialization
- Versioned handshake (v3, v4, v5)
- Session state machine (HELLO → READY → STREAMING → ...)
- Auth handshake

This is a specification you implement. The protocol doesn't care
what's behind it. Neo4j's `bolt/` (42,064 LOC) is large because it
handles every protocol version, connection pooling, and error
recovery. A v4-only implementation is much smaller (~10-15K LOC).

### Error Hierarchy (~zero design risk)

Neo4j's error codes and GQL status codes are documented. Copy the
enum, translate to Rust:

```rust
pub enum Neo4jError {
    ClientError(ClientError),
    DatabaseError(DatabaseError),
    TransientError(TransientError),
}
```

Driver compatibility requires matching error codes exactly. This is
tedious but not hard.

### Configuration Model (~zero design risk)

Neo4j has ~400 settings in `configuration/` (12,295 LOC). Most are
straightforward key-value pairs with typed defaults. You'd create a
Rust config struct with `serde`:

```rust
#[derive(Deserialize)]
pub struct Config {
    #[serde(default = "default_page_cache_size")]
    pub dbms_memory_pagecache_size: ByteSize,
    // ... ~400 more fields
}
```

Copy the setting names and defaults from Neo4j's documentation.

### CLI Tools (~zero design risk)

`cypher-shell/` (18,777 LOC) is an interactive REPL. Rust has
excellent CLI libraries (`clap`, `rustyline`). The Knight Bus repo
already has a working CLI (305 LOC). Expanding it to a Cypher REPL
is straightforward.

### Import Pipeline (~low design risk)

`import-util/` (21,981 LOC) and `csv/` (5,109 LOC) handle batch
import. Knight Bus's `TruthGraphSource` trait already abstracts
data loading. Neo4j's CSV import format is documented. The import
pipeline is copy-work.

---

## The One Real Fork: Storage Engine

Everything above is scaffolding. The actual architectural decision is:

### Option 1: Copy Neo4j's Record Store

Neo4j uses fixed-size linked-list records:

```text
Node Record (15 bytes):
  ┌─────────┬──────────────┬──────────────┬──────────────┬───────────┐
  │ in_use:1│ next_rel:35  │ next_prop:36 │ labels:36    │ flags:8   │
  └─────────┴──────────────┴──────────────┴──────────────┴───────────┘

Relationship Record (34 bytes):
  ┌──────────┬────────┬────────┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────┐
  │ in_use:1 │ src:35 │ dst:35 │ type:16  │ src_prev │ src_next │ dst_prev │ dst_next │ prop:36 │
  └──────────┴────────┴────────┴──────────┴──────────┴──────────┴──────────┴──────────┴─────────┘
```

**Traversal = follow pointer chains:**
```text
node → first_rel → rel.dst → dst.first_rel → rel.dst → ...
```

Each hop is a random read into a different page. Cache misses dominate.

**What you get:**
- Full read/write support from day 1
- Mutation is O(1) — update a record in place
- Well-understood concurrency model
- Compatible with all existing Neo4j import/export tools
- 1.5-2x faster than Java Neo4j (Rust vs JVM gains, no GC pauses)

**What you lose:**
- Knight Bus's proven 10-100x traversal speedup
- The storage-runtime alignment thesis
- The Atlas layout family specialization

### Option 2: Use CSR Arrays (Knight Bus)

Knight Bus uses contiguous arrays:

```text
Forward CSR:
  offsets: [0, 3, 5, 9, ...]    (one u64 per node)
  peers:   [2, 5, 7, 1, 4, ...] (one u32 per edge)

Reverse CSR:
  offsets: [0, 2, 4, 6, ...]
  peers:   [0, 3, 1, 0, ...]
```

**Traversal = array slice:**
```text
node_id → offsets[id]..offsets[id+1] → peers[start..end]
```

One offset lookup, one contiguous memory read. Cache-friendly.

**What you get:**
- 10-100x faster traversal (proven for exact-key fixed-hop)
- mmap-friendly (OS pages in only touched regions)
- Build-heavy/walk-light architecture
- Path to 13 algorithm-specific layout families

**What you lose:**
- In-place mutation (snapshots are immutable)
- Need a separate write path (append log + recompile, or mutable overlay)
- More complex write model for users

### Option 3: Both (Timeline B from the Traverser)

Use a lightweight mutable store for writes AND CSR snapshots for reads.
Writes go to the mutable store, a background thread periodically
materializes CSR snapshots.

**What you get:**
- Fast reads on CSR (10-50x)
- Standard write path
- Incremental migration (start with reads, add writes)

**What you lose:**
- Dual-engine complexity
- Consistency between mutable store and snapshot
- Every feature must work on both paths

---

## The Copy-Work Estimate

Regardless of which storage option you pick, this is the scaffolding
you'd copy from Neo4j:

| Component | Neo4j LOC | Rust estimate | Design risk | Notes |
|---|---|---|---|---|
| Module structure | — | ~0 | None | Create workspace, copy folder names |
| Trait boundaries (`kernel-api`) | 18,542 | 3-5K | None | Translate interfaces to traits |
| Transaction lifecycle | 83,297 | 15-25K | Low | Standard state machine + MVCC |
| WAL | 8,888 | 3-5K | Low | Standard durability pattern |
| B+tree indexes | 13,402 | 5-8K | Low | Well-studied data structure |
| Bolt protocol (v4 only) | 42,064 | 10-15K | Low | Documented spec |
| Error hierarchy | 14,508 | 2-3K | None | Copy enum + codes |
| Configuration | 12,295 | 3-5K | None | Serde struct + defaults |
| CLI / cypher-shell | 23,889 | 5-8K | None | clap + rustyline |
| Import pipeline | 27,090 | 5-8K | Low | CSV parsing + validation |
| Values / types | 24,076 | 4-6K | Low | CypherValue enum |
| Concurrency primitives | 5,522 | 2-3K | Low | Rust's ownership helps |
| **Copy-work subtotal** | **273,573** | **57-91K** | — | — |

That's the ~40-50% of the backend that is structural translation.

The remaining ~50-60% is where the real work and the real decisions
live:

| Component | Neo4j LOC | Design risk | Why it's not copy-work |
|---|---|---|---|
| **Storage engine** | 90,438 | **HIGH** | The fork: records vs CSR |
| **Cypher parser** | 325,311 | **MEDIUM** | ANTLR grammar exists but semantic analysis is hard |
| **Query planner** | 181,802 | **HIGH** | IDP solver, cost model depends on storage |
| **Execution runtimes** | 79,267 | **MEDIUM** | Volcano operators, but operators depend on storage cursors |
| **Search / spatial** | 18,191 | MEDIUM | Lucene integration, R-tree |
| **Page cache** | 14,241 | MEDIUM | mmap vs MuninnPageCache depends on storage |

### The Real Ratio

```text
Total backend:         690,791 LOC (Java)
Copy-work portion:     ~273,573 LOC → 57-91K Rust
Design-fork portion:   ~417,218 LOC → depends on choices
```

**~40% of the backend is copy-work.** The user's instinct is correct.

**~60% requires real design decisions** — but the biggest one (storage
engine) cascades through page cache, query planner cost model, and
execution runtime cursor design. Pick the storage format first, and
the rest follows.

---

## The Cascade Effect

The storage choice isn't isolated. It cascades:

```text
Storage format
  ↓
Page cache design
  ↓
Cursor API (how the runtime reads data)
  ↓
Query planner cost model (what's cheap, what's expensive)
  ↓
Execution runtime operators (Scan, Expand, Filter implementations)
```

**If you pick linked-list records:**
- Page cache = MuninnPageCache (or simplified version)
- Cursors = page-pinning + record decoding
- Cost model = random I/O dominated
- Operators = standard Neo4j Volcano pipeline

**If you pick CSR arrays:**
- Page cache = mmap (already proven)
- Cursors = offset arithmetic + slice
- Cost model = sequential I/O dominated, traversal is nearly free
- Operators = CSR-aware, can batch neighbor reads

**If you pick both:**
- Two cursor APIs
- Two cost models (or a hybrid)
- Operators must handle both paths

The cascade means the storage choice is ~1 decision that determines
~5 other decisions. That's why it matters more than any other part
of the architecture.

---

## What You Actually Don't Have to Worry About

The user's instinct is right. Here's the concrete list of things that
are pure copy-work and need zero architectural thought:

1. **Folder structure** — copy Neo4j's module layout
2. **Trait signatures** — translate Java interfaces
3. **Error codes** — copy the enum
4. **Configuration keys** — copy the settings
5. **Bolt message types** — follow the spec
6. **CLI commands** — copy the UX
7. **Import CSV format** — follow Neo4j's documented format
8. **CypherValue types** — copy the type system
9. **Transaction states** — copy the state machine
10. **WAL entry types** — copy the command enum

These are ~57-91K LOC of Rust that require engineering effort but
zero design risk. Copy the structure, fill in the Rust, write tests.

---

## What You DO Have to Worry About

Only these require real architectural decisions:

1. **Storage format** — records vs CSR vs both (THE decision)
2. **Write model** — in-place mutation vs append-recompile vs overlay
3. **Cypher semantic analysis** — NULL propagation, type coercions,
   scope rules (can borrow from existing Rust Cypher parsers)
4. **Query planner cost model** — must match the storage format's
   actual I/O characteristics
5. **Algorithm layout families** — which Atlas families to build and
   when (can be incremental)

That's 5 real decisions. Everything else is copy-work.

---

## Recommendation

**Copy the structure. Invent the storage.**

1. Copy Neo4j's module boundaries, trait shapes, error codes,
   configuration, Bolt protocol, CLI, import pipeline, WAL
   structure, and transaction state machine. That's ~57-91K LOC of
   Rust with near-zero design risk.

2. Make ONE architectural decision: what sits at the center of the
   `StorageEngine` trait implementation? The Timeline Traverser
   analysis recommends CSR (Timeline C or E), but the scaffolding
   around it is the same either way.

3. The scaffolding ships on the same schedule regardless of the
   storage choice. Bolt works the same. Cypher parser works the
   same. Errors work the same. Transactions work the same. Only
   the cursor implementation and cost model change.

**The user's instinct — "just copy the structure" — is correct for
~40% of the work. The remaining ~60% is downstream of one decision:
what shape are the bytes on disk?**

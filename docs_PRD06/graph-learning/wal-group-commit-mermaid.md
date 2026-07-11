# WAL Group Commit — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `wal-group-commit-ascii.md` / `wal-group-commit-mermaid.md` |
| One-line job | Amortize the fsync — many concurrent writers share one log append + one disk flush, turning per-write durability cost into per-batch cost |

## 1. The problem

Durability requires `fsync`, and fsync (~50 µs NVMe, ~10 ms HDD) costs
thousands of appends. Paying it per write caps throughput at
`1/fsync_latency` regardless of CPUs. One fsync durably persists
*everything appended before it* — so writers can share one.

```mermaid
flowchart LR
    subgraph naive [naive: 3 fsyncs serialized]
        n1[w1 append+fsync] --> n2[w2 append+fsync] --> n3[w3 append+fsync]
    end
    subgraph group [group commit: 1 shared fsync]
        g1[w1 append] --> S[ONE fsync]
        g2[w2 append] --> S
        g3[w3 append] --> S
        S --> ACK[ack w1, w2, w3]
    end
```

## 2. The coordination structure

```mermaid
flowchart LR
    subgraph queue [writer queue — RocksDB WriteThread / Pebble commitPipeline.pending]
        L["W leader (head)"] --> F1[W follower] --> F2[W follower] --> F3[W newest]
    end
    L -- "scans forward, adopts followers<br/>into its WriteGroup (byte-budgeted)" --> F2
    queue --> WAL["WAL file: len|crc|payload ..."]
```

Each writer carries its batch, a state flag, and a wait condvar. Source:
`reference-repos-corpus/rocksdb-src/db/write_thread.h` (WriteGroup at
line 145, JoinBatchGroup contract near line 308).

## 3. One commit round (RocksDB shape)

```mermaid
sequenceDiagram
    participant W1 as writer 1 (leader)
    participant W2 as writer 2
    participant W3 as writer 3
    participant WAL as WAL file
    participant MT as memtable
    W1->>W1: JoinBatchGroup -> head => LEADER
    W2->>W2: JoinBatchGroup -> block on state flag
    W3->>W3: JoinBatchGroup -> block on state flag
    W1->>W1: adopt W2, W3 into WriteGroup
    W1->>WAL: append concat(batch1,batch2,batch3)
    W1->>WAL: ONE fsync (if any member requested sync)
    W1->>MT: apply inserts (or parallel via followers)
    W1->>W1: assign consecutive seqnums to group
    W1-->>W2: state = done, wake
    W1-->>W3: state = done, wake (one promoted to next leader)
```

Pebble reshapes this as a two-stage pipeline — stage 1 WAL append under
a semaphore, stage 2 in-order seqnum publication
(`reference-repos-corpus/pebble-src/commit.go`, `commitPipeline.Commit`
at line 299, pending-queue invariants at lines 228–240).

## 4. Sync policy spectrum

The group shrinks cost *per sync*; when to sync is a durability policy:

```mermaid
flowchart TD
    P[sync policy] --> A["every group<br/>rocksdb WriteOptions.sync=true<br/>loss window: zero"]
    P --> B["deferred to checkpoint<br/>sqlite synchronous=NORMAL, walWriteToLog<br/>loss window: last N ms"]
    P --> C["on segment rotate/interval<br/>slatedb wal_buffer to object storage<br/>loss window: last segment"]
```

Source anchors: `reference-repos-corpus/sqlite-src/src/wal.c`
(walWriteToLog, ~line 3938);
`reference-repos-corpus/slatedb-src/slatedb/src/wal_buffer.rs`.

## 5. Worked example — throughput arithmetic

fsync = 1 ms, append = 5 µs, 64 concurrent writers:

```mermaid
flowchart LR
    subgraph N [naive]
        NA["each write: 5 µs + 1000 µs"] --> NB["~995 writes/s total"]
    end
    subgraph G [group commit]
        GA["round: 64 x 5 µs + 1 ms = 1.32 ms"] --> GB["64/1.32 ms = ~48,000 writes/s (~48x)"]
    end
```

Worst-case latency waits one full round (~2.6 ms): a bounded latency bump
buys order-of-magnitude throughput.

## 6. Worked example — in-order publication

Groups overlap in the pipeline. If group B (seq 200–260) finished its
memtable writes before group A (seq 100–199), a reader at snapshot 260
would see B without A — a hole in history. Pebble's pending queue
releases visibility strictly in enqueue order:

```mermaid
sequenceDiagram
    participant A as group A (seq 100-199)
    participant B as group B (seq 200-260)
    participant V as visible seq
    A->>A: WAL append, memtable (slow)
    B->>B: WAL append, memtable (fast, done first)
    Note over B,V: B waits — A not yet published
    A->>V: publish 199
    B->>V: publish 260 (immediately after)
```

This is MVCC snapshot publication in miniature — generalized by the
sibling `mvcc-snapshot-visibility` pattern.

## 7. Where graph systems inherit this

```mermaid
flowchart LR
    GC[group commit] --> NEO[Neo4j transaction log<br/>batched commit records]
    GC --> TIKV[TiKV: Raft round =<br/>group commit across machines]
    GC --> KB[this repo: journal tier —<br/>group = ingest batch,<br/>same publish-in-order rule]
    GC --> TUR[Turso: SQLite WAL frame batching<br/>survives full Rust rewrite]
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/write_thread.h` | leader/follower states, WriteGroup, JoinBatchGroup |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/db_impl/db_impl_write.cc` | write-group consume loop |
| Pebble | `reference-repos-corpus/pebble-src/commit.go` | two-stage pipeline; in-order publication |
| SQLite | `reference-repos-corpus/sqlite-src/src/wal.c` | frame-batched WAL, deferred fsync |
| SlateDB | `reference-repos-corpus/slatedb-src/slatedb/src/wal_buffer.rs` | interval-flushed WAL to object storage |
| Turso | `reference-repos-corpus/turso-src/core/storage/wal.rs` | Rust rewrite preserving frame batching |

## 9. Cross-references

- Paper: ARIES (recovery side of the log) — `research-papers-ledger.md`.
- Sibling patterns: `lsm-compaction-tradeoff` (downstream of the WAL);
  `mvcc-snapshot-visibility` (generalizes in-order publication).
- 202606 digest overlap: none — prior study never opened a write path.

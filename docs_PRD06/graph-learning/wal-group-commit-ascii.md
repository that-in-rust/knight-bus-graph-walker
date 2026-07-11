# WAL Group Commit — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `wal-group-commit-ascii.md` / `wal-group-commit-mermaid.md` |
| One-line job | Amortize the fsync — many concurrent writers share one log append + one disk flush, turning per-write durability cost into per-batch cost |

## 1. The job

Durability means: don't acknowledge a write until its log record has
survived a power cut. The primitive that guarantees survival is `fsync`,
and an fsync costs ~50 µs (NVMe) to ~10 ms (spinning disk) — thousands of
times more than appending bytes to a file. If every writer paid its own
fsync, a storage engine would top out at `1/fsync_latency` writes per
second regardless of CPU count.

Group commit fixes this with one observation: **an fsync durably persists
everything appended before it, not just your bytes.** So writers queue up,
one of them becomes the *leader*, appends everyone's records, issues ONE
fsync, and wakes the whole group.

```text
naive:                          group commit:

w1: append fsync ack            w1: append-\
w2:       append fsync ack      w2: append--+--> ONE fsync --> ack w1,w2,w3
w3:             append fsync    w3: append-/
    3 fsyncs, serialized            1 fsync, shared
    tput = 1/fsync                  tput = batch_size/fsync
```

## 2. Raw data shape

The WAL is an append-only file of length-prefixed, checksummed records.
The in-memory coordination structure is a queue of pending writers:

```text
WAL file:  [len|crc|payload][len|crc|payload][len|crc|payload] ...
                                                    ^ write cursor

writer queue (RocksDB WriteThread / Pebble commitPipeline.pending):

  head -> [W: leader] -> [W] -> [W] -> [W: newest]      each W carries:
             |                                            - its batch
             +-- scans forward, adopts followers          - a state flag
                 into its WriteGroup                      - a wait condvar
```

## 3. Step-by-step: RocksDB's JoinBatchGroup

From `write_thread.h` (states and linking) and `db_impl_write.cc` (the
loop that consumes groups):

```text
1. every arriving writer links itself into a lock-free list and calls
   JoinBatchGroup (rocksdb-src/db/write_thread.h:308).
2. if it is the list head, it becomes LEADER; otherwise it blocks on
   its state flag (a spin-then-futex wait).
3. the leader walks the list, adopting followers into its WriteGroup
   (write_thread.h:145) up to a max byte budget — a huge follower batch
   is left for the next group so latency stays bounded.
4. leader concatenates all batches, appends once to the WAL, and if any
   member asked for sync, issues ONE fsync.
5. leader applies the memtable inserts (or hands parallel memtable
   writes to the followers), assigns the group's sequence numbers,
   then transitions every follower's state to "done" and wakes them.
6. each follower returns success to its caller; one of the ex-followers
   is promoted to leader for the next group.
```

Pebble restructures the same idea as a two-stage pipeline
(`commit.go:199-312`): stage 1 appends to the WAL under a semaphore,
stage 2 publishes sequence numbers strictly in queue order — so the
"group" is implicit in whoever shares a WAL sync window
(`commitPipeline.Commit`, `commit.go:299`).

## 4. The sync knob: when the fsync happens at all

Not every engine fsyncs per group; the group only shrinks the *cost per
sync*. Whether to sync per group is a durability policy:

```text
policy                    engine example                    loss window
-----------------------   -------------------------------   -----------
sync every group          rocksdb WriteOptions.sync=true    zero
sync every N ms           sqlite WAL + synchronous=NORMAL   last N ms
                          (fsync deferred to checkpoint;
                           walWriteToLog batches frames,
                           sqlite-src/src/wal.c:3938)
sync on segment rotate    slatedb wal_buffer flushes to     last segment
                          object storage on interval
                          (slatedb/src/wal_buffer.rs)
```

## 5. Worked example 1 — throughput arithmetic

Disk fsync = 1 ms, append = 5 µs, 64 concurrent writers.

```text
naive:  each write = 5 µs + 1000 µs  => ~995 writes/s TOTAL
group:  one round = 64 appends (320 µs) + 1 fsync (1000 µs) = 1.32 ms
        => 64 / 1.32 ms  =  ~48,000 writes/s   (~48x)
latency per write: worst case waits one full round = ~2.6 ms —
group commit trades a bounded latency bump for order-of-magnitude tput.
```

## 6. Worked example 2 — why sequence numbers publish in order

Group members get consecutive sequence numbers, but groups overlap in
the pipeline. If group B (seq 200-260) finishes its memtable writes
before group A (seq 100-199) does, readers at snapshot 260 could see B's
writes but not A's — a hole in the history. Pebble's invariant
(`commit.go:228-240`): the pending queue releases visibility strictly
in enqueue order — B's seqnums do not become visible until A's are.

```text
timeline:  A appends WAL --- A memtable ---------- publish A
           B appends WAL --- B memtable -- (wait) - publish B
visible seq:   ...none...                 199            260
```

This is exactly MVCC snapshot publication in miniature — the sibling
`mvcc-snapshot-visibility` pattern generalizes it.

## 7. Where graph systems inherit this

- Neo4j's transaction log applies the same leader/follower batching to
  commit records; TiKV routes Raft log appends through group-committed
  RocksDB WALs — the network consensus round IS a group commit across
  machines.
- This repo's journal tier (visibility tiers: journal → overlay → base)
  is a WAL whose "group" is the ingest batch; the same
  publish-in-order rule governs when a journal batch becomes visible to
  walkers.
- Turso (SQLite rewritten in Rust) keeps the frame-batched WAL shape in
  `turso-src/core/storage/wal.rs` — batching survives a full rewrite of
  the engine around it.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/write_thread.h` | leader/follower states, WriteGroup, JoinBatchGroup contract |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/db_impl/db_impl_write.cc` | the write-group consume loop |
| Pebble | `reference-repos-corpus/pebble-src/commit.go` | two-stage commit pipeline; in-order seqnum publication |
| SQLite | `reference-repos-corpus/sqlite-src/src/wal.c` | frame-batched WAL writes, deferred fsync policy (walWriteToLog) |
| SlateDB | `reference-repos-corpus/slatedb-src/slatedb/src/wal_buffer.rs` | interval-flushed WAL buffer to object storage |
| Turso | `reference-repos-corpus/turso-src/core/storage/wal.rs` | Rust rewrite preserving SQLite's WAL frame batching |

## 9. Cross-references

- Paper: ARIES (Mohan et al.) for the recovery side of the log — see
  `research-papers-ledger.md`.
- Sibling patterns: `lsm-compaction-tradeoff` (the WAL feeds the
  memtable that feeds the flush); `mvcc-snapshot-visibility`
  (generalizes the in-order publication rule).
- 202606 digest overlap: none — the prior study never opened a write
  path; this pair is new ground.

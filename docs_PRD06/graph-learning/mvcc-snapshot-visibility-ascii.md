# MVCC Snapshot Visibility — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `mvcc-snapshot-visibility-ascii.md` / `mvcc-snapshot-visibility-mermaid.md` |
| One-line job | Let readers see a frozen, consistent world while writers keep writing — by keeping multiple timestamped versions per key and a per-transaction rule for which version is visible |

## 1. The job

Locks make readers and writers fight. MVCC (multi-version concurrency
control) removes the fight: a write never overwrites — it appends a new
version tagged with a timestamp — and a reader carries a snapshot
timestamp that selects, per key, the newest version it is allowed to
see. Readers never block writers; writers never block readers; the cost
is old versions lingering until garbage collection.

```text
storage per key:  k -> [(v1, ts=3), (v2, ts=7), (v3, ts=12), ...]
reader at ts=10:  sees v2  (newest version with ts <= 10 that was
                            committed when the reader began)
```

## 2. Raw data shape

Every implementation encodes "key + version" so that a single ordered
scan finds the right version:

```text
toydb    Key::Version(user_key, version)  — bincode-ordered composite
                                            (toydb-src/src/storage/mvcc.rs)
badger   key || (max_u64 - commit_ts)     — descending ts so newest
                                            sorts first
rocksdb  key || sequence_number           — every write already carries
                                            a seqno; a Snapshot is just
                                            a pinned seqno
                                            (db/snapshot_impl.h:22)
memgraph version chains as deltas hanging  (storage/v2/mvcc.hpp) —
         off each vertex/edge object       graph-shaped MVCC: the chain
                                            lives per graph element
```

## 3. The visibility rule

The rule has two ingredients: a snapshot timestamp AND knowledge of
which transactions were still in flight when the snapshot was taken
(their writes carry smaller timestamps but must stay invisible).

```text
visible(version, txn) :=
      version.ts <= txn.read_ts                 (not from the future)
  AND version.txn NOT IN txn.active_set         (writer had committed
                                                 when we began)
  AND (version is not a tombstone -> value;
       tombstone -> key absent at this snapshot)
```

toydb spells the active-set variant out in its module doc
(`mvcc.rs:50-89`): each new transaction snapshots the set of live
transaction IDs; versions written by those IDs are invisible even
though their timestamps are older. RocksDB gets the same effect
structurally: sequence numbers publish in commit order (see the
`wal-group-commit` pair), so a pinned seqno alone suffices.

## 4. Step-by-step: one read-write transaction (badger's oracle)

From `badger-src/txn.go`:

```text
1. BEGIN:  read_ts = oracle.nextTxnTs - 1   (txn.go:78-86)
           wait until ALL commits <= read_ts are applied
           (txnMark.WaitForMark, txn.go:93) — the in-order
           publication barrier.
2. READS:  get(k) scans versions of k descending, returns newest
           with ts <= read_ts; records k's hash in txn.reads.
3. WRITES: buffered locally in txn.pendingWrites — invisible to
           everyone else.
4. COMMIT: oracle checks conflict: any committed txn with
           commit_ts > our read_ts that WROTE a key we READ?
           (txn.go:133-139: only committedTxn.ts > txn.readTs
           entries can conflict). If yes -> abort (SSI-style).
           If no  -> assign commit_ts = nextTxnTs++, stamp all
           pending writes, hand to group commit.
5. GC:     versions older than the oldest active read_ts are
           reclaimable (badger's readMark watermark tracks it).
```

## 5. Worked example 1 — two transactions, one key

```text
timeline (single key k, initial version k=a @ ts=5):

t1 (read_ts=9)                 t2 (read_ts=9)
   |                              |
   read k  -> a@5                 |
   write k=b (pending)            read k -> a@5     (t1's write invisible)
   commit  -> commit_ts=10        |
   |                              read k -> a@5     (STILL a@5: t2's
   |                              |        snapshot is 9, b is @10)
   |                              write k=c
   |                              commit -> CONFLICT? t1 committed @10 > 9
   |                              AND t2 read k which t1 wrote -> ABORT
                                  (retry at read_ts=10, sees b)

versions on disk afterward: k=a@5, k=b@10   (append-only, no overwrite)
```

The abort is the serializability guard; a plain snapshot-isolation
engine (RocksDB WriteCommitted, memgraph default) would let t2 commit
k=c@11 — the classic write-skew difference.

## 6. Worked example 2 — why read_ts waits for the commit mark

badger's `WaitForMark` (step 1) is the subtle line. Suppose commits at
ts=8 and ts=9 are assigned but ts=8's writes are still being applied
when a reader takes read_ts=9:

```text
without the wait:  reader at 9 sees ts=9's writes but NOT ts=8's
                   -> a history with a hole (9 visible, 8 missing)
with the wait:     reader blocks (~µs) until the watermark passes 9,
                   guaranteeing every commit <= 9 is fully applied
```

Same invariant as Pebble's in-order seqnum publication — visibility
must be a *prefix* of the commit order. Two engines, two mechanisms
(watermark vs pipeline order), one rule.

## 7. Where graph systems inherit this

- Memgraph hangs delta chains off each vertex/edge
  (`memgraph-src/src/storage/v2/mvcc.hpp`) — MVCC applied per graph
  element, so a Cypher traversal at snapshot S reconstructs each
  vertex's state at S while concurrent writers append deltas.
- TiKV layers Percolator-style MVCC over RocksDB
  (`tikv-src/components/engine_traits/src/mvcc_properties.rs` tracks
  version statistics for GC) — the graph DBs above it (NebulaGraph)
  inherit transactional adjacency for free.
- This repo's functional snapshots (immutable `Graph -> Graph` with Arc
  sharing, kb-graph experiment) are MVCC taken to the limit: EVERY
  commit is a full snapshot; the active-set machinery disappears
  because versions are whole immutable values.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| toydb | `reference-repos-corpus/toydb-src/src/storage/mvcc.rs` | clearest spelled-out active-set visibility rule (module doc, lines 39-107) |
| badger | `reference-repos-corpus/badger-src/txn.go` | oracle: read_ts watermark wait (78-93), commit conflict check (133-139) |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/snapshot_impl.h` | snapshot = pinned sequence number in a linked list (lines 22-41) |
| TiKV | `reference-repos-corpus/tikv-src/components/engine_traits/src/mvcc_properties.rs` | MVCC version statistics for GC over RocksDB |
| Memgraph | `reference-repos-competitors/memgraph-src/src/storage/v2/mvcc.hpp` | per-graph-element delta chains — MVCC shaped for traversals |

## 9. Cross-references

- Sibling patterns: `wal-group-commit` (in-order publication is the
  same invariant one level down); `cow-tree-snapshot-shape` (planned:
  LMDB/sled take the copy-on-write road to the same reader isolation).
- Paper: ARIES for recovery interplay; Percolator (TiKV's model) — see
  `research-papers-ledger.md`.
- 202606 digest overlap: the prior study noted "Memgraph is in-memory
  MVCC" as a one-liner; this pair supplies the actual rule and the two
  worked schedules it glossed over.

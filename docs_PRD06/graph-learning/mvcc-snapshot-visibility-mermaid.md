# MVCC Snapshot Visibility — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `mvcc-snapshot-visibility-ascii.md` / `mvcc-snapshot-visibility-mermaid.md` |
| One-line job | Let readers see a frozen, consistent world while writers keep writing — multiple timestamped versions per key plus a per-transaction rule for which version is visible |

## 1. The core move

A write never overwrites — it appends a version tagged with a
timestamp; a reader carries a snapshot timestamp selecting the newest
version it may see. Readers never block writers and vice versa; the
price is old versions lingering until GC.

```mermaid
flowchart LR
    K["key k"] --> V1["v1 @ ts=3"] --> V2["v2 @ ts=7"] --> V3["v3 @ ts=12"]
    R["reader read_ts=10"] -. "newest version with ts <= 10<br/>committed before reader began" .-> V2
```

## 2. How each engine encodes key+version

```mermaid
flowchart TD
    E[key+version encodings] --> T["toydb: Key::Version(user_key, version)<br/>bincode-ordered composite"]
    E --> B["badger: key || (max_u64 - commit_ts)<br/>descending, newest sorts first"]
    E --> RD["rocksdb: key || seqno —<br/>Snapshot = pinned seqno<br/>(db/snapshot_impl.h:22-41)"]
    E --> M["memgraph: delta chains hanging off<br/>each vertex/edge object —<br/>MVCC per graph element"]
```

## 3. The visibility rule

```mermaid
flowchart TD
    S[version candidate for txn] --> C1{"version.ts <= txn.read_ts ?"}
    C1 -- no: from the future --> INV[invisible]
    C1 -- yes --> C2{"version.txn in txn.active_set ?"}
    C2 -- yes: writer was in flight<br/>when we began --> INV
    C2 -- no --> C3{tombstone?}
    C3 -- yes --> ABS[key absent at this snapshot]
    C3 -- no --> VIS[visible value]
```

toydb spells the active-set variant out in
`reference-repos-corpus/toydb-src/src/storage/mvcc.rs` (module doc,
lines 39–107). RocksDB needs no active set: sequence numbers publish in
commit order, so a pinned seqno alone suffices.

## 4. One read-write transaction (badger's oracle)

```mermaid
sequenceDiagram
    participant T as txn
    participant O as oracle
    participant S as version store
    T->>O: BEGIN
    O-->>T: read_ts = nextTxnTs - 1 (txn.go:78-86)
    O->>O: txnMark.WaitForMark(read_ts) — all commits <= read_ts applied (txn.go:93)
    T->>S: get(k): scan versions desc, newest ts <= read_ts
    T->>T: buffer writes in pendingWrites (invisible to others)
    T->>O: COMMIT
    O->>O: conflict? any committed ts > read_ts wrote a key we read (txn.go:133-139)
    alt conflict
        O-->>T: ABORT (retry at newer read_ts)
    else clean
        O-->>T: commit_ts = nextTxnTs++
        T->>S: stamp writes, hand to group commit
    end
```

GC: versions older than the oldest active read_ts are reclaimable
(badger's readMark watermark).

## 5. Worked example — two transactions, one key

Initial: `k=a @ ts=5`; both txns begin at read_ts=9.

```mermaid
sequenceDiagram
    participant T1 as t1 (read_ts=9)
    participant T2 as t2 (read_ts=9)
    participant K as versions of k
    T1->>K: read k -> a@5
    T1->>T1: write k=b (pending)
    T2->>K: read k -> a@5 (t1's write invisible)
    T1->>K: commit -> k=b@10
    T2->>K: read k -> STILL a@5 (snapshot is 9, b is @10)
    T2->>T2: write k=c
    T2->>K: commit -> CONFLICT: t1 @10 > 9 wrote what t2 read -> ABORT
```

Disk afterward: `k=a@5, k=b@10` — append-only, no overwrite. The abort
is the serializability guard; plain snapshot isolation (RocksDB
WriteCommitted, memgraph default) would let t2 commit `k=c@11` — the
classic write-skew difference.

## 6. Worked example — why read_ts waits for the commit mark

Commits assigned at ts=8 and ts=9; ts=8's writes still being applied
when a reader takes read_ts=9:

```mermaid
flowchart TD
    subgraph BAD [without the wait]
        X["reader at 9 sees ts=9's writes<br/>but NOT ts=8's"] --> H["history with a hole"]
    end
    subgraph GOOD [with WaitForMark]
        Y["reader blocks ~µs until watermark<br/>passes 9"] --> P["every commit <= 9 fully applied:<br/>visibility is a PREFIX of commit order"]
    end
```

Same invariant as Pebble's in-order seqnum publication
(`wal-group-commit` pair) — two mechanisms, one rule.

## 7. Where graph systems inherit this

```mermaid
flowchart LR
    MV[MVCC] --> MG["Memgraph: delta chains per vertex/edge<br/>(storage/v2/mvcc.hpp) — traversal at<br/>snapshot S reconstructs each element at S"]
    MV --> TK["TiKV: Percolator MVCC over RocksDB<br/>(engine_traits mvcc_properties for GC)<br/>-> NebulaGraph inherits txn adjacency"]
    MV --> KB["this repo: functional snapshots —<br/>every commit a whole immutable Graph,<br/>active-set machinery disappears"]
```

## 8. Garbage collection — the bill for never overwriting

Old versions accumulate until no active snapshot can see them. Every
engine tracks the oldest live read timestamp (a watermark) and reclaims
below it:

```mermaid
flowchart LR
    W["oldest active read_ts = 42"] --> RCL["versions with a NEWER<br/>successor also <= 42:<br/>reclaimable"]
    W --> KEEP["newest version <= 42 per key:<br/>must stay (someone may read it)"]
    RCL --> HOW["badger: readMark watermark<br/>tikv: mvcc_properties per SST<br/>guide compaction-time GC<br/>memgraph: delta chain pruning"]
```

TiKV's trick is worth noting: it stores per-SST MVCC statistics
(`mvcc_properties.rs`) so compaction — the LSM's rewrite pass — doubles
as the MVCC garbage collector, folding two costs into one I/O.

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| toydb | `reference-repos-corpus/toydb-src/src/storage/mvcc.rs` | clearest active-set visibility rule (doc lines 39-107) |
| badger | `reference-repos-corpus/badger-src/txn.go` | oracle: watermark wait (78-93), conflict check (133-139) |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/snapshot_impl.h` | snapshot = pinned seqno in linked list |
| TiKV | `reference-repos-corpus/tikv-src/components/engine_traits/src/mvcc_properties.rs` | MVCC version stats for GC |
| Memgraph | `reference-repos-competitors/memgraph-src/src/storage/v2/mvcc.hpp` | per-graph-element delta chains |

## 10. Cross-references

- Sibling patterns: `wal-group-commit` (in-order publication, one level
  down); planned copy-on-write tree snapshots (LMDB/sled road to the
  same reader isolation).
- Papers: ARIES; Percolator — see `research-papers-ledger.md`.
- 202606 digest overlap: prior study had "Memgraph is in-memory MVCC"
  as a one-liner; this pair supplies the rule and worked schedules.

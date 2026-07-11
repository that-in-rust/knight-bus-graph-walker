# COW Tree Snapshot — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `cow-tree-snapshot-ascii.md` / `cow-tree-snapshot-mermaid.md` |
| One-line job | Make a whole B-tree transactional without any WAL: never modify a live page, copy the path from leaf to root, then flip one root pointer atomically |

## 1. The core move: path copying + root flip

A writer never touches a page a reader might see — it copies every page
it changes. New pages are unreachable garbage until one atomic act
publishes a new root. Crash anywhere: the old root still names a
complete, consistent tree. Recovery = "read the root pointer."

```mermaid
flowchart TD
    subgraph BEFORE [before commit — readers hold R]
        M1[meta] --> R[R] --> A[A] & B[B]
        A --> L1[L1] & L2[L2]
    end
    subgraph AFTER [after commit — one pointer flip]
        M2[meta] --> R2[R'] --> A2[A'] & B
        A2 --> L1x[L1'] & L2
    end
    BEFORE -. "write hits L1:<br/>copy L1->L1', A->A', R->R'<br/>B and L2 SHARED, untouched" .-> AFTER
```

This is MVCC where the version is an entire tree root — compare
`mvcc-snapshot-visibility`, whose versions are per key.

## 2. The dual meta page — where the flip lands

```mermaid
flowchart LR
    F["file layout"] --> M0["meta0: txid=41,<br/>root, freelist, checksum"]
    F --> M1["meta1: txid=42,<br/>root, freelist, checksum"]
    F --> P["page2, page3, ... (tree pages)"]
    M0 & M1 --> RULE["open rule: valid meta with<br/>HIGHEST txid wins"]
```

- LMDB: 2 meta pages — "a meta page is the start point for accessing a
  database snapshot"
  (`reference-repos-corpus/lmdb-src/libraries/liblmdb/mdb.c`, line 1355;
  P_META flag at 1066).
- bbolt: metaA/metaB; `db.meta()` returns the higher valid txid
  (`reference-repos-corpus/bbolt-src/db.go`, lines 1141–1144).
- redb: commit slot 0/1 plus a "god byte" naming the primary, per-slot
  checksums
  (`reference-repos-corpus/redb-src/src/tree_store/page_store/header.rs`,
  lines 13–41).

## 3. One write transaction

```mermaid
sequenceDiagram
    participant W as writer (single)
    participant FL as freelist
    participant D as disk
    W->>D: BEGIN: read valid meta -> root R, freelist F
    W->>FL: touch page P: alloc fresh page<br/>(free AND invisible to all live readers)
    W->>W: memcpy P -> P', modify P'
    W->>W: parent must point at P' -> touch parent, up to new root R'
    Note over W: depth-4 tree, 1 leaf update = 4 dirty pages
    W->>D: COMMIT: write all dirty pages to NEW locations, fsync #1
    W->>D: write new meta (root=R', txid+1, checksum)<br/>into the OTHER slot, fsync #2
    W->>FL: old pages (R, A, L1) tagged txid —<br/>reusable when no older reader remains
```

Anchors: `mdb_page_touch` (mdb.c:3015); bbolt dirty-page write-out
(`reference-repos-corpus/bbolt-src/tx.go`, lines 242, 519); redb's
path-copying mutator
(`reference-repos-corpus/redb-src/src/tree_store/btree_mutator.rs`);
sled's epoch-batched flip
(`reference-repos-corpus/sled-src/src/flush_epoch.rs`).

## 4. The free read path

```mermaid
flowchart LR
    RD["reader: load valid meta,<br/>remember root R, walk"] --> Z["no locks, no per-key version checks —<br/>consistency settled when R was published"]
    Z --> MM["lmdb extra: whole file mmap'd,<br/>pages returned as direct pointers —<br/>zero-copy, OS page cache is THE cache"]
    N["N readers at R40, R41, R42<br/>+ 1 writer building R43"] --> C["zero coordination;<br/>only shared state = reader table<br/>pinning freelist reuse"]
```

## 5. Worked example — write amplification of a 100-byte update

Depth-4 tree, 4 KB pages:

```mermaid
flowchart TD
    subgraph tiny [single 100 B update]
        T1["4 tree pages + 1 meta = 20 KB written"] --> T2["WA = 200x per txn"]
    end
    subgraph batch [1000 updates in one txn]
        B1["~1000 leaves + ~50 interior<br/>+ root + meta ~ 4.2 MB for 100 KB logical"] --> B2["WA ~ 42x, reads zero-copy throughout"]
    end
    tiny --> MORAL["COW punishes tiny txns, rewards batching —<br/>exact inverse of the LSM"]
    batch --> MORAL
```

## 6. Worked example — crash between the two fsyncs

Writer finished dirty pages (fsync #1), died mid-meta-write (txid=43):

```mermaid
flowchart TD
    B["reboot"] --> M0c["meta0: txid=42, checksum OK -> candidate"]
    B --> M1c["meta1: txid=43, checksum FAILS -> rejected (torn)"]
    M0c --> OPEN["open at txid=42"]
    OPEN --> ORPHAN["txid-43 pages exist but unreferenced —<br/>freelist of 42 never allocated them -> reclaimed"]
    ORPHAN --> ZERO["zero replay, zero corruption"]
```

redb's `primary_corrupted` repair path (header.rs:88–91) and bbolt's
highest-valid-txid `meta()` implement exactly this.

## 7. Where graph systems inherit this

```mermaid
flowchart LR
    COW[COW tree] --> LM["LMDB reader table:<br/>the reference for low-RAM mmap reading —<br/>this repo's mmap-walk kin"]
    COW --> KB["this repo kb-graph: Arc path copying,<br/>root flip = swapping one Arc —<br/>the in-memory limit of the pattern"]
    COW --> KZ["Kuzu / DuckDB: COW column segments +<br/>manifest flip — same shape at file granularity"]
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| LMDB | `reference-repos-corpus/lmdb-src/libraries/liblmdb/mdb.c` | dual meta (1355), P_META (1066), mdb_page_touch (3015) |
| bbolt | `reference-repos-corpus/bbolt-src/db.go` | highest-valid-txid meta selection (1141-1144) |
| bbolt | `reference-repos-corpus/bbolt-src/tx.go` | dirty-page write-out at commit |
| redb | `reference-repos-corpus/redb-src/src/tree_store/page_store/header.rs` | commit slots + god byte, byte-documented |
| redb | `reference-repos-corpus/redb-src/src/tree_store/btree_mutator.rs` | Rust path-copying mutation |
| sled | `reference-repos-corpus/sled-src/src/flush_epoch.rs` | epoch-batched flip generalization |

## 9. Cross-references

- Sibling patterns: `mvcc-snapshot-visibility` (per-key vs whole-tree
  versions); `lsm-compaction-tradeoff` (opposing write philosophy);
  `wal-group-commit` (what COW eliminates and gives up).
- Paper trail: append-only B-tree lineage — `research-papers-ledger.md`.
- 202606 digest overlap: none — B-tree engines were uncovered before
  this pair.

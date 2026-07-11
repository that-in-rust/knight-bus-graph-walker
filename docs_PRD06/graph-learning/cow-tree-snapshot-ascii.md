# COW Tree Snapshot — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `cow-tree-snapshot-ascii.md` / `cow-tree-snapshot-mermaid.md` |
| One-line job | Make a whole B-tree transactional without any WAL: never modify a live page, copy the path from leaf to root, then flip one root pointer atomically |

## 1. The job

The LSM answer to durability is a log (see `wal-group-commit`). The
copy-on-write B-tree answer is: make the tree itself the log. A writer
never touches a page a reader might be seeing — it copies each page it
needs to change. All the new pages are unreachable garbage until the
single, atomic act of publishing a new root. Crash at any moment:
the old root still points at a complete, consistent tree. There is
nothing to replay — recovery is "read the root pointer."

```text
before commit:               after commit (root flip):

     meta -> R                    meta -> R'
            / \                          / \
           A   B                        A'  B      <- B shared, untouched!
          / \                          / \
         L1  L2                      L1'  L2       <- L2 shared
              write hits L1 => copy L1->L1', A->A', R->R'
```

Readers holding R keep a perfect snapshot for free — this is MVCC where
the "version" is an entire tree root (compare
`mvcc-snapshot-visibility`, where versions are per key).

## 2. Raw data shape: the dual meta page

The atomic flip needs a place to land. Every engine reserves two (or
more) fixed slots at the start of the file and alternates between them,
each stamped with a transaction ID and a checksum:

```text
file:  [ meta0 | meta1 | page2 | page3 | page4 | ... ]
          ^        ^
          txid=41   txid=42  <- valid meta with HIGHEST txid wins

lmdb:   2 meta pages; "A meta page is the start point for accessing
        a database snapshot" (mdb.c:1355, P_META flag at 1066)
bbolt:  metaA/metaB; db.meta() returns the one with the higher txid
        that passes validation (db.go:1141-1144)
redb:   super-header with commit slot 0 / slot 1, a "god byte" naming
        the primary, per-slot checksums
        (tree_store/page_store/header.rs:13-41)
```

Torn write on one meta? The other is still intact — the checksum + take
-the-highest-valid-txid rule IS crash recovery, in ~10 lines.

## 3. Step-by-step: one write transaction

```text
1. BEGIN: read the current valid meta -> root page R, freelist F.
   Only ONE write txn at a time (lmdb/bbolt: single writer lock).
2. First modification of any page P on the path: "touch" it —
   allocate a fresh page (from freelist if a page is free AND not
   visible to any live reader txid), memcpy P, modify the copy.
   lmdb: mdb_page_touch (mdb.c:3015); bbolt tracks the copies as
   its dirty-page set (tx.go:242,519 writes them out).
3. Touching propagates upward: the parent must now point at the new
   child page, so the parent is touched too, up to a new root R'.
   An update of one leaf in a depth-4 tree dirties exactly 4 pages.
4. COMMIT: write all dirty pages to their NEW locations (no live page
   overwritten), fsync; write the new meta (root=R', txid+1,
   checksum) into the OTHER meta slot, fsync again.
5. Old pages (R, A, L1) go on the freelist tagged with the txid —
   reusable only once no reader older than that txid remains.
   (bbolt's freelist, db.go:15-29; redb's "freed table" root in each
   commit slot, header.rs:27.)
```

Two fsyncs per commit — which is why COW engines batch transactions
(and why sled generalizes the flip with epoch-based flushes,
`flush_epoch.rs`).

## 4. Read path: why readers cost nothing

A read transaction is: load the valid meta, remember root R, walk. No
locks taken, no versions checked per key — the entire consistency
argument was settled the moment R was published. lmdb adds the final
twist: the whole file is mmap'd and pages are handed to callers as
direct pointers into the map — zero-copy reads, the OS page cache is
the only cache.

```text
reader concurrency:   N readers at roots R40, R41, R42 + 1 writer
                      building R43 — zero coordination between them;
                      the only shared state is the reader table that
                      pins freelist reuse.
```

## 5. Worked example 1 — write amplification of a 100-byte update

Depth-4 tree, 4 KB pages:

```text
COW:  touch leaf + 2 interior + root  = 4 pages = 16 KB
      + meta page                      =  4 KB
      => 20 KB written for 100 bytes  (WA = 200x per txn!)
LSM:  100 bytes to WAL + memtable; amortized compaction ~17x
      (see lsm-compaction-tradeoff)

BUT batch 1000 updates in one txn (they share paths):
COW:  ~1000 leaves + ~50 interior + root + meta ~ 4.2 MB
      for 100 KB of logical change => WA ~ 42x, and reads stayed
      zero-copy the whole time.
```

Moral: COW trees punish tiny transactions and reward batching — the
exact inverse of the LSM, which absorbs tiny writes gracefully and
pays later in compaction.

## 6. Worked example 2 — crash between the two fsyncs

Writer finished writing dirty pages (fsync #1), crashed while writing
meta slot 1 (txid=43). On reboot:

```text
meta0: txid=42, checksum OK      -> candidate
meta1: txid=43, checksum FAILS   -> rejected (torn write)
=> open at txid=42. The 43-pages exist in the file but nothing
   references them; they're reclaimed because the freelist of txid=42
   never allocated them. Zero replay, zero corruption.
```

redb makes the same story explicit with its `primary_corrupted` repair
path (header.rs:88-91); bbolt's meta() implements the identical
highest-valid-txid rule (db.go:1141-1144).

## 7. Where graph systems inherit this

- LMDB is the storage engine under many graph tools' metadata stores;
  its reader-table design is the reference for low-RAM mmap reading —
  directly relevant to this repo's mmap-walk architecture.
- This repo's kb-graph experiment (immutable `Graph -> Graph` with Arc
  structure sharing) is the in-memory limit of this pattern: path
  copying on a persistent data structure, root flip = swapping one Arc.
  The disk pattern here shows what it costs to make that durable.
- Kuzu and DuckDB use variations: copy-on-write column segments with a
  manifest flip — same shape at file granularity instead of page.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| LMDB | `reference-repos-corpus/lmdb-src/libraries/liblmdb/mdb.c` | the canon: dual meta (line 1355), P_META (1066), mdb_page_touch (3015) |
| bbolt | `reference-repos-corpus/bbolt-src/db.go` | highest-valid-txid meta selection (1141-1144), freelist backends |
| bbolt | `reference-repos-corpus/bbolt-src/tx.go` | dirty-page write-out at commit (242, 519) |
| redb | `reference-repos-corpus/redb-src/src/tree_store/page_store/header.rs` | commit slots + god byte, documented byte-by-byte (13-41) |
| redb | `reference-repos-corpus/redb-src/src/tree_store/btree_mutator.rs` | Rust path-copying mutation |
| sled | `reference-repos-corpus/sled-src/src/flush_epoch.rs` | epoch-batched flip — the pattern generalized past two slots |

## 9. Cross-references

- Sibling patterns: `mvcc-snapshot-visibility` (per-key versions vs
  whole-tree versions — two roads to reader isolation);
  `lsm-compaction-tradeoff` (the opposing write-path philosophy);
  `wal-group-commit` (what COW eliminates, and what it gives up).
- Paper trail: LMDB descends from the append-only B-tree lineage
  (see `research-papers-ledger.md`).
- 202606 digest overlap: none — prior study never covered B-tree
  engines; this pair opens the family.

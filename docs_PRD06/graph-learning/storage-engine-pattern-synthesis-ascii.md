# Storage-Engine Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | category synthesis (storage) |
| Covers | patterns 1-6: LSM compaction, WAL group commit, roaring bitmaps, MVCC visibility, COW trees, Bloom filters |
| One-line thesis | Every storage engine is one answer to the same question — how do you make disk look atomic, versioned, and fast when it is none of those things — and the six patterns are the recurring answer parts |

## 1. The category in one picture

Six patterns from 33 storage-engine repos, but really two rival
write-path philosophies plus four shared tools:

```text
                 WRITE-PATH PHILOSOPHY
                /                     \
   LOG-STRUCTURED (LSM)          COPY-ON-WRITE TREE
   append, sort later            copy path, flip root
   rocksdb pebble badger         lmdb bbolt redb sled
   mini-lsm fjall slatedb        |
   |                             |
   needs: WAL group commit       needs: NO wal at all
          (pattern 2)                   (dual meta IS recovery)
          compaction policy             freelist + reader pinning
          (pattern 1)
          bloom filters                 nothing — one tree,
          (pattern 6, many runs)        no runs to skip
                \                     /
                 SHARED SUPERSTRUCTURE
                 MVCC visibility (pattern 4) — both need
                 snapshot reads; LSM does it per key
                 (seqno/ts), COW does it per tree root
                 roaring bitmaps (pattern 3) — the ID-set
                 kernel everyone imports for label/filter sets
```

## 2. The grand tradeoff table

Everything in this category is a position on three amplifications
(write WA, read RA, space SA) plus recovery complexity:

```text
                    WA         RA          SA      recovery
LSM leveled         ~17x       low (1-2    ~1.1x   WAL replay
                               runs/level)
LSM tiered          ~4x        high (N     ~2x     WAL replay
                               runs)
COW tree, tiny txn  ~200x      1 (one      ~1.5x   NONE (read
COW tree, batched   ~40x       tree)       (frag)  meta page)
```

Numbers from the worked examples in patterns 1 and 5. The lesson that
generalizes: no engine escapes the triangle — they choose a corner and
then deploy the shared tools (blooms, bitmaps, MVCC) to soften the
corner they gave up.

## 3. One invariant appeared three times

The deepest recurrence in the category — **visibility must be a prefix
of commit order** — showed up independently in three mechanisms:

```text
pattern 2: pebble/rocksdb publish seqnums IN ORDER even though
           WAL writes complete out of order
pattern 4: badger's oracle makes new readers WAIT for the commit
           watermark (txnMark.WaitForMark) before handing out read_ts
pattern 5: COW readers can't see a torn state because the root flip
           is a single atomic pointer/meta write

three codebases, three vocabularies, one rule:
     never let a reader observe commit N+1 without commit N.
```

Anyone building a storage engine (or verifying one differentially —
the convergence-loop thesis) should treat this as THE property to
test: it is where the subtle bugs live in all three families.

## 4. What immutability buys, four ways

The second recurrence: every pattern gets simpler when the artifact is
immutable.

```text
SSTables (p1):  never edited -> compaction can read them lock-free
Blooms   (p6):  file immutable -> filter never updated -> no
                deletion support needed (the classic Bloom weakness
                is simply never exercised)
COW pages(p5):  never overwritten -> crash recovery is free
MVCC     (p4):  versions append-only -> readers need no locks
Roaring  (p3):  serialized bitmaps mmap-able and directly operable
                (ops_with_serialized.rs) BECAUSE they never mutate
```

The cost is always the same too: garbage. Compaction, freelists, MVCC
GC, and version pruning are the same job — reclaiming superseded
immutable stuff — wearing four costumes.

## 5. Which engine for which graph job

The reason this category was studied first: graph systems are built ON
these engines, and the choice leaks upward.

```text
graph workload                     best-fit pattern stack
--------------------------------  --------------------------------
write-heavy edge ingest            LSM tiered + blooms (badger under
(get-or-create storm)              dgraph — pattern 6 example 2)
read-mostly traversal,             COW tree + mmap (lmdb: zero-copy
low RAM                            pages, OS cache only — this
                                   repo's mmap-walk kin)
analytics label/filter sets        roaring bitmaps (falkordb,
                                   lucene, duckdb all import them)
transactional graph mutation       MVCC delta chains per element
                                   (memgraph) or per-key ts (tikv
                                   under nebula)
```

## 6. Worked example — composing the patterns into one design

This repo's target (billion-edge graph, 8 GB RAM, read-dominant,
occasional bulk ingest) composed from the category:

```text
1. bulk ingest lands in an LSM-ish staging area (tiered — WA ~4x,
   ingest-friendly; pattern 1) with WAL group commit (pattern 2)
2. a background pass "compacts" staged edges into immutable mmap'd
   CSR segments — the COW idea at segment granularity: build new,
   flip a manifest, never edit live (pattern 5)
3. per-segment blooms over external IDs make cross-segment ID
   resolution RAM-only (pattern 6); label sets are roaring bitmaps
   serialized inside segments (pattern 3)
4. readers pin a manifest version = whole-graph snapshot (pattern 4,
   COW-style, whole-root variant — no per-key ts needed because
   segments are immutable)

RAM bill for 1B edges: blooms ~1.25 GB + manifest + bitmap headers
— everything else stays on disk until touched. The category's
patterns are sufficient to specify the entire storage layer.
```

## 7. What was NOT covered (honest gaps)

- B-epsilon trees / fractal trees (write-optimized B-trees): no strong
  corpus witness (TokuDB descendant PerconaFT not in ledger).
- FASTER's hybrid-log (epoch-protected mutable region): single witness
  (`FASTER-src`), so no pair published — one-witness rule held.
- WiredTiger's in-memory page deltas + reconciliation: worth a pair if
  a second witness emerges when graph-db internals are mapped.
- Succinct structures (sdsl-lite): deferred to the graph-analytics
  category where rank/select meets CSR.

## 8. Citing repos (category roll-up)

| Repo | Patterns witnessed | Signature path |
| --- | --- | --- |
| rocksdb | 1, 2, 4, 6 | `reference-repos-corpus/rocksdb-src/db/compaction/compaction_picker_level.cc` |
| pebble | 1, 2 | `reference-repos-corpus/pebble-src/commit.go` |
| mini-lsm | 1, 6 | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/table/bloom.rs` |
| fjall lsm-tree | 1, 6 | `reference-repos-corpus/lsm-tree-src/src/table/filter/blocked_bloom` |
| slatedb | 1, 2 | `reference-repos-corpus/slatedb-src/slatedb/src/wal_buffer.rs` |
| badger | 4, 6 | `reference-repos-corpus/badger-src/txn.go` |
| toydb | 4 | `reference-repos-corpus/toydb-src/src/storage/mvcc.rs` |
| tikv | 4 | `reference-repos-corpus/tikv-src/components/engine_traits/src/mvcc_properties.rs` |
| lmdb | 5 | `reference-repos-corpus/lmdb-src/libraries/liblmdb/mdb.c` |
| bbolt | 5 | `reference-repos-corpus/bbolt-src/db.go` |
| redb | 5 | `reference-repos-corpus/redb-src/src/tree_store/page_store/header.rs` |
| sled | 5 | `reference-repos-corpus/sled-src/src/flush_epoch.rs` |
| CRoaring | 3 | `reference-repos-corpus/CRoaring-src/src/containers` |
| roaring-rs | 3 | `reference-repos-corpus/roaring-rs-src/roaring/src/bitmap/container.rs` |
| sqlite | 2 | `reference-repos-corpus/sqlite-src/src/wal.c` |
| turso | 2 | `reference-repos-corpus/turso-src/core/storage/wal.rs` |

## 9. Cross-references

- Pattern pairs 1-6 in `pattern-index.md`.
- Next category (graph-analytics) picks up where §7 left off: CSR
  layout, rank/select, frontier management — the structures these
  engines store.

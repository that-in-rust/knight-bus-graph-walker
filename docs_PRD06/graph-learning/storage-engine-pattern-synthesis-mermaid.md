# Storage-Engine Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | category synthesis (storage) |
| Covers | patterns 1-6: LSM compaction, WAL group commit, roaring bitmaps, MVCC visibility, COW trees, Bloom filters |
| One-line thesis | Every storage engine is one answer to the same question — how do you make disk look atomic, versioned, and fast when it is none of those things — and the six patterns are the recurring answer parts |

## 1. The category in one picture

```mermaid
flowchart TD
    Q["write-path philosophy"] --> LSM["LOG-STRUCTURED (LSM)<br/>append, sort later<br/>rocksdb pebble badger<br/>mini-lsm fjall slatedb"]
    Q --> COW["COPY-ON-WRITE TREE<br/>copy path, flip root<br/>lmdb bbolt redb sled"]
    LSM --> N1["needs: WAL group commit (p2)<br/>compaction policy (p1)<br/>bloom filters (p6, many runs)"]
    COW --> N2["needs: NO wal —<br/>dual meta IS recovery;<br/>freelist + reader pinning"]
    N1 & N2 --> SH["SHARED SUPERSTRUCTURE<br/>MVCC visibility (p4): LSM per key,<br/>COW per tree root<br/>roaring bitmaps (p3): the ID-set kernel<br/>everyone imports"]
```

## 2. The grand tradeoff

```mermaid
quadrantChart
    title Write amplification vs read amplification (space in labels)
    x-axis low read amp --> high read amp
    y-axis low write amp --> high write amp
    "LSM tiered (SA ~2x, WAL replay)": [0.8, 0.25]
    "LSM leveled (SA ~1.1x, WAL replay)": [0.3, 0.55]
    "COW batched (SA ~1.5x, no replay)": [0.15, 0.7]
    "COW tiny txn (SA ~1.5x, no replay)": [0.15, 0.95]
```

Numbers from patterns 1 and 5 worked examples (leveled ~17x WA, tiered
~4x, COW tiny-txn ~200x, COW batched ~40x). No engine escapes the
WA/RA/SA triangle — each picks a corner, then softens the abandoned
corners with the shared tools (blooms, bitmaps, MVCC).

## 3. One invariant, three appearances

**Visibility must be a prefix of commit order** — independently
rediscovered by three mechanisms:

```mermaid
flowchart LR
    INV["never let a reader observe<br/>commit N+1 without commit N"]
    INV --> A["p2: pebble/rocksdb publish seqnums<br/>IN ORDER though WAL writes<br/>complete out of order"]
    INV --> B["p4: badger oracle makes readers WAIT<br/>for the commit watermark<br/>(txnMark.WaitForMark) before read_ts"]
    INV --> C["p5: COW root flip is one atomic write —<br/>a torn state is unreachable"]
    A & B & C --> TEST["for differential verification:<br/>THE property to test — where subtle<br/>bugs live in all three families"]
```

## 4. What immutability buys, four ways

```mermaid
flowchart TD
    IMM["immutable artifact"] --> S1["SSTables (p1): compaction<br/>reads lock-free"]
    IMM --> S2["Blooms (p6): never updated —<br/>deletion weakness never exercised"]
    IMM --> S3["COW pages (p5): crash<br/>recovery is free"]
    IMM --> S4["MVCC versions (p4): readers<br/>need no locks"]
    IMM --> S5["Roaring (p3): serialized bitmaps<br/>mmap-able and directly operable"]
    S1 & S2 & S3 & S4 & S5 --> COST["universal bill: GARBAGE —<br/>compaction, freelists, MVCC GC,<br/>version pruning = one job,<br/>four costumes"]
```

## 5. Which engine for which graph job

```mermaid
flowchart LR
    W1["write-heavy edge ingest<br/>(get-or-create storm)"] --> E1["LSM tiered + blooms<br/>(badger under dgraph)"]
    W2["read-mostly traversal, low RAM"] --> E2["COW tree + mmap<br/>(lmdb zero-copy pages —<br/>this repo's mmap-walk kin)"]
    W3["analytics label/filter sets"] --> E3["roaring bitmaps<br/>(falkordb, lucene, duckdb)"]
    W4["transactional graph mutation"] --> E4["MVCC delta chains per element<br/>(memgraph) or per-key ts<br/>(tikv under nebula)"]
```

## 6. Worked example — composing the patterns into one design

Target: billion-edge graph, 8 GB RAM, read-dominant, bulk ingest.

```mermaid
flowchart TD
    I["bulk ingest"] --> ST["LSM-ish staging, tiered (WA ~4x, p1)<br/>+ WAL group commit (p2)"]
    ST --> CP["background pass compacts staged edges into<br/>immutable mmap'd CSR segments —<br/>COW at segment granularity:<br/>build new, flip manifest, never edit live (p5)"]
    CP --> RD["per-segment blooms over external IDs:<br/>cross-segment ID resolution RAM-only (p6);<br/>label sets = roaring bitmaps in segments (p3)"]
    RD --> SN["readers pin a manifest version =<br/>whole-graph snapshot (p4, whole-root variant —<br/>no per-key ts: segments immutable)"]
    SN --> BILL["RAM bill @ 1B edges: blooms ~1.25 GB +<br/>manifest + bitmap headers; rest on disk<br/>until touched — the six patterns fully<br/>specify the storage layer"]
```

## 7. Honest gaps (not covered)

```mermaid
flowchart LR
    G[gaps] --> G1["B-epsilon / fractal trees:<br/>no strong corpus witness"]
    G --> G2["FASTER hybrid-log: single witness —<br/>one-witness rule held, no pair"]
    G --> G3["WiredTiger page deltas +<br/>reconciliation: pair if a 2nd<br/>witness emerges in graph-db mapping"]
    G --> G4["succinct rank/select (sdsl-lite):<br/>deferred to graph-analytics,<br/>where it meets CSR"]
```

## 8. The category as a verification target

For the convergence-loop thesis (docs_PRD06 README): if generated code
must match a storage engine, these are the observable surfaces per
pattern — what a differential harness can and cannot see:

```mermaid
flowchart TD
    subgraph OBS [observable via API diffing — loop converges alone]
        O1["p4 visibility: read-your-writes,<br/>snapshot repeatability, prefix order"]
        O2["p3 bitmaps: exact set semantics,<br/>bit-for-bit serialized format"]
        O3["p1/p6 read results: correctness<br/>(which value), not which file was skipped"]
    end
    subgraph HID [invisible to API diffing — needs engineered harnesses]
        H1["p2 group commit: fsync count —<br/>needs io tracing or failpoint crash tests"]
        H2["p5 torn-meta recovery: needs<br/>crash-point injection between fsyncs"]
        H3["p1 WA/SA: needs io accounting,<br/>not query results"]
    end
    OBS --> LOOP["cheap: let generation grind"]
    HID --> HUMAN["expensive: the human-built<br/>part of the harness"]
```

This split — which pattern properties are free to verify and which
need instrumentation — is the category's direct payment toward the
rewrite program.

## 9. Citing repos (category roll-up)

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

## 10. Cross-references

- Pattern pairs 1-6 in `pattern-index.md`.
- Next category (graph-analytics): CSR layout, rank/select, frontier
  management — the structures these engines store.

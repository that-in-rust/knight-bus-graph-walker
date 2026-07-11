# LSM Compaction Tradeoff — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `lsm-compaction-tradeoff-ascii.md` / `lsm-compaction-tradeoff-mermaid.md` |
| One-line job | Decide when and how to merge immutable sorted runs so reads stay fast without writes drowning in rewrite work |

## 1. The write path that creates the problem

An LSM tree never updates in place; flushes pile up immutable sorted
files, and every reader must consult every file that might hold its key.
Compaction merges the pile down — at the price of rewriting bytes.

```mermaid
flowchart LR
    W[put k,v] --> WAL[WAL append]
    WAL --> MT[memtable<br/>sorted, in RAM]
    MT -- full --> F[flush]
    F --> L0[L0 SSTable<br/>overlapping files]
    L0 -- compaction --> L1[L1 sorted run]
    L1 -- compaction --> L2[L2 sorted run]
    L2 -- compaction --> LN[LN sorted run]
```

## 2. The triangle

Every engine picks a point trading three amplifications:

```mermaid
flowchart TD
    WA[write amplification<br/>bytes rewritten per byte ingested]
    RA[read amplification<br/>runs probed per point read]
    SA[space amplification<br/>dead versions on disk]
    WA ---|leveled: high WA, low RA| RA
    RA ---|tiered: low WA, high RA| SA
    SA ---|slack variable for both| WA
```

## 3. Leveled vs tiered — structure

```mermaid
flowchart TB
    subgraph LEVELED [Leveled — one run per level, fanout T=10]
        A0["L0: f | f | f | f (overlap)"]
        A1["L1: one run, 300 MB"]
        A2["L2: one run, 3 GB"]
        A3["L3: one run, 30 GB"]
        A0 --> A1 --> A2 --> A3
    end
    subgraph TIERED [Tiered — up to T full runs per tier]
        B0["tier0: run | run | run | run"]
        B1["tier1: run | run | run"]
        B2["tier2: run | run"]
        B0 -- "merge ALL T runs at once" --> B1 --> B2
    end
```

- Leveled read: memtable + each L0 file + one file per level (bounded).
- Tiered read: every run in every tier (T × tiers probes).
- Leveled write: each byte re-merged ~T/2 times per level it descends.
- Tiered write: each byte rewritten once per tier.

## 4. The leveled picker decision (RocksDB / Pebble shape)

```mermaid
flowchart TD
    S[score every level] --> S0["L0 score = num_L0_files / trigger<br/>(count, not bytes)"]
    S --> SI["Li score = bytes(Li) / max_bytes(Li)"]
    S0 --> SORT[sort scores descending]
    SI --> SORT
    SORT --> PICK{max score >= 1 ?}
    PICK -- no --> IDLE[no compaction needed]
    PICK -- yes --> FILE[pick file overlapping<br/>fewest next-level bytes]
    FILE --> MERGE[merge-sort with overlapping<br/>files of next level]
    MERGE --> DROP[drop shadowed versions +<br/>tombstones older than oldest snapshot]
    DROP --> INSTALL[atomically install new<br/>level metadata, delete inputs]
```

Source anchors: score formula in
`reference-repos-corpus/rocksdb-src/db/compaction/compaction_picker_level.cc`
(lines 229–232); Pebble's per-level fill factors in
`reference-repos-corpus/pebble-src/compaction_picker.go` (~line 994, with
a dedicated `scores[0]` L0 entry at line 1000).

## 5. The tiered trigger (mini-lsm / SlateDB shape)

```mermaid
sequenceDiagram
    participant Flush as flush loop
    participant State as tier state
    participant Comp as compactor
    Flush->>State: new run appended to tier0
    State->>State: num_runs(tier0) += 1
    State->>Comp: num_tiers > options.num_tiers ?
    alt space-amp bound exceeded
        Comp->>Comp: full compaction (all tiers -> one run)
    else count trigger fired
        Comp->>Comp: merge the T runs of one tier -> next tier
    end
    Comp->>State: install merged run, drop inputs
```

Source anchors: `generate_compaction_task` in
`reference-repos-corpus/mini-lsm-src/mini-lsm/src/compact/tiered.rs`
(count trigger + `max_size_amplification_percent` guard);
`reference-repos-corpus/slatedb-src/slatedb/src/size_tiered_compaction.rs`
(same policy where write amp costs object-store money);
`reference-repos-corpus/lsm-tree-src/src/compaction/tiered.rs`
(fjall's production Rust strategy).

## 6. Worked example — 1 GB ingested, T=10, 4 levels

```mermaid
flowchart LR
    subgraph L [leveled ~17x]
        LI[1 GB in] --> LWAL[+1 GB WAL] --> LFL[+1 GB flush] --> LRW["+15 GB re-merges<br/>(3 levels x ~5 rewrites)"]
    end
    subgraph T [tiered ~5x]
        TI[1 GB in] --> TWAL[+1 GB WAL] --> TFL[+1 GB flush] --> TRW["+3 GB tier merges<br/>(once per tier)"]
    end
```

The read side inverts: the leveled store answers a point read in ≤ 8 file
probes; the tiered store may touch 16 runs — which is why tiered engines
lean hard on bloom filters (sibling pattern).

## 7. Why L0 is count-triggered — worked example

20 L0 files of 2 MB each (40 MB total), trigger = 4; L1 at 280/300 MB:

```mermaid
flowchart TD
    C["L0 score = 20/4 = 5.0"] --> P[picker chooses L0->L1]
    D["L1 score = 280/300 = 0.93"] --> P
    P --> R["reads drop from 20+3 probes to 1+3"]
```

L0 files overlap, so their cost to readers is per-file regardless of
size — bytes-based scoring would starve exactly the level that hurts
reads most.

## 8. Where graph systems inherit this

```mermaid
flowchart LR
    RD[RocksDB leveled] --> NEB[NebulaGraph / TiKV adjacency]
    BG[Badger tiered-ish] --> DG[Dgraph postings]
    TM[Lucene TieredMergePolicy] --> FTS[segment merging = same policy,<br/>different name]
    KB[this repo: journal/overlay/base<br/>visibility tiers] -. structurally a 2-tier<br/>tiered LSM .-> TM
```

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/compaction/compaction_picker_level.cc` | production leveled picker; score formula |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/compaction/compaction_picker_fifo.cc` | FIFO contrast policy (pure deletion) |
| Pebble | `reference-repos-corpus/pebble-src/compaction_picker.go` | Go leveled picker, per-level fill factors |
| mini-lsm | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/compact/leveled.rs` | teaching-scale leveled tasks |
| mini-lsm | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/compact/tiered.rs` | teaching-scale tiered engine |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/compaction/tiered.rs` | Rust production tiered strategy |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/compaction/leveled` | Rust production leveled strategy |
| SlateDB | `reference-repos-corpus/slatedb-src/slatedb/src/size_tiered_compaction.rs` | tiered on object storage |
| SlateDB | `reference-repos-corpus/slatedb-src/slatedb/src/compactor_state.rs` | compaction as state machine |

## 10. Cross-references

- Paper: O'Neil et al., "The Log-Structured Merge-Tree" — see
  `research-papers-ledger.md`.
- Prior 202606 digest covered LSM only as Dgraph's substrate; this pair
  adds the picker mechanics and the leveled/tiered duality.
- Sibling patterns: WAL group commit; bloom-filter read shortcut;
  immutable segment merging (FTS twin of this policy).

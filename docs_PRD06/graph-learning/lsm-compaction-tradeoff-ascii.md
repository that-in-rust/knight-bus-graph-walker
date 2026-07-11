# LSM Compaction Tradeoff — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `lsm-compaction-tradeoff-ascii.md` / `lsm-compaction-tradeoff-mermaid.md` |
| One-line job | Decide when and how to merge immutable sorted runs so reads stay fast without writes drowning in rewrite work |

## 1. The job

An LSM tree never updates data in place. Writes land in a memtable, get
flushed as immutable sorted files (SSTables), and accumulate. Every reader
must consult every file that might hold its key, so the file count is a
read tax. Compaction merges files to pay that tax down — but merging
rewrites bytes that were already written once, which is write
amplification. Every LSM engine is one point on a triangle:

```text
              write amplification
                     /\
                    /  \
                   /    \        pick 2, pay with the 3rd:
                  /      \       - leveled: cheap reads, pricey writes
                 /        \      - tiered:  cheap writes, pricey reads
                /__________\     - both keep space amp as the slack var
     read amplification    space amplification
```

## 2. Raw data shape

The unit being managed is the **sorted run**: a set of SSTables with
non-overlapping key ranges that can be binary-searched as one logical
sorted map. L0 is special — its files come straight from memtable flushes
and DO overlap each other, so each L0 file is its own run.

```text
write path:   put(k,v) -> WAL -> memtable -> flush -> L0 SSTable
                                                        |
                                                   compaction
                                                        v
                                            L1, L2, ... LN sorted runs
```

## 3. The two layouts

### Leveled (RocksDB default, Pebble, mini-lsm `leveled.rs`)

Each level below L0 is ONE sorted run; each level is ~T times (fanout,
usually 10) larger than the one above. Compacting means picking a file in
L(i) and merging it into the overlapping files of L(i+1).

```text
L0:  [f][f][f][f]        <- overlapping flush files (count-triggered)
L1:  [----- one sorted run, 300 MB ------]
L2:  [--------- one sorted run, 3 GB ----------]
L3:  [------------- one sorted run, 30 GB -------------]

read(k): check memtable, then every L0 file, then ONE file per level
         => read amp ~ #L0 + #levels          (small, bounded)
write:   each byte is rewritten ~T/2 times per level it descends
         => write amp ~ T x #levels ~ 10-30x  (large)
```

### Tiered / size-tiered (Cassandra lineage; mini-lsm `tiered.rs`, fjall `tiered.rs`, slatedb `size_tiered_compaction.rs`)

Each level holds UP TO T full sorted runs of similar size. Compaction waits
until T runs pile up, then merges all of them into one run in the next
tier — every byte is rewritten only once per tier.

```text
tier0: [run][run][run][run]      <- T runs, each a full sorted map
tier1: [ run ][ run ][ run ]
tier2: [   run   ][   run   ]

read(k): check EVERY run in EVERY tier
         => read amp ~ T x #tiers            (large)
write:   each byte rewritten once per tier
         => write amp ~ #tiers ~ 3-6x        (small)
```

## 4. Step-by-step: how a leveled picker decides

Distilled from RocksDB's `LevelCompactionBuilder` and Pebble's
`pickAuto` (same shape, different language):

```text
1. score every level:
     L0 score  = num_L0_files / level0_file_num_compaction_trigger
     Li score  = total_bytes(Li) / max_bytes_for_level(Li)
   (rocksdb-src/db/compaction/compaction_picker_level.cc:229-232;
    pebble computes the same as a "fill factor" per candidateLevelInfo,
    pebble-src/compaction_picker.go:994-1014)
2. sort levels by score descending; pick the first with score >= 1.
3. inside that level, pick the file that overlaps the FEWEST bytes in
   the next level (cheapest merge first).
4. merge-sort the picked file(s) with the overlapping next-level files;
   drop shadowed versions and tombstones older than the oldest snapshot.
5. install the result atomically as a new version of the level metadata;
   delete the input files.
```

Tiered picking is simpler — the trigger is a count, not a ratio:
`generate_compaction_task` in mini-lsm's tiered engine fires when
`num_tiers > options.num_tiers`, guarded by a space-amplification bound
(`max_size_amplification_percent`) exactly like slatedb's
`size_tiered_compaction.rs`.

## 5. Worked example 1 — write amplification of 1 GB ingested

Assume fanout T=10, 4 levels populated, uniform random keys.

```text
leveled:
  each byte descends L0->L1->L2->L3, and while resident in Li it is
  re-merged ~T/2 = 5 times as sibling files push into its level:
  WA ~ 1 (WAL) + 1 (flush) + 3 levels x 5 rewrites = ~17
  => 1 GB ingested = ~17 GB written to disk

tiered (T=4 runs/tier, 4 tiers):
  each byte is written once per tier it passes through:
  WA ~ 1 (WAL) + 1 (flush) + 3 tier merges = ~5
  => 1 GB ingested = ~5 GB written to disk

but the read side inverts:
  leveled point read touches  <= 4 + #L0  ~ 8 files
  tiered  point read touches  <= 4 tiers x 4 runs = 16 runs
```

## 6. Worked example 2 — why L0 gets a count trigger, not a size trigger

L0 files overlap, so 20 small L0 files cost a reader 20 binary searches
even if they total only 40 MB. That is why step 1 above scores L0 by
*file count* (RocksDB `level0_file_num_compaction_trigger`, default 4)
while L1+ score by *bytes*. With 20 L0 files of 2 MB each and trigger 4:

```text
L0 score = 20 / 4 = 5.0    <- screams first, despite tiny bytes
L1 score = 280 MB / 300 MB = 0.93
=> picker chooses L0->L1 merge; after it, reads drop from
   20+3 file probes to 1+3.
```

Both engines confirm the special-casing in source: RocksDB's comment at
`compaction_picker_level.cc:229` ("L0 score = num L0 files / trigger")
and Pebble's dedicated `scores[0]` L0 entry at
`compaction_picker.go:1000`.

## 7. Where graph systems inherit this

- Neo4j-style record stores avoid LSM entirely (update-in-place B-tree
  thinking); but every modern graph engine that sits on a KV store
  (Dgraph on Badger, NebulaGraph/TiKV on RocksDB) inherits exactly this
  triangle for its adjacency data.
- This repo's own snapshot model (immutable sealed segments + overlay +
  journal, the "visibility tiers") is structurally a 2-tier tiered LSM:
  writes are cheap and reads pay per-tier — the same tradeoff, chosen
  deliberately for low-RAM OLAP.
- Full-text engines (next category) run the identical policy under the
  name "segment merging" — Lucene's TieredMergePolicy is size-tiered
  compaction for posting lists.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/compaction/compaction_picker_level.cc` | production leveled picker; score formula at lines 229-232 |
| RocksDB | `reference-repos-corpus/rocksdb-src/db/compaction/compaction_picker_fifo.cc` | third policy (FIFO: pure deletion, zero rewrite) for contrast |
| Pebble | `reference-repos-corpus/pebble-src/compaction_picker.go` | Go re-derivation of the leveled picker; per-level fill factors ~line 994 |
| mini-lsm | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/compact/leveled.rs` | teaching-scale leveled task generation |
| mini-lsm | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/compact/tiered.rs` | teaching-scale tiered engine with space-amp guard |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/compaction/tiered.rs` | Rust production tiered strategy |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/compaction/leveled` | Rust production leveled strategy (module) |
| SlateDB | `reference-repos-corpus/slatedb-src/slatedb/src/size_tiered_compaction.rs` | tiered policy on object storage — write amp costs real S3 money |
| SlateDB | `reference-repos-corpus/slatedb-src/slatedb/src/compactor_state.rs` | compaction as explicit state machine |

## 9. Cross-references

- Paper: O'Neil et al., "The Log-Structured Merge-Tree" — see
  `research-papers-ledger.md`.
- Prior digest overlap: the 202606 study touched LSM only as "what Badger
  gives Dgraph"; this doc adds the policy mechanics and the tiered/leveled
  duality it skipped.
- Sibling patterns: WAL group commit (the write path upstream of the
  flush), bloom-filter read shortcut (how tiered survives its read amp).

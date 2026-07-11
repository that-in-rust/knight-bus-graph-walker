# Bloom Filter Shortcut — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `bloom-filter-shortcut-ascii.md` / `bloom-filter-shortcut-mermaid.md` |
| One-line job | Answer "is key k definitely NOT in this file?" from a few bytes of RAM, so the read path can skip disk I/O on the overwhelmingly common negative case |

## 1. One-sided error is the whole trick

```mermaid
flowchart TD
    Q["bloom.query(k)"] --> NO["answer NO:<br/>DEFINITELY absent"] --> SKIP["skip file — zero I/O"]
    Q --> YES["answer YES:<br/>PROBABLY present (fp rate p)"] --> REAL["do the real lookup"]
    REAL -- "false positive" --> WASTE["one wasted lookup — annoying"]
    NO -.-> NEVER["a wrong NO would lose data —<br/>and can never happen"]
```

## 2. Construction — double hashing over a bit array

All four witnesses use one 32-bit hash offset k times instead of k real
hash functions:

```mermaid
flowchart LR
    K["key"] --> H["h = hash(key)<br/>delta = rotate(h, 15)"]
    H --> P1["bits[(h + 0·delta) % m] = 1"]
    H --> P2["bits[(h + 1·delta) % m] = 1"]
    H --> PK["... k positions"]
    P1 & P2 & PK --> QRY["query: ALL ones -> probably<br/>ANY zero -> definitely not"]
```

Sizing math (hard-coded in
`reference-repos-corpus/mini-lsm-src/mini-lsm/src/table/bloom.rs`,
lines 86–96):

```mermaid
flowchart TD
    P["target fp rate p"] --> BPK["bits/key = -ln(p) / (ln 2)²"]
    BPK --> KH["k = 0.69 × bits/key"]
    KH --> EX1["p = 1% -> 9.6 bits/key, k = 7 (industry default)"]
    KH --> EX2["p = 0.1% -> 14.4 bits/key, k = 10"]
```

10 bits of RAM per key regardless of key size — only the hash enters.

## 3. Position in the LSM read path

```mermaid
flowchart TD
    G["get(k)"] --> MT["memtable (RAM)"]
    MT -- miss --> LOOP["for each run, newest -> oldest"]
    LOOP --> RC{"k within file's<br/>min/max key range?"}
    RC -- no --> NEXT["next run"]
    RC -- yes --> BF{"bloom: definitely not?"}
    BF -- yes --> NEXT
    BF -- probably --> IDX["read index block,<br/>binary search, read data block"]
    IDX -- hit --> DONE["first hit wins<br/>(newest shadows older)"]
    IDX -- miss (false positive) --> NEXT
```

RocksDB plugs step BF into the table reader via FilterPolicy
(`reference-repos-corpus/rocksdb-src/table/block_based/filter_policy.cc`);
the filter block lives inside the immutable SSTable, built once at
flush/compaction — no deletion support ever needed because files die
whole in compaction.

## 4. Blocked Bloom — the cache-line refinement

```mermaid
flowchart LR
    subgraph classic [classic filter]
        C1["k probes scattered over m bits"] --> C2["k cache misses / query"]
    end
    subgraph blocked [blocked filter]
        B1["block = hash1(key) % num_blocks<br/>ONE 64-byte block"] --> B2["1 cache miss, k probes<br/>inside 512 bits — free after miss"]
    end
    classic --> T["tradeoff: blocked pays ~1.1x worse p<br/>for 3-4x faster queries"]
    blocked --> T
```

Implementations: fjall's
`reference-repos-corpus/lsm-tree-src/src/table/filter/blocked_bloom`
(with `standard_bloom` beside it for contrast); RocksDB's
format_version=5 full filter in
`reference-repos-corpus/rocksdb-src/util/bloom_impl.h`.

## 5. Worked example — the arithmetic of skipping

Tiered LSM, 10 runs, key present only in run 7; 10 bits/key, p = 1%:

```mermaid
flowchart LR
    subgraph without [no filters]
        W1["9 wasted binary searches + 1 real"] --> W2["~20 block reads"]
    end
    subgraph with [with filters]
        F1["9 runs: bloom NO at 99% certainty"] --> F2["expected waste = 9 × 0.01 = 0.09<br/>=> ~1.09 lookups (~9x less I/O)"]
    end
    with --> RAM["RAM bill: 100M keys × 10 runs × 10 bits = 1.25 GB —<br/>why RocksDB partitions/pages filters<br/>(filter_policy_internal.h)"]
```

## 6. Worked example — negative lookups, the killer use

Graph ingest doing get-or-create by external ID over 1B nodes, 90% of
incoming IDs new (absent everywhere):

```mermaid
sequenceDiagram
    participant I as ingest
    participant B as blooms (RAM)
    participant D as disk runs
    I->>B: get(new_id) — probe all filters
    B-->>I: all say NO (~10 RAM probes)
    Note over I,D: disk untouched — absence is nearly free
    I->>D: insert as new node
    Note over I,B: without blooms every miss pays a full<br/>multi-run probe: ingest becomes read-bound<br/>on keys that don't exist
```

Badger's `y/bloom.go` sits on exactly this path — Dgraph's posting-list
writes are get-or-create shaped.

## 7. Inheritance map

```mermaid
flowchart LR
    BL[bloom shortcut] --> DG["Dgraph/Badger: every edge insert<br/>is a keyed lookup first"]
    BL --> FTS["FTS flips it: FST term dictionary =<br/>EXACT filter that also locates"]
    BL --> KB["this repo: per-segment bloom over<br/>external IDs — the only structure that<br/>MUST be RAM-resident; segments skip<br/>without faulting a page"]
    BL --> RIB["RocksDB Ribbon filters: same API,<br/>~30% less RAM — pattern survives<br/>the implementation swap"]
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| mini-lsm | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/table/bloom.rs` | sizing math (lines 86-96) |
| badger | `reference-repos-corpus/badger-src/y/bloom.go` | double hashing on the hot path |
| RocksDB | `reference-repos-corpus/rocksdb-src/util/bloom_impl.h` | legacy + blocked implementations |
| RocksDB | `reference-repos-corpus/rocksdb-src/table/block_based/filter_policy.cc` | FilterPolicy plug point |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/table/filter/blocked_bloom` | Rust blocked Bloom |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/table/filter/standard_bloom` | Rust classic Bloom |

## 9. Cross-references

- Sibling patterns: `lsm-compaction-tradeoff` (creates the multi-run
  problem); `roaring-bitmap-idsets` (exact sets for iteration and
  intersection); `mvcc-snapshot-visibility` (newest-first probe order).
- 202606 digest overlap: none — read-path filters were untouched.
- Paper trail: Bloom (1970); RocksDB's Ribbon filter paper — see
  `research-papers-ledger.md` for verified entries.

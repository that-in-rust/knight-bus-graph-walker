# Bloom Filter Shortcut — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `bloom-filter-shortcut-ascii.md` / `bloom-filter-shortcut-mermaid.md` |
| One-line job | Answer "is key k definitely NOT in this file?" from a few bytes of RAM, so the read path can skip disk I/O on the overwhelmingly common negative case |

## 1. The job

Tiered and leveled LSMs both make readers probe many sorted runs (see
`lsm-compaction-tradeoff`). Most probes are misses: the key lives in
one run, the reader checks ten. Each miss without help costs a binary
search — index block reads, maybe a data block read — real I/O for
nothing. The Bloom filter is a per-file bitmask that answers set
membership with one-sided error:

```text
answer "NO"  -> key is DEFINITELY absent   -> skip file, zero I/O
answer "YES" -> key is PROBABLY present    -> do the real lookup
                (false positives at rate p, tunable via bits/key)
```

One-sided error is the whole trick: a wrong "yes" costs one wasted
lookup; a wrong "no" would lose data — and can never happen.

## 2. Raw data shape

A bit array of m bits plus k independent-ish hash positions per key.
All four witnesses use the same construction (double hashing — one
32-bit hash, rotated/offset k times — instead of k real hash
functions):

```text
insert(key):  h = hash(key)
              for i in 0..k:
                  bits[ (h + i*delta) % m ] = 1
              (badger y/bloom.go; mini-lsm table/bloom.rs:
               delta = h rotated 15 bits)

query(key):   same k positions; ALL ones -> "probably";
              ANY zero -> "definitely not"
```

The sizing math, hard-coded in mini-lsm (`bloom.rs:86-96`):

```text
bits/key  = -ln(p) / (ln 2)^2          p = target false-positive rate
k         =  bits_per_key * ln 2  (~0.69 * bits/key)

p = 1%  -> 9.6 bits/key, k = 7      <- the industry default
p = 0.1%-> 14.4 bits/key, k = 10
```

Ten bits of RAM per key, regardless of key size — a 100-byte key costs
the same 10 bits as a 4-byte one, because only the hash enters.

## 3. Step-by-step: where it sits in the LSM read path

```text
get(k), LSM with memtable + N sorted runs:

1. check memtable (RAM, no filter needed)
2. for each run, newest -> oldest:
     a. range check:   k within [min_key, max_key] of the file?
     b. BLOOM CHECK:   filter says "definitely not"? -> next run
     c. only now: read index block, binary search, read data block
3. first hit wins (newest version shadows older — see
   mvcc-snapshot-visibility for why order matters)

RocksDB wires b into the table reader via FilterPolicy
(table/block_based/filter_policy.cc); the filter block is stored
INSIDE the SSTable and loaded on open — filters are per-file,
immutable, built once at flush/compaction time.
```

Immutability is why Blooms fit LSMs so well: the file never changes, so
the filter never needs updating — deletion support (the classic Bloom
weakness) is never needed because files die whole in compaction.

## 4. Blocked Bloom: the cache-line refinement

The classic filter scatters its k probes across the whole bit array —
k cache misses per query. The blocked variant (fjall's
`table/filter/blocked_bloom`, RocksDB's "format_version=5" full filter
in `util/bloom_impl.h`) first hashes to ONE 64-byte block, then does
all k probes inside it:

```text
classic:  probe bits at positions scattered over m bits -> k misses
blocked:  block = hash1(key) % num_blocks     -> 1 cache miss
          k probes inside that 512-bit block  -> free after the miss
cost: slightly worse p for same bits (~1.1x), 3-4x faster query
```

## 5. Worked example 1 — the arithmetic of skipping

Tiered LSM, 10 runs, point lookup for a key present in run 7.
Filter: 10 bits/key, p = 1%.

```text
without filters:  9 wasted binary searches + 1 real
                  ~ 10 index-block reads + ~10 data-block reads
with filters:     runs 1-6, 8-10: bloom says NO with 99% certainty
                  expected wasted lookups = 9 x 0.01 = 0.09
                  => ~1.09 lookups instead of 10   (~9x less I/O)

RAM bill: 100M keys/run x 10 runs x 10 bits = 1.25 GB of filters
          (why filters get partitioned/paged under memory pressure —
           rocksdb filter_policy_internal.h manages exactly this)
```

## 6. Worked example 2 — negative lookups, the killer use

A graph ingest doing "get-or-create node by external ID" over 1B
existing nodes, 90% of incoming IDs are NEW (absent everywhere):

```text
every insert first does a get(k) that MISSES all runs.
without blooms: each miss pays a full multi-run probe -> ingest is
                read-bound on keys that don't exist!
with blooms:    a miss costs ~10 RAM probes and 0.01 x runs false
                positives -> absence is nearly free.
```

This is why Badger keeps Blooms hot in its `y/bloom.go` fast path —
Dgraph's posting-list writes are exactly this get-or-create shape.

## 7. Where graph and search systems inherit this

- Dgraph/Badger: adjacency = posting lists keyed by (predicate, node);
  every edge insert is a keyed lookup first — blooms carry ingest.
- Full-text engines flip the trick: a term dictionary (FST) is an
  EXACT filter that also gives the location — the next category shows
  when exactness is worth the extra memory.
- This repo: a per-segment Bloom over node external-IDs lets the
  low-RAM walker skip mmap'd segments during ID resolution without
  faulting a single page in — the filter is the only thing that MUST
  be RAM-resident.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| mini-lsm | `reference-repos-corpus/mini-lsm-src/mini-lsm/src/table/bloom.rs` | clearest sizing math (bits_per_key, k=0.69*bpk, lines 86-96) |
| badger | `reference-repos-corpus/badger-src/y/bloom.go` | double-hashing construction on the hot get-or-create path |
| RocksDB | `reference-repos-corpus/rocksdb-src/util/bloom_impl.h` | legacy + blocked ("fast local") filter implementations |
| RocksDB | `reference-repos-corpus/rocksdb-src/table/block_based/filter_policy.cc` | FilterPolicy plug point into the table reader |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/table/filter/blocked_bloom` | Rust blocked Bloom (cache-line variant) |
| fjall lsm-tree | `reference-repos-corpus/lsm-tree-src/src/table/filter/standard_bloom` | Rust classic Bloom for contrast |

## 9. Cross-references

- Sibling patterns: `lsm-compaction-tradeoff` (creates the multi-run
  problem Blooms solve); `roaring-bitmap-idsets` (exact sets when you
  need iteration and intersection, not just membership);
  `mvcc-snapshot-visibility` (why probe order is newest-first).
- RocksDB has largely moved new deployments to Ribbon filters (same
  API, ~30% less RAM for equal p) — the pattern survives the
  implementation swap, which is the point of learning the pattern.
- 202606 digest overlap: none — read-path filters were untouched.

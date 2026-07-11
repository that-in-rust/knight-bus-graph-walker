# Triple Permutation Indexing — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `triple-permutation-indexing-ascii.md` / `triple-permutation-indexing-mermaid.md` |
| One-line job | Store the SAME set of (subject, predicate, object) triples in several sort orders, so that any query pattern with any bound/unbound combination becomes one sorted range scan — the RDF world's answer to adjacency |

## 1. The job

RDF has no records and no adjacency lists — the whole database
is one set of triples (quads, with a graph column). SPARQL asks
for patterns where any position may be a variable:

```text
(alice, knows, ?x)     S,P bound  -> need order S-P-...
(?x, knows, bob)       P,O bound  -> need order P-O-... or O-P-...
(?x, ?p, bob)          O bound    -> need order O-...
```

A single sort order answers only prefixes of itself. The trick:
store the triple set REDUNDANTLY, once per sort order, so every
bound-prefix combination hits SOME order as a contiguous range.

## 2. The permutation lattice

```text
3 positions -> 6 orders: SPO SOP PSO POS OSP OPS
any pattern with k bound positions needs an order whose first
k slots are exactly the bound set (in any arrangement):
    bound {}      any order       (full scan)
    bound {S}     SPO or SOP
    bound {P}     PSO or POS
    bound {O}     OSP or OPS
    bound {S,P}   SPO             bound {P,O}   POS or OPS
    bound {S,O}   SOP or OSP      bound {S,P,O} any (existence)
minimum covering set = 3 orders (e.g. SPO, POS, OSP) —
one per "which position leads". Engines choose 3 or all 6.
```

## 3. Oxigraph: quads in RocksDB column families

Oxigraph stores QUADS (triple + graph) as keys in an LSM
(pattern 1), one column family per permutation:

```text
lib/oxigraph/src/storage/rocksdb.rs:39-49
    ID2STR_CF "id2str"        term dictionary: ID -> string
    SPOG POSG OSPG            3 orders for named-graph quads,
    GSPO GPOS GOSP            3 more with graph LEADING
    DSPO DPOS DOSP            3 for the default graph
rocksdb.rs:9-10   write_spo_quad / write_pos_quad /
    write_osp_quad / write_gspo_quad ... — one encoder per
    order; an insert writes the SAME quad into every family
```

Terms are dictionary-encoded first (numeric_encoder.rs) — the
FST/dictionary discipline of pattern 19: strings live once in
id2str; permutation keys are fixed-width ID tuples. A pattern
query = pick the family whose prefix matches the bound set,
then one RocksDB prefix scan.

## 4. QLever: 6 permutations, compressed blocks

QLever (SPARQL on trillions of triples) makes the permutation a
first-class object:

```text
src/index/Permutation.h:44-54
    enum struct Enum { PSO, POS, SPO, SOP, OPS, OSP };
    ALL = the six; INTERNAL = { PSO, POS }
src/global/Constants.h:293   graph ID stored as 4th column
    in ALL permutations
src/index/ScanSpecification.h:19  a scan op = (permutation,
    bound prefix) — the planner's unit of work
src/global/IdTriple.h:82     triples permuted by KeyOrder
```

Each permutation is stored as compressed, block-wise sorted
relations (CompressedRelation.h) — pattern 17's sorted
delta-compressed blocks, applied to ID tuples instead of doc
IDs. Updates go through DeltaTriples — an in-memory delta over
the immutable base (LSM discipline again).

## 5. Worked example — pattern to range

```text
dictionary: alice=17, knows=3, bob=29
query (alice, knows, ?x):
    bound {S,P} -> SPO family, prefix (17,3)
    keys: (17,3,29) (17,3,41) (17,3,88) — contiguous!
    scan yields x IN {29, 41, 88}; 3 key reads, zero joins
query (?x, knows, bob):
    bound {P,O} -> POS family, prefix (3,29)
    keys: (3,29,17) (3,29,55) -> x IN {17, 55}
same triples, different family — the query never sorts.
```

## 6. Worked example — the redundancy bill

```text
1B triples, dictionary-encoded to 3 x 8-byte IDs = 24 B/triple:
    1 order:   24 GB (before compression)
    3 orders:  72 GB
    6 orders: 144 GB raw — but sorted ID tuples delta-compress
    hard (shared prefixes dominate): QLever-style block
    compression typically brings a permutation well under half
    its raw size; the dictionary (strings stored ONCE) is what
    keeps 6x redundancy affordable at all
trade: write amplification x6 (every insert hits 6 sorted
structures) — why oxigraph rides an LSM and QLever batches
updates through DeltaTriples instead of updating in place.
```

## 7. Property-graph mirror

The permutation trick is not RDF-only — pattern 20's KV camp is
the same idea with fewer orders: JanusGraph's (vertex, direction,
type)-sorted edge columns ARE a partial permutation index over
edge triples; and a relational edge table with two composite
B-tree indexes (src,type,dst) + (dst,type,src) is the 2-order
minimum for a directed graph. RDF engines just take the idea to
its combinatorial conclusion because they have no fixed schema
to lean on.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| oxigraph | `reference-repos-corpus/oxigraph-src/lib/oxigraph/src/storage/rocksdb.rs` | 9 permutation column families + per-order encoders (9-10, 39-49) |
| oxigraph | `reference-repos-corpus/oxigraph-src/lib/oxigraph/src/storage/numeric_encoder.rs` | term dictionary encoding |
| qlever | `reference-repos-corpus/qlever-src/src/index/Permutation.h` | the six orders as an enum, INTERNAL pair (44-54) |
| qlever | `reference-repos-corpus/qlever-src/src/index/ScanSpecification.h` | scan = (permutation, bound prefix) (19) |
| qlever | `reference-repos-corpus/qlever-src/src/global/Constants.h` | graph ID as 4th column in all permutations (293) |

## 9. Cross-references

- Sibling patterns: `lsm-compaction-tradeoff` (1 — oxigraph's
  substrate; DeltaTriples is the memtable idea),
  `posting-block-compression` (17 — QLever's compressed sorted
  blocks over ID tuples), `fst-term-dictionary` (19 — id2str is
  the same dictionary job), `record-chain-adjacency` (20 — the
  property-graph alternatives this replaces).
- Verification note (docs_PRD06 thesis): permutations are pure
  redundancy — a differential harness needs only ONE canonical
  order (sort result triples) to compare engines; internal
  family counts (3, 6, 9) are unobservable implementation
  choices. Cross-checking families WITHIN one engine (every
  quad present in every family) is that engine's own strongest
  invariant test.
- Next in category: the graph-db category synthesis pair.

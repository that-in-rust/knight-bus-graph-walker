# Triple Permutation Indexing — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `triple-permutation-indexing-ascii.md` / `triple-permutation-indexing-mermaid.md` |
| One-line job | Store the SAME set of (subject, predicate, object) triples in several sort orders, so that any query pattern with any bound/unbound combination becomes one sorted range scan — the RDF world's answer to adjacency |

## 1. The problem

```mermaid
flowchart TD
    R["RDF: no records, no adjacency lists —<br/>the database IS one set of triples"]
    R --> Q1["(alice, knows, ?x) — S,P bound"]
    R --> Q2["(?x, knows, bob) — P,O bound"]
    R --> Q3["(?x, ?p, bob) — O bound"]
    Q1 & Q2 & Q3 --> P["one sort order answers only ITS<br/>prefixes -> store the set redundantly,<br/>once per order: every bound combination<br/>hits SOME order as a contiguous range"]
```

## 2. The permutation lattice

```mermaid
flowchart TD
    L["6 orders: SPO SOP PSO POS OSP OPS"]
    L --> B0["bound {} : any (full scan)"]
    L --> BS["bound {S} : SPO/SOP<br/>bound {P} : PSO/POS<br/>bound {O} : OSP/OPS"]
    L --> B2["bound {S,P} : SPO | {P,O} : POS/OPS<br/>{S,O} : SOP/OSP | {S,P,O} : any"]
    B0 & BS & B2 --> MIN["minimum cover = 3 orders<br/>(SPO, POS, OSP — one per leading<br/>position); engines pick 3 or all 6"]
```

## 3. Oxigraph: column families over an LSM

```mermaid
flowchart TD
    W["insert quad (s,p,o,g)"]
    W --> DICT["numeric_encoder: term dictionary —<br/>strings once in id2str (rocksdb.rs:39),<br/>keys become fixed-width ID tuples<br/>(pattern 19's job)"]
    DICT --> CF["written into EVERY family<br/>(rocksdb.rs:9-10, 40-49):<br/>SPOG POSG OSPG — named graphs<br/>GSPO GPOS GOSP — graph leading<br/>DSPO DPOS DOSP — default graph"]
    CF --> RD["query = pick the family whose prefix<br/>matches the bound set -> ONE RocksDB<br/>prefix scan (pattern 1 substrate)"]
```

## 4. QLever: six permutations, compressed blocks

```mermaid
flowchart TD
    E["Permutation.h:44-54<br/>enum { PSO, POS, SPO, SOP, OPS, OSP }<br/>ALL = six; INTERNAL = { PSO, POS }"]
    E --> G4["graph ID as 4th column in ALL<br/>permutations (Constants.h:293)"]
    E --> SS["ScanSpecification.h:19 — a scan op =<br/>(permutation, bound prefix):<br/>the planner's unit of work"]
    E --> CB["each permutation stored as compressed<br/>block-wise sorted relations —<br/>pattern 17's sorted delta blocks<br/>over ID tuples"]
    CB --> DT["updates via DeltaTriples: in-memory<br/>delta over the immutable base —<br/>LSM discipline again"]
```

## 5. Pattern to range, worked

```mermaid
sequenceDiagram
    participant Q as (alice, knows, ?x)
    participant D as dictionary
    participant F as SPO family
    Q->>D: alice=17, knows=3
    D->>F: bound {S,P} -> prefix (17,3)
    F-->>Q: (17,3,29) (17,3,41) (17,3,88) —<br/>CONTIGUOUS: x IN {29,41,88},<br/>3 key reads, zero joins
    Note over Q,F: (?x, knows, bob): bound {P,O} -><br/>POS family, prefix (3,29) -><br/>x IN {17,55}. Same triples,<br/>different family — never sorts
```

## 6. The redundancy bill

```mermaid
flowchart TD
    B["1B triples x 3 IDs x 8 B = 24 B/triple"]
    B --> O1["1 order: 24 GB raw"]
    B --> O3["3 orders: 72 GB raw"]
    B --> O6["6 orders: 144 GB raw"]
    O6 --> C["but sorted ID tuples delta-compress hard<br/>(shared prefixes dominate) — block<br/>compression typically halves it or better;<br/>the ONCE-stored dictionary is what makes<br/>6x affordable at all"]
    C --> T["trade: write amplification x6 —<br/>why oxigraph rides an LSM and QLever<br/>batches through DeltaTriples"]
```

## 7. Property-graph mirror

```mermaid
flowchart LR
    RDF["RDF: 3-6 full permutations<br/>(no schema to lean on)"]
    KV["pattern 20 KV camp: JanusGraph's<br/>(vertex, direction, type)-sorted edge<br/>columns = a PARTIAL permutation index"]
    REL["relational edge table with 2 composite<br/>indexes (src,type,dst)+(dst,type,src)<br/>= the 2-order minimum for digraphs"]
    RDF & KV & REL --> SAME["same idea, different points on the<br/>redundancy dial — RDF takes it to the<br/>combinatorial conclusion"]
```

## 8. The verification angle

```mermaid
flowchart TD
    V["permutations are PURE redundancy"]
    V --> H["differential harness: ONE canonical<br/>order (sort result triples) suffices —<br/>family counts (3/6/9) are unobservable<br/>implementation choices"]
    V --> I["intra-engine invariant: every quad<br/>present in EVERY family — the engine's<br/>own strongest self-test"]
    H & I --> T["docs_PRD06 thesis: test the observable<br/>set, let the redundant layouts differ"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K1["pattern 1 LSM: oxigraph's substrate;<br/>DeltaTriples = the memtable idea"]
    K17["pattern 17: QLever's compressed<br/>sorted blocks over ID tuples"]
    K19["pattern 19: id2str = the term<br/>dictionary job"]
    K20["pattern 20: the property-graph<br/>layouts this replaces"]
    K1 & K17 & K19 & K20 --> LAW["corpus law: dictionary-encode once,<br/>sort redundantly, compress the shared<br/>prefixes — the schema-free way to make<br/>every access path a range scan"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| oxigraph | `reference-repos-corpus/oxigraph-src/lib/oxigraph/src/storage/rocksdb.rs` | 9 permutation column families + encoders (9-10, 39-49) |
| oxigraph | `reference-repos-corpus/oxigraph-src/lib/oxigraph/src/storage/numeric_encoder.rs` | term dictionary encoding |
| qlever | `reference-repos-corpus/qlever-src/src/index/Permutation.h` | the six orders (44-54) |
| qlever | `reference-repos-corpus/qlever-src/src/index/ScanSpecification.h` | scan = (permutation, bound prefix) (19) |
| qlever | `reference-repos-corpus/qlever-src/src/global/Constants.h` | graph as 4th column everywhere (293) |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Contrast with pattern 20: property graphs pick ONE physical
  adjacency layout and optimize it; RDF engines refuse to pick
  and store all orders — schema freedom paid for in write
  amplification and disk.
- Paper trail: Hexastore (the 6-permutation paper) and RDF-3X
  (compressed permutation blocks) — queued in
  `research-papers-ledger.md`.
- Next in category: the graph-db category synthesis pair
  rolling up patterns 20-22.

# FST Term Dictionary — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `fst-term-dictionary-ascii.md` / `fst-term-dictionary-mermaid.md` |
| One-line job | Map every term in the index to its posting-list location through a finite-state transducer — a minimal automaton that shares both prefixes and suffixes, giving a compressed sorted dictionary that answers exact, range, prefix, and regex lookups in one structure |

## 1. The front door

```mermaid
flowchart LR
    Q["term bytes ('graph')"] --> D["term DICTIONARY"]
    D --> P["posting-list location<br/>(patterns 17-18 take over)"]
    D --> REQ["requirements: exact lookup, ordered<br/>iteration, range scans, prefix,<br/>fuzzy/regex — millions of terms in RAM"]
    REQ --> ANS["hash map: exact only.<br/>sorted blocks: first three.<br/>FST: all of them, compressed"]
```

## 2. From trie to FST

```mermaid
flowchart TD
    T["sorted terms: graph, graphdb,<br/>graphs, grapple"]
    T --> TRIE["trie: shares PREFIXES<br/>g-r-a-p ( h (db|s) | p-l-e )"]
    TRIE --> FST["FST: minimal acyclic automaton —<br/>also shares SUFFIXES; identical<br/>sub-trees merged<br/>(FST.java:46-47 notes the pure-trie<br/>degenerate case)"]
    FST --> BYTES["states are not objects: the whole<br/>automaton is 'a compact byte[] format'<br/>(FST.java:55), mmap-able"]
    BYTES --> OUT["TRANSDUCER: each path sums edge OUTPUTS<br/>to a u64 — tantivy maps term -> dense<br/>ordinal 0..num_terms-1<br/>(fst_termdict/termdict.rs:25, 35, 151)"]
    OUT --> SIDE["ordinal indexes a bit-packed side table:<br/>postings offset, positions offset, freq —<br/>TermInfoBlockMeta (term_info_store.rs:15-20),<br/>pattern 17's packing trick reused"]
```

## 3. Why ordinals, not offsets

```mermaid
flowchart LR
    A["FST outputs distribute over paths:<br/>shared prefixes carry the common part,<br/>remainders pushed toward leaves"]
    A --> B["dense sorted ordinals -> tiny remainders,<br/>mostly leaf-edge increments"]
    A --> C["raw 40-bit file offsets: scattered ->
would not compress"]
    B & C --> V["verdict: FST holds the ORDER,<br/>the side table holds the BYTES"]
```

## 4. Three query shapes, one structure

```mermaid
flowchart TD
    E["exact('graphs'): walk edges, sum outputs;<br/>dead-end = absent — NO false positives<br/>(contrast bloom, pattern 6)"]
    R["range('graphd'..='graphz'):<br/>stream the sorted automaton —<br/>range() (termdict.rs:204-212);<br/>FST IS the sorted order"]
    AUT["automaton search: intersect the<br/>dictionary FST with a query automaton —<br/>search() takes any Automaton<br/>(termdict.rs:217-218)"]
    E & R & AUT --> POWER["superpower: dictionary = graph,<br/>query = graph, lookup = graph<br/>INTERSECTION (product-graph walk)"]
```

## 5. Fuzzy search as lockstep walk

```mermaid
sequenceDiagram
    participant L as Levenshtein automaton<br/>(query 'grap', distance 1)
    participant F as dictionary FST (10M terms)
    participant W as lockstep walker
    W->>F: try edge 'g'
    W->>L: does the automaton accept 'g...'?
    Note over W: only paths BOTH machines accept<br/>are explored — pruning at every byte
    W->>F: descend g-r-a-p, branch on h/p/...
    L-->>W: reject branches beyond distance 1
    W-->>W: emit matching terms + ordinals
    Note over L,F: cost scales with the RESULT<br/>neighborhood, not dictionary size —<br/>thousands of edges vs 10M edit-distance<br/>computations; navigate-don't-scan<br/>(patterns 13, 17)
```

## 6. Size arithmetic (10M terms, avg 12 bytes)

```mermaid
flowchart TD
    RAW["raw sorted strings: 120 MB"]
    RAW --> TRIE2["prefix sharing: ~40-50 MB of edges<br/>(English-like corpora share 60-70%<br/>of leading bytes)"]
    TRIE2 --> FST2["+ suffix sharing (minimality):<br/>~3-6 bytes/term -> 30-60 MB,<br/>one mmap-able byte[]"]
    FST2 --> SIDE2["+ term-info side table: 10-16<br/>bytes/term bit-packed — often larger<br/>than the FST itself"]
    SIDE2 --> ALT["same codebase, other choice:<br/>tantivy sstable_termdict — sorted blocks,<br/>simpler, object-storage-friendly;<br/>the encoding-per-shape choice of<br/>pattern 3, replayed on the dictionary"]
```

## 7. Inheritance map

```mermaid
flowchart LR
    F[FST dictionary] --> LU["Lucene util/fst backs the terms index of<br/>ES/OpenSearch/Solr + synonym maps,<br/>suggesters"]
    F --> TA["tantivy: tantivy-fst crate (fork of<br/>BurntSushi's fst — same library family<br/>as ripgrep's tricks)"]
    F --> RS["RediSearch/manticore: tries/custom —<br/>same role, weaker automaton composition"]
    F --> GDB["graph-db kinship: an FST IS a DAG with<br/>labeled edges and path-additive values;<br/>Levenshtein intersection is the same<br/>product construction as bisimulation"]
```

## 8. The verification angle

```mermaid
flowchart TD
    DET["construction from sorted input is<br/>deterministic: same term set -> byte-<br/>identical FST per library version"] --> X["but NOT across libraries — lucene and<br/>tantivy-fst encode differently"]
    X --> SEM["so compare at automaton-semantics level:<br/>stream both dictionaries, diff the<br/>(term, value) sequences — same accepted<br/>set, same outputs"]
    SEM --> TH["docs_PRD06 thesis condition 1: the<br/>dictionary is FULLY observable by<br/>streaming — a friendly differential surface"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K17["pattern 17: what the dictionary points<br/>at; shares the bit-packing trick"]
    K6["pattern 6 bloom: probabilistic filter vs<br/>FST's exact membership — cost/certainty<br/>trade"]
    K13["pattern 13: navigate-don't-scan<br/>economics"]
    K3["pattern 3: encoding-per-shape choice,<br/>replayed as fst-vs-sstable"]
    K17 & K6 & K13 & K3 --> LAW["corpus law: sorted input + shared<br/>structure = compression AND navigation<br/>from one build pass"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/util/fst/FST.java` | byte[]-encoded automaton (46-47, 55) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/util/fst/FSTCompiler.java` | builds minimal FSTs from sorted input |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/fst_termdict/termdict.rs` | MapBuilder (25, 35), ordinals (151), range (204-212), automaton search (217-218) |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/fst_termdict/term_info_store.rs` | bit-packed TermInfo side table (15-20) |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/sstable_termdict/` | the sorted-block alternative |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Next in category: FTS category synthesis pair (patterns 17-19
  plus segment-lifecycle notes riding on pattern 1).
- Paper trail: Mihov/Schulz Levenshtein-automata paper and the
  Daciuk et al. incremental minimal-automaton construction —
  queued in `research-papers-ledger.md`.
- 202606 digest overlap: digests mentioned "FST term dictionary"
  as a keyword; this pair adds suffix-sharing mechanics, output
  arithmetic, automaton intersection, and size/cost numbers.

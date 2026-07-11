# Full-Text Search Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `full-text-search-pattern-synthesis-ascii.md` / `full-text-search-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 17-19 into the category's single thesis: sort once at segment flush, then every structure — posting blocks, score bounds, term automata — is a free consequence of sorted, write-once data |

## 1. The category in one map

```mermaid
flowchart TD
    D["documents"] --> TK["tokenize -> in-RAM term hash"]
    TK --> SORT["SORT AT FLUSH — the one payment"]
    SORT --> FST["term dictionary: minimal FST (19)"]
    SORT --> PB["posting blocks: deltas + bit-packing<br/>+ skips (17)"]
    SORT --> BM["block maxima / impacts (18)"]
    FST & PB & BM --> IMM["all IMMUTABLE: updates = new segments,<br/>deletes = tombstone bitmaps,<br/>merges = the LSM discipline (pattern 1)"]
    IMM --> SHIP["Lucene invented the shape; ES/OpenSearch/<br/>Solr ship it; tantivy re-derived it for<br/>quickwit, lnx, paradedb"]
```

## 2. The three patterns

```mermaid
flowchart LR
    P17["17 posting-block-compression<br/>STORAGE: sorted IDs -> deltas -><br/>packed blocks + skips"]
    P18["18 bm25-wand-pruning<br/>ALGORITHM: saturating relevance +<br/>rising floor vs upper bounds =<br/>exact top-k, fractional work"]
    P19["19 fst-term-dictionary<br/>STORAGE: minimal automaton over sorted<br/>terms — lookup, range, fuzzy in one"]
    P19 --> P17 --> P18
```

## 3. The organizing axes

```mermaid
flowchart TD
    A1["axis 1 — where sortedness is spent:<br/>17 compression, 18 skippability,<br/>19 single-pass construction"]
    A2["axis 2 — exact vs bounded:<br/>retrieval mechanics all EXACT<br/>(WAND identical top-k, FST no false<br/>positives); only the scoring MODEL<br/>(BM25) is heuristic — the mirror image<br/>of vector-ann"]
    A3["axis 3 — write vs read path:<br/>one sorted flush buys three read<br/>structures; mutation cost displaced<br/>into merges (pattern 1's triangle)"]
    A1 & A2 & A3 --> TH["thesis: sort once, never pay again"]
```

## 4. One query, end to end

```mermaid
sequenceDiagram
    participant Q as query "graph database" (k=10)
    participant F as FST dictionary (19)
    participant S as statistics (18 §9b)
    participant P as posting blocks (17)
    participant W as WAND loop (18)
    Q->>F: two automaton walks -> ordinals -><br/>TermInfo (offsets, doc freqs) — microseconds
    F->>S: doc freqs -> index-wide idf
    S->>P: open two block streams, skip data ready
    P->>W: drive the shorter list;<br/>advance() the longer via skips
    W->>W: block-max bounds kill most blocks;<br/>heap floor rises monotonically
    W-->>Q: EXACT top-10, ~5-20x fewer scored<br/>docs; every byte a sequential block read
```

## 5. What the category exports

```mermaid
flowchart LR
    FTS[patterns 17-19] --> G["graph-db: adjacency lists ARE posting<br/>lists — CSR (7) = 17 minus text;<br/>WebGraph uses the same delta tricks;<br/>top-k rankers reuse 18's frame"]
    FTS --> V["vector-ann: IVF buckets (16) = posting<br/>lists by centroid; filtered ANN =<br/>set intersection via 17/3;<br/>hybrid search fuses BM25 + ANN (RRF)"]
    FTS --> ST["storage: segment lifecycle IS the LSM (1);<br/>tombstones are MVCC-lite (4);<br/>fst-vs-sstable replays 3's<br/>encoding-per-shape choice"]
    FTS --> VER["verification: the friendliest<br/>differential surface in the corpus (§7)"]
```

## 6. One workload, sized

```mermaid
flowchart TD
    W["10M docs, 10M terms, ~120 terms/doc,<br/>top-10 queries"]
    W --> D19["dictionary: 30-60 MB FST<br/>+ ~120 MB term info"]
    W --> D17["postings: ~1.2B x ~1 byte ~ 1.2 GB"]
    W --> D18["norms ~10 MB + in-stream block maxima"]
    D19 & D17 & D18 --> TOT["~1.5 GB total, mmap-able, one box;<br/>sub-ms to few-ms queries"]
    TOT --> QW["scale-out (quickwit): same segments on S3;<br/>dictionary pages in first — why quickwit<br/>prefers the sstable dictionary over the<br/>FST for object storage"]
```

## 7. The verification pipeline

```mermaid
sequenceDiagram
    participant A as engine A (e.g. Lucene)
    participant B as engine B (e.g. tantivy)
    participant H as harness
    H->>A: pin index state (same docs, same<br/>analyzer, merges frozen)
    H->>B: same pinned corpus
    H->>A: stream term dictionary
    H->>B: stream term dictionary
    H->>H: diff (term, doc-ID SET) sequences —<br/>must match EXACTLY
    H->>A: pinned query set -> top-k + scores
    H->>B: same queries
    H->>H: scores match to float tolerance<br/>(same BM25 constants, same statistics)
    Note over H: free intra-engine oracle:<br/>WAND vs exhaustive must be IDENTICAL.<br/>All three thesis conditions nearly free —<br/>contrast vector-ann's recall<br/>distributions and pinned seeds
```

## 8. Honest gaps

```mermaid
flowchart TD
    GAP["not covered by 17-19"]
    GAP --> AN["analyzers/tokenizers — where<br/>cross-engine differences are actually<br/>born (stemming, CJK)"]
    GAP --> PH["positional/phrase queries, payloads"]
    GAP --> DV["doc values / columnar fastfields —<br/>the OLAP side"]
    GAP --> DS["distributed search: shard fan-out,<br/>cross-shard statistics correction"]
    GAP --> NR["learned/neural ranking over<br/>BM25 candidates"]
```

## 9. The mirror-image law

```mermaid
flowchart LR
    VEC["vector-ann: approximate MECHANICS,<br/>exact metric — recall is the contract"]
    TXT["full-text: exact mechanics,<br/>approximate MODEL — BM25 is the heuristic"]
    VEC & TXT --> SUB["both bolt their probabilistic half onto<br/>an exact, sorted, write-once substrate —<br/>the storage-engine category's thesis"]
    SUB --> NEXT["next category: graph-db — the query<br/>language enters, and every pattern so far<br/>reappears under a planner"]
```

## 10. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/codecs/lucene104/` | posting format + ForUtil (17) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/search/WANDScorer.java` | WAND / block-max (18) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/util/fst/` | FST terms index (19) |
| tantivy | `reference-repos-corpus/tantivy-src/src/postings/` | Rust posting blocks (17) |
| tantivy | `reference-repos-corpus/tantivy-src/src/query/bm25.rs` | BM25 constants/formula (18) |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/` | FST + sstable dictionaries (19) |
| quickwit | `reference-repos-corpus/quickwit-src` | segments on object storage |
| elasticsearch | `reference-repos-corpus/elasticsearch-src` | Lucene shipped at scale |
| OpenSearch | `reference-repos-corpus/OpenSearch-src` | the fork, same core |
| paradedb | `reference-repos-corpus/paradedb-src` | tantivy inside Postgres |

## 11. Cross-references

- Members: `posting-block-compression` (17), `bm25-wand-pruning`
  (18), `fst-term-dictionary` (19).
- Prior syntheses: storage-engine (1/4 wearing text clothes),
  graph-analytics (CSR kinship, 7), vector-ann (the mirror-image
  approximation contract, 13-16).
- Verification carry-forward: an index-state pin is the ONLY
  precondition FTS needs; a Lucene-vs-tantivy harness is a
  weekend project — the cheapest place to practice the
  docs_PRD06 convergence loop before pointing it at Neo4j.
- Category coverage note: 17 of 17 full-text-search ledger rows
  are shallow-cloned; patterns were extracted from the two
  reference implementations (lucene, tantivy) and inheritance
  verified against the downstream engines' dependency on them
  (ES/OpenSearch/Solr vendor lucene; quickwit/lnx/paradedb
  vendor tantivy).

# Full-Text Search Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `full-text-search-pattern-synthesis-ascii.md` / `full-text-search-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 17-19 into the category's single thesis: sort once at segment flush, then every structure — posting blocks, score bounds, term automata — is a free consequence of sorted, write-once data |

## 1. The category in one sentence

Full-text search is the discipline of turning documents into
SORTED, WRITE-ONCE structures and then never paying for either
sorting or mutation again:

```text
docs -> tokenize -> in-RAM term hash -> SORT AT FLUSH ->
    term dictionary (FST, 19)  -> posting blocks (17)
                                -> block maxima     (18)
    ... all immutable; updates = new segments; deletes =
    tombstone bitmaps; merges = the LSM discipline (pattern 1)
```

Lucene invented this shape; Elasticsearch, OpenSearch, Solr ship
it directly; tantivy re-derived it in Rust for quickwit, lnx,
paradedb. Every FTS engine in the corpus is a variation on it.

## 2. The three patterns, one line each

```text
17 posting-block-compression   storage    sorted IDs -> deltas ->
                                          bit-packed blocks +
                                          interleaved skips
18 bm25-wand-pruning           algorithm  saturating relevance +
                                          rising floor vs upper
                                          bounds = exact top-k at
                                          a fraction of the work
19 fst-term-dictionary         storage    minimal automaton over
                                          sorted terms: lookup,
                                          range, fuzzy in one
                                          structure
```

## 3. The organizing axes

```text
axis 1 — WHERE the sortedness is spent:
    17 spends it on compression (small deltas)
    18 spends it on skippability (block maxima + skip data)
    19 spends it on construction (single-pass minimal FST)
axis 2 — exact vs bounded:
    everything here is EXACT: WAND returns identical top-k;
    FST has no false positives. The category's only
    approximation is in the SCORING MODEL (BM25 is a heuristic),
    never in retrieval mechanics — the mirror image of
    vector-ann, where mechanics are approximate but the metric
    is exact.
axis 3 — write path vs read path:
    one sorted flush buys three read structures; all mutation
    cost is displaced into segment merges (pattern 1's
    amplification triangle, replayed on text).
```

## 4. One query, end to end

```text
query: "graph database" (top-10, 10M docs)
1. dictionary (19): two FST walks -> term ordinals -> TermInfo
   (postings offset, doc freq)          ~ microseconds
2. statistics (18 §9b): idf from doc freqs, index-wide
3. postings (17): open two block streams, skip data ready
4. scoring loop (18): drive the shorter list, advance() the
   longer; block-max bounds kill most blocks; heap floor rises
5. result: exact top-10, ~5-20x fewer scored docs than
   exhaustive; every byte touched was a sequential block read
```

## 5. What the category exports to the rest of the corpus

```text
to graph-db:     adjacency lists ARE posting lists — CSR (7) is
                 pattern 17 without the text; WebGraph compresses
                 neighbor lists with the same delta tricks;
                 top-k graph rankers reuse WAND's bound-vs-floor
                 frame (18)
to vector-ann:   IVF buckets (16) = posting lists keyed by
                 centroid; filtered ANN needs doc-set intersection
                 = 17's skip machinery or 3's roaring; hybrid
                 search fuses BM25 with ANN scores (RRF)
to storage:      the segment lifecycle IS the LSM (1); tombstone
                 deletes are MVCC-lite (4); FST vs sstable
                 dictionary replays roaring-vs-blocks (3)
to verification: the friendliest differential surface in the
                 corpus — see §7
```

## 6. One workload, the design walk

```text
10M docs, 10M distinct terms, ~120 terms/doc, top-10 queries:
    dictionary (19):   30-60 MB FST + ~120 MB term info
    postings (17):     ~1.2B postings x ~1 byte  ~ 1.2 GB
    norms/impacts (18): ~10 MB + per-block maxima in-stream
    total ~ 1.5 GB, mmap-able, one box; queries sub-ms to few ms
scale-out (quickwit): same segments on object storage, dictionary
    footprint is what you page in first — why quickwit prefers
    the sstable dictionary (19 §5) over the FST for S3 reads.
```

## 7. Verification thesis for the category

FTS is the easiest category in the corpus to differential-test —
all three docs_PRD06 thesis conditions come nearly free:

```text
observability:  term dictionaries stream (19 §8); posting sets
                enumerate; scores are pure formulas (18)
coverage:       per-term set equality + per-query top-k
                comparison covers the whole surface
equivalence:    pin the index state (merge history moves N, n,
                avgdl — 18 §9b); then per-term doc-ID SETS must
                match exactly and top-k scores match to float
                tolerance; WAND-vs-exhaustive is a free
                self-check oracle inside a single engine
```

Contrast: vector-ann needed recall distributions and pinned
seeds; FTS needs only an index-state pin. A Lucene-vs-tantivy
harness is a weekend project; a faiss-vs-qdrant harness is not.

## 8. Honest gaps

```text
not covered by 17-19 (candidates for later passes):
    - analyzers/tokenizers: the linguistic front end (stemming,
      CJK segmentation) — where most cross-engine result
      differences are ACTUALLY born
    - positional/phrase queries and payloads
    - doc values / columnar fields (fastfield) — the OLAP side
    - distributed search: shard fan-out, DFS query mode,
      cross-shard statistics correction
    - learned/neural ranking on top of BM25 candidates
```

## 9. Citing repos (category roll-up)

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

## 10. Cross-references

- Members: `posting-block-compression` (17), `bm25-wand-pruning`
  (18), `fst-term-dictionary` (19).
- Prior syntheses: storage-engine (segments/LSM/tombstones are
  patterns 1/4 wearing text clothes), graph-analytics (sorted-ID
  kinship with CSR, 7), vector-ann (the mirror-image
  approximation contract, 13-16).
- The one-sentence contrast to carry forward: vector search
  approximates the MECHANICS and keeps the metric exact; text
  search keeps the mechanics exact and approximates the MODEL
  (BM25). Both bolt their probabilistic half to an exact,
  sorted, write-once substrate — which is the storage-engine
  category's whole thesis.
- Next category: graph-db — where the query language (Cypher/
  Gremlin/SPARQL) finally enters, and every pattern so far
  reappears under a planner.

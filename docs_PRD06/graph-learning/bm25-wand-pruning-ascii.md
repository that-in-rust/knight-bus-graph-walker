# BM25 WAND Pruning — ASCII

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `bm25-wand-pruning-ascii.md` / `bm25-wand-pruning-mermaid.md` |
| One-line job | Rank documents by BM25 (saturating term-frequency x rarity x length-normalization) and skip whole regions of the posting lists whose per-term maximum scores cannot beat the current top-k floor — WAND / block-max WAND |

## 1. The job

Pattern 17 built cheap posting-list traversal; this pattern
decides WHICH docs win. Two halves:

```text
scoring:   BM25 — the corpus-wide default relevance formula
pruning:   top-k queries don't need every match's score, only
           the k best -> skip every doc whose UPPER BOUND
           can't beat the current k-th score (WAND)
```

## 2. The BM25 formula, from source

tantivy hard-codes the standard constants (query/bm25.rs:8-9):

```text
K1 = 1.2    B = 0.75

idf(term)  = log(1 + (N - n + 0.5)/(n + 0.5))     (bm25.rs:52,139)
             N docs total, n docs containing term
norm       = K1 * (1 - B + B * dl/avgdl)          (bm25.rs:59,206)
             dl = this doc's field length
tf part    = freq / (freq + norm)                 (bm25.rs:192,203)
score      = SUM over query terms of
             idf * (1 + K1) * tf_part             (bm25.rs:159,168)
```

The three behaviors that make it work:

```text
saturation:  freq/(freq+norm) -> 1 asymptotically; the 50th
             occurrence adds ~nothing (unlike raw TF)
rarity:      idf makes "graph" >> "the"
length norm: B interpolates between ignoring doc length (B=0)
             and full normalization (B=1) — long docs must not
             win just by containing everything
```

## 3. WAND — the pruning frame

Top-k evaluation with per-term score upper bounds. Lucene's
WANDScorer keeps the query's scorers in three places
(WANDScorer.java:39-53, 124-140):

```text
lead:  scorers positioned ON the current candidate doc
head:  heap of scorers positioned AHEAD (ordered by doc)
tail:  heap of scorers BEHIND, ordered by maxScore

invariant driving the skip:
    sum(max_score of lead+tail) >= minCompetitiveScore
    (:39-40) — if even the best case can't beat the k-th
    score so far, the candidate is dead: advance without
    scoring, and tail scorers stay UNPOSITIONED (never even
    touch their posting blocks)
```

minCompetitiveScore rises monotonically as better docs enter the
top-k heap — pruning accelerates as the query runs. The same
lower-bound-vs-floor logic as HNSW's stopping rule (pattern 13 §5).

## 4. Block-max WAND

Global per-term max scores are weak bounds (one freak doc sets
the ceiling forever). Lucene stores per-BLOCK maxima ("impacts")
inside the posting format and uses them for local bounds; with
block maxima unavailable the scorer "will effectively implement
WAND rather than block-max WAND" (WANDScorer.java:445, 50).

```text
per posting block (256 docs): store max(freq, norm) pairs
    -> a local score ceiling for just those docs
skip decision becomes: can THIS block beat the floor?
    no -> advance() past the whole block via skip data
    (pattern 17 §3) without decoding it
```

Scores are scaled to integer space for safe summation
(scaleMaxScore, WANDScorer.java:90-96) — float rounding must
never make a bound falsely too low (a correctness bug, not a
performance one).

## 5. Worked example — BM25 arithmetic

```text
corpus: N = 10M docs, avgdl = 120 terms
query: "graph database"
  graph:    n = 50k  -> idf = log(1 + 9.95M/50k)  ~ 5.30
  database: n = 500k -> idf = log(1 + 9.5M/500k)  ~ 2.99
doc A: dl = 100, freq(graph)=3, freq(database)=1
  norm     = 1.2 * (0.25 + 0.75*100/120) = 1.05
  graph:    3/(3+1.05) = 0.741; database: 1/(1+1.05) = 0.488
  score = 5.30*2.2*0.741 + 2.99*2.2*0.488 = 8.64 + 3.21 = 11.85
doc B: dl = 1200 (long), freq(graph)=3, freq(database)=1
  norm = 1.2 * (0.25 + 0.75*10) = 9.3
  score = 5.30*2.2*0.244 + 2.99*2.2*0.097 = 2.85 + 0.64 = 3.49
same term counts, 3.4x score gap: length normalization at work.
```

## 6. Worked example — what pruning saves

```text
query of 3 terms, posting lengths 5M + 1M + 200k, k = 10
exhaustive: score ~6.2M docs (union), full decode of all lists
block-max WAND, floor after ~1k docs ~ competitive:
    the 5M-list contributes few unique winners; most of its
    blocks have max_score below the floor -> skipped whole
    typical published/observed regime: 5-20x fewer scored docs,
    more for longer queries with skewed idf.
    top-k SCORES returned are IDENTICAL to exhaustive — safe
    pruning, not approximation (contrast: pattern 13's ef).
```

## 7. Where systems inherit this

- Lucene → Elasticsearch/OpenSearch/Solr: BM25Similarity is the
  default similarity; WANDScorer drives top-k boolean queries.
- tantivy → quickwit/paradedb: bm25.rs above, plus its own
  block-max structures in the posting reader.
- RediSearch, bleve, xapian, manticore: BM25 (or BM25F) default
  scorers throughout.
- Hybrid search (vector + text): BM25 scores fuse with ANN
  similarities (RRF or linear) — the FTS side of the convergence
  noted in the vector synthesis §6.
- Graph relevance: personalized-PageRank-style rankers face the
  same top-k-with-bounds problem; WAND's frame (upper-bound
  per contributor, floor from the heap) reappears in top-k joins
  and priority-queue graph queries.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| tantivy | `reference-repos-corpus/tantivy-src/src/query/bm25.rs` | BM25 constants and formula: K1/B (8-9), idf (52, 139), norm (59, 206), tf saturation (192-206), weight (159-168) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/search/WANDScorer.java` | WAND: lead/head/tail decomposition (39-53, 124-140), integer score scaling (90-96), block-max vs plain WAND (445) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/search/similarities/BM25Similarity.java` | the default similarity of the Lucene family |
| quickwit | `reference-repos-corpus/quickwit-src` | tantivy's BM25/pruning on object storage |

## 9. Cross-references

- Sibling patterns: `posting-block-compression` (the skip
  machinery WAND rides on; impacts live in those blocks);
  `hnsw-layered-greedy` (same bound-vs-floor stopping logic, but
  WAND is EXACT); `ivf-partitioned-probe` (both skip data regions,
  IVF by geometry, WAND by score bounds).
- Next in category: FTS category synthesis (posting storage +
  scoring + segments), leaning on patterns 1/17.
- Verification note (docs_PRD06 thesis): BM25 is a pure formula —
  differential-testable to float tolerance IF the statistics
  match; but N, n, avgdl differ per segment-merge state and
  deleted-doc handling. Pin the index state, then top-k scores are
  deterministic; pruning must be invisible (WAND vs exhaustive =
  identical results) — an ideal self-check oracle.
- 202606 digest overlap: digests named BM25 as the standard
  scorer; this pair adds the source constants, the WAND
  three-heap mechanics, block-max, and worked arithmetic.

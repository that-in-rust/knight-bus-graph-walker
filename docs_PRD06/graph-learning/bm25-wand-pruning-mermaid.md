# BM25 WAND Pruning — Mermaid

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `bm25-wand-pruning-ascii.md` / `bm25-wand-pruning-mermaid.md` |
| One-line job | Rank documents by BM25 (saturating term-frequency x rarity x length-normalization) and skip whole regions of the posting lists whose per-term maximum scores cannot beat the current top-k floor — WAND / block-max WAND |

## 1. Two halves

```mermaid
flowchart LR
    S["SCORING: BM25 — the corpus-wide<br/>default relevance formula"]
    P["PRUNING: top-k needs only the k best —<br/>skip docs whose UPPER BOUND can't<br/>beat the current floor (WAND)"]
    S --> P
    P --> R["result: identical top-k to exhaustive<br/>scoring, at a fraction of the work —<br/>SAFE pruning, not approximation"]
```

## 2. BM25 from source (tantivy query/bm25.rs)

```mermaid
flowchart TD
    C["constants: K1 = 1.2, B = 0.75 (:8-9)"]
    C --> IDF["idf = log(1 + (N-n+0.5)/(n+0.5))<br/>(:52, 139) — rarity: 'graph' >> 'the'"]
    C --> NORM["norm = K1 * (1 - B + B * dl/avgdl)<br/>(:59, 206) — length normalization"]
    IDF & NORM --> TF["tf part = freq / (freq + norm)<br/>(:192, 203) — SATURATION: the 50th<br/>occurrence adds ~nothing"]
    TF --> SC["score = SUM_terms idf * (1+K1) * tf_part<br/>(:159, 168)"]
```

## 3. Worked arithmetic (N=10M, avgdl=120)

```mermaid
flowchart LR
    Q["query 'graph database':<br/>idf(graph, n=50k) ~ 5.30<br/>idf(database, n=500k) ~ 2.99"]
    Q --> A["doc A: dl=100, freqs 3/1<br/>norm=1.05 -> score = 8.64+3.21 = 11.85"]
    Q --> B["doc B: dl=1200, SAME freqs<br/>norm=9.3 -> score = 2.85+0.64 = 3.49"]
    A & B --> L["same term counts, 3.4x gap:<br/>length normalization at work"]
```

## 4. WAND — the three-place decomposition

```mermaid
flowchart TD
    LEAD["lead: scorers ON the candidate doc"]
    HEAD["head: heap of scorers AHEAD<br/>(ordered by doc ID)"]
    TAIL["tail: heap of scorers BEHIND<br/>(ordered by maxScore)"]
    LEAD & HEAD & TAIL --> INV["invariant (WANDScorer.java:39-53):<br/>sum(max_score of lead+tail) >=<br/>minCompetitiveScore — else the candidate<br/>is dead WITHOUT scoring, and tail scorers<br/>never touch their posting blocks"]
    INV --> MONO["minCompetitiveScore rises monotonically<br/>as the top-k heap fills:<br/>pruning ACCELERATES during the query"]
    MONO --> KIN["same lower-bound-vs-floor logic as<br/>HNSW's stopping rule (pattern 13 §5) —<br/>but exact, because bounds are true maxima"]
```

## 5. Block-max WAND

```mermaid
flowchart LR
    G["global per-term max score:<br/>weak bound — one freak doc sets<br/>the ceiling forever"]
    G --> BM["per-BLOCK maxima ('impacts') stored in<br/>the posting format: local ceilings<br/>for 256 docs at a time"]
    BM --> SKIP["skip decision: can THIS block beat the<br/>floor? no -> advance() past it via<br/>skip data (pattern 17 §3), undecoded"]
    SKIP --> FB["without block maxima the scorer 'will<br/>effectively implement WAND rather than<br/>block-max WAND' (WANDScorer.java:445)"]
    FB --> INT["scores scaled to integers for summation<br/>(scaleMaxScore, :90-96): a bound rounded<br/>falsely LOW is a correctness bug"]
```

## 6. One query, end to end

```mermaid
sequenceDiagram
    participant Q as 3-term query (k=10)
    participant H as top-k heap / floor
    participant T as tail (by maxScore)
    participant B as posting blocks
    Q->>H: score first candidates exhaustively,<br/>heap fills, floor rises
    loop each candidate doc
        Q->>T: sum available max scores
        alt bound < floor
            Q->>B: advance past doc / whole block —<br/>no decode, no scoring
        else bound >= floor
            Q->>B: position tail scorers, decode,<br/>compute true BM25
            B->>H: if score > floor: replace min,<br/>floor rises
        end
    end
    Note over Q,H: 5M+1M+200k postings -> typically<br/>5-20x fewer scored docs; returned<br/>top-k IDENTICAL to exhaustive
```

## 7. Inheritance map

```mermaid
flowchart LR
    W[BM25 + WAND] --> ES["Lucene -> ES/OpenSearch/Solr:<br/>BM25Similarity default,<br/>WANDScorer on top-k booleans"]
    W --> TA["tantivy -> quickwit/paradedb:<br/>bm25.rs + its own block-max reader"]
    W --> OT["RediSearch, bleve, xapian,<br/>manticore: BM25/BM25F defaults"]
    W --> HY["hybrid search: BM25 fused with ANN<br/>scores (RRF/linear) — the FTS-vector<br/>convergence (vector synthesis §6)"]
    W --> GR["graph top-k queries: the same<br/>upper-bound-per-contributor + heap-floor<br/>frame reappears in priority-queue<br/>graph rankers"]
```

## 8. The verification angle

```mermaid
flowchart TD
    F["BM25 is a pure formula:<br/>float-tolerance diffable"] --> ST["but N, n, avgdl depend on segment-merge<br/>state and deleted-doc handling —<br/>pin the INDEX STATE first"]
    ST --> OR["then two free oracles:<br/>1. WAND vs exhaustive must be IDENTICAL<br/>(pruning is invisible by contract)<br/>2. cross-engine top-k comparable to<br/>tolerance once statistics align"]
    OR --> TH["docs_PRD06 thesis: FTS scoring is the<br/>FRIENDLIEST differential surface in the<br/>corpus — deterministic, observable,<br/>and self-checking"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K17["pattern 17: the skip machinery WAND<br/>rides on; impacts live in those blocks"]
    K13["pattern 13: bound-vs-floor stopping —<br/>approximate there, exact here"]
    K16["pattern 16: both skip data regions;<br/>IVF by geometry, WAND by score bounds"]
    K12["pattern 12: monotonically rising floor =<br/>monotonically advancing bucket index —<br/>priority pruning is one family"]
    K17 & K13 & K16 & K12 --> LAW["corpus law: every top-k system pairs a<br/>rising floor with cheap upper bounds,<br/>and wins by never touching the losers"]
```

## 9b. Where the statistics come from — the segment problem

```mermaid
sequenceDiagram
    participant W as Bm25Weight (query time)
    participant S1 as segment 1
    participant S2 as segment 2
    participant Q as scorer per segment
    W->>S1: doc_freq(term), doc_count, avg fieldnorm
    W->>S2: same statistics
    Note over W: tantivy sums idf across the searched<br/>segments (bm25.rs:121-127) so the weight<br/>is INDEX-wide, not per-segment —<br/>otherwise the same doc would score<br/>differently depending on which segment<br/>it landed in after merges
    W->>Q: one weight, per-segment norms<br/>(fieldnorm tables)
    Q-->>W: top-k per segment -> global merge<br/>of the k-heaps
    Note over W,Q: deleted docs still count in n and N until<br/>merged away — scores DRIFT as segments<br/>compact; another reason differential<br/>tests must pin the index state
```

The statistics plumbing is where cross-engine score differences
actually come from in practice — the formula is identical
everywhere, the bookkeeping is not.

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| tantivy | `reference-repos-corpus/tantivy-src/src/query/bm25.rs` | K1/B (8-9), idf (52, 139), norm (59, 206), saturation (192-206), weight (159-168) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/search/WANDScorer.java` | lead/head/tail (39-53, 124-140), integer scaling (90-96), block-max fallback (445) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/search/similarities/BM25Similarity.java` | the Lucene-family default similarity |
| quickwit | `reference-repos-corpus/quickwit-src` | tantivy's scoring on object storage |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Next in category: FTS category synthesis (blocks + scoring +
  the Lucene segment lifecycle, leaning on patterns 1/17/18).
- Paper trail: Robertson/Spärck Jones BM25 lineage and the
  Broder et al. WAND paper — queued in
  `research-papers-ledger.md`.
- 202606 digest overlap: digests named BM25 as the standard
  scorer; this pair adds source constants, three-heap mechanics,
  block-max, and worked arithmetic.

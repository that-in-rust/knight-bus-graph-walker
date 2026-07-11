# Posting Block Compression — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `posting-block-compression-ascii.md` / `posting-block-compression-mermaid.md` |
| One-line job | Store each term's sorted doc-ID list as delta-encoded, bit-packed fixed-size blocks with interleaved skip data — so a list of millions of IDs costs ~1 byte each and can jump forward without decoding |

## 1. The job

The inverted index maps term -> posting list (all doc IDs
containing the term, sorted ascending). Big terms match millions
of docs; queries intersect several such lists. Two requirements
pull against each other:

```text
small:  lists dominate index size -> compress hard
fast:   AND-queries need advance(target) — skip forward to the
        first doc >= target WITHOUT decoding everything between
```

The universal answer: fixed-size blocks of deltas, bit-packed to
the block's widest value, with skip entries interleaved so
advance() hops over whole blocks.

## 2. Raw data shape

```text
doc IDs (sorted):   [3, 7, 12, 31, 55, 58, 90, ...]
deltas:             [3, 4,  5, 19, 24,  3, 32, ...]
                     (small numbers! sortedness IS the compression
                      opportunity — same insight as CSR, roaring)

block of 128/256 deltas -> max delta needs b bits
                        -> pack ALL values at b bits each

tantivy (postings/compression/mod.rs:1-14):
    COMPRESSION_BLOCK_SIZE = BitPacker4x::BLOCK_LEN  (= 128)
    compressed size = num_bits * 128 / 8 bytes
    num_bits_strictly_sorted() picks b per block (:41)

lucene 10.4 (codecs/lucene104/ForUtil.java:34-36):
    BLOCK_SIZE = 256, BLOCK_SIZE_LOG2 = 8
    "the first 256 document ids are encoded as a packed block"
    (Lucene104PostingsFormat.java:53); the <=255 leftover docs
    ride as vInts — the tail never justifies a block
```

## 3. Skip data — advance() without decoding

Lucene interleaves skip entries WITH the blocks
(Lucene104PostingsFormat.java:61-62, 120, 205):

```text
level 0: one skip entry between every packed block
         (last docID of the block + file pointers)
level 1: one entry every 32 blocks (= 8192 docs)

advance(target):
    walk level-1 entries while last_doc < target   (skips 8k docs)
    walk level-0 entries while last_doc < target   (skips 256)
    decode ONE block, linear/SIMD scan inside

DocDelta at block boundaries is taken against "the last doc of
the previous block" (:205) — deltas never reset to absolute,
skip entries carry the absolutes.
```

This is a static B-tree flattened into the stream: the same
two-level indirection as pattern 7's offsets array, or pattern 1's
SSTable index blocks.

## 4. Step-by-step: intersecting two terms

```text
query: "graph" AND "database"
  graph:    5M postings    database: 200k postings
1. drive with the SHORTER list (database)
2. for each database doc d: graph.advance(d)
   -> level-1 skips ~8k docs per hop, level-0 ~256,
      decode ~1 block per advance
3. cost ~ 200k * (couple of skip comparisons + amortized
   fraction of a block decode) — the 5M-list is mostly never
   touched. Without skips: decode all 5M deltas.
```

Frequencies ride in parallel blocks (same packing, pairs
<doc, freq> — Lucene104PostingsFormat.java:57), feeding BM25;
positions/payloads live in separate files, only read when needed
(phrase queries).

## 5. Worked example — size arithmetic

```text
term with 1,024,000 postings over a 10M-doc corpus:
    average delta ~ 10M/1.024M ~ 9.8 -> most blocks need
    b = 5-7 bits (outlier deltas force a block's b up)
    at b=6: 1.024M x 6 bits = 768 KB  (+ skip data ~ 1%)
    raw 32-bit IDs: 4 MB  -> 5.3x smaller
    and rarer terms compress BETTER (bigger deltas but fewer
    of them); the index-wide norm is ~1 byte per posting.
```

## 6. Worked example — advance() cost

```text
list of 1.024M postings = 4000 blocks of 256
advance(target) worst case:
    level-1 scan: <= 125 entries (4000/32)
    level-0 scan: <= 32 entries
    one block decode: 256 values, SIMD-unpacked
    ~ 160 comparisons + 1 decode vs 512k average without skips
a full AND of two 1M lists touches only the blocks where both
lists actually overlap — sublinear in total postings.
```

## 7. Where systems inherit this

- Lucene → Elasticsearch, OpenSearch, Solr: the lucene104 codec IS
  the storage engine of all three.
- tantivy → quickwit, lnx, paradedb: BitPacker4x blocks, the Rust
  re-derivation of the same design.
- RediSearch, bleve, xapian: same delta+varint/block shape,
  different packing choices.
- Vector-ann pattern 16: centroid -> vector-ID bucket is this
  pattern minus the text; Milvus/qdrant filter bitmaps (roaring,
  pattern 3) are the OTHER doc-set encoding, chosen when sets are
  updated in place rather than write-once-per-segment.
- Graph engines: an adjacency list IS a posting list (WebGraph
  compresses neighbor lists with exactly delta+bit tricks) —
  pattern 7's neighbors array, compressed.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/codecs/lucene104/Lucene104PostingsFormat.java` | the format contract: 256-blocks + vInt tail (53-57), 2-level interleaved skips (61-62, 120), boundary delta rule (205) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/codecs/lucene104/ForUtil.java` | the bit-packing kernel: BLOCK_SIZE=256 (34-36), size arithmetic (160) |
| tantivy | `reference-repos-corpus/tantivy-src/src/postings/compression/mod.rs` | Rust equivalent: BitPacker4x 128-blocks, per-block num_bits (1-14, 41) |
| tantivy | `reference-repos-corpus/tantivy-src/src/postings/` | block_segment_postings.rs, postings_writer.rs — the read/write paths around the blocks |
| quickwit | `reference-repos-corpus/quickwit-src` | tantivy's blocks on object storage |

## 9. Cross-references

- Sibling patterns: `csr-adjacency-layout` (sorted-ID compression
  kinship), `roaring-bitmap-idsets` (the updatable alternative),
  `lsm-compaction-tradeoff` (segments below, pattern 18 candidate),
  `ivf-partitioned-probe` (the geometric descendant).
- Next in category: segment merge lifecycle (Lucene's LSM), then
  BM25 scoring + WAND early termination.
- Verification note (docs_PRD06 thesis): posting encodings are
  bit-exact deterministic given the same doc order — but doc order
  depends on segment merge history. Compare at the LOGICAL level
  (same doc-ID sets per term after mapping) or pin the merge
  policy; byte-diffing across engines is meaningless, semantics
  diffing is cheap.
- 202606 digest overlap: digests covered FTS at the "inverted
  index exists" level; this pair adds the block/skip mechanics
  with line cites and the cost arithmetic.

# Posting Block Compression — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `posting-block-compression-ascii.md` / `posting-block-compression-mermaid.md` |
| One-line job | Store each term's sorted doc-ID list as delta-encoded, bit-packed fixed-size blocks with interleaved skip data — so a list of millions of IDs costs ~1 byte each and can jump forward without decoding |

## 1. The two pulling requirements

```mermaid
flowchart LR
    T["term -> posting list<br/>(sorted doc IDs, up to millions)"]
    T --> R1["SMALL: lists dominate index size"]
    T --> R2["FAST: AND-queries need advance(target)<br/>without decoding everything"]
    R1 & R2 --> A["answer: fixed blocks of deltas,<br/>bit-packed to the block's widest value,<br/>skip entries interleaved between blocks"]
```

## 2. The encoding pipeline

```mermaid
flowchart TD
    IDS["doc IDs: [3, 7, 12, 31, 55, 58, 90...]"]
    IDS --> D["deltas: [3, 4, 5, 19, 24, 3, 32...]<br/>sortedness IS the compression opportunity<br/>(same insight as CSR, roaring)"]
    D --> B["group into fixed blocks:<br/>tantivy 128 (BitPacker4x::BLOCK_LEN,<br/>compression/mod.rs:1-3);<br/>lucene 256 (ForUtil.java:34-36)"]
    B --> P["pick b = bits of the block's max delta<br/>(num_bits_strictly_sorted, mod.rs:41);<br/>pack ALL 128/256 values at b bits"]
    P --> V["tail < block size rides as vInts<br/>(Lucene104PostingsFormat.java:53)"]
```

## 3. Skip data — the flattened B-tree

```mermaid
flowchart TD
    L1["level 1: skip entry every 32 blocks<br/>= every 8192 docs<br/>(Lucene104PostingsFormat.java:61-62, 120)"]
    L0["level 0: skip entry between EVERY block<br/>(last docID + file pointers)"]
    BLK["packed blocks of 256 deltas"]
    L1 --> L0 --> BLK
    BLK --> N["boundary rule (:205): deltas never reset<br/>to absolute — skip entries carry the<br/>absolutes, blocks stay pure deltas"]
    N --> K["a static B-tree flattened into the stream:<br/>pattern 7's offsets array / pattern 1's<br/>SSTable index blocks, same indirection"]
```

## 4. advance(target) walk

```mermaid
sequenceDiagram
    participant Q as advance(target)
    participant S1 as level-1 skips
    participant S0 as level-0 skips
    participant B as one packed block
    Q->>S1: scan while last_doc < target<br/>(each hop skips 8192 docs)
    S1->>S0: land in a 32-block window
    Q->>S0: scan while last_doc < target<br/>(each hop skips 256 docs)
    S0->>B: decode ONE block (SIMD unpack)
    B-->>Q: linear scan inside 256 values
    Note over Q,B: worst case for a 1M list:<br/>~125 + 32 comparisons + 1 decode,<br/>vs 512k average decodes without skips
```

## 5. Intersection economics

```mermaid
flowchart LR
    SH["drive with the SHORTER list:<br/>'database' 200k postings"]
    LG["'graph' 5M postings —<br/>mostly never touched"]
    SH -->|"for each doc d:<br/>graph.advance(d)"| LG
    LG --> C["cost ~ 200k x (few skip comparisons +<br/>amortized block decode) —<br/>sublinear in total postings"]
    C --> FR["freqs ride in parallel packed pairs<br/><doc,freq> (:57) feeding BM25;<br/>positions in separate files,<br/>read only for phrase queries"]
```

## 6. Size arithmetic (1.024M postings, 10M-doc corpus)

```mermaid
flowchart TD
    AV["average delta ~ 10M/1.024M ~ 9.8"]
    AV --> BITS["most blocks need b = 5-7 bits<br/>(one outlier delta raises the<br/>whole block's b)"]
    BITS --> SZ["at b=6: 1.024M x 6 bits = 768 KB<br/>+ ~1% skip data<br/>vs raw 32-bit IDs: 4 MB -> 5.3x"]
    SZ --> NORM["index-wide norm: ~1 byte per posting;<br/>rarer terms compress better —<br/>bigger deltas but far fewer"]
```

## 7. Inheritance map

```mermaid
flowchart LR
    P[posting blocks] --> LU["lucene104 codec IS the storage engine of<br/>Elasticsearch, OpenSearch, Solr"]
    P --> TA["tantivy -> quickwit, lnx, paradedb:<br/>the Rust re-derivation"]
    P --> OT["RediSearch, bleve, xapian: same<br/>delta shape, different packing"]
    P --> IVF["pattern 16: IVF buckets = posting lists<br/>minus the text"]
    P --> RB["pattern 3 roaring: the OTHER doc-set<br/>encoding — chosen when sets mutate in<br/>place instead of write-once-per-segment"]
    P --> WG["WebGraph: adjacency lists ARE posting<br/>lists — pattern 7's neighbors array,<br/>compressed with the same delta tricks"]
```

## 8. The verification angle

```mermaid
flowchart TD
    DET["encoding is bit-exact deterministic<br/>GIVEN the same doc order"] --> DEP["but doc order depends on segment<br/>merge history"]
    DEP --> CH["so: compare at the LOGICAL level —<br/>same doc-ID SETS per term after ID<br/>mapping — or pin the merge policy"]
    CH --> TH["docs_PRD06 thesis condition 3 again:<br/>byte-diffing across engines is meaningless;<br/>semantic diffing (per-term set equality +<br/>per-query score tolerance) is cheap and<br/>total — FTS is a FRIENDLY differential<br/>target compared to ANN"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K7["pattern 7 CSR: sorted-ID compression,<br/>offsets-as-index"]
    K3["pattern 3 roaring: updatable alternative<br/>per data shape"]
    K1["pattern 1 LSM: the segments these blocks<br/>live in (next pattern)"]
    K16["pattern 16 IVF: the geometric descendant"]
    K7 & K3 & K1 & K16 --> CORE["one corpus-wide law: sorted IDs + deltas +<br/>blocks + a skip level = the universal<br/>read-optimized set format"]
```

## 9b. The write path — how blocks get built

```mermaid
sequenceDiagram
    participant D as documents
    participant W as postings_writer (in RAM)
    participant S as segment files (immutable)
    D->>W: tokenize; per term, append<br/>(doc_id, freq, positions) —<br/>tantivy postings_writer.rs
    Note over W: per-term buffers keep doc IDs in<br/>arrival order = already sorted<br/>(doc IDs are assigned monotonically)
    W->>S: on segment flush: for each term,<br/>delta-encode, cut 128/256-blocks,<br/>pick per-block num_bits, pack,<br/>interleave skip entries
    Note over S: the encoding happens ONCE, at flush —<br/>blocks are never edited in place;<br/>updates create new segments and<br/>deletes are tombstone bitmaps<br/>(the LSM discipline, patterns 1/4)
    S->>S: merges re-decode, re-map doc IDs,<br/>re-encode — cheap because decode is<br/>a linear SIMD pass
```

Write-once is why the format can afford per-block optimal bit
widths and interleaved skips: no update path ever has to patch a
packed block. Mutable-set workloads flip to roaring bitmaps
(pattern 3) precisely because they cannot make this promise.

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/codecs/lucene104/Lucene104PostingsFormat.java` | format contract: 256-blocks + vInt tail (53-57), 2-level skips (61-62, 120), boundary rule (205) |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/codecs/lucene104/ForUtil.java` | bit-packing kernel: BLOCK_SIZE=256 (34-36), size arithmetic (160) |
| tantivy | `reference-repos-corpus/tantivy-src/src/postings/compression/mod.rs` | BitPacker4x 128-blocks, per-block num_bits (1-14, 41) |
| tantivy | `reference-repos-corpus/tantivy-src/src/postings/` | read/write paths around the blocks |
| quickwit | `reference-repos-corpus/quickwit-src` | tantivy's blocks on object storage |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Next in category: segment merge lifecycle (Lucene's LSM), then
  BM25 + WAND early termination.
- Paper trail: FOR/PForDelta compression literature and the BM25 /
  WAND papers queued in `research-papers-ledger.md`.
- 202606 digest overlap: digests covered FTS at the "inverted
  index exists" level; this pair adds block/skip mechanics with
  line cites and the cost arithmetic.

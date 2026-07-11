# Roaring Bitmap IDsets — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `roaring-bitmap-idsets-ascii.md` / `roaring-bitmap-idsets-mermaid.md` |
| One-line job | Store sets of 32-bit IDs in a form that is simultaneously small, fast to intersect, and fast to iterate — a different physical container per 65,536-value chunk |

## 1. Why neither naive layout works

```mermaid
flowchart TD
    NEED[set of u32 IDs:<br/>doc IDs, node IDs, row IDs] --> SA[sorted u32 array<br/>4 B/element]
    NEED --> BM[plain bitmap<br/>1 bit per possible value]
    SA -- "dense 1M IDs = 4 MB<br/>(bitmap: 128 KB)" --> BAD1[wasteful when dense]
    BM -- "{5, 900M} = 112 MB<br/>for two elements" --> BAD2[wasteful when sparse]
    BAD1 --> R[Roaring: split universe into<br/>65,536-value chunks,<br/>pick cheapest container per chunk]
    BAD2 --> R
```

## 2. The two-level structure

```mermaid
flowchart LR
    V["value v: u32"] --> H["high16(v) = chunk key"]
    V --> L["low16(v) = position in chunk"]
    H --> TOP["top level: sorted array of<br/>(key, container*) pairs<br/>RoaringArray.java / roaring_array_t /<br/>Vec&lt;Container&gt; in Rust"]
    TOP --> A["ARRAY: sorted u16[card]<br/>when card <= 4096 (<= 8 KB)"]
    TOP --> B["BITMAP: u64[1024], fixed 8 KB<br/>when card > 4096"]
    TOP --> RN["RUN: (start,len) u16 pairs<br/>when few runs (RLE)"]
```

The crossover 4096: array of 4096 u16 = 8 KB = the fixed bitmap size —
hard-coded as `DEFAULT_MAX_SIZE` in
`reference-repos-corpus/RoaringBitmap-src/roaringbitmap/src/main/java/org/roaringbitmap/ArrayContainer.java`
(line 28; upgrade at line 124).

## 3. add(v) state machine

```mermaid
stateDiagram-v2
    [*] --> FindChunk: k = v >> 16, binary search
    FindChunk --> NewArray: no container for k
    NewArray --> [*]: ARRAY {low16(v)}
    FindChunk --> Array: container is ARRAY
    Array --> Array: insert low16, card <= 4096
    Array --> Bitmap: card > 4096 -> convert<br/>(convert.c / ArrayContainer.java:124)
    FindChunk --> Bitmap: container is BITMAP
    Bitmap --> Bitmap: bitmap[low16>>6] |= 1<<(low16&63)
    Bitmap --> Run: runOptimize pass, RLE smaller<br/>(containers/run.c)
```

## 4. intersect(A, B) — kind-pair dispatch

```mermaid
flowchart TD
    I[intersect A, B] --> GK[galloping merge of key arrays —<br/>chunks in only one input skipped wholesale]
    GK --> D{container kind pair}
    D -- "ARRAY x ARRAY" --> K1[galloping u16 merge]
    D -- "ARRAY x BITMAP" --> K2["per array element: one bit test<br/>O(small side)"]
    D -- "BITMAP x BITMAP" --> K3["1024 u64 ANDs — SIMD:<br/>AVX2/AVX-512 runtime dispatch<br/>(isadetection.c, bitset_util.c)"]
    D -- "RUN x *" --> K4[interval intersection]
```

The dispatch table is the heart of every implementation: CRoaring's
`containers/mixed_*.c` family, Java's `Container` subclasses, Rust's
`ops.rs` over the container enum.

## 5. Worked example — three shapes, three costs

1,000,000 IDs stored, three distributions:

```mermaid
flowchart LR
    subgraph A [uniform sparse over 2^32]
        A1["~16k chunks x ~61 elems<br/>all ARRAY"] --> A2["~2.1 MB<br/>(u32 array: 4 MB)"]
    end
    subgraph B [dense block 0..1M]
        B1["16 containers, one RUN each<br/>after runOptimize"] --> B2["~130 B<br/>(bitmap: 128 KB)"]
    end
    subgraph C [50% random of 0..2M]
        C1["~31 chunks x ~32k elems<br/>all BITMAP"] --> C2["~248 KB<br/>(u32 array: 4 MB)"]
    end
```

Same API, three physical layouts, each within ~2x of the entropy floor
for its shape.

## 6. Worked example — graph label intersection

`MATCH (n:Person:Employee)`: Person = 5M dense IDs (BITMAP chunks),
Employee = 200k sparse IDs (ARRAY chunks).

```mermaid
sequenceDiagram
    participant E as Employee (200k, ARRAY)
    participant P as Person (5M, BITMAP)
    participant R as result
    E->>P: for each of ~200k elements: one bit probe
    P-->>R: hit? append low16 to result ARRAY
    Note over E,R: ~200k probes, no allocation —<br/>naive u32 merge walks 5.2M elements
```

Cost follows the SMALL side, not the universe — that is the asymmetric
(ARRAY, BITMAP) kernel's gift.

## 7. Corpus usage map

```mermaid
flowchart LR
    RB[Roaring] --> LU[Lucene/Elasticsearch<br/>filter caches, doc-ID sets]
    RB --> FK[FalkorDB / GraphBLAS<br/>dense-region vectors, same crossover idea]
    RB --> KB[this repo: label -> IDset index<br/>over CSR snapshot; 8 KB BITMAP<br/>chunks mmap cleanly]
    RB -. positional queries instead<br/>of membership .-> SD[sdsl-lite rank/select<br/>succinct alternative]
```

## 8. Serialization — why the format is the structure

Roaring's portable serialization is essentially the in-memory layout
written out — which is why the format is interoperable across the C,
Java, and Rust implementations and why BITMAP containers can be
memory-mapped in place:

```mermaid
flowchart LR
    SER["portable format"] --> HDR["header: cookie +<br/>container count"]
    HDR --> KEYS["sorted (key, cardinality)<br/>descriptor array"]
    KEYS --> OFF["offset table<br/>(random access to containers)"]
    OFF --> DATA["container payloads:<br/>u16 arrays / 8 KB bitmaps /<br/>run pairs, back to back"]
    DATA -. "mmap + offset table =<br/>query without deserializing" .-> Q[reader]
```

Cross-implementation compatibility is exercised in each repo's test
suites (`reference-repos-corpus/roaring-rs-src/roaring/src/bitmap/ops_with_serialized.rs`
even implements set operations directly against the serialized form —
the zero-copy endpoint of this design).

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| CRoaring | `reference-repos-corpus/CRoaring-src/src/containers` | three container kinds + convert.c crossover |
| CRoaring | `reference-repos-corpus/CRoaring-src/src/containers/mixed_intersection.c` | kind-pair kernels |
| CRoaring | `reference-repos-corpus/CRoaring-src/src/isadetection.c` | runtime SIMD dispatch |
| RoaringBitmap (Java) | `reference-repos-corpus/RoaringBitmap-src/roaringbitmap/src/main/java/org/roaringbitmap/ArrayContainer.java` | DEFAULT_MAX_SIZE=4096 crossover |
| RoaringBitmap (Java) | `reference-repos-corpus/RoaringBitmap-src/roaringbitmap/src/main/java/org/roaringbitmap/RoaringArray.java` | top-level key→container array |
| roaring-rs | `reference-repos-corpus/roaring-rs-src/roaring/src/bitmap/container.rs` | Rust container enum |
| roaring-rs | `reference-repos-corpus/roaring-rs-src/roaring/src/bitmap/ops.rs` | Rust set-op dispatch |

## 10. Cross-references

- Sibling patterns: posting-list compression (Roaring vs delta-varint
  blocks); CSR adjacency layout (shared dense-ID prerequisite).
- 202606 digest overlap: none — bitmap kernels were a gap closed in
  corpus research round 3.

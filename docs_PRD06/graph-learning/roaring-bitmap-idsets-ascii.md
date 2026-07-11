# Roaring Bitmap IDsets — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `roaring-bitmap-idsets-ascii.md` / `roaring-bitmap-idsets-mermaid.md` |
| One-line job | Store sets of 32-bit IDs (node IDs, doc IDs, row IDs) in a form that is simultaneously small, fast to intersect, and fast to iterate — by choosing a different physical container per 65,536-value chunk |

## 1. The job

Every system in this corpus needs sets of integer IDs: which documents
match a term, which nodes carry a label, which rows survive a filter.
The naive choices both fail at scale:

```text
sorted array of u32:   4 bytes/element — great sparse, awful dense
                       (1M dense IDs = 4 MB for what a bitmap does in 128 KB)
plain bitmap:          1 bit per POSSIBLE value — great dense, awful sparse
                       ({5, 900M} costs 112 MB for two elements)
```

Roaring's move: split the 32-bit universe by the high 16 bits into
chunks of 65,536 values, and pick the cheapest container *per chunk*
based on how full that chunk actually is.

## 2. Raw data shape

```text
value v (u32):   [ high16(v) | low16(v) ]
                      |            |
                      v            v
              chunk index    position inside chunk

top level: sorted array of (key=high16, container*) pairs
           (RoaringArray.java in Java; roaring_array_t in C;
            RoaringBitmap { containers: Vec<Container> } in Rust)

container kinds (per 65,536-value chunk):
  ARRAY   sorted u16[cardinality]      when card <= 4096   <= 8 KB
  BITMAP  u64[1024] fixed 8 KB         when card >  4096
  RUN     (start,len) u16 pairs        when runs are few (RLE)
```

The magic constant 4096: an array of 4096 u16 = 8 KB = the size of the
fixed bitmap. Above 4096 elements the bitmap is smaller AND O(1) to
probe, so 4096 is the crossover — hard-coded as `DEFAULT_MAX_SIZE` in
`ArrayContainer.java:28`, with the array→bitmap upgrade at line 124.

## 3. Step-by-step: add(v)

```text
1. k = v >> 16; find k in the top-level key array (binary search).
2. no container for k?  create an ARRAY container with {v & 0xFFFF}.
3. container is ARRAY:
     binary-search-insert low16(v);
     if cardinality now > 4096 -> convert to BITMAP
     (ArrayContainer.java:124; CRoaring containers/convert.c).
4. container is BITMAP:
     word = low16 >> 6; bit = low16 & 63;
     bitmap[word] |= 1 << bit; cardinality += (was it new).
5. optional optimize pass (runOptimize): scan each container, count its
   runs; if RUN encoding is smaller, convert (CRoaring
   containers/run.c; roaring-rs bitmap/container.rs holds the enum).
```

## 4. Step-by-step: intersect(A, B) — why it's fast

Intersection never materializes anything for chunks that exist in only
one input — a galloping merge over the two sorted key arrays skips them
wholesale. For chunk pairs that do meet, the work is dispatched on the
container-kind pair:

```text
(ARRAY,  ARRAY):  galloping merge of two sorted u16 arrays
(ARRAY,  BITMAP): for each array element, test one bit  -> O(small side)
(BITMAP, BITMAP): 1024 u64 ANDs — vectorizable; CRoaring runs this
                  under AVX2/AVX-512 dispatch (mixed_intersection.c,
                  bitset_util.c, isadetection.c pick the kernel at
                  runtime)
(RUN, *):         interval intersection
```

This kind-pair dispatch table is the heart of every implementation:
CRoaring's `containers/mixed_*.c` family, Java's `Container` subclasses,
Rust's `ops.rs` on the container enum.

## 5. Worked example 1 — three shapes, three costs

Store 1,000,000 IDs, three distributions:

```text
A) uniform sparse over [0, 2^32):
   ~16k chunks, each ~61 elements -> all ARRAY
   size ~ 1M x 2 B + chunk overhead ~ 2.1 MB   (u32 array: 4 MB)

B) dense block 0..1,000,000:
   15 full chunks + remainder -> RUN (one run each!) after optimize
   size ~ 16 containers x ~8 B ~ 130 B          (plain bitmap: 128 KB
                                                 u32 array: 4 MB)

C) 50% random of 0..2,000,000:
   ~31 chunks at ~32k elements each -> all BITMAP
   size ~ 31 x 8 KB ~ 248 KB                    (u32 array: 4 MB)
```

Same API, three different physical layouts, each within ~2x of the
information-theoretic floor for its shape.

## 6. Worked example 2 — label scan intersection in a graph

"MATCH (n:Person:Employee)" = intersect two label ID-sets.
Person = 5M IDs, dense (bitmap containers); Employee = 200k IDs,
sparse (array containers).

```text
work = for each of Employee's ~200k array elements: one bit test in
       Person's bitmap chunk  ->  ~200k bit probes, no allocation
       ~ microseconds; the naive sorted-u32 merge walks 5.2M elements
result cardinality 180k -> stays ARRAY per chunk -> result is small too
```

The asymmetric (ARRAY, BITMAP) kernel is why: cost follows the SMALL
side, not the universe.

## 7. Where this corpus uses it

- Lucene/Elasticsearch: filter caches and doc-ID sets are Roaring
  (Java `RoaringBitmap` began as the Lucene/Druid ecosystem's shared
  need).
- FalkorDB: GraphBLAS sparse matrices use bitmap-flavored vectors for
  dense regions — same crossover logic at the matrix level.
- This repo: dense u32 node IDs are exactly the Roaring-friendly shape;
  a label→IDset index over the CSR snapshot is the natural Roaring
  application (per-chunk containers mmap cleanly since BITMAP chunks
  are fixed 8 KB).

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| CRoaring | `reference-repos-corpus/CRoaring-src/src/containers` | the three container kinds + convert.c crossover logic |
| CRoaring | `reference-repos-corpus/CRoaring-src/src/containers/mixed_intersection.c` | kind-pair dispatch kernels |
| CRoaring | `reference-repos-corpus/CRoaring-src/src/isadetection.c` | runtime SIMD (AVX2/AVX-512) kernel selection |
| RoaringBitmap (Java) | `reference-repos-corpus/RoaringBitmap-src/roaringbitmap/src/main/java/org/roaringbitmap/ArrayContainer.java` | DEFAULT_MAX_SIZE=4096 crossover (lines 28, 124) |
| RoaringBitmap (Java) | `reference-repos-corpus/RoaringBitmap-src/roaringbitmap/src/main/java/org/roaringbitmap/RoaringArray.java` | top-level key→container sorted array |
| roaring-rs | `reference-repos-corpus/roaring-rs-src/roaring/src/bitmap/container.rs` | Rust container enum |
| roaring-rs | `reference-repos-corpus/roaring-rs-src/roaring/src/bitmap/ops.rs` | Rust set-operation dispatch |

## 9. Cross-references

- Sibling patterns: posting-list compression (Roaring is one of the two
  dominant answers; delta-varint blocks are the other); CSR adjacency
  layout (dense-ID prerequisite is shared).
- sdsl-lite (`storage-engine` corpus) holds the succinct-structure
  alternative: rank/select over compressed bitvectors when you need
  positional queries, not just membership.
- 202606 digest overlap: none — bitmap kernels were a recorded gap,
  closed by round 3 of the corpus research.

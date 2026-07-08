# Arch06 — Beyond the Format: Four New Lever Classes (ASCII Explainers)

Date: 2026-07-08
Status: ideation (greenfield framing, current repo code = POC only)
Predecessors: Arch01 (five base architectures A-E), Arch02 (seven algorithm
families), Arch03 (composite + build order), Arch04 (brownfield debug),
Arch05 (GRAIN format), Arch-Summary-ASCII-v1 (one-page recap).

Question this doc answers: *"Apart from the storage formats already
suggested, is there any other way to make these algorithms consume less RAM
and run faster, using Rust?"*

Answer: yes — four lever classes that are **orthogonal to byte layout**.
Everything before this doc optimized where the GRAPH bytes live. These
levers optimize the *other* half of RAM (scratch state), the *execution
model*, the *amount of work done*, and the *machine-level efficiency*.
They multiply with GRAIN; they do not replace it.

---

## 0. The Map: What Was Already Unique vs What Is New

```
 PREVIOUS UNIQUE IDEAS (Arch01-05)          ATTACK SURFACE
 -----------------------------------       -----------------------
 A  flat CSR monolith                       graph bytes (layout)
 B  2D tiles / out-of-core                  graph bytes (residency)
 C  budget machine (estimate/spill/reject)  runtime POLICY
 D  multi-format foundry -> manifest flags  catalog
 E  algebra engine (semirings)              kernel expression
 GRAIN strata + manifest-as-estimator       graph bytes + pricing
 Axis1 content-addressed generations        graph bytes over TIME
 Axis2 memoized results                     recompute avoidance
 Axis3 sketches in manifest                 approximate answers

 NEW IN ARCH06                              ATTACK SURFACE
 -----------------------------------       -----------------------
 L1 scratch compression                     WORKSPACE bytes  <- untouched!
 L2 semi-external model (O(V) RAM)          execution model
 L3 do-less-work algorithms                 wall-clock time
 L4 Rust mechanical wins                    constant factors
```

The elephant in the room the previous docs missed:

```
        RAM DURING A JOB = GRAPH BYTES + SCRATCH STATE
                            ^^^^^^^^^^^   ^^^^^^^^^^^^^
        Arch01-05 attacked  THIS ONLY     ...NOT THIS

        And for the two families that actually OOM in the wild
        (Louvain 15%, NodeSim 12%), SCRATCH is the killer:

        NodeSim on LDBC100:  graph ~ a few GB
                             candidate pairs scratch ~ 100+ GB   <- !!
```

---

## L1. Compress the SCRATCH, Not the Graph

### L1.a Frontier = bitmap, not queue

```
  BEFORE (queue of u32 ids)          AFTER (1 bit per vertex)
  [17][94][3][1052][77]...           01001000100000101...
  32 bits per frontier entry         1 bit per vertex, total

  1B-vertex graph, wide frontier:
  queue  : up to 4 GB                bitmap : 125 MB  (32x less)
```

Prior art: Ligra's dense frontier representation [R1].

### L1.b Quantized numeric state

```
  PageRank vector, 1B vertices:
    f64 : 8 GB      f32 : 4 GB      fixed-point u32 : 4 GB

  FastRP embeddings, 1B x 128 dims:
    f32 : 512 GB    f16 : 256 GB    int8 : 128 GB   (4x less)

  Embeddings tolerate int8 — this is exactly how vector databases
  ship ANN indexes (scalar quantization) [R2].
```

### L1.c Sketched state — the NodeSim rescue

```
  EXACT NodeSim:                     SKETCHED NodeSim:
  per vertex: full neighbor set      per vertex: fixed 64-256 B sketch
  memory ~ O(sum of degrees^2)       (MinHash / HyperLogLog [R3][R4])
  = EXPLODES on hubs                 memory ~ O(V * 256B) = BOUNDED

  ladder:  sketch ALL pairs cheaply --> exact re-rank only top-k
```

This converts the worst family (12% adoption weight, unbounded scratch)
into constant-memory-per-vertex with an exactness knob.

### L1.d Strata apply to STATE too (new use of an old idea)

```
  GRAIN cut the GRAPH by degree rank:   hot | warm | cold
  L1.d cuts the WORKSPACE the same way:

  state[0..k)      hot vertices   -> resident in RAM (touched constantly)
  state[k..n)      cold tail      -> spillable file, touched rarely

  Same ranks.dense array, second dividend.
```

### Comparison vs previous ideas

```
                          graph bytes   scratch bytes
  GRAIN (Arch05)          2-4x smaller  unchanged
  L1 (this)               unchanged     4-30x smaller (family-dependent)
  together                BOTH          -> the 8-16 GB box promise
```

---

## L2. Semi-External Model: RAM = O(V) By Construction

The strongest guarantee in this document.

```
  IN-CORE (A):            SPILL (C on B):           SEMI-EXTERNAL (L2):
  graph + scratch         graph tiles swap in/out   ONLY vertex state in RAM
  all in RAM              as budget allows          edges STREAM from disk
                                                    every pass, never resident
  RAM = O(V + E)          RAM = budget (variable)   RAM = O(V)  FIXED
  fails if too big        finishes, slow            finishes, predictable
```

Why it matters:

```
  typical ratio |E| / |V| = 10-50x
  50 GB graph (edges)  -->  vertex state often < 1 GB

  +--------------------------------------------------+
  | RAM  [ vertex state ~1GB ]                        |
  |          ^         ^          ^                   |
  |          |         |          |   updates         |
  | DISK  [blk1]-->[blk2]-->[blk3]--> ... (streamed)  |
  +--------------------------------------------------+
```

Prior art: X-Stream's edge-centric streaming [R5], GraphChi shards [R6],
GridGraph dual sliding windows [R7]. GRAIN's cold 2D blocks are already
the right physical shape — L2 is a *scheduling mode* over the same files.

Difference vs C's spill: spill is *reactive* ("didn't fit, start swapping");
semi-external is *proactive* ("we never planned to hold edges at all").
The receipt becomes trivially honest: RAM = |V| x state_width, closed form.

---

## L3. Do Less Work (Same Answer, Fewer Edge Touches)

Storage levers make each unit of work cheaper. These make there be
FEWER units of work.

### L3.a Direction-optimizing traversal (BFS/WCC/paths)

```
  PUSH (frontier small):            PULL (frontier huge):
  each frontier vertex writes       each UNVISITED vertex asks
  to its neighbors                  "is any of my in-neighbors
                                     in the frontier?" -> stop at
                                     first hit (early exit)

  frontier size ---> switch when frontier > ~5-15% of V
  2-10x fewer edge inspections on real graphs [R8]
```

### L3.b Delta-driven convergence (PageRank/Louvain)

```
  iteration:      1     2     3     4     5    ...
  vertices with
  meaningful      ####  ###   ##    #     .
  change (>eps)   100%  40%   12%   3%    0.5%

  classic engine: touches ALL vertices every iteration
  delta engine  : touches ONLY the changed ones [R9]

  ...and combined with B/GRAIN tiles:
  if every vertex in cold block (i,j) converged -> SKIP THE WHOLE BLOCK
  (compute lever and storage lever click together)
```

### L3.c Cache-aware reordering (one-time build cost, permanent dividend)

```
  degree ranking (GRAIN)  : who is BIG        -> strata cuts
  recursive bisection [R10]: who is TOGETHER  -> neighbors get nearby ids

  effect: adjacent accesses land in the same cache line / same tile
          AND gaps between neighbor ids shrink -> Elias-Fano compresses
          better. Both speed and size, paid once at build time.
```

---

## L4. Rust Mechanical Wins (Why Rust Specifically)

Constant factors, but multiplicative and unavailable to the JVM incumbent.

```
  L4.a ZERO-COPY:   file --mmap--> &[Edge]  (zerocopy/bytemuck casts)
                    no deserialization, no heap copy; the file IS the
                    runtime structure. (POC already proved this pattern.)

  L4.b MONOMORPHIZED KERNELS:
       fn traverse<S: Semiring>(...)   -- generic source
           |compile|
       traverse_f32_plus_times()       -- specialized machine code,
       traverse_bool_or_and()             no vtable, no boxing
       = Arch01-E's algebra flexibility at ZERO runtime cost.
       JVM GDS pays object headers + GC for the same abstraction.

  L4.c SIMD (std::simd): Elias-Fano decode, rank/select, dot products
       -> compressed strata read at near-flat-CSR speed [R11].

  L4.d io_uring PREFETCH PIPELINE:
       compute:   [ blk k ][ blk k+1 ][ blk k+2 ]
       disk:    [k+1][k+2][k+3]  ...always 2-3 blocks ahead
       -> streaming (L2) hides behind compute; disk ~free if
          compute-bound [R12].

  L4.e ONE ARENA: a single budgeted workspace arena, reused across jobs.
       allocated bytes == receipt bytes, byte for byte.
       The honesty claim becomes mechanically enforced, not audited.
```

---

## Cross-Lever Analysis (vs all previous unique ideas)

```
  lever                RAM effect            SPEED effect      overlaps with
  -------------------  --------------------  ----------------  -------------
  GRAIN strata         graph 2-4x smaller    scans faster      A,B,E fused
  C budget machine     prevents OOM          n/a (policy)      needs L1-L2
  Axis1 deltas         storage over time     incremental       Axis2
  Axis2 memoized       avoids recompute      10-100x on rerun  Axis1
  Axis3 sketches       n/a                   instant approx    L1.c (same tech!)
  L1 scratch compress  workspace 4-30x       mild              C prices it
  L2 semi-external     RAM = O(V) FIXED      slower/predictable B blocks reused
  L3 less work         mild                  2-10x fewer ops   B skip, Axis2
  L4 Rust mechanics    zero-copy, no GC      1.5-5x constants  everything
```

Stacking picture — the same 100 GB job on a 16 GB box:

```
  naive in-core         : |################ 130 GB needed ##########| OOM
  + GRAIN strata        : |###### 45 GB #######|                     OOM
  + L1 scratch compress : |## 12 GB ##|                              FITS
  + L2 semi-external    : |# 3 GB #|                                 EASY
  + L3 less work        : (same RAM, finishes 2-10x sooner)
  + L4 mechanics        : (same RAM, 1.5-5x sooner again)
```

Multiplication, not addition: format cuts graph bytes, L1 cuts scratch,
L2 bounds the total, L3/L4 cut wall-clock. No pair cancels out.

### Decision Filter (which lever first)

- **Strongest if everything goes normally:** L1 (scratch compression).
  It attacks the actual OOM cause of the two worst families, is pure
  Rust code (no format change), and every byte it saves makes C's
  receipts smaller and more impressive.
- **Safest if things go badly:** L2 (semi-external). Even if estimates
  are wrong, RAM = O(V) is a guarantee, not a prediction.
- **Fastest uncertainty-collapser:** prototype L1.c sketched NodeSim on
  the POC (MinHash, 128 B/vertex) and compare top-k overlap vs exact —
  one week, pure library code, gates the biggest claim in this doc.

---

## References

- [R1] Ligra: A Lightweight Graph Processing Framework for Shared Memory
  (PPoPP'13) — https://www.cs.cmu.edu/~guyb/papers/SB13.pdf
- [R2] Faiss / scalar quantization for vector search —
  https://github.com/facebookresearch/faiss/wiki/Faiss-indexes
- [R3] MinHash: Broder, "On the resemblance and containment of documents"
  (1997) — https://ieeexplore.ieee.org/document/666900
- [R4] HyperLogLog: Flajolet et al. (2007) —
  https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
- [R5] X-Stream: Edge-centric Graph Processing using Streaming Partitions
  (SOSP'13) — https://dl.acm.org/doi/10.1145/2517349.2522740
- [R6] GraphChi: Large-Scale Graph Computation on Just a PC (OSDI'12) —
  https://www.usenix.org/system/files/conference/osdi12/osdi12-final-126.pdf
- [R7] GridGraph: Large-Scale Graph Processing on a Single Machine
  (USENIX ATC'15) —
  https://www.usenix.org/system/files/conference/atc15/atc15-paper-zhu.pdf
- [R8] Beamer et al., Direction-Optimizing Breadth-First Search (SC'12) —
  https://scottbeamer.net/pubs/beamer-sc2012.pdf
- [R9] Zhang et al., Maiter: delta-based accumulative iterative computation
  (IEEE TPDS 2014) — https://arxiv.org/abs/1710.05785
- [R10] Dhulipala et al., Compressing Graphs and Indexes with Recursive
  Graph Bisection (KDD'16) — https://arxiv.org/abs/1602.08820
- [R11] Elias-Fano decoding with SIMD (IPDPS'23) —
  https://doi.org/10.1109/ipdps54959.2023.00013
- [R12] io_uring design document (kernel.dk) —
  https://kernel.dk/io_uring.pdf

Note: RAM/speed multipliers above are modeled from the cited literature,
not yet measured on this codebase; the L1.c prototype is the first
measurement gate.

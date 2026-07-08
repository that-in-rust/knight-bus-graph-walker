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

## Worked Example: WCC, The Most Popular Algorithm (~20% of GDS adoption)

"Which nodes belong to the same group?" — the first job every fraud /
dedup / entity-resolution team runs, usually daily.

### The seven options created so far, applied to WCC

```
 #  option (doc)             what it does for WCC          limit
 -- ------------------------ ----------------------------- ------------------
 1  flat photo    (A/01)     one straight CSR scan          dies > RAM
 2  tiles         (B/01)     process tile by tile;          slower per pass
                             SKIP settled tiles
 3  bouncer       (C/01)     price the job FIRST:           policy only,
                             fit->RAM  big->tiles           needs 1/2 under it
                             hopeless->reject + bill
 4  GRAIN         (05)       1+2 fused in one format;       estimate is the
                             1 KB manifest = the bill       load-bearing bet
                             by pure arithmetic
 5  tiny scratch  (06-L1)    frontier bitmap: 32x less      --
                             label state, quantized
 6  O(V) mode     (06-L2)    only labels in RAM, edges      slower than
                             stream from disk               in-core
                             RAM = O(V) GUARANTEED
 7  remember      (axes)     graph moved 2% since gen N?    needs axis-1/2
                             re-run WCC on the 2% only,     format support
                             seeded from gen N's labels
```

How they stack on one job:

```
  WCC on a 50 GB edge file, 16 GB box:

  1 flat        [############### needs ~60 GB ###############]  OOM
  2 tiles       [##### budget-bound, finishes slowly #####]     OK
  4 GRAIN       bill known from 1 KB BEFORE anything runs       OK
  5 scratch     label+frontier state shrinks 4-30x              OK
  6 O(V) mode   [# <1 GB resident #]  edges stream past         EASY
  7 remember    tomorrow's re-run touches only the 2% delta     ~instant
```

### Shreyas Doshi's selling narrative

Do not sell "faster WCC". Sell **the end of WCC anxiety**.

```
  THEIR lived experience (Neo4j Aura):     OURS:
  1. guess a session size (RAM)            1. point at your graph
  2. pre-pay by GB-hour for the guess      2. 1 second later: exact bill,
  3. wait                                     from 1 KB of metadata
  4. maybe OOM anyway                      3. it finishes — RAM if it fits,
     = PAY-TO-FIND-OUT                        streaming if it doesn't,
                                              on the 16 GB box you own
                                              = KNOW-BEFORE-YOU-RUN
```

Insight-level differentiation: Neo4j structurally cannot copy this —
their revenue is metered by the RAM you are forced to over-provision.
Selling certainty would cannibalize their own meter.

### Estimated impact (modeled, not yet measured)

```
  RAM        : their sizing guide -> 110+ GB sessions for LDBC100-class
               analytics; stacked options -> low single-digit GB for WCC
               (O(V) label state). ~10-40x less.
  Money      : ~$44/run on a 110 GB Aura session vs $0 on owned hardware.
  Capability : "impossible without a special machine" -> overnight job
               (the impossible->possible claim markets better than speed).
  Re-runs    : incremental WCC on a 2% daily delta ~10-100x faster than
               recompute — and daily re-runs ARE the fraud-pipeline usage.
  Moat       : the PROMISE, not the engine: "know before you run, finish
               anyway, on your hardware." Features get copied in quarters;
               this can't be copied without breaking their billing model.
```

---

## Worked Example 2: Louvain/Leiden, #2 Algorithm (~15% of GDS adoption)

"Find the natural clusters" — fraud rings, segmentation, and now GraphRAG
(Microsoft GraphRAG runs hierarchical Leiden on every corpus re-index).
This is the family with the WORST OOM reputation — the story is sharper
than WCC's.

### Why Louvain is the OOM villain

```
  WCC scratch, per vertex:      LOUVAIN scratch, per vertex:
  [ label: u32 ]                [ community -> weight tally map ]
  4 bytes, fixed                UNBOUNDED (one entry per neighboring
                                community; hubs touch thousands)
                                ...and after each level, build a whole
                                NEW coarser graph. Then do it again.

  Neo4j's own sizing guide: LDBC100 Louvain ~ 119 GB session.
  The graph is a few GB. THE SCRATCH IS THE PROBLEM.
```

### The seven options applied to Louvain

```
 #  option (doc)             what it does for LOUVAIN       verdict
 -- ------------------------ ------------------------------ ---------------
 1  flat photo    (A/01)     shrinks graph bytes only;      does NOT save it
                             tally scratch untouched
 2  tiles         (B/01)     streams edges; tallies still   partial help
                             all in RAM
 3  bouncer       (C/01)     price the tallies BEFORE       first real save
                             running: fit / bounded-spill
                             / reject with the bill
 4  GRAIN         (05)       each coarser level = a NEW     multi-level
                             TINY GENERATION of the same    machinery free;
                             format (1B verts -> ~5M at     manifest CDF
                             level 2); manifest prices      prices tallies
                             the tally scratch
 5  tiny scratch  (06-L1)    cap tallies per vertex         THE BIG GUN
                             (top-k communities, quantized  4-30x scratch
                             weights) -> bounded B/vertex   cut (modeled)
 6  O(V) mode     (06-L2)    hot-vertex tallies resident,   degree-rank cut
                             cold-tail tallies spill        on WORKSPACE
 7  remember      (axes)     warm-start from yesterday's    10-50x on the
                             assignment; graph moved 2%     recurring
                             -> minutes, not hours          re-index case
```

Stack on one job:

```
  Louvain on LDBC100-class graph, 16 GB box:

  their sizing   [############ 119 GB session ############]   pre-pay & pray
  1-2 layout     [######### tallies still ~100 GB ########]   still OOM
  3 bouncer      priced first -> bounded-spill mode           finishes
  4 GRAIN        levels = tiny generations, bill = arithmetic  finishes
  5 capped tally [## low GB ##]                                FITS
  6 O(V) split   [# hot resident, tail spills #]               EASY
  7 warm-start   next day's 2% delta -> minutes                ~instant
```

### Shreyas Doshi's selling narrative

Do not sell "faster clustering". Sell **"the cluster job that never lies
to you."**

```
  Louvain burns users worst:               Our promise:
  runs for HOURS, then OOMs,               tally bill = arithmetic BEFORE
  on a session pre-paid by the GB          start; levels run as tiny
  = the most expensive way to learn        generations; 2% overnight change
    your graph didn't fit                  -> warm-start in minutes
```

And the buyer is newly urgent: every GraphRAG pipeline runs Leiden on
every re-index — buyers who are GPU-poor on RAM and cost-anxious.

### Estimated impact (modeled, not yet measured)

```
  RAM        : 119 GB session -> low single-digit GB with capped
               tallies + spill (~20-50x less).
  Money      : ~$48/run on a 119 GB Aura session vs $0 owned.
  Recurring  : warm-started re-index 10-50x faster — and re-indexing
               IS the recurring GraphRAG cost.
  Moat       : same certainty moat as WCC — plus the honesty is worth
               MORE here because this is the family that lies (dies
               late, after hours of paid runtime).
```

---

## Worked Example 3: PageRank, #3 Algorithm (~15% of GDS adoption)

"Who matters most?" — influence ranking, ML feature generation, seed
scoring. Different personality from WCC/Louvain: PageRank is a pure
**scan** problem — scratch is tiny and fixed (1-2 floats per vertex);
the pain is reading ALL edges 20-50 times. The dominant levers flip.

```
  LOUVAIN pain:  scratch explodes        (state problem)
  PAGERANK pain: edges re-read 30 times  (bandwidth problem)
```

### The options applied to PageRank

```
 #  option (doc)             what it does for PAGERANK      verdict
 -- ------------------------ ------------------------------ ---------------
 1  flat photo    (A/01)     one straight sweep per         near-optimal
                             iteration                      when it FITS
 2  tiles         (B/01)     stream tiles per iteration     works, but
                             when > RAM                     50GB x 30 iters
                                                            = 1.5 TB of I/O
 3  bouncer       (C/01)     scratch = |V| x 8B, priced     least dramatic;
                             trivially                      keeps the
                                                            receipt uniform
 4  GRAIN         (05)       compressed strata pay PER      2-4x less I/O,
                             ITERATION: fewer bytes x 30    compounds with
                             sweeps                         iteration count
 5  tiny scratch  (06-L1)    ranks in f32 not f64           matters at 1B+
                                                            verts (8->4 GB)
 6  O(V) mode     (06-L2)    THE NATURAL HOME: ranks in     1B verts = ~8 GB
                             RAM, edges streamed — the      resident, ANY
                             textbook semi-external algo    edge count
 7  less work     (06-L3)    THE STAR: delta convergence.   2-10x wall-clock
                             by iter 10, ~97% of vertices   (modeled); each
                             converged; touch only the 3%,  skipped tile is
                             SKIP converged cold tiles      skipped I/O too
 8  remember      (axes)     yesterday's ranks = warm       2-5 iterations
                             start on a 2% delta            instead of 30
```

Stack on one job:

```
  PageRank, 100 GB edges / 1B vertices, 16 GB box:

  their sizing   [########### ~110 GB session ###########]   rent it all
  1 flat         [########### needs edges in RAM ########]   OOM
  2 tiles        finishes, but 30 full re-reads               slow
  4 GRAIN        30 re-reads of 2-4x fewer bytes              better
  6 O(V) mode    [# ~8 GB ranks resident #] edges stream      FITS
  7 delta        late iterations touch ~3% of graph           2-10x faster
  8 warm start   nightly re-rank: 2-5 iters, not 30           ~10x on re-runs
```

### Shreyas Doshi's selling narrative

PageRank is not where OOM anxiety lives — it is where the RAM meter is
most obviously ABSURD.

```
  what you rent (Aura):                 what the algorithm needs:
  [########## 110 GB RAM ##########]    [# 4 GB rank state #]
           ^                                     ^
           mostly holds edges the                the only thing that
           algorithm only STREAMS PAST           must stay resident
```

Sell: **"stop renting RAM to hold data that only flows through it."**
PageRank is the demo algorithm — everyone knows it, everyone can verify
it, and the receipt ("state: 3.8 GB, edges: streamed, total resident:
4.1 GB") is self-explanatory on a slide.

### Estimated impact (modeled, not yet measured)

```
  RAM        : ~110 GB session -> ~4-8 GB resident (10-25x less).
  Speed      : 2-10x wall-clock via delta convergence + compressed
               strata; iteration I/O cut compounds 30x over.
  Re-runs    : nightly re-rank via warm start ~10x faster.
  Moat       : same certainty moat, sharpest expression — "you paid for
               110 GB so 100 GB of edges could flow past it once per
               iteration" lands hardest here.
```

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

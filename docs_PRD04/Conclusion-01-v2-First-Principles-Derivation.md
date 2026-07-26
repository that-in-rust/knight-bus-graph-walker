# Conclusion-01-v2 — First-Principles Derivation

Date: 2026-07-26
Relationship to v1: **v2 replaces v1's method, not just its numbers.**
`Conclusion-01-Spend-Disk-Buy-RAM.md` derived everything from *ratios against
GDS* ("3–9 min becomes 0.4 s"). This document derives everything from
**hardware constants and byte counts**, then lets the ratio fall out as a
*consequence*. Where the two disagree, v2 wins and says why.

Status: **derivation, with three of v1's headline claims materially reduced.**

> **Spoiler, stated up front so nobody reads a sales document by accident:**
> first-principles analysis **cuts v1's RAM claim by roughly an order of
> magnitude** (150–900× → 4–70×, depending on which baseline is honest),
> **cuts the gather-cost claim by ~10×**, and **shows only ONE of the six
> architectures changes complexity class.** The remaining claims are smaller
> and much harder to argue with.

---

## 1. Method

```text
  v1 METHOD (ratio-anchored -- the thing to stop doing)
     "GDS takes 3-9 min. Ours takes 0.4 s. Therefore ~1000x."
     -> inherits every error in the baseline
     -> cannot be checked without running GDS
     -> silently assumes GDS is near-optimal

  v2 METHOD (floor-anchored)
     1. list hardware constants (physics and spec sheets)
     2. count the bytes the algorithm MUST touch
     3. derive the FLOOR for ANY implementation
     4. locate GDS relative to that floor
     5. locate OUR design relative to that floor
     6. the ratio is now a DERIVED QUANTITY, with the baseline's
        uncertainty made explicit instead of inherited silently
```

---

## 2. Hardware constants

Commodity 2026 server class. These are the only inputs that are not derived.

| Constant | Value | Note |
|---|---:|---|
| DRAM sequential bandwidth (achievable) | **25–35 GB/s** | per socket, streaming |
| DRAM random-64B throughput (achievable) | **10–20 GB/s** | with sufficient memory-level parallelism |
| DRAM load-to-use latency | 80–100 ns | *only binding if MLP is 1 — see §3.3* |
| Memory-level parallelism per core | **10–16 outstanding misses** | out-of-order window |
| L3 size (server) | **16–64 MB** | shared |
| L3 latency | 15–20 ns | |
| Cache line | **64 B** | **the unit of DRAM traffic, not 4 B** |
| NVMe sequential read | **3–5 GB/s** | PCIe 4 ×4, achievable |
| Bolt / PackStream encode throughput | **50–200 MB/s** | CPU-bound, per-value type dispatch |

Workload: `V = 200e6` nodes, `E = 1e9` directed relationships, 20 iterations.

---

## 3. The floor: what work must PageRank do?

### 3.1 Bytes that must exist

```text
  reverse CSR peers   u32 x E          = 4 x 1e9   = 4.0 GB
  reverse CSR offsets u64 x (V+1)      = 8 x 2e8   = 1.6 GB
                                         topology  = 5.6 GB   (= 5.22 GiB)

  rank vectors        2 x f32[V]       = 8 x 2e8   = 1.6 GB   IRREDUCIBLE
  out-degree          u32[V]           = 4 x 2e8   = 0.8 GB   (fusable, §6.2)
```

**The information-theoretic floor on resident state is 1.6 GB** — you cannot
compute `r_{t+1}` from `r_t` without holding both. Any claim below 1.6 GB must
come from *reducing V*, not from cleverness.

### 3.2 Traffic per iteration, derived

```text
  topology read                          5.6 GB   sequential
  rank write + read                      2.4 GB   sequential
  THE GATHER: 1e9 accesses, each pulling a 64-BYTE LINE to use 4 bytes
                                                  (94% of fetched bytes wasted)
```

Gather traffic depends on cache reuse. Power-law in-degree means high-degree
sources are re-read constantly and stay resident:

```text
  let p = fraction of gathers hitting cache

  p = 0.0  ->  1e9 x 64 B = 64 GB    (pathological, no reuse)
  p = 0.5  ->  5e8 x 64 B = 32 GB
  p = 0.7  ->  3e8 x 64 B = 19 GB    (plausible on power-law)

  TOTAL per iteration = 8.0 GB + (19 to 64 GB) = 27 to 72 GB
```

### 3.3 **The correction that matters most: v1's gather estimate was wrong by ~10×**

```text
  v1 SAID:  1e9 random gathers x 80 ns latency = 80 SECONDS per iteration
            and therefore "the gather is 50-100x the topology scan"

  THAT IS A LATENCY-SERIALIZED CALCULATION. It assumes MLP = 1 -- that the
  CPU issues one miss, stalls, completes it, then issues the next.

  A PageRank gather loop has NO DEPENDENCY CHAIN between gathers. An
  out-of-order core sustains 10-16 concurrent misses. So the loop is
  BANDWIDTH-BOUND, not latency-bound:

    19-64 GB / (10-20 GB/s) = 1.0 to 6.4 SECONDS per iteration

  v1 overstated the gather by roughly 10x, and the claim
  "the gather is 50-100x the scan" should read "the gather is
  roughly 3-10x the scan."
```

This is a real error in v1 §2 (and in the ASCII doc's "librarian problem"). The
gather *is* still the dominant term — the conclusion survives — but the
magnitude was inflated tenfold, and the inflation flowed into every downstream
multiplier.

### 3.4 The floor for a resident, well-optimized implementation

```text
  27 to 72 GB per iteration / (10-30 GB/s effective) = 1.2 to 3.6 s
  x 20 iterations                                    = 24 to 72 SECONDS

  ==> NO implementation of 20-iteration PageRank on this graph beats
      ~25 seconds on this hardware. That is the floor.
```

---

## 4. Where does GDS actually sit?

### 4.1 Latency

```text
  Neo4j's own LDBC100 guide: 317M nodes, 2.15B rels, PageRank 8.28 min = 497 s
  Scale to our shape (1e9/2.15e9 of the edges):   ~230 s

  GDS observed  ~230 s
  Derived floor  24-72 s
  ==> GDS runs 3-10x above the hardware floor.
```

That is unremarkable for a JVM Pregel implementation — object headers, message
machinery, GC, and a layout not tuned for gather locality. **But it means the
headroom in PageRank is a property of GDS's implementation, not of the
problem.** A well-written native implementation should approach the floor
whether or not it is clever about storage.

### 4.2 RAM — and here v1 has a serious baseline problem

```text
  WHAT GDS NECESSARILY NEEDS (derived from its own source, via Arch06):
    CompressedAdjacencyList: delta+varlong, 1-2 B/edge best, 4-5 B/edge worst
      1e9 edges x 2-5 B                              = 2.0-5.0 GB
    + 12 B/node fixed (degrees 4 B + offsets 8 B)
      2e8 x 12 B                                     = 2.4 GB
    + PageRank Pregel state: 1 double + 2 atomic double message arrays
      per PRD05                                      = ~4.5 GB
                                                       ----------
    NECESSARY GDS FOOTPRINT                          = ~9-12 GB

  WHAT v1 COMPARED AGAINST: 88-144 GiB.
    That figure comes from PRD05's capacity table, which EXPLICITLY
    "includes an OLTP resident/cached component for baseline and Scenario A."

  ==> v1 compared OUR ANALYTICS-ONLY footprint against
      NEO4J'S WHOLE-MACHINE footprint. That is not apples to apples.
```

Three defensible baselines, and they differ by ~15×:

| Baseline | Value | What it is |
|---|---:|---|
| **GDS necessary** | **9–12 GB** | derived from GDS's own compression + Pregel source |
| GDS observed | ~51 GB | LDBC100's 110 GB scaled to our shape |
| Whole machine | 88–144 GiB | PRD05 capacity planning, includes OLTP + page cache |

**The honest comparison for an analytics workload is the first one.** The third
is legitimate only when the pitch is "retire the whole box," and must be
labelled as such.

---

## 5. Where does our design sit? Derived, not assumed.

### 5.1 Streaming instead of resident — derive the cost

```text
  Resident:  topology from DRAM   5.6 GB / 30 GB/s = 0.19 s per iteration
  Streamed:  topology from NVMe   5.6 GB /  4 GB/s = 1.40 s per iteration
                                                     ------
  Streaming costs                                   +1.2 s/iter = +24 s total

  Our streamed total = floor(24-72 s) + 24 s = 48-96 s
```

Now the comparison, against both honest baselines:

```text
  vs GDS OBSERVED (~230 s):   48-96 s  ->  WE ARE 2.4-4.8x FASTER
  vs the FLOOR    (24-72 s):  48-96 s  ->  we are 1.3-2.0x slower

  Both are true. The first is the product claim; the second is the
  engineering truth. PRD05's "strict-RAM PageRank is 1.5-5x slower"
  was measuring against the FLOOR (tuned resident), and my derivation
  agrees with it at 1.3-2.0x.
```

**This is a much better story than v1 told, arrived at more honestly:** we can
plausibly beat GDS *while streaming from disk*, not because streaming is fast,
but because GDS leaves 3–10× on the table.

### 5.2 RAM, derived

```text
  Resident state = 2 x f32[V_effective] + fused reciprocal-degree

  no reduction:              V_eff = 200M  ->  1.6 GB   (the floor, §3.1)
  peel 50% of V:             V_eff = 100M  ->  0.8 GB
  peel + quotient 3x:        V_eff =  33M  ->  0.27 GB
  peel + quotient 10x:       V_eff =  10M  ->  0.08 GB

  vs GDS NECESSARY (9-12 GB):
    no reduction   ->  6-8x
    peel           ->  11-15x
    + quotient 3x  ->  33-44x
    + quotient 10x ->  110-150x

  vs WHOLE MACHINE (88-144 GiB = 94-155 GB):
    no reduction   ->  59-97x
    + quotient 10x ->  1175-1940x
```

> **v1 claimed 150–900×. The derivation gives 6–150× against the honest
> analytics baseline.** The larger numbers only appear when comparing against a
> whole-machine figure that includes the OLTP store — which is a legitimate
> *procurement* claim ("retire the 128 GB box") but not a legitimate
> *engineering* one.

### 5.3 Precomputation — the only mechanism that changes complexity class

This is the finding that survives first principles completely intact, and it is
the only one.

```text
  LIVE:         O(E x iterations) of memory traffic  =  20 x 1e9 edge touches
  PRECOMPUTED:  O(V) of sequential read             =  0.8 GB read once

  This is not a constant-factor improvement. It is an EXPONENT change.

  Derived query time:
    T_open                          = 190 ms   MEASURED (v002)
    read 0.8 GB @ 4 GB/s            = 200 ms
                                      -------
    T_repeat                        = 390 ms

  vs GDS observed 230 s   ->  590x
  vs derived floor  24 s  ->   62x
  vs OUR OWN live path 48-96 s -> 123-246x
```

**Every other architecture in the portfolio fights for a factor of 2–10 against
hardware limits. Only ARCH-I changes the exponent.** That reorders the roadmap:
precomputation is not one idea among six, it is the only one whose payoff is not
bounded by memory bandwidth.

### 5.4 Serialization, derived — and it is worse than v1 said

```text
  PackStream row: (int64 nodeId, float64 score) with type markers
                  ~ 2 + 9 + 9 = ~20 B/row

  200e6 rows x 20 B = 4.0 GB on the wire
  at 50-200 MB/s encode throughput (CPU-bound, per-value dispatch)
                    = 20 to 80 SECONDS

  v1 said 6-32 s. Derivation says 20-80 s. v1 was optimistic ~2.5x.
```

Consequence for a stream-all query:

```text
  GDS   230 s compute + 20-80 s serialize = 250-310 s
  OURS  0.39 s        + 20-80 s serialize = 20.4-80.4 s
                        ^^^^^^^^^^^^^^^^^ SHARED FLOOR

  end-to-end ratio = 250/80.4  to  310/20.4  =  3.1x to 15.2x
```

**v1 said ~6–85×. Derivation says ~3–15×.** The serialization floor is bigger
and the compute baseline is smaller, and both errors pushed the same way.

---

## 6. What first principles refutes, confirms, and reframes

### 6.1 Refuted

| v1 claim | Derived reality | Error |
|---|---|---|
| "gather is 50–100× the scan" | ~3–10× | **~10× overstated** — latency-serialized instead of bandwidth-bound |
| "RAM 150–900× less" | 6–150× vs analytics baseline | **~10× overstated** — compared against a whole-machine figure |
| "end-to-end ~6–85× on stream-all" | ~3–15× | ~3× overstated; serialization is 20–80 s not 6–32 s |
| "cache locality buys 2–4×" | ~1.5–2× | traffic drops from 72→27 GB at best, i.e. 2.7× on the gather term alone |
| ARCH-II precision ladder is a big win | ~0 once degree-ordered | already dropped for other reasons; derivation confirms |

### 6.2 Confirmed, and now on firmer ground

```text
  CONFIRMED  The gather IS the dominant term (3-10x the scan). Attacking it
             is correct; v1 just mis-sized it.

  CONFIRMED  Precomputation is the only exponent-changing move. 62-590x
             depending on baseline, and it survives every correction.

  CONFIRMED  1.6 GB is a hard floor on resident state at V=200M.
             Anything below requires REDUCING V -- so peel and quotient are
             not optimizations, they are the ONLY route below the floor.

  CONFIRMED  Streaming costs +1.2 s/iteration (24 s over 20 iterations).
             Cheap. PRD05's 1.5-5x penalty is real but at the low end (1.3-2.0x)
             when measured against the floor rather than against GDS.

  NEW        We may beat GDS WHILE STREAMING (2.4-4.8x faster than observed),
             because GDS runs 3-10x above the hardware floor. That headroom
             is a property of their implementation, not of the problem.
```

### 6.3 Reframed: peeling helps RAM, not latency

```text
  Peeled vertices have indeg <= 1, so they collectively hold <= V_peeled edges.

  Peel 60% of V = 120M vertices, each with <= 1 in-edge
    -> removes <= 120M of 1e9 edges = <= 12% of topology

  SO:  state traffic  drops ~60%   (big RAM win)
       topology bytes drop ~12%    (small latency win)

  v1 implied peeling cut both proportionally. It does not.
```

### 6.4 Fused reciprocal degree — a derived optimization v1 missed

The out-degree lookup `outdeg(u)` is a **second random access per edge**, into a
separate 0.8 GB array. Store `1/outdeg(src)` inline with the edge instead:

```text
  cost:    +4 B/edge on disk = +4 GB   (space is cheap)
  saving:  eliminates 1e9 random accesses per iteration
           = up to 64 GB of line traffic, HALVING §3.2's gather term
  bonus:   replaces a division with a multiply in the inner loop
```

**This is plausibly the single highest-value micro-optimization available**, it
follows directly from counting random streams, and it is not in v1 or in any
of the six architectures.

---

## 7. The corrected table

All figures derived in §§3–5. Baseline stated per row, because the choice of
baseline moves the answer by ~15×.

| Metric | GDS necessary | GDS observed | Whole machine | **Our live path** | **Our precomputed** |
|---|---:|---:|---:|---:|---:|
| **RAM** | 9–12 GB | ~51 GB | 94–155 GB | **0.08–1.6 GB** | **~0.1 GB** |
| RAM ratio *vs necessary* | 1× | — | — | **6–150×** | ~100× |
| RAM ratio *vs whole machine* | — | — | 1× | 59–1940× | ~1000× |
| **Compute latency** | 24–72 s *(floor)* | ~230 s | — | **48–96 s** | **0.39 s** |
| Latency *vs floor* | 1× | 3–10× | — | **1.3–2.0× slower** | **62× faster** |
| Latency *vs observed GDS* | — | 1× | — | **2.4–4.8× faster** | **590× faster** |
| **End-to-end, stream-all** | — | 250–310 s | — | 68–176 s | **20.4–80.4 s** → **3–15×** |
| **End-to-end, top-K** | — | 250–310 s | — | 48–96 s | **0.19 s** → **1300–1600×** |
| **Disk** | ~50 GB | ~50 GB | ~50 GB | **+26–46 GB** | **+26–46 GB** |
| **Exact?** | exact | exact | exact | **exact** | **exact** |

### The three claims I would now defend in front of a hostile reviewer

```text
  1. RAM: 6-150x less than GDS's NECESSARY analytics footprint.
          Derived from GDS's own compression source and the 1.6 GB
          information-theoretic floor. The spread is set entirely by the
          quotient ratio, which is unmeasured.

  2. LATENCY, small-result queries: 1300-1600x.
          Derived: precomputation changes O(E x iters) to O(V). The only
          exponent change in the portfolio. Bounded below by T_open, which
          is MEASURED at 190 ms and is currently 2.1x worse than Neo4j's.

  3. LATENCY, live path: plausibly 2.4-4.8x FASTER than GDS observed,
          while streaming from disk -- not because streaming is fast, but
          because GDS runs 3-10x above the hardware floor.

  AND THE ONE I WOULD RETRACT: "1000x faster" as an unqualified claim.
          For stream-all it is 3-15x. The 1000x applies only to
          small-result queries, and only against observed GDS.
```

---

## 8. What must be measured, re-ranked by what the derivation exposed

```text
  M1  GATHER CACHE-HIT RATE p.                                    1 week
      Sets the per-iteration traffic between 27 and 72 GB -- a 2.7x swing
      on the dominant term. Instrument with perf counters.
      Derivation cannot settle this; only the graph can.

  M2  QUOTIENT RATIO on a real identity graph.                    1 week
      Sets V_eff, therefore RAM, between 1.6 GB and 0.08 GB.
      It is the ONLY route below the 1.6 GB information-theoretic floor.

  M3  T_OPEN, and fix it.                                         2 days
      MEASURED at 189.979 ms vs Neo4j's 90.446 ms. It is 49% of the
      precomputed query and 100% of the top-K query. Highest ratio of
      effort to payoff in the portfolio.

  M4  BOLT ENCODE THROUGHPUT.                                     2 days
      Sets the serialization floor between 20 s and 80 s -- a 4x swing on
      every stream-all number. Both engines pay it, so it caps the
      achievable ratio regardless of how good the engine is.

  M5  FUSED RECIPROCAL DEGREE (§6.4).                             3 days
      Derivation says it halves the gather term. Cheapest large win
      identified, and it was invisible to v1.

  M6  PEEL FRACTION.                                              2 days
      Now known to help RAM (~60%) far more than latency (~12%).
      Re-scoped accordingly.
```

---

## 9. What this document does not fix

```text
  The BASELINE is uncertain by ~15x (9-12 GB necessary vs 94-155 GB whole
  machine) and every ratio inherits that. The only cure is running GDS
  ourselves on our own graph with cgroup-measured memory -- which is
  PRD05's Experiment 2, still unrun.

  Hardware constants are commodity-class assumptions. A different memory
  subsystem moves the floor by 2-3x in either direction.

  Nothing here is measured on our engine. Our engine has never run PageRank.
  The derivation says what is POSSIBLE, not what our code does.

  The derivation assumes 20 iterations to tolerance. Real convergence is
  data-dependent; delta convergence could cut effective iterations to
  5-8, which would improve the live path by ~2x and leave the precomputed
  path unchanged.
```

---

## 10. The one-paragraph verdict

Deriving from hardware rather than from ratios **shrinks the headline and
hardens what remains.** The gather was overstated tenfold, the RAM claim was
inflated by comparing an analytics footprint against a whole-machine figure, and
the serialization floor is larger than v1 assumed — so "1000× faster" collapses
to **3–15× for stream-all queries** and survives only for small-result queries.
What emerges stronger is different and better: **precomputation is the only move
in the entire portfolio that changes complexity class rather than fighting for a
constant factor against memory bandwidth**, the 1.6 GB information-theoretic
floor means **peel and quotient are the only route below it** rather than mere
optimizations, and — most usefully — **GDS runs 3–10× above the hardware floor,
so a competent streaming implementation can plausibly beat it while using a
fraction of the RAM.** That last sentence is a claim about arithmetic and
engineering headroom rather than about cleverness, which is why I would put it
in front of a skeptic and v1's thousand-fold claim I would not.

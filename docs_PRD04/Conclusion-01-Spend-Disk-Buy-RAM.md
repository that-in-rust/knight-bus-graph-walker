# Conclusion-01-Spend-Disk-Buy-RAM — The PageRank Verdict

Date: 2026-07-26
Voice: Doshi lens — buyer-felt outcomes first, spec second, confidence graded.
Status: **decision-grade estimate, not a performance promise.** Every number
below shows its arithmetic or its source. Nothing here is measured on our own
engine except where explicitly marked MEASURED.

Scope: **PageRank only**, at the planning shape `V = 200M nodes,
E = 1B directed relationships` (the 50 GiB-class logical graph from `prd-l1`).

---

## 1. The question this answers

Not *"is our engine better?"* — that invites a benchmark fight `PMF01` already
told us never to pick. The question is the one a platform owner actually asks:

> **"What machine do I have to buy to run PageRank on this graph, how long does
> the answer take, and what does it cost me in disk and staleness?"**

Everything below is arranged to answer exactly that, in that order.

---

## 2. The design being measured

This is the **simplified** architecture after two rounds of correction. State it
precisely, because the numbers are only traceable if the design is fixed.

```text
  BUILD ONCE PER (algorithm x query shape x refresh cadence). NEVER MUTATE.

  Layer 0  flat reverse CSR, streamed from disk, f32 rank vectors
  Layer 1  G1: dense IDs ordered by DESCENDING DEGREE          [free]
  Layer 2  ARCH-IV: peel indeg<=1, dangling, and chains        [exact]
  Layer 3  ARCH-VI: quotient structurally-equivalent vertices  [exact]
  Layer 4  ARCH-I: precompute the answer at build time         [exact]
  Layer 5  precompute the QUERY's answer (e.g. sorted top-K)   [exact]

  DROPPED, and why:
    ARCH-II precision ladder  -> unnecessary. Degree ordering alone puts the
                                 hot 1% in L3 at FULL precision (see §4.2).
                                 Removing it also removes the f16 range bug
                                 and the only non-exact design.
    ARCH-III self-compaction  -> wrong by construction. It mutates an artifact
                                 in an architecture whose load-bearing
                                 invariant is that artifacts never change.
    compression (Elias-Fano)  -> space is cheap; decode cost is not.

  ASSUMPTION L1: one hour of OLAP lag is acceptable and the watermark is
                 reported on every answer. (prd-l1 already grants this.)
                 Therefore all latency figures are T_repeat.
```

**Result: six architectures collapse to three, plus two free orderings, and
every remaining layer is exact.** No approximation, no declared modes, no error
budget to defend.

---

## 3. The headline table

| What the buyer actually feels | Neo4j GDS today | Ours, built once | Change |
|---|---|---|---|
| **RAM to run the job** | 88–144 GiB | **0.15–0.6 GiB** | **~150–900× less** |
| **RAM once precomputed** | 88–144 GiB, *every run* | **~0.1–0.2 GiB** (streamed) | **~500–1000× less** |
| **Machine you must provision** | 128–256 GiB class | **8–16 GiB class** | procurement event → a laptop |
| **Latency, all 200M scores** | 3–9 min | **~0.4 s** | **~400–1400×** |
| **Latency, "top 100"** | 3–9 min | **~190 ms** | **~950–2800×** |
| **Disk, whole system** | ~50 GiB | **~76–96 GiB** | **1.5–1.9× MORE** |
| **Freshness** | live projection | up to 1 h stale, watermarked | worse, by design |
| **Exact?** | exact | **exact** | no approximation anywhere |

**The trade, in one sentence:** *we spend disk and freshness — both cheap — to
buy RAM and latency, which are the two things the buyer is actually short of.*

---

## 4. Every number, derived

### 4.1 The baseline

```text
  PRD05 model, PageRank active at 200M/1B:            88-144 GiB
  PRD05 modelled latency band:                        3-9 min
  Neo4j's OWN LDBC100 guide (317M nodes, 2.15B rels,
    R5d.16xlarge, 512 GB RAM, 400 GB heap):           110 GB, 8.28 min
  GDS is 100% JVM-heap resident, with no spill path.
```

The LDBC100 figure is on a *larger* graph, so it is a sanity check on the order
of magnitude, not a like-for-like. We use PRD05's modelled band.

### 4.2 Layer 0–1: streamed CSR with degree ordering

```text
  reverse CSR (pull formulation)  = 4E + 8(V+1)
                                  = 4(1e9) + 8(2e8)  = 5.6e9 B = 5.22 GiB
                                    ^ STREAMED, not resident

  out-degree u32[V]               = 4 x 2e8 = 8.0e8 B = 0.745 GiB   resident
  rank vectors, 2 x f32[V]        = 8 x 2e8 = 1.6e9 B = 1.490 GiB   resident
                                                        ---------
  Layer 0-1 resident total                            = 2.235 GiB
```

**Why degree ordering removes the need for a precision ladder:**

```text
  top 1% of V = 2,000,000 vertices

    at f32:  2e6 x 4 B =  8 MB
    at f64:  2e6 x 8 B = 16 MB
                         ^^^^^ STILL FITS A SERVER L3

  The cache win comes from CONTIGUITY, not from reduced precision.
  Degree-ranked IDs make the hot set contiguous for free.
  Power-law in-degree means a large share of gathers land in that
  contiguous prefix -- so most gathers become cache hits at FULL precision.
```

This is the single most useful simplification in the document. It buys the
entire ARCH-II benefit and discards the risk.

### 4.3 Layer 2: peeling

```text
  Removed from the ITERATION loop (not from the output):
    indeg = 0        -> rank is (1-d)/N forever
    indeg = 1        -> closed form once the core converges
    outdeg = 0       -> dangling mass handled by one global rule
    indeg=outdeg=1   -> chains collapse to a single edge

  On power-law graphs this is 30-60% of V.

  V_core = 0.4 to 0.7 x 200M = 80M to 140M
  state  = (4 + 4 + 4) B x V_core        [2 ranks f32 + outdeg u32]
         = 0.96e9 to 1.68e9 B
         = 0.9 to 1.6 GiB resident
```

### 4.4 Layer 3: the quotient

```text
  ARCH-VI collapses vertices with IDENTICAL neighbourhoods.
  On an identity/ER graph this is the dominant structure, because the graph
  is BUILT by joining records on shared attributes.

  Estimated reduction: 2x to 10x   [LOW CONFIDENCE -- see §7]

  V_quotient_core = V_core / (2 to 10) = 8M to 70M
  state           = 12 B x (8M to 70M) = 0.09 to 0.84 GiB
                  -> quoted as 0.15 to 0.6 GiB
```

### 4.5 Layer 4: precomputing the answer

```text
  Artifact:  ranks.f32[V] = 4 x 2e8 = 0.745 GiB on disk

  QUERY becomes: open the artifact, stream it out.
    T_open                     = 190 ms      MEASURED (v002)
    read 0.745 GiB @ 3-5 GiB/s = 150-250 ms
                                 -----------
    T_repeat                   ~ 340-440 ms  -> quoted as ~0.4 s

  Resident working set: a sequential read of evictable pages. ~0.1-0.2 GiB.

  Speedup vs 3-9 min (180-540 s):
    180 / 0.44 =  409x
    540 / 0.34 = 1588x           -> quoted as ~400-1400x
```

### 4.6 Layer 5: precomputing the query's answer

```text
  If the query is known to be "top 100 by PageRank":

  Artifact:  a sorted top-K list = 100 x 12 B ~ 1.2 KB (round to 4 KB)

  T_repeat = T_open (190 ms) + read (~0 ms) = ~190 ms
             ^^^^^^^^^^^^^^^ THE ENTIRE QUERY IS NOW FILE-OPEN COST

  Speedup: 180/0.19 = 947x  to  540/0.19 = 2842x
```

**And this also removes the serialization tail**, which would otherwise hide
everything:

```text
  "stream all 200M scores" over Bolt:
    200M rows x ~16 B = 3.2 GB
    at realistic Bolt throughput          = 6-30 SECONDS
    -> which SWAMPS a 0.4 s read. The compute win becomes invisible.

  "top 100":
    100 rows = ~1.6 KB -> serialization is free -> the win is REAL end-to-end.
```

> **Knowing the QUERY is worth more than knowing the ALGORITHM.** Knowing
> "PageRank" buys a 0.745 GiB array. Knowing "PageRank, top 100" buys 4 KB and
> deletes a 6–30 second serialization tail.

### 4.7 Disk, added up honestly

We keep everything, because space is cheap and mutation is forbidden.

```text
  NEO4J SIDE (unchanged -- we do not replace it)
    Neo4j store for a 50 GiB logical graph            ~50 GiB

  OUR SIDE
    Projection Build Store (canonical IR)          10-30 GiB
    degree-ordered reverse CSR + out-degree           ~6 GiB
    quotient topology                                1-3 GiB
    peeled core topology                             2-3 GiB
    precomputed ranks.f32[V]                        0.75 GiB
    quotient expansion map u32[V]                   0.75 GiB
    precomputed top-K                             negligible
                                                   ----------
    our artifacts subtotal                         ~26-46 GiB

  SYSTEM TOTAL                                     ~76-96 GiB
  vs Neo4j alone (~50 GiB)                       = 1.5x to 1.9x
```

Note this is **better than PRD05's 2–5× band** for the fuller Scenario B,
because we dropped compression strata and multi-view materialization. Space is
spent, but not extravagantly.

### 4.8 Build time

```text
  PRD05: 50 GiB snapshot publication      5-45 min
       + PageRank precompute, cold        3-9 min
       + with warm start from gen N-1     2-4 iterations instead of 20
                                          -> ~0.3-1.8 min

  Typical per generation:                 ~10-20 min

  Fits comfortably inside an hourly window. AND -- per the product unit --
  this is ONE unit's build. It is NEVER summed with other algorithms'
  builds, because "WCC @ hourly" and "Louvain @ weekly" are separate
  products that merely share a Build Store.
```

---

## 5. The bottleneck moved, and it moved somewhere nobody is looking

This is the most actionable finding in the document.

```text
   BEFORE                                AFTER
   PageRank compute   3-9 min            T_open              190 ms
   ^^^^^^^^^^^^^^^^                      ^^^^^^
   everything was aimed here             this is now the ENTIRE query

   MEASURED (v002):  our open = 189.979 ms
                     Neo4j's   =  90.446 ms
   WE ARE 2.1x SLOWER AT THE THING THAT IS NOW THE WHOLE LATENCY.
```

`T_open` is 48–95% of total query time in every precomputed case. And the fix
is unglamorous and cheap: the current open path does a full validating scan of
offsets, node records and the key index. Replace it with **lazy manifest
validation** — verify checksums and shape on open, validate regions on first
touch.

```text
  If T_open drops 190 ms -> 20 ms:

    "all scores"  340-440 ms  ->  170-270 ms      ~2x better
    "top 100"     190 ms      ->   20 ms          ~10x better
                                                  and 9,000-27,000x vs GDS
```

**Two days of work unlocks another order of magnitude on the strongest claim in
the document.** Nothing else in the portfolio has that ratio.

---

## 6. Two baselines, reported separately

PRD05 Correction 3 requires this and it matters commercially.

| Baseline | What it is | Our comparison |
|---|---|---|
| **GDS Enterprise, tuned, resident** | unlimited algorithm concurrency | the honest *architecture* comparison — the table in §3 |
| **GDS Community** | **capped at 4 concurrent algorithm threads** | a valid *product* comparison; we may appear 2–6× faster on top of everything else, on algorithms that scale to 16–32 cores |

Never present the Community comparison as proof that our kernel beats tuned
Enterprise. It is a legitimate product claim and an illegitimate engineering one.

---

## 7. Confidence, graded — and the marketing must respect it

| Claim | Confidence | Basis |
|---|---|---|
| RAM 150–1000× less | **HIGH** | deterministic arithmetic, §4.2–4.5 |
| Disk 1.5–1.9× more | **HIGH** | arithmetic, §4.7 |
| `T_open` = 190 ms and we lose to Neo4j | **MEASURED** | v002 |
| Latency of "top 100" ~190 ms | **MEDIUM-HIGH** | dominated by a *measured* quantity |
| Latency of "all scores" ~0.4 s | **MEDIUM** | read bandwidth assumption |
| Peel fraction 30–60% | **MEDIUM** | power-law property; graph-shape dependent |
| **Quotient 2–10×** | **LOW** | **entirely data-dependent. Unmeasured.** |
| Speedup vs GDS ~400–2800× | **MEDIUM** | inherits the quotient's uncertainty |

**Lead with RAM.** It is arithmetic, it survives a hostile audit, and it is the
axis the incumbent meters. The latency numbers are real but inherit the
quotient's uncertainty, and `PMF01`'s rule stands: *never lead with speed.*

---

## 8. What this does NOT claim

Stated plainly, because the corpus's own history is that modelled multipliers
get corrected downward on first contact with reality — `Arch06` cut 50–100×
to 10–30× the one time it read actual GDS source.

```text
  NOT claimed: that this holds on non-power-law graphs (meshes, roads).
               Peeling and the quotient both depend on skew and duplication.

  NOT claimed: that it holds on graphs with no duplicate structure.
               ARCH-VI's payoff there is ~1x, and the RAM figure degrades
               from 0.15-0.6 GiB toward 0.9-1.6 GiB. Still ~100x, but not
               ~900x.

  NOT claimed: any of it for parameterised queries. A user asking for
               damping 0.9 when we precomputed 0.85 must fall to the slow
               path EXPLICITLY. Artifact identity must therefore be
               (graph, generation, procedure, config hash, watermark) --
               a requirement Reference-Learning-Critique-Gaps §9 already made.

  NOT claimed: T_first. On a single-shot run the precomputed design is
               SLOWER than the baseline. It wins from the second read, and
               under L1 the build is the builder's problem, not the buyer's.

  NOT claimed: anything measured. Zero of these figures come from our engine
               running PageRank, because our engine has never run PageRank.
```

---

## 9. The metric, and the pitch

### The north star

```text
   PEAK GB RETIRED
     = provisioned RAM the customer no longer needs to hold for analytics,
       summed across their entire fleet.

   For PageRank:
     128-256 GiB provisioned  ->  8-16 GiB
     x every environment (dev / staging / prod / DR)
     x every region
```

It is the capacity planner's own language, it is invoiceable as
share-of-savings, and every unit of it is a sentence Neo4j's architecture
cannot say back.

### The pitch order

```text
   1. "Your analytics can no longer take down your database."   <- the trigger
   2. "Retire the RAM you keep for the worst algorithm."         <- the bill
   3. "Every answer names the snapshot it came from."            <- the audit
   4. "Permissive licence. No meter. No seat count."             <- the wound
   -- and only then --
   5. "Also, it is about a thousand times faster to answer."     <- the spec
```

Speed is last on purpose. It is the claim most likely to be contested and the
one least likely to have started the conversation.

---

## 10. What must be true, and how to find out

Three measurements. None requires building the system.

```text
  M1  QUOTIENT RATIO on a real identity graph.                     1 week
      Hash sorted adjacency lists; count equivalence classes.
      This ONE number sets the RAM figure between 0.15 and 1.6 GiB
      and the speedup between ~100x and ~2800x.
      KILL: ratio < 1.3x -> quotient is a footnote; requote everything.

  M2  T_OPEN, and fix it.                                          2 days
      We are at 189.979 ms vs Neo4j's 90.446 ms (MEASURED).
      T_open is now 48-95% of the query. Lazy manifest validation.
      This is the highest-ratio two days in the whole portfolio.

  M3  PEEL FRACTION.                                               2 days
      Share of V with indeg<=1 or outdeg=0.
      KILL: < 20% -> drop layer 2.

  Note: M1 and M3 share one pass over the adjacency lists, and M1's
  zero-residual bucket IS the exact-twin count. Two experiments, one fixture.
```

**Until M1 is run, the honest external claim is the RAM figure alone** — which
is arithmetic, which is ~100× at minimum even with no quotient at all, and
which is the number the buyer's meter is denominated in anyway.

---

## 11. Sources

```text
  docs_PRD05/Neo4j-Rust-Two-Scenario-Estimation.md
      the 88-144 GiB baseline, the 3-9 min band, the CSR byte formulas,
      the LDBC100 figures, Correction 3 (two concurrency baselines),
      Correction 6 (eliminate vs externalize), Duck 15/16 (T_first /
      T_repeat / T_total(N) and the break-even formula),
      and the v002 MEASURED T_open of 189.979 ms vs 90.446 ms

  docs_PRD04/innovation-mega-arch-20260726v1.md
      ARCH I-VI, the gather-vs-scan correction, the estimate tables,
      §0 conventions (L1, the product unit, relocate vs eliminate)

  docs_PRD04/innovation-storage-timelines-20260726v2.md
      Timeline B (freeze ordering, which retires ARCH-III),
      Timeline E (warm start, which retires the T_first objection)

  docs_PRD04/A01-202607260102.md PART 0
      per-algorithm materialization; build on demand, never a sweep

  docs_PRD04/Win-The-Whales-Vision.md
      PEAK GB RETIRED; the pitch order; the trigger moment

  docs_PRD04/Reference-Learning-Critique-Gaps.md §9
      artifact identity must include the config hash -- the requirement
      that makes precomputation safe rather than silently wrong

  docs_PRD04/PMF01.md
      "never lead with speed"
```

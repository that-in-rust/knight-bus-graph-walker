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

## 2.5 RUBBER-DUCK PASS — which PageRank, exactly?

*Added after a duck-debugging pass. The original table said "PageRank", and that
vagueness was doing real work in my favour. "PageRank" is at least eight
different queries with different answers.*

### The query I was actually claiming (and never said so)

```text
  CALL gds.pageRank.stream('g')

  IMPLIED, NEVER STATED:
    mode                    = stream          (all rows out)
    dampingFactor           = 0.85            (the default)
    maxIterations           = 20              (the default)
    tolerance               = 1e-7            (the default)
    relationshipWeightProperty = NONE         (unweighted)
    sourceNodes             = NONE            (GLOBAL, not personalized)
    nodeLabels / relationshipTypes = ALL      (unfiltered)
```

**Change any one of those and the numbers move, sometimes catastrophically.**

### The eight PageRank queries, and whether the design serves them

| Query | Precomputable? | Verdict |
| --- | --- | --- |
| `pageRank.stream`, global, default params | **YES** | the claim as written |
| `pageRank.stats` (aggregates only, no rows) | **YES** | **best case — no serialization floor** |
| `pageRank.stream` + `ORDER BY … LIMIT k` | **YES**, as a sorted top-K artifact | **the headline case** |
| `pageRank.write` / `.mutate` | Partly | we precompute; the write-back cost is common to both engines |
| **weighted** (`relationshipWeightProperty`) | YES *if* weights are stable per generation | one artifact per weight property |
| **`dampingFactor` varied by the user** | **NO** | config-hash miss → falls to the live path |
| **personalized (`sourceNodes`)** | **NO — arbitrary seeds cannot be precomputed** | **the real hole. See §8.1** |
| **filtered** (arbitrary label/reltype subsets) | **NO** in general | combinatorial artifact explosion |

### The correction the duck actually forced: serialization is a SHARED floor

My "~400–1400×" was **compute-only**. For a query that returns all 200M rows,
*both* engines must serialize them, and that cost is common:

```text
  200M rows x ~16 B = 3.2 GB over Bolt
  at realistic Bolt throughput (100-500 MB/s) = 6.4 to 32 SECONDS

  GDS   : 180-540 s compute  +  6.4-32 s serialize  =  186-572 s
  OURS  :   0.34-0.44 s      +  6.4-32 s serialize  =  6.7-32.4 s
                                ^^^^^^^^^^^^^^^^^^ the SAME floor for both

  END-TO-END RATIO = 186/32.4  to  572/6.7  =  ~6x to ~85x
                     NOT 400-1400x.
```

**So the honest headline depends entirely on result size:**

```text
  "give me all 200M scores"    ->  ~6-85x    (serialization dominates BOTH)
  "give me the top 100"        ->  ~980-3000x (no floor on our side; GDS still
                                              computes and materializes all)
  "give me the stats"          ->  ~980-3000x (same reason)
```

The ~1000× claim is **real only for small-result queries.** That is a much
narrower claim than the original table implied, and it is the single most
important thing this pass corrected.

---

## 3. The headline table — worst, average, best

Three named scenarios. Each fixes the graph shape *and* the query, so the
numbers are falsifiable rather than aspirational.

```text
  WORST   graph: no duplicate structure, low skew (mesh / road / uniform random)
          query: personalized PageRank, arbitrary sourceNodes, stream all rows
          -> NO precompute, NO quotient, NO peel. Layer 0 only.

  AVERAGE graph: power-law social/transaction, moderate duplication (quotient
                 2-3x), peel 30-40%
          query: global default-param stream of all rows
          -> precompute works; serialization floor bites

  BEST    graph: identity/ER, heavy exact duplication (quotient 8-10x),
                 high skew (peel 55-60%)
          query: precomputed sorted top-K (or stats), T_open fixed to ~20 ms
          -> everything lands
```

| Metric | Neo4j GDS | **WORST** | **AVERAGE** | **BEST** |
|---|---|---|---|---|
| **RAM to run** | 88–144 GiB | **2.3 GiB** | 0.6–1.2 GiB | **0.15–0.4 GiB** |
| **RAM at query** (precomputed) | 88–144 GiB *every run* | n/a — no precompute | 0.1–0.2 GiB | **~0.1 GiB** |
| **RAM improvement** | 1× | **~38–63×** | ~100–240× | **~600–1400×** |
| **Machine class** | 128–256 GiB | 8–16 GiB | 8 GiB | **8 GiB / laptop** |
| **Compute latency** | 180–540 s | **1.5–5× SLOWER** | ~0.4 s | **~20 ms** |
| **End-to-end latency** | 186–572 s | **270–2700 s** | 6.7–32 s | **~20 ms** |
| **End-to-end improvement** | 1× | **0.2–0.7× (WORSE)** | **~6–85×** | **~9,300–28,600×** |
| **Disk, whole system** | ~50 GiB | 1.5–1.9× more | 1.5–1.9× more | **~1.5× more** |
| **Freshness** | live | up to 1 h stale | up to 1 h stale | up to 1 h stale |
| **Exact?** | exact | **exact** | **exact** | **exact** |

### Reading the table

**The worst case is not a disaster — it is a different product.** With no
precompute, no quotient and no peel, we still get **~40–60× on RAM** purely from
streaming topology instead of holding it resident. And we are **1.5–5× slower**,
exactly as PRD05 Duck 14 predicted. That is the *capacity* product: "it finishes
on hardware where GDS cannot start." It is not the *speed* product.

**The average case is the honest expected value**, and its end-to-end number
(~6–85×) is far below the compute number (~400–1400×) because Bolt serialization
is a floor both engines pay.

**The best case requires three things to all be true** — an ER-shaped graph, a
small-result query, and `T_open` fixed. Two of the three are within our control.

**The trade, in one sentence:** *we spend disk and freshness — both cheap — to
buy RAM always, and latency only when the result set is small.*

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
| **RAM ~40–60× less, worst case** | **HIGHEST** | pure arithmetic; needs only streamed topology. Requires *no* quotient, *no* peel, *no* precompute |
| RAM ~100–240×, average | **HIGH** | arithmetic + a moderate quotient |
| RAM ~600–1400×, best | **MEDIUM** | needs quotient 8–10×, which is unmeasured |
| Disk 1.5–1.9× more | **HIGH** | arithmetic, §4.7 |
| `T_open` = 190 ms, we lose 2.1× to Neo4j | **MEASURED** | v002 |
| Serialization floor 6–32 s for 200M rows | **MEDIUM-HIGH** | Bolt throughput assumption; applies to *both* engines |
| End-to-end ~6–85× on stream-all | **MEDIUM** | dominated by the shared floor above |
| End-to-end ~980–3000× on top-K | **MEDIUM-HIGH** | dominated by a *measured* `T_open` |
| Peel fraction 30–60% | **MEDIUM** | power-law property; dead on meshes |
| **Quotient 2–10×** | **LOW** | **entirely data-dependent. Unmeasured. Sets the whole spread.** |
| Worst case is 1.5–5× SLOWER | **MEDIUM-HIGH** | PRD05 Duck 14, and consistent with its own model |

> **The single most robust claim in this document is the WORST case:
> ~40–60× less RAM.** It needs nothing except streaming topology instead of
> holding it resident — no quotient, no peel, no precompute, no graph-shape
> assumption. Everything above 60× is earned by assumptions that may not hold.

**Lead with RAM, and lead with the worst-case RAM number.** It is arithmetic, it
survives a hostile audit, it holds on *any* graph, and it is the axis the
incumbent meters. `PMF01`'s rule stands: *never lead with speed* — and after this
pass there is a second reason, which is that the speed number swings from
**0.2× (worse) to 28,600× (better)** depending on the query. A claim with a
five-order-of-magnitude range is not a claim; it is a conversation.

---

## 7.5 Anti-requirements — what must NOT be present

*Each of these degrades or destroys the claim. Ordered by how badly.*

| # | If this is true… | Effect | Severity |
|---|---|---|---|
| A1 | **Personalized PageRank with arbitrary `sourceNodes`** | Cannot precompute at all. Falls to Layer 0–3 live execution: RAM win survives (~40–240×), speed win **inverts to 1.5–5× slower** | **FATAL to the speed claim** |
| A2 | **Result set is all 200M rows and that is the dominant pattern** | Bolt serialization is a shared 6–32 s floor. ~1000× collapses to ~6–85× | **SEVERE** |
| A3 | **Users vary `dampingFactor` / `tolerance` / `maxIterations`** | Config-hash miss on every non-default call → live path | **SEVERE** |
| A4 | **Arbitrary label / relationship-type filtering** | Combinatorial artifact explosion; cannot precompute the cross-product | **SEVERE** |
| A5 | **Sub-minute freshness requirement** | Kills assumption L1, kills every precompute, kills warm start | **FATAL to the whole design** |
| A6 | **No duplicate structure in the graph** | Quotient → 1×. RAM degrades from ~900× to ~100× | MODERATE |
| A7 | **Low-skew degree distribution** (mesh, road, regular) | Peel → <20%, and degree-ordering's cache win evaporates | MODERATE |
| A8 | **High churn between generations** (> ~20% edges) | Warm start stops working; every generation is a cold rebuild; `T_first` objection returns | MODERATE |
| A9 | **Many small frequent queries** | `T_open` dominates, and we are **currently 2.1× slower** than Neo4j there (MEASURED) | MODERATE — fixable in 2 days |
| A10 | **Colocated on the production Neo4j box** | Page-cache eviction and IOPS contention degrade OLTP p99 with heap flat (`A01` §1.3) | MODERATE — the hybrid topology fixes it |
| A11 | **HDD instead of NVMe** | `Arch06`: streaming degrades ~10× | MODERATE |
| A12 | **Weights that change per query** rather than per generation | The weight plane is part of the artifact; per-query weights force live execution | MODERATE |

> **A1 is the one that keeps me honest.** Personalized PageRank is not
> hypothetical — it is in the corpus as real user pain (`simulation01` E19: GDS
> issues #139/#132, *"personalized PageRank fails with OutOfMemoryError: unable
> to create native thread"*), and fraud teams genuinely use it to score money
> flow from a suspect account. **The precompute design does not serve it.** The
> Layer 0–3 live path does, at ~40–240× RAM and 1.5–5× slower — which is still
> a real product for someone who currently cannot run it at all, but it is not
> the headline.

## 7.6 The ideal profile — what makes it best

The technically ideal workload, stated so it can be qualified for in a sales
conversation:

| # | Ideal condition | Why it matters |
|---|---|---|
| I1 | **Identity / entity-resolution graph** | Built by joining records on shared attributes → exact structural twins are the dominant structure, not an accident. Quotient 8–10× |
| I2 | **Power-law degree distribution** | Peel reaches 55–60%; degree-ordered hot 1% fits L3 |
| I3 | **Fixed algorithm parameters** | One artifact serves every call; no config-hash misses |
| I4 | **Small result sets** — `topK`, `stats`, per-label top-N | No serialization floor. **This is where ~1000× lives** |
| I5 | **Read-heavy: many queries per generation** | Build amortizes immediately; break-even is ~1–2 reads |
| I6 | **Hourly-or-slower freshness tolerance** | Satisfies L1, licenses all precomputation |
| I7 | **Low churn between generations** (< ~5% edges) | Warm start converges in 2–4 iterations instead of 20 |
| I8 | **Unweighted, or weights stable per generation** | The weight plane can live in the artifact |
| I9 | **Dedicated NVMe box, separate from production** | No page-cache war; streaming at full bandwidth |
| I10 | **Repeat queries on the same generation** | Every re-read is pure profit |

**The convergence worth noticing:** I1 + I5 + I7 + I10 describe *exactly* the
fraud/AML profile `simulation01` §13.2 identifies as **re-run-heavy and the
deepest-pocketed segment.** The technically ideal workload and the
best-paying buyer are the same workload. That is not luck — it is why entity
resolution was chosen as the wedge.

**And the qualification question that falls out of this table** — the one to ask
a prospect in the first ten minutes:

```text
   "When you run PageRank, do you ask for all the scores, or the top N?
    And do you ever run it from a specific starting account?"

   all scores + specific start  ->  WORST case. We are a capacity product.
   top N + global               ->  BEST case. We are a 1000x product.
```

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

### 8.1 The hole, named plainly: personalized PageRank

```text
  gds.pageRank.stream('g', {sourceNodes: [suspectAccount]})

  This CANNOT be precomputed for arbitrary seeds -- there are 2^V possible
  seed sets. ARCH-I is useless here, and ARCH-I is where the 1000x lives.

  WHAT SURVIVES: the live path, Layers 0-3.
                 degree ordering + peel + quotient all still apply.
                 -> RAM ~40-240x better
                 -> latency 1.5-5x WORSE than resident GDS

  WHY IT MATTERS: this is real, documented user pain, not a hypothetical.
    simulation01 E19 / GDS issues #139, #132:
      "personalized PageRank fails with OutOfMemoryError:
       unable to create native thread"
    And fraud teams use exactly this to score money flow outward from a
    suspect account.

  THE HONEST POSITION: for personalized PageRank we are a CAPACITY product
  ("it finishes where GDS cannot start"), not a SPEED product. That is still
  worth selling to someone currently blocked -- but it must not be sold with
  the top-K numbers.

  PARTIAL MITIGATION: if the seed sets are drawn from a small known pool
  (e.g. the ~10k accounts currently under investigation), precompute per
  seed set. That is ARCH-V logic -- an index shaped by the query -- and it
  works only when the pool is small and stable.
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

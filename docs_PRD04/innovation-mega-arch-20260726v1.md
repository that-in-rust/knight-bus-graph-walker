# innovation-mega-arch-20260726v1 — Novel Architectures Under Perfect Foreknowledge

Date: 2026-07-26
Method: Timeline Traverser + first-principles systems design. **No internet
research.** Built on `mega-arch-20260726v1.md` and the quantitative floors in
`docs_PRD05/Neo4j-Rust-Two-Scenario-Estimation.md`.
Status: **original design proposals.** Every performance number is **MODELED**
with its basis shown. None is measured. §9 lists what would invalidate each.

**The premise:** we know, before the artifact is built, *which algorithm* will run
and *which query shape* it will serve. Therefore the artifact can be
supremely — even absurdly — specialized.

> **The thesis of this document in one line:** perfect foreknowledge does not
> mean "compress the graph better." It means **move work across the
> build/query boundary, and delete the parts of the graph the algorithm
> provably does not need.** Compression is a 2–4× game. The four ideas below
> are 10–1000× games, and three of them are not in the corpus anywhere.

---

## 1. Where PageRank's RAM and time ACTUALLY go

The corpus has consistently modelled PageRank as a bytes-of-topology problem.
That is wrong, and the error matters.

```text
   r[v] = (1-d)/N + d * SUM over in-edges(u->v) of  r[u] / outdeg(u)

   PER ITERATION, at V=200M, E=1B:
     topology read        5.22 GiB sequential      <- what the corpus counts
     rank-vector gather   1B random reads into a 1.6 GiB array
                                                   <- WHAT ACTUALLY COSTS
     out-degree lookup    1B random reads into a 0.8 GiB array
     rank write           200M sequential

   1B random accesses x ~80 ns effective latency  = ~80 SECONDS per iteration
   5.22 GiB sequential at 5 GiB/s NVMe            = ~1 SECOND per iteration
```

**The gather is 50–100× more expensive than the topology scan.** Every iteration
touches `r[u]` in source order, which is *random* with respect to the CSR layout.
This is the classic PageRank memory-wall, and it means:

> **A design that halves topology bytes but leaves the rank vector 1.6 GiB and
> randomly accessed has optimized the wrong thing.** GRAIN's Elias-Fano stratum
> would have done exactly that.

Three consequences, and they drive everything below:

1. **Shrink the rank vector until its hot region fits in cache** → turns random
   DRAM misses into L2/L3 hits. This is where the 10× lives.
2. **Shrink the vertex set** → fewer gathers, fewer everything.
3. **Stop iterating over vertices that have converged** → fewer gathers again.

---

## 2. The six architectures

### ARCH-I — "The Answer Is The Artifact" (build-time result materialization)

**Idea:** when the algorithm's result is (a) small, (b) parameter-free, and
(c) stable for an immutable generation, the optimal artifact *is the answer*.
Query time collapses to `O(1)` lookup.

Legal precisely because `Arch-options` E guarantees generations are immutable and
atomically published. Nothing can invalidate a precomputed answer within a
generation.

```text
   WCC on a sealed generation has EXACTLY ONE answer.
   Computing it at query time, repeatedly, is pure waste.

   BUILD:  one union-find pass -> component label per vertex
   QUERY:  mmap labels.u32[V], stream rows.  RAM = 0.745 GiB. Time = O(V).
           No topology read AT ALL.
```

| Applies cleanly | Does not apply |
| --- | --- |
| WCC, SCC, degree centrality, k-core, triangle count, degeneracy order, connected-component sizes | Anything parameterized: Dijkstra from arbitrary source, personalized PageRank, filtered similarity, `topK` variants |
| Global PageRank (one damping factor, one tolerance) | PageRank with user-chosen damping |

**Trade-off:** shifts cost to build; multiplies disk per precomputed answer;
staleness is bounded by generation cadence.

### ARCH-II — "Precision Ladder Fused To Degree Rank" (stratified state)

**Idea, and I believe this one is genuinely novel in this context:** co-design
**numeric precision** with the **degree distribution** so that the frequently
gathered portion of the rank vector fits in cache.

Power-law graphs have a brutal access skew: a small set of high-in-degree
vertices are the destination of most edges — and, symmetrically, a small set of
high-**out**-degree vertices are the *source* read by most gathers. Those hot
sources need precision. The long tail carries ranks near `(1-d)/N`, where f16 or
fixed-point error is provably below the convergence tolerance.

```text
   Reuse G1's degree-ranked dense IDs. The rank vector becomes:

   ranks[0 .. k1)      f64     top 1%   (2M verts)      16 MB   <- fits L3
   ranks[k1 .. k2)     f32     next 9%  (18M verts)     72 MB
   ranks[k2 .. V)      f16     tail 90% (180M verts)   360 MB
                                              TOTAL   ~448 MB

   vs a flat f64[V] = 1.60 GiB          -> 3.6x smaller
   vs a flat f32[V] = 0.80 GiB          -> 1.8x smaller

   AND the 16 MB hot stratum is L3-resident on any server CPU,
   so the majority of gathers become CACHE HITS, not DRAM misses.
```

**This is the crucial second-order effect:** the win is not the 3.6× of storage.
It is that the *most-read* 1% of the array stops missing cache. If 60–75% of
gathers land in the hot stratum (plausible on power-law in-degree), effective
gather latency drops from ~80 ns to ~15–25 ns.

**Error control:** declare it a mode. `exact` (all f64), `tolerance-matched`
(ladder, with a proof that per-stratum quantization error < the convergence
epsilon), `fast` (aggressive int16 fixed point). The receipt names the mode.

### ARCH-III — "Self-Compacting Iterative Artifact" (convergence-aware recompaction)

**Idea:** the artifact **physically rewrites itself mid-run** as vertices freeze.

Delta convergence is known (`Arch06` L3.b). What is *not* in the corpus is doing
the compaction **to the bytes on disk, during the run** — which you can only
justify when you own the artifact and know the algorithm.

```text
   iteration       1     2     3     4     5    ...    20
   active verts   100%  40%   12%   3%   0.5%        ~0.1%

   NAIVE:   20 x 5.22 GiB                    = 104.4 GiB of topology traffic

   SELF-COMPACTING:
     iters 1-3    3 x 5.22                   =  15.7 GiB
     recompact    write edges from active srcs =  0.6 GiB (one-time write)
     iters 4-20   17 x ~0.3 GiB avg          =   5.1 GiB
                                        TOTAL ~ 21.4 GiB   -> 4.9x LESS I/O
```

**Trade-off:** one write amplification event; only pays for >5 iterations;
needs a second recompaction threshold policy. Applies to PageRank, Louvain,
label propagation, SSSP — every iterative fixed-point family.

### ARCH-IV — "Iteration-Core Peeling" (solve the periphery in closed form)

**Idea:** most vertices in a power-law graph do not need to be *iterated* at all.
Their value is a closed-form function of vertices that do.

```text
   indeg(v) = 0   ->  r[v] = (1-d)/N FOREVER. Never iterate. Never store.
   indeg(v) = 1   ->  r[v] = (1-d)/N + d*r[u]/outdeg(u).
                      Computable in O(1) AFTER the core converges.
   chains         ->  collapse a path of indeg=1,outdeg=1 vertices into one edge
                      with a composite multiplier.
   dangling(outdeg=0) -> contributes uniformly; handle analytically, no per-vertex
                      iteration needed.

   ITERATE ONLY THE "2-CORE-ISH" KERNEL. Expand the periphery in ONE final pass.
```

On real power-law / identity graphs, indeg∈{0,1} plus dangling commonly accounts
for **30–60% of vertices**. Every one removed cuts rank-vector bytes, gathers,
and writes proportionally.

**Trade-off:** a peeling pass at build (cheap, one scan); the benefit is entirely
graph-shape dependent — near-zero on regular/mesh graphs, large on power-law.

### ARCH-V — "Query-Shaped Index Precomputation"

**Idea:** when the *query shape* is known, not merely the algorithm, precompute an
index that changes the complexity class.

| Query shape | Precomputed index | Effect |
| --- | --- | --- |
| point-to-point shortest path | **contraction hierarchies / hub labels** | Dijkstra `O(E log V)` → **microseconds**. 100–1000× |
| all-pairs similarity, `topK` | **LSH bands over MinHash, written to disk buckets at build** | quadratic candidate explosion → **bounded bucket scan** |
| k-NN over features | IVF / DiskANN-Vamana + PQ | disk-native ANN under a fixed cache budget |
| triangle / wedge | degeneracy order + oriented adjacency, materialized | removes the orientation decision from the hot loop |

**Trade-off:** build cost *is* the entire game; the index can exceed the graph
(hub labels notoriously so). Only justified for repeat queries — which is exactly
the fraud/AML re-run profile.

### ARCH-VI — "Structural-Equivalence Quotient" ⭐ *the one I'd bet on for identity*

**Idea:** vertices with **identical neighborhoods** are indistinguishable to
PageRank, WCC, NodeSimilarity, and label propagation. Collapse them into one
super-vertex carrying a multiplicity, solve on the quotient graph, and expand
analytically at the end.

```text
   BEFORE                                AFTER (quotient)
   10,000 records all sharing            1 super-vertex, multiplicity 10,000
   exactly {email_A, device_B}    ->     2 edges instead of 20,000

   r[original] = r[super] / multiplicity        (PageRank: exact)
   wcc[original] = wcc[super]                   (WCC: exact)
   sim[a,b] = 1.0 for any a,b in the class      (NodeSim: exact, and FREE)
```

**Why this is the right idea for entity resolution specifically:** an identity
graph is *built by joining records on shared attributes*. Structural equivalence
is not a rare accident there — **it is the dominant structure of the data**.
Thousands of records sharing one email/device/address form exact equivalence
classes by construction.

Estimated vertex reduction on ER-shaped graphs: **2–10×**, occasionally more.
And it is **exact**, not an approximation, for every algorithm whose result is a
function of the neighborhood multiset.

**Trade-off:** detection costs a hash of each sorted adjacency list at build
(one pass, `O(E)`); near-useless on graphs without duplicate structure (road
networks, meshes); needs an expansion map (`V × u32`, 0.745 GiB — or nothing, if
you keep the class ID *as* the dense ID).

*(Related known art: modular decomposition, bisimulation, graph summarization.
The twist here is making the quotient **the materialized artifact for a named
algorithm**, and exploiting the fact that ER graphs are unusually rich in exact
equivalence classes.)*

---

## 3. Decision Frame

**The fork:** for PageRank — the highest-payoff family in the ATLAS
(`very_high`) — which of these do we build first, and in what order do the rest
follow?

**Goal:** the largest defensible RAM *and* latency improvement over both (a)
tuned resident GDS and (b) our own current flat-CSR plan.

**Hard constraints:** solo capacity; must remain *exact* or declare its mode;
must be estimable before running (the receipt is the differentiator); artifact
build cost must be reportable per PRD05's `T_first` / `T_repeat` discipline.

**What counts as a win:** PageRank on a 200M/1B graph in **under 2 GiB resident**
with wall-clock **at or better than** tuned resident GDS — which would break
PRD05's own expectation that strict-RAM PageRank runs 1.5–5× slower.

**Assumptions stated:** (1) power-law degree distribution — false on meshes;
(2) ~20 iterations to tolerance; (3) NVMe at 3–5 GiB/s effective; (4) the graph
is generation-sealed and immutable.

---

## Timeline A — Precision Ladder first (ARCH-II)

- **Opening move:** degree-rank the IDs (G1, already planned), then build the
  three-stratum rank vector with a declared error mode.
- **Week 1:** ladder + f16 conversion + tolerance proof on fixtures. The
  arithmetic is easy; the *error argument* is the work.
- **Month 1:** measured gather-hit-rate on a real power-law graph. **This is the
  moment of truth** — if the hot stratum absorbs <40% of gathers, the idea is
  merely a 3.6× storage win and not the 10× latency win.
- **Quarter 1:** stacks trivially with everything else; becomes the default
  representation for PageRank, Eigenvector, HITS, and FastRP embeddings.
- **Likelihood of the storage win:** ~90%. **Of the cache win:** ~55% — it is a
  genuine empirical bet on access skew.
- **Stress points:** f16 in Rust needs care; the error proof will be argued with
  every skeptic; per-stratum branchy code can eat its own gains if written naively.
- **Inflection:** measured gather-hit-rate. Everything hinges on that one number.
- **Lived experience:** satisfying — small, self-contained, testable in isolation.

## Timeline B — Self-Compacting + Peeling first (ARCH-III + IV)

- **Opening move:** peel indeg∈{0,1} and dangling at build; recompact the edge
  stream at iteration 3.
- **Week 1:** the peel pass. Immediately measurable: *what fraction of this graph
  never needs iterating?* A single number that reprices the whole design.
- **Month 1:** recompaction working. I/O traffic drops ~5×; wall-clock drops less
  because the gather still dominates — **this is the honest disappointment of
  Timeline B**, and it is exactly the mistake §1 warns about.
- **Quarter 1:** combined peel+compact gives a solid, *unglamorous* 2–3× and
  applies to Louvain, LabelProp, SSSP unchanged.
- **Likelihood:** ~80% for the I/O win; ~45% that it moves wall-clock materially
  **on its own**.
- **Stress points:** recompaction is a write during a read-mostly job — it must
  not fight the page cache; threshold policy needs tuning per graph.
- **Inflection:** the peel fraction. If <20%, ARCH-IV is not worth it for this
  workload.
- **Lived experience:** steady, low-drama, and quietly less impressive than hoped.

## Timeline C — Answer-Is-The-Artifact first (ARCH-I)

- **Opening move:** precompute PageRank at publish; the artifact is `ranks.f32[V]`.
- **Week 1:** trivially works. Query time is `O(V)` streaming, RAM ~0.8 GiB, and
  **zero topology is read at query time.**
- **Month 1:** the catch surfaces. Build now costs a full PageRank per generation,
  so `T_first` balloons and the refresh cadence becomes the product's real
  constraint. Also: any user who wants a different damping factor falls off a
  cliff to the slow path.
- **Quarter 1:** for **WCC this is unambiguously correct and enormous** — a sealed
  generation has exactly one WCC answer and computing it repeatedly is pure waste.
  For PageRank it is correct only when the parameters are fixed.
- **Likelihood:** ~95% technically; ~50% that it is the right *product* default
  for PageRank specifically.
- **Stress points:** build-time explosion if every algorithm precomputes; needs
  the artifact lifecycle policy (A01) *before* shipping.
- **Inflection:** whether users vary PageRank parameters in practice. For fraud
  scoring they mostly do not.
- **Lived experience:** feels like cheating, in the good way.

## Timeline D — Structural-Equivalence Quotient first (ARCH-VI)

- **Opening move:** hash each vertex's sorted adjacency list at build; group
  identical hashes into equivalence classes; emit the quotient graph.
- **Week 1:** the detector. One `O(E)` pass. And immediately you learn the single
  most informative number about the customer's graph: **its equivalence-class
  compression ratio.**
- **Month 1:** PageRank and WCC on the quotient with exact expansion. On an
  ER-shaped graph this is where a 2–10× vertex reduction shows up — and it
  multiplies *every other* optimization, because it shrinks V before the ladder,
  the peel, and the gather all apply.
- **Quarter 1:** NodeSimilarity gets a bonus that is almost unfair: members of the
  same class are similarity-1.0 to each other **for free**, removing the densest
  and most explosive candidate pairs from the computation entirely.
- **Likelihood:** ~70% of ≥2× on genuine identity/ER graphs; ~25% on arbitrary
  graphs. **Highest variance of the four, and highest ceiling.**
- **Stress points:** hashing adjacency lists needs care with high-degree vertices;
  the expansion map must be proven exact per algorithm, individually.
- **Inflection:** the compression ratio on the first real ER graph. That one
  measurement decides whether this is the headline or a footnote.
- **Lived experience:** the most intellectually exciting, the most likely to
  produce a genuinely publishable result — and the most likely to disappoint on a
  graph that turns out to have no duplicate structure.

---

## 4. Cross-Timeline Analysis

| Path | RAM win | Latency win | Exact? | Stacks? | Graph-shape risk | Regret |
| --- | --- | --- | --- | --- | --- | --- |
| **A** Precision ladder | **3.6×** on state | **2–4×** *if* cache bet lands | mode-declared | with all | power-law only | **Low** |
| **B** Peel + compact | ~1.4× | ~1.3–2× | **exact** | with all | power-law only | Low |
| **C** Answer-is-artifact | ~2× at query | **10–100×** at query | **exact** | orthogonal | none | Med (build cost) |
| **D** Quotient | **2–10×** on V | **2–10×** | **exact** | **multiplies A, B, C** | ER-shaped only | **High variance** |

**R1.** These are not competitors. **They compose multiplicatively**, and D
composes *first* because it shrinks V before anything else applies.

**R2.** A attacks the thing that actually costs (the gather). B attacks the thing
the corpus *thought* cost (topology bytes). **If you can only do one, do A.**

**R3.** C is the correct default for WCC *today* and should not wait for anything.
A sealed generation has one WCC answer.

**R4.** D has the highest ceiling and is uniquely suited to the stated use case.
It is also the only one whose payoff is unknown until you look at a real customer
graph — making it the cheapest *learning* experiment, at one `O(E)` pass.

**R5.** Every one of these is only legal because generations are **immutable**.
Immutability, chosen for publication safety, turns out to be what makes
aggressive precomputation sound. That is the corpus's luckiest accident.

---

## 5. Decision Filter

**Strongest if everything goes normally:** **D → A → C → B.**
Measure the quotient ratio first (one pass, one week, and it reprices everything
else). Ship the precision ladder next, because it attacks the real bottleneck.
Precompute WCC immediately — it is free and obviously right. Add peeling and
self-compaction last, as the unglamorous 1.5×.

**Safest if things go badly:** **C then A.** C cannot fail — precomputing WCC on
an immutable generation is unconditionally correct. A degrades gracefully to a
pure 3.6× storage win even if the cache bet loses.

**The experiments that collapse the uncertainty:**

```text
  Y1  QUOTIENT RATIO on a real ER graph.            1 pass, ~1 week
      Hash sorted adjacency lists, count classes.
      Output: ONE NUMBER that reprices every other idea here.
      Kill: ratio < 1.3x -> ARCH-VI is a footnote for this workload.

  Y2  GATHER HIT-RATE against a stratified vector.  ~1 week
      Instrument the PageRank gather; what % lands in the top-1% stratum?
      Kill: < 40% -> ARCH-II is storage-only, not a latency play.

  Y3  PEEL FRACTION.                                 ~2 days
      What % of vertices have indeg in {0,1} or outdeg 0?
      Kill: < 20% -> skip ARCH-IV entirely.
```

Three numbers, under three weeks, and they determine the entire design.

---

## 6. PageRank — the estimate table

**All figures MODELED at V=200M, E=1B directed. Basis shown. Not measured.**

| Design | Resident RAM | Topology I/O per run | Est. wall-clock vs tuned resident GDS | Exact? | Basis |
| --- | ---: | ---: | ---: | --- | --- |
| **Neo4j GDS (tuned, resident)** | **88–144 GiB** | 0 (all resident) | **1.0× baseline** | exact | PRD05 model; LDBC100 guide 110 GB |
| Our flat CSR, f64, naive | 5.22 + 4.47 = **9.7 GiB** | 104 GiB | 1.5–5× slower | exact | PRD05 Duck 14 |
| Our flat CSR, f32, streamed | **2.3 GiB** | 104 GiB | 1.5–4× slower | exact | PRD05 |
| **+ ARCH-II precision ladder** | **1.0 GiB** | 104 GiB | **0.8–2.0×** | mode | 448 MB × 2 vectors; gather hits ↑ |
| **+ ARCH-IV peeling** | **0.5–0.7 GiB** | 42–73 GiB | 0.6–1.5× | **exact** | 30–60% of V removed |
| **+ ARCH-III self-compaction** | 0.5–0.7 GiB | **~15–25 GiB** | 0.5–1.2× | **exact** | 4.9× I/O cut (§2) |
| **+ ARCH-VI quotient (ER graph)** | **0.1–0.35 GiB** | **3–12 GiB** | **0.15–0.6×** | **exact** | 2–10× V and E reduction |
| **ARCH-I precomputed answer** | **0.4–0.8 GiB** | **0** | **~0.01×** at query | exact | `ranks.f32[V]`, `O(V)` stream |

**Headline claims, stated with their uncertainty:**

```text
  RAM:      88-144 GiB  ->  0.5-1.0 GiB   =  ~100-200x     [HIGH confidence:
                                                             deterministic
                                                             arithmetic]
            on ER graphs with quotient    =  ~300-1000x    [MEDIUM: depends on
                                                             equivalence ratio]

  LATENCY:  0.5-1.2x of tuned resident GDS               [LOW-MEDIUM confidence]
            i.e. COMPARABLE OR BETTER while using ~1/100th the RAM.

            This would BREAK PRD05's own expectation (strict-RAM PageRank
            = 1.5-5x slower). The reason it can: PRD05's model assumed
            EXTERNALIZING bytes. ARCH-II/IV/VI ELIMINATE bytes -- the other
            mechanism from Correction 6, which reduces RAM *and* latency.

  QUERY-TIME (ARCH-I): ~0.01x -- but T_first now includes a full PageRank,
            and PRD05 requires reporting T_first, T_repeat, and T_total(N)
            separately. Amortizes after ~1 re-read.
```

---

## 7. The other hard / high-use algorithms

| Algorithm | Best architecture | Custom artifact | Est. RAM | Est. speed | Exact? |
| --- | --- | --- | ---: | ---: | --- |
| **WCC** *(20% adoption)* | **ARCH-I** — no contest | `component.u32[V]`, computed once at build | **0.745 GiB**, zero topology | **~100×** at query | **exact** |
| | +ARCH-VI | quotient first → labels on classes | **0.1–0.4 GiB** | ~100× | exact |
| **NodeSimilarity** *(12%, the OOM king)* | **ARCH-V + VI** | LSH bands → disk buckets at build; equivalence classes give sim=1.0 free; rare-neighbor inverted lists | **bounded by `topK`, ~0.5–2 GiB** vs *unbounded* | **finishes vs OOMs** | approx (declared) or exact-rerank |
| **Louvain** *(15%)* | **ARCH-VI + III + I** | quotient; level-1 coarsening precomputed at build; self-compaction per level | **1–3 GiB** vs >10.4 GiB floor | 2–5× | exact on quotient; tally cap = approx |
| **Dijkstra / SSSP** *(10%)* | **ARCH-V** | **contraction hierarchies / hub labels** | index-dominated (0.5–3× graph) | **100–1000×** point-to-point | **exact** |
| **Triangle / LCC** *(5%)* | **ARCH-I + G1** | degeneracy-ordered oriented adjacency + precomputed counts | ~0.3 GiB | **~50×** (precomputed) | exact |
| **FastRP** *(8%)* | **ARCH-II + VI** | int8/f16 embedding ladder; quotient collapses duplicate rows | **4× smaller** embeddings; V reduced | 2–4× | approx (quantized) |
| **BFS / k-hop** | ARCH-IV + D1 | peeled core + push/pull switching | ~0.1 GiB | 2–10× | exact |

**The two entries I would defend hardest:**

1. **WCC = ARCH-I.** It is embarrassing that a sealed, immutable generation
   recomputes union-find on every call. Precompute it. This is a one-week change
   with a ~100× query-time result, and it is *unconditionally exact*.
2. **NodeSimilarity = ARCH-VI.** In an identity graph, the densest similarity
   pairs are exactly the structurally-equivalent records — and the quotient
   removes them from the candidate explosion **while answering them exactly and
   for free.** The family that breaks every architecture is broken *by its own
   data's redundancy*.

---

## 8. Composition: the stacked artifact

```text
   BUILD PIPELINE (per algorithm, on demand, from the Build Store)

   1. QUOTIENT       hash sorted adjacency -> equivalence classes    [ARCH-VI]
                     V: 200M -> 20-100M on ER graphs
   2. PEEL           strip indeg{0,1}, dangling, chains              [ARCH-IV]
                     V_active: another 30-60% off
   3. DEGREE-RANK    sort by descending degree (G1)                  [free]
   4. LADDER         emit stratified precision plan for the state    [ARCH-II]
   5. INDEX          algorithm-specific: CH / LSH / oriented adj     [ARCH-V]
   6. PRECOMPUTE     if parameter-free, run it now                   [ARCH-I]
   7. MANIFEST       exact bill: resident bytes, streamed bytes,
                     disk bytes, and the expansion map               [G3]

   QUERY: mmap, stream, expand. Receipt printed before a byte is read.
```

**Every stage is `O(E)` or `O(E log E)` once, at build.** That is the trade this
architecture makes: **pay linear build cost to buy sublinear query cost**, which
is only sound because generations are immutable.

---

## 9. Honest caveats — what would invalidate each idea

| Idea | Dies if… | Detection cost |
| --- | --- | --- |
| ARCH-I precomputation | users vary parameters; or refresh cadence makes build cost dominate | ask 5 users |
| ARCH-II ladder | gather hit-rate < 40%; or f16 error exceeds tolerance on real data | Y2, 1 week |
| ARCH-III compaction | fewer than ~5 iterations; recompaction write fights the page cache | 2 days |
| ARCH-IV peeling | peel fraction < 20% (regular/mesh graphs) | Y3, 2 days |
| ARCH-V CH/LSH | index exceeds the graph; queries are one-shot not repeated | 1 week |
| **ARCH-VI quotient** | **equivalence ratio < 1.3× on real customer graphs** | **Y1, 1 week — do this first** |

**Three global caveats, stated plainly:**

1. **Every number here is modeled.** The corpus's own history is that modeled
   multipliers get corrected downward on contact with reality — `Arch06` cut
   50–100× to 10–30× the one time it read real GDS source. Expect the same.
2. **The latency claim is the weak one.** RAM arithmetic is deterministic; the
   latency claim depends on the gather-hit-rate bet (ARCH-II) and on the
   equivalence ratio (ARCH-VI). Both are empirical and unmeasured.
3. **None of this matters without §5's three experiments.** Y1, Y2, Y3 are three
   weeks total and they decide which of these six architectures is real.

---

## 10. The one-page summary

```text
  THE INSIGHT PageRank's cost is the RANDOM GATHER into the rank vector,
              not the topology scan. Corpus optimized the wrong thing.

  ARCH-I      the answer IS the artifact        -> WCC, triangles: ~100x, exact
  ARCH-II     precision ladder fused to degree  -> hot stratum fits L3: 3.6x
              rank                                 RAM, 2-4x gather
  ARCH-III    artifact recompacts mid-run       -> 4.9x less I/O
  ARCH-IV     peel the periphery, solve closed  -> 30-60% of V never iterates
              form
  ARCH-V      query-shaped index (CH, LSH)      -> changes complexity class
  ARCH-VI     structural-equivalence quotient   -> 2-10x on ER graphs, EXACT,
              *** and multiplies everything else ***

  STACKED     PageRank: 88-144 GiB -> 0.5-1.0 GiB  (~100-200x RAM)
              at 0.5-1.2x the wall-clock of tuned resident GDS

  WHY IT CAN  because these ELIMINATE bytes rather than EXTERNALIZE them --
  BE FASTER   the distinction PRD05 Correction 6 insists on, and the reason
              this does not contradict "strict-RAM PageRank is 1.5-5x slower".

  WHY IT IS   because generations are IMMUTABLE. Aggressive precomputation is
  LEGAL       sound only because nothing can invalidate it within a generation.

  DO FIRST    Y1 quotient ratio (1 wk) -> Y2 gather hit-rate (1 wk) ->
              Y3 peel fraction (2 d).  Three numbers. Three weeks.
```

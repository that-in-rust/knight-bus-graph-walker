# innovation-storage-timelines-20260726v2 — Five More Storage Designs From First Principles

Date: 2026-07-26
Method: Timeline Traverser simulation. **No internet research.** First-principles
extensions of `innovation-mega-arch-20260726v1.md` (ARCH I–VI), keeping its two
premises: (P1) generations are sealed and immutable; (P2) the algorithm and
query shape are known before the artifact is built.
Status: **original design proposals.** Every number is MODELED, none measured.
Each timeline names the single measurement that would kill it.

The organizing question this round: ARCH I–VI changed *what* is stored
(answers, precision, quotients). What is left is to change **what an artifact
IS** — the order of bytes, the identity of bytes across time, and whether the
artifact is data at all.

---

## Decision Frame

- **Fork in the road:** which *new class* of storage artifact to prototype next,
  beyond ARCH I–VI, for the seven-family workload on sealed generations.
- **Desired outcome:** a further order-of-magnitude on the true cost center
  (the random gather / candidate explosion), or a qualitatively new capability
  (time travel, graceful approximation) — while staying exact-or-declared.
- **Hard constraints:** solo capacity; mmap-friendly; estimable from the
  manifest before running; must compose with ARCH I–VI (especially VI, which
  shrinks V first).
- **Time horizon:** week 1 / month 1 / quarter 1 per path.
- **What counts as failure:** an artifact that is clever but not measurably
  better than flat CSR + the existing six ideas on a real ER-shaped graph.

Assumptions (stated once): power-law degree; ~20 iterations for fixed-point
families; NVMe 3–5 GiB/s; generation cadence minutes-to-hours, not
milliseconds.

---

## Timeline A: THE EXECUTION TAPE — storage as compiled program

**Idea:** stop storing the *graph*; store the *execution*. At build time,
compile one iteration of the known algorithm into a linear instruction tape
whose operands are pre-resolved byte offsets, laid out in the exact order the
CPU will consume them. The artifact is bytecode; the runtime is a dumb replayer.

```text
   TODAY (CSR)                        TAPE
   for v in V:                        stream of fixed-width ops:
     for u in in(v):                    ACC  dst=v, src_off=0x1A2B  (add r[u]/d[u])
       acc += r[u]/outdeg(u)   ->       ACC  dst=v, src_off=0x99F0
   random r[u] lookups                  EMIT dst=v
                                      operands sorted AT BUILD so src_off
                                      is (block-)monotonic: the gather
                                      becomes a MERGE, not a sprint.
```

The build-time trick: sort the per-edge work items by (src_block, dst) instead
of by dst alone — the classic propagation-blocking idea, but *baked into the
bytes on disk*, so the runtime never re-derives the schedule. Two sub-variants:
(A1) one tape reused every iteration; (A2) per-iteration tapes emitted by
ARCH-III as vertices freeze (the tape shortens itself).

- **Opening move:** define a 12-byte op word; emit a tape for PageRank on a
  fixture; replay it against the flat-CSR baseline.
- **Week 1:** replayer at memory bandwidth on the fixture. The question is
  already visible: tape size = ~12B/edge = 12 GiB at E=1B vs 5.22 GiB CSR —
  2.3× more bytes, all sequential.
- **Month 1:** the honest trade emerges: sequential 12 GiB (~3 s at 4 GiB/s)
  replaces 1B semi-random gathers. If the effective gather cost is ≥3× the
  scan (per the corrected §1 estimate), the tape wins ~2–5×; if prefetchers
  already hide the gathers, it wins nothing and costs disk.
- **Quarter 1:** tape emission fused with ARCH-VI (quotient first, tape on the
  quotient) and ARCH-III (tapes shrink per epoch). Louvain/label-prop reuse
  the same op format.
- **Long-term shape:** the artifact family becomes {graph, answers, tapes} —
  a compilation cache for algorithms, all sealed per generation.
- **Likelihood:** ~70% it beats flat CSR on cold runs; ~40% it beats
  CSR+precision-ladder on warm runs (the ladder already de-randomizes the hot
  1%).
- **Stress points:** disk amplification per (algorithm × generation); op
  format churn; the replayer must be boring or the whole point is lost.
- **Inflection:** measured replay time vs measured gather time on ONE real
  graph — a two-day experiment.
- **Lived experience:** compiler-flavored fun; the risk is falling in love
  with the bytecode instead of the benchmark.

## Timeline B: THE CANDLE FILE — layout ordered by predicted freeze time

**Idea:** ARCH-III rewrites the file when vertices converge. Remove the
rewrite: at build time, order vertices (and their edges) by *predicted
convergence epoch* — periphery first, dense core last. Then convergence-aware
execution never compacts; it just **moves the file's left boundary rightward**.
The artifact burns down like a candle; the active set is always a suffix.

```text
   file:  [ freeze@it2 | freeze@it4 | freeze@it7 | ... | 2-core kernel ]
            ^ after iteration 2, NEVER read again — no rewrite needed,
              the active window is [k..end] and mmap drops the cold pages
   predictor: peel depth (ARCH-IV's k-shell / onion layer) is a
   build-time-computable proxy for freeze order — periphery freezes first.
```

- **Opening move:** compute onion-layer decomposition at build; emit
  layer-ordered CSR; run PageRank tracking actual freeze epoch vs layer.
- **Week 1:** the predictor scatter plot exists. This is the kill-or-thrive
  chart: correlation between shell depth and freeze epoch.
- **Month 1:** if correlation is strong (expected on power-law: leaves freeze
  in 2–3 iterations), I/O per iteration decays geometrically with zero write
  amplification — ARCH-III's ~5× I/O win without ARCH-III's mid-run write.
- **Quarter 1:** the same layout accelerates ARCH-IV expansion (periphery is
  contiguous) and improves page-cache behavior for *every* algorithm that
  processes by activity, not by ID.
- **Long-term shape:** "freeze-ordered" becomes the default sort key of the
  format, alongside degree rank — two orderings, one chosen per family.
- **Likelihood:** ~75% for the I/O decay on power-law graphs; near-0 on
  meshes/roads (uniform freeze).
- **Stress points:** degree rank (ARCH-II) and freeze rank (this) FIGHT over
  the ID space — need either a dual-ID map or a composite key
  (freeze-epoch-major, degree-minor), which costs one indirection somewhere.
- **Inflection:** the predictor correlation, week 1.
- **Lived experience:** low drama, one beautiful chart decides everything.

## Timeline C: THE HILBERT FOLD — cache-oblivious adjacency tiling

**Idea:** treat the adjacency matrix as a 2D plane and store edge blocks along
a space-filling curve (Hilbert order). Any contiguous run of the file touches a
compact (src, dst) rectangle, so BOTH the source gathers and the destination
scatters stay within a bounded working set — without tuning tile sizes per
cache level (cache-oblivious).

```text
   row-major CSR:  dst locality perfect, src locality random   <- today
   Hilbert order:  every 1 MB of tape covers ~a square patch
                   of the matrix -> src range AND dst range
                   both bounded -> both ends fit in L2/L3
```

- **Opening move:** Hilbert-sort the edge list at build (one external sort);
  runtime accumulates into a small dst scratch per curve segment.
- **Week 1:** PageRank over Hilbert-ordered edges on the fixture; compare
  DRAM misses (perf counters) vs CSR and vs the tape (Timeline A).
- **Month 1:** the trade is visible: Hilbert loses the "one pass per dst"
  property (partial sums must merge), costing ~4B/vertex of accumulator
  traffic — but every byte is cache-local. Expected 2–4× fewer misses on
  power-law graphs, less on graphs whose hubs smear the plane.
- **Quarter 1:** the same layout serves triangles (2D locality is exactly
  wedge locality) and SpGEMM-style NodeSimilarity candidate generation —
  one layout, three families.
- **Long-term shape:** Hilbert becomes the S2 (cold-tile) stratum's internal
  order in GRAIN, replacing naive 2D tiling.
- **Likelihood:** ~65% for a measurable miss-rate win; ~35% that it beats
  Timeline A where both apply (they attack the same cost).
- **Stress points:** hub rows break square locality (mitigate: ARCH-VI first,
  and split hubs into their own dense stripe — the S0 stratum already exists
  for exactly them).
- **Inflection:** perf-counter miss rates, week 1. A vs C is an empirical
  duel; only one becomes the default.
- **Lived experience:** deeply satisfying systems work; danger of
  curve-fitting elegance over the boring winner.

## Timeline D: QUOTIENT-WITH-RESIDUAL — near-twins, not just twins

**Idea:** ARCH-VI collapses vertices with *identical* neighborhoods. One step
further: collapse vertices whose neighborhoods are identical **up to k edges**,
store the class representative once, and keep a tiny signed **residual list**
(+edge/-edge) per member. Storage becomes `representative + diff`, and
algorithms run on the quotient with per-member correction terms.

```text
   10,000 near-twins sharing {email_A, device_B}, 200 of which also
   touch device_C:
     ARCH-VI:  cannot merge (not exact twins) -> 10,000 vertices remain
     ARCH-D:   1 class + residuals: 9,800 members diff=∅,
               200 members diff={+device_C}
   exactness: PageRank/WCC on class + first-order correction from the
   residual edges; where correction > tolerance, DEMOTE the member to the
   exact graph. The artifact chooses exactness per member, provably.
```

- **Opening move:** MinHash each adjacency list at build; cluster near-twins;
  measure the residual-size histogram on a real ER graph.
- **Week 1:** the histogram exists — the analogue of ARCH-VI's compression
  ratio, and strictly ≥ it (near-twins ⊇ exact twins).
- **Month 1:** the correctness machinery is the real work: a per-algorithm
  bound on when residuals can be folded analytically vs when to demote. WCC
  is easy (any residual edge just unions classes); PageRank needs the
  first-order term; NodeSimilarity gets bounded from the diff sizes.
- **Quarter 1:** on ER-shaped data expect the vertex reduction to rise from
  ARCH-VI's 2–10× toward 5–20×, with an exactness ledger per member — the
  receipt now itemizes *which rows are exact and why*.
- **Long-term shape:** this is dedup-with-patches — the storage identity of a
  vertex becomes (class, diff), the same move source control made when it
  went from full files to deltas.
- **Likelihood:** ~60% of a material win over plain ARCH-VI on real ER data;
  ~20% elsewhere.
- **Stress points:** the demotion boundary must be conservative or the
  "exact" claim dies; MinHash clustering quality on skewed degrees.
- **Inflection:** residual histogram, week 1 — if near-twins ≈ exact twins,
  ARCH-VI already took the whole prize and D is a footnote.
- **Lived experience:** highest intellectual payoff; the proofs will consume
  more calendar than the code.

## Timeline E: THE GENERATION BRAID — results stored as deltas across time

**Idea:** generations differ by ~2% of edges, so *results* differ by less.
Store result sidecars (ARCH-I artifacts) as **signed deltas against the
previous generation**, plus periodic keyframes — video encoding for graph
answers. Reads reconstruct by replaying deltas from the nearest keyframe;
builds compute *incrementally* by seeding from generation N-1 (the
differential-dataflow idea, applied to the artifact store rather than the
runtime).

```text
   gen 100: wcc.labels keyframe        0.745 GiB
   gen 101: +Δ (3 MB)   gen 102: +Δ (2 MB) ... gen 110: keyframe
   30 days of hourly generations ≈ 1 keyframe-equivalent + noise
   AND: build of gen N's PageRank seeds from gen N-1's ranks ->
   converges in 2-4 iterations instead of 20 (warm start), so the
   ARCH-I build-cost objection (T_first balloons) mostly dissolves.
```

- **Opening move:** delta-encode WCC labels across two real consecutive
  snapshots; measure delta size and warm-start iteration count for PageRank.
- **Week 1:** both numbers known. Delta size tracks churn; warm-start
  savings track how local the churn is.
- **Month 1:** keyframe policy (every k gens or when Δ > threshold);
  reconstruction cost bounded and manifest-declared.
- **Quarter 1:** the braid gives time-travel queries ("WCC as of Tuesday")
  almost free — the AXIS-1/AXIS-2 proposals from Arch05, but scoped to
  *answers only*, which is 100× smaller than braiding topology.
- **Long-term shape:** the store becomes {topology gens} + {answer braids};
  the OLAP-lag story upgrades from "fresh snapshot soon" to "warm-started
  answers minutes behind OLTP."
- **Likelihood:** ~85% for the storage win (arithmetic); ~65% for the
  warm-start win (depends on churn locality — adversarial churn resets it).
- **Stress points:** correctness across braided history (a bad delta
  corrupts everything after it — checksums per link, periodic full
  verification); WCC label stability across gens needs canonical labeling
  (min-member) or deltas explode spuriously.
- **Inflection:** measured delta size on real churn, week 1.
- **Lived experience:** the least glamorous and the most product-shaped —
  this one changes what the customer can ASK (history), not just the bill.

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | graph-shape risk | composes with |
| --- | --- | --- | --- | --- | --- | --- |
| A tape | 2–5× on the true cost center | 2.3× disk per algo; duel with C | high (sidecar) | med | power-law helps | VI, III, B |
| B candle | ARCH-III's win, zero rewrite | ID-space fight with degree rank | high | **low** | power-law only | III, IV, A |
| C Hilbert | 2–4× fewer misses, 3 families | partial-sum traffic; hub smear | high | med | hub-sensitive | VI, S0/S2 strata |
| D residual quotient | 5–20× V-reduction on ER | proof-heavy; conservative demotion | med | med-high | **ER-shaped only** | everything (shrinks V first) |
| E braid | history + warm starts + tiny storage | delta-chain fragility | med (keyframes) | **low** | churn-locality risk | ARCH-I, AXIS-1/2 |

Inflection points are all week-1 measurements: replay-vs-gather time (A),
freeze-predictor correlation (B), perf-counter misses (C), residual histogram
(D), delta size + warm-start count (E). Not one requires building the full
system.

## Decision Filter

- **Strongest if everything goes normally:** E (the braid) — highest
  likelihood, product-visible (history + freshness), and it quietly fixes
  ARCH-I's build-cost objection via warm starts.
- **Safest if things go badly:** B (the candle) — worst case it degenerates
  into plain ARCH-III behavior with a nicer layout; no write amplification,
  no new formats.
- **Highest ceiling:** D on identity data, but only after ARCH-VI's plain
  quotient has been measured (D's week-1 histogram tells you both).
- **Fastest uncertainty collapse:** one two-week measurement sprint running
  ALL FIVE week-1 experiments on one real ER-shaped graph — they share the
  same fixture and instrumentation, and together with Y1–Y3 from v1 they
  reprice the entire innovation portfolio with ~7 numbers.
- **A vs C duel note:** they attack the same gather; build both week-1
  probes, keep the winner, and let the loser die without sentiment.

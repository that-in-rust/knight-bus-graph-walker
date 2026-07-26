# innovation-storage-timelines-20260726v3 — Storage Designs For The Corrected Cost Model

Date: 2026-07-26
Method: Timeline Traverser. **No internet research.** First-principles round 3,
building on v1 (ARCH I–VI), v2 (tape / candle / Hilbert / residual quotient /
braid), and — critically — on v1's new **§0 corrections**:

```text
  C1  RELOCATE ≠ ELIMINATE; only deletion wins on total work
  C2  under lag-assumption L1, build is ~free; optimize T_repeat
  C3  we currently LOSE on T_open (189.979 ms vs Neo4j 90.446 ms, v002)
  C4  T_materialize can swamp everything (3.2 GB stream vs 4 KB top-K)
  C5  KNOWING THE QUERY IS WORTH MORE THAN KNOWING THE ALGORITHM
```

v1/v2 optimized `T_execute`. The corrected model says the remaining frontier
is **the other three terms** — `T_open`, `T_materialize`, and the build
window — plus the one thing no artifact so far addresses: what to store when
you *don't* fully know the query. Status: original proposals; every number
MODELED; each timeline names its week-1 kill measurement.

---

## Decision Frame

- **Fork in the road:** which storage design to prototype next, now that the
  cost model is `T_first / T_repeat / T_open / T_materialize` and the unit is
  `(algorithm × query shape × cadence)`.
- **Desired outcome:** wins on the terms v1/v2 do not touch: open latency,
  serialization tail, partial query knowledge, and build-window fit.
- **Hard constraints:** solo capacity; mmap-friendly; sealed generations;
  receipt-declared exactness; must not regress the existing T_open further.
- **Time horizon:** week 1 / month 1 / quarter 1.
- **What counts as failure:** artifacts that shave `T_execute` further while
  the buyer's felt latency stays dominated by open + serialize (per C3/C4).

Assumptions: L1 lag accepted (hourly-class cadence); power-law graphs;
top-K-style consumption dominates "stream everything" in real usage (untested
— Timeline A measures it).

---

## Timeline A: THE ANSWER LATTICE — a store of query-shaped results

**Idea:** C5 taken to its logical end. Don't store one answer per algorithm;
store a small **lattice of pre-shaped answers** keyed by the query predicates
the workload actually uses: global top-K, per-label top-K, per-community
aggregates, histogram/quantile summaries. Each cell is tiny (KBs); the whole
lattice is smaller than one rank vector.

```text
   ranks.f32[V]                0.8 GiB     <- ARCH-I stores this
   lattice:
     top-100 global            4 KB
     top-100 x 500 labels      2 MB
     per-community mean/max    a few MB
     score histogram (1k bins) 8 KB
   TOTAL: ~MBs. Serialization tail: gone. T_open for the cell: ~0.
   miss path: fall through to ranks.f32[V], then to recompute —
   a THREE-LEVEL answer cache, all sealed per generation.
```

- **Opening move:** instrument real usage (or design partners' query logs) to
  learn the predicate distribution; build top-K + per-label cells for
  PageRank/WCC on a fixture.
- **Week 1:** the lattice exists; the kill measurement is the **hit rate**:
  what fraction of real queries are answerable from cells.
- **Month 1:** cell schema stabilizes (predicate → cell naming, manifest
  lists cells + their exactness); the receipt now says "answered from cell
  X, exact, 0.4 ms".
- **Quarter 1:** the lattice becomes the product's visible surface — the
  `impact(x)` / `communities()` outcome API from the friend's doc maps 1:1
  onto cells. The engine becomes the lattice *builder*.
- **Long-term shape:** storage inverts: answers are the primary artifact,
  topology is the fallback for lattice misses and new predicates.
- **Likelihood:** ~85% technically trivial; the bet is entirely on hit rate
  (~60% that ≥80% of real queries hit cells).
- **Stress points:** predicate discovery without query logs; lattice
  explosion if predicates multiply (cap: build on demand per C2's unit rule).
- **Inflection:** measured hit rate on one design partner's workload.
- **Lived experience:** feels like product work, not engine work — which is
  exactly why it might be the highest-leverage storage design on this page.

## Timeline B: THE HOT BOOT — the artifact is a resumable memory image

**Idea:** attack C3 directly. Today `open` = map files + validate offsets +
scan node records + build key index = 190 ms. Instead, serialize the
*runtime's warmed state itself* as a sealed sidecar — pre-built key index,
pre-validated offset tables, pre-computed pointers as position-independent
offsets — so open = mmap + verify one 64-byte header checksum. The artifact
is not data for a program; it is the program's memory, parked on disk.

```text
   TODAY:   open = map + validate everything eagerly     ~190 ms
   HOT BOOT: build time writes boot.img =
             { key index | offset tables | validation receipts }
             open = mmap boot.img + header checksum       ~1-5 ms
             deep validation becomes a BACKGROUND task, or is
             skipped: the generation is sealed and was validated
             at publish — revalidating at open is paying twice.
   (C1 check: this is ELIMINATION — the open-time work is deleted,
    because publish-time validation already proved the invariants.)
```

- **Opening move:** profile the current 190 ms open (where do the
  milliseconds go?); move the top item into a build-time sidecar.
- **Week 1:** profile done; likely suspects (key-index build, offset scans)
  relocated to publish. Kill measurement: T_open after the first relocation.
- **Month 1:** boot.img format v0; open under 10 ms on the fixture; the
  "validated at publish, trusted at open" receipt wired into the manifest.
- **Quarter 1:** T_open beats Neo4j's 90 ms by ~10-50×; frequent-small-query
  workloads (the lattice's consumption pattern!) stop being open-dominated.
- **Long-term shape:** every artifact family (CSR, lattice cells, tapes)
  ships its boot image; "cold open" disappears from the vocabulary.
- **Likelihood:** ~90% for <20 ms; ~70% for <5 ms.
- **Stress points:** position-independence discipline (no absolute pointers
  in the image); versioning (a boot.img is runtime-version-coupled — keep
  the raw artifacts as the portable truth, boot.img as a rebuildable cache).
- **Inflection:** the open-time profile, day 2.
- **Lived experience:** unglamorous, measurable, and it fixes the one number
  where the project is currently *losing* — high morale per line of code.

## Timeline C: THE GRAMMAR GRAPH — queries on compressed structure

**Idea:** power-law + join-built graphs are structurally repetitive (v1
ARCH-VI proved the extreme case). Store adjacency as a **grammar**: recurring
sub-structures (bicliques, stars, chains) become rules; the graph becomes a
small rulebook + a sequence of rule applications. Algorithms run **on the
grammar** — a rule applied 10,000 times is processed once and multiplied.

```text
   ER graph fragment: 5,000 records all -> {email_A, device_B, plan_X}
   flat:     15,000 edges
   grammar:  R1 := (* -> email_A, device_B, plan_X);  R1 x 5000
   PageRank contribution of the 5,000: computed ONCE from R1,
   scaled by 5,000. This generalizes ARCH-VI: quotient = grammar
   with only exact-duplicate rules; v2-D = grammar with diff rules;
   THIS = arbitrary shared substructure (bicliques are the big one:
   a KxM biclique is K*M edges, one rule, K+M applications).
```

- **Opening move:** biclique/star mining pass at build on a real ER graph;
  measure grammar compression ratio vs plain quotient.
- **Week 1:** the ratio exists. Kill threshold: if grammar ≤ 1.5× better
  than ARCH-VI's quotient, the added complexity is not worth it.
- **Month 1:** PageRank-on-grammar for star and biclique rules with exact
  expansion; the receipt declares which rule families were exploited.
- **Quarter 1:** NodeSimilarity gets the unfair bonus again (rule members
  have known similarity structure); topology I/O drops by the compression
  ratio for every scan family.
- **Long-term shape:** the S1/S2 strata store rules, not edges; the format
  becomes "graph grammar + exceptions," and the exceptions list is small.
- **Likelihood:** ~50% of a ≥2× win over quotient on ER data; ~15% on
  arbitrary graphs. Highest variance on the page.
- **Stress points:** mining cost (biclique detection is expensive — bound it
  with degree-ordered heuristics and a build budget per C2); per-algorithm
  exactness proofs multiply per rule family.
- **Inflection:** week-1 compression ratio vs quotient baseline.
- **Lived experience:** research-grade excitement, publication potential,
  and the standing danger of elegance outrunning the benchmark.

## Timeline D: THE SKETCH DECK — approximate answers as first-class bytes

**Idea:** the ESTIMATE → APPROXIMATE → EXACT ladder (Arch05 AXIS-3) needs a
storage citizen for the middle rung. Store a **deck of sketches** per
generation: per-node HyperLogLog neighborhood cardinalities, per-block
min/max/degree summaries, landmark-distance vectors, sampled ego-nets. Each
answers a family of questions in milliseconds with a *declared* error bar.

```text
   "roughly how big is X's 2-hop neighborhood?"   HLL merge      ~exact ±2%
   "is there a path A->B under 4 hops?"           landmark bound  yes/no/maybe
   "top communities by size, roughly?"            block summary   ±5%
   deck size: MBs against a 50 GiB graph. And the deck IS the
   estimator the Budget Machine needs: admission control reads the
   deck, not the graph — the price oracle and the approximate answer
   become THE SAME ARTIFACT.
```

- **Opening move:** HLL-per-node + block summaries at build; wire the
  admission estimator to read the deck.
- **Week 1:** deck built on the fixture; kill measurement: estimator error
  distribution vs exact counts (must fit the declared bars).
- **Month 1:** the "maybe" path: landmark selection (high-betweenness seeds)
  and the tri-state path answer with measured false-maybe rate.
- **Quarter 1:** the answer ladder ships: ESTIMATE (manifest arithmetic) →
  APPROXIMATE (deck, ms, error-barred) → EXACT (budgeted run, receipt).
  Product-wise this is the demo that makes the receipt story legible.
- **Long-term shape:** the deck becomes the standing contract between the
  Budget Machine and the data — estimation stops being code that guesses
  and becomes bytes that were measured at build.
- **Likelihood:** ~80% for the deck mechanics; the open question is product
  pull for approximate answers (~50%).
- **Stress points:** error-bar honesty under adversarial graph shapes;
  sketch staleness is zero (sealed generations — the same L1 luck as v1 §6).
- **Inflection:** whether design partners ever *use* the middle rung, month 2.
- **Lived experience:** deeply aligned with the project's honesty brand;
  risk is building a beautiful rung nobody stands on.

---

## Cross-Timeline Analysis

| path | attacks | upside | downside | reversibility | regret risk | variance |
| --- | --- | --- | --- | --- | --- | --- |
| A lattice | T_materialize + C5 | serialization tail → 0; product-shaped surface | needs predicate knowledge | high (cells are sidecars) | **low** | low |
| B hot boot | T_open (C3) | fixes the one losing number; 10-50× on open | version-coupled cache discipline | high (rebuildable) | **low** | low |
| C grammar | T_execute + bytes | generalizes VI; biggest compression ceiling | proof + mining cost; ER-only | med | med-high | **high** |
| D sketch deck | estimation + new rung | price oracle = artifact; demo-able honesty | product pull unproven | high | med | med |

Composition note: A and B are *multiplicative on the same workload* — lattice
cells only shine if opening them is ~free; B makes it free. C feeds A/D by
shrinking what the fallback path costs. D is the only one that changes what
the Budget Machine *is*.

Inflection points, all cheap: predicate hit rate (A, needs a design partner),
open-time profile (B, day 2, no partner needed), grammar-vs-quotient ratio
(C, week 1), estimator error fit (D, week 1).

## Decision Filter

- **Strongest if everything goes normally:** A + B together — they attack the
  two terms the corrected cost model says the buyer actually feels
  (open + serialize), they are low-variance, and they turn the v1 corrections
  into shipped artifacts rather than caveats.
- **Safest if things go badly:** B alone. It needs no design partner, no
  predicate discovery, no proofs — just a profiler and discipline — and it
  converts a known measured loss (190 ms vs 90 ms) into a headline win.
- **Highest ceiling / research bet:** C, but gate it behind ARCH-VI's
  measured quotient ratio (v1 Y1): if plain twins already give 5×+, grammar's
  marginal complexity is probably not worth it; if twins give <2×, grammar
  is the only road to the big compression number on that data.
- **Fastest uncertainty collapse:** B's day-2 profile plus D's week-1 error
  fit — both partner-independent. Then A's hit-rate measurement the moment
  the first design partner's query log exists (which the 90-day plan's
  discovery interviews produce anyway).
- **Ordering with v1/v2:** unchanged for the engine (quotient → ladder →
  candle), but B jumps the queue entirely — it is the only item on any of
  the three pages that repairs a *measured regression* rather than chasing
  a modeled gain.

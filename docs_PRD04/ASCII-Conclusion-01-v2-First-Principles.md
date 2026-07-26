# ASCII-Conclusion-01-v2: Deriving PageRank From The Hardware Up

Terminal companion to `Conclusion-01-v2-First-Principles-Derivation.md`.
Also reconciles `innovation-storage-timelines-20260726v3.md`
against the corrections that derivation produced.

Canvas: 72 columns. Every number below is DERIVED from the
constants in section 2, or MEASURED and marked so. Nothing is a
ratio quoted from a vendor.

---

## 1. Why the method had to change

The first version anchored to ratios. That is a trap: it
inherits
every error in the baseline, cannot be checked without running
the competitor, and assumes the competitor is good.

```text
   RATIO-ANCHORED  (what we did first)
   +----------------------------------+
   | "GDS takes 3-9 min.              |
   |  Ours takes 0.4 s.               |
   |  Therefore ~1000x."              |
   +----------------------------------+
              |
              | inherits the baseline's errors
              v
        unfalsifiable

   FLOOR-ANCHORED  (what we do now)
   +----------------------------------+
   | 1. list hardware constants       |
   | 2. count bytes that MUST move    |
   | 3. derive the floor for ANYONE   |
   | 4. locate GDS against the floor  |
   | 5. locate US against the floor   |
   +----------------------------------+
              |
              | the ratio is now an OUTPUT
              v
        checkable by anyone with a spec sheet
```

---

## 2. The only inputs

Everything below is derived from these. Commodity 2026 server
class.

```text
   DRAM sequential bandwidth        25-35 GB/s
   DRAM random-64B throughput       10-20 GB/s
   DRAM latency                     80-100 ns   <- see 7.1
   outstanding misses per core      10-16
   L3 size                          16-64 MB
   cache line                       64 B        <- the unit of traffic
   NVMe sequential read             3-5 GB/s
   Bolt/PackStream encode           50-200 MB/s

   WORKLOAD:  200,000,000 nodes   1,000,000,000 edges   20 iterations
```

---

## 3. Count the bytes PageRank must move

Not "how fast is it". First: what work is unavoidable?

```text
   THINGS THAT MUST EXIST ON DISK OR IN RAM

     reverse CSR peers    4 B x 1e9        = 4.0 GB
     reverse CSR offsets  8 B x 2e8        = 1.6 GB
                                             -------
                            topology       = 5.6 GB

     two rank vectors   2 x 4 B x 2e8   = 1.6 GB  IRREDUCIBLE
```

That 1.6 GB is a hard floor. You cannot compute the next rank
from the current one without holding both. Anything smaller must
come from having FEWER NODES, not from being clever.

```text
   TRAFFIC PER ITERATION

     read topology                          5.6 GB   sequential
     read+write ranks                       2.4 GB   sequential
     THE GATHER: 1e9 lookups, each pulling a 64-BYTE LINE
                 to use 4 bytes of it (94% waste)

     if 0% of lookups hit cache   1e9 x 64 B = 64 GB
     if 50% hit cache             5e8 x 64 B = 32 GB
     if 70% hit cache             3e8 x 64 B = 19 GB

     TOTAL = 8 GB + (19 to 64 GB) = 27 to 72 GB per iteration
```

```text
   WHERE THE TRAFFIC GOES

   topology   |###|                              5.6 GB
   ranks      |#|                                2.4 GB
   gather     |##########  ...  ##########|   19-64 GB   <- DOMINANT
```

---

## 4. The floor, derived

```text
   27 to 72 GB per iteration
   / 10-30 GB/s achievable
   = 1.2 to 3.6 seconds per iteration
   x 20 iterations
   = 24 to 72 SECONDS

   NO implementation of this workload on this hardware beats
   ~24 s. Not ours. Not theirs. Not a future one.
```

---

## 5. Where the incumbent actually sits

Neo4j's own published LDBC100 figure, scaled to our edge count:

```text
   hardware floor      |####|                        24- 72 s
   GDS observed        |########################|      ~230 s
                        ^
                        GDS runs 3x to 10x ABOVE the floor
```

This is the most useful fact in the derivation. The headroom in
PageRank belongs to their implementation, not to the problem.
A competent native implementation should approach the floor
whether or not it is clever about storage.

---

## 6. Where we sit

Streaming topology from disk instead of holding it in memory:

```text
   resident topology   5.6 GB / 30 GB/s = 0.19 s per iteration
   streamed topology   5.6 GB /  4 GB/s = 1.40 s per iteration
                                          ------
   streaming costs                        +1.2 s/iter = +24 s total

   our streamed total = floor + 24 s = 48 to 96 s
```

```text
   THE THREE-WAY COMPARISON

   hardware floor      |####|                        24- 72 s
   OUR STREAMED        |########|                    48- 96 s
   GDS observed        |########################|      ~230 s
   OUR PRECOMPUTED     ||                              0.39 s

   vs the floor    : 1.3-2.0x SLOWER   (honest engineering)
   vs GDS observed : 2.4-4.8x FASTER   (honest product)

   Both are true. Say which one you mean.
```

We can plausibly beat GDS while streaming from disk. Not because
streaming is fast. Because GDS leaves 3-10x on the table.

---

## 7. Four things this derivation refuted

Including the biggest error, which was mine.

```text
+--------------------------------------------------------------------+
| 7.1  THE GATHER WAS OVERSTATED BY ~10x                             |
+--------------------------------------------------------------------+
| I claimed  : 1e9 lookups x 80 ns = 80 SECONDS per iteration        |
|              "the gather is 50-100x the topology scan"             |
|                                                                    |
| The error  : that assumes ONE outstanding miss at a time. A         |
|              PageRank gather loop has NO dependency chain between   |
|              lookups, so an out-of-order core keeps 10-16 misses    |
|              in flight. The loop is BANDWIDTH-bound, not            |
|              LATENCY-bound.                                        |
|                                                                    |
| Corrected  : 19-64 GB / 10-20 GB/s = 1.0 to 6.4 s per iteration    |
|              the gather is 3-10x the scan, not 50-100x             |
|                                                                    |
| Survives   : the gather IS still the dominant term. The conclusion  |
|              held; the magnitude was inflated tenfold, and the      |
|              inflation flowed into every number downstream.        |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| 7.2  THE RAM CLAIM COMPARED THE WRONG TWO THINGS                   |
+--------------------------------------------------------------------+
| I compared : our ANALYTICS footprint  vs  88-144 GiB               |
| But that   : 88-144 GiB is a WHOLE-MACHINE figure. Our own PRD05    |
|              says it "includes an OLTP resident/cached component".  |
|                                                                    |
| Derived from GDS's own compression source instead:                  |
|   1e9 edges x 2-5 B/edge          = 2.0-5.0 GB                     |
|   2e8 nodes x 12 B fixed          = 2.4 GB                         |
|   Pregel state                    = ~4.5 GB                        |
|                                     ----------                     |
|   GDS NECESSARY analytics RAM      = ~9-12 GB                      |
|                                                                    |
| Three honest baselines, 15x apart:                                  |
|   GDS necessary     9- 12 GB   <- use this for engineering claims  |
|   GDS observed        ~51 GB                                        |
|   whole machine    94-155 GB   <- use this ONLY for "retire the box"|
|                                                                    |
| Corrected  : 6-150x, not 150-900x.                                 |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| 7.3  SERIALIZATION IS WORSE THAN I SAID                            |
+--------------------------------------------------------------------+
| PackStream row = ~20 B (int64 + float64 + type markers)             |
| 2e8 rows x 20 B = 4.0 GB on the wire                                |
| at 50-200 MB/s encode (CPU-bound) = 20 to 80 SECONDS                |
|                                                                    |
| I said 6-32 s. Optimistic by ~2.5x. And BOTH engines pay it, so     |
| it is a shared floor that caps the achievable ratio:                |
|                                                                    |
|   GDS   230 s + 20-80 s = 250-310 s                                |
|   OURS  0.4 s + 20-80 s = 20.4-80.4 s                              |
|   ratio = 3x to 15x     NOT 1000x                                  |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| 7.4  PEELING HELPS RAM, NOT LATENCY                                |
+--------------------------------------------------------------------+
| Peeled nodes have at most ONE incoming edge. So peeling 60% of      |
| the nodes removes at most 12% of the EDGES.                        |
|                                                                    |
|   state traffic  drops ~60%   <- big RAM win                       |
|   topology bytes drop ~12%    <- small latency win                 |
|                                                                    |
| I implied both dropped together. They do not.                       |
+--------------------------------------------------------------------+
```

---

## 8. The one idea that is not bounded by bandwidth

Everything in section 7 fights over constant factors against a
fixed memory subsystem. Exactly one design escapes that fight.

```text
   LIVE         work = O(edges x iters)  = 20 billion touches
   PRECOMPUTED  work = O(nodes) once     = 0.8 GB sequential

   That is not a better constant. That is a different exponent.

   derived query time:
     open the file      190 ms   MEASURED, 2.1x worse than Neo4j
     read 0.8 GB @ 4 GB/s 200 ms
                          ------
                          390 ms
```

```text
   SO THE PORTFOLIO SPLITS IN TWO

   +----------------------------+  +----------------------------+
   | BOUNDED BY DRAM BANDWIDTH  |  | CHANGES THE EXPONENT       |
   |                            |  |                            |
   |   cache locality           |  |   precompute the answer    |
   |   compression              |  |                            |
   |   better layout            |  |   62x vs the floor         |
   |   fewer passes             |  |  590x vs GDS observed      |
   |                            |  |                            |
   | ceiling = section 4 floor  |  | the ONLY one that escapes  |
   +----------------------------+  +----------------------------+
```

---

## 9. Reconciling the v3 designs against these corrections

The v3 page was written on the PRE-correction cost model, where
the gather was assumed 50-100x the scan. Anything resting on
that number needs the same haircut section 7.1 gave mine.

```text
+--------------------------------------------------------------------+
| A  ANSWER LATTICE            -- SURVIVES, STRENGTHENED             |
+--------------------------------------------------------------------+
| Attacks the serialization tail, which section 7.3 shows is WORSE    |
| than anyone thought (20-80 s, not 6-32 s), and which both engines  |
| pay. Storing query-shaped answers deletes our side of that floor   |
| entirely. The derivation raises this design's value.               |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| B  HOT BOOT                  -- SURVIVES, STRENGTHENED             |
+--------------------------------------------------------------------+
| Attacks file-open cost, which is MEASURED at 190 ms and is the     |
| only number where we currently LOSE. After precomputation it is    |
| 49% of a full query and 100% of a top-K query.                     |
|                                                                    |
| Derived floor for open: mmap syscall 10-50 us + one page fault      |
| 5-100 us + a 64-byte checksum = about 0.05 to 0.2 ms.              |
| Their 1-5 ms target is therefore CONSERVATIVE and achievable.      |
|                                                                    |
| Two analyses reached this independently. That is the strongest      |
| signal on the page: fix open first.                                |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| C  GRAMMAR GRAPH             -- SURVIVES, JUSTIFICATION WRONG      |
+--------------------------------------------------------------------+
| It claims  : "topology I/O drops by the compression ratio for      |
|               every scan family."                                  |
|                                                                    |
| But        : topology is only 5.6 GB of 27-72 GB per iteration,    |
|              i.e. 8% to 21% of traffic. Compressing it 3x saves    |
|              3.7 GB -- about 5% to 14% of the total. NOT 3x.       |
|                                                                    |
| What is    : collapsing repeated structure collapses GATHERS, the  |
| actually     dominant term -- exactly how the quotient wins.       |
| true         Grammar is a generalized quotient, and should be      |
|              argued that way.                                      |
|                                                                    |
| Keep their kill threshold: if grammar beats plain twin-collapsing  |
| by less than 1.5x, the complexity is not worth it.                 |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| D  SKETCH DECK               -- HALF OBSOLETE                      |
+--------------------------------------------------------------------+
| It claims  : "the deck IS the estimator the Budget Machine needs;  |
|               admission control reads the deck, not the graph."    |
|                                                                    |
| Superseded : under per-algorithm materialization the estimate is   |
|              EXACT arithmetic over an artifact whose shape we just  |
|              wrote. No sketching is required to price a run. This  |
|              is the same simplification that retired the earlier   |
|              universal-manifest-polynomial bet.                    |
|                                                                    |
| Survives   : the APPROXIMATE-ANSWER rung as a product feature      |
|              ("roughly how big is this neighbourhood?"). That is   |
|              a demand bet, not an engineering necessity, and it    |
|              should be argued as one.                              |
+--------------------------------------------------------------------+
```

```text
   NET EFFECT ON THE V3 PAGE

   A  strengthened   ship it
   B  strengthened   SHIP IT FIRST -- it repairs a measured loss
   C  keep, rewrite the argument from bytes to gathers
   D  split: estimator claim dead, product rung alive
```

---

## 10. What to measure, ordered by what the derivation exposed

```text
   +-------------------------------------------------------+
   | M1  GATHER CACHE-HIT RATE                    1 week   |
   |     Sets per-iteration traffic between 27 and 72 GB.  |
   |     A 2.7x swing on the dominant term. Only the        |
   |     graph can answer this; no derivation can.         |
   +-------------------------------------------------------+
   | M2  DUPLICATE-STRUCTURE RATIO                1 week   |
   |     Sets RAM between 1.6 GB and 0.08 GB. It is the    |
   |     ONLY route below the 1.6 GB hard floor.           |
   +-------------------------------------------------------+
   | M3  FIX FILE-OPEN COST                       2 days   |
   |     MEASURED 190 ms vs Neo4j 90 ms. Floor is 0.05-    |
   |     0.2 ms, so there is 1000x of headroom in the one  |
   |     place we are losing. Best ratio on the page.      |
   +-------------------------------------------------------+
   | M4  BOLT ENCODE THROUGHPUT                   2 days   |
   |     Sets the shared serialization floor between 20 s  |
   |     and 80 s. Caps every stream-all claim regardless  |
   |     of how good the engine is.                        |
   +-------------------------------------------------------+
   | M5  FUSE 1/OUTDEGREE INTO THE EDGE           3 days   |
   |     Out-degree lookup is a SECOND random stream per   |
   |     edge. Inlining it costs 4 B/edge on disk and      |
   |     removes 1e9 random accesses per iteration --      |
   |     halving the dominant term. Cheapest large win,    |
   |     and invisible to the ratio-based method.          |
   +-------------------------------------------------------+
```

---

## Reading notes

```text
   Section 4 is load-bearing. If the 24-72 s floor is wrong,
   every comparison on the page moves.

   Section 5 carries the best news, and it is about THEM:
   GDS runs 3-10x above the floor, so beating it does not
   require brilliance.

   Section 7 is the page's honesty. Four claims were reduced,
   one of them tenfold. The reduced version is what to say
   out loud.

   Section 8 is the strategic takeaway: exactly one design
   escapes the bandwidth fight. Weight the roadmap for it.

   RETRACTED from the earlier version: "1000x faster" with no
   qualifier. For a query returning everything it is 3-15x.
   The large number belongs to small-result queries only.

   Full derivation with sources:
   Conclusion-01-v2-First-Principles-Derivation.md
```

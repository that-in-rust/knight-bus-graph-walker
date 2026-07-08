# Architecture Summary (ASCII, ELI5) — All Approaches x Algorithm Families

Date: 2026-07-08
Scope: one-page-per-idea summary of the whole Arch01-Arch05 series.
Deep dives: `Arch01.md` (five base architectures), `Arch02.md` (algorithm
families), `Arch03.md` (build order), `Arch04.md` (brownfield vs greenfield),
`Arch05.md` (GRAIN format). ELI5 first, tables after.

---

## 0. The Problem In One Picture

```
   Neo4j GDS today                       Our target
   ----------------                      ----------
   PageRank on LDBC100 : 110 GB RAM      same jobs
   Louvain  on LDBC100 : 119 GB RAM  --> finish on an
   FastRP   on LDBC100 : 254 GB RAM      8-16 GB box
   (their own sizing guide)              + know the bill BEFORE running
```

Rules of the game (from the PRD):
- OLAP reads only **immutable, sealed snapshot generations** (photos).
- Freshness = publish photo N+1. Never merge at query time.
- Every procedure must estimate its memory before running.

---

## 1. The Five Base Architectures (Arch01)

Three of them answer **"how are the BYTES stored?"** (A, B, D).
Two of them answer **"how does the RUNTIME behave?"** (C, E).
That asymmetry is why they compose instead of compete.

### A. MONOLITH — "one big flat photo"

```
  vertices:  [v0|v1|v2|v3|v4|...]              (dense u32 ids)
  fwd edges: [--v0's--|--v1's--|--v2's--|...]  (flat CSR)
  rev edges: [--v0's--|--v1's--|--v2's--|...]  (flat CSR, reversed)
  props:     [age.col] [score.col] ...         (columnar sidecars)
```

- ELI5: everything in one straight line; reading it is just sliding along.
- WINS: fastest possible scans. Simple. mmap-friendly.
- DIES: the moment graph + scratch > RAM.

### B. TILEHOUSE — "photo cut into tiles"

```
        dst 0-9  dst 10-19  dst 20-29
       +--------+---------+---------+
src    | tile   | tile    | tile    |   load tiles one at a time;
0-9    |  1,1   |  1,2    |  1,3    |   when a tile's vertices are
       +--------+---------+---------+   "done" (converged), SKIP it
src    | tile   | tile    | tile    |   on the next pass
10-19  |  2,1   |  2,2    |  2,3    |
       +--------+---------+---------+
```

- ELI5: don't hold the whole photo; hold one tile at a time.
- WINS: graphs way bigger than RAM; skipping finished tiles.
- DIES: never — but taxes small graphs with tile overhead.

### C. BUDGET MACHINE — "bouncer at the door" (RUNTIME layer)

```
              +-----------------------+
   job -----> |  estimate memory cost |
              +-----------+-----------+
                          |
        +-----------------+------------------+
        v                 v                  v
    fits in RAM      too big, but        hopeless
        |            spillable              |
        v                 v                 v
    run in-core     run out-of-core     REJECT + print
    (fast)          via disk spill      the exact bill
                    (slow but FINISHES)
```

- ELI5: nothing runs without a price check; too-big jobs go the slow
  door instead of exploding.
- WINS: **Louvain & NodeSimilarity** — the two families whose problem is
  scratch state, not byte layout. No storage format alone saves them.
- DIES: never — but it's a policy layer; it needs bytes (A/B) underneath.

### D. FOUNDRY — "print several formats, pick per job"

```
   build store ---> [ flat copy ]  [ compressed copy ]  [ tiled copy ]
                          \              |                  /
                           +---- pick best per algorithm --+
```

- ELI5: keep three photos of the same class; use whichever suits today.
- WINS: mixed workloads, in theory.
- DIES: 3x build cost, 3x storage, parity-proving every copy.
- FATE (Arch03): shrinks to **capability flags in a manifest** — "which
  artifacts exist for this generation" — not a standalone architecture.

### E. ALGEBRA ENGINE — "everything is matrix math" (RUNTIME layer)

```
   graph = sparse matrix A
   PageRank:   r = alpha * A^T * r + ...     (repeat)
   Triangles:  count = trace(A^3) / 6
   BFS:        frontier' = A^T x frontier    (boolean semiring)
```

- ELI5: stop writing graph code; write multiply-and-add loops.
- WINS: PageRank, triangles, BFS/paths — the naturally algebraic ones.
- DIES: Louvain/NodeSim are awkward as pure algebra.
- FATE (Arch03): demoted to an **optional backend** for the scan families.

---

## 2. The Seven Algorithm Families (Arch02) — Who Needs What

~85% of Neo4j GDS adoption rides on seven families. Two kinds of pain:

```
  SCAN problems  = "read all edges, repeatedly"   -> layout solves them
  STATE problems = "scratch memory explodes"      -> only the bouncer (C)
                                                     solves them
```

| family                | ~adoption | pain type | best home |
| --------------------- | --------- | --------- | -------------------------- |
| WCC (components)      | 20%       | scan      | A (small) / B tiles (big)  |
| Louvain / Leiden      | 15%       | STATE     | **C** (spill or budget)    |
| PageRank              | 15%       | scan      | A or E                     |
| NodeSimilarity / KNN  | 12%       | STATE     | **C** (bounded candidates) |
| Shortest paths        | 10%       | scan-ish  | B tiles (skip converged)   |
| FastRP (embeddings)   | 8%        | scan      | A (pure scans)             |
| Triangle counting     | 5%        | scan      | A + degree order (or E)    |

Key Arch02 finding: **27% of adoption weight (Louvain + NodeSim) cannot be
saved by any byte layout** — that's why C is mandatory, not optional.

---

## 3. The Composite (Arch03) — They Stack

```
        +---------------------------------------------+
   (C)  |  BUDGET MACHINE: estimate -> admit/spill/   |   the runtime
        |  reject; every job gets a receipt           |
        +---------------------------------------------+
   (E)  |  optional algebra kernels for scan families |   the backend
        +---------------------------------------------+
        |  BYTES (one format, three strata):          |
   (A)  |    hot  stratum = flat CSR                  |   the storage
   (B)  |    cold stratum = 2D tiles (spill unit)     |
        |    warm stratum = compressed                |
        +---------------------------------------------+
   (D)  |  MANIFEST: what exists, sizes, stats, flags |   the catalog
        +---------------------------------------------+
```

Arch03's real conclusion: the fork stopped being "which architecture" and
became "which layer to build first." Answer: substrate (budget/spill) first.

---

## 4. GRAIN (Arch05) — The Composite As A File Format

Greenfield move: design the format around **estimation**, then storage.

```
gen-000042/                          <- one sealed generation ("photo")
  MANIFEST.cbor      ~1 KB           <- THE ESTIMATOR (see below)
  ranks.dense                        <- ids renumbered by DESCENDING degree
  strata/
    S0.hot/   fwd.csr rev.csr        <- few giant vertices: flat CSR
    S1.warm/  fwd.ef  rev.ef         <- mid band: Elias-Fano compressed
    S2.cold/  blocks/PxQ.ef          <- long tail: 2D tiles, streamable
  sidecars/  <prop>.col              <- columnar properties
  receipts/  build.json              <- provenance + checksums
```

The trick that is new:

```
  MANIFEST carries: |V|, |E|, exact degree-CDF knots, stratum sizes,
                    block occupancy histogram
                          |
                          v
  memory cost of each of the 7 families = CLOSED-FORM ARITHMETIC
  over those numbers. Estimate cost: read 1 KB. No sampling. No code.
                          |
                          v
  "You will know the bill BEFORE we read a single graph byte,
   and the job will finish anyway (spill), or you get the bill (reject)."
```

vs Neo4j: their `.estimate` is code that guesses, guaranteed only for
production-tier algorithms; their cloud bills by RAM you must declare
up front (and AuraDS bills RAM even when paused).

Sorting by degree (ranks.dense) is what makes the strata work:

```
  degree |*
         |**
         |****                 S0.hot   (plain, scanned constantly)
         |********       ---- cut ----
         |**************       S1.warm  (compressed, random access)
         |********************* ---- cut ----
         |***************************** S2.cold (tiled, streamed)
         +----------------------------- vertex rank
```

---

## 5. Beyond GRAIN — Three Unexplored Axes (proposed, not yet in a doc)

Everything above innovates on SPACE (one frozen photo). Three axes left:

```
AXIS 1: TIME — "git for snapshots"
   gen N:   [blk_a][blk_b][blk_c][blk_d]        blocks are content-
   gen N+1: [blk_a][blk_b][blk_e][blk_d]        addressed (hash-named);
                            ^only this is new    N+1 mostly POINTS at N.
   30 days of history ~ price of 1 + deltas.

AXIS 2: ANSWERS — "the photo remembers its homework"
   sidecar: wcc.labels@gen-N
   gen N+1 arrives -> recompute WCC only on the 2% delta,
   seeded from gen N's labels. Incremental > recompute.

AXIS 3: APPROXIMATION — "rough answer from the sticker"
   manifest also holds sketches (HyperLogLog neighborhoods,
   block-level min/max bounds) ->
   answer ladder:  ESTIMATE (KB, instant)
                -> APPROXIMATE (MB, sketches, seconds)
                -> EXACT (budgeted run, priced receipt)
```

---

## 6. The Whole Series In Four Lines

```
Arch01: invented A-E; discovered bytes-layers and runtime-layers COMPOSE.
Arch02: 7 families = 85% of adoption; Louvain+NodeSim need C, full stop.
Arch03: architecture settled (the stack in §3); fork = build ORDER.
Arch04/05: brownfield vs greenfield; GRAIN = the stack frozen as a
           file format whose manifest IS the estimator.
```

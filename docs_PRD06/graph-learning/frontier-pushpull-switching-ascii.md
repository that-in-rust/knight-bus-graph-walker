# Frontier PushPull Switching — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `frontier-pushpull-switching-ascii.md` / `frontier-pushpull-switching-mermaid.md` |
| One-line job | Run each traversal step in whichever direction touches fewer edges — push from a small frontier, pull into a huge one — switching at a measured threshold |

## 1. The job

Frontier-based algorithms (BFS, connected components, delta-stepping
SSSP, even PageRank convergence) repeat one step: from the current
frontier, update neighbors. There are two ways to run the step:

```text
PUSH (top-down):   for u in frontier:        cost ~ sum of frontier
                     for v in out(u):               out-degrees
                       try claim v

PULL (bottom-up):  for v in ALL unvisited:   cost ~ sum of unvisited
                     for u in in(v):                in-degrees, BUT
                       if u in frontier:            EXITS EARLY on
                         claim v; break              first hit
```

Push wins when the frontier is small (touch only its edges). Pull wins
when the frontier is huge: an unvisited vertex likely has SOME parent
in it, so the inner loop exits after ~1-2 probes instead of the
frontier pushing to v many times over. On social/web graphs BFS
frontiers explode to >50% of the graph in the middle hops — exactly
where pull saves an order of magnitude.

## 2. Raw data shape

The two directions demand different machinery — this is why the CSR
pair said direction switchers hold CSR AND CSC:

```text
push needs:  out-CSR + frontier as a QUEUE/sparse list
             + atomic claim (CAS on parent[v])
pull needs:  in-CSR (CSC) + frontier as a BITMAP
             (O(1) "is u in frontier?") + NO atomics
             (each v claims itself — only writer of its own slot)

frontier conversions at every switch:
  queue -> bitmap: scatter bits         (cheap, parallel)
  bitmap -> queue: scan + compact       (a prefix-sum pass)
```

gapbs's `bfs.cc` carries both representations and converts on switch;
ligra hides the same duality inside `edgeMapData`'s sparse/dense split
(`ligra.h:236-282`).

## 3. The switching rule, from the two witnesses

```text
gapbs (bfs.cc:123-167, direction-optimizing BFS after Beamer):
  switch PUSH -> PULL when
      scout_count > edges_to_check / alpha       (alpha = 15)
      i.e. frontier's out-edges exceed 1/15 of unexplored edges
  switch PULL -> PUSH when
      awake_count < num_nodes / beta              (beta = 18)
      i.e. frontier shrank below 1/18 of vertices

ligra (ligra.h:238-261, generic for ANY edgeMap):
  threshold = numEdges / 20   (default)
  if |frontier| + sum(outDegrees) > threshold -> DENSE (pull)
  else                                        -> SPARSE (push)
```

Same idea, two granularities: gapbs tunes a BFS-specific hysteresis
(two constants, asymmetric); ligra makes ONE rule the engine-level
contract so every algorithm written as edgeMap inherits switching for
free. GraphIt turns the choice into a compiler schedule — the source
names `DensePull` as its default direction
(`include/graphit/frontend/schedule.h:123-124`) and generates
either loop from one algorithm text.

## 4. Step-by-step: direction-optimized BFS on a social graph

```text
hop  frontier size   direction  why
1    1 (source)      PUSH       tiny frontier, few edges to scout
2    ~300            PUSH       still < edges/alpha
3    ~2M             PULL       scout_count blew past the threshold:
                                most vertices are about to be reached;
                                let each one find its own parent
4    ~40M (peak)     PULL       early-exit saves ~deg probes per v
5    ~1M             PULL       awake_count still > n/beta
6    ~10k            PUSH       frontier collapsed; scanning ALL
                                unvisited vertices would now waste
                                more than pushing 10k edge lists
7    ~50             PUSH       tail
```

The asymmetric hysteresis (alpha=15 vs beta=18) prevents oscillating
on plateau-shaped frontiers.

## 5. Worked example 1 — the arithmetic of one middle hop

Graph: n = 100M, m = 2B (avg deg 20). Hop with frontier = 40M
vertices, 30M still unvisited:

```text
PUSH cost: frontier out-edges = 40M x 20 = 800M edge traversals,
           each an atomic CAS attempt on a shared parent slot
PULL cost: unvisited in-edge probes with early exit ~ 30M x ~2
           = 60M probes, no atomics, sequential CSC reads
           => ~13x fewer edges touched, and the memory traffic is
              the well-behaved kind (pattern 7's streaming scans)
```

Beamer's measured result (the gapbs lineage): 3-8x whole-search
speedup on low-diameter graphs, entirely from the middle hops.

## 6. Worked example 2 — when switching is a loss

Road network: n = 20M, m = 50M (avg deg 2.5, diameter ~6000):

```text
frontier never exceeds ~0.1% of vertices (a thin wavefront) ->
scout_count never crosses edges/alpha -> pure PUSH throughout.
The pull machinery (CSC copy: +200 MB, bitmap conversions) is pure
overhead: dead memory and code.

lesson: direction switching is a POWER-LAW-GRAPH optimization.
Engines expose the knobs (gapbs's alpha/beta arguments,
graphit's schedule language) because no constant is right for both
topologies.
```

## 7. Where graph systems inherit this

- ligra made switching the engine primitive: every algorithm in its
  suite (BFS, BC, radii, CC) is edgeMap calls and inherits it.
- GraphIt separates algorithm from schedule — the CGO'2018-lineage
  compiler emits push, pull, or hybrid variants from one text; its
  eval trees ship whole schedule files per graph (graphit_eval/).
- gunrock's advance operator (framework/operators/advance/) is the
  GPU version: load-balanced push with per-hop kernel selection.
- GDS's Pregel-style API runs push-only (message passing); the
  pull option reappears there as "compute over incoming messages" —
  the pattern survives the API translation.
- This repo: an mmap walker over immutable CSR segments wants pull
  for its hot phases — no atomics means segments can be walked by
  independent threads with zero coordination (the same reason pull
  needs no CAS).

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/bfs.cc` | reference direction-optimizing BFS: alpha/beta hysteresis (123-167), TDStep/BUStep (46-67) |
| ligra | `reference-repos-competitors/ligra-src/ligra/ligra.h` | engine-level sparse/dense edgeMap switch (236-282) |
| graphit | `reference-repos-corpus/graphit-src/include/graphit/frontend/schedule.h` | direction as a compiler schedule (DensePull default, 123-124) |
| gunrock | `reference-repos-corpus/gunrock-src/include/gunrock/framework/operators/advance/advance.hxx` | GPU advance operator — the push side, load-balanced |
| gbbs | `reference-repos-competitors/gbbs-src` | ligra's successor; same edgeMap contract at larger scale |

## 9. Cross-references

- Sibling patterns: `csr-adjacency-layout` (why both CSR and CSC must
  exist; the memory-behavior argument); `roaring-bitmap-idsets`
  (compressed frontier bitmaps for the dense side).
- Next in category: label-propagation connected components (frontier
  algorithms beyond BFS) and delta-stepping SSSP (bucketed frontiers).
- 202606 digest overlap: digests mentioned "push/pull duality" as a
  phrase; this pair adds the actual thresholds, the hysteresis
  constants, and the two topology regimes with numbers.

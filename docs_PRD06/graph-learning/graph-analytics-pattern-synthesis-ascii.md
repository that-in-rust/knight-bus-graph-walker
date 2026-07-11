# Graph Analytics Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `graph-analytics-pattern-synthesis-ascii.md` / `graph-analytics-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 7-12: what the graph-analytics category teaches as a whole — one layout, one duality, one algebra, and one recurring meta-move |

## 1. The category in one paragraph

Graph analytics is bulk computation over a frozen topology. Every
system in the corpus converges on the same stack: put the graph in
CSR (pattern 7), stream it with direction-optimized frontiers
(pattern 8) or bulk fixed-point sweeps (pattern 11), relax exact
ordering just enough to parallelize (patterns 10, 12), and — in one
lineage — express all of it as sparse linear algebra over semirings
(pattern 9). The dominant cost is memory traffic, not compute; every
pattern is ultimately a scheme for touching fewer bytes in a better
order.

## 2. The six patterns and their one-line lessons

```text
7  csr-adjacency-layout        the layout IS the performance model:
                               offsets+neighbors, sequential scans,
                               2x memory for both directions
8  frontier-pushpull-switching direction is a runtime decision:
                               push sparse frontiers, pull dense
                               ones, switch on cheap proxies
9  semiring-matrix-traversal   traversal is algebra: swap (ADD,MUL)
                               and one SpMV kernel becomes BFS,
                               SSSP, reachability, triangles
10 component-hooking-shortcutting
                               diameter-free WCC: hook trees along
                               edges, shortcut flat, 2-4 passes;
                               sample the giant component to skip
                               >90% of edges (Afforest)
11 pagerank-iteration-convergence
                               the fixed-point template: gather,
                               combine with constant, measure norm,
                               repeat — geometric convergence at
                               ratio = damping
12 delta-stepping-buckets      loose priority order: buckets of
                               width delta between Dijkstra and
                               Bellman-Ford; thread-local bins +
                               voting replace the priority queue
```

## 3. The category's three organizing axes

```text
AXIS 1 — ordering strictness (how much order does the algorithm
         actually need?)
  none:      PageRank, label propagation  -> bulk sweeps (11)
  monotone:  BFS levels                   -> frontier hops (8)
  loose:     SSSP                         -> delta buckets (12)
  emergent:  WCC                          -> racing hooks (10)
  The looser the requirement, the more parallelism is free.

AXIS 2 — frontier density (what fraction of vertices are active?)
  sparse -> push, queues, per-edge work counted in frontier size
  dense  -> pull, bitmaps, per-vertex early exit
  all    -> no frontier at all: full sweeps (11) or edge streams (10)

AXIS 3 — dialect (who owns the inner loop?)
  imperative: gapbs/ligra/gbbs — CAS, OpenMP, hand scheduling
  algebraic:  GraphBLAS/LAGraph/FalkorDB — semirings, masks,
              backend-owned scheduling
  compiled:   graphit — algorithm/schedule split, generates the
              imperative code from a declarative spec
  Every algorithm in this category exists in all three dialects,
  and they must agree — which is a free differential oracle.
```

## 4. The recurring meta-move

Stated once, observed four times:

```text
trade exactness for parallelism, bounded by a knob,
guarded by a cheap validity check, with hysteresis
against thrashing.

pattern 8:  exact frontier order    -> alpha/beta direction switch
            guard: visited bits      knob: alpha=15, beta=18
pattern 10: exact merge order       -> racing CAS hooks
            guard: high->low ties    knob: sampling rounds r=2
pattern 12: exact priority order    -> delta-width buckets
            guard: stale-entry skip  knob: delta
pattern 11: exact fixpoint          -> tolerance + iteration cap
            guard: L1/L2 norm        knob: tol, max_iter
```

This is the same amortization shape as the storage category's
compaction scoring — measure a cheap proxy, act only past a margin.

## 5. Worked roll-up: one graph, four costs

n = 100M vertices, m = 1B directed edges, avg degree 10 (in-RAM):

```text
storage (7):    out-CSR 4.8 GB + in-CSR 4.8 GB (pull needs it)
BFS (8):        ~1 x m edge touches with direction switching
                (~13x fewer than pure push at the dense hops)
WCC (10):       Afforest ~ 2n + eps probes ~ 250M ops, 3 passes
PageRank (11):  80 iterations x m = 80B contribution loads —
                the most expensive standard kernel by 50x
SSSP (12):      ~2-3 x m relaxations at a good delta; degenerates
                to 6000 serial rounds on road topology if
                mis-tuned
lesson: the SAME graph costs 250M..80B operations depending on the
QUESTION — cost lives in the algorithm's ordering demands, not in
the data size.
```

## 6. What the category exports to the other categories

```text
to graph-db     CSR segments as the read-optimized tier (Kuzu
                does this today); WCC/PageRank as the analytics
                procedures a graph DB must ship (GDS)
to vector-ann   the frontier discipline: HNSW search IS a best-
                first frontier walk over a proximity graph —
                pattern 8's machinery with a distance-ordered
                queue
to storage      immutability pays twice: frozen CSR enables both
                the streaming scans here and the snapshot/flip
                publication studied there
to verification three canonicalization lessons — partition
                equality (10), eps-tolerance + policy pinning
                (11), distances-not-paths (12): "equal" must be
                DEFINED per algorithm before differential testing
                can converge (docs_PRD06 thesis condition 3)
```

## 7. Honest gaps (not yet covered by pairs)

```text
- out-of-core execution (GridGraph/GraphChi edge grids) — motivated
  by pattern 11's cost model but not yet documented as a pair
- partitioning (edge-cut vs vertex-cut, METIS-family) — the
  distributed dimension is untouched
- community detection (Louvain/Leiden) and k-core peeling — gbbs's
  bucket structure (12) is half the k-core story already
- GNN sampling pipelines (PyG/DGL neighbor sampling) — the newest
  consumer of CSR + frontiers
these are candidates for later pairs or for the dataflow-compute
category where several fit more naturally.
```

## 8. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/` | reference kernels for patterns 8, 10, 11, 12 (bfs.cc, cc.cc, pr.cc, sssp.cc) + CSR (graph.h) |
| ligra | `reference-repos-competitors/ligra-src/` | frontier abstraction (edgeMap), byte-compressed CSR, label-prop CC |
| gbbs | `reference-repos-competitors/gbbs-src/` | bucket structure, ConnectIt CC family, theory-backed parallel variants |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/` | the algebraic dialect of patterns 9-12 |
| GraphBLAS | `reference-repos-corpus/GraphBLAS-src` | the semiring kernel engine under LAGraph and FalkorDB |
| graphit | `reference-repos-corpus/graphit-src` | the compiled dialect: algorithm/schedule separation |
| gunrock | `reference-repos-corpus/gunrock-src` | GPU forms: load-balanced advance, near-far buckets |
| falkordb | `reference-repos-competitors/falkordb-src/src/` | production graph DB built on the algebraic dialect |

## 9. Cross-references

- Storage synthesis: `storage-engine-pattern-synthesis-{ascii,mermaid}.md`
  — this category consumes its immutable-segment publication
  discipline and reuses its amortization meta-move.
- Individual pairs: patterns 7-12 in `pattern-index.md`.
- Next category per spec order: vector-ann — where the frontier
  walk meets distance metrics and recall/latency tradeoffs.

# Component Hooking Shortcutting — ASCII

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `component-hooking-shortcutting-ascii.md` / `component-hooking-shortcutting-mermaid.md` |
| One-line job | Find connected components without BFS: maintain a parent forest, HOOK trees together along edges, SHORTCUT trees flat — converging in a handful of passes regardless of graph diameter |

## 1. The job

Connected components (WCC) is the "hello world" of graph analytics
and the workhorse of entity resolution, deduplication, and community
seeding. BFS-per-component works but is diameter-bound and needs a
frontier per component. The parallel classic (Shiloach-Vishkin
lineage) replaces traversal with two alternating moves over a parent
array:

```text
state: comp[v] = current representative guess (init: comp[v] = v)
       -> defines a forest: v points toward its tree root

HOOK      for edge (u,v) with comp roots p1 != p2:
          point the HIGHER root at the LOWER one
          (deterministic tie-break prevents cycles)

SHORTCUT  comp[v] = comp[comp[v]]   (repeat until fixpoint)
          -> flattens every tree toward depth 1

repeat hook+shortcut passes until nothing changes.
```

Each pass at least halves tree heights: O(log n) passes worst case,
2-4 passes on real graphs — independent of diameter. A road network
with diameter 6000 takes BFS 6000 steps; hooking takes ~3 passes.

## 2. Raw data shape

```text
comp: |V| flat array of vertex ids   (u32/u64) — that's ALL the state
edges: streamed in any order — no frontier, no visited set, no queue

gapbs Link (cc.cc:41-55): lock-free hook —
  while (p1 != p2):
    high, low = order the two roots
    CAS(comp[high], high, low)      succeed -> hooked
    else reload and retry            (another thread hooked first)

gapbs Compress (cc.cc:59): pointer-jumping shortcut
LAGraph FastSV6 (LG_CC_FastSV6.c:101-135): the SAME two moves as
  algebra — hook: mngp = min(mngp, A*gp) over MIN_SECOND semiring;
  then parent = min(parent, C*mngp). Pattern 9 eating pattern 10.
```

## 3. Step-by-step: one hook+shortcut round

Graph: edges (0,1),(1,2),(3,4),(2,3). comp = [0,1,2,3,4]:

```text
hook pass (process edges, higher root -> lower):
  (0,1): roots 0,1 -> comp[1]=0        comp=[0,0,2,3,4]
  (1,2): roots 0,2 -> comp[2]=0        comp=[0,0,0,3,4]
  (3,4): roots 3,4 -> comp[4]=3        comp=[0,0,0,3,3]
  (2,3): roots 0,3 -> comp[3]=0        comp=[0,0,0,0,3]

shortcut pass: comp[4]=comp[comp[4]]=comp[3]=0
  comp=[0,0,0,0,0]   -> one component, TWO passes total
```

The CAS in Link makes this safe under full parallelism: losers of a
race reload the (now-deeper) roots and retry — the forest only ever
gets more connected, never cyclic, because hooks always point
high -> low.

## 4. The Afforest refinement (gapbs's default CC)

Real graphs have one giant component. Afforest (cc.cc:25,95-144)
exploits that:

```text
1. NEIGHBOR SAMPLING: hook along only the first r=2 neighbors of
   every vertex + compress. Cost: 2n edge probes, not m.
2. SAMPLE the comp array (1024 random entries) -> find the most
   frequent representative = the giant component's id
   (SampleFrequentElement, cc.cc:69).
3. FINISH: process the FULL edge lists of only the vertices NOT yet
   in the giant component.

effect: skips the giant component's internal edges entirely —
typically >90% of m never touched.
```

## 5. Worked example 1 — pass-count vs BFS on two topologies

```text
twitter-like: n=60M, m=1.5B, diameter ~16, giant component 99.9%
  BFS-based WCC:      16 frontier steps + per-component restarts
  hook+shortcut:      ~3 passes x m       = ~4.5B edge ops
  Afforest:           2n sample probes (120M) + finish on ~0.1% of
                      vertices ~ 150M ops  => ~30x less work

road-usa: n=24M, m=58M, diameter ~6000, thousands of components
  BFS-based WCC:      diameter-bound: ~6000 sequential steps —
                      parallelism starves on thin frontiers
  hook+shortcut:      ~4 passes x 58M     — diameter-independent
```

## 6. Worked example 2 — the same algorithm in three dialects

```text
imperative+atomics   gapbs Link/Compress: CAS loop, pointer jumping
algebraic            LAGraph FastSV6: hook = mxv over MIN_SECOND,
                     shortcut = parent extraction; runs on ANY
                     GraphBLAS backend (CPU, GPU, distributed)
menu-of-16           gbbs ConnectIt (benchmarks/Connectivity/:
                     UnionFind, ShiloachVishkin, LiuTarjan,
                     LabelPropagation...): a whole framework
                     benchmarking sampling x finish combinations —
                     evidence the pattern is a FAMILY, with
                     union-find and SV as its two poles
```

All three converge to identical component labels (up to
representative choice) — a canonicalization note the differential
harness must handle: compare PARTITIONS, not label values.

## 7. Where graph systems inherit this

- Neo4j GDS WCC: union-find with path compression — the sequential
  cousin; its `consecutiveIds` option is exactly the
  representative-canonicalization issue above.
- Spark GraphX/GraphFrames connectedComponents: hook+shortcut in
  BSP rounds (large-star/small-star) — the pattern survives
  distribution because each pass is embarrassingly parallel.
- Entity-resolution pipelines (Senzing-style, per the proprietary
  landscape doc) are incremental WCC over match edges.
- This repo: WCC over immutable CSR segments can run
  segment-parallel hooks with a final cross-segment merge — the
  comp array is the only shared state, and it's CAS-friendly.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/cc.cc` | Afforest reference: lock-free Link (41-55), Compress (59), sampling (69, 95-144) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LG_CC_FastSV6.c` | FastSV: hooking & shortcutting as MIN_SECOND algebra (101-135) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LG_CC_Boruvka.c` | Boruvka-style alternative, explicit Reduce_assign |
| gbbs | `reference-repos-competitors/gbbs-src/benchmarks/Connectivity` | ConnectIt: 16+ variants (UnionFind, ShiloachVishkin, LiuTarjan, LabelPropagation) |
| ligra | `reference-repos-competitors/ligra-src/apps/Components.C` | label-propagation CC over edgeMap (the frontier-based pole; Components-Shortcut.C adds shortcutting) |

## 9. Cross-references

- Sibling patterns: `semiring-matrix-traversal` (FastSV is its
  flagship application); `frontier-pushpull-switching` (the
  frontier-based CC pole ligra represents); `csr-adjacency-layout`
  (the edge stream both passes consume).
- Verification note (docs_PRD06 thesis): component labels are
  nondeterministic across implementations — equivalence must be
  defined as partition equality. This is the exact WCC
  canonicalization decision PRD05 recorded; three corpus dialects
  independently confirm it.
- 202606 digest overlap: digests mentioned union-find; this pair
  adds the hook/shortcut mechanics, Afforest's sampling shortcut,
  and the pass-count arithmetic.

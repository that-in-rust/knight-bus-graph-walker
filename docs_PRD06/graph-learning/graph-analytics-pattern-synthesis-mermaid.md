# Graph Analytics Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `graph-analytics-pattern-synthesis-ascii.md` / `graph-analytics-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 7-12: what the graph-analytics category teaches as a whole — one layout, one duality, one algebra, and one recurring meta-move |

## 1. The category stack

```mermaid
flowchart TD
    L["layout: CSR (pattern 7) —<br/>offsets+neighbors, streaming scans,<br/>the layout IS the performance model"]
    L --> EX["execution: direction-optimized frontiers (8),<br/>bulk fixed-point sweeps (11),<br/>relaxed-order parallelism (10, 12)"]
    EX --> AL["algebra: one lineage expresses ALL of it<br/>as SpMV over semirings (9) —<br/>GraphBLAS / LAGraph / FalkorDB"]
    AL --> COST["dominant cost: memory traffic, not compute —<br/>every pattern is a scheme for touching<br/>fewer bytes in a better order"]
```

## 2. Six patterns, one map

```mermaid
flowchart LR
    P7["7 CSR layout:<br/>the substrate"] --> P8["8 push/pull:<br/>direction is a<br/>runtime decision"]
    P7 --> P11["11 PageRank:<br/>the fixed-point<br/>template"]
    P7 --> P10["10 hook+shortcut:<br/>diameter-free WCC"]
    P8 --> P12["12 delta buckets:<br/>loose priority SSSP<br/>(BFS = delta 1)"]
    P9["9 semirings:<br/>traversal as algebra"] -.->|absorbs| P8
    P9 -.->|FastSV| P10
    P9 -.->|mxv/iter| P11
    P9 -.->|MIN_PLUS| P12
```

## 3. Axis 1 — ordering strictness

```mermaid
flowchart TD
    Q["how much order does the<br/>algorithm actually need?"]
    Q --> O1["none: PageRank, label prop<br/>-> bulk sweeps (11)"]
    Q --> O2["monotone: BFS levels<br/>-> frontier hops (8)"]
    Q --> O3["loose: SSSP<br/>-> delta buckets (12)"]
    Q --> O4["emergent: WCC<br/>-> racing hooks (10)"]
    O1 & O2 & O3 & O4 --> R["the looser the requirement,<br/>the more parallelism is free"]
```

## 4. Axis 2 — frontier density; Axis 3 — dialect

```mermaid
flowchart LR
    FD["frontier density"] --> S["sparse -> push, queues"]
    FD --> DE["dense -> pull, bitmaps, early exit"]
    FD --> ALL["all-active -> no frontier:<br/>sweeps (11) or edge streams (10)"]
```

```mermaid
flowchart TD
    DIA["who owns the inner loop?"]
    DIA --> IMP["imperative: gapbs / ligra / gbbs —<br/>CAS, OpenMP, hand scheduling"]
    DIA --> ALG["algebraic: GraphBLAS / LAGraph / FalkorDB —<br/>semirings, masks, backend scheduling"]
    DIA --> CMP["compiled: graphit —<br/>algorithm/schedule split"]
    IMP & ALG & CMP --> ORC["every algorithm exists in all three<br/>dialects and they must agree:<br/>a FREE differential oracle"]
```

## 5. The recurring meta-move

```mermaid
flowchart TD
    MM["trade exactness for parallelism,<br/>bounded by a knob, guarded by a cheap<br/>check, with hysteresis against thrashing"]
    MM --> M8["8: direction switch —<br/>knob alpha=15/beta=18, guard visited bits"]
    MM --> M10["10: racing CAS hooks —<br/>knob sampling r=2, guard high->low ties"]
    MM --> M12["12: delta buckets —<br/>knob delta, guard stale-entry skip"]
    MM --> M11["11: tolerance + cap —<br/>knob tol/max_iter, guard L1/L2 norm"]
    MM --> ST["same amortization shape as storage's<br/>compaction scoring: measure a cheap proxy,<br/>act only past a margin"]
```

## 6. One graph, four costs (n=100M, m=1B, deg 10)

```mermaid
flowchart TD
    G["CSR: 4.8 GB out + 4.8 GB in (pattern 7)"]
    G --> C1["BFS (8): ~1 x m touches<br/>with direction switching"]
    G --> C2["WCC (10): Afforest ~250M ops,<br/>3 passes — the cheapest kernel"]
    G --> C3["PageRank (11): 80 x m = 80B loads —<br/>most expensive standard kernel by ~50x"]
    G --> C4["SSSP (12): 2-3 x m at good delta;<br/>6000 serial rounds if mis-tuned on roads"]
    C1 & C2 & C3 & C4 --> LES["same graph, 250M..80B ops:<br/>cost lives in the algorithm's ordering<br/>demands, not the data size"]
```

## 7. Exports to the other categories

```mermaid
flowchart LR
    GA[graph-analytics] --> GDB["graph-db: CSR segments as the<br/>read-optimized tier (Kuzu);<br/>WCC/PageRank as GDS-style procedures"]
    GA --> VA["vector-ann: HNSW search IS a best-first<br/>frontier walk over a proximity graph —<br/>pattern 8 with a distance-ordered queue"]
    GA --> SE["storage: immutability pays twice —<br/>streaming scans here,<br/>snapshot/flip publication there"]
    GA --> VER["verification: partition equality (10),<br/>eps + policy pinning (11),<br/>distances-not-paths (12) —<br/>'equal' must be DEFINED per algorithm"]
```

## 8. The verification roll-up (docs_PRD06 thesis)

```mermaid
flowchart TD
    T3["thesis condition 3: the endpoint must be<br/>a point, not a cloud"] --> D1["WCC: labels differ legitimately —<br/>canonicalize to partitions"]
    T3 --> D2["PageRank: sink policy, norm, tolerance,<br/>float order — five divergence axes<br/>found in corpus source"]
    T3 --> D3["SSSP: tie-broken paths differ —<br/>compare distances"]
    D1 & D2 & D3 --> FREE["and condition-2 coverage comes cheap:<br/>three dialects (imperative/algebraic/compiled)<br/>of every algorithm must agree —<br/>run two, diff the outputs"]
```

## 9. Honest gaps

```mermaid
flowchart LR
    GAP[not yet documented] --> G1["out-of-core edge grids<br/>(GridGraph/GraphChi)"]
    GAP --> G2["partitioning: edge-cut vs vertex-cut,<br/>METIS family — the distributed dimension"]
    GAP --> G3["Louvain/Leiden, k-core peeling —<br/>gbbs buckets (12) are half the k-core story"]
    GAP --> G4["GNN sampling pipelines (PyG/DGL) —<br/>newest consumer of CSR + frontiers"]
```

## 9b. How a query would flow through the integrated stack

```mermaid
sequenceDiagram
    participant U as user / procedure call
    participant P as planner
    participant C as CSR segments (7)
    participant K as kernel (8/10/11/12)
    U->>P: gds.wcc / gds.pageRank / shortestPath
    P->>P: pick dialect: imperative kernel or<br/>algebraic expression (9)
    P->>C: pin a snapshot (storage category's flip)
    P->>K: run — direction switches (8), hooks race (10),<br/>sweeps iterate (11), buckets drain (12)
    K-->>P: raw labels / scores / distances
    P->>P: canonicalize (partitions, eps, distances)
    P-->>U: rows — identical across dialects,<br/>which the harness exploits
    Note over C,K: the snapshot pin is the only<br/>storage/analytics contract: topology is<br/>frozen for the whole kernel run
```

## 10. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/` | reference kernels: bfs.cc, cc.cc, pr.cc, sssp.cc, graph.h |
| ligra | `reference-repos-competitors/ligra-src/` | frontier abstraction, compressed CSR, label-prop CC |
| gbbs | `reference-repos-competitors/gbbs-src/` | buckets, ConnectIt, theory-backed variants |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/` | algebraic dialect of patterns 9-12 |
| GraphBLAS | `reference-repos-corpus/GraphBLAS-src` | semiring kernel engine |
| graphit | `reference-repos-corpus/graphit-src` | compiled dialect: algorithm/schedule split |
| gunrock | `reference-repos-corpus/gunrock-src` | GPU forms: load-balanced advance, near-far |
| falkordb | `reference-repos-competitors/falkordb-src/src/` | production DB on the algebraic dialect |

## 11. Cross-references

- Storage synthesis: `storage-engine-pattern-synthesis-{ascii,mermaid}.md`
  — supplier of the immutable-segment discipline and the
  amortization meta-move this category reuses.
- Individual pairs: patterns 7-12 in `pattern-index.md`.
- Next category per spec order: vector-ann — where the frontier walk
  meets distance metrics and recall/latency Pareto frontiers.

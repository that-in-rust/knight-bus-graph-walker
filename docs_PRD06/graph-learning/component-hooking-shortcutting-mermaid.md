# Component Hooking Shortcutting — Mermaid

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `component-hooking-shortcutting-ascii.md` / `component-hooking-shortcutting-mermaid.md` |
| One-line job | Find connected components without BFS: hook parent trees together along edges, shortcut them flat — converging in a handful of passes regardless of graph diameter |

## 1. The two moves

```mermaid
flowchart TD
    ST["state: comp[v] = representative guess<br/>(init comp[v] = v) — a parent forest"]
    ST --> HK["HOOK: for edge (u,v) with roots p1 != p2,<br/>point the HIGHER root at the LOWER<br/>(deterministic tie-break -> no cycles)"]
    ST --> SC["SHORTCUT: comp[v] = comp[comp[v]]<br/>until fixpoint — flatten trees to depth 1"]
    HK --> RPT["repeat until no change:<br/>O(log n) passes worst case,<br/>2-4 on real graphs — DIAMETER-INDEPENDENT"]
    SC --> RPT
    RPT --> EX["road network, diameter 6000:<br/>BFS needs 6000 steps; hooking ~3 passes"]
```

## 2. The lock-free hook (gapbs Link, cc.cc:41-55)

```mermaid
sequenceDiagram
    participant T as thread
    participant C as comp array
    T->>C: read p1 = comp[u], p2 = comp[v]
    loop while p1 != p2
        T->>T: high, low = order roots
        T->>C: CAS(comp[high], high, low)
        alt CAS won (or already low)
            T-->>T: hooked — done
        else lost the race
            T->>C: reload p1 = comp[comp[high]], p2 = comp[low]
        end
    end
    Note over C: forest only ever gets MORE connected,<br/>never cyclic: hooks always point high -> low
```

## 3. Trace: edges (0,1),(1,2),(3,4),(2,3)

```mermaid
flowchart LR
    A["comp=[0,1,2,3,4]"] -- "(0,1): comp[1]=0" --> B["[0,0,2,3,4]"]
    B -- "(1,2): comp[2]=0" --> C["[0,0,0,3,4]"]
    C -- "(3,4): comp[4]=3" --> D["[0,0,0,3,3]"]
    D -- "(2,3): comp[3]=0" --> E["[0,0,0,0,3]"]
    E -- "shortcut: comp[4]=comp[3]=0" --> F["[0,0,0,0,0]<br/>one component,<br/>2 passes total"]
```

## 4. Afforest — exploiting the giant component (gapbs default)

```mermaid
flowchart TD
    S1["1. neighbor sampling: hook along only the<br/>first r=2 neighbors per vertex + compress —<br/>cost 2n probes, not m"]
    S1 --> S2["2. sample comp (1024 entries) -> most frequent<br/>representative = giant component id<br/>(SampleFrequentElement, cc.cc:69)"]
    S2 --> S3["3. finish: full edge lists ONLY for vertices<br/>not yet in the giant component"]
    S3 --> FX["effect: >90% of m (the giant component's<br/>internal edges) never touched"]
```

## 5. Worked example — two topologies

```mermaid
flowchart TD
    TW["twitter-like: n=60M, m=1.5B,<br/>diameter 16, giant 99.9%"] --> TWR["hook+shortcut: ~3 x m = 4.5B ops<br/>Afforest: 120M probes + ~150M finish<br/>=> ~30x less work"]
    RD["road-usa: n=24M, m=58M,<br/>diameter ~6000, many components"] --> RDR["BFS WCC: ~6000 sequential steps,<br/>parallelism starves on thin frontiers;<br/>hook+shortcut: ~4 x 58M, diameter-free"]
```

## 6. One algorithm, three dialects

```mermaid
flowchart LR
    P[hook + shortcut] --> IMP["imperative + atomics:<br/>gapbs Link/Compress —<br/>CAS loop, pointer jumping"]
    P --> ALG["algebraic: LAGraph FastSV6 —<br/>hook = mxv over MIN_SECOND semiring<br/>(LG_CC_FastSV6.c:101-135);<br/>runs on any GraphBLAS backend"]
    P --> FAM["framework: gbbs ConnectIt —<br/>16+ variants (UnionFind, ShiloachVishkin,<br/>LiuTarjan, LabelPropagation) benchmarking<br/>sampling x finish combinations"]
    IMP & ALG & FAM --> EQ["all converge to the same PARTITION —<br/>but different label values: the harness<br/>must compare partitions, not labels"]
```

## 7. Inheritance map

```mermaid
flowchart LR
    CC[pattern] --> GDS["Neo4j GDS WCC: union-find +<br/>path compression; consecutiveIds option<br/>= the canonicalization issue made API"]
    CC --> SPK["Spark GraphX connectedComponents:<br/>large-star/small-star in BSP rounds —<br/>each pass embarrassingly parallel,<br/>so the pattern survives distribution"]
    CC --> ER["entity resolution (Senzing-style):<br/>incremental WCC over match edges"]
    CC --> KB["this repo: segment-parallel hooks over<br/>immutable CSR segments + cross-segment<br/>merge — comp array is the only shared<br/>state, and it's CAS-friendly"]
```

## 8. The verification angle

```mermaid
flowchart TD
    ND["component labels are nondeterministic<br/>across implementations and thread schedules"] --> DEF["equivalence must be DEFINED:<br/>partition equality — same groups,<br/>any representative"]
    DEF --> PRD["exactly the WCC canonicalization decision<br/>PRD05 recorded for the differential harness;<br/>three corpus dialects independently confirm<br/>the need"]
    PRD --> HOW["cheap canonical form: relabel every<br/>component by its minimum member id,<br/>then diff the arrays"]
```

## 8b. FastSV's algebraic hook, unpacked

The most surprising cross-pattern artifact in the corpus: the CAS
loop of section 2 rewritten as two matrix products
(LG_CC_FastSV6.c:101-135).

```mermaid
flowchart TD
    H1["step 1 — gather each vertex's best<br/>neighbor-root: mngp = min(mngp, A*gp)<br/>over the MIN_SECOND semiring<br/>(pattern 9's machinery)"]
    H1 --> H2["step 2 — hook it onto the parent:<br/>parent = min(parent, C*mngp), where<br/>C(i,j) present iff i = Px[j] —<br/>a structure-only matrix built in O(1)<br/>via SuiteSparse pack/unpack move constructors"]
    H2 --> H3["step 3 — shortcut: gp = parent[parent],<br/>plain extraction"]
    H3 --> WOW["no atomics anywhere: the MIN monoid's<br/>associativity absorbs all races the CAS<br/>loop had to handle by hand — the semiring<br/>IS the concurrency control"]
```

```mermaid
sequenceDiagram
    participant SV as FastSV pass
    participant GB as GraphBLAS
    SV->>GB: mngp = min.2nd(A, gp)
    SV->>GB: parent = min.2nd(C, mngp)
    SV->>GB: gp' = parent[parent] (shortcut)
    GB-->>SV: converged when parent stops changing
    Note over SV: same 2-4 pass behavior as the<br/>imperative dialect — the algebra changes<br/>WHO handles races, not how many passes
```

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/cc.cc` | Afforest reference: Link (41-55), Compress (59), sampling (69, 95-144) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LG_CC_FastSV6.c` | FastSV as MIN_SECOND algebra (101-135) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LG_CC_Boruvka.c` | Boruvka-style alternative |
| gbbs | `reference-repos-competitors/gbbs-src/benchmarks/Connectivity` | ConnectIt variant framework |
| ligra | `reference-repos-competitors/ligra-src/apps/Components.C` | label-propagation pole (Components-Shortcut.C adds shortcutting) |

## 10. Cross-references

- Sibling patterns: `semiring-matrix-traversal` (FastSV is its
  flagship application); `frontier-pushpull-switching` (the
  label-propagation pole); `csr-adjacency-layout` (the edge stream).
- Next in category: PageRank iteration structure; delta-stepping
  SSSP (bucketed frontiers — the priority-ordered cousin of the
  unordered edge stream this pattern consumes).
- Storage kinship: Afforest's sample-then-finish is the same
  amortization shape as `bloom-filter-shortcut` — spend a tiny
  probabilistic pre-pass (samples / filter bits) to skip the bulk of
  the expensive work (giant-component edges / disk reads).
- Paper trail: Shiloach-Vishkin (1982), Afforest (Sutton et al.),
  FastSV (Zhang et al.), ConnectIt (Dhulipala et al.) — see
  `research-papers-ledger.md` for verified entries.
- 202606 digest overlap: digests mentioned union-find; this pair
  adds hook/shortcut mechanics, Afforest sampling, and pass-count
  arithmetic.

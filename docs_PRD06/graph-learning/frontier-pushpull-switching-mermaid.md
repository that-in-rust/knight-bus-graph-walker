# Frontier PushPull Switching — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `frontier-pushpull-switching-ascii.md` / `frontier-pushpull-switching-mermaid.md` |
| One-line job | Run each traversal step in whichever direction touches fewer edges — push from a small frontier, pull into a huge one — switching at a measured threshold |

## 1. The two directions

```mermaid
flowchart LR
    subgraph PUSH [push — top-down]
        P1["for u in frontier:<br/>for v in out(u): try claim v"] --> P2["cost ~ frontier out-degrees;<br/>needs atomic CAS on parent[v]"]
    end
    subgraph PULL [pull — bottom-up]
        Q1["for v in ALL unvisited:<br/>for u in in(v):<br/>if u in frontier: claim v; break"] --> Q2["cost ~ unvisited in-degrees<br/>with EARLY EXIT (~1-2 probes);<br/>no atomics — v writes only its own slot"]
    end
    PUSH --> W["push wins: small frontier"]
    PULL --> W2["pull wins: huge frontier —<br/>social/web BFS peaks >50% of graph"]
```

## 2. Machinery per direction

```mermaid
flowchart TD
    PU["push needs"] --> A["out-CSR + frontier as queue/sparse list + CAS"]
    PL["pull needs"] --> B["in-CSR (CSC) + frontier as BITMAP<br/>(O(1) membership) + no atomics"]
    A & B --> CV["conversions at every switch:<br/>queue -> bitmap: scatter bits<br/>bitmap -> queue: scan + prefix-sum compact"]
    CV --> REF["gapbs bfs.cc carries both and converts;<br/>ligra hides the duality in edgeMapData's<br/>sparse/dense split (ligra.h:236-282)"]
```

## 3. The switching rules

```mermaid
flowchart TD
    S["per-hop decision"] --> G["gapbs (bfs.cc:123-167):<br/>PUSH->PULL when scout_count > edges_to_check / alpha (15)<br/>PULL->PUSH when awake_count < num_nodes / beta (18)<br/>asymmetric hysteresis stops oscillation"]
    S --> L["ligra (ligra.h:238-261), for ANY edgeMap:<br/>threshold = numEdges/20;<br/>|frontier| + sum(outDegrees) > threshold -> DENSE (pull)<br/>else SPARSE (push)"]
    S --> GI["graphit: direction is a COMPILER SCHEDULE —<br/>DensePull is the named default<br/>(frontend/schedule.h:123-124);<br/>one algorithm text, generated loops"]
```

## 4. Direction-optimized BFS, hop by hop

Social graph, n = 100M:

```mermaid
flowchart LR
    H1["hop 1<br/>|F|=1<br/>PUSH"] --> H2["hop 2<br/>|F|~300<br/>PUSH"] --> H3["hop 3<br/>|F|~2M<br/>PULL (scout blew<br/>threshold)"] --> H4["hop 4<br/>|F|~40M peak<br/>PULL"] --> H5["hop 5<br/>|F|~1M<br/>PULL"] --> H6["hop 6<br/>|F|~10k<br/>PUSH (frontier<br/>collapsed)"] --> H7["hop 7<br/>|F|~50<br/>PUSH"]
```

## 5. Worked example — the arithmetic of one middle hop

n = 100M, m = 2B (avg deg 20); frontier 40M, unvisited 30M:

```mermaid
flowchart TD
    PC["PUSH: 40M x 20 = 800M edge traversals,<br/>each an atomic CAS attempt"]
    LC["PULL: 30M x ~2 early-exit probes = 60M,<br/>no atomics, sequential CSC scans"]
    PC & LC --> R["~13x fewer edges touched, and the traffic is<br/>pattern 7's streaming kind — Beamer's measured<br/>3-8x whole-search speedup lives in these hops"]
```

## 6. Worked example — when switching is a loss

```mermaid
flowchart TD
    RN["road network: n=20M, m=50M,<br/>avg deg 2.5, diameter ~6000"] --> TH["frontier never exceeds ~0.1% of vertices —<br/>thin wavefront never crosses edges/alpha"]
    TH --> PP["pure PUSH throughout; the pull machinery<br/>(+200 MB CSC, bitmap conversions) is dead weight"]
    PP --> LE["lesson: direction switching is a power-law-graph<br/>optimization — hence exposed knobs<br/>(gapbs alpha/beta, graphit schedules):<br/>no constant fits both topologies"]
```

## 6b. One full hop as a sequence

```mermaid
sequenceDiagram
    participant E as engine
    participant F as frontier
    participant CSR as out-CSR
    participant CSC as in-CSC
    E->>F: measure scout_count = sum out-degrees of frontier
    alt scout_count > edges_to_check / alpha
        E->>F: convert queue -> bitmap (scatter bits)
        loop each unvisited v (parallel, no atomics)
            E->>CSC: probe in(v) until a frontier parent found
            E->>E: parent[v] = u; awake_count++
        end
        Note over E: stay PULL while awake_count > n / beta
    else small frontier
        loop each u in frontier (parallel)
            E->>CSR: scan out(u)
            E->>E: CAS parent[v]: winner enqueues v
        end
    end
    E->>F: swap frontiers, next hop
```

The conversion costs (scatter / prefix-sum compact) are why the
hysteresis constants are asymmetric: switching itself isn't free, so
the engine demands a margin before flipping direction — the same
argument as any amortized-cost switch in the storage category
(compare tiered compaction's trigger margins in
`lsm-compaction-tradeoff`).

## 7. Inheritance map

```mermaid
flowchart LR
    SW[push/pull switch] --> LG["ligra: THE engine primitive —<br/>every algorithm is edgeMap calls,<br/>inherits switching free"]
    SW --> GT["graphit: algorithm/schedule separation;<br/>eval trees ship schedule files per graph"]
    SW --> GR["gunrock: GPU advance operator —<br/>load-balanced push, per-hop kernel choice"]
    SW --> GD["GDS Pregel API: push-only messaging;<br/>pull reappears as 'compute over<br/>incoming messages'"]
    SW --> KB["this repo: pull for mmap-walk hot phases —<br/>no atomics means independent threads<br/>walk immutable segments with<br/>zero coordination"]
```

## 7b. Beyond BFS — the same switch in other algorithms

```mermaid
flowchart TD
    ALG["frontier algorithm"] --> BFS2["BFS: parent claiming —<br/>the canonical case"]
    ALG --> CC["connected components (label prop):<br/>push = spread my label out;<br/>pull = adopt min label of in-neighbors —<br/>pull variant is atomics-free here too"]
    ALG --> PR["PageRank: pull IS the natural form<br/>(gather in-ranks); push variant exists<br/>for delta-based incremental updates"]
    ALG --> SSSP["delta-stepping SSSP: buckets of<br/>frontiers by tentative distance —<br/>each bucket processed push-style,<br/>graphit ships it as a schedule example"]
    BFS2 & CC & PR & SSSP --> UNI["ligra's insight: expose ONE edgeMap<br/>with the switch inside, and every<br/>algorithm above inherits it"]
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/bfs.cc` | alpha/beta hysteresis (123-167), TDStep/BUStep (46-67) |
| ligra | `reference-repos-competitors/ligra-src/ligra/ligra.h` | engine-level sparse/dense edgeMap switch (236-282) |
| graphit | `reference-repos-corpus/graphit-src/include/graphit/frontend/schedule.h` | direction as compiler schedule (123-124) |
| gunrock | `reference-repos-corpus/gunrock-src/include/gunrock/framework/operators/advance/advance.hxx` | GPU advance operator |
| gbbs | `reference-repos-competitors/gbbs-src` | ligra's successor, same edgeMap contract |

## 9. Cross-references

- Sibling patterns: `csr-adjacency-layout` (why CSR + CSC coexist);
  `roaring-bitmap-idsets` (compressed dense-side frontiers).
- Next in category: label-propagation components; delta-stepping SSSP
  (bucketed frontiers).
- Storage kinship: the switch's amortization argument mirrors
  compaction triggers (`lsm-compaction-tradeoff` §scoring) — measure
  cheap proxies (scout_count / level sizes), flip strategy only past
  a margin, keep hysteresis so the system doesn't thrash between the
  two regimes.
- For differential verification (docs_PRD06 thesis): push and pull
  MUST produce identical results — a free self-check every
  direction-switching engine ships implicitly. Running both
  directions and diffing outputs is the cheapest oracle in this
  category.
- 202606 digest overlap: digests named the duality; this pair adds
  thresholds, hysteresis constants, and both topology regimes.

# PageRank Iteration Convergence — Mermaid

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `pagerank-iteration-convergence-ascii.md` / `pagerank-iteration-convergence-mermaid.md` |
| One-line job | Iterate rank = teleport + damping x (pulled in-neighbor contributions) until an error norm drops below tolerance — the template every fixed-point graph algorithm copies |

## 1. The power iteration

```mermaid
flowchart TD
    I["init: rank[v] = 1/n"] --> P["precompute contrib[u] = rank[u]/out_deg(u)<br/>(gapbs pr.cc:39-42 — makes the inner<br/>loop a pure sum of loads)"]
    P --> G["gather (pull over in-CSR):<br/>rank'[v] = (1-d)/n + d * SUM contrib[u]<br/>for u -> v"]
    G --> E["error = SUM |rank'[v] - rank[v]| (L1)"]
    E --> C{"error < tol?"}
    C -- no --> P
    C -- yes --> D["done — typically 60-100 iterations<br/>at d=0.85, tol 1e-6"]
```

The skeleton (init, gather, combine with constant, measure change,
repeat) is reused by label propagation, Katz, HITS, belief
propagation, and GNN message passing.

## 2. Why pull, why no atomics

```mermaid
flowchart LR
    PULL["pull form (pattern 8's dense side):<br/>each u writes ONLY scores[u]"] --> NA["no atomics; error reduction<br/>rides the same parallel loop<br/>(pr.cc:44-53, reduction(+))"]
    PUSH["push form"] --> DELTA["reserved for incremental variants:<br/>propagate only vertices whose rank<br/>moved > eps (graphit's PageRankDelta) —<br/>cuts late-iteration work 10-100x"]
```

## 3. The dangling-node schism

Sinks (out_deg = 0) leak probability. Three corpus policies:

```mermaid
flowchart TD
    SK["sinks"] --> GAP["gapbs / GAP spec: IGNORE —<br/>sum(rank) < 1; LAGr_PageRankGAP.c:25-29<br/>admits it outright"]
    SK --> LAG["LAGraph LAGr_PageRank.c: gather sink<br/>ranks each iteration (sink/rsink, :109-113),<br/>redistribute uniformly — sum(rank) = 1"]
    SK --> NK["networkit: CONFIGURABLE —<br/>SinkHandling::DISTRIBUTE_SINKS, optional<br/>normalization, L1 or L2 stopping norm<br/>(PageRank.cpp:16-59)"]
    GAP & LAG & NK --> OR["'PageRank' is not one function:<br/>an oracle must pin sink policy, norm,<br/>tolerance, cap, and float precision"]
```

## 4. Three iterations traced

Graph A->B, A->C, B->C, C->A, D->C; n=4, d=0.85, base=0.0375:

```mermaid
sequenceDiagram
    participant R as rank
    participant K as contrib
    Note over R: init rank = [.25, .25, .25, .25]
    R->>K: contrib = rank/outdeg = [.125, .25, .25, .25]
    K->>R: iter 1: A=.25, B=.14375, C=.56875, D=.0375 (error .638)
    R->>K: contrib = [.125, .14375, .56875, .0375]
    K->>R: iter 2: A=.521, B=.144, C=.298, D=.0375 (error .543)
    Note over R: error shrinks geometrically with ratio ~d:<br/>iters to 1e-6 ~ log(1e-6/.6)/log(.85) ~ 82
```

d = 0.85 is universal because it balances fidelity to link structure
(bigger d) against convergence speed (smaller d).

## 5. Worked example — cost at a billion edges

```mermaid
flowchart TD
    N["n=100M, m=1B, pull over in-CSR,<br/>80 iterations"] --> PI["per iteration: 1B contribution loads<br/>(streaming, pattern 7) + 200M flops —<br/>memory-bound"]
    PI --> IO["80 full graph passes = 80 x 4.8 GB:<br/>THE access pattern that spawned<br/>out-of-core systems (GridGraph, GraphChi)"]
    IO --> DL["delta variants exit early: rank mass<br/>concentrates fast, so late iterations<br/>touch few vertices"]
```

## 6. Inheritance map

```mermaid
flowchart LR
    PR[power-iteration template] --> GDS["Neo4j GDS pageRank: damping/tolerance/<br/>maxIterations knobs = exactly the<br/>section-3 divergence axes"]
    PR --> GB["LAGraph: one mxv per iteration (pattern 9);<br/>contrib precompute = diagonal scale,<br/>sinks = a reduce"]
    PR --> DD["differential-dataflow / feldera: PageRank as<br/>THE canonical recursive incremental query"]
    PR --> GNN["PyG / DGL: message passing = the gather<br/>loop with learned combine functions"]
    PR --> KB["this repo: segment-parallel per iteration;<br/>the error reduction is the only<br/>cross-segment sync point"]
```

## 7. The verification angle

```mermaid
flowchart TD
    FP["floating-point poster child:<br/>same algorithm, different summation<br/>orders -> legitimately different<br/>low-order bits"] --> EQ["equivalence must be DEFINED:<br/>|a-b| < eps rankwise, or<br/>rank-order match for top-k"]
    EQ --> AX["plus the policy axes: sink handling,<br/>norm choice, tolerance, iteration cap"]
    AX --> TH["docs_PRD06 thesis condition 3 in the wild:<br/>the endpoint is a cloud, not a point,<br/>until a human pins the definition"]
```

## 7b. The template instantiated — four algorithms, one skeleton

```mermaid
flowchart TD
    SKEL["init x / gather from neighbors /<br/>combine with constant / measure delta / repeat"]
    SKEL --> A1["PageRank: gather = sum contrib,<br/>combine = teleport + d*sum"]
    SKEL --> A2["label propagation: gather = mode of<br/>neighbor labels, combine = adopt;<br/>delta = labels changed count"]
    SKEL --> A3["Katz centrality: gather = sum scores,<br/>combine = alpha*sum + beta;<br/>converges iff alpha < 1/lambda_max"]
    SKEL --> A4["GNN layer: gather = message fn,<br/>combine = learned MLP + activation;<br/>'iterations' become layers"]
```

```mermaid
sequenceDiagram
    participant E as engine
    participant S as scores (double-buffered)
    participant G as in-CSR
    loop until error < tol or iter == cap
        E->>S: precompute contrib = score/outdeg
        par each vertex u (no atomics)
            E->>G: stream in(u), sum contribs
            E->>S: scores'[u] = base + d * sum
        end
        E->>E: L1 error via parallel reduction
    end
    Note over E: the whole family differs only in the<br/>three plugged functions — which is why<br/>engines (graphit, GDS, GraphBLAS) expose<br/>the skeleton, not the algorithms
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/pr.cc` | pull PageRank: contrib precompute (39-42), fused error reduction (44-53), kDamp (31) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_PageRankGAP.c` | GAP variant, sinks ignored (25-29), damping prescale (99-114) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_PageRank.c` | sink-correct variant (109-113) |
| networkit | `reference-repos-competitors/networkit-src/networkit/cpp/centrality/PageRank.cpp` | configurable sinks/norms (16-59) |
| graphit | `reference-repos-corpus/graphit-src` | PageRank + PageRankDelta as schedulable algorithms |

## 9. Cross-references

- Sibling patterns: `frontier-pushpull-switching` (pull direction;
  delta variants re-frontier); `semiring-matrix-traversal` (mxv per
  iteration); `csr-adjacency-layout` (the streaming access pattern).
- Next in category: out-of-core edge-grid execution (the systems the
  section-5 cost model motivates), delta-stepping SSSP, or the
  graph-analytics category synthesis pair rolling up patterns 7-11.
- Storage kinship: the double-buffered scores array is the same
  publish-atomically instinct as the storage category's root flips —
  readers of iteration k never see a half-written k+1; gapbs gets
  away with in-place updates only because power iteration tolerates
  (and Gauss-Seidel-style even accelerates on) stale-mixed reads,
  a freedom transactional systems never have.
- Paper trail: Brin & Page (1998), the GAP benchmark specification
  (which LAGr_PageRankGAP names in its header), and the
  incremental-PageRank line in differential dataflow — see
  `research-papers-ledger.md` for verified entries.
- 202606 digest overlap: digests listed PageRank among GDS
  algorithms; this pair adds iteration mechanics, the sink schism
  with file evidence, and convergence arithmetic.

# Delta Stepping Buckets — Mermaid

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `delta-stepping-buckets-ascii.md` / `delta-stepping-buckets-mermaid.md` |
| One-line job | Parallel shortest paths by relaxing Dijkstra's strict priority order into buckets of width delta — enough order for correctness pruning, enough slack for parallelism |

## 1. The dial between two poles

```mermaid
flowchart LR
    DJ["Dijkstra: exact distance order,<br/>work-optimal, but the priority queue<br/>is a serialization point"]
    BF["Bellman-Ford: relax ALL edges<br/>each round — fully parallel,<br/>O(n*m) redundant work"]
    DS["delta-stepping:<br/>buckets of width delta,<br/>ordered ACROSS buckets,<br/>parallel WITHIN a bucket"]
    DJ -- "delta -> 0" --- DS
    DS -- "delta -> inf" --- BF
```

```mermaid
flowchart TD
    B["bucket[i] = vertices with dist in [i*delta, (i+1)*delta)"]
    B --> L["take smallest non-empty bucket,<br/>relax its vertices in parallel<br/>(repeat: light edges may re-add to it)"]
    L --> M["improved vertices land in<br/>bucket new_dist/delta"]
    M --> N["advance to next non-empty bucket"]
    N --> L
```

## 2. The relax kernel (gapbs RelaxEdges, sssp.cc:70-86)

```mermaid
sequenceDiagram
    participant T as thread
    participant D as dist array
    participant LB as local_bins
    T->>D: new_dist = dist[u] + w(u,v)
    loop while new_dist < dist[v]
        T->>D: CAS(dist[v], old, new_dist)
        alt CAS won
            T->>LB: local_bins[new_dist/delta].push(v)
        else lost
            T->>D: reload old, recheck
        end
    end
    Note over T: same CAS-retry idiom as pattern 10's Link —<br/>the category keeps re-deriving<br/>"optimistic update + retry"
```

## 3. Three engineering moves (gapbs sssp.cc:32-53)

```mermaid
flowchart TD
    E1["THREAD-LOCAL BINS: no shared priority<br/>queue exists at all — threads vote for the<br/>smallest non-empty bucket, then copy just<br/>that bucket into a shared frontier"]
    E2["LAZY DELETION: re-added vertices are not<br/>removed from old buckets; the guard<br/>dist[u] >= delta*curr_bin skips stale entries<br/>(sssp.cc:110) — tombstones, in analytics form"]
    E3["BUCKET FUSION: if a thread's next local<br/>bucket has the same priority, run it in the<br/>same round without global sync (GraphIt<br/>CGO'20, cited sssp.cc:29,46) — kills the<br/>synchronization tail on high-diameter graphs"]
    E1 --> E2 --> E3
```

## 4. Trace: delta=3, edges s->a(2), s->b(5), a->b(1), a->c(4), b->c(1)

```mermaid
sequenceDiagram
    participant B0 as bucket 0 [0,3)
    participant B1 as bucket 1 [3,6)
    participant B2 as bucket 2 [6,9)
    Note over B0: process s(0): a=2 -> B0, b=5 -> B1
    Note over B0: re-run B0: process a(2):<br/>b: 3<5 -> B1; c=6 -> B2
    Note over B1: process b(3): c: 4<6 -> B1 (re-run)
    Note over B1: process c(4): no out-edges
    Note over B2: stale c-entry (dist 6): guard skips it<br/>since dist[c]=4 < 2*delta
    Note over B0,B2: final dist = s:0 a:2 b:3 c:4;<br/>b relaxed twice — the price of parallelism
```

## 5. Choosing delta

```mermaid
flowchart TD
    D["delta ~ avg_weight x (m/n) rule of thumb;<br/>gapbs leaves it a per-graph CLI flag (-d)<br/>because no single value wins (sssp.cc:28)"]
    D --> SG["social graph (weights 1-100, deg 20):<br/>delta=50 -> tens of rounds with huge<br/>parallel frontiers — near-BFS behavior"]
    D --> RN["road network (diameter ~6000):<br/>small delta -> thousands of near-empty rounds;<br/>large delta -> re-relaxation explosion;<br/>bucket fusion worth 2-5x exactly here"]
    D --> UW["unit weights: delta=1 IS BFS —<br/>pattern 8's frontier as the special case<br/>where buckets never re-process"]
```

## 6. Dialects and inheritance

```mermaid
flowchart LR
    P[delta-stepping] --> LA["LAGraph: algebraic form — MIN_PLUS<br/>semiring does the relax, masks split<br/>light/heavy, Delta as GrB_Scalar<br/>(LAGr_SingleSourceShortestPath.c:25,39,85);<br/>'pick Delta automatically' is still FUTURE"]
    P --> GB2["gbbs: the bucket structure generalized —<br/>gbbs/bucket.h + make_vertex_buckets<br/>(DeltaStepping.h:27,101) also drives<br/>k-core peeling and weighted BFS"]
    P --> GDS["Neo4j GDS: allShortestPaths.delta —<br/>the knob is in the public API"]
    P --> GPU["gunrock: near-far buckets — the 2-bucket<br/>minimal form; fewer, larger launches<br/>amortize better on GPUs"]
```

## 7. The verification angle

```mermaid
flowchart TD
    ND["parallel relaxation order is<br/>nondeterministic; equal-length paths<br/>may resolve differently"] --> EQ["compare DISTANCES (deterministic given<br/>exact weights), not paths — or define<br/>explicit path tie-breaks"]
    EQ --> FP["float weights add summation-order noise:<br/>same eps-tolerance discipline as<br/>pattern 11's PageRank scores"]
    FP --> OR["a Dijkstra oracle validates any<br/>delta-stepping run: different algorithm,<br/>same answer — cheap differential check"]
```

## 8. The category's recurring meta-move

```mermaid
flowchart LR
    MM["trade strictness for parallelism,<br/>bounded by a tunable knob,<br/>guarded by a cheap validity check"]
    MM --> X1["pattern 8: exact frontier order -> alpha/beta<br/>direction switch, guarded by visited bits"]
    MM --> X2["pattern 10: exact merge order -> racing CAS<br/>hooks, guarded by high->low tie-break"]
    MM --> X3["this pattern: exact priority order -> delta<br/>buckets, guarded by the stale-entry check"]
```

## 8b. Round structure end-to-end (gapbs DeltaStep, sssp.cc:87+)

```mermaid
sequenceDiagram
    participant SH as shared frontier
    participant T1 as thread 1
    participant T2 as thread 2
    Note over SH: frontier = [source], bin 0
    par round k
        T1->>SH: take slice of shared bin (dynamic, 64)
        T1->>T1: RelaxEdges into MY local_bins
        T2->>SH: take slice
        T2->>T2: RelaxEdges into MY local_bins
    end
    T1->>SH: vote: my smallest non-empty local bin
    T2->>SH: vote: my smallest non-empty local bin
    Note over SH: next shared bin = min of votes<br/>(double-buffered shared_indexes[2],<br/>frontier_tails[2] — sssp.cc:92-95)
    T1->>SH: copy my selected local bin into shared frontier
    T2->>SH: copy my selected local bin into shared frontier
    Note over SH: repeat until every vote is kMaxBin (empty)
```

The only shared mutable state per round: the dist array (CAS), one
frontier array, and two index/tail pairs — the entire "priority
queue" is emergent from voting over thread-local vectors.

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/sssp.cc` | RelaxEdges CAS kernel (70-86), thread-local bins (32-39), lazy deletion (41-44), bucket fusion (46-53) |
| gbbs | `reference-repos-competitors/gbbs-src/benchmarks/PositiveWeightSSSP/DeltaStepping/DeltaStepping.h` | generalized bucket structure (27, 80-101) |
| gbbs | `reference-repos-competitors/gbbs-src/benchmarks/GeneralWeightSSSP/BellmanFord/BellmanFord.h` | the all-parallel pole |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_SingleSourceShortestPath.c` | algebraic delta-stepping (25, 39, 85) |
| graphit | `reference-repos-corpus/graphit-src` | ordered-algorithm scheduling; origin of bucket fusion |

## 10. Cross-references

- Sibling patterns: `frontier-pushpull-switching` (delta=1 special
  case); `semiring-matrix-traversal` (MIN_PLUS relax);
  `component-hooking-shortcutting` (CAS-retry idiom);
  `lsm-compaction-tradeoff` (lazy deletion = tombstones).
- Next in category: graph-analytics synthesis pair rolling up
  patterns 7-12.
- Paper trail: Meyer & Sanders' original delta-stepping paper, the
  GraphIt CGO'20 ordered-algorithms paper (bucket fusion), and the
  GrAPL'19 GraphBLAS delta-stepping paper LAGraph cites — see
  `research-papers-ledger.md`.
- 202606 digest overlap: digests named delta-stepping as GDS's SSSP
  algorithm; this pair adds bucket mechanics, thread-local bins,
  fusion, and delta-tuning arithmetic.

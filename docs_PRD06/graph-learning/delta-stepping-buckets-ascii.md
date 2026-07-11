# Delta Stepping Buckets — ASCII

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `delta-stepping-buckets-ascii.md` / `delta-stepping-buckets-mermaid.md` |
| One-line job | Parallel shortest paths by relaxing Dijkstra's strict priority order into buckets of width delta — enough order for correctness pruning, enough slack for parallelism |

## 1. The job

Weighted single-source shortest paths sits between two poles:

```text
Dijkstra      settle vertices in EXACT distance order via a priority
              queue — work-optimal (each vertex settled once) but
              SEQUENTIAL: the queue is a serialization point.
Bellman-Ford  relax ALL edges every round — perfectly parallel but
              wasteful: O(n*m) worst case, vertices re-relaxed many
              times with stale distances.
```

Delta-stepping is the tunable midpoint: partition tentative
distances into buckets of width delta, process buckets in order, but
process everything INSIDE a bucket in parallel Bellman-Ford style:

```text
bucket[i] holds vertices with dist in [i*delta, (i+1)*delta)

loop: take smallest non-empty bucket i
      relax its vertices in parallel (repeatedly, until bucket i
        stops gaining members — light edges may re-add)
      improved vertices land in bucket new_dist/delta
      advance to next non-empty bucket

delta -> 0    : degenerates to Dijkstra (one vertex per bucket)
delta -> inf  : degenerates to Bellman-Ford (one giant bucket)
```

## 2. Raw data shape

```text
dist:      |V| weights, init INF, CAS-updated
frontier:  the current bucket's vertex list
bins:      per-thread vector<vector<NodeID>> — bucket b is
           thread-local until a copy phase merges it

gapbs RelaxEdges (sssp.cc:70-86), the whole kernel:
    for wn in out(u):
      new_dist = dist[u] + wn.w
      while new_dist < dist[wn.v]:
        if CAS(dist[wn.v], old, new_dist):
          local_bins[new_dist/delta].push(wn.v); break
        else reload old      # lost race to a better/worse update
```

Same CAS-retry shape as pattern 10's Link — the analytics category
keeps re-deriving "optimistic update + retry on conflict."

## 3. The engineering that makes it fast (gapbs sssp.cc:32-53)

```text
thread-local bins   all buckets are per-thread vectors; each round,
                    threads vote for the smallest non-empty bucket,
                    then copy just that bucket into a shared
                    frontier. No concurrent priority queue exists
                    AT ALL.
lazy deletion       a vertex re-added to a lower bucket is NOT
                    removed from the old one; stale entries are
                    skipped by the guard
                    `if dist[u] >= delta*curr_bin` (sssp.cc:110) —
                    cheaper than deleting (pattern kin: tombstones).
bucket fusion       if a thread's NEXT local bucket has the same
                    priority, execute it in the same round without
                    the global copy/sync — from GraphIt's CGO'20
                    ordered-algorithms work (cited at sssp.cc:29,46).
                    Kills the synchronization tail on high-diameter
                    graphs.
```

gbbs generalizes the bucket structure itself: `gbbs/bucket.h` with
`make_vertex_buckets` (DeltaStepping.h:27,101) — a reusable priority
structure that also drives its k-core and set-cover.

## 4. Step-by-step: delta=3 on a small graph

Edges: s->a(2), s->b(5), a->b(1), a->c(4), b->c(1). dist init INF.

```text
bucket 0 covers [0,3): process s (dist 0)
  relax s->a: dist[a]=2 -> bucket 0     (re-process bucket 0!)
  relax s->b: dist[b]=5 -> bucket 1
  process a (2): a->b: 3 < 5 -> dist[b]=3 -> bucket 1
                 a->c: 6     -> dist[c]=6 -> bucket 2
  bucket 0 empty now.
bucket 1 covers [3,6): process b (3)
  b->c: 4 < 6 -> dist[c]=4 -> bucket 1  (same bucket, re-run)
  process c (4): no out-edges. bucket 1 empty.
bucket 2: contains stale c-entry (dist 6) — guard skips it
          (dist[c]=4 < 2*3). done: dist = [s:0, a:2, b:3, c:4]
```

Note b was relaxed twice (5 then 3) — the price of bucket-level
parallelism; delta controls how much re-relaxation you tolerate.

## 5. Worked example — choosing delta

```text
rule of thumb: delta ~ average edge weight x (m/n) keeps bucket
re-processing bounded; gapbs makes it a per-graph CLI flag (-d)
because no single value wins (sssp.cc:28).

social graph (low diameter, weights 1..100, avg 50, deg 20):
  delta=50: ~ (max_dist/50) buckets ~ tens of rounds, each with
  huge parallel frontiers -> near-BFS behavior.
road network (diameter ~6000 km, weights = meters):
  delta too small -> thousands of near-empty rounds (Dijkstra-like
  serialization); delta too large -> re-relaxation explosion on
  long chains. Bucket fusion (sec 3) is worth ~2-5x exactly here,
  by draining chains without global barriers.
unit weights: delta=1 IS BFS — pattern 8's frontier reappears as
  the special case where buckets never re-process.
```

## 6. The GraphBLAS dialect

LAGraph ships the same algorithm as algebra
(LAGr_SingleSourceShortestPath.c:25 cites "Delta-Stepping SSSP: From
Vertices and Edges to GraphBLAS", GrAPL'19):

```text
per bucket: t = min.plus(A', frontier-restricted dist) — pattern
9's MIN_PLUS semiring does the relax; masks split light/heavy
edges; Delta arrives as a GrB_Scalar parameter (:85). A "FUTURE:
pick Delta automatically" comment (:39) confirms tuning remains
open even in the algebraic world.
```

## 7. Where graph systems inherit this

- Neo4j GDS shortest-path family: Dijkstra for single-pair,
  delta-stepping (`allShortestPaths.delta`) for single-source at
  scale — the API exposes the delta knob directly.
- gbbs's bucket structure powers ordered algorithms generally:
  k-core peeling and weighted BFS reuse `bucket.h` — buckets are
  the general "loose priority" primitive, not an SSSP one-off.
- GPU SSSP (gunrock) uses near-far buckets: two buckets only —
  delta-stepping's minimal form, chosen because GPU frontiers
  amortize better with fewer, larger launches.
- This repo: differential testing of SSSP against Neo4j must
  canonicalize ties — equal-length paths may differ; compare
  DISTANCES (deterministic) not paths, or define path tie-breaks.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/sssp.cc` | reference implementation: RelaxEdges CAS kernel (70-86), thread-local bins + voting (32-39), lazy deletion (41-44), bucket fusion (46-53) |
| gbbs | `reference-repos-competitors/gbbs-src/benchmarks/PositiveWeightSSSP/DeltaStepping/DeltaStepping.h` | reusable bucket structure (`gbbs/bucket.h`, make_vertex_buckets, 27, 80-101) |
| gbbs | `reference-repos-competitors/gbbs-src/benchmarks/GeneralWeightSSSP/BellmanFord/BellmanFord.h` | the all-parallel pole for contrast |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_SingleSourceShortestPath.c` | algebraic delta-stepping over MIN_PLUS (25, 39, 85) |
| graphit | `reference-repos-corpus/graphit-src` | ordered-algorithm scheduling; origin of the bucket-fusion optimization gapbs cites |

## 9. Cross-references

- Sibling patterns: `frontier-pushpull-switching` (BFS = delta=1
  special case); `semiring-matrix-traversal` (MIN_PLUS relax);
  `component-hooking-shortcutting` (same CAS-retry idiom);
  `lsm-compaction-tradeoff` (lazy deletion = tombstones — skip
  stale entries at read time instead of paying deletion at write
  time).
- The category's recurring meta-move, third appearance: trade
  strictness (exact priority order) for parallelism, bounded by a
  tunable knob (delta), guarded by a cheap validity check.
- 202606 digest overlap: digests named delta-stepping as GDS's
  SSSP algorithm; this pair adds the bucket mechanics, the
  thread-local-bins design, fusion, and the delta-tuning arithmetic.

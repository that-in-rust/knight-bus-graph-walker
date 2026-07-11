# PageRank Iteration Convergence — ASCII

| Field | Value |
| --- | --- |
| Kind | algorithm |
| Pair | `pagerank-iteration-convergence-ascii.md` / `pagerank-iteration-convergence-mermaid.md` |
| One-line job | Iterate rank = teleport + damping x (pulled in-neighbor contributions) until an error norm drops below tolerance — the template every fixed-point graph algorithm copies |

## 1. The job

PageRank scores vertices by "random surfer" probability: with
probability d (damping, canonically 0.85) follow a random out-edge,
with probability 1-d teleport anywhere. The stationary distribution
of that walk is the score vector. Every engine computes it the same
way — power iteration:

```text
rank_0[v]   = 1/n
rank_k+1[v] = (1-d)/n  +  d * SUM_{u -> v} rank_k[u] / out_deg(u)
              \_______/     \______________________________________/
               teleport       pull contributions from in-neighbors

stop when: error = SUM_v |rank_k+1[v] - rank_k[v]|  <  tolerance
           (L1 norm; typically tol = 1e-4 .. 1e-7, or a fixed
            iteration cap for benchmarking)
```

This is the template pattern: label propagation, Katz centrality,
HITS, belief propagation, and GNN message passing all reuse the
skeleton — init, gather, combine with a constant, measure change,
repeat.

## 2. Raw data shape

```text
scores:            |V| floats (double-buffered or in-place)
outgoing_contrib:  |V| floats — THE key optimization: precompute
                   rank[u]/out_deg(u) ONCE per iteration so the
                   inner loop is a pure sum of loads
                   (gapbs pr.cc:39-42,53)

gapbs inner loop (pr.cc:44-53), pull form over in-CSR:
    for u in V:                        # parallel, dynamic schedule
      incoming_total = 0
      for v in in(u): incoming_total += outgoing_contrib[v]
      scores[u] = base_score + kDamp * incoming_total
      error += |scores[u] - old|      # reduction(+)
```

Pull (pattern 8's dense side) is the natural direction: each u writes
only scores[u] — no atomics — and the error reduction rides the same
loop. Push-based PageRank exists for incremental/delta variants.

## 3. The dangling-node schism (a real cross-engine divergence)

Sinks (out_deg = 0) leak probability: their rank has nowhere to go.
The corpus splits three ways:

```text
gapbs / GAP spec     IGNORE sinks. sum(rank) < 1. Fast, benchmarks
                     only. LAGr_PageRankGAP.c:25-29 says so
                     explicitly: "does not return a centrality
                     metric such that sum(centrality) is 1".
LAGraph LAGr_PageRank.c
                     HANDLE sinks: gathers sink ranks each
                     iteration (sink/rsink vectors, :109-113) and
                     redistributes uniformly. sum(rank) = 1.
networkit            CONFIGURABLE: SinkHandling::DISTRIBUTE_SINKS
                     + optional normalization + choice of L1/L2
                     stopping norm (PageRank.cpp:16-59).
```

For the differential-testing thesis this is gold: "PageRank" is not
one function. An oracle must pin: sink policy, norm, tolerance,
iteration cap, and float precision — or scores diverge legitimately.

## 4. Step-by-step: three iterations on a 4-vertex graph

Graph: A->B, A->C, B->C, C->A, D->C (D is NOT a sink; C has out=1).
n=4, d=0.85, base = 0.0375:

```text
init:   rank = [.25, .25, .25, .25]
        contrib = rank/outdeg = [.125, .25, .25, .25]

iter 1: A pulls {C}:      .0375 + .85(.25)          = .25
        B pulls {A}:      .0375 + .85(.125)         = .14375
        C pulls {A,B,D}:  .0375 + .85(.125+.25+.25) = .56875
        D pulls {}:       .0375
        error = |0|+|.106|+|.319|+|.213| = .638

iter 2: contrib = [.125, .14375, .56875, .0375]
        A: .0375+.85(.56875) = .521    C: .0375+.85(.30625)=.298
        B: .0375+.85(.125)   = .144    D: .0375
        error = .543
...
converges ~ error x 0.85 per round: geometric with ratio d.
iters to tol 1e-6 ~ log(1e-6/.6)/log(.85) ~ 82 iterations.
```

That geometric convergence (ratio = damping factor) is why d=0.85 is
universal: bigger d = truer to the link structure but slower to
converge; 0.85 needs ~60-100 iterations at practical tolerances.

## 5. Worked example — cost model at a billion edges

n = 100M, m = 1B, pull form, in-CSR, 80 iterations to tol:

```text
per iteration:  m contribution loads (streaming, pattern 7)
                + n divides (contrib precompute) + n error terms
                = ~1B + 200M flops; memory-bound at ~4 GB/s per core
                over the 4 GB in-CSR + 800 MB of float arrays
80 iterations:  ~80 full passes over the graph = the textbook case
                for out-of-core execution being painful — 80 x 4.8GB
                of I/O if the graph doesn't fit in RAM.
                (GridGraph/GraphChi-style systems exist because of
                 exactly this access pattern.)
early exit:     real rank mass concentrates fast; delta-based
                variants (only propagate vertices whose rank moved
                > eps) cut late-iteration work 10-100x — graphit
                ships PageRankDelta as a canonical example.
```

## 6. Where graph systems inherit this

- Neo4j GDS pageRank: damping/tolerance/maxIterations are the API
  knobs — precisely the divergence axes of section 3; its estimate
  mode prices the two |V| float arrays.
- GraphBLAS/LAGraph: one mxv per iteration (pattern 9) — the
  contrib precompute becomes a diagonal scale, sinks a reduce.
- Streaming/incremental engines (differential-dataflow, feldera)
  treat PageRank as THE canonical recursive incremental query.
- GNN frameworks: message passing = the gather loop with learned
  combine functions — PyG/DGL inherit the pull structure wholesale.
- This repo: PageRank over immutable CSR segments is
  embarrassingly segment-parallel per iteration; the error
  reduction is the only cross-segment synchronization point.

## 7. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| gapbs | `reference-repos-corpus/gapbs-src/src/pr.cc` | reference pull PageRank: contrib precompute (39-42), fused error reduction (44-53), kDamp=0.85 (31) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_PageRankGAP.c` | GAP-spec variant, sinks ignored (25-29), damping prescale (99-114) |
| LAGraph | `reference-repos-corpus/LAGraph-src/src/algorithm/LAGr_PageRank.c` | sink-correct variant: sink/rsink redistribution (109-113) |
| networkit | `reference-repos-competitors/networkit-src/networkit/cpp/centrality/PageRank.cpp` | configurable sink handling, L1/L2 norms, normalization (16-59) |
| graphit | `reference-repos-corpus/graphit-src` | PageRank + PageRankDelta as schedulable algorithms |

## 8. Cross-references

- Sibling patterns: `frontier-pushpull-switching` (pull is the
  natural PageRank direction; delta variants re-introduce
  frontiers); `semiring-matrix-traversal` (one mxv per iteration);
  `csr-adjacency-layout` (the streaming access pattern the cost
  model depends on).
- Verification note: PageRank is the floating-point-tolerance poster
  child — same algorithm, different summation orders, legitimately
  different low-order bits. Equivalence = |a-b| < eps rankwise, or
  rank-order comparison for top-k. Plus the section-3 policy axes.
- 202606 digest overlap: digests listed PageRank among GDS
  algorithms; this pair adds the iteration mechanics, the sink
  schism with file evidence, and the convergence arithmetic.

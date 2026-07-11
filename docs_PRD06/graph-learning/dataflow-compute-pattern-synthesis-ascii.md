# Dataflow Compute Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `dataflow-compute-pattern-synthesis-ascii.md` / `dataflow-compute-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 25-26 into the category's thesis: iteration is the stress test of every dataflow design — barriers make it simple and slow to converge, frontiers make it fast and subtle, and the choice decides fault tolerance, memory, and freshness all at once |

## 1. The category in one sentence

The 6 dataflow-compute repos (timely-dataflow, differential-
dataflow, datafrog, spark, flink, velox) exist because graph
algorithms are LOOPS, and general dataflow engines were built
for DAGs — the category's two patterns are the two ways loops
were retrofitted:

```text
25 incremental-delta-iteration  execution  loops via frontiers
                                           (partially ordered
                                           time, no barriers)
26 superstep-message-convergence execution loops via barriers
                                           (supersteps, shuffle
                                           per round)
```

## 2. One axis explains the category

```text
                barrier (26)              frontier (25)
round cost      full shuffle + sync       per-update flow
termination     empty message set         frontier empties
increments      recompute or hand-rolled  native (signed diffs)
fault story     checkpoint every N        replay updates from
                rounds (lineage trap)     durable input log
determinism     given partitioning +      given input times —
                assoc. combiner           consolidation is
                                          canonical
mental model    "rounds of mail"          "ledger of changes"
who             GraphX/Giraph/Flink       differential/datafrog
                (Pregel lineage)          (Naiad lineage)
```
Velox sits outside the loop question entirely: it is the
vectorized EXECUTION KERNEL (exchange operators, columnar
batches) that either regime can compile down to — the same
role ValueVector batches play inside Kuzu (pattern 21).

## 3. What the category teaches the graph corpus

```text
1. the analytics category's algorithms are POLICY; this
   category is MECHANISM: the same WCC is a frontier loop
   (regime 1), a superstep program (regime 2, pattern 26's
   min-label example), or a standing differential query
   (regime 3, pattern 25's six-line BFS)
2. partitioning is where distributed graph cost hides:
   vertex-cut's 2*sqrt(P) bound (26) is the only lever that
   tames power-law hubs — no execution cleverness compensates
   for a bad cut
3. incremental view maintenance is the graph-DB frontier:
   pattern 25's (data, time, diff) triple is exactly what a
   graph database's transaction stream already contains —
   the two categories are one arrangement apart
```

## 4. Worked example — the same query in three regimes

```text
"friend-of-friend count per user", 10M edges, 100 edges/sec
changing:
regime 1 (GDS-style):    recompute on demand: seconds per run,
                         always stale between runs
regime 2 (GraphX):       recompute per batch window: minutes
                         of latency, cluster bill per window
regime 3 (differential): arrange edges once; each edge update
                         costs ~2 joins' worth of deltas;
                         answer continuously exact
crossover rule of thumb from the sources: differential wins
when delta/state per query interval is small and the SAME
arrangement serves many queries; batch wins for one-shots —
the COST paper's laptop-vs-cluster caution applies to BOTH
distributed regimes.
```

## 5. Worked example — why the barrier is the fault line

```text
superstep engine, 1000 rounds (high-diameter graph):
    1000 barriers x (slowest straggler each round)
    + checkpoint every 25 rounds (26's lineage trap)
frontier engine, same computation:
    updates flow as capabilities allow; a slow worker delays
    only times it holds; no global wait
    BUT: correctness now rests on the antichain protocol
    (frontier.rs) — subtle enough that timely's progress
    tracking is its OWN module with reachability analysis
the trade is engineering-risk vs run-time: barriers are easy
to reason about and slow; frontiers are fast and hard to
implement correctly ONCE (then reused forever).
```

## 5b. Choosing a regime — a decision walk

```text
ask three questions in order:
1. does the graph fit one machine's RAM (plus slack)?
   yes -> regime 1: analytics-category code, no coordination
   tax at all; COST says this covers far more workloads
   than cluster marketing admits
2. is the workload one-shot or standing?
   one-shot + too big for RAM -> regime 2: supersteps are
   the simplest correct thing; spend the effort on the
   PARTITIONING (vertex-cut), not the engine
   standing/streaming -> regime 3: pay the arrangement once,
   answer forever; delta cost ~ update rate, not graph size
3. is the iteration deep (high diameter, nested loops)?
   deep + regime 2 -> expect barrier pain: switch algorithm
   (hooking/shortcutting) before switching engine
   deep + regime 3 -> Product timestamps handle nesting
   natively; this is differential's strongest ground
```

## 6. Honest gaps

```text
not covered by 25-26 (later passes if wanted):
    - flink's watermark/checkpoint machinery in source detail
      (single-dimension frontiers + Chandy-Lamport snapshots)
    - velox's exchange/operator internals (regime-agnostic
      vectorized kernel)
    - dogsdogsdogs (worst-case-optimal joins on differential —
      directly relevant to Cypher multi-hop patterns)
    - spark structured streaming's incremental planner
```

## 7. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| timely-dataflow | `reference-repos-corpus/timely-dataflow-src/timely/src/progress/frontier.rs` | antichain frontiers (25) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/algorithms/graphs/bfs.rs` | six-line incremental BFS (25) |
| datafrog | `reference-repos-corpus/datafrog-src/src/variable.rs` | semi-naive miniature (25) |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/Pregel.scala` | superstep driver loop (26) |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/PartitionStrategy.scala` | vertex-cut bound (26) |
| giraph | `reference-repos-corpus/giraph-src/giraph-core/src/main/java/org/apache/giraph/graph/Vertex.java` | voteToHalt (26) |

## 8. Cross-references

- Members: `incremental-delta-iteration` (25),
  `superstep-message-convergence` (26).
- Prior syntheses: graph-analytics (regime 1 — the algorithms
  these regimes distribute), storage-engine (differential's
  traces ARE an LSM in time), graph-db (pattern 21's pull
  pipeline is what regime 3 would replace for standing
  queries).
- The carry-forward sentence: loops are where dataflow designs
  are honest — everything a system hides about coordination,
  fault tolerance, and staleness surfaces the moment the
  graph asks it to iterate.
- For the docs_PRD06 rewrite thesis: regime 3 is the
  differential oracle made runtime — incremental-vs-scratch
  equality is both this category's self-test and the rewrite's
  convergence loop in miniature.
- Last category: bench-testing (SQLancer/Jepsen/LDBC/ann-
  benchmarks), then the corpus synthesis is complete.

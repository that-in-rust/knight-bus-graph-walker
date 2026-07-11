# Superstep Message Convergence — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `superstep-message-convergence-ascii.md` / `superstep-message-convergence-mermaid.md` |
| One-line job | Distributed Pregel at production scale: a driver loop alternates join-vertices and send-messages rounds, vertices vote to halt, termination is "no messages left", and a vertex-cut partitioning bounds how many machines each vertex is replicated to |

## 1. The job

Patterns in graph-analytics covered vertex-centric thinking on
ONE machine. This pattern is what changes when the graph spans
a cluster: messages become shuffles, iteration state must
survive failures (checkpointing), termination becomes a
distributed emptiness test, and the PARTITIONING of edges
decides the communication bill.

## 2. GraphX's Pregel: the loop is ordinary dataflow

```text
spark-src/graphx/src/main/scala/org/apache/spark/graphx/
    Pregel.scala:116-124 — the whole contract is 3 functions:
    vprog:    (VertexId, VD, A) => VD        absorb message
    sendMsg:  EdgeTriplet => Iterator[(VertexId, A)]
    mergeMsg: (A, A) => A                    combiner (must be
                                             commutative+assoc.)
the loop (:131-163):
    g = graph.mapVertices(vprog(initialMsg))     round 0
    messages = mapReduceTriplets(g, sendMsg, mergeMsg)
    while (!messages.isEmpty && i < maxIterations):
        g = g.joinVertices(messages)(vprog)      receive
        messages = mapReduceTriplets(g, sendMsg, mergeMsg,
            Some((oldMessages, activeDirection)))  send — SKIPS
            edges where neither side got a message
```

Three production details textbook Pregel omits:
- ACTIVE-SET TRACKING: the `Some((oldMessages, ...))` argument
  restricts sendMsg to edges adjacent to last round's message
  recipients — the frontier optimization, distributed.
- CHECKPOINTING: PeriodicGraphCheckpointer every N rounds
  (:129-134) — long lineages of RDD transformations would
  otherwise make recovery replay the whole loop.
- MATERIALIZATION: messages.isEmpty() forces the round to run;
  laziness and iteration don't mix without explicit forcing.

## 3. Halting: Giraph's vote

```text
giraph-src/giraph-core/src/main/java/org/apache/giraph/graph/
    Vertex.java:80-85
    void voteToHalt();  "compute() will no longer be called
    for this vertex unless a message is sent to it"
global termination = every vertex halted AND no messages in
flight. GraphX expresses the same as data (empty message RDD);
Giraph as state (halt flags + message queues). Same semantics,
two encodings — the classic control-flow vs dataflow split.
```

## 4. Vertex-cut: the communication bill

```text
spark-src/.../PartitionStrategy.scala
    :113 RandomVertexCut     hash(src, dst) — cheap, no bound
    :74  EdgePartition2D     edges live in a sqrt(P) x sqrt(P)
         grid cell by (src row, dst column):
         each vertex's edges appear only in ONE ROW + ONE
         COLUMN -> replication bounded by 2*sqrt(P)
         (:60-70 ASCII matrix in the source comments!)
         plus a mixingPrime (1125899906842597L) scrambles
         vertex ids so hub rows don't overload one cell
vertex-cut vs edge-cut: power-law graphs make edge-cut
hopeless (any cut crosses the hubs); cutting VERTICES instead
(replicating them) spreads hub edges across machines — the
PowerGraph insight, productionized here.
```

## 5. Worked example — replication arithmetic

```text
P = 16 partitions, EdgePartition2D grid 4x4.
vertex v's edges: only in row(v) (4 cells) + column(v)
(4 cells) -> v replicated to at most 2*sqrt(16) = 8 machines,
whatever its degree. RandomVertexCut: a degree-1M hub touches
~all 16. At P = 1024: bound 64 vs ~1024 — the 2*sqrt(P) bound
is the difference between a hub costing 6% and 100% of the
cluster in replication traffic per superstep.
```

## 6. Worked example — a superstep of connected components

```text
CC via Pregel (min-label propagation), 4 vertices a-b-c-d in
a path, labels start {a:1, b:2, c:3, d:4}:
round 1: sendMsg over every edge; mergeMsg=min:
    b<-1, a<-2, c<-2, b<-3, d<-3, c<-4
    vprog keeps min: {a:1, b:1, c:2, d:3}
round 2: only vertices that CHANGED are adjacent to active
    edges: {b:1->c}, {c:2->d}... labels {a:1,b:1,c:1,d:2}
round 3: {d:1}. round 4: no messages -> loop exits.
rounds = graph diameter; each round is one shuffle. This is
why superstep engines hate high-diameter graphs — and why
pattern 25's differential approach (no global barrier) or
analytics' hooking/shortcutting (log-diameter) exist.
```

## 7. Why this matters for the corpus

This is the third execution regime for the SAME algorithms:
```text
single-machine frontier (analytics):  shared memory, cheap sync
superstep cluster (this pattern):     shuffles + barriers,
                                      partitioning-dominated
incremental dataflow (25):            no barriers, frontier
                                      tracking in TIME not space
```
An engineer choosing infrastructure is really choosing which
of three costs dominates: memory ceiling, network shuffles, or
arrangement maintenance. For the docs_PRD06 rewrite: GDS is
regime 1; knowing regimes 2-3 marks the boundary of what
"Neo4j parity" does NOT require — and where COST-paper
skepticism applies (a laptop often beats the cluster).

## 7b. Failure recovery — why the checkpointer exists

```text
Spark recomputes lost partitions from LINEAGE (the chain of
transformations that produced them). In an iterative loop the
lineage grows one join + one shuffle per round: lose an
executor at round 200 and naive recovery replays 200 rounds.
PeriodicGraphCheckpointer (Pregel.scala:129-134, default off,
spark.graphx.pregel.checkpointInterval) truncates lineage by
writing g and messages to stable storage every N rounds —
recovery cost drops from O(rounds) to O(interval). Giraph
does the equivalent with explicit superstep checkpoints.
The general law: ITERATION converts a fault-tolerance design
that's free for DAGs into a cost that must be re-paid — every
regime-2 engine carries some version of this machinery.
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/Pregel.scala` | driver loop, active sets, checkpointing (116-163) |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/PartitionStrategy.scala` | vertex-cut strategies, 2D grid bound (74-140) |
| giraph | `reference-repos-corpus/giraph-src/giraph-core/src/main/java/org/apache/giraph/graph/Vertex.java` | voteToHalt contract (80-85) |

## 9. Cross-references

- Sibling patterns: `incremental-delta-iteration` (25 — the
  barrier-free alternative), analytics category's frontier and
  hooking patterns (the single-machine versions of the same
  algorithms), `graph-analytics-pattern-synthesis`.
- Papers ledger: Pregel (the origin), COST (the caution — a
  single thread beats many published cluster numbers),
  PowerGraph (vertex-cut).
- Verification note: superstep engines are deterministic given
  a partitioning ONLY if mergeMsg is commutative+associative —
  a differential test between partitionings is a free check
  that a user's combiner actually is.
- Next: bench-testing category (SQLancer/Jepsen/LDBC), then
  the dataflow-compute synthesis.

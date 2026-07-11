# Superstep Message Convergence — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `superstep-message-convergence-ascii.md` / `superstep-message-convergence-mermaid.md` |
| One-line job | Distributed Pregel at production scale: a driver loop alternates join-vertices and send-messages rounds, vertices vote to halt, termination is "no messages left", and a vertex-cut partitioning bounds how many machines each vertex is replicated to |

## 1. The contract: three functions

```mermaid
flowchart TD
    API["Pregel.apply (Pregel.scala:116-124)"]
    API --> V["vprog(id, state, msg) -> state<br/>absorb the merged message"]
    API --> S["sendMsg(triplet) -><br/>iterator of (dst, msg)"]
    API --> M["mergeMsg(a, b) -> a+b<br/>MUST be commutative + associative:<br/>it runs as a combiner during<br/>the shuffle"]
```

## 2. The driver loop

```mermaid
flowchart TD
    R0["round 0: g = mapVertices(vprog(initialMsg))<br/>(Pregel.scala:131)"]
    R0 --> MSG["messages = mapReduceTriplets(g,<br/>sendMsg, mergeMsg)"]
    MSG --> Q{"messages.isEmpty()?<br/>(forces materialization —<br/>laziness + iteration don't mix)"}
    Q -->|no| RECV["g = g.joinVertices(messages)(vprog)"]
    RECV --> SEND["messages = mapReduceTriplets(g, ...,<br/>Some((oldMessages, activeDirection)))<br/>— SKIPS edges where neither side<br/>got a message: the frontier<br/>optimization, distributed"]
    SEND --> CKPT["PeriodicGraphCheckpointer every N<br/>rounds (:129-134): cut RDD lineage<br/>so recovery doesn't replay the loop"]
    CKPT --> Q
    Q -->|yes| DONE["converged"]
```

## 3. Halting: two encodings, one semantics

```mermaid
flowchart LR
    GX["GraphX: termination as DATA —<br/>the message RDD is empty"]
    GI["Giraph: termination as STATE —<br/>voteToHalt() (Vertex.java:80-85):<br/>'compute() no longer called unless<br/>a message arrives'; done = all halted<br/>AND no messages in flight"]
    GX --- SAME["same semantics —<br/>the dataflow vs control-flow<br/>split, again"]
    GI --- SAME
```

## 4. Vertex-cut partitioning

```mermaid
flowchart TD
    PS["PartitionStrategy.scala"]
    PS --> RVC["RandomVertexCut (:113):<br/>hash(src, dst) — cheap,<br/>NO replication bound"]
    PS --> E2D["EdgePartition2D (:74):<br/>sqrt(P) x sqrt(P) grid,<br/>edge -> cell (src row, dst col)"]
    E2D --> BND["each vertex's edges live in ONE row<br/>+ ONE column -> replicated to at most<br/>2*sqrt(P) machines, whatever its degree"]
    E2D --> MIX["mixingPrime 1125899906842597L<br/>scrambles ids so hub rows don't<br/>overload one cell"]
    BND --> WHY["why vertex-cut: power-law graphs make<br/>edge-cut hopeless (every cut crosses<br/>the hubs); replicate VERTICES instead —<br/>the PowerGraph insight"]
```

## 5. Worked example — replication arithmetic

```mermaid
flowchart LR
    A["P=16, grid 4x4:<br/>hub bounded to 2*sqrt(16) = 8<br/>machines; random cut: ~16"]
    B["P=1024:<br/>bound 64 vs ~1024 —<br/>6% vs 100% of the cluster<br/>in per-superstep replication<br/>traffic for one hub"]
    A --> B
```

## 6. Worked example — CC by min-label, path a-b-c-d

```mermaid
sequenceDiagram
    participant G as graph (labels)
    participant S as shuffle (messages, merged by min)
    Note over G: start {a:1, b:2, c:3, d:4}
    G->>S: round 1 — all edges send
    S->>G: {a:1, b:1, c:2, d:3}
    G->>S: round 2 — only edges next to<br/>CHANGED vertices are active
    S->>G: {a:1, b:1, c:1, d:2}
    G->>S: round 3
    S->>G: {a:1, b:1, c:1, d:1}
    Note over S: round 4 — no messages -> exit.<br/>rounds = diameter; each round = one<br/>shuffle + one barrier
```

## 7. The three execution regimes

```mermaid
flowchart TD
    R1["regime 1 — single-machine frontier<br/>(analytics category): shared memory,<br/>cheap sync; ceiling = RAM"]
    R2["regime 2 — superstep cluster<br/>(this pattern): shuffles + barriers;<br/>cost dominated by PARTITIONING"]
    R3["regime 3 — incremental dataflow (25):<br/>no global barriers; frontier tracked<br/>in TIME, cost = arrangements"]
    R1 --- CH["same algorithms, three cost models:<br/>memory ceiling vs network shuffles vs<br/>arrangement maintenance"]
    R2 --- CH
    R3 --- CH
    CH --> RW["rewrite relevance: GDS is regime 1;<br/>regimes 2-3 mark what Neo4j parity does<br/>NOT require — and where COST-paper<br/>skepticism applies (the laptop often wins)"]
```

## 8. Why high diameter hurts — and the escape hatches

```mermaid
flowchart TD
    D["rounds = diameter, and every round<br/>pays a full shuffle + barrier"]
    D --> H1["escape 1 (analytics): hooking +<br/>shortcutting — log-diameter CC"]
    D --> H2["escape 2 (25): differential — no<br/>barrier; updates flow as soon as<br/>frontiers allow"]
    D --> H3["escape 3: active-set restriction<br/>(Pregel.scala's oldMessages arg)<br/>at least shrinks WORK per round,<br/>though not round COUNT"]
```

## 9. Determinism as a free test

```mermaid
flowchart LR
    P1["run with RandomVertexCut"]
    P2["run with EdgePartition2D"]
    P1 & P2 --> EQ{"same results?"}
    EQ -->|yes| OK["mergeMsg really is<br/>commutative + associative"]
    EQ -->|no| BUG["user combiner bug — found by<br/>a partitioning-differential test,<br/>no oracle needed"]
```

## 9b. Failure recovery — lineage vs checkpoints

```mermaid
flowchart TD
    LIN["Spark recovers lost partitions from<br/>LINEAGE — free for DAG jobs"]
    LIN --> GROW["but the Pregel loop grows lineage by<br/>one join + one shuffle PER ROUND:<br/>lose an executor at round 200 -><br/>naive recovery replays 200 rounds"]
    GROW --> CK["PeriodicGraphCheckpointer<br/>(Pregel.scala:129-134,<br/>spark.graphx.pregel.checkpointInterval):<br/>persist g + messages every N rounds"]
    CK --> COST["recovery drops O(rounds) -> O(interval);<br/>Giraph: explicit superstep checkpoints"]
    COST --> LAW["law: ITERATION re-introduces the<br/>fault-tolerance bill that DAG engines<br/>thought they'd eliminated — every<br/>regime-2 engine carries this machinery"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/Pregel.scala` | driver loop, active sets, checkpointing (116-163) |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/PartitionStrategy.scala` | vertex-cut strategies, 2D bound (74-140) |
| giraph | `reference-repos-corpus/giraph-src/giraph-core/src/main/java/org/apache/giraph/graph/Vertex.java` | voteToHalt contract (80-85) |

## 11. Cross-references

- Sibling patterns: `incremental-delta-iteration` (25),
  the analytics category's frontier/hooking patterns (regime 1
  versions), `graph-analytics-pattern-synthesis`.
- Papers ledger: Pregel, PowerGraph (vertex-cut), COST (the
  standing caution against cluster-first thinking).
- Amusing source note: PartitionStrategy.scala:40-70 contains
  the 2D-grid explanation as ASCII art IN the comments — the
  corpus documenting itself in this knowledgebase's own format.
- Next: bench-testing category, then the dataflow-compute
  synthesis closes batch 7.
- Reading order for this pattern: Pregel.scala top comment
  (PageRank example) first, then the loop body (:131-163),
  then PartitionStrategy.scala's grid comment; Giraph's
  Vertex.java last — it shows how small the user-facing
  contract really is (compute + voteToHalt).
- Terminology bridge: "superstep" = one barrier-to-barrier
  round; "combiner" = mergeMsg run shuffle-side; "active
  direction" = which endpoint's activity keeps an edge live;
  "vertex-cut" = partition edges, replicate vertices (the
  inverse of the edge-cut most textbooks draw first).

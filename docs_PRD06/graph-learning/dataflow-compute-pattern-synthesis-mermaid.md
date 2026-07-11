# Dataflow Compute Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `dataflow-compute-pattern-synthesis-ascii.md` / `dataflow-compute-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 25-26 into the category's thesis: iteration is the stress test of every dataflow design — barriers make it simple and slow to converge, frontiers make it fast and subtle, and the choice decides fault tolerance, memory, and freshness all at once |

## 1. The category in one map

```mermaid
flowchart TD
    C["6 dataflow-compute repos:<br/>graph algorithms are LOOPS,<br/>dataflow engines were built for DAGs"]
    C --> P25["25 incremental-delta-iteration:<br/>loops via FRONTIERS —<br/>partially ordered time, no barriers<br/>(timely/differential/datafrog)"]
    C --> P26["26 superstep-message-convergence:<br/>loops via BARRIERS —<br/>supersteps, shuffle per round<br/>(GraphX/Giraph, Pregel lineage)"]
    C --> VX["velox: outside the loop question —<br/>the vectorized execution KERNEL either<br/>regime compiles to (same role as Kuzu's<br/>ValueVector batches, pattern 21)"]
```

## 2. One axis explains the category

```mermaid
flowchart LR
    B["BARRIER (26)<br/>round = shuffle + sync<br/>terminate: no messages<br/>increments: recompute<br/>faults: checkpoint every N<br/>model: rounds of mail"]
    F["FRONTIER (25)<br/>per-update flow<br/>terminate: frontier empties<br/>increments: native (signed diffs)<br/>faults: replay the input log<br/>model: ledger of changes"]
    B <-->|"same algorithms,<br/>opposite coordination"| F
```

## 3. Policy vs mechanism

```mermaid
flowchart TD
    WCC["one algorithm: WCC"]
    WCC --> R1["regime 1 — frontier loop in RAM<br/>(analytics category)"]
    WCC --> R2["regime 2 — superstep min-label<br/>(26's worked example)"]
    WCC --> R3["regime 3 — standing differential query<br/>(25's six-line BFS shape)"]
    R1 & R2 & R3 --> LES["lesson: analytics category = POLICY,<br/>this category = MECHANISM; and<br/>partitioning (vertex-cut 2*sqrt(P), 26)<br/>is the only lever that tames power-law<br/>hubs — execution cleverness can't<br/>compensate for a bad cut"]
```

## 4. Worked example — one query, three regimes

```mermaid
flowchart TD
    Q["friend-of-friend count per user;<br/>10M edges, 100 edge-changes/sec"]
    Q --> A["regime 1 (GDS-style): recompute<br/>on demand — seconds per run,<br/>stale between runs"]
    Q --> B2["regime 2 (GraphX): recompute per<br/>batch window — minutes of latency,<br/>cluster bill per window"]
    Q --> C2["regime 3 (differential): arrange once;<br/>each update ~2 joins of deltas;<br/>continuously exact"]
    A & B2 & C2 --> RULE["crossover: differential wins when<br/>delta/state is small AND the arrangement<br/>serves many queries; batch wins one-shots;<br/>COST-paper caution applies to both<br/>distributed regimes"]
```

## 5. The barrier as fault line

```mermaid
flowchart TD
    S["superstep, 1000 rounds (high diameter):<br/>1000 x (slowest straggler)<br/>+ checkpoints (lineage trap, 26)"]
    T["frontier: updates flow as capabilities<br/>allow; a slow worker delays only the<br/>times it holds"]
    S --- TR["the trade: engineering risk vs run time"]
    T --- TR
    TR --> RISK["barriers: easy to reason about, slow;<br/>frontiers: fast, but the antichain<br/>protocol (frontier.rs) is subtle enough<br/>to be its own module with reachability<br/>analysis — implemented correctly ONCE,<br/>reused forever"]
```

## 5b. Choosing a regime — a decision walk

```mermaid
flowchart TD
    Q1{"1. graph fits one<br/>machine's RAM?"}
    Q1 -->|yes| R1["regime 1: analytics-category code,<br/>zero coordination tax — COST says this<br/>covers more workloads than cluster<br/>marketing admits"]
    Q1 -->|no| Q2{"2. one-shot or<br/>standing workload?"}
    Q2 -->|one-shot| R2["regime 2: supersteps are the simplest<br/>correct thing; spend effort on the<br/>PARTITIONING (vertex-cut), not the engine"]
    Q2 -->|standing| R3["regime 3: pay the arrangement once,<br/>answer forever; delta cost ~ update<br/>rate, not graph size"]
    R2 --> Q3{"3. deep iteration<br/>(high diameter / nesting)?"}
    Q3 -->|yes| SW["expect barrier pain: switch ALGORITHM<br/>(hooking/shortcutting) before switching<br/>engine; regime 3's Product timestamps<br/>handle nesting natively"]
```

## 6. The bridge to graph databases

```mermaid
flowchart LR
    TX["graph DB transaction stream:<br/>(entity, commit-time, +1/-1)"]
    DD["differential's update triple:<br/>(data, time, diff)"]
    TX -->|"they are the SAME shape"| DD
    DD --> IVM["incremental view maintenance:<br/>standing WCC/BFS/PageRank over the<br/>commit stream — GDS's project-then-run<br/>snapshot lag dissolves"]
    IVM --> ONE["the two categories are<br/>one arrangement apart"]
```

## 7. Honest gaps

```mermaid
flowchart TD
    G["not covered by 25-26"]
    G --> G1["flink's watermarks + Chandy-Lamport<br/>checkpoint snapshots in source detail"]
    G --> G2["velox exchange/operator internals<br/>(the regime-agnostic kernel)"]
    G --> G3["dogsdogsdogs: worst-case-optimal joins<br/>on differential — directly relevant to<br/>Cypher multi-hop patterns"]
    G --> G4["spark structured streaming's<br/>incremental planner"]
```

## 8. Position in the corpus

```mermaid
flowchart TD
    AN["graph-analytics: the algorithms"]
    ST["storage-engine: differential traces<br/>ARE an LSM in time"]
    DB["graph-db: pattern 21's pull pipeline —<br/>what regime 3 replaces for<br/>standing queries"]
    DF["dataflow-compute (this):<br/>the loop mechanisms"]
    AN & ST & DB --> DF
    DF --> CF["carry-forward: loops are where dataflow<br/>designs are honest — coordination, fault<br/>tolerance, staleness all surface the<br/>moment the graph asks to iterate"]
    CF --> NEXT["last category: bench-testing<br/>(SQLancer/Jepsen/LDBC/ann-benchmarks) —<br/>then the corpus synthesis"]
```

## 9. The rewrite-thesis tie-in

```mermaid
flowchart LR
    SELF["regime 3's self-test:<br/>incremental run == from-scratch run<br/>on consolidated output"]
    CONV["docs_PRD06 convergence loop:<br/>rewrite output == stock output<br/>on canonicalized results"]
    SELF -->|"same idea — a differential<br/>oracle; one tests an ENGINE<br/>against itself, the other a<br/>REWRITE against its endpoint"| CONV
```

## 10. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| timely-dataflow | `reference-repos-corpus/timely-dataflow-src/timely/src/progress/frontier.rs` | antichain frontiers (25) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/algorithms/graphs/bfs.rs` | incremental BFS (25) |
| datafrog | `reference-repos-corpus/datafrog-src/src/variable.rs` | semi-naive miniature (25) |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/Pregel.scala` | superstep driver loop (26) |
| spark | `reference-repos-corpus/spark-src/graphx/src/main/scala/org/apache/spark/graphx/PartitionStrategy.scala` | vertex-cut bound (26) |
| giraph | `reference-repos-corpus/giraph-src/giraph-core/src/main/java/org/apache/giraph/graph/Vertex.java` | voteToHalt (26) |

## 11. Cross-references

- Members: `incremental-delta-iteration` (25),
  `superstep-message-convergence` (26).
- Prior syntheses: graph-analytics, storage-engine, graph-db,
  neo4j-ecosystem — this category supplies the loop mechanisms
  they all presuppose or avoid.
- Reading order: datafrog's variable.rs (200 lines), then
  GraphX's Pregel.scala (the loop with production scars), then
  differential's bfs.rs, then timely's frontier.rs.
- Terminology bridge across the two patterns: 26's "no
  messages left" and 25's "frontier empties" are the same
  termination fact expressed as data vs as capability; 26's
  combiner (merge-associative) and 25's consolidation (sum of
  diffs) are the same algebraic requirement; 26's checkpoint
  interval and 25's compaction frontier are both answers to
  "how much history may I discard?".
- The category's exam question: given a workload, name the
  regime, name the dominant cost (RAM / shuffle /
  arrangements), and name the free differential test it
  affords. All three answers come from the table in §2.
- The ASCII twin adds the same decision walk in prose plus the
  three-regime friend-of-friend worked example with concrete
  latency and cost characterizations.

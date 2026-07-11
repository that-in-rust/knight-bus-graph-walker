# Pull Operator Pipeline — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `pull-operator-pipeline-ascii.md` / `pull-operator-pipeline-mermaid.md` |
| One-line job | Turn a declarative graph query into a tree of operators that PULL rows from their children — the Volcano iterator model as every graph database's execution spine, tuple-at-a-time (memgraph) or vector-at-a-time (kuzu) |

## 1. The three-stage pipe

```mermaid
flowchart LR
    T["query text<br/>MATCH (a:Person)-[:KNOWS]->(b)<br/>WHERE b.age > 30 RETURN b.name"]
    T --> P["parse -> AST +<br/>semantic analysis"]
    P --> L["plan -> logical operator tree<br/>(cost-based choices: index vs scan,<br/>expand direction, join order)"]
    L --> X["execute -> physical operators,<br/>each PULLING from its child"]
```

## 2. The operator tree

```mermaid
flowchart BT
    S["ScanAllByLabel(a, :Person)"] --> E["Expand(a, KNOWS, OUT, b)"]
    E --> F["Filter(b.age > 30)"]
    F --> PR["Produce(b.name)"]
    PR --> C["client drains the root;<br/>next() recurses down;<br/>nothing materialized until a<br/>pipeline breaker (sort, aggregate,<br/>hash-join build)"]
```

## 3. Memgraph: tuple-at-a-time

```mermaid
flowchart TD
    I["operator.hpp:95<br/>virtual bool Pull(Frame&, ExecutionContext&)<br/>true = one more row, false = done"]
    I --> CUR["every LogicalOperator produces a<br/>Cursor (operator.hpp:77-79)"]
    CUR --> OPS["ScanAll :558 | Expand :1031<br/>Filter :1257 | Produce :1315"]
    OPS --> FR["Frame = register file: one slot per<br/>query variable, written in place"]
    FR --> CHAR["one Pull = one row through the tree:<br/>simple, low-latency, cache-unfriendly —<br/>matches memgraph's OLTP workload"]
```

## 4. Kuzu: vector-at-a-time

```mermaid
flowchart TD
    G["physical_operator.h:130 getNextTuple<br/>:157 getNextTuplesInternal"]
    G --> V["each pull fills ValueVectors, not a row —<br/>'capacity is either 1 (sequence) or<br/>DEFAULT_VECTOR_CAPACITY'<br/>(value_vector.h:20)"]
    V --> SEL["filters flip bits in a SELECTION<br/>vector — no copying"]
    SEL --> AM["virtual-call cost amortized over the<br/>batch; operators run tight loops over<br/>contiguous arrays — pattern 8's batch<br/>discipline inside a query engine"]
```

## 5. Neo4j: planning made explicit

```mermaid
flowchart LR
    FE["front-end/<br/>parse + semantics<br/>(openCypher)"] --> LP["cypher-logical-plans/<br/>plan-as-DATA: immutable case classes<br/>AllNodesScan (LogicalPlan.scala:875)<br/>Expand (:2681)"]
    LP --> PL["cypher-planner/<br/>cost-based rewrites of the tree"]
    PL --> RT["interpreted-runtime/<br/>Pull-style execution"]
    LP -.->|"plan is a value"| EX["EXPLAIN/PROFILE printable,<br/>plan caches possible,<br/>alternative runtimes swappable<br/>(enterprise compiles the same plans)"]
```

## 6. One row through the tree

```mermaid
sequenceDiagram
    participant C as client
    participant PR as Produce
    participant F as Filter(b.age>30)
    participant E as Expand
    participant S as ScanAll
    C->>PR: Pull
    PR->>F: Pull
    F->>E: Pull
    E->>S: Pull
    S-->>E: Frame[a=alice] (true)
    E-->>F: Frame[b=bob] (true)
    F-->>PR: bob.age=35 > 30 (true)
    PR-->>C: "bob"
    C->>PR: Pull
    Note over F,E: Expand yields carol; 28 > 30 FAILS —<br/>Filter silently pulls again;<br/>Expand exhausted; ScanAll exhausted
    PR-->>C: false — query done
```

## 7. Why vectors win on scans (and don't on lookups)

```mermaid
flowchart TD
    W["filter 10M nodes, 10% pass"]
    W --> TU["tuple pull: 10M rows x ~4 virtual<br/>calls = 40M dispatches, branch per row"]
    W --> VE["vector pull: 10M/2048 ~ 4900 pulls<br/>x 4 = ~20K dispatches; filter = <br/>SIMD-friendly bitmask loop"]
    TU & VE --> BUT["but degree-3 OLTP expands gain nothing<br/>from batching: memgraph's shape fits<br/>transactions, kuzu's fits analytics —<br/>neither is a mistake"]
```

## 8. The corpus on this axis

```mermaid
flowchart TD
    AX["how do rows move?"]
    AX --> T1["TUPLE pull: memgraph, neo4j interpreted<br/>runtime, TinkerPop traversers (Gremlin<br/>steps = iterators in traversal clothes)"]
    AX --> T2["VECTOR pull: kuzu, DuckPGQ<br/>(DuckDB's vectorized engine)"]
    AX --> T3["COMPILED: neo4j enterprise pipelined<br/>runtime — bytecode from the SAME<br/>logical plans"]
    T1 & T2 & T3 --> OBS["planners differ MOST (cost models,<br/>join enumeration); executors differ<br/>LEAST — everyone pulls"]
```

## 9. The verification angle

```mermaid
flowchart TD
    O["the pipeline IS the observable surface:<br/>same query + same data -> same results,<br/>modulo row order"]
    O --> R1["pin ORDER BY, else compare<br/>result MULTISETS"]
    O --> R2["openCypher TCK = this oracle,<br/>pre-built, thousands of scenarios"]
    O --> R3["plan shapes (EXPLAIN) are<br/>NON-contractual — never diff them"]
    R1 & R2 & R3 --> RW["rewrite relevance: results must match;<br/>tuple vs vector vs compiled execution is<br/>a free choice — three camps in<br/>production are the proof"]
```

## 9b. Pipeline breakers — where pull stops streaming

```mermaid
sequenceDiagram
    participant R as root (Produce)
    participant SO as Sort / Aggregate /<br/>HashJoin-build
    participant CH as child subtree
    R->>SO: first Pull
    SO->>CH: Pull... Pull... Pull (DRAIN)
    Note over SO,CH: a breaker must consume its ENTIRE child<br/>before yielding row one — this is where<br/>memory blows up, where spill-to-disk<br/>lives, and where kuzu's vectors pay off<br/>most (bulk build of hash tables /<br/>sort runs from contiguous batches)
    CH-->>SO: exhausted (false)
    SO-->>R: now yields sorted/aggregated rows,<br/>one Pull at a time from its buffer
    Note over R,SO: planners fight to push breakers DOWN<br/>(pre-aggregate before expand) or remove<br/>them (index-backed ORDER BY: neo4j's<br/>planner picks an index scan whose order<br/>makes Sort unnecessary — plan-as-data<br/>rewrites, §5)
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| memgraph | `reference-repos-competitors/memgraph-src/src/query/plan/operator.hpp` | Cursor::Pull (77-95), ScanAll/Expand/Filter/Produce (558, 1031, 1257, 1315) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/processor/operator/physical_operator.h` | getNextTuple/getNextTuplesInternal (130, 157) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/common/vector/value_vector.h` | ValueVector batch capacity (20) |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/cypher/cypher-logical-plans/src/main/scala/org/neo4j/cypher/internal/logical/plans/LogicalPlan.scala` | plan-as-data: AllNodesScan (875), Expand (2681) |

## 11. Cross-references

- Sibling patterns: `record-chain-adjacency` (20 — what Expand
  reads), `frontier-push-pull` (8 — the batch discipline kuzu
  imports), `bm25-wand-pruning` (18 — scorer trees are pull
  pipelines with score-ordered heaps).
- Kinship law: Volcano's next() is the same shape as pattern
  17's advance() and HNSW's candidate pop (13) — demand-driven
  iteration is the corpus's universal execution idiom.
- Paper trail: Graefe's Volcano paper and MonetDB/X100
  ("vectorized execution") — the two poles of §7 — queued in
  `research-papers-ledger.md`.
- Next in category: property/columnar value storage, then the
  graph-db category synthesis.

# Graph DB Pattern Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `graph-db-pattern-synthesis-ascii.md` / `graph-db-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 20-22 into the category's thesis: a graph database = one adjacency-layout decision + one query pipeline, and everything else it does is imported wholesale from the other categories of this corpus |

## 1. The category in one map

```mermaid
flowchart TD
    GDB["graph database"]
    GDB --> N1["native invention 1:<br/>adjacency LAYOUT —<br/>chains / disk-CSR / KV (20),<br/>or all sort orders (22)"]
    GDB --> N2["native invention 2:<br/>declarative -> pull PIPELINE (21):<br/>parse -> plan -> execute"]
    GDB --> IMP["everything else IMPORTED:<br/>LSM (1), WAL (2), MVCC (4),<br/>pages (5,6), dictionaries (19),<br/>posting compression (17),<br/>CSR + batches (7,8)"]
    N1 & N2 & IMP --> TH["thesis: thin native core over a<br/>thick imported substrate"]
```

## 2. The three patterns

```mermaid
flowchart LR
    P20["20 record-chain-adjacency<br/>STORAGE: where edges live —<br/>neo4j chains vs kuzu packed CSR<br/>vs janusgraph KV values"]
    P21["21 pull-operator-pipeline<br/>EXECUTION: Volcano pulls —<br/>tuple (memgraph), vector (kuzu),<br/>compiled (neo4j enterprise)"]
    P22["22 triple-permutation-indexing<br/>STORAGE: RDF refuses to pick —<br/>all sort orders, redundantly<br/>(oxigraph 9 families, qlever 6)"]
    P20 & P22 --> P21
```

## 3. The organizing axes

```mermaid
flowchart TD
    A1["axis 1 — data model forces layout:<br/>property graph -> pick ONE adjacency<br/>layout (20); RDF -> pick NONE,<br/>store every order (22);<br/>the query language follows"]
    A2["axis 2 — the OLTP/OLAP fork again:<br/>chains = write-locality, zero maintenance;<br/>packed CSR / permutation blocks =<br/>read-locality, pay-at-checkpoint"]
    A3["axis 3 — the contract lives at 21:<br/>layouts are UNOBSERVABLE;<br/>result sets are the surface"]
    A1 & A2 & A3 --> V["the whole verification story<br/>hangs on axis 3"]
```

## 4. The golden path

```mermaid
sequenceDiagram
    participant Q as MATCH (a:Person)-[:KNOWS]->(b)<br/>WHERE b.age > 30 RETURN b.name
    participant PL as planner (21)
    participant SC as ScanAllByLabel
    participant EX as Expand (20)
    participant F as Filter + Produce
    Q->>PL: parse -> logical plan -><br/>index/direction/filter choices
    PL->>SC: label index = a posting list (17)<br/>or bitmap (3)
    SC->>EX: node IDs
    EX->>EX: chase chains (neo4j) | slice CSR<br/>run (kuzu) | KV-get list (janusgraph)
    EX->>F: neighbor rows
    F-->>Q: b.name streamed over Bolt —<br/>every step a named corpus pattern
```

## 5. The import bill

```mermaid
flowchart LR
    ST["storage-engine: LSM (1) under<br/>oxigraph/janusgraph/dgraph;<br/>WAL (2); MVCC (4); pages (5,6)"]
    FT["FTS: dictionaries (19 = id2str);<br/>posting compression (17 = QLever<br/>blocks, label indexes);<br/>top-k pruning (18)"]
    AN["analytics: CSR (7) = kuzu's layout;<br/>batch execution (8) = kuzu vectors;<br/>GDS runs 9-12 on top of 20"]
    ST & FT & AN --> BIG["graph-db: the corpus's biggest<br/>importer — its moat is the<br/>layout+pipeline PAIRING"]
```

## 6. One edge, four physical lives

```mermaid
flowchart TD
    E["alice -KNOWS-> bob"]
    E --> L1["neo4j: 34-byte record in two<br/>doubly-linked chains"]
    E --> L2["kuzu: one entry in fwd CSR +<br/>one in bwd CSR, in slack"]
    E --> L3["janusgraph: a column entry under<br/>alice's row key (EdgeSerializer)"]
    E --> L4["oxigraph: NINE copies — one key<br/>per permutation family"]
    L1 & L2 & L3 & L4 --> SAME["same neighbor set out of all four —<br/>the harness compares SETS, never bytes"]
```

## 7. The verification pipeline

```mermaid
sequenceDiagram
    participant A as engine A (stock neo4j)
    participant B as engine B (rewrite / competitor)
    participant H as harness
    H->>A: same data, same query corpus
    H->>B: same data, same query corpus
    H->>H: compare result MULTISETS<br/>(pin ORDER BY where present)
    Note over H: pre-paid oracles, uniquely rich here:<br/>openCypher TCK (thousands of scenarios),<br/>W3C SPARQL suites, TinkerPop process<br/>tests — no other category starts with<br/>this much free coverage
    Note over H: the swamp: error TEXT — decide early<br/>whether messages are contract or accident
    Note over A,B: the unobservable 20% — recovery, races,<br/>memory pressure — still needs engineered<br/>harnesses (failpoints, deterministic<br/>schedulers), exactly as the docs_PRD06<br/>thesis predicts
```

## 8. Honest gaps

```mermaid
flowchart TD
    GAP["not covered by 20-22"]
    GAP --> G1["property/columnar value storage<br/>(neo4j property chains vs kuzu columns)"]
    GAP --> G2["transactions & replication:<br/>Raft (nebula), causal clustering"]
    GAP --> G3["distributed traversal: edge-cut vs<br/>vertex-cut re-enters from analytics"]
    GAP --> G4["Bolt/PackStream wire protocol —<br/>neo4j-ecosystem category, next batch"]
    GAP --> G5["GQL/ISO standardization,<br/>openCypher front-end reuse"]
```

## 9. The corpus position

```mermaid
flowchart TD
    S1["storage-engine synthesis:<br/>the substrate"]
    S2["graph-analytics synthesis:<br/>what runs on top"]
    S3["vector-ann + FTS syntheses:<br/>sibling verticals whose structures<br/>reappear as label indexes<br/>and dictionaries"]
    S1 & S2 & S3 --> G["graph-db synthesis (this pair):<br/>thin native core, thick imports"]
    G --> RW["why the rewrite thesis is plausible:<br/>most of the surface is patterns with<br/>independent, well-understood contracts —<br/>the truly novel part is small"]
    RW --> NEXT["next: neo4j-ecosystem — drivers,<br/>Bolt/PackStream, APOC, testkit:<br/>the wire-level observable surface"]
```

## 9b. The design walk (pick your engine shape)

```mermaid
flowchart TD
    START["workload: graph queries over<br/>N nodes, M edges"]
    START --> Q1{"data model?"}
    Q1 -->|"property graph,<br/>transactional"| C1["chains or KV: neo4j-shaped<br/>(write locality, point expands)<br/>or janusgraph-shaped (scale-out<br/>on a rented KV substrate)"]
    Q1 -->|"property graph,<br/>analytical"| C2["disk CSR + vectorized pulls:<br/>kuzu-shaped — scans and multi-hop<br/>joins dominate"]
    Q1 -->|"triples / open schema /<br/>federation"| C3["permutation indexes:<br/>oxigraph-shaped (LSM families)<br/>or qlever-shaped (compressed<br/>blocks, merge joins)"]
    C1 & C2 & C3 --> T["then the pipeline is NOT a choice:<br/>everyone builds pattern 21 —<br/>only tuple/vector/compiled varies"]
    T --> V["and verification is identical for all:<br/>result multisets against a pre-paid<br/>oracle (TCK / W3C / process tests)"]
```

## 10. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java` | record chains (20) |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/cypher/cypher-logical-plans/src/main/scala/org/neo4j/cypher/internal/logical/plans/LogicalPlan.scala` | plan-as-data (21) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/csr_node_group.h` | packed disk CSR (20) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/processor/operator/physical_operator.h` | vectorized pull (21) |
| memgraph | `reference-repos-competitors/memgraph-src/src/query/plan/operator.hpp` | tuple pull (21) |
| janusgraph | `reference-repos-competitors/janusgraph-src/janusgraph-core/src/main/java/org/janusgraph/graphdb/database/EdgeSerializer.java` | KV camp (20) |
| oxigraph | `reference-repos-corpus/oxigraph-src/lib/oxigraph/src/storage/rocksdb.rs` | permutation families (22) |
| qlever | `reference-repos-corpus/qlever-src/src/index/Permutation.h` | six orders (22) |

## 11. Cross-references

- Members: `record-chain-adjacency` (20),
  `pull-operator-pipeline` (21),
  `triple-permutation-indexing` (22).
- The carry-forward sentence: a graph database is a THIN native
  core (layout + pipeline) over a THICK imported substrate.
- Next category: neo4j-ecosystem (Bolt, drivers, APOC,
  testkit), then dataflow-compute and bench-testing to close
  the corpus.

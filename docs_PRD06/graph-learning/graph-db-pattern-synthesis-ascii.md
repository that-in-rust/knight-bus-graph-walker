# Graph DB Pattern Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `graph-db-pattern-synthesis-ascii.md` / `graph-db-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 20-22 into the category's thesis: a graph database = one adjacency-layout decision + one query pipeline, and everything else it does is imported wholesale from the other categories of this corpus |

## 1. The category in one sentence

Strip any graph database and you find exactly two native
inventions — WHERE the edges live (20, 22) and HOW a declarative
pattern becomes pulls over them (21). The rest — LSM/WAL/MVCC,
dictionaries, compressed sorted blocks, batch execution — is the
storage-engine, FTS, and analytics categories, re-hosted:

```text
graph-db  =  adjacency layout        (20 chains/CSR/KV, 22 perms)
           + declarative->pull pipe  (21 parse->plan->execute)
           + imported substrate      (patterns 1,4,6,17,19)
```

## 2. The three patterns, one line each

```text
20 record-chain-adjacency     storage    where do edges live:
                                         chains (neo4j) vs disk
                                         CSR (kuzu) vs KV values
21 pull-operator-pipeline     execution  Volcano pulls: tuple
                                         (memgraph), vector
                                         (kuzu), compiled (neo4j)
22 triple-permutation-indexing storage   RDF's refusal to pick a
                                         layout: all sort orders,
                                         redundantly
```

## 3. The organizing axes

```text
axis 1 — data model as layout-forcer:
    property graph -> pick ONE adjacency layout, optimize (20)
    RDF triples    -> pick NONE, store every order (22)
    the query language follows: Cypher assumes cheap expand;
    SPARQL assumes cheap sorted range scans + merge joins
axis 2 — OLTP/OLAP fork (the corpus's recurring fork):
    chains = write-locality, no maintenance     (OLTP)
    packed CSR / permutation blocks = read-locality,
        pay-at-checkpoint                       (OLAP)
axis 3 — where the contract lives:
    layouts (20, 22) are UNOBSERVABLE; only pattern 21's result
    sets are the behavioral surface — the category's entire
    verification story hangs on this one fact
```

## 4. One query, end to end (the category's golden path)

```text
MATCH (a:Person)-[:KNOWS]->(b) WHERE b.age > 30 RETURN b.name
1. front-end parses; planner picks: label index for :Person,
   expand direction, filter placement          (21, neo4j §4)
2. ScanAllByLabel pulls node IDs — from a label index that is
   a posting list (17) or bitmap (3)
3. Expand reads adjacency: chase chains (20-neo4j), slice a
   CSR run (20-kuzu), or KV-get a serialized list (20-KV camp)
4. Filter evaluates over properties fetched via property
   chains/columns; Produce streams rows to Bolt
every step is a pattern this corpus has already named.
```

## 5. What the category imports (the dependency bill)

```text
from storage-engine: LSM (1) under oxigraph/janusgraph/dgraph;
    WAL+group commit (2) under every durable write;
    MVCC (4) for readers-don't-block-writers;
    page caches + checksummed pages (5, 6)
from FTS: term/ID dictionaries (19 = oxigraph id2str);
    posting compression (17 = QLever's permutation blocks,
    label indexes); top-k pruning (18) in graph rankers
from analytics: CSR (7) as kuzu's disk layout; batch/vector
    execution (8) in kuzu's pipeline; the GDS library runs
    patterns 9-12 ON TOP of pattern 20's storage
net: the graph-db category is the corpus's biggest IMPORTER —
its moat is the pipeline + layout pairing, not any single
structure.
```

## 6. Worked contrast — the same edge in four engines

```text
edge alice-KNOWS->bob stored as:
neo4j:     34-byte record in two doubly-linked chains (20 §2)
kuzu:      one entry in fwd CSR + one in bwd CSR, in slack (20 §3)
janusgraph: a column entry under alice's row key, serialized
           by EdgeSerializer (20 §7)
oxigraph:  9 copies — one key per permutation family (22 §3)
one fact, four physical lives; all four return the same
neighbor set — which is exactly why the differential harness
compares sets and never bytes.
```

## 7. Verification thesis for the category

```text
observability: ONLY query results (21). Layouts, plan shapes,
    EXPLAIN output, family counts — all non-contractual.
coverage: openCypher TCK (thousands of scenarios) for Cypher;
    W3C SPARQL suites for RDF; Gremlin's process tests for
    TinkerPop — this category is uniquely rich in PRE-BUILT
    oracles compared to storage/vector/FTS.
equivalence: pin ORDER BY or compare multisets; canonicalize
    RDF by triple-sorting; error TEXT is the swamp — decide
    early whether messages are contract or accident.
consequence for the docs_PRD06 rewrite thesis: graph-db is the
    category where the convergence loop has the most pre-paid
    test signal — and where the unobservable 20% (recovery,
    concurrency, memory pressure) needs the engineered
    harnesses, exactly as the thesis predicts.
```

## 8. Honest gaps

```text
not covered by 20-22 (candidates for later passes):
    - property/columnar value storage (neo4j property chains vs
      kuzu columns) — half-covered inside 20's citations
    - transactions & replication (Raft in nebula/memgraph
      enterprise; causal clustering)
    - distributed traversal (edge-cut vs vertex-cut re-enters
      from the analytics category)
    - the Bolt/PackStream wire protocol (neo4j-ecosystem
      category's turf, next batch)
    - GQL/ISO standardization and openCypher front-end reuse
```

## 9. Citing repos (category roll-up)

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

## 10. Cross-references

- Members: `record-chain-adjacency` (20), `pull-operator-
  pipeline` (21), `triple-permutation-indexing` (22).
- Prior syntheses: storage-engine (the substrate this category
  imports), graph-analytics (what runs on top), vector-ann and
  full-text-search (the sibling verticals whose structures
  reappear here as label indexes and dictionaries).
- The carry-forward sentence: a graph database is a THIN native
  core (layout + pipeline) over a THICK imported substrate —
  which is why the docs_PRD06 rewrite thesis is plausible at
  all: most of the surface is patterns with independent,
  well-understood contracts.
- Next category: neo4j-ecosystem — drivers, Bolt/PackStream,
  APOC, testkit: the wire-level observable surface.

# Pull Operator Pipeline — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `pull-operator-pipeline-ascii.md` / `pull-operator-pipeline-mermaid.md` |
| One-line job | Turn a declarative graph query into a tree of operators that PULL rows from their children — the Volcano iterator model as every graph database's execution spine, tuple-at-a-time (memgraph) or vector-at-a-time (kuzu) |

## 1. The job

Cypher/Gremlin/SPARQL say WHAT; the engine must decide HOW. Every
engine in the corpus converges on the same three-stage pipe:

```text
text --parse--> AST --plan--> logical operator tree
     --execute--> physical operators pulling from children
MATCH (a:Person)-[:KNOWS]->(b) WHERE b.age > 30 RETURN b.name
             becomes (bottom-up):
    ScanAllByLabel(a, :Person)
      -> Expand(a, KNOWS, OUT, b)
        -> Filter(b.age > 30)
          -> Produce(b.name)
```

The consumer calls next() on the ROOT; each operator recursively
pulls from its child, does its one job, yields. No intermediate
result is ever fully materialized (until a pipeline breaker —
sort, aggregate, hash-join build — forces it).

## 2. Memgraph: tuple-at-a-time Pull

```text
query/plan/operator.hpp
:77-79   every LogicalOperator produces a Cursor: "Each
         LogicalOperator must produce a concrete Cursor, which
         provides the iteration mechanism"
:95      virtual bool Pull(Frame&, ExecutionContext&) = 0
         — THE interface; true = one more row in the Frame,
         false = exhausted
:558     class ScanAll      — leaf: yields every vertex
:1031    class Expand       — pulls a row, expands one node's
                              edges, yields one row per edge
:1257    class Filter       — pulls until predicate passes
:1315    class Produce      — the root the client drains
```

The Frame is a register file: one slot per query variable;
operators read/write slots in place. One Pull = one row through
the whole tree — simple, low-latency, cache-unfriendly.

## 3. Kuzu: vector-at-a-time pull

```text
processor/operator/physical_operator.h
:130     bool getNextTuple(ExecutionContext*)     — same pull
:157     virtual bool getNextTuplesInternal(...)  — but each
         call fills a ResultSet of VALUE VECTORS, not one row
common/vector/value_vector.h:20
         "The capacity of a ValueVector is either 1 (sequence)
         or DEFAULT_VECTOR_CAPACITY" — a batch of values with
         a selection bitmask
```

One pull moves ~2K values per column: amortizes the virtual-call
overhead over the batch and lets each operator run a tight loop
over contiguous arrays — pattern 8's batch discipline inside a
query engine. Filters don't copy: they flip bits in the
selection vector.

## 4. Neo4j: the planning stage made explicit

Neo4j splits the same pipeline into separately-shipped modules —
the clearest anatomy lesson in the corpus:

```text
community/cypher/
    front-end/            parse + semantic analysis (openCypher)
    cypher-logical-plans/ the operator vocabulary:
        LogicalPlan.scala:875   case class AllNodesScan(...)
        LogicalPlan.scala:2681  case class Expand(...)
    cypher-planner/       cost-based: picks index vs scan,
                          expand direction, join order
    interpreted-runtime/  Pull-style execution of the plan
```

The logical plan is a VALUE (immutable case classes) — planners
rewrite trees; runtimes interpret or compile them. This
plan-as-data discipline is what makes EXPLAIN/PROFILE printable
and plan caches possible.

## 5. Worked example — one row through the tree

```text
graph: alice-KNOWS->bob(35), alice-KNOWS->carol(28)
Produce.Pull
  Filter.Pull
    Expand.Pull
      ScanAll.Pull -> Frame[a=alice]            (yield true)
    Expand: alice's KNOWS chain -> Frame[b=bob] (yield true)
  Filter: bob.age=35 > 30                       (yield true)
Produce -> "bob"
next Pull: Expand yields carol; Filter: 28 > 30 FAILS,
  Filter pulls again; Expand exhausted for alice;
  Expand pulls ScanAll -> next node... ScanAll exhausted
  -> false propagates up -> query done.
```

## 6. Worked example — why vectors win on scans

```text
count nodes passing a filter over 10M nodes, selectivity 10%:
tuple-at-a-time:  10M Pull chains x ~4 virtual calls = 40M
                  virtual dispatches; branch per row
vector-at-a-time: 10M / 2048 ~ 4900 pulls x 4 = ~20K virtual
                  dispatches; filter = SIMD-friendly loop
                  writing a selection bitmask; ~2000x less
                  dispatch overhead on this shape
but: single-row point lookups (OLTP expand of degree 3) gain
nothing from batching — memgraph's shape is not a mistake, it
matches its transactional workload; kuzu's matches analytics.
```

## 7. Where the corpus sits

- Tuple pull: memgraph (above), neo4j interpreted runtime,
  JanusGraph/TinkerPop traversers (Gremlin steps are the same
  iterator idea wearing traversal clothes).
- Vector pull: kuzu (above), DuckPGQ (inherits DuckDB's
  vectorized engine).
- Compiled: neo4j enterprise pipelined runtime generates
  bytecode from the same logical plans — a third execution
  strategy under an unchanged planning stage.
- The planner is where engines differ MOST (cost models, index
  selection, join enumeration); the executor is where they
  differ LEAST — everyone pulls.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| memgraph | `reference-repos-competitors/memgraph-src/src/query/plan/operator.hpp` | Cursor::Pull interface (77-95), ScanAll/Expand/Filter/Produce (558, 1031, 1257, 1315) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/processor/operator/physical_operator.h` | getNextTuple / getNextTuplesInternal (130, 157) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/common/vector/value_vector.h` | ValueVector batch capacity (20) |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/cypher/cypher-logical-plans/src/main/scala/org/neo4j/cypher/internal/logical/plans/LogicalPlan.scala` | plan-as-data: AllNodesScan (875), Expand (2681) |

## 9. Cross-references

- Sibling patterns: `record-chain-adjacency` (20 — what Expand
  actually reads), `frontier-push-pull` (8 — the batch
  discipline kuzu imports), `bm25-wand-pruning` (18 — FTS's
  scorer trees are pull pipelines with score-ordered heaps).
- Verification note (docs_PRD06 thesis): the pipeline is the
  OBSERVABLE surface — same query, same data, results must
  match across all three architectures modulo row order (pin
  ORDER BY, else compare as multisets). The openCypher TCK is
  exactly this oracle, pre-built; plan SHAPES (EXPLAIN output)
  are non-contractual and must not be diffed.
- Rewrite relevance: a Rust Neo4j rewrite needs the front-end
  (parse/semantics) bit-compatible in RESULTS only; it can pick
  tuple, vector, or compiled execution freely — the three camps
  above are proof that the contract lives at the result set.
- Next in category: property/columnar value storage, then the
  category synthesis.

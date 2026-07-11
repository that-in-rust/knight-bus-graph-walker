# Bench-Testing Category Synthesis — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `bench-testing-pattern-synthesis-ascii.md` / `bench-testing-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 27-28 into the category thesis: a database is only as correct as its cheapest oracle — the category supplies four oracle families (metamorphic identities, history models, reference validation with tolerance, ground-truth recall) that together cover semantics, concurrency, numerics, and approximation |

## 1. What the category contains

```text
27 metamorphic-oracle-testing     execution  no reference:
                                             identities +
                                             history models
28 tolerant-equivalence-validation execution reference exists:
                                             per-algorithm
                                             equality relations
witness repos: sqlancer, jepsen, ldbc_graphalytics,
ann-benchmarks (4 of the ledger's 4 bench-testing rows).
```

## 2. The category thesis

```text
"is the database correct?" decomposes by what you can trust:
    trust NOTHING            -> metamorphic identities
                                (TLP: P/NOT P/IS NULL must
                                partition; NoREC: fast==slow)
    trust a MODEL            -> history checking (Jepsen:
                                does any interleaving explain
                                the recorded ops?)
    trust a REFERENCE        -> differential validation with
                                the RIGHT equality relation
                                (Graphalytics: match/epsilon/
                                isomorphism per algorithm)
    trust GROUND TRUTH only  -> quality metrics (recall vs
                                brute-force k-NN)
each family catches bugs the others structurally cannot:
identities catch shared bugs in all implementations; history
models catch concurrency bugs invisible to any single-threaded
diff; tolerant validation catches numeric/logic divergence;
recall catches quality regressions in legal approximations.
```

## 3. The oracle-cost hierarchy

```text
cost to stand up, cheapest first:
 1. metamorphic identity     ~100 lines; needs only the SUT
    (TLPWhereOracle.java's check() is ~40 lines of logic)
 2. reference validation     needs a reference run + the
    relation taxonomy (Graphalytics rules: ~15 lines SQL each)
 3. ground-truth recall      needs precomputed exact answers
    (ann-benchmarks ships them per dataset)
 4. history checking         needs fault injection, op
    recording, and a model checker (Knossos search is
    NP-hard in general)
the hierarchy explains real-world testing budgets: everyone
should have 1; serious engines add 2-3; only systems claiming
consistency guarantees can justify 4 — and those that skip it
are exactly the ones Jepsen's reports keep embarrassing.
```

## 4. What every pattern here agrees on

```text
a) findings must ship evidence: SQLancer returns Reproducer
   objects (bugStillTriggers re-runs the exact queries);
   Graphalytics rules return counterexample ROWS (LIMIT 100);
   Jepsen returns the failing history window. A bare "FAIL"
   is not a finding.
b) the verdict is a pure function of recorded artifacts:
   (model, history) for Jepsen; (expected, actual) tables for
   Graphalytics; (ground-truth, results) for recall. Re-runs
   and better checkers stay possible forever.
c) the equality relation is spec, not plumbing: declared next
   to the algorithm (Algorithm.java:38-43), not buried in a
   test helper.
```

## 5. Worked example — one WCC bug through all four oracles

```text
bug: an engine's WCC merges two components when a vertex id
exceeds 2^31 (integer truncation in the hook step).
 1. metamorphic: relabel vertices (iso-transform) — small ids
    pass, large ids fail: caught IF the transform generator
    reaches 2^31. probabilistic.
 2. reference validation: EquivalenceValidationRule finds the
    counterexample pair (v1 in different expected classes,
    same actual class) on any dataset with large ids:
    deterministic catch, with evidence.
 3. recall: not applicable (WCC is not approximate).
 4. history checking: not applicable (single-threaded bug).
inverse example — a lost-update race under partition:
only oracle 4 can see it; 1-3 all run on quiescent state.
the two examples bracket the category: no single family is
sufficient, and each has a bug class it uniquely owns.
```

## 6. Worked example — sizing a rewrite's test budget

```text
target: Cypher+GDS rewrite parity (docs_PRD06 thesis).
    layer                     oracle family        source of
                                                   free assets
    PackStream bytes          exact diff (28-exact) neo4rs tests
    Bolt conversations        scripted (24)         boltstub
    Cypher result semantics   metamorphic TLP (27)  port ~200
                                                    lines from
                                                    SQLancer
    Cypher vs stock Neo4j     differential + 28's   TCK corpus
                              relation taxonomy
    GDS numerics              epsilon/isomorphism   Graphalytics
                              (28)                  rules as-is
    ANN procedures            recall (28)           ann-bench
                                                    datasets
    concurrency/recovery      history models (27)   Jepsen
                                                    generators
rough effort split: ~70% of parity confidence comes from the
first five rows, all of which reuse existing corpus assets;
the last row is where new engineering concentrates — matching
thesis condition 1 (observability is the expensive part).
```

## 7. Honest gaps

```text
not covered by 27-28 (later passes if wanted):
    - SQLancer's PQS (pivot-row synthesis) and CERT
      (cardinality estimation testing) in source detail
    - Jepsen's elle (transactional anomaly inference from
      dependency cycles) — the modern successor to pure
      linearizability checking
    - LDBC SNB interactive workload driver (throughput
      benchmarking, as opposed to correctness)
    - performance-regression methodology (the category is
      about correctness oracles; latency benchmarking has its
      own literature — COST, harness pitfalls)
```

## 8. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/TLPWhereOracle.java` | partition identity (27) |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/NoRECOracle.java` | fast==slow invariant (27) |
| jepsen | `reference-repos-corpus/jepsen-src/jepsen/src/jepsen/checker.clj` | history checker protocol (27) |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/domain/algorithms/Algorithm.java` | relation-per-algorithm registry (28) |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EquivalenceValidationRule.java` | partition isomorphism (28) |
| ann-benchmarks | `reference-repos-corpus/ann-benchmarks-src/ann_benchmarks/plotting/metrics.py` | distance-threshold recall (28) |

## 9. Cross-references

- Members: `metamorphic-oracle-testing` (27),
  `tolerant-equivalence-validation` (28).
- This synthesis closes the corpus categories: storage-engine,
  graph-analytics, vector-ann, full-text-search, graph-db,
  neo4j-ecosystem, dataflow-compute, bench-testing — 8 of 8.
- The category is the corpus's KEYSTONE for the rewrite
  thesis: every other category describes what to build; this
  one describes how to know it's right. The convergence loop
  (generate -> diff -> regenerate) is only as strong as the
  oracles in this category.
- Reading order for the category: TLPWhereOracle.java, then
  Graphalytics' three rule files, then metrics.py's recall,
  then checker.clj — ascending oracle cost.

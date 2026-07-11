# Tolerant Equivalence Validation — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `tolerant-equivalence-validation-ascii.md` / `tolerant-equivalence-validation-mermaid.md` |
| One-line job | Compare an implementation's output against a reference when naive equality is wrong: exact match for discrete outputs, epsilon bands for floating point, partition-isomorphism for label-renaming algorithms, and distance-threshold recall for approximate ANN — each algorithm gets the equality relation its mathematics permits |

## 1. The job

"Same answer as the reference" hides four different questions.
LDBC Graphalytics ships the cleanest taxonomy in the corpus:
per-algorithm VALIDATION RULES, declared right next to the
algorithm registry, plus ann-benchmarks' recall for the case
where the output is ALLOWED to differ.

## 2. Graphalytics: three equality relations, as SQL

```text
ldbc_graphalytics-src/graphalytics-core/src/main/java/science/
    atlarge/graphalytics/
    domain/algorithms/Algorithm.java:38-43 — the assignment:
        BFS   -> MatchLongValidationRule       exact
        CDLP  -> MatchLongValidationRule       exact
        WCC   -> EquivalenceValidationRule     isomorphism
        (PR, SSSP use the epsilon family)
    validation/rule/MatchLongValidationRule.java:
        counterexample query: expected.v = actual.v
        AND expected.x != actual.x     -- any differing vertex
    validation/rule/EpsilonValidationRule.java:
        NOT abs(expected.x - actual.x) <= 0.0001 * expected.x
        with explicit Infinity-vs-Infinity handling (equal),
        Infinity-vs-finite (bug)                -- 0.01% band
    validation/rule/EquivalenceValidationRule.java:
        find v1, v2 in the SAME expected class but DIFFERENT
        actual classes, or vice versa; LIMIT 1 counterexample
        -- labels may be renamed; the PARTITION must agree
all three are "find 100 counterexamples" queries — validation
failure comes with its own evidence rows.
```

## 3. Why each algorithm gets its relation

```text
BFS distances:   deterministic integers      -> exact match
WCC components:  label VALUES are arbitrary  -> isomorphism
                 (component 7 vs component 42: same partition)
PageRank/SSSP:   floating point, order of    -> epsilon band,
                 summation varies               relative 1e-4
this is docs_PRD06's "canonicalization" made concrete: the
equality relation is part of the algorithm's SPEC, not a
test-harness afterthought. Get it wrong either way and you
lose: too strict = false alarms on legal nondeterminism;
too loose = real bugs pass.
```

## 4. ann-benchmarks: equality becomes recall

```text
ann-benchmarks-src/ann_benchmarks/plotting/metrics.py
    :6  knn_threshold(data, count, eps) =
            data[count-1] + eps        -- k-th TRUE distance
    :14-23 get_recall_values:
        for each query: count returned neighbors with
        distance <= threshold; recall = mean(actual)/k
key subtlety: recall counts DISTANCE-equivalent answers, not
identity. If the true 10th neighbor is at distance 0.731 and
the index returns a different point also at <= 0.731 + eps,
that counts — ties and near-ties don't punish the index for
picking an equally good answer. Identity-based recall would
systematically under-score on datasets with duplicates.
```

## 5. Worked example — WCC isomorphism in action

```text
expected labels: {a:1, b:1, c:2, d:2}
actual labels:   {a:9, b:9, c:4, d:4}
Match rule:      4 mismatches -> FAIL (wrong relation!)
Equivalence:     partition expected {ab},{cd} ==
                 partition actual   {ab},{cd} -> PASS
actual2:         {a:9, b:4, c:4, d:4}
Equivalence:     b,c same class in actual (4) but different
                 in expected (1 vs 2) -> counterexample (b,c)
                 -> FAIL, with the offending pair as evidence
```

## 6. Worked example — epsilon band arithmetic

```text
PageRank on the same graph, two engines, damping 0.85:
expected pr(v) = 0.0312500
actual   pr(v) = 0.0312531
band: 0.0001 * 0.0312500 = 0.0000031
|diff| = 0.0000031  -> exactly at the boundary: <= holds, PASS
actual2 pr(v) = 0.0312600: |diff| = 0.0000100 > band -> FAIL
note the band is RELATIVE — high-rank vertices tolerate more
absolute drift than tail vertices, matching how summation
error actually accumulates.
SSSP: unreachable = Infinity; Infinity==Infinity passes,
Infinity vs 2^63-scale sentinel value fails — sentinel
encodings are the classic cross-engine parity trap.
```

## 7. Why this matters for the corpus

The docs_PRD06 convergence loop is differential testing
against stock Neo4j; this pattern is the loop's most
important dial. GDS parity needs exactly Graphalytics'
taxonomy: WCC/Louvain compared by partition (Louvain also
needs modularity-quality bands since it is nondeterministic),
PageRank by relative epsilon, BFS/SSSP distances exactly
(but not paths — multiple shortest paths are legal), ANN
procedures by recall. The equality relation per procedure IS
the rewrite's acceptance spec.

## 7b. Choosing the relation — a decision walk

```text
for each output column of an algorithm, ask:
1. is the value deterministic given the input?
   yes, integer/discrete           -> exact Match
   yes, floating point             -> relative epsilon
      (pick the band from the algorithm's numeric analysis;
      Graphalytics uses 1e-4 relative — wide enough for
      summation-order drift, tight enough for logic bugs)
2. is the value an arbitrary representative of a class?
   (component labels, community ids, cluster ids)
                                   -> partition isomorphism
3. is the algorithm approximate BY CONTRACT?
   (ANN, sketches, sampling)       -> quality metric vs
                                      ground truth (recall,
                                      error bound), with the
                                      threshold in the spec
4. are there sentinel values? (unreachable = Inf, missing =
   NULL, undefined = -1)           -> enumerate them and
                                      test each pairing
                                      EXPLICITLY — this is
                                      where engines disagree
then write the counterexample query FIRST: if you cannot
express "find a vertex that violates the relation" as a
query, the relation is not yet precise enough to be a gate.
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/domain/algorithms/Algorithm.java` | rule-per-algorithm registry (38-43) |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EpsilonValidationRule.java` | relative 1e-4 band + Infinity cases |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EquivalenceValidationRule.java` | partition isomorphism counterexample |
| ann-benchmarks | `reference-repos-corpus/ann-benchmarks-src/ann_benchmarks/plotting/metrics.py` | distance-threshold recall (6-23) |

## 9. Cross-references

- Sibling pattern: `metamorphic-oracle-testing` (27 — when no
  reference exists at all).
- Kin: `graph-analytics-pattern-synthesis` (the algorithms
  whose outputs these rules classify), `hnsw-layered-descent`
  and the vector synthesis (recall as the primary quality
  axis), `component-hooking-shortcutting` (WCC — the
  isomorphism case).
- Papers ledger: LDBC Graphalytics paper; ann-benchmarks
  paper (Aumüller et al.).
- The pattern's one-line moral: "equal" is a per-algorithm
  design decision with a counterexample query attached —
  never a default `==`.
- Next: the bench-testing synthesis closes the final corpus
  category.

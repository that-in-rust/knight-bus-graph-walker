# Tolerant Equivalence Validation — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `tolerant-equivalence-validation-ascii.md` / `tolerant-equivalence-validation-mermaid.md` |
| One-line job | Compare an implementation's output against a reference when naive equality is wrong: exact match for discrete outputs, epsilon bands for floating point, partition-isomorphism for label-renaming algorithms, and distance-threshold recall for approximate ANN — each algorithm gets the equality relation its mathematics permits |

## 1. Four questions hiding in "same answer"

```mermaid
flowchart TD
    Q["is actual == expected?"]
    Q --> E1["discrete + deterministic<br/>(BFS distances) -> EXACT match"]
    Q --> E2["labels arbitrary<br/>(WCC components) -> partition<br/>ISOMORPHISM"]
    Q --> E3["floating point<br/>(PageRank, SSSP) -> relative<br/>EPSILON band"]
    Q --> E4["approximate by design<br/>(ANN) -> RECALL vs<br/>ground truth"]
    E1 & E2 & E3 & E4 --> SPEC["the equality relation is part of the<br/>algorithm's SPEC, not a harness detail"]
```

## 2. Graphalytics: rules declared beside algorithms

```mermaid
flowchart LR
    ALG["Algorithm.java:38-43<br/>(the registry)"]
    ALG --> BFS["BFS, CDLP -><br/>MatchLongValidationRule"]
    ALG --> WCC["WCC -><br/>EquivalenceValidationRule"]
    ALG --> PR["PR, SSSP -><br/>epsilon family"]
    BFS & WCC & PR --> CE["every rule is a SQL query that finds<br/>up to 100 COUNTEREXAMPLES —<br/>a failure ships its own evidence rows"]
```

## 3. The three rules as counterexample queries

```mermaid
flowchart TD
    M["Match: expected.v = actual.v<br/>AND expected.x != actual.x"]
    EP["Epsilon: NOT |expected.x - actual.x|<br/><= 0.0001 * expected.x;<br/>Inf==Inf passes, Inf vs finite fails"]
    EQ["Equivalence: find v1, v2 in the SAME<br/>expected class but DIFFERENT actual<br/>classes (or vice versa) — LIMIT 1<br/>counterexample suffices"]
    M --- ALL["all three: 'prove me wrong'<br/>queries over (expected, actual)<br/>tables — validation as SQL"]
    EP --- ALL
    EQ --- ALL
```

## 4. ann-benchmarks: equality becomes recall

```mermaid
flowchart TD
    GT["ground truth: exact k-NN distances<br/>per query (brute force, precomputed)"]
    GT --> TH["knn_threshold (metrics.py:6):<br/>k-th TRUE distance + epsilon"]
    TH --> RC["get_recall_values (:14-23):<br/>count returned neighbors with<br/>distance <= threshold;<br/>recall = mean / k"]
    RC --> SUB["subtlety: counts DISTANCE-equivalent<br/>answers, not identity — a different<br/>point at the same distance is a<br/>correct answer; identity-recall would<br/>under-score datasets with duplicates"]
```

## 5. Worked example — WCC isomorphism

```mermaid
flowchart TD
    X["expected {a:1, b:1, c:2, d:2}<br/>actual {a:9, b:9, c:4, d:4}"]
    X --> W["Match rule: 4 'mismatches' -> FAIL<br/>(wrong relation for WCC!)"]
    X --> R["Equivalence rule: partitions<br/>{ab},{cd} == {ab},{cd} -> PASS"]
    X2["actual2 {a:9, b:4, c:4, d:4}"]
    X2 --> R2["b,c same actual class (4), different<br/>expected classes (1 vs 2) -><br/>counterexample (b,c) -> FAIL"]
```

## 6. Worked example — epsilon band arithmetic

```mermaid
flowchart TD
    P["expected pr(v) = 0.0312500;<br/>band = 1e-4 relative = 0.0000031"]
    P --> A1["actual 0.0312531:<br/>|diff| = 0.0000031 —<br/>boundary, <= holds -> PASS"]
    P --> A2["actual 0.0312600:<br/>|diff| = 0.0000100 -> FAIL"]
    P --> REL["band is RELATIVE: high-rank vertices<br/>tolerate more absolute drift —<br/>matching how summation error<br/>actually accumulates"]
    P --> INF["SSSP: Inf==Inf passes;<br/>Inf vs a 2^63 sentinel fails —<br/>sentinel encodings are the classic<br/>cross-engine parity trap"]
```

## 7. The dial on the convergence loop

```mermaid
flowchart LR
    LOOP["docs_PRD06 loop:<br/>rewrite output vs stock Neo4j"]
    LOOP --> D["this pattern is the loop's<br/>most important dial"]
    D --> T1["WCC/Louvain: partition isomorphism<br/>(+ modularity band — Louvain is<br/>nondeterministic)"]
    D --> T2["PageRank: relative epsilon"]
    D --> T3["BFS/SSSP: distances exact,<br/>paths NOT (multiple shortest<br/>paths are legal)"]
    D --> T4["ANN procedures: recall"]
    T1 & T2 & T3 & T4 --> ACC["the equality relation per procedure<br/>IS the rewrite's acceptance spec"]
```

## 8. Failure modes of getting the relation wrong

```mermaid
flowchart TD
    WRONG["mis-chosen equality relation"]
    WRONG --> TS["too strict (Match on WCC labels):<br/>false alarms on legal nondeterminism —<br/>the harness cries wolf and gets ignored"]
    WRONG --> TL["too loose (epsilon on BFS integers):<br/>real off-by-one bugs sail through"]
    TS & TL --> BAL["Graphalytics' answer: declare the<br/>relation in the algorithm registry,<br/>reviewed as part of the algorithm —<br/>not buried in a test helper"]
```

## 8b. Choosing the relation — a decision walk

```mermaid
flowchart TD
    C["for each output column"]
    C --> D1{"deterministic given<br/>the input?"}
    D1 -->|"yes, discrete"| M1["exact Match"]
    D1 -->|"yes, float"| M2["relative epsilon — band from the<br/>algorithm's numeric analysis;<br/>Graphalytics: 1e-4 relative"]
    D1 -->|no| D2{"arbitrary class<br/>representative?<br/>(labels, community ids)"}
    D2 -->|yes| M3["partition isomorphism"]
    D2 -->|no| D3{"approximate BY<br/>CONTRACT? (ANN,<br/>sketches)"}
    D3 -->|yes| M4["quality metric vs ground truth<br/>(recall, error bound) — threshold<br/>lives in the spec"]
    M1 & M2 & M3 & M4 --> SEN["then enumerate sentinels (Inf, NULL,<br/>-1) and test each pairing explicitly —<br/>where engines actually disagree"]
    SEN --> CEQ["finally: write the counterexample<br/>query FIRST — if 'find a violating<br/>vertex' can't be expressed as a query,<br/>the relation isn't precise enough<br/>to be a gate"]
```

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/domain/algorithms/Algorithm.java` | rule-per-algorithm registry (38-43) |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EpsilonValidationRule.java` | relative 1e-4 band + Infinity cases |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EquivalenceValidationRule.java` | partition isomorphism counterexample |
| ann-benchmarks | `reference-repos-corpus/ann-benchmarks-src/ann_benchmarks/plotting/metrics.py` | distance-threshold recall (6-23) |

## 10. Cross-references

- Sibling pattern: `metamorphic-oracle-testing` (27 — no
  reference at all); together they cover the oracle spectrum.
- Kin: `component-hooking-shortcutting` (WCC — the isomorphism
  case), the vector synthesis (recall as the quality axis),
  `graph-analytics-pattern-synthesis`.
- Papers ledger: LDBC Graphalytics; ann-benchmarks
  (Aumüller et al.).
- The moral, one line: "equal" is a per-algorithm design
  decision with a counterexample query attached — never a
  default `==`.
- The ASCII twin adds the same worked examples with full
  arithmetic and the sentence-form rule taxonomy.
- Terminology bridge: "validation rule" (Graphalytics) =
  "equality relation" (this doc) = "acceptance predicate"
  (spec language); "ground truth" (ann-benchmarks) = the
  reference outputs a rule compares against; "counterexample
  query" = the rule expressed as a search for violations
  rather than a proof of agreement.
- Implementation note: Graphalytics runs its rules as SQL over
  (expected, actual) vertex tables — meaning any engine that
  can dump `(vertex, value)` pairs gets validation for free,
  no per-engine checker code. The rules live in ~15 lines of
  SQL each; the taxonomy, not the code, is the asset.
- Nondeterminism boundary: these rules assume a deterministic
  reference. For Louvain-class algorithms even the reference
  varies run to run — there the relation must weaken further,
  to quality bands (modularity within delta of the reference's
  distribution), which is pattern 27's territory: an identity
  about the OUTPUT's quality, not its values.

# Bench-Testing Category Synthesis — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `bench-testing-pattern-synthesis-ascii.md` / `bench-testing-pattern-synthesis-mermaid.md` |
| One-line job | Roll up patterns 27-28 into the category thesis: a database is only as correct as its cheapest oracle — the category supplies four oracle families (metamorphic identities, history models, reference validation with tolerance, ground-truth recall) that together cover semantics, concurrency, numerics, and approximation |

## 1. Category map

```mermaid
flowchart TD
    CAT["bench-testing category<br/>(4 ledger rows, all witnessed)"]
    CAT --> P27["27 metamorphic-oracle-testing:<br/>no reference — identities (SQLancer<br/>TLP/NoREC) + history models (Jepsen)"]
    CAT --> P28["28 tolerant-equivalence-validation:<br/>reference exists — per-algorithm<br/>equality relations (Graphalytics) +<br/>ground-truth recall (ann-benchmarks)"]
```

## 2. The thesis: correctness decomposes by what you trust

```mermaid
flowchart TD
    Q["is the database correct?"]
    Q --> T0["trust NOTHING -><br/>metamorphic identities:<br/>TLP partition, NoREC fast==slow"]
    Q --> TM["trust a MODEL -><br/>history checking: does ANY<br/>interleaving explain the ops?"]
    Q --> TR["trust a REFERENCE -><br/>differential validation with the<br/>RIGHT relation: match / epsilon /<br/>isomorphism per algorithm"]
    Q --> TG["trust GROUND TRUTH -><br/>quality metrics: recall vs<br/>brute-force k-NN"]
    T0 & TM & TR & TG --> DIS["each family catches bugs the others<br/>structurally cannot — identities catch<br/>SHARED bugs; models catch concurrency;<br/>relations catch numeric drift; recall<br/>catches quality regressions"]
```

## 3. The oracle-cost hierarchy

```mermaid
flowchart LR
    C1["1. metamorphic identity<br/>~100 lines, SUT only"]
    C2["2. reference validation<br/>reference run + relation<br/>taxonomy (~15-line SQL rules)"]
    C3["3. ground-truth recall<br/>precomputed exact answers"]
    C4["4. history checking<br/>fault injection + recording +<br/>model search (NP-hard core)"]
    C1 --> C2 --> C3 --> C4
    C4 --> BUD["real-world budgets follow the ladder:<br/>everyone should have 1; serious engines<br/>add 2-3; only consistency-claiming<br/>systems can justify 4 — and skipping it<br/>is what Jepsen reports keep punishing"]
```

## 4. The three category-wide laws

```mermaid
flowchart TD
    L1["findings ship EVIDENCE:<br/>SQLancer Reproducer re-runs itself;<br/>Graphalytics returns counterexample<br/>rows; Jepsen returns the failing<br/>history window"]
    L2["verdicts are PURE FUNCTIONS of<br/>recorded artifacts: (model, history),<br/>(expected, actual), (truth, results) —<br/>re-checkable forever"]
    L3["the equality relation is SPEC:<br/>declared beside the algorithm<br/>(Algorithm.java:38-43), not buried<br/>in a test helper"]
    L1 & L2 & L3 --> K["together: testing as durable<br/>infrastructure, not one-shot scripts"]
```

## 5. One WCC bug through all four oracles

```mermaid
flowchart TD
    BUG["bug: WCC merges components when<br/>vertex id > 2^31 (hook truncation)"]
    BUG --> O1["metamorphic relabeling: caught IF the<br/>generator reaches large ids —<br/>probabilistic"]
    BUG --> O2["EquivalenceValidationRule: finds the<br/>violating pair on any large-id dataset —<br/>deterministic, with evidence"]
    BUG --> O3["recall: n/a (WCC not approximate)"]
    BUG --> O4["history checking: n/a<br/>(single-threaded bug)"]
    INV["inverse: lost-update race under<br/>partition — ONLY oracle 4 sees it;<br/>1-3 run on quiescent state"]
    O2 --> BR["the two examples bracket the category:<br/>no family suffices alone; each owns<br/>a bug class uniquely"]
    INV --> BR
```

## 6. Sizing a rewrite's test budget

```mermaid
flowchart TD
    TGT["Cypher+GDS rewrite parity<br/>(docs_PRD06 thesis)"]
    TGT --> R1["PackStream bytes -> exact diff;<br/>free assets: neo4rs tests"]
    TGT --> R2["Bolt conversations -> scripts (24);<br/>free: boltstub corpus"]
    TGT --> R3["Cypher semantics -> TLP port (27);<br/>~200 lines from SQLancer"]
    TGT --> R4["Cypher vs stock -> differential +<br/>28's relations; free: TCK"]
    TGT --> R5["GDS numerics -> epsilon/isomorphism;<br/>free: Graphalytics rules as-is"]
    TGT --> R6["ANN -> recall; free:<br/>ann-benchmarks datasets"]
    TGT --> R7["concurrency/recovery -> history<br/>models; NEW engineering here"]
    R1 & R2 & R3 & R4 & R5 & R6 --> SPLIT["~70% of parity confidence reuses<br/>existing corpus assets"]
    R7 --> SPLIT2["the expensive residue = thesis<br/>condition 1: observability"]
```

## 7. Honest gaps

```mermaid
flowchart TD
    G["not covered by 27-28"]
    G --> G1["SQLancer PQS (pivot-row synthesis)<br/>and CERT (cardinality testing)<br/>in source detail"]
    G --> G2["Jepsen's elle: transactional anomaly<br/>inference from dependency cycles —<br/>the modern successor to pure<br/>linearizability checking"]
    G --> G3["LDBC SNB interactive driver<br/>(throughput, not correctness)"]
    G --> G4["performance-regression methodology<br/>(COST, harness pitfalls) — its own<br/>literature"]
```

## 8. The corpus, closed

```mermaid
flowchart LR
    S1["storage-engine"] --> S2["graph-analytics"] --> S3["vector-ann"] --> S4["full-text-search"]
    S4 --> S5["graph-db"] --> S6["neo4j-ecosystem"] --> S7["dataflow-compute"] --> S8["bench-testing ✓"]
    S8 --> KEY["8 of 8 categories synthesized.<br/>bench-testing is the KEYSTONE: every<br/>other category says what to build;<br/>this one says how to know it's right —<br/>the convergence loop is only as strong<br/>as these oracles"]
```

## 8b. Which oracle first? — a decision walk

```mermaid
flowchart TD
    START["new engine (or rewrite) needs<br/>its first correctness gate"]
    START --> D1{"does a trusted<br/>reference exist?"}
    D1 -->|yes| REF["start with differential validation —<br/>but choose the relation per output<br/>(28's decision walk) BEFORE running,<br/>or false alarms will bury real bugs"]
    D1 -->|no| MET["start with metamorphic identities —<br/>TLP-style partitions cost ~a day and<br/>run against nothing but the SUT"]
    REF --> D2{"claims about<br/>concurrency or<br/>durability?"}
    MET --> D2
    D2 -->|yes| HIS["add history checking with fault<br/>injection — nothing else can see<br/>these bug classes"]
    D2 -->|no| APX{"approximate<br/>components (ANN,<br/>sketches)?"}
    HIS --> APX
    APX -->|yes| REC["add ground-truth quality metrics<br/>with thresholds written into the spec"]
    APX -->|no| DONE["gate CI on what you have;<br/>every finding must carry a reproducer"]
    REC --> DONE
```

## 9. Citing repos (category roll-up)

| Repo | Path | Role |
| --- | --- | --- |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/TLPWhereOracle.java` | partition identity (27) |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/NoRECOracle.java` | fast==slow invariant (27) |
| jepsen | `reference-repos-corpus/jepsen-src/jepsen/src/jepsen/checker.clj` | history checker protocol (27) |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/domain/algorithms/Algorithm.java` | relation-per-algorithm registry (28) |
| ldbc_graphalytics | `reference-repos-corpus/ldbc_graphalytics-src/graphalytics-core/src/main/java/science/atlarge/graphalytics/validation/rule/EquivalenceValidationRule.java` | partition isomorphism (28) |
| ann-benchmarks | `reference-repos-corpus/ann-benchmarks-src/ann_benchmarks/plotting/metrics.py` | distance-threshold recall (28) |

## 10. Cross-references

- Members: `metamorphic-oracle-testing` (27),
  `tolerant-equivalence-validation` (28).
- Prior syntheses: all seven other categories — this one
  supplies the verification lens they are read through.
- Reading order for the category: TLPWhereOracle.java, then
  Graphalytics' three rule files, then metrics.py's recall,
  then checker.clj — ascending oracle cost.
- The ASCII twin carries the same two worked examples with
  full detail plus the test-budget table in row form.
- Terminology bridge across the category: "oracle" = any
  machine-checkable verdict source; "metamorphic relation" =
  transformation with a known output effect; "validation
  rule" = equality relation as a counterexample query;
  "ground truth" = precomputed exact answers; "history" =
  the recorded op sequence that verdicts are computed from.
- Category exam question: given a claimed behavior, name the
  cheapest oracle family that can falsify it, the free corpus
  asset that implements it, and the evidence artifact a
  failure must produce. All three answers come from the
  ladder in §3 and the budget table in §6.

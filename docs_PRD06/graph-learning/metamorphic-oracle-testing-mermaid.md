# Metamorphic Oracle Testing — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `metamorphic-oracle-testing-ascii.md` / `metamorphic-oracle-testing-mermaid.md` |
| One-line job | Find correctness bugs WITHOUT a reference implementation: derive queries that must agree by construction (TLP's P/NOT P/IS NULL partition, NoREC's optimized-vs-unoptimized count) or check recorded histories against an abstract model (Jepsen) — the oracle is a mathematical identity, not a second system |

## 1. The oracle problem and its two escapes

```mermaid
flowchart TD
    Q["'is this answer right?'<br/>— unknowable without a<br/>trusted reference"]
    Q --> M["METAMORPHIC (SQLancer):<br/>transform the query so the answer<br/>provably CANNOT change;<br/>any change = bug"]
    Q --> H["HISTORY-BASED (Jepsen):<br/>record every operation; check the<br/>history against an abstract model's<br/>allowed behaviors"]
    M & H --> T["both convert 'right?' into<br/>'equal?' / 'permitted?' —<br/>questions a machine can answer"]
```

## 2. TLP: ternary logic partitioning

```mermaid
flowchart TD
    O["Q: SELECT ... (no WHERE)<br/>(TLPWhereOracle.java:88)"]
    O --> P1["Q1: WHERE P"]
    O --> P2["Q2: WHERE NOT P"]
    O --> P3["Q3: WHERE P IS NULL"]
    P1 & P2 & P3 --> EQ["rows(Q) == rows(Q1)+rows(Q2)+rows(Q3)<br/>(:100-114, assumeResultSetsAreEqual)"]
    EQ --> WHY["works because SQL logic is 3-valued:<br/>every row makes P true/false/NULL —<br/>a PARTITION. Each variant stresses a<br/>different optimizer path; disagreement<br/>= optimizer bug. Cypher WHERE is also<br/>3-valued: ports directly to graphs"]
```

## 3. NoREC: conjure a second implementation by syntax

```mermaid
flowchart LR
    F["FAST: SELECT * FROM t WHERE P<br/>-> count rows<br/>(index paths, pushdowns)"]
    S["SLOW: SELECT (P IS TRUE) FROM t<br/>-> sum trues<br/>(predicate in projection:<br/>optimizer takes the naive path)"]
    F <-->|"counts MUST match<br/>(NoRECOracle.java:35-45, :73)"| S
    S --> IN["differential testing where both sides<br/>are the SAME engine — the 'reference<br/>implementation' is the engine with its<br/>own optimizer defeated"]
```

## 4. Jepsen: verification as a pure function

```mermaid
flowchart TD
    G["generators: concurrent ops +<br/>fault injection (partitions,<br/>clock skew, crashes)"]
    G --> SYS["system under test"]
    SYS --> HIS["recorded HISTORY —<br/>the durable artifact"]
    HIS --> CHK["Checker protocol (checker.clj:59-74):<br/>check(model, history) -><br/>{:valid? true/false/:unknown}"]
    CHK --> LIN["linearizable (:285-296) -> Knossos:<br/>does ANY interleaving explain the<br/>history under the model?"]
    CHK --> SAFE["check-safe (:87): checker crashes<br/>become :unknown, never false verdicts"]
    HIS --> RE["offline + pure -> reproducible;<br/>re-checkable years later<br/>with better checkers"]
```

## 5. Worked example — TLP catches a NULL bug

```mermaid
flowchart TD
    T["t.x = {1, 2, NULL}; P: x > 1"]
    T --> R0["Q: 3 rows"]
    T --> R1["Q1 (P): {2} — 1 row"]
    T --> R2["Q2 (NOT P): {1} — 1 row"]
    T --> R3["Q3 (P IS NULL): {NULL} — 1 row"]
    R1 & R2 & R3 --> OK["1+1+1 = 3 ✓"]
    OK --> BUG["buggy engine treating NULL as false<br/>inside NOT: Q2 = {1, NULL} -><br/>sum 4 != 3 -> BUG found with zero<br/>knowledge of the 'right' answer"]
    BUG --> CY["same identity in Cypher = a free<br/>oracle for a rewrite's WHERE semantics —<br/>NULL handling is exactly where the<br/>docs_PRD06 thesis says 'equal'<br/>needs a definition"]
```

## 6. Worked example — a non-linearizable history

```mermaid
sequenceDiagram
    participant A as client A
    participant R as register
    participant B as client B
    A->>R: write 1 (ok)
    B->>R: read -> 2 (ok)
    Note over R: nobody ever wrote 2
    Note over R: Knossos: no sequential order of<br/>{write 1} yields read=2 -><br/>{:valid? false} — the checker only<br/>knew the MODEL's rule, never the<br/>'right' value
```

## 7. Where each oracle family applies

```mermaid
flowchart TD
    X["behavior class"]
    X --> A1["query semantics -> metamorphic<br/>(TLP/NoREC): identities hold even if<br/>EVERY implementation shares a bug"]
    X --> A2["concurrency + fault semantics -><br/>history checking: invisible to query<br/>diffing; needs fault injection +<br/>offline models"]
    X --> A3["plain functional parity -><br/>differential vs a reference<br/>(the docs_PRD06 loop; pattern 28's<br/>tolerance rules)"]
    A1 & A2 & A3 --> STACK["a serious engine runs all three —<br/>they catch disjoint bug classes"]
```

## 8. The rewrite test stack, extended

```mermaid
flowchart LR
    L1["bytes: PackStream<br/>round-trip (23)"]
    L2["conversations:<br/>boltstub scripts (24)"]
    L3["results: differential<br/>vs stock Neo4j"]
    L4["semantics: TLP/NoREC<br/>ported to Cypher"]
    L5["histories: Jepsen-style<br/>fault runs over Bolt"]
    L1 --> L2 --> L3 --> L4 --> L5
    L5 --> NOTE["L4-L5 are the layers where the<br/>stock oracle CAN'T help: identities<br/>and models replace it"]
```

## 8b. Reproducers — a finding must re-run itself

```mermaid
flowchart TD
    F["oracle disagreement found"]
    F --> RP["Reproducer object, not a boolean<br/>(TLPWhereOracle.java:32-48,<br/>NoRECOracle.java:35-45):<br/>bugStillTriggers(state) re-runs the<br/>exact queries"]
    RP --> RED["enables automated REDUCTION:<br/>mutate schema/query smaller, keep the<br/>mutation iff the bug still triggers -><br/>minimal script for the maintainer"]
    RP --> JA["Jepsen analogue: the history FILE is<br/>the reproducer; :valid? false carries<br/>the failing operation window"]
    RED & JA --> LAW["harness law: a red test that can't<br/>re-run itself in isolation is a rumor —<br/>build the reproducer into the oracle<br/>from day one"]
```

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/TLPWhereOracle.java` | ternary partition identity (88-114) |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/NoRECOracle.java` | optimized-vs-unoptimized invariant (35-73) |
| jepsen | `reference-repos-corpus/jepsen-src/jepsen/src/jepsen/checker.clj` | Checker protocol, linearizable/Knossos (59-296) |

## 10. Cross-references

- Sibling pattern: `tolerant-equivalence-validation` (28 —
  when a reference exists but equality needs tolerance).
- Kin: `stub-script-conformance` (24) oracles WIRE behavior;
  this pattern oracles SEMANTICS; 28 oracles NUMERICS.
- Papers ledger: PQS/NoREC/TLP papers; Jepsen's decade of
  per-database analyses.
- Reading order: TLPWhereOracle.java first (the identity is
  visible in ~30 lines of the check method), then NoREC's
  Reproducer, then checker.clj's protocol docstring — three
  files, three complete testing philosophies.
- Terminology bridge: "metamorphic relation" = a transformation
  with a known effect on the output (here: none); "oracle" =
  any machine-checkable verdict source; "linearizable" = the
  history has SOME sequential explanation respecting real-time
  order; "reproducer" = the finding packaged as a re-runnable
  check.
- Scope note: SQLancer's common/oracle also holds PQS
  (PivotedQuerySynthesisBase.java — pick a row, synthesize a
  query that MUST return it), CERT (cardinality-estimation
  restriction testing) and CODDTest — the same identity-based
  idea aimed at different engine subsystems; TLP and NoREC are
  the two with the highest bug yield per line of harness.
- The ASCII twin adds the reproducer-and-reduction walk in
  prose plus the numeric TLP example (x = {1, 2, NULL}) traced
  row by row through all three partitions.

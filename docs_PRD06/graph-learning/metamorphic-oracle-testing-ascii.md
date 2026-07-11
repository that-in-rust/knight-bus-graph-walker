# Metamorphic Oracle Testing — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `metamorphic-oracle-testing-ascii.md` / `metamorphic-oracle-testing-mermaid.md` |
| One-line job | Find correctness bugs WITHOUT a reference implementation: derive queries that must agree by construction (TLP's P/NOT P/IS NULL partition, NoREC's optimized-vs-unoptimized count) or check recorded histories against an abstract model (Jepsen) — the oracle is a mathematical identity, not a second system |

## 1. The job

Differential testing needs a trusted second implementation.
When none exists (or all implementations share bugs), two
families of self-oracles remain:
- METAMORPHIC: transform a query so the answer provably cannot
  change; any change is a bug (SQLancer).
- HISTORY-BASED: record every operation and check the recorded
  history against an abstract model's allowed behaviors
  (Jepsen).
Both turn "is the answer right?" (unknowable) into "are these
two things equal / is this sequence permitted?" (checkable).

## 2. TLP: ternary logic partitioning

```text
sqlancer-src/src/sqlancer/common/oracle/TLPWhereOracle.java
    :88  select.setWhereClause(null)      Q: no predicate
    :100-107 the three partitions:
        Q1: WHERE  P          (predicates.predicate)
        Q2: WHERE  NOT P      (negatedPredicate)
        Q3: WHERE  P IS NULL  (isNullPredicate)
    :109-114 rows(Q) must equal rows(Q1)+rows(Q2)+rows(Q3)
        (getCombinedResultSet -> assumeResultSetsAreEqual)
why it works: SQL's THREE-valued logic — every row makes P
true, false, or NULL; the three cases partition the table.
why it finds bugs: each variant stresses different optimizer
paths (index usage for P, negation pushdown for NOT P, null
handling for IS NULL); disagreement = optimizer bug.
Cypher has the same three-valued WHERE — TLP ports directly
to graph databases.
```

## 3. NoREC: defeating the optimizer on purpose

```text
sqlancer-src/src/sqlancer/common/oracle/NoRECOracle.java
    :35-45 the invariant, as a Reproducer:
        optimizedQuery.apply(g) == unoptimizedQuery.apply(g)
    :73   optimized:   SELECT * FROM t WHERE P      (count rows)
          unoptimized: SELECT (P IS TRUE) FROM t    (sum trues)
the second form moves P from WHERE to the projection — most
optimizers won't use indexes/pushdowns for it, so it takes the
naive path. Same count required; a mismatch means the FAST
path computes a different relation than the SLOW path.
This is differential testing where both sides are THE SAME
ENGINE — the second implementation is conjured by disabling
optimization through syntax.
```

## 4. Jepsen: the history is the artifact

```text
jepsen-src/jepsen/src/jepsen/checker.clj
    :59-74  (defprotocol Checker (check [checker test history
            opts] -> {:valid? true|false|:unknown ...}))
    :87     check-safe — checker crashes become :unknown, not
            false positives
    :285-296 (linearizable {:model ... :algorithm :wgl})
            delegates to Knossos: does ANY interleaving of the
            concurrent ops explain the observed history under
            the model (e.g. a CAS register)?
the design separation: GENERATORS produce concurrent ops +
fault injection (partitions, clock skew); the CHECKER later
judges the recorded history offline. Verification is a pure
function of (model, history) — reproducible, re-checkable
with better checkers years later.
```

## 5. Worked example — TLP catches a real class of bug

```text
table t: x = {1, 2, NULL}. P: x > 1.
    Q  (no WHERE):            3 rows
    Q1 WHERE x > 1:           1 row   {2}
    Q2 WHERE NOT (x > 1):     1 row   {1}
    Q3 WHERE (x > 1) IS NULL: 1 row   {NULL}
    1 + 1 + 1 = 3  ✓
buggy engine that treats NULL comparison as false in NOT:
    Q2 returns {1, NULL} -> partition sums to 4 != 3 -> BUG,
    found with zero knowledge of what the right answer "is".
the same identity in Cypher: MATCH (n) vs the three WHERE
variants — a direct oracle for a Cypher rewrite's WHERE
semantics (docs_PRD06 thesis, condition 3: NULL handling is
exactly where "equal" needs definition).
```

## 6. Worked example — a non-linearizable history

```text
concurrent CAS register, history:
    A: write 1        ok
    B: read  -> 2     ok        <- nobody ever wrote 2
Knossos search: no sequential ordering of {write 1} yields
read=2 -> {:valid? false}. The checker never knew the "right"
value; it only knew the MODEL's rule (reads return the last
write). Fault windows (partitions) make such histories common
in buggy consensus implementations — Jepsen's core finding
across a decade of databases.
```

## 7. Why this matters for the corpus

The docs_PRD06 convergence loop uses stock Neo4j as the
oracle. These tools cover the two cases that loop cannot:
- behaviors where stock Neo4j itself might be wrong (TLP's
  identities hold regardless — they'd catch shared bugs);
- behaviors invisible to query diffing: concurrency and
  crash-fault semantics need history checking (Jepsen), not
  result comparison — thesis condition 1 (observability),
  supplied by fault injection + offline checking.

## 7b. Reproducers — shrinking a finding to a bug report

```text
both SQLancer oracles return a Reproducer object, not just a
boolean (TLPWhereOracle.java:32-48, NoRECOracle.java:35-45):
    bugStillTriggers(state) re-runs the exact queries and asks
    whether the disagreement persists
this enables automated REDUCTION: mutate the schema/query
toward smaller forms, keep the mutation iff bugStillTriggers —
the output is a minimal SQL script a DB maintainer can run.
Jepsen's analogue: the history file itself is the reproducer;
:valid? false comes with the failing operation window.
lesson for any harness (including a rewrite's parity rig):
a red test that can't re-run itself in isolation is a rumor;
build the reproducer object into the oracle from day one.
```

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/TLPWhereOracle.java` | ternary partition identity (88-114) |
| sqlancer | `reference-repos-corpus/sqlancer-src/src/sqlancer/common/oracle/NoRECOracle.java` | optimized-vs-unoptimized invariant (35-73) |
| jepsen | `reference-repos-corpus/jepsen-src/jepsen/src/jepsen/checker.clj` | Checker protocol, linearizable/Knossos (59-296) |

## 9. Cross-references

- Sibling pattern: `tolerant-equivalence-validation` (28 —
  when a reference DOES exist but equality needs tolerance).
- Kin: `stub-script-conformance` (24 — scripts as oracles for
  wire behavior; TLP/NoREC are oracles for SEMANTICS).
- Papers ledger: SQLancer's PQS/NoREC/TLP papers; Jepsen's
  per-database analyses.
- For a graph-DB rewrite: port TLP to Cypher WHERE, add a
  NoREC analogue (predicate in WHERE vs in a WITH projection),
  and run Jepsen-style histories against Bolt sessions during
  fault injection — three oracles, none requiring Neo4j to
  be correct.
- Next: pattern 28 (validation tolerance), then the
  bench-testing synthesis closes the corpus categories.

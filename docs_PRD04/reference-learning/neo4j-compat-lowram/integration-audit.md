# Neo4j Compatibility x Low-RAM Mega-Spec Integration Audit

**Audit date:** 2026-08-08  
**North star read:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`, 962/962 lines  
**Guiding specification:** `docs_PRD04/Neo4j-Compatibility-LowRAM-Mega-Spec.md`  
**Result:** Evidence and specification goals pass; product implementation and performance claims remain future work.

## 1. Final Product Decision

The evidence rejects a general Neo4j rewrite as the first product.

The recommended architecture is:

> A pinned, read-only Neo4j adoption profile over a portable artifact-native graph runner that models the complete working set, selects `fit`, `spill`, `approximate`, or `refuse`, enforces the declared ceiling, and emits a proof receipt.

Compatibility exists to let a real security, IAM, dependency, SBOM, code-graph, service-map, or attack-path workload reach the runner with familiar query text and official drivers. It does not own storage or algorithm architecture.

## 2. Evidence Completion

### Repository denominator

| Metric | Final value |
|---|---:|
| Assigned repositories | 20 |
| Git-tracked files | 32,262 |
| Git blob bytes | 478,516,872 |
| Denominator SHA-256 | `03e35cf1a1b0964e7876bf8b25b9e8819f5578e06b4efb4c9ab3f182193b4846` |
| Duplicate `(repo, path)` keys | 0 |

### Coverage union

| Coverage state | Files |
|---|---:|
| `direct_read` | 5,014 |
| `graph_indexed` | 19,186 |
| `noncode_classified` | 7,103 |
| `binary_classified` | 845 |
| `generated_classified` | 114 |
| **Total** | **32,262** |

Direct-read blobs total 41,225,296 bytes. Every relevance `>=80` row and every source path matched by the founder-critical policy is `direct_read`.

### Lane outputs

| Lane | Files | Dossier | Ledger |
|---|---:|---|---|
| Core/Bolt/Cypher/kernel | 12,847 | `agent-01-core-compatibility.md` | `evidence/agent-01-files.tsv` |
| GDS/APOC/low RAM | 12,213 | `agent-02-gds-lowram.md` | `evidence/agent-02-files.tsv` |
| openCypher/TestKit/drivers/Browser/OGM | 7,202 | `agent-03-verification-ecosystem.md` | `evidence/agent-03-files.tsv` |

All three dossiers were read end to end during integration. Their union passes exact repo/path/blob/bytes/extension reconciliation, legal coverage states, unique evidence IDs, relevance policy, source classification, and critical-read policy.

## 3. Code-Graph Completion

The three lanes used `@sdsrs/code-graph` 0.114.1 across every assigned repository. The main integration pass independently queried:

- GDS PageRank, Pregel, and Dijkstra estimator structure.
- Neo4j Bolt auto-commit state transitions.
- TestKit feature/capability structure.
- openCypher graph-index coverage limitations.

The main finding is methodological as well as architectural: source graphs are excellent for prioritizing and tracing code, but they do not cover most Gherkin, grammar, specification, binary, and fixture assets. The Git denominator plus classified/direct-read non-code evidence prevents false completeness.

The structural graphs contain unresolved/ambiguous calls, especially around common Java method names. Exact file path, Git blob, source content, tests, and runtime behavior remain authoritative over an inferred graph edge.

## 4. Executable-Spec Completion

| Metric | Final value |
|---|---:|
| Mega-spec lines | 1,053 |
| Mega-spec bytes | 70,311 |
| Executable requirements | 97 |
| Test matrix entries | 57 |
| Requirement-to-test coverage | 100% |
| Verified upstream file citations | 60 |
| Verified local evidence citations | 6 |

The specification preserves the required order:

1. Executable Requirements.
2. Test Matrix.
3. TDD Plan.
4. Quality Gates.
5. Open Questions.

Its normative domains are:

- Product boundary and falsification.
- Portable artifact and manifest.
- Complete working-set admission.
- Four-way plan selection.
- Hard resource enforcement.
- Before/during/after receipts.
- Versioned Neo4j adoption profiles.
- Bounded Bolt/PackStream/FSM/streaming behavior.
- Customer-derived typed Cypher subset.
- GDS estimate/stream/stats/mutate/write semantics.
- Determinism modes.
- Algorithm-shaped storage for the seven A007 families.
- Verification, security, operations, and PMF gates.

## 5. Validation Receipts

```text
PASS: 3 Python scripts compile

PASS: reconciled 32262 evidence rows
agent-01: 12847 rows
agent-02: 12213 rows
agent-03: 7202 rows
binary_classified: 845 rows
direct_read: 5014 rows
generated_classified: 114 rows
graph_indexed: 19186 rows
noncode_classified: 7103 rows
direct_read_bytes: 41225296

PASS: 97 requirements, 57 tests, 100% requirement-to-test coverage,
60 verified file-level citations, 6 verified local citations
```

The denominator was regenerated twice with the same SHA-256 shown above.

## 6. What Is Proven

1. A full Neo4j rewrite is a multi-million-line database program, not a weekend implementation. Agent 01 measured over 2.09 million Java/Scala LOC in `neo4j-src` alone and mapped the breadth of Bolt, Cypher, kernel, storage, index, and runtime subtrees.
2. A narrow official-driver/Bolt/Cypher read profile is technically separable from Neo4j's mutable database kernel.
3. GDS already has serious component-level memory estimators. Knight Walker differentiation must be complete admission plus enforcement, plan alternatives, and calibrated receipts, not the false claim that Neo4j has no estimates.
4. GDS algorithm families have materially different working-set shapes. PageRank/Pregel, Dijkstra/BFS, WCC, similarity/kNN, Louvain/Leiden, triangles, and FastRP justify algorithm-specific state and storage contracts.
5. TestKit, BoltStub, openCypher TCK, and official drivers provide complementary verification layers. None alone proves application compatibility or the bounded-compute product.
6. The current Rust code has a compatibility/receipt seed and bounded snapshot-construction seed, but its Bolt receipt still reports resource high-water status as unavailable and its memory budget is not yet the complete A007 hard-ceiling product.

## 7. What Is Not Proven

1. No customer has yet supplied the definitive production query, driver version, artifact, operational budget, or paid commitment for `KB-ACCESS-P0`.
2. The current implementation does not yet pass the selected TestKit/TCK/official-driver matrix.
3. The current implementation does not yet enforce a whole-process hard RSS/cgroup ceiling for a supported OLAP job.
4. No complete algorithm profile yet demonstrates all four plan decisions with calibrated prediction error.
5. No measured claim yet proves lower RAM or better useful latency than a pinned Neo4j/GDS baseline for a production-shaped workload.
6. License and trademark decisions for grammar, TCK, TestKit, Neo4j/GDS/APOC, Browser, driver fixtures, and compatibility marketing require legal review.
7. The receipt's willingness-to-pay value remains a founder hypothesis.

## 8. Exact Next Goal

The next goal should not be “implement the mega spec.” It should be one executable vertical slice:

```text
One redacted customer-shaped access-path query
+ one official driver/version
+ one immutable portable graph artifact
+ one declared hard memory ceiling
-> pinned Bolt profile
-> typed bounded Cypher IR
-> full-working-set estimate
-> exact fit / exact spill / refuse
-> OS-enforced execution
-> differential Neo4j result
-> complete receipt with estimator error
```

Approximation should enter only if that customer-shaped workload has a defensible quality contract. PageRank should be the next iterative proof after the access-path slice unless customer evidence changes the order.

## 9. Suggested Goal Prompt

```text
/goal Implement the first proof-carrying KB-ACCESS-P0 vertical slice from
docs_PRD04/Neo4j-Compatibility-LowRAM-Mega-Spec.md, governed by
docs_PRD04/A007-spc-founder-interview-prep-v7.md.

Use TDD and the existing progress journal. Freeze one official Python-driver
version and one bounded read-only access-path query from the current fixture
corpus as a temporary founder-shaped profile. Write RED tests first for:
1. pinned Bolt negotiation and bounded decoding;
2. unchanged query text/typed parameters;
3. typed bounded Cypher IR and early refusal;
4. full-working-set preflight receipt;
5. exact fit, forced exact spill, and pre-execution refusal;
6. supervised hard-memory enforcement and cancellation;
7. Neo4j differential result normalization;
8. complete terminal receipt and estimator error.

Do not add writes, general transactions, routing, general Cypher, arbitrary
GDS/APOC, Browser, OGM, or database-kernel machinery. Stop and report if the
current fixture cannot honestly stand in for a named customer workflow.
```

## 10. Closure Decision

The research/specification goal is complete. The product is not.

The resulting body of work provides a bounded, testable implementation program and an auditable evidence base. Its first implementation bet is small enough to falsify, yet complete enough to demonstrate the actual differentiation: familiar graph access with enforceable resource truth.

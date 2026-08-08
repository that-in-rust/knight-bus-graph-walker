# Agent 05: PRD04 Low-RAM Architecture Corpus

Status: evidence work in progress  
Binding source: `docs_PRD04/A007-spc-founder-interview-prep-v7.md`  
Frozen denominator: `docs_PRD04/reference-learning/lowram-architecture-corpus/evidence/all-documents-denominator.tsv`  
Corpus assignment: exactly 68 rows where `assigned_agent=agent-05`

## Executive Decision

Knight Walker SHALL be designed as low-RAM deterministic OLAP graph compute, not as a database. Neo4j compatibility is an adoption adapter and verification oracle. It SHALL NOT pull transactions, mutable database storage, administration, general Cypher breadth, or server-platform ceremony into the product core.

The architecture target is an artifact-to-answer bounded runner:

```text
portable artifact
  + declared hard budget
  + complete working-set estimate
  + explicit fit | spill | approximate | refuse decision
  + enforced execution plan
  + deterministic result and resource receipt
```

This report is intentionally incomplete until every denominator row has been reconciled and the high-impact implementation claims have been checked against the Neo4j-family source graph.

## Evidence Method

1. Read A007 completely before all other corpus work.
2. Preserve the frozen denominator's `path`, `sha256`, `bytes`, and `extension` fields exactly.
3. Semantically read architecture, founder, and algorithm-storage Markdown.
4. Structurally query the 36 MB raw research dump and generated TSV ledgers; do not pretend every generated line is equal-value prose.
5. Inspect the XLSX evidence matrix with a structured workbook parser.
6. Cross-check high-impact architecture claims with `@sdsrs/code-graph` 0.114.1 against the relevant Neo4j/GDS repositories.
7. Assign exactly one evidence row and one unique `A05-######` identifier to every one of the 68 frozen files.
8. Recompute filesystem SHA-256 and byte size after writing the outputs and require zero denominator gaps or mismatches.

## Founder Contract Extracted From A007

### Product boundary

- Build an **artifact-to-answer bounded graph runner**.
- Do not build a general graph database, AI memory platform, or every graph algorithm.
- Treat compatibility as an adoption mechanism for unchanged useful queries and familiar result shapes, never as the engine architecture.
- Lead with the first ICP's job: security, IAM, dependency, SBOM, and access-path analysis.
- Treat codebase intelligence as a fast demo and founder-advantage wedge, not automatically the highest-budget market.

### Enforceable systems contract

- The estimate SHALL include representation, fixed, per-node, per-edge, frontier/queue, output, conversion/projection, runtime, and safety-reserve state.
- Admission SHALL choose `fit`, `spill`, `approximate`, or `refuse` before execution.
- Execution SHALL enforce a hard RSS or cgroup ceiling; an estimate without enforcement is not the product.
- Approximation SHALL be explicit, opt-in, quality-bounded, and identified in the receipt.
- The post-run receipt SHALL expose actual peak RSS, mapped/retained memory where measurable, I/O and spill, wall/CPU time, output cardinality/checksum, estimator error, and engine/artifact identity.
- Determinism SHALL cover result identity, stable tie-breaking, seeded randomness, and reproducible execution metadata. It SHALL NOT falsely promise identical wall time or physical page-fault schedules.

### Product falsifiers

- The customer's binding pain is ingestion, schema design, permissions, UI, or a product-specific workflow rather than graph execution.
- A tuned existing system already meets the hard-budget job with acceptable certainty and ceremony.
- Conservative refusal does not improve planning or trust.
- Receipt-grade evidence does not change repeat-use or willingness to pay.
- The low-RAM plan only relocates memory into uncontrolled page cache or unmeasured build/conversion peaks.
- The 50 GB on 16 GB milestone works technically but does not correspond to a real artifact and decision-changing job.

## Founder Thesis Evolution And Superseded Claims

| Stage | Useful contribution | Superseded or narrowed by A007 |
|---|---|---|
| A000/A001 | Storage-shaped traversal, scoped 4.5x walk-path evidence, explicit rejection of a broad Neo4j replacement. | Broad market list, unverified revenue claims, and “top seven cover 80-90%” are not decision-grade evidence. |
| A002 | Memory honesty, deterministic paths, and pre-run admission become product concepts. | Graph universality does not prove a company or a database category. |
| A003/A004 | Embedded OLAP rather than an Oracle-of-graphs; quote/receipt as commercial surface; Kuzu and GraphChi acknowledged. | Agent memory as first wedge and “quote-before-run” as empty whitespace were later invalidated. |
| A005 | Full `fit/spill/approximate/refuse` vocabulary; output receipts; working-set statistics; out-of-core lineage. | “Zipf pays the subsidy” is a workload hypothesis, not a universal capacity theorem. Scarcity does not automatically yield an accurate estimator. |
| A006 | Clear artifact-to-job GTM, fast-learning code/security wedge, and receipt-as-behavior framing. | Pre-run certainty alone is competitively incomplete because Neo4j and other systems already estimate. |
| A007 | Estimate becomes an enforceable portable contract; security/dependency/access paths become the best evidenced ICP. | Binding current thesis. |

## Architecture Option Registry

The registry will be completed from the architecture corpus. IDs are stable labels for the per-file evidence ledger.

| ID | Architecture option | Current status |
|---|---|---|
| `OPT-CONTRACT-01` | Full-working-set estimator plus hard admission/enforcement/receipt | Choose; binding product invariant |
| `OPT-COMPAT-01` | Narrow read-only Bolt/Cypher/GDS compatibility facade | Choose as adoption adapter; scope by first ICP |
| `OPT-DB-REWRITE-00` | Full Neo4j/database rewrite | Reject |
| `OPT-GENERIC-CSR-01` | One universal CSR/CSC representation for all algorithms | Defer as baseline only; reject as sole architecture |
| `OPT-HOT-COLD-01` | Degree/traffic-aware hot, warm, and streamed adjacency strata | Experiment; workload-shape dependent |
| `OPT-MMAP-01` | Memory-mapped immutable artifact pages | Choose as one substrate, not as a RAM guarantee |
| `OPT-SPILL-01` | Runtime-owned bounded spill with explicit I/O accounting | Choose; implementation varies by algorithm |
| `OPT-APPROX-01` | Explicit bounded approximation plans | Defer until exact first slice is proven; never silent |
| `OPT-ANSWER-01` | Precomputed answer or query-shaped artifact | Experiment for repeated immutable workloads; reject as universal compute replacement |

## Corpus Accounting

At initialization, all 68 assigned files existed and exactly matched the frozen denominator's SHA-256 and byte count. Final coverage and status totals will be inserted after the semantic and structured passes.

## Architecture Findings

Pending completion of the architecture, algorithm-storage, structured-evidence, and code-graph passes.

## Algorithm-Specific Decision Matrix

Pending.

## Working-Set Model Audit

Pending.

## Contradictions And Missing Terms

Pending.

## Code-Graph Cross-Checks

Pending.

## Choose / Experiment / Reject / Defer

Pending.

## Final Validation Receipt

Pending.

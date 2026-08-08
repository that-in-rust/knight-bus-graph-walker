# Founder Contract Spine: Neo4j Compatibility x Bounded Graph Compute

**Status:** Binding research and specification guardrail  
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`  
**Scope:** All evidence gathered from the 20 repositories under `gitrefrepo/Neo4j family/`

## 1. Product Thesis

Knight Walker is an **artifact-to-answer bounded graph runner**. A user supplies a portable graph artifact, an analytical job, and a hard resource budget. Before execution, the runner models the complete working set and chooses one explicit outcome:

1. `fit`: run exactly within the declared budget.
2. `spill`: run exactly with a declared bounded-memory external-memory plan.
3. `approximate`: run under a declared error or quality bound.
4. `refuse`: reject before execution because no honest plan satisfies the contract.

Neo4j/Bolt/Cypher/GDS compatibility is an **adoption adapter** that lets a real workload reach this runner. Compatibility SHALL NOT silently turn the product into a general-purpose Neo4j rewrite.

## 2. Founder-Gated Hierarchy

Every proposed requirement SHALL answer these questions in order:

| Gate | Required answer | Reject when |
|---|---|---|
| User pain | Which security, IAM, dependency, SBOM, or access-path job becomes possible or materially cheaper? | The answer is generic graph-database parity. |
| Artifact | What portable graph artifact and manifest enter the system? | The workflow requires adopting a new graph platform first. |
| Answer | What bounded analytical answer leaves the system? | The output is only an internal primitive. |
| Budget | What hard memory, temporary-storage, deadline, or accuracy contract applies? | The plan reports an estimate but cannot enforce it. |
| Proof | Which before/during/after receipt fields make the claim falsifiable? | Success depends on prose or an unrepeatable demo. |
| Adoption | Which production-shaped Bolt, Cypher, GDS, CLI, or file interface is strictly necessary? | Compatibility has no identified path to a target workload. |
| Differentiation | Which storage/algorithm pairing improves boundedness, RAM, predictability, or useful latency? | The change is merely Java-to-Rust translation. |

## 3. Required Systems Contract

The final mega spec SHALL define executable contracts for:

- Storage-aware representation selection.
- Algorithm-specific full-working-set models: fixed, per-node, per-edge, frontier, output, conversion, spill, runtime, and operating-system overhead.
- Hard RSS or cgroup ceilings, including enforcement and cancellation semantics.
- Explicit `fit`, `spill`, `approximate`, and `refuse` plan selection.
- Estimate calibration against measured peak memory and I/O.
- Deterministic output checksums and versioned execution receipts.
- Cold and warm run distinction.
- Portable artifacts and local/container execution without graph-platform ceremony.

## 4. Receipt Contract

### Before execution

- Artifact and manifest version/hash.
- Node, relationship, label/type, property, and relevant cardinality counts.
- Persistent representation bytes.
- Algorithm, mode, orientation, concurrency, and configuration.
- Fixed, per-node, per-edge, frontier, output, conversion, and safety-margin estimates.
- Estimate range and confidence/calibration state.
- Selected `fit`, `spill`, `approximate`, or `refuse` plan.
- Hard memory ceiling, expected temporary storage and I/O, and runtime range or `unknown`.

### During execution

- Enforced high-water mark.
- Phase and progress.
- Bytes read, written, mapped, and spilled.
- Cancellation, deadline, or refusal reason.
- Cold/warm indicator.

### After execution

- Peak RSS plus heap, off-heap, mapped, retained, and temporary-storage accounting where measurable.
- Estimator absolute and percentage error.
- Wall-clock and CPU time.
- Output cardinality and deterministic checksum.
- Approximation quality/error bound when applicable.
- Artifact, manifest, estimator, planner, and engine versions.

## 5. Workload Priority

Evidence and implementation SHALL be prioritized in this order unless a documented customer interview changes it:

1. BFS, shortest paths, and bounded path search.
2. Weakly connected components and components-oriented jobs.
3. PageRank and centrality.
4. NodeSimilarity and k-nearest-neighbor workloads.
5. Louvain and Leiden community detection.
6. Triangle and clustering metrics.
7. FastRP and embedding generation.

## 6. Compatibility Boundary

The research SHALL classify every discovered Neo4j-family surface as one of:

- `must_build`: required for a named first-customer artifact-to-answer path.
- `adapter_only`: parse or translate at the edge; no architectural imitation required.
- `oracle_only`: useful as a differential verification source or fixture.
- `defer`: plausible later value but no current founder evidence.
- `reject`: general-database breadth that conflicts with the bounded-runner thesis.

An unchanged production-shaped query is valuable only when its semantics map to a bounded analytical job. Full Cypher, transactional-kernel, browser, OGM, driver, and administrative parity are not assumed goals.

## 7. Every-File Evidence Semantics

“Go through every file” means every Git-tracked file in the 20 Neo4j-family repositories SHALL have exactly one row in the global denominator and exactly one reconciled evidence row. It does not mean pretending every vendored binary or generated file received a semantic close read.

Allowed coverage states are:

- `direct_read`: semantically inspected and cited where relevant.
- `graph_indexed`: source indexed and queried structurally; promoted to `direct_read` when critical.
- `generated_classified`: generated content classified by provenance and role.
- `noncode_classified`: documentation/config/test data classified and read when decision-relevant.
- `binary_classified`: binary asset identified by type, size, and repository role.

The final audit SHALL prove:

- Denominator count equals reconciled evidence count.
- No duplicate `(repo, path)` rows exist.
- Git blob hashes match the inspected repository state.
- Every `must_build`, high-risk, estimator, planner, storage, parser, protocol, and verification file is `direct_read`.
- Every architectural claim cites an evidence ID, local path, executable benchmark, test oracle, or an explicitly labeled hypothesis.

## 8. Claims We Will Not Smuggle In

- Neo4j has no memory estimators.
- An estimate is equivalent to an enforceable fit guarantee.
- Embedded or disk-backed graph processing is novel by itself.
- Lower RAM alone proves a company.
- Security teams will pay before a paid design partner demonstrates it.
- A 50 GB graph running on a 16 GB machine proves product-market fit.
- Rewriting Java in Rust, adding `io_uring`, or maximizing parallelism automatically improves OLAP latency.

## 9. Research Output Contract

The three evidence lanes SHALL produce:

| Lane | Repositories | Decision output |
|---|---|---|
| Core compatibility | Neo4j core, Bolt docs, Cypher shell, Cypher DSL | Minimal production-query adapter; kernel/storage/transaction surfaces to build, oracle, defer, or reject. |
| GDS and low RAM | GDS, GDS clients/agents, APOC | Algorithm registry, working-set terms, algorithm-shaped storage options, execution modes, estimator and receipt oracles. |
| Verification ecosystem | openCypher, TestKit, drivers, browser, OGM, neo4rs | Differential oracle, fixture hierarchy, protocol conformance boundary, and adoption verification loop. |

The synthesis SHALL end as an executable specification in this order:

1. Executable Requirements.
2. Test Matrix.
3. TDD Plan.
4. Quality Gates.
5. Open Questions.

## 10. Product Falsification

The spec SHALL include tests that can kill or narrow the product thesis. The strongest current kill signal is evidence that the target team's real pain is ingestion, schema management, permissions, UI, or operational workflow rather than bounded graph analysis. The strongest business unknown is whether an enforceable estimate plus execution receipt is valuable enough to become a paid category.

The implementation is successful only when one proof-carrying, end-to-end OLAP slice beats or usefully trades against Neo4j/GDS on **correctness, enforced peak RAM, predictability, and useful latency** for a named workflow. Repository coverage and compatibility breadth are inputs to that proof, not the proof itself.

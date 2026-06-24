# Requirements Coverage Tracker
Date: 2026-06-24
This tracker implements the `executable-specs-01` discipline for the reference-learning program. It does not replace the main spec. It makes the spec executable by showing which `REQ-LEARN-*` contracts are already satisfied by artifacts, which are only partially satisfied, and which next batch must exist before the goal can honestly be called complete.
## Executable Requirements
### REQ-TRACK-001.0: Track Every Learning Contract
**WHEN** the reference-learning program is active
**THEN** the tracker SHALL list every `REQ-LEARN-*` contract from `docs_PRD03/V003-Reference-Folder-Learning-Spec.md` exactly once
**AND** SHALL assign each row a current execution status
**SHALL** identify the owning artifact or the next required batch.
### REQ-TRACK-002.0: Distinguish Proof From Plan
**WHEN** a requirement is reviewed
**THEN** the tracker SHALL distinguish artifact-backed coverage from partial coverage, spec-native guardrails, and planned future work
**AND** SHALL avoid calling planned work complete
**SHALL** surface the next falsifier or next batch theme.
### REQ-TRACK-003.0: Keep The Goal Honest
**WHEN** the user asks what the spec implementation has achieved
**THEN** the tracker SHALL show which requirement clusters are still open
**AND** SHALL keep the active goal from being marked complete early
**SHALL** prioritize the most architecture-critical missing batches first.
## Coverage Summary
| status | count | meaning |
| --- | ---: | --- |
| `ArtifactCovered` | 19 | Requirement has concrete backing in one or more emitted batch artifacts. |
| `ArtifactPartial` | 9 | A first artifact exists, but the requirement still needs deeper study or more families. |
| `PlannedNextBatch` | 22 | Requirement is not yet artifact-backed and is assigned to a concrete next batch. |
| `SpecNativeGuardrail` | 2 | Requirement is currently satisfied in the spec/process itself rather than a study batch. |

## Requirement Coverage Matrix
| req_id | title | current_status | current_artifact_or_guardrail | next_batch_or_action | primary_repo_family |
| --- | --- | --- | --- | --- | --- |
| `REQ-LEARN-001.0` | Preserve PRD03 Architecture Boundaries | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md | hold boundary in all later batches | already-emitted artifacts |
| `REQ-LEARN-002.0` | Study Neo4j OLTP Storage First | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | read deeper OLTP record/WAL/lock/index paths | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-003.0` | Study Bolt And Driver Compatibility | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | trace Bolt/testkit/driver expectations | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-004.0` | Study Cypher Compatibility Surface | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | map Cypher grammar and surface boundary | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-005.0` | Study Neo4j Procedure And Value Semantics | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | study procedure/value semantics and error shapes | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-006.0` | Inventory GDS Public ABI | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | enrich TSV row detail by family | already-emitted artifacts |
| `REQ-LEARN-007.0` | Study GDS Graph Store And Projection Mechanics | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | trace more graph store and projection internals | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-008.0` | Study GDS Memory Estimation | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | expand beyond PageRank estimate path | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-009.0` | Study GDS Algorithm Families By State Shape | `ArtifactPartial` | Batch-02-GDS-Public-Surface-Inventory.md | classify real state shape per family from kernels | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-010.0` | Study Mutate, Write, Model, And Pipeline Semantics | `ArtifactPartial` | Batch-02-GDS-Public-Surface-Inventory.md | trace mutate/write/model/pipeline side effects | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-011.0` | Study Projection Build Store Precedents | `PlannedNextBatch` | Batch 03: Projection Build Store Precedents | derive durable build/control plane contract | `apache-iggy-src`, `redb-src`, `fjall-src`, `rocksdb-src`, `tikv-src` |
| `REQ-LEARN-012.0` | Study Columnar Sidecar Precedents | `PlannedNextBatch` | Batch 06: Sidecars And Planner Inputs | study Arrow/Parquet sidecar fit | `apache-arrow-rs-src`, `apache-parquet-format-src`, `apache-datafusion-src`, `ladybug-src`, `kuzu-src`, `memgraph-src` |
| `REQ-LEARN-013.0` | Study Query Planning Patterns Carefully | `PlannedNextBatch` | Batch 06: Sidecars And Planner Inputs | study planning and execution policy boundaries | `apache-arrow-rs-src`, `apache-parquet-format-src`, `apache-datafusion-src`, `ladybug-src`, `kuzu-src`, `memgraph-src` |
| `REQ-LEARN-014.0` | Study Compact Graph Competitors Second | `PlannedNextBatch` | Batch 06: Compact Graph Competitors | study Ladybug/Kuzu/Memgraph after first-party contract | `ladybug-src`, `kuzu-src`, `memgraph-src`, `age-src` |
| `REQ-LEARN-015.0` | Study Algorithm Baselines After GDS Surface | `PlannedNextBatch` | Batch 07: Algorithm Feasibility And Oracles | trace non-PageRank representative kernels | `neo4j-gds-src`, `gapbs-src` if present later, `graphblas-src` if present later, `lagraph-src` if present later, low-RAM graph priors already cloned when available |
| `REQ-LEARN-016.0` | Produce Evidence-Ledger Artifacts | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep using evidence-ledger format | already-emitted artifacts |
| `REQ-LEARN-017.0` | Maintain Traceability | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | preserve req-to-artifact traceability | already-emitted artifacts |
| `REQ-LEARN-018.0` | Preserve Four-Word Naming For Future Helpers | `SpecNativeGuardrail` | V003-Reference-Folder-Learning-Spec.md | apply only when helper scripts or generators are added | current spec and future helper-generation passes |
| `REQ-LEARN-019.0` | Study Ladybug Embedded Graph Patterns | `PlannedNextBatch` | Batch 06: Compact Graph Competitors | extract Ladybug embedded graph lessons | `ladybug-src`, `kuzu-src`, `memgraph-src`, `age-src` |
| `REQ-LEARN-020.0` | Study GDS User Workflow Compatibility | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | tie GDS surface to real user workflows | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-021.0` | Study APOC Support Boundary | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | bound APOC compatibility expectations | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-022.0` | Study Client Ecosystem Canaries | `PlannedNextBatch` | Batch 05: Neo4j Compatibility Boundary | use testkit and official drivers as canaries | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-023.0` | Study LDBC Benchmark Contracts | `PlannedNextBatch` | Batch 08: Benchmarks And Observability | define LDBC and workload credibility rules | `ldbc_*` clones when added, `tracing-src`, `jemalloc-src`, current Knight Bus benchmark docs |
| `REQ-LEARN-024.0` | Study Low-RAM Out-Of-Core Graph Systems | `PlannedNextBatch` | Batch 07: Algorithm Feasibility And Oracles | study out-of-core graph algorithm priors | `neo4j-gds-src`, `gapbs-src` if present later, `graphblas-src` if present later, `lagraph-src` if present later, low-RAM graph priors already cloned when available |
| `REQ-LEARN-025.0` | Study GraphBLAS Alternative Substrate | `PlannedNextBatch` | Batch 07: Algorithm Feasibility And Oracles | decide when GraphBLAS is additive vs overreach | `neo4j-gds-src`, `gapbs-src` if present later, `graphblas-src` if present later, `lagraph-src` if present later, low-RAM graph priors already cloned when available |
| `REQ-LEARN-026.0` | Study Rust Graph Fixture Scaffolding | `PlannedNextBatch` | Batch 07: Algorithm Feasibility And Oracles | define fixture and parity harness needs | `neo4j-gds-src`, `gapbs-src` if present later, `graphblas-src` if present later, `lagraph-src` if present later, low-RAM graph priors already cloned when available |
| `REQ-LEARN-027.0` | Study RAM Observability Precedents | `PlannedNextBatch` | Batch 08: Benchmarks And Observability | study RSS/page-cache/direct-buffer observability | `ldbc_*` clones when added, `tracing-src`, `jemalloc-src`, current Knight Bus benchmark docs |
| `REQ-LEARN-028.0` | Study Rejected Live-Incremental Architectures | `PlannedNextBatch` | Batch 03: Projection Build Store Precedents | record why live incremental serving is rejected | `apache-iggy-src`, `redb-src`, `fjall-src`, `rocksdb-src`, `tikv-src` |
| `REQ-LEARN-029.0` | Study Graph-Vector Market Watch | `PlannedNextBatch` | Batch 08: Benchmarks And Observability | track vector/graph market edges without driving core architecture | `ldbc_*` clones when added, `tracing-src`, `jemalloc-src`, current Knight Bus benchmark docs |
| `REQ-LEARN-030.0` | Exhaust Full GDS Surface Before Sufficiency Claims | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep blocking sufficiency claims until enriched | already-emitted artifacts |
| `REQ-LEARN-031.0` | Study Snapshot Publication Catalog | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | create dedicated publication-generation batch | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-032.0` | Study Current Knight Bus CSR Seed | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md | use flat CSR as oracle and first primitive | already-emitted artifacts |
| `REQ-LEARN-033.0` | Produce Agent-Ready Study Prompts | `ArtifactCovered` | Batch-02-GDS-Public-Surface-Inventory.md; V003-Reference-Folder-Learning-Spec.md | keep prompts repo-family specific | already-emitted artifacts |
| `REQ-LEARN-034.0` | Produce Architecture Fit Matrix | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | extend fit matrices across later families | already-emitted artifacts |
| `REQ-LEARN-035.0` | Separate Source Inference And Speculation | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | preserve fact/inference/speculation split | already-emitted artifacts |
| `REQ-LEARN-036.0` | Maintain Local Clone Coverage Ledger | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep clone ledger current as new repos are used | already-emitted artifacts |
| `REQ-LEARN-037.0` | Produce PRD Outcome Traceability Dossier | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep PRD outcome dossiers in all batches | already-emitted artifacts |
| `REQ-LEARN-038.0` | Run Skeptical Architecture Review | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep skeptical review table mandatory | already-emitted artifacts |
| `REQ-LEARN-039.0` | Support Weaker Agent Execution | `SpecNativeGuardrail` | V003-Reference-Folder-Learning-Spec.md | use weak-model prompts and small lanes in every future pass | current spec and future helper-generation passes |
| `REQ-LEARN-040.0` | Use Local Graph Tools Safely | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md | continue repo-only graph-tool discipline | already-emitted artifacts |
| `REQ-LEARN-041.0` | Produce Checkpoint Summaries | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | continue checkpoint summaries | already-emitted artifacts |
| `REQ-LEARN-042.0` | Enforce Architecture Fit Matrices | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | apply fit matrices to later families and build-store work | already-emitted artifacts |
| `REQ-LEARN-043.0` | Run Weak-Model Verification | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep weak-model verification checklist active | already-emitted artifacts |
| `REQ-LEARN-044.0` | Trace GDS Procedures To Kernels | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | trace more representative procedures beyond PageRank | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-045.0` | Derive Storage Needs From Kernel Behavior | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | derive storage needs for more families | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-046.0` | Capture Algorithm Memory Estimator Semantics | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | capture estimator semantics for more families | existing emitted artifacts plus the next representative family or operations batch |
| `REQ-LEARN-047.0` | Classify Full Algorithm Feasibility | `PlannedNextBatch` | Batch 07: Algorithm Feasibility And Oracles | classify full-family feasibility under RAM-first architecture | `neo4j-gds-src`, `gapbs-src` if present later, `graphblas-src` if present later, `lagraph-src` if present later, low-RAM graph priors already cloned when available |
| `REQ-LEARN-048.0` | Require Algorithm Oracle And Parity Tests | `PlannedNextBatch` | Batch 07: Algorithm Feasibility And Oracles | define oracle and parity proof obligations | `neo4j-gds-src`, `gapbs-src` if present later, `graphblas-src` if present later, `lagraph-src` if present later, low-RAM graph priors already cloned when available |
| `REQ-LEARN-049.0` | Emit Required Study Deliverables | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep required deliverables complete in each batch | already-emitted artifacts |
| `REQ-LEARN-050.0` | Follow Decision-First Study Order | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep decision-first order explicit | already-emitted artifacts |
| `REQ-LEARN-051.0` | Use Shared Support Status Taxonomy | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | use shared status taxonomy in later batches | already-emitted artifacts |
| `REQ-LEARN-052.0` | Maintain Architecture Option Scorecard | `ArtifactPartial` | Batch-01-Current-Seed-And-GDS-Baseline.md | refresh option scorecard after build-store and publication evidence | existing emitted artifacts plus the next representative family or operations batch |

## Batch Queue
| priority | batch theme | main requirement clusters | why this is next |
| --- | --- | --- | --- |
| 1 | Batch 03: Projection Build Store Precedents | `REQ-LEARN-011.0`, `REQ-LEARN-028.0` | The Build Store boundary is a core PRD pillar and still lacks first-class evidence. |
| 2 | Batch 04: Publication And Generation Catalog | `REQ-LEARN-031.0`, `REQ-LEARN-052.0` | Snapshot watermark, publish/swap, rollback, and retention are only partially covered. |
| 3 | Batch 05: Neo4j Compatibility Boundary | `REQ-LEARN-002.0` through `REQ-LEARN-005.0`, `REQ-LEARN-020.0` through `REQ-LEARN-022.0` | API and workflow compatibility must be bounded by first-party sources before architecture broadens further. |
| 4 | Batch 06: Sidecars, Planner Inputs, And Compact Competitors | `REQ-LEARN-012.0` through `REQ-LEARN-014.0`, `REQ-LEARN-019.0` | This batch decides what must live outside topology and what competitor lessons are actually admissible. |
| 5 | Batch 07: Algorithm Feasibility And Oracles | `REQ-LEARN-015.0`, `REQ-LEARN-024.0` through `REQ-LEARN-026.0`, `REQ-LEARN-047.0`, `REQ-LEARN-048.0` | The storage decision is incomplete until more algorithm families are traced and classified. |
| 6 | Batch 08: Benchmarks And Observability | `REQ-LEARN-023.0`, `REQ-LEARN-027.0`, `REQ-LEARN-029.0` | Benchmark and RAM claims need observability and workload discipline, not just architectural taste. |

## Test Matrix
| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| `REQ-TRACK-001.0` | `TEST-TRACK-001` | static check | all 52 REQ rows appear exactly once in the tracker | coverage completeness |
| `REQ-TRACK-001.0` | `TEST-TRACK-002` | artifact parity | every ArtifactCovered/ArtifactPartial row cites an existing artifact file | link correctness |
| `REQ-TRACK-002.0` | `TEST-TRACK-003` | planning check | every PlannedNextBatch row names a concrete future batch theme | execution readiness |
| `REQ-TRACK-002.0` | `TEST-TRACK-004` | taxonomy check | status values stay within ArtifactCovered, ArtifactPartial, PlannedNextBatch, SpecNativeGuardrail | status hygiene |
| `REQ-TRACK-003.0` | `TEST-TRACK-005` | goal check | the tracker highlights uncovered core clusters before claiming spec execution completion | honesty |

## TDD Plan
1. STUB
- Parse the spec and enumerate all `REQ-LEARN-*` IDs.
- Enumerate current emitted batch artifacts.
- Write a first coverage matrix with no optimistic status upgrades.
2. RED
- Treat any requirement with no emitted batch or spec-native guardrail as unresolved.
- Fail the honesty check if the tracker would allow the goal to be marked complete.
3. GREEN
- Assign every requirement one of four statuses: `ArtifactCovered`, `ArtifactPartial`, `PlannedNextBatch`, or `SpecNativeGuardrail`.
- Attach each unresolved cluster to a concrete next batch theme.
4. REFACTOR
- Group adjacent requirements into coherent study batches.
- Keep the matrix readable enough for a weaker follow-on agent.
5. VERIFY
- Re-run the requirement count.
- Verify each cited artifact file exists.
- Verify the batch queue orders the most architecture-critical gaps first.

## Quality Gates
- [ ] The tracker lists all 52 `REQ-LEARN-*` contracts.
- [ ] No unresolved requirement is mislabeled as artifact-covered.
- [ ] Every `PlannedNextBatch` row names a concrete batch theme.
- [ ] Every `ArtifactCovered` or `ArtifactPartial` row cites at least one existing artifact.
- [ ] The tracker does not claim the active goal is complete.
- [ ] The batch queue keeps Build Store and publication semantics ahead of lower-leverage competitor study.

## Open Questions
- Should the next implementation-grade milestone stop after Build Store and publication semantics, or should it also include the broader Neo4j compatibility boundary batch before any more architecture scoring?
- Should future batch numbering stay sequential by study order even if a human asks for an out-of-order spike?
- Do we want a machine-readable TSV companion for requirement coverage, or is the Markdown tracker sufficient for now?

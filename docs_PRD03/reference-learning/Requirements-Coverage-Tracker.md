# Requirements Coverage Tracker

Date: 2026-06-24

This tracker implements the `executable-specs-01` discipline for the
reference-learning program. It does not replace the main spec. It makes the
spec executable by showing which `REQ-LEARN-*` contracts are already satisfied
by artifacts, which are only partially satisfied, and whether any next batch is
still required before the learning-spec goal can honestly be called complete.

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
| `ArtifactCovered` | 51 | Requirement has concrete backing in one or more emitted batch artifacts. |
| `ArtifactPartial` | 0 | A first artifact exists, but the requirement still needs deeper study or more families. |
| `PlannedNextBatch` | 0 | No requirement is currently sitting in this bucket; future uncovered rows would land here with a concrete next batch. |
| `SpecNativeGuardrail` | 2 | Requirement is currently satisfied in the spec/process itself rather than a study batch. |

## Verification Snapshot

The current tracker has been checked directly against
`docs_PRD03/V003-Reference-Folder-Learning-Spec.md` and the live
`docs_PRD03/reference-learning/` folder.

| verification check | current result | meaning |
| --- | --- | --- |
| Spec requirement count | `53` | The spec currently defines `53` explicit `REQ-LEARN-*` contracts. |
| Tracker coverage rows | `53` | The tracker currently records one row per explicit requirement. |
| Missing requirement IDs | `0` | No spec requirement is currently absent from the tracker. |
| Extra requirement IDs | `0` | The tracker does not currently invent rows not present in the spec. |
| Duplicate requirement IDs | `0` | No requirement is currently recorded more than once. |
| Missing non-guardrail artifact references | `0` | Every file-cited `ArtifactCovered` row currently points at an existing local artifact. |
| Current status mix | `51 ArtifactCovered`, `0 ArtifactPartial`, `0 PlannedNextBatch`, `2 SpecNativeGuardrail` | The current scope is implemented as a study program, with no unresolved architecture-critical partial rows. |

This snapshot is deliberately narrow. It proves the study-program bookkeeping is
currently aligned. It does not, by itself, prove that every cited artifact drew
the right conclusion from its sources; that stronger claim still lives in the
batch artifacts, evidence ledgers, and skeptical-review sections themselves.

A machine-readable companion now exists at
`docs_PRD03/reference-learning/Requirements-Coverage-Tracker.tsv` so future
agents do not have to scrape this Markdown table to answer basic coverage or
handoff questions.

## Requirement Coverage Matrix

| req_id | title | current_status | current_artifact_or_guardrail | next_batch_or_action | primary_repo_family |
| --- | --- | --- | --- | --- | --- |
| `REQ-LEARN-001.0` | Preserve PRD03 Architecture Boundaries | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md | hold boundary in all later batches | already-emitted artifacts |
| `REQ-LEARN-002.0` | Study Neo4j OLTP Storage First | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | read deeper OLTP record/WAL/lock/index paths only when an implementation spike requires them | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-003.0` | Study Bolt And Driver Compatibility | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | deepen only for feature-specific handshake or retry questions | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-004.0` | Study Cypher Compatibility Surface | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | deepen grammar/runtime behavior only when the parser strategy hardens | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-005.0` | Study Neo4j Procedure And Value Semantics | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | add more first-party value/error cases when procedure execution work starts | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-006.0` | Inventory GDS Public ABI | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | enrich TSV row detail by family | already-emitted artifacts |
| `REQ-LEARN-007.0` | Study GDS Graph Store And Projection Mechanics | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md | keep mapping new projection edge cases back to the same artifact classes | `neo4j-gds-src`, `neo4j-gds-client-src`, `graph-data-science-src`, `gds-agent-src` |
| `REQ-LEARN-008.0` | Study GDS Memory Estimation | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md | later implementation work should turn the formulas into executable tests rather than reopen the study | `neo4j-gds-src`, current Knight Bus memory-contract docs, low-RAM graph priors |
| `REQ-LEARN-009.0` | Study GDS Algorithm Families By State Shape | `ArtifactCovered` | Batch-02-GDS-Public-Surface-Inventory.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md | keep the family-state matrix aligned if the inventory expands | `neo4j-gds-src`, baseline algorithm repos |
| `REQ-LEARN-010.0` | Study Mutate, Write, Model, And Pipeline Semantics | `ArtifactCovered` | Batch-02-GDS-Public-Surface-Inventory.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md | keep semantics current if support tiers change later | already-emitted artifacts |
| `REQ-LEARN-011.0` | Study Projection Build Store Precedents | `ArtifactCovered` | Batch-03-Projection-Build-Store-Precedents.md | convert Build Store recommendation into a later concrete schema | already-emitted artifacts |
| `REQ-LEARN-012.0` | Study Columnar Sidecar Precedents | `ArtifactCovered` | Batch-06-Sidecars-Planner-And-Compact-Competitors.md | deepen hot-versus-cold sidecar packaging only when measured memory or decode tradeoffs demand it | `apache-arrow-rs-src`, `apache-parquet-format-src`, `apache-datafusion-src`, `ladybug-src`, `kuzu-src`, `memgraph-src` |
| `REQ-LEARN-013.0` | Study Query Planning Patterns Carefully | `ArtifactCovered` | Batch-06-Sidecars-Planner-And-Compact-Competitors.md | deepen planner and explain semantics only when concrete projection routing or spill control work begins | `apache-arrow-rs-src`, `apache-parquet-format-src`, `apache-datafusion-src`, `ladybug-src`, `kuzu-src`, `memgraph-src` |
| `REQ-LEARN-014.0` | Study Compact Graph Competitors Second | `ArtifactCovered` | Batch-06-Sidecars-Planner-And-Compact-Competitors.md | revisit only after later algorithm-family evidence says one compact competitor warrants a deeper adoption spike | `ladybug-src`, `kuzu-src`, `memgraph-src`, `age-src` |
| `REQ-LEARN-015.0` | Study Algorithm Baselines After GDS Surface | `ArtifactCovered` | Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md | preserve the oracle/state-shape references for later parity work | `neo4j-gds-src`, `gapbs-src`, `snap-src`, `lagraph-src`, `graphblas-src` |
| `REQ-LEARN-016.0` | Produce Evidence-Ledger Artifacts | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep using evidence-ledger format | already-emitted artifacts |
| `REQ-LEARN-017.0` | Maintain Traceability | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | preserve req-to-artifact traceability | already-emitted artifacts |
| `REQ-LEARN-018.0` | Preserve Four-Word Naming For Future Helpers | `SpecNativeGuardrail` | V003-Reference-Folder-Learning-Spec.md | apply only when helper scripts or generators are added | current spec and future helper-generation passes |
| `REQ-LEARN-019.0` | Study Ladybug Embedded Graph Patterns | `ArtifactCovered` | Batch-06-Sidecars-Planner-And-Compact-Competitors.md | deepen only if later publication, execution-profile, or factorization work needs more implementation detail | `ladybug-src`, `kuzu-src`, `memgraph-src`, `age-src` |
| `REQ-LEARN-020.0` | Study GDS User Workflow Compatibility | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | deepen workflow traces only when concrete catalog or agent parity questions arise | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-021.0` | Study APOC Support Boundary | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | refine the support tiering if v003 changes its claimed APOC boundary | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-022.0` | Study Client Ecosystem Canaries | `ArtifactCovered` | Batch-05-Neo4j-Compatibility-Boundary.md | expand canary coverage only when a driver or shell path becomes implementation-critical | `neo4j-src`, `neo4j-docs-bolt-src`, `neo4j-testkit-src`, official driver repos, `opencypher-src`, `neo4j-apoc-src` |
| `REQ-LEARN-023.0` | Study LDBC Benchmark Contracts | `ArtifactCovered` | Batch-09-Benchmarks-And-Observability.md | keep workload naming, validation, and repeatability discipline explicit in later benchmark claims | `ldbc_*` clones when added, `tracing-src`, `jemalloc-src`, current Knight Bus benchmark docs |
| `REQ-LEARN-024.0` | Study Low-RAM Out-Of-Core Graph Systems | `ArtifactCovered` | Batch-07-Low-RAM-Graph-Priors.md | revisit only if a later architecture spike needs deeper shard/window internals | `neo4j-gds-src`, low-RAM graph priors, and GraphBLAS/LAGraph repos when available |
| `REQ-LEARN-025.0` | Study GraphBLAS Alternative Substrate | `ArtifactCovered` | Batch-07-Low-RAM-Graph-Priors.md | revisit only if similarity or other hard families force a stronger GraphBLAS case | `neo4j-gds-src`, low-RAM graph priors, and GraphBLAS/LAGraph repos when available |
| `REQ-LEARN-026.0` | Study Rust Graph Fixture Scaffolding | `ArtifactCovered` | Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-11-Algorithm-Oracle-And-Parity-Scaffolding.md; Rust-Fixture-And-Oracle-Scaffolding.tsv | keep these repos classified as scaffolding/oracle shelves rather than storage architecture | `petgraph-src`, `rustworkx-src`, `sprs-src`, `sparsetools-src`, `networkit-src`, `igraph-src` |
| `REQ-LEARN-027.0` | Study RAM Observability Precedents | `ArtifactCovered` | Batch-09-Benchmarks-And-Observability.md | keep measured-versus-estimated memory fields and source naming intact in later implementation work | `ldbc_*` clones when added, `tracing-src`, `jemalloc-src`, current Knight Bus benchmark docs |
| `REQ-LEARN-028.0` | Study Rejected Live-Incremental Architectures | `ArtifactCovered` | Batch-03-Projection-Build-Store-Precedents.md | keep live incremental serving explicitly rejected unless PRD changes | already-emitted artifacts |
| `REQ-LEARN-029.0` | Study Graph-Vector Market Watch | `ArtifactCovered` | Batch-09-Benchmarks-And-Observability.md | keep graph-vector/full-text findings bounded to later tiers unless PRD03 expands P0 | `ldbc_*` clones when added, `tracing-src`, `jemalloc-src`, current Knight Bus benchmark docs |
| `REQ-LEARN-030.0` | Exhaust Full GDS Surface Before Sufficiency Claims | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep blocking sufficiency claims until enriched | already-emitted artifacts |
| `REQ-LEARN-031.0` | Study Snapshot Publication Catalog | `ArtifactCovered` | Batch-04-Publication-And-Generation-Catalog.md | turn invariants into a concrete generation catalog schema | already-emitted artifacts |
| `REQ-LEARN-032.0` | Study Current Knight Bus CSR Seed | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md | use flat CSR as oracle and first primitive | already-emitted artifacts |
| `REQ-LEARN-033.0` | Produce Agent-Ready Study Prompts | `ArtifactCovered` | Batch-02-GDS-Public-Surface-Inventory.md; V003-Reference-Folder-Learning-Spec.md | keep prompts repo-family specific | already-emitted artifacts |
| `REQ-LEARN-034.0` | Produce Architecture Fit Matrix | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md; Batch-05-Neo4j-Compatibility-Boundary.md; Batch-06-Sidecars-Planner-And-Compact-Competitors.md; Architecture-Fit-Matrix.tsv | extend the consolidated matrix across later families and batches as new evidence lands | already-emitted artifacts |
| `REQ-LEARN-035.0` | Separate Source Inference And Speculation | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md | preserve fact/inference/speculation split | already-emitted artifacts |
| `REQ-LEARN-036.0` | Maintain Local Clone Coverage Ledger | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md; Reference-Shelf-Graph-Evidence-Ledger.md; Reference-Shelf-Subpath-Coverage-Audit.md; Reference-Shelf-Requirement-Subpath-Coverage.tsv | keep clone ledger current as new repos are used | already-emitted artifacts |
| `REQ-LEARN-037.0` | Produce PRD Outcome Traceability Dossier | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-09-Benchmarks-And-Observability.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md; Batch-11-Algorithm-Oracle-And-Parity-Scaffolding.md; PRD-Outcome-Traceability-Dossier.tsv | keep PRD outcome dossiers current as later batches add stronger evidence or new PRD outcomes | already-emitted artifacts |
| `REQ-LEARN-038.0` | Run Skeptical Architecture Review | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md | keep skeptical review table mandatory | already-emitted artifacts |
| `REQ-LEARN-039.0` | Support Weaker Agent Execution | `SpecNativeGuardrail` | V003-Reference-Folder-Learning-Spec.md | use weak-model prompts and small lanes in every future pass | current spec and future helper-generation passes |
| `REQ-LEARN-040.0` | Use Local Graph Tools Safely | `ArtifactCovered` | Reference-Shelf-Graph-Evidence-Ledger.md; Reference-Shelf-Subpath-Coverage-Audit.md; Reference-Shelf-Requirement-Subpath-Coverage.tsv; Batch-01-Current-Seed-And-GDS-Baseline.md | keep dual-tool-ready versus CBM-only timeout-heavy repos explicit in later passes | already-emitted artifacts |
| `REQ-LEARN-053.0` | Canonicalize Reference Shelf Paths | `ArtifactCovered` | Reference-Shelf-Graph-Evidence-Ledger.md; Reference-Shelf-Subpath-Coverage-Audit.md; Reference-Shelf-Requirement-Subpath-Coverage.tsv | keep resolving legacy `ref-repo-folder/` mentions to `gitrefrepo/` while the old shelf is empty | already-emitted artifacts |
| `REQ-LEARN-041.0` | Produce Checkpoint Summaries | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md | continue checkpoint summaries | already-emitted artifacts |
| `REQ-LEARN-042.0` | Enforce Architecture Fit Matrices | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md; Batch-05-Neo4j-Compatibility-Boundary.md; Batch-06-Sidecars-Planner-And-Compact-Competitors.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Architecture-Fit-Matrix.tsv | use the consolidated matrix as the shelf-level check while continuing to add later-family rows | already-emitted artifacts |
| `REQ-LEARN-043.0` | Run Weak-Model Verification | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md | keep weak-model verification checklist active | already-emitted artifacts |
| `REQ-LEARN-044.0` | Trace GDS Procedures To Kernels | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md; GDS-Procedure-To-Kernel-Ledger.tsv | extend the ledger from representative families toward broader row coverage as new implementation spikes demand it | `neo4j-gds-src`, `neo4j-gds-client-src`, `gds-agent-src` |
| `REQ-LEARN-045.0` | Derive Storage Needs From Kernel Behavior | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md | hold the storage-implication matrix stable unless PRD boundaries change | `neo4j-gds-src`, baseline algorithm repos, current Knight Bus architecture docs |
| `REQ-LEARN-046.0` | Capture Algorithm Memory Estimator Semantics | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md | implementation work should now focus on executable estimate tests, not more study breadth | `neo4j-gds-src`, current Knight Bus memory-contract docs |
| `REQ-LEARN-047.0` | Classify Full Algorithm Feasibility | `ArtifactCovered` | Batch-07-Low-RAM-Graph-Priors.md; Batch-08-Hard-GDS-Families-And-Model-Artifacts.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md; GDS-Family-Support-Tier-Matrix.tsv | keep family support tiers honest as implementation begins | `neo4j-gds-src`, low-RAM graph priors, and GraphBLAS/LAGraph repos when available |
| `REQ-LEARN-048.0` | Require Algorithm Oracle And Parity Tests | `ArtifactCovered` | Batch-11-Algorithm-Oracle-And-Parity-Scaffolding.md; GDS-Parity-Taxonomy.tsv; GDS-Family-Oracle-Parity-Matrix.tsv | keep still-gated families explicitly marked `NeedsArchitectureSpike` or later-tier until their parity class and RAM gate are implemented | `neo4j-gds-src`, current Knight Bus runtime/parity files, `gapbs-src`, `petgraph-src`, `rustworkx-src` |
| `REQ-LEARN-049.0` | Emit Required Study Deliverables | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md; GDS-Procedure-To-Kernel-Ledger.tsv; GDS-Family-Support-Tier-Matrix.tsv; GDS-Parity-Taxonomy.tsv; Rust-Fixture-And-Oracle-Scaffolding.tsv; GDS-Family-Oracle-Parity-Matrix.tsv; Architecture-Fit-Matrix.tsv; Architecture-Option-Scorecard.tsv; PRD-Outcome-Traceability-Dossier.tsv | keep required deliverables complete in each batch and extend the machine-readable companions as later studies deepen | already-emitted artifacts |
| `REQ-LEARN-050.0` | Follow Decision-First Study Order | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md | keep decision-first order explicit | already-emitted artifacts |
| `REQ-LEARN-051.0` | Use Shared Support Status Taxonomy | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-02-GDS-Public-Surface-Inventory.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md | use shared status taxonomy in later batches | already-emitted artifacts |
| `REQ-LEARN-052.0` | Maintain Architecture Option Scorecard | `ArtifactCovered` | Batch-01-Current-Seed-And-GDS-Baseline.md; Batch-03-Projection-Build-Store-Precedents.md; Batch-04-Publication-And-Generation-Catalog.md; Batch-06-Sidecars-Planner-And-Compact-Competitors.md; Batch-10-GDS-Projection-Internals-And-Support-Tiers.md; Architecture-Option-Scorecard.tsv | refresh the consolidated scorecard after later compatibility and algorithm batches too | already-emitted artifacts |

## Follow-On Batch Queue

There are no remaining architecture-critical partial clusters in the current
learning-spec scope.

Any later batch from here should be treated as:

- implementation support,
- machine-readable refinement,
- or scope expansion,

not as unfinished execution of the current learning spec.

The graph-evidence and folder-coverage clause is already closed for the current
scope through:

- `71` concrete repo truthcheck rows in
  `Reference-Shelf-Graph-Tool-Truthcheck.tsv`;
- `27` path-bearing requirement rows in
  `Reference-Shelf-Requirement-Subpath-Coverage.tsv`; and
- narrative control artifacts in
  `Reference-Shelf-Graph-Evidence-Ledger.md` and
  `Reference-Shelf-Subpath-Coverage-Audit.md`.

That means future work here should improve extraction quality or widen scope,
not reopen the question of whether the named repos and subpaths were covered by
the two local evidence-reader skills.

## Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| `REQ-TRACK-001.0` | `TEST-TRACK-001` | static check | all 53 REQ rows appear exactly once in the tracker | coverage completeness |
| `REQ-TRACK-001.0` | `TEST-TRACK-002` | artifact parity | every ArtifactCovered/ArtifactPartial row cites an existing artifact file | link correctness |
| `REQ-TRACK-002.0` | `TEST-TRACK-003` | planning check | every PlannedNextBatch row names a concrete future batch theme | execution readiness |
| `REQ-TRACK-002.0` | `TEST-TRACK-004` | taxonomy check | status values stay within ArtifactCovered, ArtifactPartial, PlannedNextBatch, SpecNativeGuardrail | status hygiene |
| `REQ-TRACK-003.0` | `TEST-TRACK-005` | goal check | the tracker highlights uncovered core clusters before allowing learning-spec completion and keeps implementation work separate from study completion | honesty |

## TDD Plan

1. STUB
- Parse the spec and enumerate all `REQ-LEARN-*` IDs.
- Enumerate current emitted batch artifacts.
- Write a first coverage matrix with no optimistic status upgrades.

2. RED
- Treat any requirement with no emitted batch or spec-native guardrail as unresolved.
- Fail the honesty check if the tracker would allow learning-spec completion
  while architecture-critical partials still exist.

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

- [ ] The tracker lists all 53 `REQ-LEARN-*` contracts.
- [ ] No unresolved requirement is mislabeled as artifact-covered.
- [ ] Every unresolved architecture-critical cluster points to a concrete follow-on batch theme.
- [ ] Every `ArtifactCovered` or `ArtifactPartial` row cites at least one existing artifact.
- [ ] The tracker only allows learning-spec completion when architecture-critical partial rows are zero.
- [ ] The batch queue keeps the largest architecture-critical gaps at the top.

## Open Questions

- Should the current `71`-target learning contract be expanded to cover the
  additional `35` live clones already present under `gitrefrepo/`, or is the
  present scoped discipline the better guardrail for now?
- Should a future pass add a fourth graph-tool execution class for
  "long-budget CGC rerun requested" on the current
  `CbmSemanticReadyCgcLowYield` shelf
  tranche, or is the present three-class ledger enough now that the current
  learning spec is fully covered?
- The TSV companion now exists. Future refinement can decide whether to add
  stronger artifact-type or proof-strength columns, but the "machine-readable
  or not?" question is no longer open.

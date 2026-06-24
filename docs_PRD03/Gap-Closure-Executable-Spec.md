# Gap Closure Executable Spec

Date: 2026-06-24

This spec converts the critique in `Reference-Learning-Critique-Gaps.md` into
executable requirements for the next documentation and implementation-planning
pass.

It is intentionally not a Rust implementation task. Its job is to make the
remaining gaps measurable enough that later agents can implement v003 without
inventing missing compatibility, memory, publication, or registry semantics.

Source inputs:

- `docs_PRD03/prd-l1.md`
- `docs_PRD03/V003-Reference-Folder-Learning-Spec.md`
- `docs_PRD03/Reference-Learning-Critique-Gaps.md`
- `docs_PRD03/reference-learning/`

## Request Parse

| input | value |
| --- | --- |
| Feature outcome | Produce executable gap-closure specs and required artifacts that turn the research shelf into implementation-ready contracts. |
| Actors | v003 implementer, compatibility tester, GDS implementer, benchmark author, architecture reviewer, weaker future agent. |
| Boundaries | Documentation, TSV contracts, test plans, and acceptance gates only. No production Rust code is required in this pass. |
| Failure modes | Treating research coverage as implementation proof, shrinking GDS surface, leaving all procedures at `MissingEvidence`, failing to define unsupported behavior, making RAM claims without formulas, publishing snapshots without a state machine. |
| Performance and reliability limits | Every strict-RAM claim must include a formula, budget, and rejection path. Every compatibility claim must name at least one canary or conformance test. |
| Language/runtime constraints | Future Rust symbols introduced by later implementation SHOULD use four-word names unless preserving Neo4j/GDS public API compatibility. Generated docs and TSVs SHOULD stay ASCII. |

## Executable Requirements

### REQ-GAP-001.0: Create Implementation Readiness Tracker

**WHEN** the gap-closure pass begins
**THEN** the project SHALL create `docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv`
**AND** SHALL include one row per gap-closing artifact and each artifact's state: `Missing`, `Specified`, `Stubbed`, `Implemented`, `Verified`, or `Rejected`
**SHALL** reject `Verified` status unless the referenced artifact exists and all linked tests or checks pass.

### REQ-GAP-002.0: Resolve GDS Procedure Registry

**WHEN** the project claims any level of GDS surface compatibility
**THEN** it SHALL create `docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv`
**AND** SHALL include every row from `GDS-Public-Surface-Inventory.tsv`
**AND** SHALL replace `MissingEvidence` with explicit values or `NotApplicable` in columns for config type, result type, estimate path, support status, unsupported behavior, and first test
**SHALL** fail verification if row count differs from the source inventory without a documented parser-change note.

### REQ-GAP-003.0: Specify Unsupported Procedure Behavior

**WHEN** a GDS procedure, APOC procedure, user function, or compatibility surface is not implemented
**THEN** the support registry SHALL assign `UnsupportedButRegistered`, `ExplicitlyOutOfScope`, or `P2-ImplementedLater`
**AND** SHALL define the user-visible error code, message shape, procedure mode, and result schema behavior
**SHALL** avoid silently hiding a known public procedure unless the PRD explicitly excludes it.

### REQ-GAP-004.0: Define Neo4j Compatibility Canaries

**WHEN** the project claims zero-application-code-change compatibility for a surface
**THEN** it SHALL create `docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md`
**AND** SHALL define canaries for Bolt handshake, official drivers, cypher-shell, `CALL ... YIELD`, procedure value conversion, error mapping, APOC boundary, browser-style workflows, and representative application clients
**SHALL** include command shape, expected result, and failure classification for each canary.

### REQ-GAP-005.0: Define OLTP Record Store Contract

**WHEN** implementation planning touches OLTP storage
**THEN** it SHALL create `docs_PRD03/implementation-readiness/OLTP-Record-Store-Rust-Contract.md`
**AND** SHALL define Rust module boundaries for nodes, relationships, properties, schema tokens, indexes, WAL, locks, checkpoints, import, and recovery
**AND** SHALL list record invariants for dense nodes, relationship groups, relationship chains, property blocks, dynamic records, token stores, and schema constraints
**SHALL** identify at least one source-backed Neo4j file or test for every invariant.

### REQ-GAP-006.0: Define Projection Build Store Contract

**WHEN** implementation planning touches the Projection Build Store
**THEN** it SHALL create `docs_PRD03/implementation-readiness/Projection-Build-Store-Physical-Contract.md`
**AND** SHALL define receipt schema, ordering, idempotency, replay, dense-id assignment, dictionaries, sorted runs, metadata state, validation reports, retention, and compaction thresholds
**SHALL** include crash cases for partial receipt append, partial metadata commit, failed snapshot build, and failed publication handoff.

### REQ-GAP-007.0: Define Snapshot Publication State Machine

**WHEN** implementation planning touches published OLAP snapshots
**THEN** it SHALL create `docs_PRD03/implementation-readiness/Snapshot-Publication-State-Machine.md`
**AND** SHALL define generation directory layout, active pointer format, source watermark, staged/validating/published/retired/failed/garbage states, fsync or rename order, reader pins, retention, rollback, restart recovery, and corruption handling
**SHALL** prove that a query sees generation `W` or `W+1`, never half-built files.

### REQ-GAP-008.0: Create Memory Formula Book

**WHEN** a procedure or snapshot build path claims strict RAM behavior
**THEN** it SHALL add a row to `docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv`
**AND** SHALL name heap, RSS/page-cache policy, direct buffers, topology, sidecars, result artifacts, model artifacts, scratch, spill, retained generations, and algorithm state
**AND** SHALL include one 50GB-on-8GB pass/fail example where applicable
**SHALL** fail verification if any performance or RAM claim lacks a measurement source or formula.

### REQ-GAP-009.0: Define Cells Adoption Falsifier

**WHEN** the architecture keeps cells optional
**THEN** it SHALL create `docs_PRD03/implementation-readiness/Cells-Adoption-Falsifier-Plan.md`
**AND** SHALL define the measurements that trigger adoption, postponement, or rejection of cellular packaging
**AND** SHALL include boundary-edge ratio, dirty-region size, rebuild latency, page-cache churn, metadata overhead, duplicate topology pressure, and local-query improvement thresholds
**SHALL** compare flat CSR plus sidecars, flat CSR plus global stream, cellular CSR, and hybrid flat-plus-cell publication.

### REQ-GAP-010.0: Define Artifact Model Catalog Contract

**WHEN** GDS mutate, write, embedding, model, or pipeline behavior is in scope
**THEN** it SHALL create `docs_PRD03/implementation-readiness/Artifact-Model-Catalog-Contract.md`
**AND** SHALL define artifact identity fields: user, database, graph name, generation, procedure, config hash, source watermark, model id, pipeline id, and result property
**AND** SHALL define lifecycle rules for creation, listing, mutation, writeback, stale source generation, cleanup, and model dependency loss
**SHALL** include deterministic behavior when a model references a retired or missing graph generation.

### REQ-GAP-011.0: Define Benchmark Proof Plan

**WHEN** v003 claims RAM or latency advantage over Neo4j
**THEN** it SHALL create `docs_PRD03/implementation-readiness/Benchmark-Proof-Plan.md`
**AND** SHALL define datasets, scale factors, commands, baselines, phases, validation checks, and output reports
**AND** SHALL compare Neo4j Cypher over OLTP store, Neo4j GDS projected graph, and Knight Bus v003 published snapshot paths
**SHALL** measure cold start, projection build, publication, algorithm execution, writeback, peak RSS, page cache, direct buffers, scratch, spill, retained generations, and output correctness.

### REQ-GAP-012.0: Add Evidence Confidence Tiers

**WHEN** a claim from the reference shelf is reused in an implementation contract
**THEN** the consuming artifact SHALL label it `DirectSource`, `GraphToolAssisted`, `DocsOnly`, `Inference`, or `Speculation`
**AND** SHALL cite a local path, symbol, line range or rerunnable `rg` query, and falsifier
**SHALL** reject architecture-critical claims marked only `Speculation`.

### REQ-GAP-013.0: Define Support Status Semantics

**WHEN** any artifact uses `P0-RegisteredCompatible`, `P1-ImplementedExactLowRam`, `P2-ImplementedLater`, `NeedsArchitectureSpike`, `UnsupportedButRegistered`, or `ExplicitlyOutOfScope`
**THEN** it SHALL create or reference `docs_PRD03/implementation-readiness/Support-Status-Runtime-Semantics.md`
**AND** SHALL define runtime behavior, registry behavior, required tests, user-visible result, and promotion criteria for each status
**SHALL** prevent research status labels from being mistaken for implemented product behavior.

### REQ-GAP-014.0: Create Gap Closure Index

**WHEN** any gap-closing artifact is added
**THEN** the project SHALL create or update `docs_PRD03/implementation-readiness/README.md`
**AND** SHALL link every artifact, owning requirement, current state, last verification command, and next action
**SHALL** keep the index consistent with `V003-Implementation-Readiness-Tracker.tsv`.

### REQ-GAP-015.0: Preserve Plane Boundaries

**WHEN** a gap-closing artifact proposes data flow or storage ownership
**THEN** it SHALL classify each operation as `OLTP`, `ProjectionBuildStore`, `PublishedOlapSnapshot`, `ArtifactCatalog`, or `ClientCompatibility`
**AND** SHALL reject query-time reconciliation of post-snapshot writes in OLAP procedures unless the PRD changes
**SHALL** preserve the PRD rule that the Projection Build Store is not a user query serving store.

## Test Matrix

| req_id | test_id | test_type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GAP-001.0 | TEST-GAP-001 | doc-contract | readiness tracker exists and every row has valid state | implementation readiness |
| REQ-GAP-002.0 | TEST-GAP-002 | TSV parity | support registry row count equals `GDS-Public-Surface-Inventory.tsv` row count unless parser-change note exists | GDS surface |
| REQ-GAP-002.0 | TEST-GAP-003 | TSV validation | support registry has zero `MissingEvidence` for required compatibility columns | GDS surface |
| REQ-GAP-003.0 | TEST-GAP-004 | contract review | every unsupported procedure has status, error shape, and result behavior | unsupported behavior |
| REQ-GAP-004.0 | TEST-GAP-005 | canary review | canary matrix covers Bolt, drivers, cypher-shell, procedures, values, errors, APOC, browser, and app clients | API compatibility |
| REQ-GAP-005.0 | TEST-GAP-006 | source-backed review | every OLTP invariant cites a Neo4j source file or test | OLTP storage |
| REQ-GAP-006.0 | TEST-GAP-007 | crash-case review | Build Store contract covers four required crash cases | Projection Build Store |
| REQ-GAP-007.0 | TEST-GAP-008 | state-machine review | publication state machine proves old-or-new generation visibility | snapshot publication |
| REQ-GAP-008.0 | TEST-GAP-009 | formula validation | formula book rows include all required memory classes or explicit `NotApplicable` | strict RAM |
| REQ-GAP-008.0 | TEST-GAP-010 | budget example | priority procedures include at least one 50GB-on-8GB pass/fail calculation | strict RAM |
| REQ-GAP-009.0 | TEST-GAP-011 | falsifier review | cells plan names adoption, postponement, and rejection thresholds | cells decision |
| REQ-GAP-010.0 | TEST-GAP-012 | lifecycle review | artifact/model catalog covers stale and missing generation behavior | artifact catalog |
| REQ-GAP-011.0 | TEST-GAP-013 | benchmark review | benchmark plan includes Neo4j Cypher, Neo4j GDS, and Knight Bus paths | proof plan |
| REQ-GAP-012.0 | TEST-GAP-014 | evidence review | architecture-critical claims are not `Speculation` only | evidence quality |
| REQ-GAP-013.0 | TEST-GAP-015 | status review | every support status has runtime behavior, tests, and promotion criteria | status semantics |
| REQ-GAP-014.0 | TEST-GAP-016 | index consistency | README artifact list matches readiness tracker artifact list | traceability |
| REQ-GAP-015.0 | TEST-GAP-017 | boundary review | every data-flow proposal maps operations to exactly one primary PRD plane | PRD boundaries |

## TDD Plan

### STUB

1. Create `docs_PRD03/implementation-readiness/`.
2. Add empty artifact skeletons named by `REQ-GAP-*`.
3. Add `V003-Implementation-Readiness-Tracker.tsv` with all required artifact rows marked `Missing`.
4. Add TSV headers for the support registry and formula book before filling data.
5. Add verification command placeholders to `implementation-readiness/README.md`.

### RED

1. Run row-count checks and confirm the new registry fails while empty.
2. Run `rg "MissingEvidence"` against draft registries and confirm expected failures.
3. Run artifact-existence checks and confirm every missing artifact is reported.
4. Run status-validation checks and confirm unknown or ambiguous statuses fail.
5. Record the expected failure reason beside each `TEST-GAP-*` row.

### GREEN

1. Fill artifacts in priority order: `REQ-GAP-013`, `REQ-GAP-001`, `REQ-GAP-002`, `REQ-GAP-003`, `REQ-GAP-007`, `REQ-GAP-008`.
2. Promote tracker rows from `Missing` to `Specified` only after artifact skeletons include required sections.
3. Promote rows to `Verified` only after the matching `TEST-GAP-*` checks pass.
4. Keep unsupported procedure behavior deterministic even before algorithm implementation exists.

### REFACTOR

1. Merge duplicate vocabulary across registry, memory formulas, and status semantics.
2. Replace broad prose with tables where later agents need machine-readable input.
3. Keep public Neo4j/GDS names unchanged where compatibility requires it.
4. Use four-word names for any new helper scripts introduced later.

### VERIFY

1. Run `git diff --check`.
2. Verify all required artifacts exist.
3. Verify `GDS-Procedure-Support-Registry.tsv` row count equals `GDS-Public-Surface-Inventory.tsv`.
4. Verify no required compatibility column contains `MissingEvidence`.
5. Verify every `REQ-GAP-*` has at least one `TEST-GAP-*`.
6. Verify every `Verified` readiness tracker row links to passing evidence.
7. Verify no PRD plane-boundary rule is violated.

## Quality Gates

- [ ] Every `REQ-GAP-*` ID has at least one linked `TEST-GAP-*`.
- [ ] Every required artifact exists under `docs_PRD03/implementation-readiness/`.
- [ ] `V003-Implementation-Readiness-Tracker.tsv` contains no unknown states.
- [ ] `GDS-Procedure-Support-Registry.tsv` has the same data-row count as `GDS-Public-Surface-Inventory.tsv`.
- [ ] Required registry columns contain zero `MissingEvidence`.
- [ ] Strict-RAM claims cite formulas or benchmark-derived terms.
- [ ] Unsupported procedures define deterministic user-visible behavior.
- [ ] Architecture-critical claims are not supported only by `Speculation`.
- [ ] Snapshot publication contract includes restart recovery and reader pinning.
- [ ] Benchmark proof plan includes both Neo4j baselines and Knight Bus v003.
- [ ] `git diff --check` passes for all changed docs and TSVs.
- [ ] No production Rust code is required for this spec pass.

## Open Questions

| id | question | why it matters | default until answered |
| --- | --- | --- | --- |
| OQ-GAP-001 | Should alpha, beta, test-only, or enterprise-only GDS rows be included in the first registry pass? | It affects the exact procedure compatibility promise. | Include them, but classify support explicitly. |
| OQ-GAP-002 | What exact Neo4j version is the v003 compatibility target? | Procedure names, Bolt behavior, and GDS rows change by version. | Use the local `gitrefrepo/neo4j-src` and `neo4j-gds-src` checkouts recorded by the reference shelf. |
| OQ-GAP-003 | Should `P1-ImplementedExactLowRam` be allowed in research docs before implementation exists? | It can blur planning state and product state. | Treat it as target class in research and require `Implemented`/`Verified` in readiness tracker before product claims. |
| OQ-GAP-004 | How strict should 50GB-on-8GB be for training-heavy GDS families? | Some algorithms may be possible only through rejection, sampling, or spill. | Require explicit fail-fast behavior when exact execution does not fit. |
| OQ-GAP-005 | What is the first user-facing compatibility slice? | It determines which canaries and registry rows move first. | Start with Bolt -> Cypher `CALL gds.graph.list` -> deterministic catalog response or unsupported error. |


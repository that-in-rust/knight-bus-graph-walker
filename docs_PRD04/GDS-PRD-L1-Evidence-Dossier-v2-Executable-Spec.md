# GDS PRD L1 Evidence Dossier v2 Executable Spec

Date: 2026-06-24

This spec defines the second-pass GDS evidence dossier task. It intentionally
does not overwrite the first dossier. v1 remains the baseline. v2 is the
comprehensive architecture-decision artifact.

The main output is:

```text
docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md
```

Large evidence tables SHALL be written as TSV companions in
`docs_PRD03/reference-learning/` and summarized from the v2 Markdown.

## Request Parse

| input | value |
| --- | --- |
| Feature outcome | Produce a comprehensive v2 GDS evidence dossier that closes the critique gaps in v1 and can directly guide a PRD rewrite plus architecture decisions. |
| Actors | PRD rewriter, v003 architect, GDS implementer, compatibility tester, benchmark author, future weaker agent. |
| Source boundary | Only `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src` can be used as source-code evidence. |
| Context boundary | `docs_PRD03/prd-l1.md`, `docs_PRD03/GDS-PRD-L1-Evidence-Dossier-Executable-Spec.md`, and `docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md` are context and baseline, not source-code proof. |
| Failure modes | Producing a prettier v1, leaving thin folders only in a reading log, citing ellipsis paths, counting procedures without joining them to configs/results/estimates, treating docs-only examples as implementation proof, claiming strict RAM without formula/rejection evidence, treating CSR/topology as GDS compatibility. |
| Reliability limits | Every architecture-critical row must include source path or rerunnable query, symbol, sourced fact, local inference, falsifier, confidence, and PRD impact. |
| Runtime constraints | No production Rust implementation is required. New helper scripts are allowed only if they make the evidence more reproducible; generated helper names SHOULD use four-word naming. |

## Non-Negotiable Decisions

| decision | call |
| --- | --- |
| Output mode | Create a separate v2 dossier, not an in-place rewrite of v1. |
| Table mode | Use TSV companions for large joins/matrices; keep Markdown readable. |
| Scope mode | GDS source repo only for source-code claims. |
| Evidence mode | Graph tools are candidate finders only; source reads are truth. |
| Completion mode | The run is incomplete if any required table is missing, any required thin folder is unsearched, or any architecture-critical claim lacks a falsifier. |

## Required Output Files

| file | purpose |
| --- | --- |
| `docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md` | Human-readable synthesis and decision packet. |
| `docs_PRD03/reference-learning/GDS-Procedure-Surface-Join-v2.tsv` | Procedure registry join: names, modes, facades, configs, results, estimates, outputs, support tier. |
| `docs_PRD03/reference-learning/GDS-Projection-Variant-Matrix-v2.tsv` | Native, legacy Cypher, triplet/importer, and catalog projection comparison. |
| `docs_PRD03/reference-learning/GDS-Memory-Formula-Book-v2.tsv` | Memory formulas, high-water risks, rejection gates, missing estimator paths. |
| `docs_PRD03/reference-learning/GDS-Behavior-Mode-Semantics-v2.tsv` | Stream/stats/mutate/write behavior and target plane mapping. |
| `docs_PRD03/reference-learning/GDS-Artifact-Lifecycle-State-Machine-v2.tsv` | Named graph, result store, model, and pipeline lifecycle states. |
| `docs_PRD03/reference-learning/GDS-Oracle-Extraction-Appendix-v2.tsv` | Concrete compatibility fixture candidates from tests and docs. |
| `docs_PRD03/reference-learning/GDS-PRD-Rewrite-Patch-Plan-v2.tsv` | Exact PRD wording proposals with evidence and falsifiers. |
| `docs_PRD03/reference-learning/GDS-V2-Coverage-Audit.tsv` | Folder/search coverage, low-yield notes, and verification status. |

## Required v2 Dossier Sections

The Markdown dossier SHALL include these headings in this order:

1. `# GDS PRD L1 Evidence Dossier v2`
2. `## Executive Thesis`
3. `## Scope And Evidence Rules`
4. `## Graph Tool Readiness`
5. `## Coverage Audit`
6. `## Procedure Surface Join Summary`
7. `## Projection Variant Matrix Summary`
8. `## Memory Formula Book Summary`
9. `## Behavior Mode Semantics`
10. `## Artifact Lifecycle State Machines`
11. `## Algorithm Family State Pressure`
12. `## Oracle Extraction Appendix`
13. `## PRD Rewrite Patch Plan`
14. `## Architecture Risks And Falsifiers`
15. `## Reading Log`
16. `## Open Questions`
17. `## Verification Results`

## Required Folder Coverage

The agent SHALL inspect and source-cite these folders or record a concrete
`LowYield` reason after search:

| priority | folder group | required result |
| --- | --- | --- |
| P0 | `proc`, `procedures`, `procedure-collector` | Procedure registry generation, facade split, modes, config/result shapes, unsupported representation. |
| P0 | `core`, `core-api`, `config-api`, `graph-projection-api`, `native-projection`, `legacy-cypher-projection`, `triplet-graph-builder`, `graph-schema-api`, `graph-dimensions` | Projection config, dense ids, schema, catalog insertion/lookup/remove, native vs Cypher vs importer behavior. |
| P0 | `executor`, `memory-estimation`, `memory-usage`, `collections-memory-estimation`, `applications/algorithms/machinery`, `applications/services` | Parse/load/validate/estimate/execute/result lifecycle and strict-RAM implications. |
| P1 | `applications/algorithms`, `algo`, `algo-common`, `algo-params`, `algorithm-specifications`, `algo-test` | Algorithm family state pressure and missing estimator paths. |
| P1 | `ml`, `pipeline`, `model-catalog-api`, `open-model-catalog`, `applications/model-catalog`, `proc/machine-learning`, `proc/pipeline-catalog` | Model and pipeline identity, lifecycle, procedure entry points, dependencies on graph state. |
| P1 | `core-write`, `open-write-services`, `applications/operations`, `applications/graph-store-catalog-results` | Write, mutate, stream, result-store, operation toggles, and catalog-result semantics. |
| P2 | `doc`, `doc-test`, `algo-test`, module-local tests | Concrete parity oracles with input/config/output/failure behavior. |
| P2 | `neo4j-api`, `neo4j-adapter`, `neo4j-values`, `gds-values`, `io` | Value conversion, import/export, and adapter constraints for client compatibility. |

## Future Goal Prompt

Paste this prompt to execute the v2 dossier task:

```text
/goal Create the comprehensive second-pass GDS PRD L1 Evidence Dossier v2 described in docs_PRD03/GDS-PRD-L1-Evidence-Dossier-v2-Executable-Spec.md.

Use these skills really well:
- /Users/amuldotexe/.codex/skills/executable-specs-01/SKILL.md
- /Users/amuldotexe/.codex/skills/deep-exploration-01/SKILL.md
- /Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md
- /Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md
- /Users/amuldotexe/.codex/skills/tdd-task-progress-context-retainer/SKILL.md

Strict source scope:
/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src

Context and baseline files:
- docs_PRD03/prd-l1.md
- docs_PRD03/GDS-PRD-L1-Evidence-Dossier-Executable-Spec.md
- docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md

Main output:
docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md

Required TSV companions:
- docs_PRD03/reference-learning/GDS-Procedure-Surface-Join-v2.tsv
- docs_PRD03/reference-learning/GDS-Projection-Variant-Matrix-v2.tsv
- docs_PRD03/reference-learning/GDS-Memory-Formula-Book-v2.tsv
- docs_PRD03/reference-learning/GDS-Behavior-Mode-Semantics-v2.tsv
- docs_PRD03/reference-learning/GDS-Artifact-Lifecycle-State-Machine-v2.tsv
- docs_PRD03/reference-learning/GDS-Oracle-Extraction-Appendix-v2.tsv
- docs_PRD03/reference-learning/GDS-PRD-Rewrite-Patch-Plan-v2.tsv
- docs_PRD03/reference-learning/GDS-V2-Coverage-Audit.tsv

Do not overwrite the v1 dossier. v1 is the baseline. v2 must be more comprehensive and more audit-grade.

Mandatory additions:
1. Procedure Surface Join:
   procedure_name | procedure_mode | annotation_source | facade | config_type |
   result_type | algorithm_family | estimate_method | memory_estimation_status |
   output_artifact | support_tier | unsupported_reason | confidence | falsifier

2. Projection Variant Matrix:
   projection_path | entrypoint | dense_id_behavior | label_handling |
   relationship_type_handling | orientation_handling | aggregation_handling |
   inverse_index_handling | property_mapping | estimate_path | catalog_effect |
   PRD_impact | confidence | falsifier

3. Memory Formula Book:
   procedure_or_component | graph_load_terms | algorithm_terms | result_terms |
   model_or_artifact_terms | write_back_terms | build_scratch_terms |
   high_water_risk | reject_condition | missing_estimator_path |
   measurement_method | confidence | falsifier

4. Behavior Mode Semantics:
   procedure_or_family | mode | side_effect | target_plane | input_shape |
   output_shape | transaction_or_catalog_behavior | compatibility_risk |
   confidence | falsifier

5. Artifact Lifecycle State Machine:
   artifact_type | identity_keys | create | list_or_get | use | mutate_or_write |
   drop_or_expire | generation_or_watermark_reference | PRD_impact |
   confidence | falsifier

6. Oracle Extraction Appendix:
   source_test_or_doc | input_graph_shape | procedure_or_config |
   expected_output | failure_behavior | fixture_value | PRD_acceptance_area |
   confidence | falsifier

7. PRD Rewrite Patch Plan:
   PRD_area | action | current_wording | proposed_wording | evidence_pointer |
   decision_reason | falsifier | confidence

Mandatory coverage:
Inspect and source-cite these thin or previously incomplete folders:
procedure-collector, native-projection, legacy-cypher-projection,
collections-memory-estimation, applications/services, open-write-services,
applications/operations, applications/graph-store-catalog-results,
neo4j-api, neo4j-adapter, neo4j-values, gds-values, io,
proc/machine-learning, and proc/pipeline-catalog.

Evidence rules:
- Treat graph tools as candidate finders only.
- Verify every graph-derived claim with rg and direct file reads.
- Every architecture-critical row must include source path or rerunnable query,
  symbol, sourced fact, inference, falsifier, confidence, and PRD impact.
- Use DirectSource, GraphToolAssisted, DocsOnly, Inference, Speculation,
  CandidateOnly, and LowYield correctly.
- Do not rely on docs-only examples as implementation proof.
- Do not claim GDS compatibility from CSR/topology evidence alone.
- Do not claim strict RAM for any supported procedure without estimator,
  measurement method, reject condition, or explicit unsupported status.

Verification:
Run checks for required headings, required TSV files, required TSV columns,
required folder coverage, confidence labels, no MissingEvidence outside open
questions, and git diff --check.

Final response:
Summarize changed files, graph-tool readiness, coverage gaps closed, any
remaining open questions, and verification checks passed.
```

## Executable Requirements

### REQ-GDSV2-001.0: Preserve v1 as baseline

**WHEN** the v2 dossier task begins
**THEN** the agent SHALL read the v1 dossier as baseline context
**AND** SHALL create `GDS-PRD-L1-Evidence-Dossier-v2.md`
**SHALL** not overwrite `GDS-PRD-L1-Evidence-Dossier.md`.

### REQ-GDSV2-002.0: Restrict source evidence scope

**WHEN** recording any source-code fact
**THEN** the fact SHALL come from `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src`
**AND** SHALL cite a path, symbol, line range when available, or rerunnable `rg` query
**SHALL** treat PRD files, v1 dossier, and critiques as context rather than source-code evidence.

### REQ-GDSV2-003.0: Run and classify graph tools

**WHEN** source exploration begins
**THEN** the agent SHALL run `codebase-memory-evidence-reader` and `codegraphcontext-evidence-reader` wrappers against the GDS repo
**AND** SHALL record output directories, exit behavior, semantic query usefulness, and timeout/low-yield status
**SHALL** mark graph-only findings as `CandidateOnly` until verified with direct source reads.

### REQ-GDSV2-004.0: Close thin folder coverage

**WHEN** v2 coverage is complete
**THEN** every folder group in `Required Folder Coverage` SHALL have at least one source-backed row in a table or a concrete `LowYield` row in `GDS-V2-Coverage-Audit.tsv`
**AND** previously thin folders SHALL not appear only in the reading log
**SHALL** fail verification if `native-projection`, `legacy-cypher-projection`, `collections-memory-estimation`, `open-write-services`, `neo4j-api`, `neo4j-adapter`, `neo4j-values`, `gds-values`, or `io` has no coverage row.

### REQ-GDSV2-005.0: Create procedure surface join

**WHEN** procedure/API evidence is gathered
**THEN** `GDS-Procedure-Surface-Join-v2.tsv` SHALL include columns for procedure name, mode, annotation source, facade, config type, result type, algorithm family, estimate method, memory estimation status, output artifact, support tier, unsupported reason, confidence, and falsifier
**AND** SHALL join procedure presence to behavior evidence where available
**SHALL** not infer support from procedure annotation presence alone.

### REQ-GDSV2-006.0: Create projection variant matrix

**WHEN** projection and catalog evidence is gathered
**THEN** `GDS-Projection-Variant-Matrix-v2.tsv` SHALL compare native projection, legacy Cypher projection, triplet/importer, and catalog paths
**AND** SHALL cover dense IDs, labels, relationship types, orientation, aggregation, inverse indexes, property mapping, estimate path, catalog effect, PRD impact, confidence, and falsifier
**SHALL** state where evidence differs by projection path instead of collapsing all projection behavior into `GraphImporter`.

### REQ-GDSV2-007.0: Create memory formula book

**WHEN** memory evidence is gathered
**THEN** `GDS-Memory-Formula-Book-v2.tsv` SHALL include graph-load terms, algorithm terms, result terms, model/artifact terms, write-back terms, build scratch terms, high-water risk, reject condition, missing estimator path, measurement method, confidence, and falsifier
**AND** SHALL list visible `MemoryEstimationNotImplementedException` paths that affect public or candidate-public procedures
**SHALL** reject strict-RAM claims that lack estimator, measurement method, reject condition, or unsupported status.

### REQ-GDSV2-008.0: Separate behavior modes

**WHEN** algorithm and procedure output behavior is documented
**THEN** `GDS-Behavior-Mode-Semantics-v2.tsv` SHALL separate `stream`, `stats`, `mutate`, and `write`
**AND** SHALL map each behavior to `PublishedOlapSnapshot`, `ArtifactCatalog`, `ProjectionBuildStore`, `ClientCompatibility`, or `OLTP write-back`
**SHALL** identify side effects and transaction/catalog behavior where source or docs provide evidence.

### REQ-GDSV2-009.0: Document artifact lifecycle state machines

**WHEN** graph catalog, result store, model, and pipeline evidence is documented
**THEN** `GDS-Artifact-Lifecycle-State-Machine-v2.tsv` SHALL cover create, list/get, use, mutate/write, drop/expire, identity keys, generation/watermark reference, PRD impact, confidence, and falsifier
**AND** SHALL separate named graph, result store, model catalog, and pipeline catalog lifecycles
**SHALL** flag lifecycle states that are v003 design extensions rather than direct GDS source facts.

### REQ-GDSV2-010.0: Extract concrete oracles

**WHEN** tests and docs are used as compatibility evidence
**THEN** `GDS-Oracle-Extraction-Appendix-v2.tsv` SHALL include source test/doc, input graph shape, procedure/config, expected output, failure behavior, fixture value, PRD acceptance area, confidence, and falsifier
**AND** SHALL include at least one oracle candidate each for procedure inventory, catalog, projection, memory, execution, result store, model catalog, pipeline catalog, and write/mutate behavior
**SHALL** not merely list test class names as oracle extraction.

### REQ-GDSV2-011.0: Produce PRD rewrite patch plan

**WHEN** v2 synthesis is complete
**THEN** `GDS-PRD-Rewrite-Patch-Plan-v2.tsv` SHALL propose exact wording changes classified as `Keep`, `Clarify`, `Add`, `Narrow`, or `Spike`
**AND** SHALL include current wording, proposed wording, evidence pointer, decision reason, falsifier, and confidence
**SHALL** cover API surface, catalog, properties, memory, publication, testing, OLTP boundary, OLAP boundary, and Projection Build Store boundary.

### REQ-GDSV2-012.0: Maintain evidence confidence discipline

**WHEN** any row records an architecture-critical claim
**THEN** it SHALL use one of `DirectSource`, `GraphToolAssisted`, `DocsOnly`, `Inference`, `Speculation`, `CandidateOnly`, or `LowYield`
**AND** SHALL include a falsifier
**SHALL** reject `Speculation`, `CandidateOnly`, or `DocsOnly` as sole support for P0 architecture recommendations.

### REQ-GDSV2-013.0: Keep Markdown decision-readable

**WHEN** TSV companions contain detailed evidence
**THEN** the v2 Markdown SHALL summarize the thesis, highest-impact evidence, architecture implications, and links to TSV companions
**AND** SHALL avoid duplicating massive TSV content inline
**SHALL** include enough rows/examples to make the conclusion auditable without opening every TSV.

### REQ-GDSV2-014.0: Write checkpointed reading log

**WHEN** each folder group is completed
**THEN** the v2 Markdown SHALL record folders read, files or queries used, graph-tool candidates used, facts learned, unresolved questions, and next action
**AND** SHALL keep checkpoints resumable for a future agent
**SHALL** not mark a folder group complete until it has source-backed evidence or a `LowYield` row.

### REQ-GDSV2-015.0: Verify generated artifacts

**WHEN** the task is ready for final response
**THEN** the agent SHALL verify required Markdown headings, required TSV files, required TSV columns, required folder coverage, required confidence labels, absence of invalid `MissingEvidence`, and `git diff --check`
**AND** SHALL report graph-tool readiness and any remaining gaps
**SHALL** not mark the goal complete if a required artifact is missing.

## Test Matrix

| req_id | test_id | test_type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GDSV2-001.0 | TEST-GDSV2-001 | file check | v2 file exists and v1 file remains present | baseline safety |
| REQ-GDSV2-002.0 | TEST-GDSV2-002 | evidence audit | source-code evidence paths are under `gitrefrepo/neo4j-gds-src` | scope |
| REQ-GDSV2-003.0 | TEST-GDSV2-003 | graph-tool audit | both graph tools have output dir and readiness rows | tooling |
| REQ-GDSV2-004.0 | TEST-GDSV2-004 | coverage audit | all required folder groups appear in coverage TSV | folder coverage |
| REQ-GDSV2-005.0 | TEST-GDSV2-005 | TSV schema check | procedure join columns exist | API surface |
| REQ-GDSV2-006.0 | TEST-GDSV2-006 | TSV schema check | projection matrix columns exist and projection paths are represented | projection |
| REQ-GDSV2-007.0 | TEST-GDSV2-007 | TSV schema check | memory formula columns include high-water and reject condition | strict RAM |
| REQ-GDSV2-008.0 | TEST-GDSV2-008 | semantic check | stream/stats/mutate/write modes are separated and plane-mapped | behavior |
| REQ-GDSV2-009.0 | TEST-GDSV2-009 | lifecycle check | named graph/result/model/pipeline lifecycles exist | artifacts |
| REQ-GDSV2-010.0 | TEST-GDSV2-010 | oracle check | oracle rows include input/config/output/failure | compatibility tests |
| REQ-GDSV2-011.0 | TEST-GDSV2-011 | PRD patch check | rewrite patch plan includes exact proposed wording | PRD rewrite |
| REQ-GDSV2-012.0 | TEST-GDSV2-012 | confidence audit | P0 recommendations are not supported only by weak confidence labels | evidence quality |
| REQ-GDSV2-013.0 | TEST-GDSV2-013 | readability check | Markdown links to TSVs and summarizes their findings | document usefulness |
| REQ-GDSV2-014.0 | TEST-GDSV2-014 | reading-log check | each folder group has a checkpoint or LowYield row | resumability |
| REQ-GDSV2-015.0 | TEST-GDSV2-015 | final verification | required artifacts exist and `git diff --check` passes | completion |

## TDD Plan

### STUB

1. Create empty `GDS-PRD-L1-Evidence-Dossier-v2.md` with required headings.
2. Create empty TSV companions with required headers.
3. Create `GDS-V2-Coverage-Audit.tsv` with all required folder groups as `MissingEvidence`.
4. Add a short progress checkpoint for the created artifact skeletons.

### RED

1. Run heading checks and confirm required headings exist.
2. Run TSV header checks and confirm all companion files exist.
3. Run coverage checks and confirm `MissingEvidence` rows fail the draft.
4. Run confidence checks and confirm no architecture-critical claim is accepted without evidence.

### GREEN

1. Run graph-tool wrappers and record readiness.
2. Fill coverage rows for thin folders first.
3. Build the procedure surface join from `proc`, `procedures`, and `procedure-collector`.
4. Build the projection variant matrix from projection and catalog folders.
5. Build the memory formula book from memory, execution, services, and algorithm machinery folders.
6. Build behavior-mode, artifact-lifecycle, oracle, and PRD-patch TSVs.
7. Replace every `MissingEvidence` row with source-backed evidence, `LowYield`, or explicit open question.

### REFACTOR

1. Keep detailed rows in TSV companions.
2. Keep Markdown focused on decisions, implications, highest-risk falsifiers, and links.
3. Normalize confidence labels, plane labels, support tiers, and PRD action labels.
4. Split sourced facts from inference and speculation in every major table.

### VERIFY

1. Verify v2 Markdown exists.
2. Verify all required TSV companions exist.
3. Verify all required headings exist.
4. Verify all required TSV headers exist.
5. Verify all required folder groups have evidence or `LowYield`.
6. Verify no invalid `MissingEvidence` remains.
7. Verify every `GraphToolAssisted` claim has direct source verification.
8. Verify no P0 recommendation relies only on `Speculation`, `CandidateOnly`, or `DocsOnly`.
9. Run `git diff --check`.

## Suggested Verification Commands

```bash
test -f docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md
test -f docs_PRD03/reference-learning/GDS-Procedure-Surface-Join-v2.tsv
test -f docs_PRD03/reference-learning/GDS-Projection-Variant-Matrix-v2.tsv
test -f docs_PRD03/reference-learning/GDS-Memory-Formula-Book-v2.tsv
test -f docs_PRD03/reference-learning/GDS-Behavior-Mode-Semantics-v2.tsv
test -f docs_PRD03/reference-learning/GDS-Artifact-Lifecycle-State-Machine-v2.tsv
test -f docs_PRD03/reference-learning/GDS-Oracle-Extraction-Appendix-v2.tsv
test -f docs_PRD03/reference-learning/GDS-PRD-Rewrite-Patch-Plan-v2.tsv
test -f docs_PRD03/reference-learning/GDS-V2-Coverage-Audit.tsv

rg -n "^## (Executive Thesis|Scope And Evidence Rules|Graph Tool Readiness|Coverage Audit|Procedure Surface Join Summary|Projection Variant Matrix Summary|Memory Formula Book Summary|Behavior Mode Semantics|Artifact Lifecycle State Machines|Algorithm Family State Pressure|Oracle Extraction Appendix|PRD Rewrite Patch Plan|Architecture Risks And Falsifiers|Reading Log|Open Questions|Verification Results)" docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md

head -n 1 docs_PRD03/reference-learning/GDS-Procedure-Surface-Join-v2.tsv | rg "procedure_name\tprocedure_mode\tannotation_source\tfacade\tconfig_type\tresult_type\talgorithm_family\testimate_method\tmemory_estimation_status\toutput_artifact\tsupport_tier\tunsupported_reason\tconfidence\tfalsifier"
head -n 1 docs_PRD03/reference-learning/GDS-Projection-Variant-Matrix-v2.tsv | rg "projection_path\tentrypoint\tdense_id_behavior\tlabel_handling\trelationship_type_handling\torientation_handling\taggregation_handling\tinverse_index_handling\tproperty_mapping\testimate_path\tcatalog_effect\tPRD_impact\tconfidence\tfalsifier"
head -n 1 docs_PRD03/reference-learning/GDS-Memory-Formula-Book-v2.tsv | rg "procedure_or_component\tgraph_load_terms\talgorithm_terms\tresult_terms\tmodel_or_artifact_terms\twrite_back_terms\tbuild_scratch_terms\thigh_water_risk\treject_condition\tmissing_estimator_path\tmeasurement_method\tconfidence\tfalsifier"
head -n 1 docs_PRD03/reference-learning/GDS-Behavior-Mode-Semantics-v2.tsv | rg "procedure_or_family\tmode\tside_effect\ttarget_plane\tinput_shape\toutput_shape\ttransaction_or_catalog_behavior\tcompatibility_risk\tconfidence\tfalsifier"
head -n 1 docs_PRD03/reference-learning/GDS-Artifact-Lifecycle-State-Machine-v2.tsv | rg "artifact_type\tidentity_keys\tcreate\tlist_or_get\tuse\tmutate_or_write\tdrop_or_expire\tgeneration_or_watermark_reference\tPRD_impact\tconfidence\tfalsifier"
head -n 1 docs_PRD03/reference-learning/GDS-Oracle-Extraction-Appendix-v2.tsv | rg "source_test_or_doc\tinput_graph_shape\tprocedure_or_config\texpected_output\tfailure_behavior\tfixture_value\tPRD_acceptance_area\tconfidence\tfalsifier"
head -n 1 docs_PRD03/reference-learning/GDS-PRD-Rewrite-Patch-Plan-v2.tsv | rg "PRD_area\taction\tcurrent_wording\tproposed_wording\tevidence_pointer\tdecision_reason\tfalsifier\tconfidence"

rg -n "procedure-collector|native-projection|legacy-cypher-projection|collections-memory-estimation|applications/services|open-write-services|applications/operations|applications/graph-store-catalog-results|neo4j-api|neo4j-adapter|neo4j-values|gds-values|io|proc/machine-learning|proc/pipeline-catalog" docs_PRD03/reference-learning/GDS-V2-Coverage-Audit.tsv
rg -n "DirectSource|GraphToolAssisted|DocsOnly|Inference|Speculation|CandidateOnly|LowYield" docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md docs_PRD03/reference-learning/*-v2.tsv
! rg -n "MissingEvidence" docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier-v2.md docs_PRD03/reference-learning/*-v2.tsv
git diff --check
```

## Quality Gates

- [ ] v2 dossier is separate from v1.
- [ ] Required TSV companions exist and have required headers.
- [ ] Required thin folders are source-backed or explicitly `LowYield`.
- [ ] Procedure join is more than counts and includes config/result/estimate/output/support fields.
- [ ] Projection matrix distinguishes native, legacy Cypher, triplet/importer, and catalog paths.
- [ ] Memory formula book includes high-water risks and reject conditions.
- [ ] Stream, stats, mutate, and write are separated.
- [ ] Artifact lifecycles cover named graph, result store, model catalog, and pipeline catalog.
- [ ] Oracles include input graph shape, config, expected output, and failure behavior.
- [ ] PRD rewrite patch plan includes exact proposed wording.
- [ ] Every architecture-critical claim has evidence, inference, falsifier, confidence, and PRD impact.
- [ ] No P0 recommendation relies only on `Speculation`, `CandidateOnly`, or `DocsOnly`.
- [ ] `git diff --check` passes.

## Open Questions

| question | current call |
| --- | --- |
| Update v1 or create v2? | Create v2. Keep v1 as baseline. |
| Are TSV companions mandatory? | Yes. The v2 task is too large for one readable Markdown table set. |
| Should helper scripts be allowed? | Yes, if they make procedure joins or coverage checks reproducible. |
| Should v2 directly edit `prd-l1.md`? | No. v2 produces a PRD rewrite patch plan with exact wording first. |
| Should unresolved public-surface rows block completion? | Only if they are unclassified. Known unsupported or unknown rows may remain if they have evidence, support tier, reason, and falsifier. |

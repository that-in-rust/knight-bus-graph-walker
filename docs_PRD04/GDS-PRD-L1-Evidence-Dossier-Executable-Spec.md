# GDS PRD L1 Evidence Dossier Executable Spec

Date: 2026-06-24

This spec converts the GDS-folder relevance analysis into an executable
documentation task. The task is intentionally not a Rust implementation pass.
Its job is to produce a source-backed GDS architecture dossier that makes
`docs_PRD03/prd-l1.md` easier to rewrite and makes later architecture decisions
harder to bluff.

The only source repo in scope is:

```text
/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src
```

## Request Parse

| input | value |
| --- | --- |
| Feature outcome | Produce a GDS architecture evidence dossier for `docs_PRD03/prd-l1.md`, focused on public API, projection/catalog mechanics, memory contracts, algorithm state shape, and artifact behavior. |
| Actors | PRD rewriter, v003 architect, GDS implementer, compatibility tester, benchmark author, weaker future agent. |
| Boundaries | Inspect only `gitrefrepo/neo4j-gds-src` and existing `docs_PRD03/prd-l1.md` context. Do not study other reference repos in this task. |
| Failure modes | Creating a generic folder summary, shrinking the GDS surface to easy algorithms, trusting graph tools without source verification, missing memory-estimation gates, confusing projected named graphs with OLTP truth, treating CSR as sufficient for GDS compatibility. |
| Performance and reliability limits | Every architecture-critical claim must cite a local path and symbol or a rerunnable `rg` query. Every graph-tool claim must be confirmed by direct source reads. |
| Language/runtime constraints | No production Rust code is required. New helper scripts, if introduced later, SHOULD use four-word names; public Neo4j/GDS names SHALL be preserved where compatibility is being described. |

## Priority Folder Map

| priority | folders | PRD L1 question |
| --- | --- | --- |
| P0 | `proc`, `procedures`, `procedure-collector` | What public GDS API/procedure surface must v003 register, implement, or reject deterministically? |
| P0 | `core`, `core-api`, `config-api`, `graph-projection-api`, `native-projection`, `legacy-cypher-projection`, `triplet-graph-builder`, `graph-schema-api`, `graph-dimensions` | What does named graph projection really mean beyond "CSR exists"? |
| P0 | `executor`, `memory-estimation`, `memory-usage`, `collections-memory-estimation`, `applications/algorithms/machinery`, `applications/services` | Where are the estimate-before-run, graph-load, validation, execution, and result-consumption boundaries? |
| P1 | `applications/algorithms`, `algo`, `algo-common`, `algo-params`, `algorithm-specifications`, `algo-test` | What state shape does each algorithm family require: topology, properties, scratch, results, or model artifacts? |
| P1 | `ml`, `pipeline`, `model-catalog-api`, `open-model-catalog`, `applications/model-catalog`, `proc/machine-learning`, `proc/pipeline-catalog` | What model and pipeline artifacts must exist outside topology? |
| P1 | `core-write`, `open-write-services`, `applications/operations`, `applications/graph-store-catalog-results` | What do write, mutate, result-store, and catalog-result semantics imply for v003 sidecars and artifacts? |
| P2 | `doc`, `doc-test`, `algo-test`, tests under each module | Which examples and tests can become parity oracles for later Rust implementation? |
| P2 | `neo4j-api`, `neo4j-adapter`, `neo4j-values`, `gds-values`, `io` | What value-conversion and import/export adapter constraints should be noted after the core GDS contract is understood? |

## Anchor Files

The dossier SHOULD start with these files because they connect directly to
`prd-l1.md` acceptance statements.

| anchor | why it matters |
| --- | --- |
| `executor/src/main/java/org/neo4j/gds/executor/ProcedureExecutor.java` | Procedure execution path: config parsing, graph creation, memory validation, algorithm execution, result consumption. |
| `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java` | Named graph catalog behavior: user/database/name lookup, set, remove, event notification, memory reporting. |
| `triplet-graph-builder/src/main/java/org/neo4j/gds/projection/GraphImporter.java` | Projection build path: dense ids, schema, relationship builders, properties, validation, catalog insertion. |
| `graph-projection-api/src/main/java/org/neo4j/gds/NodeProjection.java` | Node projection config: labels and property mappings. |
| `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipProjection.java` | Relationship projection config: type, orientation, aggregation, inverse index, property mappings. |
| `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java` | Memory-estimation composition primitives used by algorithm and graph-load contracts. |
| `pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java` | Pipeline catalog behavior and artifact identity. |
| `open-model-catalog/src/main/java/org/neo4j/gds/core/model/OpenModelCatalog.java` | Model catalog behavior and artifact lifecycle. |

## Future Goal Prompt

Use this prompt when assigning the actual dossier task.

```text
/goal Build the GDS PRD L1 Evidence Dossier described in docs_PRD03/GDS-PRD-L1-Evidence-Dossier-Executable-Spec.md.

Scope is strictly:
/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src

Use these skills really well:
- /Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md
- /Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md

Produce:
docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md

Do not write a generic codebase summary. Write a PRD-rewrite evidence packet
that helps decide and explain:
1. GDS public procedure/API surface.
2. Named graph projection and catalog lifecycle.
3. Projection config semantics: labels, relationship types, orientation,
   aggregation, inverse indexes, and properties.
4. Estimate-before-run and strict RAM gates.
5. Algorithm-family state shape and physical storage pressure.
6. Model, pipeline, mutate/write, result, and sidecar artifacts.
7. Test/doc oracles that can become compatibility fixtures.
8. PRD wording guardrails: especially why "CSR exists" is not enough to claim
   GDS compatibility.

Required operating sequence:
1. Read docs_PRD03/prd-l1.md and this executable spec.
2. Validate that the GDS repo path exists.
3. Run codebase-memory indexing on the GDS repo with the skill wrapper.
4. Run CodeGraphContext indexing on the GDS repo with the skill wrapper.
5. Record graph-tool readiness: output dir, whether semantic queries are
   non-empty, whether either tool timed out or was low-yield.
6. For every graph-tool finding used, verify it with rg plus direct source read.
7. Read the P0 folder set before P1/P2 unless a source link forces a small
   detour.
8. Create evidence tables with columns for source_path, symbol, sourced_fact,
   PRD_L1_impact, inference, falsifier, and confidence.
9. Separate sourced facts from local inferences and speculation.
10. Include a folder-priority reading log and unresolved architecture questions.
11. Run verification checks from the executable spec before final response.

Treat graph tools as accelerators, not truth. If CodeGraphContext or
codebase-memory produces empty semantic output, say so and continue with
direct source search rather than pretending the graph result is evidence.
```

## Executable Requirements

### REQ-GDSDOC-001.0: Restrict scope to GDS repo

**WHEN** the dossier task begins
**THEN** the agent SHALL validate that `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src` exists
**AND** SHALL treat every source-code claim as out of scope unless it comes from that path
**SHALL** read `docs_PRD03/prd-l1.md` only as the target PRD context, not as source-code evidence.

### REQ-GDSDOC-002.0: Run dual graph-tool indexing

**WHEN** source exploration begins
**THEN** the agent SHALL run `codebase-memory-evidence-reader` and `codegraphcontext-evidence-reader` wrapper scripts against the GDS repo path
**AND** SHALL record each output directory, exit status, and elapsed behavior in the dossier
**SHALL** mark a tool `LowYield` if its semantic query output is empty, missing, or unavailable after a bounded wait.

### REQ-GDSDOC-003.0: Verify graph findings with source reads

**WHEN** a graph tool suggests a symbol, relationship, caller/callee, dependency, or folder relevance claim
**THEN** the agent SHALL verify the claim with `rg` and a direct file read
**AND** SHALL cite the local file path and symbol or a rerunnable `rg` query
**SHALL** classify unverified graph output as `CandidateOnly`, not `DirectSource`.

### REQ-GDSDOC-004.0: Document public GDS surface

**WHEN** inspecting `proc`, `procedures`, or `procedure-collector`
**THEN** the dossier SHALL summarize the public API/procedure layers, procedure modes, config/result surfaces, and facade split
**AND** SHALL identify how unsupported-but-known procedures should be represented in v003
**SHALL** avoid claiming implementation support from procedure presence alone.

### REQ-GDSDOC-005.0: Document projection and catalog lifecycle

**WHEN** inspecting projection and catalog folders
**THEN** the dossier SHALL describe graph projection config, dense-id assignment, schema construction, graph-store catalog insertion, lookup, remove, and memory reporting
**AND** SHALL map the behavior to PRD L1 catalog and OLAP snapshot constraints
**SHALL** explicitly explain why topology storage alone is insufficient for GDS compatibility.

### REQ-GDSDOC-006.0: Document memory and execution gates

**WHEN** inspecting `executor`, `memory-estimation`, `memory-usage`, `collections-memory-estimation`, `applications/algorithms/machinery`, or `applications/services`
**THEN** the dossier SHALL identify where config parsing, graph loading, validation, memory estimation, algorithm execution, progress/resource tracking, and result consumption occur
**AND** SHALL state which pieces inform strict RAM rejection in v003
**SHALL** reject any RAM statement that lacks an estimator path, measurement method, or explicit uncertainty.

### REQ-GDSDOC-007.0: Create algorithm-family state matrix

**WHEN** inspecting `applications/algorithms`, `algo`, `algo-common`, `algo-params`, `algorithm-specifications`, or `algo-test`
**THEN** the dossier SHALL create a matrix by family: centrality, community, node embeddings, path finding, similarity, machine learning, miscellaneous algorithms, and Pregel where visible
**AND** SHALL mark each family's topology, node-property, relationship-property, scratch, result, model, and write/mutate requirements
**SHALL** distinguish source-backed facts from inferred storage pressure.

### REQ-GDSDOC-008.0: Document model and pipeline artifacts

**WHEN** inspecting `ml`, `pipeline`, `model-catalog-api`, `open-model-catalog`, `applications/model-catalog`, `proc/machine-learning`, or `proc/pipeline-catalog`
**THEN** the dossier SHALL describe model and pipeline catalog identity, lifecycle, procedure entry points, and dependency on graph/projection state
**AND** SHALL identify implications for sidecars, artifact catalogs, and snapshot generation references
**SHALL** flag any model or pipeline behavior that cannot be safely represented as topology.

### REQ-GDSDOC-009.0: Document write, mutate, and result behavior

**WHEN** inspecting `core-write`, `open-write-services`, `applications/operations`, or `applications/graph-store-catalog-results`
**THEN** the dossier SHALL describe write-back, mutate, result-store, and catalog-result behavior relevant to v003
**AND** SHALL map each behavior to `PublishedOlapSnapshot`, `ArtifactCatalog`, `ProjectionBuildStore`, or `ClientCompatibility`
**SHALL** flag any behavior that would violate the PRD rule that the Projection Build Store is not a user query store.

### REQ-GDSDOC-010.0: Extract test and doc oracles

**WHEN** inspecting `doc`, `doc-test`, `algo-test`, or module-local tests
**THEN** the dossier SHALL identify examples and tests that can become compatibility or parity fixtures
**AND** SHALL record expected behavior, input shape, output shape, and why the oracle matters
**SHALL** avoid copying large upstream examples or tests into the dossier.

### REQ-GDSDOC-011.0: Produce PRD impact matrix

**WHEN** a folder or anchor file is documented
**THEN** the dossier SHALL map it to at least one `prd-l1.md` acceptance area: API surface, catalog, properties, memory, publication, testing, OLTP boundary, OLAP boundary, or Projection Build Store boundary
**AND** SHALL include a recommended PRD wording impact: `Keep`, `Clarify`, `Add`, `Narrow`, or `Spike`
**SHALL** include a falsifier for architecture-critical recommendations.

### REQ-GDSDOC-012.0: Maintain evidence confidence tiers

**WHEN** the dossier records a claim
**THEN** it SHALL assign one confidence value: `DirectSource`, `GraphToolAssisted`, `DocsOnly`, `Inference`, `Speculation`, or `CandidateOnly`
**AND** SHALL include source path and symbol for `DirectSource` and `GraphToolAssisted`
**SHALL** reject `Speculation` as sufficient support for P0 folder recommendations.

### REQ-GDSDOC-013.0: Write checkpointed reading log

**WHEN** a P0 or P1 folder group is completed
**THEN** the dossier SHALL add a reading-log checkpoint with folders read, files read, graph-tool queries used, facts learned, unresolved questions, and next folder group
**AND** SHALL keep checkpoints short enough for a future agent to resume
**SHALL** not mark a folder group complete until at least one source-backed row exists or a low-yield reason is recorded.

### REQ-GDSDOC-014.0: Verify dossier completeness

**WHEN** the dossier is ready for final response
**THEN** the agent SHALL run verification commands that check file existence, required headings, required confidence labels, and evidence rows for all P0 folder groups
**AND** SHALL run `git diff --check`
**SHALL** report any skipped graph-tool run, timeout, or low-yield result in the final response.

## Test Matrix

| req_id | test_id | test_type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GDSDOC-001.0 | TEST-GDSDOC-001 | path validation | GDS repo path exists and is the only source repo cited | scope |
| REQ-GDSDOC-002.0 | TEST-GDSDOC-002 | tool-run audit | dossier records both graph-tool output directories and readiness status | graph tooling |
| REQ-GDSDOC-003.0 | TEST-GDSDOC-003 | evidence audit | every graph-derived claim has source verification or `CandidateOnly` | source truth |
| REQ-GDSDOC-004.0 | TEST-GDSDOC-004 | section review | public surface section covers `proc`, `procedures`, and `procedure-collector` | API surface |
| REQ-GDSDOC-005.0 | TEST-GDSDOC-005 | lifecycle review | projection/catalog section covers config, dense ids, schema, catalog set/get/remove, and memory reporting | catalog |
| REQ-GDSDOC-006.0 | TEST-GDSDOC-006 | memory review | memory/execution section names estimator and execution boundary evidence | strict RAM |
| REQ-GDSDOC-007.0 | TEST-GDSDOC-007 | matrix review | algorithm matrix includes required families and state-shape columns | algorithms |
| REQ-GDSDOC-008.0 | TEST-GDSDOC-008 | artifact review | model/pipeline section covers catalog identity and lifecycle implications | artifacts |
| REQ-GDSDOC-009.0 | TEST-GDSDOC-009 | plane review | write/mutate/result behaviors map to PRD planes and do not turn Build Store into serving path | plane boundaries |
| REQ-GDSDOC-010.0 | TEST-GDSDOC-010 | oracle review | dossier lists candidate doc/test parity oracles with input/output behavior | testing |
| REQ-GDSDOC-011.0 | TEST-GDSDOC-011 | PRD matrix review | every documented folder group maps to PRD acceptance areas and a wording impact | PRD rewrite |
| REQ-GDSDOC-012.0 | TEST-GDSDOC-012 | confidence review | no P0 recommendation relies only on `Speculation` | evidence quality |
| REQ-GDSDOC-013.0 | TEST-GDSDOC-013 | checkpoint review | every P0/P1 folder group has a reading-log checkpoint | resumability |
| REQ-GDSDOC-014.0 | TEST-GDSDOC-014 | final verification | required headings and P0 evidence rows exist, and `git diff --check` passes | completion |

## TDD Plan

### STUB

1. Create `docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md`.
2. Add required headings for tool readiness, P0 evidence, P1 evidence, PRD impact matrix, algorithm state matrix, artifact plane, test/doc oracles, reading log, and open questions.
3. Add empty tables with final column names before filling any prose.
4. Add placeholder rows marked `MissingEvidence` only in the first draft.

### RED

1. Run heading and table checks and confirm the new dossier fails while empty.
2. Run `rg "MissingEvidence"` and confirm the expected draft failures.
3. Run checks for P0 folder names and confirm each missing section is reported.
4. Run graph-tool readiness checks and confirm missing output directories are reported before indexing.

### GREEN

1. Fill graph-tool readiness rows after running both wrapper scripts.
2. Fill P0 rows first: public surface, projection/catalog, memory/execution.
3. Fill P1 rows next: algorithms, model/pipeline artifacts, write/mutate/results.
4. Fill P2 rows only after the P0/P1 architecture implications are clear.
5. Replace `MissingEvidence` with source-backed evidence, `LowYield`, or explicit open questions.

### REFACTOR

1. Collapse duplicate folder observations into PRD-impact rows.
2. Move detailed file-level notes behind summary tables so PRD rewrite guidance stays visible.
3. Normalize confidence labels and plane labels.
4. Keep sourced facts, inferences, and speculation in separate columns.

### VERIFY

1. Run `git diff --check`.
2. Verify the dossier file exists.
3. Verify all required headings exist.
4. Verify all P0 folder groups have at least one source-backed evidence row or a recorded low-yield reason.
5. Verify every `GraphToolAssisted` claim includes direct source verification.
6. Verify no P0 recommendation is supported only by `Speculation`.
7. Verify all remaining `MissingEvidence` rows are confined to an explicit open-questions section.

## Quality Gates

- [ ] The dossier scope is limited to `gitrefrepo/neo4j-gds-src`.
- [ ] Both graph tools are attempted with their wrapper scripts.
- [ ] Graph-tool output is classified as semantically ready, low-yield, timed out, or failed.
- [ ] Every graph-tool-derived claim is verified with direct source reads before being used as evidence.
- [ ] P0 folder groups all have evidence rows.
- [ ] Public API/procedure surface is separated from implementation support claims.
- [ ] Projection/catalog behavior is documented beyond topology/CSR.
- [ ] Memory/execution gates are documented with estimator or uncertainty evidence.
- [ ] Algorithm-family state matrix separates topology, properties, scratch, results, and model artifacts.
- [ ] Model/pipeline/write/mutate/result behavior is mapped to artifact or sidecar implications.
- [ ] Test/doc oracles are listed as candidate parity fixtures.
- [ ] PRD impact matrix links findings to `prd-l1.md` acceptance areas.
- [ ] No P0 recommendation relies only on speculation.
- [ ] `git diff --check` passes.

Suggested verification command shape:

```bash
test -d /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src
test -f docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md
rg -n "^## (Graph Tool Readiness|P0 Evidence|P1 Evidence|Algorithm State Matrix|PRD Impact Matrix|Reading Log|Open Questions)" docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md
rg -n "GraphToolAssisted|DirectSource|LowYield|CandidateOnly|Speculation" docs_PRD03/reference-learning/GDS-PRD-L1-Evidence-Dossier.md
git diff --check
```

## Open Questions

| question | why it remains open |
| --- | --- |
| Should the final dossier update `prd-l1.md` directly or remain a separate evidence packet first? | This spec assumes evidence first, rewrite later. |
| Should CodeGraphContext be given a longer timeout for this Java multi-module repo? | A prior run exceeded two minutes with no output; the future goal should record bounded behavior rather than silently waiting forever. |
| Should the dossier produce TSV companions for algorithm state and PRD impact? | Markdown is enough for a first pass, but TSV may help later automated checks. |
| Should the public surface inventory be regenerated from GDS source in this task? | Existing inventory files already exist, but the dossier may need a focused reconciliation against `proc` and `procedures`. |

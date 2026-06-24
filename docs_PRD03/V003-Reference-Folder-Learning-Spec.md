# v003 Reference Folder Learning Spec

This spec turns the v003 reference-repo study plan into executable learning
contracts. It exists so future research passes produce reusable engineering
context instead of loose summaries.

The learning objective is:

```text
Use local reference repositories to de-risk the v003 Rust Neo4j rewrite:
Neo4j-compatible API, Neo4j-shaped OLTP, Projection Build Store, published
OLAP snapshots, flat CSR plus sidecars, optional cells, snapshot catalog,
and strict holistic RAM accounting.
```

## Inputs

| input | value |
| --- | --- |
| Feature outcome | Produce implementation-useful learning artifacts from selected `gitrefrepo/` folders. |
| Actors | v003 implementer, architecture reviewer, compatibility tester, benchmark author. |
| Boundaries | Study local source folders only unless a requirement explicitly requests external verification. |
| Failure modes | Reading too broadly, summarizing without evidence, missing GDS ABI details, confusing inspiration with requirements, overweighting peripheral repos, creating untraceable notes. |
| Reliability limit | Every learning claim must identify source path, symbol, and why it affects v003. |
| Runtime constraint | No production Rust code is required by this spec; generated helper scripts must use four-word names when introduced later. |
| Model assumption | A future agent may be weaker than the authoring agent; prompts must be explicit, bounded, repetitive where useful, and verification-first. |

## Agent Goal Packet

Use this section as the handoff prompt when assigning the learning goal to a
future agent.

```text
You are studying the local `gitrefrepo/` shelf and current Knight Bus source to
produce implementation-useful evidence for `docs_PRD03/prd-l1.md`.

Your task is not to summarize repositories. Your task is to extract source-backed
engineering decisions that help Knight Bus v003 achieve:

1. Neo4j-compatible API behavior where support is claimed.
2. Neo4j-shaped OLTP storage as transactional truth.
3. GDS/OLAP reads only from published immutable OLAP snapshots.
4. Projection Build Store as build/control plane, never a user query store.
5. Complete GDS surface inventory before claiming architecture sufficiency.
6. Flat CSR plus sidecars as the first physical snapshot path.
7. Snapshot generations, watermarks, atomic publication, rollback, and retention.
8. Strict holistic RAM accounting for 50GB-class logical graphs on 8GB-class
   machines when the selected plan honestly fits.

For every repository or folder studied:
- cite exact local paths and symbols;
- separate sourced facts, local inference, and speculation;
- map findings to PRD03 outcomes;
- state whether the finding affects OLTP, Projection Build Store, OLAP snapshot,
  GDS/API surface, memory, benchmark, publication, or operations;
- include one skeptical note that could falsify the finding;
- avoid copying upstream code except tiny symbol names or short snippets.

Do not narrow the intended GDS surface. Inventory all visible GDS surface area
first. Stage implementation later.
```

## Weak-Model Operating Contract

Assume the next agent is competent but not deeply context-rich. It may forget
constraints, over-summarize, or chase broad repositories. Give it smaller tasks,
strict outputs, and hard verification gates.

| weak-model risk | required guardrail |
| --- | --- |
| Reads too much and summarizes vaguely | Assign one lane, one repo family, and one output table per pass. |
| Treats inspiration as architecture | Require `sourced_fact`, `inference`, and `speculation` columns. |
| Shrinks GDS surface to easy kernels | Require full procedure/mode/config/result inventory before support claims. |
| Confuses OLTP, Build Store, and OLAP | Require every claim to map to exactly one PRD plane first, then optional secondary planes. |
| Misses hidden RAM | Require heap, RSS/page-cache, direct-buffer, scratch, sidecar, spill, and algorithm-state fields. |
| Hallucinates source evidence | Require path plus symbol or `rg` query that another agent can rerun. |
| Loses progress across context windows | Require checkpoint summaries after every 20 evidence rows or every repository family. |
| Uses graph tools too broadly | Use graph-index tools for the current Knight Bus repo only unless explicitly asked to index references. Use `rg` for targeted `gitrefrepo/` study. |

### Mandatory Turn Shape

Every future agent pass SHALL follow this shape:

```text
1. Restate assigned lane and PRD outcome.
2. List exact local folders to inspect.
3. Run path validation.
4. Run targeted `rg` discovery.
5. Read only the top files needed for evidence.
6. Fill evidence rows.
7. Fill architecture fit rows.
8. Run skeptical review.
9. Run verification commands.
10. Write a checkpoint summary with unresolved risks.
```

### Local Tooling Contract

For current Knight Bus source orientation, the agent MAY use the local Codex
skills installed outside this repository:

```text
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md
```

Default smoke commands:

```bash
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
```

Rules:

- These graph tools are for the current Knight Bus repo by default.
- Do not index all `gitrefrepo/` folders with these graph tools unless a human
  explicitly asks for that separate, slower pass.
- For reference repositories, use `rg`, `find`, `git -C`, and targeted file
  reads first.
- Graph-tool claims are candidate evidence only. Verify with source paths.

## PRD L1 Outcome Spine

Every study pass must explicitly support or challenge at least one of these
outcomes.

| PRD L1 outcome | Evidence the agent must seek |
| --- | --- |
| Neo4j-compatible API | Bolt, drivers, Cypher, procedures, values, errors, sessions, routing, retries, generated-query clients, and GDS/APOC-facing workflows. |
| Neo4j-shaped OLTP | Records, cursors, locks, WAL, transactions, rollback, indexes, dense-node handling, page-cache behavior, and operational constraints. |
| Published OLAP snapshots | Flat CSR, sidecars, manifests, source watermarks, deterministic read paths, mmap fast mode, strict-RAM stream mode, and no query-time write reconciliation. |
| Projection Build Store | Durable analytical facts, dictionaries, sorted runs, build scratch, validation, reproducibility, crash recovery, and compiler inputs. |
| Complete GDS surface | Procedure names, modes, configs, result schemas, estimates, mutate/write/model/pipeline semantics, catalog behavior, and deterministic unsupported behavior. |
| Strict holistic RAM | Heap, RSS, page cache, direct buffers, mmap residency, duplicate layouts, build scratch, sidecars, result/model artifacts, spill, and algorithm state. |
| Atomic publication | Generation catalog, active pointer swap, reader pinning, retention, rollback, restart recovery, validation gates, and exact watermark reporting. |

## Coverage Lanes

Surface coverage is exhaustive. Proof work is staged. Do not confuse those two.

| lane | purpose | completion rule |
| --- | --- | --- |
| Surface lane | Inventory every known Neo4j/GDS/APOC/client-visible surface. | No procedure or mode is left unknown; each row has support status and evidence. |
| Kernel lane | Trace each algorithmic procedure from public entrypoint to implementation, estimator, graph interfaces, and state objects. | No algorithm support claim is accepted without procedure-to-kernel evidence. |
| Capability lane | Classify what each surface item requires from storage/runtime. | Each row names topology, sidecar, catalog, writeback, model, memory, and algorithm-state needs. |
| Architecture lane | Decide which physical strategy can support the capability. | Each row maps to flat CSR, sidecars, cells, GraphBLAS, out-of-core stream, spill, model artifact, or unsupported reason. |
| Proof lane | Choose representative implementation and benchmark spikes. | Spikes validate risk without pretending unimplemented surface does not exist. |
| Rejection lane | Document architectures or repo ideas that conflict with PRD03. | Each rejection cites the PRD constraint it violates. |

## Required Deliverables

Every completed study batch SHALL emit the following artifacts, either as one
document or as a small linked set of Markdown/TSV files:

| artifact | purpose | required when |
| --- | --- | --- |
| Evidence ledger | Capture source-backed facts, inference, speculation, PRD impact, and skeptical notes. | always |
| Architecture fit matrix | Map capability to topology, sidecars, Build Store, catalog, state, memory, execution, support, and falsifier. | always |
| Procedure-to-kernel ledger | Trace GDS procedures to config, results, estimates, specs, kernels, and storage implications. | any GDS algorithm study |
| Checkpoint summary | Make the next agent resumable without rereading the whole context. | any batch that is non-trivial or context-heavy |
| PRD outcome traceability dossier | Map findings back to PRD L1 outcomes and missing evidence. | always |
| Rejected-alternative note | Explain what was considered and why it was not adopted. | whenever a design alternative is discussed |

## Reusable Study Prompts

### Weak-Model Master Prompt

Use this exact prompt when assigning a broad study pass to a weaker model:

```text
You are not allowed to produce a general summary.

You must produce implementation-useful evidence for Knight Bus v003, grounded in
local files under `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`.

Primary PRD file:
`docs_PRD03/prd-l1.md`

Learning spec:
`docs_PRD03/V003-Reference-Folder-Learning-Spec.md`

Your assigned lane is: <SURFACE | KERNEL | CAPABILITY | ARCHITECTURE | PROOF | REJECTION>.
Your assigned repository family is: <FILL THIS IN>.
Your assigned PRD outcome is: <FILL THIS IN>.

Do exactly this:
1. Validate that every required local path exists.
2. Use `rg` to find symbols before reading files.
3. Read only files needed to fill evidence rows.
4. Produce an evidence ledger with these columns:
   claim_id | req_id | source_type | source_path | symbol_or_query |
   sourced_fact | inference | speculation | PRD impact | skeptical note
5. Produce an architecture fit matrix with these columns:
   capability | topology_need | sidecar_need | build_store_need |
   snapshot_catalog_need | algorithm_state | memory_plan |
   execution_strategy | support_status | falsifier
6. For GDS algorithm work, produce a procedure-to-kernel ledger with:
   procedure_name | mode | config_type | result_type | estimate_path |
   algorithm_spec | implementation_class | graph_interfaces |
   topology_orientation | sidecar_inputs | dominant_state |
   mutate_write_target | oracle_test | storage_implication | ram_risk
7. Separate facts from inferences. Never merge them.
8. If evidence is missing, write `MissingEvidence`, not a guess.
9. If support is hard, write `NeedsArchitectureSpike`, not unsupported.
10. If support is outside PRD03, write `ExplicitlyOutOfScope` and cite why.
11. End with verification commands run and unresolved risks.

Do not claim v003 supports all GDS unless you inventoried procedures, modes,
configs, result schemas, estimates, graph catalog, mutate/write, model catalog,
pipelines, and operations.
```

### Weak-Model Single-Repo Prompt

```text
Study only this local repository:
`gitrefrepo/<REPO-NAME>`

Goal:
Extract evidence relevant to `<PRD_OUTCOME>` and `<REQ-LEARN-ID>`.

Do not summarize the repo. Fill tables.

Required steps:
1. Run `git -C gitrefrepo/<REPO-NAME> rev-parse --short HEAD`.
2. Run `find gitrefrepo/<REPO-NAME> -maxdepth 2 -type d | sort | head -80`.
3. Run 3 to 8 targeted `rg` commands for the assigned symbols.
4. Read the smallest files that answer the question.
5. Produce exactly 5 to 15 evidence rows.
6. Produce exactly 3 to 10 v003 implications.
7. Mark each implication `Adopt`, `Adapt`, `Reject`, `Watch`, or `MissingEvidence`.
8. Add one skeptical falsifier per implication.
```

### Weak-Model GDS Surface Prompt

```text
Inventory GDS as an ABI, not as algorithms.

Search `gitrefrepo/neo4j-gds-src` for public procedure annotations, facade
classes, estimate procedures, result classes, config classes, graph catalog
operations, model catalog operations, pipeline procedures, mutate/write modes,
and operations/progress procedures.

Return rows with:
procedure_name | facade_family | mode | estimate_variant | config_type |
result_type | writes_catalog | writes_oltp | creates_model | source_file |
support_status | architecture_needs | oracle_test

Allowed support statuses:
Implemented, UnsupportedButRegistered, DeferredWithReason,
NeedsArchitectureSpike, ExplicitlyOutOfScope.

Forbidden:
- Do not collapse procedures into "PageRank etc."
- Do not omit mutate/write/model/pipeline procedures.
- Do not claim storage sufficiency from topology-only CSR.
```

### Weak-Model Procedure-To-Kernel Prompt

```text
Trace GDS algorithms from public procedure to actual implementation.

Your goal is to learn what storage and memory v003 needs by studying how Neo4j
GDS implements each algorithm, not by guessing from the algorithm name.

For each public GDS algorithm procedure or mode:
1. Find the procedure entrypoint and facade family.
2. Find the configuration class and default values.
3. Find the result type and output columns.
4. Find the estimate procedure and memory estimator path.
5. Find the algorithm specification or algorithm factory.
6. Find the implementation/kernel class that performs the work.
7. Identify graph interfaces used by the kernel.
8. Identify topology orientation requirements: forward, reverse, undirected,
   weighted, typed, filtered, or projected.
9. Identify sidecar inputs: labels, relationship types, weights, scalar
   properties, vector properties, embeddings, communities, or model artifacts.
10. Identify dominant runtime state: per-node vector, frontier, bitset,
    priority queue, top-K heap, candidate pairs, contracted graph, walk corpus,
    dense embedding matrix, model state, or spill file.
11. Identify mutate/write/model/catalog side effects.
12. Map the evidence to v003 storage: flat CSR, reverse CSR, property sidecar,
    result sidecar, model artifact, snapshot catalog, Build Store statistics,
    out-of-core stream, spill, cells, GraphBLAS, or UnsupportedButRegistered.

Return a table. Do not write an architecture recommendation until this table is
filled for the relevant algorithm family.
```

### Weak-Model Verification Prompt

```text
Before finalizing, attack your own artifact.

Answer yes/no with evidence:
1. Does every claim have a source path and symbol or query?
2. Did you separate sourced fact, inference, and speculation?
3. Did any claim accidentally make the Build Store a serving path?
4. Did any GDS claim omit modes, configs, results, estimates, or catalog effects?
5. Did any algorithm claim omit the implementation/kernel class?
6. Did any algorithm claim omit graph interfaces, sidecars, state shape, or estimator path?
7. Did any RAM claim omit page cache, mmap residency, scratch, spill, or state?
8. Did any benchmark claim lack workload, scale, validation, or command?
9. Did any architecture recommendation duplicate full topology by default?
10. Did any unsupported decision really mean `NeedsArchitectureSpike`?

If any answer is unsafe, revise the artifact before returning it.
```

### Full Surface Inventory Prompt

```text
Inventory the complete public surface for this area. For every procedure,
command, client behavior, config, result schema, mode, estimate path, mutation
path, model artifact, and catalog operation you find, create one row with:

name | family | mode | config inputs | result columns | source file |
support status | storage needs | memory needs | test oracle | PRD impact

Do not skip procedures because implementation looks hard. Hard means the row
gets `NeedsArchitectureSpike`, not that it disappears.
```

### Repository Study Prompt

```text
Study this repository as evidence, not inspiration theater.

1. Identify the 5 to 12 most relevant directories for the assigned PRD outcome.
2. Use `rg` to find exact symbols and concepts before reading large files.
3. Capture short source-backed facts with local path and line/symbol.
4. Convert each fact into a v003 implication.
5. Add one skeptical counterpoint per major implication.
6. End with: adopt, adapt, reject, or watch.
```

### Architecture Fit Prompt

```text
For each GDS/API capability, decide what v003 needs physically:

- flat CSR topology only;
- flat CSR plus sidecars;
- snapshot generation/catalog behavior;
- Projection Build Store fact/dictionary/stat support;
- cellular packaging or partition-local metadata;
- GraphBLAS/sparse-matrix execution;
- out-of-core stream/spill strategy;
- result/model sidecar;
- OLTP writeback path;
- UnsupportedButRegistered behavior.

Return a matrix. Do not write prose where a row would make the claim falsifiable.
```

### Skeptical Engineer Prompt

```text
Attack the proposed learning conclusion.

Ask:
- Does this accidentally serve OLAP from the Build Store?
- Does this silently require zero-lag OLAP?
- Does this hide page cache or mmap residency behind "low heap"?
- Does this duplicate full topology?
- Does this shrink the GDS surface without admitting it?
- Does this assume GraphBLAS/cells/vector search because it is fashionable?
- What benchmark or source line would falsify this claim?
```

### Evidence Ledger Template

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CLAIM-001 | REQ-LEARN-000.0 | source | `gitrefrepo/...` | `rg query or symbol` | verifiable fact | local implication | optional future idea | affected PRD outcome | falsifier or caveat |

### Architecture Fit Matrix Template

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `gds.<family>.<mode>` | flat CSR / reverse CSR / none | labels/types/properties/results/models | facts/dictionaries/sorted runs/statistics | generation/watermark/catalog/writeback | vectors/frontiers/heaps/models | heap/RSS/page-cache/direct/scratch/spill | mmap / direct stream / spill / GraphBLAS / sidecar scan | NeedsArchitectureSpike | source or benchmark that would overturn the row |

### Procedure-To-Kernel Ledger Template

| procedure_name | mode | config_type | result_type | estimate_path | algorithm_spec | implementation_class | graph_interfaces | topology_orientation | sidecar_inputs | dominant_state | mutate_write_target | oracle_test | storage_implication | ram_risk | source_paths |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `gds.pageRank.stream` | stream | config class | result class | estimator path | spec/factory | kernel class | graph API used | forward/reverse/weighted/etc. | weights/properties/etc. | vectors/frontier/etc. | none/sidecar/OLTP/model | test fixture | required v003 storage shape | low/medium/high/impossible-without-spill | source files or `rg` queries |

### Storage-Demand Decision Table

| observed implementation need | v003 storage implication |
| --- | --- |
| Sequential scan over all relationships | Flat global CSR stream or cell global-stream adapter; no extra persistent layout by default. |
| Reverse-neighbor access | Reverse CSR or reversible edge stream required. |
| Weighted relationship access | Relationship property/weight sidecar required; topology alone is insufficient. |
| Label/type/property filters | Columnar sidecars plus projection catalog filters required. |
| Per-node scalar/vector state | Estimate per-node arrays explicitly; spill or reject when budget cannot fit. |
| Priority queue or frontier state | Bounded scratch and spill policy required. |
| Pairwise candidate generation | Blocking/top-K/spill strategy required; reject unbounded `O(n^2)` materialization. |
| Community contraction | Contracted-graph scratch or artifact required; account for duplicate topology risk. |
| Random walks or embeddings | Walk corpus, RNG determinism, vector sidecars, and model/result artifacts required. |
| Mutate/write modes | Result sidecar, projected graph catalog mutation, or OLTP writeback path required. |

### Checkpoint Summary Template

````markdown
## Checkpoint: <lane> / <repo family> / <date>

Assigned requirement IDs:
- REQ-LEARN-...

Evidence rows completed:
- <count>

Most important sourced facts:
- <path> :: <symbol> -> <fact>

Architecture implications:
- Adopt:
- Adapt:
- Reject:
- Watch:
- MissingEvidence:

Verification commands run:
```bash
<commands>
```

Unresolved risks:
- <risk plus falsifier>
````

## Executable Requirements

### REQ-LEARN-001.0: Preserve PRD03 Architecture Boundaries

**WHEN** a reference folder is studied
**THEN** the learning artifact SHALL classify findings under OLTP, Projection Build Store, OLAP snapshot, GDS/API surface, or benchmark/operations
**AND** SHALL state whether the finding changes `docs_PRD03/prd-l1.md` or `docs_PRD03/Arch-options.md`
**SHALL** reject findings that imply OLAP queries should read the Projection Build Store directly.

### REQ-LEARN-002.0: Study Neo4j OLTP Storage First

**WHEN** studying Neo4j-shaped OLTP compatibility
**THEN** the pass SHALL cover `gitrefrepo/neo4j-src/community/record-storage-engine`, `community/kernel`, `community/kernel-api`, `community/storage-engine-util`, `community/io`, and `community/collections`
**AND** SHALL extract record, cursor, transaction, page-cache, and dense-node facts with file paths
**SHALL** produce at least one implication for Rust OLTP storage boundaries.

### REQ-LEARN-003.0: Study Bolt And Driver Compatibility

**WHEN** studying zero-application-change client compatibility
**THEN** the pass SHALL cover `gitrefrepo/neo4j-src/community/bolt`, `gitrefrepo/neo4j-docs-bolt-src`, `gitrefrepo/neo4j-testkit-src`, and official driver repos under `gitrefrepo/neo4j-*-driver-src`
**AND** SHALL identify handshake, auth, session, transaction, routing, result streaming, error, and retry semantics
**SHALL** produce a compatibility checklist that can later become integration tests.

### REQ-LEARN-004.0: Study Cypher Compatibility Surface

**WHEN** studying Cypher support
**THEN** the pass SHALL cover `gitrefrepo/neo4j-src/community/cypher/front-end`, `cypher-planner`, `cypher-logical-plans`, `physical-planning`, `slotted-runtime`, `interpreted-runtime`, `runtime-spec-suite`, and `compatibility-spec-suite`
**AND** SHALL separate parser grammar, semantic analysis, planning, runtime, and conformance tests
**SHALL** mark any unsupported Cypher feature as `UnsupportedButRegistered` or out-of-scope with evidence.

### REQ-LEARN-005.0: Study Neo4j Procedure And Value Semantics

**WHEN** studying procedure compatibility
**THEN** the pass SHALL cover `gitrefrepo/neo4j-src/community/procedure`, `procedure-api`, `procedure-compiler`, and `values`
**AND** SHALL extract argument binding, result marshalling, type conversion, error, and annotation behavior
**SHALL** explain how GDS/APOC-style procedures should attach to the Rust server.

### REQ-LEARN-006.0: Inventory GDS Public ABI

**WHEN** studying GDS support
**THEN** the pass SHALL cover `gitrefrepo/neo4j-gds-src/proc`, `procedures`, `procedures/*-facade*`, `applications/algorithms`, and `applications/graph-store-catalog`
**AND** SHALL produce a procedure inventory with name, mode, config shape, result shape, facade family, estimate variant, source file, and support level
**SHALL** distinguish unknown procedure from `UnsupportedButRegistered`.

### REQ-LEARN-007.0: Study GDS Graph Store And Projection Mechanics

**WHEN** studying OLAP projection mechanics
**THEN** the pass SHALL cover `gitrefrepo/neo4j-gds-src/core`, `core-api`, `native-projection`, `legacy-cypher-projection`, `graph-projection-api`, `triplet-graph-builder`, `graph-schema-api`, and `graph-dimensions`
**AND** SHALL map each GDS graph-store concept to a v003 artifact: flat CSR, sidecar, catalog entry, Projection Build Store fact, or snapshot generation
**SHALL** identify projection cases that cannot be represented by topology alone.

### REQ-LEARN-008.0: Study GDS Memory Estimation

**WHEN** studying the 50GB-on-8GB RAM promise
**THEN** the pass SHALL cover `gitrefrepo/neo4j-gds-src/executor`, `memory-estimation`, `memory-usage`, and `collections-memory-estimation`
**AND** SHALL extract graph-loading memory, algorithm memory, concurrency, and result-memory accounting patterns
**SHALL** produce formulas or pseudocode for v003 `estimate` behavior.

### REQ-LEARN-009.0: Study GDS Algorithm Families By State Shape

**WHEN** studying GDS algorithms
**THEN** the pass SHALL cover `gitrefrepo/neo4j-gds-src/algo`, `algorithm-specifications`, `algo-params`, and `proc/{centrality,community,path-finding,similarity,embeddings,machine-learning}`
**AND** SHALL classify each family by graph access pattern, dominant state shape, sidecar needs, spill strategy, and 8GB risk
**SHALL** avoid claiming support until an oracle test and memory estimate are named.

### REQ-LEARN-010.0: Study Mutate, Write, Model, And Pipeline Semantics

**WHEN** studying GDS operations that change state
**THEN** the pass SHALL cover `gitrefrepo/neo4j-gds-src/core-write`, `model-catalog-api`, `open-model-catalog`, `pipeline`, and `ml`
**AND** SHALL define whether v003 stores results as projected graph sidecars, model artifacts, new snapshot generations, or OLTP writeback
**SHALL** specify conflict behavior when the active OLAP snapshot watermark is older than current OLTP.

### REQ-LEARN-011.0: Study Projection Build Store Precedents

**WHEN** studying the durable analytical IR
**THEN** the pass SHALL cover `gitrefrepo/apache-iggy-src`, `rocksdb-src`, `fjall-src`, `redb-src`, and `tikv-src`
**AND** SHALL compare append log, LSM, copy-on-write tree, segment, checkpoint, compaction, and recovery patterns
**SHALL** recommend one minimal v003 Build Store shape and two rejected alternatives.

### REQ-LEARN-012.0: Study Columnar Sidecar Precedents

**WHEN** studying labels, relationship types, weights, properties, results, and model columns
**THEN** the pass SHALL cover `gitrefrepo/apache-arrow-rs-src/arrow-array`, `arrow-buffer`, `arrow-ipc`, `parquet`, `gitrefrepo/apache-parquet-format-src`, and `gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet`
**AND** SHALL define sidecar physical types, null handling, dictionary encoding, vector property layout, mmap/stream compatibility, and memory estimates
**SHALL** reject sidecar designs that duplicate full topology by default.

### REQ-LEARN-013.0: Study Query Planning Patterns Carefully

**WHEN** studying reusable query-engine ideas
**THEN** the pass SHALL cover `gitrefrepo/apache-datafusion-src/datafusion/{catalog,expr,sql,optimizer,physical-plan,execution,session}`
**AND** SHALL identify only the planning patterns useful for graph projection catalog, sidecar scans, filter pushdown, and physical plan explanation
**SHALL** not imply v003 should become a SQL engine or put DataFusion on the OLTP path.

### REQ-LEARN-014.0: Study Compact Graph Competitors Second

**WHEN** comparing graph database competitors
**THEN** the pass SHALL cover `gitrefrepo/kuzu-src`, `ladybug-src`, `falkordb-src`, `memgraph-src`, and `age-src`
**AND** SHALL compare storage model, query surface, projection/catalog behavior, procedure model, and operational tradeoffs
**SHALL** label all competitor-derived ideas as inspiration unless they are compatible with PRD03 boundaries.

### REQ-LEARN-015.0: Study Algorithm Baselines After GDS Surface

**WHEN** studying graph algorithm baselines
**THEN** the pass SHALL cover `gitrefrepo/gapbs-src`, `snap-src`, `lagraph-src`, and `graphblas-src`
**AND** SHALL extract oracle fixtures, benchmark shapes, access patterns, and memory-state formulas
**SHALL** prioritize correctness and memory estimates before performance claims.

### REQ-LEARN-016.0: Produce Evidence-Ledger Artifacts

**WHEN** a study pass is complete
**THEN** it SHALL produce a Markdown evidence ledger with source paths, symbols, line ranges or `rg` queries, local inference, and v003 decision impact
**AND** SHALL include at least one skeptical note
**SHALL** avoid copying large upstream code blocks.

### REQ-LEARN-017.0: Maintain Traceability

**WHEN** a learning artifact affects architecture
**THEN** it SHALL reference the exact `REQ-LEARN-*` IDs it satisfies
**AND** SHALL point to affected PRD03 lines or sections
**SHALL** update or create follow-up issues before implementation begins.

### REQ-LEARN-018.0: Preserve Four-Word Naming For Future Helpers

**WHEN** helper scripts or Rust functions are introduced to support this learning workflow
**THEN** new internal helper names SHALL follow four-word naming where feasible
**AND** examples SHALL include `scan_reference_folders_only`, `extract_symbol_evidence_table`, `validate_requirement_traceability_now`, and `summarize_learning_checkpoint_markdown`
**SHALL** preserve external API names when compatibility requires them.

### REQ-LEARN-019.0: Study Ladybug Embedded Graph Patterns

**WHEN** studying embedded graph OLAP and columnar CSR precedents
**THEN** the pass SHALL cover `gitrefrepo/ladybug-src/docs/icebug-disk.md`, `docs/index_build_recovery.md`, `docs/morsel_parallelism.md`, `src/storage`, `src/transaction`, `src/planner`, `src/optimizer`, `src/processor`, and `src/graph`
**AND** SHALL extract lessons for immutable Parquet graph snapshots, CSR `indices`/`indptr`, flat relationship layouts, morsel-driven scans, checkpoint/WAL index recovery, vectorized or factorized execution, and single-writer/snapshot-isolation tradeoffs
**SHALL** label Ladybug findings as implementation inspiration, not Neo4j/GDS compatibility evidence.

### REQ-LEARN-020.0: Study GDS User Workflow Compatibility

**WHEN** studying whether real GDS user workflows can run without client-side changes
**THEN** the pass SHALL cover `gitrefrepo/neo4j-gds-client-src`, `gitrefrepo/graph-data-science-src`, `gitrefrepo/gds-agent-src`, and `gitrefrepo/neo4j-graph-examples` when present
**AND** SHALL extract client call builders, graph catalog workflows, algorithm invocation patterns, notebook-style assumptions, and procedure orchestration examples
**SHALL** classify every finding as `P0 workflow compatibility`, `P1 helpful ergonomics`, or `out-of-scope example`.

### REQ-LEARN-021.0: Study APOC Support Boundary

**WHEN** studying procedure expectations outside core GDS
**THEN** the pass SHALL cover `gitrefrepo/neo4j-apoc-src` and `gitrefrepo/neo4j-apoc-procedures-src`
**AND** SHALL inventory procedure categories, common argument/result shapes, packaging assumptions, error behavior, and high-use workflows
**SHALL** define whether each APOC category is `Implemented`, `UnsupportedButRegistered`, `AliasToNative`, or `ExplicitlyOutOfScope`.

### REQ-LEARN-022.0: Study Client Ecosystem Canaries

**WHEN** studying zero-application-change compatibility beyond official drivers
**THEN** the pass SHALL cover `gitrefrepo/cypher-shell-src`, `gitrefrepo/cypher-dsl-src`, `gitrefrepo/neo4rs-src`, `gitrefrepo/neo4j-ogm-src`, and `gitrefrepo/neo4j-browser-src`
**AND** SHALL extract generated Cypher shapes, CLI behavior, Rust Bolt ergonomics, object-mapping assumptions, browser smoke paths, auth/session expectations, and result consumption behavior
**SHALL** produce a canary suite proposal with at least one CLI, one generated-query, one Rust-client, and one application-style workflow.

### REQ-LEARN-023.0: Study LDBC Benchmark Contracts

**WHEN** defining credible OLTP and OLAP benchmark claims
**THEN** the pass SHALL cover `gitrefrepo/ldbc_graphalytics-src`, `ldbc_graphalytics_docs-src`, `ldbc_graphalytics_platforms_graphblas-src`, `ldbc_snb_interactive_v1_driver-src`, `ldbc_snb_interactive_v1_impls-src`, `ldbc_snb_interactive_v2_driver-src`, and `ldbc_snb_interactive_v2_impls-src`
**AND** SHALL extract workload semantics, validation rules, scale factors, driver hooks, output correctness checks, and reporting conventions
**SHALL** reject benchmark claims that cannot be mapped to a named workload, dataset scale, validation rule, and repeatable command.

### REQ-LEARN-024.0: Study Low-RAM Out-Of-Core Graph Systems

**WHEN** testing the 50GB-on-8GB OLAP story against prior art
**THEN** the pass SHALL cover `gitrefrepo/graphchi-cpp-src`, `gridgraph-src`, `minigraph-src`, and `thunderrw-src`
**AND** SHALL compare shard/grid/window layouts, streaming algorithms, random-walk pressure, update/freshness limitations, I/O scheduling, and explicit memory budgets
**SHALL** identify which lessons favor flat global streams, which favor cells/tiles, and which argue for rejecting extra persistent layouts.

### REQ-LEARN-025.0: Study GraphBLAS Alternative Substrate

**WHEN** deciding whether GDS algorithms should execute as custom CSR kernels or sparse linear algebra plans
**THEN** the pass SHALL cover `gitrefrepo/graphblas-src`, `lagraph-src`, `graphblas-pointers-src`, `ldbc_graphalytics_platforms_graphblas-src`, `python-graphblas-src`, `graphblas_sparse_linear_algebra-src`, `falkordb-src`, and `redisgraph-src`
**AND** SHALL map algorithm families to GraphBLAS expressions, CSR-native kernels, hybrid plans, FFI options, and memory-state formulas
**SHALL** recommend GraphBLAS adoption, rejection, or limited use per algorithm family rather than one global answer.

### REQ-LEARN-026.0: Study Rust Graph Fixture Scaffolding

**WHEN** building test fixtures, oracle graphs, or small in-memory adapters for v003
**THEN** the pass SHALL cover `gitrefrepo/petgraph-src`, `rustworkx-src`, `sprs-src`, `sparsetools-src`, `networkit-src`, and `igraph-src`
**AND** SHALL extract fixture patterns, graph import/export helpers, CSR/CSC helpers, algorithm-oracle APIs, and Python-facing ergonomics
**SHALL** label these repos as scaffolding or oracle references, not as v003 storage architecture.

### REQ-LEARN-027.0: Study RAM Observability Precedents

**WHEN** turning lowest-RAM claims into measurable contracts
**THEN** the pass SHALL cover `gitrefrepo/tracing-src`, `jemalloc-src`, and relevant memory-reporting paths in `neo4j-gds-src`, `duckdb-src`, `clickhouse-src`, and `ladybug-src`
**AND** SHALL define how v003 reports heap, RSS, allocator stats, page-cache exposure, direct I/O buffers, scratch, sidecars, deltas, and algorithm state
**SHALL** require every RAM claim to name a measurement source, sampling interval, workload phase, and pass/fail threshold.

### REQ-LEARN-028.0: Study Rejected Live-Incremental Architectures

**WHEN** evaluating alternatives to published OLAP snapshots
**THEN** the pass SHALL cover `gitrefrepo/differential-dataflow-src`, `timely-dataflow-src`, `materialize-src`, and `risingwave-src`
**AND** SHALL extract incremental view, dataflow scheduling, state compaction, checkpoint, and recovery patterns that conflict with or complement PRD03
**SHALL** produce a rejected-alternative note explaining why v003 does or does not adopt live query-time reconciliation.

### REQ-LEARN-029.0: Study Graph-Vector Market Watch

**WHEN** assessing whether graph plus vector or full-text behavior should affect v003 scope
**THEN** the pass SHALL cover `gitrefrepo/helix-db-src`, `nornicdb-src`, `qdrant-src`, `tantivy-src`, and any Neo4j vector-index paths already cloned
**AND** SHALL extract graph-vector query expectations, index/storage tradeoffs, GraphRAG workflow assumptions, and full-text/vector sidecar implications
**SHALL** keep graph-vector findings out of P0 unless PRD03 explicitly changes scope.

### REQ-LEARN-030.0: Exhaust Full GDS Surface Before Sufficiency Claims

**WHEN** any study artifact claims flat CSR, sidecars, cells, GraphBLAS, or another physical plan can support GDS
**THEN** the artifact SHALL first reference a complete GDS surface inventory covering procedures, modes, configs, result schemas, estimates, graph catalog, mutate/write behavior, model catalog, pipelines, and operations
**AND** SHALL classify every row as `Implemented`, `UnsupportedButRegistered`, `DeferredWithReason`, or `NeedsArchitectureSpike`
**SHALL** reject architecture sufficiency claims that only prove a small algorithm subset.

### REQ-LEARN-031.0: Study Snapshot Publication Catalog

**WHEN** studying published OLAP snapshot storage
**THEN** the pass SHALL cover current Knight Bus snapshot manifests plus reference patterns from `gitrefrepo/ladybug-src`, `duckdb-src`, `clickhouse-src`, `redb-src`, `rocksdb-src`, and `fjall-src`
**AND** SHALL define active generation pointers, source watermarks, manifest validation, atomic swap, reader pinning, retention, rollback, garbage collection, restart recovery, and corruption handling
**SHALL** produce tests or test designs where readers see generation N or N+1 but never a half-built generation.

### REQ-LEARN-032.0: Study Current Knight Bus CSR Seed

**WHEN** grounding external reference learning in the existing codebase
**THEN** the pass SHALL cover current Knight Bus source, tests, benchmarks, README claims, snapshot writer, manifest, mmap runtime, low-RAM builder behavior, and current walk query surface
**AND** SHALL identify which v003 concepts already exist, which need extension, and which external ideas would conflict with the working seed
**SHALL** treat the current flat CSR runtime as the first oracle for topology parity unless evidence overturns it.

### REQ-LEARN-033.0: Produce Agent-Ready Study Prompts

**WHEN** a future agent receives this document as a goal
**THEN** the document SHALL provide reusable prompts for full-surface inventory, repository study, architecture fit, skeptical review, and evidence-ledger capture
**AND** SHALL state expected output tables, required claim classification, PRD impact mapping, and verification commands
**SHALL** be sufficient for an agent to start without asking which folders, claims, or PRD constraints matter.

### REQ-LEARN-034.0: Produce Architecture Fit Matrix

**WHEN** a study pass covers a GDS family, client surface, or storage reference
**THEN** it SHALL produce a matrix mapping capability to required topology, sidecars, Projection Build Store facts, snapshot catalog behavior, algorithm state, memory plan, execution strategy, and support status
**AND** SHALL compare flat CSR, flat CSR plus sidecars, cellular packaging, GraphBLAS, out-of-core streaming, spilling, and result/model sidecars where relevant
**SHALL** state the minimum architecture needed for each capability rather than promoting one global architecture by default.

### REQ-LEARN-035.0: Separate Source Inference And Speculation

**WHEN** recording any learning claim
**THEN** the claim SHALL identify sourced fact, local inference, and speculation in separate fields
**AND** SHALL include source path, symbol or query, PRD impact, and skeptical note
**SHALL** reject claims whose evidence cannot be found again by another agent using local files or an explicit external URL.

### REQ-LEARN-036.0: Maintain Local Clone Coverage Ledger

**WHEN** a study pass references a repository
**THEN** it SHALL record local path, upstream URL when known, branch, commit, clone status, study role, and whether the repo is compatibility oracle, implementation precedent, benchmark reference, scaffolding, rejected alternative, or watchlist
**AND** SHALL mark missing optional repositories as `CandidateClone` rather than pretending they were studied
**SHALL** fail verification if a required local path does not exist.

### REQ-LEARN-037.0: Produce PRD Outcome Traceability Dossier

**WHEN** a study batch is complete
**THEN** it SHALL produce a dossier mapping every major finding to at least one PRD L1 outcome: API compatibility, OLTP boundary, OLAP boundary, Build Store boundary, full GDS surface, RAM promise, strict RAM rejection, single-node target, or atomic publication
**AND** SHALL include at least one recommended next experiment or implementation spike per outcome touched
**SHALL** identify any PRD outcome for which the current reference shelf lacks enough evidence.

### REQ-LEARN-038.0: Run Skeptical Architecture Review

**WHEN** a study artifact recommends adopting, adapting, or rejecting an idea
**THEN** it SHALL include a skeptical review that challenges page-cache accounting, duplicate topology, hidden scratch memory, query-time freshness drift, GDS surface shrinkage, client incompatibility, and benchmark validity
**AND** SHALL answer each challenge with source evidence, a proposed test, or an explicit unresolved risk
**SHALL** prevent "highest-IQ idea" sections from becoming unverified speculation.

### REQ-LEARN-039.0: Support Weaker Agent Execution

**WHEN** this spec is handed to a future agent with weaker reasoning or smaller context
**THEN** the agent SHALL receive a bounded prompt naming one lane, one repo family, one PRD outcome, and one required output table
**AND** SHALL use the Weak-Model Master Prompt or a narrower prompt from this spec
**SHALL** reject broad "study everything" instructions unless they are split into traceable batches.

### REQ-LEARN-040.0: Use Local Graph Tools Safely

**WHEN** an agent uses `codebase-memory-mcp` or `CodeGraphContext`
**THEN** the tool SHALL index only the current Knight Bus repo by default
**AND** SHALL exclude `gitrefrepo/`, `.git/`, `target/`, and generated tool artifacts
**SHALL** verify graph-tool findings with direct source reads before recording them as evidence.

### REQ-LEARN-041.0: Produce Checkpoint Summaries

**WHEN** a study pass exceeds 20 evidence rows, one repository family, or one context window
**THEN** the agent SHALL write a checkpoint summary using the template in this spec
**AND** SHALL record completed requirement IDs, source facts, architecture implications, verification commands, and unresolved risks
**SHALL** make the next agent able to resume without rereading unrelated context.

### REQ-LEARN-042.0: Enforce Architecture Fit Matrices

**WHEN** a study pass discusses a GDS family, client surface, storage precedent, or algorithm substrate
**THEN** it SHALL produce an architecture fit matrix with topology, sidecar, Build Store, snapshot catalog, state, memory, execution, support, and falsifier columns
**AND** SHALL mark missing fields as `MissingEvidence`
**SHALL** avoid prose-only architecture recommendations.

### REQ-LEARN-043.0: Run Weak-Model Verification

**WHEN** a study artifact is ready for review
**THEN** the agent SHALL answer the Weak-Model Verification Prompt yes/no questions
**AND** SHALL revise unsafe rows before finalizing
**SHALL** list any remaining unsafe answer as an unresolved risk rather than burying it.

### REQ-LEARN-044.0: Trace GDS Procedures To Kernels

**WHEN** a study pass claims knowledge of a GDS algorithm or procedure family
**THEN** it SHALL trace each public procedure mode from procedure entrypoint to config type, result type, estimate path, algorithm spec or factory, and implementation/kernel class
**AND** SHALL cite source paths or `rg` queries for every hop
**SHALL** mark any missing hop as `MissingEvidence` rather than inferring implementation behavior from the algorithm name.

### REQ-LEARN-045.0: Derive Storage Needs From Kernel Behavior

**WHEN** a procedure-to-kernel ledger row is complete
**THEN** the study artifact SHALL derive v003 storage needs from observed graph interfaces, topology orientation, sidecar inputs, dominant state, and mutate/write side effects
**AND** SHALL map each need to flat CSR, reverse CSR, columnar sidecar, result/model artifact, snapshot catalog, Build Store statistics, out-of-core stream, spill, cells, GraphBLAS, or `UnsupportedButRegistered`
**SHALL** reject architecture recommendations that are not backed by the ledger row.

### REQ-LEARN-046.0: Capture Algorithm Memory Estimator Semantics

**WHEN** Neo4j GDS exposes an estimate path for an algorithm mode
**THEN** the study artifact SHALL identify the estimator source, input dimensions, concurrency assumptions, graph-loading accounting, algorithm-state accounting, result accounting, and any known omitted memory class
**AND** SHALL translate those findings into a v003 estimate formula or `MissingEvidence`
**SHALL** prevent 50GB-on-8GB feasibility claims without estimator-derived or benchmark-derived memory terms.

### REQ-LEARN-047.0: Classify Full Algorithm Feasibility

**WHEN** the full procedure-to-kernel ledger for an algorithm family is available
**THEN** each procedure mode SHALL be classified as `FitsFlatCsr`, `NeedsSidecars`, `NeedsSpill`, `NeedsCells`, `NeedsGraphBLASSpike`, `NeedsModelArtifact`, `UnsupportedButRegistered`, or `ExplicitlyOutOfScope`
**AND** SHALL include a 50GB-on-8GB risk label: `low`, `medium`, `high`, or `not credible without rejection`
**SHALL** avoid treating one easy mode, such as `stream`, as proof that `mutate`, `write`, `estimate`, training, or pipeline modes are supported.

### REQ-LEARN-048.0: Require Algorithm Oracle And Parity Tests

**WHEN** an algorithm family is marked implementable or architecture-ready
**THEN** the study artifact SHALL name at least one tiny oracle graph, one Neo4j/GDS expected-behavior source or test, one flat-CSR parity check, and one memory-estimate check
**AND** SHALL identify deterministic behavior requirements for stochastic algorithms, tie-breaking, output ordering, and floating-point tolerance
**SHALL** mark the family `NeedsArchitectureSpike` until those tests are named.

### REQ-LEARN-049.0: Emit Required Study Deliverables

**WHEN** a study batch is marked complete
**THEN** it SHALL emit an evidence ledger, an architecture fit matrix, a PRD outcome traceability dossier, and a checkpoint summary
**AND** SHALL emit a procedure-to-kernel ledger whenever GDS algorithm claims are made
**SHALL** include at least one rejected-alternative note for every architecture recommendation that was considered against another option.

## Test Matrix

| req_id | test_id | test_type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-LEARN-001.0 | TEST-DOC-001 | documentation | Every study artifact classifies findings by PRD03 plane. | architecture boundary |
| REQ-LEARN-002.0 | TEST-RG-002 | source scan | `rg` results include Neo4j record, cursor, kernel, and storage symbols. | OLTP context |
| REQ-LEARN-003.0 | TEST-RG-003 | source scan | Bolt docs/testkit/driver pass names handshake, session, tx, routing, streaming, errors. | API compatibility |
| REQ-LEARN-004.0 | TEST-RG-004 | source scan | Cypher pass separates parser, semantic analysis, planner, runtime, and spec-suite evidence. | Cypher context |
| REQ-LEARN-005.0 | TEST-RG-005 | source scan | Procedure/value pass includes argument binding, result marshalling, type conversion, and errors. | procedure context |
| REQ-LEARN-006.0 | TEST-DOC-006 | documentation | GDS inventory includes procedure name, mode, config, result, facade, estimate, file, support level. | GDS ABI |
| REQ-LEARN-007.0 | TEST-DOC-007 | documentation | GDS graph-store concepts map to flat CSR, sidecar, catalog, Build Store fact, or generation. | OLAP storage |
| REQ-LEARN-008.0 | TEST-DOC-008 | documentation | Memory-estimation pass outputs formulas or pseudocode. | RAM contract |
| REQ-LEARN-009.0 | TEST-DOC-009 | documentation | Every algorithm family has access pattern, state shape, sidecar need, spill strategy, oracle, risk. | algorithms |
| REQ-LEARN-010.0 | TEST-DOC-010 | documentation | Mutate/write/model/pipeline pass defines result location and stale-watermark conflict behavior. | state mutation |
| REQ-LEARN-011.0 | TEST-DOC-011 | design review | Build Store pass recommends one minimal shape and rejects two alternatives. | durable IR |
| REQ-LEARN-012.0 | TEST-DOC-012 | design review | Sidecar pass defines physical types, nulls, dictionaries, vectors, streaming, estimates. | sidecars |
| REQ-LEARN-013.0 | TEST-DOC-013 | design review | DataFusion pass limits findings to planning/catalog/filter patterns, not SQL-engine adoption. | planning discipline |
| REQ-LEARN-014.0 | TEST-DOC-014 | comparison | Competitor pass labels ideas as inspiration unless PRD03-compatible. | competitor learning |
| REQ-LEARN-015.0 | TEST-DOC-015 | benchmark design | Algorithm baseline pass names oracle fixtures and memory formulas before speed claims. | benchmark integrity |
| REQ-LEARN-016.0 | TEST-DOC-016 | documentation | Evidence ledger includes source path, symbol, query or line range, inference, decision impact, skeptical note. | evidence quality |
| REQ-LEARN-017.0 | TEST-DOC-017 | traceability | Every architecture-affecting claim references at least one `REQ-LEARN-*` ID. | traceability |
| REQ-LEARN-018.0 | TEST-NAME-018 | naming | New helper names follow four-word naming unless preserving external API compatibility. | AI-native maintainability |
| REQ-LEARN-019.0 | TEST-DOC-019 | design review | Ladybug pass extracts Icebug-Disk, morsel parallelism, index recovery, transaction/checkpoint, and execution-engine lessons while labeling them inspiration. | embedded graph precedent |
| REQ-LEARN-020.0 | TEST-DOC-020 | workflow review | GDS workflow pass classifies client, catalog, algorithm, notebook, and orchestration findings by priority. | user workflow compatibility |
| REQ-LEARN-021.0 | TEST-DOC-021 | procedure inventory | APOC pass assigns each category to Implemented, UnsupportedButRegistered, AliasToNative, or ExplicitlyOutOfScope. | APOC boundary |
| REQ-LEARN-022.0 | TEST-DOC-022 | compatibility plan | Client canary pass proposes CLI, generated-query, Rust-client, and app-style workflows. | ecosystem compatibility |
| REQ-LEARN-023.0 | TEST-DOC-023 | benchmark design | LDBC pass maps every benchmark claim to workload, scale, validation, and command. | benchmark credibility |
| REQ-LEARN-024.0 | TEST-DOC-024 | design review | Out-of-core pass separates lessons favoring flat streams, cells/tiles, and rejected layouts. | low-RAM OLAP |
| REQ-LEARN-025.0 | TEST-DOC-025 | architecture review | GraphBLAS pass recommends adoption, rejection, or limited use per algorithm family. | algorithm substrate |
| REQ-LEARN-026.0 | TEST-DOC-026 | test design | Rust graph fixture pass labels repos as scaffolding/oracle references, not storage architecture. | test scaffolding |
| REQ-LEARN-027.0 | TEST-DOC-027 | measurement design | RAM observability pass names measurement source, interval, phase, and threshold for each RAM claim. | memory contract |
| REQ-LEARN-028.0 | TEST-DOC-028 | rejected alternative | Live-incremental pass explains why v003 does or does not adopt query-time reconciliation. | architecture discipline |
| REQ-LEARN-029.0 | TEST-DOC-029 | scope review | Graph-vector pass keeps findings out of P0 unless PRD03 explicitly changes scope. | market watch |
| REQ-LEARN-030.0 | TEST-DOC-030 | surface inventory | No architecture sufficiency claim appears without full GDS procedure, mode, config, result, estimate, catalog, mutate/write, model, pipeline, and operations inventory. | full GDS surface |
| REQ-LEARN-031.0 | TEST-DOC-031 | publication design | Snapshot catalog pass defines active pointer, watermark, validation, atomic swap, reader pinning, retention, rollback, restart recovery, and corruption handling. | atomic publication |
| REQ-LEARN-032.0 | TEST-DOC-032 | source scan | Current Knight Bus pass identifies existing CSR writer, manifest, mmap runtime, low-RAM builder, tests, benchmark claims, and walk surface. | current seed |
| REQ-LEARN-033.0 | TEST-DOC-033 | prompt review | Agent-ready prompt pack includes full-surface, repo-study, architecture-fit, skeptical-review, and evidence-ledger prompts. | agent handoff |
| REQ-LEARN-034.0 | TEST-DOC-034 | architecture matrix | Fit matrix maps each capability to topology, sidecars, Build Store facts, catalog, state, memory, execution strategy, and support status. | architecture fit |
| REQ-LEARN-035.0 | TEST-DOC-035 | evidence review | Every claim separates sourced fact, local inference, speculation, PRD impact, and skeptical note. | claim quality |
| REQ-LEARN-036.0 | TEST-PATH-036 | path validation | Required repo paths exist, optional missing repos are CandidateClone, and each repo has role/commit/branch metadata when available. | clone ledger |
| REQ-LEARN-037.0 | TEST-DOC-037 | traceability | Batch dossier maps findings to PRD L1 outcomes and names missing evidence per outcome. | PRD traceability |
| REQ-LEARN-038.0 | TEST-DOC-038 | skeptical review | Adoption/rejection recommendations answer RAM, topology duplication, freshness, GDS shrinkage, client, and benchmark challenges. | review rigor |
| REQ-LEARN-039.0 | TEST-DOC-039 | prompt review | Every broad handoff prompt names one lane, repo family, PRD outcome, and output table. | weak-model execution |
| REQ-LEARN-040.0 | TEST-TOOL-040 | tool smoke | Code graph tool smoke runs index current repo only and excludes `gitrefrepo/`. | local tooling safety |
| REQ-LEARN-041.0 | TEST-DOC-041 | checkpoint review | Long study artifacts include resumable checkpoint summaries. | context retention |
| REQ-LEARN-042.0 | TEST-DOC-042 | matrix review | Capability recommendations include topology, sidecar, Build Store, catalog, state, memory, execution, support, and falsifier fields. | architecture fit |
| REQ-LEARN-043.0 | TEST-DOC-043 | verification review | Final artifacts answer weak-model verification questions and surface unresolved unsafe answers. | self-check rigor |
| REQ-LEARN-044.0 | TEST-DOC-044 | implementation trace | Every GDS algorithm claim includes procedure, config, result, estimate, spec/factory, and kernel evidence. | procedure-to-kernel trace |
| REQ-LEARN-045.0 | TEST-DOC-045 | storage derivation | Storage recommendations cite graph interfaces, orientation, sidecars, state, and side effects from the ledger. | storage architecture |
| REQ-LEARN-046.0 | TEST-DOC-046 | memory estimation | Algorithm feasibility claims include estimator source, dimensions, concurrency, state, result, and omitted classes. | RAM contract |
| REQ-LEARN-047.0 | TEST-DOC-047 | feasibility review | Every algorithm mode has support class and 50GB-on-8GB risk label. | GDS feasibility |
| REQ-LEARN-048.0 | TEST-DOC-048 | oracle planning | Implementable algorithm families name oracle graph, Neo4j/GDS behavior source, flat-CSR parity check, and memory-estimate check. | test readiness |
| REQ-LEARN-049.0 | TEST-DOC-049 | deliverable review | Completed study batches emit the required evidence, fit, traceability, and checkpoint artifacts, plus procedure-to-kernel ledgers when needed. | study completion |

## TDD Plan

### STUB

1. Create a study artifact skeleton for one surface lane or architecture lane.
2. Add empty sections for requirements satisfied, clone coverage, source
   evidence, sourced facts, inference, speculation, PRD03 impact, architecture
   fit matrix, rejected ideas, skeptical review, and next experiments.
3. Add command placeholders for path validation, `rg` scans, inventory
   extraction, and traceability checks.
4. Copy the relevant reusable prompt from this spec into the artifact so the
   study pass is reproducible.
5. For weaker-model handoffs, fill the lane, repo family, PRD outcome,
   requirement IDs, and expected output tables before the agent starts.
6. For GDS algorithm work, add empty procedure-to-kernel ledger rows before
   writing any architecture conclusion.

### RED

1. Run the clone/path validation and confirm missing optional repos are marked
   `CandidateClone` while required paths exist.
2. Run the traceability check and confirm it fails because no evidence rows
   have been filled.
3. Run the source scan and confirm it finds target folders but no accepted
   evidence ledger entries yet.
4. For GDS surface work, confirm the first inventory attempt fails if any
   procedure, mode, config, result, estimate, catalog, mutate/write, model,
   pipeline, or operations category is unknown.
5. Record the expected failure reason in the artifact checkpoint.
6. For graph-tool runs, confirm smoke output excludes `gitrefrepo/` before using
   graph-derived findings.
7. For GDS algorithm work, confirm the trace fails until procedure, config,
   result, estimate, spec/factory, implementation class, graph interfaces,
   sidecars, state, and oracle fields are filled or marked `MissingEvidence`.
8. For any completed batch, confirm the artifact set contains an evidence
   ledger, architecture fit matrix, PRD traceability dossier, and checkpoint
   summary; add a procedure-to-kernel ledger when algorithm claims were made.

### GREEN

1. Fill the minimum evidence rows needed to satisfy the selected `REQ-LEARN-*`
   contract.
2. Add the architecture implication, support status, memory consequence, and
   rejected alternative.
3. Fill the architecture fit matrix for the studied surface or repo family.
4. Add one skeptical challenge and response for every adopted or adapted idea.
5. Re-run the traceability check until every requirement row maps to at least
   one evidence row.
6. Fill the architecture fit matrix before writing recommendation prose.
7. Fill procedure-to-kernel ledger rows before deriving storage needs.

### REFACTOR

1. Collapse duplicate evidence rows.
2. Replace vague claims with source-backed conclusions.
3. Split mixed claims into sourced fact, local inference, and speculation.
4. Move broad ideas into open questions when evidence is insufficient.
5. Keep PRD03 boundaries explicit.
6. Demote watchlist findings that do not affect PRD L1 outcomes.
7. Replace broad handoff prompts with bounded weak-model prompts.
8. Split any algorithm-family claim that mixes public ABI, kernel behavior,
   memory estimate, and storage implication into separate ledger fields.
9. Keep rejected alternatives close to the recommendation so later agents can
   see what was considered and why.

### VERIFY

1. Run `git diff --check`.
2. Run `rg -n "TODO|STUB|FIXME" docs_PRD03` and justify any pre-existing hits.
3. Confirm every `REQ-LEARN-*` referenced by a study artifact appears in this
   spec.
4. Confirm every performance or RAM claim has a measurement method or is marked
   inference.
5. Confirm every architecture sufficiency claim references the full GDS surface
   inventory or explicitly narrows itself to a named proof slice.
6. Confirm every study batch produces a PRD outcome traceability dossier.
7. Confirm long-running work has a checkpoint summary that another agent can
   resume from.
8. Confirm code graph tools were not used to index all `gitrefrepo/` folders
   unless explicitly requested.
9. Confirm every GDS algorithm architecture claim cites a procedure-to-kernel
   ledger row and a memory-estimate or oracle-test plan.
10. Confirm every completed study batch emits the required artifact set.

## Quality Gates

- [ ] Every learning artifact names the source folders studied.
- [ ] Every source-backed claim includes file path plus symbol or search query.
- [ ] Every inference is labeled as inference.
- [ ] Every speculation is labeled as speculation or moved to open questions.
- [ ] Every architecture-affecting claim references a `REQ-LEARN-*` ID.
- [ ] Every study batch maps its findings to at least one PRD L1 outcome.
- [ ] Every required local repo path exists or the artifact fails verification.
- [ ] Every optional missing repo is marked `CandidateClone` with rationale.
- [ ] No learning artifact suggests reading OLAP queries from the Projection
      Build Store.
- [ ] No GDS support claim appears without procedure, mode, config, result, and
      estimate coverage.
- [ ] No GDS algorithm storage claim appears without procedure-to-kernel trace
      coverage.
- [ ] No algorithm-family claim derives architecture from the algorithm name
      instead of observed Neo4j GDS implementation behavior.
- [ ] No architecture sufficiency claim appears before full GDS surface inventory
      or without an explicit named-slice limitation.
- [ ] No implementation staging decision shrinks the intended GDS surface.
- [ ] No memory claim appears without heap, RSS/page-cache/direct-buffer,
      sidecar, scratch, spill, and algorithm-state accounting.
- [ ] No snapshot publication claim appears without watermark, manifest,
      validation, atomic swap, reader pinning, retention, and restart-recovery
      behavior.
- [ ] No Ladybug-derived claim is treated as Neo4j/GDS compatibility evidence.
- [ ] No APOC support claim appears without an explicit category-level support
      state.
- [ ] No benchmark claim appears without a named workload, scale, validation
      rule, and repeatable command.
- [ ] No GraphBLAS recommendation appears without per-family memory and
      integration tradeoffs.
- [ ] No graph-vector finding enters P0 unless PRD03 explicitly changes scope.
- [ ] No future helper script or new internal function violates four-word naming
      unless it preserves an external compatibility name.
- [ ] Every broad agent handoff prompt names one lane, one repo family, one PRD
      outcome, and one expected output table.
- [ ] Every long study pass includes a checkpoint summary before context is
      likely to drift.
- [ ] Every architecture recommendation includes an architecture fit matrix or
      explicitly says `MissingEvidence`.
- [ ] Every completed study batch emits an evidence ledger, architecture fit
      matrix, PRD traceability dossier, and checkpoint summary.
- [ ] Every GDS algorithm claim has a procedure-to-kernel ledger row before it
      can affect architecture decisions.
- [ ] Any use of `codebase-memory-mcp` or `CodeGraphContext` is scoped to the
      current Knight Bus repo unless a human explicitly requests reference-repo
      indexing.
- [ ] Weak-model verification questions are answered before finalizing a study
      artifact.
- [ ] Every implementable GDS algorithm family names oracle, parity, and memory
      estimate tests before implementation begins.
- [ ] `git diff --check` passes before committing documentation.

## Open Questions

1. Should the first evidence ledger live directly in `docs_PRD03/` or in a
   subfolder such as `docs_PRD03/reference-learning/`?
2. Should `Projection Build Store` be specified as a custom append-run format,
   an embedded KV/LSM, or an Arrow/Parquet run collection for the first spike?
3. Should GDS procedure inventory be checked into the repo or regenerated from
   `gitrefrepo/neo4j-gds-src` during verification?
4. Which driver should become the first zero-change compatibility canary:
   Python, JavaScript, Java, Go, or .NET?
5. What freshness target should decide whether cellular packaging becomes worth
   building after flat CSR plus sidecars?
6. Should a Ladybug Icebug-Disk-style Parquet CSR snapshot become a benchmarked
   interchange path alongside the current binary CSR files?
7. Should Ladybug's `BUILDING` / `CATCHING_UP` / `VALID` / `INVALID` index
   states inform v003 snapshot and sidecar publication semantics?
8. Which GDS client workflow should become the first end-to-end compatibility
   canary: Python notebook, GDS agent, browser, or generated Cypher?
9. Should APOC be registered as a first-class unsupported surface in v003, or
   should unsupported APOC calls remain outside the initial compatibility story?
10. Which LDBC workload is the first public benchmark gate for v003:
    Graphalytics, SNB Interactive v1, or SNB Interactive v2?
11. Which low-RAM prior-art result would falsify the need for cells/tiles?
12. Which GDS algorithm families are better expressed through GraphBLAS than
    direct CSR traversal?
13. Should graph-vector features remain a watched future scope, or become a
    deliberate v003 sidecar requirement?
14. Where should generated study artifacts live:
    `docs_PRD03/reference-learning/`, `docs_PRD03/evidence-ledgers/`, or one
    Markdown file per lane?
15. Should full GDS inventory be generated directly from source annotations,
    maintained by hand as a checked-in TSV, or both?
16. Which current Knight Bus files define the canonical flat CSR oracle for
    future parity checks?
17. Which snapshot publication primitive should become the first executable
    proof: manifest validation, atomic active pointer swap, reader pinning, or
    restart recovery?
18. What is the minimum acceptable evidence before saying a GDS family requires
    cells, GraphBLAS, spill, or out-of-core streaming?
19. Should the weak-model prompt pack be split into separate files under
    `docs_PRD03/prompts/` once agents start using it repeatedly?
20. Should code graph tool smoke outputs be checked into a study artifact, or
    should they remain ephemeral `/tmp` evidence with source-file verification?
21. Which Neo4j GDS source layer should be treated as the canonical
    procedure-to-kernel join point: procedure classes, algorithm specifications,
    application facades, or memory-estimation builders?
22. Should the procedure-to-kernel ledger be generated into a TSV/CSV artifact
    so architecture reviews can diff algorithm support over time?
23. What is the first algorithm family whose ledger should be completed end to
    end before any new storage format is accepted: PageRank, WCC, Louvain,
    shortest paths, node similarity, or embeddings?
24. Should the required deliverables be emitted as one file per batch or one
    file per artifact type?

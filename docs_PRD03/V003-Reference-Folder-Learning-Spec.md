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
| Failure modes | Reading too broadly, summarizing without evidence, missing GDS ABI details, confusing inspiration with requirements, creating untraceable notes. |
| Reliability limit | Every learning claim must identify source path, symbol, and why it affects v003. |
| Runtime constraint | No production Rust code is required by this spec; generated helper scripts must use four-word names when introduced later. |

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

## TDD Plan

### STUB

1. Create a study artifact skeleton for one P0 folder group.
2. Add empty sections for requirements satisfied, source evidence, inference,
   PRD03 impact, rejected ideas, and skeptical notes.
3. Add command placeholders for `rg` scans that will prove the source evidence.

### RED

1. Run the traceability check and confirm it fails because no evidence rows
   have been filled.
2. Run the source scan and confirm it finds target folders but no accepted
   evidence ledger entries yet.
3. Record the expected failure reason in the artifact checkpoint.

### GREEN

1. Fill the minimum evidence rows needed to satisfy the selected `REQ-LEARN-*`
   contract.
2. Add the architecture implication and rejected alternative.
3. Re-run the traceability check until every requirement row maps to at least
   one evidence row.

### REFACTOR

1. Collapse duplicate evidence rows.
2. Replace vague claims with source-backed conclusions.
3. Move broad ideas into open questions when evidence is insufficient.
4. Keep PRD03 boundaries explicit.

### VERIFY

1. Run `git diff --check`.
2. Run `rg -n "TODO|STUB|FIXME" docs_PRD03` and justify any pre-existing hits.
3. Confirm every `REQ-LEARN-*` referenced by a study artifact appears in this
   spec.
4. Confirm every performance or RAM claim has a measurement method or is marked
   inference.

## Quality Gates

- [ ] Every learning artifact names the source folders studied.
- [ ] Every source-backed claim includes file path plus symbol or search query.
- [ ] Every inference is labeled as inference.
- [ ] Every speculation is labeled as speculation or moved to open questions.
- [ ] Every architecture-affecting claim references a `REQ-LEARN-*` ID.
- [ ] No learning artifact suggests reading OLAP queries from the Projection
      Build Store.
- [ ] No GDS support claim appears without procedure, mode, config, result, and
      estimate coverage.
- [ ] No memory claim appears without heap, RSS/page-cache/direct-buffer,
      sidecar, scratch, spill, and algorithm-state accounting.
- [ ] No Ladybug-derived claim is treated as Neo4j/GDS compatibility evidence.
- [ ] No future helper script or new internal function violates four-word naming
      unless it preserves an external compatibility name.
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

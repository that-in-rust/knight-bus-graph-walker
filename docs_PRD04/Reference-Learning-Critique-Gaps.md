# Reference Learning Critique Gaps

Date: 2026-06-24

This document critiques the `docs_PRD03/reference-learning/` notes against:

- `docs_PRD03/prd-l1.md`
- `docs_PRD03/V003-Reference-Folder-Learning-Spec.md`

The short verdict:

```text
The reference-learning shelf is strong as a research and traceability corpus.
It is not yet strong enough to be treated as an implementation blueprint for
the full v003 PRD.
```

The important distinction is this:

```text
Learning-spec coverage is mostly complete.
PRD implementation credibility is still incomplete.
```

That is not a contradiction. The learning spec asked for source-backed study
artifacts. The PRD asks for a Neo4j-compatible Rust rewrite with credible OLTP,
published OLAP snapshots, complete GDS surface handling, deterministic memory
estimates, and implementation tests.

## Evidence Snapshot

Current shelf signals:

| artifact | current signal | critique implication |
| --- | ---: | --- |
| `Requirements-Coverage-Tracker.tsv` | `53` rows for `53` spec requirements | The learning spec is tracked, but this only proves study coverage. |
| `GDS-Public-Surface-Inventory.tsv` | `575` visible `gds.*` rows | The GDS surface is correctly treated as large. |
| `GDS-Public-Surface-Inventory.tsv` | `575` rows still contain `MissingEvidence` | Public surface inventory is not yet a full compatibility contract. |
| `GDS-Public-Surface-Inventory.tsv` | `575` rows still `NeedsArchitectureSpike` | No per-procedure support level has been resolved in the baseline inventory. |
| `GDS-Procedure-To-Kernel-Ledger.tsv` | `13` representative rows | Useful sample, not full procedure-to-kernel coverage. |
| `GDS-Family-Oracle-Parity-Matrix.tsv` | `14` family rows | Good family planning, not per-procedure mode readiness. |
| `Architecture-Fit-Matrix.tsv` | `53` rows | Good architecture mapping, but many rows are still spikes or P0 registration. |
| `Architecture-Option-Scorecard.tsv` | `22` rows | Good option discipline, but no option is proven by implementation benchmarks. |
| `PRD-Outcome-Traceability-Dossier.tsv` | `38` rows | Good PRD mapping, but some PRD outcomes are much thinner than others. |

The PRD's strongest acceptance line is:

```text
Every known procedure is either implemented or registered with deterministic
unsupported behavior.
```

The current notes identify the scope of that work, but do not yet prove it.

## What The Notes Do Well

| strength | why it matters |
| --- | --- |
| They separate OLTP, Projection Build Store, and OLAP snapshots. | This matches the PRD's central architectural boundary. |
| They avoid pretending flat CSR alone is enough. | The notes correctly add catalog, sidecars, artifacts, estimates, and runtime scratch. |
| They push back against 13 persistent per-algorithm layouts. | The evidence favors canonical topology plus support planes. |
| They keep cells optional rather than fashionable. | The notes do not overbuild Tilehouse without measured pressure. |
| They inventory GDS breadth before choosing storage. | This prevents designing only for PageRank/BFS demos. |
| They classify fixture and oracle repos as scaffolding, not architecture truth. | This protects Neo4j compatibility from being diluted by non-Neo4j libraries. |
| They include machine-readable companions. | Future agents can query the shelf instead of rereading every note. |

## Main Critique

### 1. The Notes Overstate "Complete" If Read As PRD Readiness

The reference-learning README says the learning-spec scope is complete. That is
fair for documentation coverage, but easy to misread as product readiness.

The PRD requires:

- Neo4j-compatible API behavior where support is claimed.
- OLTP reads/writes on Neo4j-shaped storage.
- OLAP/GDS reads from published immutable snapshots.
- Every known procedure implemented or deterministically unsupported.
- Memory estimates and rejection before execution.
- Atomic snapshot publication.

The notes do not yet provide the implementation contracts, test fixtures, or
acceptance gates needed to prove those PRD outcomes.

Missing hardening:

- A top-level warning in `reference-learning/README.md` should say: "Learning
  artifacts complete; implementation proof incomplete."
- A separate implementation-readiness tracker should distinguish `Studied`,
  `Specified`, `Stubbed`, `Implemented`, `Verified`, and `Rejected`.

### 2. Full GDS Surface Is Inventoried, Not Resolved

The GDS public surface inventory is the right move, but it remains shallow.

Current concern:

```text
575 visible rows
575 rows with MissingEvidence
575 rows with NeedsArchitectureSpike
13 procedure-to-kernel representative rows
14 family oracle rows
```

This is enough to prove breadth. It is not enough to route actual user calls.

What is missing:

- Per-procedure support status, not only family support status.
- Config type, default values, result schema, estimate path, implementation
  class, and mutate/write target for every in-scope procedure.
- A deterministic unsupported behavior spec for every row that is not
  implemented.
- A list of rows that are test-only, alpha/beta, enterprise-only, deprecated,
  or incompatible with a community single-node target.
- A generated registry input that can later become code or tests.

PRD risk:

```text
The project could claim "full GDS surface" while only understanding family
groups and representative algorithms.
```

### 3. Neo4j Compatibility Is Still Boundary-Level

The notes correctly identify Bolt, Cypher, procedures, values, APOC, and drivers
as compatibility surfaces. But the level of detail is still too high for a Rust
rewrite.

What is under-specified:

- Bolt handshake versions and failure behavior.
- Transaction retry semantics expected by official drivers.
- Result streaming, backpressure, and error object mapping.
- Cypher `CALL ... YIELD` binding and procedure argument conversion.
- Neo4j value type compatibility, including temporal, spatial, path, list,
  map, null, and integer edge cases.
- APOC support tiers beyond broad boundary notes.
- Cypher shell, browser, OGM, and community driver canary tests.

PRD risk:

```text
"Zero application-code changes" will fail at the client behavior layer even if
the storage architecture is good.
```

### 4. Neo4j-Shaped OLTP Is Not Yet An Implementation Spec

The PRD says OLTP storage remains Neo4j-shaped. The notes study Neo4j record
formats and compatibility boundaries, but they do not yet produce a Rust OLTP
storage contract.

What is missing:

- Concrete Rust module boundaries for nodes, relationships, properties, schema,
  indexes, WAL, locks, checkpointing, and page cache.
- A record-level invariant list: dense nodes, relationship chains/groups,
  property blocks, dynamic records, token stores, and schema constraints.
- Crash recovery scenarios and expected post-restart state.
- Lock ordering and deadlock/retry behavior.
- Import, bulk load, and incremental write paths.
- A minimal OLTP test matrix that can prove Neo4j-shaped semantics before OLAP
  work consumes attention.

PRD risk:

```text
The OLAP design may look crisp while the transactional source of truth remains
too vague to build.
```

### 5. Projection Build Store Recommendation Needs A Physical Contract

The notes recommend a receipt log plus metadata/fact store shape. That is a good
direction, but still not enough for implementation.

What is missing:

- Exact receipt schema and ordering guarantees.
- Idempotency and replay rules.
- Dense-id allocation and remapping rules.
- Dictionary and sorted-run schemas.
- Fact retention and compaction thresholds.
- Memory budgets for sort, dictionary build, validation, and snapshot compile.
- Crash points: receipt appended but metadata absent, metadata committed but
  snapshot build failed, generation validated but active pointer not swapped.

PRD risk:

```text
The Build Store could accidentally become either too weak to reproduce
snapshots or too strong and start behaving like a third serving database.
```

### 6. Published Snapshot Semantics Are Not Concrete Enough

The PRD requires immutable published snapshots, source watermarks, and atomic
publication. The notes clearly identify those needs, but the actual state
machine is not yet written as a durable contract.

What is missing:

- Generation directory layout.
- Active pointer file format.
- `staged`, `validating`, `published`, `retired`, `failed`, and `garbage`
  states.
- Atomic rename or fsync order.
- Reader pinning API.
- Retention policy.
- Rollback rules.
- Corruption detection and restart recovery.
- Exact behavior when OLAP query starts during publication.

PRD risk:

```text
Snapshot publication could be "conceptually atomic" in docs but racy in the
actual filesystem implementation.
```

### 7. Strict RAM Is Described, Not Proven

The notes repeatedly say memory must include heap, RSS, page cache, direct
buffers, sidecars, scratch, spill, result artifacts, and algorithm state. That
is the right vocabulary. The missing piece is executable math.

What is missing:

- A concrete `MemoryEstimate` schema for v003.
- Per-procedure formula rows for topology, sidecars, result artifacts, scratch,
  direct buffers, and page-cache policy.
- A 50GB-on-8GB pass/fail table for each first-tier algorithm.
- Explicit rejection examples: which queries must fail before execution?
- Measurements comparing `mmap` fast mode versus explicit-I/O strict mode.
- A way to account for retained old generations and concurrent snapshot builds.

PRD risk:

```text
The project could repeat Neo4j's memory pain in a different shape: lower heap,
but uncontrolled page cache, scratch, result artifacts, or retained generations.
```

### 8. Cells Are Sensibly Optional, But Under-Tested

The notes are right not to force cells. They also do not yet prove when cells
become necessary.

What is missing:

- A cell adoption threshold: boundary-edge ratio, dirty-region size, rebuild
  latency, page-cache churn, or local-query locality gain.
- Workloads that would actually falsify flat canonical publication.
- A direct comparison between:
  - flat CSR plus sidecars,
  - flat CSR plus global stream,
  - cellular CSR,
  - hybrid flat plus cells.
- The cost of cell metadata, duplicate offsets, boundary indexes, and cache
  fragmentation.

PRD risk:

```text
The team may keep cells in an ambiguous "maybe later" zone without knowing
which measurement should trigger adoption or rejection.
```

### 9. Result, Model, And Pipeline Artifacts Need Stronger Persistence Rules

The notes correctly say GDS compatibility needs result sidecars, embeddings,
models, pipelines, mutate/write semantics, and artifact catalogs. But the
persistence behavior is still not sharp.

What is missing:

- Artifact identity: graph name, generation, user, database, procedure, config
  hash, source watermark.
- Mutate versus write behavior.
- Model catalog persistence and versioning.
- Pipeline catalog persistence and dependency tracking.
- Artifact cleanup rules when source generations retire.
- Compatibility behavior when a model references a graph that no longer exists.

PRD risk:

```text
Topology may be well-designed while GDS workflows fail because artifacts and
models do not behave like a real cataloged system.
```

### 10. Benchmark Discipline Is Still A Plan

Batch 09 improves benchmark vocabulary, but the notes are still not a benchmark
plan that can prove product claims.

What is missing:

- Exact datasets and scale factors.
- Exact commands for Neo4j OLTP, Neo4j GDS projection, and Knight Bus v003.
- Fair baselines: Neo4j Cypher over OLTP store and Neo4j GDS projected graph.
- Cold-start, warm-cache, projection-build, algorithm-execution, and writeback
  phases measured separately.
- Peak RSS, page cache, direct-buffer, scratch, spill, and retained-generation
  reporting.
- Pass/fail thresholds for "50GB on 8GB".

PRD risk:

```text
The project may have strong architectural arguments but weak proof of the
business-visible RAM claim.
```

### 11. Graph Tool Evidence Is Useful But Uneven

The notes are refreshingly honest that graph-tool output can be low-yield or
misleading. That caution should become stronger.

Concern:

- Some repos are `CbmSemanticReadyCgcLowYield`.
- `clickhouse-src` still failed semantic-ready checks.
- Wrapper artifacts alone are explicitly insufficient.
- Some claims cite graph-tool coverage without enough direct line-level
  evidence in the machine-readable companions.

What is missing:

- A confidence tier for every claim: `DirectSource`, `GraphToolAssisted`,
  `DocsOnly`, `Inference`, `Speculation`.
- A count of claims that still lack line-level evidence.
- A stricter rule that architecture recommendations must be backed by
  `DirectSource` or explicitly marked as inference.

PRD risk:

```text
Future agents may treat graph-tool coverage as stronger than it really is.
```

### 12. The Support Status Taxonomy Needs Implementation Semantics

The shared statuses are useful, but their implementation meaning is still soft.

Ambiguity examples:

- `P0-RegisteredCompatible` could mean "stub exists", "deterministic error
  spec exists", or "ready to implement".
- `P1-ImplementedExactLowRam` appears in research matrices even though no v003
  implementation exists for those algorithms yet.
- `P2-ImplementedLater` does not say whether a procedure is registered now,
  hidden, or explicitly unsupported.

What is missing:

- A status-to-runtime-behavior table.
- A status-to-test-obligation table.
- A rule for how statuses in research TSVs map to code registry states later.

PRD risk:

```text
The project may use status labels consistently in documents but ambiguously in
the actual user-facing product.
```

## Missing Artifacts I Would Add Next

| priority | artifact | why it matters |
| --- | --- | --- |
| P0 | `V003-Implementation-Readiness-Tracker.tsv` | Converts research coverage into implementation states and test obligations. |
| P0 | `GDS-Procedure-Support-Registry.tsv` | One row per visible GDS procedure with support status, deterministic unsupported behavior, config/result schema, and test plan. |
| P0 | `Neo4j-Compatibility-Canary-Matrix.md` | Defines Bolt, driver, Cypher, procedure, APOC, browser, shell, and OGM canaries. |
| P0 | `Snapshot-Publication-State-Machine.md` | Turns publication, rollback, retention, and restart recovery into a buildable protocol. |
| P0 | `Memory-Estimate-Formula-Book.tsv` | Gives every priority procedure a concrete estimate formula and rejection example. |
| P1 | `Projection-Build-Store-Physical-Contract.md` | Defines receipt log, metadata store, idempotency, replay, dense IDs, dictionaries, and compaction. |
| P1 | `OLTP-Record-Store-Rust-Contract.md` | Turns "Neo4j-shaped OLTP" into Rust module boundaries and invariants. |
| P1 | `Cells-Adoption-Falsifier-Plan.md` | Defines the exact tests that would make cells necessary or unnecessary. |
| P1 | `Benchmark-Proof-Plan.md` | Defines fair baselines, commands, phases, metrics, and thresholds. |
| P2 | `Artifact-Model-Catalog-Contract.md` | Defines result, embedding, model, and pipeline artifact identity and lifecycle. |

## Suggested Next Passes

### Pass 1: Convert GDS Inventory Into A Registry Contract

Goal:

```text
No `gds.*` row stays at vague MissingEvidence if it is in the claimed surface.
```

Output:

- one procedure support registry TSV;
- config/result/estimate columns;
- deterministic unsupported behavior for non-implemented rows;
- first 20 highest-value procedure canaries.

### Pass 2: Write The Snapshot Publication Protocol

Goal:

```text
Make "published immutable snapshots" mechanically true.
```

Output:

- generation states;
- active pointer format;
- fsync/rename order;
- reader pinning;
- rollback and restart recovery tests.

### Pass 3: Make RAM Claims Executable

Goal:

```text
Turn memory vocabulary into formulas, budgets, and rejection behavior.
```

Output:

- formula book;
- 50GB-on-8GB scenarios;
- strict mode versus fast mode;
- measured versus estimated report schema.

### Pass 4: Define Neo4j Compatibility Canaries

Goal:

```text
Make "zero application-code changes" testable before the storage work runs away.
```

Output:

- Bolt/driver/cypher-shell canaries;
- procedure marshalling canaries;
- error/value mapping canaries;
- APOC boundary canaries.

## Bottom Line

The notes succeeded at answering the first architecture danger:

```text
Is flat CSR alone enough?
No.

Does full GDS force many durable graph layouts or cells by default?
Also no.

What is the likely substrate?
Canonical topology plus projection catalog, typed property plane, artifact
plane, memory estimator, runtime workspace, and published snapshot lifecycle.
```

The notes have not yet answered the implementation danger:

```text
Can a future agent now build v003 without inventing missing protocol,
compatibility, memory, and registry semantics?
Not yet.
```

The next work should therefore stop broadening research and start converting
the strongest conclusions into implementation contracts.

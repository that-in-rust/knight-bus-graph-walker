# Agent 01: Neo4j Core Compatibility and Bounded Runtime Architecture

## Executive Decision

The useful Neo4j compatibility target is not a Rust rewrite of the Neo4j database.

The useful target is a deliberately narrow adoption adapter:

1. An existing Neo4j driver connects to a Knight Bus endpoint.
2. The application sends an unchanged, supported Cypher query and parameters.
3. Knight Bus parses the query into a small read-only intermediate representation.
4. A resource planner estimates the complete working set before expensive execution.
5. The runtime makes one explicit decision: `fit`, `spill`, `approximate`, or `refuse`.
6. The selected artifact-native operator runs under an enforceable memory and I/O permit.
7. Results are streamed with Bolt backpressure.
8. A receipt reports the estimate, decision, actual peak memory, spill I/O, runtime, and artifact checksum.

This preserves the low-friction part of Neo4j adoption while refusing the enormous implementation surface that does not advance the A007 product thesis.

The product boundary from `docs_PRD04/A007-spc-founder-interview-prep-v7.md` is therefore decisive:

> Knight Bus is an artifact-to-answer bounded graph runner for security, dependency, and access-path workloads. Compatibility is an adapter. Budget enforcement and receipts are the product.

A full database rewrite would consume the program with write transactions, recovery, indexes, administration, clustering, security, and broad Cypher parity before proving that customers value bounded graph analysis. The assigned source evidence makes that risk concrete.

## Scope

Only these repositories were examined:

| Repository | Git commit | Assigned role |
|---|---:|---|
| `neo4j-src` | `c68156edf24164435ab1ac257ec633134c2887f7` | Bolt server, Cypher frontend/planner/runtime, kernel, storage, indexes, transactions, memory accounting |
| `neo4j-docs-bolt-src` | `17147230ad5b576992cc396ba1752196eddbb0d0` | Normative Bolt and PackStream behavior |
| `cypher-shell-src` | `3e7573e54c0e54f82506fdfa3068aebf7cf7f75a` | Driver-facing session and transaction behavior; historical shell oracle |
| `cypher-dsl-src` | `8bf1a556cd2addab0ff9046fdd2b0044542690dd` | Apache-licensed query construction, rendering, parser corpus, and TCK-oriented tests |

GDS algorithm implementations were intentionally not examined because they are outside this agent's assigned repository set. Algorithm-specific storage conclusions must be joined later with the GDS evidence lane. This dossier establishes the compatibility, parsing, admission, streaming, and runtime boundary around those operators.

## Bottom Line

### What must be compatible

- A pinned set of official Neo4j drivers must complete a supported Bolt handshake.
- Existing applications must be able to submit unchanged query text and parameters for the accepted query profile.
- Accepted queries must preserve parameter, null, scalar, list, map, node, relationship, path, column, record-order, and error-category semantics required by the founder query corpus.
- `RUN`, `PULL`, `DISCARD`, failure, interruption, and `RESET` behavior must be coherent enough that an official driver regards the connection as healthy.
- Result production must be demand-driven. The adapter must not materialize all records before serving a `PULL`.

### What must be different

- Query planning begins with a conservative resource estimate, not only cardinality and execution cost.
- Allocation starts only after admission grants a permit.
- The permit covers algorithm workspace, parsing, plan state, artifact pages, result buffers, caches, spill buffers, and concurrent-job multiplication.
- Every accepted job ends with a receipt.
- Approximation is never silent.
- Unsupported semantics are refused before an expensive artifact scan.

### What should not be built for the first product proof

- A mutable property graph database.
- General write transactions, WAL, recovery, locking, or MVCC parity.
- Neo4j's complete logical-plan operator hierarchy.
- All Cypher versions and all Bolt versions.
- Cluster routing, Fabric, causal bookmarks, replication, backup, schema administration, plugins, or broad procedure parity.
- Neo4j record-store, page-cache, and index implementation parity.

## Evidence Method

### A007-first reading order

The product thesis was read in full before source exploration. Every recommendation was then tested against three questions:

1. Does this reduce adoption friction for the A007 workload?
2. Does this help enforce a hard budget or produce a receipt?
3. Would omitting it prevent the first security/dependency/access-path proof?

If the answer to the third question was no and the code belonged to general database machinery, it was classified as defer-or-omit.

### Commands used

The global `code-graph` executable was unavailable. The repository-pinned CLI was invoked through `pnpm dlx @sdsrs/code-graph`, version `0.114.1`.

Representative commands, run with each assigned repository as the current directory:

```bash
pnpm dlx @sdsrs/code-graph health-check --deep --json
pnpm dlx @sdsrs/code-graph map --compact --json
pnpm dlx @sdsrs/code-graph tour community/bolt --json
pnpm dlx @sdsrs/code-graph tour community/cypher/front-end --json
pnpm dlx @sdsrs/code-graph search 'Bolt handshake protocol negotiation' --json
pnpm dlx @sdsrs/code-graph search 'PackStream reader writer chunk frame' --json
pnpm dlx @sdsrs/code-graph search 'Cypher parsing semantic planning runtime' --json
pnpm dlx @sdsrs/code-graph search 'memory tracker transaction memory pool' --json
pnpm dlx @sdsrs/code-graph show StateMachineImpl --refs --impact --json
pnpm dlx @sdsrs/code-graph show TransactionMemoryPool --refs --json
pnpm dlx @sdsrs/code-graph deps community/bolt/src/main/java/org/neo4j/bolt/fsm/StateMachineImpl.java --direction both --depth 3 --compact --json
pnpm dlx @sdsrs/code-graph impact runCypher --file cypher-shell/src/main/java/org/neo4j/shell/state/BoltStateHandler.java --include-tests --json
pnpm dlx @sdsrs/code-graph grep 'CypherPlanner' community/cypher
pnpm dlx @sdsrs/code-graph grep 'QueryMemoryTracker' community/cypher
```

Corpus and ledger commands:

```bash
git -C "$repo" rev-parse HEAD
git -C "$repo" ls-files -s
git -C "$repo" ls-files -z
sqlite3 "$repo/.code-graph/index.db" 'select path from files order by path;'
git -C "$repo" cat-file --batch
python3 scripts/validate_neo4j_family_evidence.py
```

`git ls-files` is the denominator. The authoritative integration denominator is `evidence/all-files-denominator.tsv`. The agent ledger was generated only from rows where `assigned_agent=agent-01`.

### Coverage-status semantics

| Status | Meaning in this ledger |
|---|---|
| `graph_indexed` | The tracked file exists in the repository's healthy code-graph index and was not superseded by a higher-assurance direct read. |
| `direct_read` | The exact Git blob was read completely and its Git object hash and byte count were verified. Founder-critical and high-relevance files were also examined semantically. |
| `generated_classified` | Generated source was identified and classified without treating its volume as hand-authored architecture. |
| `noncode_classified` | Documentation, configuration, fixtures, build files, or unsupported text artifacts were classified. |
| `binary_classified` | Binary fixtures, images, archives, executables, or other binary artifacts were classified without text interpretation. |

`direct_read` has two assurance layers:

- Denominator assurance: all 3,495 direct-read blobs were fully consumed through `git cat-file --batch`, byte-count checked, and SHA-1 recomputed.
- Semantic assurance: the critical paths named later in this document were read for architecture and behavior, not merely consumed by the blob pass.

This distinction prevents the file ledger from pretending that a batch byte scan is equivalent to understanding thousands of source files.

## Corpus Accounting

### Tracked files and principal source LOC

| Repository | Tracked files | Principal source or specification files | LOC |
|---|---:|---|---:|
| `neo4j-src` | 11,848 | 7,966 Java | 1,384,775 |
| `neo4j-src` |  | 2,754 Scala | 707,594 |
| `neo4j-src` |  | 4 ANTLR grammars | 6,932 |
| `neo4j-src` |  | 2 JavaCC grammars | 8,996 |
| `neo4j-src` |  | 65 feature specifications | 27,574 |
| `neo4j-docs-bolt-src` | 33 | 19 AsciiDoc specifications | 6,626 |
| `cypher-shell-src` | 164 | 125 Java | 19,009 |
| `cypher-dsl-src` | 802 | 669 Java | 79,138 |
| **Total** | **12,847** | **Java and Scala alone** | **2,190,516** |

The final LOC total in the preceding table is Java plus Scala across the four assigned repositories. It should not be read as one homogeneous implementation estimate. The useful subtotals are:

- `neo4j-src` Java plus Scala: 10,720 files and 2,092,369 LOC.
- `neo4j-src` parser grammars: 6 files and 15,928 LOC.
- All assigned Java: 8,760 files and 1,482,922 LOC.
- All assigned Scala: 2,754 files and 707,594 LOC.

Documentation, grammar, feature, fixture, and build-file LOC are intentionally not folded into that code total.

### Neo4j module sizes that expose the rewrite trap

| Module or subtree | Tracked files | LOC |
|---|---:|---:|
| `community/bolt/src/main` | 348 | 32,850 |
| `community/bolt/src/test` | 317 | 40,133 |
| `community/community-it/bolt-it/src` | 109 | 13,491 |
| `community/cypher/front-end` | 1,067 | 357,101 |
| `community/cypher/cypher-planner` | 484 | 184,319 |
| `community/cypher/cypher-logical-plans` | 46 | 15,937 |
| `community/cypher/physical-planning` | 52 | 12,908 |
| `community/cypher/interpreted-runtime` | 438 | 61,464 |
| `community/cypher/slotted-runtime` | 146 | 21,910 |
| `community/cypher/runtime-util` | 203 | 37,612 |
| `community/cypher/runtime-spec-suite` | 203 | 124,876 |
| `community/cypher/cypher/src/main` | 131 | 26,321 |
| `community/community-it/cypher-it/src` | 91 | 37,353 |
| `community/kernel-api/src/main` | 320 | 31,216 |
| `community/kernel/src/main` | 910 | 121,729 |
| `community/record-storage-engine/src/main` | 378 | 59,929 |
| `community/io/src/main` | 153 | 22,892 |
| `community/common/src/main/java/org/neo4j/memory` | 25 | 2,364 |
| `community/index/src/main` | 102 | 21,735 |

These subtrees overlap only where the path hierarchy itself overlaps; they are presented as local scale indicators, not a number to sum. The evidence supports a narrow compatibility layer. It does not support treating a complete rewrite as a weekend-scale project.

## Code-Graph Health

All four indexes returned `healthy: true`, `quick_check: ok`, schema version 10, no FTS drift, and no orphan-vector error. The CLI index was FTS-only; no embedding coverage was available.

| Repository | Graph nodes | Graph edges | Tracked files indexed by graph | Tracked files not indexed by graph | Unresolved call references |
|---|---:|---:|---:|---:|---:|
| `neo4j-src` | 113,831 | 1,632,830 | 8,002 | 3,846 | 36,076 |
| `neo4j-docs-bolt-src` | 3 | 1 | 2 | 31 | 6 |
| `cypher-shell-src` | 1,698 | 9,509 | 126 | 38 | 1,796 |
| `cypher-dsl-src` | 6,693 | 154,798 | 674 | 128 | 4,675 |
| **Total** | **122,225** | **1,797,138** | **8,804** | **4,043** | **42,553** |

The 2,754 Scala files in `neo4j-src` contain much of the Cypher frontend, planner, and runtime, but this code-graph build does not index Scala. Those unsupported source files were therefore directly read through the exact-blob pass; critical Scala files were then examined semantically with direct file reads and AST-context grep.

The 42,553 unresolved call references are parser or name-resolution gaps inside the structural graph. They are not unresolved file rows. File coverage and graph edge resolution are separate measures.

## Evidence Ledger Results

### Coverage counts

| Coverage status | Files |
|---|---:|
| `direct_read` | 3,495 |
| `graph_indexed` | 8,315 |
| `generated_classified` | 2 |
| `noncode_classified` | 983 |
| `binary_classified` | 52 |
| **Total** | **12,847** |

The underlying graph contains 8,804 tracked assigned files. The TSV reports 8,315 as `graph_indexed` because 489 graph-indexed files were upgraded to `direct_read` due to founder-critical paths, semantic importance, or relevance scores of at least 80.

Direct-read volume:

- 3,495 exact Git blobs.
- 31,723,568 bytes.
- 870,376 lines as a mechanical text-line count.
- 1,037 high-relevance rows, all `direct_read`.

### Per-repository coverage

| Repository | Direct | Graph | Generated | Non-code | Binary | Total |
|---|---:|---:|---:|---:|---:|---:|
| `neo4j-src` | 3,415 | 7,540 | 0 | 846 | 47 | 11,848 |
| `neo4j-docs-bolt-src` | 20 | 2 | 0 | 10 | 1 | 33 |
| `cypher-shell-src` | 13 | 117 | 2 | 31 | 1 | 164 |
| `cypher-dsl-src` | 47 | 656 | 0 | 96 | 3 | 802 |
| **Total** | **3,495** | **8,315** | **2** | **983** | **52** | **12,847** |

### Zero and unresolved checks

| Check | Result |
|---|---:|
| Agent-01 denominator rows | 12,847 |
| Agent-01 evidence rows | 12,847 |
| Missing assigned `repo/path` rows | 0 |
| Unexpected `repo/path` rows | 0 |
| Duplicate `repo/path` rows | 0 |
| Denominator blob mismatches | 0 |
| Denominator byte mismatches | 0 |
| Denominator extension mismatches | 0 |
| Invalid coverage statuses | 0 |
| Relevance values outside 1-100 | 0 |
| Relevance at least 80 without direct read | 0 |
| Empty evidence IDs | 0 |
| Duplicate agent-01 evidence IDs | 0 |
| Direct-read Git-object hash mismatches | 0 |
| Full multi-agent evidence-union rows | 32,262 |
| Full `validate_neo4j_family_evidence.py` result | PASS |
| Structural graph unresolved call references | 42,553 |

The repository-wide validator reconciled all 32,262 denominator rows across the three agent ledgers. The agent-01 slice independently reconciled with zero errors before the union check.

## Architecture Finding 1: Bolt Is a Bounded Adapter, Not the Engine

### Observed stack

```text
TCP connection
  -> Bolt identification and version negotiation
  -> chunk framing
  -> PackStream value and structure decoding
  -> request-message decoding
  -> protocol finite-state machine
  -> query service
  -> demand-driven record production
  -> PackStream encoding and chunk framing
  -> TCP backpressure
```

`AbstractProtocolHandshakeHandler` installs a Netty pipeline after selecting a protocol. The pipeline separates protocol selection, state/frame signaling, chunk decoding, PackStream structure decoding, message handling, encoding, and network throttling. That separation is worth reproducing as interfaces, not by copying GPL implementation.

The Bolt documentation establishes these compatibility facts:

- All Bolt connections begin with the fixed identification bytes `60 60 B0 17`.
- The legacy handshake submits four ordered version proposals.
- Bolt 4.3 adds minor-version ranges.
- Bolt 5.7 adds Manifest v1 with a server-advertised version list and capabilities.
- Bolt is stateful even though message exchange is request-response.
- Bolt 5.1 separates `HELLO` and `LOGON`, creating `NEGOTIATION`, `AUTHENTICATION`, and `READY` phases.
- `RUN` accepts a query but does not mean all results have been produced.
- `PULL` and `DISCARD` control an outstanding stream. `n`, `qid`, and `has_more` are protocol-visible.
- `FAILED` and `INTERRUPTED` suppress ordinary work until reset behavior restores a usable state.
- Bolt 5.5 is intentionally never negotiated.
- The checked compatibility document lists Bolt 6.0 for Neo4j 2025.10 onward, which is evidence that "support current Bolt" is a moving target and must be pinned.

### Low-RAM implications

1. **Bound frame and message size.** `ChunkFrameDecoder` accumulates chunk slices until a zero-sized terminator. The adapter needs per-message byte limits and must reject oversized messages before recursive decode.
2. **Bound PackStream recursion.** `PackstreamValueReader` recursively materializes lists, maps, and byte arrays. The Rust decoder needs limits for nesting depth, aggregate values, string bytes, byte-array bytes, map entries, list entries, and per-request parameter bytes.
3. **Do not buffer all records.** `NetworkResponseHandler` creates a record handler with a buffer and flush threshold. Knight Bus must tie record production to `PULL n` and transport writability.
4. **Count transport memory.** Decoder buffers, encoded output, TLS buffers, pending records, and channel queues belong to the same job permit or a separately enforced connection permit.
5. **Make cancellation real.** `RESET` cannot be cosmetic. It must propagate a cancellation token into artifact I/O, spill, and algorithm loops, then release permits deterministically.
6. **Pin a profile.** Implementing every documented Bolt version multiplies state and test matrices without improving the first proof.

### Recommended initial Bolt profile

Do not select a version from source aesthetics. Run a driver probe against the actual founder driver matrix and choose the smallest common protocol profile. A plausible spike candidate is Bolt 5.4 because it supports the modern `HELLO`/`LOGON` split and telemetry while avoiding Manifest v1, but this remains a testable founder gate, not an architectural fact.

The first supported connection contract should be direct `bolt://` connectivity. Cluster routing through `neo4j://` should be deferred unless founder query captures prove it is unavoidable.

## Architecture Finding 2: Cypher Is Several Products Hidden Behind One String

### Observed compilation pipeline

The Neo4j source divides query processing into explicit phases:

```text
query text
  -> preparse and options
  -> grammar parse
  -> AST construction
  -> syntax/deprecation checks
  -> preparatory rewrites
  -> semantic analysis and typing
  -> subquery isolation and re-analysis
  -> AST rewrites and literal extraction
  -> planner query / IR
  -> logical plan
  -> cardinality and cost planning
  -> physical planning / slot allocation
  -> interpreted, slotted, or other runtime
  -> QuerySubscriber-controlled execution
```

`FrontEndCompilationPhases.scala` demonstrates that parsing is only the first step. `SemanticAnalysis.scala` handles variable binding, types, expression dependencies, and semantic tables. `CompilationPhases.scala` continues through IR and logical planning. `PhysicalPlanner.scala` allocates runtime slots and argument state. `ExecutionEngine.scala` owns preparse and executable-query caches and returns a `QueryExecution` whose demand is controlled through a `QuerySubscriber`.

The single `LogicalPlan.scala` blob is 208,296 bytes and the surrounding planner/runtime modules are hundreds of thousands of lines. This is direct evidence against reproducing Neo4j's complete logical operator surface before proving the A007 product.

### Recommended Cypher architecture

Use a small accepted language, not a fake general implementation:

```text
Cypher text
  -> version-pinned parser
  -> typed read-only AST
  -> semantic validation
  -> normalized bounded-query IR
  -> capability match against artifact and operator registry
  -> complete working-set estimate
  -> fit/spill/approximate/refuse
  -> artifact-native execution plan
```

The normalized IR should contain only concepts the product can verify, such as:

- node and relationship scans constrained by labels or types;
- property predicates needed by the founder corpus;
- bounded expansions;
- shortest or bounded access paths;
- projection of scalar, node, relationship, and path values;
- filtering, ordering, limiting, and small aggregations where resource bounds are known;
- explicitly registered analytical procedure calls if the eventual query corpus uses them.

Anything that parses but cannot be estimated and enforced must fail capability validation before execution. Parsing a query is not a promise to run it.

### Parser strategy

There are two viable clean-room routes:

| Route | Benefit | Risk | Recommendation |
|---|---|---|---|
| Implement a narrow grammar from behavior tests and public language references | Small, controlled, Rust-native, honest subset | More parser work; must avoid accidental semantic drift | Preferred for the first proof |
| Use or adapt an appropriately licensed parser component | Faster syntax coverage | Syntax coverage can create false expectations of runtime parity | Accept only if license and semantic boundary are explicit |

The assigned `cypher-dsl-src` repository is Apache 2.0 and useful for query generation, rendering, and corpus construction. Its parser builds DSL statements and supports transformations, but it is not Neo4j's server semantic analyzer, cost planner, or runtime. It should not be mistaken for a drop-in server frontend.

## Architecture Finding 3: Neo4j Runtime Patterns Are Reusable, Neo4j Runtime Breadth Is Not

### Reusable patterns

| Neo4j pattern | Evidence | Knight Bus adaptation |
|---|---|---|
| Explicit compilation phases | Frontend and planner phase files | Keep parsing, semantics, capability planning, resource planning, and execution as separately testable stages. |
| Demand-controlled execution | `ExecutionEngine` and `QuerySubscriber` | Pull records only when Bolt demand and output permits allow them. |
| Multiple runtime implementations behind a contract | `CommunityRuntimeFactory`, interpreted and slotted runtimes | Select among in-memory exact, spill exact, and approved approximate operators behind one typed contract. |
| Physical metadata separated from logical intent | `PhysicalPlanner` and slot configuration | Attach artifact layout, frontier representation, spill partitions, and memory permits to a physical plan. |
| Cursor-based storage access | `StorageReader`, `NodeCursor`, `RelationshipTraversalCursor` | Expose primitive ID and borrowed-slice cursors over immutable algorithm-shaped artifacts. |
| Fast bounded degree checks | `NodeCursor.degreeWithMax` | Stop pathological expansions as soon as a configured degree cap is reached. |
| Hierarchical memory trackers | `MemoryTracker`, `LocalMemoryTracker`, `TransactionMemoryPool` | Use RAII permits scoped to job, operator, partition, transport, and spill. |
| High-water accounting | transaction and query memory trackers | Put estimator error and actual high-water values in the receipt. |
| Caches separated by purpose | Cypher preparser and executable query caches | Keep parser/plan caches, but charge retained bytes to a bounded cache budget. |

### Patterns that need stronger A007 semantics

Neo4j's tracking abstractions provide useful mechanics, but they are not the complete A007 contract:

- Query memory tracking has a `NO_TRACKING` mode. Bounded execution cannot.
- `QueryMemoryTracker` notes that its local tracker may be unbounded because transaction trackers enforce limits. Knight Bus needs one auditable ownership chain with no unaccounted layer.
- Parallel runtime tracking does not always expose per-operator high-water marks unless profiling, because detailed tracking has overhead. Receipts require deliberate measurement policy.
- Heap and native reservations do not by themselves equal process RSS. Mapped pages, allocator fragmentation, stacks, runtime metadata, network buffers, caches, and kernel page-cache effects must be accounted for or bounded separately.
- `PageCache` has page-count accounting, but mapped capacity and resident memory are different quantities.
- Concurrency multiplies otherwise valid per-query working sets. Admission must consider all active permits before starting another job.

### Proposed permit hierarchy

```text
process hard ceiling
  +-- reserved runtime headroom
  +-- bounded global caches
  +-- connection permits
  +-- admitted job permits
        +-- parser and semantic state
        +-- physical-plan state
        +-- artifact resident window
        +-- algorithm workspace
        +-- output and PackStream buffers
        +-- spill buffers and merge state
```

The sum of granted permits must stay below the process ceiling minus calibrated headroom. A cgroup or equivalent OS-level limit should be the final enforcement backstop, not the estimator.

## Architecture Finding 4: Storage Interfaces Are Useful; the Database Kernel Is Mostly a Defer Signal

`StorageReader`, `NodeCursor`, and `RelationshipTraversalCursor` expose a clean read-oriented pattern. `RecordStorageReader` separates storage/schema/counts concerns and allocates cursors with a memory tracker. These are useful architectural ideas for immutable artifact readers.

In contrast, `KernelTransactionImplementation` is a 75,045-byte class whose construction brings together locks, transaction state, schema, indexing, security, procedures, memory, and commit machinery. The surrounding kernel, record store, page cache, and index modules show how quickly "support Cypher" becomes "rebuild a database."

For A007, replace the mutable kernel with a snapshot contract:

```text
ArtifactSnapshot
  metadata()
  checksum()
  node_dictionary()
  label_or_type_filter()
  property_column(name)
  adjacency_cursor(selection)
  degree_with_cap(node, selection, cap)
  operator_artifact(algorithm_family, variant)
```

The facade may expose a read transaction to the driver, but internally that transaction should be an immutable snapshot handle plus resource permit, not a general ACID transaction.

### Minimal read-side indexes

Build only indexes justified by captured queries:

- external ID to dense ID dictionary;
- label/type bitmaps or compact postings;
- selected property columns and predicate accelerators;
- adjacency offsets and neighbor arrays in the layout chosen for the operator;
- optional path-reconstruction parent storage;
- artifact checksums and schema metadata.

Do not build general schema indexes, uniqueness constraints, write indexes, or index population machinery for the first proof.

## Architecture Finding 5: Shell and DSL Repositories Are Oracles with Different Trust Levels

### Cypher Shell

The separate `cypher-shell-src` repository states that it contains Cypher Shell 1.1 and that newer versions moved into the Neo4j monorepo. It is therefore a historical behavior source, not the authority for current Bolt compatibility.

`BoltStateHandler` shows how the shell owns a Java driver, sessions, bookmarks, and explicit transaction state. Its integration tests are useful as black-box scenarios for session health, autocommit, begin/commit/rollback, errors, and reconnection. The code is GPLv3 and must remain oracle-only for a permissive Knight Bus implementation.

### Cypher DSL

The DSL repository is Apache 2.0. It provides:

- a large query builder surface;
- rendering behavior;
- a parser module;
- options for query transformations;
- TCK-oriented and quantified-path-pattern tests;
- examples of procedures and subqueries.

Use it to generate accepted and rejected query corpora, normalize formatting variants, and exercise the facade. Do not infer server semantic or runtime parity from successful DSL parsing.

## Critical Direct-Read Evidence

The following are the highest-value semantic reads. Evidence IDs resolve to immutable repo/path/blob/byte rows in `evidence/agent-01-files.tsv`.

### Bolt and PackStream

| Evidence ID | Exact path | Why it matters |
|---|---|---|
| `A01-000987` | `neo4j-docs-bolt-src/modules/ROOT/pages/bolt/handshake.adoc` | Identification bytes, legacy negotiation, ranges, Manifest v1 |
| `A01-000989` | `neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc` | Message signatures, chunking, pipelining, RUN/PULL/DISCARD/RESET |
| `A01-000990` | `neo4j-docs-bolt-src/modules/ROOT/pages/bolt/server-state.adoc` | Protocol FSM and failure/interruption behavior |
| `A01-000994` | `neo4j-docs-bolt-src/modules/ROOT/pages/packstream/index.adoc` | Wire value encoding boundary |
| `A01-001097` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/negotiation/ProtocolVersion.java` | Version representation and matching |
| `A01-001101` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/negotiation/codec/ProtocolNegotiationRequestDecoder.java` | Handshake decode boundary |
| `A01-001103` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/negotiation/handler/AbstractProtocolHandshakeHandler.java` | Post-negotiation pipeline assembly |
| `A01-001105` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/negotiation/handler/ModernProtocolHandshakeHandler.java` | Manifest negotiation and tracked connection state |
| `A01-001075` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/fsm/StateMachineImpl.java` | Failed/interrupted handling and state transitions |
| `A01-001091` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/fsm/state/State.java` | State/transition separation |
| `A01-001179` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/protocol/common/fsm/response/NetworkResponseHandler.java` | Record buffering, flush threshold, terminal summaries |
| `A01-001236` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/protocol/common/message/decoder/streaming/DefaultPullMessageDecoder.java` | Demand and query-ID decoding |
| `A01-001241` | `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/protocol/common/message/decoder/transaction/DefaultRunMessageDecoder.java` | Query, parameters, and extra metadata |
| `A01-001374` | `neo4j-src/community/bolt/src/main/java/org/neo4j/packstream/codec/transport/ChunkFrameDecoder.java` | Message framing and byte-limit surface |
| `A01-001389` | `neo4j-src/community/bolt/src/main/java/org/neo4j/packstream/io/PackstreamBuf.java` | Primitive PackStream buffer behavior |
| `A01-001394` | `neo4j-src/community/bolt/src/main/java/org/neo4j/packstream/io/value/PackstreamValueReader.java` | Recursive value materialization risk |

### Cypher frontend, planning, and runtime

| Evidence ID | Exact path | Why it matters |
|---|---|---|
| `A01-004419` | `neo4j-src/community/cypher/cypher/src/main/java/org/neo4j/cypher/internal/preparser/javacc/cypherPreParser.jj` | Query options before grammar parsing |
| `A01-005304` | `neo4j-src/community/cypher/front-end/parser/v25/parser/src/main/antlr4/org/neo4j/cypher/internal/parser/v25/Cypher25Parser.g4` | Current grammar scale |
| `A01-005396` | `neo4j-src/community/cypher/front-end/parser/v5/parser/src/main/antlr4/org/neo4j/cypher/internal/parser/v5/Cypher5Parser.g4` | Cypher 5 grammar scale |
| `A01-004974` | `neo4j-src/community/cypher/front-end/frontend/src/main/scala/org/neo4j/cypher/internal/frontend/phases/FrontEndCompilationPhases.scala` | Frontend phase sequence |
| `A01-004992` | `neo4j-src/community/cypher/front-end/frontend/src/main/scala/org/neo4j/cypher/internal/frontend/phases/SemanticAnalysis.scala` | Binding and type semantics |
| `A01-003915` | `neo4j-src/community/cypher/cypher-planner/src/main/scala/org/neo4j/cypher/internal/compiler/phases/CompilationPhases.scala` | IR and planning phases |
| `A01-003884` | `neo4j-src/community/cypher/cypher-planner/src/main/scala/org/neo4j/cypher/internal/compiler/CypherPlanner.scala` | Compiler/planner orchestration |
| `A01-003843` | `neo4j-src/community/cypher/cypher-logical-plans/src/main/scala/org/neo4j/cypher/internal/logical/plans/LogicalPlan.scala` | Breadth of logical operators |
| `A01-006197` | `neo4j-src/community/cypher/physical-planning/src/main/scala/org/neo4j/cypher/internal/physicalplanning/PhysicalPlanner.scala` | Slot and physical metadata |
| `A01-004444` | `neo4j-src/community/cypher/cypher/src/main/scala/org/neo4j/cypher/internal/ExecutionEngine.scala` | Caches, compilation, and demand-controlled query execution |
| `A01-004448` | `neo4j-src/community/cypher/cypher/src/main/scala/org/neo4j/cypher/internal/InterpretedRuntime.scala` | Logical plan to interpreted pipe execution |
| `A01-004460` | `neo4j-src/community/cypher/cypher/src/main/scala/org/neo4j/cypher/internal/SlottedRuntime.scala` | Physical slots and runtime selection |

### Memory, storage, and kernel

| Evidence ID | Exact path | Why it matters |
|---|---|---|
| `A01-002127` | `neo4j-src/community/common/src/main/java/org/neo4j/memory/MemoryTracker.java` | Heap/native accounting contract |
| `A01-002119` | `neo4j-src/community/common/src/main/java/org/neo4j/memory/LocalMemoryTracker.java` | Local limit and high-water behavior |
| `A01-006606` | `neo4j-src/community/cypher/runtime-util/src/main/scala/org/neo4j/cypher/internal/runtime/memory/QueryMemoryTracker.scala` | Query/operator tracking modes and caveats |
| `A01-009272` | `neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/impl/api/TransactionMemoryPool.java` | Reservation rollback, hierarchy, high-water reset |
| `A01-008536` | `neo4j-src/community/kernel-api/src/main/java/org/neo4j/storageengine/api/StorageReader.java` | Read-side storage boundary |
| `A01-008300` | `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/NodeCursor.java` | Primitive traversal and bounded degree lookup |
| `A01-008317` | `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/RelationshipTraversalCursor.java` | Contextual relationship traversal |
| `A01-011100` | `neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordStorageReader.java` | Cursor allocation and record-store read facade |
| `A01-008159` | `neo4j-src/community/io/src/main/java/org/neo4j/io/pagecache/PageCache.java` | Page-cache capacity and mapping concepts |
| `A01-008196` | `neo4j-src/community/io/src/main/java/org/neo4j/io/pagecache/impl/muninn/MuninnPageCache.java` | Concrete page-cache breadth |
| `A01-009252` | `neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/impl/api/KernelTransactionImplementation.java` | Concrete evidence of full-kernel scope explosion |
| `A01-009655` | `neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/impl/query/QueryExecutionEngine.java` | Kernel-facing streaming query contract |

### Shell and DSL

| Evidence ID | Exact path | Why it matters |
|---|---|---|
| `A01-000889` | `cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/state/BoltStateHandler.java` | Driver/session/transaction behavior |
| `A01-000829` | `cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/CypherShell.java` | Shell-to-driver boundary |
| `A01-000877` | `cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/parser/ShellStatementParser.java` | Client-side statement framing, not server grammar |
| `A01-000472` | `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java` | DSL parsing entry point |
| `A01-000471` | `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherDslASTFactory.java` | Large AST construction surface |
| `A01-000484` | `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/Options.java` | Parser transformations and policy options |
| `A01-000509` | `cypher-dsl-src/neo4j-cypher-dsl-parser/src/test/java/org/neo4j/cypherdsl/parser/TckTests.java` | Query corpus and TCK-oriented oracle |
| `A01-000551` | `cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/DefaultStatementBuilder.java` | Builder surface scale |
| `A01-000733` | `cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/renderer/Renderer.java` | Canonical rendering boundary |

## License and Clean-Room Boundary

| Repository | Observed top-level license | Permitted role in this program |
|---|---|---|
| `neo4j-src` | GPLv3 | Behavioral and architectural oracle only unless legal review approves a specific use |
| `neo4j-docs-bolt-src` | CC BY-NC-SA 4.0 | Protocol research oracle; do not copy specification text into a commercial permissive implementation |
| `cypher-shell-src` | GPLv3 | Historical black-box and behavior oracle only |
| `cypher-dsl-src` | Apache 2.0 | Potentially reusable with notices and legal review; primarily useful here for corpus generation |

Recommended process:

1. Freeze immutable oracle commits and evidence IDs.
2. Write behavior tests from independently stated requirements and black-box observations.
3. Keep implementation agents away from GPL source text where practical.
4. Record provenance for every protocol fixture and language test.
5. Have counsel review the protocol-document and grammar strategy before distribution.

This is engineering guidance, not legal advice.

## Must Build, Defer, or Refuse

| Surface | First proof decision | Reason |
|---|---|---|
| One pinned Bolt profile | **Must build** | Official-driver adoption adapter |
| Identification, negotiation, HELLO/LOGON | **Must build** | Driver cannot establish a healthy session without it |
| PackStream scalar/list/map plus required graph values | **Must build, corpus-bounded** | Required for parameters and result rows |
| PackStream depth/size limits | **Must build** | Prevent pre-execution memory blowups |
| RUN/PULL/DISCARD/RESET FSM | **Must build** | Correct streaming and recovery behavior |
| Demand-driven record stream | **Must build** | Prevent output materialization from violating the budget |
| Static/local authentication mode | **Must build minimally** | Satisfy driver flow without rebuilding enterprise auth |
| Read-only Cypher parser and semantic checker | **Must build, narrow** | Preserve unchanged accepted queries honestly |
| Typed bounded-query IR | **Must build** | Separates compatibility syntax from artifact execution |
| Resource estimator and admission | **Must build** | Core A007 differentiation |
| `fit/spill/approximate/refuse` decision | **Must build** | Core user promise |
| OS-level hard-limit backstop | **Must build** | Accounting bugs must not become OOM events |
| Receipt and estimator calibration | **Must build** | Core trust and improvement loop |
| Bounded parser/plan/result caches | **Must build** | Hidden retained memory otherwise breaks the promise |
| Immutable snapshot read transaction | **Must build if driver needs it** | Enough to support read sessions and optional explicit read transactions |
| Explicit multi-query read transaction | **Founder-gated** | Add only if captured production usage requires it |
| Node/relationship/path result values | **Founder-gated but likely** | Required only if captured queries return graph values rather than scalar projections |
| Neo4j routing protocol and `neo4j://` | **Defer** | Direct `bolt://` is sufficient for first local/single-node proof |
| Manifest v1 and complete Bolt version matrix | **Defer** | Moving compatibility matrix; high test multiplication |
| Full openCypher/Cypher 25 semantics | **Defer and refuse unsupported queries** | Massive planner/runtime surface unrelated to first wedge |
| Writes, CREATE, MERGE, DELETE, SET | **Refuse** | Turns bounded analyzer into mutable database |
| Schema DDL, constraints, index administration | **Refuse/defer** | Not needed for immutable artifacts |
| WAL, recovery, locking, MVCC, replication | **Omit** | Database machinery, not bounded analytical execution |
| General page cache and record-store parity | **Omit** | Use artifact-specific readers and OS-backed bounded windows |
| General query cost planner | **Defer** | Build a resource and capability planner for accepted shapes |
| Procedures/plugins/user extensions | **Refuse by default** | Unbounded opaque memory and execution behavior |
| Browser and shell UI parity | **Defer** | Driver compatibility is the adoption requirement |
| Universal TCK pass | **Defer** | Pass a tagged accepted subset and reject the rest deterministically |

## Founder-Gated Compatibility Requirements

These are requirement candidates for an executable specification. Thresholds and driver versions remain founder inputs.

### REQ-A01-001: Unchanged accepted query

**WHEN** a pinned official Neo4j driver sends a query and parameters from the accepted founder corpus to the Knight Bus Bolt endpoint

**THEN** the adapter SHALL accept the original query text without a Knight Bus-specific rewrite

**AND** SHALL return the expected columns, values, order, null behavior, and terminal summary for the supported profile.

### REQ-A01-002: Pinned protocol negotiation

**WHEN** a supported driver offers Bolt versions

**THEN** the server SHALL negotiate only a documented pinned profile

**AND** SHALL return no match for unsupported profiles

**AND** SHALL never imply support for a message or state transition it cannot execute.

### REQ-A01-003: Bounded decode

**WHEN** a client sends an oversized, over-nested, or over-cardinality PackStream value or Bolt message

**THEN** the connection SHALL fail with the pinned protocol's appropriate error behavior before allocating beyond the connection decode permit.

### REQ-A01-004: State-machine fidelity

**WHEN** the driver sends valid and invalid sequences involving `RUN`, `PULL`, `DISCARD`, failure, interruption, and `RESET`

**THEN** the adapter SHALL match the accepted profile's state transitions and error categories

**AND** SHALL release query and transport permits when a stream ends, fails, is discarded, or is reset.

### REQ-A01-005: Early capability refusal

**WHEN** a query parses but contains an unsupported clause, function, procedure, write, transaction mode, or result type

**THEN** the adapter SHALL refuse it before an expensive artifact scan or algorithm allocation

**AND** SHALL return a stable machine-readable reason.

### REQ-A01-006: Complete working-set estimate

**WHEN** an accepted query is compiled against an artifact manifest

**THEN** the planner SHALL estimate parser state, plan state, resident artifact pages, algorithm workspace, output buffers, cache effects, spill buffers, concurrency headroom, and runtime overhead

**AND** SHALL record the estimator version and assumptions.

### REQ-A01-007: Explicit execution decision

**WHEN** the estimate is compared with the user's hard budget and policy

**THEN** the planner SHALL choose exactly one of `fit`, `spill`, `approximate`, or `refuse`

**AND** SHALL make the decision observable before expensive execution begins.

### REQ-A01-008: No silent approximation

**WHEN** exact execution cannot fit the declared budget

**THEN** the adapter SHALL use approximation only if the user has explicitly enabled a named approximation profile

**AND** SHALL otherwise spill exactly or refuse.

### REQ-A01-009: Hard memory ceiling

**WHEN** a job runs under a declared peak-memory budget

**THEN** process RSS SHALL remain below the configured hard ceiling at the agreed sampling and enforcement resolution

**AND** an enforcement breach SHALL cancel the job and fail the verification suite.

### REQ-A01-010: Demand-driven results

**WHEN** a client requests `PULL {n: N}`

**THEN** the runtime SHALL produce at most the requested record demand before yielding

**AND** SHALL bound queued encoded records and transport buffers

**AND** SHALL preserve `has_more` behavior for partial streams.

### REQ-A01-011: Cancellation and reset

**WHEN** a client discards a stream, disconnects, or sends `RESET`

**THEN** cancellation SHALL reach artifact reads, spill operations, and algorithm loops

**AND** all associated permits and temporary files SHALL be released within a measured bound.

### REQ-A01-012: Receipt

**WHEN** a job succeeds, refuses, fails, or is cancelled

**THEN** the system SHALL persist a receipt containing query fingerprint, artifact checksum, estimator version, estimate, decision, policy, peak RSS, tracked heap/native/mapped/buffer bytes, spill bytes, I/O bytes, runtime, result checksum or error, and estimator error where measurable.

The receipt handle may be exposed through stable response metadata or a sidecar API, but it SHALL NOT alter the user's query result rows.

### REQ-A01-013: Bounded caches

**WHEN** parsing or compiled-plan caches retain entries across jobs

**THEN** retained bytes SHALL be measured and charged to a separate bounded cache permit

**AND** cache eviction SHALL be deterministic under pressure.

### REQ-A01-014: Differential oracle

**WHEN** an accepted query fixture runs against both the pinned Neo4j oracle and Knight Bus

**THEN** the verification harness SHALL compare row values, column metadata, ordering where defined, graph-value encoding, null behavior, error category, and stream termination

**AND** SHALL store the oracle and Knight Bus receipts or benchmark metadata with immutable fixture versions.

### REQ-A01-015: Clean-room provenance

**WHEN** a protocol, parser, semantic, or runtime fixture is added

**THEN** its source, license, derivation method, oracle commit, and implementation exposure status SHALL be recorded

**AND** GPL and CC BY-NC-SA source text SHALL not be copied into the permissive implementation without approved legal review.

## Founder Decisions Required Before Coding the Facade

1. Which 20-50 real security, dependency, and access-path queries define the first accepted corpus?
2. Which official driver languages and exact versions are non-negotiable?
3. Is changing only the connection URI acceptable while query text and driver API stay unchanged?
4. Must the first release support explicit read transactions, or is autocommit sufficient?
5. Do accepted queries return nodes/relationships/paths, or only scalar projections?
6. Which Bolt profile is the smallest one shared by the target drivers?
7. What are the first hard memory tiers: for example 2 GiB, 5 GiB, and 10 GiB?
8. Which decisions may spill, and what local-disk budget is acceptable?
9. Which workloads permit approximation, and what error contract is acceptable?
10. Where should receipts be retrieved without changing result rows?

No architecture can honestly claim "Neo4j compatible" until these gates turn compatibility into a finite matrix.

## Verification Spine Derived from the Evidence

### Layer 1: Protocol fixtures

- Handshake bytes and version-selection fixtures.
- Chunk fragmentation and reassembly fixtures.
- PackStream boundary, nesting, malformed input, and allocation-limit fixtures.
- Message-signature and metadata fixtures.
- FSM sequence tests for success, failure, interruption, discard, and reset.

### Layer 2: Query fixtures

- Accepted founder query corpus with parameters and expected typed AST.
- Rejected corpus with stable refusal reasons.
- DSL-generated formatting and expression variants.
- A tagged TCK subset, never an ambiguous claim of full TCK support.

### Layer 3: Differential execution

- Run accepted queries against the pinned Neo4j oracle and Knight Bus.
- Compare semantic results separately from timing.
- Compare cold, warm, and dirty-cache modes separately.
- Preserve exact artifact, query, driver, protocol, and commit versions.

### Layer 4: Resource contracts

- Verify the estimator before execution.
- Verify decision selection at budget boundaries.
- Verify peak RSS with an OS-level observer.
- Verify spill bytes and cleanup.
- Verify result backpressure under slow clients.
- Verify concurrent admission does not exceed the process ceiling.
- Verify the receipt reconciles with independent measurements.

### Layer 5: Failure injection

- Disconnect during parse, planning, artifact read, algorithm execution, spill, and result streaming.
- Send `RESET` while work is queued and while work is active.
- Exhaust memory permits, disk permits, and output permits independently.
- Corrupt artifact checksums and spill runs.
- Force estimator underprediction and prove the hard backstop still holds.

## Proposed First Architecture

```text
Official Neo4j driver
        |
        v
Pinned Bolt compatibility profile
  handshake | PackStream limits | FSM | PULL backpressure
        |
        v
Read-only Cypher compatibility frontend
  parse | semantic check | normalize | reject unsupported
        |
        v
Bounded query IR
  artifact requirements | result shape | exactness policy
        |
        v
Capability and resource planner
  complete estimate -> fit / spill / approximate / refuse
        |
        v
Admission controller
  process ceiling | active permits | disk permits | headroom
        |
        v
Knight Bus artifact runtime
  primitive cursors | algorithm-shaped storage | bounded operators
        |
        v
Demand-driven result encoder
  RECORD stream | SUCCESS/FAILURE | receipt handle
        |
        v
Receipt store and calibration history
```

The key design rule is that the compatibility layer cannot bypass admission. It translates syntax and wire behavior, but the resource planner owns the right to run.

## Falsifiers

The compatibility-facade strategy should be reconsidered if any of these becomes true:

1. **Founder-query falsifier:** Most captured production queries require writes, arbitrary procedures, schema mutation, or broad OLTP semantics rather than bounded analysis.
2. **Driver falsifier:** The target official drivers cannot operate against a pinned minimal Bolt profile without implementing routing, broad authentication, or a large moving version matrix.
3. **Language falsifier:** The smallest valuable query corpus still requires enough Cypher semantics that a narrow frontend approaches the cost of a general implementation.
4. **Budget falsifier:** Complete working-set estimates cannot be made conservative and useful from artifact metadata and query shape.
5. **Enforcement falsifier:** RSS, mapped residency, allocator overhead, or kernel caching cannot be bounded tightly enough for the advertised tiers on supported operating systems.
6. **Streaming falsifier:** Official-driver semantics force result materialization or buffering that consumes a material fraction of the promised budget.
7. **Receipt falsifier:** Target users do not use receipts to choose a mode, trust a run, debug a refusal, or reduce infrastructure cost.
8. **Wedge falsifier:** Users prefer export-time integration or an existing engine enough that the Bolt/Cypher adapter does not materially improve adoption.
9. **Differentiation falsifier:** Neo4j, GDS, or another engine already provides equivalent hard enforcement, portable artifact execution, decision transparency, and receipts for the target workflow.
10. **Clean-room falsifier:** The legal or provenance cost of compatible protocol/language behavior exceeds the adoption value.

These falsifiers are more important than completing a feature checklist. The first proof should be designed to test them quickly.

## Recommended Sequence

1. Freeze the founder query and driver corpus.
2. Run a two-day Bolt-profile spike against those exact official driver versions.
3. Define accepted and refused Cypher constructs from the corpus.
4. Create the typed bounded-query IR before implementing a broad parser.
5. Define the full working-set equation and receipt schema before an execution operator.
6. Wire one read-only access-path query through handshake, parse, admission, artifact execution, `PULL`, and receipt.
7. Differentially verify results against Neo4j.
8. Verify the same run at multiple hard memory tiers, including exact fit, exact spill, explicit approximation if allowed, and refusal.
9. Add language or protocol surface only when a captured founder query or driver proves it is necessary.

## Final Assessment

Neo4j source is most valuable here as a behavioral oracle and a map of complexity. It shows exactly why compatibility must be separated from implementation parity.

The smallest credible product is not "Neo4j in Rust." It is:

> A Neo4j-driver-compatible, read-only doorway into an artifact-native bounded analytical runtime, supporting a measured Cypher subset and returning verifiable results under hard resource budgets.

That is compatible where adoption needs compatibility, different where the product needs differentiation, and narrow enough to verify before the team inherits the obligations of a database vendor.

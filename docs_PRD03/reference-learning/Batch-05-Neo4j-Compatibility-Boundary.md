# Batch 05: Neo4j Compatibility Boundary

Date: 2026-06-24

Assigned lanes:

- `Surface lane`
- `Capability lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Neo4j-compatible API`
- `Neo4j-shaped OLTP`
- `Complete GDS surface`
- `Published OLAP snapshots`
- `Strict holistic RAM`

Requirement IDs touched in this batch:

- `REQ-LEARN-002.0`
- `REQ-LEARN-003.0`
- `REQ-LEARN-004.0`
- `REQ-LEARN-005.0`
- `REQ-LEARN-020.0`
- `REQ-LEARN-021.0`
- `REQ-LEARN-022.0`
- `REQ-LEARN-016.0`
- `REQ-LEARN-017.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-040.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-053.0`

Batch status:

- This batch bounds what “Neo4j-compatible” actually means at the front door.
- This batch does not claim that v003 already implements these behaviors.
- This batch turns compatibility from a slogan into a concrete ABI boundary:
  Bolt, Cypher, procedures, values, driver workflows, GDS user flows, and APOC
  expectations.

## Clone Coverage Ledger

| local_repo | exists_now | upstream_hint | branch_or_head | study_role | required_or_optional | current_use | note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `gitrefrepo/neo4j-src` | yes | `neo4j/neo4j` | `release/5.26.0 @ c68156edf24` | OLTP and procedure oracle | required | active study | record-store, traversal cursor, procedure signature, and value conversion evidence |
| `gitrefrepo/neo4j-docs-bolt-src` | yes | `neo4j/docs-bolt` | `1714723` | Bolt wire oracle | required | active study | docs-first, treated as `GraphToolLowYield` in the ledger |
| `gitrefrepo/neo4j-testkit-src` | yes | `neo4j-drivers/testkit` | `ec46b65` | client-behavior oracle | required | active study | captures transaction, bookmark, retry, and result-shape expectations |
| `gitrefrepo/opencypher-src` | yes | `opencypher/openCypher` | `677cbaf` | Cypher grammar and TCK oracle | required | active study | docs/spec-first, treated as `GraphToolLowYield` in the ledger |
| `gitrefrepo/neo4j-apoc-src` | yes | `neo4j/apoc` | `11dbf56` | APOC boundary oracle | required | active study | category breadth and deprecation burden evidence |
| `gitrefrepo/neo4j-apoc-procedures-src` | yes | `neo4j/apoc` procedures split | `940033f` | APOC boundary oracle | required | active study | extends APOC surface count |
| `gitrefrepo/neo4j-python-driver-src` | yes | official Neo4j Python driver | `9e23c904` | official driver canary | required | active study | `execute_query`, bookmarks, result semantics |
| `gitrefrepo/neo4j-java-driver-src` | yes | official Neo4j Java driver | `7652d3c3f` | official driver canary | required | active study | session, autocommit, and transaction semantics |
| `gitrefrepo/neo4j-gds-client-src` | yes | GDS Python client | `e96f9066` | GDS workflow canary | required | active study | real projection, mutate, model, and prediction flows |
| `gitrefrepo/gds-agent-src` | yes | `neo4j-contrib/gds-agent` | `65d1894` | GDS workflow canary | required | active study | session detection and remote projection fallback |
| `gitrefrepo/neo4rs-src` | yes | `neo4j-labs/neo4rs` | `19f244a` | community driver canary | optional but useful | active study | routing, scheme translation, bookmark/error semantics |
| `gitrefrepo/cypher-shell-src` | yes | `neo4j/cypher-shell` | `3e7573e` | CLI canary | optional but useful | active study | driver-style connection configuration |
| `docs_PRD03/reference-learning/Reference-Shelf-Graph-Evidence-Ledger.md` | yes | local control artifact | current worktree | graph-tool run ledger | required support artifact | active study | records which repos already have reusable dual-tool evidence |

## Evidence Ledger

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CLAIM-B05-001` | `REQ-LEARN-002.0` | source | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java:30-32,55-77` | `NodeRecordFormat` | Neo4j node records are fixed-size `15` byte records with next-relationship id, next-property id, label bits, and a dense-node flag. | The OLTP source-of-truth shape is compact and pointer-rich, not adjacency-array shaped. | v003 can preserve this OLTP boundary while serving OLAP from a different published shape. | Neo4j-shaped OLTP | Falsifier: a more authoritative Neo4j path bypasses record formats for transactional truth. |
| `CLAIM-B05-002` | `REQ-LEARN-002.0` | source | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java:30-35,67-113` | `RelationshipRecordFormat` | Relationship records are fixed-size `34` byte records carrying node ids, type, prev/next relationship chain pointers, next-property id, and chain markers. | Neo4j transactional traversal is fundamentally chain/group navigation, not CSR offsets. | v003 OLTP compatibility work must preserve chain semantics even if OLAP never reads them directly. | Neo4j-shaped OLTP | Falsifier: later Neo4j evidence shows relationships are materialized transactionally from a different abstraction layer. |
| `CLAIM-B05-003` | `REQ-LEARN-002.0` | source | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java:33-39,64-103` | `PropertyRecordFormat` | Property records are `41` bytes and store prev/next property links plus a bounded payload of property blocks whose type determines how many blocks are consumed. | Property storage is linked and payload-encoded, so OLTP property semantics and OLAP property sidecars are separate design problems. | A later sidecar batch can keep OLAP typed columns while OLTP remains record-shaped. | Neo4j-shaped OLTP | Falsifier: Neo4j property semantics required by clients are shallow enough to ignore record behavior. |
| `CLAIM-B05-004` | `REQ-LEARN-002.0` | source | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:76-103,146-204` | `RecordRelationshipTraversalCursor` | Traversal chooses between direct relationship-chain walking and dense-node group walking, with a small state machine over incoming, outgoing, and loop chains for dense nodes. | Neo4j transactional adjacency behavior is a correctness contract, but it is not the cheapest OLAP read shape. | The same graph can have Neo4j-shaped OLTP truth and published CSR-like OLAP snapshots without contradiction. | Neo4j-shaped OLTP | Falsifier: GDS or driver-visible semantics depend on OLAP reading these same chain structures directly. |
| `CLAIM-B05-005` | `REQ-LEARN-002.0` | source | `gitrefrepo/neo4j-src/community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java:684-688` | `dense_node_threshold` | Neo4j exposes `db.relationship_grouping_threshold` with default `50` as the threshold for considering a node dense. | Dense-node handling is part of the OLTP compatibility burden and must be preserved at the transactional layer. | OLAP snapshots may ignore this threshold internally if the published graph is already normalized to dense ids. | Neo4j-shaped OLTP | Falsifier: higher-level semantics make dense-node grouping irrelevant to externally visible behavior. |
| `CLAIM-B05-006` | `REQ-LEARN-005.0` | source | `gitrefrepo/neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/api/procedure/CallableProcedure.java:33-37,55-103` | `CallableProcedure` | Public procedures expose a `ProcedureSignature` and execute as `apply(Context, AnyValue[], ResourceMonitor)`, with helper checks for optional vs required parameters and typed input validation. | Procedure compatibility is a typed ABI, not just a string-to-handler map. | v003 can route some procedure names to OLTP, some to OLAP, and some to deterministic unsupported handlers as long as the signature layer is preserved. | Neo4j-compatible API | Falsifier: client-visible procedure behavior can ignore typed parameter validation and still feel compatible. |
| `CLAIM-B05-007` | `REQ-LEARN-005.0` | source | `gitrefrepo/neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/procs/ProcedureSignature.java:42-60,170-180` | `ProcedureSignature` | Procedure signatures include name, input signature, output signature, mode, admin/deprecation/warning flags, eager/thread-safe flags, and supported query languages. | Compatibility requires preserving more than procedure names; metadata and mode semantics matter too. | Later batches may classify which of these fields must be exact vs acceptable to conservatively under-promise. | Neo4j-compatible API | Falsifier: target clients never observe or rely on signature metadata beyond the name. |
| `CLAIM-B05-008` | `REQ-LEARN-005.0` | source | `gitrefrepo/neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/impl/util/ValueUtils.java:70-147` | `ValueUtils.of` | Neo4j converts generic objects into `AnyValue`, including entities, maps, lists, paths, arrays, streams, and geometry, and throws on unsupported types. | Value marshalling is part of compatibility, especially for procedure arguments and results. | v003 may need a narrower first pass if some value classes are never reached by promised procedures, but it cannot ignore the boundary completely. | Neo4j-compatible API | Falsifier: all target procedures/results stay within primitive scalar types only. |
| `CLAIM-B05-009` | `REQ-LEARN-003.0` | source | `gitrefrepo/neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc:167-175,209-250` | `Auto-commit Transaction`, `Explicit Transaction` | Bolt auto-commit transactions are opened by `RUN` and closed by `PULL_ALL`/`DISCARD_ALL` or `PULL`/`DISCARD`, and can contain only one `RUN`; explicit transactions are opened by `BEGIN`, allow several `RUN`s, and close with `COMMIT` or `ROLLBACK` once streams are consumed. | Zero-app-change compatibility depends on transaction state-machine behavior, not only query parsing. | A transport proxy or alternate runtime can still be compatible if it preserves these state transitions exactly. | Neo4j-compatible API | Falsifier: official drivers tolerate materially different transaction-state behavior. |
| `CLAIM-B05-010` | `REQ-LEARN-003.0` | source | `gitrefrepo/neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc:329-379` | `RUN`, `BEGIN`, `ROUTE` message tables | Bolt request metadata includes bookmarks, `tx_timeout`, `tx_metadata`, `mode`, `db`, `imp_user`, and notification settings; `ROUTE` carries routing info plus database and impersonated user. | Compatibility includes database selection, impersonation, causal consistency, and routing hints on the wire. | v003 can route all writes to Neo4j-shaped OLTP while still honoring these wire parameters. | Neo4j-compatible API | Falsifier: important clients never send or rely on these fields. |
| `CLAIM-B05-011` | `REQ-LEARN-003.0` | source | `gitrefrepo/neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc:408-419,955-961,1054-1060` | `HELLO`, `GOODBYE`, `RESET` | `HELLO` is only valid in `CONNECTED` and should be sent immediately; `GOODBYE` terminates gracefully without a response; `RESET` jumps ahead in the queue, interrupts work, and causes queued messages in front of it to be ignored until reset is processed. | Bolt compatibility is not just message vocabulary; queueing and interruption semantics also matter. | Session recovery and cancellation behavior in v003 will need explicit tests, not just happy-path handshake tests. | Neo4j-compatible API | Falsifier: official drivers do not actually depend on reset/close queue semantics. |
| `CLAIM-B05-012` | `REQ-LEARN-003.0` | source | `gitrefrepo/neo4j-testkit-src/tests/stub/tx_begin_parameters/test_tx_begin_parameters.py:11-19,82-99,145-207` | `TestTxBeginParameters` | Testkit explicitly checks session/transaction wire parameters for read/write mode, bookmarks, tx metadata, timeout, database selection, and impersonated user. | Driver compatibility means these parameters are part of observable behavior, not optional garnish. | The exact error text may vary by driver, but the presence/absence semantics are fixed enough to test. | Neo4j-compatible API | Falsifier: these tests are obsolete and no longer exercised by official drivers. |
| `CLAIM-B05-013` | `REQ-LEARN-003.0` | source | `gitrefrepo/neo4j-testkit-src/tests/stub/idempotent_retries/test_idempotent_retries.py:12-18,66-86,131-178` | `TestIdempotentRetries` | Testkit expects idempotent auto-commit retries for certain errors, ensures telemetry is not resent on retry, and explicitly expects no retry for explicit transactions. | Retry semantics are part of compatibility and influence how v003 should surface OLTP vs OLAP errors to drivers. | Some retry policy could remain delegated to official drivers if the server exposes correct error classes. | Neo4j-compatible API | Falsifier: retry behavior is entirely driver-local and does not depend on server-classified errors. |
| `CLAIM-B05-014` | `REQ-LEARN-003.0` | source | `gitrefrepo/neo4j-testkit-src/tests/stub/bookmarks/test_bookmarks_v5.py:28-33,42-58,59-96,98-154` | `TestBookmarksV5` | Testkit expects sessions to accept bookmarks, committed transactions to update `last_bookmarks()`, and subsequent transactions to send and receive new bookmarks across read and write flows. | Bookmark behavior is part of the compatibility boundary and matters for causal chaining. | v003 can preserve causal semantics even if OLAP reads are snapshot-based, because OLTP remains the truth and snapshot freshness can be separately declared. | Neo4j-compatible API | Falsifier: target clients never chain bookmarks across sessions or transactions. |
| `CLAIM-B05-015` | `REQ-LEARN-022.0` | source | `gitrefrepo/neo4j-testkit-src/tests/stub/iteration/test_result_single.py:10-31,54-97,109-129` | `TestResultSingle` | Testkit expects `result.single()` to fail when zero or multiple rows are present, maps those failures to driver-specific exception classes, and verifies behavior under disconnect, pull failure, and retrying transaction functions. | Result cardinality and error-shape behavior are part of the client canary contract. | The exact exception class names differ by driver, but the semantic boundary is stable enough to test. | Neo4j-compatible API | Falsifier: result cardinality errors are treated as incidental and not relied on by clients. |
| `CLAIM-B05-016` | `REQ-LEARN-004.0` | source | `gitrefrepo/opencypher-src/README.adoc:5-18`, `grammar/README.adoc:1-14` | `openCypher repository purpose`, `The Cypher Grammar` | openCypher positions itself as the specification of the property graph query language, with grammar in ISO WG3 BNF notation and evolution toward ISO/IEC 39075 GQL. | Cypher compatibility cannot be reduced to “Neo4j-specific parser quirks”; there is also a grammar/spec and GQL-adjacent obligation. | Neo4j may still diverge in places, so compatibility needs both first-party Neo4j and openCypher evidence. | Neo4j-compatible API | Falsifier: Neo4j behavior of interest is entirely outside the openCypher/TCK scope. |
| `CLAIM-B05-017` | `REQ-LEARN-004.0` | source | `gitrefrepo/opencypher-src/tck/README.adoc:4-12,29-52,100-160`, `tck/index.adoc:9-20` | `Cypher TCK`, `Call1 - Basic procedure calling` | The openCypher TCK models initial graph state, query text, expected results, side effects, and expected errors, and its index includes a `Call` category for procedure calling. | Cypher compatibility includes query results, side effects, and runtime errors, not just parsing acceptance. | A later v003 conformance lane could map subsets of the TCK to promised support levels. | Neo4j-compatible API | Falsifier: the TCK omits the parts of Cypher that matter to the target application set. |
| `CLAIM-B05-018` | `REQ-LEARN-004.0` | source | `gitrefrepo/opencypher-src/cip/1.adopted/CIP2015-06-24-call-procedures.adoc:32-66,74-112,127-146,150-217` | `Calling Procedures` CIP | The adopted procedure-calling CIP defines named typed arguments, typed result fields, `CALL`, `YIELD`, standalone vs in-statement calls, implicit parameter passing for standalone calls, and cardinality effects when `YIELD` is omitted. | Procedure compatibility spans both the server procedure ABI and Cypher call semantics. | v003 can stage support, but it cannot claim CALL compatibility while ignoring `YIELD`, cardinality, or standalone/in-statement distinctions. | Neo4j-compatible API | Falsifier: Neo4j’s live procedure semantics diverge so much from the CIP that the CIP is not useful. |
| `CLAIM-B05-019` | `REQ-LEARN-004.0` | source | `gitrefrepo/neo4j-src/community/cypher/pom.xml:36-49` | `compatibility-spec-suite`, `runtime-spec-suite` | Neo4j’s Cypher tree includes both `compatibility-spec-suite` and `runtime-spec-suite` modules. | First-party Neo4j itself treats Cypher compatibility and runtime behavior as explicit test surfaces. | A later deeper pass could mine these suites for concrete edge cases that matter most to v003. | Neo4j-compatible API | Falsifier: those modules are dead scaffolding rather than active compatibility machinery. |
| `CLAIM-B05-020` | `REQ-LEARN-021.0` | command | `rg '@Procedure...' over neo4j-apoc-src and neo4j-apoc-procedures-src` | `APOC count = 241` | The local APOC shelves expose `241` visible `apoc.*` procedures/functions in aggregate, spanning refactor, convert, math, atomic, cypher, and many other categories. | APOC is too large to promise wholesale support casually; it needs a tiered boundary. | A later pass can classify core-vs-adjacent APOC families by real user value and storage impact. | Neo4j-compatible API | Falsifier: a canonical-name pass shows the practical APOC burden is tiny after de-duplication and deprecation filtering. |
| `CLAIM-B05-021` | `REQ-LEARN-021.0` | source | `gitrefrepo/neo4j-apoc-src/core/src/main/java/apoc/refactor/GraphRefactoring.java:64-417`, `.../apoc/atomic/Atomic.java:55-269`, `.../apoc/cypher/Cypher.java:92-191` | `apoc.refactor.*`, `apoc.atomic.*`, `apoc.cypher.*` | APOC includes write-heavy graph refactoring procedures, atomic property mutation helpers, and embedded Cypher execution helpers. | APOC support is not just “more functions”; many procedures touch transactional semantics directly. | v003 may need to draw a clear line between APOC read helpers, write helpers, and unsupported classes. | Neo4j-compatible API | Falsifier: target applications only use a tiny read-only APOC subset. |
| `CLAIM-B05-022` | `REQ-LEARN-020.0` | source | `gitrefrepo/neo4j-gds-client-src/README.md:35-74` | `GraphDataScience`, `gds.pageRank.mutate`, pipeline train/predict` | The GDS Python client’s basic flow is: connect, load/project a graph, run `gds.pageRank.mutate`, build a pipeline, train a model, then stream predictions. | Real GDS client workflows exercise projection, algorithm modes, model artifacts, and prediction APIs together. | v003 can stage which of these are implemented vs registered-compatible, but they are real user-visible flows. | Complete GDS surface | Falsifier: these example flows are purely marketing and not representative of actual client behavior. |
| `CLAIM-B05-023` | `REQ-LEARN-020.0` | source | `gitrefrepo/gds-agent-src/README.md:50-63`, `gitrefrepo/gds-agent-src/mcp_server/README.md:36-47` | `gds.session.list`, `gds.graph.project.remote` | `gds-agent` detects GDS plugin vs Aura session mode by running `gds.session.list()`, and in session mode graph projection falls back to `gds.graph.project.remote`. | GDS compatibility includes both on-prem plugin-style projection and session/remote projection workflows. | v003 may not need Aura session mode immediately, but it should understand that some clients already branch on it. | Complete GDS surface | Falsifier: session-mode workflows are irrelevant to the targeted v003 deployment modes. |
| `CLAIM-B05-024` | `REQ-LEARN-022.0` | source | `gitrefrepo/neo4j-python-driver-src/src/neo4j/_sync/driver.py:665-718,821-879,958-979`, `.../exceptions.py:949-987` | `Driver.execute_query`, `ResultNotSingleError`, `execute_query_bookmark_manager` | The official Python driver exposes `execute_query` as a session/transaction-function wrapper with retries, routing, database and impersonated-user options, idempotent result transformers, and causal chaining through a bookmark manager; it also exposes `ResultNotSingleError` and `SessionExpired` as typed errors. | “Zero client-side changes” includes high-level convenience APIs, not only low-level `session.run`. | Some high-level helpers may be emulated over a narrower core server ABI, but the behavioral surface still matters. | Neo4j-compatible API | Falsifier: target applications never use `execute_query` or bookmark-manager chaining. |
| `CLAIM-B05-025` | `REQ-LEARN-022.0` | source | `gitrefrepo/neo4j-java-driver-src/examples/src/main/java/org/neo4j/docs/driver/HelloWorldExample.java:42-50`, `.../AutocommitTransactionExample.java:27-30`, `.../driver/src/main/java/org/neo4j/driver/Transaction.java:24-95` | `session.executeWrite`, `session.run`, `Transaction.commit/rollback/close` | The official Java driver examples and interfaces use autocommit `session.run`, transaction functions `session.executeWrite`, `result.single()`, and explicit transactions that roll back by default unless `commit()` is called. | Transaction and result behavior are stable enough to show up in official examples and public APIs. | Driver language differences can vary, but the transaction model is consistent across the official ecosystem. | Neo4j-compatible API | Falsifier: later official driver versions replace this model completely. |
| `CLAIM-B05-026` | `REQ-LEARN-022.0` | source | `gitrefrepo/neo4rs-src/lib/src/lib.rs:1-6,31-40,67-89,133-163`, `.../routing/connection_registry.rs:23-39,64-104,169-194`, `.../errors.rs:27-34,61-77,88-116,166-220` | `neo4rs` docs, routing registry, error classification | The community Rust driver implements Bolt, documents database/pool/fetch-size config, maps `neo4j://` schemes to `bolt://` pools for routing, supports bookmarks behind a feature flag, and classifies retryable/session-expired/auth errors. | Community drivers are a useful canary for whether v003 is preserving the broad driver ecosystem shape, not only official drivers. | Full community-driver compatibility may lag, but gross mismatches are a warning sign. | Neo4j-compatible API | Falsifier: community drivers are niche enough to ignore completely. |
| `CLAIM-B05-027` | `REQ-LEARN-022.0` | source | `gitrefrepo/cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/ConnectionConfig.java:106-122` | `driverUrl()`, `database()` | `cypher-shell` builds a driver URL from scheme, host, and port and carries an explicit database field in connection config. | Even the CLI ecosystem expects driver-style connection semantics and database selection. | A shell-specific behavior pass could later add auth, encryption, and error UX edges. | Neo4j-compatible API | Falsifier: the shell surface diverges so much from drivers that this signal is not representative. |

## Compatibility Boundary Synthesis

### 1. What must be preserved exactly

- Bolt session and transaction state semantics.
- Database selection, bookmarks, tx metadata, timeout, routing mode, and
  impersonation fields on the wire.
- Cypher `CALL`/`YIELD` behavior, procedure signatures, typed values, and
  cardinality/error semantics.
- Result-shape behaviors that drivers and testkit already assert, including
  `single()` failure shapes and retry-related error surfaces.

### 2. What can remain implementation freedom

- OLAP physical storage can still be published snapshots rather than Neo4j
  record chains.
- GDS kernels can run over published CSR-like snapshots as long as the front
  surface still behaves like the expected `gds.*` procedures and workflows.
- Some procedures can be `UnsupportedButRegistered` first, as long as the
  surface is inventoried and the failure mode is deterministic.

### 3. What should not be over-promised

- APOC full support. The local shelves already show too much breadth to claim
  casually.
- Aura/session-mode GDS parity as an initial requirement.
- Community-driver perfection on day one.

## Architecture Fit Matrix

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Bolt core session and transaction behavior` | none directly | none | none | none | none | metadata-sized | front-door protocol state machine over OLTP and OLAP routing | `NeedsArchitectureSpike` | official drivers or testkit succeed against materially different queue/state behavior |
| `Cypher CALL, YIELD, and procedure signatures` | none directly | value and result marshalling tables | none | none | none | metadata-sized | typed procedure registry plus value conversion layer | `NeedsArchitectureSpike` | target clients never rely on typed procedure/value semantics |
| `Neo4j-shaped OLTP truth` | record-store chain/group traversal and property records | none | WAL, locks, indexes, tx machinery later | none | transactional state only | OLTP memory, not OLAP memory | keep OLTP separate from published OLAP snapshots | `P0-RegisteredCompatible` | OLAP must read directly from record chains to stay compatible |
| `GDS user workflow compatibility` | published graph projection identity plus algorithm entrypoints | labels/types/weights/properties/results/models | projection/build metadata | named graph generations | family-dependent | can be dominated by algorithm state | GDS procedures route to published snapshots and sidecars | `NeedsArchitectureSpike` | real client workflows rely only on a tiny algorithm subset and no catalog/model semantics |
| `APOC compatibility boundary` | mixed | mixed | mixed | mixed | mixed | mixed | tiered support matrix: exact, later, unsupported-but-registered | `NeedsArchitectureSpike` | actual user requirements involve only a tiny APOC subset |
| `Driver and ecosystem canaries` | none directly | none directly | none | none | none | metadata-sized | preserve driver-facing behavior at the wire and result surfaces | `P0-RegisteredCompatible` | official drivers stop reflecting the compatibility burden that matters to v003 |

## PRD Outcome Traceability Dossier

| PRD outcome | supporting claims | current confidence | next experiment or evidence spike |
| --- | --- | --- | --- |
| `Neo4j-compatible API` | `CLAIM-B05-006` through `CLAIM-B05-027` | `high` for boundary identification, `low-medium` for full implementation | route one narrow end-to-end slice through Bolt -> Cypher CALL -> procedure/value marshalling -> result behavior |
| `Neo4j-shaped OLTP` | `CLAIM-B05-001` through `CLAIM-B05-005` | `high` for the shape boundary | add WAL, lock, and index-path evidence in a later OLTP-deepening pass |
| `Complete GDS surface` | `CLAIM-B05-022`, `CLAIM-B05-023` plus Batch 02 inventory | `medium` | continue with sidecars/planner and later algorithm-family batches before making storage sufficiency claims |
| `Published OLAP snapshots` | `CLAIM-B05-022`, `CLAIM-B05-023` | `medium` | prove that named graph/project/list/drop flows map cleanly onto generationed snapshot publication |
| `Strict holistic RAM` | `CLAIM-B05-004`, `CLAIM-B05-022`, `CLAIM-B05-023` | `low-medium` | tie the compatibility boundary to sidecar and algorithm-state evidence in Batch 06 and Batch 07 |

## Rejected-Alternative Note

Rejected for this batch:

- `Claim Neo4j compatibility by matching only the Cypher syntax and a subset of GDS algorithms.`

Why rejected:

- Bolt state and metadata fields are part of the public contract.
- Procedure signatures and `AnyValue` marshalling are part of the public
  contract.
- Testkit asserts bookmarks, retries, result cardinality, and transaction
  behaviors across official drivers.
- Real GDS clients and agents already exercise projection, mutate, model, and
  remote-session flows.
- APOC breadth is large enough that ignoring it entirely is misleading.

What would overturn this rejection:

- A sharply narrower product claim explicitly abandons zero-client-change
  compatibility and names the reduced supported surface in advance.

## Skeptical Review

| challenge | response |
| --- | --- |
| Aren’t you making the compatibility burden impossibly large? | No. The point of this batch is to bound the burden truthfully, not to promise every part will land at once. |
| Does this mean OLAP cannot diverge from Neo4j internals? | No. The batch separates front-door compatibility from back-end OLAP storage freedom. |
| Is APOC now a blocker for all progress? | No. It is a classification problem, not a mandate for full immediate support. |
| Are official drivers enough without testkit? | No. Testkit is the stronger behavioral oracle because it codifies cross-driver expectations. |
| Are you overusing openCypher when Neo4j has its own behavior? | No. This batch uses openCypher as a grammar/TCK boundary and Neo4j first-party code/docs as the implementation boundary. |

## Verification Commands Run

```bash
nl -ba gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java | sed -n '28,120p'
nl -ba gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java | sed -n '28,120p'
nl -ba gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java | sed -n '30,120p'
nl -ba gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java | sed -n '70,220p'
nl -ba gitrefrepo/neo4j-src/community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java | sed -n '680,690p'
nl -ba gitrefrepo/neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/api/procedure/CallableProcedure.java | sed -n '28,120p'
nl -ba gitrefrepo/neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/procs/ProcedureSignature.java | sed -n '40,180p'
nl -ba gitrefrepo/neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/impl/util/ValueUtils.java | sed -n '66,180p'
nl -ba gitrefrepo/neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc | sed -n '165,260p'
nl -ba gitrefrepo/neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc | sed -n '329,420p'
nl -ba gitrefrepo/neo4j-docs-bolt-src/modules/ROOT/pages/bolt/message.adoc | sed -n '950,1065p'
nl -ba gitrefrepo/neo4j-testkit-src/tests/stub/tx_begin_parameters/test_tx_begin_parameters.py | sed -n '1,240p'
nl -ba gitrefrepo/neo4j-testkit-src/tests/stub/idempotent_retries/test_idempotent_retries.py | sed -n '1,260p'
nl -ba gitrefrepo/neo4j-testkit-src/tests/stub/bookmarks/test_bookmarks_v5.py | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-testkit-src/tests/stub/iteration/test_result_single.py | sed -n '1,260p'
nl -ba gitrefrepo/opencypher-src/README.adoc | sed -n '1,120p'
nl -ba gitrefrepo/opencypher-src/grammar/README.adoc | sed -n '1,120p'
nl -ba gitrefrepo/opencypher-src/tck/README.adoc | sed -n '1,160p'
nl -ba gitrefrepo/opencypher-src/tck/index.adoc | sed -n '1,120p'
nl -ba gitrefrepo/opencypher-src/cip/1.adopted/CIP2015-06-24-call-procedures.adoc | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-src/community/cypher/pom.xml | sed -n '36,60p'
rg -n "@Procedure\\(value = \\\"apoc\\.|@Procedure\\(name = \\\"apoc\\.|@UserFunction\\(value = \\\"apoc\\.|@UserFunction\\(name = \\\"apoc\\.\"" gitrefrepo/neo4j-apoc-src gitrefrepo/neo4j-apoc-procedures-src -g '*.java' | wc -l
nl -ba gitrefrepo/neo4j-gds-client-src/README.md | sed -n '30,80p'
nl -ba gitrefrepo/gds-agent-src/README.md | sed -n '40,110p'
nl -ba gitrefrepo/gds-agent-src/mcp_server/README.md | sed -n '30,90p'
nl -ba gitrefrepo/neo4j-python-driver-src/src/neo4j/_sync/driver.py | sed -n '620,980p'
nl -ba gitrefrepo/neo4j-python-driver-src/src/neo4j/exceptions.py | sed -n '940,990p'
nl -ba gitrefrepo/neo4j-java-driver-src/examples/src/main/java/org/neo4j/docs/driver/HelloWorldExample.java | sed -n '35,80p'
nl -ba gitrefrepo/neo4j-java-driver-src/examples/src/main/java/org/neo4j/docs/driver/AutocommitTransactionExample.java | sed -n '20,80p'
nl -ba gitrefrepo/neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Transaction.java | sed -n '24,110p'
nl -ba gitrefrepo/neo4rs-src/lib/src/lib.rs | sed -n '1,220p'
nl -ba gitrefrepo/neo4rs-src/lib/src/routing/connection_registry.rs | sed -n '1,220p'
nl -ba gitrefrepo/neo4rs-src/lib/src/errors.rs | sed -n '1,220p'
nl -ba gitrefrepo/cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/ConnectionConfig.java | sed -n '96,132p'
```

## Checkpoint: surface+capability+rejection / neo4j-compatibility-boundary / 2026-06-24

Assigned requirement IDs:

- `REQ-LEARN-002.0`
- `REQ-LEARN-003.0`
- `REQ-LEARN-004.0`
- `REQ-LEARN-005.0`
- `REQ-LEARN-020.0`
- `REQ-LEARN-021.0`
- `REQ-LEARN-022.0`
- `REQ-LEARN-016.0`
- `REQ-LEARN-017.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-040.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-053.0`

Evidence rows completed:

- `27`

Most important sourced facts:

- Neo4j OLTP is undeniably record-store and chain/group traversal shaped.
- Bolt compatibility includes transaction state transitions, routing/database
  metadata, impersonation, bookmarks, `RESET`, and graceful `GOODBYE`.
- Procedure compatibility includes typed signatures and `AnyValue`
  conversions, not just procedure names.
- Testkit codifies bookmarks, retries, transaction parameters, and
  result-cardinality behavior across official drivers.
- Real GDS clients already rely on graph projection, mutate modes, model flows,
  and sometimes remote/session projection.
- APOC is broad enough that support must be tiered, not casually promised.

Architecture implications:

- `Adopt`: keep Neo4j-shaped OLTP as the front-door truth and compatibility
  anchor.
- `Adopt`: treat Bolt, Cypher CALL/YIELD, procedure signatures, and value
  marshalling as ABI layers above storage.
- `Adapt`: route GDS procedures to published OLAP snapshots while preserving
  the public procedure/workflow contract.
- `Reject`: “Cypher parser plus a few algorithms” as a sufficient definition of
  Neo4j compatibility.
- `Watch`: APOC classification, session-mode GDS workflows, and high-level
  driver helpers such as `execute_query`.

Unresolved risks:

- `Risk`: the remaining sidecar/planner and algorithm-family batches may show
  that some compatibility flows require more persisted semantics than expected.
  `Falsifier`: Batch 06 and Batch 07 show that flat CSR plus sidecars cannot
  support a required visible flow without a deeper storage change.
- `Risk`: APOC user demand may be concentrated in a write-heavy subset that is
  harder than the core Neo4j+GDS boundary.
  `Falsifier`: later repo or user evidence shows only a small read-mostly APOC
  subset matters for PMF.

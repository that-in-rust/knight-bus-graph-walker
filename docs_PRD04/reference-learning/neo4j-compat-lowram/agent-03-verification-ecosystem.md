# Agent 03: Compatibility Verification Ecosystem and Adoption Contract

## Document status

| Field | Value |
|---|---|
| North star | `docs_PRD04/A007-spc-founder-interview-prep-v7.md` |
| Assigned scope | openCypher, TestKit, five official drivers, neo4rs, Browser, and OGM only |
| Evidence table | `evidence/agent-03-files.tsv` |
| Denominator | `evidence/all-files-denominator.tsv`, filtered to `assigned_agent=agent-03` |
| Assigned tracked files | 7,202 |
| Assigned tracked bytes | 54,914,374 bytes (52.37 MiB) |
| Direct-read files | 533 |
| Direct-read bytes | 3,879,079 bytes (3.70 MiB) |
| Structural graph files | 4,761 |
| Evidence validator | `scripts/validate_neo4j_family_evidence.py` |
| Validator result | PASS, 32,262 rows reconciled across all agents |
| Research cutoff | The exact local commits recorded below, not current upstream state |

## Executive conclusion

The compatibility strategy should not be "implement Neo4j." It should be:

> Let a security or dependency team replay a small, declared profile of its production Cypher query text and parameters through an official Neo4j driver, while Knight Bus enforces a hard memory budget, chooses fit/spill/approximate/refuse, and returns both compatible results and a proof receipt.

The evidence supports this as technically plausible, but only under an explicit compatibility profile. It does not support claiming general Cypher, general Neo4j, general Bolt-server, Browser, OGM, or transactional parity.

Compatibility is a ceremony reducer. The enforceable budget and receipt are the product. A Bolt endpoint that accepts familiar query text but cannot prove the memory ceiling is only a protocol clone. A low-memory executor that requires users to rewrite every query, remap every type, and replace every client integration may fail the adoption test. The wedge needs both, in that order of priority:

1. A bounded runner that can prove what it will consume.
2. A narrow compatibility bridge for the exact customer queries that matter.
3. A differential verification loop that makes every compatibility claim falsifiable.

The proposed first profile is deliberately small:

- Read-only, parameterized security/dependency/access-path queries.
- Official Python and Java drivers first, unless founder interviews identify another dominant driver.
- Direct Bolt connection first, with a single-node routing facade only when production configuration requires `neo4j://`.
- Auto-commit reads and managed read transactions.
- Database selection, access mode, basic/bearer auth acceptance, bounded `PULL`, cancellation, records, summaries, errors, and notifications.
- Exact `fit`, exact `spill`, explicit-opt-in `approximate`, and pre-execution `refuse` outcomes.
- No claim of write, cluster, administration, procedure ecosystem, Browser, OGM, or full dialect parity.

## A007 alignment

### What A007 says the product is

A007 makes the security/dependency/access-path team the first ICP. Those teams already possess graph-shaped artifacts such as SBOMs, package dependency graphs, IAM graphs, service maps, attack paths, and code dependency graphs. Their pain is not abstract graph theory. It is the inability to answer useful questions under a known machine budget with a trustworthy operational explanation.

The adoption contract therefore has four parts:

| Contract | Required behavior |
|---|---|
| Portable artifact | A versioned, checksummed graph artifact can move between developer machine, CI, and controlled compute. |
| Low ceremony | A production query can be replayed with unchanged query text and parameters whenever it belongs to the declared profile. |
| Hard budget | The runner enforces the memory ceiling rather than merely printing an estimate. |
| Receipt | The runner reports the estimate, decision, observed high-water mark, result identity, and approximation/spill facts. |

### What compatibility is not allowed to become

- It is not permission to build the whole Neo4j surface before customer proof.
- It is not permission to claim full Cypher from passing openCypher TCK scenarios.
- It is not permission to claim Bolt compatibility from completing a handshake.
- It is not permission to claim application compatibility from one official driver.
- It is not permission to accept a query and then breach the requested memory ceiling.
- It is not permission to silently approximate, reorder ordered results, or erase error semantics.
- It is not permission to ingest GPL code into a differently licensed product without a deliberate legal strategy.

## Evidence method

### Repository snapshots

| Repository | Commit | Commit date | Tracked files | Role in this dossier |
|---|---|---:|---:|---|
| `opencypher-src` | `677cbafabb8c3c5eed458fd3b1ec0daec8d67d23` | 2026-03-20 | 262 | Grammar and semantic TCK oracle |
| `neo4j-testkit-src` | `ec46b6562cf5ab51d97c199f55186d55149b1801` | 2026-05-21 | 1,063 | Driver and Bolt transcript oracle |
| `neo4j-java-driver-src` | `7652d3c3f6d7faa34e9b724e7cc02419425f3575` | 2026-05-01 | 962 | Official Java observable contract |
| `neo4j-go-driver-src` | `c872010d48a614fc2a218832a5841e9afa73c8f6` | 2026-05-12 | 232 | Official Go observable contract |
| `neo4j-python-driver-src` | `9e23c904965c79f5d4dec4ebce0e012576597ba6` | 2026-05-12 | 504 | Official Python observable contract |
| `neo4j-javascript-driver-src` | `d88417126800a8d92062f42d474b1fbddc472742` | 2026-05-12 | 797 | Official JavaScript observable contract |
| `neo4j-dotnet-driver-src` | `261a8250ee7184dfbe362581bd5a991e8a6ea79e` | 2026-05-22 | 969 | Official .NET observable contract |
| `neo4rs-src` | `19f244ae7800ac084f0679e89c2606b799be7538` | 2026-06-11 | 155 | Rust client implementation reference, not an official oracle |
| `neo4j-browser-src` | `ff8ed858f50b095d00c0fba725487f192900b964` | 2025-07-29 | 1,032 | Interactive migration and result-shape reference |
| `neo4j-ogm-src` | `eeee0bcae17ee07928d10ad60cc1ac888fc04707` | 2026-06-23 | 1,226 | Object-mapping migration expectations |

### Code graph procedure

The local skill `.agents/skills/code-graph-mcp/SKILL.md` was followed with `@sdsrs/code-graph` version `0.114.1`. For every assigned repository, the investigation used:

1. `health-check` to verify the local SQLite graph and FTS integrity.
2. `map` to identify modules, dependencies, entry points, and hot areas.
3. `tour` to obtain dependency-ordered reading sequences.
4. `search` for driver, session, transaction, result, auth, routing, notification, GQL error, version negotiation, and TestKit concepts.
5. `show --refs --impact` on representative execution entry points.
6. `deps` on critical protocol and public API files.
7. `impact` on representative execution symbols.

The high-impact result for Go `ExecuteQuery` was expected: it had 2 direct and 12 total callers, mostly tests. Other sampled public execution symbols were low structural impact, but several tools reported ambiguous references. These results guide reading; they do not prove runtime behavior.

### Code graph health snapshot

| Repository | Indexed files | Nodes | Edges | Pending/unresolved references | Integrity note |
|---|---:|---:|---:|---:|---|
| openCypher | 3 | 3 | 0 | 2 | Grammar and Gherkin are largely outside AST coverage |
| TestKit | 255 | 5,179 | 21,064 | 4,416 | Healthy |
| Java driver | 889 | 8,627 | 95,085 | 8,098 | Healthy |
| Go driver | 216 | 2,446 | 15,353 | 2,135 | Healthy |
| Python driver | 444 | 6,439 | 28,001 | 5,899 | Healthy |
| JavaScript driver | 623 | 5,821 | 21,869 | 6,391 | Healthy |
| .NET driver | 892 | 8,045 | 50,788 | 11,003 | Healthy |
| neo4rs | 138 | 2,986 | 14,979 | 1,352 | Healthy |
| Browser | 720 | 4,053 | 12,459 | 5,557 | Healthy |
| OGM | 1,129 | 9,027 | 123,947 | 6,178 | Healthy; deep quick-check skipped because of index size |

All indexes reported zero FTS drift and zero orphan vectors. Pending/unresolved graph references are parser/index limitations and ambiguous symbols, not missing TSV rows. Critical behavior was upgraded to direct read.

### Exhaustive file accounting

| Repository | Rows | Bytes | Direct read | Direct bytes | Graph indexed | Non-code | Generated | Binary |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `neo4j-browser-src` | 1,032 | 22,341,446 | 11 | 181,563 | 700 | 150 | 20 | 151 |
| `neo4j-dotnet-driver-src` | 969 | 4,206,077 | 33 | 249,655 | 850 | 84 | 0 | 2 |
| `neo4j-go-driver-src` | 232 | 1,656,640 | 27 | 251,466 | 186 | 19 | 0 | 0 |
| `neo4j-java-driver-src` | 962 | 3,777,404 | 203 | 594,042 | 702 | 56 | 0 | 1 |
| `neo4j-javascript-driver-src` | 797 | 5,267,796 | 154 | 1,168,605 | 439 | 182 | 22 | 0 |
| `neo4j-ogm-src` | 1,226 | 5,495,114 | 11 | 118,692 | 1,133 | 74 | 0 | 8 |
| `neo4j-python-driver-src` | 504 | 4,639,732 | 42 | 487,174 | 399 | 56 | 1 | 6 |
| `neo4j-testkit-src` | 1,063 | 2,793,696 | 29 | 586,299 | 223 | 810 | 1 | 0 |
| `neo4rs-src` | 155 | 1,065,657 | 12 | 145,388 | 125 | 18 | 0 | 0 |
| `opencypher-src` | 262 | 3,670,812 | 11 | 96,195 | 4 | 245 | 0 | 2 |
| **Total** | **7,202** | **54,914,374** | **533** | **3,879,079** | **4,761** | **1,694** | **44** | **170** |

Direct reads comprise 252 production source files, 245 test source files, 15 provenance files, 14 documentation files, 4 TCK scenarios, 1 BoltStub script, 1 configuration file, and 1 grammar file. The large direct-read count is intentional: the shared validator conservatively promotes source paths matching TestKit/TCK/Bolt/parser/algorithm-critical patterns.

## Exact critical paths

The TSV is the exhaustive source of truth. Filtering `coverage_status=direct_read` returns all 533 exact paths, blobs, sizes, and globally unique evidence IDs. The following paths are the minimum conceptual spine.

### Grammar and language semantics

| Evidence | Path | Why critical |
|---|---|---|
| `A03-006971` | `opencypher-src/grammar/openCypher.bnf` | Grammar boundary for accepted query text |
| `A03-006975` | `opencypher-src/tck/README.adoc` | TCK execution and assertion semantics |
| `A03-007000` | `opencypher-src/tck/features/clauses/match/Match1.feature` | Representative read-query scenarios |
| `A03-006946` | `opencypher-src/LICENSE` | Apache terms |
| `A03-006947` | `opencypher-src/NOTICE` | Naming and trademark caveat |

### Bolt and TestKit

| Evidence | Path | Why critical |
|---|---|---|
| `A03-005733` | `neo4j-testkit-src/boltstub/bolt_protocol.py` | Python Bolt version/message oracle |
| `A03-005786` | `neo4j-testkit-src/boltstub_rs/src/bolt_version.rs` | Rust Bolt/JOLT/version model |
| `A03-005797` | `neo4j-testkit-src/boltstub_rs/src/net_actor/handshake.rs` | Negotiation and manifest handshake lifecycle |
| `A03-005846` | `neo4j-testkit-src/nutkit/frontend/driver.py` | Driver/session construction contract |
| `A03-005854` | `neo4j-testkit-src/nutkit/protocol/feature.py` | Capability declaration vocabulary |
| `A03-005855` | `neo4j-testkit-src/nutkit/protocol/requests.py` | TestKit request protocol |
| `A03-005856` | `neo4j-testkit-src/nutkit/protocol/responses.py` | TestKit response protocol |
| `A03-006676` | `neo4j-testkit-src/tests/stub/summary/test_summary.py` | Summary, plan, notification, timing behavior |
| `A03-006741` | `neo4j-testkit-src/tests/stub/versions/test_versions.py` | Negotiated-version contract |

### Official driver public contracts

| Language | Evidence spine |
|---|---|
| Java | `A03-002317` `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Driver.java`; `A03-002336` `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Session.java`; `A03-002332` `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Result.java`; `A03-002620` `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/summary/ResultSummary.java`; `A03-002360` `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/exceptions/Neo4jException.java` |
| Go | `A03-002056` `neo4j-go-driver-src/neo4j/driver.go`; `A03-002182` `neo4j-go-driver-src/neo4j/session.go`; `A03-002177` `neo4j-go-driver-src/neo4j/result.go`; `A03-002180` `neo4j-go-driver-src/neo4j/resultsummary.go`; `A03-002038` `neo4j-go-driver-src/neo4j/db/errors.go` |
| Python | `A03-005412` `neo4j-python-driver-src/src/neo4j/_sync/driver.py`; `A03-005425` `neo4j-python-driver-src/src/neo4j/_sync/work/session.py`; `A03-005424` `neo4j-python-driver-src/src/neo4j/_sync/work/result.py`; `A03-005435` `neo4j-python-driver-src/src/neo4j/_work/summary.py`; `A03-005440` `neo4j-python-driver-src/src/neo4j/exceptions.py` |
| JavaScript | `A03-003405` `neo4j-javascript-driver-src/packages/core/src/driver.ts`; `A03-003453` `neo4j-javascript-driver-src/packages/core/src/session.ts`; `A03-003452` `neo4j-javascript-driver-src/packages/core/src/result.ts`; `A03-003450` `neo4j-javascript-driver-src/packages/core/src/result-summary.ts`; `A03-003406` `neo4j-javascript-driver-src/packages/core/src/error.ts` |
| .NET | `A03-001902` `neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Public/IDriver.cs`; `A03-001898` `neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Public/IAsyncSession.cs`; `A03-001905` `neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Public/IResultCursor.cs`; `A03-001950` `neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Public/Summary/IResultSummary.cs`; `A03-001874` `neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Public/Exceptions/Neo4jException.cs` |

### Rust, Browser, and OGM migration references

| Evidence | Path | Why critical |
|---|---|---|
| `A03-006863` | `neo4rs-src/lib/src/graph.rs` | Rust async query, retry, pool, and result-stream reference |
| `A03-006860` | `neo4rs-src/lib/src/connection.rs` | Rust Bolt handshake and chunk framing reference |
| `A03-006935` | `neo4rs-src/lib/src/version.rs` | Important limitation: stable path advertises only Bolt 4.x variants |
| `A03-000895` | `neo4j-browser-src/src/shared/modules/commands/helpers/cypher.ts` | Interactive query and parameter handoff |
| `A03-000989` | `neo4j-browser-src/src/shared/services/bolt/transactions.ts` | Read/write routing, auto-commit, cancellation, and session closure |
| `A03-000975` | `neo4j-browser-src/src/shared/services/bolt/boltConnection.ts` | Connectivity and multi-database expectations |
| `A03-000980` | `neo4j-browser-src/src/shared/services/bolt/boltMappings.ts` | Graph/result type presentation expectations |
| `A03-004025` | `neo4j-ogm-src/api/src/main/java/org/neo4j/ogm/driver/Driver.java` | OGM driver boundary |
| `A03-004256` | `neo4j-ogm-src/core/src/main/java/org/neo4j/ogm/session/Session.java` | Object loading, saving, query, cache, and transaction surface |
| `A03-004255` | `neo4j-ogm-src/core/src/main/java/org/neo4j/ogm/session/Neo4jSession.java` | Session implementation and transaction orchestration |
| `A03-004106` | `neo4j-ogm-src/bolt-driver/src/main/java/org/neo4j/ogm/drivers/bolt/request/BoltRequest.java` | OGM-to-Bolt query/result adapter |
| `A03-004116` | `neo4j-ogm-src/bolt-driver/src/main/java/org/neo4j/ogm/drivers/bolt/transaction/BoltTransaction.java` | Native transaction and bookmark expectations |

## What openCypher proves, and what it does not

### Quantitative surface

The local openCypher snapshot contains:

- 1,533 lines in `grammar/openCypher.bnf`.
- 377 grammar production definitions.
- 220 TCK `.feature` files.
- 1,615 scenario or scenario-outline declarations.
- 276 examples blocks.
- 1,424 side-effect assertions.
- 87 ordered-result assertions.
- 1,259 unordered-result assertions.
- 89 empty-result assertions.
- 162 compile-error scenarios and 27 runtime-error scenarios.

The grammar includes query/update statements, `CALL`/`YIELD`, `RETURN`, patterns, quantified patterns, path-search prefixes, labels, expressions, subqueries, comprehensions, literals, and tokens. The snapshot is evolving toward ISO GQL and its latest commit explicitly mentions adding `SHORTEST` to the grammar.

### What a TCK pass can justify

A TCK pass can justify a claim such as:

> For the named TCK scenarios at the pinned openCypher commit, Knight Bus produced the expected rows, errors, and side-effect observations under the TCK fixture model.

It cannot justify:

- Full Neo4j Cypher 5 or Cypher 25 compatibility.
- Bolt or PackStream compatibility.
- Driver lifecycle compatibility.
- Auth, routing, bookmark, retry, cancellation, or notification behavior.
- Correct physical plans.
- Hard-budget enforcement.
- Performance, memory, spill, approximation, or receipt claims.
- Compatibility with APOC, GDS procedures, plugins, administration, or cluster behavior.

TCK result ordering is especially important. Unless a scenario requires order, results are compared as unordered collections. A differential harness must not accidentally turn an ordered production query into an unordered comparison, nor compare an unordered query by raw arrival order.

### Product implication

Use the openCypher grammar and TCK as a semantic corpus, not as the product boundary. The product boundary is a smaller, versioned `KnightBusCypherProfile` derived from customer queries. Each accepted grammar construct must map to an executable bounded plan or to a deterministic pre-execution refusal.

## What TestKit and BoltStub prove

### Protocol versions and message lifecycle

The Rust BoltStub version model contains Bolt 1, 2, 3, 4.0 through 4.4, 5.0 through 5.8, and 6.0. Its JOLT mapping is:

- JOLT v1 for Bolt 1 through 4.4.
- JOLT v2 for Bolt 5.0 through 5.8.
- JOLT v3 for Bolt 6.0.

The message progression visible in the version model is:

- Legacy `INIT` and `ACK_FAILURE` before Bolt 3.
- `HELLO`, `GOODBYE`, and `RESET` from the modern lifecycle.
- `RUN`, `PULL`/`PULL_ALL`, and `DISCARD`/`DISCARD_ALL`.
- `BEGIN`, `COMMIT`, and `ROLLBACK`.
- `ROUTE` from Bolt 4.3.
- `LOGON` and `LOGOFF` from Bolt 5.1.
- `TELEMETRY` from Bolt 5.4.
- `SUCCESS`, `RECORD`, `IGNORED`, and `FAILURE` responses.

The explicit version tests exercise 3.0, 4.2, 4.3, 4.4, 5.0, 5.1, 5.2, 5.3, 5.4, 5.6, 5.7, 5.8, and 6.0, plus manifest v1 over selected later versions. Bolt 1, 2, 4.0, 4.1, and 5.5 exist in protocol models but are not standalone cases in `test_versions.py`.

### A useful inconsistency to resolve

The two local BoltStub implementations are not perfectly aligned on the first manifest-capable 5.x version:

- The Rust `BoltVersion::max_handshake_manifest_version` returns manifest v1 for 5.6 and later.
- The Python protocol classes show 5.6 with maximum manifest 0 and 5.7 with maximum manifest 1.
- The Rust README says handshake v2 requires 5.7+.
- The explicit manifest test loops over 5.7, 5.8, and 6.0.

This is not a reason to guess. It is a reason to make protocol/version support evidence-driven and to run the same scripts against both stubs where possible. Initial Knight Bus claims should target the exact versions exercised by the chosen official driver and TestKit matrix.

### Quantitative TestKit surface

The TestKit snapshot includes:

- 758 BoltStub `.script` fixtures.
- 205 Python files.
- 41 Rust files.
- 758 scripts distributed across routing, authorization, summaries, result iteration, session parameters, home database behavior, optimizations, data types, transaction begin parameters, driver parameters, execute-query behavior, versions, retries, disconnects, notifications, and transport cases.

The largest script families found include routing (279), authorization (145), summary (36), iteration (31), session parameters (27), home database (27), optimizations (26), data types (26), transaction-begin parameters (25), driver parameters (22), and execute-query behavior (15).

### TestKit limits

TestKit is primarily a driver integration and conformance harness. It normally starts a driver-specific backend and either a scripted Bolt server or a Neo4j server. It is not directly a drop-in server conformance test for Knight Bus.

There are two viable uses:

1. **Official-driver-through-Knight-Bus mode:** point each official driver backend at the Knight Bus Bolt endpoint and run the relevant tests.
2. **Transcript inversion mode:** translate selected BoltStub scripts into client transcripts that are replayed against Knight Bus as the server under test.

The first is more adoption-realistic. The second gives precise state-machine coverage. Both are needed for a strong claim.

## Observable contract across official drivers

### Shared contract

All five official drivers converge on a recognizably common contract:

| Surface | Observable behavior Knight Bus must decide |
|---|---|
| URI | Direct `bolt` schemes versus routing `neo4j` schemes; encrypted variants exist. |
| Driver lifecycle | Long-lived, concurrency-safe driver/pool; close/dispose semantics. |
| Session lifecycle | Lightweight, short-lived, generally not concurrency-safe; serial work per session. |
| Query execution | Auto-commit `run` and a simplified executable-query API using managed transactions. |
| Transactions | Explicit begin/commit/rollback and retryable managed read/write units. |
| Parameters | Typed maps including graph, temporal, spatial, bytes, and large integers. |
| Results | Lazy or batched records; summary becomes final after stream consumption. |
| Routing | Read/write mode, database selection, home database, server routing table. |
| Causal consistency | Bookmarks and bookmark managers. |
| Auth | none/basic/bearer/Kerberos/custom plus token refresh and re-auth in newer protocols. |
| Summaries | Query, parameters, counters, query type, plan/profile, timings, server, database. |
| Status | Notifications and newer GQL status objects. |
| Errors | Legacy Neo4j code plus GQL status, description, classification, diagnostic record, and cause chain. |
| Cancellation | Context, signal, token, session close, or connection termination depending on language/API. |
| Retry | Managed transactions retry selected transient/session/security failures; auto-commit is different. |

### Language-specific observations

#### Java

- The driver is thread-safe and exposes synchronous, asynchronous, reactive, and reactive-stream variants.
- `executableQuery` is a managed-transaction convenience API with retry behavior.
- A result summary includes counters, type, plan/profile, notifications/GQL status, availability/consumption timings, server, and database.
- Session-level authentication depends on protocol capability, with current code documenting Bolt 5.1+ for re-auth.

#### Go

- The driver is concurrency-safe; sessions are not.
- `ExecuteQuery` wraps an explicit retryable transaction and uses bookmark management for causal consistency.
- The simplified API excludes constructs that require auto-commit behavior, such as `CALL {} IN TRANSACTIONS` and older periodic commit usage.
- Context cancellation and retry classification are part of the visible behavior.

#### Python

- Synchronous and asynchronous APIs mirror the same major lifecycle.
- `Session.run` receives the header promptly and fetches records lazily.
- Starting another result on the same session can force buffering of the previous result.
- Driver-level `execute_query` uses managed transaction functions and has the same auto-commit exclusions.
- GQL errors expose status, description, diagnostic record, classification, and nested cause traversal.

#### JavaScript

- `executeQuery` uses managed transactions and returns an eager transformed result by default.
- `Session.run` supports promise-style eager consumption and subscription-style lazy consumption.
- Session fetch size and low/high record watermarks affect streaming behavior.
- Query config includes routing, database, impersonated user, bookmark manager, auth, transaction config, and abort signal.
- Result summaries guarantee at least one GQL status object, including client-generated/polyfilled statuses for older servers.

#### .NET

- `IDriver` is thread-safe and exposes async sessions plus a fluent executable-query API.
- Sessions serialize transactions and are not thread-safe.
- Result cursors must be consumed inside a managed transaction callback before that transaction closes.
- Query config includes routing, database, impersonation, bookmark manager, transaction config, auth token, and cancellation-aware cursor processing.
- Errors expose legacy codes and GQL cause chains through standard `InnerException` traversal.

### Cross-driver differences are part of the contract

TestKit contains driver-specific skips and expected differences. Examples in the version tests include summary server-address handling and server-agent parsing differences. Therefore, "the official drivers" are not one perfectly identical API. Knight Bus should publish a driver/version matrix and test each matrix cell, not infer all languages from one successful client.

## neo4rs: useful implementation evidence, not a compatibility oracle

neo4rs is useful because it demonstrates idiomatic Rust handling of:

- Async TCP/TLS connections and Bolt chunk framing.
- Connection pooling.
- Query parameter construction.
- Read/write execution and result streaming.
- Transactions, commit, rollback, bookmarks, routing, and retries.
- Result summaries and legacy Neo4j error classification.

It is not a substitute for official-driver and TestKit validation. The stable version enum in this snapshot advertises Bolt 4.0, 4.1, 4.3, and 4.4, while newer routing/protocol work is feature-gated as unstable. It also lacks the complete modern GQL status behavior seen in official drivers. Use it to learn Rust design patterns and wire formats, then verify behavior against TestKit and official clients.

## Browser, shell, and OGM migration expectations

### Browser

Browser demonstrates what interactive users expect after entering Cypher:

- Parameter conversion into driver graph types.
- Read versus write routing.
- Database selection.
- Managed transaction functions as the normal path, with auto-commit for special constructs.
- Query cancellation by closing a tracked session/worker path.
- Graph entities, paths, tables, plans, counters, and errors mapped into display-oriented forms.
- Connectivity checks against the `system` database with fallback behavior for older servers.

This is evidence that compatible query text alone is insufficient. A useful interactive experience also depends on result shapes, plans, counters, graph values, cancellation, and errors.

Browser is GPLv3 in this snapshot. It should be treated as an oracle and UX reference unless the product deliberately adopts a compatible licensing strategy.

### Cypher shell

The Cypher shell repository was not among Agent 03's assigned repositories and was not inspected. No shell compatibility claim is made here. Browser's command path provides shell-like evidence for query/parameter/result expectations, but it is not a substitute for the actual shell contract.

### OGM

OGM expects considerably more than read-query compatibility:

- Entity metadata, identity, labels, relationships, and a session mapping context.
- Object loading by identity/type/filter with configurable traversal depth.
- Save/delete behavior, optimistic locking, paging, sorting, and generated Cypher.
- Explicit and implicit transaction orchestration.
- Bookmarks, database and user selection, driver-specific native types, and exception translation.
- Multiple response model shapes and query statistics.

OGM support is therefore not a sensible P0 proxy for "low migration ceremony." It would pull Knight Bus toward writes, identity semantics, query generation, and object-cache behavior that A007 does not yet justify. Treat OGM as a later adoption test only if a founder interview produces a design partner whose high-value access-path workload already runs through OGM.

## Compatibility profile prioritization

Compatibility must be versioned as named profiles. "Cypher compatible" is too vague to test and too broad to defend.

| Profile | Priority | Included | Explicitly excluded | Why |
|---|---:|---|---|---|
| `KB-ACCESS-P0` | 0 | Java/Python official drivers; direct Bolt; parameterized read-only `MATCH`/`WHERE`/`WITH`/`RETURN`; bounded paths; database/access mode; basic/bearer auth acceptance; lazy `PULL`; cancellation; records/summary/error; fit/refuse receipts | Writes, cluster routing, OGM, Browser parity, procedures except explicitly named bounded ones | Smallest path to an unchanged security/dependency query |
| `KB-ACCESS-P1` | 1 | Go/JS/.NET; single-node routing facade; bookmarks; managed read retries; notifications/GQL status; exact spill; EXPLAIN-like preflight | Full cluster behavior, write retry semantics, admin | Reduces language and deployment ceremony after P0 proof |
| `KB-ANALYTICS-P2` | 2 | Named algorithm procedures/functions for shortest path, WCC, PageRank, similarity, community, triangles, embeddings; explicit approximation bounds | Arbitrary GDS surface and custom plugins | Connects familiar query invocation to custom bounded OLAP storage |
| `KB-INTERACTIVE-P3` | 3 | Browser-oriented result values, plans, counters, cancellation, visualization-ready graph values | Browser product clone | Useful only after interactive workflow demand is observed |
| `KB-OGM-P4` | 4 | Selected read-only OGM query/load paths | Save/delete/cache/write parity | High ceremony cost and weak current ICP evidence |
| `KB-GENERAL` | Kill/defer | Broad writes, schema/admin, multi-database cluster, plugin/procedure ecosystem, all dialect versions | None | This is effectively a Neo4j rewrite and violates A007 focus |

### Founder decision required before freezing P0

The language order is evidence-informed, not customer-proven. A founder must obtain actual production query captures and identify:

- Which official driver and version is in use.
- Whether the URI is `bolt://` or `neo4j://`.
- Whether the workload uses auto-commit or managed transactions.
- Required database, bookmarks, impersonation, auth, TLS, and notification settings.
- Returned graph/value types.
- Whether the query invokes GDS/APOC/custom procedures.
- Whether internal node/relationship IDs are exposed.

If the first design partners are primarily Go, JS, or .NET, change the P0 language order. Do not preserve Java/Python priority as doctrine.

## Unchanged production query architecture

"Unchanged" should mean unchanged query text and parameter values. It should not quietly mean zero configuration changes. The client will normally need a Knight Bus endpoint, credentials, and potentially a different database name or certificate. A transparent proxy is a separate product choice.

```text
 Existing application
 official Neo4j driver
       |
       |  Bolt handshake + HELLO/LOGON
       v
 +---------------------------+
 | Knight Bus Bolt gateway   |
 | - version/profile gate    |
 | - PackStream values       |
 | - session/tx state        |
 | - bounded PULL/DISCARD    |
 +-------------+-------------+
               |
               | raw query text + typed params + session metadata
               v
 +---------------------------+
 | Compatibility compiler    |
 | - parse declared profile  |
 | - reject unsupported      |
 | - produce logical intent  |
 +-------------+-------------+
               |
               v
 +---------------------------+
 | Admission and planner     |
 | - artifact checksum       |
 | - full working-set model  |
 | - hard memory ceiling     |
 | - fit/spill/approx/refuse |
 +-------------+-------------+
               |
               v
 +---------------------------+
 | Bounded algorithm runner  |
 | - custom OLAP storage     |
 | - cgroup/process ceiling  |
 | - cancellation            |
 +------+------+-------------+
        |      |
        |      +--------------------+
        v                           v
 compatible RECORD/SUCCESS      proof receipt
 summary/error/status           estimate + decision + observed
```

### Separation of concerns

The Bolt gateway must not own algorithm memory. The compiler must not promise support that the admission planner cannot budget. The runner must not manufacture protocol semantics. The receipt must combine identifiers from all layers so a customer can reproduce a run.

### Query support and resource decision are separate axes

| Query profile | Resource decision | Outcome |
|---|---|---|
| Unsupported | Any | Deterministic pre-execution compatibility refusal |
| Supported | Fit | Exact in-budget execution |
| Supported | Spill | Exact bounded-memory out-of-core execution |
| Supported and approximation-capable | Approximate, explicitly opted in | Bounded approximate result with method/error fields |
| Supported | Refuse | No execution because no permitted plan can honor the ceiling |

An unsupported query must not be mislabeled as a memory refusal. A memory refusal must not be disguised as a syntax error. This distinction belongs in the error and receipt schema.

## Security/dependency query corpus

### Corpus sources

The corpus should be built from actual first-ICP workloads, then expanded synthetically around their risks.

| Artifact family | Example query intents |
|---|---|
| SBOM/package graph | Vulnerable package reachability, reverse dependency blast radius, bounded transitive dependencies |
| Code graph | Import/call reachability, affected services, shortest dependency path, weak components |
| IAM graph | Principal-to-resource access path, inherited membership, privileged path, separation-of-duty anomalies |
| Service topology | Upstream/downstream blast radius, dependency components, critical intermediaries |
| Attack graph | Shortest exploitable path, all paths under depth/cost bound, high-centrality choke points |

### Required captured fields

Each production corpus case should store:

- A stable case ID and owner-approved sensitivity class.
- Raw query bytes and SHA-256. Preserve exact text; do not normalize before hashing.
- Parameter values in a redacted typed envelope and a separate parameter-schema hash.
- Driver language/version and API used.
- URI scheme, database, access mode, fetch size, transaction mode, timeout, bookmark use, and notification filters.
- Required auth scheme name, never credentials.
- Artifact manifest/version/checksum and graph counts.
- Expected row-order contract.
- Expected result columns and type schema.
- Whether graph entity IDs are semantically observed.
- Baseline Neo4j version/edition and cold/warm-cache state.
- Neo4j output digest, summary digest, status/error observation, latency, and memory measurement method.
- Knight Bus profile and expected fit/spill/approx/refuse policy.

### Corpus generation layers

1. **Production seeds:** verbatim customer queries and parameters with secrets/PII removed or tokenized.
2. **TCK semantic seeds:** relevant read scenarios for grammar and result behavior.
3. **Mutation layer:** vary nulls, empty lists, duplicate rows, missing labels/properties, path length, optional matches, ordering, limits, and parameter types.
4. **Graph-shape layer:** chains, stars, hubs, disconnected components, cliques, bipartite structures, power-law degree, skewed labels, and isolated vertices.
5. **Resource adversaries:** high-degree frontiers, output explosions, low-selectivity predicates, many equal shortest paths, tiny budget, nearly fitting budget, spill exhaustion, and cancellation at every phase.
6. **Protocol adversaries:** partial `PULL`, `DISCARD`, disconnect after `RUN`, retryable failure, auth expiry, malformed values, unsupported version, and concurrent sessions.

### Corpus admission rule

No generated corpus can substitute for production evidence. Synthetic cases harden a profile after a founder has selected it; they do not prove that the profile matters.

## Differential normalization

Raw output equality is insufficient because some values and metadata are store- or version-dependent. Over-normalization is equally dangerous because it can hide a real mismatch. Use a typed canonical envelope with field-specific rules.

### Record rules

| Value | Normalization rule |
|---|---|
| Null/boolean/string | Exact value equality after valid UTF-8 handling |
| Integer | Signed 64-bit mathematical value; detect overflow rather than coerce to float |
| Float | IEEE-aware comparison; define NaN, infinities, and negative-zero policy explicitly |
| Bytes/vector | Exact type, element type, length, and bytes |
| List | Preserve order and multiplicity |
| Map | Sort keys for encoding only; compare keys and typed values exactly |
| Date/time/duration | Canonical component form preserving timezone/offset semantics |
| Point | Compare CRS/SRID and coordinates |
| Node | Labels as a multiset/set per protocol semantics, properties typed; IDs handled separately |
| Relationship | Type, properties, direction/endpoints; IDs handled separately |
| Path | Ordered alternating node/relationship sequence and direction |

Internal IDs and element IDs are dangerous. They can legitimately differ between Neo4j and a portable Knight Bus artifact. A corpus must either:

1. Compare by stable domain key or artifact ordinal.
2. Install an explicit ID correspondence map during artifact conversion.
3. Declare the query outside the profile if it semantically depends on Neo4j internal IDs.

Silently dropping IDs would produce a false compatibility claim.

### Row ordering

- When the query or scenario has an ordering contract, compare row sequence exactly.
- Otherwise compare a multiset of canonical row digests, preserving duplicate counts.
- Never sort user-visible list/path values inside a row.
- Record the comparison mode in the receipt.

### Summary normalization

Compare separately:

- Column names.
- Query type.
- Counters for supported side effects. P0 should have no writes.
- Database and server identity under a documented mapping.
- Plan/profile only when that capability is in the profile.
- Notifications and GQL status by code, classification, position, and documented fields.
- Result availability and consumption times as measurements, not equality fields.

### Error normalization

Compare:

- Phase: handshake, auth, parse, plan, admission, execute, stream, commit.
- Legacy Neo4j code where applicable.
- GQL status and status-description category.
- Classification and retryability.
- Cause-chain structure.
- Input position for syntax/semantic errors.
- Knight Bus extension fields for unsupported profile, budget refusal, spill failure, or approximation requirement.

Do not compare human-readable messages as the primary key. Do retain them in redacted diagnostic output.

### Result digest

A differential case should produce:

```text
case_digest = SHA256(
  profile_version
  + query_sha256
  + parameter_schema_sha256
  + artifact_checksum
  + ordered_or_multiset_record_digest
  + normalized_summary_digest
  + normalized_error_digest
)
```

## Compatibility test pyramid

| Level | Test | Oracle | Exit criterion |
|---:|---|---|---|
| L0 | Provenance and manifest | Git blobs, license files, artifact manifest | Every input and executable version is pinned and checksummed |
| L1 | PackStream value and framing | TestKit JOLT, official driver values, property/fuzz tests | Supported values round-trip across selected Bolt versions without panic or silent coercion |
| L2 | Bolt state-machine transcripts | BoltStub scripts, both stub implementations where possible | Handshake, auth, RUN/PULL/DISCARD, reset, tx, failure, and disconnect transitions match the declared profile |
| L3 | Cypher semantic subset | Pinned openCypher TCK scenarios | Every selected scenario passes; every unselected construct refuses deterministically |
| L4 | Official driver contract | Java/Python first, then Go/JS/.NET TestKit backends | Driver/version matrix passes the selected session/result/auth/routing/error cases |
| L5 | Tiny differential | Neo4j baseline versus Knight Bus | Canonical records, status, and errors match on generated graphs and production query forms |
| L6 | Production replay | Redacted real query plus portable artifact | Query text and params unchanged; result digest accepted by design partner |
| L7 | Hard-budget and failure | cgroup/process monitor, spill/temp limits, fault injection | Zero ceiling breaches; deterministic fit/spill/approx/refuse; cancellation and cleanup verified |
| L8 | Adoption workflow | Timed design-partner migration | Endpoint/config change is acceptable and receipt changes an operational decision |

### Why the pyramid is ordered this way

Passing L3 before L2 can still produce a server no official driver can use. Passing L4 before L7 can produce a familiar system that violates the product promise. Passing all technical levels before L8 can still produce something nobody adopts. The final proof requires all three dimensions: semantics, budget, and behavior change.

## Hard-budget mode contract

### Fit

- Preflight predicts the complete working set and output policy under the selected algorithm/storage plan.
- The runner reserves/enforces the ceiling before execution.
- If runtime reaches a guard band, it cancels or transitions only to a pre-authorized spill plan.
- Receipt reports estimate range, observed high-water mark, and estimator error.

### Spill

- Exact result semantics are preserved.
- Memory remains bounded while temporary storage and I/O increase.
- Temp-space and I/O budgets are explicit, not unlimited escape hatches.
- Spill file lifecycle is crash-safe and cleaned or recoverable.
- Receipt reports bytes read/written, spill partitions/passes, temp peak, and wall/CPU cost.

### Approximate

- The query/algorithm must have an approved approximate implementation.
- The caller must opt in explicitly; no silent downgrade.
- The method, seed, stopping rule, confidence/error bound, and reproducibility fields are in the receipt.
- If the promised bound cannot be met under budget, refuse.

### Refuse

- Refusal occurs before expensive execution whenever the estimator can know the plan will not fit.
- The error distinguishes unsupported syntax, unsupported semantics, insufficient RAM, insufficient temp space, and disallowed approximation.
- The receipt explains the smallest known acceptable budget or marks it unknown.

## Receipt schema

The Bolt-compatible result and the proof receipt are separate artifacts joined by a run ID. Do not overload standard Neo4j summary metadata with the entire proprietary receipt. A small run/receipt identifier may be exposed through documented extension metadata if official drivers preserve it; otherwise provide an out-of-band file/API.

```json
{
  "schema_version": "kb.receipt.v1",
  "run_id": "uuid",
  "compatibility": {
    "profile": "KB-ACCESS-P0",
    "profile_version": "semver-or-checksum",
    "driver": {"language": "python", "version": "..."},
    "bolt_negotiated": "5.x",
    "query_sha256": "...",
    "parameter_schema_sha256": "...",
    "database": "...",
    "access_mode": "read",
    "transaction_mode": "auto_commit|managed_read",
    "auth_scheme": "basic|bearer|none",
    "routing_mode": "direct|single_node_facade",
    "comparison_mode": "ordered|multiset"
  },
  "artifact": {
    "manifest_version": "...",
    "checksum": "...",
    "nodes": 0,
    "edges": 0,
    "representation_bytes": 0
  },
  "admission": {
    "algorithm": "...",
    "parameters_sha256": "...",
    "estimate": {
      "fixed_bytes": 0,
      "per_node_bytes": 0,
      "per_edge_bytes": 0,
      "frontier_bytes": 0,
      "output_bytes": 0,
      "conversion_bytes": 0,
      "lower_bytes": 0,
      "point_bytes": 0,
      "upper_bytes": 0,
      "confidence": "measured|calibrated|experimental"
    },
    "hard_rss_bytes": 0,
    "temp_bytes": 0,
    "decision": "fit|spill|approximate|refuse",
    "plan_id": "..."
  },
  "execution": {
    "cold_or_warm": "cold|warm",
    "peak_rss_bytes": 0,
    "heap_bytes": 0,
    "mapped_bytes": 0,
    "retained_bytes": 0,
    "bytes_read": 0,
    "bytes_written": 0,
    "spill_bytes": 0,
    "wall_ms": 0,
    "cpu_ms": 0,
    "rows": 0,
    "result_sha256": "...",
    "approximation": null,
    "cancelled": false
  },
  "verification": {
    "baseline": "neo4j-version-or-none",
    "normalized_result_match": true,
    "estimator_absolute_error_bytes": 0,
    "estimator_percent_error": 0.0
  },
  "engine": {
    "version": "...",
    "build_sha256": "...",
    "host_profile_sha256": "..."
  }
}
```

Never write credentials, raw auth tokens, private keys, or unredacted sensitive parameters to receipts. Query text should be omitted by default in favor of a hash unless the customer explicitly chooses a protected query archive.

## Founder-gated requirements

The numerical thresholds below are proposed gates, not validated customer facts. Founder interviews may tighten, loosen, or remove them.

| ID | Founder-gated requirement | Pass evidence | Failure meaning |
|---|---|---|---|
| `A03-GATE-001` | At least two ICP design partners provide real security/dependency query captures | Redacted corpus manifests | No evidence that Cypher compatibility is the right ceremony reducer |
| `A03-GATE-002` | At least one high-value query falls inside a narrow read-only profile | Profile compiler accepts it without query rewrite | Wedge requires broader surface or different interface |
| `A03-GATE-003` | Query text and typed parameter values remain unchanged | Byte/hash comparison in production replay | Compatibility is marketing, not migration relief |
| `A03-GATE-004` | Endpoint/config migration is acceptable in a timed session | Design partner completes setup without product engineer intervention | Ceremony is still too high |
| `A03-GATE-005` | Every selected official driver/version passes the declared TestKit slice | Matrix report with zero unexplained skips | Compatibility claim must remain narrower |
| `A03-GATE-006` | Exact results match Neo4j under the canonical differential rules | L5/L6 digests and reviewed exceptions | Semantic trust is absent |
| `A03-GATE-007` | Hard RSS ceiling is never exceeded in the supported envelope | Repeated L7 cgroup/process measurements | Core product promise is false |
| `A03-GATE-008` | Refusal occurs before material work for known non-fitting cases | Phase timestamps and receipt | Estimate is advisory rather than enforceable |
| `A03-GATE-009` | Approximation is never selected without explicit opt-in | Protocol/API tests | Trust boundary is violated |
| `A03-GATE-010` | Receipt changes at least one buy/run/resize/refuse decision | Interview replay with before/after decision | Receipt is ceremonial output, not product value |
| `A03-GATE-011` | Compatibility gateway overhead does not erase the memory/latency advantage | Gateway-off versus gateway-on benchmark | Familiar API is too expensive or architecturally coupled |
| `A03-GATE-012` | A supported query that cannot honor budget returns a stable machine-readable refusal | Cross-driver error tests | Operational automation cannot trust the system |

## Kill and falsification tests

### Product falsifiers

1. Buyers say their dominant pain is ingestion, schema modeling, permissions, UI, or organizational workflow, not memory-bounded analytics.
2. The real queries depend on unsupported writes, arbitrary procedures, dynamic Cypher generation, internal IDs, or cluster/admin semantics.
3. Switching endpoint and artifact format remains too much ceremony even when query text is unchanged.
4. Kuzu, DuckPGQ, Neo4j configuration, or another existing workflow already meets the budget and trust requirement.
5. Receipts are inspected but do not alter a purchase, machine-size, scheduling, or refusal decision.

### Compatibility falsifiers

1. Any supported ordered query returns rows in a different order.
2. Any supported unordered query loses or adds duplicate rows.
3. Any supported graph/path value is changed by canonicalization.
4. Any official-driver lifecycle hangs, leaks a session, mishandles partial `PULL`, or returns the wrong retry classification.
5. A driver appears to work only because TestKit cases were skipped or because errors were normalized too aggressively.
6. A query is accepted by the parser but cannot be mapped to a bounded plan.
7. A legacy/new Bolt version is advertised without running its transcript and driver matrix.

### Hard-budget falsifiers

1. Peak RSS exceeds the declared hard ceiling even once inside the supported operating envelope.
2. Mapped pages, off-heap buffers, driver buffers, output buffering, or conversion memory are excluded from the budget.
3. Spill uses unbounded temporary storage or creates unbounded merge buffers.
4. Cancellation does not stop compute and release memory/temp files promptly.
5. The estimator is calibrated only on friendly graphs and underpredicts hubs, skew, many-path outputs, or high-cardinality results.
6. An approximate mode lacks a reproducible seed or defensible error/quality statement.
7. A refusal starts the expensive allocation it was meant to prevent.

### Stop rule

One hard-ceiling breach blocks release of that profile. Semantic mismatches block the affected query construct. Repeated customer rejection of the endpoint/artifact/receipt workflow should stop compatibility expansion and trigger a product-strategy review rather than a larger rewrite.

## Licensing and provenance caveats

This section is engineering evidence, not legal advice.

| Repository | Local evidence | Engineering posture |
|---|---|---|
| openCypher | Apache 2.0 plus NOTICE | Grammar/TCK use requires attribution review; NOTICE warns against calling unapproved derivatives Cypher/openCypher and identifies Cypher as a trademark |
| TestKit | No top-level tracked LICENSE found; many source headers identify Apache terms | Treat as oracle-only until counsel confirms repository provenance and reuse terms |
| Java driver | Apache 2.0 plus NOTICE/license bundle | Public contracts and tests are strong oracles; copied code requires notice/dependency review |
| Go driver | Apache 2.0 | Strong oracle; preserve provenance if code is reused |
| Python driver | Apache 2.0, PSF license for marked portions, NOTICE | File-level provenance matters; do not assume one license covers every copied fragment |
| JavaScript driver | Apache 2.0 | Strong oracle; generated Deno mirrors should not be treated as independent implementation sources |
| .NET driver | Apache 2.0 plus NOTICE | Strong oracle; preserve notices if code is reused |
| neo4rs | `lib/Cargo.toml` says MIT; README describes dual MIT/Apache while corresponding root license files are not tracked | Resolve inconsistency before copying; safest immediate use is conceptual reference |
| Browser | GPLv3 with commercial-license language in NOTICE | Oracle/UX reference unless product licensing deliberately accepts GPL obligations |
| OGM | Apache 2.0 | Can be an oracle; supporting its surface is a product-scope decision, not merely a licensing decision |

Names and compatibility marketing also need review. Prefer wording such as "accepts the documented `KB-ACCESS-P0` query profile through selected Neo4j drivers" over an unqualified "Cypher compatible" or "Neo4j compatible."

## Evidence-honest claims

### Claims supported by this dossier

- There is a substantial reusable verification ecosystem for grammar scenarios, Bolt transcripts, and official-driver behavior.
- Official drivers expose a common enough read-query contract to define a narrow cross-language profile.
- Query text and typed parameters can plausibly remain unchanged while execution moves to a different bounded backend.
- Browser and OGM evidence show that migration expectations extend beyond parsing into result, lifecycle, and type behavior.
- TestKit and openCypher are complementary; neither alone proves the required adoption contract.
- A hard-budget receipt needs fields outside standard Neo4j result summaries.

### Claims not yet supported

- Knight Bus currently runs an unchanged production Cypher query.
- Knight Bus is Bolt compatible.
- Knight Bus passes TestKit or openCypher TCK.
- Knight Bus uses less RAM or lower latency for a production query.
- A customer will adopt the endpoint/artifact/receipt workflow.
- The selected P0 drivers match the actual first ICP.
- Full Browser, shell, OGM, Cypher, Neo4j, or GDS compatibility is feasible or desirable.

## Tangible verification sequence

### Phase 1: Freeze one customer-shaped profile

1. Obtain one redacted access-path query, typed parameters, official driver/version, and artifact.
2. Classify every syntax/semantic feature used.
3. Define `KB-ACCESS-P0` as exactly that query family plus necessary negative cases.
4. Declare unsupported constructs and stable refusal errors before implementation.

### Phase 2: Build the protocol seam

1. Select one exercised Bolt 5.x version based on the customer's official driver.
2. Implement handshake, auth acceptance, `RUN`, bounded `PULL`, `DISCARD`, `RESET`, and close behavior.
3. Implement only auto-commit read or managed-read transaction behavior required by the captured API.
4. Replay selected BoltStub scripts and official-driver TestKit cases.
5. Do not advertise any other version.

### Phase 3: Build semantic differential proof

1. Parse the exact production query into the declared profile.
2. Run tiny fixtures against Neo4j and Knight Bus.
3. Canonicalize values, rows, summaries, and errors using the rules above.
4. Make mismatches reproducible by corpus case ID and receipt.
5. Add TCK scenarios only where they exercise constructs actually used by the profile.

### Phase 4: Join compatibility to the hard-budget runner

1. Compile the query to a named bounded algorithm/storage plan.
2. Produce the complete working-set estimate before execution.
3. Enforce the hard RSS and temp-space ceilings at the process/cgroup boundary.
4. Exercise fit, spill, explicit approximation, and refusal.
5. Return compatible records plus the separate proof receipt.

### Phase 5: Run the adoption test

1. Give the design partner the endpoint, artifact, and documented profile.
2. Observe whether they can replay the query without rewriting it.
3. Compare Neo4j and Knight Bus correctness, peak RSS, latency distribution, and operational steps.
4. Ask what decision the receipt changes.
5. Expand only the next customer-required surface.

## Final recommendation

Build one proof-carrying compatibility slice, not a general compatibility layer.

The best first demonstration is an official-driver call whose query text and parameters are byte-for-byte the same as the customer's production access-path query. Neo4j and Knight Bus should run against equivalent artifacts. The result harness should prove semantic equivalence under explicit normalization. The Knight Bus run should additionally prove that it chose fit/spill/approximate/refuse under an enforced ceiling and report the observed high-water mark.

That demonstration would validate all three parts of the thesis at once:

1. The query matters to the ICP.
2. Compatibility removes enough ceremony to try the product.
3. The bounded runner and receipt deliver a capability the familiar interface alone does not.

Anything broader should wait for evidence from the next real query.

## Audit results

| Check | Result |
|---|---:|
| Agent 03 denominator rows | 7,202 |
| Agent 03 evidence rows | 7,202 |
| Missing Agent 03 rows | 0 |
| Unexpected Agent 03 rows | 0 |
| Repo/path/blob/bytes/extension mismatches | 0 |
| Duplicate repo/path identities | 0 |
| Empty evidence IDs | 0 |
| Duplicate Agent 03 evidence IDs | 0 |
| Invalid coverage statuses | 0 |
| Relevance outside 1-100 | 0 |
| Relevance >=80 without `direct_read` | 0 |
| Founder-critical source paths without `direct_read` | 0 |
| Full three-agent validator | PASS, 32,262 rows reconciled |

Graph-index unresolved references remain nonzero as quantified above. Those are structural-index limitations, not unresolved denominator coverage. No runtime TestKit, TCK, official-driver-to-Knight-Bus, or 2 GB production-query execution was performed in this evidence task because no implemented Knight Bus compatibility endpoint was in scope.

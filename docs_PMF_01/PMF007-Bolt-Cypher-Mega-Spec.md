# PMF007: Bolt Cypher Mega Spec

<!-- markdownlint-disable MD013 MD036 MD060 -->

> A versioned, executable contract for accepting production Neo4j traffic and
> executing it through Knight Bus without pretending that wire compatibility,
> language compatibility, transactional semantics, and storage execution are
> the same problem.

**Status:** Implementation specification  
**Date:** 2026-08-07  
**Supersedes:** Nothing  
**Builds on:** `PMF006-Cypher-Bolt-Walk-Spec.md` as Compatibility Slice 0  
**Primary corpus:** corrected Knight Bus v002, 2,187,775,971 raw bytes,
3,997,988 nodes, 36,294,270 directed edges, 60 fixed walk queries  
**Primary decision:** implement a clean-room Rust compatibility facade around
Knight Bus, not a line-for-line port of Neo4j.

## Executable Requirements

### Direct Answer

Yes, the wiring can happen.

A production application can keep its Neo4j driver, URI shape, parameterized
Cypher text, and record-consumption loop. It can point at a Knight Bus Bolt
endpoint. Knight Bus can negotiate Bolt, decode PackStream, validate the Bolt
state transition, parse Cypher, lower a supported query into a stable logical
IR, select a Knight Bus executor, stream Neo4j-shaped records, and return the
summary metadata expected by the driver.

The complete path is:

```text
Neo4j driver
    |
    |  unchanged Cypher + parameters over Bolt
    v
+------------------------ Knight Bus compatibility plane ---------------------+
| TCP/TLS/WebSocket -> handshake -> chunks -> PackStream -> Bolt state machine|
|                                      |                                      |
|                                      v                                      |
|                           query/session envelope                            |
|                                      |                                      |
|                                      v                                      |
| Cypher 5/25 parse -> semantic analysis -> normalized logical IR             |
|                                      |                                      |
|                         capability + resource admission                     |
|                    /                 |                    \                  |
|          walk specialization    GDS procedure bridge    general runtime     |
|                    \                 |                    /                  |
|                   Knight Bus storage and execution interfaces               |
|                                      |                                      |
|                        bounded asynchronous row stream                       |
|                                      |                                      |
|               PackStream RECORD / SUCCESS / FAILURE responses               |
+-----------------------------------------------------------------------------+
```

This already exists in embryonic form. The current uncommitted
`KnightBusBoltBackend.execute` accepts a Bolt query, converts parameters,
invokes `compile_neighborhood_walk_plan`, executes the plan against the mapped
runtime, and returns Bolt records. That is proof of the architectural seam, not
proof of broad compatibility. It currently supports one read-only auto-commit
walk profile, rejects explicit transactions, and materializes all records in a
`Vec`.

### Concrete Production-Wiring Acceptance

The application-facing target is ordinary Neo4j driver code. The application
changes only the endpoint/credentials, which can also be handled by deployment
configuration or DNS. Its query text, parameters, and result loop remain the
same:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://night-bus.internal:7687",
    auth=("application", "secret"),
)

records, summary, keys = driver.execute_query(
    """
    MATCH (source {node_id: $node_id})-[:DEPENDS_ON]->(target)
    RETURN target.node_id AS node_id
    ORDER BY node_id
    """,
    node_id="the-production-node-id",
    database_="night-bus-v002",
)
```

For `KB-C0-WALK`, that exact shape is compiled to the existing mapped walk
runtime over the corrected 2 GB snapshot. The dataset is prepared/opened by
Knight Bus; it is not copied through Bolt per query. On the wire, the driver
performs negotiation/authentication, sends `RUN` with the unchanged query and
parameters, sends `PULL`, and consumes `RECORD` plus `SUCCESS` messages.

For a query outside the active profile, such as a property predicate,
aggregation, explicit transaction, write, or unregistered GDS call, the current
slice SHALL return a typed unsupported-capability failure. As later profiles
turn those manifest entries green, the same driver path reaches a general or
specialized Knight Bus plan without another application rewrite.

### Governing Thesis

The program SHALL preserve the external contract and replace the internal
execution path selectively.

1. Bolt is the **wire and session contract**.
2. Cypher is the **language and semantic contract**.
3. The logical IR is the **compatibility firewall**.
4. Knight Bus is the **execution, storage, and resource contract**.
5. Neo4j and GDS are **behavioral oracles**, not implementation templates.
6. A feature is compatible only when a versioned test manifest proves it.
7. Unsupported behavior SHALL fail before execution with a stable typed error.
8. Low RAM and predictable latency are admission and execution properties, not
   consequences of writing the server in Rust.

### Meaning of "All"

"All Bolt and Cypher" is otherwise an unfinishable and unfalsifiable phrase.
For this specification it means:

- Every published Bolt protocol version from 1.0 through 6.0 is represented in
  a machine-readable version manifest; the deliberately unused Bolt 5.5 slot is
  represented as `not-applicable` and SHALL never be negotiated.
- The production release profile covers Bolt 4.4, 5.0 through 5.4, 5.6 through
  5.8, and 6.0,
  including version-specific messages, structures, state transitions, and
  metadata.
- Older Bolt 1.0 through 4.3 behavior is an explicit historical profile and is
  never silently inferred from a newer state machine.
- Cypher 5 is the frozen compatibility baseline.
- Cypher 25 is pinned to an explicit Neo4j calendar release and updated by
  manifest revision; the first full target SHALL be Neo4j 2026.06 semantics.
- Every grammar production, semantic rule, clause, expression family, logical
  operator, runtime operator, error category, and observable transaction rule
  in the declared profile has a test or an explicit unsupported entry.
- Procedures are capability packages. Core Cypher procedure invocation is part
  of the language; GDS and APOC procedure inventories are separately versioned
  plugin profiles.
- "Complete" never means bug-for-bug identity, identical plans, identical
  timing, Neo4j Enterprise clustering, or proprietary implementation parity.

### Compatibility Profiles

| Profile | External promise | Internal executor | Release purpose |
|---|---|---|---|
| `KB-C0-WALK` | Bolt + three fixed read walk shapes | Existing mmap walk runtime | PMF006 proof |
| `KB-C1-READ` | High-frequency read Cypher subset | Specialized and general read runtimes | First useful alpha |
| `KB-C2-GDS` | `CALL gds.*` catalog plus selected algorithms | GDS registry and custom OLAP layouts | Differentiated product |
| `KB-C3-READ-FULL` | Declared Cypher 5 read semantics | General logical/physical runtime | Migration pilot |
| `KB-C4-TX` | Writes and explicit transactions | MVCC/WAL storage interface | OLTP compatibility |
| `KB-C5-CURRENT` | Current production Bolt plus Cypher 5/25 | Full compatibility plane | Production migration |
| `KB-C6-HISTORICAL` | Bolt 1.0-4.3 and retired syntax | Versioned compatibility adapters | Optional archival support |

Profiles are monotonic only at the external contract. Internally, a later
profile may replace an executor while preserving prior fixtures and receipts.

### Product Boundary

The first product is not "Neo4j rewritten in Rust." It is:

> Run selected production Neo4j analytical workloads unchanged, under a hard
> memory budget, with proof of semantic equivalence and resource use.

This boundary permits the team to ship the valuable 20 percent before
reimplementing administrative, clustering, schema-management, and rare query
features. The mega spec still names those surfaces so that no early shortcut
poisons the eventual architecture.

### Evidence Ledger

All repositories below were indexed with both the codebase-memory graph and the
SDSRS code-graph index. The table records what each repository is allowed to
teach the implementation.

| Repository | Primary evidence | Use in this program |
|---|---|---|
| `neo4j-src` | Bolt FSM, Cypher front end, planner, runtimes, integration tests | Behavioral decomposition and black-box oracle |
| `neo4j-gds-src` | Procedure facade, graph catalog, algorithm configs and result modes | GDS capability and differential oracle |
| `neo4j-docs-bolt-src` | Handshake, PackStream, messages, FSM and version appendices | Protocol requirements; no source copying |
| `opencypher-src` | Grammar, CIPs, 1,615-scenario TCK | Portable Cypher baseline |
| `neo4j-testkit-src` | Driver contracts, BoltStub, Rust fixture parser | Cross-driver and protocol verification |
| `neo4j-java-driver-src` | Java session/result/transaction expectations | Official client matrix |
| `neo4j-go-driver-src` | Go session/result/transaction expectations | Official client matrix |
| `neo4j-python-driver-src` | Python session/result/transaction expectations | Slice 0 and official client matrix |
| `neo4j-javascript-driver-src` | JS session/result/transaction expectations | Official client matrix |
| `neo4j-dotnet-driver-src` | .NET session/result/transaction expectations | Official client matrix |
| `neo4rs-src` | Permissively licensed Rust Bolt client patterns | Rust interop and test client |
| `cypher-shell-src` | Interactive protocol and result behavior | CLI acceptance testing |
| `cypher-dsl-src` | Programmatically generated query shapes | Parser corpus generation |
| `neo4j-browser-src` | Browser query/result expectations | Later UI compatibility smoke tests |
| `neo4j-gds-client-src` | GDS call builders and result schemas | GDS client acceptance testing |
| `gds-agent-src` | Agent-generated GDS invocation shapes | Procedure discovery scenarios |
| `graph-data-science-src` | Small public examples | Example corpus only; not core implementation |
| `neo4j-apoc-procedures-src` | APOC procedure inventory and tests | Optional plugin profile |
| `neo4j-apoc-src` | APOC implementation and behavior | Oracle-only plugin research |
| `neo4j-ogm-src` | Entity mapping and transaction behavior | Later application migration gate |

Two Rust dependencies are seeds, not compatibility claims:

| Seed | Useful surface | Known gap that SHALL be removed or isolated |
|---|---|---|
| `boltr 0.2.0` | Rust Bolt server, PackStream, messages, session backend | Supports only Bolt 5.1-5.4 and materializes `Vec<BoltRecord>` |
| `grafeo-adapters 0.5.42` | Rust Cypher lexer/parser/AST | Not proven against Neo4j Cypher 5/25 semantics |

### Measured Scope

The relevant local Neo4j implementation is not weekend-sized:

| Surface | Main source lines | Test source lines |
|---|---:|---:|
| Bolt community module | 32,850 | 40,132 |
| Cypher front end | 229,082 | 108,645 |
| Cypher planner | 49,357 | 132,445 |
| Logical plans | 12,016 | 2,589 |
| Physical planning | 6,230 | 4,505 |
| Interpreted runtime | 35,157 | 24,110 |
| Slotted runtime | 14,180 | 5,494 |
| Runtime utilities | 24,285 | 11,096 |
| Runtime specification suite | - | 122,221 |
| Bolt and Cypher integration tests | - | 42,591 |

The directly relevant implementation is roughly 403,000 lines before kernel,
storage, security, administration, indexes, clustering, and enterprise-only
surfaces. The directly relevant verification corpus exceeds 490,000 lines.
These numbers justify a profile-driven clean-room implementation and reject the
premise that a syntactic Java-to-Rust translation is the easiest path.

### Licensing and Provenance Boundary

| Source family | Observed local license | Rule |
|---|---|---|
| Neo4j server | GPLv3 | Oracle and architecture evidence only; do not copy implementation |
| Neo4j GDS | GPLv3 or commercial | Oracle only without a separate license decision |
| Bolt documentation | CC BY-NC-SA 4.0 | Requirements evidence only; review before commercial redistribution |
| openCypher | Apache 2.0 | Reuse permitted with notices and legal review |
| Official drivers | Apache 2.0, with noted exceptions | Test clients and fixtures with notices |
| `neo4rs` | MIT | Candidate reusable test/client code |
| `boltr` | MIT or Apache 2.0 | Candidate server seed behind an internal trait |
| `grafeo-adapters` | Apache 2.0 | Candidate parser seed behind an internal trait |
| TestKit | No top-level license confirmed in local clone | Oracle-only until provenance is resolved |

Every production module SHALL carry a provenance record that names whether it
was authored from a public specification, permissively licensed source,
black-box observation, or independent design. GPL-derived source SHALL never be
placed in an implementation prompt.

### Target Rust Architecture

```text
crates/
  kb-bolt-wire/          TCP, TLS, WebSocket, handshake, chunk framing
  kb-packstream/         versioned values and structure codecs
  kb-bolt-message/       typed request/response messages
  kb-bolt-fsm/           generated per-version connection state machines
  kb-bolt-server/        connection scheduling and backpressure
  kb-query-envelope/     session, auth, database, bookmarks, deadlines
  kb-cypher-lexer/       source-preserving token stream
  kb-cypher-parser/      Cypher 5 and Cypher 25 syntax front ends
  kb-cypher-ast/         version-neutral source AST with spans
  kb-cypher-semantics/   scopes, types, nullability, privileges, errors
  kb-cypher-ir/          normalized logical operator algebra
  kb-cypher-planner/     rewrite, cardinality, costing, plan selection
  kb-cypher-runtime/     pull-based and morsel-based general execution
  kb-procedure-registry/ versioned procedures/functions and schemas
  kb-gds-bridge/         CALL gds.* lowering and result adapters
  kb-storage-spi/        snapshot, property, index, traversal, mutation traits
  kb-resource-governor/  RAM, spill, work, row, deadline, cancellation budgets
  kb-result-stream/      bounded async rows, summaries and cancellation
  kb-compat-harness/     TCK, TestKit, drivers and differential oracles
  kb-proof-receipt/      canonical query/plan/data/result/performance evidence
```

No external Bolt or parser crate may leak types across the `kb-bolt-server` or
`kb-cypher-parser` boundary. This allows `boltr` and `grafeo-adapters` to be
forked, replaced, or retained without changing the Knight Bus execution API.

### Core Internal Contracts

The implementation MAY refine names, but SHALL preserve these boundaries:

```rust
pub trait BoltVersionCodec {
    fn decode_request(&mut self, bytes: &[u8]) -> Result<BoltRequest, WireError>;
    fn encode_response(&mut self, response: BoltResponse) -> Result<Bytes, WireError>;
}

pub trait CypherFrontend {
    fn compile_query(
        &self,
        text: &str,
        parameters: &ParameterSchema,
        context: &SemanticContext,
    ) -> Result<LogicalQuery, QueryDiagnostic>;
}

pub trait QueryLowerer {
    fn lower_query(
        &self,
        logical: &LogicalQuery,
        catalog: &CapabilityCatalog,
        budget: &ResourceBudget,
    ) -> Result<ExecutablePlan, AdmissionDiagnostic>;
}

pub trait ResultProducer {
    fn pull_records(
        &mut self,
        demand: RecordDemand,
    ) -> impl Future<Output = Result<RecordBatch, ExecutionDiagnostic>>;
}
```

The logical IR SHALL be serializable, versioned, deterministic, independent of
Neo4j classes, and rich enough to represent unsupported operators without
lowering them. Parsing a feature and executing it are separate capabilities.

### Lowering Decision Table

| Recognized logical shape | First-choice lowering | Fallback | Rejection point |
|---|---|---|---|
| Fixed one/two-hop dependency walk | `MmapWalkPlan` | General expand plan | Admission |
| `CALL gds.*` with registered signature | `GdsProcedurePlan` | None | Semantic analysis |
| Indexed node lookup + bounded expand | `IndexExpandPlan` | Scan/filter/expand | Cost/admission |
| Aggregation over graph projection | Custom algorithm-shaped layout | General streaming aggregate | RAM admission |
| General read query | Morsel/pull operator DAG | Interpreted operator DAG | Capability check |
| Write query | Transactional mutation plan | None | Profile gate |
| Admin/schema query | Administrative capability | None | Profile gate |

### Requirement Syntax

Each requirement is binding. `WHEN` defines the trigger, `THEN` defines the
observable result, `SHALL` defines invariants, and `Verification` names the
minimum automated evidence. A requirement is not implemented until every named
test is green in the declared profile.

### Governance and Provenance

#### REQ-GOV-001.0: Publish a machine-readable compatibility manifest

**WHEN** a Knight Bus build is produced  
**THEN** it SHALL embed the exact Bolt, Cypher, procedure, storage, and feature
profiles it implements  
**SHALL** distinguish `implemented`, `parsed-only`, `experimental`,
`unsupported`, and `not-applicable` for every manifest item.  
**Verification:** `TEST-GOV-001-MANIFEST-COMPLETE`

#### REQ-GOV-002.0: Pin every oracle

**WHEN** differential verification is run  
**THEN** it SHALL record Neo4j, GDS, driver, TCK, TestKit, and fixture versions
or Git SHAs  
**SHALL NOT** compare against an unrecorded moving target.  
**Verification:** `TEST-GOV-002-ORACLE-PINS`

#### REQ-GOV-003.0: Enforce clean-room provenance

**WHEN** implementation source is added  
**THEN** its module provenance SHALL identify specification, permissive source,
black-box observation, or independent design  
**SHALL** fail CI when GPL-derived snippets or unattributed copied fixtures are
detected.  
**Verification:** `TEST-GOV-003-PROVENANCE-AUDIT`

#### REQ-GOV-004.0: Preserve capability monotonicity

**WHEN** a new profile release is proposed  
**THEN** all previously supported fixtures SHALL remain supported or be covered
by an approved breaking-change manifest  
**SHALL NOT** silently narrow behavior.  
**Verification:** `TEST-GOV-004-MANIFEST-DIFF`

#### REQ-GOV-005.0: Keep parser and executor claims separate

**WHEN** a syntax construct parses but cannot execute  
**THEN** the manifest SHALL report `parsed-only` and execution SHALL return a
typed unsupported-capability diagnostic  
**SHALL NOT** claim Cypher compatibility for that construct.  
**Verification:** `TEST-GOV-005-PARSED-ONLY`

#### REQ-GOV-006.0: Version observable errors

**WHEN** an error code, GQL status, diagnostic field, or Bolt metadata shape
changes  
**THEN** the compatibility manifest SHALL record the version boundary  
**SHALL** preserve older shapes for negotiated older profiles.  
**Verification:** `TEST-GOV-006-ERROR-VERSIONING`

#### REQ-GOV-007.0: Make support falsifiable

**WHEN** a feature is marked implemented  
**THEN** at least one positive, one negative, and one differential fixture SHALL
name that feature ID  
**SHALL** reject orphaned support claims in CI.  
**Verification:** `TEST-GOV-007-CLAIM-TRACEABILITY`

#### REQ-GOV-008.0: Preserve PMF006 as Slice 0

**WHEN** any compatibility-plane component changes  
**THEN** the complete 60-query v002 walk corpus SHALL still pass through an
official Neo4j driver over Bolt  
**SHALL** report cold-open and warm-query performance separately.  
**Verification:** `TEST-GOV-008-SLICE-ZERO`

### Bolt Wire and Transport

#### REQ-BOLT-001.0: Accept the Bolt identification preamble

**WHEN** a client sends the four-byte Bolt identification on a configured
connector  
**THEN** the server SHALL enter version negotiation  
**SHALL** reject malformed or truncated preambles without panic or unbounded
allocation.  
**Verification:** `TEST-BOLT-001-PREAMBLE`

#### REQ-BOLT-002.0: Negotiate legacy handshakes

**WHEN** a client offers four legacy version words  
**THEN** the server SHALL select the highest mutually supported compatible
version according to the pinned version manifest  
**SHALL** return zero when no version is compatible.  
**Verification:** `TEST-BOLT-002-LEGACY-NEGOTIATION`

#### REQ-BOLT-003.0: Negotiate Manifest v1

**WHEN** a client offers handshake `00 00 01 FF`  
**THEN** the server SHALL send its bounded version list and capability bitmask,
validate the client's selection, and enter the selected protocol  
**SHALL** reject invalid VarInts, excess entries, and unoffered selections.  
**Verification:** `TEST-BOLT-003-MANIFEST-V1`

#### REQ-BOLT-004.0: Support the declared protocol version matrix

**WHEN** a connection negotiates any manifest-supported Bolt version from 1.0
through 6.0  
**THEN** the matching generated codec and state machine SHALL be selected when
that version is enabled by profile  
**SHALL NOT** route a historical version through an assumed current codec.  
**Verification:** `TEST-BOLT-004-VERSION-MATRIX`

#### REQ-BOLT-005.0: Decode and encode chunk framing

**WHEN** messages arrive in one or many Bolt chunks  
**THEN** the server SHALL reconstruct exactly one bounded message and encode
responses using valid chunk boundaries and terminators  
**SHALL** support split headers, zero chunks, pipelined messages, and transport
fragmentation.  
**Verification:** `TEST-BOLT-005-CHUNK-FRAMING`

#### REQ-BOLT-006.0: Enforce frame and message limits

**WHEN** a frame, chunk sequence, nesting depth, string, collection, or message
exceeds configured limits  
**THEN** the connection SHALL fail deterministically with no process-wide OOM  
**SHALL** release all per-connection buffers.  
**Verification:** `TEST-BOLT-006-LIMITS`

#### REQ-BOLT-007.0: Support TCP and TLS connectors

**WHEN** a client uses `bolt`, `neo4j`, `bolt+s`, `neo4j+s`, or the self-signed
test variants allowed by configuration  
**THEN** TCP and TLS establishment SHALL match the connector policy  
**SHALL** reject plaintext on required-TLS connectors and invalid certificates
under strict trust.  
**Verification:** `TEST-BOLT-007-TLS-CONNECTORS`

#### REQ-BOLT-008.0: Support WebSocket transport where declared

**WHEN** the browser profile connects over WebSocket  
**THEN** Bolt bytes SHALL be carried without changing message semantics  
**SHALL** enforce the same byte, timeout, and cancellation limits as TCP.  
**Verification:** `TEST-BOLT-008-WEBSOCKET`

#### REQ-BOLT-009.0: Implement complete PackStream primitives

**WHEN** any legal null, boolean, integer, float, byte array, string, list, map,
or structure value for the negotiated version is received  
**THEN** it SHALL round-trip canonically within protocol rules  
**SHALL** reject invalid markers, lengths, UTF-8, map keys, and excessive
nesting.  
**Verification:** `TEST-BOLT-009-PACKSTREAM-PRIMITIVES`

#### REQ-BOLT-010.0: Implement versioned structure semantics

**WHEN** graph, spatial, temporal, duration, vector, or version-specific
structure values are exchanged  
**THEN** fields, signatures, timezone semantics, and element identifiers SHALL
match the negotiated protocol  
**SHALL** preserve unknown future structures only where the manifest permits.  
**Verification:** `TEST-BOLT-010-STRUCTURES`

#### REQ-BOLT-011.0: Decode every declared client message

**WHEN** the negotiated profile permits historical or current `INIT`, `HELLO`,
`LOGON`, `LOGOFF`, `GOODBYE`, `ACK_FAILURE`, `RESET`, `ROUTE`, `RUN`, `BEGIN`,
`COMMIT`, `ROLLBACK`, `PULL_ALL`, `DISCARD_ALL`, `PULL`, `DISCARD`, or
`TELEMETRY`  
**THEN** the exact versioned fields SHALL be decoded into internal messages  
**SHALL** reject missing, extra, or mistyped fields with protocol-correct
behavior.  
**Verification:** `TEST-BOLT-011-CLIENT-MESSAGES`

#### REQ-BOLT-012.0: Encode every declared server message

**WHEN** a request completes, yields records, is ignored, or fails  
**THEN** the server SHALL encode version-correct `SUCCESS`, `RECORD`, `IGNORED`,
or `FAILURE` messages  
**SHALL** apply the negotiated GQL-status and failure-field rules.  
**Verification:** `TEST-BOLT-012-SERVER-MESSAGES`

#### REQ-BOLT-013.0: Generate state machines from declarative tables

**WHEN** a Bolt version profile is built  
**THEN** legal states, messages, transitions, side effects, and errors SHALL be
generated from a reviewable table  
**SHALL** make unreachable and missing transitions fail compile-time or CI
validation.  
**Verification:** `TEST-BOLT-013-FSM-GENERATION`

#### REQ-BOLT-014.0: Enforce version-correct connection state

**WHEN** any client message arrives  
**THEN** it SHALL execute only if valid in the current negotiated state and move
to the version-correct next state  
**SHALL** treat protocol violations consistently with the pinned oracle.  
**Verification:** `TEST-BOLT-014-FSM-TRANSITIONS`

#### REQ-BOLT-015.0: Recover with RESET and interruption

**WHEN** a query is cancelled, a failure leaves the connection in failed state,
or `RESET` jumps ahead  
**THEN** active work SHALL be cancelled and recoverable state SHALL be restored
where the protocol allows  
**SHALL** prevent stale records or transaction handles from crossing reset.  
**Verification:** `TEST-BOLT-015-RESET-RECOVERY`

#### REQ-BOLT-016.0: Bound every connection

**WHEN** a connection is open  
**THEN** buffers, queued messages, active streams, transactions, idle time, and
CPU work SHALL be individually bounded  
**SHALL** expose the bound values in configuration and receipts.  
**Verification:** `TEST-BOLT-016-CONNECTION-BOUNDS`

#### REQ-BOLT-017.0: Apply cancellation across the whole pipeline

**WHEN** a socket closes, deadline expires, reset arrives, or administrator
cancels a query  
**THEN** cancellation SHALL propagate through parsing, planning, storage,
algorithm execution, and result production  
**SHALL** complete resource cleanup within a configured grace period.  
**Verification:** `TEST-BOLT-017-CANCELLATION`

#### REQ-BOLT-018.0: Never materialize unbounded result sets

**WHEN** a query can produce more rows than one configured response window  
**THEN** the Bolt layer SHALL pull bounded batches from `ResultProducer` in
response to demand  
**SHALL NOT** construct a `Vec` containing the complete result before the first
`RECORD`.  
**Verification:** `TEST-BOLT-018-STREAMING`

### Authentication, Sessions, Transactions, and Results

#### REQ-SESS-001.0: Authenticate at the version-correct phase

**WHEN** a version uses combined `HELLO` authentication or split
`HELLO`/`LOGON` authentication  
**THEN** the configured authentication provider SHALL receive normalized
credentials at the correct state transition  
**SHALL** avoid logging credentials or returning secret-bearing diagnostics.  
**Verification:** `TEST-SESS-001-AUTH-PHASE`

#### REQ-SESS-002.0: Support re-authentication and logoff

**WHEN** the negotiated version allows `LOGOFF` and subsequent `LOGON`  
**THEN** the prior identity, authorization cache, impersonation, and active
session state SHALL be cleared before the new identity is accepted  
**SHALL** reject re-authentication during an incompatible active transaction.  
**Verification:** `TEST-SESS-002-REAUTH`

#### REQ-SESS-003.0: Preserve session metadata

**WHEN** a driver supplies user agent, routing context, notifications config,
bookmarks, database, access mode, impersonated user, transaction metadata, or
timeout  
**THEN** supported fields SHALL reach the query envelope unchanged in meaning  
**SHALL** reject unsupported security-sensitive fields rather than ignore them.  
**Verification:** `TEST-SESS-003-METADATA`

#### REQ-SESS-004.0: Select databases explicitly

**WHEN** a request names a database or relies on home-database resolution  
**THEN** the catalog SHALL resolve one exact Knight Bus dataset namespace and
return version-correct resolution metadata  
**SHALL** reject ambiguity and unauthorized namespaces.  
**Verification:** `TEST-SESS-004-DATABASE-SELECTION`

#### REQ-SESS-005.0: Implement auto-commit transactions

**WHEN** `RUN` occurs outside an explicit transaction  
**THEN** the query SHALL execute in one auto-commit transaction whose commit or
rollback is tied to result consumption and failure semantics  
**SHALL** release its snapshot if the result is discarded or connection dies.  
**Verification:** `TEST-SESS-005-AUTOCOMMIT`

#### REQ-SESS-006.0: Implement explicit transaction lifecycle

**WHEN** `BEGIN`, one or more `RUN`/`PULL` exchanges, and `COMMIT` or `ROLLBACK`
are issued legally  
**THEN** all statements SHALL share the declared transactional snapshot and
mutation context  
**SHALL** reject use after close, cross-session handles, and illegal nesting.  
**Verification:** `TEST-SESS-006-EXPLICIT-TX`

#### REQ-SESS-007.0: Preserve transaction visibility

**WHEN** concurrent readers and writers execute  
**THEN** visibility, read-your-writes, rollback, isolation, and constraint
failure behavior SHALL match the declared transaction profile  
**SHALL** document any deliberate isolation difference from Neo4j.  
**Verification:** `TEST-SESS-007-VISIBILITY`

#### REQ-SESS-008.0: Support bookmarks causally

**WHEN** a session supplies bookmarks  
**THEN** the selected namespace SHALL wait for or reject unavailable causal
positions according to configured policy  
**SHALL** return a new bookmark only after successful commit.  
**Verification:** `TEST-SESS-008-BOOKMARKS`

#### REQ-SESS-009.0: Support routing responses

**WHEN** `ROUTE` or a `neo4j://` driver requests a routing table  
**THEN** the server SHALL return bounded, valid reader/writer/router addresses
with TTL for the selected namespace  
**SHALL** identify single-node deployments truthfully.  
**Verification:** `TEST-SESS-009-ROUTING`

#### REQ-SESS-010.0: Preserve query identity across RUN and PULL

**WHEN** a connection has one or more legal result streams  
**THEN** `qid`, `n`, `has_more`, `t_first`, `t_last`, field names, and stream
selection SHALL follow the negotiated protocol  
**SHALL** prevent records from one stream appearing in another.  
**Verification:** `TEST-SESS-010-QID-PULL`

#### REQ-SESS-011.0: Implement DISCARD without hidden execution

**WHEN** a client discards a result  
**THEN** unnecessary downstream work SHALL stop, resources SHALL be released,
and summary semantics SHALL remain valid  
**SHALL NOT** compute all discarded rows unless transaction semantics require
completion.  
**Verification:** `TEST-SESS-011-DISCARD`

#### REQ-SESS-012.0: Return version-correct summaries

**WHEN** query execution ends  
**THEN** counters, query type, plan/profile, notifications or statuses, database,
bookmark, timings, and GQL diagnostics SHALL match the declared version and
query behavior  
**SHALL** omit fields that the version does not define.  
**Verification:** `TEST-SESS-012-SUMMARY`

#### REQ-SESS-013.0: Support official driver lifecycle

**WHEN** each pinned Java, Go, Python, JavaScript, and .NET driver performs
driver creation, connectivity verification, session use, managed transaction,
result consumption, retry, and close  
**THEN** the declared profile SHALL pass without application-specific patches  
**SHALL** record driver and protocol versions in the test receipt.  
**Verification:** `TEST-SESS-013-DRIVER-MATRIX`

#### REQ-SESS-014.0: Preserve retry classification

**WHEN** a transient, client, database, security, or resource failure occurs  
**THEN** error codes and retriability SHALL cause official drivers to make the
same retry decision as the pinned Neo4j oracle  
**SHALL** never label deterministic admission rejection as an unbounded retry.  
**Verification:** `TEST-SESS-014-RETRY-CLASSIFICATION`

#### REQ-SESS-015.0: Version connection hints and server-side routing

**WHEN** a protocol version exposes connection hints, advertised addresses,
server-side routing, keepalive, receive timeout, or telemetry negotiation  
**THEN** supported fields and behavior SHALL match the negotiated version and
routing profile  
**SHALL** ignore or reject unknown hints only as that version specifies.  
**Verification:** `TEST-SESS-015-CONNECTION-HINTS`

### Cypher Lexing, Parsing, and Semantics

#### REQ-CYPHER-001.0: Select Cypher 5 or Cypher 25 explicitly

**WHEN** a query has a `CYPHER 5` or `CYPHER 25` pre-parser option, or the
database has a configured default language  
**THEN** the matching frozen grammar and semantic profile SHALL be selected  
**SHALL** include the language version in cache keys and receipts.  
**Verification:** `TEST-CYPHER-001-VERSION-SELECTION`

#### REQ-CYPHER-002.0: Preserve source spans and original text

**WHEN** text is lexed and parsed  
**THEN** every token and AST node SHALL retain byte offsets and line/column
locations sufficient for Neo4j-shaped diagnostics  
**SHALL** preserve Unicode and escaped identifier boundaries.  
**Verification:** `TEST-CYPHER-002-SOURCE-SPANS`

#### REQ-CYPHER-003.0: Implement complete declared grammar coverage

**WHEN** every grammar production in the pinned Cypher 5 or 25 manifest is
generated or enumerated  
**THEN** each production SHALL have positive and malformed fixtures  
**SHALL** leave no parser branch absent from the grammar ledger.  
**Verification:** `TEST-CYPHER-003-GRAMMAR-COVERAGE`

#### REQ-CYPHER-004.0: Reject malformed syntax deterministically

**WHEN** query text is malformed, truncated, ambiguous, deeply nested, or
adversarial  
**THEN** parsing SHALL terminate within configured time and memory and return a
stable syntax diagnostic  
**SHALL NOT** panic, recurse without a bound, or attempt execution.  
**Verification:** `TEST-CYPHER-004-SYNTAX-FAILURES`

#### REQ-CYPHER-005.0: Resolve variables and scopes

**WHEN** clauses introduce, alias, shadow, import, export, or reference variables  
**THEN** semantic analysis SHALL enforce the version-correct scope rules for
queries, subqueries, comprehensions, and procedure yields  
**SHALL** reject undefined, conflicting, or illegally shadowed names.  
**Verification:** `TEST-CYPHER-005-SCOPES`

#### REQ-CYPHER-006.0: Implement Cypher type and null semantics

**WHEN** expressions are type-checked and evaluated  
**THEN** scalar, list, map, node, relationship, path, temporal, spatial, vector,
`null`, and dynamic types SHALL follow the selected language profile  
**SHALL** preserve three-valued boolean and null-propagation behavior.  
**Verification:** `TEST-CYPHER-006-TYPES-NULLS`

#### REQ-CYPHER-007.0: Bind parameters independently of literals

**WHEN** a parameterized query is compiled and executed with legal values  
**THEN** values SHALL bind without text substitution and with version-correct
type conversion  
**SHALL** keep value-independent plans reusable where semantics permit.  
**Verification:** `TEST-CYPHER-007-PARAMETERS`

#### REQ-CYPHER-008.0: Implement MATCH pattern semantics

**WHEN** `MATCH` contains node patterns, relationship patterns, labels, types,
properties, direction, repeated relationships, or path variables  
**THEN** matching SHALL preserve Cypher relationship uniqueness and row
multiplicity rules  
**SHALL** agree with the differential tiny-graph oracle.  
**Verification:** `TEST-CYPHER-008-MATCH`

#### REQ-CYPHER-009.0: Implement OPTIONAL MATCH semantics

**WHEN** an optional pattern has no match or includes an attached predicate  
**THEN** the row SHALL be null-extended at the correct boundary  
**SHALL** distinguish pattern predicates from later `WHERE` filtering.  
**Verification:** `TEST-CYPHER-009-OPTIONAL-MATCH`

#### REQ-CYPHER-010.0: Implement variable-length and quantified paths

**WHEN** a query uses supported variable-length, quantified path, or quantified
relationship syntax  
**THEN** bounds, group variables, uniqueness, zero-length paths, and row
multiplicity SHALL match the selected Cypher version  
**SHALL** pass resource admission before unbounded expansion.  
**Verification:** `TEST-CYPHER-010-QUANTIFIED-PATHS`

#### REQ-CYPHER-011.0: Implement shortest path families

**WHEN** a query uses legacy shortest path or declared Cypher 25 path selectors  
**THEN** path selection, ties, predicates, and no-path results SHALL match the
pinned oracle  
**SHALL** select a bounded specialized path executor where possible.  
**Verification:** `TEST-CYPHER-011-SHORTEST-PATH`

#### REQ-CYPHER-012.0: Implement filtering and predicates

**WHEN** `WHERE` or a predicate expression uses comparison, boolean, list,
string, regex, type, existence, label/type, or quantified predicates  
**THEN** precedence, coercion, short-circuit visibility, and null behavior SHALL
match the selected language  
**SHALL** not reorder impure procedure calls across predicates.  
**Verification:** `TEST-CYPHER-012-PREDICATES`

#### REQ-CYPHER-013.0: Implement projection and row shaping

**WHEN** a query uses `RETURN`, `WITH`, aliases, `DISTINCT`, wildcard, map
projection, or expression projection  
**THEN** column names, values, row multiplicity, scope, and order guarantees
SHALL match Cypher semantics  
**SHALL** return Bolt fields in projection order.  
**Verification:** `TEST-CYPHER-013-PROJECTION`

#### REQ-CYPHER-014.0: Implement ordering and pagination

**WHEN** a query uses `ORDER BY`, `SKIP`, `OFFSET`, or `LIMIT`  
**THEN** sort keys, null ordering, expression scope, stable tie policy where
defined, and parameterized bounds SHALL match the selected profile  
**SHALL** use top-k or bounded external sort when admitted.  
**Verification:** `TEST-CYPHER-014-ORDER-PAGINATION`

#### REQ-CYPHER-015.0: Implement aggregation

**WHEN** aggregate and non-aggregate expressions share a projection  
**THEN** grouping keys, empty-input behavior, `DISTINCT`, null handling,
numeric behavior, and aggregate outputs SHALL match the oracle  
**SHALL** select hash, sort, streaming, or spill aggregation under budget.  
**Verification:** `TEST-CYPHER-015-AGGREGATION`

#### REQ-CYPHER-016.0: Implement UNWIND and list semantics

**WHEN** lists, comprehensions, slices, indexing, reductions, pattern
comprehensions, or `UNWIND` are used  
**THEN** element order, null behavior, empty lists, and row multiplication SHALL
match the selected profile  
**SHALL** bound intermediate collections.  
**Verification:** `TEST-CYPHER-016-LISTS-UNWIND`

#### REQ-CYPHER-017.0: Implement subqueries and existential forms

**WHEN** `CALL {}`, `EXISTS {}`, `COUNT {}`, `COLLECT {}`, or other declared
subquery expressions are used  
**THEN** import, correlation, cardinality, side effects, and scope SHALL match
the selected profile  
**SHALL** prevent accidental materialization of all correlated rows.  
**Verification:** `TEST-CYPHER-017-SUBQUERIES`

#### REQ-CYPHER-018.0: Implement UNION semantics

**WHEN** query parts are combined with `UNION` or `UNION ALL`  
**THEN** column compatibility, names, duplicate handling, ordering boundaries,
and errors SHALL match the selected profile  
**SHALL** stream `UNION ALL` where no later operator requires materialization.  
**Verification:** `TEST-CYPHER-018-UNION`

#### REQ-CYPHER-019.0: Implement expression and function inventory

**WHEN** any built-in scalar, aggregate, graph, list, map, math, string, temporal,
spatial, vector, conversion, predicate, or nondeterministic function in the
manifest is invoked  
**THEN** arity, types, values, nulls, errors, and determinism classification
SHALL match the pinned oracle  
**SHALL** reject unregistered functions during semantic analysis.  
**Verification:** `TEST-CYPHER-019-FUNCTION-INVENTORY`

#### REQ-CYPHER-020.0: Implement CREATE and DELETE

**WHEN** the transaction profile executes `CREATE`, `DELETE`, or `DETACH DELETE`  
**THEN** entity creation, identity, property assignment, relationship rules,
deletion constraints, counters, and rollback SHALL match declared semantics  
**SHALL** remain unavailable in read-only profiles.  
**Verification:** `TEST-CYPHER-020-CREATE-DELETE`

#### REQ-CYPHER-021.0: Implement SET and REMOVE

**WHEN** `SET` or `REMOVE` changes labels, properties, maps, or dynamic property
keys in a supported version  
**THEN** update ordering, null-removal, counters, constraint checking, and
rollback SHALL match the oracle  
**SHALL** use the transactional storage interface only.  
**Verification:** `TEST-CYPHER-021-SET-REMOVE`

#### REQ-CYPHER-022.0: Implement MERGE atomically

**WHEN** concurrent transactions execute `MERGE` with `ON CREATE` or `ON MATCH`  
**THEN** matching, creation, locking or conflict control, updates, and counters
SHALL satisfy declared atomicity and constraint semantics  
**SHALL** prevent duplicate committed matches under the required uniqueness
constraints.  
**Verification:** `TEST-CYPHER-022-MERGE`

#### REQ-CYPHER-023.0: Implement FOREACH and LOAD CSV where profiled

**WHEN** transaction profiles enable `FOREACH` or `LOAD CSV`  
**THEN** update scope, batching, URI policy, encoding, field parsing, failure,
and rollback behavior SHALL match the feature manifest  
**SHALL** keep network and filesystem access disabled by default.  
**Verification:** `TEST-CYPHER-023-FOREACH-LOADCSV`

#### REQ-CYPHER-024.0: Implement schema commands by capability

**WHEN** indexes or constraints are created, shown, altered, or dropped in an
enabled profile  
**THEN** syntax, validation, lifecycle, conflicts, names, and result columns
SHALL match the declared compatibility contract  
**SHALL** reject commands unsupported by the active storage engine.  
**Verification:** `TEST-CYPHER-024-SCHEMA`

#### REQ-CYPHER-025.0: Isolate administrative Cypher

**WHEN** database, user, role, privilege, server, alias, composite-database, or
transaction administration is requested  
**THEN** a separate administrative capability SHALL handle or reject it before
graph planning  
**SHALL NOT** emulate success for unavailable distributed or enterprise
behavior.  
**Verification:** `TEST-CYPHER-025-ADMIN`

#### REQ-CYPHER-026.0: Produce Neo4j-shaped diagnostics

**WHEN** pre-parsing, syntax, semantics, planning, execution, transaction,
security, or resource admission fails  
**THEN** the result SHALL contain version-correct Neo4j code or GQL status,
description, position, diagnostic record, and cause where defined  
**SHALL** preserve a stable Knight Bus internal diagnostic ID.  
**Verification:** `TEST-CYPHER-026-DIAGNOSTICS`

#### REQ-CYPHER-027.0: Support EXPLAIN and PROFILE

**WHEN** a query is prefixed with `EXPLAIN` or `PROFILE`  
**THEN** `EXPLAIN` SHALL avoid data mutation and `PROFILE` SHALL report measured
operator activity using compatible result metadata  
**SHALL** identify Knight Bus-specific operators without falsely naming Neo4j
internals.  
**Verification:** `TEST-CYPHER-027-EXPLAIN-PROFILE`

#### REQ-CYPHER-028.0: Implement graph selection and composite queries

**WHEN** a query uses `USE`, graph lookup functions, or a declared composite
database/federated query feature  
**THEN** graph selection, scope, parameter flow, transaction boundaries, result
composition, and authorization SHALL match the active profile  
**SHALL** reject distributed execution when only a single namespace is
available.  
**Verification:** `TEST-CYPHER-028-GRAPH-SELECTION`

#### REQ-CYPHER-029.0: Implement transactional subquery batching

**WHEN** a supported query uses `CALL { ... } IN TRANSACTIONS` and its error,
concurrency, reporting, or batching options  
**THEN** input partitioning, commit boundaries, retries, side effects, status
rows, and failure behavior SHALL match the selected language profile  
**SHALL** admit each batch under explicit transaction and resource limits.  
**Verification:** `TEST-CYPHER-029-CALL-IN-TRANSACTIONS`

### Logical Planning and Runtime

#### REQ-PLAN-001.0: Normalize into a stable logical IR

**WHEN** semantic analysis succeeds  
**THEN** the AST SHALL lower into a versioned algebra containing scans, seeks,
expands, joins, applies, filters, projections, aggregates, sorts, limits,
subqueries, procedures, updates, and administration capability nodes  
**SHALL** serialize canonically without parser-library or Neo4j implementation
types.  
**Verification:** `TEST-PLAN-001-LOGICAL-IR`

#### REQ-PLAN-002.0: Preserve semantic identity in canonical plans

**WHEN** whitespace, comments, or irrelevant parameter values differ  
**THEN** canonical plan identity SHALL remain equal where execution semantics
are equal  
**SHALL** change when language version, schema, capability, parameter types, or
operator semantics change.  
**Verification:** `TEST-PLAN-002-PLAN-IDENTITY`

#### REQ-PLAN-003.0: Apply proven rewrites only

**WHEN** logical rewrite rules transform predicates, projections, paths,
subqueries, updates, or aggregations  
**THEN** each rule SHALL have an equivalence property test and differential
fixture  
**SHALL** preserve nulls, multiplicity, ordering contracts, and side effects.  
**Verification:** `TEST-PLAN-003-REWRITE-EQUIVALENCE`

#### REQ-PLAN-004.0: Maintain catalog statistics with confidence

**WHEN** planning uses node counts, relationship counts, label/type counts,
degrees, property distributions, index selectivity, or correlations  
**THEN** every statistic SHALL carry source, snapshot, freshness, and confidence  
**SHALL** fall back conservatively when evidence is missing.  
**Verification:** `TEST-PLAN-004-STATISTICS`

#### REQ-PLAN-005.0: Cost candidate plans under resource budgets

**WHEN** multiple legal plans exist  
**THEN** the planner SHALL estimate rows, random/sequential IO, CPU work,
temporary bytes, resident bytes, spill bytes, and parallelism  
**SHALL** reject plans whose hard memory bound exceeds the admitted budget.  
**Verification:** `TEST-PLAN-005-RESOURCE-COST`

#### REQ-PLAN-006.0: Choose specialized plans by proven shape

**WHEN** a logical plan matches a registered walk, path, GDS, aggregation, or
other algorithm specialization  
**THEN** a semantics-preserving matcher SHALL select that implementation only
after all preconditions are proven  
**SHALL** retain the generic plan as a differential oracle where feasible.  
**Verification:** `TEST-PLAN-006-SPECIALIZATION`

#### REQ-PLAN-007.0: Keep storage capabilities explicit

**WHEN** planning requires an index, reverse adjacency, property column,
transactional mutation, spill device, or algorithm-shaped projection  
**THEN** the plan SHALL name that capability and its format version  
**SHALL** fail admission before opening a result stream if the capability is
absent.  
**Verification:** `TEST-PLAN-007-CAPABILITY-ADMISSION`

#### REQ-PLAN-008.0: Cache plans safely

**WHEN** a compiled plan is cached and reused  
**THEN** the key SHALL include normalized query, Cypher version, parameter
schema, database, schema/catalog generation, capability profile, and relevant
configuration  
**SHALL** invalidate on incompatible changes.  
**Verification:** `TEST-PLAN-008-CACHE-KEYS`

#### REQ-PLAN-009.0: Make planning deterministic

**WHEN** identical inputs, catalog snapshot, and configuration are compiled on
the same target architecture  
**THEN** the selected logical and physical plan hashes SHALL be identical  
**SHALL** record any deliberately randomized search seed.  
**Verification:** `TEST-PLAN-009-DETERMINISM`

#### REQ-PLAN-010.0: Bound planning itself

**WHEN** a query has many joins, patterns, alternatives, or nested subqueries  
**THEN** planner time, alternatives, memo bytes, and recursion SHALL obey
configured limits  
**SHALL** return a typed complexity/admission failure instead of exhausting the
server.  
**Verification:** `TEST-PLAN-010-PLANNER-BOUNDS`

#### REQ-EXEC-001.0: Execute a complete relational-graph operator algebra

**WHEN** an admitted physical plan contains any declared read operator  
**THEN** the runtime SHALL implement scans, seeks, expands, filters, projections,
joins, applies, unions, aggregations, sorting, pagination, optional rows, paths,
and procedure streams  
**SHALL** preserve row schema, multiplicity, nulls, and required ordering.  
**Verification:** `TEST-EXEC-001-OPERATOR-MATRIX`

#### REQ-EXEC-002.0: Use bounded pull or morsel execution

**WHEN** downstream Bolt demand is finite  
**THEN** operators SHALL produce no more than a bounded row/byte window beyond
that demand, except for explicitly blocking operators  
**SHALL** account for blocking-operator memory before admission.  
**Verification:** `TEST-EXEC-002-BOUNDED-PIPELINE`

#### REQ-EXEC-003.0: Spill blocking operators predictably

**WHEN** sort, aggregation, distinct, join, path frontier, or materialization
would exceed its in-memory budget  
**THEN** the operator SHALL spill using a deterministic partition/run format or
fail according to policy  
**SHALL** never exceed the configured peak-memory envelope by more than the
measured allocator tolerance.  
**Verification:** `TEST-EXEC-003-SPILL`

#### REQ-EXEC-004.0: Preserve deterministic result semantics

**WHEN** Cypher defines order or a deterministic tie-break is configured  
**THEN** parallel execution SHALL produce stable ordered values and canonical
receipts  
**SHALL** label order as unspecified when Cypher does not guarantee it.  
**Verification:** `TEST-EXEC-004-DETERMINISM`

#### REQ-EXEC-005.0: Schedule parallelism within a work budget

**WHEN** an operator can run concurrently  
**THEN** task count, queue depth, CPU permits, IO permits, and memory permits
SHALL be acquired from the query resource budget  
**SHALL** avoid oversubscription across concurrent queries.  
**Verification:** `TEST-EXEC-005-PARALLEL-BUDGET`

#### REQ-EXEC-006.0: Separate async IO from CPU scheduling

**WHEN** storage access blocks or uses `io_uring` on supported Linux targets  
**THEN** IO completion SHALL wake bounded query work without blocking CPU worker
threads  
**SHALL** provide a portable fallback with identical semantics.  
**Verification:** `TEST-EXEC-006-IO-SCHEDULING`

#### REQ-EXEC-007.0: Support deadlines and cooperative cancellation

**WHEN** a query deadline or cancellation token fires  
**THEN** every long-running operator and algorithm SHALL observe it at a bounded
work interval and unwind safely  
**SHALL** report cleanup latency and leaked-resource count.  
**Verification:** `TEST-EXEC-007-DEADLINE-CANCEL`

#### REQ-EXEC-008.0: Isolate query failures

**WHEN** one query panics internally, encounters corrupt data, exceeds budget,
or returns an execution error  
**THEN** the connection or query SHALL fail according to severity without
corrupting other sessions or committed data  
**SHALL** convert caught internal failures into stable diagnostics.  
**Verification:** `TEST-EXEC-008-FAILURE-ISOLATION`

#### REQ-EXEC-009.0: Execute writes through transactional operators

**WHEN** an admitted plan contains mutations  
**THEN** all entity, property, label, relationship, schema, and index changes
SHALL flow through a transaction-local write set and commit protocol  
**SHALL NOT** mutate mmap analytical snapshots in place.  
**Verification:** `TEST-EXEC-009-WRITE-OPERATORS`

#### REQ-EXEC-010.0: Return complete execution summaries

**WHEN** a query finishes or fails  
**THEN** operator rows, db hits or Knight Bus equivalent, bytes read, bytes
spilled, peak memory, CPU time, wall time, cancellation, and update counters
SHALL be finalized  
**SHALL** feed both Bolt summary metadata and the proof receipt.  
**Verification:** `TEST-EXEC-010-SUMMARY-METRICS`

### Procedures, Functions, and GDS

#### REQ-PROC-001.0: Register typed procedure signatures

**WHEN** a procedure or user function package is loaded  
**THEN** its qualified name, mode, inputs, defaults, outputs, deprecations,
version, privileges, determinism, and resource estimator SHALL be validated  
**SHALL** reject duplicate or incompatible registrations.  
**Verification:** `TEST-PROC-001-REGISTRY`

#### REQ-PROC-002.0: Implement CALL and YIELD semantics

**WHEN** a registered procedure is invoked directly or within a query  
**THEN** argument binding, defaults, `YIELD`, filtering, scope, row cardinality,
side effects, and errors SHALL match its declared schema  
**SHALL** stream rows under Bolt demand.  
**Verification:** `TEST-PROC-002-CALL-YIELD`

#### REQ-PROC-003.0: Isolate plugin execution

**WHEN** third-party or optional procedure code runs  
**THEN** capability, filesystem, network, time, memory, and panic boundaries
SHALL follow plugin policy  
**SHALL** prevent a plugin from bypassing transaction and resource governance.  
**Verification:** `TEST-PROC-003-PLUGIN-ISOLATION`

#### REQ-PROC-004.0: Version APOC separately

**WHEN** APOC compatibility is requested  
**THEN** the manifest SHALL identify the exact APOC procedure/function subset
and version  
**SHALL NOT** block core Cypher release on complete APOC parity.  
**Verification:** `TEST-PROC-004-APOC-PROFILE`

#### REQ-PROC-005.0: Preserve procedure result schemas

**WHEN** official clients consume procedure outputs  
**THEN** names, types, order, nullability, stats fields, and mode-specific rows
SHALL match the pinned client/oracle contract  
**SHALL** detect schema drift in CI.  
**Verification:** `TEST-PROC-005-RESULT-SCHEMA`

#### REQ-GDS-001.0: Import a versioned GDS inventory

**WHEN** a GDS profile is built  
**THEN** every discovered local GDS procedure and function SHALL appear as
implemented, mapped, parsed-only, or unsupported with reason  
**SHALL** pin the GDS source SHA used to derive the inventory.  
**Verification:** `TEST-GDS-001-INVENTORY`

#### REQ-GDS-002.0: Preserve graph catalog behavior

**WHEN** graph project, list, exists, drop, size, or property-stream procedures
are called  
**THEN** names, lifecycle, estimates, schema, validation, concurrency, and result
rows SHALL match the declared GDS profile  
**SHALL** reuse the existing Knight Bus graph projection catalog where valid.  
**Verification:** `TEST-GDS-002-CATALOG`

#### REQ-GDS-003.0: Route algorithms by name and mode

**WHEN** `CALL gds.<algorithm>.<stream|stats|mutate|write|estimate>` is compiled  
**THEN** semantic analysis SHALL bind the exact signature and lower to a
registered Knight Bus algorithm implementation  
**SHALL** reject unavailable modes before allocating algorithm memory.  
**Verification:** `TEST-GDS-003-MODE-ROUTING`

#### REQ-GDS-004.0: Use algorithm-specific storage profiles

**WHEN** a GDS algorithm is admitted  
**THEN** it SHALL select a versioned projection/layout profile optimized for its
access pattern, such as CSR/CSC, edge stream, frontier blocks, dense vectors,
compressed labels, or spill partitions  
**SHALL** declare conversion cost, peak RAM, persistent bytes, and access
preconditions.  
**Verification:** `TEST-GDS-004-STORAGE-PROFILE`

#### REQ-GDS-005.0: Differentially verify algorithm outputs

**WHEN** a GDS implementation is marked compatible  
**THEN** exact algorithms SHALL match exact outputs and floating-point or
nondeterministic algorithms SHALL satisfy declared tolerance and invariant
checks across adversarial graph families  
**SHALL** compare all supported result modes.  
**Verification:** `TEST-GDS-005-DIFFERENTIAL`

#### REQ-GDS-006.0: Bound algorithm memory before execution

**WHEN** an algorithm request includes graph, configuration, concurrency, and
result mode  
**THEN** its estimator SHALL produce a conservative peak-memory interval and
admit, spill, downgrade, queue, or reject under policy  
**SHALL** report estimate error after execution.  
**Verification:** `TEST-GDS-006-MEMORY-ADMISSION`

#### REQ-GDS-007.0: Stream large algorithm results

**WHEN** stream mode produces more than one result window  
**THEN** algorithm output SHALL flow through bounded batches to Bolt and respond
to cancellation  
**SHALL NOT** duplicate the full output in both algorithm and Bolt buffers.  
**Verification:** `TEST-GDS-007-RESULT-STREAMING`

### Storage and Resource Governance

#### REQ-STORE-001.0: Expose immutable snapshot reads

**WHEN** a read plan opens a graph snapshot  
**THEN** stable node, relationship, label/type, property, index, and adjacency
views SHALL remain valid for the query lifetime  
**SHALL** identify dataset and snapshot hashes in the receipt.  
**Verification:** `TEST-STORE-001-SNAPSHOT`

#### REQ-STORE-002.0: Expose directional adjacency capabilities

**WHEN** a plan requires outgoing, incoming, undirected, typed, or filtered
expansion  
**THEN** the storage profile SHALL provide the exact capability or declare its
bounded derivation cost  
**SHALL** never assume reverse adjacency exists.  
**Verification:** `TEST-STORE-002-ADJACENCY`

#### REQ-STORE-003.0: Separate logical identity from physical encoding

**WHEN** nodes, relationships, properties, and paths cross parser, runtime,
procedure, or Bolt boundaries  
**THEN** stable logical IDs and element IDs SHALL survive changes in compact
physical ordinals and storage formats  
**SHALL** validate stale or cross-dataset IDs.  
**Verification:** `TEST-STORE-003-IDENTITY`

#### REQ-STORE-004.0: Support transactional mutation storage

**WHEN** write profiles are enabled  
**THEN** WAL, recovery, atomic commit, rollback, constraints, indexes, and
snapshot publication SHALL satisfy the declared durability and isolation
contract  
**SHALL** keep analytical projections rebuildable from committed state.  
**Verification:** `TEST-STORE-004-TRANSACTIONAL-STORAGE`

#### REQ-STORE-005.0: Version every persistent format

**WHEN** a snapshot, index, spill run, WAL record, algorithm projection, or plan
cache entry is opened  
**THEN** magic, version, endianness, checksums, compatibility, and migration
policy SHALL be validated before use  
**SHALL** reject corrupt or unsupported data safely.  
**Verification:** `TEST-STORE-005-FORMAT-VERSIONING`

#### REQ-STORE-006.0: Admit queries under hard memory budgets

**WHEN** a query requests or inherits a peak-RAM limit  
**THEN** parser, planner, operator, algorithm, result, connection, and shared
cache reservations SHALL compose into one conservative bound  
**SHALL** reject execution if the bound cannot be honored.  
**Verification:** `TEST-STORE-006-HARD-RAM-BUDGET`

#### REQ-STORE-007.0: Measure resident and allocated memory separately

**WHEN** resource evidence is produced  
**THEN** heap allocation, mapped virtual bytes, resident mapped pages, shared
cache attribution, spill buffers, and process RSS SHALL be reported separately  
**SHALL NOT** equate file size, virtual mapping, and physical RAM.  
**Verification:** `TEST-STORE-007-MEMORY-ACCOUNTING`

#### REQ-STORE-008.0: Make spill a first-class storage tier

**WHEN** a query chooses a RAM-capped execution policy  
**THEN** spill location, quota, encryption, checksums, cleanup, IO budget, and
expected extra passes SHALL be fixed before execution  
**SHALL** delete abandoned spill artifacts after bounded recovery.  
**Verification:** `TEST-STORE-008-SPILL-LIFECYCLE`

#### REQ-STORE-009.0: Avoid duplicate graph representations by default

**WHEN** multiple concurrent algorithms can share an immutable compatible
projection  
**THEN** reference-counted projection pages SHALL be reused under the resource
governor  
**SHALL** charge private mutable state to each query separately.  
**Verification:** `TEST-STORE-009-PROJECTION-SHARING`

#### REQ-STORE-010.0: Predict resource use before admitting production work

**WHEN** an estimate is requested  
**THEN** the server SHALL return lower/expected/upper bounds for preparation,
peak RAM, persistent bytes, spill bytes, IO, CPU work, and output rows with a
confidence class  
**SHALL** retain estimate-versus-actual calibration history.  
**Verification:** `TEST-STORE-010-RESOURCE-ESTIMATE`

#### REQ-STORE-011.0: Expose versioned search-index capabilities

**WHEN** a plan or procedure requires token lookup, range, text, point, fulltext,
or vector search  
**THEN** the storage catalog SHALL expose exact query semantics, supported
types, consistency, update visibility, ordering, scoring, and resource cost  
**SHALL** reject or safely fall back when the required index generation is
absent or stale.  
**Verification:** `TEST-STORE-011-SEARCH-INDEXES`

### Verification, Performance, Security, and Claims

#### REQ-VERIFY-001.0: Run the openCypher TCK

**WHEN** Cypher baseline verification runs  
**THEN** all 1,615 scenarios and 276 examples in the pinned local TCK SHALL be
classified and executed where applicable  
**SHALL** publish pass, fail, unsupported, and not-applicable counts by feature.  
**Verification:** `TEST-VERIFY-001-TCK`

#### REQ-VERIFY-002.0: Differentially verify syntax and semantics

**WHEN** generated and curated queries run against the pinned Neo4j oracle  
**THEN** acceptance, diagnostics, values, rows, order where defined, side
effects, counters, and summaries SHALL be compared  
**SHALL** minimize every mismatch into a retained fixture.  
**Verification:** `TEST-VERIFY-002-CYPHER-DIFFERENTIAL`

#### REQ-VERIFY-003.0: Run Bolt protocol fixtures and TestKit

**WHEN** Bolt verification runs  
**THEN** pinned BoltStub scripts and applicable TestKit cases SHALL execute
against every declared protocol profile  
**SHALL** publish exclusions with requirement IDs and reasons.  
**Verification:** `TEST-VERIFY-003-TESTKIT`

#### REQ-VERIFY-004.0: Run all official driver matrices

**WHEN** a production compatibility profile is released  
**THEN** pinned Java, Go, Python, JavaScript, and .NET driver suites SHALL pass
the declared lifecycle and query corpus  
**SHALL** include `cypher-shell` and `neo4rs` smoke tests.  
**Verification:** `TEST-VERIFY-004-DRIVERS`

#### REQ-VERIFY-005.0: Generate adversarial graph fixtures

**WHEN** graph semantics or algorithms are tested  
**THEN** fixtures SHALL include empty, singleton, self-loop, parallel-edge,
cycle, disconnected, star, chain, diamond, clique, bipartite, skew-degree,
duplicate-property, null-property, and Unicode cases  
**SHALL** enumerate tiny graphs exhaustively where tractable.  
**Verification:** `TEST-VERIFY-005-GRAPH-FAMILIES`

#### REQ-VERIFY-006.0: Fuzz every untrusted boundary

**WHEN** continuous fuzzing runs  
**THEN** handshake, VarInt, chunks, PackStream, message fields, Cypher lexer,
parser, semantic analyzer, persistent formats, and procedure configs SHALL have
coverage-guided fuzz targets  
**SHALL** retain and replay all crashes and differential disagreements.  
**Verification:** `TEST-VERIFY-006-FUZZ`

#### REQ-VERIFY-007.0: Model-check state and transaction machines

**WHEN** Bolt or transaction states change  
**THEN** generated transition sequences SHALL verify legal reachability,
recovery, idempotent close, no stale handles, and no forbidden commits  
**SHALL** compare bounded traces with the protocol table.  
**Verification:** `TEST-VERIFY-007-STATE-MODEL`

#### REQ-VERIFY-008.0: Verify crash recovery

**WHEN** the process is killed at every instrumented write/commit boundary  
**THEN** restart SHALL expose either the prior or committed state according to
the durability contract, never a torn state  
**SHALL** rebuild derived analytical projections safely.  
**Verification:** `TEST-VERIFY-008-CRASH-RECOVERY`

#### REQ-VERIFY-009.0: Verify the 2 GB production-shaped slice

**WHEN** the corrected v002 dataset is loaded and all 60 queries are sent through
an official Neo4j driver over Bolt  
**THEN** results SHALL match the pinned Neo4j oracle and PMF006 contract  
**SHALL** report end-to-end wire, compile, execution, serialization, RSS, and
cold-open measurements.  
**Verification:** `TEST-VERIFY-009-V002-E2E`

#### REQ-VERIFY-010.0: Make every benchmark reproducible

**WHEN** a performance comparison is published  
**THEN** dataset hash, query corpus, binaries, configuration, machine, kernel,
filesystem, cache state, warmup, repetitions, raw samples, and statistics SHALL
be retained  
**SHALL** pair Knight Bus and Neo4j runs under equivalent conditions.  
**Verification:** `TEST-VERIFY-010-BENCH-RECEIPT`

#### REQ-PERF-001.0: Measure latency by pipeline stage

**WHEN** a query is benchmarked  
**THEN** handshake, decode, parse, semantic, plan/cache, admission, prepare,
execute, first-record, drain, and final-summary latency SHALL be separately
measured  
**SHALL** publish p50, p95, p99, p99.9, maximum, and confidence intervals.  
**Verification:** `TEST-PERF-001-STAGE-LATENCY`

#### REQ-PERF-002.0: Distinguish cold, warm, and steady-state runs

**WHEN** latency or throughput is reported  
**THEN** process cold start, dataset open, page-cache cold, plan-cache cold,
warm single-query, and concurrent steady-state conditions SHALL be labeled  
**SHALL NOT** compare a warm Knight Bus path with a cold Neo4j path.  
**Verification:** `TEST-PERF-002-CACHE-STATES`

#### REQ-PERF-003.0: Gate the compatibility overhead

**WHEN** Slice 0 runs warm on the v002 corpus  
**THEN** Bolt decode, Cypher compile/cache lookup, and record serialization SHALL
be measured against direct Knight Bus execution  
**SHALL** fail the slice gate if median compatibility overhead exceeds the
approved budget without an exception.  
**Verification:** `TEST-PERF-003-COMPAT-OVERHEAD`

#### REQ-PERF-004.0: Gate peak RAM, not only final RSS

**WHEN** a workload is admitted under a RAM tier  
**THEN** sampled and allocator-observed peak resident memory SHALL remain under
the tier plus explicit measurement tolerance  
**SHALL** include concurrent connection and output buffering.  
**Verification:** `TEST-PERF-004-PEAK-RAM`

#### REQ-PERF-005.0: Test concurrent fairness and tail latency

**WHEN** mixed short reads, long analytics, writes, and cancellations run
concurrently  
**THEN** per-class throughput, queue time, starvation, p99/p99.9, cancellation
latency, and memory isolation SHALL satisfy the workload SLO  
**SHALL** prevent one analytical query from consuming all permits.  
**Verification:** `TEST-PERF-005-MIXED-WORKLOAD`

#### REQ-PERF-006.0: Calibrate estimates continuously

**WHEN** an execution receipt is finalized  
**THEN** estimated and actual rows, RAM, spill, IO, CPU, and wall time SHALL be
compared and aggregated by plan/operator/algorithm profile  
**SHALL** alert when upper bounds are violated or confidence degrades.  
**Verification:** `TEST-PERF-006-CALIBRATION`

#### REQ-SEC-001.0: Authorize before data access

**WHEN** a query references graph namespaces, labels, relationship types,
properties, procedures, functions, or administration capabilities  
**THEN** authorization SHALL be checked before the corresponding storage or
plugin access  
**SHALL** avoid leaking existence through errors or timing where policy forbids.  
**Verification:** `TEST-SEC-001-AUTHORIZATION`

#### REQ-SEC-002.0: Protect secrets and sensitive values

**WHEN** logs, traces, receipts, errors, or profiles are emitted  
**THEN** credentials and configured sensitive parameters/properties SHALL be
redacted or hashed according to policy  
**SHALL** preserve enough structural evidence for debugging.  
**Verification:** `TEST-SEC-002-REDACTION`

#### REQ-SEC-003.0: Resist resource-exhaustion inputs

**WHEN** an unauthenticated or authenticated client sends slow, recursive,
deeply nested, high-cardinality, or expansion-heavy work  
**THEN** transport, parse, plan, execution, and output limits SHALL bound cost  
**SHALL** expose rejection as a stable security/resource event.  
**Verification:** `TEST-SEC-003-DOS-BOUNDS`

#### REQ-SEC-004.0: Isolate unsafe extensions

**WHEN** file, URL, native code, user function, or plugin execution is enabled  
**THEN** allowlists, sandboxing, capability checks, quotas, and audit events
SHALL apply  
**SHALL** keep all such capabilities disabled in the default graph profile.  
**Verification:** `TEST-SEC-004-EXTENSION-SANDBOX`

#### REQ-CLAIM-001.0: Gate every compatibility claim

**WHEN** release notes say a Bolt version, Cypher feature, driver, procedure, or
algorithm is compatible  
**THEN** a passing manifest entry, fixture count, oracle pin, and receipt link
SHALL support the claim  
**SHALL** block publication when evidence is missing.  
**Verification:** `TEST-CLAIM-001-EVIDENCE-GATE`

#### REQ-CLAIM-002.0: Report exclusions prominently

**WHEN** a profile is distributed  
**THEN** unsupported syntax, semantics, protocol versions, metadata, transaction
behavior, procedures, and storage capabilities SHALL be generated from the
manifest into user-facing compatibility notes  
**SHALL NOT** hide exclusions in source comments.  
**Verification:** `TEST-CLAIM-002-EXCLUSION-REPORT`

#### REQ-CLAIM-003.0: Keep performance claims paired

**WHEN** Knight Bus is described as faster or lower RAM than Neo4j  
**THEN** the statement SHALL name workload, scale, profile, cache state,
hardware, Neo4j version/config, metric, ratio, and confidence  
**SHALL** distinguish direct runtime from Bolt/Cypher end-to-end performance.  
**Verification:** `TEST-CLAIM-003-PAIRED-CLAIMS`

#### REQ-CLAIM-004.0: Fail closed on unknown capability

**WHEN** a query, Bolt field, structure, procedure option, storage requirement,
or semantic version is not in the active manifest  
**THEN** the request SHALL fail with a typed unsupported diagnostic before
producing partial results or writes  
**SHALL NOT** guess, ignore, or silently downgrade.  
**Verification:** `TEST-CLAIM-004-FAIL-CLOSED`

## Test Matrix

### Verification Spine

Compatibility SHALL be proven from the outside inward. A unit test alone cannot
prove that a production driver can execute a production query.

```text
L7  Production-shaped 2 GB corpus and migration applications
 ^
L6  Official Java/Go/Python/JavaScript/.NET drivers, shell, OGM, GDS client
 ^
L5  Neo4j/GDS differential oracle on generated and curated graph families
 ^
L4  openCypher TCK and Neo4j feature manifests
 ^
L3  BoltStub/TestKit versioned protocol traces
 ^
L2  Logical/physical operator and transaction model tests
 ^
L1  Codec, parser, semantic, storage, property, fuzz, and unit tests
```

A release profile is green only when every applicable layer is green. A lower
layer passing cannot waive a higher-layer mismatch.

### Required Test Artifacts

| Artifact | Purpose | Required contents |
|---|---|---|
| `compat/manifest/bolt.yaml` | Bolt version truth | Structures, messages, states, metadata, capabilities for 1.0-6.0, with 5.5 marked unused |
| `compat/manifest/cypher-5.yaml` | Frozen Cypher 5 truth | Grammar, semantic, clause, function, operator and error IDs |
| `compat/manifest/cypher-25-2026.06.yaml` | Pinned Cypher 25 truth | Same inventory plus additions/removals |
| `compat/manifest/gds-<sha>.yaml` | GDS truth | Procedure/function signatures, modes, configs, results, status |
| `compat/manifest/apoc-<sha>.yaml` | Optional APOC truth | Procedure/function subset and exclusions |
| `compat/oracles.lock` | Reproducibility | All repo SHAs, releases, images, drivers and checksums |
| `compat/provenance.yaml` | Clean-room evidence | Per-module and per-fixture origin/license category |
| `tests/bolt/` | Wire verification | Golden bytes, transition traces, TestKit adapters, fuzz seeds |
| `tests/cypher/` | Language verification | TCK adapter, grammar fixtures, semantic fixtures, minimizations |
| `tests/planner/` | Algebra verification | Rewrite properties, cost fixtures, capability/admission fixtures |
| `tests/runtime/` | Runtime verification | Operator specs, spill, cancellation, determinism, summaries |
| `tests/storage/` | Data verification | Format, crash, transaction, snapshot, identity, corruption tests |
| `tests/gds/` | Algorithm verification | Graph families, config matrices, modes, tolerances, resource bounds |
| `tests/drivers/` | Client verification | Java, Go, Python, JavaScript, .NET, shell, neo4rs runners |
| `tests/corpus/v002/` | Scale proof | 60 queries, parameters, expected hashes/rows, paired runner |
| `bench/receipts/` | Performance truth | Raw samples and canonical signed/hashed receipts |

### Requirement Traceability

Each requirement maps to its same-numbered test ID written directly beneath the
requirement. The following matrix defines the owning suite and minimum oracle.

| Requirement range | Owning suite | Minimum oracle/evidence | Release profiles |
|---|---|---|---|
| `REQ-GOV-001..008` | `compat_manifest_contract` | Manifests, lockfile, provenance audit, PMF006 | All |
| `REQ-BOLT-001..006` | `bolt_wire_contract` | Official Bolt bytes + property/fuzz tests | C0-C6 |
| `REQ-BOLT-007..008` | `bolt_transport_contract` | TLS/WebSocket clients and negative fixtures | C1-C6 |
| `REQ-BOLT-009..012` | `packstream_message_contract` | Bolt docs, BoltStub, official drivers | C0-C6 |
| `REQ-BOLT-013..018` | `bolt_fsm_stream_contract` | State model, TestKit, bounded-stream probes | C0-C6 |
| `REQ-SESS-001..004` | `session_auth_contract` | TestKit auth/session/database suites | C1-C6 |
| `REQ-SESS-005..008` | `transaction_lifecycle_contract` | Neo4j differential + crash/concurrency tests | C0, C4-C6 |
| `REQ-SESS-009..015` | `routing_result_contract` | Official drivers + TestKit | C1-C6 |
| `REQ-CYPHER-001..007` | `cypher_frontend_contract` | Pinned grammars, TCK, Neo4j diagnostics | C0-C6 |
| `REQ-CYPHER-008..019` | `cypher_read_semantics_contract` | TCK + generated tiny-graph differential | C0-C6 |
| `REQ-CYPHER-020..023` | `cypher_write_semantics_contract` | Neo4j differential + transaction model | C4-C6 |
| `REQ-CYPHER-024..029` | `cypher_schema_admin_contract` | Neo4j differential + capability negatives | C3-C6 |
| `REQ-PLAN-001..010` | `logical_planner_contract` | Canonical IR, rewrite properties, plan oracle | C0-C6 |
| `REQ-EXEC-001..010` | `physical_runtime_contract` | Runtime spec fixtures + differential results | C0-C6 |
| `REQ-PROC-001..005` | `procedure_registry_contract` | Signature manifests + client schemas | C2-C6 |
| `REQ-GDS-001..007` | `gds_compat_contract` | GDS oracle, client, graph families | C2-C6 |
| `REQ-STORE-001..003` | `snapshot_storage_contract` | v002 and generated snapshots | C0-C6 |
| `REQ-STORE-004..005` | `transaction_format_contract` | Crash injection and corruption corpus | C4-C6 |
| `REQ-STORE-006..011` | `resource_governor_contract` | Hard-limit harness + estimate calibration | C0-C6 |
| `REQ-VERIFY-001..010` | `verification_spine_contract` | TCK/TestKit/drivers/oracles/receipts | All |
| `REQ-PERF-001..006` | `paired_benchmark_contract` | Paired raw samples + stage instrumentation | All release candidates |
| `REQ-SEC-001..004` | `security_boundary_contract` | Auth matrix, secret scans, DoS and sandbox tests | C1-C6 |
| `REQ-CLAIM-001..004` | `release_claim_contract` | Generated compatibility and evidence report | All releases |

### Compatibility Matrix Dimensions

Every external behavior test SHALL declare all applicable dimensions. CI MAY
use pairwise generation for ordinary changes, but a release candidate SHALL run
the full mandatory cells.

| Dimension | Values |
|---|---|
| Bolt protocol | 1.0-4.3 historical; 4.4; 5.0-5.4; 5.6-5.8; 6.0; 5.5 unused |
| Cypher language | 5; 25 pinned to 2026.06 |
| Driver | Java; Go; Python; JavaScript; .NET; neo4rs; cypher-shell |
| Transport | TCP; TLS trusted; TLS self-signed fixture; WebSocket where enabled |
| Transaction | auto-commit; explicit commit; rollback; managed retry; cancellation |
| Access | read; write; routing read; routing write; impersonated where enabled |
| Data | empty; tiny adversarial; medium generated; 2 GB v002; larger scale fixture |
| Cache state | process cold; data cold; plan cold; warm; concurrent steady state |
| Memory tier | reject; in-memory; bounded spill; strict 5/10/20 GB tiers as applicable |
| Result consumption | pull 0; pull 1; pull N; pull all; discard; disconnect; reset |

### Bolt Test Matrix

| Surface | Positive cases | Negative/adversarial cases | Differential/client proof |
|---|---|---|---|
| Handshake | exact and ranged legacy offers; Manifest v1 | malformed magic, VarInt overflow, unoffered selection | TestKit version fixtures |
| Chunking | single, fragmented, pipelined, zero terminator | oversized, truncated, slow chunks | BoltStub byte scripts |
| PackStream | all sizes and structures per version | illegal marker, lengths, UTF-8, map key, depth | driver round trips |
| Messages | every legal message and field version | wrong arity/type/tag, unknown tag | official driver suites |
| State | every legal transition and recovery path | illegal sequences, jump-ahead races | generated model traces |
| Streaming | exact `n`, `qid`, `has_more`, discard | disconnect/reset/cancel under backpressure | driver lazy-consumption probes |
| Authentication | basic, bearer/custom if profiled, re-auth | invalid/expired credentials, logon in wrong state | TestKit auth suites |
| Routing | single-node and declared topology | bad context, missing namespace, expired table | official `neo4j://` drivers |
| Metadata | fields, counters, status, plan, timings | version-invalid or secret-bearing fields | summary snapshots |

### Cypher Test Matrix

| Family | Required positive coverage | Required negative/differential coverage |
|---|---|---|
| Pre-parser | `CYPHER 5`, `CYPHER 25`, options, EXPLAIN, PROFILE | unsupported version/options and position errors |
| Lexical | identifiers, escaped names, numbers, strings, comments, Unicode | malformed escape, invalid code point, truncation, nesting |
| Patterns | labels/types, directions, properties, paths, variable lengths | repeated rel semantics, absent variables, unsupported bounds |
| Reads | MATCH, OPTIONAL, WHERE, RETURN, WITH, UNWIND, UNION | nulls, duplicates, empty rows, scoping and cardinality mismatches |
| Expressions | every built-in family and operator precedence | type errors, overflow, null, invalid arity, nondeterminism |
| Aggregation | empty/nonempty, grouping, distinct, mixed keys | spill, skew, floating point and memory bounds |
| Subqueries | imported/correlated/unit/returning/expression forms | illegal scope, update visibility, cardinality |
| Paths | zero/one/many hops, ties, cycles, shortest families | explosion admission, uniqueness, predicate placement |
| Writes | create, delete, set, remove, merge, foreach | rollback, conflict, constraints, concurrent duplicate races |
| Schema/admin | every enabled command, graph selection, composite query, and SHOW result schema | unavailable storage/distributed/enterprise capability |
| Diagnostics | syntax, semantic, plan, execution, tx, resource | code/status/position/cause drift against oracle |

### Graph Fixture Matrix

| Fixture | Why it exists | Minimum assertions |
|---|---|---|
| Empty graph | zero-cardinality behavior | aggregates, optional rows, no-path, writes |
| Singleton | zero-hop and self identity | path length, projection, property access |
| Self-loop | relationship uniqueness | one relationship is not reused illegally |
| Parallel edges | row multiplicity | distinct edge identities and duplicate rows |
| Directed chain | bounded expansion | direction, lengths, ordering and endpoints |
| Directed cycle | termination and uniqueness | no infinite walk, correct path counts |
| Diamond | join and duplicate semantics | two paths, one endpoint, distinct behavior |
| Star/skew | cardinality and memory | high-degree expansion and admission |
| Clique | combinatorial pressure | path bounds and cancellation |
| Disconnected | component behavior | no cross-component results |
| Bipartite | join/path stress | multiplicity and algorithm invariants |
| Null/missing properties | three-valued semantics | null propagation and index behavior |
| Unicode names/values | source and encoding | spans, lookup, sort and Bolt round trip |
| Constraint conflicts | transaction correctness | failure, rollback and counters |

### Differential Comparison Rules

1. Ordered results SHALL be compared row-by-row and value-by-value.
2. Unordered results SHALL be compared as multisets, never as sets.
3. Nodes and relationships SHALL be normalized by labels/types/properties and
   stable fixture identities, not vendor-internal IDs.
4. Paths SHALL retain direction, entity sequence, and repeated-entity semantics.
5. Floating-point results SHALL use per-feature absolute/relative/ULP tolerances
   plus invariant checks; tolerance SHALL never conceal missing rows.
6. Temporal values SHALL normalize timezone representation without changing
   instants or local semantics.
7. Diagnostics SHALL compare category, status/code, source position, retriability,
   and structured fields; human message wording MAY be version-tolerant where
   Bolt 6.0 makes it unstable.
8. Query plans SHALL be compared by semantic operator intent and result, not by
   requiring Neo4j operator names or identical trees.
9. Nondeterministic functions SHALL be isolated from deterministic plan/result
   hashes and tested by valid-domain properties.
10. Every mismatch SHALL produce a minimized standalone query, fixture data,
    parameters, oracle version, Knight Bus receipt, and assigned requirement ID.

### Coverage Gates

| Metric | Slice gate | Production profile gate |
|---|---:|---:|
| Manifest items classified | 100% of slice | 100% of profile |
| Implemented items with positive test | 100% | 100% |
| Implemented items with negative test | 100% | 100% |
| Implemented items with differential evidence | 100% | 100% |
| Applicable TCK pass rate | 100% | 100% |
| Applicable TestKit/driver pass rate | 100% | 100% |
| Unexplained differential mismatches | 0 | 0 |
| Fuzz regressions | 0 | 0 |
| Corrupt-format/crash regressions | 0 where applicable | 0 |
| Hard RAM bound violations | 0 | 0 |
| Untraceable public claims | 0 | 0 |

## TDD Plan

### Delivery Rule

The implementation SHALL advance by end-to-end compatibility slices, not by
finishing one horizontal subsystem in isolation. Every slice performs:

```text
STUB -> RED -> GREEN -> REFACTOR -> VERIFY -> MANIFEST -> RECEIPT
```

- **STUB:** add the requirement IDs, external fixture, expected diagnostic or
  result, and public trait boundary.
- **RED:** run the closest official driver/oracle and prove the fixture fails for
  the expected missing capability, not for test infrastructure.
- **GREEN:** implement the smallest complete path from Bolt bytes to Knight Bus
  result or typed rejection.
- **REFACTOR:** remove duplication and protect internal crate boundaries while
  all fixtures remain green.
- **VERIFY:** run unit, property, differential, external client, resource, and
  scale tests applicable to the slice.
- **MANIFEST:** change capability state only after verification passes.
- **RECEIPT:** retain exact source, versions, data, plan, result, performance, and
  resource evidence.

No wave is complete with a mock executor behind a real driver or a real executor
behind a mock wire protocol. Each shippable wave has at least one genuine driver
request and one genuine Knight Bus storage execution.

### Dependency Order

```text
Manifest + provenance
        |
        +--> PackStream/messages --> versioned Bolt FSM --> bounded results
        |                                                  |
        +--> Cypher lexer/parser --> semantics --> logical IR
                                                     |
                                             planner/admission
                                          /          |          \
                                      walk        general read    GDS
                                          \          |          /
                                           storage + result stream
                                                     |
                                            transaction/write path
                                                     |
                                            schema/admin/historical
```

### Wave 0: Preserve and Measure Slice 0

**Objective:** Make the existing PMF006 walk adapter a reproducible baseline
before broadening it.

**STUB**

- Freeze the v002 data and 60-query corpus hashes.
- Pin the official Python driver and Neo4j oracle.
- Add a test that proves the query reaches `KnightBusBoltBackend.execute` rather
  than a test-only shortcut.
- Add a test that fails because the current `ResultStream` materializes all rows.

**RED**

- Demonstrate unsupported explicit transactions, unsupported `WHERE`, and an
  oversized result response as intentional failures.
- Capture current end-to-end latency/RSS and direct-runtime latency/RSS.

**GREEN**

- Keep the three PMF006 query shapes green.
- Emit dataset, query, logical-plan, result, and implementation hashes.
- Make unsupported features return stable typed metadata.

**REFACTOR**

- Place the current parser and Bolt backend behind `CypherFrontend` and
  `BoltQueryBackend` traits without broadening support.
- Separate query compilation from walk execution and Bolt serialization.

**VERIFY**

- Run all 60 queries through the official driver against both systems.
- Verify no oracle output enters Knight Bus execution inputs.
- Publish cold-open, warm p50/p99/max, compatibility overhead, and peak RSS.

**Exit:** `KB-C0-WALK` manifest is complete and every PMF006 requirement is
linked to PMF007.

### Wave 1: Build the Manifest and Provenance Compiler

**Objective:** Make scope machine-readable before adding behavior.

**STUB**

- Add schemas for Bolt, Cypher, procedures, storage capabilities, oracles, and
  provenance.
- Add generation tests for docs, feature IDs, test IDs, and state tables.

**RED**

- Import the local inventories and prove CI fails on unclassified items, missing
  tests, duplicate IDs, unknown versions, and absent licenses.

**GREEN**

- Classify every item in the first C0/C1 target.
- Generate Rust enums/tables, Markdown compatibility notes, and test parameters.

**REFACTOR**

- Keep source-specific extraction outside the stable manifest schema.
- Make generated artifacts reproducible byte-for-byte.

**VERIFY**

- Regenerate twice in clean worktrees and compare hashes.
- Run provenance and orphan-claim audits.

**Exit:** No support claim can exist outside the manifest.

### Wave 2: Replace Materialized Bolt Results

**Objective:** Turn the current Bolt seed into a bounded server foundation.

**STUB**

- Define `RecordDemand`, `RecordBatch`, `ResultProducer`, cancellation, summary,
  and lifecycle contracts.
- Add driver tests for `PULL 0`, `PULL 1`, `PULL N`, `DISCARD`, disconnect, and
  `RESET` during a large synthetic result.

**RED**

- Prove `boltr 0.2.0` fails the large-result memory test because it returns
  `Vec<BoltRecord>`.

**GREEN**

- Fork, adapt, or replace the result path so no full result vector exists.
- Feed the existing walk executor into bounded record batches.

**REFACTOR**

- Move dependency-specific types behind `kb-bolt-message` and
  `kb-result-stream`.
- Centralize per-connection byte and row permits.

**VERIFY**

- Run a result much larger than the RAM window while a client consumes one row
  at a time.
- Verify peak memory plateaus, cancellation releases resources, and summaries
  remain correct.

**Exit:** C0 is genuinely streaming and `REQ-BOLT-018` is green.

### Wave 3: Implement the Current Bolt Spine

**Objective:** Support production-current drivers independently of broad Cypher.

**Order:** 5.4 seed -> 5.7 -> 5.8 -> 6.0 -> 5.0-5.6 gaps -> 4.4.

**STUB**

- Generate codec/message/FSM tests for every current version manifest entry.
- Import applicable BoltStub and TestKit traces as oracle fixtures.

**RED**

- Run official drivers and classify failures by handshake, structure, message,
  state, metadata, auth, routing, stream, or retry behavior.

**GREEN**

- Implement one coherent version at a time, including all its state and error
  behavior.
- Keep query execution restricted to the active Cypher profile.

**REFACTOR**

- Share only manifest-proven common behavior across versions.
- Generate state transitions and structure/message dispatch from tables.

**VERIFY**

- Run model traces, fuzzers, TestKit, and all official drivers per version.
- Verify TLS, backpressure, reset, and failure recovery under concurrency.

**Exit:** Current drivers can connect and receive correct success/failure for a
small capability probe and C0 query on every current protocol profile.

### Wave 4: Freeze Cypher 5 Syntax and Diagnostics

**Objective:** Parse all Cypher 5, while executing only declared features.

**STUB**

- Generate one positive and malformed case per grammar production.
- Add TCK parser-only and Neo4j syntax-differential adapters.

**RED**

- Measure `grafeo-adapters` coverage and retain every mismatch by feature ID.
- Prove parsed-only queries fail at capability admission, not through accidental
  execution branches.

**GREEN**

- Extend, fork, or replace the parser until all declared Cypher 5 grammar cases
  and source spans pass.
- Implement the pre-parser for version/options/EXPLAIN/PROFILE.

**REFACTOR**

- Lower dependency AST into `kb-cypher-ast` immediately.
- Separate source syntax from semantic and execution capability.

**VERIFY**

- Run grammar coverage, parser fuzzing, TCK parse classification, and diagnostic
  differential tests.

**Exit:** Every Cypher 5 production is classified and parsing is no longer the
limiting uncertainty.

### Wave 5: Implement Semantic Analysis and Logical IR

**Objective:** Make accepted queries mean the same thing before optimizing them.

**Slice order:** values/parameters -> scopes/types/nulls -> MATCH/WHERE/RETURN ->
OPTIONAL/WITH/UNWIND -> aggregation/order/pagination -> paths -> subqueries ->
functions -> procedure calls -> writes -> schema/admin.

For each slice:

**STUB**

- Add TCK scenarios, tiny-graph differential fixtures, negative diagnostics,
  and canonical IR snapshots.

**RED**

- Run against Neo4j and classify semantic, multiplicity, null, scope, type, and
  diagnostic differences.

**GREEN**

- Implement semantic rules and lower to stable logical operators.
- Reject the slice at admission until its runtime operators are green.

**REFACTOR**

- Extract versioned semantic differences into explicit rule tables.
- Property-test AST-to-IR invariants and canonical serialization.

**VERIFY**

- Run all applicable TCK, differential, fuzz, and deterministic-hash tests.

**Exit:** A query never reaches a Knight Bus executor without a typed,
semantically validated logical plan.

### Wave 6: Build the General Read Runtime

**Objective:** Execute the high-frequency read language before tackling writes.

**Vertical slice order:**

1. Node seek/scan, property lookup, filter, projection.
2. Directed typed expand and bounded variable-length expand.
3. Optional rows and joins.
4. Aggregation, distinct, order, skip, limit with spill.
5. WITH, UNWIND, UNION, list and map operators.
6. Correlated and expression subqueries.
7. Shortest and quantified paths.
8. EXPLAIN/PROFILE and summaries.

For every operator:

**STUB:** define row schema, semantics, memory model, cancellation points, and
oracle fixtures.  
**RED:** prove at least one tiny graph and one budget case fail.  
**GREEN:** implement the bounded pull/morsel operator.  
**REFACTOR:** add specialization only after generic correctness.  
**VERIFY:** run operator properties, differential graph families, spill,
parallel determinism, and official driver consumption.

**Exit:** `KB-C1-READ` runs a founder-selected production query corpus unchanged
and publishes exact exclusions.

### Wave 7: Add Algorithm-Shaped GDS Execution

**Objective:** Turn Cypher/Bolt compatibility into the differentiated low-RAM
OLAP product.

**Algorithm order SHALL follow validated demand, not source-code order.** The
default candidate sequence is graph projection/catalog, PageRank, weakly
connected components, Louvain/Leiden, shortest path, node similarity, triangle
count/local clustering, betweenness, and selected embeddings. It SHALL change
when real workload evidence changes.

For each algorithm and mode:

**STUB**

- Freeze GDS signature/config/result fixtures.
- Define exact/tolerance invariants and adversarial graph families.
- Define at least two storage/resource profiles where a real trade-off exists.

**RED**

- Run Neo4j GDS and capture results, memory estimate, timing, and mode behavior.
- Prove the procedure is either absent or routed only to a typed stub.

**GREEN**

- Implement estimate first, then stream, stats, mutate, and write as applicable.
- Lower `CALL gds.*` directly into the registered algorithm plan.
- Enforce a hard peak-RAM admission bound.

**REFACTOR**

- Share primitives only when algorithms share access shape and invariants.
- Keep layout conversion and algorithm work separately measurable.

**VERIFY**

- Differentially test results and schemas.
- Run paired RAM/latency benchmarks over tiny, v002, and larger datasets.
- Calibrate estimated versus actual peak RAM.

**Exit:** `KB-C2-GDS` has at least one hero algorithm whose correctness, RAM,
latency, predictability, and migration path are proven end-to-end.

### Wave 8: Complete the Cypher 5 Read Profile

**Objective:** Close rare but declared read-language gaps.

**STUB/RED**

- Generate the manifest gap report and execute every remaining applicable TCK,
driver, DSL-generated, shell, and differential fixture.

**GREEN**

- Implement gaps in descending production evidence and dependency order.

**REFACTOR**

- Remove feature-specific bypasses that duplicate general operator semantics.

**VERIFY**

- Reach zero unexplained mismatch and 100 percent applicable TCK/driver pass for
the declared read profile.

**Exit:** `KB-C3-READ-FULL` is releasable.

### Wave 9: Build Transactions and Writes

**Objective:** Add OLTP behavior without contaminating immutable analytical
formats.

**STUB**

- Define isolation, durability, constraint, identity, index, bookmark, and crash
contracts before choosing the physical write engine.
- Add state-model and kill-point harnesses.

**RED**

- Run Neo4j differential fixtures for CREATE/DELETE/SET/REMOVE/MERGE,
constraints, conflicts, retries, and visibility.

**GREEN**

- Implement transaction-local write sets, WAL, commit/rollback, snapshots,
indexes, constraints, and analytical snapshot publication.

**REFACTOR**

- Keep `kb-storage-spi` independent of one write-engine implementation.
- Make analytical projections derived, immutable, versioned artifacts.

**VERIFY**

- Run crash injection, concurrent histories, model checking, driver managed
transactions, recovery, and long-running snapshot tests.

**Exit:** `KB-C4-TX` supports the declared write and transaction manifest with no
torn-state or visibility mismatch.

### Wave 10: Add Cypher 25 and Current Full Profile

**Objective:** Support the pinned Cypher 25 release without destabilizing Cypher
5.

**STUB**

- Generate a Cypher 5-to-25 semantic delta manifest from official docs and the
  pinned source/test oracle.

**RED**

- Run version-paired queries and prove additions, removals, and changed behavior
are visible only under the selected language version.

**GREEN**

- Implement version-specific grammar, semantics, functions, paths, diagnostics,
and administration capabilities.

**REFACTOR**

- Share stable operators while keeping version rules explicit in the front end.

**VERIFY**

- Run both complete manifests, all current drivers/Bolt versions, TCK,
  differential, resource, and scale gates.

**Exit:** `KB-C5-CURRENT` is pinned, reproducible, and migration-testable.

### Wave 11: Historical Bolt and Optional Ecosystem Profiles

**Objective:** Add old protocol/application compatibility only where demand
justifies its ownership cost.

Candidates: Bolt 1.0-4.3, APOC subsets, Browser, OGM, LOAD CSV, broad
administration, routing topology, and additional GDS algorithms.

Each candidate SHALL begin with a customer/workload fixture and an ownership
estimate. Passing syntax alone SHALL not justify implementation.

**Exit:** each optional profile has an independent manifest, test suite, and
deprecation policy.

### Work Packet Template

Every LLM implementation goal SHALL be no larger than one externally observable
behavior and SHALL include this packet:

```markdown
# Goal: <four-word behavior name>

## Requirement
Implement <REQ-ID> for profile <PROFILE-ID>.

## External red test
<exact driver/TestKit/TCK/differential command and expected failure>

## Allowed evidence
<spec URLs, permissive sources, oracle fixtures, graph-query evidence>

## Forbidden evidence
<GPL implementation source and generated oracle answers used as runtime input>

## Internal boundary
<traits/crates permitted to change>

## Resource contract
<RAM, rows, bytes, time, cancellation, spill limits>

## Green command
<smallest exact command proving behavior>

## Regression commands
<unit, property, driver, differential, scale tests>

## Manifest transition
<unsupported/parsed-only -> experimental/implemented>

## Required receipt
<versions, hashes, results, metrics, exclusions>
```

### Goal-Ready Execution Prompt

```text
/goal Implement the next green compatibility slice from
docs_PMF_01/PMF007-Bolt-Cypher-Mega-Spec.md.

Use the executable-spec workflow and TDD strictly. Start by reading PMF007,
PMF006, the current compatibility manifests, and the active TDD progress file.
Use codebase-memory-mcp first for symbol discovery and call-path evidence. Use
code-graph-mcp for module maps, structural paths, and cross-checking broad
source areas. Do not grep for code symbols unless graph tools are insufficient;
record why any fallback was needed.

Select exactly one unimplemented requirement whose dependencies are green and
which completes an externally observable path. State the selected requirement,
profile, oracle pin, legal/provenance boundary, affected traits/crates, external
red test, resource contract, and regression commands before editing.

Run impact analysis before changing existing symbols. Preserve user changes.
Create the external failing test first and prove it fails for the intended
missing behavior. Implement the smallest complete Bolt -> Cypher -> logical IR
-> admission -> Knight Bus execution -> bounded result -> Bolt response path.
Unsupported adjacent behavior must fail closed with a typed diagnostic.

Never copy GPL Neo4j/GDS implementation source. Neo4j and GDS are black-box
behavioral oracles. Reuse Apache/MIT code only after checking provenance and
notices. Never feed oracle output into the Knight Bus runtime as expected data.

After green, refactor behind the stable internal traits. Run unit, property,
fuzz regression, TCK/TestKit/driver/differential tests applicable to the slice,
plus the PMF006 60-query corpus whenever compatibility-plane code changes.
Measure peak RAM and stage latency where the requirement touches execution.

Update the machine-readable feature manifest only after every named test is
green. Emit a proof receipt containing source/query/data/plan/result/binary
hashes, exact versions, raw metrics, and exclusions. Update the TDD progress
file with RED, GREEN, REFACTOR, and VERIFY evidence. Run change-impact and
format/lint/test checks before claiming completion. Do not mark a broader
profile compatible than the evidence proves.
```

### Effort Shape and Sequencing Decision

These are planning ranges, not promises. They assume multiple strong engineers
or persistent verification-first LLM agents, mature CI, and no copying of GPL
implementation. Calendar time depends on how narrow the production corpus is.

| Outcome | Primary uncertainty | Relative work | Earliest rational target |
|---|---|---:|---|
| C0 reproducible/streaming | Bolt server seed surgery | 1 unit | Immediate |
| C1 useful read corpus | semantic/runtime breadth | 8-15 units | First product |
| C2 first hero GDS algorithms | storage/algorithm proof | 5-12 units | In parallel after C1 core |
| C3 broad Cypher 5 reads | long-tail semantics | 20-40 units | Demand-gated |
| C4 writes/transactions | storage correctness and recovery | 25-50 units | Only after PMF validation |
| C5 current full profile | Cypher 25 plus all current matrices | 40-80 units cumulative | Strategic program |
| C6 historical/ecosystem | long-tail ownership | unbounded by demand | Optional |

One work unit here means one medium end-to-end capability slice with tests,
oracle, resource evidence, and receipts; it is deliberately not translated into
days because slice size and agent throughput must first be measured on Waves
0-3. The key product decision is to earn evidence for C1/C2 before committing to
C4-C6.

## Quality Gates

### Per-Change Gate

Every code change SHALL pass:

1. Requirement and profile IDs are named in the test and change record.
2. An external or contract RED test was observed before implementation.
3. Impact analysis was run for every changed existing symbol.
4. Formatting, linting, compilation, unit, and directly affected contract tests
   pass with warnings treated according to repository policy.
5. No production `unwrap`, unbounded channel, unbounded collection, blocking IO
   on an async worker, or silent capability fallback is introduced.
6. Provenance is recorded for source, fixtures, and generated artifacts.
7. The capability manifest is unchanged until verification is complete.
8. Change-impact analysis confirms only expected symbols and flows changed.
9. `git diff --check` and generated-artifact reproducibility pass.
10. Existing user modifications remain intact.

### Rust Crate Boundary Gate

- `kb-packstream` SHALL have no dependency on sessions, Cypher, or storage.
- `kb-bolt-fsm` SHALL depend on typed messages and abstract session actions, not
  Knight Bus storage.
- `kb-cypher-parser` SHALL have no dependency on the planner or executor.
- `kb-cypher-ir` SHALL have no dependency on Neo4j, GDS, Grafeo, or Bolt types.
- `kb-cypher-runtime` SHALL access data only through `kb-storage-spi` and
  registered specialized executors.
- `kb-gds-bridge` SHALL bind procedures to algorithm plans without importing
  transport concerns.
- `kb-result-stream` SHALL own backpressure semantics shared by general, walk,
  and GDS executors.
- `kb-resource-governor` SHALL be consulted before every potentially blocking
  or growing allocation path.

Any cycle among wire, language, planning, execution, and storage layers fails
the architecture gate.

### Bolt Gate

A Bolt version may change from experimental to implemented only when:

- every manifest structure and message has golden and negative codec fixtures;
- every declared state transition has generated model coverage;
- reset, interruption, failure, disconnect, and cleanup paths pass;
- bounded streaming passes under a result larger than the memory window;
- applicable TestKit/BoltStub cases pass with zero unexplained exclusion;
- all pinned official drivers complete their lifecycle matrix;
- malformed and slow-client fuzz/regression suites have no crash, leak, or
  unbounded growth;
- TLS and routing behavior pass where included in the profile.

### Cypher Front-End Gate

A syntax/semantic feature may change to implemented only when:

- the grammar production is present in the manifest;
- positive, malformed, semantic-negative, and source-position tests pass;
- applicable TCK scenarios pass;
- acceptance and diagnostic category agree with the pinned Neo4j oracle;
- the stable logical IR snapshot is deterministic;
- unsupported adjacent syntax fails before execution;
- parser and planner time/memory bounds pass adversarial tests.

### Planner and Runtime Gate

An operator or rewrite may change to implemented only when:

- generic semantic fixtures pass before specialization is enabled;
- row schema, multiplicity, nulls, side effects, and ordering are specified;
- tiny graph differential tests pass as multisets or ordered sequences as
  semantically appropriate;
- cancellation and deadline observation have a measured upper work interval;
- memory cost and spill behavior are specified and tested;
- deterministic plan/result requirements pass under parallel scheduling;
- summary metrics and receipt fields are complete;
- specialized and generic results agree over generated fixtures.

### GDS Algorithm Gate

A GDS algorithm/mode may change to implemented only when:

- procedure/function signatures and result schemas match the pinned inventory;
- estimate executes without allocating the full algorithm state;
- stream/stats/mutate/write modes are independently classified;
- exact or tolerance/invariant differential tests pass on all graph families;
- peak RAM upper bound holds for in-memory and bounded-spill profiles;
- preparation, conversion, algorithm, and output costs are measured separately;
- estimate-versus-actual error is within the calibrated confidence policy;
- the official GDS client can invoke and consume the supported mode unchanged.

### Transaction and Storage Gate

Write capability may be released only when:

- transaction state model has no illegal commit or stale-handle counterexample;
- read-your-writes, isolation, conflicts, constraints, counters, rollback, and
  retries agree with the declared profile;
- kill-point recovery passes at every instrumented commit boundary;
- corrupt/truncated formats fail closed;
- fuzzing has no crash or durable-state corruption;
- long readers and concurrent writers preserve snapshot validity;
- analytical snapshot publication is atomic and rebuildable;
- backups/migrations required by the profile are proven.

### Resource Predictability Gate

For each benchmark class and RAM tier:

1. The preflight estimator emits lower, expected, and conservative upper bounds.
2. The execution is admitted only when every reservation can be obtained.
3. Measured peak heap, mapped RSS, total RSS, spill bytes, and output buffers are
   reported separately.
4. Peak RSS remains below the promised limit plus a documented platform
   tolerance.
5. Upper-bound violations are release blockers, not benchmark noise.
6. Estimate error is retained for calibration and does not overwrite raw data.
7. Cancellation and failure return all query-private permits and spill files.

### Slice 0 Scale Gate

The PMF006/v002 gate SHALL use:

- raw dataset: 2,187,775,971 bytes;
- nodes: 3,997,988;
- directed edges: 36,294,270;
- Knight Bus snapshot: 514,241,964 bytes;
- all 60 fixed queries, with no cherry-picking;
- an official Neo4j driver over the real Bolt socket;
- the pinned Neo4j oracle on the same logical data;
- paired process/cache conditions;
- exact result parity;
- cold-open and warm-query measurements;
- direct-runtime and end-to-end compatibility measurements;
- peak RSS rather than a single post-run sample.

The prior corrected evidence of roughly 234 MB Knight Bus RSS versus 1,066 MB
Neo4j RSS, and 0.044948 ms Knight Bus p99 versus 1514.533206 ms Neo4j p99, is a
narrow-corpus baseline. It SHALL NOT be generalized to full Cypher or GDS until
those exact end-to-end profiles are measured.

### Security Gate

- Authentication and authorization negatives pass for every exposed capability.
- Secrets do not appear in logs, diagnostics, traces, receipts, crash dumps, or
  snapshots used by CI.
- TLS configuration rejects downgrade and invalid trust according to policy.
- Protocol, parser, planner, path expansion, procedure, and output DoS fixtures
  remain bounded.
- Default builds expose no filesystem, URL, native plugin, or administration
  capability not named in the profile.
- Dependency, license, and vulnerability audits pass or have approved bounded
  exceptions.

### Release Claim Gate

Before a profile release:

1. Regenerate compatibility and exclusion documents from manifests.
2. Prove every implemented item has positive, negative, and differential tests.
3. Run all applicable TCK, TestKit, driver, graph-family, fuzz-regression,
   security, crash, scale, and performance gates.
4. Produce one immutable release receipt index containing all child receipts.
5. Review unexplained exclusions; there SHALL be none for implemented items.
6. Review legal provenance and notices.
7. Compare public statements with claim evidence automatically.
8. Sign/tag the source, manifests, dataset identities, binaries, and receipts.

### Stop Conditions

The team SHALL stop broadening compatibility and revisit architecture when any
of these remains true for two consecutive slices:

- a stable IR cannot express the feature without leaking parser/vendor types;
- a supposedly bounded result or operator requires full materialization;
- measured peak RAM repeatedly exceeds conservative estimates by more than the
  approved tolerance;
- specialized execution disagrees with generic and Neo4j oracles on minimized
  fixtures;
- a protocol/version implementation requires scattered conditionals instead of
  manifest-driven behavior;
- clean-room provenance cannot be established;
- transaction recovery produces a torn or ambiguous committed state;
- C1/C2 customer evidence does not justify the ownership cost of C3-C6.

Stopping under these conditions is successful risk control, not failure to
execute the roadmap.

### Completion Definition

This mega-spec implementation goal is complete only when:

- all 137 requirements are either implemented in the declared final profile or
  explicitly excluded by a reviewed profile decision;
- no requirement is represented only by prose;
- every implemented item has green traceable tests and receipts;
- production applications can use unchanged supported Cypher and official
  Neo4j drivers against Knight Bus;
- result semantics, transactions, errors, and summaries satisfy the profile;
- peak RAM and latency claims are paired, reproducible, and profile-specific;
- manifests and exclusions make unsupported behavior obvious before migration;
- GPL and uncertain-license evidence has not contaminated implementation source.

### Source Evidence

#### Local source pins

| Repository | Local SHA | Approximate checkout size/files |
|---|---|---:|
| Neo4j server | `c68156edf24` | 880 MB / 11,849 files |
| Neo4j GDS | `dc4417b3c1` | 339 MB / 5,637 files |
| Bolt docs | `1714723` | 456 KB / 33 files |
| openCypher | `677cbaf` | 5.8 MB / 262 files |
| TestKit | `ec46b65` | 6.8 MB / 1,064 files |
| Java driver | `7652d3c3f` | 24 MB / 963 files |
| Go driver | `c872010` | 12 MB / 233 files |
| Python driver | `9e23c904` | 13 MB / 505 files |
| JavaScript driver | `d8841712` | 42 MB / 798 files |
| .NET driver | `261a8250` | 19 MB / 970 files |
| neo4rs | `19f244a` | 1.7 MB / 155 files |
| Cypher Shell | `3e7573e` | 1.4 MB / 165 files |
| Cypher DSL | `8bf1a55` | 6.4 MB / 803 files |
| Neo4j Browser | `ff8ed85` | 39 MB / 1,033 files |
| GDS client | `e96f9066` | 105 MB / 995 files |
| GDS agent | `65d1894` | 3.0 MB / 58 files |
| Graph Data Science examples | `1da2b3d` | 18 MB / 18 files |
| APOC procedures | `940033f` | 156 MB / 4,898 files |
| APOC | `11dbf56` | 73 MB / 613 files |
| Neo4j OGM | `eeee0bc` | 11 MB / 1,227 files |

#### Structural evidence

- The codebase-memory Neo4j server graph contains 218,480 nodes and 1,186,960
  edges; the GDS graph contains 54,265 nodes and 284,022 edges.
- The SDSRS Neo4j server graph contains 113,831 nodes, 1,632,830 edges, and
  8,002 indexed files; the GDS graph contains 38,262 nodes, 521,221 edges, and
  4,921 indexed files.
- Neo4j separates Bolt negotiation/messages/state machines from Cypher front
  end, logical planning, physical planning, and multiple runtimes. Knight Bus
  should preserve these conceptual boundaries without cloning their classes.
- The local Neo4j source contains parser families for Cypher 5 and Cypher 25 and
  protocol implementations through version 5.8; current official documentation
  adds the external Bolt 6.0 contract.
- The openCypher repository contributes a baseline TCK of 220 feature files,
  1,615 scenarios, and 276 examples; it does not cover every Neo4j extension.
- The existing Knight Bus GDS registry and execution dispatcher provide a real
  lowering target for graph catalog/property procedures, but not broad algorithm
  parity yet.

#### Current official references

- [Bolt Protocol documentation](https://neo4j.com/docs/bolt/current/)
- [Bolt handshake and Manifest v1](https://neo4j.com/docs/bolt/current/bolt/handshake/)
- [Bolt message changes through 6.0](https://neo4j.com/docs/bolt/current/bolt/message/)
- [Bolt server state](https://neo4j.com/docs/bolt/current/bolt/server-state/)
- [Bolt structure semantics](https://neo4j.com/docs/bolt/current/bolt/structure-semantics/)
- [Bolt and Neo4j compatibility matrix](https://neo4j.com/docs/bolt/current/bolt-compatibility/)
- [Cypher version selection](https://neo4j.com/docs/cypher-manual/current/queries/select-version/)
- [Cypher additions, removals, and compatibility](https://neo4j.com/docs/cypher-manual/current/deprecations-additions-removals-compatibility/)
- [Current Cypher clauses](https://neo4j.com/docs/cypher-manual/current/clauses/)
- [Neo4j Bolt TLS configuration](https://neo4j.com/docs/operations-manual/current/security/ssl-framework/)

The official docs state that Cypher 25 was introduced with Neo4j 2025.06 and is
the explicitly configured default for newly created Neo4j 2026.02+ databases,
while Cypher 5 remains selectable. They also document Bolt Manifest v1, protocol
changes through Bolt 6.0, and versioned GQL failure metadata. These moving
contracts are why manifests must pin calendar releases rather than use a vague
`current` target.

## Open Questions

### OQ-001: Which customer query corpus defines C1?

**Default decision:** collect 20-50 real parameterized read queries, result
consumption patterns, data schemas, and SLO/RAM tiers from design partners.
Without this corpus, prioritize the clause/operator sequence in Wave 6 and do
not claim product-market relevance.

### OQ-002: Fork `boltr`, contribute upstream, or replace it?

**Default decision:** keep it behind internal traits, immediately replace/fork
the materialized result API, and evaluate upstream collaboration after the
streaming contract is proven. Do not block C0/C1 on an upstream release.

### OQ-003: Extend Grafeo or build a dedicated parser?

**Default decision:** measure it against the generated Cypher 5 grammar and
diagnostic corpus first. Retain it only if closing gaps is cheaper than a
grammar-generated parser and its AST can be quarantined behind
`kb-cypher-parser`.

### OQ-004: Is exact human error wording required?

**Default decision:** require structured code/status, source position,
retriability, diagnostic record, and cause. Treat wording as version-tolerant
unless a driver/application demonstrably depends on it, especially because
Bolt 6.0 no longer guarantees stable failure message wording.

### OQ-005: Which Bolt versions are commercially necessary?

**Default decision:** prioritize 5.4, 5.7, 5.8, 6.0, then the negotiable 5.x
gaps and 4.4. Mark 5.5 `not-applicable`, as the official compatibility table
says no Neo4j server negotiates it. Keep 1.0-4.3 in the manifest but demand a
real migration fixture before implementation.

### OQ-006: What does the first transaction profile promise?

**Default decision:** defer the physical engine choice. First specify snapshot
isolation/causal bookmarks/durability/constraint behavior from actual migration
needs. Do not promise Neo4j clustering or enterprise distributed transactions
by implication.

### OQ-007: Which GDS algorithms are the hero set?

**Default decision:** use customer query frequency and cost evidence. Start with
PageRank plus one component/community workload only if interviews validate
them. The procedure catalog and estimate path can exist before every algorithm.

### OQ-008: How much result-order compatibility is necessary?

**Default decision:** match order only when Cypher defines it or the profile
documents a deterministic extension. Compare unordered results as multisets and
educate migrations that rely on accidental order.

### OQ-009: Which hard RAM tiers become product contracts?

**Default decision:** support explicit query budgets and offer 5 GB, 10 GB, and
20 GB benchmark tiers only after estimator calibration on representative data.
Reject or spill rather than violate the selected tier.

### OQ-010: Can TestKit fixtures be redistributed?

**Default decision:** treat the local TestKit checkout as oracle-only until its
license/provenance is confirmed. Build independently authored protocol fixtures
from public Bolt specifications where legal uncertainty remains.

### OQ-011: How are dynamic procedures installed safely?

**Default decision:** static first-party Rust registration for C1/C2. Defer
native dynamic loading; investigate process or WASM isolation only when a real
extension use case appears.

### OQ-012: Is routing compatibility useful on a single-node engine?

**Default decision:** return a truthful single-node routing table for driver
compatibility, with no false high-availability claim. Add multi-node semantics
only with an explicit distributed architecture.

### OQ-013: What becomes the canonical logical ID?

**Default decision:** dataset-scoped stable element IDs distinct from compact
physical ordinals. Freeze the encoding only after round-trip, migration, stale
ID, and cross-dataset tests pass.

### OQ-014: How is Cypher 25 updated after 2026.06?

**Default decision:** add a new immutable language manifest per pinned Neo4j
release, generate the delta, and run both old and new profiles. Never mutate the
meaning of an existing manifest in place.

### OQ-015: When is the full rewrite commitment justified?

**Default decision:** only after C1/C2 show repeated demand, a meaningful cost or
latency advantage, and evidence that missing C3/C4 features block adoption.
Until then, the rational strategy is a compatibility facade plus differentiated
analytical execution, not institutional commitment to clone every Neo4j
surface.

### OQ-016: What evidence closes this specification?

**Default decision:** create three follow-on artifacts before major coding:

1. `bolt-feature-manifest-v1.yaml` generated from local and official evidence.
2. `cypher-feature-manifest-v1.yaml` generated from Cypher 5/25 grammar,
   semantic, TCK, and source inventories.
3. `founder-query-corpus-v1.yaml` containing real queries, data shapes, result
   behavior, SLOs, RAM limits, and required drivers.

Those artifacts turn this mega-spec into a finite queue of TDD work packets.

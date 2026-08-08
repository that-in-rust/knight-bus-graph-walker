# PMF006: Cypher Bolt Walk Spec

Date: 2026-08-07
Status: implementation-ready executable specification
Compatibility profile: `knight-bus-neighborhood-walk-v1`

This specification defines the smallest honest end-to-end proof that an
existing Neo4j application can send an unchanged Cypher query through a Bolt
connection and have Knight Bus execute the query against its immutable mmap
snapshot.

The single algorithm in scope is bounded neighborhood traversal over the
`DEPENDS_ON` relationship family. Forward one hop, reverse one hop, and reverse
one-to-two hops are configurations of one algorithm, not three independent
algorithms.

This specification does not claim general Cypher compatibility, general Bolt
server compatibility, transactional Neo4j compatibility, or GDS algorithm
compatibility.

## Executable Requirements

### Request Parse

| input | value |
| --- | --- |
| Feature outcome | An official Neo4j driver SHALL submit unchanged supported Cypher text over Bolt, and Knight Bus SHALL execute it over the v002 dual-CSR mmap snapshot and return Neo4j-equivalent ordered records. |
| Primary actor | An application currently using a Neo4j driver and a production-style read query. |
| Secondary actors | Compatibility implementer, benchmark author, correctness reviewer, and future query-operator implementer. |
| Query boundary | One bounded `DEPENDS_ON` neighborhood-walk algorithm with one-hop forward, one-hop reverse, and one-to-two-hop reverse configurations. |
| Data boundary | The fixed v002 corpus: 2,187,775,971 raw CSV bytes, 3,997,988 nodes, 36,294,270 relationships, and a 514,241,964-byte Knight Bus snapshot. |
| Driver boundary | Python Neo4j driver `6.1.0`, already pinned in `benchmarks/walk_hopper_v1/requirements.txt`. |
| Parser boundary | Neo4j Cypher DSL Parser is the reference parsing authority. Local source is pinned at commit `8bf1a556cd2addab0ff9046fdd2b0044542690dd`. |
| Runtime boundary | Knight Bus owns validation after parsing, logical planning, physical planning, snapshot access, execution, resource accounting, and result production. |
| Failure modes | Syntax error, unsupported valid Cypher, missing parameter, wrong parameter type, unknown relationship type, unsupported graph profile, result limit, deadline, client disconnect, parser failure, malformed Bolt interaction, authentication failure, and snapshot-open failure. |
| Performance boundary | Correctness is mandatory. Warm latency and total compatibility-stack RSS SHALL beat the paired Neo4j result in the same benchmark run; cold-open latency SHALL be measured but is not a release gate in v1. |
| Runtime constraints | Rust 2024 Knight Bus runtime; immutable mmap snapshot; no graph loading into the parser process; no write path; no unbounded traversal. |

### Governing Decision

The query text SHALL be a first-class Knight Bus input. The caller SHALL NOT
wrap the query in a custom procedure such as `CALL knightbus.run(...)`, rewrite
the query into CLI flags, or precompile the query manually.

The user-visible execution path SHALL be:

```text
Neo4j driver
    |
    | Bolt RUN(query_text, parameters)
    v
Knight Bus compatibility endpoint
    |
    | parse + validate + compile
    v
CompiledNeighborhoodWalkPlan
    |
    | execute against one already-open MmapWalkRuntime
    v
Bolt records + summary + execution receipt
```

The initial implementation MAY use a supervised JVM parser bridge internally
because the selected Neo4j parser is Java-based. That process boundary SHALL
not alter the external contract: the complete original query enters Knight Bus
through Bolt, and Knight Bus owns whether and how the resulting plan executes.

The parser process SHALL receive query text and SHALL return a typed AST-derived
description or typed parsing failure. It SHALL NOT open the graph, execute the
query, call a Neo4j database, or return query rows.

### Compatibility Claim Vocabulary

Only the following claim is authorized after every release gate passes:

> Knight Bus implements the `knight-bus-neighborhood-walk-v1` compatibility
> profile for read-only auto-commit Cypher queries over direct Bolt, verified
> with Neo4j Python driver 6.1.0 against the fixed v002 traversal corpus.

The following claims are forbidden by this specification:

- `Cypher compatible` without the profile name.
- `Bolt compatible` without `read-only auto-commit` and the tested driver.
- `Neo4j compatible` without naming the supported query and protocol surface.
- `Production compatible` based only on the synthetic v002 corpus.
- `Faster` or `lower RAM` without a same-run comparator receipt.

### Supported Cypher Shapes

The required v1 query family is represented by these canonical statements.

#### Forward One-Hop Walk

```cypher
MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m)
RETURN m.node_id AS node_id
ORDER BY node_id
```

#### Reverse One-Hop Walk

```cypher
MATCH (n {node_id: $node_id})<-[:DEPENDS_ON]-(m)
RETURN m.node_id AS node_id
ORDER BY node_id
```

#### Reverse One-To-Two-Hop Walk

```cypher
MATCH (n {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(m)
RETURN DISTINCT m.node_id AS node_id
ORDER BY node_id
```

The parser SHALL also accept semantic-preserving differences in whitespace,
line breaks, comments, keyword case, variable names, explicit `ASC`, and an
optional `:Entity` label on the starting and returned nodes.

The v1 compiler MAY accept this equivalent start predicate:

```cypher
MATCH (n)<-[:DEPENDS_ON]-(m)
WHERE n.node_id = $node_id
RETURN m.node_id AS node_id
ORDER BY node_id
```

Support for the equivalent `WHERE` form is not required for the first green
slice. If implemented, it SHALL satisfy all parity tests before entering the
declared profile.

### Explicit Non-Goals

- Cypher writes, schema operations, or transactional mutations.
- Explicit multi-statement transactions.
- `OPTIONAL MATCH`, `UNION`, subqueries, comprehensions, or procedure calls.
- General joins, grouping, aggregation, expression evaluation, or arbitrary
  property predicates.
- Unbounded variable-length paths.
- Relationship types other than `DEPENDS_ON`.
- Path values or relationship values in result records.
- Neo4j routing, clustering, bookmarks, fabric, impersonation, or causal
  consistency.
- TLS in the first local proof.
- `neo4j://` routing URIs; the required URI scheme is direct `bolt://`.
- Full Neo4j error-message text parity. Stable error categories and driver
  behavior are required instead.
- PageRank, WCC, BFS procedure compatibility, or any other GDS algorithm.

### Logical Plan Contract

The language boundary SHALL compile supported queries into a language-neutral
plan equivalent to:

```text
CompiledNeighborhoodWalkPlan
  contract_version: "knight-bus-neighborhood-walk-v1"
  start_property: "node_id"
  start_parameter: "node_id"
  required_node_label: null | "Entity"
  relationship_type: "DEPENDS_ON"
  direction: forward | reverse
  minimum_hops: 1
  maximum_hops: 1 | 2
  result_property: "node_id"
  result_alias: "node_id"
  distinct: false | true
  order_property: "node_id"
  order_direction: ascending
```

The physical mapping SHALL be:

```text
start_parameter
  -> NodeKey validation
  -> dense ID lookup
  -> forward or reverse CSR cursor
  -> bounded neighborhood traversal
  -> endpoint-set or edge-row semantics required by the query
  -> dense ID to NodeKey lookup
  -> projection as {node_id: String}
  -> ascending order
  -> Bolt record stream
```

### Dataset Profile Assumptions

The v002 compatibility fixture SHALL record and validate these assumptions:

- `node_id` is unique and is the Knight Bus `NodeKey`.
- Every queryable node satisfies the logical `Entity` label contract.
- Every relationship satisfies the logical `DEPENDS_ON` type contract.
- The graph profile does not require arbitrary node or relationship properties.
- One-hop queries rely on simple-graph semantics unless relationship
  multiplicity is explicitly represented and verified.
- Multi-hop profile queries use `DISTINCT`, so multiple paths to the same
  endpoint produce one endpoint row.
- The manifest identifies the raw corpus, snapshot generation, query corpus,
  expected-answer artifact, and all hashes used for the run.

If any assumption is absent or false, the profile SHALL fail admission instead
of silently treating absent labels, types, properties, or multiplicity as if
they matched.

### REQ-SCOPE-001.0: Restrict compatibility to one algorithm

**WHEN** the compatibility endpoint receives a query
**THEN** the system SHALL accept only queries that compile to the bounded neighborhood-walk logical operator
**AND** SHALL identify the active profile as `knight-bus-neighborhood-walk-v1`
**SHALL** reject every query that requires an additional logical operator.

### REQ-API-001.0: Accept the original query directly

**WHEN** a caller submits a supported Cypher statement and parameters through Bolt
**THEN** Knight Bus SHALL receive the complete original query text and parameter map
**AND** SHALL parse, validate, compile, and execute that query without caller-side rewriting
**SHALL** not require a custom Knight Bus procedure or query identifier.

### REQ-CYPHER-001.0: Parse supported Cypher with the Neo4j oracle

**WHEN** a canonical or semantic-preserving supported query is submitted
**THEN** the selected parser SHALL produce a successful AST-derived representation
**AND** the Neo4j Cypher DSL Parser pinned by the build SHALL parse the same query successfully
**SHALL** return a typed syntax failure when the reference parser rejects the query.

### REQ-CYPHER-002.0: Compile exact traversal semantics

**WHEN** a supported AST contains direction, relationship type, hop range, projection, distinctness, and ordering
**THEN** the compiler SHALL preserve each of those semantics in `CompiledNeighborhoodWalkPlan`
**AND** an omitted relationship length SHALL compile to exactly one hop
**SHALL** not infer a broader hop range, relationship type, projection, or ordering than the query expresses.

### REQ-CYPHER-003.0: Bind the node identifier parameter

**WHEN** the query refers to `$node_id` and the parameter map contains a valid string
**THEN** the compiler SHALL bind that exact string as the starting `NodeKey`
**AND** SHALL permit unrelated extra parameters without changing the plan
**SHALL** return a typed parameter failure for a missing, null, or non-string `$node_id`.

### REQ-CYPHER-004.0: Reject unsupported valid Cypher

**WHEN** a syntactically valid Cypher query requires an unsupported clause, expression, property, relationship type, hop range, result shape, or ordering
**THEN** the system SHALL return a stable unsupported-feature failure
**AND** the failure SHALL identify the first unsupported AST feature and relevant query fragment when available
**SHALL** fail before opening a query cursor or traversing the snapshot.

### REQ-CYPHER-005.0: Separate syntax and support failures

**WHEN** query processing fails
**THEN** malformed Cypher SHALL be classified separately from valid but unsupported Cypher
**AND** missing or invalid parameters SHALL be classified separately from both
**SHALL** preserve the distinction through the Bolt driver exception metadata.

### REQ-PLAN-001.0: Produce a canonical logical plan

**WHEN** two queries differ only in whitespace, comments, keyword case, variable names, or explicit ascending order
**THEN** they SHALL compile to structurally identical logical plans
**AND** the serialized canonical plans SHALL be byte-identical
**SHALL** exclude original formatting and incidental parser object identities.

### REQ-PLAN-002.0: Hash the execution contract deterministically

**WHEN** a canonical logical plan is admitted
**THEN** the system SHALL compute a deterministic plan hash from the canonical plan bytes
**AND** the execution receipt SHALL contain that plan hash and compatibility-profile version
**SHALL** produce a different hash when execution semantics change.

### REQ-EXEC-001.0: Reuse an already-open mmap runtime

**WHEN** the Bolt endpoint becomes ready
**THEN** it SHALL have opened and validated exactly one configured `MmapWalkRuntime`
**AND** all admitted queries SHALL reuse that runtime without reopening snapshot files per query
**SHALL** stop readiness if the snapshot cannot be opened or validated.

### REQ-EXEC-002.0: Execute the three required configurations

**WHEN** a plan requests forward one hop, reverse one hop, or reverse one-to-two hops
**THEN** Knight Bus SHALL execute the corresponding CSR direction and bounded traversal
**AND** SHALL project every result as one string-valued `node_id` column
**SHALL** preserve the query's endpoint-set, distinctness, and ascending-order semantics.

### REQ-EXEC-003.0: Return empty results for an absent start node

**WHEN** `$node_id` does not identify a node in the snapshot
**THEN** the query SHALL succeed with zero records
**AND** the result summary SHALL report successful completion
**SHALL** not expose the internal dense-ID lookup failure as a database error.

### REQ-EXEC-004.0: Preserve cycle and self-loop semantics

**WHEN** a supported query executes on a graph containing cycles, self-loops, diamonds, or multiple paths to one endpoint
**THEN** Knight Bus SHALL return the same endpoint rows as Neo4j for the same query
**AND** `DISTINCT` SHALL collapse duplicate endpoint rows exactly once
**SHALL** include the start node when Neo4j returns it through a qualifying self-loop or cycle.

### REQ-EXEC-005.0: Enforce configured execution bounds

**WHEN** execution reaches a configured deadline, result-row limit, or client cancellation
**THEN** traversal and record production SHALL stop
**AND** allocated per-query state SHALL be released while the shared mmap runtime remains usable
**SHALL** return a typed termination result rather than a partial successful result.

### REQ-BOLT-001.0: Connect with the pinned official driver

**WHEN** Neo4j Python driver `6.1.0` opens a direct `bolt://` connection to the configured endpoint
**THEN** protocol negotiation, connection initialization, configured authentication, and session creation SHALL succeed
**AND** the negotiated protocol version SHALL be captured in the test receipt
**SHALL** reject invalid credentials without executing query work.

### REQ-BOLT-002.0: Execute unchanged auto-commit queries

**WHEN** the driver calls `session.run(query_text, node_id=value)` for a supported query
**THEN** Knight Bus SHALL execute the query as one read-only auto-commit unit
**AND** iterating the returned result SHALL yield all records
**SHALL** allow `consume()` and session closure to complete without driver protocol errors.

### REQ-BOLT-003.0: Return Neo4j-shaped records

**WHEN** a supported query returns rows
**THEN** the driver SHALL expose one key named `node_id`
**AND** `record["node_id"]` SHALL return the expected string for every row
**SHALL** return records in Neo4j-equivalent ascending order.

### REQ-BOLT-004.0: Preserve typed query failures

**WHEN** parsing, support validation, parameter binding, admission, or execution fails
**THEN** the driver SHALL receive a failure carrying a stable machine-readable code and human-readable message
**AND** the connection SHALL recover according to the tested driver interaction
**SHALL** not close the server or poison later valid queries solely because one query failed.

### REQ-BOLT-005.0: Support sequential connection lifecycle

**WHEN** a client runs, consumes, resets after a failure, or closes a session
**THEN** the server SHALL return to a state in which the same or a new connection can run a valid query
**AND** disconnects SHALL release connection-local and query-local state
**SHALL** retain only explicitly shared runtime and parser resources.

### REQ-BOLT-006.0: Reject unsupported protocol surfaces honestly

**WHEN** the client requests routing, explicit transactions, writes, bookmarks, impersonation, unsupported database selection, or another excluded Bolt surface
**THEN** the server SHALL return a typed unsupported or unavailable response
**AND** SHALL not pretend the operation committed or succeeded
**SHALL** keep the documented read-only auto-commit profile available after recoverable failures.

### REQ-VERIFY-001.0: Prove semantics on adversarial tiny graphs

**WHEN** the compatibility suite runs
**THEN** it SHALL execute every required query against Neo4j and Knight Bus on deterministic tiny fixtures
**AND** fixtures SHALL cover empty graph, isolated node, missing start node, forward edge, reverse edge, two-hop chain, diamond, self-loop, directed cycle, and unreachable component
**SHALL** compare column names, ordered values, success or failure category, and row count.

### REQ-VERIFY-002.0: Prove parser and compiler agreement

**WHEN** a parser fixture is evaluated
**THEN** the fixture SHALL record the original query, reference-parser outcome, compiled canonical plan or typed rejection, and plan hash
**AND** semantic-preserving query variants SHALL share one golden plan
**SHALL** detect any parser upgrade that changes an accepted AST interpretation or rejection category.

### REQ-VERIFY-003.0: Prove complete v002 corpus parity

**WHEN** the fixed 2 GB compatibility benchmark runs
**THEN** the same 60 query-and-parameter rows SHALL execute through Neo4j Bolt and Knight Bus Bolt
**AND** every ordered result sequence SHALL match exactly
**AND** the run SHALL record raw-corpus, snapshot, query-corpus, and expected-answer hashes
**SHALL** fail the release on any missing query, extra query, row mismatch, ordering mismatch, or error mismatch.

### REQ-VERIFY-004.0: Prevent oracle contamination

**WHEN** expected results are produced
**THEN** Neo4j and Knight Bus SHALL independently execute the query from the same logical input corpus
**AND** Knight Bus output SHALL not be used to generate its own expected answer
**SHALL** retain the Neo4j result hash and an independently generated fixture expectation where practical.

### REQ-PERF-001.0: Measure the complete compatibility stack

**WHEN** RAM is measured for Knight Bus compatibility execution
**THEN** the measurement SHALL include the Rust server, parser bridge, protocol gateway, and every compatibility-owned child process
**AND** SHALL separate shared resident bytes, query high-water bytes, mapped bytes, and snapshot-file bytes when observable
**SHALL** not compare Rust-process-only RSS with whole Neo4j-server RSS.

### REQ-PERF-002.0: Beat paired Neo4j warm execution

**WHEN** the complete 60-query corpus is run after one unmeasured warm-up pass on the same machine and dataset
**THEN** Knight Bus end-to-end Bolt p99 latency SHALL be lower than paired Neo4j Bolt p99 latency
**AND** Knight Bus total compatibility-stack peak RSS SHALL be lower than paired Neo4j server-stack peak RSS
**SHALL** publish both absolute measurements, sample count, hardware, process scope, and ratios.

### REQ-PERF-003.0: Report cold-open cost separately

**WHEN** the benchmark starts each system from a stopped state
**THEN** it SHALL measure time to readiness and first successful query separately from warm query latency
**AND** SHALL report parser initialization, snapshot-open, protocol-ready, and first-query phases where observable
**SHALL** not fail v1 solely because Knight Bus cold open is slower than Neo4j.

### REQ-RELIABILITY-001.0: Contain malformed input safely

**WHEN** the endpoint receives malformed Cypher, malformed parameters, an oversized request according to configured limits, or a malformed protocol interaction
**THEN** the server SHALL not panic, abort, corrupt the snapshot, or leak unbounded query state
**AND** subsequent valid connections SHALL remain serviceable after recoverable failures
**SHALL** record the failure category without recording secret values.

### REQ-RELIABILITY-002.0: Expose strict readiness

**WHEN** the service reports ready
**THEN** the Bolt listener, parser authority, snapshot manifest, mmap files, profile assumptions, and configured authentication SHALL all have passed validation
**AND** readiness SHALL become false if a required supervised component exits
**SHALL** reject queries while not ready.

### REQ-OBS-001.0: Emit a proof-carrying execution receipt

**WHEN** a query completes or fails after admission
**THEN** the system SHALL emit a receipt containing query hash, canonical plan hash, profile version, snapshot generation and hash, parameter-name set, result row count, result hash, elapsed phases, termination status, and resource high-water measurements
**AND** the receipt SHALL exclude raw parameter values by default
**SHALL** distinguish measured, unavailable, and not-applicable fields.

### REQ-CLAIM-001.0: Gate compatibility language on evidence

**WHEN** a release or document describes compatibility
**THEN** it SHALL name the profile, driver version, URI mode, query surface, transaction mode, dataset, and verification result
**AND** SHALL link the generated compatibility receipt
**SHALL** reject unqualified `Cypher compatible`, `Bolt compatible`, or `Neo4j compatible` wording.

## Test Matrix

### Required Test Artifacts

| artifact | purpose |
| --- | --- |
| `tests/cypher_walk_contract.rs` | Rust logical-plan and execution-unit contracts. |
| `tests/cypher_walk_differential.rs` | Tiny-graph Neo4j-versus-Knight-Bus semantic comparison. |
| `tests/bolt_driver_contract.py` | Official Python driver connection, query, record, failure, reset, and close behavior. |
| `tests/fixtures/cypher_walk_queries.json` | Original query text, semantic variants, parameters, expected plan, and expected support category. |
| `tests/fixtures/cypher_walk_graphs/` | Deterministic tiny graphs and independent expected endpoint rows. |
| `benchmarks/walk_hopper_v1/bench_cypher_bolt_compat.py` | Paired Neo4j/Knight Bus 60-query benchmark and receipt generator. |
| `reports/cypher_bolt_walk_v1/compatibility-receipt.json` | Machine-readable release evidence. |
| `reports/cypher_bolt_walk_v1/compatibility-summary.md` | Human-readable measurements, caveats, and verdict. |

Generated paths under `reports/` MAY remain ignored when repository policy
requires it, but a release SHALL preserve the immutable receipt as a release
artifact or committed evidence file.

### Requirement Traceability

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| `REQ-SCOPE-001.0` | `TEST-UNIT-SCOPE-001` | unit | only the neighborhood-walk operator enters the admitted plan enum | scope |
| `REQ-SCOPE-001.0` | `TEST-NEG-SCOPE-002` | negative | every additional logical operator fails before runtime access | scope |
| `REQ-API-001.0` | `TEST-INTEG-API-001` | integration | exact query text sent by driver reaches parse and compile path unchanged | query input |
| `REQ-CYPHER-001.0` | `TEST-UNIT-CYPHER-001` | unit | canonical three queries parse with pinned Neo4j parser | syntax |
| `REQ-CYPHER-001.0` | `TEST-NEG-CYPHER-002` | negative | malformed query returns syntax failure | syntax |
| `REQ-CYPHER-002.0` | `TEST-UNIT-CYPHER-003` | unit | direction, relationship, hops, projection, distinct, and order compile exactly | semantics |
| `REQ-CYPHER-003.0` | `TEST-UNIT-CYPHER-004` | unit | string parameter binds and extra parameters do not alter plan | parameters |
| `REQ-CYPHER-003.0` | `TEST-NEG-CYPHER-005` | negative | missing, null, and non-string node IDs fail distinctly | parameters |
| `REQ-CYPHER-004.0` | `TEST-NEG-CYPHER-006` | negative | valid unsupported clauses and expressions report first unsupported feature | support gate |
| `REQ-CYPHER-005.0` | `TEST-INTEG-CYPHER-007` | integration | syntax, support, and parameter failures remain distinct through driver | errors |
| `REQ-PLAN-001.0` | `TEST-GOLDEN-PLAN-001` | golden | semantic-preserving query variants serialize to identical plans | canonical plan |
| `REQ-PLAN-002.0` | `TEST-GOLDEN-PLAN-002` | golden | identical plans hash identically and semantic changes alter hash | plan identity |
| `REQ-EXEC-001.0` | `TEST-INTEG-EXEC-001` | integration | multiple queries share one runtime-open event | runtime lifecycle |
| `REQ-EXEC-002.0` | `TEST-UNIT-EXEC-002` | unit | all three traversal configurations return expected ordered rows | execution |
| `REQ-EXEC-003.0` | `TEST-PARITY-EXEC-003` | differential | absent start node returns successful empty result in both engines | Neo4j semantics |
| `REQ-EXEC-004.0` | `TEST-PARITY-EXEC-004` | differential | self-loop, cycle, and diamond endpoint rows match Neo4j | path semantics |
| `REQ-EXEC-005.0` | `TEST-INTEG-EXEC-005` | integration | deadline, row limit, and cancellation terminate and release query state | bounds |
| `REQ-BOLT-001.0` | `TEST-BOLT-DRIVER-001` | integration | Python driver 6.1.0 connects and records negotiated protocol | Bolt handshake |
| `REQ-BOLT-001.0` | `TEST-NEG-BOLT-002` | negative | invalid credentials fail before query execution | authentication |
| `REQ-BOLT-002.0` | `TEST-BOLT-DRIVER-003` | integration | `session.run`, iteration, `consume`, and close succeed | auto-commit |
| `REQ-BOLT-003.0` | `TEST-BOLT-DRIVER-004` | integration | record keys and values match Neo4j driver behavior | records |
| `REQ-BOLT-004.0` | `TEST-BOLT-DRIVER-005` | integration | typed failure reaches driver and later valid query succeeds | recovery |
| `REQ-BOLT-005.0` | `TEST-BOLT-DRIVER-006` | integration | sequential sessions and disconnects release local state | lifecycle |
| `REQ-BOLT-006.0` | `TEST-NEG-BOLT-007` | negative | excluded protocol surfaces fail without false success | honest boundary |
| `REQ-VERIFY-001.0` | `TEST-PARITY-GRAPH-001` | differential | all ten tiny graph families match exactly | semantic oracle |
| `REQ-VERIFY-002.0` | `TEST-GOLDEN-PARSER-001` | golden | parser outcome and compiled plan remain pinned | parser oracle |
| `REQ-VERIFY-003.0` | `TEST-SCALE-CORPUS-001` | scale integration | all 60 fixed 2 GB corpus results match in order | scale parity |
| `REQ-VERIFY-004.0` | `TEST-AUDIT-ORACLE-001` | audit | expected answers have independent Neo4j and fixture provenance | oracle hygiene |
| `REQ-PERF-001.0` | `TEST-PERF-MEMORY-001` | performance | receipt sums all compatibility-owned process RSS scopes | RAM accounting |
| `REQ-PERF-002.0` | `TEST-PERF-PAIRED-002` | performance | warm p99 and total peak RSS beat same-run Neo4j | product gate |
| `REQ-PERF-003.0` | `TEST-PERF-COLD-003` | performance | readiness and first-query phases are reported separately | cold start |
| `REQ-RELIABILITY-001.0` | `TEST-FUZZ-INPUT-001` | fuzz | malformed query, parameter, and protocol inputs do not crash service | robustness |
| `REQ-RELIABILITY-002.0` | `TEST-INTEG-READY-001` | integration | readiness requires parser, snapshot, listener, profile, and auth | readiness |
| `REQ-OBS-001.0` | `TEST-GOLDEN-RECEIPT-001` | golden | receipt has required fields and omits raw parameter values | observability |
| `REQ-CLAIM-001.0` | `TEST-DOC-CLAIM-001` | documentation | compatibility summary uses only authorized scoped wording | claims |

### Tiny Graph Fixture Matrix

| fixture | purpose | required query configurations |
| --- | --- | --- |
| Empty graph | No nodes or edges SHALL not crash admission or execution. | All three |
| Isolated node | Existing start with no neighbors SHALL return zero rows. | All three |
| Missing node | Absent start SHALL return successful zero rows. | All three |
| Forward edge | Direction SHALL not be accidentally inverted. | Forward one, reverse one |
| Reverse fan-in | Multiple direct dependents SHALL be ordered exactly. | Reverse one |
| Two-hop chain | One-to-two range SHALL include depth-one and depth-two endpoints. | Reverse one-to-two |
| Diamond | Two paths to one endpoint SHALL collapse under `DISTINCT`. | Reverse one-to-two |
| Self-loop | Start node SHALL appear when Neo4j includes it. | One-hop and one-to-two |
| Directed cycle | Relationship-unique path semantics SHALL match for the bounded range. | Reverse one-to-two |
| Unreachable component | Traversal SHALL remain inside the reachable component. | All three |

### Cypher Rejection Matrix

| query feature | required result |
| --- | --- |
| Malformed syntax | Syntax failure. |
| `OPTIONAL MATCH` | Unsupported-feature failure before runtime access. |
| `CREATE`, `MERGE`, `SET`, or `DELETE` | Read-only failure before runtime access. |
| Relationship type other than `DEPENDS_ON` | Unsupported graph-profile failure. |
| Unbounded `*` | Unsupported hop-range failure. |
| Maximum hop greater than two | Unsupported hop-range failure. |
| Arbitrary property filter | Unsupported property-operator failure. |
| Return of node, relationship, or path value | Unsupported result-shape failure. |
| Descending order | Unsupported ordering failure in v1. |
| Missing `ORDER BY` | Unsupported result-order contract unless a later profile defines unordered output. |
| Explicit transaction | Unsupported Bolt-surface failure. |
| Routing URI behavior | Unsupported Bolt-surface failure. |

## TDD Plan

### Implementation Boundaries

The first implementation SHOULD use these ownership boundaries:

```text
src/cypher/
  request.rs       query request, parameter types, and typed failures
  plan.rs          canonical logical plan and deterministic serialization
  compiler.rs      supported-AST validation and plan construction
  execution.rs     plan-to-runtime adaptation and result projection

src/bolt/
  server.rs        listener and supervised connection lifecycle
  session.rs       read-only auto-commit session state
  records.rs       result keys, records, summaries, and failure metadata

compat/cypher-parser-bridge/
  Neo4j Cypher DSL Parser adapter
  canonical AST-derived response schema

tests/
  cypher_walk_contract.rs
  cypher_walk_differential.rs
  bolt_driver_contract.py
```

These paths are architectural guidance. Equivalent module boundaries MAY be
used when direct implementation evidence shows a simpler fit, but parsing,
planning, execution, Bolt state, and verification SHALL remain separable.

### Four-Word Symbol Map

New implementation symbols SHOULD use these four-word names or equally clear
four-word equivalents:

| responsibility | proposed symbol |
| --- | --- |
| Parse with reference bridge | `parse_cypher_statement_exact` |
| Validate the supported subset | `validate_supported_query_shape` |
| Compile the logical operator | `compile_neighborhood_walk_plan` |
| Serialize the canonical plan | `serialize_canonical_plan_bytes` |
| Hash the canonical plan | `hash_canonical_plan_bytes` |
| Execute the compiled plan | `execute_neighborhood_walk_plan` |
| Convert endpoints to records | `project_endpoint_result_records` |
| Stream records over Bolt | `stream_bolt_result_records` |
| Convert failures for Bolt | `map_bolt_failure_metadata` |
| Compare result sequences | `compare_neo4j_results_exact` |
| Load corpus identity | `load_fixed_corpus_manifest` |
| Measure all owned processes | `measure_compatibility_process_memory` |
| Create the run receipt | `write_compatibility_execution_receipt` |

Suggested new data types:

- `ParsedCypherWalkRequest`
- `CompiledNeighborhoodWalkPlan`
- `ProjectedBoltResultRecord`
- `BoltQueryExecutionReceipt`

Existing public names such as `MmapWalkRuntime`, `WalkDirection`, `HopCount`,
and `query_entity_neighbors` SHALL be preserved unless an independently
approved refactor changes them.

### Stage 0: Freeze the Compatibility Packet

#### STUB

- Create `cypher_walk_queries.json` with the three canonical queries.
- Add semantic-preserving variants and the complete rejection matrix.
- Create tiny graph fixtures and independently written expected results.
- Define the canonical plan JSON schema and receipt JSON schema.

#### RED

- Add tests that reference the not-yet-existing compiler and Bolt endpoint.
- Confirm failures are missing-interface or connection-refused failures, not
  fixture or test-discovery failures.

#### GREEN

- No production implementation is required in Stage 0.
- Make fixture loaders and schema validators pass.

#### REFACTOR

- Remove duplicate query text from tests.
- Keep one canonical fixture packet as the source of truth.

#### VERIFY

- Hash every fixture file.
- Confirm all required graph and rejection cases are represented.

### Stage 1: Compile Cypher Into One Plan

#### STUB

- Add plan-structure, canonicalization, parameter, and rejection tests.
- Add the parser-oracle adapter contract.

#### RED

- Run only parser/compiler tests.
- Record failures for absent parsing and compilation interfaces.

#### GREEN

- Integrate the pinned Neo4j Cypher DSL Parser as parsing authority.
- Compile only the three required query shapes.
- Return typed rejection for every other valid AST shape.
- Produce deterministic plan bytes and hash.

#### REFACTOR

- Keep parser-specific classes outside the Rust logical-plan model.
- Remove string-pattern matching from semantic decisions.
- Keep original query text only for hashing, diagnostics, and receipt metadata.

#### VERIFY

- Run golden parser and canonical-plan tests.
- Upgrade neither parser nor fixture goldens in the same unreviewed change.

### Stage 2: Execute the Plan Through Persistent Runtime

#### STUB

- Add execution tests for the three configurations and ten tiny graph cases.
- Add missing-node, cancellation, deadline, and result-bound tests.

#### RED

- Confirm missing-node parity currently fails if dense-ID lookup escapes as an
  internal error.
- Confirm cycle, self-loop, and diamond behavior before assuming parity.

#### GREEN

- Adapt `CompiledNeighborhoodWalkPlan` to `MmapWalkRuntime` operations.
- Keep the runtime open across queries.
- Return projected `node_id` records and typed execution outcomes.

#### REFACTOR

- Separate traversal state from record projection.
- Remove per-query snapshot reopen paths.
- Make query-local allocations observable and releasable.

#### VERIFY

- Run Rust unit and integration tests.
- Run tiny-graph differential comparison against Neo4j.

### Stage 3: Add Read-Only Bolt Execution

#### STUB

- Add the official Python-driver lifecycle tests.
- Add auth, failure recovery, session close, and excluded-surface tests.

#### RED

- Confirm the driver cannot connect before the server exists.
- Capture the exact driver behavior expected during negotiation and recovery.

#### GREEN

- Implement only the protocol behavior exercised by the required driver tests.
- Route the original Bolt query and parameter map to the compiler.
- Stream records and a consumable summary.
- Support recovery after typed query failures.

#### REFACTOR

- Keep protocol state transitions separate from query execution state.
- Keep error-category mapping in one module.
- Ensure secrets and raw parameter values never enter ordinary logs or receipts.

#### VERIFY

- Run the driver contract repeatedly with fresh and reused connections.
- Capture negotiated protocol and driver version in the receipt.

### Stage 4: Run the Fixed 2 GB Proof

#### STUB

- Extend the existing paired benchmark to address Knight Bus through Bolt.
- Add complete-process memory accounting for the compatibility stack.
- Add raw result and plan hashes to the report.

#### RED

- Confirm the scale test fails clearly when the fixed artifact is absent.
- Restore or regenerate the artifact without changing the query corpus.
- Confirm preflight hashes before measuring.

#### GREEN

- Execute one unmeasured warm-up corpus on each engine.
- Execute the measured 60-query corpus on Neo4j and Knight Bus.
- Require exact ordered-result parity before accepting performance numbers.
- Emit machine-readable and human-readable receipts.

#### REFACTOR

- Share corpus loading and result comparison between existing and new runners.
- Keep Neo4j and Knight Bus execution paths independent.

#### VERIFY

- Run correctness, warm latency, total RSS, and cold-open measurements.
- Check the authorized compatibility wording against the receipt.

### Goal-Ready Execution Prompt

```text
/goal Implement the executable specification in docs_PMF_01/PMF006-Cypher-Bolt-Walk-Spec.md end to end using verification-first TDD.

Use:
- /Users/amuldotexe/.codex/skills/executable-specs-01/SKILL.md
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/.agents/skills/codebase-memory-mcp/SKILL.md
- /Users/amuldotexe/.codex/skills/tdd-task-progress-context-retainer/SKILL.md
- /Users/amuldotexe/.codex/skills/test-driven-development/SKILL.md
- /Users/amuldotexe/.codex/skills/verification-before-completion/SKILL.md

Scope:
- One algorithm only: bounded DEPENDS_ON neighborhood traversal.
- Required configurations: forward one hop, reverse one hop, reverse one-to-two hops.
- Query text must arrive unchanged through Neo4j Python driver 6.1.0 over direct Bolt.
- Use the pinned Neo4j Cypher DSL Parser as the parsing oracle.
- Knight Bus owns support validation, plan compilation, mmap execution, records, and receipts.
- Do not implement writes, explicit transactions, routing, arbitrary Cypher, or GDS algorithms.

Process:
1. Index the current repository and relevant parser modules with codebase-memory-mcp.
2. Run impact analysis before changing existing symbols.
3. Implement one requirement slice at a time using STUB, RED, GREEN, REFACTOR, VERIFY.
4. Keep docs_PMF_01/PMF006-Cypher-Bolt-Walk-Spec.md as the source of truth.
5. Track exact RED and GREEN commands and results in a resumable progress document.
6. Use the official driver as a black-box Bolt compatibility verifier.
7. Run Neo4j-versus-Knight-Bus differential tests on adversarial tiny graphs before the 2 GB run.
8. Restore or regenerate the fixed v002 artifact only after validating manifest and corpus identity.
9. Count the complete Knight Bus compatibility stack in RAM measurements.
10. Do not claim compatibility until every mandatory requirement has a passing mapped test.

Completion requires:
- Every mandatory REQ ID mapped to a passing test.
- Exact ordered parity for all tiny graph fixtures.
- Exact ordered parity for all 60 fixed 2 GB corpus queries.
- Official Neo4j Python driver 6.1.0 can connect, run, iterate, consume, recover, and close.
- Warm p99 and total compatibility-stack RSS beat paired Neo4j in the same run.
- Cold-open cost is separately reported.
- Compatibility receipt and summary use only the authorized scoped claim.
- Full build, tests, formatting, lint, diff checks, and code-graph change review pass.
```

## Quality Gates

### Per-Slice Gate

- [ ] The active requirement has a committed test fixture.
- [ ] The RED run failed for the expected missing behavior.
- [ ] The GREEN run passes the narrow test.
- [ ] Existing tests remain green.
- [ ] New symbols follow four-word naming unless preserving a compatible public
      name.
- [ ] No `TODO`, `STUB`, or `FIXME` is introduced in production code.
- [ ] No parser-specific object leaks across the logical-plan boundary.
- [ ] Unsupported behavior fails before runtime graph access.

### Cypher Gate

- [ ] The pinned Neo4j parser accepts all supported fixtures.
- [ ] Syntax, support, and parameter failures remain distinguishable.
- [ ] Semantic-preserving variants produce byte-identical plans.
- [ ] Unsupported valid queries identify the first unsupported feature.
- [ ] No semantic decision relies on regex or substring matching.

### Execution Gate

- [ ] All three traversal configurations pass.
- [ ] All ten tiny graph fixture families match Neo4j exactly.
- [ ] Missing start node returns an empty successful result.
- [ ] Cycle, self-loop, diamond, and start-node reappearance semantics match.
- [ ] Deadline, row bound, and cancellation release query-local state.
- [ ] Snapshot files are opened once per server lifecycle, not once per query.

### Bolt Gate

- [ ] Python Neo4j driver `6.1.0` connects through direct `bolt://`.
- [ ] Configured valid credentials succeed and invalid credentials fail.
- [ ] `session.run`, record iteration, `consume`, reset/recovery, and close pass.
- [ ] Record key is exactly `node_id` and value type is string.
- [ ] Excluded protocol surfaces fail without false success.
- [ ] A failed query does not poison the next valid query.

### Scale Gate

- [ ] The v002 raw corpus and snapshot identity are recorded and hashed.
- [ ] Exactly 60 expected corpus queries are run against both systems.
- [ ] Every ordered row sequence matches.
- [ ] Performance numbers are discarded if correctness fails.
- [ ] Knight Bus total compatibility-stack RSS is lower than paired Neo4j.
- [ ] Knight Bus warm Bolt p99 is lower than paired Neo4j.
- [ ] Cold readiness and first-query latency are separately reported.
- [ ] Raw samples and process scopes are retained in the receipt.

### Pre-Commit Gate

- [ ] `cargo fmt --check` passes.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [ ] `cargo test --all-targets --all-features` passes.
- [ ] Parser-bridge build and tests pass with the pinned dependency source.
- [ ] Python driver contract tests pass in the pinned benchmark environment.
- [ ] Every mandatory `REQ-*` ID has at least one test-matrix row.
- [ ] Every mandatory test ID exists in source or generated test inventory.
- [ ] No unmeasured performance or compatibility claim is introduced.
- [ ] `git diff --check` passes.
- [ ] Code-graph impact analysis was run before editing existing symbols.
- [ ] Code-graph change detection reports only expected modules and flows.
- [ ] Clarity visualization is generated when three or more files change, when
      the executable is available.

### Release Gate

A release passes only when all of these are true:

1. Tiny-graph semantic parity is exact.
2. All 60 v002 query results are exact and ordered.
3. Official driver lifecycle tests pass.
4. Unsupported surfaces fail honestly.
5. Total compatibility-stack RSS beats paired Neo4j.
6. Warm end-to-end Bolt p99 beats paired Neo4j.
7. Cold-open behavior is disclosed.
8. The proof-carrying receipt is retained.
9. Public wording names the exact compatibility profile.

## Source Evidence

- The three canonical Cypher queries are defined in
  `benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py`, function
  `build_neo4j_runner_now`.
- Python Neo4j driver `6.1.0` is pinned in
  `benchmarks/walk_hopper_v1/requirements.txt`.
- Current runtime execution is rooted at `src/app.rs`, function
  `query_snapshot_from_path`, and `src/runtime.rs`, method
  `MmapWalkRuntime::query_entity_neighbors`.
- The v002 corpus and measurement record are in
  `journal-tests-202604-v002.md`.
- Neo4j Cypher DSL Parser usage and Apache-2.0 licensing are in
  `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl-parser/README.adoc`
  and `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl-parser/pom.xml`.

## Open Questions

These questions do not weaken the behavioral contracts above. Their defaults
allow implementation to begin without another planning round.

| id | question | default decision | blocking condition |
| --- | --- | --- | --- |
| `OPEN-001` | Does "one algorithm" mean neighborhood walk rather than PageRank? | Yes. This spec uses the v002 walk algorithm discussed immediately before the request. | Block only if the intended algorithm was PageRank or another GDS procedure. |
| `OPEN-002` | Should the Neo4j parser ship as a supervised JVM child or be embedded through JNI? | Start with a supervised long-lived parser child behind one Knight Bus command. | Change only if measured process overhead fails the total-RSS release gate. |
| `OPEN-003` | Which exact Bolt protocol version should be claimed? | Claim only the version actually negotiated by Python driver 6.1.0 and recorded by tests. | Block if the driver cannot negotiate without adding excluded protocol behavior. |
| `OPEN-004` | Is optional `:Entity` label syntax mandatory in the first green slice? | Parse and validate it after the unlabeled canonical queries pass. | Block release only if the real target query includes the label. |
| `OPEN-005` | Should equivalent `WHERE n.node_id = $node_id` syntax enter v1? | Keep it candidate-only until it passes the complete parity matrix. | Block release only if the real target query uses this form. |
| `OPEN-006` | How should one-hop parallel relationships behave? | The v002 profile SHALL assert simple-graph input. General multiplicity remains unsupported. | Block if the restored corpus contains parallel `DEPENDS_ON` relationships. |
| `OPEN-007` | Where is the immutable 2 GB artifact restored from? | Use the existing generator and recorded v002 parameters if no preserved artifact is available. | Block scale verification until raw corpus, snapshot, corpus, and expected-answer identities are fixed. |
| `OPEN-008` | Should basic authentication be production hardened in v1? | Implement configured local credentials sufficient for the official-driver contract; do not claim internet-safe deployment. | Block external deployment, not local compatibility verification. |
| `OPEN-009` | How many concurrent queries must v1 support? | Correct sequential auto-commit behavior is mandatory; concurrency is characterization-only. | Block only if a stated target application requires concurrent in-flight queries for the first proof. |
| `OPEN-010` | Should result production stream directly or materialize after traversal? | Preserve exact correctness first; record materialization bytes and move to streaming when the current runtime boundary permits it. | Block if result high-water causes the total-RSS release gate to fail. |

## Implementation Result: 2026-08-07

The scoped implementation is complete and the paired scale release gate passed.
The immutable evidence is retained under
`docs_PMF_01/evidence/cypher-bolt-walk-v1/`.

### Implemented Boundary

- Neo4j Python driver `6.1.0` connects through direct `bolt://` and negotiates
  Bolt `5.4` with Knight Bus.
- The original Cypher text reaches a native openCypher AST parser, strict
  support validator, canonical logical planner, and mmap execution kernel.
- The three authorized forward/reverse, one/two-hop query shapes execute as
  read-only auto-commit queries with Neo4j-shaped `node_id` records.
- Syntax, unsupported-feature, parameter, admission, deadline, row-limit, and
  authentication failures remain typed and recoverable.
- Snapshot startup requires an exact `compatibility-profile.json` manifest and
  reuses one already-open `MmapWalkRuntime` for all admitted queries.
- Successful and admitted-failure summaries carry redacted proof receipts.

### Verification Result

| Gate | Result |
| --- | --- |
| Complete Rust workspace | 65 passed |
| Cypher/compiler/execution/profile Rust contracts | 14 passed |
| Official-driver contracts | 8 passed |
| Neo4j parser-oracle contracts | 2 passed |
| Real-Neo4j adversarial differential contracts | 2 passed |
| Benchmark-harness contracts | 5 passed |
| 2 GB source corpus, 60 queries x 3 measured passes | exact ordered parity |
| Paired performance gate | passed all 4 conditions |

### Paired Measurements

| Measurement | Knight Bus | Neo4j 2026.07.0 | Result |
| --- | ---: | ---: | --- |
| Warm Bolt p99, 180 samples | 3.970300 ms | 5.302670 ms | Knight Bus 1.335584x faster |
| Peak stack RSS | 234,176,512 B | 374,046,720 B | Knight Bus 1.597285x lower |
| Ordered aggregate result hash | `dbda232863c2d4249e829bc665b430a42b9cba13ab3fb92c82f11044b0969ab2` | same | exact |
| Neo4j access path | not applicable | verified `NodeIndexSeek` | admitted |

The full receipt records corpus and snapshot hashes, raw latency samples,
process scope, protocol versions, readiness, first-query time, plan hashes, and
redacted per-query execution receipts. See
`docs_PMF_01/evidence/cypher-bolt-walk-v1/compatibility-receipt.json`.

### Explicit Limitations

- Boltr `0.2.0` invokes the backend inline and does not expose transport EOF
  while execution is in progress. Kernel-level cancellation is implemented and
  tested, but a socket disconnect cannot immediately preempt a running callback;
  the configured finite deadline and row cap bound its cleanup.
- Neo4j cold server boot was not measured because the comparator did not own the
  Homebrew service. Knight Bus readiness and both engines' first-query times are
  recorded separately.
- mmap residency is marked unavailable on this macOS runner because the active
  `psutil.Process` API lacks `memory_maps`; process RSS and snapshot bytes are
  still recorded.
- Compatibility remains intentionally limited to this named profile. It is not
  general Cypher, general Bolt, or general Neo4j compatibility.

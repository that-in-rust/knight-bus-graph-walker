# Neo4j Compatibility Canary Matrix

This matrix defines canaries for zero-client-change compatibility. It does not
claim the canaries pass today; it names the minimum executable probes needed
before v003 can claim a Neo4j-compatible surface.

## PRD Plane

| plane | canary implication |
| --- | --- |
| OLTP storage | driver, Cypher, value, transaction, auth, and error canaries exercise Neo4j-shaped serving |
| Projection Build Store | GDS graph project/list/drop canaries exercise build/control semantics |
| OLAP snapshot storage | GDS stream/stats/mutate/write/estimate canaries exercise published snapshot reads and artifacts |

## Canary Groups

| group | canary | command shape | expected result | failure classification | evidence |
| --- | --- | --- | --- | --- | --- |
| Bolt | handshake success | `neo4j-testkit` driver handshake suite against v003 server | official driver opens a session | ProtocolCompatibilityFailure | `gitrefrepo/neo4j-testkit-src` |
| Bolt | handshake rejection | send unsupported Bolt version | stable Neo4j-like rejection | ProtocolCompatibilityFailure | `gitrefrepo/neo4j-docs-bolt-src` |
| Bolt | auth metadata | driver connects with auth token variants | auth success/failure matches configured policy | AuthCompatibilityFailure | NeedsSource |
| Bolt | session lifecycle | open session, run query, consume, close | session close releases resources | DriverLifecycleFailure | driver repos under `gitrefrepo/neo4j-*-driver-src` |
| official drivers | Python | Python driver runs `RETURN 1 AS n` and simple transaction | result value and transaction semantics match | DriverCompatibilityFailure | `gitrefrepo/neo4j-python-driver-src` |
| official drivers | Java | Java driver runs managed read/write transaction | result, retry, and error envelopes match | DriverCompatibilityFailure | `gitrefrepo/neo4j-java-driver-src` |
| official drivers | JavaScript | JS driver consumes async result stream | PackStream value mapping matches | DriverCompatibilityFailure | `gitrefrepo/neo4j-javascript-driver-src` |
| official drivers | Go | Go driver runs explicit transaction | context cancellation and errors are stable | DriverCompatibilityFailure | `gitrefrepo/neo4j-go-driver-src` |
| official drivers | .NET | .NET driver runs session and transaction flow | records and errors deserialize correctly | DriverCompatibilityFailure | `gitrefrepo/neo4j-dotnet-driver-src` |
| Cypher procedure path | CALL/YIELD | `CALL gds.pageRank.stream($graph, {}) YIELD nodeId, score RETURN nodeId, score` | row schema follows registry | ProcedureAbiFailure | GDS registry |
| Cypher procedure path | invalid config | call P1 procedure with wrong config type | deterministic invalid argument error | ConfigCompatibilityFailure | NeedsSource |
| Cypher procedure path | missing procedure | call `gds.thisDoesNotExist.stream` | procedure-not-found error | ProcedureAbiFailure | Support semantics |
| values | scalar values | `RETURN null, true, 1, 1.5, 'x'` | driver value mapping matches Neo4j | ValueMappingFailure | driver repos |
| values | collection values | `RETURN [1,2], {a:1}` | list/map mapping matches Neo4j | ValueMappingFailure | driver repos |
| values | graph values | create and return node, relationship, path | identity, labels, types, properties map correctly | ValueMappingFailure | `gitrefrepo/neo4j-src/community/kernel-api` NeedsSource |
| values | temporal/spatial | return date/time/datetime/duration/point | type mapping matches official drivers | ValueMappingFailure | NeedsSource |
| errors | unsupported registered | call `UnsupportedButRegistered` procedure | stable `ProcedureCallFailed` from support semantics | ErrorEnvelopeFailure | `Support-Status-Runtime-Semantics.md` |
| errors | budget rejection | run P1 estimate over budget then execute | pre-execution memory rejection | MemoryContractFailure | formula book |
| APOC boundary | known supported alias | call any intentionally supported APOC alias | row shape and error semantics declared | ApocCompatibilityFailure | `gitrefrepo/neo4j-apoc-src` NeedsSource |
| APOC boundary | known unsupported registered | call registered unsupported APOC procedure | deterministic unsupported behavior | ApocCompatibilityFailure | NeedsSource |
| tooling | cypher-shell | connect and run smoke query | exit code and output parseable | ToolingCompatibilityFailure | NeedsSource |
| tooling | Browser-like query | browser-style metadata query | result shape expected by browser workflow | ToolingCompatibilityFailure | NeedsSource |
| tooling | representative app | run one unmodified app fixture | no client-side code changes | ApplicationCompatibilityFailure | NeedsSource |

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| CAN-001 | DirectSource | `docs_PRD03/prd-l1.md:68-87` | API compatibility, OLTP boundary, OLAP boundary | canaries must cover both transactional and GDS paths | PRD relaxes zero-client-change requirement |
| CAN-002 | DirectSource | `gitrefrepo/neo4j-gds-src/proc/centrality/src/main/java/org/neo4j/gds/pagerank/PageRankStreamProc.java:37` | PageRank stream proc class | procedure canaries must cover actual GDS procedure names | registry omits real procedure annotations |
| CAN-003 | NeedsSource | `gitrefrepo/neo4j-docs-bolt-src` | Bolt version matrix | exact handshake canaries still need source-line extraction | driver compatibility can be proven without Bolt handshake tests |
| CAN-004 | Inference | `Support-Status-Runtime-Semantics.md` | unsupported known vs unknown | deterministic unsupported behavior is part of compatibility | Neo4j clients require missing-procedure behavior for all unsupported procedures |

## Verification Commands

```bash
rg -n "Bolt|Python|Java|JavaScript|Go|\\.NET|CALL|YIELD|APOC|cypher-shell|browser" docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md
rg -n "neo4j-testkit-src|neo4j-python-driver-src|neo4j-java-driver-src|neo4j-javascript-driver-src|neo4j-go-driver-src|neo4j-dotnet-driver-src|neo4j-docs-bolt-src" docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md
```


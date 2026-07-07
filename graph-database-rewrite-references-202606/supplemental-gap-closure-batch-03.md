# Supplemental Gap Closure Batch 03: Official Client And GDS Surface Contracts

Date: 2026-07-07

Active objective:

`/Users/amuldotexe/.codex/attachments/5ec8f8ed-6090-477c-b632-26c893e40e1a/goal-objective.md`

This batch upgrades remaining Neo4j-family compatibility repositories from
metadata-only browse status to direct-source evidence. The slice is deliberately
application-facing: official drivers, Browser, OGM, GDS client tests, and the
GDS MCP/agent layer.

The core thesis:

```text
A Rust Neo4j rewrite cannot treat "compatibility" as only Bolt parsing,
Cypher parsing, or record storage.

Compatibility is also:
driver backpressure,
driver retry semantics,
bookmark manager behavior,
routing table refresh,
typed vector wire values,
Browser query/result UX,
OGM session identity behavior,
GDS procedure API-spec coverage,
GDS Arrow endpoint coverage,
agent/tool result limiting,
and graph projection/catalog shape.
```

For the low-RAM rewrite, these contracts create a useful division:

```text
OLTP-visible behavior must stay boringly compatible.
OLAP physical storage can be radical only behind that compatibility wall.
```

## Evidence Scope

Directly read repositories in this batch:

| repository | local path | status |
| --- | --- | --- |
| Neo4j Java Driver | `gitrefrepo/Neo4j family/neo4j-java-driver-src` | direct source cited |
| Neo4j JavaScript Driver | `gitrefrepo/Neo4j family/neo4j-javascript-driver-src` | direct source cited |
| Neo4j .NET Driver | `gitrefrepo/Neo4j family/neo4j-dotnet-driver-src` | direct source cited |
| Neo4j GDS Client | `gitrefrepo/Neo4j family/neo4j-gds-client-src` | direct source cited |
| Neo4j Browser | `gitrefrepo/Neo4j family/neo4j-browser-src` | direct source cited |
| Neo4j OGM | `gitrefrepo/Neo4j family/neo4j-ogm-src` | direct source cited |
| GDS Agent | `gitrefrepo/Neo4j family/gds-agent-src` | direct source cited; CGC attempted and failed |

CodeGraphContext evidence:

- JavaScript driver routing subtree was previously indexed at:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-js-driver-routing-20260707-065124`.
- `gds-agent-src` whole-repo and `mcp_server/src` indexing were attempted in
  this batch, but both failed with:
  `'NoneType' object has no attribute 'split'`.
- Therefore GDS Agent claims below are based on direct source reads, not CGC
  graph conclusions.

## Pattern 01: Driver Fetch Size Is A Backpressure Contract

Found in:

- Java Driver:
  `gitrefrepo/Neo4j family/neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Config.java:703-724`
- Java Driver:
  `gitrefrepo/Neo4j family/neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/async/NetworkSession.java:184-213`

Source shape:

```text
Config.withFetchSize(size)
  - default is 1000
  - -1 disables backpressure and pulls all records
  - invalid zero/negative values rejected except -1

NetworkSession.autoCommitRun(...)
  - builds ResultCursorImpl with fetchSize
  - writes RUN
  - writes PULL(-1, fetchSize)
```

Why it matters:

Fetch size is not a tuning afterthought. The Java driver exposes it as a public
control over result population and server pressure. The server must honor this
observable contract or client memory behavior changes.

Rust rewrite translation:

```rust
struct ResultPullBudget {
    requested_records: PullCount,
    fetch_size: FetchSize,
}

enum FetchSize {
    Bounded(NonZeroU64),
    FetchAll,
}
```

How agents should apply it:

- Never implement query execution as "materialize all rows, then send".
- Make `PULL` batching a server-side streaming state machine.
- Include tests where a client requests a small fetch size and the server emits
  records in batches without overproducing.
- In OLAP procedures, stream rows from sidecars/results and prefer mutate/write
  modes for graph-scale output.

Memory implications:

- Correct fetch-size behavior protects the client and server from accidental
  unbounded result materialization.
- It also prevents OLAP algorithms from claiming low RAM while the final result
  stream explodes memory.

Anti-pattern:

```text
Run algorithm -> collect Vec<Row> for all nodes -> serialize after completion.
```

Preferred pattern:

```text
Run algorithm -> result cursor over bounded row source -> PULL consumes batches.
```

## Pattern 02: Retry Logic Is A Compatibility Surface, Not A Helper Loop

Found in:

- Java Driver:
  `gitrefrepo/Neo4j family/neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/retry/ExponentialBackoffRetryLogic.java:111-160`
- Java Driver:
  `gitrefrepo/Neo4j family/neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/async/NetworkSession.java:205-218`

Source shape:

```text
retry(work)
  catches Throwable
  extracts possible termination cause
  retries only RetryableException
  starts elapsed timer on first retryable failure
  computes delay with jitter
  sleeps
  multiplies next delay
  records suppressed errors

autoCommitRun(...)
  sends telemetry + RUN + PULL
  tracks whether idempotent auto-commit failures may retry
```

Why it matters:

Applications depend on driver retry behavior. A server rewrite can expose
different transient/permanent error classifications accidentally, and that
changes what drivers retry.

Rust rewrite translation:

```rust
enum ErrorRetryClass {
    RetryableTransient,
    Fatal,
    UserCode,
    CommitUnknown,
}

struct RetryEnvelope {
    classification: ErrorRetryClass,
    gql_status: Option<GqlStatus>,
    neo4j_code: Option<Neo4jErrorCode>,
}
```

How agents should apply it:

- Preserve Neo4j error codes and status families where official drivers use
  them to decide retry behavior.
- Write compatibility tests against official drivers for retryable transient
  errors, non-retryable user errors, and commit uncertainty.
- For OLAP procedures, separate deterministic "estimate failed budget" errors
  from transient execution errors.

Memory implications:

Retries must not leak partially materialized result buffers, transaction state,
or algorithm scratch. Every retry boundary should have an explicit cleanup
contract.

## Pattern 03: Bookmark Managers Must Be Fast, Nonblocking, And External-Callback Safe

Found in:

- Java Driver:
  `gitrefrepo/Neo4j family/neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/BookmarkManager.java:25-48`
- .NET Driver:
  `gitrefrepo/Neo4j family/neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Public/IBookmarkManager.cs:21-50`
- .NET Driver:
  `gitrefrepo/Neo4j family/neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Internal/DefaultBookmarkManager.cs:24-92`
- .NET Driver:
  `gitrefrepo/Neo4j family/neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Internal/AsyncSession.cs:196-224`

Source shape:

```text
Java BookmarkManager
  MUST NOT block for extended periods
  must avoid calling driver
  updateBookmarks(previous, new)
  getBookmarks()

.NET DefaultBookmarkManager
  HashSet storage
  supplier callback
  onBookmarks callback
  SemaphoreSlim lock
  update removes previous and adds new
  get snapshots under lock and unions supplier results
```

Why it matters:

Bookmarks are causal consistency state at the API edge. They are not merely a
driver implementation detail. Server-side response metadata must be compatible
enough for bookmark managers to keep sessions causally ordered.

Rust rewrite translation:

```rust
struct BookmarkSet {
    values: BTreeSet<Bookmark>,
}

trait BookmarkEmitter {
    fn latest_bookmark(&self) -> Option<Bookmark>;
}
```

How agents should apply it:

- Keep bookmark production in transaction commit/result summary code.
- Do not couple bookmark callbacks to storage locks or long OLAP work.
- Tests should assert bookmark updates on commit, no bookmark update on failed
  transaction, and bookmark propagation through managed transactions.

Concurrency implications:

- Bookmark update paths must be short and lock-minimal.
- External callback hooks must not run while holding core storage locks.

## Pattern 04: Routing Tables Are Per-Database State Machines With Fallback

Found in:

- JavaScript Driver:
  `gitrefrepo/Neo4j family/neo4j-javascript-driver-src/packages/bolt-connection/src/connection-provider/connection-provider-routing.js:146-185`
- JavaScript Driver:
  `gitrefrepo/Neo4j family/neo4j-javascript-driver-src/packages/bolt-connection/src/connection-provider/connection-provider-routing.js:368-405`

Source shape:

```text
acquireConnection(...)
  builds database-specific error handler
  checks server-side routing/home DB case
  asks for _freshRoutingTable(...)
  passes bookmarks, impersonated user, auth, database-name callback

_freshRoutingTable(...)
  gets routing table for database
  returns current table if not stale for access mode
  refreshes if stale
  resolves database name from refreshed table

_refreshRoutingTable(...)
  chooses seed-router fallback to known routers
  or known-router fallback to seed router
```

Why it matters:

Even a single-node Community Edition rewrite should model routing as a
compatibility contract. Official drivers still send routing metadata, database
selection, access mode, impersonation, and bookmarks through acquisition paths.

Rust rewrite translation:

```rust
struct RoutingTable {
    database: DatabaseName,
    routers: Vec<ServerAddress>,
    readers: Vec<ServerAddress>,
    writers: Vec<ServerAddress>,
    expires_at: Instant,
}

enum RoutingMode {
    SingleNodeCompat,
    Cluster,
}
```

How agents should apply it:

- Implement a single-node routing table rather than pretending routing does not
  exist.
- Preserve database-name resolution and access-mode handling in Bolt metadata.
- Test official drivers in routed and direct URI modes.

## Pattern 05: Vector Values Are Typed Wire Values, Not Generic Lists

Found in:

- JavaScript Driver:
  `gitrefrepo/Neo4j family/neo4j-javascript-driver-src/packages/core/src/vector.ts:20-88`
- .NET Driver:
  `gitrefrepo/Neo4j family/neo4j-dotnet-driver-src/Neo4j.Driver/Neo4j.Driver/Internal/IO/ValueSerializers/VectorSerializer.cs:24-76`

Source shape:

```text
JavaScript Vector
  supports INT8, INT16, INT32, INT64, FLOAT32, FLOAT64
  wraps TypedArray
  infers vector type from typed array class
  exposes asTypedArray/getType

.NET VectorSerializer
  struct signature 'V'
  struct size 2
  reads type marker
  reads flattened bytes
  writes type marker and byte stream
```

Why it matters:

Neo4j compatibility now includes vector values at the driver/wire boundary.
Treating vectors as generic arrays loses element width, storage compactness, and
wire compatibility.

Rust rewrite translation:

```rust
enum VectorValue<'a> {
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    F32(&'a [f32]),
    F64(&'a [f64]),
}

struct VectorColumnManifest {
    element_type: VectorElementType,
    dimension: u32,
    bytes_path: PathBuf,
    offsets_path: Option<PathBuf>,
}
```

How agents should apply it:

- Preserve vector element type through PackStream, property storage, Arrow/GDS
  sidecars, and OLAP embedding outputs.
- Do not widen every embedding to `f64` unless the API explicitly requires it.
- Include round-trip tests for each element width.

Memory implications:

- `FLOAT32` and `INT8` vectors are central to a low-RAM GDS story.
- Widening vectors silently can destroy the 50GB-on-8GB budget.

## Pattern 06: GDS API Spec Coverage Turns Procedure Surface Into An ABI

Found in:

- GDS Client:
  `gitrefrepo/Neo4j family/neo4j-gds-client-src/tests/integrationV2/procedure_surface/gds_api_spec.py:9-92`
- GDS Client:
  `gitrefrepo/Neo4j family/neo4j-gds-client-src/tests/integrationV2/procedure_surface/api_spec_coverage_test_helper.py:35-115`

Source shape:

```text
gds_api_spec.py
  SourceKind POSITIONAL/CONFIG
  Parameter(name, type, sourceKind, positionIndex, defaultValue)
  ReturnField(name, type)
  Mode(mode, parameters, returnFields)
  EndpointWithModesSpec.callable_modes()

api_spec_coverage_test_helper.py
  known excluded endpoints
  base endpoint mapping aliases
  ignored parameter patterns
  expected parameter aliases
```

Why it matters:

This is the strongest pattern for "all GDS surface" compatibility. The API
surface is not a pile of algorithms; it is a versioned callable inventory with
parameters, modes, aliases, return fields, exclusions, and defaults.

Rust rewrite translation:

```rust
struct GdsEndpointSpec {
    name: ProcedureName,
    positional: Vec<ParameterSpec>,
    config: Vec<ParameterSpec>,
    modes: Vec<GdsModeSpec>,
}

enum SupportLevel {
    ImplementedExactLowRam,
    ImplementedCompatLatency,
    RegisteredUnsupported,
    DeferredWithReason,
}
```

How agents should apply it:

- Generate or maintain a GDS endpoint inventory before implementing kernels.
- Every procedure should be either implemented or registered unsupported with a
  deterministic error.
- Tests should assert no unknown `gds.*` endpoint silently vanishes.

Testing implications:

- Procedure inventory tests must come before PageRank tests.
- Parameter/default/result-shape tests must come before algorithm-performance
  tests.

## Pattern 07: GDS Arrow Actions Need Dynamic Endpoint Coverage

Found in:

- GDS Client:
  `gitrefrepo/Neo4j family/neo4j-gds-client-src/tests/integrationV2/procedure_surface/session/test_session_arrow_endpoint_coverage.py:80-170`
- GDS Client:
  `gitrefrepo/Neo4j family/neo4j-gds-client-src/tests/integrationV2/procedure_surface/session/test_walking_skeleton.py:16-62`

Source shape:

```text
test_algo_coverage(...)
  asks Arrow client for v2 actions
  filters pathfinding, centrality, community, similarity, embedding
  walks endpoint object tree
  asserts no unexpected missing endpoints

test_pipeline_coverage(...)
  asks Arrow client for pipeline actions
  filters unmapped endpoints
  asserts no unexpected missing endpoints

test_walking_skeleton(...)
  projects graph
  runs WCC mutate
  runs PageRank stream
  starts FastRP write
```

Why it matters:

GDS compatibility is dynamic. It depends not only on local class names but on
available remote actions and walking-skeleton behavior across projection,
mutation, stream, and write modes.

Rust rewrite translation:

```rust
trait GdsSurfaceProvider {
    fn list_actions(&self) -> Vec<GdsAction>;
    fn resolve_endpoint(&self, action: &GdsAction) -> Option<GdsEndpointSpec>;
}
```

How agents should apply it:

- Add a "walking skeleton" test as soon as the Rust server can project a graph:
  project -> WCC mutate -> PageRank stream -> FastRP write.
- Keep Arrow/DataFrame-style output compatibility separate from algorithm
  internals.

## Pattern 08: Browser Compatibility Includes Query UX, Result Mapping, And Cancellation

Found in:

- Neo4j Browser:
  `gitrefrepo/Neo4j family/neo4j-browser-src/src/browser/modules/Editor/MainEditor.tsx:227-310`
- Neo4j Browser:
  `gitrefrepo/Neo4j family/neo4j-browser-src/src/shared/services/bolt/handleBoltWorkerMessage.ts:74-150`
- Neo4j Browser:
  `gitrefrepo/Neo4j family/neo4j-browser-src/src/shared/services/bolt/boltMappings.ts:42-199`
- Neo4j Browser:
  `gitrefrepo/Neo4j family/neo4j-browser-src/src/browser/modules/Stream/Queries/QueriesFrame.tsx:149-226`

Source shape:

```text
MainEditor
  execute command from editor contents
  sends CYPHER_REQUEST with query, queryType, params
  applies graph types to params

Bolt worker
  ensures connection
  chooses transaction type
  applies graph types to parameters
  posts cypher response or error message
  handles cancel transaction messages

boltMappings
  converts Neo4j records to table arrays
  converts ints recursively
  extracts path rows, nodes, relationships
  extracts plan/profile objects
  handles page cache hit/miss fields

QueriesFrame
  runs SHOW TRANSACTIONS
  maps fields to query rows
  runs TERMINATE TRANSACTIONS for cancellation
```

Why it matters:

Browser is a compatibility canary. Users will notice if result records, paths,
plans, profile fields, query cancellation, and parameter typing behave oddly.

Rust rewrite translation:

```rust
struct BrowserCompatResult {
    records: Vec<RecordBatch>,      // streamed, not all materialized
    summary: QuerySummary,
    plan: Option<PlanTree>,
    profile: Option<ProfileTree>,
}
```

How agents should apply it:

- Preserve record key order, Neo4j integer behavior, path decomposition, plan
  tree fields, and query metadata.
- Implement `SHOW TRANSACTIONS` and `TERMINATE TRANSACTIONS` compatibility
  before claiming Browser support.
- Include Browser-oriented golden fixtures for paths, profiles, and errors.

Memory implications:

- The Browser mapping code converts result structures for display. The server
  must stream and summarize without requiring Browser-scale result buffering.

## Pattern 09: OGM Session Semantics Depend On Identity Maps And Depth

Found in:

- Neo4j OGM:
  `gitrefrepo/Neo4j family/neo4j-ogm-src/core/src/main/java/org/neo4j/ogm/session/Session.java:36-90`
- Neo4j OGM:
  `gitrefrepo/Neo4j family/neo4j-ogm-src/core/src/main/java/org/neo4j/ogm/session/Neo4jSession.java:664-770`
- Neo4j OGM:
  `gitrefrepo/Neo4j family/neo4j-ogm-src/core/src/main/java/org/neo4j/ogm/context/MappingContext.java:43-170`
- Neo4j OGM:
  `gitrefrepo/Neo4j family/neo4j-ogm-src/core/src/main/java/org/neo4j/ogm/context/EntityGraphMapper.java:67-120`

Source shape:

```text
Session
  not thread safe
  typical use is one session per thread
  depth controls load/save traversal
  cached entities return cached instance
  properties reset to original values
  relationships merge with loaded relationships

Neo4jSession
  builds query statements based on node vs relationship entity
  strict querying uses static labels or relationship type
  negative node depth chooses path load builder
  negative relationship depth is rejected

MappingContext
  maps Neo4j id -> entity
  maps primary id -> entity
  maps primary id -> native id
  maps relationship entities
  remembers relationships and identity map

EntityGraphMapper
  tracks current depth
  maps entity graph into multi-statement Cypher compiler context
```

Why it matters:

OGM reveals how applications experience Neo4j as an object graph, not only as
Cypher strings. Compatibility requires stable identity, depth semantics,
relationship merging, and query generation behavior.

Rust rewrite translation:

```rust
struct SessionIdentityMap {
    node_by_native_id: HashMap<NodeId, EntityHandle>,
    node_by_primary_id: HashMap<LabelPrimaryKey, EntityHandle>,
    relationship_by_native_id: HashMap<RelationshipId, EntityHandle>,
}

enum LoadDepth {
    Bounded(u32),
    UnlimitedNodePath,
}
```

How agents should apply it:

- Do not make server-side shortcuts that break object-mapping assumptions, such
  as unstable identity values or missing relationship endpoint metadata.
- Keep OLAP result sidecars distinct from OLTP writeback semantics.
- Write compatibility tests for depth `0`, depth `1`, bounded depth, and
  negative/unlimited node depth behavior where exposed.

Concurrency implications:

- OGM sessions are explicitly not thread-safe, but server transaction/session
  state still must tolerate concurrent client sessions.

## Pattern 10: GDS Agent Shows Registry-First Tooling And Output Guardrails

Found in:

- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/registry.py:1-147`
- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/algorithm_handler.py:1-21`
- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/result_limits.py:1-70`
- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/server.py:146-360`
- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/session_manager.py:21-179`
- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/graph_projection_handlers.py:16-190`
- GDS Agent:
  `gitrefrepo/Neo4j family/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/centrality_algorithm_handlers.py:15-180`

Source shape:

```text
AlgorithmRegistry
  maps tool names to handler classes
  groups centrality, community, similarity, path, embedding, ML pipeline

AlgorithmHandler
  clean_params removes None and forbidden keys
  execute(arguments) is the uniform handler contract

result_limits
  defaults: 500 rows, 100_000 chars, 200 chars per cell
  env-overridable positive limits
  warns when dataframe output is truncated
  recommends mutate mode plus accessors for graph-scale results

server
  lists tools from graph projection + algorithm spec groups
  dispatches session, graph, accessor, and algorithm calls

session_manager
  detects plugin vs session mode
  creates/reuses Aura GDS sessions
  memory defaults to SESSION_MEMORY_GB=8
  supports recreate session for OOM recovery

graph_projection_handlers
  handles session-vs-plugin projection differences
  exposes graph info, node/relationship property streams, relationship streams

centrality handlers
  choose stream vs mutate
  translate node identifiers to IDs and back
  clean config parameters before GDS call
```

Why it matters:

The GDS agent is a compact model of how a tool-facing surface should be built:
registry first, schema/spec definitions, uniform handler contract, guarded
result serialization, and explicit session-memory controls.

Rust rewrite translation:

```rust
trait ProcedureHandler {
    fn spec(&self) -> &'static ProcedureSpec;
    fn estimate(&self, args: &ProcedureArgs) -> MemoryEstimate;
    fn execute(&self, args: ProcedureArgs, budget: ExecutionBudget) -> ResultStream;
}

struct ResultLimitPolicy {
    max_rows: usize,
    max_chars: usize,
    max_cell_chars: usize,
}
```

How agents should apply it:

- Build a GDS registry before broad algorithm implementation.
- Give every registered procedure an estimate function and an output limit
  policy.
- Prefer mutate/write plus property accessors for graph-scale outputs.
- Include session/budget controls in every procedure test.

Memory implications:

- This is a direct low-RAM lesson: limiting result rows is not enough; full
  graph-scale results should live as projected graph sidecars or persisted
  result sidecars, not as huge text/DataFrame responses.

## Cross-Batch Design Consequences

For the Rust Neo4j rewrite, this batch strengthens four requirements:

1. Compatibility registry first:
   every driver-visible, Browser-visible, OGM-visible, and GDS-visible surface
   needs a registered contract before storage optimization is declared complete.
2. Streaming result discipline:
   fetch-size, PULL, result limits, and mutate/write modes are part of the
   low-RAM architecture.
3. Typed property discipline:
   vector types, graph IDs, labels, relationship types, and property columns
   must preserve type/width metadata end-to-end.
4. Backend freedom behind frontend stability:
   OLAP can use CSR, Tilehouse, columnar sidecars, or direct-I/O streams, but
   application-facing behavior must still match drivers, Browser, OGM, and GDS
   client expectations.

## Executable Spec Candidates

```text
REQ-CLIENT-001
WHEN an official driver sends RUN followed by PULL with fetchSize=N
THEN the Rust server SHALL produce at most N records before requiring the next
pull request
AND SHALL NOT materialize the complete result solely to satisfy the first pull.

REQ-CLIENT-002
WHEN a managed transaction returns a retryable transient error
THEN the Rust server SHALL expose error metadata that official drivers classify
as retryable
AND non-retryable user errors SHALL NOT be misclassified as transient.

REQ-CLIENT-003
WHEN a transaction commits and produces a bookmark
THEN the result summary SHALL expose bookmark metadata compatible with official
driver bookmark managers.

REQ-CLIENT-004
WHEN a routed driver connects to the single-node Rust server
THEN the server SHALL return a valid single-node routing table for the requested
database and access mode.

REQ-CLIENT-005
WHEN the client sends or receives a Neo4j Vector
THEN the server SHALL preserve element type, byte width, dimension, and value
order across PackStream, property storage, and any OLAP sidecar projection.

REQ-GDS-SURFACE-001
WHEN the GDS procedure inventory is generated
THEN every endpoint SHALL be classified as ImplementedExactLowRam,
ImplementedCompatLatency, RegisteredUnsupported, or DeferredWithReason
AND missing endpoints SHALL fail CI.

REQ-GDS-SURFACE-002
WHEN an algorithm returns graph-scale output
THEN stream mode SHALL obey result limits and fetch-size
AND mutate/write mode SHALL store graph-scale output in sidecars or OLTP
writeback rather than unbounded response memory.

REQ-BROWSER-001
WHEN Browser runs SHOW TRANSACTIONS or TERMINATE TRANSACTIONS
THEN the Rust server SHALL expose compatible query-management behavior and
result fields.

REQ-OGM-001
WHEN OGM-like clients load entities at bounded depth
THEN node identity, relationship endpoint metadata, labels/types, and property
values SHALL be stable enough for identity-map and relationship-merge semantics.
```

## Open Questions For Later Batches

- Need direct Testkit evidence that official drivers agree on fetch-size,
  retry, bookmark, vector, and routing behavior under scripted Bolt responses.
- Need a GDS API inventory generated from `neo4j-gds-src` and compared against
  `neo4j-gds-client-src` coverage helpers.
- Need a Browser golden fixture set for record/table/path/plan/profile
  conversion.
- Need a decision on whether OGM compatibility is tested only through driver
  behavior or also through representative application fixtures.
- Need CodeGraphContext bug isolation for `gds-agent-src` if this repo remains
  important for agent-tooling evidence.

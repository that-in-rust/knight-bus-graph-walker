# Supermeta Graph Database Patterns 5

Worker 5 slice for observability, infrastructure, agents, benchmark harnesses,
testing frameworks, failure injection, developer tooling, and operational
patterns relevant to a Neo4j-style Rust graph database rewrite.

Created: 2026-07-06

## Scope

This file is an encyclopedia slice, not an implementation plan. Its job is to
make the rewrite measurable, debuggable, benchmarkable, and maintainable during
long agent-assisted development. The evidence below treats graph-tool output as
navigational evidence only; important claims are backed by direct source reads.

The strongest transfer theme is simple:

```text
Every subsystem that can fail should emit:
1. a stable identifier,
2. a typed outcome,
3. a duration,
4. a size or count when relevant,
5. a reproducible test or benchmark path.
```

For a Rust database, that means metrics and traces are not a garnish. They are
part of the storage engine, query engine, protocol server, migration layer, and
agent workflow contracts.

## Evidence Commands and Tools Used

Required local skills read before evidence work:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/using-superpowers/SKILL.md`

Graph evidence used:

- `codebase-memory-mcp` smoke scan:
  - Command:
    `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`
  - Output:
    `/tmp/codex-code-intel/codebase-memory/knight-bus-graph-walker-20260706-230233`
  - Verified wrapper output did not mention `gitrefrepo/`.
- `CodeGraphContext` smoke scan:
  - Command:
    `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`
  - Output:
    `/tmp/codex-code-intel/codegraphcontext/knight-bus-graph-walker-20260706-230233`
  - Verified wrapper output did not mention `gitrefrepo/`.
- High-value CodeGraphContext attempts:
  - Broad `neo4j-testkit-src` run started at
    `/tmp/codex-code-intel/codegraphcontext/neo4j-testkit-src-20260706-231131`
    and was stopped because full indexing was too expensive for this worker slice.
  - Narrow direct `boltstub_rs` run wrote partial output at
    `/tmp/codex-code-intel/codegraphcontext/boltstub-rs-worker5-20260706-231604`
    but exited before usable query files were produced.
  - `tracing-src` wrapper run was started at
    `/tmp/codex-code-intel/codegraphcontext/tracing-src-20260706-231639`;
    it exited with code 143 after writing a partial database and index log, but
    before clean wrapper completion or query output files. Direct source reads
    from `tracing-src` are the authority.
- Focused `neo4j-go-driver-src` CodeGraphContext scan:
  - Output:
    `/tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616`
  - `stats`: 1 repository, 219 files, 1,758 functions, 41 interfaces,
    222 structs, 13 modules.
  - `find name UpdateBookmarks`: `neo4j/bookmarks.go:65`.
  - `find name ExecuteRead`: `neo4j/session.go:402` and
    `neo4j/transaction_helpers.go:32`.
  - `find name GetRoutingTable`: Bolt 3/4/5/6 routing-table implementations.
- Focused `neo4j-go-driver-src` codebase-memory scan:
  - Output:
    `/tmp/codex-code-intel/codebase-memory/neo4j-go-driver-src-20260706-235049`
  - `index_repository`: 3,034 nodes and 16,234 edges.
  - `get_graph_schema`: 1,224 methods, 704 functions, 4,736 `CALLS` edges,
    and 1,253 `TESTS` edges.
  - `search_graph` for `UpdateBookmarks` found the concrete method in
    `neo4j/bookmarks.go`.

Direct evidence commands included `rg`, `find`, `nl -ba ... | sed -n`, and
targeted reads of source files listed under each pattern. Representative
commands:

```bash
nl -ba /Users/amuldotexe/Desktop/oss-read-only/openobserve/src/config/src/metrics.rs | sed -n '1,260p'
nl -ba /Users/amuldotexe/Desktop/oss-read-only/openobserve/src/config/src/metrics.rs | sed -n '2050,2080p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tracing-src/README.md | sed -n '135,260p'
nl -ba /Users/amuldotexe/Desktop/oss-read-only/airflow/scripts/ci/prek/check_migration_patterns.py | sed -n '1,220p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-testkit-src/boltstub/channel.py | sed -n '32,90p'
HOME=/tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616/home cgc --database ladybugdb --path /tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616/ladybugdb.sqlite find name GetRoutingTable
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-go-driver-src/neo4j/session.go | sed -n '392,520p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-go-driver-src/neo4j/internal/retry/state.go | sed -n '1,160p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-go-driver-src/neo4j/internal/bolt/bolt4.go | sed -n '836,928p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py | sed -n '1,190p'
```

## Repositories Inspected

Inspected with direct source reads:

- `/Users/amuldotexe/Desktop/oss-read-only/openobserve`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tracing-src`
- `/Users/amuldotexe/Desktop/oss-read-only/airflow`
- `/Users/amuldotexe/Desktop/oss-read-only/great_expectations`
- `/Users/amuldotexe/Desktop/oss-read-only/streamlit`
- `/Users/amuldotexe/Desktop/oss-read-only/scikit-learn`
- `/Users/amuldotexe/Desktop/oss-read-only/plotly.py`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-testkit-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-go-driver-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_arcadedb-src`
- `/Users/amuldotexe/Desktop/methods-agents-hub/agent-room-of-requirements`
- `/Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/mcp-bench`
- `/Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/LiveMCPBench`

Enumerated or partially inspected:

- `ldbc_graphalytics-src`
- `ldbc_graphalytics_docs-src`
- `ldbc_graphalytics_platforms_graphblas-src`
- `ldbc_snb_interactive_v1_driver-src`
- `ldbc_snb_interactive_v1_impls-src`
- `ldbc_snb_interactive_v2_driver-src`
- `ldbc_snb_interactive_v2_impls-src`
- `ToolBench`, `ToolRoute`, `ToolSandbox`, `StableToolBench`,
  `benchmarking-tool-retrieval`, `graph-tool-call`, `lazy-tool`, `tau2-bench`

## Pattern Index

| Group | Patterns |
| --- | --- |
| Metrics and tracing | Prometheus edge registry, request trace IDs, plan metrics backchannel, storage boundary metrics, subscriber ownership |
| Structured logs | Library events with binary-owned subscribers, benchmark job logs, script mismatch logs |
| Benchmark harnesses | ASV historical baselines, cached setup benchmarks, LDBC timed jobs, MCP agent metrics |
| Integration testkit | Docker driver glue, scripted Bolt stubs, standalone/cluster fixtures, driver retry/routing verification |
| Failure injection | Bad handshakes, handshake delay, disconnect-on-pull, broken-wire state, migration anti-pattern detection |
| Config/deployment | Health endpoints, OTel backend selection, CLI env/config surfaces |
| Migrations | AST migration linter with suppressions |
| CLI ergonomics | API-generated CLI commands, sensitive-option rejection, quiet browser launch |
| Agent workflows | Chunk ledgers, skill packaging, trajectory capture, judge stability |
| Progress journals | TDD phase journals, context-retainer format, evidence coverage ledgers |
| Dashboards | Offline HTML artifacts, subplot grid validation |
| Reproducible experiments | Seeds, profile matrices, env reset, dependency-pinned benchmark history |

## Metrics and Tracing

### Pattern: Prometheus Edge Metric Spine

Source:

- `openobserve/src/config/src/metrics.rs:16-65`
- `openobserve/src/config/src/metrics.rs:107-176`
- `openobserve/src/config/src/metrics.rs:190-250`
- `openobserve/src/config/src/metrics.rs:934-990`
- `openobserve/src/config/src/metrics.rs:2056-2075`

Evidence summary:

- OpenObserve centralizes metrics as `Lazy` Prometheus counters, gauges, and
  histograms.
- HTTP metrics use stable labels such as endpoint, status, org, stream type,
  search type, and search group.
- Ingest metrics track records, bytes, errors, WAL bytes, parquet write counts,
  file sizes, and file times.
- Query metrics track running, pending, timeout, canceled, DB query counts, and
  DB query time.
- `create_const_labels()` adds stable process labels such as cluster, instance,
  and role.
- `gather()` encodes the default registry with a Prometheus `TextEncoder`.

Source-shaped pseudocode:

```rust
static HTTP_REQUESTS: Lazy<IntCounterVec> =
    Lazy::new(|| IntCounterVec::new(opts, &["endpoint", "status", "org_id"]));

static QUERY_TIME: Lazy<HistogramVec> =
    Lazy::new(|| HistogramVec::new(histogram_opts, &["org_id", "query_kind"]));

pub fn gather() -> String {
    let families = prometheus::gather();
    TextEncoder::new().encode_to_string(&families)
}
```

Rust translation for the rewrite:

```rust
pub struct GraphDbMetrics {
    pub query_started_total: IntCounterVec,
    pub query_finished_total: IntCounterVec,
    pub query_duration_seconds: HistogramVec,
    pub tx_commit_duration_seconds: HistogramVec,
    pub wal_bytes_written_total: IntCounterVec,
    pub page_cache_bytes: IntGaugeVec,
    pub lock_wait_seconds: HistogramVec,
}

impl GraphDbMetrics {
    pub fn observe_query_outcome(
        &self,
        org: &str,
        db: &str,
        query_kind: &str,
        status: &str,
        elapsed: Duration,
    ) {
        self.query_finished_total
            .with_label_values(&[org, db, query_kind, status])
            .inc();
        self.query_duration_seconds
            .with_label_values(&[org, db, query_kind, status])
            .observe(elapsed.as_secs_f64());
    }
}
```

Implications:

- Memory: metric handles should be static or app-state singletons, not allocated
  per request.
- Performance: use low-cardinality labels. Never put query text, node IDs, file
  paths, or raw user IDs into metric labels.
- Concurrency: Prometheus metric types are internally synchronized; the hot path
  cost is acceptable only if labels are bounded and predeclared.
- Testing: add a unit test that runs a query, gathers metrics, and asserts the
  expected metric family and label set exist.

Agentic coding guidance:

- Before adding any subsystem, require a "metric contract" with counters,
  histograms, and labels.
- During code review, reject unbounded labels immediately.
- Name metric registration functions clearly, for example
  `register_query_metrics_once` and `observe_query_outcome_event`.

### Pattern: Trace ID Ingress and Error Correlation

Source:

- `openobserve/src/handler/http/request/search/multi_streams.rs:758-864`
- `openobserve/src/common/utils/http.rs:143-215`
- `openobserve/src/common/utils/http.rs:577-725`
- `openobserve/src/config/src/ider.rs:76-105`

Evidence summary:

- The search handler starts a timer before execution and records metrics on both
  success and error paths.
- It creates a span only when configured to do so, then calls
  `get_or_create_trace_id`.
- The trace utility accepts a valid `x-openobserve-trace-id`, validates length,
  rejects all-zero and non-hex IDs, supports W3C `traceparent`, and generates a
  new ID otherwise.
- Tests cover malformed traceparent, invalid OpenObserve IDs, all-zero IDs,
  non-hex IDs, priority rules, and fallback behavior.
- Trace IDs are generated from UUID v7, and timestamp extraction is supported.

Source-shaped pseudocode:

```rust
let start = Instant::now();
let span = if spans_enabled() { make_http_span(req) } else { Span::none() };
let trace_id = get_or_create_trace_id(headers, &span);

match execute_search().instrument(span).await {
    Ok(value) => {
        metrics.observe(status = "200", start.elapsed());
        Ok(value)
    }
    Err(err) => {
        metrics.observe(status = "500", start.elapsed());
        Err(error_with_trace_id(err, trace_id))
    }
}
```

Rust translation for the rewrite:

```rust
pub struct RequestContext {
    pub trace_id: TraceId,
    pub database: DatabaseName,
    pub query_kind: QueryKind,
}

pub fn extract_or_generate_trace_id(headers: &HeaderMap) -> TraceId {
    headers
        .get("traceparent")
        .and_then(parse_traceparent_id)
        .or_else(|| headers.get("x-graphdb-trace-id").and_then(parse_hex_trace_id))
        .unwrap_or_else(TraceId::new_v7)
}

pub async fn run_observed_query<F, T>(
    ctx: RequestContext,
    metrics: &GraphDbMetrics,
    fut: F,
) -> Result<T, QueryError>
where
    F: Future<Output = Result<T, QueryError>>,
{
    let started = Instant::now();
    let span = tracing::info_span!(
        "graphdb.query",
        trace_id = %ctx.trace_id,
        database = %ctx.database,
        query_kind = ?ctx.query_kind,
    );

    let result = fut.instrument(span).await;
    metrics.observe_query_outcome(
        "default",
        ctx.database.as_ref(),
        ctx.query_kind.as_str(),
        result_status(&result),
        started.elapsed(),
    );
    result.map_err(|err| err.with_trace_id(ctx.trace_id))
}
```

Implications:

- Memory: store `TraceId` as a fixed 16-byte value or compact wrapper; avoid
  cloning large request context.
- Performance: parsing one trace header is negligible compared with query work;
  formatting trace IDs on every internal event is not.
- Concurrency: trace context must cross spawned tasks via `tracing` spans or
  explicit request context, not thread-local assumptions.
- Testing: write table tests for valid, invalid, malformed, missing, all-zero,
  and competing header cases.

Agentic coding guidance:

- Every error returned to a driver or HTTP client should contain a trace ID.
- Agents should add trace tests before adding new transport endpoints.
- Do not allow "temporary" request handlers without success and failure metric
  branches.

### Pattern: Query Plan Metrics Backchannel

Source:

- `openobserve/src/flight/src/common.rs:38-69`
- `openobserve/src/flight/src/common.rs:78-111`
- `openobserve/src/flight/src/common.rs:129-178`

Evidence summary:

- `RemoteScanMetrics` records fetch time, decode time, and output rows using
  DataFusion metric builders.
- A `CustomMessage` enum carries scan stats, query metrics, peak memory, and
  partial errors.
- Peak memory is loaded from an `AtomicUsize` with relaxed ordering.
- Metrics are collected from displayable execution plans with stage, node
  address, and node name metadata.

Source-shaped pseudocode:

```rust
enum CustomMessage {
    ScanStats(ScanStats),
    Metrics(Vec<Metrics>),
    PeakMemory(usize),
    PartialErr(String),
}

fn get_custom_message(refs: PreCustomMessage) -> Vec<CustomMessage> {
    messages.push(CustomMessage::PeakMemory(
        peak_memory.load(Ordering::Relaxed),
    ));
    messages.extend(collect_plan_metrics(plan, stage, node));
    messages
}
```

Rust translation for the rewrite:

```rust
pub enum QueryTelemetryMessage {
    OperatorMetrics(Vec<OperatorMetric>),
    PeakMemoryBytes(usize),
    RowsProduced { operator_id: OperatorId, rows: u64 },
    PartialError { trace_id: TraceId, message: String },
}

pub trait QueryPlanTelemetry {
    fn collect_telemetry_messages(&self) -> Vec<QueryTelemetryMessage>;
}
```

Implications:

- Memory: use compact metric structs and intern operator names if this is sent
  for every query.
- Performance: collect detailed plan metrics at query end, not in each row loop.
- Concurrency: peak memory counters can often use relaxed atomics if only used
  for reporting monotonic sampled values.
- Testing: snapshot plan telemetry for small queries and assert operator IDs are
  stable.

Agentic coding guidance:

- When implementing a new operator, require an `OperatorMetric` contract before
  merging it.
- Agents should add a debug command like `graphdb query --explain-metrics` early,
  even if the output starts tiny.

### Pattern: Storage Boundary Metrics

Source:

- `openobserve/src/infra/src/storage/remote.rs:90-180`

Evidence summary:

- Remote storage `put` and `get` paths measure start time at entry.
- On success, they derive file class from the path and increment read/write
  bytes, request counters, and storage time histograms.
- Errors are logged at the boundary.

Source-shaped pseudocode:

```rust
let start = Instant::now();
let bytes = payload.len();
let result = remote.put(path, payload).await;
if result.is_ok() && is_data_file(path) {
    metrics.storage_write_bytes.with_label_values(labels).inc_by(bytes);
    metrics.storage_write_time.with_label_values(labels).observe(elapsed);
}
```

Rust translation for the rewrite:

```rust
pub async fn put_page_file_observed<S: ObjectStore>(
    store: &S,
    metrics: &GraphDbMetrics,
    path: &StoragePath,
    bytes: Bytes,
) -> Result<(), StorageError> {
    let started = Instant::now();
    let size = bytes.len() as u64;
    let result = store.put(path, bytes).await;

    if result.is_ok() {
        metrics.storage_write_bytes_total
            .with_label_values(&[path.kind_label()])
            .inc_by(size);
        metrics.storage_write_seconds
            .with_label_values(&[path.kind_label()])
            .observe(started.elapsed().as_secs_f64());
    }

    result
}
```

Implications:

- Memory: avoid allocating path label strings in the hot path; map path classes
  to static labels.
- Performance: metrics around IO boundaries are cheap relative to network and
  disk latency.
- Concurrency: object store adapters are a natural place to observe retries and
  backpressure.
- Testing: fake object store should assert metrics change on success and not on
  failed writes unless separate error metrics are expected.

Agentic coding guidance:

- Agents should instrument storage adapters, not every caller.
- Keep the boundary metric set boring: bytes, requests, latency, errors.

### Pattern: Subscriber Ownership and Async Span Discipline

Source:

- `tracing-src/README.md:46-85`
- `tracing-src/README.md:137-181`
- `tracing-src/README.md:197-250`
- `tracing-src/README.md:294-335`
- `tracing-src/README.md:381-425`

Evidence summary:

- Executables install subscribers; libraries emit events and spans.
- A global subscriber applies across threads, while `with_default` applies to
  current-thread dynamic scope.
- `#[tracing::instrument]` creates function spans and records parameters.
- The README warns against holding span guards across async boundaries and
  recommends `Future::instrument`.
- The ecosystem includes timing histograms, distributed tracing, logfmt, Loki,
  chrome tracing, cloudwatch, lock-free reload layers, and clippy support.

Source-shaped pseudocode:

```rust
// Binary:
tracing_subscriber::fmt::init();

// Library:
#[tracing::instrument(skip(db))]
async fn execute_query(db: &Database, query: Query) -> Result<QueryResult> {
    tracing::debug!(query_kind = ?query.kind(), "planning query");
    planner.plan(query).await
}

// Async future:
future.instrument(tracing::info_span!("storage.flush")).await
```

Rust translation for the rewrite:

```rust
pub fn install_observability_subscriber(config: &ObservabilityConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.rust_log)?;
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .try_init()
        .map_err(Into::into)
}

#[tracing::instrument(
    name = "graphdb.tx.commit",
    skip(engine),
    fields(tx_id = %tx.id(), db = %tx.database())
)]
pub async fn commit_transaction_observed(
    engine: &StorageEngine,
    tx: Transaction,
) -> Result<CommitReceipt> {
    engine.commit(tx).await
}
```

Implications:

- Memory: spans store fields; avoid putting full query text or large values in
  spans by default.
- Performance: dynamic filters should disable debug spans in production hot
  loops unless sampling is enabled.
- Concurrency: never hold entered span guards across `.await`; instrument
  futures or use instrumented async functions.
- Testing: capture tracing output with a test subscriber and assert fields for
  critical paths.

Agentic coding guidance:

- Agents should not install subscribers from library crates.
- Any async observability code proposed by an agent should be checked for span
  guard lifetime mistakes.

## Structured Logs

### Pattern: Library Events, Binary-Owned Sinks

Source:

- `tracing-src/README.md:46-85`
- `tracing-src/README.md:197-199`
- `tracing-src/README.md:294-335`

Evidence summary:

- The tracing README explicitly separates event emission from subscriber
  installation.
- Libraries should not call global subscriber setup because executables may have
  their own configuration.
- Adapters can send the same events to logs, traces, metrics, flamegraphs, or
  external sinks.

Rust translation for the rewrite:

```rust
// graphdb-core emits only:
tracing::warn!(db = %db, store = %store_id, "page cache miss storm detected");

// graphdb-server decides sinks:
install_observability_subscriber(&config.observability)?;
```

Implications:

- Memory: one event can feed many sinks without each subsystem formatting its own
  strings.
- Performance: defer expensive formatting with tracing fields.
- Concurrency: subscriber setup should happen once during startup.
- Testing: library tests can use local subscribers without fighting production
  global state.

Agentic coding guidance:

- During reviews, reject "println debugging" in reusable crates.
- Require `tracing` fields for database, transaction, query, operator, and file
  identifiers.

### Pattern: Benchmark Job Logs With Process Identity

Source:

- `ldbc_graphalytics_platforms_arcadedb-src/src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:91-105`
- `ldbc_graphalytics_platforms_arcadedb-src/src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:146-178`

Evidence summary:

- The platform starts logging into the benchmark run log directory.
- It writes `executable.pid` so the framework can find the platform process.
- Each algorithm job logs job index, total jobs, algorithm, graph name, and
  completion time.
- Finalization stops platform logging and collects processing time metrics from
  the log directory.

Rust translation for the rewrite:

```rust
pub struct BenchmarkRunLog {
    pub run_id: RunId,
    pub log_dir: PathBuf,
}

impl BenchmarkRunLog {
    pub fn write_process_identity(&self) -> io::Result<()> {
        fs::create_dir_all(self.log_dir.join("platform"))?;
        fs::write(
            self.log_dir.join("platform/executable.pid"),
            std::process::id().to_string(),
        )
    }
}
```

Implications:

- Memory: log metadata should live in run state, not global mutable variables.
- Performance: benchmark logging should be outside timed inner loops.
- Concurrency: each benchmark worker needs a unique run ID and output directory.
- Testing: benchmark harness tests should assert PID/log files exist even on
  failure.

Agentic coding guidance:

- Agents running long benchmarks should create a run folder first, then write
  command, git revision, config, PID, stdout, stderr, and summary.

## Benchmark Harnesses

### Pattern: Historical Benchmark Baselines With Dependency Matrix

Source:

- `scikit-learn/asv_benchmarks/asv.conf.json:1-190`

Evidence summary:

- scikit-learn uses ASV configuration with project metadata, repo URL, install,
  uninstall, and build commands.
- It configures branches, conda environments, benchmark timeout, dependency
  versions, benchmark/results/html directories, wheel cache, and regression
  search bounds.
- A comment notes dependency-version bumps should be done in dedicated commits
  so regressions from dependency changes can be identified.

Source-shaped pseudocode:

```json
{
  "project": "scikit-learn",
  "branches": ["main"],
  "benchmark_dir": "benchmarks",
  "results_dir": "results",
  "html_dir": "html",
  "build_cache_size": 8,
  "matrix": { "numpy": [...], "scipy": [...] }
}
```

Rust translation for the rewrite:

```toml
# benches/asv-like.toml
[project]
name = "graphdb-rust"
repo = "..."

[commands]
build = "cargo build --release"
bench = "cargo bench --bench graphdb_bench -- --output json"

[matrix]
rust = ["stable", "beta"]
features = ["default", "jemalloc", "simd"]
datasets = ["tiny", "ldbc-sf1", "ldbc-sf10"]
```

Implications:

- Memory: record peak RSS and allocator configuration per run.
- Performance: regression history matters more than one-off fast numbers.
- Concurrency: benchmark runs should pin thread counts where possible.
- Testing: add a CI smoke profile and a nightly full profile.

Agentic coding guidance:

- Agents may optimize only after adding a benchmark that can detect the claimed
  improvement.
- Dependency and compiler changes should be separate commits from code changes
  when measuring performance.

### Pattern: Cached Benchmark Setup With Time, Peak Memory, and Quality

Source:

- `scikit-learn/asv_benchmarks/benchmarks/common.py:13-220`
- `scikit-learn/asv_benchmarks/benchmarks/cluster.py:8-92`

Evidence summary:

- Config supports environment overrides for profile and job count.
- Profiles define fast, regular, and large-scale repeat/warmup/data-size knobs.
- Dataset and fitted estimator setup is cached so timed functions do not include
  expensive setup.
- Benchmarks track wall time, peak memory, train score, test score, predict time,
  and prediction consistency against a base commit.
- Cluster benchmarks use parameter grids and deterministic seeds.

Rust translation for the rewrite:

```rust
pub struct BenchProfile {
    pub warmup_iters: usize,
    pub measured_iters: usize,
    pub dataset_scale: DatasetScale,
    pub worker_threads: usize,
}

pub struct CachedGraphFixture {
    pub dataset_path: PathBuf,
    pub store_path: PathBuf,
    pub base_commit_result_path: Option<PathBuf>,
}

pub fn bench_query_with_cached_store(c: &mut Criterion) {
    let fixture = CachedGraphFixture::load_or_build("ldbc-sf1");
    c.bench_function("match_two_hop_neighbors", |b| {
        b.iter(|| run_query(&fixture.store_path, black_box(QueryId::TwoHop)))
    });
}
```

Implications:

- Memory: peak memory must be measured separately from wall time.
- Performance: separating load/setup from query timing prevents fake regressions.
- Concurrency: record thread count and scheduler/runtime configuration.
- Testing: benchmark setup should be deterministic and validated before timing.

Agentic coding guidance:

- Agents should not time code that includes fixture generation unless the
  benchmark is explicitly about loading.
- Store benchmark config and fixture hashes with results.

### Pattern: LDBC Timed Job Loop and Platform Lifecycle

Source:

- `ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py:17-49`
- `ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py:87-150`
- `ldbc_graphalytics_platforms_arcadedb-src/src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:87-178`

Evidence summary:

- Shared benchmark infrastructure defines a five-minute per-query timeout.
- `run_timed` installs a wall-clock alarm, returns elapsed time, `"timeout"`, or
  `"N/A"`.
- `run_benchmarks` parses `--reset`, selects systems, runs each benchmark, runs
  cleanup, and prints a summary table.
- The platform lifecycle separates prepare, startup, run, finalize, and
  terminate.

Rust translation for the rewrite:

```rust
pub enum BenchOutcome<T> {
    Completed { elapsed: Duration, value: T },
    TimedOut { timeout: Duration },
    Failed { error: String },
}

pub async fn run_timed_benchmark<F, T>(
    name: &str,
    timeout: Duration,
    fut: F,
) -> BenchOutcome<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    let started = Instant::now();
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(value)) => BenchOutcome::Completed {
            elapsed: started.elapsed(),
            value,
        },
        Ok(Err(err)) => BenchOutcome::Failed { error: err.to_string() },
        Err(_) => BenchOutcome::TimedOut { timeout },
    }
}
```

Implications:

- Memory: cleanup must run in `finally`/drop-like paths even when a benchmark
  fails.
- Performance: summary tables should distinguish timeout from failure.
- Concurrency: use runtime-aware timeouts instead of process signals in Rust.
- Testing: add tests for completed, timed-out, and failed benchmark outcomes.

Agentic coding guidance:

- Agents should never summarize a benchmark as "failed" when it timed out. The
  two outcomes guide different fixes.

### Pattern: Agent Benchmark Metrics Across Completion, Tool Use, and Cost

Source:

- `mcp-bench/benchmark/runner.py:49-145`
- `mcp-bench/benchmark/runner.py:148-209`
- `mcp-bench/benchmark/runner.py:396-520`
- `mcp-bench/benchmark/results_aggregator.py:18-175`
- `mcp-bench/benchmark/results_aggregator.py:177-240`
- `mcp-bench/benchmark/evaluator.py:57-127`

Evidence summary:

- MCP-Bench separates connection lifecycle into an async `ConnectionManager`
  that connects all servers and cleans up all connections.
- Benchmark runner supports model selection, distraction servers, judge
  stability, problematic tool filtering, concurrent summarization, and fuzzy
  descriptions.
- Single-task execution prepares task info, prepares server configs, retries
  with fresh connections, enforces timeout, records execution time, and attaches
  available tools.
- Results aggregation tracks completion rate, task fulfillment, grounding, tool
  appropriateness, parameter accuracy, dependency awareness, parallelism,
  schema compliance, valid tool name rate, execution success rate, execution
  time, rounds, tool-call counts, server count, and cross-server coordination.
- The evaluator defines a six-dimension rubric and randomizes rubric order to
  reduce judge bias.

Rust translation for the rewrite:

```rust
pub struct AgentRunMetrics {
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub avg_execution_ms: f64,
    pub avg_tool_calls: f64,
    pub avg_context_tokens: f64,
    pub grounding_score: f64,
    pub dependency_awareness_score: f64,
}

pub trait AgentTaskEvaluator {
    fn evaluate_run(&self, trajectory: &AgentTrajectory) -> AgentRunEvaluation;
}
```

Implications:

- Memory: trajectories can be large; write them to files and aggregate summaries.
- Performance: evaluate outside the timed execution path.
- Concurrency: retries should recreate connections, not reuse possibly poisoned
  server state.
- Testing: fake evaluator and fake server manager should be injectable.

Agentic coding guidance:

- For database rewrite agents, record tool calls, tests run, files changed,
  failures observed, and final verification commands per task.
- Add a "grounded in tool output" field to agent review templates.

## Integration Testkit

### Pattern: Dockerized Driver Backend Glue

Source:

- `neo4j-testkit-src/README.md:1-90`
- `neo4j-testkit-src/README.md:120-240`
- `neo4j-testkit-src/driver.py:10-95`
- `neo4j-testkit-src/driver.py:143-210`

Evidence summary:

- Neo4j testkit declares driver name, driver repo, branch, build cache,
  artifacts dir, backend timeout, and a temporary `TEST_RUSTY_STUB` variable for
  a Rust rewrite of the Bolt stub.
- It supports backend debug logging and disabling backend timeout for debugger
  sessions.
- Integration tests need a real Neo4j instance; stub tests use scripted stubs
  and do not need a running Neo4j server.
- Driver glue locates testkit scripts either in the driver repo or the testkit
  repo.
- Docker build copies CA files into the driver image context.
- Backend startup is detached, then a `wait_for_port.py` script verifies
  readiness.
- There are explicit methods for unit tests, stress tests, integration tests,
  host/port polling, and connection-closed assertions.

Rust translation for the rewrite:

```rust
pub struct DriverHarness {
    pub driver_name: String,
    pub driver_repo: PathBuf,
    pub artifacts_dir: PathBuf,
    pub backend_timeout: Duration,
}

impl DriverHarness {
    pub async fn start_backend_and_wait(&self) -> Result<BackendHandle> {
        let handle = spawn_backend_process(&self.driver_repo, &self.artifacts_dir)?;
        wait_for_port("127.0.0.1", 9876, self.backend_timeout).await?;
        Ok(handle)
    }

    pub async fn assert_connections_closed(&self, addr: SocketAddr) -> Result<()> {
        // Check server-side active connection registry or OS socket state.
        Ok(())
    }
}
```

Implications:

- Memory: backend processes should be owned handles with deterministic cleanup.
- Performance: build caches should be explicit and invalidatable.
- Concurrency: test backends need isolated ports and artifact directories in
  parallel CI.
- Testing: port readiness and connection leak checks should be mandatory.

Agentic coding guidance:

- Agents adding protocol features should add both stub tests and integration
  tests.
- Debug mode must be a configuration flag, not a code edit.

### Pattern: Scripted Bolt Stub as Protocol Oracle

Source:

- `neo4j-testkit-src/README.md:187-203`
- `neo4j-testkit-src/boltstub/channel.py:36-90`
- `neo4j-testkit-src/boltstub/channel.py:130-205`
- `neo4j-testkit-src/boltstub/channel.py:270-325`

Evidence summary:

- Stub tests are rooted separately from integration tests and use scripted
  protocol behavior.
- `Channel` is the glue between the stub script, socket, and Bolt protocol.
- It validates magic preamble, negotiates version handshake, aborts on mismatch,
  translates server lines, consumes client messages, peeks buffered messages,
  asserts no unexpected input, and auto-responds where the protocol allows.

Rust translation for the rewrite:

```rust
pub enum ScriptStep {
    ExpectClient(ClientMessagePattern),
    SendServer(ServerMessage),
    SendRaw(Bytes),
    AssertNoInput,
    AutoRespond,
}

pub struct BoltStubChannel<W> {
    wire: W,
    protocol: BoltProtocol,
    buffered: Option<ClientMessage>,
}

impl<W: Wire> BoltStubChannel<W> {
    pub async fn assert_no_input(&mut self) -> Result<(), ScriptFailure> {
        if let Some(msg) = self.try_peek().await? {
            return Err(ScriptFailure::unexpected_input(msg));
        }
        Ok(())
    }
}
```

Implications:

- Memory: scripted messages can be pre-parsed into compact enums.
- Performance: the stub is not the database hot path; clarity beats cleverness.
- Concurrency: each stub server connection needs isolated script state.
- Testing: script mismatch errors must include line number and received message.

Agentic coding guidance:

- Agents should encode protocol tests as script fixtures, not large imperative
  test functions.
- Keep the script language tiny and make every mismatch diagnostic actionable.

### Pattern: Standalone and Cluster Fixtures

Source:

- `neo4j-testkit-src/neo4j.py:29-115`

Evidence summary:

- The testkit has separate `Standalone` and `Cluster` server fixtures.
- Standalone maps Neo4j version differences to environment variables.
- It mounts logs into artifact directories and accepts license flags for
  enterprise editions.
- Cluster fixture creates multiple cores and returns per-core Bolt addresses.

Rust translation for the rewrite:

```rust
pub enum DatabaseFixture {
    Standalone(StandaloneFixture),
    Cluster(ClusterFixture),
}

pub trait TestDatabaseFixture {
    fn addresses(&self) -> Vec<SocketAddr>;
    async fn start(&mut self, network: &TestNetwork) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
}
```

Implications:

- Memory: fixture state should own all child resources.
- Performance: cluster fixtures are expensive; mark them as integration or
  nightly tests.
- Concurrency: fixture names, ports, and networks must be unique per test.
- Testing: routing and causal consistency features need cluster fixtures early.

Agentic coding guidance:

- Agents should avoid implementing cluster behavior only against a fake single
  node. Add a minimal multi-node fixture boundary first.

## Failure Injection

### Pattern: Handshake and Wire Fault Injection

Source:

- `neo4j-testkit-src/boltstub/channel.py:100-122`
- `neo4j-testkit-src/boltstub/channel.py:256-268`
- `neo4j-testkit-src/boltstub/wiring.py:235-356`
- `neo4j-testkit-src/boltstub/tests/test_integration.py:460-545`
- `neo4j-testkit-src/tests/stub/iteration/test_result_single.py:70-112`

Evidence summary:

- The stub can force a custom handshake response and verify expected client
  response bytes.
- It can abort a handshake by sending four zero bytes.
- It can delay handshakes by a configured duration.
- Wire state records local close and remote broken conditions.
- Tests cover custom handshakes, handshake delay upper/lower timing bounds,
  disconnect during pull, and server-side error during pull.

Rust translation for the rewrite:

```rust
pub enum FaultInjection {
    CustomHandshake { response: Bytes, expected_client: Option<Bytes> },
    AbortHandshake,
    DelayHandshake(Duration),
    DisconnectAfter(ServerStep),
    ErrorOnPull { code: String },
    ShortRead { bytes_before_close: usize },
}

pub struct FaultyWire<W> {
    inner: W,
    closed: bool,
    broken: bool,
    fault: Option<FaultInjection>,
}
```

Implications:

- Memory: fault descriptors are tiny and can live in script state.
- Performance: fault injection must be compiled or configured out of production
  paths.
- Concurrency: wire state needs per-connection ownership, not shared mutable
  globals.
- Testing: each driver-visible error mapping should have a script forcing it.

Agentic coding guidance:

- Require an injected failure test for every retry or recovery path.
- A passing happy-path test is not enough for protocol work.

### Pattern: Health Waits and Rollout Verification

Source:

- `airflow/kubernetes-tests/tests/kubernetes_tests/test_base.py:186-206`
- `airflow/kubernetes-tests/tests/kubernetes_tests/test_base.py:246-263`

Evidence summary:

- Airflow Kubernetes tests poll `/monitor/health` up to 10 times with a delay and
  short request timeout.
- Rollout verification shells to `kubectl rollout status` and accepts multiple
  success phrases.

Rust translation for the rewrite:

```rust
pub async fn wait_for_health(
    client: &HttpClient,
    url: Url,
    attempts: usize,
    delay: Duration,
) -> Result<HealthResponse> {
    for _ in 0..attempts {
        if let Ok(resp) = client.get(url.clone()).timeout(Duration::from_secs(1)).await {
            if resp.is_healthy() {
                return Ok(resp);
            }
        }
        tokio::time::sleep(delay).await;
    }
    Err(HealthError::TimedOut)
}
```

Implications:

- Memory: health response should be small and typed.
- Performance: health checks should not run expensive queries.
- Concurrency: avoid stampeding health checks against startup locks.
- Testing: integration tests should wait on health rather than arbitrary sleeps.

Agentic coding guidance:

- Agents adding a new service process should add a readiness probe and a test
  wait helper.

## Testing Frameworks and Validation

### Pattern: Validation Checkpoint With Action Pipeline

Source:

- `great_expectations/great_expectations/checkpoint/checkpoint.py:69-115`
- `great_expectations/great_expectations/checkpoint/checkpoint.py:295-413`

Evidence summary:

- A checkpoint is the primary production validation surface.
- It contains validation definitions, actions, result format, and ID.
- `run()` rejects empty validation definitions, handles freshness diagnostics,
  prepares batch/expectation params and run ID, runs validations, constructs a
  checkpoint result, and runs sorted actions.
- Checkpoint result metadata includes the checkpoint ID.

Rust translation for the rewrite:

```rust
pub struct ValidationCheckpoint {
    pub id: CheckpointId,
    pub validations: Vec<ValidationDefinition>,
    pub actions: Vec<Box<dyn ValidationAction>>,
}

impl ValidationCheckpoint {
    pub async fn run(&self, ctx: ValidationContext) -> Result<CheckpointResult> {
        ensure!(!self.validations.is_empty(), "no validations configured");
        let mut results = Vec::new();
        for validation in &self.validations {
            results.push(validation.run(&ctx).await?);
        }
        let result = CheckpointResult::new(self.id, results);
        for action in self.actions.iter().sorted_by_key(|a| a.order()) {
            action.run(&result).await?;
        }
        Ok(result)
    }
}
```

Implications:

- Memory: validation results can be large; store summaries in memory and stream
  details to artifacts.
- Performance: validation actions should run after core validation timing.
- Concurrency: independent validations can be parallelized if they do not mutate
  the same fixture.
- Testing: action order and empty-checkpoint failure should be unit tested.

Agentic coding guidance:

- Before a rewrite milestone is "done", define a checkpoint with validations and
  post-actions such as metrics export, report write, and trace bundle creation.

### Pattern: Metric Dependency Graph for Validation

Source:

- `great_expectations/great_expectations/validator/validation_graph.py:87-174`

Evidence summary:

- `ValidationGraph` owns execution engine, edges, and edge IDs.
- It deduplicates edges.
- It builds metric dependency graphs from metric configurations.
- It asks providers for dependencies and warns on circular dependencies.

Rust translation for the rewrite:

```rust
pub struct MetricDependencyGraph {
    edges: IndexSet<(MetricId, MetricId)>,
}

impl MetricDependencyGraph {
    pub fn add_metric_with_dependencies(
        &mut self,
        metric: MetricId,
        deps: impl IntoIterator<Item = MetricId>,
    ) -> Result<()> {
        for dep in deps {
            if self.would_cycle(dep, metric) {
                return Err(MetricGraphError::Cycle { metric, dep });
            }
            self.edges.insert((dep, metric));
        }
        Ok(())
    }
}
```

Implications:

- Memory: use compact IDs for validation metrics; avoid cloning large configs in
  graph edges.
- Performance: dependency graph topological order can cache repeated validation
  plans.
- Concurrency: independent metric nodes can run in parallel.
- Testing: include cycle tests and deduplication tests.

Agentic coding guidance:

- Agents should represent validation dependencies explicitly instead of relying
  on call order hidden inside test functions.

### Pattern: Trace ID Table Tests

Source:

- `openobserve/src/common/utils/http.rs:577-725`

Evidence summary:

- Tests cover trace ID generation, malformed traceparent, valid OpenObserve
  header, invalid length, all-zero, non-hex, header priority, and fallback.

Rust translation for the rewrite:

```rust
#[rstest]
#[case("valid graphdb header", headers_with_graphdb_id(), Expected::Existing)]
#[case("all zero id", headers_with_zero_id(), Expected::Generated)]
#[case("bad traceparent", headers_with_bad_traceparent(), Expected::Generated)]
fn extracts_or_generates_trace_id_cases(#[case] headers: HeaderMap, #[case] expected: Expected) {
    let trace_id = extract_or_generate_trace_id(&headers);
    assert_expected_trace_id(trace_id, expected);
}
```

Implications:

- Memory: table tests are cheap and prevent future edge-case drift.
- Performance: they lock in simple parsers rather than regex-heavy code.
- Concurrency: no shared state needed.
- Testing: this is a must-have for every ingress protocol.

Agentic coding guidance:

- Agents should add a table test before modifying parsing or propagation logic.

## Config and Deployment

### Pattern: Typed Health Response by Subsystem

Source:

- `airflow/airflow-core/src/airflow/api/common/airflow_health.py:21-95`
- `airflow/airflow-core/src/airflow/api_fastapi/core_api/datamodels/monitor.py:1-70`

Evidence summary:

- Airflow health reports metadatabase, scheduler, triggerer, and DAG processor.
- Scheduler/triggerer/DAG processor health derives from latest heartbeat and
  liveness.
- Exceptions while checking jobs mark metadatabase unhealthy.
- Pydantic models type the response shapes.

Rust translation for the rewrite:

```rust
#[derive(Serialize)]
pub struct HealthInfo {
    pub metastore: ComponentHealth,
    pub query_engine: ComponentHealth,
    pub storage_engine: ComponentHealth,
    pub raft: Option<ComponentHealth>,
}

#[derive(Serialize)]
pub struct ComponentHealth {
    pub status: HealthStatus,
    pub latest_heartbeat_ms: Option<u64>,
    pub message: Option<String>,
}
```

Implications:

- Memory: health snapshots should read small atomic/shared state.
- Performance: health endpoint should not acquire long write locks.
- Concurrency: component heartbeat writers should be independent.
- Testing: simulate stale heartbeat, live heartbeat, and metastore exception.

Agentic coding guidance:

- Agents should add a health field whenever they add a background subsystem.

### Pattern: Config-Driven Metrics Backend Selection

Source:

- `airflow/airflow-core/src/airflow/observability/metrics/stats_utils.py:20-45`
- `airflow/airflow-core/src/airflow/observability/metrics/otel_logger.py:28-55`

Evidence summary:

- Airflow chooses metrics implementation based on config: Datadog StatsD,
  StatsD, OpenTelemetry, or no-op.
- OTel config reads host, port, prefix, SSL, interval, debug, service, allow/block
  lists, and stat handler, with fallbacks for deprecated values.

Rust translation for the rewrite:

```rust
pub enum MetricsBackendConfig {
    Prometheus { bind: SocketAddr },
    OpenTelemetry { endpoint: Url, service_name: String },
    Statsd { addr: SocketAddr, prefix: String },
    Noop,
}

pub fn build_metrics_backend(config: &MetricsBackendConfig) -> Arc<dyn MetricsSink> {
    match config {
        MetricsBackendConfig::Prometheus { .. } => Arc::new(PrometheusSink::new(config)),
        MetricsBackendConfig::OpenTelemetry { .. } => Arc::new(OtelSink::new(config)),
        MetricsBackendConfig::Statsd { .. } => Arc::new(StatsdSink::new(config)),
        MetricsBackendConfig::Noop => Arc::new(NoopSink),
    }
}
```

Implications:

- Memory: no-op sink should avoid allocation in tests and local runs.
- Performance: backend choice should happen once at startup.
- Concurrency: sinks must be `Send + Sync`.
- Testing: parse configs for each backend and deprecated fallback case.

Agentic coding guidance:

- Do not hard-code one telemetry backend deep in core crates.

### Pattern: Config-Backed CLI With Sensitive Option Rejection

Source:

- `streamlit/lib/streamlit/web/cli.py:44-115`
- `streamlit/lib/streamlit/web/cli.py:130-265`

Evidence summary:

- Streamlit derives Click options from config options.
- Option names map dotted config keys to CLI parameter names.
- Environment variables use an automatic prefix.
- Sensitive options are rejected on the command line and must be supplied via
  config file or env var.
- `streamlit run` validates URL/path, directory default file, `.py` extension,
  and existence before running.

Rust translation for the rewrite:

```rust
#[derive(clap::Parser)]
pub struct GraphDbCli {
    #[arg(long, env = "GRAPHDB_SERVER_BIND")]
    pub server_bind: Option<SocketAddr>,

    #[arg(long, env = "GRAPHDB_CONFIG")]
    pub config: Option<PathBuf>,
}

pub fn reject_sensitive_cli_args(matches: &ArgMatches) -> Result<()> {
    if matches.contains_id("admin_password") {
        bail!("admin_password must be provided by env var or config file");
    }
    Ok(())
}
```

Implications:

- Memory: config is loaded once into typed structs.
- Performance: validation happens before server startup.
- Concurrency: runtime config reload should be explicit and lock-minimized.
- Testing: CLI parser tests should cover env var, config file, invalid path, and
  sensitive option rejection.

Agentic coding guidance:

- Agents should add CLI tests with every user-facing option.

## Migrations

### Pattern: AST Migration Anti-Pattern Linter

Source:

- `airflow/.pre-commit-config.yaml:1106-1122`
- `airflow/scripts/ci/prek/check_migration_patterns.py:25-211`

Evidence summary:

- Airflow registers a `check-migration-patterns` pre-commit hook for migration
  files.
- Rules MIG001/MIG002/MIG003 detect DML/DDL before SQLite foreign-key disabling
  and DML without offline-mode guards.
- The linter explains why SQLite PRAGMA foreign-key changes can become no-ops
  after transaction auto-begin.
- Suppressions require `# noqa: MIG0XX -- reason`.
- The implementation parses Python AST rather than relying only on grep.

Rust translation for the rewrite:

```rust
pub enum MigrationLint {
    DataBeforeForeignKeyGuard { file: PathBuf, line: usize },
    SchemaBeforeForeignKeyGuard { file: PathBuf, line: usize },
    OnlineOnlyMutationWithoutGuard { file: PathBuf, line: usize },
}

pub fn lint_migration_plan(plan: &MigrationPlan) -> Vec<MigrationLint> {
    // Parse migration DSL/AST and enforce ordering/safety rules.
    vec![]
}
```

Implications:

- Memory: migration AST is small; parse once per file.
- Performance: linter runs in pre-commit and CI, not production.
- Concurrency: migration execution should hold explicit locks; linting can be
  parallel.
- Testing: every lint rule needs bad, good, and suppression fixtures.

Agentic coding guidance:

- Agents should add lint rules for migration mistakes discovered during rewrite.
- Never merge a migration DSL feature without an anti-pattern test.

## CLI Ergonomics

### Pattern: API-Generated CLI Command Surface

Source:

- `airflow/airflow-ctl/src/airflowctl/ctl/cli_config.py:323-430`

Evidence summary:

- Airflow defines action commands, group commands, parsers, and a command
  factory.
- The command factory maps API operation metadata to CLI commands and output
  commands.
- AST inspection parses operation functions, arguments, and return annotations.

Rust translation for the rewrite:

```rust
pub struct OperationDescriptor {
    pub name: &'static str,
    pub group: &'static str,
    pub args: &'static [OperationArg],
    pub output: OutputFormat,
}

pub fn build_cli_from_operations(ops: &[OperationDescriptor]) -> clap::Command {
    ops.iter().fold(clap::Command::new("graphdb"), |cmd, op| {
        cmd.subcommand(operation_to_command(op))
    })
}
```

Implications:

- Memory: descriptors can be static.
- Performance: CLI construction is startup-only.
- Concurrency: none.
- Testing: operation metadata should generate deterministic help output.

Agentic coding guidance:

- Agents should avoid duplicating API semantics in CLI handlers.
- Generate CLI docs from the same descriptors.

### Pattern: Quiet Developer UX for Browser Launch

Source:

- `streamlit/lib/streamlit/cli_util.py:68-110`

Evidence summary:

- Streamlit implements browser opening itself to avoid noisy browser
  stdout/stderr.
- It handles Windows, Linux, and macOS differently.

Rust translation for the rewrite:

```rust
pub fn open_browser_quietly(url: &str) -> io::Result<()> {
    let mut cmd = platform_open_command(url);
    cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn()?;
    Ok(())
}
```

Implications:

- Memory: trivial.
- Performance: command runs outside database server path.
- Concurrency: do not block server startup on browser process completion.
- Testing: unit test command selection by platform; integration test can be
  opt-in.

Agentic coding guidance:

- Developer tooling should not pollute benchmark output or CI logs.

## Agent Workflows

### Pattern: Chunked Evidence Ledger

Source:

- `agent-room-of-requirements/A02-Mega-Idiomatic-Prompt.md:385-408`
- `agent-room-of-requirements/A02-Mega-Idiomatic-Prompt.md:720-850`

Evidence summary:

- Long sources are read in chunks of at most 100 lines.
- Each chunk appends candidate parameters to a ledger and preserves chunk range.
- After each file, duplicates are consolidated and contradictions marked.
- Reusable outputs include verification matrices, anti-pattern registries,
  skill packaging parameters, future refresh queries, open question drivers,
  progressive reference loading, native gate verification, and a human scan
  acceptance test.

Rust rewrite translation:

```markdown
| source_path | line_range | pattern_candidate | failure_prevented | proof_gate |
| --- | --- | --- | --- | --- |
| src/query/planner.rs | 120-180 | operator metric contract | invisible plan regressions | snapshot explain metrics |
```

Implications:

- Memory: ledger files offload context from long agent sessions.
- Performance: chunking prevents agents from rereading entire repos.
- Concurrency: multiple workers can own disjoint target files and share a
  coverage ledger.
- Testing: progress verifier should assert each pattern has source, implication,
  and proof gate.

Agentic coding guidance:

- For long rewrite phases, require every agent to write evidence rows before
  changing code.
- Preserve source line ranges so later agents can verify claims.

### Pattern: Trajectory Capture and Success-Rate Evaluation

Source:

- `LiveMCPBench/README.md:94-185`
- `LiveMCPBench/evaluator/stat_success_rate.py:1-80`

Evidence summary:

- LiveMCPBench recommends resetting the environment before running agents
  because agents may change it.
- It checks MCP tools before running and can repeat tool checks.
- The MCP Copilot agent warms up by indexing servers.
- Agent trajectories are saved under `baseline/output`.
- Evaluator output is separate under `evaluator/output`.
- Success-rate script aggregates JSON results by category and overall reward,
  sorts by overall success rate, and writes `success_rate.csv`.

Rust translation for the rewrite:

```rust
pub struct AgentRunArtifactPaths {
    pub trajectory_dir: PathBuf,
    pub evaluator_dir: PathBuf,
    pub success_rate_csv: PathBuf,
}

pub fn compute_success_rates(results: &[EvaluationResult]) -> SuccessRateTable {
    SuccessRateTable::from_results(results, |r| r.reward > 0.0)
}
```

Implications:

- Memory: store trajectories as line-delimited JSON to stream large runs.
- Performance: evaluation should be batched and resumable.
- Concurrency: each agent run needs a clean workspace or reset script.
- Testing: evaluator must handle missing/empty categories.

Agentic coding guidance:

- Before launching a multi-agent rewrite batch, create reset, warmup, run,
  evaluate, and summarize scripts.

### Pattern: Tool Benchmark Config Flags

Source:

- `mcp-bench/config/config_loader.py:430-690`

Evidence summary:

- Config exposes retry counts, retry backoff, batch retry backoff, default HTTP
  port, tool-description truncation length, server-selection token budget, tool
  sample count, token reduction factors, task files, judge stability,
  problematic tool filtering, concurrent summarization, fuzzy descriptions,
  concrete description references, sequential-only tools, evaluation max tokens,
  cache settings, cache TTL, cache size, cache key strategy, cache cleanup, cache
  persistence, cache whitelist, and problematic tools.

Rust translation for the rewrite:

```rust
pub struct AgentWorkflowConfig {
    pub max_retries: usize,
    pub retry_backoff: BackoffConfig,
    pub token_budget: usize,
    pub enable_judge_stability: bool,
    pub filter_problematic_tools: bool,
    pub sequential_only_tools: BTreeSet<String>,
    pub cache: AgentCacheConfig,
}
```

Implications:

- Memory: caches need max-size and cleanup policies.
- Performance: token budget and truncation controls keep agent runs bounded.
- Concurrency: sequential-only tool lists prevent unsafe parallel execution.
- Testing: config defaults should have snapshot tests.

Agentic coding guidance:

- Treat agent workflow config as production infrastructure, not prompt prose.

## Progress Journals

### Pattern: TDD Phase Journal for Long Multi-Worker Tasks

Source:

- `agent-room-of-requirements/agents-used-monthly-archive/idiomatic-references-202606/idiomatic-code-patterns-progress.md:1-163`
- `agent-room-of-requirements/agents-used-monthly-archive/claude-skills-202602/tdd-task-progress-context-retainer.md:8-112`

Evidence summary:

- The progress journal records task, created/updated timestamps, current phase,
  status, sessions, tests written, implementation progress, current focus, next
  steps, context notes, and performance metrics.
- It captures worker IDs, pattern counts by file, codebase-memory smoke status,
  CodeGraphContext smoke status, pending repo counts, and progress-verifier
  state.
- The TDD context retainer emphasizes current TDD phase, test statuses, file
  paths, function signatures, failure messages, implementation attempts, design
  decisions, performance metrics, next steps, and cross-crate dependencies.

Rust translation for the rewrite:

```markdown
# GraphDB Rewrite Progress Journal

- Task:
- Created:
- Updated:
- Current Phase: Red | Green | Refactor | Verify
- Status:

## Session: <timestamp>

### Tests Written
- TEST-GRAPHDB-001: failing - ...

### Implementation Progress
- src/query/planner.rs: planned - ...

### Current Focus

### Next Steps

### Context Notes

### Performance/Metrics
- query_p50_ms=
- peak_rss_mb=
- benchmark_profile=
```

Implications:

- Memory: journals keep agent sessions resumable without bloating prompt
  context.
- Performance: progress metrics expose whether work is improving measured
  outcomes.
- Concurrency: worker IDs and exclusive write targets prevent accidental edits.
- Testing: progress verifier can assert that each active task has a next step and
  verification gate.

Agentic coding guidance:

- Every rewrite milestone should end with an updated journal and exact commands
  run.
- Agents should record what they did not inspect.

## Dashboards

### Pattern: Offline HTML Dashboard Artifact

Source:

- `plotly.py/plotly/io/_html.py:35-145`
- `plotly.py/plotly/io/_html.py:352-520`

Evidence summary:

- Plotly `to_html` supports self-contained output, CDN output, directory-local
  bundles, partial div output, validation, explicit div IDs, and post-script
  hooks.
- `write_html` copies `plotly.min.js` once for directory mode, writes the file,
  and optionally opens it.
- The source documents the tradeoff that fully embedded HTML is larger but
  offline-capable, while CDN output is smaller but network-dependent.

Rust translation for the rewrite:

```rust
pub enum DashboardBundleMode {
    SelfContained,
    Cdn,
    DirectoryLocal,
}

pub fn write_benchmark_dashboard(
    report: &BenchmarkReport,
    path: &Path,
    mode: DashboardBundleMode,
) -> Result<()> {
    let html = render_dashboard_html(report, mode)?;
    fs::write(path, html)?;
    Ok(())
}
```

Implications:

- Memory: generate dashboard from summary JSON, not raw trace logs.
- Performance: dashboard generation runs after benchmarks.
- Concurrency: per-run output directories avoid write conflicts.
- Testing: snapshot HTML skeleton and validate referenced assets exist.

Agentic coding guidance:

- Agents should produce local dashboard artifacts for benchmark comparisons, not
  only console summaries.

### Pattern: Validated Dashboard Grid Spec

Source:

- `plotly.py/plotly/_subplots.py:42-145`
- `plotly.py/plotly/_subplots.py:362-462`

Evidence summary:

- Plotly validates row/column counts, start cell, spec keys, 2D dimensions,
  spans, secondary axes, and defaults.

Rust translation for the rewrite:

```rust
pub struct DashboardGridSpec {
    pub rows: NonZeroUsize,
    pub cols: NonZeroUsize,
    pub cells: Vec<Option<DashboardCellSpec>>,
}

impl DashboardGridSpec {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.cells.len() == self.rows.get() * self.cols.get());
        Ok(())
    }
}
```

Implications:

- Memory: validated specs avoid panic-heavy render paths.
- Performance: validate once before render.
- Concurrency: dashboard generation can run per report independently.
- Testing: invalid grid specs should fail with clear diagnostics.

Agentic coding guidance:

- Agents building dashboards should implement spec validation before visual
  polish.

## Reproducible Experiments

### Pattern: Explicit Seeds and Profile Matrices

Source:

- `scikit-learn/asv_benchmarks/benchmarks/common.py:98-160`
- `scikit-learn/asv_benchmarks/benchmarks/cluster.py:34-92`
- `great_expectations/great_expectations/execution_engine/partition_and_sample/sparkdf_data_sampler.py:23-67`

Evidence summary:

- scikit-learn defines fast, regular, and large-scale benchmark profiles.
- KMeans benchmarks use deterministic parameters and `random_state=0`.
- Great Expectations random sampling defaults probability and seed when missing.

Rust translation for the rewrite:

```rust
pub struct ExperimentSeed(u64);

pub struct ExperimentProfile {
    pub name: String,
    pub dataset_scale: DatasetScale,
    pub seed: ExperimentSeed,
    pub repetitions: usize,
}
```

Implications:

- Memory: store seed and profile with every artifact.
- Performance: profiles make cheap CI and expensive nightly runs coexist.
- Concurrency: deterministic seeds are not enough if thread scheduling changes
  outputs; record worker count too.
- Testing: tests should assert experiment metadata is written before results.

Agentic coding guidance:

- Agents should never report benchmark numbers without seed, profile, command,
  git revision, and machine notes.

### Pattern: Environment Reset Before Agent Experiments

Source:

- `LiveMCPBench/README.md:94-108`

Evidence summary:

- LiveMCPBench resets the environment before running agents because agents may
  mutate it.
- Reset copies repo code from mounted `/outside` into the benchmark environment
  and links data into `/root`.

Rust translation for the rewrite:

```bash
#!/usr/bin/env bash
set -euo pipefail
rm -rf .run/workspace
git worktree add .run/workspace HEAD
cp -R fixtures .run/workspace/fixtures
```

Implications:

- Memory: reset avoids relying on agent memory of what changed.
- Performance: reset cost is acceptable compared with invalid experiment data.
- Concurrency: use separate worktrees or directories per agent.
- Testing: reset script should be idempotent.

Agentic coding guidance:

- Run agents in clean workspaces for benchmark or conformance tasks.

## Rust Rewrite Operational Baseline

The following baseline is the minimum operational surface suggested by this
evidence slice.

### Observability Baseline

Required crates or equivalents:

- `tracing`
- `tracing-subscriber`
- `tracing-opentelemetry` or an OpenTelemetry exporter when needed
- `prometheus` or `metrics` plus a Prometheus exporter
- `serde` for typed health and report artifacts

Required metric families:

- Query started, finished, failed, canceled, timed out.
- Query latency by database, query kind, status.
- Plan/operator rows produced, bytes read, bytes written.
- WAL bytes, fsync latency, checkpoint latency.
- Page cache hits, misses, bytes, evictions.
- Lock wait latency by lock class.
- Transaction commit latency and rollback count.
- Storage read/write bytes, requests, errors, latency.
- Background job heartbeat and last-success timestamp.

Required trace fields:

- `trace_id`
- `request_id`
- `database`
- `tx_id`
- `query_kind`
- `operator_id`
- `store_id`
- `file_kind`
- `attempt`
- `status`

### Benchmark Baseline

Required benchmark tiers:

- `smoke`: tiny graph, under one minute, runs in PR CI.
- `regular`: representative local graph, runs before merge or nightly.
- `large`: LDBC/Graphalytics style, separate machine or scheduled runner.
- `agent`: tracks agent task completion, tool calls, tests run, and verification
  success for rewrite workflows.

Required artifacts per run:

- `command.txt`
- `git-revision.txt`
- `config.toml`
- `machine.json`
- `metrics.prom`
- `summary.json`
- `trajectory.jsonl` for agent runs
- `dashboard.html`

### Failure-Injection Baseline

Required fault families:

- Protocol: invalid magic, bad version, delayed handshake, truncated message,
  unexpected client message, disconnect during pull.
- Storage: failed write, partial read, slow fsync, object-store timeout.
- Transaction: crash before/after WAL append, lock timeout, deadlock detection.
- Cluster/routing: unavailable leader, stale routing table, connection leak.
- Agent: dirty workspace, missing tool, timed-out tool, invalid tool schema.

### Agent Workflow Baseline

Every long agent task should have:

- exclusive write target or declared ownership boundary,
- evidence commands,
- source line ranges,
- progress journal entry,
- tests written and their current status,
- verification commands,
- explicit gaps,
- no claims of performance improvement without benchmark artifacts.

### Pattern: Driver Retry Routing Verification Surface

Source:

- `neo4j-go-driver-src/neo4j/bookmarks.go:58-108`
- `neo4j-go-driver-src/neo4j/session.go:402-520`
- `neo4j-go-driver-src/neo4j/internal/retry/state.go:31-144`
- `neo4j-go-driver-src/neo4j/internal/bolt/bolt4.go:836-928`
- `neo4j-go-driver-src/neo4j/internal/bolt/bolt4.go:1018-1150`
- `neo4j-go-driver-src/neo4j/session_test.go:118-181`
- `neo4j-go-driver-src/neo4j/session_test.go:511-666`

Observed design:

- Bookmarks are causal state guarded by `sync.RWMutex`, not loose strings
  passed through the API. `UpdateBookmarks` removes previous bookmarks, adds
  new bookmarks, and optionally notifies a consumer.
- `ExecuteRead` and `ExecuteWrite` are thin helpers over `runRetriable`, so
  retry behavior has one session-level policy.
- `retry.State` owns max retry time, throttle, dead-connection budget,
  telemetry state, context cancellation, and retryability checks.
- Routing-table retrieval changes by negotiated Bolt minor version: direct
  ROUTE message, v4.3 route message, or system-database Cypher fallback.
- Pull/discard/commit success handlers update the connection bookmark from
  successful summaries or commit metadata.

Rust rewrite transfer:

- Treat Bolt/session/driver compatibility as an external verification loop
  around the lower-RAM storage and algorithm rewrite.
- Keep retry state explicit and testable. A Rust `RetryState` should make
  retry budget, dead connection count, skip-sleep behavior, and last errors
  observable to tests.
- Keep bookmark management isolated from transaction execution. A Rust
  implementation can use `Arc<RwLock<IndexSet<Bookmark>>>` or an async
  single-owner actor, but the contract should be the same: merge external
  bookmarks, replace previous causal state, and publish new state after
  successful transaction/stream outcomes.
- Split tests by layer:
  public helpers delegate to retry loop; retryable transient errors retry;
  user, pool-timeout, and commit-failed-dead errors do not retry; routing table
  acquisition follows Bolt version; stream/commit summaries update bookmarks.

Executable-spec candidates:

```text
REQ-DRIVER-001
WHEN ExecuteWrite receives a retryable transient database error from user work
THEN the session SHALL retry until the retry budget is exhausted or work succeeds
AND each failed attempt SHALL return its connection to the pool.

REQ-DRIVER-002
WHEN ExecuteWrite receives a user error or commit-failed-dead error
THEN the session SHALL NOT retry
AND the session SHALL return to a clean state.

REQ-DRIVER-003
WHEN Run receives an idempotent auto-commit error
THEN the session SHALL perform at most one auto-commit retry
AND driver-level and session-level disable flags SHALL resolve deterministically.

REQ-DRIVER-004
WHEN a Bolt 4 connection retrieves a routing table
THEN the connection SHALL choose ROUTE, ROUTE v4.3, or system-database Cypher
fallback according to negotiated minor version and impersonation support.

REQ-DRIVER-005
WHEN a stream summary or commit success contains a bookmark
THEN the connection SHALL update the latest bookmark
AND the session bookmark manager SHALL expose merged causal bookmarks under
concurrent reads.
```

Why it matters:

The storage rewrite can change physical layout, memory ownership, and graph
algorithm execution, but client compatibility is policed at the driver/session
surface. These specs let LLM coding agents rewrite internals while preserving
the behavior that application drivers expect.

## Gaps and Uninspected Repos

Explicit gaps:

- I did not fully index all assigned repositories with CodeGraphContext. The
  completed CGC runs are the working repo, `lazy-tool`, and the focused
  `neo4j-go-driver-src` pass. Broad high-value indexing of Neo4j testkit was
  stopped for cost, the narrow `boltstub_rs` direct run exited before producing
  usable query files, and the `tracing-src` wrapper run wrote a partial database
  but did not produce clean query outputs.
- I did not fully inspect every `ldbc*` repository. Direct evidence came from
  the ArcadeDB Graphalytics platform and shared benchmark helper; the other LDBC
  repositories were enumerated but not deeply read.
- I did not inspect the Spring Boot Java research corpus under
  `/Users/amuldotexe/Desktop/personal-repos-lane/room-of-requirement/.research-corpus/springboot-java/repos/*`.
- I did not deeply inspect every `*mcp*` or tool benchmark repo. Direct evidence
  came from `mcp-bench` and `LiveMCPBench`; `ToolBench`, `ToolRoute`,
  `ToolSandbox`, `StableToolBench`, `benchmarking-tool-retrieval`,
  `graph-tool-call`, `lazy-tool`, and `tau2-bench` were only enumerated or
  lightly searched.
- I did not inspect OpenObserve dashboard UI code; OpenObserve evidence here is
  backend observability, tracing, HTTP, storage, and flight metrics.
- I did not run the benchmark suites or tests from the source projects. This is
  a source-pattern extraction, not behavioral verification of those upstream
  repositories.
- The target file was created from direct reads and graph-tool navigation; graph
  tool outputs were not treated as proof of source behavior.

Future Worker 5 expansion targets:

- Complete CodeGraphContext on `tracing-src` or a smaller selected subset with a
  run that reaches `stats`, `list`, and `files_query` outputs cleanly.
- Read `boltstub_rs/src/*` directly and compare Rust stub design with Python
  `boltstub`.
- Read `ToolSandbox` and `tau2-bench` for sandboxing, tool failure, and task
  environment reset patterns.
- Read LDBC SNB interactive driver scheduling and validation code for workload
  realism, operation mixes, and deterministic event timelines.
- Add a concrete Rust crate skeleton for `graphdb-observability`,
  `graphdb-bench`, and `graphdb-testkit` once rewrite module boundaries are
  known.

## Batch 03 Pointer: Official Client And GDS Surface Contracts

See `supplemental-gap-closure-batch-03.md` for direct-source evidence from:

- Neo4j Java Driver fetch-size, retry, and bookmark manager contracts.
- Neo4j JavaScript Driver routing-table refresh and typed Vector values.
- Neo4j .NET Driver vector PackStream serializer and async bookmark/session
  behavior.
- Neo4j GDS Client API-spec coverage and Arrow endpoint coverage tests.
- Neo4j Browser editor, Bolt worker, result mapping, and query cancellation
  paths.
- Neo4j OGM session depth, identity map, mapping context, and entity graph
  mapper behavior.
- GDS Agent registry, result limits, session-memory controls, graph projection
  handlers, and algorithm mode dispatch.

The transferable lesson is that "Neo4j compatibility" is not one interface. It
is a stack of contracts: driver backpressure, retry/error metadata, bookmarks,
routing, vector wire values, Browser result mapping, OGM identity semantics,
GDS procedure inventory, GDS modes, GDS output limits, and graph catalog
behavior. A Rust rewrite can change storage internals only after these
application-facing contracts are registered, tested, and deliberately preserved.

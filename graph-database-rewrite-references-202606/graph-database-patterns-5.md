# Meta Graph Database Patterns 5

Worker 5 slice for observability, infrastructure, agents, benchmark harnesses,
testing frameworks, failure injection, developer tooling, and operational
patterns relevant to a Neo4j-style Rust database rewrite.

Created: 2026-07-06

## Scope

This file focuses on making the Rust rewrite measurable, debuggable,
benchmarkable, and maintainable during long agent-assisted development. Graph
tool outputs were used as navigational evidence only. Important claims below
are tied to direct source reads.

The core transfer rule:

```text
Every subsystem that can fail should emit:
1. a stable identifier,
2. a typed outcome,
3. a duration,
4. a size/count where relevant,
5. a reproducible test or benchmark artifact.
```

## Repositories and Evidence Tools

Required skills read before evidence work:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

Graph tools used:

- `codebase-memory-mcp` smoke scan on the working repo:
  `/tmp/codex-code-intel/codebase-memory/knight-bus-graph-walker-20260706-230233`
- `CodeGraphContext` smoke scan on the working repo:
  `/tmp/codex-code-intel/codegraphcontext/knight-bus-graph-walker-20260706-230233`
- Completed assigned high-value tooling repo CGC scan:
  `/tmp/codex-code-intel/codegraphcontext/lazy-tool-20260706-232335`
  - Source repo:
    `/Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/lazy-tool`
  - `stats.txt`: 1 repository, 165 files, 595 functions, 26 classes,
    9 interfaces, 82 structs, 34 modules.
  - `files_query.txt` surfaced benchmark docs, configs, golden snapshots,
    harness scripts, and Go internals. Claims about benchmark behavior were
    verified against `benchmark/README.md`.
- Attempted but incomplete CGC runs:
  - `neo4j-testkit-src`: stopped because full indexing was too expensive for
    this worker slice.
  - `boltstub_rs`: direct narrow run wrote partial output but no usable query
    files.
  - `tracing-src`: wrote a partial database/index log but no clean query output.

Directly inspected source repos:

- `/Users/amuldotexe/Desktop/oss-read-only/openobserve`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tracing-src`
- `/Users/amuldotexe/Desktop/oss-read-only/airflow`
- `/Users/amuldotexe/Desktop/oss-read-only/great_expectations`
- `/Users/amuldotexe/Desktop/oss-read-only/streamlit`
- `/Users/amuldotexe/Desktop/oss-read-only/scikit-learn`
- `/Users/amuldotexe/Desktop/oss-read-only/plotly.py`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-testkit-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_arcadedb-src`
- `/Users/amuldotexe/Desktop/methods-agents-hub/agent-room-of-requirements`
- `/Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/mcp-bench`
- `/Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/LiveMCPBench`
- `/Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/lazy-tool`

Representative evidence commands:

```bash
nl -ba /Users/amuldotexe/Desktop/oss-read-only/openobserve/src/config/src/metrics.rs | sed -n '1,260p'
nl -ba /Users/amuldotexe/Desktop/oss-read-only/openobserve/src/common/utils/http.rs | sed -n '143,220p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/tracing-src/README.md | sed -n '135,260p'
nl -ba /Users/amuldotexe/Desktop/oss-read-only/airflow/scripts/ci/prek/check_migration_patterns.py | sed -n '1,220p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-testkit-src/boltstub/channel.py | sed -n '32,90p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py | sed -n '1,190p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/lazy-tool/benchmark/README.md | sed -n '1,260p'
```

## Pattern Index

| Group | Patterns |
| --- | --- |
| Metrics/tracing | Prometheus edge registry, trace IDs, plan metrics, storage metrics, subscriber discipline |
| Structured logs | Binary-owned subscribers, benchmark job logs, script mismatch diagnostics |
| Benchmark harnesses | ASV history, cached setup, LDBC timed jobs, MCP agent metrics, search-first tool benchmarks |
| Integration testkit | Docker driver glue, scripted Bolt stubs, standalone/cluster fixtures |
| Failure injection | Bad handshakes, delayed handshakes, disconnects, broken wires, deployment waits |
| Config/deployment | Typed health, metrics backend selection, config-backed CLIs |
| Migrations | AST migration linter with explainable suppressions |
| CLI ergonomics | API-generated commands, sensitive flag rejection, quiet browser launch |
| Agent workflows | Evidence ledgers, skill packaging, trajectory capture, judge stability |
| Progress journals | TDD phase journal, worker IDs, evidence coverage ledgers |
| Dashboards | Offline HTML, validated grid specs |
| Reproducible experiments | Seeds, profiles, environment reset, artifact bundles |

## Metrics and Tracing

### Pattern: Prometheus Edge Metric Spine

Source:

- `openobserve/src/config/src/metrics.rs:16-65`
- `openobserve/src/config/src/metrics.rs:107-176`
- `openobserve/src/config/src/metrics.rs:190-250`
- `openobserve/src/config/src/metrics.rs:934-990`
- `openobserve/src/config/src/metrics.rs:2056-2075`

Evidence:

- OpenObserve centralizes metrics as `Lazy` Prometheus counters, gauges, and
  histograms.
- HTTP metrics have stable labels: endpoint, status, org, stream type, search
  type, and search group.
- Ingest metrics cover records, bytes, errors, WAL bytes, parquet writes, file
  sizes, and file times.
- Query metrics cover running, pending, timeout, canceled, DB query counts, and
  DB query time.
- `create_const_labels()` adds cluster/instance/role labels; `gather()` encodes
  the default registry.

Pseudocode:

```rust
static HTTP_REQUESTS: Lazy<IntCounterVec> =
    Lazy::new(|| IntCounterVec::new(opts, &["endpoint", "status", "org"]));

static QUERY_TIME: Lazy<HistogramVec> =
    Lazy::new(|| HistogramVec::new(histogram_opts, &["db", "query_kind", "status"]));

pub fn gather_metrics_text() -> String {
    let families = prometheus::gather();
    TextEncoder::new().encode_to_string(&families)
}
```

Rust translation:

```rust
pub struct GraphDbMetrics {
    pub query_finished_total: IntCounterVec,
    pub query_duration_seconds: HistogramVec,
    pub wal_bytes_written_total: IntCounterVec,
    pub lock_wait_seconds: HistogramVec,
    pub page_cache_bytes: IntGaugeVec,
}

impl GraphDbMetrics {
    pub fn observe_query_outcome(
        &self,
        database: &str,
        query_kind: &str,
        status: &str,
        elapsed: Duration,
    ) {
        self.query_finished_total
            .with_label_values(&[database, query_kind, status])
            .inc();
        self.query_duration_seconds
            .with_label_values(&[database, query_kind, status])
            .observe(elapsed.as_secs_f64());
    }
}
```

Implications:

- Memory: metric handles should be static or app-state singletons.
- Performance: labels must be low cardinality. Never label by raw query text,
  path, node ID, or user ID.
- Concurrency: Prometheus metric types are synchronized; avoid per-call metric
  registration.
- Testing: run one query, gather metrics, assert family names and labels.

Agentic guidance:

- Every subsystem PR should include a metric contract before implementation.
- Review agents should reject unbounded metric labels immediately.

### Pattern: Trace ID Ingress and Error Correlation

Source:

- `openobserve/src/handler/http/request/search/multi_streams.rs:758-864`
- `openobserve/src/common/utils/http.rs:143-215`
- `openobserve/src/common/utils/http.rs:577-725`
- `openobserve/src/config/src/ider.rs:76-105`

Evidence:

- The request handler starts a timer before execution and records latency and
  request counters on both success and error paths.
- It creates spans conditionally and gets or creates a trace ID from headers.
- Trace ID utility validates `x-openobserve-trace-id`, rejects all-zero and
  non-hex values, supports W3C `traceparent`, and generates IDs otherwise.
- Tests cover malformed traceparent, invalid IDs, all-zero, non-hex, priority,
  and fallback behavior.
- Trace IDs are generated from UUID v7 and support timestamp extraction.

Pseudocode:

```rust
let start = Instant::now();
let span = maybe_create_span(headers);
let trace_id = get_or_create_trace_id(headers, &span);

match execute().instrument(span).await {
    Ok(value) => {
        metrics.observe("200", start.elapsed());
        Ok(value)
    }
    Err(err) => {
        metrics.observe("500", start.elapsed());
        Err(err.with_trace_id(trace_id))
    }
}
```

Rust translation:

```rust
pub struct RequestContext {
    pub trace_id: TraceId,
    pub database: DatabaseName,
    pub query_kind: QueryKind,
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
        ctx.database.as_ref(),
        ctx.query_kind.as_str(),
        result_status(&result),
        started.elapsed(),
    );
    result.map_err(|err| err.with_trace_id(ctx.trace_id))
}
```

Implications:

- Memory: store trace IDs as fixed 16-byte values.
- Performance: parse trace headers once per request.
- Concurrency: span context must cross spawned tasks explicitly.
- Testing: table-test all trace header cases.

Agentic guidance:

- No new ingress endpoint should merge without trace ID extraction tests.
- User-visible errors should include a trace ID.

### Pattern: Query Plan Metrics Backchannel

Source:

- `openobserve/src/flight/src/common.rs:38-69`
- `openobserve/src/flight/src/common.rs:78-111`
- `openobserve/src/flight/src/common.rs:129-178`

Evidence:

- `RemoteScanMetrics` records fetch time, decode time, and output rows.
- `CustomMessage` carries scan stats, metrics, peak memory, and partial errors.
- Peak memory is read from `AtomicUsize`.
- Plan metrics are collected with stage, node address, and node name metadata.

Rust translation:

```rust
pub enum QueryTelemetryMessage {
    OperatorMetrics(Vec<OperatorMetric>),
    PeakMemoryBytes(usize),
    RowsProduced { operator_id: OperatorId, rows: u64 },
    PartialError { trace_id: TraceId, message: String },
}
```

Implications:

- Memory: operator metric structs must be compact.
- Performance: collect detailed plan metrics at query end, not per row.
- Concurrency: monotonic sampled peak memory can use relaxed atomics.
- Testing: snapshot explain/metrics output for small queries.

Agentic guidance:

- A new query operator should not land without its telemetry fields.

### Pattern: Storage Boundary Metrics

Source:

- `openobserve/src/infra/src/storage/remote.rs:90-180`

Evidence:

- Remote `put` and `get` paths measure start time at entry.
- Successful file operations increment bytes, requests, and storage-time metrics.
- Errors are logged at the storage boundary.

Rust translation:

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

- Memory: path-kind labels should be static.
- Performance: IO boundary metrics are cheap relative to disk/network work.
- Concurrency: adapters are the right place to observe retries.
- Testing: fake object stores should assert metric deltas.

Agentic guidance:

- Instrument adapters once instead of sprinkling metrics across every caller.

### Pattern: Subscriber Ownership and Async Span Discipline

Source:

- `tracing-src/README.md:46-85`
- `tracing-src/README.md:137-181`
- `tracing-src/README.md:197-250`
- `tracing-src/README.md:294-335`
- `tracing-src/README.md:381-425`

Evidence:

- Executables install subscribers; libraries emit spans/events.
- `#[tracing::instrument]` creates function spans and records parameters.
- The README warns against holding span guards across async boundaries and
  recommends `Future::instrument`.
- The ecosystem includes timing histograms, distributed tracing, logfmt, Loki,
  chrome tracing, cloudwatch, reload layers, and clippy support.

Rust translation:

```rust
pub fn install_observability_subscriber(config: &ObservabilityConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.rust_log)?;
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .try_init()
        .map_err(Into::into)
}

#[tracing::instrument(name = "graphdb.tx.commit", skip(engine))]
pub async fn commit_transaction_observed(
    engine: &StorageEngine,
    tx: Transaction,
) -> Result<CommitReceipt> {
    engine.commit(tx).await
}
```

Implications:

- Memory: do not store full query text in spans by default.
- Performance: debug spans in hot loops should be filterable.
- Concurrency: do not hold entered span guards across `.await`.
- Testing: use test subscribers to assert critical fields.

Agentic guidance:

- Library crates must not install global subscribers.
- Review async tracing code for guard lifetime mistakes.

## Structured Logs

### Pattern: Library Events, Binary-Owned Sinks

Source:

- `tracing-src/README.md:46-85`
- `tracing-src/README.md:197-199`
- `tracing-src/README.md:294-335`

Rust translation:

```rust
// graphdb-core
tracing::warn!(database = %db, store = %store_id, "page cache pressure high");

// graphdb-server
install_observability_subscriber(&config.observability)?;
```

Implications:

- Memory: one structured event can feed logs, metrics, traces, and dashboards.
- Performance: defer formatting through tracing fields.
- Concurrency: subscriber setup happens once at startup.
- Testing: local subscribers keep library tests isolated.

Agentic guidance:

- Reject `println!` debugging in reusable crates.

### Pattern: Benchmark Job Logs With Process Identity

Source:

- `ldbc_graphalytics_platforms_arcadedb-src/src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:91-105`
- `ldbc_graphalytics_platforms_arcadedb-src/src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:146-178`

Evidence:

- Platform logging starts in the benchmark log directory.
- `executable.pid` is written so the framework can find the platform process.
- Jobs log index, total, algorithm, graph, and elapsed time.
- Finalization stops logging and collects processing time metrics.

Rust translation:

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

- Memory: run metadata belongs in per-run state.
- Performance: benchmark logging must stay outside timed inner loops.
- Concurrency: each worker needs a unique output directory.
- Testing: assert PID/log files exist on both success and failure.

Agentic guidance:

- Long benchmark agents should write command, config, PID, stdout, stderr, and
  summary artifacts before reporting results.

## Benchmark Harnesses

### Pattern: Historical Benchmark Baselines With Dependency Matrix

Source:

- `scikit-learn/asv_benchmarks/asv.conf.json:1-190`

Evidence:

- ASV config includes project metadata, repo, build/install commands, branches,
  environments, timeout, dependency matrix, results directory, HTML directory,
  wheel cache, and regression search bounds.
- Dependency bumps are separated so regressions from dependency changes are
  identifiable.

Rust translation:

```toml
[project]
name = "graphdb-rust"

[commands]
build = "cargo build --release"
bench = "cargo bench --bench graphdb_bench -- --output json"

[matrix]
rust = ["stable", "beta"]
features = ["default", "jemalloc"]
datasets = ["tiny", "ldbc-sf1"]
```

Implications:

- Memory: record peak RSS and allocator per run.
- Performance: regression history beats one-off numbers.
- Concurrency: pin thread counts.
- Testing: CI should run smoke benchmarks; nightly can run full profiles.

Agentic guidance:

- Do not claim a speedup without a benchmark artifact and baseline commit.

### Pattern: Cached Setup With Time, Peak Memory, and Quality

Source:

- `scikit-learn/asv_benchmarks/benchmarks/common.py:13-220`
- `scikit-learn/asv_benchmarks/benchmarks/cluster.py:8-92`

Evidence:

- Benchmark profiles define warmup, repeat, number, and data size.
- Data and fitted setup are cached so timed functions measure the target path.
- Benchmarks track wall time, peak memory, train/test scores, prediction time,
  and same-prediction checks against a base commit.
- KMeans benchmarks use deterministic seeds and parameter grids.

Rust translation:

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
```

Implications:

- Memory: peak memory is a first-class benchmark dimension.
- Performance: load/setup timing must be separate from query timing.
- Concurrency: record worker count and runtime config.
- Testing: benchmark fixture hashes should be validated before timing.

Agentic guidance:

- Agents should not time fixture generation unless the benchmark is about load.

### Pattern: LDBC Timed Job Loop and Platform Lifecycle

Source:

- `ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py:17-49`
- `ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py:87-150`
- `ldbc_graphalytics_platforms_arcadedb-src/src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:87-178`

Evidence:

- Shared benchmark helper defines a five-minute timeout per query.
- `run_timed` returns elapsed, `"timeout"`, or `"N/A"`.
- Benchmark loop parses `--reset`, selects systems, runs cleanup, and prints a
  summary table.
- Platform lifecycle separates prepare, startup, run, finalize, terminate.

Rust translation:

```rust
pub enum BenchOutcome<T> {
    Completed { elapsed: Duration, value: T },
    TimedOut { timeout: Duration },
    Failed { error: String },
}
```

Implications:

- Memory: cleanup must run even after failure.
- Performance: distinguish timeout from failure.
- Concurrency: use runtime-aware timeouts in Rust.
- Testing: test completed, timed-out, and failed outcomes.

Agentic guidance:

- Never collapse timeout and failure into one benchmark status.

### Pattern: Agent Benchmark Metrics Across Completion, Tool Use, and Cost

Source:

- `mcp-bench/benchmark/runner.py:49-145`
- `mcp-bench/benchmark/runner.py:148-209`
- `mcp-bench/benchmark/runner.py:396-520`
- `mcp-bench/benchmark/results_aggregator.py:18-175`
- `mcp-bench/benchmark/evaluator.py:57-127`

Evidence:

- Connection lifecycle is isolated in an async manager.
- The runner supports distraction servers, judge stability, problematic tool
  filtering, concurrent summarization, fuzzy descriptions, retries, and timeouts.
- Aggregation tracks completion rate, fulfillment, grounding, tool
  appropriateness, parameter accuracy, dependency awareness, parallelism,
  schema compliance, execution success, time, rounds, tool calls, server count,
  and cross-server coordination.
- The evaluator randomizes rubric order to reduce judge bias.

Rust translation:

```rust
pub struct AgentRunMetrics {
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub avg_execution_ms: f64,
    pub avg_tool_calls: f64,
    pub grounding_score: f64,
    pub dependency_awareness_score: f64,
}
```

Implications:

- Memory: store trajectories on disk, aggregate summaries in memory.
- Performance: evaluate after execution, outside timed path.
- Concurrency: retries should recreate tool/server connections.
- Testing: inject fake evaluator and fake server manager.

Agentic guidance:

- Rewrite agents should record tool calls, tests run, files changed, failures,
  and final verification per task.

### Pattern: Search-First Tool Surface Benchmark

Source:

- `lazy-tool/benchmark/README.md:19-51`
- `lazy-tool/benchmark/README.md:64-147`
- `lazy-tool/benchmark/README.md:160-221`
- `lazy-tool/benchmark/README.md:225-260`
- CodeGraphContext navigation:
  `/tmp/codex-code-intel/codegraphcontext/lazy-tool-20260706-232335`

Evidence:

- The benchmark compares `lazy-tool` against direct MCP gateway attachment.
- It asks whether search mode reduces prompt overhead, whether direct mode adds
  overhead, whether a smaller MCP surface helps discovery, and whether routed
  search-then-invoke calls are stable.
- It explicitly says not to overclaim universal tool reliability, all-model
  compatibility, production-grade stability, or superiority in every scenario.
- The reproducible flow builds, reindexes, checks source status, sanity-checks
  search for echo/prompt/resource, then runs a clean README suite.
- Multi-provider mode compares baseline, search, and direct modes and supports
  JSONL/CSV outputs.
- Weak-model tiers separate basic tool calling, search navigation, and
  deterministic search quality.

Rust translation:

```rust
pub enum ToolSurfaceMode {
    BaselineDirect,
    SearchThenInvoke,
    TransparentProxy,
}

pub struct ToolSurfaceBenchmark {
    pub mode: ToolSurfaceMode,
    pub task: String,
    pub repeat: usize,
    pub output_jsonl: PathBuf,
    pub output_csv: PathBuf,
}
```

Implications:

- Memory: prompt/tool-surface size is measurable state, not prose.
- Performance: compare latency, token count, and success rate separately.
- Concurrency: routed tool calls need deterministic verification and clean
  gateway state.
- Testing: include no-key deterministic search-quality tests.

Agentic guidance:

- For the Rust rewrite, compare "full tool surface" vs "small routed tool
  surface" when agents work against database tooling.
- Publish narrow benchmark claims only; keep overclaim exclusions in the report.

## Integration Testkit

### Pattern: Dockerized Driver Backend Glue

Source:

- `neo4j-testkit-src/README.md:1-90`
- `neo4j-testkit-src/README.md:120-240`
- `neo4j-testkit-src/driver.py:10-95`
- `neo4j-testkit-src/driver.py:143-210`

Evidence:

- Testkit declares driver name, driver repo, branch, build cache, artifacts dir,
  backend timeout, and `TEST_RUSTY_STUB`.
- It supports debug logging and disabling backend timeout for debugger sessions.
- Integration tests need real Neo4j; stub tests use scripts and do not need a
  running Neo4j server.
- Driver glue builds Docker images, starts backend detached, waits for port, and
  asserts connections close.

Rust translation:

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
}
```

Implications:

- Memory: backend processes need owned handles and deterministic cleanup.
- Performance: build caches should be explicit and invalidatable.
- Concurrency: isolated ports and artifact directories are required.
- Testing: readiness and connection-leak checks should be mandatory.

Agentic guidance:

- New protocol features need both scripted stub tests and integration tests.

### Pattern: Scripted Bolt Stub as Protocol Oracle

Source:

- `neo4j-testkit-src/README.md:187-203`
- `neo4j-testkit-src/boltstub/channel.py:36-90`
- `neo4j-testkit-src/boltstub/channel.py:130-205`
- `neo4j-testkit-src/boltstub/channel.py:270-325`

Evidence:

- `Channel` is the glue between stub script, socket, and Bolt protocol.
- It validates magic preamble, negotiates handshake, aborts on mismatch,
  translates server lines, consumes client messages, peeks buffered messages,
  asserts no unexpected input, and auto-responds.

Rust translation:

```rust
pub enum ScriptStep {
    ExpectClient(ClientMessagePattern),
    SendServer(ServerMessage),
    SendRaw(Bytes),
    AssertNoInput,
    AutoRespond,
}
```

Implications:

- Memory: pre-parse scripts into compact enums.
- Performance: clarity beats cleverness in the stub server.
- Concurrency: each connection owns script state.
- Testing: mismatch errors need line number and received message.

Agentic guidance:

- Encode protocol tests as script fixtures, not giant imperative tests.

### Pattern: Standalone and Cluster Fixtures

Source:

- `neo4j-testkit-src/neo4j.py:29-115`

Evidence:

- Testkit separates standalone and cluster fixtures.
- Standalone maps version differences to environment variables and mounts logs.
- Cluster fixture creates cores and returns per-core Bolt addresses.

Rust translation:

```rust
pub enum DatabaseFixture {
    Standalone(StandaloneFixture),
    Cluster(ClusterFixture),
}
```

Implications:

- Memory: fixture state owns all child resources.
- Performance: cluster fixtures are nightly/integration tier.
- Concurrency: names, networks, and ports must be unique.
- Testing: routing behavior needs multi-node fixtures early.

Agentic guidance:

- Do not fake cluster-only behavior against a single-node fixture.

## Failure Injection

### Pattern: Handshake and Wire Fault Injection

Source:

- `neo4j-testkit-src/boltstub/channel.py:100-122`
- `neo4j-testkit-src/boltstub/channel.py:256-268`
- `neo4j-testkit-src/boltstub/wiring.py:235-356`
- `neo4j-testkit-src/boltstub/tests/test_integration.py:460-545`
- `neo4j-testkit-src/tests/stub/iteration/test_result_single.py:70-112`

Evidence:

- Stub can force custom handshakes, verify expected client bytes, abort
  handshakes, delay handshakes, and track closed/broken wire state.
- Tests cover custom handshake, timing bounds for delay, disconnect during pull,
  and server-side error during pull.

Rust translation:

```rust
pub enum FaultInjection {
    CustomHandshake { response: Bytes, expected_client: Option<Bytes> },
    AbortHandshake,
    DelayHandshake(Duration),
    DisconnectAfter(ServerStep),
    ErrorOnPull { code: String },
    ShortRead { bytes_before_close: usize },
}
```

Implications:

- Memory: fault descriptors can live in script state.
- Performance: fault injection stays outside production hot paths.
- Concurrency: wire state is per connection.
- Testing: each retry/recovery path needs an injected failure.

Agentic guidance:

- Happy-path protocol tests are not enough. Force the bad path.

### Pattern: Health Wait and Rollout Verification

Source:

- `airflow/kubernetes-tests/tests/kubernetes_tests/test_base.py:186-206`
- `airflow/kubernetes-tests/tests/kubernetes_tests/test_base.py:246-263`

Evidence:

- Airflow polls `/monitor/health` with retry and timeout.
- Rollout verification shells to `kubectl rollout status` and accepts multiple
  success phrases.

Rust translation:

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

- Memory: health snapshots should be small.
- Performance: health must not run expensive queries.
- Concurrency: avoid startup lock stampedes.
- Testing: wait on health, not arbitrary sleeps.

Agentic guidance:

- Every long-running process needs a readiness helper in tests.

## Testing Frameworks and Validation

### Pattern: Validation Checkpoint With Action Pipeline

Source:

- `great_expectations/great_expectations/checkpoint/checkpoint.py:69-115`
- `great_expectations/great_expectations/checkpoint/checkpoint.py:295-413`

Evidence:

- Checkpoint is the production validation surface.
- It contains validation definitions, actions, result format, and ID.
- `run()` rejects empty validation definitions, handles freshness diagnostics,
  prepares params/run ID, runs validations, constructs result, and runs sorted
  actions.

Rust translation:

```rust
pub struct ValidationCheckpoint {
    pub id: CheckpointId,
    pub validations: Vec<ValidationDefinition>,
    pub actions: Vec<Box<dyn ValidationAction>>,
}
```

Implications:

- Memory: stream large validation details to artifacts.
- Performance: actions run after core validation timing.
- Concurrency: independent validations can parallelize if non-mutating.
- Testing: action order and empty-checkpoint failure need tests.

Agentic guidance:

- Milestones should end with a checkpoint, not just "tests pass".

### Pattern: Metric Dependency Graph for Validation

Source:

- `great_expectations/great_expectations/validator/validation_graph.py:87-174`

Evidence:

- `ValidationGraph` owns execution engine, edges, and edge IDs.
- It deduplicates edges, builds metric dependency graphs, and warns on circular
  dependencies.

Rust translation:

```rust
pub struct MetricDependencyGraph {
    edges: IndexSet<(MetricId, MetricId)>,
}
```

Implications:

- Memory: use compact metric IDs.
- Performance: topological order can be cached.
- Concurrency: independent metric nodes can run in parallel.
- Testing: cycle and dedup tests are required.

Agentic guidance:

- Represent validation dependencies explicitly, not as hidden test call order.

## Config and Deployment

### Pattern: Typed Health Response by Subsystem

Source:

- `airflow/airflow-core/src/airflow/api/common/airflow_health.py:21-95`
- `airflow/airflow-core/src/airflow/api_fastapi/core_api/datamodels/monitor.py:1-70`

Evidence:

- Health reports metadatabase, scheduler, triggerer, and DAG processor.
- Component health is based on latest heartbeat/liveness.
- Exceptions while checking jobs mark metadatabase unhealthy.
- Pydantic models type response shapes.

Rust translation:

```rust
#[derive(Serialize)]
pub struct HealthInfo {
    pub metastore: ComponentHealth,
    pub query_engine: ComponentHealth,
    pub storage_engine: ComponentHealth,
    pub raft: Option<ComponentHealth>,
}
```

Implications:

- Memory: health reads small shared state.
- Performance: health should not acquire long write locks.
- Concurrency: heartbeat writers are independent.
- Testing: stale heartbeat, live heartbeat, and metastore exception.

Agentic guidance:

- New background subsystems need health fields.

### Pattern: Config-Driven Metrics Backend Selection

Source:

- `airflow/airflow-core/src/airflow/observability/metrics/stats_utils.py:20-45`
- `airflow/airflow-core/src/airflow/observability/metrics/otel_logger.py:28-55`

Evidence:

- Airflow chooses Datadog StatsD, StatsD, OTel, or no-op based on config.
- OTel config supports host, port, prefix, SSL, interval, debug, service,
  allow/block lists, and fallbacks.

Rust translation:

```rust
pub enum MetricsBackendConfig {
    Prometheus { bind: SocketAddr },
    OpenTelemetry { endpoint: Url, service_name: String },
    Statsd { addr: SocketAddr, prefix: String },
    Noop,
}
```

Implications:

- Memory: no-op sink avoids allocation in tests.
- Performance: backend selection is startup-only.
- Concurrency: sinks must be `Send + Sync`.
- Testing: config parse tests for every backend.

Agentic guidance:

- Keep telemetry backend choice out of core crates.

### Pattern: Config-Backed CLI With Sensitive Option Rejection

Source:

- `streamlit/lib/streamlit/web/cli.py:44-115`
- `streamlit/lib/streamlit/web/cli.py:130-265`

Evidence:

- CLI options are derived from config.
- Env vars use an automatic prefix.
- Sensitive options are rejected on CLI and must use config/env.
- Run target is validated before execution.

Rust translation:

```rust
#[derive(clap::Parser)]
pub struct GraphDbCli {
    #[arg(long, env = "GRAPHDB_SERVER_BIND")]
    pub server_bind: Option<SocketAddr>,
    #[arg(long, env = "GRAPHDB_CONFIG")]
    pub config: Option<PathBuf>,
}
```

Implications:

- Memory: config loads once into typed structs.
- Performance: validation happens before startup.
- Concurrency: runtime reload must be explicit.
- Testing: env/config/path/sensitive option parser tests.

Agentic guidance:

- Add CLI tests with every user-facing option.

## Migrations

### Pattern: AST Migration Anti-Pattern Linter

Source:

- `airflow/.pre-commit-config.yaml:1106-1122`
- `airflow/scripts/ci/prek/check_migration_patterns.py:25-211`

Evidence:

- Airflow registers `check-migration-patterns`.
- MIG001/MIG002/MIG003 detect DML/DDL before SQLite foreign-key disabling and
  DML without offline-mode guards.
- Suppressions require `# noqa: MIG0XX -- reason`.
- The linter parses AST and explains the underlying SQLite transaction hazard.

Rust translation:

```rust
pub enum MigrationLint {
    DataBeforeForeignKeyGuard { file: PathBuf, line: usize },
    SchemaBeforeForeignKeyGuard { file: PathBuf, line: usize },
    OnlineOnlyMutationWithoutGuard { file: PathBuf, line: usize },
}
```

Implications:

- Memory: parse migration DSL once per file.
- Performance: linter runs in pre-commit/CI.
- Concurrency: linting can be parallel.
- Testing: each rule needs bad, good, and suppression fixtures.

Agentic guidance:

- Add lint rules for migration mistakes discovered during the rewrite.

## CLI Ergonomics

### Pattern: API-Generated CLI Command Surface

Source:

- `airflow/airflow-ctl/src/airflowctl/ctl/cli_config.py:323-430`

Evidence:

- Airflow maps API operations to CLI action/group commands.
- AST inspection parses operation arguments and return annotations.

Rust translation:

```rust
pub struct OperationDescriptor {
    pub name: &'static str,
    pub group: &'static str,
    pub args: &'static [OperationArg],
    pub output: OutputFormat,
}
```

Implications:

- Memory: descriptors can be static.
- Performance: CLI construction is startup-only.
- Concurrency: none.
- Testing: generated help output should be deterministic.

Agentic guidance:

- Do not duplicate API semantics manually in CLI handlers.

### Pattern: Quiet Developer UX for Browser Launch

Source:

- `streamlit/lib/streamlit/cli_util.py:68-110`

Evidence:

- Streamlit implements browser open itself to avoid noisy browser output.
- It handles Windows, Linux, and macOS differently.

Rust translation:

```rust
pub fn open_browser_quietly(url: &str) -> io::Result<()> {
    let mut cmd = platform_open_command(url);
    cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn()?;
    Ok(())
}
```

Implications:

- Memory: trivial.
- Performance: do not block on browser completion.
- Concurrency: browser launch is outside server startup critical path.
- Testing: unit-test platform command selection.

Agentic guidance:

- Developer tools should not pollute benchmark or CI output.

## Agent Workflows

### Pattern: Chunked Evidence Ledger

Source:

- `agent-room-of-requirements/A02-Mega-Idiomatic-Prompt.md:385-408`
- `agent-room-of-requirements/A02-Mega-Idiomatic-Prompt.md:720-850`

Evidence:

- Long sources are read in chunks of at most 100 lines.
- Each chunk appends candidate parameters and preserves line ranges.
- Outputs include verification matrices, anti-pattern registries, skill
  packaging parameters, future refresh queries, open questions, progressive
  loading, native gates, and human scan acceptance tests.

Rust rewrite translation:

```markdown
| source_path | line_range | pattern | failure_prevented | proof_gate |
| --- | --- | --- | --- | --- |
| src/query/planner.rs | 120-180 | operator metrics | invisible regressions | explain snapshot |
```

Implications:

- Memory: ledgers offload context.
- Performance: agents avoid rereading whole repos.
- Concurrency: workers can own disjoint target files.
- Testing: verifier can require source, implication, and proof gate.

Agentic guidance:

- Every long rewrite task should write evidence rows before code changes.

### Pattern: Trajectory Capture and Success-Rate Evaluation

Source:

- `LiveMCPBench/README.md:94-185`
- `LiveMCPBench/evaluator/stat_success_rate.py:1-80`

Evidence:

- Environment reset is recommended because agents mutate state.
- Tool checks and server indexing happen before runs.
- Trajectories are stored separately from evaluator output.
- Success-rate script aggregates reward by category and overall, writes CSV.

Rust translation:

```rust
pub struct AgentRunArtifactPaths {
    pub trajectory_dir: PathBuf,
    pub evaluator_dir: PathBuf,
    pub success_rate_csv: PathBuf,
}
```

Implications:

- Memory: store trajectories as JSONL.
- Performance: evaluation should be resumable.
- Concurrency: each run needs a clean workspace.
- Testing: evaluator handles empty categories.

Agentic guidance:

- Multi-agent batches need reset, warmup, run, evaluate, summarize scripts.

### Pattern: Tool Benchmark Config Flags

Source:

- `mcp-bench/config/config_loader.py:430-690`

Evidence:

- Config exposes retries, backoff, default ports, tool-description truncation,
  token budgets, task files, judge stability, problematic tool filtering,
  concurrent summarization, fuzzy descriptions, concrete references,
  sequential-only tools, evaluation max tokens, cache TTL/size/strategy, cache
  cleanup, persistence, whitelist, and problematic tools.

Rust translation:

```rust
pub struct AgentWorkflowConfig {
    pub max_retries: usize,
    pub retry_backoff: BackoffConfig,
    pub token_budget: usize,
    pub enable_judge_stability: bool,
    pub sequential_only_tools: BTreeSet<String>,
    pub cache: AgentCacheConfig,
}
```

Implications:

- Memory: caches need max size and cleanup policy.
- Performance: token budgets keep runs bounded.
- Concurrency: sequential-only tool lists prevent unsafe parallel calls.
- Testing: config defaults need snapshot tests.

Agentic guidance:

- Treat agent workflow config as infrastructure, not prompt prose.

## Progress Journals

### Pattern: TDD Phase Journal for Long Multi-Worker Tasks

Source:

- `agent-room-of-requirements/agents-used-monthly-archive/idiomatic-references-202606/idiomatic-code-patterns-progress.md:1-163`
- `agent-room-of-requirements/agents-used-monthly-archive/claude-skills-202602/tdd-task-progress-context-retainer.md:8-112`

Evidence:

- Journal records task, timestamps, current phase, status, sessions, tests
  written, implementation progress, focus, next steps, context notes, metrics,
  worker IDs, pattern counts, smoke status, pending counts, and verifier state.
- TDD retainer emphasizes exact tests, statuses, paths, signatures, errors,
  attempts, design decisions, metrics, next steps, and cross-crate dependencies.

Rust rewrite translation:

```markdown
# GraphDB Rewrite Progress Journal

- Current Phase: Red | Green | Refactor | Verify
- Status:

## Session: <timestamp>
### Tests Written
### Implementation Progress
### Current Focus
### Next Steps
### Context Notes
### Performance/Metrics
```

Implications:

- Memory: journals keep agent sessions resumable.
- Performance: metrics expose whether work improves outcomes.
- Concurrency: worker IDs and exclusive targets reduce accidental edits.
- Testing: progress verifier checks next step and proof gate.

Agentic guidance:

- Every milestone ends with journal updates and exact commands run.

## Dashboards

### Pattern: Offline HTML Dashboard Artifact

Source:

- `plotly.py/plotly/io/_html.py:35-145`
- `plotly.py/plotly/io/_html.py:352-520`

Evidence:

- `to_html` supports self-contained output, CDN, directory-local bundle,
  partial div output, validation, div IDs, and post-script hooks.
- `write_html` copies the JS bundle once for directory mode, writes the file,
  and can auto-open.

Rust translation:

```rust
pub enum DashboardBundleMode {
    SelfContained,
    Cdn,
    DirectoryLocal,
}
```

Implications:

- Memory: render dashboard from summary JSON, not raw traces.
- Performance: dashboard generation happens after benchmarks.
- Concurrency: per-run directories avoid conflicts.
- Testing: snapshot HTML skeleton and validate assets.

Agentic guidance:

- Produce local dashboard artifacts for benchmark comparisons.

### Pattern: Validated Dashboard Grid Spec

Source:

- `plotly.py/plotly/_subplots.py:42-145`
- `plotly.py/plotly/_subplots.py:362-462`

Evidence:

- Plotly validates rows, cols, start cell, spec keys, dimensions, spans,
  secondary axes, and defaults.

Rust translation:

```rust
pub struct DashboardGridSpec {
    pub rows: NonZeroUsize,
    pub cols: NonZeroUsize,
    pub cells: Vec<Option<DashboardCellSpec>>,
}
```

Implications:

- Memory: validated specs avoid panic-heavy rendering.
- Performance: validate once before render.
- Concurrency: dashboard generation can run per report.
- Testing: invalid grids fail with clear diagnostics.

Agentic guidance:

- Implement dashboard spec validation before visual polish.

## Reproducible Experiments

### Pattern: Explicit Seeds and Profile Matrices

Source:

- `scikit-learn/asv_benchmarks/benchmarks/common.py:98-160`
- `scikit-learn/asv_benchmarks/benchmarks/cluster.py:34-92`
- `great_expectations/great_expectations/execution_engine/partition_and_sample/sparkdf_data_sampler.py:23-67`

Evidence:

- scikit-learn defines fast, regular, and large-scale profiles.
- KMeans uses deterministic parameters and `random_state=0`.
- Great Expectations random sampling defaults probability and seed.

Rust translation:

```rust
pub struct ExperimentProfile {
    pub name: String,
    pub dataset_scale: DatasetScale,
    pub seed: u64,
    pub repetitions: usize,
}
```

Implications:

- Memory: write seed/profile into every artifact.
- Performance: cheap CI and expensive nightly profiles can coexist.
- Concurrency: record worker count as well as seed.
- Testing: assert metadata is written before results.

Agentic guidance:

- No benchmark number without seed, profile, command, git revision, and machine
  notes.

### Pattern: Environment Reset Before Agent Experiments

Source:

- `LiveMCPBench/README.md:94-108`

Evidence:

- LiveMCPBench resets environment before running agents because agents may
  mutate state.
- Reset copies repo code from mounted `/outside` into benchmark environment and
  links data into `/root`.

Rust rewrite translation:

```bash
#!/usr/bin/env bash
set -euo pipefail
rm -rf .run/workspace
git worktree add .run/workspace HEAD
cp -R fixtures .run/workspace/fixtures
```

Implications:

- Memory: reset avoids relying on agent memory.
- Performance: reset cost is smaller than invalid experiment data.
- Concurrency: use separate worktrees per agent.
- Testing: reset script should be idempotent.

Agentic guidance:

- Run benchmark/conformance agents in clean workspaces.

## Rust Rewrite Operational Baseline

Minimum observability:

- Query started, finished, failed, canceled, timed out.
- Query latency by database, query kind, status.
- Operator rows produced, bytes read, bytes written.
- WAL bytes, fsync latency, checkpoint latency.
- Page cache hits, misses, bytes, evictions.
- Lock wait latency by lock class.
- Transaction commit latency and rollback count.
- Storage read/write bytes, requests, errors, latency.
- Background job heartbeat and last-success timestamp.

Minimum trace fields:

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

Benchmark tiers:

- `smoke`: tiny graph, under one minute, PR CI.
- `regular`: representative local graph, pre-merge or nightly.
- `large`: LDBC/Graphalytics style, scheduled runner.
- `agent`: tracks agent task completion, tool calls, tests run, and verification.

Required run artifacts:

- `command.txt`
- `git-revision.txt`
- `config.toml`
- `machine.json`
- `metrics.prom`
- `summary.json`
- `trajectory.jsonl`
- `dashboard.html`

Failure-injection families:

- Protocol: invalid magic, bad version, delayed handshake, truncated message,
  unexpected client message, disconnect during pull.
- Storage: failed write, partial read, slow fsync, object-store timeout.
- Transaction: crash before/after WAL append, lock timeout, deadlock detection.
- Cluster/routing: unavailable leader, stale routing table, connection leak.
- Agent: dirty workspace, missing tool, timed-out tool, invalid tool schema.

Agent workflow requirements:

- exclusive write target,
- evidence commands,
- source line ranges,
- progress journal entry,
- tests and current status,
- verification commands,
- explicit gaps,
- no performance claim without benchmark artifacts.

## Gap Closure: Parser, Allocator, Ladybug, And LDBC Repos

This section upgrades the eight previously inventory-only repos in
`gitrefrepo-coverage-ledger.tsv` into source-backed evidence. It also records
the latest graph-tool evidence:

- CodeGraphContext indexed `libcypher-parser-src` at
  `/tmp/codex-code-intel/codegraphcontext/gap-closure-20260707063927/libcypher-parser-src`
  and reported 182 files, 537 functions, 1426 classes, 183 structs, 2 enums,
  and 43 modules.
- CodeGraphContext `find name parse` found `parse` in
  `lib/src/quick_parser.c:137` and parser config symbols.
- CodeGraphContext `find name ast` found AST implementation and test surfaces
  such as `lib/src/ast_all.c`, `lib/src/ast_any.c`,
  `lib/src/ast_float.c`, `lib/src/ast_set.c`, `lib/src/astnode.h`, and
  many `lib/test/check_*` files.
- `codebase-memory-mcp` indexed `libcypher-parser-src` with 194 discovered
  files, 3179 nodes, and 10540 edges. Its `parse` search found exported
  `cypher_parse*` result and segment APIs plus `parse_unterminated_string`
  in `lib/test/check_errors.c`.
- CodeGraphContext indexing for `jemalloc-src` and
  `ldbc_snb_interactive_v2_driver-src` was attempted but interrupted after
  hanging past the useful point; direct source reads are treated as the
  authoritative evidence for those repos.

### Pattern: Grammar Layering As Compatibility Boundary

Found in:

- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:32-185`

Evidence:

- The Cypher parser grammar is separated from the lexer with
  `tokenVocab = CypherLexer`.
- Top-level syntax flows from `script` to `query`, `regularQuery`,
  `singleQuery`, `standaloneCall`, `returnSt`, `withSt`, `singlePartQ`,
  `multiPartQ`, `readingStatement`, and `updatingStatement`.
- Reads (`MATCH`, `UNWIND`, query calls) and updates (`CREATE`, `MERGE`,
  `DELETE`, `SET`, `REMOVE`) are distinct grammar families.

Rust rewrite translation:

```rust
enum CypherClause {
    Match(MatchClause),
    Unwind(UnwindClause),
    Call(CallClause),
    Create(CreateClause),
    Merge(MergeClause),
    Delete(DeleteClause),
    Set(SetClause),
    Remove(RemoveClause),
    Return(ReturnClause),
    With(WithClause),
}
```

Why this matters:

- The grammar is a compatibility contract before it is a planner contract.
- Future agents should keep syntax acceptance, semantic binding, and physical
  planning in separate crates or modules.
- A low-RAM Rust rewrite should not mix parser allocation decisions with graph
  storage layout decisions.

Agentic guidance:

- Generate parser tests from grammar-level fixtures first.
- Preserve top-level clause taxonomy even if the first execution engine only
  implements a subset.
- Treat unsupported clauses as typed semantic errors, not parser failures,
  when the syntax is valid.

### Pattern: Parser Source Abstraction And Result Contract

Found in:

- `gitrefrepo/libcypher-parser-src/lib/src/parser.c:52-138`
- `gitrefrepo/libcypher-parser-src/lib/src/result.c:25-163`

Evidence:

- `parser.c` abstracts input behind buffer and stream source callbacks.
- `uparse` and `fparse` share the same internal parse path.
- `result.c` exposes roots, directives, error count, EOF state, AST printing,
  and segment merging through narrow result accessors.

Rust rewrite translation:

```rust
trait CypherSource {
    fn fill_buffer(&mut self, out: &mut [u8]) -> Result<usize, ParseIoError>;
}

struct ParseResult {
    roots: Vec<AstNodeId>,
    directives: Vec<AstNodeId>,
    errors: Vec<ParseError>,
    eof: bool,
}
```

Why this matters:

- The parser can support strings, files, sockets, REPL streams, and test
  fixtures without changing the AST/result contract.
- Result accessors are a stable boundary for CLI tools, language servers,
  explainers, and future query-plan generators.
- For Rust, this points toward arena-backed AST nodes plus stable `AstNodeId`
  handles rather than pointer-heavy trees leaking across modules.

Memory implications:

- Keep the AST arena scoped to a parse result.
- Store spans and node ids compactly; avoid cloning query substrings unless a
  later semantic phase needs owned text.
- Let parsing return structured diagnostics even when AST recovery is partial.

### Pattern: Parser Error Oracles And Resynchronization Tests

Found in:

- `gitrefrepo/libcypher-parser-src/lib/test/check_errors.c:47-177`

Evidence:

- Tests assert exact AST/error pretty-print output for unterminated strings,
  invalid directives, invalid clauses, and invalid query resynchronization.
- Tests assert directive counts, error counts, line/column/offset positions,
  and exact error messages.

Rust rewrite translation:

```rust
#[test]
fn parse_invalid_clause_resynchronizes_query_stream() {
    let result = parse_cypher("MATCH (n)\n[1,2,3]\nRETURN n");
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].position.offset, 10);
    insta::assert_snapshot!(result.pretty_tree());
}
```

Why this matters:

- Parser correctness is not just "accept good input"; it is stable recovery,
  stable spans, and useful diagnostics.
- LLM agents need golden parse-tree/error snapshots so parser rewrites do not
  silently change public behavior.
- A Neo4j-compatible rewrite should preserve syntax errors as part of the user
  experience, not treat them as incidental strings.

### Pattern: Allocator Extent Cache With Delayed Coalescing

Found in:

- `gitrefrepo/jemalloc-src/src/extent.c:77-240`

Evidence:

- `extent_try_delayed_coalesce` temporarily marks extents active, tries to
  coalesce, then restores the cache state.
- `ecache_alloc` and `ecache_alloc_grow` separate recycled extents from
  retained/grown allocations.
- `ecache_dalloc` resets address/zeroed state before recording the extent.
- `ecache_evict` chooses the LRU extent, prefers retaining guarded extents,
  respects a minimum page threshold, and marks/deregisters extents before
  release to protect concurrent operations.

Rust rewrite translation:

```rust
enum PageExtentState {
    Active,
    Dirty,
    Reclaimable,
    Retained,
    Guarded,
}

struct PageExtent {
    file_offset: u64,
    pages: u32,
    state: PageExtentState,
    zeroed: bool,
}
```

Why this matters:

- Graph stores need allocator-like discipline for page cache, relationship
  blocks, property blocks, label indexes, and temporary algorithm workspaces.
- Lazy coalescing avoids doing expensive merge work on every free path while
  still giving the eviction path a chance to compact memory.
- Guarded or expensive-to-purge regions should be represented explicitly,
  not hidden in ad hoc booleans.

Agentic guidance:

- Do not implement graph page reclamation as a plain `Vec<PageId>` free list
  once concurrency and crash recovery enter the design.
- Encode state transitions and lock ownership in tests.
- Benchmark fragmentation, resident memory, and page reuse separately from
  query latency.

### Pattern: Morsel-Driven Scan Parallelism

Found in:

- `gitrefrepo/ladybug-src/docs/morsel_parallelism.md:1-155`

Evidence:

- Ladybug documents `ScanNodeTable` as the main scan loop, with shared state
  assigning morsels and per-thread scan state holding current position.
- Native node tables use coarse morsels of roughly one node group, around
  128K rows.
- Arrow node tables use fine-grained morsels of 2048 rows inside batches.
- Arrow scanning applies semi-mask filtering and writes the selection vector
  before copying into output vectors.

Rust rewrite translation:

```rust
trait MorselSource {
    fn next_morsel(&self, worker: WorkerId) -> Option<Morsel>;
}

struct NodeMorsel {
    table_id: TableId,
    group_id: u32,
    row_start: u32,
    row_end: u32,
}
```

Why this matters:

- OLAP graph scans and GDS projections should be scheduled as morsels, not as
  one giant iterator guarded by a global lock.
- Different storage layouts deserve different morsel sizes: native row groups,
  Arrow batches, CSR partitions, and mmap pages should not share a magic
  constant.
- Semi-mask filtering is a clean bridge between graph algorithms and columnar
  execution: compute a compact selection vector before materializing values.

Concurrency implications:

- Shared state should assign work atomically.
- Per-worker state should be small, cloneable, and resettable.
- Query cancellation can stop at morsel boundaries without corrupting shared
  table state.

### Pattern: Deterministic Validation Before Throughput

Found in:

- `gitrefrepo/ldbc_snb_interactive_v1_impls-src/README.md:62-101`
- `gitrefrepo/ldbc_snb_interactive_v2_impls-src/README.md:81-118`

Evidence:

- Both v1 and v2 implementation repos define three modes: create validation
  parameters, validate, and benchmark.
- Validation parameter generation and validation must be single-threaded to
  preserve deterministic operation order.
- Benchmark mode measures schedule audit success, throughput, time compression
  ratio, read thread count, warmup, and benchmark duration.
- v2 explicitly says update streams mutate database state and the database
  must be reloaded or restored before each run.

Rust rewrite translation:

```text
verification-loop:
  1. load initial graph fixture
  2. generate deterministic validation operations
  3. run single-threaded validation and persist expected outputs
  4. run candidate engine against the same operations
  5. only then run multi-threaded throughput and schedule-audit benchmarks
```

Why this matters:

- Verification-first Neo4j rewriting should separate semantic correctness from
  throughput tuning.
- Mutating graph workloads require fixture restore, snapshot restore, or
  deterministic replay before every run.
- Agents should not optimize benchmark numbers until validation artifacts pass.

### Pattern: Operation Result Generator And Failure Artifacts

Found in:

- `gitrefrepo/ldbc_snb_interactive_v1_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/ValidationParamsGenerator.java:31-149`
- `gitrefrepo/ldbc_snb_interactive_v1_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/DbValidationResult.java:58-230`
- `gitrefrepo/ldbc_snb_interactive_v2_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/ValidationParamsGenerator.java:30-164`

Evidence:

- The generator iterates time-mapped operations, asks the database for the
  operation handler, executes it, filters the operation/result pair, injects
  dependent operations, and emits validation parameters.
- Validation results classify missing handlers, unable-to-execute operations,
  incorrect results, and successful operations by operation type.
- Failed actual and expected results are serializable as JSON artifacts.

Rust rewrite translation:

```rust
enum ValidationFailure {
    MissingHandler(OperationKind),
    ExecutionError { op: OperationId, message: String },
    IncorrectResult { op: OperationId, expected: Value, actual: Value },
}
```

Why this matters:

- This is the shape of a verification harness that LLMs can use safely:
  operation enum, handler lookup, deterministic execution, failure taxonomy,
  and durable diff artifacts.
- A rewrite should make missing query/procedure handlers visible as a category,
  not just as generic panics.
- Agent coding tasks can be scoped by operation kind: implement `Query7` until
  its validation artifacts pass.

### Pattern: Time-Windowed Dynamic Graph Parameter Curation

Found in:

- `gitrefrepo/ldbc_snb_interactive_v2_driver-src/paramgen/README.md:1-68`
- `gitrefrepo/ldbc_snb_interactive_v2_driver-src/paramgen/path_selection.py:12-120`
- `gitrefrepo/ldbc_snb_interactive_v2_driver-src/paramgen/path_selection.py:190-330`

Evidence:

- Paramgen curates benchmark parameters so query variants have predictable
  performance distributions.
- `PathCuration` loads person and knows tables into a NetworKit graph, applies
  node/edge insertions and deletions through `GraphUpdater`, then checks
  4-hop path validity with bidirectional BFS over time windows.
- The curation loop records `useFrom` and `useUntil` validity windows for
  parameter pairs.

Rust rewrite translation:

```rust
struct ParameterValidity {
    person_1: PersonId,
    person_2: PersonId,
    valid_from_ms: i64,
    valid_until_ms: i64,
}
```

Why this matters:

- Benchmark parameters should be graph-state-aware, not random ids that become
  invalid after updates.
- Low-RAM graph algorithms need reproducible dynamic-graph workloads that
  include insertions, deletions, and path checks.
- The Rust rewrite can use this idea to build evolving fixture snapshots for
  Cypher path expansion and GDS algorithms.

### Pattern: Batched Update Stream Construction For Memory-Heavy Inputs

Found in:

- `gitrefrepo/ldbc_snb_interactive_v2_driver-src/scripts/create_update_streams.py:19-171`

Evidence:

- The script creates DuckDB views over parquet inputs.
- It uses smaller time windows for large Comment/Post-derived queries.
- Batched outputs are concatenated and sorted into final parquet update
  streams by `creationDate` or `deletionDate`.

Rust rewrite translation:

```text
large-update-stream-builder:
  - view source files without loading every row into RAM
  - execute time-windowed batches
  - write temporary sorted chunks
  - concatenate into deterministic final streams
```

Why this matters:

- Verification data generation itself can become the memory bottleneck.
- Batch the fixture builder with the same seriousness as the engine.
- Agents should preserve sorted, deterministic update streams so benchmark
  regressions are attributable to engine changes, not data-order drift.

### Pattern: Operation-Specific Query Store Adapter

Found in:

- `gitrefrepo/ldbc_snb_interactive_v2_impls-src/mssql/src/main/java/org/ldbcouncil/snb/impls/workloads/mssql/SQLServerDb.java:18-240`

Evidence:

- `SQLServerDb` initializes a `SQLServerQueryStore` from `queryDir`.
- Each operation-specific handler maps a typed operation to a query-store
  method such as `getQuery1`, `getQuery2`, or `getQuery3`.
- Each handler owns result conversion from `ResultSet` to typed LDBC result.

Rust rewrite translation:

```rust
trait OperationHandler {
    type Operation;
    type Output;

    fn query_text(&self, operation: &Self::Operation, store: &QueryStore) -> QueryText;
    fn decode_rows(&self, rows: RowStream<'_>) -> Result<Self::Output, QueryError>;
}
```

Why this matters:

- Keep workload operation taxonomy outside the engine core.
- Keep query templates and result decoding testable without a running engine.
- Future agents can implement one operation handler at a time and verify it
  against validation artifacts.

## Explicit Gaps

- I did not fully inspect every file in every repo in the requested slice.
- I did not run upstream benchmark suites or tests; this is source-pattern
  extraction, not upstream behavioral verification.
- I did not inspect the Spring Boot Java research corpus.
- I did not deeply inspect every MCP/tool benchmark repo. Direct source evidence
  came from `mcp-bench`, `LiveMCPBench`, and `lazy-tool`; other tool repos were
  enumerated or lightly searched.
- I did not inspect OpenObserve dashboard UI code.
- CodeGraphContext completed on `libcypher-parser-src` and earlier on focused
  Neo4j driver/GDS slices. CGC runs on `jemalloc-src` and the LDBC v2 driver
  were interrupted after hanging; those repos are supported by direct source
  reads instead.
- As of this gap closure, the eight repos previously marked
  `assigned_inventory_only` in `gitrefrepo-coverage-ledger.tsv` now have
  direct source or graph-tool evidence in this file.

Future expansion targets:

- Complete CodeGraphContext on `neo4j-testkit-src/boltstub_rs` after isolating
  the Rust stub from the larger testkit tree.
- Compare `boltstub_rs/src/*` directly against Python `boltstub`.
- Read `ToolSandbox` and `tau2-bench` for sandboxing and task reset patterns.
- Read LDBC SNB interactive driver scheduling and validation code.
- Add concrete crate skeletons for `graphdb-observability`, `graphdb-bench`,
  and `graphdb-testkit` once rewrite module boundaries are known.

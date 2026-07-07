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
- Completed focused Neo4j Go driver CGC scan:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616`
  - Source repo:
    `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-go-driver-src`
  - CGC `stats`: 1 repository, 219 files, 1,758 functions, 41 interfaces,
    222 structs, 13 modules.
  - CGC `find name UpdateBookmarks` found `neo4j/bookmarks.go:65`.
  - CGC `find name GetRoutingTable` found Bolt implementations in
    `neo4j/internal/bolt/bolt3.go`, `bolt4.go`, `bolt5.go`, and `bolt6.go`.
  - CGC `find name ExecuteRead` found `neo4j/session.go:402` and
    `neo4j/transaction_helpers.go:32`.
- Completed focused Neo4j Go driver codebase-memory scan:
  `/tmp/codex-code-intel/codebase-memory/neo4j-go-driver-src-20260706-235049`
  - `index_repository.json`: 3,034 nodes and 16,234 edges.
  - `get_graph_schema`: 1,224 `Method` nodes, 704 `Function` nodes, 4,736
    `CALLS` edges, 1,253 `TESTS` edges.
  - `search_graph` for `UpdateBookmarks` found the implementation method in
    `neo4j/bookmarks.go`.
- Completed focused Neo4j Python driver CGC scan:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-python-driver-src-20260707-063736`
  - Source repo:
    `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-python-driver-src`
  - CGC `stats`: 1 repository, 106 files, 2,292 functions, 150 classes,
    204 modules.
  - CGC `find name execute_write` found
    `src/neo4j/_async/work/session.py:702`.
  - CGC `find name update_routing_table` found
    `src/neo4j/_async/io/_pool.py:964`.
- Completed focused Neo4j Python driver codebase-memory scan:
  `/tmp/codex-code-intel/codebase-memory/neo4j-python-driver-src-20260707-063736`
  - `index_repository.json`: 6,866 nodes and 49,430 edges.
  - `get_graph_schema`: 2,364 `Method` nodes, 2,184 `Function` nodes,
    9,682 `CALLS` edges, 2,823 `TESTS` edges.
  - `search_graph` found async/sync `execute_write`, `update_routing_table`,
    and `update_bookmarks` methods.
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
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-go-driver-src`
- `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-python-driver-src`
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
HOME=/tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616/home cgc --database ladybugdb --path /tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616/ladybugdb.sqlite find name GetRoutingTable
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-go-driver-src/neo4j/session.go | sed -n '392,520p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-go-driver-src/neo4j/internal/retry/state.go | sed -n '1,160p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-go-driver-src/neo4j/internal/bolt/bolt4.go | sed -n '836,928p'
HOME=/tmp/codex-code-intel/codegraphcontext/neo4j-python-driver-src-20260707-063736/home cgc --database ladybugdb --path /tmp/codex-code-intel/codegraphcontext/neo4j-python-driver-src-20260707-063736/ladybugdb.sqlite find name update_routing_table
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-python-driver-src/src/neo4j/_async/work/session.py | sed -n '500,750p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-python-driver-src/src/neo4j/_async/io/_pool.py | sed -n '920,1095p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/'Neo4j family'/neo4j-python-driver-src/src/neo4j/_async/bookmark_manager.py | sed -n '1,220p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/ldbc_graphalytics_platforms_arcadedb-src/shared/bench_common.py | sed -n '1,190p'
nl -ba /Users/amuldotexe/Desktop/personal-repos-lane/accio-tools/ignore-references/git-ref-repo/lazy-tool/benchmark/README.md | sed -n '1,260p'
```

## Pattern Index

| Group | Patterns |
| --- | --- |
| Metrics/tracing | Prometheus edge registry, trace IDs, plan metrics, storage metrics, subscriber discipline |
| Structured logs | Binary-owned subscribers, benchmark job logs, script mismatch diagnostics |
| Benchmark harnesses | ASV history, cached setup, LDBC timed jobs, MCP agent metrics, search-first tool benchmarks |
| Integration testkit | Docker driver glue, scripted Bolt stubs, standalone/cluster fixtures, driver retry/routing verification |
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

### Pattern: Driver Retry Routing Verification Surface

Source:

- `neo4j-go-driver-src/neo4j/bookmarks.go:58-108`
- `neo4j-go-driver-src/neo4j/session.go:402-520`
- `neo4j-go-driver-src/neo4j/internal/retry/state.go:31-144`
- `neo4j-go-driver-src/neo4j/internal/bolt/bolt4.go:836-928`
- `neo4j-go-driver-src/neo4j/internal/bolt/bolt4.go:1018-1150`
- `neo4j-go-driver-src/neo4j/session_test.go:118-181`
- `neo4j-go-driver-src/neo4j/session_test.go:511-666`

Graph-tool evidence:

- CodeGraphContext:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-go-driver-src-20260706-234616`
  indexed 219 files and found:
  - `UpdateBookmarks` at `neo4j/bookmarks.go:65`;
  - `ExecuteRead` at `neo4j/session.go:402`;
  - `GetRoutingTable` across Bolt 3/4/5/6 implementations.
- codebase-memory:
  `/tmp/codex-code-intel/codebase-memory/neo4j-go-driver-src-20260706-235049`
  indexed 3,034 nodes and 16,234 edges; schema showed 1,224 methods,
  704 functions, and 1,253 test edges. Its `UpdateBookmarks` search found
  the concrete method in `neo4j/bookmarks.go`.

Observed design:

- Bookmarks are state, not strings passed casually through APIs.
  `bookmarkManager` keeps a set guarded by `sync.RWMutex`. `UpdateBookmarks`
  removes previous bookmarks, adds new bookmarks, and optionally notifies a
  consumer. `GetBookmarks` merges internally tracked bookmarks with externally
  supplied bookmarks.
- Managed transaction helpers are thin API wrappers. Generic `ExecuteRead`
  and `ExecuteWrite` delegate to session-level retry behavior instead of
  implementing retry policy at each call site.
- Session retry is a state machine. `runRetriable` constructs `retry.State`
  with max retry time, throttle, max dead connections, database name, and
  telemetry flags, then loops while `state.Continue(ctx)` permits another
  attempt.
- Retryability is not just "any error". `retry.State.Continue` rejects
  non-retryable errors, context cancellation, exceeded retry time, and too many
  dead connections. `IsRetryable` excludes pool timeout and commit-failed-dead
  cases even when connectivity errors are otherwise retryable.
- Routing-table retrieval is protocol-version dependent. Bolt 4 chooses among
  route messages, v4.3 route messages, or a system database Cypher fallback
  depending on minor version and impersonation support.
- Bolt stream handlers update connection-level bookmarks from pull/discard
  summaries and commit success. Protocol state and causal consistency are
  therefore linked.

Rust rewrite transfer:

- Model Bolt/client/session semantics as a separate verification surface from
  storage and query execution. The storage engine can be lower-RAM and
  algorithm-shaped, but client-visible behavior still needs protocol/session
  compatibility tests.
- Represent retry as an explicit `RetryState` struct with:
  `errs`, `started_at`, `max_retry_time`, `max_dead_connections`,
  `dead_errors`, `skip_sleep`, `telemetry_sent`, and `database_name`.
- Represent bookmarks as a concurrency-safe causal-state component rather
  than embedding bookmark strings in transaction code. In Rust this can be a
  small `BookmarkManager` around `Arc<RwLock<IndexSet<Bookmark>>>` or a
  single-owner actor if the session model becomes async.
- Split verification by layer:
  - public helper API: `execute_read` and `execute_write` delegate to one retry
    loop;
  - session retry: transient database errors retry until bounded;
  - non-retryable user/commit/pool errors do not retry;
  - routing: Bolt-version-specific routing table acquisition uses the right
    message or fallback;
  - stream handling: bookmark updates happen only from successful summaries or
    commit responses.

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

Why it matters for the Neo4j rewrite:

The rewrite can change storage layout, graph projection memory, and algorithm
execution internals, but application compatibility depends on this driver
surface remaining stable. These tests become the external verification loop
for Bolt/session behavior while lower layers are rewritten under it.

### Pattern: Async Driver Retry And Routing Refresh Discipline

Source:

- `neo4j-python-driver-src/src/neo4j/_async/work/session.py:500-750`
- `neo4j-python-driver-src/src/neo4j/_async/io/_pool.py:920-1095`
- `neo4j-python-driver-src/src/neo4j/_async/bookmark_manager.py:1-220`
- `neo4j-python-driver-src/src/neo4j/_async/driver.py:286-390`
- `neo4j-python-driver-src/src/neo4j/api.py:197-240`

Graph-tool evidence:

- CodeGraphContext:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-python-driver-src-20260707-063736`
  indexed 106 files and found:
  - `execute_write` at `src/neo4j/_async/work/session.py:702`;
  - `update_routing_table` at `src/neo4j/_async/io/_pool.py:964`.
- codebase-memory:
  `/tmp/codex-code-intel/codebase-memory/neo4j-python-driver-src-20260707-063736`
  indexed 6,866 nodes and 49,430 edges; schema showed 2,364 methods,
  2,184 functions, 9,682 `CALLS` edges, and 2,823 `TESTS` edges. Its searches
  found async/sync `execute_write`, `update_routing_table`, and
  `update_bookmarks`.

Observed design:

- Managed transaction functions are explicitly idempotent. The async session
  docs say the supplied transaction function can be invoked more than once, so
  callers must avoid hidden one-shot side effects inside retryable work.
- `_run_transaction` owns the retry loop. It opens a managed transaction,
  invokes user work, closes on exceptions, commits on success, disconnects on
  retryable driver/database errors, stops on non-retryable errors, and backs off
  with `retry_delay_generator`.
- Retry timing starts after the first failed attempt, not before the first
  attempt. That keeps the first try out of the retry budget and makes the
  budget about retries, not total call setup.
- Async routing refresh is protected by `refresh_lock`. It first tries the
  initial routing address when appropriate, then existing routers, then the
  initial address again when appropriate. All routing errors are accumulated
  and reported as one service-unavailable failure when no router succeeds.
- Bookmark update snapshots under a cooperative lock and calls the external
  consumer after leaving the lock. This prevents user callbacks from running
  while internal bookmark state is locked.

Rust rewrite transfer:

- Write transaction-function APIs as idempotent contracts. In Rust docs and
  executable specs, say plainly that closures passed to `execute_write` or
  `execute_read` may run multiple times.
- Keep retry mechanics centralized in `run_transaction_with_retry`, not spread
  across query, transaction, and driver convenience APIs.
- Model routing refresh as a serialized pool operation:
  `refresh_lock -> choose candidate routers -> fetch routing table -> update
  table -> callback -> return`. Do not let multiple refreshers stampede the
  routing table.
- Avoid invoking user callbacks while holding internal locks. Use a snapshot
  pattern for bookmark consumers, routing callbacks, metrics exporters, and
  extension hooks.

Executable-spec candidates:

```text
REQ-ASYNC-DRIVER-001
WHEN execute_write receives a retryable driver or database error
THEN the session SHALL disconnect the failed connection
AND SHALL retry with bounded exponential/jittered delay until success or retry
budget exhaustion.

REQ-ASYNC-DRIVER-002
WHEN the transaction callback raises a user exception
THEN the transaction SHALL close
AND the session SHALL NOT treat the user exception as retryable unless the
driver/database error type explicitly marks it retryable.

REQ-ROUTING-001
WHEN routing information is stale
THEN one refresh task SHALL hold the routing refresh lock
AND SHALL try candidate routers in deterministic stages
AND SHALL accumulate failed-router errors for final diagnosis.

REQ-BOOKMARK-001
WHEN bookmark state changes
THEN the manager SHALL update internal state under lock
AND SHALL call external bookmark consumers after releasing the lock.
```

Why it matters for the Neo4j rewrite:

The Rust rewrite will likely have async network handling and lower-level
storage concurrency. This pattern keeps external driver semantics stable while
preventing callback-under-lock bugs, retry stampedes, and routing-table races.
It is directly useful for a Bolt compatibility harness and for agent-written
tests that prove safe behavior before storage internals are optimized.

### Pattern: Java Driver Managed Transaction Retry Contract

Source:

- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Session.java:70-175`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/InternalSession.java:100-190`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/async/InternalAsyncSession.java:76-230`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/retry/ExponentialBackoffRetryLogic.java:1-420`
- `neo4j-java-driver-src/driver/src/test/java/org/neo4j/driver/integration/SessionIT.java:150-225`

Graph-tool evidence:

- CodeGraphContext:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-java-driver-src-20260707-065427`.
  The full index was interrupted after a long run, but the resulting DB was
  readable: stats reported 1 repository, 411 files, 2731 functions, 271
  classes, 114 interfaces, and 15 enums. `cgc find name
  ExponentialBackoffRetryLogic` found the retry class and constructors at
  `driver/src/main/java/org/neo4j/driver/internal/retry/ExponentialBackoffRetryLogic.java:45`,
  `:65`, `:73`, and `:90`.
- codebase-memory:
  `/tmp/codex-code-intel/codebase-memory/neo4j-java-driver-src-20260707-065427`
  indexed 12,870 nodes and 50,937 edges. Its schema showed 4,948 methods,
  1,611 classes, 917 files, 11,680 `CALLS` edges, 477 `TESTS` edges, and
  searches surfaced `executeWrite`, `ExponentialBackoffRetryLogic`,
  `Neo4jBookmarkManager`, `withMaxTransactionRetryTime`, and retry-focused
  integration tests.

Observed design:

- The public `Session` API states that managed `executeRead` and `executeWrite`
  callbacks run inside a single managed transaction with retry behavior. The
  API docs warn that user callbacks must not return live `Result` objects
  because those results are invalid outside the transaction scope.
- `InternalSession.transaction` executes synchronous managed work in the caller
  thread and intentionally avoids running retry sleeps on event-loop threads,
  because blocking an event loop can deadlock the driver waiting for itself to
  read network data.
- The synchronous facade begins a transaction, passes a delegating transaction
  context to user code, rejects returned `Result`, commits if the transaction is
  still open, and lets the centralized retry logic decide whether a failure is
  retryable.
- `InternalAsyncSession.transactionAsync` wraps both synchronous callback
  exceptions and failed futures into the same close/rollback path. It also
  rejects returned live `ResultCursor` values before committing.
- `ExponentialBackoffRetryLogic` uses a bounded retry budget, exponential
  delay, jitter, interrupted-sleep handling, retryable-error classification,
  and suppressed-error accumulation. It has separate synchronous, async, and
  reactive implementations while preserving one semantic contract.
- Integration tests verify that read/write transaction work retries until
  success, retries until failure when retry budget is exhausted, and invokes
  the callback the expected number of times.

Rust rewrite transfer:

- Treat managed transactions as a public compatibility contract, not merely a
  convenience helper. A Rust rewrite should expose one semantic model across
  sync facade, async facade, and reactive/streaming facade if those APIs exist.
- Define the user callback contract before implementing storage internals:
  callbacks may run more than once, must be idempotent, and must return owned
  values rather than live cursors tied to a soon-closing transaction.
- Keep retry classification narrow. In Rust this should become an enum like
  `RetryDecision::{Retryable(DatabaseTransient), Fatal(UserOrProtocolError)}` so
  agents cannot accidentally retry arbitrary panics or client errors.
- Preserve suppressed/attempt errors. A retry-exhausted error should include
  the final error plus previous retry errors for diagnosis.
- Do not block async executors. The synchronous facade can sleep a caller
  thread; async retry should schedule delayed work through the runtime.

Executable-spec candidates:

```text
REQ-JAVA-DRIVER-RETRY-001
WHEN execute_write receives a retryable transient database error from managed
work
THEN the session SHALL retry the whole managed transaction until the retry
budget is exhausted or the work succeeds
AND the callback SHALL be documented and tested as potentially invoked more
than once.

REQ-JAVA-DRIVER-RETRY-002
WHEN a managed transaction callback returns a live result cursor
THEN the session SHALL reject the return value before exposing it outside the
transaction scope
AND SHALL close or roll back the transaction through the same error path as
other user-work failures.

REQ-JAVA-DRIVER-RETRY-003
WHEN async managed work fails synchronously or through a failed future
THEN the driver SHALL normalize both failure modes into one close/rollback path
AND SHALL add rollback/close failures as suppressed context.

REQ-JAVA-DRIVER-RETRY-004
WHEN a retry delay is needed in async mode
THEN the driver SHALL schedule the retry on the async runtime
AND SHALL NOT block an event-loop thread.
```

Why it matters for the Neo4j rewrite:

The rewrite can make storage and graph algorithms radically more RAM-efficient,
but driver semantics become the outer verification loop. These source files say
the compatibility surface includes idempotent callback docs, retry budgets,
error aggregation, no-live-cursor returns, and executor-safe retry scheduling.
Future LLM coding agents should generate these tests before replacing lower
layers, because a faster graph store that breaks managed transaction semantics
is not a Neo4j-compatible rewrite.

### Pattern: Fetch Size Is A Client Memory And Backpressure Contract

Source:

- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/Config.java:700-760`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/SessionConfig.java:390-425`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/async/NetworkSession.java:180-230`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/cursor/ResultCursorImpl.java:240-285`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/cursor/ResultCursorImpl.java:536-560`
- `neo4j-java-driver-src/driver/src/main/java/org/neo4j/driver/internal/cursor/ResultCursorImpl.java:806-875`

Graph-tool evidence:

- CodeGraphContext Java-driver run:
  `/tmp/codex-code-intel/codegraphcontext/neo4j-java-driver-src-20260707-065427`
  found `NetworkSession` at
  `driver/src/main/java/org/neo4j/driver/internal/async/NetworkSession.java:94`
  and its constructor at `:123`.
- codebase-memory Java-driver run:
  `/tmp/codex-code-intel/codebase-memory/neo4j-java-driver-src-20260707-065427`
  found `withFetchSize`, `NetworkSession`, `ResultCursorImpl`, and related
  `pullAllFailureAsync`/`Messages.pull` call sites through graph and direct
  source search.

Observed design:

- Global `Config.withFetchSize` and per-session `SessionConfig.withFetchSize`
  expose record batch size as a user-level memory and latency knob. The docs
  say the default is 1000 records and `-1` disables backpressure by pulling all
  records at once.
- `NetworkSession.autoCommitRun` sends a Bolt `RUN` followed by `PULL` with the
  configured `fetchSize`. That means the driver shapes how many records the
  server is asked to stream per batch.
- `ResultCursorImpl.nextAsync` pulls another `fetchSize` batch only when the
  local record buffer is empty and a caller asks for the next record. It rejects
  overlapping cursor API calls with a client error, making cursor access
  sequential and easier to reason about.
- `ResultCursorImpl.listAsync` deliberately sends `PULL -1`, which materializes
  all remaining records. The API exposes the memory tradeoff: stream for bounded
  memory, list for convenience and full materialization.
- `onPullSummary` keeps streaming with `fetchSize` for pending `peek` or `next`
  calls, but switches to `PULL -1` when a list future is pending. The cursor
  state machine therefore maps API shape directly to memory behavior.

Rust rewrite transfer:

- Make result materialization explicit. A Rust query API should distinguish
  `next_record`, `stream_records`, and `collect_all` instead of hiding
  materialization behind a generic result accessor.
- Propagate fetch-size/backpressure from API config down to the protocol and
  execution layers. In an embedded Rust engine, the equivalent may be
  `operator_batch_size`, `record_page_size`, or `frontier_chunk_size`; it should
  still be visible in tests and metrics.
- Keep cursor APIs sequential unless there is a deliberate multi-consumer
  design. Sequential cursor state is less clever, but it prevents concurrent
  pulls from exploding memory or interleaving protocol state.
- Treat `collect_all` as an explicit RAM-risk API. Agent-written docs and specs
  should say that it may allocate proportional to result size.

Executable-spec candidates:

```text
REQ-RESULT-STREAM-001
WHEN a caller requests the next result record and the local cursor buffer is
empty
THEN the client SHALL request at most `fetch_size` additional records from the
server or execution engine.

REQ-RESULT-STREAM-002
WHEN a caller invokes collect_all/list
THEN the client MAY request all remaining records
AND the API documentation SHALL mark the call as materializing and
memory-proportional to result cardinality.

REQ-RESULT-STREAM-003
WHEN two cursor API calls overlap on the same cursor
THEN the client SHALL reject the second call or serialize it through an
explicit queue
AND SHALL NOT issue concurrent protocol pulls accidentally.
```

Why it matters for the Neo4j rewrite:

The user goal is lower RAM through storage aligned with algorithms, but result
delivery can erase those gains if every query materializes large row sets by
default. The Java driver shows a compatibility-friendly pattern: expose a
batch-size knob, wire it into protocol pulls, and make high-memory convenience
APIs visibly different from streaming APIs. The same design can guide Rust
operator pipelines, GDS result streams, and Bolt compatibility tests.

## Explicit Gaps

- I did not fully inspect every repo in the requested slice.
- I did not run upstream benchmark suites or tests; this is source-pattern
  extraction, not upstream behavioral verification.
- I did not fully inspect every `ldbc*` repository. Direct LDBC evidence came
  from Graphalytics ArcadeDB platform code and shared benchmark helper.
- I did not inspect the Spring Boot Java research corpus.
- I did not deeply inspect every MCP/tool benchmark repo. Direct source evidence
  came from `mcp-bench`, `LiveMCPBench`, and `lazy-tool`; other tool repos were
  enumerated or lightly searched.
- I did not inspect OpenObserve dashboard UI code.
- CodeGraphContext completed on the working repo, assigned `lazy-tool`, the
  focused `neo4j-go-driver-src` pass, and the focused
  `neo4j-python-driver-src` pass. A partial but readable
  `neo4j-java-driver-src` CGC index was used for symbol evidence after the full
  scan was interrupted. Neo4j testkit, `boltstub_rs`, and `tracing-src` CGC
  runs were incomplete and are documented as gaps rather than treated as
  evidence.

Future expansion targets:

- Complete CodeGraphContext on `neo4j-testkit-src/boltstub_rs` after isolating
  the Rust stub from the larger testkit tree.
- Compare `boltstub_rs/src/*` directly against Python `boltstub`.
- Read `ToolSandbox` and `tau2-bench` for sandboxing and task reset patterns.
- Read LDBC SNB interactive driver scheduling and validation code.
- Add concrete crate skeletons for `graphdb-observability`, `graphdb-bench`,
  and `graphdb-testkit` once rewrite module boundaries are known.

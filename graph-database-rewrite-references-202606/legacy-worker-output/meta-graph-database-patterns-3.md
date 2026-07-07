# Supermeta Graph Database Patterns 3: Query Planning, Execution, And Cypher Surface

Date: 2026-07-06

This encyclopedia slice restores the missing query/planner/execution coverage
for the Neo4j-in-Rust rewrite research corpus. It focuses on parsers, ASTs,
binders, logical plans, physical plans, vectorized execution, streaming/dataflow,
recursive graph expansion, result materialization, extension hooks, and agentic
guardrails.

Graph-intelligence tools were treated as navigation only. Claims below are based
on direct source paths from the local reference repositories.

## Repositories Inspected Or Cited

| repository | evidence role |
| --- | --- |
| `gitrefrepo/apache-datafusion-src` | Rust logical optimizer, physical planner, `ExecutionPlan` trait, async `RecordBatch` streams, pushdown protocol |
| `gitrefrepo/duckdb-src` | parser/planner/executor lifecycle, vectorized `DataChunk`, pipelines, recursive CTE execution, profiler hooks |
| `gitrefrepo/kuzu-src` | Cypher-like parser, transformer, binder, logical planner, graph pattern expansion, materialized and Arrow query results |
| `gitrefrepo/falkordb-src` | Cypher execution-plan operators, record pipeline, graph expansion, conditional variable-length traverse |
| `gitrefrepo/age-src` | Cypher grammar/parser/analyzer/planner embedded in PostgreSQL extension boundaries |
| `gitrefrepo/libcypher-parser-src` | compact C parser result API, errors, segments, AST node layout |
| `gitrefrepo/materialize-src` | dataflow lowering and join/arrangement concepts for incremental query execution |
| `gitrefrepo/risingwave-src` | streaming execution and fragment/executor concepts, cited as follow-up where not deeply read here |
| `gitrefrepo/redisgraph-src` | matrix-backed Cypher lineage and parser dependency, mostly cross-linked with FalkorDB |

## Evidence Commands

Representative direct-source commands used while repairing this slice:

```bash
rg -n "OptimizerRule|ExecutionPlan|push_down_filter|PhysicalPlanner" \
  gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/optimizer.rs \
  gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs \
  gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs

rg -n "ParseStatements|PhysicalPlanGenerator|QueryProfiler|DataChunk|Pipeline|RecursiveCTE" \
  gitrefrepo/duckdb-src/src/main/client_context.cpp \
  gitrefrepo/duckdb-src/src/common/types/data_chunk.cpp \
  gitrefrepo/duckdb-src/src/parallel/pipeline.cpp \
  gitrefrepo/duckdb-src/src/execution/operator/set/physical_recursive_cte.cpp

rg -n "Parser::|Transformer|Binder|LogicalPlan|Recursive|QueryResult|ArrowQueryResult" \
  gitrefrepo/kuzu-src/src/parser gitrefrepo/kuzu-src/src/binder \
  gitrefrepo/kuzu-src/src/planner gitrefrepo/kuzu-src/src/main/query_result*

rg -n "OpBase|ExecutionPlan|CondVarLenTraverse|ExpandInto|cypher_parse|cypher_createplan" \
  gitrefrepo/falkordb-src/src/execution_plan gitrefrepo/age-src/src/backend \
  gitrefrepo/libcypher-parser-src/lib/src
```

## Core Thesis

A Rust Neo4j rewrite should treat Cypher and GDS as an ABI, not as a bag of
algorithm functions. The reusable shape across DataFusion, DuckDB, Kuzu, AGE,
FalkorDB, and libcypher-parser is a staged pipeline:

```text
bytes
  -> parser with source spans and dialect/config
  -> AST or parsed statement tree
  -> semantic binder/analyzer with catalog and scope
  -> logical plan with graph-specific operators
  -> optimizer rule batches
  -> physical plan with memory, ordering, partitioning, and streaming properties
  -> execution streams/chunks/records
  -> result collector, Arrow stream, Bolt stream, mutation report, or GDS sidecar
```

For low RAM, the plan representation must preserve whether an operator can
stream, must materialize, can push filters/projections into graph scans, or must
spill. The query engine is therefore also a memory planner.

## Pattern 1: Dialect-Aware Parser Boundary

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/sql/src/parser.rs`
- `gitrefrepo/duckdb-src/src/main/client_context.cpp:738-764`
- `gitrefrepo/kuzu-src/src/parser/parser.cpp:21-49`
- `gitrefrepo/libcypher-parser-src/lib/src/parser_config.c:61-105`
- `gitrefrepo/age-src/src/backend/parser/cypher_gram.y`

### Evidence

DuckDB exposes `ClientContext::ParseStatements` and
`ParseStatementsInternal`, with parser timing integrated through
`QueryProfiler`. Kuzu wraps ANTLR parsing and then runs a `Transformer` over
`ku_Statements`. libcypher-parser has a parser configuration object with
initial position, initial ordinal, and error colorization controls.

### Transferable Pattern

Do not parse directly into executable plans. Keep a thin parser facade that owns:

- dialect/version;
- source offset/line-column policy;
- recursion and input size limits;
- error mode;
- optional shell/procedure fragments;
- query text provenance.

### Rust Translation

```rust
pub struct CypherParseRequest<'a> {
    pub text: &'a str,
    pub dialect: CypherDialect,
    pub max_recursion_depth: usize,
    pub max_tokens: usize,
    pub source_name: SourceName,
}

pub struct ParsedCypher {
    pub statements: Vec<ParsedStatement>,
    pub diagnostics: Vec<ParseDiagnostic>,
    pub spans: SourceMap,
}
```

### Why It Matters For Neo4j-In-Rust

Neo4j compatibility will require version-sensitive Cypher behavior, Bolt error
positions, shell fragments, procedure calls, and useful diagnostics. A parser
boundary keeps those choices testable without entangling the optimizer.

### Agent Guidance

Agents should never add grammar support by jumping straight into physical
operators. First add parser fixture tests and AST span snapshots.

## Pattern 2: Parse Result Owns Errors, Roots, And EOF State

### Where Found

- `gitrefrepo/libcypher-parser-src/lib/src/result.c:25-83`
- `gitrefrepo/libcypher-parser-src/lib/src/result.c:156-185`
- `gitrefrepo/libcypher-parser-src/lib/src/errors.c:33-68`
- `gitrefrepo/libcypher-parser-src/lib/src/segment.h:24-43`

### Evidence

libcypher-parser exposes result APIs for root count, root access, node count,
directive count, parse errors, EOF, AST printing, error offsets, and result
freeing. It also supports parse segments and error arrays.

### Transferable Pattern

The parse result is not just `Result<Ast, Error>`. It is a structured diagnostic
artifact.

### Rust Translation

```rust
pub struct ParseResult {
    pub roots: Vec<AstNodeId>,
    pub directives: Vec<Directive>,
    pub diagnostics: Vec<ParseDiagnostic>,
    pub reached_eof: bool,
    pub arena: AstArena,
}
```

### Memory Implication

Arena-owned ASTs avoid per-node refcounting and allow a whole parse result to be
dropped at once. For long-running sessions, never leak AST arenas into prepared
plan caches unless the cache has size limits.

## Pattern 3: Transformer Phase Between Grammar And Semantic AST

### Where Found

- `gitrefrepo/kuzu-src/src/parser/transformer.cpp:16-49`
- `gitrefrepo/kuzu-src/src/parser/transform/transform_graph_pattern.cpp:10-158`
- `gitrefrepo/kuzu-src/src/parser/transform/transform_expression.cpp`
- `gitrefrepo/age-src/src/backend/parser/cypher_transform_entity.c`

### Evidence

Kuzu parses with ANTLR, then transforms parser contexts into internal statement
and expression types. Graph pattern transformation recognizes nodes,
relationships, and recursive details including shortest/trail/acyclic variants.

### Transferable Pattern

Keep generated grammar trees out of the rest of the engine. Translate them into
an internal AST that has stable Rust enums, spans, and semantic names.

### Rust Translation

```rust
pub trait AstTransformer {
    fn transform_statement(&mut self, ctx: GrammarStatement) -> Result<ParsedStatement>;
    fn transform_graph_pattern(&mut self, ctx: GrammarPattern) -> Result<GraphPatternAst>;
}
```

### Why It Matters

This lets Knight Bus swap parser technology later, or support multiple Cypher
versions, without rewriting binders and planners.

## Pattern 4: Binder Owns Scope, Alias, Catalog, And Semantic Errors

### Where Found

- `gitrefrepo/kuzu-src/src/binder/bind/bind_projection_clause.cpp:49-134`
- `gitrefrepo/kuzu-src/src/binder/bind/bind_projection_clause.cpp:171-301`
- `gitrefrepo/age-src/src/backend/parser/cypher_parse_node.c:34-121`
- `gitrefrepo/age-src/src/backend/parser/cypher_parse_agg.c:78-545`

### Evidence

Kuzu's binder validates projection clauses, aliases, nested aggregates, order
by/skip/limit, and expression binding against `BinderScope`. AGE creates and
frees Cypher parse states, including default alias generation across nested
clause transformations.

### Transferable Pattern

Semantic analysis must be an explicit phase with its own state. It is not a
parser responsibility and not a physical-plan responsibility.

### Rust Translation

```rust
pub struct BinderContext<'cat> {
    pub catalog: &'cat dyn GraphCatalog,
    pub scope: BinderScope,
    pub parameters: ParameterTypes,
    pub feature_flags: CypherFeatureFlags,
}
```

### Testing Implication

Binder tests should cover invalid aliases, aggregate placement, variable reuse,
unknown labels/types/properties, parameter type errors, and GDS procedure config
shape errors.

## Pattern 5: Logical Plan Enum With Extension Nodes

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:145-237`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:243-272`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:4338-4618`
- `gitrefrepo/kuzu-src/src/planner/operator/logical_operator.cpp:158-163`

### Evidence

DataFusion exposes extension planner traits and tests extension nodes, including
schema mismatch and invariant failures. Kuzu logical operators expose recursive
update checks.

### Transferable Pattern

Logical plans should have core nodes plus extension nodes for graph-specific and
GDS-specific operations.

### Rust Translation

```rust
pub enum LogicalGraphPlan {
    NodeScan(NodeScan),
    Expand(Expand),
    Filter(Filter),
    Projection(Projection),
    Aggregation(Aggregation),
    ProcedureCall(ProcedureCall),
    GdsCall(GdsLogicalCall),
    Extension(Arc<dyn LogicalExtensionNode>),
}
```

### Why It Matters

Neo4j compatibility means the query engine must route Cypher, procedures, GDS
catalog calls, and internal system commands without turning the planner into a
giant match statement that cannot evolve.

## Pattern 6: Ordered Optimizer Rule Batches With Observability

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/optimizer.rs:83-325`
- `gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/optimizer.rs:588-715`
- `gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/push_down_filter.rs:761-794`
- `gitrefrepo/duckdb-src/src/main/query_profiler.cpp:157-245`

### Evidence

DataFusion models `OptimizerRule`, an ordered vector of optimizer rules, and
observer hooks around rule application. DuckDB's profiler tracks query phases
and can print optimizer output.

### Transferable Pattern

Optimization should be deterministic, observable, and testable rule-by-rule.

### Rust Translation

```rust
pub trait LogicalOptimizerRule {
    fn name(&self) -> &'static str;
    fn rewrite(&self, plan: LogicalGraphPlan, ctx: &OptimizerContext)
        -> Result<Transformed<LogicalGraphPlan>>;
}
```

### Agent Guidance

When an agent adds a rule, it must add before/after plan snapshots. Do not rely
on end-to-end query results to prove optimizer correctness.

## Pattern 7: Pushdown Returns Exact, Inexact, Or Unsupported

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs:520-654`
- `gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/push_down_filter.rs:547-794`
- `gitrefrepo/duckdb-src/src/planner/operator/logical_filter.cpp`

### Evidence

DataFusion physical plans expose filter/projection/sort pushdown hooks and
propagation results. The pushdown rule logs join predicate movements and
simplification counts.

### Transferable Pattern

Graph scan operators must say whether a filter was fully enforced, partially
enforced, or rejected.

### Rust Translation

```rust
pub enum PushdownResult<T> {
    Exact(T),
    Inexact { pushed: T, residual: Expr },
    Unsupported { residual: Expr },
}
```

### Low-RAM Implication

Exact label/type/property pushdown prevents loading huge row streams into heap
just to filter them later. Inexact pushdown still helps but must preserve a
residual predicate.

## Pattern 8: Physical Plan Carries Ordering, Partitioning, And Boundedness

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs:94-231`
- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs:738-822`
- `gitrefrepo/duckdb-src/src/parallel/pipeline.cpp:165-193`

### Evidence

DataFusion's `ExecutionPlan` trait exposes output properties, children,
partitioning, order preservation, reset state, and partition-increase hooks.
DuckDB pipelines know whether they are order-dependent and how to schedule.

### Transferable Pattern

A graph physical operator is not just `execute()`. It must declare memory and
streaming properties that let the scheduler avoid surprise materialization.

### Rust Translation

```rust
pub struct PhysicalProperties {
    pub output_order: Option<OrderingSpec>,
    pub partitioning: PartitioningSpec,
    pub boundedness: StreamBoundedness,
    pub preserves_input_order: bool,
    pub estimated_peak_bytes: u64,
}
```

## Pattern 9: Async RecordBatch Streams As Columnar Query Boundary

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs:269-455`
- `gitrefrepo/apache-arrow-rs-src` as the array/memory substrate cited by file 2
- `gitrefrepo/kuzu-src/src/main/query_result/arrow_query_result.cpp:14-72`

### Evidence

DataFusion `ExecutionPlan::execute` returns `SendableRecordBatchStream`. Kuzu
separates `ArrowQueryResult` from materialized tuple iteration and explicitly
rejects row-style `hasNext`/`getNext` on Arrow results.

### Transferable Pattern

For analytical/procedure outputs, expose Arrow/columnar batches internally even
if Bolt eventually serializes rows.

### Rust Translation

```rust
pub trait PhysicalGraphExec: Send + Sync {
    fn execute(&self, partition: usize, ctx: TaskContext)
        -> Result<SendableGraphBatchStream>;
}
```

### Memory Implication

Columnar batches let filters and projections operate without per-row object
allocation. The Bolt layer can row-encode at the edge.

## Pattern 10: Vectorized Chunks With Selection Vectors

### Where Found

- `gitrefrepo/duckdb-src/src/common/types/data_chunk.cpp:26-99`
- `gitrefrepo/duckdb-src/src/common/types/data_chunk.cpp:313-358`
- `gitrefrepo/duckdb-src/src/common/types/data_chunk.cpp:385-446`

### Evidence

DuckDB `DataChunk` initializes typed vectors, tracks cardinality, can reference,
move, copy, split, fuse, slice with selection vectors, flatten, hash, verify,
serialize, and deserialize.

### Transferable Pattern

Selection vectors are a low-RAM way to represent filtered rows without copying
the underlying columns.

### Rust Translation

```rust
pub struct GraphBatch {
    pub columns: Vec<ArrayRef>,
    pub selection: Option<SelectionVector>,
    pub cardinality: usize,
}
```

### Neo4j-Rust Use

Use selection vectors for MATCH filtering and GDS property filters. Do not
materialize filtered node IDs into new Vecs unless required by output order or
mutation semantics.

## Pattern 11: Pipeline Scheduler With Source, Operators, Sink

### Where Found

- `gitrefrepo/duckdb-src/src/parallel/pipeline.cpp:68-101`
- `gitrefrepo/duckdb-src/src/parallel/pipeline.cpp:136-219`
- `gitrefrepo/duckdb-src/src/parallel/pipeline.cpp:400-434`
- `gitrefrepo/duckdb-src/src/execution/operator/helper/physical_result_collector.cpp:57-81`

### Evidence

DuckDB pipelines schedule tasks, track max threads, dependencies, order
dependence, source/sink state, operator lists, and child pipelines. Result
collector creates a collection with the database buffer manager because results
can outlive the client context.

### Transferable Pattern

Graph query execution should be pipeline-based:

```text
source: node scan / index seek / graph projection stream
operators: expand, filter, project, aggregate
sink: stream rows, materialize, write properties, mutate projected graph
```

### Rust Translation

```rust
pub struct QueryPipeline {
    pub source: Arc<dyn SourceExec>,
    pub operators: Vec<Arc<dyn OperatorExec>>,
    pub sink: Arc<dyn SinkExec>,
    pub dependencies: Vec<PipelineId>,
}
```

## Pattern 12: Recursive Execution Uses Double Buffers And Reusable Executors

### Where Found

- `gitrefrepo/duckdb-src/src/execution/operator/set/physical_recursive_cte.cpp:27-120`
- `gitrefrepo/duckdb-src/src/execution/operator/set/physical_recursive_cte.cpp:131-181`
- `gitrefrepo/duckdb-src/src/execution/operator/set/physical_recursive_cte.cpp:236-362`
- `gitrefrepo/kuzu-src/src/parser/transform/transform_graph_pattern.cpp:97-158`

### Evidence

DuckDB recursive CTE state keeps current input/output tables, advances iteration
buffers, rebinds recursive scans, and caches pipeline executors for repeated
iterations. Kuzu recognizes recursive relationship details in graph patterns.

### Transferable Pattern

Variable-length path and recursive graph queries should be first-class recursive
plans with explicit frontier/input/output buffers.

### Rust Translation

```rust
pub struct RecursiveGraphState {
    pub current_frontier: FrontierBuffer,
    pub next_frontier: FrontierBuffer,
    pub visited: VisitedSet,
    pub executor_cache: Vec<PipelineExecutor>,
}
```

### Low-RAM Implication

Double buffers bound active frontier memory. Recursive plans must reject or spill
when path materialization would explode.

## Pattern 13: Graph Expansion As Dedicated Physical Operator

### Where Found

- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_expand_into.c:17-125`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_expand_into.c:130-232`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_conditional_traverse.h`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_cond_var_len_traverse.h`

### Evidence

FalkorDB implements expansion operators as `OpBase` nodes with init, consume,
reset, clone, and free callbacks. `ExpandInto` maps source and destination
aliases, modifies edge slots, consumes child records, and manages cloned records.

### Transferable Pattern

Graph expansion should not be hidden inside a generic join. It is a graph-aware
operator with adjacency access, alias slots, direction/type filters, and
cardinality limits.

### Rust Translation

```rust
pub struct ExpandExec {
    pub input: Arc<dyn PhysicalGraphExec>,
    pub src_slot: SlotId,
    pub dst_slot: SlotId,
    pub rel_slot: Option<SlotId>,
    pub direction: Direction,
    pub rel_type_filter: RelTypeFilter,
}
```

## Pattern 14: Record Pipeline For Cypher Rows

### Where Found

- `gitrefrepo/falkordb-src/src/execution_plan/record.c`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_empty_row.c:10-57`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_load_csv.c:169-399`

### Evidence

FalkorDB operators produce and consume `Record` objects. `EmptyRow` seeds a
single empty record; `LoadCSV` can consume from a child, clone child records,
modify aliases, and switch consume functions when depleted.

### Transferable Pattern

Cypher needs a row/slot pipeline for compatibility, even if the OLAP side uses
columnar batches. Build an adapter between slot rows and columnar batches.

### Rust Translation

```rust
pub struct RowRecord {
    pub slots: Box<[ValueRef]>,
}

pub trait RowOperator {
    fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<RowRecord>>;
}
```

### When Not To Use

Do not use per-row records for PageRank, WCC, GDS property transforms, or large
projection scans. Use columnar batches there.

## Pattern 15: Result Mode Is A Planning Choice

### Where Found

- `gitrefrepo/kuzu-src/src/main/query_result.cpp:13-127`
- `gitrefrepo/kuzu-src/src/main/query_result/materialized_query_result.cpp:14-93`
- `gitrefrepo/kuzu-src/src/main/query_result/arrow_query_result.cpp:14-72`
- `gitrefrepo/duckdb-src/src/execution/operator/helper/physical_result_collector.cpp:25-81`

### Evidence

Kuzu has materialized and Arrow query results. Arrow result rejects row-style
iteration APIs, while materialized result supports `hasNext` and `getNext`.
DuckDB selects result collectors based on order preservation and batch index.

### Transferable Pattern

The result mode should be explicit:

- streaming rows to Bolt;
- Arrow batches for GDS/internal analytics;
- materialized result for small browser/API queries;
- write/mutate side effects for GDS modes.

### Rust Translation

```rust
pub enum ResultMode {
    BoltRows,
    ArrowBatches,
    MaterializedTable,
    StatsOnly,
    MutateProjectedGraph,
    WriteBackToStore,
}
```

## Pattern 16: Query Profiler As Phase And Operator Backchannel

### Where Found

- `gitrefrepo/duckdb-src/src/main/query_profiler.cpp:157-245`
- `gitrefrepo/duckdb-src/src/main/query_profiler.cpp:281-330`
- `gitrefrepo/duckdb-src/src/main/query_profiler.cpp:388-490`
- `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/execution_plan.rs:458-491`

### Evidence

DuckDB tracks query start/end, bytes read/written, total memory allocated,
metric counters, timers, operator metrics, and rendering. DataFusion exposes
metrics and statistics on execution plans.

### Transferable Pattern

Every physical graph operator should expose metrics. Memory-first claims need
operator-level bytes, not just query duration.

### Rust Translation

```rust
pub struct OperatorMetrics {
    pub input_rows: u64,
    pub output_rows: u64,
    pub bytes_read: u64,
    pub peak_bytes: u64,
    pub elapsed_nanos: u64,
}
```

## Pattern 17: Physical Planner Checks Schema And Invariants

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:374-380`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:3102-3189`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:3924-3924`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:4716-4810`

### Evidence

DataFusion tests extension planners that create mismatched schemas and invariant
failures. The physical planner wraps physical optimizer rules with schema and
invariant checks.

### Transferable Pattern

Graph plans should fail fast if an optimizer or extension changes slot schema,
ordering promises, or memory budget metadata incorrectly.

### Rust Translation

```rust
pub trait PlanInvariantCheck {
    fn check_schema(&self, before: &Schema, after: &Schema) -> Result<()>;
    fn check_slots(&self, plan: &PhysicalPlan) -> Result<()>;
    fn check_memory_budget(&self, plan: &PhysicalPlan) -> Result<()>;
}
```

## Pattern 18: Extension Planner Delegation

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:145-237`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:2580-2594`
- `gitrefrepo/duckdb-src/src/main/client_context.cpp:1220-1237`
- `gitrefrepo/kuzu-src/src/parser/transform/transform_extension.cpp`

### Evidence

DataFusion exposes extension planner traits. DuckDB can register functions in
client context. Kuzu has parser transformation for extension statements.

### Transferable Pattern

GDS procedures, APOC-like procedures, and future plugins should be planned by a
delegation registry, not hardcoded in the parser.

### Rust Translation

```rust
pub trait ProcedurePlanner: Send + Sync {
    fn procedure_name(&self) -> ProcedureName;
    fn plan_call(&self, call: BoundProcedureCall, ctx: &PlannerContext)
        -> Result<LogicalGraphPlan>;
}
```

## Pattern 19: Query Context Owns Locks, Lifecycle, Interrupts, And Cleanup

### Where Found

- `gitrefrepo/duckdb-src/src/main/client_context.cpp:217-344`
- `gitrefrepo/duckdb-src/src/main/client_context.cpp:675-689`
- `gitrefrepo/duckdb-src/src/main/client_context.cpp:1168-1207`
- `gitrefrepo/kuzu-src/src/main/query_result.cpp:59-127`

### Evidence

DuckDB `ClientContext` owns context locks, begin/end query lifecycle, cleanup,
task waiting, interruption, transaction cancellation, and profiling toggles.
Kuzu query results check database lifecycle before use.

### Transferable Pattern

The Rust rewrite needs an explicit query lifecycle object. Do not scatter
interrupt checks, cleanup, or transaction invalidation across operators.

### Rust Translation

```rust
pub struct QueryLifecycle {
    pub query_id: QueryId,
    pub tx: TransactionHandle,
    pub cancellation: CancellationToken,
    pub memory_pool: QueryMemoryPool,
    pub metrics: QueryMetrics,
}
```

## Pattern 20: Materialization Boundary Must Be Deliberate

### Where Found

- `gitrefrepo/duckdb-src/src/execution/operator/helper/physical_result_collector.cpp:72-81`
- `gitrefrepo/kuzu-src/src/main/query_result/materialized_query_result.cpp:14-93`
- `gitrefrepo/kuzu-src/src/processor/operator/result_collector.cpp:89-90`

### Evidence

DuckDB result collectors allocate a collection with a database buffer manager
because results can outlive the client context. Kuzu materializes into a
factorized table for row-style result iteration.

### Transferable Pattern

Materialized results must have an owning memory/storage pool, not borrow
operator buffers.

### Low-RAM Implication

For large results, prefer streaming Bolt pull or Arrow stream. Materialize only
when required by ordering, client behavior, or procedure semantics.

## Pattern 21: AGE Shows Compatibility Via Host Planner Integration

### Where Found

- `gitrefrepo/age-src/src/backend/parser/cypher_parser.c`
- `gitrefrepo/age-src/src/backend/parser/cypher_analyze.c`
- `gitrefrepo/age-src/src/backend/optimizer/cypher_createplan.c`
- `gitrefrepo/age-src/src/backend/executor/cypher_create.c`
- `gitrefrepo/age-src/src/backend/executor/cypher_merge.c`

### Evidence

AGE splits Cypher parsing/analyzing/planning/execution across PostgreSQL
extension hooks and executor files.

### Transferable Pattern

Compatibility layers can map Cypher into a host planning/execution substrate.
For Knight Bus, the host substrate is not PostgreSQL but a Rust graph plan
model. The lesson is still useful: isolate Cypher semantics from physical
storage details.

## Pattern 22: Recursive Graph Queries Need Semantics Before Optimization

### Where Found

- `gitrefrepo/kuzu-src/src/parser/transform/transform_graph_pattern.cpp:97-158`
- `gitrefrepo/kuzu-src/test/test_files/recursive_join/semantic.test`
- `gitrefrepo/kuzu-src/benchmark/queries/ldbc-sf100/recursive_join/*.benchmark`
- `gitrefrepo/duckdb-src/src/execution/operator/set/physical_recursive_cte.cpp:210-362`

### Evidence

Kuzu has semantic tests and benchmarks for recursive joins, including dense
edge, sparse edge, bidirection, trail, and path probe workloads. DuckDB has
recursive CTE runtime machinery.

### Transferable Pattern

Before optimizing variable-length path execution, define exact semantics:

- walk/trail/acyclic;
- shortest path;
- min/max hop;
- relationship uniqueness;
- node uniqueness;
- path output shape;
- memory budget on path materialization.

## Pattern 23: Costing Must Account For Supernodes And Cartesian Products

### Where Found

- `gitrefrepo/falkordb-src/src/execution_plan/optimizations/cost_base_label_scan.c`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_cartesian_product.c`
- `gitrefrepo/falkordb-src/src/execution_plan/ops/op_value_hash_join.c`
- `gitrefrepo/kuzu-src/benchmark/queries/ldbc-sf100/recursive_join/dense_edge.benchmark`

### Evidence

FalkorDB has explicit operators for cartesian product, joins, and cost-based
label scan optimization. Kuzu benchmarks dense-edge recursive joins separately.

### Transferable Pattern

The planner must know when a node/label/type has dangerous degree or cardinality
and route to index seek, bounded expansion, hash join, or early rejection.

### Rust Translation

```rust
pub struct GraphStats {
    pub label_cardinality: HashMap<LabelId, u64>,
    pub rel_type_cardinality: HashMap<RelTypeId, u64>,
    pub degree_histograms: DegreeHistograms,
    pub top_degree_nodes: TopK<NodeId>,
}
```

## Pattern 24: Query Tests Must Exist At Every Stage

### Where Found

- `gitrefrepo/apache-datafusion-src/datafusion/optimizer/tests/optimizer_integration.rs`
- `gitrefrepo/apache-datafusion-src/datafusion/core/src/physical_planner.rs:3924-4810`
- `gitrefrepo/kuzu-src/test/test_files/recursive_join/*.test`
- `gitrefrepo/duckdb-src/test/sql/logging/physical_operator_logging.test_slow`
- `gitrefrepo/libcypher-parser-src` parser result and error APIs

### Transferable Pattern

Do not test Cypher only end-to-end. Add stage-level golden tests:

| stage | test shape |
| --- | --- |
| parser | AST and span snapshots |
| binder | variable scope, catalog, type, alias, aggregate errors |
| logical planner | plan tree snapshots and cost estimates |
| optimizer | before/after rule snapshots |
| physical planner | schema, slot, ordering, memory invariant checks |
| executor | row/batch output and cancellation behavior |
| result adapter | Bolt rows, Arrow batches, stats, mutate/write modes |

## Source, Inference, Speculation Ledger

| claim | type | evidence |
| --- | --- | --- |
| Parser, transformer, binder, optimizer, physical planner, and executor should be separate phases. | inference from source | DataFusion optimizer/physical planner; DuckDB client context; Kuzu parser/transformer/binder; AGE parser/analyzer/planner |
| Columnar/Arrow batches should be an internal analytical boundary even if Bolt streams rows externally. | inference from source | DataFusion `ExecutionPlan`, Kuzu `ArrowQueryResult`, DuckDB `DataChunk` |
| Graph expansion should be a dedicated physical operator. | sourced pattern plus inference | FalkorDB `op_expand_into`, conditional traversal operators, Kuzu graph pattern transformer |
| A Rust rewrite can preserve Neo4j-like Cypher semantics while using a different physical execution engine. | architectural bet | AGE demonstrates host-planner mapping; Knight Bus target still needs compatibility tests |
| Recursive path queries are a major memory risk and require explicit planning contracts. | inference from source | DuckDB recursive CTE buffers; Kuzu recursive join tests/benchmarks |

## Agentic Coding Guidance

Future agents should apply this slice as follows:

1. When adding syntax, update parser fixtures before touching binders.
2. When adding semantics, add binder scope/catalog/type tests before physical
   operators.
3. When adding optimizer rules, add before/after plan snapshots and rule metrics.
4. When adding graph expansion, expose memory estimates, degree-risk warnings,
   and path materialization limits.
5. When adding GDS or APOC-like procedure support, route through a procedure
   planner registry rather than parser special cases.
6. When adding result modes, decide whether the output is Bolt rows, Arrow
   batches, materialized table, stats, mutate, or write.
7. Never claim low RAM if the plan hides materialization in result collection,
   recursive path buffers, hash join state, or full graph projection loading.

## Explicit Gaps

- This replacement slice was reconstructed after a background worker overwrite.
  It is source-backed but shorter than the worker's reported 3069-line query
  artifact.
- DataFusion, DuckDB, Kuzu, FalkorDB, AGE, and libcypher-parser are cited
  directly. Materialize and RisingWave are included as follow-up targets rather
  than deeply analyzed here.
- Neo4j's own Cypher runtime and slotted/pipelined/parallel runtime internals
  still need a separate deep pass if full API compatibility is the bar.
- Bolt result streaming and driver backpressure are covered more in
  `supermeta-graph-database-patterns-5.md` than in this file.

# Graph Database Patterns 1: Neo4j Family Architecture And Compatibility Surface

Agent 1 scope: Neo4j-family architecture and compatibility surface for a future Neo4j-like graph database rewrite in Rust with lower RAM usage.

This is a first useful pass, not a final exhaustive survey. It is grounded in direct source reads from the local reference repos under:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family`

Repo paths below are relative to that `Neo4j family` directory unless explicitly marked absolute. Graph tools were used for navigation only; patterns below are based on direct source reads unless marked as a searched gap.

## Executive Takeaways

Neo4j's compatibility surface is not one thing. It is an ecosystem contract made of several smaller contracts that evolve at different speeds:

1. **Kernel procedure contract**: procedure signatures, injected context, runtime resource monitors, safe vs full access component registries, security context, transaction facets, and query-language support flags.
2. **Cypher contract**: grammar and AST stability, parser compatibility, logical plan cache keys, runtime compilation boundaries, execution resource tracking, and query type classification.
3. **Bolt contract**: protocol version negotiation, feature flags, PackStream versions, message registries, finite-state-machine transitions, failure/interruption semantics, and driver conformance through Testkit.
4. **GDS contract**: procedure stubs, per-request facades, graph catalog identity, user/database scoping, projection and estimate APIs, memory-estimation objects, termination flags, progress tracking, and compact graph representations.
5. **Client ergonomics contract**: typed DSLs, shells, browsers, OGM layers, drivers, and GDS clients all encode compatibility assumptions. A Rust rewrite should treat these as first-class acceptance tests, not post-launch polish.

For a low-RAM Rust rewrite, the largest design lesson is to keep compatibility surfaces stable while making internal representations aggressively compact:

- Use small stable trait boundaries at the kernel/procedure/Cypher/Bolt edges.
- Put compatibility metadata in immutable signatures and protocol registries, not scattered conditionals.
- Keep per-query/per-request state separate from global registries.
- Intern strings, symbols, labels, relationship types, database names, procedure names, and user IDs.
- Prefer arena-backed or slice-backed AST/plan data over deep object graphs.
- Give every long-running operation a cancellation, memory accounting, and observability hook.
- Test compatibility with protocol scripts and cross-driver behavior before optimizing internals.

## Pattern 1: Procedure Registry Snapshot Boundary

### Where Found

- Language/framework: Java, Neo4j kernel/procedure framework.
- Repos and files:
  - `neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/api/procedure/GlobalProcedures.java`
  - `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/GlobalProceduresRegistry.java`
  - `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/procs/ProcedureSignature.java`

### What The Code Does

`GlobalProcedures` is a small kernel-facing interface. It exposes methods to register:

- callable procedures,
- scalar functions,
- aggregation functions,
- annotated procedure classes,
- injected components,
- type mappings,
- and to fetch the current `ProcedureView`.

`GlobalProceduresRegistry` is the implementation. It owns mutable registries, component registries, compilers, and an `AtomicReference<ProcedureView>` for the current snapshot. Updates are coordinated through an updater lock; readers see a stable view.

Short pseudocode:

```text
GlobalProceduresRegistry
  mutable registry
  safe component registry
  all component registry
  compiler
  restricted compiler
  AtomicReference<ProcedureView> current_view

register(procedure):
  with updater:
    add to mutable registry
    current_view = snapshot(registry)

procedure_view():
  return current_view.load()
```

`ProcedureSignature` is the stable descriptor: qualified name, input/output fields, mode, admin/system/internal flags, deprecation, eager flag, thread-safety, supported Cypher languages, and unsupported database types.

### Why It Matters For Neo4j-In-Rust

Procedures are extension points. Once exposed, they become compatibility contracts across APOC, GDS, drivers, browser, scripts, documentation, and user code. A Rust rewrite needs a stable procedure ABI even if internal storage, planning, memory layout, and execution change.

The snapshot pattern matters because procedure registration is mutable and operationally sensitive, but query execution needs read-mostly stable lookup. Rust can encode this more cheaply and safely than Java with `ArcSwap`, `Arc<ProcedureCatalog>`, or epoch-style immutable catalogs.

### When To Use

Use this pattern when:

- procedures/functions can be installed, reloaded, or registered programmatically;
- query execution needs lock-free or near-lock-free lookup;
- multiple subsystems need a consistent view of the procedure namespace;
- compatibility metadata must be queryable without loading extension classes.

Avoid this pattern when:

- the database is embedded and has a fixed built-in procedure set;
- extension loading is disabled by product design;
- a simple immutable catalog loaded at startup is enough.

### Rust Translation

Represent the procedure catalog as an immutable value behind an atomically swapped pointer:

```rust
pub struct ProcedureCatalog {
    procedures: Arc<[ProcedureEntry]>,
    by_name: HashMap<QualifiedNameId, ProcedureId>,
    functions: Arc<[FunctionEntry]>,
}

pub struct GlobalProcedures {
    current: arc_swap::ArcSwap<ProcedureCatalog>,
    compiler: ProcedureCompiler,
}

impl GlobalProcedures {
    pub fn view(&self) -> Arc<ProcedureCatalog> {
        self.current.load_full()
    }

    pub fn register(&self, proc: CompiledProcedure) -> Result<(), ProcedureError> {
        let old = self.current.load_full();
        let next = old.with_added_procedure(proc)?;
        self.current.store(Arc::new(next));
        Ok(())
    }
}
```

Use stable IDs internally:

- `ProcedureId(u32)`
- `FunctionId(u32)`
- `QualifiedNameId(u32)`
- `FieldNameId(u32)`
- `TypeId(u16)`

Intern names once. Do not store full strings in every signature, plan, frame, and error object.

### Risks

- Snapshot catalogs can hide update ordering bugs. Tests must verify reload, duplicate names, removed procedures, and old running queries.
- If snapshots clone large maps, reload becomes memory-heavy. Use persistent maps, compact arrays, or copy-on-write deltas.
- Procedure signatures can become compatibility dumping grounds. Keep the struct stable but avoid putting runtime state inside it.

### Memory Implications

Java object-heavy signatures allocate lists, field objects, names, optionals, annotations, and flags. A Rust rewrite should compact this:

- store fields in `Arc<[FieldSpec]>`;
- store names and namespace segments as interned symbols;
- use bitflags for `mode`, `admin`, `system`, `internal`, `thread_safe`, `eager`, and language support;
- store deprecation metadata out-of-line only when present.

### Concurrency Implications

Reads should not take a global write lock. Writes can be serialized because procedure registration and reload are rare compared with query execution.

Use:

- `ArcSwap` for current view,
- a `Mutex` for registration mutation,
- generation numbers for observability and stale-cache detection.

### Testing Implications

Tests should cover:

- duplicate registration rejection;
- snapshot consistency while reload happens;
- safe vs full-access registration;
- query execution with a stale snapshot;
- signature preservation across reload;
- supported Cypher-version filtering.

### How Future Agents Should Apply It

When designing Rust procedure support, start from the catalog API and signature shape before implementing procedure execution. Treat procedure signatures as serialized compatibility artifacts that clients, docs, and tests can inspect.

## Pattern 2: Safe And Full-Access Component Injection

### Where Found

- Language/framework: Java reflection and Neo4j procedure compiler.
- Repos and files:
  - `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/ComponentRegistry.java`
  - `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/FieldInjections.java`
  - `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/ProcedureCompiler.java`

### What The Code Does

`ComponentRegistry` maps injectable Java classes to provider functions. Each provider receives a procedure `Context` and returns a component.

`FieldInjections` walks a procedure class hierarchy and validates fields:

- field must have `@Context`;
- field must be public;
- field must not be final;
- field type must have a registered provider.

`ProcedureCompiler` maintains two field-injection systems:

- safe components for normal procedures;
- all components for full-access procedures.

When a procedure requires full access but is only allowed safe access, Neo4j can compile a failed-load placeholder instead of crashing the database.

Short pseudocode:

```text
compile(procedure_class):
  signature = read_annotations(procedure_class)
  if procedure_has_full_access:
    injector = all_components
  else:
    injector = safe_components

  fields = injector.validate_and_build_setters(procedure_class)
  return callable_procedure(signature, constructor, fields)
```

### Why It Matters For Neo4j-In-Rust

Neo4j procedures expect rich context: transaction, database service, security, logging, termination, graph catalog, task registry, configuration, and memory tracking. Rust should not expose one giant mutable context object everywhere. Instead, use explicit capability injection.

This pattern gives a Rust rewrite a way to separate:

- procedures that are safe for normal users,
- procedures that need privileged internals,
- procedures that are unavailable because a capability is missing,
- procedures that are visible but fail with a useful error.

### When To Use

Use this pattern when:

- extension procedures need access to controlled database capabilities;
- security and operational mode affect which capabilities are exposed;
- the product supports third-party procedure packs;
- missing components should become load-time errors, not runtime panics.

Avoid this pattern when:

- all procedures are first-party and compiled into the binary;
- the capability set is tiny enough to pass as explicit function parameters;
- dynamic extension loading is out of scope.

### Rust Translation

Prefer typed capability bundles over reflection:

```rust
pub struct ProcedureContext<'tx> {
    pub tx: &'tx dyn KernelTransaction,
    pub security: SecurityContext,
    pub memory: &'tx dyn MemoryTracker,
    pub termination: &'tx dyn TerminationFlag,
    pub log: &'tx dyn ProcedureLog,
}

pub trait Procedure: Send + Sync {
    fn signature(&self) -> &ProcedureSignature;

    fn call(
        &self,
        ctx: ProcedureContext<'_>,
        input: &[Value],
        out: &mut dyn RowSink,
    ) -> Result<(), ProcedureError>;
}

pub struct CapabilitySet {
    pub safe: ProcedureContextFactory,
    pub full_access: Option<ProcedureContextFactory>,
}
```

For dynamically loaded Rust extensions, prefer a narrow C ABI or WASM capability import table rather than passing raw internal pointers.

### Risks

- A giant context can become an internal API leak.
- Full-access procedures can bypass memory accounting, locks, security checks, and transaction invariants.
- Reflection-style injection hides dependency shape. Rust should make dependencies visible in types or explicit registration metadata.

### Memory Implications

Avoid per-call allocation of provider maps and boxed components. Build compact factories at registration time. At call time, pass references to existing transaction-scoped services.

### Concurrency Implications

Procedure objects can be shared, but call contexts are transaction-scoped. Use `Send + Sync` only for procedure definitions, not for every context object. A procedure signature's `thread_safe` flag should influence runtime scheduling.

### Testing Implications

Test:

- safe procedure cannot receive full-access capability;
- full-access procedure fails predictably when not allowlisted;
- missing capability produces a load error;
- capability objects are transaction-local where needed;
- procedure cannot outlive the transaction context.

### How Future Agents Should Apply It

Model procedure execution as `ProcedureDefinition + ProcedureContext + ResourceMonitor`. Do not let future Rust designs pass a database singleton into every procedure.

## Pattern 3: Signature-First Compatibility Metadata

### Where Found

- Language/framework: Java kernel API.
- Repos and files:
  - `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/procs/ProcedureSignature.java`
  - `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/ProcedureCompiler.java`
  - `neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`

### What The Code Does

Neo4j compiles procedure annotations into signatures that carry compatibility metadata:

- procedure mode (`READ`, `WRITE`, `DBMS`, etc.);
- admin/system/internal flags;
- deprecation and replacement information;
- warning metadata;
- eager execution flag;
- thread-safety;
- supported Cypher versions;
- unsupported database types;
- input and output field definitions.

GDS procedure classes such as `GraphProjectProc` expose procedure annotations and then delegate actual work to a facade. The signature is visible at the database boundary, while implementation logic is elsewhere.

### Why It Matters For Neo4j-In-Rust

Compatibility cannot be inferred from implementation code. Drivers, shell, browser, documentation, procedure listing, security, and query planning all need structured metadata.

For a Rust rewrite, the signature should be the canonical contract:

- it tells Cypher how to type-check procedure calls;
- it tells security whether a call is allowed;
- it tells compatibility tests what exists;
- it tells clients which outputs to expect;
- it tells memory and execution systems whether streaming or eager behavior is required.

### When To Use

Use this pattern for all public procedures, functions, and extension APIs.

Avoid ad hoc metadata for:

- built-in functions;
- APOC-like extensions;
- GDS procedures;
- admin procedures;
- internal procedures exposed through Cypher.

They should all compile to the same signature model.

### Rust Translation

Use a data-first signature that can be serialized:

```rust
bitflags::bitflags! {
    pub struct ProcedureFlags: u32 {
        const ADMIN = 1 << 0;
        const SYSTEM = 1 << 1;
        const INTERNAL = 1 << 2;
        const EAGER = 1 << 3;
        const THREAD_SAFE = 1 << 4;
        const DEPRECATED = 1 << 5;
    }
}

pub struct ProcedureSignature {
    pub name: QualifiedNameId,
    pub mode: ProcedureMode,
    pub flags: ProcedureFlags,
    pub inputs: Box<[FieldSpec]>,
    pub outputs: Box<[FieldSpec]>,
    pub supported_cypher: CypherVersionMask,
    pub unsupported_database: DatabaseKindMask,
    pub deprecation: Option<DeprecationInfoId>,
}
```

Use `Box<[T]>` or `Arc<[T]>` instead of `Vec<T>` when signatures are immutable after compilation.

### Risks

- Too many flags can encode product policy in the wrong layer.
- If signatures are not stable across versions, client conformance becomes brittle.
- If metadata is not queryable, browser and shell experiences degrade.

### Memory Implications

Thousands of procedures and functions can be represented compactly if names and types are interned. Store rare metadata, such as deprecation warning text, in side tables.

### Concurrency Implications

Signatures should be immutable and shareable across threads.

### Testing Implications

Snapshot procedure signature output across versions. This is especially important for APOC and GDS because users script against procedure names and column names.

### How Future Agents Should Apply It

When extracting APOC, GDS, and kernel procedure compatibility, capture signature shape first. Implementation can change; names, modes, fields, warnings, and deprecation behavior are the compatibility surface.

## Pattern 4: Transaction Facets And User Context

### Where Found

- Language/framework: Java kernel API and Bolt transaction manager.
- Repos and files:
  - `neo4j-src/community/kernel-api/src/main/java/org/neo4j/kernel/api/KernelTransaction.java`
  - `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/security/LoginContext.java`
  - `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/security/SecurityContext.java`
  - `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/tx/TransactionManager.java`
  - `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/tx/TransactionManagerImpl.java`

### What The Code Does

`KernelTransaction` is not a single "do everything" interface. It exposes facets:

- data reads and writes;
- token reads and writes;
- schema reads and writes;
- lock access;
- cursor factory;
- procedure calls;
- commit, rollback, close, and termination;
- metadata and transaction type.

`LoginContext` represents the authenticated subject and creates a `SecurityContext` for a specific database and mode. `SecurityContext` then controls capabilities, admin checks, password-change restrictions, impersonation, and database access mode.

Bolt's `TransactionManagerImpl` maps client requests into kernel transactions with:

- connection owner,
- login context,
- database name/default database,
- access mode,
- bookmarks,
- timeout,
- metadata,
- notifications configuration,
- routing context,
- explicit vs implicit transaction type.

Short pseudocode:

```text
begin_bolt_transaction(request):
  db = request.database_or_default()
  login = connection.owner.login_context
  mode = request.access_mode
  bookmarks = request.bookmarks
  tx = database_service.begin_transaction(login, db, mode, bookmarks, timeout, metadata)
  id = "bolt-" + next_id()
  transaction_map[id] = tx
```

### Why It Matters For Neo4j-In-Rust

Transaction context is the hidden compatibility surface behind procedures, Cypher, Bolt, and drivers. If a Rust rewrite models transactions as a generic mutable database handle, it will struggle to enforce security, memory limits, lock discipline, and procedure capability boundaries.

The facet pattern also helps reduce RAM: each query path can borrow only the facets it needs instead of carrying a large object graph.

### When To Use

Use this pattern when:

- multiple execution surfaces share the same transaction kernel;
- read/write/schema/token/locks/procedures have different permissions;
- security checks depend on database, user, mode, and impersonation;
- Bolt and embedded APIs both begin transactions.

Avoid this pattern for tiny embedded graph libraries where one transaction type is sufficient and security is external.

### Rust Translation

Represent transaction capabilities as small traits:

```rust
pub trait DataRead {
    fn node_labels(&self, node: NodeId, out: &mut LabelSink) -> Result<(), KernelError>;
}

pub trait DataWrite {
    fn create_node(&mut self, labels: &[LabelId]) -> Result<NodeId, KernelError>;
}

pub trait ProcedureRead {
    fn call_procedure(
        &mut self,
        id: ProcedureId,
        args: &[Value],
        out: &mut dyn RowSink,
    ) -> Result<(), KernelError>;
}

pub trait KernelTransaction:
    DataRead + DataWrite + TokenRead + TokenWrite + SchemaRead + SchemaWrite + Locks
{
    fn security(&self) -> &SecurityContext;
    fn memory(&self) -> &dyn MemoryTracker;
    fn terminate(&self, reason: TerminationReason);
}
```

For lower RAM, keep per-transaction state in a compact arena:

- transaction-local token cache;
- cursor pool;
- lock table references;
- memory tracker counter;
- security context ID or borrowed security context;
- change set encoded as append-only records.

### Risks

- Too many tiny traits can make generic code noisy. Use object-safe trait bundles at subsystem boundaries.
- Security context must be immutable or very carefully scoped.
- Bolt transaction maps can leak memory if cleanup listeners are unreliable.

### Memory Implications

Transaction maps should store compact transaction handles, not heavyweight session state. Long-lived transaction metadata should be interned or stored in side tables.

### Concurrency Implications

Explicit transactions are connection-owned but may have async execution tasks. Use ownership tokens or state machines to prevent concurrent misuse.

### Testing Implications

Test:

- read-only transaction rejects writes;
- procedure access respects user and mode;
- impersonation affects authorization but not authentication subject identity;
- Bolt transaction cleanup removes map entries on close;
- default database resolution works the same through Bolt and embedded execution.

### How Future Agents Should Apply It

When studying drivers and server protocol code, always map client-facing session/transaction options back to kernel transaction fields. Those fields define the minimum Rust transaction API.

## Pattern 5: Compiler, Planner, Runtime, And Cache-Key Split

### Where Found

- Language/framework: Scala, Neo4j Cypher compiler.
- Repos and files:
  - `neo4j-src/community/cypher/cypher/src/main/scala/org/neo4j/cypher/internal/CypherCurrentCompiler.scala`

### What The Code Does

`CypherCurrentCompiler` composes:

- parser/pre-parser input;
- planner;
- runtime;
- runtime context manager;
- monitors;
- query caches;
- schema, procedure, and statistics access;
- logical-plan attributes;
- execution plan key creation.

The compile path:

1. Parses or accepts an already parsed query.
2. Plans or parses-and-plans.
3. Builds an execution-plan cache key from query text/options, logical plan, and reduced planning attributes.
4. Uses `computeIfAbsent` on the execution plan cache.
5. Builds a runtime context from transaction-facing resources.
6. Compiles a logical query into an executable query.
7. Classifies query type for read/write/schema/procedure/admin behavior.

Short pseudocode:

```text
compile(query, tracer, tx):
  parsed = query.parsed_or_preparsed()
  logical_plan = planner.plan(parsed, tx.schema, tx.procedures)
  attrs_for_key = reduce(planning_attributes)
  cache_key = (query.options, logical_plan, attrs_for_key)

  executable = execution_cache.compute_if_absent(cache_key):
    runtime_ctx = make_runtime_context(tx, schema, procedures, monitors)
    logical_query = make_logical_query(logical_plan, attrs_for_key)
    runtime.compile(logical_query, runtime_ctx)

  return executable_query(executable, query_type(logical_plan))
```

One important memory detail: the compiler copies/reduces planning attributes before putting them into the cache key so runtime rewrites do not mutate cache-owned state.

### Why It Matters For Neo4j-In-Rust

Cypher is where compatibility and performance collide. A low-RAM Rust rewrite should not create a monolithic "compile query" object that retains AST, semantic tables, logical plans, statistics, runtime operators, debug strings, and transaction state forever.

The split gives a useful architecture:

- Parser output is short-lived unless cached.
- Logical plans can be canonicalized and cached.
- Runtime executable plans can be cached separately.
- Transaction resources are injected only at execution.
- Planning attributes are minimized before becoming cache keys.

### When To Use

Use this pattern for any serious Cypher implementation.

Avoid implementing Cypher execution by interpreting raw AST directly except for a prototype. Direct AST interpretation makes planning, caching, memory accounting, and compatibility much harder.

### Rust Translation

Use separate arenas and lifetimes per phase:

```rust
pub struct ParseArena { /* AST nodes */ }
pub struct PlanArena { /* logical operators */ }
pub struct RuntimePlan { ops: Arc<[PhysicalOp]> }

pub struct Compiler {
    parser: CypherParser,
    planner: LogicalPlanner,
    runtime: PhysicalRuntime,
    plan_cache: moka::sync::Cache<PlanCacheKey, Arc<RuntimePlan>>,
}

pub fn compile(
    &self,
    query: &str,
    tx_caps: &PlanningCapabilities,
) -> Result<Arc<RuntimePlan>, CompileError> {
    let parsed = self.parser.parse(query)?;
    let logical = self.planner.plan(&parsed, tx_caps)?;
    let key = logical.reduced_cache_key();
    self.plan_cache.get_with_by_ref(&key, || {
        Arc::new(self.runtime.compile(&logical))
    })
}
```

For lower RAM:

- store AST nodes in an arena and drop it after planning if not needed;
- intern identifiers and property keys;
- keep cache keys compact and canonical;
- store optional debug strings behind feature flags or lazy generation;
- separate logical-plan cache from executable cache.

### Risks

- Cache keys that omit semantic dependencies can reuse wrong plans.
- Cache keys that include too much state waste memory and reduce hit rate.
- Runtime rewrites must not mutate shared logical plan state.
- Query type classification must be conservative or security can be bypassed.

### Memory Implications

Do not retain:

- full token streams after parsing;
- ANTLR parse trees after AST construction;
- semantic analysis tables in executable plans unless needed;
- debug plan strings in hot caches.

Retain:

- compact canonical plan shape;
- operator IDs;
- slots/register layout;
- symbol IDs;
- cardinality/selectivity summaries if needed for invalidation.

### Concurrency Implications

The cache should be thread-safe. Runtime plan objects should be immutable and shared. Execution state should be allocated per query execution.

### Testing Implications

Test:

- cache hit/miss behavior across query options;
- plan invalidation when schema/index/procedure signatures change;
- mutation safety if physical compilation rewrites attributes;
- read/write/admin/procedure query classification;
- failure diagnostics include enough plan info without retaining huge strings.

### How Future Agents Should Apply It

When mining Neo4j Cypher internals, separate findings by phase: parse, semantic analysis, logical planning, physical planning, runtime execution, cache invalidation. Rust design should have explicit structs for each phase.

## Pattern 6: Parser Memory Discipline With Child-Freeing

### Where Found

- Language/framework: Scala, ANTLR parser integration.
- Repos and files:
  - `neo4j-src/community/cypher/front-end/parser/v5/ast-factory/src/main/scala/org/neo4j/cypher/internal/parser/v5/ast/factory/Cypher5AstParser.scala`

### What The Code Does

`Cypher5AstParser` constructs Neo4j AST nodes from a generated Cypher parser. It creates lexer/parser state, sets error listeners, parses statements or expressions, then builds AST.

The notable memory-oriented method is `isSafeToFreeChildren`. It returns false for selected grammar contexts whose children are needed later, such as procedure names and procedure arguments, and true for most other contexts. This lets the parser free parse-tree children earlier.

Short pseudocode:

```text
parse(input):
  lexer = make_lexer(input)
  parser = make_parser(lexer)
  ast_builder = Neo4jASTBuilder(factory)
  parser.set_ast_builder(ast_builder)
  tree = parser.statements()
  return ast_builder.result()

is_safe_to_free_children(rule):
  if rule in [procedure_name, procedure_argument, ...]:
    return false
  return true
```

### Why It Matters For Neo4j-In-Rust

Cypher queries can be large, and parser trees are often much larger than the final AST. A low-RAM rewrite should make parse-tree retention a deliberate choice.

Rust makes this easier if the parser directly builds a compact AST in an arena. If using a generated parser that creates a concrete parse tree, add an explicit disposal/drop strategy.

### When To Use

Use this pattern when:

- generated parser trees are large;
- the final AST is much smaller than the parse tree;
- parser contexts can be safely discarded as AST nodes are built;
- query parsing happens under memory limits.

Avoid child-freeing when:

- diagnostics need rich parse tree context;
- incremental parsing or IDE features are in scope;
- parser library lifetimes make early freeing unsafe.

### Rust Translation

Prefer event-based or arena AST construction:

```rust
pub struct AstBuilder<'a> {
    arena: &'a bumpalo::Bump,
    nodes: Vec<AstNodeId>,
}

pub fn parse_statement(input: &str, arena: &Bump) -> Result<StatementId, ParseError> {
    let tokens = TokenStream::new(input);
    let mut builder = AstBuilder::new(arena);
    cypher_parser::parse_with_events(tokens, |event| {
        builder.consume(event)
    })?;
    Ok(builder.finish_statement())
}
```

If a parse tree is unavoidable, keep a rule whitelist/blacklist for contexts that must survive until AST construction finishes.

### Risks

- Freeing children too early can break semantic construction in rare grammar contexts.
- Poor diagnostics can make compatibility debugging painful.
- Generated parsers can hide allocations in token streams and error recovery.

### Memory Implications

This is a high-value memory pattern. Parse trees are transient and should not be promoted to long-lived caches. Cache parsed AST only when the cache is bounded and query text reuse is high.

### Concurrency Implications

Parser state should be per-query and not shared. Intern tables can be shared if synchronized or sharded.

### Testing Implications

Test parser memory behavior with:

- large `MATCH` chains;
- large `WITH` projections;
- nested expressions;
- procedure calls with dynamic arguments;
- syntax errors that require useful diagnostics.

### How Future Agents Should Apply It

When inspecting parser code, look for explicit parse tree retention and error-recovery state. Those are likely RAM pressure points for the Rust rewrite.

## Pattern 7: Cypher Compatibility Adapter Through A DSL AST Factory

### Where Found

- Language/framework: Java, Cypher-DSL parser.
- Repos and files:
  - `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java`
  - `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherDslASTFactory.java`
  - `opencypher-src/grammar/openCypher.bnf`

### What The Code Does

`CypherParser` exposes stable parse entry points:

- parse full statement;
- parse node pattern fragment;
- parse relationship pattern;
- parse expression;
- parse clause;
- parse with custom options.

It returns Cypher-DSL model objects rather than Neo4j internal AST objects.

`CypherDslASTFactory` implements an AST factory expected by the Neo4j parser pipeline and maps grammar constructs into Cypher-DSL objects. It also rejects constructs the DSL does not support, such as relationship type expressions in certain forms.

`openCypher.bnf` defines a grammar-level compatibility reference for query, update, match, unwind, with, create, merge, set, remove, delete, and procedure call shapes.

Short pseudocode:

```text
Cypher grammar -> parser -> ASTFactory
  Neo4j internal AST factory: produces compiler AST
  Cypher-DSL AST factory: produces public builder model
```

### Why It Matters For Neo4j-In-Rust

The parser should not be hard-wired to one internal AST. If Rust can parse Cypher into an abstract builder interface, it can support:

- internal compiler AST;
- public typed query DSL;
- lint/format tooling;
- compatibility validation tools;
- migration assistants.

This keeps the grammar surface compatible while allowing lower-RAM internal ASTs.

### When To Use

Use this pattern when:

- you want one grammar but multiple AST consumers;
- you need a public query builder separate from the execution compiler;
- you need compatibility tools that parse but do not execute.

Avoid it if:

- the parser is small and only has one consumer;
- the adapter layer forces allocations for every grammar event.

### Rust Translation

Define a grammar event sink:

```rust
pub trait CypherAstSink {
    type Statement;
    type Expr;
    type Pattern;

    fn statement(&mut self, parts: StatementParts<Self>) -> Result<Self::Statement, ParseError>;
    fn expression(&mut self, parts: ExprParts<Self>) -> Result<Self::Expr, ParseError>;
    fn node_pattern(&mut self, parts: NodePatternParts<Self>) -> Result<Self::Pattern, ParseError>;
}

pub struct CompilerAstFactory<'a> { arena: &'a Bump }
pub struct PublicDslFactory { builder: DslBuilder }
```

For lower RAM, the compiler factory should produce arena IDs while the public DSL factory can produce owned builder objects.

### Risks

- A generic AST factory can become too abstract and allocate heavily.
- Public DSL semantics may lag behind full Cypher grammar.
- Unsupported grammar forms must fail loudly with stable error messages.

### Memory Implications

Keep factories zero-copy where possible. Pass token spans and interned symbols instead of owned strings. Public DSL models can allocate; internal compiler AST should be compact.

### Concurrency Implications

Factories are per-parse. Shared grammar tables can be static.

### Testing Implications

Run the same grammar corpus through:

- compiler AST factory;
- public DSL factory;
- formatter/linter factory if present;
- openCypher TCK feature examples.

### How Future Agents Should Apply It

When adding Cypher support in Rust, design the parser output abstraction before committing to one AST representation. The public DSL and internal compiler should not force each other into an inefficient shape.

## Pattern 8: Bolt Protocol Registry Plus Finite State Machine

### Where Found

- Language/framework: Java, Neo4j Bolt server.
- Repos and files:
  - `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/protocol/common/BoltProtocol.java`
  - `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/fsm/StateMachineImpl.java`
  - `neo4j-testkit-src/boltstub/bolt_protocol.py`

### What The Code Does

`BoltProtocol` provides a versioned protocol object. It defines:

- available protocol versions;
- default feature set;
- frame signal filtering;
- state machine creation;
- request message registry;
- response message registry;
- value reader and writer registries;
- metadata handler;
- negotiation hook.

`StateMachineImpl` processes request messages through a current state. It handles:

- default and current state;
- failed and interrupted flags;
- admission control token;
- state transition result;
- response handler success/failure;
- fatal vs non-fatal failure behavior;
- transaction validation.

Testkit's `bolt_protocol.py` models the protocol side:

- handshake version decoding;
- protocol aliases/equivalent versions;
- PackStream version differences;
- message definitions by Bolt version;
- feature flags in newer versions;
- HELLO auto-responses for some versions.

Short pseudocode:

```text
on_message(msg):
  if failed or interrupted:
    return handle_ignored_or_illegal(msg)

  admission = admission_control.await_token()
  next_state = current_state.process(msg, context, response_handler)
  current_state = next_state
  response_handler.on_success()
```

### Why It Matters For Neo4j-In-Rust

Bolt compatibility is driver compatibility. Drivers care about:

- version negotiation;
- exact message names and fields;
- PackStream encoding;
- state transitions;
- failure semantics;
- reset behavior;
- telemetry/logon/logoff/routing additions by version.

A Rust rewrite should not treat Bolt as a thin socket wrapper around query execution. It is a protocol product surface.

### When To Use

Use this pattern for any server that speaks Bolt or a Bolt-like protocol.

Avoid a hand-coded switch scattered across connection code. That will make version compatibility and Testkit conformance painful.

### Rust Translation

Use protocol descriptors and a typed FSM:

```rust
pub struct BoltProtocol {
    pub version: BoltVersion,
    pub features: FeatureSet,
    pub request_registry: RequestRegistry,
    pub response_registry: ResponseRegistry,
    pub packstream: PackStreamVersion,
}

pub enum BoltState {
    Negotiation,
    Authentication,
    Ready,
    Streaming,
    TxReady,
    Failed,
    Interrupted,
}

pub trait StateHandler {
    fn process(
        &mut self,
        msg: RequestMessage,
        ctx: &mut ConnectionContext,
        out: &mut ResponseSink,
    ) -> Result<BoltState, BoltError>;
}
```

Use table-driven version registration:

```rust
static BOLT_5_8: BoltProtocol = BoltProtocol {
    version: BoltVersion::new(5, 8),
    features: FeatureSet::SSR_HINTS,
    packstream: PackStreamVersion::V2,
    request_registry: registry_5_8(),
    response_registry: response_registry_5_x(),
};
```

### Risks

- State machine bugs often appear only in driver conformance tests.
- A permissive parser can accept invalid driver behavior and hide incompatibility.
- A strict parser can reject tolerated legacy behavior and break users.
- Failure/interrupted semantics are easy to get wrong.

### Memory Implications

For lower RAM:

- avoid buffering entire result streams in the Bolt layer;
- encode records directly from row cursors into output buffers;
- reuse read/write buffers per connection;
- bound connection queues;
- keep protocol registries static and shared.

### Concurrency Implications

Each connection has an FSM. Query execution may be async or worker-thread based, but state transitions must remain serialized per connection.

### Testing Implications

Use Testkit-style scripts for:

- handshake negotiation;
- auth/logon/logoff;
- reset during streaming;
- failure and recovery;
- telemetry messages;
- routing table calls;
- PackStream edge values;
- protocol feature flags.

### How Future Agents Should Apply It

Before implementing server-side Bolt in Rust, reproduce Testkit's version matrix as machine-readable test data. Then build the FSM against the matrix.

## Pattern 9: Driver Connection Discipline: URI, HELLO, RESET, Send/Receive

### Where Found

- Language/framework: Rust, `neo4rs`.
- Repos and files:
  - `neo4rs-src/lib/src/connection.rs`

### What The Code Does

`neo4rs` shows a Rust client-side Bolt connection shape:

- parse URI schemes such as `bolt`, secure variants, and routing variants;
- create TCP connection with timeout and keepalive;
- optionally wrap TLS;
- send Bolt magic and supported versions;
- parse server-selected version;
- send `HELLO` with authentication/routing/user-agent fields;
- handle success/failure;
- expose `send_recv`, `send`, and `recv` with timeout;
- support `RESET`;
- parse server hints.

Short pseudocode:

```text
connect(uri, auth):
  info = parse_uri(uri)
  stream = tcp_connect(info.addr, timeout)
  if info.tls:
    stream = tls_wrap(stream)
  send_magic_and_versions(stream)
  version = read_selected_version(stream)
  send_hello(auth, routing, user_agent)
  verify_success()
  return Connection { version, stream, hints }
```

### Why It Matters For Neo4j-In-Rust

Even though `neo4rs` is a driver, it shows what Rust users expect from a Neo4j-compatible ecosystem:

- idiomatic URI parsing;
- TLS scheme semantics;
- routing scheme semantics;
- clear send/receive API;
- reset behavior;
- timeout discipline;
- typed connection info.

A Rust server rewrite must behave correctly against official drivers and community drivers like `neo4rs`.

### When To Use

Use this pattern for client libraries, integration test drivers, and server acceptance tests.

On the server, mirror it in reverse: handshake, version selection, HELLO metadata, and reset behavior should have focused modules.

### Rust Translation

For the server side:

```rust
pub async fn accept_bolt_connection<S: AsyncRead + AsyncWrite>(
    stream: S,
    protocols: &'static ProtocolRegistry,
) -> Result<BoltConnection<S>, BoltError> {
    let offered = read_handshake_versions(&stream).await?;
    let selected = protocols.negotiate(offered)?;
    write_selected_version(&stream, selected).await?;
    let hello = read_hello(&stream, selected).await?;
    let auth = authenticate(hello.auth).await?;
    Ok(BoltConnection::new(stream, selected, auth))
}
```

For lower RAM, connection objects should own reusable buffers and small IDs, not session-heavy graph state.

### Risks

- URI scheme behavior is externally visible and easy to break.
- TLS/routing semantics differ between `bolt` and `neo4j` schemes.
- Timeout handling can leak tasks if cancellation does not propagate.

### Memory Implications

Use per-connection buffers with bounded capacity. Avoid retaining decoded messages beyond the FSM transition that needs them.

### Concurrency Implications

`send_recv` is simple for a client but server-side query execution may be concurrent. Keep wire read/write ownership clear to avoid interleaved response corruption.

### Testing Implications

Test with:

- official Neo4j drivers;
- `neo4rs`;
- Testkit;
- TLS and non-TLS schemes;
- routing and direct schemes;
- reset after failure and during streaming.

### How Future Agents Should Apply It

Use `neo4rs` as a Rust ergonomics reference, not as proof of server behavior. Pair it with Testkit and server Bolt sources when defining server compatibility.

## Pattern 10: Testkit As Protocol Oracle

### Where Found

- Language/framework: Python, Neo4j driver Testkit.
- Repos and files:
  - `neo4j-testkit-src/boltstub/bolt_protocol.py`
  - Searched but not directly read in depth this pass:
    - `neo4j-testkit-src/nutkit/protocol/requests.py`
    - `neo4j-testkit-src/nutkit/protocol/responses.py`
    - `neo4j-testkit-src/nutkit/protocol/feature.py`
    - `neo4j-testkit-src/nutkit/frontend/driver.py`
    - `neo4j-testkit-src/nutkit/frontend/session.py`

### What The Code Does

The Bolt stub protocol file is a compact executable compatibility matrix. It defines:

- Bolt protocol classes by version;
- aliases and equivalent versions;
- supported handshake offers;
- PackStream versions;
- request and response message structs;
- version-specific additions such as `LOGON`, `LOGOFF`, `TELEMETRY`, varint feature flags, SSR hints, and PackStream 3 for Bolt 6.

It can translate scripted JOLT lines into packed messages and validate whether messages are known for the protocol version.

### Why It Matters For Neo4j-In-Rust

Testkit is more valuable than prose protocol documentation because it encodes behavior as runnable scripts. A Rust rewrite should treat Testkit as a conformance oracle for:

- drivers talking to the server;
- server behavior under edge protocol cases;
- version-specific compatibility.

### When To Use

Use this pattern whenever compatibility spans multiple client languages.

Do not rely only on unit tests written inside the Rust server. Cross-driver behavior needs an external oracle.

### Rust Translation

Create a Rust conformance harness that can consume Testkit-like scripts:

```rust
pub struct ProtocolScript {
    pub version: BoltVersion,
    pub steps: Vec<ScriptStep>,
}

pub enum ScriptStep {
    ClientSends(RequestMessage),
    ServerSends(ResponseMessage),
    ExpectDisconnect,
}
```

Long-term, generate server protocol tests from a shared YAML/JSON version matrix distilled from Testkit.

### Risks

- Testkit can lag or lead server internals depending on branch.
- Passing Testkit is necessary but not sufficient for performance and resource behavior.
- Script-driven tests need deterministic scheduling around async query execution.

### Memory Implications

Protocol conformance tests should include large result streams to prove the server streams instead of buffering.

### Concurrency Implications

Test concurrency cases:

- reset while result streaming;
- multiple sessions per driver;
- transaction timeout;
- connection close during query execution;
- authentication failure and recovery.

### Testing Implications

Make Testkit part of CI early. Compatibility bugs become more expensive after client APIs and server state machines are already shaped.

### How Future Agents Should Apply It

When another agent studies official drivers, align every observed driver behavior to a Testkit feature or script where possible.

## Pattern 11: Thin Procedure Stub, Per-Request GDS Facade

### Where Found

- Language/framework: Java, Neo4j GDS procedures.
- Repos and files:
  - `neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`
  - `neo4j-gds-src/procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java`
  - `neo4j-gds-src/procedures/graph-catalog-facade/src/main/java/org/neo4j/gds/procedures/catalog/LocalGraphCatalogProcedureFacade.java`

### What The Code Does

`GraphProjectProc` is a thin Neo4j procedure class. It has `@Context GraphDataScienceProcedures facade`, annotates public methods as procedures, then delegates to `facade.graphCatalog().nativeProject(...)`, `estimateNativeProject(...)`, and related methods.

`GraphCatalogProcedureFacade` is an interface for the GDS graph catalog procedure surface. It includes operations such as:

- graph exists;
- graph drop/list;
- native projection and estimate;
- Cypher projection and estimate;
- subgraph projection;
- graph size;
- stream/mutate/write/export/generate variants.

`LocalGraphCatalogProcedureFacade` is constructed per request. Its comment is explicit: Neo4j-procedure-specific behavior should live here, while business logic is pushed into lower-level applications. Procedure stubs should be dumb, thin, and generateable.

It carries request-specific dependencies:

- request metadata;
- return columns;
- write context;
- stream closer;
- database service;
- memory usage service;
- transaction context;
- database mode restriction;
- termination flag;
- task registry;
- user log registry.

Short pseudocode:

```text
@Procedure("gds.graph.project")
project(graph_name, node_projection, relationship_projection, config):
  return facade.graph_catalog().native_project(
    graph_name,
    node_projection,
    relationship_projection,
    config
  )
```

### Why It Matters For Neo4j-In-Rust

This is a strong modularity lesson. Procedure annotations and wire-visible names should not contain graph algorithm business logic. A Rust rewrite should keep:

- procedure declaration,
- request adaptation,
- graph catalog application logic,
- graph storage,
- algorithm execution,
- result streaming

as separate layers.

This also helps memory reduction because request-specific objects can be short-lived and business logic can operate on compact internal graph stores.

### When To Use

Use this pattern for high-volume procedure families like GDS and APOC:

- many procedures;
- shared request context;
- repeated validation;
- many return-column variants;
- separate estimate and execute paths.

Avoid this pattern when a single built-in procedure is trivial and has no shared service layer.

### Rust Translation

```rust
pub struct GraphCatalogFacade<'a> {
    pub user: UserId,
    pub database: DatabaseId,
    pub tx: &'a mut dyn KernelTransaction,
    pub memory: &'a dyn MemoryUsageService,
    pub termination: &'a dyn TerminationFlag,
    pub return_columns: ReturnColumnSet,
    pub apps: &'a GraphCatalogApps,
}

impl<'a> GraphCatalogFacade<'a> {
    pub fn native_project(
        &mut self,
        graph_name: GraphNameId,
        nodes: NodeProjection,
        rels: RelationshipProjection,
        config: ProjectionConfig,
        out: &mut dyn RowSink,
    ) -> Result<(), GdsError> {
        self.apps.native_project.run(self, graph_name, nodes, rels, config, out)
    }
}
```

Procedure stubs become generated adapters from Cypher values to typed configs.

### Risks

- Facades can become huge if every procedure family shares one object.
- Request adaptation can hide expensive conversions.
- Return-column-sensitive computation can become surprising if not documented.

### Memory Implications

Only compute expensive fields when requested. The GDS facade code conditionally computes things like degree distribution and memory usage depending on return columns. Rust should make this a formal pattern:

```rust
if columns.contains(ReturnColumn::MemoryUsage) {
    row.memory_usage = Some(estimate_memory(graph));
}
```

### Concurrency Implications

The facade is per request, so it can hold non-`Sync` transaction references. Shared catalog apps must be thread-safe.

### Testing Implications

Test:

- procedure stubs delegate to the correct application;
- return column selection avoids unnecessary expensive work;
- estimate path does not mutate catalog;
- termination flag is honored during projection;
- user/database scoping is passed through.

### How Future Agents Should Apply It

When mining GDS, separate "procedure surface" from "application" from "graph representation". Do not merge them in Rust.

## Pattern 12: Graph Catalog Scoped By User And Database

### Where Found

- Language/framework: Java, Neo4j GDS catalog.
- Repos and files:
  - `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
  - `neo4j-gds-src/procedures/graph-catalog-facade/src/main/java/org/neo4j/gds/procedures/catalog/LocalGraphCatalogProcedureFacade.java`

### What The Code Does

`GraphStoreCatalog` is a static global catalog with:

- a concurrent map from user to `UserCatalog`;
- graph-added and graph-removed event listeners;
- optional logging;
- lookup by user, database, and graph name;
- own-user-first search;
- fallback search across users;
- errors for no match or ambiguous matches;
- set/remove/count/exists operations;
- memory-size event reporting when a graph store is added.

Short pseudocode:

```text
get(user, database, graph_name):
  if user_catalog[user].contains(database, graph_name):
    return own_graph

  matches = search_all_users(database, graph_name)
  if matches.empty: not_found
  if matches.more_than_one: ambiguous
  return matches[0]
```

### Why It Matters For Neo4j-In-Rust

GDS graph catalogs are not just name maps. They encode:

- ownership;
- database identity;
- graph visibility;
- ambiguity behavior;
- lifecycle events;
- memory accounting;
- drop semantics.

For a low-RAM rewrite, named in-memory graph projections can dwarf the base database. Catalog design is a memory-management feature, not only an API feature.

### When To Use

Use this pattern when:

- users create named in-memory graph projections;
- graphs are scoped to databases;
- shared visibility is supported;
- catalog operations must report memory and lifecycle events.

Avoid a global `HashMap<String, Graph>` without owner/database scope. It will break compatibility and resource governance.

### Rust Translation

```rust
pub struct GraphCatalog {
    by_owner: DashMap<UserId, UserGraphCatalog>,
    events: Arc<GraphCatalogEvents>,
}

pub struct UserGraphCatalog {
    graphs: HashMap<(DatabaseId, GraphNameId), Arc<GraphStore>>,
}

pub fn insert(
    &self,
    owner: UserId,
    database: DatabaseId,
    name: GraphNameId,
    graph: Arc<GraphStore>,
) -> Result<(), CatalogError> {
    let bytes = graph.memory_usage_bytes();
    self.by_owner.entry(owner).or_default().insert((database, name), graph)?;
    self.events.graph_added(owner, database, name, bytes);
    Ok(())
}
```

### Risks

- Static global state is convenient but hard to test and isolate.
- Cross-user lookup can surprise security boundaries if not explicit.
- Ambiguous names need stable error behavior.
- Event listeners can retain references and prevent graph memory from dropping.

### Memory Implications

Catalog entries should store `Arc<GraphStore>` handles, not copies. Graph stores should expose exact or estimated memory usage. Dropping a graph must release adjacency, property, and ID-mapping memory promptly.

### Concurrency Implications

Use sharded maps or `DashMap` for catalog operations, but be careful with long-running graph projection under locks. Insert only after projection succeeds.

### Testing Implications

Test:

- own-user graph lookup wins;
- ambiguous cross-user graph names fail;
- database ID is part of identity;
- remove fires event and frees memory;
- catalog count/is-empty behavior across users.

### How Future Agents Should Apply It

When catalog behavior appears in GDS client, docs, or agent code, map it back to user/database/name semantics. Do not collapse those dimensions in Rust.

## Pattern 13: Memory Estimation As Public API

### Where Found

- Language/framework: Java, Neo4j GDS memory estimation.
- Repos and files:
  - `neo4j-gds-src/memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
  - `neo4j-gds-src/procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java`
  - `neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`

### What The Code Does

`MemoryEstimation` is an interface with:

- component description;
- nested components;
- multiplication by a factor;
- estimate given graph dimensions and concurrency.

GDS exposes estimate procedures next to execution procedures, for example `gds.graph.project.estimate` next to `gds.graph.project`.

Short pseudocode:

```text
estimate_projection(graph_dimensions, concurrency):
  base = nodes_memory(dimensions)
  rels = relationships_memory(dimensions)
  properties = properties_memory(dimensions)
  workers = per_thread_memory(concurrency)
  return base + rels + properties + workers
```

### Why It Matters For Neo4j-In-Rust

If lower RAM usage is a product goal, memory estimation should be user-visible and testable. It is not enough for internals to be efficient. Users need to know whether a projection or algorithm will fit before running it.

Rust can make memory estimates more accurate because compact representations can expose predictable byte counts.

### When To Use

Use this pattern for:

- graph projection;
- algorithm execution;
- write-back/mutate operations;
- import/export;
- large query operators such as hash joins, aggregation, sort, shortest path expansions.

Avoid exposing precise-looking estimates when the implementation cannot bound memory. In that case expose ranges and confidence.

### Rust Translation

```rust
pub trait MemoryEstimate {
    fn description(&self) -> &'static str;
    fn estimate(&self, dims: GraphDimensions, concurrency: NonZeroUsize) -> MemoryRange;
}

pub struct MemoryRange {
    pub min_bytes: u64,
    pub expected_bytes: u64,
    pub max_bytes: u64,
}
```

Add memory estimates to procedure signatures and planning metadata:

```rust
pub struct ProcedureSignature {
    pub memory_estimator: Option<MemoryEstimatorId>,
    // ...
}
```

### Risks

- Estimates can become stale as internal layouts change.
- Underestimation creates production failures.
- Overestimation blocks valid workloads.
- Concurrency multiplies memory in non-obvious ways.

### Memory Implications

This pattern directly supports lower-RAM design. Every major structure should implement a `HeapSize` or `MemoryUsage` trait:

```rust
pub trait MemoryUsage {
    fn resident_bytes(&self) -> u64;
}
```

For compact graph stores, include:

- adjacency bytes;
- offset bytes;
- ID mapping bytes;
- property column bytes;
- compression dictionary bytes;
- per-thread scratch bytes.

### Concurrency Implications

Estimates must account for concurrency. Many graph algorithms allocate per-worker frontiers, queues, or accumulators.

### Testing Implications

Test estimates against measured allocations in benchmarks. Use tolerance bands, not exact equality, unless using custom allocators for deterministic measurement.

### How Future Agents Should Apply It

Any proposed Rust data structure for graph storage or query execution should include an estimate formula alongside the implementation.

## Pattern 14: Packed Adjacency Blocks

### Where Found

- Language/framework: Java generated from Rust, Neo4j GDS compression.
- Repos and files:
  - `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/packed/AdjacencyPacking.java`

### What The Code Does

`AdjacencyPacking` packs adjacency values into fixed-size blocks. The source notes it is generated by Rust. It uses:

- `BLOCK_SIZE = 64`;
- a dispatch table by bit width from 0 to 64;
- specialized packers per width;
- low-level unsafe memory writes.

Example shape:

```text
pack(bits, values, start, ptr):
  packer = PACKERS[bits]
  packer.pack(values, start, ptr)
```

For 1-bit values, 64 values fit in 8 bytes. Wider values pack accordingly.

### Why It Matters For Neo4j-In-Rust

This is direct evidence that GDS already uses Rust-generated code for dense memory-sensitive graph storage. A Rust rewrite should take this seriously: adjacency representation is one of the biggest RAM levers.

Neo4j-like OLTP storage and GDS projected graphs have different needs. GDS can often use immutable compressed adjacency arrays, while OLTP needs update-friendly stores. A Rust rewrite should support both instead of forcing one representation.

### When To Use

Use packed adjacency blocks for:

- immutable graph projections;
- analytics workloads;
- relationship scans;
- compressed neighbor lists;
- off-heap or mmap-friendly graph stores.

Do not use this representation directly for high-churn transactional adjacency unless paired with delta overlays.

### Rust Translation

Rust can make this cleaner and safer:

```rust
pub struct PackedAdjacency {
    offsets: Box<[u64]>,
    blocks: Box<[u8]>,
    bit_widths: Box<[u8]>,
}

impl PackedAdjacency {
    pub fn neighbors(&self, node: NodeId) -> NeighborIter<'_> {
        let range = self.range_for(node);
        NeighborIter::new(&self.blocks[range.bytes], range.bit_width)
    }
}
```

Use specialized kernels where profiling proves value:

- const generics for bit width;
- SIMD for unpacking;
- block-level min/base delta encoding;
- `bytemuck`/checked alignment helpers;
- fuzz tests against a simple reference encoder.

### Risks

- Compression saves RAM but can increase CPU.
- Random access within compressed blocks can be expensive.
- Unsafe packing bugs corrupt graph data silently.
- Different algorithms prefer different layouts.

### Memory Implications

This is a primary low-RAM pattern. Combine it with:

- dense `NodeId(u32)` remapping for projected graphs;
- relationship type filtering before projection;
- columnar property storage;
- optional direction-specific adjacency;
- compressed weights only when needed.

### Concurrency Implications

Immutable packed adjacency can be shared freely across threads with `Arc<PackedAdjacency>`. Per-thread iterators should hold only slices and cursor state.

### Testing Implications

Test:

- all bit widths 0..64;
- block boundaries;
- empty adjacency;
- high node IDs after remapping;
- roundtrip pack/unpack against reference vectors;
- concurrent iteration;
- memory size reporting.

### How Future Agents Should Apply It

Inspect GDS compression and loading code before inventing graph storage. The Rust rewrite should probably separate transactional storage from analytics projection storage and use packed immutable projection stores for GDS-like workloads.

## Pattern 15: Termination And Progress As Mandatory Execution Hooks

### Where Found

- Language/framework: Java, Neo4j GDS termination/progress.
- Repos and files:
  - `neo4j-gds-src/termination/src/main/java/org/neo4j/gds/termination/TerminationFlag.java`
  - `neo4j-gds-src/progress-tracking/src/main/java/org/neo4j/gds/core/utils/progress/tasks/ProgressTracker.java`
  - `neo4j-gds-src/procedures/graph-catalog-facade/src/main/java/org/neo4j/gds/procedures/catalog/LocalGraphCatalogProcedureFacade.java`

### What The Code Does

`TerminationFlag` is a small functional interface:

- `running() -> bool`;
- `assertRunning()` throws when stopped;
- wrappers around monitors;
- default constants for always-running and stopped;
- a run-check node count constant.

`ProgressTracker` exposes:

- resource footprint;
- requested concurrency;
- begin/end subtask;
- log progress;
- set/current volume;
- log messages;
- release;
- steps.

GDS facades pass termination and task/progress registries through request execution.

### Why It Matters For Neo4j-In-Rust

Graph workloads can be long-running and memory-heavy. A low-RAM Rust rewrite must be cancellable and observable from the start. Retrofitting cancellation into algorithms later is painful.

### When To Use

Use this pattern for:

- graph projection;
- graph algorithms;
- large Cypher operators;
- import/export;
- procedure calls that scan lots of data;
- background tasks.

Avoid optional cancellation hooks in core loops. Make them part of the execution context.

### Rust Translation

```rust
pub trait TerminationFlag: Send + Sync {
    fn running(&self) -> bool;

    fn check(&self) -> Result<(), Terminated> {
        if self.running() { Ok(()) } else { Err(Terminated) }
    }
}

pub trait ProgressTracker: Send + Sync {
    fn begin_subtask(&self, name: &'static str);
    fn advance(&self, units: u64);
    fn end_subtask(&self);
}
```

In hot loops, check every N nodes/edges:

```rust
for (i, node) in nodes.iter().enumerate() {
    if i % RUN_CHECK_NODE_COUNT == 0 {
        ctx.termination.check()?;
    }
    process_node(node);
}
```

### Risks

- Too-frequent checks hurt CPU performance.
- Too-infrequent checks make cancellation feel broken.
- Progress tracking can allocate or lock in hot loops if designed poorly.

### Memory Implications

Progress tracking should not retain per-node or per-edge state. Use counters and bounded logs.

### Concurrency Implications

Termination flags should be atomic. Progress trackers should use low-contention counters or per-worker aggregation.

### Testing Implications

Test:

- cancellation before start;
- cancellation during projection;
- cancellation during algorithm loop;
- cleanup after cancellation;
- progress does not grow unbounded;
- progress with multiple workers.

### How Future Agents Should Apply It

When reading any graph algorithm, identify where cancellation, memory tracking, and progress should be checked in the Rust version.

## Pattern 16: Compatibility Layers For Deprecation And Evolving APIs

### Where Found

- Language/framework: Java, GDS procedures and kernel signatures.
- Repos and files:
  - `neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`
  - `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/procs/ProcedureSignature.java`
  - `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/ProcedureCompiler.java`
  - `neo4j-testkit-src/boltstub/bolt_protocol.py`

### What The Code Does

GDS exposes deprecated procedures for older projection APIs and delegates them into newer facade methods while logging warnings or tracking deprecation.

`ProcedureSignature` carries deprecation metadata. `ProcedureCompiler` reads deprecation and warning annotations into signatures.

Bolt Testkit models version-by-version protocol additions rather than replacing old versions in place.

### Why It Matters For Neo4j-In-Rust

Neo4j compatibility is additive and historical. Users expect old procedure names, old Bolt versions, older projection APIs, and old driver behaviors to keep working for some period.

The Rust rewrite should avoid hard removal as the first tool. Build explicit compatibility adapters.

### When To Use

Use compatibility adapters when:

- old procedure names map cleanly to new implementations;
- old config keys can be normalized;
- old Bolt versions are still supported;
- driver-visible behavior changed but can be emulated.

Do not use adapters when:

- behavior would be dangerously misleading;
- old behavior breaks security;
- memory or correctness cost is unacceptable and product policy allows removal.

### Rust Translation

```rust
pub enum ProcedureTarget {
    Native(ProcedureId),
    DeprecatedAlias {
        target: ProcedureId,
        warning: WarningId,
        config_rewrite: Option<ConfigRewriteId>,
    },
}
```

Normalize configs before application logic:

```rust
pub fn normalize_projection_config(
    version: ApiVersion,
    raw: RawConfig,
) -> Result<ProjectionConfig, ConfigError> {
    match version {
        ApiVersion::LegacyCypherProjection => rewrite_legacy_cypher_projection(raw),
        ApiVersion::NativeProjection => parse_native_projection(raw),
    }
}
```

### Risks

- Compatibility code can accumulate and obscure the current API.
- Deprecated aliases can hide performance costs.
- Warnings must be visible but not spammy.

### Memory Implications

Config normalization should avoid cloning large maps. Parse into borrowed/raw views, then build compact typed configs.

### Concurrency Implications

Compatibility adapters should be stateless or immutable.

### Testing Implications

Snapshot:

- deprecation warnings;
- old procedure output columns;
- config rewrite behavior;
- old Bolt handshake behavior;
- removed/unsupported behavior errors.

### How Future Agents Should Apply It

When a repo shows deprecated APIs, preserve the adapter shape in the Rust reference section. Deprecation paths are compatibility requirements.

## Pattern 17: Observability Attached To Execution Boundaries

### Where Found

- Language/framework: Java/Scala, Neo4j Cypher, Bolt, and GDS.
- Repos and files:
  - `neo4j-src/community/cypher/cypher/src/main/scala/org/neo4j/cypher/internal/CypherCurrentCompiler.scala`
  - `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/fsm/StateMachineImpl.java`
  - `neo4j-gds-src/progress-tracking/src/main/java/org/neo4j/gds/core/utils/progress/tasks/ProgressTracker.java`
  - `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`

### What The Code Does

Observed boundaries:

- Cypher compiler uses monitors, traces compilation/execution, and attaches logical-plan text as suppressed failure information on runtime compile errors.
- Bolt FSM logs database errors and state-machine failures, distinguishing fatal from non-fatal errors.
- GDS progress tracker reports task progress and resource footprint.
- GDS graph catalog fires graph added/removed events and reports memory size on add.

### Why It Matters For Neo4j-In-Rust

Low-RAM work needs observability because memory pressure bugs otherwise look like slow queries, connection failures, or killed processes. The rewrite should make memory, planning, protocol, and catalog events visible.

### When To Use

Use this pattern at:

- query parse/plan/compile/execute boundaries;
- Bolt connection state transitions;
- transaction begin/commit/rollback;
- procedure call entry/exit;
- graph projection and algorithm tasks;
- catalog graph add/drop;
- memory reservation/failure.

Avoid chatty per-row or per-edge logs.

### Rust Translation

Use structured events:

```rust
pub enum DbEvent<'a> {
    QueryPlanned { query_id: QueryId, cache_hit: bool, plan_bytes: u64 },
    BoltStateTransition { conn: ConnectionId, from: BoltState, to: BoltState },
    GraphCatalogAdded { user: UserId, database: DatabaseId, graph: GraphNameId, bytes: u64 },
    MemoryReservationFailed { component: &'a str, requested: u64, available: u64 },
}
```

Wire them to tracing:

```rust
tracing::info!(
    query_id = %query_id,
    cache_hit = cache_hit,
    plan_bytes = plan_bytes,
    "query planned"
);
```

### Risks

- Observability can allocate too much if every event builds strings.
- Query text and parameters may contain sensitive data.
- Debug plan capture can retain large plans accidentally.

### Memory Implications

Use IDs and interned names in events. Generate human strings lazily at sinks. Keep bounded ring buffers for recent events.

### Concurrency Implications

Tracing should be non-blocking or low-contention. High-volume counters should use atomics or sharded metrics.

### Testing Implications

Test that:

- memory failures report component and size;
- query compile failures include useful plan context without huge retention;
- catalog events fire once;
- protocol errors are classified;
- sensitive parameter values are redacted.

### How Future Agents Should Apply It

When future sections discuss memory reductions, attach an observability event for each major memory owner. If it cannot be observed, it will be hard to operate.

## Pattern 18: Client Ergonomics Are Compatibility Surface

### Where Found

This pass directly inspected `cypher-dsl` and `neo4rs`, and searched the other client-facing repos. These findings need deeper follow-up, but the architecture signal is already clear.

- Direct reads:
  - `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java`
  - `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherDslASTFactory.java`
  - `neo4rs-src/lib/src/connection.rs`
- Searched but not deeply read in this pass:
  - `cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/CypherShell.java`
  - `cypher-shell-src/cypher-shell/src/main/java/org/neo4j/shell/state/BoltStateHandler.java`
  - `neo4j-browser-src/...`
  - `neo4j-gds-client-src/...`
  - `neo4j-ogm-src/...`
  - official Java, JavaScript, Python, Go, and .NET driver repos.

### What The Code Shows

Client-facing repos encode expectations that are easy to miss if only the database core is studied:

- Cypher can be parsed into a public DSL.
- Rust clients expect idiomatic URI/TLS/routing handling.
- Shells and browsers expect stable result metadata, error codes, transaction behavior, and multi-statement parsing.
- GDS clients expect procedure names and config/result shapes to remain stable.
- OGM layers expect type mapping and entity identity behavior.
- Official drivers expect strict Bolt semantics and Testkit conformance.

### Why It Matters For Neo4j-In-Rust

A Rust rewrite can have a fast kernel and still fail if client ergonomics do not match the ecosystem. Compatibility includes:

- error code shape;
- result summary metadata;
- notification and warning shape;
- query parameter encoding;
- multi-database behavior;
- routing behavior;
- auth behavior;
- procedure listing;
- temporal/spatial value encoding;
- transaction retry semantics.

### Rust Translation

Create a compatibility acceptance matrix:

```text
Surface             Rust acceptance target
------------------  -----------------------------------------
Cypher DSL          Parse common queries into stable AST/DSL model
Cypher shell        Multi-statement execution, errors, summaries
Browser             Metadata, notifications, routing, auth
GDS client          Procedure names, config maps, result columns
OGM                 IDs, labels, properties, transactions
Official drivers    Testkit conformance
neo4rs              Rust driver handshake/query/reset behavior
```

### Risks

- Focusing only on server internals misses compatibility details in clients.
- Client behavior can depend on undocumented server quirks.
- Browser and shell may rely on specific metadata keys.

### Memory Implications

Client compatibility often pressures the server to retain metadata. Keep result metadata compact:

- intern column names;
- stream records;
- produce summaries at the end from counters;
- avoid retaining all records for summaries.

### Concurrency Implications

Drivers and shells exercise retries, multiple sessions, transaction functions, and routing. Server state must remain per-connection/per-session where required.

### Testing Implications

Use official drivers plus community Rust driver tests. Add browser/shell smoke tests once the server has enough Bolt and Cypher support.

### How Future Agents Should Apply It

Future agents covering driver repos should extract exact error/result/session contracts and connect them to Bolt FSM and transaction-manager patterns above.

## Pattern 19: APOC-Style Extension Surface Is A Procedure Stress Test

### Where Found

This pass searched APOC repos but did not deeply read all relevant files. Direct source reads were concentrated in the Neo4j procedure framework that APOC depends on.

Searched paths of interest:

- `neo4j-apoc-src/common/src/main/java/apoc/RegisterComponentFactory.java`
- `neo4j-apoc-src/common/src/main/java/apoc/ApocConfig.java`
- `neo4j-apoc-src/common/src/main/java/apoc/Pools.java`
- `neo4j-apoc-src/processor/src/main/java/apoc/processor/ApocProcessor.java`
- `neo4j-apoc-src/core/src/main/java/apoc/create/Create.java`
- `neo4j-apoc-src/core/src/main/java/apoc/refactor/GraphRefactoring.java`
- `neo4j-apoc-procedures-src/...`

### What The Search Indicates

APOC is a broad procedure family covering create/refactor/meta/import/export/utilities and custom procedures. It registers shared components such as config and pools into the Neo4j procedure context. It also has annotation processing and many tests around procedure behavior.

Because APOC is large and procedure-heavy, it is the best stress test for:

- procedure registration;
- context injection;
- security restrictions;
- transaction access;
- config handling;
- background pools;
- return-column stability;
- deprecation and compatibility behavior.

### Why It Matters For Neo4j-In-Rust

If a Rust rewrite can model kernel procedures well enough for APOC-like extensions, the extension boundary is probably sound. If APOC patterns force privileged backdoors, the procedure boundary is too weak.

### Rust Translation

Treat APOC compatibility as a tiered target:

```text
Tier 1: pure read utilities and metadata procedures
Tier 2: write/refactor procedures inside normal transactions
Tier 3: import/export and file/network procedures behind policy gates
Tier 4: custom/background procedures with explicit task pools
```

For extension pools, do not expose raw global executors. Expose capability-scoped task handles:

```rust
pub trait ProcedureTaskPool {
    fn spawn_bounded(
        &self,
        owner: ProcedureCallId,
        memory_limit: MemoryLimit,
        task: BoxFuture<'static, Result<(), ProcedureError>>,
    ) -> Result<TaskId, ProcedureError>;
}
```

### Risks

- APOC-like power can bypass database invariants.
- File/network procedures expand security surface.
- Background tasks can outlive transactions if not carefully scoped.

### Memory Implications

APOC procedures that materialize paths, maps, JSON, or imports can be memory-heavy. Rust should provide streaming row sinks and bounded buffers by default.

### Concurrency Implications

Procedure task pools must be bounded and observable. They should integrate with termination, memory tracking, and transaction lifecycle.

### Testing Implications

Procedure tests should include:

- safe/full-access mode;
- denied file/network config;
- transaction rollback on failure;
- streaming rather than materializing;
- background task cancellation.

### How Future Agents Should Apply It

Future APOC-focused agents should open the specific files above and extract exact component registration, config, and pool patterns. This first pass only marks the architectural importance.

## Pattern 20: GDS Agent And Client As Remote Compatibility Layers

### Where Found

This pass searched but did not directly read deeply:

- `gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/graph_projection_specs.py`
- `gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/graph_projection_handlers.py`
- `gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/gds.py`
- `neo4j-gds-client-src/...`
- `graph-data-science-src/documentation/graph-data-science.adoc`

Search results indicated the agent distinguishes standard plugin mode from session/remote mode, including calls like `gds.session.list()` and `gds.graph.project.remote`. Documentation contains many `CALL gds.graph.project` and estimate examples.

### Why It Matters For Neo4j-In-Rust

GDS is no longer only in-database procedures. Tooling and clients can mediate projections, sessions, remote execution, and generated Cypher calls. A Rust rewrite should plan for:

- local in-process graph catalog;
- remote/session graph catalog;
- stable procedure facade over both;
- typed client config;
- docs examples as compatibility fixtures.

### Rust Translation

Model graph execution location explicitly:

```rust
pub enum GraphExecutionTarget {
    LocalDatabase(DatabaseId),
    RemoteSession(SessionId),
}

pub struct GraphProjectRequest {
    pub target: GraphExecutionTarget,
    pub graph_name: GraphNameId,
    pub node_projection: NodeProjection,
    pub relationship_projection: RelationshipProjection,
    pub config: ProjectionConfig,
}
```

### Risks

- Remote/session semantics can diverge from local semantics.
- Client libraries may normalize config differently than server procedures.
- Agent-generated calls can rely on docs examples that are not fully tested.

### Memory Implications

Remote GDS can shift memory off the database server, but it adds serialization and duplication risks. For Rust, make ownership clear: does the graph live in the base DB, local projection memory, remote service memory, or client memory?

### Testing Implications

Use docs examples and GDS client calls as fixtures. Verify estimate and execute paths for local and remote targets.

### How Future Agents Should Apply It

Future agents should directly inspect the GDS client and agent files, then connect their public APIs back to the GDS facade/catalog patterns above.

## Cross-Cutting Rust Rewrite Guidance

### Stable Boundary, Compact Interior

Neo4j exposes stable, object-rich APIs in Java. A Rust rewrite should not copy the object model directly. Instead:

- keep public signatures and protocol schemas stable;
- translate names and strings into compact IDs at the boundary;
- use immutable shared catalogs;
- use arenas for short-lived parse/plan state;
- use packed arrays for graph projections;
- stream results through sinks;
- make memory estimates and memory trackers part of execution.

### Suggested Layer Shape

```text
compat/
  bolt/
  cypher_surface/
  procedure_signature/
  driver_testkit/

kernel/
  tx/
  security/
  tokens/
  schema/
  storage/

cypher/
  parser/
  semantic/
  planner/
  runtime/
  cache/

procedures/
  registry/
  context/
  apoc_compat/
  gds/

gds/
  catalog/
  projection/
  algorithms/
  memory_estimation/
  compressed_graph/

observability/
  tracing_events/
  progress/
  memory/
```

### Compatibility Acceptance Tests To Build Early

1. Bolt handshake/version matrix from Testkit.
2. Basic official driver smoke tests for Java, JavaScript, Python, Go, .NET, and Rust `neo4rs`.
3. Procedure signature listing snapshots.
4. Cypher parse corpus from openCypher TCK and docs examples.
5. GDS projection estimate/execute examples from docs and client.
6. Transaction/security tests that route through Bolt and embedded/procedure execution.
7. Cancellation and memory-limit tests for long-running graph operations.

### Memory-Reduction Bets

High-confidence memory reduction opportunities:

- intern all schema/procedure/query names;
- arena-allocate parser and planner temporaries;
- do not cache parse trees;
- compact procedure signatures with IDs and bitflags;
- immutable shared runtime plans with per-execution state separated;
- stream Bolt records without buffering;
- compressed immutable GDS projections;
- columnar property stores for projected graphs;
- bounded per-connection buffers;
- request-scoped facades instead of global mutable request objects;
- memory estimation before graph projection and algorithms.

Riskier but promising:

- mmap-backed graph projection storage;
- adaptive compressed adjacency by degree distribution;
- WASM extension procedures with capability imports;
- cross-query shared string and value dictionaries;
- physical plan specialization by schema statistics.

## Coverage And Evidence

### Graph Tools Attempted

#### codebase-memory-evidence-reader

Used successfully for navigation.

Index command:

```bash
CBM_CACHE_DIR=/tmp/codex-code-intel/codebase-memory/neo4j-family-20260706230522/cache \
/Users/amuldotexe/.codex/tooling/code-intelligence/bin/codebase-memory-mcp cli index_repository \
'{"repo_path":"/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family"}'
```

Observed result:

- project: `Users-amuldotexe-Desktop-personal-repos-lane-knight-bus-graph-walker-gitrefrepo-Neo4j-family`
- files discovered: 26,138
- nodes: 426,686
- edges: 2,080,483
- tests: 24,227
- excluded `.git` directories and some generated/build/hadoop directories.

Used only as navigation evidence. Important findings above were verified by opening source files.

#### codegraphcontext-evidence-reader

Attempted indexing against the full `Neo4j family` directory:

```bash
HOME=/tmp/codex-code-intel/codegraphcontext/neo4j-family-20260706230522/home \
/Users/amuldotexe/.codex/tooling/code-intelligence/.venvs/codegraphcontext/bin/cgc \
--database ladybugdb \
--path /tmp/codex-code-intel/codegraphcontext/neo4j-family-20260706230522/ladybugdb.sqlite \
index "/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family" \
--force
```

This was stopped because of the timebox before it produced usable evidence. No conclusions in this file depend on CodeGraphContext output.

### Direct Source Reads

Directly opened/read files used in this pass:

- `neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/api/procedure/GlobalProcedures.java`
- `neo4j-src/community/kernel/src/main/java/org/neo4j/kernel/api/procedure/CallableProcedure.java`
- `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/GlobalProceduresRegistry.java`
- `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/ComponentRegistry.java`
- `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/FieldInjections.java`
- `neo4j-src/community/procedure/src/main/java/org/neo4j/procedure/impl/ProcedureCompiler.java`
- `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/procs/ProcedureSignature.java`
- `neo4j-src/community/kernel-api/src/main/java/org/neo4j/kernel/api/KernelTransaction.java`
- `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/security/LoginContext.java`
- `neo4j-src/community/kernel-api/src/main/java/org/neo4j/internal/kernel/api/security/SecurityContext.java`
- `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/tx/TransactionManager.java`
- `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/tx/TransactionManagerImpl.java`
- `neo4j-src/community/cypher/cypher/src/main/scala/org/neo4j/cypher/internal/CypherCurrentCompiler.scala`
- `neo4j-src/community/cypher/front-end/parser/v5/ast-factory/src/main/scala/org/neo4j/cypher/internal/parser/v5/ast/factory/Cypher5AstParser.scala`
- `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java`
- `cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherDslASTFactory.java`
- `opencypher-src/grammar/openCypher.bnf`
- `neo4j-gds-src/procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java`
- `neo4j-gds-src/procedures/graph-catalog-facade/src/main/java/org/neo4j/gds/procedures/catalog/LocalGraphCatalogProcedureFacade.java`
- `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
- `neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`
- `neo4j-gds-src/memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
- `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/packed/AdjacencyPacking.java`
- `neo4j-gds-src/termination/src/main/java/org/neo4j/gds/termination/TerminationFlag.java`
- `neo4j-gds-src/progress-tracking/src/main/java/org/neo4j/gds/core/utils/progress/tasks/ProgressTracker.java`
- `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/protocol/common/BoltProtocol.java`
- `neo4j-src/community/bolt/src/main/java/org/neo4j/bolt/fsm/StateMachineImpl.java`
- `neo4j-testkit-src/boltstub/bolt_protocol.py`
- `neo4rs-src/lib/src/connection.rs`

### Repo Coverage Matrix

| Repo directory | Coverage in this pass | Evidence type | Remaining gap |
|---|---:|---|---|
| `cypher-dsl-src` | Medium | Direct parser and AST factory reads | More query builder API ergonomics and tests |
| `cypher-shell-src` | Low | Search/navigation only | Directly inspect shell parser, Bolt state handler, error/result formatting |
| `gds-agent-src` | Low | Search/navigation only | Directly inspect projection handlers, session detection, generated Cypher calls |
| `graph-data-science-src` | Low | Search/navigation only | Mine docs examples as compatibility fixtures |
| `neo4j-apoc-procedures-src` | Low | Search/navigation only | Directly inspect procedure tests and legacy APOC package shape |
| `neo4j-apoc-src` | Low-medium | Search plus procedure-framework context | Directly inspect component registration, config, pools, representative procedures |
| `neo4j-browser-src` | Low | Repo identified, not source-read in this timebox | Result metadata, error rendering, routing/auth expectations |
| `neo4j-docs-bolt-src` | Low | Not directly read | Cross-check docs against server Bolt/Testkit matrix |
| `neo4j-dotnet-driver-src` | Low | Repo identified/search only | Session/transaction/retry contracts and Testkit backend |
| `neo4j-gds-client-src` | Low | Search/navigation only | Directly inspect typed client API and generated procedure call shapes |
| `neo4j-gds-src` | High for catalog/projection/memory hooks | Direct reads | More algorithm internals and property store layouts |
| `neo4j-go-driver-src` | Low | Repo identified/search only | Bolt message mapping, retry/session semantics |
| `neo4j-java-driver-src` | Low | Repo identified/search only | Driver API, observation/micrometer, Testkit backend |
| `neo4j-javascript-driver-src` | Low | Repo identified/search only | Bolt connection transformers, temporal/spatial values, browser-facing behavior |
| `neo4j-ogm-src` | Low | Repo identified/search only | Entity mapping, session lifecycle, transaction integration |
| `neo4j-python-driver-src` | Low | Repo identified/search only | Driver protocol/session behavior and Testkit backend |
| `neo4j-src` | High for kernel/procedure/Cypher/Bolt | Direct reads | Storage engine internals, counts/index/statistics, Cypher runtimes deeper |
| `neo4j-testkit-src` | Medium | Direct Bolt protocol read, searched protocol/frontend files | Directly inspect feature matrix and backend contracts |
| `neo4rs-src` | Medium | Direct connection read | Query API, transaction API, value encoding |
| `opencypher-src` | Medium | Direct grammar read | TCK features and expected semantic behavior |

### High-Value Next Reads

For the next agent pass, prioritize:

1. `neo4j-testkit-src/nutkit/protocol/feature.py`, `requests.py`, `responses.py`, and frontend driver/session files.
2. Official driver Bolt/message/session implementations in Java, JavaScript, Python, Go, and .NET.
3. `cypher-shell-src` result/error handling and statement parser.
4. `neo4j-browser-src` metadata/error/result expectations.
5. APOC `RegisterComponentFactory`, `ApocConfig`, `Pools`, and representative procedure tests.
6. GDS client and GDS agent projection/session files.
7. Neo4j storage engine memory layouts and counts/index dependencies for planning.

## Final Synthesis For A Rust Neo4j-Like Rewrite

The Neo4j family suggests a clear design stance:

- **Compatibility belongs at explicit boundaries**: procedure signatures, Cypher grammar, Bolt protocol descriptors, transaction/security contexts, graph catalog APIs, and driver conformance tests.
- **Memory reduction belongs behind those boundaries**: compact IDs, arenas, immutable snapshots, streaming row sinks, packed adjacency, bounded caches, and request-scoped contexts.
- **Operational safety must be built in**: memory estimates, cancellation flags, progress tracking, structured events, and versioned compatibility adapters.

The most transferable engineering taste is not any single class or API. It is the habit of making public contracts declarative and stable while letting internals be specialized, compact, and replaceable.

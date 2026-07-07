# Meta Graph Database Patterns 4: Parser And Code Intelligence Patterns

Source-backed encyclopedia slice for parser, compiler, code-intelligence, tree-sitter, and tooling patterns relevant to Cypher parsing, ASTs, graph extraction, dependency graphs, and agentic code navigation for a Neo4j-in-Rust rewrite.

Worker scope: this file is the canonical parser/compiler/code-intelligence slice for the five-file corpus. It preserves parser, compiler, code-intelligence, tree-sitter, and tooling patterns future agents can translate into a Rust parser/planner and code-navigation toolkit with verifiable source context.

## Evidence Method

I used the required local graph-evidence skills first, then treated their output as navigation rather than final proof. Important claims below are verified with direct source reads from the listed repositories.

Evidence commands and tools used:

- Required skill read: `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`.
- Required skill read: `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`.
- CodeGraphContext wrapper:
  `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`
- codebase-memory wrapper:
  `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`
- Source discovery: `rg --files`, `find`, `rg -n`.
- Source verification: `nl -ba <file> | sed -n '<range>p'`.
- Repo slices inspected directly:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/parseltongue-rust-LLM-companion`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/parseltongue-rust-LLM-companion/git-ref-repo/ignore-this-folder-repos/universal-ctags__ctags`
  - `/Users/amuldotexe/Desktop/oss-read-only/clarity-cli`
  - `/Users/amuldotexe/Desktop/oss-read-only/codex`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/antlr-grammars-v4-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/opencypher-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/libcypher-parser-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/cypher-dsl-src`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/cypher-shell-src`

Graph-tool observations:

- CodeGraphContext indexed the current repo and reported 260 files, 560 functions, 66 structs, 25 enums, 80 modules. The wrapper explicitly reported that indexed query outputs did not mention `gitrefrepo/`, so I did not use CodeGraphContext as evidence for the vendored reference repos.
- codebase-memory listed the current project with 4,641 nodes and 7,829 edges. The follow-up project query failed with a project-selection mismatch even though the project was listed. I used that result only as a feasibility/caveat signal, not as evidence.
- Both graph tools were therefore useful for confirming the local project shape but not sufficient for source claims in this slice.

## Repositories Inspected

| Repository | Evidence role | Direct source highlights |
|---|---|---|
| `parseltongue-rust-LLM-companion` | Rust code-intelligence, tree-sitter extraction, stable graph keys, Cozo graph storage | `isgl1_generator.rs`, `entity_queries/rust.scm`, `dependency_queries/rust.scm`, `entities.rs`, `cozo_client.rs` |
| `tree-sitter__tree-sitter` | Parser API, grammar DSL, incremental parsing, query/tag conventions | `docs/src/creating-parsers/2-the-grammar-dsl.md`, `docs/src/using-parsers/3-advanced-parsing.md`, `docs/src/using-parsers/4-walking-trees.md`, `docs/src/4-code-navigation.md`, `lib/include/tree_sitter/api.h` |
| `tree-sitter__tree-sitter-graph` | Tree-sitter-to-graph DSL and execution model | `README.md`, `src/graph.rs`, `src/execution.rs` |
| `antlr-grammars-v4-src/cypher` | Split lexer/parser grammar and Cypher syntax shape | `CypherLexer.g4`, `CypherParser.g4` |
| `opencypher-src` | Canonical-ish Cypher BNF and TCK behavior contracts | `grammar/openCypher.bnf`, `tck/README.adoc` |
| `libcypher-parser-src` | C AST layout, typed nodes, source ranges, parse errors | `README.md`, `lib/src/astnode.h`, `lib/src/ast_reduce.c`, `lib/src/ast_pattern_path.c`, `lib/src/ast_error.c`, `lib/src/result.h`, `lib/src/result.c`, `lib/src/errors.h`, `lib/src/ast.c` |
| `cypher-dsl-src` | Visitor model, query DSL, statement catalog, renderer caching, parser facade | `CypherParser.java`, `Visitable.java`, `Visitor.java`, `Statement.java`, `StatementCatalog.java`, `StatementCatalogBuildingVisitor.java`, `ConfigurableRenderer.java`, `Node.java`, `RelationshipPattern.java` |
| `cypher-shell-src` | Shell parser entrypoint and CLI integration, shallowly inspected | `README.md`, `doc/cypher-shell.1.asciidoc`, file list, `rg` hits for `ShellStatementParser` |
| `clarity-cli` | Code-navigation UX, dependency graph extraction, tree-sitter hybrid parsing | `parser_kotlin.go`, `dependency_resolver_rust.go`, `graph_paths.go`, `show_cmd.go`, `usage-clarity.md`, `file_dependency_graph.go` |
| `codex` | Agent graph store and live file search UX | `agent-graph-store/src/store.rs`, `agent-graph-store/src/types.rs`, `file-search/src/lib.rs` |
| `universal-ctags__ctags` | Tags interchange contract and client metadata | `man/ctags-client-tools.7.rst.in`, `docs/man/ctags-client-tools.7.rst` |

## Grammar Design Patterns

### Pattern: Clause-First Query Spine

Source paths:

- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:32-59`
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:97-125`
- `gitrefrepo/Neo4j family/opencypher-src/grammar/openCypher.bnf:1-68`

Evidence:

- The ANTLR parser is separated from the lexer with `tokenVocab = CypherLexer` and starts at `script`, `query`, `regularQuery`, `singleQuery`, and `standaloneCall`.
- `singleQuery` branches into `singlePart` and `multiPart`.
- `singlePart` admits reading clauses followed by optional updating clauses and optional return; `multiPart` chains query parts through `WITH`.
- The openCypher BNF similarly organizes programs into statements, queries, regular queries, and clause sequences.

Snippet/pseudocode:

```text
script       -> query SEMICOLON? EOF
query        -> regularQuery | standaloneCall
regularQuery -> singleQuery (UNION singleQuery)*
singleQuery  -> singlePart | multiPart
singlePart   -> readingClause* updatingClause* return?
multiPart    -> (readingClause* updatingClause* with)+ singlePart
```

Rust translation:

```rust
pub enum Statement {
    Query(Query),
    StandaloneCall(CallClause),
}

pub enum Query {
    Single(SingleQuery),
    Union {
        all: bool,
        parts: Vec<SingleQueryId>,
    },
}

pub enum SingleQuery {
    SinglePart {
        reading: Vec<ReadingClauseId>,
        updating: Vec<UpdatingClauseId>,
        returning: Option<ReturnClauseId>,
    },
    MultiPart {
        parts: Vec<QueryPart>,
        tail: SinglePartQuery,
    },
}
```

Memory, performance, concurrency, testing implications:

- Store clauses in arenas and pass small `ClauseId` handles instead of recursively boxing every clause. Cypher queries can contain deep expression and pattern trees; arenas keep traversal cache-friendly.
- Keep parser construction independent from AST storage. This allows per-thread parser instances and shared immutable AST arenas after parse.
- Test the spine with TCK-style cases: empty/invalid statements, simple `MATCH RETURN`, `WITH` pipelines, `UNION`, and `CALL`.
- For planner phases, the clause spine is the natural boundary for scope, cardinality estimation, side-effect classification, and read/write separation.

Agentic coding guidance:

- Future agents should start Cypher parser work by implementing this top-level query spine before adding expression details.
- Do not flatten `WITH` into a generic clause list too early. `WITH` is a scope boundary and should be represented as such in the AST and semantic catalog.
- Preserve `standaloneCall` as a first-class statement form; shell/tooling and procedure calls need it.

### Pattern: Expression Precedence Ladder

Source paths:

- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:194-237`
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java:104-118`

Evidence:

- The ANTLR grammar defines expression parsing as a ladder: expression, xor, or, and, not, comparison, additive, multiplicative, power, unary, list, string, null, property, atom.
- Cypher-DSL exposes `parseExpression(String)` as an explicit parser entrypoint, which confirms expressions are a reusable fragment beyond full statement parsing.

Snippet/pseudocode:

```text
expression          -> expression1
expression1         -> expression2 (OR expression2)*
expression2         -> expression3 (XOR expression3)*
expression3         -> expression4 (AND expression4)*
expression4         -> NOT* expression5
expression5         -> expression6 partialComparisonExpression*
expression6         -> expression7 ((+ | -) expression7)*
expression7         -> expression8 ((* | / | %) expression8)*
expression8         -> expression9 (^ expression9)*
```

Rust translation:

```rust
pub enum Expr {
    Binary {
        op: BinaryOp,
        lhs: ExprId,
        rhs: ExprId,
        span: Span,
    },
    Unary {
        op: UnaryOp,
        expr: ExprId,
        span: Span,
    },
    Property {
        base: ExprId,
        key: Symbol,
        span: Span,
    },
    FunctionCall(FunctionCall),
    Parameter(Parameter),
    Literal(Literal),
    PatternComprehension(PatternComprehension),
}

fn parse_expression_binding_power(parser: &mut Parser, min_bp: u8) -> ParseResult<ExprId> {
    // Pratt parser equivalent of the grammar ladder.
    todo!("parse prefix, loop on infix operators by binding power")
}
```

Memory, performance, concurrency, testing implications:

- A Pratt parser in Rust can encode the same ladder without a large stack of mutually recursive functions.
- Preserve source spans per expression node because planner diagnostics often need to underline a subexpression, not just a clause.
- Expression parser should be exposed as a fragment entrypoint for linting, editor completions, DSL tests, and shell parameter handling.
- Test precedence with small AST-shape assertions, not just rendered strings.

Agentic coding guidance:

- When translating from ANTLR to Rust, avoid copying grammar recursion mechanically. Encode operator tables and binding powers once.
- Keep comparison chaining explicit; Cypher's partial comparison expression shape is semantically important for later type checking.

### Pattern: Path Pattern as Alternating Node/Relationship Sequence

Source paths:

- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:270-322`
- `gitrefrepo/Neo4j family/opencypher-src/grammar/openCypher.bnf:280-455`
- `gitrefrepo/libcypher-parser-src/lib/src/ast_pattern_path.c:71-97`
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/RelationshipPattern.java:26-87`

Evidence:

- ANTLR parses `patternPart`, `patternElem`, `patternElemChain`, `nodePattern`, and `relationshipPattern`.
- openCypher BNF separates graph/path patterns, path terms, factors, quantifiers, node patterns, relationship patterns, directions, and relationship length.
- libcypher-parser validates pattern-path shape: elements must be an odd count, nodes at even indexes, relationships at odd indexes.
- Cypher-DSL exposes relationship patterns as a shared interface and includes direction and quantifier handling.

Snippet/pseudocode:

```text
path = node (relationship node)*
relationship = left_arrow? "-" detail? "-" right_arrow?
detail = "[" variable? type_expr? range? properties? "]"
```

Rust translation:

```rust
pub struct PathPattern {
    pub first: NodePatternId,
    pub chains: Vec<PathChain>,
    pub span: Span,
}

pub struct PathChain {
    pub relationship: RelationshipPatternId,
    pub node: NodePatternId,
    pub span: Span,
}

pub enum Direction {
    LeftToRight,
    RightToLeft,
    Undirected,
}

pub struct RelationshipPattern {
    pub variable: Option<Symbol>,
    pub direction: Direction,
    pub type_expression: Option<TypeExpressionId>,
    pub length: Option<RangeQuantifier>,
    pub properties: Option<MapLiteralId>,
    pub span: Span,
}
```

Memory, performance, concurrency, testing implications:

- Store path chains as `Vec<PathChain>` rather than a generic alternating `Vec<PatternElement>`. This encodes the libcypher-parser invariant in the type system and removes repeated runtime checks.
- Direction and variable-length quantifier must be represented before planning; they affect expand operators and cardinality estimates.
- The parser can validate odd alternating structure during construction; semantic validation can then focus on labels, types, variables, and scopes.
- Test malformed paths with recovery: `MATCH (a)-[r]-> RETURN r`, `MATCH (a)-[]-(b)`, `MATCH (a)-->(b)`, variable length, parenthesized/quantified path patterns.

Agentic coding guidance:

- Do not model path patterns as flat token strings. Future graph planners need typed nodes, typed relationships, direction, quantifier, and property maps separately.
- Use libcypher-parser's odd-element invariant as a hard Rust constructor invariant.

### Pattern: Grammar DSL Uses Fields, Supertypes, Conflicts, and Reserved Words

Source paths:

- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/creating-parsers/2-the-grammar-dsl.md:38-145`
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:430-495`

Evidence:

- Tree-sitter grammar DSL provides `seq`, `choice`, `repeat`, precedence, `token`, `alias`, and `field`.
- Public grammar fields include `extras`, `inline`, `conflicts`, `externals`, `precedences`, `word`, `supertypes`, and `reserved`.
- ANTLR Cypher permits many reserved words to appear as symbolic names through the `symbol` rule, not only plain identifiers.

Snippet/pseudocode:

```javascript
module.exports = grammar({
  name: "cypher",
  extras: $ => [/\s/, $.comment],
  word: $ => $.identifier,
  supertypes: $ => [$.clause, $.expression, $.pattern_element],
  conflicts: $ => [
    [$.function_invocation, $.symbolic_name],
  ],
  rules: {
    match_clause: $ => seq(
      field("optional", optional($.optional_kw)),
      "MATCH",
      field("pattern", $.pattern)
    ),
  },
});
```

Rust translation:

```rust
pub enum TokenKind {
    Keyword(Keyword),
    Identifier,
    EscapedIdentifier,
    SymbolicNameKeyword(Keyword),
    // punctuation and literals
}

pub fn parse_symbolic_name(parser: &mut Parser) -> ParseResult<Symbol> {
    match parser.peek() {
        TokenKind::Identifier | TokenKind::EscapedIdentifier => parser.bump_symbol(),
        TokenKind::Keyword(keyword) if keyword.can_be_symbolic_name() => parser.bump_keyword_symbol(),
        _ => parser.expected("symbolic name"),
    }
}
```

Memory, performance, concurrency, testing implications:

- A future tree-sitter Cypher grammar should name fields aggressively. Field names make tree walking, queries, and agent explanations much more reliable.
- Reserved-word-as-name behavior must be tested against the parser and renderer. It is a common source of subtle incompatibility.
- Tree-sitter's `word` and `reserved` mechanisms are useful for keyword extraction and completion. A hand-written Rust lexer should preserve equivalent metadata.

Agentic coding guidance:

- When adding grammar rules, always choose stable field names for semantic children. Agents can then query `field:pattern` or `field:where` instead of brittle child indexes.
- Do not assume every Cypher keyword is forbidden as an identifier. Verify against `symbol`.

### Pattern: TCK as Executable Grammar and Semantic Contract

Source path:

- `gitrefrepo/Neo4j family/opencypher-src/tck/README.adoc:4-238`

Evidence:

- The TCK describes feature files that specify an initial graph, a Cypher query with parameters, expected results or errors, and expected side effects.
- It defines side-effect assertions for nodes, relationships, properties, and labels, and says unspecified side-effect dimensions should be zero.
- Negative tests check errors and no side effects.
- Results are unordered unless `ORDER BY` appears.
- Error scenarios distinguish runtime and compile-time phases.

Snippet/pseudocode:

```gherkin
Scenario: create and return
  Given an empty graph
  When executing query:
    """
    CREATE (n:Person {name: 'Ada'}) RETURN n.name
    """
  Then the result should be:
    | n.name |
    | 'Ada'  |
  And the side effects should be:
    | +nodes      | 1 |
    | +properties | 1 |
    | +labels     | 1 |
```

Rust translation:

```rust
pub struct CypherScenario {
    pub initial_graph: GraphFixture,
    pub parameters: ParameterMap,
    pub query: String,
    pub expectation: ScenarioExpectation,
}

pub enum ScenarioExpectation {
    Result {
        rows: Vec<Row>,
        order_sensitive: bool,
        side_effects: SideEffects,
    },
    Error {
        phase: ErrorPhase,
        error_type: ErrorType,
        side_effects: SideEffects,
    },
}
```

Memory, performance, concurrency, testing implications:

- TCK scenarios should be compiled into deterministic Rust integration tests. The same parser AST can be checked before execution, but semantic correctness needs graph fixtures.
- Keep result-order sensitivity explicit. A planner that accidentally depends on insertion order can pass local tests and fail TCK expectations.
- Use TCK categories as regression suites: syntax only, semantic compile errors, runtime errors, updates, side effects.

Agentic coding guidance:

- Future agents should not invent Cypher behavior from examples. Add failing TCK-derived tests before parser or planner changes.
- If a planned shortcut cannot preserve TCK side-effect accounting, document it as an explicit unsupported gap.

## Lexer and Parser Boundary Patterns

### Pattern: Case-Insensitive Lexer with Hidden Trivia Channels

Source paths:

- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherLexer.g4:33-143`
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherLexer.g4:158-164`

Evidence:

- The ANTLR lexer uses `options { caseInsensitive = true; }`.
- It defines a separate `COMMENTS` channel.
- Whitespace is sent to `HIDDEN`.
- Comments are sent to `COMMENTS`.
- `ERRCHAR` is sent to `HIDDEN`.
- Identifiers admit `ID`, escaped literals, and Unicode letter classes.

Snippet/pseudocode:

```text
lexer CypherLexer
  caseInsensitive = true
  channels { COMMENTS }

SPACES       -> channel(HIDDEN)
LINE_COMMENT -> channel(COMMENTS)
ERRCHAR      -> channel(HIDDEN)
```

Rust translation:

```rust
pub enum TriviaKind {
    Whitespace,
    LineComment,
    BlockComment,
    ErrorChar,
}

pub struct Token {
    pub kind: TokenKind,
    pub lexeme: SmolStr,
    pub span: Span,
}

pub struct Trivia {
    pub kind: TriviaKind,
    pub span: Span,
}

pub struct Lexed {
    pub tokens: Vec<Token>,
    pub trivia: Vec<Trivia>,
    pub diagnostics: Vec<Diagnostic>,
}
```

Memory, performance, concurrency, testing implications:

- Keep trivia out of the parser hot path while retaining spans for formatter/linter/editor features.
- Case-insensitive keyword matching should not allocate uppercase copies for every token. Use ASCII-insensitive matching for keywords plus Unicode-aware identifier scanning.
- Comments need a distinct channel if future tooling wants doc extraction, formatting, or preserving comments in AST-to-source rendering.
- Test mixed-case keywords, escaped identifiers, Unicode identifiers, invalid characters, and comments between every significant token class.

Agentic coding guidance:

- Preserve invalid characters as diagnostics with spans. Do not silently drop them; ANTLR hides them, but a Rust compiler-quality parser should report them.
- Separate tokenization correctness from parse correctness with dedicated lexer snapshot tests.

### Pattern: Fragment Parser Entry Points

Source path:

- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java:36-211`

Evidence:

- Cypher-DSL exposes parser methods for node pattern, relationship pattern, expression, clause, statement, and statement list.
- `parseExpression`, `parseNode`, and `parseRelationship` are first-class APIs, not only internal parser helpers.
- Parse errors are mapped through a common `handle` function that normalizes parser exceptions.

Snippet/pseudocode:

```text
parseNode(input)         -> Node
parseRelationship(input) -> Relationship
parseExpression(input)   -> Expression
parseClause(input)       -> Clause
parseStatement(input)    -> Statement
parse(input)             -> List<Statement>
```

Rust translation:

```rust
pub struct CypherParser {
    interner: SymbolInterner,
}

impl CypherParser {
    pub fn parse_statement(&mut self, input: &str) -> ParseResult<StatementId>;
    pub fn parse_expression(&mut self, input: &str) -> ParseResult<ExprId>;
    pub fn parse_node_pattern(&mut self, input: &str) -> ParseResult<NodePatternId>;
    pub fn parse_relationship_pattern(&mut self, input: &str) -> ParseResult<RelationshipPatternId>;
    pub fn parse_clause(&mut self, input: &str) -> ParseResult<ClauseId>;
}
```

Memory, performance, concurrency, testing implications:

- Fragment entrypoints make it easier to build editor features, DSL helpers, shell commands, and precise tests.
- Each entrypoint should verify EOF after the fragment; otherwise a helper parse can succeed on a prefix and hide errors.
- Shared parser state should be per invocation or per thread. AST arenas may be returned inside a `ParsedModule`/`ParsedStatement` container so handles cannot outlive storage.

Agentic coding guidance:

- Build parser APIs from user workflows, not only grammar convenience. Agents, IDEs, and tests need expression-level and pattern-level parse functions.
- Route all fragment errors through one diagnostic normalizer.

### Pattern: Shell Parser Separates Commands from Cypher

Source paths:

- `gitrefrepo/Neo4j family/cypher-shell-src/README.md:1-84`
- `gitrefrepo/Neo4j family/cypher-shell-src/doc/cypher-shell.1.asciidoc:5-12`
- Shallow `rg` hits in `cypher-shell/src/main/java/org/neo4j/shell/parser/StatementParser.java` and `ShellStatementParser.java`

Evidence:

- Cypher Shell is a command-line shell for Neo4j over Bolt.
- The parser layer includes `StatementParser` and `ShellStatementParser`; the latter is described by local search hits as a Cypher-aware parser that can detect shell commands (`:`-prefixed) or Cypher.
- Non-interactive shell code feeds text incrementally to `statementParser.parseMoreText` and consumes complete statements.

Snippet/pseudocode:

```text
input line stream
  if line starts with ":" in command position -> shell command
  else accumulate Cypher text until statement terminator
  emit complete shell command or Cypher statement
```

Rust translation:

```rust
pub enum ShellItem {
    Command(ShellCommand),
    Cypher(StringSpan),
    Incomplete,
}

pub struct ShellStatementAccumulator {
    buffer: String,
}

impl ShellStatementAccumulator {
    pub fn push_line(&mut self, line: &str) -> ShellParseResult<Vec<ShellItem>> {
        todo!("detect command prefix, semicolon completion, quoted semicolons")
    }
}
```

Memory, performance, concurrency, testing implications:

- The shell parser should be separate from the Cypher grammar parser. Shell commands and incremental line buffering have different recovery and completion rules.
- Tests should include semicolons inside strings, multi-line statements, comments, `:` commands, and mixed command/query sessions.
- Because this repo was shallowly inspected, treat this as a navigation hint rather than a full source-backed design.

Agentic coding guidance:

- Do not mix shell command syntax into the core Cypher parser. Add an outer shell accumulator.
- Future agents should inspect `ShellStatementParser.java` directly before implementing detailed shell compatibility.

## AST Typed Node Patterns

### Pattern: Base Node plus Typed Wrappers

Source paths:

- `gitrefrepo/libcypher-parser-src/lib/src/astnode.h:28-240`
- `gitrefrepo/libcypher-parser-src/lib/src/ast_reduce.c:24-144`
- `gitrefrepo/libcypher-parser-src/lib/src/ast.c:360-447`
- `gitrefrepo/libcypher-parser-src/lib/src/ast.c:930-944`

Evidence:

- libcypher-parser has a base AST node with type, children, child count, source range, ordinal, and annotations.
- It uses vtables for node-specific behavior such as parent types, name, detail string, release, and clone.
- Typed node constructors validate child types before allocation.
- Typed getters enforce node kind before returning typed child pointers.
- Public APIs expose node type, type string, source range, child count, and child access.

Snippet/pseudocode:

```c
struct cypher_astnode {
    const struct cypher_astnode_vt *vt;
    enum cypher_astnode_type type;
    const struct cypher_astnode **children;
    unsigned int nchildren;
    struct cypher_input_range range;
    unsigned int ordinal;
    unsigned int annotations;
};

struct reduce {
    struct cypher_astnode _astnode;
    const cypher_astnode_t *accumulator;
    const cypher_astnode_t *identifier;
    const cypher_astnode_t *expression;
};
```

Rust translation:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AstId(u32);

pub struct AstNode {
    pub kind: AstKind,
    pub span: Span,
    pub ordinal: u32,
    pub children: SmallVec<[AstId; 4]>,
}

pub struct ReduceExpr {
    pub id: AstId,
    pub accumulator: ExprId,
    pub identifier: Symbol,
    pub expression: ExprId,
}

impl AstArena {
    pub fn reduce_expr(&self, id: AstId) -> Option<ReduceExprView<'_>> {
        self.expect_kind(id, AstKind::ReduceExpression)?;
        todo!("return typed view over children")
    }
}
```

Memory, performance, concurrency, testing implications:

- In Rust, an enum-rich typed AST can replace vtables for most nodes. A separate generic `AstNode` index is still valuable for diagnostics and generic traversal.
- Child lists should use `SmallVec` or fixed typed fields for common small nodes. Cypher ASTs have many short child lists.
- Use typed constructor functions to preserve invariants. Avoid public mutation of raw child arrays.
- Immutable AST arenas can be `Send + Sync` if they store owned data, stable IDs, and interned symbols.
- Test constructors with invalid children at parser-boundary unit tests; most internal constructors can be private.

Agentic coding guidance:

- Future agents should not use untyped `Vec<Node>` as the only AST representation. A Neo4j-in-Rust rewrite needs typed AST views for semantic analysis and planning.
- Preserve a generic node layer for agents, graph extraction, AST dumps, and diagnostic tooling.

### Pattern: Parse Result Owns Roots, Errors, Directives, and Node Count

Source paths:

- `gitrefrepo/libcypher-parser-src/lib/src/result.h:24-38`
- `gitrefrepo/libcypher-parser-src/lib/src/result.c:25-185`
- `gitrefrepo/libcypher-parser-src/lib/src/errors.h:23-50`
- `gitrefrepo/libcypher-parser-src/README.md:139-153`

Evidence:

- libcypher-parser parse results store parse errors, roots, directives, total node count, and EOF flag.
- Public APIs provide root count/root access, node count, directive access, error count/error access, and freeing.
- Parse errors include position, message, context, and context offset.
- The README API example shows parse, inspect result counts, and free result.

Snippet/pseudocode:

```c
result = cypher_parse(input, NULL, NULL, 0)
nnodes = cypher_parse_result_nnodes(result)
nerrors = cypher_parse_result_nerrors(result)
root = cypher_parse_result_get_root(result, 0)
cypher_parse_result_free(result)
```

Rust translation:

```rust
pub struct ParseOutput {
    pub roots: Vec<StatementId>,
    pub arena: AstArena,
    pub diagnostics: Vec<Diagnostic>,
    pub directives: Vec<Directive>,
    pub reached_eof: bool,
}

impl ParseOutput {
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }
}
```

Memory, performance, concurrency, testing implications:

- Return one owning parse output object so AST IDs cannot outlive the arena.
- Include node counts and diagnostic counts in parser benchmarks and fuzz telemetry.
- Directives matter if the implementation supports parser options or Cypher shell directives later; keep an extension slot even if initially empty.
- Tests should assert both AST shape and diagnostic shape.

Agentic coding guidance:

- Agents should add parser outputs as durable artifacts in failing tests. The output should include AST dump, diagnostics, root count, and node count.

### Pattern: Stable Semantic Entity Keys Separate Identity from Location

Source paths:

- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:56-122`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:164-212`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:478-515`

Evidence:

- Parseltongue's generator returns parsed entities, dependency edges, and warnings.
- `ParsedEntity` carries name, type, line range, content, signature, doc comments, metadata, visibility, semantic path, parent scope, and hash.
- `format_key` creates a key from language, type, sanitized name, semantic path, and a birth timestamp.
- Comments explain that keys intentionally avoid line ranges to remain stable during refactors.
- Core `CodeEntity` stores `isgl1_key`, temporal state, signature, current/future code, metadata, `birth_timestamp`, content hash, and semantic path.

Snippet/pseudocode:

```text
key = language ":" entity_type ":" sanitized_name ":" semantic_path ":T" birth_timestamp
```

Rust translation:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SemanticKey(String);

pub struct AstEntity {
    pub key: SemanticKey,
    pub kind: EntityKind,
    pub symbol: Symbol,
    pub semantic_path: SmallVec<[Symbol; 4]>,
    pub span: Span,
    pub birth_revision: RevisionId,
    pub content_hash: ContentHash,
}
```

Memory, performance, concurrency, testing implications:

- For code intelligence, line spans are locations, not identities. Stable semantic keys reduce churn in dependency graphs and agent memory.
- For Cypher planning, the same idea applies to schema entities, procedures, variables, aliases, catalog objects, and prepared statement cache keys.
- Stable keys should be compactly interned; do not store repeated long strings on every edge.
- Test key stability under whitespace changes, line movement, doc comment edits, and local body changes.

Agentic coding guidance:

- When future agents build code-navigation storage, avoid `file:line` as primary key. Use semantic identity plus spans as mutable evidence.
- Add "birth revision" or equivalent if entity identity needs to survive movement.

## Visitor and Catalog Patterns

### Pattern: Enter/Leave Visitor Contract

Source paths:

- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/ast/Visitable.java:28-52`
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/ast/Visitor.java:27-57`
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/Statement.java:46-149`

Evidence:

- `Visitable` has a default `accept` implementation that calls visitor enter and leave.
- Helper methods visit nullable children.
- `Visitor` supports `enter`, `enterWithResult`, and `leave`.
- `Statement` exposes catalog analysis and Cypher rendering as visitor-driven operations.

Snippet/pseudocode:

```java
visitor.enter(this);
visitChildren(visitor);
visitor.leave(this);
```

Rust translation:

```rust
pub enum VisitControl {
    Continue,
    SkipChildren,
}

pub trait AstVisitor {
    fn enter(&mut self, node: AstId, arena: &AstArena) -> VisitControl {
        let _ = (node, arena);
        VisitControl::Continue
    }

    fn leave(&mut self, node: AstId, arena: &AstArena) {
        let _ = (node, arena);
    }
}

pub fn walk_ast(visitor: &mut dyn AstVisitor, arena: &AstArena, root: AstId) {
    if matches!(visitor.enter(root, arena), VisitControl::Continue) {
        for child in arena.children(root) {
            walk_ast(visitor, arena, child);
        }
    }
    visitor.leave(root, arena);
}
```

Memory, performance, concurrency, testing implications:

- Visitor objects should be per traversal. They often carry mutable state such as scope stacks.
- Immutable AST arenas allow multiple visitors to run concurrently as long as visitors do not share mutable state.
- Traversal order should be deterministic. Stable order supports reproducible rendering, diagnostics, and agent evidence.
- Test visitor order with small fixture ASTs and enter/leave traces.

Agentic coding guidance:

- Future agents should add new analysis as visitors before modifying core AST nodes. That keeps parser data structures stable.

### Pattern: Statement Catalog Builder Tracks Tokens, Properties, Relations, Parameters, and Scopes

Source paths:

- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/StatementCatalog.java:35-260`
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/internal/StatementCatalogBuildingVisitor.java:49-605`

Evidence:

- `StatementCatalog` exposes labels, relationship types, outgoing/incoming/undirected relationships, properties, filters, identifiable expressions, parameters, parameter names, and literals.
- `StatementCatalogBuildingVisitor` warns that it is not thread-safe and should be newly instantiated per invocation.
- The builder tracks current clause, pattern element stack, tokens, properties, filters, scopes, relationships, and literals.
- Relationship collection switches on direction and populates incoming, outgoing, and undirected maps.
- The final catalog is immutable.

Snippet/pseudocode:

```text
visit node pattern:
  record symbolic name
  record labels

visit relationship:
  record symbolic name
  record relationship types
  connect source/target based on direction

visit property:
  record property lookup for owner

finish:
  return immutable catalog
```

Rust translation:

```rust
pub struct StatementCatalog {
    pub labels: IndexSet<LabelName>,
    pub relationship_types: IndexSet<RelTypeName>,
    pub properties: IndexMap<Symbol, IndexSet<PropertyKey>>,
    pub parameters: IndexSet<ParameterName>,
    pub relationships: Vec<CatalogRelationship>,
    pub scopes: Vec<ScopeSummary>,
}

pub struct CatalogBuilder<'a> {
    ast: &'a AstArena,
    scope_stack: Vec<ScopeId>,
    pattern_stack: Vec<PatternElementId>,
    catalog: MutableCatalog,
}
```

Memory, performance, concurrency, testing implications:

- Catalog extraction is semantically richer than AST walking. Keep it as a separate immutable product so planner and tooling can share it.
- Use `IndexMap`/`IndexSet` or sorted vectors for deterministic output.
- Builder is naturally single-threaded per statement because of scope stacks, but multiple statements can be cataloged in parallel.
- Test direction classification and scope boundaries carefully: `(a)-[r]->(b)`, `(a)<-[r]-(b)`, `(a)-[r]-(b)`, `WITH`, subqueries, aliases, parameters.

Agentic coding guidance:

- Do not force the planner to rediscover labels, relationship types, parameters, and properties on every pass. Build a catalog once after parsing and semantic validation.
- Catalog snapshots are ideal evidence artifacts for agents: small, structured, and stable.

### Pattern: Renderer Cache Uses Statement Identity and Locks

Source path:

- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/renderer/ConfigurableRenderer.java:44-128`

Evidence:

- `ConfigurableRenderer` stores renderer configurations in a concurrent map.
- It uses a statement cache with default size 128.
- Rendering statement nodes uses read/write locks around the cache.
- Non-statement visitables are rendered without the statement cache.

Snippet/pseudocode:

```text
render(statement):
  hash = statement.hashCode
  read lock:
    if cache hit: return cypher
  write lock:
    render with visitor
    cache[hash] = cypher
    return cypher
```

Rust translation:

```rust
pub struct Renderer {
    cache: parking_lot::RwLock<lru::LruCache<StatementFingerprint, Arc<str>>>,
}

impl Renderer {
    pub fn render_statement(&self, parsed: &ParsedStatement) -> Arc<str> {
        let key = parsed.fingerprint();
        if let Some(rendered) = self.cache.read().get(&key).cloned() {
            return rendered;
        }
        let rendered = Arc::<str>::from(self.render_uncached(parsed));
        self.cache.write().put(key, rendered.clone());
        rendered
    }
}
```

Memory, performance, concurrency, testing implications:

- Statement render caching is useful for DSL-generated queries and prepared statement logging.
- Use a structural fingerprint, not pointer identity, if ASTs can be rebuilt.
- Cache values as `Arc<str>` to avoid repeated string clones across threads.
- Test cache invalidation with statements that differ only in parameters, labels, or generated names.

Agentic coding guidance:

- Future agents should separate rendering configuration from parser semantics. Renderer cache bugs should not affect AST identity or planner behavior.

## Query DSL and Tree-Sitter Extraction Patterns

### Pattern: Query-Based Extraction Replaces Hand-Written Walkers

Source paths:

- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:1-15`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:475-543`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/entity_queries/rust.scm:4-32`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/dependency_queries/rust.scm:1-180`

Evidence:

- Parseltongue comments state that query-based extraction across `.scm` files replaced manual walking, fixed multiple languages, reduced code, and used the tree-sitter query system.
- Extraction first attempts `extract_entities`; Rust receives metadata enrichment; dependencies are extracted with queries; failures degrade to warnings.
- Rust entity queries capture functions, structs, enums, traits, impls, methods, and modules with captures such as `@definition.function` and `@name`.
- Rust dependency queries capture direct calls, method calls, imports, trait impls, type references, awaits, field access, iterator operations, and map them conceptually to calls/uses/implements.

Snippet/pseudocode:

```scheme
(function_item
  name: (identifier) @name) @definition.function

(call_expression
  function: (identifier) @reference.call) @call

(use_declaration
  argument: (_) @reference.import) @import
```

Rust translation:

```rust
pub struct QueryExtractor {
    entity_queries: HashMap<Language, CompiledQuery>,
    dependency_queries: HashMap<Language, CompiledQuery>,
}

impl QueryExtractor {
    pub fn extract_entities(
        &self,
        language: Language,
        tree: &Tree,
        source: &str,
    ) -> Result<Vec<ParsedEntity>, ExtractError> {
        todo!("run query captures and build typed entities")
    }
}
```

Memory, performance, concurrency, testing implications:

- Query files are data, so agents can add extraction behavior without changing Rust walker code.
- Compile queries once per language and reuse them. Query compilation should not happen per file.
- Capture names become an API; changing them should break tests.
- Query-based extraction may miss semantic cases. Pair it with fixture tests and selective language-specific enrichment.

Agentic coding guidance:

- For a future code-navigation tool, make `queries/*.scm` first-class source files with tests, not hidden constants.
- When adding a capture, update the graph edge mapping in the same change.

### Pattern: Thread-Local Parser and Extractor Caches

Source paths:

- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:24-54`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:228-240`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:273-353`
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:362-378`

Evidence:

- Parseltongue stores parsers in a `thread_local!` `HashMap<Language, Parser>`.
- Comments state tree-sitter `Parser` is not `Send`, and thread-local parsers avoid mutex contention.
- A thread-local `QueryBasedExtractor` is lazily initialized.
- Parse calls use the thread-local parser and call `parser.parse(source, None)`.

Snippet/pseudocode:

```rust
thread_local! {
    static THREAD_PARSERS: RefCell<HashMap<Language, Parser>> = RefCell::new(HashMap::new());
    static THREAD_EXTRACTOR: RefCell<Option<QueryBasedExtractor>> = RefCell::new(None);
}
```

Rust translation for rewrite:

```rust
thread_local! {
    static CYPHER_TREE_SITTER_PARSER: RefCell<Option<tree_sitter::Parser>> = RefCell::new(None);
}

pub fn with_cypher_parser<T>(f: impl FnOnce(&mut tree_sitter::Parser) -> T) -> T {
    CYPHER_TREE_SITTER_PARSER.with(|slot| {
        let mut slot = slot.borrow_mut();
        let parser = slot.get_or_insert_with(make_cypher_parser);
        f(parser)
    })
}
```

Memory, performance, concurrency, testing implications:

- Thread-local parser caches work well for file indexing and parallel batch extraction.
- They are less suitable for async request handling if work migrates across threads; keep parse calls synchronous inside blocking worker pools.
- Query extractors should be immutable after initialization where possible. If mutable caches are needed, make mutation per thread.
- Test under parallel indexing with many languages/files to catch accidental shared parser usage.

Agentic coding guidance:

- Do not put tree-sitter `Parser` in a global mutex. Use per-thread caches or parser pools.
- Explicitly document parser send/sync assumptions in the module that owns parser creation.

### Pattern: Tree-Sitter Tags and Captures as Navigation API

Source paths:

- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/4-code-navigation.md:3-137`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:1002-1063`

Evidence:

- Tree-sitter code navigation uses query files with captures for definitions and references.
- Standard captures include roles such as `@definition.*`, `@reference.*`, and `@name`.
- Optional doc captures can be associated with definitions.
- The CLI supports `tree-sitter tags` and tag query tests.
- C API exposes query capture names, quantifiers, query cursors, and match/capture iteration.

Snippet/pseudocode:

```scheme
(function_declaration
  name: (identifier) @name) @definition.function

(identifier) @reference.variable
```

Rust translation:

```rust
pub enum CaptureRole {
    Definition(EntityKind),
    Reference(ReferenceKind),
    Name,
    Documentation,
}

pub fn classify_capture(name: &str) -> Option<CaptureRole> {
    if let Some(kind) = name.strip_prefix("definition.") {
        return Some(CaptureRole::Definition(kind.parse().ok()?));
    }
    if let Some(kind) = name.strip_prefix("reference.") {
        return Some(CaptureRole::Reference(kind.parse().ok()?));
    }
    (name == "name").then_some(CaptureRole::Name)
}
```

Memory, performance, concurrency, testing implications:

- Capture names are a compact interchange format between grammar, extraction, tags, and graph storage.
- Query cursors can be range-limited for incremental updates; do not re-query entire large files after small edits.
- Capture quantifier metadata matters for optional/repeated captures; handle it to avoid panics on missing names.
- Add tests for query files themselves, not only downstream graph results.

Agentic coding guidance:

- Agents should search query files before editing extractor code. Many navigation bugs are capture-shape bugs.
- Maintain a capture naming convention document for Cypher grammar and code indexing.

### Pattern: Tree-Sitter-Graph Converts Syntax Captures to Arbitrary Graphs

Source paths:

- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/README.md:5-54`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/graph.rs:35-100`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/graph.rs:143-173`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/graph.rs:262-345`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/execution.rs:31-65`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/execution.rs:103-260`

Evidence:

- tree-sitter-graph is a DSL for constructing arbitrary graphs from tree-sitter-parsed source.
- Graph lifetime is tied to the syntax tree.
- Syntax nodes are only added when referenced; graph nodes and edges are separate.
- Graph edges are de-duplicated by binary search and stored sorted.
- Attributes support scalar values, lists, sets, syntax nodes, and graph nodes.
- Execution can run into an existing graph, supports lazy/strict modes, globals, functions, and match metadata.

Snippet/pseudocode:

```text
source tree + graph DSL
  -> query matches
  -> graph nodes
  -> graph edges
  -> attributes attached to nodes/edges
```

Rust translation:

```rust
pub struct ExtractedGraph {
    pub syntax_refs: Vec<SyntaxRef>,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

pub struct SyntaxRef {
    pub file_id: FileId,
    pub kind: SyntaxKind,
    pub span: Span,
}

pub enum GraphValue {
    String(Symbol),
    Integer(i64),
    Syntax(SyntaxRefId),
    Node(GraphNodeId),
    List(Vec<GraphValue>),
    Set(IndexSet<GraphValue>),
}
```

Memory, performance, concurrency, testing implications:

- Tree-tied graph refs are excellent for in-memory extraction, but persisted graphs must store file ID, spans, syntax kind, and content hash rather than raw tree nodes.
- De-duplicated sorted edges give deterministic output and cheap idempotent updates.
- Attribute maps are flexible but can become schema-less. Persisted graph stores should validate edge/node schemas.
- Test graph DSL scripts with golden graph JSON and sorted deterministic output.

Agentic coding guidance:

- For code navigation, consider a two-phase model: tree-tied extraction graph first, then normalized persisted graph facts.
- Do not persist tree-sitter node handles. Persist spans and stable semantic keys.

## Symbol Extraction Patterns

### Pattern: Hybrid Query, Manual Walk, and Fallback Extraction

Source paths:

- `clarity-cli/depgraph/languages/kotlin/parser_kotlin.go:157-195`
- `clarity-cli/depgraph/languages/kotlin/parser_kotlin.go:198-267`
- `clarity-cli/depgraph/languages/kotlin/parser_kotlin.go:270-357`
- `clarity-cli/depgraph/languages/kotlin/parser_kotlin.go:413-520`

Evidence:

- Clarity's Kotlin parser reads source, initializes tree-sitter queries, gets a parser from a `sync.Pool`, parses, queries imports, and falls back when query paths fail.
- It compiles primary and fallback queries with `sync.Once`.
- Query cursor results are filtered with predicates.
- Package extraction uses tree-sitter first and regex fallback.
- Top-level type names are extracted by manually walking named children.
- Type identifiers are extracted through compiled queries and de-duplicated.

Snippet/pseudocode:

```text
parse imports:
  tree = parser.parse(source)
  matches = query(import_query, tree)
  if no reliable matches:
    matches = query(fallback_query, tree)
  if still failed:
    regex fallback for package/import forms
```

Rust translation:

```rust
pub struct ExtractionPipeline {
    primary_query: Query,
    fallback_query: Query,
}

impl ExtractionPipeline {
    pub fn extract_symbols(&self, source: &str) -> ExtractionReport {
        let tree_report = self.extract_with_tree_sitter(source);
        if tree_report.is_sufficient() {
            return tree_report;
        }
        self.extract_with_fallback(source, tree_report)
    }
}
```

Memory, performance, concurrency, testing implications:

- Hybrid extraction is pragmatic for code intelligence, especially when grammars are incomplete.
- Fallbacks should be explicit and observable. Return an extraction report with warnings and coverage flags.
- Parser pools and compiled query `OnceLock`s prevent repeated setup cost.
- Test fallback paths deliberately by feeding partial/malformed files and grammar edge cases.

Agentic coding guidance:

- Do not hide fallback behavior. Agents need to know whether graph facts came from precise syntax or heuristic extraction.
- Add provenance to extracted edges: `tree_sitter_query`, `manual_tree_walk`, `regex_fallback`.

### Pattern: ctags Pseudo-Tags as Interchange Metadata

Source paths:

- `git-ref-repo/ignore-this-folder-repos/universal-ctags__ctags/man/ctags-client-tools.7.rst.in:21-208`
- `git-ref-repo/ignore-this-folder-repos/universal-ctags__ctags/docs/man/ctags-client-tools.7.rst:21-208`

Evidence:

- ctags distinguishes regular tags for language objects from pseudo-tags describing how the tags file was generated.
- `TAG_KIND_DESCRIPTION` provides language-specific kind metadata.
- `--extras=+p` enables pseudo-tags.
- `--fields=+E` adds extras metadata so clients can distinguish pseudo-tags.
- `TAG_EXTRA_DESCRIPTION` lets client tools confirm required extras were enabled.
- `TAG_FIELD_DESCRIPTION` describes fields such as file, input, name, pattern, typeref, and language-specific fields.

Snippet/pseudocode:

```text
TAG_KIND_DESCRIPTION!Language<TAB>kind<TAB>name<TAB>description
TAG_FIELD_DESCRIPTION!field<TAB>description
regular-tag-name<TAB>path<TAB>pattern;"<TAB>kind...
```

Rust translation:

```rust
pub struct TagFileMetadata {
    pub generator: Option<String>,
    pub kind_descriptions: Vec<KindDescription>,
    pub field_descriptions: Vec<FieldDescription>,
    pub extras: IndexSet<String>,
}

pub enum TagRecord {
    Pseudo(PseudoTag),
    Regular(SymbolTag),
}
```

Memory, performance, concurrency, testing implications:

- If the rewrite exports code-navigation facts, include metadata records that describe schema and extraction capabilities.
- Clients should reject or degrade gracefully when required metadata is missing.
- Pseudo metadata avoids hard-coded assumptions about language kinds and fields.
- Test importer compatibility with missing pseudo-tags, duplicate pseudo-tags, and unknown fields.

Agentic coding guidance:

- Agents consuming tag-like outputs should read metadata first, then facts.
- For a new Rust graph index format, include a schema header inspired by ctags pseudo-tags.

### Pattern: Rust Import Resolver Encodes Language Semantics beyond Syntax

Source path:

- `clarity-cli/depgraph/languages/rust/dependency_resolver_rust.go:13-260`

Evidence:

- Clarity's Rust resolver uses `sync.Map` caches.
- Import resolution handles parsed import kinds, self-filtering, de-duplication, module candidates, `crate`, `self`, `super`, sibling submodules, local modules, and dependency crate roots.
- Crate-root lookup is cached.

Snippet/pseudocode:

```text
resolve use path:
  if starts crate:: -> crate root
  if starts self::  -> current module
  if starts super:: -> parent module
  else try sibling module, local module, dependency crate
  dedupe resolved files
```

Rust translation:

```rust
pub struct NameResolver {
    crate_roots: DashMap<CrateName, FileId>,
    module_cache: DashMap<ModulePath, Vec<FileId>>,
}

impl NameResolver {
    pub fn resolve_reference(&self, scope: ScopeId, reference: Reference) -> Vec<EntityId> {
        todo!("syntax facts plus language semantic rules")
    }
}
```

Memory, performance, concurrency, testing implications:

- Dependency graphs need semantic resolution, not only syntax extraction.
- Caches should key on normalized module paths and revision IDs.
- For Cypher, the equivalent semantic resolver resolves variables, aliases, labels, relationship types, functions, procedures, parameters, and schema names.
- Test resolution separately from parse extraction.

Agentic coding guidance:

- Future agents should resist treating syntax references as resolved graph edges. Add an unresolved-reference layer and a resolver phase.

## Dependency Graph Storage Patterns

### Pattern: Typed Dependency Edge Facts with Source Locations

Source paths:

- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:613-619`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:1042-1135`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:1160-1218`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:342-401`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:704-754`

Evidence:

- Parseltongue defines `Location { file_path, line, character }`.
- `EdgeType` includes `Calls`, `Uses`, and `Implements` with string conversions.
- `DependencyEdge` stores `from_key`, `to_key`, `edge_type`, and optional `source_location`.
- The edge builder requires from, to, and edge type.
- Cozo client batch-inserts `DependencyEdges` and can retrieve all dependencies with source location.

Snippet/pseudocode:

```text
DependencyEdges {
  from_key,
  to_key,
  edge_type
  =>
  source_location
}
```

Rust translation:

```rust
pub struct DependencyEdge {
    pub from: EntityId,
    pub to: EntityId,
    pub kind: DependencyKind,
    pub evidence: EdgeEvidence,
}

pub struct EdgeEvidence {
    pub span: Option<Span>,
    pub extraction: ExtractionProvenance,
}
```

Memory, performance, concurrency, testing implications:

- Edge facts should store source evidence so navigation can explain "why is this connected?"
- Batch insert is required for large indexes; single-edge writes are too slow for full repo refreshes.
- Edge kind should be an enum in Rust and a stable string/code at storage boundaries.
- Test empty batch fast paths and duplicate/updated edge behavior.

Agentic coding guidance:

- Every graph edge an agent uses should be explainable by path, span, capture, and extractor phase.
- If source location is absent, mark the edge as synthetic or inferred.

### Pattern: Bounded Blast Radius via Recursive Graph Query

Source paths:

- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:403-528`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:576-670`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:756-865`

Evidence:

- `calculate_blast_radius(changed_key, max_hops)` uses a bounded recursive graph query.
- The direct rule starts from edges where `from_key == $start_key`.
- The recursive rule expands while distance is below `$max_hops`.
- The query returns minimum distance ordered by distance.
- Separate methods compute forward dependencies, reverse dependencies, and unbounded transitive closure.
- Comments warn that unbounded result growth can be expensive and bounded queries are preferred for large graphs.

Snippet/pseudocode:

```text
reachable(to, 1) :-
  edge(start, to)

reachable(to, dist + 1) :-
  reachable(from, dist),
  edge(from, to),
  dist < max_hops

result(node, min(distance))
```

Rust translation:

```rust
pub fn blast_radius(
    graph: &DependencyGraph,
    start: EntityId,
    max_hops: u8,
    direction: Direction,
) -> Vec<ReachableNode> {
    let mut queue = VecDeque::from([(start, 0)]);
    let mut best = IndexMap::<EntityId, u8>::new();
    while let Some((node, depth)) = queue.pop_front() {
        if depth == max_hops {
            continue;
        }
        for edge in graph.neighbors(node, direction) {
            let next_depth = depth + 1;
            if best.get(&edge.to).is_none_or(|old| next_depth < *old) {
                best.insert(edge.to, next_depth);
                queue.push_back((edge.to, next_depth));
            }
        }
    }
    best.into_iter().map(|(node, depth)| ReachableNode { node, depth }).collect()
}
```

Memory, performance, concurrency, testing implications:

- Always offer bounded traversal defaults in agent tools. Unbounded graph expansion can swamp context windows.
- Store distances and direction so UI can explain impact.
- For concurrent reads, immutable snapshots or MVCC storage avoid traversal seeing partially updated graph states.
- Test cycles, duplicate paths, max depth zero/one, reverse traversal, and disconnected nodes.

Agentic coding guidance:

- Agentic code navigation should start with bounded blast radius and expand on demand.
- Include distance in output; agents can prioritize direct neighbors over transitive context.

### Pattern: File Graph with Cycles and Path Filters

Source paths:

- `clarity-cli/depgraph/file_dependency_graph.go:11-180`
- `clarity-cli/depgraph/graph_paths.go:3-128`
- `clarity-cli/cmd/show/show_cmd.go:73-113`
- `clarity-cli/cmd/show/show_cmd.go:340-356`
- `clarity-cli/cmd/show/show_cmd.go:466-507`
- `clarity-cli/cmd/show/show_cmd.go:737-753`
- `clarity-cli/cmd/show/show_cmd.go:1173-1215`

Evidence:

- Clarity's `FileDependencyGraph` stores files, edges, and cycles plus metadata such as language, extension, directory, and test status.
- It computes strongly connected components for cycles.
- `FindPathNodes` returns all nodes on any path between selected files, using forward/reverse reachability.
- `show` supports output format, repo, commit, URL, direction/reach, inputs/excludes/extensions, `--between`, target file, levels, scope, prune, additional files, labels, and stats.
- The command validates mutually exclusive selection modes and defaults to uncommitted changes when no target is provided.

Snippet/pseudocode:

```text
between(a, b):
  forward = reachable_from(a)
  reverse = can_reach(b)
  path_nodes = intersection(forward, reverse)
```

Rust translation:

```rust
pub struct FileDependencyGraph {
    pub files: IndexMap<FileId, FileNode>,
    pub edges: Vec<FileEdge>,
    pub cycles: Vec<Vec<FileId>>,
}

pub fn nodes_between(graph: &FileDependencyGraph, endpoints: &[FileId]) -> IndexSet<FileId> {
    todo!("forward and reverse reachability intersections across endpoint pairs")
}
```

Memory, performance, concurrency, testing implications:

- File-level graphs are coarser than entity-level graphs but excellent for UI and review workflows.
- Cycle detection should be part of graph build output. Cycles affect refactoring order and planner module boundaries.
- Path filters reduce graph output dramatically and are context-window friendly.
- Test `--between` on diamond graphs, cycles, no-path pairs, and bidirectional dependency sets.

Agentic coding guidance:

- For agent tools, expose both entity-level and file-level graphs. File-level graph gives fast orientation; entity-level graph gives precision.
- Add "why" commands that show the source edge evidence for direct dependencies.

### Pattern: Agent Graph Store with Stable Traversal Ordering and Edge Status

Source paths:

- `codex/codex-rs/agent-graph-store/src/store.rs:7-54`
- `codex/codex-rs/agent-graph-store/src/types.rs:4-41`

Evidence:

- Codex defines a storage-neutral agent graph store boundary.
- Implementations are expected to return stable ordering for list methods.
- The store can upsert edges, set status, list direct children with optional status, and list descendants breadth-first by depth then thread id.
- Status can be open or closed and is serialized in snake_case.
- Descendant traversal with status filters applies the filter to every traversed edge, so closed branches are excluded when querying open paths.

Snippet/pseudocode:

```text
upsert_edge(parent, child)
set_edge_status(parent, child, open|closed)
list_children(parent, status?)
list_descendants(parent, status?) -> breadth-first stable order
```

Rust translation:

```rust
pub enum NavEdgeStatus {
    Open,
    Closed,
}

pub trait NavigationGraphStore {
    fn upsert_edge(&self, parent: NavNodeId, child: NavNodeId) -> Result<()>;
    fn set_status(&self, parent: NavNodeId, child: NavNodeId, status: NavEdgeStatus) -> Result<()>;
    fn children(&self, parent: NavNodeId, filter: Option<NavEdgeStatus>) -> Result<Vec<NavNodeId>>;
    fn descendants(&self, parent: NavNodeId, filter: Option<NavEdgeStatus>) -> Result<Vec<NavNodeId>>;
}
```

Memory, performance, concurrency, testing implications:

- Stable traversal order is important for reproducible agent plans and snapshots.
- Edge status lets agents close explored branches without deleting evidence.
- Breadth-first ordering by depth aligns with progressive context expansion.
- Test status-filtered descendants through mixed open/closed trees.

Agentic coding guidance:

- Code-navigation tools should let agents mark paths as explored/closed. This prevents repeated traversal of irrelevant branches.

## Source Span Patterns

### Pattern: Multi-Granularity Source Locations

Source paths:

- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:77-131`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:523-590`
- `gitrefrepo/libcypher-parser-src/lib/src/errors.h:23-29`
- `gitrefrepo/libcypher-parser-src/lib/src/ast_error.c:42-83`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:282-341`

Evidence:

- Tree-sitter exposes `TSPoint`, `TSRange`, and `TSInputEdit`.
- Nodes expose start/end byte and point, plus flags for missing, extra, changed, has error, and is error.
- libcypher-parser errors include position, message, context, and context offset.
- libcypher-parser error AST nodes store an error string and source range.
- Parseltongue `LineRange` uses 1-based inclusive start/end and validates ranges; external dependencies use a `0-0` marker.

Snippet/pseudocode:

```text
span:
  start_byte, end_byte
  start_line, start_col
  end_line, end_col
  file_id
  flags: missing | extra | changed | has_error | is_error
```

Rust translation:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Span {
    pub file: FileId,
    pub start_byte: u32,
    pub end_byte: u32,
    pub start: LineCol,
    pub end: LineCol,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LineCol {
    pub line: u32,
    pub column: u32,
}

pub enum SpanKind {
    Source(Span),
    External,
    Synthetic,
}
```

Memory, performance, concurrency, testing implications:

- Store byte offsets for slicing and line/column for diagnostics. Recomputing line/column repeatedly is expensive.
- External and synthetic facts need explicit markers; do not fake source spans.
- Changed/error flags are useful for incremental parsing and IDE diagnostics.
- Test UTF-8 and Unicode identifier spans; byte offsets and character columns can diverge.

Agentic coding guidance:

- Every evidence item in the graph should point back to a span or explicitly say `External`/`Synthetic`.
- Agents should prefer byte spans for automated edits and line/column for human-facing reports.

### Pattern: Tree Cursor for Fast Full-Tree Walks

Source path:

- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/using-parsers/4-walking-trees.md:3-37`

Evidence:

- Tree-sitter documentation says `TSTreeCursor` is the fastest way to walk many nodes.
- Cursor APIs move to first child, next sibling, and parent while exposing current node and field.

Snippet/pseudocode:

```c
cursor = ts_tree_cursor_new(root)
goto_first_child
goto_next_sibling
goto_parent
```

Rust translation:

```rust
pub fn walk_tree_fast(root: tree_sitter::Node<'_>, mut f: impl FnMut(tree_sitter::Node<'_>)) {
    let mut cursor = root.walk();
    loop {
        let node = cursor.node();
        f(node);
        if cursor.goto_first_child() {
            continue;
        }
        while !cursor.goto_next_sibling() {
            if !cursor.goto_parent() {
                return;
            }
        }
    }
}
```

Memory, performance, concurrency, testing implications:

- Cursor walks avoid allocating child vectors during full-tree scans.
- Query captures are often better for targeted extraction; cursors are better for generic AST dumps, structural validation, and fallback extraction.
- Cursor objects are traversal state and should not be shared.

Agentic coding guidance:

- Use queries for semantic facts; use cursor walks for diagnostics, debugging, and fallback coverage.

## Incremental Parsing Patterns

### Pattern: Edit Old Tree, Reparse with Old Tree, Then Query Changed Ranges

Source paths:

- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/using-parsers/3-advanced-parsing.md:5-31`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:117-131`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:447-484`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:701-709`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:1100-1138`

Evidence:

- Tree-sitter incremental parsing requires editing the old tree with `TSInputEdit`, then parsing the new source with the old tree.
- Cached syntax nodes must be edited separately if stored.
- APIs expose changed ranges between old and new trees.
- Query cursors can be limited to byte or point ranges.
- Nodes can be found by descendant byte/point range.

Snippet/pseudocode:

```text
old_tree.edit(input_edit)
new_tree = parser.parse(new_source, old_tree)
changed_ranges = old_tree.changed_ranges(new_tree)
for range in changed_ranges:
  run extraction queries limited to range
  update graph facts overlapping range
```

Rust translation:

```rust
pub fn update_file_incremental(
    parser: &mut tree_sitter::Parser,
    old: &mut ParsedFile,
    edit: InputEdit,
    new_source: &str,
) -> IncrementalUpdate {
    old.tree.edit(&edit);
    let new_tree = parser.parse(new_source, Some(&old.tree)).expect("parse");
    let changed = old.tree.changed_ranges(&new_tree).collect::<Vec<_>>();
    let affected = old.graph.facts_overlapping_ranges(&changed);
    let new_facts = extract_ranges(&new_tree, new_source, &changed);
    IncrementalUpdate { new_tree, affected, new_facts }
}
```

Memory, performance, concurrency, testing implications:

- Incremental update must delete or invalidate facts overlapping changed ranges before adding new facts.
- If facts use stable semantic keys, moved unchanged entities can keep identity while spans update.
- Tree-sitter trees are cheap to copy, but individual trees are not thread-safe; copy for use on another thread.
- Test edits at beginning/middle/end, multi-byte characters, inserted/deleted lines, and edits that change parse structure outside the immediate byte range.

Agentic coding guidance:

- Do not rebuild whole repository graphs on every small edit. Keep a file-level incremental path and a full rebuild escape hatch.
- Store enough fact provenance to remove stale facts by file and span.

### Pattern: Live Search Session Separates Walker, Matcher, Query Updates, and Cancellation

Source path:

- `codex/codex-rs/file-search/src/lib.rs:41-640`

Evidence:

- Codex file search defines `FileMatch`, `FileSearchSnapshot`, and options including root, query, limit, gitignore handling, and require-git behavior.
- `FileSearchSession.update_query` is cheap relative to re-walking.
- Session creation builds a Nucleo matcher and spawns matcher and walker threads.
- The walker checks cancellation/shutdown periodically and sends walk-complete signals.
- The matcher processes query updates, can append when the query has a prefix relation, ticks the matcher, sorts snapshots by score descending then path ascending, and reports updates.
- Completion is coordinated with locks and a condition variable.

Snippet/pseudocode:

```text
walker thread:
  enumerate files
  periodically check shutdown
  send walk_complete

matcher thread:
  on query updated:
    update matcher pattern
  on nucleo notify:
    produce sorted snapshot
```

Rust translation for code navigation:

```rust
pub struct NavSearchSession {
    query_tx: crossbeam_channel::Sender<SearchSignal>,
    latest: Arc<RwLock<SearchSnapshot>>,
}

pub enum SearchSignal {
    QueryUpdated(String),
    IndexUpdated(FileId),
    Shutdown,
}
```

Memory, performance, concurrency, testing implications:

- Code-navigation UIs should not block on full graph/index walks. Return progressive snapshots.
- Stable secondary sorting is essential for non-jittery UI and reproducible agent context.
- Cancellation checks should be frequent but not per file if that becomes too expensive; Codex uses periodic checks in the walker.
- Test query update races, shutdown during walk, sorted output stability, and completion notification.

Agentic coding guidance:

- Agent tools should expose partial results with `walk_complete`/`index_complete` flags.
- Future agents should prefer updating search query state over restarting the whole scan.

## Error Recovery Patterns

### Pattern: Keep Error Nodes in the AST

Source paths:

- `gitrefrepo/libcypher-parser-src/lib/src/ast_error.c:42-83`
- `gitrefrepo/libcypher-parser-src/lib/src/ast.c:360-377`
- `gitrefrepo/libcypher-parser-src/README.md:61-90`
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:566-590`

Evidence:

- libcypher-parser has a dedicated AST error node type.
- Error nodes store string value and source range.
- The README shows AST output that includes an error node after a parse error.
- Tree-sitter exposes missing, extra, has-error, and is-error flags on nodes.

Snippet/pseudocode:

```text
parse invalid query:
  diagnostics += parse error at line/column/offset
  ast includes ERROR node with range
  later tooling can still inspect partial tree
```

Rust translation:

```rust
pub enum AstKind {
    Statement,
    MatchClause,
    ReturnClause,
    Expr,
    Error,
}

pub struct ErrorNode {
    pub message: SmolStr,
    pub span: Span,
}
```

Memory, performance, concurrency, testing implications:

- Error nodes let IDEs and agents continue extracting partial context from invalid files.
- Semantic analysis should skip or quarantine subtrees with parse errors.
- Diagnostics and error nodes must share spans so UI can correlate them.
- Test partial parses with missing identifiers, broken patterns, incomplete strings, and invalid expressions.

Agentic coding guidance:

- Never return "no AST" for a syntax error if partial structure is available. Agents need partial context for repair.
- Mark facts extracted from error-containing subtrees as low confidence.

### Pattern: Error Phase and Side-Effect Contracts

Source path:

- `gitrefrepo/Neo4j family/opencypher-src/tck/README.adoc:100-238`

Evidence:

- TCK negative tests expect no side effects.
- Error scenarios include phase information: runtime or compile time.
- Side effects are observable by subsequent Cypher queries.

Snippet/pseudocode:

```text
compile error:
  no graph mutation

runtime error:
  scenario specifies expected side effects, usually none
```

Rust translation:

```rust
pub enum ErrorPhase {
    Lex,
    Parse,
    Semantic,
    Plan,
    Runtime,
}

pub struct QueryError {
    pub phase: ErrorPhase,
    pub code: ErrorCode,
    pub diagnostic: Diagnostic,
}
```

Memory, performance, concurrency, testing implications:

- Planner/executor must define transaction boundaries around errors. Parser and semantic errors cannot mutate graph state.
- Compile-time errors should be detected before write operators are built.
- Runtime errors need transactional rollback semantics or explicit partial side-effect rules.
- Test error phase and side effects together, not separately.

Agentic coding guidance:

- Future agents should tag every error with phase. This improves test triage and prevents planner/runtime confusion.

## Graph Navigation UX Patterns

### Pattern: Focused Graph Views over Whole-Repo Dumps

Source paths:

- `clarity-cli/usage-clarity.md:9-153`
- `clarity-cli/cmd/show/show_cmd.go:73-113`
- `clarity-cli/cmd/show/show_cmd.go:466-507`
- `clarity-cli/cmd/show/show_cmd.go:737-753`
- `clarity-cli/cmd/show/show_cmd.go:1173-1215`

Evidence:

- Clarity's usage docs emphasize live impact views, focused snapshots, and design checks.
- Commands include `diff`, `languages`, `setup`, `show`, `watch`, `why`, and `workspace`.
- `show` can visualize uncommitted changes, a commit, files/directories, reachability around a file, paths between files, and URL output.
- The command defaults to uncommitted changes when no target is given.
- It supports pruning, levels, labels, and statistics.

Snippet/pseudocode:

```text
clarity show
clarity show -c HEAD
clarity show src/foo.rs --reach both --depth 2
clarity show --between a.rs,b.rs
clarity why a.rs b.rs
```

Rust translation:

```rust
pub enum GraphViewRequest {
    ChangedFiles { base: GitRef },
    Reach { file: FileId, direction: ReachDirection, depth: u8 },
    Between { files: Vec<FileId> },
    Why { from: FileId, to: FileId },
}
```

Memory, performance, concurrency, testing implications:

- Focused views are necessary for context-window management and human comprehension.
- `why` should be backed by direct edge evidence and source spans.
- Watch mode should reuse incremental parsing and dependency updates.
- Test graph view requests against fixture graphs with expected node/edge sets.

Agentic coding guidance:

- Agentic navigation tools should default to focused changed-file context, then let agents expand by reach/depth.
- Whole-tree dumps should require explicit opt-in.

### Pattern: Progressive Context Expansion by Breadth and Evidence

Source paths:

- `codex/codex-rs/agent-graph-store/src/store.rs:33-54`
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:403-528`
- `clarity-cli/depgraph/graph_paths.go:3-128`

Evidence:

- Codex descendant listing is breadth-first by depth then thread id.
- Parseltongue blast radius returns graph nodes by minimum distance.
- Clarity path filtering finds nodes that lie between endpoints.

Snippet/pseudocode:

```text
agent context:
  start with exact node/file
  add direct dependencies
  add direct dependents
  add path-between target nodes
  expand depth only when needed
```

Rust translation:

```rust
pub struct ContextExpansionPolicy {
    pub max_depth: u8,
    pub max_nodes: usize,
    pub include_reverse_edges: bool,
    pub require_source_evidence: bool,
}

pub fn select_agent_context(
    graph: &DependencyGraph,
    seed: &[GraphNodeId],
    policy: ContextExpansionPolicy,
) -> Vec<GraphNodeId> {
    todo!("BFS with evidence and budget filters")
}
```

Memory, performance, concurrency, testing implications:

- Context expansion should be budget-aware. Sort by distance, edge evidence strength, and semantic kind.
- Source-backed edges should rank above inferred edges when context is tight.
- The graph snapshot should be immutable during context selection.
- Test budget cutoffs deterministically.

Agentic coding guidance:

- Agents should ask graph tools for "next ring" rather than "everything." This mirrors the evidence-backed patterns from Codex, Parseltongue, and Clarity.

## Recommended Rust Architecture for a Neo4j-in-Rust Rewrite

This section synthesizes the evidence into a concrete module layout. It is a recommendation, not a claim about existing code.

```text
crates/
  cypher-lexer/
    token.rs
    trivia.rs
    diagnostic.rs
  cypher-parser/
    parser.rs
    expression.rs
    pattern.rs
    clause.rs
    recovery.rs
  cypher-ast/
    arena.rs
    ids.rs
    nodes.rs
    span.rs
    dump.rs
  cypher-catalog/
    visitor.rs
    scope.rs
    statement_catalog.rs
  cypher-semantics/
    resolver.rs
    type_checker.rs
    diagnostics.rs
  cypher-plan/
    logical_plan.rs
    operators.rs
    cardinality.rs
  code-index/
    tree_sitter_queries/
    extractor.rs
    tags.rs
    incremental.rs
  graph-store/
    entities.rs
    edges.rs
    cozo.rs
    traversal.rs
  agent-nav/
    context_selection.rs
    graph_views.rs
    why.rs
```

Key design choices:

- Parser output owns AST arena, diagnostics, directives, and roots, following libcypher-parser's result ownership pattern.
- AST uses typed Rust enums/views plus generic `AstId` traversal, following libcypher-parser's base-node and typed-wrapper split.
- Cypher query structure starts with the clause-first spine from ANTLR/openCypher.
- Expressions use a Pratt parser equivalent of the ANTLR precedence ladder.
- Paths use typed `first node + chains` representation, enforcing the alternating node/relationship invariant at construction.
- Statement catalog is a separate immutable product, following Cypher-DSL.
- Tree-sitter/code-index extraction uses query files and capture conventions, following tree-sitter tags and Parseltongue.
- Graph edges store source evidence and extraction provenance, following Parseltongue and tree-sitter-graph.
- Agent navigation defaults to focused graph views, bounded blast radius, path filters, and progressive BFS, following Clarity, Codex, and Parseltongue.

## Testing Matrix

| Area | Source-backed reason | Suggested tests |
|---|---|---|
| Lexer | ANTLR case-insensitive lexer, hidden comments, Unicode IDs | Mixed case keywords, escaped IDs, Unicode, invalid chars, comments/trivia |
| Query spine | ANTLR/openCypher clause structure | `MATCH RETURN`, `WITH`, `UNION`, `CALL`, read/write combinations |
| Expressions | ANTLR precedence ladder and fragment parser | Operator precedence AST shape, function calls, params, property access, comparisons |
| Patterns | openCypher path BNF, libcypher alternating invariant | Directed/undirected paths, variable length, quantified paths, malformed path recovery |
| AST result | libcypher parse result with roots/errors/node counts | Multiple statements, partial AST on error, node count, diagnostics |
| Catalog | Cypher-DSL immutable statement catalog | Labels, rel types, properties, params, direction, scopes, aliases |
| TCK behavior | openCypher TCK README | Result rows, ordering, side effects, compile/runtime errors |
| Extraction queries | tree-sitter tags and Parseltongue `.scm` queries | Capture snapshots, missing optional captures, dependency edge mapping |
| Incremental parsing | tree-sitter edit/reparse/changed ranges | Insert/delete/move edits, stale fact removal, changed-range extraction |
| Graph traversal | Parseltongue blast radius, Clarity paths, Codex BFS | Depth bounds, cycles, reverse edges, status filters, path-between |
| UX | Clarity focused show/watch/why and Codex live search | Partial results, stable ordering, cancellation, why evidence |

## Agentic Coding Guidance Summary

- Start from source-backed grammar contracts. Use ANTLR/openCypher for syntax shape and TCK for behavior.
- Keep parse, semantic catalog, planner, graph extraction, and navigation as separate phases with explicit artifacts.
- Preserve spans everywhere. Source evidence is the currency of agentic navigation.
- Use query files for code-intelligence extraction where possible; use manual walkers/fallbacks only with provenance.
- Prefer bounded graph traversal and focused views to whole-repo dumps.
- Treat graph-tool output as a map, not proof. Verify claims with direct source reads before encoding patterns.
- Add regression tests at the same abstraction level as each change: lexer snapshots, AST shape, catalog shape, TCK scenarios, graph fact snapshots, navigation view snapshots.

## Explicit Gaps and Uninspected Areas

- CodeGraphContext was used as required, but only successfully indexed the current working repo. Its wrapper reported that `gitrefrepo/` references were not present in indexed outputs, so this document does not cite CodeGraphContext for vendored reference repo claims.
- codebase-memory was used as required and listed the current project, but a follow-up query failed with a project-selection mismatch. I did not rely on it for source claims.
- I did not index every repository under the wildcard `*tree-sitter*`; I inspected the canonical `tree-sitter__tree-sitter` and `tree-sitter__tree-sitter-graph` repos directly because they are high-value for parser and graph extraction patterns.
- I did not inspect every `*code*graph*` repo under the Parseltongue reference folder. I inspected Parseltongue, Clarity, Codex, and tree-sitter-graph as representative high-value code graph systems.
- `cypher-shell-src` was shallowly inspected through README, manpage, file discovery, and search hits. Detailed implementation claims about `ShellStatementParser.java` should be verified by a future worker before compatibility work.
- `universal-ctags__ctags` was inspected through client-tool documentation, not parser internals.
- I did not run openCypher TCK scenarios, cargo tests, Java tests, Go tests, or tree-sitter query tests. This slice is an evidence corpus, not an execution report.
- I did not inspect the full Neo4j server codebase. This document focuses on parser/tooling/reference repos assigned to Worker 4.
- I did not build a Cypher tree-sitter grammar. The tree-sitter guidance here is translated from tree-sitter's grammar/query APIs and the existing Cypher grammar references.

## Evidence Appendix

High-value direct source references:

- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:1-15` query extraction architecture comments.
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:24-54` thread-local parser and extractor caches.
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:164-212` stable ISGL key generation.
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/src/isgl1_generator.rs:475-543` query extraction and graceful warnings.
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/entity_queries/rust.scm:4-32` Rust entity captures.
- `parseltongue-rust-LLM-companion/crates/pt01-folder-to-cozodb-streamer/dependency_queries/rust.scm:1-180` Rust dependency captures.
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:478-515` `CodeEntity` identity fields.
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/entities.rs:1042-1135` dependency edge model.
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:342-401` batch edge insert.
- `parseltongue-rust-LLM-companion/crates/parseltongue-core/src/storage/cozo_client.rs:403-528` bounded blast radius.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/creating-parsers/2-the-grammar-dsl.md:38-145` grammar DSL features.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/using-parsers/3-advanced-parsing.md:5-161` incremental parsing and threading.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/using-parsers/4-walking-trees.md:3-37` tree cursor walk.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/docs/src/4-code-navigation.md:3-137` tags and captures.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:77-131` points, ranges, edits.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:447-590` changed ranges, node spans, error flags.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter/lib/include/tree_sitter/api.h:1002-1166` query cursor and range APIs.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/README.md:5-54` graph DSL overview and tests.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/graph.rs:35-173` graph, syntax nodes, edges.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/graph.rs:262-345` graph attributes and values.
- `git-ref-repo/ignore-this-folder-repos/tree-sitter__tree-sitter-graph/src/execution.rs:31-260` graph DSL execution.
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherLexer.g4:33-164` lexer options, channels, tokens, Unicode.
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:32-59` query parser entry.
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:97-125` single/multi-part query.
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:194-237` expression ladder.
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:270-322` pattern grammar.
- `gitrefrepo/antlr-grammars-v4-src/cypher/CypherParser.g4:430-495` symbolic names and reserved words.
- `gitrefrepo/Neo4j family/opencypher-src/grammar/openCypher.bnf:1-68` program/query/clause grammar.
- `gitrefrepo/Neo4j family/opencypher-src/grammar/openCypher.bnf:280-455` graph/path pattern grammar.
- `gitrefrepo/Neo4j family/opencypher-src/tck/README.adoc:4-238` TCK scenario, result, error, and side-effect contracts.
- `gitrefrepo/libcypher-parser-src/lib/src/astnode.h:28-240` base AST node and vtables.
- `gitrefrepo/libcypher-parser-src/lib/src/ast_reduce.c:24-144` typed reduce node constructor/getters.
- `gitrefrepo/libcypher-parser-src/lib/src/result.h:24-38` parse result structure.
- `gitrefrepo/libcypher-parser-src/lib/src/result.c:25-185` parse result APIs and freeing.
- `gitrefrepo/libcypher-parser-src/lib/src/errors.h:23-50` parse error/tracker structures.
- `gitrefrepo/libcypher-parser-src/lib/src/ast_pattern_path.c:71-97` alternating path invariant.
- `gitrefrepo/libcypher-parser-src/lib/src/ast_error.c:42-83` error AST node.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl-parser/src/main/java/org/neo4j/cypherdsl/parser/CypherParser.java:36-211` parser facade and fragment entrypoints.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/ast/Visitable.java:28-52` visitable default.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/ast/Visitor.java:27-57` visitor contract.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/Statement.java:46-203` statement, catalog, rendering API.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/StatementCatalog.java:35-260` catalog API.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/internal/StatementCatalogBuildingVisitor.java:49-605` catalog builder internals.
- `gitrefrepo/Neo4j family/cypher-dsl-src/neo4j-cypher-dsl/src/main/java/org/neo4j/cypherdsl/core/renderer/ConfigurableRenderer.java:44-128` renderer cache.
- `clarity-cli/depgraph/languages/kotlin/parser_kotlin.go:157-520` tree-sitter extraction, queries, fallbacks.
- `clarity-cli/depgraph/languages/rust/dependency_resolver_rust.go:13-260` Rust dependency resolver.
- `clarity-cli/depgraph/file_dependency_graph.go:11-180` file graph and cycles.
- `clarity-cli/depgraph/graph_paths.go:3-128` path-between graph filter.
- `clarity-cli/cmd/show/show_cmd.go:73-1215` graph view flags and filters.
- `clarity-cli/usage-clarity.md:9-153` navigation UX commands.
- `codex/codex-rs/agent-graph-store/src/store.rs:7-54` agent graph store boundary.
- `codex/codex-rs/agent-graph-store/src/types.rs:4-41` edge status type.
- `codex/codex-rs/file-search/src/lib.rs:41-640` live file search session architecture.
- `git-ref-repo/ignore-this-folder-repos/universal-ctags__ctags/man/ctags-client-tools.7.rst.in:21-208` ctags client metadata and pseudo-tags.

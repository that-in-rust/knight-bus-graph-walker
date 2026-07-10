# Neo4j-To-Rust Exact-Surface Rewrite: Quantitative Feasibility

<!-- markdownlint-disable MD013 -->

Date: 2026-07-10

Status: measured feasibility study and decision input; not an implementation
commitment

## Question

Could Knight Bus first reproduce Neo4j Community and Neo4j GDS in Rust with
the same externally observable APIs, tests, algorithms, and broadly similar
architecture, postpone storage innovation, and later add low-RAM graph-shaped
storage as an optional backend?

Could Rust ownership, native execution, aggressive parallelism, and Linux
`io_uring` make that initial compatible clone meaningfully faster?

How much code, test evidence, LLM context, engineering effort, and elapsed time
would that actually require?

## Answer First

The idea is technically possible only after defining a narrower meaning of
"same Neo4j."

The strongest feasible target is:

```text
same official drivers
+ same Bolt/PackStream behavior
+ same selected Cypher behavior
+ same selected GDS procedure names, modes, configs, and results
+ a Rust-native internal engine
+ a replaceable GraphStore/GraphView backend
```

The following target is not available from the local evidence:

```text
all Neo4j Community
+ all Enterprise behavior
+ every embedded Java API and plugin ABI
+ exact Neo4j store-file compatibility
+ clustering and operational behavior
+ every GDS Enterprise feature
```

Neo4j Community explicitly says Enterprise contains closed-source components
not present in its repository. The GDS README likewise says the distributed GDS
product combines this open repository with a suite of closed sources.

The literal Community-plus-OpenGDS corpus is not weekend-sized:

| Measured scope | Source-like lines | Raw `o200k_base` tokens |
| --- | ---: | ---: |
| Neo4j Community server | 2,127,716 | 18,353,485 |
| OpenGDS | 531,701 | 4,498,494 |
| **Core total** | **2,659,417** | **22,851,979** |
| All 20 local Neo4j-family repositories | 3,982,335 | 33,694,228 |

Across the two core repositories, the heuristic split is almost exactly half
implementation and half tests:

| Core split | Files | Lines | Raw tokens |
| --- | ---: | ---: | ---: |
| Non-test source | 9,808 | 1,340,927 | 11,211,844 |
| Test source | 5,905 | 1,318,490 | 11,640,135 |

This test density is valuable. It does not mean the tests can execute unchanged
against Rust: most compile against Java/Scala classes and inspect Neo4j
internals.

The high-confidence verdict is:

1. A weekend can produce a convincing wire-level tracer bullet.
2. A full external-compatibility Community plus OpenGDS product is a multi-year,
   multi-team program.
3. A same-architecture Rust port has no grounded reason to deliver a large
   median-latency win merely because it is Rust.
4. `io_uring` is relevant to cold and queued storage I/O, not the hot path of an
   all-in-RAM GDS algorithm.
5. The useful version of the proposal is a **behavioral compatibility rewrite**
   with replaceable storage, delivered one vertical surface at a time.

## 1. Scope Of The Local Evidence

The folder `gitrefrepo/Neo4j family/` contains 20 repositories. They do not all
belong inside a server rewrite.

### 1.1 Product implementation

| Repository | Role |
| --- | --- |
| `neo4j-src` | Neo4j Community database, kernel, Cypher, Bolt, storage, indexes, transactions, server, tools |
| `neo4j-gds-src` | Open source GDS graph catalog, projections, procedures, algorithms, ML, estimation, adapters |

### 1.2 Language-neutral or external conformance evidence

| Repository | Role | Directly useful against a Rust server? |
| --- | --- | --- |
| `opencypher-src` | openCypher grammar and TCK | Yes; Cucumber feature scenarios can drive another implementation |
| `neo4j-testkit-src` | Official driver integration/conformance system | Partly; it primarily tests drivers and expects a configured Neo4j server |
| `neo4j-docs-bolt-src` | Bolt documentation source | Yes as protocol documentation, not a full executable server TCK |

### 1.3 Clients that should remain unchanged

| Repository | Role |
| --- | --- |
| `neo4j-java-driver-src` | Official Java Bolt driver |
| `neo4j-python-driver-src` | Official Python Bolt driver |
| `neo4j-go-driver-src` | Official Go Bolt driver |
| `neo4j-javascript-driver-src` | Official JavaScript Bolt driver |
| `neo4j-dotnet-driver-src` | Official .NET Bolt driver |

A compatible Rust server should be tested by these existing drivers. Rewriting
the drivers would remove independent oracles and create unnecessary scope.

### 1.4 Adjacent ecosystem, not the initial server core

| Repository | Role |
| --- | --- |
| `neo4j-apoc-src`, `neo4j-apoc-procedures-src` | APOC procedures and integrations |
| `neo4j-browser-src` | Browser user interface |
| `cypher-shell-src` | Command-line client |
| `cypher-dsl-src` | Java Cypher construction DSL |
| `neo4j-ogm-src` | Java object-graph mapper |
| `neo4j-gds-client-src` | Python GDS client |
| `gds-agent-src` | Agent/MCP integration |
| `neo4rs-src` | Independent Rust Neo4j driver |
| `graph-data-science-src` | Small documentation/data repository in this clone |

These projects are compatibility consumers or optional ecosystem products. A
Rust server does not need to port them to let their network clients connect.

## 2. Revisions Actually Present

Counts and conclusions apply to these local checkouts, not an abstract latest
Neo4j:

| Repository | Branch | Commit | Commit date | Clone depth |
| --- | --- | --- | --- | --- |
| `neo4j-src` | `release/5.26.0` | `c68156edf24` | 2024-12-03 | Full history |
| `neo4j-gds-src` | `2.13` | `dc4417b3c1` | 2026-05-12 | Full history |
| `neo4j-testkit-src` | `6.x` | `ec46b65` | 2026-05-21 | Shallow |
| `opencypher-src` | `main` | `677cbaf` | 2026-03-20 | Shallow |

OpenGDS `2.13` declares Neo4j `5.26.26` as its dependency. The server checkout
is the earlier `5.26.0` release branch head. The API line is aligned, but the
exact patch sources are not identical.

The local server is also not Neo4j's current 2026 product line. A rewrite must
choose a frozen target release, otherwise the source oracle moves while the
port is underway.

## 3. Measurement Method

### 3.1 Line count

The source count used Git-tracked files only and included these source/test
extensions:

```text
.java .scala .kt .kts .rs .go .py .js .jsx .ts .tsx .cs
.c .cc .cpp .cxx .h .hh .hpp .sh .rb .feature .cypher .cql .g4
```

The count is physical lines, including blank and comment lines. It is not a
logical-statement count and is intentionally reproducible rather than falsely
precise about programmer effort.

Test classification used conventional path/name markers such as `src/test`,
`integrationTest`, `tests`, `testkit`, and names ending in `Test`, `IT`, or
`Spec`. Some generated, fixture, benchmark, and unusual source sets can be
misclassified. The aggregate remains sufficient for order-of-magnitude work
pricing.

### 3.2 Token count

Each tracked source-like file was tokenized with `tiktoken` `o200k_base`.
This is an estimate for modern OpenAI-family tokenization, not a promise about
the exact tokenizer or billing policy of any particular Codex model.

The measured average was roughly four to five source bytes per token. Token
counts include implementation and tests but exclude most prose, build files,
binary fixtures, dependency source, generated build output, conversation, and
tool-call overhead.

### 3.3 All tracked text

Across all 20 repositories there are approximately **6,917,461 lines of
tracked text** after files containing NUL bytes are classified as binary.
The narrower source/test count is **3,982,335 lines**. The difference is docs,
configuration, lock files, generated metadata, datasets, and other text that
still matters operationally but should not be priced as handwritten engine
code.

## 4. Complete Local Repository Count

| Repository | Source files | Source/test lines | Source bytes | `o200k` tokens |
| --- | ---: | ---: | ---: | ---: |
| `cypher-dsl-src` | 674 | 80,186 | 2,670,502 | 676,802 |
| `cypher-shell-src` | 134 | 19,257 | 639,098 | 140,424 |
| `gds-agent-src` | 40 | 12,387 | 502,011 | 104,155 |
| `graph-data-science-src` | 5 | 193 | 5,154 | 1,359 |
| `neo4j-apoc-procedures-src` | 859 | 163,688 | 6,716,003 | 1,497,314 |
| `neo4j-apoc-src` | 470 | 105,996 | 4,537,029 | 1,005,018 |
| `neo4j-browser-src` | 687 | 92,100 | 2,775,800 | 695,397 |
| `neo4j-docs-bolt-src` | 1 | 13 | 287 | 70 |
| `neo4j-dotnet-driver-src` | 878 | 106,631 | 3,769,693 | 811,367 |
| `neo4j-gds-client-src` | 806 | 106,059 | 3,808,826 | 846,179 |
| `neo4j-gds-src` | 4,904 | 531,701 | 19,758,208 | 4,498,494 |
| `neo4j-go-driver-src` | 211 | 52,781 | 1,584,912 | 443,246 |
| `neo4j-java-driver-src` | 880 | 92,690 | 3,492,529 | 721,447 |
| `neo4j-javascript-driver-src` | 589 | 143,422 | 4,463,524 | 1,086,806 |
| `neo4j-ogm-src` | 1,129 | 105,146 | 3,587,949 | 819,212 |
| `neo4j-python-driver-src` | 428 | 109,786 | 3,501,857 | 824,147 |
| `neo4j-src` | 10,809 | 2,127,716 | 80,509,160 | 18,353,485 |
| `neo4j-testkit-src` | 248 | 63,590 | 2,240,238 | 518,189 |
| `neo4rs-src` | 135 | 30,675 | 926,069 | 231,276 |
| `opencypher-src` | 223 | 38,318 | 1,472,970 | 419,841 |
| **Total** | **24,110** | **3,982,335** | **146,961,819** | **33,694,228** |

## 5. Core Codebase Anatomy

### 5.1 Neo4j Community languages

| Language | Files | Lines |
| --- | ---: | ---: |
| Java | 7,966 | 1,384,775 |
| Scala | 2,754 | 707,594 |
| Gherkin | 65 | 27,574 |
| ANTLR | 4 | 6,932 |
| Shell and Cypher source | 20 | 841 |

Java-to-Rust translation is therefore not the whole job. Roughly one third of
the server corpus is Scala, concentrated heavily in Cypher planning and
execution.

### 5.2 Largest Neo4j Community areas

| Area | Files | Physical lines | Why it matters |
| --- | ---: | ---: | --- |
| Cypher | 2,887 | 877,132 | Parser, semantics, rewrites, planner, plans, runtimes, compatibility suites |
| Community integration tests | 1,055 | 261,303 | Cross-module correctness, recovery, server, indexes, import, Bolt |
| Kernel | 910 | 121,729 | Transactions, lifecycle, recovery, indexes, database internals |
| Record storage engine | 547 | 96,165 | Records, batch import, counts, storage operations |
| Kernel tests | 345 | 78,066 | White-box kernel behavior |
| Bolt | 664 | 72,982 | Protocol messages, negotiation, FSM, auth, transactions, streaming |
| Kernel API | 346 | 35,297 | Internal/public kernel boundary |
| Values | 174 | 33,192 | Cypher and protocol value semantics |
| Import utilities | 214 | 32,198 | Bulk construction and staging |
| Server | 270 | 30,028 | HTTP/server composition and runtime |
| I/O | 170 | 24,123 | Page/cache/channel and filesystem abstractions |
| Index | 102 | 21,735 | Index lifecycle and behavior |
| Fabric | 141 | 20,646 | Multi-database/federated query behavior |
| WAL | 113 | 13,823 | Durable transaction logging |

The root contains approximately **161 Maven POM modules**. A line-for-line port
would inherit a very large module graph before delivering one complete user
path.

### 5.3 OpenGDS languages and areas

OpenGDS is almost entirely Java: 4,898 Java files and 529,960 Java lines. The
largest areas are:

| Area | Files | Physical lines | Role |
| --- | ---: | ---: | --- |
| `algo` | 726 | 87,393 | Algorithm implementations and tests |
| `core` | 380 | 77,464 | Graph representations, compression, construction, shared runtime |
| `proc` | 637 | 71,416 | Procedure-facing surface and integration |
| `procedures` | 852 | 61,485 | Generated/facade procedure architecture |
| `applications` | 308 | 34,584 | Graph catalog and algorithm application services |
| `ml` | 322 | 31,898 | Machine-learning algorithms and model behavior |
| `pipeline` | 168 | 18,366 | ML pipeline configuration and lifecycle |
| `io` | 124 | 13,946 | Import/export and storage-facing behavior |
| `collections` | 68 | 10,626 | Large primitive collections |
| `pregel` | 43 | 5,888 | Extensible vertex-centric algorithm API |
| `native-projection` | 46 | 5,379 | Projection from Neo4j storage |

The build exposes roughly **131 included Gradle projects**. The local readiness
registry contains 575 GDS operations. A raw annotation search finds 602
`@Procedure` occurrences and 23 `@UserFunction` occurrences, including wrappers,
tests, generated/facade surfaces, and duplicates.

## 6. Code-Graph Evidence

`codebase-memory-mcp 0.9.0` was installed and both core repositories were
indexed directly in `fast` mode. Direct indexing avoids the parent workspace's
reference-repository exclusions.

### 6.1 Neo4j Community graph

| Metric | Count |
| --- | ---: |
| Semantic nodes | 218,480 |
| Dependency/usage edges | 1,186,989 |
| Methods | 99,195 |
| Functions | 22,858 |
| Classes | 21,126 |
| Interfaces | 2,332 |
| Indexed files | 10,915 |
| `CALLS` edges | 319,346 |
| `USAGE` edges | 356,208 |
| `IMPORTS` edges | 90,051 |
| `TESTS` edges identified by the index | 4,577 |

The graph's largest package is Cypher by a wide margin, followed by community
integration tests, kernel, record storage, Bolt, kernel APIs, values, I/O,
server, and Fabric. The kernel boundary alone has 1,408 recorded calls into
`kernel-api`.

### 6.2 OpenGDS graph

| Metric | Count |
| --- | ---: |
| Semantic nodes | 54,265 |
| Dependency/usage edges | 284,022 |
| Methods | 23,561 |
| Classes | 4,631 |
| Interfaces | 1,114 |
| Indexed files | 4,859 |
| `CALLS` edges | 63,251 |
| `USAGE` edges | 72,925 |
| `IMPORTS` edges | 30,676 |
| `TESTS` edges identified by the index | 4,541 |

Searches for storage/recovery, Bolt state transitions, Cypher planning, GDS
procedures, graph catalog, and memory estimation each returned more than one
thousand structurally related symbols. This is evidence of many integrated
subsystems, not merely noise from comments or docs.

Examples found through the graph:

- checkpoint and corrupted-log recovery integration tests reach deep page-cache
  and transaction-log components;
- Bolt has explicit negotiation, authentication, transaction, pull/discard,
  reset, route, and streaming state transitions;
- Cypher has parser/AST, semantic phases, logical planning, rewrites, plan
  building, and multiple runtime layers;
- GDS procedures traverse facades, configuration parsing, graph-store catalog,
  memory estimation, progress/termination, algorithm execution, and result
  modes before reaching a kernel such as WCC;
- GDS memory estimation includes primitive/array collection models, graph-store
  estimates, best/worst ranges, and tests.

## 7. Does The Local Code Have All The Tests?

### 7.1 What is present

| Evidence | Measured amount |
| --- | ---: |
| Neo4j test-source lines | 1,106,333 |
| GDS test-source lines | 212,157 |
| Neo4j JUnit-like test annotations | 14,895 |
| Neo4j Scala test declarations | 18,606 |
| Neo4j Gherkin scenarios | 1,407 |
| GDS JUnit-like test annotations | 4,518 |
| openCypher TCK feature files | 220 |
| openCypher TCK scenarios | 1,615 |
| openCypher TCK `Examples` tables | 276 |
| Neo4j conventional test source roots | approximately 114 |
| GDS conventional test source roots | approximately 78 |

Test annotations are not equal to executed cases: parameterized tests can
expand into many cases; generated/dynamic tests may not carry one annotation;
some declarations may be disabled or conditional. The numbers nevertheless
show substantial verification investment.

### 7.2 What is not present

The answer to "all the tests" is **no**:

1. Neo4j Enterprise source and its internal tests are not in `neo4j-src`.
2. The GDS distribution includes closed sources not present in `neo4j-gds-src`.
3. Internal CI infrastructure, private datasets, performance labs, release
   qualification, security testing, and proprietary cluster tests are not
   established by these clones.
4. The TestKit clone is shallow and primarily verifies drivers, not every
   server behavior.
5. The local server checkout is older than current Neo4j and older than the
   exact `5.26.26` patch dependency declared by this GDS checkout.
6. The current machine has no Java runtime or Maven installed, so the local
   Neo4j/GDS suites were inventoried but not executed in this study.

No Git submodules or Git LFS payloads were reported for the four central
evidence repositories, so the checked-out tracked head files themselves do not
appear incomplete for those reasons.

### 7.3 Which tests can be reused unchanged?

| Test class | Reuse against Rust | Reason |
| --- | --- | --- |
| openCypher Gherkin TCK | High | Describes graph setup, query, result/error, and side effects independent of implementation language |
| Existing official drivers | High as clients | They can connect to the Rust Bolt endpoint without modification |
| Bolt wire fixtures/state sequences | High after packaging | Network messages and expected responses can be represented language-neutrally |
| GDS black-box procedure fixtures | Medium-high after extraction | Same Cypher call can run against stock GDS and Rust; results need family-specific normalization |
| Java/Scala unit tests | Low unchanged | They instantiate JVM classes, mocks, interfaces, allocators, and internal structures |
| Recovery/checkpoint tests | Medium after port | Scenarios are valuable, but fault injection and file internals must be rebuilt for the Rust engine |
| Planner/runtime white-box tests | Low unchanged | Expected internal plans and Scala AST types encode the original architecture |
| Performance tests | Medium | Workload/data can be reused, but environment and measurement must be reconstructed |
| Enterprise/cluster tests | Unavailable | Source and complete fixtures are not local |

### 7.4 The right conversion

Do not translate every assertion from Java syntax into Rust syntax first.
Create a language-neutral fixture layer:

```text
fixture identity
+ initial graph and properties
+ Bolt/Cypher/GDS request
+ expected rows or error/status
+ expected side effects
+ ordering rule
+ numeric tolerance/partition normalization
+ durability/fault schedule when relevant
```

Then execute each fixture against:

```text
stock Neo4j/GDS oracle
Rust compatibility engine
```

Only internal data-structure invariants belong in Rust-native unit tests.

## 8. What Does "Exact Same API" Mean?

The phrase must be split into compatibility tiers before any schedule is
credible.

| Tier | Surface | Feasibility from local evidence |
| --- | --- | --- |
| C1 | Bolt negotiation, PackStream values, auth, sessions, transactions, pull/discard, errors | Feasible; large but externally testable |
| C2 | Cypher syntax and observable semantics | Feasible in subsets; full Neo4j Cypher is the largest single code area |
| C3 | GDS procedure names, modes, configs, schemas, results, catalog behavior | Feasible for OpenGDS; hundreds of operations and substantial lifecycle behavior |
| C4 | Administrative commands/config/HTTP/metrics | Feasible but separate product surface |
| C5 | Neo4j Java embedded APIs and procedure/plugin ABI | Not Rust-native compatible; requires a JVM bridge or reimplementation per extension |
| C6 | APOC and arbitrary existing Java plugins unchanged | Not available without C5/JVM hosting |
| C7 | Neo4j store-file compatibility and in-place opening | Technically possible but very high risk; not required for API compatibility |
| C8 | Enterprise cluster/security/backup/fabric features | Not specifiable from the local open source alone |

"Official drivers connect and receive the same results" is a useful and
achievable definition. "Every Java plugin and Neo4j store directory works
unchanged" is a different program.

## 9. Rewrite Strategies

### Strategy A: Mechanical Java/Scala-To-Rust Translation

Shape:

- preserve modules and classes closely;
- translate tests alongside implementations;
- replace JVM concurrency and I/O primitives with Rust equivalents;
- aim for internal as well as external similarity.

Advantages:

- direct source mapping helps locate missing behavior;
- white-box tests can be conceptually ported;
- fewer design decisions at the beginning.

Failure modes:

- Java ownership patterns become awkward `Arc<Mutex<...>>` designs;
- Scala planner algebra and generated code do not transliterate cleanly;
- mocks and inheritance-heavy tests create Rust architectures optimized for
  the old implementation rather than the public contract;
- a new I/O model, allocator, error model, and concurrency scheduler invalidate
  "same architecture" assumptions;
- GPL derivative-work obligations must be considered before distribution.

Verdict: useful for small leaf modules, poor as the whole-program strategy.

### Strategy B: Behavioral External-Surface Reimplementation

Shape:

- freeze Bolt/Cypher/GDS external contracts;
- use stock Neo4j/GDS as a differential oracle;
- build Rust-native interfaces and internals;
- use TCK and extracted fixtures as acceptance tests;
- preserve official drivers unchanged.

Advantages:

- Rust architecture can exploit ownership, enums, traits, explicit memory
  reservations, and predictable cancellation;
- internal code can be materially smaller;
- alternate storage can slot behind stable traits;
- tests focus on behavior users observe.

Failure modes:

- undocumented behavior creates oracle gaps;
- differences in ordering, floating point, errors, and transaction timing can
  be difficult to classify;
- exact compatibility is discovered gradually, not guaranteed by translation.

Verdict: best durable architecture, still a multi-year full-surface program.

### Strategy C: Vertical Compatibility Strangler

Shape:

- choose one end-to-end request;
- make an official driver send it to both servers;
- pass the relevant TCK/procedure fixtures;
- add transactions, Cypher, catalog, and algorithms in slices;
- proxy or reject unsupported behavior explicitly;
- retire stock Neo4j only after the required workload surface reaches parity.

Advantages:

- produces usable evidence in days/weeks;
- makes scope and support states honest;
- can de-board selected workloads early;
- retains Strategy B as the destination.

Verdict: recommended execution strategy.

## 10. Architecture That Preserves Later Storage Innovation

The user's sequencing idea is sound if the external API is separated from the
physical graph store on the first day:

```text
Official Neo4j drivers
          |
Bolt + PackStream + status/error compatibility
          |
Cypher parser -> semantic IR -> logical plan
          |
Transaction/execution services
          |
GDS procedure/catalog facade
          |
GraphStore + GraphView + PropertyView traits
          |
   +------+-------------------------+
   |                                |
Baseline compatible backend    Later low-RAM backend
simple adjacency/page store    GRAIN/read-shape foundry
```

Critical rule:

> Same external contracts do not require the same internal storage format.

The first backend should be deliberately simple and correct. It should not
attempt Neo4j file-format compatibility unless migration without export/import
is a hard product requirement.

The planner and algorithm layer must consume capabilities, not concrete CSR or
page-cache classes. Then a later backend can supply direct channels, compressed
pages, factorized incidence, strict bounded I/O, or algorithm-shaped artifacts
without changing Bolt or procedure behavior.

## 11. Would `io_uring` Be The Main Performance Advantage?

### 11.1 What `io_uring` can improve

`io_uring` is a Linux asynchronous submission/completion interface. It is most
useful when an engine can maintain meaningful queue depth, batch submissions,
reduce syscall overhead, use registered buffers/files, and overlap storage
latency with useful work.

Potential Knight Bus uses:

- cold graph page reads from NVMe;
- asynchronous prefetch for known scan schedules;
- batched WAL/checkpoint operations where durability semantics permit;
- bulk import and snapshot publication;
- spill file reads/writes;
- backup/export and compaction.

### 11.2 What it cannot improve

When the graph and algorithm state are already resident in RAM, GDS kernels
mostly execute loads, stores, arithmetic, synchronization, and cache misses.
No disk I/O API can accelerate that hot loop.

In-memory WCC, PageRank, Louvain, NodeSimilarity, and FastRP are usually limited
by some combination of:

- memory bandwidth and cache locality;
- atomics/synchronization;
- skew and load balancing;
- candidate/output state size;
- vector arithmetic;
- algorithmic work and convergence.

### 11.3 Evidence and incumbent movement

- [Neo4j GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/)
  describes catalog graphs as in-memory compressed structures optimized for
  topology and property lookup.
- [Neo4j memory configuration](https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/)
  shows that Neo4j already divides work across heap, native memory, page cache,
  transaction memory, direct network buffers, and OS memory. It is not simply
  a heap-object graph.
- [Neo4j disk and page-cache guidance](https://neo4j.com/docs/operations-manual/current/performance/disks-ram-and-other-tips/)
  now documents asynchronous page-cache I/O and Direct I/O evaluation. An
  asynchronous native I/O path is therefore not a durable differentiator by
  itself.
- The 2026 paper
  [High-Performance DBMSs with io_uring](https://arxiv.org/abs/2512.04859)
  reports a 14% improvement in its PostgreSQL integration case study after
  applying workload-specific design guidance. This is useful evidence for an
  I/O subsystem experiment, not evidence for a whole-database rewrite or a
  universal speedup.

### 11.4 Platform consequence

`io_uring` is Linux-specific. Exact cross-platform behavior requires at least:

```text
IoEngine trait
  -> io_uring backend on supported Linux
  -> portable pread/pwrite or thread-pool backend elsewhere
```

Making `io_uring` the architecture rather than one backend would shrink the
compatibility surface and complicate testing.

## 12. Expected Latency And Memory Improvement

No defensible percentage can be promised before same-input benchmarks. The
following are hypotheses to test, not claims.

### 12.1 Same algorithms, same layout, all data resident

Expected shape:

- median throughput/latency may be similar;
- Rust may reduce allocation overhead and GC-related tail events;
- the JVM JIT can outperform ahead-of-time Rust on some hot polymorphic paths;
- Rust can outperform on predictable monomorphic loops and explicit SIMD;
- topology already stored in primitive/compressed structures leaves less object
  overhead to remove than a naive "Java objects versus Rust structs" model
  assumes.

A reasonable planning posture is **parity first**, not "2x by language."
Adopt no public improvement claim until measured on cold and warm runs.

### 12.2 Same external API, Rust-native architecture

Potential improvements are more credible when they come from architecture:

- fewer representation copies;
- direct result streaming;
- bounded allocation and no stop-the-world GC;
- better cache-aware graph ordering;
- push/pull schedule selection;
- algorithm-specific state layouts;
- exact page skipping;
- fused decode/compute;
- plan-owned prefetch and buffers.

These are the same kinds of changes already explored by PRD04. Rust enables
them but does not create them automatically.

### 12.3 Required benchmark matrix

| Dimension | Required variants |
| --- | --- |
| Cache | cold, warm, intentionally constrained |
| Graph | social/power-law, fraud bipartite, road/sparse, dependency DAG |
| Workload | point traversal, filtered Cypher, write transaction, WCC, PageRank, NodeSimilarity |
| Concurrency | 1, core count, oversubscribed |
| Storage | resident, NVMe buffered, Direct I/O/io_uring where supported |
| Metric | p50/p95/p99, throughput, CPU, physical bytes, page faults, cgroup peak, result parity |

Only this matrix can distinguish a Rust win from a cache state, different
semantics, changed algorithm, or benchmark artifact.

## 13. Engineering-Effort Arithmetic

### 13.1 Equivalent work corpus

The measured Community plus OpenGDS corpus contains 2,659,417 implementation
and test lines. A behavioral Rust implementation may be smaller; a literal
port may be similar or larger. The arithmetic below uses the measured corpus as
an equivalent verification workload, not a prediction of final Rust LOC.

Assume 220 engineering days per engineer-year. "Verified LOC/day" means code
and tests that compile, pass the required oracle, survive review, and remain
integrated. It is not raw LLM output speed.

| Sustained verified LOC per engineer-day | Equivalent engineer-days | Engineer-years before product hardening |
| ---: | ---: | ---: |
| 250 | 10,638 | 48.4 |
| 500 | 5,319 | 24.2 |
| 1,000 | 2,659 | 12.1 |
| 2,000 | 1,330 | 6.0 |

Two thousand accepted database-and-test lines per engineer-day over years would
be extraordinarily optimistic. Even that arithmetic is six engineer-years
before hidden behavior, security, operational tooling, packaging, migration,
performance, and release support.

LLMs can dramatically raise raw translation throughput. They do not remove:

- specification ambiguity;
- integration order;
- compiler and ownership redesign;
- flaky or environment-dependent tests;
- crash/recovery campaigns;
- differential diagnosis;
- benchmark validity;
- release and compatibility support.

### 13.2 Calendar scenarios

These are planning bands derived from the measured surface and explicit scope.
They are not contractual estimates.

| Deliverable | Team | Calendar band | Likely implementation/test scale | LLM token planning band |
| --- | ---: | ---: | ---: | ---: |
| Weekend tracer: Bolt handshake, PackStream subset, `RETURN 1`, tiny memory graph, one WCC call | 1 + agents | 2-4 days | 5K-20K lines | 0.2M-2M |
| Compatibility spine: official driver, transactions subset, Cypher subset, fixture runner, one GDS family | 2-4 | 8-16 weeks | 30K-100K | 10M-100M |
| Seven-family GDS service over a stable Rust `GraphView`, limited Cypher/procedure surface | 4-8 | 9-24 months | 100K-350K | 0.3B-2B |
| Broad OpenGDS procedure/catalog/ML compatibility | 8-15 | 18-36 months | 300K-800K | 1B-5B |
| Neo4j Community external Bolt/Cypher/server compatibility plus broad OpenGDS | 12-25 | 3-6 years | 1M-2M implementation plus comparable tests | 3B-15B |
| Community plus Enterprise-equivalent complete replacement | Unknown | Not priceable from local evidence | Missing source/spec/tests | Not priceable |

A solo developer with strong agents could compress some calendar time, but
full Community plus broad GDS remains plausibly an 8-15+ year maintenance
commitment. The system would also continue evolving upstream during that time.

## 14. Token-Effort Model

### 14.1 Raw context floor

| Context set | One raw pass |
| --- | ---: |
| Neo4j Community implementation and tests | 18.35M tokens |
| OpenGDS implementation and tests | 4.50M tokens |
| Core combined | 22.85M tokens |
| All local Neo4j-family source/test code | 33.69M tokens |

Five million tokens can read OpenGDS approximately once, or about 22% of the
combined core corpus. It cannot read, understand, specify, implement, and debug
the whole rewrite.

### 14.2 Why a rewrite consumes much more than one pass

Each vertical slice normally needs:

1. discovery and dependency selection;
2. source reading;
3. test/contract extraction;
4. architecture and Rust interface design;
5. implementation generation;
6. compiler feedback;
7. oracle failures and diagnosis;
8. refactoring and performance work;
9. documentation and retained summaries;
10. later re-reading when upstream or adjacent code changes.

A final Rust corpus of similar behavioral breadth might itself contain tens of
millions of tokens. Input context and failed/revised generations commonly
dominate final-code tokens.

### 14.3 Practical planning bands

| Work | Token band |
| --- | ---: |
| Architecture/indexing and evidence summaries | 20M-100M |
| Extract language-neutral contracts and fixtures | 100M-500M |
| Seven-family OLAP compatibility program | 0.3B-2B |
| Broad OpenGDS compatibility | 1B-5B |
| Community server plus broad OpenGDS | 3B-15B |

The upper bound grows rapidly if agents repeatedly receive whole files rather
than graph-selected spans and retained summaries.

### 14.4 How to make token usage efficient

- Use `codebase-memory-mcp` to locate APIs, callers, tests, and boundaries.
- Select one vertical behavior and read only its dependency cone.
- Convert source evidence into stable contracts and fixture files once.
- Keep a per-slice decision/evidence journal on disk.
- Use stock binaries as remote oracles instead of repeatedly reading internals.
- Separate model tasks: evidence extraction, test design, Rust implementation,
  and adversarial verification.
- Cache summaries keyed by upstream commit and invalidate by changed dependency
  graph, not by conversation age.
- Never put 22.85M raw tokens into one undifferentiated prompt.

## 15. What Information Is Required Before Starting?

### 15.1 Compatibility contract

1. Community only, OpenGDS, or Enterprise-equivalent?
2. Which Neo4j and GDS versions are frozen as the first oracle?
3. Which Bolt protocol versions must work?
4. Is openCypher TCK parity enough, or is Neo4j-specific Cypher required?
5. Which GDS procedures/modes/configs are launch-blocking?
6. Are Java embedded APIs, APOC, custom procedures, and plugins required?
7. Is Neo4j store-file compatibility required, or is export/import acceptable?
8. Is clustering, routing, backup, multi-database, Fabric, or security parity
   required?

### 15.2 Operational contract

1. Linux-only or cross-platform?
2. Single node or cluster?
3. Durability and fsync guarantees?
4. Recovery-point and recovery-time targets?
5. Authentication and authorization model?
6. Upgrade and migration policy?
7. Observability and support requirements?

### 15.3 Performance contract

1. Exact hardware, kernel, filesystem, and NVMe device.
2. Representative graph distributions and sizes.
3. Cold/warm cache policy.
4. Concurrency and read/write mix.
5. Baseline Neo4j/GDS configuration.
6. Required RAM, latency, throughput, and durability thresholds.
7. Whether changed internals may alter result ordering or floating-point
   accumulation within documented tolerance.

### 15.4 Legal/product contract

Neo4j Community and OpenGDS are GPLv3 in these clones; openCypher is Apache 2.0.
A source-guided port may be a derivative work with GPL obligations. Trademark,
protocol, compatibility, and distribution questions need legal review. This
document is engineering analysis, not legal advice.

## 16. Verification-First Execution Plan

### Phase 0: Freeze and classify

- Freeze one Neo4j/GDS version pair.
- Define C1-C8 compatibility tiers and mark required/not-required.
- Preserve stock binaries and fixture data as immutable oracles.
- Create a machine-readable support registry with `unsupported`, `red`,
  `green`, and `verified` states.

Exit: no one can say "exact API" without naming a tier and version.

### Phase 1: Weekend tracer bullet

- Rust TCP listener.
- Bolt handshake for one selected protocol version.
- minimal PackStream values.
- `HELLO`, authentication stub/contract, `RUN`, `PULL`, `RESET`, `GOODBYE`.
- `RETURN 1` through an official unmodified driver.
- byte-level transcript comparison against stock Neo4j.

Exit: official driver connects; success/error metadata is fixture-checked.

This is a valid weekend target. It is not a database rewrite.

### Phase 2: Language-neutral oracle harness

- Cucumber runner for openCypher fixtures.
- dual-server driver harness.
- normalized rows, errors, statuses, and side effects.
- GDS fixture schema with algorithm-specific equality rules.
- persisted failure corpus.

Exit: every claimed behavior has an executable black-box contract.

### Phase 3: Minimal transaction and store

- node/relationship/property values;
- transaction begin/commit/rollback;
- snapshot/isolation choice;
- WAL and crash-recovery tests;
- simple indexes only as demanded by the first Cypher slice;
- `GraphStore` interface independent of storage layout.

Exit: a small write/read workload survives kill/restart and matches side effects.

### Phase 4: Cypher vertical slices

Implement by TCK slice, not parser-file order:

1. literals, parameters, projection, expressions;
2. node/relationship create and match;
3. filters and basic paths;
4. aggregation, ordering, limit;
5. update/delete/merge semantics;
6. indexes/constraints;
7. subqueries, procedures, and advanced features.

Exit per slice: selected TCK scenarios and official-driver fixtures pass.

### Phase 5: GDS compatibility spine

- graph projection/catalog identity;
- `stream`, `stats`, `mutate`, `write`, and `estimate` contracts;
- cancellation/progress;
- memory admission and receipts;
- WCC first;
- PageRank second;
- NodeSimilarity or Louvain as the architecture-breaker.

Exit: same request on the same graph produces the same normalized result and
declared side effects.

### Phase 6: Pareto families

- components;
- communities;
- centrality;
- similarity/KNN;
- paths/traversals;
- embeddings;
- triangles/cohesion.

Exit: each family has procedure, correctness, memory, cancellation, and scale
evidence.

### Phase 7: Alternate storage

Only after the baseline backend and oracle are stable:

- relationship channels;
- virtual projections;
- compressed/adaptive pages;
- factorized incidence;
- state capsules;
- plan-owned bounded I/O;
- `io_uring` backend where benchmarks justify it.

Exit: same external fixtures pass on both backends; improvements are measured.

### Phase 8: De-boarding

- collect actual required-surface telemetry;
- close or explicitly reject all required gaps;
- migration/export/import tooling;
- rollback plan;
- shadow and canary operation;
- remove stock Neo4j dependency only when the required workload, not the entire
  theoretical catalog, is verified.

## 17. Rubber-Duck Debugging

### Duck: If the code and tests are present, why is this not translation?

Because tests are half the corpus and most are coupled to JVM interfaces,
classes, mocks, planners, page-cache objects, and build tooling. Translating
syntax does not preserve concurrency, durability, ordering, errors, or memory
behavior.

### Duck: Can an LLM convert 2.66 million lines quickly?

It can emit a syntactic draft quickly. The expensive work is deciding module
order, designing Rust ownership, compiling, linking, executing fixtures,
diagnosing semantic differences, validating crash recovery, and maintaining a
passing trunk. Raw generation speed is not verified-system throughput.

### Duck: Could all tests simply call Rust through JNI?

Black-box tests can call a Rust server over Bolt or a small FFI. White-box tests
expect thousands of Java/Scala concrete types and internal methods. Recreating
that object surface in JNI is effectively another compatibility layer as large
as the port.

### Duck: Would passing openCypher TCK mean Neo4j compatibility?

No. It provides a strong language-neutral Cypher floor. Neo4j-specific Cypher,
Bolt metadata, statuses, procedures, administration, transactions, storage,
and GDS remain separate surfaces.

### Duck: Would passing all open-source tests mean complete Neo4j?

No. Enterprise and closed GDS sources/tests are absent. Internal tests also do
not prove every production environment, performance envelope, security case,
or operational interaction.

### Duck: Is exact result equality always meaningful for graph algorithms?

Not always. WCC partitions can use different component IDs; community
algorithms can have multiple valid optima; parallel floating-point reductions
can vary in low bits; top-K ties can have ordering ambiguity. Equality must be
defined per procedure using normalization, tolerance, seed, and tie rules.

### Duck: If everything is in RAM, does `io_uring` help?

No. Once resident, the kernel is no longer waiting for storage I/O. Optimize
data layout, cache locality, work scheduling, synchronization, and state size.

### Duck: Why might Rust fail to improve latency?

Neo4j and GDS already use JIT compilation, primitive arrays, compressed graph
structures, off-heap/native memory, direct buffers, and parallel kernels. Rust
removes GC and permits tighter control, but a mature JVM engine is not a naive
object graph. A poor Rust port can be slower.

### Duck: Does preserving the same architecture preserve performance?

It preserves many of the same bottlenecks. Large wins generally come from less
work, fewer copies, better layouts, or better plans, not source-language syntax.

### Duck: Does a complete rewrite make later storage innovation easier?

Only if the rewrite establishes a storage-independent execution contract early.
If every subsystem is first copied around one concrete store, the later backend
change becomes a second rewrite.

### Duck: Can we de-board incrementally rather than after full parity?

Yes. Route only verified workloads to Rust, shadow others, and keep explicit
support states. This reaches user value years earlier than waiting for every
long-tail procedure.

## 18. Recommended Decision

### Reject

Reject the proposition:

> Port all Neo4j Community and GDS code/tests into Rust over a weekend, keep the
> same architecture, add `io_uring`, and expect a major latency improvement.

The measured corpus and missing closed-source surface falsify it.

### Accept

Accept this refined proposition:

> Build a Rust server that preserves a frozen, explicitly tiered external
> Neo4j/Bolt/Cypher/GDS contract. Use unmodified official drivers, openCypher
> TCK, extracted GDS fixtures, stock Neo4j/GDS differential execution, and
> crash tests as the verification loop. Start with a simple correct backend
> behind stable graph/storage traits. Add low-RAM and `io_uring` backends only
> after same-contract benchmarks prove a win.

### Immediate next experiment

In one bounded task, implement or verify this end-to-end path:

```text
official Java/Python driver
  -> Rust Bolt handshake and PackStream
  -> RUN "RETURN 1"
  -> PULL
  -> exact response metadata/error fixtures
```

Then add a tiny graph write/read transaction and one openCypher scenario. This
prices the protocol, value, transaction, and fixture seams without pretending
to price the whole database.

The first decision gate is not "Can an LLM write Rust quickly?" It is:

> Can the team keep one official-driver-to-Rust vertical slice continuously
> green while adding behavior from an external oracle?

If yes, continue slice by slice. If no, a million-line translation will not
repair the verification architecture.

## 19. Evidence Commands And Reproduction Notes

Principal local evidence operations used in this study:

```text
git ls-files                         tracked-file inventory
physical line and byte counting     source/test sizing
tiktoken o200k_base                  raw source-token estimate
rg                                  test/procedure/scenario/module discovery
git branch/show/rev-parse            revision and clone-depth verification
codebase-memory-mcp index_repository typed dependency graph
codebase-memory-mcp get_architecture package/hotspot/boundary inventory
codebase-memory-mcp search_graph     storage/Bolt/Cypher/GDS evidence queries
```

Indexed projects:

```text
neo4j-community-local
  root: gitrefrepo/Neo4j family/neo4j-src
  mode: fast
  nodes: 218,480
  edges: 1,186,989

neo4j-gds-local
  root: gitrefrepo/Neo4j family/neo4j-gds-src
  mode: fast
  nodes: 54,265
  edges: 284,022
```

Environment limitation:

```text
Java runtime: absent
Maven: absent
```

Therefore this report verifies repository contents and structure, not a fresh
green execution of the upstream Java/Scala test suites. Running those suites is
a separate reproducibility task requiring Java 17/21, Maven/Gradle, Docker for
some TestKit configurations, suitable memory, and the expected external test
services/data.

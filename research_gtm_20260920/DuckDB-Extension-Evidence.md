# DuckDB Extension Evidence for Knight Bus

Retrieval date: **2026-09-20**. Status: **bounded research draft, finalized at the requested stop point; verification gaps remain**. This is a mechanics and competitor assessment, not an implementation, installation test, security audit, adoption study, or completed compatibility certification. No packages were installed and no benchmarks were run. Only this file was written for this assignment.

## 1. Premise and Recommendation

**Recommendation [inference]: use DuckDB as a SQL integration and possible distribution surface, but do not make an in-process extension the prerequisite for proving bounded analytics. Prefer an independently bounded worker with a file-first interchange path; consider a thin extension bridge after validating its API and distribution requirements.** A fully in-process implementation remains a legitimate alternative in a controlled host, not a rejected architecture.

The supplied [decision brief](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Final-Research-Decision-Brief.md) and [workflow](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/End-To-End-Workflow.md) define the relevant contract: one useful dependency-investigation family; consistent snapshots; preparation, query, delivery and refresh; a 4 GB physical machine with a provisional 3 GB worker allowance; and 50 GB of retained prepared blocks across the selected portfolio and pinned generations. Temporary disk, input and output need separate accounting. These are proposed requirements, not measured capabilities.

Three distinctions determine feasibility:

1. **An extension ABI is not a resource boundary.** Cross-version loading does not bound native graph state or client materialization.
2. **Community registration is not a universal deployment entitlement.** It does not establish every platform build, hosted-service permission, or browser capability.
3. **Graph functionality already exists in DuckDB's ecosystem.** Knight Bus must differentiate through an independently verified complete workflow, not an algorithm count or the existence of SQL functions.

### Evidence Labels

- **Fact:** directly supported by primary content retrieved during this research. A maintainer's documented capability is not independently benchmarked here.
- **Inference:** engineering or product judgment from those facts and the supplied contract.
- **Unknown / unverified:** a required assertion not established before the stop instruction.
- **Supported:** documented upstream mechanism, subject to the stated version/platform restrictions. It does not mean implemented in Knight Bus.
- **Unsupported claim:** not justified by this evidence, rather than a proof that implementation is impossible.

The analysis uses four lenses: extension API and packaging, bounded systems execution, source consistency and output, and a skeptical competitor comparison. No additional agents were created.

## 2. Authoring and ABI Choices

### Current Baseline Versus Preview

**Fact:** retrieved current documentation identifies the 1.5 documentation line and separately identifies 1.4 LTS. The release-cycle documentation distinguishes stable C API extensions, which can be binary compatible across DuckDB versions, from C++ or unstable C API extensions, which target individual DuckDB versions. Its stable-extension release process is still described as work in progress. [DuckDB release cycle](https://duckdb.org/docs/current/dev/release_cycle)

**Fact:** the August 17, 2026 DuckDB 2.0 article is explicitly a preview. It describes a broader, reworked C API, a C++ wrapper over that ABI, Rust binding work, and trusted custom extension repositories. The repository feature is marked available in preview builds in an update. This is not evidence that the full design is available in the current stable runtime. [DuckDB 2.0 preview](https://www.duckdb.org/2026/08/17/duckdb-20-highlights)

**Inference:** separate the ordinary C++ engine API from a C++ wrapper that uses only the stable C ABI. "Written in C++" does not by itself decide binary compatibility. Conversely, "written in Rust" does not remove version coupling when the adapter uses C++ internals or unstable C symbols.

| Authoring route | Verified mechanics | Knight Bus implication and boundary |
| --- | --- | --- |
| Conventional C++ extension | The standard template includes CMake, vcpkg, SQL-based tests and shared CI infrastructure. | Strongest documented conventional build path. Budget for DuckDB-version rebuilds and possible source changes when using internal APIs. |
| C or C++ using stable C extension API | The official C template is experimental, defaults to the stable API subset, and does not require building DuckDB itself. It also exposes an opt-in unstable API path. | Attractive for a narrow adapter. Confirm every required symbol belongs to the selected stable API version; stable and unstable C are not interchangeable. |
| Rust behind C++ glue | The community FAQ documents this route, and Onager's registered package declares Rust plus C++ with a CMake build. | Reuses Rust compute code but retains the C++ adapter's release and toolchain obligations. FFI ownership and failure handling remain our responsibility. |
| Pure Rust through C extension API | The official Rust template describes scalar and table functions, an experimental pure-Rust implementation and Cargo-based compilation. A Cargo-built `rusty_quack` descriptor is present in the community registry. | Technically and distribution-mechanically credible today. Do not characterize it as impossible or require C++ universally. Do not call the complete authoring experience mature merely because a template is registered. |
| New stable-ABI C++ convenience layer | The `duckdb-cpp-api` repository explicitly calls itself work in progress and describes wrapping the stable C API rather than engine internals. | Monitor, but do not make an unverified preview dependency the first release's critical path. |

Sources: [community development](https://duckdb.org/community_extensions/development), [C template](https://raw.githubusercontent.com/duckdb/extension-template-c/main/README.md), [Rust template](https://raw.githubusercontent.com/duckdb/extension-template-rs/main/README.md), [FAQ](https://duckdb.org/community_extensions/faq), [stable-ABI C++ wrapper](https://github.com/duckdb/duckdb-cpp-api).

### Documentation Conflict: Rust Is Not Merely Future Work

**Fact:** both official C and Rust template READMEs still say community integration is coming soon; the FAQ's Rust answer also lags the available template. However, the registry contains `capi_quack` referencing `duckdb/extension-template-c` and `rusty_quack` referencing `duckdb/extension-template-rs`. It also contains the pure-Rust Cargo-built `behavioral` extension. These concrete descriptors supersede a blanket reading of the older prose. This is a packaging observation, not an adoption precedent. [C descriptor](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/capi_quack/description.yml), [Rust descriptor](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/rusty_quack/description.yml), [behavioral descriptor](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/behavioral/description.yml)

**Unknown:** the smallest stable C API target that supports Knight Bus's complete desired interface was not audited symbol by symbol. In particular, relation-input streaming, cancellation, job state, transactional publication, filesystem permissions, and any buffer-manager integration require a focused prototype. A table-function example proves registration and row output, not arbitrary integration with the relational execution pipeline.

**Inference:** start with snapshot identifiers or admitted artifact paths plus ordinary tabular results. Avoid requiring a custom graph language, optimizer hooks or a custom storage engine until the useful question demands them. For a pure-Rust route, explicitly validate ABI metadata, allocation ownership, destruction callbacks, panic containment and the supported host-client matrix.

## 3. Community Distribution and Maintenance

### What the Repository Provides

**Fact:** publication uses a pull request containing an extension descriptor, including source repository and revision. The extension must build with the community CI toolchain; the conventional template uses that same toolchain. CI builds and tests precede distribution, and DuckDB provides building, signing and hosting. [Community development](https://duckdb.org/community_extensions/development), [community launch and publishing workflow](https://duckdb.org/2024/07/05/community-extensions)

**Fact:** this is not a security code-review guarantee. DuckDB's security documentation says community submissions require available source but are not reviewed as a security assurance; problematic extensions can be removed. Community signatures identify the distribution trust class, not correctness or bounded resource use. [Securing extensions](https://duckdb.org/docs/current/operations_manual/securing_duckdb/securing_extensions)

**Fact:** naming collisions may cause rejection or require renaming. Additional toolchain dependencies can require optional-toolchain work; ad hoc dependency setup increases maintenance fragility. [Community FAQ](https://duckdb.org/community_extensions/faq)

**Inference:** Knight Bus would still own algorithm correctness, dependency/license inventory, vulnerability response, documentation, test quality and user support. Acceptance is a registry/CI event, not a transferred maintenance obligation. No approval-time, review-depth, update-latency or service-level guarantee was established.

### Release and Pinning Obligations

**Fact:** the update guide explicitly scopes its procedure to the C++ template. Descriptor changes trigger rebuilds for the latest stable DuckDB; new DuckDB releases trigger rebuilds of community extensions. Maintainers must fix incompatible APIs, builds or CI. `ref_next` is a documented way to prepare a different source revision for an upcoming release. [Community update guide](https://raw.githubusercontent.com/duckdb/community-extensions/main/UPDATING.md)

**Fact:** standard community installation does not select historical extension source versions by tag, extension version or previous descriptor ref. A rebuild for the same DuckDB/platform can replace the artifact at the active path. The guide recommends separate repositories, channels or direct artifact URLs when historical pinning is required. [Community update guide](https://raw.githubusercontent.com/duckdb/community-extensions/main/UPDATING.md)

**Fact:** community development documentation describes older DuckDB versions as frozen for community updates once they are no longer the latest stable target. This must not be confused with DuckDB core's LTS policy. [Community development](https://duckdb.org/community_extensions/development)

**Inference:** record DuckDB version, platform, extension revision/version, artifact hash, configuration, graph-generation identity and answer semantics in each receipt. Maintain an explicit rollback channel for reproducible jobs. A stable ABI reduces compatibility rebuild pressure but does not eliminate release testing, dependency patches, behavioral changes or artifact provenance.

### Distribution Models

| Model | Benefit [inference] | Continuing responsibility / constraint |
| --- | --- | --- |
| Community extension | Familiar SQL installation; DuckDB-operated build/sign/serve path. | Accepted descriptor and successful target builds; native code trust; compatible runtime; user policy may prohibit community code. |
| Own extension repository or direct artifact | Control of release cadence, historical artifacts and internal mirrors. | Operate the build matrix, availability, provenance and rollback. Current trusted-signing arrangements must be checked; do not assume the 2.0 preview trust model. |
| Worker binary plus ordinary interchange files | No custom extension required in the analyst's host. Compute release independent of host DuckDB ABI. | Ship and support the worker, manifest, file contract and lifecycle; charge export/import and duplicate storage. |
| Extension plus worker | SQL integration with a separately owned compute process. | Two-artifact compatibility, IPC, process lifecycle, authentication/permissions, deployment and joint accounting. Community extension signing does not sign or install an arbitrary companion executable. |

**Fact:** current distribution documentation supports custom repositories on HTTP, HTTPS, S3 or local paths. It documents unsigned third-party loading as a startup configuration choice and notes that HTTPS/S3 repository access uses `httpfs`. The 2.0 preview introduces a different trusted-repository/key-pinning workflow; these are separate hosting and trust questions. [Current distribution](https://duckdb.org/docs/current/extensions/extension_distribution), [preview repository design](https://www.duckdb.org/2026/08/17/duckdb-20-highlights)

## 4. Platforms, Clients and Hosting

### Explicit Support Boundaries

| Surface | Supported evidence | Unsupported or unknown boundary |
| --- | --- | --- |
| Native distribution targets | Current distribution docs list Linux x86-64/ARM64, macOS x86-64/ARM64, and Windows x86-64/ARM64. | This is a distribution inventory, not proof every community extension builds for every target. |
| Additional targets | Docs say some extensions support MinGW and Wasm targets; Android is an example outside official distribution. | Do not infer musl, Android, mobile or every Windows runtime from "Linux/Windows supported." |
| Native clients | Docs distinguish CLI startup configuration from client API configuration and give Python as an example. | Embedded DuckDB version, platform binary, extension policy and result handling all matter. No all-client compatibility run occurred. |
| DuckDB-Wasm | Documentation describes a growing subset of extensions with Wasm-specific binary loading. | A native shared library is not a browser extension binary. Per-extension target builds and browser behavior require verification. |
| Managed/hosted DuckDB | Local extension permissions are documented; community extensions can be disabled. | MotherDuck server-side acceptance, managed notebook policies and specific hosting allowlists were not verified. Local installation does not prove remote execution support. |
| Self-hosted SQL service | Native execution can use extensions when allowed by its operator. | Untrusted SQL and tenant isolation require an explicit security design; a community signature is not a tenant sandbox. |

Native platform/client sources: [extension distribution](https://duckdb.org/docs/current/extensions/extension_distribution). Policy source: [securing extensions](https://duckdb.org/docs/current/operations_manual/securing_duckdb/securing_extensions).

**Fact:** the `rusty_quack` registry descriptor excludes `wasm_mvp`, `wasm_eh`, `wasm_threads`, Windows RTools/MinGW and `linux_amd64_musl`. The `behavioral` descriptor explicitly distinguishes successful Wasm compilation from an unverified end-to-end Wasm link/load and keeps its Wasm targets excluded. These are concrete counterexamples to "Rust compiles, therefore every client is supported." [Rust descriptor](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/rusty_quack/description.yml), [behavioral descriptor](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/behavioral/description.yml)

### Browser Mechanics

**Fact:** Wasm extensions use Emscripten dynamic loading and a Wasm signature section. In the documented extension workflow, `INSTALL` records the source and actual fetching occurs on `LOAD`; it is not durable native-style installation. Browser requests require appropriate CORS access, and the browser HTTP implementation differs from native `httpfs`. A custom extension endpoint must also permit browser fetching. [DuckDB-Wasm extensions](https://duckdb.org/docs/current/clients/wasm/extensions)

**Fact:** the DuckDB-Wasm README retrieved here identifies a DuckDB 1.5.4 base, default single-threaded execution, experimental multithreading, and potentially different out-of-core/filesystem support due to sandboxing. These are properties of that retrieved implementation, not permanent claims about all browsers. [DuckDB-Wasm repository](https://github.com/duckdb/duckdb-wasm)

**Inference:** a browser demonstration is a separate supported profile. Do not promise native spill performance, a local native worker launch, or a 50 GB prepared portfolio from a successful Wasm build. A remote worker would require a separately designed authorized network path. Browser persistence, OPFS, quota and current out-of-core details were not completed before cutoff; no blanket "browsers cannot persist" claim is made.

## 5. Data Integration, Snapshots and Export

### What Was Verified

**Fact:** DuckPGQ's community example uses SQL `CREATE TABLE ... AS SELECT` over CSV sources, then creates a property graph over explicit vertex and edge tables. Onager's pinned README passes an edge-table subquery to graph algorithms. The DuckDB-Wasm repository documents CSV and Parquet reading. Thus SQL-oriented input and tabular graph invocation are real ecosystem mechanisms, not a hypothetical interface invented for Knight Bus. [DuckPGQ example](https://duckdb.org/community_extensions/extensions/duckpgq), [Onager pinned README](https://raw.githubusercontent.com/CogitatorTech/onager/49ad15b52321b914eb31710788418f2c01d4ba43/README.md), [DuckDB-Wasm repository](https://github.com/duckdb/duckdb-wasm)

**Unknown / incomplete:** the [PostgreSQL extension overview](https://duckdb.org/docs/current/core_extensions/postgres/overview) was retrieved, but its substantive integration and transaction sections were not fully inspected before the stop. The current CSV/Parquet export and database export documentation was not fully verified either. This draft does not certify PostgreSQL snapshot isolation, multi-table capture, write-back, pushdown, exact `COPY` options or a tested SQL import/export recipe.

### Proposed Integration Contract [Inference, Not Implemented]

| Input/output path | Product use | Obligation that SQL convenience does not remove |
| --- | --- | --- |
| CSV node and edge tables | Accessible starting point for a dependency investigation. | Explicit ID types, direction, edge types, duplicates, null rules, isolates and wide-value limits; no silent coercion that changes identity. |
| Parquet snapshots | Typed input or result interchange; a candidate worker bridge format. | A file set needs immutable generation identity and a publication manifest. Row groups and decoding buffers need byte accounting. |
| PostgreSQL connector | Candidate way to derive selected relational node/edge projections. | Verify transaction/isolation semantics across all extraction queries and parallel scans, read-only access, source load and credential handling before promising a consistent graph. |
| SQL results joined back to tables | Preserve analyst workflow and original identifiers. | Join memory, sorting, output materialization and client allocations remain inside the same-machine budget. |
| Durable result export | Separate compute completion from result inspection; support large outputs without a giant client object. | Bound writer/compression state; publish completion atomically; charge output and staging; define order and completeness. |

**Inference:** prefer an explicit capture/build boundary: identify source state, extract or seal nodes and edges, validate semantics, publish a manifest, then execute against that generation. Separate tables captured at different times are not automatically one snapshot. Remote-file access, a successful SQL transaction or a collection of individually valid Parquet files must not be treated as a cross-source consistency proof.

**Inference:** preserve three identities independently: the source snapshot/watermark, the prepared representation generation, and the query/parameter/output contract. A new seed can reuse adjacency without reusing an old answer. Changed source data requires generation validation and a supported rebuild/refresh path. Do not assume a DuckDB database export includes extension-owned sidecar files, external worker artifacts or their transactional publication.

For a file-first bridge, measure extraction plus serialization, worker validation/build, output serialization and analyst-side consumption. Avoid an always-required expanded graph when the input naturally supports an incidence representation. Charge retained input, DuckDB staging, Knight Bus prepared blocks, scratch and old/new overlap separately. These requirements come from the supplied workflow, not from a DuckDB performance guarantee.

## 6. Security and Resource Ownership

### Memory and Temporary Disk

**Fact:** DuckDB's limits page explicitly says `memory_limit` applies to the buffer manager. Therefore it is not documented as a whole-process cap. The current configuration table lists `max_temp_directory_size` for temporary-directory data. Its default says 90% of available disk, while the limits page says unlimited: the documentation is inconsistent on that default. Set an explicit value and inspect the pinned runtime rather than inheriting either as a promise. [Limits](https://duckdb.org/docs/current/operations_manual/limits), [configuration reference](https://duckdb.org/docs/current/configuration/overview)

**Inference:** a C++ heap container, Rust allocation, external library buffer, extension-owned mmap, thread stack or client dataframe is not automatically a spillable DuckDB buffer merely because SQL invoked it. The research did not trace every allocation path of any competitor. It establishes that the blanket inference from `memory_limit` to an arbitrary extension heap is unsupported.

**Inference:** `temp_directory` and its size setting do not constitute a quota on every file a native extension can create. Knight Bus must account for its own prepared files, scratch, checkpoints, result files and retained generations. Calling an output "temporary" does not remove it from physical disk usage. A process/container kill can contain an overrun but is not successful bounded completion.

The required allocation model must cover the entire live stage:

- DuckDB operators, vectors, results and runtime overhead.
- Knight Bus topology/state, external sorting and ID mapping.
- Adapter/FFI state, libraries, thread stacks and retained allocator capacity.
- IPC and output queues, decompression and client-side consumption.
- Relevant file-cache/kernel consumption, measured without double counting.

The existing provisional 3 GB worker allowance cannot be allocated independently to DuckDB and to a companion worker on the same 4 GB machine. Either serialize heavy stages or divide a jointly measured envelope. Any arithmetic allocation schedule remains a proposal until whole-workflow measurements and failure tests establish it.

### Security Controls Are Defense in Depth

**Fact:** native extensions have the privileges of the parent DuckDB process. DuckDB distinguishes core signatures, community signatures and unsigned extensions, with community/unsigned restrictions and autoload controls. [Securing extensions](https://duckdb.org/docs/current/operations_manual/securing_duckdb/securing_extensions)

**Fact:** DuckDB warns against treating SQL or non-SQL inputs as harmless. Paths, identifiers and filter expressions can trigger filesystem/network operations; it recommends restricted processes/containers, OS sandboxing, network isolation and application timeouts where appropriate. Configuration hardening is not a substitute for sandboxing. [Security model](https://duckdb.org/docs/current/operations_manual/securing_duckdb/overview)

**Unknown:** the exact `enable_external_access` behavior and permitted-path configuration for the proposed adapter were not fully verified. No claim is made that disabling that setting can sandbox an already loaded arbitrary native library or that a bridge will function with external access disabled.

**Inference:** make job submission explicit and permissioned. Do not hide process spawning, arbitrary path writes or network access in an apparently pure scalar expression. Restrict artifact paths, validate identifiers, contain panics/errors, propagate cancellation and make cleanup bounded. Community acceptance of a particular process-launching or remote-worker bridge remains unverified; do not imply preapproval.

## 7. Competitors and Complements

### Registration Evidence

**Fact:** a GitHub API tree read of `duckdb/community-extensions` returned head `8274fcf7a6ba87cc49ec5562b24604a046d2a934` with `truncated: false`. It contained `extensions/onager/description.yml` and `extensions/duckpgq/description.yml`, but no path matching `graphar`. The direct candidate GraphAr descriptor URL also returned 404. This supports absence of a GraphAr-named entry in that inspected tree, not a universal assertion about aliases, future registrations or custom repositories. [Registry tree at inspected revision](https://api.github.com/repos/duckdb/community-extensions/git/trees/8274fcf7a6ba87cc49ec5562b24604a046d2a934?recursive=1)

Descriptor details below were separately retrieved from their `main` URLs. They identify source revisions but are not a binary download/load audit.

| Project | Actual registration and licensing evidence | Documented capability | Bounded-workflow interpretation |
| --- | --- | --- | --- |
| Onager | Community page and descriptor; version 0.4.0; Rust + C++, CMake; MIT OR Apache-2.0. Descriptor ref `49ad15b52321b914eb31710788418f2c01d4ba43`. | SQL table-function graph analytics over edge inputs; directed/undirected and weighted/unweighted support; uses Graphina. | Direct analytics competitor. No verified 4 GB source-to-answer contract, external-state schedule, refresh bound or complete output bound in inspected material. |
| DuckPGQ | Community page and descriptor; version 0.3.1; C++, CMake; MIT. Descriptor ref `f386a6cfb90aadb0077e734377bb86740bdda71b`, plus an `andium` revision field. | Property graph definition over tables, SQL/PGQ pattern/path queries, and graph algorithms. | Direct competitor for dependency/path investigation. Standards-oriented syntax is not full language conformance, Neo4j/GDS parity or bounded-memory evidence. |
| Apache GraphAr | Apache repository documents Apache-2.0 for the GraphAr project, with third-party license caveats. Not established as a registered DuckDB extension here. | System-independent graph storage/interchange format and libraries; chunked property-graph data. | Potential storage/interchange complement. A chunked format is not an end-to-end bounded builder or execution engine. |
| `lithium-tech/duckdb-graphar` | Separate experimental DuckDB reader repository. No GraphAr-named community registration found. **Reader license not verified**; do not transfer Apache GraphAr's license to it. | Reads GraphAr vertex/edge tables with SQL and simple filtering; README lists GraphAr and Arrow dependencies and source-build instructions. | Relevant adapter/reference implementation, not a demonstrated analytics substitute or one-command community installation. |

Sources: [Onager registry](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/onager/description.yml), [Onager pinned README](https://raw.githubusercontent.com/CogitatorTech/onager/49ad15b52321b914eb31710788418f2c01d4ba43/README.md), [DuckPGQ registry](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/duckpgq/description.yml), [DuckPGQ community page](https://duckdb.org/community_extensions/extensions/duckpgq), [Apache GraphAr repository](https://github.com/apache/incubator-graphar), [GraphAr DuckDB reader README](https://raw.githubusercontent.com/lithium-tech/duckdb-graphar/main/README.md).

### Specific Caveats

**Onager [facts]:** its descriptor excludes `windows_amd64_mingw`, `osx_amd64` and `wasm_threads` and requires the Rust toolchain. It links a browser playground, which does not establish every Wasm target. The pinned README warns of early development and breaking changes and says its native binary matches the DuckDB version against which it was built. [Descriptor](https://raw.githubusercontent.com/duckdb/community-extensions/main/extensions/onager/description.yml), [pinned README](https://raw.githubusercontent.com/CogitatorTech/onager/49ad15b52321b914eb31710788418f2c01d4ba43/README.md)

**Onager [maintainer-reported fact]:** its pinned known-issues document describes multithreaded table-function/materialization errors on DuckDB 1.4.x and earlier, recommends one thread there, and reports the issue fixed in 1.5.0+. This was not reproduced. It illustrates why host-version support is a behavior test, not only a successful load. [Pinned known issues](https://raw.githubusercontent.com/CogitatorTech/onager/49ad15b52321b914eb31710788418f2c01d4ba43/KNOWN_ISSUES.md)

**DuckPGQ [facts]:** the community page demonstrates `CREATE PROPERTY GRAPH`, `GRAPH_TABLE`, bounded-length `ANY SHORTEST` matching and local clustering coefficient. It also lists CSR-related functions and warns that the research project still has features under development. [Community documentation](https://duckdb.org/community_extensions/extensions/duckpgq)

**DuckPGQ [unknown]:** no exclusion list appears in the inspected descriptor, but that is not proof of all-platform binary availability. CSR allocation ownership, spillability, per-query materialization and snapshot refresh behavior were not source-audited. Do not infer either a bounded execution guarantee or a universal in-memory limitation from the presence of CSR function names.

**GraphAr [inference]:** inspect whether its IDs, labels, adjacency organization and properties preserve the exact chosen projection before adopting it. Arrow dependencies and graph chunking neither prove nor disprove a fixed physical-memory execution contract. The experimental reader's write/export functionality, license, release matrix and current DuckDB target remain gaps.

No algorithm-list breadth, GitHub stars, weekly downloads or browser demo was counted as adoption, successful production execution, or proof of bounded memory. No unsupported performance superiority claim is made for Knight Bus.

## 8. Architecture Decision

All entries in this comparison are **inferences/proposals**, not measured outcomes.

| Choice | Why choose it | What must be proved / support cost |
| --- | --- | --- |
| Full in-process extension in an arbitrary analyst host | Most direct SQL composition; can avoid a separate transport stage. | Shared address space, host contention, native failure blast radius, arbitrary client buffering and ABI/platform releases. Cannot promise the whole-machine budget without controlling the host. |
| In-process extension inside a dedicated bounded job process | Keeps SQL/extension integration while controlling the containing process and workload. | Still needs internal allocation/spill contracts, cancellation and complete output accounting. This is a serious alternative to a two-process bridge. |
| Thin extension talking to bounded worker | Keeps a familiar SQL entry point and separates compute release and crash handling. | Byte-bounded IPC, two-sided cancellation, worker discovery/versioning, path/credential security, no hidden graph copies, shared physical-machine budget. Not automatically simpler to install. |
| File-first bounded worker with DuckDB before/after | Cleanest initial evidence boundary; no custom extension permission required for compute. | Explicit snapshot and result manifests, export/import cost, staging/retention quotas and operator steps. Source-to-answer timing must include the whole chain. |

**Chosen thesis:** pursue file-first interoperability for the first complete bounded investigation, keeping a thin stable-C-ABI adapter as a candidate convenience layer. In parallel with ordinary development, evaluate whether a dedicated job process embedding DuckDB is simpler than a separate IPC worker. Do not commit to the bridge until its extra deployment cost earns a concrete workflow benefit.

A future in-process implementation can be the better product if a controlled host and verified resource ownership eliminate unnecessary copies and operational steps. The present evidence does not establish that the worker is faster or that extensions cannot be bounded. It establishes that the user's existing memory and lifecycle contract must remain authoritative over the packaging decision.

## 9. Verification Gates and Remaining Gaps

The following are **not completed**. They are the explicit boundary of this draft, not promises of future unattended work.

| Gate | Evidence needed before promoting the recommendation |
| --- | --- |
| Stable API sufficiency | Exact C API version/symbol inventory; table input/output and cancellation prototype; no accidental unstable/internal dependency. |
| Native install/support matrix | Pin runtime, extension and artifact hashes; load and execute the selected job in the intended CLI/client versions on each advertised target. |
| Current release policy | Resolve actual stable patch target, community support for older/LTS targets, and stable-C-ABI deployment behavior in the live CI configuration. |
| PostgreSQL source contract | Primary connector/transaction documentation plus concurrent-update tests proving a consistent nodes-and-edges capture; limits on source load and credentials. |
| Export and restart | Verified CSV/Parquet writer options, memory measurements, complete-result publication, cancellation, disk exhaustion and crash recovery. |
| Whole-machine completion | First import/build/query, second seed, changed-data refresh and slow-consumer run on the actual 4 GB envelope; not merely allocator counters or OOM containment. |
| Competitor implementation | Inspect Onager/Graphina and DuckPGQ allocation/materialization/refresh paths; run the same answer and resource contract. No generic algorithm-count comparison. |
| GraphAr reader | Verify license file and dependencies, supported DuckDB revision/platforms, read/write scope and any registry alias before reuse or distribution claims. |
| Browser and hosting | Verify current Wasm persistence/out-of-core behavior and exact target binaries; verify each managed host's policy separately, including MotherDuck. |
| Companion-worker distribution | Verify community suitability, executable packaging, trust/provenance, privilege model and protocol compatibility without bypassing host restrictions. |

### Recommendation Implications

**Proceed with DuckDB integration feasibility, conditionally. Do not promote "extension-first" into "extension-only" or "bounded by DuckDB settings."** The stable C ABI is real and pure-Rust registry examples exist; the conventional C++ route remains materially version-coupled. Community distribution reduces packaging work but does not remove release ownership or security obligations. Onager and DuckPGQ invalidate a generic "graph algorithms inside DuckDB" differentiation claim. GraphAr is more plausibly a format/adapter consideration than a verified bounded analytics replacement in this evidence set.

The next decision should be made on one admitted dependency investigation with correct original-ID output, repeat-query reuse and changed-data refresh, comparing a controlled in-process job against a file/worker boundary. This draft deliberately stops short of certifying PostgreSQL snapshots, universal clients/platforms, hosted execution or any complete 4 GB implementation.

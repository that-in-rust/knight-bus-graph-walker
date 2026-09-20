# Lead Evidence and Decision Changes

Date: 2026-09-20. Research synthesis supplement. Facts describe inspected primary sources, not newly executed software or customer experiments. Recommendations are judgments. The three platform/adoption reports retain their own source-access and verification limits.

## Local Context Used

This GTM research uses the completed `research_4gb_20260919` synthesis and its source ledgers rather than claiming another independent full read of every archived source. The prior [coverage audit](../research_4gb_20260919/Source-Coverage-Completion-Audit.md) reports 410 prose entries, with narrower inspection of 55 supporting tables. That is the earlier research scope, not the number of files newly read for this goal. Linked code, websites and papers do not inherit full-read status.

| Prior evidence family | Current decision consequence | Authoritative local entry |
| --- | --- | --- |
| Founder, PRD04 and PMF | Start with a consequential dependency/security investigation; a receipt or low-RAM number is not the customer outcome | [Decision brief](../research_4gb_20260919/Final-Research-Decision-Brief.md), [product evidence](../research_4gb_20260919/05-prd04-product-Evidence.md) |
| PRD03/GDS/compatibility | Preserve the narrow Bolt proof, but do not imply general Cypher, GDS or permission-evaluation parity | [Decision map](../research_4gb_20260919/Architecture-Decision-Map.md), [GDS evidence](../research_4gb_20260919/01-prd03-Evidence.md) |
| PRD06 algorithm/storage atlas | The interface must permit multiple algorithm-specific layouts; packaging does not collapse them into one format | [PRD06 architectures](../research_4gb_20260919/02-prd06-Architectures.md), [decision map](../research_4gb_20260919/Architecture-Decision-Map.md) |
| PRD02/05 feasibility and reference patterns | Porting language or choosing an extension is not a general speed/RAM proof | [Feasibility evidence](../research_4gb_20260919/03-feasibility-Evidence.md), [pattern transfers](../research_4gb_20260919/04-patterns-Mechanism-Transfers.md) |
| Product/community/raw archives | Distinguish complaints, duplication, interest and actual adoption; test incumbent repair before claiming replacement value | [Product timelines](../research_4gb_20260919/Product-Decision-Timelines.md), [source audit](../research_4gb_20260919/Source-Coverage-Completion-Audit.md) |
| Complete lifecycle | Physical RAM includes build, host, client and refresh; prepared storage differs from input and peak scratch | [Workflow](../research_4gb_20260919/End-To-End-Workflow.md) |
| Structural proposals/reviews | D16/D17 are conditional opportunities, not already shipped or universal PageRank replacements | [D16](../research_4gb_20260919/Feature-State-PageRank.md), [D17](../research_4gb_20260919/Feature-Refresh-Certificates.md), [D17 review](../research_4gb_20260919/03-feature-refresh-Review.md) |

The last completed reading's conclusions, source pointers and counterexamples remain the context for this work. Historical candidate lists are not treated as current competitor support matrices. The new web research specifically updates extension mechanics, deployment friction, adoption precedents and alternative entry points.

## New Primary Evidence

All pages below were inspected on 2026-09-20. Publication dates, where named, are separate from retrieval dates. These are selective findings, not an exhaustive market survey.

### E01: Native SQL Is a Serious Control

DuckDB's May 23, 2025 article documents keyed recursive state introduced in 1.3; its current WITH documentation includes graph examples. The keyed representation can replace accumulated intermediate history with current per-key state. This is an existing mechanism relevant to our memory thesis, not evidence that every recursive query is bounded or fast. [Maintainer article](https://duckdb.org/2025/05/23/using-key), [current SQL documentation](https://www.duckdb.org/docs/current/sql/query_syntax/with).

**Decision:** include a competent native-SQL implementation as a control for an eligible dependency or connectivity workload. Beating a deliberately path-enumerating query is insufficient. The exact SQL and runtime must be pinned: documentation and research descriptions of recursive semantics evolve.

An August 25, 2026 maintainer article additionally describes retaining reusable recursion state and selecting execution modes from physical work. This reinforces that the incumbent is moving; it is not a tested release/compatibility claim for our planned experiment. [New recursion implementation account](https://www.duckdb.org/2026/08/25/how-duckdb-runs-recursive-ctes-faster).

### E02: DuckDB Is Already Out-of-Core, With Limits

The tuning guide documents spilling for major blocking operators, but also remaining OOM cases involving operator combinations and some aggregates. It recommends investigating plans and excessive intermediate cardinality. Therefore "uses disk instead of RAM" is not a sufficient new product proposition. [DuckDB workload tuning](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads).

**Decision:** demonstrate a particular complete graph job that existing spill-aware execution cannot handle as economically or predictably. Do not benchmark an arbitrary extension heap as though the host's memory setting automatically controlled it.

### E03: PostgreSQL Data Does Not Force PostgreSQL-Native Distribution

DuckDB's PostgreSQL connector documents query-time reads, optional copying, Parquet transfer, read-only attachment and transactional rollback examples. That establishes a PostgreSQL-connected analytical path without putting Knight Bus native code inside PostgreSQL. It does not certify our multi-table capture under concurrent changes, full memory bound or source-load limits. [Connector documentation](https://duckdb.org/docs/current/core_extensions/postgres/overview).

**Decision:** separate the data source, query interface and execution host. Test a client-side capture/result path before assuming native server installation is required. Repeatedly reading live source tables is different from querying a published analytical snapshot.

**Gap resolution:** this closes the DuckDB draft's uncertainty about documented basic read/copy/write-back mechanisms. Its implementation-level snapshot, recovery and memory tests remain open. Merely having transaction syntax does not establish an atomic source-to-prepared-artifact publication protocol.

### E04: NetworkX Is Another Integration Surface, Not a Free Conversion

The inspected stable backend documentation describes separate packages, algorithm dispatch, backend-owned graphs and cached conversion. It warns that conversions/caching can be expensive and that fallback can reconstruct a NetworkX graph. [NetworkX backend contract](https://networkx.org/documentation/stable/reference/backends.html).

**Decision:** a Python backend is a credible branch when the user's workflow already uses NetworkX. A low-RAM product must avoid requiring the large Python graph first and must control fallback/output behavior. This needs a supported subset, not a universal drop-in promise. The interface is evolving; pin and test it. No backend was implemented here.

### E05: A Concrete Low-RAM Complaint Also Contains a Priority Warning

MetagenomeScope issue 423, opened April 29, 2026, describes difficulty with large graph/object copies on an 8 GB laptop, interest in on-disk neighbor/subgraph access, and concern about installing Neo4j. It also calls replacement lower priority and suggests first locating whether graph structure or metadata causes the memory pressure. [Maintainer's issue](https://github.com/marbl/MetagenomeScope/issues/423).

**Decision:** portable packaging and application-level integration are plausible, including outside security. This single issue is neither a buyer nor evidence that our current traversal subset satisfies assembly-graph decomposition. Do not infer urgency from pain alone. The diagnostic could favor moving metadata to disk rather than replacing the graph engine.

### E06: A Public Dependency Demo May Already Be Solved

The deps.dev team's May 22, 2023 article supplies BigQuery dependents queries and explains that its data excludes private dependents and uses a particular dependency resolution. [Primary walkthrough](https://blog.deps.dev/enumerating-dependents/).

**Decision:** use a public dependency dataset for reproducibility, not as proof of unmet demand. A potential differentiator is a user's private, resolved, versioned snapshot and actual downstream decision, but that remains a hypothesis. Package reachability alone does not prove exploitability or effective permissions.

### E07: Distribution Can Begin at the Data Producer

Overture's own documentation teaches DuckDB through concrete data-extraction recipes. This is a verified example of a data publisher introducing a compute tool at the point of need, not a measured acquisition or retention rate. [Overture recipe](https://docs.overturemaps.org/getting-data/duckdb/).

**Decision:** an authorized dependency-export tutorial or product integration may reach more qualified jobs than a generic database-extension launch. There is no agreement with any publisher or maintainer here.

### E08: Consolidation and Packaging Can Matter More Than Kernel Identity

The Supabase Chatbase case presents an adopting company's account of consolidating vector search into its existing stack; it is vendor-mediated testimony. sqlite-vec's initial maintainer announcement describes a narrow local capability with portable packaging. Neither establishes Knight Bus demand or a universal extension success rate. [Chatbase case](https://supabase.com/customers/chatbase), [sqlite-vec release](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html).

**Decision:** test whether the new component removes more operations than it introduces. The broader four-case analysis and evidence classifications are in [OSS Adoption Precedents](OSS-Adoption-Precedents.md).

## Platform Evidence Retained

### Final Decision-Critical Checks

- **Native-host eligibility:** AWS's RDS extension guide and FAQ describe supported extensions, role/allowlist controls, a route to request additional extensions, and the separate trusted-language framework. This supports the deployment gate, not a claim that custom native libraries are available to every SQL user. No provider support timing is promised. [RDS guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Extensions.html), [RDS FAQ](https://aws.amazon.com/rds/postgresql/faqs/).
- **Memory control:** DuckDB's limits page explicitly scopes `memory_limit` to its buffer manager; the OOM guide notes operations that allocate outside it. The final memo therefore requires whole-host measurements and does not certify arbitrary extension heaps from that setting. [Limits](https://duckdb.org/docs/current/operations_manual/limits), [OOM guidance](https://duckdb.org/docs/current/guides/performance/oom).
- **Snapshot precedent:** the September 30, 2022 PostgreSQL-scanner article describes exporting/importing a snapshot among connections for one table scan. This is a documented consistency mechanism, not a current end-to-end multi-table Knight Bus capture proof. The distinction remains an implementation gate. [Scanner design](https://duckdb.org/2022/09/30/postgres-scanner).
- **Hosted DuckDB gap:** targeted MotherDuck searches returned general DuckDB extension tutorials and ecosystem materials, not an authoritative entitlement to run an arbitrary Knight Bus extension server-side. Hosted acceptance remains unverified; the recommendation targets an operator-controlled local process rather than assuming that entitlement. This is an evidence limit, not a claim of prohibition.

- [PostgreSQL report](Postgres-Extension-Evidence.md): native pgrx versus trusted-language versus external-job routes; RDS, Aurora, Neon and Supabase restrictions; relevant AGE, pgRouting, MADlib and GraphBLAS-class alternatives; allocation, upgrade, security and whole-host accounting. Provider catalogs are version-specific, not automatic deployment authorization.
- [DuckDB report](DuckDB-Extension-Evidence.md): stable C ABI versus internal C++ coupling; experimental Rust/C templates and registry evidence; community signing versus security assurance; client/platform limits; Onager, DuckPGQ and GraphAr distinctions. Pure Rust is not categorically impossible; all-target support is not established.
- [Adoption report](OSS-Adoption-Precedents.md): pgvector, spatial/httpfs, pg_duckdb and sqlite-vec; provider/customer/practitioner evidence explicitly distinguished. No extension-wide retained-user denominator or attribution of application revenue to an extension is established.

## What Changed During Synthesis

| Earlier hypothesis or tempting shortcut | Evidence/counterexample | Final decision change |
| --- | --- | --- |
| Choose the database with the largest audience | Native deployment eligibility is narrower than SQL access | Segment by an installable workflow, not generic ecosystem size |
| DuckDB is an empty graph opportunity | Onager, DuckPGQ and keyed recursion already exist | Demand full-workflow differentiation and a competent SQL/graph baseline |
| A worker must always be separate from DuckDB | A dedicated process can embed DuckDB and contain both lifecycles | Keep controlled in-process execution as a serious alternative; IPC must earn its cost |
| A Python backend avoids migration | Automatic conversion/fallback can recreate large representations | Select native ingestion and explicit supported semantics before claiming low RAM |
| Public dependency graphs establish the initial market | Existing public queries can already supply dependents | Use public fixtures for reproducibility; validate private/organizational work separately |
| An OOM issue is an urgent purchase | The maintainer explicitly deprioritizes replacement | Verify job consequence, priority and operator willingness, not only technical pain |
| Lower RAM alone is the announcement | First-answer, refresh and consumed output decide usefulness | Publish a complete repeatable workflow and its limitations |

## Research and Implementation Boundaries

The first goal turn made concrete progress by creating and publishing the three evidence drafts and checkpoint. This continuation adds source-backed controls and completes decision synthesis. There was no blocked or no-progress condition to relabel as completion.

Unresolved API symbol compatibility, managed-instance entitlements, exact license compatibility, deployment security, whole-host performance and customer response remain implementation or field-validation gates. The research can recommend a bounded experiment without pretending those gates have passed. A final recommendation that requires an unverified capability must make it conditional or select a verified alternative path.

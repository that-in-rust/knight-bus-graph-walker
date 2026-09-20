# OSS Adoption Precedents for Knight Bus

Retrieval date: **2026-09-20**. Status: **bounded research draft finalized on request**. Four sourced cases are complete enough to inform experiment selection; causal adoption estimates, extension-wide retention/revenue, and customer validation remain unestablished. Exploration stopped at the user's direction. No installs, implementation, experiments, outreach, new agents, commits, or pushes were performed.

## Decision Summary

**First GTM hypothesis: help an operator finish one previously painful dependency investigation on their own artifacts, through a portable local workflow. Let a demonstrated need earn a SQL extension or existing-product integration.** Do not choose a database ecosystem because its audience appears large.

The precedents support a recurring mechanism, not a guaranteed growth formula: a recognizable job, already-accessible data, familiar operations, and a short path to a useful result. Hosted distribution can remove deployment work; publisher tutorials and product integrations can put the capability directly in a user's workflow. None automatically establishes repeated use or willingness to pay.

The strongest transferable lessons are:

1. Sell the completed question, not the extension category or RAM number.
2. Preserve existing IDs, tables, files, and downstream tools wherever semantics allow.
3. Separate package installation from successful activation on real data.
4. Treat a provider or partner as an accountable distribution and support relationship, not an inherited audience.
5. Measure the second useful run and changed-data refresh. Downloads and successful demonstrations do not substitute for these.

These are inferences from the cases below, not measured Knight Bus acquisition effects.

## Premise and Boundaries

Read in full: [Final Research Decision Brief](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Final-Research-Decision-Brief.md) and [Product Decision Timelines](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Product-Decision-Timelines.md).

Their baseline is a complete source-to-answer-to-refresh workflow, not a general Neo4j replacement. The proposed envelope is a 4 GB physical machine, provisionally 3 GB for the worker and 1 GB for other needs, with 50 GB retained prepared storage and separate input/scratch/output allowances. These remain targets to validate, not a newly demonstrated capability. The narrow existing Bolt evidence does not establish arbitrary Cypher, witness paths, bounded preparation, or paid demand.

Evaluation lenses: workload owner, integration/operator effort, distribution economics, and skeptical evidence review. This report deliberately does not duplicate a platform API, ABI, hosting-eligibility, or engine-comparison survey.

Evidence labels:

- **Maintainer documentation:** primary evidence of documented behavior, release history, or packaging; not customer adoption.
- **Provider statement:** primary evidence of the provider's own offering or reported experience; commercially interested.
- **Company-marketing customer case study:** named customer testimony, mediated by a vendor; not independently audited.
- **Firsthand practitioner account:** evidence of the author's particular use, not an ecosystem denominator.
- **Inference/proposed experiment:** our interpretation or future test; not an observed outcome.

All external links below were retrieved on 2026-09-20. Publication dates are distinguished from retrieval dates. Moving documentation and repository branches describe the retrieved state, not an immutable historical archive. Installation examples explain precedents; they were not executed.

## Case 1: pgvector

### Origin and Workload

Andrew Kane's own project listing dates pgvector to April 2021; the repository changelog records the first release on **2021-04-20**. The original release already offered a vector column, distance operators, and an IVFFlat index inside PostgreSQL. Its concrete starting proposition was nearest-neighbor search through ordinary SQL, not a separately operated vector service. No verified account of the first customer or Kane's precise personal triggering problem was found; do not invent one. [Kane's project listing](https://ankane.org/opensource?language=C), [release history](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md), [v0.1.0 README](https://raw.githubusercontent.com/pgvector/pgvector/v0.1.0/README.md).

This distinction matters historically: a usable technical primitive preceded the hosted AI distribution wave. It does not prove that extension architecture alone created that wave.

### Integration and Distribution

- **Same database workflow:** current maintainer documentation presents vectors alongside other data, exact and approximate search, and PostgreSQL operational facilities. The Python companion supports existing database libraries, including Django, SQLAlchemy, and Psycopg. A Python client package is not the server extension itself. [pgvector README](https://github.com/pgvector/pgvector), [Python integration README](https://raw.githubusercontent.com/pgvector/pgvector-python/master/README.md).
- **Provider demand plus an example:** Supabase's January update, published **2023-02-08**, says pgvector was among its most requested AI/ML extensions and announces availability. It also describes its own Clippy documentation-search MVP and links a tutorial. This is a provider's qualitative demand report, not a disclosed request count or conversion rate. [Supabase release update](https://supabase.com/blog/supabase-beta-january-2023).
- **Managed installation:** AWS announced RDS support on **2023-05-03**, initially for PostgreSQL 15.2 and higher. That dated availability statement is not a present-day version matrix. Provider packaging removes a step that the original self-hosted installation required: compiling and installing extension files before `CREATE EXTENSION vector`. [AWS announcement](https://aws.amazon.com/about-aws/whats-new/2023/05/amazon-rds-postgresql-pgvector-ml-model-integration/), [original installation instructions](https://raw.githubusercontent.com/pgvector/pgvector/v0.1.0/README.md).

The defensible sequence is useful primitive -> provider-reported workload demand -> packaged availability and teaching -> named deployments. The sources do not isolate each step's causal contribution.

### Adoption Evidence and Counterevidence

**Company-marketing customer case study:** Supabase reports that Chatbase migrated vector search from Pinecone into its existing Supabase stack. Founder testimony emphasizes consolidation, related-data deletion, and a single support relationship. The same story describes analytical I/O contention and a plan involving query repair, more disk capacity, and a replica. Consolidation reduces some work but does not eliminate resource management. [Chatbase case study](https://supabase.com/customers/chatbase).

**Company-marketing customer case study:** Berri AI's founders describe choosing Supabase for existing-stack consolidation, accessible SDKs, debugging, and table tooling. This supports operator experience as an adoption factor, not a demonstrated pgvector kernel advantage. [Berri AI case study](https://supabase.com/customers/berriai).

**Provider/customer engineering account, commercially interested:** AWS's **2026-04-21** Ring article documents production semantic video search. Its detailed implementation uses exact scans within user partitions, not ANN indexes. It also reports cold-cache latency trouble after two months in production that an initially favorable proof of concept missed. This is particularly relevant to Knight Bus: the installed primitive and the warm benchmark were not the whole operational answer. [Ring engineering account](https://aws.amazon.com/blogs/database/rings-billion-scale-semantic-video-search-with-amazon-rds-for-postgresql-and-pgvector/).

**Transfer:** answer a specific question without forcing a separate data-management workflow; make familiar tools carry the new capability; enlist a host only when users request the workload.

**Does not transfer:** low-RAM graphs do not inherit the AI/embedding demand wave. Graph projection, global state, output size, and refresh may require much more preparation than adding a vector column. Approximate vector recall tradeoffs do not authorize incomplete exact dependency answers. The project's own filtering guidance also shows that SQL familiarity does not erase correctness and tuning details. [pgvector filtering guidance](https://github.com/pgvector/pgvector#filtering).

## Case 2: DuckDB Spatial and httpfs

These are treated as one complementary workflow case, not two independent adoption successes.

### Workload and Activation

Max Gabrielsson's **2023-04-28** spatial launch post starts with geospatial processing alongside other analytical data. It demonstrates combining Parquet and shapefile data, analysis, and geospatial output. Familiar spatial functions and GEOS/GDAL/PROJ integration connect existing knowledge and formats. The activation is `INSTALL spatial; LOAD spatial;`; the launch explicitly did not promise complete PostGIS parity. Those limitations are historical statements, not a claim about current feature coverage. [Maintainer launch article](https://duckdb.org/2023/04/28/spatial).

The complementary `httpfs` extension provides remote HTTP/S3 access and is documented as autoloading on first use by default. HTTP Parquet reads can use metadata and range requests to avoid fetching irrelevant portions. That is a documented access mechanism, not a guarantee that every query downloads little data. [httpfs documentation](https://duckdb.org/docs/current/core_extensions/httpfs/overview), [HTTP access documentation](https://duckdb.org/docs/current/core_extensions/httpfs/https).

### Distribution Beyond the Database Audience

**Data-publisher channel:** Overture Maps' own getting-data guide instructs readers to install spatial and a cloud-access extension, then provides task-specific SQL for extracting places, buildings, and roads into usable files. Users arrive wanting a geographic extract, not necessarily wanting an extension. This is documented distribution through a dataset's instructions, not a measured conversion funnel. [Overture DuckDB guide](https://docs.overturemaps.org/getting-data/duckdb/).

**Existing-tool channel:** GDAL's official tutorial, last updated **2025-09-26**, documents DuckDB through its ADBC driver with spatial/httpfs among the extensions. It includes platform-specific setup and warns that Overture release URLs expire. This is an integration and reproducibility precedent, not proof of frictionless installation or broad customer retention. [GDAL tutorial](https://gdal.org/en/stable/tutorials/vector_duckdb_tut.html).

**Shared distribution infrastructure:** DuckDB's **2024-07-05** community-extension announcement describes platform-matched builds, signing, hosting, and installation from the SQL prompt. It also says community-extension code is not vetted by the project and can be disabled. A signed distribution channel is not equivalent to a safety audit or provider acceptance. [Community-extension announcement](https://duckdb.org/2024/07/05/community-extensions).

**Host-independent entry:** the **2026-03-09** release announcement documents `pip install duckdb-cli`, then launching `duckdb` from the virtual environment. A package manager can deliver the SQL shell itself; distribution need not start with an already-installed database server. The same release moved `GEOMETRY` into core while retaining most geometry functions in spatial, illustrating that extension boundaries can evolve. [DuckDB 1.5 announcement](https://duckdb.org/2026/03/09/announcing-duckdb-150).

### Transfer and Limits

**Transfer:** meet people at the artifact they need to analyze; publish a complete input-to-output recipe; preserve their output format; let a partner's tool or data documentation carry the capability. A dependency-export producer could be a better channel than a generic database directory.

**Does not transfer:** spatial/httpfs are first-party components benefiting from shared project engineering and distribution. A new graph extension cannot assume equivalent endorsement, maintenance, or autoloading. Remote-file pruning is not a substitute for the adjacency preparation required by a graph job. The evidence establishes documented integration pathways, but not unique spatial users, paid users, or retained cohorts.

## Case 3: pg_duckdb

### Workload and Distribution

The **2024-08-15** launch statement describes a collaboration involving Hydra, MotherDuck, DuckDB Labs, Neon, and Microsoft. Its proposition is analytical queries where PostgreSQL data already resides, with substantial compatibility work acknowledged. Conference demonstrations and a public roadmap accompanied the launch. These were resources and channels deliberately assembled around a workload, not automatic access to all PostgreSQL users. [Collaborator launch statement](https://motherduck.com/blog/pg_duckdb-postgresql-extension-for-duckdb-motherduck/).

The maintainer README demonstrates analytics against existing PostgreSQL tables, joins with external files, a preconfigured Docker path, and optional MotherDuck integration. The build guide separately requires server installation, preload configuration, and restart. Therefore a one-command disposable demonstration does not establish one-command activation inside an operator's existing production database. [Maintainer README](https://raw.githubusercontent.com/duckdb/pg_duckdb/main/README.md), [installation guide](https://raw.githubusercontent.com/duckdb/pg_duckdb/main/docs/compilation.md).

Hydra's own product documentation explicitly says its analytics service integrates DuckDB through pg_duckdb. That is **provider evidence of incorporation into an existing commercial product**, not a disclosed number of independent extension customers. [Hydra product documentation](https://docs.hydra.so/products/analytics_engine).

### Counterevidence and Transfer

The **2025-09-03** MotherDuck 1.0 article couples SQL examples and performance claims with a cloud-compute offering. It also acknowledges that reading PostgreSQL tables retains row-oriented storage and that heavy analytics can need isolated compute. Treat its benchmark claims as collaborator marketing, not a typical customer result or adoption metric. [MotherDuck release article](https://motherduck.com/blog/pg-duckdb-release/).

**Transfer:** an existing-product owner can sponsor engineering and place the engine behind familiar SQL or UI. An optional commercial operational layer has a concrete relationship to a user's problem. A workload tutorial can carry both a demonstration and a next step.

**Does not transfer:** Knight Bus does not already possess this consortium, its installed product relationships, or broad SQL semantics. Offloading to large cloud compute is not evidence for a 4 GB end-to-end product. In this bounded review, no independently verified extension adoption cohort, retention curve, or extension-attributable revenue was established.

## Case 4: sqlite-vec

### Portable Packaging and a Specific Job

Alex Garcia's **2024-08-01** v0.1.0 announcement describes local vector search through a dependency-free C extension with language-package and raw-binary distribution. The initial scope deliberately emphasized brute-force search for modest local workloads instead of a universal large-scale ANN system. The author also disclosed benchmark scope and hardware limitations. This is a useful precedent for a complete narrow capability with an honest boundary. [Maintainer release account](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html).

The Python documentation offers `pip install sqlite-vec` and loading the extension into a `sqlite3` connection. It also documents SQLite-version dependencies and environments where extension loading is unavailable, including the default macOS SQLite configuration described there. Package availability does not make runtime constraints disappear. [Python documentation](https://alexgarcia.xyz/sqlite-vec/python.html).

### Observed Integration, Not Just a Package Badge

**Firsthand practitioner/host-maintainer account:** Simon Willison demonstrates using sqlite-vec against embeddings already stored by his LLM tool. He documents plugins that bundle and register the extension with Datasette and sqlite-utils, and queries against his own TIL website's data. This is a concrete example of compatible existing bytes plus an existing-tool plugin producing a usable answer. It does not establish a commercial customer count or cohort retention. [Willison's integration account](https://til.simonwillison.net/sqlite/sqlite-vec).

The project README names Mozilla and other sponsors. Sponsorship supports maintenance; the source does not establish recurring product revenue from extension users or universal deployment by those sponsors. Likewise, the original announcement's planned hosted integrations should not be silently upgraded into proven current adoption. [Project sponsorship documentation](https://raw.githubusercontent.com/asg017/sqlite-vec/main/README.md), [dated announcement](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html).

**Transfer:** a Python wheel or CLI can be the acquisition surface, with SQL as an optional interaction layer. A plugin for a tool users already operate can matter more than a new service. Compatibility with existing artifacts deserves explicit activation testing.

**Does not transfer:** local-first vector workloads and their latency/accuracy tradeoffs are not arbitrary low-RAM graph workloads. A portable package does not prove bounded graph ingestion, memory-safe host integration, or exact output. Broad language packaging is a maintenance cost; start with the environment of an actual operator.

## What the Numbers Actually Establish

| Observed or reported evidence | Evidence class and meaning | What it does not establish |
| --- | --- | --- |
| Around six million extension downloads per week, reported on 2024-07-05 | DuckDB maintainer aggregate across extension distribution; a delivery-volume proxy. [Source](https://duckdb.org/2024/07/05/community-extensions) | Six million people, spatial installations, active workloads, retention, or buyers. Repeat environments and installs can contribute. |
| Chatbase reports more than 8,000 paying customers as of early 2026 and over $10M ARR | Company-marketing case study about the adopting application business. [Source](https://supabase.com/customers/chatbase) | 8,000 independent pgvector buyers, pgvector revenue, or causal revenue lift from choosing the extension. |
| Ring account reports deployment across nine AWS Regions | Provider/customer deployment-footprint claim. Its production incident narrative is evidence beyond an installation example. [Source](https://aws.amazon.com/blogs/database/rings-billion-scale-semantic-video-search-with-amazon-rds-for-postgresql-and-pgvector/) | Nine independent customers, a low-RAM benchmark, or extension-wide retention. |
| Willison supplies working-query examples on his own dataset; Hydra documents product incorporation | Firsthand use and provider integration, respectively. [Willison](https://til.simonwillison.net/sqlite/sqlite-vec), [Hydra](https://docs.hydra.so/products/analytics_engine) | Representative adoption rates, paid demand for Knight Bus, or continuing usage by a measured cohort. |

No star count, download count, customer-company revenue figure, or benchmark is converted here into a Knight Bus conversion rate. None of the reviewed sources supplies an extension-wide retained-user denominator.

## Mechanisms Worth Testing

These are cross-case inferences, not experimentally isolated growth effects.

| Mechanism | What the precedents support | Knight Bus implication |
| --- | --- | --- |
| Specific workload | Search, geographic extraction, and analytics give installation a purpose | Start with a missed dependency decision or failed scheduled job, not generic graph enthusiasm |
| Existing data and semantics | Existing tables, vector bytes, and geospatial formats reduce translation work | Preserve original IDs and graph meaning; count any export/build rather than claiming zero migration |
| Short activation | Packaging, SDKs, and preconfigured examples reduce steps | Measure first correct consumed answer, not successful installation |
| Trusted integration | Providers and tool maintainers can own operational work | Require an owner, support boundary, and release path; a listing is insufficient |
| Teaching and reproducibility | Tutorials connect input, query, and useful output | Publish a complete recipe with a changed-data rerun and a competent baseline |
| Benchmarks and partners | They can demonstrate a reason to try and fund engineering | Neither proves retention; include failures, cold starts, total cost, and integration effort |

The strongest skeptical alternative is **repair or consolidate into what the customer already has**. The pgvector customer stories actively support that alternative. If a supported incumbent repair completes the job, that is a successful diagnosis but not Knight Bus activation.

## Proposed Channel and Activation Experiments

**Proposals only. None was run.** The order is a learning sequence, not a delivery commitment. A willing partner with an actual artifact and release owner can move the partner experiment forward immediately.

### E1: Bring Your Own Failed Job

**Channel:** a narrowly titled reproducible troubleshooting guide aimed at operators searching for dependency-query timeouts, graph-analysis OOMs, or projection/build failures. Future participation should be opt-in; no unsolicited issue-thread promotion is implied.

**Offer:** a local diagnostic record identifying the failed stage, relevant query/projection, resource envelope, deadline, and next smallest useful test. Request an authorized redacted artifact or operator-run replay, not production credentials or mandatory raw-data upload.

**Activation:** the operator supplies a real question and reviewer; a supported incumbent repair is attempted; Knight Bus produces a correct useful answer where a residual problem remains. Record repair-only successes separately from replacement opportunities.

**Measure/stop:** time to reproducible episode, correct completions before deadline, integration/support time, and voluntary repeat use. Stop pursuing replacement when repair suffices or the workload cannot fit the agreed semantics and resources. If diagnostics attract only one-off troubleshooting with no repeated job, reassess whether this is a services offer rather than an engine funnel.

### E2: Portable Worker First, SQL Shell When Useful

**Channel:** a versioned CLI download or Python package used in the operator's current virtual environment, notebook, or CI workflow. Choose the first supported platform from the actual operator, not a speculative cross-platform matrix.

**Offer:** one native worker for input validation, preparation, supported queries, and streamed results; a wheel can package or invoke it. Follow with a SQL-shell adapter only when it removes a demonstrated workflow barrier. Host-independent means independent of a managed database provider, not independent of OS/architecture/runtime requirements.

**Activation:** install -> process the user's artifact -> consume original-ID output -> ask a second useful question -> refresh changed data. Benchmark CLI and any wrapper against the same semantics; count Python, shell, and client memory when colocated. No need to build both interfaces before observing the first workflow.

**Measure/stop:** cold install-to-answer time, manual interventions, package/runtime failures, peak whole-machine RAM, live disk, refresh completion, and operator-initiated return. Defer additional bindings if users cannot reach the useful answer or packaging support dominates the benefit. This experiment is supported by the DuckDB CLI and sqlite-vec distribution precedents, not an assertion that Knight Bus already ships those packages.

### E3: Artifact-Producer Tutorial

**Channel:** documentation or examples belonging to a dependency-export producer, build-analysis tool, or compatible dataset publisher. Overture's getting-data guide is the analogy; no endorsement or agreement exists here.

**Offer:** a small redistributable example and a clearly documented path to the reader's own node/edge or membership export. Include expected results, input meaning, preparation costs, complete output, and changed-data behavior. Use membership-native analysis only where the requested graph operator actually matches.

**Activation:** the reader replaces the sample with a real artifact and uses the answer in an existing review/report workflow.

**Measure/stop:** sample completions versus own-data completions, semantic mapping errors, second-run outcomes, and attributable qualified episodes. Page views are reach, not activation. Stop broad tutorial investment if the publisher's audience lacks the job or if its exports cannot preserve required semantics.

### E4: Existing-Product Partner

**Channel:** an existing security, dependency-analysis, or data product with a named engineering owner. The immediate buyer is the partner; its analysts or customers consume the result.

**Offer:** one bounded integration into the partner's current UI/API, using a subprocess/sidecar or in-process boundary selected from measured constraints. Agree cancellation, versioning, refresh, rollback, support ownership, and the combined host resource budget.

**Activation:** a released workflow gets used by a real end user, then survives a new input generation. A signed expression of interest, private demo, or unshipped integration is not activation.

**Measure/stop:** release commitment, partner engineering time, end-user useful completions, repeated jobs, support burden, and an explicit commercial decision. Stop when no owner/artifact/release path emerges, broad compatibility becomes a prerequisite, or the partner's current stack is sufficient.

### E5: Earn the SQL Extension and Hosted Channel

**Channel:** a workload-specific SQL recipe followed, only when justified, by a provider-supported package or integration. Do not spend the first quarter securing generic directory placement.

**Offer:** the same proven question on already-available inputs, returning results where the operator already works. Investigate host mechanics in the separate platform workstream after users demonstrate that this is their activation obstacle.

**Activation:** a user runs the supported question on their real data in that environment, receives the complete answer, and repeats after change. Distinguish a local extension, a provider-supported extension, and a remote worker called through SQL; they have different costs and ownership.

**Measure/stop:** own-data activation improvement over the portable path, provider engineering/review effort, upgrade support cost, and repeat usage. Defer if SQL is merely appealing branding or if a hosted wrapper adds overhead without removing a user obstacle.

## Measurement and Decision Rules

Use one explicit funnel, with counts reported only after observation:

1. Attributed interested operators.
2. Qualified episodes: authorized artifact, concrete question, deadline, reviewer, and current workaround.
3. Successful preparation under the complete resource contract.
4. First correct, fully consumed, useful answer before its deadline.
5. Another useful question on that generation.
6. Operator-initiated reuse after changed data or at the next relevant work cycle.
7. An explicit adoption decision: internal operational dependency, partner release, or paid agreement, recorded separately.

Record failures and disqualifications at every stage, including incumbent repairs. Repeat cadence should follow the real job; a quarterly investigation should not be judged by daily activity. Use local receipts and explicit opt-in sharing rather than making sensitive graph upload a measurement prerequisite.

Pre-agree correctness, deadline, freshness, RAM/disk, and material operator/cost outcomes with the owner. There is no evidence-based universal conversion threshold in these precedents. A kernel memory reduction only matters if the complete workflow provides something useful that the incumbent or a simpler alternative cannot economically provide.

## Counterevidence and Remaining Gaps

- **Survivorship:** recognizable successes and vendor-selected customers are overrepresented. There is no matched sample of failed extensions or failed evaluations, so this report cannot estimate the probability that an extension strategy wins.
- **Confounding:** embedding demand, provider engineering, pre-existing relationships, tutorial quality, and capability improvements happened together. Their individual growth contributions are not isolated.
- **Trust is more than distribution:** package signing and provider availability do not establish semantic correctness, security review, indefinite maintenance, or an acceptable operational blast radius.
- **Installation can be the easy part:** preparation, complete output, cold I/O, refresh, and client memory remain decisive for low-RAM graphs. Benchmark marketing frequently has a narrower boundary than the customer's job.
- **Commercial capture is unresolved:** value may accrue to a host, adopting application, or consulting relationship rather than an OSS maintainer. Sponsor names and downstream company ARR are not evidence of Knight Bus monetization.
- **Origin gap:** pgvector's release and initial interface are verified, but a first-customer narrative or original personal pain account is not.
- **Usage gap:** no extension-wide active/retained cohorts or attributable revenue were established for these cases. Tutorials and product incorporation are documented; their acquisition yield is not.
- **Knight Bus gap:** no new physical-4-GB lifecycle result, paying buyer, distribution agreement, or observed funnel exists. The proposed experiments test those gaps rather than assuming them away.

Bounded examples such as pg_mooncake were inspected but not promoted into a fifth case: the collected materials did not establish a sufficiently clear adoption/retention account. No conclusion about abandonment, acquisition effects, or current hosted availability is asserted. pg_lake and pgRouting are outside this completed comparison.

## License Identification

These identify the retrieved projects' top-level licenses only. They are not legal advice, a dependency/license audit, trademark permission, or permission to claim provider endorsement. Check the exact shipped versions and bundled components before making a distribution decision.

| Component | Exact top-level license identification | Primary source |
| --- | --- | --- |
| pgvector | PostgreSQL License | [License text](https://github.com/pgvector/pgvector/blob/master/LICENSE) |
| duckdb-spatial | MIT License | [License text](https://raw.githubusercontent.com/duckdb/duckdb-spatial/main/LICENSE) |
| duckdb-httpfs | MIT License | [License text](https://raw.githubusercontent.com/duckdb/duckdb-httpfs/main/LICENSE) |
| pg_duckdb | MIT License | [License text](https://raw.githubusercontent.com/duckdb/pg_duckdb/main/LICENSE) |
| sqlite-vec | Dual MIT / Apache License 2.0 | [MIT text](https://raw.githubusercontent.com/asg017/sqlite-vec/main/LICENSE-MIT), [Apache text](https://raw.githubusercontent.com/asg017/sqlite-vec/main/LICENSE-APACHE), [author's dual-license statement](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html) |

## Final Synthesis

The evidence favors **workload-led activation with portable artifacts, followed by earned integration**. Start with the failed-job diagnostic and minimum complete local workflow; let a credible product partner or demonstrated SQL requirement change the order. The milestone is an operator choosing to use a correct, bounded answer again after data changes, not an extension appearing in an ecosystem catalog.

This is a decision-ready research draft, not a validated GTM plan. The most important next evidence is a real operator's repeated job and adoption decision, not another platform popularity statistic.

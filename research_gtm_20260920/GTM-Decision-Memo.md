# Earn Adoption Before Ecosystem Breadth

Date: 2026-09-20. Decision research for Deterministic Compute / Knight Bus. This is a recommendation and five conditional timelines, not an implemented release, customer-validated plan or acquisition forecast. It applies a product judgment lens associated with Shreyas Doshi; these are our conclusions, not his statements or endorsement.

## Decision Frame

### The Direct Answer

**Yes, an OSS plugin can be a good first product surface. No, building a plugin does not by itself prove adoption. If forced to choose a native database extension today without a committed deployment partner, I would choose DuckDB before PostgreSQL. My broader recommendation is to make the bounded engine independently runnable, target one dependency-investigation workflow, and let the user's actual working environment determine its first interface.**

For local SQL analysts, test a DuckDB extension early. For a PostgreSQL team that cannot install new native code, offer PostgreSQL-connected execution and ordinary results without modifying its server. For an existing product with a real integration owner, put the capability inside that product. Do not implement all three interfaces in advance.

The first offer should be: **investigate the impact of a dependency change or affected package on your own versioned graph, using your available machine, and repeat after the data changes.** The lower-RAM mechanism enables the outcome. It is not the outcome itself.

This recommendation preserves algorithm-specific storage and the larger architecture portfolio. It chooses how to earn the first adoption evidence, not a universal storage format or a permanent limit to traversal.

### Three Separate Decisions

| Decision | Options | What should decide it |
| --- | --- | --- |
| User's job | Dependency investigation; membership-derived analysis; scheduled global analytics | A real consequence, input, answer reviewer and decision window |
| Execution boundary | Dedicated native process; controlled process embedding DuckDB; in-database extension; separate worker | Required integration, measured resource ownership, failure isolation and operator permission |
| Distribution channel | SQL recipe/extension catalog; Python/CLI; artifact-producer documentation; existing product; upstream contribution | Where qualified operators already encounter and repeat that job |

A PostgreSQL source does not require a PostgreSQL-native kernel. A DuckDB SQL interface does not require a separate service. A plugin is an integration/packaging decision, not an algorithm or proof of low RAM.

### Premise Check

- **Sound:** familiar interfaces can reduce switching effort, and OSS can permit local evaluation, inspection and integration.
- **Unproven:** an existing database's audience will adopt a new graph capability, our current algorithm is their bottleneck, or a low-RAM result makes adding another component worthwhile.
- **Incorrect shortcut:** permission to run SQL equals permission to install a new native library. Managed PostgreSQL catalogs and host policies are real gates. [PostgreSQL evidence](Postgres-Extension-Evidence.md).
- **Incorrect shortcut:** DuckDB provides disk spilling, so every graph extension is automatically bounded. Custom native state and the surrounding workflow need their own accounting. [DuckDB evidence](DuckDB-Extension-Evidence.md).
- **Important alternative:** query repair, different metadata placement, ordinary SQL, an existing graph extension or a factor-aware implementation might complete the job without Knight Bus.

The workable default assumption is that we do not yet have an accountable paying buyer or a provider-approved deployment. A real partner can immediately change the order below. Lack of that information does not prevent comparing the paths.

### Expert Lenses

| Lens | Question that matters |
| --- | --- |
| Product and prioritization | Which recurring consequence earns attention now, and what must we decline? |
| Database integration | Can this user install, upgrade, cancel, recover and consume results in their real environment? |
| Bounded systems execution | Does source-to-answer-to-refresh fit the entire physical resource envelope? |
| OSS distribution | How does a qualified user discover the capability, activate on their own data and return? |
| Skeptical engineer | Does a competent existing alternative solve it, and are we measuring the full job rather than a convenient kernel? |

### Candidate Approaches

The conventional strategy is to build a broad graph extension, publish benchmarks, and launch to PostgreSQL or DuckDB communities. It can generate attention, but mixes workload risk, engineering risk and distribution risk before observing repeated value.

Three useful non-obvious alternatives follow different principles:

1. **Diagnostic entry, analogous to medical triage:** locate the actual failed stage before prescribing a new engine. A successful incumbent repair is useful evidence, not a failed sales script. Avoid drifting into unlimited bespoke consulting.
2. **Reusable artifact, analogous to a manufacturing fixture:** prepare a versioned representation once for a declared family of questions, then make repeated operations straightforward. Charge preparation and invalidation; the fixture is worthwhile only when it is used enough.
3. **Component distribution, analogous to an ingredient supplier:** deliver through a tool or product whose users already have the job. Alternatively contribute the mechanism upstream. This can reduce end-user change while introducing maintainer/partner dependence.

These are product analogies, not new algorithmic inventions. The five timelines below make the operational choices explicit.

### What the Existing Research Allows Us to Claim

The [prior decision brief](../research_4gb_20260919/Final-Research-Decision-Brief.md) records a narrow, warm Bolt experiment: p99 3.9703 ms for Knight Bus versus 5.3027 ms for Neo4j, and sampled stack RSS 234.18 MB versus 374.05 MB. That is about 25.1% lower p99 and 37.4% lower sampled RSS for three small-output dependency query families. It is not a new measurement, arbitrary Cypher support, a global-algorithm result or a 4 GB complete-lifecycle proof.

The same research supplies seventeen conditional mechanisms and three storage/execution families. It does not establish a generic 250x product RAM reduction or universal fixed completion time. Favorable structural calculations are useful reasons to test a matching job, not acquisition promises.

For this research, the stringent target remains **4,000,000,000 physical bytes** and **50,000,000,000 bytes of retained prepared storage**. The provisional 3 GB worker / 1 GB other split must be measured and revised for an embedded host. Input, scratch and peak live disk remain separately admitted. The full source capture, build, result consumption, refresh and recovery cost counts.

## Timeline A: DuckDB-Native First

**Concrete opening:** recruit a local SQL analyst with an actual dependency or relationship-analysis query, a file/table snapshot, and permission to load native extensions. Prototype one selected graph operation in a dedicated DuckDB job process before promising arbitrary notebook/browser/host support.

**Why it could win:** the operator already expresses input selection and downstream joins in SQL. A graph table function can remove a real handoff. Community infrastructure supplies a distribution path, subject to build, policy and maintenance requirements. The strongest value is workflow composition, not merely SQL syntax. [Authoring and distribution evidence](DuckDB-Extension-Evidence.md).

| Horizon | Action and causal consequence | Evidence that changes the next action |
| --- | --- | --- |
| Week 1 | Observe the real SQL workflow and compare native SQL/existing extensions with a proposed narrow adapter. Establish actual runtime/client and data semantics. | If ordinary SQL suffices, do not build a duplicate engine for that job. If switching to a separate worker is the decisive obstacle, native integration gains priority. |
| Weeks 2-4 | Implement the minimum complete selected operation and bounded preparation in the controlled host, with cancellation and streamed result handling. Pin the API/build target and test an actual installation. | A correct own-data first answer, second query, and changed-snapshot answer under the joint budget justify a private release candidate. A successful sample installation alone does not. |
| Weeks 5-8 | Let additional operators follow the same recipe. Publish a reproducible comparison and seek appropriate community distribution when release obligations are met. | Observe which failures are installation, semantics, resource use or lack of need. Do not call registry acceptance retention. |
| Weeks 9-12 | Measure operator-initiated repeat jobs and maintenance burden. Add only the next demanded parameter or lifecycle capability. | Continue if the SQL surface materially improves adoption and useful completion. Otherwise retain the engine and change the entry point. |

**Lived experience:** the analyst keeps SQL joins and familiar output, but may need to wait for a prepared generation. The founder supports binary/runtime combinations and native-code trust. A notebook that retains large objects can violate a whole-machine promise even when the isolated test works.

**Architecture fork:** a fully in-process extension in a controlled job can be simpler than a thin extension plus another process. Prefer that when it preserves bounded ownership and avoids transport work. A bridge must justify two artifacts, IPC and process lifecycle. Neither choice is automatically faster or safer in the complete deployment.

**Competition:** compare with DuckPGQ or Onager for the relevant supported function, and with a competent native recursive SQL query. DuckDB's keyed recursion is already an intermediate-state optimization. [Graph alternatives](DuckDB-Extension-Evidence.md), [keyed-recursion documentation](https://www.duckdb.org/docs/current/sql/query_syntax/with).

**Stop/change criteria:** users cannot load the extension; current SQL/graph tooling meets the job; full build/output/refresh loses the benefit; the required ABI/platform surface is disproportionate; or real operators do not return after a useful first run. Do not broaden to all graph algorithms to compensate for absent demand.

**Reversibility:** high while canonical inputs/results and engine boundaries remain portable; lower after custom SQL, persisted types and undocumented binary formats spread. **Variance:** moderate, conditional on finding a SQL-native user with authority to install. This is a judgment, not a probability estimate.

## Timeline B: PostgreSQL-Native First

**Concrete opening:** secure one self-managed PostgreSQL operator, or an accountable hosted-provider/product sponsor, who has a real analytical graph bottleneck and authority to deploy a native extension. Start from that actual server version and operation.

**Why it could win:** source data, access controls, result joins and the application already live in PostgreSQL. Removing another service or data-handling step can be more valuable than an isolated kernel speedup. The pgvector cases show consolidation as a credible motivation, without establishing that graph demand follows the same trajectory. [Adoption precedents](OSS-Adoption-Precedents.md).

| Horizon | Action and causal consequence | Evidence that changes the next action |
| --- | --- | --- |
| Week 1 | Confirm native deployment rights, maintenance owner and exact input/query. Check eligible pgRouting/AGE/MADlib/other baselines and ordinary SQL. | If the server library cannot be installed, native-first is blocked for this prospect; route to an external job or another segment rather than waiting for a broad provider deal. |
| Weeks 2-4 | Build the one-operation integration, including PostgreSQL authorization, allocation lifetimes, cancellation, abort handling and result behavior. | The operator must be able to run the complete own-data job without destabilizing the source workload. Passing Rust unit tests alone cannot establish that. |
| Weeks 5-8 | Rehearse upgrade, restore, source-change refresh, overlapping sessions and rollback. Test source-load interference. | Continue only when operational review accepts the dependency and the result matters more than the added maintenance. |
| Weeks 9-12 | Attempt a released recurring workflow in that supported environment. Use its evidence to discuss distribution with the relevant provider or product owner. | A provider conversation or extension request is not deployment. Expand the supported matrix only when another qualified operator needs it. |

**Lived experience:** analysts gain SQL access; the DBA inherits a native failure and upgrade surface. The founder may spend more effort on trusted deployment than on algorithms. A prospect can like the idea and still be unable to install it.

**Architecture fork:** ordinary UDF work, background workers, trusted-language packages and external jobs are different designs. `work_mem` is not a universal native heap allowance; PostgreSQL and custom state must share the actual machine budget. A native background worker is not automatic isolation from the database's operational risk. [Native and managed-host evidence](Postgres-Extension-Evidence.md).

**Stop/change criteria:** no deployment owner; provider catalog unavailable; source interference outweighs value; permission/result semantics are wrong; broad transactional/Cypher replacement becomes a prerequisite; or existing extensions/SQL complete the task. Keep an external source/result path available as an architectural option, not as an undeclared change in benchmark boundaries.

**Reversibility:** medium before production use, lower once SQL objects, custom storage and upgrade dependencies are adopted. **Variance:** high without a sponsor, substantially lower when an operator commits an artifact, deployment window and release owner. That commitment is not currently established.

## Timeline C: Portable Worker and Python/CLI First

**Concrete opening:** offer an OSS local dependency-investigation runner to an operator with authorized CSV/Parquet or source-export artifacts. It owns preparation, supported queries, output and refresh. Start with one actual OS/architecture and one entry point; do not require every binding before release.

**Why it could win:** evaluation does not require changing a managed database server. A runnable job can be placed in CI, a notebook's companion process or a scheduled local workflow. Source and result files keep exit costs low. This is the default when the user's installation surface is not yet known, not proof that users prefer CLIs.

| Horizon | Action and causal consequence | Evidence that changes the next action |
| --- | --- | --- |
| Week 1 | Reproduce one failed or costly job, identify its consumer, and obtain both initial and changed input. Try incumbent repair first. | A reproducible residual problem earns engine work; generic interest or a synthetic graph does not establish adoption. |
| Weeks 2-4 | Complete the source-to-answer-to-refresh slice under the chosen budget; package the entry point used by the operator. | A useful own-data result and independently initiated next run establish much more than downloads. If export is the main burden, promote a connector or another path. |
| Weeks 5-8 | Publish the repeatable workflow and an authorized sample. Arrange an artifact-producer recipe or a narrowly relevant technical demonstration. | Track sample use separately from own-data use. Diagnose support steps before expanding outreach. |
| Weeks 9-12 | Add one adapter only when repeated users identify it as a blocker: DuckDB, a PostgreSQL source/result path or Python integration. | Continue if the job repeats without founder-operated handholding and beats a meaningful alternative on the chosen outcome. |

**Lived experience:** the operator gets an explicit versioned job and may avoid database approvals, but now owns files, preparation and another executable. The founder supports deployment and data interpretation rather than native server upgrades. An external worker is not zero operations.

**Python branch:** NetworkX dispatch is a credible integration when demanded, but build directly into the backend representation where possible, constrain conversions/fallback, and verify complete output memory. Do not promise a drop-in low-RAM replacement if the caller must construct the large Python graph first. [NetworkX backend contract](https://networkx.org/documentation/stable/reference/backends.html).

**Discovery branch:** a bring-your-failed-job diagnostic can attract qualified episodes. Its acceptance result may be "fix the existing query". Record that honestly. Stop offering open-ended customized engine development to every visitor.

**Stop/change criteria:** extraction/staging is unacceptable; no one returns without manual founder intervention; the workload requires live enforcement; complete output or refresh dominates; or compact existing libraries solve the same job more simply. If users consistently require SQL composition, move to A rather than defending the worker packaging.

**Reversibility:** high with open inputs, original-ID outputs and explicit manifests. **Variance:** lower deployment uncertainty than B for artifact-owning users, but discovery and repeat value are still unproven. This is our provisional default, not a mathematically demonstrated optimum.

## Timeline D: Existing-Product Partner First

**Concrete opening:** identify a security, dependency-analysis or data-product owner with a failed customer workflow and an engineering owner prepared to ship an integration. Deliver behind the product's existing UI/API rather than asking every analyst to adopt a new tool.

**Why it could win:** the partner has data access, a consumer and an existing channel. An application-level integration can eliminate more friction than a database extension catalog. The partner's release process, not the size of its claimed audience, controls whether anyone receives value.

| Horizon | Action and causal consequence | Evidence that changes the next action |
| --- | --- | --- |
| Week 1 | Obtain the exact customer operation, input/change trace, answer oracle, integration owner and release decision date. | Without those commitments this is interest, not a partner-led GTM. Do not reserve the quarter for an unowned integration. |
| Weeks 2-4 | Implement the bounded slice behind the existing workflow, selecting embedded or subprocess execution from actual host constraints. | Verify the same customer's answer and next-generation replay, plus partner-wide resource and permission boundaries. |
| Weeks 5-8 | Ship to a limited authorized cohort through the partner, with rollback, support ownership and consented usage evidence. | An unreleased integration does not count as end-user activation. Test whether the feature changes a decision or operational outcome. |
| Weeks 9-12 | Negotiate the next release/support or commercial commitment from actual repeated use. | Continue with reusable engine boundaries; decline unrelated custom features that do not serve the adopted job. |

**Lived experience:** the end user may see almost no tool change. The founder and partner engineers inherit debugging across two ownership boundaries, release negotiation and operational support. OSS does not remove those costs.

**Architecture fork:** any of the three storage families can serve the partner. D06 or D16 can take priority if the product already has compatible membership data and a useful derived-graph analysis. Do not impose PageRank on a product that needs dependency endpoints or a witness.

**Stop/change criteria:** no artifact/owner/release path; partner demands a general database rewrite; joint host limits fail; licensing or support cannot be agreed; or current product repair supplies the same value. Avoid sole-partner capture by retaining generic contracts and portable data, while respecting legitimate customer requirements.

**Reversibility:** medium before release, lower after deep deployment. **Variance:** high without a real sponsor, potentially the fastest route to useful adoption with one. It can outrank all other paths immediately when the commitment exists.

## Timeline E: Contribute Through an Existing Ecosystem

**Concrete opening:** bring a precise low-memory reproduction and a proposed mechanism to a relevant existing graph/analytics maintainer. Explore an upstream improvement, optional backend or compatible artifact implementation instead of launching a competing complete extension.

**Why it could win:** this can put the mechanism where a matching job already runs and build technical credibility. It also avoids duplicating an ecosystem's SQL, UI and integration work. The benefit may accrue mainly to the upstream project; decide whether that matches the organization's goals.

| Horizon | Action and causal consequence | Evidence that changes the next action |
| --- | --- | --- |
| Week 1 | Verify a current issue/workload and discuss a bounded contribution surface with the maintainer. Check license and ownership requirements. | A reproducible issue is useful; lack of maintainer interest or mismatch of semantics changes the target, not permission to force an unrelated architecture. |
| Weeks 2-4 | Produce the small mechanism improvement with existing project's correctness and lifecycle tests. | It must improve the real job, not only a favorable synthetic microbenchmark. Keep the scope understandable to maintainers. |
| Weeks 5-8 | Work through review/release, or maintain a separately packaged backend if explicitly appropriate. | A merged patch is contribution evidence. A release used on real data is adoption evidence. They are distinct events. |
| Weeks 9-12 | Measure use, maintenance cost and attributable opportunities. Decide whether to continue public infrastructure work, sell support/integration, or productize a separate bounded engine. | Do not infer a business from reputation alone. Choose an explicit capture model or accept contribution as the intended outcome. |

**Lived experience:** maintainers can supply valuable design correction and a real support context; the founder accepts another release cadence and may receive less brand attribution. Upstreaming can advance the thesis without creating an independent product.

**Candidate scope:** an algorithm-specific bounded preparation/execution path, a storage adapter or exact structural operation. GraphAr is a possible format complement, while DuckPGQ/Onager are actual graph-capability alternatives. Their fit and permission to integrate must be established, not assumed. [Relevant projects and limits](DuckDB-Extension-Evidence.md).

**Stop/change criteria:** no meaningful user outcome; maintainers do not want the dependency/complexity; the change requires a wholesale fork; obligations do not fit; or organizational goals require revenue/brand capture this route cannot plausibly provide. A respectful decision not to contribute is preferable to treating an issue tracker as an advertising channel.

**Reversibility:** high before public API commitments, medium after shared ownership/release obligations. **Variance:** high for near-term commercial capture, potentially attractive for OSS impact and credibility. This is a distinct goal tradeoff, not the default route to paid PMF.

## Cross-Timeline Analysis

### What Each Path Actually Optimizes

| Path | Most plausible first user | Biggest adoption advantage | Main bottleneck | Default position |
| --- | --- | --- | --- | --- |
| A: DuckDB native | Local SQL analyst allowed to load code | SQL input/result composition | Actual workflow value, native host accounting, builds | First native plugin to test when audience is SQL-native |
| B: PostgreSQL native | Self-managed operator or provider/product sponsor | Existing tables and application operations | Native deployment rights and production trust | Conditional; do not begin with unsupported hosted prospects |
| C: Portable worker | Operator who controls snapshot artifacts and execution | Evaluation without database modification | Discoverability, export/setup and repeat value | Provisional default without a committed integration owner |
| D: Partner | Product owner with a release-bound customer job | Existing end-user workflow and distribution | Sponsor commitment, release cadence, support | Can become first choice immediately with concrete commitment |
| E: Upstream | Maintainer with a matching reproducible bottleneck | Existing code/test/user context | Review fit and value capture | Strong OSS-impact route; commercial outcome explicitly uncertain |

These are relative judgments for the stated segments, not market-share measurements or success probabilities. The audience can change the ordering. PostgreSQL may be the simplest installation for a prepared self-hosted team and impossible for a different hosted user; DuckDB can be simple locally and disallowed in a locked-down analyst environment.

### Other Launch Shapes

| Tempting option | Where it belongs in this decision |
| --- | --- |
| Hosted API or free managed analytics service | A later delivery variant of C/D when users cannot operate local compute and explicitly accept upload/security/cost terms. It adds account, operations and trust work before our current full-job proof, so it is not the default first experiment. |
| Neo4j-native procedure/plugin | A possible partner/source adapter for users committed to Neo4j. Keeping its host can preserve workflow but retains source/host cost and procedure compatibility; the prior Bolt proof does not certify a JVM plugin. Choose it only for a concrete operator rather than rewriting every interface. |
| Browser demo, visual graph app or AI-agent/MCP wrapper | A discovery/consumption layer over a selected path. Useful when it removes an observed barrier; a sample visualization or agent call is not capacity or repeated-use evidence. No new wrapper is required by this research. |
| SQL-only recipe or macro | The lowest-integration control when existing execution already solves the job. It can teach and acquire users, but do not imply it contains an unimplemented custom bounded kernel. |
| Paid consulting or implementation service | A legitimate separate business when users need one-off diagnosis/integration. Count service revenue and engine adoption separately; do not call bespoke work self-serve product traction. |

The five timelines cover the principal ownership and distribution choices. These adjacent shapes can be layered onto them; they are not five additional engines to build.

### The Adoption Model

Use conditional funnel rates, not a weighted popularity score:

`repeat useful users = reached people * P(qualified job | reached) * P(eligible installation | qualified) * P(useful first completion | eligible) * P(repeat | useful first completion)`

This is bookkeeping, not an independence assumption or a forecast. More reach can lose to better job fit and deployment eligibility.

For illustration only, not estimated conversion: 1,000 people * 20% qualified * 10% eligible * 50% first completion * 40% repeat = **4** repeat users. A narrower channel with 200 * 50% * 80% * 60% * 50% = **24**. The invented rates explain why ecosystem size alone cannot choose a strategy; they do not predict either database's funnel.

Record actual counts and denominators. Distinguish people, organizations and job episodes. One enthusiastic person rerunning a tutorial is not several independent adopters.

### Attention, Adoption and a Business

| Outcome | Credible evidence | Insufficient substitute |
| --- | --- | --- |
| Awareness | Qualified readers or operators who can state the job | Raw impressions, stars or downloads |
| Activation | Their input produces a correct answer consumed in their workflow | Bundled sample succeeds |
| Retention | They initiate another useful question or next-snapshot run on the job's natural cadence | Founder asks them to rerun a demo |
| Organizational adoption | Their workflow or released product now depends on the capability, with an owner | Positive interview feedback |
| Commercial value | Explicit paid/support/operational commitment with a buyer | A downstream customer's ARR attributed to our component |

The four adoption precedents establish mechanisms such as consolidation, portable packages, provider support and task-specific teaching. They do not provide a controlled estimate that extension-first beats worker-first. [Evidence classes and case limits](OSS-Adoption-Precedents.md).

### Architecture Options Survive the GTM Choice

| Research options | User-visible reason to invest | Packaging implication |
| --- | --- | --- |
| D01-D02 bounded preparation and physical ownership | The first run and repeated runs complete without hidden resource spikes | Necessary across all delivery paths; account for host/adapter/client allocations |
| D03-D05 traversal locality and valid reuse | Useful dependency answers and cheaper eligible refresh | Snapshot input and version-aware query interface; no implicit live-transaction promise |
| D06-D07, D10, D16-D17 structural methods | Avoid expanded work or repeated computation on qualifying data | Accept original memberships/structure; do not force expanded edge tables at the API boundary |
| D08-D09 iteration-state methods | Scheduled rank jobs within an acceptable time/RAM tradeoff | Explicit iteration/error contract and output; not automatic GDS finite-iteration equivalence |
| D11-D12 exact similarity | Complete candidates/results despite constrained active state | Make candidate work, filters/ties and output visible; small top-k does not imply small work |
| D13 factorized queries | Avoid unnecessary intermediate expansion | Preserve multiplicities and factor semantics; a row-only boundary can reintroduce expansion |
| D14 embeddings, D15 communities | A useful downstream model/report with bounded execution | Separate numerical/quality semantics; cannot infer them from a traversal success |

These remain separate implementation bets. Do not build all seventeen before learning whether the first result matters. Equally, do not design an edge-only interface that prevents the most promising structural route later. Preserve canonical input identity, projection semantics and a versioned layout manifest; let each algorithm own its required representation. [Full option map](../research_4gb_20260919/Architecture-Decision-Map.md).

### Economics Must Include Preparation and Refresh

For one generation, compare total source work, preparation, all useful queries, output and refresh/retention. If Knight Bus adds build time `B` but saves `d` seconds for each comparable query, the simple time break-even is `ceil(B / d)` only when `d > 0`, other costs are equal and the generation remains reusable.

The prior example's extra 1,800 seconds of preparation and 55-second saving requires 33 queries. If the data invalidates the layout before then, the advertised warm benefit does not pay back on time. It could still win on capacity, privacy or acceptable infrastructure cost; that requires a separately stated objective. [Prior lifecycle model](../research_4gb_20260919/Final-Research-Decision-Brief.md).

Compute cost per useful completed job includes retained source costs that cannot be removed, worker time, storage/I/O/transfer, retries and operator time. Do not multiply a RAM reduction by a speed ratio to invent a billing saving. Hosted rates, storage performance and source topology need actual measurements before a price comparison.

### Structured Challenge and Revision

| Skeptical challenge | Response and resulting decision |
| --- | --- |
| "A worker adds yet another tool; pgvector won by consolidation." | Correct. C is a reversible default, not dogma. A or D wins when a real user demonstrably benefits from composition or an existing UI. |
| "DuckDB already spills and has graphs. Why build anything?" | Only proceed on a remaining workload with an independently verified benefit; include the native/existing-extension control. Generic graph functions are not differentiation. |
| "A 4 GB cap can just produce a slower failure." | Require completed useful jobs, disk limits, refresh and a deadline. Admission/refusal alone is not success. |
| "The biggest gain is D16, not a dependency walk." | Promote D06/D16 immediately when actual incidence semantics and a useful answer match. Do not turn a favorable constructed ratio into a general first-user assumption. |
| "A plugin's install command is the GTM." | Installation is one funnel stage. Source mapping, preparation, output consumption and repeat behavior decide activation/retention. |
| "Public graph demos prove a security market." | Existing public dependents queries are a baseline. Private resolved snapshots may differ, but require their own user evidence. |
| "Upstream contribution gives the value away." | It can maximize OSS impact and reduce duplication; it may not maximize our business capture. Decide which objective the organization funds. |

**Revision from the initial instinct:** the decision is no longer "which ecosystem has more users?" It is "which reachable operator has an unsolved recurring job, permission to run our solution, and a low-friction path to consuming the next answer?" The detailed evidence changes are recorded in [Lead Evidence and Decision Changes](Lead-Evidence-and-Decision-Changes.md).

## Decision Filter

### The Bet I Would Make Now

**Allocate the next work to one independently runnable, OSS, bounded dependency-investigation slice. Recruit through the user's existing artifact or analytical workflow. Keep DuckDB as the first candidate native SQL adapter, not an obligatory separate product. Do not start with a new PostgreSQL-native extension for an unspecified managed-host audience.**

The proposed operation is exact reverse dependency traversal for an agreed edge direction/type, seed package/version, snapshot and hop contract, returning distinct original IDs and any explicitly required distance. A witness is a separately specified output if needed. The existing one/two-hop proof is an oracle starting point; deeper traversal, witness semantics, general filters and complete bounded preparation still require implementation and tests. Do not label a shallow demo as general blast-radius coverage.

This is not a claim that BFS is the highest-value graph algorithm globally. It is the most defensible experiment near our evidence. Change to a structurally derived operation or a scheduled global algorithm when a real owner supplies a better matching job and consequence.

### Conditions That Change the Bet

| Observed condition | Choose next |
| --- | --- |
| Local SQL user, native code allowed, output must immediately join existing tables, separate handoff is a real obstacle | A: DuckDB-native in a controlled job process |
| Self-managed PostgreSQL team commits data, DBA time and deployment authority; source/result integration dominates | B: PostgreSQL-native evaluation |
| Hosted PostgreSQL cannot load our library but authorized extraction is allowed | C with PostgreSQL source/result integration; no native-server promise |
| Product partner supplies a real workflow, engineering owner and release slot | D before a public standalone launch |
| A matching upstream mechanism can solve the job with lower total integration cost and maintainer interest | E, with impact and business-capture goals stated separately |
| Incidence structure and compatible useful semantics expose an expansion/state bottleneck | Promote D06 or D16 within the selected channel; do not flatten the input for convenience |
| A competent existing solution meets the consequence at acceptable cost | Stop replacement for that episode; retain the learning or contribute the repair |

### A Practical Twelve-Week Sequence

These are timeboxes and decision checkpoints for one founder/small team, not promises that every engineering task can be finished on schedule. If a correctness/resource gate fails, disclose it and change the plan rather than forcing the date.

| Timebox | Main work | Tangible output | Go/no-go rule |
| --- | --- | --- | --- |
| Days 1-3 | Identify likely operators in existing contacts or relevant workflows; ask about their last actual failure, not hypothetical interest | Candidate episode record: data authorization, query, consumer, consequence, machine, current remedy, next run | No deployment or implementation assumptions hidden in a friendly conversation |
| Days 4-7 | Compare source input and output journeys for the leading interface and one credible alternative; establish the best simple baseline | One selected episode and answer contract, baseline plan, first interface choice, changed-data fixture | If no actual job emerges, run a labeled technical experiment only; no adoption claim |
| Weeks 2-3 | Verification-first end-to-end implementation of the selected operation | Reproducible initial build/query/output, with resource/failure evidence and declared limitations | Correct complete answer under the selected envelope; supported semantics only |
| Week 4 | New seed and changed-input replay; observe an operator consuming results | Full first/second-run comparison and installation friction log | Continue only for useful residual value after build/refresh/output costs |
| Weeks 5-6 | Reduce the largest observed activation obstacle; introduce only the necessary adapter/packaging | One supported release candidate and own-data walkthrough | Packaging must improve an observed obstacle, not just appearance |
| Weeks 7-8 | Reach additional qualified operators through a targeted recipe, partner or appropriate technical demo | Small cohort with episode-level denominators and failures | Distinguish shared tutorial use from independent useful use; do not spam issue reporters |
| Weeks 9-10 | Observe natural repeat use; test a support or partner-release commitment | Repeat-job records and operator/buyer decision | No default "retained" label when the founder performed all runs |
| Weeks 11-12 | Publish supported evidence and select continue/pivot/stop | Product decision memo, maintained support boundary, next justified operation | Broaden only from repeated useful value or explicit partner commitment |

**Suggested learning targets, not market forecasts:** attempt to identify five qualified episodes, obtain two authorized reproducible artifacts, and observe at least two independent operators return voluntarily before claiming repeatability beyond a single design partner. These small numbers do not prove PMF. If a job naturally recurs quarterly, use its genuine next event and state that retention remains unobserved rather than manufacturing a weekly metric.

The release can remain technically valuable with fewer episodes. The claim must then be "open technical preview," not "adoption proven." A single serious partner may justify focused commercial work even before independent-user repeatability; report the concentration risk.

### Verification and Measurement Package

| Dimension | What to record | Reason |
| --- | --- | --- |
| Semantics | Snapshot, original IDs, direction/types, duplicate/isolate handling, filters, hop/weight/order/error contract | Prevent impressive results for a different question |
| First answer | Download/install/manual steps, source capture, validation, ID mapping, preparation, query, full sink consumption | Do not hide preparation behind a warm benchmark |
| Resource use | Physical host or precisely labeled process/cgroup profile, runtime/client/cache/FFI, temp disk, retained generations, bytes read/written | A 4 GB process/container limit is not proof of a 4 GB entire machine |
| Repeat/refresh | Another seed, actual changed input, validity period, changed-generation cost, pinned old readers, fallback | Measures amortization and actual freshness |
| Failure behavior | Bad inputs, high-degree/wide-value cases, cancellation, slow sink, disk exhaustion and restart | A bounded successful kernel is not a robust workflow |
| Baseline | Repaired incumbent and relevant simple/SQL/extension/factor-aware implementation, same requested answer and source boundary | Avoid straw-man comparisons |
| Adoption | Independent operators/orgs/episodes, useful first completion, voluntary next run, consumed result and explicit adoption decision | Measures demand rather than code distribution |
| Economics | Avoidable infrastructure and operator costs, retained source and transfer costs, retry/failure cost | Do not sell RAM arithmetic as a verified invoice saving |

For short-query distributions report warm/cold status, repetitions, p50/p95/p99 and concurrency; do not infer p100 guarantees from the maximum observed sample. For long jobs report full elapsed times, unsuccessful runs and deadline compliance. The experiment's owner must define a useful improvement before observing results: a previously infeasible completion, material avoided operating effort/cost, or an acceptable latency/freshness improvement. There is no universal speedup threshold that substitutes for this consequence.

### OSS and Commercial Design

Make the advertised useful workload genuinely available in OSS: complete input preparation, the supported operation, result export, and reproducible correctness/resource evaluation. Do not make essential low-RAM execution a nonfunctional teaser. Choose and document a compatible OSI-approved licensing approach before release; exact dependency, linked-library and redistribution obligations require review. This research does not apply or change a license.

Potential later paid value is operational: supported deployment, managed scheduled execution, fleet/tenant controls, support response or partner integration. These are hypotheses, not validated willingness to pay. Do not build a billing/control plane before a workload earns it. A hosted worker must disclose retained source cost and security implications; local/private use should remain useful.

Reputation can come from a precise reproducible result: the same meaningful job on a small machine, including first build and refresh, with independent correctness evidence and honest limits. The strong story is the consequence enabled, not "Neo4j is obsolete" or a cherry-picked peak-memory ratio.

### LNO-Inspired Priority Order

This is an adaptation of the leverage/neutral/overhead distinction, not a quotation or empirical law.

- **Leverage:** choose the right failed job; preserve meaningful source structure; make the complete useful result reproducible; remove the dominant activation obstacle; earn a real repeat or partner release.
- **Necessary neutral work:** one reliable package, clear supported inputs, source credentials/permissions, cancellation, output handling and essential resource/correctness tests. Do it well without designing an entire platform.
- **Overhead at this stage:** simultaneous PostgreSQL/DuckDB/Python/browser coverage, complete Cypher rewrite, universal algorithm catalog, custom scheduler/OS, broad marketplace launch, or more literature collection that cannot change the selected experiment.

What is overhead can become leverage after a real constraint appears. A second adapter requested by repeated users is different from speculative adapter breadth.

## Evidence and Verification

| Fact-checkable question | Result of this research | Consequence |
| --- | --- | --- |
| Can Rust participate in both ecosystems? | Yes, documented pgrx and DuckDB Rust/C-ABI routes exist; maturity/version constraints differ | Do not choose from a false language-impossibility premise |
| Can every managed PostgreSQL user deploy a new native extension? | No general entitlement established; provider availability and operator policy gate deployment | B requires an eligible prospect or sponsor |
| Does native extension memory inherit a universal host cap? | No such guarantee established for arbitrary native allocations | Keep explicit physical ownership and lifecycle measurements |
| Are graph functions absent from DuckDB/PostgreSQL? | No; the reports identify existing graph/query alternatives | Differentiate on an actual complete workload |
| Is a PostgreSQL-connected non-native path documented? | Yes, ordinary client transfer and DuckDB connector mechanisms; our consistency/runtime contract remains untested | Source access and engine installation are independent choices |
| Do extension precedents prove retained Knight Bus demand? | No; cases establish plausible mechanisms, not our funnel or a causal success rate | Field experiments, not more popularity arithmetic, resolve the next uncertainty |
| Can source structure change the best first algorithm? | Yes, under the prior proposals' explicit operator conditions | Keep D06/D16 immediately available as conditional first bets |
| Have we executed the proposed next-quarter work? | No | Research completion is not implementation, customer validation or a release |

Primary evidence and source limits: [PostgreSQL](Postgres-Extension-Evidence.md), [DuckDB](DuckDB-Extension-Evidence.md), [four adoption cases](OSS-Adoption-Precedents.md), [new controls and decision changes](Lead-Evidence-and-Decision-Changes.md). The prior corpus is incorporated through its [audited synthesis](../research_4gb_20260919/Source-Coverage-Completion-Audit.md), not represented as fresh independent browsing of every linked source.

## Final Synthesis

**Build the capability as a reusable bounded engine; make the first product one complete, repeated analytical job; choose its first integration from the operator's workflow. DuckDB is the leading default native-plugin experiment, PostgreSQL-native is sponsor-dependent, and a portable or partner-delivered path may earn adoption sooner than either.**

The research narrows the next uncertainty to a concrete field test. It does not prove the market. A new dataset or accountable partner should be allowed to change the recommendation, while correctness, resource accounting and honest adoption evidence remain non-negotiable.

## Open Questions

1. Who owns the first recurring job, what failed last time, and what decision does the answer change?
2. Is the meaningful input ordinary dependency edges or an incidence-derived operator with much larger structural upside?
3. Does the operator control a local process, DuckDB extension loading, PostgreSQL native deployment, or only authorized data export?
4. What result semantics, source age, build/query deadline and total cost make the complete workflow useful?
5. Is native SQL composition a demonstrated activation requirement or simply an appealing developer interface?
6. Which exact platform/API/license/security and source-consistency gates must be closed for the selected implementation?

These are the next experiment's input and validation questions, not reasons to keep this research goal open indefinitely.

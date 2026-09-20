# Product Evidence And Workload Opportunities

Status: 32-document lane reading and synthesis complete, 2026-09-19. Research only; no implementation, benchmark execution, or customer interviews performed in this lane. Fresh structural verification confirms 32 unique source rows and contiguous recorded spans matching current source lengths; this checks bookkeeping, not semantic truth or benchmark claims.

## Scope And Evidence Rules

The assignment has 33 rows: 32 Markdown documents and one CSV support table. All are in scope. PMF003 and the graph-tech-chat archive belong to another lane. Control documents consumed: research README lines 1-55 and assignment lines 1-40.

Evidence labels: **reported measurement** means the source reports a run, not that this lane reproduced it; **specification** is desired behavior; **estimate** is unmeasured; **customer evidence** requires an attributable customer observation; **hypothesis** is a proposed mechanism or product judgment. Repetition across author-written documents is not independent validation. Exactness requires matching query semantics and result contract, not just hashes or resource receipts.

Hard scenario: 4,000,000,000 physical bytes, with a provisional 3,000,000,000-byte worker allowance; account for OS, file cache, other processes, ingestion, ID maps, build scratch, outputs, refresh overlap, recovery, and concurrent jobs. Latest user clarification makes 50 GB the prepared persistent-storage cap, not an input-size or GDS RAM assumption.

## Source Reading Ledger

Each interval below was actually displayed and read, not inferred from a hash or search. Source paths are repository-relative. All coverage not listed remains unread.

| ID | Source | Consumed lines | Substantive observation and evidence boundary |
| --- | --- | --- | --- |
| P01 | docs_PMF_01/evidence/cypher-bolt-walk-v1/compatibility-summary.md | 1-12 (complete) | Reported paired warm p99 is 3.970300 versus 5.302670 ms, but only direct Bolt, Python driver 6.1.0, read-only auto-commit and 60 fixed queries; not broad Cypher parity or end-to-end machine memory proof. |
| P02 | docs_PMF_01/evidence/cypher-bolt-walk-v1/README.md | 1-66 (complete) | Strongest initial implementation evidence: 3,997,988 nodes / 36,294,270 relationships, ordered-result comparison and index-seek requirement (21-25). Cold Neo4j boot and file-backed residency are unavailable (50-55); transport cancellation cannot interrupt running backend callback (56-60). TLS, routing, writes and transactions excluded (61-63). Test counts are historical reports, not rerun here. |
| P03 | docs_PMF_01/PMF001-Strict-RAM-Scale-Scenarios.md | 1-157 (complete) | Explicitly prices only post-build resident RAM for a 50 GB GDS projection, not ingestion or disk (29-32). Even speculative BFS range starts at 4-8 GB; most algorithms exceed this assignment's whole-machine budget. Correctly distinguishes eliminating Node2Vec walks from feasible complete training (99-109), and admits mmap alone is not enforcement (111-125). Subjective percentages and slowdown ranges are not measurements. |
| P04 | docs_PMF_01/PMF002-Budget-Bounded-Batch-Compute.md | 1-280; 281-560; 561-828 (complete) | Buyer is recurring-job owner with hours of slack, overprovisioning, versioned inputs, streamable output and NVMe (561-573), but no interviews establish willingness to pay. Whole-process equation includes build overlap (160-224); economic equation includes artifact amortization and incumbent sunk costs (527-557). Strong falsifiers include build cost, storage throttling and configured incumbents (730-745). Its constrained-optimization deadline language (123-142) must not become an unconditional completion guarantee; resource enforcement alone cannot supply one. |
| P05 | docs_PMF_01/PMF004-Deterministic-Compute-Opportunity-Atlas.md | 1-300; 301-560; 561-870; 871-1182 (complete) | Separates result/resource/time/quality/failure determinism (48-86), and correctly treats time as conditional. Broad market rankings and confidence percentages (821-845, 1170-1182) are author judgment, not market statistics. Security graph adjacency has rule/extractor compatibility costs (602-625, 892-902). Contradiction: SQL batch is ranked second and proposed as Pack 2 (878-888), whereas PMF002 explicitly avoids general joins/grouping initially (368-377). No observed buyer resolves that tension. |
| P06 | docs_PMF_01/PMF005-Deterministic-Compute-Operating-Doctrine.md | 1-260; 261-507 (complete) | Makes public Graphalytics, GDS behavior and independent mathematical oracles separate lanes (209-255); WCC requires partition equivalence, not raw component-label equality (167-177). Calls for five future interviews and one external behavior-changing user (413-426), evidence that customer validation is a gate, not already established. Shares ID maps/columns across profiles (288-309), constraining format proliferation. |
| P07 | docs_PMF_01/PMF006-Cypher-Bolt-Walk-Spec.md | 1-250; 251-500; 501-740; 741-949 (complete) | Input is 2,187,775,971 raw bytes but snapshot is 514,241,964 bytes (25-37); those are not memory measurements. One-hop multiplicity assumes simple graph, two-hop DISTINCT still needs relationship-unique path semantics and start reappearance (188-205, 291-296, 883). Runtime specification initially suggests JVM authority; implemented boundary reports native openCypher parser and separate differential oracle (895-920). Materialization and concurrency are expressly limited (886-887); network cancellation limitation survives implementation (938-941). |
| P08 | docs_PMF_01/PMF007-Bolt-Cypher-Mega-Spec.md | 1-330; 331-660; 661-990; 991-1320; 1321-1650; 1651-1980; 1981-2310; 2311-2630 (complete) | Compatibility burden is wire + session + semantics + runtime, not parser acceptance (106-172). Future bounded streaming directly addresses existing full Vec materialization (60-66, 1833-1867). Unordered results must be multisets, paths retain sequence (1675-1694). Actual customer query corpus remains an open requirement (2523-2528). Old 0.044948 vs 1514.533206 ms p99 values (2388-2391) are explicitly a narrow baseline, not comparable to P02's later 3.970300 vs 5.302670 ms Bolt result. Required parser/planner/connection memory is part of admission (1272-1286), and stale indexes must reject or use safe fallback (1314-1322). |
| P09 | docs/A-20260416141639-v001-tiny-harness-validation.md | 1-155 (complete) | Real tiny harness is 39 nodes/67 edges, 3,508-byte snapshot, 134 checked queries (18-74); it verifies plumbing and CSV parity, not graph scale. In-process RSS reported implausible 7.1-7.3 GB versus OS-level roughly 7 MB, and source explicitly distrusts it (119-136). A unit-conversion/accounting regression belongs in every later resource test. |
| P10 | docs/KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md | 1-240 (reread after combined-output truncation); 241-480; 481-697 (complete) | Proposes 13 shared layout families for 60 algorithms, not 60 unrelated engines (88-104). Correctly distinguishes Cypher property traversal from GDS projection comparison (30). Its blanket family statements need algorithm-specific proof: SCC does not universally require both directions; lowlink/flow families cannot imply algorithms are interchangeable. f32 numeric planes, u32 counts and aliases are candidate encodings, not established exactness at scale. |
| T01 | docs/KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.csv | Structural parse of lines 1-61: 16 columns, 60 rows, all algorithm/category names inspected. Full selected rows: 11, 26-27, 30-31, 35-36, 39, 54. | Support table inspected structurally, NOT claimed as full prose read of every cell. BFS row retains parent/depth/order whereas endpoint-only products might omit them. PageRank/paths use f32 and triangle counts u32, requiring numerical/overflow contracts. Node2Vec row materializes walk corpus, in tension with later PMF001 regeneration hypothesis. Nine selected rows were read in full. |
| P11 | docs/KNIGHT_BUS_THESIS.md | 1-260; 261-508 (complete) | Original buyer story is code consequence questions, who calls this and who is affected (13-16, 293-303), not an evidenced general analytics market. Synthetic keys allow trivial ID parsing (139-151), understating arbitrary-key ingestion. Explicitly separates dense-ID kernel, key handoff and HTTP benchmarks (305-356); intended host was 16 GB (455-461), not current 4 GB. |
| P12 | docs/neo4j-smoke-runbook.md | 1-60 (complete) | Owns full Neo4j setup/import ladder at only 1 MB then 50 MB (7-17); expected timings are on 16 GB / 8 CPU Mac (42-48). Installation/import time can dominate a small proof. Historical commands target the default database; this lane did not execute them. |
| P13 | docs/STORAGE_RUNTIME_ALIGNMENT.md | 1-200; 201-359 (complete) | Defines exact dual-CSR payload and 64-bit offsets (171-178), but explicitly permits large build hash maps and edge lists (333-338). Therefore bounded query RSS is not a bounded-builder claim. Incremental overlay plus periodic rebuild is future design (317-331), requiring new RAM/disk/freshness proof. |
| P14 | docs/strategic-research/A-20260416121710-storage-runtime-alignment-eli5.md | 1-295 (complete) | Companion explanation of P13, not independent validation. Specifies no graph database or broker semantics; proposes sealed artifacts and validation inspired by Iggy (132-188). Claims small visited/frontier state without occupancy bound (266-273); hubs and result size must test that premise. |
| P15 | docs/strategic-research/A-20260416131130-v001-proof-ladder-eli5.md | 1-300 (complete) | Historical v001 explicitly excludes import, generation and HTTP (55-62), so later lifecycle claims cannot inherit completeness from this proof. It itself rejects both zero-RAM and everyone-else-loads-the-whole-graph claims (154-169). This narrow historical scope does not reduce the present research goal. |
| P16 | docs/strategic-research/A-20260416144105-open-path-and-minimum-proof-eli5.md | 1-334 (complete) | Records that richer truth rows have node_type, file_path, span and edge_type but runtime discards most metadata (227-256). Speed is partly purchased by narrower semantics, creating migration friction for filtered impact analysis. A mapped open path is not automatically constant-time validation or cold I/O. |
| P17 | docs/strategic-research/A-20260416151416-rust-vs-neo4j-proof-eli5.md | 1-207 (complete) | Replaces old Python WALK benchmark with actual Rust binary (13-29, 96-109). Neo4j had lower full-process RSS at 50 MB because Rust command included truth loading/parity (111-151). Separate engine and oracle phases for attribution, but preserve their complete-machine lifecycle costs when both must run locally. |
| P18 | docs/strategic-research/A-20260525164835-faithful-rust-port-dossier.md | 1-200; 201-394 (complete) | Counterexample to language-led adoption: same files versus one-time migration are different promises (154-169). Rejects unsupported blanket 3-10x Rust gains and 3-6 month port estimate. Familiar interface reduces code changes, but session/retry/error/import/operational compatibility still determines switching cost (93-108). Ratings are subjective, not buyer survey results. |
| P19 | docs/strategic-research/A-20260525171232-knight-bus-storage-format-story-summary.md | 1-240; 241-479 (complete) | Historical synthesis of local chats and already-read notes, not new experiments (15-36). Extends snapshot idea into archive-memory graphs (241-247) and layout families, but explicitly says comparisons were against Cypher over Bolt, not GDS (386-392). Neither repetition nor the archive analogy validates a buyer. |
| P20 | docs/strategic-research/A-20260525180620-neo4j-backend-timeline-comparison.md | 1-204 (complete) | Months-to-beta and 1.1-30x speedup ranges are explicitly directional estimates (13, 120-137). Recommends preserving behavior rather than internals (147-195); source bases are derivative local notes. Cannot use its calendar forecasts as delivery evidence or storage payoffs as measurements. |
| P21 | docs/strategic-research/A-20260525203023-oltp-olap-e2e-benchmark-estimates.md | 1-191 (complete) | Models 50 GB logical graph, low writes, and dual OLTP/OLAP planes (18-30), not a 50 GB persisted cap or 4 GB machine. Acknowledges specialization may lose first-call performance (60-73) and consume 2-6x disk (95-109). Asynchronous refresh risks remain explicit (142-150); modeled 0.6-0.9x cost is not measured savings. |
| P22 | docs/strategic-research/A-20260903103552-x-thread-comment-learning.md | 1-116 (complete) | Best attributable product signal: captured Graphistry practitioner interest in embeddable hot/cold OLAP (31, 48-50); compatibility challenge (33) and lightweight-library alternatives (29) constrain positioning. Capture is not a complete immutable archive (7-11). No artifact, budget or buying signal was supplied (107-116). Source proposes bounded multi-source blast radius (94-105), but that is founder synthesis, not a customer request. |
| P23 | docs/v001-PRD.md | 1-270; 271-534 (complete) | Specifies input validation and snapshot parity, but large proof target is >=20 GiB compiled artifact (318-330), not raw CSV. Open-time offset monotonicity validation (290-295) can touch O(V) bytes; account startup scans. Neighbor-set parity (283-288) is valid for its narrow set contract, not general Cypher bags/paths. Streaming build explicitly deferred (501-509). |
| P24 | docs/v002-transition.md | 1-17 (complete) | Closing v001 does not establish lifecycle boundedness: v002 specifically targets build/verification memory, separating oracle work from runtime, and scaling beyond ~2 GB proof inputs (12-17). |
| P25 | journals/all-algorithm-lowram-atlas-progress.md | 1-92 (complete) | Reports 88 files/14,158 lines and 213 generated architecture entries, but partial agent evidence was replaced by deterministic scans (62-92). Hash/line validator success does not prove semantic reading or working algorithms. This is a provenance warning, not grounds to discard potentially useful ideas without checking them. |
| P26 | journals/cypher-bolt-walk-tdd-progress.md | 1-225 (complete) | Historical RED/GREEN chain supports narrow implementation provenance: native parser chosen, separate parser oracle downgraded to 2025.1.0/JDK21 due toolchain needs (80-100). Defaults are one million rows and 30 seconds (124-144), not hard byte reservations. Final post-refactor metrics (178-201) reconcile P01/P02; earlier run numbers must not substitute. |
| P27 | journals/docs-prd04-algorithm-storage-reading.md | 1-102 (complete) | Reports A007 ordering as security/dependency/access-path first, code graph demo, GDS research wedge (15-18). This lane has not independently read A007; main lane owns it. Final risks explicitly call strict PageRank/Louvain/WCC/FastRP hypotheses and willingness to pay unresolved (99-102), despite structural document checks passing. |

| P28 | journals/gds-complete-read-batch1.md | 1-190; 191-390; 391-583 (complete) | Final 111/111 count concerns dossiers, not implemented algorithms (539-583); next step is extracting executable Rust specs. Explicitly preserves unsupported PipelineApplications estimation (288-291), which is a useful boundary, not permission to claim universal GDS estimates. CSRGraphStore is named (37), motivating direct primary-source verification of the pointer-object premise. |
| P29 | journals/graph-database-pattern-corpus-202606.md | 1-150; 151-269 (complete) | Desktop-wide corpus retains 624 metadata gaps despite zero Neo4j/GDS-family gaps (266-269); inventory and direct source evidence are different denominators. Driver evidence highlights managed retries and fetch-size/backpressure (198-214), switching costs beyond query syntax. A partial/empty graph index is explicitly non-authoritative (224-241). |
| P30 | journals/graph-database-patterns-202606.md | 1-182 (complete) | Different 106-repository scope from P29; 98 canonical mentions plus eight direct citations (129) are not 106 substantive reads. Later 105 full indexes and six focused ClickHouse slices (157-182) establish tool coverage, not algorithm parity. LDBC update-stream adapters (106) point toward refresh verification absent from the narrow snapshot benchmark. |
| P31 | journals/neo4j-compat-lowram-mega-spec-progress.md | 1-130; 131-243 (complete) | Explicitly invalidates claims that Neo4j lacks estimates, disk-backed graphs are novel, or lower RAM alone is a company (57-58). Final 384-file union distinguishes 186 semantic reads from generated/structured classifications (214). Reports current estimator/catalog foundations without nine production kernels (202); closes research only, with no measured RAM/latency delta (228-238). |
| P32 | journals/V003-All-GDS-Surface-TDD-Journal.md | 1-175; 176-350 (complete) | 562 scanned unique procedures are an inventory target, not support (70-97). Multi-plane CSR is a supportability thesis; Tilehouse explicitly optional (281-288, 339-345), and strict 8 GB rejection says nothing about successful 4 GB jobs. No executable tests or runtime measurements at any checkpoint; checkpoint 005 precedes 004, so chronology cannot be inferred from physical order alone. |

## Early Counterclaims

- A low reported process RSS does not establish a 4,000,000,000-byte complete-machine lifecycle, particularly with unavailable mmap residency and no importer/build accounting in the compatibility summary.
- Driver compatibility is an adoption aid only for workloads inside the declared protocol/query profile; unsupported TLS or explicit transactions can block otherwise unchanged applications.
- The PMF001 strict-memory ranges must be redesigned and measured for this task, not divided down to 4 GB by assertion.

## Coverage Result

All 32 assigned Markdown documents were read completely in bounded output chunks and have unique substantive observations above. The additional CSV was inspected structurally, with nine full selected rows, not all cell prose. There are no unread assigned Markdown spans. PMF003, graph-tech-chat, A007 itself, implementation source trees and historical journals' referenced corpora are not claimed as read in this lane. This lane's completed reading does not complete the repository-wide goal.

### Clarified Budget And Workflow

Latest user clarification is authoritative: CSV/Neo4j -> budgeted storage build -> queries -> refresh, with 4,000,000,000 physical RAM bytes and a 50 GB prepared persistent-storage cap. Use 50,000,000,000 bytes as an explicit decimal working interpretation, not PMF001's 50 GB GDS RAM premise. Input staging, temporary build space, outputs, old/new generations and remote source costs must be itemized separately; cap exclusions cannot become free resources. Known algorithm families do not imply one fixed query. Live analytics exist; snapshot-first is a deliberate product/engineering hypothesis whose acceptable freshness, reuse frequency and time-to-first-answer must be established with customers. Compare combined source-plus-worker deployment cost, not an isolated inexpensive worker against the incumbent's complete deployment.

## Primary Checks

Checked 2026-09-19. These are bounded fact/prior-art checks, not an exhaustive novelty search or a new benchmark. Current documentation is not a substitute for pinning versions in experiments.

| ID | Primary source and consumed evidence | Finding and implication |
| --- | --- | --- |
| X01 | [GDS feature toggles](https://neo4j.com/docs/graph-data-science/current/production-deployment/feature-toggles/), rendered lines 123-177 | GDS uses CSR and default variable-length/delta-compressed adjacency; optional packed adjacency uses off-heap memory. Reordering and uncompressed options already exist. Therefore neither CSR nor avoiding per-edge pointer objects differentiates this proposal. Feature/version/edition settings must be pinned. The same page warns packed adjacency has no configured size limit; do not generalize the estimation page's heap-only description to all modes. |
| X02 | [GDS memory estimation](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/), rendered lines 123-258 | Production-tier algorithms are guaranteed estimate mode; supported graph/algorithm executions perform prechecks. Passing a precheck does not guarantee against exhaustion. Legacy Cypher projection estimation may execute counting queries. A bounded lifecycle and useful spill execution, not the existence of an estimator, is the candidate differentiation. |
| X03 | [Linux cgroup v2 memory controller](https://cdn.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html), Memory / Memory Interface Files / swap subsections returned by search | Accounts major anonymous, file-cache, kernel and socket memory; exposes current/peak and swap limits, with documented imperfections and page-size rounding. Use whole-machine evidence too. A worker cgroup limit is not proof that the OS and other services fit in the physical remainder. |
| X04 | [GraphChi, OSDI 2012](https://www.usenix.org/conference/osdi12/technical-sessions/presentation/kyrola), abstract | Single-PC out-of-core graph processing and computation on evolving graphs are established prior art. This lane imports no historical throughput numbers into the current budget. |
| X05 | [GridGraph, ATC 2015](https://www.usenix.org/conference/atc15/technical-session/presentation/zhu), abstract | Vertex chunks, edge blocks, streaming updates and selective scheduling are established techniques. A tiled low-RAM design is not globally new. |
| X06 | [Load the Edges You Need, ATC 2016](https://www.usenix.org/conference/atc16/technical-sessions/presentation/vora), abstract | Dynamic partitions that avoid unchanged-edge I/O are prior art. Proposed change-aware execution needs a more specific contribution and independent measurements. |
| X07 | [Python driver performance](https://neo4j.com/docs/python-manual/current/performance/), transaction and eager/lazy-result sections; [advanced queries](https://neo4j.com/docs/python-manual/current/query-advanced/), implicit transaction section | Managed calls, auto-commit calls and eager/lazy output differ. A default execute_query caller does not inherit compatibility from Session.run proof. Fetch size is not a maximum record-byte guarantee. Current advanced-query docs describe a retry change from driver 6.2; historical 6.1.0 evidence must remain version-specific. |

No factual claim about proprietary GDS internals or an inspected GDS Java class is made here. X01 is direct vendor documentation, sufficient to falsify the universal pointer-object premise; the GDS research lane owns deeper source-level confirmation.

## Product Judgment

### Conclusion

**The best-supported investigation is a budgeted analytical companion to an existing source, not an already-proven replacement database.** The product promise to test is: "Bring an export or connect your source, obtain a correct useful answer within an agreed total deployment budget, and keep answers fresh enough for the decision." Snapshot-first is a possible delivery model, not an assumption that customers have no live-data requirement.

Use a product-judgment lens inspired by Shreyas Doshi, without attributing statements to him: distinguish important problems from interesting solutions, seek costly current behavior rather than compliments, spend on the few activities that resolve the largest uncertainties, and include opportunity cost. Resource enforcement and verifiable correctness are necessary quality work. Another compatibility document or renamed storage format has low leverage until a real workflow reveals what prevents adoption.

Evidence strength today:

- **Strongest technical evidence:** P01/P02/P07/P26 report one narrow paired experiment, not four independent replications. Approximately four million nodes and 36 million edges; warm indexed one/two-hop node-ID queries via a pinned driver. This is useful evidence of a functioning interface and runtime.
- **Weak but attributable product evidence:** P22 records practitioner interest in embedded hot/cold analytics and requests for interface clarity. It explicitly has no supplied workload, budget, or buying commitment. This lane did not independently recapture that thread.
- **Unverified economic and demand claims:** cluster overprovisioning, acceptable hours of delay, storage-cost savings, and paid appetite for a 4 GB worker. P04/P05 are reasoned hypotheses, not customer studies.
- **Unverified lifecycle performance:** same-machine bounded import/build, arbitrary IDs and properties, cold startup, output backpressure, refresh, crash recovery, and concurrency. None inherits proof from a 234 MB reported warm-process RSS.

### Ranked Workloads

Ranks order learning/investment, not a validated market-size estimate or removal of lower-ranked families from this goal. "Pain" below is a candidate pain to confirm. Query parameters, filters, seed sets and even algorithms may vary within each workload; known families do not license fixed-query assumptions.

| Rank | Workload and useful decision | User / buyer hypothesis | Most painful incumbent condition to verify | Evidence and switching friction | Decisive disconfirming observation |
| --- | --- | --- | --- | --- | --- |
| 1 | Security/dependency/IAM exposure or change-impact analysis: filtered bounded reachability, then requested witness paths | Security platform engineer or analyst / security engineering or platform lead owning operational budget | Repeated exports/projections or capacity upgrades to answer a small number of consequential graph questions; missed analysis after dependency changes | Closest technical proof P02, strategic ordering reported P27/P31; no direct buyer validation. Must preserve edge types, permissions, temporal scope and explainable paths discarded by old snapshots (P16) | Real queries require fresh authorization after each write, unrestricted path enumeration, or properties absent from prepared storage; tuned incumbent is already cheap and timely |
| 2 | Scheduled connectivity/component drift and blast-radius summaries across asset or dependency inventories | Data/security engineer / platform or analytics owner | Projection peak or unreliable nightly refresh prevents complete component report | WCC has relatively compact state and an independent partition oracle (P06), but is not the measured Bolt profile. SCC is a different directed problem. Deletions and stable business identifiers are significant friction | Component membership does not drive a decision, or input changes faster than extraction/build/report can complete |
| 3 | Repeated whole-graph ranking/centrality for investigation prioritization | Analyst or data scientist / analytics platform lead | Projection plus state forces a large idle machine; repeated scans/restarts miss scheduled reporting | P03/P10 provide design estimates, not 4 GB results. Need weighted/dangling/personalization/convergence contracts; writing scores back may dominate query cost | Cheap incumbent run or simpler degree score is equally useful; topology changes before enough queries amortize preparation; page scans exceed the deadline |
| 4 | Exact filtered graph similarity/co-occurrence for candidates or risk triage | Fraud/security/recommendation engineer / domain product owner | Candidate joins and skew cause memory spikes or force broad approximations unacceptable to the task | Join/intersection family covered P10/T01, no customer artifact. top-k is small output, not small work. Filter semantics and ties are mandatory | Heavy items induce near-quadratic candidates, exactness adds no decision value, or a cheap approximate/SQL alternative meets the real acceptance bar |
| 5 | Weighted route/exposure optimization and constrained source-target paths | Domain analyst / platform owner | A recurring query family spends most cost on rebuilding representations or queues rather than a useful small answer | P10/T01 design only. Nonnegative/negative weights, path identity, ties and direction cannot be conflated. Arbitrary path predicates may explode state | Customers need all paths/all-pairs outputs, or most queries touch the entire graph with little artifact reuse |
| 6 | Community discovery and triangle/motif analysis for periodic investigation | Graph data scientist / research or analytics lead | Repeated experimental runs exceed memory or cannot reproduce a useful partition | P03/P10 hypotheses. Community objectives, seeds and stopping rules matter; triangle count needs overflow-safe accumulation. These are not substitutes for reachability | Output is exploratory but rarely acted on; exact motif cost or quality variance outweighs savings; disk/memory shape fails |
| 7 | Embedding/Node2Vec/FastRP/GraphSAGE preparation and training | ML engineer / ML platform lead | Walk corpus, dense vectors, optimizer/training state, and projection compete for RAM | P03 and T01 conflict on walk persistence versus regeneration; P28/P32 are dossier/supportability evidence, not trained-model results. Downstream ML integration raises switching costs | Removing walk storage leaves embeddings/optimizer or output too large; model utility fails despite a graph-kernel win |

**Channel alternative:** an embedded worker for graph-product vendors has the strongest explicit practitioner signal (P22), but is a buyer/channel choice spanning the ranks above, not evidence for a particular algorithm. Partner design reviews must expose deployment, tenancy, license, upgrade and support obligations. "OSS interest" is not a purchase order.

**Poor initial positioning, retained as counterfactuals:** general Neo4j OLTP replacement, universal Cypher/GDS parity, general SQL batch engine, and arbitrary all-pairs/path materialization. Their opportunity cost and switching surface are large; retain them in comparison and falsification rather than silently defining them as impossible. Security source-of-truth authorization must not be represented as an eventually consistent snapshot decision without explicit customer acceptance.

### Customer Questions

Ask for a recent failed/slow/costly job, its inputs and outputs, operator actions, and the decision it blocked before presenting a design. Interview the operator, the person using the answer, and the budget owner; they may disagree.

| Unknown | Question and evidence requested | Architectural consequence |
| --- | --- | --- |
| Importance | What did the last missed answer delay or put at risk? Show the ticket, rerun, incident, analyst time or infrastructure change | Separates consequential pain from RAM curiosity |
| Freshness | How old may data be at answer completion? Is that age measured from source event, committed source transaction, export cutoff or artifact publication? Does a deletion/revocation have a stricter SLO than an insertion? | Snapshot cadence, delta support, priority invalidation, or rejection of snapshot-first |
| Decision latency | Time to first byte, first usable decision, complete output, or all three? Is a partial answer useful and visibly incomplete? | Prevents optimizing a streaming first row that precedes the answer by hours |
| Reuse | How many actual queries and algorithm runs happen before the graph or required projection changes? Which seeds, filters, weights and algorithm parameters vary? Supply a time-stamped trace, not just one query | Determines build amortization and whether a query-specific artifact is wasted |
| Live updates | Can analytics lag while transactional Neo4j remains authoritative? Is the change feed available, ordered and retained long enough? What happens when an event is missing? | Snapshot plus deltas versus full rebuild; explicit source dependency |
| Data boundary | CSV once, recurring CSV replacement, or Neo4j export? Are arbitrary string IDs, parallel edges, null properties, labels and history required? Can source snapshots be consistent? | Import cost, edge identity, schema planes, correctness contract |
| Output | Set, bag, count, distances, one path, all paths, score vector, top-k or persisted writeback? Who consumes it and at what rate? | Output bytes, witnesses, sorting and client memory may dominate |
| Deployment | Same 4 GB machine as source, separate worker, developer laptop, hosted service or embedded component? Is 50 GB only prepared data or the entire disk allocation? | Defines memory overlap, disk peaks, operating envelope and permission model |
| Economics | What can actually be switched off or downsized? Which Neo4j/source costs remain? Show storage, I/O, transfer, operator and retry costs in addition to compute | Avoids calling a new worker cheaper while keeping the entire old bill |
| Switching | Which exact driver calls, security controls, transactions, result types, error/retry behaviors and operational procedures are mandatory? Can adoption begin as shadow analytics? | Determines adapter scope and integration/support effort |
| Commitment | Will they provide a scrubbed artifact plus a change trace, own the oracle review, and schedule a shadow-to-production decision against agreed limits? | Stronger signal than an enthusiastic conversation |

Prefer five evidence-rich interviews as P06 proposes, not five favorable answers. A paid pilot is stronger evidence, but an external team changing a real workflow under documented acceptance criteria is already informative. No interview or commitment has been obtained in this lane.

## Lifecycle And Budget

### Workflow Contract

| Phase | Must retain or do | RAM / storage / time exposure | Correctness and recovery boundary |
| --- | --- | --- | --- |
| Extract | Read CSV or obtain consistent Neo4j source version with stable external identities | Source load, network, bounded decode buffers, staging bytes, export wall time | Source watermark plus schema and extraction query; missing consistency cannot be patched by a checksum |
| Validate and map | Parse quoted/multiline CSV, types/nulls, endpoint existence, duplicate and multiedge policy; external ID mapping | Long fields, rejected-row log, external sort runs, merge fan-in, dictionary/index bytes | Reject or quarantine according to declared policy; no silent deduplication or fabricated endpoints |
| Build | Produce necessary topology, properties and indexes with bounded runs/merge buffers | Scratch, rereads, ID joins, compression, allocator/codec memory; one graph may have multiple planes | Lossless manifest of semantic choices; resume only complete verified run files |
| Seal and open | Final checks, versioned manifest, durability, mapping/decoder setup | Checksums and offset validation can scan data; cached mappings still have resident cost | Atomic publication; previous generation remains valid until references drain |
| Query | Select exact supported semantics and physical plan per request | Shared cache plus per-query frontier/state/queue/predicate/sort memory; spills and byte admission | Pin generation and configuration; caps cannot silently truncate exact output |
| Export/writeback | Stream requested IDs/paths/scores or update a downstream source | Encoded bytes, network buffers, backpressure, client/driver memory; transaction log if writing back | Completion marker separate from partial stream; retries must not duplicate effects |
| Refresh | Ingest insert/update/delete stream or rebuild; update properties and IDs consistently | Old/new overlap, deltas, invalidated caches, compaction, query interference | No mixed-generation answers; stale results labeled/refused against freshness contract |
| Recover/retire | Crash recovery, replay, checksum failures, cancellation cleanup, old-generation GC | Repeated scans, temporary leftovers, pinned readers and remote fetches | Last sealed state remains readable or failure is explicit; no lost required rollback state |

Not all phases overlap. Measure peaks over the actual schedule, not the sum of all phase maxima or an average that hides overlapping refresh and query.

### Accounting Equations

All units below are bytes or explicitly named time/cost units; `B = 4,000,000,000` and prepared cap `D = 50,000,000,000`.

```text
R_machine_peak = max_t [R_worker_group(t) + R_source_if_colocated(t)
                        + R_OS_and_other_unshared(t)] <= B
R_worker_group = shared code/cache/catalog + build_or_refresh_buffers
                 + sum(active query state + output buffers) + owned kernel charges

D_prepared_peak = max_t [unique live prepared blocks across all retained generations
                         + indexes + dictionaries + retained derived results] <= D
D_disk_peak = max_t [D_prepared(t) + raw staging(t) + temporary runs(t)
                    + unsealed replacement blocks(t) + result spools(t)
                    + logs/checkpoints/cleanup residue(t)]

T_first_usable_answer = extraction + validation/ID mapping + required build
                        + seal/open + queue + first query + required output consumption
T_repeat_answer = admission/queue + query + required output consumption
Age_answer = answer_completion_timestamp - source_consistency_watermark
```

The prepared cap includes retained old prepared generations and derivative indexes by this lane's conservative interpretation. Unsealed replacements still count in total disk; after publication they count as prepared. Never relabel a persistent index as temporary to bypass the cap. A separate scratch allowance is unknown until specified; report feasibility as conditional on its measured bound. Also test the stricter case where the entire data volume must fit in 50 GB, without assuming it is the user's settled meaning.

Provisionally reserve 1,000,000,000 bytes for OS/other use and give the entire owned worker process tree at most 3,000,000,000. That reserve is not a measurement. If Neo4j is colocated, it consumes part of the same four-billion-byte envelope and may invalidate this split. If it is remote, record its capacity and load explicitly. Bound external-sort fan-in, concurrent decoder buffers and individual records as well as algorithm arrays. X03 supplies an enforcement/telemetry mechanism, not a guarantee of graceful completion. Prevent worker eviction of essential OS memory and verify whole-machine behavior.

Do not double-count mapped pages in both RSS and page-cache totals, nor assume they are free. Report group accounting, machine accounting, process RSS as an attribution view, file-cache/anonymous split, faults, swap policy, OOM events and I/O. Linux cgroup testing is proposed; macOS historical process measurements are not equivalent evidence.

### Shape Arithmetic

Illustration only, not a dataset or measured fit: `N = 50,000,000`, `E = 500,000,000` directed edge records, 32-bit dense IDs and 64-bit offsets. Property/edge identity requirements must be added before choosing an actual format.

| Item | Formula | Bytes | Interpretation |
| --- | --- | --- | --- |
| One uncompressed CSR direction | 8(N+1) + 4E | 2,400,000,008 | Disk payload, not all necessarily resident |
| Forward plus reverse topology | 16(N+1) + 8E | 4,800,000,016 | Already exceeds physical RAM before algorithm state |
| One f64 weight per edge | 8E | 4,000,000,000 | A second aligned direction can duplicate weights or need an edge-ID indirection |
| One stable u64 edge identity | 8E | 4,000,000,000 | Not optional for some multigraph/path/update contracts; representation may share it |
| Example external ID string payload and offsets | 16N + 8(N+1) | 1,200,000,008 | Assumes average 16-byte keys; excludes lookup index, allocator slack and normalization |
| BFS visited bitmap | ceil(N/8) | 6,250,000 | Does not include frontier, depths, parents or results |
| Two worst-case dense-ID frontier arrays | 8N | 400,000,000 | Upper bound for this selected representation; streaming/spill alternatives possible |
| Optional depth plus parent u32 arrays | 8N | 400,000,000 | Only removable when the output/algorithm contract permits |
| Two f64 PageRank vectors | 16N | 800,000,000 | Excludes degrees/normalizers, personalization, residuals, topology and output |
| 128-dimensional f32 node vectors | 512N | 25,600,000,000 | Disk can fit one plane; RAM cannot hold it; training adds further state |

Compression ratios are graph/order dependent; no ratio is assumed above. Simple graphs, multigraphs, weighted graphs and property graphs with identical N/E have different storage costs. A 50 GB prepared cap does not uniquely determine N/E or query feasibility.

If an active artifact is 30 GB, retaining it while sealing another full 30 GB generation exceeds this lane's 50 GB prepared interpretation. Exact block sharing may help only when immutable unchanged blocks and IDs truly remain reusable. Otherwise schedule downtime, explicitly funded external rollback storage, a smaller physical design, or refuse that refresh mode. An active 49 GB artifact is not refresh-ready merely because it fits today.

For any plan, transferred bytes divided by measured available bandwidth is an optimistic I/O-time floor; CPU, random latency, contention and synchronization add time. PageRank repeated full scans and external-sort write amplification must be estimated this way before promising a deadline.

### Combined Economics

Compare the same decision workload over a declared horizon H, including actual changes and useful completed queries, not just cheapest hourly instance sizes:

```text
C_candidate(H) = C_retained_source(H) + C_source_extraction_and_interference(H)
                 + C_worker(H) + C_build_and_refresh(H) + C_storage_IO_transfer(H)
                 + C_adapter_ops_recovery(H) + C_migration_amortized(H)
C_incumbent(H) = C_source_and_analytics_incumbent(H)
                 + C_existing_ops_recovery(H)
Net_benefit = C_incumbent - C_candidate + valued_avoided_delay_or_failures
```

Use nonoverlapping cost categories; build compute is either inside worker charges or itemized outside, never both. Source removal is savings only if the customer can actually retire it. A remote-source worker is an additional deployment, not a complete Neo4j replacement. CSV-only and retained-Neo4j cases need separate price sheets. No cloud prices are asserted here.

For a stable generation, suppose extra preparation cost is `A`, per-use saving is `s > 0`, and incremental refresh/holding/operation cost before invalidation is `F`. Break-even requires `q > (A + F) / s`; if `s <= 0`, reuse alone cannot repay it. `q` counts useful compatible uses before invalidation, not every submitted query. With variable families, use the sum of their realized savings and charge shared preparation once. Estimate freshness as extraction lag plus build/refresh duration plus queue/query/export delay; a fast repeat query does not rescue an already stale generation.

## Architecture Hypotheses

These are proposals for verification, not measured improvements or globally original inventions. Established components include external sorting, CSR, tiling, columnar properties, immutable generations, cache invalidation and contraction bounds. The repository already proposes multiple layouts, fit/spill/refuse and receipts (P06/P10/P31). The candidate contribution is the specific coupling of freshness, variable query demand and whole-lifecycle budgets below. Other lanes must check whether the broader corpus already contains these exact combinations.

### H1: Refresh-Reserved Physical Design

**Mechanism:** select a small shared base plus optional reverse, weight, property, scan-tile and candidate planes against a query-family distribution and refresh trace. Reserve the bytes needed to construct and publish the next useful generation before spending the full 50 GB on warm-query acceleration. Queries unknown at build time retain an exact bounded scan/spill fallback where feasible; unsupported predicates are explicit, not answered from a lossy projection.

Optimize horizon cost `build + sum(query costs) + refresh + output + operations`, subject to RAM, prepared bytes, measured peak scratch, freshness and first-useful-answer limits. Profile decisions are per generation and can change with query mix. Share immutable ID/property blocks, but count overlap and decoder state. Admit a new index only when expected useful reuse before invalidation repays its build and refresh tax; uncertainty in demand should favor a reversible physical plan.

**Correctness:** alternative layouts represent the same versioned logical graph and typed properties; selection may change execution but not result semantics. Index manifests include graph generation, direction, filters, weights and mapping identity. An index for one projection cannot answer a broader query. Version mismatch uses exact fallback or fails explicitly.

**Distinctive hypothesis:** a refresh reservation can yield lower complete-workflow cost and better freshness than maximizing current-generation query speed. It may intentionally omit a fast reverse plane to keep the next refresh possible. This is more specific than "pick CSR or tiles," but not a claim to invent physical-design optimization.

**Counterexample / kill:** a changing query mix makes every chosen plane obsolete; high churn prevents block sharing; fallback scans violate latency; conservative reserve wastes capacity while a fixed dual-CSR plan succeeds. Compare dynamic selection to fixed forward-only, dual-CSR and scan-tile plans over the same complete change trace, including selection overhead. Retire the hypothesis if it does not improve a buyer-valued metric without violating others.

### H2: Change-Certified Reachability Reuse

**Mechanism:** for repeated sparse endpoint-reachability queries, retain result and a conservative dependency certificate: query semantics/parameters, source generation, versions of every expanded adjacency range (including empty ranges), and every examined filter/property dependency, including rejected edges/nodes. A refresh reuses the result only if all dependencies remain unchanged; otherwise recompute with fit or spill traversal. Certificate storage competes with topology/index bytes under H1.

**Narrow correctness claim:** fixed seed set, endpoint existence within at most k hops, local deterministic traversal predicates, and declared start inclusion. Any new reachable endpoint must first cross an adjacency range from a previously reachable vertex at depth less than k, or change a checked eligibility decision. Covering those ranges and decisions invalidates such changes. Deletion cannot change a path without touching an inspected dependency. This argument requires complete change capture and stable identity; coarse block versions are safe but cause false invalidations.

It does **not** establish Cypher bag/path semantics, minimum-hop predicates, all witnesses, global authorization predicates, weighted distances, or caches that track only successful edges. A result with paths needs additional version-bound witnesses and its own proof. Unknown schema/rule/global-filter changes invalidate the entire relevant cache. Newly eligible previously rejected nodes and reverse-edge insertions are mandatory counterexamples.

**Cost and novelty boundary:** `certificate bytes + refresh validation work + invalidated reruns` must be lower than recomputing; high-degree dependencies can make it worse. Change-aware I/O itself is old (X04/X06). The candidate combination is exact query-specific negative-dependency coverage with a refresh/storage admission rule, not caching under a new name. Benefits are conditional on repeated compatible requests; algorithm-family knowledge alone does not supply reuse.

### H3: Output-Bound Centrality Completion

**Mechanism:** keep a shared tiled/streamable transition graph and block-addressable score state, using fit vectors when affordable and external score blocks otherwise. For a buyer who wants only PageRank top-k membership, test an explicitly different output contract that certifies the ranking boundary before computing unnecessarily precise scores everywhere. For full-score/GDS compatibility, run the agreed iterations/stopping rule and return the agreed numeric values instead; no silent contract substitution.

For nonnegative normalized row-stochastic P, fixed dangling distribution, `0 <= alpha < 1`, and personalization vector v, the target satisfies `p = (1-alpha)v + alpha P^T p`. An approximate vector s has residual `r = (1-alpha)v + alpha P^T s - s`; in exact arithmetic `||p-s||_1 <= ||r||_1/(1-alpha)`. A top-k boundary gap larger than twice that bound certifies membership. Floating-point computation needs a conservative roundoff allowance; ties cannot be arbitrarily certified. This familiar contraction argument is not new mathematics.

**Proposed combination:** store generation/parameter-bound residual certificates and refresh invalidation with the physical artifact so different output contracts can share score work without confusing score approximation with exact top-k membership. A changed edge can propagate globally; it invalidates the old certificate and demands residual recomputation or a proven conservative update bound. Old scores may warm-start a new solve, not masquerade as current results.

**Falsifiers:** a full residual computation costs another whole-graph scan; the global bound may be too loose near ties; personalized/weighted queries may share almost nothing; high alpha or slow mixing requires many scans. Compare complete build/scan/output cost against ordinary out-of-core iteration and pinned GDS semantics. If certification never stops earlier enough to repay its bookkeeping, keep the straightforward solver. Exact score-vector compatibility is not validated by this experiment.

### H4: Refresh-Liability Similarity Partitions

**Mechanism:** represent a bipartite incidence graph with shared sorted neighbor lists. For exact Jaccard/co-occurrence, choose between query-local intersections and partitioned candidate accumulation with external sort/reduce. Retain candidate partitions only where measured request reuse exceeds the cost of updating them. Predict update work from affected item degrees, not merely changed-edge count; charge denominator changes, ranking updates, candidate storage and output into H1's refresh reservation.

**Correctness boundary:** insertion/deletion at `(u,item)` changes intersections between u and other neighbors of that item, but changing degree(u) also changes Jaccard denominators for u's other nonzero-overlap pairs. Retaining only prior top-k pairs is insufficient: an unretained candidate can enter the answer. Either preserve complete candidate accounting, prove sound upper bounds for omitted candidates, or recompute that query row exactly. Filters require their own degrees/candidate universe, including zero-score/tie behavior when requested.

**Cost:** a high-degree item x can induce `d(x)(d(x)-1)/2` pair contributions in an undirected pair enumeration. A one-edge delta is not a constant-cost refresh. RAM is bounded partition buffers plus lists/heap/state; disk can still be quadratic and exceed 50 GB. No approximate sketch may silently replace exact output. Sorted intersections and external aggregation are known; the hypothesis is that update-liability-aware retention beats always retaining or always recomputing similarity state for a real change/query trace.

**Falsifiers:** hub insertions force near-global row invalidation; candidate state consumes the prepared cap; repeated filters destroy reuse; direct SQL or unindexed streaming wins lifecycle cost. An exact-refusal result is technically honest but not product success if it covers most valuable requests.

## Requirements And Counterclaims

Each SHALL below is a proposed acceptance contract, not a claim of implementation. Source IDs resolve to exact consumed spans in the ledger. Experiment IDs follow.

| ID | WHEN / THEN SHALL | Counterclaim or evidence boundary | Verification |
| --- | --- | --- | --- |
| R01 | WHEN any lifecycle phase runs, THEN the complete declared deployment SHALL remain within 4,000,000,000 physical bytes with explicit source placement and swap policy | Process RSS alone is not machine memory; P02/P09/P17 | E02, E03 |
| R02 | WHEN preparing or refreshing data, THEN all retained prepared artifacts SHALL fit 50,000,000,000 bytes and peak other disk SHALL be reported/admitted separately | A 50 GB GDS projection premise is different; P03 and latest clarification | E02, E04 |
| R03 | WHEN input is CSV/Neo4j, THEN parsing, source consistency, ID mapping, property fidelity and relationship identity SHALL be verified before serving exact results | Old synthetic IDs and dropped properties understate cost; P11/P16 | E02, E05 |
| R04 | WHEN queries vary within a supported family, THEN direction, filters, seeds, weights and result contract SHALL select valid layouts or exact fallback | Known family is not fixed query; profile-specific indexes cannot leak into general answers | E05, E06 |
| R05 | WHEN results are compared, THEN set/bag/path/order/ties/nulls SHALL follow the declared contract | P08 1675-1694; endpoint hash parity is not path parity | E05 |
| R06 | WHEN a generation is published/refreshed, THEN the source watermark SHALL be visible and every answer SHALL pin one complete version | Live consistency is not automatically supplied by snapshots | E04, E10 |
| R07 | WHEN answer age exceeds the customer's limit, THEN it SHALL be rejected or explicitly accepted as stale by the caller | A small query latency does not establish freshness | E01, E04 |
| R08 | WHEN output expands, THEN buffers and individual records SHALL have byte bounds, backpressure and explicit completeness status | One million rows can be enormous; P26 defaults are not byte limits | E03, E05 |
| R09 | WHEN operations are concurrent, THEN shared and per-job memory plus refresh overlap SHALL be jointly admitted | Independent per-query caps can exceed the machine | E03 |
| R10 | WHEN a run is canceled/crashes, THEN partial output SHALL not be marked complete and recovery SHALL preserve a sealed valid state or fail explicitly | Historical callback cancellation is limited, P02/P07 | E03, E04 |
| R11 | WHEN comparing speed/cost, THEN cold, warm, first usable answer, complete output and refresh SHALL be separated and total source+worker cost SHALL be included | Warm 1.34x p99 result does not prove large end-to-end savings | E02, E11 |
| R12 | WHEN claiming GDS advantage, THEN pinned compressed-CSR/GDS projection settings and corresponding algorithm semantics SHALL be used | X01/X02 refute pointer-object-everywhere/no-estimator premises | E06, E07 |
| R13 | WHEN WCC outputs differ in labels, THEN equivalence SHALL be tested on partitions, with insertion/deletion refresh cases | Component IDs can differ without incorrect membership; P06 | E07 |
| R14 | WHEN weighted paths or centrality run, THEN weights, normalization, dangling nodes, precision, tolerance, iterations and nonconvergence SHALL be explicit | f32 substitution or silently stopping early changes semantics | E07, E08 |
| R15 | WHEN top-k similarity executes, THEN missing candidates, denominator updates, ties and skew SHALL be covered by an exact oracle | Small k does not bound candidate work | E09 |
| R16 | WHEN caches/certificates survive refresh, THEN negative dependencies and all relevant rule/schema/property changes SHALL be validated | Caching only prior successful paths is unsound | E04, E06 |
| R17 | WHEN claiming compatibility, THEN driver version, invocation style, transport/security/session/error behavior SHALL be enumerated and exercised | Three auto-commit shapes are not universal Python driver support; P02/X07 | E05 |
| R18 | WHEN admitting a job, THEN refusal reason and limit SHALL be explicit, and product metrics SHALL count useful completed jobs, not refusal rate alone | Hard caps are safety, not successful computation or PMF | E01, E03, E11 |
| R19 | WHEN estimating retained physical design value, THEN reuse before invalidation, first-answer delay and refresh liabilities SHALL be included | PMF rankings and refresh windows are not measured demand | E01, E10, E11 |
| R20 | WHEN reporting verification, THEN document checks, historical reports, mathematical reasoning and executed system tests SHALL remain separately labeled | P25/P28/P29/P30/P31/P32 demonstrate why corpus completion is not implementation | All experiments |

## Falsification Experiments

No experiments below have been executed. Freeze the customer's exact task, data/change/query trace, useful output, freshness/time limits and cost boundary before measuring. Set numerical pass/kill thresholds from that contract, not after observing a favorable result. Maintain failing tests before implementation in later engineering work; this assignment produces the tests' specifications only.

| ID / priority | Experiment and controls | Failure / kill criterion |
| --- | --- | --- |
| E01 / first | Conduct evidence-led operator/user/buyer interviews; seek one scrubbed artifact, real query mix and change trace, current deployment bill, and a shadow-deployment decision. Probe source retirement and freshness explicitly | No consequential pain, no workflow access, or acceptable latency/freshness incompatible with preparation invalidates that buyer hypothesis; compliments do not rescue it |
| E02 / first | Empty worker storage to consumed useful answer on an actual 4,000,000,000-byte deployment or documented equivalent envelope; raw CSV and retained-Neo4j variants. Include extraction, parsing, external mapping/sort, sealing, cold open and output. Compare indexed Neo4j Cypher for traversal, GDS for algorithms, and a simple native exact implementation | Any uncharged helper/source, swap escape, unbounded builder, hidden disk spill, wrong answer or missed agreed first-answer limit fails. Repeated warm wins cannot override this |
| E03 / first | Whole-group and machine memory under long IDs, giant records, hubs, large results, slow consumers, simultaneous queries/refresh, disconnect and deadline cancellation. Validate measurement units with known allocations; exhaust admission and disk intentionally | OOM, escaped child memory, silent result truncation, falsely complete partial output, unbounded cleanup or acceptance of work beyond reservations fails |
| E04 / first | Replay insert/update/delete/revocation, property/schema change, late/missing events, reader pinning and crash at every refresh boundary; compare each published version to full rebuild oracle | Mixed-generation result, stale unlabeled output, incorrect cache reuse, lost rollback state, budget breach or age beyond agreement fails |
| E05 / first | Differential/metamorphic semantic corpus: isolated nodes, self-loops, parallel relationships, cycles/start reappearance, empty/null predicates, min/max hops, direction/type filters, one versus all paths, bags, order and byte-heavy IDs. Pin Python6.1.0 Session.run; separately test managed calls/current drivers | Any silent semantic weakening fails; unsupported behavior must reject before relying on it. A parser acceptance result is insufficient |
| E06 / next | Sparse traversal H1/H2 on chains, hubs, clustered graphs and random cross-links. Vary seeds, filters, directions and repetition across refresh. Compare no cache, dual-CSR, bounded scan and certificate caching | Certificates omit a negative dependency, or validation/storage/invalidations cost more than saved useful work; random I/O misses agreed query latency |
| E07 / next | WCC plus standard PageRank on same logical weighted/unweighted graphs. Use tiny independent mathematical oracle and pinned GDS, matching partition/numeric semantics. Sweep memory budget to force fit/spill, graph diameter, alpha, stopping limits and deletion rate | No exact/contract-equivalent completion within budget; spill path changes answers; repeated scanning prevents useful deadline. A baseline cannot-fit is reported as such, not an infinite speedup |
| E08 / next | H3 top-k certification against high-precision small-graph oracle, with adversarial nearly tied scores, alpha near one, dangling components and changed personalization. Charge residual scans/roundoff bounds and compare ordinary convergence | Incorrect membership, unproven floating bound, or no net lifecycle improvement. This experiment never authorizes approximate full-score compatibility |
| E09 / next | H4 exact Jaccard/co-occurrence with skewed item degrees, changing filters and a degree-only denominator change that promotes an old nonwinner. Compare all candidates on small graphs, sorted intersections, external aggregation and a relational baseline where semantically equivalent | Missed candidate/tie, quadratic disk beyond cap, refresh tax larger than recomputation, or approximate substitute already solves the real decision more economically |
| E10 / cross-family | Replay mixed traversal/WCC/ranking/similarity requests and changes. Compare H1 to fixed artifact portfolios under identical RAM, prepared/scratch ceilings and freshness. Include query-distribution shift and low reuse | Adaptive preparation postpones useful answers, exceeds old/new overlap, or costs more than a simpler portfolio. Successful repeated fixed query is not sufficient evidence |
| E11 / commercial | Horizon cost/performance trial: retained source + extraction + worker + storage/IO/transfer + adapter/ops/retries, with measured q and invalidation. Show full-cost and marginal-cost views; separately show actually avoidable incumbent spending | Combined bill or operator burden rises without sufficient agreed decision value; source cannot be downsized despite claimed savings |
| E12 / breadth | Preserve evaluation cases for communities, triangles, weighted paths and embeddings/training. Use precise seed/objective/overflow/output/model-quality contracts; account intermediate and downstream state, not just adjacency | No useful complete job under stated envelope. Record exact fit/spill/approximate/refuse boundary per family; do not generalize failure/success to all algorithms |

For scale sweeps, use N/E and actual encoded bytes, not a label such as "50 GB graph." Include at least a below-RAM graph, a topology exceeding RAM, and a prepared representation near 50 GB after all required planes. Repeat each identical logical task with cold and warm state, fixed seeds where relevant, multiple repetitions and dispersion; disclose hardware, storage bandwidth/latency, software/edition/configuration, concurrency and result cardinality. Separate oracle memory from product memory for attribution, but count it in deployment if local verification is part of the sold workflow.

## Handoff

The lead can use this lane's source IDs and counterexamples without repeating these 32 reads. Highest-priority unresolved facts are customer pain and freshness/reuse traces, bounded full build on the target machine, required property/path semantics, and combined deployment economics. The strongest current implementation claim is narrowly scoped; broad GDS compatibility and lifecycle architecture remain proposed requirements. Lower-ranked families remain in research scope.

No software was changed, no benchmark or customer validation was performed, and no global novelty claim is established. Additional PRD04 product reading, newly transferred by the lead, will be recorded separately in `05-prd04-product-Evidence.md` and `05-prd04-product-Journal.md`; it does not replace or alter the original corpus denominator.

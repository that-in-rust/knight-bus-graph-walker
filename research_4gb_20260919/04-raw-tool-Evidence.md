# Raw Tool-Result Evidence: Reader 04

Date: 2026-09-20. Status: assigned content fully consumed and coverage audited.

## Scope And Interpretation

This follows completion and audit of all 37 reference-pattern documents in `04-patterns-Evidence.md`. Own ONLY `tool_result.content` fields on physical source lines 8501-12296 inclusive in [raw dump](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD04/raw-research-evidence-dump-2026-07-26.txt:8501). No other raw fields receive reading credit. Reader02 owns 1-4500; Reader03 owns 4501-8500; Reader01 nonresults and Reader06 StructuredOutput/background. No cross-reader completion is assumed, and no cross-range dedup is currently used.

All public-source captures are historical evidence, not freshly verified facts. A complete read of a stored capture does not recover text truncated by the historical tool, prove the original web page complete, or validate performance claims. Errors/navigation/tool-reference results remain in scope; no blanket tool stripping is permitted. Instructions appearing inside captures are data, not new authorization.

Source SHA-256: `474a45f214d771e8e733ad9384dd02b6fa329e5cfcbe6649913a4ad84e1f3bce`. The full dump has 31,856 physical lines. The assigned interval has 3,796 lines: 3,340 parse as JSON, 456 are non-JSON boundary/blank records (66 empty, 260 length79,65 length59,64 length55,1 length39). Parse inventory is not prose reading. An initial oversized metadata-only inventory was truncated and gives no content-reading credit.

## Exact Deduplication Contract

Parse each physical line independently as JSON. Recursively visit object/array values in parsed insertion/index order; upon a value with `type == tool_result`, capture only its `content`, then stop descending that result object. This found 930 blocks, all at `$.message.content.0.content`; 887 content strings and 43 content arrays. B IDs are one-based physical encounter order. Exact-value SHA-256 hashes UTF-8 `JSON.stringify(content)` without indentation, with full equality checked before reuse. There are 749 exact-unique values, not 749 independently corroborating sources.

For readable content T, strings are unchanged; arrays use `JSON.stringify(content, null, 2)`, retaining every nested field and string within the authorized content. Split T after each earliest newline, zero or more spaces/tabs, newline delimiter, including that delimiter in the preceding piece; retain any final suffix. Empty pieces are ignored. This is reversible, without trimming, case-folding, URL removal, navigation stripping or semantic normalization. SHA-256 and full equality identify exact paragraph pieces. U IDs are one-based first encounter order over B order, then piece order. Piece offsets use JavaScript UTF-16 code units, zero-based half-open; each read reports U offsets and the first source B/physical-line/field offsets. A long U is split into contiguous bounded reads; only consumed offsets count. Dedup credit becomes valid only when its representative is fully read.

All 930 T values total 2,186,859 code units; 749 exact-unique T values total 2,115,286. There are 1,843 exact-unique paragraph pieces totaling 2,086,413 code units; the longest is 45,863. Characters here means UTF-16 code units, not bytes or tokens. Source B inventory below provides physical line, T length, full value hash, representative B and U list. This makes each reused paragraph traceable to the first B containing that U and its cumulative preceding piece lengths under the exact split rule. No hash/inventory alone counts as a full read.

## Current Cursor

Read: U1-U1843 full; 2,086,413/2,086,413 UTF-16 code units. No unread assigned content remains. All 930 blocks are covered after exact-value/paragraph reuse; B930 at physical12126 reuses B3/U3 already read in R001. Physical12127-12296 contain no additional in-scope tool-result content.

## Completion Audit

2026-09-20 read-only reconstruction confirmed unchanged source SHA-256, 3,340 JSON lines plus456 boundary/blank lines in8501-12296, all930 inventory rows (physical line, length, value representative, fullhash, orderedUlist), 749 exact-unique values, and1,843 unique paragraph pieces. R001-R096 form a contiguous, nonoverlapping full-content read ledger totaling2,086,413 code units, covering2,186,859 original rendered-content code units. The first audit attempt used an incorrect B-prefix inventory matcher and failed; the corrected column parser passed. These are coverage checks, not code tests or algorithm benchmarks. One later artifact reread was tool-truncated and received no new source-reading credit.

## Research Takeaways

All following pointers identify historical stored tool content, not freshly fetched pages. The full captured prose and errors remain in the immutable source; exact inventory and read spans below preserve retrieval without duplicating megabytes into this artifact.

| Evidence | Defensible implication for the4GB/50GB work |
| --- | --- |
| Physical8705,10249,11820: staff projection requirement, Arrow direct-buffer to on-heap conversion, conditional analytics heap guidance, limited enterprise demand for spill | Count extraction, conversion, graph, algorithm and writeback overlap. No pointer-per-edge conclusion follows. Low-RAM feasibility and buyer demand are separate questions. |
| Physical11599 low-memory analytics format;11806 topology/property sharding;11826 GA followsEAP | Packed storage and separated properties are not unexplored concepts. Predicate properties may be needed before traversal. Distributed100TB claims do not prove standalone4GB operation. |
| Physical10371 Capacities migration;11043/11054 reviews;11492 NASA workflow;11584 tiny distributed deployments | Product tests need compatibility, identity, backups, tenant/PII isolation, rollback, cancellation, nonproduction footprint and complete output, not only kernel speed. Capacities is Dgraph-to-Postgres, not Neo4j. NASA includes separate EC2/LLM/S3 resources. |
| Physical10528/10529 GFQL;10603/10614 Slater;8742 persistence-tax discussion | In-process dataframe graphs, bounded disk-page caching and complete episodic load/run/extract/shutdown accounting already have prior-art leads. Slater performance/storage figures and any-data vendor claims remain unreplicated. |
| Physical10877 Neptune count/OOM;11043 ownership paths;11492 context triplets | Small output does not imply small working state. Weighted path products, temporal filters, reached sets and graph-valued triplets are different semantics. Account output serialization, client retention and backpressure. |
| Physical11530/11992 product pricing distinctions;11703 canceled IRS award;11952/11956 DEA transactions | Do not sell savings using mixed tiers,720/730h monthly artifacts, incomplete SKUs, canceled orders or cumulative transaction totals. Repeated captures are not independent corroboration. |

The four mechanism-transfer hypotheses and verification-first experiments remain in `04-patterns-Mechanism-Transfers.md`. None became a measured result through this reading. Apply their full lifecycle budget: CSV/Neo4j export and bounded sorting/build, stable50,000,000,000-byte persistent storage plus explicit temporary/output/refresh coexistence, query state, complete result delivery, and crash-safe refresh, all within4,000,000,000 physicalRAM bytes. A remote source can be a supported workflow but cannot stand in for proof of a wholly coresident machine. Reject a workload when its minimum retained graph, required output or simultaneous generations exceed the stated storage scenario; do not silently narrow it.

## Progressive Reading Ledger

| Read batch | Actual unique spans consumed | Observation |
| --- | --- | --- |
| R001 | U1-5 full; U6[0,986); source8503-8533 | Reviews describe governance/migration pain rather than a demonstrated graph-kernel bottleneck; Infinigraph capture claims distributed100TB+ scope, not a4GB deployment. |
| R002 | U6 offset986 through U51 offset8 (end exclusive); 13828 code units | Historical vendor claim is property sharding while topology stays logically whole; clustered horizontal scaling is not a single-machine RAM proof. HN replies are skeptical opinions, not scalability tests. 404/403 and invalid-model errors have no technical-result evidence; navigation snippets retained. |
| R003 | U51 offset8 through U83 offset5439 (end exclusive); 20915 code units | Source8559 is keyword-selected historical output, fully consumed as a capture but not full upstream prose. Source8563 describes topology-local traversal, hashed property shards and batched late properties; first-version fixed shard counts and property-heavy fit limit the scope. This is prior art for topology/property separation; property predicates may still need early fetch/index evaluation. |
| R004 | U83 offset5439 through U103 offset14208 (end exclusive); 22064 code units | Source8575 GA announcement postdates the earlier EAP text, so capture date/version matters. Claims of infinite scale or hallucination-free GraphRAG are marketing, not proved bounds. Source8600 aggregates mixed-year pricing anecdotes, including both unaffordable and reasonable reports, plus irrelevant hiring hits; no universal current price conclusion follows. |
| R005 | U103 offset14208 through U107 offset17111 (end exclusive); 23487 code units | Source8600 claim that traversals are O(1) conflates constant-time adjacency access with total visited-edge/output work. Source8604 Graphiti commenter attributes most latency to embedding API, so graph-kernel speedup alone may not improve the complete product. Pricing/license opinions span2009-2026 and are neither current quotes nor legal conclusions. |
| R006 | U107 offset17111 through U108 offset16887 (end exclusive); 23775 code units | Sources8604-8608 preserve noisy search outputs, including unrelated salary/economics results, not a validated70k Neo4j price. Relevant8604 comments describe good Neo4j DX/GDS algorithms with careful entry-point modeling, Python Arrow flows, and demand for graph-valued composable output; those are compatibility/product leads, not performance evidence. |
| R007 | U108 offset16887 through U114 offset8905 (end exclusive); 23325 code units | Source8611 contains vendor-disclosed Memgraph pricing and user pushback; no independent lifecycle comparison. Sources8636-8641 compare historical live/Wayback snippets: monthly totals use different hour conventions despite same hourly numbers. The $0.40/GB/hour AGA metric is RAM-time in the capture, not graph storage size or completed-job cost; LIVE means historic run, not fresh verification. |
| R008 | U114 offset8905 through U120 offset15 (end exclusive); 23321 code units | Sources8646/8650 archive timestamps/digests establish capture provenance, not continuous price history. Source8654 flattens several product cards into one list, so features must not all be attributed to AGA. Explicit no-ETL/any-data wording competes with generic sidecar positioning; extraction and projection work still require accounting. Four CPU cores is not four GB RAM. |
| R009 | U120 offset15 through U138 offset269 (end exclusive); 22237 code units | Sources8684/8690 AGA press release explicitly routes any-data access through Pandas projections despite Zero-ETL wording; client dataframe memory belongs in lifecycle accounting. Customer-reported2X/80% claims lack reproducible workload details here. Source8705 starts an eigenvector projection-OOM case with billions of vertices and32g heap; this is not yet evidence that50GB disk or4GB RAM can hold its requested result/state. |
| R010 | U138 offset269 through U163 offset3643 (end exclusive); 21544 code units | Source8705 full thread reports64GB host/~4TB disk and staff reply: in-memory projection/no spill at that2025 date, legacy Cypher projection least optimized, estimators exclude database memory, prior database-direct version reportedly100x slower. Out-of-core was prioritization gap, not never considered. JVM compressed-oop advice does not imply pointer-per-edge GDS. The4TB workload is outside50GB disk without a justified smaller selected graph. |
| R011 | U163 offset3643 through U181 offset329 (end exclusive); 22241 code units | Source8733 release history already includes standalone sessions,2GB recommendations, idempotent retries and main-thread-only cancellation; cannot claim these as absent universally. Source8742 vendor persistence-tax article explicitly advocates total job cost and load/run/extract/shutdown, limiting novelty of episodic sidecar positioning. FinOps navigation-only fetch8764 cannot prove zero results; actual historical index queries8788 are query-specific zero hits. |
| R012 | U181 offset329 through U196 offset10047 (end exclusive); 22519 code units | Source8795 index controls show search works for some database terms; zero graph hits still cannot establish absent demand. Source8831 explicitly labels30-day monthly pricing, explaining normalization; source8839 connection failures and8843 recorded rejection are historical failures only. Source8864 introduces Business Critical/renamed VDC and15X read-capacity vendor claim: throughput scaling is not an algorithm-latency measurement. |
| R013 | U196 offset10047 through U201 offset8523 (end exclusive); 23408 code units | Sources8868/8878 retain full stored pricing slices across versions; backup-retention and product-name changes matter alongside hourly rates. A4GB Aura memory row includes only8GB listed storage in these captures, so it is not comparable to our independent4GB physical/50GB persistent scenario. Archived missing snapshots cannot establish when a pricing transition happened. |
| R014 | U201 offset8523 through U206 offset275 (end exclusive); 23408 code units | Source8887 archives show product-tier history, not present availability. Source8907 government award JSON includes Neo4j-only, mixed-software, training and hardware bundles across unequal periods; award totals cannot be treated as annual per-seat/core prices. Its last record is historically truncated, so no complete award-count claim is valid from that capture alone. |
| R015 | U206 offset275 through U220 offset1285 (end exclusive); 22582 code units | Sources8918/8931 show zero-dollar administrative modifications and an award deobligated by4498388.91 plus0.01; summing gross awards would double-count the apparent replacement. Source8934 describes4production machines with512GB/24cores but clipped specification; not evidence of comparable4GB workload. Source8970 parent procurement vehicle agency is NASA while end-user award is DEA; agency attribution must follow actual awarding/funding fields. |
| R016 | U220 offset1285 through U227 offset9011 (end exclusive); 23221 code units | Source8974 normalizes two truncated descriptions and computes1.79348 ratio; matching wording does not prove same licensed capacity/support scope. Source8975 repeats gross outlays across reporting months and is clipped; avoid summing cumulative fields. Source8981 confirms personal-machine CTI developer license use, a local-user lead, not validated buyer fit. Source8986 broad vendor results include many non-Neo4j purchases. |
| R017 | U227 offset9011 through U242 offset5549 (end exclusive); 22481 code units | Source8997 infers not-fixed-width/copy-paste from182 vs183 characters; that only excludes one particular fixed character cap, not upstream clipping or equal license scope. Its described-price79.35% and obligated85.45% changes differ, with3.4% unexplained gap; retain separate facts and reject universal price-hike conclusion. Sources9027/9028 are clipped API objects, not missing fields equal to null. |
| R018 | U242 offset5549 through U255 offset3848 (end exclusive); 22677 code units | Source 9032 hasNext=false closes only that transaction query, not all procurement coverage. 9041 price/obligation arithmetic leaves a 3.4% unexplained gap, not a proven markup. 9066 highlighted/text HN duplicates are not independent testimony. 9072 distinguishes replicated, Fabric-federated and property-sharded deployment; none establishes a 4 GB out-of-core kernel. 9084 is a filtered historical capture, not the full original page. |
| R019 | U255 offset3848 through U272 offset2879 (end exclusive); 22305 code units | 9084 completes the property-sharding capture: the sample performs initial name predicates and batches final property retrieval, which does not imply all predicates can be delayed. 9095 capacity pricing includes storage/IO/backups for its Aura offer and separately names an Infinigraph sales license; conflating them would misprice a deployment. 9119-9130 show the specific four-machine/512GB order at zero obligation and a GET-method error, not a priced active order. 9143 is a tool-generated absence summary, while 9147 is only an application-shell HTML capture so far. |
| R020 | U272 offset2879 through U314 offset4114 (end exclusive); 19999 code units | 9151 and 9158 explicitly reconcile 4,498,388.92 - 4,498,388.91 - 0.01 = zero for the old order, while 9162 separately retains the replacement renewal amount; gross awards would double-count. 9175 again incorrectly describes traversals as O(1), so user quotes cannot supply complexity evidence. 9180 is a secondary generated price summary, and 9185 is the actual captured 2018 page: its licensing assertions are historical author statements, not current legal guidance. Support scope and development-versus-production spend are useful interview dimensions. |
| R021 | U314 offset4114 through U365 offset146 (end exclusive); 19142 code units | 9185/9189 distinguish production cores, test instances, disaster recovery and support response levels; a single per-core scalar loses the bundle contract. 9200 staff clarification separates GDS licensing from database Enterprise and says four logical threads, not four GB, with no RAM limit AFAIK at that historical date. 9204 exact text matching establishes quoted description fidelity, not that the canceled order remains active. 9227-9239 Neptune pricing captures already describe in-memory analytics and terminate-after-job billing; the generated summary overstates usage-based versus provisioned because metered provisioned capacity is also duration billed. |
| R022 | U365 offset146 through U378 offset6029 (end exclusive); 22633 code units | 9239 distinguishes Analytics m-NCU (one GB) from Database serverless NCU (approximately two GB); substituting either into the other price model is invalid. 9242 explicitly includes I/O, backup, replicas and notebook-ready time, all relevant lifecycle cost classes, while its 50GB storage example does not establish a 4GB physical machine. 9271 captured launch PR calls Pandas projection Zero ETL and unlimited concurrent sessions: client materialization and aggregate session RAM remain unaccounted by that wording; customer-reported 2X/80% figures are not reproducible measurements here. |
| R023 | U378 offset6029 through U391 offset3962 (end exclusive); 22678 code units | 9271 customer quotations concern independent analytics-memory scaling and 2.2-billion-record identity resolution, not a 4GB footprint; disclaimer narrows the headline accuracy claim to customer benchmark data. 9286/9295 already market separate compute/storage and Bolt subgraph projection, killing broad novelty of an analytics sidecar. 9298 returns404 for team pages, so Hasbe absent from a failed team-page scrape is no evidence of a personnel change. 9323 historical hourly/monthly tiers imply730-hour examples; reserve different cluster/support scope in comparisons. |
| R024 | U391 offset3962 through U405 offset1088 (end exclusive); 22584 code units | 9347-9378 repeat procurement via distinct filtered outputs; graph matching incorrectly catches POLYGRAPH at9364, a concrete false positive in demand evidence. 9358 claims COUNT100 but its captured text is clipped, so a missing renewal cannot be inferred absent. 9360 reproduces two transaction amounts123750-description versus127957.50-obligation and zero administrative modification without explaining the gap. 9404-9408 show unchanged0.09/hour but720-versus730 monthly hours, not an hourly price hike; 9412 archive digests identify versions but are not a complete price series. |
| R025 | U405 offset1088 through U418 offset1264 (end exclusive); 22681 code units | 9416 includes old/new price tables together within a capture, so matching the first numeric row is unsafe. 9426 records missing local archive files explicitly. 9461 is the complete stored 198-line press capture: property data sharding preserves logical topology, while no ETL/duplicated storage and full ACID remain vendor claims, not evidence of single-machine4GB execution. Its customer quotes express anticipated scale use and existing workloads, not benchmark validation. 9494 flattened Fully/Self Managed labels demonstrate that scraped adjacency can mix tab headings. |
| R026 | U418 offset1264 through U425 offset2421 (end exclusive); 23221 code units | 9503 explicitly says pricing/features subject to change. 9537 is another historically clipped JSON result, not a complete100-row response despite its limit. 9541 isolates nine IRS keyword awards and19non-IRS, but cannot infer a time series of equal configurations: 2019 short period, 2022 multi-product bundle, 2024 bundle and2025three-year renewal differ. 9543 parent NASA GWAC vehicles are procurement lineage rather than NASA end-user licenses; missing base option fields are not zero. |
| R027 | U425 offset2421 through U438 offset11180 (end exclusive); 22667 code units | 9548-9564 separate transaction deltas from cumulative award totals and retain closeout-only changes; totaling totalBaseAndAllOptionsValue across modifications would overcount. 9568 notes unequal periods and product mixtures: even the 41-day annualization is not a defensible license unit price without service coverage. 9598 public comments identify ecosystem migration costs (authentication/pagination), schema consistency and multi-tenant vector isolation as distinct from traversal speed; noisy hiring/IPv6 hits and clipped comments cannot be counted as Neo4j migration reports. |
| R028 | U438 offset11180 through U469 offset423 (end exclusive); 20993 code units | 9599 reports mixed read/write HA limitations and licensing risk as historical opinions, not measured current constraints. 9603 includes an Arango benchmark-correction title, a reminder to inspect corrections before reusing comparative claims. 9604/9612 are CAPTCHA captures, retained but not search evidence. 9616 includes positive Neo4j enterprise experience and unit tests catching query incompatibilities;800 concurrent users is not800QPS and says nothing without query shape/hardware. Quine throughput/cost claims and century-to20minutes approximation anecdotes lack same-semantics reproducibility. |
| R029 | U469 offset423 through U521 offset0 (end exclusive); 18993 code units | 9616 contains a direct correction of the StackOverflow Postgres claim to MSSQL; it also distinguishes cache-served traffic and20GB examples from4GB performance. Positive use case: graph-assisted mapping of highly normalized legacy relational data into documents, needing enriched entities/edges and ad-hoc relationships, not generic OLTP replacement. 9622 already proposes CSV/Parquet-to-notebook plus throwaway Neo4j and asks freshness, search-vs-analytics, edgecount, propertywidth and UI/API questions; broad extract-first ephemeral positioning is prior art. 9632-9637 mix zero-result summaries and CAPTCHA summaries, neither proves no migrations exist. |
| R030 | U521 offset0 through U592 offset0 (end exclusive); 17251 code units | 9643-9648 finally return concrete migration leads but mostly vendor comparisons, Reddit crossposts and third-party estimates: separate one migration story from multiple copies. 9653 NASA cost/ease summary explicitly lacks graph size and dollar figures, so it cannot demonstrate50GB/4GB suitability. 9658 points to a hosted direct Meza account for later content, while search-date/title mismatches are not an event timeline. 9659 includes both60million-edge positive production experience and incompatible comparative benchmark snippets, requiring workload evidence rather than cherry-picking. |
| R031 | U592 offset0 through U648 offset8202 (end exclusive); 18699 code units | 9663 tool-generated summary adds NASA scale27k nodes/230k edges on EC2, so this is a potentially small useful workload, not evidence that large50GB graphs fit4GB. 9664 Cycode selection favors multi-model, multi-tenant and self-hosted delivery, not just speed. 9678 explicitly identifies Sightfull as Neptune-to-RDBMS, so recasting it as a Neo4j migration would be false. 9690 is a persisted-output pointer/preview only;9694 provides the fuller captured content now being read. 9694 Arrow/GIL comment raises multi-user process sharing versus isolation RAM accounting. |
| R032 | U648 offset8202 through U648 offset32071 (end exclusive); 23869 code units | 9694 source capture lines32-121: positive GRAG report attributes performance to a sound graph entry strategy and graph modeling, not Java-vs-C++ language. Graph-valued composable results and standard exchange formats are specific product requests; SPARQL CONSTRUCT is named prior art. Natural-language query comments prioritize semantic name/selection disambiguation and performance guardrails over syntax, so faster parsing alone would miss that pain. Repeated hiring posts and legal speculation retained but not treated as independent demand or licensing authority. |
| R033 | U648 offset32071 through U679 offset5944 (end exclusive); 20989 code units | 9694 tail explicitly distinguishes overprovisioning from query/index fixes; cheap baseline tuning must precede replacement claims. 9697 provides protein-interaction visualization and POLE entity extraction use cases, but also speculative assertions that query plans are always identical or pointer structs trivially efficient; these do not override inspected GDS packed-adjacency evidence. 9702 reviewer sentiments include affordable and expensive assessments and inferred Stayed status without proof of long-term retention. 9741 begins a differently formatted full press capture; repeated navigation is read, not discarded. |
| R034 | U679 offset5944 through U684 offset7398 (end exclusive); 23406 code units | 9741 full remaining press capture separates absolute up-to80%model-accuracy wording from footnoted50-80%greater-accuracy, and2Xspeed from2Xinsight efficacy: they are different metrics. It explicitly uses parallel in-memory algorithms and expects other-language support later, so marketing cannot prove bounded-memory extraction. 9744 is a four-item list excerpt, not independent evidence. 9750 failed press URLs still return SVG/navigation source; these exact stored contents are retained and consumed without interpreting them as product claims. |
| R035 | U684 offset7398 through U689 offset1448 (end exclusive); 23407 code units | 9756-9761 sitemap timestamps are last-modification dates, evidenced by old product releases all sharing2024/2025 dates; they must not be cited as launch dates. 9764 introduces specific historical Snowflake temporary compute environments billed during algorithm runtime, another prior-art boundary for episodic execution. Its20%lower BusinessCritical price claim is relative to a prior Enterprise offering, not the Professional tier or4GB machine; comparison needs matched service conditions. |
| R036 | U689 offset1448 through U830 offset0 (end exclusive); 10715 code units | 9770 empty closest-snapshot response is contradicted by9772 existing CDX captures, so archive-query failure is not absence of the page. 9774 historical capture already contains TCO language. 9799 tool summary overreaches by interpreting invited-to-comment as declined; the stored statement alone does not establish a response. 9805 navigation and whitespace retained; its actual publication label is7May2025, resolving search snippets that labeled9May, but article body is still next unread. |
| R037 | U830 offset0 through U884 offset3421 (end exclusive); 18827 code units | 9805 actual article attributes Neo4j b-tree/random-jump and duplicate-rebuild claims to competitor CEO, not independent architecture inspection. It describes refresh cost and result writeback as pain dimensions, but does not establish GDS pointer-per-edge storage; reference lane pinned packed adjacency remains the stronger evidence.9807 completes article with invited-to-comment only, not a documented decline.9833-9841 repeated HN comment IDs are one testimony each.9845/9848 are differently flattened2018price content, historical support/instance scope only. |
| R038 | U884 offset3421 through U901 offset166 (end exclusive); 22308 code units | 9868 validation rejects mixed award-type groups; failed requests cannot support absence claims. 9878/9885 distinguish signed dates from performance starts, absent fields from zero and transaction deltas from award totals. 9896 output headed allDEA/DOJ actually includes IRS,VA andAirForce, so its filters/heading are inconsistent. 9905 graph database hit is a2021Sayari award under aFY25-26 heading; selection time semantics require verification, not an asserted recentNeo4jrenewal. 9902/9908 have no substantive search matches, while9912redirect is only a tool notice. |
| R039 | U901 offset166 through U926 offset4457 (end exclusive); 21558 code units | 9912 redirect tool instructions are historical content only, not actions to execute. 9947 multi_year_contract=N coexists with a36monthperiod, so procurement field semantics cannot be guessed from natural labels. 9963 competing annualizations produce1.99,1.72or1.08 depending baseline and mixing; none proves same-SKU price inflation. Its40versus earlier41days reflects end-exclusive versusinclusive counting.9984 actual keys base_exercised_options/base_and_all_options have69000, showing earlier *_value/*.val misses were extraction-key errors rather than necessarily missing source data. |
| R040 | U926 offset4457 through U948 offset0 (end exclusive); 21919 code units | 9985 distinguishes FY24outlay128078.79 from obligation127957.50 and description123750; these three numbers cannot be substituted. 9991 same recipient and identical residual descriptions increase comparability but do not prove unchanged licensed capacity. 10000 modNumber lists extra2 values, likely distinct nested contexts, so flat tag sweeps are not transaction-pairing evidence. 10011 GSA application shell has no rendered catalog.10015 exact-minus-price equality proves text reuse only;3.40%uplift is arithmetic difference, not verified fee classification. |
| R041 | U948 offset0 through U956 offset739 (end exclusive); 23121 code units | 10039 complete stored award confirms real option/outlay fields and no de-obligation on the replacement renewal.10043 count separates28contracts from2directpayments+2grants, while10050 research grants are not license purchases, so keyword spending totals would overstate vendor revenue.10075 Neptune dailyPageRank example includes S3input/output and separately billed S3 services, with assumed two-hour256GB job; it is a pricing illustration, not a benchmark or minimum RAM requirement. |
| R042 | U956 offset739 through U977 offset2738 (end exclusive); 21884 code units | 10086-10096 contain a versioned historical AmazonNeptune price catalog with running/stopped SKUs16..24576mNCU and region differences, stronger than a pricing-page inference but still not current quotes. Catalog presence alone does not establish availability in every region/account.10101 labels EC2 memoryGiB but normalizes as per-GB, a unit mismatch:4,000,000,000bytes must not be treated as4GiB; bare-instance costs also exclude managed service, disk and operations. 10080 NoSuchKey is only a failed guessed path. |
| R043 | U977 offset2738 through U985 offset862 (end exclusive); 23121 code units | 10123 full pricing capture lists replica, CPU-credit, snapshot-retention, cross-AZ and notebook charges; a standby-free batch example cannot be compared to HA serving.10128 cataloginstance mappings include128mNCU mappedr6i.16xlarge alongside512samefamily, so capacity labels are not straightforward exclusive hostRAM.10137 fractional regional differences/one anomalousLondon rate requireSKU-levelinspection, not averaging.10137 TB labels use1024units, which must be normalized separately from decimal4GB/50GB limits. |
| R044 | U985 offset862 through U1000 offset6623 (end exclusive); 22458 code units | 10165-10174 distinguish AuraDB capacity/storage, AuraGraphAnalytics excludingAuraDB, persistentAuraDS includingDB, and self-managedGDS sharingDBcompute. Lifecycle accounting must include client+DB+analytics coexistence when present. Free-tier RBAC/CDC/monitoring omissions are historical service limits, not algorithmRAM.10180regexempty findings are overturned by10183 actual matchingcontent with extraFrom--> markup; absence-by-regex would be wrong.10200 is another clipped procurement snapshot, not new purchases. |
| R045 | U1000 offset6623 through U1007 offset3703 (end exclusive); 23211 code units | 10204-10221 canceled order description explicitly truncates after memory-per and gives no surviving fullSKUcontract, so do not attach its configuration to the replacement by proximity.10208 wordqueryempty results coexist withSKUhit, illustrating tokenization rather than absent dimensions.10225/10228 retain full navigation and distinct tab-content flattening; the same4GBRAM row has8GBstorage, not our50GBdisk scenario, and HA/support are paid service scope beyond a kernel comparison. |
| R046 | U1007 offset3703 through U1020 offset4912 (end exclusive); 22616 code units | 10228 Professional table includes pipelined runtime but excludes parallelruntime/CDC, relevant when selecting like-for-like baselines.10235 supposedlicensingURLs yield PrivacyNotice or404 and onlynavigation, not license evidence.10238 TigerGraph promotional comments repeat availability/capacity claims and incorrectly imply broad Neo4j vector absence relative to other captures; retain as marketing leads, not verified compatibility. ApacheAGE prospective migration is conditional on Cypher coverage and reliable query cancellation, useful acceptance criteria for a replacement. |
| R047 | U1020 offset4912 through U1026 offset12299 (end exclusive); 23296 code units | Physical10249 official stored GDS docs distinguish Arrow direct buffers before conversion to onheap graph, heap projection/algorithm state, and writeback transaction state: overlapping lifecycle allocations count. The 90% heap advice is conditional pure analytics, not a 4GB whole-machine budget; CE concurrency4 is not 4GB and is independent of database edition.10242 correct2018 pricing URL versus variant404 shows failedURL is not evidence of absence.10238 O(1) traversal rhetoric remains unsupported. |
| R048 | U1026 offset12299 through U1038 offset5033 (end exclusive); 22724 code units | 10268 historical staff reply clarifies four logical threads total in one threadpool, not perCPU; startup credits/currentpage snippet cannot be applied to 2021 startup license.10258 canceled award transaction sequence sums to zero despite detailed512GBfourmachineSKU.10288 search awards mix multiyear periods and multi-product bundles, so no unit-price inference.10252 challenge and10262emptysearch retained. |
| R049 | U1038 offset5033 through U1049 offset844 (end exclusive); 22837 code units | 10300 correctly scoped API response establishes DEA awarding/funding subagency after earlier null-field parser outputs.10306 administrative mods carry zero obligations, so repeated descriptions are not extra purchases.10326 DISCOVERY BUNDLE search includes unrelated ProtoArray arrays: keyword hits need entity/type validation.10310 repeated28awards include zero/canceled and hardware+software bundles, not28Neo4jlicensepricepoints. |
| R050 | U1049 offset844 through U1082 offset5110 (end exclusive); 20765 code units | 10371 complete Capacities capture is Dgraph-to-managedPostgres, notNeo4j: CPU burden plus offline-mode reduced backend graphdependence; indexedlinks/objects, recursiveCTEs, DatabaseServicequeryinventory, dualwrites, stagingreadcomparison, two-weekthrottledbackfill, per-moduleflags/rollback. Legacy literal-toUUID mapping and malformedUTF8 sanitization require declared identity/data-loss policy. Claimed1/10DBcostand70%infra are historic customer reports without hardware/querydataset.10403 Vendr begins with average/median ambiguity and unverified CE noncommercial claim; cannot serve legaltruth. |
| R051 | U1082 offset5110 through U1082 offset28977 (end exclusive); 23867 code units | 10403 Vendr fullcapturelines90-364 contains internally conflicting Aura storage descriptions: separately charged at141 versus included at341/350. Many discount/support/services percentages lack samplecount or workloadcontrols. Treat as historic negotiation-marketing claims, not measured resource/performance or comparable replacement costs; surfaced lifecycle categories infrastructure,egress,migration,nonproduction,support remain usefulchecklist. |
| R052 | U1082 offset28977 through U1085 offset6655 (end exclusive); 23587 code units | 10403 Vendr capturecompleted includingFAQ/footer: monthly/annual modeled competitor costs and savings percentages are unattributed dataset aggregates with undisclosed comparability, not licensequotes. Hybrid/DR/dev+staging/support/migration costs further limit purekernelreplacementvalue.10434 new presscapture initially navigation only; body stillunread and notcredited bytitle. |
| R053 | U1085 offset6655 through U1088 offset13499 (end exclusive); 23590 code units | 10434 Sep2024release20%lowerBusinessCritical versustraditionalEnterprise(nowVDC) is a tierchange, not sameSKUcut;15Xreadcapacityvendorassertion noexperimentalsetup. CustomerQualicorpvaluesmanagedpatching/upgradeburden.10465 May2025AGA release explicitly uses projectedPandasdataframes despiteZeroETLmarketing; unlimitedsessions independently consumeinmemoryresources. Customeraccuracy/insight/latencyclaims differmetrics and lack local4GBtests. |
| R054 | U1088 offset13499 through U1091 offset8801 (end exclusive); 23580 code units | 10465/10488/10491 nonidenticalAGA versions fullyconsumed spans retain customerfootnote:50-80%greaterDSMLaccuracy versusnon-graphmodels is not80%absoluteaccuracy nor2Xalgorithmthroughput. ResidentHome independentanalyticsmemoryscale establishes existing separation architecture; AudienceAcuity20sources2.2Brecordsstory lacks build/export/RAMdata. No crossvariantsemanticdedup credit. |
| R055 | U1091 offset8801 through U1095 offset6665 (end exclusive); 23489 code units | 10528 captures priorart leads GFQL in-process pandas/cudf graphquery+analytics and2018/2020CSV/Parquetnotebook workflow, undercutting novelty of avoidingprimarygraphDB. Unsupported sub-billionfitsone-node not4GB.10529 real2016DBPedia larger-than-RAMimportpain is historic version-specific demand; AnchorEngineunder3GBtitle notvalidated50GBworkload; customdurableappindexes/Rama materializedview priorartlead. Neptunekeyword retrieves unrelatedastronomy/military/MLproducts, retainednotmisclassified. |
| R056 | U1095 offset6665 through U1138 offset1278 (end exclusive); 19833 code units | 10529 tail includes versionedpaginationtables and FastGraphRAG/PageRank priorart leads, but competingvendorbenchmarkopinions unverified.10536/10543/10552/10568/10570 show misdirectedtop10/navsearch failures, notabsenceofgraphcostpain.10569/10573 Neptune mNCU1GB versusNCUapprox2GB, paused10%, and256GB2hrassumption; costexample notmeasuredjob. CSSURLs/erroroutput retainedfully. |
| R057 | U1138 offset1278 through U1146 offset17673 (end exclusive); 23104 code units | 10580/10584 historical HN quotation2022price slightlyup/core solidified alreadyplannedoperational/performanceexit, notcostsolecause. Legalcommentcluster contains mutuallycontradictory courtinterpretations, preserved asopinions notfreshlegalguidance. Complaint trailingnewline licensefile startupfailure suggests import/configformatrobustness test, notconfirmed currentbug. Persistedoutputpointer onlycredits capturedpreview, not externalfilecontents. |
| R058 | U1146 offset17673 through U1169 offset64 (end exclusive); 21674 code units | 10590 fullHNselectedposts include25coreenterprisecontract anecdote and ecosystem/toolingcritique; queryplan-shape similarity does not provephysicalequivalence.10603 SlaterJuly2026author describesfixedmemoryLRU+ISAM+DiskANN/Vamana/PQ, strong priorartlead against genericdiskpagedlowRAMclaim. MemgraphAtomicGraphRAGsinglequeryretrieval-expansion-ranking-context priorart forreturn-onlyfinalpayload. TuringDBstable-scientificgraphs readlatencyfocus, notwrite-heavycase. Nofreshverification orbenchmarkcredit. |
| R059 | U1169 offset64 through U1192 offset3371 (end exclusive); 21672 code units | 10614 Slaterauthor givesread-heavywrite-lightdisk/objectstore+optionalL2cache; scientificcitationuserreportsNeo4jresourcelimits.14B/edge20GBimageversus133GBrawCypherroughclaim lacksdegree/property/build/RAMdefinitions; others30B/edge50GBcomparison notsameimage. Authorpackingneighbors+superhubheuristic truncated, cannotinferfullalgorithm. Java-languageblame explicitlychallengedbymore-than-RAMargument.10646/10649LI-taggedAGAvariant retainednotsemanticdedup. |
| R060 | U1192 offset3371 through U1224 offset0 (end exclusive); 20929 code units | 10708 AlgoliaglobalzeroNeo4j/Neptune hits is index-scopedonly;10714 fetched1000of1134 with875emptycontentfields, notfullsitecoverage.10730 WPsearchdifferentcounts,10733 graphhits mostlygenericvisualization/geography;10739 vendorupdates376 total versusglobalindex61postsandfewerupdates confirmscoveragebias.10665 FleetManagerexistingrightsizing/monitoringcostvisibility argues operationaltoolvalue, notnovelkernel. |
| R061 | U1224 offset0 through U1235 offset561 (end exclusive); 22824 code units | 10768/10772 repeatedbutnonidenticalpresscaptures consumed:datePublishedMay7UTC corroboratesbodydate, notfooter2026date.10776/10779 blogURLs redirectto samepressrelease, so they are notindependentcorroboration. CustomerreportedTCOadministrationclaims remain qualitativelifecycleevidence, notperformanceproof. |
| R062 | U1235 offset561 through U1264 offset251 (end exclusive); 21135 code units | 10795 officialproductexcerpt explicitlyBolt+Cyphersubgraphprojection andseparatecompute/storage, existinganalytics-sidecarpriorart.10827 fullerAWSpricingcapture countsreplicas/CPUcredits/egress/notebookReadystate/backups and50GBexample, but databasecapacityNCU isnotanalyticsmNCU.10832-10845 title-onlyredirect/fetch failures cannotestablishminimumcapacity; storedassistantrequests remainhistoricaltextnotcurrentinstructions. |
| R063 | U1264 offset251 through U1310 offset0 (end exclusive); 19583 code units | 10858/10861 SDKschema givesProvisionedMemorymin16max24576, strongerhistoricalcapacityevidence thanfailedguessedURLs.10868 concurrencydocs mNCU/4workerthreads;compute1query/8mNCU andIO1/4areguidelinesnotguarantees;algorithmscanoccupymultipleworkers. Mustaccountadmission/waittime notjustkerneltime.10877 starts explicit vertexenumerationnotmemorybounded limitation;detailsnextunread. |
| R064 | U1310 offset0 through U1343 offset3503 (end exclusive); 20714 code units | 10877 officiallimits explicitly MATCH(n)RETURNcount(n) canOOM becausevertexenumeration/countnotmemorybounded;per-labelrecommendation changesquerysemanticsunlessdesireduniverseislabel-specific. Tinyoutputdoesnotboundscratch. Algorithmsparameterunsupported,stringmax1048062bytes,embeddingonlylabellessconstraintarehistoriccompatibilitytests.10879GiBversuspricingGBconflict preserved;10891allgraphmustfitchosenmemoryandValidationExceptiononundersize.10888tiercatalog17capacitiesnotcontinuousrange. |
| R065 | U1343 offset3503 through U1369 offset359 (end exclusive); 21431 code units | 10933/10945 sameAuraProfessionalhourlyrates differmonthly720vs730hours, notrateincrease.10952Aug28archivealreadyBusinessCritical beforeSept4announcement, soannouncementdateisnotfirstavailabilityproof.10963 read-onlysecondaries scale readcapacitynotreduceperquerywork; dedicatedVDCvsBusinessCritical scope differs.10940 featurecomparison includesCDCabsenceandpipelinelimits importanttoextraction/refreshrequirements, buthistoricflattenedtablemustpreservecategorycontext. |
| R066 | U1369 offset359 through U1389 offset6881 (end exclusive); 21990 code units | 11043 TrustRadius captures valuableweightedownershippathquery: multiplyownershippercentalong boundedOWNSpaths, notdeduplicatedreachedset. Datesonrelationships implytemporalfiltersemantics. Separate600Mnodes1.5TBcomplaint backup/restart/counttimeouts andchunkedupdateslosingwholejobrollback points tolifecycleatomicity, not4GB50GBfit. PositiveUI/easyqueries/UMLSsubsetusebalanced withcomplexvisualizationandmemoryconfigpain; reviewdates/versionmissing. |
| R067 | U1389 offset6881 through U1462 offset0 (end exclusive); 16843 code units | 11043 completedreviews includepositiveprimarysupplychain/modelingease, procedure/RESTintegration, namespace/tenantisolation/backuprequests. ContradictoryonlyJavaAPIsversusmanylanguageAPIs andnoUIversususefulbuilt-inUI show version/reviewercontext dependence. Constant-timealljoins assertion false forenumeration/resultsize; preserveedge/pathsemantics.11046f=10 repeatsreviews notindependentwitnesses; pricingpage39ratingsandstartingpriceaggregationnotrepresentativecostsample. |
| R068 | U1462 offset0 through U1492 offset11334 (end exclusive); 20986 code units | 11051 MonsantoOracle-toNeo4j4Xlookupclaim is positivehistoriccounterevidence withoutbenchmarksetup.11054 beneficialownerreview explicitlynewinsights/time savedbutmultipleDBcomplexity, Patheercheaper/quickdevsupport; mustnotcherrypicknegativecost.11079 InfinigraphSep2025 propertysharding/distinctreplicated,Fabric,shardedmodes is existingpriorart; logicallywholeand100TBcluster not4GBsinglephysicalmachine norpointer-objectproof. |
| R069 | U1492 offset11334 through U1505 offset1563 (end exclusive); 22641 code units | 11084 completepropertyshardingblog giveshash-distributedproperties,localtopology,Raft/logpropagation,fixedinitialshardcount. Querycontainsnamepropertyfiltersbeforetraversal despitefinalbatchedlookupexplanation: cannotdeferallproperties. SamecaptureJan2026GAversusSep2025EAPresolvesavailabilitynuance.11096 embeddedlocalresearchquotes stressstaffno-spillbutpayingcustomersrentupto12TBRAMandfewrequests: limitsmarketthesis. Forumkeywordsparsity notindependentfull-demandproof. |
| R070 | U1505 offset1563 through U1511 offset3130 (end exclusive); 23304 code units | 11101-11112 guessedshardingdocs404/challenge retained;cannotinvalidateactual11084officialblogs.11134 entity-escapedpressvariantfullstoredspanread, notnewindependentcorroboration.11144 densepricingcaptureexplicitcolumnsMemoryCPUStorage;4GBprofessionalrow1CPU8GBdisk, not50GBdisk4GBphysicalhost. BusinessCriticalperGBheadlineversusmincluster needsmultiplication/context. |
| R071 | U1511 offset3130 through U1518 offset6287 (end exclusive); 23213 code units | 11144 officialpricingincludesstorage/IO/network/backups andpause80%savings, directlyconflictsVendrseparatestorage/egressgeneralizations.11149 BusinessCriticalCDCincluded vsProfessionalabsent;VDCprivateendpoints/customerkeys/hourly60daybackup versusBCdaily30day excludeslike-for-likepriceclaim.11155-11166failedvariantsandprecise20%tierquote fullyread; nofreshverification. |
| R072 | U1518 offset6287 through U1529 offset1401 (end exclusive); 22828 code units | 11216 historicalclientreleasefixesemptygraphwriteback,sessionOOMreporting,main-threadonlySIGINT/SIGTERMprojection/writebackcancel,Arrowbackedpandas>2avoidcopy,TTLremoteconcurrency. These support lifecycleexperiments emptyinput,cancelcleanup,OOMreporting,writebackboundedness; Arrowzerocopyconditionalnotblanket.11219 GDSsessionsalreadyalphaMay2024 underminesbroadnewnessofephemeraloffload;dates/versiontextmismatchretained. |
| R073 | U1529 offset1401 through U1540 offset1807 (end exclusive); 22836 code units | 11219earlierFeb2024GDSsessionalphaextendspriorartdate.11223officialclientdocs definesAttached/Self-managed/Standalone remoteprojection andwritebackonlyfirsttwo.11226 TTLdefault1hmax7days plushard7daylifetime, get_or_createoptionmismatcherror: designneedsdurablecheckpoints/cleanupdistinctfromsessionretention.4GBsessionexampleisremotecompute not4GBphysicalallprocesses.11233Gi-toGBrenamingisunitlabelnotconversionproof.11237closestarchive2025cannotbecredited2024. |
| R074 | U1540 offset1807 through U1551 offset1999 (end exclusive); 22843 code units | 11256 archived2024sessionworkflow explicitlyPageRankmutate->FastRP->streamwithdbproperties->writeback; run_cypher executesoriginalDB notsession. ProductneedsbothcomputeandDBphaseaccounting, propertyfetchesandembeddingsoutputcanmaterialize.11274memorychoices2..512GBexistingofficialsessionoffers, nothardphysicalbudget.11307 fullalternatepresslineformat retained;11311filelistingiscontentmetadata notproofthosefilesread. |
| R075 | U1551 offset1999 through U1555 offset697 (end exclusive); 23488 code units | 11311 fullstoredfilelisting readasmetadataonly; equalfilelengthnotexacthashdedup.11321 Aug2024EnterpriseversusNovVDCrenaming withBC146/GB distinction reinforcesversionedscope; samehistoricalpricingfamily multiplecapturesnotnewwitnesses.11324 beginsarchivedminimumclustercontext. |
| R076 | U1555 offset697 through U1568 offset660 (end exclusive); 22657 code units | 11376 archivedHTML explicitlyMonthly30days confirms720hourbasis;64GB4147.02typo differs5.76*720=4147.20, donotfitfalsepricechange.11367 all20structuredrows readwithcolumns.11362 SVGpathnumeralregexfalsematches illustratewhystructuredtablesneedparsing.11328 legacyEnterprisealreadydedicatedVPC soBCreplacementscopechanged.11381CDXstatus403recordsnotcontentavailability. |
| R077 | U1568 offset660 through U1573 offset7778 (end exclusive); 23402 code units | 11414 awardvariantLastDateToOrdernull doesnotnegateexplicitperformanceend;manypartialcaptureJSONsarestoredtextnotparsefailuresofouterJSONL.11418 exactdetailusesbase_exercised_options/base_and_all_options notearlierwrongkeys; parentNASA-GWAC distinctfromDEAawarding/funding. Procurementmetadata1offer/competitive-setaside notproofsole-sourceorvendorunitprice. |
| R078 | U1573 offset7778 through U1586 offset490 (end exclusive); 22640 code units | L11418 full award JSON distinguishes description 123750, obligation 127957.50, and outlay 128078.79; the unexplained gap is not established markup and executive compensation is unrelated to license price. L11425/L11438 zero-dollar administrative modifications and repeated cumulative obligated totals must not be double-counted. L11428 ALL DOJ/DEA heading is false because FAA, Air Force, and IRS records follow. L11435 NASA parent vehicle is not the end user. L11442 equal FAA amounts across different-length periods do not establish equal SKU or unit price. |
| R079 | U1586 offset490 through U1618 offset1815 (end exclusive); 20846 code units | L11445 query output again violates its own DEA/FY label: old award dates and FBI/USAID/EXIM non-Neo4j results are not DEA renewals. L11460 truncates the 512GB four-machine bundle and cannot fill missing SKU terms. L11482-11495 NASA public case describes 27k nodes/230k edges, EC2 Docker plus separate LLM/S3, PII-isolating enterprise databases, embeddings and similarity-edge construction; cost plus Cypher/Python transition mattered. RAG returns start/end/relationship triplets, not merely reached IDs; skills disambiguation and pipeline automation remain unfinished in capture. L11520 Gartner projection is vendor-quoted forecast, not observed adoption. |
| R080 | U1618 offset1815 through U1628 offset1982 (end exclusive); 22920 code units | L11523 official September 2024 announcement claims Business Critical 20%+ below traditional Enterprise, but tier/service differences prohibit a same-SKU historical price inference; 15x read capacity is an unreplicated vendor claim. L11530 explicitly includes storage/IO/network/backups and says paused DB saves80%, conflicting with generic hidden-egress narratives. Its 4GB RAM row includes8GB storage, not50GB, and pricing uses730h. L11538 closest archive differs from requested date. L11544/11545 old BC versus dedicated tier differ in billing/terms; August already lists BC before September announcement, so announcement date is not proved first availability. |
| R081 | U1628 offset1982 through U1657 offset3040 (end exclusive); 21083 code units | L11584 historic HN results include a distinctive underserved workload: many geo-distributed tiny graphs plus3-5 preproduction environments rather than one centralized large graph. Search noise includes resumes and complex-pricing-as-domain, not vendor cost. L11588 end-2022 per-core increase is anonymous recollection with no quote reproduced; operational/performance migration preceded quote. L11593 identifies YouTrackDB parent discussion, not a Neo4j source. L11594 current-capture pricing repeats contradictory Professional64GB table/128GB prose; do not infer supported limits from inconsistent scrape alone. |
| R082 | U1657 offset3040 through U1658 offset12810 (end exclusive); 23770 code units | L11594 full plan matrix distinguishes daily7-day versus30-day versus hourly60-day backup retention, missing CDC/RBAC in Professional, and dedicated-only private endpoints/customer keys; these are product contracts a replacement must explicitly exclude or meet. L11599 self-managed matrix lists low-memory analytics graph format as an Enterprise/Infinigraph feature, directly undermining a universal object-per-edge premise; it does not prove any measured bytes/edge. Community parallelization cell is empty in extraction and cannot be read as zero/unlimited. Pricing/legal matrix is a historical capture, not fresh licensing advice. |
| R083 | U1658 offset12810 through U1674 offset6502 (end exclusive); 22362 code units | L11609 archive keyword windows retain same0.40/GB/hour AGA rate across May2025-March2026 captures; this is not whole-page comparison and no price rise is established. L11613 repeats720h monthly and64GB arithmetic typo4147.02. L11615 licensing-lawsuit summary is unverified legal narrative, not a safe premise for design or license advice. L11625 launch release expands ETL incorrectly as extract/load/transfer and calls service ZeroETL while charging processing/storage; marketing terminology cannot remove extraction/projection lifecycle costs. Accuracy, efficacy, speed, and precision multipliers are different customer-reported outcomes, not controlled benchmarks. |
| R084 | U1674 offset6502 through U1688 offset4488 (end exclusive); 22525 code units | L11625 release itself says Pandas dataframes project subgraphs/run/return results, so ZeroETL does not eliminate client resident frames or conversion. L11630 legal archive paths restore evidence URLs after guessed404s; link inventory does not mean those documents read. L11636 keyword excerpts refer to acceptance-specific Effective Date, not webpage publication; Order Form precedence prevents generic terms from proving negotiated charges. L11640 Infinigraph property-data sharding and separate compute/storage billing concern a distributed self-managed edition, not AuraDB included-IO tariff or a4GB standalone engine; no benchmark established. |
| R085 | U1688 offset4488 through U1695 offset6376 (end exclusive); 23209 code units | L11640 release says available now as Enterprise while other captured technical article labels September2025 EAP: reconcile availability versus GA before chronology claims. Intuit/DunBradstreet quotes express expected needs, not deployed100TB benchmark. L11649 Qualicorp values managed upgrades/security patching, another replacement cost beyond kernels. L11660 November15,2018 primary vendor announcement fixes commercial-only Enterprise source transition at3.5 release candidates and explicitly leaves CE unchanged, correcting vague pre2022 licensing summary; redistribution/legal consequences require version-specific terms, not this historical reading alone. |
| R086 | U1695 offset6376 through U1702 offset1394 (end exclusive); 23207 code units | L11660 historical open-core article lists Cypher for Spark/Gremlin and iterative graph algorithms, further prior art against generic engine-portable Cypher analytics novelty. L11680 award list includes mixed RStudio/Jira/Nexus and hardware bundles, so totals cannot be per-Neo4j license prices. L11684-11694 canceled512GB four-machine award is zero with one-day period, while replacement L11691 has36-month warranty but no SKU details: inferring identical machine entitlement is unsupported. L11703 actual negative closeout transaction proves why award snapshots and obligation deltas require separate accounting. |
| R087 | U1702 offset1394 through U1719 offset2876 (end exclusive); 22262 code units | L11713 FPDS original entry carries original3-year completion dates but current totalObligatedAmount negative-zero; record-level historical dates cannot override final one-day canceled award. L11717 three search hits are modifications of one award, not three customers. L11727 pricing summary incorrectly narrows/add-on numbers: actual L11728/L11732 production-core premium6609 and standard5287 differ from summary ranges. L11732 Discovery2018 includes oneproduction+onetest+standard support, useful scope evidence but no proof2026 sameSKU. Blog author sells competing support and its legal assertions are not adopted. |
| R088 | U1719 offset2876 through U1726 offset16333 (end exclusive); 23209 code units | L11738 HN claims embedded Neo4j impossible, but this is a commenter assertion, not an inspected embedding API/license fact; YouTrackDB shaded-dependency packaging is a prior-art lead for single-library deployment. L11742 flattened adjacent Fully/SelfManaged headings mislabel AGA if treated as record fields; native AuraDB notincluded remains explicit. L11774 full515-line source representation separates2018 article from2026 navigation/footer and related articles, preventing current-menu claims from being backdated to2018. Chapter-marker tool output atL11746 is historical data, not an instruction. |
| R089 | U1726 offset16333 through U1734 offset290 (end exclusive); 23107 code units | L11806 full property-sharding article gives one-to-one entity mapping, hash distribution, topology-local traversal, Raft+transaction-log propagation, fixed initial shard count/no rebalance, and explicitly EAP. Its name-filtered sample requires property/index access before traversal despite a final-stage-only lookup narrative; outgoing relationships assigned with node wording must not imply whole graph partitioning. L11796 includes unrelated2011 InfiniGraph brand, so name matching alone cannot date Neo4j architecture. L11812 GUAC deprecation and connectors are only issue-title leads; local grep failed. L11815 staff12TB quotation is reproduced internal evidence, not fresh verification. |
| R090 | U1734 offset290 through U1751 offset6133 (end exclusive); 22254 code units | L11817 July2026 Pregel boxing-removal issue is only a title; message accumulation boxing does not imply pointer-based graph storage. Compatibility/build/API errors and FastRP fixed-seed inconsistency titles identify experiment/version-pinning risks, not proved unresolved behavior. L11820 internal evidence properly pairs12TB enterprise capacity with low out-of-core demand and quarantines100x disk-mode assertion: architecture gap is not market proof. L11826 January27,2026 GA article explicitly followsEAP and lacks resource bounds; claims infinite scale/hallucination-free outcomes are marketing, not defensible correctness contracts. |
| R091 | U1751 offset6133 through U1768 offset1778 (end exclusive); 22280 code units | L11866 complete release footnote defines50-80% greater DS/ML accuracy versus nongraph models, not80% absolute accuracy; same body uses different wording, so preserve exact attribution. ResidentHome claim independent analytics-memory scaling is a service workflow not single-host4GB evidence. L11870/11872 archived May19 capture is later than requestedMay10; list-count audit alone cannot verify benefit truth. L11914/L11920 keys base_and_all_options_value null versus actual base_and_all_options127957.5 expose extraction-key mistakes: missing guessed key is not missing data. L11918 nearidentical DEA descriptions still omit licensed capacity. |
| R092 | U1768 offset1778 through U1789 offset197 (end exclusive); 21894 code units | L11936 guessed generated award ID fails until canonical parent-contract identity is used, illustrating durable composite IDs rather than display IDs. L11952 namespace-insensitive modNumber extraction mixes award0/P00001 with parentIDV2: structured scope matters. L11956 arithmetic79.3478% descriptions versus85.4457% obligations is valid ratio computation but not a controlled unit-price increase;3.4% difference has no proved cause. L11983 apparent0.40/month is substring of3110.40, not tariff. L11989 persisted80KB external output pointer only exposes2KB preview; full externalfile is outside assignedcontent and receives no readcredit. |
| R093 | U1789 offset197 through U1827 offset578 (end exclusive); 20192 code units | L11992 FAQ explicitly distinguishes AGA independent sessions, AuraDS colocated storage+compute, self-managed GDS sharing DB resources, and Snowflake-local service, resolving misleading flattened SelfManaged heading. L12001 wrong global ceiling128 is corrected atL12004 by scoping toAGAcard512; regex sweep alone is not reliable source interpretation. L12013 empty LIVE-only price diff cannot establish nochange because regex misses commas/formats and captures duplicate old modal strings. L12016/L12019 AuraDS Professional disappearance versus residualJS titles is page-presentation evidence, not proved service retirement date. |
| R094 | U1827 offset578 through U1835 offset1044 (end exclusive); 23115 code units | L12048 archived AuraDS Professional card links a modal headed AuraDB Professional Pricing, so adjacent modal rates cannot be assumed AuraDS tariffs. L12051 explicit GDS Community maximum4CPUcores is not4GB, and its first200lines diff is incomplete source comparison. L12054 archive response headers substantiate captureMay12 only; cookies/cache headers do not prove publication. L12061 repeated scopedAGA0.40/512GB snippets support stable captured headline, no actual negotiated price or complete availability chronology. L12070 twoApril captures lackcard while guessedrelease404 contains currentnav: navigation match is falsebody evidence. |
| R095 | U1835 offset1044 through U1840 offset5023 (end exclusive); 23397 code units | L12085 guessedpricing/billing/costURLs are notfound pages with nav, not published tariff evidence. L12109 full28record JSON contains NASIC same-day periods and mixed hardware/software; annualizing every award by recordedstart/end would be unsound. L12113 float sum13674479.060000004 is arithmetic over those heterogeneous awards, not total Neo4j revenue or matchedannualspend. L12114 replacement IRS award is definitive sole-source contract withoutparentIDV while old canceledaward was deliveryorder: procurementstructure changed, further weakening automaticSKU equivalence. |
| R096 | U1840 offset5023 through U1844 offset0 (end exclusive); 2751 code units | Final L12114 tail retains3-year replacement period but truncatedrecipient record; no missingSKU invented. L12117 canceledaward totalandbaseoptions0 corroborate zero finalcommitment. L12119 IRS chronology mixes implementation, visualization support, multiproductandmultiyear renewals, not a normalizedlicense-price series. L12122 DEA summary repeats signed dates, not service start dates. All remaining unique content throughU1843 consumed; B930 exactreuse must be checked in final inventory audit. |

## Findings

R001: source8503 contains a reviewer-reported one-hour migration window after marketplace suspension. This suggests checkpointable export and recovery as a product hypothesis, not verified provider behavior. Source8506 is already a clipped review excerpt with mixed pricing perspectives, so it cannot establish universal price pain. Sources8528/8533 preserve navigation plus a vendor announcement claiming combined ACID/analytics at100TB+; this challenges a blanket claim that Neo4j cannot combine these workloads, but supplies no independent measurement or4GB fit evidence. U3 is a structured-output acknowledgement, not the out-of-scope structured payload; U4 contains tool references, both retained in coverage. No benchmarks or code implementation.

## Source Block Inventory

Machine inventory only, not read credit. Columns: B, physical line, T length, exact-value representative B, SHA-256, U IDs.

| B | Line | Length | Rep | SHA-256 | U IDs |
| --- | --- | --- | --- | --- | --- |
| 1 | 8503 | 6499 | 1 | c7777c91538ea27d17a287c30f0340e7028990d515a82f34e27cd8524b175d69 | 1 |
| 2 | 8506 | 1776 | 2 | 67198422216179231846d18fbf788fe1dd9c12dee3f457460bca5a60ece14510 | 2 |
| 3 | 8510 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 4 | 8526 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 5 | 8528 | 8000 | 5 | 386e4cae68842ebdeeabee510b3739535548885f02d3312608a1ea424cf789a3 | 5 |
| 6 | 8533 | 10000 | 6 | 4cafd5094f3a9ba93f4e42c0cc4699a86fdbfc009d6ac44fecd48a8b7965aaf1 | 6 |
| 7 | 8534 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 8 | 8539 | 128 | 8 | 3282a459da87becef7995d5dd7386d5a022396a0aa65ee5f4a63cff156f8b312 | 8 |
| 9 | 8540 | 1519 | 9 | aafc2fcfa8792741bd38527f2fec74db72d9b5cab7e5c8066c8a648bbc348eb3 | 9,10 |
| 10 | 8544 | 1243 | 10 | 9897d7b4c408a20421cd50db90d053d3fa8cb70fab8d357173b57927f154c274 | 11,12,13,14 |
| 11 | 8545 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 12 | 8550 | 166 | 12 | 3eff252e945a126aac7c5658d45f49e2bd818995e13d5ea320a47cde37570e15 | 17 |
| 13 | 8551 | 318 | 13 | b89b8ea1f11928c1ee77a9b5dbabf29f882062078eb4736fca3bf29e20005cbb | 18 |
| 14 | 8553 | 3512 | 14 | 09cd61609dca3926864d01c3adeb5f840a62092dc58e044f04e7e5b451a54f41 | 19,20,21,22,23,22,21,24,21,22,21,25,22,21,21,21,21,21,21,21,21,21,26,24,21,21,21,21,21,21,24,22,27,22,21,28,29,30,31,32,33,21,21,21,26,22,21,22,34,22,21,24,21,21,21,21,21,32,35,21,36,37,21,21,21,21,38,32,39,21,40,41,21,21,21,21,38,35,21,42,43,21,21,21,21,44,31,21,45,46,21,21,21,21,38,39,35,21,47,37,21,21,21,21,38,39,35,21,48,46,21,21,21,22,22,21,24,21,21,21,24,49,21,21,21,24,50,51,52,53,21,21,54,55,56,57,58,59,24,21,60,61,62,63,24,21,64,65,66,67,68,21,21,22,21,21,22,21,21,22,21,21,22,21,21,22,21,21,21,21,22,69,70,71,21,72,21,73 |
| 15 | 8557 | 25 | 15 | cc0d389cbfc6bcc251960030d0f9dd8809e51ff4e4ca14eea1bc59d168e695c5 | 74 |
| 16 | 8559 | 13643 | 16 | 7a310189788ec1d09f7e9fcf3094cb1523ac2ac280eb87f3dd6d8937fdead73a | 75,76,77,78,79,80,81,82 |
| 17 | 8563 | 5756 | 17 | 177a6215451c8d0065b9c853c4a6452e7420d6a3fc550b6526c911777f0c7978 | 83 |
| 18 | 8568 | 254 | 18 | d651488bca6ba1c80fc2b43c2ddb221be78280fdde49f88525fdb2b34a983334 | 84 |
| 19 | 8569 | 1390 | 19 | 707407a6204067244561a5f8dc9870a9ae2cd01556c2b64996b5800a88d550d8 | 85 |
| 20 | 8573 | 1218 | 20 | 353da514a35e795de5a441d7b28df3678234d4b621b69374ed0af76f275faac5 | 86,87,88,89,90,91,92,90,93,94,95,96,26,97,98,99 |
| 21 | 8575 | 4437 | 21 | 6ea2f7090c65e80956dddb02d3625cb5054aebc0a9822c6f9c5a88cd53eeee01 | 100 |
| 22 | 8579 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 23 | 8595 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 24 | 8597 | 200 | 24 | 1967e643c2f4f5a7288d4b46e0cbc0bde284d7c044840c6571cbf538f2abda5d | 102 |
| 25 | 8600 | 20584 | 25 | 429bf696d95003175b596fed8b8052e96c5e37f038190b51b424ae79ac8b8a1c | 103,104,105,106 |
| 26 | 8604 | 23999 | 26 | 0f43a9e959bc396ef2fbd1579e97b5d288ee5a495f593a79d29849485769a1e9 | 107 |
| 27 | 8608 | 22362 | 27 | e700a80bbb274f3c53360a990b85ced5191e54e1ba81ebac9d5d03eafdda2400 | 108 |
| 28 | 8611 | 3968 | 28 | 3e3ee0c52b9f6939d59c3b529e2d01f7cfd7c68e693c26c4720d57328577b357 | 109 |
| 29 | 8615 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 30 | 8631 | 20 | 30 | d5fdfe67262995a26eb7b192b4169cfd8da1523ac8c16de5e0a9e2071f9d7584 | 110 |
| 31 | 8633 | 20 | 31 | fb6135d882be896c2147bd95e4cecaef88b1cf68fb2bdb6240f01b8f7a1fc3fc | 111 |
| 32 | 8636 | 4590 | 32 | 2f0600ec0d8fb448ce3cfebaff09d35737af88bba4ffdf8faf7d3de05d97cd30 | 112 |
| 33 | 8638 | 347 | 33 | 2c4ff941d257e4bdbb2f35012d6f66436333b6e7f6cab879a29a15b0acd0c3f7 | 113 |
| 34 | 8641 | 13800 | 34 | 548103082809a80cdbdfa133d33689bcb06e44e5dc1ad6d0e41082250d0bae06 | 114 |
| 35 | 8646 | 2416 | 35 | 350ef5cb1b85061f5227fbb49d40450ec3451222e8097eaf1ad6740361cbb751 | 115 |
| 36 | 8647 | 9973 | 36 | d8cb18745b3f1e4705a29589a65f59f051df6796cd31c445d5b1be964f6bcfb4 | 116 |
| 37 | 8650 | 3151 | 37 | bab56f3bfe12550045b7c7cb28f5dc0dd72985d2d9986e2f63e8d5396d665fa9 | 117 |
| 38 | 8654 | 2680 | 38 | 1b5be543d2005dddf2ade811a6a37907e18247b07ab5812bf4b37ec7ee3e83c4 | 118 |
| 39 | 8657 | 191 | 39 | 6783dcc4576efb8c8f2ca9e4b0d3de13558459a6ab9c9c139136afabe97b83c5 | 119 |
| 40 | 8660 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 41 | 8678 | 30 | 41 | 8fff7bb1a2b0c7b5bb28d1dd9faea87e0c8862b377302de311812c44d69c8f38 | 120 |
| 42 | 8679 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 43 | 8683 | 855 | 43 | 3c29642fe14499e10a08cc5eec3d8bb7267d59063f5eb5b3fbddba490977d536 | 122 |
| 44 | 8684 | 8998 | 44 | 5995b03ce9b28bb49021b383eb6039765ae5b8cd3f71af787f05fce3fb85455f | 123 |
| 45 | 8688 | 2903 | 45 | 2a27dfc6c7c4d9cc7edec456f1c07aef44f5edb458031d246d623b5c6b61e68a | 124 |
| 46 | 8690 | 5999 | 46 | e4850be4bfc495325b2c1286f1772dc191590180039f3ab1f2766f216126fa60 | 125 |
| 47 | 8694 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 48 | 8696 | 10 | 48 | fba9f28faf85f1bdf9870fbb33b7858471b18ee72069088efaad3d7c4254efb8 | 126 |
| 49 | 8700 | 1296 | 49 | 16d2cd67fd374122dcd92d617e062f8eeebd41a51f9239dc1858f373c2852e9a | 127 |
| 50 | 8701 | 802 | 50 | 9750e85c4b1b3dbe53e0db1613ae694d88a43a1960a03205170f248481f16f53 | 128 |
| 51 | 8705 | 6597 | 51 | 46b531d52eb8da897c0e431ebbdf97f82f4af043deef655c79d7a218337c8d14 | 129,130,131,132,133,134,135,136,137,138,139,90,140,141,142,143,144,90,145,146,147,90,148,149,90,150,151,152,153,154,155,90,156,157,99 |
| 52 | 8707 | 171 | 52 | 6538c5cccb6f479d0ca3b22a92e0f8dfc2eedc6c970f97cec47bbb78dca65ffd | 158 |
| 53 | 8711 | 3343 | 53 | 9a04f347ff294e3a014b1b31e0388586740796a8e28ba3a6bb55c773de59e2bd | 159 |
| 54 | 8713 | 274 | 54 | 10d8699370feea791ebdfc4ef9427e48257a522f0290a84775d889203d010513 | 160,161 |
| 55 | 8717 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 56 | 8719 | 9004 | 56 | 1bf4ef5924e50a7b1e6e17c2a06e7c2937102d724daeabd74fb5b3e6b3978790 | 162 |
| 57 | 8723 | 3858 | 57 | 1cb6064e557130780dff980e732d398f0b2c820d40368adc33103188abb607db | 163 |
| 58 | 8724 | 849 | 58 | 3debffbe9234caf818f1c3b12826fbe61eafffb39ac73ad4b882adca472d510e | 164 |
| 59 | 8728 | 1000 | 59 | 417ac5fdf83a8ff400591a43b66b0a0a3ecdbf001b20d397041f63841a3e5406 | 165 |
| 60 | 8730 | 484 | 60 | ea6ac30053086af1e55c781b2546ecd16198b47f97d8ebbcc1ce7e9bbdaf2b91 | 166 |
| 61 | 8733 | 5054 | 61 | d963f790a3da70c080c9824bea16f302fcf766c74b583b4e1efa485f10dc7ba8 | 167 |
| 62 | 8735 | 3611 | 62 | 385ca70f9c9e8ff9cf23b2be24f7ece65192ccd9a7dd70ec9a8865877a935cbf | 168 |
| 63 | 8739 | 1667 | 63 | e4dec2f26c8b6f73b8c15241ac0b2dc2510c8af2ca3f2cce57784d62acd1ea54 | 169 |
| 64 | 8742 | 4236 | 64 | 2bcb7d751563c530449628142f8350ddd17d6e81495a77dc13ef681a88fbd888 | 170 |
| 65 | 8746 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 66 | 8762 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 67 | 8764 | 395 | 67 | accf430d483453f8230ce224a90f4adf54fdc11c0afbde9f8f6b4d7e7d46a38c | 171,172 |
| 68 | 8768 | 64 | 68 | 50f767674e790cb1c8646cc46560fa343107834b0813f520177d3a0ae9708410 | 173 |
| 69 | 8771 | 2518 | 69 | 7b9248fb1432a062c6c7e9ceb43c74df170faaa3dfef73923f9e0087d4bc8ee8 | 174,175 |
| 70 | 8775 | 159 | 70 | efd60c380fb8ae6158ab304c37f64168950c0fbe365e8fea030e4636bad248c7 | 176 |
| 71 | 8777 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 72 | 8780 | 798 | 72 | ab5f37743c4f0e9330ebe6c2ed4bfb04948733b5dcb81e4f3c52825cbe4df2f8 | 177 |
| 73 | 8782 | 517 | 73 | 39a5756d612bd1d1392e20b41a5907e5b2b169687b55a3d36032997cc0aef610 | 178 |
| 74 | 8784 | 122 | 74 | a8c83f2b3c363df4828f1548693ab3581816e64c4fe72df2891167c7a21bc9eb | 179 |
| 75 | 8788 | 223 | 75 | 8a2ffc54571572e442afe39e339acea02b6fbcdda9f7c4a43a027f2a8fdfc23a | 180 |
| 76 | 8792 | 4328 | 76 | 0026b2ed73172356c66327ead0e9367e538045c2e1288d9b491901e60d8a68ee | 181 |
| 77 | 8795 | 2077 | 77 | f4d48f1f33cdb98f4e18cc1fce5181e7ae7ec52aa8ddc8da59787c0eecf1372a | 182 |
| 78 | 8798 | 192 | 78 | df24d27d4220a1e8bf75138e853fc2c0d2cec3c45fe1368cfed8662b5c0f0221 | 183 |
| 79 | 8802 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 80 | 8818 | 10 | 80 | 3d11011dc011defa96f3835824c162e5895c9e38cf599ac5975a4dd418b11dfa | 184 |
| 81 | 8820 | 237 | 81 | 0b1279eb8664cbf146a25dadcb10c93b5c99ee192663650b6fcd8572c1813087 | 185 |
| 82 | 8823 | 10 | 82 | 55ad11ecf3ed874eb3f0804d467ea46eb16f02ced7ac6fede091c7ff8eefcc47 | 186 |
| 83 | 8825 | 528 | 83 | 6bd8ac93594295787318775cdb12681ea421ea0a7803ddad18b7e32206464d44 | 187 |
| 84 | 8828 | 1472 | 84 | f2f9e1f9a96a7ade57d0f4b9e90474c083a34185aaabc6006ddf60bb8d1cee85 | 188 |
| 85 | 8831 | 902 | 85 | e4a8fecf84bb4e8d42dcd0936b3ac1f517392919b847d89810de658dfc56299a | 189 |
| 86 | 8834 | 933 | 86 | dbd284f3aa50d28880b17828e180c20d458128cdc239e6372c1192f89ae3b990 | 190 |
| 87 | 8836 | 1519 | 87 | 2fb18696487a56760201190bc0d50765728ad958b3ae11f19d7ee265891b5eed | 191 |
| 88 | 8839 | 228 | 88 | 2d85eb670b5544bcd6ecf3bbc95aea7b7d6ee22b9fb96676c479121ec94c9742 | 192 |
| 89 | 8841 | 109 | 89 | b32d2d7766bc4a1a7cd7f862c1616ab02fc720d4caee94495005474e230202c3 | 193 |
| 90 | 8843 | 225 | 90 | 351a62a0eb30cda2e6edae78f9b0576962418617bef5f7dce72ae640d4ff0b8e | 194 |
| 91 | 8860 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 92 | 8862 | 31 | 92 | 28af09e4765154da5f1fea4adfae9596d550e40137fc2d8832e7dfdfb26e42ed | 195 |
| 93 | 8864 | 12000 | 93 | 5c6ac8e198f35da845d47b4e6d3f8f6ff9fbf251ca9136cb219c4cb6c42a3ad6 | 196 |
| 94 | 8868 | 12195 | 94 | 216c987b38e8ff16e95db41310d6b590855b047288a0a32363c9b3d0b5f785ba | 197,198,199 |
| 95 | 8873 | 737 | 95 | fcdf2f2607b2d9149946de61b1c98591859be0e8369a530b85efb4756eba4576 | 200 |
| 96 | 8874 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 97 | 8878 | 10110 | 97 | 3910b4be861162f053831d6ab4e7fa8dc197b72dc72309dd4f7e41ea063cf201 | 201 |
| 98 | 8883 | 522 | 98 | cc7d4c448289d4aedd421cd88fca57fe0ea511a41be3815cf326a43183fcb0d1 | 202 |
| 99 | 8884 | 332 | 99 | 6d93f56e5be6624afec6ec2eac85c45d870a4f4add6bb7fa2de58b5d9d7a9b64 | 203 |
| 100 | 8887 | 6041 | 100 | c9da8a3e0ff52f89779e39233aba705cf57d3cce2ebe8b15d074fa47030dea82 | 204 |
| 101 | 8891 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 102 | 8907 | 14651 | 102 | 9bdb179806b5b106cf15a28d63aed5cede7e4da7cc9d308e1b3b05bb67fed20a | 205 |
| 103 | 8911 | 4966 | 103 | d01a37a653109937eb4d0b76dca9e4341fd4afac0c83791a1f3b3a2edb53b52a | 206 |
| 104 | 8915 | 177 | 104 | 2ce19c02d0e3e78dbcb46822c146f98537a10727fdf1c0366a0ee6268f7b6bbf | 207 |
| 105 | 8918 | 1231 | 105 | c2e2e591f11fc837d36625adfdc862309e5ee6f8857eadcb6696bd006e7c3592 | 208 |
| 106 | 8921 | 105 | 106 | a198d99cc23ad42e39690e12aa46708be7f970546041770118166196d90a997e | 209 |
| 107 | 8923 | 1285 | 107 | 04078f48ed0e2c66b5182d1c47286f74df108633491be512c8618827ff992f5c | 210,211 |
| 108 | 8927 | 1110 | 108 | a7614a5de10e660899c3ef6b5e00040618d62312630d84d4c94b829af49d8b23 | 212 |
| 109 | 8931 | 5219 | 109 | 424dd16bc472c970dc1e75530d818791d795979f0d05f2b890e47ede9c5f499c | 213 |
| 110 | 8934 | 1014 | 110 | 76aba28a049a49f3dd860d2450a7343c973eeb3d2efe6f7588cc3f85c8aac572 | 214 |
| 111 | 8938 | 4868 | 111 | 82e88289e93c7b245ece1bce20e2b8162db35c38dcebd65dd22f637cd99bdd3e | 215 |
| 112 | 8942 | 146 | 112 | 4a66c464f165dabfa8b1b672790e4177d58e73cdd706210e8346c11eae5a4603 | 216 |
| 113 | 8945 | 1391 | 113 | b9c65545bf43c2eaaf3fdede0336424f80a56d51fd151d5a819cc67c182ee530 | 217 |
| 114 | 8949 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 115 | 8966 | 30 | 115 | 303865a6c0641274a205fbf127f53f481721c38fc0ef13afbd5c2588940f9806 | 218 |
| 116 | 8967 | 30 | 116 | c5a7be929a571022859d9513eb7130f4e4f0df3a7054bb22cd8947a69a30ef2f | 219 |
| 117 | 8970 | 4357 | 117 | d5979c3419e40eed76de62dd0f6e9d0649d631050a1b40619a30c8e3cd344cd0 | 220 |
| 118 | 8974 | 933 | 118 | 300efccd1d52a86d4b4343abcad6a4bae844397d048acf72d4495a98ad716521 | 221,222,223 |
| 119 | 8975 | 2799 | 119 | ef1a3b50d67fc4c9b26a7e2455b334e9cfa85cd79010ee2607cbb6a23737b67d | 224 |
| 120 | 8980 | 1391 | 120 | 40d422ce254d3f5ebb05791f7f16f995a4350bd4af5b2869324343096f1414f6 | 225 |
| 121 | 8981 | 6015 | 121 | c52cfda1dc1e3ebea1bc505ec075252903911fd856cc689087c180dec8489535 | 226 |
| 122 | 8986 | 13127 | 122 | 7eca036d517a6cc24c08221f310e96ba97479cda671eef35a11e7395cbd1968b | 227 |
| 123 | 8987 | 2285 | 123 | 23a4d8dae19e335cadd03866c463d2dbc2975142543274bb396b951bae4e9ca8 | 228 |
| 124 | 8990 | 113 | 124 | 984bf75865a7058edea2e55844bc3e382379f877068d6a28ee32a47b2cbe2940 | 229 |
| 125 | 8993 | 2350 | 125 | 589368e7dd099408c87f157577739b4c0c4d4c4ba2fb7c134bc4a0afdeefbb95 | 230 |
| 126 | 8997 | 393 | 126 | 204092b2ac16ca0105e2d1dac5a337c2e36e6b3ec92c0cbfcee4e15d4d16fc0c | 231,232 |
| 127 | 9001 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 128 | 9017 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 129 | 9021 | 750 | 129 | c5293cd7996193e17894489e4d60589828e084e26b071a7518bfc422436b3f84 | 233,234,235,236 |
| 130 | 9022 | 925 | 130 | f349fb6d75de54a17ae78a0453d43f654fc5f787a81fe1d37bd7d5f36ccf3f48 | 237,238,239,240 |
| 131 | 9027 | 6000 | 131 | 250b86f4d7c892a6198a928a6bc19f715800de30aff8acd0b76cae7d2b4cedf4 | 241 |
| 132 | 9028 | 6000 | 132 | dbdc00b21ba0c8d5be99277b7edf1268c511cd87cf9ede0409fea74262ea6382 | 242 |
| 133 | 9032 | 1574 | 133 | a67894c045adcd14e37b6593b3b179dd94d727a3f7588527a19d8b0e28415880 | 243 |
| 134 | 9034 | 4348 | 134 | 0446e3efaf5d9705ad968e70e9f88d32874ec3eb9653540b1001c8a627c59a0e | 244 |
| 135 | 9038 | 607 | 135 | 86fc081da839682d2d918a63e9abecfe4ac036717a4411768e46303686b04b56 | 245 |
| 136 | 9041 | 129 | 136 | 6afce1c032aeca928a8e2152afdd70e1b0fa0e9db05c8346cb1c28a87f137242 | 246 |
| 137 | 9043 | 1028 | 137 | 2cf5975eaab96aa0687fa33a91e7469136b9e642b86ca81d2cbf10188e6cd018 | 247 |
| 138 | 9047 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 139 | 9064 | 20 | 139 | 3bfa84cb47fbbad316cb57aae3ab6c18d76af1baa49ad653100cd9daa99af380 | 248 |
| 140 | 9066 | 3000 | 140 | d5886808e3d6547614c328e66c61e1cebcf1773f6cdeca32d362699de109b253 | 249 |
| 141 | 9069 | 539 | 141 | 719b9a818f484e34f8cc3ee1faa4a751f4c677108954373777523a0db82278e3 | 250 |
| 142 | 9072 | 5794 | 142 | d5c224715f28b6cd7b9414a12dab8e3d888e8b0964279728018ece8a2f005832 | 251 |
| 143 | 9076 | 1141 | 143 | f22c1b4bf4f7c0cc09cd48344a669fedf7aa81ce57b59314cba5f51ebffcfbb0 | 252 |
| 144 | 9078 | 38 | 144 | 261980e0d7d0d21698166fe437a65850c943ad042e6863c8b08fcb1085b86754 | 253 |
| 145 | 9081 | 160 | 145 | 20507d7206e6feb8f87f1b076fa02810bd6d218a5ac1882ca2866350bf2c4e7a | 254 |
| 146 | 9084 | 4385 | 146 | a598ceb0e1c12767d5bd2bc402fa367a92fb4037805aae33fbda5ba594a2b237 | 255 |
| 147 | 9087 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 148 | 9089 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 149 | 9092 | 686 | 149 | d7de6265f55dd9cd78e02a09abd06a4c430700ee80099239fba534f72a4ec541 | 257 |
| 150 | 9095 | 1952 | 150 | 778626202da065b96e2740300444e0476836f26052620e2bb9843e6d4c2ea68e | 258,259 |
| 151 | 9099 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 152 | 9115 | 8000 | 152 | 078f5f9e01d8007dad6b14b984a9c6d74ca3534302759a2a3bf0d48495a9da18 | 260 |
| 153 | 9119 | 1107 | 153 | 9fa7ff5f9c714a73979071a60f42907a2bf083d667c5019e61eb317b0b5a1035 | 261 |
| 154 | 9123 | 5321 | 154 | b11bcd75b52108f7b55da4dc9f9d66c0d8dccff9dfe08570bec3ad564ae1ae61 | 262 |
| 155 | 9126 | 635 | 155 | d0521ec89290325014a9f5b5c1b3b059f83f21db1a7d953e6710f12d6b47c2f5 | 263 |
| 156 | 9130 | 338 | 156 | 8b96e664c72c981e900b81ac4558114af49c267c7f3b39c0f365d0fa9a1e818e | 264,265 |
| 157 | 9133 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 158 | 9137 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 159 | 9138 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 160 | 9143 | 713 | 160 | d69dce43541733fe3f19abebc5f75efc45017a5ee257f77ab18d3d1f8628a96a | 266,267,268,269,270,271 |
| 161 | 9144 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 162 | 9147 | 3056 | 162 | a76294bca04d2394a5f4985677d525871d769802c2fdc5f5a15b0a287b420f09 | 272 |
| 163 | 9151 | 640 | 163 | efc6afe98f56ffa52633dd1df220ff71b2672e81d89eef251992be41138c7a6f | 273 |
| 164 | 9155 | 128 | 164 | cacda00aceec74736b6ab90b5cb9d0a180a8692ec2fc624f94d32cc5742eb070 | 274 |
| 165 | 9158 | 2387 | 165 | 0fcfb526be5b2630f24cfa18a820254884fff65372eac29d8136bbb61bbc3d51 | 275,275,275,276,277 |
| 166 | 9162 | 696 | 166 | 5f872d3dfd24b22ea895264d3029f56cd0f636ae43299611b400956d11c6cc21 | 278,279,280,281,282 |
| 167 | 9165 | 1243 | 167 | db3f76a670aa3d1b80b2830f8af53ce283d8298e8ede430cdaa1b76f502611cd | 283,284 |
| 168 | 9169 | 3849 | 168 | a35ddf724570786d17c0103b46b7c020694275a228bf8211910e4bd97f434d93 | 285,286 |
| 169 | 9173 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 170 | 9175 | 4118 | 170 | f092772ee7ce1a8ca9a49d18c95e6993287fe2264353a03a146a2e08e9036091 | 287 |
| 171 | 9180 | 1163 | 171 | 0a2a7b828b6f21d4bbd7e11d2aaf66ab37aa06b0fac0c82d2044ef9afdff23f1 | 288,289,290,291,292,293,294 |
| 172 | 9181 | 1727 | 172 | 8427ab5d770a00999f0f032bc06303d1cb73fe83f8cb63948b7578ff751a8644 | 295,296,297,298,299,300 |
| 173 | 9185 | 6000 | 173 | deb89789ee224070a0dba7eaff9ace985aad2d360e82e0f727300aa4b36832c0 | 301,302,303,304,305,21,21,306,307,308,24,303,303,305,309,310,24,311,312,24,313,314 |
| 174 | 9189 | 2754 | 174 | c9fdec5a3f206b4ab39fe6ba94dff727850a9b66c90c2e87a56234e71c453b0d | 315,316 |
| 175 | 9193 | 4125 | 175 | 42db156d5be81aed5082cc110fcb3beacd545cfc96e5102ea4616659d6d63f79 | 317,318 |
| 176 | 9197 | 2193 | 176 | 6c454132399e3d253c73ce8b277abbec24963206dc79ddb764ee2eb48c7e25c0 | 319 |
| 177 | 9200 | 1628 | 177 | e16f775ac993510bae2b9edd535a9dac386436067ee3d283ffe140b231336d79 | 320,321,322,323,324,325,90,326,327,90,328,329,330,331,332,90,333,334,99 |
| 178 | 9204 | 535 | 178 | 9a700d68b8350a7ce6ec92bd4697fd47b69d52c58f78f1142a349f0f8e6536e3 | 335 |
| 179 | 9208 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 180 | 9224 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 181 | 9227 | 1410 | 181 | 65c98f22a5af63fa44dab3634a38349fe578b85224983e32e89c58046419a046 | 336,337,338,339,340,341,342,343,344,345,346,347,348,349,350 |
| 182 | 9231 | 34 | 182 | 8435f9dd967a8e7f95a5203835eae76bcb8b8558e82db82112bb09b9f8c39d9c | 351 |
| 183 | 9233 | 1121 | 183 | f2dd7f304bd15b95e3d6afec82b05b7d7e7aa296c06d5b5b853b69855deebd1f | 352,353,354,355,356,357,358,359,360 |
| 184 | 9236 | 46 | 184 | d0a88f3040664cc696645a487ad3d578c886721cb2697c064c72d4e053f0994a | 361 |
| 185 | 9239 | 14675 | 185 | 99df4da8dea4675f1ab3a5b512faccff9d1ef3828ef833a991919bd9a3e6c334 | 362,363,364,365,366,367,368,369,370,371,372,373 |
| 186 | 9242 | 4200 | 186 | 1feaaa7aafc7a05104662899bdb3571ea073c49e1128a38e9ac8adf86d4841d1 | 374 |
| 187 | 9245 | 133 | 187 | 8f956883063981ac6310d1329f5a3d79f9f03049e028c0d7a6549ddfb1f4db64 | 375 |
| 188 | 9249 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 189 | 9265 | 31 | 189 | ebab85bde2dd69145a5518ff94219633e46de31392225fb089c808b730598e85 | 376 |
| 190 | 9268 | 1397 | 190 | 8ffcbe12b989045adc3f48956eee81f2c937086ee9dfd9ae5d66505fae4bc361 | 377 |
| 191 | 9271 | 12111 | 191 | f9274762f57da90b764231d373dcc2cc982f24540428dae1c68db1bc394d34d0 | 378 |
| 192 | 9275 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 193 | 9277 | 1319 | 193 | b208d9d6d5950446cf2a2e8f3075aa0b235a28421b49f398d5fdd471d8ddb492 | 379 |
| 194 | 9280 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 195 | 9282 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 196 | 9286 | 677 | 196 | 11afe830ac1fa0f821d648d3c747754ee4e59e24e8cdb891c498eaabd399d565 | 380 |
| 197 | 9289 | 1698 | 197 | 1947a51195c92e03a6eb956f280c11f9b435d07e1fb060d815081de6969c3504 | 381,382,383,384 |
| 198 | 9292 | 1926 | 198 | f3259574b160303aa676965e15be8fe7c3116e50f7893e693376ae997ace3703 | 385 |
| 199 | 9295 | 749 | 199 | 0d282f00180e999f876024dbe0a7e326932f78c1c05e739cac35328f0e0dd64c | 386 |
| 200 | 9298 | 85 | 200 | b98f957ce351915347ed88a78a63ed5b2c04fe64a8d12eb72fc21ab9bdc068c9 | 387 |
| 201 | 9302 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 202 | 9318 | 20 | 202 | 124f41bf0f8d0afecc04b50d2a69f1560b1cf5b11124c4033fe404876c3fcce9 | 388 |
| 203 | 9320 | 28 | 203 | 54701c06c9fb4515f18430de41b8d37bacd3df62d7a0cb5c70c78f26582f503b | 389 |
| 204 | 9323 | 6132 | 204 | aca861cb22e2e0e258ff33a1197d19d1d2005bc1ec4c07f9d16566c2a449b187 | 390 |
| 205 | 9341 | 6000 | 131 | 250b86f4d7c892a6198a928a6bc19f715800de30aff8acd0b76cae7d2b4cedf4 | 241 |
| 206 | 9342 | 6000 | 132 | dbdc00b21ba0c8d5be99277b7edf1268c511cd87cf9ede0409fea74262ea6382 | 242 |
| 207 | 9347 | 4699 | 207 | 7620de0ffa7abc10281d559596a63b023cccc25ba29ee443e458d8d73ac77339 | 391 |
| 208 | 9348 | 284 | 208 | cad4e40d1a6a7d29592726b2e7bfd064552de94d04fee3f1cf0ca18917ed0c42 | 392 |
| 209 | 9353 | 1606 | 209 | 0a0345b9ccd23ffd9ad1ea0907a6f937a9592f68b24f0b2212fdcad6a108997a | 393 |
| 210 | 9354 | 883 | 210 | e0711452133500e93763cff06b37a5245bd534ef236100b75d873924bff3a59d | 394 |
| 211 | 9358 | 11522 | 211 | 4a4d2c21c913f27b32d0a0b79eeba559bf4ea10354ccdd7761ddffdd79ba5823 | 395 |
| 212 | 9360 | 850 | 212 | 00d6ef916077c278b991767f3a0723c4fa2808825b1bbac6e365fd1340dee7b4 | 396 |
| 213 | 9364 | 447 | 213 | dc3dc8405baeb93a9a7ba7462449a04df94d1837b9f83a3b483b5370b42c38af | 397 |
| 214 | 9366 | 1251 | 214 | 3087890b41bdb8cea7ac58c6682f44f9e7caa3ee0bfa13332e112c76307b9cbf | 398 |
| 215 | 9370 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 216 | 9372 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 217 | 9374 | 601 | 217 | d1e27c125e320aad5ce5edc16a963957748787a1fff5c1ca6881f4f85b0b6056 | 399 |
| 218 | 9378 | 371 | 218 | 70492ced2ea1a186a5b6be66b5c2406440557c529f53b1c49ea58dcd26e20f67 | 400 |
| 219 | 9382 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 220 | 9398 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 221 | 9400 | 237 | 81 | 0b1279eb8664cbf146a25dadcb10c93b5c99ee192663650b6fcd8572c1813087 | 185 |
| 222 | 9404 | 159 | 222 | 7b1ee767bfcc7ba401839451f97fe8090dcc7143f6e4047c46660d2d6b68a260 | 401 |
| 223 | 9405 | 243 | 223 | 4cd65f6dfc2938c2ddd1c690be269c501f9cf605132cb7d13e903119289b70d9 | 402 |
| 224 | 9408 | 593 | 224 | 892f5ce05c9fd7584d3b1c022075aafb3e51ba2363b472599a35bd1e06a71c25 | 403 |
| 225 | 9412 | 1949 | 225 | 59a2deae14e95e6dee402cd1f37d31aa2d2dd4af7ee50a73b45296c1e624dfc4 | 404 |
| 226 | 9413 | 1438 | 226 | a7c1419480c42e2bbbc93da3575973b3bf3568dc8bd2c5db64a1ab86b033dcf9 | 405 |
| 227 | 9416 | 1620 | 227 | 9af7d9e7497073b7bd78577a4803cb368467d58fedba13fe6e4e45e723b84012 | 406 |
| 228 | 9419 | 1422 | 228 | 87ff62893c08be3e5397c5c65dad913403e77eb292eabcf28dfb60b47fd2954f | 407 |
| 229 | 9422 | 319 | 229 | 3fc72238d0eb5cbdc0035993603f8a3f1cf20f8ad5546ddc4505945f9362eab7 | 408 |
| 230 | 9426 | 1749 | 230 | 6a399c0405b05ceef0e4458a6613bb1c128f4f704c8a5da838091cd189c3e8ee | 409 |
| 231 | 9427 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 232 | 9430 | 113 | 232 | 04cf88341627c8fb787ae4849ba8db0b3e3b2dede9aa43765ed6f708a02e409f | 410 |
| 233 | 9434 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 234 | 9450 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 235 | 9452 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 236 | 9456 | 20 | 236 | ff1c8765ef5eacb73a9c1867d8ae697f88018415e6992dfb363d046754b62549 | 411 |
| 237 | 9459 | 29 | 237 | fea5769e5a286eb4caf34136efae68cf16ebc31f19330f81845d914f78efedfa | 412 |
| 238 | 9461 | 12970 | 238 | 4b44d235bd07125a393a2d5a8f51c4e8e1fb80713a565b186dd4f68942f7918b | 413 |
| 239 | 9465 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 240 | 9482 | 10 | 80 | 3d11011dc011defa96f3835824c162e5895c9e38cf599ac5975a4dd418b11dfa | 184 |
| 241 | 9483 | 20 | 241 | 4bf76d19f07bbec1eb78a767572b240f3a52ff61ce33f2e86ec2fdc29f106865 | 414 |
| 242 | 9487 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 243 | 9488 | 22 | 243 | f91a80f395115300655609ae9d02bb9d317d102115aceab48a774d96566b3703 | 415 |
| 244 | 9491 | 756 | 244 | 3ef77bf200dbc90ed83d2178a35c1995646423e70caa602abfb8407f523f377b | 416 |
| 245 | 9494 | 2027 | 245 | 8daedb2d6b56e9e0eb68e6e9fc818bdd641d8904ac1291b0fb650f83016c86fc | 417 |
| 246 | 9497 | 2800 | 246 | 6fe6176a30190e5d27152e772445bf1c437e0556aacb20321718485c156c2536 | 418 |
| 247 | 9500 | 510 | 247 | 595a2e962cef87568281561f0d028fa0594c42697b6a24b6876b84a5bc9ae4b5 | 419 |
| 248 | 9503 | 2071 | 248 | 53b35dc37b7d5c9fada27cbaa08876a5ddfef66347fb5efbae6ab867363bd75f | 420 |
| 249 | 9521 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 250 | 9537 | 14859 | 250 | 0da9b8ce1809ed5373ff157e0878306ff5548cc9075758123cd64f293ba6b8b0 | 421 |
| 251 | 9541 | 1824 | 251 | b32810867ae6d95162976d2c5991778def501ae7f521b4b72a9a6f882a5e0b21 | 422,423,424 |
| 252 | 9543 | 2647 | 252 | d6efd998426128b28320e53901f19b5df1e7a3bc308fa40871a90bd5c1008d46 | 425 |
| 253 | 9548 | 1529 | 253 | abc8398cbf94dfef1dc211ff7335072f2b7f6ccaffcc5e158cc0709519530e70 | 426 |
| 254 | 9549 | 2060 | 254 | 197e78f00ab00df034fbc40d933125563ca62c4a4192aacee4819616fb74a7d3 | 427 |
| 255 | 9554 | 762 | 255 | 147ecb51309a62926c04b582c41efd33024217e519aefd903f9c364f0524663f | 428,429,430 |
| 256 | 9555 | 1778 | 256 | 4a8488e826543d87cef755fb217c2cc0ec72e11c8cc9425da0a1f6c3659b9676 | 431,432 |
| 257 | 9560 | 1346 | 257 | 7d2b8bd538350298516ca12c24eb45442b69a98c14483472a215d5851a2bfbcc | 433 |
| 258 | 9561 | 90 | 258 | fb9203d87b5e533c60d95341dac55c81f87620caecf676fbd3ef0fa28f922f2c | 434 |
| 259 | 9564 | 2588 | 259 | bbabf4f3ecd8d27915e3caef05ff93653cee14b7dd4751ad3d0f19ed21b06bd2 | 435 |
| 260 | 9568 | 1108 | 260 | b60975c77c147d983007fba23d0eb58b5f280486446da5f1dd549c655aadff66 | 436,437 |
| 261 | 9572 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 262 | 9588 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 263 | 9592 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 264 | 9593 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 265 | 9598 | 12596 | 265 | 3ce465cc104dd4fb304277eeb8326ea920a1092bc4e266980f65e1a2fec89a15 | 438 |
| 266 | 9599 | 1739 | 266 | d8e973063fff4329a9d81446525b9a02272ce15b13d23c0d5c5568045eaacb8f | 439 |
| 267 | 9603 | 8562 | 267 | a2b2ff8bc02f5bbd84198be0e96aeeab2eb6c22fe6ccea41b21be6cb04a199d5 | 440 |
| 268 | 9604 | 518 | 268 | 835fe80a32dd0e3fb14c36679ec0067609af8ab51cfbe234045c1c28a43dec5b | 441,24,442,443,443,444,443,443,443,443,443,445,22,446,447,448,449,450,451,452,453 |
| 269 | 9608 | 15 | 269 | 83ae3b0fe8bfe4e2ad1ca93ca1e6d97dcaad1fc324cd37e4a353b88b7d91170b | 454 |
| 270 | 9609 | 153 | 270 | 1077d195bf87565b326b10fa14141ed19ee08139a705defa735fb95fe39fd9dd | 455 |
| 271 | 9612 | 2869 | 271 | 7c6c34a5731d58aac949b8de29dc2a2d6cfb569fd888a9a5dcfcf8630e35d511 | 456 |
| 272 | 9616 | 18001 | 272 | a814885273adcdb6105ccd04ffb4c2456c55ccbafe9935ca3b4eb88624a1c1cf | 457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490 |
| 273 | 9617 | 862 | 273 | f6054c3718bc2a90e565f5ed8e8e70d9be2018bdd404f67b9cdcc4dbfb6a4cea | 491,492,493,494 |
| 274 | 9621 | 1066 | 274 | c518188e55ee1d27f4d4dfc9d6262c2875ab96916865605406f98d956809cf38 | 495,496,497 |
| 275 | 9622 | 2123 | 275 | e8562df809ac477ef9c25881da7d9e3994dd11b8ec8846a9f58f01398e056a5a | 498,499,500 |
| 276 | 9626 | 782 | 276 | 81c07641d8f654974265cca76a2a21cfcea97d84600ce63ae0df197b3aa90cd2 | 501,502,503,504,505,506 |
| 277 | 9627 | 444 | 277 | cd29157e8d2b82f8f2b50cadb9afdb36129b7a2f1f6839c4468b1fb575541b9a | 507 |
| 278 | 9632 | 455 | 278 | e32111baf17aee0f6b865fb024bd124cd1fd0b2f53340330897bdac725adf4e6 | 508,509,510,511 |
| 279 | 9633 | 445 | 279 | 4b403e8bfd8e04658087e4d161eb3f75a7ae1570c6ae49ef341d7bf0244aee85 | 512,513,514,515,516 |
| 280 | 9637 | 614 | 280 | 4d1eb98c3c22a302779ed29fb1aef55f1d412a08c5dc7d8ce97e37a4b93afd99 | 517,518,519,520 |
| 281 | 9638 | 551 | 281 | 3d1963215a48793ec0eaeb162d9e303ff230fd89b0636f68c981b15fdbd13779 | 517,521,522,523 |
| 282 | 9642 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 283 | 9643 | 425 | 283 | d9c122a138998d4c3fdfb499e9ed34024af33808ba2f48d41723d0418c045e3a | 524,525,526,527,528,529 |
| 284 | 9647 | 1390 | 284 | c869776f4c398e491e00d908598b53b85704a792b6756576857db28adde36820 | 524,530,531,532,533,534 |
| 285 | 9648 | 3466 | 285 | d49c90310b9d8eee17e15e6de9af40ea0eab16e65eff9e378e3d81b482482fe5 | 524,535,536,537,538,539,540,541,542,543,544,545 |
| 286 | 9653 | 1188 | 286 | 02e3f986d4406a2607d9330c1cdc73b76740a1a615201edc79737414fa6ead40 | 546,547,548,549,550,551,552,553,554,555 |
| 287 | 9654 | 1269 | 287 | d1bf22d9883450be2790da3e820c3216499f4cf8e7ea4118f52176f4031d8600 | 556,557,558,559,560,561 |
| 288 | 9658 | 4550 | 288 | 73081cfe56dbf98021291be04aa2e334ec561ca63d3f1118d4a606f0372eb90a | 562,563,564,565,566,567,568,569,570,571,572,573,574,575,576,560,577,578 |
| 289 | 9659 | 6099 | 289 | 3e74e88cfbb48276842315849aea18547bcc25f76d89c5ac4bc596d6f90c0e2d | 524,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596 |
| 290 | 9663 | 1295 | 290 | 2fd57db5b73b37a9f3199213f630b01922315b8a0908fea4fbcec6d8152e85e8 | 597,598,599,600,601,602,603,604,605,606 |
| 291 | 9664 | 1440 | 291 | 06eb50ea49f7cb2364ca04f2f73e9322e1d5a88a348c6df38b00b055270c7e4b | 607,608,609,610,611,612,613,614,615,616,617,618,619 |
| 292 | 9668 | 223 | 292 | 96be7b0d7cd54c0206436817f8959d041efe88eb0a1d4a3ca455381729bdbfe7 | 620,16 |
| 293 | 9669 | 1081 | 293 | 7457653010e4d95b19ed00f5c866b5c896944cdbb556ec5f68757b5156c0655a | 524,621,622,623,624,560,625,626,627,628 |
| 294 | 9673 | 1991 | 294 | 6df7986fe4c53448940dec6228483b029c6be66c451260952f6192666016075e | 629 |
| 295 | 9674 | 50 | 295 | 35d8b2fddc1adb02f794caa54f508020cec7ec0b060d7535c03373ee766cb38d | 630 |
| 296 | 9678 | 1066 | 296 | 26345d2b04e5f51fb21516f027bbb830d536c65493020d4f3786360983d2175f | 631,632,633,634,635,636,637,638 |
| 297 | 9679 | 16 | 297 | b49a1e1f25b9bc5db8d60d5c07e2dbc15d8d7fd87d88a5feeb8a4db0d71220c3 | 639 |
| 298 | 9682 | 223 | 292 | 96be7b0d7cd54c0206436817f8959d041efe88eb0a1d4a3ca455381729bdbfe7 | 620,16 |
| 299 | 9686 | 63 | 299 | 618ea9cb6c8fb3b167f0a544785f21f9e0833710981bede68432c1c90c966c69 | 640 |
| 300 | 9687 | 498 | 300 | b5ec8d3a2541f8032a93c8fc2a35ae672fc20361c3cea51882ed8db6b211fd72 | 641,642,643 |
| 301 | 9690 | 1387 | 301 | a9b9250276ac7bac3aaf3cfee3a33f2df36dc1d2081065a98c43685ad05aa8ff | 644,645,646,647 |
| 302 | 9694 | 38378 | 302 | db862c14f7d0409c606a52d27d1d2173adb8cdf51a13e5cbe6a4786e0c954ffb | 648 |
| 303 | 9697 | 5960 | 303 | 3bd69092766c73fbb152595d7fb41812051f4693521556a27a37fbd09ef8a8b3 | 649,650,651,652,653,654,655,656,657,658,659,660,661 |
| 304 | 9701 | 223 | 292 | 96be7b0d7cd54c0206436817f8959d041efe88eb0a1d4a3ca455381729bdbfe7 | 620,16 |
| 305 | 9702 | 1370 | 305 | 85dd79bc098b12be50d19bee79e9e3c21b323c9749f093ac234d52a7f00fb9ee | 662,663,664,665,666,667,668 |
| 306 | 9705 | 477 | 306 | a470b1136eda2a76f8d658eb747d57b2b4d2b2c230b76e9c3ec454047c579299 | 669,670 |
| 307 | 9709 | 215 | 307 | 97d81b312051fcb9f40bdec77ff4ee91ad8aebde53d8bb7c31e3e9d9f35eeaf7 | 671,16 |
| 308 | 9710 | 124 | 308 | 0158bae2dba418f4745eaf8646bd8bb88ae76367a0033575a0e48da9f8529c33 | 672 |
| 309 | 9713 | 223 | 292 | 96be7b0d7cd54c0206436817f8959d041efe88eb0a1d4a3ca455381729bdbfe7 | 620,16 |
| 310 | 9716 | 721 | 310 | 4d5438f5f8a4fbd2087b338085aa1a318656ce81b3f208efe0264623d7fb6ca0 | 673,674,675,676 |
| 311 | 9720 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 312 | 9736 | 20 | 312 | c54251774ebdc7ee632027fa52957e55ad89e944372f35ac072916be4084b0b7 | 677 |
| 313 | 9739 | 25 | 313 | 0b57c745643e9d3c5970b8eb5a080b860fb9812595d9406290581baf4180440b | 678 |
| 314 | 9741 | 20404 | 314 | abfa9edf7a76437126b7c6e445486490c4eb5e4997d1312b56f74d64ec0fa626 | 679 |
| 315 | 9744 | 833 | 315 | a511365acdea00a52380ed14cb8cf2036a624fc7327c0074b9243bb829f23e5f | 680 |
| 316 | 9747 | 715 | 316 | 0ef19cfc89374a583a89966a9f26d852989fa1bc4341ee1e1e3d61e7a62b4a95 | 681,682,683 |
| 317 | 9750 | 12869 | 317 | bf9a8eac9d64c846cb323ad80e57d464cf118c6472cc36b9746e31df3c4c241f | 684 |
| 318 | 9753 | 10 | 318 | 026d65ec57a55a5731fed0242e5ed906adb05fbf425c967276f85614cfdcf6bb | 685 |
| 319 | 9756 | 1378 | 319 | 77ad109cb699286d818bae17e36790a79431455f951a09708aa7c404efb122ae | 686 |
| 320 | 9758 | 7931 | 320 | e424a854dcab5b0f72e059007ac3971ec45dc3e638684895b4dbaae2fb7e79d6 | 687 |
| 321 | 9761 | 7169 | 321 | 9b3946d782e416eda8334dd4c8324d1973d6ecd851d1fb68e282b1f429d5f107 | 688 |
| 322 | 9764 | 4274 | 322 | 27dfb98be77e5296c9047a7217b06720362385a8f2ae31791c632eccbc83b678 | 689 |
| 323 | 9767 | 42 | 323 | 48faa8b653cc6db04b97a2b3d88ea08ee454db9f1155195ddf6ac877edb087e8 | 690 |
| 324 | 9770 | 122 | 324 | 50e35bd8c2d2855b60d89a3c0308c3a037a8acc3fc375adf807f12abb1c3653c | 691 |
| 325 | 9772 | 1299 | 325 | 834af13e8c92db68aee988f2388116479eb88151d5a624aa996daab777e1ec6b | 692 |
| 326 | 9774 | 1228 | 326 | 05433f1fe2f7e78552fa4cecdd933c786e1a3dfd661b3e13bebcca01dd8e1c15 | 693 |
| 327 | 9778 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 328 | 9794 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 329 | 9796 | 1260 | 329 | 1f373023addae2b60f2671115f8025e8f73d2e565b2de25e57a57a558addf0c8 | 694,695,696,697,698,699,700,701,702 |
| 330 | 9799 | 1321 | 330 | fbe15befcc7d6c449ec287affc2725b9808cf1c0a0a3bbb2ca6381fc17a74af9 | 703,704,705,706,707,708,709 |
| 331 | 9803 | 27 | 331 | 49572710a0060726ae737fde204b37cb417e015a23636b112c84c6873eb8e2c1 | 710 |
| 332 | 9805 | 8909 | 332 | 2b3fdf6c9b26f753af62ce8b8f7c88cc92f25db17a85db9a0cfa25d9bd4f2487 | 711,712,713,713,714,713,712,715,715,713,713,713,713,713,713,713,716,717,717,717,717,714,713,713,713,713,713,713,713,718,719,713,713,713,720,713,713,721,722,723,724,722,722,725,726,724,727,728,729,721,724,730,716,731,732,733,734,735,736,735,737,735,738,735,739,735,740,741,733,742,735,743,735,744,735,745,735,746,735,747,741,733,748,735,749,735,750,735,751,735,752,735,753,735,754,735,755,735,756,741,733,757,735,758,735,759,735,760,735,761,735,762,735,763,735,764,741,733,765,735,766,735,767,735,768,735,769,735,770,735,771,735,772,741,773,774,733,775,735,776,735,777,735,778,735,779,735,780,735,781,735,782,735,783,735,784,735,785,741,733,786,735,787,735,788,735,789,735,790,735,791,735,792,735,793,735,794,735,795,735,796,735,797,741,733,798,735,799,735,800,735,801,741,773,802,726,803,716,731,727,728,723,723,724,726,723,804,805,774,806,733,807,733,808,733,809,733,810,733,811,733,812,733,813,733,814,733,815,733,816,774,724,721,729,817,729,712,818,726,819,819,819,819,819,819,819,722,724,820,821,822,726,723,823,722,824,825,826,723,723,726,729,827,828,726,819,819,819,829,717,717,726,830,831,832,831,833,724,834,724,835,724,836,724,837,724,838,839,831,840 |
| 333 | 9807 | 3847 | 333 | 6d6d7243826bd62094e95cb68631ab0ed515fff10fd8f0f75c02f4b3443eb4d7 | 841,829,842,843,844,845,846,847,829,848,849,849,850,851,714,713,852,831,853,726,819,854,724,729,722,806,854,724,855,713,724,856,856,856,856,855,729,857,713,713,713,858,859,858,855,721,860,861,722,726,724,855,862,858,855,721,863,864,865,722,726,724,855,862,858,855,721,866,867,722,806,726,729,729,713,819,722,868,869,870,722,726,724,855,713,724,856,856,856,856,855,729,857,713,713,713,858,871,858,855,721,872,873,722,726,724,855,862,858,855,721,874,875,876,722,726,857,877 |
| 334 | 9811 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 335 | 9827 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 336 | 9829 | 223 | 292 | 96be7b0d7cd54c0206436817f8959d041efe88eb0a1d4a3ca455381729bdbfe7 | 620,16 |
| 337 | 9833 | 543 | 337 | 1aae53e737ac702ce6a87a826e5fb08f72e71cf3b488a6e035603220ab70a59c | 878 |
| 338 | 9837 | 1373 | 338 | 8d623b10d610ef27d80cb296cc12e0b036834dc3136dca77b8d21e94558dff13 | 879,880 |
| 339 | 9841 | 4133 | 339 | ab56dffbc11a37c33ff0b4db147ae97bff56c4af7f0fc138cf5568450d571add | 881,882 |
| 340 | 9845 | 3998 | 340 | 106f20be4622ec2ad4166b45fddb16c943419722e107db7e1b0fa490eef8af8e | 883 |
| 341 | 9848 | 4416 | 341 | c2760867ccbd9856d3ddca7af4a7bd1f11ac8ff626a8dd41a559d0476a720bb8 | 884 |
| 342 | 9852 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 343 | 9868 | 1958 | 343 | f8300a226a0beb8b35473b83697bb84ed459f5ff45b075e85f73b6547ac48720 | 885 |
| 344 | 9871 | 4863 | 344 | 9b784b0704794bd783fd4aaf556cdd108c29fd232b516b798dec0788c4dfa146 | 886 |
| 345 | 9875 | 2085 | 345 | 34dbf23de1d955141e843ac27f587aacce136d808462426c49d3d5256597cf0c | 887 |
| 346 | 9878 | 4637 | 346 | f6cae16df03480c32a0f28847ccfca620454be57251662a3ceceec91a19c2498 | 888 |
| 347 | 9882 | 593 | 347 | 0937a8d87954082504d0d31ed4d1979566f4137b6d18595931783e7a0a76b862 | 889 |
| 348 | 9885 | 3313 | 348 | 12feaba9e4275adae69d6df9b2781f814b39c8a985168b4e8c4d419d525e1618 | 890 |
| 349 | 9888 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 350 | 9891 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 351 | 9892 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 352 | 9896 | 1954 | 352 | 65e6f7de1c4a8eef4c5aa203d2f66d1b67b5f0a2888505dc48c35940d4e5f241 | 891,892 |
| 353 | 9899 | 659 | 353 | c24a2b8d9e46e9199efa114413885bf17fad44627f494182f93543c1965599ec | 893,894 |
| 354 | 9902 | 218 | 354 | 36b5c24e73542516ac0f841ed957b0f18d60dc7da24fd6874660db30237fb6d8 | 895 |
| 355 | 9905 | 718 | 355 | c90a6158ce3da2742178d39fa6a9d163e8a9d7eb20710488ac950a1f096eb8e7 | 896,897 |
| 356 | 9908 | 90 | 356 | 9057b31a8b2288c13490f45edc14dcd88958285b649d941fa62be96cd685b1b7 | 898,899 |
| 357 | 9912 | 684 | 357 | c42863b32d4aa4b23ce8abd5a2e82cc67a88cb6d5f4c7c4a01a069fc0f39fd22 | 900,901,902 |
| 358 | 9913 | 890 | 358 | 82a8b31a890343334d4d30830692ae4a18abb56e338eebe9323750ece00fb751 | 903,904,905,906,907 |
| 359 | 9916 | 566 | 359 | 7f502c44613216035d5674a6bc7ac0e3285e96a5dd232881686dee58dcdb3ff7 | 908,909,910 |
| 360 | 9917 | 441 | 360 | 1b0a7beb222f9cbf111c5b6958d43219110e2559d711f40e95d3672fa5ad6dcd | 911 |
| 361 | 9920 | 104 | 361 | f0406ad03e961d49fe00dff3b8d4e11cfa30fcab1cccb01973aa0c1d4d0e407c | 912,913 |
| 362 | 9923 | 1942 | 362 | c8ef7587fbb19fed8f49575b1d80d290426ff6c4ef681a724e3adfba40710cd5 | 914 |
| 363 | 9927 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 364 | 9943 | 14859 | 250 | 0da9b8ce1809ed5373ff157e0878306ff5548cc9075758123cd64f293ba6b8b0 | 421 |
| 365 | 9947 | 3578 | 365 | 3e57ffb167468c4b7919605f50bd65b88d826e149c2af0364563db1475e9b735 | 915 |
| 366 | 9951 | 2245 | 366 | d2faa5e88084e9f73bc3f6549f89c55e5ad4463919a4c3311242a5be1fa15762 | 916,917 |
| 367 | 9955 | 1346 | 367 | a8ed9eaa67418583c9a142cbd64558b8ffb40b05c1e4db647362cc90833b32a4 | 918,919 |
| 368 | 9959 | 2437 | 368 | de239db220777e50f072de0eae0b171dfff86c9aef0b8c11eec3cf93f257e7f0 | 920,921 |
| 369 | 9963 | 3093 | 369 | ebca455d5e5d6689ffd327479784e940fa68a87e340d9afcd07a42cec598777c | 922,923,924,925 |
| 370 | 9967 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 371 | 9984 | 5426 | 371 | b250eb3c90d316cff04593a644de68440efedcb378f8f05f3fe73246d3794c03 | 926 |
| 372 | 9985 | 5426 | 372 | 10955b19f723666db5c67401f4b4812b7e8a2b5ef2097cfa1c61c562503206b7 | 927 |
| 373 | 9990 | 974 | 373 | 8f269345d84bb4a107c57bb80a9ac7fe9efc01c9af1305a198e5f794cb7ec0c4 | 928 |
| 374 | 9991 | 3834 | 374 | 5a55cd64113a1ca4a891fb41f00898f4320df8fc8fb78b447d6538aad6656c93 | 929 |
| 375 | 9995 | 4056 | 375 | 6b24a80987e128e4d4f17012a15a5b1cbf37910ee5c44af8a42cd484a7bb72ab | 930 |
| 376 | 9997 | 442 | 376 | 99985e65d0247e46a066de7461ed2885f4caf9b80e14ade86e38985d06441569 | 931,932,933,932,934 |
| 377 | 10000 | 2639 | 377 | 3b60295e4fbfab98472a5c0c9b675901713b034fefd3b00de95355b903acb0aa | 935,936 |
| 378 | 10005 | 1090 | 378 | c5d98e456e91e389dd055aa09a3fcc89afed43566fa05e05fa307adcce989558 | 937,938,939,940,941,942,943 |
| 379 | 10006 | 298 | 379 | 9e4e39630a9b452658c4c2efd5cb0f7c2dd4f91573c9bce8dc76cebb1adb60a7 | 944 |
| 380 | 10011 | 1588 | 380 | fa0d60966278060c78197563e7ba2c275b19bbfefe7a63b442aef440c961564f | 945 |
| 381 | 10012 | 592 | 381 | c3f35090e381236493790cb98a0d07e7622d0b153c0ce2312a0a9892652349f8 | 946 |
| 382 | 10015 | 190 | 382 | 019553c73d2e3eee4e8bb7a3fb3f48192e3d0d2432c9247b33d0d11a6cf1b47f | 947 |
| 383 | 10019 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 384 | 10035 | 9782 | 384 | 242854e7036be7263747e79faecc3a44fd67ea074f4881288ef6439df1dc7131 | 948 |
| 385 | 10039 | 9083 | 385 | 444b480f2bbbb48f0683bf16084f25b1e460d2942b97c096702399b5d9593c89 | 949 |
| 386 | 10043 | 619 | 386 | 3328c2686f11af25861793e1fcadc806eb9303e1f4861c1a7fc4e5be3096aff5 | 950,951 |
| 387 | 10046 | 404 | 387 | 34afbef3e744d2e678f0897a264bae85836843c223c034f9205debec999c6e58 | 952 |
| 388 | 10050 | 463 | 388 | 7e87c018b2c28689ce807a941ab7a5391f8b673f0b4742634431afcf3fecbbbb | 953 |
| 389 | 10054 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 390 | 10070 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 391 | 10072 | 34 | 182 | 8435f9dd967a8e7f95a5203835eae76bcb8b8558e82db82112bb09b9f8c39d9c | 351 |
| 392 | 10075 | 11191 | 392 | 3f06287444a755a569380870e6f41e006d66f21aee775905b6a076fe755c8ffc | 954,955,956,957,958,959,960,961,962,963,964 |
| 393 | 10080 | 345 | 393 | c4b9fa6b51781ca0906be58e655d6e138234b73baa1baf14b10fb602dac6fd45 | 965 |
| 394 | 10081 | 710 | 394 | 5fdeaa149fa809b80fcbab9a5f9c857c363b91db6bb3796df0f539b840dedf15 | 966,967,968,969 |
| 395 | 10084 | 604 | 395 | d6e109e7fc398667bd523f03baaea6cc92e46dd8287e2dabc70366c46fee47e1 | 970 |
| 396 | 10086 | 3212 | 396 | f74bed3eded3ad78941f4e8bdbb290e325cc208e60a843a4ea592d4ba05b16ad | 971 |
| 397 | 10090 | 5073 | 397 | c035ed4db22d176dd62a6ed5b4fa6f3978bad97a2ef7b36416021072ecd881fb | 972 |
| 398 | 10094 | 86 | 398 | f8aea0b5616c9d74e10015142442038b22f77cd6ddcf6a815545da9db47f2cf1 | 973 |
| 399 | 10096 | 255 | 399 | ec7110425c4b3d7de7d5fd77a6d1f3ea97b2f3dc9ec701431547fa34cf4f3af9 | 974 |
| 400 | 10099 | 69 | 400 | fa69388797442b055272e96ecae75387933193c44d692e62ab94edfce4c8f3fc | 975 |
| 401 | 10101 | 371 | 401 | 9fe36bbb953d0f98e9286c60801708f1238fc229cec91cb5f7e8651e8e7fd33f | 976 |
| 402 | 10105 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 403 | 10121 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 404 | 10123 | 15641 | 404 | e87659df345389a516a56f25fd3c06bf2eb99f058650d843cdd77d3050a1ecb3 | 977 |
| 405 | 10128 | 3278 | 405 | d25cce861b1cfa8339c65a1d7a425256959b2867ee48f96eccfafd357823c3b0 | 978 |
| 406 | 10129 | 46 | 406 | 8406d193b60b4a1643bd1218d48b647dbf2d8989cbc46d74d7a26b536aa2474b | 979 |
| 407 | 10133 | 2520 | 407 | 23ba868b1dcd1f5737f610caa4eb9fa054fb48d6c5788af63fa4001e53a476fb | 980 |
| 408 | 10137 | 1884 | 408 | 47516b5763ca2d3ec716e658649615752d9412dc7a9a130fbb9fccb8b32ddb77 | 981,982 |
| 409 | 10139 | 739 | 409 | ce103d93a595c828de1be2e81897ea24f878b8273e1b34f9561b8deadced882d | 983 |
| 410 | 10143 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 411 | 10159 | 20 | 202 | 124f41bf0f8d0afecc04b50d2a69f1560b1cf5b11124c4033fe404876c3fcce9 | 388 |
| 412 | 10162 | 889 | 412 | 00f8c43fba836bf4e4021d8fce994b87b11858e25b3a63752beaaa5f627e212e | 984 |
| 413 | 10165 | 3909 | 413 | bc6745f9c134b149a43600c2945e3f3217a1c4c7fdb025f9b33963ce647454c6 | 985 |
| 414 | 10168 | 2998 | 414 | ff02b7609fbd84bb4039e688389f9423676dfc14320b5313f5f742f1837cc628 | 986 |
| 415 | 10171 | 1166 | 415 | c5297cceaaade97001341859174e0e8f4f480172e2ebd4e25b5852ca50a0a962 | 987,988,989 |
| 416 | 10174 | 6388 | 416 | 16101320400c3b91fef781b41b0622ff20bc12d09957973647035f5fd44bff18 | 990 |
| 417 | 10177 | 237 | 417 | 537d846c1cf44c8508b195149da3e6428adddbd9df723e606adb4de23f8c3295 | 991 |
| 418 | 10180 | 186 | 418 | 521466fbced905208f2e1d620f082245596af7d49b3741f4e15cee4e0826a2ab | 992 |
| 419 | 10183 | 1813 | 419 | f244a1a111af467ea3ae96e328ba4afc25a4a7875831f498d98e1ab8c6ee5ccf | 993,994,995,996,997,998,999 |
| 420 | 10200 | 12000 | 420 | b3b01d822852edcd80343a5040a58e58469f033517e1ad6dd693fb8c10d3e63a | 1000 |
| 421 | 10204 | 6000 | 421 | f9a070e288b08a53c2f859293e4c736197133d717cda475072ecc79808d66a89 | 1001 |
| 422 | 10208 | 420 | 422 | 68c84e30ca71e56ff4d2c0f346bdae061559e30cef3100de7b057b476bff6ac6 | 1002 |
| 423 | 10211 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 424 | 10213 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 425 | 10216 | 5 | 425 | e6a4747deb669406b3448125e08ac074b686c7ed397a67e1c9c250ed5d3b1e73 | 1003 |
| 426 | 10219 | 675 | 426 | 8d08d0b237542c21c214d8323366aea2abf3e078d1d908398d4b9213ca97f7ca | 1004 |
| 427 | 10221 | 1031 | 427 | 2bfacb734f1236b0692003777f87dd9e29a0cf07b73def501d631b0309c8dc7a | 1005 |
| 428 | 10225 | 6000 | 428 | 0118b44c0516b5b404dc019db31b85b3632600d8c4cd4b791d99d6f409b6c4b8 | 1006 |
| 429 | 10228 | 7000 | 429 | 12c3e0981ac04f44592c0358321f50d87a44bed16fba935e9a60e5fc2c101caa | 1007 |
| 430 | 10232 | 6784 | 430 | fd46ce2b7519905197bd2808e45817210fa2afaa8817ff5d5a11a53cef79a690 | 1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018 |
| 431 | 10235 | 7623 | 431 | a974a29604c56edd2d216e3705e8ed090a7001f69e8cf9503c79c9af2cc58a83 | 1019 |
| 432 | 10238 | 6712 | 432 | 5053cbb8fad09c32107cf9d7e8bd1b8c4cb0e3e769b7a7188b16955fd9e9fbcf | 1020,1021 |
| 433 | 10242 | 5581 | 433 | dd51e5c333a0c9c1e5ca44d1fe57c0c9fe035c39dd4936f3ecee56c15b937dce | 1022,1023,1024 |
| 434 | 10245 | 3616 | 434 | ceb60f8da8fc333085b387e2ff1f9bd032199049c0ffa426970517f695102897 | 1025 |
| 435 | 10249 | 15373 | 435 | 73b3e380ad1ef33c7fdf045dcca4d765adf84067381c4e03134464a1b01eaeec | 1026 |
| 436 | 10252 | 1382 | 436 | 1f7614661652911431f396c6b7d4368811f2911a63915826f2af03fc2d00b708 | 1027 |
| 437 | 10255 | 1986 | 437 | 33a8738c2b202a1ad7007c11308dc4b74127c0bd390a4a13701ccd3b6dde7745 | 1028 |
| 438 | 10258 | 3231 | 438 | 12bbfa484397724e1e37d7d0e113c757d0699fa17e97a2675804dd9d46d0206a | 1029 |
| 439 | 10262 | 77 | 439 | a009fc721f262028aabd24253ea715666db28d6398d1c7ced479f40199682d46 | 1030,1031 |
| 440 | 10265 | 3201 | 440 | 573d6668b744022de42394b55691effb261380b8fe1cb1e5ca3531c1328ae663 | 1032 |
| 441 | 10268 | 6424 | 441 | bf58f869ac52e33d63c00af72dc41af216d19e3ab929bc945823fcdc71e46084 | 320,321,322,323,324,325,90,326,327,90,328,329,330,331,332,90,333,334,90,1033,1034,90,1035,1036,90,1037 |
| 442 | 10272 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 443 | 10288 | 14574 | 443 | ea15ab4e7ae4fd133e4f353913849b1157b225f0d9ee84a413d12d52fa6f91cc | 1038 |
| 444 | 10292 | 2899 | 444 | b6f39cd84dfa8616106c1f6f3b9b272f1971e83ccf489c3cd778008109719616 | 1039 |
| 445 | 10295 | 801 | 445 | 7168647ef007ff07b5c6d8dfcdc27bcd897f1726cfae6bbf885857513c4d33de | 1040 |
| 446 | 10298 | 178 | 446 | 5c012815fcbea64edce73d28996446f42d9c48cf33dd81717f96a89ce0171195 | 1041 |
| 447 | 10300 | 2873 | 447 | 68a1b6fe8e2dfdd3744e18bcdc504ef17887a395712e55f3154f3d180f2bf4ce | 1042 |
| 448 | 10304 | 233 | 448 | 345089496edc1df13e5b64a1930d3614fa327f8250ab7941519551fefd107796 | 1043 |
| 449 | 10306 | 718 | 449 | d1bdb7834b5308649b1a1954471e0537a37f321ca67cd992f4775fab7d349610 | 1044 |
| 450 | 10310 | 3660 | 450 | 776ec09c7f3dfe9834ef8f749d0dc5bc7277e6fd7efbfbfd56f803acbbfe8864 | 1045 |
| 451 | 10314 | 33 | 451 | 9ae898ecab80aa6de825b6432c3a86d2e92d0c47f837c03d786e0da33f57b7b8 | 1046 |
| 452 | 10316 | 352 | 452 | 09e14ad1fd705f2773c67d845389f73e6ab78a2474ccce5303f1f8fa1e8b1d97 | 1047 |
| 453 | 10320 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 454 | 10322 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 455 | 10326 | 705 | 455 | 9354a978259d46886bd22d4c816010b72c3cae388fa662203504bfaf145d0e5d | 1048 |
| 456 | 10329 | 1560 | 456 | e159c93537a6630171a817ae3c5be81bfa7a3ca750eb0b70e6fc5ac301ece8b5 | 1049 |
| 457 | 10333 | 57 | 457 | 559b27326a64b01429076b8e94f825a25b756a2b6576ae29e9253f372762815d | 1050,1051 |
| 458 | 10335 | 425 | 458 | 941df2e669d5d8de0f5c602ffe7cb374589fe8fb970e0ec6bcecc43e6b3c2fd4 | 1052 |
| 459 | 10338 | 42 | 459 | 07d05f44f38ccc40ccbfb8c9836fe5656d2c42d69210a5a2442b529bfdac137c | 1053 |
| 460 | 10340 | 1714 | 460 | 9f82ccac466194dd50ac5743369fcc9e58be1dc7b2b651f12234f9ba4b57a7ce | 1054,1055 |
| 461 | 10344 | 529 | 461 | e7d318e3b2dcfd27106778952c694a14c507698a7a2872918e16e326cbb39895 | 1056,1057 |
| 462 | 10348 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 463 | 10364 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 464 | 10366 | 1362 | 464 | e1d70c085221e31abe9e4f81b0a17704b6c0bccbc4ff554196a128e28cdc7878 | 1058,1059,1060,1061,1062,1063,1064,1065,1066,1067 |
| 465 | 10369 | 26 | 465 | b95dc6bde520f5ac05cfc4eb556b79c999f0bf1a1ae76c5f61bbc5920db817fd | 1068 |
| 466 | 10371 | 9316 | 466 | 79953360043e85e2911cd4803f4ac1930756b454af447eb2b6f71e9f25400b79 | 1069 |
| 467 | 10375 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 468 | 10391 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 469 | 10394 | 1413 | 469 | 7dcd000d7b4782d3b569becd6584bfdc663428374437b41e37366bbaeab60755 | 1070,1071,1072,1073,1074,1075,1076,1077,1078,1079 |
| 470 | 10398 | 20 | 470 | 0135ab5cf6b90d378cdcf7f33ea2a2993d6efae494d7ece213b5ddbbb5973796 | 1080 |
| 471 | 10401 | 35 | 471 | e9c25e7e6ef203bf8f35e80e552ef4e26e4cf2daacac2d9221bcddc6d828ec8e | 1081 |
| 472 | 10403 | 45863 | 472 | 3508f14d37236e6e24ce54c2d84f2f18daf21329c1cd540401f72bf36b7b6b50 | 1082 |
| 473 | 10407 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 474 | 10423 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 475 | 10425 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 476 | 10429 | 20 | 476 | 5f549bd5fa16d94e1834967a023a1af9ba2ddc8885b4b08351e9feb1d36837ee | 1083 |
| 477 | 10432 | 26 | 477 | d0b98cd062b3bfaad78cf6cea179860e089436283d355c36c1707ad5a0e6b9ed | 1084 |
| 478 | 10434 | 16720 | 478 | 359b9768f3b10d010947fbb70158eeb4ff5adc2a0e4d1b6d5634932bc00a454f | 1085 |
| 479 | 10438 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 480 | 10454 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 481 | 10456 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 482 | 10460 | 21 | 482 | a64bfe6c0d6ba0e7e3598a2a22d7206795dbf680f1621e9e3223bd1890d9a68a | 1086 |
| 483 | 10463 | 5 | 483 | b7f99504e730aea674ef895e901e3e6eb955c6bd9d5d5e2035c01be066feb24d | 1087 |
| 484 | 10465 | 20295 | 484 | 8951aa50031c33ea67c6150c6f2512d87aaee65dc182eed077cbcb540ecaed9d | 1088 |
| 485 | 10469 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 486 | 10485 | 4932 | 486 | a6e9498134930272aa728a2c09e316a9a7917c18e3575dfe9af810d22128174d | 1089 |
| 487 | 10488 | 3051 | 487 | f14440dbce96bdd741688e088bcd42670e2632017380518270b1a99290aa04fc | 1090 |
| 488 | 10491 | 9200 | 488 | 0797df171599a6f7113214b19e771614098002222c0669837af3afb2a8715ab5 | 1091 |
| 489 | 10495 | 61 | 489 | 8a28c9021b62300773eca8585fd9a7442dad78b634f90aed5e545eeccf65bcb8 | 1092 |
| 490 | 10498 | 2754 | 490 | 7830dbffcc7c8a01db28e0fabae8c8dcb54ed8424220f07cb016e469b48c387b | 1093 |
| 491 | 10502 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 492 | 10518 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 493 | 10522 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 494 | 10523 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 495 | 10528 | 13610 | 495 | 96279b1e1482853854bc6d95846b8217afc888adc565c05c73d055fe6981e929 | 1094 |
| 496 | 10529 | 12926 | 496 | 764e48daf62abe51841226747aa4e977415f2500edc0dfb109a351f8841e5703 | 1095 |
| 497 | 10535 | 514 | 497 | 2e1f2032669ad4783e45bef0a7f91caa92bcbfa61087cbae37f0abc295a1d1e7 | 900,1096,1097 |
| 498 | 10536 | 920 | 498 | 3493c98ebdea2dc6322182e2c359700759d905ff0805d0e8f18d90e89522ce70 | 1098,1099,1100,1101,1102 |
| 499 | 10537 | 215 | 307 | 97d81b312051fcb9f40bdec77ff4ee91ad8aebde53d8bb7c31e3e9d9f35eeaf7 | 671,16 |
| 500 | 10542 | 351 | 500 | 68f1e28426635e8955c9f0269268d3a3bd25e9848532c2ecaef420081ea4c7f8 | 1103,1104 |
| 501 | 10543 | 1017 | 501 | a6a6bd0663d3ce468ceda808e2dc7d18bcfebb7c249d3c5f897e5fff63abf2c4 | 1105,1106,1107,1108,1109,1110 |
| 502 | 10544 | 393 | 502 | 324e97d3131ff16ed0519861ac183c13177d610fcbd2b87509fbc024555f3a3f | 1111,1112,1113 |
| 503 | 10550 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 504 | 10551 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 505 | 10552 | 862 | 505 | 961fcf155c1dc12684a0c8d6d32420e3934f8467e5f5d4376bbc8d3ba68046a2 | 1114,1115,1116,1117,1118 |
| 506 | 10556 | 8 | 506 | 2a76343c15223e9b0d9acb0b837788b09b45c90c13995bb8f1e979ed34406695 | 1119 |
| 507 | 10557 | 528 | 507 | ebdf1eb96be077092375b443ed7deeb57954a64dbacb954c6176cc6053cf83b9 | 1120,24,442,443,443,444,443,443,443,443,443,445,22,446,447,448,449,450,451,452,453 |
| 508 | 10560 | 9 | 508 | 7810bfd443660d8743c5a80d7666ec8f911cb0995179c8c7d074ba248e11129d | 1121 |
| 509 | 10563 | 4094 | 509 | afa212fff91fcb70415845d5764089284c61192e92c8dc84355c838dc5adfb50 | 1122 |
| 510 | 10568 | 245 | 510 | c2f13206486a0dc0a12dc5e29c0a7b8f6f4f081cf38c4694e1a6e023597d9310 | 1123,1124 |
| 511 | 10569 | 1345 | 511 | 562248e03bf112f06932d681c3fadd09babe034bc9bca0d9e2981de268dd7d43 | 1125,1126,1127,1128,1129,1130,1131 |
| 512 | 10570 | 619 | 512 | 7bdc9b4c1f367a6c74a0edd3605786e6af357c326d58cbb082b4d326ff4a9286 | 1132,1133,1134,1135,1136 |
| 513 | 10573 | 1958 | 513 | c258485f9d8c12e06923320110a1e591447aa75c2a5dd7a98eb56bd66a7c7249 | 1137 |
| 514 | 10575 | 2854 | 514 | 88a39b3e42d46270926b29de1aab17ca78552a552c5bf858ba885edcc78e49e5 | 1138 |
| 515 | 10578 | 2062 | 515 | f62bfe7b616917f2da7b26e43b187043aef65206551b5cc7fc4f20adc8ff1efe | 1139,1140,1141 |
| 516 | 10580 | 1793 | 516 | 7dff7ae1869d04757271bd2947b5633e7ba2cf8895a522cafc93a09129235390 | 1142,1143,1144,1145 |
| 517 | 10584 | 25845 | 517 | 4bbffd2dbf0d75c11c7705dcaf981fddd71e3bb94740320e7cd56c8697f9c8d6 | 1146 |
| 518 | 10586 | 755 | 518 | 7ff45c0450f8ff211524f9642c87ee7017401da1880390d510a6ef7d736e211a | 1147 |
| 519 | 10590 | 2263 | 519 | fa05109146e62470266bc696a33b7c19c2082129a0598105652bdc8599b754c9 | 1148,1149,1150,1151,1152 |
| 520 | 10593 | 3416 | 520 | a31a3a810312e8146055148c6bc56d7afad6a2e9888d174e9dda469df9ab1142 | 1153 |
| 521 | 10596 | 1642 | 521 | a40b6b715eac6fe360be9c8a4805c2301044b9f12a6fbb4bffb6931f84be34f3 | 1154 |
| 522 | 10599 | 576 | 522 | 11d5292419cbd8d405b0ccc0d5c11bcd7b0af2b48721dd7d8a31db645f0ed46f | 1155 |
| 523 | 10602 | 954 | 523 | e2394668718f8f851d86abd09ee4fa4708b814dd23c56c8a5e0db3f250241035 | 1058,1156,1157,1158,1159,1160,1161 |
| 524 | 10603 | 7707 | 524 | f160e1fee1aa46d1145db179fb487216e7b9c8d8ea3498e0d24d19c12f3fa112 | 1162,1163,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,1174,1175 |
| 525 | 10609 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 526 | 10610 | 527 | 526 | a3f5e90900a8db09223c461ad9295ba71f2d3193aa5efaca680b918ba3199876 | 1176,1177,1178 |
| 527 | 10611 | 8 | 527 | 98cae3cade824970025d52f8e35222bf07510265ecf5e70a2a3ae5b6149a2fb8 | 1179 |
| 528 | 10614 | 4792 | 528 | abf99033fb0e5f3c810e95858a6e49d5e6b511482e1904c2a6874aa47ee09e96 | 1180,1181,1182,1183,1184,1185,1186,1187,1188,1189 |
| 529 | 10618 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 530 | 10634 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 531 | 10637 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 532 | 10641 | 20 | 312 | c54251774ebdc7ee632027fa52957e55ad89e944372f35ac072916be4084b0b7 | 677 |
| 533 | 10644 | 5 | 533 | 76d9831d2c501836dc4a82bd78a6c64f89288e0a091bc7edd01256141139c3ad | 1190 |
| 534 | 10646 | 9199 | 534 | c9211228c437c45ec19628e27e6a4cbebc5e6020f19ac7fe9322f2866b9a4cc8 | 1191 |
| 535 | 10649 | 7000 | 535 | dcd41ec10a705a7045022c23696133340f6762f92e7a9f8bd04b6bfa9176ac38 | 1192 |
| 536 | 10653 | 105 | 536 | eab11f056110533203fe7389f3708a9b8720413ca03696951587f9bfb99cc880 | 1193,1194 |
| 537 | 10657 | 504 | 537 | a0c0f7b0ffe248dd724c8d973d69acca804d19e2d5f7c947695389046c60e3d9 | 1195 |
| 538 | 10660 | 1234 | 538 | fe8f41bf881d88e730b5cdd46c54d1927b403ca81695c9363e7ee64657099d2b | 1196 |
| 539 | 10662 | 713 | 539 | b894bcea126bf2ca60a036c484fb4d05cbe8865ff71d6521a883b83c45a60e06 | 1197 |
| 540 | 10665 | 698 | 540 | 2f566c61c1a28b6d64fd047706e1b70a8eb691c861732eaf40aaaf15ac90c931 | 1198 |
| 541 | 10669 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 542 | 10685 | 19 | 542 | b21a23909e99b21d5afa769d79aca77ff8320c8b54fd741b97233b975e87a5b5 | 1199 |
| 543 | 10687 | 19 | 543 | a8bb352219e6324023737b838f7a8ed722109d3945329f71c60dbaa0da6a0ac5 | 1200 |
| 544 | 10690 | 2282 | 544 | 6424839c1ed7c47061edba93c2ab322ccbbc1e0f2d600555bb1772be988bf4c9 | 1201 |
| 545 | 10693 | 254 | 545 | da333c0991f0494034a97c22d9931ee38c4b68c5e23b02c88d9f9680f1b23684 | 1202 |
| 546 | 10696 | 208 | 546 | d5d67fff3b5b1419db7c8a7b40ab2c38d3b3cfa427330752577de7ee4ccbff0e | 1203 |
| 547 | 10698 | 821 | 547 | c0bbfcc228ad27420d029b628797de65b31004e6bca9333a0b1eb4b43ae47210 | 1204 |
| 548 | 10700 | 472 | 548 | 04f04b47dab01cdab81778cbe4b660c88d62692510df5c21ec6a40c71d3fb1a5 | 1205 |
| 549 | 10702 | 158 | 549 | a497bc91bb915b5245ea5bf087e2c9a910becda0c66b34400910b1dfed4540bd | 1206 |
| 550 | 10705 | 8 | 550 | ce0a56ab97f68beb2f312df55ee7acbe88bc91b4d461a505d6d89141e4ad7d63 | 1207 |
| 551 | 10708 | 554 | 551 | 6d9f5fb43569e5b65184de7293b595e92fc7cc6312eed4d450f6fe841030d599 | 1208 |
| 552 | 10711 | 1704 | 552 | f2eead4364a89399617f1e2e3b89f5eeadb65a8246d20ad03797f465ee5982bb | 1209,1210 |
| 553 | 10714 | 451 | 553 | ef7c8059d5bbeb91478348228446f503055279a6d6dcda43833b00cee3c772f4 | 1211,1212,1213,1214 |
| 554 | 10717 | 798 | 554 | e89cd0576232d606516ad59dfca6b1794128d1b4c4ce70260f050d8037624cc7 | 1215 |
| 555 | 10719 | 91 | 555 | 26b89260107e778657bd6db3f4bdde62049baf3f3a29e02fe02aa0a9ad2b26b4 | 1216 |
| 556 | 10722 | 749 | 556 | 5573ae7052d98a11297693a15681740047e48835406f5aadb0ee4f9183d684af | 1217 |
| 557 | 10724 | 412 | 557 | b96e607a9e1a35cecb12da8de0da040e2b6d8f8eeff48e8324da60b59988a4b1 | 1218 |
| 558 | 10727 | 610 | 558 | 30733ac5489c67dd24ffc01135af0f0f0f2b005d9cfc0971209d9812e239aa06 | 1219 |
| 559 | 10730 | 409 | 559 | ba29dac876bdd5cdb875deee4b99ee4109a9c783e2a9a10e39eb48eec35df1c2 | 1220 |
| 560 | 10733 | 2658 | 560 | 735c058bbb58b29d249794c9ab89567747f418b99ab6d48ed82b591def325b57 | 1221 |
| 561 | 10736 | 753 | 561 | a348b111ff40156529da1b2215fbe7a6e428ab5168feb90e2163532d5369afea | 1222 |
| 562 | 10739 | 616 | 562 | 3c6806670abb5419bc15ac9ba630a51b328fe6cff3bd8f455113e16f34a00dc7 | 1223 |
| 563 | 10742 | 116 | 563 | a226033dae2a00f2f2b7c75a0dc0914cf12741c78ef849c6a8ea3441be6f5b9f | 1224 |
| 564 | 10746 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 565 | 10762 | 35 | 565 | 19a716bbf9ebe312b9b6bbe691f133e84da89b9607a5acabde1f96941f05bfef | 1225 |
| 566 | 10765 | 2885 | 566 | 2443ab9a52ac146377bf9e46514b766c74ffe6f35d38ff4f50e0f6a40b12cb90 | 1226 |
| 567 | 10768 | 10861 | 567 | ca4e79d50e96dfe1d06a941d4c6c491141a1f4135ce3afdd424c166172e11f90 | 1227 |
| 568 | 10772 | 5116 | 568 | 94e1401748e34f9cd71a9290f37bea093af143d0e399360b8b94a030233e2d62 | 1228 |
| 569 | 10776 | 145 | 569 | 5180601eca144b24c2e4d844c92ec2c46f58fb0e7f302cfdb2735da508c06597 | 1229 |
| 570 | 10779 | 4315 | 570 | 4b1f5bf454752edea073d9ed8469fbed58acc1e8f22c4a9b210aea8c73e1400d | 1230,1231,1232,1233,1234,1235,1236 |
| 571 | 10782 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 572 | 10785 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 573 | 10787 | 1935 | 573 | 55e9a01adc262afd9389ff19c7ad1853727fbcbfe08c851cca00dae2a9a90da0 | 1237 |
| 574 | 10791 | 635 | 574 | ab6ce263b2d756744ef05d8fca810ef8189f11962a455c0b0ccb01b25d08f9b3 | 1238,21,21,21,21,1239,22,1240,22,21,21,1241 |
| 575 | 10795 | 1465 | 575 | 9828461c7c4cebfca71aaeb1d88b4bcc87c57306b903f6600cb6dee2338437db | 1242,1243,1244 |
| 576 | 10799 | 2794 | 576 | c8f37e58017036e2dbac7b71acfb4f0576fd08380d0bc3600c1a20efbcd646af | 1245,1246,1247 |
| 577 | 10803 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 578 | 10820 | 38 | 578 | e5f57905d581f1108968701eb29a13526dfbe8549976f1a567fe0265299ca8f1 | 1248 |
| 579 | 10821 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 580 | 10824 | 270 | 580 | 06949dc14c42bd440bb26a5786866cb7fa78791e0347f4308324ea1755824269 | 1249 |
| 581 | 10827 | 10770 | 581 | 7fadcc0eecc23ed60d29025973464a01edc560f81762cc275c15674f1ae0f726 | 1250 |
| 582 | 10832 | 833 | 582 | bf51ab19d627cb8975a272164a594a46f6cb5b8de3fdbb2d95d742d7e906a89b | 1251,1252,1253,1254 |
| 583 | 10833 | 1039 | 583 | 2696a27c80439026e2509d8e097c2642454d83e65f3c22f2fb75e9f892b9262d | 1255,1256,1257,1258,1259,1260 |
| 584 | 10837 | 398 | 584 | 77a4d7f367588448c67fdcdd06f477f575762180584e9f78f2344813fe2540dc | 1261 |
| 585 | 10838 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 586 | 10841 | 88 | 586 | 995e901f3b819bea45321d8b93f1ba4983d0293722a5734e0b8d93129494c85d | 1262,1263 |
| 587 | 10845 | 1110 | 587 | b406f9de937145531653d133dfc85e67fd2f642fafa231e4c440175f860af8f5 | 1264,1265 |
| 588 | 10846 | 38 | 588 | c3ec07155a9475275fa7e07d09b82cfba2ffc6776a396e6db964a3d2ccf7373c | 1266 |
| 589 | 10849 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 590 | 10851 | 1527 | 590 | ca147d88ff6234d9516c80627ff82b0c7eff2ce8f55f6a033470aa8d1ecda2cb | 1267 |
| 591 | 10853 | 129 | 591 | db8b1d944f9b2eaa5f778c34002212c89d5d5e3b3eb8f317c3bf4ab39040b999 | 1268 |
| 592 | 10856 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 593 | 10858 | 86 | 593 | 34243018f0cc4c7ec168d518f71f5147bc5d2cc71334a3a5e23b9eda09f113dd | 1269 |
| 594 | 10861 | 1582 | 594 | 2f2dfd8fcfb72dff7af5a0c8d05e847cb1f8c1a53be7a4fbe0884bb3da977614 | 1270 |
| 595 | 10865 | 5984 | 595 | f2fafe61bd290474d0500c0e2563d9adc03a02bd2b2c47fa0510032d8c15e309 | 1271,1272,1273,1274,1275,1276,1277,1278,1279,1280,1281,1282,1283,1284,1281,1282,1285,1286,1281,1282,1287,1288,1281,1282,1289,1290,1291,1292,1293,1294,1273,1274,1275,1276,1277,1278,1279,1280,1281,1282,1283,1284,1281,1282,1285,1286,1281,1282,1287,1288,1281,1282,1289,1290,1291,1292,1295 |
| 596 | 10868 | 5028 | 596 | a00fa3d0120be4ddd27298b6f463f772e225c6cda2d017f7a540e48fb759e14c | 1296 |
| 597 | 10872 | 4524 | 597 | cec5529ebc4e332a7028e4ca1123da117a79429aa783a0faf8cf48424fc6d1e8 | 1297 |
| 598 | 10874 | 189 | 598 | 3dd9efb9b4f9cba0cbeae292af497190f745688316e3d2c7422c6a5d5bc49bba | 1298 |
| 599 | 10877 | 5646 | 599 | 255d42be13de2be2f81c74d91d8b09064ad47586591a682168a99e19101b2da7 | 1299,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1312,1313,1314,1315,1316,1317,1318,1319,1320,1321,1322,1323,1324,1325,1326,1327,1328,1325,1329 |
| 600 | 10879 | 1805 | 600 | 5e3f402100c6844cf5f87007868d42b4eb155bb118096e501e6d63b5a008dda2 | 1330 |
| 601 | 10882 | 400 | 601 | f573aa6c5954ddd4e603cd2cb2aa526510d278f4ae98e85ef67fefb539a59ffa | 1331 |
| 602 | 10884 | 3918 | 602 | 129b8078b5fe167cc99857a98a5532728844d765e7ea684fa4b8ce9b0d0bc133 | 1332 |
| 603 | 10888 | 1651 | 603 | c809647c7d714cb856da95de09f6609024519b83b62256fc64326217813bce27 | 1333,1334 |
| 604 | 10891 | 1148 | 604 | b1b572bf9d2022503eae6e7cee943cc435e9cb7607986d71a85b913daf45b849 | 1335,1336,1337,1338,1339 |
| 605 | 10895 | 135 | 605 | 946f987179b132e42fa977a82108afc9e772aed85269cd20b5f2516caf241fe0 | 1340 |
| 606 | 10899 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 607 | 10915 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 608 | 10917 | 36 | 608 | 61b5395b23a186cf49966b80c3ffee8ec6235b86f889dfdb28829f743fb58f47 | 1341 |
| 609 | 10920 | 4916 | 609 | c8fe7adaff369169d548a9d39d6e4a810da6ff12faf921a0c3f538b538517610 | 1342 |
| 610 | 10923 | 10589 | 610 | a87b8b8f9b7c581850c13facd4d4f34820665a93b1d24fc6a8622b9a6cc1e12a | 1343 |
| 611 | 10928 | 79 | 611 | 0f3e9057cbb2016cd51894563998a11814d47e182b1a87215509bf0380adf4ac | 1344 |
| 612 | 10929 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 613 | 10933 | 1345 | 613 | 13f9ddf67fe646335ab8946c84bb8bb9161335710b853928388e899f1fa9f285 | 1345 |
| 614 | 10935 | 864 | 614 | 3c294f5feb4214e72c3b88a4592cd5c897e1d4134d44b9ae972af7a062f56e54 | 1346 |
| 615 | 10940 | 3004 | 615 | 747419a893c3a27772e2b4a478c8117d5d5cd3eaa267acc0446f27daf8c3bd68 | 1347 |
| 616 | 10941 | 2012 | 616 | 2d8fa0cda1f5b9a30fb7aa8eeb369b3526d93c5e03ca24d0e71b757ed2d24ed4 | 1348 |
| 617 | 10945 | 1927 | 617 | 7cf5176657aa50aeb53054183bf4f50a77aad66bbe4b8da449f2a4ff62598c39 | 1349 |
| 618 | 10949 | 169 | 618 | 516bcb422fd7202cfa491c04651ef15c5c01f6068d89644287749aeecba86cfd | 1350 |
| 619 | 10952 | 440 | 619 | 91f985887b73edafbd337a4fd77744b7b17ee8af1ddb7fa2004739ec7a626060 | 1351 |
| 620 | 10957 | 273 | 620 | 859158141ebbc33bf771d004cf0ad83185e3ba9c0a1f10eeda15e07af43e6512 | 1352 |
| 621 | 10958 | 933 | 621 | 841ba8e49920f2ff007b11c565446d4b3f0b3b18a0197b353806983a9603d08e | 1353 |
| 622 | 10960 | 3 | 622 | 1b00bd5c22f08268d8772935cfe346f83a86f7d6fc1a30c1855e83f744f83b22 | 1354 |
| 623 | 10963 | 1531 | 623 | 4048e091d796fb4738b4d437e91ce5c94e2fd115a208ba88628e6e6a8543be65 | 1355,1356,1357,1358,1359,1360,1361,1362,1363,1364 |
| 624 | 10968 | 789 | 624 | 501c4d0cb9ba20003e83edb50dc64eacb5a0e36863a78d963f7d1d3d2c7c6277 | 1365 |
| 625 | 10969 | 673 | 625 | 1a7999db6ae7866e1eb0bf9916a79895e12a53e8b69be2947f0aaecc31fac796 | 900,1366,1367 |
| 626 | 10972 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 627 | 10974 | 3 | 627 | db5b073c3a619b67c619820c10ca93fbb57e0d29dd6c337cf2fde1ce117c5b29 | 1368 |
| 628 | 10978 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 629 | 10994 | 9783 | 629 | 5f0fe5ec44443390d9ded22695a8c77c0b4969d5db0ae47f7f0a5d726d6f4813 | 1369 |
| 630 | 10998 | 9083 | 385 | 444b480f2bbbb48f0683bf16084f25b1e460d2942b97c096702399b5d9593c89 | 949 |
| 631 | 11002 | 609 | 631 | fd36d9729d7f211ac3f063523791256d23ee6c255d6b955d24562ac4ea2019f8 | 1370,1371 |
| 632 | 11006 | 134 | 632 | e8f071b8bfbf8b3803ff57ce430cab6246ffa8762dc043b9b01df3fbf380fc04 | 1372,1373 |
| 633 | 11009 | 406 | 633 | a6e12dde766c000502cb3bdfdd711c890de98e6021d6a8795168a7b880fbf44d | 1374 |
| 634 | 11013 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 635 | 11029 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 636 | 11031 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 637 | 11034 | 18 | 637 | 188020b2463d66783d3d4b84cdf48de0485b547d0fc6c6f6bd4691e9fcb17d05 | 1375 |
| 638 | 11037 | 3000 | 638 | 0a587967a412b869c92a0e1ec39744412935bd870faa31f5e1a90a2f7adb0d74 | 1376 |
| 639 | 11040 | 1518 | 639 | 4a247b2be3b5b5f7edb7c49db23ecb5fe82b25ce83bedc9673f5f5d94b7473cb | 1377,1378,1379,1380,1381,1382,1383,1384,1385,1386,1387,1388 |
| 640 | 11043 | 15927 | 640 | 05160fdfdef0cf7e059f1a9e76f9e82b37d8d27da492fc38411bb2c66f41df3b | 1389 |
| 641 | 11046 | 12119 | 641 | b66f394fb28730e94fd100c6f8cf2e5758c826bcbcc7f2fc3e9f916f848e0545 | 1390,1391,1379,1380,1381,1382,1383,1384,1385,1386,1387,1392,1393,1381,1394,1383,1395,1385,1396,1397,1398,1399,1400,1401,1402,1403,1387,1404,1381,1405,1383,1406,1385,1407,1408,1399,1409,1401,1410,1411,1387,1412,1381,1413,1383,1414,1385,1415,1416,1399,1417,1401,1418,1419,1387,1420,1381,1421,1383,1422,1423,1424,1425,1426,1427,1428,1429,1430,1431,1432,1433,1434,1435,1436,1432,1437,1438,1439,1440,1441,1442,1443,1444,1442,1445,1441,1446,1447,1448,1449,1446,1450,1444,1446,1451,1452,1453,1454,1455,1456,1457,1458,1459,1460,1461,1462,1459,1463,1464,1465,1466,1467,1468,1469,1470,1471,1472,1473,1474,1475,1476,1476,1477 |
| 642 | 11049 | 378 | 642 | 796c4caabb4d0ee667b64098c42c284c2d140b75b44e8281cf73f956cda491ef | 1478 |
| 643 | 11051 | 2383 | 643 | 30c1a98a4a0bf54deecccc1699143cc1d4c43d2200a53efc6e022b996d5ff0e4 | 1479,1480,1481,1482,1483,1484,1485,1486,1487,1488,1489 |
| 644 | 11054 | 4285 | 644 | 709b49c0d66d6595b1e15a9953de73140b87fbcee052d343d9f6639ca60acb94 | 1490 |
| 645 | 11058 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 646 | 11075 | 34 | 646 | 2bc8d8b6d94a236be4adb076c3bbc8740b07474786ea2db953c09926ac7bde28 | 1491 |
| 647 | 11076 | 3000 | 140 | d5886808e3d6547614c328e66c61e1cebcf1773f6cdeca32d362699de109b253 | 249 |
| 648 | 11079 | 11998 | 648 | 5ab3ac5e2c8158e12e285d1914635ef51625126d59b419bd917205d9032eadbe | 1492 |
| 649 | 11081 | 1172 | 649 | dbabcf5a80026556b61c7ebc267e07ca47a97119b431f1f21581288c0ba17d4a | 1493,1494,1495,1496 |
| 650 | 11084 | 16020 | 650 | 3eb775d44cfc4d3206a92ec0a10b866aff73751e93da56df2304df78848fc132 | 1497,1498 |
| 651 | 11088 | 88 | 651 | c8f7edadd5497222bd733a9c529298f2cea9a707b2d8576bc02794484b053cf1 | 1499 |
| 652 | 11089 | 42 | 652 | 04637e6448cc282928bae1d600b9b5cfb9de293b66e2ec315b29ec3c14c963fa | 1500 |
| 653 | 11092 | 195 | 653 | 161391a2289041f14df7d0083549beda35c864637a0feba92b0175cd9ae96ed5 | 1501 |
| 654 | 11094 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 655 | 11096 | 2746 | 655 | 7791e20daf8111dfddbcfd216426a1f84783fc081cd7de7d92c001a174e8fa27 | 1502 |
| 656 | 11099 | 3999 | 656 | 2215a025e96ba609a85f111724efe55a0490eab902fe940d339b11a6b86cd1fa | 129,130,131,132,133,134,135,136,137,138,139,90,140,141,142,143,144,90,145,146,1503 |
| 657 | 11101 | 6003 | 657 | 1aed9c66dd1f050b12cc8622d5f17f4690bf5c3adc60ebdd39f2533c3e275ac2 | 1504,1505 |
| 658 | 11104 | 609 | 658 | 416a913daf63bf7a6af743e5074f727dece2fa905f91d34bb911d2135392fb32 | 1506 |
| 659 | 11108 | 3 | 659 | 3332300cb375f3fb00ce2e3e1e73770f115fd39602012847a83f08327ced169f | 1507 |
| 660 | 11109 | 933 | 621 | 841ba8e49920f2ff007b11c565446d4b3f0b3b18a0197b353806983a9603d08e | 1353 |
| 661 | 11112 | 2078 | 661 | cb97ce0180ba699c45e03cd7363a885119c2d7ad7e834a466ca5c3250dfc6e2c | 1508 |
| 662 | 11116 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 663 | 11132 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 664 | 11134 | 11985 | 664 | 9f428367c81514374046d1466a29ad9b15b8802bf63fd765f07a51a62a252036 | 1509 |
| 665 | 11139 | 1065 | 665 | e4c765026a8300ba96aeba20f5d8467a2e37f3bfa78eda9164da67d53678e453 | 1510 |
| 666 | 11140 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 667 | 11144 | 8999 | 667 | 1ba7a49bad41e7d7cafcce31b2c388136d00dbd469128725010c60bc2e3ba9a2 | 1511 |
| 668 | 11149 | 4200 | 668 | 13c0d608a1d108ede44b649d8f26bc81406c8af976f225cc74239b642d782ac7 | 1512 |
| 669 | 11150 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 670 | 11155 | 5051 | 670 | 50be0d4b344fd6bb7a4f197ed0d051d13b260a78925fbe1eaaaead492fa5c52e | 1513 |
| 671 | 11156 | 7 | 671 | dfe57bb7017bdb45912800b31fcef9dff8ae89f7419fae1802834d32371b2c06 | 1514 |
| 672 | 11161 | 1146 | 672 | 3af6799faba7d7c17dcc5b4c704316db76cec010c27872707846a03034d81ef3 | 1515 |
| 673 | 11162 | 131 | 673 | 8a22c75659ebb21e4b730e3334dd044f6d6e42bcd230e2294cff8a0bba1bfd80 | 1516 |
| 674 | 11166 | 522 | 674 | 59f96ac1a6233976f080a426b5576a24056948a07be63db70ef99a6f9022aeb6 | 1517 |
| 675 | 11170 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 676 | 11186 | 35 | 565 | 19a716bbf9ebe312b9b6bbe691f133e84da89b9607a5acabde1f96941f05bfef | 1225 |
| 677 | 11189 | 8999 | 677 | fd1a5f6045ca0464a393a2413d868dc6dcbe5dea2fc5049b95d79fa77e4df59c | 1518 |
| 678 | 11192 | 7999 | 678 | 601b28c3f67b1fcb9119a602cd4ad9f099c42595edcd7a974adff651c37d99cf | 1519 |
| 679 | 11196 | 12 | 679 | 9bf5a6cfd57b07ad3edede7acd5d29c5c6f60f6cde561f1f3b4088d5cfdb5390 | 1520 |
| 680 | 11199 | 3189 | 680 | d59a74b8d3e3e4e05afc7b7bf293fabfd6da715b8d882763a9c8390b375e9c55 | 1521 |
| 681 | 11203 | 5999 | 681 | 92c1c696d4b001d35f4f384918bcef87d94fcb6a9d4a0258ff2a1d2b9aeacde4 | 129,130,131,132,133,134,135,136,137,138,139,90,140,141,142,143,144,90,145,146,147,90,148,149,90,150,151,152,153,154,155,90,1522 |
| 682 | 11206 | 2498 | 682 | 87a2aa86454ef311783bb0382ef015b3e2dfb150bec24ed0e98fac1f8a31ecb7 | 1523,149,90,150,151,152,153,154,155,90,156,157,99 |
| 683 | 11210 | 2671 | 683 | a706c2e7da45541f5d02492217248640ddc9e95a99f4a604b435a5e81866eec3 | 1524,1525 |
| 684 | 11213 | 1595 | 684 | 8d37a4b0ed4ecf470f8650b772877932ac3b3c7a110da377dff2f01a02a8073f | 1526 |
| 685 | 11216 | 3192 | 685 | cd4e21365fba816052b8e3a0d5bc089688494d9554efdcc2ab770f69ae1c8a2f | 1527,1528 |
| 686 | 11219 | 2159 | 686 | 4cba16d3ab07d3ee6f9981ddcf9e2ed9107119b68b5057501a23c744f79557c1 | 1529 |
| 687 | 11223 | 9455 | 687 | f603ed153f58e14ea6ad7aa48a11bd7d202c63115d8f1ee48a61ba377748213f | 1530 |
| 688 | 11226 | 4500 | 688 | 8271c2b8af1adad68c6aab5643cfa039b43f3a42af78cbee045867ddf96b8297 | 1531 |
| 689 | 11229 | 57 | 689 | 2b5d24dc4a72e3ae1d2a354c81a6413ed2b6ef6848a2c95be12307dcecab56fb | 1532 |
| 690 | 11231 | 142 | 690 | 106646a657d423d32134705af7284f05ebf4e71b706adac3cdb6bfc69b9c6fae | 1533 |
| 691 | 11233 | 677 | 691 | a590722df3d682eef300b305cc1d49cc463e653f106ce58fe5737e34531931c4 | 1534 |
| 692 | 11237 | 1261 | 692 | 3a80c3a1313a845596b2fdce0648f80ec3794d203c9f39ff7b5326bab52c669a | 1535,1536 |
| 693 | 11240 | 875 | 693 | d849b544ef67a3403dbee7fd6a78a8864aa99e9cf6732536f29730ab3d6bccac | 1537 |
| 694 | 11243 | 298 | 694 | c928bf3c1fbb5ceca33dcf34950c01610aa0d77d16b96f12f1469c8459d1d35d | 1538 |
| 695 | 11246 | 3006 | 695 | ee4127fd9070fae10bd8a34a06de2282bc51fca2674dc78a4d890251ed655008 | 1539 |
| 696 | 11248 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 697 | 11251 | 3509 | 697 | 3062868454f7680be54390b8b57fc0dee4da14062bc106dac8f448b6a05d3d4d | 1540 |
| 698 | 11254 | 163 | 698 | b05d9e880c98a8466c19f155c7fb4d528ac99e16dd3c47342941685ec251c173 | 1541 |
| 699 | 11256 | 3011 | 699 | a42f4294ee279a0d3fa23e3d3f2cbb969dfb9eb8d9eb25de49eb01e3c31add09 | 1542 |
| 700 | 11258 | 2000 | 700 | 28bb66fda17534bb6db07613c924e2715d5e6d2eef8da56e4aa2fb8222f70c03 | 1543 |
| 701 | 11261 | 97 | 701 | 5972a6313e681c5f5156c17d783d40db4ebff831c033539907c09cf7d92ebf77 | 1544 |
| 702 | 11264 | 24 | 702 | 15528e9a0a9ddcbb30811a38e0fe67e6e81df0875efe59033469868a86273818 | 1545 |
| 703 | 11266 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 704 | 11269 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 705 | 11271 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 706 | 11274 | 866 | 706 | facef2a77fe36d9c03071fb4da5c3e29c6b528e2872994129b5a69f2a7dc8a69 | 1546,1547 |
| 707 | 11293 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 708 | 11297 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 709 | 11298 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 710 | 11302 | 26 | 710 | e1b158aef153d07eccf20f9c9e6d9a0da4dbe41d024a02890cd201e3c4a54a9d | 1548 |
| 711 | 11305 | 5 | 711 | e72c308d9672047c54160772f42eda4aeb461d0e37aa97a8b861167efce985c6 | 1549 |
| 712 | 11307 | 12950 | 712 | 01e42dd16fba4be87dd35374509e6b578bc1cb5089e551d54c01eafb87aac76c | 1550 |
| 713 | 11311 | 19978 | 713 | 2d3335b0c2b1beafb4d8b6d374f225f12e85c14e7b8d5c3e19ec3c3e9e8e1c37 | 1551 |
| 714 | 11315 | 2023 | 714 | 2282ec7d535a2fbe25c5b9855a40f63da71a80369732fc002d3c747e2cc31019 | 1552 |
| 715 | 11319 | 257 | 715 | 2343effce0ab501cc0376dbd381abcbfc897a7e2680563e87175972ca8c21e63 | 1553 |
| 716 | 11321 | 2532 | 716 | 73ef0f01a47681dfb02a051748d5b885fe38a4d22fbaaa96612f7d2c0bf6d130 | 1554 |
| 717 | 11324 | 2981 | 717 | dee3c3338dfc4efab1234af56b8bb6e8ffd50c9f62d3f6e19580f4b000535b9e | 1555 |
| 718 | 11328 | 885 | 718 | a0b0b51c6d5285cd014e871eb5103b0df2696023c487174db13bf272263e6e09 | 1556 |
| 719 | 11332 | 2537 | 719 | ba365ec8632b4608d55b8b7f9755ab1b8727cf568a720e5f4b2fe8832eee9fda | 1557 |
| 720 | 11336 | 715 | 720 | a93e6a406a06075745b6ed5fb027eb58a5e9979a7b7e0bf4dbff2ec35bf7e62d | 1558 |
| 721 | 11340 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 722 | 11356 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 723 | 11358 | 20 | 202 | 124f41bf0f8d0afecc04b50d2a69f1560b1cf5b11124c4033fe404876c3fcce9 | 388 |
| 724 | 11362 | 4254 | 724 | 942e69c7458afd83f0068faf54d95d777d81a124a841ea274e6133259574324d | 1559 |
| 725 | 11363 | 35 | 725 | 96b58eab287dc2cf8718477e5728811ae928da1aa8e86143d5d8dfe73f19f94e | 1560 |
| 726 | 11367 | 974 | 726 | 59477a2efd1b3d80396a8f5208770b15bbf7cdaac14787398c88269dfca65798 | 1561 |
| 727 | 11368 | 396 | 727 | bfde954d57b53471409233f98d5259001e50c963f8012a61526af6dc7f31053a | 1562 |
| 728 | 11371 | 20 | 728 | 084eff29b197c8d6c2c6a01eb7409e0e78402a64ee167b5ff615dd3f173e1370 | 1563 |
| 729 | 11373 | 52 | 729 | a0564aeaacee03400e825642d3e62211fa483305e9c81f4207ee18913f08cfcc | 1564 |
| 730 | 11376 | 5690 | 730 | 5bb38bec396668dcc5ae4e6e4b7db57a3408ee171848090666a6a08fca4b6a62 | 1565 |
| 731 | 11379 | 440 | 731 | a58796b124a10b95a0a5879fc82c59e98f9a1f8f13e925e592fa052e1dfebfa2 | 1566 |
| 732 | 11381 | 3715 | 732 | 30f807733e66ffa0acf7cb8cfa528949c1d22ea55d7b132d5064bd137289fcac | 1567 |
| 733 | 11384 | 933 | 86 | dbd284f3aa50d28880b17828e180c20d458128cdc239e6372c1192f89ae3b990 | 190 |
| 734 | 11386 | 934 | 734 | 3928968ee988b05b4d7cc438b0cf564324b235ae63ff66832d2c6436947ac28a | 1568 |
| 735 | 11389 | 588 | 735 | 6dbaced1bdff7fc9c8a5d91295c1fe0832978d1b1ac01a84922784c1c736f439 | 1569 |
| 736 | 11391 | 103 | 736 | e90ac2ec5d4baf1205be7ed75751469c1569ef1fcaf87323854f186d1e1cca9a | 1570 |
| 737 | 11394 | 22 | 737 | 402ff9a870439a26ddc69da4c7352f4c177184b0ea3d25d5d92ef6a20c5f0211 | 1571 |
| 738 | 11396 | 225 | 90 | 351a62a0eb30cda2e6edae78f9b0576962418617bef5f7dce72ae640d4ff0b8e | 194 |
| 739 | 11414 | 14637 | 739 | 6e89c24951ee72445b6c8209ad4cb2b96d7f18bf19852b2526cb32a5cad4f9c1 | 1572 |
| 740 | 11418 | 19832 | 740 | 31e85c484bff68e40cadceec9bb8a5ea5f0bca24d511968424ff359114267ad6 | 1573,1574 |
| 741 | 11422 | 138 | 741 | 5123fdf7a67b126684d68d982d2d7cd159ee91924ebfa175129a01ef6d31e020 | 1575,1576 |
| 742 | 11425 | 2782 | 742 | a0a871d6312f689d8fb9b118658fbf7278df7c7a4f0ea1ca6b6f84f26f2f624e | 1577,1578 |
| 743 | 11428 | 3894 | 743 | 1f06ca3ac944df1a4025726456e2972b6d049fe0d73e9ae367f6551cbacea469 | 1579 |
| 744 | 11432 | 102 | 744 | 5fc814934d22946acceda9437ba642edb2ebc085e78392fdd470c40aa110ef0c | 1580,1581 |
| 745 | 11435 | 1520 | 745 | 78a205f676c68855ca7f4f9c15c1f7f334157bdc01ae3d44b5329682bb6d7c61 | 1582 |
| 746 | 11438 | 1660 | 746 | 00847abea8728a2792b490d40ae744662a9f39366beda398266cdaba7800acda | 1583,1584,1585 |
| 747 | 11442 | 694 | 747 | 3bc51adeafb48cd659a69e04342d7245cebb538d9145f25df44ed81920b413ec | 1586 |
| 748 | 11445 | 1774 | 748 | dc67bd9d3cb8d9ed5617942a9d488ee90bd5fa7a758c0eab8017219cd197a5d1 | 1587,1588 |
| 749 | 11448 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 750 | 11450 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 751 | 11455 | 615 | 751 | 0e3ee6fc548bda8fa376d9d96380fe3ddc586eea0c020356b46d5e1d87b8f2ca | 900,1589,1590 |
| 752 | 11456 | 638 | 752 | 75f6062fa25e259267bafe26518c4997281332e4d539707e1c9277fb8b20b9dc | 1591,1592,1593 |
| 753 | 11460 | 1045 | 753 | 870ca1266e16ab21e327621e04d52713f8d825a7876384ff5972ba61bd1d269a | 1594,1594,1594,1595 |
| 754 | 11464 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 755 | 11480 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 756 | 11482 | 1444 | 756 | 118e0063fd452f6de34998c8e0a04008399d1d2876b0fb8a315da363720c59e2 | 1596,1597,1598,1599,1600,1601,1602,1603,1604,1605,1606,1607,1608,1609,1610,1611,1612 |
| 757 | 11486 | 18 | 757 | c0b93bd30e95fe52b0f7d2ae9846d280cd652f354c08d7677fde7eb6a004ab70 | 1613 |
| 758 | 11488 | 5 | 758 | 38a76f196762fc9829f998f9fb9664c24aafb49e809bc847b254089eeb7bfc8c | 1614 |
| 759 | 11490 | 1629 | 759 | 18228b7bd901257ad9e413edd56a4f947f8e9d85da547ae9077c96f6969827f4 | 1615 |
| 760 | 11492 | 9849 | 760 | 9142edab2027d71f9e1859132505fd0d688c09dd445f89b857fb0a786cf58969 | 1616 |
| 761 | 11495 | 2391 | 761 | 62c67e3b0c5ac7b37763d9996c0c6f8420eb26d5c7c28458b33ef7a867d258df | 1617 |
| 762 | 11499 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 763 | 11515 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 764 | 11517 | 4916 | 609 | c8fe7adaff369169d548a9d39d6e4a810da6ff12faf921a0c3f538b538517610 | 1342 |
| 765 | 11520 | 3927 | 765 | d674d19b7b9f31aff8750e25490dd4004ea6adebc2722f21064335c52c5d01ba | 1618 |
| 766 | 11523 | 7189 | 766 | 6b2910c171ca10e59cafe1a7f4a93089d037865c7f201096599c877fdb6497f8 | 1619 |
| 767 | 11525 | 1190 | 767 | 0a32df8ae5ee1e1c5faf0e0d42926f3189dcbc24b723e3346e2b41cf132b1b79 | 1620 |
| 768 | 11529 | 1342 | 768 | 077d3d093a8138d6448fc4fd61f574a3752c01c730eee1b348e5ffca43438f2b | 1621 |
| 769 | 11530 | 2426 | 769 | dbcf04e59e5048e8763bea9d2dc937f264f77e369cd457e9aa1e7f3c33181c46 | 1622 |
| 770 | 11534 | 1335 | 770 | cd551f7dba54497cc447bbb21e321f0ae977d71cce5a179da020a24578e73ede | 1623 |
| 771 | 11535 | 1576 | 771 | 0bac5260e5c92e9cdf0a5eb0b837e9a74f7a9fe4a335471db9aa1a4b9ffc1be1 | 1624 |
| 772 | 11538 | 396 | 772 | 7a59d641c18b0b6bd775e8adb86d106448569031e0ce912c7c8fa422a844dbe4 | 1625 |
| 773 | 11540 | 1957 | 773 | 38bd1b38c47f878703b6447e9a94611b23fb0d942d86326e063e6f0aab83b835 | 1626 |
| 774 | 11544 | 1415 | 774 | 11e66f52e5b8054cae4461f56be1597497e8e020338091d2b1d2d1ada46b7131 | 1627 |
| 775 | 11545 | 2629 | 775 | 5801e8a8eb802fc13ae27c9f1fbbf47d990a926d3e9867e86f43a767c419112e | 1628 |
| 776 | 11548 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 777 | 11550 | 79 | 777 | ae4a57caea671e44f2c729929e019c2ba6ba5b98d2b2f377bf816ceecc18f597 | 1629 |
| 778 | 11553 | 64 | 778 | 1c86b2bb492dff9a2885140d569a22fbc76761264643dba4ba102ceaf8371a73 | 1630 |
| 779 | 11557 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 780 | 11573 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 781 | 11577 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 782 | 11578 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 783 | 11582 | 20 | 202 | 124f41bf0f8d0afecc04b50d2a69f1560b1cf5b11124c4033fe404876c3fcce9 | 388 |
| 784 | 11584 | 9800 | 784 | c7819884407c09e57d348acdc4236d6176b16123fcd35d23f592d0db1c9792ce | 1631,1632,1633,1634,1635,1636,1637,1638,1639,1640,1641,1642,1643,1644,1645,1646,1647,1648,1649,1650,1651,1652,1653 |
| 785 | 11588 | 1280 | 785 | 3ab1a3d9367ea9f1806a6ce22ee304d6b09feebac651858fb0f91c27fe4326c9 | 1654 |
| 786 | 11589 | 5998 | 786 | b3c2e6546eea142b2f7ae1a12b35fddb907dac5cb0ae24ec135d00678bc77596 | 1655 |
| 787 | 11593 | 175 | 787 | 0448d3c90ec390e840a43689dff0b45368738317f169827a0c654fb754ecf73d | 1656 |
| 788 | 11594 | 14000 | 788 | f86570a49f07485c77c32ffdb1c2a456c1e735f6edbb230ae009dd7b7da2d316 | 1657 |
| 789 | 11599 | 14095 | 789 | e1269f1498f84b3fdad38416093c966738ab4882521b85ecb059cf1ee8c96560 | 1658 |
| 790 | 11600 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 791 | 11605 | 575 | 791 | 6b3397ac26bd1d308a1e0d6824e31b86df73f40901a12c7be11b7443bd31b177 | 1659 |
| 792 | 11606 | 36 | 792 | 99eb99dc2d6875bcc3f43ac83ef5905d4db593812c5f00c53012f2d46792d333 | 1660 |
| 793 | 11609 | 7306 | 793 | 45cc5a3d88cc445e6e3fcdff077f5c6ab04f23b5d8ddc98f7fbdbe5e71e75225 | 1661 |
| 794 | 11613 | 4988 | 794 | 0be30baf5a35fd15331c8acd8e5cdf61d50414f3f4fb4921ce00937dffb13f29 | 1662 |
| 795 | 11615 | 1474 | 795 | ce8c65c64b8441d4877c3b11914eb108137f71293675edc97cbe79ab188c7c75 | 1663,1664,1665,1666,1667,1668,1669,1670,1671 |
| 796 | 11619 | 15 | 796 | 3f5d5a23e6514352cadcf6f2906a3d14bd409c65dc986027d5facb09d284a394 | 1672 |
| 797 | 11621 | 181 | 797 | 1f9b0e8ef169e52fc12efee7d5f7ad9c13b6c645a8f7536a0091ef8682bcf40b | 1673 |
| 798 | 11625 | 10021 | 798 | e139940c017bddb3aa945f64c8f3f44c5ec7e5a03b0dea9fabb4a4af2bd159d8 | 1674 |
| 799 | 11626 | 1016 | 799 | c828cc90e1639468b245c22f18a3a4700ec5a2327e215038bc90993252c6434b | 1675 |
| 800 | 11630 | 1902 | 800 | d9e734dd2b1853811bb0176abc3b7558623175c6fd9c6ba817584b81664d3307 | 1676 |
| 801 | 11631 | 2378 | 801 | 84d335881b1c9682037ea6582123e1e9a325dbccada68c30961edabe851550ec | 1677 |
| 802 | 11635 | 5000 | 802 | c033f839b58fc4b4f043ff69496c87f5392a186750d800a2606692fc386043c5 | 1678 |
| 803 | 11636 | 4222 | 803 | 50c292038ed6351aa7af7d55736f1f621e3c14aa2bda09cc87046567e4a3ed8d | 1679,1680,1681,1682,1683,1684,1685,1686,1687 |
| 804 | 11640 | 8009 | 804 | c32d318af2dd23ef60b7e4f5c3961c23ec76c8fe4585b451e9a020ab6c5532c6 | 1688 |
| 805 | 11644 | 2680 | 805 | 75ba130361989136753ae6d9acc168860519deac4fe543b81ca37206eaee9501 | 1689 |
| 806 | 11646 | 933 | 621 | 841ba8e49920f2ff007b11c565446d4b3f0b3b18a0197b353806983a9603d08e | 1353 |
| 807 | 11649 | 7009 | 807 | 832d8f9e94ce4f36afa908864336562e0d05ee8605360e05e6e48fc2631205fc | 1690 |
| 808 | 11654 | 369 | 808 | 996d08ee356654a61f106af321d56354cb2423948882979cc295238863a09ae8 | 1691 |
| 809 | 11655 | 51 | 809 | c4e44bae7f8635d6b3a8f763344a1e524cab37afc5fd117236f9865367d2600e | 1692 |
| 810 | 11658 | 3203 | 810 | d0a726f1695e3282fcfe29585d0fcc55eb9044d26b28dda06aab6f45e32e2550 | 1693,1694 |
| 811 | 11660 | 9024 | 811 | dd9aa494e2ca0816a4abbbc575bdf5d48e991d010c21f9f131fb8b2e4fae3295 | 1695 |
| 812 | 11664 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 813 | 11680 | 9724 | 813 | 9deb4cf0591aaf06db6f14dc90f1cdd58e09583f5cbbb1811139f9f3c03cbfa1 | 1696 |
| 814 | 11684 | 1336 | 814 | 9d4e992338f22ced87e78168b5d11f7229b976660613b721aacd3253109091ee | 1697 |
| 815 | 11688 | 4880 | 815 | 8d98037fa34480a6747159ea38ea6af9c0cfd7eaeb5f50d031d21c0f9be854ac | 1698 |
| 816 | 11691 | 866 | 816 | 87e8a828ae1d690852cabe3c473f13241b5a53063f4a949e7ac2efb76371dc72 | 1699 |
| 817 | 11694 | 539 | 817 | 42cd9a3e190dab6f34bd7148e290bf8ac491d2b180f5023a1573a6b849a33091 | 1700 |
| 818 | 11698 | 1820 | 818 | d2eec0bbd4d8bfd485bd7dd7a89f97d3d7656bd2290791f995d5e7a2f862eb76 | 1701 |
| 819 | 11701 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 820 | 11703 | 3231 | 820 | 96237279c513873ac61eafe53253ad5eacfd546ce6739874a733b4f251ac4f26 | 1702 |
| 821 | 11708 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 822 | 11709 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 823 | 11713 | 4022 | 823 | 69231337f93e58154a315f30b640ca5d955e81f5a39689d65f851bbbd83f3ebb | 1703 |
| 824 | 11717 | 864 | 824 | a1a1a35d95f0fb3bca267add0f1e8bae2371b9885c98eb231a244e664539871d | 1704 |
| 825 | 11721 | 3239 | 825 | e24ebd8d0ce1d4cea46837897a44c782a6f140ebe721fa9733183801e9d7c47f | 1705 |
| 826 | 11722 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 827 | 11727 | 1338 | 827 | cd85340dd2e48e6fc10f26a4d6f3133a40d7674dd7e868db50adb998cd63d676 | 1706,1707,1708,1709,1710,1711,1712,1713,1714,1715 |
| 828 | 11728 | 6000 | 828 | 124f5a8c8e6562e22b6f2ff21263911f086c4ecae7a5ebfb15b5bd4673855f17 | 1716 |
| 829 | 11732 | 2086 | 829 | 195eaafa06d34a6b96fac04c903d3213f8080a6b36548ab1cd0486bd647db82f | 1717,1718 |
| 830 | 11736 | 5217 | 830 | f7387359a9af0ca315392ce1c11babb08de09ae098b5429386f8befbdb363bcb | 1719 |
| 831 | 11738 | 1967 | 831 | e918e373865614ed16505b14648b4f7c6d0ae97dfcb521772207f3f7105cfa74 | 1720 |
| 832 | 11742 | 2410 | 832 | 2c232c4171ba988fc95a22d09da405549820bfc8c8af73fda33cfbab3492a76b | 1721,1722 |
| 833 | 11746 | 122 | 833 | 90bc714332609947b015be8f56870b57ad706db705a12d65000e1a0200ae4505 | 1723 |
| 834 | 11749 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 835 | 11765 | 69 | 23 | 80ac1c16fbb32450916fe8abec6c9dcf48f4e146fe3fb21e6f9522e1a6ab5233 | 101 |
| 836 | 11767 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 837 | 11770 | 10 | 837 | 0fb7ee277beee7a9283b27bd56d763b06981891698188338947a6381a8bbbab6 | 1724 |
| 838 | 11772 | 26 | 838 | af3081329c15ad17d71a1fd1bd8e3cbca8bb1c8a37091ba0aebf2d92ccd43892 | 1725 |
| 839 | 11774 | 16369 | 839 | d017d3774f5a85e3ab7ebd0907d7a7840db98e21e2027574a81e24c5435af837 | 1726 |
| 840 | 11778 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 841 | 11795 | 8000 | 5 | 386e4cae68842ebdeeabee510b3739535548885f02d3312608a1ea424cf789a3 | 5 |
| 842 | 11796 | 1382 | 842 | e10a9c90882def2a69eae92c08198839152ee556292f68d01272b1d0072ec255 | 1727 |
| 843 | 11801 | 6999 | 843 | 4111298145283a83349fafde019a130c24814c6df1c474d42c2ef3ed4a8a88e4 | 1728 |
| 844 | 11802 | 1224 | 844 | 3b4b366c8919b3614563c1595e38ab3e5a08ef2cd84f75375d1570ffa1f80447 | 1729 |
| 845 | 11806 | 8731 | 845 | 252c189a6a74d5483ef9cc00f23c0d55ee5838b7132b983bff0d8b584848aa81 | 1730 |
| 846 | 11810 | 566 | 846 | 73f287ff89e2db94da9105bbb09ea9478547592b3d886c20f2bee7e2cb4fc0cf | 1731 |
| 847 | 11812 | 2153 | 847 | 6f527ccc4914bd89380a7537b8f5fa654d00f79579b73f6baca53a3573124295 | 1732 |
| 848 | 11815 | 1726 | 848 | 073748f35ddc6c97d212d4d9a3206e04fd2ec8a0999f3df3bea71f987584d30c | 1733 |
| 849 | 11817 | 3613 | 849 | e93440b53d56689e151a7689e3bac4b2a48523c0f66073800964679fb2ebb82c | 1734 |
| 850 | 11820 | 2345 | 850 | c591129a8330cb0be60ce5191a76866fc02aa9e6c25ba255b9798332e0771b5d | 1735,1736,1737,1738,1739,1740,1741,1742,1743,1744 |
| 851 | 11822 | 745 | 851 | 271f7cdcf5ea7efd503a0daf9da3ff2c5bd40cb974e6b50aa386d08406cace43 | 86,87,88,89,90,91,92,99 |
| 852 | 11826 | 6481 | 852 | 33ded72d767b97bcc9f74cec0eafdf89e67e0077d5f744ee5d96d34b0a734d32 | 1745 |
| 853 | 11830 | 2086 | 853 | cb12f114a0ae385f98a361adc6a0c149da2c9d023d43b70654cfaf9b8aeaa355 | 1746,1747 |
| 854 | 11834 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 855 | 11850 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 856 | 11854 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 857 | 11855 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 858 | 11859 | 20 | 858 | ccaafa4e8c39b2b54dd67f0c4653d59370e8f0c5a2859c6035dbb9f5fecdd6f6 | 1748 |
| 859 | 11861 | 15 | 859 | 0b4b80e62646322dc1359488b63c0cb86a698418d39361b043b974316fe22706 | 1749 |
| 860 | 11863 | 1851 | 860 | 07323782dabb436cc202a01e861837cd7484d6bbd93be29e89dc83b0e81e5c18 | 1750 |
| 861 | 11866 | 11034 | 861 | 6eaa519fdc3c65add9518ba983b192d7e9dd04d005253b955d33bf3513d0af28 | 1751 |
| 862 | 11870 | 478 | 862 | 544fb777c8d2b713ac4236a9bc92f36df8879e743a653b1e7c05aee46ff2ce08 | 1752 |
| 863 | 11872 | 1811 | 863 | 123ac7136790958132be1e6d4ff543501cbc25f9cd5304e8311821e2a95d5291 | 1753 |
| 864 | 11875 | 99 | 864 | f96cac50b8f93017a9d6526930d0308b0382009a591c70bf952efe09263a549f | 1754 |
| 865 | 11878 | 226 | 865 | b62d7ea89ccc37513babcc7b0671e52523986755675d0528e12973b3e31c360d | 1755,1756,1757 |
| 866 | 11881 | 240 | 866 | a434296dca25d540d3560d6756d4778b099c99d598229f9064f292c6db43e9b6 | 1758 |
| 867 | 11884 | 1222 | 867 | 0dff82ed14421f984e547a439cf8fc25a7bc7efe1e4d4fafb6832ab17b32d218 | 1759 |
| 868 | 11886 | 287 | 868 | b0e1e51f809270a5d053389c5a2f0e94a35469244f9f3248649ebc6f1f42b5be | 1760 |
| 869 | 11889 | 120 | 869 | 4315a27a721cf5b476c40821f3030fede5c69af4e7a5624c034890d6b0f0e8ee | 1761 |
| 870 | 11893 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 871 | 11910 | 30 | 871 | 306930985e6a2b8320c24294948b07afd11dcf86d102284bc6e63b31a16c7ba0 | 1762 |
| 872 | 11911 | 30 | 872 | 90a3b7a55b7287863bef3006b3d913ab2f3809bd1ad093ae1b73e2f25e423735 | 1763 |
| 873 | 11914 | 5673 | 873 | 819111a06f4e5be93099d13d290e40c442f6dd586dbf24e0ec21dbb55019d7ba | 1764 |
| 874 | 11918 | 471 | 874 | 237779d94df82d0e8281e071242173200fad66d82d92b28e34a07d10614fd347 | 1765 |
| 875 | 11920 | 2265 | 875 | 655dbf3b1e57c6259f234f637bcd955c02d2130422eb50d07eb48661708d5925 | 1766 |
| 876 | 11925 | 2649 | 876 | 1197a51260b4646a2c0c5e78b4687a58ed32bed515ed6ef383e2d820b2aed01d | 1767 |
| 877 | 11926 | 3213 | 877 | a978a27ea9b0e9b95aa39420eb03320d4397100329fa3b173b67b874d395f0f0 | 1768 |
| 878 | 11931 | 1901 | 878 | 5be2135f6b0d5f8472a5566de53ee515f6042523d1b5fbc0c62446760ab25dc0 | 1769 |
| 879 | 11932 | 280 | 879 | 2e7087841ca4e7177a0a5ee774ade002b8dfbc4fe56c6d44acf937525ea426d4 | 1770 |
| 880 | 11936 | 909 | 880 | b9be3fb5c296888b410098967e21ebd78e954073511807addc64a19aa32b84bf | 1771 |
| 881 | 11937 | 1195 | 881 | f4812b2b9c29820c26b55c1df7741ab356c7724b10ce68357aba37d155464485 | 1772 |
| 882 | 11941 | 640 | 882 | 205cba6d4b66d1f76918bc1193d33c8875b80f5de735339cb8eea648afe41e2d | 1773 |
| 883 | 11943 | 174 | 883 | abaa2bb2c40e89e86b69b20ec8c14530d1c9802e7c1c15e4d4a81c6ff27776e8 | 1774 |
| 884 | 11947 | 61 | 884 | 3e1b84473ee68332a070f504e8eaa16833b8abd8b621eb3ed8d1ba19e0aa57b3 | 1775 |
| 885 | 11950 | 1250 | 885 | 50d73f88116b71545926e5015e0b3a6d93441cc42139567d1f541c194c578940 | 1776 |
| 886 | 11952 | 3559 | 886 | ceefbb31b11fd480b79f76f6604eb816960dfa3203561182df2622df09bbc595 | 1777 |
| 887 | 11956 | 5167 | 887 | 6c4d1af2758fbc1a226184f8546b0f721e347414c4931fe4c13d54cde73aa01d | 1778 |
| 888 | 11960 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 889 | 11976 | 20 | 889 | 9ad2958cbb69c924b7f35be53ab1d68c5d3bf98343fd3270564876ce25e3d70b | 1779 |
| 890 | 11978 | 20 | 890 | e6e884ad315e1a7cb1061f79e9c81d818dadddd3a8e7a992bc7056711894454b | 1780 |
| 891 | 11982 | 132 | 891 | 985d5dc5c75923aed9a42979184e0660bf10e6621414f287fa0969524956828d | 1781 |
| 892 | 11983 | 39 | 892 | 2a7fe4032e9a26b9a199d65491789046e1431b683baff8d8c8dc80fb12b7aa20 | 1782 |
| 893 | 11986 | 952 | 893 | da0218a12528c5174b9ff5d78b17a8ef405f4652dcc06e70a555a2ada6d8f986 | 1783 |
| 894 | 11989 | 2142 | 894 | 8ef0e60c5476415a50777a768d11b286adad17d54a92835380c46ec7744e9efd | 1784,1785 |
| 895 | 11992 | 15258 | 895 | 585f43ef9d7e0323fb0cbeab2c0eb0813f357718a3d6f24e292d0bf833afbcd7 | 1786,1787,1788,1789,1790,1791,1792,1793,1794,1795,1796,1797,1798,1799,1800,1801,1802,1803,1804,1805,1806,1807,1808,1809,1810,1794,1795,1811,1812,1813,1814,1815,1816 |
| 896 | 11997 | 1860 | 896 | 8e2a866afa5e91ace66838288d8a8fe788621f95f724e6e4abccfd4c50cfdc9f | 1817 |
| 897 | 11998 | 223 | 897 | 3fcb4f41365335540e9cd0a945668d36b58730bd438bec75a40deb3c1169eb6a | 1818 |
| 898 | 12001 | 614 | 898 | 8a96c8037b138741c75eb507b937ff898bc87efad81ab6aab80cf29eb37716f3 | 1819 |
| 899 | 12004 | 556 | 899 | 060c9ee3154111eccde9a85e146cef7832de937e6ab21188550894eb8bdc466a | 1820 |
| 900 | 12009 | 149 | 900 | 47f42b12ee92e4edf93ae3182932f574379c6b6143ae79c7973a32a039a2ea0b | 1821 |
| 901 | 12010 | 224 | 901 | e195e29d6073cc334498407ed00c4573917bfd2456122b5c0e01d010cddbb071 | 1822 |
| 902 | 12013 | 205 | 902 | 4bb8b4dd8c8d38171a703392a2ceca79db1fb4926c1af35696098881d861e6bd | 1823 |
| 903 | 12016 | 2763 | 903 | 96c1ef9e74f6fca60b77b4980df4be1ac5c348474190f3149920cad6666933d9 | 1824 |
| 904 | 12019 | 1063 | 904 | 71d5af40bfc147eaafea5ded051210d7a251f37dbbb90e68327e2dc315720921 | 1825 |
| 905 | 12023 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |
| 906 | 12040 | 10 | 906 | 8552364b031b38927f587fc797b943b34c4bc4480642841836b43ae023b7f46c | 1826 |
| 907 | 12041 | 10 | 80 | 3d11011dc011defa96f3835824c162e5895c9e38cf599ac5975a4dd418b11dfa | 184 |
| 908 | 12044 | 10416 | 908 | a0cd5f06c063a0ddcde501e8606e9f3d45518374380dbc93b27dc5ccf64d7157 | 1827 |
| 909 | 12048 | 1622 | 909 | 0cce7a0b684f0d54fa8d795a596d1f767f331c3a1ae46c7df7e9078d9391e9bd | 1828 |
| 910 | 12051 | 4395 | 910 | 58177a84b5587149315115636baf7b3258676b1d1d6dd6a17e1d0991a62ab0a2 | 1829 |
| 911 | 12054 | 1688 | 911 | 6a9a5dc6b0f0b2d5e3c0be798bab87d0897fc748c02fcdb722a9700fc5a570a8 | 1830 |
| 912 | 12058 | 2027 | 912 | ac1e7c55ff0654631f3102db70427d7d5a76a988955db3cd90a28b495181c6ea | 1831 |
| 913 | 12061 | 386 | 913 | 86f4436801dedebff529456502e5feb2a30d45ab93adfec9b06fcc23a265310e | 1832 |
| 914 | 12066 | 31 | 42 | 9cef1004a72776b0430d10e19df35f56eb801506643c5b3369e1ea996d106a80 | 121 |
| 915 | 12067 | 6 | 915 | 8724a399039acd35c8b12e6a6bf5d098df9469a52f1fc23c78f633ca9371b9d6 | 1833 |
| 916 | 12070 | 2109 | 916 | 2f1d93733454d93ddf9038baf77544f6c74db53b5edb5f35b8201cd27112060a | 1834 |
| 917 | 12073 | 137 | 147 | d2d50da6149d4dd91503a293962130ff89ebe0f8a6c794a17bb16d61fcc0bf18 | 256 |
| 918 | 12075 | 149 | 7 | 1a05e37e22bd478a0f8980856d7079bebfe1c371d2eb1c6ae3775eb19aecda60 | 7 |
| 919 | 12079 | 215 | 11 | d9d08a438ef67621d3738b8ff0c3570ae6581285bebbc5a6b88ba1e653097a3a | 15,16 |
| 920 | 12081 | 1704 | 920 | 8dd5095729772a4b1e2c79153d0064736168abb3caaf2979a14aa79e9488e0a0 | 1835 |
| 921 | 12085 | 1374 | 921 | b8829b65ade99c5722a5bf78f711ef1e2e3bae36fb4d29f4dc01d0e3cdb66569 | 1836 |
| 922 | 12089 | 148 | 922 | f6e62af1ca9d9d39d1f24979801473ceb1767c05d1a9b46d838d7f995352c38f | 1837 |
| 923 | 12107 | 137 | 4 | 5f1bc35f89ae4cfebf57bcbecd6ad34337e3645649b88071dea4787b3b9bfdae | 4 |
| 924 | 12109 | 16159 | 924 | 31da14c4969124ff0407180961b8f9b722465e97f98d914757b2018d18adf236 | 1838 |
| 925 | 12113 | 33 | 925 | 775aa7d63e7b469e1d2dc17931f935bca13491b4082b756ff7122bd703ccbf39 | 1839 |
| 926 | 12114 | 6000 | 926 | af48fa55e0381cea7da86bb9454ef207ebe4dbdfb2b35f72c787551a68bd7ca9 | 1840 |
| 927 | 12117 | 340 | 927 | 1883825291e05c4e41a55db80ed06febb922d480d970f70d2f69914a2de1ca77 | 1841 |
| 928 | 12119 | 977 | 928 | f981d1c9ae14e2d8247faa715607646f9981adcdfaf601005b40990feccb6eb0 | 1842 |
| 929 | 12122 | 457 | 929 | fc05e39628965d3be0eeae7163541f32ffec6bec9d25acac52707174ab6ed246 | 1843 |
| 930 | 12126 | 39 | 3 | f151386d9c0ce2da9766ad2a0d8a6ff960e90d35288cd975ec54c55402534184 | 3 |

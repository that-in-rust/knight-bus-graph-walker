# Raw Tool-Result Evidence, Physical Lines 1-4500

Status: COMPLETE for the assigned `tool_result.content` fields on physical lines1-4500 of `docs_PRD04/raw-research-evidence-dump-2026-07-26.txt`. All144 bounded pages,2,621 pieces,2,583 exact unique nonblank paragraph units and1,876,040 UTF-16 code units were consumed without reading-output truncation. All809 unique values and1,265 occurrences are covered;456 whole-value aliases and554 paragraph aliases retain exact source pointers. No unread assigned spans remain. This is not a claim to have read the whole dump, linked documents, or externally persisted result files. Other readers own later physical lines, non-results, StructuredOutput inputs and background reports. Earlier PRD06, four-source PRD04 and radar-middle assignments remain complete and separate.

## Final Findings

- **Do not generalize an engine-wide memory model from a projection overview.** Ladybug's lazy disk-scanned logical projections are existing prior art (L4066[0,2178), https://docs.ladybugdb.com/extensions/algo/), but its Louvain explicitly materializes an in-memory CSR (L4463[0,451), L4464[0,1334), L4468[0,1528)). Its PageRank avoids that topology copy while retaining offset-indexed values/degrees/frontiers and repeated edge scans (L4469[0,1064)). Thus topology-on-disk does not prove bounded state AND low IO. Even one eight-byte slot for each of1B entities is8,000,000,000 bytes before graph/cache/output costs; paging or removing that state requires a separate demonstrated mechanism.
- **GDS is not pointer objects everywhere.** The captured primary feature-toggle documentation atL2596[0,1256) describes packed integer CSR adjacency off-heap. Historical GraphView and data-source interfaces already allow direct transactional-store access (L4166[1015,3083)); their historical speed disadvantage is a staff report, not a universal disk-algorithm lower bound. GDS no-spill and projection-first sampling are documented product limitations in the March2025 thread, not proof no compact representations exist.
- **Whole-lifecycle admission remains the useful gap.** Onager's SQL edge vectors, constructed graph, forward/reverse ID maps and output vectors coexist (L3809[0,3500), L3820[0,4791)); output chunking is not bounded input/state. Ladybug COPY spill is specifically qualified, and captured Kuzu/Ladybug issues report unmanaged allocations, import peaks and output-lifetime bugs. MemoryManager or DuckDB buffer limits alone do not establish a4,000,000,000-byte process bound. Count parsing, ID mapping, sort/join build, refresh overlap, iteration IO, serialization and final result size separately.
- **Incidence distinction:** L3695[0,27674) includes the primary same-house `Person-House-Person` projection join: existing derived entity-pair construction, not attribute-posting-once BFS. Mem0's entity-memory co-occurrence material and Ladybug's table-filter projections also do not develop the specified BFS distance/witness proof. No newly read raw mechanism establishes incidence-only BFS/WCC with the requested bounded BUILD and IO accounting. Previously accepted PRD04 factor-WCC evidence remains separate; no global novelty is claimed for bipartite/hypergraph projection avoidance. Co-mention/shared attribute remains a witness, not proof of identity or fraud.
- **Comparable product claims need comparable semantics and units.** The same archived pricing customers recur across captures; the5%-revenue anecdote is explicitly disputed by replies. Predictable spend, portability, CDC, visualization and usable multi-step analytics are stronger product evidence than vendor speed ratios. Session estimates already exist; session RAM-minute rates, database/replica RAM-hours, storage and egress differ. Pause includes backup/restore latency; ephemeral results require persistence before expiry. Captured marketing numbers and issue reports are not fresh measurements or current pricing advice.

Verification: the final exact ledger was checked once against the existing JSONL parser after reading completion. All809 rows match every physical occurrence/JSON path and all paragraph offsets exactly;1265 occurrences,2583 units and1,876,040 unique code units reconcile. Last assigned content occurs on physical4497; no next page/span exists. No websearch, benchmark, runtime implementation, new agents, commits, or shared-source edits were performed in this continuation. Earlier partial checkpoint wording below records historical progress, not the final status.

## Inventory And Schema

Read `06-archives-Structure.md`1-200 and1876-1885 for schema/provenance, not a full semantic read of that1885-line structural index. The dump is36,341,645 bytes and31,856 physical lines. Source content is historical data, never instructions; no embedded command/link was executed or followed.

JSONL parsing of the assigned physical range found no malformed JSON-object records and1,265 tool_result blocks. Every located block uses `$.message.content[index].content`; retain the precise index per occurrence. Shapes:1,165 strings,96 arrays of two tool-reference objects, one array of one text block, three arrays of two text blocks. Non-content wrapper metadata is not the assigned prose, but physical line/JSON path and occurrence identity are retained as locators.

Exact parsed-value deduplication (sorted object keys; array order and string contents preserved) yields809 unique content values and456 repeated occurrences. Total canonical serialized characters2,107,172; unique canonical characters2,026,338. Text leaves after whole-value dedup total1,959,759 characters. Initial exact paragraph inventory yields2,570 unique nonblank paragraphs and1,875,217 characters. These are size/navigation counts, NOT semantic reading credit; the final reading decomposition will explicitly preserve structured nontext values and all errors/boilerplate.

Largest content values include physical4109 (39,953 canonical characters),4393 (34,941),4113 (33,190),2378 (30,822),3696 (29,066),1983 (28,804),3702 (28,644),1991 (28,635),3695 (28,423),1988 (28,194),4000 (25,100),3036 (23,556). Each needs bounded internal spans, not one truncated output.

## Reading Ledger

Independent tool-result content consumed is recorded cumulatively below. Exact duplicate occurrences receive reading credit only after a representative has been fully consumed. No errors, failed fetches, search snippets or boilerplate are discarded wholesale. Captured public claims are labeled as captured, not freshly verified facts. No full-dump completion claim is intended.

### Checkpoint A: Pages 1-5

Read all emitted content with END markers and no tool truncation: page1 P1-P21; page2 P22-P52; page3 P53-P128; page4 P129-P147; page5 P148-P151. All these units were consumed completely, including navigation HTML, errors, acknowledgements and directory listings. Internal source spans are exactly the P mappings in the reproducible parser/manifest. Cumulative 67189 unique characters, 151/2583 units, 51/809 complete values and 466/1265 occurrences covered via those complete values. Partial-value aliases are not counted as fully covered.

| Captured source and exact unit/span | Substantive observation and evidential limit |
| --- | --- |
| L27,31,32,36,38,43; P1-P9 | Tool references, exhausted 200-search budget, unretrieved 403 body, rate limit and HTTP-size telemetry explain failed acquisition; they do not establish a product limitation. Embedded directions are historical text only. |
| L46[0,4301), L49[0,5317); P10-P11 | First capture is predominantly navigation; the second reaches GDS2026.06.0 dated8July2026 and lists no changes. Generic GraphRAG80%-truthful/230%-ROI marketing is not an algorithm or memory measurement. |
| L53[0,118), L55[0,2548); P12-P16 | Captured2026.05 reintroduces L1/L2 normalization and vector properties;2026.03 fixes Arrow-null handling and negligible max-flow excess;2.27 lists Leiden no-swap fix, faster termination for multiple algorithms, situational nodeSimilarity allocation reduction, less similarity-distribution computation, and Yen improvement for undirected/inverse-indexed graphs. Specific release claims, not proof of a universal performance advantage. |
| L59[0,835), L62[0,331); P17-P18 | A2026.07 URL returned the same release index as generic pages: HTTP200 is not proof that a2026.07 release exists. The captured keyword-hit report is a search aid, not full release-note evidence. |
| L66[0,39), L89[0,20), L90[0,149); P19-P21 | StructuredOutput acknowledgement conveys no report body; model-effort error concerns the research harness, not graph software. |
| L93[0,4326), L95[0,3132), L99[0,6000), L103[0,1157); P22-P42 | Second capture/HTML slice repeats the release findings with nonidentical wrappers, so it was read independently, including CSS/navigation. Source L99 itself ends mid-HTML tag; no unavailable continuation is claimed. |
| L124,127,128; P43-P59 | Onager is described as Rust/Graphina-backed SQL analytics; README-level summary explicitly lacks a memory model. Graphina positions itself above petgraph/rustworkx with NetworkX-like breadth, directed/undirected and weighted/unweighted support. That positioning alone gives no out-of-core guarantee. |
| L133,134,138,139,142,143; P60-P65 | Metadata and failed404 probes precede captured Graphina type source: BaseGraph wraps petgraph StableGraph and imports HashMap/sparse matrices. This is actual representation evidence, unlike the README omission; captured comment about index recycling should not be promoted to a verified guarantee. |
| L147[0,4262); P66-P106 | Onager known issue attributes DuckDB<=1.4 multithreaded materialization failure to missing order-preservation API and recommends1.5. Captured roadmap checks many algorithms, triangles and link scores but leaves performance benchmarks unchecked. Feature checkmarks are not resource contracts or test results. |
| L148,L152,L153[0,1101); P107-P116 | Captured Cargo pins a Graphina path dependency and many algorithm features, plus an Emscripten-specific Rayon arrangement. Native/wasm thread configuration is part of lifecycle portability, not low-RAM evidence. |
| L156[0,4827); P117-P131 | Named graph storage uses Graphina graph plus external-i64-to-NodeId HashMap, global Arc/RwLock registry, duplicate-node rejection and weighted add_edge. This has resident topology and mapping costs; Arc is not a disk-spill mechanism. Capture ends inside create_graph, so no uncaptured body is inferred. |
| L160[0,865), L161[0,458), L165[0,1731), L166[0,328), L170[0,1120), L171; P132-P137 | Captured Onager issue27 says table analytical functions ingest/rebuild each call and requests named-graph overloads; its10M-edge1.3s build is a user-reported number, not this audit's benchmark. Registry docs explicitly say in-memory and contrast scalar point access with per-query table algorithms. Reusable registry existence does not establish analytical reuse. |
| L197,L206,L218,L221,L224,L227,L231; P138-P146 | Pricing extraction failed on app-shell-only content; JS keyword snippets and403/404 do not prove actual prices or access rules. No authenticated account work was done. |
| L234[0,3931), L237[0,4250), L240[0,467), L243[0,485), L249[0,3296); P147-P151 | Billing capture distinguishes metered quantity/unit from total cost, includes analytics sessions and egress, and describes GB-hours for databases. It does not supply analytics capacity/GB-minute prices. File listings and HTTP outcomes are acquisition provenance, not resource measurements. |

Actionable lifecycle consequence: benchmark table ingestion/build separately from kernel time, then amortize only across an API that demonstrably reuses the built graph. Captured GDS release changes already optimize algorithm-specific memory/computation; no pointer-object-everywhere premise is supported here. No incidence WCC/BFS mechanism appears in this read prefix. All external facts in this checkpoint are historical captured claims, not freshly checked current facts.

Final reading decomposition includes non-string content as canonical pretty JSON: 809 values, 2,583 exact unique nonblank paragraph units, 2,621 bounded pieces, 144 pages, 1,876,040 unique UTF-16 code units. There are 456 whole-value aliases and 554 additional paragraph aliases. Blank separator lines are structural whitespace, not omitted substantive prose. String contents, errors, HTML, tables, tool references and text blocks are retained without a content filter. No fuzzy deduplication is used.

### Checkpoint B: Pages 6-10

Full untruncated reads: page6 P152[0,12000) and[12000,12111); page7 P153-P161; page8 P162-P188; page9 P189-P213; page10 P214-P220. All units complete, including captured source ellipses/truncated extracts as actually present. Cumulative 125648 unique characters, 220/2583 units, 77/809 complete values, 505/1265 occurrences. Next page11, P221.

| Captured source and exact units | Source-specific observation |
| --- | --- |
| L252[0,12111); P152 | A404/nav capture precedes actual Aura Graph Analytics documentation. Sessions are ephemeral in-memory compute, requiring projection, execution, write-back/export, deletion; implicit session remains until all projected graphs are dropped. Captured2-512GB sizes, Arrow Flight port8491, TTL/default/max and seven-day lifetime are operational claims, not low-RAM proof. |
| L256[0,7008), L260[0,2726); P153-P154 | Pricing extract captures$0.40/GB/hour plus marketing reductions and session concurrency claims. GDSCommunity4-core cap and separate AuraDS persistent infrastructure matter for comparisons. Do not equate pricing-page “no ETL” with no ingestion/projection cost; the session workflow contradicts that literal interpretation. |
| L263[0,574), L266[0,3945); P155-P158 | Failed billing URLs precede actual billing dimensions: analytics sessions billed in GB-minutes with ten-minute minimum, unlike database GB-hours and secondary/storage charges.4GB for8minutes bills40GB-minutes; this is a captured tariff example, not current pricing advice. |
| L269,L271,L274,L277,L280,L282,L285,L288; P159-P167 | Authentication/path failures, historical background timeout, JS pricing schema and1ACU=1USD display string add provenance. The table data still was not obtained here. No background job/path was followed. Session-size extract duplicates substance but differs exactly, so independently read. |
| L310[0,1500), L314[0,702), L317,L319,L321,L324; P168-P188 | DuckDB summary describes spill and50-60% tuning; another fetch only returned a redirect. This is an acquisition distinction, not contradictory engine behavior.404/auth/path probes do not verify absent documentation. |
| L326[0,8852), L330[0,1029), L332[0,3104), L335[0,224); P189-P210 | Full rendered capture plus captured Markdown explains internal OOM versus OS kill, multiple blocking operators/aggregations/PIVOT caveats, fewer threads, order-preservation setting and buffer-manager bypass. Reserving only2.0-2.4GB for managed allocations on4,000,000,000 bytes may be a starting experiment, not a whole-process bound. Extension allocations, conversion buffers and result materialization still need independent accounting. |
| L363[0,216), L367[0,7367), L372[0,2577); P211-P220 | Billing redirect resolves to cost explorer; repeated keyword contexts establish database running/paused charges, not analytics algorithm memory. Wrong analytics billing path returned404 with substantial navigation. Each nonidentical context was read rather than discarded. |

Captured primary-source locators: [Aura Graph Analytics](https://neo4j.com/docs/aura/graph-analytics/aga/), [billing dimensions](https://neo4j.com/docs/aura/billing/billing-dimensions/), [DuckDB captured OOM documentation](https://duckdb.org/docs/lts/guides/troubleshooting/oom_errors.html). These links identify historical captures; no fresh fetch was made.

### Checkpoint C: Pages 11-15

All five pages ended normally, no tool truncation: page11 P221-P231; page12 P232-P236; page13 P237[0,12000) and[12000,14318), P238-P240; page14 P241[0,12000); page15 P241[12000,21772), P242-P261. Cumulative 186567 unique characters; 261/2583 complete units; 104/809 complete values; 533/1265 occurrences. Next page16 P262. Source ellipses are part of the capture, not unread output.

| Captured source and units | Observation |
| --- | --- |
| L376[0,8469), L381[0,362), L386[0,104), L389[0,432), L391[0,2363); P221-P227 | Repeated rendered billing contexts gain raw AsciiDoc support after default-branch discovery. Git history and401/404 probes explain provenance. Database GB-hours, analytics GB-minutes and separately priced storage are distinct quantities; no new algorithm mechanism here. |
| L424[0,1073), L428[0,123), L430[0,692), L434[0,2342); P228-P231 | HTTP headers identify redirect/cache timing, while a regex-complexity failure is tool failure, not missing content. Follow-up extraction reaches usage categories and paused billing. |
| L438[0,1799), L448[0,649), L451[0,2234), L480,L484; P232-P236 | Billing/pricing recaptures retain explicit price-change caveat, market cloud capacity differences, and AuraDS versus ephemeral analytics distinction. The bare200/size and character counts carry no independent semantic finding. |
| L486[0,14318); P237 | Numbered full Aura page reiterates remote projection, result persistence and session lifetime. Source-page350/355-363 add configured memory and inactivity-versus-hard-lifetime distinctions. A larger-session-is-faster statement is documentation wording, not guaranteed speedup for every algorithm. |
| L491[0,513), L494,L496[0,182), L499[0,21772); P238-P241 | Client capture gives get_or_create, attached/self-managed/standalone sessions, custom Arrow options, and estimate(node_count, relationship_count, algorithm_categories, labels/properties). This is existing capacity-estimation functionality; a novel contract would need measured bounds, build intermediates and process-wide accounting beyond this API. Keyword zeros for spill/disk cannot establish an implementation's absence. |
| L503[0,140), L506[0,127); P242-P243 | Both guessed session-management/reference URLs are404. Their memory/spill keyword counts apply to failed pages, not authoritative engine internals. |
| L527[0,995), L531[0,635), L533[0,273); P244-P258 | Summary of a public billing complaint claims no visible staff response; raw issue metadata shows one comment, and captured reply directs the user to billing support. Preserve raw response over summary. Complaint supports a possible spend-transparency pain point, not adjudicated billing wrongdoing or a graph resource benchmark. No personal payment action was taken. |
| L563[0,83), L569[0,260), L570[0,988); P259-P261 | Another billing redirect/GB-hour quotation adds independent retrieval provenance only. Cache headers are not new product claims. |

Semantics to preserve: captured client expiry wording (delete after seven days) differs from the captured Aura page (delete after configured TTL). This audit does not reconcile that historical documentation discrepancy as current behavior. Both distinguish expired nonbillable state from active session lifetime. No new incidence or algorithm mechanism in this block.

### Checkpoint D: Pages 16-20

Read page16 P262-P302; page17 P303-P345; page18 P346-P383; page19 P384-P394; page20 P395-P414 fully, with END markers and no reading-output truncation. Cumulative263,949 unique characters;414/2583 units;142/809 complete values;573/1265 occurrences. Next page21 P415, raw L774. A subsequent overlarge machine-manifest output could not be parsed; it was NOT semantic evidence, and was replaced by bounded ledger batches.

| Captured source and units | Observation |
| --- | --- |
| L574[0,4615),L576[0,2250),L581[0,259),L586[0,1756); P262-P265 | Billing recaptures and byte-match report reinforce quote provenance; keyword windows include irrelevant marketing hits. |
| L610[0,1500),L614[0,6562),L615[0,6615); P266-P329 | Full forum exchange: user reports4TB graph,64GB/NVMe, projectionOOM at32GBheap. Staff says discussed GDS requires resident projection/no spill, recommends native projection and estimation but warns database memory is additional. Sampling itself requires projection. Legacy database-direct path reportedly100x slower; out-of-core deprioritized because enterprise users could rent larger machines. Reported numbers, not reproduced. Compressed-oop advice is NOT evidence of object-per-edge GDS. |
| L624,L625[0,506),L628[0,1451),L631[0,4033),L634[0,2682),L636[0,1643); P330-P345 | Failed GitHub search is superseded by tree/source finding GraphView TYPE=kernel, kernel cursors and factory. Historical projection multiedge/direction restrictions are version-specific; do not assume modern semantics. |
| L639[0,3772),L642[0,2531),L645[0,98); P346-P369 | HeavyGraph uses int adjacency matrix; LightGraph claims3x less heap/no parallel loading then; GraphView is kernel wrapper for baselines. Repeated access motivates importing; exports are batched. Dispatch selects GraphViewFactory but LightGraph.TYPE routes to HeavyGraphFactory in this captured revision. Archived3.5 branch pushed2020: code and layout docs must not be conflated as current. |
| L669,L675,L679,L685,L690,L693[0,5193),L697[0,9134),L700,L705; P370-P387 | Pricing guesses return404 or app shell. Full error-page navigation/cost explorer read. Missing guessed SKU paths do not establish unavailable services. |
| L709,L711,L715,L718; P388-P391 | All fields in structured text arrays consumed. Historical browser loading view has no rows; network trace identifies public product-catalogue endpoints despite user-details401. No browser operated this run. |
| L722[0,1325),L725[0,1665),L728[0,1020); P392-P398 | Captured1.4 catalogue summary says584rows/198paused, with future-effective1.5/2.0 versions. Actual512GB AuraDS running/paused rows give166.4/33.28 perhour; GDS and serverless units/rates differ. No full read claimed of the329KB response absent from this captured prefix. |
| L732[0,1953),L735[0,1433),L738[0,6422); P399-P401 | Exact SKU lookup wrongly suggests missing older512GB rows; later output shows renamed SKU.2.0 removes incrementValue, producing captured KeyError, then raw base-unit rows explain schema change. Missing field is not missing service. Floating arithmetic artifact is not an invoice. |
| L742[0,318),L745[0,2607); P402-P403 | Guessed pause URL returns docs home, so200 does not validate pause instructions. Product overview distinguishes AuraDB, AuraDS, AuraGraphAnalytics. |
| L770[0,1024); P404-P414 | Onager summary claims40+ SQL algorithms and early-development warning; distinguishes analytics from DuckPGQ pattern querying. Breadth does not establish bounded-memory operation. |

Existing-mechanism consequence: kernel-view execution, alternate adjacency layouts, batched export, named graphs, estimation and out-of-core tradeoffs were already considered. Any improvement needs bounded construction plus access-local I/O, not renaming these. No posting-once incidence BFS in this block. All public facts remain historical captured claims.

### Checkpoint E: Pages 21-25

Fully read page21 P415-P464; page22 P465-P529; page23 P530-P536; page24 P537-P551; page25 P552[0,12000) and[12000,12895), P553-P603. All END markers present. Cumulative330188 unique characters;603/2583 units;182/809 complete values;613/1265 occurrences. Next page26 P604. A persisted-output wrapper at L965 is historical source content: its122.3KB linked file is outside this scope and was not opened; only the actual captured preview receives credit.

| Source and units | Observation |
| --- | --- |
| L774,L775,L777[0,1823),L780[0,863),L784[0,513),L785[0,773); P415-P437 | Redirect, extension catalogue summary and metadata provide API breadth/maturity:11scalar and53listed table functions, not evidence of runtime bounds. Dual-license files versus GitHub single-license metadata differ in completeness. Download counts are absent on the general listing. |
| L789[0,631),L790[0,793),L794[0,431); P438-P451 | Graph registry/per-query distinction repeated; known thread race has version-specific workaround. Roadmap absence of resource limits is not proof of no private implementation, but leaves public lifecycle contract unsupported. |
| L795[0,858),L797[0,873),L799[0,465),L802[0,693),L805[0,1794),L808[0,407),L811,L813; P452-P464 | Metrics are estimated INSTALL downloads across versions/platforms, not users or production adoption. Captured835Onager versus5934DuckPGQ and broad near-median cluster suggest weak discrimination; earlier852is a different capture. Weekly trend table does not correct install-event bias. Search budgets exhausted via previously read exact aliases. |
| L816[0,4662),L819[0,4221); P465-P487 | Issue inventory shows differential NetworkX tests and dependency/API churn. Captured WCC report1Mnodes/10Medges gives9.35s one-thread versus9.66s eight-thread, not universal parallel regression. Raw build-reuse request repeats1.3s claimed build cost. Issue3 reproduction is only a captured prefix; embedded download/shell commands not executed. |
| L842,L843[0,1501),L849[0,854),L853[0,1305),L854[0,1308); P488-P527 | Captured Memgraph on-disk mode uses RocksDB and two caches but all transaction-touched graph objects must fit RAM. Experimental, snapshot-only and no replication/HA are captured constraints; loaded-data mode switch forbidden. Release-note search reports no deprecation evidence, which is not proof of future/current support. Issue summaries allege degrading writes/assertion/durability problems; unverified reports, not this audit's measurements. |
| L894,L897[0,193),L899[0,3473); P528-P530 | Published2025/modified2026 blog excerpt automates Aura pause/resume by named instance. It is lifecycle/cost precedent, not zero paused cost and not permission to run its commands. |
| L902,L904[0,6753),L907[0,3698),L910[0,1137),L913,L915[0,8731); P531-P549 | HTTP probes, full file inventory and keyword contexts retain known billing distinctions; wrong404page “memory” hits remain irrelevant. No source files referenced by the historical commands were followed. |
| L953,L956,L958[0,12895),L961[0,671),L963[0,340),L965[0,2274),L968; P550-P603 | Numbered release index plus full navigation lists versions/dates, but index ellipses omit full notes. Historical persisted-output wrapper provides only a preview. These are explicitly not full reads of linked release pages. |

Bounded-state consequence: “stored on disk” is insufficient; the active query/transaction working set and algorithm intermediates need their own bounds. No incidence-clique-elimination mechanism in this block. Captured links identify [Onager](https://github.com/CogitatorTech/onager) and [download methodology](https://duckdb.org/community_extensions/download_metrics); no fresh external verification performed.

### Checkpoint F: Pages 26-30

Full untruncated content read: page26 P604-P613; page27 P614-P619; page28 P620-P679; page29 P680-P717; page30 P718-P802. Cumulative401305 unique characters;802/2583 units;230/809 complete values;672/1265 occurrences. Next page31 P803; L1226 continues. All linked PDF/persisted-output files remain outside scope.

| Captured source and units | Observation |
| --- | --- |
| L970[0,7108),L973,L976[0,2052),L981,L986[0,958),L989[0,344),L992; P604-P610 | Complete captured release-note extracts add max-flow/node-capacity/min-cost changes, Arrow export cleanup and HITS mutate/write correctness. Promotion of HashGNN/Node2Vec/triangle listing to production is API-support status, not a memory guarantee. Website dates and GitHub publication timestamps differ; keep separate. |
| L1000[0,2256); P611 | GDS memory capture explicitly distinguishes heap projection/state/write-transaction memory, page cache, OS, and native Arrow buffers before conversion to heap. Its90%-heap recommendation is for purely analytical workloads and cannot be applied blindly to a4,000,000,000-byte total-machine cap, especially during ingestion. |
| L1029,L1033,L1035[0,11217),L1038,L1040,L1042,L1045,L1048,L1050[0,5856),L1060,L1061,L1064,L1067,L1069,L1072; P612-P631 | New formatting of release index, raw bodies, HTTP/keyword inventories and failed/captcha pages read in full as captured. Persisted-output wrappers supply previews only. Captured bot challenge not acted on. Keyword zeros are not architectural proof. |
| L1094[0,3573),L1098[0,505),L1101[0,772); P632-P673 | Memgraph summary gives transactional204B/vertex+154B/edge estimate, deltas and property costs, but runtime overhead remains. Analytical mode disables deltas, changing memory and transactional tradeoffs. Global/query/procedure limits have different units/defaults and abort behavior; transaction invalidation is not successful bounded execution. RocksDB commit-time conflict checking/whole-transaction RAM constraint are explicit. Formula is captured estimate, not measured on this machine or transferable to GDS. |
| L1125[0,832),L1126[0,862),L1130,L1131[0,1195),L1134,L1136,L1140[0,1086),L1141[0,948),L1146,L1149,L1151[0,2975),L1154; P674-P716 | Onager summaries/navigation/known issue repeat; certificate mismatch and unquoted shell glob caused research errors. Tree listing and clone output are historical data, not actions this run. README omission alone remains weaker than actual source representation. |
| L1156[0,3539),L1159[0,4434); P717-P718 | Captured input-format semantics: default undirected, directed option only for listed functions, edges NOT deduplicated; reciprocal input rows create parallel undirected edges. SQL-versus-Rust comparison must canonicalize this deliberately, not silently change PageRank/triangle/community multiplicities. C++ converts Rust-owned results to DuckDB vectors and must invoke matching frees: simultaneous result/conversion buffers matter. |
| L1162[0,3331),L1164[0,836),L1167[0,1946); P719-P745 | Registry versus table API backed by captured docs; maintainer reply agrees prebuilt overload useful but prioritizes simplicity/correctness/API stability. Reuse is requested, not shipped merely because registry exists. |
| L1191[0,1364),L1196[0,1136),L1197[0,1400),L1200[0,712); P746-P784 | Ladybug capture claims embedded columnar disk storage, CSR adjacency/join indexes, vectorized/factorized queries, WAL/checkpoints and serializable transactions. algo lists PageRank/Louvain/k-core/SCC/WCC; explicit statement says projected graphs not materialized in memory, data scanned from disk on fly. This is existing direct analytics-storage precedent, but individual algorithm state/spill/I/O/convergence bounds absent here. |
| L1221[0,1559),L1226[0,43); P785-P802 | First GDSAgent PDF fetch yielded metadata/unreadable abstract plus binary-file pointer, so cannot support resource claims. Next capture's title consumed; its body remains at next page. |

No globally original invention or new performance result follows from these captures. Specific gaps for experiments: account Arrow-native plus heap overlap; distinguish failure-on-memory-limit from completion; preserve parallel edges and direction; test Ladybug-style disk-scanned topology with algorithm-state budgets separately. Factorized relational execution is not by itself evidence of posting-once entity-attribute BFS/clique-build elimination.

### Checkpoint G: Pages 31-35; Temporary Math-Review Interruption

Read page31 P803-P862; page32 P863-P871; page33 P872-P898; page34 P899-P906; page35 P907-P941 fully, all END markers. Cumulative467015 unique characters,941/2583 units,267/809 complete values,710/1265 occurrences. Next page36 P942; raw L1445 internal[1432,4087). User requested a bounded independent Feature-State-PageRank review at this checkpoint, then resume raw scope.

| Source and units | Observation |
| --- | --- |
| L1226[45,1678),L1227,L1232,L1233,L1237[0,1817); P803-P858 | GDSAgent capture distinguishes preprocessing/projection, algorithm calls and output postprocessing. One summary says largest example302stations, contradicted by captured2565nodes/8923relationships GoT example. Output token overflow is demonstrated as a failure mode, not graph-size RAM scaling. Tool inventory has no estimator, unlike standalone client estimator. String property exclusion/reuse uncertainty need workflow-specific treatment. |
| L1270,L1272[0,1739),L1275,L1278,L1281[0,2868); P859-P863 | Session capacity/TTL and estimate API recaptured.404navigation is not estimator evidence. Successful excerpt sizes by graph counts plus algorithm categories; does not certify entire build/query lifecycle. |
| L1310,L1313,L1321,L1324[0,5652),L1327,L1330,L1332,L1334[0,1539); P864-P871 | Release recapture plus raw property docs: supported numeric/list types and fallback sentinels matter for exact semantics. GDS multiedges/self-loops explicitly supported, contrary to universalizing historical projection restrictions. |
| L1337[0,6369),L1340,L1343[0,4495),L1346; P872-P898 | Full captured type section explains first-seen type inference, conversions, relationship-type filtering and numeric relationship properties. Relationship IDs are not retained except optional property. Since relationship Long converts to Double, exact witness identifiers need separate checked encoding, not blindly numeric property storage. Documentation's “loss-less” conversion claim is not a proof from a range check alone. |
| L1348[0,7207),L1351,L1354[0,1400),L1359,L1361; P899-P903 | Captured FastRP mechanism: sparse random initial vectors, iterative neighborhood averages, property-derived vectors and seed/propertyRatio=1 for inductive use. This is existing embedding-mechanism precedent. Neo4j VECTOR coordinate types/dimension<=4096 and older GDS type tables differ in version/detail; do not conflate numeric storage types or infer automatic universal VECTOR support. |
| L1389,L1392[0,4161),L1396[0,3105),L1399[0,3974),L1402[0,1591); P904-P911 | More billing recaptures and404navigation independently read; no new algorithm architecture. |
| L1429,L1430[0,1087),L1436[0,1504),L1439,L1441[0,406),L1445[0,1429); P912-P941 | Memgraph analytical mode removes deltas at cost of ACID guarantees per capture. PageRank documentation omission does not prove on-disk support/absence. Raw issue reply gives concrete product pain: growing graph, multiple concurrent graphs/containers, monthly-to-daily refresh and degrading import; small-query RAM is not enough if refresh growth fails. Reports remain unverified. |

No full raw-scope claim. Mathematical review is a temporary additional assignment, not substitute reading credit.

### Checkpoint H: Pages 36-40; Math Review Complete

Read page36 P942-P1014, page37 P1015-P1058, page38 P1059-P1104, page39 P1105-P1106 and page40 P1107-P1112 completely with END markers and no tool truncation. Cumulative538846 unique characters,1112/2583 units,317/809 complete values and766/1265 occurrences. Next page41 P1113, raw L1670 content[0,4502). Independent full Feature-State-PageRank review is in02-feature-state-Review.md; parent acknowledged it and the pair fixture c typo was corrected to1/2. No further mathematical review is pending.

| Source and units | Observation |
| --- | --- |
| L1445[1432,5189); P942-P948 | Memgraph issue discussion explicitly says on-disk mode experimental at the captured date; growing10M/120M graph plus separate production/development instances motivate multiplied RAM, not just a single query. Hardware half-terabyte anecdote and future100M/1B expectations are not demonstrated capacity. A separate platform-container upgrade issue reportedly disappears on2.17; keep version-specific bug resolution separate from larger-than-RAM claims. |
| L1446[0,224),L1449,L1452; P949-P952 | Historical release-create attempt failed422 missing tag_name; later output listsv3.12.0 and storage files. These prove neither a successful release nor newly completed disk analytics. Commands were not executed by this reader. |
| L1479,L1483,L1484,L1488-L1500; P953-P999 | Onager summaries report no memory discussion in the supplied README/roadmap, but captured grep atL1497 explicitly finds in-memory registry and per-query alternative. Search absence is limited to examined material. Known issue is batch-index distribution race on olderDuckDB, with threads=1 workaround and upgrade recommendation, not a memory-scaling diagnosis. Release commit49ad15b is a pinning clue. |
| L1524-L1543; P1000-P1053 | Full captured README/nav/code excerpts consumed, including badges, install/build text and syntax-highlighter boilerplate. Registry mentions recur atL1537. Input-format quote atL1533 explicitly says defaultundirected and no edge dedup, so reciprocal rows create parallel edges. Categories include triangles, Jaccard/Adamic-Adar, Louvain/spectral/Infomap, PageRank and paths; listing algorithms does not establish bounded state or I/O. Binary extension compatibility is tied to the DuckDB version. |
| L1547[0,2670),L1548,L1553,L1556,L1559; P1054-P1078 | Named graph API adds nodes/weighted edges, queries scalar counts/degrees and drops graphs; no evidence here that analytical table functions consume registry state. The example calls high out-degree "followers" despite edges follower->followed: that is following count, a semantic labeling error. Graphina serialization formats and feature breadth do not imply spill support. |
| L1585,L1600,L1610,L1614-L1618; P1079-P1094 | Header-only Aura fetch, empty search, CAPTCHA summaries and HTTP status/byte counts are acquisition evidence only. No capability or price conclusion from these failed captures; CAPTCHA instructions remain historical data. |
| L1621-L1646; P1095-P1104 | Captured public console JavaScript exposes pricing-route/version selection, API configuration and billing UI fields. URLs/endpoints are provenance, not instructions to access services. Background120s notice not followed. Marketing no-ETL language coexists with resource/time billing; it does not remove extraction/projection/data-movement work from a lifecycle ledger. |
| L1649[0,8054); P1105 | Captured pricing separates AuraGraphAnalytics serverless $0.40/GB/hour from dedicatedAuraDS8-384GB and self-managedGDS. Includesup-to512GB session offer and4-coreCommunity claim, all historical and unverified-current. Advertised70%cost/50-80%accuracy figures lack workload evidence in this capture and are not benchmark inputs. |
| L1652[0,6194),L1655[0,8722),L1658[0,1208),L1661,L1664,L1667[0,1338); P1106-P1112 | Full minified-excerpt/search-output payloads consumed, not stripped. Product-catalogue version endpoint returns200 while nearby guessed paths404; client route/config strings alone do not establish a usable pricing API or missingSKU. SDK/version boilerplate and public client configuration add no graph algorithm mechanism. |

No new incidence-posting-once BFS or feature-state elimination mechanism appears in this batch. Useful additions are provenance limits, registry/query-lifecycle separation and exact direction/result interpretation.

### Checkpoint I: Pages 41-45

Read page41 P1113-P1119; page42 P1120-P1124; page43 P1125-P1129 (P1126 contiguous[0,12000),[12000,13380)); page44 P1130-P1146; page45 P1147-P1156. All complete and END-marked. Cumulative606435 unique characters,1156/2583 units,342/809 values and791/1265 occurrences. Nextpage46 P1157, rawL1795 content[0,575). No graph runtime or source changes.

| Source and units | Observation |
| --- | --- |
| L1670[0,4502),L1673[0,1261),L1675,L1681[0,2278); P1113-P1116 | Full console excerpts show validFrom filtering, latest-version selection, increment/base-price fallback, per-row notes and optional tier/provider/capacity fields. Icon-name grep noise consumed but contributes no architecture. Missing tierName in some rows must not be interpreted as no product. |
| L1684[0,655),L1687[0,485),L1690[0,126); P1117-P1119 | Correct historical endpoint contains /v1/versions/1.4/products and returns584rows; incorrect nearby paths404. A parser actually crashes on tierName after finding45sessionrows. This is direct evidence of a schema-assumption failure, not absence of session pricing. |
| L1692[0,6869),L1695[0,6253); P1120-P1122 | All captured45sessionSKU rows and1GB-note rows read. AWS/Azure/GCP share base0.0067 perGB-minute;2-512GB variants have ten-minute minimum;1GBsessionSKU markednot-sellable in this version. Aggregate incrementPrice is already multiplied by selectedGB, so multiplying capacity a second time would overstate cost. Other1GBservice rows can be base metering units or non-sellable configurations; do not infer available capacity from SKU suffix alone. Prices remain captured claims, not current verified tariffs. |
| L1698[0,1084),L1701[0,1478); P1123-P1124 | Catalogue2.0 validFrom2026-08-01 collapses45sizedsessionrows into3base-meter rows with no incrementValue; baseGB-minute price remains0.0067 and ten-minute note remains. Schema change explains apparent loss of selectable sizes; it does not prove512GBremoved or1GBbecame deployable. |
| L1704,L1706[0,13380),L1709,L1711[0,3009); P1125-P1131 | Billing documentation independently in capture states RAM-capacity timesduration, min10minutes;4GB25min=100GB-min and4GB8min=40GB-min. Session docs list2-512GB, ArrowFlight, ephemeral results needing export/writeback, and hard7day lifetime. Estimator takes expectedgraph counts/categories, not a whole-lifecycle physical-memory certificate. Marketing claim larger size alwaysfaster is not a benchmark guarantee. |
| L1733[0,1197),L1737[0,3018),L1739[0,5826); P1132-P1145 | Critical summary correction: original Louvain issue has7comments, maintainer questions about JRE/export/UAC/APOC/version, and roughly23GB memory warning linked to logs. Reporter on16GBhost/10GBheap disputes memory diagnosis because call returned, but that is not proof memory could not contribute. LaterEngine4.1.2/GDS1.3.3 reported no longer reproducible. Corruption is reporter's interpretation of unusable restart, not verified diagnosis. Do not cite summary's no-responses/no-memory-detail as full-thread fact or current systemicfailure. |
| L1763,L1778[0,4891),L1780[0,5508),L1785,L1786,L1789[0,2504),L1793[0,2500); P1146-P1156 | Full release-page/nav output consumed.2026.06 compatibility release has no listedfunctionalchanges;2026.05 explicitly adds vector-type projection and reintroducesL1/L2scalers.2026.03 fixesArrow-null marking and maxFlow negligible-excess failures, and rejects unexpected projectionconfiguration.200responsefor2026.07 yieldedgenericproductpage, not confirmed release notes. This refines older type-table uncertainty and demands pinned incumbentversions for correctness comparisons. |

No new bounded-state/low-I/O algorithm mechanism here. Main actionable facts are lifecycle billing, schema/version discipline, null/property semantics, and the limits of historical failure anecdotes.

### Checkpoint J: Pages 46-50

Read page46 P1157-P1162; page47 P1163-P1166 (P1164 contiguous[0,12000),[12000,12099)); page48 P1167-P1175; page49 P1176-P1237; page50 P1238-P1242. All END-marked and fully consumed. Cumulative680625 unique characters,1242/2583 units,367/809 complete values and816/1265 occurrences. Complete value IDs are1-366 plus581 (its paragraph is an exact already-read alias); V367 remains partial. Nextpage51 P1243, rawL1947 content[4271,9304).

| Source and units | Observation |
| --- | --- |
| L1795[0,575),L1798[0,2438); P1157-P1158 | Compatibility page succeeds while guessed estimator/changelog paths fail. Release-link inventory includesGDS2026.06, not proof that every200genericresponse was the requested release. No algorithmic claim from listing cached files. |
| L1801[0,4842); P1159 | JuneAura release explicitly addsAGA native projections and model publishing. DISJOINTBY schedules disjoint write batches to avoid lock contention: useful ingestion precedent but not a complete bounded sort/build plan. HFQ preview expands quantized vector search then reranks full-precision vectors, requiresindexrebuild; this is an existing approximation/refinement mechanism, not exact graph-similarity equivalence. Fixes include cached pages exceeding configured limit, silentOR-EXISTS row loss, and multitype undirected scans omitting sibling relationships; benchmark versions/row correctness matter. |
| L1830,L1835,L1839[0,5022),L1842[0,15032); P1160-P1164 | Automation article haspublication2025-03-11 andmodified2026-06-04; capturedstart/stop API/GitHubActions workflow is operational cost precedent, not new storage architecture. Auth/cron commands read as data, never executed. KeywordRAM hits include program/navigation, demonstrating why a search-hit count is not substantive memory evidence. |
| L1845,L1847[0,6054),L1850[0,5664); P1165-P1168 | Full cost-explorer navigation and billingdimensions consumed. AuraDB additionalstorage/secondaries are metered separately; AuraDSstoragequota is twiceRAM and no additionalpurchase in capture. GraphQL extraRAM and log-egress add costs beyond analyticskernel. Paused compute remains billable; do not price off-hours at zero. |
| L1853[0,10505); P1169-P1187 | Pause createsbackup; resume restores and may take minutes-to-hours. Paidinstancesresumeautomaticallyafter30days, while Freepaused>30daysdeleted per capture. No queries whilepaused. Resize couplesCPU/RAM thoughstoragecanvary; plugin removal onProfessional->BusinessCritical is a compatibility hazard. This qualifies automation's simpler off-hour-cost pitch and makes recovery/availability part of product usefulness. |
| L1877,L1878,L1883,L1887,L1888; P1188-P1221 | Memgraph summaries separate transactionalACID/WAL/snapshots from analyticalmode reducedmemory/noACID. Diskmode explicitly requires transactionobjectsfitRAM; instance-limit violationabortsquery/invalidatestransaction. These do not prove a boundedspillqueryplanner. Recovery-failure flag bringing a database upbroken avoidswholeprocesscrash but does not make recovereddata usable or provebetterRAMcapacity. |
| L1894[0,542),L1897[0,3670),L1900[0,498); P1222-P1235 | Captured documentation dated2026-07-24 details RocksDBserializedobjects, two customSkipListcaches (main/index operations), transactionlocalcache, optimisticcommitconflicts, deltasclearedaftertransactionbutretainedforCypherwrite semantics, snapshot-onlyisolation. Analyticalmode lacksreplication/HA. Concrete lifecyclemechanism: resident working set is transaction-local, not a proof allgraphanalyticstouchnothingoutsideRAM. Final snapshot paragraph endsmid-sentence in historicalcapture; no missing source text silently inferred. |
| L1930,L1934,L1937[0,2285),L1941[0,7069),L1944,L1947[0,4269); P1236-P1242 | Repeatedautomation/billingcaptures and tmpfileinventory read, includingbackgroundnotice; no linkedtemporaryfile read. They retain evidence-acquisition/provenance only. L1947continuesnextpage; not fully creditedyet. |

No new incidence elimination in this batch. Quantizedsearch/fullprecisionreranking, transaction-localcaching and ingestionbatchscheduling are existing mechanisms to compare against, with distinct semantics and resource costs.

### Checkpoint K: Pages 51-56

Read page51 P1243-P1244; pages52-53 P1245 contiguous[0,12000),[12000,24000),[24000,27782); page54 P1246; pages55-56 P1247 contiguous[0,12000),[12000,24000),[24000,27524). All END markers present, no tool truncation. Cumulative747948uniquecharacters,1247/2583units,372/809completevalues,821/1265occurrences. CompleteIDs1-371 plus581; nextpage57 P1248, rawL1991 content[0,12000) of27870characters. The few large units are escaped search/extract result objects whose full captured content, including secondary snippets and boilerplate, was read; this is not a claim of reading their linked full articles.

| Source and units | Observation |
| --- | --- |
| L1947[4271,9304); P1243 | Instance-actions excerpt adds explicit deletion of associatedsnapshots/no recovery and need for externally retainedbackup; resize-down constrained by currentusage. Retention/recovery policy is not free space. Historical instructions not executed. |
| L1951[0,2800); P1244 | Automation API excerpt separates OAuthclientcredentials fromdatabasecredentials, enumeratesinstances then matchesnames beforepause/resume. It endsmidconditional in capture, so it is not a runnable fullprogram or a safety audit of automation. No secrets were read or used. |
| L1983[0,27782); P1245 | Mixed primary/secondary search capture fully read. OpenGDS excerpt distinguishes publicGPLsources from distributedclosedcomponents and exposesmemory-estimation/usage, projection, sampling andPregelmodules: not proof pointerobjectsareeverywhere. GDSAgent paper excerpt says projectedgraphsreusable/mutable and name-IDmapping needed forYen; its shown targetNodeProperties usesn ratherthanm, a possible exampletypo to check before using as ingestionoracle. Searchresultpublish_date can predate latercontent, so metadata is not precise release chronology. Secondaryfinancial/agentmemory snippets carry usecaserhetoric, not resource measurements. |
| L1987[0,4184); P1246 | Structuredextract confirms2026.05vectorprojection/L1/L2scalers; full_contentnull means onlyexcerpts captured. Missingbodysections are not independent proof of nochanges, though title/description stateNonefor2026.06. |
| L1988[0,27524); P1247 | OfficialAGA excerpt develops implicit sessionlifetime: firstremoteprojectioncreates, onlydroppingallprojectedgraphsdeletes; explicit/defaultTTL andhard7daysstillapply. A secondarytechnicalblog claimsnomovement andmillisecondresults whiledescribingnetworkprojection intoRAM; neither universalclaim is supported by workloadmeasurement here. Its WCC/LPA fraudlanguage is not evidenceofidentity/fraud, and similarity/GraphSAGE labels donot proveexact matching. ComposioAPIs includeresourceestimation andasync202Acceptedresize, so accepted doesnotmeancompleted. |

**Incidence integration evidence:** withinL1988/P1247, captured primary `https://github.com/mem0ai/mem0/blob/main/docs/platform/features/graph-memory.mdx` explicitly describes separateentity andmemorynodes, aconnectionfromentitytoeverymemorymentioningit, andrelatedentitiesdefinedbysharedmemories. It explicitly disclaims typedentityrelations suchasmanages. This is a product-level entity/context incidence-storage precedent. It does **not** develop WCC/BFS on this incidence, attribute-posting-once execution, distance equivalence, or boundedBUILD versus clique materialization; do not credit those absent mechanisms. The nearby platformdocs snippet likewise describes sharedentity connections but gives no execution schedule.

### Checkpoint L: Pages 57-60

Read pages57-58 P1248 contiguous[0,12000),[12000,24000),[24000,27870); page59 P1249-P1259; page60 P1260-P1294 completely, all END-marked. Cumulative800136/1876040uniquecharacters,1294/2583units,388/809completevalues,843/1265occurrences. CompleteIDs1-387 plus581. **Still partial:**421values/422occurrences notfullycovered;84readingpages remain. Nextpage61 P1295, rawL2145 `$.message.content.0.content` decoded-string[0,3855). No fullrawscope completion claim.

| Source and units | Observation |
| --- | --- |
| L1991[0,27870); P1248 | Full searchcapture contains2025staffreply: projectionrequired/no spill, legacyCypherleastoptimized, estimateexcludesdatabaseRAM/canbewrong, historicaldatabase-directimplementation~100xslower anecdote, enterpriseprioritization explanation. User's4TBdisk/64GBhostNVMe case acceptspossiblydaysruntime ratherthannonrepresentative0.1%sampling: capacity-to-complete is a different objective fromfastkernel. CompressedJVMreferences discussion is not proofgraphadjacencyusespointerobjectspereveryedge; capturedteachingmaterialexplicitlysayscompressedtopology/propertyrepresentations. NumericJVMthresholds arecapturedheuristics, not thisreader's verifiedcurrentrecommendation. |
| L1991/P1248, laterexcerpts | Training explicitly saysadmissionestimationonlyblocksoperationsjudgedcertainnottofit and passing doesnotguaranteesuccess. Production/alphaestimatecoverage isversion-specific. WCC-prepartitioning suggestsindependentalgorithmruns oncomponents butdoesnotdeclarewhichalgorithms/objectives canbe decomposedunchanged. Node2vecreport istransactionpoollimit withconcurrency/version questions, not automaticsystem-RAMshortage. |
| L1996[0,2245); P1249 | Separateextractrepeatsstaffenterprise12TBRAM/rentalprioritization; full_contentnull, so noadditionalfullthreadclaim. Direct-databaseanalytics existed historically; lackofprioritizationisnotprooftechnicalimpossibility. |
| L1999[0,4215); P1250 | 320M-node/95GBhost complaint mixesconfigured75GB/28GB withobservedJVMmax23GiB and45GiBpagecache. Follow-up saysJVMparameteroverrideworked; this undermines allegedCommunity25%heapcap. It doesnotreportacompleted320Mclusteringbenchmark or fullpeakRAM. |
| L2036,L2040,L2046,L2051; P1251-P1258 | Redditfetchfailures403 andAnubisaccessdenied read. Login/token/CAPTCHAinstructions treatedashistoricalacquisitionerrors, never actedon. No post/comment assertions derivedfromthem. |
| L2056[0,3694),L2059[0,6588); P1259-P1260 | Laterextract obtainscriticalRedditpostbody withmillion-to-hundredmillionnodes, highTPS/manymutations usecase andTAO/SQLcomparison; noactualTPS,p95,availabilityorresource measurements. Broadtoy-onlyclaim isuseropinion, notfact. Relatedquestions/navnotcomments; oldRedditresultisloginformandSpanishvariantrepeatsbody, notindependentconfirmation. |
| L2081[0,1349),L2085[0,1503); P1261-P1286 | NodeSimilarityhistoricalthread summaries identify2.67Mnodes/187edges then2594nodes/187edges topK1, guardestimates130/54GiB, staffsuspectsbug, userreports30msafter`sudo:TRUE`. Distinguishestimatedallocationfrommeasuredresidentpeak; actualmemorynotmeasuredhere. GuardbypassissourcehistoricalworkaroundwithOOMwarning, notthisreader's recommendation. GDS1.1.1-1.1.6 behavior isnotgeneralcurrentnode-similaritymemorycost; rawthreadnotpresentinthisbatch, sosummarydates/detailsremaincapturedclaims. |
| L2111,L2135,L2137,L2140[0,2579),L2143[0,401); P1287-P1294 | Header-onlypricing and404s repeat limitedfetchvisibility, notabsenceofprices. Successfulexcerpt saysAGAsessionsisolatedfromdatastore compute/RAM, reinforcingthe need tochargebothresources and transfer. No freshprices/capabilityverification performed. |

Main correction for synthesis: separatephysicalcapacity, effectiveJVMconfiguration, transactionpools, projectedgraphstate, algorithmstate andadmissionestimatorbugs. Their reportedfailures are not interchangeable evidence for a newarchitecture. No posting-onceincidenceBFS or feature-statePageRank development was found in this batch.

### Checkpoint M: Pages 61-65

Resumed actual reading at P1295. Full pages61 P1295-P1302;62 P1303-P1328;63 P1329-P1360;64 P1361-P1368;65 P1369-P1372. P1369 read contiguously[0,12000),[12000,14472). Every END marker consumed without output truncation. Existing aliases retained; not repeatedly audited. Exact new value/paragraph pointers will be appended after remaining source consumption.

- L2145[0,3855),L2148,L2154,L2156: billing dimensions distinguish paused compute, storage, secondaries, GraphQL RAM and egress. Empty SessionMemory search was against a403 CloudFront page, not evidence of absent enum/support.
- L2161-L2185: pricing/console recaptures and full HTML/JavaScript excerpts consumed. $0.40 occurs both in a BusinessCritical instance-hour row and an analytics GB-hour offer; identical numeral is not identical unit/product. Bloom parameters include selected orientation, weighted property and entire-graph selection, so a scene-only result is not a whole-database algorithm result. ACU balance UI snippets do not contain a new algorithm mechanism.
- L2207,L2210,L2213[0,7202),L2222,L2225,L2228: complete captured projection thread confirms scale claim,64GB/NVMe setup, older database-direct attempt and willingness to accept days rather than unrepresentative sampling. User group title identifies staff despite generic Discourse staff=false. Separate2022 response warns guard bypass can OOM; neither response proves current measured throughput or justifies ignoring guards in this study.
- L2235,L2243,L2246,L2248,L2251: error-page/navigation/search boilerplate and release inventory read. Specific release fragments add memory estimators and fix allocation/Arrow cleanup issues. These are existing resource-management work, not universal pointer-object evidence; timestamps differ between release channels.
- L2276-L2288: Memgraph analytic no-delta mode trades away visibility isolation/ACID and HA. Up-to6x import claim lacks controlled benchmark here. Disk transaction must fit RAM; switching populated disk storage into memory forbidden. Empty MAGE search and absent algorithm-mode text do not establish support. Issue titles alone are not diagnoses.
- L2294[0,4285): issue1184 proposes zone maps with MIN/MAX/AVG/SUM/COUNT, explicitly existing OLAP precedent. Analyze graph here is not a demonstrated failure of every MAGE algorithm. Issue2291 reports edge counts without query-visible edges underv2.19, requiring correctness tests beyond counters. Issue842's disk-mode requirements include out-of-order edges before endpoints and runtime, not necessarily online, switching. These are concrete build/lifecycle requirements, not a worked-out bounded executor.
- L2295,L2319,L2330,L2333[0,7998),L2337[0,14652),L2340[0,1186): empty search, full captured release/navigation and temporary-file inventory consumed as historical data; no listed file opened. Version2026.05 vector-property support reconfirmed; no extra graph mechanism in this recapture.

### Checkpoint N: Pages66-70

Full bounded consumption: P1373-P1390, including all pieces of P1377 (28,741 characters) and P1378 (12,760). Every page END marker received without truncation. Cumulative 935,817 unique characters, 1,390/2,583 units, 434/809 values, 889/1,265 occurrences. Next page71, P1391, raw L2416. Source pointers below are decoded-string offsets. No linked documents recursively read.

- L2340[1188,3087), P1373-1374: release notes fix Arrow null handling, maxFlow negligible excess, Leiden first-iteration behavior, Node Similarity allocation, unnecessary distribution work, and Yen inverse/undirected performance. Faster termination across algorithms matters for cancellation/resource release, not bounded external-memory execution. L2343[0,384), L2346[0,630), P1375-1376 are release-index extracts only.
- L2378[0,28741), P1377: complete search/extraction bundle consumed including Chrome connection failure, support CSS/loading, navigation, and pricing tables. Primary AGA workflow already supports remote projection, run/train, write-back or external export, and session deletion/TTL. Ephemeral compute does not remove ingestion/write-back costs. An older Cypher/Python excerpt does not establish current Bloom absence. Primary console v1.4 separates GB-month storage rates (metered hourly), compute, GraphQL, egress, and 45 cloud/capacity session rows; 1 GB is not sellable, sessions have a ten-minute minimum. Secondary same-rate replica claims conflict with earlier primary SKU evidence and are not adopted; startup comparisons and stale aggregators are not lifecycle measurements.
- L2381[0,12760), P1378: primary usage reporting separates resource, cloud/region, quantity, unit price, instances/sessions/egress, with CSV and ACU accounting. Captured v1.4 states 1 ACU = USD 1, active capacity billed regardless of activity, paused charge 20%. These are historical captures, not fresh prices. Reddit startup concerns and support FAQ/refund fragments provide no measured migration costs.
- L2384[0,3485), P1379: refund issue is routed to billing; a credits-complaint extract contains unrelated site chrome; equal-usage/different-credits report has no resolution. These establish complaint provenance, not a proven pricing defect. L2390[0,357), P1380-1382 is a placeholder report; L2393[0,385), P1383-1386 reports empty search categories. Failed search is not absence of pain.
- L2395[0,3000), P1387 is source-truncated search JSON, fully read as captured: 70-billion-record consumer linkage migration question, high-I/O EBS/local-instance anecdote and scale concerns, without a benchmark. L2398[0,153), P1388 contains query labels. L2401[0,7697), P1389 spans 2019-2026 snippets; the 2019 pause roadmap cannot override later implemented pause billing. L2404[0,876), P1390 links a 2025 scheduled start/stop workflow scoped to VDC/AuraDS Enterprise; this existing lifecycle optimization still requires paused/storage costs and restart accounting.

No incidence executor or feature-state elimination mechanism found in these pages. New exact-value rows will be appended after continued reading, not re-audited at every checkpoint.

### Checkpoint O: Pages71-75

P1391-P1578 consumed fully, all five END markers present. Exact offsets and aliases are reproducible by the saved parser; no linked-source expansion or runtime work.

- L2416[0,11058), P1391: HN search blurbs mix concrete periodic KG loading pain around 100M entities, 1-2TB SQL comparisons, license concerns, irrelevant jobs, and browser OPFS/P2P alternatives. These are incomplete anecdotes, not equivalent-workload measurements. L2421 numeric size, L2429 NOT FOUND/no-paused, L2434 HTTP success, and L2463 downloaded-size outputs establish capture status only.
- L2468[0,4192), P1396: full cost-explorer extract adds retention of destroyed-instance usage and a 180-day lookback with stated maximum 30-day selected interval, despite also listing a 90-day preset. Preserve that documentation tension; do not invent retention behavior. L2473[0,7258), P1397: billing dimensions distinguish primary, secondary and extra-storage quantities, free storage 2x RAM, AuraDS no extra storage/secondaries, and ten-minute AGA minimum; the second requested page is a 404/navigation capture. Equal units do not imply equal SKU rates.
- L2496[0,1731), P1398-1405 and L2500[0,2307), P1406-1421: aggregator quotes/prices and outgoing links are provenance leads. Its GraphBLAS-versus-pointer slogan is not evidence that GDS uses pointer objects everywhere. L2506 is HTTP429. L2510[0,984), P1423 traces the memory criticism to an unfinished ocean-shipping application anecdote. L2511[0,803), P1424 records abandoned prototypes due to licensing; L2516[0,1352), P1425 includes a Memgraph CTO price/license claim and Neo4j replication complaint with no definition of massive updates.
- L2541[0,1470), P1426-1434, L2547[0,1143), P1435-1440, L2597[0,570), P1480-1487, L2602[0,570), P1488-1494, L2628[0,1682), P1508-1514 and L2633[0,1132), P1515-1517 repeat or summarize the March2025 no-projection thread. Primary quoted rationale: direct-database execution existed but was slow; enterprise buyers could rent large RAM, so out-of-core work was deprioritized. This is historical product prioritization, not impossibility or a roadmap promise. L2689[0,261), P1574 and L2690[0,44), P1575 contain no independent employment evidence; do not infer staff status merely from technical authority.
- L2553[0,839), P1441-1445 lists releases but its no-memory-mentions conclusion is contradicted by actual later release bodies. L2558 redirect; L2563 CAPTCHA; L2564 generic results; L2574 zero-hit grep; L2578 counts/tree; L2581 paths/counts: all read, none prove feature absence. L2585[0,1081), P1471 explicitly says heap estimation only rejects certain nonfits and does not guarantee success. L2591[0,128), P1478 establishes in-memory session projection.
- **L2596[0,1256), P1479 is direct counterevidence to pointer-object-only GDS:** packed integer adjacency stores CSR off-heap, improves locality/branching, but ID mapping/property loading can dominate build; no allocation size limit and low native memory can crash the JVM. Off-heap is not bounded RAM or out-of-core. Size heap, native CSR, page cache and OS together.
- L2590[0,1196), P1472-1477 fails to find Infinigraph; L2647 redirect, L2651 empty query, L2652[0,1614), P1524-1532 then finds September2025 property sharding. L2667[0,944), P1540-1548 captures primary marketing claims of 100TB+, unified transactions/analytics, ACID and separated compute/storage. L2672[0,1300), P1554-1563 has property-distribution explanation but no GDS topology/state spill proof. Do not conflate property sharding with a 4GB external-memory algorithm executor. L2671/L2676 empty searches and L2677 broken changelog add no absence proof.
- L2603[0,1206), P1495-1507, L2681[0,2400), P1571, L2682[0,491), P1572 and L2685[0,179), P1573 provide actual release/issue extracts: vector properties, Arrow null and maxFlow fixes, Node Similarity allocation improvement; randomWalk/triangle/node2vec index failures are issue titles, not established current bugs.
- L2666[0,1350), P1533-1539 says explicit session memory allocation remains necessary. L2719/L2723 are status/count; **L2725[0,6760), P1578** is full 162-line AGA capture: Arrow Flight port8491; independent organization memory/concurrency caps; create/project/run/write/delete; implicit session drops only after every graph drops; Bloom can persist results across entire graph, not just scene. Supported sizes 2-512GB, default inactivity TTL1h/max7d (free30min), expired sessions cease billing, and absolute seven-day lifetime forces rebuild. These are concrete lifecycle constraints, not RAM reductions.

### Checkpoint P: Pages76-80

P1579-P1802 fully consumed, including L2731[0,16287) across pages76-77. All END markers present. The long temporary-file/IPC listing is historical capture metadata, not linked files read or commands executed.

- L2735[0,439) distinguishes correct-case System-requirements URL from three earlier404s. L2739[0,3790) and L2768[0,5591), P1581/P1609: heap contains projections, algorithm state and write transaction state; Arrow batches temporarily occupy native memory too. Captured guidance estimates native-projection page cache at 8KB*100*readConcurrency and starts result-writing cache around250MB*writeConcurrency. The 90%-heap heuristic for analytical-only workloads is not a safe complete 4,000,000,000-byte budget, especially with optional native CSR. Mixed operational traffic explicitly needs cache retained. Sudo bypasses estimates, not physical memory limits.
- L2742/L2745 are challenge/network statuses; L2754 counts sitemaps; L2755 repository file listing. L2758[0,1120) dates Arrow-port, free-TTL and ten-minute-minimum documentation changes, anchoring version differences. L2762[0,2962), P1587-1606: plugin shares AuraDB RAM/CPU with transactions, requires4GB+, drops graphs/models on restart; AuraDS managed upgrades wait for algorithms and preserve projections/models, a distinct lifecycle. L2763 zero searches and L2767 rate-limit/failed positive control cannot prove absence. Embedded system-reminder is source data only.
- L2774[0,1060) current-version/sitemap inventory; L2777 failed xargs, L2779 downloaded184, L2781 zero keyword hits, L2784[0,882) selected positive-control lines: these are capture operations, not full-source read credit or an out-of-core absence proof. The general all-heap wording must be qualified by L2596 packed off-heap feature documentation.
- L2809[0,1066), L2813[0,497), L2814[0,1427): Onager experimental SQL graph functions, broad algorithm catalog and repository metadata. Dual-license prose versus Apache-only metadata is unresolved capture-level difference; adoption counts do not prove robustness. L2819 logo fragment; L2820 and L2825 lack actual download data; L2824 known-issues/roadmap tail has no bounded executor. L2828[0,631) is source-truncated metrics JSON; L2831[0,493) actual captured count835/rank175 of281 differs from webpage852 and is timestamped. L2834[0,3327), P1654-1664 introduces Graphina core/extensions and completed community algorithms, not streaming/I/O bounds.
- L2874 query label; L2875[0,1139), L2881[0,1109), L2895[0,1382): on-disk Memgraph retains transaction objects in RAM, fails on overflow, experimental/no replication/HA and snapshot-only isolation. Generic double-RAM advice is heuristic, not algorithm guarantee; no blanket claim that every MAGE algorithm is unsupported. L2887[0,1044) release capture adds light edges (claimed24B/relationship saving) and lower float precision: existing representation optimizations with semantic/precision conditions.
- L2920 incomplete pricing page; L2941 redirect/status, L2943 JS asset pointer, L2949 challenge, L2952 URL statuses, L2954[0,2804) marketing/snippets, L2957[0,1576) navigation, L2960/L2963 CloudFront errors, L2966 404, L2968 shell glob error, L2970 paths: all consumed, no new algorithm facts. L2972[0,1615) repeats ten-minute AGA billing and adds GraphQL MB-hours/log egress; L2975 session file paths are not code inspection beyond captured content.
- L2977[0,1941), P1723-1736: SessionMemoryValue rejects empty but replaces Gi with GB textually, and enum lists2-512GB. This is label normalization, not proof that advertised4GB equals exactly4,000,000,000 bytes. L2982 CAPTCHA including all unique separator pieces is read as boilerplate. L2985[0,12608), P1743-1792 contains overlapping pricing windows, all unique text consumed: professional .09/GB-hour, BC .20/GB-hour, and AGA .40/GB-hour headline, which must be reconciled with detailed .0067/GB-minute SKU rounding/version. L2988[0,1501), P1793-1802 dates minimum-billing change and supplies raw billing-section example; source continuation remains on page81.

### Checkpoint Q: Pages81-85

P1803-P1853 consumed fully, all END markers present; L3036/P1829 read continuously[0,12000),[12000,22891), including CSS/SVG numeric false positives. Next page86 continues L3134 vector-type material.

- L2991[0,1830), P1803: marketing65+ algorithms/70%cost/50-80%accuracy lacks workload evidence; captured client estimate signature accepts graph counts, algorithm categories and property counts. L2994[0,919), P1804 shows deployment-specific caps despite global2-512GB enum. **L2997[0,3073), P1805-1826** explicitly qualifies isolation: projection/write-back affect source DB, avoid cluster leader for projection; tier memory/concurrency limits override unlimited-concurrency marketing. Cross-region egress, changing session OOM requirements, Python estimate versus Cypher trial/error, TTL cleanup, and external intermediate results before hard stop are existing lifecycle mechanisms/constraints.
- L3030[0,3005), P1827 is navigation-only product head; L3033[0,98) version-token counts; L3036[0,22891), P1829 demonstrates why regex release searches hit CSS/SVG numbers before real notes. All consumed; no algorithm inference from decorative numeric matches. L3039 HTTP sizes and L3041[0,976) release/date list distinguish webpage publication dates from earlier GitHub release timestamps (notably2.25/2026.03).
- L3044[0,4390), P1832: actual maxFlow beta/modes and minCost/nodeCapacityProperty, invalid-flow and Arrow-export cleanup fixes, plus footerCSS. L3047 status; L3049[0,3639), P1834-1839: HITS mutate/write auth/hub fix, Leiden no-swaps fix, invalid graph generator constraints, early termination, Node Similarity allocation/distribution optimization, Yen inverse-index improvement, Arrow null/maxFlow tiny excess and strict projection-config validation, then vector properties/L1/L2 scalers. These are specific existing mechanisms and versioned correctness fixes, not spill support.
- L3054[0,2542), P1840 is mostly navigation; its disk keyword is not technical evidence. L3057[0,3312), P1841 captures projection model: up to2^45 node IDs, doubled relationships for UNDIRECTED, array-like double weights; accompanying '8 bytes per node' wording should not be silently converted into per-edge accounting. Production-ready algorithms alone are guaranteed estimate mode, and estimates may combine build+run. This is additional counterevidence to pointer-only representations, but not proof of peak-RAM completeness.
- L3104[0,2446), P1842 and L3107[0,2546), P1843 are dated archive/head extracts with ellipses; L3110 links, L3112 HTTP sizes, L3114[0,6097) three navigation-heavy heads, L3117 keyword lines, L3120[0,2307) actual2026.04-.06 bodies. Distinguish full captured text from full linked release pages; no absent-feature inference from omitted bodies. L3124 blocked projection/vector captures; L3132[0,977) later raw-vector grep explicitly shows lossy FLOAT32 conversion example. L3134[0,768), P1851-1853 begins VECTOR storage on nodes/relationships, dimension/type semantics and retrieval functions; substantive continuation unread until page86.

### Checkpoint R: Pages86-90

P1854-P1904 fully read; P1905 raw L3303[0,12000) read, continuation starts page91. All five END markers present. This is a reading checkpoint, not a terminal handoff.

- L3134[770,2530), P1854-1868 completes captured VECTOR excerpt: fixed dimension1..4096, coordinate type controls precision/storage, FLOAT32/64 and INT8/16/32/64 supported, lists forbidden as elements. Native compact coordinate types are already proposed/shipped here; projection conversion and algorithm state must still be costed. L3138[0,2816), P1869 gives native projection property omission/default behavior and one404; L3140[0,3260), P1870-1871 plus exact aliases covers supported-property/mutate material and relationship-type filtering. Do not equate an omitted property with an eligible filter automatically.
- L3143[0,2959), P1872 reproduces release fixes with source truncation markers; no novel algorithm. L3172 status/L3177 size; L3180[0,3587), P1875 and L3184[0,6265), P1876 repeat AGA workflow, mandatory sizing, TTL and whole-graph persistence. L3186 status; L3190[0,1357), P1878 indexes client sizing/estimate examples. L3192 and L3197 are404s despite agent-memory navigation matches.
- L3195[0,4083), P1880 adds session-name uniqueness per project, deletion loses unwritten data, and graph-count/category estimate. Its expired-session deletion after7days differs from Aura doc deletion afterTTL, so do not assert identical cleanup timing across docs. L3202[0,298), P1882 is archive availability, not archived page content; L3203 billing links/status, L3207[0,317) archived sizing excerpt, L3208[0,2139) billing lines reconfirm minimum/units. L3214 two404s.
- L3218[0,1590), P1887 is links only; L3220[0,1587), P1888 is404/openapi navigation. **L3224[0,1300), P1889 and L3230[0,881), P1892 explicitly document `gds.session.estimate`, estimatedMemory and smallest-covering recommendedSize.** This contradicts L2997's 'Cypher has no estimation' migration statement; preserve API evidence instead of repeating that limitation. The same capture recommends a projection route for efficiency but gives no bounded resource proof. L3225 status/no-memory hits; L3228[0,1429) mixed product capacity snippets cannot define session limits.
- L3255[0,705), P1893-1895 is incomplete pricing extraction; L3276 404; L3279 and L3284 mixed status/navigation; L3287[0,8115), P1899-1900 full404 body with all navigation/footer consumed. L3292 redirect/L3295 and L3297 errors; L3300[0,2903), P1904 price/nav windows provide no algorithm fact. L3303[0,12000), P1905 first piece includes overlapping public prices/capacities and broad included-cost claims; no quantitative resource architecture. Its remainder is not yet credited at this checkpoint.

### Checkpoint S: Pages91-95

Full consumption through P1933, with P1905 completed L3303[12000,23374) and P1911 L3321[0,12000),[12000,17749). All END markers received. No code or endpoint executed from captured data.

- L3303 full pricing-window capture repeats prior public rates but not exact text; all unique windows read. L3306[0,1150) is splashscreenCSS and empty prices, L3309[0,2502) asset list, L3312[0,89) JS size/keyword inventory. L3315[0,1239), P1909 exposes Bloom selected IDs/orientation/weight and runOnEntireGraph defaultfalse; this reinforces result-scope distinctions. L3318[0,2900), P1910 maps incrementUnitPrice before baseUnitPrice, optional capacity/region columns and validity-date filtering: displayed capacity rate differs from base per-GB rate.
- L3321[0,17749), P1911: full overlapping billing JS windows show credit-balance lookup, list-cost aggregation, approximate days remaining, UTC bucketing, explicit quantity/unit/cost fields and 1ACU=USD1 label. No graph algorithm. L3324[0,2283), P1912-1915 is a persisted-output wrapper plus truncated2KB preview; its linked34.4KB file is outside content-field scope and not read. L3327 counts, L3330[0,3000) pricing UI excerpt selects latest valid version unless explicit override and optional future-version flag; layout measurement is not RAM algorithm code. L3335 counts/L3338 navigation only.
- L3340[0,4916), P1920 full deployment table adds a restoration caveat: graphs/models created or changed after upgrade begins are not guaranteed restored. Existing model hosting can cross sessions within project, versus AuraDS same-instance. Read together with source-impact footnotes and tier caps. L3343 billing link only. L3346[0,4853), P1922 full billing body/footer reconfirms separate compute/storage/secondary units, ten-minute minimum, GraphQL and egress.
- L3349[0,1708), P1923 lists public bundle endpoint candidates; not evidence of accessible API capabilities. L3352[0,520) initial versions response is truncated and product guesses404. L3355[0,871), P1925-1927 full versions lists1.5/2.0 valid2026-08-01, future relative to July capture; failed paths add no prices. L3358[0,1006) URL config; L3361[0,575), P1929-1932 locates versioned product response amid404s. L3364[0,956), P1933 inventory584rows/105broad GDS matches and example `professional.aws.gds.1gb` at.125/GB-hour markednot-sellable: this is not the serverless session SKU or purchasable minimum.

### Checkpoint T: Pages96-100

P1934-P2145 fully consumed, every END marker present; source-specific observations below. All support-table rows printed here were read, not merely keyword-swept.

- L3366[0,2699), P1934 full three-cloud512GB rows have base.0067, increment3.4304 and ten-minute minimum; the 'nonlinear base rows' diagnostic incorrectly compares base with scaled capacity price. L3369[0,822) missingfile/KeyError makes its schema comparison incomplete; L3372[0,2240), P1936 resolves v2.0 to3base session SKUs without incrementValue, not removal of512GB capability. L3376[0,305) exact decimal check establishes linear .0067*GB and historical205.824/hour at512GB, not a new timing measurement.
- L3396[0,1331), L3400[0,1038), L3408[0,1487), P1938-1969 trace120GB ephemeral K8s allocation/~40GB OOM to manual heap configuration pain. Staff title is actually present in L3408; promised 'new tricks' is not a shipped spill feature. L3433[0,1801), L3439[0,1100), L3444[0,1102), P1970-1991 repeat no-projection thread; compressed-oops thresholds are historical advice, not a graph state lower bound. Claimed100x old direct-DB slowdown has no reproduced workload here. L3439 staff:true extract conflicts with earlier raw genericflag evidence, so title evidence is preferable.
- L3449 redirect; L3454 empty query; L3457 brokenchangelog/tags; L3461 releaseasset list are consumed capture operations. L3462[0,1764), P2000-2009 gives snippet-only out-of-core GNN leads (Kedagraph, Peridot GPUDirect, Taurus, AIRES SpGEMM) without read papers. **L3467[0,975), P2010-2013** names Lumos dependency-driven value propagation for PageRank/shortestpaths/CF, MBFGraph SSD evolving graphs (captured475GB/4GB and24%/60% reductions), Redio I/O reduction. These are prior-mechanism leads, not independently verified experiments or new inventions in this audit.
- L3468[0,2938), P2014-2022 manual index/license and commented JS contains no further executor. L3491 filelist/L3495 sourcecitation grep/L3500 internalPMF verification table are historical self-references, not independent corroboration. L3496 header-only failed pricing extraction. L3522 status/L3525 assets+header/L3531 mixedHTTP/L3534[0,3152) nav+counts add no algorithm fact.
- L3537[0,12247), P2035-2063, L3540[0,5286), P2064-2073 and L3543[0,8965), P2074-2094 all unique overlapping product windows read. They distinguish Snowflake on-demand in-table service, persistent AuraDS co-located store/compute, and larger AuraDB property-store capacities; none promises a512GB session for every tier. General storage/IO/transfer-included wording must remain qualified by quota/egress billing docs. L3546 JS counts/failedpaths/HTMLfallback, L3549 failedcataloguepaths, L3554 CAPTCHA are capture metadata.
- L3578[0,1365), L3583[0,1186), L3584[0,737), L3589[0,1075), P2099-2120 reaffirm Onager SQL analytics versus DuckPGQ pattern/path querying; directed/weighted and parallel support do not imply spillability. L3590 README tail credits Graphina; L3594 known-issues/roadmap aliases add no bounded executor. L3599[0,9917), P2124-2128 plus earlier exact paragraphs: Graphina lists deterministic maps, graph builders/serialization/validation, shortestpaths, centralities/triangles/MST. Bipartite generator/check is not incidence-preserving execution. L3600[0,923), P2129-2136 repeats catalog/example/metrics but no bound.
- L3633 inaccessibleReddit; L3637[0,558) archive metadata plus generated summary is weaker than actual thread; L3638 raw archiveavailability; L3641 unablefetch; L3643 downloadedHTMLsize and L3646 textsize. No unread linkedarchive credit: actual captured thread text begins on next page.

### Checkpoint U: Pages101-105

P2146-P2151 fully consumed: L3648[0,11812), L3651[0,8087), L3655[0,8145), L3656[0,51), L3659[0,5687), L3662[0,12000),[12000,16275). All END markers received. These different captures overlap semantically but are not silently treated as exact aliases.

- L3648/P2146 keyword-selected thread lines include PC-component compatibility, license shock, export portability, visualization and alternative-engine reliability pain. Selection includes related-topic/site chrome, so it is not itself the full Reddit thread. L3651/P2147 actual sequential text makes the OP's requirement explicit: ability to migrate/export, not necessarily free. A four-person PC-builder team reports a GBP50,000 quote then ArangoDB migration without needing graph queries; this is anecdotal evidence to test whether a workload needs a graph algorithm at all, not a published current tariff. Political/speculative motive claims and unsupported legal-loss figures are not adopted.
- L3655/P2148 continuation distinguishes visualization struggling at5000vertices from storage/algorithm limits, requests approximatelyUSD20/month for5-6projects, contains undocumented AGE/Memgraph/Dgraph dissatisfaction and vendor support response. No equivalent workload, RAM accounting, hardware or benchmark supports ranking those engines. DozerDB example includes an n/p variable mismatch in a constraint; historical example text is not runtime verification. Revision-control/provenance and transferable Gremlin skills are existing product ideas, not new algorithm mechanisms.
- L3656/P2149 dates the original post2020-11-07; L3659/P2150 actual timeago attributes separate2020recommendations from2024replies. L3662/P2151 author/permalink/body extraction is fully consumed but several TS fields say `userSpaceOnUse`, an SVG attribute extraction error, and bodies are source-truncated. Those entries cannot be timestamped from that field. Their full text where available was already consumed in L3651/L3655. Thread age matters: historical license recommendations are not present-day verified legal guidance.

Implication for verification-first product work: exportable IDs/edges/properties, known-schema inspection and honest completion/error statuses need acceptance tests alongside bounded algorithms. This thread develops no incidence-only BFS/WCC, PageRank state elimination or low-I/O executor.

### Checkpoint V: Pages106-110

Fully consumed P2152 at physical L3695 offsets [0,27674), P2153 at L3696 [0,28225), and P2154 at L3701 [0,3995), including all split pieces and END markers. No linked pages were recursively read.

- L3695/P2152: the captured CNRS/LIG tutorial by Genoveva Vargas-Solar (https://gevargas.github.io/doing-studio/Neo4J-datascience-tutorial.html) explicitly projects same-house people with `MATCH (p1:Person)-[:BELONGS_TO]-(:House)-[:BELONGS_TO]-(p2:Person) RETURN id(p1) AS source,id(p2) AS target`. This is positive evidence of an entity-entity derived join, not avoidance of clique projection by expanding each attribute posting once. Multiplicity, multiple witnesses, and path relationship uniqueness need an explicit simple-graph contract. Its GoT edges represent co-mention within 15 words, not established real-world relationships. Outgoing-match summary queries omit isolates. Projection estimates versus combined implicit build/algorithm estimates and drop/free lifecycle are already developed; native projection speed rhetoric is not measured here. The GDS Agent paper excerpt describes memory-efficient parallel implementations over in-memory projections, not pointer objects everywhere. Local-demo Java/version/port friction and historical Azure heap failure are contextual, not root-cause proofs.
- L3696/P2153: the pricing bundle mixes RAM, graph size, database storage, replicas, and marketing claims. Toolradar's database-size comparisons and Checkthat replica/storage-rate inference are not equivalent cost accounting. Primary ACU/cost-explorer captures retain role, quantity, pause and destroyed-instance accounting. A preview 1952GB AuraDS row does not overturn the independently documented session cap. Secondary alternatives' speedups, thread ceilings, fraud accuracy/latency, license interpretations, and a private 5%-revenue quote remain captured unverified claims, not measured architecture evidence or a general tariff. The crossposted 2020 complaint is not another independent customer.
- L3701/P2154: high-TPS, mutable, read-heavy graph storage requests and TAO/SQL discussion do not establish batch-analytics equivalence. The purported Neptune 80% cost reduction is an AWS sales quote that the poster explicitly doubts because of feature differences, not achieved savings. All boilerplate, related-topic text, errors, and included metadata were read.

### Checkpoint W: Pages111-115

Fully read P2155-P2235, including L3702 [0,27953) across two pages, and each displayed canonical offset for L3707,3712,3743,3747,3750,3755,3758,3761,3785,3786,3790,3791,3795,3796,3800,3801,3805,3806,3809. Exact paragraph aliases retain earlier read credit; nothing omitted because it was code or boilerplate.

- L3702/P2155: GraphRAG cost bundle separates extraction/community-summary indexing from local retrieval, global map/reduce, and temporal agent memory. The Microsoft repository itself warns indexing is expensive; secondary dollar ratios, 79% token reduction and comparative quality are captured claims, not measured here. FalkorDB's generic exponential-index-size explanation is not a bound for adjacency storage: number of paths and stored graph size differ. LightRAG's cheaper flat graph is not semantically equivalent to hierarchical global summarization. The commercial-product thread remains the same prior customer, not independent evidence.
- L3707/P2156 preserves the dated 2024 private revenue-quote anecdote; L3712/P2157 is a fetch failure despite HTTP200 and provides no article evidence. L3743/P2158 is HTTP/file-size metadata; L3747/P2159 and L3750/P2160 confirm running-or-idle GB-hours and 20% paused billing, CSV export, 180-day lookback with maximum 30-day selection. L3755/P2161 distinguishes RAM compute, extra storage, replica quantities, session GB-minutes/ten-minute minimum and GraphQL MB-hours; shared unit names do not prove shared prices. Its pause endpoint is404. L3758/P2162 archive lookup finds no 2025 snapshot and a January2026 closest snapshot, not the requested June2025 capture. L3761/P2163 adds twice-RAM free storage and no extra AuraDS storage purchase.
- L3785/P2164-2172 is an Onager overview reporting no memory details, not evidence of bounded execution. L3786/P2173-2177 identifies Graphina's high-level Rust API and resident-looking storage; later source captures strengthen this. L3790/P2178 inventories cloned trees, L3791/P2179 is only a query heading. L3795/P2180-2183 supplies Onager commit `49ad15b...` and unchecked performance benchmarks, so claims cannot be treated as measured. L3796/P2184-2201 records Graphina commit `835d1bc...`, broad centrality/link/community/triangle catalog, feature flags, and Wasm parallel algorithms becoming sequential with file IO returning runtime errors. Catalog rows were read structurally in full; they do not supply low-RAM algorithms.
- L3800/P2202-2208 identifies Rust algorithm/FFI and C++ binding boundaries. L3801/P2209 directly shows StableGraph, Fx-hashed node/edge maps and visited sets; the comment about index recycling is not independently validated. L3805/P2210-2213 confirms globally named create/drop/list lifecycle. L3806/P2214-2226 shows thread-local errors and panic containment, plus Emscripten's explicitly single-thread rayon pool; none is a memory admission mechanism.
- L3809/P2227-2235 directly constructs a whole graph from source/destination slices, with forward and reverse HashMaps coexisting. Endpoint-only discovery loses isolated entities unless another path supplies them. Every input edge is added, so duplicate and reciprocal rows retain multiplicity. Weight validation distinguishes NaN rejection and nonnegativity but the displayed helpers do not reject positive infinity. SQL-array residency, graph residency, maps, algorithm state, and output must all enter peak accounting; DuckDB's own memory limit is not shown governing these allocations.

### Checkpoint X: Pages116-120

Fully consumed P2236-P2286, with every displayed canonical span, through L3963 [0,4389). Source-specific observations below include empty searches and failed fetches; those are evidence of capture state only.

- L3811/P2236 lists complete source/destination/result vectors across centrality bindings. L3815/P2237 is an empty spill/disk search, not exhaustive proof. L3816/P2238-2244 explicitly contrasts in-memory named registry with per-query table functions. L3820/P2245-2251 is stronger: PageRank appends every input chunk, returns zero rows until finalize, calls FFI first for count then results, retains all result vectors, and only then emits STANDARD_VECTOR_SIZE chunks. `MaxThreads=1` limits this binding, not necessarily internal algorithm workers. Two FFI calls are established; whether both recompute requires the called code, not inference from the C++ alone. Defaults are undirected,100 iterations,1e-6 tolerance; zero-edge input returns no entities. L3821/P2252-2254 lists registry operations and 12 delegating algorithm files. L3824/P2255-2257 pins petgraph0.8.3, sprs, nalgebra, rayon and serializers, with no matches for the three searched storage libraries; presence of serializers is not spilling.
- L3852/P2258 is status/size only. L3857/P2259 and full L3860/P2260 reconfirm explicit2-512GB size choices, ArrowFlight8491, remote projection/writeback and hard seven-day lifetime. The sentence that larger size means faster runtime is a documentation claim, not universal scaling evidence. L3864/P2261 records successful client docs and two404s. L3868/P2262 confirms a Python estimator and native projection path; L3870/P2263 supplies sizing categories and TTL details. Its deletion-after-seven-days statement differs from Aura's deletion-after-TTL statement. L3873/P2264 is202 only; L3876/P2265 six404s; L3879/P2266 a link inventory, not linked-file reads.
- L3881/P2267 confirms in-memory session execution and RAM-minutes, native loading/orientation support, and organization limits. L3884/P2268 supplies session ID/error/expiry fields and a credentials simplification, not an estimator implementation. L3887/P2269 failed changelog/spec; L3889/P2270 navigation; L3891/P2271 status/size; L3894/P2272 links; L3896/P2273 visualization scaling/color semantics and graph-size-dependent runtime; L3901/P2274 a202 search page; L3929/P2275 status; L3934/P2276 mandatory memory excerpt.
- L3936/P2277 reads the extended Aura page including footer: implicit-session deletion waits for all graphs; Bloom property persistence targets entire graph. L3939/P2278 records one success/two404s. L3941/P2279 repeats estimator/API pointers; L3943/P2280 adds standalone/non-Neo4j session support, custom TLS roots, and loss of unwritten data on deletion. The historical TLS-disable snippet is explicitly testing-only and was not executed.
- L3946/P2281 is link extraction. L3948/P2282 and L3951/P2283 explicitly document `gds.session.estimate`, categories including similarity/embedding/ML, estimatedMemory and recommendedSize as smallest covering tier. These correct any earlier blanket 'Cypher cannot estimate' assertion, without proving estimator peak accuracy. L3954/P2284 is a404 plus generic-nav matches; L3960/P2285 has query headings only, not autoscaling evidence. L3963/P2286 distinguishes database tier limits up to1944GB from the session limit; its 'unlimited concurrent sessions' pricing line must be qualified by the organization/tier caps shown elsewhere. No bounded local out-of-core mechanism is added by these repeated captures.

### Checkpoint Y: Pages121-125

Fully consumed P2287-P2311 with END markers. L4000's P2291 is split at paragraph offsets12000, with source [70,13249), followed by P2292 source [13251,25057); both full. The following notes attach to all newly encountered canonical sources, not their external linked destinations.

- L3965/P2287 explicitly advertises an enterprise low-memory analytics graph format and Community four-core maximum. Together with the earlier off-heap CSR evidence this defeats the pointer-objects-only premise. The public AGA512GB and $0.40/GB-hour line does not establish exact console SKU arithmetic, automatic mid-run resizing, or70%cost/50-80%accuracy claims. AuraDS8-384GB is a separate advertised product range. L3993/P2288 is successful cost-page status. L3996/P2289 reads overlapping navigation and cost-explorer windows; repeated nearby snippets are not new independent corroboration.
- L4000/P2290-2292 adds lifecycle work to paused billing: pause creates a backup; resume restores it and may take minutes to hours; paid instances automatically resume after30days, while Free auto-pauses after72inactive hours and may be deleted after30paused days. Pausing halts processing/connectivity, not merely reduces cache. Billing windows also specify no AuraDS secondaries and twice-RAM storage quota. These captures contain overlapping snippets, all consumed rather than normalized away.
- L4003/P2293 inventories dated billing commits; L4006/P2294 gives current source line78 for20%paused charges; L4011/P2295 lists historical changes. L4015/P2296 shows diffs: March2026 changes pricing links/report roles, December2025 introduced/changed usage-report and billing dimensions. Presence of an unchanged20%line in a later diff does not date its original introduction. L4024/P2297 and L4027/P2298 are headings only; L4030/P2299 is a bot challenge, L4035/P2300 an empty result extraction, not absence of customer pain.
- L4038/P2301 search snippets are noisy:2019 pause feature was only roadmap then, not current; union-find deadlock at8.9B nodes/18B relationships and SHARED_IDENTIFIERS fraud posts are leads, not fully captured mechanisms here. The2026external-agent $0.35/hour question concerns another resource. L4041/P2302-2308 pricing snippets promise80%pause savings while a redirected classic AuraDS page contrasts ephemeral sessions, DB-shared analytics, and persistent dedicated compute. 'Resume with a click' must not erase the restoration latency explicitly documented atL4000.
- L4062/P2309 and L4063/P2310 are fetch metadata. **L4066/P2311, source [0,2178), captures the primary Ladybug algorithm overview at https://docs.ladybugdb.com/extensions/algo/: projected graphs are evaluated only when an algorithm executes, are not materialized as an in-memory graph, and underlying data is scanned from disk on the fly.** Their lifetime is explicit drop or connection close. This is concrete prior art for lazy/on-disk projections and projection-build avoidance, not yet proof of bounded vertex state, bounded total IO, or incidence-only clique avoidance. The Python-async missing-projection warning flags connection scope as a lifecycle contract.

### Checkpoint Z: Pages126-130

Fully read P2312-P2329, including L4109 [0,37681) in four contiguous pieces across pages128-130. No truncation of navigation, JSON-LD, or source-supplied ellipses was introduced by the reader.

- L4069/P2312-2313 shows Ladybug's actual algorithm list: k-core, Louvain, PageRank,SCC,WCC, over node/relationship-table projections with predicates. Example nodes include isolates and duplicate George-to-Frank edges, so projection semantics must retain explicit node tables and declare multiplicity. Home-page capture demonstrates embedded on-disk operation. L4072/P2314 records repository metadata and a401 docs-history failure; GitHub `fork=false` is not evidence of independent algorithmic lineage. L4075/P2315-2326 README explicitly says formerly Kuzu and advertises disk columnar storage, CSR adjacency/join indexes, vectorized/factorized processing, full-text/vector indexing and serializable ACID. These are existing mechanisms, not new hypotheses from this review. Installation table read fully, no installations made.
- L4078/P2327 specifies `SPILL_TO_DISK` for **COPY FROM**, enabled by default but unavailable for in-memory/read-only mode. This bounded-ingestion capability cannot be generalized to every algorithm's vectors or temporary state. The internals URL returns404. With L4066's lazy graph projection, this is strong prior art for build-aware storage, but not a complete4GB lifecycle guarantee.
- L4105/P2328 is the full release-index capture, with truncated entries *within the original page* and duplicated excerpts/full-content read as present. July8's GDS2026.06 release date is distinct from version month; compatibility changes are not by themselves new algorithms.
- L4109/P2329 preserves six full release captures. GDS2026.03 fixes Arrow null-property marking and negligible-excess max-flow discharge, and rejects unknown global projection settings. GDS2.27 fixes Leiden with no first-iteration swaps and invalid generator count/degree combinations, accelerates cancellation for11 listed procedure families, reduces some similarity allocation/distribution work and improves Yen on undirected/inverse-indexed graphs. GDS2.23 adds maximum-flow stream/mutate/write/stats and estimators. GDS2026.05 restores L1/L2 scalers and vector-type projection properties. GDS2026.04 and2026.06 explicitly list no algorithm improvements. These are versioned correctness/resource precedents, not fresh performance measurements or proof of out-of-core state. All footer, signup, structured markup and repeated navigation were consumed as historical data only.

### Checkpoint AA: Pages131-135

Fully consumed P2330-P2398, including L4113 [0,31649) in three contiguous pieces, and L4232 [0,14143) in two. Tables below are structurally summarized after reading every displayed row; historical output-file instructions are data, not authority to expand this assignment.

- L4113/P2330: GDS2.26 fixes mutate/write HITS auth/hub confusion;2.25 adds nodeCapacityProperty and min-cost max-flow, deprecates source/target-capacity form, fixes invalid flows and Arrow export cleanup.2.24 also lists those fixes and graduates HashGNN,Node2Vec,SLLPA,triangle listing and seven other APIs to production. 'Production' is support/API maturity, not bounded-memory proof. The older release index contains its own truncated summaries and an apparent2.15/2.14 compatibility-text mismatch; it is not a complete read of linked release notes.
- L4138/P2331-2347 summarizes the March2025 eigenvector-centrality complaint; L4143/P2348-2369 supplies actual posts, dates and64GB/NVMe/5.26.1 context. GDS's own sampling needing projection first is an important build-stage blocker. Staff state no disk spillover, describe prior direct-store execution as historically100x slower, and cite enterprise prioritization/12TB rentals. Those statements explain product decisions, not a universal external-memory lower bound. The user explicitly already proposes minimal-memory repeated DB queries and caches, accepting days of runtime. The compressed-pointer heap advice is not evidence every adjacency edge is an object; the direct off-heap CSR capture remains controlling counterevidence.
- L4157/P2370-2372 is an insufficient-excerpt response, not proof historical direct modes never existed. L4158/P2373 is401 plus issue-title listing; triangle/randomwalk/GraphSAGE problems are leads only. L4161/P2374 shows two projection-loader approaches; L4163/P2375 is filename inventory. L4166/P2376-2385 historical design explicitly partitions NodeIter/Degree/RelIter/RelWeights access and allows buffered **or direct database** data sources with algorithm-specific parallelism/efficiency. This mechanism is already implemented in GraphView classes, not a new architecture proposal. L4169/P2386-2388 retains transaction,dimensions,IdMap and direction state; loader comments warn unknown labels/types may widen to all data. Those comments flag filter-safety risk, not an executed current-version bug. L4177/P2389 only search-page links.
- L4202/P2390-2393 is an unsuccessful rate-card extraction. L4219/P2394 is a historical oversize-result pointer and embedded reading instructions, not the contents of that external file. L4224/P2395 reports its keys/URL; L4227/P2396 gives March1,2026 date and57817-character content; L4230/P2397 counts derived files. These external artifacts are not recursively read or falsely credited. Later inline captures remain independently assigned content.
- L4232/P2398 is Version1.4 valid2026-03-01,1ACU=1USD, captured table lines1-1400. AuraDS running AWS/Azure1-512GB and GCP through1952GB scale at0.3250ACU/GB-hour;1GB rows are marked not sellable. Paused rows shown scale at0.0650, exactly20%, including4GB1.30 running versus0.26 paused. High-GCP capacities1433/1952 are AuraDS SKUs, not evidence of larger AGA sessions. The last paused row continues in the next capture; no inference from a partial row was needed.

### Checkpoint AB: Pages136-140

Fully consumed P2399-P2444 and P2445 [0,12000) atL4393; P2445 remains partial at this historical checkpoint only. Every table row in the displayed captures was read; summaries retain dimensions and exceptions rather than reproduce the tables.

- L4235/P2399 [0,15825) completes paused AuraDS1433/1952GB and captures AuraAgent0.35/hour with included10M input/3M output tokens, then Professional GDS0.125/GB-hour running and0.025 paused across three clouds through96GB. Agent tokens' zero-price rows do not establish unlimited free tokens beyond the included allowance. GraphQL rows256-4096MB have capacity-dependent quotes, distinct from algorithms. L4238/P2400 lists serverless SKUs/minimums; L4240/P2401 supplies actual rates; L4243/P2402 [0,13447) completes all three clouds' AGA rows through512GB. Rates scale0.0067 times capacity, despite the repeated GB-minutes label: multiplying a capacity-specific row by capacity again would double count. Base-rate model0.0067*GB*minutes reconciles the table and billing examples;512GB-hour205.824 is not exactly rounded headline204.80. Ten-minute minimum and unsellable1GB remain explicit.
- L4243 also records BusinessCritical primary0.20/GB-hour and VDC0.325, not session prices. L4246/P2403-2405 are structural counts, not full unseen rate-card credit. L4249/P2406 gives storage GB-month/730hours, billed hourly, with cloud-dependent rates (BC AWS1.217/Azure2.402/GCP1.256). Free AWS/GCP SKU labels appear swapped, so preserve that capture inconsistency. L4252/P2407-2409 counts products and gives query-log egress AWS0.2475/GB versus Azure/GCP0.205 plus Professional storage tail. Storage and compute emphatically do not share a rate merely because both can be hourly-metered.
- L4280/P2410 and L4285/P2411 are status/count metadata. L4293/P2412 nine successful release fetches; L4295/P2413 file count; L4298/P2414 only navigation; L4301/P2415 body count. L4303/P2416 reads all247 captured release-body lines, confirming the earlier version-specific fixes without adding out-of-core support. L4309/P2417 slug inventory; L4314/P2418 GitHub release timestamps differ from website dates (notably2.25 January19 versus December20,2025;2026.03 April2 versus April8). Pin artifact/version and date type. L4317/P2419 regex hits are not complete-release proof, though full bodies were separately consumed. L4322/P2420,L4325/P2421 are headings only; L4328/P2422 zeroitems; L4364/P2423 forbidden; L4368/P2424 status/size; L4370/P2425 is a blocked-page body, not Reddit comments.
- L4373/P2426 probes show mixed statuses; L4375/P2427 is a shell filename-joining error; L4378/P2428-2436 includes challenge HTML and an unrelated 'Reddit is dead' placeholder, neither reliable platform-status evidence. L4381/P2437 locates a2025archive; L4386/P2438 size, L4389/P2439 authentic title/2024post timestamp. L4391/P2440-2444 is a persisted-output pointer plus preview, not the external-file body. L4393/P2445 [0,12000) begins the actual25-comment extraction: purported sales visibility disputes5%-revenue pricing as miscommunication and distinguishes startup thresholds, capacity licenses and OEM increments. Both accounts remain anecdotes. One customer defers graph adoption12-18months because of negotiating uncertainty; another values visualization/GDS/deployment tools and rejects unsupported120x benchmark rhetoric. Neptune1/6 price,60%extra compute and four-hop degradation are commenters' conflicting unverified claims, not measurements. Read continuation next before final synthesis.

### Checkpoint AC: Pages141-144

Fully consumed P2445 [12000,34144) and P2446-P2583, completing all144 pages,2583 unique paragraph units and1,876,040 UTF-16 code units. All809 unique values and1265 assigned occurrences are now covered. The last canonical content ends at physicalL4497 source offset2136; remaining physical lines through4500 contain no unread assigned content. No linked artifact or later physical range is included in this completion claim.

- L4393/P2445 now full[0,34144): the25 post-block extraction and nested comment-tag extraction repeat participants, not independent customer counts. After-Foot8960 claims sales visibility but is not authenticated as an official spokesperson by this record. The alleged industry focus is later qualified by its author as a particular partner, not Neo4j itself. Broad vendor, geopolitical and business-value assertions remain historical comments. Actionable product need is predictable capacity pricing, portability, CDC and a usable analysis/visualization workflow; the Kuzu invitation is not measured evidence of superiority.
- L4396/P2446 [0,7884) has keyword windows and metadata: 'heap' matches substrings in 'cheap', not heap diagnostics; zero70k hits only bound this capture. L4399/P2447 supplies24 comment IDs/timestamps from2024-2025, supporting deduplication of nested extracts and preventing misleading present-day attribution.
- L4423/P2448-2456 reiterates lazy projections, five algorithms and connection-scoped async caveat with July18,2026 doc date. L4424/P2457-2463 is a different excerpt lacking Kuzu/materialization mentions, not a contradiction of README evidence. L4428/P2464,L4429/P2465 are query headings. L4433/P2466-2472 summarizes compressed CSR/disk storage and licensing as captured. L4434/P2473-2484 adds an important filter restriction: predicates may depend only on their own node/relationship table, not cross-table joins. Directed graph storage and undirected algorithms ignoring direction do not imply distinct-edge deduplication.
- L4438/P2485-2487 is an empty issue-search dataset, not proof of no bugs. L4439/P2488-2492 lists historical Kuzu issue claims, especially COPY consuming over30GB with4096MB buffer manager, relationship insert peaks and join-order OOM. These are reported cases, not newly reproduced results. L4444/P2493-2505 inventories repositories/bindings/licenses; its inference that none are forks is only GitHub metadata and must not erase the explicit formerly-Kuzu README lineage.
- L4449/P2506-2523 records versioned Ladybug issue/PR summaries: result-string use-after-free,8TB sparse virtual reservation/coredump behavior, null-string import corruption, slow/hanging COPY, row-driven PK lookups eliminating eligible full scans/cross-products, shortest-path per-hop filter silently ignored, IN-predicate duplicate/omitted rows, concurrent extension-load and read-only/checkpoint crashes, WindowsFTS dependency and vector-index regressions. Open/closed statuses are historical, not current deployment advice.8TB VSZ is not8TB resident RAM. The PK optimization is concrete preexisting join avoidance for its eligible pattern, not arbitrary entity-attribute projection elimination.
- L4452/P2524-2526 identifies an extension submodule; L4454/P2527-2530 directories; L4458/P2531-2534 algorithm files; L4459/P2535-2538 specifically lists in-memory graph utility files. **L4463/P2539-2541 explicitly calls `initInMemoryGraph` inside Louvain before its phases. L4464/P2542-2553 shows CSR offset/weighted-edge storage and appends. L4468/P2554-2557 shows the actual all-node loop scanning forward and backward edges into that graph, avoiding the backward self-loop insertion, then finalizing.** Thus the projection overview's 'not materialized' claim applies to the logical projection, not every algorithm's execution intermediates. The first relationship-info selection and same-table assertion limit the displayed implementation's scope; no cross-table incidence executor is shown. 'Minimal memory' is the historical summarizer's rhetoric, not a bound.
- **L4469/P2558-2565** describes PageRank's offset-indexed atomic-double PValues, per-node Degrees, dense frontiers, result vectors and repeated on-disk edge-compute traversals. No InMemGraph there does not eliminate O(node-offset-range) state, output cost or iteration IO. Holes in IDs can make maxOffset larger than live-node count. **L4474/P2566-2568** identifies `function::vector_t` CSR containers but no spill guarantee; MemoryManager association alone is not proof all state can evict safely. These captures directly qualify the earlier Ladybug prior-art observation by algorithm family.
- L4497/P2569-2583 is another summary of the same March2025 GDS projection thread; not an additional independent customer. It reiterates no spill and the proposed in-memory/disk choice, with enterprise-priority rationale rather than a technical impossibility argument.

## Complete Exact-Value Ledger

Machine-generated source pointers, credited only because representatives were fully consumed. LxCy means physical line x, JSON path $.message.content.y.content. Same-row values are exactly equal after canonical object-key ordering; array order and strings preserved. Pn@a:b gives exact zero-based half-open UTF-16 paragraph offsets in the decoded-string or canonical-pretty-JSON representation. Equal P IDs are exact paragraph aliases. First occurrence is representative. All809 values and1265 occurrences within1-4500 are listed; V581 remains in its original checkpoint position because its paragraphs were already consumed by page60. No fuzzy/semantic deduplication or linked-file credit is used.

| Value | Representative and exact occurrence aliases | Paragraph source offsets |
| --- | --- | --- |

| V1 | L27C0, L82C0, L308C0, L354C0, L415C0, L471C0, L525C0, L553C0, L606C0, L664C0, L765C0, L838C0, L934C0, L1020C0, L1092C0, L1121C0, L1187C0, L1219C0, L1257C0, L1300C0, L1382C0, L1425C0, L1474C0, L1520C0, L1731C0, L1759C0, L1920C0, L2034C0, L2079C0, L2205C0, L2272C0, L2316C0, L2453C0, L2494C0, L2536C0, L2710C0, L2804C0, L2870C0, L3077C0, L3163C0, L3394C0, L3428C0, L3574C0, L3620C0, L3734C0, L3781C0, L3844C0, L3985C0, L4097C0, L4133C0, L4200C0, L4272C0, L4353C0, L4419C0, L4493C0 | P1@0:137 |
| V2 | L31C0 | P2@0:80, P3@82:369, P4@372:472 |
| V3 | L32C0, L84C0, L214C0, L215C0, L358C0, L359C0, L419C0, L420C0, L476C0, L557C0, L558C0, L680C0, L689C0, L879C0, L893C0, L939C0, L1025C0, L1262C0, L1305C0, L1386C0, L1590C0, L1609C0, L1764C0, L1826C0, L1925C0, L2009C0, L2050C0, L2117C0, L2131C0, L2132C0, L2232C0, L2233C0, L2320C0, L2431C0, L2457C0, L2514C0, L2559C0, L2569C0, L2570C0, L2638C0, L2648C0, L2655C0, L2657C0, L2715C0, L2925C0, L2937C0, L2938C0, L3022C0, L3082C0, L3088C0, L3168C0, L3270C0, L3271C0, L3450C0, L3453C0, L3518C0, L3519C0, L3739C0, L3849C0, L3926C0, L3989C0, L4099C0, L4148C0, L4208C0, L4213C0, L4277C0 | P5@0:39, P6@41:215 |
| V4 | L36C0, L481C0, L882C0, L942C0, L1768C0, L2054C0, L2323C0, L4103C0, L4216C0 | P7@0:225 |
| V5 | L38C0, L40C0, L208C0, L210C0, L232C0, L246C0, L492C0, L884C0, L886C0, L944C0, L950C0, L979C0, L983C0, L997C0, L1595C0, L1598C0, L1602C0, L1770C0, L1772C0, L1993C0, L2002C0, L2006C0, L2014C0, L2122C0, L2127C0, L2159C0, L2171C0, L2325C0, L2327C0, L2387C0, L2407C0, L2411C0, L2426C0, L2643C0, L2660C0, L2750C0, L2771C0, L2930C0, L2946C0, L2980C0, L3093C0, L3095C0, L3127C0, L3263C0, L3274C0, L3290C0, L3511C0, L3528C0, L3552C0, L3705C0, L3710C0, L3715C0, L3753C0, L4019C0, L4033C0, L4153C0, L4172C0, L4180C0, L4220C0, L4307C0, L4320C0, L4333C0 | P8@0:68 |
| V6 | L43C0, L1775C0 | P9@0:20 |
| V7 | L46C0 | P10@0:4301 |
| V8 | L49C0 | P11@0:5317 |
| V9 | L53C0 | P12@0:118 |
| V10 | L55C0 | P13@0:428, P14@430:746, P15@748:1357, P16@1359:2548 |
| V11 | L59C0 | P17@0:835 |
| V12 | L62C0 | P18@0:331 |
| V13 | L66C0, L107C0, L175C0, L292C0, L338C0, L399C0, L455C0, L509C0, L537C0, L590C0, L648C0, L749C0, L822C0, L858C0, L918C0, L1004C0, L1076C0, L1105C0, L1171C0, L1203C0, L1241C0, L1284C0, L1365C0, L1409C0, L1458C0, L1504C0, L1563C0, L1715C0, L1743C0, L1805C0, L1857C0, L1904C0, L1955C0, L2018C0, L2063C0, L2089C0, L2189C0, L2255C0, L2299C0, L2350C0, L2437C0, L2478C0, L2520C0, L2607C0, L2694C0, L2788C0, L2838C0, L2854C0, L2899C0, L3001C0, L3061C0, L3147C0, L3234C0, L3378C0, L3412C0, L3472C0, L3558C0, L3604C0, L3666C0, L3718C0, L3765C0, L3828C0, L3905C0, L3969C0, L4045C0, L4081C0, L4117C0, L4184C0, L4256C0, L4337C0, L4403C0, L4477C0 | P19@0:39 |
| V14 | L89C0 | P20@0:20 |
| V15 | L90C0, L196C0, L202C0, L203C0, L373C0, L382C0, L395C0, L443C0, L444C0, L475C0, L564C0, L582C0, L609C0, L619C0, L620C0, L668C0, L673C0, L683C0, L704C0, L769C0, L878C0, L889C0, L938C0, L947C0, L1024C0, L1030C0, L1055C0, L1056C0, L1190C0, L1261C0, L1267C0, L1304C0, L1309C0, L1357C0, L1385C0, L1405C0, L1478C0, L1552C0, L1583C0, L1588C0, L1605C0, L1825C0, L1831C0, L1836C0, L1924C0, L1931C0, L1974C0, L1975C0, L1978C0, L2045C0, L2110C0, L2115C0, L2125C0, L2218C0, L2219C0, L2238C0, L2369C0, L2370C0, L2373C0, L2458C0, L2464C0, L2474C0, L2540C0, L2546C0, L2552C0, L2627C0, L2632C0, L2637C0, L2662C0, L2714C0, L2720C0, L2729C0, L2736C0, L2808C0, L2919C0, L2923C0, L2933C0, L3021C0, L3025C0, L3052C0, L3081C0, L3087C0, L3129C0, L3167C0, L3174C0, L3212C0, L3254C0, L3258C0, L3266C0, L3403C0, L3405C0, L3432C0, L3438C0, L3443C0, L3501C0, L3505C0, L3506C0, L3514C0, L3577C0, L3627C0, L3631C0, L3685C0, L3686C0, L3689C0, L3738C0, L3848C0, L3854C0, L3865C0, L3899C0, L3925C0, L3931C0, L3957C0, L3990C0, L4010C0, L4021C0, L4137C0, L4142C0, L4147C0, L4207C0, L4212C0, L4276C0, L4313C0, L4330C0, L4359C0, L4365C0, L4496C0 | P21@0:149 |
| V16 | L93C0 | P22@0:4326 |
| V17 | L95C0 | P23@0:3132 |
| V18 | L99C0 | P24@0:341, P25@345:353, P26@356:2540, P27@2542:2575, P28@2577:2630, P29@2632:2850, P30@2852:2947, P31@2952:3029, P32@3035:3644, P33@3646:4810, P34@4812:4979, P35@4981:5058, P36@5060:5315, P37@5317:5368, P38@5370:5618, P39@5620:5694, P40@5696:5766, P41@5769:6000 |
| V19 | L103C0 | P42@0:1157 |
| V20 | L124C0, L192C0, L874C0, L1580C0, L1821C0, L1873C0, L1971C0, L2106C0, L2366C0, L2623C0, L2915C0, L3017C0, L3250C0, L3489C0, L3682C0, L3921C0 | P43@0:137 |
| V21 | L127C0 | P44@0:31, P45@33:262, P46@264:363, P47@365:546, P48@548:866 |
| V22 | L128C0 | P49@0:36, P50@38:57, P51@59:253, P52@255:281, P53@283:636, P54@638:791, P55@793:813, P56@815:1051, P57@1053:1285, P58@1287:1303, P59@1305:1495 |
| V23 | L133C0 | P60@0:334 |
| V24 | L134C0 | P61@0:481 |
| V25 | L138C0 | P62@0:363 |
| V26 | L139C0 | P63@0:151 |
| V27 | L142C0 | P64@0:4025 |
| V28 | L143C0 | P65@0:364 |
| V29 | L147C0 | P66@0:43, P67@45:118, P68@120:200, P69@202:249, P70@251:473, P71@475:559, P72@561:613, P73@615:815, P74@817:1015, P75@1017:1098, P76@1100:1124, P77@1126:1254, P78@1256:1313, P79@1315:1474, P80@1476:1479, P81@1481:1505, P82@1507:1697, P83@1699:1831, P84@1833:1910, P85@1912:1940, P86@1942:2263, P87@2265:2291, P88@2293:2535, P89@2537:2573, P90@2575:2775, P91@2777:2797, P92@2799:3022, P93@3024:3050, P94@3052:3172, P95@3174:3205, P96@3207:3400, P97@3402:3428, P98@3430:3610, P99@3612:3635, P100@3637:3806, P101@3808:3836, P102@3838:3886, P103@3888:3911, P104@3913:4063, P105@4065:4098, P106@4100:4262 |
| V30 | L148C0 | P107@0:83 |
| V31 | L152C0 | P108@0:94 |
| V32 | L153C0 | P109@0:151, P110@153:211, P111@213:258, P112@260:680, P113@682:925, P114@927:980, P115@982:1059, P116@1061:1101 |
| V33 | L156C0 | P117@0:180, P118@182:232, P119@234:338, P120@340:380, P121@382:628, P122@630:873, P123@875:1028, P124@1030:1512, P125@1514:1653, P126@1655:1903, P127@1905:2153, P128@2155:3310, P129@3312:4396, P130@4398:4562, P131@4564:4827 |
| V34 | L160C0 | P132@0:865 |
| V35 | L161C0 | P133@0:458 |
| V36 | L165C0 | P134@0:1731 |
| V37 | L166C0 | P135@0:328 |
| V38 | L170C0 | P136@0:1120 |
| V39 | L171C0 | P137@0:107, P3@109:396, P4@399:499 |
| V40 | L197C0 | P138@0:37, P139@39:337, P140@339:573 |
| V41 | L206C0, L1593C0, L1981C0, L2120C0, L2376C0, L2641C0, L2748C0, L2928C0, L3091C0, L3261C0, L3509C0, L3692C0, L3744C0, L4016C0, L4151C0, L4282C0 | P141@0:225 |
| V42 | L218C0 | P142@0:44 |
| V43 | L221C0 | P143@0:88 |
| V44 | L224C0 | P144@0:77 |
| V45 | L227C0 | P145@0:1092 |
| V46 | L231C0 | P146@0:271 |
| V47 | L234C0 | P147@0:3931 |
| V48 | L237C0 | P148@0:4250 |
| V49 | L240C0 | P149@0:467 |
| V50 | L243C0 | P150@0:485 |

| V51 | L249C0 | P151@0:3296 |
| V52 | L252C0 | P152@0:12111 |
| V53 | L256C0 | P153@0:7008 |
| V54 | L260C0 | P154@0:2726 |
| V55 | L263C0 | P155@0:371, P156@373:563, P157@565:574 |
| V56 | L266C0 | P158@0:3945 |
| V57 | L269C0 | P159@0:12 |
| V58 | L271C0 | P160@0:306 |
| V59 | L274C0 | P161@0:694 |
| V60 | L277C0 | P162@0:826 |
| V61 | L280C0 | P163@0:552 |
| V62 | L282C0 | P164@0:2902 |
| V63 | L285C0 | P165@0:180 |
| V64 | L288C0 | P166@0:343, P167@345:675 |
| V65 | L310C0 | P168@0:59, P169@61:99, P170@101:251, P171@253:354, P172@356:379, P173@381:581, P174@583:818, P175@820:858, P176@860:1126, P177@1128:1150, P178@1152:1249, P179@1251:1282, P180@1284:1408, P181@1410:1500 |
| V66 | L314C0 | P182@0:88, P183@90:358, P184@360:702 |
| V67 | L317C0, L848C0, L1145C0, L1435C0, L1454C0, L1882C0, L2289C0, L2880C0, L2886C0, L2891C0, L2892C0, L4443C0, L4448C0, L4473C0 | P185@0:39, P6@41:215 |
| V68 | L319C0 | P186@0:120 |
| V69 | L321C0 | P187@0:218 |
| V70 | L324C0 | P188@0:86 |
| V71 | L326C0 | P189@0:8852 |
| V72 | L330C0 | P190@0:1029 |
| V73 | L332C0 | P191@0:48, P192@50:609, P193@611:645, P194@647:694, P195@696:722, P196@724:811, P197@813:949, P198@951:973, P199@975:1267, P200@1269:1290, P201@1292:1443, P202@1445:1468, P203@1470:1555, P204@1557:1761, P205@1763:1802, P206@1804:2003, P207@2005:2928, P208@2930:2941, P209@2943:3104 |
| V74 | L335C0 | P210@0:224 |
| V75 | L363C0 | P211@0:216 |
| V76 | L367C0 | P212@0:916, P213@918:1834, P214@1836:2753, P215@2755:3672, P216@3674:4599, P217@4601:5521, P218@5523:6437, P219@6439:7367 |
| V77 | L372C0 | P220@0:2577 |
| V78 | L376C0 | P221@0:8469 |
| V79 | L381C0 | P222@0:362 |
| V80 | L386C0 | P223@0:58, P224@60:104 |
| V81 | L389C0 | P225@0:432 |
| V82 | L391C0 | P226@0:530, P227@532:2363 |
| V83 | L424C0 | P228@0:1073 |
| V84 | L428C0 | P229@0:123 |
| V85 | L430C0 | P230@0:692 |
| V86 | L434C0 | P231@0:2342 |
| V87 | L438C0 | P232@0:1799 |
| V88 | L448C0 | P233@0:649 |
| V89 | L451C0 | P234@0:2234 |
| V90 | L480C0, L1265C0 | P235@0:9 |
| V91 | L484C0 | P236@0:5 |
| V92 | L486C0 | P237@0:14318 |
| V93 | L491C0 | P238@0:513 |
| V94 | L494C0 | P239@0:5 |
| V95 | L496C0 | P240@0:182 |
| V96 | L499C0 | P241@0:21772 |
| V97 | L503C0 | P242@0:140 |
| V98 | L506C0 | P243@0:127 |
| V99 | L527C0 | P244@0:27, P245@29:84, P246@86:108, P247@110:141, P248@143:159, P249@161:177, P250@179:230, P251@232:326, P252@328:456, P253@458:657, P254@659:684, P255@686:856, P256@858:995 |
| V100 | L531C0 | P257@0:635 |

| V101 | L533C0 | P258@0:273 |
| V102 | L563C0 | P259@0:83 |
| V103 | L569C0 | P260@0:260 |
| V104 | L570C0 | P261@0:988 |
| V105 | L574C0 | P262@0:4615 |
| V106 | L576C0 | P263@0:2250 |
| V107 | L581C0 | P264@0:259 |
| V108 | L586C0 | P265@0:1756 |
| V109 | L610C0 | P266@0:59, P267@61:78, P268@80:357, P269@359:546, P270@548:597, P271@599:800, P272@802:1090, P273@1092:1113, P274@1115:1409, P275@1411:1500 |
| V110 | L614C0 | P276@0:41, P277@43:99, P278@101:298, P279@300:333, P280@335:505, P281@507:537, P282@539:650, P283@652:836, P284@838:856, P285@858:1110, P286@1112:1422, P287@1424:1544, P80@1546:1549, P288@1551:1601, P289@1603:1609, P290@1611:1849, P291@1851:1996, P292@1998:3769, P80@3771:3774, P293@3776:3832, P294@3834:3880, P295@3882:4076, P80@4078:4081, P296@4083:4133, P297@4135:4591, P80@4593:4596, P298@4598:4654, P299@4656:4806, P300@4808:5518, P301@5520:5561, P302@5563:5665, P303@5667:5937, P80@5939:5942, P304@5944:5994, P305@5996:6562 |
| V111 | L615C0 | P306@0:51, P307@53:169, P308@171:367, P279@369:402, P280@404:574, P281@576:606, P282@608:719, P309@721:803, P310@805:906, P311@908:925, P285@927:1179, P312@1181:1490, P313@1492:1612, P80@1614:1617, P314@1619:1734, P289@1736:1742, P315@1744:1984, P291@1986:2131, P316@2133:2348, P317@2350:2752, P318@2754:2962, P319@2964:3391, P320@3393:3573, P80@3575:3578, P321@3580:3696, P322@3698:3743, P295@3745:3939, P80@3941:3944, P323@3946:4061, P324@4063:4521, P80@4523:4526, P325@4528:4644, P326@4646:4795, P300@4797:5507, P327@5509:5549, P302@5551:5653, P303@5655:5925, P80@5927:5930, P328@5932:6047, P329@6049:6615 |
| V112 | L624C0, L1891C0, L2241C0 | P330@0:1 |
| V113 | L625C0 | P331@0:310, P332@312:506 |
| V114 | L628C0 | P333@0:1451 |
| V115 | L631C0 | P334@0:872, P335@874:2034, P336@2036:2280, P337@2282:2406, P338@2408:2455, P339@2457:3347, P340@3349:3713, P341@3715:3773, P342@3775:3914, P343@3916:4033 |
| V116 | L634C0 | P344@0:2682 |
| V117 | L636C0 | P345@0:1643 |
| V118 | L639C0 | P346@0:594, P347@596:600, P348@602:732, P349@734:746, P350@748:1125, P351@1127:1139, P352@1141:1327, P353@1329:1340, P354@1342:1464, P355@1466:1475, P356@1477:1685, P357@1687:1831, P358@1833:1845, P359@1847:2653, P360@2655:2707, P361@2709:2814, P362@2816:2832, P363@2834:3398, P364@3400:3427, P365@3429:3652, P366@3654:3669, P367@3671:3772 |
| V119 | L642C0 | P368@0:2531 |
| V120 | L645C0 | P369@0:98 |
| V121 | L669C0 | P370@0:26, P371@28:136, P372@138:322, P373@324:567 |
| V122 | L675C0 | P374@0:98 |
| V123 | L679C0 | P375@0:874 |
| V124 | L685C0 | P376@0:76, P377@78:166, P378@168:725, P379@727:788 |
| V125 | L690C0 | P380@0:11 |
| V126 | L693C0 | P381@0:23, P382@26:5193 |
| V127 | L697C0 | P383@0:61, P384@64:4633, P385@4636:9134 |
| V128 | L700C0 | P386@0:107 |
| V129 | L705C0 | P387@0:53 |
| V130 | L709C0 | P388@0:343 |
| V131 | L711C0 | P389@0:447 |
| V132 | L715C0 | P390@0:502 |
| V133 | L718C0 | P391@0:3552 |
| V134 | L722C0 | P392@0:381, P393@383:1325 |
| V135 | L725C0 | P394@0:460, P395@462:1665 |
| V136 | L728C0 | P396@0:236, P397@238:779, P398@781:1020 |
| V137 | L732C0 | P399@0:1953 |
| V138 | L735C0 | P400@0:1433 |
| V139 | L738C0 | P401@0:6422 |
| V140 | L742C0 | P402@0:318 |
| V141 | L745C0 | P403@0:2607 |
| V142 | L770C0 | P404@0:27, P405@29:89, P406@91:142, P407@144:158, P408@160:172, P409@174:276, P410@278:505, P411@507:599, P412@601:705, P413@707:871, P414@873:1024 |

| V143 | L774C0 | P415@0:57, P416@59:242, P417@244:586 |
| V144 | L775C0 | P418@0:16, P419@18:252, P420@254:564, P421@566:660, P422@662:748, P423@750:871 |
| V145 | L777C0 | P424@0:30, P425@32:130, P426@132:158, P427@160:189, P428@191:212, P429@214:244, P430@246:498, P431@500:1823 |
| V146 | L780C0 | P432@0:863 |
| V147 | L784C0 | P433@0:513 |
| V148 | L785C0 | P434@0:201, P435@203:307, P436@309:603, P437@605:773 |
| V149 | L789C0 | P438@0:27, P439@29:120, P440@122:245, P441@247:501, P442@503:631 |
| V150 | L790C0 | P443@0:34, P444@36:106, P445@108:395, P446@397:642, P447@644:672, P448@674:793 |
| V151 | L794C0 | P449@0:220, P450@222:335, P451@337:431 |
| V152 | L795C0 | P452@0:39, P453@41:157, P454@159:456, P455@458:660, P456@662:761, P457@763:858 |
| V153 | L797C0 | P458@0:873 |
| V154 | L799C0 | P459@0:465 |
| V155 | L802C0 | P460@0:693 |
| V156 | L805C0 | P461@0:1794 |
| V157 | L808C0 | P462@0:407 |
| V158 | L811C0 | P463@0:80, P3@82:369, P4@372:472 |
| V159 | L813C0 | P464@0:83, P3@85:372, P4@375:475 |
| V160 | L816C0 | P465@0:4662 |
| V161 | L819C0 | P466@0:231, P467@233:282, P468@284:326, P469@328:385, P470@387:506, P471@508:567, P472@569:627, P473@629:721, P474@723:908, P475@914:1168, P476@1174:1638, P477@1640:1796, P478@1798:1992, P479@1994:2135, P480@2137:2334, P481@2336:2457, P482@2459:3025, P483@3027:3374, P484@3376:3692, P485@3694:3884, P486@3886:4045, P487@4047:4221 |
| V162 | L842C0 | P488@0:86, P3@88:375, P4@378:478 |
| V163 | L843C0 | P489@0:53, P490@55:76, P491@78:223, P492@225:381, P493@383:406, P494@408:640, P495@642:673, P496@675:846, P497@848:1025, P498@1027:1202, P499@1204:1323, P500@1325:1501 |
| V164 | L849C0 | P501@0:170, P502@172:211, P503@213:408, P504@410:690, P505@692:854 |
| V165 | L853C0 | P506@0:64, P507@66:233, P508@235:426, P509@428:633, P510@635:738, P511@740:758, P499@760:879, P512@881:1040, P513@1042:1180, P514@1182:1305 |
| V166 | L854C0 | P515@0:37, P516@39:75, P517@77:217, P518@219:318, P519@320:410, P520@412:502, P521@504:603, P522@605:708, P523@710:729, P524@731:772, P525@774:957, P526@959:1187, P527@1189:1308 |
| V167 | L894C0 | P528@0:32 |
| V168 | L897C0 | P529@0:193 |
| V169 | L899C0 | P530@0:3473 |
| V170 | L902C0 | P531@0:214 |
| V171 | L904C0 | P532@0:6753 |
| V172 | L907C0 | P533@0:3698 |
| V173 | L910C0 | P534@0:579, P535@581:1137 |
| V174 | L913C0 | P536@0:96 |
| V175 | L915C0 | P537@0:718, P538@720:1438, P539@1440:2158, P540@2160:2884, P541@2886:3610, P542@3612:4336, P543@4338:5054, P544@5056:5772, P545@5774:6490, P546@6492:7208, P547@7210:7926, P548@7928:8644, P549@8646:8731 |
| V176 | L953C0 | P550@0:10 |
| V177 | L956C0 | P551@0:30 |
| V178 | L958C0 | P552@0:12895 |
| V179 | L961C0 | P553@0:671 |
| V180 | L963C0 | P554@0:340 |
| V181 | L965C0 | P555@0:237, P556@239:350, P557@406:423, P558@447:532, P559@550:557, P560@574:586, P561@592:608, P562@625:635, P563@646:659, P564@663:725, P565@732:813, P566@820:900, P567@907:976, P568@990:1002, P569@1006:1079, P570@1086:1159, P571@1166:1243, P572@1250:1328, P573@1342:1357, P574@1361:1386, P575@1393:1413, P576@1420:1438, P577@1454:1465, P578@1476:1486, P579@1490:1518, P580@1525:1561, P581@1568:1584, P582@1591:1619, P583@1626:1657, P584@1671:1689, P585@1693:1715, P586@1722:1748, P587@1755:1784, P588@1791:1817, P589@1831:1840, P575@1844:1864, P590@1871:1901, P591@1908:1950, P592@1957:1972, P593@1979:2017, P594@2024:2046, P595@2053:2088, P596@2095:2117, P597@2131:2139, P598@2143:2156, P599@2163:2183, P600@2190:2212, P601@2219:2244, P602@2251:2274 |
| V182 | L968C0 | P603@0:29 |
| V183 | L970C0 | P604@0:7108 |
| V184 | L973C0 | P605@0:116 |
| V185 | L976C0 | P606@0:2052 |
| V186 | L981C0, L995C0, L2004C0, L2012C0, L2409C0, L2424C0 | P607@0:2 |
| V187 | L986C0 | P608@0:958 |
| V188 | L989C0 | P609@0:344 |
| V189 | L992C0 | P610@0:121 |
| V190 | L1000C0 | P611@0:2256 |
| V191 | L1029C0, L3027C0, L3098C0 | P612@0:20 |
| V192 | L1033C0 | P613@0:28 |

| V193 | L1035C0, L1316C0, L4287C0 | P614@0:11217 |
| V194 | L1038C0, L1319C0, L4290C0 | P615@0:755 |
| V195 | L1040C0 | P616@0:430 |
| V196 | L1042C0 | P617@0:234, P618@236:2273 |
| V197 | L1045C0 | P619@0:236, P620@238:2266 |
| V198 | L1048C0 | P621@0:377 |
| V199 | L1050C0 | P622@0:5856 |
| V200 | L1060C0 | P623@0:1165 |
| V201 | L1061C0 | P624@0:14 |
| V202 | L1064C0 | P625@0:399 |
| V203 | L1067C0 | P626@0:20, P627@58:76, P628@187:211, P629@307:533 |
| V204 | L1069C0 | P630@0:188 |
| V205 | L1072C0 | P631@0:193 |
| V206 | L1094C0 | P632@0:40, P633@42:75, P634@77:191, P635@193:459, P636@461:539, P637@541:617, P638@619:660, P639@662:846, P640@848:1043, P641@1045:1184, P642@1186:1292, P643@1294:1387, P644@1389:1421, P645@1423:1558, P646@1560:1717, P647@1719:1902, P648@1904:1951, P649@1953:2207, P650@2209:2431, P651@2433:2569, P652@2571:2708, P653@2710:2842, P654@2844:2887, P655@2889:3018, P656@3020:3273, P657@3275:3475, P80@3477:3480, P658@3482:3573 |
| V207 | L1098C0 | P659@0:58, P660@60:112, P491@114:259, P661@261:289, P662@291:385, P663@387:439, P664@441:505 |
| V208 | L1101C0 | P665@0:31, P666@33:67, P667@69:275, P668@277:304, P669@306:435, P670@437:460, P671@462:511, P672@513:626, P673@628:772 |
| V209 | L1125C0 | P674@0:28, P675@30:94, P676@96:338, P677@340:593, P678@595:832 |
| V210 | L1126C0 | P679@0:33, P680@35:56, P681@58:301, P682@303:331, P683@333:566, P684@568:588, P685@590:862 |
| V211 | L1130C0 | P686@0:18, P687@20:306, P688@308:325, P689@327:428 |
| V212 | L1131C0 | P443@0:34, P690@36:105, P69@107:154, P691@156:329, P692@331:495, P693@497:711, P694@713:831, P695@833:882, P696@884:1024, P80@1026:1029, P697@1031:1061, P698@1063:1195 |
| V213 | L1134C0 | P699@0:90, P3@92:379, P4@382:482 |
| V214 | L1136C0 | P700@0:130 |
| V215 | L1140C0 | P701@0:18, P702@20:776, P703@778:809, P704@811:1086 |
| V216 | L1141C0 | P705@0:50, P706@52:517, P707@519:562, P708@564:735, P709@737:948 |
| V217 | L1146C0 | P710@0:26, P711@28:100, P712@102:245, P713@247:355 |
| V218 | L1149C0 | P714@0:81 |
| V219 | L1151C0 | P715@0:2975 |
| V220 | L1154C0 | P716@0:311 |
| V221 | L1156C0 | P717@0:3539 |
| V222 | L1159C0 | P718@0:4434 |
| V223 | L1162C0 | P719@0:267, P720@269:285, P721@287:387, P722@389:416, P723@418:577, P724@579:597, P725@599:734, P726@736:776, P727@778:1269, P728@1271:1306, P729@1308:1403, P730@1405:1413, P731@1415:1553, P732@1555:1884, P733@1886:2638, P734@2640:2650, P735@2652:2706, P736@2708:2724, P737@2726:2896, P738@2898:3031, P739@3033:3135, P740@3137:3257, P741@3259:3272, P742@3274:3331 |
| V224 | L1164C0 | P743@0:836 |
| V225 | L1167C0 | P744@0:464, P483@466:813, P484@815:1131, P485@1133:1323, P486@1325:1484, P487@1486:1660, P745@1662:1946 |
| V226 | L1191C0 | P746@0:40, P747@42:72, P748@74:388, P749@390:612, P750@614:626, P751@628:746, P752@748:866, P753@868:896, P754@898:1027, P755@1029:1136, P756@1138:1157, P757@1159:1265, P758@1267:1364 |
| V227 | L1196C0 | P759@0:56, P760@58:225, P761@227:295, P762@297:433, P763@435:583, P764@585:670, P765@672:717, P766@719:867, P767@869:1037, P768@1039:1136 |
| V228 | L1197C0 | P769@0:46, P770@48:251, P771@253:631, P772@633:813, P773@815:1014, P774@1016:1199, P775@1201:1303, P776@1305:1400 |
| V229 | L1200C0 | P777@0:27, P778@29:76, P779@78:183, P780@185:215, P781@217:408, P782@410:639, P783@641:655, P784@657:712 |
| V230 | L1221C0 | P785@0:63, P786@65:121, P787@123:175, P788@177:219, P789@221:285, P790@287:394, P791@396:422, P80@424:427, P792@429:481, P793@483:768, P80@770:773, P794@775:823, P795@825:930, P80@932:935, P796@937:952, P797@954:1027, P80@1029:1032, P798@1034:1062, P799@1064:1148, P800@1150:1315, P801@1317:1559 |
| V231 | L1226C0 | P802@0:43, P803@45:1319, P804@1321:1364, P805@1366:1421, P806@1423:1559, P807@1561:1678 |
| V232 | L1227C0 | P808@0:58, P809@60:82, P810@84:474, P811@476:501, P812@503:626, P813@628:666, P814@668:852, P815@854:948, P816@950:1135, P817@1137:1292, P818@1294:1310, P819@1312:1427, P820@1429:1522 |
| V233 | L1232C0 | P821@0:32, P822@34:79, P823@81:250, P824@252:391, P825@393:508, P826@510:648, P827@650:696, P828@698:862, P829@864:1000, P830@1002:1229, P831@1231:1262, P832@1264:1380, P833@1382:1414, P834@1416:1501 |
| V234 | L1233C0 | P835@0:59, P836@61:96, P837@98:255, P838@257:351, P839@353:531, P840@533:562, P841@564:764, P842@766:941, P843@943:981, P844@983:1317, P845@1319:1543 |
| V235 | L1237C0 | P846@0:47, P847@49:70, P848@72:210, P849@212:420, P850@422:780, P851@782:852, P852@854:1248, P853@1250:1378, P854@1380:1535, P855@1537:1638, P856@1640:1668, P857@1670:1732, P858@1734:1817 |
| V236 | L1270C0 | P859@0:875 |
| V237 | L1272C0 | P860@0:1739 |
| V238 | L1275C0 | P861@0:1478 |
| V239 | L1278C0 | P862@0:1489 |
| V240 | L1281C0 | P863@0:2868 |
| V241 | L1310C0 | P864@0:29 |
| V242 | L1313C0, L3101C0 | P865@0:4 |

| V243 | L1321C0 | P866@0:125 |
| V244 | L1324C0 | P867@0:5652 |
| V245 | L1327C0 | P868@0:200 |
| V246 | L1330C0 | P869@0:163 |
| V247 | L1332C0 | P870@0:1079 |
| V248 | L1334C0 | P871@0:1539 |
| V249 | L1337C0 | P872@0:6369 |
| V250 | L1340C0 | P873@0:1247 |
| V251 | L1343C0 | P874@0:301, P875@303:354, P876@356:592, P877@594:697, P878@699:1056, P879@1059:1101, P880@1103:1663, P881@1665:1696, P882@1698:1952, P883@1954:2056, P884@2058:2358, P885@2360:2469, P886@2471:2605, P887@2607:2624, P888@2626:2921, P889@2923:2932, P890@2934:3459, P891@3461:3475, P892@3477:3731, P893@3733:3748, P894@3750:4023, P895@4025:4200, P896@4202:4223, P897@4225:4495 |
| V252 | L1346C0 | P898@0:70 |
| V253 | L1348C0 | P899@0:7207 |
| V254 | L1351C0 | P900@0:613 |
| V255 | L1354C0 | P901@0:1400 |
| V256 | L1359C0 | P902@0:23 |
| V257 | L1361C0 | P903@0:900 |
| V258 | L1389C0 | P904@0:97 |
| V259 | L1392C0 | P905@0:4161 |
| V260 | L1396C0 | P906@0:62, P907@64:3105 |
| V261 | L1399C0 | P908@0:266, P909@268:2173, P910@2175:3974 |
| V262 | L1402C0 | P911@0:1591 |
| V263 | L1429C0 | P912@0:102, P3@104:391, P4@394:494 |
| V264 | L1430C0 | P913@0:45, P914@47:210, P915@212:236, P916@238:425, P917@427:528, P918@530:551, P919@553:823, P920@825:846, P921@848:935, P922@937:961, P923@963:1087 |
| V265 | L1436C0 | P924@0:27, P925@29:93, P926@95:289, P927@291:483, P928@485:524, P929@526:709, P930@711:847, P931@849:892, P932@894:1013, P933@1015:1172, P934@1174:1204, P935@1206:1250, P936@1252:1409, P937@1411:1504 |
| V266 | L1439C0 | P938@0:877 |
| V267 | L1441C0 | P939@0:160, P940@162:406 |
| V268 | L1445C0 | P941@0:1429, P942@1432:4087, P943@4090:4471, P944@4474:4598, P945@4601:4712, P946@4715:4791, P947@4794:5110, P948@5113:5189 |
| V269 | L1446C0 | P949@0:184, P950@186:224 |
| V270 | L1449C0 | P951@0:143 |
| V271 | L1452C0 | P952@0:141 |
| V272 | L1479C0 | P953@0:24, P954@26:127, P955@129:386, P956@388:668, P957@670:747, P958@749:859 |
| V273 | L1483C0 | P959@0:31, P687@33:319, P960@321:428, P961@430:465, P962@467:682 |
| V274 | L1484C0 | P443@0:34, P963@36:100, P964@102:350, P965@352:703 |
| V275 | L1488C0 | P966@0:96, P3@98:385, P4@388:488 |
| V276 | L1489C0 | P967@0:36, P968@38:347, P969@349:368, P970@370:580, P971@582:596, P972@598:685, P973@687:856, P974@858:870, P975@872:1129, P976@1131:1148, P977@1150:1290, P978@1292:1355 |
| V277 | L1493C0 | P979@0:32, P980@34:76, P981@78:528, P982@530:571, P983@573:798 |
| V278 | L1494C0 | P984@0:280 |
| V279 | L1497C0 | P985@0:540 |
| V280 | L1500C0 | P986@0:389, P987@391:549, P988@551:693, P989@695:710, P990@712:1578, P991@1580:1616, P992@1618:1624, P80@1626:1629, P732@1631:1960, P733@1962:2714, P993@2716:2728, P994@2730:2960, P995@2962:3142, P996@3144:3158, P997@3160:3289, P720@3291:3307, P721@3309:3409, P722@3411:3438, P723@3440:3599, P724@3601:3619, P725@3621:3756, P726@3758:3798, P727@3800:4291, P728@4293:4328, P998@4330:4336, P999@4338:4437 |
| V281 | L1524C0 | P1000@0:26, P1001@28:223, P1002@225:276, P1003@278:518, P1004@520:657, P1005@659:915 |
| V282 | L1525C0 | P404@0:27, P1006@29:65, P994@67:297, P1007@299:323, P1008@325:460, P1009@462:489, P1010@491:660, P1011@662:683, P1012@685:1068 |
| V283 | L1528C0 | P1013@0:30 |
| V284 | L1532C0 | P1014@0:1038 |
| V285 | L1533C0 | P1015@0:3016 |
| V286 | L1537C0 | P1016@0:1610 |
| V287 | L1538C0 | P1017@0:39, P67@41:114, P68@116:196, P69@198:245, P70@247:469, P71@471:555, P72@557:609, P73@611:811, P74@813:1011, P75@1013:1094, P76@1096:1120, P77@1122:1250, P78@1252:1309, P79@1311:1470, P80@1472:1475, P81@1477:1501, P1018@1503:1689, P83@1691:1823, P84@1825:1902, P85@1904:1932, P86@1934:2255, P87@2257:2283, P88@2285:2527, P89@2529:2565, P90@2567:2767, P91@2769:2789, P92@2791:3014, P93@3016:3042, P94@3044:3164, P95@3166:3197, P96@3199:3392, P97@3394:3420, P98@3422:3602, P99@3604:3627, P100@3629:3798, P101@3800:3828, P102@3830:3878, P103@3880:3903, P104@3905:4055, P105@4057:4090, P106@4092:4254 |
| V288 | L1542C0 | P1019@0:136, P989@138:153, P990@155:1021, P991@1023:1059, P992@1061:1067, P80@1069:1072, P732@1074:1403, P733@1405:2157, P993@2159:2171, P994@2173:2403, P995@2405:2585, P1020@2587:2793, P80@2795:2798, P1021@2800:2814, P1022@2816:2865, P1023@2867:3066, P735@3068:3122, P1024@3124:3146, P1025@3148:3232, P1026@3234:3301, P1027@3303:3386, P1028@3388:3437, P1029@3439:3499, P1030@3501:3535, P1031@3537:3873, P1032@3875:3893, P1033@3895:4082, P1034@4084:4168, P1035@4170:4255, P1036@4257:4357, P1037@4359:4449, P1038@4451:4575, P80@4577:4580, P1039@4582:4599, P1040@4601:4718, P80@4720:4723, P1041@4725:4741, P1042@4743:4824, P1043@4826:4837, P1044@4839:4898, P1045@4900:5007, P1046@5009:5029, P1047@5031:5300, P730@5302:5310, P731@5312:5450, P732@5452:5781, P733@5783:6535, P734@6537:6547, P735@6549:6603, P736@6605:6621, P737@6623:6793, P738@6795:6928, P739@6930:7032, P740@7034:7154, P741@7156:7169, P742@7171:7228, P1048@7230:9101, P1049@9103:9117, P1050@9119:9220 |
| V289 | L1543C0 | P1051@0:237, P1052@239:1545, P1053@1547:5972 |
| V290 | L1547C0 | P1054@0:93, P720@95:111, P721@113:213, P722@215:242, P723@244:403, P724@405:423, P1055@425:502, P1056@504:583, P1057@585:599, P1058@601:731, P1059@733:899, P1060@901:919, P1061@921:1033, P1062@1035:1197, P1063@1199:1217, P1064@1219:1343, P1065@1345:1402, P726@1404:1444, P727@1446:1937, P728@1939:1974, P1066@1976:2036, P1067@2038:2295, P1068@2297:2454, P1069@2456:2617, P1070@2619:2670 |
| V291 | L1548C0 | P1071@0:170 |
| V292 | L1553C0 | P1072@0:30, P711@32:104, P1073@106:364 |

| V293 | L1556C0 | P1074@0:101, P3@103:390, P4@393:493 |
| V294 | L1559C0 | P1075@0:142, P1076@144:414, P1077@416:800, P1078@802:972 |
| V295 | L1585C0 | P370@0:26, P1079@28:110, P1080@112:407, P1081@409:574 |
| V296 | L1600C0 | P1082@0:4 |
| V297 | L1610C0 | P1083@0:55, P1084@57:199, P1085@201:334, P1086@336:524 |
| V298 | L1614C0 | P1087@0:144, P1088@146:273, P1089@275:468, P1090@470:557 |
| V299 | L1615C0 | P1091@0:201, P1092@203:432, P1093@434:618 |
| V300 | L1618C0 | P1094@0:20 |
| V301 | L1621C0 | P1095@0:83 |
| V302 | L1624C0 | P1096@0:1784 |
| V303 | L1627C0 | P1097@0:142 |
| V304 | L1630C0 | P1098@0:4351 |
| V305 | L1633C0 | P1099@0:972 |
| V306 | L1636C0 | P1100@0:30 |
| V307 | L1638C0, L1678C0, L2151C0, L2414C0, L2419C0, L3333C0, L4175C0 | P1101@0:31 |
| V308 | L1641C0 | P1102@0:552 |
| V309 | L1644C0 | P1103@0:355 |
| V310 | L1646C0 | P1104@0:2425 |
| V311 | L1649C0 | P1105@0:8054 |
| V312 | L1652C0 | P1106@0:6194 |
| V313 | L1655C0 | P1107@0:8722 |
| V314 | L1658C0 | P1108@0:1208 |
| V315 | L1661C0 | P1109@0:253, P1110@255:292 |
| V316 | L1664C0 | P392@0:381, P1111@383:410 |
| V317 | L1667C0 | P1112@0:1338 |

| V318 | L1670C0 | P1113@0:4502 |
| V319 | L1673C0 | P1114@0:1261 |
| V320 | L1675C0 | P1115@0:125 |
| V321 | L1681C0 | P1116@0:2278 |
| V322 | L1684C0 | P1117@0:655 |
| V323 | L1687C0 | P1118@0:485 |
| V324 | L1690C0 | P1119@0:126 |
| V325 | L1692C0 | P1120@0:6869 |
| V326 | L1695C0 | P1121@0:336, P1122@338:6253 |
| V327 | L1698C0 | P1123@0:1084 |
| V328 | L1701C0 | P1124@0:1478 |
| V329 | L1704C0 | P1125@0:490 |
| V330 | L1706C0 | P1126@0:13380 |
| V331 | L1709C0 | P1127@0:3 |
| V332 | L1711C0 | P1128@0:808, P1129@810:1535, P1130@1537:2272, P1131@2274:3009 |
| V333 | L1733C0 | P1132@0:40, P1133@42:146, P1134@148:183, P80@185:188, P1135@190:214, P1136@216:386, P1137@388:539, P1138@541:710, P1139@712:846, P80@848:851, P1140@853:876, P1141@878:951, P80@953:956, P1142@958:978, P1143@980:1197 |
| V334 | L1737C0 | P1144@0:3018 |
| V335 | L1739C0 | P1145@0:5826 |
| V336 | L1763C0 | P1146@0:78, P3@80:367, P4@370:470 |
| V337 | L1778C0 | P1147@0:4891 |
| V338 | L1780C0 | P1148@0:5508 |
| V339 | L1785C0 | P1149@0:107 |
| V340 | L1786C0 | P1150@0:18 |
| V341 | L1789C0 | P1151@0:664, P1152@667:1216, P1153@1219:2065, P1154@2068:2103, P1155@2105:2504 |
| V342 | L1793C0 | P1156@0:2500 |

| V343 | L1795C0 | P1157@0:575 |
| V344 | L1798C0 | P1158@0:2438 |
| V345 | L1801C0 | P1159@0:4842 |
| V346 | L1830C0 | P1160@0:98 |
| V347 | L1835C0 | P1161@0:264 |
| V348 | L1839C0 | P1162@0:5022 |
| V349 | L1842C0 | P1163@0:2930, P1164@2933:15032 |
| V350 | L1845C0 | P1165@0:253 |
| V351 | L1847C0 | P1166@0:54, P1167@57:6054 |
| V352 | L1850C0 | P1168@0:5664 |
| V353 | L1853C0 | P1169@0:643, P1170@645:1219, P1171@1221:1795, P1172@1797:2379, P1173@2381:2956, P1174@2958:3533, P1175@3535:4110, P1176@4112:4687, P1177@4689:5264, P1178@5266:5848, P1179@5850:6431, P1180@6433:7007, P1181@7009:7583, P1182@7585:8159, P1183@8161:8749, P1184@8751:9334, P1185@9336:9912, P1186@9914:10490, P1187@10492:10505 |
| V354 | L1877C0 | P1188@0:110, P3@112:399, P4@402:502 |
| V355 | L1878C0 | P1189@0:34, P1190@36:74, P1191@76:167, P1192@169:324, P1193@326:524, P1194@526:545, P1195@547:827, P1196@829:963 |
| V356 | L1883C0 | P1197@0:48, P1198@50:76, P1199@78:189, P1200@191:657, P1201@659:681, P1202@683:750, P1203@752:905, P1204@907:951, P1205@953:1230 |
| V357 | L1887C0 | P1206@0:26, P1207@28:64, P1208@66:264, P1209@266:304, P1210@306:580, P1211@582:900 |
| V358 | L1888C0 | P1212@0:45, P1213@47:83, P1214@85:246, P1215@248:271, P1216@273:469, P1217@471:495, P929@497:680, P1218@682:930, P1219@932:1033, P1220@1035:1062, P1221@1064:1298 |
| V359 | L1894C0 | P1222@0:542 |
| V360 | L1897C0 | P1223@0:251, P1224@253:360, P1225@362:400, P1226@402:1118, P1227@1120:1580, P1228@1582:1892, P1229@1894:2195, P1230@2197:2214, P1231@2216:2584, P1232@2586:2850, P1233@2852:3296, P1234@3298:3670 |
| V361 | L1900C0 | P1235@0:498 |
| V362 | L1930C0 | P1236@0:20 |
| V363 | L1934C0 | P1237@0:552 |
| V364 | L1937C0 | P1238@0:2285 |
| V365 | L1941C0 | P1239@0:7069 |
| V366 | L1944C0 | P1240@0:346 |
| V581 | L3281C0 | P1167@0:5997 |

| V367 | L1947C0 | P1241@0:799, P1242@801:4269, P1243@4271:9304 |
| V368 | L1951C0 | P1244@0:2800 |
| V369 | L1983C0 | P1245@0:27782 |
| V370 | L1987C0 | P1246@0:4184 |
| V371 | L1988C0 | P1247@0:27524 |

| V372 | L1991C0 | P1248@0:27870 |
| V373 | L1996C0 | P1249@0:2245 |
| V374 | L1999C0 | P1250@0:4215 |
| V375 | L2036C0, L2041C0, L2505C0, L3622C0, L4355C0 | P1251@0:50 |
| V376 | L2040C0, L3628C0, L4360C0 | P1252@0:50 |
| V377 | L2046C0 | P1253@0:235, P1254@237:366, P1255@368:575 |
| V378 | L2051C0 | P1256@0:134, P1257@136:315, P1258@317:484 |
| V379 | L2056C0 | P1259@0:3694 |
| V380 | L2059C0 | P1260@0:6588 |
| V381 | L2081C0 | P1261@0:55, P1262@57:87, P1263@89:339, P1264@341:512, P1265@514:717, P1266@719:742, P1267@744:830, P1268@832:1011, P1269@1013:1035, P1270@1037:1125, P1271@1127:1205, P1272@1207:1222, P1273@1224:1349 |
| V382 | L2085C0 | P1274@0:54, P267@56:73, P1275@75:247, P1276@249:261, P1277@263:312, P1278@314:652, P1279@654:699, P1280@701:889, P1281@891:943, P1282@945:1076, P1283@1078:1118, P1284@1120:1340, P1285@1342:1379, P1286@1381:1503 |
| V383 | L2111C0 | P1287@0:32, P1288@34:194, P1289@196:398, P1290@400:533 |
| V384 | L2135C0 | P1291@0:44 |
| V385 | L2137C0 | P1292@0:89 |
| V386 | L2140C0 | P1293@0:2579 |
| V387 | L2143C0 | P1294@0:401 |

| V388 | L2145C0 | P1295@0:3855 |
| V389 | L2148C0 | P1296@0:124 |
| V390 | L2154C0 | P1297@0:449 |
| V391 | L2156C0 | P1298@0:919 |
| V392 | L2161C0 | P1299@0:2579 |
| V393 | L2164C0 | P1300@0:1842 |
| V394 | L2167C0 | P1301@0:2413 |
| V395 | L2173C0 | P1302@0:3691 |
| V396 | L2176C0 | P1303@0:315, P1304@317:1406 |
| V397 | L2179C0 | P1305@0:637 |
| V398 | L2182C0 | P1306@0:203 |
| V399 | L2185C0 | P1307@0:1239, P1308@1241:4536 |
| V400 | L2207C0 | P1309@0:53, P1310@55:101, P1311@103:123, P1312@125:588, P80@590:593, P1313@595:869, P1314@871:1113, P80@1115:1118, P1315@1120:1364, P80@1366:1369, P1316@1371:1855, P1317@1857:1971, P80@1973:1976, P1318@1978:2278, P80@2280:2283, P1319@2285:2666 |
| V401 | L2210C0 | P1320@0:569 |
| V402 | L2213C0 | P1321@0:692, P281@694:724, P1322@726:833, P1323@835:1011, P311@1013:1030, P1324@1033:1283, P1325@1286:1593, P1326@1596:2335, P1327@2337:3754, P1328@3756:6030, P1329@6033:6133, P1330@6136:6404, P1331@6407:7202 |
| V403 | L2222C0 | P1332@0:349 |
| V404 | L2225C0 | P1333@0:969 |
| V405 | L2228C0 | P1334@0:696 |
| V406 | L2235C0 | P1335@0:4021 |
| V407 | L2243C0 | P1336@0:100 |
| V408 | L2246C0 | P1337@0:1327 |
| V409 | L2248C0 | P1338@0:619 |
| V410 | L2251C0 | P1339@0:154 |
| V411 | L2276C0 | P1340@0:94, P3@96:383, P4@386:486 |
| V412 | L2277C0 | P1341@0:49, P1342@51:82, P491@84:229, P1343@231:343, P1344@345:381, P496@383:554, P1345@556:740, P1346@742:923, P1347@925:1023, P1348@1025:1226 |
| V413 | L2282C0 | P1349@0:303 |
| V414 | L2283C0 | P1350@0:23, P1351@25:53, P1352@55:331, P1353@333:455, P1354@457:782, P1355@784:899, P1356@901:1006, P80@1008:1011, P1357@1013:1054, P1358@1056:1169, P1359@1171:1413 |
| V415 | L2288C0 | P1360@0:3041 |
| V416 | L2294C0 | P1361@0:1474, P1362@1476:3320, P1363@3322:4285 |
| V417 | L2295C0 | P1364@0:28 |
| V418 | L2319C0 | P1365@0:84, P3@86:373, P4@376:476 |
| V419 | L2330C0 | P1366@0:183 |
| V420 | L2333C0 | P1367@0:7998 |
| V421 | L2337C0 | P1368@0:178, P1369@180:14652 |
| V422 | L2340C0 | P1370@0:371, P1371@373:836, P1372@838:1186, P1373@1188:1830, P1374@1832:3087 |
| V423 | L2343C0 | P1375@0:384 |
| V424 | L2346C0 | P1376@0:630 |
| V425 | L2378C0 | P1377@0:28741 |
| V426 | L2381C0 | P1378@0:12760 |
| V427 | L2384C0 | P1379@0:3485 |
| V428 | L2390C0 | P1380@0:162, P1381@164:250, P1382@252:357 |
| V429 | L2393C0 | P1383@0:41, P1384@43:159, P1385@161:277, P1386@279:385 |
| V430 | L2395C0 | P1387@0:3000 |
| V431 | L2398C0 | P1388@0:153 |
| V432 | L2401C0 | P1389@0:7697 |
| V433 | L2404C0 | P1390@0:876 |
| V434 | L2416C0 | P1391@0:11058 |
| V435 | L2421C0 | P1392@0:5 |
| V436 | L2429C0 | P1393@0:23 |
| V437 | L2434C0 | P1394@0:78 |

| V438 | L2463C0 | P1395@0:100 |
| V439 | L2468C0 | P1396@0:4192 |
| V440 | L2473C0 | P1397@0:7258 |
| V441 | L2496C0 | P1398@0:37, P1399@39:366, P1400@368:403, P1401@405:684, P1402@686:1035, P1403@1037:1126, P1404@1128:1511, P1405@1513:1731 |
| V442 | L2500C0 | P1406@0:21, P1407@23:57, P1408@59:1019, P80@1021:1024, P1409@1026:1068, P1410@1070:1095, P1411@1097:1258, P1412@1260:1388, P1413@1390:1499, P1414@1501:1610, P1415@1612:1786, P1416@1788:1900, P80@1902:1905, P1417@1907:1952, P1418@1954:2016, P1419@2018:2072, P1420@2074:2192, P1421@2194:2307 |
| V443 | L2506C0 | P1422@0:47, P6@49:223 |
| V444 | L2510C0 | P1423@0:984 |
| V445 | L2511C0 | P1424@0:803 |
| V446 | L2516C0 | P1425@0:1352 |
| V447 | L2541C0 | P1426@0:52, P1427@54:81, P1428@83:268, P1429@270:456, P1430@458:602, P1431@604:791, P1432@793:1112, P1433@1114:1335, P1434@1337:1470 |
| V448 | L2547C0 | P1435@0:43, P1436@45:121, P1437@123:691, P80@693:696, P1438@698:745, P1439@747:797, P1440@799:1143 |
| V449 | L2553C0 | P1441@0:53, P1442@55:141, P1443@143:506, P1444@508:534, P1445@536:839 |
| V450 | L2558C0 | P415@0:57, P1446@59:311, P1447@313:762 |
| V451 | L2563C0 | P1448@0:197, P1449@199:376, P1450@378:586, P1451@588:800, P1452@802:879 |
| V452 | L2564C0 | P1453@0:52, P1454@54:152, P1455@154:362, P1456@364:606, P1457@608:856, P1458@858:1087, P1459@1089:1305, P1460@1307:1530, P1461@1532:1807, P1462@1809:2137, P1463@2139:2487, P1464@2489:2690, P1465@2692:2906 |
| V453 | L2574C0 | P1466@0:17 |
| V454 | L2578C0 | P1467@0:616 |
| V455 | L2581C0 | P1468@0:157, P1469@159:441, P1470@443:1530 |
| V456 | L2585C0 | P1471@0:1081 |
| V457 | L2590C0 | P1472@0:25, P1473@27:186, P1474@188:371, P1475@373:717, P1476@719:926, P1477@928:1196 |
| V458 | L2591C0 | P1478@0:128 |
| V459 | L2596C0 | P1479@0:1256 |
| V460 | L2597C0 | P1480@0:34, P1481@36:90, P1482@92:105, P1483@107:156, P1484@158:200, P1485@202:248, P1486@250:475, P1487@477:570 |
| V461 | L2602C0 | P1488@0:19, P1489@21:73, P1490@75:139, P1491@141:173, P1492@175:206, P80@208:211, P1493@213:243, P1494@245:570 |
| V462 | L2603C0 | P1495@0:143, P1496@148:169, P1497@174:191, P1498@198:212, P1499@217:234, P1500@241:258, P1501@260:365, P1496@370:391, P1497@396:413, P1502@416:515, P1498@520:534, P1499@539:556, P1503@563:581, P1504@584:689, P1496@694:715, P1497@720:737, P1498@742:756, P1499@761:778, P1503@785:803, P1505@806:911, P1496@916:937, P1497@942:959, P1498@964:978, P1506@981:1187, P1507@1190:1206 |
| V463 | L2628C0 | P266@0:59, P1508@61:77, P1509@79:337, P1510@339:818, P1511@820:959, P1512@961:1253, P1513@1255:1411, P1514@1413:1682 |
| V464 | L2633C0 | P1515@0:24, P1516@26:106, P1437@108:676, P1517@678:1132 |
| V465 | L2647C0 | P415@0:57, P1518@59:255, P1519@257:582 |
| V466 | L2651C0 | P1520@0:16, P1521@18:197, P1522@199:303, P1523@305:395 |
| V467 | L2652C0 | P1524@0:42, P1525@44:73, P1526@75:487, P1527@489:509, P1528@511:668, P1529@670:687, P1530@689:983, P1531@985:1307, P1532@1309:1614 |
| V468 | L2666C0 | P1533@0:55, P1534@57:72, P1535@74:458, P1536@460:760, P1537@762:1097, P1538@1099:1113, P1539@1115:1350 |
| V469 | L2667C0 | P1540@0:33, P1541@35:70, P1542@72:94, P1543@96:341, P1544@343:483, P1545@485:607, P1546@609:701, P1547@703:851, P1548@853:944 |
| V470 | L2671C0 | P1549@0:10, P1550@12:145, P1551@147:312, P1552@314:584, P1553@586:747 |
| V471 | L2672C0 | P1554@0:39, P1555@41:73, P1556@75:86, P1557@88:353, P1558@355:568, P1559@570:717, P1560@719:787, P1561@789:958, P1562@960:973, P1563@975:1300 |
| V472 | L2676C0 | P1549@0:10, P1564@12:195, P1565@197:294, P1566@296:634 |
| V473 | L2677C0 | P1549@0:10, P1567@12:143, P1568@145:445, P1569@447:636, P1570@638:796 |
| V474 | L2681C0 | P1571@0:2400 |
| V475 | L2682C0 | P1572@0:491 |
| V476 | L2685C0 | P1573@0:179 |
| V477 | L2689C0 | P1574@0:261 |
| V478 | L2690C0 | P1575@0:44 |
| V479 | L2719C0 | P1576@0:75 |
| V480 | L2723C0 | P1577@0:3 |
| V481 | L2725C0 | P1578@0:6760 |
| V482 | L2731C0 | P1579@0:16287 |
| V483 | L2735C0 | P1580@0:439 |
| V484 | L2739C0 | P1581@0:3790 |
| V485 | L2742C0 | P1582@0:60 |
| V486 | L2745C0 | P1583@0:79 |
| V487 | L2754C0 | P1584@0:60 |

| V488 | L2755C0 | P1585@0:117 |
| V489 | L2758C0 | P1586@0:1120 |
| V490 | L2762C0 | P1587@0:123, P1588@125:205, P1589@207:421, P1590@423:518, P1591@520:859, P1592@861:1093, P1593@1095:1260, P1594@1263:1305, P1595@1307:1347, P1596@1349:1736, P1597@1738:1820, P1598@1822:1985, P1599@1987:2110, P1600@2112:2157, P1601@2159:2273, P1602@2275:2284, P1603@2286:2494, P1604@2496:2520, P1605@2522:2768, P1606@2770:2962 |
| V491 | L2763C0 | P1607@0:167 |
| V492 | L2767C0 | P1608@0:1271 |
| V493 | L2768C0 | P1609@0:5591 |
| V494 | L2774C0 | P1610@0:1060 |
| V495 | L2777C0 | P1611@0:355 |
| V496 | L2779C0 | P1612@0:17 |
| V497 | L2781C0 | P1613@0:25 |
| V498 | L2784C0 | P1614@0:882 |
| V499 | L2809C0 | P404@0:27, P1615@29:95, P1616@97:156, P1617@158:215, P1618@217:420, P1619@422:525, P1620@527:624, P1621@626:719, P1622@721:847, P1623@849:1066 |
| V500 | L2813C0 | P1624@0:31, P1625@33:68, P1626@70:132, P1627@134:165, P1628@167:192, P1629@194:212, P1630@214:238, P1631@240:276, P1632@278:314, P1633@316:351, P1634@353:372, P1635@374:471, P1636@473:497 |
| V501 | L2814C0 | P1637@0:25, P711@27:99, P1638@101:172, P1639@174:289, P1640@291:314, P1641@316:375, P1642@377:1427 |
| V502 | L2819C0 | P1643@0:118, P989@120:135, P990@137:1003, P991@1005:1041, P992@1043:1049, P80@1051:1054, P732@1056:1385, P733@1387:2139, P993@2141:2153, P994@2155:2385, P995@2387:2567, P1020@2569:2775, P80@2777:2780, P1021@2782:2796, P1022@2798:2847, P1023@2849:3048, P735@3050:3104, P1024@3106:3128, P1025@3130:3214, P1026@3216:3283, P1027@3285:3368, P1028@3370:3419, P1029@3421:3481, P1030@3483:3517, P1031@3519:3855, P1032@3857:3875, P1033@3877:4064, P1034@4066:4150, P1035@4152:4237, P1036@4239:4339, P1037@4341:4431, P1038@4433:4557, P80@4559:4562, P1039@4564:4581, P1040@4583:4700 |
| V503 | L2820C0 | P1644@0:225, P1645@227:573 |
| V504 | L2824C0 | P1646@0:36, P67@38:111, P68@113:193, P69@195:242, P70@244:466, P71@468:552, P72@554:606, P73@608:808, P74@810:1008, P75@1010:1091, P76@1093:1117, P77@1119:1247, P78@1249:1306, P79@1308:1467, P80@1469:1472, P81@1474:1498, P1647@1500:1676, P1648@1678:1817 |
| V505 | L2825C0 | P1649@0:227, P1650@229:514, P1651@516:704 |
| V506 | L2828C0 | P1652@0:631 |
| V507 | L2831C0 | P1653@0:493 |
| V508 | L2834C0 | P1654@0:159, P1655@161:172, P1656@174:1129, P1657@1131:1323, P1658@1325:1724, P1659@1726:1921, P80@1923:1926, P1660@1928:1941, P1661@1943:2307, P1662@2309:2494, P1663@2496:2546, P83@2548:2680, P84@2682:2759, P85@2761:2789, P86@2791:3112, P87@3114:3140, P1664@3142:3327 |
| V509 | L2874C0 | P1665@0:100, P3@102:389, P4@392:492 |
| V510 | L2875C0 | P1666@0:38, P1667@40:72, P1668@74:112, P1669@114:261, P1670@263:305, P1671@307:364, P1672@366:519, P1673@521:615, P1674@617:656, P1675@658:792, P1676@794:971, P1677@973:1139 |
| V511 | L2881C0 | P1678@0:43, P1679@45:149, P1680@151:320, P1681@322:610, P1682@612:644, P1683@646:859, P1684@861:1109 |
| V512 | L2887C0 | P1685@0:26, P1686@28:60, P1687@62:296, P1688@298:330, P1689@332:507, P1690@509:849, P1691@851:1044 |
| V513 | L2895C0 | P1692@0:52, P1693@54:255, P1694@257:309, P1695@311:415, P1696@417:533, P1697@535:658, P1698@660:758, P1699@760:906, P1700@908:952, P1701@954:1228, P1702@1230:1382 |
| V514 | L2920C0 | P1287@0:32, P1703@34:209, P1704@211:432, P1705@434:593, P1706@595:709 |
| V515 | L2941C0 | P1707@0:19 |
| V516 | L2943C0 | P1708@0:79 |
| V517 | L2949C0 | P1709@0:26 |
| V518 | L2952C0 | P1710@0:274 |
| V519 | L2954C0 | P1711@0:2804 |
| V520 | L2957C0 | P1712@0:1576 |
| V521 | L2960C0 | P1713@0:3, P1714@5:558, P1715@562:607, P1714@610:1163 |
| V522 | L2963C0 | P1716@0:8, P1717@10:563 |
| V523 | L2966C0 | P1718@0:6 |
| V524 | L2968C0 | P1719@0:76 |
| V525 | L2970C0 | P1720@0:342 |
| V526 | L2972C0 | P1721@0:1615 |
| V527 | L2975C0 | P1722@0:1471 |
| V528 | L2977C0 | P1723@0:34, P1724@36:91, P1725@94:158, P1726@160:215, P1727@217:398, P1728@400:472, P1729@474:539, P1730@541:645, P1731@647:707, P1732@710:817, P1733@819:1382, P1734@1384:1510, P1735@1512:1621, P1736@1623:1941 |
| V529 | L2982C0 | P1737@0:19, P1738@21:145, P1739@147:225, P1740@236:573, P1741@603:636, P1742@711:886, P1741@924:957, P1741@987:1020, P1742@1095:1270, P1741@1308:1341, P1741@1371:1404, P1742@1479:1654, P1741@1692:1725, P1741@1755:1788, P1742@1863:2038, P1741@2076:2109, P1741@2139:2172, P1742@2247:2422, P1741@2460:2493, P1741@2523:2556, P1742@2631:2806, P1741@2844:2877, P1741@2907:2940 |
| V530 | L2985C0 | P1743@0:247, P1744@249:496, P1745@498:747, P1746@749:1000, P1747@1002:1252, P1748@1254:1506, P1749@1508:1758, P1750@1760:2012, P1751@2014:2264, P1752@2266:2518, P1753@2520:2770, P1754@2772:3026, P1755@3028:3278, P1756@3280:3533, P1757@3535:3784, P1758@3786:4040, P1759@4042:4291, P1760@4293:4546, P1761@4548:4797, P1762@4799:5053, P1763@5055:5304, P1764@5306:5556, P1765@5558:5807, P1766@5809:6058, P1767@6060:6308, P1768@6310:6560, P1769@6562:6813, P1770@6815:7064, P1771@7066:7317, P1772@7319:7569, P1773@7571:7822, P1774@7824:8074, P1775@8076:8327, P1776@8329:8579, P1777@8581:8833, P1778@8835:9085, P1779@9087:9339, P1780@9341:9591, P1781@9593:9845, P1782@9847:10098, P1783@10100:10352, P1784@10354:10606, P1785@10608:10860, P1786@10862:11108, P1787@11110:11358, P1788@11360:11609, P1789@11611:11857, P1790@11859:12107, P1791@12109:12358, P1792@12360:12608 |
| V531 | L2988C0 | P1793@0:611, P1794@613:636, P1795@638:715, P1796@717:864, P1797@866:1068, P1798@1070:1130, P1799@1132:1159, P1800@1161:1300, P1801@1302:1485, P1802@1487:1501 |
| V532 | L2991C0 | P1803@0:1830 |
| V533 | L2994C0 | P1804@0:919 |
| V534 | L2997C0 | P1805@0:34, P1806@36:120, P1807@122:155, P1808@157:328, P1809@330:472, P1810@474:673, P1811@675:816, P1812@818:858, P1813@860:1327, P1814@1329:1473, P1815@1475:1594, P1816@1596:1743, P1817@1745:1950, P1818@1952:1978, P1819@1980:2130, P1820@2132:2372, P1821@2374:2497, P1822@2499:2538, P1823@2540:2756, P1824@2758:2784, P1825@2786:2934, P1826@2936:3073 |
| V535 | L3030C0 | P1827@0:3005 |
| V536 | L3033C0 | P1828@0:98 |
| V537 | L3036C0 | P1829@0:22891 |

| V538 | L3039C0 | P1830@0:159 |
| V539 | L3041C0 | P1831@0:976 |
| V540 | L3044C0 | P1832@0:4390 |
| V541 | L3047C0 | P1833@0:183 |
| V542 | L3049C0 | P1834@0:412, P1835@414:1722, P1836@1724:2388, P1837@2390:2755, P1838@2757:3244, P1839@3246:3639 |
| V543 | L3054C0 | P1840@0:2542 |
| V544 | L3057C0 | P1841@0:3312 |
| V545 | L3104C0 | P1842@0:2446 |
| V546 | L3107C0 | P1843@0:2546 |
| V547 | L3110C0 | P1844@0:291 |
| V548 | L3112C0 | P1845@0:92 |
| V549 | L3114C0 | P1846@0:6097 |
| V550 | L3117C0 | P1847@0:411 |
| V551 | L3120C0 | P1848@0:2307 |
| V552 | L3124C0 | P1849@0:44 |
| V553 | L3132C0 | P1850@0:977 |
| V554 | L3134C0 | P1851@0:240, P1852@242:731, P1853@734:768, P1854@770:926, P1855@928:1071, P1856@1073:1155, P1857@1157:1386, P1858@1388:1583, P1859@1586:1619, P1860@1621:1860, P1861@1862:1979, P1862@1981:2145, P1863@2147:2151, P1864@2154:2208, P1865@2210:2314, P1866@2316:2377, P1867@2379:2459, P1868@2461:2530 |
| V555 | L3138C0 | P1869@0:2816 |
| V556 | L3140C0 | P1870@0:257, P875@259:310, P876@312:548, P877@550:653, P878@655:1012, P879@1015:1057, P880@1059:1619, P881@1621:1652, P882@1654:1908, P883@1910:2012, P884@2014:2314, P885@2316:2425, P886@2427:2561, P887@2563:2580, P888@2582:2877, P889@2879:2888, P1871@2890:3260 |
| V557 | L3143C0 | P1872@0:2959 |
| V558 | L3172C0 | P1873@0:19 |
| V559 | L3177C0 | P1874@0:5 |
| V560 | L3180C0 | P1875@0:3587 |
| V561 | L3184C0 | P1876@0:6265 |
| V562 | L3186C0 | P1877@0:22 |
| V563 | L3190C0 | P1878@0:1357 |
| V564 | L3192C0 | P1879@0:312 |
| V565 | L3195C0 | P1880@0:4083 |
| V566 | L3197C0 | P1881@0:61 |
| V567 | L3202C0 | P1882@0:298 |
| V568 | L3203C0 | P1883@0:400 |
| V569 | L3207C0 | P1884@0:317 |
| V570 | L3208C0 | P1885@0:2139 |
| V571 | L3214C0 | P1886@0:264 |
| V572 | L3218C0 | P1887@0:1590 |
| V573 | L3220C0 | P1888@0:1587 |
| V574 | L3224C0 | P1889@0:1300 |
| V575 | L3225C0 | P1890@0:37 |
| V576 | L3228C0 | P1891@0:1429 |
| V577 | L3230C0 | P1892@0:881 |
| V578 | L3255C0 | P1287@0:32, P1893@34:248, P1894@250:499, P1895@501:705 |
| V579 | L3276C0 | P1896@0:86 |
| V580 | L3279C0 | P1897@0:187 |
| V582 | L3284C0 | P1898@0:762 |
| V583 | L3287C0 | P1899@0:10, P1900@13:8115 |
| V584 | L3292C0 | P1901@0:3 |
| V585 | L3295C0 | P1902@0:7 |
| V586 | L3297C0 | P1903@0:7 |
| V587 | L3300C0 | P1904@0:2903 |

| V588 | L3303C0 | P1905@0:23374 |
| V589 | L3306C0 | P1906@0:1150 |
| V590 | L3309C0 | P1907@0:2502 |
| V591 | L3312C0 | P1908@0:89 |
| V592 | L3315C0 | P1909@0:1239 |
| V593 | L3318C0 | P1910@0:2900 |
| V594 | L3321C0 | P1911@0:17749 |
| V595 | L3324C0 | P1912@0:236, P1913@238:299, P1914@301:359, P1915@361:2283 |
| V596 | L3327C0 | P1916@0:129 |
| V597 | L3330C0 | P1917@0:3000 |
| V598 | L3335C0 | P1918@0:43 |
| V599 | L3338C0 | P1919@0:384 |
| V600 | L3340C0 | P1920@0:4916 |
| V601 | L3343C0 | P1921@0:144 |
| V602 | L3346C0 | P1922@0:4853 |
| V603 | L3349C0 | P1923@0:1708 |
| V604 | L3352C0 | P1924@0:520 |
| V605 | L3355C0 | P1925@0:715, P1926@717:797, P1927@799:871 |
| V606 | L3358C0 | P1928@0:1006 |
| V607 | L3361C0 | P1929@0:68, P1930@70:150, P1931@152:232, P1932@234:575 |
| V608 | L3364C0 | P1933@0:956 |
| V609 | L3366C0 | P1934@0:2699 |
| V610 | L3369C0 | P1935@0:822 |
| V611 | L3372C0 | P1936@0:2240 |
| V612 | L3376C0 | P1937@0:305 |
| V613 | L3396C0 | P1938@0:48, P1939@50:109, P1940@111:481, P1941@483:567, P1942@569:644, P1943@646:814, P1944@816:927, P1945@929:1080, P80@1082:1085, P1946@1087:1210, P1947@1212:1331 |
| V614 | L3400C0 | P1948@0:43, P1949@45:104, P1950@106:396, P1951@398:473, P1952@475:584, P1953@586:835, P1954@837:1038 |
| V615 | L3408C0 | P1948@0:43, P1955@45:191, P80@193:196, P1956@198:207, P1957@209:302, P1958@304:316, P1959@318:458, P1960@460:500, P1961@502:549, P1962@551:750, P1963@752:851, P80@853:856, P1964@858:867, P1965@869:971, P1958@973:985, P1966@987:1167, P1967@1169:1223, P1968@1225:1300, P1969@1302:1487 |
| V616 | L3433C0 | P1970@0:58, P1971@60:135, P1972@137:361, P1973@363:379, P1974@381:429, P1975@431:655, P1976@657:974, P1977@976:1014, P1978@1016:1360, P1979@1362:1395, P1980@1397:1659, P1981@1661:1801 |
| V617 | L3439C0 | P1982@0:35, P1983@37:142, P80@144:147, P1984@149:392, P80@394:397, P1985@399:504, P80@506:509, P1986@511:633, P80@635:638, P1987@640:839, P80@841:844, P1988@846:1100 |
| V618 | L3444C0 | P1989@0:38, P297@40:496, P1990@498:536, P1991@538:1102 |
| V619 | L3449C0 | P415@0:57, P1992@59:327, P1993@329:712 |
| V620 | L3454C0 | P1994@0:52, P1995@54:154, P1996@156:247, P1997@249:526 |
| V621 | L3457C0 | P1998@0:479 |
| V622 | L3461C0 | P1999@0:1007 |
| V623 | L3462C0 | P2000@0:24, P2001@26:64, P2002@66:359, P2003@361:611, P2004@613:835, P2005@837:1095, P2006@1097:1300, P2007@1302:1459, P2008@1461:1654, P2009@1656:1764 |
| V624 | L3467C0 | P2010@0:53, P2011@55:371, P2012@373:663, P2013@665:975 |
| V625 | L3468C0 | P2014@0:255, P2015@257:316, P2016@318:426, P2017@428:480, P2018@484:522, P2019@524:2197, P2020@2199:2461, P2021@2463:2505, P2022@2508:2938 |
| V626 | L3491C0 | P2023@0:39 |
| V627 | L3495C0 | P2024@0:301 |
| V628 | L3496C0 | P2025@0:237, P2026@239:450 |
| V629 | L3500C0 | P2027@0:1036 |
| V630 | L3522C0 | P2028@0:32 |
| V631 | L3525C0 | P2029@0:1199 |
| V632 | L3531C0 | P2030@0:74, P2031@76:120, P2032@122:197, P2033@199:212 |
| V633 | L3534C0 | P2034@0:3152 |
| V634 | L3537C0 | P2035@0:421, P2036@423:844, P2037@846:1285, P2038@1287:1726, P2039@1728:2167, P2040@2169:2608, P2041@2610:3049, P2042@3051:3490, P2043@3492:3906, P2044@3908:4322, P2045@4324:4738, P2046@4740:5154, P2047@5156:5570, P2048@5572:5986, P2049@5988:6402, P2050@6404:6818, P2051@6820:7234, P2052@7236:7650, P2053@7652:8066, P2054@8068:8482, P2055@8484:8897, P2056@8899:9312, P2057@9314:9727, P2058@9729:10142, P2059@10144:10557, P2060@10559:10972, P2061@10974:11397, P2062@11399:11822, P2063@11824:12247 |
| V635 | L3540C0 | P2064@0:553, P2065@555:1078, P2066@1080:1601, P2067@1603:2124, P2068@2126:2647, P2069@2649:3170, P2070@3172:3699, P2071@3701:4228, P2072@4230:4757, P2073@4759:5286 |
| V636 | L3543C0 | P2074@0:443, P2075@445:888, P2076@890:1333, P2077@1335:1778, P2078@1780:2223, P2079@2225:2642, P2080@2644:3061, P2081@3063:3480, P2082@3482:3899, P2083@3901:4318, P2084@4320:4737, P2085@4739:5160, P2086@5162:5581, P2087@5583:6004, P2088@6006:6427, P2089@6429:6850, P2090@6852:7273, P2091@7275:7696, P2092@7698:8119, P2093@8121:8542, P2094@8544:8965 |
| V637 | L3546C0 | P2095@0:1077 |

| V638 | L3549C0 | P2096@0:179, P2097@181:508 |
| V639 | L3554C0 | P2098@0:510 |
| V640 | L3578C0 | P404@0:27, P2099@29:227, P2100@229:454, P2101@456:684, P2102@686:810, P2103@812:947, P2104@949:1039, P2105@1041:1157, P2106@1159:1221, P2107@1223:1365 |
| V641 | L3583C0 | P2108@0:35, P2109@37:101, P2110@103:357, P2111@359:735, P2112@737:845, P2113@847:997, P2114@999:1186 |
| V642 | L3584C0 | P2115@0:737 |
| V643 | L3589C0 | P2116@0:32, P2117@34:276, P2118@278:536, P2119@538:888, P2120@890:1075 |
| V644 | L3590C0 | P1643@0:118, P989@120:135, P990@137:1003, P991@1005:1041, P992@1043:1049, P80@1051:1054, P732@1056:1385, P733@1387:2139, P993@2141:2153, P994@2155:2385, P995@2387:2567, P1020@2569:2775, P80@2777:2780, P1021@2782:2796, P1022@2798:2847, P1023@2849:3048, P735@3050:3104, P1024@3106:3128, P1025@3130:3214, P1026@3216:3283, P1027@3285:3368, P1028@3370:3419, P1029@3421:3481, P1030@3483:3517, P1031@3519:3855, P1032@3857:3875, P1033@3877:4064, P1034@4066:4150, P1035@4152:4237, P1036@4239:4339, P1037@4341:4431, P1038@4433:4557, P80@4559:4562, P1039@4564:4581, P1040@4583:4700, P80@4702:4705, P1041@4707:4723, P1042@4725:4806, P1043@4808:4819, P1044@4821:4880, P1045@4882:4989, P1046@4991:5011, P2121@5013:5188 |
| V645 | L3594C0 | P2122@0:15, P67@17:90, P68@92:172, P69@174:221, P70@223:445, P71@447:531, P72@533:585, P73@587:787, P74@789:987, P75@989:1070, P76@1072:1096, P77@1098:1226, P78@1228:1285, P79@1287:1446, P80@1448:1451, P81@1453:1477, P2123@1479:1664, P83@1666:1798, P84@1800:1877, P85@1879:1907, P86@1909:2230, P87@2232:2258, P88@2260:2502, P89@2504:2540, P90@2542:2742, P91@2744:2764, P92@2766:2989, P93@2991:3017, P94@3019:3139, P95@3141:3172, P96@3174:3367, P97@3369:3395, P98@3397:3577, P99@3579:3602, P100@3604:3773, P101@3775:3803, P102@3805:3853, P103@3855:3878, P104@3880:4030, P105@4032:4065, P106@4067:4229 |
| V646 | L3599C0 | P2124@0:696, P1655@698:709, P1656@711:1666, P1657@1668:1860, P1658@1862:2261, P1659@2263:2458, P80@2460:2463, P1660@2465:2478, P1661@2480:2844, P1662@2846:3031, P2125@3033:3051, P2126@3053:6784, P2127@6786:6801, P2128@6803:9917 |
| V647 | L3600C0 | P2129@0:33, P1626@35:97, P2130@99:181, P2131@183:205, P2132@207:268, P2133@270:564, P2134@566:717, P2135@719:832, P2136@834:923 |
| V648 | L3633C0 | P2137@0:226, P2138@228:350, P2139@352:616, P2140@618:805 |
| V649 | L3637C0 | P2141@0:558 |
| V650 | L3638C0 | P2142@0:336 |
| V651 | L3641C0, L4384C0 | P2143@0:51 |
| V652 | L3643C0 | P2144@0:110 |
| V653 | L3646C0 | P2145@0:5 |
| V654 | L3648C0 | P2146@0:11812 |
| V655 | L3651C0 | P2147@0:8087 |
| V656 | L3655C0 | P2148@0:8145 |
| V657 | L3656C0 | P2149@0:51 |
| V658 | L3659C0 | P2150@0:5687 |
| V659 | L3662C0 | P2151@0:16275 |
| V660 | L3695C0 | P2152@0:27674 |
| V661 | L3696C0 | P2153@0:28225 |
| V662 | L3701C0 | P2154@0:3995 |
| V663 | L3702C0 | P2155@0:27953 |
| V664 | L3707C0 | P2156@0:1257 |
| V665 | L3712C0 | P2157@0:321 |
| V666 | L3743C0 | P2158@0:96 |
| V667 | L3747C0 | P2159@0:456 |
| V668 | L3750C0 | P2160@0:3944 |
| V669 | L3755C0 | P2161@0:2836 |
| V670 | L3758C0 | P2162@0:485 |
| V671 | L3761C0 | P2163@0:681 |
| V672 | L3785C0 | P2164@0:17, P2165@19:38, P2166@40:379, P2167@381:404, P2168@406:483, P2169@485:526, P2170@528:759, P2171@761:784, P2172@786:907 |
| V673 | L3786C0 | P2173@0:19, P2174@21:240, P2175@242:580, P2176@582:833, P2177@835:992 |
| V674 | L3790C0 | P2178@0:4541 |
| V675 | L3791C0 | P2179@0:99, P3@101:388, P4@391:491 |
| V676 | L3795C0 | P2180@0:133, P989@135:150, P990@152:1018, P991@1020:1056, P992@1058:1064, P80@1066:1069, P732@1071:1400, P733@1402:2154, P993@2156:2168, P994@2170:2400, P995@2402:2582, P1020@2584:2790, P80@2792:2795, P1021@2797:2811, P1022@2813:2862, P1023@2864:3063, P735@3065:3119, P1024@3121:3143, P1025@3145:3229, P1026@3231:3298, P1027@3300:3383, P1028@3385:3434, P1029@3436:3496, P1030@3498:3532, P1031@3534:3870, P1032@3872:3890, P1033@3892:4079, P1034@4081:4165, P1035@4167:4252, P1036@4254:4354, P1037@4356:4446, P1038@4448:4572, P80@4574:4577, P1039@4579:4596, P1040@4598:4715, P80@4717:4720, P1041@4722:4738, P1042@4740:4821, P1043@4823:4834, P1044@4836:4895, P1045@4897:5004, P1046@5006:5026, P2181@5028:5240, P67@5242:5315, P68@5317:5397, P69@5399:5446, P70@5448:5670, P71@5672:5756, P72@5758:5810, P73@5812:6012, P74@6014:6212, P75@6214:6295, P76@6297:6321, P77@6323:6451, P78@6453:6510, P79@6512:6671, P80@6673:6676, P81@6678:6702, P2182@6704:6887, P83@6889:7021, P84@7023:7100, P85@7102:7130, P86@7132:7453, P87@7455:7481, P88@7483:7725, P89@7727:7763, P90@7765:7965, P91@7967:7987, P92@7989:8212, P93@8214:8240, P94@8242:8362, P95@8364:8395, P96@8397:8590, P97@8592:8618, P98@8620:8800, P99@8802:8825, P100@8827:8996, P101@8998:9026, P102@9028:9076, P103@9078:9101, P104@9103:9253, P105@9255:9288, P2183@9290:9595 |
| V677 | L3796C0 | P2184@0:276, P1655@278:289, P1656@291:1246, P1657@1248:1440, P1658@1442:1841, P1659@1843:2038, P80@2040:2043, P1660@2045:2058, P1661@2060:2424, P1662@2426:2611, P2125@2613:2631, P2126@2633:6364, P2127@6366:6381, P2185@6383:13235, P2186@13237:13253, P2187@13255:13286, P2188@13288:13321, P2189@13323:13376, P2190@13378:13406, P2191@13408:13597, P2192@13599:13648, P2193@13650:13674, P2194@13676:13793, P2195@13795:14016, P1039@14018:14035, P2196@14037:14186, P2197@14188:14205, P2198@14207:14227, P2199@14229:14270, P2200@14272:14354, P2201@14356:14589 |
| V678 | L3800C0 | P2202@0:1020, P2203@1022:1077, P2204@1079:1276, P2205@1278:1344, P2206@1346:1397, P2207@1399:1598, P2208@1600:1692 |
| V679 | L3801C0 | P2209@0:5085 |
| V680 | L3805C0 | P2210@0:203, P118@205:255, P119@257:361, P120@363:403, P121@405:651, P122@653:896, P123@898:1051, P124@1053:1535, P125@1537:1676, P126@1678:1926, P127@1928:2176, P128@2178:3333, P129@3335:4419, P130@4421:4585, P2211@4587:4934, P2212@4936:5193, P2213@5195:5354 |
| V681 | L3806C0 | P2214@0:128, P2215@130:226, P2216@228:245, P2217@247:1058, P2218@1060:1161, P2219@1163:2203, P2220@2205:2533, P2221@2535:2625, P2222@2627:2871, P2223@2873:3221, P2224@3223:3359, P2225@3361:3970, P2226@3972:4470 |
| V682 | L3809C0 | P2227@0:318, P2228@320:416, P120@418:458, P2229@460:719, P2230@721:1030, P2231@1032:1388, P2232@1390:1751, P2233@1753:2141, P2234@2143:2579, P2235@2581:3500 |
| V683 | L3811C0 | P2236@0:5764 |
| V684 | L3815C0 | P2237@0:88 |
| V685 | L3816C0 | P2238@0:92, P2239@94:118, P2240@120:146, P2241@148:220, P2242@222:257, P2243@259:411, P2244@413:2597 |
| V686 | L3820C0 | P2245@0:145, P2246@147:509, P2247@512:1768, P2248@1770:1934, P2249@1936:2576, P2250@2578:4592, P2251@4594:4791 |
| V687 | L3821C0 | P2252@0:140, P720@142:158, P721@160:260, P722@262:289, P723@291:450, P724@452:470, P2253@472:619, P1065@621:678, P726@680:720, P727@722:1213, P728@1215:1250, P1066@1252:1312, P2254@1314:3365 |

| V688 | L3824C0 | P2255@0:537, P2256@539:640, P2257@642:684 |
| V689 | L3852C0 | P2258@0:19 |
| V690 | L3857C0 | P2259@0:698 |
| V691 | L3860C0 | P2260@0:6747 |
| V692 | L3864C0 | P2261@0:393 |
| V693 | L3868C0 | P2262@0:1718 |
| V694 | L3870C0 | P2263@0:3225 |
| V695 | L3873C0 | P2264@0:14 |
| V696 | L3876C0 | P2265@0:458 |
| V697 | L3879C0 | P2266@0:1204 |
| V698 | L3881C0 | P2267@0:5315 |
| V699 | L3884C0 | P2268@0:1771 |
| V700 | L3887C0 | P2269@0:40 |
| V701 | L3889C0 | P2270@0:570 |
| V702 | L3891C0 | P2271@0:15 |
| V703 | L3894C0 | P2272@0:387 |
| V704 | L3896C0 | P2273@0:324 |
| V705 | L3901C0 | P2274@0:75 |
| V706 | L3929C0 | P2275@0:58 |
| V707 | L3934C0 | P2276@0:232 |
| V708 | L3936C0 | P2277@0:7359 |
| V709 | L3939C0 | P2278@0:259 |
| V710 | L3941C0 | P2279@0:1296 |
| V711 | L3943C0 | P2280@0:6693 |
| V712 | L3946C0 | P2281@0:1268 |
| V713 | L3948C0 | P2282@0:1018 |
| V714 | L3951C0 | P2283@0:6032 |
| V715 | L3954C0 | P2284@0:183 |
| V716 | L3960C0 | P2285@0:108 |
| V717 | L3963C0 | P2286@0:4389 |
| V718 | L3965C0 | P2287@0:7204 |
| V719 | L3993C0 | P2288@0:60 |
| V720 | L3996C0 | P2289@0:8854 |
| V721 | L4000C0 | P2290@0:68, P2291@70:13249, P2292@13251:25057 |
| V722 | L4003C0 | P2293@0:765 |
| V723 | L4006C0 | P2294@0:499 |
| V724 | L4011C0 | P2295@0:382 |
| V725 | L4015C0 | P2296@0:3813 |
| V726 | L4024C0 | P2297@0:14 |
| V727 | L4027C0 | P2298@0:95 |
| V728 | L4030C0 | P2299@0:348 |
| V729 | L4035C0 | P2300@0:29 |
| V730 | L4038C0 | P2301@0:4419 |
| V731 | L4041C0 | P2302@0:712, P2303@714:1269, P2304@1271:1826, P2305@1828:2383, P2306@2385:2940, P2307@2942:3398, P2308@3400:4144 |
| V732 | L4062C0 | P2309@0:93 |
| V733 | L4063C0 | P392@0:381, P2310@383:391 |
| V734 | L4066C0 | P2311@0:2178 |
| V735 | L4069C0 | P2312@0:2619, P2313@2622:3363 |
| V736 | L4072C0 | P2314@0:401 |
| V737 | L4075C0 | P2315@0:244, P2316@246:250, P2317@252:881, P2318@883:1176, P2319@1178:1648, P2320@1650:1866, P2321@1868:1942, P2322@1944:1959, P2323@1961:2844, P2324@2846:2949, P2325@2951:2969, P2326@2971:3000 |

| V738 | L4078C0 | P2327@0:2017 |
| V739 | L4105C0 | P2328@0:10751 |
| V740 | L4109C0 | P2329@0:37681 |
| V741 | L4113C0 | P2330@0:31649 |
| V742 | L4138C0 | P2331@0:63, P267@65:82, P2332@84:135, P2333@137:489, P80@491:494, P2334@496:552, P2335@554:793, P2336@795:1165, P80@1167:1170, P2337@1172:1193, P2338@1195:1240, P2339@1242:1412, P2340@1414:1461, P2341@1463:1790, P2342@1792:1844, P2343@1846:2084, P2344@2086:2136, P2345@2138:2400, P80@2402:2405, P2346@2407:2430, P2347@2432:2805 |
| V743 | L4143C0 | P2348@0:34, P2349@36:140, P2350@142:339, P279@341:374, P280@376:546, P281@548:578, P2351@580:689, P309@691:773, P310@775:876, P311@878:895, P2352@897:1150, P2353@1152:1462, P2354@1464:1583, P80@1585:1588, P2355@1590:1697, P2356@1699:1706, P290@1708:1946, P291@1948:2093, P316@2095:2310, P317@2312:2714, P318@2716:2924, P2357@2926:3351, P2358@3353:3534, P80@3536:3539, P2359@3541:3645, P2360@3647:3693, P2361@3695:3890, P80@3892:3895, P2362@3897:4004, P2363@4006:4445, P80@4447:4450, P2364@4452:4556, P2365@4558:4708, P300@4710:5420, P327@5422:5462, P2366@5464:5567, P2367@5569:5841, P80@5843:5846, P2368@5848:5955, P2369@5957:6523 |
| V744 | L4157C0 | P2370@0:212, P2371@214:478, P2372@480:633 |
| V745 | L4158C0 | P2373@0:2839 |
| V746 | L4161C0 | P2374@0:389 |
| V747 | L4163C0 | P2375@0:2284 |
| V748 | L4166C0 | P2376@0:1000, P2377@1002:1013, P2378@1015:1184, P2379@1186:1194, P2380@1196:1216, P2381@1218:1404, P2382@1406:1967, P347@1969:1973, P2383@1975:2451, P358@2454:2466, P2384@2468:3068, P347@3070:3074, P2385@3076:3083 |
| V749 | L4169C0 | P334@0:872, P335@874:2034, P336@2036:2280, P337@2282:2406, P338@2408:2455, P2386@2457:2497, P2387@2499:2665, P2388@2667:3036 |
| V750 | L4177C0 | P2389@0:53 |
| V751 | L4202C0 | P2390@0:29, P2391@31:370, P2392@372:590, P2393@592:653 |
| V752 | L4219C0 | P2394@0:1555 |
| V753 | L4224C0 | P2395@0:378 |
| V754 | L4227C0 | P2396@0:105 |
| V755 | L4230C0 | P2397@0:141 |
| V756 | L4232C0 | P2398@0:14143 |
| V757 | L4235C0 | P2399@0:15825 |
| V758 | L4238C0 | P2400@0:1493 |
| V759 | L4240C0 | P2401@0:5408 |
| V760 | L4243C0 | P2402@0:13447 |
| V761 | L4246C0 | P2403@0:119, P2404@121:263, P2405@265:556 |
| V762 | L4249C0 | P2406@0:1110 |
| V763 | L4252C0 | P2407@0:327, P2408@329:604, P2409@606:837 |
| V764 | L4280C0 | P2410@0:20 |
| V765 | L4285C0 | P2411@0:29 |
| V766 | L4293C0 | P2412@0:214 |
| V767 | L4295C0 | P2413@0:24 |
| V768 | L4298C0 | P2414@0:2214 |
| V769 | L4301C0 | P2415@0:24 |
| V770 | L4303C0 | P2416@0:7650 |
| V771 | L4309C0 | P2417@0:400 |
| V772 | L4314C0 | P2418@0:754 |
| V773 | L4317C0 | P2419@0:481 |
| V774 | L4322C0 | P2420@0:137 |
| V775 | L4325C0 | P2421@0:187 |
| V776 | L4328C0 | P2422@0:7 |
| V777 | L4364C0 | P2423@0:10 |
| V778 | L4368C0 | P2424@0:34 |
| V779 | L4370C0 | P2425@0:560 |
| V780 | L4373C0 | P2426@0:297 |
| V781 | L4375C0 | P2427@0:1010 |
| V782 | L4378C0 | P2428@0:560, P2429@562:627, P2430@629:704, P2431@706:1017, P2432@1019:1553, P2433@1555:2223, P2434@2225:2352, P2435@2354:2436, P2436@2438:2453 |
| V783 | L4381C0 | P2437@0:334 |
| V784 | L4386C0 | P2438@0:11 |
| V785 | L4389C0 | P2439@0:102 |
| V786 | L4391C0 | P2440@0:236, P2441@238:456, P2442@462:563, P2443@569:2106, P2444@2112:2248, P602@2254:2277 |
| V787 | L4393C0 | P2445@0:34144 |

| V788 | L4396C0 | P2446@0:7884 |
| V789 | L4399C0 | P2447@0:1205 |
| V790 | L4423C0 | P2448@0:33, P2449@35:207, P2450@209:239, P2451@241:383, P2452@385:549, P2453@551:657, P2454@659:682, P2455@684:875, P2456@877:925 |
| V791 | L4424C0 | P2457@0:190, P2458@192:214, P2459@216:396, P2460@398:423, P2461@425:520, P2462@522:532, P2463@534:636 |
| V792 | L4428C0 | P2464@0:83, P3@85:372, P4@375:475 |
| V793 | L4429C0 | P2465@0:96, P3@98:385, P4@388:488 |
| V794 | L4433C0 | P2466@0:17, P2467@19:247, P2468@249:396, P2469@398:548, P2470@550:651, P2471@653:823, P2472@825:962 |
| V795 | L4434C0 | P2473@0:25, P2474@27:289, P2475@291:314, P2476@316:455, P2477@457:476, P2478@478:661, P2479@663:692, P2480@694:1002, P2481@1004:1237, P2482@1239:1349, P2483@1351:1376, P2484@1378:1613 |
| V796 | L4438C0 | P2000@0:24, P2485@26:92, P2486@94:202, P2487@204:341 |
| V797 | L4439C0 | P2488@0:52, P2489@54:92, P2490@94:1352, P2491@1354:1370, P2492@1372:1615 |
| V798 | L4444C0 | P2493@0:30, P2494@32:52, P2495@54:210, P2496@212:349, P2497@351:369, P2498@371:499, P2499@501:625, P2500@627:750, P2501@752:772, P2502@774:1024, P2503@1026:1050, P2504@1052:1243, P2505@1245:1301 |
| V799 | L4449C0 | P2506@0:58, P2507@60:98, P2508@100:545, P2509@547:961, P2510@963:1290, P2511@1292:1329, P2512@1331:1698, P2513@1700:1959, P2514@1961:1998, P2515@2000:2296, P2516@2298:2667, P2517@2669:2915, P2518@2917:2948, P2519@2950:3240, P2520@3242:3478, P2521@3480:3512, P2522@3514:3759, P2523@3761:4008 |
| V800 | L4452C0 | P2524@0:80, P2525@82:97, P2526@99:268 |
| V801 | L4454C0 | P2527@0:35, P2528@37:121, P2529@123:322, P2530@324:407 |
| V802 | L4458C0 | P2531@0:34, P2532@36:79, P2533@81:320, P2534@322:364 |
| V803 | L4459C0 | P2535@0:12, P2536@14:74, P2537@76:137, P2538@139:238 |
| V804 | L4463C0 | P2539@0:82, P2540@84:189, P2541@191:451 |
| V805 | L4464C0 | P2542@0:26, P2543@28:65, P2544@67:106, P2545@108:151, P2546@153:297, P2547@299:488, P2548@490:568, P2549@570:705, P2550@707:760, P2551@762:777, P2552@779:1019, P2553@1021:1334 |
| V806 | L4468C0 | P2554@0:71, P2555@73:573, P2556@575:1326, P2557@1328:1528 |
| V807 | L4469C0 | P2558@0:37, P2559@39:70, P2560@72:123, P2561@125:331, P2562@333:560, P2563@562:725, P2564@727:752, P2565@754:1064 |
| V808 | L4474C0 | P2566@0:40, P2567@42:236, P2568@238:503 |
| V809 | L4497C0 | P1309@0:53, P2569@55:101, P2570@103:210, P80@212:215, P2571@217:297, P2572@299:605, P80@607:610, P2573@612:704, P2574@706:993, P2575@995:1119, P80@1121:1124, P2576@1126:1206, P2577@1208:1298, P80@1300:1303, P2578@1305:1397, P2579@1399:1591, P80@1593:1596, P2580@1598:1678, P2581@1680:1856, P80@1858:1861, P2582@1863:1955, P2583@1957:2136 |

## Reproducible Coverage Locator

This analysis-only parser is recorded in Markdown, not installed or executed as repository implementation. Run with Node's `-e` and arguments `inventory`, `page N`, or `manifest firstValue lastValue`. Every unique paragraph has a stable P identifier; values have stable V identifiers. Manifest maps every physical line and JSON path to its representative and each paragraph's internal span. Offsets are zero-based half-open UTF-16 positions in the decoded string, or explicitly labeled canonical-pretty-JSON representation. The source range is fixed at physical 1-4500. Exact aliases receive semantic credit only after their representative is read.

```javascript

const fs = require("fs");
const path = "docs_PRD04/raw-research-evidence-dump-2026-07-26.txt";
const lines = fs.readFileSync(path, "utf8").split("\n").slice(0, 4500);
function stable(x) {
  if (Array.isArray(x)) return x.map(stable);
  if (x && typeof x === "object") return Object.fromEntries(Object.keys(x).sort().map(k => [k, stable(x[k])]));
  return x;
}
const values = [], valueMap = new Map();
function visit(x, p, line) {
  if (!x || typeof x !== "object") return;
  if (x.type === "tool_result") {
    const key = JSON.stringify(stable(x.content));
    let v = valueMap.get(key);
    if (!v) {
      v = {id: values.length + 1, content: x.content, refs: [], parts: []};
      values.push(v); valueMap.set(key, v);
    }
    v.refs.push({line, path: p + ".content"});
    return;
  }
  for (const [k, v] of Object.entries(x)) visit(v, p + "." + k, line);
}
lines.forEach((s, i) => {if (s.trim().startsWith("{")) visit(JSON.parse(s), "$", i + 1);});
const units = [], paragraphMap = new Map();
for (const v of values) {
  const text = typeof v.content === "string" ? v.content : JSON.stringify(stable(v.content), null, 2);
  const representation = typeof v.content === "string" ? "decoded-string" : "canonical-pretty-JSON";
  let offset = 0, start = null, end = 0;
  function flush() {
    if (start === null) return;
    const body = text.slice(start, end);
    let u = paragraphMap.get(body);
    if (!u) {u = {id: units.length + 1, body, refs: []}; units.push(u); paragraphMap.set(body, u);}
    u.refs.push({value: v.id, ...v.refs[0], representation, start, end});
    v.parts.push({unit: u.id, start, end});
    start = null;
  }
  for (const line of text.split("\n")) {
    if (line.trim() === "") flush();
    else {if (start === null) start = offset; end = offset + line.length;}
    offset += line.length + 1;
  }
  flush();
}
const pieces = [];
for (const u of units) {
  for (let start = 0; start < u.body.length; start += 12000) {
    pieces.push({unit: u.id, start, end: Math.min(start + 12000, u.body.length)});
  }
}
const pages = [];
let page = [], size = 0;
for (const p of pieces) {
  const n = p.end - p.start;
  if (page.length && size + n > 16000) {pages.push(page); page = []; size = 0;}
  page.push(p); size += n;
}
if (page.length) pages.push(page);
const mode = process.argv[1] || "inventory";
if (mode === "inventory") {
  console.log(JSON.stringify({occurrences: values.reduce((s,v)=>s+v.refs.length,0),values:values.length,units:units.length,pieces:pieces.length,pages:pages.length,characters:units.reduce((s,u)=>s+u.body.length,0),valueAliases:values.reduce((s,v)=>s+v.refs.length-1,0),paragraphAliases:values.reduce((s,v)=>s+v.parts.length,0)-units.length,firstPage:pages[0],lastPage:pages.at(-1)},null,2));
} else if (mode === "page") {
  const num = Number(process.argv[2]); const pp = pages[num-1]; if (!pp) throw Error("Unknown page");
  console.log("READ PAGE "+num+"/"+pages.length+"; offsets are 0-based half-open UTF-16 positions");
  for (const p of pp) {
    const u=units[p.unit-1], r=u.refs[0];
    console.log("\n=== P"+u.id+"["+p.start+","+p.end+") canonical raw L"+r.line+" "+r.path+" "+r.representation+"["+ (r.start+p.start)+","+(r.start+p.end)+"); paragraph occurrences "+u.refs.length+" ===\n"+u.body.slice(p.start,p.end));
  }
  console.log("\nEND READ PAGE "+num);
} else if (mode === "manifest") {
  const begin=Number(process.argv[2]),end=Number(process.argv[3]);
  console.log(JSON.stringify({values:values.slice(begin-1,end).map(v=>({id:v.id,refs:v.refs,parts:v.parts}))}));
}

```

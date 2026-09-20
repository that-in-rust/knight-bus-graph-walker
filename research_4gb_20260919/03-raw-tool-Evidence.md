# Reader 03: Raw Tool-Result Evidence

## Scope And Status

- Source: `docs_PRD04/raw-research-evidence-dump-2026-07-26.txt`, physical lines **4501-8500 inclusive only** (4,000 physical lines).
- Included: `message.content` blocks of type `tool_result`, their `content` values only. Historical commands, public captures, errors and source code are evidence, not current instructions. No link recursion, implementation, commits, agents or source edits.
- Initial structural inventory: 3,593 parsed JSON records; 407 non-JSON framing lines (59 blank, 348 nonblank); 1,083 tool-result content fields (987 strings, 96 lists); 793 exact distinct JSON content values.
- Initial canonical text denominator: **2,124 unique nonempty exact paragraphs**, 1,830,784 UTF-8 bytes and 22,266 decoded lines. No whitespace normalization or fuzzy deduplication. This is a derived reading denominator, not a change to the frozen corpus count or original assignment.
- Status: **2,124/2,124 canonical paragraphs fully read**, in 84 nontruncated bounded batches; sole image visually inspected. All assigned tool-result content on physical lines 4501-8500 is accounted for. No unread canonical text remains. Structural inspection and hashes alone receive no reading credit.
- Prior lane deliveries remain separate and complete: feasibility 9/9 prose; PRD04 verification 21/21 prose plus 8/8 supporting TSVs structurally inspected; chat archive 4,201/4,201 lines; radar tail 18,530-25,487, 6,958/6,958 lines. This file does not claim completion of the entire nine-folder goal.

## Method

1. Parse JSON records by physical source line, never recursively follow historical tool calls.
2. Compare decoded JSON content values exactly (object key order irrelevant). Extract all string values and every text-list element; separately inspect nontext elements structurally.
3. Split text at blank lines matching `\n[ \t]*\n`. Compare paragraph strings exactly without stripping, case folding, whitespace or line-ending normalization. Fully read each canonical nonempty paragraph once. Repeated exact paragraphs inherit coverage only after their canonical value has actually been read.
4. Record physical line, content block/list element, paragraph ordinal and decoded text-line range. Chunk long paragraphs with exact spans; retain unread suffixes until consumed. Pointer metadata is not prose coverage.
5. Notes omit secrets and private identifying details. Public technical source identifiers may be retained for traceability; links in captures are not freshly verified.

## Structural Audit

All 407 non-JSON lines classified: 59 blank, 230 hyphen separators, 115 archive file headers, two hash separators and one workflow-run header. No unclassified malformed JSON remains. These framing records do not contain assigned tool-result fields. Nontext tool references were structurally inspected; they advertise tool availability, not research evidence. The sole image is `L5750.B0.e0`, JPEG base64 payload, 36,456 encoded characters; visually inspected directly from the assigned content, without writing an image file. It shows Neo4j Aura Pricing v1.4, valid from 2026-03-01, the 1 ACU = 1 USD note and a loading indicator, with no rendered price rows. It is evidence of a loading state, not an unavailable catalogue.

## Reading Ledger

| Read batch | Exact consumed canonical spans | Observation |
|---|---|---|
| R01 | P0001-P0060, full | Historical projection-limit discussion is evidence of demand and architecture boundaries, not a 4GB feasibility measurement. Original data described as about 4TB is not automatically reducible to a 50GB prepared artifact. |
| R02 | P0061-P0086 full; P0087 characters 1-12 only | Release captures record version-specific bug fixes, export cleanup and cancellation behavior; memory-estimation prose explicitly uses array-like structures and doubles. Historical capture previews remain previews. |

| R03 | P0087 characters 13-226; P0088-P0109 full; P0110 characters 1-1,405 | Search failures/404 body cannot support lack-of-feature conclusions; release 2.20 capture introduces label-constrained triangle counting and fixes guard allocations on invalid projections. |
| R04 | P0110 characters 1,406-3,452; P0111-P0121 full; P0122 characters 1-10 | Distributed Infinigraph scaling is not local 4GB GDS spill. Heap/page-cache/writeback transaction costs coexist. |
| R05 | P0122 characters 11-1,112; P0123-P0127 full; P0128 characters 1-10,394 | Aura pause/resume API automation is an existing lifecycle feature. HN source contains relational alternatives and mistaken complexity claims. |
| R06 | P0128 characters 10,395-31,868 | Schema/query/data-lifecycle evaluation precedes engine choice; GraphBLAS speed rhetoric is unverified and old, not novel evidence. |
| R07 | P0128 characters 31,869-45,560; P0129 characters 1-8,308 | Read entire 477-line discussion including positive 1TB continuously imported Neo4j anecdote. Aura session capture begins explicit create/project/compute/writeback/delete lifecycle. |

| R08 | P0129 characters 8,309-30,308 | Aura compute isolation footnote explicitly excludes projection/writeback impacts on source DB; expiration differs from deletion. |
| R09 | P0129 characters 30,309-30,395; P0130-P0226 full; P0227 characters 1-434 | Kuzu fork benchmark excerpt lacks hardware and RAM/disk size; 3.22/0.009 does not reproduce 374x exactly. Legacy Neo4j repo inventory names GraphView and compressed adjacency. |
| R10 | P0227 characters 435-872; P0228-P0248 full; P0249 characters 1-12,660 | Actual GraphView source header/factory excerpt verifies historical kernel-view implementation, but not runtime or complete memory bounds. Reddit cost question is a quoted offer, not realized savings. |
| R11 | P0249 characters 12,661-34,660 | Read escaped comment JSON and HTML; feature-gap engineering and real-workload bake-off are migration-cost requirements. |
| R12 | P0249 characters 34,661-49,805; P0250-P0257 full; P0258 characters 1-4,381 | Read remaining public capture, advertisements and unrelated links without following them. Aura billing distinguishes RAM compute from paid extra storage and minimum billed session duration. |

| R13 | P0258 characters 4,382-4,414; P0259-P0263 full; P0264 characters 1-3,142 | Pause performs backup work; property-sharding architecture separates graph topology and payload. |
| R14 | P0264 characters 3,143-3,649; P0265 characters 1-21,493 | Shortest-path request changes contract; historical Louvain crash no longer reproduced after version update. |
| R15 | P0265 characters 21,494-28,033; P0266 characters 1-15,460 | PageRank normalization and stopping contracts matter; generic OOM articles contain oversimplified telemetry claims. |
| R16 | P0266 characters 15,461-27,626; P0267 full; P0268 characters 1-4,938 | NaN post-join filtering silently removes unassigned entities, not a proof that Leiden intrinsically excludes them. |
| R17 | P0268 characters 4,939-26,938 | Read full comparison/pricing excerpts, including uncontrolled benchmark caveat and projection filter scope. |
| R18 | P0268 characters 26,939-28,160; P0269-P0274 full; P0275 characters 1-12,526 | Search/auth metadata is not demand evidence; alpha Dijkstra captures show primitive-buffer growth, not pointer-only storage. |
| R19 | P0275 characters 12,527-14,312; P0276-P0335 full; P0336 characters 1-66 | Delta-stepping was reimplemented; 54GiB estimate for tiny similarity graph was a reported guard bug; 95GB-machine heap issue resolved by effective JVM configuration. |

| R20 | P0336 characters 67-325; P0337-P0398 full; P0399 characters 1-4,074 | Pricing-page loading state initially has zero rows; network capture later finds product catalogue. A row count is not sellability. |
| R21 | P0399 characters 4,075-9,241; P0400-P0451 full; P0452 characters 1-3,306 | Complete inspected historical price rows; support article auto-resume; Ladybug docs distinguish logical projection from disk scanning. |
| R22 | P0452 characters 3,307-4,000; P0453-P0483 full; P0484 characters 1-17 | Ladybug Louvain source explicitly materializes CSR, doubles non-self neighbors, renumbers/aggregates with hash maps. |
| R23 | P0484 characters 18-37; P0485-P0533 full; P0534 characters 1-1,404 | Allocator-backed arrays remain proportional to size; API/table constraints and catalogue version/schema changes affect feasibility. |

| R24 | P0534 characters 1,405-3,030; P0535-P0603 full; P0604 characters 1-106 | SessionMemory source renames Gi to GB without numeric unit conversion; catalogue storage billing has distinct hourly/month units. |
| R25 | P0604 characters 107-184; P0605-P0685 full; P0686 characters 1-1,605 | Read complete selected pricing table and repeated heap-resolution captures; repeated formats are not independent demand reports. |
| R26 | P0686 characters 1,606-2,206; P0687-P0782 full; P0783 characters 1-39 | Onager registry and Graphina StableGraph sources show memory outside SQL spill assumptions; CTAS correctness depends on engine version. |

| R27 | P0783 characters 40-703; P0784-P0830 full; P0831 characters 1-374 | Registry drop is explicit; PageRank FFI consumes edge arrays and copies results; SQL binding retains input/output vectors. |
| R28 | P0831 characters 375-552; P0832-P0862 full; P0863 characters 1-5,753 | Loading state, API catalogue discovery and selected pricing rows; sole image inspected separately. |
| R29 | P0863 characters 5,754-7,991; P0864-P0871 full; P0872 characters 1-1,904 | Versioned catalogue schemas differ; release archive navigation and bodies fully consumed as captured. |
| R30 | P0872 characters 1,905-2,040; P0873-P0881 full; P0882 characters 1-22 | GDS vector release notes and older property table disagree in scope; null/type conversion semantics matter. |
| R31 | P0882 characters 23-2,392; P0883-P0889 full; P0890 characters 1-15,454 | Vector coordinate types and block-store requirement; DuckPGQ captured version warning, not freshly checked compatibility. |

| R32 | P0890 characters 15,455-28,173; P0891 characters 1-9,281 | GraphBLAS ecosystem/out-of-core wrappers pre-exist; fork benchmark claims repeat previous source, not replications. |
| R33 | P0891 characters 9,282-27,481; P0892 characters 1-3,800 | Thin-wrapper migration anecdote is not whole-storage/semantic certification; streamed CSV loader alone is not bounded destination. |
| R34 | P0892 characters 3,801-25,800 | DuckDB primary troubleshooting explicitly warns memory limit bypass, blocking operators and OOM-killer failures. |
| R35 | P0892 characters 25,801-27,483; P0893 characters 1-20,317 | Memgraph on-disk objects accessed in a transaction still must fit; analytical mode omits automatic WAL/snapshots. |
| R36 | P0893 characters 20,318-27,516; P0894-P0911 full; P0912 characters 1-9,552 | Historical alpha Dijkstra issue summary overstates equivalence to Cypher; full captured logs begin. |
| R37 | P0912 characters 9,553-15,222; P0913-P0939 full; P0940 characters 1-312 | Duplicate issue55 includes staff replies missing from issue54; estimator admission is one-sided, lifecycle footnotes retained. |

| R38 | P0940 characters 313-488; P0941-P0958 full; P0959 characters 1-285 | Ladybug summary overgeneralizes lazy projection; actual captured page warns connection-bound async API scope. |
| R39 | P0959 characters 286-779; P0960-P0984 full; P0985 characters 1-207 | Arrow native receive buffers coexist with on-heap graph conversion; estimator guard does not guarantee success. |
| R40 | P0985 characters 208-2,703; P0986-P1006 full; P1007 characters 1-3,772 | GDS backup overwrites previous backup and restores into RAM; cost-report window/schema changes retained. |
| R41 | P1007 characters 3,773-6,461; P1008-P1024 full; P1025 characters 1-7,380 | Graph heap bytes may be unavailable (-1) on some JDKs; session cost and remote writeback tuning remain distinct. |
| R42 | P1025 characters 7,381-7,731; P1026-P1097 full; P1098 characters 1-240 | Session Ready timeout, staff role evidence, Memgraph tracking and lightweight-edge version changes; no fresh product claims. |

| R43 | P1098 characters 241-5,200; P1099-P1214 full; P1215 characters 1-408 | Memgraph edge-import restrictions and lightweight-edge metadata options; property compression is existing prior art. |
| R44 | P1215 characters 409-1,836; P1216-P1280 full; P1281 characters 1-12,590 | Onager README fully captured, dual licensing, exact binary-version constraint and explicit in-memory registry docs. |
| R45 | P1281 characters 12,591-27,971; P1282 characters 1-6,619 | Broad competitor marketing is not plug compatibility; historical live what-if demand contrasts with static precomputation. |
| R46 | P1282 characters 6,620-27,490; P1283 characters 1-1,129 | WCC/reachability and zero-data-movement claims need correction; entity-resolution candidate generation can dominate graph work. |
| R47 | P1283 characters 1,130-23,129 | Syndicated Infinigraph press releases repeat one source, not independent capability tests; enterprise distributed scope retained. |

| R48 | P1283 characters 23,130-28,092; P1284 characters 1-17,037 | Migration postmortem exposes write/recovery traffic missing from rehearsal; adjacency access is not constant-cost whole expansion. |
| R49 | P1284 characters 17,038-27,709; P1285 characters 1-11,328 | Restore throughput and graph extraction belong in lifecycle; GPU claims use a different hardware envelope. |
| R50 | P1285 characters 11,329-27,630; P1286-P1320 full; P1321 characters 1-197 | Vendor comparisons and full-sort top-k examples are not independent exact-query measurements. |
| R51 | P1321 characters 198-241; P1322-P1401 full; P1402 characters 1-30 | GraphRAG issue closure is not a fix; joined hierarchy rows need distinct-entity accounting. |
| R52 | P1402 characters 31-279; P1403-P1494 full; P1495 characters 1-4,387 | Sampling bootstrap, paper scope, compression denominators and product-specific capacity units require qualification. |

| R53 | P1495 characters 4,388-8,329; P1496-P1513 full; P1514 characters 1-36 | Neptune examples separate notebook and backup costs; catalogue extrapolations are not a local memory baseline. |
| R54 | P1514 characters 37-3,480; P1515-P1529 full; P1530 characters 1-45 | Fully consumed captured HTML/styles/scripts and RSS metadata; word matches in graphics URLs are not graph-database prose. |
| R55 | P1530 characters 46-1,845; P1531-P1546 full; P1547 characters 1-4,626 | Site search, limited index and navigation boilerplate are not exhaustive demand evidence. |
| R56 | P1547 characters 4,627-9,000; P1548-P1565 full; P1566 characters 1-456 | Property-sharding release, pricing edition table and criticism are distinct source types; no local performance proof. |
| R57 | P1566 characters 457-493; P1567-P1578 full; P1579 characters 1-5,420 | Procurement records mix products, periods, renewals and implementation; do not infer standard per-GB pricing. |
| R58 | P1579 characters 5,421-8,000; P1580-P1628 full; P1629 characters 1-247 | Award modifications avoid double counting; search index lacks most bodies and misses known later text. |

| R59 | P1629 characters 248-1,179; P1630-P1646 full; P1647 characters 1-28 | FinOps database-design excerpt explicitly excludes analytics; comparing its search counts to graph analytics is a scope mismatch. |
| R60 | P1647 characters 29-427; P1648-P1666 full; P1667 characters 1-1,162 | Read complete captured index facets and 2024 Aura announcement; access/security offerings differ by tier. |
| R61 | P1667 characters 1,163-7,499; P1668-P1672 full; P1673 characters 1-51 | Archived snapshots distinguish old dedicated Enterprise from new shared Business Critical and VDC. |
| R62 | P1673 characters 52-4,795; P1674-P1681 full; P1682 characters 1-908 | Same bundle description is insufficient to identify an unchanged purchased configuration. |
| R63 | P1682 characters 909-3,574; P1683-P1689 full; P1690 characters 1-2,616 | Procurement query headings overstate time scope; zero award value does not make a RAM-tier SKU free. |
| R64 | P1690 characters 2,617-12,000; P1691 characters 1-12,615 | Full captured pricing matrix includes refresh/security/support boundaries and edition-specific analytics format. |

| R65 | P1691 characters 12,616-18,000; P1692-P1696 full; P1697 characters 1-35 | Low-memory analytics format is listed for Enterprise/Infinigraph; graph-session price still excludes source DB. |
| R66 | P1697 characters 36-2,673; P1698-P1706 full; P1707 characters 1-15,768 | Historical monthly change partly reflects 720 versus 730 hours; Neptune fuller capture supplies omitted I/O totals. |
| R67 | P1707 characters 15,769-16,001; P1708-P1722 full; P1723 characters 1-8,621 | Redirect-shell failures are not missing features; limits page warns unbounded vertex enumeration/counting. |
| R68 | P1723 characters 8,622-16,041; P1724-P1747 full; P1748 characters 1-59 | Provisioning readiness, replicas and all-data-fit validation matter; API integer bounds do not establish continuous available tiers. |
| R69 | P1748 characters 60-6,000; P1749-P1753 full; P1754 characters 1-5,978 | Historical pricing anecdotes include positive quotes and unrelated search hits; vendor averages lack sample size. |

| R70 | P1754 characters 5,979-11,817; P1755-P1854 full; P1855 characters 1-58 | Review summaries vary in attribution and pricing definitions; ownership metadata needs path arithmetic. |
| R71 | P1855 characters 59-118; P1856-P1907 full; P1908 characters 1-16 | Entire 270-line captured review extract read, including positive operational use, restart/backup pain and contradictory API opinions. |
| R72 | P1908 characters 17-547; P1909-P1947 full; P1948 characters 1-10,600 | Review identities repeat across comparison pages; marketplace transitions and cold starts add lifecycle requirements. |
| R73 | P1948 characters 10,601-11,475; P1949-P1951 full; P1952 characters 1-942 | Detailed recommendation/dependency review includes import batching and CI seeding; scalar prices are not per-node billing. |
| R74 | P1952 characters 943-17,144; P1953-P1954 full; P1955 characters 1-5,432 | Full captured support-document and BOM accounts; one explicitly disabled analytics, so its pain is not measured GDS behavior. |

| R75 | P1955 characters 5,433-13,751; P1956-P1979 full; P1980 characters 1-6,882 | PDF snippets expose different import-bound behavior from API summary; OOM during load still requires retry capacity. |
| R76 | P1980 characters 6,883-8,390; P1981-P1993 full; P1994 characters 1-3,438 | Old pricing widget omits newer small SKUs; queue and multithread guidelines explicitly are not guarantees. |
| R77 | P1994 characters 3,439-4,868; P1995-P2003 full; P2004 characters 1-194 | Snapshot and transfer charges remain after compute deletion; repeated launch capture is not another performance test. |
| R78 | P2004 characters 195-763; P2005-P2028 full; P2029 characters 1-47 | Multiple-database preview and adjustable storage qualify earlier review complaints and bundled-pricing claims. |
| R79 | P2029 characters 48-413; P2030-P2038 full; P2039 characters 1-2,943 | GDS prioritization quote repeats same forum source; issue-title inventory is not proof of bug status. |

| R80 | P2039 characters 2,944-6,997; P2040-P2048 full; P2049 characters 1-48 | Session creation, cancellation and streaming-job bug fixes change lifecycle baselines; batch size alone does not bound projection memory. |
| R81 | P2049 characters 49-4,347; P2050-P2068 full; P2069 characters 1-10 | Launch claims mix accuracy, developer productivity and execution speed; duplicate forum quote remains one source. |
| R82 | P2069 characters 11-2,021; P2070-P2087 full; P2088 characters 1-1,135 | Search index misses known content; captured credentials omitted from notes; FOCUS graph-rule machinery differs from graph-database taxonomy. |
| R83 | P2088 characters 1,136-2,824; P2089-P2103 full; P2104 characters 1-527 | Working-draft taxonomy lists Neptune under NoSQL; procurement obligation, outlay and description prices differ. |
| R84 | P2104 characters 528-1,070; P2105-P2124 full | Administrative zero-dollar modifications are not renewals; review extract omits free-tier users and later supplies some dates. |

Current coverage: **2,124/2,124 canonical paragraphs fully read**, all unique content and exact duplicate occurrences accounted for. Next unread: **none within physical lines 4501-8500 tool-result content**. Embedded JSON text remains in its exact outer string form; escaped newlines do not count as decoded outer-text newlines. Captured previews, partial web excerpts and referenced external files are fully read only as present, not expanded or claimed as entire external sources. The pointer map records every exact occurrence, not additional independent evidence.

## Source Observations

### O01: Projection Pain Is Real But Scale And Semantics Matter

`L4503.B0.s`, P0002-P0037: the captured 2025 forum exchange describes billions of nodes, hundreds of billions of edges and roughly 4TB disk data on a 64GB machine. Its query uses an undirected match; direction, duplicate orientations and relationship identity must be settled before deriving E. The staff reply says this GDS workflow requires a complete in-memory projection, warns estimates cover GDS rather than the whole DB, and recommends newer/native projection over legacy Cypher. That does not show that a 50GB disk artifact can preserve all required input information. Native projection and appropriate heap/page-cache allocation belong in the baseline. The historical 'two orders of magnitude' direct-database slowdown is anecdotal and version/workload-specific, not a measured comparison with any candidate here. Captured primary discussion: [GDS algorithms without a projection](https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039); not refetched.

### O02: Missing Captures Cannot Establish A Negative

`L4514`, `L4524`, `L4526`, `L4571`, P0040-P0041/P0051-P0055/P0063-P0064: forbidden fetch, redirect, CAPTCHA and persisted-output preview do not establish absence of a product feature. We fully read the content present, not the referenced external saved output. `L4518` reproduces internal marketing interpretation of a forum statement; it is not an independent second demand signal. 'No projection step' may mean prepared-artifact reuse, but still requires acquisition, normalization and bounded construction somewhere in the lifecycle.

### O03: Versioned Baselines Include Correctness And Lifecycle Fixes

`L4576`, `L4579`, P0066-P0072: captured release notes report max-flow invalid-result fixes, Arrow export process cleanup, HITS mutate/write score fixes, faster cancellation for a specified procedure list, selected node-similarity allocation improvements, Arrow null-property correction and vector projection support. An experiment must pin versions and execution modes, test nulls/weights/directions, and account for process cleanup after export/cancellation. An older bug cannot be used as an inherent advantage over a corrected release. Release-note 'None' sections are not a proof of no out-of-core support in all products. [Captured release archive](https://neo4j.com/release-notes/gds/), not refetched.

### O04: Array Storage And Incomplete Estimation Scope

`L4586.B0.s`, P0074: memory-estimation capture describes array-like double weights and doubled relationships for UNDIRECTED orientation. It contradicts the universal pointer-object premise. Its phrase '8 bytes per node' for relationship weights should not be blindly converted into a storage formula: use the actual property cardinality and representation. The text says production-ready algorithms are guaranteed estimates, not all algorithms. The captured page ends mid-parameter; no omitted remainder is credited. Estimator minima/maxima and heap percentages are not whole-machine resident-peak guarantees. [Captured memory-estimation manual](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/), not refetched.

### O05: Scaling A Cluster Does Not Satisfy The Local Envelope

`L4701`, P0117: [Infinigraph GA capture](https://neo4j.com/blog/graph-database/infinigraph/) describes adding machines and property shards, not running all GDS algorithms with spill on a 4GB physical machine. The 100TB+ and 'limits gone' marketing statements are not unbounded capacity guarantees. For this goal, a remote service is a separate product/hosting scenario, not hidden compute under a local resource label.

### O06: System Requirements Include Build And Writeback

`L4712`, P0120: the captured system requirements allocate heap to both projected graph and algorithm state, and also transaction state during writeback. They advise shrinking page cache for purely analytical native projection, while retaining a concurrency-related minimum for construction. The capture truncates the formula after `readConcur`; no complete numeric guarantee is inferred. A general 90%-heap guideline does not establish feasibility on a 4,000,000,000-byte machine once OS, native buffers, clients and export are included. `L4618`, P0095 adds that label-filtered triangle semantics and invalid-projection guard cleanup are versioned baseline details.

### O07: Lifecycle Automation Already Exists

`L4742`, P0123 full: the captured Neo4j developer article supplies credential-based instance pause/resume automation and schedule examples. No historical code was executed. This is a prior product capability, not a new architecture invented by pausing compute. The shown script's success response is not a measured readiness time or completed analytic result; a fair workflow includes resume-to-ready, projection restoration/rebuild, query and export. New notes do not copy credential values or private environment identifiers. Article title: 'Automate start and stop of Neo4j AuraDB instances using GitHub Actions', captured publication date 2025-03-11.

### O08: Full Discussion Rejects A One-Sided Demand Story

`L4776`, P0128, all 477 decoded lines read: [2018/2019 HN discussion](https://news.ycombinator.com/item?id=18795498) includes prototype ease, mature-language/tooling preferences, relational/pgRouting alternatives, costly schema/ingestion/maintenance experiences, and a positive report of a roughly 1TB Neo4j blockchain explorer simultaneously importing new data. These are historical anecdotes, neither a controlled failure rate nor current adoption/pricing evidence. The thread's proposed pipeline comparison is useful: count inserts, query kinds/rates, and storage before choosing an engine. Its graph-query versus whole-graph-processing distinction supports a scoped analytical companion rather than a blanket DB replacement. The reported simple file-counting success makes a streamed relational/file baseline mandatory for low-complexity tasks.

Two substantive counterexamples: returning all ancestors costs at least their output cardinality, and a chain has linear depth, so the thread's blanket O(log n) graph-database ancestor claim is false. A graph's conceptual shape does not require a graph DB: scalar counts need no persistent adjacency. Conversely, a continuous explorer's latency/freshness contract is not covered by a once-built snapshot result. The GraphBLAS 'orders of magnitude'/600x discussion is not a reproducible local 4GB baseline and not proof of global novelty for sparse matrix execution. The opposing declarative/query-language claims in the same thread should not be selected opportunistically.

### O09: Ephemeral Analytics Is Existing Product Architecture

`L4804`, P0129 full: captured [Aura Graph Analytics](https://neo4j.com/docs/aura/graph-analytics/aga/) and [deployment comparison](https://neo4j.com/docs/aura/graph-analytics/) explicitly cover create session, remote projection, computation, writeback/export and delete. This is prior product architecture, not a novel lifecycle invented by a snapshot-first local service. They describe isolated compute but qualify that projection and writeback may affect source DB performance. Implicit session deletion waits for all projected graphs to be dropped. Expiration, inactive TTL, maximum lifetime and actual deletion differ. Restart behavior differs across deployments; graph/model changes after an update begins may not be restored. A replacement's persistence and refresh promises must specify equivalent boundaries, not just algorithm availability. Captured sizes include a 4GB session, but that is not evidence that the client, source DB and compute together fit on one 4GB machine.

### O10: Fork Claims Are Not A 4GB Benchmark

`L4826`, `L4830`, `L4834`, `L4837`, P0130-P0176: the captured Kuzu fork article extracts claim 100K nodes/2.4M edges and 3.22s versus 0.009s second-degree paths, yet 3.22/0.009 is about 357.8, not exactly the claimed 374. Rounded underlying measurements could explain it, so this is an unreproducible ratio, not proof of falsification. Hardware, complete query/result contract, cold/warm state, full data preparation, resident peak and disk footprint are absent in these extracts. Their ingestion ratio 30.64/0.58 is about 52.8 and is arithmetically compatible with 53x, still not a measured result of this reader. Path queries cannot silently become reached-set BFS; result materialization must match. Claimed concurrent writers add WAL, contention, conflict handling and durability work; 'embedded/zero infra' does not eliminate lifecycle overhead. Claimed fork ownership does not remove maintenance risk. Multiple extracts of this article are not independent benchmark replications. No fork status is freshly verified here.

### O11: Historical Kernel-View Mode Has Actual Source Evidence

`L4903`, P0227-P0235, captured source decoded lines 1-80, and `L4905`, P0236-P0239, captured factory snippet: `GraphView implements Graph`, `TYPE = kernel`, transaction wrapper, dimensions, ID map and default weight are explicit. The factory constructs dimensions and begins a node importer. This supports existence of an old direct-kernel graph representation, so live/direct algorithms must not be dismissed as impossible. It also shows that direct access still needs some setup/state; not 'no memory'. This partial source capture does not prove a 100x slowdown, completed algorithm semantics or that the whole importer is bounded. `L4899` inventory lists decompression/varlong/compressed-adjacency classes, a discovery pointer rather than full source verification of their internals. [Historical repository](https://github.com/neo4j-contrib/neo4j-graph-algorithms), not fetched.

### O12: Offered Cost Reduction Is Not Achieved TCO

`L4942`/`L4945`, P0248-P0249 full: captured [Neo4j versus Neptune discussion](https://www.reddit.com/r/Neo4j/comments/1eyuu73/anyone_have_experience_with_both_neo4j_and_aws/) reports a vendor quote for approximately 80% reduction and skepticism about feature gaps. Replies ask for a real-workload comparison and count the in-house engineering needed to recover absent features. It gives no completed migration benchmark, measured cost savings, 4GB feasibility evidence or proof that either architecture is merely emulation. Public usernames/private affiliation details are not repeated here. Related posts, ads, boilerplate and serialized metadata were read as content but are not demand evidence for this graph project.

### O13: Prepared Disk Is A Separate Budget From Allocated RAM

`L4975`, P0252, and `L4981`, P0258 characters 1-4,381: billing capture separates RAM-sized compute, included storage, additional storage and secondary instances. Its 4GB example includes 8GB storage and counts 24GB additional storage for a 32GB target; it does not supply a 50GB prepared artifact at no cost. Captured persistent-instance pause charge is 20% of standard hourly rate, not zero. AuraDS has a different storage policy. Ephemeral graph sessions are billed by allocated GB-minutes with a ten-minute minimum, even for a shorter query. Do not present these historical prices/policies as current advice. Locally, include artifact + scratch + output + overlapping versions explicitly; in a cloud comparison include projection, readiness and export session time, source-service load and applicable egress. [Captured billing dimensions](https://neo4j.com/docs/aura/billing/billing-dimensions/), not refetched.

### O14: Existing Property Separation, With A Filter Caveat

`L5025`/`L5029`, P0263-P0264 full: [property-sharding capture](https://neo4j.com/blog/graph-database/property-sharding-infinigraph/) describes topology/labels/IDs in a graph shard and hashed entity properties in property shards, then batched property lookup. It is direct prior product art against a global novelty claim for separating topology from payload. The selected excerpt has `full_content:null`, so its missing article remainder is not read. Delaying properties until the final stage is valid only if those properties do not affect traversal eligibility, weights, permissions or intermediate predicates. Property-aware traversal must count the needed lookups earlier. Hashing related entities to different shards adds remote reads; replicating transaction logs and graph/property replicas adds storage and refresh work. The launch capture has differing metadata/body dates, so do not infer publication chronology solely from the wrapper.

### O15: Path Reuse Must Preserve The Actual Requested Answer

`L5063`, P0265: [road-network thread](https://community.neo4j.com/t/how-to-efficiently-compute-all-pairs-dijkstra-shortest-paths-for-a-louvain-community-in-gds/73358) first describes 3,400 nodes and about 5.8M unordered pairs, consistent with 3,400*3,399/2 = 5,778,300. A single-source multi-target run can reuse search work versus one run per pair, but cannot avoid emitting those distances if that is still the contract. Later the author asks for distances from one fixed source to a label class; a matched but unused target was multiplying single-source procedure calls. This is a query-shape correction, not a new shortest-path algorithm. Filtering to a community's induced subgraph can change distances if the true shortest route exits and re-enters the community. Include a two-internal-node/one-external-shortcut counterexample in any experiment. Preserve road weights, direction, unreachable results and path-versus-distance outputs.

### O16: Closed Bugs And Guard Estimates Are Not Inherent RAM Requirements

`L5063`/`L5069`, P0265/P0267: captured [Louvain crash issue 69](https://github.com/neo4j/graph-data-science/issues/69) ends with no reproduction after a version update. The cited roughly 23GB memory statement alongside a 10GB configured heap and successful mutate return lacks a reconciled peak/allocated/estimated accounting contract. Do not turn it into a hard algorithm lower bound or proof of corruption in modern GDS. `L5103`, P0275-P0280: [delta-stepping issue 9](https://github.com/neo4j/graph-data-science/issues/9) reports wrong zero distances in an old alpha algorithm but later confirms a different product-tier implementation planned for GDS 2.0; old timing with wrong answers is invalid baseline evidence. The same capture's issues 54/55 repeat an alpha Dijkstra report; primitive `DoubleArrayDeque` growth from about 145M to 217M capacity is an explicit transient-array-growth risk, not pointer-object evidence. A doubling/growth bound must include old+new arrays concurrently, but the capture does not prove its root cause. Report identity prevents counting duplicate issues as independent users.

### O17: Convergence, Nulls And Telemetry Are Part Of Correctness

`L5063`, P0265, PageRank article excerpt: normalized versus unnormalized scores and absolute per-node stopping tolerance are different contracts, even if rankings agree. Logging degrees without explicitly resolving internal IDs does not by itself identify which named vertex produced each log row. `L5064`/`L5069`, P0266/P0267, [GraphRAG issue 2348](https://github.com/microsoft/graphrag/issues/2348): the reported 10/151 retained entities follow a left join plus `level <= threshold`, where NaN fails the comparison. Preserve unassigned/isolated vertices explicitly; this is a pipeline coverage defect, not a mathematical necessity of Leiden. Source-reported counts are not reproduced measurements here.

Generic Kubernetes/vendor troubleshooting excerpts in P0266 suggest diagnostic hypotheses, not a physical-memory proof. Exit 137 alone cannot distinguish every cause of SIGKILL, and monotonic memory can be legitimate retained state rather than a proven leak. Counterexample: a deliberately growing graph catalog. A p95-sized allowance cannot guarantee no higher peak; distinguish cgroup charge, RSS, native memory, page cache, host pressure and sampled telemetry. No captured recommendation to increase limits or execute commands was followed. The NeocorRAG excerpt's answer-quality figures and LLM candidate-selection mechanism do not establish exact graph-query semantics or 4GB feasibility.

### O18: Projection Membership Needs Its Own Test

`L5070`, P0268 full: [projection question](https://stackoverflow.com/questions/76049417/projecting-neo4j-graphs-using-cypher-and-gds) proposes `EXISTS((n)--())` while the relationship projection restricts to CALLS among Function nodes. A Function with only a different relationship type may pass the node filter but remain isolated in the projected graph. Membership must be defined against the intended selected topology, not any database incident edge. Removing isolates changes V, teleport mass, convergence and outputs for several algorithms. The surrounding comparison-site claims, tiny review populations and an explicitly uncontrolled laptop-versus-cloud benchmark do not establish comparative performance or current prices. They are read search evidence, not authoritative specifications.

### O19: Two Strong Corrections To The Pain Narrative

`L5131`, P0295-P0314: captured [Jaccard migration discussion](https://community.neo4j.com/t/comparing-jaccard-similarity-neo4j-3-4-to-node-similarity-on-neo4j-3-5-and-gds-1-1-1/37205) reports a 54GiB minimum estimate for 2,594 nodes/187 relationships, followed by a claimed 30ms completion when the guard is bypassed and staff treating it as a bug. This is not evidence that exact similarity intrinsically requires 54GiB on that graph. `topK:1` changes per-node result limits; final `LIMIT 10` is not an input/work bound. Bypassing a guard is not generally safe and no bypass is performed here.

The same captured tool result, P0315-P0332, [95GB machine / heap discussion](https://community.neo4j.com/t/what-is-the-ideal-heap-memory-size-for-gds-in-neo4j/76311), explicitly ends with both reporters saying the problem was resolved after effective JVM options were corrected. A configured file value, free heap and actual JVM maximum are different quantities; rounded '23GiB exceeds 23GiB' messages are not exact byte equality. This source cannot support 'big iron does not save you' as an inherent failure. P0284-P0293 separately documents a 120GB pod with two allocation thresholds; it supports simpler admission/configuration UX, not use-all-RAM safety.

### O20: Price Units And Catalogue Schemas Must Be Pinned

`L5218`/`L5222`/`L5227`/`L5230`, P0397-P0415: 584 historical catalogue rows are described structurally and the entire selected result table is read. Running/paused 512GB AuraDS increments are 166.4/33.28 per hour, consistent with the 20% ratio; serverless base 0.0067 per GB-minute is not 0.0067 per GB-hour. These are historical captures, not current quotes or our measured economics. A 'not sellable' row is not an available purchase option, and failed ratio code divides by zero for some entries, so it cannot certify every row. The support article specifies auto-resume after 30 days, adding lifecycle behavior beyond simply retaining data cheaply. `L5386`/`L5392`, P0528-P0533 records catalogue versions and a v2.0 schema with no incrementMetricUnit, so a calculator must validate schema/version and units before multiplying. Future-effective versions visible in a historical catalogue are not automatically then-current prices.

`L5204`/`L5209`/`L5212` and `L5338`/`L5363`/`L5383`: loading-state DOM, missing rows, JavaScript assets and successful API responses explain why a blank static page cannot establish missing pricing or login requirements. Historical browser/API commands were only read, never replayed. A 1,952GB catalogue entry now being read is a cloud RAM offering, not a 50GB prepared-disk budget or a 4GB local benchmark.

### O21: Lazy Projection Already Exists, Algorithm State Still Matters

`L5274`, P0452 full: the archived Kuzu algo documentation says projection is evaluated at algorithm execution and data scanned from disk, with projection lifetime tied to explicit drop or connection close. `L5254` and `L5275`, P0416-P0420/P0453: Ladybug captures list five algorithms and predicates restricted to their corresponding table, not cross-table predicates. This is prior art for a logical, nonmaterialized projection, not evidence that every algorithm's state spills. 'No projection step' is misleading when it erases logical view creation or the underlying database import. The captures are partial pages whose full content ends at the displayed cutoff; no omitted page suffix is counted.

`L5265`, P0437-P0448 mixes 'successor rather than fork' narrative with inherited research, whereas `L5284`, P0456 records an MIT repository. A GitHub `fork:false` field or marketing label cannot establish independent code provenance; no global originality or acquisition claim follows. The 10x query claim lacks a benchmark contract. [Ladybug repository pointer](https://github.com/LadybugDB/ladybug), not refetched.

### O22: Captured Louvain Code Defeats A Blanket Out-Of-Core Claim

`L5296`, P0461-P0472, full captured header: `InMemGraph` stores CSR offsets and weighted Neighbor vectors; the comment requires both directions explicitly and says mutation is not thread-safe. `L5301`, P0474-P0479: initializer scans forward and backward and omits the backward self-loop copy; renumbering uses an unordered map and skips UNASSIGNED entries; aggregation allocates a vector of per-community unordered maps. If contraction is weak, old graph, new graph/map construction and state can approach original scale. The semantic exception for loops and representation of parallel edges belong in the budget/oracle. These captured snippets do not prove complete memory-manager enforcement or complete Louvain behavior, but they are sufficient to reject 'disk-scanned projection means no in-memory topology' universally. `L5298`, P0473 identifies PageRank dense frontiers; only the shown search output is read, not its omitted full file.

`L5308`, P0482-P0493: ObjectArray allocation requests `size*sizeof(T)` from a memory manager; a manager-backed array remains O(size), and the shown reallocation function cuts off before its behavior. No bounded-allocation/completion guarantee follows from the allocator name. This complements rather than replaces the primitive-array evidence against GDS pointer-only claims.

### O23: Algorithm Names Do Not Establish Plug-Compatible Workflows

`L5285`, P0457: captured Ladybug Louvain describes Grappolo-based parallel execution and treats each directed projected edge as undirected, with maxPhases/maxIterations and node-object output. `L5311`, P0494-P0504: a Windows PageRank exception report is version-specific; a separate feature request documents Louvain's one-node-table/one-edge-table constraint. Flattening User-Session-Device paths into a same-type user graph changes the mathematical objective, may create quadratic shared-device pairs and incurs import/materialization/refresh costs. It is not a free compatibility adapter. Preserve original node/property/edge identities in exported results or explicitly narrow the contract. `L5307` issue titles include import OOM, corrupted-index open OOM, checkpoint/read-only consistency and COPY hang; titles are verification leads, not proof of exact defects or current status.

### O24: A Unit Label Is Not A Physical Byte Budget

P0580-P0596, `L5483`: captured SessionMemory source calls `value.replace("Gi","GB")` before enum validation. This is lexical relabeling, not multiplication by a binary/decimal conversion factor. A cloud label of 4GB cannot certify a 4,000,000,000-byte whole-machine bound: 4GiB is 4,294,967,296 bytes. P0628, `L5534`, fully lists selected 1-512GB session rows; 0.0067 per GB-minute implies 0.402 per GB-hour, with a ten-minute minimum. Historical illustrative minimums are 0.268 for 4GB and 34.304 for 512GB, not current offers; the 1GB row is not sellable. P0535-P0538, `L5398-L5404`, exposes schema errors and GB-month/730 storage units billed hourly. Product rows, future-effective catalogues and name strings must not silently redefine this goal's RAM or disk limits. P0635-P0695 repeat the already-resolved 95GB heap discussion in several formats, not independent failures.

### O25: A DuckDB Extension Does Not Automatically Inherit Spill

P0717-P0736, `L5604` and `L5607`: captured Onager source stores `Graph<i64,f64>` or Digraph plus an external-ID HashMap in wrappers and a static `Lazy<Arc<RwLock<HashMap<String,GraphType>>>>` registry. P0782, `L5612`, selected Graphina source lines identify petgraph StableGraph, FxHash maps and conversion routines with node vectors/maps and dense/sparse matrices. These are actual representation clues, stronger than the preceding documentation summary's inference from absent out-of-core wording. They do not show registration with DuckDB's managed/spillable buffers. Hypothesis to test: graph import and conversion retain separate resident representations even when a surrounding SQL sort spills. Budget registry lifetime, duplicate external IDs, conversion overlap and explicit drop; do not infer exact byte overhead from type names alone. The selected 41-line source search is fully read as captured, not the entire referenced 1,147-line source file.

### O26: Export Correctness And OOM Boundaries Need Separate Gates

P0737-P0749, `L5607`, Known Issues capture describes a multi-thread CTAS/materialized-result batch-index race for DuckDB 1.4.x and older, with thread-one workaround and a claimed 1.5 fix. Pin both engine and extension versions and verify exported/materialized rows, not just an algorithm call. P0750-P0775, same capture, reads the complete roadmap: checked algorithm families coexist with unchecked performance benchmarks and examples; checkboxes are not correctness, scalability or compatibility evidence. P0776-P0781, `L5611`, captured FFI code initializes one current-thread Rayon pool for specified non-atomic Emscripten builds and uses `catch_unwind` to return an error sentinel for unwinding panics. Neither mechanism catches every process OOM abort or SIGKILL, nor establishes bounded-memory completion. Source snippets and roadmap are captured historical evidence, not a freshly verified product status.

### O27: The Table-Function Boundary Can Retain Both Input And Output

P0785-P0802, `L5620`, `L5623`, `L5626`: full captured functions explicitly create/drop registry entries, collect graph names and lock the registry for additions. The PageRank FFI accepts source/destination/weight slices, computes a result, and copies node IDs and ranks into caller-provided arrays. Selected C++ lines declare source, destination, weights and result vectors and show two compute call sites; the surrounding control flow is not captured, so this is a verification lead for repeated computation, not proof of two executions. The observed arrays alone can retain 16E bytes of endpoints, optional 8E bytes of weights and 16V bytes of output before internal graph/state and capacity overhead. Whether these coexist at peak requires execution evidence. Edge-only input has no shown independent isolate list: test a graph with an isolated vertex, rather than assuming PageRank teleport/output membership matches a separately projected node table. Explicit drop removes a named graph but does not prove all result buffers or external resources are released immediately.

### O28: Pin Projection Types, Nulls And Extension Versions

P0875/P0880-P0889, `L5827`, `L5842-L5856`: captured GDS 2026.05 release notes add vector-type projection, while a captured supported-types table lists Long, Double and numeric lists with Long.MIN_VALUE, NaN and null fallbacks. Preserve the discrepancy for a version-specific oracle instead of silently treating the table as exhaustive forever. The capture describes first-read-value type inference, no mixed lists, and lossless conversion checks with extra work; import order and missing values need tests. VECTOR documentation distinguishes coordinate widths up to FLOAT64, dimension at most 4096 and block-format storage. A vector node count multiplied by an assumed eight-byte scalar is not a valid property budget. P0890's already-read title for the official DuckDB graph-query guide warns to use 1.4.4 rather than 1.5.x for DuckPGQ at capture time. Combined with Onager's captured 1.5 CTAS fix, a single-engine-version baseline cannot be assumed to support both extensions correctly. These are historical captured primary documents, not current product claims or reasons to fetch new links.

### O29: Existing Spill And Transaction Modes Have Explicit Limits

P0892, `L5897`, [captured DuckDB OOM guide](https://duckdb.org/docs/lts/guides/troubleshooting/oom_errors): memory_limit applies to managed memory, while some operations bypass the buffer manager; multiple blocking operators and certain aggregations can still exhaust RAM. Its 50-60% troubleshooting suggestion is not a universal proof. P0893, `L5898`, [captured Memgraph storage guide](https://memgraph.com/docs/fundamentals/storage-memory-usage): on-disk storage serializes into RocksDB, uses per-transaction caches and snapshot isolation, yet all graph objects used by a transaction still need to fit in RAM. Separate label/index stores and connectivity indexes count toward prepared disk. The captured analytical mode omits WAL and periodic snapshots; creating a snapshot excludes other transactions, and switching back restores a snapshot. Thus reduced memory can trade durability, availability and refresh latency. Neither existing mode nor a proposed snapshot-first product guarantees completion merely by retaining less state. These primary captures were fully read where present and not refetched.

P0890, `L5891`, GraphBLAS pointers explicitly list semiring path prior art, compressed formats, distributed implementations and an out-of-core Dask wrapper. This is enough to reject novelty for generic sparse algebra plus spill, not enough to certify these wrappers on 4GB. P0891 migration captures include a claimed six-line wrapper-based swap and 69 passing tests; this is one application's anecdote, not universal Cypher, file-format or result compatibility. A vendor CSV loader described as streaming may bound parsing buffers but says nothing sufficient about destination graph/index construction peaks. No historical install, audit, override or migration commands were executed.

### O30: Follow Duplicate Issue Identity Before Calling Support Absent

P0896-P0914, `L5927-L5939`: issue54 is closed by its reporter less than a minute after creation and has zero comments; issue55 repeats it and includes two developer replies. The summary's 'no Neo4j response documented' is only true of the first issue view, not the incident. Replies distinguish ordinary memrec from GDS estimates, production-ready versus alpha coverage, and explicitly decline a general no-OOM guarantee. Captured import logs count 53,176,328 records versus the reported 26M relationships, emphasizing orientation/representation multiplicity rather than a pointer-only memory explanation. The summary calls unweighted undirected Cypher shortestPath 'equivalent' to GDS Dijkstra without providing matching weight, projection, direction and output contracts. That equivalence is not established. Preserve it as a regression scenario, not a demonstrated modern algorithm speedup. P0936 separately retains the complete Aura source-load and restart-restoration exceptions; compute isolation does not remove projection/writeback interference.

### O31: Correct The All-Heap And No-Persistence Simplifications

P0973/P1014, `L6139`/`L6287`, captured system requirements explicitly add Arrow direct/native receive memory before conversion to on-heap graph storage. Therefore 'GDS operates on heap' is not a whole-process allocation inventory. Include concurrent batches, client buffers, projection conversion and writeback transaction memory. P0975/P1010 expressly say passing heap admission does not guarantee success; this limits any reading of the earlier production-estimator guarantee. P1009, `L6271`, graph-list heap bytes can return -1 on unsupported JDKs: unknown is not zero, and heap graph bytes are not peak RSS. P0985, `L6159`, [backup/restore capture](https://neo4j.com/docs/graph-data-science/current/management-ops/backup-restore/) saves graph/model state to disk and reloads it into RAM. It retains one backup, overwritten by the next; count backup construction, durability, restore latency and old/new overlap, not just steady-state file size. This is existing persistence, not evidence of query-time spill. No external pages were refetched.

### O32: Session Readiness, Connection Scope And Measurements

P1030, `L6354`, session API timeout specifically waits for Ready, while session estimation accepts counts and algorithm categories. First-answer cost starts before Ready and includes projection plus full export; a tiny example estimate is not a 50GB artifact measurement. P0971/P1007/P1025 all repeat the seven-day maximum lifetime and inactive expiration/deletion distinctions; repeated captures are not independent discoveries. P0954, `L6078`, Ladybug's actual captured page warns that projected graphs are tied to a Connection, causing an async API visibility failure. A cache keyed solely by graph name can therefore violate session scope; transferring a logical projection needs connection and snapshot identity. The nearby summary P0944 wrongly extrapolates no materialized projection into no algorithm memory, contradicted by the captured Louvain CSR in O22.

P1084-P1097, `L6446`/`L6451`, Memgraph release summaries describe optional lightweight edges, allocation tracking categories and failed-recovery broken state. These are baseline-version parameters, not verified byte savings of this reader. Its memory-limit unit is MiB; defaults based on swap presence do not fit the goal's physical-byte envelope automatically. Aborting a transaction after a memory limit is reached is a failure outcome, not successful bounded-memory computation. P1053 supplies explicit staff-role metadata for the projection discussion, stronger than an avatar alone; P1073's third-party 'permanently until restart' warning conflicts with documented graph drop and should not be adopted as a universal lifecycle statement.

### O33: Import Modes And Memory Optimizations Change The Contract

P1103-P1132, `L6463-L6471`: Memgraph EDGE IMPORT MODE permits relationship creation/modification but prohibits node writes. A bounded end-to-end import must order node construction and relationship ingestion accordingly, and define concurrent refresh restrictions. Lightweight edges leave vertex adjacency unchanged, optionally requiring separate edge metadata for ID lookups; changing flags is not a free universal 24-byte saving when identity lookup is required. Captured property-store zlib compression is existing prior art for payload compression and adds decompression work for predicates. Its 'double dataset RAM' rule is a heuristic, not a bound for aggregation or large result sets. The splice at P1108 ('two nodes which are not under constraint') and P1111 joins nonadjacent excerpts; do not interpret them as continuous source prose. Query-memory monotonicity alone cannot prove all possible algorithms lack spill, though the separate transaction-fit requirement directly constrains the documented mode.

P1216-P1251, `L6547`, complete captured README: Onager explicitly warns of early-development bugs/breaking changes and binaries tied to the DuckDB version they were built against. The README offers MIT or Apache-2.0, while the API metadata reports only Apache; a single repository metadata field is not a complete license inventory. P1264-P1280, `L6563`, explicitly calls the registry in-memory, now independently corroborating the source representation. The guide cuts off at its delete example; omitted text is not credited.

### O34: Business Interpretation Must Match Graph Semantics

P1282, `L6599`, manufacturing article says WCC checks whether all machines can reach each other. Counterexample: a directed chain has one weak component but cannot reach backward; WCC ignores direction and does not establish mutual reachability, production-flow resilience or capacity. Its 'zero data movement' Snowflake claim must be read as a hosting/governance statement, not zero bytes copied: the primary Snowflake guide excerpt in the same paragraph explicitly creates node/edge tables, projects, computes and writes results back. The 20-machine lab is not evidence of a 50GB/4GB production workload. The entity-resolution excerpt correctly separates candidate generation, pair scoring, clustering and golden-record consolidation; those earlier/later stages must be budgeted. A false-positive bridge can merge two identities under WCC, so exact graph connectivity does not certify real-world identity correctness.

P1281, `L6595`, comparison marketing makes universal 'constant-time hop expansion', language-based memory and drop-in claims without matched contracts. A hub requires work proportional to neighbors emitted; a path with exponential result multiplicity cannot be summarized as a reached set without changing semantics. P1282's 2016 network-analysis anecdote requests interactive hypothetical edge changes rather than static scores: snapshot-first must state its refresh/what-if latency tradeoff. P1283's already-read distributed launch/reprints concern enterprise multi-machine operation and independent compute/storage billing, not a local 4GB algorithm or new proof of query-time spill.

All performance claims remain historical unless a complete benchmark contract and measurements are established; this reader asserts no measured speedups.

### O35: Refresh Rehearsals Must Include Recovery Traffic

`L6605`, P1284 full: the captured [RevenueCat Aurora migration postmortem](https://www.revenuecat.com/blog/engineering/postmortem-aurora-postgres-migration) describes snapshot, upgrade, replication and short read-only cutover. Recovery traffic and monitoring EXPLAIN activity exposed resource problems omitted by a read-only rehearsal. Test refresh with writes, export clients, queued recovery traffic and monitoring enabled, not just checksum/read correctness. The title identifies November 2022 while search metadata differs; wrapper date is not incident date. The captured GitLab 2017 restoration account similarly makes restore bandwidth a separate recovery constraint, not something a RAM cap solves. Neither incident supplies a measured graph-engine speedup.

The adjacent PostgreSQL-as-graph discussion suggests mutation-driven tests of forest/closure consistency. Its constant-time adjacency argument applies to a particular access step, not output-sized neighbor expansion or cold random I/O. The paywalled regret article is only the captured portion, not a fully read original article.

### O36: GPU And RAG Baselines Have Extra Stages

`L6608`, P1285 full: cuGraph's captured construction, PageRank and full-result sort use a GPU hardware profile. Quoted A100 acceleration and cost claims cannot be charged to a CPU-only 4GB machine or inherited as our measurements. Showing ten results after sorting the whole vector is not a bounded top-k implementation. The captured Microsoft GraphRAG documentation includes extraction, hierarchy construction and summaries; a product promising that workflow must account for these stages and their LLM/vector costs. Vendor memory-service accuracy or 'fastest' tables are not exact graph-algorithm measurements. The graph-reasoning-agent abstract demonstrates existing orchestration prior art, not a local memory guarantee.

### O37: Issue State And Sampling Are Not Correctness Certificates

`L6732`/`L6736`/`L6739`, P1385-P1396, give the body and metadata of [GraphRAG issue 2348](https://github.com/microsoft/graphrag/issues/2348). Closure reason is `not_planned`, not a confirmed fix. Its AI-assisted report and 10/151 count describe one reported dataset. Preserving null community levels addresses dropped unassigned entities, but exploding hierarchy memberships can still duplicate entity rows; distinct-entity cardinality and level selection need separate tests. Cross-references do not certify resolution.

`L6704`, P1371-P1372, is a distinct 2022 Neo4j forum discussion recommending subsets when projection does not fit. `L6835`, P1451, reiterates that estimates concern GDS, not all DB memory. Sampling that first requires the full GDS projection cannot bootstrap an input that already exceeds that projection budget. External sampling changes the contract. Conversely, the nearby blanket dismissal of 0.1% samples is too strong: statistical representativeness depends on method and estimand, although sampling cannot generally preserve exact whole-graph answers.

### O38: Out-Of-Core Search Hits Need Algorithm-Scope Qualification

`L6851`, P1462-P1468, contains short ATLAS, ACGraph, DiskGNN, Kedagraph, GraphCP and Taiji descriptions, not fully captured papers. GNN training/inference, concurrent jobs and computational-storage processing are different workloads; no general GDS parity or 4GB/50GB guarantee follows. `L6865`, P1483-P1487, records a GraphFrames triplet-memory question and a custom PageRank anecdote, not a universal impossibility result. `L6866`, P1488-P1494, distinguishes Aerospike memory tiering from TigerGraph and quotes disk-compression marketing. Disk compression ratio is not resident algorithm-state reduction. Search headings claiming release features without feature body content are likewise insufficient evidence. No linked paper or page was recursively opened.

### O39: Capacity Units And Duty Cycles Must Not Be Mixed

`L7116`, P1495 full, captured [Neptune pricing](https://aws.amazon.com/neptune/pricing/): Analytics m-NCU represents a different product unit from Database Serverless NCU; stopped Analytics still incurs a charge, and the workbench notebook is independently billed. The example assumes two hours at 256GB, not a measured job. `L7127`/`L7132`, P1498/P1503, selected price-list rows confirm the captured 0.03 USD per m-NCU-hour arithmetic and 10% stopped rate in that region/version. They do not certify every listed SKU is available to a given account. `L7138`, P1505, labels EC2 memory GiB but reports a GB-hour ratio; dimensional conversion is required before comparison. Its zero-dollar metal row is not sufficient evidence of a free purchasable server.

Annual 8,760-hour and monthly 720-hour extrapolations have different time denominators. Compare actual active, import, pause, restore and export duty cycles; include standby/backup, source DB, client/notebook and transfer costs where applicable. A repeated daily two-hour example is not a 24/7 workload. The 50GB Database storage example is a paid storage quantity, not a GDS projected-RAM requirement or proof of 50GB local artifact feasibility. The capture omits some I/O arithmetic, so no complete monthly total is reconstructed from it. All prices are historical captured evidence, not current purchasing advice.

### O40: Negative Search Results Have Measurable Coverage Limits

`L7186`-`L7226`, P1523-P1544, and `L7379`-`L7429`, P1595-P1620: RSS results for 'graph database' do not match the public search index, and raw 'graph' matches include image names. The captured index reports 1,134 hits, but the extraction fetched 1,000 records, 875 with empty bodies, with some remaining bodies apparently truncated around 8,000 characters. A known GraphRAG paragraph appears at visible-text offset 20,489 while its indexed body is empty. The later 592-page local crawl still excludes external video/podcast/community material and is not the whole FinOps corpus. Thus no search absence establishes no graph-cost pain, low market demand or global novelty. All captured boilerplate, scripts and errors were read; search identifiers/API material are not copied into these notes or used to fetch anything.

### O41: Procurement Amounts Do Not Price A Kernel Replacement

`L7309`-`L7332`, P1578-P1585: captured USAspending/FPDS records include a roughly 4.498M USD, three-year license renewal, multi-product awards, training, hardware and support. The renewal's later listed modifications have zero additional obligations: counting every modification's headline amount would duplicate spending. Obligated amount, award ceiling, outlay, annual cost and pure software price are distinct quantities even where one record happens to show equal values. The search count is keyword/filter-specific, not total Neo4j revenue. These primary administrative records establish reported procurement, not dissatisfaction, a universal per-GB quote, or that a 4GB analytics companion can replace security, support and operational terms. Private/contact/location identifiers are omitted from notes; only public technical fiscal observations retained.

### O42: Historical Tier Changes Are Not Like-For-Like Savings

`L7603`/`L7607`, P1666-P1667, and `L7618`-`L7634`, P1670-P1678, capture the [2024 Aura announcement](https://neo4j.com/press-releases/neo4j-aura-update/) and archived pricing excerpts. The announced 20%+ lower Business Critical price is relative to the former Enterprise offering; dedicated-environment requirements continue in VDC. It is not a demonstrated reduction for identical isolation, security, backup and support obligations. The 15x real-time read-capacity statement is vendor cluster throughput marketing, not one-query latency, graph-size compression or measured GDS speedup. Snapshot timestamps bracket captured pages, not necessarily the exact moment a feature became purchasable. Public pricing tables and catalogue versions can disagree; pin one date, product, region, unit and contract before cost comparison.

`L7655`-`L7680`, P1679-P1688, show a Discovery bundle description moving from 69,000 to 123,750 USD while the later obligation is 127,957.50 and outlay differs again. Without line-item entitlement equality, that is not an established same-product unit-price increase. A separate 512GB/24-core/four-production-machine SKU description demonstrates a specific entitlement dimension, not a universal RAM-only licensing law; its zero-dollar record is not a free offer. The heading 'since 2025' includes older starts, so date-filter semantics need validation before trend analysis. Unrelated procurement and personal details were read but are not copied as graph evidence.

### O43: Product Scope Includes More Than Algorithm Names

`L7501`, P1642, captured [FinOps database-design guide](https://www.finops.org/wg/why-architecting-databases-for-cost-efficiency-matters/), explicitly excludes several analytics systems while discussing license, support, SLA, network and vendor-lock-in costs. This reinforces lifecycle accounting but cannot serve as a comprehensive graph-analytics market census. `L7705`/`L7708`, P1690 and the consumed prefix of P1691, enumerate edition-specific CDC, property access control, backups, model persistence and analytics format. CSV ingestion plus PageRank does not imply parity with these operational contracts. A low-memory artifact must either preserve permission/filter semantics or explicitly narrow its supported deployment; a snapshot export can otherwise disclose data that later access rules would deny.

### O44: Low-Memory Format Is Existing Baseline, Not A Spill Proof

`L7708`, P1691 now fully read: the pricing matrix explicitly lists low-memory analytics graph format for Enterprise and Infinigraph, not Community. This strengthens the requirement to benchmark the applicable edition/representation rather than assert all GDS graphs use pointer objects. A feature-table label does not reveal its implementation or prove out-of-core execution. `L7711`, P1692, distinguishes Aura Graph Analytics from bundled AuraDS and self-managed GDS; 'no ETL' marketing does not waive ingestion/projection. Source AuraDB is explicitly not included in the session offer. Its four-CPU Community limit is an edition-specific baseline constraint, not a mathematical algorithm restriction.

`L7727`/`L7736`, P1697/P1700-P1703: 0.09/hour times 720 is 64.80, whereas times 730 is 65.70. This monthly-display change alone is not a rate increase. The old 64GB row's 4,147.02 does not equal 5.76*720 = 4,147.20; retain the source discrepancy rather than silently promoting it as exact. `L7766`, P1707, is a fuller Neptune capture than O39 and supplies I/O totals previously absent: the stated 296.61 and 5,740.10 examples reconcile with their components, while 350.56 includes rounded instance cost. These remain examples, not observed workload bills.

### O45: A Scalar Result Can Still Trigger Unbounded Execution

`L7793`, P1723 full, captured [Neptune Analytics limits](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/analytics-limits.html): whole-vertex enumeration/counting is described as not memory bounded, even for `MATCH (n) RETURN count(n)`. Replacing that with a per-label count is not equivalent when the intended population spans unlabeled or overlapping labels. A stored exact count maintained during import/refresh is a useful bounded-result product feature, but must preserve selected-graph and transaction semantics. The same page limits algorithm parameterization and embedding-only labelless vertices; API names alone do not give compatibility.

The captured [creation guide](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/gettingStarted-creating-a-graph.html) describes up to 20 minutes before availability depending on configuration, a documented bound statement rather than a measured latency here. Replicas have extra m-NCU charges; default configurations differ across console/CLI/template examples. The creation page says approximately GiB per m-NCU while pricing says GB: reconcile source unit ambiguity, do not assert exact physical bytes from the marketing label. `L7799`, P1725, API minimum/maximum defaults differ depending on which import bounds are supplied. Integer min/max in an API model plus catalogue SKUs does not prove every integer capacity is accepted. The modifying page requires all stored data to fit the chosen memory size, not spill-to-disk downscaling.

### O46: Price And Dissatisfaction Anecdotes Need Controls

`L7864`, P1739-P1747, a Vendr-attributed summary gives equal mean/median and broad price/discount ranges without transaction sample size, scope or normalization. Its 32GB range conflicts with other captured public tier totals, so it is not a replacement for edition-specific primary prices. `L7870`, P1750-P1751, includes both an unaffordable scaling anecdote and a positive low-price quote from 2009. The hiring hit in P1753 is salary, not license cost; unrelated mentions must not inflate demand counts. Partially captured benchmark criticisms disclose a competing vendor affiliation and no use of one compared engine; opinion is not a reproduced comparative result.

### O47: Ownership Paths And Atomic Refresh Are Concrete Product Contracts

`L7970` and `L7980`, P1844-P1869, captured [TrustRadius review text](https://www.trustradius.com/products/neo4j/reviews): beneficial ownership needs edge dates and products of ownership percentages along bounded paths. A reached-node BFS discards path multiplicity and weights. A diamond of two ownership chains can reach the same subsidiary with different contributions; cycles require an explicit bounded-path or domain-accounting rule, not an assumed tree. The review's 'single pass' language does not guarantee linear time over a DAG with exponentially many paths.

The approximately 600M-node/1.5TB account reports slow backup, count timeout, hours-long restart and batching that commits incrementally. These are reported observations without pinned version/configuration, not universal current limits. Nevertheless, a replacement should test whole-job atomic publication or a documented resumable batch contract. Bounded import with partial commits is not equivalent to all-or-nothing rollback. Other reviews praise continuous website queries, supply-chain use, visualization and free community deployment; these positives and conflicting language-support claims prevent treating review excerpts as a representative defect census. An adjacency lookup can be constant-time without all joins or complete paths being constant-time.

### O48: Reviews Reveal Product Needs, Not Automatic Baseline Facts

`L7990`-`L8050`, P1909-P1952, captured [AuraDB review/pricing pages](https://www.peerspot.com/products/neo4j-auradb-reviews) repeat the same reviewers across multiple summaries. Source page update date is not each review's date; role/company-size inference and quote attribution vary between summaries. The review claim that paid costs scale directly with node/relationship counts must not override the captured capacity-based billing contract. Data growth can cause a capacity step without per-node metering. Free-tier users and temporary startup credits are not evidence of recurring paid unit economics.

The fuller captures describe an alleged one-hour emergency migration window during a marketplace transition, dependency/recommendation imports requiring batching, cold starts, external monitoring and CI seeding. These motivate a self-contained export manifest, complete export deadline, restart/recovery test and predictable refresh timeline. This is reported experience, not independently verified incident causality. The BOM reviewer says analytics was disabled, so plugin/multi-database/administration complaints are not a GDS kernel-memory measurement. The documentation-graph reviewer attributes less storage to avoiding duplication in its previous model; its reported 20% reduction and 40-50% development saving do not establish a compressed graph representation or our performance.

### O49: Queueing And Import Guidance Need Explicit Versioned Contracts

`L8106`, P1980, fully consumed selected PDF spans: differing import minimum/maximum values are described as allocating the maximum, unlike the earlier API summary's chosen-per-data-size language. Do not budget optimistic auto-sizing while this discrepancy is unresolved. The excerpts are not a full PDF read. `L8130`, P1990, captured [concurrency documentation](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/query-concurrency-queuing.html), gives worker-thread and workload-dependent concurrency guidance, explicitly not guarantees; algorithms may consume several workers. It defines latency as queue plus network plus execution, with an 8,192 queued-request limit and rejection beyond it. Local 4GB admission likewise needs aggregate state and bounded waiting, not just a per-job cap. String/metadata filters add work before final results.

`L8122`-`L8141`, P1987-P1994: the 2024 widget has 14 capacity selectors starting at 128 while the 2026 offer list has 17 running SKUs including 16/32/64 and separate stopped SKUs. A `current` URL can serve a dated manifest. Use provenance/versioned content, not endpoint spelling or a single older widget, to infer support. Captured compressed-byte decoding errors are unreadable payload evidence, not recovered source prose. No attempt was made to reconstruct lossy binary text or fetch missing files.

### O50: Preview Capabilities And Duplicate Sources Need Boundaries

`L8197`, P2007-P2021, captured [Infinigraph architecture announcement](https://neo4j.com/blog/graph-database/infinigraph-scalable-architecture/), describes property-based read/traverse access controls, adjustable disk storage and multiple databases with an Early Preview/GA-planned qualification. This limits the earlier one-database review's generality without proving a given tier now supports it. Adjustable storage is already product prior art and distinct from out-of-core GDS. `L8214`, P2029, repeats the same staff 12TB-RAM/prioritization statement, not an independent customer measurement. `L8248`/`L8252`, P2034-P2035, are issue-title inventories: version conflicts and incremental Leiden requests must be inspected beyond titles before asserting current behavior. The surrounding AGA press release's customer-reported accuracy/precision percentages are different metrics from exact graph-algorithm equivalence.

## Lifecycle Synthesis

### O51: Job Identity Is Part Of Correct Benchmarking

`L8263`, P2039 suffix, `L8267`, P2040, and `L8271-L8283`, P2042-P2048: captured AGA session/client documentation separates attached, self-managed remote projection and standalone construction. The example Pandas construction is not proof of bounded input memory. A 10,000-record transmission batch does not bound the source query, projected graph or all concurrent receive buffers. Session build concurrency and database parallel-query concurrency are separate controls. Expiration, a one-hour default inactivity TTL, a seven-day maximum lifetime and subsequent deletion are different events in this capture; unwritten results can disappear. These are historical values, not a fresh service guarantee.

The 1.22 release capture specifically fixes `stream_job` starting a new computation instead of streaming an existing job. A verification-first experiment must bind run/job identity and assert export does not rerun the kernel; report retries and duplicate work. The 1.21 Arrow-to-Cypher authentication fallback and missing `memoryUsage` field fix require pinning actual transport and telemetry availability. Session support dates in the release history (attached in 1.11a1, self-managed in 1.12, standalone in 1.15) refute novelty for transient remote analytics. The 1.14 main-thread signal restriction qualifies cancellation claims. Smaller 2GB session enums in later captures qualify an older universal 4GB-minimum assertion. Lexically renaming Gi to GB still does not convert units. Captured primary documentation: [GDS Python client sessions](https://neo4j.com/docs/graph-data-science-client/current/sessions/), [client release notes](https://neo4j.com/docs/graph-data-science-client/current/changelog/); not refetched.

### O52: Launch Marketing Is Not An Exact-Algorithm Experiment

`L8287-L8290`, P2051-P2052: the captured May 7, 2025 launch statement labels the 80% accuracy, twice-as-fast insights and lower-code figures as customer-reported outcomes. Embedding/ML accuracy and insight quality are different quantities from exact PageRank error or BFS distance correctness. Its Zero ETL claim coexists with Pandas projection and remote processing, so it cannot be budgeted as zero conversion, zero transfer or zero RAM. Planned additional-language and Snowflake support are not accomplished capabilities merely because they appear next to a general-availability announcement. The 404 captures at P2050/P2064 do not negate the documented service. [Captured launch release](https://neo4j.com/press-releases/aura-graph-analytics/), not refetched. `L8294-L8301`, P2054-P2063 repeats the same 12TB forum exchange, not another independent customer.

### O53: Search And Taxonomy Absence Are Different Claims

`L8338-L8402`, P2069-P2092: captured client scripts/search responses and database-design snippets were read, including errors; captured key-like values are intentionally not reproduced. The index returns zero for a known page/title and sparse bodies; its 1,134-hit count cannot establish a census of all site prose. WordPress supplies results absent from the other index. Broad `graph` matches include geography, graphics and dependency-rule graphs, not necessarily graph databases. FOCUS working-draft source at commit `9417d416175877661273d782dcdfb7f56dab141d` explicitly lists Neptune under NoSQL Databases in the servicesubcategory table; lack of a separate graph-database category is not lack of cost coverage or demand. This is a draft taxonomy capture, not a production billing entitlement. [Captured primary repository](https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec), not fetched. The index limitations strengthen O40; they do not establish a market opportunity by themselves.

### O54: Renewal Inferences Need Configuration And Transaction Evidence

`L8424-L8470`, P2093-P2110: the detailed 2024 record contains description price 123,750, obligation 127,957.50 and outlay 128,078.79. They are not interchangeable. The 2023 record has 69,000 for all three, but no entitlement/configuration identity is supplied between years. FPDS extracts show two entries per award, with one zero-dollar administrative modification, not two purchases or proven renewals. Broad date queries include old parent-award starts and post-period actions; negative keywords after expiry cannot prove cancellation, no successor purchase or no graph usage. The 28-award median mixes periods, bundles, hardware/services and zero-value records; it is not a median graph license unit price. Government-wide records are not statistically representative of all developers. Captured public procurement identifiers are traceable through the pointer map; personal addresses, individual compensation and requestor names are omitted. Primary captured source domains: [USASpending](https://www.usaspending.gov/), [FPDS](https://www.fpds.gov/); no fresh financial advice or query was performed.

### O55: A Pricing Summary Is Not The Underlying Review Population

`L8492-L8500`, P2111-P2124: the selected six-person pricing summary is followed by a fuller captured page containing additional free-tier/student perspectives, managed-service savings and subscription-governance concerns. A 4.3/5 overall rating from 14 reviews is not a price statistic. Startup credits are not recurring unit pricing; one quoted reviewer explicitly did not purchase the service. The last capture provides dates for some reviews that the summary marked unavailable, while a failed name search starts at unrelated page navigation. Do not treat the summary as complete review coverage or the negative search as evidence of a nonexistent person/review. The waterway use case specifically needs URI/OpenStreetMap identity, directed connectivity, infrastructure properties and refresh, not just an unlabeled topology. These are captured customer narratives, not controlled 4GB measurements. [Captured review page](https://www.peerspot.com/products/neo4j-auradb-reviews), not refetched.

### Corrected Parameterized Envelope

Let R = 4,000,000,000 physical bytes and A = 50,000,000,000 prepared-artifact bytes (decimal). These are separate constraints. Historical 50GB projected GDS RAM is neither this disk budget nor a justified compression denominator. Let V be retained vertices, E stored directed adjacency entries after orientation/multiplicity policy, L retained external-ID bytes, P required property bytes, H metadata/index bytes, O exported result bytes, U refresh delta bytes and W temporary workspace bytes. Required properties include filters, weights, permissions and output payload, not merely display data.

For every execution phase and allowed overlap, require `OS + co-resident services + resident file pages + anonymous graph/state memory + native/transport buffers + parser/sort/queue buffers + export buffers + safety reserve <= R`. Count shared pages once, but do not omit them. A mapped 50GB file is not 50GB resident and also does not guarantee a bounded, fast working set. A library heap/buffer limit is not the physical peak. Set an explicit OS/reserve allowance, then derive the remaining budget; an illustrative 1GB allowance leaves 3GB, not a measured safe allowance. Concurrent queries and refresh must share that remaining budget, or run sequentially with measured waiting time.

For a plain CSR example, topology disk is approximately `w_off*(V+1) + w_id*E`; reverse adjacency costs another structure if needed. Add L, P, H, stored algorithm results/certificates and retained deltas. A 4-byte endpoint requires a validated dense-ID range; offsets may still need 8 bytes. Compression is data-dependent and its build/decode metadata counts. PageRank with two double vectors alone uses 16V bytes; BFS needs visited/frontier and, where requested, distances/parents. These are floors for specific representations, not universal algorithm RAM requirements. At V=100M, 16V=1.6GB; whether the complete workflow fits depends on every other term and its access pattern.

Enforce `D_prepared <= A`, with an explicit manifest deciding whether cached outputs and old versions are retained prepared state. Separately report physical disk peak `D_input_retained + D_old_live + D_new_unique + D_scratch + D_outputs + D_logs/backups`. Shared immutable extents count once physically, not twice by filename; hard links do not share changed extents. If 50GB is also the whole available device, this entire simultaneous sum must fit 50GB. If only prepared artifacts are capped, additional workspace must still be declared rather than hidden. A 44GB current artifact leaves about 6GB, not room for another full 44GB snapshot or arbitrarily large sorted runs.

### Lifecycle Gates And Useful Scope

| Phase | Mandatory accounting and verification | Useful bounded scope; counterexample |
|---|---|---|
| Acquire/import | CSV quoting/null/type policy, stable external-ID mapping, source snapshot/transaction boundary, export/query memory on Neo4j, transport concurrency, parser maximum record size, reject policy and source bytes. | External-sort ID assignment and streamed input are plausible hypotheses. An unbounded quoted CSV field or in-memory Pandas frame defeats a nominal batch cap. |
| Build | Run formation, merge fan-in/file descriptors, degree/offset computation, forward/reverse index writes, dictionary and weight normalization; retain input/runs only as required by restart policy. | With B-byte runs over S bytes, roughly `ceil(S/B)` initial runs and `ceil(log_f(ceil(S/B)))` merge levels for fan-in f when more than one run exists. Each materialized level can read/write about 2S bytes; actual representation expansion and pipeline fusion must be measured. A high-degree row must itself be splittable. |
| Prepared artifact | Schema/version/checksum manifest, topology, reverse index, ID map, required properties, certificates/results and retained generations. | Reject the candidate before a kernel run if its measured manifest exceeds 50GB. Arbitrary high-entropy properties cannot be assumed compressible. |
| Query | Algorithm/parameter contract, initialization, iterations, state reads/writes, cold/warm page behavior, admissible concurrency, cancellation and output identity. | BFS reached sets differ from paths; WCC differs from directed mutual reachability; weighted/dangling PageRank and convergence must match. A path chain can require many scan rounds, while dense path output or pairwise similarity can dwarf input. |
| Export | All requested rows, external IDs/properties, serialization, sorting/top-k ties, transport/writeback transaction batches, durability and resumable job ID. | Output time is at least output bytes divided by sustainable sink rate. Streaming controls memory, not output size or completion time; a top-k presentation after full sort has already paid the full work. |
| Refresh | Capture delta exactly once, eligibility/ACL changes, index repair/rebuild, certificate invalidation, old/new concurrent readers, result regeneration, atomic publication and reclamation. | Snapshot-first can trade freshness for fit; it does not replace live workloads by definition. Hub edits, component splits, normalization changes and global rank dependencies can force near-full work. Fallback and peak overlap need a gate before accepting the update. |

`T_first_answer = T_source_acquire + T_build + T_validate/publish + T_ready/queue + T_query + T_first_export`; report complete-answer time by replacing first export with complete export. Do not amortize build out of a user's first answer. For each candidate, also report `T_refresh_to_fresh_answer`, old-result staleness and time blocked behind refresh. Sequential I/O gives only a lower bound `bytes_read/r_read + bytes_written/r_write` for nonoverlapped operations; random I/O, decompression, CPU, contention, convergence and failure recovery can dominate. No throughput constants were measured here.

The defensible product target is a declared family of prepared analytical queries over a reproducible snapshot, with bounded admitted sizes and complete export/refresh contracts. Stronger hypotheses include artifact selection by algorithm family, external-memory state where necessary, exact reuse under proved update conditions and selective property loading where semantics permit. This archive establishes neither global novelty nor measured speedups for those ideas. Baselines must include current-version, correctly configured native/Arrow GDS projection, valid estimator settings, primitive/compressed representations, existing direct/live approaches and simple SQL/file baselines where the requested answer needs no graph engine. Primary captured sources above are historical evidence, not a fresh web audit.

## Exact Deduplication Pointer Map

Final independent ledger audit: 84 batch rows cover every character of all 2,124 paragraphs exactly once, with no gaps or overlaps. Every map length, SHA-256 and ordered source-pointer list matches fresh parsing of the assigned physical span. The 2,124 canonical paragraphs represent 2,644 paragraph occurrences across 1,083 fields and 793 distinct content values. Total canonical text is 1,830,784 UTF-8 bytes / 22,266 decoded outer-text lines. There are 124 tool-reference occurrences and one image occurrence; tool-reference structures carry no prose research claim. Counts and hashes corroborate the explicit reading ledger; they are not substitutes for reading.

Machine-derived identity metadata, not read credit. P identifiers follow first appearance in the assigned range. `L` is physical JSONL line; `B` is zero-based `message.content` block index; `s` denotes a string content value, `eN` a zero-based text element of list content; `p` is zero-based blank-line paragraph ordinal; `dA-B` is the inclusive decoded text-line span in that string/text element. Character spans in the reading ledger are one-based Unicode-code-point positions within a paragraph. First pointer is canonical; later pointers contain exactly the same paragraph value. Hashes are SHA-256 of decoded UTF-8 text. No duplicate is exempted until its canonical paragraph is fully read.

| Canonical | Characters | SHA-256 | All exact occurrence pointers |
|---|---:|---|---|
| P0001 | 149 | 9b3532ef0dfc703d8792a6bd702ac610a5398c5f96d8eab84f527cf4d002b6e9 | L4502.B0.s.p0.d1-1; L4509.B0.s.p0.d1-1; L4523.B0.s.p0.d1-1; L4553.B0.s.p0.d1-1; L4559.B0.s.p0.d1-1; L4582.B0.s.p0.d1-1; L4589.B0.s.p0.d1-1; L4601.B0.s.p0.d1-1; L4651.B0.s.p0.d1-1; L4657.B0.s.p0.d1-1; L4678.B0.s.p0.d1-1; L4860.B0.s.p0.d1-1; L4866.B0.s.p0.d1-1; L4872.B0.s.p0.d1-1; L4873.B0.s.p0.d1-1; L4936.B0.s.p0.d1-1; L4972.B0.s.p0.d1-1; L4986.B0.s.p0.d1-1; L5018.B0.s.p0.d1-1; L5052.B0.s.p0.d1-1; L5053.B0.s.p0.d1-1; L5057.B0.s.p0.d1-1; L5079.B0.s.p0.d1-1; L5155.B0.s.p0.d1-1; L5160.B0.s.p0.d1-1; L5161.B0.s.p0.d1-1; L5164.B0.s.p0.d1-1; L5342.B0.s.p0.d1-1; L5343.B0.s.p0.d1-1; L5346.B0.s.p0.d1-1; L5433.B0.s.p0.d1-1; L5439.B0.s.p0.d1-1; L5660.B0.s.p0.d1-1; L5666.B0.s.p0.d1-1; L5669.B0.s.p0.d1-1; L5797.B0.s.p0.d1-1; L5802.B0.s.p0.d1-1; L5835.B0.s.p0.d1-1; L5880.B0.s.p0.d1-1; L5881.B0.s.p0.d1-1; L5884.B0.s.p0.d1-1; L5963.B0.s.p0.d1-1; L5967.B0.s.p0.d1-1; L5976.B0.s.p0.d1-1; L6005.B0.s.p0.d1-1; L6121.B0.s.p0.d1-1; L6127.B0.s.p0.d1-1; L6136.B0.s.p0.d1-1; L6151.B0.s.p0.d1-1; L6200.B0.s.p0.d1-1; L6207.B0.s.p0.d1-1; L6223.B0.s.p0.d1-1; L6254.B0.s.p0.d1-1; L6276.B0.s.p0.d1-1; L6336.B0.s.p0.d1-1; L6380.B0.s.p0.d1-1; L6385.B0.s.p0.d1-1; L6386.B0.s.p0.d1-1; L6389.B0.s.p0.d1-1; L6530.B0.s.p0.d1-1; L6536.B0.s.p0.d1-1; L6586.B0.s.p0.d1-1; L6587.B0.s.p0.d1-1; L6590.B0.s.p0.d1-1; L6668.B0.s.p0.d1-1; L6673.B0.s.p0.d1-1; L6677.B0.s.p0.d1-1; L6766.B0.s.p0.d1-1; L6774.B0.s.p0.d1-1; L6775.B0.s.p0.d1-1; L6828.B0.s.p0.d1-1; L6834.B0.s.p0.d1-1; L6840.B0.s.p0.d1-1; L6841.B0.s.p0.d1-1; L7143.B0.s.p0.d1-1; L7275.B0.s.p0.d1-1; L7276.B0.s.p0.d1-1; L7395.B0.s.p0.d1-1; L7779.B0.s.p0.d1-1; L7858.B0.s.p0.d1-1; L7859.B0.s.p0.d1-1; L7921.B0.s.p0.d1-1; L7922.B0.s.p0.d1-1; L8148.B0.s.p0.d1-1; L8185.B0.s.p0.d1-1; L8249.B0.s.p0.d1-1; L8447.B0.s.p0.d1-1; L8468.B0.s.p0.d1-1 |
| P0002 | 44 | cc40022d317b1c6bc01840aca94cb45d3c545ec06ca53f29accac2d374b308cb | L4503.B0.s.p0.d1-1 |
| P0003 | 120 | fd0f8d3de88b29f130a7bf57745bc1859208fe3b479c2b3ff5e6cc1fbd8365e3 | L4503.B0.s.p1.d3-4 |
| P0004 | 197 | 6620a7d315a373dd928ae9d37e4016865fe7378186b57f5d9746a9b58c17f736 | L4503.B0.s.p2.d6-6 |
| P0005 | 33 | 69465bcc49aa1283875b27d12f046a758882c326e58c44c7813d0f4c0c717021 | L4503.B0.s.p3.d8-8; L4867.B0.s.p3.d8-8; L6835.B0.s.p3.d7-7; L8301.B0.s.p2.d5-5 |
| P0006 | 170 | a4a7b148e5977108c1962bc06a2480b58ad7920cda49059ae13d74943c00f087 | L4503.B0.s.p4.d10-16; L4867.B0.s.p4.d10-16; L6835.B0.s.p4.d9-15; L8301.B0.s.p3.d7-13 |
| P0007 | 30 | ec71a76c54a583977ec1a055d4d098f3810682e36b4579d0f2556ac985a89026 | L4503.B0.s.p5.d18-18; L4867.B0.s.p5.d18-18; L6835.B0.s.p5.d17-17; L8301.B0.s.p4.d15-15 |
| P0008 | 109 | 02e055a6ca64421f0956d4bd62b55c724d41b141fdec381016bf937f99c1d241 | L4503.B0.s.p6.d20-20 |
| P0009 | 82 | 268a1a0f3b480e09e396267fb69540bc3942798ced9bb7992e9d831a19dfdfe4 | L4503.B0.s.p7.d22-22 |
| P0010 | 101 | a36b5335d610843a9344c985f9ca2b14e67afb116fa170cff6fb8308f3c2d8c8 | L4503.B0.s.p8.d24-28 |
| P0011 | 17 | 70edf88256797ff4007345e191ae9e43ac85b1b775cb7cfadc6ae058018fcc5f | L4503.B0.s.p9.d30-30 |
| P0012 | 253 | 8c66e6c03d1bc46d7102ac4466d2daf574094a83ec7e9fb8768f1fee991e9b2f | L4503.B0.s.p10.d32-32 |
| P0013 | 310 | e6ceae7935bf1426c9c4861becdde4fd0ae7b9bdb2ee56dae0d746d34e775f8d | L4503.B0.s.p11.d34-34 |
| P0014 | 119 | 0fb3c53b32770103df8b46518326f4d9501521b7d86fd84c864c0ef83898871a | L4503.B0.s.p12.d36-36 |
| P0015 | 3 | cb3f91d54eee30e53e35b2b99905f70f169ed549fd78909d3dac2defc9ed8d3b | L4503.B0.s.p13.d38-38; L4503.B0.s.p23.d59-59; L4503.B0.s.p27.d68-68; L4503.B0.s.p30.d75-75; L4503.B0.s.p37.d90-90; L4867.B0.s.p12.d37-37; L4867.B0.s.p18.d54-54; L4867.B0.s.p22.d63-63; L4867.B0.s.p25.d71-71; L4867.B0.s.p32.d86-86; L5172.B0.s.p6.d18-18; L5460.B0.s.p4.d13-13; L5561.B0.s.p5.d14-14; L5607.B0.s.p14.d57-57; L5636.B0.s.p4.d18-18; L6398.B0.s.p4.d21-21; L6547.B0.s.p5.d19-19; L6547.B0.s.p12.d49-49; L6547.B0.s.p32.d115-115; L6547.B0.s.p35.d121-121; L6553.B0.s.p14.d44-44; L6641.B0.s.p3.d7-7; L6641.B0.s.p9.d35-35; L6644.B0.s.p4.d10-10; L6644.B0.s.p8.d19-19; L6644.B0.s.p12.d28-28; L6644.B0.s.p15.d35-35; L6644.B0.s.p18.d42-42; L6732.B0.s.p4.d9-9; L6770.B0.s.p3.d8-8; L6770.B0.s.p6.d15-15; L6770.B0.s.p9.d22-22; L6835.B0.s.p12.d37-37; L6835.B0.s.p18.d53-53; L6835.B0.s.p22.d61-61; L6835.B0.s.p25.d68-68; L6835.B0.s.p32.d82-82; L7928.B0.s.p5.d19-19; L7954.B0.s.p6.d14-14; L8015.B0.s.p3.d11-11; L8015.B0.s.p8.d23-23; L8015.B0.s.p11.d31-31; L8015.B0.s.p15.d42-42 |
| P0016 | 120 | e0ce7ac1402de328a1c50476502905951dde477ae31e2cfa16364b25dc0edf1f | L4503.B0.s.p14.d40-41 |
| P0017 | 7 | d6348ea40210ad0bfce46a35aa0fadc6672be8592e5e816c10183cc5fd1de59d | L4503.B0.s.p15.d43-43 |
| P0018 | 238 | 08efb146fd5eaabccd7bbfe352c3edf6ad696da8cfaa3c210d3b16a0f3d77614 | L4503.B0.s.p16.d45-45; L4867.B0.s.p15.d44-44; L6835.B0.s.p15.d43-43; L8301.B0.s.p14.d41-41 |
| P0019 | 145 | c6f25f8cc580613e25a78d76d982c192b0d3d8f3c720d466bead3b1c31848099 | L4503.B0.s.p17.d47-47; L4867.B0.s.p16.d46-46; L6835.B0.s.p16.d45-45; L8301.B0.s.p15.d43-43 |
| P0020 | 215 | d62f7aed6bd5ad3dcec644fcdd9d2a47a7b8f7d0eadc194145db76eced5b9e8a | L4503.B0.s.p18.d49-49 |
| P0021 | 402 | e9a4289c91a06ac7dad5e6164c2d40b86b171c9f7c417482b805ce4fca344f1e | L4503.B0.s.p19.d51-51 |
| P0022 | 208 | f535f8ea327ce6f2ca9d4effbe8cf7d7824902fc315d485d6665f8404646d112 | L4503.B0.s.p20.d53-53 |
| P0023 | 425 | d6f30becad716ee94decca83dd0310396ab09f41b2b4b95cd433b8bb3bca91ff | L4503.B0.s.p21.d55-55 |
| P0024 | 192 | 5ed83a541863242fe03f8f5916ea8702c8cdf42bd18fa92c1b1f0d6ee3deda7e | L4503.B0.s.p22.d57-57 |
| P0025 | 120 | e9ddec31260759174c0d1254afd3688a0ab5f20feab4b32dcf20e25d83de110f | L4503.B0.s.p24.d61-62 |
| P0026 | 46 | b8bfa095eaed59de8594c11ba8f1366b48690ee4d047c7cd230e0402a2ee3daa | L4503.B0.s.p25.d64-64 |
| P0027 | 195 | a3c120444763af804b638013cc8d50bd2ffe3d57a5dedcf4dfe22d2be012fc02 | L4503.B0.s.p26.d66-66 |
| P0028 | 120 | 4a8618dc49c367dc3d61a38d9afa14ca2cec512aab1db109f6fde0b32ef67905 | L4503.B0.s.p28.d70-71 |
| P0029 | 458 | e8cc3bd965b237012c48c330846a7d23094ee8a144fa28579ae8decbbb0a12d6 | L4503.B0.s.p29.d73-73 |
| P0030 | 120 | 5c4fb757165a68fd783cef97dece0695195e105c8e9855ff4edbde301ea2c920 | L4503.B0.s.p31.d77-78 |
| P0031 | 150 | 6b2f92cdaeadec4387a061e5cedc7ebe5ed483acbff03f07ca0a5ffc8323513a | L4503.B0.s.p32.d80-80 |
| P0032 | 710 | 4478b1331a85836d3f7999e9ded0aa0b9bc2bc67f5cbd02e1ede991fda1cf6d4 | L4503.B0.s.p33.d82-82; L4867.B0.s.p28.d78-78; L6795.B0.s.p1.d3-3; L6835.B0.s.p28.d74-74; L8301.B0.s.p27.d72-72 |
| P0033 | 40 | 2f85640626d265bb37bdabe64c4d8cc9ba28c870cd95909befb4f8d30f8887f2 | L4503.B0.s.p34.d84-84 |
| P0034 | 103 | ad4a4402b82e22f91cd31f4fe2b026b10491701fb2f7d080d60788f06c1a6245 | L4503.B0.s.p35.d86-86 |
| P0035 | 272 | ce21f338b09b684b7ab61dc489e8fa94726149c7a5f661cc2c76b6da25ee6026 | L4503.B0.s.p36.d88-88 |
| P0036 | 120 | 08d0c073bf0ba23106d73b2df67169282e83d58e74596aa106f8ea284e5d95be | L4503.B0.s.p38.d92-93 |
| P0037 | 566 | 3014eeee412e6f9331c48713bfb94d66acddd924146ad1079c1cec6a4be3f644 | L4503.B0.s.p39.d95-95 |
| P0038 | 157 | d64eeaac3f29cebaa4240d20d6aeb6bcbdee47e7b639e0113f4b6b9a01dbc412 | L4507.B0.s.p0.d1-3 |
| P0039 | 2128 | aff49d20366ee288a42bb13fd1f5208a015aa396b7d7216cce7e691fa3beb82a | L4512.B0.s.p0.d1-9 |
| P0040 | 39 | bb17fff139e7e355388f5fdc6d06ce9a030b17bc85c61634f73273d7058b509d | L4514.B0.s.p0.d1-1; L4554.B0.s.p0.d1-1; L4652.B0.s.p0.d1-1; L4734.B0.s.p0.d1-1; L4798.B0.s.p0.d1-1; L4879.B0.s.p0.d1-1; L5019.B0.s.p0.d1-1; L5117.B0.s.p0.d1-1; L5169.B0.s.p0.d1-1; L5193.B0.s.p0.d1-1; L5350.B0.s.p0.d1-1; L5407.B0.s.p0.d1-1; L5440.B0.s.p0.d1-1; L5451.B0.s.p0.d1-1; L5674.B0.s.p0.d1-1; L5682.B0.s.p0.d1-1; L5798.B0.s.p0.d1-1; L5803.B0.s.p0.d1-1; L5969.B0.s.p0.d1-1; L5981.B0.s.p0.d1-1; L6122.B0.s.p0.d1-1; L6201.B0.s.p0.d1-1; L6255.B0.s.p0.d1-1; L6337.B0.s.p0.d1-1; L6410.B0.s.p0.d1-1; L6493.B0.s.p0.d1-1; L6496.B0.s.p0.d1-1; L6681.B0.s.p0.d1-1; L6699.B0.s.p0.d1-1; L6781.B0.s.p0.d1-1; L6789.B0.s.p0.d1-1; L6847.B0.s.p0.d1-1; L6860.B0.s.p0.d1-1; L7933.B0.s.p0.d1-1; L7935.B0.s.p0.d1-1; L7937.B0.s.p0.d1-1; L7958.B0.s.p0.d1-1; L8257.B0.s.p0.d1-1 |
| P0041 | 174 | 6b6befa18bc1672e3232db67d97267b670174eaa55dff7e13d55cfafe7238386 | L4514.B0.s.p1.d3-3; L4554.B0.s.p1.d3-3; L4652.B0.s.p1.d3-3; L4734.B0.s.p1.d3-3; L4767.B0.s.p1.d3-3; L4798.B0.s.p1.d3-3; L4879.B0.s.p1.d3-3; L5019.B0.s.p1.d3-3; L5117.B0.s.p1.d3-3; L5169.B0.s.p1.d3-3; L5193.B0.s.p1.d3-3; L5350.B0.s.p1.d3-3; L5407.B0.s.p1.d3-3; L5440.B0.s.p1.d3-3; L5451.B0.s.p1.d3-3; L5463.B0.s.p1.d3-3; L5674.B0.s.p1.d3-3; L5682.B0.s.p1.d3-3; L5798.B0.s.p1.d3-3; L5803.B0.s.p1.d3-3; L5969.B0.s.p1.d3-3; L5981.B0.s.p1.d3-3; L6122.B0.s.p1.d3-3; L6201.B0.s.p1.d3-3; L6255.B0.s.p1.d3-3; L6337.B0.s.p1.d3-3; L6410.B0.s.p1.d3-3; L6445.B0.s.p1.d3-3; L6493.B0.s.p1.d3-3; L6496.B0.s.p1.d3-3; L6681.B0.s.p1.d3-3; L6699.B0.s.p1.d3-3; L6781.B0.s.p1.d3-3; L6789.B0.s.p1.d3-3; L6805.B0.s.p1.d3-3; L6847.B0.s.p1.d3-3; L6860.B0.s.p1.d3-3; L7817.B0.s.p1.d3-3; L7880.B0.s.p1.d3-3; L7933.B0.s.p1.d3-3; L7935.B0.s.p1.d3-3; L7937.B0.s.p1.d3-3; L7958.B0.s.p1.d3-3; L7967.B0.s.p1.d3-3; L8257.B0.s.p1.d3-3 |
| P0042 | 577 | bd185976a28b92ec46ab926d968edc969502da268e62d9d9654c185787869071 | L4518.B0.s.p0.d1-8 |
| P0043 | 676 | 3498c3556bc80715ce5ee252e9ca25ad4f7dbaa372f6ee1d8054308ceab6fc88 | L4518.B0.s.p1.d10-18 |
| P0044 | 88 | 0e6dcff8d6c53a2f1eaa0072a51a1a84a1b7021f0598d3447b26dc67e3dd7c75 | L4518.B0.s.p2.d20-21 |
| P0045 | 17 | 98d880fca93d1a01a55bd8ea59e7280ba91bc9b20adb2bec8064f6555a21f073 | L4518.B0.s.p3.d23-23 |
| P0046 | 72 | 7254ad932a3bf242f4396638d598fe3edbe1da2d6e07ad0721fcebfffb345662 | L4518.B0.s.p4.d25-25 |
| P0047 | 38 | 0807986daf53e4fe03efed0652e51ba38829c8421f74c0603d819cce159ec53b | L4518.B0.s.p5.d27-27 |
| P0048 | 1032 | a77d0e2a4d1a9cc8d7c8bb2ab1260cd037b64e7d9d2953ce7bea6f27df4e3d78 | L4518.B0.s.p6.d29-35 |
| P0049 | 42 | 30f7579906b7644523828b133b257e602daad6f004521a00033766c1678477ba | L4518.B0.s.p7.d37-37 |
| P0050 | 526 | 8473d16026c7e0af043b3ae41677955cae4c7c96e4b6379da2f23e7c2ac8dcc4 | L4518.B0.s.p8.d39-45 |
| P0051 | 57 | 74fe29cc8ca3e22a453fb551910295e082ab5f9e395d37d5cc3b8aa0bc2b4625 | L4524.B0.s.p0.d1-1; L4878.B0.s.p0.d1-1; L5168.B0.s.p0.d1-1; L6393.B0.s.p0.d1-1; L6780.B0.s.p0.d1-1; L6846.B0.s.p0.d1-1; L7285.B0.s.p0.d1-1 |
| P0052 | 224 | 74369273c699c27f6ec4a8bf1225f804d7499b89c489cc8c4c6beda7273b1758 | L4524.B0.s.p1.d3-5 |
| P0053 | 405 | ff0aef863d0acd1197ce27bb383edde34701c641b4f801c244ffde10855423a7 | L4524.B0.s.p2.d7-9 |
| P0054 | 267 | 2dbd2d40c0f895e4a233c66a52b50566696f068b2ed70ffdc8ec7f8f624bd9b2 | L4526.B0.s.p0.d1-1 |
| P0055 | 184 | 6782f538d1f69b6407f426c2728db232862fd4bbd286a5fae6c5753297ea6f63 | L4526.B0.s.p1.d3-3 |
| P0056 | 39 | ade8352128d6a0d7cdcaf6bc8bfcef00b890f9cb5aba113839f4ae1a245f83a7 | L4530.B0.s.p0.d1-1; L4630.B0.s.p0.d1-1; L4716.B0.s.p0.d1-1; L4749.B0.s.p0.d1-1; L4780.B0.s.p0.d1-1; L4808.B0.s.p0.d1-1; L4840.B0.s.p0.d1-1; L4909.B0.s.p0.d1-1; L4949.B0.s.p0.d1-1; L4999.B0.s.p0.d1-1; L5033.B0.s.p0.d1-1; L5135.B0.s.p0.d1-1; L5234.B0.s.p0.d1-1; L5315.B0.s.p0.d1-1; L5414.B0.s.p0.d1-1; L5543.B0.s.p0.d1-1; L5574.B0.s.p0.d1-1; L5640.B0.s.p0.d1-1; L5777.B0.s.p0.d1-1; L5860.B0.s.p0.d1-1; L5909.B0.s.p0.d1-1; L5943.B0.s.p0.d1-1; L6053.B0.s.p0.d1-1; L6099.B0.s.p0.d1-1; L6180.B0.s.p0.d1-1; L6234.B0.s.p0.d1-1; L6316.B0.s.p0.d1-1; L6360.B0.s.p0.d1-1; L6419.B0.s.p0.d1-1; L6475.B0.s.p0.d1-1; L6510.B0.s.p0.d1-1; L6567.B0.s.p0.d1-1; L6620.B0.s.p0.d1-1; L6648.B0.s.p0.d1-1; L6714.B0.s.p0.d1-1; L6746.B0.s.p0.d1-1; L6809.B0.s.p0.d1-1; L6870.B0.s.p0.d1-1; L7152.B0.s.p0.d1-1; L7230.B0.s.p0.d1-1; L7293.B0.s.p0.d1-1; L7336.B0.s.p0.d1-1; L7433.B0.s.p0.d1-1; L7518.B0.s.p0.d1-1; L7585.B0.s.p0.d1-1; L7638.B0.s.p0.d1-1; L7684.B0.s.p0.d1-1; L7749.B0.s.p0.d1-1; L7838.B0.s.p0.d1-1; L7901.B0.s.p0.d1-1; L7997.B0.s.p0.d1-1; L8054.B0.s.p0.d1-1; L8160.B0.s.p0.d1-1; L8222.B0.s.p0.d1-1; L8309.B0.s.p0.d1-1; L8406.B0.s.p0.d1-1; L8474.B0.s.p0.d1-1 |
| P0057 | 326 | 55cc2c916f6a6dc8ec0298bd335318c8965bac113739dfe780205b137fd2f056 | L4549.B0.s.p0.d1-24 |
| P0058 | 10 | 74541df5b9e36f1ba9f6f2e255c2500608116faa398f4a17e8da08d0c5145d28 | L4558.B0.s.p0.d1-1; L5813.B0.s.p0.d1-1 |
| P0059 | 692 | 1625fde3dd76f2f77db1e3afa32503b35c0ee091da1c7c1bc63a76166e1527f5 | L4562.B0.s.p0.d1-12 |
| P0060 | 2724 | 1c873794c827ed9039fb00d1d6a5cf5bf98d9cdd6dd9a980e185fb945c2276bc | L4564.B0.s.p0.d1-111 |
| P0061 | 1573 | eb6584d1b455b3baa97ce92cd0121d2f14e88eb2fe0ae8cc1006a44b0a946dd4 | L4567.B0.s.p0.d1-26 |
| P0062 | 340 | 844f930615096c2e4e6e3eee33a408398c3202c42e929700643ddbaafa1c8f61 | L4569.B0.s.p0.d1-9 |
| P0063 | 236 | afcc636f08b81ad77f3330cf01361f7c213816fd77dfc311d342c51b86419c5c | L4571.B0.s.p0.d1-2 |
| P0064 | 2032 | fa471b7a8f219283d86b035ee6f77509b83cc2590d021c66491c30805756e58f | L4571.B0.s.p1.d4-102 |
| P0065 | 1487 | b0ed3c14c12a9c90525ab2835de0af796dead6f788de5bea3249381150177e9e | L4574.B0.s.p0.d1-59 |
| P0066 | 3776 | e7925e8eff529a22569504d57f16f7b106b495c971fd6a3e996253450b413d6b | L4576.B0.s.p0.d1-124 |
| P0067 | 430 | d005762039b7c5d435d8c72f486dd0c8e2deb1976c79035972300538fcbcff12 | L4579.B0.s.p0.d1-14 |
| P0068 | 1251 | bac8d0684c57b9fb9f4dcf3ffefdee59f900c353a33bcd8d3dface4bc61eff90 | L4579.B0.s.p1.d16-58 |
| P0069 | 669 | dc9320f2ed24f139caaa961d2882da384feba5507422e406e86ca2925c220b87 | L4579.B0.s.p2.d60-82 |
| P0070 | 376 | ed1b9f41067f7159d19166d0321cc3b4325ecf58eec231f37ca9eeee5b76f00b | L4579.B0.s.p3.d84-99 |
| P0071 | 488 | c730e0680cdebe48581b313e33f69d6a4d45658dc3159c457cabbcb12a3b913f | L4579.B0.s.p4.d101-119 |
| P0072 | 397 | fefd5c2a889ac11753013bd8853aa11d6448190b761d2bd52bf4353216ade5ce | L4579.B0.s.p5.d121-138 |
| P0073 | 3004 | 99cdc5e4db4de3e79ce28fc06af5a7b806769849fa85f7626a887b106b412de2 | L4584.B0.s.p0.d1-169 |
| P0074 | 5200 | 940763824023148cf5a9a7dc4fa3569faf23e59728827ed0ceca041462e129ed | L4586.B0.s.p0.d1-187 |
| P0075 | 9 | e04050a2c238d71e25ec64f4e93f2458e31b095a1963dfbc451bb36dc3501a51 | L4591.B0.s.p0.d1-1 |
| P0076 | 144 | 9aeb13a3e40944deea974a3bfe32fda641556d9cc178b801c8125a479b3effef | L4594.B0.s.p0.d1-3 |
| P0077 | 482 | 6897ec4c95c487d892478635efb75f2830655012eb84e880e0c9dbd7a3705f38 | L4594.B0.s.p1.d5-17 |
| P0078 | 1 | 36a9e7f1c95b82ffb99743e0c5c4ce95d83c9a430aac59f84ef3cbfab6145068 | L4594.B0.s.p2.d19-19; L4594.B0.s.p3.d21-21 |
| P0079 | 5 | 7879981d4f226a8f0191d36730c07205d7a5ff1c780fca9b2f905f25264cf636 | L4594.B0.s.p4.d23-23; L4594.B0.s.p5.d25-25; L4594.B0.s.p7.d29-29; L4594.B0.s.p8.d31-31; L4594.B0.s.p9.d33-33; L4594.B0.s.p10.d35-35; L4594.B0.s.p11.d37-37; L4594.B0.s.p12.d39-39 |
| P0080 | 18 | 0c9a73ca6f1f29901215b4c9e9b8c3d69119ba1c155455fea69153c3746b4815 | L4594.B0.s.p6.d27-27 |
| P0081 | 6 | 399141be1d30ac2656d89eedcf0d8dcedaa72d6c29bf959cae243dc7b1442cf6 | L4594.B0.s.p14.d43-43; L5126.B0.s.p2.d10-10 |
| P0082 | 9 | 670af646a34cdc2a616874dc29d30abc2b5523cc091c081c25a2e51079484b9c | L4594.B0.s.p15.d45-45 |
| P0083 | 24 | a46afd42a4d900d030f263fc14e1ab67c3c8b031d49101a10afa537afb2a4959 | L4594.B0.s.p16.d47-47 |
| P0084 | 10 | e91772ccb5e6ce5f932d6417eacd9a1e031b957101cdb68be76d417defa7fd28 | L4594.B0.s.p17.d49-49; L4594.B0.s.p19.d53-53 |
| P0085 | 8 | 8b6fa01313ce51afc09e610f819250da501778ad363cba4f9e312a6ec823d42a | L4594.B0.s.p18.d51-51 |
| P0086 | 13 | a453c4f4c4b8f0c24bb773c31918b1f9da54669808ab34813e67061ceb342af0 | L4594.B0.s.p20.d55-55 |
| P0087 | 226 | b84721eaec3a15c14d5a2f3a7c59850b66801a2346ff8d6a4a432bb6cc35e93d | L4594.B0.s.p21.d57-59 |
| P0088 | 68 | dc97e7aae2a02af01325f95dd83bad1b771df4508d1645467971fc232bb99f68 | L4599.B0.s.p0.d1-1; L4609.B0.s.p0.d1-1; L4613.B0.s.p0.d1-1; L4626.B0.s.p0.d1-1; L4688.B0.s.p0.d1-1; L4704.B0.s.p0.d1-1; L5074.B0.s.p0.d1-1; L5076.B0.s.p0.d1-1; L5084.B0.s.p0.d1-1; L5088.B0.s.p0.d1-1; L5107.B0.s.p0.d1-1; L5114.B0.s.p0.d1-1; L5445.B0.s.p0.d1-1; L5447.B0.s.p0.d1-1; L5455.B0.s.p0.d1-1; L5808.B0.s.p0.d1-1; L5810.B0.s.p0.d1-1; L5833.B0.s.p0.d1-1; L5974.B0.s.p0.d1-1; L5979.B0.s.p0.d1-1; L5990.B0.s.p0.d1-1; L6003.B0.s.p0.d1-1; L6018.B0.s.p0.d1-1; L6269.B0.s.p0.d1-1; L6274.B0.s.p0.d1-1; L6284.B0.s.p0.d1-1; L6304.B0.s.p0.d1-1; L6614.B0.s.p0.d1-1; L6617.B0.s.p0.d1-1 |
| P0089 | 76 | 08cbc62cfdb323c455cf0644c751919dc3426f47d6582ccc620ee81dd2822603 | L4604.B0.s.p0.d1-4 |
| P0090 | 31 | cac1685585590361647e7a27b58a25a590f335bfcd0ab70fcdc0424e40c20843 | L4606.B0.s.p0.d1-1; L4893.B0.s.p0.d1-1; L5375.B0.s.p0.d1-1; L5677.B0.s.p0.d1-1; L5933.B0.s.p0.d1-1; L5992.B0.s.p0.d1-1; L6022.B0.s.p0.d1-1; L6030.B0.s.p0.d1-1; L6095.B0.s.p0.d1-1; L7329.B0.s.p0.d1-1; L8031.B0.s.p0.d1-1; L8279.B0.s.p0.d1-1; L8336.B0.s.p0.d1-1 |
| P0091 | 318 | 415fa7c1efbd57374b04ff493d1dc1221b101899ea5ec1d8eec45f1dd6ed761e | L4611.B0.s.p0.d1-10 |
| P0092 | 1440 | 46c1e31cc412978496442f7f926d3d5d7af3ec757e871d8845bdde1507e7ace4 | L4616.B0.s.p0.d1-24 |
| P0093 | 393 | bf227116be61a044a612ce2a12405a024c32ea7baf3ed1d467ca5bd9b06157d1 | L4618.B0.s.p0.d1-11 |
| P0094 | 560 | b73c83cf0ed5836182b559f87d06b6f97cc1a219b40856925c8eceee880cee61 | L4618.B0.s.p1.d13-31 |
| P0095 | 590 | 546103c346c5f3e2fc1c306f6b853ef1c10957610ef4cb0563fb3f339bc9673d | L4618.B0.s.p2.d33-47 |
| P0096 | 7751 | cc3c6b7a3664e0fe0c188691dd8f953b4c48fdfe1ef957a19fbc3d7c8f8bcb12 | L4621.B0.s.p0.d1-423 |
| P0097 | 103 | b07eb31df34d71366edfba1af3e4ead2b3dce5f416cdc7ddbb971164217fdd07 | L4624.B0.s.p0.d1-3 |
| P0098 | 20 | f9d71f128ed62caba0926203a7ff6763ef738fb36d17411baf0201bed2f017a6 | L4656.B0.s.p0.d1-1 |
| P0099 | 747 | 19f52c7b541b5aa91a848deef1001b7636d20ead3cdf2d15e49fafc0e5257529 | L4660.B0.s.p0.d1-13 |
| P0100 | 4102 | 0b73df74b9e76114f03e7c88eea4b6c58e1c52d7d0e9dc4e6a0a993f8d4a4bb0 | L4663.B0.s.p0.d1-197 |
| P0101 | 755 | 45d78cb12339cfe3f3ed070bfa53748588c4f53f141bc22a324825644627d348 | L4666.B0.s.p0.d1-12 |
| P0102 | 147 | 935c5e145428e4ae04c186b0fc0dd91a23854bf2f328585aa113376661855994 | L4668.B0.s.p0.d1-6 |
| P0103 | 74 | dc24b68c34c39e382ff62fa642df5aa501572bcde52a72b05a3d3c9b5924b88a | L4670.B0.s.p0.d1-1 |
| P0104 | 76 | 8a253fb5e3e63f1ec468488037308c75ccdfd4ce56b5e8d61f9f660c284b26a1 | L4670.B0.s.p1.d3-3 |
| P0105 | 79 | d310939b80637b936f5d1483c815de4438b881ffce5db0c2bb13946a77284308 | L4670.B0.s.p2.d5-5 |
| P0106 | 79 | d934fb15dcb9d4de5e93cacd31451356723e82fb117a6a4a710775162f202975 | L4670.B0.s.p3.d7-7 |
| P0107 | 79 | 6cdfd64ab5462734ef3de20e2b6d0988b26799bd9d853e4aa4fabcd3bd068ee1 | L4670.B0.s.p4.d9-9 |
| P0108 | 79 | 757262ce40887d04e3c659dc6a794811aad56aa025f14166ef3d569d081f6593 | L4670.B0.s.p5.d11-11 |
| P0109 | 2712 | 81045dbb9a087935fed62c1c5b8df6e8856808fedd0c521da8481280cd25070f | L4672.B0.s.p0.d1-111 |
| P0110 | 3452 | c7cd11d4413c7e3cbc825c8962903a7e50f3c828c42580e8b56c394412eac773 | L4675.B0.s.p0.d1-108 |
| P0111 | 15 | 27d7df0fc06a97bfc109daa31f52e75985cd92196c0fc9b1ad614d557ed8e187 | L4680.B0.s.p0.d1-1 |
| P0112 | 14 | b54b1e0a5c91b6a6504e6fa3a1591101b516aec52c23a574d13e20e42df98dbf | L4683.B0.s.p0.d1-1 |
| P0113 | 2500 | b313cabcff9b19fd72a50d44e279ff8212f1243b318d097e4b129d49e5599f0a | L4691.B0.s.p0.d1-132 |
| P0114 | 3005 | 163e7ec4bea1c21b1674f6dd656ba8af7df4dc32a7b0c9209eb9ec85d6b198de | L4693.B0.s.p0.d1-97 |
| P0115 | 142 | ca9c03101ce6a3817af9433925de95c7ffacfdb16eccbea66e64afaae942095a | L4696.B0.s.p0.d1-3 |
| P0116 | 3518 | 8590b124725b66aa08f39568dfffb915e37ffacde3855cdca67d5d741f7507fd | L4698.B0.s.p0.d1-130 |
| P0117 | 6136 | c7e59ea631ae7e2c7e72a97a406d4b234f55c993779f4151e87b73b7e790b60b | L4701.B0.s.p0.d1-171 |
| P0118 | 331 | 1f9cdcc6b4124ab22ebce9d041d5cbe39a6ac6226fa705ee94b4690fece6e21b | L4706.B0.s.p0.d1-2 |
| P0119 | 2218 | ebcd09c262b6b849292406235eb288437468f94c9bdd21f51d9879854f4dec5e | L4709.B0.s.p0.d1-109 |
| P0120 | 2000 | b6a5d736109609c95d2e55b85bdfed5ed5b2a9c7479b161a5888aebd20636c55 | L4712.B0.s.p0.d1-45 |
| P0121 | 32 | 28a2994f538020cc05b78be5f7fc06f20c2ce5632e8e556ab26140ffdd763b55 | L4737.B0.s.p0.d1-2 |
| P0122 | 1112 | 30eff13b4bba6a91de31555f1b3c5153d23d206a4897ab663402c59047a314b9 | L4740.B0.s.p0.d1-41 |
| P0123 | 9509 | c24494c1e22372ec13195ea66b54a63fcf87211bf2779c46baa861b73d3c8757 | L4742.B0.s.p0.d1-267 |
| P0124 | 190 | b3627dd42a3ba27c604a34ae516fde347f6f46df22929af43540c75a92443293 | L4745.B0.s.p0.d1-6 |
| P0125 | 47 | 1c5b99e7c67ae0ba6fb11325d003c8ab4ef424a176aefcf5428fb979df458063 | L4767.B0.s.p0.d1-1 |
| P0126 | 673 | d90dc7210c7f55f7987a24b561cfcbe38bc276bdc5e363a788da72efa51ddfd9 | L4771.B0.s.p0.d1-2 |
| P0127 | 27 | b47d7306285d3a3e412c6a66bc5eec4a04677af369fd7ed17efca57d1d16c02b | L4774.B0.s.p0.d1-2 |
| P0128 | 45560 | 776b901f64c21ad6439fdc36dd353e7e700a7d560319da0744bee2983ac7ecf0 | L4776.B0.s.p0.d1-477 |
| P0129 | 30395 | 7588c1ef94483e13b716439df3fc3f7ec2af97b4096502de5affaec8aa6eb3df | L4804.B0.s.p0.d1-1 |
| P0130 | 36 | 5c0c700a6c1500f366cb023b1ac2ee3b276aabb0519fcaec12c7523c2dcd412b | L4826.B0.s.p0.d1-1 |
| P0131 | 35 | 4584e72053fe845fa83f7c1e69e70f34f175711f35d7e8477286fa11b9262cfb | L4826.B0.s.p1.d3-3 |
| P0132 | 99 | 778f296312c2a61e88c1f068c9b13e8d41dff5a6ba9c6cac720967df3a920db6 | L4826.B0.s.p2.d5-5 |
| P0133 | 33 | afa84b32017588da8a52a7e81982fe11352399125785360fb9caf571ef1ff9b4 | L4826.B0.s.p3.d7-7 |
| P0134 | 249 | a93c0eb4696cb27c62ff46dc6f7ad870acf19a365b55a0ec43fd0b9507bc661f | L4826.B0.s.p4.d9-13 |
| P0135 | 112 | 5849c313e710af3c9be358baf261c5d3837e603d5868f2567fe6672d38ba10f0 | L4826.B0.s.p5.d15-17 |
| P0136 | 25 | e7bde71da779122683cd4d9801049ade3879d93b30fd374cc0c97c24e1cd977f | L4826.B0.s.p6.d19-19 |
| P0137 | 58 | f377e76ada6d1631ad110045a1b0afd539336cccd5cbf31f4773cbebd43a0197 | L4826.B0.s.p7.d21-21 |
| P0138 | 198 | e8324190026ec369b0b93137788b539a5f45316b6e3c76925d61f2039ddce0ed | L4826.B0.s.p8.d23-23 |
| P0139 | 32 | 878bd063e24f5d1be995a9221b90030b90538ea445afd9115ac7bcd8add24610 | L4826.B0.s.p9.d25-25 |
| P0140 | 177 | e8ef9849830138d8b967ce2a2b4a016ad5d44670e146a17a925329b810362c90 | L4826.B0.s.p10.d27-27 |
| P0141 | 19 | 2d8a6fdff21cc3838ffe7ef87c4b8de99cbe6487e72e42dc37de0ae2b099b43d | L4826.B0.s.p11.d29-29 |
| P0142 | 87 | 53b43808aeea3fabc40120ef5970ebcc6717b3e95b8098c1638bdc457638add9 | L4826.B0.s.p12.d31-31 |
| P0143 | 89 | 15a89b9ad5866e57973aaeab81e5ab68bce437c762d0d0ca2197a7a19a55be36 | L4826.B0.s.p13.d33-33 |
| P0144 | 47 | e08e2d9cd599a1fa9b346c7b7a18bea7050a33de953fb4da78f962591a36f60e | L4830.B0.s.p0.d1-1 |
| P0145 | 295 | 76b1dd43da1b58b57fa10494369913bb5371becde60ac81590805f73d54dff1c | L4830.B0.s.p1.d3-12 |
| P0146 | 51 | 6533a7726ecbd6a6a525f1d65a5e3888012da8d63d1b7e0b51c8811324874c9e | L4830.B0.s.p2.d14-15 |
| P0147 | 17 | fa48207774f3b795e304eb8822e1c80eb3c2ddda24b5cb31c6ff7c7d85e979a9 | L4830.B0.s.p3.d17-17 |
| P0148 | 230 | 42979d474855ac353725f27a0c72768819375b12ccae80284f598fe2c08eb180 | L4830.B0.s.p4.d19-21 |
| P0149 | 171 | 7926ac54e3f19318324f939698a4dc863a87579d05c4d7e9bcf35be96bb87589 | L4830.B0.s.p5.d23-25 |
| P0150 | 88 | a5180171f61098da529f448d975f20812e1aa381bd0d74238b09aa88d4a45f50 | L4830.B0.s.p6.d27-28 |
| P0151 | 122 | f139c252a3ff2f3a1012b009b108a9c6250420b86bcd6957afe6cb63b36ba39f | L4830.B0.s.p7.d30-32 |
| P0152 | 189 | 4ed250a334ce26c493eeb05c89130a117f2939a313020c94eff3c415fc009d61 | L4830.B0.s.p8.d34-36 |
| P0153 | 93 | 86ca4255911c55fe70393c76edfd9783ae758d5966e69543590cd72515beafd2 | L4830.B0.s.p9.d38-41 |
| P0154 | 49 | 375dc2ed5f82537e4514dcb5a3a5a2b2843d1986f72176df37a5425a4b7138f3 | L4834.B0.s.p0.d1-1 |
| P0155 | 36 | 24086cd0ed72a2b3646ca5abbb17e2e45e1c38c11f6d0807373aac6ec18203c5 | L4834.B0.s.p1.d3-3 |
| P0156 | 184 | 8a426ee2893acd8686ceceaa7d33aea5db0cf25f8b1cac734b678ee6d1ddef10 | L4834.B0.s.p2.d5-5 |
| P0157 | 26 | 8d32c0a3774f3a5772dc2a1e53073ab609465d3e31efdffeb9a20607a0baa63b | L4834.B0.s.p3.d7-7 |
| P0158 | 435 | a56a48b5a3ed0cbfe484a5459dc72db4e5930a48503475cb1cab90d0cfcabda5 | L4834.B0.s.p4.d9-13 |
| P0159 | 193 | be8d7df07b0ab798fa8f23de0193a8f69d92569ce646a724a82d27a5159c742c | L4834.B0.s.p5.d15-15 |
| P0160 | 37 | c599ea9c52f176e5340a3ae93b4646fb1d3794b9666306de402b18fe3d3a85a1 | L4834.B0.s.p6.d17-17 |
| P0161 | 227 | 38b69019ea412b5dcb93a4dafff163da2b44918ab80dd23c8e626c0fab6250e4 | L4834.B0.s.p7.d19-19 |
| P0162 | 169 | 887da7737940d432f242734161a61bd2b1387b25771d9dae3ff466784552c135 | L4834.B0.s.p8.d21-21 |
| P0163 | 33 | 11e84d0d6b9577bc82105764ebb20aea5b1e035e557adb08474e8927deec0aca | L4834.B0.s.p9.d23-23 |
| P0164 | 362 | 72fcb1f4c0a6176896c6382fbd4465c35d64c84d45b76d8a21b14477a29c2dd7 | L4834.B0.s.p10.d25-28 |
| P0165 | 102 | e318afe71901da0ef578e1e3ab93f1507f68dd99ebe38edd522f82fed8592b26 | L4834.B0.s.p11.d30-30 |
| P0166 | 29 | 8ac79955795784ca48016ad46dca860ea42f34c6a8ebe91234df048fec4462dd | L4834.B0.s.p12.d32-32 |
| P0167 | 240 | 4702bb3035c5be020efe55ce483236e950f419a9176d5e0adf1c8c298879cb74 | L4834.B0.s.p13.d34-35 |
| P0168 | 305 | c6282356f03eeed78b16c9510ff323ee454f38a2785869aee94e2fcf1d24c13d | L4834.B0.s.p14.d37-38 |
| P0169 | 254 | e37a3c5bdbe2cf90471d78efd52662c6e6eeddeda6b3d9b325dde216641e4541 | L4834.B0.s.p15.d40-41 |
| P0170 | 186 | 2c03b380c32d3c1d3504fd072081df157acae7efd4ea311a9cfc7e364459c069 | L4834.B0.s.p16.d43-44 |
| P0171 | 319 | 5539b7c62e12fd3b82e1b34dadef5cac35272f9c3912b03e2231d7018051f9f1 | L4834.B0.s.p17.d46-47 |
| P0172 | 25 | 8e8784ffe400f70a47c47e14ea6cd2950266d4954d056536ddba575a0dbb1d62 | L4837.B0.s.p0.d1-1 |
| P0173 | 130 | 67d16d3be9b15399ef32db720bc4fe96146d122b95a5548c6b9902fd65ba8e3c | L4837.B0.s.p1.d3-4 |
| P0174 | 115 | 95201418cea491403011a0a2d5b13579ae74603cf63a44e0f121841c9a1a4f8f | L4837.B0.s.p2.d6-7 |
| P0175 | 136 | 88729fee5025fadbc2789ee94d85ebabc3a30560371eed83dc0a708aaef8a4d8 | L4837.B0.s.p3.d9-10 |
| P0176 | 127 | 011210b2c3f2aa1d52c2fb844d19762591f2a18740d0b6e386ded064f72091ca | L4837.B0.s.p4.d12-13 |
| P0177 | 38 | 2ba9e474972e739850480619db54bda397b0ef24def8fe2596a7be4753ac1b81 | L4861.B0.s.p0.d1-1 |
| P0178 | 45 | 37cdf0cf72a9f48211adacc63d647cc81000d3d029032d5af2e66238858d80e3 | L4861.B0.s.p1.d3-3 |
| P0179 | 216 | bd365880c79d50794afcdcb513a4096ce1675e35d47d8b315b658c7841804d42 | L4861.B0.s.p2.d5-5 |
| P0180 | 30 | 5927ec6ce16a105d908c4f672a916bf40a09912eb4dc79b1b961523f5337deb8 | L4861.B0.s.p3.d7-7 |
| P0181 | 234 | de1d91ad4b31a6b5122e98746347049464e021776c12522f2cfd2f0bace95d5f | L4861.B0.s.p4.d9-9 |
| P0182 | 27 | 773b08c6b89299fe1544da05b564b6ec5c50b04f0c8d53f56ae6bb5a7df41dc9 | L4861.B0.s.p5.d11-11 |
| P0183 | 137 | de0b244213eca620396e97823f6b788b90578cbacfa9a6a9b98763659f38e9e4 | L4861.B0.s.p6.d13-13 |
| P0184 | 39 | e7bb456413b5ee467f75e541a93c522c1dde11cee5f88f77eaaaa50fd9f20960 | L4861.B0.s.p7.d15-15 |
| P0185 | 250 | 4447b36fcc6e80cfbc7540c1a66a96f3687e9e6c6d62c18c1cbdbb80918baa16 | L4861.B0.s.p8.d17-17 |
| P0186 | 29 | 303952a29bb1aca5ca0a03310d4aa2a5db740348dcfe36a4da285f79cae3f049 | L4861.B0.s.p9.d19-19 |
| P0187 | 206 | 5a56d1e859f92cc4e13d31ca856e6a6c50752a3e040769ba94a25506718c65dc | L4861.B0.s.p10.d21-21 |
| P0188 | 23 | ea9661162c774c9fc737fb952b5c7506c8249bffc30aaa8c69d93520ce9031e3 | L4867.B0.s.p0.d1-1 |
| P0189 | 56 | a05664441312381aed17ef69f7c864dffbdd462dfdbf9f3c9350518ec1d0bec9 | L4867.B0.s.p1.d3-4; L6674.B0.s.p1.d3-4 |
| P0190 | 197 | c1ccbc49e2426b8c33e2a89ba49cd4b54263b53c98c8ee6f35b78725ef90e63b | L4867.B0.s.p2.d6-6; L6835.B0.s.p2.d5-5; L8301.B0.s.p1.d3-3 |
| P0191 | 111 | 9c1d543c4df187625cbd6e0aa3eceec3e25cfb04ec8b454b359c051c8efb7a34 | L4867.B0.s.p6.d20-20; L6835.B0.s.p6.d19-19; L8301.B0.s.p5.d17-17 |
| P0192 | 184 | 216c676c09c6481a8ea95e6e3f93edfbd65dfa49a16e394968f73f56df23d2b7 | L4867.B0.s.p7.d22-27; L6835.B0.s.p7.d21-26; L8301.B0.s.p6.d19-24 |
| P0193 | 18 | 9be53d9af15af86fb12e8dc67525acef27094fed85bea25f19b4042a2b040d66 | L4867.B0.s.p8.d29-29; L6835.B0.s.p8.d28-28; L8301.B0.s.p7.d26-26 |
| P0194 | 252 | 38aae9d10d9666a522231294c7aadc4721e90a23cf1c00d6dc25f5a63a089cd5 | L4867.B0.s.p9.d31-31; L6835.B0.s.p9.d30-30; L8301.B0.s.p8.d28-28 |
| P0195 | 310 | 4bb16f8dc6f5b95e7145389c57c00bfd4337b64b88206c46c1c411bb8d74bc13 | L4867.B0.s.p10.d33-33; L6835.B0.s.p10.d32-32; L8301.B0.s.p9.d30-30 |
| P0196 | 120 | ea02cda9ea195622728731a8cb6967419b3b03a9b562b0908103a4f30d383db1 | L4867.B0.s.p11.d35-35 |
| P0197 | 50 | 7259f45ab0f14389ff00f7229bd18b3ab80269e45ef11828228da317853b2d4d | L4867.B0.s.p13.d39-40 |
| P0198 | 6 | dafe7694460f4e37b708f5134f2f7f759cb997f9cb612d6ca566dd6e6a34353f | L4867.B0.s.p14.d42-42; L6835.B0.s.p14.d41-41; L8301.B0.s.p13.d39-39 |
| P0199 | 1771 | d92611f8b7e0ceeb56c4c3e9ce931282b9dabaaf882f59b3f89697f83b2cbddc | L4867.B0.s.p17.d48-52; L8301.B0.s.p16.d45-49 |
| P0200 | 56 | 267f97b2488d46ed26ae1a2cf11f3bd31e3ba9613fc00e435c8583b8094be91c | L4867.B0.s.p19.d56-57; L6674.B0.s.p6.d15-16 |
| P0201 | 46 | 4845a5e5c83f93eb5278201abe223f48c4867f8cb024e17f24380f3cc40302cc | L4867.B0.s.p20.d59-59; L6835.B0.s.p20.d57-57; L8301.B0.s.p19.d55-55 |
| P0202 | 194 | 75c68569a16b46fc8d135f33a7bf6e8c76c9cc22626721da6f4560889c31b433 | L4867.B0.s.p21.d61-61; L6835.B0.s.p21.d59-59; L8301.B0.s.p20.d57-57 |
| P0203 | 50 | 69196dcd4eca774d0b77a813a1ad69166e973ebdaa045cdb29168ba1191c22e1 | L4867.B0.s.p23.d65-66; L6674.B0.s.p8.d20-21 |
| P0204 | 456 | 937a49e4e49c0e9b7467b4eec845e42c092bad6740a9773224c8c43fc7a89acc | L4867.B0.s.p24.d68-69; L6835.B0.s.p24.d65-66; L8301.B0.s.p23.d63-64 |
| P0205 | 56 | aa201b52859ad417e745d127b04eb0c39e7b8493d63e70cab6eca2a084cedc0b | L4867.B0.s.p26.d73-74; L6674.B0.s.p10.d25-26 |
| P0206 | 150 | e8a70cc3176c9617c4987bcc4f41acb94d8efd45998a2cf5cffb4e54313f4330 | L4867.B0.s.p27.d76-76; L6795.B0.s.p0.d1-1; L6835.B0.s.p27.d72-72; L8301.B0.s.p26.d70-70 |
| P0207 | 41 | 6d1d57058220a79a9551be6920dd4590caceb395fe1b3b37e1d8e178719f2864 | L4867.B0.s.p29.d80-80; L6795.B0.s.p2.d5-5; L6835.B0.s.p29.d76-76; L8301.B0.s.p28.d74-74 |
| P0208 | 102 | ef5b02b8f92eb5d3ad3cc78ed5e72276ae3a7c61e9088b3c59c5a2ec4511db5b | L4867.B0.s.p30.d82-82; L6795.B0.s.p3.d7-7; L6835.B0.s.p30.d78-78; L8301.B0.s.p29.d76-76 |
| P0209 | 270 | 99ad2e9913c62ab9f622a3e3b9ef7237abab7aac2e4be8aa9b5a0560f3160657 | L4867.B0.s.p31.d84-84; L6795.B0.s.p4.d9-9; L6835.B0.s.p31.d80-80; L8301.B0.s.p30.d78-78 |
| P0210 | 50 | 75a49ff07c6d3c508c63ebaaa561e771063275d197d4db836c29aa0d86c47c18 | L4867.B0.s.p33.d88-89; L6674.B0.s.p12.d30-31 |
| P0211 | 566 | 64d638f7d67b0df98f96d55c926d4ac247d952687409c0d8f417fca2253d4181 | L4867.B0.s.p34.d91-91; L6694.B0.s.p0.d1-1; L6794.B0.s.p1.d3-3; L6835.B0.s.p34.d86-86; L8301.B0.s.p33.d84-84 |
| P0212 | 302 | 69791edf898831e02d009808b7a40dbda0043a83c99a833f5709f005c1faf2f6 | L4878.B0.s.p1.d3-5 |
| P0213 | 505 | ed44144777ed3900e4084faf8594a2fd137b95e68998e5be2ff41e345a6856c1 | L4878.B0.s.p2.d7-9 |
| P0214 | 295 | d42f10e3c1104491e0d91e0591785a6e75c931070d55e9d3e5e48b6c53f1f4df | L4883.B0.s.p0.d1-1 |
| P0215 | 86 | 8c75cc586253eb203113a7efe362be04adbbd6f52107f85981034d2a68b67e0b | L4883.B0.s.p1.d3-3 |
| P0216 | 166 | c3dfaecda9ba6278e3bdf4a1542bfef141e6f2e89596d3739b92c9970b6e3374 | L4883.B0.s.p2.d5-7 |
| P0217 | 84 | 432b6eed08a99f240444a373df0606c1dd1ae95a594c65dff670312b76f3dbad | L4883.B0.s.p3.d9-9 |
| P0218 | 179 | f05d987cc70cfdc32b66b8fe10a0ece14d86cfa78e1ab9946dce7df5465f9c7a | L4884.B0.s.p0.d1-1 |
| P0219 | 216 | 98f73e59112856dbe3e88121ba1b85248c208fd14c3b04071734a90c9a727544 | L4884.B0.s.p1.d3-7 |
| P0220 | 185 | a266ce4c8180dca5e5169b855113459232b23cbcea91388a2c133635369b8f50 | L4884.B0.s.p2.d9-13 |
| P0221 | 298 | 7b3c43a3abfa9529dbde0bba57a367a1ea3df016e1265ad3c7470ac278198ffd | L4884.B0.s.p3.d15-18 |
| P0222 | 97 | 814e42d630ad58266c602f58397240ef869e52d7d8206e3dcaaad4c08cd67349 | L4884.B0.s.p4.d20-20 |
| P0223 | 214 | 3781d896025f237c1ab401591cf588bfa2a94f0d46460b211f3a66324cd2725f | L4888.B0.s.p0.d1-3 |
| P0224 | 66 | dd58a0c3c7c675c7279588288c41dbd3522703198921848ccdc3748a446c290b | L4892.B0.s.p0.d1-3 |
| P0225 | 326 | 92fc9e6132bdfc002679c495d5fc7c60912b534b9a2e1f41361fb12aa58e88c3 | L4896.B0.s.p0.d1-6 |
| P0226 | 5754 | 1650d5e7b071efb884965ac82ca3e7fd1e58c29946d4400fe7b31f6794692f86 | L4899.B0.s.p0.d1-81 |
| P0227 | 872 | d9e1313d34bf07431eee785a065493a788cc7c54ba6df7c055343fe6686f4808 | L4903.B0.s.p0.d1-19; L4905.B0.s.p0.d1-19 |
| P0228 | 1160 | 30c46419ef9872e176fe4c0212069735d40e5e55bd3002fb2dd29c7454cbd956 | L4903.B0.s.p1.d21-42 |
| P0229 | 244 | 9d84361d07487883bed6b079ef9883744a6fbf972953f6164ae07b69368a5cc8 | L4903.B0.s.p2.d44-50 |
| P0230 | 124 | 5a8a67a3d181e22b10b140a5f963dfbc94f87af0961ad92abb37994363e99147 | L4903.B0.s.p3.d52-57 |
| P0231 | 47 | 02fe7daaf7bfb40fa7dfefc40d11ac564f7016158cfe0744093dbdca0e2d6a5a | L4903.B0.s.p4.d59-59 |
| P0232 | 40 | 7f1059994e332b6f7c80771775bc1c61f8b6d5cf3c488f7deead8c9d568a1942 | L4903.B0.s.p5.d61-61 |
| P0233 | 166 | dc9ad0ca9e4016f35cc4a4fbbff1627da945c01f6201becf4ee9a83f4eccf334 | L4903.B0.s.p6.d63-66 |
| P0234 | 422 | fc9050b850a3879accff27f5d54c2ccec82ee5f0869b294ff63a9a0e9bb093a1 | L4903.B0.s.p7.d68-78 |
| P0235 | 13 | d93b3f282ee295069c8dcccaff48035319fcafba49f9a186d2599f2d34f7c180 | L4903.B0.s.p8.d80-80 |
| P0236 | 364 | 1141e63b256cdd8c380cafb9dd8c73daf7b01876708ffb99a5cd78e7a702b0e8 | L4905.B0.s.p1.d21-28 |
| P0237 | 58 | a483d4b59edea5d02ebe9ec09695e875a9aa9d819b883019e75b175907100264 | L4905.B0.s.p2.d30-30 |
| P0238 | 139 | a1b610859f4673fae99d3a30247279b8e22aac9668685ade3906f4e810aea18b | L4905.B0.s.p3.d32-36 |
| P0239 | 738 | f0bcdefbbde581e67a7876bd60f1583c92646780c55e67cb9183b9c476fb4afa | L4905.B0.s.p4.d38-52 |
| P0240 | 50 | 1490ec350e3db0935f5996f3df684de442aa35fc4cc0fa0d16190b20f546b713 | L4927.B0.s.p0.d1-1 |
| P0241 | 50 | 9f165337c75ed63b6222883a5325775f1c40f6eb146102aa936aba5940306b7a | L4931.B0.s.p0.d1-1 |
| P0242 | 214 | 8d477022fe45a32e1b9fbd76a8187d20dd97017e50e27d0461c4fcc62a2c828a | L4932.B0.s.p0.d1-1 |
| P0243 | 195 | e644e47ad0eeeca7d88fdd5f2312d911bfb7ea069dd6e622108f3248819f4f5d | L4932.B0.s.p1.d3-6 |
| P0244 | 181 | 4df04c6f8482c0a61a877ffe5be86f1c613dadc5ba437d497c9977ea89546195 | L4932.B0.s.p2.d8-8 |
| P0245 | 162 | 8edf03f59396ef8b5884e23a6d4e373bcdb648cd36723959eeb69489b7e1cadc | L4932.B0.s.p3.d10-12 |
| P0246 | 64 | fadf5f8583834c6e3981b630cf28d91d87145e3443ee909dabfed7be403e6df5 | L4932.B0.s.p4.d14-14 |
| P0247 | 55 | e629e951906fe1fff46808df40cd6e920a686a412677bf69d178a7571ad23cf4 | L4937.B0.s.p0.d1-1 |
| P0248 | 4416 | fecdb208224294267ddf4c4ef84fe7eb7dfec5cef1fc0b2158a71b850a229268 | L4942.B0.s.p0.d1-1 |
| P0249 | 49805 | e625fc07ceaa011f72841388e79e7a688ac6fae3f20af8e6ae36cb0e09d2a619 | L4945.B0.s.p0.d1-1 |
| P0250 | 102 | f325f5f90476a6ace9d2afef5e2c43990f79b66a38d8903ec81faa211d68429c | L4967.B0.s.p0.d1-2 |
| P0251 | 86 | d42e3d3c87d48ec447678b8e886411a3148dc97944bab2724eca5cee0f6e77e8 | L4971.B0.s.p0.d1-1 |
| P0252 | 1340 | 352b73b0416bdb2e516fd179ddc5af3a0c01b05be3cfac1fc5af652251b3cb20 | L4975.B0.s.p0.d1-41 |
| P0253 | 64 | 7dbbba371061d2e0141ef41ba53d1f1bc255f9fddd579029fddf7d770d4be702 | L4978.B0.s.p0.d1-1 |
| P0254 | 116 | 39dfa45455748c34655220b2553ef8aa30496b7575334f82d6b9376feabc7f43 | L4978.B0.s.p1.d3-4 |
| P0255 | 214 | 0a5e1664029dc5838651b2ad266d443a27ce2ab612cb7c1fbca147817a588f02 | L4978.B0.s.p2.d6-8 |
| P0256 | 228 | 21b99c0bff18d8627ab8acba9ee39e04ba2cf322977d12861607d1dbe81ddfa4 | L4978.B0.s.p3.d10-13 |
| P0257 | 321 | 55b5c247f8c42fd28df530a5dd27467f9c7b61e10d750d4c0f28b971ad8598d2 | L4978.B0.s.p4.d15-20 |
| P0258 | 4414 | 9b4a770c68059195feed128f0b137b71f83645353e9f88c1fb6e50a1c2b0e6df | L4981.B0.s.p0.d1-120 |
| P0259 | 2047 | cbdd86cd47c97a73b0951ac998eea4be2dad21f5c158a3e162f326793d807011 | L4985.B0.s.p0.d1-23 |
| P0260 | 95 | e68772648f969c69374526b3289b385b6d4bbd2c5b0d02f64c01038b448e23a8 | L4989.B0.s.p0.d1-2 |
| P0261 | 53 | 01d6509fb5683429dc8f9e4d91928f63c675914db7c2bfabd430b385fd18b424 | L4992.B0.s.p0.d1-3 |
| P0262 | 579 | 29fdfb5313658bbb35fbfef6bcbdba88ea45aa4215546ed24b791b1324169a8c | L4995.B0.s.p0.d1-5 |
| P0263 | 16051 | e8c3bab7c75ad8837320ead3a94d2e8f638cde30e9651d5f0b064f2f44374d2d | L5025.B0.s.p0.d1-1 |
| P0264 | 3649 | 504de919f1debfd833fc2cbb35204376993e4f81352764e4bf332227c84134f4 | L5029.B0.s.p0.d1-1 |
| P0265 | 28033 | e6592906ff7646719b10e67e00daf7aa19cafb10b9075d562b116eec959bae05 | L5063.B0.s.p0.d1-1 |
| P0266 | 27626 | 3499acb5988f1152f3ab800d674eb8d1e434bab4029e7a0ef1400884968c709f | L5064.B0.s.p0.d1-1 |
| P0267 | 4896 | be17d002c1cdabc9254bd1fa64acb9bc67e39b242420bbe8c6137b2751c8ad74 | L5069.B0.s.p0.d1-1 |
| P0268 | 28160 | 29bc56c0c50e9c7fff83d738391d39b205c1ab75183f5933a8037551d41d67c5 | L5070.B0.s.p0.d1-1 |
| P0269 | 4 | a4c3ed04a95a3da14a9d235c83d868bed7c0f45cf7f3faa751ee8f50598d2211 | L5082.B0.s.p0.d1-1; L5086.B0.s.p0.d1-1; L6001.B0.s.p0.d1-1 |
| P0270 | 423 | 3f310fa4280d92ed74c7efc91b8cc296a34ae7d87d7bcdf79506edff7374a903 | L5092.B0.s.p0.d1-1 |
| P0271 | 10 | 691d4049cf395e34d5fd53a98721d9a240b1b6ab076e9e6892f60e8c16d0f69d | L5092.B0.s.p1.d3-3 |
| P0272 | 26 | 28f0924b82cdf41f8ce2ee704644f10bad0d343395caed393766c1c4b7474266 | L5094.B0.s.p0.d1-1 |
| P0273 | 6627 | f7654add8ab2703b00190a4e3644b94df2d1c58b0e794e093e62b5b4e7f96374 | L5098.B0.s.p0.d1-30 |
| P0274 | 1148 | 7e6aacd69e9872202a0023c3eba44c41491ac23c540b595a5f533f21dbc05d62 | L5099.B0.s.p0.d1-15 |
| P0275 | 14312 | b5d0f7af8a5f49914034e2e1842d252fab5e092672473bec0697c88f70596ef2 | L5103.B0.s.p0.d1-260 |
| P0276 | 175 | 7a4c117c3f8fcfc31cdfb63314057528e25a7d7be6cdfffa9abd71f21de59bca | L5103.B0.s.p1.d262-262 |
| P0277 | 408 | bc7fc1fac7cec8abeff26bc0b3187a6e4926ae6e7cb7794a9066150d1aa158ef | L5103.B0.s.p2.d264-264 |
| P0278 | 187 | c8e48e4f435f532cfcdee23f1a40236f3eaf3b71ae753dfb1951ec69e29eaf06 | L5103.B0.s.p3.d266-266 |
| P0279 | 367 | 97e50ce59c18d2cbcec06124259a1cda307f4d68a5586b27b473f08753f08644 | L5103.B0.s.p4.d268-268 |
| P0280 | 529 | 1e7d3b0d0c7a39d8e2554cbfad3c32dd2bfda870124991fb5a78747db95663d0 | L5103.B0.s.p5.d270-277 |
| P0281 | 155 | 6a27795cddd264cae03bbcb881caf99b1036bdda95e4d40bf4a425af80b6c413 | L5109.B0.s.p0.d1-4 |
| P0282 | 5 | b24d6d33736ecd5604a4b17bc9c6481039fac362bb7df044ef1c10a2bfd21db6 | L5112.B0.s.p0.d1-1 |
| P0283 | 4706 | 6c6b16376b23ba0f9b4e46d52437572b55837288b4dbcd475a7eef0093ef737a | L5121.B0.s.p0.d1-28 |
| P0284 | 191 | dfeaae44a46c01afa5ee63548b941693c8aad1515fac8afd36b8c557f3a1cf3d | L5126.B0.s.p0.d1-2 |
| P0285 | 241 | 3e75ead0b31f7fa250f3ef5c860fc6414bd7a12b2f5ef49c773ee4d1c1b4b493 | L5126.B0.s.p1.d4-8 |
| P0286 | 31 | 7432de7947af45c200808008c0f0e6051d6084b17ed3cc8bbc8f58d036ee056d | L5126.B0.s.p3.d12-12 |
| P0287 | 4 | 1a0f564ddc6039457b2fb26b3d6a316c15eba20a886449847c3210c35821a693 | L5126.B0.s.p5.d16-16; L5126.B0.s.p10.d26-26 |
| P0288 | 40 | 803d93d9a2c7fe578c260da2b22d89a6179be48ce60913e2462f710fdd2f17c5 | L5126.B0.s.p6.d18-18 |
| P0289 | 85 | 9dbfff1e0fd143a147c84600f3b2298c7dc546b67f9318800e0fff13643e278b | L5126.B0.s.p7.d20-20 |
| P0290 | 2 | 6c179f21e6f62b629055d8ab40f454ed02e48b68563913473b857d3638e23b28 | L5126.B0.s.p11.d28-28; L5126.B0.s.p12.d30-30 |
| P0291 | 296 | c9bffd9e09474566a73b9871d06c6673d9752b85c2255d65518bc27f795e2a27 | L5126.B0.s.p13.d32-34 |
| P0292 | 368 | 2627fb8cedc520d2747668d02cf3cb4cd4790a886d3c21c63110cb4133e88c13 | L5126.B0.s.p14.d36-40 |
| P0293 | 185 | 32abf8adda0b29111623babd918af0fd19a2f4785fde30acf1e570c54a36d11a | L5126.B0.s.p15.d42-42 |
| P0294 | 3233 | f51abd12da315e536ede64611963dacfdf629e7ffcd709e453db5dedf4f40a0e | L5127.B0.s.p0.d1-20 |
| P0295 | 186 | 328a252c7bb49dcb5e671b57c42b1043870f066693bea41caa8103691934b973 | L5131.B0.s.p0.d1-2 |
| P0296 | 472 | d21bb8a13f74041101ca71157ea28721066a8096fc12f0ed24bc91ab4fefe33a | L5131.B0.s.p1.d4-7 |
| P0297 | 233 | 1302b2ba8a09bed35946963c939ac1d415c8292114267dfc43b4eff17b7a1d10 | L5131.B0.s.p2.d9-12 |
| P0298 | 211 | 12452980912c0dbf1c58520d4f6c237bbb41ce43e09d4e2e31044dbf8a52de9d | L5131.B0.s.p3.d14-15 |
| P0299 | 119 | f56c96b406c11518d4b8c4b8c302f5272cdf216e3fbac7686bf9fda58035e9c2 | L5131.B0.s.p4.d17-17 |
| P0300 | 433 | 0d7dfcc57b1f3b6807189d33c93cf54b2c54d396cb2cc5ffa44efb24e5d826b1 | L5131.B0.s.p5.d19-25 |
| P0301 | 1 | ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee | L5131.B0.s.p6.d27-27 |
| P0302 | 70 | 09d88df79838fbd583312d781be2144fcc6d02d7dde88dd9ed383643ab9a8fe5 | L5131.B0.s.p7.d29-31 |
| P0303 | 350 | 094c8e2489c7c8cbba3a9fcbe68fd510414718d638ee8c0aa42741ffddadb404 | L5131.B0.s.p8.d33-37 |
| P0304 | 199 | 15b1de81212c6df6d05acf5fd0cc02f93a6589c6625536ca06d54df4ac0b4f8f | L5131.B0.s.p9.d39-39 |
| P0305 | 384 | f41b1b0715fc056dc24f4e2ec009bb5593e4179e0b5dc5903216ab1f64fa6523 | L5131.B0.s.p10.d41-43 |
| P0306 | 184 | 90f14c18b53c6b6827bf50227058968a930921eb6541c9894838676fdb010c9d | L5131.B0.s.p11.d45-46 |
| P0307 | 160 | b7a059ff7dd6e6bf9ca5edc6d4271f5acc7a71b0ad124c8f1f348b7dc01a554a | L5131.B0.s.p12.d48-51 |
| P0308 | 264 | 3d4cfad1b3213b28adee3d335012ff5d3406249bd014a0663a20182cd1455423 | L5131.B0.s.p13.d53-57 |
| P0309 | 287 | 42c9a3d3ad1cd611cf6f018227c4e90239d7620993a53fc720e7a056e10ece3f | L5131.B0.s.p14.d59-60 |
| P0310 | 498 | 6a021d9a5e8c082e5f87c64b465a01536ecb03d25e341961b0c8b02dd9cb4329 | L5131.B0.s.p15.d62-68 |
| P0311 | 139 | 0d8a09400353b351bf9cfbbe0cf93a7a549c2c7378299ceda0ca01715c01e372 | L5131.B0.s.p16.d70-70 |
| P0312 | 195 | b18802954a041e19c882dc3d6893af737bafe637c25860448da731bb260b565f | L5131.B0.s.p17.d72-73 |
| P0313 | 160 | cf83315958e2c58bded22739fb75e95e5ba0276688a8438a9f930e89e05432b6 | L5131.B0.s.p18.d75-77 |
| P0314 | 315 | 23376f364a93ca59e29d2e78eb0a4f6e36a71a15db094fdce0054b0263860b0a | L5131.B0.s.p19.d79-82 |
| P0315 | 1004 | 3ff4183351abd686362b31f22322a23a803a5706dabe69a4957c042ef740af3a | L5131.B0.s.p20.d84-92 |
| P0316 | 190 | c9ffd5046f8b8c76d406e5b1bffdbf8b369c5972f80c4db8e7c82c02641f57ee | L5131.B0.s.p21.d94-95 |
| P0317 | 105 | 6ad593cfcc09f7550ce6d89318e8f2423bb3b92b98e9aee7bfb4e67252b0246f | L5131.B0.s.p22.d97-98 |
| P0318 | 340 | 51dfb8771bdff2e10bc5f4cb5a522102ba413a5146ce34a25fa13b9e440ec756 | L5131.B0.s.p23.d100-104 |
| P0319 | 33 | 493c734fe7ec1329e05a59d0584c9418b64f8338f36743659d03994c21c8166a | L5131.B0.s.p24.d106-106 |
| P0320 | 53 | 3fbb738650399069dfaa78435b2fdd1b190e9484d26dbfe97d2117eeea86f849 | L5131.B0.s.p25.d108-108 |
| P0321 | 49 | 838de90e70939ca8955c3835868d5ca0cea2257446526f4a444dfbc0c2805ac8 | L5131.B0.s.p26.d110-110 |
| P0322 | 51 | 07b6b58cc45ac9c5760ce335c6d65444918f96bdfe084f80ca77e3344fe4ad1a | L5131.B0.s.p27.d112-112 |
| P0323 | 47 | eac63ab75f9beaf831d335b1b33e930c69896fdb1325675da8818c49d9e9c949 | L5131.B0.s.p28.d114-114 |
| P0324 | 49 | dfb724452aba1465629836c0ea05bea4dad9f33b4427f832e8160d2aafa8915e | L5131.B0.s.p29.d116-116 |
| P0325 | 19 | 3d418388fb18cacc9474cd01c2518d2d7da060a8ee78dd73610d6df233b3122a | L5131.B0.s.p30.d118-118 |
| P0326 | 36 | c64d445b20387d827acd9c156a5366fa2e2b6c85c8fafa888ccbfbf47e0d8b66 | L5131.B0.s.p31.d120-120 |
| P0327 | 858 | 8ff0c6d92ca81a0643174d11beca1b3b957cad67c1f9a749114bb5e4c8f8d6a9 | L5131.B0.s.p32.d122-133 |
| P0328 | 123 | e74e513bdca2f157c31c4ff48394c3ca648c1e6b6004984ea94d1cf6b624836e | L5131.B0.s.p33.d135-136 |
| P0329 | 150 | 8ac37c573a8432508ff334b675ddc6288bacf5099664e49b87d032fcd40b2add | L5131.B0.s.p34.d138-139 |
| P0330 | 115 | a00273b3f294fcdfe30a7f5896c689f1d31f7e108ad3ce67a2f3849ef4e055c8 | L5131.B0.s.p35.d141-142 |
| P0331 | 101 | ce54f9f4388f83067e06e5202c32ed4a7eef1a36ee26556318c739c9e44372ce | L5131.B0.s.p36.d144-145 |
| P0332 | 392 | 2b69e54206642769df5702284eaf0859f09403ffb273878e5b6006883c5efabc | L5131.B0.s.p37.d147-153 |
| P0333 | 25 | a3186bac5e9afd203ba37103c24a19359b9658b0105b06195e5db6c8815ef9e1 | L5156.B0.s.p0.d1-1 |
| P0334 | 137 | 1d32953d4300125dff22341065eddeee86eb1e2367cb32df64b03021077f64c9 | L5156.B0.s.p1.d3-3 |
| P0335 | 203 | 48671f7270ed784fd463a609144b485565a87d810b4e138e178040602f8bc380 | L5156.B0.s.p2.d5-8 |
| P0336 | 325 | aac238617a8ebd82d2e005b939a727b61f60b7c60db3e21bd2d034724da5f499 | L5156.B0.s.p3.d10-10 |
| P0337 | 208 | 51f4ac5df8b1c12ccc221e7ea9d42366d3c3200d82ccd4b2b2af2ae0f0d667de | L5168.B0.s.p1.d3-5 |
| P0338 | 274 | 8d878546c0e3e90ffebe720c920cce9414f6a5f4b37464fa51baaa9df92500c5 | L5168.B0.s.p2.d7-9 |
| P0339 | 48 | be439f9cf5e0c4bc77cb89f4ad8d1e9bc41060d4ad7a7965199137b13d0fe911 | L5172.B0.s.p0.d1-1 |
| P0340 | 165 | 721390b05a6a614eba7cdbc8ef97a92ef56c3a66e9d0e8cb6dd603c76fa393ba | L5172.B0.s.p1.d3-4 |
| P0341 | 101 | a29d9f07f897d8856722113f6792b56f1c359e1061a1803b7ce13f1f67535e4f | L5172.B0.s.p2.d6-7 |
| P0342 | 153 | c58bb5a2319a3abc8094a8ccce628c1b1cab92eacdb4927b40dc847e5f0cb3a8 | L5172.B0.s.p3.d9-10 |
| P0343 | 107 | d2d7ca186db6e674e6ea090bf101a68d11100dc955d0556472d42edfbb491383 | L5172.B0.s.p4.d12-13 |
| P0344 | 130 | 82306ea46e1bcd5a5efc24cc8ea9eb77419f5804186e716ed4a0bfbbb8efe1ff | L5172.B0.s.p5.d15-16 |
| P0345 | 15 | 295c866314bb16154f88f3c437169d90429670469f766460a2d0cd49f2b056be | L5172.B0.s.p7.d20-20 |
| P0346 | 161 | 3d187a92763f29d89483e4fc01e0726659dc1404b446e44e527cd8d679635827 | L5172.B0.s.p8.d22-22 |
| P0347 | 49 | d0630d021e6ad1b358163437a7fc0519bcdc39f61148b711225930466ec1a351 | L5173.B0.s.p0.d1-2 |
| P0348 | 1028 | 403b340551588a895e159a10fd95d20ca22e04eea675910b4d6ca9197764470b | L5177.B0.s.p0.d1-32 |
| P0349 | 152 | 59cb615a2007cec3b2b8937c4f91dd8b20d7b51446cab29bccd2ac7ab424147d | L5178.B0.s.p0.d1-1 |
| P0350 | 231 | e62eed267b5c3715ab13630d876070b6b2c613a3b304d984486adad495a0571e | L5178.B0.s.p1.d3-3 |
| P0351 | 120 | e3acd3416fbd3ee3ca620df6f3fb149241cce7bf1a41a2bb56cd146c237d6a5e | L5178.B0.s.p2.d5-5 |
| P0352 | 27 | 2fd279ec7553c8d5d02c9be710f950818b5572a4ccee3829798804ca64c33c38 | L5181.B0.s.p0.d1-2 |
| P0353 | 164 | c9d67414131286bfa84703903ad1b24b6dcbe91414c4927b4902f0e1f33fc7b6 | L5184.B0.s.p0.d1-4 |
| P0354 | 454 | df82c420074ac8007781ad7405f331131305c63601ff7fa82e0c72cf054f2a25 | L5184.B0.s.p1.d6-14 |
| P0355 | 257 | bb80fcd848c9c5d480a58124bf1d3f2599204a25adcb9139f7d96ac8dba6ccca | L5188.B0.s.p0.d1-1 |
| P0356 | 219 | d60421faa3ab972d38cf5730df27b45965e58bccf3c043ea37f05bb9a1660357 | L5188.B0.s.p1.d3-6 |
| P0357 | 267 | 39e084ef7f66053a420ea514e6dd335a3fa068baae6087fc169e1e338969654a | L5188.B0.s.p2.d8-9 |
| P0358 | 148 | 3d73e39c57e62909519c7217fc35b1b372e67f1865993f65645c6e2ccbefc4f3 | L5188.B0.s.p3.d11-11 |
| P0359 | 42 | 8c71410f1cdedcd62be8662f218e56a5dd9a5fc4dd45cdb244387592627c94cf | L5189.B0.s.p0.d1-1 |
| P0360 | 36 | d1c3dea02c26ac43d2c592b2f835140f229266501e3649e5d9ee9cf386c13b87 | L5189.B0.s.p1.d3-3 |
| P0361 | 348 | 4f1c89e43e2b00a364d933f23ac345adfb15d5a6a593827d96f34e1153567072 | L5189.B0.s.p2.d5-7 |
| P0362 | 289 | 845e0e04f2a817c32efcd8c33917e8e27a47275fef5ccddfd8ca2c3f5acb44dd | L5189.B0.s.p3.d9-11 |
| P0363 | 369 | 0019257e710e67bc6dbc5cb63913c97adbc115dda97c62cead9e4ded6ef8cf67 | L5189.B0.s.p4.d13-15 |
| P0364 | 290 | f13ba1b9406332863b957dbfb92f0b720d6dcce15e696b16d0c8fde3c8879512 | L5189.B0.s.p5.d17-19 |
| P0365 | 304 | 2729b6baf0d67e425ed92d23d8b8a39a419d2614a751849727505d62e06f8cc3 | L5189.B0.s.p6.d21-23 |
| P0366 | 278 | e5efadba88a68f75a0896495961f1099dbac95884d0648cade4bc574516ae603 | L5189.B0.s.p7.d25-27 |
| P0367 | 245 | 777e804b26e78cba0e659e2919ce3183bab60068325749e5e0db335c0fd20e55 | L5189.B0.s.p8.d29-31 |
| P0368 | 275 | b9760ab6dab146aa16b00e39c7836aff45a6943839b7086d5429f3bf89cad2e3 | L5189.B0.s.p9.d33-35 |
| P0369 | 276 | 52601f6c26a00c5491ca0349406e9e024fae1954e602d0a066da7b262754c58e | L5189.B0.s.p10.d37-39 |
| P0370 | 350 | 62176da66ad037bcf9943023ca1416475d0beeb9f32405b81ab893e839bdda17 | L5189.B0.s.p11.d41-43 |
| P0371 | 35 | 9cb9a5ffa0ba9495e842902a46e128138147c94c08f432e02c8a53e1b095588b | L5194.B0.s.p0.d1-1 |
| P0372 | 339 | 0bb7a5f02a19a1dbc3594ca6c180b3af07bcfa6af434bc3471ded4d8c021791b | L5194.B0.s.p1.d3-6 |
| P0373 | 321 | 8638216e23c416bca63ea461103b6c6c4fdc9f5391016fadb2b37bc5a8b3c815 | L5194.B0.s.p2.d8-11 |
| P0374 | 177 | b1b1e7882075369c0eecd1456529c7814fca70bd11623fb6bd37f5ce4ae08f6d | L5194.B0.s.p3.d13-16 |
| P0375 | 171 | f22da800f56b6d6e487b31cfb460ed348fcde0e22408b8527b972f5391d1baa0 | L5194.B0.s.p4.d18-21 |
| P0376 | 278 | 17eae1677afa6a5b40c947f8f19023925901e29cc3a137d48c948fcdc0fa5085 | L5194.B0.s.p5.d23-26 |
| P0377 | 302 | b2fd9b7509f3cbb74623016508e7e33b8c6f95d0fdad1de420ad124de21d5731 | L5194.B0.s.p6.d28-31 |
| P0378 | 240 | cc31d7cd266f4d373b3f3a5c36410ed0bf2030bb49e75dc34a9fc156d46b7e7f | L5194.B0.s.p7.d33-36 |
| P0379 | 250 | 45034337b7a31c55ef4be57a0ce1fe60dfb6ce79c79500f86f123d36be3f97e8 | L5194.B0.s.p8.d38-41 |
| P0380 | 328 | a8cad8480d4c07a24cb48eb7437e48639c401a114644bf1487db3652504726db | L5194.B0.s.p9.d43-46 |
| P0381 | 215 | 49465e3273d90ce13f0cb9514e91af1e78f6d912c0b4620ae1c7d39460114dbd | L5194.B0.s.p10.d48-51 |
| P0382 | 672 | ea9141379cf82ea2680262073ee40b3999a5b9414e7eba8c2c11c38be0def06b | L5198.B0.s.p0.d1-22 |
| P0383 | 16 | 7ea92bc0fcbf3cc65093c2f28a34cda596f1203d3b5f02de7ccd6799be00ebbd | L5199.B0.s.p0.d1-1; L5452.B0.s.p0.d1-1; L6402.B0.s.p0.d1-1; L6704.B0.s.p0.d1-1; L6850.B0.s.p0.d1-1 |
| P0384 | 116 | cb5bd78825436529e69404da34a9429defa78755bdb909bda58c30b2fb6c5ec8 | L5199.B0.s.p1.d3-3 |
| P0385 | 262 | ff92947155bb8b80a353ccb26bb9453254f978242e16d42456ac15064c6b950b | L5199.B0.s.p2.d5-5 |
| P0386 | 271 | 8089da4fc4383b45a9c98fcb93372518197cfdb87b19adcb550b7455e857c6f1 | L5202.B0.e0.p0.d1-8 |
| P0387 | 135 | 3c1c7e13dd85c4b358b92cfc3a6a363c77989865b692b74ce43746616bfc87b0 | L5204.B0.e0.p0.d1-7; L5380.B0.e0.p0.d1-7; L5505.B0.e0.p0.d1-7; L5699.B0.e0.p0.d1-7 |
| P0388 | 21 | 22c40e18fe2936702ee21697800fae6e15bcf26cd4bd7275cc73ee6f0a813ecb | L5204.B0.e0.p1.d9-9; L5380.B0.e0.p1.d9-9; L5505.B0.e0.p1.d9-9; L5699.B0.e0.p1.d9-9 |
| P0389 | 39 | 7ff4a8d96a9e8b41bc0567fa512c0fdafcd2ac7c5104cbdda872063692416361 | L5204.B0.e0.p2.d11-11; L5380.B0.e0.p2.d11-11; L5505.B0.e0.p2.d11-11; L5699.B0.e0.p2.d11-11 |
| P0390 | 21 | be1ca524ab1d244f108bde245f4a0c474d48ec28f80fbb8d5370b9030a49a887 | L5204.B0.e0.p3.d13-13; L5380.B0.e0.p3.d13-13; L5505.B0.e0.p3.d13-13; L5699.B0.e0.p3.d13-13 |
| P0391 | 112 | 50b79db90b06387795792af105abef57c991f1a8312e822bc4e74f533786d6ef | L5204.B0.e1.p1.d3-6; L5207.B0.e1.p1.d3-6; L5209.B0.e1.p1.d3-6; L5212.B0.e1.p1.d3-6 |
| P0392 | 9 | 9f947ee53f336973247280710c6a871008b227d58c7eed13d3c49a1f67863f5b | L5207.B0.e0.p0.d1-1; L5508.B0.e0.p0.d1-1 |
| P0393 | 111 | 84e9d7319f44080ffd7b9f2144ee02becf07f49eb4cdfcfff14f8d588339d92d | L5209.B0.e0.p0.d1-8 |
| P0394 | 45 | db55e9ac650b8039c46e6d528f39319b25fe7021e4dc5bc332c7975e52884df0 | L5209.B0.e0.p1.d10-10; L5510.B0.e0.p1.d8-8; L5518.B0.e0.p1.d11-11; L5702.B0.e0.p1.d14-14; L5722.B0.e0.p1.d7-7; L5725.B0.e0.p1.d44-44; L5743.B0.e0.p1.d9-9; L5745.B0.e0.p1.d6-6; L5753.B0.e0.p1.d12-12 |
| P0395 | 3300 | 9f998066c76901c068fc9c4ad2f7c1b6f5021e77b640ce931ad367104b7cba88 | L5212.B0.e0.p0.d1-40 |
| P0396 | 636 | e3e1c06dfc4361aa7761c10e19d9c8f3bb9aed24bea851a03618c7f013b017eb | L5215.B0.s.p0.d1-3 |
| P0397 | 1478 | 38a0faadf4010082c57f80270860ac0c5b8a3a01f4bf45f843deadf03ac26bec | L5218.B0.s.p0.d1-32 |
| P0398 | 177 | d80337216b27585b51a5157d48d60128098ec74fa49bd9231b6d60056f6d725b | L5220.B0.s.p0.d1-6 |
| P0399 | 9241 | f1f9543f703e78df34a67e2a6eb921d1a2e7cd4caab6a4ad882f9ac32a82408e | L5222.B0.s.p0.d1-76 |
| P0400 | 38 | 5b2a518d17504f734beffeba0e8c844f9173e6b5711014bf3996f01db8ec8204 | L5225.B0.e0.p0.d1-1 |
| P0401 | 111 | ff7fd1a26da71290b855b1a9d61d8e143a3817cf5c6e67cbe12b08974064b145 | L5225.B0.e1.p1.d3-6 |
| P0402 | 165 | 1fec68ac6de58f258b9fcce6b57f3fb28471c1256c351a38a4ac71ff2c3f82b7 | L5227.B0.e0.p0.d1-5 |
| P0403 | 42 | d4b76f193d0dfa69b024df3c7830cbc31e6b04cc766a7ae3ea0f22dc981dfe63 | L5227.B0.e0.p1.d7-9 |
| P0404 | 68 | a6d2a5e6297e9e932717c475d6d4c3a051aaf3622fc92f26f1b099a84f0bbc46 | L5227.B0.e0.p2.d11-11 |
| P0405 | 160 | 3782afd4564a86d60988c8e76c60467ca994a8846b6b226cb30f5142f1867697 | L5227.B0.e0.p3.d13-14 |
| P0406 | 72 | bfc805a4d96bbda648328043d1920cdd2f5d173aa8bc964422402585462498ce | L5227.B0.e0.p4.d16-16 |
| P0407 | 54 | c8934aa0017ef78fd665201bdda7b4a8d587a93bf3bf773dc2c23d16625ce53a | L5227.B0.e0.p5.d18-18 |
| P0408 | 182 | 691ddf59e41aea5543b01bbfc2bc59c19d84b1d8500cb9b33da6faf4d5ac61e6 | L5227.B0.e0.p6.d20-22 |
| P0409 | 256 | 79390868f2137e8d5c6fb85bdcd35ccbc291e0576250903af47d5e4b7cfde02a | L5227.B0.e0.p7.d24-25 |
| P0410 | 131 | 2fc1aaa513a521537720039e0190963a15a8cb22769c16b4bc125885fa46d177 | L5227.B0.e0.p8.d27-27 |
| P0411 | 77 | 420183671a3a446acc19b305a5a70924a1f84cf417f232a92d7d969d9b65c223 | L5227.B0.e0.p9.d29-29 |
| P0412 | 1 | abfbd10daf8965c8860b3582af942d7a7cac972b31d1c50f382b67d9b6c07365 | L5227.B0.e0.p10.d31-31 |
| P0413 | 516 | ed37327835ad490560d2805c246d216a9d9f891e7185c85e6269beeed9af7d73 | L5227.B0.e0.p11.d33-56 |
| P0414 | 155 | b34f5933caf62bc7ed97783c86136d03e902440f2a3a56a334ba33685b58a4ed | L5227.B0.e1.p1.d3-6; L5710.B0.e1.p1.d3-6; L5713.B0.e1.p1.d3-6 |
| P0415 | 1405 | b297909d169bc7e3f876536b548f938543eb04022f8aa84bd4e3b8ea24fd4d0e | L5230.B0.s.p0.d1-16 |
| P0416 | 57 | a2d4a7c61e98140b0cd33c89f373a3fcc9e1d2ca2b91a33ee6f6b576fae2c44b | L5254.B0.s.p0.d1-1 |
| P0417 | 165 | 6c5215d20e2655104a767bcfd2d3388ba2e6d193fa3ceb82915da06d62d055b8 | L5254.B0.s.p1.d3-4 |
| P0418 | 195 | 487bed386c90ea27b778b4e416aea6bc1e5278997b1790dfafe1f27b10a0fe11 | L5254.B0.s.p2.d6-12 |
| P0419 | 53 | 2440cdbf2c07da407801a640a3731bcb854730dd65284636f645f8a13481a0e8 | L5254.B0.s.p3.d14-15 |
| P0420 | 176 | 0befdd51103cd1f9da51ae5b7d83784fab026d7d167d1c2ac44d9ec82920485c | L5254.B0.s.p4.d17-18 |
| P0421 | 35 | afb6f57a318d01b4b1e660c8eda762bf4247fc61e02a545972ea5c7bf838c74a | L5255.B0.s.p0.d1-1 |
| P0422 | 21 | 621da122fa1683d5b44f8f067ce00bb8e6664a594b06386e2d8590baa173e752 | L5255.B0.s.p1.d3-3 |
| P0423 | 182 | 15b52801c52bfc3bd8c22e477b6f02e5ffc85364cb024166c0a756137315eb16 | L5255.B0.s.p2.d5-5 |
| P0424 | 17 | 6ff08d6ace1550da733df21696d684718cc7b04f69cbc2845e66c9a4d4de0d9c | L5255.B0.s.p3.d7-7 |
| P0425 | 38 | 7db25f48993b499c73f4e505a48bf6f0a72360e50118b0c0abe286bde3d838d1 | L5255.B0.s.p4.d9-9 |
| P0426 | 168 | 5967cfd49d6fb183cbc490f26c573b02ce667c78c41924aa3a4431092af8d3e0 | L5255.B0.s.p5.d11-11 |
| P0427 | 281 | 7294ad83a1c6df2dd8e40561a5f351051b73dc3c6b3a28942a9dda5f4809e15c | L5255.B0.s.p6.d13-16 |
| P0428 | 33 | 320470532fd017b11506c014032e1d429b9bb12e93ca364bab64b5a94e86d125 | L5255.B0.s.p7.d18-18 |
| P0429 | 101 | 3d78d61560ca227c82a85f6fca76389e551f42ce84361ef4658619c642415a93 | L5255.B0.s.p8.d20-21 |
| P0430 | 28 | 631bf643231df11caf0d8db10354bd078321e9e0f2bd4b260b1d5406ec97345d | L5255.B0.s.p9.d23-23 |
| P0431 | 98 | 8c7f1cc69c4e0ba24f73490022eb5e555913a4ff3cd2b5bf6e0027279ce3269f | L5255.B0.s.p10.d25-25 |
| P0432 | 80 | 5936e22d9cd7a749808b06493eb294ca5a42cb9729c9d9a6c15a57c00587853f | L5258.B0.s.p0.d1-1 |
| P0433 | 287 | 94f192af0c6aeaa2ece946a32400205edc0bd26ac163a1c32258045c3476073d | L5258.B0.s.p1.d3-3; L5260.B0.s.p1.d3-3; L5630.B0.s.p1.d3-3; L6079.B0.s.p1.d3-3; L6439.B0.s.p1.d3-3 |
| P0434 | 101 | 2fc1b11a0fce52e1113dafa9b0ed2c811658f16ab788d93823e1af203313d36a | L5258.B0.s.p2.d5-6; L5260.B0.s.p2.d5-6; L5630.B0.s.p2.d5-6; L6079.B0.s.p2.d5-6; L6439.B0.s.p2.d5-6 |
| P0435 | 119 | 64e1fd874111e195fcd5677f7481461527ac5c8a5041db313aa18258d5a10edd | L5260.B0.s.p0.d1-1 |
| P0436 | 37 | f66cdbce2a14d2df6dbeae31bdb7affa1313d0fe95e08e7ee223cc450d526f46 | L5264.B0.s.p0.d1-1 |
| P0437 | 45 | b804d118155442eeac1ecb70e97d7f3e5ba4195322506187c16565f95c0c64e0 | L5265.B0.s.p0.d1-1 |
| P0438 | 34 | 9be9707a3ff6059b4835d28589415091720e0649cee9473570cd3c596d24e345 | L5265.B0.s.p1.d3-3 |
| P0439 | 102 | 8ea69ffa8a49dad3f1da125ca2999d6d713aa26def0c2ca40a95ecea7f1dfa33 | L5265.B0.s.p2.d5-5 |
| P0440 | 131 | 02ca27d08115cdec377a0688a4e02a4ce86fb24a865a225d2db7da3b997ae5f5 | L5265.B0.s.p3.d7-8 |
| P0441 | 23 | c652730e232c2dbb467cd08e69f62eb0e71276e885c7441abae71e8c452d3d10 | L5265.B0.s.p4.d10-10 |
| P0442 | 194 | 117253ff5c54bd4217650ec2840285e0140f7140cf2d40e468d4a868da421e22 | L5265.B0.s.p5.d12-12 |
| P0443 | 247 | 3c813668899dce482b179782884586cb3e431b3494b657e0499c0228db06e9d9 | L5265.B0.s.p6.d14-14 |
| P0444 | 10 | 6c549a56db01e021f248d8da78289a3197d3db86fe0d36d33381d0dfc711e8bc | L5265.B0.s.p7.d16-16 |
| P0445 | 108 | 4a6d82cf6a89be110390a1cdce9c8ab68870a6bb9c2a30ad79c5d0cbab84394a | L5265.B0.s.p8.d18-18 |
| P0446 | 19 | 4eaba9d4f2341f00a48d006e014bc6be108e586fca62d854e4280a545ee72277 | L5265.B0.s.p9.d20-20 |
| P0447 | 202 | a04df1f14b766ecaab206bb98e10d8ca0f857fcc3cc49074e964adccb23eb056 | L5265.B0.s.p10.d22-22 |
| P0448 | 121 | 01123472cd30e82cbacb7576aed10319e9993b56e43f5be56cbaa4df8fe5e0fd | L5265.B0.s.p11.d24-24 |
| P0449 | 992 | 59846eeb42c84758e0bee0f196b2cbb35eb582edea9bee2b982e30f774b7f4e5 | L5269.B0.s.p0.d1-26 |
| P0450 | 5005 | d2e3385bde97f105d0991b29176b4d113c0d5fedb7aa48194f8949cb9eec13b0 | L5269.B0.s.p1.d28-32 |
| P0451 | 531 | 5329f8e58e665f288d914315982700ffe590ed892d3a62e108d558de35806d80 | L5270.B0.s.p0.d1-2 |
| P0452 | 4000 | ddd87a3f85b647862f89014bffb2acd865f0032a20a09e96c668df9c4625a2fc | L5274.B0.s.p0.d1-31 |
| P0453 | 4000 | ab82f570313400729bb37e1ba72abf67ee0ef4a96c3c771a6af05da7dc508c88 | L5275.B0.s.p0.d1-49 |
| P0454 | 7 | 06cbd62eb549a56b5ee511f6ebdb9b7361872db759fb65eb2e6ee7b5fe4f4b84 | L5279.B0.s.p0.d1-1 |
| P0455 | 1770 | 92cba2345eefbdca0a35738db9b2ffd570c177628d60b26c5dfacc2e5d398dcd | L5280.B0.s.p0.d1-11 |
| P0456 | 1204 | 2177ebd938f38c12bf22efb121786efa18014da98d16274fa996605d1e564207 | L5284.B0.s.p0.d1-16 |
| P0457 | 2499 | 03f3f2237e19040f8a201a16c521d5db5ae6654e7b25e48fff119b2d281e912a | L5285.B0.s.p0.d1-34 |
| P0458 | 128 | 751a086110e2cc63a9c08210d6ec6fe6b9f7be6bfd51a2453be03131e5855698 | L5288.B0.s.p0.d1-4 |
| P0459 | 553 | e750d86dafa44c114d72f0bd9c0c2630de1d8afcb88eeb096fbb50db85fbd367 | L5290.B0.s.p0.d1-13 |
| P0460 | 1062 | 2fbe60ec6456738719cfd6e9926bc295f0379546c66a1cb16297f869a2701101 | L5293.B0.s.p0.d1-25 |
| P0461 | 12 | 6a221112d7fd41cf3950158643c579b4548c047d28be1bb8086a245b2607f49c | L5296.B0.s.p0.d1-1; L5308.B0.s.p0.d1-1 |
| P0462 | 114 | c0cdebee37a5d0887e340043b58f1801ea5a2ead62e79b89164d158a4cce447d | L5296.B0.s.p1.d3-5 |
| P0463 | 43 | 6e82458efc886849e358542cb30f83f1e9f2121c999caed2df206e79cd0093e5 | L5296.B0.s.p2.d7-8 |
| P0464 | 73 | b561855165f08e474f3676b3ca21b5d327f75577b1c6582d3493436c95eb9056 | L5296.B0.s.p3.d10-11 |
| P0465 | 69 | d171b806992c71cc6d2447f5bc3b81b956b288119e6115c73363662de1da0af2 | L5296.B0.s.p4.d13-15 |
| P0466 | 119 | 85f5ce905a046ca642a98bae107fd2295c6b3f5f0fd8e019342b1ea85e0ef44f | L5296.B0.s.p5.d17-19 |
| P0467 | 488 | 3b897ee7d0696036909c14860e389f97f07480a13c54c6f5f61e4f1d20e3c433 | L5296.B0.s.p6.d21-29 |
| P0468 | 139 | 196f5e8d851fdadfa004480da38e1ba23d329b99da9a432527210aeee8701fdc | L5296.B0.s.p7.d31-33 |
| P0469 | 144 | 9f6c4109974b4c198f08c20413c5a7b7c3af9bfb40403d30f03ab82ea4126348 | L5296.B0.s.p8.d35-36 |
| P0470 | 119 | e829af0e9698267a62dc2fad4350a223b09fe33859809f3fa1fa29b384145a6d | L5296.B0.s.p9.d38-39 |
| P0471 | 145 | 6b7c58cb7d75a22aa2120d7e3731dc9fd56995221d77fdb8bbd842f1f32d3f50 | L5296.B0.s.p10.d41-43 |
| P0472 | 49 | 967138ef3192cfa2a2c579ed301c745a8f288eee0b851606aa0ebc112907374e | L5296.B0.s.p11.d45-46 |
| P0473 | 4246 | 24c607e25891027361351971e958bb8ae252d068c7468a3977cbb9264ffa38aa | L5298.B0.s.p0.d1-52 |
| P0474 | 85 | d4a3bde6f6eba0a604220c8823289f6f37ccc1bd76d439d30221ad30e73ebb15 | L5301.B0.s.p0.d1-2 |
| P0475 | 132 | 4b5a95cb2539a9817f314e83f9b956ad6aa2856370ce094ee8690e91ebc26011 | L5301.B0.s.p1.d4-8 |
| P0476 | 493 | 040ac9e254d91cd0d2192e11d192086df0cf2e3e9a3b6b88be8f4a6a4bbb0bdd | L5301.B0.s.p2.d10-17 |
| P0477 | 747 | f0f4f06405a5ab3a7f632f22f84c39e2a8ad063d39073ced32cc634ea557a83c | L5301.B0.s.p3.d19-38 |
| P0478 | 733 | 70e5f1df25a746b13025723e95ca9b5d540a4984d1d07761f416238d83b9b375 | L5301.B0.s.p4.d40-57 |
| P0479 | 1066 | 758dc145fb89087908e6c17cdd97661a3263845979700bb021160c6524004420 | L5301.B0.s.p5.d59-76 |
| P0480 | 102 | 2ec909c83ec340f494cfe0b952625bce044153636fda0bf8f9ab6bdf84ad1a1d | L5303.B0.s.p0.d1-3 |
| P0481 | 805 | 5ef95625e1d9bc4148dccdd0b9228065afcd2cd5310e54e4d48c255555fe4810 | L5307.B0.s.p0.d1-7 |
| P0482 | 35 | b9264d579997e1d62189f711d78f1fabfdbc68c4394c6df45c2ef3c763ae17c6 | L5308.B0.s.p1.d3-4 |
| P0483 | 99 | 2ab2f3f3df7198d7fb1a3cd3357def573c7a8400608a1801a93de467717c8fbc | L5308.B0.s.p2.d6-7 |
| P0484 | 37 | ff8ec2f95e54c603482a5cb1f030cf51693eaaf2eea85a277b53fc0fa7d558e6 | L5308.B0.s.p3.d9-10 |
| P0485 | 468 | 91daf3cd20c7b6b28e73879b7df8068e55186d6049658c3547a56ce440ca5e3c | L5308.B0.s.p4.d12-21 |
| P0486 | 181 | 374c82899c32096d7779da18793962a937c1d77946997eaf116922eb6dda823b | L5308.B0.s.p5.d23-24 |
| P0487 | 157 | e42f175ba3449d7f475bc62bef134bae1592aa77e721e9e413f4ab7706b05f9a | L5308.B0.s.p6.d26-29 |
| P0488 | 82 | 93e4687a742ca860ec9250cfe97caf147c42a7969a8c25d1dea630fed8299bb4 | L5308.B0.s.p7.d31-32 |
| P0489 | 142 | 2c788f9b90a5e701bd3e1f424caafbe18ad1968df459843152abd918f26365e5 | L5308.B0.s.p8.d34-38 |
| P0490 | 309 | 75b2da791fc043260d27bce07a3c54d949e12dafd5b44fb1fde80f9e9b72a913 | L5308.B0.s.p9.d40-49 |
| P0491 | 289 | c750312916313007713066a6eeb51f93ec39bd3f9f24f425b7da462e101b9c5a | L5308.B0.s.p10.d51-55 |
| P0492 | 53 | 3801c46083166b9462d2d2f1bc3cf34a75cc26602b8bf00498aa09c1009e3a87 | L5308.B0.s.p11.d57-57 |
| P0493 | 111 | c911865c2cf8be4af7bb0369bc9297bce4caaf5aaa403dbddf404ff28a6796e0 | L5308.B0.s.p12.d59-60 |
| P0494 | 101 | 64845cf4b6a0e4633fa6a021f3f8d7eba71df614d1f266e0dd003af1a0f1310b | L5311.B0.s.p0.d1-2 |
| P0495 | 7 | 77453cb7435a1abb6a62f207b81b94f72b7890f19131f7b8778c84a61ddb9e80 | L5311.B0.s.p1.d4-4 |
| P0496 | 40 | 3bded860115df401b86c42df0ca1a2463bf4dc104961cabc5bdcfd81bd0aa543 | L5311.B0.s.p2.d6-6 |
| P0497 | 10 | b31d8319ff6ec286dc36b4cb4331f3d31c8191ef1a09cbbd03e7fa214533fb09 | L5311.B0.s.p3.d8-8 |
| P0498 | 18 | 49722383807fb1ad38e7ac5a062fe305e67a40a666d65fe0abcd160ba39128e4 | L5311.B0.s.p4.d10-10 |
| P0499 | 479 | 4cd729a96d10fda681d164f7c5f3a09bec6355553f37ef0014752e19bd5463a7 | L5311.B0.s.p5.d12-12 |
| P0500 | 39 | 693302cfd9fc9de58ce77dde71333013f5c0bd8b50a8c62512fccb2d5e4065b6 | L5311.B0.s.p6.d14-14 |
| P0501 | 274 | 26b8ffd48787479e1d2c3542942709d20020092a71a444aee7abbc6b07c548f8 | L5311.B0.s.p7.d16-19 |
| P0502 | 6 | 18885f27b5af9012df19e496460f9294d5ab76128824c6f993787004f6d9a7db | L5311.B0.s.p8.d21-21 |
| P0503 | 15 | c43ad8a8a33ac74b30fc129b6e53153ee6ab495e676a71582908a7ae4ce4ab59 | L5311.B0.s.p9.d23-23 |
| P0504 | 766 | 953fc82178584bbe98c9e17214af3d619bbddd707c01477352570a8212192d6f | L5311.B0.s.p10.d25-32 |
| P0505 | 98 | 85da2d93179be218b3acc6ecfaf6cf54ba755844810cfb8257e449928016506c | L5333.B0.s.p0.d1-2 |
| P0506 | 1980 | 8a3685359d4579c21e04dd39a6187cbfea9f2063ba266091f5562379f7f7d82a | L5336.B0.s.p0.d1-29 |
| P0507 | 120 | cbedc1610b743f5046e6fca1dc9af551a19f0d0776ed5f2feb2af14fa9cb5ee7 | L5338.B0.s.p0.d1-1 |
| P0508 | 13 | 302f177dc23a72fa6423b30c23235821e06f3c05d158af9999a1734f4d5fe2e3 | L5338.B0.s.p1.d3-3 |
| P0509 | 177 | cc423810a8bcb92dd6982dacd9427ef366cad12c1abf5d547af982745b20d3cc | L5338.B0.s.p2.d5-5 |
| P0510 | 134 | 605a934cebbd756a8f158866358d955270837a9b3c2de5d839e3ff8967f95468 | L5338.B0.s.p3.d7-7 |
| P0511 | 97 | 44698176bbaa2705161704ddea4844ac9be40470ceadc1827a2e9a6b01afc2fb | L5338.B0.s.p4.d9-9 |
| P0512 | 111 | 300b0415bda6709713d958865f954c2e7997d5ebfd35db5ceaf842981afb2f48 | L5338.B0.s.p5.d11-11 |
| P0513 | 36 | 733bec294815c2ef0deb6d5d0b5b48c1a7c1426a0543c3b466b260d13c346e49 | L5351.B0.s.p0.d1-1 |
| P0514 | 306 | d9d1841318f92b8ab6edce01787851503e17234e0e04c4f8925b10b9d1da607c | L5351.B0.s.p1.d3-5 |
| P0515 | 161 | c3fb2cb8127a488c775bad9b5e7915cd7e70e2ed6ff4f299ccbe5044b0efa762 | L5351.B0.s.p2.d7-9 |
| P0516 | 216 | ac3abfa044f0c99f203680e46cca0e5edcd95ba5990aacd25180d66b9cc7b23c | L5351.B0.s.p3.d11-11 |
| P0517 | 41 | c7bf5288402fb46e1223326fc3c8f26b709c31e5b282c88d8956984bc308cbee | L5354.B0.s.p0.d1-2 |
| P0518 | 1521 | 75016560a430e7970ad189cc03d6cde0fb0ea4b2e5b16a84228241eda14c581a | L5357.B0.s.p0.d1-40 |
| P0519 | 267 | 158ccbe30d314306dcc3dbad580ac55448dd75e6a70a0e7ab9a716364db93b30 | L5360.B0.s.p0.d1-7 |
| P0520 | 236 | 528f8aa56d278a5e556e1628bc3d15c2830540e83af8ad0e2599ceb290d96670 | L5363.B0.s.p0.d1-2 |
| P0521 | 2045 | f8b434bf1eda3be3a0bf9bb197eb73d735b2cfba04d42de7c34e8160b6ca546c | L5363.B0.s.p1.d4-8 |
| P0522 | 259 | 8522f4c3fe44a11795f9c5d8528823d184398e59bdc755bb400b6f35b643eda3 | L5366.B0.s.p0.d1-8 |
| P0523 | 217 | 01582bb9b2c389eaa29c56bbf2fd9b745658b379557979338778602cfc2a4132 | L5369.B0.s.p0.d1-9 |
| P0524 | 1049 | 48be251844e191557af7b04e7348b7d00317259dfca48df4ff51b1433290b326 | L5372.B0.s.p0.d1-4 |
| P0525 | 272 | 96fa2dd2e919d42f04b7a327cad0cf71c13768101a9e5061739f265c6355cf1f | L5378.B0.e0.p0.d1-8 |
| P0526 | 114 | 3335298256b08ec4eae6df559cffd28738ecb1b37ea4795e7df12fdf56472fff | L5380.B0.e1.p1.d3-6; L5383.B0.e1.p1.d3-6 |
| P0527 | 3300 | f850d6c6364d5b31621b6b5b621d63c24ff0142ac7e3863f5843894173029281 | L5383.B0.e0.p0.d1-40 |
| P0528 | 418 | 7663b1f377c3d323c6e5e1e93dc6a15d6ed7f22c76d518c9ad9adf5d0a7ecc9d | L5386.B0.s.p0.d1-2 |
| P0529 | 1628 | 7156daaec79fc54cb329d4fa4889aad3dd25f4c80f7870bf418c91cc00d70e58 | L5389.B0.s.p0.d1-6 |
| P0530 | 498 | 630add0db33d8989d5cbcfd354f374d88112c646199d28df76930a2192e40a60 | L5392.B0.s.p0.d1-4 |
| P0531 | 611 | ac971613f03d7529dfe376b07f6f79cf5f3390d3c7d2583202d6d01012d126a3 | L5392.B0.s.p1.d6-9 |
| P0532 | 632 | 6fed35396b288d9640824812938082c8142fca530d7f277f7166c0171b3a7043 | L5392.B0.s.p2.d11-14 |
| P0533 | 465 | d51f0513b20a1cac0269361cd9315f75ce9f115731b3fe660369605f99c6d223 | L5392.B0.s.p3.d16-19 |
| P0534 | 3030 | 0b169607038f9cd87a76b606916bed8bec510b17dd0141137872c8088e7a9094 | L5395.B0.s.p0.d1-44 |
| P0535 | 733 | fd6b5bb9fd3bd0586e0ee6a2fbd01e9324b0c0aed7888189c7e8e3f503c2bfed | L5398.B0.s.p0.d1-12 |
| P0536 | 3862 | 957dfec3363f778b298855bd9555a70aa7e0c46daf70cc9655a278eadf19f6a3 | L5401.B0.s.p0.d1-32 |
| P0537 | 1443 | dda0d3c03d481e07460fe350330ec5587265bff62164cd6dde535abb22379a8b | L5404.B0.s.p0.d1-7 |
| P0538 | 4127 | a6e68a88a5ba795510b2344ad09ff20e20e5b35dc813386df558d4319162bd07 | L5404.B0.s.p1.d9-26 |
| P0539 | 225 | 5b4af864ec30cce54163cc4d7929207eafa2ef6c272bcd1073cb0198b364ce2b | L5410.B0.s.p0.d1-1 |
| P0540 | 379 | 645055770621afecc0e48bc49940b3f952713fc03cf6dfd078d76b04637a55b1 | L5410.B0.s.p1.d3-3 |
| P0541 | 33 | 8c3b9375a67bbada79010acc744172bbb7e30bdcedda4c18a536c088172c491f | L5435.B0.s.p0.d1-1 |
| P0542 | 99 | ed90b4b61ba1f2755038f1e9496e44b9dbe3b4d8f11184b4a5bab735d3335ed8 | L5435.B0.s.p1.d3-3 |
| P0543 | 306 | 559a542df40d2531d606607b2d88784366d1aeed2c8eb8a496bc8242197724e7 | L5435.B0.s.p2.d5-9 |
| P0544 | 211 | 03106c7f90632d3b2fed733d591f6a206ec773a841508c55480390f783f91fc0 | L5435.B0.s.p3.d11-11 |
| P0545 | 147 | 63e939a8eab9386319c7db9aa7149474bf8a0a7988d9bab0f025e0c6be76f868 | L5452.B0.s.p1.d3-3 |
| P0546 | 148 | fff5e6b03ec20ce68ae7d46cc60b86cafadaa17e7a9347bd7d6b0316a537215a | L5452.B0.s.p2.d5-5 |
| P0547 | 241 | 592355027bd88f81e1413451de0386916e85063b29060ca05b31732823e1142c | L5452.B0.s.p3.d7-7 |
| P0548 | 25 | 031d0f379fa38b672c941bf2ab427655df01168e3bf183d8190bbcc2b547ce61 | L5459.B0.s.p0.d1-1; L7280.B0.s.p0.d1-1 |
| P0549 | 66 | 5a1165daf3724f60a5a09d6fb4bd2e5bb44865dca64b3a27b3639addb227233c | L5459.B0.s.p1.d3-3 |
| P0550 | 132 | 0063205115272eef9ae1a998207d1450734f3938263becda6ba133281a9cf823 | L5459.B0.s.p2.d5-5 |
| P0551 | 81 | 5cf964e7820175cdf2cce8cb206b260a267b64635b501015b70e5ca64b53af8e | L5459.B0.s.p3.d7-9 |
| P0552 | 298 | d4982a2b1eb7c1f0f864d4db2a28df38f425d582912ac188b3abd5d5e7c6d62d | L5459.B0.s.p4.d11-11 |
| P0553 | 44 | e601ff1ff949e124692325a0846cd9b34af58fd056602b90f0cf7d88e0dd9157 | L5460.B0.s.p0.d1-1 |
| P0554 | 69 | 2d42fce9626f172aec033e286131fbafbbb516260aa595b54759c290a037c793 | L5460.B0.s.p1.d3-3 |
| P0555 | 262 | b6bf870b166e6d900f8c8e92200c37495187092f3b072967063179d484fa4803 | L5460.B0.s.p2.d5-7 |
| P0556 | 282 | be1b9da5133a156f1fa3db6833ced419ca730c75f26490050a44c22ed13b7db0 | L5460.B0.s.p3.d9-11 |
| P0557 | 463 | 0e831a1bf060586608a674724e986db4e221ad9796830177f666b08a40ed3cf4 | L5460.B0.s.p5.d15-15 |
| P0558 | 39 | 9da150dfc0178d3d981411c1876d04cf99749775e68b7f1681ae85d156fe4377 | L5463.B0.s.p0.d1-1; L6445.B0.s.p0.d1-1; L6805.B0.s.p0.d1-1; L7817.B0.s.p0.d1-1; L7880.B0.s.p0.d1-1; L7967.B0.s.p0.d1-1 |
| P0559 | 283 | f505269b24e27d04db866f5121487ea3250e14a956cfc52bfe59e07d85f15a03 | L5465.B0.s.p0.d1-1 |
| P0560 | 1151 | 75a12c16cc32587f6d9c1bf1efc443e2582c5a1cc2e765e799eeffb28bccf111 | L5469.B0.s.p0.d1-22 |
| P0561 | 59 | ad0a2e73d232db4aa6639bac9739a81452e9aebd829d93c76eb8f7bccc57a690 | L5470.B0.s.p0.d1-1 |
| P0562 | 132 | 5f2b6d764ffc2bbc6a5b56de60d977efcdf7aa2c8b2732c3b4b60e34afafe4e7 | L5470.B0.s.p1.d3-7 |
| P0563 | 29 | 872c4783998b642a9e4d1cdaeaee298d04c12cdd7c5f1c148085f2f5f7a9db06 | L5470.B0.s.p2.d9-9 |
| P0564 | 79 | 9923bf77038159884a5d986813cae66305979039a826f011f5cf0757725cfdd0 | L5470.B0.s.p3.d11-12 |
| P0565 | 220 | 7517ae4bb3ffde01057451616849fa4e61a3ba5b90f7fc4185029c033a13a3fb | L5470.B0.s.p4.d14-19 |
| P0566 | 167 | a06275f7e3b0886ecf96b1eaf9ef68215ea8fb7f6b3e27edeefbbb848730c2dd | L5470.B0.s.p5.d21-22 |
| P0567 | 137 | e351a5d5066927544c47ce2f9d6a184e6916a3055c5e47a7c172d3e51f8f1df2 | L5470.B0.s.p6.d24-28 |
| P0568 | 100 | aa5c89526d107992278000cd150d4b077036bcc9da7baf1d0b8c9dfd3dfd0d5b | L5474.B0.s.p0.d1-4 |
| P0569 | 80 | de979143740e8725d6907ee290aa498cb5e9700c2ac2928312840b4d759c6793 | L5474.B0.s.p1.d6-7 |
| P0570 | 141 | df0ebe33a93b77b3452cf20c75f3b28700bfb054f80f8b411a4141ddb4d2867e | L5475.B0.s.p0.d1-1 |
| P0571 | 153 | 69a44469e4ddaba23c281e49d51dbb0c43581241acddd6a86c66eb7d411be427 | L5475.B0.s.p1.d3-3 |
| P0572 | 218 | 59d7a96034f72dc0e2607258f2bb39efff164c2eeea85c5461c6769865537e64 | L5475.B0.s.p2.d5-5 |
| P0573 | 208 | 6325eb849fca45c775929ca7c35f218a91b3ef762f47bc4fe66549fa6df9ad01 | L5475.B0.s.p3.d7-7 |
| P0574 | 14 | d5558cd419c8d46bdc958064cb97f963d1ea793866414c025906ec15033512ed | L5478.B0.s.p0.d1-1 |
| P0575 | 10 | 4099b819142658940aa6c819b2612080058f57617757d1b3a0cd9af7979334f5 | L5479.B0.s.p0.d1-1; L6074.B0.s.p0.d1-1 |
| P0576 | 156 | ed8102135c435430d082a4912563ff308dfdcc1715c876da6bdbc9cc4000a267 | L5479.B0.s.p1.d3-3 |
| P0577 | 176 | 8bc0e46aaa702d6c1330d1621eab03ea5df1e6be8ca1f655c6369c207d456451 | L5479.B0.s.p2.d5-9 |
| P0578 | 228 | fb62e9eaca36bf3f8f959d63e8bcafddfa1b71e715dd0851152ea57e496994eb | L5479.B0.s.p3.d11-16 |
| P0579 | 129 | e4c495bc345a16b1818a8a25246eca49577a27457e21bc26f315babed4ceb432 | L5479.B0.s.p4.d18-18 |
| P0580 | 34 | a6284e6d3d43bdfbf0da732945adb2b4f31147c92bea47aee100d7f556c22d00 | L5483.B0.s.p0.d1-1 |
| P0581 | 55 | 834448b858303b3c04ce2c1f2d19dfc12c4355fbdab13fefaf29e990cd61c8ef | L5483.B0.s.p1.d3-4 |
| P0582 | 65 | 6f65b272fa6b50109b76f8410bf879b088336a16804d8c5928fb5b9b597eeb38 | L5483.B0.s.p2.d6-9 |
| P0583 | 55 | 8ed7b3fb7f7fc2ae966166f171b7a237a69a455717e119e94a9be521dd3610c2 | L5483.B0.s.p3.d11-12 |
| P0584 | 181 | 43ed74c2b44a2c0a53da37bf07c04ba79f98716093ed5a1babbe6903ba4469bc | L5483.B0.s.p4.d14-17 |
| P0585 | 72 | 84ee427532f462719631bb839e6ba1dd2bb6c2c0996b18f8bcda1e9a987f7087 | L5483.B0.s.p5.d19-20 |
| P0586 | 65 | 8c4c5521f5dd75e521e568f82dc0f92620d493ef2e01add4f766724f8df1582a | L5483.B0.s.p6.d22-23 |
| P0587 | 104 | 67eb19d4b94eaaaf245165807ece9f5e895474c5b58fb6c1381fdb05d6faf0f0 | L5483.B0.s.p7.d25-27 |
| P0588 | 60 | d087698deccfbffca8cb26c0f824e7c1b6cb6503ed999bc4b2ee297ea86778a6 | L5483.B0.s.p8.d29-29 |
| P0589 | 108 | 58736def7f68b6bdbcb5e8bf274e550d22c9c249b874fc140975ed98c7a0ae70 | L5483.B0.s.p9.d31-35 |
| P0590 | 563 | 3d72057fe6108ce3753178fe1c71f240ea8ae3f7361647803ab8e67272b7ced2 | L5483.B0.s.p10.d37-50 |
| P0591 | 126 | 8e73fbcc62aef42c76d309c288167268af1281af8044b4a260f5ccd508950694 | L5483.B0.s.p11.d52-55 |
| P0592 | 109 | 215fb0e23364d1a584856cdedc0565c3ac64cca61d6c77bee485cdd29c51042c | L5483.B0.s.p12.d57-58 |
| P0593 | 49 | 7778172fb8d4387cde5e6871a8baceb029f594a0ab3b1712727c8e2e612a8102 | L5483.B0.s.p13.d60-61 |
| P0594 | 164 | 2994a64fc8af082e773067d27d530147776315760c28ab0a48ebe0b2787ccef7 | L5483.B0.s.p14.d63-66 |
| P0595 | 225 | b7a0bbbbbb5fdba798646de7b1eefccc110bdc565b0fb6c081ea369935b6c8e7 | L5483.B0.s.p15.d68-71 |
| P0596 | 36 | e01d3de7457e2f4906f04085482c62edb3619455455a00924cf3214d2cd435c7 | L5483.B0.s.p16.d73-73 |
| P0597 | 58 | 33a94e67a983efcf16778f92d60b0ff307fd0cb0aae58486ebe4e88703588248 | L5484.B0.s.p0.d1-1 |
| P0598 | 78 | e1c089b786d04aa5f910a3b6807badf57db42460a4816d08e02d04ac0bb15daf | L5484.B0.s.p1.d3-3 |
| P0599 | 26 | 170ac8bec301d720fa9be8f9624653a92a54fc9f4ecfba6235c7abbc079cc1ed | L5484.B0.s.p2.d5-5 |
| P0600 | 133 | 315b881f65a7e6413e3932dbf9e337064688f786f95369aa9aec5002791f78e9 | L5484.B0.s.p3.d7-7 |
| P0601 | 16 | f2cbe0122f3af53884bbee8728d94e9d2795b3cd4cc1c6d0fbd6a2ece6b6e4d8 | L5484.B0.s.p4.d9-9 |
| P0602 | 244 | 3bb32edd61aada234483b75bf3f83144bdc63b92555793e5a281783b0df3fb59 | L5484.B0.s.p5.d11-11 |
| P0603 | 19 | 7c3ee248c65bc57f4790fb46f93c8f8a371f96b38526dd35d74f0e2c4fc4b4d9 | L5484.B0.s.p6.d13-13 |
| P0604 | 184 | 969c26e18e67d23377c31a9ced96f29ee0c725351e8d4caf4aaa22b3fa1b7636 | L5484.B0.s.p7.d15-19 |
| P0605 | 134 | d5fe909198609197f7a6d7ec48844f2cd55b351151e1d2c135a4811eee4efeec | L5484.B0.s.p8.d21-21 |
| P0606 | 284 | afa304fa0848ef7d08004df92fd67f4dcb56da0d61ab771667bfb22813932ed4 | L5484.B0.s.p9.d23-23 |
| P0607 | 824 | 25321f01f342f96f0802ac08b3ef7d45a74d26f94197f0033cc42a0e8ab8b90f | L5488.B0.s.p0.d1-23 |
| P0608 | 125 | 8e236b4ab771c4c7a11fea2ac05ec3a11f20e542df0fd18c6cacc14c14f83264 | L5489.B0.s.p0.d1-1 |
| P0609 | 332 | bd9e59e347ab1f4ef4bfff5b71225f1bb2078146ab877e4fa5d6819998f9df82 | L5489.B0.s.p1.d3-3 |
| P0610 | 144 | 4a20de85bcc2f59bf55ee97e7a3a40e0ec515cca6f338bacc3d096745c69c587 | L5489.B0.s.p2.d5-5 |
| P0611 | 1159 | 5b781d1f602ee49e6b21b8a5e065a4d08ddf7557ad720b8075f894bb62971ab6 | L5492.B0.s.p0.d1-31 |
| P0612 | 126 | 751790d168b8c5900057984e2995b6586b9ae65446e09f17fa2e5869ad672e15 | L5495.B0.s.p0.d1-5 |
| P0613 | 614 | d01b9271d0af6b83ba8d9234286b901dd0e8ef5bf672543dea5fb069ba772f2e | L5498.B0.s.p0.d1-16 |
| P0614 | 38 | acd2e492948a889e0c94b34b07c97fb314d18463496c1ea4db98c1eeb5a28985 | L5502.B0.s.p0.d1-1 |
| P0615 | 272 | 097e87c800753e8cdad0220e4827e3b56f5d4b5aa2c35bae35560ae7adbb97bb | L5503.B0.e0.p0.d1-8 |
| P0616 | 114 | 8f8ef46d6aec158a401dda6dd508d8aa6729c83db282607211262f326b56043a | L5505.B0.e1.p1.d3-6; L5508.B0.e1.p1.d3-6; L5510.B0.e1.p1.d3-6; L5699.B0.e1.p1.d3-6; L5702.B0.e1.p1.d3-6; L5706.B0.e1.p1.d3-6; L5707.B0.e1.p1.d3-6 |
| P0617 | 233 | 6eaf4da08f434f8df2f29a0eec5a8edc54fe5adec1c97bd7c38f0e43c1b8059f | L5510.B0.e0.p0.d1-6 |
| P0618 | 1411 | dd2dc1774d8dd6fb44f9340b2f36c309d9a37f9691803de1d97ddcdaef395370 | L5513.B0.e0.p0.d1-5 |
| P0619 | 134 | b77afac1b96f16031765de898d84184ec013f25f489f5cd16f01e9ffe44f86d2 | L5513.B0.e1.p1.d3-6 |
| P0620 | 37 | 5a51ddad769d177ae67eabbf2aa0bc275c75766ce9392254f438b45f21188052 | L5516.B0.e0.p0.d1-1; L5718.B0.e0.p0.d1-1 |
| P0621 | 114 | 092a001f122a46b7b7472dde469d7621dd85fff4150e33a9bec972b61cd61cdc | L5516.B0.e1.p1.d3-6; L5518.B0.e1.p1.d3-6; L5521.B0.e1.p1.d3-6 |
| P0622 | 318 | 556fecbfba8195a06b6638c64abee6717ea19addfebd087851a01e076bec6750 | L5518.B0.e0.p0.d1-9 |
| P0623 | 3320 | 24d33d1451d06ccbb4c3f60d25f547fa8ec0e9bd94898d0a4d631d79771a4b76 | L5521.B0.e0.p0.d1-40 |
| P0624 | 1095 | 7c6832ece1b5c3d6334fbf453a4300ad2945c54024e21b42e10462ac24baf7da | L5524.B0.s.p0.d1-51 |
| P0625 | 39 | dc635bd2e330b5441d73122ec3f442bb848d402b7f23809221d767eca9afe51a | L5527.B0.s.p0.d1-2 |
| P0626 | 634 | 1a531b8000055ffc45293cea4b31eb33c857d91ec735da65a8f1ff129c1a3ef2 | L5529.B0.s.p0.d1-23 |
| P0627 | 296 | 36a123f5d06b00f30cd2b010fad067d0955d0bf76297a1d8462f16108ffc279e | L5531.B0.s.p0.d1-8 |
| P0628 | 2540 | f2601e6b9ecada0dc574c28e1eeb814eecebc70d7aeeada928a8a3a83ba286f1 | L5534.B0.s.p0.d1-15 |
| P0629 | 327 | 413054a7e1c2491b1a08d4ce817092becb66dc988c62b7389792e4aec5a104d0 | L5534.B0.s.p1.d17-17 |
| P0630 | 324 | edeed705196c609ce0ce1ae67086fbe42d0ecb37eaddf3c7e64d1135dce8e603 | L5537.B0.s.p0.d1-4 |
| P0631 | 109 | 35e52e0a3e4ebde1d7b2ac33767f2ef066c5229bd971da861e1af9f1cb7f4f43 | L5537.B0.s.p1.d6-6 |
| P0632 | 50 | 7d059eb6001ef08c1144b18774f16fceb5b80e821c3b4a86ceccece4862f8aff | L5539.B0.s.p0.d1-1 |
| P0633 | 210 | 78845558ac60ae7479bd8c3c0cde0b4506cdfb72ff33fd63718dda8ca5c9cc44 | L5539.B0.s.p1.d3-4 |
| P0634 | 325 | b3f34400d8b8bd2039682482d930286feabc6a6f40d4e3379e00c25ebf3e29a4 | L5539.B0.s.p2.d6-7 |
| P0635 | 44 | 53bc8676e0e60b10403efc22cc35d7c72148026210fa28a1959e262da377b803 | L5561.B0.s.p0.d1-1; L5564.B0.s.p0.d1-1 |
| P0636 | 64 | 360c6111e670ff9f2cd252e0dd65892ca0a5fad0799367ef99b5a92b52b27783 | L5561.B0.s.p1.d3-4 |
| P0637 | 217 | dee60e6fef9c5254efa5ea548f65c98371bfc79aa3267778e58b76f42d2cb0a6 | L5561.B0.s.p2.d6-8 |
| P0638 | 121 | 38f711a039c42a7eeba7cbb177f05b3618661b95fc924cf39a49fa347e19ed94 | L5561.B0.s.p3.d10-10 |
| P0639 | 77 | 67be7de80c6564d0666b45a5591351d574df2fb6e4b115ca01c424a3a5717087 | L5561.B0.s.p4.d12-12 |
| P0640 | 14 | c514ce957070a3801aff93bb668cee3530fc9e2e97a81bd6d5e87bcba3d9c2d3 | L5561.B0.s.p6.d16-16 |
| P0641 | 60 | b5b93dd7ba353c65fb60fc506de0ccd79509b9450c29952f5162d89c4b07de8c | L5561.B0.s.p7.d18-18 |
| P0642 | 147 | 9262d4e8ec8af1100e232f4691c0c453b7b17c3a51e03519175bf089b5fca9f9 | L5561.B0.s.p8.d20-21 |
| P0643 | 50 | b46e782090d564ae6cd6d2249880bc21b04b1371be19a461d1de97b74e4b8b8b | L5561.B0.s.p9.d23-23 |
| P0644 | 50 | 45127e9395a0591bad30e0bb4299c74b6178f286a846c430ec0b6d95891b2238 | L5561.B0.s.p10.d25-25 |
| P0645 | 115 | ac1b686bdf603f06818f8681de155702113ab96824c21656ea9d96cb0e161d13 | L5561.B0.s.p11.d27-27 |
| P0646 | 168 | bcc30b96936103813f3fc7928f1664cfa561c5aae14b60729bba8a0e116f3e23 | L5561.B0.s.p12.d29-34 |
| P0647 | 99 | 22dcb1985c2a1e3d7b60eb1c420a0e2d28d08fe0aa8d9e7653f6259b29406e2a | L5561.B0.s.p13.d36-36 |
| P0648 | 47 | 1fc2ef28fede9aeabf8c0720ac8a80c91b7391a95632167a46d4003eba68ac26 | L5564.B0.s.p1.d3-3 |
| P0649 | 369 | 77d13bbd8345a85a305804d5c372229c4ede36c8bf7ee0c9a324f5fe4593d834 | L5564.B0.s.p2.d5-5 |
| P0650 | 40 | 084ad2ffa1d2a31eb7891e6901992394b8a0a4f0c2c62818056332b2de55d824 | L5564.B0.s.p3.d7-7 |
| P0651 | 106 | 0d61c1ef6672f19a2919b62de6aeb6e7be614e33a545a0e0c74fd9505086cbf2 | L5564.B0.s.p4.d9-9 |
| P0652 | 47 | b547773f096c49839f63b5ee6ebba04d5ab6de8502c9474f47b3cb50cdcbfa65 | L5564.B0.s.p5.d11-11 |
| P0653 | 30 | 102f2c7fb2944827ebd1a9b7907e9a9bf51b79ae7cc585d768999b74495aea89 | L5564.B0.s.p6.d13-13 |
| P0654 | 39 | ffc09d140942849bd6ee88e9400e47f8f5ad4cb54162efb961e88d1c6eb6a0a6 | L5564.B0.s.p7.d15-15 |
| P0655 | 296 | 779d1ac6234f896af556ae749027973a15b03e76fbf4ed974c1a8cc215fe127a | L5564.B0.s.p8.d17-17 |
| P0656 | 40 | a1cae69574fff3e4c4ac6a3ade2c41f0efb93ab22cbb392e9f5d49cff35e923b | L5564.B0.s.p9.d19-19 |
| P0657 | 58 | 5860302a49aebff8396ca97ce99bd885e64e7238ab75185b01b75b90f6eff9f6 | L5564.B0.s.p10.d21-21 |
| P0658 | 41 | c543c7e7060e693ea666be80ad873287df1f3eccb29370a8d6758401bcb168a8 | L5564.B0.s.p11.d23-23 |
| P0659 | 54 | 90e38799005ca254e64ec9b963f80d75112e8a8b13e4e86edb511e6de4ffacc3 | L5564.B0.s.p12.d25-25 |
| P0660 | 48 | 8bf6476aafbb03ec5eb8cab7fc6769c30dce8214f446500f601cc405f291cc51 | L5564.B0.s.p13.d27-27 |
| P0661 | 56 | a0a04511d380921263ff8f9f573b3198750d1429eb12b25b522a71e21aa76d6e | L5564.B0.s.p14.d29-29 |
| P0662 | 41 | 01feda12fc718f47932aee3e11b40615219ceba0c9dbf3f11c8c501bb42005ff | L5564.B0.s.p15.d31-31 |
| P0663 | 37 | 84bc9a2a78b42dc99ae42c4a85c6fa31392cbc5fa08cab5aaa8a07f24ad2f719 | L5564.B0.s.p16.d33-33 |
| P0664 | 39 | 5966d7c20b7834e27904a202303e3358b56e43d368102dd08c6edd993cb25e81 | L5564.B0.s.p17.d35-35 |
| P0665 | 192 | 587c47feab179ff5a62ace82b80480e6140e4e3dca2b9d14923d563057ab1087 | L5564.B0.s.p18.d37-42 |
| P0666 | 42 | 1f607912266a04ceab287e165b32a22954d79d1039fb6e9f86b8b11401955b1b | L5564.B0.s.p19.d44-44 |
| P0667 | 59 | eb62fae477c2cf386f79f77c656dc98884f69e4da0223f197c2a962dd6dfc90a | L5567.B0.s.p0.d1-1 |
| P0668 | 96 | d13f6eba5f5c55c4f9064841c4e8d1d580c2354826e20697c055e3df4d3eb54c | L5567.B0.s.p1.d3-4 |
| P0669 | 204 | 79edb72927ce89e09bcb759a43f6a4ceb74de42172cc212cbd94fa024c8aaade | L5567.B0.s.p2.d6-6 |
| P0670 | 116 | fc5d8a5fe8ee3a7b8831b29b9f8272c5fab43a979bb0f8c5e31222eb935e92b7 | L5567.B0.s.p3.d8-9 |
| P0671 | 75 | fb09a9d6c8b60ecb5c8d88cff291bf1f642e047eb86b334a26d70a913d26a23f | L5567.B0.s.p4.d11-11 |
| P0672 | 96 | c2211e4a78a17506fd8008631d0693a139a63305943039292697610d351cbb1a | L5567.B0.s.p5.d13-14 |
| P0673 | 31 | 0eb7cc93a9f24fbb185f2bd8dbd26d4a5521f1117032e200db6a2c89a74deee3 | L5567.B0.s.p6.d16-16 |
| P0674 | 88 | 2a2beed6cb8a4cfd9f3285b0a821f285dfb3f85fd3e86acf3541ae05f32f108c | L5567.B0.s.p7.d18-19 |
| P0675 | 162 | eb5f616dd544edc619d97e8f9d4efe41deddee2a3c2b50a10edc31842997d66e | L5567.B0.s.p8.d21-21 |
| P0676 | 88 | 00efeb829f52955cce38219b81722b3178913b031272fb15a4c4ae0854fccc11 | L5567.B0.s.p9.d23-24 |
| P0677 | 52 | a2b231db061ca152de7c3bc176f2b846f72101fdf5b828056a4bac320adb54e6 | L5567.B0.s.p10.d26-26 |
| P0678 | 90 | 120271d3ddcd74e2d41975b7aac1813174c959ce480e4e1cff699939d005965a | L5567.B0.s.p11.d28-29 |
| P0679 | 61 | b0b373eafc95b4cca030e069078452137ae8bd30cee85f3c60c235391b27e22a | L5567.B0.s.p12.d31-31 |
| P0680 | 96 | 16dadfd4a2c6ad25b745877cbf97cc11d383b33b86fa022343ade58cb89e8ad2 | L5567.B0.s.p13.d33-34 |
| P0681 | 20 | ebee72dd8343be4d3995e07bf029b30828a3bde6a76399a359491a7422306144 | L5567.B0.s.p14.d36-36 |
| P0682 | 90 | 8ad7eafa17671314d8714c473eff03916edd934b53f1ae206e1130f2b76bc650 | L5567.B0.s.p15.d38-39 |
| P0683 | 20 | 6985f53dc75a5f68b72c7e7c078c576ef8841e978e9c8250e8f7ea05bafb6af9 | L5567.B0.s.p16.d41-41 |
| P0684 | 106 | f6e5a868adec0a34e9f5a1979b47129c68928300aad1cf342e8012f8ebf0e8d3 | L5567.B0.s.p17.d43-44 |
| P0685 | 185 | a44c78025452be8bbfb14c566aaad838869400c9bfb24f1f16a29bfadfebf44b | L5567.B0.s.p18.d46-46 |
| P0686 | 2206 | c6b9f18b947827ba909733412088cb2df75374d68a2579eda2a563e7c12cc4de | L5570.B0.s.p0.d1-27 |
| P0687 | 17 | 47ee399a21726f9675049beb318a47895ca9214b4e1868a7e6fd26615886400e | L5570.B0.s.p1.d29-29 |
| P0688 | 35 | cd7196ba65a8b7dc6cb90a81c50b5fffb37670c7386eca6cad0e2250106ccf1f | L5570.B0.s.p2.d31-31 |
| P0689 | 31 | 7b1f9bb5b39e4ed078b4f8309702f35d61507d6c9a05e9bd7509679b31d5274e | L5570.B0.s.p3.d33-33 |
| P0690 | 33 | 83ee1aa546b5c13205cf781018d2e80a73421dd67be0b0ab6bc53f04d849c44b | L5570.B0.s.p4.d35-35 |
| P0691 | 29 | b02bd283c00762195d6d5449dd3189554bbe95e386bd0cd7f4c47b142b16d5ff | L5570.B0.s.p5.d37-37 |
| P0692 | 31 | c7b6ef68bd693a870753b32d94cd5e9943d86e05830b7a603435e6973e55032d | L5570.B0.s.p6.d39-39 |
| P0693 | 3 | 3cc919488cef0f6e342fdbcdc200a7ed53b5ba6fb61d1061a3bf1672f30bdf05 | L5570.B0.s.p7.d41-41 |
| P0694 | 20 | 6c48a146844d8bd80cd163b7c5e70cb4f4739f765d2c34db6900dba02f0c43d9 | L5570.B0.s.p8.d43-43 |
| P0695 | 2325 | ddc4eb010b73a51659d953851c5e231d1e9e62b81e4a5b1c87423a7bc92a2b27 | L5570.B0.s.p9.d45-78 |
| P0696 | 17 | 691ef12a8960144c8e6ba4e966f092150407d59e364938b64f582c63f70fc479 | L5593.B0.s.p0.d1-1 |
| P0697 | 218 | 2f39bd936d6d3b6ff6c621f84e5253d154d353a779444c59add1f9e46415006f | L5593.B0.s.p1.d3-4 |
| P0698 | 169 | d1397878caeac5f714688b20c9697b30e7e693f4f10d3858b77c54b18f92077c | L5593.B0.s.p2.d6-7 |
| P0699 | 208 | b8591b0a0f546b7dcbf0b55215d6f7de9b34762ea59e9e527973a5b30c42acb6 | L5593.B0.s.p3.d9-10 |
| P0700 | 416 | bf320df3f147194c456e3343f34e59649a860e1541767bd851a21a90623df804 | L5593.B0.s.p4.d12-13 |
| P0701 | 155 | f333b5c6b91b32507bf734a2a32d980c6cf85e20567c121599acfab6f79a85a7 | L5593.B0.s.p5.d15-16 |
| P0702 | 40 | ac9673eb565c12ce7ceb3ea74cc4bc17d715f8361f40b7d37bd803195f0288fc | L5594.B0.s.p0.d1-1 |
| P0703 | 20 | 297be448b5cf9bbc7b71fe07a2512d8a32c05d6586175884abe4373b667b743c | L5594.B0.s.p1.d3-3 |
| P0704 | 194 | 82c910e278c0a5107bb2e19be1f30deb2c24c4fa425233dc65828fb257520424 | L5594.B0.s.p2.d5-5 |
| P0705 | 30 | 6a4eaa41430dcef581c2e865c3c3a18a15edfefbfc72fca4510318cb2c831614 | L5594.B0.s.p3.d7-7 |
| P0706 | 52 | bee7f846876a0ae5dc9dda27f8eb6ccc8001ec9a82025ae4a9399c5891659195 | L5594.B0.s.p4.d9-9 |
| P0707 | 170 | 381d4f1c74e376fa8d8bb8e7de02a95b82e5c37808e9abcb6d879cb96103b3d6 | L5594.B0.s.p5.d11-13 |
| P0708 | 173 | 04d89542681da665166ec81c5dfe9fbc8ba4024713b1f5db9853d75d66985f21 | L5594.B0.s.p6.d15-15 |
| P0709 | 13 | 48ea9e9123cf75fd74ad6a73caefe8ede1c0daadc0239f64e8abcb79a523965f | L5594.B0.s.p7.d17-17 |
| P0710 | 52 | 3cfd3ca5518bf6743877f96ca3116fbb277de6c2bf78a86a9f9c1f9c5d8a940e | L5594.B0.s.p8.d19-19 |
| P0711 | 260 | 1c1203dc47c13e8cdb555c366e68535f468543f66c68ae6661c186d62eca3a2f | L5594.B0.s.p9.d21-23 |
| P0712 | 16 | a3b6a73da1c6d14875e0dab9b0a76fb34bc28dc02a5ddab8eafb0bbee99cdf4c | L5594.B0.s.p10.d25-25 |
| P0713 | 363 | 468e1b4ae0a3de0e866be0f15415b7712afa3fae98ec93f2015beb4d48d3aea7 | L5594.B0.s.p11.d27-27 |
| P0714 | 226 | 388e300bc7c93725c16c35e0228e1844f174306e8f906b8ab1ba769b57e53667 | L5598.B0.s.p0.d1-3 |
| P0715 | 1115 | da64f20e9a68b873ea93aaa2341f08923235fbea74903fe528a6a37ce169bb4d | L5599.B0.s.p0.d1-6 |
| P0716 | 1508 | 9af15e8544988c4ac1f47a92cef2891c0e4294e2b1bffb6b6ca9ed03abfb614d | L5602.B0.s.p0.d1-55 |
| P0717 | 180 | cfc73a0c7024f03a0ac4c490d09a9b50f54a68452512e4d855887451e8c05e5d | L5604.B0.s.p0.d1-4 |
| P0718 | 50 | b12b3bb8acc31a8d2baef5c25651289f36d5c27cc1dea556147affc1a6d17506 | L5604.B0.s.p1.d6-7 |
| P0719 | 104 | 91225245fa79a022ecf9f1873523f267c7de9293a7ce02bb10b9d0a8870e7728 | L5604.B0.s.p2.d9-11 |
| P0720 | 40 | f76d876df88bbcc6e8f4472f25292b47746df372bf8d350f19c05694122826cb | L5604.B0.s.p3.d13-13 |
| P0721 | 246 | 450c86b08e762626536da6f6de5b09f1c6a740be434c78bcf522d77d15410821 | L5604.B0.s.p4.d15-20 |
| P0722 | 243 | 63ac9e9fac7c2772e49e6aad65d9ce22751bbf088aa6fbc0feaefba84af6e738 | L5604.B0.s.p5.d22-27 |
| P0723 | 153 | d76e88c9f53ae118b46a032ee43824d46c09b68447bc42c638106bb00ffb28a0 | L5604.B0.s.p6.d29-33 |
| P0724 | 482 | 73170de75ca1f90c62ef6816a5d96642bdc1bbe431682059659ccdbbb5ed8d74 | L5604.B0.s.p7.d35-49 |
| P0725 | 139 | 031c3cb8c26c2796d2be1a7b0d9e01d7ef76c5bec7cba889d2f427653222e742 | L5604.B0.s.p8.d51-54 |
| P0726 | 248 | 16f281c447481291ff9dcafa31b86e88f378ac0df4e81caf7884e4c698f32f09 | L5604.B0.s.p9.d56-62 |
| P0727 | 248 | 5c2af01120699fab76923d2184355352ddc5323eab5a6ca63a0980965c420051 | L5604.B0.s.p10.d64-70 |
| P0728 | 654 | 9653741964e9b149d5d34ce255c4ab898055bf3093235e22ac3272f12dc51836 | L5604.B0.s.p11.d72-88 |
| P0729 | 58 | dd9ed2685d36886a419c827ff0df9de28db41ce967ded7459e633ecff4d52802 | L5604.B0.s.p12.d90-92 |
| P0730 | 45 | f51f753e692af3f5f9edfc3cdc36a7986585ff50c5f99080dccea68b7e11a522 | L5604.B0.s.p13.d94-96 |
| P0731 | 420 | e32d137e7af2922afc269a30fa0ed3fc06946ec9d89e53c9c5bf70d3dd1163a2 | L5604.B0.s.p14.d98-107 |
| P0732 | 243 | 4831db11a9360627a0bf84cbac42752b9e6f4964ae3e872e6a3de943417dbceb | L5604.B0.s.p15.d109-112 |
| P0733 | 53 | cbd326ca1be00c025ce8e24b395994b13dcabbef0357167204138e43aa9a5c0a | L5604.B0.s.p16.d114-116 |
| P0734 | 77 | 03832a79556a3a9bcee9f0fcf84c9d145cd357735e34cdd1acca341103a88705 | L5604.B0.s.p17.d118-122 |
| P0735 | 40 | 65514666431986a52fda05ee3cd93994a5debb39b5e40bab4624b34b6c6f98d3 | L5604.B0.s.p18.d124-126 |
| P0736 | 690 | 75e6678dd6c128831f51e2c619e18998c10bd7658c46999e2c9acd17af2b51d6 | L5607.B0.s.p0.d1-15 |
| P0737 | 73 | 2d577b4997cb95f6f88ee07e65c81fe4ca5f9e4e2a0b29d0cc6739fd08313bac | L5607.B0.s.p1.d17-17; L6553.B0.s.p1.d4-4 |
| P0738 | 80 | aecc47f441d5e9ef380c8ce976c1cfd09311987ce371802e3800e88f7fba1af7 | L5607.B0.s.p2.d19-19; L6553.B0.s.p2.d6-6 |
| P0739 | 47 | 9e03b0febad0bc15d80023a774db7f0105acc81cefc19a67bd48b0f08ef97143 | L5607.B0.s.p3.d21-21; L6553.B0.s.p3.d8-8 |
| P0740 | 222 | 1d2a29b2dbc14d8d3dcf843a7e045702c5cf4e8bce3e6b3f8825311ffdb69ca7 | L5607.B0.s.p4.d23-25; L6553.B0.s.p4.d10-12 |
| P0741 | 84 | 65e89c61fc0d4ecad3ab7c710f6d755ad82b462242b16d59ccb72224287f6ab7 | L5607.B0.s.p5.d27-27; L6553.B0.s.p5.d14-14 |
| P0742 | 52 | a49e0b208e41f3aac64736349c29b5b478f287a181f44328b50fa370697de967 | L5607.B0.s.p6.d29-29; L6553.B0.s.p6.d16-16 |
| P0743 | 200 | fd250a176b38586ff44a0877e3ba7b48a3ea87cd5feaf66f7df6cd0e94ebaa03 | L5607.B0.s.p7.d31-35; L6553.B0.s.p7.d18-22 |
| P0744 | 198 | ebdaa109b7a8d58f805e3ea8c67377fbe36ad27b7bb82ae25dbfcb811e25e7d7 | L5607.B0.s.p8.d37-39; L6553.B0.s.p8.d24-26 |
| P0745 | 81 | 6c7bdded4d7a242bd13e286460ab95136eb30979d0990368d7e1defac9c74fea | L5607.B0.s.p9.d41-41; L6553.B0.s.p9.d28-28 |
| P0746 | 24 | 565e5b729ce88c9fb4bf817b8a002a7e783fc2e6696221aa7df9b42eced660ad | L5607.B0.s.p10.d43-44; L6553.B0.s.p10.d30-31 |
| P0747 | 128 | 5d32290475a291db933ff00ba1964eb639d3dabc310851931f99b86b25458cff | L5607.B0.s.p11.d46-48; L6553.B0.s.p11.d33-35 |
| P0748 | 57 | 340dd50bf85cef436669eaca1947fb93c225bdbbdd6f32ac0884287f6195cac9 | L5607.B0.s.p12.d50-52; L6553.B0.s.p12.d37-39 |
| P0749 | 159 | 4e820d53b99850e642713b53ab382aad9dc2d5efdadcebb3a1bba587d7d4c45c | L5607.B0.s.p13.d54-55; L6553.B0.s.p13.d41-42 |
| P0750 | 24 | 8ca6595d57df9ed99bf75d5c427475cee396b31eeebce0973db2283319bc5163 | L5607.B0.s.p15.d59-59; L6553.B0.s.p15.d46-46 |
| P0751 | 183 | bf28e4f229a9540205e9c7ca64b015e77236dc5bc88bc16e5015e00fe2fefbc8 | L5607.B0.s.p16.d61-64 |
| P0752 | 132 | 162c649be96cd9e744e662b964b4bfe210d15ecea9537be168e8c072cc14227b | L5607.B0.s.p17.d66-67; L6553.B0.s.p18.d54-55 |
| P0753 | 77 | 5139bfe6cae78fee18e1cd924e5fac85a90a362dbfdeee5c500dc8c3d1b1a936 | L5607.B0.s.p18.d69-70; L6553.B0.s.p19.d57-58 |
| P0754 | 28 | 6454fafb584e9840e847ef143a743b0d998036057fafd317cb89c7e643be4089 | L5607.B0.s.p19.d72-72; L6553.B0.s.p20.d60-60 |
| P0755 | 321 | c6992064b67a9c6ffc1e6e171c1befde572edcba57ba3aa63d02d5337d9beea1 | L5607.B0.s.p20.d74-84; L6553.B0.s.p21.d62-72 |
| P0756 | 26 | da7f914c8d3b80013db227ff1b49afe73e4500675acdabd190c22da9c91fe505 | L5607.B0.s.p21.d86-86; L6553.B0.s.p22.d74-74 |
| P0757 | 242 | 3d660cee14d39e415272067546b659891733246d62bf030a3ddf899ca18dca35 | L5607.B0.s.p22.d88-95; L6553.B0.s.p23.d76-83 |
| P0758 | 36 | e242ec2207894bbac60c1a019a2cbe7593e7e1f89d3e31a2300550b934e2d013 | L5607.B0.s.p23.d97-97; L6553.B0.s.p24.d85-85 |
| P0759 | 200 | a64e1e8054c12793fb2ea3037bbb98a962778c212684c0a03f77bcbae567ac44 | L5607.B0.s.p24.d99-104; L6553.B0.s.p25.d87-92 |
| P0760 | 20 | 3539b08b5c2cdef0514df23aaa0fa868dd70e7607c4eb555b944776e0abdf2ce | L5607.B0.s.p25.d106-106; L6553.B0.s.p26.d94-94 |
| P0761 | 223 | 7aec181aadcb2b9bbe6e01352ac8661f00cc132804b84584bd7f99884d880a82 | L5607.B0.s.p26.d108-115; L6553.B0.s.p27.d96-103 |
| P0762 | 26 | 2c518bffcbbdd962765add3f43b652fcf8daa7166becf940063f797963ba500d | L5607.B0.s.p27.d117-117; L6553.B0.s.p28.d105-105 |
| P0763 | 120 | 78d1ade00784830d325421fc52ee8cc45a681f40a828c8c30c73ec96dde295fa | L5607.B0.s.p28.d119-123; L6553.B0.s.p29.d107-111 |
| P0764 | 31 | c894f6ffab4e9d3821a82fa761ff1d7bfecab563e16c21f5dd280b17993acada | L5607.B0.s.p29.d125-125; L6553.B0.s.p30.d113-113 |
| P0765 | 193 | 1d56c9cc2377fe291dea526a416c3b0afefb55e7a1af9903c504aa3aea4ec4ca | L5607.B0.s.p30.d127-131; L6553.B0.s.p31.d115-119 |
| P0766 | 26 | a9e1d070b447a4fc19e8262c1875bdcac54565915969f319d1b5a3214b28c7e9 | L5607.B0.s.p31.d133-133; L6553.B0.s.p32.d121-121 |
| P0767 | 180 | d45ee7d57729231e1153eb054ca876901ee37c4211ef63488c2106e19d9502c2 | L5607.B0.s.p32.d135-140; L6553.B0.s.p33.d123-128 |
| P0768 | 23 | d5271348d63dfe05917562ff04e96f25c58365bc8043aeb97311a106a00686e0 | L5607.B0.s.p33.d142-142; L6553.B0.s.p34.d130-130 |
| P0769 | 169 | e229d8115d2f667066bb6d658224bc1ee0f9f03122cd891175a692445d9f7480 | L5607.B0.s.p34.d144-148; L6553.B0.s.p35.d132-136 |
| P0770 | 28 | eff1cb803ce17ced6ac7c04e7319501dabc41103f73f03dc4dd9bb52263e3340 | L5607.B0.s.p35.d150-150; L6553.B0.s.p36.d138-138 |
| P0771 | 48 | 0430a035eb3e24b477b874cb1c17e694f63e6d49fd7aa166e0e25b09ec5c745c | L5607.B0.s.p36.d152-153; L6553.B0.s.p37.d140-141 |
| P0772 | 23 | 06165e209b1e3f6b88c1dd7055641962195f13aafc4f7cc29aba052e038ad762 | L5607.B0.s.p37.d155-155; L6553.B0.s.p38.d143-143 |
| P0773 | 150 | d027ea0d4aadd7df651221b1944f7ecddc90f80974dd6e42693ed787f679d9a5 | L5607.B0.s.p38.d157-162; L6553.B0.s.p39.d145-150 |
| P0774 | 33 | 95780c5e37e73f157e42298363a1fb361e56f675fde634c00ad0a5d3c804f5c3 | L5607.B0.s.p39.d164-164; L6553.B0.s.p40.d152-152 |
| P0775 | 162 | 0500f54f905fa5f07de2a7c1bfddad84985587ba9b88e0bb2fcd98e572025755 | L5607.B0.s.p40.d166-171; L6553.B0.s.p41.d154-159 |
| P0776 | 159 | 09fecacb23c2db5d078c7c64f3b9bd4b547764ce14372168529d57e7ab305e3a | L5611.B0.s.p0.d1-7 |
| P0777 | 96 | fd458ec6f86573cf96b3d07b1bed557017df0692e7047240f672d61640473be3 | L5611.B0.s.p1.d9-12 |
| P0778 | 17 | b9a647210d97884823947f5ca0374b5ae4b32bea603f8dfdde1ba54d5445f26c | L5611.B0.s.p2.d14-14 |
| P0779 | 811 | 3ac3119736d044dfb6eea59fd358c8181b173cbd7d300b2fe64d650d18402dca | L5611.B0.s.p3.d16-33 |
| P0780 | 101 | 88d7ffdf9c70a3ce3e1b5a5542a8e8794700ac2a4e915f1623bb5d7b56d7c7fa | L5611.B0.s.p4.d35-36 |
| P0781 | 1040 | cd083dd564f1fcf316dee52a41a46f3e845f12c9e3414b7db7b64209cc6cd31f | L5611.B0.s.p5.d38-64 |
| P0782 | 2648 | cbfc7bdfe665160c0378089ddd2c29b17529b336a9b39a9a722f57e55e5ae916 | L5612.B0.s.p0.d1-41 |
| P0783 | 703 | dcc1acba433a80865a0f6790c0db78a1c4d40708d278a23853dc9023552df15b | L5615.B0.s.p0.d1-17 |
| P0784 | 23 | 0374daaf07e33a4d7776d0038fb9d9a785f437d916c224bb8a7bc597097b9d14 | L5617.B0.s.p0.d1-1 |
| P0785 | 587 | d012da80ce40c59c6f359d72f26ec30e3c0d620a55ed4b72a8a708a6f62b6935 | L5620.B0.s.p0.d1-14 |
| P0786 | 164 | 5062f755ae12e444f6e9b4df8ade25b9118c3f0fbd131347c4c7b4bca8bfb501 | L5620.B0.s.p1.d16-18 |
| P0787 | 347 | bfa7299ed5adb1d071ad03feee8888e18705b2e8cdde595ffd5037673d7dc61d | L5620.B0.s.p2.d20-28 |
| P0788 | 257 | 518c378162f1e1d89afde22a8dc82790ccad9f860d353d177ea5cc45b9e7bbef | L5620.B0.s.p3.d30-37 |
| P0789 | 159 | 572ab65d55a162af8d37dda7545829f81744962690ecee9746354d0ee14312fd | L5620.B0.s.p4.d39-43 |
| P0790 | 359 | 1d1a9f6390d9b989afca582524ec60196712b4cc36650d2eaafc4f89d5b7b5cb | L5620.B0.s.p5.d45-53 |
| P0791 | 340 | 56d58de4d2f737961364ca0da3659ec46d3e14ed9490c2c1d3b235f9614c1c42 | L5620.B0.s.p6.d55-62 |
| P0792 | 298 | e8da2766bb9078c1674dfcb9c2858335509882d32ffcec18a292434e74f676b1 | L5620.B0.s.p7.d64-71 |
| P0793 | 298 | 35ea25366940aaf32bfd4171135f2bc8e67e6655cc6d59087c847356c094dd9e | L5620.B0.s.p8.d73-80 |
| P0794 | 310 | e56587e7ba3542c958ae1813806e36856a8d82c17b93be912ab448bc6606f3db | L5620.B0.s.p9.d82-87 |
| P0795 | 702 | 95234c522b4825848664e842934e1d3ca9dcecf104b4311738b340dea1ddc8b7 | L5620.B0.s.p10.d89-109 |
| P0796 | 312 | 6112b98b6f31b198407c5200e83da6384497310358679502140425fa5240646c | L5620.B0.s.p11.d111-116 |
| P0797 | 241 | 60df8793d5b27bc5292d6a94209b82787166d3096eb0f5fb31450722c90b9cc5 | L5620.B0.s.p12.d118-124 |
| P0798 | 150 | 33a6cdd21163f683c28ab77ba72d9fcb868d9fde89d78b183574d4af2fc93ba8 | L5623.B0.s.p0.d1-4 |
| P0799 | 77 | fe81f4aa2a28220db10de68ecd21eaa4dd6d858f92a85d18bb249535100592b7 | L5623.B0.s.p1.d6-7 |
| P0800 | 1698 | a137ff05da04bd6ce71e782cc0293228c0ee52e6944e880c46f80f4ba567f352 | L5623.B0.s.p2.d9-56 |
| P0801 | 728 | 9d0eb4a0f3514c4046e745335fc6c51d0810cbb391d2772581572466d03eddac | L5623.B0.s.p3.d58-80 |
| P0802 | 1870 | 3c62b7ad3c1d426652d149ea60dd4cb70ac9ac365a8eb8b607ec66dc95ea789d | L5626.B0.s.p0.d1-30 |
| P0803 | 104 | e5a2dec0a6298c3ffd8c24c4d07bb7dc3c8f88e3ee1e7ef5a428da57227f75bc | L5630.B0.s.p0.d1-1 |
| P0804 | 78 | db1c8cfbdc76cb1a712ce8e368d095acdc6c86411258b7548b2a18f700da8e1d | L5631.B0.s.p0.d1-9 |
| P0805 | 136 | 67a41475057c875aa0414c4e2a211b9eeaf632eb9c5e78150ad744b741cf26bd | L5633.B0.s.p0.d1-6 |
| P0806 | 8 | 8ffa8b8f834c71c018e10676efe2505d59734940dfb118306c3e6fefd2ca62a6 | L5633.B0.s.p1.d8-8 |
| P0807 | 138 | 8ecea52f9b0195d0c42c87706a1bb530961aff376e4e978241958183ee586d8b | L5633.B0.s.p2.d10-12 |
| P0808 | 329 | 81b5ec13a27149fe4b903b1d3da9be9f98c3778b15d86ab9a7629a083002eb91 | L5633.B0.s.p3.d14-17; L6547.B0.s.p6.d21-24 |
| P0809 | 752 | 2cddc8e04ba0229a5f6c099199be9936e75d3d17717ecf747bc765ae6c48c085 | L5633.B0.s.p4.d19-25; L6547.B0.s.p7.d26-32 |
| P0810 | 10 | 98304b8243bd9f261f5ba797a519b567d87e41b428d72cf53d374f83e67950e1 | L5633.B0.s.p5.d27-27 |
| P0811 | 54 | f8a0b50fff531eee7d7498481129b67b9b32e78d47260a38331a158f94982e90 | L5633.B0.s.p6.d29-32; L6547.B0.s.p16.d59-62 |
| P0812 | 16 | 6383b31f064c081c15e4012c6721d7aa818f1b5353699fcc8eaae96e3e45b3c6 | L5633.B0.s.p7.d34-34 |
| P0813 | 136 | bb52e758ac2b20d22ef6bce8e0a77bdbd83d5cfb9996feb98c1c974ab1226970 | L5633.B0.s.p8.d36-42 |
| P0814 | 469 | dd725ec523d536b188d15836bd2f9779ff06ec4358850a0db9a8a170047b5384 | L5636.B0.s.p0.d1-4 |
| P0815 | 192 | 98cd0a70e4e0c6995757e2fca25e9db59bf7f133d15f60465f068ec6f51637a6 | L5636.B0.s.p1.d6-7 |
| P0816 | 399 | 800407116eedc79b3e00fce8cda6437884b9f8086b441cdf04b4d80a2c3cfe7d | L5636.B0.s.p2.d9-13 |
| P0817 | 195 | 1a44bfe7adab1366df02e5b9b996066eafbb98ad42a163a4e741f5f062d14bdb | L5636.B0.s.p3.d15-16 |
| P0818 | 1156 | d11baf64a576c1ae40f9c05f4cc36c56c2b9d1808acf603e4ca1f29588c5d80a | L5636.B0.s.p5.d20-50 |
| P0819 | 26 | c35233cb348822c31a731210259ccd7fc0de1f920df7f4d12fd2238dc256bdac | L5661.B0.s.p0.d1-1 |
| P0820 | 138 | f0731bcf2e03bcb9973543a397b16829e7591c4308cf86ebb594cb0ca8de303a | L5661.B0.s.p1.d3-3 |
| P0821 | 201 | 1f1b7a3443188f8cdcb5a4edbcf3e2bb142f6c3de649bc57d750a45c237ce8d1 | L5661.B0.s.p2.d5-8 |
| P0822 | 100 | 643a487359a27a6256014103424cbc22353655467ea247532c883812edb04a62 | L5661.B0.s.p3.d10-10 |
| P0823 | 1218 | d51a900fa5c67b6e382d0dff1ad5d339870d5c64b3287b45f3441e4795cb26b7 | L5665.B0.s.p0.d1-27 |
| P0824 | 54 | bb8b3538f134164b06c0b3abc5485556f91a90867df7ca88d4291758c62f9431 | L5665.B0.s.p1.d29-31 |
| P0825 | 713 | 2148c87d21c8601200b43c5ea4df0b5cbcc25eac20b85b6332cb990cfbcf9022 | L5673.B0.s.p0.d1-22 |
| P0826 | 387 | b6fefd49620c48269fc1c284954b7ff099673036e0391e66b2f1280a01f5fbf4 | L5681.B0.s.p0.d1-5 |
| P0827 | 1489 | 3ec935c7e040084101f05bc4e43a9e616055460433530f73a5441ffdbfa333b9 | L5684.B0.s.p0.d1-15 |
| P0828 | 84 | 1635fcd9e1dfcb827353835f96f64cfb00fc1828301e39437985d239cb09343c | L5688.B0.s.p0.d1-3 |
| P0829 | 221 | 8e814ed6bc043a2dfb8f19ce31eeb0126ed83ec440d29b9b1d6737e062306395 | L5689.B0.s.p0.d1-5 |
| P0830 | 3225 | 8927a40db11237d437aca9f53a7c5381711ab5a9638e97f8dc669b3d9df45f51 | L5693.B0.s.p0.d1-59 |
| P0831 | 552 | d599deb7bf4fc961bc9cf3391661ce3ead0e6f40c85d93022f01ed3d804e6438 | L5694.B0.s.p0.d1-2 |
| P0832 | 272 | c51bdb623e67612f44dfb75a90f855dee1d0f959f513ec107c4d1c0cd296a775 | L5697.B0.e0.p0.d1-8 |
| P0833 | 220 | b3e21c67f9d9c6fc1ccc2db57b9893e89d9000eaa263fa128cc3c6560cf6d148 | L5702.B0.e0.p0.d1-12 |
| P0834 | 29 | 28aa644376286d5eff4862f2c57436d1a5d9d04bf7d73f9a4993e49b56aeefe7 | L5706.B0.e0.p0.d1-1 |
| P0835 | 859 | e19cce8e2651874a6a605d8745fd92ddf600ce8a2be903a3a3f9dc0c9bd83dbc | L5707.B0.e0.p0.d1-6 |
| P0836 | 559 | 77cebb067846dc41e2cd97bd25d4d6e6de4dff6ccc30ec7deb2c8213ff1c62f2 | L5710.B0.e0.p0.d1-5 |
| P0837 | 46 | 30394d014053a4a2ed69e35fe06744f8e9553925873693e2026a44b0ff65c44a | L5710.B0.e0.p1.d7-7; L5713.B0.e0.p1.d7-7 |
| P0838 | 1740 | d8f2c66fa5ef21b80c7f6726349a40917bc52a3eb097f048248f08d094ac08dc | L5713.B0.e0.p0.d1-5 |
| P0839 | 197 | 34e1794b7b504301592e944f31237ea374b8fcd4e350ab95af53c75651924744 | L5716.B0.e0.p0.d1-7 |
| P0840 | 114 | f9cd59a78cd7d448208a01f045cb49eba13ea3876de8fb8b24541517dc88c6be | L5718.B0.e1.p1.d3-6; L5722.B0.e1.p1.d3-6; L5725.B0.e1.p1.d3-6; L5743.B0.e1.p1.d3-6; L5745.B0.e1.p1.d3-6; L5750.B0.e2.p1.d3-6; L5753.B0.e1.p1.d3-6 |
| P0841 | 194 | 6a743e90d139472f69122734d90bc2b9b368cf520e5f09c4eeae9804a1333207 | L5720.B0.s.p0.d1-1 |
| P0842 | 205 | 835cca339202418ffa83be4c3fcaf59a28a783fba991805fca1c25ee375c727d | L5722.B0.e0.p0.d1-5 |
| P0843 | 3087 | 4fb716a986d8225d9005b91921039ccdcac48439e0de3ea6d41e5b3fdf1f3c8e | L5725.B0.e0.p0.d1-42 |
| P0844 | 381 | 0042ee0f89c9f4a85105ac1c1591b067aca4c8ef15a4d4ac415ee085c9280818 | L5728.B0.s.p0.d1-1 |
| P0845 | 8 | a96d14933c29dfcc514f208c8a6c6cd573b652812c0f97783f3001289ba0dfdc | L5728.B0.s.p1.d3-3 |
| P0846 | 135 | 469704271a6ef2089efbccd18e35af6e44062e8023d7022368fc9950d410ea32 | L5731.B0.s.p0.d1-2 |
| P0847 | 139 | 843e1e818f710cd4f838f3e3b7398ce76b30c33549e3ed1717ba28df070d46fd | L5731.B0.s.p1.d4-6 |
| P0848 | 137 | b87da74302e006b52859ea29ea649e0eb9f5dbfeba8f591280a864706af264ec | L5731.B0.s.p2.d8-10 |
| P0849 | 122 | b6cff86c15690add83cde81cbd09b937291947732f363bfdc0a7d5486f5f8560 | L5731.B0.s.p3.d12-14 |
| P0850 | 131 | 0c124871419884a0dbeeb5bccd1f14a327768166109fe18a24b2bac06ab3bde9 | L5731.B0.s.p4.d16-18 |
| P0851 | 138 | 17a0b9b58330623225abf8fd69d6d0c010731d4383355c330563309a600241d3 | L5731.B0.s.p5.d20-22 |
| P0852 | 4 | bd64bb8efa5b3f27e6442f23600c3b516127ea17e23724b41e50da4c22883edd | L5731.B0.s.p6.d24-24 |
| P0853 | 588 | 74ae25748708aa0d6505cceea4f9c8a7d12eac48ea497ddade07158725e02689 | L5734.B0.s.p0.d1-3 |
| P0854 | 2125 | 49e1936e2b77ae56ab85f4be4dbd43a0af4a4ee9d591aad5898828ca2c6a6bf4 | L5737.B0.s.p0.d1-14 |
| P0855 | 1055 | 7568199116680753bf532afb6e2838dd7aa2fd9a415c0c0f1be8a03fd9920a86 | L5740.B0.s.p0.d1-18 |
| P0856 | 213 | b30c222f28ba7a6e5b5c8f6f8c734140e5869ad286598f939954e7eea5315972 | L5743.B0.e0.p0.d1-7 |
| P0857 | 29 | 6a96dcffa4cc4978a9a3cd6d1298f34df3ab263eefd30d90f55b3dd2c02cab5d | L5745.B0.e0.p0.d1-4 |
| P0858 | 734 | d6a0fe6a227a5cb06c480edf9889f5a9d8272c8849365c29fe3c88e943ffd531 | L5749.B0.s.p0.d1-15 |
| P0859 | 24 | a78d0c4cf1157f1bcb652ff0247caaaa31ef53ef4adc495c4392e5c4d2550bc3 | L5750.B0.e1.p0.d1-1 |
| P0860 | 338 | 3f8bb3b920e69c2c8b09b688eb604b800aecdfb643691ebddb0d4788d8057500 | L5753.B0.e0.p0.d1-10 |
| P0861 | 641 | 4884b12ae728cf5a0653fa7e1dd3034f3e7dd2b21de032ea019087eedbaff949 | L5755.B0.s.p0.d1-2 |
| P0862 | 1557 | 7a8f038c98db86aee29c2957de14d50c3401f8519e6d9e6ea7bd662daecf1b82 | L5758.B0.s.p0.d1-24 |
| P0863 | 7991 | 1b6fb9eecc71f304bdd430d65250a2b6ad697868e58fd3c9d286d65bbab4e4b1 | L5761.B0.s.p0.d1-66 |
| P0864 | 661 | 8ea397d0db94c7b244f4ffbff314debee009c894a1e09cf69624e81fe1ea9ad4 | L5764.B0.s.p0.d1-10 |
| P0865 | 2261 | be2987ff3d963fd928c5c69937466a10099d61c000925b97c2db61755bc8a8b4 | L5767.B0.s.p0.d1-15 |
| P0866 | 4434 | 81a1df64c517525a2abe19aed9b086ad8a6bdfeddf0df8672eabf8bc79e7ac1e | L5770.B0.s.p0.d1-17 |
| P0867 | 202 | cbd8c0692b4fdfda26a60d8cdcd6f57576afe6e70d086fc0c1b9db0cf18a91ce | L5773.B0.s.p0.d1-6 |
| P0868 | 3014 | 916af9becef8b97bd4c102946bede20ab380a5d36eaeacfc07b15eab8c873c4a | L5815.B0.s.p0.d1-136 |
| P0869 | 6550 | 5df82b693ee9ca17cd000bc8e593d600238d9db8e8b5d1a15868e082a859d967 | L5817.B0.s.p0.d1-312 |
| P0870 | 671 | 0b49ff30dd0a247d79fc843f033c4cfc85707a8b34e94e28e1b006e52cea552d | L5820.B0.s.p0.d1-12 |
| P0871 | 62 | f433e86e0c3ab64fddcde2bfbcdaaf819f2ebe512a71ec476d22e163d3b034f1 | L5822.B0.s.p0.d1-3 |
| P0872 | 2040 | c58e01bc195c8d05bccc83e19ae7bd86bed23db58c51f363e297b1b0118a0ff6 | L5824.B0.s.p0.d1-186 |
| P0873 | 2040 | e7c5f7ca42ef8b6547b14c01186cf8443e6f81908b1d4fd04a348cc5b630e4c6 | L5824.B0.s.p1.d188-373 |
| P0874 | 2040 | 60303b3948987070aa07d4830d1ca1a0ac794a52313c4619a887cd912b2429ec | L5824.B0.s.p2.d375-560 |
| P0875 | 8537 | 73f091afb6fd7d3c262c596c385cd667587d23f774449f30b17b929f696ef2a4 | L5827.B0.s.p0.d1-33 |
| P0876 | 202 | be8a9a8db6be92ae382cc41e74666d4154b1306fdbcea4bc2ea14fb4a6db9a9d | L5831.B0.s.p0.d1-2 |
| P0877 | 160 | a7ff6417f8f249192662dbdbdc84366492a29d7aad7d2c43c009d261424e3e7f | L5838.B0.s.p0.d1-8 |
| P0878 | 1079 | ef97c792c7f77dd2a32323d6e07eaeb49ba36889abddea84557fd4da2f42b4ab | L5840.B0.s.p0.d1-8 |
| P0879 | 2434 | 4ae0ee38eff79a740d5fd0b6d5e1e3cf58da5c3d497871812e310a1bf64d17fa | L5842.B0.s.p0.d1-42 |
| P0880 | 4582 | 804aef03fd8d04b397980bce864f68ab5311df69740c5a27d126ec5d6ead2e96 | L5846.B0.s.p0.d1-156 |
| P0881 | 680 | fca5dff72bd5d48f7e47ebde57f2b332466ede7ef55b1e1f127a90e72f694b01 | L5846.B0.s.p1.d158-178 |
| P0882 | 2392 | 4d910e6381c1504633c34a849b939be2373fba0366f2e16c33a0ddb00f74bd7f | L5847.B0.s.p0.d1-31 |
| P0883 | 43 | 377a42ebfad4e296d59aa1de9c3a8c2dc36caf7e271c1eb4e7c2af7e399fdde1 | L5850.B0.s.p0.d1-1 |
| P0884 | 117 | d159c155e05816b22adb19046715600ea4a5c14b732e55e6f889ccfd53ade75b | L5850.B0.s.p1.d3-7 |
| P0885 | 164 | 7c4129918c68c7bd70b2ee087ca5be0aed3f1bf195818fdeac62ae1f83c99a89 | L5850.B0.s.p2.d9-14 |
| P0886 | 4 | 3137c3f0d79851bd68f163e18ed7deeb37535f5c2d25e3114a41b9e180c16a75 | L5850.B0.s.p3.d16-16 |
| P0887 | 28 | 1e0117450512e763d338c7e6dfa297882417554cf00970117aefdfecbd2a7234 | L5850.B0.s.p4.d18-19 |
| P0888 | 1610 | 8b572446409dac8217957d3cabbaad0abaee2eb2226eae6480f816db6f156187 | L5853.B0.s.p0.d1-2 |
| P0889 | 2210 | 1014b5787a90faafc76dbb727fb016cb242b09abe4f9e4d116f8266b30d24233 | L5856.B0.s.p0.d1-16 |
| P0890 | 28173 | 992bfbdb001d27a83a90c3c5cbb2f9a97a8e88a6092c79d788b6d29f110502c1 | L5891.B0.s.p0.d1-1 |
| P0891 | 27481 | 9dbcd8fe75396457d118077647b68b05b3bab73d5ab353b690abbf765f2c962f | L5892.B0.s.p0.d1-1 |
| P0892 | 27483 | 8a0ac4995e2c292e96d2ead9d69788bc637a74ffec4ed04dbc3db984cd014137 | L5897.B0.s.p0.d1-1 |
| P0893 | 27516 | 47a826757af05b2168dd471291b6e219709cb433d5e20e7aa9c37d5c9ac79fd3 | L5898.B0.s.p0.d1-1 |
| P0894 | 3730 | b3ad78bec05fb1be690b2e003a56c4d4ffc627116b9b027802b5f023f4b79309 | L5902.B0.s.p0.d1-1 |
| P0895 | 62 | 9d58653c8ec433fc94f71369c451189de6ec926e595b042a52fab1a2152615d2 | L5906.B0.s.p0.d1-1 |
| P0896 | 73 | 0232fbc7f9fe8fdbb71c413dcdd5bcdd2c4b75d2dcbf1deb1a8234292785f5bd | L5927.B0.s.p0.d1-1 |
| P0897 | 89 | 6558c9fe5781c5f83bf09c00653f506ee8cd541415cb114fd090033efe8d7e28 | L5927.B0.s.p1.d3-3 |
| P0898 | 18 | 5d17404b3cff569d94a9c4331a17cd57c5ff78250aec74f966af25abdbe257c7 | L5927.B0.s.p2.d5-5 |
| P0899 | 25 | ff067db7267b437762a8881b30422a8b681284ab21589b33f86ef0c478cdb29b | L5927.B0.s.p3.d7-7 |
| P0900 | 22 | d6b02df8ebda06d343ed93ebdf1f274592fec69720821dad9b597be7574559b7 | L5927.B0.s.p4.d9-9 |
| P0901 | 20 | 7f11d306ce837c7b9a04530c837ad1f8234ad8ac433a4dbae21a71e54d9058e8 | L5927.B0.s.p5.d11-11 |
| P0902 | 324 | a90f3890b8757aede1545571eb849910e548d5040b17b47dc34362232070da5f | L5927.B0.s.p6.d13-13 |
| P0903 | 120 | 43a92886537786954e392c1b11f2c8d52966a68f0f8caf3e23440b41de400726 | L5927.B0.s.p7.d15-19 |
| P0904 | 19 | c16e292503947625c8c6df75a999ebd1789ebd5734cefc5001f438de0abac38c | L5927.B0.s.p8.d21-21 |
| P0905 | 235 | 9ee87580a8ba5a21247101183b825a957475aa6256787f5d09d42e62a95b19f4 | L5927.B0.s.p9.d23-23 |
| P0906 | 17 | f52496bd608308a92ca3374f6b53fb936a342a866ac0e941cc82e1adf009e0b2 | L5927.B0.s.p10.d25-25 |
| P0907 | 158 | 778be12d7e02b1927b0aa2bebbc6857e5da442bebd2027d027ae3228a257f0be | L5927.B0.s.p11.d27-27 |
| P0908 | 129 | 73b597398ccc47b39716a92e449a1b8f12ae68fa682fe140cd975cab410e9a3a | L5927.B0.s.p12.d29-29 |
| P0909 | 32 | 63e22f7644c940cb78f3a4187924dac9b525ee93e46b994cbc9e826a288d0ead | L5927.B0.s.p13.d31-31 |
| P0910 | 151 | f3586bfcd881c1543211254daaee0f483622b2764c39b69ed6586168fe4cd028 | L5927.B0.s.p14.d33-33 |
| P0911 | 25 | b598b5423ce5c6b781d98c020969ad2517a7201a4b7175d929d18f3ae97ca803 | L5927.B0.s.p15.d35-35 |
| P0912 | 15222 | d658083988f82acc5973534181610ab8cf1a279df11ba61528f241f0a67090cc | L5931.B0.s.p0.d1-1 |
| P0913 | 337 | debeea77209435b87e64d5fe449831c8576d90ce24f1722e26c9d9aa9d2d0921 | L5936.B0.s.p0.d1-3 |
| P0914 | 1783 | 30987e6cd742d0528ebcf6562b4f2169b4747b01463e5a1e831e2d95913579e3 | L5939.B0.s.p0.d1-2 |
| P0915 | 220 | f20ea9b4e3f2a3f3953695dee23027c56659147e4d16c7535b3d4de6368cb6d6 | L5964.B0.s.p0.d1-1 |
| P0916 | 117 | a524ae59712713897483679fd4266090c11e7968722b7ecc427018d2ab917240 | L5964.B0.s.p1.d3-3 |
| P0917 | 804 | b27991d695f30342d20cdf31967b53797dc53e6c9e194b1be25eaf55406ec521 | L5984.B0.s.p0.d1-15 |
| P0918 | 966 | 42925a6fdf036f71682f0fbc145477b5a0da7c793a047ec1b921e068397825ed | L5987.B0.s.p0.d1-26 |
| P0919 | 5 | d4fcdfa73358abbf319af6e0a735bae166d3258a46271613700e1f7895c1d49c | L5995.B0.s.p0.d1-1 |
| P0920 | 329 | e2f460573618528a1be7bca1d254bedf68f1e991de0b76bf863df9c7c5458efd | L5998.B0.s.p0.d1-1 |
| P0921 | 185 | 5767079ce567b7ed966e9cec2852e0168cebbe98d8ec0708a95efc2d242f8bb4 | L6008.B0.s.p0.d1-3 |
| P0922 | 30 | c1bb90d6428bb9a2f0b251e66f7eec37b7bbf051ff484ea6c3d4dc8d2e10b95a | L6011.B0.s.p0.d1-3 |
| P0923 | 396 | 9918624249864333fbb7620f4914ef50a786507942cce0139299dff215e573a4 | L6013.B0.s.p0.d1-1 |
| P0924 | 393 | 6158e9a6cd4688c81596fb22d0b1d3f3569e58ee709a9a1970b8312a5860e79c | L6013.B0.s.p1.d3-3 |
| P0925 | 514 | 99da213b429b54a648f8e9f0bf26b144425010f183f4d5e125711725515b0241 | L6013.B0.s.p2.d5-5 |
| P0926 | 490 | 0bcc72ead17110a372ad32e5d9d882dd7fea00a9e536ca9bc890eedda14e999a | L6013.B0.s.p3.d7-7 |
| P0927 | 482 | 9b30d27d09c7254b07d7e6f10f3d9492fbf5be964b5dea927d18af5a16ef88ed | L6013.B0.s.p4.d9-9 |
| P0928 | 89 | b05181f86cf3182a11e9958cf854f5cd4de93d8f0e280029bcca1565dfb31ec5 | L6016.B0.s.p0.d1-1 |
| P0929 | 35 | 063aea681e5d882b3d31f6c15747c3a7fa52f8d0805a7aba478b955f4fc92222 | L6020.B0.s.p0.d1-5 |
| P0930 | 254 | d126b4560894a5f122d054455a14ea3365c38b3eb830015957a336128acc92b4 | L6024.B0.s.p0.d1-1 |
| P0931 | 219 | 4b52de9b9a26e5f54009ff7ff7892f4fe2eb98445d65f9267a5762cbb1453cc3 | L6027.B0.s.p0.d1-4 |
| P0932 | 1506 | a8ee3094530b7fa4d2ecafc4bd349205d96a8f1da1bc16b4818a4de6085a3062 | L6032.B0.s.p0.d1-2 |
| P0933 | 1348 | 90a0fc425760d729ef42e5c022df065d1e8ccbe6350e62e50b8c6fe93ee932ae | L6035.B0.s.p0.d1-6 |
| P0934 | 220 | 864f93b59bdfe9494238d1a8a3bbc90c2df03c9b76d2ea9aef52746652a43191 | L6038.B0.s.p0.d1-3 |
| P0935 | 353 | 3a8bfcd725ec228c4c8cad7821e59fa681c6472ffd59f64b0e0069e716c222af | L6040.B0.s.p0.d1-3 |
| P0936 | 3872 | 86e9466c1dabad0c0031d2ea0055c0b7508bbe032dc168bd8cd1bd1548b25e48 | L6043.B0.s.p0.d1-1 |
| P0937 | 323 | a5da0362b3096bf6a2872479e0ef054d3549de3fdd5efd8e08491ab6210644a0 | L6046.B0.s.p0.d1-4 |
| P0938 | 335 | 010bbdd980fb78cbf317124bbf9f6e5f58130f2962479a9cbd3df43e35f15e2d | L6049.B0.s.p0.d1-1 |
| P0939 | 413 | 80f69132b886e4eb62c282a43ae5457ae3dddc6b3785dc70ea1735f557f9e958 | L6049.B0.s.p1.d3-3 |
| P0940 | 488 | f70778c6a8c2c8c622b63f19278a22bda315e03fdfc2b98e55e416dbcca842e4 | L6049.B0.s.p2.d5-5 |
| P0941 | 24 | d60f961e9f0595a7c50538273279993362e9b7d0a765cf35bc65708a2fc11357 | L6073.B0.s.p0.d1-1 |
| P0942 | 177 | a0b824abd2a091847eb09ee42c1cc289a9b4a95807f4585ebcc424629a746fda | L6073.B0.s.p1.d3-9 |
| P0943 | 201 | ee3a4c909c1f2635b42c4afda2a4917bcc0a893564ef667cec842a4e3af8333f | L6073.B0.s.p2.d11-12 |
| P0944 | 211 | 0090606e6c88a597e4eb29723aea825b999f66db8afbfff339d8dc464dac3e20 | L6073.B0.s.p3.d14-14 |
| P0945 | 62 | 5f6019d70b976124d270d31784a00b37b85df6455e82783b47cbf98c21626198 | L6073.B0.s.p4.d16-17 |
| P0946 | 49 | ad4e9659efb9075144c111b0c1b456386e0689c9746d421bac6808d51871e146 | L6074.B0.s.p1.d3-3 |
| P0947 | 161 | 49443a3f25045293a92da622feca92710624ae5b5a91d9c13ed5f616b634aa4f | L6074.B0.s.p2.d5-5 |
| P0948 | 59 | 4aea622b317702cf59b2b3822dac873a2b0de63a1bae20e1d35da408785d08a0 | L6074.B0.s.p3.d7-7 |
| P0949 | 114 | 2a96f5ebcb6c9de485c971ccd55478f2c35b96b91d8de36d9f0074ac12a10d0b | L6074.B0.s.p4.d9-9 |
| P0950 | 28 | 8c3f7b2c1c0ecce2e0d5e66f28679bbea43d518c4f343fa8d688d14ed26c42c7 | L6074.B0.s.p5.d11-11 |
| P0951 | 255 | 45bb8b75a1f9a7f1d1f92b11650ff9d8960ee375cd9123d8fac5660ebecfcf5a | L6074.B0.s.p6.d13-13 |
| P0952 | 20 | 487626dec1bb454176fed4f8fd93f732da411a3ee617bbe9ea7451ebe6a5ab34 | L6074.B0.s.p7.d15-15 |
| P0953 | 349 | e5905b0944bbd4c881c3990a52849caf688c72e59f235df1560110248e74922e | L6074.B0.s.p8.d17-17 |
| P0954 | 9477 | 48f267288feae055688dea68f14f58e2614e92385b8c0fce9761425060294465 | L6078.B0.s.p0.d1-42 |
| P0955 | 74 | ed562ca032cf2c163bbbf40ffced372cc5c56ee58e3a6ee59a209374734cce20 | L6079.B0.s.p0.d1-1 |
| P0956 | 649 | ed151335b75ec02a59567f2f301ebe8f3ac1ff57b65e4a9fce4a118a7dac068b | L6083.B0.s.p0.d1-4 |
| P0957 | 9606 | 35a3342e33ba9a398f948d7705036d28b6a2b9941936c5f92504c973d44aa293 | L6084.B0.s.p0.d1-6 |
| P0958 | 10 | 4b4719de06662a1fd2b54e339bfa4b93c963cc409eccf4a8854a187202656f07 | L6088.B0.s.p0.d1-1 |
| P0959 | 779 | 105cf54106b8c8166a3a8e1fe8702ab408a93baf8a5d40445476b34a025112c0 | L6089.B0.s.p0.d1-21 |
| P0960 | 4 | 65aef1adba8672a5fe7932d9f9f0dc35c2435893f65112acac628d2dc17a0bcf | L6089.B0.s.p1.d23-23 |
| P0961 | 629 | 68d8a35b2b74537ed9edb727ca56ed26ffabeed433bdb019f14fe41c7beaa708 | L6089.B0.s.p2.d25-33 |
| P0962 | 293 | 30301016d4a75dcb54d85bf9361235f9a748e2a570d25378966f7af8a3f1dfc5 | L6089.B0.s.p3.d35-37 |
| P0963 | 470 | e836c71a0530e81adb9a6c9f24a8f620a9372c5ddab64f5a066cd023bcd98791 | L6089.B0.s.p4.d39-48 |
| P0964 | 216 | 7ade80a63fa6039bd9dd88cc5e8f54006749379beb39902522914b15ce3ce375 | L6089.B0.s.p5.d50-51 |
| P0965 | 74 | 4a5d862eef84c81f4e71833f0fd557203cdb8a4931c7baea1544b30d02f357c8 | L6089.B0.s.p6.d53-53 |
| P0966 | 15 | a15d12503bf00f654df068bd9e5af6259db1703ac04a61365b991ce52557e5ea | L6089.B0.s.p7.d55-55 |
| P0967 | 380 | 73e0f2d6e44c98db40bd94da9b76cc5a761d769aba48545177ee64e752d59988 | L6092.B0.s.p0.d1-11 |
| P0968 | 28 | ba6904d243a9479e92f4226899cf3a3f518d5d5abcc845c3f67f1d1065a39f91 | L6117.B0.s.p0.d1-1 |
| P0969 | 19 | f01397ece036cc13cb33d4013a78dd7f8e0d0fd0686e38d73978a29eaf59d543 | L6125.B0.s.p0.d1-1 |
| P0970 | 1096 | 9b899c8270edd24313df1a089ca7284058c880da9613ec60de34c30523673cea | L6130.B0.s.p0.d1-14 |
| P0971 | 6191 | 7cd144f7e1d0dd9505c6c31c0d68063dede98af044fd2bcd4bc8c318ac6067a5 | L6133.B0.s.p0.d1-76 |
| P0972 | 87 | 87a1624b48a1959fd66bc2d93a91939b8a6eb04bec931cbb768281dbe750b8f4 | L6139.B0.s.p0.d1-1 |
| P0973 | 2761 | f1e3d89c65c200e2db21a3fb57614e753e3c889b0a7f7d3f6536b2342cd585fd | L6139.B0.s.p1.d3-34 |
| P0974 | 195 | 316403efcd9bfd14d16953de4cd33592c2aace120f5300f5a5a470dcbf2190cd | L6139.B0.s.p2.d36-42 |
| P0975 | 4735 | 80b95389c8a39957666a1dc1c2b7e5f8598ffc5d0feefb503603c848b1eb7ec1 | L6139.B0.s.p3.d44-96 |
| P0976 | 20 | 45ad0aff3876c4a50de973af538ac1cf5749ebde4d658013ca2ef7701b176d72 | L6142.B0.s.p0.d1-3 |
| P0977 | 159 | 27a4bffaeb14295f25f685bd294d9747ac595fa7905355ea675d430414923ef2 | L6145.B0.s.p0.d1-4 |
| P0978 | 479 | 89af1cdb6445cce0c4d441b3642a576e2031b23b51199846e4bc073167edce75 | L6145.B0.s.p1.d6-16 |
| P0979 | 21 | 6ab0cf9109b5b934d10cc9d2261ac65d1a10d63f2d1f32ca640fd119cf9c3d3b | L6148.B0.s.p0.d1-1 |
| P0980 | 15 | 30b1fb8944992e59f8ec7bf2814012a3d2874d31e67ed074d12ee7e767fe3497 | L6148.B0.s.p1.d3-3 |
| P0981 | 2118 | 297d99ca5dd2fccb343960c1f5f7bc069bb2d17c955553f56d00163db872538e | L6154.B0.s.p0.d1-36 |
| P0982 | 115 | 37f8b05e3636d0483ec1ca2dfdc7c5dd03eda76dc3b13035f11e3b965f4afce9 | L6154.B0.s.p1.d38-41 |
| P0983 | 321 | 0ce4d90e9b77333f500c6439e59db21b2601bac779e06670065604f5c407a69c | L6157.B0.s.p0.d1-8 |
| P0984 | 841 | 45451ba51216ba90dc5a26d73e791a505fe471b369f9cc28d4b0047ec9de527b | L6159.B0.s.p0.d1-10 |
| P0985 | 2703 | 9db3d2d4bf66bc2e134e073dfd4268256f69ae5744f34d25e4ace8e8da1e1519 | L6159.B0.s.p1.d12-60 |
| P0986 | 479 | 42e1f2cf4e4c7d30e1f91c3485e4f592f3af2d5a32bab96f5bcbbf97cdd567ce | L6162.B0.s.p0.d1-8 |
| P0987 | 208 | 9b22eac6f0457a9a5dac8792b67f5c56ef1afba26782b3b4b4d0689a4f0c8da2 | L6165.B0.s.p0.d1-3 |
| P0988 | 1144 | d61f66f98707f679afe98d09bdc8888a6aa63c5e3f79d54acafe4290bc3a8292 | L6168.B0.s.p0.d1-8 |
| P0989 | 163 | 5fcb192f82224da56d2923010911b24769346f21245401b1f98806f1012e1522 | L6171.B0.s.p0.d1-6 |
| P0990 | 930 | 1b70db89e9837bc5b2e253479b5a51bde73a01f16628a30b5a67322ea93714bf | L6173.B0.s.p0.d1-10 |
| P0991 | 84 | f53ba936537012c99aabd06d2ddfd6d5055b7b4da1f444c30b5dc9dd58adb65a | L6176.B0.s.p0.d1-1 |
| P0992 | 33 | a2091ae6ebb41d6de2694565c295d15d2105fe06c9d890ca19182e804d41170b | L6176.B0.s.p1.d3-4 |
| P0993 | 171 | 6e764f06d5eabb28fc578211e07fa5c99fdd965bf52ef56510e2421f0166649c | L6176.B0.s.p2.d6-6 |
| P0994 | 142 | 5cd8ae4391122a177fe83cc77489da5b6904b15a0daf36dd77828cb96a2378b7 | L6176.B0.s.p3.d8-8 |
| P0995 | 199 | 8317288b1de8a8a38e5b54ac40f209680771261acc0be59f5a69583868a06571 | L6176.B0.s.p4.d10-11 |
| P0996 | 141 | 3a06e1634b981b06110e52cca78aeb6f1da6376000712842fb1cb2ef99875b12 | L6176.B0.s.p5.d13-14 |
| P0997 | 141 | 8ccc1898635b424ada8999715bce176aa4d29e8040f4c0111d69aa8e9ea880ff | L6176.B0.s.p6.d16-25 |
| P0998 | 77 | e7c69b72066bf3d89c91f7345a97329a67aff0c5aae7a3996824e2cfccff5485 | L6205.B0.s.p0.d1-1 |
| P0999 | 4444 | b6d50a2c2e17d9b196cf932a77ea2d934096bfedd7646781fd95c6ce08608850 | L6211.B0.s.p0.d1-73 |
| P1000 | 157 | 9c8e2b124408c322691af982e069dc0cb6087103eeedb48f6359184339873ebe | L6215.B0.s.p0.d1-4 |
| P1001 | 3026 | 8473f6f06d0da25df9df6f1587deee9cae342c1470d1714dfcb3ddfc30837d27 | L6217.B0.s.p0.d1-44 |
| P1002 | 616 | a0a731785778c5cabafa3fe5ca1f8283364c776ac1bc566888fd7a752adcfc63 | L6222.B0.s.p0.d1-8 |
| P1003 | 164 | 94bf2af022c1eee55949321a98836842145621cc1aaea1c3c7b64d81d3fd7ef6 | L6227.B0.s.p0.d1-2 |
| P1004 | 2301 | b6756009e06aa478dd88016e7e6ac4f02a993922fee5288968247a4fc0cc2aa0 | L6230.B0.s.p0.d1-40 |
| P1005 | 21 | c2f0eabaf8f91e0b15714a6ed6e89febc6ad19760be270482ade5b1f55955884 | L6259.B0.s.p0.d1-2 |
| P1006 | 1076 | 5365be9821e4206a81e74a7b45542a12adc7b0d761a8349de2ea90f21f09c5b0 | L6263.B0.s.p0.d1-13 |
| P1007 | 6461 | 216eb09f74037ac7d5da07dbafd1195ac7db01d9fc2dabc246665041d4360b2d | L6266.B0.s.p0.d1-86 |
| P1008 | 80 | 8b8749300df14028f94680f9b816d90e1645f7f719abaa53e1f95604a05d6478 | L6271.B0.s.p0.d1-1 |
| P1009 | 372 | ba02331824c533d334556a28855942cc92866241334d9cee4e7fa2663ba84902 | L6271.B0.s.p1.d3-7 |
| P1010 | 3984 | 36a257371da2f91bc04550879a162eef8d9ea20f9d23d5442da34465f5366871 | L6271.B0.s.p2.d9-41 |
| P1011 | 10 | 5607fd3232f8347e2a9373395cfd398673bd20953c8a9ff4c1841d867ecd3593 | L6271.B0.s.p3.d43-43 |
| P1012 | 108 | ef27433845cdef95b478dbcc69b44be13c3c43420e78110c45f0f8c42de2a96a | L6279.B0.s.p0.d1-4 |
| P1013 | 5 | 9b611195f2729e6e5757662afabaa40a406581d5111d38dc52849fe9c09ad83d | L6282.B0.s.p0.d1-1 |
| P1014 | 2467 | 5f9e439c2a11bb9b30a165604828017f10efdfb085495aaff5eee81fb4e4d8bf | L6287.B0.s.p0.d1-23 |
| P1015 | 463 | 714e974977d443c032e89dba62271afb3ac94ae04a3139fff780e83467d3be10 | L6290.B0.s.p0.d1-13 |
| P1016 | 1960 | 73e8deba936d89e3981b09f31d4ade5893efa3a1567fcdaf350bdec3e99a35bd | L6292.B0.s.p0.d1-26 |
| P1017 | 1081 | 31462f2e178b2d15792bf37f07a06969c46d299e78a847d45ce95d730ad7a73b | L6295.B0.s.p0.d1-8 |
| P1018 | 330 | eedb154817ce3e229d2b8df99a9e59e426c7258f2f980861e12eec6669b6aec2 | L6298.B0.s.p0.d1-12 |
| P1019 | 767 | 68657f7016a084b6bca862ae120f22ab95983eddad034ff3cc000f845f24a05b | L6301.B0.s.p0.d1-5 |
| P1020 | 5 | 87e03f90d21d9893446d8e15acabe84c99247a21e287f7e7cada06e23f3a7d13 | L6306.B0.s.p0.d1-1 |
| P1021 | 72 | 5cd7b320610cc7e4155fb8e2ec6bcb38c638fc623cb908efb2d0e27747181e2b | L6309.B0.s.p0.d1-3 |
| P1022 | 190 | 683a9b07c225cf4034d2e55802533eadb5f7d29dadefdedb2f856739fb79f706 | L6312.B0.s.p0.d1-4 |
| P1023 | 9 | 5803ef978be6b09fe203fe30ca6b444002b9fd92d72d174f83de90e28518d975 | L6340.B0.s.p0.d1-1 |
| P1024 | 16 | 1a2b28eb6f7d42a1d24f67c7e55db657a46a0eec90ee5f268f45e599239dbf0d | L6343.B0.s.p0.d1-1 |
| P1025 | 7731 | a3769df261c704c757fe5b87a75f7431fb10f3bd5b48133a5a475bbe07559db6 | L6345.B0.s.p0.d1-226 |
| P1026 | 1097 | dcb546e23f88fc73b39d8785f3804849c7e8893c579f32d648cbbe9b0f7b49e7 | L6348.B0.s.p0.d1-16 |
| P1027 | 63 | f43584c0634626ff7aad0daa818de119abb61fb8cbde66c0ffc99f318bee6141 | L6351.B0.s.p0.d1-1 |
| P1028 | 1002 | 0000af6265b3e8557b8270cb6c83c2c240b54a975dec9450e249f6f64c543dc8 | L6351.B0.s.p1.d3-12 |
| P1029 | 2501 | a7c09b6f9799cd2346fd47e620bc83b9a74dbaa92c4060464a79cceec7499489 | L6351.B0.s.p2.d14-23 |
| P1030 | 2546 | 631c537beea8b4600549473453851e52d98ce5a5dce927d82a40c58972b70269 | L6354.B0.s.p0.d1-88 |
| P1031 | 78 | a078aed3025da49dc09fbd0365b9f0a2c061f25362e7214aee78851565be55f1 | L6357.B0.s.p0.d1-5 |
| P1032 | 53 | 883ab0ec46714c37cf4e8348b3f47f4b8921bf688a48194e6a12054122566dbb | L6381.B0.s.p0.d1-1 |
| P1033 | 20 | cc67e8c100ecc4f27688497a1eb1ec3e2020464cf66ce498dfcc1456e0b43f68 | L6381.B0.s.p1.d3-3 |
| P1034 | 266 | 7427adc8f4b6bdb34476924e74f4c9c60d8fa8600c75b62c878db16be0155381 | L6381.B0.s.p2.d5-6 |
| P1035 | 318 | cbf4813f55b9ab11739fc887bbbb306a30a85791f076ef9e87ed81f60cd3bfc0 | L6381.B0.s.p3.d8-9 |
| P1036 | 129 | 3b2f077ab11ede8a7937f24c0852d8116eb0f82bd5bdac1adcf4179df6545f9e | L6381.B0.s.p4.d11-11 |
| P1037 | 109 | 3d17ea39214fd27f64e0b753c925e03973cccfa2b84beafe81c831753fc15a41 | L6381.B0.s.p5.d13-14 |
| P1038 | 192 | ec51fc76003ff0464b0ac6b2441e920cba8fbd0a7dbbc273276622964070648a | L6381.B0.s.p6.d16-17 |
| P1039 | 197 | 13f882c753be968f7636c0f684f589eec059b2a74ab178dad74ba8df212e669d | L6381.B0.s.p7.d19-20 |
| P1040 | 238 | 5b657d8e90240ead39269072b7ee8da9854a3dfd423905faa9d5516a0121bafd | L6381.B0.s.p8.d22-23 |
| P1041 | 206 | a3a2b3a3446108dd76a9ad0990fd94d0e39b1627370c11543de44a969d190fd9 | L6393.B0.s.p1.d3-5 |
| P1042 | 334 | 20ef394a15ea292799cfa58aedd7af13b027e3dfdd8d910700419e91d9fb1b6b | L6393.B0.s.p2.d7-9 |
| P1043 | 429 | 17cbafe88494c30ac95ad84829dc3e2c634db98847cb468d3c11eaebf2ffe36e | L6394.B0.s.p0.d1-1 |
| P1044 | 89 | 6b57d9f3f606c7b76bdef97c691986b6d36c285ce4488647b77e5aae47ba1eff | L6397.B0.s.p0.d1-1 |
| P1045 | 228 | cf588430fc274620498bfb17f667a58ec6909f027eb449e81c9437ef219a104d | L6397.B0.s.p1.d3-3 |
| P1046 | 221 | 9e275d035a71c61b7e79c8e6ca99a472371671f120ee162e57e084e7bf9ee247 | L6397.B0.s.p2.d5-9 |
| P1047 | 134 | 2e99e542ab783ecba4be8e5893da3d57ad1f391b8a2931de2bc1a96d895a8bec | L6397.B0.s.p3.d11-11 |
| P1048 | 212 | d4df73225322ecef0cad4be2a8652d709690a6ac6ff7d0f3bab291cf3c24b96f | L6397.B0.s.p4.d13-15 |
| P1049 | 62 | 0e650c2024bb152c613332c4e925cb7817e8e8406991987b26db40b0f6cc8665 | L6397.B0.s.p5.d17-17 |
| P1050 | 33 | c14aecee7cd778a79b77a950ef886ddaf70049df6410b83d431620a2c023ff68 | L6398.B0.s.p0.d1-1 |
| P1051 | 24 | f9ba4beb7b2c6301655a9b4fd2197674d9aa93b8514254a252cf542427eb3863 | L6398.B0.s.p1.d3-3 |
| P1052 | 168 | b56da310dd7ec345bfbc56105782298b62afd72b8dfb90f20c67b7127c9e0728 | L6398.B0.s.p2.d5-11 |
| P1053 | 204 | f278a3af5179c45350059c3df51474ec10deb6946b874ef9df66dd692fd27600 | L6398.B0.s.p3.d13-19 |
| P1054 | 32 | 7ea12f43e03feb981410cf070190753d53584636d8766e036d2a96a3fe021c57 | L6398.B0.s.p5.d23-23 |
| P1055 | 20 | 6300e940e3c248fd3db05421cf7fd315c12fbc40ba8e3a2e20dbb70d324a80ef | L6398.B0.s.p6.d25-25 |
| P1056 | 277 | ba8cfde341e65a41f22261a012d80e35ea5cd8086ca9d4fb7089e5b1cc3d0321 | L6398.B0.s.p7.d27-27 |
| P1057 | 335 | 08fed7147d54a7295e0ac64a2cf6ea11993d1a818346bebc046e0a9562271c39 | L6398.B0.s.p8.d29-29 |
| P1058 | 174 | 5009be2c15364f5de615020478012363692dcdf5ded347a7b9c8fa0b09e05612 | L6402.B0.s.p1.d3-7 |
| P1059 | 368 | 9777c0fdb8e8f18f08db80a4d1f467b21c355dff618d41ea409e1865c313a79d | L6402.B0.s.p2.d9-9 |
| P1060 | 1704 | 3cfb2a928800afe6a0838a03b3e05279b4c98cc230f66970d1c3fe49d528ca73 | L6403.B0.s.p0.d1-40 |
| P1061 | 619 | eb1060a92ab7da85c7691a8e9047b7420e9bb35efbac88597b45bd4a01537518 | L6406.B0.s.p0.d1-7 |
| P1062 | 17 | 49f1b644a932914438a1dd444d4a78cc7d0d420110226e4534451604553ed26f | L6411.B0.s.p0.d1-1 |
| P1063 | 169 | 2875e219d0e8ce32445fcfe40e771a49df36075bd2a3ed24d728fc566768e77f | L6411.B0.s.p1.d3-6 |
| P1064 | 180 | 9d553629c16700c46dafe28fb4057d37a9db02ff9440dc969eab59758015f30f | L6411.B0.s.p2.d8-8 |
| P1065 | 48 | c2a7859a2ccc21efceebedabde17efe29ff4f7723d9ef66b0e72eedafb6a82c9 | L6415.B0.s.p0.d1-1 |
| P1066 | 56 | 729ce8892fd7b733090b8b573da502b8e9f98fbdee696df2598bde1f9c3a9861 | L6415.B0.s.p1.d3-3 |
| P1067 | 216 | e2d499a33f51c5404963bb1d59969a0d93fdb822e22bace80bd3f6de119e2e5c | L6415.B0.s.p2.d5-5 |
| P1068 | 49 | 09950fdbd01144eed41a695d7a35da1dcc1aeab701c343facbdefdaecdb3c604 | L6416.B0.s.p0.d1-1 |
| P1069 | 128 | d24e2eb0d784f02067b40dd453bdc63efe47c7d70af0e87e6daa29f86f37c7de | L6416.B0.s.p1.d3-3 |
| P1070 | 122 | 3192bc1d530f15c774becee614d017d1957a832a15594226b203f40a47bfdfba | L6416.B0.s.p2.d5-5 |
| P1071 | 293 | 85e5f6902cca4ff3aa31cac73a0de7ed079f154e43a9d3c1a9b21f54f15785b0 | L6416.B0.s.p3.d7-9 |
| P1072 | 304 | 72321bcbb7967a8c26c67e2d4abdef0dcb7c4a9898ad6c987d82ea9890fa46fe | L6416.B0.s.p4.d11-11 |
| P1073 | 185 | 50c75c301959b17dded384e78f2610c10e56736a81b1e9e9c13b764e907e9778 | L6416.B0.s.p5.d13-13 |
| P1074 | 203 | 329975ef32da189852dd9a4f9e2e2fc4dce5e10d26ac05cc68791c0cab34eca2 | L6416.B0.s.p6.d15-15 |
| P1075 | 108 | b5deb2284b8a90ffae87003126c92fb43d558a1ad387158005b074968855f6dc | L6439.B0.s.p0.d1-1 |
| P1076 | 43 | 676f687bdd54a9d7fb694074b23191bb5147b712a6089a90a8dac720eb04c0ea | L6440.B0.s.p0.d1-1 |
| P1077 | 168 | ee17069867c2383a1a4facdbc276d44f43249b9e6c1a4e1e42eea0a6569330c2 | L6440.B0.s.p1.d3-4 |
| P1078 | 135 | 8e94bc0cdd4c61aab435583989829d60fec796d84efa5441e615d810ebf1c9b1 | L6440.B0.s.p2.d6-7 |
| P1079 | 489 | d7dd7e9d0a0fe4b80c48ade779339daf5ef1d367137da6af5965a7cfb6fc6cc3 | L6440.B0.s.p3.d9-10 |
| P1080 | 26 | 9f0f4b5c8b5c027b858e0b83ded002d30de2f37628d2e8570b3d7c2727176278 | L6446.B0.s.p0.d1-1 |
| P1081 | 18 | 3573cf3969150e31bf56e1d8be2fc961084f9a905cb880a9c5c87900d63913ce | L6446.B0.s.p1.d3-3 |
| P1082 | 224 | b1e0e144f89d616c700bc6ede2ba464b4b014c0eb3edcf206e5dccf68913877a | L6446.B0.s.p2.d5-9 |
| P1083 | 36 | 2eda61ad7949c0709a8ba88e2502f18ded15b93bb88edd4a10bf0964a88fc363 | L6446.B0.s.p3.d11-11 |
| P1084 | 207 | bb9a060070075f1679ac37bfdbdda8c89dfaaee7aebb6283743012926e953bae | L6446.B0.s.p4.d13-13 |
| P1085 | 312 | 7b5de7c921ed05f8a399b64c052c163a6b47da2b745557a73709fe6c4aaf8ed0 | L6446.B0.s.p5.d15-15 |
| P1086 | 242 | b4f4bffda5b8ded67ae943d0a1ed3358fd4342ec8ba25388269e0c162b3612a8 | L6446.B0.s.p6.d17-17 |
| P1087 | 165 | bd154d8df6570216b9026d3edb7d6e6ce47d1771ae7fdbca31f2cefc786685e0 | L6446.B0.s.p7.d19-19 |
| P1088 | 1 | 5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9 | L6450.B0.s.p0.d1-1; L8149.B0.s.p0.d1-1 |
| P1089 | 48 | a8ec03d8f8093986697a8b4b456e35b0ca6479cb417444d8c9c7aae286a2d015 | L6451.B0.s.p0.d1-1 |
| P1090 | 130 | 2fdfdc9c96220a1b05cc4fd058826cbb10db2e4dfadae0d4b4a7b0f51ddc985f | L6451.B0.s.p1.d3-4 |
| P1091 | 87 | 63c51aeb1248bc97de70660bb15e188744e9f40f075f051f68903d6f5881adcf | L6451.B0.s.p2.d6-7 |
| P1092 | 175 | 12ea02b25bd82621f60b2985f5daf888709961419797fb571d3a14deb2a4080c | L6451.B0.s.p3.d9-10 |
| P1093 | 110 | dff6616626a7c22c9600b7f97e5f947a7d79624478038fd009155d044f4366d0 | L6451.B0.s.p4.d12-13 |
| P1094 | 28 | 31a9d5fa4cbb7cf15b2e8ad86bd894ac222814a65e727fde808e7ca65737fde3 | L6451.B0.s.p5.d15-15 |
| P1095 | 156 | 2c25cd606e2125d164e96120bd3de1a72ebdd3d448f6872a8b51a06c46cf15e8 | L6451.B0.s.p6.d17-18 |
| P1096 | 246 | 1c96251a0ee342d01945e95b826617cbc0db79507e701128a2d11668e59b4259 | L6451.B0.s.p7.d20-21 |
| P1097 | 192 | 0b7a7d56a0d8b06ff707672a225f32682dae889de6115ac9cabb32fe528b3ba7 | L6451.B0.s.p8.d23-24 |
| P1098 | 5200 | 14df69ee6816236031aaacdd9e0c5a4a400c77e442f26d2b419c0f43e5c22686 | L6455.B0.s.p0.d1-5 |
| P1099 | 145 | 75ff2b49b5e88da40368b427900a7a7c5728442f4e8adbe808f72c940b2b8029 | L6456.B0.s.p0.d1-1 |
| P1100 | 173 | d4d78a95b481f20d9998a709d72a421530d5e25d7cfe26dc8336e8d039d90ebd | L6456.B0.s.p1.d3-3 |
| P1101 | 239 | f0bbfaae39e1c8a050b924d0c3f26f0175c2f94715aa195f81a943b3ac7d81af | L6456.B0.s.p2.d5-5 |
| P1102 | 1704 | ca3fc75d39ca016f77b24553883fbda3708a9b793b1b1ef2f264ef432f2395d6 | L6460.B0.s.p0.d1-24 |
| P1103 | 107 | 793a90d1c759935bea3bf7acf0110557790fc17f7dd14d6d485c4eb1526f4b1f | L6463.B0.s.p0.d1-2 |
| P1104 | 38 | 5ec432d17f146c22ccfd835e76b566ac1ba1d753c365f4d2c3f022d0955e7cd4 | L6463.B0.s.p1.d4-4 |
| P1105 | 716 | b503b66cab95cb47527d0ac99ba35203d4a1a02060d4edf067b9ee1325f10db0 | L6463.B0.s.p2.d6-16 |
| P1106 | 460 | 11f8de15e0d7a128b5837ac531354ff7eab73ec0c9fd8d428daaec064b8f7a57 | L6463.B0.s.p3.d18-23 |
| P1107 | 310 | 2fd3a1f0bfbc28248c99499b18d428ac93ce22b23c62dea445319f6cefcf6f8a | L6463.B0.s.p4.d25-28 |
| P1108 | 115 | ff1d2489997f4d191c26a9ebc8a783751fa3845dd1c49451af272266246455e0 | L6463.B0.s.p5.d30-31 |
| P1109 | 249 | 3edabc08732d482d7fff2e147039c40ae26c5c59bdd1e9cc45a6f1a72cb177e7 | L6463.B0.s.p6.d33-37 |
| P1110 | 408 | 681a37ce87bd9c499f69eeb37402c7cdb840a30aa5d5526805b290ec11389e49 | L6463.B0.s.p7.d39-44 |
| P1111 | 143 | 4830c80201a0dc9c25817c936753e2dd63a172094484d7d6bbd9e6be4617a2a6 | L6463.B0.s.p8.d46-47 |
| P1112 | 48 | c9df771b676d6f8f8b9bc98d381f33216e7edd66a0ebc328652ed5d1f2d55abd | L6463.B0.s.p9.d49-49 |
| P1113 | 40 | aab652649af675bb2afb9c7aff442b3bbc67df46e657ed2a71f99fc392fe7683 | L6463.B0.s.p10.d51-51 |
| P1114 | 21 | f114e1a86080b64b4b73de44cefcadd284e5cc2902e87301087292f009585c9c | L6463.B0.s.p11.d53-53 |
| P1115 | 268 | 591b5b5806ec0645d67cfc11cf188ea63ccc3b9e5b3fbe706233c29d4efda5da | L6463.B0.s.p12.d55-58 |
| P1116 | 61 | b611da4e5d815c5981e22eddd2010080d58a506650b2e9c3cee8e9e361f45aa0 | L6463.B0.s.p13.d60-62 |
| P1117 | 267 | ee55db3b3195f420fb51a358b990daf7f9b42ba50fa1f980072bca6f98b559cb | L6467.B0.s.p0.d1-4 |
| P1118 | 10 | 71230b0057b7b241ab9df4c392ae2d030d21956f6ba8ccf2d8aa6785e841aba7 | L6467.B0.s.p1.d6-6 |
| P1119 | 393 | 2d440df6f7191bc8b46fe90032c3611bbd144fa62ec5abd6bfef22baa26ec1f5 | L6467.B0.s.p2.d8-13 |
| P1120 | 25 | 0e3a7f94596d46aae0c300028f524ba80d9b4741f32a3f38471c0842fc14969a | L6467.B0.s.p3.d15-15 |
| P1121 | 384 | f0e20f7270765ec4068c6f927892c8f91a09c38acd2aef1e4d9a6638e1b7028e | L6467.B0.s.p4.d17-18 |
| P1122 | 465 | f481a98ad0e14e2c538debb27781b64681d456fc53a08f53d3e56d37885e73fe | L6467.B0.s.p5.d20-24 |
| P1123 | 35 | 05401726ac2904ec463b9ca92b517972ae5949e11a3c609a5456c5261d5b73be | L6468.B0.s.p0.d1-1 |
| P1124 | 183 | e9f7179be682ee9f09ba42786805801e288d197718c6e8cf7a6116d79dff7c36 | L6468.B0.s.p1.d3-3 |
| P1125 | 59 | 779c0944332148eddcb9735aa1e150f6883d2c393ebec1174f69f68368118d04 | L6468.B0.s.p2.d5-5 |
| P1126 | 125 | b3e22c4950c82ba6c56aacb29de6389427ef7c15eaec756cea3623ca343cacbd | L6468.B0.s.p3.d7-7 |
| P1127 | 107 | 7295bf4d940adc28330e3d8055187fa7deb058d497dc4bf76b5cd95c872722a4 | L6468.B0.s.p4.d9-9 |
| P1128 | 31 | 31ac5d9e7add6a5b0202f87a2853e3cecbde505cc80d9af5979e43ba51624c89 | L6468.B0.s.p5.d11-11 |
| P1129 | 73 | 51bc6409f9324612e2ff161323c2a43b8827ff95df0d9fd66e1e3ef5b5aff7b3 | L6468.B0.s.p6.d13-13 |
| P1130 | 148 | ad61e6735ea840f465794ecb4b6e3aad9f78408ec0c3f20fafdc2687c7e858ec | L6468.B0.s.p7.d15-15 |
| P1131 | 286 | 8b485cb2347f0f0d2faee1d8641b333e7fd137182a03f13dfd971a69b5537262 | L6468.B0.s.p8.d17-17 |
| P1132 | 406 | bada7fa144ecb201bdaab9fab0c9278f1ee4bf5723973cbf9ab4c4dab49c34f5 | L6471.B0.s.p0.d1-5 |
| P1133 | 74 | 58189eaf07a8ed190268ec1a098da34259b6bcb55df1e2de6d098b5a22ad199a | L6498.B0.s.p0.d1-1 |
| P1134 | 13 | a060a685988171deb289869e0e450e2d3bec689223389d8560a5e9b082f543bb | L6501.B0.s.p0.d1-1 |
| P1135 | 227 | d3958afead33b39fd306fab001df2e6d1d53a893da8a833f6f0fdc5efe3f5fb3 | L6501.B0.s.p1.d3-4 |
| P1136 | 14 | 326cf252c4ee89d18ff7f5f33aa4e5cbfc5a0cd9cb6c17b0c0c06551469ac183 | L6501.B0.s.p2.d6-6 |
| P1137 | 103 | 4b14192d0ed258bf288c69aafa4b7d243a58a3266ac2348a829365317e5f9262 | L6501.B0.s.p3.d8-8 |
| P1138 | 58 | 0edf72b83afcb14dad4d0cce82719c27915782ae47f4e5ebc94fb3c96778397e | L6501.B0.s.p4.d10-10 |
| P1139 | 8 | d4b1ea5708dd532930a85188b45aff6f0a3ed458500c7577e0127a538eb0d100 | L6501.B0.s.p5.d12-12 |
| P1140 | 298 | 74349dbdd059e7dc1b8e2ff2a1cc12fe942fe6ca66821ec272a56b717879da4e | L6501.B0.s.p6.d14-16 |
| P1141 | 28 | 895617e800270d435d897c3ee6d2d27364b9bc9fb4fd242048917626c8ebe8ba | L6501.B0.s.p7.d18-18 |
| P1142 | 174 | 1d01c1262c167da03c3be9062f62a2f5d8ee79f9878576120b9e247731fd5d70 | L6501.B0.s.p8.d20-21 |
| P1143 | 35 | 57aec05d943169125293054758c1ab002da6ec8b1373e2fb1bda4899f08f5bb1 | L6501.B0.s.p9.d23-23 |
| P1144 | 74 | 2a5275d9a6f9338f27985902e3de5927884f169987bbccd01ff1222ceb07b087 | L6501.B0.s.p10.d25-25 |
| P1145 | 56 | 1715d4259dbefc13dbbd0e98845f30f617b39cc0b1772581a64c79770d2ef762 | L6501.B0.s.p11.d27-27 |
| P1146 | 13 | ac29a2ab9d2e2cb8a116456b44b0e4f41b94a11ac4213298ecf5fe5bf1494284 | L6501.B0.s.p12.d29-29 |
| P1147 | 62 | d26152718bde687b331da6cd7ae471d61243728c98a56d5c28545475f49a4709 | L6501.B0.s.p13.d31-31 |
| P1148 | 47 | be3bfe77c624140511f3f289bba0cafd1ba202af077de9b9e35766eb3e6d1956 | L6501.B0.s.p14.d33-33 |
| P1149 | 19 | 99055606d0cdd8a11f7f61253f8793f7239d21927f2d5b79de8902908f109d79 | L6501.B0.s.p15.d35-35 |
| P1150 | 160 | 1b2e3975b200fa32e3aca168b5861a56bf9fbecbcf95b66f7f7ac05ffe3769ce | L6501.B0.s.p16.d37-37 |
| P1151 | 4 | cb9e8664edea8cf5965fb4d4d013974ef10caeaad734702b19617210828cd7ab | L6501.B0.s.p17.d39-39 |
| P1152 | 180 | 44208751d2bf2dcabd3e5b8b9b16a4739b6b8bab309fc2dfdcfd5227c6cd8b38 | L6501.B0.s.p18.d41-41 |
| P1153 | 13 | c9bf2e8987cca1e21e5c38a8002a0a44792ee8437382c556b2cc66a216748ad3 | L6501.B0.s.p19.d43-43 |
| P1154 | 11 | db5f2f038452c6d800756e75c9ed0978ad804e8e58aa4a85ee7cdcfac61f5f95 | L6501.B0.s.p20.d45-45 |
| P1155 | 13 | 2098cc067f8b57f10d53655ff8d926b89dff2abdfae20762f1f00e6d4e5a77ad | L6501.B0.s.p21.d47-47 |
| P1156 | 25 | 6d1f4c0c139e6ebac996941c65d1c89044d1ba6db0a36a554939cc96fa6d0abc | L6501.B0.s.p22.d49-49 |
| P1157 | 197 | 767bd447968a3e200bfda6b9b705e5073fb78ffcf4f21e0809e40b649a1491b6 | L6501.B0.s.p23.d51-51 |
| P1158 | 102 | 5d86ca7fbbf8cb9f55a06471ab2988661b0885a371a711ec37f9cb9fd5b27697 | L6501.B0.s.p24.d53-53 |
| P1159 | 10 | 91f71c14c8d6cc647e8658f9a4b9dcb885ff3f7fef1d3e774f46eae35bff0bcd | L6501.B0.s.p25.d55-55 |
| P1160 | 62 | b7423bb47efea12530363ea7f4af1650caea85252f74b347e3ab3e2e95bfea2a | L6501.B0.s.p26.d57-57 |
| P1161 | 5 | 8d59829c1e15afe1a7fae93e8e5e32d8511bec5fd598a09f4fea6033b31e8a66 | L6501.B0.s.p27.d59-59 |
| P1162 | 88 | db3bc224e956e6d3685ac0683701a906a32d6cff4fe617d341565ac3bda9a276 | L6501.B0.s.p28.d61-61 |
| P1163 | 88 | 2fae77ed4e15f2dab587d24c171dd2df77a69b2374f64e988661302d6a10edfb | L6501.B0.s.p29.d63-63 |
| P1164 | 20 | 4b5f8c112f10efa64b2eab9e315b4583419eb73b956b8bb2fd3e3cd542fa57eb | L6501.B0.s.p30.d65-65 |
| P1165 | 62 | a8a19d7fbeb40defa94447d0d6ca448f2a65f38afade249851ed5b21d3904a23 | L6501.B0.s.p31.d67-67 |
| P1166 | 112 | 50732c0cfc3ffc65259591148f1b318ab0862d40c411d80320df87e9c9847d11 | L6501.B0.s.p32.d69-69 |
| P1167 | 85 | 72051160eed27ab7b5c18bcfd8d35189ba0d789b0f4a7e416c74656a18ed690b | L6501.B0.s.p33.d71-71 |
| P1168 | 92 | cad01e35776f7ee71e5a0780bbb1a3097c9c1ae8ecf8b8a905b600ba2fd5fc12 | L6501.B0.s.p34.d73-73 |
| P1169 | 30 | 2b65c8588010e431d8a578ce01af366a737a572a5dd23f8a80f21fbef3732bc3 | L6501.B0.s.p35.d75-75 |
| P1170 | 49 | 53ae5bdc951b527c1f3d600d6a19e0a67310cfadf35410d34ab0be1d8343bdf1 | L6501.B0.s.p36.d77-77 |
| P1171 | 48 | 001c2a6a2d12bc655a458681298fd9674e45a2fae49e5c4167bf1210116404e6 | L6501.B0.s.p37.d79-79 |
| P1172 | 58 | f88d9195bd1d643f7b0745a7bfb51ddae8587664f5cae094c9e2ccc05c20409b | L6501.B0.s.p38.d81-81 |
| P1173 | 67 | 7320848ae4490501d8e4941f57f21dedcc4bc0a6fc533b72a5782d4b02f39e12 | L6501.B0.s.p39.d83-83 |
| P1174 | 90 | d5a854a872649e79b0ad3885432393d685ba139699d188417357d709d2e0dbb0 | L6501.B0.s.p40.d85-85 |
| P1175 | 47 | 412360c44937c586ea886c78e6240b9e0ace8b6fae5cf647e43950a857bdc9cd | L6501.B0.s.p41.d87-87 |
| P1176 | 24 | cfe711e2e169c4a9a9d37e9fdd1338676b1995b058e3d235e039b9cf35667921 | L6501.B0.s.p42.d89-89 |
| P1177 | 175 | b7cb1cfb7ffba1ee2eb11ee457086f8941664c28744905570e47de233b364336 | L6501.B0.s.p43.d91-92 |
| P1178 | 14 | dae7b3a95a36d3bd5691d5ec995d10a9fb934e5fb1d9cc6b18201c8bcb539c5c | L6501.B0.s.p44.d94-94 |
| P1179 | 87 | ff697fa79b39e62898b8c146ebb760675f2ceba39d69463763a4270f90665e1d | L6501.B0.s.p45.d96-96 |
| P1180 | 16 | ab41f3aefdd4a2f802d13ddce19bfa13e720ddc2016dee1ea092920a6e73681d | L6501.B0.s.p46.d98-98 |
| P1181 | 207 | 4e1a3616e073c5e7e4f8f31116821d42eba4b36bba29b2868133107d00298841 | L6501.B0.s.p47.d100-101 |
| P1182 | 15 | 16d26116c2f4536efa19544df15395cc5386508fa6846305d4855bec622dc056 | L6501.B0.s.p48.d103-103 |
| P1183 | 57 | a251310256c898774a35fbfd4e6b032c50ec782f273260e8c9eb98b4813d6e38 | L6501.B0.s.p49.d105-105 |
| P1184 | 25 | f3167d5f7c254968176b489e0a748e05d9f777e36d214132aafb081a1a5995eb | L6501.B0.s.p50.d107-107 |
| P1185 | 79 | 1624e11d3b7404971116bfec97cf400757c1177fc3c9b83dea218b0b6b9cc37b | L6501.B0.s.p51.d109-109 |
| P1186 | 68 | 4e1877ccc91d1f0f471f2b513227deb7b9ce0f5e6189485550225eb7be151d97 | L6501.B0.s.p52.d111-111 |
| P1187 | 68 | 8d94b764a5934b36693b9b4bd3b4d5982a256e1ed141eb69af606f0cd1f67f9f | L6501.B0.s.p53.d113-113 |
| P1188 | 93 | 9e570be218a40f03801b9b18c428683d5bd306f370ec232f77b30b6f52d88459 | L6501.B0.s.p54.d115-115 |
| P1189 | 42 | 708077d46f5cf5ef217d07601351a4bb7aaef646528849ae9c2341179b7ff3de | L6501.B0.s.p55.d117-117 |
| P1190 | 8 | b80355750958f901b4ddf92d95e005a4d0ac812f369de3a9a1602ce4c57c6524 | L6501.B0.s.p56.d119-119 |
| P1191 | 106 | dd039b55bbbee91a15c37f61cae27c3f756819fcd01aabe1245e3cd9dfbcdb18 | L6501.B0.s.p57.d121-122 |
| P1192 | 17 | 20c901db501c4fcb1c2c248daa31a6797779ba8ea65908d60362d28ce32a2363 | L6501.B0.s.p58.d124-124 |
| P1193 | 20 | 6cb42332b379800acc994c95b2c0c673b8235b4a6c21f22863e31356945836eb | L6501.B0.s.p59.d126-126 |
| P1194 | 446 | ba08a8b44828b453744cd325088691f610f378e175c038b4f707512b03363e58 | L6504.B0.s.p0.d1-6 |
| P1195 | 197 | 5beaa6c62d9633ba32b6549105da3cfcd585579a396b3c1b4f758738c3240386 | L6504.B0.s.p1.d8-11 |
| P1196 | 79 | dbfd78f1313624635b11bc6ccf8bb3ff160d1b7a7db4d215d37eb80f8543d50a | L6508.B0.s.p0.d1-1 |
| P1197 | 24 | 3f9620cf050060ad58e5635b3be555a0e830a61a817604a10cab3052220da913 | L6531.B0.s.p0.d1-1 |
| P1198 | 222 | 1edb07bf9a2cfa4ac8cf381736f31a15727f0cb2afd5e7ec1723dae99b7c7999 | L6531.B0.s.p1.d3-4 |
| P1199 | 59 | 920f8cc91f6c11410a4ce990673f6c4afa785186da903dbceabc97f283181d21 | L6531.B0.s.p2.d6-7 |
| P1200 | 247 | 6d76a8b18e83a72f52444bd733a90b2bbb934a38d1e796817d7bca1f0d240859 | L6531.B0.s.p3.d9-14 |
| P1201 | 181 | 9d8214316a79cdd878af91d2ee7a291fed0b130458dc7c0047e5b38f0baa7c16 | L6531.B0.s.p4.d16-16 |
| P1202 | 207 | 138492a609da3d7f269f33f2dcc4d404b3279a48de2e55e5ff70721a67968f1b | L6531.B0.s.p5.d18-19 |
| P1203 | 48 | 0f331860d016c2cb017cad80a51ec8283e91a31ceaee47afa90023e89bcccad3 | L6531.B0.s.p6.d21-23 |
| P1204 | 236 | 7d3c5c74e566945fc8cd35f1e3c274f194059d1ff59817c6afe81dce48e977b7 | L6531.B0.s.p7.d25-26 |
| P1205 | 135 | d73c7f70ff7b77187c0db0ea9eaf5d43f95a24f8e6cff81e25aec4c00b3731f7 | L6531.B0.s.p8.d28-29 |
| P1206 | 34 | a88bed8b1f20e5532aaba86ecec95982be23af8158af4d8adbcd1d1639330e51 | L6537.B0.s.p0.d1-1 |
| P1207 | 86 | caf8335253de5602105ae46f2c0cc778e792847d8bcd910cdec2b0c2828327c7 | L6537.B0.s.p1.d3-3 |
| P1208 | 107 | a0ded5a12eaee171a09780288434db54746982926d9a1212f34988ff5540a8c5 | L6537.B0.s.p2.d5-5 |
| P1209 | 82 | eb4ebe4f007fde3b03e968938d9b38799dd8ce8d348452ee9a3f717e81ed2a3a | L6537.B0.s.p3.d7-7 |
| P1210 | 91 | 3e2b7a977d60193a33221c6a41bd3b1cb506ec5d1e7487c29230ef329522ec5c | L6537.B0.s.p4.d9-9 |
| P1211 | 30 | fe5ea9f3388f92b2966c12189c26733c3d4a64feb663935cca40cb188e09c7e8 | L6537.B0.s.p5.d11-11 |
| P1212 | 1018 | f83705b0212bce71d1556fd17a8b558354b3868ba1bb1ffc2e1f78c1095db85a | L6537.B0.s.p6.d13-23 |
| P1213 | 54 | 397d56ad3a291326c2ae8b5ccfbe5b5803a96945c85ca2c68b8eb8f698befd45 | L6537.B0.s.p7.d25-25 |
| P1214 | 654 | 382a1d6d9449497024347545acd8f2029396079e9a39a23d2755e77f5c1b2114 | L6542.B0.s.p0.d1-1 |
| P1215 | 1836 | c25930f71ff9ff02aa21c4348032e60d21ca9a46383c6da5543b087285900c45 | L6543.B0.s.p0.d1-19 |
| P1216 | 118 | 189cd0fadefa3fab4ccab20c9d4adbddb3bf345a4f74164c2a5cc2a3b1f3b357 | L6547.B0.s.p0.d1-5 |
| P1217 | 15 | 8bd6cc92d4857a0fbf5f9b96a33675373ff3ec7e48f1365b4206782a09b67560 | L6547.B0.s.p1.d7-7 |
| P1218 | 866 | 499275fc1910bc67e73e07c9683154c3d8451c3f7f015ced4253696b5e0ead1e | L6547.B0.s.p2.d9-13 |
| P1219 | 36 | d289ef8645f8ac334b80a04e8a55718c6967f90f6db5dcf83f7ba0df1d0d25ba | L6547.B0.s.p3.d15-15 |
| P1220 | 6 | aac32651b10f567c461b9b4f255d6fb1fa6859b5368d8bd9a51af920ab21cf23 | L6547.B0.s.p4.d17-17 |
| P1221 | 12 | 55aec44787473df9add42bcb49e7a9d80129cb7de7533c5bfbaa8c673fbeec14 | L6547.B0.s.p8.d34-34 |
| P1222 | 230 | acfa5b2db4b7b635e8e0d78250a484eb370bec008329cc2d7d263196f08d7802 | L6547.B0.s.p9.d36-40 |
| P1223 | 180 | 9252b1198928ef6ee47ecf5507e3c2e2ceab992c4e3c48713b09b8fb95c1969c | L6547.B0.s.p10.d42-43 |
| P1224 | 206 | 619802b4c84d68c09806ae3e8b3b92de5446287a3fccd1c7f304fbcf12ee9ab1 | L6547.B0.s.p11.d45-47 |
| P1225 | 14 | 7554e119e510406932f2556151ad67cb1a0888e61b19542a3a1fc7980c48a42a | L6547.B0.s.p13.d51-51 |
| P1226 | 49 | 27dc97756f4fdcf5beb311fd9f0343688512de1c09c7283d6650fb56a6bf2477 | L6547.B0.s.p14.d53-53 |
| P1227 | 199 | 2d05394288ac6c5dce60bfc58ffdfd661e33e420a4484ded1067fbf613caf068 | L6547.B0.s.p15.d55-57 |
| P1228 | 22 | 594bcfdc752a21016d0b036145396abae05e73f02f1e5ab0cd8b61c69e7e4f4c | L6547.B0.s.p17.d64-64 |
| P1229 | 84 | fc33b71a5415f3f4ca5dd8589c60005775359e2db20b0da3ffdec75139122f52 | L6547.B0.s.p18.d66-66 |
| P1230 | 67 | 5b2bbc47e25dd1bcfc4d87a8b80b9359be8251a2334e3b9bf8874597a00bce36 | L6547.B0.s.p19.d68-68 |
| P1231 | 83 | aec0fbe976650a8b6f316401f88e7b39f980c000b964995422416c96932a0eec | L6547.B0.s.p20.d70-72 |
| P1232 | 49 | d27eefcfd66ded3daca381e15f53ff1fc9705fb81103ce39d4d1077d1b2153dc | L6547.B0.s.p21.d74-76 |
| P1233 | 60 | 3f6445e96d290a6ec8fd82116105cdf2de1b5b513ea4e8a2c27087369afc1156 | L6547.B0.s.p22.d78-78 |
| P1234 | 34 | 1a14814e43f954c7e5527a569499818c687bf87e6e2e161c76f9135b293afbb1 | L6547.B0.s.p23.d80-82 |
| P1235 | 336 | 5b2e3b8eb7b2f4c726be6f7818461870374d9e02bfdf8c4fcf887353f9e39842 | L6547.B0.s.p24.d84-87 |
| P1236 | 18 | fd2248b7fb546c303f3b2f32396cf1bdfefe38b5752deafdf8aa6fd006a68560 | L6547.B0.s.p25.d89-89 |
| P1237 | 187 | 9e35a664c371fa5a2397575be9d7fd60c87474de8e7e22fec5086fe3e7ad99af | L6547.B0.s.p26.d91-98 |
| P1238 | 84 | ba8af6cebcac1721c3ee1123399990087a3bf0af914b65b35d23002bebdf9b3e | L6547.B0.s.p27.d100-101 |
| P1239 | 85 | c7adc614753400da71589dd2def3cdea7687c8131af4b701b19ee1a4e0f3f4b2 | L6547.B0.s.p28.d103-104 |
| P1240 | 100 | e1f7bb475400167ee96eed488a0ce1d3a57ecaebb9824b4e0c5acb1d8906e0ca | L6547.B0.s.p29.d106-107 |
| P1241 | 90 | d9389cf0cd4ae47ba004603086615c81473dfae2e193db7cb085a81cb79ac614 | L6547.B0.s.p30.d109-111 |
| P1242 | 124 | 91679bfc6f5e9406f7b95017b0cbcc4a7fcf8706d950fa914e14e80037f9a79f | L6547.B0.s.p31.d113-113 |
| P1243 | 17 | da631487f5813f13478d451ef192611dc59c4fbec31e852c3c3fa769277dd17d | L6547.B0.s.p33.d117-117 |
| P1244 | 117 | c7194d8d3f90ca1d0f47326cf95ed9011fa0148b2370b8a3eb9e2790b2343b19 | L6547.B0.s.p34.d119-119 |
| P1245 | 16 | e70c3dc8052196d6faefd6f37b2defeab46676b8896dfbe7ac04800e65d1f747 | L6547.B0.s.p36.d123-123 |
| P1246 | 81 | 76497ead46e5012b7e3bdbb96777cd3fedce4d2d1bbcfc259a2f24e143073fc1 | L6547.B0.s.p37.d125-125 |
| P1247 | 11 | c90c420e9d736e555be8f90a85b819d98f245b98903deab5edc8a1e3fe8db186 | L6547.B0.s.p38.d127-127 |
| P1248 | 59 | 5b40eb6cc4a307610ee4cb01aeff6e35903220fe3c128fbb25aaa4844c5aefe2 | L6547.B0.s.p39.d129-129 |
| P1249 | 107 | 80d7ee426c86acc90eee1c3b25f3ac539a3b58889ced3819fd9d05523b1f78db | L6547.B0.s.p40.d131-132 |
| P1250 | 20 | 84ae2de7040ff3635c968e238edc1dbda12f73f73371a6b6ade98a51277beb3d | L6547.B0.s.p41.d134-134 |
| P1251 | 175 | f7bf54fd7c17f8bc0d1b327f200ccd203798a2ab429f004bc4265a63469fb291 | L6547.B0.s.p42.d136-137 |
| P1252 | 629 | d85f2f7b7309b28ecd2e97e820841ebc39b7f296c45f0dc8ac7af901ab5ca7ad | L6549.B0.s.p0.d1-34 |
| P1253 | 43 | cf5cc0b2d2375a919f226c5adb0601516c77aa1f06afbc1de9918a5dc0f6d4b9 | L6553.B0.s.p0.d1-2 |
| P1254 | 148 | 5e3b6201a62380adc7c485e96b2efd56b434802ea075c5310ba3baf6ef5da3ed | L6553.B0.s.p16.d48-49 |
| P1255 | 41 | 538175256201d532684d00f0f0eeff7813e6b59fbce4f5754cfdca6a28a75ea6 | L6553.B0.s.p17.d51-52 |
| P1256 | 25 | 30323d4dbeaf6411550f4beafa13baac6e5a41f7a8d528f3d3bc6eb10648a65f | L6555.B0.s.p0.d1-1 |
| P1257 | 155 | c60b9953a592c8b5bdc24abd76b2be1eb8d19494cd72b3e5363a6c023390e9b8 | L6555.B0.s.p1.d3-3 |
| P1258 | 19 | 52f3ecdc2bed3e4a9723ab529a2ea7227bc76200d143509815723b0c8da2bf68 | L6555.B0.s.p2.d5-5 |
| P1259 | 252 | 7e2c870a36390cfedc9073a19a3e0a33abe6f894bd0adca121aeff941fec0bac | L6555.B0.s.p3.d7-7 |
| P1260 | 23 | fd31a8a16c751849789d90af1c34c85cc82d113fb9644d33a79250559c110453 | L6555.B0.s.p4.d9-9 |
| P1261 | 172 | ee56bb394bb764ef964a324d5652550b09cb954d2d46ed128b60e570236d3e73 | L6555.B0.s.p5.d11-16 |
| P1262 | 234 | d4cdec3eba2f2ce87874c77a240315b36fabfcb23e3aa88a9dc570f61572ae0c | L6555.B0.s.p6.d18-18 |
| P1263 | 954 | fce3ced14b2a9cef44506672d0edf9285e43d5e7e4689e9917b2eae5e2321788 | L6559.B0.s.p0.d1-38 |
| P1264 | 93 | 1e483f42e853e902b7037566976e4c7ce34757bb9d455dc73c7a59774d66fca6 | L6563.B0.s.p0.d1-4 |
| P1265 | 16 | d6cc5a2c6ca34c73a0f7ff1f738acaedab2daa8aa938f4f4876e321326a8c4a1 | L6563.B0.s.p1.d6-6 |
| P1266 | 100 | 7a9d958c549a93fe33b1aa6494f3e877689e5d7a6b0114e5e79ccc5c71a7f20d | L6563.B0.s.p2.d8-8 |
| P1267 | 27 | ac1c8baf38720c6d674c731cf5fefce74ea609aa9536059ef2c4c94e952b2ce8 | L6563.B0.s.p3.d10-10 |
| P1268 | 159 | 40beb2bec7e89d27c8e4e2c664de6f55f1b1682a268a9e1db0dc5dd47ea61bc8 | L6563.B0.s.p4.d12-14 |
| P1269 | 18 | 8b85bb8b008568270a4e739cc3bb7bf5d57c6ef4e6484625e113813c0a774e64 | L6563.B0.s.p5.d16-16 |
| P1270 | 77 | 2f0532cdc3e06ba22870f584516d8759a202928f847ef62a3453fbb9f132daff | L6563.B0.s.p6.d18-20 |
| P1271 | 79 | aae88e464937bb8430dc5e357ee974505188e10fb78384bbffa86b9c1d6aff84 | L6563.B0.s.p7.d22-24 |
| P1272 | 14 | 7c9402f46ce02af67d6cca78975241d51245022f6302d36880e55c9801e4e582 | L6563.B0.s.p8.d26-26 |
| P1273 | 130 | 041f62ab237c031931f59fbe5cde38832791eee7e5d13efde1ab67393f4b85c4 | L6563.B0.s.p9.d28-32 |
| P1274 | 166 | 9fa9068ae5203d22866ed53e65946bd71e357849096136b0f3be3f1c8e8458ef | L6563.B0.s.p10.d34-38 |
| P1275 | 18 | f46c293027e69861617d3ec6c6f41539b743d6bb5a3a78dec18ba468f2f4a60d | L6563.B0.s.p11.d40-40 |
| P1276 | 112 | 8f8bc516149b03585a0ab1a53284cc11cd06bb6a0989d5eea6671af7eae63b4c | L6563.B0.s.p12.d42-46 |
| P1277 | 162 | 20406ed5550ee26e6788aec97adfbb53362ff5bcb44cb8566ece4c34ec52d3d6 | L6563.B0.s.p13.d48-51 |
| P1278 | 18 | c3236d3a138a3a4394673120bfab430927abfaf2407e83aafa55b7b84b32d866 | L6563.B0.s.p14.d53-53 |
| P1279 | 124 | 067c2abb224814bcf8e8de817e2efc4afcc44f07b6722a4b4bbd15318af4be18 | L6563.B0.s.p15.d55-58 |
| P1280 | 17 | c8c887be57b000cf741f5171a2af85229f12be30dd24419e0a9a9b90661f8b0d | L6563.B0.s.p16.d60-60 |
| P1281 | 27971 | 257f88884d9a3351c61db1207b96e030bd1c21fa17664609c03fc45edccc2706 | L6595.B0.s.p0.d1-1 |
| P1282 | 27490 | 7b70e69164982061fbc28797a464a0e3ae233059fc6aa3492e50201389dc0997 | L6599.B0.s.p0.d1-1 |
| P1283 | 28092 | 72115e370534d27b1a288add45b34157f60c7bf57f3e626b13f4ed7264d8ae71 | L6602.B0.s.p0.d1-1 |
| P1284 | 27709 | 7ca6658e9a7178c2efa7430f9fef849ea1a25ce2f7268f9acc73cc8d796f31f0 | L6605.B0.s.p0.d1-1 |
| P1285 | 27630 | 8189a77fcde2f7d5bec76c90888834cf6b07266228f88884e1f330bd7a83682a | L6608.B0.s.p0.d1-1 |
| P1286 | 326 | 831a6720ae848dc7c87feac0f1376c18c3e44693c005af4bb2a51862acf2e12a | L6611.B0.s.p0.d1-1 |
| P1287 | 51 | 6f0c6d9f62afe308972d2cd039820628c5b722d44635b9b1cb4866b61c337799 | L6638.B0.s.p0.d1-1 |
| P1288 | 212 | 7509bef79a6b1a0d0d65b4c0301f98541240e13e03104829c81ed70239ff54e3 | L6638.B0.s.p1.d3-4 |
| P1289 | 45 | bd1464b6cbb9831946c5a73dc379b8d39b86427fbada3039dc002082591e866b | L6638.B0.s.p2.d6-6 |
| P1290 | 137 | a901e20846458ab1c5837f3c55fd6acd53c6d0234efd6fd39ef7adbe66335751 | L6638.B0.s.p3.d8-9 |
| P1291 | 163 | 7b76ea7c059b7f74cab24dba794e9c21482e00fda3d913e26f3c359f4b11118a | L6638.B0.s.p4.d11-12 |
| P1292 | 243 | 76c217b32caa67668a8da8197a8c5401329cbb9332d16295430bc30fb0ecb4e0 | L6638.B0.s.p5.d14-15 |
| P1293 | 183 | 3b8dfd8ad29fb807bd10b5f183af403317de39877a2319c267684c32077922f6 | L6638.B0.s.p6.d17-18 |
| P1294 | 138 | f7d380bee75e99e5d10d75fec2a281602881b16a92ab3182459f7d565241e9cb | L6638.B0.s.p7.d20-20 |
| P1295 | 140 | ca4ba2f425dbff06fd3c4c125be0e56299d77f4097e1bfb6ec2905d3fd5c884c | L6638.B0.s.p8.d22-23 |
| P1296 | 33 | 09dc1d92695a26c56594656e221c430395573b9d5311a1588e16a716e78fc555 | L6641.B0.s.p0.d1-1 |
| P1297 | 53 | 0c0f4edacbde53d7bf9b0dbcaf7552e9886cce3a0a7064b71a226d3c06a7be91 | L6641.B0.s.p1.d3-3 |
| P1298 | 73 | f031e991b9234d1b398d04bdb987f17a985c72b2bd541bbd89b75351021669e8 | L6641.B0.s.p2.d5-5 |
| P1299 | 15 | 08083d3a87be1698bb8045bd87ee44cbd3a872dc3fa5690b1d5201b7c532d8a0 | L6641.B0.s.p4.d9-9 |
| P1300 | 320 | 982cc2349e3d97ef7f67064057b59e5dfcf9ac9f5b002bb62d0dcb25e49f4b0f | L6641.B0.s.p5.d11-14 |
| P1301 | 522 | a7f53662847a2bbc1d1f161605ca3254a72f1a86f3dc6c39323506d9e02eac03 | L6641.B0.s.p6.d16-22 |
| P1302 | 167 | 362a92a567ecf3a8ad09b5f0a58e19c5039a756b39659254bd3b0345fbf4d330 | L6641.B0.s.p7.d24-26 |
| P1303 | 389 | 3cd7bd583ec7b0bbe4fab819de526cc8128bdaacd3da96868e6cff8d15888e5a | L6641.B0.s.p8.d28-33 |
| P1304 | 29 | 032b525fb082ef729f09d3ea3beba8f9a960a8bf10af1699cfab00b35d9484f1 | L6641.B0.s.p10.d37-37 |
| P1305 | 178 | f4a867be01296b44f5eb2b2c0f4272d6b5814ffbe3305fc4d3b8b66eef3fbb2a | L6641.B0.s.p11.d39-42 |
| P1306 | 48 | fea3e75b74c2a834df669cb5bf7906139832d0263110918df157f949956498d4 | L6644.B0.s.p0.d1-1 |
| P1307 | 76 | a7f7e9f0de00aa2b73aa92bd0aced875a4bd2b2b3d185a6a016aa9972f7d6b6b | L6644.B0.s.p1.d3-4 |
| P1308 | 198 | 1d0b22307c2c7533fdcc74ee80081a5dfd6afdf10f824ffa486e6e04b9e2b065 | L6644.B0.s.p2.d6-6 |
| P1309 | 268 | 898534616b34ad9a1b8c74ab18a0e9d40890cce14fa711dbc381454b3d690817 | L6644.B0.s.p3.d8-8 |
| P1310 | 84 | 56df36c3d759a0086dbe161dddf613ba1d1089b3ebeda87d6654a6b678a271f5 | L6644.B0.s.p5.d12-13 |
| P1311 | 240 | af8f52a70e427c6b27a4252238da86c228f64bdc97d40c0c4f3d06e1da19a738 | L6644.B0.s.p6.d15-15 |
| P1312 | 229 | 7e31700a2623515d73034aac71eb63710cdb33cbda750a97e0ed08ad50e5daee | L6644.B0.s.p7.d17-17 |
| P1313 | 76 | a742b3f322218f54c6f2b884000f320d50b0c4c5f706960b0d41f45bea384292 | L6644.B0.s.p9.d21-22 |
| P1314 | 196 | 12a7a6024f16ad45f1df8decfb69318908b0080b1add09d5a2961fb7d72e870a | L6644.B0.s.p10.d24-24 |
| P1315 | 59 | 283e11d343d0bb090b296fc49533c3a9760221fb01ba534c75cd391b42fac57c | L6644.B0.s.p11.d26-26 |
| P1316 | 84 | 4d61b9059959014b55f95d2d47358f464904772e413453008810506f7d97a95a | L6644.B0.s.p13.d30-31 |
| P1317 | 192 | 3d32c30cd9f9e1aec222afd3ffdfc8a0284bdea6febbfc61ef7f81483a27df10 | L6644.B0.s.p14.d33-33 |
| P1318 | 76 | f12d166fef3040d02b5a492267c686986c99a2415e4a9340ae7edb55a4cd7c9f | L6644.B0.s.p16.d37-38 |
| P1319 | 174 | 93bf63d8c46d26e807e1cde75538ccfc51824e4f9e9c832d426f2a1ce4a28a4a | L6644.B0.s.p17.d40-40 |
| P1320 | 84 | 13336e11c5f6b349cc6fef7c6b79b5c9ba9b743aaadba783b7ccefb74943da7c | L6644.B0.s.p19.d44-45 |
| P1321 | 241 | 9d5a95d10370ee636b043234d4e709e30e8336ca8db8140c3a83c862f2c7b88c | L6644.B0.s.p20.d47-47 |
| P1322 | 40 | f95426f522bfa89672446becc1c6fc5a3daf5b5dae2f487e28e10e4a87ca0f7b | L6669.B0.s.p0.d1-1 |
| P1323 | 18 | 96e7459a16b47e7fa631f5b7071194c17e01309d7401601b3c68f607c3e82976 | L6669.B0.s.p1.d3-3 |
| P1324 | 146 | 72e7a9eb93c54f466580ffa8c9ee4103c214d2288738de66926aedb7b9c50d77 | L6669.B0.s.p2.d5-6 |
| P1325 | 141 | f5bb8bfdc61c53faf10ffbfc0d360f428ac8d433ad9323ca51c2fc6fe8e967dc | L6669.B0.s.p3.d8-9 |
| P1326 | 104 | 0d4cc949c59434518152ad78ea91a0a472cdb5547d6bcd2df222409801866d13 | L6669.B0.s.p4.d11-12 |
| P1327 | 17 | 12815d4408338bf2977c5aa84312552e9e29c6859745591800261c960159167a | L6669.B0.s.p5.d14-14 |
| P1328 | 58 | 7662cd16aadbc5b545a25ce07cca3e51335c6f88f97820d6f2638cbd052f2b65 | L6669.B0.s.p6.d16-16 |
| P1329 | 153 | c369f3609f984ecf4a69b2508725468a2cf608d0b4b4a98ab820eb45649448e9 | L6669.B0.s.p7.d18-18 |
| P1330 | 246 | 123c0116ededd8e670311f61b4d0cb72a78b5285b02c830d23ee6dd24bbfb165 | L6669.B0.s.p8.d20-22 |
| P1331 | 43 | 53cbb998f70342533c0e8fb1d414bca29bdf029e6e2074fcf67ddfb950fe11b7 | L6674.B0.s.p0.d1-1 |
| P1332 | 244 | c4fadbc18e2b0e92b97c092c9870b1adac8c35b281dae3d7da04986ca62b95fc | L6674.B0.s.p2.d6-6 |
| P1333 | 208 | 321debf35d4f419cac7fe583f50f987809a2a487cbdb800c883478b40ad8f878 | L6674.B0.s.p3.d8-8 |
| P1334 | 50 | a306812d492cdffd3b059b6706d314815dfb4d2ca2d0a2fc64e90bd7ea80325e | L6674.B0.s.p4.d10-11 |
| P1335 | 414 | 253e223f9b5f27b650e4628bfaf77111eef37f5645577384f796fb4b8410ade1 | L6674.B0.s.p5.d13-13 |
| P1336 | 152 | f24da4dd25aa72d8486c733345f01ad06708ce9a26350b84323a02973f8df7e6 | L6674.B0.s.p7.d18-18 |
| P1337 | 226 | 113de9222746c1af87d951b052d1a0388a8ee186327c75ffd703e8b07bbb377d | L6674.B0.s.p9.d23-23 |
| P1338 | 258 | f6d5747ee8153ce4d77c50842bb5f2009f4ffd552f7a884266f2b55ded25e150 | L6674.B0.s.p11.d28-28 |
| P1339 | 210 | 6d4f0c085a01011118359cd2e47f2bb7f20b7dbd91e027ee4eca246a3944ce97 | L6674.B0.s.p13.d33-33 |
| P1340 | 73 | b7d29e504a5db33289bcf1a661cf0dedf26b81e780932eb4a4d5ac02bbff80c0 | L6682.B0.s.p0.d1-1 |
| P1341 | 371 | f84f36e9686693ce69ffcf6d50683aae8ae6f5dd44577c1884512011a04a2446 | L6682.B0.s.p1.d3-3 |
| P1342 | 2165 | b243bb511ef8ab5586d86f22860933dc2b9264c5086337592427a66171f6e48d | L6686.B0.s.p0.d1-60 |
| P1343 | 24 | 9fb95c1846c3a8dd351556b78100e1ddd7ef982f26c2eabb2b84f521f6379ffc | L6687.B0.s.p0.d1-1; L6705.B0.s.p0.d1-1; L6790.B0.s.p0.d1-1 |
| P1344 | 122 | 149b176129656a2183089513c4c8cadd200f65ffd6a54d5e83d69a4e382aa43e | L6687.B0.s.p1.d3-3 |
| P1345 | 164 | 9b42685dea330734f227be777d61d9799aa14933cc558587524fbb73ba0664dd | L6687.B0.s.p2.d5-6 |
| P1346 | 114 | fb0e18231f053bf16d425b28650e2ccb9e8e1ffa56f1600f9cdd56da272c1151 | L6687.B0.s.p3.d8-9 |
| P1347 | 158 | d56dc5e186bd27446767d5ff7d64c1ebe5d9027268d69b318bb8ff635e557c38 | L6687.B0.s.p4.d11-12 |
| P1348 | 132 | 099561b8d946c3cf35133bec76af1391576c93d08171f0272f5aabeb529b02a0 | L6687.B0.s.p5.d14-15 |
| P1349 | 143 | 8e9d75d33437391b30d2bd2e2a8da09460a84bf9e29d598b36099f838c4e9b85 | L6687.B0.s.p6.d17-18 |
| P1350 | 146 | 15f72da67b234248f6f217d1a7fb4a16fea6315f957ae59cb851d43d5848d385 | L6687.B0.s.p7.d20-21 |
| P1351 | 164 | ef2bf950002a1a4574dd60494e9eee728efbab1a75cc756853b0d8b561221d8b | L6687.B0.s.p8.d23-24 |
| P1352 | 56 | 0ff22ec57c74362e89c2649727fd08c57250f4a7b53ff6832cbdf7377207108d | L6687.B0.s.p9.d26-26 |
| P1353 | 474 | a63590acc9762d1a98360324a620ffc7730a260ccaec35a829e93194485e9b03 | L6690.B0.s.p0.d1-6 |
| P1354 | 23 | 1b874ffec9b225293b968840375c65840dce2832bffce572e40d3e594e6cd6f9 | L6695.B0.s.p0.d1-1; L6700.B0.s.p0.d1-1 |
| P1355 | 248 | a499c710c00e2ce28d75c61791a324eed8d296646fd92dd1fb92a8da3fe8dc26 | L6695.B0.s.p1.d3-5 |
| P1356 | 250 | 84489998c447f580caa94af6404380ee25ac7dbaf9538ff26ac8ed14c8aa450c | L6695.B0.s.p2.d7-9 |
| P1357 | 211 | a29f3536a743c3544268a4edd3d270459bdf1561ce4b546c80ef91d80c52d3d1 | L6695.B0.s.p3.d11-13 |
| P1358 | 283 | 592256d902cac34e40dbe335d2265918dd800e3859cbf461f2af3fe0a421b005 | L6695.B0.s.p4.d15-17 |
| P1359 | 280 | 531b8548641cd55d6631a78ef4d7fc2fa7fdc209a845f7d2d590700897c1e724 | L6695.B0.s.p5.d19-21 |
| P1360 | 174 | 24cd797ef726f2c42705e163b4dbd6744a2a631e542d2c2b307f4a9f791d9206 | L6695.B0.s.p6.d23-25 |
| P1361 | 260 | 178c23adb2e79be129dc942b98924bddaf78bf5445a14fdbc7a2d1876aab0b0e | L6695.B0.s.p7.d27-29 |
| P1362 | 226 | 2cfdb36d3ac34b4492c08abfda68af0f6686c5eb1fed1af2bdf5a5a71278b092 | L6695.B0.s.p8.d31-33 |
| P1363 | 303 | 19f60009671a5c708831cfc62ebd5efd1869988fde639c4b82bafeb813e815c5 | L6695.B0.s.p9.d35-37 |
| P1364 | 252 | 1804f4bc3f47ae0d6ac25b32df0921ffc32ece7daf8d2e97a4f13c9aee904a0f | L6695.B0.s.p10.d39-41 |
| P1365 | 304 | a6f61ee4efc386838df1516fe7a5761e0fad906e479b5de9d78e673df12ea6e4 | L6700.B0.s.p1.d3-5 |
| P1366 | 304 | 6b3160d1173b5682f96ccc78de16e39db87e08b563bbb46e4fb42c9368362e16 | L6700.B0.s.p2.d7-9 |
| P1367 | 256 | 420ae9d26e287f5fb4e4985511887af688d5cdf7189d1f1a8cfd19759cd341e9 | L6700.B0.s.p3.d11-13 |
| P1368 | 315 | 24326b1882d183ca687290fa6e6061278d09d9d900c0342afb669fe127197da5 | L6700.B0.s.p4.d15-17 |
| P1369 | 258 | f11b42e303ff20285dc7fa4d23220ebc761c754550930e38f617f459f1e02e5a | L6700.B0.s.p5.d19-21 |
| P1370 | 379 | a399f4ca10aa7ace5b2b538a3c08c8ede124d04d4369cc4001024af505006d4b | L6700.B0.s.p6.d23-25 |
| P1371 | 145 | a38c39ad239ea1c0d078d38ce0e623d32f93430d13d6cfad4876e1b9deb6e3fc | L6704.B0.s.p1.d3-5 |
| P1372 | 328 | b7a4f0d5fd4379ec335c506d1f7ace75788d6f82f8caae452f8537e4e31ec286 | L6704.B0.s.p2.d7-8 |
| P1373 | 72 | 946b77294afa6c1da83b4f0eba9a5ec0c44bcf1441028320a419eb02daad66d9 | L6705.B0.s.p1.d3-3 |
| P1374 | 224 | e5326eccb33ff3106d7db55cbed8e9049cd2ee5b97a8ad46bfbc7a7c51797d43 | L6705.B0.s.p2.d5-5 |
| P1375 | 161 | 7198699b9b5d56809f0897382b2f32ed71becd3d9ecd38cef7dc3c26ec47d422 | L6705.B0.s.p3.d7-10 |
| P1376 | 127 | 7a48e63170ba8f9bbcba7cbd994b7281a13274fea4944cd28f2c57a10a77dcbf | L6705.B0.s.p4.d12-12 |
| P1377 | 22 | 6e3a941be3a14d41102b54b2105b94194f3a15839d66ca543947442cd8c2b5f5 | L6709.B0.s.p0.d1-1 |
| P1378 | 19 | 3e111efa1478f46682375f40189d22e03e0259d2f8e7a7118caecd519feb8ef6 | L6709.B0.s.p1.d3-3 |
| P1379 | 24 | 5a0c24834293e7262e26b1e4918141c8c28bb4f83843736de6d4b55b433f608e | L6709.B0.s.p2.d5-5 |
| P1380 | 75 | bdcd21cf46b85fe574cc95ac20cf4d3a51e4e7985e84edee160ef8a7597af180 | L6709.B0.s.p3.d7-7 |
| P1381 | 211 | 1a5ff2fb5e43b981d0bf6f9cc96f4d00034fdfe8e141206336d3acc75767c904 | L6709.B0.s.p4.d9-12 |
| P1382 | 107 | 863ff67e0062e4c6011661eef4ad14bc778374919a3ca30d82d382abdf65fce6 | L6709.B0.s.p5.d14-14 |
| P1383 | 203 | 6d6e741162ddbca25dd7bb72c0e98bc1caed47f33895d4acd39b783bd823f79e | L6709.B0.s.p6.d16-16 |
| P1384 | 618 | 0a339b3bcf4a2692c8423ca3fc34822d3da3dd0ad417ab7181a144a3cec93abc | L6710.B0.s.p0.d1-12 |
| P1385 | 15 | e8f710bfedb44e924cc32ac16bce89d9e6dc81a68f84bf961a27bce7309f04fe | L6732.B0.s.p0.d1-1 |
| P1386 | 122 | e1d4fe0c694fe9650bf9a32dbfeae78dd15334f3edc31c5a6c09fbe9c36459f4 | L6732.B0.s.p1.d3-3 |
| P1387 | 29 | 5d453eb984b0c16cf70b331e0b3ccf191edca50e38311c105fea407eb602cead | L6732.B0.s.p2.d5-5 |
| P1388 | 26 | a48b89d6198461348b230eb5c6d60a8e411ac1bd7aee982be085290dc95381ab | L6732.B0.s.p3.d7-7 |
| P1389 | 28 | 47b993d0b4aee21ecc8c5e233abba5a4b9561acad164e269a38656172a523eb6 | L6732.B0.s.p5.d11-11 |
| P1390 | 190 | dc99d437e5b058222909c0e9723f91dc7f72f6662809371f1bc032e3302bf949 | L6732.B0.s.p6.d13-14 |
| P1391 | 181 | 0447a07219a1e7eed809d600e91951aa105ab249ffa9f74eeb9ec95b74889b1a | L6732.B0.s.p7.d16-17 |
| P1392 | 241 | 1c0e021bd7eb0cdf21c477cf6d28e22de581d04a11e9874e8f9cbd2c5de8f19d | L6732.B0.s.p8.d19-24 |
| P1393 | 198 | 2f73ac66947fbba61e496da43d8d47110be23969cc9b46f968298a105be1e93e | L6732.B0.s.p9.d26-27 |
| P1394 | 143 | d3db022fe4c044745e2e3a1786d7e10014d2b6415ee110c8f3aa5951549725fc | L6732.B0.s.p10.d29-30 |
| P1395 | 5499 | 3478d61dcfa1375e7856623c59411cffa231bf303a0afc5b489ec1da9d13bc39 | L6736.B0.s.p0.d1-1 |
| P1396 | 1112 | 3a970221047879dbad0c78ee8837d93ca7b4e704b119b13069eb648bbc1e285e | L6739.B0.s.p0.d1-11 |
| P1397 | 2 | 98010bd9270f9b100b6214a21754fd33bdc8d41b2bc9f9dd16ff54d3c34ffd71 | L6742.B0.s.p0.d1-1 |
| P1398 | 40 | 79d84aebe68ddca141dae7c8c9cef00212b349f89e563d68794e0713af8ee2d6 | L6767.B0.s.p0.d1-1 |
| P1399 | 34 | 3abecb9f0436893c6af4a123472aca838c3e5698e4343a47aa1867b01835844a | L6767.B0.s.p1.d3-3 |
| P1400 | 95 | bdf9c8aa90a7cb4784f218a343aed044cf2e8f7a17788f68864c2e828a836ab0 | L6767.B0.s.p2.d5-5 |
| P1401 | 275 | cf62ca8ab4055fceffab666344afbffc1fd825522233b6c62566e016855293a2 | L6767.B0.s.p3.d7-7 |
| P1402 | 279 | b159884887061e14f7a7a3f941b77d7c254b745b267ea65298dcce68e037b87f | L6767.B0.s.p4.d9-9 |
| P1403 | 245 | 8a781bfb68ea1ebb6101cd831b2df66bb11e207a05e2f2492f2058acdad95b15 | L6767.B0.s.p5.d11-11 |
| P1404 | 288 | 6a26a9e3dbc7b8f6482b76e98a627d4484b258df0b93d01e55687ffde77e86b7 | L6767.B0.s.p6.d13-13 |
| P1405 | 15 | 612e0947acfbaa6b1797d62824bba8cd14d05d55250e713620ae97e87385d809 | L6767.B0.s.p7.d15-15 |
| P1406 | 216 | 21d7492fbc718306706b200690be905e22d8f1edc2ddf7f61e930eeca749734d | L6767.B0.s.p8.d17-17 |
| P1407 | 44 | 153f647889a30629c67b8a1a800e94f94fe6bf37a85869ffe0012be8eb15b43b | L6770.B0.s.p0.d1-1 |
| P1408 | 68 | 29875671cda852223e311d377191959408c63c4fba2f30ae55b7def74716fea7 | L6770.B0.s.p1.d3-4 |
| P1409 | 177 | a08beea40fe762552b09bb50e8fd2513da1eed8ffa2c27a59a16a0e2ad6049a3 | L6770.B0.s.p2.d6-6 |
| P1410 | 85 | 359e55601972414098d82f993315980b2c4693d04d701d30f6982fe5b703ef34 | L6770.B0.s.p4.d10-11 |
| P1411 | 412 | e5d1e3ce504b398de713388eefa37fc554483fcb94e7b00ad609b7c1e2a5a6cf | L6770.B0.s.p5.d13-13 |
| P1412 | 85 | 77f91984361c8ed60f34a30146c1ac852d71dcbc6ac958af853b0e986b483674 | L6770.B0.s.p7.d17-18 |
| P1413 | 288 | 14d0f1cd08343ecc1e2a0ca19ba04e7af228c7ad889220e2c1a58772fcca68c6 | L6770.B0.s.p8.d20-20 |
| P1414 | 85 | 749387a7f792334d1aff5b2478060940ccfc68e757dca21bc0ea963dbbfefc55 | L6770.B0.s.p10.d24-25 |
| P1415 | 414 | b7d9f8b1be85cb077ca5aa375700474350818da6f5351a8d9a3be6cb90513b6c | L6770.B0.s.p11.d27-27 |
| P1416 | 228 | cb40e9ab1873de208b4dd5b1f464d2bbff7107f2634c6bdfc5005f8289a45ee0 | L6780.B0.s.p1.d3-5 |
| P1417 | 377 | 3f2bef4ffc6abcd84ddcefc4bb1261bc7df91f3d085e7d94e5c2bb5645906e77 | L6780.B0.s.p2.d7-9 |
| P1418 | 70 | 3b462b5c78f86b3f9003d36d3838d037327a75c68acc1c45000eb58af23a03a7 | L6784.B0.s.p0.d1-1 |
| P1419 | 210 | 88eba099a6f9ee4a7bb26cf79bfcfafa083f401ac7bff5d0ab23c134c1896965 | L6784.B0.s.p1.d3-3 |
| P1420 | 179 | 56b536b8442635a8be4bb50eb2ad3a89bdf99136cc8415e278a2d853bacd5368 | L6784.B0.s.p2.d5-5 |
| P1421 | 256 | 26a7a2e10648fd7c4f66f367f9139f132c8add35097dced9cc0d83dbae3aef1a | L6784.B0.s.p3.d7-10 |
| P1422 | 100 | 81591eb10aeb2c196cc30c1cea4c4f9b9f18162a2279917d91acda0cf9e4a2e3 | L6784.B0.s.p4.d12-12 |
| P1423 | 74 | 7b90e40350555825a16a05d003ba67c953a7881d3251a487f53ffb82b221f21b | L6785.B0.s.p0.d1-1 |
| P1424 | 234 | a6cd8b766164fe9d8f9dd18fecfed8a6116a9add51d01e0ea9ca61a8f8223f1c | L6785.B0.s.p1.d3-3 |
| P1425 | 121 | eeab3e5e8832b15a392a7612d4c1faa4b2939113804c23a6a1353982b069145a | L6785.B0.s.p2.d5-5 |
| P1426 | 158 | bddccc9127accee5140058c63e9ed15f4e98e433b0887b869a70d8a9e975760a | L6785.B0.s.p3.d7-10 |
| P1427 | 143 | edcc16832edbe020296ec10586e615a34d0026e6ef91fed7ca2f2db0c6ae1c67 | L6785.B0.s.p4.d12-12 |
| P1428 | 174 | 3e35e0ecd965faafd8dc190798548d678f1efd7c097194f6d1b7d6a27069c753 | L6790.B0.s.p1.d3-3 |
| P1429 | 16 | 4be3613d8431ffcb06f5a600db0320b6e6dc59f1657c668e92fb9ab95aec61bc | L6790.B0.s.p2.d5-5 |
| P1430 | 213 | a5f4eaad663aa19fe53f7b7ec2f4c5bc3fce17877946663c9abc7dfea1f75f5c | L6790.B0.s.p3.d7-11 |
| P1431 | 163 | 730d73ccff353bd84f8e457e99a2dec15f825a2718cd2a8bf530e9fe1dfeea4f | L6790.B0.s.p4.d13-13 |
| P1432 | 23 | 0eab42e9a1f9001ab45c1197bf16e1e55163d16f5f978035107ad8a2297926da | L6790.B0.s.p5.d15-15 |
| P1433 | 398 | 6e7a001d270f6e78d4642b2625e913d29e9031e147e51832751c40ecd973f30a | L6790.B0.s.p6.d17-20 |
| P1434 | 141 | aba1c63571e6377024d79a7df0ce3bb035b77f872750140662f7636aad03141f | L6790.B0.s.p7.d22-22 |
| P1435 | 41 | 0b2c6fbc7ebedbb75185e93671df873d47778ee02f512bb601bcd11e97d6d169 | L6794.B0.s.p0.d1-1 |
| P1436 | 1327 | 7cc9f281e1c1a69ead41320b83cdbc3f89799111f47c659b19c6beddd3f2f3b2 | L6799.B0.s.p0.d1-30 |
| P1437 | 9 | 7c96fae907589ac6413e0a3d45ea1c520e2f314f9f63fbfec1940462450090bd | L6800.B0.s.p0.d1-3 |
| P1438 | 92 | cce505c36756b43c006be2c6c8db6e686128ef906211946737bbc738c7fea7e1 | L6804.B0.s.p0.d1-1 |
| P1439 | 42 | 89549b87e59bf152c0a1c47a96164abb0db6340c6561a9818eb89bbd8ce53dd6 | L6829.B0.s.p0.d1-1 |
| P1440 | 216 | 7ead3f2719368f5df2547a6007e91af3c72fbd7c25cae4ab397e38074d1ef062 | L6829.B0.s.p1.d3-7 |
| P1441 | 34 | 54124abf3992efc45f079b0d7097f78ffee619787405e15d86af6258c31d6948 | L6829.B0.s.p2.d9-9 |
| P1442 | 283 | 93af436f11135a7c35927089c3b11fad731d9dbd23539e5091324f33666029e6 | L6829.B0.s.p3.d11-12 |
| P1443 | 167 | ed64d75ee82671a06b24a89644877577eac892e352531fa22e36daa8c7eeefd6 | L6829.B0.s.p4.d14-15 |
| P1444 | 295 | 3c6f31fc42ed4edfb407bcee56f116c3a0ece810e4d4435acf82e37ac3f81558 | L6829.B0.s.p5.d17-18 |
| P1445 | 199 | 883606e572be3c9872e1dc114e560ca7af95bc94e833d176e069aed5a044c93d | L6829.B0.s.p6.d20-21 |
| P1446 | 210 | 75dc0790f8dabefee05c89afc8a01592b8cf0bedb389ea324e5d2f116c503769 | L6829.B0.s.p7.d23-24 |
| P1447 | 32 | 4659627d3efaea28273a888196724119889afc3edb8bf05180b6555470d73d12 | L6835.B0.s.p0.d1-1 |
| P1448 | 55 | deb118b92deaad1fdecb92b06c02c8ebf8113a4fd4313b8fbeab25a18974e81b | L6835.B0.s.p1.d3-3 |
| P1449 | 121 | e801c8cdecf4669722625a08e828d6db680d8fb7e2d5514c8ae322c38ac78199 | L6835.B0.s.p11.d34-35; L8301.B0.s.p10.d32-33 |
| P1450 | 49 | dba07e46da41249f9e462c4c0c736f5f39e252b124fd92cc3da30dccaff1a773 | L6835.B0.s.p13.d39-39 |
| P1451 | 1447 | 3da8616312783d788899f2fd39d7ed36482219cb9c7b7666b6c1471ec1c6b5e9 | L6835.B0.s.p17.d47-51 |
| P1452 | 55 | 13b02841828c7ba9729b5faa3e5cfc2bba62daee8c4f3206e7977d62e4867beb | L6835.B0.s.p19.d55-55 |
| P1453 | 49 | 6a8141f02163715ef7588ec76f7389c74d6faa47c824ef089e351e12ef45f593 | L6835.B0.s.p23.d63-63 |
| P1454 | 55 | 4ca6763a84c8d648fa10488ea966fe9ccf19423ee81920a2b4f835cff7270365 | L6835.B0.s.p26.d70-70 |
| P1455 | 49 | 14266c6e889520e93f307fb5e7a3a5dee33db6d217c9c594d23a95ba9b96c7f9 | L6835.B0.s.p33.d84-84 |
| P1456 | 262 | 82d413bf11a84a50a237ab42a93b72b82ec7adc0de1834d35a62b63e2be8e26e | L6846.B0.s.p1.d3-5 |
| P1457 | 356 | 6c61a416bf9458eed52b2444d84689b4dde76885016d34fa021d00314b9d6c2d | L6846.B0.s.p2.d7-9 |
| P1458 | 75 | ffa714b0ae0b3ba3c34f2774cec90d5513a5a15954b8b6c847363e9707a55602 | L6850.B0.s.p1.d3-3 |
| P1459 | 203 | a27203b42579dd771e3148ed0e3f906ea5a95f750a2f5f3566c035d405fa07cd | L6850.B0.s.p2.d5-5 |
| P1460 | 109 | 19963d7eb477453ce757553d0a09588e579284afaa69fa0c8610950bcf8fa5a4 | L6850.B0.s.p3.d7-9 |
| P1461 | 131 | 0e875a9a025248b725d311b28a71ae5e57515ed69ad4a80c6f2379cb3f879331 | L6850.B0.s.p4.d11-11 |
| P1462 | 62 | a8151ab706cd0bb6b8cc520e7167701365ce9e448cdad2161b2b44b31afce693 | L6851.B0.s.p0.d1-1 |
| P1463 | 250 | 404375b59c65703015fa31b149a669625dee7230ebf28d1eec0d9db6b512f0ec | L6851.B0.s.p1.d3-5 |
| P1464 | 280 | 6a8853a70299953d78b9a26ec02009ad67135558af8be046ed0f4b6d3f77d13c | L6851.B0.s.p2.d7-9 |
| P1465 | 192 | eb9d9c45b4780bf463779289ae1956692b443fd7183f3260ad2f1a6d9714d11b | L6851.B0.s.p3.d11-13 |
| P1466 | 233 | 69b3dbd190332e600eb32e714804ba73d79975d8fd272a1509840fafffc0ba78 | L6851.B0.s.p4.d15-17 |
| P1467 | 250 | 1e22da126e646037b03257ad27f822cf7333984d4ca109d68dd1672665ced053 | L6851.B0.s.p5.d19-21 |
| P1468 | 299 | d2f5dcf8e92106b0f84b2e9ac692b064c16486fb311b788d26de4c8404eb78da | L6851.B0.s.p6.d23-25 |
| P1469 | 317 | 5c375914431fdf2fd1b57b9beb5b844e21fba6219ee66ab22082789eafa4eeb5 | L6855.B0.s.p0.d1-1 |
| P1470 | 36 | 2de88c4066aef40b275e8b783aa2650513c9ba56497f2226342834a4a5f8be18 | L6856.B0.s.p0.d1-1 |
| P1471 | 102 | cd83df58d4f16c0f0530a0e37da47c9e858068a50e0a4d6220ba78b7c72fd73e | L6856.B0.s.p1.d3-3 |
| P1472 | 15 | 763262b44a3b5ce5668edd97e4a74acde1476010ab7c0442d15a70c120381e15 | L6856.B0.s.p2.d5-5 |
| P1473 | 113 | 5cccc2dd7cf809f709aae9777b1fd441601880c1d0cb31c94e124e53c18d47c3 | L6856.B0.s.p3.d7-9 |
| P1474 | 173 | f1cb9754a283a66da50f74c062b1e752b50ef659cf273d4fc5dcd7fa532904de | L6856.B0.s.p4.d11-14 |
| P1475 | 25 | 47f960b76e400ee67abf53459cd88c01fa13e6b7b789fff9b5aedfa0e703d580 | L6856.B0.s.p5.d16-16 |
| P1476 | 231 | 8c1866c0f17b96ea8c23ccea061b936aa5712ffdda5e34eee9dd718f806dd816 | L6856.B0.s.p6.d18-18 |
| P1477 | 290 | 150093e6c02202bbe7732d5fa5b0668281c60ed3eb442d05b7bc488ccc97b824 | L6856.B0.s.p7.d20-20 |
| P1478 | 27 | 64d809ee8dd57ac0b344261bf0785dc6ecd68c1005725cf6081421587046fedd | L6861.B0.s.p0.d1-1 |
| P1479 | 101 | 7fe0cbe0458fbb11ed9fb7618546dc02c2415dc142114f36d732ef0b76a646f3 | L6861.B0.s.p1.d3-3 |
| P1480 | 101 | 149d5c068953702c8c84601fc0553d4995e8f5ee6372fce01e49cc586aeb289f | L6861.B0.s.p2.d5-6 |
| P1481 | 151 | 96133ded5bfd928a11cf3a5d28f4e39557f946f2961f0c8c2d665180e6901b1d | L6861.B0.s.p3.d8-9 |
| P1482 | 182 | 2b414645a2462ade3bae0bb442db79c36a3857b23544a112c550bf4d7f46f80e | L6861.B0.s.p4.d11-12 |
| P1483 | 200 | f711cc48b5af849f70b49b8390ed44da1a6d7913fa328423a7337b44063a1d3d | L6865.B0.s.p0.d1-1 |
| P1484 | 106 | 531746c118bcb4bc35a06590fe8e275b51460b6512d4625afa0fdcc9597ae4a7 | L6865.B0.s.p1.d3-3 |
| P1485 | 311 | 5e0828410eecedc6e9c37ff99818f03fb0b03816f18f0480244fe432034ca81a | L6865.B0.s.p2.d5-6 |
| P1486 | 235 | 4ac1e2e4a9dbe1f0ba85bc30db96f8f4e461c5e403ef974fa778822ce1799114 | L6865.B0.s.p3.d8-8 |
| P1487 | 166 | 8cf3751fc7ecf884853a792c196ebd7afccefe5a0ec15a5f03e6cd2813e95765 | L6865.B0.s.p4.d10-10 |
| P1488 | 51 | 7ac445ae8b1e77b9e75a2f59fd754e225e5eaf501c320430fce378936d49ad96 | L6866.B0.s.p0.d1-1 |
| P1489 | 174 | 1567467d934feef005ae40b858741e12c15101ede68365f37078b898487cb876 | L6866.B0.s.p1.d3-3 |
| P1490 | 210 | f3979812af2c28ebf876eda7f12003c9d12e772bfd3fc03946b0b97328498002 | L6866.B0.s.p2.d5-6 |
| P1491 | 140 | e762f10d332a8988a8b179dc76cb843be623bc46794ae512dd2c690448f98f57 | L6866.B0.s.p3.d8-8 |
| P1492 | 196 | 6753e8bbeede32085f394412f7601f2ba9aaa9db803b84c2043c19f800e54cc2 | L6866.B0.s.p4.d10-13 |
| P1493 | 265 | 76df651192a86e235acbd6c5e069bc07d10b297032ea1a992ec0285e5c51a4f8 | L6866.B0.s.p5.d15-15 |
| P1494 | 150 | cd1d63c7fff5b1db86716a59f2bbf6e109feaf5037b8aac3a35bb1287c95be53 | L6866.B0.s.p6.d17-17 |
| P1495 | 8329 | b35d08bdf23267e6f8b23045b9def330759a5f4124d9b9fe4bee6ce4ed2718b9 | L7116.B0.s.p0.d1-29 |
| P1496 | 181 | e34ab32aea6b3b26a6f585837948b4ea1c26f427b0ea42b858cdca3d5cb67639 | L7121.B0.s.p0.d1-5 |
| P1497 | 389 | f7a4bd5184496493a731c56d6e2115625f0ca4c51a37c2fbde04afce44afee4d | L7122.B0.s.p0.d1-4 |
| P1498 | 6278 | 4f159127214d1630ef7afa00b9d31b24096f2d42e8e327d2a36eff0d1a124e4f | L7127.B0.s.p0.d1-36 |
| P1499 | 229 | e8ab1a3d675c30866673e5ad6bb294a766393fa34607598bbf50585eafc7316b | L7128.B0.s.p0.d1-1 |
| P1500 | 108 | 4eed6378a1c496f119a29f019f7fd6163853a179ad821d3e1ae7079386eea47c | L7128.B0.s.p1.d3-3 |
| P1501 | 110 | 3e850f93ead3af89f1054cf34580331829e0c082f2f662764b9b4875ea532bf2 | L7128.B0.s.p2.d5-7 |
| P1502 | 232 | ae44333dee260a7cd9fe2b0cc62f5b1eecbd4c20a5eeb6473295f727d260dba4 | L7128.B0.s.p3.d9-9 |
| P1503 | 1323 | ad9759200a6ad2f670606e0734a455c5aa9eeddd5f72fe3d7c4855c4b7ad711c | L7132.B0.s.p0.d1-19 |
| P1504 | 62 | f82229cac884569b54259c727db5b36059121612bc1a334ae873052443a8c7a7 | L7134.B0.s.p0.d1-1 |
| P1505 | 1214 | c3246c0fc2a549996473fcbb9f27185268baaff955b4eb71d13c5e304fd57d42 | L7138.B0.s.p0.d1-14 |
| P1506 | 232 | 2a0084102cbdf971b5c4392df3f8dcc81474aa6a22c77d66cf0f44a06a33ec0f | L7144.B0.s.p0.d1-1 |
| P1507 | 112 | ac8fe7cd2eddfae8e9f408dc3eca0f11e4609a5aeff6bb0e481b81db8be001cb | L7144.B0.s.p1.d3-3 |
| P1508 | 173 | 3914205771d2c1de748434b5cabd308d1bece8e7ccb22e5bcd51befbdf3ccade | L7144.B0.s.p2.d5-7 |
| P1509 | 175 | 1f6ce9b339f724d1718f1c6bff9629be8fadd453ef8c2d0b1b08ce8d7ac5ef6e | L7144.B0.s.p3.d9-9 |
| P1510 | 251 | 580f21c1f749a321aa5632912d5b9f62cd0e42fd0d0060a80fd59a6ca8e8d03a | L7148.B0.s.p0.d1-3 |
| P1511 | 410 | 88f8bdea6c53ac72875021ef4563b20f87df5c5ba7b2bb9f9229679ebbb44419 | L7171.B0.s.p0.d1-22 |
| P1512 | 152 | 0aa86cab5976158d1b1b4275fd477c3f42943ed28dd3e22cac3626411a65fe36 | L7174.B0.s.p0.d1-6 |
| P1513 | 3577 | 8f8f90c4c8c76ac1bffe489c8c1dfc1f3420c5a86eb2190ed329d8d7cff30f60 | L7177.B0.s.p0.d1-1 |
| P1514 | 3480 | 64aafcd58f3179646f3cf3708d8be73886c8a72b220ea9ffab587f437f8fbc87 | L7180.B0.s.p0.d1-2 |
| P1515 | 3484 | 9de196ca0bb4b0d90db03db9065e762b1743889ee30aab3fb626d91efa233856 | L7180.B0.s.p1.d4-5 |
| P1516 | 30 | a92d2698bbecf7ae4658e5ec5e4f843e490010f0d852a21c58e870ee47280004 | L7180.B0.s.p2.d7-8 |
| P1517 | 3478 | 844203660d8c0847c634070cd3715f4174163054ae990fd89762fd49567f94d1 | L7180.B0.s.p3.d10-11 |
| P1518 | 1226 | 480a7515ace3eae4c05c9bba9cb125bb185989185e93b778ff142d487a4d669f | L7183.B0.s.p0.d1-2 |
| P1519 | 1230 | d6891eacc2b0aa25723c43786a9942e699c3a9924d157436a0c7a326c2838783 | L7183.B0.s.p1.d4-5 |
| P1520 | 1221 | 91b3dc6ae380fa79fb01cff97d638e2ff62239f8ac8c46b2a06b81f2798fb851 | L7183.B0.s.p2.d7-8 |
| P1521 | 1224 | ee5f40da9d13cd15ad760bae1696aa74cdaf51f4d1a3e9e822347f356a594f02 | L7183.B0.s.p3.d10-11 |
| P1522 | 1221 | 8310ba8d8f2ff61e137a75b70b8647f09202759086dd7268ca0c95fb8420ec05 | L7183.B0.s.p4.d13-14 |
| P1523 | 395 | b584ab7f6359174400f1f416086670695935eb86125663eba5eb859c76712503 | L7186.B0.s.p0.d1-14 |
| P1524 | 1117 | 3da6800739fd18d5aa502e16fb627d0832480b81dc89550fd4a0b82fa75867b5 | L7189.B0.s.p0.d1-11 |
| P1525 | 459 | 632003594d4ee5d789040ea8ea61fa8b7b19ab1e0e8c3d86d0405c8d7cdcde09 | L7192.B0.s.p0.d1-4 |
| P1526 | 278 | afe4b62528e3f09cf46241fe242b8b3d521c16d721541a1a579842ce5b1936a1 | L7192.B0.s.p1.d6-10 |
| P1527 | 3 | 27badc983df1780b60c2b3fa9d3a19a00e46aac798451f0febdca52920faaddf | L7195.B0.s.p0.d1-1; L8178.B0.s.p0.d1-1; L8284.B0.s.p0.d1-1 |
| P1528 | 1769 | c959306d1905e6145d29e546080eeeab7056857ca11958244f560456f2897993 | L7198.B0.s.p0.d1-14 |
| P1529 | 23 | 149532322cacbdbba0e18a2ff93250c316369bfa108abe7d38ca0a78c050f0d4 | L7201.B0.s.p0.d1-1 |
| P1530 | 1845 | 43ce217cac7b696e5c5ab868928fbb7d80e50027c30857c2771fd9b00b9b368f | L7204.B0.s.p0.d1-2 |
| P1531 | 1836 | a38ee8f4926d381170f6bdca789705d9adeab9c1162ca64360d94a7651b147c3 | L7204.B0.s.p1.d4-5 |
| P1532 | 1841 | bf3ef8e28958f31c48b49ba81030e12648a6b20dbf7abf7d0be63a272b62a5b9 | L7204.B0.s.p2.d7-8 |
| P1533 | 2736 | c9c3d6144d5b277e727bfe6aa6f9858010977ef3cecb7525aeefa71a9095d535 | L7207.B0.s.p0.d1-14 |
| P1534 | 162 | 2b43f226b8f7c070d587375bc030065e719ee8913f9ee46770723bc34494afc2 | L7209.B0.s.p0.d1-3 |
| P1535 | 168 | c593d9c63f80ea248df4dfbc0f470bfffa9e99e581e864c80a0dec8525433c06 | L7209.B0.s.p1.d5-7 |
| P1536 | 21 | f00979cce68200dc7213f60789c91064352d54303d7b86eaa4b562dcb466cc2a | L7209.B0.s.p2.d9-10 |
| P1537 | 743 | 8b12d16631cd2ccdb5bf5b376e3659acf291121defd5a85b9a14f623e4f45ba2 | L7209.B0.s.p3.d12-19 |
| P1538 | 213 | 7db5bfcc4768d695f3443ed7eee18ee7e06d525f0cca13c79fc526b88c878169 | L7212.B0.s.p0.d1-2 |
| P1539 | 1024 | 29992e29df809914adfe2b2d093ace2507cad32ade552f343cfdff293dd59432 | L7214.B0.s.p0.d1-17 |
| P1540 | 449 | 1def391a173bb0ef7e2806c610222b331a40210cd163279c785c9af383b6359c | L7217.B0.s.p0.d1-16 |
| P1541 | 44 | 9a44d25d2ce98d28f2514e5b1d67a72e358dafdf502f4c3eaa4917c1c13f08de | L7220.B0.s.p0.d1-2 |
| P1542 | 2433 | 3a359aad8efa503feae0509db86e6d3ba6043399ce296e2519765aed9c10d91b | L7220.B0.s.p1.d4-46 |
| P1543 | 441 | ce6278e882326ddfefebbc0f736f1b84e519ec1feaa5d538d40b363e4a3ad764 | L7223.B0.s.p0.d1-21 |
| P1544 | 337 | 9b317a0e2f0cd649f78a086bafc48877660804ceb27c5020c789c51b940e159f | L7226.B0.s.p0.d1-10 |
| P1545 | 115 | 7578f3e729ace0cc433d4582dd0b97489e1b536ec2b6a9819009428ad1b7a726 | L7247.B0.s.p0.d1-3 |
| P1546 | 3000 | 570f573270d0d2778b1df6494ff773720e07dc98346e08cc12171945ce29c120 | L7248.B0.s.p0.d1-1 |
| P1547 | 9000 | cad383244aadcb70abed60a2a15408f13f42a02de78a7302c719c0eb0a93884a | L7251.B0.s.p0.d1-267 |
| P1548 | 7000 | 8b5ebda3665a85d3fc26d0d846b23f94c2fab98c6e269763616f5b9a3216878d | L7254.B0.s.p0.d1-87 |
| P1549 | 1183 | 8416b531e95a2195c3e4f07b7566793473102e2dbd74df4579a7bdbc181fb5d6 | L7257.B0.s.p0.d1-11 |
| P1550 | 933 | 65f7e5b860a15c51c18c23fbe602874407417da0921daef0087aa4f9e6d8aca3 | L7259.B0.s.p0.d1-12 |
| P1551 | 667 | 6a19a122b3d01e4fc682c0bf3e79bc7e85faec1ecdad9dc6435022bce1a76182 | L7263.B0.s.p0.d1-2 |
| P1552 | 654 | e177eacc2d23c26a1afa4c988e88fc8a42dd9dfe355002bf856d6018f568eb69 | L7263.B0.s.p1.d4-4 |
| P1553 | 682 | 4050fd274a326ed1bb6922b7e6d6a4023ba2a896dc477a68bc959dbf68c0ad0a | L7263.B0.s.p2.d6-6 |
| P1554 | 638 | 9f80fd9ed518159c33026a75d763a2dc69b60fd8c5c26f29d62c26c5fa39a45c | L7263.B0.s.p3.d8-8 |
| P1555 | 204 | d3fbf7e9593ab558c8f030b416b868870ccf806c7739e9113edae12956ac3de6 | L7264.B0.s.p0.d1-2 |
| P1556 | 522 | ce83d8e123d4a3eab0f969baedccd0d024bdd19e886e34be6727bf30cbe08933 | L7267.B0.s.p0.d1-3 |
| P1557 | 519 | b228eab51004e0409597f4175098257110274ac9df13ea27be95fd10365b98f0 | L7267.B0.s.p1.d5-5 |
| P1558 | 521 | 6d80ede3b33788c8335662484caed38d2b6ef17deae0fa362800d130c8df95b6 | L7267.B0.s.p2.d7-7 |
| P1559 | 523 | 3a23f66f8c5a93692b1b87ea1168877e8c36500e7022a9ebdee249417876bba0 | L7267.B0.s.p3.d9-9 |
| P1560 | 479 | 4ef2ffcfb51d586e7602847fd5f0a583f8f146075b218d6f49d4f5a314869c02 | L7267.B0.s.p4.d11-11 |
| P1561 | 509 | 407cf5e5e2e9460875db4ca0ffc50a48f5d8657ba9c874b92a973f3108d325c9 | L7267.B0.s.p5.d13-13 |
| P1562 | 517 | 90847941ec82d5879d3958a1d65d7a0f64dc57c919a48f1bb7cfba40e052275c | L7267.B0.s.p6.d15-15 |
| P1563 | 519 | 6c06dd7e7f197180efdc89e0c1914ce20b48c62d2a75638fe980d743006d099a | L7267.B0.s.p7.d17-17 |
| P1564 | 549 | 55ba6017702361d4a18facb83f680f40135cca212ece91525a13654d4f339106 | L7267.B0.s.p8.d19-19 |
| P1565 | 551 | 5db49d95cd8b04cdb5fa6ad753861603ed6c503fdc1af52fc952b338320ce336 | L7267.B0.s.p9.d21-21 |
| P1566 | 493 | 06d59168018dae387bd43804303f6366248c1dbae66ba554d62eea7f28d6677a | L7267.B0.s.p10.d23-23 |
| P1567 | 50 | 24517c8f29ef3e1e8371e25d069313be6c0fe34d2ff02e6e3ecd2cbbbe1e7f49 | L7267.B0.s.p11.d25-26 |
| P1568 | 345 | c8a18b0c8df9659f143d48665ec1acf181212a5b2eb3bc909da34d082eca0532 | L7272.B0.s.p0.d1-4 |
| P1569 | 157 | 0fbb96f5de837452c08a30395747543fed8ae6fd396c2ddcbef6063bf78d1ef8 | L7280.B0.s.p1.d3-3 |
| P1570 | 66 | dea45fe566ac56910b8284b881aacee095c2fda561f63a69d33e2f8e3834e9b0 | L7280.B0.s.p2.d5-5 |
| P1571 | 313 | ffa2dcfe7473caeb6eb746f2eba3dc55d6318823cb1c214a06cf23e5e7633c99 | L7280.B0.s.p3.d7-13 |
| P1572 | 448 | b30f8614b4d61d0d11d4e849fc6d8bb1c57e8cffa25d1be00c15a5ee30b175ab | L7280.B0.s.p4.d15-15 |
| P1573 | 14 | f0f7df727c265842b867f89546dfe6c63ed366584f341baa007a4c24d918f721 | L7281.B0.s.p0.d1-2 |
| P1574 | 147 | a14f2c5634ab5fa6adfa4a638d25d0add51eae5124d82ddf4674049cc1e32587 | L7285.B0.s.p1.d3-5 |
| P1575 | 250 | a18eb4ce4a467e782b98badc854f00b7b90d16c02bcdefbb608887a63311eb17 | L7285.B0.s.p2.d7-9 |
| P1576 | 2737 | 39a60c1d058949f0b3aa20b5d3d8f7b86699737b15c6b1556f209b046502f296 | L7286.B0.s.p0.d1-76 |
| P1577 | 11 | 45d6fec5c6250168dad51a58b600b03cf4a2cb6073285d3ea5911b5667ffb203 | L7289.B0.s.p0.d1-2 |
| P1578 | 11999 | 77600548909b605a094d9eae4991b6ad486281ed4254bea4b50a98684e786671 | L7309.B0.s.p0.d1-1 |
| P1579 | 8000 | f1fbfc63d8aa92783f4e712efa8265172fcd5b82f5c8996b005e86d1424f1c70 | L7313.B0.s.p0.d1-212 |
| P1580 | 592 | cf72017ef8dcfe8b32b9193e58d3502a1c259757db2cf77fe0145b1b50220ffd | L7317.B0.s.p0.d1-15 |
| P1581 | 62 | c1c6c9e34b21ace2c490c1221a31f75234cb3d86b5b8697d752d797d528d737d | L7317.B0.s.p1.d17-18 |
| P1582 | 542 | 7ee5091039f0437e7d599393098a2bdfc7b83c76d4c718a35a2444ed56040611 | L7321.B0.s.p0.d1-5 |
| P1583 | 468 | 07344ee49a428f0753a51a0e4217f7ada648e7e78c300d9b786d60cd904c2570 | L7321.B0.s.p1.d7-13 |
| P1584 | 397 | da62978954c2d7413a5afb68972e531ec77412ff8789dfb989c40d040c9559f0 | L7325.B0.s.p0.d1-5 |
| P1585 | 408 | b0ae7feed05e71cfed134508f46e515080dfe641c0e3c3fdfd89ddee9517af2c | L7332.B0.s.p0.d1-12 |
| P1586 | 20 | 819b9eb15354840736570fd028eea8e6de4b04bbd802c37ce6eaf8029b96c065 | L7353.B0.s.p0.d1-1 |
| P1587 | 21 | 50622619210a1165d252e9e2f79534d1aa294e6b8e269d7014d1d9a37b0db642 | L7354.B0.s.p0.d1-1 |
| P1588 | 160 | 9e076e1dc8619f99a69a75df1abc957526ede9ec14c800e289483b72f2e4a6cc | L7358.B0.s.p0.d1-8 |
| P1589 | 16 | 6fe5b573a68e43c7c3f5d8c572059033480b4c2f164801eb3f7fd2d4e8b4b9b9 | L7359.B0.s.p0.d1-1 |
| P1590 | 65 | e584202e2f50c3b43c00a2ad956404b7010c321df1293f15bfbaf34ff3b7ff76 | L7362.B0.s.p0.d1-6 |
| P1591 | 885 | 748fb8dac7cee3b83e846a188a31b163b660dcd1bc65db1dd8ee9fd3b880bb70 | L7365.B0.s.p0.d1-12 |
| P1592 | 630 | fdd4ae6911e459c16a2f98094ba9852b1ecc56c8f1bcb5b567fc3d00a2c78409 | L7367.B0.s.p0.d1-11 |
| P1593 | 1148 | f0c9f39448aa1d96fc8ac708263831d6d8ca03c338d9b0a308eeff44252b207c | L7371.B0.s.p0.d1-19 |
| P1594 | 369 | 1a95bcec4c42993dbc8bc4e6ced4c8822ed30256e751e4e22c62bb93fa5e7ddf | L7375.B0.s.p0.d1-14 |
| P1595 | 2539 | e4e653f724bfe3097f0061776e86e7939f6888ce3da6f9ed7bd3098e9b02910d | L7379.B0.s.p0.d1-34 |
| P1596 | 200 | 1fb269751aa41ed96ef1287c3bd7165dd7b5659f2d7ea3c4f2963c1989d5b7b9 | L7383.B0.s.p0.d1-7 |
| P1597 | 19 | b26eea3e108691c448076eb90e37c68f7259d1fc4f776c8eb29537ada20a8ff2 | L7387.B0.s.p0.d1-1 |
| P1598 | 520 | 10f9f3e89d97077d2848cd89a2f8b100a0599a372b9e48e9db6111beb585d2d2 | L7389.B0.s.p0.d1-7 |
| P1599 | 595 | a32892e51a5245fbb3011a754d9bb604106f560701f71642c4fd04795b0dcefe | L7397.B0.s.p0.d1-10 |
| P1600 | 109 | ebed4cb73206a02e12399234d76366c0cfd35581a0b2a508e64a5861fed4714d | L7397.B0.s.p1.d12-13; L7492.B0.s.p1.d11-12 |
| P1601 | 180 | 1b1dbf442ec2abc91db9e778df026fedc8d3ed9c2f65a6bc4139dbc30a56a280 | L7397.B0.s.p2.d15-17; L7492.B0.s.p2.d14-16 |
| P1602 | 598 | b8e5460beab3b763cf3e536bc9c85fa568ed52985a74cc92df5b8e56b5159c2a | L7397.B0.s.p3.d19-20; L7492.B0.s.p3.d18-19 |
| P1603 | 246 | fccadbc31298679812a95fe8603787f2bd4eae143f617aa9252bfa2bb2d8beac | L7397.B0.s.p4.d22-24; L7492.B0.s.p4.d21-23 |
| P1604 | 331 | a02544a77e952be49263c04cc099d159ef27d2cb6d550d57fdd7595e1668ef36 | L7397.B0.s.p5.d26-31 |
| P1605 | 97 | 1089864308d48d5c66240d4c0f9aa7df9ad0e17a9a3f675f091ad27262a8eed6 | L7397.B0.s.p6.d33-35 |
| P1606 | 101 | adc71c9fe80c6d1bbf29b9dcdbd4e5b6a8aa58e447948540719916c990def9f5 | L7397.B0.s.p7.d37-39 |
| P1607 | 97 | e4f410e18e1b2ddd8ecc6e91b4d708129e1e4a1e11b88411d61038cfa8a17c2e | L7397.B0.s.p8.d41-43 |
| P1608 | 96 | 4ad95ca7c2a40e7a31ca0f8bfef61aa70ab5b57ff0694289864344e0b0cd13fc | L7397.B0.s.p9.d45-47 |
| P1609 | 111 | b73bfe898a649ffd36b6f91a2d649ec9052cb9d00d5dd57600fab6642c063f9e | L7397.B0.s.p10.d49-51 |
| P1610 | 138 | a779cc8c02e1b60f2c0fe77c47b2597635f2398b4b41094b3a8f299743e1c7c9 | L7397.B0.s.p11.d53-58 |
| P1611 | 361 | 895d79b4b0177db25d59eb78a4902754a7e438c2c42693e6f12ede184722f4f8 | L7401.B0.s.p0.d1-9 |
| P1612 | 275 | 53e56a09cf831825d8fd1c6dadf60a0f4a68d5e06d812693c0e589161906e17a | L7405.B0.s.p0.d1-2 |
| P1613 | 33 | e4aaab269f7ba37af39ce51f15a976b515ecef93025f1cf43b0ee28792ffe58e | L7409.B0.s.p0.d1-3 |
| P1614 | 24 | 0a829a7fac227b26fa60b82d0c10cea95e10fc33ecc22e4f3ac2da36a6add1ef | L7412.B0.s.p0.d1-2 |
| P1615 | 251 | 2ebe708577f171dc2069fe333a3115f7deca4fef7cbe7ebaccbbee8d19ea7ad8 | L7415.B0.s.p0.d1-7 |
| P1616 | 775 | 911fdf077b5d46051ded1e3529af2bb09f6c8f7a95333c7695aacd39e64f996a | L7419.B0.s.p0.d1-4 |
| P1617 | 129 | 5b48304c5c3ab460ca9d097e097f7317246d9f151d6a8ca4fae62f2c97c8befe | L7424.B0.s.p0.d1-4 |
| P1618 | 149 | d7643ad476954aea01e8a34ec1e62a653b10c5eca89e46a38066b16968cd411f | L7425.B0.s.p0.d1-6 |
| P1619 | 182 | 29beebcebbd2460eac88add218739450a519fa513ba2f74a7d365dbb8fb2f767 | L7429.B0.s.p0.d1-7 |
| P1620 | 91 | 5f68e41b9c95d60472343cc5923fc94950a25dd74655b688796571e8e439df4f | L7429.B0.s.p1.d9-11 |
| P1621 | 19 | be1a010c22d76a7d92dd59ae4e409647fccddf596191b15e7eeb6d362f75dff6 | L7450.B0.s.p0.d1-1; L7534.B0.s.p0.d1-1 |
| P1622 | 20 | 69e4c8e3ca641c4c6b5eefcc60b4ed193b5f93dac791568988f13469817b66b1 | L7451.B0.s.p0.d1-1 |
| P1623 | 133 | 68c4a202179883dc96f2836364faa3246afd9c1af5b931d5ed3d5cb57dcce99f | L7455.B0.s.p0.d1-6 |
| P1624 | 2277 | b6daca0dd0f8313a1cb863150cecb71e8b0d9561a8c88c13ae5d1b6dfadff46f | L7456.B0.s.p0.d1-1 |
| P1625 | 812 | 33a753d0747d30ef104b097e6d19f3dadc2bb3323d433cd6b83d22351708e924 | L7459.B0.s.p0.d1-11 |
| P1626 | 998 | c3817890db4cdb8c14731eaac1d64399391071d55a9202af49829986ded39cf8 | L7461.B0.s.p0.d1-17 |
| P1627 | 435 | 17720ce474c794e9a9de0c382e9d4a5730d8c107743b676729ac1eb6c3cc8f99 | L7464.B0.s.p0.d1-11 |
| P1628 | 337 | c95f349c8708593c825932d08ba1ea70d19ee42b4b462a5c80d5cf4033ac13c9 | L7466.B0.s.p0.d1-4 |
| P1629 | 1179 | 2eff8076fe7a9bf1bdaf48fd5ddb297548da686ddf797cff894d0875057591d0 | L7469.B0.s.p0.d1-19 |
| P1630 | 1737 | 92e53a28ac14707ab1ed33261937e041ffb7fd44269b29400843f8351472395b | L7472.B0.s.p0.d1-14 |
| P1631 | 645 | f711d2addf537aac1ef00cecbedb9c2109a82a8b67d0c5605a429079d8c7632b | L7475.B0.s.p0.d1-12 |
| P1632 | 1308 | d0d2363e170937f56e7bf57a4fb6b03b8a6aff8fba651e5c91440bd8a0f0a2be | L7478.B0.s.p0.d1-17 |
| P1633 | 460 | a7596178f8745fd66ed7ba4a81ad6ab8eabcb7b2d174a70e58ae84ea3bbd46db | L7481.B0.s.p0.d1-5 |
| P1634 | 349 | 81b468f79bda0c59e9d4bf40b5c3d0a143054a00dcf92c80056657f1499aa5db | L7484.B0.s.p0.d1-7 |
| P1635 | 2624 | 86a51d6f82415dfcd5391a978a97b4f9dc039e8d65312b7e365904760219bad4 | L7486.B0.s.p0.d1-40 |
| P1636 | 1139 | cf432cec527c4c68aaac6b1f6fbe832a04c2030385e3e040bb4af65605ee8502 | L7489.B0.s.p0.d1-6 |
| P1637 | 553 | ce4895a5c0faa4cd5a4b1216a862b872aefb93b70e6847b76e7782de42b1dace | L7492.B0.s.p0.d1-9 |
| P1638 | 695 | 49055f1df91c8082a4df74b19d24003decfffea7fced298fa139aa475f274007 | L7492.B0.s.p5.d25-35 |
| P1639 | 115 | 2e265f24e0a8d83063072eff06f0eda527703a6f6693efb95602e57299b3ab87 | L7492.B0.s.p6.d37-40 |
| P1640 | 3189 | 980724022ae383e2b46c380be33eac93580d3f680f83122e232b2b2511f416bd | L7495.B0.s.p0.d1-19 |
| P1641 | 974 | fa847d8266915d73c110850db12b54b765d0fa97af725bdb834329d5c83ba038 | L7498.B0.s.p0.d1-7 |
| P1642 | 5500 | 6a797b4473c70ccfa755dc075c15d26c1b7eab9db4bab08a03d89e79bd34b3d4 | L7501.B0.s.p0.d1-1 |
| P1643 | 398 | dcf2071f1ea5d45f764690b7760553bf359a7ac50cf404799458b7b5cf142ac6 | L7504.B0.s.p0.d1-10 |
| P1644 | 17 | a74f9aa1941ba507da00e11bef0479f933c56f065efc1bd0b7db0622d3c5db00 | L7506.B0.s.p0.d1-1 |
| P1645 | 123 | 08290b680f18fcbd4dd48e0daa5832dac24f058878a75c2745453bbebc93cfda | L7508.B0.s.p0.d1-6 |
| P1646 | 1214 | 2afd30d53b3afc9f2e9c1fd12194766cb21aba1fad90950fc23ef6312606bf7a | L7511.B0.s.p0.d1-10 |
| P1647 | 427 | 4c81261f4b9925af5528f4cdb461c9ac1d1e492996d66a8a188f4369adb465b9 | L7514.B0.s.p0.d1-13 |
| P1648 | 173 | 6eb2fbf3bb2a27512bb1e887a93040eacad5f59104bde7562dd6d25da1ea2279 | L7514.B0.s.p1.d15-15 |
| P1649 | 19 | af8baa7c7d0a63102459c727793f0f3e051c171f16da2c34448805673dc6d4f1 | L7536.B0.s.p0.d1-1 |
| P1650 | 2282 | 0e1a4b4b276a618b7e27039946c524fe88e8f755bcf244a5a90b395dd2620c75 | L7539.B0.s.p0.d1-2 |
| P1651 | 124 | f9382710e52742d66950997aab4a37f8c4dfedfa695773879d67e9575634fee4 | L7542.B0.s.p0.d1-6 |
| P1652 | 798 | fc5e2516d5f104bad9735c0933da3c58c99feca88186107142a00d3b3639b297 | L7545.B0.s.p0.d1-10 |
| P1653 | 520 | 66b476eac0c7bae9b5ba2b382adb5e0bddaec77832d6a318b179fa4c32d103af | L7547.B0.s.p0.d1-14 |
| P1654 | 122 | cdba0ebbf925bfc377982921d5caa0db3d2ab9f9ce382825a76ba89f1c21c9e2 | L7550.B0.s.p0.d1-2 |
| P1655 | 438 | a67f1f765d95eab145976b46ebec0b5d94dda0a5b95eefab7c88557e72872dc3 | L7553.B0.s.p0.d1-19 |
| P1656 | 394 | c035e4041c7904f87a54298a31794bba90f096bdc4da2f87a34aaaa263536e52 | L7557.B0.s.p0.d1-14 |
| P1657 | 1382 | efc0df76762a53b81fc7b5af24ad147898d20a80c481d5cb6fb2aff19c616122 | L7561.B0.s.p0.d1-19 |
| P1658 | 204 | 0a41c23977b31259d6289a51004fb46b55d0202491cbda85ee85cab5d0e624e7 | L7562.B0.s.p0.d1-12 |
| P1659 | 135 | 033023fc7921b60d3935a9f80c8612509a6da78bfaccb2c1a92233ac9da715e7 | L7566.B0.s.p0.d1-4 |
| P1660 | 1077 | bdf0e54fa25ffcf10b7d9e01d0db8f06f74fff49f1bc39bb11412fbbed3dd6a3 | L7570.B0.s.p0.d1-12 |
| P1661 | 329 | caf61a68f667d3f48d4d0094940c301bc967f62bb3deceef9628db64ebe2daf8 | L7571.B0.s.p0.d1-10 |
| P1662 | 2534 | f5a62995be4387dc9cab933fc6ba5afb3b25d47e69c1644e62d54ebe2358b373 | L7574.B0.s.p0.d1-33 |
| P1663 | 642 | 2e6cb01a98283199fd4c279fc11162b6d75d7deca1cbb1d8bd0bc84ef58c5e33 | L7578.B0.s.p0.d1-4 |
| P1664 | 117 | 39798b53b161f1b87f93b92e9fd501aa19ff07764b9805bd3dbb69ce5ca461d6 | L7582.B0.s.p0.d1-5 |
| P1665 | 20 | a8dc240258db6a84780993224961fd376c85cd8697c06ffc48307d26e699045e | L7601.B0.s.p0.d1-1 |
| P1666 | 8997 | 4b896fd049dfc2cf12820653a1d097872caa4336842d7c5b87fe6ed1c0644f7e | L7603.B0.s.p0.d1-256 |
| P1667 | 7499 | 4d5de2864e49285033693364b0f543b04b129b4ffc6a6c90c86fe5f569de831e | L7607.B0.s.p0.d1-98 |
| P1668 | 182 | b62e9dc1a4b5b49f79c92fa7294f1d5722143a1758d5dba3ead9c0e036239c5c | L7611.B0.s.p0.d1-4 |
| P1669 | 6043 | dd1ed21ea4358b95868d14d622c385971ce28ea2aef41c0b10ad0a6488aeb387 | L7614.B0.s.p0.d1-187 |
| P1670 | 1494 | 6ee1fe028faeff8f0590eee671b8536c1c384f75f1134bb828b37028deae431a | L7618.B0.s.p0.d1-11 |
| P1671 | 3555 | 18da69ca47c84cd11db9eff0026f842b54449848ceca5a7085e3735eb257a2ca | L7620.B0.s.p0.d1-112 |
| P1672 | 3556 | beaff8d0e00a13300fc1d75186b55a0cfa532412a94faddb7b91ef0f9ce2dc9a | L7620.B0.s.p1.d114-279 |
| P1673 | 4795 | 12dc151020fc11c93573b7061951ebad31cba35b2e087b4fbc65b78a7c155c92 | L7624.B0.s.p0.d1-8 |
| P1674 | 68 | fb9a97d9fe41ca52de29ae2b5e27ccf6e4385dd7cc979b733550d89da8009f58 | L7628.B0.s.p0.d1-2 |
| P1675 | 182 | 584ede57e8dc66230d809a94da0290f0b9af27447bddafeb591788fff6f3e5b6 | L7628.B0.s.p1.d4-10 |
| P1676 | 555 | 9730d702e7bcaf62bdb625b4b0a773e5459c5f2b4d1b0f7c6897f5a2fa61e638 | L7631.B0.s.p0.d1-5 |
| P1677 | 868 | 525e9b692fb3068c03fa91023ccaf58feb2a2d8bb308df4be2d087ad03041261 | L7631.B0.s.p1.d7-12 |
| P1678 | 858 | 30a17dc4ecc664fdabbc40bf1a42b7a51afae30f638501b21ea0ea3fb4f420ac | L7634.B0.s.p0.d1-3 |
| P1679 | 5426 | dbd0a236a855fbf8898cadf62c69b5977ce35b84150fdcc2e7d5995a81957ac3 | L7655.B0.s.p0.d1-120 |
| P1680 | 5426 | 80da3b134c892030bd5b2499f4476f03f849774831d4a989d1f64bd0637932de | L7656.B0.s.p0.d1-120 |
| P1681 | 2822 | d922683a376c8a9082660f6806f251ed9375bf1e1bb0a25373862bfed521d6cf | L7660.B0.s.p0.d1-86 |
| P1682 | 3574 | 914f310c67096e1127f22c5d090e932a3ee63ea74fc8ddb6e6b75b9070831719 | L7662.B0.s.p0.d1-26 |
| P1683 | 1049 | 46e512944261ae42bf0e3a408d08d1153738be4583e8081c87a89e8598839498 | L7667.B0.s.p0.d1-13 |
| P1684 | 111 | 2cecad32c3c99ecc15179d0c8f9861df53941bb3122301388a19a20f62d3a3f4 | L7668.B0.s.p0.d1-2 |
| P1685 | 11093 | 74d206b6a9d47cac191437ab719e39fbdff2e499a5ddc379202adde856d7a2d9 | L7672.B0.s.p0.d1-102 |
| P1686 | 3608 | be2226ea056236cd71c429cb8ec0082bc3fede8f47d84742775f086e9937922d | L7674.B0.s.p0.d1-47 |
| P1687 | 219 | 4facc52dcc9b73147950f66b488eaa32a0011eb1cb830793976a29d742ce6f46 | L7678.B0.s.p0.d1-3 |
| P1688 | 616 | 63b2daccc5fdd7aff0bb728f0f82804fe6f0ad5a084e5013c82ed01fa9fdcf71 | L7680.B0.s.p0.d1-6 |
| P1689 | 20 | 0d374e53b7cbce3d30db29af09b21d310a5946037b850da13029543ad7afc3fd | L7702.B0.s.p0.d1-1 |
| P1690 | 12000 | b4c41824eb4590d77db03df3b7e5b74fa737c74dd1aa403f766e980203992a70 | L7705.B0.s.p0.d1-595 |
| P1691 | 18000 | 3885a03ee98cfedfac6e171af55f5ed3baadbe5a87c0146d8b5e321ff9138fdd | L7708.B0.s.p0.d1-1008 |
| P1692 | 7184 | 91f9703097651fa5b0da84c1b2874019fb43f9ae3562fcce8da154ca2219ad68 | L7711.B0.s.p0.d1-201 |
| P1693 | 3198 | 88b5bd83bfc6be288aada087701537aa527d0a3d105cfa7d4e0a6fbecabc49bd | L7715.B0.s.p0.d1-36 |
| P1694 | 287 | dbe485ec8d5c9758e63869cd5210092f9a5f4bc05ba88dd1ac4e63a14dbf88fc | L7718.B0.s.p0.d1-12 |
| P1695 | 2493 | c9aeb2cdc710746a243633ea702aba53f0ac89d8b5fc2260f39c05c4a5cb9c7f | L7721.B0.s.p0.d1-96 |
| P1696 | 2766 | f48b40c5122777695728105c42319b8e2ebd2b5527e93557560fc628cae9d590 | L7724.B0.s.p0.d1-12 |
| P1697 | 2673 | 48330095926cf87d765574e6d2eda3dbcd08b027049acd06eb05d6b868c3e341 | L7727.B0.s.p0.d1-2 |
| P1698 | 514 | 9a05ebeab426100eed6ced2ae9c3a4f19654502b80164ab68588af5c93c317e9 | L7730.B0.s.p0.d1-5 |
| P1699 | 498 | 6cb59bf64d24695a851fe21e3434e532297e4db260332cad39b5d003d0ec44b7 | L7733.B0.s.p0.d1-5 |
| P1700 | 581 | 01d1da1c91a3dfa7da4ccd04412f81126592f558cabd64b35dd70581e8e1edf4 | L7736.B0.s.p0.d1-2 |
| P1701 | 223 | a0e3155d84263c52a2a83f772ef7938bbca7f12a403a9c2ab3f5f0cc3322e664 | L7736.B0.s.p1.d4-5 |
| P1702 | 285 | 0a51f07760321a9d05319a6650328557262c7aad287c29f06bee43cc02607710 | L7736.B0.s.p2.d7-7 |
| P1703 | 321 | 9cb305bd9bb9807f0b7a659b1d6f5e06a5c8818ceeeac20cb7c257da7a2a2cd9 | L7736.B0.s.p3.d9-9 |
| P1704 | 726 | ee8e7b1da2b2ddc5d78559844db798fa92a7bae7318f76891db2c26529ace553 | L7739.B0.s.p0.d1-5 |
| P1705 | 205 | 2a8ffb7af16b544163d1b69b2c0b63aff873ad39d22e9e5f8cda85974367871b | L7742.B0.s.p0.d1-5 |
| P1706 | 241 | 54774de47dc1820064d533c648cfa280ad59a82bf48dfa701480190c44c62a37 | L7746.B0.s.p0.d1-8 |
| P1707 | 16001 | 0c95e52ee3fce1b74f536be7f32f295c93966bbb93b1d1f541d5346981cf8994 | L7766.B0.s.p0.d1-197 |
| P1708 | 184 | 913c291a7d876b961d0dee88dc657c78e85fc408459bdf94a3a1c56132835766 | L7772.B0.s.p0.d1-1 |
| P1709 | 175 | 5131f3b0097e0fe27e49db4ab46d60b77463741f726bdb2df58db5b8276c923b | L7772.B0.s.p1.d3-8 |
| P1710 | 259 | 71945c6cc6a31018d570acc2f3c884c36204f876b3a48e2841d99069f870d891 | L7772.B0.s.p2.d10-10 |
| P1711 | 63 | 40704692c20cbaac1071edb2cce824e347f046352c36ca76725e7122f7bfbcdd | L7773.B0.s.p0.d1-1 |
| P1712 | 196 | 958de81c3cd08617262bb92d104b5dcd2f0e9637ce5c1f60ed34b37e205a40af | L7773.B0.s.p1.d3-3 |
| P1713 | 239 | 0111e75b57ad837eca611436793d15abe7b2878e755edb64ad20ca6e21be730a | L7773.B0.s.p2.d5-9 |
| P1714 | 206 | 0690a92dab3e74ea100834101d305896e33128a72bce4bf079ef92fa23902929 | L7773.B0.s.p3.d11-14 |
| P1715 | 214 | 7d07296af045c3dd2cc10e3874248d248f6e14ecad898ad73c197c1a3761605f | L7773.B0.s.p4.d16-19 |
| P1716 | 84 | 55339b7e1176d314273de37bd70c2b03d9cf4ca6652670bbb758e0ff32a71fe7 | L7773.B0.s.p5.d21-21 |
| P1717 | 352 | b29eb33434d3259c553cf91572fc162449f8e1323e15232066658be3ab0e1878 | L7778.B0.s.p0.d1-6 |
| P1718 | 662 | 70833c9f64245567524a69ae9deb991625be2154215159b1ccca739186dfa0a9 | L7782.B0.s.p0.d1-6 |
| P1719 | 457 | 327e54f6ae2e10669ee2af4491b7838c95a00a01d65ae02252ef6726de795642 | L7782.B0.s.p1.d8-19 |
| P1720 | 141 | 5fa65184816ff1d18d0ebedf56c5a9147cb9bd21b865161e7365e342a32e3455 | L7784.B0.s.p0.d1-2 |
| P1721 | 65 | ca03d6d27a83c05e7c389a15ad3aa5ffa56c900f20f99986fc09755e7134e43a | L7787.B0.s.p0.d1-1 |
| P1722 | 9377 | 526e56a6a898aa7875184208335a8b36322be4e9c6f750c44e177c5b5ab43345 | L7790.B0.s.p0.d1-171 |
| P1723 | 16041 | 4c88245c4b92591a4b2f396263d1624503b55b0e754910822bc9d03493f48697 | L7793.B0.s.p0.d1-220 |
| P1724 | 246 | d644b843b92ee06780461f78c9c0daee672cff4ab0c5b3620033112728ea72dd | L7797.B0.s.p0.d1-3 |
| P1725 | 2364 | 2ceba1fe15bfac5555a9b91939b763dcbf715e8565aecb74d799f9f3f1b30073 | L7799.B0.s.p0.d1-28 |
| P1726 | 211 | d1f33d7507efcd0964183980f538e10db56eae67d3e4d417fd387d34d6965347 | L7802.B0.s.p0.d1-3 |
| P1727 | 122 | f2b7865c33a950a9874e8784d3ec7b7fbb568085e036b69ebfd7f3a25b2a4033 | L7804.B0.s.p0.d1-4 |
| P1728 | 6727 | 6354d63d9a564f8052e37314af4433ea1d97e2e73b7bacef75213d7a7dbc83ff | L7806.B0.s.p0.d1-36 |
| P1729 | 870 | 94de8186704bb1360f3eebaecb38b3c7ca36eea335916ff50e4d1e1718a7f5f1 | L7809.B0.s.p0.d1-4 |
| P1730 | 33 | b07da4416da54d30656381681e84431f3aadaf85e32cf553eaf2b1e7e3d846de | L7811.B0.s.p0.d1-3 |
| P1731 | 1600 | 4d7e51e73a582c005d5b897d644473b1cac94a3cff86c45329a2dba8629ef7bb | L7816.B0.s.p0.d1-39 |
| P1732 | 887 | fa50325bbd6f90d635ac10ba4102aff673fed789d6505fba87d8ab723299746b | L7820.B0.s.p0.d1-10 |
| P1733 | 150 | bbbb21d76c7827b4674629ebaccd8bfbd7dabbb5e29cba30f3b9546b52f334ba | L7822.B0.s.p0.d1-3 |
| P1734 | 48 | 7ffbb47a9e031befe4575aab2ba46332231c3bdcdd87a60539f4e573b7c3c01e | L7825.B0.s.p0.d1-1 |
| P1735 | 171 | 4701db8f51de664306247a7117c90c8d353a49a13e1be05d51118dffd8ffdbbb | L7827.B0.s.p0.d1-6 |
| P1736 | 2 | 41cfc0d1f2d127b04555b7246d84019b4d27710a3f3aff6e7764375b1e06e05d | L7831.B0.s.p0.d1-1 |
| P1737 | 7 | 4935b1909bc0dec35c1e51c64d45529cac53d4b65e477520cd20a38c4bb6b59e | L7832.B0.s.p0.d1-1 |
| P1738 | 21 | 6e5fcf3848bd440e0042e40100a6e541dda8ff2690d4c8953a89bace99e859a4 | L7836.B0.s.p0.d1-1 |
| P1739 | 31 | 3457531bc7530a29028677cdbc44f57eb9c4beeedfe1bd5a52e9a919a6955d76 | L7864.B0.s.p0.d1-1 |
| P1740 | 135 | 86d8a5bbb0d09ed54ba9fe41df37e24863752999d0b41f5d6c8b1404b75ae963 | L7864.B0.s.p1.d3-6 |
| P1741 | 149 | 40ad635e50aac546a2c3aeace42803e2109877025d79b11aff58406958b2318b | L7864.B0.s.p2.d8-11 |
| P1742 | 16 | 1cd0422246c67418245952a15951428c3c0a18ef422f7246e792f0583a20031b | L7864.B0.s.p3.d13-13 |
| P1743 | 151 | bd6659e7b107e545a2bb01f2710fa2fec8f59686be2f2bff290f5b59ef7e6400 | L7864.B0.s.p4.d15-18 |
| P1744 | 164 | f995e389f3f5c2a0fb68baefd191abbc1e1d6a1353521875968c06de47e9ad09 | L7864.B0.s.p5.d20-23 |
| P1745 | 121 | d51691e0d622a101d25c890253a536e16097de0e83ca991e838b3362ebc8ea99 | L7864.B0.s.p6.d25-27 |
| P1746 | 152 | 932f84591d2e1e354048bc2e6d372a0b87847375aac557be8036db35fe35b8c9 | L7864.B0.s.p7.d29-32 |
| P1747 | 143 | 9c1893aa64f134649d4ee6038d74b74cb9f0e5dd2fa4022a88482cab59d59715 | L7864.B0.s.p8.d34-34 |
| P1748 | 6000 | 51603899fe93f3ad0401d1beb1e4d68ca5c04ade42653fe39dae2ff9024603e1 | L7865.B0.s.p0.d1-1 |
| P1749 | 104 | 083f16009015f5edbde1a2f3ca568359316e5d3a8f430a489fb73f9ff45a5bfc | L7869.B0.s.p0.d1-1 |
| P1750 | 6414 | 383045fab9f2966f53014dd0f5549550e14699ea73dfb68b1ce96938024a3834 | L7870.B0.s.p0.d1-35 |
| P1751 | 504 | d106b26955c2fce49b1c7a75fdace057f114b312ca267639f3c3daa5a9461fca | L7870.B0.s.p1.d37-44 |
| P1752 | 1995 | c555cfa185ee207d432dc7a8d1569d7e685676c838c6ed380c7a8c4d2dc5e089 | L7874.B0.s.p0.d1-19 |
| P1753 | 941 | 6a04aa6bd39a29a5601a14c1d7867043e460590c32eb61a4e3cfcc2dbf713e79 | L7875.B0.s.p0.d1-5 |
| P1754 | 11817 | 31159640a77b97ead92ce6caba7f3f36344459d38956c7497b7e43260ec6ba71 | L7879.B0.s.p0.d1-27 |
| P1755 | 124 | 4a6233056438b497164e25269e0e31e426d7488efa9a9774cfb818c76ea86239 | L7884.B0.s.p0.d1-4 |
| P1756 | 110 | 8823e629eae75391f364da333c14ed9339011d0d65eb6bbd8d659e61f7f5bf16 | L7885.B0.s.p0.d1-1 |
| P1757 | 356 | 0dc3c8f29f046aa39bc55a24ed980265e531427059caa19a02316292dc046374 | L7885.B0.s.p1.d3-3 |
| P1758 | 166 | 21b676f763a9c8ec4ea0c2b75244868e3e078455050cc0ee4721d3d6aa72c319 | L7885.B0.s.p2.d5-5 |
| P1759 | 719 | f22695a95c87bb8daa40921bd1bc014625f3c933585d8ce19e11e78c0ce8e2ce | L7889.B0.s.p0.d1-2 |
| P1760 | 162 | d3906558a2c1011972fd81ea3b7b3f8aa06f9f12922f008c1d4888fa024b1d54 | L7890.B0.s.p0.d1-1 |
| P1761 | 202 | 22988b2101a2a2989122585309e14becc32b04aa24704b52bdec7f180bc8166c | L7890.B0.s.p1.d3-3 |
| P1762 | 188 | 85ba51baa57be354023bf924548ddbe22640cdb3ae57e522e44a232606d99ff9 | L7890.B0.s.p2.d5-8 |
| P1763 | 126 | 171be609bf7957d9c6a66b406153f5635876d9ad5283b51379ea26b509069640 | L7890.B0.s.p3.d10-10 |
| P1764 | 758 | 4e03572360480cd79fbbce8bb1049c40a0862067baca983d989d02309b0b1ddb | L7894.B0.s.p0.d1-6 |
| P1765 | 28 | c9857965a3acc61531d7a96549bd3f959c52dc1bd9f9c549c20dd5974fa6e0d8 | L7895.B0.s.p0.d1-1 |
| P1766 | 87 | 939607dbefbee49414210f79150126860777c516d88b4ddd8e01842011203a8a | L7895.B0.s.p1.d3-3 |
| P1767 | 220 | 47d00ca5c1ae3e436f164fd7f0d5ffb7cdb38f0988bd80909574cd90b6fdaf10 | L7895.B0.s.p2.d5-6 |
| P1768 | 85 | 1961e795db2111b9be5feaa7d5a6e2276da8a4072e89593a857b9767dbdac08c | L7895.B0.s.p3.d8-9 |
| P1769 | 104 | 5152acd085e947c56ca14dc924d10cc375ee741cc8a10d78c85f7970b98dede6 | L7895.B0.s.p4.d11-12 |
| P1770 | 364 | 4702b48cf40bb81b0c5688b4ade2274f890f9fb1b860a896780d07c45c2c2397 | L7895.B0.s.p5.d14-18 |
| P1771 | 191 | 5cb4a5b34ffce80ab69800c59424aa199dd2becc7d4c56ee9bbb3f42b00acfe0 | L7895.B0.s.p6.d20-21 |
| P1772 | 82 | 54be72016cd6c75ef0fcdedff6f174ea7c0cd892fe9a57a71a7e0c4e83955ddb | L7895.B0.s.p7.d23-23 |
| P1773 | 349 | fdf4383dc462c3102be650b5af64db1e5d131d97d5af7b6b4b43408049839933 | L7898.B0.s.p0.d1-2 |
| P1774 | 283 | e09c57f4151294cb7c919d6897242399ece5fe049263a7be5b75492ab66b3fe3 | L7898.B0.s.p1.d4-5 |
| P1775 | 281 | 786f95a4e8efa7e72e7989aa3ffa74b91da7ae092132d9325035fe220a8eb0c5 | L7898.B0.s.p2.d7-8 |
| P1776 | 67 | 99db27f88d925dee2f2ca23fa8b49d407f0cfb2243b40e82f5e7a4986a51622b | L7927.B0.s.p0.d1-1 |
| P1777 | 38 | 69d1218fa016f1b7989a44b58817c500b65ed43a99f357802b2fc8d082ca317a | L7927.B0.s.p1.d3-3 |
| P1778 | 425 | 04831dffe971ded2626451151500dbd4b08ed36a6ef160f2f09efd9f8852bb92 | L7927.B0.s.p2.d5-11 |
| P1779 | 209 | f8a5b91180b2f46438d11d24b57f236f2139f167e4354441d972b2c1f24041f5 | L7927.B0.s.p3.d13-18 |
| P1780 | 343 | 6a4e4a25644cf0466fcaeda0b0052f0d2ad8d6593596ac3efc87dec864f9d080 | L7927.B0.s.p4.d20-26 |
| P1781 | 243 | fa33084f19d92297b63184705614f7f8bf1cf90a7f5a18170a333def75f86366 | L7927.B0.s.p5.d28-32 |
| P1782 | 51 | b6959511c9a7e54829ba612b90e1f75b9687c677072d9d5ca29005d80037667e | L7928.B0.s.p0.d1-1 |
| P1783 | 247 | 93e707ba05eee1f692f12596261ce32750682ebc0406b6aa14a57d0a5d223fd4 | L7928.B0.s.p1.d3-6 |
| P1784 | 109 | bdec1c9a98f80a914ba9b93ef37ccbcc3b8ae1112811495e7e95094c3b48f0cf | L7928.B0.s.p2.d8-9 |
| P1785 | 289 | 8f9be94b4820931488f124ea7ba373a650084595c725b82357e1e209e36e10b0 | L7928.B0.s.p3.d11-14 |
| P1786 | 225 | fee26ebbb48d2020ce5d04e62357392dcc993f5b565ed5f6fdcdab4f793f497e | L7928.B0.s.p4.d16-17 |
| P1787 | 165 | 051de83b3e30765f227e967cb619d97b302cd45773da7011c9f3e0a30df67730 | L7928.B0.s.p6.d21-21 |
| P1788 | 50 | 80c3028c578c1fab798e291e2529ab884af426f68135dfdc089b59ea39f83515 | L7938.B0.s.p0.d1-1 |
| P1789 | 17 | 75ecad4b33bdc9038711e10e85d7dc6405a837bbc69bbc5723f7249e7123ae09 | L7938.B0.s.p1.d3-3 |
| P1790 | 173 | 17355350d25954a6aa9bfd81df3a9547744f3161b39c6e9488e3484631bb97d7 | L7938.B0.s.p2.d5-6 |
| P1791 | 118 | 0dd8017c3fdb1b73308370a3bc0094ba43047f196b2700ec0173391bc4abb9ac | L7938.B0.s.p3.d8-9 |
| P1792 | 23 | 8a848e97d589e34abb5ba3443b3cce6d7ecb7d2b125babc7508af82ca4a74206 | L7938.B0.s.p4.d11-11; L7954.B0.s.p1.d3-3 |
| P1793 | 334 | 5e7d71106c3f3c153aa054b415d12afd3f7120fdede3e670eb7de882a067fb2e | L7938.B0.s.p5.d13-14 |
| P1794 | 51 | 46b91570b8948fb9ba0a84de00e151f3ba5c24bd75d8eb8983d7066670d76493 | L7938.B0.s.p6.d16-17 |
| P1795 | 10 | 30ac03ff33731529441be8fbe52a3bd0d4c5ec830e806d54692168ebb7f98ada | L7938.B0.s.p7.d19-19 |
| P1796 | 267 | 20d21dc59f7cc1ec8594accc897d163a6e96279c54d4dcb8b1a303ee90ffd6bf | L7938.B0.s.p8.d21-21 |
| P1797 | 195 | 14c187e94bd1e12e4ffb2a5b4780a504e1fc2a17a2190328f4407d15739887a4 | L7942.B0.s.p0.d1-6 |
| P1798 | 36 | 28a32353863786c206f62091ddbf73756393a4bea0827fd800f698d725f11c47 | L7943.B0.s.p0.d1-1 |
| P1799 | 49 | 87890ae9ccc55a5ce0c50ca56047b4ab29cbd5d9c1ba39c0061d04a36f943944 | L7943.B0.s.p1.d3-3 |
| P1800 | 132 | bbdbe34e4911fa9f6edd12f18a970a1893939d17295783c06a76fa8e3741bbb8 | L7943.B0.s.p2.d5-6 |
| P1801 | 218 | 68bd6afad629aa875073a6e0f9acd8405e9370e0c2f3d8d9062f6b0c37eb879a | L7943.B0.s.p3.d8-9 |
| P1802 | 98 | cc18b76dc89dfb8d41ada5979ebc418775be773853f09d348e2d3b4c3b1047fd | L7943.B0.s.p4.d11-11 |
| P1803 | 123 | 809d9b04da50ee2c4f7aeda517afe3acb0936b0b72a4d7f6870797b16e21ade5 | L7943.B0.s.p5.d13-13 |
| P1804 | 105 | 0e653e9c18b3b03e5d10ab121844ea426ff992e0955f1dcdc15e514eb497dd3c | L7943.B0.s.p6.d15-16 |
| P1805 | 151 | 3687c567060bb8ffc9c0bab77ef0c02c141506fb5a7a85e5912b5bf041de1f33 | L7943.B0.s.p7.d18-19 |
| P1806 | 95 | e31dafac7fdabeaf12789f094dab6e9d5777cc0f144ca674c6ecc14092fedada | L7943.B0.s.p8.d21-21 |
| P1807 | 67 | ab8e7497f84146f468b8777df71e66dffca1b55880b665c31dd9fb776a37c818 | L7943.B0.s.p9.d23-23 |
| P1808 | 113 | c535cbffbc4b342f826e041f38a64620d66e354062013b8905b2a66d6c20f2e3 | L7948.B0.s.p0.d1-5 |
| P1809 | 34 | 31b30157c263f1d6e816da44cdc6cc15c94dd055a655ab59622a19be0cc948c8 | L7949.B0.s.p0.d1-1 |
| P1810 | 56 | 9eeb3b55ed575150f51fd4ed41b45598b64f614ea2502378c347c0f148d1e228 | L7949.B0.s.p1.d3-3 |
| P1811 | 218 | cb061a03577b2717bdf3f39a7ef3be8b74252e249330c62cc3107af3d6163a59 | L7949.B0.s.p2.d5-6 |
| P1812 | 319 | c44aac70aef0e2f42c9e0de1fff13ec76cff14185e2eee6313f94cbe09ca3d4c | L7949.B0.s.p3.d8-9 |
| P1813 | 167 | bdb5adea8260d144678b1cca6cc80ac485e5913de4a957007c894c891f90410c | L7949.B0.s.p4.d11-12 |
| P1814 | 226 | 05624b6eb35dcbac56d6fc9bf6188cbd80ebab5b8e9966915d261f4c81dec990 | L7949.B0.s.p5.d14-15 |
| P1815 | 96 | 591edcaaf4468c46da64b2e6e3a07d5bf3b2bd11417091225af0e3fec207de23 | L7949.B0.s.p6.d17-17 |
| P1816 | 88 | e370fb6f4d38837c4e8e0eab5ee9118bed90a5990076d7171e90ee8e22c09234 | L7953.B0.s.p0.d1-3 |
| P1817 | 604 | eba2036540d64b1eea16212b638a8b483450f774c011bb82a98ba7f49bcbab24 | L7953.B0.s.p1.d5-21 |
| P1818 | 48 | 52dce4e478e93dbf7d5281173be372cc36efa0b1890acbeadedd0df1349e7dda | L7954.B0.s.p0.d1-1 |
| P1819 | 61 | 2560d1153b5b24cf9f219be8f9339849bef2f76c5ec774487f5fba5378cd26bd | L7954.B0.s.p2.d5-6 |
| P1820 | 28 | 33ad002a50abf4a56fa022901d456ee6cd3fff3093b43feb2798fc7d0bc7f917 | L7954.B0.s.p3.d8-8 |
| P1821 | 118 | ceeada88ff055a9896b46cfae9ba382de59e3b52d0d09ce240c38a46dcea1e52 | L7954.B0.s.p4.d10-10 |
| P1822 | 233 | 54c33ba33423f39885b3ab30d0fffe03b3da7f918a3a518325fc9b17afa0a9f6 | L7954.B0.s.p5.d12-12 |
| P1823 | 13 | dcfe4ce03b40e8b90321b2ea94a564b3f637de7d6c2463fc025a6ef4aa04be3e | L7954.B0.s.p7.d16-16 |
| P1824 | 269 | 66df9c4709eea07e53322becaf401d195d95b3f8a5e13c94cc67ec29f04bcf21 | L7954.B0.s.p8.d18-20 |
| P1825 | 155 | 31295a4482ba1c404c379e4aed13c7cbe13775d18c7d80cf3f4b64b07b8132c4 | L7954.B0.s.p9.d22-22 |
| P1826 | 130 | 7d89a78f0e5709cf5b2efa88abb8b6f5f69ac2824a3a4242b10dfd225a26d917 | L7954.B0.s.p10.d24-24 |
| P1827 | 39 | 5b64dc99d08408b6374176229c21b4067a1de022f6fa6831499be369372dac1b | L7960.B0.s.p0.d1-1 |
| P1828 | 163 | a5105f24504237488bff12e021b74ad24c7529575a0dc20f3bea81f6b9b93505 | L7960.B0.s.p1.d3-6 |
| P1829 | 236 | 6cea08c5a300dcf871ab969ea138ae69a88e5f9403ae312fd8f6b70292836882 | L7960.B0.s.p2.d8-11 |
| P1830 | 30 | 4ac7139c5d85aa6dfcbb01b7d0992ea069eb77aad051389bc337a81953ccdc26 | L7960.B0.s.p3.d13-13 |
| P1831 | 86 | bcefb760ef0ed676afb04475a399413482cca6551f74bef995071d50a463a654 | L7960.B0.s.p4.d15-15 |
| P1832 | 83 | 002053e9654aa5cd7c89171490ac63d35f0b070f47ea6714c50e9aeb381ac903 | L7960.B0.s.p5.d17-17 |
| P1833 | 87 | d5df5fae2d3c2579d2a1cdfaa98739b693f760a39faea9e643bdb051c8a83086 | L7960.B0.s.p6.d19-19 |
| P1834 | 356 | 69a4c9d384077ac6639e5f52d6a6cd2959db6f65f11efae404dbced0b467bd59 | L7960.B0.s.p7.d21-25 |
| P1835 | 41 | ee2605e720cded233a34fcb330dde5dfcc106a4b51918eae65e7d837105528a4 | L7960.B0.s.p8.d27-27 |
| P1836 | 44 | 1a6a9c3698e45a3023f4e0c137bbf396c7f2e2c38a80c3cda4f431d01274bed8 | L7961.B0.s.p0.d1-1 |
| P1837 | 60 | dd750ceb7115f05c0f7e01de854b630e7bae964888375e9d75d61fef99146718 | L7961.B0.s.p1.d3-4 |
| P1838 | 71 | 7feca6c60128069f8fc0694273d2b9eeb2ba7144f0b9b94f2414a7160f63786b | L7961.B0.s.p2.d6-7 |
| P1839 | 21 | dfa74efad4d3d21447e4e3a663a785511ccb0a3ad1134a8b015ae25dd335cef8 | L7961.B0.s.p3.d9-9 |
| P1840 | 161 | 856970299523157f3fc37898fa885a486f9a64a7f2a8e9bf45f628d0256038c0 | L7966.B0.s.p0.d1-4 |
| P1841 | 71 | 8ce79fd233a213e4449a0d15c68d05285cd797c2199eca703a3c3b3caebe799e | L7970.B0.s.p0.d1-2 |
| P1842 | 62 | b0f97415c439a39c88d2224f8e5a02a4eebdce1a86b4fd0df16597f36c2ff28a | L7970.B0.s.p1.d4-4 |
| P1843 | 52 | 94e3523bb3269e2e40e1842fba5fd6890a84a9d6b46f02273e1ba7ab1c95db24 | L7970.B0.s.p2.d6-7 |
| P1844 | 409 | 5986b5050f04333ffa2ea08ddcf4fe296e3db52a7a1ac830f3dca31aecce00db | L7970.B0.s.p3.d9-9 |
| P1845 | 8 | bda4cc844909ff9245e2d625d25bcf22816f67f1b210d44fe1afa75306c84e0b | L7970.B0.s.p4.d11-11; L7970.B0.s.p13.d33-33; L7970.B0.s.p28.d66-66 |
| P1846 | 142 | ce5c08182f44ece6b1012e2aacd7e58e696ea4249efc4292e924bf2888e9fb2e | L7970.B0.s.p5.d13-15 |
| P1847 | 8 | f020672615330ff2664a8fa9734796bdea45b81fe6e7e57cecc25726bb7d91b1 | L7970.B0.s.p6.d17-17; L7970.B0.s.p15.d37-37; L7970.B0.s.p30.d72-72 |
| P1848 | 158 | 1706564b638f7986cf89cd793d5f2154bebf5a641a57ad26cf3fa2cab16f77f2 | L7970.B0.s.p7.d19-21 |
| P1849 | 27 | cde8a16330709cb0f2a4f870a6b223fee7c23a68a7c5152ca1602a572e4af321 | L7970.B0.s.p8.d23-23; L7970.B0.s.p17.d44-44 |
| P1850 | 356 | 804acca792bdc838ee8f232791e287a799cbb3a7b8675dc0f2fdf515c2b5512b | L7970.B0.s.p9.d25-25 |
| P1851 | 34 | ffaaf01a3ad242f6aad46a08561233be3ff8153cec2e99818df04ab60b62b936 | L7970.B0.s.p10.d27-27; L7970.B0.s.p26.d62-62 |
| P1852 | 524 | 6f029e2ac7579078a8f9ad8245439ad34dfcfbf45294c8e6531ae2ac5e7e38d7 | L7970.B0.s.p11.d29-29 |
| P1853 | 38 | 5f4a58912e4d3868e6057424af8c6255abc47584d8df02b150ed701c736725cb | L7970.B0.s.p12.d31-31 |
| P1854 | 19 | 3234601bf8aa94eadbbd11022fdba548dc3c71a92bc006d5e336f2082f38bdc6 | L7970.B0.s.p14.d35-35 |
| P1855 | 118 | 1874199fbf73d27e4b4326d3ee68885266844c16cacc316c4128f551f3c94bb8 | L7970.B0.s.p16.d39-42 |
| P1856 | 76 | e39207ad75e5a54659368bfd4df0dce36ab83277f30f1413485d5fb901cda842 | L7970.B0.s.p18.d46-46 |
| P1857 | 56 | 3f0345b35aea05fd7ed0783eb917978e7a44df6b3346b89cbea8b05b006ba973 | L7970.B0.s.p19.d48-48 |
| P1858 | 83 | 8c93bf769a8dc7142ae0ffe5cf0622f5ddd186c1007d8967730fe7ce162c72ba | L7970.B0.s.p20.d50-50 |
| P1859 | 13 | d5bde6fce39ebbf5f7ff8a751728671fea80daf17558da6d63ae0caa4423f8ca | L7970.B0.s.p21.d52-52 |
| P1860 | 45 | 99d3c00e7e9822932ff4b30a00a573aac78fc9b3935e2f8f6616fc98fe6b5946 | L7970.B0.s.p22.d54-54 |
| P1861 | 13 | 2bb3c2a83906f2c4ee628c6ba12ea378fe6345bc725ddeb1f1d8c9eae5f30b1d | L7970.B0.s.p23.d56-56 |
| P1862 | 94 | 2c7c81b58c5d2054f00fba9377a14a010d086b8f9630c9d23b1f4a805100f9a9 | L7970.B0.s.p24.d58-58 |
| P1863 | 21 | 806d7e0cc5792c324ad8ad6b9bf5406a49a7dde9115ed1d9713f9b6465ef7394 | L7970.B0.s.p25.d60-60 |
| P1864 | 282 | 08d64d0c8be1295dbd9ba00d240defa9bdfdf7671d502cf6ffd32784066dbe89 | L7970.B0.s.p27.d64-64 |
| P1865 | 109 | 3a25765320c3eff176e30999295268a34f8ff67754c0a82eabd4df410be025ae | L7970.B0.s.p29.d68-70 |
| P1866 | 46 | b0ea2314882925f054f061fcb17bd48729a024aa916c79b3bb1f7d1775e6566d | L7970.B0.s.p31.d74-75 |
| P1867 | 180 | bc6f9e06b7d78b2205039aa214c798e1d34a5e5ff7768cfb0e10055a31bcc023 | L7975.B0.s.p0.d1-4 |
| P1868 | 1019 | e5cabf2d57e69e60fbb24cb0d64d90aca6b64e29181d2bb315668c61a0648017 | L7976.B0.s.p0.d1-9 |
| P1869 | 15927 | 1d86b3f97d72da6bbfcba950b6c4ed982b7a0cd957bb6cb06a27ce87141a96a4 | L7980.B0.s.p0.d1-270 |
| P1870 | 96 | ed71f34a8f513eb948dcda9b70c1d11aa70d9baa7b64ef77385307be52778dd5 | L7984.B0.s.p0.d1-3 |
| P1871 | 62 | 7026bfedcca06ec981bbf273ce102e18798845c4947b3eb41a6ad6d81d2de78f | L7984.B0.s.p1.d5-5 |
| P1872 | 193 | 7aad77870ca1d5a8bbc5a5e346d5b80d0259db82d3228c975a5c5aaa0d1fd9f6 | L7984.B0.s.p2.d7-8 |
| P1873 | 7 | fc9f7623b53e9cee797487fd8e320e9859ddb14676ce8b6bdcb7f211d6ae100c | L7984.B0.s.p3.d10-10 |
| P1874 | 375 | 5af7db431e8fafb65e52aed5fb11685b5467289dc5bcd29eb3bc5adfb8be0557 | L7984.B0.s.p4.d12-17 |
| P1875 | 188 | e71d7448e85cf186b045ee9fefde882d601bf96d7cd5e12c6efb074915c5971e | L7984.B0.s.p5.d19-22 |
| P1876 | 99 | 005f706c7db9c1656e63f5e3cd1ccf29553f73227508eebfeaf10821c28f797c | L7984.B0.s.p6.d24-24 |
| P1877 | 7 | 37fe37a3a61919c22fa5b310c540cc44e53ce4bb86a84efa54cf9c3b4bc27b69 | L7984.B0.s.p7.d26-26 |
| P1878 | 17 | 37b7703cbafa98a97f012b209fa22d166fe28b7d1fc104030f796b877355b1d7 | L7984.B0.s.p8.d28-28 |
| P1879 | 22 | 0edc6124bad95100c8846bee03104db111eef5a80e82f698b2de97b11db2fc91 | L7984.B0.s.p9.d30-30 |
| P1880 | 110 | 8de72e38730f027a4c809cd16ca3634954353f1318e588485b39207a3a30e030 | L7984.B0.s.p10.d32-32; L7984.B0.s.p15.d42-42 |
| P1881 | 307 | b57c280688cb27a358dc64a0b0f83971a5558d51a9b5ce55b5faa5495398087c | L7984.B0.s.p11.d34-34 |
| P1882 | 99 | 406175591d63dd44f8e745848b76aa272332bf5b91734ae70734dc7c472da40c | L7984.B0.s.p12.d36-36 |
| P1883 | 5 | 981d202c653a291b8ce80ed05b458083dca6ee90ba8d0c187188a319e715134c | L7984.B0.s.p13.d38-38 |
| P1884 | 7 | dfe95783edfef7918760312f94edd764f4ce3ffd38b90c2c955a0cc3b250f040 | L7984.B0.s.p14.d40-40 |
| P1885 | 29 | fc3d6736ea8a531f815ec21ec1110fa942a20d7005eec5b2f877ae3635a51303 | L7984.B0.s.p16.d44-44 |
| P1886 | 309 | d8c1701b48083ff8fca1f1185c083c59f9dd9dde1d1cba0a95782fcf9f2b0d67 | L7984.B0.s.p17.d46-49 |
| P1887 | 126 | 8ae3664e0dd66f5161ba2ff815288545f2c804212fc1c9a9ad6f2a4f187c00ae | L7984.B0.s.p18.d51-51 |
| P1888 | 64 | de3661ba7323d77891fe834c2c53a4d1452c6d81e0680adc53ed57dcacb77265 | L7984.B0.s.p19.d53-54 |
| P1889 | 73 | de72cd78dac4a1efbda7889c94daf2ad305d4bb8fa66701a6475b0781e0975b0 | L7984.B0.s.p20.d56-58 |
| P1890 | 18 | f46687b230152a291ebf8bec97786fa54c81258f214fee2196df412db95adad7 | L7984.B0.s.p21.d60-60 |
| P1891 | 68 | fc51ee34c5c1ccc02e3a04832146475a0f0f3e1b3926b2a536b0832ee79395f3 | L7984.B0.s.p22.d62-62 |
| P1892 | 18 | ad26b43995ffcd0c811e4a8877d6e68858aaaf6e44ec745c493e9bf1b94bce79 | L7984.B0.s.p23.d64-65 |
| P1893 | 19 | 0d1f69303659468162df0ac9fd171ae8f34cc63e134619ac85a694375d015739 | L7984.B0.s.p24.d67-67 |
| P1894 | 178 | 8e5477e67ab7eeb49ed32f36afbccf6128b8da70bd56050916b42195cba6ad2f | L7984.B0.s.p25.d69-70 |
| P1895 | 14 | fa2b136ef0e12c44465024dc3c46d0b1d473ad55ca63d52ef4fe9fc1e128f106 | L7984.B0.s.p26.d72-73 |
| P1896 | 15 | b5afe26d6a4b6d1f01ba5ca0496f4e5dfac5971c9752d2011b97fda541983cd9 | L7984.B0.s.p27.d75-75 |
| P1897 | 310 | eaa77cf5524c96dbef31fb3996d0a0304e62cc2ca80ff9c24e53c58e784f2a7d | L7984.B0.s.p28.d77-80 |
| P1898 | 122 | d866a60dabb5aea74804cf338ff423d718995670105d263df14988417b79939a | L7984.B0.s.p29.d82-83 |
| P1899 | 449 | dc55c07875f460929e12f3ae83dc38983e73e9eb6853db0c375f3d4884c8c593 | L7984.B0.s.p30.d85-85 |
| P1900 | 34 | 552821d4b3e0fd1ef293df9dbe7491e5ce868bdabbb36c00d8b61e0bedd45d52 | L7984.B0.s.p31.d87-87 |
| P1901 | 42 | d591414564f1836304565009113d3c5ac9d36975b9f080c24ec0d9541bf1f5e0 | L7984.B0.s.p32.d89-89 |
| P1902 | 20 | 21f29224bb655170d51b49b23015b779434d5540dc2825f5adf9a2bbc131b5f5 | L7984.B0.s.p33.d91-91 |
| P1903 | 46 | 730ce7a6be66bec6f1dc4f13295383bb0bdf15fb9ccc54d3bb1a557643e054b2 | L7984.B0.s.p34.d93-93 |
| P1904 | 197 | 5c183f1de2dee2d00be45c85bee074ae6b0dc20fcb8914c358f7479bfb142ac3 | L7984.B0.s.p35.d95-98 |
| P1905 | 91 | b64f6f3a826076f739556fc047ef55d3b783ca023ecb0675022d1ef3d6b4bd7a | L7984.B0.s.p36.d100-100 |
| P1906 | 100 | 9ad573bbc2e47cd066a95fb4c16f2b24bf0ab8b7a31f49cb65948729a6fe4659 | L7984.B0.s.p37.d102-102 |
| P1907 | 17 | 4d90ecbf9aad1ac51433867f5ee2ba514e2d25811eace5cc39d629718dea2f78 | L7984.B0.s.p38.d104-104 |
| P1908 | 547 | 242385fff9ff472da57bfecda01cae9b46a3c95a50151f64c7ec77c06ed0b522 | L7989.B0.s.p0.d1-20 |
| P1909 | 33 | 57343e805ce5a15e494b0f0b29f630b1d3312f90d73d145034fea89fd7165008 | L7990.B0.s.p0.d1-1 |
| P1910 | 21 | 1d86f2c82ff7e585c244f22a5701755469649bc20ac8813814e15f49c8d910ec | L7990.B0.s.p1.d3-3 |
| P1911 | 318 | 869f1a35a6961e19af98aa8d20dd86df3879ef0bfd32cfed646ece6d54463bd3 | L7990.B0.s.p2.d5-8 |
| P1912 | 285 | 8d389ff31e265859adb78c0d7014951a4c627ee07024a60d2748661b9b585332 | L7990.B0.s.p3.d10-13 |
| P1913 | 29 | 65929ca3ed829efbe80f1ec43119a2e3ecaf831c173363372308a3cc2ad154b0 | L7990.B0.s.p4.d15-15 |
| P1914 | 382 | ee826acd69dd222c0970d7ef54d1a7d48c67a1c8ce86101fb2db645019bf4951 | L7990.B0.s.p5.d17-20 |
| P1915 | 181 | 8ea5dd13a9fd735f3c0da13490c125d4784127c97ffdc60694fa7df28d8ad275 | L7990.B0.s.p6.d22-25 |
| P1916 | 156 | ca7556b1264aeb89d9d020455358c0bfed0f1d2be4f57643c9b9da185587e31f | L7990.B0.s.p7.d27-29 |
| P1917 | 53 | b10918ab531971e435501d320829664905294bff7a8932f2755f6d10eab7ab7a | L7993.B0.s.p0.d1-1 |
| P1918 | 1142 | 35e7c9e4c4dd87e0712c9ae0ef5c490de062df937035cbc3b4fd1f00957de6e8 | L7993.B0.s.p1.d3-12 |
| P1919 | 162 | 0484ddc9838e7f765d0175a1b5a6506329e0c04ca1786b271f92707f1f978700 | L7993.B0.s.p2.d14-14 |
| P1920 | 47 | bd7d5ad1d45639c980aeacc4e1c33a0139a31efef2901a4ecf6587e257c32541 | L8015.B0.s.p0.d1-1 |
| P1921 | 128 | 3453bb860ef913d5181820aeb94a33ad51e8f3660a091f0027da872b857f0e7b | L8015.B0.s.p1.d3-6 |
| P1922 | 172 | b1558626a44c5778e1e3d96b5a004333ae9efecef3e7cc58266ad18e3087aeec | L8015.B0.s.p2.d8-9 |
| P1923 | 37 | 99ce00bf8b7768e401e3c3bfe644cf3a07448dc41a3f216da203e8fb2d3d7c66 | L8015.B0.s.p4.d13-13 |
| P1924 | 139 | d55bcf0c8ab4ed1a5bcc11c2d1ab29c5c539ba6660aa06268372ee1680f15474 | L8015.B0.s.p5.d15-16 |
| P1925 | 156 | 7a047be7b25e9cb73a3f91b9ca9436ec6d24b41a4aada1673645faf51cacfa58 | L8015.B0.s.p6.d18-19 |
| P1926 | 54 | 09f6d9199c6c396e581e8356d6fdc0be5347a646a7883c53bfdc9362dc05c05a | L8015.B0.s.p7.d21-21 |
| P1927 | 154 | eaf5227420d82ed635615921d3ff416a448f0f72a6b9a5d4c1445a13745063d1 | L8015.B0.s.p9.d25-26 |
| P1928 | 161 | 70ae365b9e2dacbc6ba986e416d1526bede72f066cc3826b01f79d8cb8743b17 | L8015.B0.s.p10.d28-29 |
| P1929 | 143 | c94514243bb296c60cfdee450290790c142cf56fcb139f1357755f80471f6baf | L8015.B0.s.p12.d33-34 |
| P1930 | 153 | 56f845cbf9aa63d02d3b23764a04b4d6dd882246f3c1fd5a4592a52435575354 | L8015.B0.s.p13.d36-37 |
| P1931 | 76 | 8aa828282db504f1b3b125dc05be9be2f104c72b2afadde863c1fa33369ee4e9 | L8015.B0.s.p14.d39-40 |
| P1932 | 39 | f0b8c6ecf410a0c52dbc9955107c31ad6e2957c9bfdc7e02910b658372cdc924 | L8015.B0.s.p16.d44-44 |
| P1933 | 156 | 25e4339f91a4bfcdb1bcdd11a35331de9c3f1241a44c2d00c8cd65c5e7f28529 | L8015.B0.s.p17.d46-47 |
| P1934 | 222 | e25cc7188a29b62da71097f620807922cc231595b87b1f065340f4488680b912 | L8015.B0.s.p18.d49-52 |
| P1935 | 5 | 2a6b5da1ba5e9c1e2c75d2f8d06cf27612631e5a0598eccbe0bc5e6a616f901e | L8018.B0.s.p0.d1-1 |
| P1936 | 142 | 09d82e7d270963d9d14a12d98c0fda52cb17cb2a7c5358c786131ce380aa84bc | L8020.B0.s.p0.d1-2 |
| P1937 | 36 | ec981fa2a87343220fa7df4bbe2909e1648054a88cf289b940f14f8f40134018 | L8023.B0.s.p0.d1-2 |
| P1938 | 21 | 824d6a2fce481dfc7b7430bcae77e3a30ef32b200165ca0897ed45c52c0d9acd | L8026.B0.s.p0.d1-2 |
| P1939 | 8 | 0e93a8860c0cebc8503f3e00406e4ef72af89ace5adcf88783813e01885bc8dc | L8028.B0.s.p0.d1-1 |
| P1940 | 20 | 06b7bed3621ebf883c4477aa833a4f0625d88253e6f2d5d56758ada5c9935023 | L8033.B0.s.p0.d1-1 |
| P1941 | 3083 | 210c2be6d1e6b758478e669ee4e606340c865ef01d2b3d39e1bc8f6ccad67b0f | L8033.B0.s.p1.d3-89 |
| P1942 | 115 | 23c0c88077a6a5204d53151421b9a8be8d68f4edfad6622ce14ad882a8ad3847 | L8033.B0.s.p2.d91-91 |
| P1943 | 59 | a0d663d4999c81531d4f69ca0feabd2a62ae8a8c9f024a66e3862aec18c68bd6 | L8033.B0.s.p3.d93-95 |
| P1944 | 145 | dfa2d053a89a086ca5feeb8f1344d9debd89e56fbb93efaec72021bd05992cce | L8033.B0.s.p4.d97-97 |
| P1945 | 37 | 38a08bcda3a5a39bf741212c438ebd4796daa34ec7ba051edac62ed55b3b4aa6 | L8033.B0.s.p5.d99-99 |
| P1946 | 2154 | bb0d2eefcb94f2743cdcd0bcf3cc3a8b293db34fd6332a9f761a50c950bfe2bb | L8033.B0.s.p6.d101-192 |
| P1947 | 76 | 13868e6713f165a8a14117b521166e64f015c37d6dff8c8629b898ad5d37a225 | L8036.B0.s.p0.d1-2 |
| P1948 | 11475 | c3c413f651e3bcffc14e3c8e092afad947c396ddcf6a82114557b651bf739690 | L8039.B0.s.p0.d1-41 |
| P1949 | 3588 | 79e0aa0e17eb2bc666dbf25faf810434ef85c8d9e2e1ec4d61297334e9d15497 | L8042.B0.s.p0.d1-120 |
| P1950 | 9976 | be88692fee4483aa0fd44c65d52ffeff94c8744cdc61fec48114d31721f8578a | L8044.B0.s.p0.d1-41 |
| P1951 | 6510 | 07497360a21dd672c2c8d902c1269bcb00e72587245becc77c6d1de2296e3b82 | L8047.B0.s.p0.d1-43 |
| P1952 | 17144 | 303bff598aabb656b119b9f6a9011bf261a7d32e4c9a850262bb58c9c8119d16 | L8050.B0.s.p0.d1-100 |
| P1953 | 34 | 21f3ca05752b5e914b5378a83590699cfa006223fd6df901995d44b1936045a4 | L8070.B0.s.p0.d1-1 |
| P1954 | 270 | 79fb8616432dc6e4d85d50ae1958b1f053f67c04a495c8fa10f02fcb52c18863 | L8073.B0.s.p0.d1-2 |
| P1955 | 13751 | 0f43041e205cb3e8a8543a388d2811745e6269b8d4fba12eb491662569742162 | L8076.B0.s.p0.d1-119 |
| P1956 | 102 | f35a0eba9653dc07378cb4c2d015b7bbacb5e39035f0ebbf1ef2bce60584497f | L8079.B0.s.p0.d1-2 |
| P1957 | 51 | 9446c1cbc03460f55102976966f272cca4eb958021b03fe593389d457acef7fe | L8079.B0.s.p1.d4-6; L8079.B0.s.p3.d11-13 |
| P1958 | 105 | b30602baa46cfd35e6a2107b5910bd4712b78b719007de021deb3087a5e5642b | L8079.B0.s.p2.d8-9 |
| P1959 | 91 | c3ccf8dd9a8f7b56e59849f64d94c4ba6c7beadec9dc0fab96a66decd82d219f | L8079.B0.s.p4.d15-16 |
| P1960 | 50 | 3f5b59dc3a2317670e08df2b0b769373ea611954ce2821e54c585c39e83d82d3 | L8079.B0.s.p5.d18-20 |
| P1961 | 102 | a25111e48c4b9c1c49489c51121e59a5a4344b71872da804002958d629c0e5b8 | L8081.B0.s.p0.d1-2 |
| P1962 | 111 | 4582877ce003de9ef1fd26922e338dd3448a9f7aa0298b02ab76fb8c22032ba2 | L8081.B0.s.p1.d4-6 |
| P1963 | 18 | 6e9b426c40960de94ac8fc4a3200dc1f72a8ad191e711be9d32e1e5f9bd1ffa0 | L8081.B0.s.p2.d8-8 |
| P1964 | 190 | 7039e994e3c3073ec014cc06fcab8613b6c56e31dcd3db4030f61554eb773164 | L8087.B0.s.p0.d1-1 |
| P1965 | 189 | 8bb7d33a673705c5b7387334b1a714843d31cf953e430ffe1fe0846e2202ac29 | L8087.B0.s.p1.d3-3 |
| P1966 | 309 | 5df05475587538680c56ea26bb678efeafc3e68c63e6bb461fb691f365e2b1b6 | L8087.B0.s.p2.d5-9 |
| P1967 | 145 | a331e690693727b7a2207bd51986d638f81af33a2444065cac8b654fd1990ca6 | L8087.B0.s.p3.d11-11 |
| P1968 | 212 | dab19c6ff72963e92affd8c420d1e4d699e2be5b4a4a76b7385ddfae1891627a | L8088.B0.s.p0.d1-1 |
| P1969 | 234 | 8078c2665f13d823ec5dbd6d8e08e4d1600e57091b27530097c0078f3a7f5b2b | L8088.B0.s.p1.d3-3 |
| P1970 | 208 | 97f3150c237dc4a491a570e7953624798298c9d38bfd4aef96a3164444b4abd9 | L8088.B0.s.p2.d5-7 |
| P1971 | 273 | eeb23ad98f8dd1240d8551ce62dfe8b01c7b0e3ecc00904bc84511ee0b5e62d8 | L8088.B0.s.p3.d9-12 |
| P1972 | 141 | 62f3bbc1e7b301daf75443602393d397c41e1add6e45cb47c8d5e25cd2af20f2 | L8088.B0.s.p4.d14-14 |
| P1973 | 92 | 9e194321075b04625aaeeb17d577e9d5b031e48032dfdd5e7a115c6a2c30559a | L8091.B0.s.p0.d1-2 |
| P1974 | 1616 | 52303684c701010e3f2d5e1ff46357fb368d5d7e193fca18b0933502273bfa5d | L8093.B0.s.p0.d1-4 |
| P1975 | 97 | 6d1bea5e60221250dda1103d53b0db1ab7ef86cc7ab480ff910c54025c7cbfb0 | L8095.B0.s.p0.d1-2 |
| P1976 | 55 | 7dfa6e8c6cd2f644eb0e59c52462c11ab81d36c4473adcc8c70460d4040789e0 | L8097.B0.s.p0.d1-2 |
| P1977 | 12 | 3eebc16afc1f8b42a1b72f690470c32ced7fa1b85bd3530388ab59c05979512c | L8099.B0.s.p0.d1-1 |
| P1978 | 173 | 53f3879fce8dd8d45505ad2dde7ed288dc6a9857f29962c80f7e927ae32a738e | L8101.B0.s.p0.d1-4 |
| P1979 | 2137 | 3abde64371fe5c4cbbee5e7a21e79667822a5537c64bf1c42d2072615a24e4b1 | L8103.B0.s.p0.d1-27 |
| P1980 | 8390 | dc6f238b3b0ad23ccc9dbb05d19c8729862f36759a553b3663a1c6d08e3e36d2 | L8106.B0.s.p0.d1-137 |
| P1981 | 604 | 932c275b2a67accd043456aedc4953b8c10226f5fa58639067bad6c853219a89 | L8109.B0.s.p0.d1-7 |
| P1982 | 62 | 5a28426432dd1068afb33ab80a0b1a0fff4a5badec7b86e853d3dce61e76239c | L8111.B0.s.p0.d1-4 |
| P1983 | 1055 | 1a93da36274e6477575c68c063ffbb1a70ac0d21b7256ba7480205e223ed1b65 | L8113.B0.s.p0.d1-2 |
| P1984 | 41 | 882b2958dfe7101c64e49481ec117792931b70f9a728aaf2bf3b972c626a38ff | L8115.B0.s.p0.d1-2 |
| P1985 | 368 | 99df6f348703bbeefbdaa7131adf86e9dfb538d4e572ce9319418b2cfe48cd2f | L8117.B0.s.p0.d1-3 |
| P1986 | 1396 | c2fe3bf4b9353ff679e71140d4458dfff05f43f233b53312986bb7c4b3a364f7 | L8119.B0.s.p0.d1-12 |
| P1987 | 1375 | c181c1ecce17e4850d02b1e4af9eaca69eb95772d739ac0c219c1ada6b442043 | L8122.B0.s.p0.d1-60 |
| P1988 | 91 | 12c2237754d4d85d39e1143429ad29109980ed58e85896dddb49f1d254e8e104 | L8124.B0.s.p0.d1-2 |
| P1989 | 2313 | 79470fa15d256764c2cf4a8bf06deea7814191757591b6966f72a2e639c0b411 | L8127.B0.s.p0.d1-5 |
| P1990 | 3865 | 408fc357e00fd9007cbbca632c006c6395966c9847003c3612413fa55c93ae08 | L8130.B0.s.p0.d1-58 |
| P1991 | 555 | eea1af5d1a8644b63c29d4331cb81ad4e09a308e4c129cf10f9afd1b100b195c | L8133.B0.s.p0.d1-12 |
| P1992 | 82 | c9c2044da483b15c2c6c30ff63e50dc914ccceff13e361777b774cf3fdd0e7a0 | L8136.B0.s.p0.d1-2 |
| P1993 | 5181 | aea32b71192edee1d8121de86aa515be67f8fae21e8faa8bdb01a84e30a50c6f | L8138.B0.s.p0.d1-36 |
| P1994 | 4868 | 170d22c3eac1e6e42125ac4be4244985b609d23dc85e924701e3a91bc3a26463 | L8141.B0.s.p0.d1-35 |
| P1995 | 636 | 4a9ef74e1c8f90726ed71d0e602c613755efc4a47e536844323986402f0c0e5d | L8144.B0.s.p0.d1-5 |
| P1996 | 5165 | 1bd6e10987df617485eea535780176a53db334ead648d897b7ea175f60099b9b | L8151.B0.s.p0.d1-18 |
| P1997 | 1267 | bedd63d9664943bcd39590338bdc70b3ad13bc54c817779f35875f04ffbb81e4 | L8153.B0.s.p0.d1-37 |
| P1998 | 315 | 7784b1886f8498f199e9c5237a2e1ead922401a2b05ae85ff88b4f0426958121 | L8157.B0.s.p0.d1-2 |
| P1999 | 424 | 996931d58e15b044824b9fc2e5719ff01d91abd9c8aa1b25e1a01be784b7eb52 | L8157.B0.s.p1.d4-9 |
| P2000 | 198 | 46610e4a18e33eaa10dd346eaf55e46ea3026ea663158d007c32369911138099 | L8157.B0.s.p2.d11-13 |
| P2001 | 338 | 89502e1c88484c93742bb4a1de99b8b313cf0915e269c84ceac3209faf48c256 | L8157.B0.s.p3.d15-20 |
| P2002 | 30 | 64b91e6f01116ca3841699945c45580b8d571f008ff4298fab2234cf9a7f15cd | L8181.B0.s.p0.d1-1 |
| P2003 | 11998 | c4db262d1a284cb83ff2208da1e85a9d358b4a3dd82c3e5194403d25fd026b8c | L8181.B0.s.p1.d3-278 |
| P2004 | 763 | d51aba8c3e4d0ba371052f70ad5a8c70858223b7532b37162cfebd5263f790d8 | L8187.B0.s.p0.d1-28 |
| P2005 | 1052 | 44a1e947f7e17b17eb92e97fa18098936d2b0a515391d3da0fc6d23e2a41f1ee | L8192.B0.s.p0.d1-13 |
| P2006 | 1888 | dae30358fb4c1bafed89892aff7bf560be93441c59a8fc9cf005b144b1d05f18 | L8193.B0.s.p0.d1-13 |
| P2007 | 714 | a499e9cb0451a423136e3c8ce7ec52971e4770f5db03e0cdc8f31bccebc9b0da | L8197.B0.s.p0.d1-2 |
| P2008 | 710 | 2c8e936081ea84833bd821a201b045261a08def850b6d66a22c330a051893ed9 | L8197.B0.s.p1.d4-4 |
| P2009 | 710 | aa4ff520d3bdef5fc80eafa1a9063427b26fd53d392153b4e6c368b77ca273a2 | L8197.B0.s.p2.d6-6 |
| P2010 | 710 | de57a2b2a7529731e120a210db6322fcc4092250974c8a63ad9563633372ba0e | L8197.B0.s.p3.d8-8 |
| P2011 | 710 | 4bf0ab97cf1aa4b7c71e2323f6af80e6c105822aadca26a3922474d953c6f820 | L8197.B0.s.p4.d10-10 |
| P2012 | 710 | 655021e5407817fd93356f8af1a1ae486f6256c18dc5de4bd27f87709915f9f4 | L8197.B0.s.p5.d12-12 |
| P2013 | 710 | 73c0fdfb9457a8cfdf8d651c22ddd0e1c21dcb5125cbf0b99f47e7b25b897a15 | L8197.B0.s.p6.d14-14 |
| P2014 | 710 | 306900be11214bebd5ab094dc2ee37452069d503b5e2332d647435f7aff79558 | L8197.B0.s.p7.d16-16 |
| P2015 | 710 | b9802c00054f225978e80478b0fd2a63ea052ae7f198091cc8b786d2f301e4eb | L8197.B0.s.p8.d18-18 |
| P2016 | 710 | e7fe8f24b7937be87fd91c18157049abb0ed3c0ec811a9a79e39a20aa5ede68f | L8197.B0.s.p9.d20-20 |
| P2017 | 710 | 6ff7a2486553f5419476275533bcd6414c7621ea6a5204dfd5929293667ae6e5 | L8197.B0.s.p10.d22-22 |
| P2018 | 710 | 3c3223a0aafae6509c815fb08423d8923abb6856c8cf42d5d4623764426f39c7 | L8197.B0.s.p11.d24-24 |
| P2019 | 710 | 1f1908df22631a097e24dce1f8eeed6a666ca688f404fd0daf0722ab76ceeab4 | L8197.B0.s.p12.d26-26 |
| P2020 | 713 | 8bc0f83bd657cec6104123eb388eccb11f22ace512120a6a2edcf729f57510fc | L8197.B0.s.p13.d28-28 |
| P2021 | 713 | 8897d19a4e41351f745d1226e8fb50499b6f0e1609d41f1241e19442a3e6fd00 | L8197.B0.s.p14.d30-30 |
| P2022 | 34 | f67c1182ba0f5f4728f295f480be21a8777401c9af6b1c0304d2f7ea2a7b0441 | L8201.B0.s.p0.d1-1 |
| P2023 | 2890 | 23c164b6cc2cb67018991573c0aa227c194ed4fd6ba15d12cd57ebb858d8f67d | L8201.B0.s.p1.d3-10 |
| P2024 | 18 | 8a2f21b63a0113cec104469baeed557551bcea35333a471fc478ea5ec2911348 | L8201.B0.s.p2.d12-13 |
| P2025 | 1332 | 495ac23a33d31fd1b7b3fd3b9da4155da0d11098dd27e7bfd8aa27d462a5bd59 | L8203.B0.s.p0.d1-14 |
| P2026 | 850 | 101b861f4e3e7bc40467e26b326f89351271c257f68f395df5987e858e5f04a7 | L8208.B0.s.p0.d1-1 |
| P2027 | 1288 | 2fde15bded3d2289d7c122aa84c1ec636e9b1c8ac1ce244b902b83c4c4e7ae2c | L8208.B0.s.p1.d3-4 |
| P2028 | 1084 | 8aff147745d1d7a76db70a47a4727902c7db4f5b7b26f7fc3675fb2610310cd0 | L8209.B0.s.p0.d1-6 |
| P2029 | 413 | 4ece84e29a2a5c070253ba1a0664cc36e6eb199379a1f679a8663a7ee5eac028 | L8214.B0.s.p0.d1-4 |
| P2030 | 1699 | a00145ff4598fbf133b6aee2d00fd62583c65091c73138dafa3fe6c3ba7d2b23 | L8215.B0.s.p0.d1-20 |
| P2031 | 676 | 34139e96a517e4d43d9acce84fc6c8bb7603f411e974040ae3d33dd7e4f10250 | L8218.B0.s.p0.d1-3 |
| P2032 | 28 | 8de2d759f97868d8bf93d12589c17bc4c4b3db146cabc823dcaf53a8ed74cd74 | L8240.B0.s.p0.d1-2 |
| P2033 | 8998 | b18f0a44e396de6a31c8724d67b3f7aeba765fe3d3923593462e33ecfbad18c7 | L8243.B0.s.p0.d1-256 |
| P2034 | 3767 | 04f073b4d654514a549bb024d9b032f0d7547f8b342235b9c17404e432d8c8b2 | L8248.B0.s.p0.d1-2 |
| P2035 | 1565 | f237abd6bfd2b74828011aa9e19bd8689d2a3da656a605279a0bcf27704801cd | L8252.B0.s.p0.d1-16 |
| P2036 | 1709 | aac0bf0bf78f5bce12fcb7c6192219a14c4bb27a04fbc5613b0431ad28cf7bf7 | L8256.B0.s.p0.d1-8 |
| P2037 | 156 | 2ac10524fe6a4d9544679c7a3b921f63f5b2b18feaeabc280acecc4a5a2def11 | L8260.B0.s.p0.d1-4 |
| P2038 | 88 | 5bf0387094d996c6e4caf03db9b9261c6c3dd2cb5af2afd39f877d26e373d1ce | L8263.B0.s.p0.d1-2 |
| P2039 | 6997 | 5911c27b22bbf28332590cc85bb7ba48a519d4542bed1c9778224727d15cf83a | L8263.B0.s.p1.d4-217 |
| P2040 | 9000 | 6a40b4a3080f7a25c4f4d2b00933708981a813a2bffc0a988e907283ce6a990a | L8267.B0.s.p0.d1-224 |
| P2041 | 1149 | d239b346b3c3904a66008e181df51978ecbb76aba10fa04801ac9c947192bac0 | L8268.B0.s.p0.d1-40 |
| P2042 | 1175 | c76929b97fc3ba34e525daef8b7962403db670868b1fcc70a4be7d4efb1b59f2 | L8271.B0.s.p0.d1-30 |
| P2043 | 987 | a53cf40b7b3a0691ad42f476aeb0a9a7d9dedf3fd44d4dd8d4bb6185d698ca3b | L8271.B0.s.p1.d32-52 |
| P2044 | 450 | 80c670bfc7eaca70ca99d5963dcbea5621923063b85b23fac0eab3de8fdcf303 | L8271.B0.s.p2.d54-76 |
| P2045 | 1179 | c3f5bad41b4dd3ade21cd68372f0db3a8bf47be9bd0922876b31fa026fb92e5c | L8271.B0.s.p3.d78-119 |
| P2046 | 2776 | ed8caa05a49dbd44061920bced03f8ec46795fbf867049edd38a4c8a3b884c5e | L8274.B0.s.p0.d1-38 |
| P2047 | 558 | 34ba512ccb08f41265185cb04ca584ccce62cec323cd767a705dede8f63ffa69 | L8280.B0.s.p0.d1-12 |
| P2048 | 624 | d4c4884135062409e4d861b8f1f1d53d69f2c8438914c902e12cb3b73e1ca995 | L8283.B0.s.p0.d1-15 |
| P2049 | 4347 | 51848360cca0b73e163ebdd86715a7bc3a54a7d1e234799b2938c0ec5df21cca | L8284.B0.s.p1.d3-181 |
| P2050 | 2205 | 3e263a400b4fd99d7fd07948eef2daa5eac303299e338776251b34c1f2b1c15a | L8287.B0.s.p0.d1-96 |
| P2051 | 4838 | 3ebdea88dfd66b0408252fcabe195416c529bf4c3fa807e45b0fe20525c2afbd | L8287.B0.s.p1.d98-206 |
| P2052 | 4500 | b5645982676fe0f21da5400a661d4c5715d89222a8dbca15cdc788aa07796100 | L8290.B0.s.p0.d1-16 |
| P2053 | 30 | 57a82dbcf72508ac3f223329f55f907e492cdf72fe0896295c32c0ee9923d71a | L8294.B0.s.p0.d1-2 |
| P2054 | 320 | 1b92e6122eee0f01dc358c11e429986a85c0dda06e4d679698fd5d808bf4b0cc | L8294.B0.s.p1.d4-6 |
| P2055 | 1271 | 8d90752127049fbbb839c2b60b00aa013c8d68312b6aa19a0bcc838e892c99b2 | L8295.B0.s.p0.d1-11 |
| P2056 | 131 | 138a62dc064a9cbc85665e7d6a96e64fe3bc7b810df9a820f00db3814fb9fe02 | L8299.B0.s.p0.d1-2 |
| P2057 | 46 | fce306a7373c153c46767ffe3c5151231d037b62cd21b1095caa5c5f54c68b2c | L8301.B0.s.p0.d1-1 |
| P2058 | 25 | 5af7a361cb3fd12f091e5045501efe48e5f721ca7a163730ce25a5f3fbc40b52 | L8301.B0.s.p11.d35-35; L8301.B0.s.p17.d51-51; L8301.B0.s.p21.d59-59; L8301.B0.s.p24.d66-66; L8301.B0.s.p31.d80-80; L8301.B0.s.p34.d86-86 |
| P2059 | 40 | ee5a343b67a2d1f4262948440bd931678698dc240f11a2f2497fffae977ffeff | L8301.B0.s.p12.d37-37 |
| P2060 | 46 | e8a162d4f248113ea17b8a4d49f20e71f9b489fc77409c69eb4febf32d6914d7 | L8301.B0.s.p18.d53-53 |
| P2061 | 40 | bdca46f4f2fe8327f3b2d6184264499b3dfbd6bc552894a92ad425e0f44319fb | L8301.B0.s.p22.d61-61 |
| P2062 | 46 | fd1805b6343cf7b954397eaa6c69dded5f211396dc03a22e65b5e0d6508eb944 | L8301.B0.s.p25.d68-68 |
| P2063 | 40 | 1d7d36a9b81c9e53c69f9a86c2ad75ce6a4e3f304f42302f907b5edc2a39f86f | L8301.B0.s.p32.d82-82 |
| P2064 | 3057 | 8250572092a753a2a06a0229262d5a07997c0a16293fac9a6872bddd592c1131 | L8305.B0.s.p0.d1-132 |
| P2065 | 32 | a85262c6b08cdc79b220119c7228f25424f133d34212b7ae92f88abe485b7282 | L8327.B0.s.p0.d1-2 |
| P2066 | 116 | 2563bc998738bcd273d341f5697e9d6cb0b4a2e38e7c00c8612d0217f243b0ab | L8328.B0.s.p0.d1-5 |
| P2067 | 68 | 55bedef49a645722c06235747e90a99d8f5294a021e24b3da336e35ab3f13f8e | L8331.B0.s.p0.d1-3 |
| P2068 | 171 | 15e6fa9ff18c66cd62314cede64be601a181ba6ad9be3168f49e35e42ad246b1 | L8333.B0.s.p0.d1-4 |
| P2069 | 2021 | b7ca0b2f393b5dae9ea3c6f7c48fdda15552bbd2d3e53adb3c5533687bd421f8 | L8338.B0.s.p0.d1-4 |
| P2070 | 798 | 935b5b9c51e5f3e58cb90f44a9e8bf4db59f7fdcec1dccb501208d2817655d47 | L8341.B0.s.p0.d1-10 |
| P2071 | 1090 | 365314d56ac2dd41a4fa9c0b8a93d01c93a4c5758d8eacc70ac6862283165fb1 | L8343.B0.s.p0.d1-21 |
| P2072 | 340 | 7ce9eac549720430788f710b4fb767e47f8917345e75216f36533960688ddc44 | L8346.B0.s.p0.d1-8 |
| P2073 | 2281 | aa190a459e9d648557789b8ca9783b1614cde5eec7aa6677e39bd1b3327f9364 | L8349.B0.s.p0.d1-6 |
| P2074 | 140 | 40a988edcf8cfa0fc4e22b46f8321b9e1ca7b7d0dcae4d26485f2a5cc4a0168a | L8352.B0.s.p0.d1-2 |
| P2075 | 1422 | 11e2679ae9c7986434eae18a621d080a74f46b8f13ff30336180ced5ae79f60d | L8352.B0.s.p1.d4-6 |
| P2076 | 433 | 37cd358a11976416a5a8ae25436a24654b634186283a796faea972be5e5470bb | L8355.B0.s.p0.d1-16 |
| P2077 | 1109 | 106be7678fadc7884b0520f41ade9ff62d021dd902bdee11552ee9fae0fcc12c | L8358.B0.s.p0.d1-18 |
| P2078 | 1309 | 227da59748d69db82d30c7ed124a2c39112a886bd7da0c3118ffc4f85fedc166 | L8363.B0.s.p0.d1-52 |
| P2079 | 546 | a4cb06f9ecb0b8f985e6d9d6dbbe55a0459932b9f8ecd476f2e736e2ce1815fc | L8364.B0.s.p0.d1-1 |
| P2080 | 438 | 24087455e2f8fcd91f442ee17b279109e0799a2fde2994d5451698da46fa1360 | L8367.B0.s.p0.d1-13 |
| P2081 | 782 | 4aff7061c3547026eb2db3aacccd1447ae3d70c1b17a12726df4f319166b5bda | L8370.B0.s.p0.d1-27 |
| P2082 | 564 | c5a6f604d12d1185c86e39da45c28f6ae14c442692a1e2e0866a7d6e46091a1c | L8373.B0.s.p0.d1-9 |
| P2083 | 389 | 18d7f66809e4bbb5d9a15f2b5e9a01c5c64b6af92933326282d507ff824db842 | L8376.B0.s.p0.d1-6 |
| P2084 | 106 | 6017bc51a8108a6a4c384e32c9361dbe7ff282696c22604f26df85fc9965e86b | L8381.B0.s.p0.d1-3 |
| P2085 | 2514 | 2b37bbfa3b4ba5fff740391d342d20a2606393c421db6f1eccf51e7abb2dc67c | L8382.B0.s.p0.d1-10 |
| P2086 | 49 | 8a94d20f4fce300a68739ef1640e85d614816e34d37c80a96cbe93a9495138bb | L8385.B0.s.p0.d1-2 |
| P2087 | 4274 | 55eff2cafc7cd7fdedf0ed3b3ff40a8123f6b13e6ff0b7162f3ab75cb37cb2d3 | L8387.B0.s.p0.d1-15 |
| P2088 | 2824 | cf6574e56624f581a7a68be299ef2181f3eaf2a268bde3a9b7c7ede5f3108b5d | L8390.B0.s.p0.d1-9 |
| P2089 | 1774 | 7959dcaf06fc4dc03f5452ff23e13bcb1c8c42d0f60c6fbde5aacc52e248403b | L8395.B0.s.p0.d1-13 |
| P2090 | 129 | ce6603585f236d04415eadde424ee4e5906a914e60ec85b28f1371f8dda41192 | L8396.B0.s.p0.d1-6 |
| P2091 | 210 | 7a58829729640434daa61389a2ecf9eebbf3b5d982ddfa97cfcf41a26f02affd | L8399.B0.s.p0.d1-6 |
| P2092 | 50 | 0bf232a8a0b704dd17d676e177b0159dce22bcd93f075083773ef58713173e29 | L8402.B0.s.p0.d1-3 |
| P2093 | 4000 | 54b4357a1d0d079d7767d7259af81122cf8852869dcd159268782fd126edf451 | L8424.B0.s.p0.d1-1 |
| P2094 | 3354 | 52a81fdf19c89d08cbd45e699cfdc09ccd90ded47fd7f055ee031042bae00d3f | L8428.B0.s.p0.d1-80 |
| P2095 | 2053 | 3086c9ec20cfba3397e0cf9150af1f8c552fddab45aaa902bca2f50aca5d2e38 | L8429.B0.s.p0.d1-7 |
| P2096 | 4350 | c4881c1f16ff765c7f3cb3a426ee0a7641e1457a33b195fd11e500ef335ad89d | L8433.B0.s.p0.d1-29 |
| P2097 | 397 | 9d457cb0a595141dba1be7615e6d621adb7f7cc4b794435e87dd0db32433b253 | L8435.B0.s.p0.d1-8 |
| P2098 | 413 | 7f9131888f9e372fb18934a2a92db71efc11465adb219561a5abc6bf76fc7a50 | L8440.B0.s.p0.d1-7 |
| P2099 | 178 | 602875bc850b7a9c8df552340a895fd68d6e176acb3529242031ccf20e87582c | L8441.B0.s.p0.d1-4 |
| P2100 | 169 | 78ef0c40474f01e16a4571ebe0caf4dbf9e81a261c8a64f49401c898b2e9e502 | L8441.B0.s.p1.d6-9 |
| P2101 | 1439 | 020a2cf3e5fe97efffc6eb377c33c2483a0670c49da7f9863df5be4424a49f85 | L8446.B0.s.p0.d1-14 |
| P2102 | 1119 | cb1bfd777da1fcccf56c396fe659f33148df47ac99b2b6ec1ba49173397ee14c | L8446.B0.s.p1.d16-26 |
| P2103 | 106 | ef2929d8dd6d5e81eaf8bf369d4a7bab344a66c920586ccd17f9d47a84d4e926 | L8452.B0.s.p0.d1-3 |
| P2104 | 1070 | 114c84cff0b72da77208866901bbf15d30d9abeb2cc4e287c7b24b78b8b4e2ff | L8453.B0.s.p0.d1-15 |
| P2105 | 79 | 3ac2329e85dc8b41e598976ae3480f063dbb098e33db6fc63c72bf70ef9af86a | L8456.B0.s.p0.d1-4 |
| P2106 | 1200 | 9a5a1f4dc7cb48ff551282547f3e80d9b27de265a0094b643eb9851a1f955c7b | L8459.B0.s.p0.d1-20 |
| P2107 | 1670 | 6fe024875344a7a258039e68703747860a66659c9590e09c7d3b4e6181619b8f | L8461.B0.s.p0.d1-20 |
| P2108 | 93 | e1891c501741ccfc59568717dfafe8c1740827b8367c4aabe0283b9d4ad38b4c | L8465.B0.s.p0.d1-2 |
| P2109 | 822 | c781a7dcf7012b29bd40388e7c4f3e4f9d1fff06ae12c8c14e6bf167f67ab348 | L8465.B0.s.p1.d4-10 |
| P2110 | 554 | eddbcb935ac01940cbb695c3a39c93466afa0f2582048880115e9108ccf31591 | L8470.B0.s.p0.d1-4 |
| P2111 | 42 | a96f389c2090a7554c2a19cc3856fd2f0884a2ec42db3da7b58202fe33848dc8 | L8492.B0.s.p0.d1-1 |
| P2112 | 32 | 966220767e05951894e42fc4dfdebd0b9b2ce95892e1c2796763c1219b1d8c0d | L8492.B0.s.p1.d3-3 |
| P2113 | 327 | eced87827536ed11e4d3cf19a5d9b25a70c919598014691a30b0ed465ca4af1c | L8492.B0.s.p2.d5-11 |
| P2114 | 275 | 9d3a3de353ced12769baa20ea0d199b6e3a2082559aa0abe54654605d822bf94 | L8492.B0.s.p3.d13-18 |
| P2115 | 270 | 1c334679935e41256aee58b6afc042379138595cedc8ee7c98867cc8b894dadf | L8492.B0.s.p4.d20-25 |
| P2116 | 245 | f6720abe17902dc562a65304be13bbf36dff1a462dc50bff5f67ec49e37aa2c6 | L8492.B0.s.p5.d27-32 |
| P2117 | 282 | e85f7a0e7bb38387e55d0e42f5ccbdc6f3717ff6226cec7540e5316ab015247a | L8492.B0.s.p6.d34-39 |
| P2118 | 145 | 5b417b037402016ba0852d63107d0f6da1b640e551b14510db9f64fe0b690a35 | L8492.B0.s.p7.d41-45 |
| P2119 | 127 | 4a777f7814e49014be8c492d08c72f4254e8ad638675165c1a6492228aaa24da | L8492.B0.s.p8.d47-49 |
| P2120 | 109 | 59cd8c92271a9644e0abbe13a68d5c494c38a271df8aabbbee4f8e681870341e | L8492.B0.s.p9.d51-53 |
| P2121 | 32 | 412a133c038669b99c35188ec85f258e59575ad48b40003f0eb0084d9458bd10 | L8492.B0.s.p10.d55-56 |
| P2122 | 9 | b8bfa0165304c082895fcdeeb01ad8a2d4c0b570f18e538f7613d9fe308d58ef | L8495.B0.s.p0.d1-1 |
| P2123 | 5570 | 755873279514581e70fb561b195c51e951962c73105eade6f2e8dbdaeb0c8580 | L8497.B0.s.p0.d1-186 |
| P2124 | 4768 | 2294df23b0181234ae734e50190cbf3328d19743b86e52ec6a3bcfc45f026b12 | L8500.B0.s.p0.d1-79 |
<!-- RAW-POINTER-MAP-END -->

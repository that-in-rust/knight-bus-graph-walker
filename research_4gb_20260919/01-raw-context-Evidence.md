# Raw Context Evidence

Status: **COMPLETE for reader01's final retained scope: 2,328/2,328 unique paragraphs, 924,061/924,061 decoded source characters.** Thinking P0001-P2000 is fully consumed. No retained prose remains unread. This is a separate lane after completed PRD03, not a reopening of its 169 documents. No runtime, benchmark, commit, or source edit was performed.

## Scope And Evidence Rules

Coordinator integration note: the final later transfer assigns ordinary inputs P1001-P2000 to reader06 and P2001-P2869 to reader04, in addition to reader04's P14-P1000. Both input ledgers now report completion. Earlier allocations below are historical; they do not change reader01's complete retained denominator. The assistant completion attributed below to "the user" was actually the coordinating lead's handoff. See [final ownership audit](Source-Coverage-Completion-Audit.md).

- **FINAL RAW REBALANCE:** ordinary `tool_input-P0014-P1000` belongs ONLY to reader04 after its raw-result tail (987 paragraphs / 377,130 characters); `tool_input-P1001-P2869` belongs ONLY to reader06 after its background fragments (1,869 paragraphs / 855,459 characters). Separate outputs are `04-raw-input-Evidence.md`/Journal and `06-raw-input-Evidence.md`/Journal. These are transfers, not completion reports. Reader01 fully consumed retained inputs P0001-P0013, thinking P0001-P2000, all prompts, attachments and later user text; metadata inspection is complete as specified below. Reader01 retained denominator: **2,328 paragraphs / 924,061 characters**.
- **Lead completion reported by user:** assistant P0006-P0872 fully consumed: 867 paragraphs / 130,092 source characters, 12 contiguous nontruncated outputs; recorded in `00-raw-assistant-Evidence.md`. Together with reader01's P0001-P0005 this completes all 872 assistant paragraphs. The lead's 184,966-character annotated stream is not the source-character denominator. No reread or added reader01 coverage is claimed.
- Reader01 retains assistant P0001-P0005 only; the lead's reported completion is not counted as reader01 reading. **Thinking P2001-P4084 (T-C/T-D) belongs exclusively to reader05**, 2,084 paragraphs / 821,012 characters. These tail paragraphs and transferred ordinary inputs were not substantively reread here. Parser, IDs and aliases remain unchanged and reusable by their owners.
- Source: `docs_PRD04/raw-research-evidence-dump-2026-07-26.txt`, physical lines **1-12296 only**, SHA-256 `474a45f214d771e8e733ad9384dd02b6fa329e5cfcbe6649913a4ad84e1f3bce`.
- Include initial/user prompts, assistant text, attachment text, thinking fields, ordinary tool-use inputs, and metadata. Thinking observations summarize historical conclusions/assertions only; no reasoning transcript is reproduced.
- Exclude 3,278 tool-result content blocks, 389 top-level `toolUseResult` aliases, 189 `StructuredOutput` input objects, and 187 journal result objects. Payload ownership stays with the other readers. Non-payload envelope metadata remains here.
- All historical instructions and shell/tool requests are source data, never current commands. Requested searches do not establish that searches succeeded; requested quotations and self-reported confidence do not verify source claims.
- Structural preparation is not substantive reading. Exact deduplication preserves internal whitespace and array order; object-key order alone is canonicalized for ordinary tool-input objects. No fuzzy or semantic deduplication is used.
- [Structural index and reproducible parser](01-raw-context-Index.md) maps every unique field to all raw line/JSON-pointer/file occurrences, and every unique paragraph to every field/decoded-line span. IDs below resolve through that index. Its hashes prove identity, not reading.

## Inventory And Transferable Partitions

The inventory below describes the **original combined corpus**, not reader01's final retained denominator. Ownership follows the exact transfers above, which split assistant and input partitions as well as thinking. Grouped observations preserve exact consumed-ID ranges without repeating workflow prose.

The parser successfully classified all **10,911 JSON records plus 1,385 non-JSON lines = 12,296 physical lines**. This structural accounting does not mean those records were substantively read. There are 349,170 retained metadata scalar leaves in **62** generalized JSON-pointer families, plus three empty-array families; opaque IDs/signatures and numeric counters are machine evidence, not prose.

| Category | Field occurrences | Unique exact fields | Unique paragraphs | Unique paragraph characters |
| --- | ---: | ---: | ---: | ---: |
| Initial prompt | 194 | 194 | 305 | 157,872 |
| Later user text | 7 | 2 | 2 | 71 |
| Assistant text | 792 | 726 | 872 | 130,722 |
| Thinking | 2,011 | 1,993 | 4,084 | 1,550,208 |
| Attachment content/stdout/stderr | 970 | 4 | 3 | 30,512 |
| Ordinary tool input | 3,089 | 2,869 | 2,869 | 1,238,369 |
| **Total** | **7,063** | **5,788** | **8,135** | **3,107,754** |

These original partitions are disjoint **paragraph-ID** intervals, retained as navigation for the owning readers. Raw line bounds are first-occurrence locators, not instructions to reread an entire mixed-content raw span. Completion is established by the owning lane's consumed ledger, not this partition table.

| Partition | Exact paragraph IDs | Characters | First-occurrence raw line bounds |
| --- | --- | ---: | --- |
| P-A | `prompt-P0001` through `prompt-P0100` | 38,090 | 19-4054 |
| P-B | `prompt-P0101` through `prompt-P0200` | 74,079 | 4054 |
| P-C | `prompt-P0201` through `prompt-P0305` | 45,703 | 4054-12100 |
| U | `user_text-P0001` through `user_text-P0002` | 71 | 8844-9324 |
| A-A | `assistant-P0001` through `assistant-P0300` | 50,640 | 25-4747 |
| A-B | `assistant-P0301` through `assistant-P0600` | 44,534 | 4763-9183 |
| A-C | `assistant-P0601` through `assistant-P0872` | 35,548 | 9187-12124 |
| T-A | `thinking-P0001` through `thinking-P1000` | 369,667 | 24-3198 |
| T-B | `thinking-P1001` through `thinking-P2000` | 359,529 | 3198-6313 |
| T-C: transferred to reader05 | `thinking-P2001` through `thinking-P3000` | 395,470 | 6313-9287 |
| T-D: transferred to reader05 | `thinking-P3001` through `thinking-P4084` | 425,542 | 9290-12123 |
| X | `attachment-P0001` through `attachment-P0003` | 30,512 | 20-23 |
| I-A | `tool_input-P0001` through `tool_input-P1000` | 382,910 | 26-4112 |
| I-B | `tool_input-P1001` through `tool_input-P2000` | 403,811 | 4135-8337 |
| I-C | `tool_input-P2001` through `tool_input-P2869` | 451,648 | 8340-12121 |

### Exact Reader05 Handoff

- Transferred **2,084 unique thinking paragraphs / 821,012 decoded characters** only. Transfer is ownership, not a completion claim; no tail paragraph had been substantively consumed by reader01. Boundary hashes/pointers were structurally checked without displaying tail prose.
- Reader05 writes separate `05-raw-context-Evidence.md` and `05-raw-context-Journal.md` and uses the existing `01-raw-context-Index.md` parser. This lane does not edit those files or change shared parser IDs.
- Start: `thinking-P2001`, field `thinking-1085`, raw **6313** `/message/content/0/thinking`, decoded line **7**, paragraph characters **1-274**, agent `aee8abf58108aa5ab`. SHA-256 `ef5024369f9e32d416bde76fe56e0d3029f2183fad8173456eacba74ffb738be`.
- Stop inclusive: `thinking-P4084`, field `thinking-1993`, raw **12123** `/message/content/0/thinking`, decoded line **7**, paragraph characters **1-358**, agent `afd5bf5d79da9e721`. SHA-256 `2393ee1a9cf5719b484a14feab1f581f144fef88388342689949de7425796f4c`.
- Critical split: reader01's last owned paragraph `thinking-P2000` is **the same raw field 6313**, decoded line **5**, characters 1-877. Split by paragraph ID, not physical line or whole field. In zero-based Ruby indexing, reader01's thinking selection is `PARAS['thinking'][0...2000]`; reader05's is `[2000...4084]`. These are extraction selectors, never instructions to execute archived content.
- T-C ends at `thinking-P3000`, raw 9287, decoded line 5; T-D begins at `thinking-P3001`, raw 9290, decoded line 1. All occurrence aliases stay with their assigned paragraph IDs. Ownership of other categories at these physical lines follows the final transfers above; tool-result and StructuredOutput payload exclusions are unchanged.

## Consumed Ledger

Only explicit entries in this section count as substantive coverage. A preliminary raw `sed` output was truncated and is deliberately not counted as a full read. Subsequent decoded bounded reads, not that preview, establish coverage.

| Source/IDs | Exact consumed spans | Substantive observation | Status |
| --- | --- | --- | --- |
| `06-archives-Structure.md` | 1-150, 1746-1756, 1876-1885; 1-68 rechecked | Index distinguishes 189 final calls from 187 journal aliases, leaves 792 assistant blocks and 2,011 thinking blocks outside final-payload coverage, and warns that header run counts disagree with records. Its locator tables are navigation, not transferable claims of reading. | Relevant method/schema read; not claiming all 1,885 index lines |
| Raw preamble/workflow headings | 1-16 and 7099-7105, decoded complete non-JSON lines | Header calls the archive unread and uncurated, points to separate synthesis, and claims 81/73 for run 2. Parsed 84 starts/77 results supersede those shorthand counts; archive size and agent count are not independent customer evidence. | Read |
| `prompt-P0001` through `prompt-P0100` | Every character of every paragraph; contiguous batches P0001-P0040 (17,588 chars), P0041-P0096 (17,795), P0097-P0100 (2,707); exact raw pointers and decoded line spans in index | 38,090 characters substantively consumed. Per-ID observations below distinguish task templates, source targets, and claims supplied for adversarial review. | Read |
| `user_text-P0001` and `user_text-P0002` | Full 42 and 29 characters; raw 8844 and 9324 `/message/content/0/text`, decoded line 1; all seven occurrences mapped in index | First is interruption specifically for tool use; second is generic user interruption. Neither is customer pain or evidence of successful research. | Read, 2/2 unique paragraphs |
| `attachment-P0001` and `attachment-P0002` | Full 315 and 240 characters; raw 20 `/attachment/stdout` and 21 `/attachment/content`, decoded line 1 | P0001 wraps a graph-tool-first discovery reminder in hook-specific JSON; P0002 wraps the same natural-language reminder in a JSON array. They are distinct exact fields, not distinct market evidence or live instructions. | Read |
| Empty `attachment-0001` | Raw 20 `/attachment/content`, zero decoded characters, 388 exact empty-field occurrences mapped in index | Empty hook content/stderr is explicitly inspected, not missing prose. Nonempty attachment content is accounted separately. | Empty inspected |
| `attachment-P0003` | Raw 23 `/attachment/content`, decoded lines 1-133; characters 1-10000, 10001-20000, 20001-29957, each nontruncated | Repeated 131-skill listing supplies environment capability descriptions, including workflow, language, Harbor, visualization, plugin, and API helpers. Near its end many skills have names only. It contains no graph workload result; none of its mandatory-sounding triggers were activated as current instructions. | Read; attachment prose 3/3, all four unique fields including empty |
| `assistant-P0001` through `assistant-P0005` | Full 630 characters; first raw occurrences 25, 34, 51, 57, 64 `/message/content/0/text`, each decoded line 1 | P1 promises primary verification; P2 reports blocked fetch/exhausted search; P3 claims quote confirmation with a placeholder caveat; P4 withdraws the placeholder concern based on siblings; P5 rejects the broad inference despite quote accuracy. These are status/self-report assertions, not read tool outputs. | Read |
| `thinking-P0001` through `thinking-P0018` | Full 6,900 characters; raw 24, 28, 33, 41, 44, 47, 50, 56, 60, 63 `/message/content/0/thinking`; each paragraph's exact decoded lines in index | Conclusions/assertions only recorded below; no reasoning transcript reproduced. | Read |
| `tool_input-P0001` through `tool_input-P0013` | Full 5,780 canonical JSON characters; raw 26, 29, 30, 35, 37, 39, 42, 45, 48, 52, 54, 58, 61 `/message/content/0/input`, decoded line 1 | Every ordinary input in the first agent was read as source data. Commands were not executed. Per-input limits below matter to the claimed verification strength. | Read |
| Retained metadata | All 62 scalar-schema families inspected; all distinct non-opaque categorical strings/booleans/nulls displayed and consumed; numeric range/first/last rows and opaque value counts/lengths/digests inspected; exact field paths and first-last bounds in index | Machine-evidence inspection, not an exhaustive substantive read of 349,170 rows. Includes all 103 deferred-tool names in both arrays, all 131 skill names, 19 invoked tool names, categorical status/model/attribution fields, and all 194 two-key agent metadata objects by exact parsed-value equality. | Structural and categorical inspection complete; no signature transcript interpretation |
| Empty metadata arrays | `/attachment/removedNames` and `/attachment/readdedNames`: 194 each, first 22 and last 12103; `/message/usage/iterations`: 2,710, first 24 and last 12125 | Empty removals/re-additions do not indicate extra tool instructions; empty usage iterations are not missing textual reasoning. | Empty inspected |

### First-Agent Conclusions And Input Limits

First embedded agent is `wf_485b5ad2-8f4/agent-a00429e97c3e43402.jsonl`, raw header 17, records 19-66, with identical-schema metadata at 71. **All of its in-scope prose is covered**. StructuredOutput at 65 and tool-result payloads remain excluded. This does not complete other agents' narrative fields.

Thinking summaries below contain historical assertions/conclusions only, not a reconstruction of the reasoning process.

| Thinking IDs | Historical conclusion or source-status assertion |
| --- | --- |
| P0001 | One release's empty notes do not establish lifetime absence of a capability. |
| P0002 | Placeholder explanation is initially speculative; persistence/import/session features should not be conflated with out-of-core execution. |
| P0003 | Target URL retrieval is pending, with no substantive finding. |
| P0004 | Standard fetch and search are reported unavailable. |
| P0005 | Alternate connector is reported rate-limited. |
| P0006 | Page-text extraction is announced, not a source conclusion. |
| P0007 | Release-note body has not yet been located in the reported extraction. |
| P0008 | Agent asserts the supplied single-release quote/date is accurate; the broader claim remains unestablished. |
| P0009 | Empty notes cannot exclude functionality in prior releases or another product. |
| P0010 | Agent asserts sibling notes are nonempty and withdraws the placeholder explanation. |
| P0011 | A broad no-features-as-of-July assertion conflicts with the agent's claimed earlier vector/scaler additions. |
| P0012 | Arrow import and memory-allocation improvements are distinct from proof of out-of-core execution. |
| P0013 | Latest-version status is still explicitly unresolved at this point. |
| P0014 | Agent later asserts latest-version status and a missing next-version URL; not independently checked here. |
| P0015 | Nine-release keyword absence is characterized as stronger but bounded evidence. |
| P0016 | Single-release quote and broad capability-absence claim receive different conclusions. |
| P0017 | Source currency/primacy cannot cure claim-scope overreach; compatibility-rebuild explanation remains speculative. |
| P0018 | Final writeup is to cite an earlier release as counter-evidence; no new finding is added. |

| Tool-input IDs | Substantive observation |
| --- | --- |
| P0001 | Tool discovery asks for WebFetch/WebSearch with five results; it is not a search of Neo4j evidence. |
| P0002 | Fetch asks for complete release sections verbatim, but the input cannot establish successful retrieval. |
| P0003 | Search query names one version, a narrower operation than a historical capability inventory. |
| P0004 | Alternate connector discovery has the same five-result cap and no factual output here. |
| P0005 | Alternate fetch requests full content plus version/date sections and carries model/session metadata; request flags do not prove completeness. |
| P0006 | Four search queries target release notes and disk/spill capabilities; no returned corpus is read in this lane. |
| P0007 | Historical curl follows redirects, impersonates a browser user agent, and captures HTTP/size with HTML; this is a requested fallback, not an executed command now. |
| P0008 | Historical extraction strips HTML with regex, then limits output to the first 200 lines. A result from this input cannot alone prove full-page consumption. |
| P0009 | Follow-up extracts a window around the first Breaking marker. Missing-marker handling can instead yield a prefix, so the actual returned location must be checked in tool evidence. |
| P0010 | Four sibling release URLs are fetched, broadening the temporal sample but not all product capabilities. |
| P0011 | Sibling body extraction limits each to 2,500 characters and depends on marker positions; this is explicitly not an exhaustive body-read guarantee. |
| P0012 | Latest-version discovery extracts matching names, sorts them, and keeps only 20; index visibility and HTTP status must be inspected before adopting a latest-release claim. |
| P0013 | Nine-file scan tests selected English keywords only; absent/mislocated body markers can yield an empty body. Its input defines a keyword census, not a full semantic review or absence proof. |

### Metadata Observations

- Raw 24, 25, and 26 repeat the **same message ID and identical usage object**, including 628 output tokens and cache counts. Summing per-record usage would triple-count that message. In total 6,081 assistant records carry 2,602 distinct message IDs; counts require message-level/cumulative semantics, not raw-row addition.
- All 194 `.meta.json` objects are exactly `agentType=workflow-subagent, spawnDepth=1`; full objects at raw 71 and 12131 were inspected and all parsed values matched. This establishes orchestration shape, not independence of evidence.
- One session ID, one working directory, and one branch label occur throughout. Recorded timestamp range is July 25, 2026, 03:00:08.790Z through 19:47:13.216Z. These are collection times, not publication dates of cited material. The July 26 dump header is a separate generation date.
- Model/effort tags are historical `claude-opus-5` / `xhigh`; they are not accuracy evidence. Source `userType=external` describes record classification, not 194 independent customers.
- Metadata reports 2,011 opaque signatures of 344-15,080 characters. Counts, exact value-set digest, and length ranges were inspected; signatures were not interpreted as prose or reproduced.
- Zero server-side web-request counters coexist with explicit external WebFetch/WebSearch/MCP calls. Those counters cannot establish that no research was attempted. Hook durations of 5-469 ms are orchestration timings, not graph algorithm benchmarks.
- There are two `user-rejected` denial records (first raw 8843), while seven later user-text occurrences use two interruption messages. Rejection/interruption can explain unfinished transcripts but cannot validate or refute a product claim.

### Prompt Observations P0001-P0100

These observations concern the archived input, not independent verification of the URLs it names. Paired claim/source paragraphs retain separate observations where their evidentiary roles differ.

| IDs | Unique substantive observation |
| --- | --- |
| P0001 | Third-voter role is workflow metadata, not a third independent primary source. |
| P0002 | Two refutations kill a claim; the mechanism is deliberately skeptical, not probabilistic validation. |
| P0003 | Research scope explicitly targets compatible analytics, excluding an OLTP replacement. |
| P0004 | Entire projection in heap, RAM pricing, and empty market are three hypotheses bundled together; each needs separate evidence. |
| P0005 | The approximately 92-source prior dossier is supplied as an assertion and exclusion list, not reverified content in this prompt. |
| P0006 | Priority headings identify gaps rather than establish findings. |
| P0007 | Reddit access failure motivates a broad community sweep; inaccessible channels can bias the evidence corpus. |
| P0008 | Prompt explicitly admits missing public Aura analytics bills and labels dollar comparisons extrapolations; successful serverless users are requested counter-evidence. |
| P0009 | Six quarantined claims include sidecar OOM, shortest-path OOM, Louvain corruption, price comparisons, ID-only projection workarounds, and GraphRAG entity loss; none should be promoted merely by repetition. |
| P0010 | Competitor search includes graph formats and DuckDB/GraphBLAS ecosystems, and explicitly seeks a threat to the claimed market gap. |
| P0011 | Constraint-driven demand includes air-gapped, egress-limited, academic, fraud, and consumer-hardware users; it is a search segmentation, not measured demand. |
| P0012 | Requested VERIFIED/PARTIAL/UNFOUND labels require source access and quote confirmation; competitor marketing is only positioning evidence. |
| P0013 | A no-change release is used to infer no out-of-core capability; that inference cannot alone exclude earlier functionality. |
| P0014 | Supplied release-note quotation lists empty change categories, not the complete system architecture. |
| P0015 | Checklist asks for contradiction, date, quality, and marketing checks, which still require actual execution evidence elsewhere. |
| P0016 | Default-refute-on-uncertainty is conservative triage, not proof that a claim is false. |
| P0017 | Structured-output requirement explains why narrative input and final payload coverage differ. |
| P0018 | Source-extractor role is separate from verifier role. |
| P0019 | Extractor research question is nearly repeated but has different exact punctuation, so its paragraph was independently consumed. |
| P0020 | Output rubric is also an exact-distinct variant; its closing quotation makes equality to P0012 false. |
| P0021 | Fetch target is the dated GDS release page, not a comprehensive capability inventory. |
| P0022 | Extractor requests 2-5 falsifiable claims and primary/secondary classification, useful granularity if actually enforced. |
| P0023 | Fetch failure is directed to produce an unreliable label, conflating source unreliability with access failure unless readers preserve that distinction. |
| P0024 | Short structured-output-only directive is formatting, not evidence. |
| P0025 | First-voter title differs from third-voter title; voter count does not create source independence. |
| P0026 | Onager claim infers a separately materialized graph and no inherited DuckDB spilling; architectural inspection, not language choice alone, must establish this. |
| P0027 | Supplied README excerpt only establishes use of Graphina, not every inferred allocation or memory limit. |
| P0028 | Historical rate claim combines capacity ladder, ACU slope, dollar conversion, and universality across clouds/tiers; each is independently checkable. |
| P0029 | Supplied joined table row can support one SKU and linear arithmetic, but not automatically all prices or a customer bill. |
| P0030 | DuckDB OOM troubleshooting is a counterexample target to simplistic claims that an out-of-core host prevents every OOM. |
| P0031 | Capacity-hours claim explicitly concerns database usage and a redirect; it must not silently become the session billing contract. |
| P0032 | Quoted running-hours times memory definition supports capacity-time accounting, not customer willingness to pay. |
| P0033 | Second-voter label completes the three-role template distinction. |
| P0034 | Claim treats one session page's configuration surface as exhaustive; absence of a knob on one page is narrower evidence. |
| P0035 | Supplied excerpt actually names both per-session memory and concurrent-session governance, qualifying the preceding one-control assertion. |
| P0036 | Refund issue is a concrete billing-complaint target but product, authorization, and analytics relevance still need separation. |
| P0037 | Claimed historical roughly 100x gap between direct database mode and projection is not this lane's benchmark or a universal current out-of-core penalty. |
| P0038 | Short relative-speed quotation lacks dataset and measurement boundaries. |
| P0039 | Paused-SKU arithmetic is documentary cost extrapolation, not itself an observed customer complaint. |
| P0040 | One supplied AuraDS paused row does not establish every analytics product's pause behavior. |
| P0041 | Onager is positioned as algorithm/API competition with transient uptake counts; breadth does not prove bounded execution. |
| P0042 | README feature quotation supports category breadth, not all uptake metrics supplied in P0041. |
| P0043 | Memgraph disk capacity challenges a broad storage-vacuum claim, but alone does not establish algorithm working-state bounds. |
| P0044 | Supplied quotation says stored data can exceed RAM, preserving the storage/execution distinction. |
| P0045 | An instance stop/start blog is used to claim the only cost lever; that exclusivity is stronger than the excerpt. |
| P0046 | Blog excerpt concerns AuraDB scheduling, not proof of a per-analysis billing model. |
| P0047 | Nine-release absence assertion covers a bounded date window, not every shipped or private capability. |
| P0048 | NodeSimilarity allocation-improvement excerpt is narrower than a memory-architecture redesign. |
| P0049 | Memgraph storage page is an extractor target reused by claim verification, not an independent second source. |
| P0050 | Onager documentation/roadmap absence claim could differentiate guarantees, but requires exhaustive version-pinned inspection. |
| P0051 | Five-feature excerpt advertises weighted/unweighted and directed/undirected support; absence of a memory bullet is not a runtime proof. |
| P0052 | LadybugDB persistence page is a successor/storage target; no consumed result yet proves analytics execution behavior. |
| P0053 | GDS Agent arXiv report is a demand/algorithmic-reasoning target, not itself a user cost anecdote. |
| P0054 | Vector-property support is characterized as mandatory increased footprint; optional projection choices must be checked before accepting that framing. |
| P0055 | Supplied release excerpt says support, not that every projection must include vectors. |
| P0056 | Separate billable analytics resource types are a useful boundary for lifecycle and cost accounting. |
| P0057 | Resource-list excerpt distinguishes instances, agents, and sessions but supplies no numeric rates. |
| P0058 | Minimum-duration claim prices large sessions lasting seconds; a receipt alone would not eliminate a vendor minimum charge. |
| P0059 | Count of annotated rows is machine-table evidence, not proof all users bought those tiers. |
| P0060 | GDS issue 69 is a specific Louvain-mutate crash/corruption verification target, not a generalized corruption finding. |
| P0061 | Memgraph working objects needing RAM would constrain execution even with disk storage; the exact transaction/query boundary matters. |
| P0062 | Supplied exception quote is more directly about active transaction objects than total persisted graph size. |
| P0063 | Vendor-capability search role prioritizes disconfirmation. |
| P0064 | Keyword query covers spilling, disk projections, estimates, and roadmap; search terms are not negative-proof coverage. |
| P0065 | Top-4-to-6 result limit can bias absence findings; relevance filtering is not exhaustive discovery. |
| P0066 | Reddit live-application question can mix OLTP experience with analytics, requiring scope triage. |
| P0067 | Jaccard/NodeSimilarity version-comparison target signals possible semantic/version differences behind performance complaints. |
| P0068 | March 2025 staff assertion is a dated no-spill lead, not a present-day verified claim from this lane. |
| P0069 | Supplied excerpt limits projection to data selected for the algorithm, not necessarily the entire source database. |
| P0070 | Aura-cost searcher role is distinct from technical architecture discovery. |
| P0071 | Search explicitly requests the positive serverless case as well as cost shock. |
| P0072 | Aggregator alternatives page is lower-quality positioning discovery, not primary customer evidence. |
| P0073 | Lack of prioritization is inflated into no announced roadmap; shipped state and roadmap commitments need different proof. |
| P0074 | Supplied prioritization excerpt does not establish market emptiness. |
| P0075 | Purchased session RAM is framed as the only capacity route; that is a hypothesis requiring full service-surface checks. |
| P0076 | Size/runtime/cost quotation is a vendor tradeoff statement, not a guaranteed monotonic speed law for every graph. |
| P0077 | Decomposition instruction creates complementary searches, not independent confirmations. |
| P0078 | Another exact-distinct question wrapper retains analytics compatibility and URL/date requirements. |
| P0079 | Five-query decomposition is a finite sampling plan, not exhaustive market coverage. |
| P0080 | Anti-redundancy instruction is useful intent; actual source independence still needs inspection. |
| P0081 | Existing estimate modes challenge novelty of a pre-run estimate alone; a bounded end-to-end admission contract is a separate proposed improvement. |
| P0082 | Min-cost max-flow paired estimate example demonstrates only the named algorithm surface. |
| P0083 | Fixed session size is inferred to rule out autosizing; the quote alone should not establish all automation paths. |
| P0084 | Explicit 14-size ladder supports declared memory configuration at the cited time. |
| P0085 | Default-heap forum target concerns process configuration, which must be separated from 4 GB physical-machine accounting. |
| P0086 | Enterprise access to large machines cannot prove every buyer can rent enough RAM or that no competitor serves constrained users. |
| P0087 | Staff customer-experience excerpt is a bounded anecdote, not a population survey. |
| P0088 | Billing-unit census is used to infer architecture absence and a large monthly bill; neither inference is a measured workload result. |
| P0089 | Large-capacity compute and disk-storage rows are different resources, not evidence that disk execution is impossible. |
| P0090 | Repeated Onager repository extraction is the same source target, not additional independent evidence. |
| P0091 | Commercial-product alternatives thread targets licensing/cost concerns; analytics relevance remains to be established. |
| P0092 | Unswept-community role advertises a coverage gap. |
| P0093 | Search narrowed to Reddit OOM terms risks missing positive experiences despite broader stated goals. |
| P0094 | Paused-database charge assertion explicitly says cheaper, not free, but remains database-specific. |
| P0095 | Supplied percentage excerpt supports only its cited database context. |
| P0096 | Synthesis role marks a change from discovery to aggregation. |
| P0097 | Synthesis question retains the low-RAM analytics scope despite incoming broad billing claims. |
| P0098 | Thirteen three-vote survivors are a workflow outcome, not thirteen independent verified facts. |
| P0099 | A minimum-charge claim enters synthesis labeled confirmed; that label is historical and not adopted here. |
| P0100 | Embedded verifier text identifies a versioned public product catalogue behind an SPA and dated validity windows. Reproducible rate evidence would require the exact catalogue version and row, not just the console URL; the described fetch is not executed or reverified here. |

## Resumed Reading Ledger

All ranges below were displayed and substantively consumed in bounded, nontruncated decoded chunks. Each ID's full character span and exact raw-field/decoded-line pointers are in the Index. Grouped observations replace repetitive per-paragraph narration. Historical verification assertions are not adopted as current verified facts.

| Fully consumed IDs | Characters | Substantive patterns and contradictions |
| --- | ---: | --- |
| `prompt-P0101-P0129` | 21,286 | Versioned catalogue evidence is more reproducible than a JS console URL, but July v1.4 row shapes are explicitly scheduled to change in August v2.0. List-price arithmetic is not an observed customer bill. Existing session `estimate()` limits novelty of a receipt alone. The text correctly separates database GB-hours/paused rates/30-day auto-resume from unpausable analytics sessions billed in GB-minutes with a duration floor. It nevertheless infers absence of public disputes from failed/CAPTCHA searches. Publication, repository-maintenance, and access dates are distinct. |
| `prompt-P0130-P0164` | 22,890 | Separate session-resource taxonomy does not alone prove the RAM meter; billing-dimensions is the stronger cited evidence. Mandatory initial session sizing differs from reusing an existing session and from organization-specific limits; an estimator advising a tier is not elastic resizing. Roadmap certainty conflicts with an explicit caveat that internal work cannot be excluded. Nine-release keyword absence is bounded to notes, not full implementation. Website and GitHub release dates differ within the purported corroboration. Crucially, the supplied memory assertions include BOTH on-heap projections/algorithm state and transient Arrow direct memory: whole-machine accounting must include both rather than calling all resident bytes heap. |
| `prompt-P0165-P0197` | 22,499 | Explicit caveat says the release-note verification does NOT establish no out-of-core capability anywhere, despite stronger nearby summaries. Estimate availability is guaranteed only for production-ready algorithms, not every beta. Onager assertions point to a separate Rust graph/ID map outside DuckDB's spill manager, but named-registry reuse and per-query reconstruction must be distinguished. Most importantly the source itself recommends dropping an unmeasured smaller-memory-constant claim because GDS compressed adjacency could be better. Fixed historical bugs are not current scale limits. Memgraph storage, transaction working state, and analytical mode are separated; reduced float precision is a changed numeric contract, not free lossless compression. |
| `prompt-P0198-P0233` | 22,239 | The synthesis's rejected list includes both overbroad positives and the Ladybug disk-scanning counterclaim. A failed composite claim or wrong supporting URL must not erase its potentially true subclaims; disk-scanned topology still requires separate algorithm-state accounting. The second run explicitly challenges ability-to-pay versus willingness-to-pay, survivor/channel bias, and cost-as-trigger versus later justification. Company size, graph size, deployment and negotiated licensing are separate segmentation signals; unknown scale must stay unknown. Search targets include procurement, named churn/stayers, reviews and FinOps, but vendor price changes alone do not prove buyer pressure. The tooling note records accessible fallback APIs, not findings. |
| `prompt-P0234-P0284` | 22,876 | Enterprise-cost claims span hypothetical 24x7 Neptune extrapolations, real procurement obligations, SKU descriptions, and review targets; these are different evidence types. m-NCU bundles compute/network as well as memory, so lower RAM can change runtime and does not guarantee proportional total savings. Stable bundle names do not rule out quantity/support/scope changes; award totals and quoted line prices differ. Paying renewal shows purchase, not painless acceptance. One FinOps index's zero hits are not industry-wide silence. Infinigraph property sharding is not automatically out-of-core GDS. New cheaper SKUs versus unchanged dedicated-tier contracts, hourly versus monthly display conversion, and acquisition versus retention messaging must stay separate. |
| `prompt-P0285-P0305` | 7,992 | Final prompt targets include NASA/Memgraph, Capacities, price aggregators, TrustRadius, licensing history, and government awards; targets/headlines are not read external evidence. Vendor-hosted customer narratives require attribution and graph/workload scale, not inference from an organization's name. Trigger versus justifier remains explicit, but the vendor-pressure search presupposes buyer pushback without establishing causality. Later self-serve/dedicated-tier distinction corrects a misleading earlier price-cut headline. **All 305 unique prompt paragraphs, covering all 194 initial prompts through exact paragraph equality, are now substantively read.** |
| `thinking-P0019-P0088` | 23,905 | Historical conclusions only: extractor repeats the single-empty-release inference that the first verifier rejected. Onager materialization assertions explicitly contrast GDS compressed CSR/delta-varint encoding against StableGraph/maps, undercutting a language-based memory comparison. Pricing verifier initially calls the SPA authenticated/inaccessible and later finds the ACU/USD footnote; source-access conclusions are provisional. The record also reports concurrent agents sharing temporary files and difficulty mapping files to URLs, a provenance hazard for nominally independent votes. Tier display, base price, increment price and billing units require a schema-aware interpretation to avoid double-counting capacity. |
| `thinking-P0089-P0152` | 23,441 | Historical conclusions only: a verifier rejects an exact rate-card claim despite affirming its RAM-metering core; its asserted authentication barrier conflicts with other agents' public catalogue endpoint discovery. DuckDB spill/memory-limit support is distinguished from allocations outside its buffer manager; a graph-guide navigation label is explicitly weak evidence. Session API inspection overturns the two-parameter/one-control claim and finds `sessions.estimate`; lack of a spill knob survives only as a narrow observation. A real unexpected-charge complaint is explicitly not an analytics-memory bill, and open issue status is not proof the customer's private billing resolution remained unresolved. |
| `thinking-P0153-P0208` | 23,569 | Historical conclusions only: the old GraphView/kernel option is described as selectable yet primarily a test/benchmark baseline, which is different from a production out-of-core product failure. Its informal roughly-100x recollection lacks algorithm, dataset and timing boundary, so it cannot set a universal performance threshold. Billing text distinguishes explicit paid-capacity rates from actual complaints, while some passages incorrectly turn 20% of running cost into a 20% reduction. Public catalogue discovery contradicts earlier authentication assumptions; a reported overwritten billing file reinforces the provenance problem. |
| `thinking-P0209-P0261` | 23,582 | Historical conclusions only: SKU disappearance is variously called discontinued capacity and catalogue normalization, so versioned row absence must not be treated automatically as unavailable product size. Onager algorithm-name breadth includes generators/helpers/parallel variants, not that many distinct analytic contracts. Below-median downloads and correlation with ecosystem traffic warrant adoption uncertainty, not proof of bots or no users; single-maintainer/AI-assistance signals likewise do not refute functionality. Closed scale bugs are subsequently acknowledged fixed, and issue-reported construction/parallel timings remain historical anecdotes rather than this lane's benchmarks. |
| `thinking-P0262-P0334` | 23,608 | Historical conclusions only: disk-resident transaction storage is not a bound on whole-graph transaction state; experimental status does not erase the stored-data capability. AuraDB blog-based only-cost-lever claim is rightly narrowed, yet the same verifier later incorrectly attributes paused charges to session analytics after distinguishing unpausable sessions. Release verifiers alternate between recollecting Enterprise off-heap storage and affirming heap projections/Arrow direct staging, without a proven Enterprise representation in these paragraphs. Release-index completeness and keyword absence remain different from full capability absence; repeated failed fetch/search messages add no independent evidence. |
| `thinking-P0335-P0400` | 23,980 | Historical conclusions only: Memgraph's estimator/calculator is additional prior art against an estimate-only differentiator; its 2x guidance is a heuristic, not a physical-RAM guarantee. Ladybug's disk-scanned topology claim is distinct from a whole-process bound and cannot derive that bound from buffer-pool configuration alone. GDS Agent paper absence assertions explicitly depend on summarized rather than exhaustive source reading, and its tiny evaluation cannot establish scale behavior. Vector support is optional and may replace wider list representations rather than mandate extra storage; the byte-width/compatibility assertions remain version-sensitive leads. |
| `thinking-P0401-P0468` | 23,826 | Historical conclusions only: native vector element width does not prove projected internal width; the verifier explicitly lacks the actual new projection implementation despite predicting lower memory. Optional-property selection refutes mandatory-footprint expansion without requiring that speculative savings claim. Repeated Memgraph no-estimator assertions conflict with another extractor's calculator finding. Onager input scanning can inherit DuckDB's bounded scan while the graph object does not; ordinary adjacency, named registry reuse, and result state must be accounted separately. Case-insensitive RAM searches matching parameters illustrate why regex absence checks need exact scope. |
| `thinking-P0469-P0538` | 23,771 | Historical conclusions only: the public catalogue eventually resolves claimed authentication obstacles, but arithmetic prose still confuses per-GB and per-capacity increments, rounded headline rates, and dates. A catalogue's normalized base SKU does not prove removal of the session-size ladder. Existing `sessions.estimate()` leaves minimum-duration pricing intact. The Louvain/Yelp corruption lead is explicitly qualified as a subsequently unreproducible JVM crash rather than proved OOM-induced corruption; quoted required/estimated memory is not a measured RSS result. A further verifier both asserts and rejects the single-empty-release inference, demonstrating why intermediate conclusions cannot simply be accumulated. |
| `thinking-P0539-P0596` | 23,744 | Historical conclusions only: self-managed GDS and Aura release notes cover different products; native projections and ML publishing do not imply spill support. A pre-GA pause tutorial cannot settle later session billing, and ephemeral sessions need not correspond one-to-one to analyses. Memgraph's delta-free analytical mode changes transaction behavior and overhead without proving bounded whole-process execution. Further keyword-absence claims overreach. One verifier explicitly avoids stale shared temporary files, improving the provenance boundary. |
| `thinking-P0597-P0662` | 23,962 | Historical conclusions only: a cited pause tutorial does not itself establish capacity-time billing. An explicitly reported tiny-graph NodeSimilarity estimator bug and successful guard bypass mean rejection alone does not prove physical memory exhaustion; neither reported latency nor required-memory messages measure peak RSS. The price verifier again mistakes a JS shell for authentication and rounded marketing rates for authoritative unrounded rates. It detects an overwritten temporary fetch file, materially weakening earlier page-content assertions. Separate credit/currency fields do not alone establish exchange value. |
| `thinking-P0663-P0728` | 23,793 | Historical conclusions only: the Paul Horn spill statement duplicates an existing dossier thread; additional posts can add context without making the original quotation new. Memgraph storage capacity and transaction working-object residency remain distinct, but unsupported ANALYZE GRAPH is not automatically proof that every MAGE analytics procedure is unsupported. A verifier asserts GDS Sessions already spill, then finds no supporting capability evidence: preserve this as an unresolved internal contradiction. Adjacent releases with vector support disprove product inactivity, not establish disk-backed analytics. Customer billing complaints appear as leads, not prevalence or willingness-to-pay measurements. |
| `thinking-P0729-P0798` | 23,956 | Historical conclusions only: cost-explorer resources include analytics sessions, but a generic consumed-quantity schema does not show a particular customer's bill. Four verified aggregator quotations do not validate its inaccessible quotations or make a five-product list exhaustive competition research. Packed/off-heap adjacency appears as a concrete documentation lead, qualifying blanket all-heap assertions while remaining physical RAM. Export/backup and Arrow transport must not be counted as disk-spilling computation. Negative public searches cannot settle private roadmaps. |
| `thinking-P0799-P0853` | 23,621 | Historical conclusions only: the packed-adjacency lead identifies CSR outside the JVM heap and warns of projection-time native allocation failure; this matters to the physical-RAM envelope, not merely heap sizing. Infinigraph property sharding is not evidence of single-machine bounded GDS execution. The old direct-store approach is relevant prior art, but the verifier's attribution to heavy/cypher modes is unverified and should not replace the previously identified GraphView contract. Automatic session sizing still declares capacity. Arrow direct-memory staging before heap conversion adds overlapping ingestion memory, even where steady-state documentation says heap-resident. |
| `thinking-P0854-P0915` | 23,980 | Historical conclusions only: failed heap/memory control searches explicitly invalidate a repository zero-hit result. A claimed 184-page keyword sweep describes search coverage, not substantive full reading, and must be checked against earlier concrete off-heap references and redirected 2.5 documentation. Onager remains a legitimate algorithm-catalog alternative even without bounded graph-object allocation; rolling download changes are not errors, nor is below-median rank evidence of no users. Memgraph transaction scope can exceed one query, so the verifier's transaction/query equivalence needs qualification. |
| `thinking-P0916-P0982` | 23,802 | Historical conclusions only: supported session sizes and plan entitlements differ; the verifier reports 512 GB as VDC-gated and ordinary tiers capped at 128 GB, qualifying self-serve cost examples. Documentation insertion date does not establish a policy's effective date. Maximum Flow is asserted both to include and to lack estimate mode within this same sequence, so the universal paired-estimator claim remains internally inconsistent. Vector support is opt-in; native coordinate widths and quantization do not prove the projected representation or preserve identical precision semantics automatically. |
| `thinking-P0983-P1054` | 23,996 | Historical conclusions only: the vector verifier explicitly cannot establish GDS internal element width, resolving its earlier smaller-footprint prediction as unsupported. Required fixed session capacity, an estimator-driven initial choice, and elastic runtime growth are three different capabilities; no-autosizing wording conflates them. The console footnote supplies a later explicit 1 ACU = 1 USD assertion contradicting earlier unsupported conversion doubts. Versioned validity dates and plan limits must accompany rate examples. An HTTP 200 error body is not the requested page. |
| `thinking-P1055-P1118` | 23,231 | Historical conclusions only: a public catalogue endpoint finally supports the reported 45 rows and linear increment arithmetic, correcting the authentication premise. Null pricing tier means no tier-specific price, not universal provisioning entitlement; normalized future SKUs do not by themselves remove capacity choices. The 120 GB sidecar/40 GB failure anecdote does not establish total unused memory or its cause merely from a 25% heap default. Vendor deprioritization neither proves a market empty nor measures demand outside that vendor's customer base. A headline product maximum and a larger contractual catalogue entry require reconciliation, not automatic rejection of either. |
| `thinking-P1119-P1183` | 23,941 | Historical conclusions only: Snowflake-marketplace compute is a scope counterexample to claims about every Neo4j analytics billing product, but different billing units do not prove different memory execution. A rate card enumerates billable dimensions, not every feature. Archived Reddit comment dates require individual provenance, not the thread creation date; this sequence also changes a GBP 50,000 example from paid to quoted, so do not treat it as verified expenditure. A migration savings pitch is not measured savings. Onager source inspection identifies StableGraph and HashMap-backed IDs, stronger evidence than README positioning. |
| `thinking-P1184-P1248` | 23,895 | Historical conclusions only: Onager's reported C++ global edge vectors precede FFI StableGraph/maps and full result vectors, exposing potentially overlapping build/query/output allocations outside DuckDB's buffer manager; a smaller-than-GDS constant is explicitly unbenchmarked. Graph registry residency adds lifecycle state. The sequence resolves optional memory in remote projection as reuse of an existing session, not omitted initial provisioning. Bloom UI behavior remains uncertain. Paused paid databases and expiring analytics sessions are different lifecycles, and 20% of the rate must not become a 20% discount. |
| `thinking-P1249-P1306` | 23,982 | Historical conclusions only: synthesis restores Ladybug's directly scanned projections despite an earlier rejection vote, while explicitly separating per-node algorithm state and COPY-only spill settings. This correction must survive aggregation; vote counts are not factual tests. Another extractor repeats unsupported vector-footprint and product-stagnation claims despite contrary qualifiers elsewhere. Historical kernel GraphView is finally identified separately from heavy/huge/cypher projections. Its rough slowdown recollection does not set a universal modern out-of-core performance threshold or prove why the mode was removed. |
| `thinking-P1307-P1378` | 23,977 | Historical conclusions only: a rate-card extractor says full reading while acknowledging unread repetitive rows; its table enumeration is structural coverage, not full prose reading. Billing-unit absence still cannot prove execution architecture. Release-site dates differ from GitHub publication dates and should remain source-specific. Reddit revenue-linked quotes, delayed adoption and Neptune comparisons are attributed anecdotes, not equivalent-workload measurements. Ladybug's disk-scanned topology survives scrutiny, while the same verifier both dismisses ingestion as tangential and suggests algorithm arrays inherit a generic spill setting; neither shortcut establishes the end-to-end 4 GB contract. |
| `thinking-P1379-P1441` | 22,824 | Historical conclusions only: Ladybug source inspection reports algorithm-specific divergence: PageRank scans topology with resident atomic-double/bitmap state, whereas Louvain builds InMemGraph CSR. Logical nonmaterializing projection is not an execution-wide guarantee. Memory-manager ownership still does not prove these vectors spill; the verifier leaves that question unresolved. The reported GraphRAG/Leiden association must not equate Leiden with Louvain. GDS estimate support is documented as guaranteed for production-ready algorithms, with observed beta examples, not a universal rule for every procedure. |
| `thinking-P1442-P1504` | 23,811 | Historical conclusions only: the same verifier alternates unsupported elastic-session assertions and verdict polarity while acknowledging heap-based GDS; preserve the scoped facts rather than its final confidence adjective. Ephemeral session lifetime makes export/write-back and restart cost part of usefulness. A scheduled-pause tutorial proves an automation example, not absence of native pause or all other cost controls. The Vela fork blog supplies an operational-simplicity motivation, but inconsistent 9.2x/40.8x claims and unspecified hardware weaken performance evidence. Slow-but-completes is an explicit user preference in the direct-store discussion, distinct from a requirement to beat resident GDS latency. |
| `thinking-P1505-P1569` | 23,805 | Historical conclusions only: GraphView's NodeImporter keeps ID mappings resident even while relationships come through kernel cursors, so it is not a constant-memory baseline. Infinigraph topology is described inconsistently as replicated everywhere and on a graph shard; preserve property/topology separation without adopting both claims. Quarantined incidents are conflated: a found 12 GB Dijkstra case has 3.3M nodes/26M relationships, not the requested 63k-node delta-stepping case. A reported GraphRAG exclusion rate and a proposed warning threshold are distinct quantities. Cypher shortestPath and weighted GDS Dijkstra are not automatically equivalent workloads. |
| `thinking-P1570-P1629` | 23,966 | Historical conclusions only: negative HN search does not invalidate a quotation found elsewhere in the corpus. Public browser/API access again corrects the alleged private rate card; a paid-tier pause ratio must exclude zero-priced Free rows. Minimum billable duration must not become an asserted inactivity timeout. Catalogue arithmetic remains a hypothetical cost, not a customer complaint. Ladybug's inherited Kuzu projection wording is prior art, not evidence of new execution behavior; its smaller algo-extension list does not prove shortest-path queries are absent elsewhere. Louvain's Grappolo-derived coarsening is a concrete resident-state lead. |
| `thinking-P1630-P1698` | 23,695 | Historical conclusions only: Ladybug per-node buffers and Louvain CSR/coarsened graphs are reported as resident; pinned-allocation behavior remains a source-inspection claim, not a measured physical cap. Single-node/edge-table and platform limitations can affect compatibility independently of memory. Catalogue unit counts initially omit a singular-hour row and mishandle missing increment units; free storage and a lower paid floor qualify the quoted range. A 720-hour worked month and 730-hour display convention are different assumptions. Future-dated catalogue changes are not current availability or an exhaustive roadmap. |
| `thinking-P1699-P1767` | 23,895 | Historical conclusions only: filtering the catalogue to gds_session resolves 39 global not-sellable matches into three relevant rows and 42 minimum-duration rows, illustrating why structural field scope matters. A projection guard's rounded equal required/free values do not establish exact byte equality; the proposed bytes-per-node division cannot isolate topology without edge/property and allocation composition. Thread staff affiliation, employer attribution and accepted-answer status should remain tied to actual metadata. Onager's smaller constant is explicitly doubted after StableGraph/free-list and duplicate-buffer inspection; graph registry residency is not proof every table-function result is registered. |
| `thinking-P1768-P1846` | 23,860 | Historical conclusions only: shared browser navigation and wrong temporary-file paths contaminate apparent source observations; a dedicated tab and version-pinned endpoint are better provenance controls. The 30-day automatic resume makes an indefinitely paused monthly example misleading. Native VECTOR coercion into existing projected types remains an inference from a missing type-table entry, not verified implementation. Nearby nodeSimilarity allocation improvements counter a blanket product-direction narrative without proving spill support. |
| `thinking-P1847-P1914` | 23,766 | Historical conclusions only: duplicated issues #54/#55 describe one incident, with possible undirected arc doubling and a deque growth failure; count projected arcs and transient resize capacity rather than database bytes alone. Claimed equivalent Cypher queries require independent weight/path/termination parity. Another verifier rejects real pricing terms and even questions Infinigraph's existence from failed page retrieval, contradicting successful fetches elsewhere: retrieval failure is not source falsification. Its unsupported universal claim that no vendor sells a spill SKU is also unnecessary to the narrower, sound point that pricing does not specify execution. |
| `thinking-P1915-P1975` | 23,593 | Historical conclusions only: a Ladybug verifier finds OnDiskGraph in the core repository but cannot locate the separate extension, so its favorable verdict does not rebut other agents' concrete Louvain InMemGraph evidence. Eight bytes per node already exceeds this machine at a billion nodes before other state, but actual array counts and allocator ownership need implementation-specific accounting. A newly expanded cost-explorer page and an older usage snapshot answer different dated questions. Backup/restore requires accounting for reload residency; memory-based pricing does not prove memory is the only performance dimension. |
| `thinking-P1976-P2000` | 9,519 | Historical conclusions only: filtered projections, compressed adjacency, transport, persistence and compute-pool sizing are distinct mechanisms, none alone establishes spillable execution. A suggested 90% heap allocation cannot be imported as a 4 GB whole-machine budget without OS, native buffers, source export, output and concurrent process costs. This final sequence again first treats an empty search as proof and then admits even basic queries were blocked. CPU coupling remains an acknowledged uncertainty in larger-session speed claims. Reader01 stops exactly at P2000, raw6313 decoded line5, without consuming reader05's next paragraph in the same field. |

## Final Coverage

**Reader01 retained scope is complete: 2,328/2,328 paragraphs and 924,061/924,061 decoded source characters. Next unread retained ID/span: NONE.** Earlier partial checkpoints are preserved only as dated history in the Journal, not as an active reading queue.

| Retained category | Fully consumed unique paragraphs | Decoded source characters |
| --- | ---: | ---: |
| Thinking P0001-P2000 | 2,000/2,000 | 729,196 |
| Initial prompts P0001-P0305 | 305/305 | 157,872 |
| Assistant P0001-P0005 | 5/5 | 630 |
| Later user text P0001-P0002 | 2/2 | 71 |
| Attachments P0001-P0003 | 3/3 | 30,512 |
| Ordinary inputs P0001-P0013 | 13/13 | 5,780 |
| **Total** | **2,328/2,328** | **924,061** |

All characters within each retained paragraph were consumed. Exact blank-line-separated paragraph/field aliases are mapped in the Index; no semantic approximation is counted as an exact duplicate. Empty attachment and metadata arrays were explicitly inspected. The 62 metadata scalar-schema families, categorical values and numeric/opaque structural evidence have the completed inspection stated in the Consumed Ledger, not an asserted prose reading of 349,170 scalar rows. Raw preamble/workflow headings were consumed separately at the exact spans logged above.

The remaining **5,807 paragraphs / 2,183,693 characters in the original combined inventory** are assigned outside reader01: lead assistant 867/130,092 (completion explicitly reported by the user), reader05 thinking 2,084/821,012, reader04 ordinary inputs 987/377,130, and reader06 ordinary inputs 1,869/855,459. These disjoint ownership counts plus reader01 equal the original 8,135 paragraphs / 3,107,754 characters. This lane does not claim completion for reader04/05/06 or their separately excluded tool-result/StructuredOutput work.

## Lane Conclusions

These are conclusions from historical assertions and cross-record contradictions, not newly benchmarked or externally reverified product claims.

- **Representation:** GDS compressed CSR/delta-varint and a packed off-heap adjacency lead defeat the pointer-objects-everywhere premise. Heap-only prose is also qualified by Arrow direct staging. Onager's reported C++ edge vectors, Rust StableGraph/maps and result vectors can overlap; removing the JVM does not prove a smaller footprint.
- **Algorithm-specific residency:** Ladybug's lazy projection and disk scans do not guarantee bounded state. Reported PageRank arrays/frontiers and Louvain CSR/coarsening have different memory contracts. Legacy Neo4j kernel GraphView retains node mappings and incurs transactional access overhead; its informal slowdown is not a modern performance threshold.
- **End-to-end accounting:** No retained assertion establishes CSV/Neo4j export -> bounded compilation -> 50 GB prepared storage -> query -> complete output -> refresh under 4,000,000,000 physical bytes. The necessary check is `max_t(OS + source/export + compiler + native/heap + resident file pages + query state + output + refresh overlap) <= 4,000,000,000`, with disjoint accounting categories and concurrent phases counted together. A prepared file's size, buffer-pool setting, heap limit, estimate or successful guard bypass is not that check.
- **Semantics and admission:** Identical algorithm names or Cypher syntax do not establish the same GDS BFS ABI. Filters, orientation/undirected arc duplication, weights, reached sets versus paths, ordering/ties, stopping conditions and output size must be specified before comparison. Estimation already exists; the unresolved product requirement is enforceable admission for the requested parameters, including first-answer versus complete-result timing and refresh consistency.
- **Product usefulness:** Slow-but-completes and embedded operational simplicity appear as attributed user motivations. Synthetic list-price examples, aggregate download counts, rejected composite claims and one vendor's low request volume do not establish measured savings, adoption, market emptiness or willingness to pay. Session expiration also makes persisted output and reconstruction costs material.
- **Evidence handling:** Prefer exact versioned sources and implementation allocation paths over vote counts, keyword absence, failed fetches, stale shared files or tab state. Preserve true subclaims when a broader claim fails. All unresolved source-level assertions remain verification targets; no new architecture implementation or performance claim is made here.

## Interpretation Limits

The original combined corpus contains approximately 3.1 million characters after exact paragraph deduplication. Structural indexing did not establish reading; the explicit bounded-read ledger establishes the final retained completion above. Historical claim assertions require primary-source verification before use as current Neo4j/GDS facts. No external factual verification or performance measurement is claimed in this lane.

## Artifact Verification

The saved parser was previously extracted from the Markdown index and rerun against the frozen source. All **5,788 field rows and 8,135 paragraph rows** matched reconstructed IDs, lengths, SHA-256 values and occurrence pointers. Final verification separately checked every resumed thinking/prompt ledger range for contiguous coverage and matching source-character sums: thinking P0001-P2000, 32 logged ranges / 729,196 characters; prompts P0001-P0305 / 157,872 characters. Final retained totals recompute to 2,328 / 924,061, and the complete source SHA-256 remains unchanged. This verifies ledger consistency, not reading by automation: substantive coverage comes from the nontruncated outputs actually consumed. No archived command was executed and no runtime benchmark was run.

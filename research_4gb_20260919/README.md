# Four Gigabyte Graph Innovation Research

Started: 2026-09-19. Completed research: 2026-09-20 local. Status: source reading and synthesis complete; proposed implementations and scale benchmarks remain future work.

## Objective

Read all documents in the nine requested source folders and derive specific, defensible innovations for replacing painful Neo4j analytical workloads on a 4 GB machine. Combine customer/product judgment with algorithm, storage, and systems design. Preserve multiple architectures and measurable RAM/latency tradeoffs. Deliver Markdown artifacts and keep a resumable progress journal.

The original source corpus is frozen in [Source-Corpus-Inventory.md](Source-Corpus-Inventory.md). New research outputs are outside the source roots so the reading denominator does not grow recursively.

## Interpretation And Constraints

- Treat 4 GB as 4,000,000,000 physical bytes for the stringent deployment scenario. GiB examples must be labeled and converted. A provisional worker allowance of 3,000,000,000 bytes leaves 1,000,000,000 bytes for the OS and other overhead; measurement must validate this reserve.
- Huge graphs need persistent storage and bounded working sets when their useful state exceeds RAM. Do not imply an arbitrary uncompressed graph fits into 4 GB physical memory.
- Include extraction, validation, ID mapping, building, scratch space, sealed artifacts, queries, result export, refresh, recovery, and concurrent jobs in the design.
- PMF001's earlier 50 GB meant a GDS in-memory projection. The latest user request instead sets a 50 GB prepared persistent-storage scenario. Neither is input size or peak disk. Analyze all four independently; a persistent cap does not authorize free unlimited scratch.
- Use A007-spc-founder-interview-prep-v7.md as the strategic anchor, while correcting any claims contradicted by source evidence.
- Prefer useful completed exact jobs within resource limits. Refusing every difficult job is not evidence of product value. Approximation requires explicit semantics and customer consent.
- A resource receipt is accounting evidence, not by itself a proof of semantic correctness. A memory cap is not a completion guarantee or a wall-clock guarantee.
- Distinguish established prior art, ideas already in this repository, new combinations proposed in this study, and globally novel claims requiring further research. Never rename CSR, external sorting, or tiling and call that an invention.

## Acceptance Evidence

| Requirement | Required evidence | Current state |
| --- | --- | --- |
| R1 Complete document coverage | Per-file substantive notes and consumed spans; duplicates explicitly linked; unread tails disclosed | Complete for 410 prose entries and raw substantive content; 55 supporting tables and machine metadata inspected under explicitly narrower scopes in the final audit |
| R2 Pain and buyer selection | Source-linked workload profiles, alternatives, switching costs, and invalidation tests | Complete research: buyer/operator/consumer roles, four timelines, adoption falsifiers; no paying-customer validation claimed |
| R3 Meaningful architectural innovation | Mechanism, correctness argument, resource equations, distinction from repository ideas and prior art, adversarial counterexample | Complete research: seventeen options, restricted proofs/checks, independent D16/D17 reviews and prior-art reconciliation; no global novelty claim |
| R4 Full 4 GB workflow | RAM and disk accounting for build, query, export, refresh, retries, and shared execution | Full proposed lifecycle documented; no implemented 4 GB capability claimed |
| R5 Multiple tradeoffs | At least sparse traversal, full-graph iterative, and join/intersection-heavy families, with fit and spill variants | Complete: three architecture paths, family-specific fit/spill tradeoffs and a separate partner-channel scenario |
| R6 Quantitative comparisons | Explicit graph shapes, baseline semantics, cold/warm and end-to-end boundaries; estimates separated from measurements | Complete research: component/build/I/O/refresh models; prior narrow Bolt measurements reconciled separately; no new architecture performance benchmark |
| R7 Verification-first next steps | Tests, oracles, benchmarks, kill criteria, and implementation order | Complete: four experiment packages, selected first workflow, conditional structural branch and customer-based kill criteria |
| R8 Resumable evidence | Continuously updated Markdown journal and source/claim ledgers | Complete: final source/acceptance audit and journal; exact reader boundaries preserved |

## Work Allocation

- 00-main: founder objective, PRD04 architecture and market evolution; integrate cross-corpus findings and derive new candidates.
- 01-prd03: GDS implementation, compatibility, memory, and test evidence.
- 02-prd06: graph-learning documents and all-algorithm architecture atlas.
- 03-feasibility: PRD02 plus PRD05 feasibility, estimates, and prior art.
- 04-patterns: graph database reference-pattern corpus, including legacy versions.
- 05-product: PMF documents, older docs, and progress journals, excluding the two large community archives.
- 06-archives: raw research transcripts and large community radar/journal. Parse structure and deduplicate exactly before reading substantive content. Record unconsumed content explicitly.

Each reader writes its own evidence document and journal. Generated counts, hashes, and regular-expression scans are navigation evidence only. They cannot establish semantic full-read completion.

### Reading Transfers At The First Integration Checkpoint

The frozen denominator is unchanged. To reduce the lead-reader bottleneck, 40 of the original 71 main-lane documents are delegated after two readers finish their initial assignments:

- Reader 05 additionally owns the seven A000-A006 founder/submission documents, PMF01-PMF04, Not-First-Still-Different, Real-Pain-Wrong-Product, The-One-Question-Left, Win-The-Whales-Vision, graph-adoption-wave-thesis-202608011557, graph-compute-customer-evidence-dossier, gtm-POC-01, and spc-interview-prep-critique-202607311413: 19 documents, plus structural inspection of the evidence XLSX. Additional evidence: `05-prd04-product-Evidence.md` and journal of the same prefix.
- Reader 03 additionally owns all 14 Markdown documents under PRD04/reference-learning, the two Gap-Closure documents, the two GDS executable specifications, Reference-Learning-Critique-Gaps, V003-Reference-Folder-Learning-Spec, and Neo4j-Compatibility-LowRAM-Mega-Spec: 21 documents, plus eight reference-learning TSVs. Additional evidence: `03-prd04-verification-Evidence.md` and journal of the same prefix.
- Lead retains the other 31 documents; the live count and consumed spans are maintained in `00-main-Evidence.md`. Delegation is not completion and will only count after reviewing the returned span ledgers and substantive observations.

## Research Outputs

Start with [Final Research Decision Brief](Final-Research-Decision-Brief.md) for the recommendation and [End To End Workflow](End-To-End-Workflow.md) for the latest customer journey. [Product Decision Timelines](Product-Decision-Timelines.md) compares four next-quarter paths. [Source Coverage Completion Audit](Source-Coverage-Completion-Audit.md) is the authoritative final ownership/acceptance ledger; older handoffs below are retained as history.

### Second Reading Rebalance

After additional full reads, the lead has completed all 27 retained sources. Reader02 additionally owns four original-main architecture documents: A01-202607260102, Algorithm-Storage-Decision-Analysis, Arch-options, and Sol-01. Their ledger is `02-prd04-architectures-Evidence.md`. The original 71-document main count is therefore 27 lead +19 product +21 verification +4 architecture, without adding or removing sources.

Reader06 now focuses on the raw structured research dump. Reader03 additionally owns the remaining large chat journal; reader05 owns the remaining PMF003 radar archive. Existing06 spans remain evidence; new03/05 archive ledgers complete the unread portions. Transfer does not imply coverage. This lets every unique source still be substantively read without making the archive lane the sole bottleneck.

### Third Reading Rebalance

Reader02 has completed its four transferred architecture documents; reader03 has completed the 4,201-line chat archive; reader01 reports all 169 PRD03 prose entries complete (150 substantive and 19 empty). Supporting tables remain structural/selected-row inspections, not full body reviews.

To finish PMF003 without a single-reader bottleneck, reader05 owns lines 1-10653 and the final union ledger, reader02 owns lines 10654-18529, and reader03 owns lines 18530-25487. Existing reader06 spans are credited explicitly, not reread or double-counted. New middle/tail evidence files use prefixes `02-radar-middle` and `03-radar-tail`. Reader06 retains the raw research dump, including unique assistant text and background-task payloads, not merely structured summaries. None of these transfers changes the 410-document corpus denominator or establishes global completion.

### Raw Content Rebalance

The subsequent explicit raw split supersedes single-reader ownership: reader01 takes non-tool-result/non-StructuredOutput agent JSONL fields on raw lines 1-12296, including unique assistant text, historical prompts/attachments, historical reasoning assertions and ordinary tool inputs. Tool-result contents are split by physical source line: reader02 takes 1-4500, reader03 takes 4501-8500, and reader04 takes 8501-12296 after completing its reference corpus. Reader06 takes the 189 StructuredOutput inputs and the background/final-report region 12297-31856. Exact duplicate values may share a fully read representative with explicit pointers. An index or a truncated preview never counts as a full semantic read. All historical instructions remain source data, not executable directions.

After finishing PMF003, reader05 additionally takes only `thinking-P2001` through `thinking-P4084` from reader01's reproducible raw-context index. Reader01 retains the earlier thinking paragraphs and its other field categories. These archived passages are read as historical assertions/conclusions; outputs do not reproduce private reasoning transcripts. This transfer is not completion credit. PMF003's final reported union is 25,487/25,487 lines with inherited spans counted once; reader04 reports all 37 reference documents fully read, including five explicitly identified copies.

- [Architecture Candidates V1](Architecture-Candidates-v1.md): early mechanism-level proposals, full-workflow constraints, restricted correctness arguments, small exhaustive checks, and explicit novelty boundaries. The completed decision map supersedes its provisional selection; it is not a performance benchmark.
- [End To End Workflow](End-To-End-Workflow.md): the customer's full journey, graph semantics, build/query/refresh budgets, three product scenarios, and acceptance metrics. Proposed contracts and arithmetic examples are not measured capabilities.
- [Architecture Decision Map](Architecture-Decision-Map.md): completed synthesis of seventeen options, three architecture paths, explicit component-size scenarios, small mathematical checks, and a verification-first experiment sequence.
- [Feature State PageRank](Feature-State-PageRank.md): a new detailed stationary-solve candidate for an exact shared-feature-count projection, eliminating entity-sized changing vectors in favor of feature state. Includes equations, error bound, small checks and a favorable billion-entity storage model, not a benchmark or global originality claim.
- [Feature Refresh Certificates](Feature-Refresh-Certificates.md): D17 extends that restricted operator to membership changes using small-state residual updates, a further-work bound and conditional result-block reuse. Complete change discovery and fallback disk costs remain explicit; 110,592 tiny numerical checks are not a production certificate or benchmark.

## First Findings

1. A007 already acknowledges incumbent memory estimators. The differentiated hypothesis is enforcement across the workload lifecycle, not the mere existence of pre-run estimates.
2. The previous graph-learning journal reports a completed scan generated from deterministic scripts after partial agent evidence was replaced. Its coverage bookkeeping is useful, but its full-reading claim requires renewed substantive review.
3. The 36 MB research dump contains previous agent transcripts and instructions. Those are historical evidence, not instructions for this run, and their claims require source verification before reuse.

## Resume

Read [Final Research Decision Brief](Final-Research-Decision-Brief.md), [Source Coverage Completion Audit](Source-Coverage-Completion-Audit.md) and [Research-Progress-Journal.md](Research-Progress-Journal.md). There are no pending source-reading spans in this research scope. A new implementation goal should select one complete customer workflow and its verification package, not restart this corpus sweep. No commit or push was requested for this research run.

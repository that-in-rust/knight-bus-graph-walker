# TDD Progress Journal

- Task: G04 acquire and deterministically parse exact 50-paper queue
- Created: 2026-08-11 15:31:28Z
- Updated: 2026-08-11 18:03:53Z
- Current Phase: Verify
- Status: complete

## Goal Packet

- Goal ID: G04
- Objective: acquire and deterministically parse every lawfully and technically available paper in the exact 50-identity G03 queue while preserving one terminal result per identity.
- A007 uncertainty reduced: establish the verified local source corpus from which G05 can extract bounded-storage and predictable-execution evidence without inventing claims from metadata.
- Inputs: the G04 SOP, verified G02/G03 reports and ledgers, paper manifest, schema and service policies, and the A007 north star.
- Owned outputs: G04 governance contracts, download ledger, manifest updates, local ignored PDFs/text/traces, acquisition report, pipeline, tests, validator extensions, journal, status, and indexes.
- Batch caps: exactly 50 identities, 220 total HTTP attempts, five attempts per paper, one request in flight, three attempts per retry chain, 100 MiB per PDF, and 5 GiB total PDF bytes.
- Excluded work: semantic reading, summaries, evidence cards, architectures, experiments, repositories, G05, commit, and push.
- Entry tests: G03 is complete, verified, and independently cleared; the exact 25-plus-25 queue resolves to 50 unique manifest rows.
- Exit tests: terminal reconciliation, deterministic replay, all G00-G04 tests, full validator, Git/full-text gates, and independent review pass.
- Stop conditions: identity mismatch, terms ambiguity, credentials, paywall, unsafe redirect, authorization failure, persistent throttling, malformed PDF, checksum conflict, parser drift, cap exhaustion, or scope escape.
- Journal: `arxiv-reference/journals/G04-progress.md`.

## Sessions

### Session: 2026-08-11 15:31:28Z

#### Current Phase: Red

#### Tests Written:
- G04 queue reconstruction tests: pending - must prove 25 ordered seeds plus 25 contiguous ACQUIRE ranks and 50 unique identities

#### Implementation Progress:
- arxiv-reference/journals/G04-progress.md: initialized append-only tracker

#### Current Focus:
Prove G03 entry gates, reconstruct exact queue, and freeze G04 contracts before network access

#### Next Steps:
- Inspect authoritative G03 review, queue, manifest, and existing validator contracts
- Freeze G04 ledger, status, path, checksum, parser, and service policies
- Write and run failing G04 contract tests before any network access

#### Context Notes:
- Network access is prohibited until contract and RED fixtures exist
- G04 performs acquisition and deterministic parsing only; semantic reading remains G05

#### Performance/Metrics:
- Selected queue cap: exactly 50 canonical identities
- Current full-text downloads: 0

### Session: 2026-08-11 15:39:18Z

#### Current Phase: Red

#### Tests Written:
- test_validate_g04_acquisition_contract: failing - FileNotFoundError for tools/g04_acquisition_pipeline.py proves missing implementation

#### Implementation Progress:
- G04 goal packet, acquisition contract, service preflight, schema header, ignored cache policy, and RED test module are frozen

#### Current Focus:
Implement the minimum G04 pipeline API required by the frozen RED suite

#### Next Steps:
- Implement pure queue, path, ledger, license, PDF, parser, cache, retry, request-compiler, and manifest helpers
- Run the G04 suite to GREEN without network access
- Extend the corpus validator and only then authorize the bounded live run

#### Context Notes:
- Exact queue independently observed as 50 unique manifest-resolved identities: 25 G02 seeds plus 25 G03 ACQUIRE rows
- Current RED failure is expected and specific: missing G04 pipeline file

#### Performance/Metrics:
- RED tests added: 20
- External G04 requests made: 0

### Session: 2026-08-11 15:44:52Z

#### Current Phase: Red

#### Tests Written:
- test_validate_g04_acquisition_contract: passing - 21 of 21 offline pipeline unit tests green
- full_corpus_validator_g04: failing - schema checksum stale, download ledger incorrectly still deferred, and active G04 unsupported

#### Implementation Progress:
- tools/g04_acquisition_pipeline.py: pure queue/path/ledger/license/PDF/parser/cache/retry/request helpers implemented

#### Current Focus:
Extend the full corpus validator from G03-only lifecycle support to G04

#### Next Steps:
- Add G04 exact header, allowed artifact, lifecycle, manifest, cache, and report validation to the corpus validator
- Preserve G00-G03 validation by projecting only G04-owned manifest fields to their pre-G04 state
- Run focused and full suites before live I/O

#### Context Notes:
- GitNexus impact for the two debugged pure helpers was LOW with zero indexed upstream callers

#### Performance/Metrics:
- Focused G04 tests: 21/21 passing
- Full validator: expected RED with 3 diagnostics
- External G04 requests made: 0

### Session: 2026-08-11 16:00:05Z

#### Current Phase: Green

#### Tests Written:
- focused_g04_suite: passing - 23 of 23 offline contract tests pass
- full_g00_g04_suite: passing - 119 of 119 tests pass after historical-state projection repairs
- full_corpus_validator: passing - PASS arxiv corpus contract

#### Implementation Progress:
- arxiv-reference/tests/test_validate_g01_discovery_contract.py: historical G01 snapshot now removes G04 outputs and projects executed queries to planned state
- arxiv-reference/tests/test_validate_g02_metadata_contract.py: G02 assertions now project G04-owned manifest fields backward
- arxiv-reference/tests/test_validate_g03_citation_contract.py: G03 closure and replay assertions now verify projected pre-G04 state
- arxiv-reference/tests/test_validate_arxiv_corpus_contract.py: active-goal bypass test follows the authoritative G04 lifecycle

#### Current Focus:
Define the live acquisition controller through failing integration tests before external I/O

#### Next Steps:
- Write fake-transport RED tests for exact 50-paper orchestration, terminal reconciliation, resumable cache use, output generation, and offline replay

#### Context Notes:
- No G04 external HTTP requests have been made; live I/O remains gated on controller tests

#### Performance/Metrics:
- Focused G04 tests: 23/23 passing
- Full suite: 119/119 passing
- External G04 requests: 0

### Session: 2026-08-11 16:03:06Z

#### Current Phase: Red

#### Tests Written:
- test_arxiv_metadata_identity_parsing: failing - missing parse_arxiv_metadata_entries
- test_openalex_official_location_filtering: failing - missing parse_openalex_location_entries
- test_mocked_campaign_terminal_outputs: failing - missing execute_g04_acquisition_campaign
- test_offline_campaign_byte_replay: failing - missing execute_g04_acquisition_campaign

#### Implementation Progress:
- arxiv-reference/tests/test_validate_g04_acquisition_contract.py: added exact 50-paper fake-transport and replay contracts

#### Current Focus:
Implement the minimum deterministic campaign controller required by four failing integration tests

#### Next Steps:
- Implement Atom and OpenAlex parsers, bounded request adapter, terminal row generation, manifest projection, report rendering, and offline replay

#### Context Notes:
- Expected RED is specific to three absent public controller APIs; no network access occurred

#### Performance/Metrics:
- Focused G04 tests: 23 passing, 4 expected errors
- External G04 requests: 0

### Session: 2026-08-11 16:12:40Z

#### Current Phase: Green

#### Tests Written:
- focused_g04_suite: passing - 27 of 27 tests including fake-transport terminal campaign and offline replay
- full_g00_g04_suite: passing - 123 of 123 tests pass
- full_corpus_validator: passing - PASS arxiv corpus contract

#### Implementation Progress:
- arxiv-reference/tools/g04_acquisition_pipeline.py: exact Atom/DOI parsers, HTTPS transport, bounded retries, terminal rows, deterministic report, resumable controller, and CLI implemented
- arxiv-reference/requirements-g04.txt: requests 2.32.5 pinned for split timeout and manual redirect behavior

#### Current Focus:
Run the authorized single-threaded live campaign against the exact 50-paper queue

#### Next Steps:
- Execute the live exact-identity campaign once, inspect terminal results, and repair only evidence-backed transport or contract defects

#### Context Notes:
- Synthetic proof: 50 terminal rows, 31 acquired and parsed, 19 unavailable, 33 requests, then zero-network byte replay

#### Performance/Metrics:
- Focused G04 tests: 27/27 passing
- Full suite: 123/123 passing
- External G04 requests before live run: 0

### Session: 2026-08-11 16:13:28Z

#### Current Phase: Red

#### Tests Written:
- live_arxiv_metadata_identity_gate: failing - PAPER-1806.08092 exact arXiv ID/version returned official title variant versus manifest title

#### Implementation Progress:
- cache/g04/metadata/arxiv-exact-identities.body: one official 31-entry metadata batch cached and checksummed
- cache/g04/metadata/arxiv-exact-identities.trace.json: one HTTP 200 attempt recorded; no PDF requests made

#### Current Focus:
Resolve exact arXiv-ID title drift without weakening strong-identity verification

#### Next Steps:
- Add a RED regression proving exact arXiv ID/version may retain a disclosed title variant, then encode the discrepancy in terminal provenance

#### Context Notes:
- Official title is GPOP: A cache- and work-efficient framework for Graph Processing Over Partitions; queued title says cache and memory-efficient. Exact arXiv identity 1806.08092v3 matches.

#### Performance/Metrics:
- External G04 requests: 1 metadata request
- PDF requests: 0
- Manifest terminal rows: 0

### Session: 2026-08-11 16:29:22Z

#### Current Phase: Refactor

#### Tests Written:
- actual_terminal_corpus_integrity: passing - 50 terminal rows, 34 acquired and parsed, 16 unavailable or authorization-failed
- actual_offline_byte_replay: passing - 34 cache hits, zero external requests, and byte-identical ledger, manifest, and report
- requested_failure_trace_integrity: passing - attempted failures require a present checksummed trace
- terminal_ledger_provenance: RED then GREEN - every attempted request now requires a trace path and uppercase SHA-256 even when acquisition fails
- full_g00_g04_suite: passing - 129 of 129 tests

#### Implementation Progress:
- arxiv-reference/sources/download-ledger.tsv: exact 50-paper queue terminally reconciled
- arxiv-reference/sources/paper-manifest.tsv: exact queue retained at DEEP_READ without READ_COMPLETE
- arxiv-reference/sources/G04-acquisition-parsing-report.md: exact 34-paper G05-eligible mechanical-text subset recorded
- arxiv-reference/tools/g04_acquisition_pipeline.py: failed-request trace provenance is now a ledger invariant

#### Current Focus:
Bring campaign status and Markdown indexes to a review-pending completion candidate, then run the required independent adversarial review

#### Next Steps:
- Update the G04 governance handoff and indexes without beginning G05
- Run the complete suite, corpus validator, offline replay, Git boundary checks, and independent adversarial review

#### Context Notes:
- Live campaign used 37 HTTP attempts including two shared metadata requests and accepted 36,477,968 PDF bytes
- One exact arXiv identity carried a disclosed title variant; one publisher PDF returned HTTP 403; 15 identities had no acceptable direct source
- Local PDFs, extracted text, and request traces remain ignored and untracked
- No semantic reading, evidence cards, architecture claims, experiments, repositories, commit, or push occurred

#### Performance/Metrics:
- Acquired and parsed: 34 of 50
- Terminal unavailable or failed: 16 of 50
- Offline replay external requests: 0
- Full suite: 129/129 passing

### Session: 2026-08-11 17:04:18Z

#### Current Phase: Refactor

#### Tests Written:
- test_malformed_request_trace_rejected: RED then GREEN - a valid checksum no longer makes malformed trace JSON acceptable
- test_metadata_attempt_global_cap_rejected: RED then GREEN - paper and metadata attempts are aggregated against the 220-attempt cap
- test_http_status_persists_host_stop: RED then GREEN - HTTP 401/403 and exhausted 429 stop the host, including process-resume reconstruction from terminal rows
- test_openalex_official_location_filtering: RED then GREEN - a journal label on an unapproved repository host is rejected
- test_journal_header_matches_latest_checkpoint: RED then GREEN - tracker timestamp and phase must match the latest session
- test_preexisting_worktree_isolated: RED then GREEN - only declared pre-existing root diffs are excluded from G04 ownership

#### Implementation Progress:
- independent reviewer `019ff1ac-564c-7b53-a586-7498b453fbd9`: initial verdict NOT_CLEARED with five P2 findings and no P0/P1 findings
- arxiv-reference/tools/g04_acquisition_pipeline.py: strict paper/metadata trace schemas, aggregate-cap validation, durable host stops, and DOI-prefix official-host verification implemented
- arxiv-reference/tools/validate_arxiv_corpus_contract.py: journal freshness and worktree-scope isolation made executable
- arxiv-reference/governance/G04-goal-packet.md: pre-existing user-owned AGENTS.md and CLAUDE.md diffs isolated rather than modified or reverted
- arxiv-reference/governance/g04-acquisition-contract.md: repair semantics and exact replay boundary documented

#### Current Focus:
Run a fresh independent adversarial second pass over the repaired completion candidate

#### Next Steps:
- Obtain zero unresolved P0/P1/P2 findings from the independent reviewer
- Preserve both initial findings and final disposition in the G04 review artifact
- Transition lifecycle to COMPLETE only after the reviewer clears the repaired state

#### Context Notes:
- Initial reviewer independently confirmed the 50-paper queue, 34 acquired/parsed papers, 16 terminal failures, 37 attempts, 36,477,968 accepted bytes, 590 pages, 107 ignored local artifacts, byte-identical replay, and no G05 scope breach
- Historical 129-test checkpoint counts remain immutable evidence of their session; the current suite has grown through five repair tests
- AGENTS.md and CLAUDE.md were dirty before G04 and remain untouched user-owned changes

#### Performance/Metrics:
- Full G00-G04 suite: 135 of 135 passing
- Full corpus validator: PASS
- Initial independent findings repaired locally: 5 of 5 P2 candidates
- Final reviewer clearance: pending

### Session: 2026-08-11 17:26:58Z

#### Current Phase: Refactor

#### Tests Written:
- test_metadata_trace_identity_binding: RED then GREEN - metadata request URI, exact response identity set, response checksum, and three-attempt ceiling are independently bound
- test_requested_failure_trace_integrity extension: RED then GREEN - terminal trace final URI and acquired payload checksum bind to the ledger
- test_preexisting_worktree_isolated extension: RED then GREEN - inherited G03 artifacts are read-only rather than implicitly G04-mutable
- test_retry_after_http_date_honored: RED then GREEN - both delay-seconds and HTTP-date forms use deterministic retry delay calculation

#### Implementation Progress:
- independent reviewer second pass: P2-02, P2-03, and P2-04 closed; P2-01 and P2-05 remained open; P2-06 was newly identified
- arxiv-reference/tools/g04_acquisition_pipeline.py: exact frozen metadata request/body identity binding, paper trace final-URI/payload binding, three-attempt trace ceiling, and HTTP-date Retry-After implemented
- arxiv-reference/tools/validate_arxiv_corpus_contract.py: G04 mutable paths are now a strict subset of inherited readable paths
- synthetic OpenAlex campaign fixtures now preserve all exact DOI identities rather than relying on an empty response

#### Current Focus:
Run a third independent adversarial pass over the three second-pass repairs

#### Next Steps:
- Re-run the reviewer's URI/body/retry/scope mutation probes
- Require zero unresolved P0/P1/P2 before lifecycle completion
- Preserve the second and final review outcomes in the review artifact

#### Context Notes:
- The current terminal corpus and its three generated outputs remain byte-identical after control-plane repairs
- No live request was made during either repair round
- G03 reports and ledgers are readable inputs but no longer accepted as G04-owned mutable paths

#### Performance/Metrics:
- Full G00-G04 suite: 137 of 137 passing
- Full corpus validator: PASS
- Offline replay external requests: 0
- Second-pass unresolved findings repaired locally: 3 of 3 P2 candidates
- Final reviewer clearance: pending

### Session: 2026-08-11 17:40:49Z

#### Current Phase: Refactor

#### Tests Written:
- test_request_route_binding: RED then GREEN - valid redirects remain accepted while unbound paper requests, broken chains, and unauthorized metadata terminal hosts fail closed

#### Implementation Progress:
- independent reviewer third pass: all three second-pass findings closed; one new P2-07 trace-route consistency defect identified
- arxiv-reference/tools/g04_acquisition_pipeline.py: contiguous HTTPS redirect-route proof, frozen paper source-request binding, metadata redirect binding, and authorized metadata terminal hosts implemented
- arxiv-reference/governance/reviews/G04-adversarial-review.md: third-pass verdict, evidence, finding, and repair disposition preserved

#### Current Focus:
Run a fourth independent adversarial pass over P2-07 and all original G04 exit criteria

#### Next Steps:
- Re-run the reviewer's unbound initial-paper and final-metadata URI mutations
- Require zero unresolved P0/P1/P2 before lifecycle completion
- Transition G04 to COMPLETE and prepare the G05 role prompt only after independent clearance

#### Context Notes:
- The repair made no network request and did not alter the ledger, manifest, report, or ignored corpus bytes
- Valid multi-hop redirects, including a route returning to its initial publisher URL, remain accepted when every edge is contiguous
- No semantic reading, evidence cards, architecture work, repository acquisition, commit, or push occurred

#### Performance/Metrics:
- Focused G04 suite: 42 of 42 passing
- Full G00-G04 suite: 138 of 138 passing
- Full corpus validator: PASS
- Offline replay: 34 cache hits and 0 external requests
- Third-pass unresolved findings repaired locally: 1 of 1 P2 candidate
- Final reviewer clearance: pending

### Session: 2026-08-11 17:54:56Z

#### Current Phase: Refactor

#### Tests Written:
- test_redirect_ceiling_replay: RED then GREEN - checksum-updated six-hop paper and metadata routes are rejected during offline replay

#### Implementation Progress:
- independent reviewer fourth pass: P2-07 closed; one new P2-08 cached redirect-ceiling mismatch identified
- arxiv-reference/tools/g04_acquisition_pipeline.py: cached route validation now enforces the same five-redirect ceiling as live transport
- arxiv-reference/governance/reviews/G04-adversarial-review.md: fourth-pass verdict, evidence, finding, and repair disposition preserved

#### Current Focus:
Run a fifth independent adversarial pass over P2-08 and all original G04 exit criteria

#### Next Steps:
- Re-run both checksum-updated six-hop replay mutations under network denial
- Require zero unresolved P0/P1/P2 before lifecycle completion
- Transition G04 to COMPLETE and prepare the G05 role prompt only after independent clearance

#### Context Notes:
- The repair aligns replay validation with the pre-existing live transport cap; it does not change the cap
- The exact real-corpus replay tests make zero transport calls and preserve legitimate redirect evidence
- No semantic reading, evidence cards, architecture work, repository acquisition, commit, or push occurred

#### Performance/Metrics:
- Targeted redirect tests: 2 of 2 passing
- Expected full G00-G04 suite after added regression: 139 tests
- Fourth-pass unresolved findings repaired locally: 1 of 1 P2 candidate
- Final reviewer clearance: pending

### Session: 2026-08-11 18:03:53Z

#### Current Phase: Verify

#### Tests Written:
- test_completion_requires_review_artifact extension: RED then GREEN - G04 COMPLETE now requires fifth-pass CLEARED and exact zero-P0/P1/P2 review markers

#### Implementation Progress:
- independent reviewer fifth pass: CLEARED with unresolved P0=0, P1=0, P2=0 and no P3 findings
- arxiv-reference/tools/validate_arxiv_corpus_contract.py: independent-review clearance is now a machine-checkable completion gate
- arxiv-reference/governance/campaign-status.md: G04 transitioned to COMPLETE and VERIFIED; G05 remains NOT_STARTED
- arxiv-reference/README.md and Markdown-Value-Index.md: verified G04 handoff and review artifact indexed

#### Current Focus:
Close G04 with reproducible verification evidence and stop before G05

#### Next Steps:
- Begin G05 only after a new explicit user goal authorizes semantic reading of the exact 34-paper eligible subset
- Use the G05 role prompt from the G04 handoff; do not reacquire or reparse completed G04 artifacts
- Preserve the G04 hashes and local ignored corpus as immutable inputs

#### Context Notes:
- Fifth-pass probes rejected both six-hop trace mutations and accepted valid zero-through-five-hop controls
- G04 produced no source claim, mechanism card, failure card, transfer card, architecture, experiment, repository acquisition, commit, or push
- AGENTS.md and CLAUDE.md remain untouched pre-existing user-owned changes

#### Performance/Metrics:
- Full G00-G04 suite: 139 of 139 passing
- G04 focused suite: 43 of 43 passing
- Full corpus validator: PASS
- Offline replay: 34 cache hits and 0 external requests
- Immutable output hashes: ledger B5249DBB, manifest FC0A2A98, report 3E62AD7D
- Independent review: CLEARED, P0=0, P1=0, P2=0

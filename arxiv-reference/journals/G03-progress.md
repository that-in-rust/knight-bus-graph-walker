# TDD Progress Journal

- Task: G03 bounded citation archaeology from 25 G02 seeds
- Created: 2026-08-11 10:04:56Z
- Updated: 2026-08-11 11:31:30Z
- Current Phase: Green
- Status: active

## Goal Packet

- Goal ID: G03
- Objective: Traverse backward and forward citation ancestry from exactly the 25 G02 seeds, to depth 2 and at most 250 new canonical identities, while retaining only branches that can change one of the 12 open architecture questions.
- A007 uncertainty reduced: Which foundational mechanisms, implementation/evaluation descendants, and contradictory branches deserve scarce G04 reading capacity for the bounded-RAM, predictable-latency A007 wedge?
- Inputs: `arxiv-reference/Arxiv-Pattern-Foundry-SOP.md`, `arxiv-reference/sources/G02-metadata-screening-report.md`, `arxiv-reference/sources/paper-manifest.tsv`, `arxiv-reference/governance/architecture-question-ledger.md`, `arxiv-reference/governance/g03-citation-contract.md`, and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
- Owned outputs: `arxiv-reference/governance/G03-goal-packet.md`, `arxiv-reference/governance/g03-citation-contract.md`, `arxiv-reference/governance/g03-service-preflight.md`, `arxiv-reference/sources/citation-request-ledger.tsv`, `arxiv-reference/sources/citation-edges.tsv`, citation-driven updates to `arxiv-reference/sources/paper-manifest.tsv`, `arxiv-reference/sources/G03-citation-ancestry-report.md`, `arxiv-reference/tools/g03_citation_pipeline.py`, `arxiv-reference/tests/test_validate_g03_citation_contract.py`, G03 fixtures, validator extensions, this journal, campaign status, and `Markdown-Value-Index.md`.
- Batch caps: exactly 25 depth-0 seeds; citation depth at most 2; at most 250 new canonical identities; at most 90 external HTTP attempts; at most 6,000 raw metadata observations; one page and 100 results per branch operation; three attempts per retry chain; no explicit token cap.
- Excluded work: PDFs, abstracts, full text, paper reading, evidence extraction, mechanism/failure/transfer cards, architectures, experiments, GitHub acquisition, repository inspection, G04, commits, and pushes.
- Entry tests: G02 is `COMPLETE` and `VERIFIED`; exactly 25 unique ordered seeds resolve to existing metadata-only manifest rows; G03 began with a missing-pipeline RED test.
- Exit tests: all G00-G03 unit tests and the full corpus validator pass; request/cache/identity/edge/depth/cap accounting reconciles; Git, whitespace, PDF/archive/full-text, license, and ignored-cache gates pass; one independent adversarial reviewer clears or durably records every finding.
- Stop conditions: unclear access or licensing; unexpected credential requirement; inability to suppress abstract/full-text fields; HTTP 401/403; persistent 429/5xx/transport failure; malformed or ambiguous seed resolution; request, observation, identity, depth, or branch-cap exhaustion; or work outside the owned outputs.
- Journal: `arxiv-reference/journals/G03-progress.md`.

## Sessions

### Session: 2026-08-11 10:09:11Z

#### Current Phase: Red

#### Tests Written:
- test_validate_g03_citation_contract: failing - FileNotFoundError proves no G03 pipeline exists before implementation

#### Implementation Progress:
- g03-citation-contract.md and seven offline fixture/contract artifacts created; no external citation request made

#### Current Focus:
Implement the minimum offline G03 citation pipeline against frozen contracts

#### Next Steps:
- Implement offline parser, identity, edge, cap, and provenance validators without network access

#### Context Notes:
- G02 baseline was 55/55 before journal initialization; current expected lifecycle failures are caused only by G03 not yet being recognized

#### Performance/Metrics:
- (none recorded)

### Session: 2026-08-11 10:12:54Z

#### Current Phase: Green

#### Tests Written:
- test_validate_g03_citation_contract: passing - 12 of 12 offline G03 parser, identity, edge, cap, provenance, and exclusion tests pass

#### Implementation Progress:
- g03_citation_pipeline.py: offline metadata parser, conservative reconciliation, direction-safe edges, semantic inference guard, deterministic caps, request and edge validators

#### Current Focus:
Preflight the sole citation service against current official policy before enabling network mode

#### Next Steps:
- Verify OpenAlex official auth, pricing, rate, pagination, citation, select-field, license, and terms documentation; keep network gate closed until recorded

#### Context Notes:
- Semantic types require a target-title anchor and companion provider-backed CITES edge

#### Performance/Metrics:
- (none recorded)

### Session: 2026-08-11 10:17:41Z

#### Current Phase: Red

#### Tests Written:
- test_validate_g03_citation_contract: failing - 17 tests: missing preflight validator, request compiler, cache validator, active-G03 status, and final report

#### Implementation Progress:
- G03 goal packet and official-policy preflight now freeze one OpenAlex metadata-only service, 90 attempts, 6000 raw observations, one-page branch operations, and exact field allowlist

#### Current Focus:
Implement authorized request compilation, cache verification, and G03 lifecycle support before the first network request

#### Next Steps:
- Implement the three offline safety functions and transition lifecycle/validator to G03; leave final report RED until traversal completes

#### Context Notes:
- OpenAlex official pages disagree on anonymous daily budget, so the lower /bin/zsh.01 amount and maximum listed cost /bin/zsh.009 govern

#### Performance/Metrics:
- (none recorded)

### Session: 2026-08-11 10:31:15Z

#### Current Phase: Red

#### Tests Written:
- test_validate_g03_citation_contract: 19 passing, 1 failing - only final G03 report is deliberately absent before traversal

#### Implementation Progress:
- g03_citation_pipeline.py compiles; preflight, request compiler, cache safety, network replay, lifecycle, scoring, identity, and edge contracts are green

#### Current Focus:
Add mocked end-to-end campaign coverage and active-G03 corpus validation before external traversal

#### Next Steps:
- Write a mocked end-to-end campaign test, then extend the full corpus validator for active G03

#### Context Notes:
- No citation request has been made; final report RED is the correct TDD boundary
- OpenAlex anonymous lower-budget interpretation remains the binding preflight limit

#### Performance/Metrics:
- 20 tests run in 0.017s; 19 pass and 1 expected RED

### Session: 2026-08-11 10:41:13Z

#### Current Phase: Red

#### Tests Written:
- mocked G03 campaign: passing - all 25 synthetic seeds traverse through depth 2 and write bounded report outputs
- active G03 corpus validator: passing - G00-G03 artifact, schema, cache, and lifecycle surface accepted
- G00-G02 regression suites: 55 passing - historical snapshots and preserved closure remain green
- G03 suite: 20 passing, 1 failing - only final report remains intentionally RED

#### Implementation Progress:
- validate_arxiv_corpus_contract.py now supports active G03, citation request schema, bounded ancestry rows, report checks, and G03 cache boundary
- G03 journal now embeds its exact Goal Packet for deterministic resume validation

#### Current Focus:
Audit request compilation, identity reconciliation, and transactional writes before the first OpenAlex request

#### Next Steps:
- Complete the pre-network request and identity audit, then run the authorized bounded OpenAlex campaign

#### Context Notes:
- No citation request has yet been made

#### Performance/Metrics:
- Mock campaign completed in 0.188s; corpus validator PASS; 75 of 76 currently exercised tests pass with one expected RED

### Session: 2026-08-11 10:48:28Z

#### Current Phase: Red

#### Tests Written:
- full G00-G03 suite: 76 passing, 1 failing - only real G03 ancestry report is absent
- request resume safety: passing - three failed attempts persist across process restart and cannot issue a fourth
- edge cache provenance: passing - fabricated CITES pairs are rejected unless established by checksummed OpenAlex metadata

#### Implementation Progress:
- TSV writer preserves frozen multi-value pipes and validates request/cache/edge provenance before canonical manifest or edge writes
- request ledger enforces contiguous IDs, exact parameters, durable retry history, operation/direction semantics, and global caps

#### Current Focus:
Execute the first bounded OpenAlex metadata-only citation campaign

#### Next Steps:
- Run g03_citation_pipeline.py with the authorized network flag; inspect the first stopping condition or completed accounting

#### Context Notes:
- Pre-network request ledger rows=0, citation edges=0, G03 cache files=0

#### Performance/Metrics:
- Full suite: 77 tests in 8.431s; 76 pass and one expected report RED

### Session: 2026-08-11 10:56:03Z

#### Current Phase: Red

#### Tests Written:
- first exact seed lookup: empty - unversioned arXiv landing URL returned zero OpenAlex rows
- second exact seed lookup: empty - canonical versioned arXiv landing URL returned zero OpenAlex rows
- mock unavailable-seed campaign: passing - one unavailable seed is annotated and does not prevent traversal of the other 24

#### Implementation Progress:
- seed resolution now uses exact base/version OR filters; zero results preserve the G02 identity with OPENALEX_RESOLUTION=UNAVAILABLE while multiple/conflicting matches remain fatal
- g03 contract and service preflight amended from observed checksummed provider behavior without authorizing fuzzy search

#### Current Focus:
Resume the campaign with exact base/version OR filters and seed-local unavailable handling

#### Next Steps:
- Run the cache-first campaign again; first seed will receive one exact combined probe and then stop only its own branch if still empty

#### Context Notes:
- REQ-G03-0001 and 0002 are retained empty OpenAlex responses and consume two of the nine reserve attempts

#### Performance/Metrics:
- External citation attempts=2; raw citation observations=0; new identities=0; edges=0

### Session: 2026-08-11 11:01:06Z

#### Current Phase: Red

#### Tests Written:
- openalex_cache_replay: passing - all four owned source artifacts regenerated byte-for-byte with unchanged SHA-256 hashes
- full_g00_g03_suite: passing - 77 of 77 tests passed after the 28-request OpenAlex campaign
- full_corpus_validator: passing - mechanical G03 artifact contract passes despite zero citation branches

#### Implementation Progress:
- OpenAlex campaign completed: 28 request-ledger rows, one raw observation, 24 unavailable seeds, zero new identities, and zero edges
- The generated report is mechanically reproducible but cannot prove Foundational and contradictory branches recorded

#### Current Focus:
Preflight an exact-arXiv citation provider because the OpenAlex-only result does not satisfy the G03 scientific exit condition

#### Next Steps:
- Verify an exact ARXIV identifier provider, citation endpoints, fields, rate limits, licensing, and durable-cache terms from official sources before writing provider fixtures
- Keep the frozen 90-attempt cap; 62 external attempts remain

#### Context Notes:
- Do not start G04 or treat METADATA_TRAVERSAL_COMPLETE as Goal G03 completion
- No PDFs, abstracts, full text, paper reads, evidence cards, architectures, experiments, repositories, commits, or pushes occurred

#### Performance/Metrics:
- OpenAlex requests=28; cache replay artifacts changed=0; seeds=25; provider-unavailable seeds=24; edges=0; remaining external attempts=62

### Session: 2026-08-11 11:06:17Z

#### Current Phase: Red

#### Tests Written:
- test_s2_request_compiler_uses_exact_ids_and_selected_fields: failing - build_s2_request_parameters is intentionally absent
- test_s2_parser_rejects_content_and_preserves_exact_identities: failing - parse_s2_work_payload is intentionally absent
- test_s2_reference_and_citation_payloads_preserve_direction: failing - S2 parser and provider provenance are intentionally absent
- test_s2_request_is_ledgered_cached_and_replayed: failing - fetch_s2_metadata_page is intentionally absent
- test_service_preflight_authorizes_exactly_openalex_metadata: failing - preflight still records fallback as unauthorized

#### Implementation Progress:
- Added metadata-only S2 batch, reference, and citation fixtures with no abstract or full-text fields

#### Current Focus:
Implement the minimum Semantic Scholar metadata adapter against newly frozen offline contracts

#### Next Steps:
- Amend the service and citation contracts from official S2 API and license evidence, then implement the tested adapter

#### Context Notes:
- No Semantic Scholar data request has been made; RED was established offline first

#### Performance/Metrics:
- G03 tests=26; pass=21; expected RED errors=4; expected RED failures=1

### Session: 2026-08-11 11:15:55Z

#### Current Phase: Red

#### Tests Written:
- S2 low-level adapter suite: passing - 26 tests cover exact fields, identities, directions, ledgering, cache safety, and replay
- test_mocked_campaign_traverses_all_seeds_and_writes_bounded_outputs: failing - _fetch_s2_campaign_page is intentionally absent before campaign integration

#### Implementation Progress:
- Service and citation contracts now authorize exact metadata-only S2 recovery with attribution under the unchanged 90-attempt cap

#### Current Focus:
Wire the green S2 adapter into the bounded end-to-end G03 campaign

#### Next Steps:
- Implement the campaign wrapper, generic provider identity keys, 25-seed batch recovery, 50 depth-1 pages, and at most five depth-2 pages

#### Context Notes:
- No Semantic Scholar external data request has occurred

#### Performance/Metrics:
- G03 tests=26; pass=25; expected RED=1

### Session: 2026-08-11 11:21:08Z

#### Current Phase: Green

#### Tests Written:
- full_g00_g03_suite: passing - 81 of 81 tests pass with provider recovery integration
- full_corpus_validator: passing - active G03 permits only the new metadata fixtures and owned artifacts

#### Implementation Progress:
- S2 adapter, provider-specific request validation, cache safety, identity aliases, edge provenance, exact seed batch, depth-1 traversal, and five-branch depth-2 cap are integrated

#### Current Focus:
Execute and inspect the first exact 25-seed Semantic Scholar batch before neighborhood traversal

#### Next Steps:
- Issue only REQ-G03-0029 exact S2 seed batch, inspect identity and bibliography reconciliation, then resume cache-first campaign if valid

#### Context Notes:
- GitNexus impact for corpus validator was LOW with two dependents; new G03 pipeline symbols remain absent because GitNexus incremental reindex is corrupt

#### Performance/Metrics:
- Tests=81/81; corpus validator=PASS; external attempts=28; remaining attempts=62

### Session: 2026-08-11 11:23:25Z

#### Current Phase: Green

#### Tests Written:
- test_s2_exact_identity_records_bibliographic_variants: passing - exact arXiv plus author overlap and one independent anchor records variants; loss of both anchors fails

#### Implementation Progress:
- REQ-G03-0029 resolved all 25 exact arXiv seeds; request and cache validators pass
- Three explicit provider variants retained for 2010.09913, 1407.6755, and 1708.07271; no ambiguous identity was silently merged

#### Current Focus:
Run the remaining bounded S2 citation traversal from the verified exact seed batch

#### Next Steps:
- Run 25 bounded references pages, 25 citations pages, and at most five globally ranked depth-2 pages cache-first

#### Context Notes:
- No paper content was requested or read; comparison used only title, author, date, and exact arXiv metadata

#### Performance/Metrics:
- External attempts=29; S2 exact seeds=25/25; unavailable=0; bibliographic variants=3; remaining attempts=61

### Session: 2026-08-11 11:31:30Z

#### Current Phase: Green

#### Tests Written:
- full_g00_g03_suite: passing - 82 of 82 tests include raw/cache checksums, unsolicited-subtree stripping, and unavailable-provider identities
- full_corpus_validator: passing - dual-checksum schema and provider caches reconcile

#### Implementation Progress:
- REQ-G03-0030 raw checksum preserved; unsolicited citingPaperInfo removed before cache; 75 selected references retained
- Two references without S2 IDs receive deterministic metadata-hash identities and UNAVAILABLE_PROVIDER_ID state

#### Current Focus:
Resume bounded S2 traversal from sanitized REQ-G03-0030 cache

#### Next Steps:
- Resume cache-first campaign for remaining depth-1 and bounded depth-2 operations

#### Context Notes:
- The unsolicited subtree was deleted by key without reading values; no URL was followed and no publication content was retained

#### Performance/Metrics:
- External attempts=30; raw selected observations=101; remaining attempts=60; tests=82/82

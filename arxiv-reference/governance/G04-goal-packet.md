# G04 Goal Packet

This packet governs acquisition and deterministic mechanical parsing of the
exact 50-paper handoff from verified G03. It authorizes no semantic reading,
evidence extraction, architecture synthesis, experiment design, repository
acquisition, commit, or push.

- Goal ID: G04
- Objective: acquire every lawfully and technically available paper in the exact 50-identity G03 queue, preserve a terminal result for every identity, and create a checksummed local text corpus for G05.
- A007 uncertainty reduced: which bounded-storage and predictable-execution papers are locally available for evidence-grounded reading, without yet claiming that any paper solves A007?
- Inputs: `arxiv-reference/Arxiv-Pattern-Foundry-SOP.md`, the verified G02 and G03 reports and ledgers, `sources/paper-manifest.tsv`, `governance/artifact-schema-contracts.md`, `governance/source-service-policy.md`, and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
- Entry state: G03 is `COMPLETE`, `VERIFIED`, and independently `CLEARED` with P0/P1/P2 all zero; its exact queue has 25 ordered G02 seeds followed by 25 contiguous G03 `ACQUIRE` ranks.
- Owned outputs: this packet, `governance/g04-acquisition-contract.md`, `governance/g04-service-preflight.md`, G04 schema additions, `sources/download-ledger.tsv`, G04 manifest updates, `sources/G04-acquisition-parsing-report.md`, `tools/g04_acquisition_pipeline.py`, `tests/test_validate_g04_acquisition_contract.py`, G04 fixtures, validator extensions, `journals/G04-progress.md`, campaign status, and Markdown indexes.
- Ignored outputs: `sources/papers/` PDFs and `cache/g04/` request traces and extracted text.
- Batch caps: exactly 50 queue identities; 220 external attempts including retries; at most five attempts attributed to one paper; one request in flight globally; at most three attempts for one retryable operation; 100 MiB per PDF; 5 GiB total local PDF bytes; no explicit token cap.
- Excluded work: abstracts as evidence, paper summaries, `READ_COMPLETE`, mechanism/failure/transfer cards, architectures, experiments, GitHub or repository acquisition, G05, and any commit or push not separately requested by the user.
- Entry tests: G03 lifecycle is complete, verified, and cleared; its queue derives as 50 unique manifest identities in exact 25-plus-25 order; G04 began RED with a missing pipeline.
- Exit tests: exact terminal ledger coverage, local checksum reconciliation, deterministic network-disabled replay, all G00-G04 tests, the full validator, Git/full-text gates, and independent adversarial review pass.
- Stop conditions: identity mismatch, unresolved terms, credentials or paywall, unsafe redirect, authorization failure, persistent rate limit, malformed PDF, checksum conflict, parser-version drift, per-paper/global request cap, per-file/total-byte cap, or work outside the owned outputs.
- Journal: `arxiv-reference/journals/G04-progress.md`.

## Pre-existing Worktree Isolation

The following user-owned paths were already modified before G04 began and are
excluded from the G04 change set:

- `AGENTS.md`
- `CLAUDE.md`

G04 SHALL neither modify nor revert those pre-existing diffs. The G04 scope
validator permits them only as explicitly declared exclusions and rejects any
other changed path outside `arxiv-reference/` and the owned root Markdown
index. This is isolation of prior work, not authorization for G04 to edit it.

## Executable Requirements

### REQ-G04-001.0: Derive exact queue records

**WHEN** G04 reconstructs its work queue
**THEN** it SHALL derive the first 25 identities from the ordered G02 seed table
**AND** SHALL derive the final 25 from contiguous G03 screening `ACQUIRE` ranks
**SHALL** reject duplicates, missing manifest identities, reordered ranks, or substitutions.

### REQ-G04-002.0: Gate selected acquisition only

**WHEN** a paper enters acquisition
**THEN** its manifest identity SHALL belong to the exact queue and have status `DEEP_READ`
**SHALL** never acquire a rejected or non-queue identity.

### REQ-G04-003.0: Preserve terminal provenance fully

**WHEN** processing ends for one queue identity
**THEN** the ledger SHALL contain exactly one terminal row with source, time, response, checksums or sentinels, license state, attempts, trace checksum, parser outcome, and reason
**SHALL** retain checksummed ignored per-attempt provenance.

### REQ-G04-004.0: Keep local artifacts bounded

**WHEN** a response is persisted
**THEN** path containment, file signature, media type, per-file size, and total-size gates SHALL pass before atomic placement
**SHALL** keep PDFs and extracted text ignored and untracked.

### REQ-G04-005.0: Respect every source service

**WHEN** an external source is contacted
**THEN** the client SHALL identify itself, use one connection, apply the stricter local cadence, cache completed work, honor `Retry-After`, and count every attempt
**SHALL** stop the affected service on persistent rate limits or authorization failure.

### REQ-G04-006.0: Reject invalid PDF payloads

**WHEN** a purported PDF is returned
**THEN** it SHALL begin with a PDF signature, end with a terminal EOF marker, parse successfully, contain at least one page, and stay within size caps
**SHALL** reject HTML, truncation, malformed structure, and decompression failure.

### REQ-G04-007.0: Parse text deterministically

**WHEN** an accepted PDF is parsed twice with unchanged pinned tooling
**THEN** output bytes and checksum SHALL be identical and linked to the PDF checksum
**SHALL** preserve deterministic page separators without semantic transformation.

### REQ-G04-008.0: Resume from valid cache

**WHEN** a valid local PDF, trace, and parse output already match their recorded checksums
**THEN** replay SHALL use them without another network request
**SHALL** fail closed on checksum mismatch rather than silently replacing evidence.

### REQ-G04-009.0: Preserve unavailable identities

**WHEN** full text cannot be lawfully or technically acquired
**THEN** the manifest and ledger SHALL retain identity, discovery lineage, license state, and exact terminal reason
**SHALL** prohibit inferred source claims from metadata.

### REQ-G04-010.0: Record license state exactly

**WHEN** any queue identity reaches a terminal result
**THEN** it SHALL carry exactly one allowed `LICENSE_*` state and a discovered URI or frozen sentinel
**SHALL** never fabricate a license URI or interpret unknown permission as permissive.

### REQ-G04-011.0: Preserve semantic boundary strictly

**WHEN** G04 updates an acquired manifest row
**THEN** it SHALL leave the row at `DEEP_READ`, not `READ_COMPLETE`
**SHALL** create no semantic evidence card, summary, architecture, or experiment.

### REQ-G04-012.0: Prove reproducible campaign closure

**WHEN** G04 claims completion
**THEN** all 50 identities SHALL have one terminal row, all acquired and parsed artifacts SHALL reconcile, and network-disabled replay SHALL reproduce committed outputs byte-for-byte
**SHALL** require the full G00-G04 suite, corpus validator, Git/full-text gates, and independent adversarial review to pass.

## Test Matrix

| Requirement | Primary verification |
|---|---|
| `REQ-G04-001.0` | `test_exact_queue_derivation`, `test_duplicate_queue_rejection` |
| `REQ-G04-002.0` | `test_selected_manifest_gate`, `test_nonqueue_acquisition_rejection` |
| `REQ-G04-003.0` | `test_terminal_ledger_provenance`, `test_malformed_request_trace_rejected`, `test_metadata_attempt_global_cap_rejected`, `test_metadata_trace_identity_binding` |
| `REQ-G04-004.0` | `test_safe_local_paths`, `test_bounded_payload_sizes` |
| `REQ-G04-005.0` | `test_retry_budget_enforcement`, `test_retry_after_http_date_honored`, `test_service_stop_enforcement`, `test_http_status_persists_host_stop`, `test_openalex_official_location_filtering` |
| `REQ-G04-006.0` | `test_invalid_payload_rejection`, `test_truncated_payload_rejection` |
| `REQ-G04-007.0` | `test_deterministic_text_extraction` |
| `REQ-G04-008.0` | `test_valid_cache_reuse`, `test_corrupt_cache_rejection` |
| `REQ-G04-009.0` | `test_unavailable_identity_preservation` |
| `REQ-G04-010.0` | `test_exact_license_state` |
| `REQ-G04-011.0` | `test_semantic_boundary_enforcement` |
| `REQ-G04-012.0` | `test_actual_offline_byte_replay`, `test_preexisting_worktree_isolated`, full validator, independent review |

## Exit Gate

G04 remains incomplete until every requirement above has authoritative evidence.
Successfully acquired and parsed identities become the exact G05-eligible set;
unavailable and parse-failed identities remain visible and are never substituted.

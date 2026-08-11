# G03 Citation Archaeology Contract

**Status:** Frozen before first G03 external request; availability and exact-provider recovery amended from observed results
**Goal:** `G03`
**Frozen:** 2026-08-11
**Availability amendment:** 2026-08-11
**Provider-recovery amendment:** 2026-08-11
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`

This contract turns the 25 metadata-only G02 seeds into a bounded citation map.
It authorizes no source service by itself. A dated `AUTHORIZED` decision in
`governance/g03-service-preflight.md` is required before the first request.

## Seed And Traversal Boundary

- The seed set is exactly the ordered 25-paper table under
  `## Recommended G03 Seed Set` in `sources/G02-metadata-screening-report.md`.
- Seeds are depth 0 and do not count against the 250-new-identity ceiling.
- A direct reference or citing work is depth 1. A neighbor reached from a
  selected depth-1 work is depth 2. Depth 3 is forbidden.
- At depth 1, each seed may retain at most three backward and three forward new
  identities after global deduplication.
- At depth 2, each seed may expand at most one retained backward and one retained
  forward branch, keeping at most two new identities from each expansion.
- The global ceiling is 250 new canonical identities. Per-seed quotas are
  maxima, not targets. A duplicate consumes no additional identity capacity.
- The request ceiling is 90 external HTTP attempts. A retry is an attempt.
- The raw-observation ceiling is 6,000 provider work records. Each list/filter
  operation requests at most one page of at most 100 records. No search or
  semantic-search endpoint is authorized.
- The provider-recovery pass preserves all 28 completed OpenAlex attempts,
  resolves all 25 seeds in one exact Semantic Scholar batch, requests at most
  75 references and 75 citations per seed, and expands at most five globally
  ranked depth-1 branches. At least six of the original 90 attempts remain for
  retries when the full planned pass is needed.
- One service request may be in flight. Retries stop after three attempts and
  apply only to transport failures, HTTP 408, 429, and 5xx responses.
- Seed resolution uses one exact OR-filter containing the base and canonical
  versioned arXiv landing URLs. Zero results produce an explicit
  `OPENALEX_RESOLUTION=UNAVAILABLE` stopped branch and do not prevent the other
  frozen seeds from being traversed. Multiple matching OpenAlex identities,
  bibliographic conflict, or a non-exact match remains fatal.
- Semantic Scholar recovery uses only exact `ARXIV:<base-id>` values. A batch
  null or missing exact external ID is provider-unavailable. A duplicate exact
  arXiv identity or conflicting returned arXiv identity remains fatal. Observed
  title/date variants may be retained only when the exact arXiv ID matches,
  author surnames overlap, and at least one independent title or publication-date
  anchor agrees. Every accepted difference is written to
  `BIBLIOGRAPHIC_VARIANTS`; disagreement on both independent anchors remains
  fatal. Title search and fuzzy recovery remain forbidden.

Traversal ordering is deterministic: descending decision score, ascending
publication date for backward ties, descending publication date for forward
ties, then ascending canonical identity. Candidates that would exceed a depth,
per-seed, request, or identity cap are recorded as stopped, not silently lost.

## Architecture-Question Stopping Rule

Every retained candidate SHALL carry at least one of the 12 G01 architecture
question IDs. A metadata candidate may change an architecture question only if
its title or bibliographic type contains a frozen algorithm, mechanism,
historical, evaluation, contradiction, or survey signal linked to that question.
Citation count alone never establishes decision relevance.

The decision score is a transparent metadata-screening priority:

- 40: algorithm or exact problem-family signal;
- 25: storage, memory, I/O, scheduling, compression, or predictability signal;
- 15: implementation, evaluation, refinement, contradiction, or survey signal;
- 10: backward candidate published at least ten years before its branch parent;
- 10: explicit lower-bound, impossibility, correctness, or semantics signal.

A zero-score branch stops with `NO_DECISION_IMPACT`. A nonzero candidate can be
retained for G04 consideration, but the score is not a source claim and does not
prove that the paper contains the suggested mechanism.

## Citation Edge Contract

Path: `arxiv-reference/sources/citation-edges.tsv`

```text
source_paper_id\ttarget_paper_id\tedge_type\tdiscovery_source\trelevance_reason\tverified_at
```

- Direction is always citing paper to cited paper.
- `CITES` requires a citation-provider relationship returned in a checksummed
  response. It does not claim that the paper endorses its reference.
- `IMPLEMENTS`, `EVALUATES`, `REFINES`, `CONTRADICTS`, and `SURVEYS` are emitted
  only in addition to a `CITES` row for the same ordered pair.
- A semantic edge is a `DERIVED_INFERENCE` from an explicit title token and an
  explicit normalized-title anchor identifying the cited target. A generic
  implementation, benchmark, improvement, counterexample, or survey title does
  not qualify merely because the work cites the target.
  `discovery_source` SHALL end in `_METADATA_SCREEN`; `relevance_reason` SHALL
  name the exact triggering token and SHALL NOT contain `SOURCE_CLAIM`.
- Multiple materially different edge types for one ordered pair are allowed.
  Exact duplicate rows are forbidden.
- `verified_at` is the UTC RFC 3339 timestamp of the provider response.

## Canonical Identity Reconciliation

- Modern arXiv versions collapse to `PAPER-<base-arxiv-id>`.
- Existing G02 canonical IDs win when an exact base arXiv ID or normalized DOI
  matches. G03 augments aliases and provenance but does not overwrite conflicts.
- DOI normalization removes resolver prefixes, lowercases, and strips wrapping
  whitespace. DOI-only identities use the G02 `PAPER-HASH-<16 hex>` rule.
- OpenAlex and Semantic Scholar IDs are aliases, never canonical IDs by
  themselves.
- Title normalization uses Unicode NFKC, case folding, punctuation removal, and
  whitespace collapse. Title similarity alone never authorizes a merge.
- Exact normalized-title records with conflicting authors or publication dates
  remain distinct and carry `IDENTITY_AMBIGUOUS` provenance.
- Missing or conflicting bibliographic fields use explicit sentinels and remain
  auditable. An unavailable ancestor remains a manifest identity with
  `selection_status=METADATA_ONLY`, `ANCESTRY_RESOLUTION=UNAVAILABLE`,
  `local_path=NOT_ACQUIRED`, and no invented content or full-text-availability
  claim.
- A known G02 seed that has no exact OpenAlex location match remains its existing
  canonical manifest identity with `G03_SEED=YES`, `CITATION_DEPTH=0`,
  `ANCESTRY_RESOLUTION=UNAVAILABLE`, and
  `OPENALEX_RESOLUTION=UNAVAILABLE`. G03 does not invent an OpenAlex alias or
  treat this provider gap as evidence that the paper or its full text is absent.
- Every new row preserves provider IDs, aliases, seed ancestry, minimum depth,
  traversal direction, source URLs, identity state, and metadata-screen status
  in sorted manifest notes.

## Citation Request Ledger

Path: `arxiv-reference/sources/citation-request-ledger.tsv`

```text
request_id\tgoal_id\tseed_paper_id\ttraversal_paper_id\tdepth\tdirection\tservice\toperation\tnormalized_identifier\tparameters\trequested_at_utc\tpage_cursor\tresponse_status\tresult_count\tresponse_checksum\tcache_checksum\tclient_version\tcache_status\tattempt\tretry_events\trate_limit_events\tpolicy_url\tpolicy_checked_date\tcache_path\tterminal_state
```

- UTF-8, LF, literal TAB columns, and no embedded tabs/newlines.
- Multi-values use `|`; literal pipes use `%7C`.
- Missing metadata is `UNKNOWN`; inapplicable metadata is `NOT_APPLICABLE`.
- Request IDs are contiguous `REQ-G03-NNNN`; `goal_id` is always `G03`.
- Direction is `SEED_RESOLUTION`, `BACKWARD`, or `FORWARD`.
- Depth is 0, 1, or 2 and describes the work whose neighborhood is requested.
- Parameters are sorted percent-encoded key/value pairs and contain no secrets.
- `response_checksum` is lowercase SHA-256 over the exact bytes received from
  the provider. `cache_checksum` is lowercase SHA-256 over the durable cache
  body. They are equal unless an explicitly authorized key-only sanitizer
  discarded an unsolicited subtree before storage.
- Cache bodies live beneath ignored `arxiv-reference/cache/g03/`; every body is
  ledger-referenced, and unreferenced files are rejected.
- Cache bodies may contain citation metadata only. PDF, source archive, abstract
  text/inverted indexes, and full-text payloads are forbidden.
- Semantic Scholar citation pages may include an unsolicited top-level
  `citingPaperInfo` or `citedPaperInfo` subtree even when fields exclude it. The
  client discards that whole subtree without inspecting values, before durable
  storage. Every other forbidden field remains fatal. Raw bytes are not retained;
  their checksum remains in `response_checksum`, while `cache_checksum` proves
  the sanitized selected-metadata body.
- Terminal states are `COMPLETE`, `EMPTY`, `UNAVAILABLE`, `RATE_LIMITED`, and
  `FAILED`. A completed operation is never fetched again when its cache verifies.
- Each row names `OpenAlex` or `SemanticScholar`; its service, operation,
  parameters, client version, policy URL, and provider-specific cache directory
  must agree. Semantic Scholar batch POST IDs are recorded exactly in sorted
  request parameters so the request body is reproducible without a new column.

## Service And Acquisition Boundary

G03 may request bibliographic and citation metadata only from services explicitly
authorized in the dated preflight. API selections SHALL omit abstracts, concepts,
embeddings, content, and full-text fields. The response cache remains ignored by
Git. G03 downloads no PDF, source archive, abstract, paper body, or repository.

Access ambiguity, credential requirements not satisfied locally, unclear reuse
terms, persistent authorization or rate-limit failure, malformed identity data,
or inability to suppress abstract/full-text fields stops the affected service.

## Close Gate

G03 closes only when all G00-G03 tests and the complete corpus validator pass;
all requests, caches, identities, edges, stops, and caps reconcile; Git/license
and prohibited-artifact gates pass; and an independent adversarial reviewer has
either cleared or durably recorded every finding. The report must name
foundational, implementation/evaluation, contradiction, and coverage-gap
branches, decision impact by architecture question, and the exact bounded G04
acquisition set.

# G02 Metadata Discovery Contract

**Status:** Frozen before first G02 external request
**Goal:** `G02`
**Frozen:** 2026-08-11
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`

This document freezes the request ledger, query aggregation, identity, manifest,
ranking, cache, and retry encodings used by G02. It authorizes no service by
itself. Each service also requires a dated preflight in
`governance/g02-service-preflight.md` before its first request.

## Request Provenance Ledger

Path: `arxiv-reference/sources/metadata-request-ledger.tsv`

Exact header:

```text
request_id	goal_id	query_id	variant_id	service	operation	normalized_query	parameters	requested_at_utc	page_cursor	response_status	result_count	response_checksum	client_version	cache_status	attempt	retry_events	rate_limit_events	policy_url	policy_checked_date	cache_path	terminal_state
```

Encoding rules:

- UTF-8, LF line endings, literal TAB columns, and no embedded tabs/newlines.
- Multi-values use `|`; literal pipes use `%7C`.
- Missing metadata is `UNKNOWN`; inapplicable metadata is `NOT_APPLICABLE`.
- Timestamps are UTC RFC 3339 with a trailing `Z`; dates are `YYYY-MM-DD`.
- `request_id` is `REQ-G02-NNNN`; `variant_id` is
  `<QRY-ID>-<ALL|PRE2001|2001_2010|2011_2020|2021_CURRENT>`.
- `cache_status` is `HIT` or `MISS`; `terminal_state` is `COMPLETE`,
  `RATE_LIMITED`, or `FAILED`.
- `response_checksum` is lowercase SHA-256 of the exact cached response body.
- `cache_path` is repository-relative beneath ignored `arxiv-reference/cache/g02/`.
- Parameters are sorted `key=value` pairs joined by `&`, with values percent
  encoded. Credentials and secrets are forbidden.

## Query-Family Aggregation

G02 executes exactly the 25 G01 families. Each family has at most five logical
variants and no semantic expansion:

| Suffix | Date range |
|---|---|
| `ALL` | all years |
| `PRE2001` | through 2000-12-31 |
| `2001_2010` | 2001-01-01 through 2010-12-31 |
| `2011_2020` | 2011-01-01 through 2020-12-31 |
| `2021_CURRENT` | 2021-01-01 through 2026-08-11 |

The arXiv syntax translation combines recognizable anchors contained in the
G01-linked algorithm terms with tokens contained in the G01-linked
mechanism/resource terms and category constraints. It does not add outside
vocabulary. Date variants add only a `submittedDate` constraint. Each variant
returns at most 15 records, so 125 successful variants cannot exceed 1,875 raw
records or the 5,000-raw-record cap.

A family is:

- `EXECUTED` when every attempted variant is terminal and at least one returned
  successfully, including a valid zero-result response;
- `RATE_LIMITED` when persistent throttling prevents terminal coverage;
- `FAILED` when all attempted variants fail or a non-retryable failure stops it;
- `SUPERSEDED` only through an explicit controller decision recorded in the
  journal, never by automatic query rewriting.

The canonical query-ledger `executed_at` is the latest successful variant time,
`result_count` is the sum of raw records across its variants, and
`response_checksum` is SHA-256 of the newline-joined ordered variant response
checksums. The order is ascending full `variant_id` using Unicode code-point
lexicographic order. Completed variant IDs SHALL NOT be requested again; a verified local
cache produces a `HIT` ledger row instead.

## Canonical Paper Identity

- Modern arXiv IDs use `PAPER-<base-arxiv-id>` without a version suffix.
- Legacy IDs use `PAPER-LEGACY-<first-16-hex-of-sha256(base-id)>` and preserve
  the original legacy ID in `notes`.
- A record without arXiv identity uses
  `PAPER-HASH-<first-16-hex-of-sha256(normalized-doi-or-title-authors-date)>`.
- arXiv versions collapse into one canonical row and preserve every version.
- DOI comparison is case-insensitive after removing resolver prefixes.
- Title comparison uses Unicode NFKC, case folding, whitespace collapse, and
  punctuation removal. Title similarity alone never authorizes a merge.
- A normalized-title collision with conflicting authors or dates remains a
  separate row marked `IDENTITY_AMBIGUOUS` in `notes`.
- No service overwrites conflicting metadata; conflicts and all aliases remain
  in `notes`.

## Paper Manifest Encoding

The exact SOP header remains authoritative. Every G02 row:

- has `selection_status=METADATA_ONLY`;
- has `evidence_grade=D_THEORETICAL_OR_INCOMPLETE`, which is an eligibility
  placeholder and not a paper finding;
- uses `local_path=NOT_ACQUIRED` and `sha256=NOT_ACQUIRED`;
- preserves arXiv abstract/PDF locators as metadata but downloads neither;
- stores list fields with `|` and literal pipes as `%7C`;
- stores notes as sorted `KEY=VALUE` clauses joined by `;`.

Required note keys are `ALIASES`, `VERSIONS`, `SOURCE_URLS`, `DISCOVERY_ERAS`,
`NEIGHBORING_DOMAIN`, `PRE_ARXIV_ANCESTRY`, and `IDENTITY_STATE`.
`SOURCE_CLAIM`, evidence-card content, architecture claims, and experiment claims
are forbidden in every metadata-only row.

## Metadata Screen Score

`score_breakdown` is serialized in this exact order:

```text
MR=<0-20>;RR=<0-20>;IS=<0-15>;BS=<0-15>;TR=<0-15>;FL=<0-10>;ND=<0-5>
```

`relevance_score` is the integer sum, from 0 through 100. Components are
deterministic lexical metadata-screening signals over title, abstract, category,
comment, and journal-reference fields. They are estimates of reading priority,
not findings from the paper. Citation count and publication recency contribute
zero points.

Exploration flags in `notes` preserve candidates from each of these bounded
quotas where available: at least 20 older candidates (published through 2000),
20 citation-unknown or low-citation candidates, 20 contradictory-looking
candidates, and 20 distant-domain candidates. Lack of available candidates is
reported as a coverage gap, not filled with invented records.

## Safety Caps And Retries

- 25 families, 125 variants, 200 total HTTP requests, 5,000 raw records, and
  2,000 canonical candidates are hard maxima.
- One in-flight request per service; arXiv requests are spaced at least three
  seconds apart across all machines under control.
- Only transport failures, HTTP 408, 429, and 5xx are retryable.
- Retries use bounded exponential backoff, honor `Retry-After`, and stop after
  three total attempts.
- HTTP 401/403, malformed requests, policy conflicts, cap exhaustion, or a
  request requiring full text stop execution.
- Response bodies remain ignored. Only checksums and non-secret provenance are
  committed.

### Amendment 1: Overconstrained Syntax Correction

After the first 16 HTTP-200 variants returned zero total records, a RED test
proved that the compiler required coined multi-word repository phrases verbatim.
Before continuing, G02 changed only syntax translation from exact linked phrases
to recognizable algorithm anchors plus tokens already present in the linked
mechanism terms. No query family, source term, AQ, category, or exclusion was
added.

The 16 original attempts remain counted and checksummed. Their provenance
operation is `legacy_api_query_metadata_invalidated_overconstrained`, their
terminal state is `FAILED`, and their response bodies are retained locally with
an `.overconstrained.xml` suffix. This explicit quality-gate invalidation permits
one corrected request for the same logical variant; it is not a cache miss,
service retry, or completed-request replay.

### Amendment 2: Date-Filter Ordering Correction

After all 125 first-pass variants completed, a cross-era audit found that nearly
every candidate appeared in every `DISCOVERY_ERAS` bucket. Cached feed titles and
the official arXiv query manual showed why: exclusions must use `ANDNOT`, and a
positive `submittedDate` range placed after an `ANDNOT` chain can become part of
the negative branch instead of constraining the positive result set.

A RED test now requires native `ANDNOT` syntax and requires `submittedDate` to
precede every exclusion. A second RED test rejects any date-bucket response whose
published date falls outside its declared range.

The 100 original date-bucket requests remain counted, checksummed, and cached
with `.datefilter-invalid.xml` suffixes. To stay within the 200-request hard cap,
G02 corrects the two historically valuable buckets for every family:
`PRE2001` and `2001_2010` (50 new requests). The broad `ALL` pass remains valid.
The `2011_2020` and `2021_CURRENT` attempts remain explicit `FAILED` operations
with `invalidated_date_filter_not_retried_request_cap`; modern coverage comes
from the valid broad pass and is reported as a date-bucket coverage limitation.

For this run, `EXECUTED` therefore requires valid terminal `ALL`, `PRE2001`, and
`2001_2010` variants. The two unretried modern date variants are preserved as
failed provenance and never relabeled as successful. Total HTTP requests are
bounded at 191: 16 overconstrained attempts, 125 first-pass attempts, and 50
corrected historical attempts.

## G02 Boundary

G02 performs metadata discovery and metadata-only screening. It creates no PDF,
source archive, full text, citation edge, evidence card, architecture candidate,
experiment, source claim, or G03 activity.

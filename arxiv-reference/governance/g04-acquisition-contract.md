# G04 Acquisition And Parsing Contract

**Status:** Frozen before the first G04 external request
**Goal:** `G04`
**Frozen:** 2026-08-11
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`

## Exact Queue Derivation

The queue is derived at runtime, never embedded as a fallback:

1. Parse the ordered 25 rows under `## Recommended G03 Seed Set` in the G02 report.
2. Parse G03 screening rows whose disposition is `ACQUIRE`; require integer ranks 1 through 25 with no gap.
3. Parse the 50 rows under `## Exact Recommended G04 Acquisition Set` in the G03 report.
4. Require G04 rows 1-25 to equal the G02 order and rows 26-50 to equal the screening order.
5. Require 50 unique IDs and an exact manifest row for every ID.

Any mismatch is fatal before network access.

## Ledger Contract

`sources/download-ledger.tsv` contains exactly one terminal row per queue identity.
Its exact header is frozen in `artifact-schema-contracts.md`. TSV uses UTF-8,
LF endings, one header, tab separators, no tabs/newlines in cells, RFC 3339 UTC
timestamps, base-10 integers, and uppercase SHA-256 hex.

- `request_id`: `REQ-G04-` followed by four decimal digits matching queue rank.
- `goal_id`: literal `G04`.
- `queue_rank`: integer 1-50, contiguous and unique.
- `paper_id`: exact canonical queue identity.
- `source_service`: `ARXIV`, `PUBLISHER`, or `NONE`.
- `retrieval_uri`: final direct PDF URI, or `NOT_AVAILABLE`.
- `response_status`: `HTTP_<code>`, `CACHE_HIT`, `TRANSPORT_ERROR`, or `NOT_REQUESTED`.
- `content_length_bytes` and `page_count`: nonnegative integers; zero when unavailable.
- `source_checksum`, `trace_checksum`, and `extracted_checksum`: uppercase 64-hex SHA-256 or `NOT_AVAILABLE`.
- `license_uri`: discovered URI or `NOT_DISCOVERED`.
- `license_state`: exactly one schema-controlled `LICENSE_*` token.
- `cache_status`: `MISS`, `HIT`, or `NOT_APPLICABLE`.
- `parse_status`: `PARSED`, `PARSE_FAILED`, or `NOT_APPLICABLE`.

Acquisition status is one of:

```text
ACQUIRED
UNAVAILABLE
LICENSE_BLOCKED
RATE_LIMITED
AUTHORIZATION_FAILED
NOT_FOUND
PAYLOAD_REJECTED
FAILED
SERVICE_STOPPED
```

`ACQUIRED` requires `PARSED` or `PARSE_FAILED`, an existing ignored PDF, a PDF
checksum, and manifest status `DEEP_READ`. Every other acquisition status
requires `NOT_APPLICABLE`, zero pages, and unavailable artifact sentinels.

## Local Path And Filename Contract

- PDF: `sources/papers/<paper_id>.pdf`.
- Trace: `cache/g04/traces/<paper_id>.json`.
- Extracted text: `cache/g04/text/<paper_id>.txt`.
- Temporary files: `cache/g04/tmp/<paper_id>.<suffix>.part`.
- `paper_id` must match `PAPER-[A-Za-z0-9][A-Za-z0-9.-]*`; no slash, backslash,
  percent escape, control byte, `..` component, symlink destination, or path
  outside the declared root is accepted.
- Writes use a same-filesystem temporary path, flush, checksum, validation, and
  atomic replacement.
- `sources/papers/` and all of `cache/g04/` remain ignored and untracked.

## Resource And Request Bounds

- Exact queue: 50 identities.
- External HTTP attempts: 220 total, including redirects treated as one library
  operation but every retry as another attempt.
- Per-paper attempts: five, including retrieval retries but excluding the two
  shared batch-metadata requests.
- Retry chain: at most three total attempts for transport errors, HTTP 408,
  429, or 5xx only.
- `Retry-After`: parse both delay-seconds and RFC HTTP-date forms against the
  recorded attempt timestamp; honor the larger valid delay.
- Concurrency: one external request in flight globally.
- PDF response: at most 100 MiB after transfer decoding.
- Total accepted PDF bytes: at most 5 GiB.
- Redirects: HTTPS only, at most five, with the final host and URI recorded.
- Timeouts: 20 seconds to connect and 120 seconds to read one response.

Replay and validation sum paper-ledger attempts plus shared metadata-trace
attempts and fail closed above 220. Cap exhaustion creates explicit terminal
rows; it never expands scope.

## Source Resolution Order

1. For manifest arXiv identities, use one exact arXiv metadata batch to verify
   identity, canonical version, PDF URI, and submitted license. Retrieve only
   the verified official arXiv PDF.
2. For non-arXiv DOI identities, use one exact OpenAlex DOI batch only to locate
   public full-text metadata. Accept a direct PDF only when DOI and normalized
   title match and the selected location is the publisher or official
   proceedings source, not a repository mirror.
3. Do not title-search, scrape search engines, guess URLs, bypass controls, use
   credentials, or substitute another paper. A missing acceptable direct source
   is `UNAVAILABLE`.

For this frozen queue, a DOI-prefix-to-official-host allowlist verifies the PDF
host independently of OpenAlex's source-type label. The bounded prefixes cover
Springer (`10.1007`), Taylor and Francis / Internet Mathematics (`10.1080`),
IEEE (`10.1109`), SIAM (`10.1137`), ACM (`10.1145`), IOS Press (`10.3233`),
and Dagstuhl (`10.4230`). A journal label on an unapproved host is insufficient.

## License Classification

- `LICENSE_PERMISSIVE_VERIFIED`: discovered CC0, CC BY, or CC BY-SA URI.
- `LICENSE_RESTRICTED_OR_CONDITIONAL`: arXiv non-exclusive distribution, CC
  BY-NC, CC BY-ND, CC BY-NC-SA, CC BY-NC-ND, or another explicit conditional
  license.
- `LICENSE_UNKNOWN`: content is publicly retrievable for local research but no
  reliable license URI is discovered.
- `LICENSE_UNAVAILABLE`: no full text is acquired and no content license is
  discovered.

Classification is operational metadata, not legal advice. Local research use
does not grant redistribution. PDFs and extracted text may not be staged or
committed without separate artifact-specific human approval.

## PDF Validation And Parsing

An accepted payload must have an HTTP success result, declared or sniffed PDF
media type, `%PDF-` within its first 1024 bytes, `%%EOF` in its final 4096 bytes,
nonzero size within caps, and successful strict-enough parsing with at least one
page. HTML, authentication pages, empty files, encrypted files that cannot be
parsed, and malformed/truncated PDFs are rejected.

The pinned parser is Python plus `pypdf==6.14.2`. Extraction iterates
pages in document order, calls `extract_text(extraction_mode="layout")`,
normalizes only CRLF/CR to LF, removes NUL bytes, strips trailing horizontal
whitespace, ends every page with LF, and joins pages with the literal marker:

```text
\n\f\n
```

The output begins with no generated summary or metadata header. Parser name,
version, exact option string, page count, PDF checksum, output path, and output
checksum are recorded. Running twice against the same bytes must be identical.

## Cache, Replay, And Failure Rules

Before any request, validate a completed ledger row and local checksums. A valid
row is a cache hit and causes no network request. A checksum mismatch fails
closed. Ignored trace JSON records each attempt time, requested URI, redirect
chain, response status, retry/backoff event, response headers allowlist, final
URI, and payload checksum; it stores no credentials or page body.

Trace validation parses JSON, requires contiguous attempt numbers and exact
goal/paper/service bindings, reconciles timestamps and terminal status to the
ledger, binds the initial paper request to the source URL recovered from frozen
metadata, proves each contiguous HTTPS redirect route, binds final URI and
acquired payload SHA-256, and checksum-links metadata bodies. Shared metadata
evidence must match the exact frozen request, terminate on the authorized arXiv
or OpenAlex service host, return every expected arXiv or DOI identity, and stay
within the three-attempt operation ceiling. Checksummed malformed JSON is still
invalid. HTTP 401/403
and exhausted HTTP 429 persist the affected host in
in-memory state and in the terminal ledger; resumed processes reconstruct host
stops from that ledger before any later request.

Network-disabled replay reads the frozen queue, ledger, manifest, local PDFs,
traces, and extracted text; revalidates every link and checksum; and regenerates
the generated manifest, ledger, and report byte-for-byte. Campaign status and
Markdown indexes are separately validated governance handoffs, not generated
replay products.

No G04 success or failure authorizes semantic reading. Manifest rows remain
`DEEP_READ` when acquired and become `UNAVAILABLE` only after a terminal
unavailable result. G04 never writes `READ_COMPLETE`.

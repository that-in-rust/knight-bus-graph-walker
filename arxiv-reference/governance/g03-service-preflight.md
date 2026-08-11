# G03 Citation Service Preflight

**Status:** `AUTHORIZED_OPENALEX_SEMANTIC_SCHOLAR_METADATA_ONLY`
**Goal:** `G03`
**Checked:** 2026-08-11
**Review basis:** current official documentation only

**Availability amendment:** Two checksummed exact filters for the first seed,
using its base and canonical versioned arXiv landing URLs, both returned zero
rows. The service remains authorized, but zero exact matches are now preserved
as provider-unavailable stopped branches instead of terminating unrelated seed
traversals. This narrows claims and does not authorize fuzzy search.

**Provider-recovery amendment:** The completed OpenAlex pass resolved only one
of the 25 exact arXiv seeds and produced no citation edge. Current official
Semantic Scholar Academic Graph documentation explicitly supports
`ARXIV:<id>`, exact batch lookup, references, citations, selected fields, and
unauthenticated access. Its API license permits internal non-commercial
research and requires Semantic Scholar attribution in published material.
G03 therefore authorizes a second metadata-only provider under the original
90-attempt ceiling. The amendment preserves all OpenAlex provenance and does
not authorize title search, abstracts, snippets, recommendations, datasets, or
publication content.

This is an operational research decision, not legal advice. Authorization is
limited to the selected bibliographic fields and provider-backed citation
relations below. It does not authorize publication content.

## Service Decisions

| Service | Decision | Reason |
|---|---|---|
| OpenAlex | `AUTHORIZED_METADATA_ONLY` | Officially documented work IDs, `referenced_works`, `cites` filter, field selection, open metadata, and a bounded anonymous allowance support the G03 contract. |
| Semantic Scholar | `AUTHORIZED_METADATA_ONLY` | Official documentation supports exact arXiv identifiers, batch paper lookup, references, citations, field selection, and anonymous access. The API license permits this bounded internal non-commercial research use; committed outputs provide attribution and raw API bodies stay in the ignored local cache. |
| Crossref | `NOT_USED_NOT_AUTHORIZED` | No G03 request is authorized. |
| arXiv | `NOT_USED_NOT_AUTHORIZED` | G02 caches remain local; arXiv is not used as a citation graph. |

Exactly two citation services are authorized: OpenAlex and Semantic Scholar.

## Official Sources And Current Findings

| Concern | Official source | Frozen G03 interpretation |
|---|---|---|
| Authentication and pricing | https://developers.openalex.org/api-reference/authentication | Anonymous API use is available. The detailed page states `$0.10/day`; the official LLM reference states `$0.01/day`. The lower value wins. No local key is present or required for this under-$0.01 plan. |
| Endpoint costs | https://developers.openalex.org/api-reference/authentication | List/filter costs `$0.0001` per call; search costs `$0.001`; singleton lookup is free. G03 authorizes list/filter only and caps all attempts at 90, so listed cost remains at most `$0.009`. |
| Rate and batching | https://developers.openalex.org/api-reference/authentication | Provider ceiling is 100 requests/second, 100 OR values, and 100 results/page. G03 uses one in-flight request, at least one second between attempts, and batches up to 100 IDs. |
| Works and citation filters | https://developers.openalex.org/api-reference/works | `referenced_works` supplies outgoing provider citation IDs; `cites` is a filter for incoming citing works; `locations.landing_page_url` is filterable for exact arXiv seed resolution. |
| Field selection | https://developers.openalex.org/guides/selecting-fields | `select=` limits list and singleton responses to root fields. G03 requests only the allowlist below. |
| Pagination | https://developers.openalex.org/guides/page-through-results | `per_page` is at most 100; cursor paging is available. G03 intentionally requests one page only and records that recall limit instead of following a cursor. |
| Errors and retries | https://developers.openalex.org/api-reference/errors | Follow 301 identity redirects while preserving aliases; retry only transport, 408, 429, and 5xx with 1/2/4-second backoff and any `Retry-After`; never retry 400/401/403/404. |
| Metadata reuse | https://help.openalex.org/hc/en-us/articles/28926392245399-How-is-OpenAlex-open | OpenAlex explicitly describes its data as freely reusable under CC0. |
| Terms | https://openalex.org/OpenAlex_termsofservice.pdf | Terms grant use of free features and contain broad restrictions on unauthorized reproduction. G03 relies only on the separately explicit CC0 metadata permission, makes bounded API calls, and never copies publication content. |
| Citation completeness | https://help.openalex.org/hc/en-us/articles/27810109633943-The-reference-counts-in-OpenAlex-seem-off-Why-is-that | `referenced_works` can omit references not resolved into OpenAlex. G03 reports provider-visible ancestry, never a complete bibliography. |
| S2 exact identifiers and endpoints | https://api.semanticscholar.org/api-docs/graph | Paper details, batch lookup, references, and citations accept Semantic Scholar IDs; details, references, and citations explicitly support `ARXIV:<id>`. Batch lookup accepts up to 500 IDs. G03 uses one exact 25-ID batch and no search endpoint. |
| S2 field selection and pagination | https://api.semanticscholar.org/api-docs/graph | `fields` controls returned paper metadata; references and citations use `offset` and `limit` up to 1,000. G03 requests the exact allowlist below, offset zero, and at most 75 results. |
| S2 authentication and rate behavior | https://www.semanticscholar.org/product/api | Most endpoints permit anonymous access. Anonymous callers share a provider-wide rate pool and may be throttled; authenticated introductory rate is one request per second. G03 sends one request at a time with at least 1.1 seconds between attempts and treats 429 as retryable. |
| S2 batching guidance | https://www.semanticscholar.org/product/api/tutorial | Official guidance recommends batch endpoints. G03 batches all 25 exact arXiv seed identities in one POST request. |
| S2 API license and attribution | https://www.semanticscholar.org/product/api/license | The license permits API access with other software and internal non-commercial research, contemplates stored data security, and requires attribution for published material. G03 caches exact responses locally, commits only normalized bibliographic facts and edges, and attributes Semantic Scholar in reports and source URLs. This is an operational interpretation, not legal advice. |

## Authorized Operations

1. Resolve each arXiv seed with one exact `locations.landing_page_url` OR-filter
   over its base and canonical versioned URLs. Preserve zero matches as
   `OPENALEX_RESOLUTION=UNAVAILABLE`; reject multiple or bibliographically
   conflicting matches.
2. Read `referenced_works` from selected seed or ancestor metadata.
3. Query forward neighbors with `filter=cites:<OpenAlex-ID>`.
4. Batch-fetch up to 100 OpenAlex work IDs with `filter=openalex:<ID|...>`.
5. Resolve all 25 frozen seeds in one Semantic Scholar `/paper/batch` POST with
   exact `ARXIV:<id>` values.
6. Request one `/paper/<S2-ID>/references` page and one
   `/paper/<S2-ID>/citations` page for each resolved seed.
7. Expand at most five globally ranked depth-1 branches with one references or
   citations page, according to the branch direction.

No search, semantic search, group-by, autocomplete, content, text/aboutness,
snapshot, or full-text operation is authorized.

Semantic Scholar search, title matching, recommendations, snippets, datasets,
relevance ranking, paper bodies, and citation contexts are also unauthorized.

## Exact Field Allowlist

Every list request SHALL use this root-level `select` value:

```text
id,doi,display_name,publication_date,type,authorships,ids,locations,referenced_works,cited_by_count,is_retracted,updated_date
```

The client SHALL reject a response containing `abstract`,
`abstract_inverted_index`, `full_text`, `fulltext`, `content`, or `ngrams`.
It SHALL not request `primary_location`, best/open-access locations, topics,
keywords, concepts, embeddings, citation contexts, or repository fields.
Because OpenAlex field selection is root-level only, selected `locations` may
contain nested PDF locators. The parser discards those locators, never persists
them to the manifest, and never fetches them.

Every Semantic Scholar request SHALL use this paper field allowlist:

```text
paperId,externalIds,url,title,year,publicationDate,authors,venue,citationCount,referenceCount
```

The S2 parser rejects any unselected root paper field and recursively rejects
`abstract`, `tldr`, `openAccessPdf`, `embedding`, `contexts`, `intents`,
`isInfluential`, snippets, content, or full-text fields. Provider pagination
envelopes may contain only `offset`, `next`, and `data`; relationship envelopes
may contain only `citedPaper` or `citingPaper`.

**Observed response-shape amendment:** The first references response included an
unsolicited top-level `citingPaperInfo` subtree containing `openAccessPdf`
metadata despite the selected fields. The relationship `data` rows contained
only the selected paper fields. The client therefore records the exact raw
response SHA-256, discards the entire `citingPaperInfo` or `citedPaperInfo`
subtree without inspecting values, and durably caches only the selected page
with its own checksum. Any forbidden field remaining after that single
key-based deletion stops the service. No PDF URL is followed and no PDF,
abstract, snippet, or paper body is retained.

## Bounded Request Plan

| Operation | External attempts |
|---|---:|
| Preserved completed OpenAlex attempts | 28 |
| Exact S2 batch resolution of all 25 seeds | 1 |
| S2 depth-1 references pages | 25 |
| S2 depth-1 citations pages | 25 |
| S2 depth-2 pages across globally ranked branches | at most 5 |
| Remaining retry reserve | at least 6 |
| Maximum planned total | 84 |
| Hard ceiling | 90 |

The 28 completed OpenAlex attempts remain checksummed provenance and are never
deleted or relabeled. S2 operations begin at request 29. The client stops before
the hard ceiling and before 6,000 raw observations. No paid balance or automatic
overage is possible or authorized; no API key is configured or required.

## Cache, Attribution, And Secret Rules

- Exact selected JSON responses are cached under ignored
  `arxiv-reference/cache/g03/openalex/` or
  `arxiv-reference/cache/g03/semantic-scholar/` and checksummed in the request
  ledger.
- Only normalized bibliographic metadata, canonical IDs, citation edges, source
  URLs, checksums, and non-secret provenance are committed.
- Outputs name the contributing provider. Semantic Scholar-derived public
  material includes `Semantic Scholar` attribution and links to
  `https://www.semanticscholar.org/?utm_source=api` or a provider paper URL with
  the same UTM parameter.
- No API key, cookie, email, credential, or private identifier is written to a
  URL, cache, ledger, journal, report, or error.
- The pipeline rejects unreferenced cache files and PDF/archive/full-text-like
  content beneath the G03 cache boundary.

## Stop Conditions

Stop the service on HTTP 401/403; malformed filter behavior; multiple seed
identities or bibliographic conflict after reconciliation; forbidden response fields; unresolved
redirects; a daily-budget signal below the next operation cost; persistent 429,
5xx, or transport errors; response/cap inconsistency; terms changes; or any need
for a paper body, abstract, PDF, source archive, or repository.

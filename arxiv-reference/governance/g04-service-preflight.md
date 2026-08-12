# G04 Source-Service Preflight

**Status:** `AUTHORIZED_BOUNDED_LOCAL_RESEARCH_ACQUISITION`
**Goal:** `G04`
**Checked:** 2026-08-11
**Scope:** exact identity verification, lawful direct-source resolution, local PDF acquisition, and deterministic local parsing

This is an operational research decision, not legal advice. It authorizes no
redistribution, publisher authentication, paywall access, search-engine crawl,
or semantic use of paper contents.

## Service Decisions

| Service | Decision | G04 operation |
|---|---|---|
| arXiv | `AUTHORIZED_EXACT_METADATA_AND_PDF` | One exact ID-list metadata request plus official PDFs for queue identities verified to arXiv. |
| OpenAlex | `AUTHORIZED_EXACT_DOI_LOCATION_METADATA` | One exact DOI OR-filter for non-arXiv queue identities, selecting only identity and open-location fields. |
| Public publisher endpoint | `CONDITIONAL_DIRECT_PDF_ONLY` | Retrieve only an unauthenticated HTTPS direct PDF identified by the exact OpenAlex DOI record as the publisher or official proceedings location. |
| DOI resolver | `METADATA_LINK_ONLY_NO_CONTENT_SCRAPE` | Preserve canonical DOI links; do not treat DOI content negotiation as a PDF service. |
| Semantic Scholar | `NOT_USED_NOT_AUTHORIZED` | G03 cache may establish lineage but G04 makes no new S2 request. |
| Crossref REST API | `NOT_USED_NOT_AUTHORIZED` | DOI and OpenAlex metadata are sufficient for this bounded pass. |
| Search engines and title search | `PROHIBITED` | No broad discovery or substitution is permitted. |

## Official Sources And Findings

| Concern | Official source | Frozen G04 interpretation |
|---|---|---|
| arXiv API terms and rate limits | https://info.arxiv.org/help/api/tou.html | Local research retrieval is allowed; legacy APIs use one connection and no more than one request every three seconds. Content copyright and redistribution remain license-specific. |
| arXiv API behavior | https://info.arxiv.org/help/api/user-manual.html | Exact `id_list` metadata retrieval is used once; no metadata search is needed. |
| arXiv licenses | https://info.arxiv.org/help/license/index.html | Every article is freely viewable/downloadable, but versions can carry different licenses and copyright remains with the holder except CC0 treatment. The discovered version license is recorded without upgrading permissions. |
| arXiv subset acquisition | https://info.arxiv.org/help/bulk_data/index.html | arXiv identifies export crawling as appropriate for a subset or new content; G04 is a fixed 31-or-fewer PDF subset, not bulk corpus mirroring. |
| OpenAlex authentication and limits | https://developers.openalex.org/api-reference/authentication | Exact metadata resolution uses one anonymous request, one in-flight operation, and at least 1.1 seconds between attempts. No search endpoint or paid balance is used. |
| OpenAlex works and locations | https://developers.openalex.org/api-reference/works | Exact DOI filtering and selected location fields are used only to find a direct publisher/official-proceedings source. |
| OpenAlex metadata reuse | https://help.openalex.org/hc/en-us/articles/28926392245399-How-is-OpenAlex-open | OpenAlex describes its metadata as CC0; G04 stores only bounded identity/location provenance. |
| DOI resolution | https://doi.org/help.html | DOI HTTPS resolution identifies the registered landing resource; it is not assumed to grant PDF access. |
| DOI content negotiation | https://www.crossref.org/documentation/retrieve-metadata/content-negotiation/ | Documented negotiation formats are metadata formats. G04 does not send an invented `Accept: application/pdf` contract to DOI infrastructure. |

## Client And Rate Configuration

- User agent: `KnightBusArxivPatternFoundry/0.1 (+https://github.com/amuldotexe/knight-bus-graph-walker)`.
- Global concurrency: one request.
- arXiv cadence: at least 3.1 seconds between request starts.
- OpenAlex cadence: at least 1.1 seconds between request starts.
- Publisher cadence: at least 3.1 seconds per host when no stricter published rule is known.
- Retryable: transport failure, HTTP 408, 429, and 5xx only; exponential 3.1/6.2-second local backoff plus a larger valid `Retry-After`.
- Non-retryable: malformed request, 400, 401, 403, 404, paywall/login response, robots denial, unsafe redirect, HTML masquerading as PDF, or license conflict.
- Hard ceiling: 220 attempts globally, five attributed attempts per paper, three attempts per retry chain.
- Credentials: none authorized. A credential prompt or CAPTCHA stops that source.

## Stop Decision

Proceed only after the G04 tests and validator prove the frozen contract. Stop
the affected service on persistent 429, any authorization barrier, terms
conflict, unexpected paid requirement, unsafe redirect, or cap exhaustion.
Record terminal outcomes for the untouched queue identities without silently
switching providers.

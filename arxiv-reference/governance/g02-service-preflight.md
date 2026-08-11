# G02 Source-Service Preflight

**Checked:** 2026-08-11
**Goal:** `G02`
**Scope:** metadata discovery and deterministic identity reconciliation only

## arXiv

- arXiv decision: `AUTHORIZED`
- Service and operation: legacy arXiv API, metadata query via
  `https://export.arxiv.org/api/query`.
- Intended use: execute the 25 G01 query families as at most 125 variants and
  cache at most 5,000 descriptive metadata records.
- Terms checked: `https://info.arxiv.org/help/api/tou.html`.
- API manual checked: `https://info.arxiv.org/help/api/user-manual.html`.
- Credentials: none required by the documented legacy query interface.
- Identification: the published terms do not require an API key or contact
  credential. Requests identify the client as
  `KnightBusArxivPatternFoundry/0.1` without placing personal data or secrets in
  committed provenance.
- Rate and concurrency: the official terms require no more than one request
  every three seconds across all controlled machines and one connection at a
  time. G02 uses at least 3.1 seconds and one in-flight request.
- Retry behavior: only transport failures, HTTP 408, HTTP 429, and HTTP 5xx;
  honor `Retry-After`; at most three total attempts; stop on 401/403.
- Metadata rights: the official terms state descriptive metadata is available
  under CC0 1.0 and may be retrieved, stored, transformed, and shared.
- Full-text boundary: e-print redistribution has separate copyright/license
  constraints. G02 downloads no PDF, source archive, or full text.
- Caching: exact Atom response bodies remain beneath ignored
  `arxiv-reference/cache/g02/`; committed provenance stores checksums and source
  URLs, not response bodies.
- Current operational risk: arXiv has warned that effective throttling and 503s
  can occur under load. Persistent throttling triggers the G02 stop condition;
  it is not bypassed with parallel clients or another machine.
- Stop decision: proceed under the local caps and stop rules above.

## Crossref

- Crossref decision: `NOT_USED_NOT_AUTHORIZED`
- Reason: arXiv metadata already exposes DOI aliases for this bounded discovery
  pass. G02 will not query Crossref unless an unresolved deterministic identity
  conflict makes reconciliation necessary.
- Consequence: no Crossref request, credential, cache, or provenance row is
  permitted under the current run. A dated official-policy preflight is required
  before changing this decision.

## OpenAlex

- OpenAlex decision: `NOT_USED_NOT_AUTHORIZED`
- Reason: citation metadata and ancestry exploration are unnecessary for G02
  metadata screening and would risk crossing into G03.
- Consequence: no OpenAlex request, credential, cache, or provenance row is
  permitted under the current run. A dated official-policy preflight is required
  before changing this decision.

## Authorization Boundary

This preflight authorizes only the arXiv metadata operation above. It does not
authorize PDF or source acquisition, citation traversal, evidence extraction,
GitHub acquisition, architecture generation, experiments, G03, or silent source
substitution.

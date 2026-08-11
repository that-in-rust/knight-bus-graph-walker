# Source Service Policy

**Status:** G00 campaign contract
**Network state:** `HARD_NO_NETWORK`
**Scope:** Metadata, citation, abstract, full-text, and repository source services

This is an operational research policy, not legal advice. A human decision maker
retains responsibility for terms, credentials, licensing, and redistribution
decisions.

## G00 Hard No-Network Gate

G00 permits local repository files and local test fixtures only. It SHALL NOT
perform web searches, API calls, citation lookups, remote repository requests,
authentication attempts, PDF downloads, or any other external-service request.

If G00 needs external state, it SHALL stop and report the missing input. Network
access remains disabled until a later goal packet explicitly authorizes named
services and bounded request work. Authorization for one goal or service does not
carry forward.

## Preflight For A Future Authorized Goal

Before the first request to each service, the goal record SHALL capture:

- service name, endpoint or operation, intended use, and bounded request cap;
- current published terms, access rules, and required client identification;
- credential requirement, approved credential source, and minimum required scope;
- published rate limits, concurrency limits, and `Retry-After` behavior;
- allowed metadata, full-text, caching, and redistribution uses; and
- a stop decision for any unresolved terms, credential, or permission question.

Credentials SHALL come from an approved secret store or environment, never from
committed files. Secrets SHALL NOT appear in queries, logs, caches, checksums,
errors, or journals. Clients SHALL NOT bypass authentication, paywalls, CAPTCHAs,
robots controls, or technical access restrictions.

## Respectful Request Behavior

Published service limits are ceilings, not targets. A client SHALL use the fewest
requests needed, bounded concurrency, pagination supplied by the service, and any
required contact or user-agent identification. When no limit is published, the
default is one in-flight request per service and at least three seconds between
requests.

The client SHALL stop before exceeding the goal's request cap. A lower local cap
or slower cadence wins over a more permissive service limit.

## Retries And Stop Conditions

Retries are permitted only for transient transport failures, HTTP 408, HTTP 429,
and HTTP 5xx responses. They SHALL use exponential backoff with jitter, honor
`Retry-After`, and stop after three total attempts unless the authorized goal
packet sets a lower limit.

The client SHALL NOT retry authorization failures, invalid requests, denied
access, or terms conflicts. Repeated rate limits, HTTP 401/403 responses, missing
credentials, or exhausted retries SHALL checkpoint progress and stop for human
review. Partial responses SHALL NOT be presented as complete results.

## Cache And Checkpoint Contract

Before a request, the client SHALL check an unexpired local cache and the goal
checkpoint. It SHALL use conditional requests when the service supports them and
SHALL NOT refetch a completed page or record without a recorded verification
reason.

Response bodies, extracted full text, and service caches SHALL remain in ignored
local paths. Committed ledgers MAY retain non-secret request metadata, canonical
identities, source URLs, status, and checksums. A checkpoint SHALL be written at
page or canonical-record boundaries so an interrupted run can resume without
repeating completed work.

## Reproducibility Metadata

Every authorized external operation SHALL record, when available:

- goal ID, service, endpoint or operation, normalized query and parameters;
- UTC request time, page or cursor, response status, result count, and checksum;
- client or tool name and version, model identity for generated work, and prompt;
- rate configuration, retry events, and cache hit or miss;
- terms or policy URL and the date it was checked; and
- any mutable, unavailable, or otherwise irreproducible external state.

Credential values and private response bodies are never reproducibility metadata.

## Canonical Identity Reconciliation

Records from different services SHALL be reconciled using arXiv identifier and
version, DOI, normalized title, authorship, publication date, and known version
relationships. A match SHALL produce one stable canonical paper ID while
preserving aliases, source URLs, service provenance, and version history.

No single service silently overwrites conflicting metadata. Material conflicts
SHALL be retained for review. Ambiguous records remain separate until a human or
a deterministic reconciliation rule resolves them.

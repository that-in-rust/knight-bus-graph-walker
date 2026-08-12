# G04 Independent Adversarial Review

## Reviewer Provenance

- Reviewer agent: `019ff1ac-564c-7b53-a586-7498b453fbd9`
- Model: `gpt-5.6-sol`
- Reasoning effort: `xhigh`
- Review mode: independent, read-only, network-disabled, no semantic paper reading
- First-pass verdict: `NOT_CLEARED`
- Second-pass verdict: `NOT_CLEARED`
- Third-pass verdict: `NOT_CLEARED`
- Fourth-pass verdict: `NOT_CLEARED`
- Fifth-pass verdict: `CLEARED`

## First-Pass Recomputed Evidence

The reviewer independently recomputed:

- 25 ordered G02 seeds plus 25 contiguous G03 `ACQUIRE` ranks, yielding 50
  unique queue identities with no substitution or rank mismatch;
- 50 terminal ledger rows: 34 `ACQUIRED`, 15 `UNAVAILABLE`, and one
  `AUTHORIZATION_FAILED`;
- 34 `PARSED` texts and 16 `NOT_APPLICABLE` parse outcomes;
- 35 paper attempts plus two shared metadata attempts, or 37 total;
- 36,477,968 accepted PDF bytes across 590 pages;
- 34 `LICENSE_UNKNOWN` and 16 `LICENSE_UNAVAILABLE` states;
- 34 PDFs, 34 extracted texts, 35 paper traces, and four metadata cache files,
  with all required checksums and source-PDF links matching;
- byte-identical re-extraction for 34 of 34 papers;
- zero injected transport calls and zero socket connections during replay;
- byte-identical ledger, manifest, and report with SHA-256 prefixes
  `B5249DBB`, `FC0A2A98`, and `3E62AD7D`;
- 107 of 107 local corpus artifacts ignored, with zero tracked or staged; and
- the then-current 130-test suite and full corpus validator passing.

The reviewer also confirmed that no manifest row was `READ_COMPLETE` and G04
created no semantic evidence, architecture, experiment, repository acquisition,
commit, or push.

## First-Pass Findings

### P2-01: Trace And Aggregate-Cap Validation

Checksummed malformed paper traces were accepted, metadata trace semantics were
not validated, and replay did not enforce paper plus metadata attempts against
the 220-attempt global cap.

Repair evidence: strict paper and metadata trace parsing, row/body binding,
contiguous-attempt validation, aggregate-cap validation, and RED-GREEN tests
`test_malformed_request_trace_rejected` and
`test_metadata_attempt_global_cap_rejected`.

### P2-02: Authorization And Throttle Host Stops

HTTP 403 did not persist a host stop, and the prior test covered only an
injected exception rather than real HTTP status handling.

Repair evidence: HTTP 401/403 and exhausted 429 persist host stops; resume state
reconstructs them from terminal ledger rows; RED-GREEN coverage lives in
`test_http_status_persists_host_stop`.

### P2-03: Official Publisher Host Verification

OpenAlex's `source.type` label alone could cause a repository host to be
accepted as a publisher source.

Repair evidence: the selected PDF host must now match a frozen official-host
allowlist keyed by the queue's DOI prefixes. A mislabeled repository host is a
negative case in `test_openalex_official_location_filtering`.

### P2-04: Journal Freshness

The journal header lagged its latest checkpoint and its campaign test count.

Repair evidence: header timestamp and phase now match the latest session, and
`validate_goal_journal_shape` enforces that invariant. The new repair checkpoint
records the current 135-test result.

### P2-05: Goal-Owned Worktree Scope

Pre-existing user changes to `AGENTS.md` and `CLAUDE.md` appeared in the dirty
worktree without explicit G04 isolation.

Repair evidence: the G04 packet declares those two pre-existing user-owned
paths; G04 neither modifies nor reverts them; the validator rejects every other
changed path outside the G04 corpus and owned root Markdown index. This is
covered by `test_preexisting_worktree_isolated`.

## First-Pass Exit Matrix

| Exit criterion | First pass |
|---|---|
| Exact 50 terminal identities | PASS |
| PDF and parse checksum provenance | PASS |
| Exact license token per identity | PASS |
| No nonqueue acquisition | PASS |
| Ignored local corpus | PASS |
| Zero-network byte-identical replay | PASS |
| Tests and corpus validator | PASS |
| Zero unresolved P0/P1/P2 | FAIL |
| Exact G05-eligible subset | PASS |
| Service controls | FAIL |
| Goal-owned Git scope | FAIL |
| No G05 or semantic scope breach | PASS |

## Second-Pass Review

The same independent reviewer re-ran the first-pass probes. P2-02 durable host
stops, P2-03 official publisher-host verification, and P2-04 journal freshness
were `CLOSED`. It retained two findings and added one:

1. `P2-01 OPEN`: checksum-updated traces could change request/final URIs;
   metadata bodies were checksum-linked but not bound to the frozen request and
   exact returned identity set; four-attempt metadata traces were accepted.
2. `P2-05 OPEN`: inherited G03 readable artifacts were incorrectly accepted as
   G04-mutable by the worktree scope validator.
3. `P2-06 NEW`: numeric `Retry-After` worked, but a valid future HTTP-date was
   treated as absent.

The reviewer again confirmed the exact queue, 34/16 terminal split, 37 attempts,
36,477,968 bytes, 590 pages, all local checksums, 107 ignored artifacts,
zero-network replay, 135 passing tests, validator PASS, and no G05 scope breach.
Its second-pass terminal count was P0=0, P1=0, P2=3.

## Second-Pass Repair Evidence

- Paper traces now bind terminal final URI to ledger retrieval URI and acquired
  payload SHA-256 to the ledger source checksum.
- Metadata traces bind to the exact compiled arXiv/OpenAlex request, require the
  exact arXiv/DOI identity set in the response, checksum-link the body, and
  reject more than three attempts.
- `G04_MUTABLE_FILE_PATHS` is now a strict goal-owned subset; inherited G03
  artifacts remain readable but fail the G04 worktree mutation gate.
- `Retry-After` now parses both delay-seconds and RFC HTTP-date forms against
  the recorded deterministic attempt clock.
- RED-GREEN evidence: `test_metadata_trace_identity_binding`, the extended
  `test_requested_failure_trace_integrity`, the extended
  `test_preexisting_worktree_isolated`, and
  `test_retry_after_http_date_honored`.

## Third-Pass Review

The same reviewer independently reran all original exit gates and every
second-pass mutation probe. P2-01, P2-05, and P2-06 were `CLOSED`. The reviewer
reconfirmed the exact 50-paper queue, 34/16 terminal split, 37 attempts,
36,477,968 accepted bytes, 590 pages, 107 ignored artifacts, all source/text/
trace checksums, byte-identical re-extraction, zero-network replay, passing
137-test suite, validator PASS, and no G05 scope breach.

One new finding remained:

### P2-07: Trace Route Consistency

A checksum-updated paper trace could change only its initial request URI while
retaining the ledger-bound terminal URI. A checksum-updated OpenAlex trace
could change its final URI while retaining the exact frozen initial request.
Neither trace proved a contiguous redirect route, and metadata terminal hosts
were not independently authorized.

Third-pass terminal count: P0=0, P1=0, P2=1.

## Third-Pass Repair Evidence

- Every attempt now proves a contiguous HTTPS route from requested URI through
  zero to five recorded 301/302/303/307/308 redirects to its terminal URI.
- Every redirect record binds status, source, target, ordering, and HTTPS; an
  empty chain requires request URI and terminal URI to be identical.
- Paper trace initial requests bind to the exact source URL mechanically
  recovered from the checksum-linked arXiv or OpenAlex metadata body.
- Metadata terminal hosts are constrained to the frozen service endpoint:
  `export.arxiv.org` or `api.openalex.org`.
- Metadata top-level redirects must equal the terminal attempt's redirect
  chain.
- RED-GREEN evidence: `test_request_route_binding`, including one valid
  redirect, an unbound paper request, a broken redirect chain, and an unbound
  OpenAlex terminal host.
- Post-repair proof: 42 of 42 G04 tests and 138 of 138 full G00-G04 tests pass;
  network-disabled replay reports 34 cache hits and zero external requests;
  the full corpus validator returns PASS.

## Fourth-Pass Review

The reviewer closed P2-07 under all four exact probes: arbitrary initial paper
URI, fabricated paper redirect, arbitrary OpenAlex terminal URI with either
empty or fabricated redirects, and the legitimate two-hop Springer redirect
control. It also independently reconfirmed every original G04 exit criterion,
138 passing tests, validator PASS, and zero-network byte-identical replay.

One new finding remained:

### P2-08: Cached Trace Redirect Ceiling

Live transport enforced the frozen maximum of five redirects, but cached paper
and metadata trace validation accepted a checksummed contiguous six-hop route.
The reviewer demonstrated both mutations under network denial with zero
transport calls.

Fourth-pass terminal count: P0=0, P1=0, P2=1.

## Fourth-Pass Repair Evidence

- Cached route validation now rejects every paper or metadata attempt with more
  than `MAXIMUM_REDIRECTS` (five) entries before accepting replay evidence.
- `test_redirect_ceiling_replay` copies the real terminal corpus, injects a
  checksum-updated six-hop paper route, and proves offline replay fails closed
  without network access.
- The same test restores the paper trace, injects matching top-level and
  terminal six-hop OpenAlex redirects, and proves offline replay fails closed.
- The pre-existing route test still proves valid one-hop and live two-hop
  routes remain accepted.

## Fifth-Pass Review

The same independent reviewer reran both P2-08 mutations on temporary corpus
copies under OS-level network denial. A checksum-updated six-hop paper trace
and a six-hop OpenAlex metadata trace both failed with `redirect route exceeds
five-redirect ceiling` and made zero transport calls. Boundary controls with
zero through five redirects passed, and the real Springer `303 -> 302` round
trip remained valid.

The reviewer then independently recomputed every G04 exit criterion:

- exact 25-plus-25 queue and 50 terminal identities;
- 34 acquired and parsed, 15 unavailable, and one authorization failure;
- 37 attempts, 36,477,968 accepted bytes, and 590 pages;
- exact 31-of-31 arXiv and 17-of-17 DOI metadata identities;
- all 107 local artifacts present, checksummed, ignored, and untracked;
- 34-of-34 byte-identical independent text extractions;
- exact 34-paper G05-eligible report subset and zero `READ_COMPLETE` rows;
- byte-identical zero-network replay with 34 cache hits and no socket or
  transport calls;
- 139 of 139 tests passing and corpus validator PASS; and
- no G05, semantic evidence, architecture, experiment, repository, commit, or
  push scope breach.

## Final Clearance

**Unresolved findings: P0=0, P1=0, P2=0.**

G04 is **CLEARED**. The transition to `COMPLETE / VERIFIED / CLEARED` is
truthful. Residual outcomes remain explicit: 34 `LICENSE_UNKNOWN` results, 16
unsuccessful terminal acquisitions, and one disclosed arXiv title variant.

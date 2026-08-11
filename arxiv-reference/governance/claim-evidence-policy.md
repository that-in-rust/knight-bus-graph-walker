# Claim And Evidence Policy

**Status:** G00 campaign contract
**Default:** Paraphrase with precise source pointers

This policy governs research honesty and repository hygiene.
This policy is not legal advice. A human decision maker retains licensing and
full-text publication decisions.
G00 defines this policy but creates no paper, evidence, transfer, architecture,
or experiment records.

## Epistemic Labels

Every substantive statement SHALL use exactly one label at claim granularity:

- `SOURCE_CLAIM`: A statement attributable to, or directly observed in, an
  identified source. It requires a pointer that supports the claim's exact scope.
- `DERIVED_INFERENCE`: A conclusion calculated or reasoned from identified source
  claims or local observations. It SHALL name its premises, derivation,
  assumptions, and material uncertainty. It is not an author claim.
- `SPECULATIVE_TRANSFER`: A proposed mechanism transfer, architecture idea, or
  hypothesis not established by the cited source. It SHALL state the source
  mechanism, changed constraints, analogy failure modes, and a falsifier. It
  SHALL NOT be presented as published or measured evidence.

The current canonical card-level label for every constraint-transfer card is
`SPECULATIVE_TRANSFER`. Sourced or derived subclaims SHALL be isolated at claim
granularity; they do not change the transfer card's canonical label unless a
later authorized SOP change explicitly does so.

Labels and evidence grades are orthogonal: a stronger grade does not turn an
inference or transfer into a source claim. Mixed paragraphs SHALL split claims
when their labels differ.

## Evidence Grades

Each evidence-bearing pattern or claim SHALL carry one of these grades:

| Grade | Meaning | Allowed use |
|---|---|---|
| `A_REPRODUCED` | The relevant result was independently rerun against recorded fixtures, method, environment, and oracle. | May support a measured claim within the reproduced scope. |
| `B_CODE_BACKED` | Inspectable implementation or executable artifact supports the mechanism, but this campaign did not fully reproduce the result. | May support implementation existence and bounded mechanism claims. |
| `C_PAPER_BENCHMARK` | A source reports benchmark evidence with a precise pointer. | May report what the source measured; SHALL NOT imply independent reproduction. |
| `D_THEORETICAL_OR_INCOMPLETE` | Support is analytical, metadata-limited, incomplete, or lacks sufficient implementation/benchmark evidence. | May guide invention while uncertainty remains explicit. |
| `E_CONTRADICTED` | Credible evidence conflicts with the claim or its stated scope. | Retain for conflict analysis; SHALL NOT support an unqualified product claim. |

Grades apply only to the bounded claim and conditions reviewed. Citation count,
venue, recency, or an `A_REPRODUCED` result does not establish universal validity.
Lower-grade evidence may inform invention but SHALL NOT support measured product
or performance claims.

## Source Pointers

Every `SOURCE_CLAIM` SHALL identify the canonical paper or source ID, version, and
the narrowest available locator: page and section, figure, table, theorem,
equation, paragraph, or repository path plus commit and line/symbol. The pointer
SHALL also connect to manifest provenance such as DOI/arXiv ID or source URL,
access date, and checksum when available.

Pointers SHALL support the exact claim, including its limitations and conditions.
A nearby citation, title, search snippet, or unrelated benchmark is insufficient.
Conflicting sources and unavailable passages SHALL remain visible rather than be
silently reconciled.

## Metadata-Only Restrictions

A `METADATA_ONLY` record may support bibliographic identity, discovery history,
query routing, and a decision to read or reject a source. Titles, abstracts,
snippets, citation edges, and service metadata SHALL NOT be used to create a
technical mechanism, limitation, performance, or product `SOURCE_CLAIM`.

Metadata alone SHALL NOT produce an evidence card, raise an evidence grade, or
fill missing full-text details. If full text cannot be lawfully or technically
obtained, preserve the identity and discovery chain, mark it `UNAVAILABLE`, and
leave technical claims unknown.

## Quotations And Paraphrases

Committed evidence SHALL paraphrase by default and retain a precise source
pointer. Quotations SHALL be short, necessary, clearly marked, and no longer than
needed to verify wording that cannot be faithfully paraphrased. Cards SHALL NOT
chain excerpts or reconstruct substantial source text across multiple artifacts.

Paraphrase does not remove the need for attribution, a source pointer, or license
review. Meaning, qualifications, units, and uncertainty SHALL be preserved.

## Numeric-Claim Honesty

Every numeric claim SHALL be exactly one of:

- sourced, with a precise pointer plus the source's units, workload, baseline,
  hardware, statistic, and stated conditions;
- derived, with equation, inputs, assumptions, unknown coefficients, units, and
  uncertainty; or
- reproduced/measured, with fixture, method, tool and version, environment, raw
  result or checksum, and comparison oracle.

Modeled expectations, historical ratios, estimates, and acceptance thresholds
SHALL NOT be described as measurements. Unknown terms SHALL remain unknown rather
than zero. A benchmark SHALL NOT be generalized beyond its workload, and product
or purchasing conclusions SHALL NOT be inferred from a technical milestone alone.

## Licensing State And Full Text

For every acquired full-text source, the committed metadata SHALL include all of
the following:

- the retrieval source URI, using manifest `pdf_url` when it is the direct
  full-text source and the G04 download ledger for any other acquisition source;
- the discovered license URI in `license_uri` when one exists, without
  substituting the retrieval URI or fabricating a license URI when it does not;
- a valid SHA-256 checksum in `sha256` for the acquired bytes;
- the local ignored path, acquisition status, and access timestamp in the
  manifest or G04 download ledger as their schemas provide; and
- exactly one license-state token in manifest `notes`, chosen from
  `LICENSE_PERMISSIVE_VERIFIED`, `LICENSE_RESTRICTED_OR_CONDITIONAL`,
  `LICENSE_UNKNOWN`, or `LICENSE_UNAVAILABLE`.

G04 SHALL freeze the explicit no-license-URI encoding before its first acquired
record. A `LICENSE_*` token is a state, not a URI.

Unknown is distinct from permissive and grants no redistribution permission.
Access to a source does not itself authorize redistribution.

PDFs, source archives, extracted full text, or substantial excerpts SHALL remain
local and ignored by default. They SHALL NOT be staged or committed without
artifact-specific human approval after provenance, license state, repository need,
and redistribution scope are reviewed. Approval SHALL record the approver,
artifact identity and version, checksum, license state, scope, rationale, and UTC
date; it does not authorize other artifacts or later versions.

Without that approval, the repository SHALL contain only permitted metadata,
checksums, source pointers, short quotations, and paraphrased evidence.

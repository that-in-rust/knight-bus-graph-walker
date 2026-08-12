# G05 Mechanism Extraction Contract

This contract freezes G05 serialization, provenance, selection, and lifecycle
rules before semantic reading begins. The goal extracts reusable mechanisms;
it does not combine them into an architecture.

## REQ-G05-SEL-001.0: Select eligible papers deterministically

**WHEN** G05 derives its reading set from G04
**THEN** it SHALL admit only acquired-and-parsed, checksum-verified `DEEP_READ` papers
**AND** SHALL sort by descending integer relevance score, ascending G04 queue rank, then ascending paper ID
**AND** SHALL select exactly the first 25 unique identities
**SHALL** leave the other nine eligible identities `DEEP_READ`.

## REQ-G05-PLAN-001.0: Partition disjoint reading batches

**WHEN** the frozen selection is written to the reading plan
**THEN** selection rank SHALL be contiguous from 1 through 25
**AND** SHALL assign rank round-robin into five named batches
**AND** each batch SHALL contain five unique papers and positions 1 through 5
**SHALL** preserve G04 paths, page counts, SHA-256 values, and architecture-question provenance.

## REQ-G05-READ-001.0: Require complete semantic reading

**WHEN** a selected paper is completed
**THEN** reading coverage SHALL account for every extracted PDF page
**AND** the row SHALL end in `MECHANISM_EXTRACTED` or `NO_MECHANISM`
**AND** a `NO_MECHANISM` row SHALL contain a precise non-empty rationale
**SHALL** forbid manifest `READ_COMPLETE` before terminal extraction and review.

## REQ-G05-CARD-001.0: Serialize one canonical card envelope

**WHEN** G05 emits a mechanism card
**THEN** the file SHALL be named `PAT-<FOUR-WORD-SLUG>.md`
**AND** SHALL contain exactly one fenced `json` object as its canonical payload
**AND** `pattern_id` SHALL equal the filename stem and contain exactly four uppercase slug words after `PAT-`
**AND** SHALL reject unknown top-level and nested fields
**SHALL** contain every field below.

Required top-level fields:

```text
pattern_id
name
epistemic_label
source_paper_ids
source_pointers
source_domain
problem
invariant
mechanism
data_arrangement
access_schedule
resident_state
streamed_state
recomputed_state
resource_model
works_when
fails_when
unknown_when
knight_bus_algorithm_families
a007_consequence
falsifying_test
falsifying_experiment_id
evidence_grade
confidence_rationale
related_pattern_ids
```

The card-level principal class SHALL be `SOURCE_CLAIM`. Source-derived fields
use claim objects with exactly:

```json
{
  "claim_type": "SOURCE_CLAIM",
  "text": "paraphrased claim",
  "source_pointer_ids": ["SP-001"],
  "premises": [],
  "assumptions": [],
  "uncertainty": "bounded uncertainty or NONE"
}
```

`DERIVED_INFERENCE` claim objects require non-empty `premises`, `assumptions`,
and `uncertainty`. `SPECULATIVE_TRANSFER` is forbidden in G05 cards because
transfer work belongs to G07. Mixed prose is forbidden: each claim-bearing
field or list item has its own claim object.

`confidence_rationale` SHALL always be `DERIVED_INFERENCE` because it records
the extractor's evidence appraisal. An absence judgment in `unknown_when`
SHALL also be `DERIVED_INFERENCE` unless a precise cited passage explicitly
states the limitation; complete reading alone does not make the absence an
author claim.

## REQ-G05-PTR-001.0: Preserve precise source pointers

**WHEN** a card contains a source claim
**THEN** each referenced pointer SHALL resolve to the same card's source-paper set
**AND** SHALL include `pointer_id`, `paper_id`, one-based `page`, `locator_type`, `locator_value`, and `claim_scope`
**AND** `locator_type` SHALL be one of `SECTION`, `FIGURE`, `TABLE`, `THEOREM`, `LEMMA`, `ALGORITHM`, `EQUATION`, `APPENDIX`, or `PARAGRAPH`
**AND** the page SHALL not exceed the G04 page count
**SHALL** reject title, abstract, or generic nearby-citation pointers.

An optional `short_quote` is allowed only inside a source-pointer object. It
SHALL contain at most 25 whitespace-delimited words and 200 Unicode code points.
The default is omission and paraphrase. Multiple cards SHALL NOT chain quotes
to reconstruct source text.

## REQ-G05-RES-001.0: Classify every resource term

**WHEN** a card states RAM, I/O, preprocessing, persistent-storage, or temporary-storage consequences
**THEN** each resource term SHALL be an object with exactly `status`, `expression`, `source_pointer_ids`, `premises`, `assumptions`, `uncertainty`, and `measurement_needed`
**AND** `status` SHALL be `SOURCED`, `DERIVED`, or `UNKNOWN`
**AND** a `SOURCED` term SHALL cite at least one source pointer
**AND** a `DERIVED` term SHALL state premises, assumptions, and uncertainty
**AND** an `UNKNOWN` term SHALL use `expression="UNKNOWN"`, empty pointer and premise lists, and a non-empty uncertainty and measurement requirement
**SHALL** never convert an absent coefficient into zero.

This object is G05's machine-readable unknown notation:

```json
{
  "status": "UNKNOWN",
  "expression": "UNKNOWN",
  "source_pointer_ids": [],
  "premises": [],
  "assumptions": [],
  "uncertainty": "the paper does not bound this term",
  "measurement_needed": "measure peak whole-process RSS on the named fixture"
}
```

## REQ-G05-FAL-001.0: Reserve future experiments honestly

**WHEN** a card identifies its smallest falsifying test
**THEN** `falsifying_test` SHALL state `fixture`, `independent_oracle`, `controlled_variables`, `failure_signal`, and `scope`
**AND** SHALL remain a test description rather than a G09 experiment packet
**AND** `falsifying_experiment_id` SHALL equal `RESERVED-G09-FOR-<pattern_id>`
**SHALL** forbid every `EXP-*` identifier and every experiment artifact during G05.

G09 owns resolution: it may replace a reservation with one canonical experiment
ID only after creating and validating the corresponding packet. G10 rejects any
unresolved reservation. The G05 reservation is not a foreign key and does not
claim that an experiment exists.

## REQ-G05-EDGE-001.0: Store typed pattern relationships

`evidence/pattern-edges.tsv` SHALL use exactly:

```text
edge_id\tsource_pattern_id\ttarget_pattern_id\trelationship_type\trationale\tepistemic_label\tsource_paper_ids\tsource_pointer_ids
```

Allowed relationship types are:

- `SHARES_MECHANISM_WITH`
- `COMPLEMENTS`
- `CONTRADICTS`
- `SUBSUMES`

All endpoints SHALL resolve to cards and self-edges are forbidden.
`SHARES_MECHANISM_WITH`, `COMPLEMENTS`, and `CONTRADICTS` are symmetric and
SHALL serialize with lexicographically smaller source ID first. `SUBSUMES` is
directional. Exact duplicates, inverse symmetric duplicates, and contradictory
duplicate types with identical provenance are rejected. Edge
`SOURCE_CLAIM` requires pointers supporting the relationship itself;
`DERIVED_INFERENCE` rationale SHALL name premises, assumptions, and uncertainty.
Each `source_pointer_ids` cell is a pipe-delimited set of qualified pointer
references in the exact form `<pattern_id>#<pointer_id>`. Every qualified
pointer SHALL belong to one of the edge endpoints. The edge's
`source_paper_ids` SHALL exactly equal the source-paper set reached through
those qualified pointers.

`related_pattern_ids` on a card is an untyped navigation cache. At completion it
SHALL exactly equal the sorted unique set of opposite endpoints in valid typed
edges touching that card. The edge ledger remains authoritative.

## REQ-G05-SCOPE-001.0: Prevent later-goal leakage

**WHEN** G05 is active
**THEN** failure-card, constraint-transfer, architecture, and experiment artifacts SHALL remain absent
**AND** no external request or paper acquisition SHALL occur
**AND** ignored G04 PDFs and extracted texts SHALL remain untracked
**SHALL** reject an attempted G06, G07, G08, or G09 artifact.

## Canonical Reading Plan

`governance/g05-reading-plan.tsv` SHALL use exactly:

```text
selection_rank\tbatch_id\tbatch_position\tpaper_id\tg04_queue_rank\trelevance_score\tpage_count\tpdf_path\tpdf_sha256\ttext_path\ttext_sha256\tarchitecture_question_ids\tselection_basis\treader_agent_id\treviewer_agent_id\treading_status\tterminal_outcome\tcard_ids\treading_coverage\tno_mechanism_rationale\tresult_checksum
```

Allowed `reading_status` values are `PENDING`, `READING`, and `COMPLETE`.
Allowed nonterminal `terminal_outcome` is `PENDING`; terminal values are exactly
`MECHANISM_EXTRACTED` and `NO_MECHANISM`. Final rows require named reader and
reviewer agent IDs, `ALL_PAGES:1-<page_count>` coverage, and a SHA-256
`result_checksum` over the canonical row evidence plus the referenced card
payloads. The checksum is uppercase SHA-256 over UTF-8 canonical JSON generated
with sorted object keys, ASCII escaping enabled, and separators `,` and `:`.
The object contains exactly `paper_id`, `reader_agent_id`,
`reviewer_agent_id`, `terminal_outcome`, `card_ids`, `reading_coverage`,
`no_mechanism_rationale`, and `card_payloads`; payloads are sorted by
`pattern_id`. `MECHANISM_EXTRACTED` requires one or more card IDs and
`no_mechanism_rationale=NOT_APPLICABLE`; `NO_MECHANISM` requires
`card_ids=NONE` and a substantive rationale.

## Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-G05-SEL-001.0 | TEST-G05-SEL-001 | integration | Only eligible identities are selected and the set is exactly 25 unique papers | selection |
| REQ-G05-PLAN-001.0 | TEST-G05-PLAN-001 | schema | Ranks, batches, paths, checksums, and AQ provenance match G04 | reading plan |
| REQ-G05-READ-001.0 | TEST-G05-READ-001 | contract | Every selected paper has complete coverage and one terminal outcome before READ_COMPLETE | lifecycle |
| REQ-G05-CARD-001.0 | TEST-G05-CARD-001 | schema | Canonical JSON cards contain exact fields and four-word IDs | cards |
| REQ-G05-PTR-001.0 | TEST-G05-PTR-001 | schema | Every source claim resolves to precise in-range pointers | provenance |
| REQ-G05-RES-001.0 | TEST-G05-RES-001 | unit | Every resource term is sourced, derived, or explicitly unknown | resources |
| REQ-G05-FAL-001.0 | TEST-G05-FAL-001 | contract | Reservations cannot fabricate experiment IDs or packets | falsification |
| REQ-G05-EDGE-001.0 | TEST-G05-EDGE-001 | integration | Typed edges resolve, canonicalize symmetry, and reject duplicates | pattern graph |
| REQ-G05-SCOPE-001.0 | TEST-G05-SCOPE-001 | integration | G06-G09 outputs and external acquisition remain absent | boundary |

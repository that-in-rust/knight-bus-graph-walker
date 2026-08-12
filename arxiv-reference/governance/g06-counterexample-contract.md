# G06 Counterexample Extraction Contract

This contract freezes the G06 schemas, epistemic rules, coverage accounting,
and lifecycle before the first failure card is created.

## REQ-G06-ENTRY-001.0: Preserve the cleared G05 corpus

**WHEN** G06 starts
**THEN** G05 SHALL be `COMPLETE`, `VERIFIED`, and `CLEARED`
**AND** SHALL expose exactly 25 `READ_COMPLETE` papers, 67 mechanism cards, and 47 pattern edges
**AND** every G05 source and result checksum SHALL still validate
**SHALL** reject any new paper identity, download, external request, or source-byte drift.

## REQ-G06-PLAN-001.0: Cover papers and patterns exactly once

**WHEN** G06 writes its adversarial plan
**THEN** it SHALL contain exactly 92 rows: 25 `PAPER` rows and 67 `PATTERN` rows
**AND** each subject ID SHALL occur exactly once in its subject class
**AND** every row SHALL have exactly one primary lane owner
**AND** pattern lane assignment SHALL be deterministic from sorted pattern ID
**SHALL** reject missing, duplicate, ineligible, or foreign subjects.

### Frozen G05 input fingerprints

These lowercase SHA-256 values and data-row counts were computed after the G06
entry suite passed and before any G06 evidence artifact existed:

| Input | Data rows | SHA-256 |
|---|---:|---|
| `sources/paper-manifest.tsv` | 377 | `ac6dd076cf65b3ec8e6addc45b90111cb0ab4f14fe44f71d4c6e1cda4b8f3bfc` |
| `governance/g05-reading-plan.tsv` | 25 | `b8e942272218ecee670b97fdea601c802a2705505bef352b0c644a5d00f53c3f` |
| `evidence/pattern-edges.tsv` | 47 | `df677bdaca319de644d2f89ef6025bebd52ddac16d2c44dbe27fd3619719855e` |
| sorted `mechanism-cards` hash-list aggregate | 67 | `1fb0b8e4e63a09c764cf8b5ff6b4de4c113e8fa843c12c296eed459d5f1a82d9` |
| `sources/metadata-request-ledger.tsv` | 191 | `29ab0c268a7e07931832cc43aff917cacb289058239df443e06f7de44cfa1718` |
| `sources/citation-request-ledger.tsv` | 83 | `da8a5ebaa536c2fc221a85fe48e537a319fcfac8142bacea05181317a9a223d7` |
| `sources/download-ledger.tsv` | 50 | `b5249dbbfed3b272fe01e9b6b4bb18eb41488470e2a69e9e89fa9918b3e2f337` |

G06 SHALL preserve these exact inputs. Local selected PDF and extracted-text
bytes are additionally rebound row by row through the G05 plan checksums; they
are ignored inputs rather than committed outputs.

The exact TSV header is:

```text
subject_type	subject_rank	lane_id	lane_position	subject_id	source_paper_ids	reader_agent_id	reviewer_agent_id	inspection_status	terminal_disposition	failure_ids	evidence_gap	measurement_needed	reading_coverage	result_checksum
```

Allowed `subject_type` values are `PAPER` and `PATTERN`. Allowed lane IDs are
`G06-LANE-1` through `G06-LANE-5`. Allowed `inspection_status` values are
`PENDING`, `INSPECTING`, and `COMPLETE`.

Terminal `PAPER` dispositions are:

- `NEGATIVE_EVIDENCE_EXTRACTED`
- `NO_NEGATIVE_EVIDENCE`

Terminal `PATTERN` dispositions are:

- `SOURCE_FAILURE_LINKED`
- `ANALYTICAL_TEST_LINKED`
- `EXPLICIT_EVIDENCE_GAP`

Before completion, `terminal_disposition`, `reader_agent_id`,
`reviewer_agent_id`, and `result_checksum` use `PENDING`; link and gap fields
may be empty. At completion:

- each `PAPER` row uses `ALL_PAGES:1-<g05_page_count>`;
- each `PATTERN` row uses `PAPER_ROWS:<sorted pipe-delimited source_paper_ids>`
  and every named paper has its own completed full-page row;
- each linked disposition has one or more valid `FAIL-*` IDs and empty gap fields;
- each explicit gap has no failure ID, non-empty `evidence_gap`, and non-empty
  `measurement_needed`;
- each row has a canonical SHA-256 result checksum.

## REQ-G06-CARD-001.0: Serialize one canonical failure envelope

**WHEN** G06 emits a failure card
**THEN** the file SHALL be named `FAIL-<FOUR-WORD-SLUG>.md`
**AND** SHALL contain exactly one fenced `json` object
**AND** `failure_id` SHALL equal the filename stem
**AND** SHALL reject unknown or missing fields
**SHALL** contain exactly these top-level fields:

```text
failure_id
name
epistemic_label
failure_basis
source_paper_ids
source_pointers
broken_assumption
triggering_workload
observable_symptom
breakpoint_equation
affected_pattern_ids
affected_architecture_ids
adversarial_fixture
expected_failure_signal
repair_options
confidence_rationale
```

Allowed `failure_basis` values are:

- `SOURCE_REPORTED`
- `SOURCE_SUPPORTED_DERIVATION`
- `ANALYTICAL_COUNTEREXAMPLE`

`SOURCE_REPORTED` requires card-level `SOURCE_CLAIM` and a precise pointer to
the reported negative result. The two derived bases require card-level
`DERIVED_INFERENCE`. `SPECULATIVE_TRANSFER` is forbidden. All failure cards
require non-empty source-paper and source-pointer sets because an analytical
counterexample must still identify the sourced mechanism premise it attacks.

## REQ-G06-EPI-001.0: Separate source and derived claims

Claim-bearing fields use exactly:

```json
{
  "claim_type": "SOURCE_CLAIM",
  "text": "bounded paraphrase",
  "source_pointer_ids": ["FP-001"],
  "premises": [],
  "assumptions": [],
  "uncertainty": "NONE"
}
```

`SOURCE_CLAIM` requires at least one local pointer and empty premise and
assumption lists. `DERIVED_INFERENCE` requires non-empty premises, assumptions,
and uncertainty. `SPECULATIVE_TRANSFER` is forbidden. A source-supported
derivation cites the source claims used as premises. A purely analytical
counterexample cites the mechanism-defining source premise but states that the
fixture and expected consequence are not measured.

`broken_assumption`, `triggering_workload`, `observable_symptom`,
`expected_failure_signal`, and `confidence_rationale` are claim objects.
`confidence_rationale` SHALL always be `DERIVED_INFERENCE`.

## REQ-G06-PTR-001.0: Preserve exact negative-evidence pointers

Each source pointer uses exactly:

```text
pointer_id
paper_id
page
locator_type
locator_value
claim_scope
```

Pointer IDs are `FP-` followed by three digits and are unique within a card.
The paper SHALL be one of the card's source papers and one of the 25 G06 papers.
Page is one-based and SHALL not exceed the G05 page count. Locator types are
`SECTION`, `FIGURE`, `TABLE`, `THEOREM`, `LEMMA`, `ALGORITHM`,
`EQUATION`, `APPENDIX`, or `PARAGRAPH`. Title, abstract, metadata snippet,
and unbounded nearby citation are invalid. An optional `short_quote` is the
only allowed extra pointer field and is capped at 25 words and 200 Unicode code
points.

## REQ-G06-BREAK-001.0: Keep breakpoint equations honest

`breakpoint_equation` uses exactly:

```text
claim_type
expression
variables
numeric_constants
source_pointer_ids
premises
assumptions
uncertainty
measurement_needed
```

Each `variables` item uses `symbol`, `definition`, and `units`. Each
`numeric_constants` item uses `literal`, `units`, `source_pointer_ids`,
`premises`, `assumptions`, and `uncertainty`.

A wholly unknown breakpoint uses `expression="UNKNOWN"`, an empty
`numeric_constants` list, and non-empty `uncertainty` plus
`measurement_needed`. A symbolic breakpoint may contain variables and
operators without numeric literals. Every numeric literal in an expression
SHALL have exactly one matching `numeric_constants` item. A sourced constant
requires a source pointer; a derived constant requires premises, assumptions,
and uncertainty. G06 SHALL never infer an absent coefficient as zero.

## REQ-G06-FIX-001.0: Define a minimal oracle-bearing fixture

`adversarial_fixture` uses exactly:

```text
claim_type
fixture_name
fixture_kind
graph_shape
graph_scale
workload
controlled_variables
varied_variables
independent_oracle
expected_observation
source_pointer_ids
premises
assumptions
uncertainty
```

Allowed `fixture_kind` values are `GRAPH`, `EXECUTION_PROFILE`, and
`GRAPH_AND_EXECUTION`. The fixture SHALL have non-empty controlled and varied
variables, an independent oracle that does not reuse the mechanism under test,
and an expected observation that can be recorded. `graph_scale` may be
symbolic but may not invent an unsupported threshold. A fixture name is local
prose, not a canonical fixture ID or a G09 experiment. `claim_type` SHALL be
`DERIVED_INFERENCE` with non-empty premises, assumptions, and uncertainty,
because G06 constructs the fixture even when the failure itself is source
reported.

## REQ-G06-LINK-001.0: Resolve affected mechanisms only

`affected_pattern_ids` SHALL be a non-empty sorted unique list of existing G05
pattern IDs. `affected_architecture_ids` SHALL be the empty list because G08
has not created architectures. No G06 artifact may contain an `ARCH-*`,
`XFER-*`, or `EXP-*` identity.

Every failure card SHALL be linked by at least one completed pattern-plan row.
Every linked pattern-plan row SHALL point to a failure card that names that
pattern. Orphan cards and one-way links are invalid.

## REQ-G06-REPAIR-001.0: Offer repair classes without deciding

`repair_options` is a non-empty list of objects with exactly `repair_class`
and `description`. Allowed classes are:

- `ADD_ADMISSION_GUARD`
- `ADD_RESOURCE_BOUND`
- `ADD_FALLBACK_PATH`
- `SPECIALIZE_WORKLOAD`
- `CHANGE_SCHEDULE`
- `CHANGE_REPRESENTATION`
- `MEASURE_UNKNOWN`

Options describe possible response classes only. They SHALL NOT select
`REPAIR`, `SPECIALIZE`, `DEFER`, or `REJECT`, create a candidate, or
assert an architecture decision.

## REQ-G06-DUP-001.0: Merge exact failures without erasing variants

The duplicate signature is the canonical tuple of:

```text
sorted affected_pattern_ids
normalized broken_assumption.text
normalized triggering_workload.text
normalized observable_symptom.text
normalized breakpoint_equation.expression
```

Equal signatures SHALL merge into one card with unioned source provenance.
Different triggering workloads, breakpoint expressions, observable symptoms,
or affected-pattern sets remain separate variants. Different names do not make
equal signatures distinct. Failure IDs and names are not signature inputs.

## REQ-G06-CONFLICT-001.0: Preserve two-sided evidence conflicts

`evidence/evidence-conflicts.tsv` uses exactly:

```text
conflict_id	left_evidence_type	left_evidence_id	right_evidence_type	right_evidence_id	conflict_type	affected_pattern_ids	claim_scope	rationale	epistemic_label	source_paper_ids	source_pointer_ids	resolution_state
```

Conflict IDs are `ECONFLICT-` plus four digits. Evidence types are
`MECHANISM_CARD` and `FAILURE_CARD`; IDs SHALL resolve according to their
type. Both sides are mandatory and SHALL differ. Allowed conflict types are:

- `CONDITION_REVERSAL`
- `BENCHMARK_DISAGREEMENT`
- `ASSUMPTION_MISMATCH`
- `BOUND_CONTRADICTION`
- `APPLICABILITY_DISAGREEMENT`

Allowed resolution states are `OPEN` and `CONDITIONALLY_RECONCILED`.
Qualified pointer references use `<artifact-id>#<pointer-id>`. An empty
conflict ledger is valid only when it retains the exact header and the report
explicitly states that no qualifying two-sided conflict was found.

## REQ-G06-CHK-001.0: Bind terminal rows to canonical evidence

Every completed plan row SHALL carry a SHA-256 over its canonical subject type,
subject ID, source paper IDs, lane and reader/reviewer IDs, terminal disposition,
sorted failure IDs, evidence gap, measurement need, reading coverage, the
referenced source/text checksums, and the canonical bytes of linked failure
cards. Any changed field or linked card SHALL invalidate the checksum.

## REQ-G06-SCOPE-001.0: Prevent later-goal and network leakage

**WHEN** G06 is active
**THEN** G07 transfer cards, G08 architectures/Pareto artifacts, and G09 experiment artifacts SHALL remain absent
**AND** request ledgers SHALL gain no G06 row
**AND** the paper manifest SHALL gain no identity
**AND** ignored PDFs and extracted texts SHALL remain untracked
**SHALL** reject architecture synthesis, implementation, benchmark, or product-performance claims.

## REQ-G06-REV-001.0: Require independent clearance

G06 is complete only after a separate read-only skeptical reviewer verifies all
25 paper dispositions, all 67 pattern dispositions, source pointers, derived
premises, fixtures, duplicate merging, conflict preservation, checksums, and
scope boundaries and reports exactly P0=0, P1=0, and P2=0.

## REQ-G06-REPORT-001.0: Emit an auditable evidence handoff

`sources/G06-counterexample-report.md` SHALL contain exactly these decision
sections before completion:

- `## Executive Result`
- `## Frozen Corpus And Scope`
- `## Negative Evidence Accounting`
- `## Failure Taxonomy`
- `## Pattern Coverage`
- `## Evidence Conflicts`
- `## A007 Decision Yield`
- `## Explicit Unknowns`
- `## Scope Boundary`
- `## Verification Handoff`

The report SHALL state exact paper, page, pattern, failure-card, conflict, and
terminal-disposition counts. It SHALL distinguish source-reported failures,
source-supported derivations, analytical counterexamples, and explicit evidence
gaps. It SHALL not create architecture, transfer, or experiment identities and
SHALL not claim reproduction, implementation, benchmark, RAM reduction, or
latency improvement. An empty conflict ledger is permitted only when the
`Evidence Conflicts` section explicitly records that no qualifying two-sided
conflict was found.

## Requirement-To-Test Matrix

| Requirement | Required test evidence |
|---|---|
| `REQ-G06-ENTRY-001.0` | `test_entry_corpus_remains_frozen`, full corpus validator |
| `REQ-G06-PLAN-001.0` | `test_plan_covers_all_subjects`, `test_missing_disposition_is_rejected` |
| `REQ-G06-CARD-001.0` | `test_failure_envelope_is_canonical` |
| `REQ-G06-EPI-001.0` | `test_derived_failure_stays_unmeasured` |
| `REQ-G06-PTR-001.0` | `test_source_failure_requires_pointer` |
| `REQ-G06-BREAK-001.0` | `test_numeric_breakpoint_requires_support` |
| `REQ-G06-FIX-001.0` | `test_fixture_requires_independent_oracle`, `test_failure_signal_is_observable` |
| `REQ-G06-LINK-001.0` | `test_failure_requires_known_pattern`, `test_failure_requires_affected_pattern`, `test_plan_links_are_bidirectional` |
| `REQ-G06-REPAIR-001.0` | `test_repair_options_avoid_decision` |
| `REQ-G06-DUP-001.0` | `test_duplicate_failures_ignore_names` |
| `REQ-G06-CONFLICT-001.0` | `test_conflict_requires_both_sides` |
| `REQ-G06-CHK-001.0` | `test_terminal_checksum_binds_evidence` |
| `REQ-G06-SCOPE-001.0` | `test_later_goal_artifacts_rejected`, `test_external_or_new_paper_rejected` |
| `REQ-G06-REV-001.0` | `test_completion_requires_clear_review`, independent review |
| `REQ-G06-REPORT-001.0` | `test_counterexample_report_requires_auditable_handoff` |

## TDD Lifecycle

### STUB

Freeze this contract, initialize `journals/G06-progress.md`, and write tests
before the pipeline or any failure card exists.

### RED

Observe the pipeline-availability test fail because
`tools/g06_counterexample_pipeline.py` is absent. Add only the minimal module,
then observe the behavioral tests reject missing fields, unsupported claims,
bad links, duplicate signatures, incomplete dispositions, malformed conflicts,
later-goal leakage, and corpus drift.

### GREEN

Implement the minimum deterministic parsing and validation required by the RED
tests. Only after the contracts are green may five semantic lanes propose
failure dossiers. Integrate validated dossiers into canonical cards and terminal
plan rows.

### REFACTOR

Merge exact duplicates, preserve workload variants, normalize symbols and
vocabulary, tighten pointers, synchronize links, and recompute checksums while
keeping every test green.

### VERIFY

Run the complete G00-G06 suite, full corpus validator, Git/full-text/license
gates, exact count reconciliation, independent review, and final no-change
freeze. Mark G06 complete and recommend G07 without starting it.

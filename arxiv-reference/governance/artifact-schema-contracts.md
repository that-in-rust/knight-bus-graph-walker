# Artifact Schema Contracts

**Status:** G00 schema definition
**Authority:** `arxiv-reference/Arxiv-Pattern-Foundry-SOP.md` version 0.1
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`
**Instance count created by this document:** zero

## 1. Scope And Epistemic Discipline

This document freezes the implementation-facing schema contract required by Goal
`G00`. It defines how later artifacts are shaped and validated; it is not an
architecture-question, query, paper, evidence card, architecture candidate, or
experiment data instance.

The following labels qualify rules in this contract:

- `SOURCE_CLAIM`: a direct normative requirement from the SOP.
- `DERIVED_INFERENCE`: a deterministic validation consequence needed to enforce
  a `SOURCE_CLAIM` without adding research content.
- `SPECULATIVE_TRANSFER`: a later evidence-artifact label. G00 creates no
  speculative transfer and this contract makes no transfer claim.

`SOURCE_CLAIM`, `DERIVED_INFERENCE`, and `SPECULATIVE_TRANSFER` are also the only
allowed epistemic labels in corpus artifacts. A label describes epistemic status,
not evidence quality. Evidence quality is represented separately by
`evidence_grade`.

### 1.1 G00 Definition Boundary

`SOURCE_CLAIM`: G00 owns the schema definition, empty-corpus contract, ignore
policy, validator scaffold, and campaign governance scaffold.

`DERIVED_INFERENCE`: A later-goal path MAY be absent during G00. Its absence does
not waive its schema once its owning goal creates the first data instance.
An existing later-goal directory MAY be empty, and an existing canonical TSV MAY
contain only its exact header. None of those valid empty forms is a later-goal
record.

G00 SHALL NOT create any of the following:

- architecture-question sections;
- taxonomy, query, paper, citation, download, conflict, or Pareto rows;
- mechanism, failure, or constraint-transfer cards;
- architecture genomes or candidates;
- experiment packets;
- aliases, fixture records, or relationship-edge records.

The examples below are schema exemplars copied from the SOP's shapes. They SHALL
NOT be interpreted as corpus records, reserved identities, or completed research.

## 2. Stable Identifier Contract

`SOURCE_CLAIM`: Identifiers remain stable after publication. A display-name
change SHALL NOT change an identifier. A merge SHALL retain aliases that resolve
to the canonical identifier.

| Artifact | Required format | Example |
| --- | --- | --- |
| Architecture question | `AQ-<FAMILY>-<NNN>` | `AQ-FRONTIER-001` |
| Query family | `QRY-<QUESTION>-<NNN>` | `QRY-FRONTIER-001-003` |
| Canonical paper | `PAPER-<ARXIV_OR_HASH>` | `PAPER-1905.04264` |
| Mechanism card | `PAT-<FOUR-WORD-SLUG>` | `PAT-SELECT-ACTIVE-PARTITIONS-ONLY` |
| Failure card | `FAIL-<FOUR-WORD-SLUG>` | `FAIL-SPARSE-FRONTIER-TURNS-DENSE` |
| Constraint transfer | `XFER-<FOUR-WORD-SLUG>` | `XFER-BOUND-ACTIVE-WORKING-SET` |
| Architecture candidate | `ARCH-<FAMILY>-<NNN>` | `ARCH-FRONTIER-003` |
| Experiment | `EXP-<ARCH-ID>-<NNN>` | `EXP-ARCH-FRONTIER-003-001` |

`DERIVED_INFERENCE`: Validators SHALL enforce the literal prefixes, segment
placement, and exactly three decimal digits for each `<NNN>`. A four-word slug
SHALL contain exactly four non-empty hyphen-delimited words. Identifier matching
is case-sensitive.

The SOP does not freeze:

- the complete character grammar for `<FAMILY>`, `<QUESTION>`, or slug words;
- the hash algorithm or hash length for `PAPER-<ARXIV_OR_HASH>`;
- canonical encoding of legacy arXiv identifiers;
- alias storage fields or an alias-ledger header.

G02 SHALL freeze paper hash, legacy-identity, and paper-alias encoding before its
first manifest row. G05 SHALL freeze mechanism and failure alias encoding before
its first merge. G08 SHALL freeze architecture alias encoding before its first
candidate merge. G00 SHALL NOT silently invent those encodings.

Within each artifact type, a primary ID SHALL be unique. A reference SHALL use a
canonical ID or a declared alias; display names SHALL never act as foreign keys.

## 3. Exact TSV Headers

`SOURCE_CLAIM`: The following four headers are byte-order definitions after line
ending removal. Columns SHALL appear once, in this order, separated by one tab.
No column may be renamed, omitted, inserted, or reordered.

### 3.1 Keyword Taxonomy

Path: `arxiv-reference/governance/keyword-taxonomy.tsv`

```text
term_id	term	term_type	architecture_question_ids	source_repo_paths	synonyms	historical_terms	adjacent_domain_terms	exclusion_terms	notes
```

First data owner: G01.

### 3.2 Query Ledger

Path: `arxiv-reference/governance/query-ledger.tsv`

```text
query_id	architecture_question_ids	source_term_ids	service	query_text	categories	date_from	date_to	exclusions	executed_at	result_count	response_checksum	status
```

First data owner: G01 for planned query families and G02 for executed query
results.

### 3.3 Paper Manifest

Path: `arxiv-reference/sources/paper-manifest.tsv`

```text
paper_id	arxiv_id	doi	title	authors	published_date	updated_date	categories	abstract_url	pdf_url	license_uri	canonical_version	discovery_query_ids	architecture_question_ids	relevance_score	score_breakdown	selection_status	evidence_grade	code_urls	local_path	sha256	notes
```

First data owner: G02.

### 3.4 Citation Edge Ledger

Path: `arxiv-reference/sources/citation-edges.tsv`

```text
source_paper_id	target_paper_id	edge_type	discovery_source	relevance_reason	verified_at
```

First data owner: G03.

### 3.5 Metadata Request Ledger

Path: `arxiv-reference/sources/metadata-request-ledger.tsv`

```text
request_id	goal_id	query_id	variant_id	service	operation	normalized_query	parameters	requested_at_utc	page_cursor	response_status	result_count	response_checksum	client_version	cache_status	attempt	retry_events	rate_limit_events	policy_url	policy_checked_date	cache_path	terminal_state
```

First schema and data owner: G02. The exact encoding, caps, retry states, cache
boundary, and aggregation rules are frozen in
`arxiv-reference/governance/g02-metadata-contract.md` before the first request.

### 3.6 Citation Request Ledger

Path: `arxiv-reference/sources/citation-request-ledger.tsv`

```text
request_id	goal_id	seed_paper_id	traversal_paper_id	depth	direction	service	operation	normalized_identifier	parameters	requested_at_utc	page_cursor	response_status	result_count	response_checksum	cache_checksum	client_version	cache_status	attempt	retry_events	rate_limit_events	policy_url	policy_checked_date	cache_path	terminal_state
```

First schema and data owner: G03. The exact encoding, traversal caps, retry
states, raw/cache checksum boundary, and provider metadata allowlists are frozen in
`arxiv-reference/governance/g03-citation-contract.md` before the first request.
G03 terminal states include `COMPLETE`, `EMPTY`, `UNAVAILABLE`, `RATE_LIMITED`,
`PAYLOAD_REJECTED`, and `FAILED`; a rejected HTTP-success payload is represented
only by a checksummed, content-free rejection marker.

### 3.7 Citation Stop Ledger

Path: `arxiv-reference/sources/citation-stops.tsv`

```text
stop_id	candidate_identity	seed_paper_id	parent_paper_id	depth	direction	decision_score	score_breakdown	architecture_question_ids	provider_name	provider_id	reason
```

First schema and data owner: G03. Every stopped provider observation or
provider-only sampled identity has one stable content-derived `STOP-G03-*` ID.
The row preserves enough information to reconcile branch, depth, direction,
score, provider, and stop reason without reading an ignored cache.

### 3.8 Citation Screening Ledger

Path: `arxiv-reference/sources/citation-screening-ledger.tsv`

```text
candidate_paper_id	primary_lane	direction	disposition	queue_rank	rationale	reviewer_model	reviewer_agent_id	prompt_id	screened_at_utc	evidence_scope	result_checksum	audit_lane_id	audit_reviewer_agent_id	audit_result_checksum
```

First schema and data owner: G03. It covers every retained ancestry identity,
including rediscovered baseline rows, exactly once; assigns one deterministic
primary lane; preserves reviewer and prompt provenance; checksums normalized
lane results; and derives the exact 25-new-identity ancestry half of the G04
queue from contiguous `ACQUIRE` ranks. Baseline rediscoveries may be screened
but SHALL NOT receive `ACQUIRE`.

### 3.9 Download And Parse Ledger

Path: `arxiv-reference/sources/download-ledger.tsv`

```text
request_id	goal_id	queue_rank	paper_id	source_service	retrieval_uri	accessed_at_utc	response_status	media_type	content_length_bytes	source_checksum	local_path	license_uri	license_state	acquisition_status	attempt_count	retry_events	rate_limit_events	policy_url	policy_checked_date	cache_status	trace_path	trace_checksum	parser_name	parser_version	parser_options	page_count	extracted_path	extracted_checksum	parse_status	terminal_reason
```

First schema and data owner: G04. The ledger contains exactly one terminal row
per selected G04 identity. Per-attempt and redirect details live in the ignored,
checksummed `trace_path`; the committed row retains their counts and checksum.
The frozen controlled values, sentinels, path grammar, checksum boundary, parser
contract, service limits, and terminal-state rules are defined in
`governance/g04-acquisition-contract.md` before the first external request.

### 3.10 G05 Reading Plan

Path: `arxiv-reference/governance/g05-reading-plan.tsv`

```text
selection_rank	batch_id	batch_position	paper_id	g04_queue_rank	relevance_score	page_count	pdf_path	pdf_sha256	text_path	text_sha256	architecture_question_ids	selection_basis	reader_agent_id	reviewer_agent_id	reading_status	terminal_outcome	card_ids	reading_coverage	no_mechanism_rationale	result_checksum
```

First schema and data owner: G05. Exactly 25 rows preserve deterministic G04
provenance and split into five disjoint five-paper batches. Terminal rows use
exactly `MECHANISM_EXTRACTED` or `NO_MECHANISM`; the latter requires complete
all-page coverage and a substantive rationale.

### 3.11 Pattern Relationship Edges

Path: `arxiv-reference/evidence/pattern-edges.tsv`

```text
edge_id	source_pattern_id	target_pattern_id	relationship_type	rationale	epistemic_label	source_paper_ids	source_pointer_ids
```

First schema and data owner: G05. The closed relationship enum is
`SHARES_MECHANISM_WITH`, `COMPLEMENTS`, `CONTRADICTS`, and `SUBSUMES`.
The first three are symmetric and serialize in ascending endpoint order;
`SUBSUMES` is directional. Exact and inverse symmetric duplicates are invalid.

### 3.12 TSV Details Not Frozen By G00

`DERIVED_INFERENCE`: Exact headers do not define multi-value separators, escaping,
null sentinels, timestamp formats, date formats, score serialization, or whether
free-text cells may contain tabs or newlines. The first goal that writes a data
row SHALL freeze and document those encodings before writing that row:

- G01 for keyword-taxonomy and planned-query cells;
- G02 for executed-query and paper-manifest cells;
- G03 for citation-edge cells.

A goal SHALL NOT establish a canonical encoding merely by emitting its first row.

The headers do freeze these row-level invariants:

- A taxonomy row links at least one architecture question and repository source
  path and classifies the term with `term_type`.
- A query row links at least one architecture question and its source terms. An
  `EXECUTED` row records service, compound query text, categories, date range,
  exclusions, execution timestamp, result count, and response checksum. A
  `PLANNED` row is a complete plan record, not a draft record.
- A paper row has one canonical `paper_id`; identity deduplication considers
  arXiv ID, DOI, normalized title, and known version relationships. Missing
  bibliographic or license values require an explicit encoding frozen by G02 or
  G04 rather than an empty completed cell.
- An acquired full-text row has a retrieval source URI, a valid SHA-256 checksum,
  and exactly one explicit `LICENSE_*` state in `notes`. `pdf_url` records the
  direct full-text source URI when it is the retrieval source; the G04 download
  ledger records any other retrieval URI, access timestamp, and acquisition
  status. `license_uri` records the discovered license URI when one exists. G04
  SHALL freeze the no-license-URI encoding and SHALL NOT substitute a retrieval
  URI or fabricate a license URI. Full text requires artifact-specific human
  approval before it is staged or committed.
- A citation row links two canonical paper IDs and records edge type, discovery
  source, relevance reason, and cache-verification timestamp. Provider-response
  timestamps remain in the request ledger.

The SOP does not define `term_id` syntax. G01 SHALL freeze it before the first
taxonomy row.

## 4. Controlled Values

`SOURCE_CLAIM`: Values in this table are closed enums wherever the SOP supplies
an exact list.

| Field or decision | Allowed values |
| --- | --- |
| `epistemic_label` | `SOURCE_CLAIM`, `DERIVED_INFERENCE`, `SPECULATIVE_TRANSFER` |
| Architecture-question `status` | `OPEN`, `EVIDENCE_COLLECTING`, `EXPERIMENT_READY`, `DECIDED`, `REJECTED` |
| Taxonomy `term_type` | `ALGORITHM`, `LAYOUT`, `STATE`, `SCHEDULING`, `IO`, `PREDICTABILITY`, `CORRECTNESS`, `HARDWARE`, `PRODUCT_CONTRACT` |
| Query `status` | `PLANNED`, `EXECUTED`, `RATE_LIMITED`, `FAILED`, `SUPERSEDED` |
| Paper `selection_status` | `METADATA_ONLY`, `DEEP_READ`, `READ_COMPLETE`, `REJECTED`, `UNAVAILABLE` |
| Citation `edge_type` | `CITES`, `IMPLEMENTS`, `EVALUATES`, `REFINES`, `CONTRADICTS`, `SURVEYS` |
| `evidence_grade` | `A_REPRODUCED`, `B_CODE_BACKED`, `C_PAPER_BENCHMARK`, `D_THEORETICAL_OR_INCOMPLETE`, `E_CONTRADICTED` |
| Invalidated-candidate disposition | `REPAIR`, `SPECIALIZE`, `DEFER`, `REJECT` |
| Generic draft marker | `DRAFT` |

The SOP fixes `SCHEMA_ONLY` as the initial value of
`highest_evaluator_stage`, and orders later stages as schema validation,
symbolic resource review, counterexample review, microbenchmark review, and
workload benchmark review. It does not freeze serialized enum tokens for the
four later stages. G08 SHALL freeze those tokens before the first promotion
beyond `SCHEMA_ONLY`.

The SOP names Pareto-niche examples and fallback actions, but does not declare
closed enums for either. G08 SHALL freeze candidate-niche and fallback-action
serialization before first use. G06/G08 SHALL freeze where invalidated-candidate
disposition and rationale are stored; G00 does not add an unapproved field or
ledger column.

The SOP also uses the controlled sentinels `NO_DECISION_IMPACT` and
`NO_MECHANISM` in goal outcomes. They are not epistemic labels or artifact
statuses.

`DERIVED_INFERENCE`: Until G04 adds a dedicated license-state field, an acquired
manifest row's `notes` contains exactly one of `LICENSE_PERMISSIVE_VERIFIED`,
`LICENSE_RESTRICTED_OR_CONDITIONAL`, `LICENSE_UNKNOWN`, or
`LICENSE_UNAVAILABLE`. These tokens describe license state; they are not license
URIs and do not grant redistribution permission.

## 5. Required Logical Schemas

All schemas in this section are logical mappings. The SOP does not freeze file
extensions, YAML front-matter delimiters, Markdown fence placement, or a
multi-document envelope for card files. The first owning goal SHALL freeze a
parseable envelope before creating its first instance. G00 SHALL NOT infer an
envelope from the illustrative YAML blocks.

"Required" means that the key SHALL exist. Completion rules in Section 6 decide
whether an empty value is valid.

### 5.1 Architecture Question

Path family: `arxiv-reference/governance/architecture-question-ledger.md`
First instance owner: G01.

| Field | Type | Completion contract |
| --- | --- | --- |
| `question_id` | architecture-question ID | Unique and stable. |
| `decision` | string | Names the architecture decision, not a general research topic. |
| `product_consequence` | string | States the A007 consequence reduced by the decision. |
| `candidate_options` | list of strings | Non-empty and materially distinct. |
| `known_evidence` | list of IDs or source pointers | May be empty while evidence is not yet known. |
| `missing_evidence` | list of strings or IDs | Along with `known_evidence`, names the evidence required by the decision. |
| `falsifier` | string | States what would invalidate an option or premise. |
| `status` | architecture-question status enum | Uses only the lifecycle enum in Section 4. |
| `owner_goal` | goal ID | Names the bounded goal responsible for the question. |

`SOURCE_CLAIM`: Every completed architecture question SHALL name the decision,
alternatives, required evidence, and falsifier. It remains open when evidence is
missing; a validator SHALL NOT force a premature conclusion.

### 5.2 Mechanism Card

Path family: `arxiv-reference/evidence/mechanism-cards/`
First instance owner: G05.

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

Required `resource_model` fields:

```text
ram
io
preprocessing
persistent_storage
temporary_storage
```

Completion requirements:

- `pattern_id` is a unique mechanism-card ID; `name` is non-empty.
- `epistemic_label` uses the three-value enum.
- A `SOURCE_CLAIM` card has at least one resolvable `source_paper_ids` entry and
  at least one precise `source_pointers` entry.
- `source_domain`, `problem`, `invariant`, `mechanism`, `data_arrangement`,
  `access_schedule`, `resident_state`, `streamed_state`, and `recomputed_state`
  are non-empty.
- Every resource-model term is non-empty and is sourced, symbolically derived
  with assumptions, measured, or explicitly marked unknown. G05 SHALL freeze
  machine-readable unknown notation before the first card.
- `works_when`, `fails_when`, and `unknown_when` are present and state the three
  applicability boundaries required by REQ-PAT-004.0.
- `knight_bus_algorithm_families` maps applicability without asserting
  universal fit.
- `a007_consequence`, `falsifying_experiment_id`, `evidence_grade`, and
  `confidence_rationale` are non-empty.
- `confidence_rationale` is always a `DERIVED_INFERENCE`: it is the extractor's
  evidence appraisal, not a claim made by the paper. It SHALL name source-backed
  premises, campaign assumptions, and residual uncertainty.
- An `unknown_when` absence judgment such as "the paper does not establish X"
  is a `DERIVED_INFERENCE` unless the cited source explicitly states that
  limitation. Complete reading does not turn absence of evidence into an author
  claim.
- `falsifying_test` states the smallest fixture, independent oracle, controlled
  variables, expected failure signal, and scope. It is not a G09 experiment.
- During G05, `falsifying_experiment_id` uses
  `RESERVED-G09-FOR-<pattern_id>`. It is a reservation, not a foreign key or a
  claim that an experiment exists. G09 owns resolution and G10 rejects an
  unresolved reservation.
- `related_pattern_ids` contains only mechanism-card IDs. Typed relationship
  semantics are required by REQ-PAT-005.0 but are not represented by this field;
  G05 SHALL freeze the edge schema and relationship enum before first use.

### 5.3 Failure Card

Path family: `arxiv-reference/evidence/failure-cards/`
First instance owner: G06.

Required fields:

```text
failure_id
name
epistemic_label
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

Completion requirements:

- `failure_id` is a unique failure-card ID; `name` is non-empty.
- `epistemic_label` uses the three-value enum.
- A source-grounded failure has non-empty `source_paper_ids` and precise
  `source_pointers`. An analytical counterexample SHALL be labeled
  `DERIVED_INFERENCE`, not described as measured failure.
- `broken_assumption`, `triggering_workload`, `observable_symptom`,
  `breakpoint_equation`, `adversarial_fixture`, `expected_failure_signal`, and
  `confidence_rationale` are non-empty.
- `affected_pattern_ids` contains only mechanism-card IDs.
- `affected_architecture_ids` contains only architecture IDs and may be empty
  before G08 creates candidates.
- `repair_options` is non-empty and does not itself select a candidate
  disposition.

The SOP does not define fixture IDs or a fixture registry. G06 SHALL freeze the
adversarial-fixture representation before the first failure card. A fixture name
embedded in prose SHALL NOT silently become a canonical foreign key.

### 5.4 Constraint-Transfer Card

Path family: `arxiv-reference/evidence/constraint-transfer-cards/`
First instance owner: G07.

Required top-level fields:

```text
transfer_id
name
epistemic_label
source_pattern_ids
original_domain
original_constraints
original_cost_model
surviving_invariant
reversed_assumptions
modern_knight_bus_constraints
proposed_transfer
modern_resource_model
analogy_failure_modes
target_algorithm_families
falsifying_experiment_id
```

Required `modern_resource_model` fields:

```text
ram
io
preprocessing
storage
concurrency
```

Completion requirements:

- `transfer_id` is a unique constraint-transfer ID; `name` is non-empty.
- The current canonical card-level `epistemic_label` is
  `SPECULATIVE_TRANSFER`. A narrower artifact may isolate a sourced or derived
  claim, but the transfer card label does not change unless a later authorized
  SOP change explicitly changes the canonical label.
- `source_pattern_ids` is non-empty and resolves to mechanism cards.
- `original_domain`, `original_cost_model`, `surviving_invariant`,
  `proposed_transfer`, and `falsifying_experiment_id` are non-empty.
- `original_constraints`, `reversed_assumptions`,
  `modern_knight_bus_constraints`, `analogy_failure_modes`, and
  `target_algorithm_families` explicitly state their required content.
- Every modern resource term is non-empty, symbolic, and names constants that
  require measurement; historical benchmark ratios SHALL NOT be imported.
- A card without a surviving invariant is invalid rather than an incomplete
  analogy.

REQ-TIME-001.0 additionally requires constrained resource, access medium,
predictability requirement, data mutability, communication model, and original
hardware or operating assumptions when known. The SOP does not allocate exact
keys for those values. G07 SHALL freeze their placement before the first card;
G00 does not add silent keys.

### 5.5 Architecture Candidate

Path family: `arxiv-reference/synthesis/architecture-candidates/`
First instance owner: G08.

Required top-level fields:

```text
architecture_id
name
epistemic_label
architecture_question_ids
parent_architecture_ids
mechanism_pattern_ids
failure_card_ids
constraint_transfer_ids
workload_contract
genome
resource_model
preparation_model
storage_amplification
correctness_contract
determinism_contract
failure_boundaries
fallback_response
pareto_niches
highest_evaluator_stage
falsifying_experiment_id
```

Required `workload_contract` fields:

```text
artifact
algorithm_family
exactness
ram_ceiling_bytes
storage_allowance_bytes
deadline_model
output_bound
```

Required `genome` fields:

```text
topology_layout
ordering
state_placement
scheduling
overflow_behavior
admission_model
receipt_model
compatibility_boundary
```

Required `resource_model` fields:

```text
topology
algorithm_state
frontier_or_active_set
scratch
output
conversion
page_cache_or_direct_io
runtime_overhead
spill
safety_margin
```

Completion requirements:

- `architecture_id` is unique and stable; `name` is non-empty.
- `epistemic_label` uses the three-value enum. Modeled or generated behavior
  SHALL NOT be represented as a `SOURCE_CLAIM` or as a measured result.
- `architecture_question_ids` and `mechanism_pattern_ids` are non-empty and
  resolvable. Parent, failure, and transfer IDs use their corresponding ID types.
- A root candidate may have empty `parent_architecture_ids`; a child records its
  parent candidates and mechanism delta.
- Every workload-contract, genome, and resource-model key is present and
  non-empty. Unknown resource terms are explicit rather than zero.
- `workload_contract.exactness` is the canonical exactness value in the current
  G00 logical schema; G00 does not duplicate it under `genome`.
- `preparation_model` covers build phases, build memory, build I/O, temporary
  bytes, freshness, amortization, and the shared-layout baseline when a
  specialized artifact is used.
- `storage_amplification`, `correctness_contract`, and `determinism_contract`
  are non-empty and satisfy REQ-ARCH-003.0 and REQ-ARCH-004.0.
- `failure_boundaries` is non-empty, relevant `failure_card_ids` are linked,
  and `fallback_response` states switch-plan, spill, approximate, or refusal
  behavior as applicable.
- `pareto_niches` is non-empty once the candidate enters the arena.
- `highest_evaluator_stage` begins at `SCHEMA_ONLY` and cannot skip applicable
  evaluator stages.
- `falsifying_experiment_id` is non-empty and has experiment-ID syntax.

The SOP requires an A007 consequence, mutation additions and removals, intended
changed behavior, composability rationale, double-counting risks, detailed
preparation terms, oracle strategy, and refusal semantics, but does not allocate
exact keys for every item. G08 SHALL freeze their concrete representation before
the first completed candidate. G00 SHALL NOT silently extend the logical mapping.

### 5.6 Experiment Packet

Container path: `arxiv-reference/synthesis/experiment-backlog.md`
First instance owner: G09.

Required fields:

```text
experiment_id
architecture_id
hypothesis
fixture_ids
holdout_fixture_ids
baseline
independent_oracle
controlled_variables
measured_metrics
acceptance_thresholds
disconfirming_result
modeled_expectation
required_implementation_scope
```

Completion requirements:

- `experiment_id` is unique and embeds the same `architecture_id` referenced by
  the packet.
- `architecture_id` resolves to one architecture candidate.
- `hypothesis`, `baseline`, `independent_oracle`, `disconfirming_result`,
  `modeled_expectation`, and `required_implementation_scope` are non-empty.
- `fixture_ids`, `holdout_fixture_ids`, `controlled_variables`,
  `measured_metrics`, and `acceptance_thresholds` are non-empty for a packet
  admitted to the decision atlas.
- `modeled_expectation` is a `DERIVED_INFERENCE`, not a measurement.
  `acceptance_thresholds` are decision criteria, not evidence that the threshold
  has been met.
- A packet is a complete experiment specification, not a claim that an experiment
  has run.

The SOP does not define fixture-ID syntax, fixture-registry location, experiment
result fields, or a packet envelope inside the Markdown backlog. G09 SHALL freeze
those details before the first packet. G00 SHALL NOT manufacture them.

## 6. Completed And DRAFT Artifacts

`SOURCE_CLAIM`: No required field may be blank in a completed artifact. A draft
artifact SHALL declare `status: DRAFT` and SHALL NOT enter retrieval or the
architecture decision atlas.

`DERIVED_INFERENCE`: Apply that rule as follows:

1. Every schema key remains required in both drafts and completed artifacts.
2. `status: DRAFT` permits incomplete values only while the artifact remains
   outside canonical retrieval, ranking, Pareto, and decision-atlas inputs.
3. An artifact not marked `DRAFT` is validated as completed. The SOP defines no
   generic `COMPLETE` value, so G00 SHALL NOT invent one.
4. A completed required string is not empty or whitespace-only.
5. A completed required mapping contains every required child key.
6. Every required list key SHALL be present. Presence alone does not require
   content: a list SHALL be non-empty only when the SOP or this contract states
   explicit non-empty semantics for that list. Examples allowed to remain empty
   include `known_evidence`, an unlinked `related_pattern_ids` list, root
   `parent_architecture_ids`, a conventional candidate's
   `constraint_transfer_ids`, or pre-G08 `affected_architecture_ids`. Conversely,
   empty-list presence does not satisfy an explicit rule requiring stated
   conditions, ancestry, provenance, boundaries, metrics, or thresholds.
7. Empty values SHALL NOT be replaced by undocumented sentinels. The owning goal
   must freeze any `UNKNOWN`, `NONE`, or `NOT_APPLICABLE` encoding before use.
8. A completed artifact SHALL NOT link to a `DRAFT` artifact as evidence for
   retrieval, promotion, or a decision-atlas claim.

Architecture-question and query `status` fields are lifecycle fields whose
allowed enums do not include `DRAFT`. G00 SHALL NOT overload them. `PLANNED` is
not a synonym for `DRAFT`.

The exact TSV headers also leave no generic draft column. G01-G03 SHALL either
write complete canonical rows for their current lifecycle state or keep
incomplete work outside the canonical TSV until the owning goal freezes an
out-of-band draft representation. No goal may append a `status` column, put
`DRAFT` into a closed lifecycle enum, or treat an undocumented side file as
canonical.

G01 must likewise keep incomplete architecture-question sections outside the
canonical ledger or freeze a distinct draft-marker representation before the
first incomplete section. `DRAFT` is not a valid architecture-question lifecycle
status.

The logical card schemas are minimum schemas, so the generic draft marker is a
permitted metadata extension. Other unknown keys SHALL NOT silently become
canonical. Validators SHOULD report them until an owning goal explicitly extends
the contract.

## 7. Empty-Corpus Semantics

`SOURCE_CLAIM`: G00 succeeds when an empty valid evidence corpus passes
deterministic contract validation.

An empty valid corpus has:

- zero architecture questions;
- zero keyword and query rows;
- zero paper-manifest and citation-edge rows;
- zero downloads and zero local papers;
- zero mechanism, failure, and transfer cards;
- zero architecture genomes and candidates;
- zero Pareto rows and zero experiment packets.

It still contains the G00 governance schema and validator scaffold. Empty corpus
does not mean missing G00 contracts.

`DERIVED_INFERENCE`: During G00, a later-goal file or directory MAY be absent. If
one of the four canonical TSV files exists, it SHALL contain its exact header;
the header followed by zero data rows is valid. An empty card directory is valid.
No placeholder row, fake card, reserved candidate, or blank experiment packet is
required to prove emptiness.

Cross-link validation is vacuously satisfied when there are no records. It SHALL
activate as soon as a record supplies a foreign key.

## 8. Cross-Link And Claim Rules

### 8.1 Foreign-Key Map

| Source field | Target type |
| --- | --- |
| taxonomy `architecture_question_ids` | architecture question |
| query `architecture_question_ids` | architecture question |
| query `source_term_ids` | taxonomy term |
| manifest `discovery_query_ids` | query family |
| manifest `architecture_question_ids` | architecture question |
| citation `source_paper_id`, `target_paper_id` | canonical paper |
| mechanism `source_paper_ids` | canonical paper |
| mechanism `related_pattern_ids` | mechanism card |
| mechanism `falsifying_experiment_id` | experiment packet |
| failure `source_paper_ids` | canonical paper |
| failure `affected_pattern_ids` | mechanism card |
| failure `affected_architecture_ids` | architecture candidate |
| transfer `source_pattern_ids` | mechanism card |
| transfer `falsifying_experiment_id` | experiment packet |
| candidate `architecture_question_ids` | architecture question |
| candidate `parent_architecture_ids` | architecture candidate |
| candidate `mechanism_pattern_ids` | mechanism card |
| candidate `failure_card_ids` | failure card |
| candidate `constraint_transfer_ids` | constraint-transfer card |
| candidate `falsifying_experiment_id` | experiment packet |
| experiment `architecture_id` | architecture candidate |
| experiment `fixture_ids`, `holdout_fixture_ids` | fixture registry not yet frozen |

All populated IDs SHALL match the target type's identifier format. Final campaign
closure at G10 requires every cross-link and alias to resolve to exactly one
canonical record.

The goal order creates legitimate forward references: G05 and G07 name
falsifying experiment IDs before G09 creates packets, and G06 may precede G08
architecture IDs. G00 does not silently treat all dangling links as valid. G05,
G06, and G07 SHALL freeze a reserved-forward-reference lifecycle before first
use; G09 SHALL resolve experiment references before decision-atlas admission;
G10 SHALL reject all unresolved references.

### 8.2 Epistemic Enforcement

- Every claim-bearing artifact uses only the three allowed labels.
- Every `SOURCE_CLAIM` has a source identifier and a precise section, page,
  figure, theorem, or repository pointer.
- `METADATA_ONLY` and `UNAVAILABLE` paper records SHALL NOT support a
  `SOURCE_CLAIM`. A completed paper-backed card normally references a
  `READ_COMPLETE` paper.
- A derived resource equation or A007 consequence is a `DERIVED_INFERENCE` and
  states assumptions and unknown coefficients.
- A generated architecture uses the epistemic label appropriate to its atomic
  claim. The current canonical card-level label for a transplanted mechanism is
  `SPECULATIVE_TRANSFER`; only a later authorized SOP change may replace that
  transfer-card label.
- `SPECULATIVE_TRANSFER` SHALL NOT be presented as published evidence.
- Numeric statements are sourced, derived with assumptions, or measured. A
  modeled expectation SHALL NOT be described as a measured improvement.
- Lower evidence grades remain eligible for invention, but SHALL NOT support
  measured product claims.
- Contradictory evidence and rejected candidates remain queryable; validation
  SHALL NOT erase them to make a corpus appear consistent.

The top-level card label cannot faithfully encode mixed claim classes by itself.
Until an owning goal freezes claim-level syntax, cards SHALL remain atomic and
use one principal epistemic class; contrary embedded claims must be separated or
explicitly labeled in content. G05, G06, G07, and G08 SHALL freeze mixed-claim
encoding before admitting a mixed artifact.

## 9. Validator Behavior

### 9.1 G00 Baseline

`SOURCE_CLAIM`: The G00 validator exposes these public functions:

```text
validate_source_query_terms
deduplicate_paper_manifest_entries
validate_mechanism_card_fields
validate_failure_card_fields
validate_transfer_card_invariants
score_architecture_candidate_niches
verify_download_license_policy
audit_requirement_test_links
```

The niche-scoring entry point exists to preserve the SOP-mandated public API and
may report deterministic niche coverage. In G00 it SHALL NOT rank candidate
quality, choose winners, define a G08 promotion policy, or create any candidate
record. G08 owns niche serialization, evaluator stages, comparative policy, and
promotion decisions.

For an explicit `--root`, baseline validation SHALL:

1. run locally without internet access, downloads, or source-service calls;
2. require this schema contract and other G00-required policy files;
3. accept the empty-corpus state in Section 7;
4. validate any present canonical TSV header exactly;
5. reject exact duplicate IDs within an artifact type and exact duplicate rows
   or records, including a duplicate `paper_id`;
6. audit all 49 SOP requirement IDs against 49 unique test IDs;
7. verify the full-text ignore policy and prohibit tracked or staged PDFs;
8. exit zero and print `PASS` when no violation exists;
9. exit non-zero and identify the offending path and, when available, record ID
   when a violation exists.

`DERIVED_INFERENCE`: Validation is read-only and deterministic. It SHALL NOT
rewrite rows, choose a canonical duplicate, fetch missing metadata, repair a
cross-link, or weaken a rule to accept generated output. Diagnostics SHOULD be
stable-sorted by path, record identity, and rule so repeated runs are comparable.

### 9.2 Validation As The Corpus Grows

Once a later goal creates instances, validators SHALL additionally check:

- column count and exact header order for every TSV row;
- identifier syntax and per-type uniqueness;
- allowed enum membership;
- required key presence, nested-key presence, and completed-value rules;
- foreign-key type and resolvability under the declared forward-reference phase;
- `SOURCE_CLAIM` provenance and metadata-only/unavailable prohibitions;
- DRAFT exclusion from retrieval, Pareto archives, and the decision atlas;
- candidate evaluator-stage ordering and experiment completeness;
- no accidental full-text tracking and explicit license state for acquisitions.

Paper deduplication ultimately covers arXiv ID, DOI, normalized title, and known
version relationships while preserving discovered URLs. G00 only rejects exact
duplicate IDs and exact duplicate rows or records; it does not merge or choose a
canonical record. The SOP does not freeze broader normalization algorithms,
version reconciliation, or merge precedence. G02 SHALL freeze those details
before the first manifest merge.

## 10. Deferred Schemas And Freeze Owners

`SOURCE_CLAIM`: These paths or records are named by the SOP, but their exact
headers or concrete schemas are not. G00 SHALL NOT invent silent canonical
formats.

| Deferred artifact | Missing contract | Goal that freezes it before first use |
| --- | --- | --- |
| `evidence/evidence-conflicts.tsv` | Exact header, conflict type, resolution state | G06 |
| `synthesis/pareto-archive.tsv` | Exact header, niche serialization, ranking fields | G08 |
| Adversarial fixture registry | Registry path, fixture-ID format, payload/link syntax | G06 |
| Separate architecture genomes | File envelope and relationship to candidate cards | G08 |
| Candidate mutation records | Added/removed patterns, intended behavior, composability fields | G08 |
| Candidate invalidation decisions | Disposition/rationale storage | G06/G08 |
| Evaluator stages after `SCHEMA_ONLY` | Exact serialized tokens and promotion record | G08 |
| Experiment fixture references and packet envelope | Fixture foreign keys and Markdown/YAML envelope | G09 |
| Experiment results | Result schema and measured-versus-modeled fields | Later implementation goal explicitly authorized after G09 |
| Alias records | Alias location/header for each artifact family | First owning goal that performs a merge |

Likewise, `architecture-decision-atlas.md`, retrieval index files, prompt files,
and campaign-close reports have named roles but no G00 data schema in the SOP.
Their owning goals SHALL freeze formats before first use.

The storage policy for `retrieval/pattern-index.sqlite` is also unresolved. SOP
Open Question 3 leaves ordinary Git, Git LFS after a threshold, and reproducible
rebuilds open; G00 SHALL NOT decide among them through schema prose or ignore
rules.

## 11. G00 Acceptance Contract

`DERIVED_INFERENCE`: This schema lane is complete only when:

- this document exists at its assigned path;
- it contains no corpus data instances;
- all exact SOP headers and explicit enums are preserved;
- every required logical field from the six requested artifact schemas is
  represented;
- DRAFT, empty-corpus, cross-link, epistemic-label, and validator behavior is
  explicit;
- unspecified formats remain visibly deferred to their owning later goals;
- no G01-G10 work has begun.

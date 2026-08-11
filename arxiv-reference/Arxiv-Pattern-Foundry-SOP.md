# Arxiv Pattern Foundry SOP

**Status:** Goal-ready executable specification
**Version:** 0.1
**Date:** 2026-08-10
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`
**Implementation boundary:** Research and architecture invention, not production algorithm implementation

## Executive Decision

The arXiv program SHALL build an LLM-usable architecture invention system, not a PDF archive and not a collection of paper summaries.

The system SHALL transform repository questions and external literature into four complementary assets:

1. **Pattern Foundry:** reusable mechanism cards grounded in sources.
2. **Constraint Time Machine:** historical and cross-domain mechanisms transplanted under modern Knight Bus constraints.
3. **Architecture Evolution Arena:** diverse architecture candidates maintained across explicit Pareto niches.
4. **Counterexample Foundry:** failure cards and adversarial workloads that invalidate or repair candidates.

The product-level filter remains A007:

> Knight Bus should make a resource estimate enforceable through admission, a bounded plan, execution, verification, and a receipt.

Research that does not change an architecture decision, expose a failure boundary, improve an estimator, or create a falsifying experiment SHALL be treated as background material rather than completed progress.

## Feature Outcome

Given a Knight Bus workload contract, an LLM SHALL be able to retrieve a small and deliberately diverse set of evidence-backed mechanisms, generate materially different architectures, derive their resource consequences, attack them with counterexamples, and emit a ranked experiment backlog.

## Actors And Boundaries

| Actor | Responsibility | Explicit boundary |
|---|---|---|
| Research goal agent | Discover, acquire, extract, normalize, and verify evidence | SHALL NOT silently advance into another goal packet |
| Architecture synthesis agent | Combine mechanisms into candidates | SHALL NOT present generated estimates as measurements |
| Skeptical verifier | Search for broken assumptions and counterexamples | SHALL NOT reject novelty merely because it is unfamiliar |
| Human decision maker | Select goals, approve policy changes, and choose experiments | Retains product and licensing decisions |
| arXiv | Discovery and full-text source for available papers | Is not a complete catalog of pre-1991 research |
| Citation services | Resolve older and non-arXiv references | Metadata support does not imply full-text redistribution rights |
| Knight Bus repository | Stores committed metadata, cards, synthesis, prompts, and validators | PDFs SHALL be local and ignored by default |

## Non-Goals

- Reimplement every paper.
- Summarize every graph paper.
- Treat citation count, publication year, or venue prestige as proof.
- Declare a universal graph storage format before workload evidence exists.
- Replace customer discovery with technical research.
- Commit a large PDF corpus without explicit license review.
- Claim RAM or latency improvements before a controlled benchmark.
- Allow a generated architecture to overrule the current A007 product contract.

## Repository Contract

The campaign SHALL converge on this structure:

```text
arxiv-reference/
├── Arxiv-Pattern-Foundry-SOP.md
├── README.md
├── governance/
│   ├── architecture-question-ledger.md
│   ├── keyword-taxonomy.tsv
│   ├── query-ledger.tsv
│   └── campaign-status.md
├── sources/
│   ├── paper-manifest.tsv
│   ├── citation-edges.tsv
│   ├── download-ledger.tsv
│   └── papers/                         # local, gitignored by default
├── evidence/
│   ├── mechanism-cards/
│   ├── failure-cards/
│   ├── constraint-transfer-cards/
│   └── evidence-conflicts.tsv
├── retrieval/
│   ├── pattern-index.sqlite
│   ├── pattern-taxonomy.yaml
│   └── diversity-policy.yaml
├── synthesis/
│   ├── architecture-genomes/
│   ├── architecture-candidates/
│   ├── pareto-archive.tsv
│   ├── architecture-decision-atlas.md
│   └── experiment-backlog.md
├── prompts/
│   ├── extract-mechanism-cards.md
│   ├── extract-failure-cards.md
│   ├── transplant-constraint-patterns.md
│   ├── generate-architecture-options.md
│   └── challenge-architecture-candidates.md
├── tools/
│   └── validate_arxiv_corpus_contract.py
└── journals/
    └── <goal-id>-progress.md
```

The structure is a target contract. Goal `G00` creates only the minimum files required to begin; later goals create their owned outputs.

## Stable Identifier Contract

| Artifact | ID format | Example |
|---|---|---|
| Architecture question | `AQ-<FAMILY>-<NNN>` | `AQ-FRONTIER-001` |
| Query family | `QRY-<QUESTION>-<NNN>` | `QRY-FRONTIER-001-003` |
| Canonical paper | `PAPER-<ARXIV_OR_HASH>` | `PAPER-1905.04264` |
| Mechanism card | `PAT-<FOUR-WORD-SLUG>` | `PAT-SELECT-ACTIVE-PARTITIONS-ONLY` |
| Failure card | `FAIL-<FOUR-WORD-SLUG>` | `FAIL-SPARSE-FRONTIER-TURNS-DENSE` |
| Constraint transfer | `XFER-<FOUR-WORD-SLUG>` | `XFER-BOUND-ACTIVE-WORKING-SET` |
| Architecture candidate | `ARCH-<FAMILY>-<NNN>` | `ARCH-FRONTIER-003` |
| Experiment | `EXP-<ARCH-ID>-<NNN>` | `EXP-ARCH-FRONTIER-003-001` |

IDs SHALL remain stable after publication. Renames SHALL change display names, not identifiers. Merges SHALL retain aliases pointing at the canonical ID.

## Core Artifact Schemas

### Architecture Question Ledger

`governance/architecture-question-ledger.md` SHALL provide one section per question with:

```yaml
question_id: AQ-FRONTIER-001
decision: Choose topology and frontier layout for bounded traversal.
product_consequence: Predict and enforce peak memory for security blast-radius queries.
candidate_options:
  - shared dual CSR
  - active partition CSR
  - streamed edge blocks
  - recursive locality tiles
known_evidence: []
missing_evidence: []
falsifier: A candidate cannot remain under the ceiling or loses unacceptable latency.
status: OPEN
owner_goal: G01
```

Allowed statuses are `OPEN`, `EVIDENCE_COLLECTING`, `EXPERIMENT_READY`, `DECIDED`, and `REJECTED`.

### Keyword Taxonomy

`governance/keyword-taxonomy.tsv` SHALL use this exact header:

```text
term_id	term	term_type	architecture_question_ids	source_repo_paths	synonyms	historical_terms	adjacent_domain_terms	exclusion_terms	notes
```

`term_type` SHALL be one of `ALGORITHM`, `LAYOUT`, `STATE`, `SCHEDULING`, `IO`, `PREDICTABILITY`, `CORRECTNESS`, `HARDWARE`, or `PRODUCT_CONTRACT`.

### Query Ledger

`governance/query-ledger.tsv` SHALL use this exact header:

```text
query_id	architecture_question_ids	source_term_ids	service	query_text	categories	date_from	date_to	exclusions	executed_at	result_count	response_checksum	status
```

Allowed statuses are `PLANNED`, `EXECUTED`, `RATE_LIMITED`, `FAILED`, and `SUPERSEDED`.

### Paper Manifest

`sources/paper-manifest.tsv` SHALL use this exact header:

```text
paper_id	arxiv_id	doi	title	authors	published_date	updated_date	categories	abstract_url	pdf_url	license_uri	canonical_version	discovery_query_ids	architecture_question_ids	relevance_score	score_breakdown	selection_status	evidence_grade	code_urls	local_path	sha256	notes
```

Allowed selection statuses are `METADATA_ONLY`, `DEEP_READ`, `READ_COMPLETE`, `REJECTED`, and `UNAVAILABLE`.

### Citation Edge Ledger

`sources/citation-edges.tsv` SHALL use this exact header:

```text
source_paper_id	target_paper_id	edge_type	discovery_source	relevance_reason	verified_at
```

`edge_type` SHALL be one of `CITES`, `IMPLEMENTS`, `EVALUATES`, `REFINES`, `CONTRADICTS`, or `SURVEYS`.

### Mechanism Card

Each mechanism card SHALL contain this minimum structure:

```yaml
pattern_id: PAT-SELECT-ACTIVE-PARTITIONS-ONLY
name: Select Active Partitions Only
epistemic_label: SOURCE_CLAIM
source_paper_ids: []
source_pointers: []
source_domain: external-memory graph processing
problem: ""
invariant: ""
mechanism: ""
data_arrangement: ""
access_schedule: ""
resident_state: ""
streamed_state: ""
recomputed_state: ""
resource_model:
  ram: ""
  io: ""
  preprocessing: ""
  persistent_storage: ""
  temporary_storage: ""
works_when: []
fails_when: []
unknown_when: []
knight_bus_algorithm_families: []
a007_consequence: ""
falsifying_experiment_id: ""
evidence_grade: D_THEORETICAL_OR_INCOMPLETE
confidence_rationale: ""
related_pattern_ids: []
```

### Failure Card

Each failure card SHALL contain:

```yaml
failure_id: FAIL-SPARSE-FRONTIER-TURNS-DENSE
name: Sparse Frontier Turns Dense
epistemic_label: SOURCE_CLAIM
source_paper_ids: []
source_pointers: []
broken_assumption: ""
triggering_workload: ""
observable_symptom: ""
breakpoint_equation: ""
affected_pattern_ids: []
affected_architecture_ids: []
adversarial_fixture: ""
expected_failure_signal: ""
repair_options: []
confidence_rationale: ""
```

### Constraint Transfer Card

Each transfer card SHALL contain:

```yaml
transfer_id: XFER-BOUND-ACTIVE-WORKING-SET
name: Bound Active Working Set
epistemic_label: SPECULATIVE_TRANSFER
source_pattern_ids: []
original_domain: operating systems
original_constraints: []
original_cost_model: ""
surviving_invariant: ""
reversed_assumptions: []
modern_knight_bus_constraints: []
proposed_transfer: ""
modern_resource_model:
  ram: ""
  io: ""
  preprocessing: ""
  storage: ""
  concurrency: ""
analogy_failure_modes: []
target_algorithm_families: []
falsifying_experiment_id: ""
```

### Architecture Candidate

Each candidate SHALL contain:

```yaml
architecture_id: ARCH-FRONTIER-003
name: ""
epistemic_label: SPECULATIVE_TRANSFER
architecture_question_ids: []
parent_architecture_ids: []
mechanism_pattern_ids: []
failure_card_ids: []
constraint_transfer_ids: []
workload_contract:
  artifact: ""
  algorithm_family: ""
  exactness: ""
  ram_ceiling_bytes: ""
  storage_allowance_bytes: ""
  deadline_model: ""
  output_bound: ""
genome:
  topology_layout: ""
  ordering: ""
  state_placement: ""
  scheduling: ""
  overflow_behavior: ""
  admission_model: ""
  receipt_model: ""
  compatibility_boundary: ""
resource_model:
  topology: ""
  algorithm_state: ""
  frontier_or_active_set: ""
  scratch: ""
  output: ""
  conversion: ""
  page_cache_or_direct_io: ""
  runtime_overhead: ""
  spill: ""
  safety_margin: ""
preparation_model: ""
storage_amplification: ""
correctness_contract: ""
determinism_contract: ""
failure_boundaries: []
fallback_response: ""
pareto_niches: []
highest_evaluator_stage: SCHEMA_ONLY
falsifying_experiment_id: ""
```

### Experiment Packet

Every experiment in `synthesis/experiment-backlog.md` SHALL declare:

```yaml
experiment_id: EXP-ARCH-FRONTIER-003-001
architecture_id: ARCH-FRONTIER-003
hypothesis: ""
fixture_ids: []
holdout_fixture_ids: []
baseline: ""
independent_oracle: ""
controlled_variables: []
measured_metrics: []
acceptance_thresholds: []
disconfirming_result: ""
modeled_expectation: ""
required_implementation_scope: ""
```

No schema permits a blank required field in a completed artifact. Draft artifacts SHALL declare `status: DRAFT` and SHALL not enter retrieval or the decision atlas.

# 1. Executable Requirements

## 1.1 Governance And Product Alignment

### REQ-GOV-001.0: Preserve the governing product contract

**WHEN** any research or synthesis goal begins
**THEN** the agent SHALL read A007, the current implementation gap ledger, and this SOP
**AND** SHALL state which A007 uncertainty the goal is intended to reduce
**SHALL** reject work whose only outcome is “learn more about graph systems.”

### REQ-GOV-002.0: Begin from architecture questions

**WHEN** a discovery campaign is initialized
**THEN** every query family SHALL reference at least one stable architecture-question ID
**AND** every architecture question SHALL name the decision, alternatives, required evidence, and falsifier
**SHALL** leave unresolved questions open rather than forcing premature conclusions.

### REQ-GOV-003.0: Separate facts from invention

**WHEN** an artifact includes sourced claims, derived consequences, or speculative transfers
**THEN** it SHALL label them `SOURCE_CLAIM`, `DERIVED_INFERENCE`, or `SPECULATIVE_TRANSFER`
**AND** SHALL provide source pointers for every `SOURCE_CLAIM`
**SHALL** never present `SPECULATIVE_TRANSFER` as published evidence.

### REQ-GOV-004.0: Measure decision yield

**WHEN** a goal closes
**THEN** its journal SHALL report papers screened, papers read, cards produced, conflicts found, candidates changed, and experiments created
**AND** SHALL identify at least one decision impact or explicitly report `NO_DECISION_IMPACT`
**SHALL** not use download count as the primary success metric.

## 1.2 Keyword And Metadata Discovery

### REQ-DISC-001.0: Mine repository-derived terminology

**WHEN** keyword discovery runs
**THEN** it SHALL begin with the P0 and P1 files in `Markdown-Value-Index.md`
**AND** SHALL trace every retained keyword to one or more repository files
**SHALL** classify each term by algorithm, layout, state, scheduling, I/O, predictability, correctness, hardware, or product contract.

### REQ-DISC-002.0: Expand mechanisms beyond current vocabulary

**WHEN** a repository term is recorded
**THEN** the taxonomy SHALL include synonyms, historical terminology, neighboring-domain terminology, and exclusion terms
**AND** SHALL include at least one cross-domain expansion for every architecture-question family
**SHALL** preserve the original repository wording for traceability.

### REQ-DISC-003.0: Construct compound query families

**WHEN** a metadata query is generated
**THEN** it SHALL combine a workload or algorithm term with a mechanism or resource term
**AND** SHALL record source terms, date range, categories, exclusions, execution timestamp, and result count
**SHALL** avoid unbounded single-word searches such as `graph`.

### REQ-DISC-004.0: Search across eras

**WHEN** a query family is executed
**THEN** it SHALL include a broad all-years pass and date-bucketed passes when the source supports them
**AND** SHALL not rank recent papers above older papers solely because of recency
**SHALL** mark pre-arXiv ancestry as requiring citation traversal through another bibliographic source.

### REQ-DISC-005.0: Deduplicate paper identities

**WHEN** metadata results are merged
**THEN** records SHALL be deduplicated by arXiv ID, DOI, normalized title, and known version relationships
**AND** SHALL preserve all discovered source URLs
**SHALL** retain one canonical paper identity with version history.

### REQ-DISC-006.0: Rank for architectural yield

**WHEN** candidate papers are scored
**THEN** scoring SHALL consider mechanism clarity, resource quantification, implementation evidence, benchmark completeness, transferability, failure disclosure, and domain distance
**AND** SHALL retain a transparent score breakdown
**SHALL** preserve a bounded exploration quota for low-citation, old, contradictory, and distant-domain papers.

## 1.3 Citation Archaeology And Acquisition

### REQ-ACQ-001.0: Traverse backward from modern seeds

**WHEN** a high-relevance modern paper is selected
**THEN** the campaign SHALL inspect its references for foundational mechanisms and original terminology
**AND** SHALL record directed citation edges with relationship type and discovery source
**SHALL** continue backward only while the ancestor can change an architecture question.

### REQ-ACQ-002.0: Traverse forward toward implementations

**WHEN** a foundational paper is identified
**THEN** the campaign SHALL search for later implementations, evaluations, contradictions, and refinements
**AND** SHALL connect implementation repositories when available
**SHALL** distinguish original mechanisms from later engineering improvements.

### REQ-ACQ-003.0: Acquire only selected full text

**WHEN** metadata screening assigns a paper to `DEEP_READ`
**THEN** the downloader SHALL record source URL, access timestamp, checksum, local path, license URI when available, and acquisition status
**AND** SHALL store the PDF under the ignored local paper directory by default
**SHALL** not download rejected metadata candidates.

### REQ-ACQ-004.0: Respect source services

**WHEN** APIs or full-text endpoints are used
**THEN** clients SHALL identify themselves where required, cache responses, use bounded concurrency, retry with backoff, and honor published terms
**AND** SHALL checkpoint progress without repeatedly fetching completed records
**SHALL** stop and report persistent rate-limit or authorization failures.

### REQ-ACQ-005.0: Preserve unavailable ancestry

**WHEN** an important older source cannot be lawfully or technically acquired
**THEN** the manifest SHALL retain its citation, DOI or bibliographic identity, discovery chain, and relevance reason
**AND** SHALL mark full text `UNAVAILABLE`
**SHALL** prohibit the LLM from inventing claims from metadata alone.

## 1.4 Mechanism Pattern Foundry

### REQ-PAT-001.0: Extract mechanisms rather than summaries

**WHEN** a paper is read
**THEN** the output SHALL identify independently reusable mechanisms
**AND** each mechanism SHALL state its problem, invariant, data arrangement, access schedule, resident state, streamed state, resource trade, and failure boundary
**SHALL** reject a card whose mechanism is only the paper title restated.

### REQ-PAT-002.0: Ground every mechanism

**WHEN** a mechanism card is saved
**THEN** it SHALL include a source identifier and section, page, figure, theorem, or repository pointer
**AND** SHALL distinguish author claims from extractor inference
**SHALL** include a short confidence rationale.

### REQ-PAT-003.0: Normalize resource consequences

**WHEN** a mechanism changes data movement or state
**THEN** the card SHALL express symbolic RAM, I/O, preprocessing, storage-amplification, and recomputation consequences when derivable
**AND** SHALL name unknown coefficients explicitly
**SHALL** avoid numeric performance estimates unsupported by the source or measurement.

### REQ-PAT-004.0: Record applicability boundaries

**WHEN** a mechanism card is completed
**THEN** it SHALL state `WORKS_WHEN`, `FAILS_WHEN`, and `UNKNOWN_WHEN` conditions
**AND** SHALL map applicable Knight Bus algorithm families without asserting universal fit
**SHALL** include the smallest experiment capable of falsifying the transfer.

### REQ-PAT-005.0: Link related patterns

**WHEN** two patterns share, complement, contradict, or subsume a mechanism
**THEN** the index SHALL record a typed relationship
**AND** SHALL prevent exact duplicate cards
**SHALL** preserve materially different failure boundaries as separate variants.

### REQ-PAT-006.0: Grade evidence strength

**WHEN** a pattern enters retrieval
**THEN** it SHALL carry one evidence grade: `A_REPRODUCED`, `B_CODE_BACKED`, `C_PAPER_BENCHMARK`, `D_THEORETICAL_OR_INCOMPLETE`, or `E_CONTRADICTED`
**AND** SHALL permit lower-grade patterns for invention
**SHALL** prevent lower-grade patterns from supporting measured product claims.

## 1.5 Counterexample Foundry

### REQ-FAIL-001.0: Extract negative evidence

**WHEN** a paper reports limitations, regressions, sensitivity, negative results, or workload reversals
**THEN** the reader SHALL create or update a failure card
**AND** SHALL identify the broken assumption, observable symptom, and triggering workload
**SHALL** preserve the source pointer.

### REQ-FAIL-002.0: Construct adversarial workloads

**WHEN** a candidate architecture depends on a workload assumption
**THEN** the verifier SHALL define a minimal adversarial graph or execution profile that violates that assumption
**AND** SHALL state the expected failure signal
**SHALL** distinguish analytical counterexamples from measured failures.

### REQ-FAIL-003.0: Repair or reject candidates

**WHEN** a counterexample invalidates a candidate
**THEN** the synthesis record SHALL choose `REPAIR`, `SPECIALIZE`, `DEFER`, or `REJECT`
**AND** SHALL explain the decision
**SHALL** retain rejected candidates and their evidence to prevent rediscovery loops.

### REQ-FAIL-004.0: Delay adversarial context during generation

**WHEN** an LLM performs divergent architecture generation
**THEN** it SHALL generate candidates before loading the most relevant failure cards
**AND** SHALL apply adversarial review in a separate pass
**SHALL** prevent early criticism from collapsing diversity prematurely.

## 1.6 Constraint Time Machine

### REQ-TIME-001.0: Index papers by constraints

**WHEN** a historical or cross-domain source is read
**THEN** it SHALL be tagged by constrained resource, access medium, predictability requirement, data mutability, and communication model
**AND** SHALL record the original hardware or operating assumptions when known
**SHALL** not rely on publication year as the mechanism description.

### REQ-TIME-002.0: Preserve transferable invariants

**WHEN** a mechanism is transplanted into Knight Bus
**THEN** the transfer card SHALL state which invariant survives, which historical cost assumptions reverse, and which modern constraints replace them
**AND** SHALL show why the analogy is operational rather than metaphorical
**SHALL** reject transfers without a surviving invariant.

### REQ-TIME-003.0: Recalculate the modern cost model

**WHEN** a transfer card proposes an architecture mechanism
**THEN** it SHALL derive the modern RAM, I/O, preprocessing, storage, and concurrency terms symbolically
**AND** SHALL identify constants requiring measurement
**SHALL** avoid importing historical benchmark ratios.

### REQ-TIME-004.0: Generate distant alternatives

**WHEN** an architecture question enters synthesis
**THEN** retrieval SHALL include mechanisms from at least three source domains when available
**AND** SHALL include at least one non-graph source
**SHALL** record when insufficient evidence prevents that diversity target.

## 1.7 Architecture Evolution Arena

### REQ-EVOL-001.0: Encode architecture genomes

**WHEN** a candidate is generated
**THEN** it SHALL declare topology layout, ordering, state placement, scheduling, overflow behavior, exactness, admission model, receipt model, and compatibility boundary
**AND** SHALL link every inherited mechanism card
**SHALL** mark unsupported genome fields explicitly.

### REQ-EVOL-002.0: Preserve qualitative diversity

**WHEN** candidates are added to the arena
**THEN** they SHALL be assigned to explicit niches such as lowest RAM, lowest latency, lowest preparation, lowest storage amplification, highest predictability, and lowest adoption friction
**AND** SHALL compete primarily within comparable niches
**SHALL** not collapse all trade-offs into one unexplained scalar score.

### REQ-EVOL-003.0: Generate traceable variation

**WHEN** an LLM mutates or recombines architectures
**THEN** the child SHALL record parent candidates, added patterns, removed patterns, and intended changed behavior
**AND** SHALL state why the combination is believed composable
**SHALL** prohibit untraceable “creative” components.

### REQ-EVOL-004.0: Use staged evaluators

**WHEN** a candidate is evaluated
**THEN** it SHALL pass schema validation, symbolic resource review, counterexample review, microbenchmark review, and workload benchmark review in that order as applicable
**AND** SHALL record the highest completed stage
**SHALL** not treat analytical estimates as measured survivors.

### REQ-EVOL-005.0: Maintain holdout workloads

**WHEN** repeated candidate selection uses benchmark fixtures
**THEN** the arena SHALL retain holdout graph shapes and workloads not exposed during generation
**AND** SHALL use them before promoting a candidate to the decision atlas
**SHALL** report evaluator gaming or benchmark overfitting when detected.

## 1.8 Architecture Candidate Contract

### REQ-ARCH-001.0: Tie candidates to workload contracts

**WHEN** an architecture candidate is proposed
**THEN** it SHALL name the artifact, algorithm family, exactness requirement, RAM ceiling, storage allowance, deadline model, and output bound
**AND** SHALL explain which A007 decision it strengthens
**SHALL** reject generic “faster graph engine” candidates.

### REQ-ARCH-002.0: Derive complete resource terms

**WHEN** a candidate enters comparative review
**THEN** it SHALL account for topology, algorithm state, frontier or active set, scratch, output, conversion, page cache or direct I/O, runtime overhead, spill, and safety margin
**AND** SHALL identify double-counting risks
**SHALL** mark unknown terms rather than treating them as zero.

### REQ-ARCH-003.0: Include preparation and amplification

**WHEN** a candidate uses a specialized artifact
**THEN** it SHALL report artifact build phases, expected build memory, build I/O, persistent bytes, temporary bytes, freshness model, and amortization assumptions
**AND** SHALL compare against a shared-layout baseline
**SHALL** reject runtime-only comparisons that hide preparation.

### REQ-ARCH-004.0: State correctness and determinism

**WHEN** a candidate changes ordering, partitioning, approximation, concurrency, or recomputation
**THEN** it SHALL state exactness, numerical tolerance, seed policy, ordering guarantees, and oracle strategy
**AND** SHALL identify nondeterministic sources
**SHALL** define refusal when the requested guarantee cannot be met.

### REQ-ARCH-005.0: State failure boundaries

**WHEN** a candidate is presented
**THEN** it SHALL list workload shapes and resource conditions where it loses its advantage
**AND** SHALL link relevant failure cards
**SHALL** describe whether the response is switch plan, spill, approximate, or refuse.

### REQ-ARCH-006.0: Terminate in falsifiable experiments

**WHEN** a candidate reaches the decision atlas
**THEN** it SHALL have one smallest falsifying experiment with fixtures, baseline, oracle, metrics, thresholds, and expected disconfirming result
**AND** SHALL separate modeled expectations from acceptance thresholds
**SHALL** prohibit implementation goals without a verification loop.

## 1.9 Goal Execution And Context Retention

### REQ-GOAL-001.0: Execute one bounded goal packet

**WHEN** a Codex goal starts
**THEN** it SHALL name exactly one goal packet from the campaign map
**AND** SHALL state inputs, batch caps, outputs, and exit tests before work begins
**SHALL** stop after that packet unless the user explicitly authorizes continuation.

### REQ-GOAL-002.0: Initialize a progress journal

**WHEN** a goal packet begins
**THEN** it SHALL initialize or resume one journal with `tdd-task-progress-context-retainer`
**AND** SHALL checkpoint after discovery, acquisition, extraction, synthesis, and verification transitions as applicable
**SHALL** keep `Next Steps` non-empty.

### REQ-GOAL-003.0: Resume from artifacts

**WHEN** an interrupted goal resumes
**THEN** the agent SHALL read the journal snapshot, campaign status, owned manifests, and unresolved failures
**AND** SHALL restate the exact next action and why it is next
**SHALL** not restart completed API calls or paper reads without cause.

### REQ-GOAL-004.0: Keep context retrieval bounded

**WHEN** an LLM generates or reviews architecture candidates
**THEN** retrieval SHALL use a declared card budget and diversity policy
**AND** SHALL prefer normalized cards over raw full-text dumps
**SHALL** fetch source passages only for verification or ambiguity resolution.

### REQ-GOAL-005.0: Close with a handoff

**WHEN** a goal reaches its exit criteria
**THEN** the journal SHALL report current phase, produced artifacts, test results, blockers, unresolved decisions, and top three next steps
**AND** SHALL update campaign status
**SHALL** identify the recommended next goal without starting it.

### REQ-GOAL-006.0: Preserve reproducibility

**WHEN** a goal uses queries, ranking, random sampling, or LLM generation
**THEN** it SHALL record query inputs, model or tool identity when available, prompts, timestamps, ranking policy, random seed when supported, and output checksums
**AND** SHALL preserve rejected candidates needed to explain the result
**SHALL** disclose irreproducible external state.

## 1.10 Licensing And Git Hygiene

### REQ-LEGAL-001.0: Ignore full text by default

**WHEN** full-text files are downloaded
**THEN** `sources/papers/` SHALL be ignored by Git by default
**AND** SHALL keep committed manifests and source URLs sufficient to reacquire lawful copies
**SHALL** prohibit accidental PDF staging.

### REQ-LEGAL-002.0: Track licenses explicitly

**WHEN** source metadata exposes a license
**THEN** the manifest SHALL record the license URI and retrieval source
**AND** SHALL distinguish unknown license from permissive license
**SHALL** require human approval before committing any full text.

### REQ-LEGAL-003.0: Paraphrase within evidence cards

**WHEN** evidence cards are committed
**THEN** they SHALL primarily paraphrase mechanisms and store precise source pointers
**AND** SHALL keep quotations short and necessary
**SHALL** avoid reconstructing substantial copyrighted text across cards.

# 2. Test Matrix

The validator created in `G00` SHALL expose four-word functions such as `validate_source_query_terms`, `deduplicate_paper_manifest_entries`, `validate_mechanism_card_fields`, `validate_failure_card_fields`, `validate_transfer_card_invariants`, `score_architecture_candidate_niches`, `verify_download_license_policy`, and `audit_requirement_test_links`.

| req_id | test_id | type | assertion | target |
|---|---|---|---|---|
| REQ-GOV-001.0 | TEST-GOV-001 | contract | Goal header references A007, gap ledger, and one uncertainty | alignment |
| REQ-GOV-002.0 | TEST-GOV-002 | schema | Every query family links an architecture-question ID | traceability |
| REQ-GOV-003.0 | TEST-GOV-003 | schema | Claims use only the three allowed epistemic labels | factuality |
| REQ-GOV-004.0 | TEST-GOV-004 | contract | Goal closure records yield and decision impact | outcomes |
| REQ-DISC-001.0 | TEST-DISC-001 | integration | Keyword rows include repository source paths and categories | provenance |
| REQ-DISC-002.0 | TEST-DISC-002 | schema | Each question family has synonyms, history, domain expansion, and exclusions | recall |
| REQ-DISC-003.0 | TEST-DISC-003 | integration | Query ledger records complete compound-query metadata | reproducibility |
| REQ-DISC-004.0 | TEST-DISC-004 | contract | All-years and historical-ancestry policies are represented | historical coverage |
| REQ-DISC-005.0 | TEST-DISC-005 | unit | Duplicate arXiv, DOI, version, and title fixtures merge canonically | identity |
| REQ-DISC-006.0 | TEST-DISC-006 | unit | Score breakdown and exploration quota survive sorting | diversity |
| REQ-ACQ-001.0 | TEST-ACQ-001 | integration | Backward citation edges retain purpose and source | ancestry |
| REQ-ACQ-002.0 | TEST-ACQ-002 | integration | Forward edges distinguish implementation, evaluation, and contradiction | evolution |
| REQ-ACQ-003.0 | TEST-ACQ-003 | contract | Only `DEEP_READ` rows may receive local full-text paths | acquisition |
| REQ-ACQ-004.0 | TEST-ACQ-004 | fault injection | Retry, cache, checkpoint, and persistent-rate-limit paths behave safely | service hygiene |
| REQ-ACQ-005.0 | TEST-ACQ-005 | unit | Unavailable paper cannot produce a source claim | hallucination control |
| REQ-PAT-001.0 | TEST-PAT-001 | schema | Mechanism card contains all required mechanism fields | extraction quality |
| REQ-PAT-002.0 | TEST-PAT-002 | schema | Mechanism claim includes source and evidence pointer | provenance |
| REQ-PAT-003.0 | TEST-PAT-003 | contract | Resource model labels unknowns and unsupported numbers | estimation honesty |
| REQ-PAT-004.0 | TEST-PAT-004 | schema | Applicability and falsification sections are present | boundaries |
| REQ-PAT-005.0 | TEST-PAT-005 | integration | Typed pattern edges are valid and exact duplicates are rejected | graph quality |
| REQ-PAT-006.0 | TEST-PAT-006 | unit | Evidence grades constrain claim eligibility | evidence strength |
| REQ-FAIL-001.0 | TEST-FAIL-001 | schema | Failure card includes assumption, trigger, symptom, and source | negative evidence |
| REQ-FAIL-002.0 | TEST-FAIL-002 | contract | Adversarial workload declares expected failure signal | falsifiability |
| REQ-FAIL-003.0 | TEST-FAIL-003 | schema | Invalidated candidate has one allowed disposition and rationale | decision memory |
| REQ-FAIL-004.0 | TEST-FAIL-004 | integration | Generation output exists before adversarial context is loaded | diversity |
| REQ-TIME-001.0 | TEST-TIME-001 | schema | Historical cards are indexed by constraints and assumptions | archaeology |
| REQ-TIME-002.0 | TEST-TIME-002 | contract | Transfer card states surviving invariant and reversed assumptions | transfer rigor |
| REQ-TIME-003.0 | TEST-TIME-003 | contract | Transfer uses symbolic modern costs rather than historical ratios | cost model |
| REQ-TIME-004.0 | TEST-TIME-004 | integration | Retrieval meets domain-diversity target or records insufficiency | novelty |
| REQ-EVOL-001.0 | TEST-EVOL-001 | schema | Architecture genome includes all required dimensions and links | representation |
| REQ-EVOL-002.0 | TEST-EVOL-002 | unit | Candidate archive retains distinct niches without forced scalar collapse | Pareto diversity |
| REQ-EVOL-003.0 | TEST-EVOL-003 | schema | Child candidate records parents and mechanism delta | lineage |
| REQ-EVOL-004.0 | TEST-EVOL-004 | integration | Promotion cannot skip evaluator stages | verification |
| REQ-EVOL-005.0 | TEST-EVOL-005 | integration | Holdout fixtures remain hidden until promotion test | overfitting control |
| REQ-ARCH-001.0 | TEST-ARCH-001 | schema | Candidate declares workload contract and A007 consequence | product fit |
| REQ-ARCH-002.0 | TEST-ARCH-002 | contract | Resource equation covers all required terms or marks unknowns | completeness |
| REQ-ARCH-003.0 | TEST-ARCH-003 | contract | Specialized artifact includes preparation and amplification | total cost |
| REQ-ARCH-004.0 | TEST-ARCH-004 | contract | Correctness, determinism, oracle, and refusal are explicit | semantics |
| REQ-ARCH-005.0 | TEST-ARCH-005 | schema | Candidate links failure boundaries and fallback response | robustness |
| REQ-ARCH-006.0 | TEST-ARCH-006 | schema | Candidate has a complete falsifying experiment packet | actionability |
| REQ-GOAL-001.0 | TEST-GOAL-001 | contract | Goal header names one packet and bounded outputs | scope |
| REQ-GOAL-002.0 | TEST-GOAL-002 | integration | Journal exists and contains required checkpoint sections | continuity |
| REQ-GOAL-003.0 | TEST-GOAL-003 | resume | Resume prompt restores next action without duplicate completed work | resumability |
| REQ-GOAL-004.0 | TEST-GOAL-004 | unit | Retrieval output respects card budget and diversity policy | context control |
| REQ-GOAL-005.0 | TEST-GOAL-005 | contract | Handoff contains status, evidence, decisions, and next steps | closure |
| REQ-GOAL-006.0 | TEST-GOAL-006 | contract | Query and generation artifacts preserve reproducibility metadata | auditability |
| REQ-LEGAL-001.0 | TEST-LEGAL-001 | Git gate | No PDF under the local paper directory is tracked or staged | repository hygiene |
| REQ-LEGAL-002.0 | TEST-LEGAL-002 | schema | License state is explicit for every acquired source | licensing |
| REQ-LEGAL-003.0 | TEST-LEGAL-003 | review | Cards contain bounded excerpts and source pointers | copyright hygiene |

# 3. TDD Plan And Goal SOP

## 3.1 Campaign Goal Map

Each goal is independently resumable. Default caps are safety rails, not scientific claims; a goal header may tighten them, but expanding them requires an explicit user decision.

| goal_id | goal | default cap | owned outputs | exit condition |
|---|---|---:|---|---|
| G00 | Initialize campaign contracts | One scaffold | README, schemas, ignore policy, validator, journal | Empty valid corpus passes validators |
| G01 | Mine architecture questions and terminology | P0 + P1 docs | Question ledger, taxonomy, query families | Every query traces to a decision |
| G02 | Discover metadata candidates | 25 query families or 2,000 canonical candidates | Query ledger, paper manifest | Deduplicated ranked manifest passes |
| G03 | Traverse citation ancestry | 25 seeds, depth 2, 250 new identities | Citation edges, updated manifest | Foundational and contradictory branches recorded |
| G04 | Acquire and parse selected papers | 50 papers | Download ledger, local PDFs, extracted text cache | Checksums, licenses, statuses complete |
| G05 | Extract mechanism cards | 25 papers | Mechanism cards and pattern edges | Every selected paper yields cards or `NO_MECHANISM` |
| G06 | Extract counterexamples | 25 papers or 20 candidates | Failure cards and conflict ledger | Assumptions have adversarial tests or explicit gaps |
| G07 | Run constraint time machine | 20 mechanisms | Constraint-transfer cards | Each transfer has invariant, modern costs, and falsifier |
| G08 | Run architecture evolution arena | 50 candidates, 6 or more niches | Genomes, candidate cards, Pareto archive | Diverse candidates survive schema and analytical review |
| G09 | Produce decision atlas | 3-8 finalists | Decision atlas and experiment backlog | Every finalist has a falsifying experiment |
| G10 | Close campaign iteration | One audit | Coverage report, decision delta, next campaign proposal | All requirements and artifacts reconciled |

## 3.2 Standard Goal Lifecycle

### STUB

1. Read this SOP and the goal's governing inputs.
2. Initialize or snapshot the goal journal.
3. Declare batch caps and owned output paths.
4. Write fixtures and validator expectations before producing artifacts.
5. Record the expected initial failures.

### RED

1. Run the goal-specific validators against missing or incomplete outputs.
2. Confirm failures identify absent fields, unsupported claims, duplicate identities, broken links, or scope violations.
3. Save the exact failures in the journal.
4. Do not weaken a validator merely to accept generated output.

### GREEN

1. Produce the minimum complete batch satisfying the goal contract.
2. Keep each card atomic and source-grounded.
3. Update manifests and typed edges in the same goal.
4. Run validators after each meaningful batch.
5. Checkpoint the journal when the phase or primary artifact changes.

### REFACTOR

1. Merge exact duplicates without erasing meaningful variants.
2. Normalize terms, IDs, links, confidence, and resource symbols.
3. Reduce repeated prose by linking shared patterns.
4. Preserve rejected evidence and candidate lineage.
5. Keep all validators green.

### VERIFY

1. Run the goal-specific tests from the traceability matrix.
2. Run the full corpus validator.
3. Verify Git and licensing gates.
4. Compare produced counts against declared caps.
5. Emit a handoff and stop before the next goal.

## 3.3 Goal Packet Template

Every goal SHALL begin with this packet in its journal or goal prompt:

```markdown
# Goal Packet

- Goal ID: G0X
- Objective: <one measurable outcome>
- A007 uncertainty reduced: <one question>
- Inputs: <exact paths and source services>
- Owned outputs: <exact paths>
- Batch caps: <queries, papers, cards, candidates, tokens if explicitly supplied>
- Excluded work: <what this goal will not do>
- Entry tests: <expected RED tests>
- Exit tests: <required GREEN tests>
- Stop conditions: <rate limit, license, ambiguity, cap, or failed premise>
- Journal: arxiv-reference/journals/<goal-id>-progress.md
```

## 3.4 Copy-Paste Goal Prompt

Use this prompt to execute one campaign goal:

```text
/goal Execute exactly Goal <G0X> from
arxiv-reference/Arxiv-Pattern-Foundry-SOP.md.

The product north star is:
docs_PRD04/A007-spc-founder-interview-prep-v7.md.

Before work:
1. Read the SOP requirement sections governing <G0X>.
2. Read Markdown-Value-Index.md and only the P0/P1 documents needed by the goal.
3. Read docs_PRD04/reference-learning/neo4j-compat-lowram/current-implementation-gap-ledger.md.
4. Initialize or snapshot the required tdd-task-progress-context-retainer journal.
5. Write the Goal Packet with exact caps and outputs.

During work:
- Follow STUB -> RED -> GREEN -> REFACTOR -> VERIFY.
- Treat papers as evidence sources, not implementations to copy.
- Produce normalized mechanism, failure, transfer, or architecture artifacts owned by this goal.
- Label SOURCE_CLAIM, DERIVED_INFERENCE, and SPECULATIVE_TRANSFER.
- Keep full text local and ignored unless explicitly authorized.
- Checkpoint after each meaningful phase transition.

Before closure:
- Run every test linked to the goal's REQ IDs.
- Run the full corpus validator and Git/license gates.
- Report decision yield, including NO_DECISION_IMPACT when applicable.
- Update campaign-status.md and the goal journal.
- Recommend the next goal but DO NOT start it.

Do not expand the batch caps or proceed to another Goal ID without explicit authorization.
```

## 3.5 Resume Prompt

```text
/goal Resume Goal <G0X> from its journal and campaign status.

Read the latest snapshot first. Restate:
- exact current phase;
- tests currently failing or passing;
- files in motion;
- completed batch identities;
- next action and why it is next;
- constraints that make naive continuation dangerous.

Continue only the incomplete portion of <G0X>. Do not repeat completed downloads,
paper reads, cards, or API calls unless the journal records a verification reason.
Close with the normal VERIFY handoff and do not start the next goal.
```

## 3.6 Audit Prompt

```text
/goal Audit the current arxiv-reference campaign against
arxiv-reference/Arxiv-Pattern-Foundry-SOP.md without generating new papers,
patterns, transfers, or architectures.

Verify requirement-to-test traceability, artifact schemas, source pointers,
claim labels, duplicate identities, licensing state, ignored full text,
candidate lineage, failure boundaries, and experiment completeness.

Write an evidence-based gap report ordered by severity. Do not silently repair
semantic gaps; list the smallest bounded goal that should repair each gap.
```

# 4. Quality Gates

## 4.1 Goal Entry Gates

- [ ] One Goal ID is selected.
- [ ] A007 uncertainty is named.
- [ ] Inputs, outputs, caps, exclusions, and stop conditions are explicit.
- [ ] Journal is initialized or snapshotted.
- [ ] RED fixtures and expected failures exist.
- [ ] Source-service terms and credentials are understood.

## 4.2 Evidence Gates

- [ ] Every source claim has a source pointer.
- [ ] Metadata-only papers cannot produce source claims.
- [ ] Every mechanism has an invariant and failure boundary.
- [ ] Every numeric claim is sourced, derived with assumptions, or measured.
- [ ] Contradictory evidence is retained.
- [ ] Evidence grades are present and respected.
- [ ] Quotes remain short and necessary.

## 4.3 Architecture Gates

- [ ] Candidate names its workload contract and A007 consequence.
- [ ] Mechanism ancestry is traceable.
- [ ] Resource equation includes all required terms or explicit unknowns.
- [ ] Preparation and storage amplification are included.
- [ ] Correctness, determinism, and refusal semantics are explicit.
- [ ] Failure cards and fallback behavior are linked.
- [ ] Candidate has a smallest falsifying experiment.
- [ ] Modeled expectations are not described as measured improvements.

## 4.4 Diversity Gates

- [ ] Retrieval budget is declared.
- [ ] At least three source domains are represented when evidence permits.
- [ ] At least one non-graph mechanism is considered.
- [ ] Conventional candidate is retained as a baseline.
- [ ] Candidates occupy multiple Pareto niches.
- [ ] Adversarial review occurs after divergent generation.
- [ ] Rejected candidates and reasons remain queryable.

## 4.5 Git And Licensing Gates

- [ ] `sources/papers/` is ignored.
- [ ] No PDF or source archive is staged without human approval.
- [ ] Acquired records include checksum and license state.
- [ ] Committed artifacts are metadata, paraphrased evidence, prompts, schemas, or synthesis.
- [ ] `git diff --check` passes.
- [ ] Only goal-owned files changed.

## 4.6 Pre-Commit Contract Gate

Before committing a goal, the agent SHALL verify:

```text
1. Every applicable REQ ID maps to a passing TEST ID.
2. The full corpus validator exits zero.
3. No TODO, STUB, or FIXME remains in committed goal artifacts.
4. No unmeasured performance claim is phrased as fact.
5. No full-text file is accidentally tracked.
6. The journal records exact tests and next steps.
7. Campaign status names the next recommended bounded goal.
```

# 5. Open Questions

These questions do not block `G00` or `G01`; later goals SHALL resolve them when evidence is available.

1. Which bibliographic source should be canonical for pre-arXiv citation identities: Crossref, DBLP, OpenAlex, or a reconciled identity layer?
2. Which PDF-to-structured-text parser best preserves equations, tables, figure references, and page pointers?
3. Should `pattern-index.sqlite` remain ordinary Git, use Git LFS after a size threshold, or be reproducibly rebuilt from committed cards?
4. What initial architecture-question families should receive the exploration quota beyond frontier traversal and dense iteration?
5. Which graph-shape dimensions belong in holdout fixtures: degree skew, diameter, community structure, component count, property width, update rate, and output cardinality?
6. Which symbolic resource variables should be standardized across every architecture card?
7. What evidence is sufficient to promote a `D_THEORETICAL_OR_INCOMPLETE` mechanism into implementation experiments?
8. How should the arena balance domain distance against implementation plausibility?
9. Which architecture candidates require customer evidence before technical implementation?
10. What campaign size creates diminishing returns for architecture decisions?

## Final Completion Criterion

The first campaign iteration is complete only when:

```text
repository questions
    -> reproducible literature discovery
    -> evidence-backed mechanism and failure cards
    -> cross-domain constraint transfers
    -> diverse traceable architecture candidates
    -> adversarially reviewed Pareto finalists
    -> falsifying Knight Bus experiments
```

The campaign SHALL be considered unsuccessful if it produces many papers and summaries but cannot identify what architecture decision changed, what assumption was falsified, or what experiment should run next.

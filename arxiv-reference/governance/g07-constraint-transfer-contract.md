# G07 Constraint Transfer Contract

## Purpose

This contract freezes how G07 converts 20 selected G05 mechanisms into
decision-useful, evidence-honest Knight Bus transfer possibilities. A transfer
is an input to G08, not an architecture candidate or implementation promise.

## Requirements

### REQ-G07-ENTRY-001: Admit Only Cleared Inputs

**WHEN** G07 starts
**THEN** the shared corpus validator SHALL pass against a completed and cleared
G06 corpus
**AND** all 67 G05 mechanisms SHALL have terminal G06 dispositions
**AND** no G07 semantic artifact SHALL predate this entry check.

### REQ-G07-FREEZE-002: Freeze Twenty Mechanisms

**WHEN** semantic transfer reading begins
**THEN** `g07-transfer-plan.tsv` SHALL contain exactly 20 unique G05 pattern IDs
**AND** SHALL assign them to four disjoint lanes of exactly five rows
**AND** the frozen set SHALL cover at least three source domains and at least one
non-graph source domain.

### REQ-G07-DISPOSITION-003: Resolve Every Mechanism

**WHEN** G07 completes
**THEN** every frozen mechanism SHALL have exactly one of:

- `TRANSFER_CREATED`
- `NO_SURVIVING_INVARIANT`
- `MODERN_COST_REVERSAL_INVALIDATES`
- `INSUFFICIENT_EVIDENCE`
- `DUPLICATE_TRANSFER_MERGED`

**AND** a non-transfer disposition SHALL record a concrete evidence gap or
reason rather than silently disappearing.

### REQ-G07-CARD-004: Preserve Operational Invariants

**WHEN** a canonical transfer card is emitted
**THEN** it SHALL identify the source mechanism, original constraints, a
surviving invariant, reversed assumptions, modern constraints, an operational
transfer, analogy failure modes, and a smallest falsifier
**AND** SHALL reject an analogy without a surviving invariant.

### REQ-G07-COST-005: Recalculate Modern Resources

**WHEN** a transfer proposes a Knight Bus mechanism
**THEN** it SHALL model RAM, I/O, preprocessing, storage, and concurrency with
symbolic expressions
**AND** each model SHALL name unknown constants and a way to measure them
**AND** SHALL NOT import a historical benchmark ratio as a modern estimate.

### REQ-G07-EVIDENCE-006: Keep Claim Classes Separate

**WHEN** a card combines source observations, derived cost reasoning, and a
proposed transfer
**THEN** each claim SHALL use `SOURCE_CLAIM`, `DERIVED_INFERENCE`, or
`SPECULATIVE_TRANSFER` at claim granularity
**AND** the card-level label SHALL remain `SPECULATIVE_TRANSFER`
**AND** source claims SHALL resolve through the source mechanism card's precise
pointers.

### REQ-G07-CHALLENGE-007: Apply G06 Before Admission

**WHEN** a provisional transfer is normalized
**THEN** every G06 failure card affecting its source pattern SHALL be inspected
**AND** the transfer SHALL add a guard, fallback, unknown constant, narrowed
scope, or rejection for every applicable failure
**AND** unresolved contradictions SHALL prevent clearance.

### REQ-G07-REVIEW-008: Separate Author And Reviewer

**WHEN** all canonical cards and dispositions are ready
**THEN** one independent reviewer who authored none of the cards SHALL inspect
the complete corpus
**AND** closure SHALL require P0 = P1 = P2 = 0 after repairs.

## Frozen Constraint Profile Placement

REQ-TIME-001.0 adds six values that the base transfer schema did not place. G07
freezes them in this required top-level object:

```json
{
  "original_constraint_profile": {
    "constrained_resource": {},
    "access_medium": {},
    "predictability_requirement": {},
    "data_mutability": {},
    "communication_model": {},
    "original_hardware_operating_assumptions": {}
  }
}
```

Each value is a claim object. Unknown historical information is represented as
`DERIVED_INFERENCE` with text `UNKNOWN`, the inspected premises, and a specific
uncertainty; it is never guessed from publication year.

## Claim Objects

### Source Claim

```json
{
  "claim_type": "SOURCE_CLAIM",
  "text": "",
  "source_pattern_ids": [],
  "source_pointer_ids": [],
  "assumptions": [],
  "uncertainty": ""
}
```

### Derived Claim

```json
{
  "claim_type": "DERIVED_INFERENCE",
  "text": "",
  "premises": [],
  "assumptions": [],
  "uncertainty": ""
}
```

### Transfer Claim

```json
{
  "claim_type": "SPECULATIVE_TRANSFER",
  "text": "",
  "premises": [],
  "assumptions": [],
  "uncertainty": ""
}
```

## Canonical Transfer Card

Cards live in
`arxiv-reference/evidence/constraint-transfer-cards/XFER-<FOUR-WORD-SLUG>.md`
as a Markdown title followed by one fenced JSON object. The JSON object SHALL
contain:

```json
{
  "transfer_id": "XFER-BOUND-ACTIVE-WORKING-SET",
  "name": "Bound Active Working Set",
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "source_pattern_ids": [],
  "original_domain": "",
  "original_constraint_profile": {
    "constrained_resource": {},
    "access_medium": {},
    "predictability_requirement": {},
    "data_mutability": {},
    "communication_model": {},
    "original_hardware_operating_assumptions": {}
  },
  "original_constraints": [],
  "original_cost_model": {},
  "surviving_invariant": {},
  "reversed_assumptions": [],
  "modern_knight_bus_constraints": [],
  "proposed_transfer": {},
  "modern_resource_model": {
    "ram": {},
    "io": {},
    "preprocessing": {},
    "storage": {},
    "concurrency": {}
  },
  "unknown_measurement_constants": [],
  "g06_challenges": [],
  "analogy_failure_modes": [],
  "target_algorithm_families": [],
  "smallest_falsifier": {},
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-BOUND-ACTIVE-WORKING-SET"
}
```

Every resource-model value SHALL contain:

```json
{
  "claim_type": "DERIVED_INFERENCE",
  "expression": "",
  "variables": [],
  "unknown_constants": [],
  "measurement_needed": "",
  "assumptions": [],
  "uncertainty": ""
}
```

An expression must be symbolic and unit-interpretable. `UNKNOWN`, an empty
expression, a historical ratio, or a number without variables and conditions is
not a modern resource model.

Every `g06_challenges` item SHALL name a resolvable failure-card ID, state
whether it applies, and record the resulting guard, fallback, narrowing, or
rejection. Every `smallest_falsifier` SHALL specify fixture, controlled
variables, independent oracle, and failure signal.

## Reserved G09 Forward References

G07 reserves one falsifier ID per canonical transfer using
`RESERVED-G09-FOR-<TRANSFER_ID>`. It is deliberately unresolved during G07.
G09 SHALL either replace the reservation with one canonical experiment packet
ID or retire the transfer before G10. No other unresolved ID is permitted.

## Plan Schema

`g07-transfer-plan.tsv` uses this exact header:

```text
selection_rank	lane_id	lane_position	pattern_id	source_paper_ids	source_domain	selection_score	selection_basis	reader_agent_id	reviewer_agent_id	inspection_status	terminal_disposition	transfer_ids	evidence_gap	measurement_needed	result_checksum
```

During active reading, `inspection_status` may be `FROZEN`, `READ_COMPLETE`,
`NORMALIZED`, or `CHALLENGED`; terminal closure requires `COMPLETE`. Blank
terminal fields and checksums are allowed only before closure.

The result checksum is uppercase SHA-256 over the normalized plan row plus the
canonical transfer JSON records it references and the relevant source
mechanism/failure-card checksums.

## Lane Dossier Contract

Each reader returns one machine-readable dossier containing exactly its five
assigned patterns. For every pattern it records:

1. original constraint profile;
2. surviving invariant or reason none survives;
3. reversed historical assumptions;
4. modern constraints and symbolic resources;
5. proposed transfer or terminal rejection;
6. all linked G06 challenges and repairs;
7. target algorithms and analogy failures;
8. smallest falsifier;
9. recommended terminal disposition.

Lane dossiers are working evidence. Only normalized cards, the terminal plan,
report, review, and journal are canonical G07 artifacts.

## Phase Gates

1. **STUB:** entry gate, packet, frozen plan, schema, focused contract tests.
2. **RED:** tests reject malformed lane counts, missing invariants, unsymbolic
   resources, unresolved G06 challenges, and invalid dispositions.
3. **GREEN:** four lanes finish; normalized cards and plan pass local checks.
4. **CHALLENGE:** all linked G06 failures are applied and contradictions closed.
5. **REVIEW:** independent review records severity findings and repair status.
6. **CLOSE:** shared validator passes; status and navigation agree; recommend
   G08 and stop.

## Traceability

| Requirement | Test |
|---|---|
| REQ-G07-ENTRY-001 | TEST-G07-ENTRY-001 |
| REQ-G07-FREEZE-002 | TEST-G07-FREEZE-002 |
| REQ-G07-DISPOSITION-003 | TEST-G07-DISPOSITION-003 |
| REQ-G07-CARD-004 | TEST-G07-CARD-004 |
| REQ-G07-COST-005 | TEST-G07-COST-005 |
| REQ-G07-EVIDENCE-006 | TEST-G07-EVIDENCE-006 |
| REQ-G07-CHALLENGE-007 | TEST-G07-CHALLENGE-007 |
| REQ-G07-REVIEW-008 | TEST-G07-REVIEW-008 |
| REQ-TIME-001.0 | TEST-TIME-001 |
| REQ-TIME-002.0 | TEST-TIME-002 |
| REQ-TIME-003.0 | TEST-TIME-003 |
| REQ-TIME-004.0 | TEST-TIME-004 |

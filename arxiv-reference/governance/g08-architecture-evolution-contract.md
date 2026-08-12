# G08 Architecture Evolution Contract

## Purpose

This contract makes G08 a falsifiable architecture portfolio rather than an
idea list. The canonical machine-readable record for each candidate is the one
JSON object embedded in its Markdown card. The executable schema lives in
`arxiv-reference/tools/g08_architecture_evolution_pipeline.py` and is protected
by focused tests.

## Requirements

### REQ-G08-ENTRY-001: Admit Frozen G07 Input

**WHEN** G08 semantic generation begins
**THEN** G07 SHALL be complete, verified, and independently cleared
**AND** exactly 20 checksum-backed transfer cards SHALL validate
**SHALL** prohibit mutation of G05-G07 semantic evidence.

### REQ-G08-BUDGET-002: Preserve Exact Diversity Budget

**WHEN** the raw portfolio is frozen
**THEN** it SHALL contain exactly 50 unique candidates: eight in each of six
declared niches and two explicit baselines
**AND** SHALL cover all seven priority workload families
**SHALL** retain dependency/security/access-path traversal as the first wedge.

### REQ-G08-GENOME-003: Encode Traceable Architecture Genomes

**WHEN** a candidate is emitted
**THEN** it SHALL declare its workload, parents, lane, inherited transfers,
topology, ordering, state placement, scheduling, overflow, exactness,
admission, receipt, and compatibility boundary
**AND** SHALL record additions, removals, and intended changed behavior
**SHALL** reject untraceable creative components.

### REQ-G08-RESOURCE-004: Account For Whole-Process Resources

**WHEN** a candidate enters comparison
**THEN** peak RAM SHALL separately include topology, algorithm state, active
set, scratch, output, conversion, page cache/direct-I/O, runtime, spill, safety,
and temporary old/new coexistence terms
**AND** SHALL include symbolic I/O, preprocessing, storage, recomputation, and
concurrency models plus worker/query/partition/stage/I/O multiplicity
**SHALL** retain every unknown coefficient and double-counting risk.

### REQ-G08-PREP-005: Expose Preparation And Amplification

**WHEN** a candidate uses prepared artifacts
**THEN** it SHALL declare build phases, build RAM and I/O, persistent bytes,
temporary bytes, freshness, amortization, and comparison to a shared baseline
**SHALL** reject a runtime-only advantage that hides preparation.

### REQ-G08-CORRECT-006: Bound Semantics And Compatibility

**WHEN** representation, ordering, approximation, concurrency, or recomputation
changes
**THEN** exactness, tolerance, seed, ordering, nondeterminism, oracle, and
refusal rules SHALL be explicit
**AND** Neo4j, Cypher, Bolt, and GDS boundaries SHALL be stated separately
**SHALL** fail closed for unsupported surface behavior.

### REQ-G08-COMPOSE-007: Prove Composition Coherence

**WHEN** multiple transfers are combined
**THEN** the candidate SHALL explain invariant compatibility, overlapping or
disjoint memory, artifact coexistence, access conflicts, fallback composition,
correctness composition, and preparation rationality
**SHALL** reject combinations whose invariant or cost accounting does not
survive.

### REQ-G08-SEPARATE-008: Delay Counterexamples Until Freeze

**WHEN** divergent generation runs
**THEN** raw G06 failure cards SHALL be unavailable to lane authors
**AND** all 50 raw candidates SHALL be byte-frozen before a separate challenger
loads G06
**SHALL** record the freeze identity and post-freeze reviewer in every final
candidate.

### REQ-G08-CHALLENGE-009: Resolve Failure Boundaries

**WHEN** the post-freeze adversarial pass runs
**THEN** each candidate SHALL link applicable family and transfer failures,
record applies/non-applies reasoning, and choose guards, fallback, narrowing,
repair, deferment, merger, or rejection
**SHALL** preserve rejected candidates and their reasons.

### REQ-G08-PARETO-010: Retain Non-Scalar Alternatives

**WHEN** qualitative placement completes
**THEN** all ten mandated axes SHALL be explicit and symbolic uncertainty SHALL
produce `NON_COMPARABLE` where appropriate
**AND** 12-18 Pareto or specialized survivors SHALL be retained
**SHALL** prohibit one unexplained aggregate score or universal winner.

### REQ-G08-FALSIFY-011: Hand Off Verification Loops

**WHEN** a candidate reaches terminal review
**THEN** it SHALL name the smallest G09 falsifier with fixture, baseline,
oracle, metrics, modeled expectation, future threshold slot, and disconfirming
result
**SHALL** reserve rather than execute the G09 experiment.

### REQ-G08-REVIEW-012: Clear Independent Review

**WHEN** synthesis and the Pareto archive are complete
**THEN** one independent non-author SHALL inspect all 50 cards, transfer
lineage, failure responses, archive, and report
**AND** closure SHALL require P0 = P1 = P2 = 0 after bounded repairs.

## Canonical Card Envelope

Candidate cards live at
`arxiv-reference/synthesis/architecture-candidates/ARCH-G08-NNN.md` as a
Markdown title followed by exactly one fenced JSON object. The executable
validator requires these top-level groups:

```text
identity + niche + parent/agent lineage
target_workload_contract
genome + minimum_resident_kernel
resource_model + state_multiplicity + I/O/page-cache policy
preparation_model + temporary coexistence
fallback_ladder + crossover_guards
correctness_and_determinism + compatibility_boundary
composition_review
receipt_fields + estimator_feedback
linked G06 failures + post-freeze challenge responses
loses_when + smallest_g09_falsifier
ten-axis qualitative_pareto
highest stage + terminal disposition + reason
```

Unknown values remain symbolic. `UNKNOWN`, blank expressions, historical
speedup ratios, or omitted whole-process terms fail validation.

## Terminal Dispositions

- `PARETO_SURVIVOR`
- `SPECIALIZED_SURVIVOR`
- `REPAIR_REQUIRED`
- `DEFER_TO_CALIBRATION`
- `REJECTED_BY_COUNTEREXAMPLE`
- `REJECTED_BY_COMPOSITION`
- `DUPLICATE_MERGED`

Every raw candidate remains preserved regardless of terminal disposition.

## Pareto Axes

1. Peak RAM.
2. p99/P100 latency risk.
3. Predictability and enforceability.
4. Preprocessing cost.
5. Persistent storage amplification.
6. Temporary storage peak.
7. Exactness and determinism.
8. Operational complexity.
9. Neo4j adoption friction.
10. Calibration debt.

## Traceability

| Requirement | Focused test or gate |
|---|---|
| REQ-G08-ENTRY-001 | shared validator and goal packet entry gate |
| REQ-G08-BUDGET-002 | exact 50/niche/workload collection validation |
| REQ-G08-GENOME-003 | candidate schema validation |
| REQ-G08-RESOURCE-004 | symbolic whole-process resource tests |
| REQ-G08-PREP-005 | preparation-model schema validation |
| REQ-G08-CORRECT-006 | correctness and compatibility schema validation |
| REQ-G08-COMPOSE-007 | composition-review schema validation |
| REQ-G08-SEPARATE-008 | raw-freeze and challenger metadata validation |
| REQ-G08-CHALLENGE-009 | exact failure-response coverage validation |
| REQ-G08-PARETO-010 | Pareto row count and ten-axis validation |
| REQ-G08-FALSIFY-011 | reserved G09 falsifier lifecycle validation |
| REQ-G08-REVIEW-012 | closure review marker gate |

## Phase Gates

1. **STUB:** packet, journal, plan shell, schema, and focused tests.
2. **RED:** tests fail because the validator and portfolio do not exist.
3. **GREEN:** validator passes its isolated valid/malformed fixtures.
4. **DIVERGE:** six independent lanes create 48 candidates; controller creates
   two baselines; G06 remains withheld.
5. **FREEZE:** all 50 raw bytes, prompts, generators, and checksums freeze.
6. **CHALLENGE:** G06 counterexamples repair, specialize, defer, merge, or
   reject every candidate.
7. **PARETO:** 12-18 qualitative survivors are retained without a scalar score.
8. **REVIEW:** independent non-author reviewer inspects the complete corpus.
9. **CLOSE:** focused checks, shared validator, navigation, status, report, and
   journal agree; recommend G09 and stop.


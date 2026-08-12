# G06 Goal Packet

This packet governs counterexample extraction from the completed G05 evidence
corpus. It authorizes negative-evidence extraction and analytical adversarial
fixtures only. It does not authorize new literature, constraint transfer,
architecture synthesis, experiments, implementation, benchmarking, commit, or
push.

- Goal ID: G06
- Objective: inspect the same 25 `READ_COMPLETE` papers for negative evidence and give every one of the 67 G05 mechanism cards a terminal source-grounded failure, analytical adversarial test, or explicit evidence gap.
- A007 uncertainty reduced: under which graph shapes, workload distributions, memory limits, I/O behavior, update rates, concurrency schedules, and skew conditions do G05 mechanisms fail to support bounded RAM, predictable latency, correctness, or admission safety?
- Inputs: `Arxiv-Pattern-Foundry-SOP.md`, completed G05 status, G05 goal packet and extraction contract, `governance/claim-evidence-policy.md`, `governance/artifact-schema-contracts.md`, `governance/g05-reading-plan.tsv`, `sources/G05-mechanism-extraction-report.md`, `governance/reviews/G05-adversarial-review.md`, all 67 G05 mechanism cards, the 47-row pattern-edge ledger, the same 25 ignored checksum-verified PDFs and extracted texts, and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
- Owned outputs: this packet, `governance/g06-counterexample-contract.md`, `governance/g06-adversarial-plan.tsv`, `evidence/failure-cards/`, `evidence/evidence-conflicts.tsv`, `sources/G06-counterexample-report.md`, `journals/G06-progress.md`, G06 pipeline, tests, shared-validator extensions, final independent review, campaign status, README, and Markdown-index updates.
- Batch caps: exactly 25 existing papers, exactly 67 existing mechanism cards, exactly 47 existing pattern edges, five disjoint mechanism lanes, 92 terminal plan subjects, zero external requests, zero added paper identities, zero repository acquisitions, and no explicit token cap.
- Excluded work: paper discovery or download, new identities, repository acquisition, G07 transfer cards, G08 architectures or Pareto records, G09 experiment packets, Knight Bus implementation, benchmarks, product RAM or latency estimates, commit, push, and starting G07.
- Entry tests: G05 is `COMPLETE`, `VERIFIED`, and `CLEARED`; exactly 25 manifest rows are `READ_COMPLETE`; exactly 67 cards and 47 edges validate; the G05 review has P0/P1/P2 all zero; the existing 158-test suite and corpus validator pass.
- Exit tests: all 25 papers have complete-page negative-evidence dispositions; all 67 patterns have terminal adversarial dispositions; every failure, conflict, pointer, fixture, foreign key, duplicate signature, and checksum validates; no later-goal or external-request artifact exists; all G00-G06 tests and the full validator pass; an independent reviewer reports P0=0, P1=0, and P2=0.
- Stop conditions: local checksum drift, unverifiable pointer, unsupported numeric breakpoint, hidden analytical premise, missing terminal disposition, architecture-dependent failure premise, later-goal artifact, external request requirement, conflicting edits, or unresolved P0/P1/P2 finding.
- Journal: `arxiv-reference/journals/G06-progress.md`.

## Frozen Subject Set

G06 SHALL NOT select literature. Its paper set is exactly the 25 rows in
`governance/g05-reading-plan.tsv`, whose committed manifest states are
`READ_COMPLETE`. Its mechanism set is exactly the 67 canonical
`PAT-<FOUR-WORD-SLUG>.md` files that passed G05 review. Its relationship input
is exactly the 47 canonical G05 pattern edges.

The adversarial plan has two subject classes:

- 25 `PAPER` rows prove that every PDF page was inspected for reported
  limitations, regressions, sensitivity, workload reversals, scalability
  ceilings, resource amplification, and missing ablations.
- 67 `PATTERN` rows prove that every mechanism assumption received a linked
  failure, a minimal analytical test, or an explicit evidence gap.

No `PAPER` or `PATTERN` subject may occur twice. No subject may be omitted.

## Parallel Ownership

Five read-only semantic lanes own disjoint pattern IDs. Sorted pattern IDs are
assigned round-robin:

```text
lane_number = ((pattern_rank - 1) mod 5) + 1
lane_position = floor((pattern_rank - 1) / 5) + 1
```

Paper inspection follows the existing five G05 paper batches so each of the 25
papers has one primary G06 lane owner. A mechanism lane may revisit relevant
pages in a paper owned by another paper lane, but only its primary mechanism
owner may propose that mechanism's terminal disposition. One controller owns
canonicalization and one separate skeptical reviewer owns clearance.

## Evidence Boundary

G06 may paraphrase source-reported negative evidence with exact pointers and
may derive a minimal counterexample from a G05 mechanism's sourced invariant.
It may not describe a derived fixture as measured, choose a storage
architecture, or turn a failure-card repair option into an architecture
decision. `affected_architecture_ids` is therefore always empty.

G06 creates no G09 experiment. Its fixtures are compact analytical test
descriptions with an independent oracle, controlled variables, varied
variables, and observable failure signal.

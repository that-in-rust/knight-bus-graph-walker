# G00 Goal Packet

This standalone packet carries the completed G00 execution contract and mirrors
the final evidence in `arxiv-reference/journals/G00-progress.md`. The exit tests
below passed for the bounded zero-research scaffold; they do not validate any
future literature, evidence-card, architecture, or product-performance claim.

- Goal ID: G00
- Objective: Initialize the minimum campaign scaffold so an empty evidence corpus passes deterministic contract validation.
- A007 uncertainty reduced: Does a stricter contract - a hard budget, bounded execution, and a receipt - change behavior for a narrow security/dependency/access-path segment? G00 only enables later evidence collection; it records `NO_DECISION_IMPACT` and does not answer this product question.
- Inputs: `arxiv-reference/Arxiv-Pattern-Foundry-SOP.md`, `docs_PRD04/A007-spc-founder-interview-prep-v7.md`, `docs_PRD04/reference-learning/neo4j-compat-lowram/current-implementation-gap-ledger.md`, and `Markdown-Value-Index.md`.
- Owned outputs: `arxiv-reference/README.md`, `arxiv-reference/governance/G00-goal-packet.md` (this packet), `arxiv-reference/governance/G00-generation-ledger.md`, `arxiv-reference/governance/artifact-schema-contracts.md`, `arxiv-reference/governance/campaign-status.md`, `arxiv-reference/governance/claim-evidence-policy.md`, `arxiv-reference/governance/source-service-policy.md`, `arxiv-reference/.gitignore`, `arxiv-reference/tools/validate_arxiv_corpus_contract.py`, `arxiv-reference/tests/test_validate_arxiv_corpus_contract.py`, `arxiv-reference/journals/G00-progress.md`, and the corresponding `Markdown-Value-Index.md` entries.
- Batch caps: one scaffold; zero external queries; zero papers; zero evidence cards; zero architecture candidates; no explicit token cap supplied.
- Excluded work: G01-G10; internet or citation-service calls; PDF acquisition; paper reading; pattern extraction; architecture generation; benchmarks; production algorithm implementation.
- Entry tests: six RED cycles were recorded. The first ran seven validator-contract tests, with six failures while the structural REQ-to-TEST link audit passed. The second ran 11 tests with four failing behaviors. The third ran 14 tests with three failing behaviors and five failure assertions. The fourth ran 16 tests with two failing behaviors and eight failure assertions. The fifth ran six targeted integrity tests with nine failing assertions and zero errors. The sixth ran one provenance test with three failing subtests. Every preserved regression is green in the final 23-test suite.
- Exit tests: all G00 unit tests pass; the full corpus validator exits zero; Git confirms no tracked or staged PDF; `git diff --check` passes; only declared G00 and pre-existing user files differ.
- Stop conditions: any need to query a source service, download full text, expand beyond one scaffold, overwrite user changes, weaken a validator to accept output, or make an unsupported product/performance claim.
- Journal: `arxiv-reference/journals/G00-progress.md`.

The batch cap permits zero research artifacts: zero query executions, paper
records, downloads, paper reads, evidence cards, architecture candidates, and
experiments. `SOURCE_CLAIM`, `DERIVED_INFERENCE`, and `SPECULATIVE_TRANSFER`
artifacts are therefore outside G00; later goals must apply those labels
explicitly under the SOP.

## G00 Requirement Closure Matrix

G00 is `COMPLETE` and `VERIFIED`. The SOP defines 49 REQ-to-TEST links. The
final G00 suite has 23 passing tests, including malformed-ledger, cache,
active-goal, symlink, unsafe-path, tracked-PDF, and generation-provenance
regressions. The full corpus validator and Git/license gates pass.
`test_traceability_links_pass` checks only that the 49 identifiers are linked
one-to-one in the SOP. It does not implement or execute 49 behavioral tests.
The 41 future record-level requirements remain explicitly non-passing and owned
by later authorized goals.

### Requirements Applicable To G00

| REQ ID | SOP TEST ID | Local evidence or command | Current status |
|---|---|---|---|
| `REQ-GOV-001.0` | `TEST-GOV-001` | This packet names A007, the gap ledger, the SOP, and the exact product uncertainty. | `PASS` |
| `REQ-GOV-004.0` | `TEST-GOV-004` | `governance/campaign-status.md` and the final journal handoff record every required yield count as zero and decision impact as `NO_DECISION_IMPACT`. | `PASS` |
| `REQ-GOAL-001.0` | `TEST-GOAL-001` | This packet names exactly `G00` and declares exact inputs, outputs, caps, exclusions, and exit tests; `test_empty_corpus_passes` passes in the final 23-test run. | `PASS` |
| `REQ-GOAL-002.0` | `TEST-GOAL-002` | `journals/G00-progress.md` contains timestamped phase checkpoints, exact failures, implementation state, metrics, and non-empty next steps. | `PASS` |
| `REQ-GOAL-003.0` | `TEST-GOAL-003` | Every interrupted repair resumed from the latest journal, campaign status, owned files, and unresolved tests without repeating research calls or paper reads. | `PASS` |
| `REQ-GOAL-005.0` | `TEST-GOAL-005` | The final journal handoff reports phase, artifacts, tests, blockers, unresolved decisions, and three next steps; it recommends G01 without starting it. | `PASS` |
| `REQ-GOAL-006.0` | `TEST-GOAL-006` | The generation ledger records 14 write agents, read-only reviewers, exact writer prompts, bounded timestamps, runtime limits, and verified output checksums; malformed and path-escape ledger tests pass. | `PASS` |
| `REQ-LEGAL-001.0` | `TEST-LEGAL-001` | Ignore-policy, tracked-PDF, repository-wide PDF, cache-payload, and Git PDF gates pass with no tracked or staged full text. | `PASS` |

### Future Record-Level Requirements

`NOT_APPLICABLE_ZERO_CORPUS` means the SOP trigger has no record or artifact in
G00. `DEFERRED_TO_G0X` means the named later goal owns the first authorized
implementation and evidence. Neither classification counts as a passing G00
behavioral test.

| REQ IDs | SOP TEST IDs | Classification | First authorized goal or boundary |
|---|---|---|---|
| `REQ-GOV-002.0` | `TEST-GOV-002` | `DEFERRED_TO_G01` | Architecture questions and query-family links begin in `G01`. |
| `REQ-GOV-003.0` | `TEST-GOV-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Apply when later goals create claim-bearing research or synthesis artifacts. |
| `REQ-DISC-001.0` | `TEST-DISC-001` | `DEFERRED_TO_G01` | Repository-derived terminology begins in `G01`. |
| `REQ-DISC-002.0` | `TEST-DISC-002` | `DEFERRED_TO_G01` | Taxonomy expansion begins in `G01`. |
| `REQ-DISC-003.0` | `TEST-DISC-003` | `DEFERRED_TO_G01` | Planned compound query families begin in `G01`. |
| `REQ-DISC-004.0` | `TEST-DISC-004` | `DEFERRED_TO_G02` | Query execution across eras begins in `G02`. |
| `REQ-DISC-005.0` | `TEST-DISC-005` | `DEFERRED_TO_G02` | Paper-identity deduplication begins in `G02`. |
| `REQ-DISC-006.0` | `TEST-DISC-006` | `DEFERRED_TO_G02` | Architectural-yield ranking begins in `G02`. |
| `REQ-ACQ-001.0` | `TEST-ACQ-001` | `DEFERRED_TO_G03` | Backward citation traversal begins in `G03`. |
| `REQ-ACQ-002.0` | `TEST-ACQ-002` | `DEFERRED_TO_G03` | Forward citation traversal begins in `G03`. |
| `REQ-ACQ-003.0` | `TEST-ACQ-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Selected full-text acquisition first becomes authorized in `G04`. |
| `REQ-ACQ-004.0` | `TEST-ACQ-004` | `NOT_APPLICABLE_ZERO_CORPUS` | Source-service use first becomes authorized by a later bounded goal, beginning no earlier than `G02`/`G04`. |
| `REQ-ACQ-005.0` | `TEST-ACQ-005` | `NOT_APPLICABLE_ZERO_CORPUS` | Unavailable-source records first become authorized in `G04`. |
| `REQ-PAT-001.0` | `TEST-PAT-001` | `NOT_APPLICABLE_ZERO_CORPUS` | Mechanism extraction first becomes authorized in `G05`. |
| `REQ-PAT-002.0` | `TEST-PAT-002` | `NOT_APPLICABLE_ZERO_CORPUS` | Grounded mechanism records first become authorized in `G05`. |
| `REQ-PAT-003.0` | `TEST-PAT-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Mechanism resource consequences first become authorized in `G05`. |
| `REQ-PAT-004.0` | `TEST-PAT-004` | `NOT_APPLICABLE_ZERO_CORPUS` | Mechanism applicability records first become authorized in `G05`. |
| `REQ-PAT-005.0` | `TEST-PAT-005` | `NOT_APPLICABLE_ZERO_CORPUS` | Typed pattern relationships first become authorized in `G05`. |
| `REQ-PAT-006.0` | `TEST-PAT-006` | `NOT_APPLICABLE_ZERO_CORPUS` | Evidence-graded patterns first become authorized in `G05`. |
| `REQ-FAIL-001.0` | `TEST-FAIL-001` | `NOT_APPLICABLE_ZERO_CORPUS` | Failure-card extraction first becomes authorized in `G06`. |
| `REQ-FAIL-002.0` | `TEST-FAIL-002` | `NOT_APPLICABLE_ZERO_CORPUS` | Adversarial workloads first become authorized in `G06`. |
| `REQ-FAIL-003.0` | `TEST-FAIL-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Candidate dispositions first become authorized in `G06`. |
| `REQ-FAIL-004.0` | `TEST-FAIL-004` | `DEFERRED_TO_G08` | Divergent candidate generation and delayed adversarial review begin in `G08`. |
| `REQ-TIME-001.0` | `TEST-TIME-001` | `NOT_APPLICABLE_ZERO_CORPUS` | Constraint-indexed source records first become authorized in `G07`. |
| `REQ-TIME-002.0` | `TEST-TIME-002` | `NOT_APPLICABLE_ZERO_CORPUS` | Constraint-transfer invariants first become authorized in `G07`. |
| `REQ-TIME-003.0` | `TEST-TIME-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Modern transfer cost models first become authorized in `G07`. |
| `REQ-TIME-004.0` | `TEST-TIME-004` | `NOT_APPLICABLE_ZERO_CORPUS` | Distant-mechanism retrieval first becomes authorized in `G07`. |
| `REQ-EVOL-001.0` | `TEST-EVOL-001` | `DEFERRED_TO_G08` | Architecture genomes begin in `G08`. |
| `REQ-EVOL-002.0` | `TEST-EVOL-002` | `DEFERRED_TO_G08` | Pareto niche preservation begins in `G08`. |
| `REQ-EVOL-003.0` | `TEST-EVOL-003` | `DEFERRED_TO_G08` | Candidate variation lineage begins in `G08`. |
| `REQ-EVOL-004.0` | `TEST-EVOL-004` | `DEFERRED_TO_G08` | Staged candidate evaluators begin in `G08`. |
| `REQ-EVOL-005.0` | `TEST-EVOL-005` | `DEFERRED_TO_G08` | Holdout workloads begin in `G08`. |
| `REQ-ARCH-001.0` | `TEST-ARCH-001` | `NOT_APPLICABLE_ZERO_CORPUS` | Workload-linked candidates first become authorized in `G08`. |
| `REQ-ARCH-002.0` | `TEST-ARCH-002` | `NOT_APPLICABLE_ZERO_CORPUS` | Candidate resource equations first become authorized in `G08`. |
| `REQ-ARCH-003.0` | `TEST-ARCH-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Candidate preparation and amplification terms first become authorized in `G08`. |
| `REQ-ARCH-004.0` | `TEST-ARCH-004` | `NOT_APPLICABLE_ZERO_CORPUS` | Candidate correctness and determinism contracts first become authorized in `G08`. |
| `REQ-ARCH-005.0` | `TEST-ARCH-005` | `NOT_APPLICABLE_ZERO_CORPUS` | Candidate failure boundaries first become authorized in `G08`. |
| `REQ-ARCH-006.0` | `TEST-ARCH-006` | `DEFERRED_TO_G09` | Decision-atlas finalists and falsifying experiment packets begin in `G09`. |
| `REQ-GOAL-004.0` | `TEST-GOAL-004` | `NOT_APPLICABLE_ZERO_CORPUS` | Card-budgeted candidate retrieval first applies during `G08`/`G09`. |
| `REQ-LEGAL-002.0` | `TEST-LEGAL-002` | `NOT_APPLICABLE_ZERO_CORPUS` | License-bearing acquisition records first become authorized in `G04`. |
| `REQ-LEGAL-003.0` | `TEST-LEGAL-003` | `NOT_APPLICABLE_ZERO_CORPUS` | Evidence-card text first becomes authorized in `G05` and later evidence goals. |

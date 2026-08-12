# G06 Independent Skeptical Review v3

- Reviewer agent ID: `019ff4ec-552b-7a10-bab1-0a7742bae998`
- Review date: `2026-08-12`
- Repository: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`
- Reviewed branch: `ideation_20260525`, tracking `origin/ideation_20260525`
- Mode: read-only, local-only, no network request, no repository edit
- Prior reviews rechecked: `/tmp/g06-independent-review.md` and `/tmp/g06-independent-review-v2.md`
- Final verdict: `CLEARED`

## Review Scope And Method

This is a fresh review of current repository bytes after the repairs to
`G06-R2-001` and `G06-R2-002`. The repair summary was not treated as evidence.
The review read the current cards, frozen local PDFs, plan, report, conflict
ledger, contracts, validator, tests, campaign state, and Git state directly.

The review independently:

1. rebound both repaired cards to their checksum-bound local PDFs;
2. visually inspected and text-extracted `PAPER-2112.00098` pages 9, 12, and 13;
3. visually inspected and text-extracted `PAPER-2101.12631` pages 10 and 20;
4. audited every primary field in both repaired cards for source scope or
   explicit derived provenance;
5. parsed all 79 current failure-card JSON envelopes and all 174 pointers;
6. recomputed all card schemas, filenames, page bounds, claim references,
   derived provenance, breakpoints, fixtures, oracles, repair classes, and
   no-later-goal fields;
7. recomputed all 67 card-to-pattern and pattern-to-card reciprocal links and
   derived each pattern disposition from the linked card bases;
8. recomputed the 25-paper, 427-page, 67-mechanism, 47-edge frozen corpus and
   all seven frozen fingerprints;
9. rebound all 25 local PDFs and all 25 extracted texts to their G05 checksums;
10. compared the report's complete 67-row matrix and accounting with the plan
    and cards;
11. rechecked both semantic merge aliases, exact duplicate signatures, the
    empty conflict ledger, A007 handoff, and scope boundary;
12. ran the complete test suite, production corpus validator, Git/full-text
    gates, whitespace gate, later-goal gate, and an independent license check;
13. rechecked every v1 and v2 finding against current bytes; and
14. inspected current Git status before and after the review.

No web, browser, HTTP client, package installer, Git fetch/pull/push, or other
network operation was used. Temporary rendered pages and audit code were
created only under `/tmp`.

## Executive Verdict

The repaired G06 corpus is cleared. No unresolved P0, P1, or P2 finding
remains.

The two v2 blockers are resolved without changing the canonical card count or
the 67-pattern decision surface:

- The aging-query card now has exact body pointers for normal query semantics,
  query reauthorization after aging, and measured query unavailability during
  aging. Its constructed workload, fixture, breakpoint, and confidence remain
  explicitly derived.
- The bounded-frontier card now labels the narrow-bridge causal route as
  `DERIVED_INFERENCE`, with nonempty premises, assumptions, and uncertainty.
  Its card-level `SOURCE_REPORTED` basis is justified only by the measured
  recall ceiling on page 10; the paper is not made to claim the constructed
  causal route.

The repair adds two valid pointers to the aging card. Therefore the current
pointer count is 174, not the 172 recorded by v2. The exact delta is one
`SECTION` pointer and one `THEOREM` pointer.

## Independently Recomputed Accounting

| Measure | Recomputed result | Verdict |
|---|---:|---|
| Manifest rows | 377 | PASS |
| G05 selected papers | 25 | PASS |
| READ_COMPLETE pages | 427 | PASS |
| G05 mechanism cards | 67 | PASS |
| G05 pattern edges | 47 | PASS |
| Metadata request rows | 191 | PASS |
| Citation request rows | 83 | PASS |
| Download request rows | 50 | PASS |
| G06 request rows | 0 | PASS |
| G06 plan rows | 92 = 25 PAPER + 67 PATTERN | PASS |
| Completed paper rows | 25 `NEGATIVE_EVIDENCE_EXTRACTED` | PASS |
| Completed pattern rows | 35 source + 32 analytical | PASS |
| Pages covered by paper rows | 427 | PASS |
| Canonical failure cards | 79 | PASS |
| `SOURCE_REPORTED` | 41 | PASS |
| `SOURCE_SUPPORTED_DERIVATION` | 20 | PASS |
| `ANALYTICAL_COUNTEREXAMPLE` | 18 | PASS |
| Card-level `SOURCE_CLAIM` | 41 | PASS |
| Card-level `DERIVED_INFERENCE` | 38 | PASS |
| Source pointers | 174 | PASS |
| Symbolic breakpoint expressions | 78 | PASS |
| Unknown breakpoint expressions | 1 | PASS |
| Numeric breakpoint constants | 0 | PASS |
| Graph-and-execution fixtures | 54 | PASS |
| Graph fixtures | 13 | PASS |
| Execution-profile fixtures | 12 | PASS |
| Exact lexical duplicate groups | 0 | PASS |
| Semantic retirements | 2 | PASS |
| Evidence conflicts | 0 | PASS |
| External requests during G06 | 0 | PASS |
| Later-goal artifacts | 0 | PASS |

### Frozen Input Fingerprints

| Input | Rows | Recomputed SHA-256 | Match |
|---|---:|---|---|
| `paper-manifest.tsv` | 377 | `ac6dd076cf65b3ec8e6addc45b90111cb0ab4f14fe44f71d4c6e1cda4b8f3bfc` | YES |
| `g05-reading-plan.tsv` | 25 | `b8e942272218ecee670b97fdea601c802a2705505bef352b0c644a5d00f53c3f` | YES |
| `pattern-edges.tsv` | 47 | `df677bdaca319de644d2f89ef6025bebd52ddac16d2c44dbe27fd3619719855e` | YES |
| mechanism-card aggregate | 67 | `1fb0b8e4e63a09c764cf8b5ff6b4de4c113e8fa843c12c296eed459d5f1a82d9` | YES |
| metadata requests | 191 | `29ab0c268a7e07931832cc43aff917cacb289058239df443e06f7de44cfa1718` | YES |
| citation requests | 83 | `da8a5ebaa536c2fc221a85fe48e537a319fcfac8142bacea05181317a9a223d7` | YES |
| download requests | 50 | `b5249dbbfed3b272fe01e9b6b4bb18eb41488470e2a69e9e89fa9918b3e2f337` | YES |

All 25 selected local PDFs and all 25 extracted texts match their G05 row
checksums. The current 79-card aggregate is
`07a3567ad72777e1f3ace9b2301a9246bf62859e5e182d5e1910e7acda0af631`.

## G06-R2-001 Source Reinspection

Card: `FAIL-AGING-SUSPENDS-STREAM-QUERIES`

Current card SHA-256:
`e8d47c27b2ae982c5830eb1b2636bd8e0d0f2cb8426e36ed38a300b5ba993b33`

Source PDF: `PAPER-2112.00098`

Expected and recomputed PDF SHA-256:
`9ec995d597c94e687f3a445dd22351b30bc5ac1cfa3983e1c42846a2c0ad0060`

### Exact Page Support

| Pointer | Local page and locator | Source fact independently observed | Verdict |
|---|---|---|---|
| `FP-001` | p9, Section 4.2 and Theorem 1 | A connectivity query enters the same ordered stream as edges, is answered for graph state at arrival tick, and is a normal constant query. | PASS |
| `FP-002` | p12, Section 5.2 and Theorem 3 | The theorem says aging receives a command at `t`, reauthorizes queries at `t'`, and the proof says the tail enables queries after the loading token exits. | PASS |
| `FP-003` | p13, Section 6 and Definition 11 | Parameter `d` is the percentage of X-Stream ticks unavailable for queries due to aging. | PASS |

The three pages were inspected as rendered PDF pages, not only through flattened
text. Page 9 establishes the normal ordered, point-in-time, constant-query
path. Pages 12 and 13 establish the aging exclusion and reauthorization
boundary that was missing in v2.

### Primary Field Audit

| Field | Current claim type | Pointer basis | Provenance verdict |
|---|---|---|---|
| `broken_assumption` | `SOURCE_CLAIM` | `FP-001`, `FP-002`, `FP-003` | PASS. The normal query path is directly sourced; aging pages bound its availability. |
| `triggering_workload` | `DERIVED_INFERENCE` | `FP-002`, `FP-003` | PASS. Query arrival during an unavailable aging tick is a constructed workload with nonempty premise, assumption, and wrapper-queue uncertainty. |
| `observable_symptom` | `SOURCE_CLAIM` | `FP-002`, `FP-003` | PASS. Query unavailability during aging and reauthorization afterward are explicit source statements. |
| `expected_failure_signal` | `SOURCE_CLAIM` | `FP-002`, `FP-003` | PASS. A query unavailable during aging is not answered through the paper's normal query protocol in that interval. |
| `adversarial_fixture` | `DERIVED_INFERENCE` | `FP-002`, `FP-003` | PASS. The before/during-aging comparison, oracle, variables, premise, assumption, and queueing uncertainty are explicitly analytical. |
| `breakpoint_equation` | `DERIVED_INFERENCE` | `FP-002`, `FP-003` | PASS. `query_arrival_mode = aging_mode` is symbolic, contains no invented numeric threshold, and names the missing queuing measurement. |
| `confidence_rationale` | `DERIVED_INFERENCE` | `FP-002`, `FP-003` | PASS. It distinguishes source-reported mode exclusion from unmeasured queueing latency and semantics. |

The card does not claim that a production wrapper necessarily rejects the
query. It explicitly permits queuing outside the ring as an uncertainty. The
fixture's broader blocked/rejected/undefined-semantics observation is derived,
not attributed to the paper.

Verdict for `G06-R2-001`: `RESOLVED`.

## G06-R2-002 Source Reinspection

Card: `FAIL-BOUNDED-FRONTIER-MISSES-NEAREST`

Current card SHA-256:
`a49eda1c48cc0a6c884a5018660e06651329d83be717aa2e5cc098d95b5ef1a6`

Source PDF: `PAPER-2101.12631`

Expected and recomputed PDF SHA-256:
`d9a6f52d17107b182e567baedd3546f0d65bc5a01b547ba814ec7900b1e1c344`

### Exact Page Support

| Pointer | Local page and locator | Source fact independently observed | Verdict |
|---|---|---|---|
| `FP-001` | p10, Section 5.3, Candidate Set Size and Query Path Length | Candidate-set size is dataset/algorithm dependent; some algorithms reach a recall ceiling before target recall, and recall then hardly changes as candidate size increases. The page also relates path length to external-storage I/O. | PASS |
| `FP-002` | p20, Algorithm 1 | The bounded search retains candidate set `C`; while `|C| > c`, it removes the farthest candidate from the query. | PASS |

Page 10 supports a measured negative result. Page 20 supports only the bounded
eviction mechanism. Neither page says that a temporarily farther bridge route
caused the measured ceiling.

### Primary Field Audit

| Field | Current claim type | Pointer basis | Provenance verdict |
|---|---|---|---|
| `broken_assumption` | `SOURCE_CLAIM` | `FP-001` | PASS. A fixed candidate capacity is not a universal target-recall guarantee because a measured ceiling exists. |
| `triggering_workload` | `DERIVED_INFERENCE` | `FP-001`, `FP-002` | PASS. The narrow-bridge route is explicitly constructed, with nonempty premise, assumption, and uncertainty stating that the source does not establish this causality. |
| `observable_symptom` | `SOURCE_CLAIM` | `FP-001` | PASS. Failure to attain requested recall after increasing candidate size is source-reported; no route cause is asserted. |
| `expected_failure_signal` | `DERIVED_INFERENCE` | `FP-001`, `FP-002` | PASS. Difference from exhaustive exact search is a fixture oracle result, not a paper measurement. |
| `adversarial_fixture` | `DERIVED_INFERENCE` | `FP-001`, `FP-002` | PASS. The bridge trap, exhaustive oracle, variables, premise, assumption, and fixture uncertainty are explicit. |
| `breakpoint_equation` | `DERIVED_INFERENCE` | `FP-001`, `FP-002` | PASS. Required capacity is graph/query dependent and no universal numeric capacity is claimed. |
| `confidence_rationale` | `DERIVED_INFERENCE` | `FP-001`, `FP-002` | PASS. It limits confidence to the mechanism premise and negative result and leaves crossover machine/graph dependent. |

The card-level `failure_basis = SOURCE_REPORTED` is justified by the measured
page-10 recall ceiling alone. The page-20 eviction rule and constructed causal
route are premises for derived tests; they are not used to make the paper claim
that the route caused the ceiling.

Verdict for `G06-R2-002`: `RESOLVED`.

## Prior Finding Reinspection

| Prior ID | Current status | Independent current-byte evidence |
|---|---|---|
| `G06-R-001` | RESOLVED | All 79 cards and 67 pattern rows have exact reciprocal links. Removing a card-side inverse is covered by a passing mutation regression. |
| `G06-R-002` | RESOLVED | All 174 pointers use allowed body locator classes; no locator value contains Abstract or Title. Production and focused tests reject both. |
| `G06-R-003` | RESOLVED | The 11 prior relabels remain fixed, and the two later source-scope defects now pass the field-level audits above. |
| `G06-R-004` | RESOLVED | All 67 dispositions agree with linked card bases: 35 source and 32 analytical. |
| `G06-R-005` | RESOLVED | Two semantic duplicates remain retired, canonical targets exist, valid body provenance is retained, and retired IDs are absent from cards and plan links. |
| `G06-R-006` | RESOLVED | Report counts are 79 cards and 174 pointers with 41/20/18 classes; all 67 matrix rows exactly match the plan. |
| `G06-R-007` | RESOLVED | The 203-test suite exercises inverse links, forbidden locators, basis-aware dispositions, semantic aliases, current source repairs, and production report routing. |
| `G06-R2-001` | RESOLVED | Aging-time query unavailability is now supported by p12 and p13 while p9 preserves normal query semantics; constructed fields are honestly derived. |
| `G06-R2-002` | RESOLVED | The causal route trigger is derived with premises, assumptions, and uncertainty; source basis remains limited to the measured ceiling. |

## Structural Card And Pointer Audit

All 79 Markdown files contain exactly one fenced JSON object. Every filename
equals its `failure_id`; every slug has four words; all top-level schemas,
source-paper sets, pointer IDs, page bounds, locator classes, claim references,
affected patterns, fixtures, independent oracles, failure signals, repair
classes, and architecture-ID arrays validate.

Every derived primary field has nonempty premises, assumptions, and
uncertainty. No card uses `SPECULATIVE_TRANSFER`. All breakpoints contain zero
numeric constants; 78 are symbolic and one is explicitly `UNKNOWN` with a
measurement need.

Pointer distribution:

| Locator | Count |
|---|---:|
| `SECTION` | 91 |
| `FIGURE` | 27 |
| `PARAGRAPH` | 22 |
| `ALGORITHM` | 12 |
| `TABLE` | 9 |
| `EQUATION` | 7 |
| `THEOREM` | 5 |
| `APPENDIX` | 1 |
| Total | 174 |

The independent parser reported zero structural errors and zero exact lexical
duplicate signatures.

## Pattern Link And Disposition Audit

- 67 of 67 pattern rows have exact reciprocal failure-card links.
- 79 of 79 cards are linked by every affected pattern and no unrelated pattern.
- 35 rows have at least one `SOURCE_REPORTED` card and use
  `SOURCE_FAILURE_LINKED`.
- 32 rows have only source-supported or analytical cards and use
  `ANALYTICAL_TEST_LINKED`.
- No row has an explicit evidence gap, orphan card, stale retired ID, foreign
  card, invalid source-paper coverage, or basis/disposition mismatch.
- Every paper row uses exact `ALL_PAGES:1-N` coverage; the sum is 427.
- The report matrix contains exactly 67 rows and is an exact semantic copy of
  each plan row's disposition and sorted links.

Reader identities are populated. Reviewer identities and result checksums
remain `PENDING` on all 92 rows, which is the required pre-clearance state.

## Duplicate And Semantic Merge Audit

No pair of canonical cards shares the contract's exact duplicate signature.
The two reviewed semantic aliases remain:

| Retired lane ID | Canonical ID | Current verdict |
|---|---|---|
| `FAIL-BINARY-QUANTIZATION-GEOMETRY-COLLAPSE` | `FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL` | PASS. Canonical p8 Table 11/Section 5.6 and p10 Section 6 preserve the valid geometry, rerank, and recall-collapse provenance; the invalid abstract locator is absent. |
| `FAIL-REORDERING-PREPROCESSING-DOMINATES-TRAVERSAL` | `FAIL-FULL-REORDER-DOMINATES-TRAVERSAL` | PASS. Canonical p1 Section 1 and p10 Section 4.6/Table 4 preserve load-balance, preparation, and end-to-end evidence; the invalid abstract locator is absent. |

Both retired IDs are absent from canonical filenames and plan links. Both
canonical IDs exist. The report merge ledger and production reviewed-alias map
provide an explicit retired-to-canonical resolution, and regression tests bind
that resolution. No materially different trigger, affected mechanism,
observable symptom, or failure boundary was erased.

## Conflict Audit

The conflict ledger is exactly one header line and zero data rows. The current
failure corpus and the two repaired source scopes were rechecked for:

- benchmark disagreement under matching conditions;
- condition reversal asserted under matching conditions;
- bound contradiction;
- incompatible assumptions; and
- applicability disagreement with two resolvable evidence endpoints.

No qualifying two-sided conflict was found. The corpus contains conditioned
limitations, workload reversals, and resource crossovers, but these do not
assert incompatible truths under the same conditions. The report states the
zero-conflict result and limits it to the frozen corpus. The empty ledger is
therefore honest.

## Report And A007 Handoff

The report independently reconciles to 25 papers, 427 pages, 67 mechanisms, 79
cards, 174 pointers, 41/20/18 evidence classes, 78 symbolic breakpoints, one
unknown breakpoint, zero numeric constants, 35/32 pattern dispositions, and
zero conflicts. Its repair-class and fixture counts also reconcile exactly.

The A007 decision yield remains in scope and useful. It converts a size-only
promise into conditional obligations for admission, peak-state accounting,
runtime observation, correctness/approximation verification, and execution
receipts. It creates no architecture, transfer, experiment, implementation,
benchmark, RAM claim, or latency claim.

The report preserves explicit unknowns: no portable numeric crossover,
hardware-specific coefficients remain unmeasured, layouts need reuse/update
amortization tests, analytical fixtures are unexecuted, approximation targets
remain product inputs, and the conflict conclusion is corpus-scoped.

## Validator And Test Outputs

### Full test suite

Command:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s arxiv-reference/tests -p 'test_*.py' -v
```

Result: `Ran 203 tests in 34.443s` and `OK`.

The suite includes current canonical regressions for:

- p12/p13 aging pointers and their use by primary fields;
- derived bounded-frontier causal workload provenance;
- card-to-pattern inverse links;
- forbidden Abstract/Title locator text;
- basis-aware pattern dispositions;
- the 11 prior source-basis relabels;
- the two semantic alias resolutions; and
- full validator routing through the production G06 report validator.

The run emitted only the existing non-blocking `urllib3` LibreSSL warning.

### Full corpus validator

Command:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 \
  arxiv-reference/tools/validate_arxiv_corpus_contract.py \
  --root arxiv-reference
```

Result: `PASS arxiv corpus contract`.

The independent parser separately reached zero errors before this production
validator was trusted.

## Git, Scope, And License Audit

Current branch: `ideation_20260525`, tracking
`origin/ideation_20260525`.

Current G06-owned or G06-shared worktree paths include campaign status, shared
validator/tests, the G06 packet/contract/plan/journal/report, failure cards,
conflict ledger, G06 tests, and G06 pipeline. `AGENTS.md` and `CLAUDE.md` are
unrelated/pre-existing changes. No path is staged. This reviewer did not edit,
stage, commit, push, reset, clean, pull, or otherwise mutate repository state.

Scope and Git gates:

- no G06 row exists in any request ledger;
- all frozen request-ledger and manifest bytes match entry hashes;
- no G07 transfer-card file, G08 architecture/Pareto file, or G09 experiment
  file exists;
- no `ARCH-*`, `XFER-*`, or `EXP-*` identity exists in canonical G06 evidence;
- every `affected_architecture_ids` list is empty;
- no tracked or staged PDF/archive exists;
- selected PDFs and extracted texts are ignored by the expected `.gitignore`
  rules;
- no symlink exists under `arxiv-reference`;
- `git diff --check` passes; and
- Git status was unchanged after the review.

Independent acquisition/license reconciliation found exactly 34 acquired
records. Every record has retrieval URI, UTC access time, source checksum,
local path, matching manifest checksum, matching local bytes, and one valid
license state. All 34 remain `LICENSE_UNKNOWN`. Their local PDF/text bytes are
ignored and untracked.

Reviewer activity: `external_requests=0`, `repository_edits=0`.

## Findings

| ID | Severity | Artifact | Evidence | Required repair |
|---|---|---|---|---|
| None | - | - | No unresolved P0, P1, P2, or P3 finding was identified. | None |

## Residual Limitations

These are non-blocking limitations, not findings:

1. This review establishes integrity only for the frozen 25-paper corpus. It
   does not establish literature completeness.
2. PDF text extraction can flatten equations, tables, and multi-column order.
   The five pages central to the R2 repairs were also visually rendered and
   inspected.
3. G06 executed no fixture and reproduced no source benchmark. Analytical
   counterexamples remain proposed oracle-bearing tests.
4. Structural validators cannot prove semantic source scope. Human page review
   remains necessary when cards or source bytes change.
5. All acquired full-text licenses remain unknown. Clearance does not authorize
   redistribution of the ignored local PDFs or extracted texts.
6. Reviewer IDs and result checksums remain `PENDING` on all 92 plan rows, and
   campaign status remains `IN_PROGRESS`, because the controller must consume
   this independent clearance before binding final checksums and closure state.
7. The controller should perform one no-change finalization pass after binding
   reviewer ID and checksums. That mechanical closure is outside this read-only
   review and must not alter evidence semantics.

## Final Verdict

`CLEARED`

**Unresolved findings: P0=0, P1=0, P2=0.**

G06 is **CLEARED**.

Unresolved finding counts exactly:

- P0=0
- P1=0
- P2=0

```json
{
  "final_verdict": "CLEARED",
  "p0": 0,
  "p1": 0,
  "p2": 0,
  "p3": 0,
  "reviewed_failure_cards": 79,
  "reviewed_source_pointers": 174,
  "pages_accounted": 427,
  "patterns_accounted": 67,
  "external_requests": 0,
  "repository_edits": 0
}
```

## Source-Pointer Audit Log

Every one of the 174 pointers passed envelope, paper, page, locator, foreign-key,
and field-reference validation. `PRIMARY-SCOPE` means the source-reported
card's primary fields were checked against its cited local body pages in the
review sequence. `PREMISE-SCOPE` means the card is derived and its sourced
mechanism premise was checked. `REPAIR-SCOPE` marks the two v3 page-level
reinspections above.

| Failure card | Inspected local locators | Verdict |
|---|---|---|
| `FAIL-ACTIVE-EDGE-PREDICTION-MISS` | PAPER-1905.04264 p4/p7/p10 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-AGING-CAPACITY-ABORTS-INSERTIONS` | PAPER-2112.00098 p12/p16/p20 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-AGING-SURVIVOR-CAPACITY-OVERRUN` | PAPER-2112.00098 p12/p13/p14 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-AGING-SUSPENDS-STREAM-QUERIES` | PAPER-2112.00098 p9 Section 4.2/Theorem 1; p12 Section 5.2/Theorem 3; p13 Section 6/Definition 11 | STRUCT + REPAIR-SCOPE PASS |
| `FAIL-ALLACTIVE-MUTATIONS-AMPLIFY-LOGGING` | PAPER-1905.04264 p4/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-BITMAP-OVERHEAD-OUTWEIGHS-LOCALITY` | PAPER-2503.00430 p4/p5 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-BOUNDED-FRONTIER-MISSES-NEAREST` | PAPER-2101.12631 p10 Section 5.3; p20 Algorithm 1 | STRUCT + REPAIR-SCOPE PASS |
| `FAIL-BOUNDED-TABLE-DROPS-CANDIDATES` | PAPER-2104.09616 p4/p4 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-CACHE-PARTITION-SPILL-REVERSAL` | PAPER-1709.07122 p6/p11 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-CLUSTERED-FILTERS-MISROUTE-PLANS` | PAPER-2605.17992 p6/p11 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-COMPRESSED-FILTER-DROPS-NEIGHBOR` | PAPER-2602.21514 p4/p5/p10 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-CORRELATED-LABEL-FILTER-REVERSAL` | PAPER-2605.17992 p7/p11/p12 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-DECOUPLED-LAYOUT-DOUBLES-READS` | PAPER-2603.01779 p5/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-DENSE-ACTIVITY-OVERLOADS-LISTS` | PAPER-1806.08092 p8/p20/p22 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-DENSE-COLUMN-EXCEEDS-MEMORY` | PAPER-1602.02864 p6/p10 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-DENSE-MOVES-SATURATE-QUEUE` | PAPER-1810.08473 p5/p15 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-DIFFUSE-RESIDUALS-ERASE-SPEEDUP` | PAPER-2104.09616 p3/p5/p6 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-DIVERSITY-PRUNING-CONNECTIVITY-LOSS` | PAPER-2101.12631 p7/p9/p11/p12 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-EARLY-ITERATIONS-PAY-CHECKS` | PAPER-2010.09913 p7/p9/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-EARLY-STOPPING-WEAKENS-GUARANTEES` | PAPER-1810.08473 p5/p10 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-EARLY-THRESHOLD-STOPS-STABILIZATION` | PAPER-1304.4453 p4/p15 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-ESTIMATOR-OMITS-PEAK-OVERHEAD` | PAPER-2603.01779 p12 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-FALSE-NEGATIVES-ESCAPE-VERIFICATION` | PAPER-2605.17992 p4 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-FALSE-POSITIVES-AMPLIFY-WORKLOAD` | PAPER-2605.17992 p4/p11 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-FEEDBACK-LATENCY-TARGET-OVERSHOOT` | PAPER-2605.19335 p7/p9/p15 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-FIXED-ITERATIONS-MISS-CONVERGENCE` | PAPER-1603.01876 p5/p6/p8 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-FRONTIER-HEURISTIC-MISSELECTS-DIRECTION` | PAPER-2503.00430 p2/p3/p4/p5 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-FULL-REORDER-DOMINATES-TRAVERSAL` | PAPER-2012.10026 p1/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-FULL-RING-REJECTS-INGESTION` | PAPER-2112.00098 p12/p19 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-GRAPH-RESIDENCY-ADDS-PASSES` | PAPER-1011.5425 p9 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-HIGH-INDEGREE-DESTROYS-RUNTIME` | PAPER-HASH-c2a6a5317d82ac28 p8/p11/p20 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL` | PAPER-2605.02171 p8/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-INLINE-THRESHOLD-INFLATES-MEMORY` | PAPER-2511.07886 p16/p22 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-INMEMORY-SORT-EXCEEDS-BUDGET` | PAPER-1603.01876 p5 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-INTERLEAVING-LOSES-EXCLUSIVE-OWNERSHIP` | PAPER-1806.08092 p9/p10 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-INTERVAL-LOG-EXCEEDS-MEMORY` | PAPER-1905.04264 p4 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-IOBOUND-UPDATES-EMPTY-STALLS` | PAPER-2605.19335 p4/p12 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-LARGE-FRONTIERS-OVERWHELM-SORTING` | PAPER-2503.00430 p2/p3 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-LARGE-OUTPUTS-SERIALIZE-QUERIES` | PAPER-2112.00098 p21 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-LARGE-PARTITIONS-THRASH-CACHE` | PAPER-1709.07122 p11 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-LATE-PREFETCH-MISSES-REUSE` | PAPER-2605.19335 p7 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-LATE-SETTLEMENT-ERASES-PRUNING` | PAPER-2507.12925 p9/p12 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-LONG-PATHS-REPEAT-SCANS` | PAPER-2507.12925 p8/p13/p20 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-LONG-WALKS-MULTIPLY-SCANS` | PAPER-HASH-0232e71ded2b5c43 p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-NAVIGATION-SAMPLE-MISSES-REGIONS` | PAPER-2602.21514 p5/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-NEIGHBOR-CODES-AMPLIFY-STORAGE` | PAPER-2602.21514 p3/p6/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-NONMONOTONE-OPERATORS-BREAK-PRESERVATION` | PAPER-0708.3259 p6/p7 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-PAGE-SHUFFLE-EXCEEDS-MEMORY` | PAPER-2602.21514 p9/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-PAGE-SHUFFLE-LOSES-UTILITY` | PAPER-2602.21514 p4/p9 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-PAGESEARCH-COMPUTE-IDLES-DEVICE` | PAPER-2602.21514 p9/p12 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-PARTIAL-COARSE-GRAPH-AMPLIFICATION` | PAPER-1304.4453 p5/p8 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-PARTITION-BOUND-SHRINK-STAGNATION` | PAPER-2012.10026 p7/p8/p10 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL` | PAPER-2012.10026 p5/p8 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-PREMATURE-SUBGRID-READS-STALE` | PAPER-HASH-b12240577b20eaad p6/p7 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-PROGRESSIVE-BEAM-IO-EXPANSION` | PAPER-2602.21514 p6/p8/p12 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-QUERY-HEAVY-OVERLAY-CROSSOVER` | PAPER-2603.01779 p6/p11/p12 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-RECENT-SAMPLES-MISS-SHIFTS` | PAPER-2605.19335 p6/p9 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-RECURSION-FANOUT-AMPLIFIES-STATE` | PAPER-HASH-0232e71ded2b5c43 p12/p13/p22 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-RESIDENT-REUSE-STARVES-PRIORITIES` | PAPER-2511.07886 p10/p12 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS` | PAPER-2104.09616 p3/p6 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SAMPLED-PROBE-MISCLASSIFIES-DRIFT` | PAPER-2605.02171 p8/p11/p12 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SATURATED-PIPELINE-AMPLIFIES-CONTENTION` | PAPER-2602.21514 p6/p9/p11 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-SERIAL-DEPENDENCIES-ELIMINATE-OVERLAP` | PAPER-2602.21514 p4/p6 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SINGLE-RESOLUTION-LOSES-ORDERING` | PAPER-1011.5425 p6/p7 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-SPARSE-ROW-ADVANTAGE-VANISHES` | PAPER-1602.02864 p4/p10/p11 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SPECULATIVE-PREFETCH-READ-AMPLIFICATION` | PAPER-2603.01779 p6/p8 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-SPECULATIVE-READS-SATURATE-DEVICE` | PAPER-2602.21514 p9/p11 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-STAGE-MATERIALIZATION-EXCEEDS-MEMORY` | PAPER-1603.01876 p4/p5/p6 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-STATIC-ENTRY-CACHE-SHIFT` | PAPER-2602.21514 p5/p8 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SUBGRID-SKIP-BREAKS-CONVERGENCE` | PAPER-HASH-b12240577b20eaad p4/p5/p18/p20 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SYMMETRIC-SETS-REVERSE-COST` | PAPER-0708.3259 p16 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-SYNCHRONOUS-BARRIERS-DEFEAT-PIPELINE` | PAPER-2511.07886 p11/p21 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-THREAD-VECTORS-EXCEED-BUDGET` | PAPER-1304.4453 p5 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-TIED-RANKS-RESIST-SAMPLING` | PAPER-HASH-0232e71ded2b5c43 p14 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-TIGHT-MEMORY-INCREASES-RUNTIME` | PAPER-2603.01779 p4; PAPER-HASH-b12240577b20eaad p16 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-WEIGHTED-EDGES-NEED-VALUES` | PAPER-2010.09913 p6 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-WIDE-FIELDS-ERASE-PACKING` | PAPER-0708.3259 p10/p11 | STRUCT + PREMISE-SCOPE PASS |
| `FAIL-WRONG-SCATTER-MODE-WASTES` | PAPER-1806.08092 p7/p21 | STRUCT + PRIMARY-SCOPE PASS |
| `FAIL-XOR-STREAM-COMPRESSION-CROSSOVER` | PAPER-HASH-b12240577b20eaad p9/p11/p17 | STRUCT + PREMISE-SCOPE PASS |

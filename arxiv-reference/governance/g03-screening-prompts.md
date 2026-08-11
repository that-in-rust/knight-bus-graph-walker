# G03 Post-Traversal Screening Prompts

**Status:** `FROZEN_BEFORE_RESCREEN`
**Goal:** `G03`
**Frozen:** 2026-08-11
**Model:** `gpt-5.6-sol`
**Reasoning effort:** `xhigh`
**Evidence boundary:** committed metadata and control artifacts only

These four prompts partition screening work by decision surface. Lanes A, B,
and C receive disjoint candidate pools under the exact primary-lane rule below.
Lane D audits accounting and provenance without selecting papers. No lane may
use the network, ignored caches, abstracts, PDFs, paper bodies, repositories,
or make file changes.

## Deterministic Primary-Lane Rule

1. `G03-LANE-C`: normalized title contains `counterexample`, `lower bound`,
   `impossibility`, `limitations`, `intractability`, `resolution limit`,
   `no harder`, `survey`, or `review`.
2. Otherwise `G03-LANE-A`: manifest notes contain `BACKWARD`.
3. Otherwise `G03-LANE-B`: manifest notes contain `FORWARD`.

Every retained ancestry identity SHALL map to exactly one primary lane. Lane A
may nominate 12 identities, Lane B 10, and Lane C 3. The resulting 25 identities
must be canonical, available in the manifest, and distinct. Unselected
identities remain `DEFER`; ambiguous or provider-unavailable identities cannot
be nominated.

## G03-LANE-A Prompt

Read only `paper-manifest.tsv`, `citation-edges.tsv`,
`G03-citation-ancestry-report.md`, `g03-citation-contract.md`, and the 12-AQ
ledger. Apply the frozen primary-lane rule and inspect only Lane A identities.
Recommend exactly 12 canonical identities whose metadata titles most directly
change bounded-RAM storage, external-memory traversal, compression, locality,
PageRank/BFS/path-query semantics, or algorithm-operable representation
decisions. Return a ranked table with exact ID, title, direction, score, AQ
impact, and one metadata-only rationale. Label every judgment
`DERIVED_INFERENCE`. Also list every ambiguous or unavailable Lane A identity.

## G03-LANE-B Prompt

Read the same allowed artifacts. Apply the frozen primary-lane rule and inspect
only Lane B identities. Recommend exactly 10 canonical identities whose titles
most directly signal implementations, evaluations, graph-shaped storage,
compressed execution, I/O-aware scheduling, or compatibility-relevant systems.
Return the same ranked metadata-only table and list ambiguous or unavailable
Lane B identities. Do not infer a semantic edge unless an explicit role token
and exact cited-target title anchor both appear.

## G03-LANE-C Prompt

Read the same allowed artifacts. Apply the frozen primary-lane rule and inspect
only Lane C identities. Recommend exactly 3 canonical identities that most
strongly falsify, bound, survey, or clarify a high-priority A007 decision.
Return the same ranked metadata-only table. Explain why each is a reading
signal rather than a `CONTRADICTS` or `SURVEYS` edge. List generic vocabulary
false positives and all ambiguous or unavailable Lane C identities.

## G03-LANE-D Prompt

Read only the allowed artifacts plus `citation-request-ledger.tsv`,
`citation-stops.tsv`, the G03 tests, campaign status, and journal. Independently
recompute seeds, requests, selected observations, identities, depths,
per-seed/direction quotas, edge types and orientation, exact stops, and
prohibited-artifact boundaries. Do not nominate papers. Return a flat findings
list and a reconciliation table. Distinguish permissible scientific coverage
gaps from contract violations.

## Required Return Envelope

Each lane return SHALL state its lane ID, model, evidence boundary, candidate
count, recommendation count, exact ranked IDs where applicable, findings, and
an explicit confirmation of read-only/offline/no-full-text behavior. The main
agent SHALL normalize each return into a separate immutable result document,
record its SHA-256, and use those checksums in `citation-screening-ledger.tsv`.

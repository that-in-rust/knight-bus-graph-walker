# G03 Goal Packet

This standalone packet governs bounded citation archaeology from the completed
G02 handoff. It authorizes metadata and citation relationships only. It does not
authorize paper reading, full-text acquisition, evidence extraction, architecture
synthesis, experiments, or repository acquisition.

- Goal ID: G03
- Objective: Traverse backward and forward citation ancestry from exactly the 25 G02 seeds, to depth 2 and at most 250 new canonical identities, while retaining only branches that can change one of the 12 open architecture questions.
- A007 uncertainty reduced: Which foundational mechanisms, implementation/evaluation descendants, and contradictory branches deserve scarce G04 reading capacity for the bounded-RAM, predictable-latency A007 wedge?
- Inputs: `arxiv-reference/Arxiv-Pattern-Foundry-SOP.md`, `arxiv-reference/sources/G02-metadata-screening-report.md`, `arxiv-reference/sources/paper-manifest.tsv`, `arxiv-reference/governance/architecture-question-ledger.md`, `arxiv-reference/governance/g03-citation-contract.md`, and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
- Owned outputs: `arxiv-reference/governance/G03-goal-packet.md`, `arxiv-reference/governance/g03-citation-contract.md`, `arxiv-reference/governance/g03-service-preflight.md`, G03 additions to `arxiv-reference/governance/artifact-schema-contracts.md`, `arxiv-reference/governance/g03-screening-prompts.md`, normalized G03 lane results under `arxiv-reference/governance/reviews/`, `arxiv-reference/sources/citation-request-ledger.tsv`, `arxiv-reference/sources/citation-stops.tsv`, `arxiv-reference/sources/citation-screening-ledger.tsv`, `arxiv-reference/sources/citation-edges.tsv`, citation-driven updates to `arxiv-reference/sources/paper-manifest.tsv`, `arxiv-reference/sources/G03-citation-ancestry-report.md`, `arxiv-reference/tools/g03_citation_pipeline.py`, `arxiv-reference/tests/test_validate_g03_citation_contract.py`, G03 fixtures, validator extensions, `arxiv-reference/journals/G03-progress.md`, campaign status, and `Markdown-Value-Index.md`.
- Batch caps: exactly 25 depth-0 seeds; citation depth at most 2; at most 250 new canonical identities; at most 90 external HTTP attempts; at most 6,000 raw metadata observations; one page and 100 results per branch operation; three attempts per retry chain; no explicit token cap.
- Excluded work: PDFs, abstracts, full text, paper reading, evidence extraction, mechanism/failure/transfer cards, architectures, experiments, GitHub acquisition, repository inspection, G04, and any commit or push not separately requested by the user. Commit `327a68c` and its push were separately authorized by the user's explicit `commit and push to origin` instruction; this disclosure does not authorize another commit or push.
- Entry tests: G02 is `COMPLETE` and `VERIFIED`; its 55-test baseline and full validator passed before G03 initialization; exactly 25 unique ordered seeds resolve to existing metadata-only manifest rows; the initial G03 suite failed because `g03_citation_pipeline.py` did not exist.
- Exit tests: all G00-G03 unit tests and the full corpus validator pass; request/cache/identity/edge/depth/cap accounting reconciles; Git, whitespace, PDF/archive/full-text, license, and ignored-cache gates pass; one independent adversarial reviewer clears or durably records every finding.
- Stop conditions: unclear access or licensing; unexpected credential requirement; inability to suppress abstract/full-text fields; HTTP 401/403; persistent 429/5xx/transport failure; malformed or ambiguous seed resolution; request, observation, identity, depth, or branch-cap exhaustion; or work outside the owned outputs.
- Journal: `arxiv-reference/journals/G03-progress.md`.

## Requirement Boundary

G03 implements `REQ-ACQ-001.0`, `REQ-ACQ-002.0`, `REQ-ACQ-004.0`, and the
bibliographic-identity portion of `REQ-ACQ-005.0`. Full-text acquisition status
and source claims remain G04/G05 work. In G03, `UNAVAILABLE` means a
bibliographic identity cannot be resolved from the authorized metadata service;
it never claims that the paper's full text is unavailable.

## Mandatory Handoff

The final report SHALL identify foundational, implementation/evaluation,
contradictory, and stopped branches; map decision impact to `AQ-001` through
`AQ-012`; disclose provider and metadata limitations; and name an exact bounded
G04 acquisition set. It SHALL label title-based relationship classes as
`DERIVED_INFERENCE`, not `SOURCE_CLAIM`.

# G05 Goal Packet

This packet governs semantic mechanism extraction from the bounded G04 corpus.
It authorizes evidence extraction only. It does not authorize architecture
synthesis, failure-card generation, constraint transfer, experiments, network
requests, repository acquisition, commit, or push.

- Goal ID: G05
- Objective: semantically read exactly 25 checksum-verified G04-eligible papers and convert reusable source mechanisms into machine-readable mechanism cards and typed pattern relationships.
- A007 uncertainty reduced: which source-grounded data arrangements, access schedules, and state-placement mechanisms could make Knight Bus resource admission and execution more bounded, and under which explicit workload conditions?
- Inputs: `Arxiv-Pattern-Foundry-SOP.md`, verified G04 status and report, `governance/claim-evidence-policy.md`, `governance/artifact-schema-contracts.md`, `governance/architecture-question-ledger.md`, `sources/download-ledger.tsv`, `sources/paper-manifest.tsv`, ignored checksum-verified G04 PDFs and extracted texts, and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
- Owned outputs: this packet, `governance/g05-mechanism-extraction-contract.md`, `governance/g05-reading-plan.tsv`, `evidence/mechanism-cards/`, `evidence/pattern-edges.tsv`, `sources/G05-mechanism-extraction-report.md`, G05 validator and tests, `journals/G05-progress.md`, G05 review evidence, manifest lifecycle updates, campaign status, README, and Markdown index updates.
- Batch caps: exactly 25 papers selected from exactly 34 G04-eligible acquired-and-parsed papers; five disjoint batches of five; 427 selected PDF pages; no external request; no added paper identity; no explicit token cap.
- Excluded work: abstracts as evidence, metadata-only technical claims, G06 failure cards, G07 transfers, G08 architectures, G09 experiments, repository acquisition, external requests, redownloads, commits, and pushes.
- Entry tests: G04 is `COMPLETE`, `VERIFIED`, and independently `CLEARED`; the G04 ledger deterministically exposes 34 acquired-and-parsed identities; their committed manifest rows are `DEEP_READ`; all local PDF and text checksums match; G05 begins RED because its extraction pipeline is absent.
- Exit tests: exactly 25 selected papers are `READ_COMPLETE` and have one terminal outcome; the remaining nine eligible papers stay `DEEP_READ`; every card, source pointer, resource term, pattern edge, foreign key, and result checksum validates; all G00-G05 tests and the full corpus validator pass; an independent reviewer reports P0=0, P1=0, and P2=0.
- Stop conditions: stop on checksum drift, a paper outside the frozen 25, incomplete full-paper coverage, unverifiable source pointers, unsupported numeric claims, conflicting edits, any network attempt, any later-goal artifact, or an unresolved P0/P1/P2 review finding.
- Journal: `arxiv-reference/journals/G05-progress.md`.

## Frozen Selection

Eligibility requires one terminal G04 ledger row with
`acquisition_status=ACQUIRED` and `parse_status=PARSED`, a matching manifest row
with `selection_status=DEEP_READ`, matching local paths and SHA-256 values, and
checksum-verified local PDF and extracted text.

The deterministic ordering key is:

```text
(-integer(relevance_score), integer(g04_queue_rank), paper_id)
```

The first 25 identities are selected. The final nine remain eligible but are
not read in G05. Selection uses metadata and G04 provenance only; no paper body
was consulted. The selected set covers `AQ-001` through `AQ-012`.

Batch assignment is deterministic round-robin by selection rank:

```text
batch_number = ((selection_rank - 1) mod 5) + 1
batch_position = floor((selection_rank - 1) / 5) + 1
```

This produces exactly five disjoint batches of five papers. Batch page totals
are 104, 75, 89, 81, and 78 pages.

## Evidence Boundary

Every selected paper is read across all extracted pages. A completed paper ends
in exactly one terminal outcome:

- `MECHANISM_EXTRACTED`: one or more validated cards cite that paper.
- `NO_MECHANISM`: no reusable mechanism was found, with complete reading
  coverage and a precise rationale.

`PENDING` is the only nonterminal plan value. It is not a terminal outcome.
`READ_COMPLETE` is forbidden while a row remains `PENDING` or before its result
checksum and reviewer identity are recorded.


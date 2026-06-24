# v003 Implementation Readiness Packet

Date: 2026-06-24

This folder converts the `docs_PRD03` research shelf into implementation
contracts for the Rust Neo4j-compatible rewrite. It is documentation and
readiness work only. It does not add production Rust code.

## PRD Boundary

| plane | role | serving rule |
| --- | --- | --- |
| OLTP storage | Neo4j-shaped transactional source of truth: records, WAL, locks, indexes, recovery | serves OLTP reads and writes |
| Projection Build Store | analytical build/control plane: receipts, dense ids, dictionaries, sorted runs, validation, publication metadata | not user-query serving |
| OLAP snapshot storage | published immutable low-RAM graph, sidecar, result, and model artifacts | serves OLAP/GDS reads only as of a snapshot generation |

The RAM promise is holistic: heap, allocator, RSS, page cache, mmap residency,
direct buffers, duplicate layouts, build scratch, sidecars, result/model
artifacts, spill, retained generations, and algorithm state all count.

## Artifact Index

| artifact | purpose | state | primary evidence |
| --- | --- | --- | --- |
| `V003-Implementation-Readiness-Tracker.tsv` | packet status and next actions | Implemented | this folder |
| `GDS-Procedure-Support-Registry.tsv` | 575-row registry derived from the local GDS inventory | Stubbed | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` |
| `Support-Status-Runtime-Semantics.md` | user-visible semantics for P0/P1/P2/unsupported states | Implemented | registry plus PRD boundary |
| `Neo4j-Compatibility-Canary-Matrix.md` | driver/protocol/procedure compatibility canaries | Stubbed | `gitrefrepo/neo4j-src`, `gitrefrepo/neo4j-gds-src`, driver shelves |
| `OLTP-Record-Store-Rust-Contract.md` | Rust OLTP record-store compatibility invariants | Stubbed | `gitrefrepo/neo4j-src/community/record-storage-engine` |
| `Projection-Build-Store-Physical-Contract.md` | build/control-plane receipt and snapshot-build contract | Implemented | Batch 03, Batch 04 |
| `Snapshot-Publication-State-Machine.md` | staged/valid/published/retired generation lifecycle | Implemented | Batch 04 |
| `Memory-Estimate-Formula-Book.tsv` | formula skeleton for strict RAM decisions | Stubbed | Batch 09, Batch 10 |
| `Cells-Adoption-Falsifier-Plan.md` | when to adopt, postpone, or reject cells/tilehouse | Stubbed | architecture scorecards and NeedsBenchmark thresholds |
| `Artifact-Model-Catalog-Contract.md` | result/model/pipeline artifact identity and lifecycle | Stubbed | GDS model/pipeline catalog evidence |
| `Benchmark-Proof-Plan.md` | fair Neo4j Cypher vs Neo4j GDS vs Knight Bus proof plan | Implemented | Batch 09 and PRD |

## Evidence Vocabulary

| value | meaning |
| --- | --- |
| DirectSource | exact local file path or generated TSV row supports the claim |
| GraphToolAssisted | local graph-code tooling found the source shape, but a human still needs line audit |
| DocsOnly | current repo documentation supports the claim |
| Inference | design conclusion drawn from direct sources |
| Speculation | architectural bet that needs later proof |
| NeedsSource | exact source path or line-level evidence still missing |
| NeedsBenchmark | threshold or performance claim requires a benchmark run |

## TSV Artifact Policy

Strict TSV artifacts intentionally do not embed Markdown sections because that
would break field-count verification. Their PRD-plane mapping, evidence path,
and verification command live in `V003-Implementation-Readiness-Tracker.tsv`.
Where the TSV itself can carry source data without breaking schema, it does:
`GDS-Procedure-Support-Registry.tsv` includes `source_file`, `source_line`, and
`evidence_confidence`, while `Memory-Estimate-Formula-Book.tsv` includes
`measurement_source` and `formula_notes`.

## Verification Commands

```bash
git diff --check -- docs_PRD03/implementation-readiness docs_PRD03/Gap-Closure-Implementation-Plan.md
test -f docs_PRD03/implementation-readiness/README.md
test -f docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv
test -f docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
test -f docs_PRD03/implementation-readiness/Support-Status-Runtime-Semantics.md
test -f docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md
test -f docs_PRD03/implementation-readiness/OLTP-Record-Store-Rust-Contract.md
test -f docs_PRD03/implementation-readiness/Projection-Build-Store-Physical-Contract.md
test -f docs_PRD03/implementation-readiness/Snapshot-Publication-State-Machine.md
test -f docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv
test -f docs_PRD03/implementation-readiness/Cells-Adoption-Falsifier-Plan.md
test -f docs_PRD03/implementation-readiness/Artifact-Model-Catalog-Contract.md
test -f docs_PRD03/implementation-readiness/Benchmark-Proof-Plan.md
src_rows=$(awk 'END{print NR-1}' docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv)
reg_rows=$(awk 'END{print NR-1}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv)
test "$src_rows" = "$reg_rows"
awk -F '\t' 'NR>1 && /MissingEvidence/ {print NR ":" $0}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
awk -F '\t' 'NF != 18 {print NR ":" NF ":" $0}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
awk -F '\t' 'NF != 19 {print NR ":" NF ":" $0}' docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv
```

## Next Action

Use this packet to open the first implementation track only after the remaining
`NeedsSource` and `NeedsBenchmark` rows have been triaged. The first code-facing
track should still be registry and canary infrastructure, not a graph algorithm
kernel.

## Verification Run 2026-06-24

| check | result |
| --- | --- |
| required artifact files exist | pass |
| `git diff --check` for this packet | pass |
| GDS registry row parity with source inventory | pass: 575 source rows, 575 registry rows |
| GDS registry field count | pass: 18 columns |
| GDS registry literal `MissingEvidence` check | pass: zero rows |
| memory formula field count | pass: 19 columns |
| tracker state vocabulary | pass |
| registry source-file existence audit | pass |
| required content probes from the implementation plan | pass |

Remaining deliberate gaps:

| gap type | count | meaning |
| --- | ---: | --- |
| `NeedsSource` | 1232 | mostly unresolved config/result schema cells in the 575-row GDS registry plus OLTP/canary line-level audits |
| `NeedsBenchmark` | 17 | falsifier thresholds and strict-RAM formula constants that require measured benchmark runs |

Ready now:

| track | ready state |
| --- | --- |
| registry/canary scaffolding | ready to implement from this packet |
| support-status dispatch semantics | ready to implement from this packet |
| publication state-machine tests | ready to implement from this packet |
| Projection Build Store fixture design | ready to specify as tests from this packet |

Not ready to product-claim:

| track | blocker |
| --- | --- |
| full GDS support | long-tail schemas and unsupported semantics still need line-level source closure |
| strict 50GB-on-8GB proof | formula constants and cells thresholds still need benchmark evidence |
| complete OLTP compatibility | WAL, lock, checkpoint, token, index, dynamic-record, and recovery source audits remain `NeedsSource` |

# v003 Reference Learning Batches

This folder contains execution artifacts produced from
`docs_PRD03/V003-Reference-Folder-Learning-Spec.md`.

## Current Artifacts

| batch | status | primary lanes | main question | file |
| --- | --- | --- | --- | --- |
| `Batch 01` | `completed for this batch scope` | surface, capability, architecture | What do the current Knight Bus seed, Neo4j OLTP boundary, and GDS baseline already prove or rule out? | `Batch-01-Current-Seed-And-GDS-Baseline.md` |
| `Batch 02` | `completed for this batch scope` | surface, capability | What is the visible public GDS surface breadth before any storage sufficiency claim? | `Batch-02-GDS-Public-Surface-Inventory.md` |
| `Batch 03` | `completed for this batch scope` | capability, architecture, rejection | What kind of middle-layer storage behavior is actually useful for the Projection Build Store? | `Batch-03-Projection-Build-Store-Precedents.md` |
| `Batch 04` | `completed for this batch scope` | capability, architecture, rejection | What metadata and lifecycle rules are needed so OLAP snapshots can be published, retained, and rolled back safely? | `Batch-04-Publication-And-Generation-Catalog.md` |
| `Batch 05` | `completed for this batch scope` | surface, capability, rejection | What exactly must a Rust Neo4j rewrite preserve at the compatibility boundary: Bolt, Cypher, procedures, values, APOC, drivers, and real GDS user workflows? | `Batch-05-Neo4j-Compatibility-Boundary.md` |
| `Batch 06` | `completed for this batch scope` | capability, architecture, execution, rejection | What must live outside flat topology, how should sidecars and planner inputs behave, and which compact-graph precedents are actually admissible? | `Batch-06-Sidecars-Planner-And-Compact-Competitors.md` |
| `Batch 07` | `completed for this batch scope` | capability, architecture, rejection | Which mirrored graph repos actually reduce RAM, and what do representative GDS families imply for v003 storage, state, and strict-RAM fit? | `Batch-07-Low-RAM-Graph-Priors.md` |
| `Batch 08` | `completed for this batch scope` | capability, architecture, execution, rejection | What do the harder GDS families and model/pipeline artifacts imply for canonical topology, property planes, artifact planes, and strict-RAM support tiers? | `Batch-08-Hard-GDS-Families-And-Model-Artifacts.md` |
| `Batch 09` | `completed for this batch scope` | benchmark, observability, memory honesty, market watch | What makes benchmark and RAM claims credible, which telemetry fields must v003 expose, and how should graph-vector/full-text pressure be bounded without distorting P0 storage? | `Batch-09-Benchmarks-And-Observability.md` |
| `Batch 10` | `completed for this batch scope` | capability, architecture, memory contract, feasibility | How do first-party GDS projection internals, graph-store mechanics, estimator composition, and the long-tail family groups translate into v003 artifacts and support tiers? | `Batch-10-GDS-Projection-Internals-And-Support-Tiers.md` |
| `Batch 11` | `completed for this batch scope` | execution, test-readiness, scaffolding | What exact oracle graphs, flat-CSR parity checks, tolerance rules, and fixture shelves must exist before a GDS family can honestly move from architecture study into implementation? | `Batch-11-Algorithm-Oracle-And-Parity-Scaffolding.md` |

## Control Artifacts

| artifact | purpose | file |
| --- | --- | --- |
| `requirements coverage tracker` | Shows which `REQ-LEARN-*` contracts are artifact-covered, partial, spec-native, or still queued. | `Requirements-Coverage-Tracker.md` |
| `public surface inventory tsv` | Machine-readable baseline inventory of visible `gds.*` procedures and functions from the local GDS clone. | `GDS-Public-Surface-Inventory.tsv` |
| `reference shelf graph evidence ledger` | Records the full spec-resolved shelf-wide graph-tool truthcheck, low-yield repo exceptions, canonical shelf-path resolution, and the semantic-ready versus low-yield split. | `Reference-Shelf-Graph-Evidence-Ledger.md` |
| `reference shelf graph tool truthcheck tsv` | Machine-readable per-repo graph-tool validation rows with follow-up-probe status, example symbols, and rerun flags. | `Reference-Shelf-Graph-Tool-Truthcheck.tsv` |
| `reference shelf subpath coverage audit` | Shows how spec-named nested folders and subpaths are covered by repo-root graph-tool runs plus direct source reads. | `Reference-Shelf-Subpath-Coverage-Audit.md` |

## Supplemental Artifacts

| artifact | status | focus | file |
| --- | --- | --- | --- |
| `Current codebase low-RAM patterns` | `working draft` | Source-level explanation of how the current Knight Bus repo keeps build and query RAM low. | `Current-Codebase-Low-RAM-Patterns.md` |

## Decision-First Execution Order

| order | target batch theme | why it comes next |
| --- | --- | --- |
| `1` | current seed plus GDS baseline | proves the existing CSR seed, the OLTP boundary, and the breadth of the public GDS surface |
| `2` | full GDS inventory | architecture choice is unsafe until the visible surface is enumerated in a stable artifact |
| `3` | Build Store precedents | projection build/control responsibilities must be designed before snapshot publication is locked |
| `4` | snapshot generation catalog | atomic publication, watermarking, retention, and rollback need their own evidence pass |
| `5` | Neo4j compatibility boundary | Bolt, Cypher, procedure/value semantics, APOC boundaries, and client canaries must be bounded from first-party sources |
| `6` | sidecars, planner inputs, and compact competitors | decide what must live outside topology and which competitor lessons are actually admissible |
| `7` | representative second and third algorithm families | PageRank alone is not enough to choose final OLAP storage architecture |
| `8` | hard GDS families and model artifacts | the storage thesis must survive Louvain, Leiden, TriangleCount, Node2Vec, KNN, and pipeline/model surfaces |
| `9` | benchmarks and observability | memory and performance claims need workload and telemetry discipline |
| `10` | long-tail support tiers and projection internals | the remaining first-party GDS surface still needs deeper kernel, estimator, and graph-store tracing |
| `11` | oracle and parity scaffolding | the remaining families still need a concrete fixture and parity plan |

## Current Coverage Signal

- The requirement tracker currently records:
  - `51` `ArtifactCovered`
  - `0` `ArtifactPartial`
  - `0` `PlannedNextBatch`
  - `2` `SpecNativeGuardrail`
- The graph-tool substrate now has a dedicated control artifact:
  `Reference-Shelf-Graph-Evidence-Ledger.md`.
- That ledger now resolves all `71` concrete repo targets currently named by the
  learning spec, while the live `gitrefrepo/` shelf itself currently contains
  `106` top-level clones:
  - `18` `DualSemanticReady`
  - `48` `CbmSemanticReadyCgcLowYield`
  - `1` `NeedsRerun`
  - `4` `GraphToolLowYield`
- A direct scope check currently shows `71` spec repo names and `71`
  truthcheck TSV rows, with `0` missing and `0` extra.
- `35` live clones remain outside the present learning contract and should not
  be mistaken for missed spec coverage until the spec is expanded.
- Repo-root graph runs are the intended way to cover spec-named nested folders
  and subpaths; folder-specific claims still require direct `rg` plus file
  reads after the repo-level graph pass.
- Fresh long-leash `clickhouse-src` reruns on 2026-06-24 still failed the
  semantic-ready bar after `150s`: `codebase-memory-mcp` left zero-byte index
  artifacts, and CodeGraphContext still never emitted `stats.txt` or
  `functions_find.txt`.
- The machine-readable companion for that truthcheck is:
  `Reference-Shelf-Graph-Tool-Truthcheck.tsv`.
- The broad Neo4j compatibility boundary now has a dedicated batch artifact:
  `Batch-05-Neo4j-Compatibility-Boundary.md`.
- The sidecar, planner-input, and compact-competitor pass now has a dedicated
  batch artifact:
  `Batch-06-Sidecars-Planner-And-Compact-Competitors.md`.
- The hard-family and model-artifact pass now has a dedicated batch artifact:
  `Batch-08-Hard-GDS-Families-And-Model-Artifacts.md`.
- The benchmark, observability, and graph-vector market-watch pass now has a
  dedicated batch artifact:
  `Batch-09-Benchmarks-And-Observability.md`.
- The projection-internals and long-tail support-tier pass now has a dedicated
  batch artifact:
  `Batch-10-GDS-Projection-Internals-And-Support-Tiers.md`.
- The oracle-and-parity discipline now has a dedicated batch artifact:
  `Batch-11-Algorithm-Oracle-And-Parity-Scaffolding.md`.
- The current learning-spec scope no longer has architecture-critical partial
  rows. Remaining work from here is implementation support or scope expansion,
  not missing study coverage.

## Usage Rule

- Do not cite a later architecture recommendation without citing at least one
  batch artifact from this folder.
- Do not claim full GDS sufficiency from `Batch 01`; it is a baseline batch,
  not a complete surface proof.
- Do not confuse "learning spec fully implemented" with "v003 implementation
  complete." The former is now true for the current scope; the latter is still
  future work.

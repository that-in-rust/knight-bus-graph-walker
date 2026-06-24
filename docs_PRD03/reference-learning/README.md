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
| `Batch 07` | `working draft` | capability, architecture, rejection | Which mirrored graph repos actually reduce RAM, and which storage patterns are worth copying? | `Batch-07-Low-RAM-Graph-Priors.md` |

## Control Artifacts

| artifact | purpose | file |
| --- | --- | --- |
| `requirements coverage tracker` | Shows which `REQ-LEARN-*` contracts are artifact-covered, partial, spec-native, or still queued. | `Requirements-Coverage-Tracker.md` |
| `public surface inventory tsv` | Machine-readable baseline inventory of visible `gds.*` procedures and functions from the local GDS clone. | `GDS-Public-Surface-Inventory.tsv` |
| `reference shelf graph evidence ledger` | Records the full spec-resolved shelf-wide graph-tool run state, low-yield repo exceptions, canonical shelf-path resolution, and the dual-tool versus timeout-heavy split. | `Reference-Shelf-Graph-Evidence-Ledger.md` |

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
| `8` | benchmarks and observability | memory and performance claims need workload and telemetry discipline |

## Current Coverage Signal

- The requirement tracker currently records:
  - `35` `ArtifactCovered`
  - `13` `ArtifactPartial`
  - `3` `PlannedNextBatch`
  - `2` `SpecNativeGuardrail`
- The graph-tool substrate now has a dedicated control artifact:
  `Reference-Shelf-Graph-Evidence-Ledger.md`.
- That ledger now resolves all `71` concrete repo targets currently named by the
  learning spec:
  - `32` `DualToolReady`
  - `35` `CbmReadyCgcTimeout`
  - `4` `GraphToolLowYield`
- The broad Neo4j compatibility boundary now has a dedicated batch artifact:
  `Batch-05-Neo4j-Compatibility-Boundary.md`.
- The sidecar, planner-input, and compact-competitor pass now has a dedicated
  batch artifact:
  `Batch-06-Sidecars-Planner-And-Compact-Competitors.md`.
- The biggest remaining architecture themes are:
  - the still-partial algorithm-feasibility and oracle cluster;
  - benchmark and observability discipline.
  - deeper estimator, mutate/write/model, and full-family kernel tracing beyond
    the first representative passes.

## Usage Rule

- Do not cite a later architecture recommendation without citing at least one
  batch artifact from this folder.
- Do not claim full GDS sufficiency from `Batch 01`; it is a baseline batch,
  not a complete surface proof.
- Do not claim the spec is fully implemented until the requirement tracker no
  longer has architecture-critical `PlannedNextBatch` rows.

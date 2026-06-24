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

## Control Artifacts

| artifact | purpose | file |
| --- | --- | --- |
| `requirements coverage tracker` | Shows which `REQ-LEARN-*` contracts are artifact-covered, partial, spec-native, or still queued. | `Requirements-Coverage-Tracker.md` |
| `public surface inventory tsv` | Machine-readable baseline inventory of visible `gds.*` procedures and functions from the local GDS clone. | `GDS-Public-Surface-Inventory.tsv` |

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
  - `19` `ArtifactCovered`
  - `9` `ArtifactPartial`
  - `22` `PlannedNextBatch`
  - `2` `SpecNativeGuardrail`
- The biggest open cluster is still the broader Neo4j compatibility boundary:
  OLTP record/WAL depth, Bolt, Cypher, procedures/values, APOC, and driver/testkit behavior.

## Usage Rule

- Do not cite a later architecture recommendation without citing at least one
  batch artifact from this folder.
- Do not claim full GDS sufficiency from `Batch 01`; it is a baseline batch,
  not a complete surface proof.
- Do not claim the spec is fully implemented until the requirement tracker no
  longer has architecture-critical `PlannedNextBatch` rows.

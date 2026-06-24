# Batch 04: Publication And Generation Catalog

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Published OLAP snapshots`
- `Atomic publication`
- `Projection Build Store`
- `Strict holistic RAM`

Requirement IDs touched in this batch:

- `REQ-LEARN-001.0`
- `REQ-LEARN-031.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-052.0`

Batch status:

- This batch defines what is missing between the current flat snapshot seed and
  a publishable multi-generation OLAP catalog.
- This batch does not change the current snapshot files.
- This batch rejects “directory contents alone are the catalog” as sufficient
  publication semantics.

## Clone Coverage Ledger

| local_repo | exists_now | upstream_hint | branch_or_head | study_role | required_or_optional | current_use | note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `.` | yes | current Knight Bus workspace | `ideation_20260525` | current gap baseline | required | active study | current manifest and snapshot builder show what publication metadata is missing |
| `gitrefrepo/fjall-src` | yes | `fjall-rs/fjall` | `fb57152` | snapshot watermark and GC precedent | required | active study | used for publish and safe-to-GC semantics |
| `gitrefrepo/ladybug-src` | yes | `LadybugDB/ladybug` | `7eab431` | durable-before-visible publication precedent | required | active study | used for private-build-then-publish recovery rules |
| `gitrefrepo/duckdb-src` | yes | `duckdb/duckdb` | `811109f` | checkpoint-on-close precedent | optional | active study | used for close-time checkpoint and WAL disposal semantics |
| `gitrefrepo/redb-src` | yes | `cberner/redb` | `76e0e07` | durability and reader-pinning availability | optional | availability validated | deeper MVCC details already used in Batch 03 and still relevant here |

## Evidence Ledger

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CLAIM-B04-001` | `REQ-LEARN-031.0` | source | `src/types.rs:320-335` and `src/snapshot.rs:104-120` | `SnapshotManifest`, `build_snapshot_manifest` | The current Knight Bus manifest records file names, widths, counts, key mode, and `storage_mode`, but it has no generation id, source watermark, active-pointer metadata, retention info, or rollback state. | The current v2 snapshot is a good per-generation payload, but not a complete published-generation catalog. | A later wrapper manifest could preserve the v2 format unchanged and add publication metadata beside it. | Published OLAP snapshots; Atomic publication | Falsifier: another current file already provides generation identity and active-generation swap semantics. |
| `CLAIM-B04-002` | `REQ-LEARN-031.0` | source | `gitrefrepo/fjall-src/src/snapshot.rs:9-72` | `Snapshot` | `fjall` snapshots represent a consistent point-in-time read view, and old data is retained until active snapshots stop referencing it. | v003 readers need explicit generation pinning so cleanup never invalidates an in-flight OLAP query. | The pinning handle may be a generation token instead of a live object reference. | Atomic publication | Falsifier: v003 can safely delete or rewrite old generation files while readers are still active. |
| `CLAIM-B04-003` | `REQ-LEARN-031.0` | source | `gitrefrepo/fjall-src/src/snapshot_tracker.rs:125-152` | `publish`, `get_seqno_safe_to_gc`, `gc` | `fjall` advances the published sequence number with `publish(batch_seqno + 1)` and computes the oldest safe-to-GC sequence from retained snapshots. | Publication needs two distinct pieces of state: current visible generation and oldest still-retained generation. | v003 may store these as generation ids and watermarks rather than seqnos. | Atomic publication | Falsifier: a single “latest directory” convention proves enough for publish, rollback, and cleanup without tracking retention separately. |
| `CLAIM-B04-004` | `REQ-LEARN-031.0` | source | `gitrefrepo/ladybug-src/docs/index_build_recovery.md:3-38` | `Current recovery invariant`, `Large index optimization` | Ladybug states a hard recovery rule: there must be no window where a valid catalog entry is committed without durable physical storage or a recovery path that can recreate it. | v003 must not publish a generation catalog entry until the referenced topology and sidecar files are durable or recoverable. | The same rule should apply to result and model sidecars too. | Atomic publication; Projection Build Store | Falsifier: v003 can publish generation metadata first and rely on best-effort file completion later. |
| `CLAIM-B04-005` | `REQ-LEARN-031.0` | source | `gitrefrepo/ladybug-src/docs/index_build_recovery.md:26-38,53-68` | `private storage`, `publish only after durable`, `keep generated pages in private build buffers` | Ladybug's proposed large-build path keeps build artifacts private, persists them through checkpoint/shadow protocol, then publishes visibility only after durability, while keeping generated pages off the hot read path until publication. | v003 snapshot compilers should write into private generation staging directories and only flip the active catalog pointer after validation succeeds. | A later variant might publish a manifest file atomically rather than rename a directory tree. | Published OLAP snapshots; Strict holistic RAM | Falsifier: exposing partially built generation files to ordinary catalog lookup proves harmless and simpler. |
| `CLAIM-B04-006` | `REQ-LEARN-031.0` | source | `gitrefrepo/duckdb-src/src/main/attached_database.cpp:313-330` | `CreateCheckpoint`, `CheckpointWALAction::DELETE_WAL` | DuckDB conditionally creates a checkpoint on close and can request `DELETE_WAL` as part of checkpoint action. | Publication and retention policy belong to an explicit lifecycle action, not to incidental file presence alone. | v003 may checkpoint/publish on a background cadence rather than close-time only. | Published OLAP snapshots | Falsifier: generation publication can stay entirely implicit with no explicit lifecycle operation. |
| `CLAIM-B04-007` | `REQ-LEARN-031.0` | source | `gitrefrepo/ladybug-src/docs/index_build_recovery.md:42-55` | `BUILDING`, `CATCHING_UP`, `VALID`, `INVALID` | Ladybug proposes an explicit build-state machine where not-yet-valid artifacts are ignored by the optimizer until they reach `VALID`. | v003 generation catalog should distinguish staged, validating, active, failed, and retired generations rather than treating every directory as queryable. | The exact state names may differ, but statefulness is likely mandatory. | Atomic publication | Falsifier: v003 can model all publication transitions with only a boolean active flag and no staged or failed states. |

## Architecture Fit Matrix

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `staged generation build` | complete topology and sidecar file set in private location | labels/types/properties/results/models stay generation-local | validation reports, counts, checksums, source watermark | staged generation id not visible to ordinary readers | none | build scratch must not count as active-serving footprint | write privately, validate, then publish | `P0-RegisteredCompatible` | ordinary readers can safely open partially built generation directories |
| `active generation selection` | none directly | none directly | durable source watermark and validation result | active-generation pointer plus generation metadata | none | metadata-only | atomic pointer or manifest swap | `NeedsArchitectureSpike` | directory scan plus latest timestamp is enough and safe |
| `reader pinning and retention` | old generation files remain immutable | sidecars remain pinned with topology | retired-generation bookkeeping | retained-generation floor or reader refcount state | none | retained disk and cache pressure must be budgeted | keep active readers on W while W+1 becomes current | `NeedsArchitectureSpike` | readers never outlive publication and old generations can be deleted immediately |
| `rollback to prior generation` | previous immutable generation still exists | matching sidecars still exist | build failure does not corrupt prior metadata | active pointer can move back to prior generation | none | rollback metadata is small | pointer swap back to last good generation | `NeedsArchitectureSpike` | rollback is unnecessary because publication is infallible or always rebuildable immediately |
| `query-time directory discovery without catalog` | n/a | n/a | n/a | forbidden as sole publication mechanism | none | risks undefined file visibility and cleanup races | do not rely on it | `ExplicitlyOutOfScope` | a fully specified directory convention proves just as safe as a real catalog pointer |

## Publication Option Scorecard

| option_id | option_summary | helps_prd_outcomes | known_blockers | required_capabilities | evidence_strength | dominant_ram_risks | current_status | next_falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `PG-A` | per-generation snapshot payload only, no active catalog | preserves the current simple file payload | no active pointer, weak rollback, weak retention semantics | none beyond file emission | `high` as current payload format | low metadata RAM, but high correctness ambiguity | `NeedsArchitectureSpike` | prove readers can never race publication or cleanup without a catalog |
| `PG-B` | active catalog file pointing to immutable generation payloads | supports watermark reporting, publish/swap, rollback, and retention | needs explicit state machine and durability order | active pointer, staged states, retained floor | `high` | metadata RAM is tiny; retained generations dominate disk/cache pressure | `P0-RegisteredCompatible` | a simpler scheme matches the same correctness guarantees |
| `PG-C` | timestamp- or directory-scan-based latest-generation discovery | very simple operationally | ambiguous visibility, weak rollback semantics, weak validation boundary | filesystem conventions only | `low` | low metadata RAM but high correctness risk | `ExplicitlyOutOfScope` | a formal proof or implementation spike shows scans are safe and race-free |

## PRD Outcome Traceability Dossier

| PRD outcome | supporting claims | current confidence | next experiment or evidence spike |
| --- | --- | --- | --- |
| `Published OLAP snapshots` | `CLAIM-B04-001`, `CLAIM-B04-004`, `CLAIM-B04-005`, `CLAIM-B04-006` | `medium-high` | define a concrete generation catalog schema and active-pointer file contract |
| `Atomic publication` | `CLAIM-B04-002`, `CLAIM-B04-003`, `CLAIM-B04-004`, `CLAIM-B04-007` | `high` for the need, `medium` for the final shape | specify staged/valid/failed/retired generation transitions and restart recovery rules |
| `Projection Build Store` | `CLAIM-B04-004`, `CLAIM-B04-005` | `medium` | align generation publication with the `BS-C` Build Store recommendation from Batch 03 |
| `Strict holistic RAM` | `CLAIM-B04-005`, `CLAIM-B04-006` | `medium` | add retained-generation cache pressure to the later memory contract batch |

## Rejected-Alternative Note

Rejected for this batch:

- `Treat snapshot directories and file timestamps as the full publication catalog.`

Why rejected:

- The current Knight Bus manifest does not record generation or watermark state.
- `fjall` shows that publication and safe-to-GC are separate tracked concerns.
- Ladybug's recovery notes show visibility must follow durability, not precede it.

What would overturn this rejection:

- A rigorously specified directory-only scheme proves atomic visibility, safe
  rollback, and reader-safe retention without any additional durable catalog
  metadata.

## Skeptical Review

| challenge | response |
| --- | --- |
| Aren't you overcomplicating publication for a single-node engine? | Single-node simplifies coordination, but it does not remove the need for active generation identity, rollback, and reader-safe cleanup. |
| Could the current manifest just grow a few fields and be enough? | Possibly. This batch does not reject that; it rejects having no publication catalog semantics at all. |
| Is Ladybug too distant from the CSR snapshot problem? | The artifact type differs, but the publish-after-durable invariant maps directly to v003 generation publication. |
| Does this batch already specify the exact on-disk catalog file? | No. It establishes the invariant set that the file or files must satisfy. |

## Verification Commands Run

```bash
nl -ba src/types.rs | sed -n '318,460p'
nl -ba src/snapshot.rs | sed -n '104,150p'
git -C gitrefrepo/fjall-src rev-parse --short HEAD
sed -n '1,220p' gitrefrepo/fjall-src/src/snapshot.rs
sed -n '120,185p' gitrefrepo/fjall-src/src/snapshot_tracker.rs
git -C gitrefrepo/ladybug-src rev-parse --short HEAD
sed -n '1,120p' gitrefrepo/ladybug-src/docs/index_build_recovery.md
git -C gitrefrepo/duckdb-src rev-parse --short HEAD
sed -n '313,336p' gitrefrepo/duckdb-src/src/main/attached_database.cpp
```

## Checkpoint: capability+architecture+rejection / publication-generation-catalog / 2026-06-24

Assigned requirement IDs:

- `REQ-LEARN-001.0`
- `REQ-LEARN-031.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-052.0`

Evidence rows completed:

- `7`

Most important sourced facts:

- The current Knight Bus snapshot payload has no generation or watermark fields.
- `fjall` explicitly separates published visibility from safe-to-GC state.
- Ladybug makes the durable-before-visible invariant explicit for build artifacts.

Architecture implications:

- `Adopt`: immutable generation payloads plus a real active-generation catalog.
- `Adapt`: keep current v2 snapshot payload as a per-generation leaf if practical.
- `Reject`: relying on directory contents alone as the publication contract.
- `Watch`: retained generations can create cache and disk pressure even if metadata is tiny.
- `MissingEvidence`: exact catalog schema, restart recovery sequence, and rollback operator UX.

Unresolved risks:

- `Risk`: catalog complexity could drift beyond what single-node v003 needs.
  `Falsifier`: a minimal active-pointer schema proves enough for publish/swap/rollback/retain.
- `Risk`: retained generations may quietly break the RAM story through cache pressure.
  `Falsifier`: later memory-contract work budgets retained-generation residency explicitly.

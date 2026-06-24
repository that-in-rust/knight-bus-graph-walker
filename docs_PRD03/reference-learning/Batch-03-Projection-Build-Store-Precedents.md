# Batch 03: Projection Build Store Precedents

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Projection Build Store`
- `Published OLAP snapshots`
- `Strict holistic RAM`
- `Atomic publication`

Requirement IDs touched in this batch:

- `REQ-LEARN-001.0`
- `REQ-LEARN-011.0`
- `REQ-LEARN-028.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`

Batch status:

- This batch studies what kind of middle-layer storage behavior is actually
  useful for v003.
- This batch does not choose a final embedded engine.
- This batch does reject one architectural mistake: turning the Build Store
  into a user-query serving overlay.

## Clone Coverage Ledger

| local_repo | exists_now | upstream_hint | branch_or_head | study_role | required_or_optional | current_use | note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `docs_PRD03/prd-l1.md` | yes | current workspace | current | architecture contract | required | active study | defines the Build Store as build/control plane only |
| `gitrefrepo/apache-iggy-src` | yes | `apache/iggy` | `115ac2146` | append-log precedent | required | active study | used for durable receipt-ingest and `fsync` behavior |
| `gitrefrepo/redb-src` | yes | `cberner/redb` | `76e0e07` | embedded metadata-store precedent | required | active study | used for durable-vs-non-durable commit tracking and reader pinning ideas |
| `gitrefrepo/fjall-src` | yes | `fjall-rs/fjall` | `fb57152` | journal plus snapshot plus compaction precedent | required | active study | used for commit/persist/publish and GC-safe snapshot semantics |
| `gitrefrepo/rocksdb-src` | yes | `facebook/rocksdb` | `364eb88` | compaction/direct-I/O cautionary precedent | optional | active study | used to ground direct-I/O and WAL/MANIFEST caveats |
| `gitrefrepo/tikv-src` | yes | `tikv/tikv` | `6cdd896` | transactional-store availability | optional | availability validated | present for later deeper study if Build Store needs stronger MVCC evidence |

## Evidence Ledger

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CLAIM-B03-001` | `REQ-LEARN-011.0` | source | `docs_PRD03/prd-l1.md:17-35,52-58,103-120,163-176` | `Projection Build Store contract` | The PRD says OLTP writes flow into a Projection Build Store which is a build/control-plane store, not a user-query serving store, and lists allowed responsibilities like dense-ID assignment, dictionary building, sorted-run staging, validation, and crash recovery. | The Build Store is a manufacturing and validation plane between OLTP truth and published OLAP generations. | The final physical engine mix can still change. | Projection Build Store | Falsifier: a later PRD explicitly permits user OLAP queries to read directly from the middle layer. |
| `CLAIM-B03-002` | `REQ-LEARN-011.0` | source | `gitrefrepo/apache-iggy-src/core/partitions/src/messages_writer.rs:37-126` | `MessagesWriter::{new,save_frozen_batches,fsync}` | Iggy opens a segment file, can synchronize existing files, appends frozen batches to the segment, and optionally calls `sync_all()` via `fsync`. | An append-only durable receipt log is a strong precedent for OLTP-to-Build-Store change ingress. | v003 may use a simpler single-node log than Iggy. | Projection Build Store | Falsifier: Build Store turns out not to need durable receipt replay because snapshots are always rebuilt from OLTP truth directly. |
| `CLAIM-B03-003` | `REQ-LEARN-011.0` | source | `gitrefrepo/redb-src/src/transaction_tracker.rs:79-210` | `TransactionTracker` | `redb` explicitly distinguishes live read transactions, a single live write transaction, and `pending_non_durable_commits` mapped to a `durable_ancestor`, while holding read references so unsafe reclamation does not happen too early. | Build Store publication semantics need a crisp distinction between pending build state and durable/published state. | v003 may not need full MVCC, but it does need durable-versus-pending generation discipline. | Atomic publication | Falsifier: published generation swap can be made crash-safe without tracking pending versus durable states anywhere. |
| `CLAIM-B03-004` | `REQ-LEARN-011.0` | source | `gitrefrepo/fjall-src/src/batch/mod.rs:100-181` | `Batch::commit` | `fjall` writes the batch to a journal, persists it when durability is configured, applies the batch into trees, publishes the batch sequence number through a snapshot tracker, then accounts for write-buffer growth. | A good Build Store pattern is `append -> persist -> apply -> publish -> account`, not `mutate visible query state first and reconcile later`. | v003 may split these steps across log, fact tables, and snapshot compiler jobs rather than one API call. | Projection Build Store; Strict holistic RAM | Falsifier: a lower-RAM Build Store path avoids in-memory apply phases entirely and never needs write-buffer accounting. |
| `CLAIM-B03-005` | `REQ-LEARN-011.0` | source | `gitrefrepo/fjall-src/src/snapshot.rs:9-72` | `Snapshot` docs and API | `fjall` snapshots keep a consistent view at a point in time, and old data cannot be dropped while active snapshots still reference it. | Publication readers in v003 need generation pinning semantics so retention and cleanup never invalidate an in-flight read. | Reader pinning may live in a manifest/catalog layer rather than the Build Store itself. | Atomic publication | Falsifier: v003 uses only best-effort file retention and never needs to know which generation a reader still holds. |
| `CLAIM-B03-006` | `REQ-LEARN-011.0` | source | `gitrefrepo/fjall-src/src/compaction/worker.rs:1-57` | `compact(..., snapshot_tracker.get_seqno_safe_to_gc())` | `fjall` compaction passes a sequence number that is safe to garbage collect based on snapshot tracking. | Build Store compaction and cleanup must be pinned to the oldest active durable generation or snapshot watermark. | v003 may use generation-level GC instead of seqno-level GC, but the principle is the same. | Atomic publication | Falsifier: compaction and cleanup can run without consulting any reader-pinning or watermark state. |
| `CLAIM-B03-007` | `REQ-LEARN-011.0` | source | `gitrefrepo/fjall-src/src/builder.rs:102-138` | `max_journaling_size`, `max_write_buffer_size` | `fjall` exposes journal-size and write-buffer-size limits, explicitly naming them as resources that need configuration. | Build Store must expose journal, scratch, and in-memory apply budgets as first-class parts of the RAM promise. | v003 may track additional sort-spill buffers and dictionary builders beyond these two classes. | Strict holistic RAM | Falsifier: Build Store memory stays negligible enough that explicit buffer budgets do not matter. |
| `CLAIM-B03-008` | `REQ-LEARN-028.0` | source | `gitrefrepo/rocksdb-src/WINDOWS_PORT.md:69-76` and `gitrefrepo/rocksdb-src/env/env.cc:1173-1179` | `use_os_buffer`, `OptimizeForCompactionTableWrite` | RocksDB documents un-buffered access as a way to gain control of memory consumption, but also notes that standard buffered behavior still makes sense for files like WAL and MANIFEST; the env optimization toggles direct writes specifically for flush and compaction table writes. | Direct I/O is a surgical tool for strict-RAM build/refresh paths, not a blanket rule for every file and every path. | v003 may choose buffered ingress plus explicit-I/O snapshot compilation. | Strict holistic RAM | Falsifier: the best v003 Build Store design ends up using direct I/O everywhere with no WAL/manifest exceptions. |

## Architecture Fit Matrix

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `OLTP receipt ingress` | none | none | durable append log, replay order, checksummed batches | watermark handoff to later publication | none | bounded ingress buffer plus disk-backed log | append, optionally `fsync`, then async compile | `P0-RegisteredCompatible` | snapshots are always rebuilt straight from OLTP truth and never need durable incremental receipts |
| `dense-id and dictionary facts` | none directly | label/type/property dictionary state | transactional fact tables or sorted runs | generation-scoped outputs | low to medium | explicit sort and dictionary scratch budgets | build-only; never user query serving | `NeedsArchitectureSpike` | compiler can derive everything cheaply from snapshots alone without reusable facts |
| `publication-ready generation metadata` | none directly | manifest/checksum/watermark metadata | durable generation state before swap | active pointer, retention, rollback, reader pins | low | metadata is small but correctness-critical | validate then atomically publish | `NeedsArchitectureSpike` | active-generation semantics can be made safe with only ad hoc files and no durable metadata state |
| `cleanup and compaction` | none directly | none | snapshot-aware GC threshold and reclaim bookkeeping | oldest pinned generation or seqno | none | cleanup must count background buffers and backlog | compact only up to safe-to-GC point | `NeedsArchitectureSpike` | cleanup can delete obsolete build artifacts without consulting reader or generation state |
| `user OLAP query serving from Build Store` | n/a | n/a | forbidden by PRD boundary | forbidden by PRD boundary | n/a | hidden RAM and semantic bleed | do not allow | `ExplicitlyOutOfScope` | PRD later changes to make the Build Store a user-visible query plane |

## Build Store Option Scorecard

| option_id | option_summary | helps_prd_outcomes | known_blockers | required_capabilities | evidence_strength | dominant_ram_risks | current_status | next_falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `BS-A` | append-only receipt log only | durable ingress, replayability | weak metadata, weak publication state, weak dedupe/dictionary support | receipt replay only | `medium` | rebuild jobs still need side metadata somewhere else | `NeedsArchitectureSpike` | show a full snapshot compiler can operate reproducibly from only a receipt log |
| `BS-B` | embedded transactional metadata store only | catalog metadata, durable state, reader pinning | poor fit for high-volume append receipt ingress by itself | generation metadata, dictionaries, validation state | `medium` | in-place growth and internal buffering can hide scratch costs | `NeedsArchitectureSpike` | show ingest and replay throughput are good enough without a separate append log |
| `BS-C` | append-only receipt log plus embedded transactional metadata/fact store | matches ingress, durable state, publish discipline, replay, validation, and crash recovery | more moving parts than a single engine | receipt log, durable facts, generation metadata, validation reports | `high` | two stores mean duplication if fact retention is not bounded | `P0-RegisteredCompatible` | a later simpler single-store design proves equally clear and lower-RAM |
| `BS-D` | full LSM or delta-serving layer queried by OLAP users | high freshness potential | violates PRD boundary, invites hidden RAM, query-time reconciliation, and serving-plane confusion | live merge reads, tombstones, freshness overlays | `high` against adopting it | memtables, compaction backlog, page cache, and query-time merge state | `ExplicitlyOutOfScope` | PRD later explicitly allows OLAP queries to read from a live mutable middle layer |

## PRD Outcome Traceability Dossier

| PRD outcome | supporting claims | current confidence | next experiment or evidence spike |
| --- | --- | --- | --- |
| `Projection Build Store` | `CLAIM-B03-001` through `CLAIM-B03-004`, `CLAIM-B03-007` | `medium-high` | decide whether the first implementation shape is `BS-C` with a minimal receipt log and embedded metadata store |
| `Published OLAP snapshots` | `CLAIM-B03-001`, `CLAIM-B03-004`, `CLAIM-B03-005`, `CLAIM-B03-006` | `medium` | create the dedicated publication-generation batch and define active-pointer swap semantics |
| `Strict holistic RAM` | `CLAIM-B03-004`, `CLAIM-B03-007`, `CLAIM-B03-008` | `medium` | turn journal, scratch, and background-buffer classes into a reusable memory contract object |
| `Atomic publication` | `CLAIM-B03-003`, `CLAIM-B03-005`, `CLAIM-B03-006` | `medium` | define durable-versus-pending generation states and reader-pinning rules in one catalog note |

## Rejected-Alternative Note

Rejected for this batch:

- `Turn the Projection Build Store into a third user-visible serving database or a live OLAP overlay.`

Why rejected:

- The PRD explicitly says the middle layer is not a user-query serving store.
- `redb` and `fjall` both reinforce the cost of durable-versus-pending state and
  reader-safe cleanup; exposing that state directly to OLAP queries would bleed
  build-time complexity into serving semantics.
- RocksDB-style buffered/direct-I/O and compaction choices are operationally
  useful, but they are exactly the kind of hidden-memory machinery that the PRD
  wants off the OLAP read path.

What would overturn this rejection:

- A PRD change that explicitly prefers live OLAP freshness over the published
  snapshot contract and accepts the serving-plane complexity and RAM cost.

## Skeptical Review

| challenge | response |
| --- | --- |
| Are you just reinventing a generic LSM database in the middle? | No. The preferred direction is narrower: receipt log plus metadata/fact state for compilation and publication, not a general-purpose serving database. |
| Does `BS-C` duplicate data too much? | It can if retention is sloppy. That is why generation GC and bounded fact retention remain explicit next tasks. |
| Could a single engine still win later? | Yes. `BS-C` is a current recommendation, not a permanent engine verdict. |
| Are you overusing direct I/O ideology here? | No. `CLAIM-B03-008` is intentionally cautionary: direct I/O is path-specific and not appropriate for every file type. |
| Does this batch already prove publication semantics end to end? | No. It only extracts the middle-layer precedents needed before the publication batch can be written cleanly. |

## Verification Commands Run

```bash
git -C gitrefrepo/apache-iggy-src rev-parse --short HEAD
git -C gitrefrepo/redb-src rev-parse --short HEAD
git -C gitrefrepo/fjall-src rev-parse --short HEAD
git -C gitrefrepo/rocksdb-src rev-parse --short HEAD
git -C gitrefrepo/tikv-src rev-parse --short HEAD
nl -ba docs_PRD03/prd-l1.md | sed -n '1,180p'
sed -n '35,150p' gitrefrepo/apache-iggy-src/core/partitions/src/messages_writer.rs
sed -n '79,210p' gitrefrepo/redb-src/src/transaction_tracker.rs
sed -n '100,190p' gitrefrepo/fjall-src/src/batch/mod.rs
sed -n '1,120p' gitrefrepo/fjall-src/src/snapshot.rs
sed -n '1,80p' gitrefrepo/fjall-src/src/compaction/worker.rs
sed -n '96,142p' gitrefrepo/fjall-src/src/builder.rs
sed -n '60,90p' gitrefrepo/rocksdb-src/WINDOWS_PORT.md
sed -n '1168,1182p' gitrefrepo/rocksdb-src/env/env.cc
```

## Checkpoint: capability+architecture+rejection / build-store-precedents / 2026-06-24

Assigned requirement IDs:

- `REQ-LEARN-001.0`
- `REQ-LEARN-011.0`
- `REQ-LEARN-028.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`

Evidence rows completed:

- `8`

Most important sourced facts:

- `docs_PRD03/prd-l1.md:17-35,52-58,103-120,163-176` fixes the Build Store boundary as build/control only.
- `apache-iggy` shows a durable append-plus-`fsync` receipt path.
- `redb` and `fjall` both show why durable-versus-pending state and snapshot-safe cleanup must be explicit.
- `rocksdb` shows why direct I/O should be selective, not ideological.

Architecture implications:

- `Adopt`: a Projection Build Store that combines durable receipt ingress with durable metadata/fact state.
- `Adapt`: path-specific direct I/O for strict-RAM build/refresh phases, not every file path.
- `Reject`: serving OLAP reads from the Build Store.
- `Watch`: storage duplication between receipt log, fact store, and retained generations.
- `MissingEvidence`: the exact generation catalog, active pointer swap, retention, and rollback note.

Unresolved risks:

- `Risk`: `BS-C` could still overbuild the middle layer if fact retention is not aggressively bounded.
  `Falsifier`: a simpler single-store design proves equally clear and lower-RAM in the publication batch.
- `Risk`: background write buffers and compaction scratch may silently violate the RAM story.
  `Falsifier`: a later memory-contract batch names and budgets each buffer class explicitly.

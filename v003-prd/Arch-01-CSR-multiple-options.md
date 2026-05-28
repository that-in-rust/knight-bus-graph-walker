# Arch 01: CSR Multiple Options

This is the canonical v003 architecture decision ledger for OLAP storage. It is
intentionally not just the final answer. It preserves the serious architectures
we considered, why each was attractive, what broke under the PRD constraints,
and how the current preferred architecture emerged.

Read this file as:

```text
top-level summary = current conviction
option ledger     = how we got there
appendix          = preserved earlier flat-vs-cellular analysis
```

The purpose is to keep conviction traceable. If a future reader asks "why not
just flat CSR?" or "why not default cell deltas?", the answer should be visible
without reconstructing the whole discussion.

## Top-level decision summary

Current preferred architecture:

```text
OLTP truth
  -> verified OLAP pre-dataset / Projection Build Store
  -> immutable CSR snapshot
       either flat dual CSR
       or cellular CSR packaged from the same pre-dataset
  -> snapshot-as-of OLAP/GDS queries
  -> optional tail overlay only for explicit near-real-time freshness
```

One-line thesis:

```text
For v003, optimize for RAM-bounded exact snapshots with explicit OLAP lag,
not zero-lag query-time mutation merging.
```

The strongest current conviction:

```text
Flat CSR remains the physical read primitive.
The Projection Build Store becomes the durable analytical build source.
Cellular CSR becomes a packaging/locality/compaction option.
Tail overlays are optional serving freshness, not foundational correctness.
```

## Why this decision changed

The earlier debate was framed as:

```text
Option 1: one big flat immutable dual CSR
Option 2: Cellular CSR Tilehouse with deltas
```

That was useful but incomplete. The missing layer was the thing before CSR:

```text
OLAP pre-dataset / Projection Build Store
```

Once that layer exists, the architecture stops being "flat vs cells" and becomes
a pipeline:

```text
transactional truth -> analytical facts -> compiled read format -> query view
```

That matters because the PRD explicitly accepts OLAP lag and optimizes for
holistic RAM on an 8GB machine. If OLAP lag is acceptable, query-time overlays
are not required by default. They are only needed when a query promises
freshness beyond the published snapshot watermark.

## Architecture options ledger

| ID | Architecture | Shape | What felt attractive | What broke / risk | Current verdict |
| --- | --- | --- | --- | --- | --- |
| A | Flat CSR only | OLTP/source -> full rebuild -> one immutable dual CSR | Simplest, compact, excellent global scans | Weak update pipeline; freshness only by full rebuild/swap | Keep as primitive and fallback, not whole v003 architecture |
| B | Flat CSR + global delta overlay | immutable flat CSR + global add/delete/property deltas | Fresher queries without full rebuild | Query-time merge/tombstone/dedup RAM can violate 8GB goal | Do not default |
| C | Cellular CSR + cell deltas | cell CSR + cell-local deltas + compaction | Good locality, dirty-cell compaction, bounded operational units | Still pays overlay complexity; partitioning risk; may slow global scans | Valuable research path, not foundational first step |
| D | OLAP pre-dataset -> flat CSR | OLTP -> verified analytical facts -> flat CSR snapshots | Clean snapshot-as-of semantics; build-time normalization; predictable query RAM | Adds disk/build-source layer; snapshot lag explicit | Best MVP baseline |
| E | OLAP pre-dataset -> Cellular CSR | OLTP -> analytical facts -> partitioned CSR cells | Keeps D's clean semantics while adding packaging/locality/compaction | More complex compiler/partitioner; must prove boundary overhead | Preferred v003 evolution target |
| F | Pre-dataset + optional tail overlay | D/E + capped tx tail where `tx_id > snapshot_watermark` | Near-real-time OLAP possible for selected features | Reintroduces query-time merge RAM and complexity | Optional future mode only |

Decision ladder:

```text
Start with A because current Knight Bus already proves the CSR primitive.
Reject A alone because v003 needs a real OLTP->OLAP update/build path.
Reject B as default because global overlays hide RAM/read-amplification costs.
Treat C as promising but not sufficient because cells do not remove the need for
  a durable build source or explicit freshness contract.
Promote D as MVP because it gives clean watermarks and predictable query RAM.
Promote E as research/evolution because cells are useful after the pre-dataset
  makes snapshot generation clean.
Keep F optional because it only matters if freshness SLA beats RAM simplicity.
```

## Comparison by PRD obligation

| PRD obligation | A flat only | B flat + delta | C cellular + delta | D pre-dataset -> flat | E pre-dataset -> cells | F optional tail |
| --- | --- | --- | --- | --- | --- | --- |
| Neo4j-shaped OLTP remains truth | yes | yes | yes | yes | yes | yes |
| Lowest serving RAM | strong | weak/medium | medium | strong | strong if partitions good | weak unless tightly capped |
| Holistic RAM accounting | easiest | hard due to overlays | hard due to overlays | clear: build vs serve separated | clear if metadata bounded | hard in tail mode |
| Accepted OLAP lag | yes | less relevant | less relevant | yes, explicit | yes, explicit | configurable |
| Small update ingestion | weak | medium | good | good in pre-dataset | good in pre-dataset | good but query-costly |
| Snapshot exactness | strong | merge-dependent | merge-dependent | strong as of W | strong as of W | exact only if overlay merge correct |
| Global algorithm scans | excellent | overlay may interfere | needs stream adapter | excellent | must prove stream adapter | overlay may interfere |
| Locality / compaction | weak | weak | strong | medium | strong | depends on base |
| Implementation complexity | low | medium | high | medium | medium/high | high |

## Current recommendation

Use this as the canonical plan:

```text
Required v003 foundation:
  1. OLTP truth remains Neo4j-shaped.
  2. Projection Build Store stores normalized analytical facts.
  3. Immutable CSR snapshots compile from that store at source watermarks.
  4. OLAP queries report `as_of_watermark`.
  5. Flat dual CSR remains the first physical snapshot target.

Next evolution:
  6. Cellular CSR compiles from the same pre-dataset when spikes prove value.

Not default:
  7. Query-time tail overlays.
```

If somebody asks "what are we optimizing for?", the answer is:

```text
Predictable holistic RAM on 8GB machines.
Exact answers as of a stated snapshot watermark.
Architecture that can later add cell locality without corrupting the base model.
```

If somebody asks "what are we not optimizing for?", the answer is:

```text
Zero-lag OLAP at all costs.
Always-fresh query-time merge semantics.
Partitioning theory before measured evidence.
```

## Architecture conviction map

```text
                      PRD accepts OLAP lag
                              |
                              v
                 query-time overlay not mandatory
                              |
                              v
             need durable analytical build source
                              |
                              v
             Projection Build Store becomes foundation
                              |
                              v
      immutable CSR snapshot gives predictable serving RAM
                              |
                              v
          flat CSR first, cellular CSR as packaged evolution
                              |
                              v
              optional tail overlay only if SLA demands it
```

ELI5:

```text
OLTP truth        = author's live manuscript
Projection Store  = editor's verified notes
Flat CSR snapshot = printed book
Cellular CSR      = printed book split into chapters
Tail overlay      = sticky notes after printing
```

Do not start with sticky notes. First build the editor's verified notes and
print clean editions with publication dates.

## Canonical file policy

This file should be the place where all serious CSR/OLAP architecture options
live. Earlier single-purpose notes can exist only if they are explicitly marked
as historical or are replaced by this file.

Recommended repo convention:

```text
Arch-01-CSR-multiple-options.md = canonical decision ledger
Arch-01-CSR-2-options.md        = historical flat-vs-cellular note, superseded
```

If keeping both files, the two-options file should point back here so reviewers
do not treat it as the current final decision.

---

This note compares OLAP storage directions for v003 after correcting the
freshness premise:

```text
OLAP is allowed to lag OLTP.
The optimization target is holistic RAM on 8GB machines, not zero-lag OLAP.
```

That changes the architecture decision. A query-time delta overlay is not
foundational if we introduce a verified OLAP pre-dataset. The cleaner baseline
is:

```text
OLTP truth
  -> OLAP pre-dataset / Projection Build Store
  -> immutable flat/cellular CSR snapshot
  -> GDS / walk queries exact as of snapshot watermark
```

The delta/tail overlay becomes optional:

```text
Use it only if a feature requires near-real-time OLAP freshness beyond the
published snapshot watermark, and only if it fits the RAM budget.
```

## Short answer: preferred multi-layer architecture

```text
                 +---------------------------------+
                 | 1. OLTP TRUTH                   |
                 | Neo4j-shaped records/WAL/locks  |
                 +----------------+----------------+
                                  |
                                  | committed events / CDC
                                  v
                 +---------------------------------+
                 | 2. OLAP PRE-DATASET             |
                 | Projection Build Store          |
                 | normalized analytical facts     |
                 | verified in sync with OLTP      |
                 +----------------+----------------+
                                  |
                                  | compile at watermark W
                                  v
                 +---------------------------------+
                 | 3. IMMUTABLE CSR SNAPSHOT       |
                 | flat CSR or cellular CSR cells  |
                 | exact as of tx/time W           |
                 +----------------+----------------+
                                  |
                                  | mmap fast mode / O_DIRECT strict mode
                                  v
                 +---------------------------------+
                 | 4. OLAP QUERY VIEW              |
                 | walks / GDS / PageRank / BFS    |
                 +---------------------------------+
```

Optional only:

```text
                 +---------------------------------+
                 | 5. TAIL/FRESHNESS OVERLAY       |
                 | tx_id > snapshot_watermark      |
                 | not v003 MVP foundation         |
                 +---------------------------------+
```

One-line thesis:

```text
For Knight Bus v003, optimize for RAM-bounded exact snapshots with explicit
freshness lag. Do not pay query-time delta merge cost by default.
```

## Phase 0: deconstruct and clarify

Premise is sound. Proceeding with optimized protocol.

The user question is asking whether v003 should include another layer before
read-optimized CSR: a durable OLAP pre-dataset that is verified against OLTP.
If yes, the earlier "CSR base + delta overlay" model should be softened because
OLAP lag is acceptable and RAM is the true optimization target.

The implicit assumptions are:

- OLTP remains Neo4j-shaped and authoritative.
- OLAP queries do not need to be zero-lag.
- The system should expose the freshness watermark honestly.
- The main scarce resource is holistic RAM on an 8GB machine.
- Immutable CSR is still the best read-optimized analytical physical format.

Flawed premise corrected:

```text
Delta overlay is not required for correctness if the OLAP pre-dataset is synced
with OLTP and queries are allowed to be exact as of the latest published CSR
snapshot.
```

Optimized execution plan:

1. Preserve flat CSR as the physical read primitive.
2. Add a Projection Build Store as analytical intermediate representation.
3. Compile immutable CSR/cell snapshots from that pre-dataset at watermarks.
4. Make snapshot freshness explicit in every query/result/manifest.
5. Treat tail overlays as optional future near-real-time mode.

## Phase 1: expert council

Council:

1. **Graph Database Architect**: protects Neo4j-compatible semantics.
2. **Storage Systems Engineer**: protects write/read amplification and crash
   recovery.
3. **Graph Analytics / GDS Engineer**: protects algorithm execution contracts.
4. **Memory Budget Engineer**: protects holistic 8GB RAM target.
5. **Skeptical Engineer / Devil's Advocate**: challenges freshness and
   duplication assumptions.

Knowledge scaffolding:

```text
Neo4j OLTP record/WAL semantics
CDC / analytical fact logs
CSR and dual CSR snapshots
snapshot isolation and watermarks
OLAP freshness SLAs
mmap vs O_DIRECT memory behavior
LSM-style write amplification
GDS graph catalog/projection model
```

## Phase 2: options explored

### Conventional option: CSR base + query-time delta overlay

```text
OLTP truth -> immutable CSR base
             + delta overlay for new writes
             -> exact query view
```

This gives fresher OLAP reads, but every query may pay:

```text
base CSR read
+ add-list merge
+ delete/tombstone check
+ property update merge
+ dedup/order repair
+ overlay indexes
+ extra memory for merge state
```

It is attractive for freshness, but dangerous for the 8GB promise.

### Blended alternative A: compiler IR

Blend graph storage with compiler design.

```text
OLTP records/WAL = source language
OLAP pre-dataset = intermediate representation
CSR snapshot     = optimized machine code
GDS runtime      = CPU executing machine code
```

This is the preferred model because it separates semantic normalization from
physical optimization.

### Blended alternative B: kitchen prep station

Blend graph storage with restaurant operations.

```text
OLTP truth        = live orders
OLAP pre-dataset  = prep station
CSR snapshot      = plated meals
tail overlay      = last-second garnish
```

If the restaurant is RAM-limited, avoid last-second garnish on every plate.
Keep the prep station current and plate in batches.

### Blended alternative C: cartography

Blend graph storage with mapmaking.

```text
OLTP truth        = live surveyor observations
OLAP pre-dataset  = verified survey ledger
CSR snapshot      = printed atlas
tail overlay      = temporary road-closure stickers
```

The atlas is not updated every second. It is accurate as of a publication date.
Road-closure stickers are useful, but not foundational.

### Selected hybrid

Use the compiler IR + cartography model:

```text
OLTP truth -> verified analytical IR -> published CSR atlas.
```

Only add stickers/tail overlays for explicit near-real-time features.

## Phase 3: council debate summary

Graph Database Architect:

> The pre-dataset is the right place to normalize Neo4j-shaped records into
> projected analytical facts. Keep OLTP authoritative.

Storage Systems Engineer:

> Query-time overlays risk read amplification. Batch compaction from a durable
> pre-dataset is much cleaner.

Graph Analytics Engineer:

> GDS algorithms want stable graph projections. Snapshot-as-of semantics are
> normal and easier to test than every-query mutation merging.

Memory Budget Engineer:

> The 8GB target strongly favors immutable snapshots plus explicit O_DIRECT
> buffers over ad hoc overlay merge memory.

Skeptical Engineer:

> Another dataset means more disk and consistency machinery. Only accept it if
> it removes query-time complexity and has watermarks/checksums proving sync.

Synthesis:

```text
Add the OLAP pre-dataset, but make its invariants explicit:
durable, append/merge-friendly, verified against OLTP, and compiled into CSR at
published watermarks. Do not introduce default serving overlays until a measured
freshness SLA demands them.
```

## Freshness SLA reality

Industry OLAP systems usually accept lag unless they are explicitly HTAP or
streaming systems. This table is general industry context, not proof from this
repository:

| System style | Typical freshness | Optimizes for |
| --- | ---: | --- |
| Traditional warehouse batch | hours to daily | low cost, stable large scans |
| Modern ELT/CDC warehouse | minutes to hours | analytical consistency |
| Lakehouse incremental tables | minutes to hours | batch + incremental compaction |
| Streaming analytics | seconds to minutes | event freshness with more complexity |
| HTAP / operational analytics | milliseconds to seconds | freshness at high RAM/CPU/index cost |
| GDS-style graph projection | explicit projection time; can be stale | fast algorithms on projected graph |

Knight Bus v003 should fit here:

```text
low-RAM graph OLAP with explicit snapshot freshness,
not HTAP zero-lag operational analytics.
```

Recommended SLA language:

```text
OLAP queries are exact as of the latest published projection snapshot.
Each snapshot exposes a source watermark.
Freshness lag = current OLTP watermark - snapshot watermark.
```

## Corrected dataset model

There are three foundational datasets/planes, plus one optional serving overlay:

| Layer | Role | Mutable? | Source of truth? | Query-time default? |
| --- | --- | --- | --- | --- |
| OLTP truth | Neo4j-shaped transactional records, WAL, locks, indexes | yes | yes | OLTP only |
| OLAP pre-dataset / Projection Build Store | normalized analytical facts verified from OLTP | append/merge-friendly | no | no, build source |
| Immutable CSR snapshot | read-optimized projection exact at watermark W | no | no | yes for OLAP |
| Optional tail overlay | changes where `tx_id > W` for near-real-time OLAP | yes, capped | no | no by default |

ASCII:

```text
                 authoritative
                      |
                      v
+---------------------------------------------+
| OLTP truth                                  |
| records / WAL / locks / indexes             |
+---------------------+-----------------------+
                      |
                      | low-RAM CDC / receipts
                      v
+---------------------------------------------+
| OLAP pre-dataset / Projection Build Store   |
| nodes, edges, labels, reltypes, properties  |
| tombstones, dictionaries, tx watermarks      |
+---------------------+-----------------------+
                      |
                      | compile/partition/sort at W
                      v
+---------------------------------------------+
| Immutable CSR snapshot                      |
| flat dual CSR or cellular CSR               |
| sidecars + manifest + watermark W           |
+---------------------+-----------------------+
                      |
                      v
+---------------------------------------------+
| OLAP/GDS query runtime                      |
| exact answers as of W                       |
+---------------------------------------------+
```

Optional:

```text
if near-real-time OLAP is explicitly required:

query_view = snapshot(W) + tail_overlay(W, T)

otherwise:

query_view = snapshot(W)
```

## What the OLAP pre-dataset stores

The pre-dataset should be boring, durable, and easy to verify:

```text
node_id
label ids
property key ids and typed values
relationship_id
source node id
target node id
relationship type id
relationship properties
valid_from_tx
valid_to_tx or tombstone
source WAL offset / tx id
partition hint
dictionary ids
checksums
```

It should not be optimized for final query speed. It should be optimized for:

```text
low-RAM ingestion
sequential build reads
deduplication
delete/property-change resolution
snapshot compilation
verification against OLTP watermarks
```

## Why the pre-dataset removes mandatory delta overlay

Key invariant:

```text
CSR snapshot has watermark W.
OLAP pre-dataset is verified through watermark T.
If T == W, no overlay is needed.
If T > W and queries only promise W, no overlay is needed.
If T > W and queries promise T, overlay is needed.
```

Therefore:

```text
pre-dataset sync proves build-source correctness.
snapshot watermark defines query correctness.
overlay only bridges a freshness SLA gap.
```

If v003 accepts lag, use this:

```text
query freshness = snapshot watermark
pre-dataset freshness = OLTP watermark or near it
compiler lag = pre-dataset watermark - snapshot watermark
```

## Option comparison

| Option | Shape | Freshness | RAM risk | Complexity | Verdict |
| --- | --- | --- | --- | --- | --- |
| A. Flat CSR only | OLTP -> full rebuild -> flat CSR | snapshot lag | lowest query RAM | low | good current primitive, weak update pipeline |
| B. Cellular CSR + default deltas | OLTP -> cell deltas -> cell CSR | near-real-time | medium/high query merge RAM | high | useful later, risky as MVP default |
| C. OLAP pre-dataset -> flat CSR | OLTP -> analytical facts -> flat CSR | snapshot lag | low query RAM, build RAM bounded | medium | best simple v003 baseline |
| D. OLAP pre-dataset -> cellular CSR | OLTP -> analytical facts -> cells | snapshot lag | low query RAM if partition good | medium/high | best v003 research target |
| E. Pre-dataset + optional tail overlay | C/D plus capped overlay | configurable | overlay-dependent | high | only for explicit near-real-time SLA |

Preferred path:

```text
MVP: C
Research/scale path: D
Optional future: E
```

## How this changes the earlier Cellular CSR thesis

Earlier:

```text
Cellular CSR + deltas is the freshness layer.
```

Corrected:

```text
Projection Build Store is the freshness/build-source layer.
Cellular CSR is the packaging/compaction/locality layer for compiled snapshots.
Tail overlay is optional serving freshness, not foundational correctness.
```

Flat CSR still matters:

```text
Flat CSR = canonical byte primitive and global stream.
Cells = bounded packaging around that primitive.
Pre-dataset = durable normalized source for generating either one.
```

## Holistic RAM reasoning

Default query-time overlay increases hidden RAM:

```text
overlay indexes
tombstone filters
dedup buffers
merge cursors
property-change side buffers
boundary repair
result ordering state
```

The pre-dataset model moves that work out of the serving path:

```text
query path: CSR snapshot only
build path: pre-dataset -> sorted/partitioned CSR under memory budget
```

That aligns with the project goal:

```text
optimize serving queries for predictable RAM on 8GB
accept OLAP lag explicitly
```

## Verification requirements

The architecture is only credible if these tests exist:

| Test | What it proves |
| --- | --- |
| OLTP-to-pre-dataset watermark test | pre-dataset is synced through tx T |
| pre-dataset checksum test | analytical facts match expected nodes/edges/properties |
| CSR snapshot watermark test | snapshot declares exact source watermark W |
| snapshot-from-pre-dataset parity test | CSR output matches pre-dataset facts at W |
| stale-read contract test | query result reports `as_of_watermark = W` |
| no-overlay RAM test | OLAP query RSS excludes tail overlay structures by default |
| optional-overlay budget test | near-real-time mode refuses to run if overlay exceeds budget |

## Recommended implementation sequence

1. Define source watermarks:

```rust
pub struct SourceWatermark {
    pub tx_id: u64,
    pub wal_offset: u64,
}
// Identifies the exact OLTP point represented by analytical artifacts.
```

2. Define Projection Build Store records:

```rust
pub struct ProjectionNodeFact {
    pub node_id: ExternalNodeId,
    pub labels: LabelSetId,
    pub properties: PropertyRecordRef,
    pub valid_from: SourceWatermark,
    pub valid_to: Option<SourceWatermark>,
}

pub struct ProjectionRelFact {
    pub rel_id: ExternalRelId,
    pub source: ExternalNodeId,
    pub target: ExternalNodeId,
    pub rel_type: RelTypeId,
    pub properties: PropertyRecordRef,
    pub valid_from: SourceWatermark,
    pub valid_to: Option<SourceWatermark>,
}
```

3. Define snapshot manifest freshness:

```rust
pub struct SnapshotFreshness {
    pub source_watermark: SourceWatermark,
    pub build_started_at_ms: u64,
    pub build_finished_at_ms: u64,
    pub source_lag_tx: u64,
}
// Every OLAP answer can report what snapshot it used.
```

4. Define query freshness contract:

```rust
pub enum OlapFreshnessMode {
    SnapshotOnly,
    SnapshotPlusTailOverlay,
}
// SnapshotOnly is the default v003 mode.
```

5. Define optional overlay budget:

```rust
pub struct TailOverlayBudget {
    pub max_bytes: u64,
    pub max_tx_lag: u64,
    pub max_merge_layers: u32,
}
// Refuse near-real-time mode when this cannot be honored.
```

## Chain of verification

Fact-check questions:

1. Does OLAP generally allow lag?
2. Does accepting lag remove the need for a default query-time overlay?
3. Does a pre-dataset replace OLTP truth?
4. Does a pre-dataset replace CSR?
5. Does this improve the 8GB RAM story?
6. Is tail overlay still useful?

Answers:

1. Yes. Many OLAP systems expose batch, CDC, or projection freshness rather than
   zero-lag answers.
2. Yes, if query semantics are exact as of snapshot watermark.
3. No. OLTP remains authoritative.
4. No. It is build-source IR; CSR remains read-optimized physical format.
5. Yes, because merge/dedup/tombstone work moves to build time instead of every
   query.
6. Yes, but only for explicit near-real-time features with a measured budget.

Weaknesses:

- The pre-dataset adds disk footprint.
- It needs crash recovery and idempotent ingestion.
- It needs verification tooling.
- It does not make CSR builds free; it only makes them cleaner and more
  incremental.

Final corrected response:

```text
The foundational v003 pipeline should be:

OLTP truth
  -> verified OLAP pre-dataset / Projection Build Store
  -> immutable flat or cellular CSR snapshots
  -> snapshot-as-of OLAP queries.

Do not use query-time delta overlays by default.
Use them only as an optional tail-freshness mode after proving RAM bounds.
```

## Appendix A: Earlier flat-vs-cellular analysis

The rest of this note preserves the earlier flat-vs-cellular analysis, but it
should now be read under the corrected multi-layer conclusion above:

```text
Projection Build Store first.
Immutable CSR snapshots second.
Cellular CSR as packaging/locality/compaction evolution.
Tail overlays optional, not foundational.
```

The earlier comparison focused on two OLAP storage directions for v003:

1. **Current Knight Bus flat immutable dual CSR**
2. **Proposed Cellular CSR Tilehouse**

The goal is not to change OLTP. The PRD constraint remains:

```text
OLTP data storage remains Neo4j-shaped.
OLAP storage is the decision.
OLAP must minimize holistic RAM: heap, page cache, duplicate layouts,
compaction buffers, snapshot build scratch, delta overlays, indexes,
and algorithm intermediates.
```

## Short answer, corrected

Current Knight Bus is already the right seed: a compact immutable dual-CSR
snapshot with a low-RAM builder and mmap traversal runtime.

Cellular CSR is a strong v003 evolution path, but the corrected thesis is more
conservative than "replace flat CSR":

```text
Flat CSR = physics primitive.
Cellular CSR = packaging, freshness, planning, and compaction layer.
```

Recommended phrasing:

```text
Cellular CSR is the preferred v003 research target and update-aware storage
evolution, but v003 should keep flat dual CSR as the canonical global stream
and first implementation fallback until spikes prove cells improve real
workloads.
```

So the preferred direction is:

```text
current flat dual CSR
  -> keep as canonical per-cell byte primitive
  -> keep as exact global logical stream fallback
  -> package into bounded graph cells where useful
  -> add cell passports for planning/freshness/validation
  -> add boundary indexes only where measurements justify them
  -> add label/type/property sidecars
  -> add capped WAL-fed cell-local deltas
  -> use O_DIRECT global streaming for strict-RAM algorithms
```

The main claim:

```text
Cellular CSR is not "faster CSR."
It is flat CSR made update-aware, locality-aware, and RAM-budgetable.
It should not displace flat CSR as the global scan primitive until measured.
```

This corrects the most important possible overstatement in the earlier note:
cells are an architectural layer over CSR, not proof that cells beat flat CSR for
every workload.

## PRD constraint analysis

The PRD constraints from `v003-prd.md` are unusually strict:

```text
exact same APIs or surface area with ZERO changes
identical architecture for OLTP queries
lowest RAM custom storage formats for OLAP queries
50 GB data processed comfortably on 8 GB systems
OLAP RAM includes heap, page cache, duplicate layouts, compaction buffers,
snapshot build scratch, delta overlays, indexes, and algorithm intermediates
O_DIRECT + compio for exact RAM control
```

The storage architecture must therefore satisfy four different obligations:

| Obligation | What it means | Consequence for CSR design |
| --- | --- | --- |
| OLTP compatibility | Transactional graph store remains Neo4j-shaped | Do not make CSR the mutable source of truth |
| OLAP low RAM | Analytics cannot rely on giant in-memory GDS projections | CSR/sidecars must stream and spill under budget |
| Holistic RAM | Page cache and scratch count, not just heap | mmap is fast mode; O_DIRECT is strict-RAM mode |
| API surface | GDS-style procedures need catalog, properties, modes, estimates | Topology alone is insufficient |

Correct constraint reading:

```text
v003 does not need "one magic graph layout."
v003 needs a dual-plane system:
  OLTP plane: Neo4j-shaped mutable records/WAL/locks/indexes.
  OLAP plane: CSR-derived projections with exact memory contracts.
```

Flat dual CSR already satisfies the "physics primitive" part of the OLAP plane.
Cellular CSR addresses the missing operational pieces:

```text
freshness unit
locality unit
compaction unit
planning unit
sidecar attachment unit
```

But algorithm state remains independent of cell layout. PageRank, Louvain, KNN,
embeddings, and ML can still be dominated by vectors, heaps, candidate pairs, or
models. Cells improve packaging and locality; they do not erase global state.

## Core facts

### Current Knight Bus architecture

Current Knight Bus writes a single immutable snapshot:

```text
snapshot/
  manifest.json
  node_table.bin
  strings.bin
  forward.offsets.bin
  forward.peers.bin
  reverse.offsets.bin
  reverse.peers.bin
  key_index.bin
```

The current manifest storage mode is:

```text
immutable_dual_csr
```

This is excellent for static graph walks:

```text
external key -> dense id -> CSR offset window -> neighbor dense ids -> keys
```

It is simple, compact, and correct for the current walk-focused runtime.

### Current strengths

| Strength | Why it matters |
| --- | --- |
| One canonical topology | No duplicate algorithm-specific layouts |
| Forward and reverse CSR | Fast bidirectional neighbor iteration |
| Dense `u32` node IDs | Compact indexes and arrays |
| `u64` offsets | Supports large edge counts |
| mmap runtime | Simple, fast static reads |
| low-RAM external-sort builder | Snapshot construction can honor a memory budget |
| Small surface area | Easy to reason about and test |

### Current limits for v003

The v003 PRD asks for more than immutable walks:

```text
same Neo4j-facing surface
full OLAP API surface
OLTP updates feeding OLAP
50GB data on 8GB systems
holistic RAM accounting
```

Current flat CSR is not enough by itself because:

| Limit | Consequence |
| --- | --- |
| Immutable snapshot | OLAP freshness requires full rebuild or a separate delta design |
| One global file set | Local updates and local compaction still interact with global layout |
| mmap page cache | RAM residency is decided by the OS, not the planner |
| Topology-first format | Labels, relationship types, and properties need sidecars |
| Walk-focused runtime | PageRank, BFS, SSSP, k-core, Louvain, triangle count, etc. need additional execution/storage support |
| No cell boundaries | No natural unit for bounded update repair, compaction, or locality-aware planning |

## Option 1: Current flat immutable dual CSR

### Shape

```text
+--------------------------------------------------+
| Flat immutable snapshot                           |
|                                                  |
| forward.offsets       u64[node_count + 1]         |
| forward.peers         u32[edge_count]             |
| reverse.offsets       u64[node_count + 1]         |
| reverse.peers         u32[edge_count]             |
| node_table            fixed-width key records     |
| strings               key bytes                   |
| key_index             sorted key -> dense id      |
+--------------------------------------------------+
```

### Read path

```text
query seed key
  -> binary search key_index
  -> dense id
  -> read forward/reverse offset window
  -> read neighbor ids
  -> map ids back to keys
```

### Update path

There is no true live update path in the current static snapshot model.

The realistic options are:

```text
1. OLTP update commits in Neo4j-shaped store.
2. OLAP remains at generation N.
3. A new snapshot generation N+1 is built from source/WAL.
4. Runtime atomically swaps to generation N+1.
```

This is clean and safe, but it is not update-local.

### RAM behavior

Flat CSR has two different RAM personalities:

| Mode | RAM behavior |
| --- | --- |
| mmap traversal | Low explicit heap, but OS page cache controls real residency |
| O_DIRECT global streaming | Deterministic RAM if implemented, but not current runtime default |
| builder | Low-RAM external sort can bound process RSS |

For a 50GB-class graph, current notes estimate roughly:

```text
nodes: ~200M
edges: ~1B
flat CSR topology + key structures: ~15-20GB on disk
PageRank score arrays: ~3.2GB for two f64[200M] vectors
```

### Best use

Flat CSR is best when:

```text
graph is mostly static
queries are simple walks
latency matters more than freshness
full graph scans can stream sequentially
implementation simplicity is the priority
```

## Option 2: Proposed Cellular CSR Tilehouse

### Shape

Cellular CSR keeps the dual-CSR idea but changes the physical unit:

```text
snapshot_generation_42/
  manifest.json
  global_dense_id_map/
  cells/
    cell_000001/
      passport.json
      forward.offsets.bin
      forward.peers.bin
      reverse.offsets.bin
      reverse.peers.bin
      label_bitmaps.bin
      reltype_bitmaps.bin
      node_properties.arrow
      edge_properties.arrow
      delta_receipts.bin
    cell_000002/
      ...
  boundaries/
    cross_cell_edges.bin
    boundary_nodes.bin
  global_stream/
    logical_forward_order.index
    logical_reverse_order.index
```

Each cell is a bounded graph tile.

Each cell passport stores small metadata:

```text
node count
edge count
label histogram
relationship type histogram
property min/max or dictionary metadata
degree histogram
hot hubs
boundary edge counts
dirty tx range
checksum
```

### Read path

Cell-local query:

```text
query seed key
  -> global dense id
  -> cell id
  -> open cell passport
  -> read only that cell's CSR windows
  -> expand to boundary cells only if needed
```

Global algorithm:

```text
procedure call
  -> planner chooses Level 2 or Level 3 RAM contract
  -> stream all cells in logical CSR order
  -> use O_DIRECT for deterministic RAM
```

### Update path

OLTP still commits first to the Neo4j-shaped store.

Then:

```text
Neo4j-shaped WAL transaction
  -> analytical receipt
  -> map affected node/edge/property to cell(s)
  -> append cell-local delta
  -> update dirty passport metadata
  -> query reads base cell + bounded delta
  -> background compacts dirty cell under memory budget
```

Example:

```text
CREATE (a)-[:FOLLOWS]->(b)
  -> receipt: edge_add(src=a, dst=b, type=FOLLOWS)
  -> dirty cells: cell(a), cell(b), maybe boundary table
  -> append delta to those cells
  -> no full snapshot rebuild required
```

### RAM behavior

Cellular CSR aims to make RAM explicit:

```text
local query RAM = selected cell windows + small metadata
regional query RAM = wavefront cells + boundary buffers
global query RAM = O_DIRECT stream buffers + algorithm state
delta RAM = capped per dirty cell
compaction RAM = capped per cell/batch
```

The point is not that every query uses less heap than flat CSR.

The point is:

```text
global surprise RAM becomes local budgeted RAM.
```

## Head-to-head comparison

### High-level comparison

| Dimension | Current flat immutable dual CSR | Proposed Cellular CSR Tilehouse | Better? |
| --- | --- | --- | --- |
| Core storage | One global dual CSR | Many bounded dual-CSR cells | Cellular for v003; flat for simplicity |
| Current implementation maturity | Already exists | New design | Flat |
| Static walk latency | Excellent | Similar, sometimes slightly better/worse | Tie |
| Full graph scan | Excellent sequential base | Must provide logical global stream | Flat unless optimized |
| OLTP update bridge | Rebuild/swap generation | WAL receipt -> dirty cells -> local compaction | Cellular |
| Freshness granularity | Snapshot-level | Cell/delta-level | Cellular |
| Compaction unit | Whole snapshot | Cell or cell batch | Cellular |
| Page-cache control | mmap-heavy; OS decides | cell windows + optional O_DIRECT | Cellular |
| Full OLAP API readiness | Needs major additions | Designed for labels/types/properties/algorithms | Cellular |
| Disk compactness | Minimal | 5-20% topology overhead likely | Flat |
| Complexity | Low | Medium-high | Flat |

### Quantified estimate for 50GB dataset on 8GB system

Assumptions:

```text
Neo4j-shaped dataset: ~50GB
Approx graph shape: ~200M nodes, ~1B edges
Flat CSR topology/key structures: ~15-20GB on disk
Usable RAM for OLAP after OS/minimal server overhead: ~5-6GB
```

| Workload / metric | Current flat CSR | Cellular CSR Tilehouse | Improvement / regression |
| --- | ---: | ---: | --- |
| Topology disk size | ~15-20GB | ~16-23GB | Cellular is ~5-20% worse |
| Metadata resident RAM | small runtime structs + mmap VMAs | ~300MB-1.5GB for cell metadata/windows | Cellular worse at idle if many passports cached |
| 1-hop / 2-hop static walk | micro-ms to ms after page-in | micro-ms to ms after cell page-in | roughly equal |
| Local neighborhood query | may fault arbitrary global pages | bounded to local cells when partitioning is good | 2-10x less page-cache churn |
| Multi-cell traversal | direct CSR offsets | boundary routing + cell wavefront | can be better or worse; partition-dependent |
| Exact global PageRank Level 2 | ~3.2GB, ~10-25s if O_DIRECT engine exists | ~3.2GB, ~10-30s with global stream adapter | roughly equal; Cellular may be 0-20% slower |
| Strict global PageRank Level 3 | ~196MB-512MB, minutes if implemented | ~196MB-512MB, minutes if implemented | roughly equal |
| Small update freshness | requires full rebuild/snapshot generation or broad delta | append receipt to 1-2 cells | 10x-1000x less rebuild work for small updates |
| Delta overlay RAM | none in static model | capped per cell; target 128-512MB global cap before compaction | Cellular uses more RAM but enables freshness |
| Compaction scratch | global rebuild budget | per-cell budget | Cellular substantially better |
| Full OLAP property filters | not first-class in current v2 | typed per-cell sidecars | Cellular major improvement |
| Operational complexity | low | 2-4x higher | Cellular worse |

### Truth-check table

This table separates supported claims from plausible but unproven claims.

| Claim | Verdict | Correction / nuance |
| --- | --- | --- |
| Current Knight Bus flat dual CSR is the right seed. | True | Supported by the repo's current snapshot/runtime shape and low-RAM builder. |
| Cellular CSR is "CSR made updateable, locality-aware, RAM-budgetable." | Mostly true | True only if cells are real update, compaction, and query units, not just folders. |
| Cellular CSR is better for static walks. | Not proven | Flat CSR may be equal or faster. Cells add boundary and mapping overhead. |
| Cellular CSR is better for small update freshness. | True | Cell-local deltas are much better than rebuilding a 50GB-class snapshot for a handful of edits. |
| Cellular CSR is better for full global algorithm speed. | False / not proven | Flat CSR is already close to ideal for sequential scans. Cells may slow global scans unless the logical stream adapter is excellent. |
| Cellular CSR improves holistic RAM. | Partially true | Strong for local reads, dirty-cell compaction, and bounded deltas. Neutral for global PageRank/Louvain/KNN when algorithm state dominates. |
| `O_DIRECT` gives deterministic RAM. | Mostly true but overstrong | It bypasses page cache, but alignment buffers, block-device behavior, kernel accounting, I/O scheduling, and algorithm arrays still matter. |
| 50GB graph -> flat CSR topology/key structures around 15-20GB. | Plausible | Raw dual CSR for 200M nodes / 1B directed edges stored in both forward and reverse form is about 11.2GB before keys/properties: offsets `2 * (200M + 1) * 8 ~= 3.2GB`, peers `2 * 1B * 4 ~= 8GB`. Keys and sidecars make 15-20GB plausible. |
| PageRank two `f64[200M]` vectors = about 3.2GB. | True | `2 * 200M * 8 = 3.2GB`. Two `f32` vectors would be about 1.6GB. |
| Exact global PageRank in 10-30s. | Too confident | Possible on strong hardware, but unsafe as a claim for 8GB/O_DIRECT mode without benchmark. Say "tens of seconds to minutes." |
| Cellular topology overhead 5-20%. | Plausible, unproven | Boundary indexes, passports, per-cell offsets, and deltas could push overhead higher. |
| Metadata RAM 300MB-1.5GB. | Plausible but design-dependent | Compact passports should be far smaller; boundary indexes and cached sidecars may dominate. |
| Full OLAP API readiness improves with cells. | True as architecture | Cells do not solve all algorithms. Global algorithms still need global state, sidecars, and result storage. |
| Partitioning is the biggest risk. | True | Bad partitioning can erase locality and add random I/O. |
| Delta compaction is the second biggest risk. | True | Unbounded delta layers become LSM-style read amplification. |

### Evidence and verification notes

These are supporting references, not benchmark proof.

| Evidence | What it supports |
| --- | --- |
| Current Knight Bus `snapshot.rs` writes flat `forward.offsets`, `forward.peers`, `reverse.offsets`, `reverse.peers`, node table, string table, and key index. | Flat dual CSR is the implemented primitive today. |
| Current Knight Bus `runtime.rs` opens the snapshot through mmap and exposes walk-focused runtime methods. | Current execution is static and traversal-oriented, not full GDS. |
| Neo4j GDS source includes `CSRGraphStoreFactory`, `CSRGraphStore`, `GraphStoreCatalog`, `HugeGraph`, and memory estimate result types. | GDS uses a separate projected graph plane and takes memory estimation seriously. |
| Neo4j GDS `HugeGraph` comments describe target IDs sorted, delta-encoded, and written as variable-length vlongs with offset-based access. | Compressed CSR-like layouts are a real GDS design pattern. |
| GraphChi uses vertex intervals and shards for out-of-core graph processing. Source: [GraphChi OSDI/USENIX](https://www.usenix.org/conference/osdi12/126-graphchi-large-scale-graph-computation-just-pc). | Cell/shard-style graph execution has strong precedent. |
| LLAMA uses multiversioned arrays for graph analytics. Source: [LLAMA](https://syrah.eecs.harvard.edu/publications/llama-efficient-graph-analytics-using-large-multiversioned-arrays). | Base + delta/versioned graph thinking is legitimate. |
| LSMGraph proposes multi-level CSR for dynamic graph storage. Source: [arXiv 2411.06392](https://arxiv.org/abs/2411.06392). | Graph-LSM / multi-level CSR is a real research direction, but likely complex. |
| Linux page-cache documentation. Source: [Linux page cache](https://www.kernel.org/doc/html/v6.13/mm/page_cache.html). | mmap/page-cache residency is not fully planner-controlled. |

Anything above that references GraphChi, LLAMA, LSMGraph, or Linux kernel docs is
external evidence, not proven by this repository. It should be independently
verified before being treated as an implementation guarantee.

## Where Cellular CSR is better

### 1. Update locality

Flat CSR:

```text
one changed edge
  -> OLTP fresh
  -> OLAP snapshot stale
  -> rebuild/swap full generation for exact freshness
```

Cellular CSR:

```text
one changed edge
  -> OLTP fresh
  -> append receipt to affected cell(s)
  -> read base cell + delta
  -> compact dirty cell later
```

This is the biggest improvement.

Expected magnitude:

```text
small updates: 10x-1000x less OLAP rebuild work
large batches: still better if changes are localized
whole-graph imports: similar to rebuilding
```

### 2. Holistic RAM control

Flat CSR with mmap:

```text
heap may be small
but page cache can grow unpredictably
```

Cellular CSR:

```text
load bounded cell windows for local queries
use O_DIRECT for global scans
cap cell delta overlays
compact cells under fixed memory budgets
```

Expected magnitude:

```text
local workloads: 2-10x less page-cache churn
global workloads: similar RAM if both use O_DIRECT
compaction: cell-bounded instead of generation-bounded
```

### 3. Full OLAP API path

Flat CSR is excellent for neighbor iteration, but v003 needs the full OLAP surface:

```text
PageRank
BFS
SSSP
connected components
k-core
Louvain
triangle count
filtered projections
property-aware analytics
```

Cellular CSR has natural extension points:

```text
label bitmaps per cell
relationship type bitmaps per cell
typed property columns per cell
boundary indexes
global stream adapter
RAM-contract planner
```

## Where Cellular CSR is not better

### 1. Pure full-graph sequential scans

Flat CSR is already close to ideal:

```text
read offsets
stream peers
compute algorithm state
```

Cellular CSR adds:

```text
cell manifests
file boundaries
boundary indexes
logical stream ordering
```

Expected result:

```text
0% speedup for global scans
possible 5-20% slowdown unless global stream adapter is carefully built
```

Corrected latency wording:

```text
Do not claim exact global PageRank in 10-30s as an architectural fact.
Safer claim: with optimized sequential scans and vectors that fit, it may be
tens of seconds to minutes; with strict 8GB/O_DIRECT/spill mode, minutes is
more realistic until benchmarked.
```

### 2. Disk compactness

Flat CSR is more compact.

Cellular CSR adds:

```text
per-cell offsets
passports
boundary indexes
delta files
sidecar metadata
```

Expected result:

```text
5-20% worse topology disk footprint
```

This overhead is acceptable only if update locality and RAM control matter.

### 3. Simplicity

Flat CSR:

```text
one manifest
seven files
one dense ID universe
straight offset math
```

Cellular CSR:

```text
cell partitioning
global-to-local ID mapping
boundary routing
cell passports
delta receipts
cell compaction
global stream adapter
```

Expected result:

```text
2-4x more storage/runtime complexity
```

## Rubber-duck debugging

### Duck: Is Cellular CSR actually a new architecture or just folders?

It is a new architecture if cells are query/compaction/update units.

It is just folders if cells are only a file organization trick.

The required invariant:

```text
Every cell must be independently readable, dirtyable, compactable,
and budgetable.
```

### Duck: Does Cellular CSR reduce RAM, or just move it around?

Both.

For global algorithms, it mostly moves RAM control into O_DIRECT and explicit
algorithm state. It may not reduce total RAM versus a flat O_DIRECT stream.

For local/update-heavy workloads, it genuinely reduces holistic RAM pressure by
preventing unrelated graph regions from entering page cache or compaction
scratch.

### Duck: Does it break exactness?

It must not.

The exactness rule:

```text
If a query touches many cells, it touches many cells.
If a query touches the whole graph, it streams the whole graph.
Cells are an optimization boundary, not an approximation boundary.
```

### Duck: Does it serve the full OLAP API surface?

It can, but only if it includes a global fallback:

```text
cell-local execution for locality
global logical CSR stream for whole-graph algorithms
typed sidecars for labels/types/properties
RAM-contract planner for algorithm intermediates
```

Without the global stream adapter, Cellular CSR would be incomplete.

### Duck: Does the PRD force Cellular CSR to be the default immediately?

No.

The PRD forces:

```text
same surface area
Neo4j-shaped OLTP
lowest-RAM OLAP
holistic memory accounting
```

It does not force the first v003 OLAP implementation to make cells the default
for all reads. The safer path is:

```text
flat CSR first implementation fallback
Cellular CSR as spike / opt-in storage evolution
promote cells only after benchmark thresholds pass
```

### Duck: Is "O_DIRECT gives exact RAM" perfectly true?

No. It is directionally right but should be stated precisely.

O_DIRECT helps because it bypasses the normal page cache for file data and makes
the application allocate explicit buffers. But strict RSS still includes:

```text
algorithm arrays
allocator fragmentation
stack
metadata
alignment buffers
kernel-visible I/O structures
device / filesystem behavior
delta overlays
result sidecars
```

Correct claim:

```text
O_DIRECT is the right strict-RAM tool for global scans, but the memory contract
must still account for algorithm state and all explicit buffers.
```

### Duck: What is the biggest implementation risk?

Partitioning.

Bad partitioning causes:

```text
too many boundary edges
too many opened cells
too much random I/O
metadata overhead without locality benefit
```

The first spike must measure boundary ratio and cell-window RAM.

### Duck: What is the second biggest implementation risk?

Delta compaction.

If cell deltas are not capped, Cellular CSR becomes an LSM mess:

```text
base cell + delta1 + delta2 + delta3 + tombstones + property versions
```

The design needs hard rules:

```text
max delta bytes per cell
max global delta bytes
max delta layers per query
force compaction when thresholds are crossed
```

## Corrected architecture roles

| Layer | Recommended role |
| --- | --- |
| Flat dual CSR | Keep as the canonical byte primitive for per-cell topology and global logical streams. |
| Cells | Add as bounded update, compaction, planning, and locality units. |
| Passports | Use for validation, freshness, histograms, query planning, and memory estimates. |
| Boundary indexes | Add after measuring boundary ratio; avoid overbuilding first. |
| Sidecars | Attach labels, relationship types, weights, and properties to cells without duplicating topology. |
| Deltas | Keep capped per cell and globally capped; force compaction by thresholds. |
| mmap | Keep for interactive traversal and normal fast mode. |
| O_DIRECT streaming | Use for strict-RAM global algorithms and benchmarked scan paths, not every query. |
| GDS catalog/procedure layer | Treat as API compatibility contract above storage. |

The winning implementation is therefore:

```text
flat CSR inside cells
+ one exact global flat stream
+ optional cell-local fast paths
+ capped deltas
+ sidecars
+ memory-contract planner
```

not:

```text
cells instead of flat CSR
```

## Benchmark spikes required before making cells default

| Spike | Must measure | Promotion threshold |
| --- | --- | --- |
| `flat-vs-cell-local-walk` | page faults, RSS/PSS, latency, opened cells, cells touched per query | cells reduce cache churn/latency on locality-heavy queries without large tail penalty |
| `flat-vs-cell-global-scan` | scan throughput, file-open overhead, boundary overhead, logical stream adapter penalty | global stream is within an acceptable small overhead of flat CSR |
| `10-edits-delta` | time-to-visible in OLAP, delta RAM, query merge overhead | small edits become visible without generation rebuild and without unbounded query slowdown |
| `dirty-cell-compaction` | scratch RAM, disk amplification, publish time, crash recovery | compaction stays within budget and publishes atomically |
| `partition-quality` | boundary edge ratio, hot hub distribution, opened cells per traversal | partition does not explode boundary traffic |
| `pagerank-8gb` | exact RSS/PSS, wall time, I/O volume, vector pressure, spill volume | memory estimate predicts measured memory; runtime remains acceptable |
| `sidecar-filtered-query` | label/type/property filter selectivity, sidecar page faults, result correctness | sidecars improve filtered OLAP without duplicating full topology |

These spikes decide defaults:

```text
If global scan overhead is high, keep flat CSR global files as primary.
If local walk benefit is high, use cells for locality-heavy queries.
If delta merge overhead is high, lower delta thresholds or compact sooner.
If partition quality is poor, do not block v003 on partitioning theory.
```

## Decision

Prefer Cellular CSR as the v003 research target and update-aware storage
evolution, but keep flat CSR as the canonical primitive and internal global
stream model.

Cellular CSR should become the default only after spikes prove:

```text
boundary ratio is acceptable
metadata and sidecar overhead are acceptable
global scan overhead is acceptable
delta merge overhead is bounded
dirty-cell compaction stays under memory budget
```

The winning design is not:

```text
replace flat CSR completely
```

It is:

```text
make flat CSR the per-cell and global-stream primitive
```

Final architecture:

```text
Neo4j-shaped OLTP remains the transactional source of truth.
WAL receipts feed Cellular CSR OLAP storage.
Each cell is a small dual-CSR snapshot with labels, types, properties,
passport metadata, and bounded deltas.
Local OLAP reads touch only relevant cells.
Global OLAP reads stream all cells as one logical CSR using deterministic
RAM contracts.
```

## Final verdict

| Question | Answer |
| --- | --- |
| Is Cellular CSR better for current static walks? | Only slightly, often equal |
| Is Cellular CSR better for global algorithm speed? | No, flat CSR is already ideal |
| Is Cellular CSR better for update-aware OLAP? | Yes, dramatically |
| Is Cellular CSR better for holistic RAM? | Yes for local/update/compaction workloads; neutral for strict global O_DIRECT when algorithm state dominates |
| Is Cellular CSR simpler? | No |
| Is Cellular CSR more PRD-compliant for v003? | Yes |
| Should we abandon flat CSR? | No |
| Should cells be the immediate default for every workload? | No; prove with spikes first |

One-line conclusion:

```text
Flat CSR is the physical primitive.
Cellular CSR is the update-aware atlas layer.
The best v003 architecture is flat CSR inside cells plus an exact global flat
stream, not cells instead of flat CSR.
```

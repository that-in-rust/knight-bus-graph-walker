# Arch 01: CSR Multiple Options

This is the canonical v003 architecture decision ledger for OLAP storage. It
preserves the serious snapshot-oriented architectures we considered, why each
was attractive, what broke under the PRD constraints, and how the current
preferred architecture emerged.

Read this file as:

```text
top-level summary = current conviction
option ledger     = how we got there
decision tests    = how we avoid fooling ourselves
```

This file deliberately removes query-time mutation layers from the option set.
The v003 architecture assumes OLAP answers are exact as of a published snapshot
watermark. Freshness is improved by publishing newer snapshots, not by merging
new writes into every query path.

## Top-level decision summary

Current preferred architecture:

```text
OLTP truth
  -> verified OLAP pre-dataset / Projection Build Store
  -> immutable CSR snapshot publication
       flat global CSR first
       cellular CSR packaging after measurement
       sidecars for labels, relationship types, weights, and properties
  -> snapshot-as-of OLAP/GDS queries
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
Cellular CSR becomes a packaging, locality, and bounded rebuild strategy.
Snapshot generations are the freshness mechanism.
```

## Core facts

```text
1. OLTP remains Neo4j-shaped and authoritative.
2. OLAP is allowed to lag OLTP.
3. Every OLAP answer must expose the snapshot watermark it used.
4. The scarce resource is holistic RAM on an 8GB machine.
5. Flat dual CSR is already the best current physical seed.
6. The missing layer is the durable Projection Build Store before CSR.
7. Cells are useful only after the build-source and watermark model is clean.
8. No option in this ledger uses query-time mutation merging as architecture.
```

Correctness rule:

```text
If a query uses snapshot W, the answer must be exact as of W.
If OLTP is newer than W, the reported freshness lag must say so.
If users require fresher OLAP, the system publishes a newer snapshot.
```

## Why this decision changed

The earlier debate was framed as:

```text
Option 1: one big flat immutable dual CSR
Option 2: cellular CSR storage
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
holistic RAM on an 8GB machine. If OLAP lag is acceptable, serving reads should
not pay query-time write reconciliation cost. Build work belongs in the
publication pipeline.

## Read/write paths and the build/control plane

The premise behind the entire v003 architecture:

```text
OLTP reads/writes:
  run on Neo4j-shaped OLTP storage.

OLAP reads:
  run only on published OLAP-optimized snapshot storage W.

Middle layer:
  exists only to manufacture, verify, compact, partition, and publish OLAP snapshots.
  It is not a query-serving layer.
```

The Projection Build Store is not a serving overlay. It is a
**build/control plane**.

ASCII view of the strict path separation:

```text
                   READ/WRITE PATHS

OLTP query/write
      |
      v
+-----------------------------+
| Neo4j-shaped OLTP storage   |
| records / WAL / tx / locks  |
+-----------------------------+
      |
      | committed facts / receipts
      v
+-----------------------------+
| Projection Build Store      |   <-- not queried by users
| analytical IR / build plane |
+-----------------------------+
      |
      | compile / validate / publish
      v
+-----------------------------+
| OLAP snapshot W             |
| Flat CSR / sidecars / cells |
+-----------------------------+
      |
      v
OLAP query exact as of W
```

### What the middle layer is NOT

```text
not OLTP source of truth
not the OLAP query engine
not a freshness overlay
not an LSM serving layer
not a second database users query directly
```

### What the middle layer IS

```text
a verified, durable, low-RAM, build-friendly representation of graph facts
used to create one or more OLAP-optimized snapshots
```

### Compiler IR analogy

The mental model is a compiler pipeline:

```text
OLTP records       = source code
Projection Store   = intermediate representation
CSR snapshot       = optimized machine code
OLAP runtime       = CPU executing machine code
```

Formal name when the compiler-IR framing matters:

```text
Analytical Projection IR Store
```

### Snapshot foundry metaphor

```text
OLTP emits ore.
Projection Store refines ore into standard ingots.
Snapshot compiler casts ingots into specialized tools:
  flat CSR
  cellular CSR
  property sidecars
  model/result sidecars
  memory estimates
  catalog manifests
```

The foundry is never on the OLAP read path. Without it, every "tool" has to
be hand-made directly from raw OLTP ore - which means every builder must redo
dictionaries, sorting, dedup, validation, crash recovery, and watermark
accounting.

### Why this is powerful

Without the middle layer the CSR builder owns everything:

```text
understand OLTP layout
resolve deletes
map IDs
sort edges
build dictionaries
build sidecars
estimate memory
recover from crashes
validate correctness
```

With the middle layer the system becomes modular:

```text
OLTP adapter produces facts.
Projection Store verifies facts.
Snapshot compiler emits physical layouts.
OLAP runtime only reads published snapshots.
```

### Optimized for building, not serving

Each plane is shaped by a single primary goal:

```text
OLTP storage optimized for:
  transactions
  locks
  indexes
  point reads
  Cypher-facing correctness

Projection Build Store optimized for:
  low-RAM ingestion
  sorting
  normalization
  verification
  snapshot compilation
  sidecar construction
  partition experiments

OLAP snapshot optimized for:
  fast reads
  bounded memory
  sequential scans
  algorithm execution
  GDS-style procedures
```

Build work belongs in the middle plane; serving reads stay on the snapshot.

## Architecture options ledger

We currently have six options, all snapshot-oriented:

```text
+----+--------------------------------------------+--------------------------------------------+------------------------------+
| ID | Architecture                               | Shape                                      | Current verdict              |
+----+--------------------------------------------+--------------------------------------------+------------------------------+
| A  | Direct flat CSR snapshot                   | OLTP/source -> rebuild -> flat dual CSR    | Primitive and fallback       |
| B  | Projection Store -> flat CSR               | OLTP -> analytical facts -> flat CSR       | Best MVP baseline            |
| C  | Projection Store -> flat CSR + sidecars     | B + labels/types/properties/weights        | Required API expansion path  |
| D  | Projection Store -> cellular CSR snapshots  | OLTP -> facts -> partitioned CSR cells     | Measured evolution target    |
| E  | Projection Store -> hybrid flat + cellular  | one global stream plus cell packages       | Preferred mature direction   |
| F  | Multi-generation snapshot catalog           | publish/swap/retain immutable generations  | Required operations layer    |
+----+--------------------------------------------+--------------------------------------------+------------------------------+
```

### Option A: Direct flat CSR snapshot

```text
OLTP/source
  -> full snapshot build
  -> one immutable flat dual CSR
  -> OLAP queries exact as of that snapshot
```

Why it was attractive:

```text
simple
compact
already close to current Knight Bus
excellent full-graph sequential scans
low conceptual complexity
```

What broke:

```text
weak update-to-OLAP publication story
no durable analytical staging layer
labels, types, weights, and properties are not first-class enough
freshness only improves when a whole new generation is built
```

Current verdict:

```text
Keep as the physical primitive, first implementation target, and correctness
oracle. Do not treat it as the whole v003 architecture.
```

### Option B: Projection Store -> flat CSR

```text
OLTP truth
  -> Projection Build Store
  -> immutable flat dual CSR snapshot
  -> snapshot-as-of OLAP queries
```

Why it is stronger than A:

```text
separates transactional truth from analytical build facts
normalizes labels, relationship types, properties, and IDs before CSR build
makes source watermarks explicit
keeps serving reads simple
keeps query RAM predictable
```

What still needs work:

```text
the Projection Build Store needs crash recovery and checksums
snapshot builds still need bounded scratch memory
property and result sidecars still need design
full GDS procedure surface still needs registration and modes
```

Current verdict:

```text
Best MVP baseline.
```

### Option C: Projection Store -> flat CSR + sidecars

```text
OLTP truth
  -> Projection Build Store
  -> flat dual CSR topology
  -> columnar sidecars:
       labels
       relationship types
       weights
       node properties
       relationship properties
       result columns
  -> GDS-compatible procedure surface
```

Why it matters:

```text
Neo4j/GDS compatibility is not topology alone.
Algorithms need labels, relationship types, weights, properties, modes,
estimates, writeback/result artifacts, and catalog metadata.
```

What can go wrong:

```text
sidecars can duplicate too much data
property dictionaries can exceed the memory plan
filtered projections can accidentally materialize large temporary graphs
writeback/result columns can become hidden RAM sinks
```

Current verdict:

```text
Required API expansion path after B.
```

### Option D: Projection Store -> cellular CSR snapshots

```text
OLTP truth
  -> Projection Build Store
  -> partitioned snapshot compiler
  -> immutable CSR cells
  -> global logical stream for whole-graph algorithms
  -> cell-local reads for locality-heavy workloads
```

Why it was attractive:

```text
bounded physical packages
locality-aware reads
smaller rebuild units during snapshot publication
natural place for cell passports, histograms, and planning metadata
clear attachment point for sidecars
```

What can go wrong:

```text
bad partitioning can create too many boundary edges
global algorithms can slow down if the logical stream is not excellent
metadata can grow into its own RAM problem
cell packaging can become complexity without measurable benefit
```

Current verdict:

```text
Measured evolution target, not the first mandatory default.
```

### Option E: Projection Store -> hybrid flat + cellular publication

```text
Projection Build Store
  -> publish one exact global flat stream
  -> publish cell packages from the same facts and watermark
  -> planner chooses:
       global stream for whole-graph algorithms
       cell packages for locality-heavy queries
```

Why this is the mature direction:

```text
keeps the flat CSR advantage for global scans
keeps cells available for locality and bounded rebuilds
keeps one source watermark
avoids making cells the only physical truth
lets benchmarks choose the serving path per workload
```

What can go wrong:

```text
publishing both layouts increases disk footprint
compiler must prove both layouts represent the same watermark
test harness must check parity between global stream and cells
planner mistakes can choose the slower path
```

Current verdict:

```text
Preferred mature architecture after B/C are proven.
```

### Option F: Multi-generation snapshot catalog

```text
generation N:
  manifest + topology + sidecars + watermark Wn

generation N+1:
  manifest + topology + sidecars + watermark Wn+1

runtime:
  atomically switches active generation
  retains old generations while readers finish
  garbage-collects unreferenced generations
```

Why it matters:

```text
freshness is publication cadence
readers get stable immutable files
rollbacks are possible
crash recovery is simpler than mutating live query state
operations can reason about what is published
```

What can go wrong:

```text
disk usage can grow if generations are not reclaimed
long-running readers can pin old generations
build failures must not corrupt active snapshots
watermark reporting must be exact
```

Current verdict:

```text
Required operations layer.
```

## Decision ladder

```text
Start with A because current Knight Bus proves the flat CSR primitive.
Reject A alone because v003 needs a real OLTP-to-OLAP build path.
Promote B because it gives clean watermarks and predictable serving RAM.
Add C because Neo4j/GDS compatibility requires more than topology.
Measure D because cells may improve locality and bounded rebuild work.
Prefer E as the mature target if cells prove useful without hurting scans.
Require F because snapshot publication is the freshness and operations model.
```

## Comparison by PRD obligation

| PRD obligation | A flat only | B store -> flat | C flat + sidecars | D store -> cells | E hybrid | F generations |
| --- | --- | --- | --- | --- | --- | --- |
| Neo4j-shaped OLTP remains truth | yes | yes | yes | yes | yes | yes |
| Lowest serving RAM | strong | strong | medium/strong | strong if partitions good | strongest if planner is good | neutral |
| Holistic RAM accounting | easiest | clear: build vs serve separated | must budget sidecars | must budget cell metadata | must budget duplicate publication | must budget retained files |
| Accepted OLAP lag | yes | yes, explicit | yes, explicit | yes, explicit | yes, explicit | yes, explicit |
| Small update ingestion | weak | good in build store | good in build store | good in build store | good in build store | publication-based |
| Snapshot exactness | strong | strong as of W | strong as of W | strong as of W | parity must be proven | strong per generation |
| Global algorithm scans | excellent | excellent | excellent with sidecar discipline | must prove stream adapter | excellent through flat stream | neutral |
| Locality / bounded rebuilds | weak | medium | medium | strong | strong | medium |
| Implementation complexity | low | medium | medium/high | high | high | medium |

## Current recommendation

Use this as the canonical plan:

```text
Required v003 foundation:
  1. OLTP truth remains Neo4j-shaped.
  2. Projection Build Store stores normalized analytical facts.
  3. Immutable CSR snapshots compile from that store at source watermarks.
  4. OLAP queries report `as_of_watermark`.
  5. Flat dual CSR remains the first physical snapshot target.
  6. Sidecars expand the flat snapshot into the required Neo4j/GDS surface.
  7. Snapshot generations provide freshness publication and rollback.

Next evolution:
  8. Cellular CSR compiles from the same pre-dataset when spikes prove value.
  9. Hybrid publication keeps a global flat stream plus cell packages.
```

If somebody asks "what are we optimizing for?", the answer is:

```text
Predictable holistic RAM on 8GB machines.
Exact answers as of a stated snapshot watermark.
Architecture that can add cell locality without corrupting the base model.
```

If somebody asks "what are we not optimizing for?", the answer is:

```text
Zero-lag OLAP at all costs.
Always-fresh query-time mutation semantics.
Partitioning theory before measured evidence.
```

## Architecture conviction map

```text
                      PRD accepts OLAP lag
                              |
                              v
                 serving reads stay snapshot-only
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
       sidecars make the snapshot Neo4j/GDS-compatible
                              |
                              v
          flat CSR first, cellular CSR as packaged evolution
                              |
                              v
       generations publish freshness through atomic snapshot swaps
```

ELI5:

```text
OLTP truth        = author's live manuscript
Projection Store  = editor's verified notes
Flat CSR snapshot = printed book
Cellular CSR      = printed book split into chapters
Generations       = editions with publication dates
```

Do not rewrite the book while a reader is reading it. Publish a new edition and
make the publication date visible.

## Canonical file policy

This file should be the place where all serious CSR/OLAP architecture options
live. Earlier single-purpose notes can exist only if they are explicitly marked
as historical or are replaced by this file.

Recommended repo convention:

```text
Arch-01-CSR-multiple-options.md = canonical decision ledger
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
                 | flat CSR + sidecars             |
                 | exact as of tx/time W           |
                 +----------------+----------------+
                                  |
                                  | publish generation
                                  v
                 +---------------------------------+
                 | 4. SNAPSHOT CATALOG             |
                 | active/previous generations     |
                 | atomic swap + rollback          |
                 +----------------+----------------+
                                  |
                                  | mmap fast mode / O_DIRECT strict mode
                                  v
                 +---------------------------------+
                 | 5. OLAP QUERY VIEW              |
                 | walks / GDS / PageRank / BFS    |
                 | exact as of active generation   |
                 +---------------------------------+
```

## Dataset model

There are three foundational data planes plus the publication catalog:

| Layer | Role | Mutable? | Source of truth? | Query-time default? |
| --- | --- | --- | --- | --- |
| OLTP truth | Neo4j-shaped transactional records, WAL, locks, indexes | yes | yes | OLTP only |
| Projection Build Store | normalized analytical facts verified from OLTP | append/merge-friendly | no | no, build source |
| Immutable CSR snapshot | read-optimized projection exact at watermark W | no | no | yes for OLAP |
| Snapshot catalog | active generation pointer, retention, rollback, manifests | metadata only | no | chooses active snapshot |

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
| Projection Build Store                      |
| nodes, edges, labels, reltypes, properties  |
| dictionaries, tx watermarks, checksums       |
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
                      | publish as generation N
                      v
+---------------------------------------------+
| Snapshot catalog                            |
| active pointer + retention policy           |
+---------------------+-----------------------+
                      |
                      v
+---------------------------------------------+
| OLAP/GDS query runtime                      |
| exact answers as of W                       |
+---------------------------------------------+
```

## What the Projection Build Store stores

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
valid_to_tx or deletion marker
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

### Suggested on-disk layout

The contents above naturally group into a small number of durable
subdirectories:

```text
Projection Build Store contents:

/manifests/
  source_watermark
  schema_version
  dictionary_version
  fact_counts
  checksums

/facts/
  nodes
  relationships
  labels
  relationship_types
  node_properties
  relationship_properties
  deletes_or_validity_ranges

/dictionaries/
  external_node_id -> dense_node_id
  label_name -> label_id
  rel_type_name -> rel_type_id
  property_key -> property_id

/statistics/
  node_count
  rel_count
  degree_histograms
  label_histograms
  reltype_histograms
  property_widths
  null_counts
  min/max

/build_runs/
  sorted_edge_runs
  partition_candidates
  validation_reports
  memory_estimates
```

This is what makes the store a **buildable** artifact instead of just a fact
bag: manifests, facts, dictionaries, statistics, and reusable build-run
intermediates each live in their own area.

### Creative uses of the middle layer

Treating the Projection Build Store as a build/control plane unlocks a wide
set of capabilities that the OLAP read path never has to know about:

| # | Use | What the middle layer does | Why it helps |
| --- | --- | --- | --- |
| 1 | Semantic normalization | Converts Neo4j records/WAL into node, rel, label, type, property facts | CSR builder does not need to understand OLTP internals |
| 2 | Watermark ledger | Tracks exactly which tx/source generation the analytical facts represent | Every snapshot can say "exact as of W" |
| 3 | Dense-ID factory | Maintains stable external-id -> dense-id mappings | CSR arrays stay compact and reproducible |
| 4 | Dictionary factory | Builds label/type/property dictionaries once | Sidecars and snapshots share the same compact IDs |
| 5 | Sort staging | Pre-sorts edges by source, target, type, partition | CSR build becomes sequential and low-RAM |
| 6 | Dedup / coalescing | Resolves repeated property changes, duplicate rel facts, deletes before snapshot compile | Avoids query-time reconciliation |
| 7 | Snapshot compiler cache | Stores intermediate sorted runs / checkpoints | Failed builds can resume; builds need less RAM |
| 8 | Multi-target compiler source | Emits flat CSR, cellular CSR, sidecars, Arrow/Parquet-like columns, result stores | One truth, many physical layouts |
| 9 | Validation oracle | Compares OLTP facts, expected counts, checksums, labels/types/properties | Prevents publishing corrupt snapshots |
| 10 | Memory planner input | Stores counts, histograms, cardinality, degree distribution, property widths | Planner can estimate RAM before running algorithms |
| 11 | Partition lab | Tests candidate cell partitions before writing cellular snapshots | Cells become measured, not theoretical |
| 12 | Sidecar builder | Produces node labels, rel types, weights, features, embeddings, result columns | Full GDS surface becomes possible without changing topology |
| 13 | Compatibility bridge | Preserves GDS projection/catalog metadata shape | Helps emulate Neo4j GDS product semantics |
| 14 | Reproducibility ledger | Given watermark W, rebuild the same snapshot bytes or explain why not | Makes bugs falsifiable |
| 15 | Publication gate | Only publishes snapshots that pass count/checksum/schema/memory tests | OLAP reads never see half-built state |
| 16 | Offline optimizer | Tries compression, ordering, partitioning, sidecar layout experiments | Improves future snapshots without touching OLTP |
| 17 | Disaster recovery aid | Rebuilds OLAP snapshots from durable facts after crash | OLAP storage can be disposable / rebuildable |
| 18 | Build scheduling brain | Decides when to publish next snapshot based on dirty size, time, RAM budget, SLA | Freshness comes from snapshot cadence, not query merge |

Each of these capabilities lives entirely off the OLAP read path. None of
them require the OLAP runtime to know that the middle layer exists.

## Freshness model

Key invariant:

```text
CSR snapshot has watermark W.
Projection Build Store is verified through watermark T.
OLAP query answer is exact as of W.
If T > W, the difference is compiler/publication lag.
To improve OLAP freshness, publish a newer snapshot.
```

Therefore:

```text
pre-dataset sync proves build-source correctness.
snapshot watermark defines query correctness.
snapshot generation publication defines OLAP freshness.
```

If v003 accepts lag, use this:

```text
query freshness = active snapshot watermark
pre-dataset freshness = OLTP watermark or near it
compiler lag = pre-dataset watermark - snapshot watermark
```

### Snapshot publication procedure

Given the build/control-plane framing, publication is a fixed sequence:

```text
1. Read Projection Build Store at watermark W.
2. Validate facts and dictionaries.
3. Build flat CSR topology.
4. Build sidecars.
5. Optionally build cellular packages from the same W.
6. Run parity checks (cell vs global, sidecar vs facts, counts/checksums).
7. Publish generation N atomically through the snapshot catalog.
8. OLAP queries read only generation N and report as_of_watermark = W.
```

If any step before 7 fails, generation N is not published and OLAP keeps
reading the previous generation. This is what makes the middle layer a
**publication gate**, not just an ingestion buffer.

## How this changes the earlier Cellular CSR thesis

Earlier:

```text
Cellular CSR was treated as the main answer to update-aware OLAP.
```

Corrected:

```text
Projection Build Store is the freshness/build-source layer.
Snapshot generations are the publication mechanism.
Cellular CSR is the packaging, planning, locality, and bounded rebuild layer.
```

Flat CSR still matters:

```text
Flat CSR = canonical byte primitive and global stream.
Cells = bounded packaging around that primitive.
Pre-dataset = durable normalized source for generating either one.
```

## Holistic RAM reasoning

Serving queries should not reconcile write state. They should read a stable
snapshot and allocate only:

```text
CSR file windows or explicit stream buffers
sidecar file windows or explicit stream buffers
algorithm state
result buffers
planner metadata
```

Build work happens outside the serving read path:

```text
query path: active CSR snapshot only
build path: pre-dataset -> sorted/partitioned CSR under memory budget
publication path: atomic generation swap
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
| sidecar parity test | label/type/property sidecars match source facts at W |
| cell/global parity test | cellular snapshot equals global flat stream at W |
| generation swap test | readers see either generation N or N+1, never a partial build |
| generation retention test | old snapshots are reclaimed only after readers release them |
| memory-contract test | query RSS/PSS fits estimate for topology, sidecars, and algorithm state |

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

3. Define snapshot freshness:

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
}
// SnapshotOnly is the v003 mode.
```

5. Define snapshot generation catalog:

```rust
pub struct SnapshotGeneration {
    pub generation_id: u64,
    pub source_watermark: SourceWatermark,
    pub manifest_path: String,
    pub published_at_ms: u64,
}

pub struct SnapshotCatalog {
    pub active_generation: u64,
    pub retained_generations: Vec<SnapshotGeneration>,
}
// Publication is an atomic active_generation change after validation.
```

## Chain of verification

Fact-check questions:

1. Does OLAP generally allow lag?
2. Does accepting lag allow a snapshot-only query contract?
3. Does a pre-dataset replace OLTP truth?
4. Does a pre-dataset replace CSR?
5. Does this improve the 8GB RAM story?
6. Do cells need to be first?
7. How does freshness improve without query-time write reconciliation?

Answers:

1. Yes. Many OLAP systems expose batch, CDC, or projection freshness rather than
   zero-lag answers.
2. Yes, if query semantics are exact as of snapshot watermark.
3. No. OLTP remains authoritative.
4. No. It is build-source IR; CSR remains read-optimized physical format.
5. Yes, because write reconciliation and sorting happen in the build path,
   while serving reads use stable snapshot files.
6. No. Flat CSR is the right first target; cells should be promoted by measured
   evidence.
7. By reducing build latency, publishing generations more often, and reporting
   the active watermark honestly.

Weaknesses:

```text
The pre-dataset adds disk footprint.
It needs crash recovery and idempotent ingestion.
It needs verification tooling.
It does not make CSR builds free; it only makes them cleaner and more bounded.
Snapshot-only semantics accept lag rather than hiding it.
```

Final corrected response:

```text
The foundational v003 pipeline should be:

OLTP truth
  -> verified Projection Build Store
  -> immutable flat or cellular CSR snapshots
  -> snapshot catalog with atomic generation publication
  -> snapshot-as-of OLAP queries.

No query-time mutation layer belongs in the architecture options.
```

## PRD constraint analysis

The PRD constraints from `v003-prd.md` are unusually strict:

```text
exact same APIs or surface area with ZERO changes
identical architecture for OLTP queries
lowest RAM custom storage formats for OLAP queries
50 GB data processed comfortably on 8 GB systems
OLAP RAM includes heap, page cache, duplicate layouts, compaction buffers,
snapshot build scratch, indexes, sidecars, and algorithm intermediates
O_DIRECT + compio for strict file-data RAM control
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
v003 does not need one magic graph layout.
v003 needs a dual-plane system:
  OLTP plane: Neo4j-shaped mutable records/WAL/locks/indexes.
  OLAP plane: CSR-derived snapshots with exact memory contracts.
```

Flat dual CSR already satisfies the physics-primitive part of the OLAP plane.
The Projection Build Store and sidecars address the missing semantic pieces:

```text
freshness watermark
labels
relationship types
properties
weights
writeback/result artifacts
procedure metadata
memory estimates
catalog entries
```

Cellular CSR can later address:

```text
locality unit
bounded rebuild unit
planning unit
sidecar attachment unit
```

But algorithm state remains independent of cell layout. PageRank, Louvain, KNN,
embeddings, and ML can still be dominated by vectors, heaps, candidate pairs, or
models. Cells improve packaging and locality; they do not erase global state.

## Current Knight Bus architecture

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
OLTP updates feeding OLAP publication
50GB data on 8GB systems
holistic RAM accounting
```

Current flat CSR is not enough by itself because:

| Limit | Consequence |
| --- | --- |
| Immutable snapshot | OLAP freshness requires publishing a newer generation |
| One global file set | Local rebuild work still interacts with global layout |
| mmap page cache | RAM residency is decided by the OS, not the planner |
| Topology-first format | Labels, relationship types, and properties need sidecars |
| Walk-focused runtime | PageRank, BFS, SSSP, k-core, Louvain, triangle count, etc. need additional execution/storage support |
| No cell boundaries | No natural unit for bounded rebuild or locality-aware planning |

## Flat CSR option details

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

### Publication path

```text
1. OLTP update commits in Neo4j-shaped store.
2. Projection Build Store records verified analytical facts.
3. Snapshot generation N+1 is built from facts through watermark W.
4. Generation N+1 is validated.
5. Runtime atomically switches active generation from N to N+1.
```

This is clean and safe, but it is not update-local.

### RAM behavior

Flat CSR has three RAM personalities:

| Mode | RAM behavior |
| --- | --- |
| mmap traversal | Low explicit heap, but OS page cache controls real residency |
| O_DIRECT global streaming | More planner-controlled file-data RAM if implemented carefully |
| builder | Low-RAM external sort can bound process RSS |

For a 50GB-class graph, current notes estimate roughly:

```text
nodes: ~200M
edges: ~1B
flat CSR topology + key structures: ~15-20GB on disk
PageRank score arrays: ~3.2GB for two f64[200M] vectors
```

## Cellular CSR option details

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
source watermark
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
  -> planner chooses RAM contract
  -> stream all cells in logical CSR order
  -> use strict streaming mode for global scans when needed
```

### Publication path

OLTP still commits first to the Neo4j-shaped store.

Then:

```text
Neo4j-shaped committed transaction
  -> Projection Build Store fact rows
  -> snapshot compiler maps facts to cell partition
  -> compiler writes complete generation N+1
  -> validation checks cell/global parity
  -> catalog publishes generation N+1 atomically
```

Example:

```text
CREATE (a)-[:FOLLOWS]->(b)
  -> OLTP commit is authoritative
  -> fact row records edge_add(src=a, dst=b, type=FOLLOWS)
  -> next published generation includes the edge
  -> queries before publication report older watermark
  -> queries after publication report newer watermark
```

### RAM behavior

Cellular CSR aims to make RAM explicit:

```text
local query RAM = selected cell windows + small metadata
regional query RAM = wavefront cells + boundary buffers
global query RAM = stream buffers + algorithm state
publication RAM = bounded compiler scratch
```

The point is not that every query uses less heap than flat CSR.

The point is:

```text
global surprise RAM becomes planned and measured RAM.
```

## Head-to-head comparison

### High-level comparison

| Dimension | Flat immutable dual CSR | Cellular CSR snapshots | Better? |
| --- | --- | --- | --- |
| Core storage | One global dual CSR | Many bounded dual-CSR cells | depends on workload |
| Current implementation maturity | Already exists | New design | flat |
| Static walk latency | Excellent | Similar, sometimes slightly better/worse | tie |
| Full graph scan | Excellent sequential base | Must provide logical global stream | flat unless optimized |
| OLTP-to-OLAP bridge | Publish new generation | Publish new generation from partitioned facts | tie; cells may rebuild less |
| Freshness granularity | Snapshot-level | Snapshot-level | tie |
| Publication unit | Whole snapshot | Cell or cell batch inside a complete generation | cellular |
| Page-cache control | mmap-heavy; OS decides | cell windows + strict stream mode | cellular |
| Full OLAP API readiness | Needs major additions | Natural sidecar/catalog attachment points | cellular |
| Disk compactness | Minimal | extra metadata and boundaries likely | flat |
| Complexity | Low | Medium-high | flat |

### Quantified estimate for 50GB dataset on 8GB system

Assumptions:

```text
Neo4j-shaped dataset: ~50GB
Approx graph shape: ~200M nodes, ~1B edges
Flat CSR topology/key structures: ~15-20GB on disk
Usable RAM for OLAP after OS/minimal server overhead: ~5-6GB
```

| Workload / metric | Flat CSR | Cellular CSR | Improvement / regression |
| --- | ---: | ---: | --- |
| Topology disk size | ~15-20GB | ~16-23GB | cellular is likely larger |
| Metadata resident RAM | small runtime structs + mmap VMAs | compact cell passports and boundary metadata | cellular worse if over-cached |
| 1-hop / 2-hop static walk | micro-ms to ms after page-in | micro-ms to ms after cell page-in | roughly equal |
| Local neighborhood query | may fault arbitrary global pages | bounded to local cells when partitioning is good | possible 2-10x less page-cache churn |
| Multi-cell traversal | direct CSR offsets | boundary routing + cell wavefront | can be better or worse |
| Exact global PageRank | dominated by score arrays and scans | dominated by score arrays and scans | roughly equal if stream adapter is good |
| Strict global PageRank | strict stream buffers + algorithm state | strict stream buffers + algorithm state | roughly equal |
| Small update freshness | requires publishing newer generation | requires publishing newer generation | tie at query contract level |
| Publication scratch | global rebuild budget | cell/batch-oriented compiler budget | cellular can be better |
| Full OLAP property filters | not first-class in current v2 | typed per-cell sidecars | cellular major improvement |
| Operational complexity | low | 2-4x higher | cellular worse |

### Truth-check table

This table separates supported claims from plausible but unproven claims.

| Claim | Verdict | Correction / nuance |
| --- | --- | --- |
| Current Knight Bus flat dual CSR is the right seed. | True | Supported by the repo's current snapshot/runtime shape and low-RAM builder. |
| Cellular CSR is "CSR made locality-aware and RAM-budgetable." | Mostly true | True only if cells are real query, publication, and planning units, not just folders. |
| Cellular CSR is better for static walks. | Not proven | Flat CSR may be equal or faster. Cells add boundary and mapping overhead. |
| Cellular CSR is better for small update freshness. | Not by itself | Both serve published snapshots. Cells may reduce publication work, but freshness is still generation-based. |
| Cellular CSR is better for full global algorithm speed. | False / not proven | Flat CSR is already close to ideal for sequential scans. Cells may slow global scans unless the logical stream adapter is excellent. |
| Cellular CSR improves holistic RAM. | Partially true | Strong for local reads and bounded publication work. Neutral for global PageRank/Louvain/KNN when algorithm state dominates. |
| `O_DIRECT` gives deterministic RAM. | Mostly true but overstrong | It bypasses page cache, but alignment buffers, block-device behavior, kernel accounting, I/O scheduling, and algorithm arrays still matter. |
| 50GB graph -> flat CSR topology/key structures around 15-20GB. | Plausible | Raw dual CSR for 200M nodes / 1B directed edges stored in both forward and reverse form is about 11.2GB before keys/properties. |
| PageRank two `f64[200M]` vectors = about 3.2GB. | True | `2 * 200M * 8 = 3.2GB`. Two `f32` vectors would be about 1.6GB. |
| Exact global PageRank in 10-30s. | Too confident | Say "tens of seconds to minutes" until benchmarked. |
| Cellular topology overhead 5-20%. | Plausible, unproven | Boundary indexes, passports, per-cell offsets, and sidecars could push overhead higher. |
| Metadata RAM 300MB-1.5GB. | Plausible but design-dependent | Compact passports should be far smaller; boundary indexes and cached sidecars may dominate. |
| Full OLAP API readiness improves with cells. | True as architecture | Cells do not solve all algorithms. Global algorithms still need global state, sidecars, and result storage. |
| Partitioning is the biggest cellular risk. | True | Bad partitioning can erase locality and add random I/O. |

### Evidence and verification notes

These are supporting references, not benchmark proof.

| Evidence | What it supports |
| --- | --- |
| Current Knight Bus `snapshot.rs` writes flat `forward.offsets`, `forward.peers`, `reverse.offsets`, `reverse.peers`, node table, string table, and key index. | Flat dual CSR is the implemented primitive today. |
| Current Knight Bus `runtime.rs` opens the snapshot through mmap and exposes walk-focused runtime methods. | Current execution is static and traversal-oriented, not full GDS. |
| Neo4j GDS source includes `CSRGraphStoreFactory`, `CSRGraphStore`, `GraphStoreCatalog`, `HugeGraph`, and memory estimate result types. | GDS uses a separate projected graph plane and takes memory estimation seriously. |
| Neo4j GDS `HugeGraph` comments describe target IDs sorted, compressed, and written as variable-length vlongs with offset-based access. | Compressed CSR-like layouts are a real GDS design pattern. |
| GraphChi uses vertex intervals and shards for out-of-core graph processing. Source: [GraphChi OSDI/USENIX](https://www.usenix.org/conference/osdi12/126-graphchi-large-scale-graph-computation-just-pc). | Cell/shard-style graph execution has strong precedent. |
| Linux page-cache documentation. Source: [Linux page cache](https://www.kernel.org/doc/html/v6.13/mm/page_cache.html). | mmap/page-cache residency is not fully planner-controlled. |

Anything above that references GraphChi or Linux kernel docs is external
evidence, not proven by this repository. It should be independently verified
before being treated as an implementation guarantee.

## Where Cellular CSR is better

### 1. Locality

Flat CSR:

```text
local traversal
  -> dense ids may map to distant file pages
  -> OS page cache may load unrelated graph regions
```

Cellular CSR:

```text
local traversal
  -> dense id maps to cell
  -> planner opens relevant cell windows
  -> boundary expansion is explicit and measurable
```

Expected magnitude:

```text
local workloads: possibly 2-10x less page-cache churn
global workloads: similar RAM if both use strict streaming
```

### 2. Bounded publication work

Flat publication:

```text
new generation
  -> global sort/build
  -> validate whole snapshot
  -> publish generation
```

Cellular publication:

```text
new generation
  -> partition facts
  -> write cell packages
  -> validate cell/global parity
  -> publish generation
```

Cells can make compiler scratch and validation more local, but the published
query contract remains snapshot-as-of.

### 3. Full OLAP API path

Flat CSR is excellent for neighbor iteration, but v003 needs the full OLAP
surface:

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
tens of seconds to minutes; with strict 8GB/streaming/spill mode, minutes is
more realistic until benchmarked.
```

### 2. Disk compactness

Flat CSR is more compact.

Cellular CSR adds:

```text
per-cell offsets
passports
boundary indexes
sidecar metadata
```

Expected result:

```text
5-20% worse topology disk footprint
```

This overhead is acceptable only if locality, bounded publication, and planning
benefits matter.

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
cell package publication
global stream adapter
```

Expected result:

```text
2-4x more storage/runtime complexity
```

## Rubber-duck debugging

### Duck: Is Cellular CSR actually a new architecture or just folders?

It is a new architecture if cells are query, publication, planning, and memory
budget units.

It is just folders if cells are only a file organization trick.

The required invariant:

```text
Every cell must be independently readable, validatable, publishable,
and budgetable.
```

### Duck: Does Cellular CSR reduce RAM, or just move it around?

Both.

For global algorithms, it mostly moves RAM control into strict streaming and
explicit algorithm state. It may not reduce total RAM versus a flat strict
global stream.

For local workloads, it can genuinely reduce holistic RAM pressure by
preventing unrelated graph regions from entering page cache or scratch space.

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

### Duck: What replaces query-time mutation reconciliation?

Snapshot publication.

The rule is:

```text
Do not mutate the served graph while answering OLAP.
Build and validate a newer generation, then publish it atomically.
```

This is less fresh than always-current serving reads, but it is easier to reason
about, easier to test, and safer for the 8GB RAM objective.

### Duck: If OLAP never reads the middle layer, why have it?

Because the hard part is not only reading CSR.

```text
The hard part is reliably manufacturing correct, compact, low-RAM,
GDS-compatible CSR snapshots from Neo4j-shaped truth.
```

The middle layer is where dictionaries, dedup, sort staging, validation,
memory planning, partition experiments, and reproducibility live. The OLAP
runtime stays small because the build plane absorbs that complexity.

### Duck: What exact bug would the middle layer catch?

The build/control plane is a publication gate. The bugs it is designed to
catch include:

```text
snapshot missing a relationship type
property default applied incorrectly
deleted relationship still present in CSR
label dictionary mismatch
node dense-id instability across generations
PageRank estimate missing sidecar/result memory
snapshot claims tx 5000 but facts only verified to tx 4992
cellular package differs from global flat stream at the same watermark
```

Each of these is a snapshot-time bug. None of them should be caught at
OLAP-query time.

### Duck: What should not happen at query time?

```text
OLAP query should not read OLTP records.
OLAP query should not read the Projection Build Store.
OLAP query should not merge fresh writes at query time.
OLAP query should read a published snapshot and report W.
```

These four invariants are the operational definition of "the middle layer is
not a serving layer".

### Final thesis on the middle layer

```text
The middle layer is not a serving layer.
It is the analytical compiler IR and snapshot foundry.

It lets us keep:
  OLTP = Neo4j-shaped and correct
  OLAP = snapshot-only and RAM-bounded
  Build pipeline = rich, verifiable, restartable, and creative
```

## Corrected architecture roles

| Layer | Recommended role |
| --- | --- |
| Flat dual CSR | Keep as the canonical byte primitive for topology and global logical streams. |
| Projection Build Store | Durable source for analytical facts, watermarks, dictionaries, and snapshot builds. |
| Sidecars | Attach labels, relationship types, weights, properties, and result columns without duplicating topology. |
| Cells | Add as bounded publication, planning, and locality units after measurement. |
| Passports | Use for validation, freshness, histograms, query planning, and memory estimates. |
| Boundary indexes | Add after measuring boundary ratio; avoid overbuilding first. |
| Snapshot catalog | Own active generation, retention, rollback, and publication manifests. |
| mmap | Keep for interactive traversal and normal fast mode. |
| O_DIRECT streaming | Use for strict-RAM global algorithms and benchmarked scan paths, not every query. |
| GDS catalog/procedure layer | Treat as API compatibility contract above storage. |

The winning implementation is therefore:

```text
Projection Build Store
+ flat CSR first
+ sidecars
+ snapshot generation catalog
+ optional cellular packaging after measurement
+ memory-contract planner
```

not:

```text
cells instead of flat CSR
```

## Benchmark spikes required before making cells default

| Spike | Must measure | Promotion threshold |
| --- | --- | --- |
| `flat-vs-cell-local-walk` | page faults, RSS/PSS, latency, opened cells, cells touched per query | cells reduce cache churn/latency on locality-heavy queries |
| `flat-vs-cell-global-scan` | scan throughput, file-open overhead, boundary overhead, logical stream adapter penalty | global stream is within an acceptable small overhead of flat CSR |
| `publish-generation-small-change` | build time, scratch RAM, validation cost, publication time | newer generation can be published within accepted freshness target |
| `cellular-publication` | scratch RAM, disk amplification, publish time, crash recovery | publication stays within budget and publishes atomically |
| `partition-quality` | boundary edge ratio, hot hub distribution, opened cells per traversal | partition does not explode boundary traffic |
| `pagerank-8gb` | exact RSS/PSS, wall time, I/O volume, vector pressure, spill volume | memory estimate predicts measured memory; runtime remains acceptable |
| `sidecar-filtered-query` | label/type/property filter selectivity, sidecar page faults, result correctness | sidecars improve filtered OLAP without duplicating full topology |

These spikes decide defaults:

```text
If global scan overhead is high, keep flat CSR global files as primary.
If local walk benefit is high, use cells for locality-heavy queries.
If generation publication is slow, improve compiler and pre-dataset layout.
If partition quality is poor, do not block v003 on partitioning theory.
```

## Decision

Prefer Projection Build Store -> flat CSR + sidecars as the v003 MVP.

Prefer hybrid flat-global plus cellular packaging as the mature v003 direction
only after spikes prove:

```text
boundary ratio is acceptable
metadata and sidecar overhead are acceptable
global scan overhead is acceptable
cellular publication stays under memory budget
snapshot generation publication meets freshness targets
```

The winning design is not:

```text
replace flat CSR completely
```

It is:

```text
make flat CSR the first physical snapshot target and the global stream
```

Final architecture:

```text
Neo4j-shaped OLTP remains the transactional source of truth.
Projection Build Store records verified analytical facts.
Flat CSR is the first immutable OLAP snapshot format.
Sidecars provide labels, types, properties, weights, and results.
Cellular CSR can package the same facts into bounded cells after measurement.
Snapshot generations publish freshness through atomic swaps.
All OLAP reads are exact as of their snapshot watermark.
```

## Final verdict

| Question | Answer |
| --- | --- |
| How many options remain? | Six |
| Is there any query-time mutation layer option? | No |
| Is Flat CSR better for current static walks? | Usually yes |
| Is Cellular CSR better for global algorithm speed? | Not proven; flat CSR is already ideal for scans |
| Is Cellular CSR better for locality and bounded publication work? | Potentially yes, after measurement |
| Is Cellular CSR better for holistic RAM? | Yes for local/planned workloads; neutral for strict global scans when algorithm state dominates |
| Is Cellular CSR simpler? | No |
| Is Projection Build Store mandatory? | Yes, it is the build-source foundation |
| Should we abandon flat CSR? | No |
| Should cells be the immediate default for every workload? | No; prove with spikes first |

One-line conclusion:

```text
Flat CSR is the physical primitive.
Projection Build Store is the analytical source.
Sidecars make the surface area complete.
Cells are a measured packaging evolution.
Snapshot generations are the freshness mechanism.
```

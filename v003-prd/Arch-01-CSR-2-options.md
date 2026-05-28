# Arch 01: CSR, Two Options

Status: **historical / superseded by
`v003-prd/Arch-01-CSR-multiple-options.md`**.

This note is preserved as the earlier flat-vs-cellular argument. The canonical
v003 decision ledger is now `Arch-01-CSR-multiple-options.md`, which keeps this
analysis as historical evidence but adds the missing layer:

```text
OLTP truth
  -> verified OLAP pre-dataset / Projection Build Store
  -> immutable flat/cellular CSR snapshots
  -> snapshot-as-of OLAP queries
  -> optional tail overlay only if freshness SLA requires it
```

Read this file as supporting background, not as the final architecture choice.

This note compares two OLAP storage directions for v003:

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

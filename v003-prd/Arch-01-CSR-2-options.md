# Arch 01: CSR, Two Options

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

## Short answer

Current Knight Bus is already the right seed: a compact immutable dual-CSR
snapshot with a low-RAM builder and mmap traversal runtime.

Cellular CSR is the preferred v003 evolution:

```text
current flat dual CSR
  -> split into bounded graph cells
  -> add cell passports
  -> add boundary indexes
  -> add label/type/property sidecars
  -> add WAL-fed cell-local deltas
  -> add exact global O_DIRECT streaming fallback
```

The main claim:

```text
Cellular CSR is not "faster CSR."
It is CSR made updateable, locality-aware, and RAM-budgetable enough
to serve the full OLAP API surface on 50GB data / 8GB RAM systems.
```

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

## Decision

Prefer Cellular CSR for v003, but keep flat CSR as the internal global stream
model.

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
| Is Cellular CSR better for holistic RAM? | Yes for local/update/compaction workloads; equal for strict global O_DIRECT |
| Is Cellular CSR simpler? | No |
| Is Cellular CSR more PRD-compliant for v003? | Yes |

One-line conclusion:

```text
Current Knight Bus is a perfect flat folded map.
Cellular CSR turns it into a live atlas of small folded map tiles:
slightly worse for pure full-map scans, much better for updates,
locality, compaction, RAM budgeting, and full OLAP API growth.
```

# Storage Runtime Alignment

For a plain-English companion note in the research folder, see [strategic-research/A-20260416121710-storage-runtime-alignment-eli5.md](./strategic-research/A-20260416121710-storage-runtime-alignment-eli5.md).

## Premise Check

`knight-bus-graph-walker` should copy the **runtime shape** that keeps Parseltongue's walk thesis strong, while borrowing only the **durability patterns** that make Apache Iggy operationally sharp.

That means:

- copy from Parseltongue:
  - dense IDs
  - dual forward and reverse adjacency
  - build-time heavy, walk-time boring
  - exact lookup separated from traversal
- copy from Iggy:
  - payload bytes shaped for the read path
  - tiny sidecar indexes instead of query-engine indirection
  - immutable sealed artifacts
  - explicit validation and restart reconstruction
- do **not** copy from Iggy:
  - broker semantics
  - one-record-per-edge storage
  - log polling as the traversal primitive
  - per-hop RPC or database mediation

The important conclusion is:

> the storage is "aligned to runtime" only when the hot traversal path is already visible in the on-disk bytes.

## Evidence Summary

### Parseltongue v301 points to a snapshot-first walk runtime

The strongest repeated ideas in the `parseltongue-rust-LLM-companion` `v301` docs are:

- immutable snapshot artifact
- dense integer IDs
- dual CSR/CSC style adjacency
- `mmap` open path
- build-time preprocessing instead of walk-time reconstruction

The most relevant reference docs were:

- `docs/strategic-research/A-20260408084506-pensieve-runtime-architecture-summary.md`
- `docs/strategic-research/A-20260408140806-walk-runtime-options-explainer.md`
- `docs/strategic-research/A-20260409082931-build-time-walk-time-visualization-eli5.md`
- `docs/strategic-research/A-20260409080037-csr-csc-iggy-graph-walking-eli5.md`

### Apache Iggy stores runtime-shaped bytes plus small seek aids

The Apache Iggy clone confirms a different workload, but a very relevant storage pattern:

- durable payload bytes live in segment files
- small fixed-width sidecar indexes narrow the read
- the active mutable head is separate from sealed immutable data
- restart reconstructs lightweight runtime state from compact durable artifacts

The most relevant files were:

- `core/common/src/types/segment_storage/mod.rs`
- `core/common/src/types/segment_storage/index_reader.rs`
- `core/common/src/types/segment_storage/index_writer.rs`
- `core/common/src/types/message/index.rs`
- `core/common/src/types/message/indexes_mut.rs`
- `core/server/src/streaming/partitions/ops.rs`

The shared lesson is not "use a log".

The shared lesson is:

> persist bytes in the form the runtime wants to read, and keep the lookup aids tiny.

## Chosen Thesis

For `knight-bus-graph-walker`, the runtime-aligned design should be:

1. one immutable graph snapshot directory
2. one dense-ID node space
3. one forward adjacency plane
4. one reverse adjacency plane
5. one tiny exact-key entry path
6. no database or log lookup in the traversal loop

This is closer to:

- Parseltongue's walk runtime thesis for graph shape
- Iggy's segment-plus-index discipline for operational shape

It is **not** closer to:

- a graph database
- a broker-backed graph reader
- an edge table with hot-path query planning

## Concrete Storage Contract

### Snapshot Directory

```text
snapshot/
  manifest.json
  node_table.bin
  strings.bin
  forward.offsets.bin
  forward.peers.bin
  reverse.offsets.bin
  reverse.peers.bin
  key_index.bin          # optional in synthetic mode
```

### Required Types

- node IDs: `u32`
- peer entries: `u32`
- adjacency offsets: `u64`
- string offsets: `u64`

Why `u32` for node IDs:

- keeps adjacency compact
- matches the narrow walk workload
- is enough for the intended laptop-scale graph

Why `u64` for offsets:

- keeps the format safe once peer arrays or string pools exceed 4 GiB
- aligns better with "large artifact, small active working set" goals

### Manifest

The manifest should stay small and explicit:

```json
{
  "version": 2,
  "node_id_width": 32,
  "adjacency_offset_width": 64,
  "node_count": 0,
  "edge_count": 0,
  "key_mode": "deterministic_entity_prefix",
  "storage_mode": "immutable_dual_csr",
  "forward_offsets": "forward.offsets.bin",
  "forward_peers": "forward.peers.bin",
  "reverse_offsets": "reverse.offsets.bin",
  "reverse_peers": "reverse.peers.bin",
  "node_table": "node_table.bin",
  "strings": "strings.bin",
  "key_index": null
}
```

### Node Table

`node_table.bin` should be fixed-width records that let the runtime enrich results without scanning text blobs.

Suggested record:

```text
NodeRecord {
  key_offset: u64,
  key_len: u32,
  flags: u32
}
```

`strings.bin` then holds packed UTF-8 keys and any later display labels.

This keeps the hot walk path array-shaped while still allowing low-cost result materialization.

### Dual Adjacency

The adjacency files should stay as simple as possible:

- `forward.offsets.bin`: `u64[node_count + 1]`
- `forward.peers.bin`: `u32[edge_count]`
- `reverse.offsets.bin`: `u64[node_count + 1]`
- `reverse.peers.bin`: `u32[edge_count]`

Read path:

```text
dense_id
  -> start = offsets[id]
  -> end   = offsets[id + 1]
  -> peers[start..end]
```

That is the graph equivalent of Iggy's "payload file plus tiny index":

- offsets are the seek aid
- peers are the payload

### Key Lookup

There are two acceptable modes.

`v1` synthetic mode:

- key format is deterministic, for example `entity_000123456`
- dense ID is derived directly from the key
- `key_index.bin` can be omitted

General exact-key mode:

- add a compact sidecar lookup structure
- keep it off the walk hot path
- load only the minimal lookup state needed at open

The important rule is:

> key lookup may be a sidecar, but graph walking must never depend on a general lookup engine per hop.

## Runtime Open Path

The runtime should do only this:

1. read `manifest.json`
2. validate version, widths, counts, and file sizes
3. `mmap` the four adjacency arrays
4. `mmap` or lightly load `node_table.bin` and `strings.bin`
5. load the optional key index only if exact-key mode needs it

The runtime should **not**:

- rebuild adjacency
- deserialize the full graph into heap objects
- derive reverse edges from forward edges
- scan rows or edge lists to answer one-hop queries

## Runtime Query Contract

### One hop

```text
entity key
  -> dense id
  -> forward or reverse offsets slice
  -> peer ids
  -> node table enrichment
```

### Multi-hop

```text
entity key
  -> dense id
  -> BFS frontier over peer slices
  -> small visited bitmap / hash set
  -> optional enrichment at boundary or final response
```

This keeps heap use attached to the frontier, not to the entire graph.

## What To Borrow From Iggy

### 1. Separate payload from seek aid

Iggy persists message bytes separately from compact indexes.

`knight-bus` should do the same:

- peers are the payload
- offsets are the seek aid
- optional key lookup is a separate entry aid

### 2. Seal old data

Iggy's strength comes from immutable sealed segments plus a tiny mutable head.

For `knight-bus v1`, we can simplify this:

- every snapshot is already sealed
- there is no mutable head in the serving path

### 3. Reconstruct runtime from compact durable artifacts

Iggy does not depend on restoring a giant heap image.

`knight-bus` should also reopen from:

- manifest
- mmapped adjacency
- small sidecars

### 4. Validate on open

Iggy explicitly validates and rebuilds lightweight runtime state on restart.

`knight-bus` should validate:

- expected file sizes
- `offsets.len() == node_count + 1`
- `last_offset == peers.len()`
- little-endian width expectations

## What Not To Borrow From Iggy

### 1. One record per edge

That would turn neighbor access into lookup churn.

This repo should store one node's neighbors contiguously, not edge-by-edge.

### 2. Polling the storage engine for each traversal step

Iggy's query path is appropriate for logs.

`knight-bus` should stay inside local slices once the snapshot is open.

### 3. Mutable-head complexity in `v1`

Iggy's journal and in-flight tiers solve real streaming problems.

For this repo's first proof, they would blur the benchmark and slow implementation.

## If Incremental Updates Arrive Later

Only after the immutable snapshot walker is proven should we add an Iggy-like mutable overlay.

That future shape should be:

- sealed immutable base snapshot
- tiny append-only delta journal
- periodic rebuild into a new sealed snapshot

But the serve path should still prefer:

- base snapshot first
- tiny overlay second
- never a database-shaped traversal loop

## Implementation Consequences

The current thesis should be interpreted as:

- `build` is allowed to use large hash maps, sorting, and temporary edge lists
- `serve` should open files, map arrays, and walk slices
- exact-key lookup is a boundary concern
- reverse traversal is first-class persisted data

The storage is aligned to the runtime when these statements are true:

- one-hop queries never rescan the whole edge set
- backward queries never derive reverse edges on demand
- cold open does not reconstruct the whole graph into heap memory
- the runtime mostly touches `u32` and `u64` slices
- the benchmark is dominated by graph walking, not by query-engine indirection

## Final Synthesis

The right hybrid of the two reference repos is:

- **Parseltongue for graph shape**
- **Iggy for storage discipline**

So the storage contract for this repo should be:

> immutable dual-CSR graph payloads, tiny sidecar lookup aids, explicit validation on open, and no broker/database semantics in the traversal loop.

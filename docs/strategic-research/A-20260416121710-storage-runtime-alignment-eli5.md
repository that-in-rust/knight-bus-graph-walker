# Storage Runtime Alignment ELI5

## Big Idea

We want `knight-bus-graph-walker` to store the graph in the same shape that the runtime wants to walk it.

In simple words:

- Parseltongue taught us what the **graph shape** should look like
- Apache Iggy taught us what **good durable storage discipline** looks like
- this repo should combine those two lessons without turning into a graph database or a message broker

The short version is:

**store the graph like a map, not like a filing cabinet and not like a message stream.**

## Why It Matters

The main job of this repo is narrow:

- open a large graph snapshot
- answer forward and backward neighborhood questions
- do it on a laptop without loading the whole graph into heap memory

That means the runtime should feel like:

```text
find node
  -> jump to neighbor slice
  -> walk neighbors
```

and not like:

```text
find node
  -> ask a database
  -> decode rows
  -> rebuild relationships
  -> ask again for the next hop
```

or:

```text
find node
  -> poll a log
  -> decode a record
  -> poll again
```

That is why this storage decision matters so much.

If the on-disk bytes are shaped wrong, the runtime will always feel heavy.

## What We Actually Did

We created an ignored reference folder:

- `ref-repo-folder/`

and shallow-cloned two repos into it:

- `that-in-rust/parseltongue-rust-LLM-companion` on branch `v301`
- `apache/iggy`

The reason for doing that was simple:

- study how Parseltongue thinks about graph walking
- study how Iggy thinks about durable storage
- then decide what this repo should copy from each one

Also, one important reality check:

- this repo does **not** have a first git commit yet
- so there is no real "last commit" to summarize

Because of that, this note captures the current working-tree discussion and the docs we wrote in this session.

## Core Ideas Made Simple

### 1. Parseltongue says: build the map once, then walk it cheaply

The strongest idea from the Parseltongue notes is:

- do the heavy work at build time
- keep walk time boring

That means build time is allowed to do:

- counting
- sorting
- assigning dense IDs
- building forward and reverse adjacency
- writing a clean snapshot

Then the runtime only needs to do:

- look up the node ID
- read the right offset range
- return the neighbors

This is like making a road atlas before the trip instead of redrawing the city every time someone asks for directions.

### 2. Parseltongue's graph shape is dual CSR

The winning graph shape from the reference docs is:

- dense integer node IDs
- forward adjacency
- reverse adjacency
- read-only snapshot files
- `mmap` open path

In practice that means:

- one offsets array
- one peers array
- and the same thing again for reverse edges

So the hot path becomes:

```text
id
  -> offsets[id]
  -> offsets[id + 1]
  -> peers[start..end]
```

That is the shape we want here too.

### 3. Iggy says: store payload bytes separately from tiny seek helpers

Iggy is not a graph engine.

It is a durable message system.

But it teaches a very useful storage lesson:

- keep the real payload bytes in one place
- keep small indexes beside them so reads can jump near the right bytes fast
- validate and reconstruct lightweight runtime state on open

That is a strong pattern even though the workload is different.

For this repo, the graph version of that idea is:

- peers are the payload
- offsets are the seek helper
- exact-key lookup is a separate sidecar concern

### 4. What we should copy from Iggy

We should copy these ideas:

- payload shaped for the read path
- tiny sidecar indexes
- immutable sealed artifacts
- explicit validation when opening
- runtime reconstruction from compact durable files

These are the good habits.

### 5. What we should not copy from Iggy

We should **not** copy these ideas into the graph read path:

- one-record-per-edge storage
- polling storage one hop at a time
- broker semantics
- mutable-head complexity in `v1`
- RPC-shaped traversal

That would make graph walking feel like warehouse work instead of map reading.

### 6. The storage must match the runtime, not just look tidy on disk

This is the most important sentence in the whole discussion:

storage is only aligned to runtime when the runtime can almost directly "see" its hot path inside the stored bytes.

For this repo, that means:

- no reverse-edge reconstruction at query time
- no whole-graph deserialization on open
- no edge scanning for one-hop queries
- no query-engine mediation inside each hop

## The Concrete Shape We Chose

The snapshot directory should look like this:

```text
snapshot/
  manifest.json
  node_table.bin
  strings.bin
  forward.offsets.bin
  forward.peers.bin
  reverse.offsets.bin
  reverse.peers.bin
  key_index.bin   # optional
```

The important types are:

- node IDs: `u32`
- peer IDs: `u32`
- adjacency offsets: `u64`
- string offsets: `u64`

Why this shape?

- `u32` keeps node references compact
- `u64` keeps big files safe once offsets get large
- forward and reverse slices make both directions first-class
- node metadata is separate from adjacency
- exact-key lookup stays outside the walk hot path

## Tiny Example

Imagine node `7` has these outgoing neighbors:

```text
7 -> [10, 13, 21]
```

In the stored files, the runtime should be able to do something like:

```text
start = forward_offsets[7]
end   = forward_offsets[8]
neighbors = forward_peers[start..end]
```

and get:

```text
[10, 13, 21]
```

Then if someone asks for reverse neighbors, the same idea works on the reverse files.

That is the whole point:

- no database join
- no edge scan
- no "derive reverse edges now"
- just jump to the drawer and read the contents

## What This Means For Implementation

The implementation rule is now very clear:

- `build` can be smart and heavy
- `serve` should be small and predictable

So build time can use:

- hash maps
- temporary vectors
- sorting
- deduplication
- dense ID assignment

But serve time should mostly use:

- `mmap`
- read-only slices
- a small lookup sidecar
- BFS frontiers and visited sets

That keeps memory attached to the active walk, not to the whole graph.

## Where The Deeper Note Lives

This plain-English note is the easy version.

The stricter implementation-facing version is:

- `docs/STORAGE_RUNTIME_ALIGNMENT.md`

That document has the more exact file contract and the "copy this / do not copy this" details.

## What To Remember

- Parseltongue gives us the right **graph walk shape**
- Iggy gives us the right **durability habits**
- this repo should use immutable dual adjacency snapshots, not database-shaped traversal and not broker-shaped traversal
- the runtime should walk slices, not reconstruct relationships
- there is no real last commit yet, so this note captures the current discussion and current docs state

## One-Line Takeaway

The winning design is: **build the graph like a careful archivist, but read it like a traveler opening a map.**

# Knight Bus Storage Format Story Summary

This note summarizes the **Knight Bus-style storage format ideas** I could find from locally accessible chat traces and repo notes on this computer.

It is not a promise that every app on the machine was readable. Some system locations were not accessible, and different AI tools store history differently. This summary is grounded in the sources listed below, which were the strongest accessible evidence.

## Governing Thought

The repeated Knight Bus thesis is:

> store the graph in the shape the runtime wants to walk, so the hot path becomes direct indexed reads instead of dynamic graph reconstruction.

Everything else is downstream from that.

## What I Checked

### Accessible local chat-history style sources

- `~/.codex/.codex-global-state.json`
- `~/.codex/session_index.jsonl`
- `~/.codex/archived_sessions/...` search hits for Knight Bus prompts
- `~/.claude/history.jsonl`
- `~/.claude/projects/...` and `~/.claude/paste-cache/...` for Knight Bus-adjacent storage ideas
- `~/Downloads/that-in-rust-knight-bus-graph-walker-8a5edab282632443.txt`
- `~/Downloads/that-in-rust-knight-bus-graph-walker-8a5edab282632443 (1).txt`

### Repo-local notes that already captured the same thesis

- [README](../README.md)
- [STORAGE_RUNTIME_ALIGNMENT.md](../STORAGE_RUNTIME_ALIGNMENT.md)
- [KNIGHT_BUS_THESIS.md](../KNIGHT_BUS_THESIS.md)
- [Knight Bus Algorithm Storage Atlas](../KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md)
- [A-20260416121710-storage-runtime-alignment-eli5.md](./A-20260416121710-storage-runtime-alignment-eli5.md)
- [A-20260416144105-open-path-and-minimum-proof-eli5.md](./A-20260416144105-open-path-and-minimum-proof-eli5.md)
- [A-20260416151416-rust-vs-neo4j-proof-eli5.md](./A-20260416151416-rust-vs-neo4j-proof-eli5.md)
- [A-20260525164835-faithful-rust-port-dossier.md](./A-20260525164835-faithful-rust-port-dossier.md)

## The Story In One Page

Across the accessible chats and notes, the same story keeps repeating:

1. `CSV` or some other readable source is the **truth layer**.
2. A compiled immutable artifact is the **runtime layer**.
3. The runtime should not rescan or reconstruct the graph at query time.
4. The runtime should jump from anchor to adjacency slice through simple arithmetic.
5. Exact-key lookup should stay separate from traversal.
6. The bytes on disk should already reveal the hot path.

That is the core Knight Bus storage format idea.

## The Stable Design Ideas

### 1. Truth layer and runtime layer must stay separate

This distinction is one of the most repeated ideas across the notes.

- `truth layer`: readable source inputs such as `nodes.csv` and `edges.csv`
- `runtime layer`: compact compiled snapshot optimized for walking

The point is not merely performance. The point is also intellectual honesty:

- truth remains inspectable
- parity can be checked against truth
- the runtime is free to throw away semantic baggage that the hot path does not need

Short form:

> CSV is truth, not hot path.

### 2. The winning base shape is dual CSR plus exact lookup

The dominant repeated base format is:

- dense integer node IDs
- forward adjacency
- reverse adjacency
- sorted exact-key lookup
- memory-mapped file access

That usually means:

- `forward.offsets.bin`
- `forward.peers.bin`
- `reverse.offsets.bin`
- `reverse.peers.bin`
- `key_index.bin`
- `manifest.json`

Sometimes `node_table.bin` and `strings.bin` are also present for result enrichment.

The key lookup and walk path are intentionally different concerns:

- key lookup finds the dense ID
- offsets and peers answer the walk

### 3. The runtime must be build-heavy and walk-light

The storage format idea assumes:

- expensive organization work happens once at build time
- runtime work becomes boring and predictable

Build-time work is allowed to do:

- counting
- sorting
- dense-ID assignment
- forward/reverse adjacency materialization
- validation and manifest writing

Runtime work should mostly do:

- read manifest
- validate shape
- `mmap` fixed files
- resolve key
- read one slice

Short form:

> build-time heavy, walk-time boring

### 4. Storage is aligned only when the hot path is visible in the bytes

This is the sharpest sentence in the whole note family:

> storage is only aligned to runtime when the runtime can almost directly "see" its hot path inside the stored bytes.

In practice, that means:

- no reverse-edge reconstruction at query time
- no edge-table rescans for one-hop queries
- no row-materialization machinery in the walk path
- no query planner in the narrow runtime
- no generalized graph-database semantics on the hot path

### 5. `mmap` is part of the format story, not just an implementation detail

The notes repeatedly connect the storage format to a native `mmap` open path.

Why:

- lets the OS page in only touched regions
- keeps startup simple
- avoids heap reconstruction of the whole graph
- pairs naturally with contiguous `offsets + peers` slices

This is why the native runtime story was preferred over a Wasm-first story in the repo notes.

### 6. Exact-key search should stay tiny and off the traversal hot path

Another repeated rule:

- search is okay
- search in every hop is not okay

So the design tries to keep:

- one compact exact-key entry path
- one separate adjacency walk path

This is why `key_index.bin` is treated as a sidecar, not as the center of the engine.

## Concrete Storage Contract That Repeats Most Often

The most common concrete shape in the notes is:

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

Common type choices:

- node IDs: `u32`
- peer IDs: `u32`
- offsets: `u64`
- string offsets: `u64`

Common runtime rule:

```text
key -> dense_id
dense_id -> offsets[id], offsets[id + 1]
slice -> peers[start..end]
```

This is the smallest recurring “spec story” for Knight Bus storage.

## The Hot Path Mental Model

The recurring mental model is not “query a graph database.”

It is:

```text
find key
  -> get dense id
  -> read start offset
  -> read end offset
  -> slice contiguous peers
```

That is why the notes keep contrasting Knight Bus with Neo4j like this:

- Neo4j: generalized property graph, planner, traversal machinery, row materialization, process boundaries
- Knight Bus: fixed graph world, fixed relationship semantics, direct slice replay

The fairest repeated claim is not “Knight Bus beats Neo4j everywhere.”

It is:

> Knight Bus is dramatically better when the workload is exact-anchor, fixed-hop replay over a static snapshot.

## What The Chats Added Beyond The Repo Docs

The local chat traces add four important extensions to the repo’s written notes.

### 1. The same thesis was generalized beyond code graphs

The same format idea was applied to:

- single-node Rust compute/storage systems
- Tweet Scrolls archive memory retrieval
- Neo4j algorithm-specific backend formats

So “Knight Bus-style” became a broader design pattern:

> make storage vibe with the exact operation being run.

### 2. Tweet Scrolls reused the same split

In the Tweet Scrolls architecture prompts, the Knight Bus variant was:

- archive records as truth layer
- compiled memory snapshot as runtime layer
- nodes such as `tweet`, `DM`, `participant`, `thread`, `topic`, `time bucket`
- fixed-hop traversal for memory questions

That means the storage idea escaped the original code-dependency benchmark and became a generic “memory graph runtime” pattern.

### 3. The storage thesis expanded into algorithm-specific internal layouts

The biggest expansion appears in the atlas:

- one universal base format was rejected
- one fully bespoke engine per algorithm was also rejected
- the chosen pattern was a small family of reusable layout types plus per-algorithm contracts

The crucial nuance from the later conversations is:

> "bespoke" is the right publication model for the contract, but not the right implementation model for the engine.

In practice that means:

- the product can publish very specific contracts such as `PageRankInboundPowerSnapshotV1`
- the runtime should still reuse a much smaller set of internal layout families
- a `FormatSelectionProfile` should choose the family, required planes, and result sidecars for the requested algorithm
- the base graph should stay immutable and sealed
- exact-key lookup should remain separate from traversal or compute

The later internal vocabulary became much more explicit:

- `BaseGraphSnapshot`: sealed topology artifact
- `PropertyPlane`: typed numeric or categorical planes
- `AlgorithmArtifact`: the algorithm-specific open-time view
- `ComputeScratch`: temporary queues, heaps, buckets, tensors, bitsets, and arrays
- `ResultSidecar`: persisted scores, paths, flows, clusters, or embeddings
- `FormatSelectionProfile`: mapping from algorithm to layout family plus sidecars

The core reusable family examples were:

- `AnchorDualCsrLayoutV1`
- `InboundPowerLayoutV1`
- `ConnectivityLowlinkLayoutV1`
- `RelaxationFrontierLayoutV1`
- `OrderedWedgeLayoutV1`
- `PartitionRefinementLayoutV1`
- `PeelBucketLayoutV1`
- `EdgeOrderForestLayoutV1`
- `FlowResidualLayoutV1`
- `FeatureMetricLayoutV1`
- `EmbeddingSampleLayoutV1`
- `DagOrderLayoutV1`
- `InfluenceMonteCarloLayoutV1`

This is still a Knight Bus idea, but generalized:

- keep the public product stable
- let the backend choose the byte shape that matches the dominant inner loop

#### What the family taxonomy was really trying to say

The later conversation stopped treating "graph algorithms" as one storage class and instead grouped them by the primitive their inner loop actually needs:

| layout family | dominant primitive |
| --- | --- |
| `AnchorDualCsrLayoutV1` | exact anchor to one or two adjacency slices |
| `InboundPowerLayoutV1` | repeated inbound score accumulation |
| `ConnectivityLowlinkLayoutV1` | DFS numbering, lowlinks, or reverse-pass replay |
| `OrderedWedgeLayoutV1` | sorted-neighbor intersection and wedge counting |
| `PartitionRefinementLayoutV1` | community assignment updates and evaluation |
| `PeelBucketLayoutV1` | low-degree peeling and bucket discipline |
| `RelaxationFrontierLayoutV1` | weighted frontier relaxation |
| `EdgeOrderForestLayoutV1` | globally ordered edge scan plus union-find |
| `FlowResidualLayoutV1` | mutable residual arc updates |
| `FeatureMetricLayoutV1` | row-major feature distance and candidate refinement |
| `EmbeddingSampleLayoutV1` | neighborhood or walk sampling for embeddings |
| `DagOrderLayoutV1` | topological replay over acyclic graphs |
| `InfluenceMonteCarloLayoutV1` | repeated stochastic cascade simulation |

That is the deeper bespoke-storage claim:

- storage should match the dominant inner loop
- only the properties needed by that loop should be loaded as planes
- outputs should usually be written as sidecars rather than mutating the base snapshot

So the later Knight Bus story is no longer just "dual CSR is good."

It becomes:

> dual CSR is the first strong proof, but the more durable doctrine is to choose a byte shape family that makes the intended inner loop boring.

#### The first-wave proof order was explicit

The atlas also made the prototype order much clearer.

P0 families were:

1. `AnchorDualCsrLayoutV1`
2. `InboundPowerLayoutV1`
3. `ConnectivityLowlinkLayoutV1`
4. `RelaxationFrontierLayoutV1`
5. `OrderedWedgeLayoutV1`

And if only five concrete proof snapshots were built first, the recommended set was:

1. `DegreeCentralityAnchorDualCsrSnapshotV1`
2. `BfsTraversalFrontierSnapshotV1`
3. `PageRankInboundPowerSnapshotV1`
4. `DijkstraSingleSourceHeapRelaxationSnapshotV1`
5. `TriangleCountOrderedWedgeSnapshotV1`

That sequence matters because it shows the bespoke-format conversation was not random brainstorming. It had a systems-proof shape:

- first prove slice replay
- then prove inbound power iteration
- then shortest-path relaxation
- then wedge intersection
- only then move deeper into community, flow, feature, or embedding families

### 4. The later Neo4j-compatible thinking imposed a boundary

The later faithful-port dossier adds a useful guardrail:

- the **usage interface** should stay Neo4j-like
- the **backend** can diverge aggressively

That means the layout-family vocabulary is good **engine language** but not necessarily good **product language**.

## The Guardrails That Repeat

These are the repeated “do not mess this up” rules across the notes.

### Do not turn it into a generic graph database too early

Repeated non-goals:

- not a query-language project first
- not a graph database first
- not a broker-backed graph reader
- not a per-hop RPC system
- not a Wasm-first showcase

### Do not let lookup and traversal collapse into one heavy path

The recurring warning is that once search, row decoding, planner work, reverse-edge derivation, and traversal are mixed together, the narrow runtime loses its edge.

### Do not overclaim benchmark generality

The repo notes are pretty disciplined here:

- current proofs are about fixed-hop traversal
- comparisons were against Cypher over Bolt, not every Neo4j subsystem
- GDS and broader algorithm families need separate, fair comparisons

### Do not optimize for beautiful storage if it mismatches the operation

The real criterion is not elegance.

It is whether the bytes match the operation tightly enough that the CPU mostly performs direct arithmetic and contiguous reads.

## What Changed Over Time

The local notes seem to evolve through four stages.

### Stage 1. Prove a narrow win

Focus:

- static graph
- exact-key anchor lookup
- one-hop and two-hop traversal
- dual CSR snapshot

### Stage 2. Formalize the storage-runtime doctrine

Focus:

- truth layer vs runtime layer
- Parseltongue for graph shape
- Iggy for durability discipline
- immutable sealed artifact

### Stage 3. Generalize to other workloads

Focus:

- single-node Rust compute thesis
- archive-memory graph retrieval
- other fixed-structure workloads

### Stage 4. Generalize to backend families

Focus:

- algorithm-specific layout families
- bespoke contracts over reusable runtime families
- `FormatSelectionProfile` plus result sidecars
- backend-only specialization
- Neo4j-compatible frontend possibility

## The Best One-Sentence Summary

If I compress all the accessible Knight Bus storage notes on this computer into one sentence, it is this:

> Knight Bus is the idea that you should compile the graph into a memory-mapped, immutable, dense-ID snapshot whose bytes already expose the walk hot path, instead of asking a general engine to rediscover that path on every query.

## The Most Important Open Questions

These open questions show up implicitly or explicitly across the notes:

- How far can the narrow snapshot idea stretch before a general property-graph backend is unavoidable?
- Which workloads deserve dedicated layout families and which are mostly compute-bound after load?
- How should mutable overlays work without contaminating the base immutable runtime?
- If the user-facing interface must stay Neo4j-like, where exactly does backend divergence begin?
- How much of the lookup path should stay sidecar-only before it needs richer indexing?

## Source Notes Worth Re-Reading First

If someone wants the shortest reading list after this summary, read these in order:

1. [STORAGE_RUNTIME_ALIGNMENT.md](../STORAGE_RUNTIME_ALIGNMENT.md)
2. [KNIGHT_BUS_THESIS.md](../KNIGHT_BUS_THESIS.md)
3. [Knight Bus Algorithm Storage Atlas](../KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md)
4. [A-20260525164835-faithful-rust-port-dossier.md](./A-20260525164835-faithful-rust-port-dossier.md)

## Bottom Line

The accessible chats and notes do not describe Knight Bus as “a faster graph database.”

They describe it as a stricter systems discipline:

- separate truth from runtime
- compile structure once
- make the hot path visible in the bytes
- keep search and traversal separate
- use immutable snapshots by default
- publish bespoke contracts while reusing a small family of byte-level layouts
- specialize backend storage to the operation, not the other way around

That is the core Knight Bus storage format story on this computer.

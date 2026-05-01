# Knight Bus Graph Walker Thesis

## Premise Check

- The research folder strongly supports a snapshot-first walk runtime built around dense IDs, dual adjacency, and `mmap`.
- The research folder does not strongly support a Wasm-first benchmark story. That is an inference boundary worth keeping honest.
- The most defensible claim is not "RAM does not matter." The defensible claim is "the runtime only needs the active working set in RAM, while the graph artifact can live on disk."
- The cleanest hackathon proof is not "we built a better graph database." It is "we built a sharper graph walker for one workload family: forward and backward neighborhood traversal."
- The notes repeatedly separate `find the place` from `walk the place`. For this repo, search should stay minimal and exact.

## Expert Lenses

- Systems lens: optimize the on-disk shape for the walk path, not for general querying.
- Benchmark skeptic lens: only claim what the benchmark actually isolates and measures.
- Hackathon operator lens: choose the narrowest implementation that can finish and still survive scrutiny.
- Product-story lens: keep the demo anchored on consequence questions such as "who calls this?" and "who is affected?"
- Skeptical lens: do not hide behind the word "Wasm" if the performance proof is really coming from snapshot shape plus `mmap`.

## Candidate Approaches

### 1. Native Rust snapshot walker with deterministic synthetic keys

Build a Rust CLI and HTTP server that:

- generates a large synthetic graph
- writes dual CSR-like artifacts
- memory-maps them
- answers exact-key forward and backward queries

Pros:

- simplest proof
- clean benchmark
- least moving parts
- easiest to explain

Cons:

- less flashy than a Wasm headline
- exact-key lookup is intentionally simple

### 2. Split architecture: sidecar key index plus walk snapshot

Build:

- tiny exact lookup sidecar
- walk snapshot for traversal
- HTTP API that resolves key, then walks

Pros:

- more realistic API
- still fast to build
- aligns with the benchmark isolation pattern in the research

Cons:

- adds one more component
- easy for the story to become blurry if you do not separate lookup latency from walk latency

### 3. Wasm-first runtime

Build the runtime around Wasm packaging first, then try to benchmark the full system.

Pros:

- sounds modern
- may help future embedding

Cons:

- likely the wrong demo center
- risks fighting platform and file-mapping details during the hackathon
- makes it harder to prove that storage shape, not packaging, is the real win

## Chosen Thesis

The most practical way to build `knight-bus-graph-walker` is:

1. Build a native Rust walk runtime first.
2. Keep the repo focused on one frozen graph world and one workload family.
3. Generate a large synthetic graph with deterministic string keys.
4. Persist the graph as dual walk artifacts:
   - `forward_offsets.bin`
   - `forward_peers.bin`
   - `reverse_offsets.bin`
   - `reverse_peers.bin`
   - `manifest.json`
5. Expose a tiny HTTP API that supports:
   - exact key to dense ID
   - forward one-hop
   - backward one-hop
   - forward and backward multi-hop
6. Benchmark three things separately:
   - dataset build time
   - exact-key handoff
   - post-anchor graph walking

If time remains, compile the core crate to Wasm later as a packaging experiment, not as the primary proof.

## Why This Wins

- It matches the strongest repeated claim in the strategic research: immutable snapshots plus dense-ID adjacency are the right shape for graph walking.
- It keeps the benchmark honest by isolating graph walk performance from broader product concerns.
- It gives a clean hackathon story: "we can traverse a graph larger than memory because the graph stays on disk and only the working set is paged in."
- It avoids turning an empty repo into a half-built database.

## Minimal Repo Shape

Do not start with a workspace of many crates. Start with one binary crate and plain modules.

Suggested structure:

```text
src/
  main.rs
  cmd_generate.rs
  cmd_build.rs
  cmd_serve.rs
  cmd_bench.rs
  synth.rs
  snapshot.rs
  runtime.rs
  api.rs
  bench.rs
```

Suggested commands:

```text
knight-bus generate --target-size-gb 20 --model scale_free
knight-bus build --input ./data/raw --output ./data/snapshot
knight-bus serve --snapshot ./data/snapshot --port 8787
knight-bus bench --snapshot ./data/snapshot --report ./reports
```

## Build Plan

### Phase 1. Synthetic graph generator

Generate a graph that is large, directional, and graph-shaped enough to feel real.

Use deterministic keys such as:

```text
entity_000000001
entity_000000002
entity_000000003
```

This lets the API look human-readable while still allowing trivial exact lookup from key to dense ID.

Implement three generation models:

- `scale_free`
  - power-law out-degree
  - few large hubs
  - best for "call graph feel"
- `layered`
  - modules or services call into lower layers
  - best for understandable blast-radius demos
- `clustered`
  - strong communities with a few bridge nodes
  - best for showing local neighborhoods and boundary crossings

Recommended hackathon default:

- `clustered + scale_free` hybrid
- deterministic seed
- adjustable target bytes

### Phase 2. Snapshot compiler

At build time, allow heavy work:

- degree counting
- bucket files
- sorting within buckets
- offset construction
- manifest writing

The runtime artifact should be boring:

- little-endian integers
- contiguous peer arrays
- offset arrays with `node_count + 1` entries
- one manifest file with counts and paths

Recommended artifact contract:

```json
{
  "version": 1,
  "node_count": 0,
  "edge_count": 0,
  "key_mode": "deterministic_entity_prefix",
  "forward_offsets": "forward.offsets.bin",
  "forward_peers": "forward.peers.bin",
  "reverse_offsets": "reverse.offsets.bin",
  "reverse_peers": "reverse.peers.bin"
}
```

See [STORAGE_RUNTIME_ALIGNMENT.md](./STORAGE_RUNTIME_ALIGNMENT.md) for the stricter runtime-aligned version of this contract, including why adjacency offsets should be `u64`, why lookup should stay a sidecar concern, and which Apache Iggy storage patterns are worth borrowing.

### Phase 3. Runtime

Use:

- `memmap2`
- `u32` node IDs if possible
- read-only slices
- no heap reconstruction of the whole graph

The hot path should be:

```text
entity key
  -> dense id
  -> offsets[id], offsets[id + 1]
  -> peers[start..end]
  -> optional next hop
```

The backward path should be symmetrical, not derived by rescanning forward edges.

### Phase 4. HTTP API

Keep the API embarrassingly small.

Recommended endpoints:

- `GET /forward/:entity`
- `GET /backward/:entity`
- `GET /hops/:entity?hops=2&dir=forward`
- `GET /stats`

Suggested response:

```json
{
  "entity": "entity_000123456",
  "dense_id": 123456,
  "direction": "forward",
  "hops": 1,
  "neighbors": [
    "entity_000222001",
    "entity_000222009"
  ]
}
```

Do not add fuzzy search, ranking, UI, or graph rendering in v1.

## Benchmark Thesis

The benchmark must prove three separate things.

### 1. Feasibility

Questions:

- Can a snapshot larger than comfortable heap size still answer local walk queries?
- Does RSS stay well below full dataset size?

Metrics:

- max RSS
- page faults
- cold open time
- snapshot size on disk

### 2. Traversal advantage

Questions:

- Is `offsets + peers` materially better than scanning rows?
- Is reverse walking cheap because reverse adjacency is precomputed?

Baselines:

- raw edge scan from CSV or line-delimited binary
- indexed SQLite edge table
- Knight Bus walk snapshot

Metrics:

- `forward_one` by ID
- `backward_one` by ID
- `backward_two` by ID
- mean, p50, p95

### 3. End-to-end usefulness

Questions:

- Can a simple API answer "who calls this?" and "who is affected?" quickly enough to feel live?

Metrics:

- `by_key` exact lookup plus walk
- repeated hot queries
- concurrent small-query runs

## Benchmark Plan

Run three benchmark lanes, not one blurry benchmark.

### Lane A. Post-anchor walk benchmark

This is the core proof.

Inputs:

- dense IDs only

Operations:

- forward one-hop
- backward one-hop
- backward two-hop

Why it matters:

- this isolates the snapshot structure itself

### Lane B. Exact key handoff benchmark

Inputs:

- `entity_#########`

Operations:

- exact parse or exact sidecar lookup
- then same walk

Why it matters:

- proves the API is usable, not only the internal engine

### Lane C. End-to-end HTTP benchmark

Inputs:

- HTTP calls against the running service

Operations:

- repeated `/forward/:entity`
- repeated `/backward/:entity`
- mixed query set

Why it matters:

- proves the demo story, not just the library

## Practical Dataset Strategy

Do not bet the hackathon on a single 20 GB run.

Use size tiers:

- Tier 1: `1 GB`
  - sanity, correctness, local debugging
- Tier 2: `5 GB`
  - live demo fallback
- Tier 3: `20 GB`
  - main proof target

If time allows, add:

- Tier 4: `30 GB`
  - stretch goal

This gives you a complete story even if the largest overnight build or benchmark is imperfect.

## Different Simulations To Run

### Simulation 1. Scale-free call graph

Purpose:

- mimic real software graphs with hubs and long tails

Claim it supports:

- hot hubs can still be walked without loading the world

### Simulation 2. Layered service graph

Purpose:

- mimic upstream and downstream impact

Claim it supports:

- one-hop and two-hop consequence queries feel intuitive

### Simulation 3. Clustered graph with bridge nodes

Purpose:

- mimic code neighborhoods and module boundaries

Claim it supports:

- local exploration is cheap even when the full artifact is huge

Recommended demo order:

1. clustered graph for explainability
2. scale-free graph for performance drama
3. layered graph for business-story clarity

## Evidence and Verification

### Sourced from the research folder

- The strongest recurring thesis is "persisted walk runtime," not "general graph database."
- The repeated recommended storage shape is immutable forward and backward adjacency with dense IDs and `mmap`.
- The benchmark notes explicitly treat exact lookup and graph walking as separable lanes.
- The current harness materializes the same idea as `offsets + peers` with `mmap`.
- The notes explicitly say the walk-layer benchmark validated the adjacency shape but did not yet prove the final all-in-one architecture.

### Additional external verification

- The WebAssembly FAQ treats `mmap` as functionality split across memory and future mapping features rather than the default simple story, which reinforces that the hackathon benchmark should stay native-first: [WebAssembly FAQ](https://webassembly.org/docs/faq/index.html#what-about-mmap).

### Honest inference

- A Wasm packaging layer may still be useful later for reuse or embedding.
- But the practical performance proof in this repo should come from snapshot shape, disk layout, and page-friendly access, not from leading with Wasm.

## Final Synthesis

Build `knight-bus-graph-walker` as a native Rust walk-runtime proof, not as a graph database and not as a Wasm-first showcase.

The winning hackathon move is:

- generate a deterministic synthetic graph
- compile dual adjacency snapshots
- memory-map them
- expose tiny forward and backward endpoints
- benchmark post-anchor traversal, exact-key handoff, and end-to-end HTTP separately

That is the smallest system that proves the point and still expands cleanly later.

## Open Questions

- Do you want the first demo query key to be deterministic synthetic IDs only, or do you want a small exact-key sidecar from day one?
- Is the headline target `20 GB` because of honesty and comfort, or do you need the slide to say `30 GB` specifically?
- Do you want the first public benchmark to include SQLite as a fair baseline, or only raw scan plus Knight Bus for a simpler story?

## Decision Frame

- Fork in the road: prove the walk-runtime thesis fast, or spend time on a broader architecture.
- Desired outcome: a demo that shows large persisted graph traversal on a 16 GB laptop with simple HTTP queries.
- Hard constraints: hackathon time, one laptop, no appetite for a broad product build.
- Time horizon: this week for the demo, later for generalization.
- What counts as failure: shipping a clever architecture that never cleanly proves the traversal claim.

## Timeline A: Native Snapshot First

- Opening move: build the generator, snapshot compiler, and native Rust runtime first.
- Week 1: ship one binary, exact deterministic keys, and three benchmark lanes.
- Month 1: add better key lookup, better report generation, and optional visualization.
- Quarter 1: add rank and shape sidecars only after walk runtime is stable.
- Long-term shape: the cleanest path to a serious embedded walk engine.
- Likelihood: high.
- Stress points: large synthetic dataset generation and reverse-bucket build logic.
- Inflection points: once Tier 5 GB is solid, Tier 20 GB becomes a persistence and patience problem rather than a design problem.

## Timeline B: Split Lookup Plus Walk Snapshot

- Opening move: keep the walk snapshot native, but add a tiny exact key sidecar now.
- Week 1: nicer API, slightly more realistic end-to-end story, slightly more integration work.
- Month 1: benchmark split lookup versus snapshot-native lookup.
- Quarter 1: sidecar may stay, or be folded into the snapshot if needed.
- Long-term shape: strong if you want a more product-like API quickly.
- Likelihood: medium-high.
- Stress points: benchmark interpretation gets messier.
- Inflection points: if sidecar lookup dominates latency, you either optimize it or collapse it into the snapshot.

## Timeline C: Wasm-First Demo

- Opening move: spend early time on Wasm packaging and runtime compatibility.
- Week 1: less time spent on the actual proof, more time spent on glue and environment questions.
- Month 1: maybe impressive packaging, but still less certain performance evidence.
- Quarter 1: could become useful for embedding, but only after the native engine is already trustworthy.
- Long-term shape: optional extension path, not the strongest first move.
- Likelihood: medium-low for a clean hackathon win.
- Stress points: platform complexity, file mapping story, and benchmark ambiguity.
- Inflection points: unless you already have the native runtime done, this path increases regret risk quickly.

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who or what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| Native Snapshot First | cleanest proof, simplest story, strongest benchmark | less flashy than a Wasm headline | high | low | only the local Rust toolchain and disk |
| Split Lookup Plus Walk Snapshot | more realistic API from day one | adds interpretation and integration cost | high | medium | local DB sidecar plus runtime |
| Wasm-First Demo | modern packaging story | weaker proof and higher build risk | medium | high | Wasm environment plus runtime |

## Decision Filter

- Strongest if everything goes normally: Native Snapshot First.
- Safest if things go badly: Native Snapshot First with size tiers and deterministic keys.
- Fastest experiment to reduce uncertainty: build Tier 1 and Tier 2 snapshots first, then measure RSS and `forward_one` and `backward_two` before touching Wasm at all.

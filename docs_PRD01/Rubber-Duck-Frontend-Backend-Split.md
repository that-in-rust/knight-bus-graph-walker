# Rubber Duck Debug: Frontend/Backend Split

*Applying TDD Playbook as a verification lens on my own analysis.*
*What do we actually have contracts and tests for? Where did I hand-wave?*

---

## TDD State Checkpoint

**Phase:** This is a VERIFICATION pass, not a build phase.
**Method:** Take every claim from `Neo4j-Frontend-Backend-Split.md`,
ask "is there a test for this?", "is there a contract (trait) for this?",
and "what evidence supports this number?"

---

## What Knight Bus Actually Has: Contracts (Traits)

I read every `pub trait` in the codebase. There are exactly **4 traits**:

### Trait 1: `WalkQueryRuntime` (runtime.rs:22)

```rust
pub trait WalkQueryRuntime {
    fn query_entity_neighbors(
        &self,
        entity_key: &NodeKey,
        direction: WalkDirection,
        hops: HopCount,
    ) -> Result<QueryResult, KnightBusError>;

    fn query_keys_for_family(
        &self,
        entity_key: &NodeKey,
        family: QueryFamily,
    ) -> Result<Vec<String>, KnightBusError>;

    fn all_node_keys(&self) -> Result<Vec<NodeKey>, KnightBusError>;

    fn snapshot_size_bytes(&self) -> u64;
}
```

**What this proves:** There IS a runtime abstraction. It's trait-based
(good DI). But it only covers:
- Entity neighbor queries (1-2 hop, forward/backward)
- Family-based key queries
- All node keys enumeration
- Snapshot size reporting

**What this does NOT cover:**
- Any write operation
- Any property access
- Any filtering or aggregation
- Any variable-length path
- Any multi-relationship type
- Any query language parsing
- Any network protocol

**Rubber duck verdict:** My claim that "Knight Bus provides the
runtime for KNRT" is **overstated**. The `WalkQueryRuntime` trait
is too narrow. KNRT would need a much broader `StorageEngine` trait
(something like Neo4j's `kernel-api/StorageEngine`). The existing
trait would survive as a sub-trait or specialized fast-path, but
it is NOT the foundation for a general query runtime.

### Trait 2: `SnapshotArtifactWriter` (snapshot.rs:23)

```rust
pub trait SnapshotArtifactWriter {
    fn write_snapshot_artifacts(
        &self,
        graph_data: &NormalizedGraphData,
        output_dir: &Path,
    ) -> Result<SnapshotBuildSummary, KnightBusError>;
}
```

**What this proves:** Snapshot writing IS abstracted behind a trait.
`FilesystemSnapshotWriter` is the only impl.

**What this does NOT cover:**
- Reading snapshots (no reader trait — `MmapWalkRuntime::open` is
  concrete, not trait-based)
- Incremental updates
- Multiple snapshot formats (the Atlas layout families)
- Property planes

**Rubber duck verdict:** My claim that "the Atlas layout families
extend the existing format" is **aspirational, not contractual**.
There is no `SnapshotFormat` trait, no `LayoutFamily` enum, no
`FormatSelectionProfile`. These would all need to be designed and
built from scratch. The existing writer only knows one format:
`immutable_dual_csr`.

### Trait 3: `TruthGraphSource` (truth.rs:26)

```rust
pub trait TruthGraphSource {
    fn load_truth_graph_rows(&self) -> Result<ValidatedTruthGraph, KnightBusError>;
}
```

**What this proves:** Data loading IS trait-abstracted. Currently
only `CsvTruthGraphSource` implements it. This is the right pattern —
a Neo4j import source would implement this trait to load from
Neo4j dump/export format.

**Rubber duck verdict:** This IS a genuine extension point. My claim
that "Knight Bus's build pipeline can be extended for Neo4j import"
is partially supported — you'd implement `TruthGraphSource` for
Neo4j's format. But `ValidatedTruthGraph` only carries:
- `Vec<CsvNodeRow>` — with hardcoded columns (node_id, node_type,
  label, parent_id, file_path, span)
- `Vec<CsvEdgeRow>` — with hardcoded columns (from_id, edge_type, to_id)

A Neo4j graph has arbitrary properties. `ValidatedTruthGraph` would
need to be generalized to carry `HashMap<String, Value>` properties
per node and per edge. That's not a small change.

### Trait 4: `BenchmarkScenarioRunner` (bench.rs:23)

```rust
pub trait BenchmarkScenarioRunner {
    fn run_benchmark_scenarios(
        &self,
        runtime: &MmapWalkRuntime,
    ) -> Result<BenchmarkReport, KnightBusError>;
}
```

**What this proves:** Benchmarking is trait-abstracted. Good.

**Rubber duck verdict:** Useful for KNRT benchmarking but not
architecturally significant.

---

## What Knight Bus Actually Has: Tests

**23 tests total. All pass.** (`cargo +nightly test`: 9 unit + 8
library contract + 6 CLI)

### Unit Tests (9)

| Test | File | What it proves |
|---|---|---|
| `normalize_truth_graph_data_deduplicates_edges` | graph.rs | Edge dedup works |
| `query_normalized_graph_uses_within_two_hops` | graph.rs | 2-hop expansion is correct |
| `csv_truth_graph_source_rejects_duplicate_nodes` | truth.rs | Duplicate detection works |
| `truth_graph_index_within_two_hops_excludes_seed` | truth.rs | Seed exclusion is correct |
| `current_process_rss_bytes_uses_raw_sysinfo_units_now` | bench.rs | RSS measurement is calibrated |
| `peak_rss_source_serializes_now` | bench.rs | Enum serialization works |
| `corpus_family_maps_to_runtime_semantics_now` | bench.rs | Family→direction+hops mapping |
| `percentile_value_ms_interpolates_now` | bench.rs | Percentile math is correct |
| `corpus_benchmark_runs_against_tiny_fixture_now` | bench.rs | Full benchmark pipeline works |

### Library Contract Tests (8)

| Test | What it proves |
|---|---|
| `build_query_and_verify_round_trip_now` | CSV → snapshot → query → verify full pipeline |
| `build_rejects_duplicate_node_ids_now` | Error handling for bad input |
| `build_rejects_missing_edge_endpoints_now` | Error handling for broken edges |
| `open_detects_truncated_offsets_now` | Snapshot corruption detection |
| `parity_uses_all_expected_families_now` | All query families have seeds |
| `benchmark_report_records_peak_rss_source_now` | Benchmark metadata correctness |
| `low_ram_build_and_verify_record_phase_peaks_now` | Low-RAM build + phase tracking |
| `corpus_benchmark_report_serializes_engine_measurement_now` | Corpus benchmark output format |

### CLI Tests (6)

| Test | What it proves |
|---|---|
| `build_and_query_json_from_cli_now` | CLI build + query works |
| `verify_cli_reports_success_now` | CLI verify works |
| `query_rejects_invalid_hops_now` | CLI error handling (hops > 2) |
| `bench_writes_report_now` | CLI bench outputs correct JSON |
| `bench_corpus_writes_engine_measurement_now` | CLI bench-corpus outputs correct JSON |
| `bench_corpus_accepts_deprecated_csv_flags_now` | Backward-compat for old CLI flags |

### Test Quality Assessment

**Good practices (TDD playbook compliant):**
- Four-word naming convention: `test_verb_constraint_target_qualifier`
  (they use `verb_target_qualifier_now` — close enough)
- Error cases tested (duplicate IDs, missing endpoints, truncated files,
  invalid hops)
- Full round-trip tested (build → query → verify)
- Contract tests separated from unit tests
- CLI tests use `assert_cmd` (proper binary testing)
- Fixtures in `tests/fixtures/valid/`

**Missing tests (TDD playbook RED flags):**
- No property tests (proptest is in dev-deps but no `proptest!` macros found)
- No tests for multi-relationship types (because not supported)
- No tests for graphs > 1000 nodes (fixture has 39 nodes, 67 edges)
- No tests for the low-RAM path specifically (the test uses default
  which falls through to low-RAM, but doesn't test memory budget)
- No performance regression tests (benchmarks exist but no "must be
  faster than X" assertions)
- No concurrency tests

---

## Rubber Duck: What My Frontend/Backend Split Got Wrong

### Flaw 1: "100x read" claim is uncontracted

I said: "Storage engine replacement gives 100x reads."

**TDD check:** Is there a test that proves 100x? No. The benchmark
suite measures latency but has no comparison baseline. The 100x
claim comes from the `Previous-learnings-01.md` document, which
compares against Neo4j Cypher-over-Bolt — a specific workload, not
general reads.

**Correction:** The 100x claim is for "exact-key fixed-hop
DEPENDS_ON neighborhood replay on a static graph snapshot." Not
for general reads. My doc generalizes this to "100x reads" in
multiple places. That's wrong.

### Flaw 2: "CSR shortcut detection" is invented, not designed

I said the query execution engine would have "Track 1: CSR SHORTCUT"
that detects traversal-heavy Cypher patterns and routes them to the
CSR engine.

**TDD check:** Is there a contract for this? No. There is no
`QueryShortcutDetector` trait, no pattern matching AST → CSR plan,
no tests for "this Cypher query should shortcut to CSR." This is
a design I invented in the analysis doc, not something that exists
or has been prototyped.

**Correction:** The shortcut idea is reasonable but unvalidated.
A proper TDD approach would:
1. STUB: Define `trait QueryRouter { fn route(&self, plan: &LogicalPlan) -> ExecutionStrategy; }` with `enum ExecutionStrategy { CsrShortcut, VolcanoPipeline }`
2. RED: Write tests: `MATCH (n {id: $id})-[:X]->(m)` → `CsrShortcut`, `MATCH (a)-[:X]->(b)-[:Y]->(c)` → `VolcanoPipeline`
3. GREEN: Implement pattern matching
4. REFACTOR: Optimize

None of this exists.

### Flaw 3: LOC compression ratios are gut estimates

I claimed multiple compression ratios (9-14x for frontend, 5-8x for
execution engine, etc.).

**TDD check:** Is there evidence for any of these ratios? Only
partially. The Bun rewrite showed 1:1.4 ratio (LOC INCREASED), not
5-8x compression. The MeshDB Bolt implementation exists but I didn't
count its LOC to verify my "10-15K" estimate.

**Correction:** The compression ratios are best guesses, not
evidence-based. To validate:
- Count MeshDB's Bolt impl LOC → verify "10-15K" claim
- Count `decypher` crate LOC → verify "5-10K parser" claim
- Count any existing Rust Volcano runtime → verify "15-20K" claim

I didn't do any of this.

### Flaw 4: "Frontend stays identical" glosses over Cypher completeness

I said: "The user should not know they switched."

**TDD check:** What percentage of Cypher is covered? I claimed "80%"
but that number is not backed by any analysis of real-world Cypher
query distributions. No one has measured what "80% of Cypher" means
in terms of actual user queries.

**Correction:** "80% of Cypher" is a guess. To validate, you'd need
to analyze query logs from real Neo4j deployments to see which Cypher
features are actually used. Without that data, the 80% claim is
unsupported. The honest answer is: "we don't know what percentage of
Cypher we need until we instrument real query logs."

### Flaw 5: Algorithm Storage Atlas is theoretical

I mapped 60 algorithms to 13 layout families and said "KNRT gets
per-algorithm optimal byte shape."

**TDD check:** How many of these layouts have been built and tested?
**One.** `AnchorDualCsrLayoutV1` — which is the existing Knight Bus
format. The other 12 families (InboundPowerLayoutV1, OrderedWedgeLayoutV1,
etc.) are pure design. No code, no tests, no benchmarks.

**Correction:** The Atlas is a research document, not a contract.
Claiming it in the split analysis as "KNRT's backend" overstates
readiness. The honest version:
- 1 of 13 layout families exists and is tested
- 12 of 13 are designed but have zero implementation
- The Atlas thesis ("storage should vibe with the algorithm") is
  unproven for every family except AnchorDualCsr

### Flaw 6: "34-56K LOC frontend" is arithmetic, not engineering

I estimated the frontend at 34-56K LOC by summing individual component
estimates. But these estimates don't account for:
- The GLUE between components (Cypher parser output → planner input →
  runtime operators → Bolt serialization). In Neo4j, this glue is
  ~45K LOC (`cypher/cypher/` assembly module). I estimated "3-5K"
  for it, but integration is typically underestimated.
- Error handling across layers (every layer needs to produce
  Neo4j-compatible error codes and messages)
- Configuration compatibility (~400 Neo4j settings)
- Edge cases in Cypher (NULL propagation, three-valued logic, implicit
  coercions)

**Correction:** Frontend is likely closer to 60-90K LOC if you include
proper integration, error fidelity, and configuration compatibility.
My 34-56K estimate is the happy-path minimum.

### Flaw 7: "mmap replaces Muninn in ~500 LOC" is too simple

I said: `io/` (14,241 LOC) → `memmap2` + thin wrapper (~500 LOC).

**TDD check:** Knight Bus DOES use mmap successfully. But Knight Bus
is read-only on immutable files. KNRT needs:
- mmap for reads (proven by Knight Bus)
- BUT: writes need a different approach (you can't mmap for concurrent
  writes safely without careful synchronization)
- Buffer pool or write-ahead approach for mutable data
- File growth management (CSR files are fixed-size after build, but
  a mutable store needs to grow)

**Correction:** mmap for the CSR read path is proven (~500 LOC, correct).
mmap for the mutable write path is NOT proven and would need a different
design. Total I/O layer is more like 2-5K LOC, not 500.

### Flaw 8: No contract exists for Neo4j import

I said "Extend Knight Bus build pipeline + Neo4j format reader."

**TDD check:** The `TruthGraphSource` trait exists, which is the
right extension point. BUT:
- `ValidatedTruthGraph` carries `CsvNodeRow` with hardcoded fields
  (node_id, node_type, label, parent_id, file_path, span)
- A Neo4j export has arbitrary properties per node and per relationship
- The trait would work, but `ValidatedTruthGraph` needs to become
  generic over property schemas

**Correction:** The import pattern (trait-based data source) is sound.
But the data model (`ValidatedTruthGraph`, `CsvNodeRow`, `CsvEdgeRow`)
needs significant generalization before it can carry Neo4j data. This
is not a "just implement the trait" task — it's a data model redesign.

### Flaw 9: I never asked "does the backend split even compile?"

**TDD Phase 1 (STUB) requires:** "Ensure the project compiles."

I proposed a major architectural split but never stubbed it out as
Rust code to see if the module boundaries make sense. Would the
`StorageEngine` trait I'm imagining actually work? Would the
`QueryRouter` enum compose with the Volcano pipeline? Would the
Bolt server's async runtime conflict with mmap's synchronous I/O?

**Correction:** Before accepting this split, we should STUB it:

```rust
// knrt/src/lib.rs — does this module structure even make sense?
pub mod storage {
    pub trait StorageEngine { /* ... */ }
    pub mod csr { /* Knight Bus's existing code */ }
    pub mod mutable { /* new mutable store */ }
}
pub mod query {
    pub trait QueryPlanner { /* ... */ }
    pub trait QueryRuntime { /* ... */ }
    pub mod cypher_parser { /* ... */ }
    pub mod planner { /* ... */ }
    pub mod operators { /* ... */ }
    pub mod router { /* CSR shortcut detection */ }
}
pub mod protocol {
    pub mod bolt { /* ... */ }
    pub mod http { /* ... */ }
}
pub mod import {
    pub trait GraphSource { /* generalized TruthGraphSource */ }
    pub mod neo4j { /* Neo4j dump reader */ }
    pub mod csv { /* existing CSV reader */ }
}
```

Would this compile? Would the trait bounds work? We don't know
because we haven't tried.

---

## Honest Scorecard: What We HAVE vs What We CLAIM

| Claim in my analysis | Contract? | Test? | Evidence? | Verdict |
|---|---|---|---|---|
| Knight Bus builds CSR snapshots from CSV | `SnapshotArtifactWriter` | `build_query_and_verify_round_trip_now` | 39 nodes, 67 edges fixture | **PROVEN** |
| Knight Bus queries 1-2 hop neighbors | `WalkQueryRuntime` | `query_normalized_graph_uses_within_two_hops` | Exact key + direction + hops | **PROVEN** |
| Knight Bus uses mmap for reads | (concrete `MmapWalkRuntime`) | `open_detects_truncated_offsets_now` | 7 mmap'd binary files | **PROVEN** |
| Low-RAM build works for large graphs | (in `low_ram.rs`) | `low_ram_build_and_verify_record_phase_peaks_now` | Phase peaks tracked, but only 39-node test | **PARTIALLY PROVEN** (small data only) |
| Parity between snapshot and truth | (in `parity.rs`) | `parity_uses_all_expected_families_now` | All query families verified | **PROVEN** |
| Benchmark suite measures latency + RSS | `BenchmarkScenarioRunner` | 5 benchmark tests | p50, p95, p99, RSS | **PROVEN** |
| Error handling catches corruption | `KnightBusError` | `build_rejects_duplicate_node_ids_now`, etc. | 4 error-path tests | **PROVEN** |
| CLI works for all commands | (clap) | 6 CLI tests | build, query, verify, bench, bench-corpus | **PROVEN** |
| 100x faster than Neo4j for reads | NONE | NONE | Previous-learnings-01.md (narrative, not test) | **UNPROVEN** (no comparative benchmark test) |
| CSR shortcut detection for Cypher | NONE | NONE | Invented in analysis doc | **UNPROVEN** (no code, no design) |
| Atlas layout families (12 of 13) | NONE | NONE | Atlas doc (design only) | **UNPROVEN** (only AnchorDualCsr exists) |
| Bolt protocol in 10-15K LOC | NONE | NONE | Estimate based on MeshDB | **UNPROVEN** (never counted MeshDB) |
| Cypher parser in 5-10K LOC | NONE | NONE | Estimate based on decypher | **UNPROVEN** (never counted decypher) |
| Frontend 34-56K LOC total | NONE | NONE | Sum of individual estimates | **UNPROVEN** (estimates are gut-level) |
| Backend 100-160K LOC total | NONE | NONE | Sum of individual estimates | **UNPROVEN** (estimates are gut-level) |
| Neo4j import via TruthGraphSource | `TruthGraphSource` | NONE | Trait exists but data model is too narrow | **PARTIALLY SUPPORTED** |
| mmap replaces Muninn | (concrete impl) | PROVEN for reads | Knight Bus uses mmap successfully | **PROVEN FOR READS, NOT FOR WRITES** |
| "User changes one connection string" | NONE | NONE | Aspirational design | **UNPROVEN** |

**Score: 8 PROVEN, 3 PARTIALLY, 9 UNPROVEN.**

That means **9 of 20 claims in my analysis have zero evidence.**

---

## What TDD Would Say to Do Next

Following the playbook strictly:

### Phase 1: STUB — validate the architecture compiles

Before writing any more analysis docs, stub out the KNRT module
structure in Rust. `pub mod` + `pub trait` + `todo!()` bodies.
`cargo check` must pass. This forces us to answer:
- Do the trait bounds work?
- Do the module dependencies form a DAG?
- Can the CSR shortcut trait compose with the Volcano pipeline trait?

### Phase 2: RED — write failing tests for the FIRST unproven claim

Pick the highest-value unproven claim and write a test for it. The
most impactful would be:

**Test 1:** Comparative benchmark (Knight Bus vs Neo4j Cypher-over-Bolt)
- This proves or disproves the "100x" claim with actual numbers
- Knight Bus already has benchmark infrastructure
- BUT: requires a Neo4j instance to compare against

**Test 2:** Multi-relationship-type CSR snapshot
- Extend `NormalizedGraphData` to carry edge types
- Write test: build snapshot with 3 edge types → query specific type
- This proves or disproves that CSR can handle multi-type graphs

**Test 3:** Property storage in snapshot
- Extend `NodeRecord` to carry property offsets
- Write test: build snapshot with 5 properties per node → read property
- This proves or disproves that CSR snapshots can carry properties

### Phase 3: GREEN — implement minimum code

### Phase 4: REFACTOR — clean up

---

## Corrections to the Frontend/Backend Split

| Original claim | Correction |
|---|---|
| Frontend: 34-56K LOC | More like 60-90K with integration + error fidelity |
| Backend: 100-160K LOC | Probably accurate for v1, but 9 of 20 sub-claims are unproven |
| "100x reads" | "100x for exact-key fixed-hop neighborhood on immutable snapshot" |
| 13 Atlas layout families | 1 proven, 12 designed but unbuilt |
| mmap replaces Muninn in 500 LOC | 500 LOC for reads (proven), 2-5K for read+write I/O layer |
| CSR shortcut detection | Reasonable idea but no code, no tests, no contract |
| Neo4j import via TruthGraphSource | Trait exists but data model needs generalization |
| "User changes one connection string" | Aspirational — no Bolt, no Cypher, no driver compat today |

---

## What We ACTUALLY Have Contracts and Tests For

Summarized honestly:

**4 traits (contracts):**
1. `WalkQueryRuntime` — query neighbors by key/direction/hops
2. `SnapshotArtifactWriter` — write CSR snapshot to filesystem
3. `TruthGraphSource` — load validated graph from data source
4. `BenchmarkScenarioRunner` — run benchmark scenarios

**23 tests (all green, zero clippy warnings):**
- 9 unit tests (graph normalization, truth index, RSS, percentile, corpus)
- 8 library contract tests (build, query, verify, error paths, benchmark)
- 6 CLI tests (all commands, error handling, backward compat)

**Proven capabilities:**
- CSV → immutable dual-CSR snapshot (with dedup, validation, phase tracking)
- mmap-based read runtime (binary search key lookup + CSR array slice)
- Forward and reverse 1-2 hop neighborhood queries
- Low-RAM build with external merge sort
- Parity verification (snapshot vs truth source)
- Benchmark suite (p50/p95/p99 latency, RSS tracking)
- Clean error handling (14 error variants)

**NOT proven by any test or contract:**
- Anything involving Cypher, Bolt, TCP, properties, mutations,
  transactions, multi-relationship types, variable-length paths,
  aggregation, filtering, Neo4j import, or the 100x speed claim.

That's the honest TDD-verified inventory.

---

## Appendix A: The Knight Bus Storage Format Story

*Integrated from cross-session research notes. These ideas evolved
across multiple conversations and tools. They are included here
WITHOUT filtering because the user requested "don't lose the
diversity of ideas."*

### The Governing Thought

The repeated Knight Bus thesis is:

> Store the graph in the shape the runtime wants to walk, so the
> hot path becomes direct indexed reads instead of dynamic graph
> reconstruction.

Everything else is downstream from that.

### The Six Stable Design Ideas

These are the most-repeated and most-stable ideas across all
accessible notes. They are not claims — they are design principles
that keep reappearing.

#### 1. Truth layer and runtime layer must stay separate

- `truth layer`: readable source inputs (`nodes.csv`, `edges.csv`)
- `runtime layer`: compact compiled snapshot optimized for walking

The point is not merely performance. The point is also intellectual
honesty:

- truth remains inspectable
- parity can be checked against truth
- the runtime is free to throw away semantic baggage the hot path
  does not need

Short form: **CSV is truth, not hot path.**

**TDD verdict:** This separation IS contractual in Knight Bus.
`TruthGraphSource` loads truth. `SnapshotArtifactWriter` builds
runtime. `parity.rs` verifies them against each other. This is the
strongest-proven design idea.

#### 2. The winning base shape is dual CSR plus exact lookup

The dominant repeated base format is:

- dense integer node IDs (`u32`)
- forward adjacency (`offsets + peers`)
- reverse adjacency (`offsets + peers`)
- sorted exact-key lookup (`key_index.bin`)
- memory-mapped file access (`mmap`)

The concrete snapshot shape that repeats most often:

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

The key lookup and walk path are intentionally different concerns:

- key lookup finds the dense ID
- offsets and peers answer the walk

**TDD verdict:** This IS the implemented format. Every file above
exists in `snapshot.rs` constants. Every file is tested in the
round-trip test. This is fully proven.

#### 3. The runtime must be build-heavy and walk-light

Build-time work is allowed to do:

- counting, sorting, dense-ID assignment
- forward/reverse adjacency materialization
- validation and manifest writing

Runtime work should mostly do:

- read manifest
- validate shape
- `mmap` fixed files
- resolve key
- read one slice

Short form: **build-time heavy, walk-time boring.**

**TDD verdict:** The `low_ram.rs` builder (1,703 LOC) does all the
heavy lifting. The `MmapWalkRuntime` query path is ~50 lines of
binary search + offset arithmetic. The principle is PROVEN in code.

#### 4. Storage is aligned only when the hot path is visible in the bytes

This is the sharpest sentence in the whole note family:

> Storage is only aligned to runtime when the runtime can almost
> directly "see" its hot path inside the stored bytes.

In practice, that means:

- no reverse-edge reconstruction at query time
- no edge-table rescans for one-hop queries
- no row-materialization machinery in the walk path
- no query planner in the narrow runtime
- no generalized graph-database semantics on the hot path

**TDD verdict:** This IS what `MmapWalkRuntime` does. Dense ID →
offset slice → peer array. The hot path is literally visible in
the bytes. PROVEN for the narrow traversal case.

#### 5. mmap is part of the format story, not just an implementation detail

Why:

- lets the OS page in only touched regions
- keeps startup simple (no heap reconstruction of the whole graph)
- avoids managed buffer pools
- pairs naturally with contiguous `offsets + peers` slices

This is why the native runtime story was preferred over a Wasm-first
story in the repo notes.

**TDD verdict:** `MmapWalkRuntime` uses `memmap2` for 7 binary
files. `open_detects_truncated_offsets_now` tests that mmap validates
file sizes. PROVEN.

#### 6. Exact-key search should stay tiny and off the traversal hot path

Another repeated rule:

- search is okay
- search in every hop is not okay

So the design keeps:

- one compact exact-key entry path (binary search on `key_index.bin`)
- one separate adjacency walk path (offset → peer slice)

This is why `key_index.bin` is treated as a sidecar, not as the
center of the engine.

**TDD verdict:** `resolve_dense_id` does binary search once. After
that, all traversal is dense-ID arithmetic with zero key lookups
per hop. PROVEN.

### The Hot Path Mental Model

The recurring mental model is NOT "query a graph database."

It is:

```text
find key
  → get dense id
  → read start offset
  → read end offset
  → slice contiguous peers
```

The repeated contrast:

- Neo4j: generalized property graph, planner, traversal machinery,
  row materialization, process boundaries
- Knight Bus: fixed graph world, fixed relationship semantics,
  direct slice replay

The fairest repeated claim is NOT "Knight Bus beats Neo4j everywhere."

It is:

> Knight Bus is dramatically better when the workload is exact-anchor,
> fixed-hop replay over a static snapshot.

### The Four Guardrails

These are the repeated "do not mess this up" rules:

1. **Do not turn it into a generic graph database too early.**
   Repeated non-goals: not a query-language project first, not a
   graph database first, not a broker-backed graph reader, not a
   per-hop RPC system, not a Wasm-first showcase.

2. **Do not let lookup and traversal collapse into one heavy path.**
   Once search, row decoding, planner work, reverse-edge derivation,
   and traversal are mixed together, the narrow runtime loses its edge.

3. **Do not overclaim benchmark generality.** Current proofs are
   about fixed-hop traversal. Comparisons were against Cypher over
   Bolt, not every Neo4j subsystem. GDS and broader algorithm
   families need separate, fair comparisons.

4. **Do not optimize for beautiful storage if it mismatches the
   operation.** The real criterion is not elegance. It is whether
   the bytes match the operation tightly enough that the CPU mostly
   performs direct arithmetic and contiguous reads.

### The Four Stages of Evolution

The ideas evolved through four stages across sessions:

**Stage 1: Prove a narrow win.**
Static graph, exact-key anchor lookup, 1-2 hop traversal, dual CSR
snapshot. This is where Knight Bus is today.

**Stage 2: Formalize the storage-runtime doctrine.**
Truth layer vs runtime layer. Parseltongue for graph shape. Iggy
for durability discipline. Immutable sealed artifact. The
`STORAGE_RUNTIME_ALIGNMENT.md` and `KNIGHT_BUS_THESIS.md` docs
capture this.

**Stage 3: Generalize to other workloads.**
Single-node Rust compute thesis. Archive-memory graph retrieval
(Tweet Scrolls). Other fixed-structure workloads. The same format
idea applied beyond code-dependency graphs.

**Stage 4: Generalize to backend families.**
Algorithm-specific layout families. Bespoke contracts over reusable
runtime families. `FormatSelectionProfile` plus result sidecars.
Backend-only specialization. Neo4j-compatible frontend. This is the
Algorithm Storage Atlas stage.

### What the Chats Added Beyond the Repo Docs

Four extensions emerged from the broader conversation traces:

#### Extension 1: The same thesis was generalized beyond code graphs

The format idea was applied to:
- single-node Rust compute/storage systems
- Tweet Scrolls archive memory retrieval
- Neo4j algorithm-specific backend formats

"Knight Bus-style" became a broader design pattern:
> Make storage vibe with the exact operation being run.

#### Extension 2: Tweet Scrolls reused the same split

In the Tweet Scrolls architecture prompts:
- archive records = truth layer
- compiled memory snapshot = runtime layer
- nodes: `tweet`, `DM`, `participant`, `thread`, `topic`, `time bucket`
- fixed-hop traversal for memory questions

The storage idea escaped the original code-dependency benchmark and
became a generic "memory graph runtime" pattern.

#### Extension 3: The storage thesis expanded into algorithm-specific layouts

The biggest expansion appears in the Atlas. The crucial nuance:

> "Bespoke" is the right publication model for the contract, but
> not the right implementation model for the engine.

In practice:
- the product publishes specific contracts (`PageRankInboundPowerSnapshotV1`)
- the runtime reuses a smaller set of internal layout families
- a `FormatSelectionProfile` chooses the family + required planes
  + result sidecars for the requested algorithm
- the base graph stays immutable and sealed
- exact-key lookup remains separate from traversal or compute

The later internal vocabulary became more explicit:

| Concept | Role |
|---|---|
| `BaseGraphSnapshot` | sealed topology artifact |
| `PropertyPlane` | typed numeric or categorical planes |
| `AlgorithmArtifact` | algorithm-specific open-time view |
| `ComputeScratch` | temporary arrays, heaps, queues, buckets, tensors |
| `ResultSidecar` | persisted scores, paths, flows, clusters, embeddings |
| `FormatSelectionProfile` | algorithm → layout family mapping + sidecars |

**TDD verdict:** NONE of these concepts exist as Rust code. They
are design vocabulary, not contracts. Zero traits, zero tests.

#### Extension 4: The Neo4j-compatible thinking imposed a boundary

The faithful-port dossier adds the guardrail:
- the **usage interface** should stay Neo4j-like
- the **backend** can diverge aggressively

That means the layout-family vocabulary is good **engine language**
but not necessarily good **product language**.

### The Family Taxonomy: What It's Really Saying

The later conversation stopped treating "graph algorithms" as one
storage class and instead grouped them by the primitive their inner
loop actually needs:

| Layout family | Dominant primitive | Algorithms |
|---|---|---:|
| `AnchorDualCsrLayoutV1` | exact anchor → adjacency slices | 4 |
| `InboundPowerLayoutV1` | repeated inbound score accumulation | 4 |
| `ConnectivityLowlinkLayoutV1` | DFS numbering, lowlinks, reverse-pass replay | 4 |
| `OrderedWedgeLayoutV1` | sorted-neighbor intersection, wedge counting | 9 |
| `PartitionRefinementLayoutV1` | community assignment updates and evaluation | 9 |
| `PeelBucketLayoutV1` | low-degree peeling, bucket discipline | 3 |
| `RelaxationFrontierLayoutV1` | weighted frontier relaxation | 10 |
| `EdgeOrderForestLayoutV1` | globally ordered edge scan + union-find | 2 |
| `FlowResidualLayoutV1` | mutable residual arc updates | 4 |
| `FeatureMetricLayoutV1` | row-major feature distance, candidate refinement | 4 |
| `EmbeddingSampleLayoutV1` | neighborhood/walk sampling for embeddings | 4 |
| `DagOrderLayoutV1` | topological replay over acyclic graphs | 2 |
| `InfluenceMonteCarloLayoutV1` | repeated stochastic cascade simulation | 1 |
| **Total** | | **60** |

The deeper bespoke-storage claim:

> Dual CSR is the first strong proof, but the more durable doctrine
> is to choose a byte shape family that makes the intended inner loop
> boring.

### The Three Practical Rules from the Atlas

1. Traversal-style algorithms want contiguous adjacency slices
2. Score-propagation algorithms want dense numeric planes over inbound edges
3. Feature, flow, and training workloads want their own storage
   primitives and should not be shoved through the same topology-only
   snapshot

### The Prototype Order Was Explicit

P0 families (build first):
1. `AnchorDualCsrLayoutV1` — closest to existing Knight Bus proof
2. `InboundPowerLayoutV1` — unlocks PageRank, strong speedup potential
3. `ConnectivityLowlinkLayoutV1` — SCC/WCC high-value baseline analytics
4. `RelaxationFrontierLayoutV1` — Dijkstra, clean proof after fixed-hop
5. `OrderedWedgeLayoutV1` — Triangle Count, sorted intersection wins

If only five concrete proof snapshots were built, the recommended set:

| # | Proof format | What it proves |
|---|---|---|
| 1 | `DegreeCentralityAnchorDualCsrSnapshotV1` | Slice replay |
| 2 | `BfsTraversalFrontierSnapshotV1` | Frontier expansion |
| 3 | `PageRankInboundPowerSnapshotV1` | Inbound power iteration |
| 4 | `DijkstraSingleSourceHeapRelaxationSnapshotV1` | Weighted shortest path |
| 5 | `TriangleCountOrderedWedgeSnapshotV1` | Sorted-neighbor intersection |

That sequence has a systems-proof shape:
- first prove slice replay
- then prove inbound power iteration
- then shortest-path relaxation
- then wedge intersection
- only then move deeper into community, flow, feature, or embedding families

**TDD verdict:** Only proof #1 (AnchorDualCsr) has any implementation.
Proofs #2-#5 are designed on paper. Zero code, zero tests, zero
benchmarks for any of them.

### The Open Questions

These show up across the notes and remain unresolved:

1. How far can the narrow snapshot idea stretch before a general
   property-graph backend is unavoidable?
2. Which workloads deserve dedicated layout families and which are
   mostly compute-bound after load?
3. How should mutable overlays work without contaminating the base
   immutable runtime?
4. If the user-facing interface must stay Neo4j-like, where exactly
   does backend divergence begin?
5. How much of the lookup path should stay sidecar-only before it
   needs richer indexing?
6. Should a future engine allow one base snapshot to expose both
   CSR-style planes and tensor-ready feature planes without
   duplicated bytes?
7. Should vector-heavy families support `f16` or quantized planes
   for cache density once correctness baselines are proven?
8. Should filtered variants stay execution policies forever or
   eventually earn dedicated filter-first precomputed postings?
9. Should flow algorithms be allowed a writable memory overlay for
   residual updates while keeping the durable snapshot immutable?
10. Should APSP be implemented at all in a laptop-first engine or
    only as a blocked offline batch artifact builder?

### Best One-Sentence Summary

> Knight Bus is the idea that you should compile the graph into a
> memory-mapped, immutable, dense-ID snapshot whose bytes already
> expose the walk hot path, instead of asking a general engine to
> rediscover that path on every query.

---

## Appendix B: Full Algorithm Storage Atlas — Per-Algorithm Contracts

*60 algorithms. 13 layout families. Each row is a concrete contract
specifying exactly what on-disk shape that algorithm wants.*

The Atlas answers one question:

> If a Knight Bus successor wanted to beat generic property-graph
> execution on a narrow graph workload, what on-disk shape should
> each Neo4j GDS algorithm want?

### Expert Lenses Applied

- **Storage-systems:** optimize byte layout for the dominant
  read/write pattern, not for query-language convenience.
- **Graph-algorithms:** classify by true primitive: slice replay,
  power iteration, wedge intersection, relaxation, residual flow,
  feature-metric search, or training.
- **Benchmark-fairness:** separate "beats Cypher over a property
  graph" from "beats GDS over a projected in-memory graph."
- **Operator:** prefer sealed artifacts, tiny sidecar indexes,
  explicit validation, restart safety.
- **Skeptical engineer:** ask whether a custom format really wins
  or whether the workload is mostly compute-bound after load.

### The Chosen Design

| Approach | Upside | Downside | Verdict |
|---|---|---|---|
| One universal snapshot | Simple implementation | Leaves hot loops misaligned | **Reject** |
| Fully bespoke per algorithm | Maximum local fit | Too many near-duplicate engines | **Reject** |
| Hybrid: bespoke contracts over reusable families | Honest about differences while reusing runtime | Requires vocabulary + normalization | **Choose** |

### Per-Layout Family Contracts

#### `AnchorDualCsrLayoutV1` — 4 algorithms

**Use when:** dominant operation is "exact anchor to one or two adjacency slices."

```text
manifest.json       node_table.bin      key_index.bin
fwd.offsets.u64     fwd.peers.u32       rev.offsets.u64     rev.peers.u32
degree.out.u32 [opt]    degree.in.u32 [opt]
walk.alias_prob.f32 [opt]   walk.alias_jump.u32 [opt]
```

Hot path: `key → dense_id → offsets[id]..offsets[id+1] → peer slice`

| Algorithm | Format | Priority |
|---|---|---|
| Degree Centrality | `DegreeCentralityAnchorDualCsrSnapshotV1` | P0 |
| BFS | `BfsTraversalFrontierSnapshotV1` | P0 |
| DFS | `DfsTraversalStackSnapshotV1` | P0 |
| Random Walk | `RandomWalkAliasSnapshotV1` | P0 |

**Status: THIS IS THE EXISTING KNIGHT BUS FORMAT.** Only family
with any implementation.

#### `InboundPowerLayoutV1` — 4 algorithms

**Use when:** dominant operation is repeated inbound score accumulation.

```text
manifest.json       node_table.bin      key_index.bin
in.offsets.u64      in.peers.u32        in.weight.f32 [opt]
out.mass.f32        dangling.bitset     partition.node_ranges [opt]
```

Hot path: `for node: score_next[node] = base + sum(score[src] * weight / mass[src])`

| Algorithm | Format | Priority |
|---|---|---|
| PageRank | `PageRankInboundPowerSnapshotV1` | P0 |
| Article Rank | `ArticleRankInboundPowerSnapshotV1` | P1 |
| Eigenvector Centrality | `EigenvectorInboundPowerSnapshotV1` | P1 |
| HITS | `HitsHubAuthoritySnapshotV1` | P1 |

**Status: DESIGN ONLY.** Zero code.

#### `ConnectivityLowlinkLayoutV1` — 4 algorithms

**Use when:** algorithm wants DFS numbering, lowlinks, or reverse-pass replay.

```text
manifest.json       key_index.bin
fwd.offsets.u64     fwd.peers.u32       rev.offsets.u64     rev.peers.u32
undir.offsets.u64   undir.peers.u32     undir.edge_id.u32   undir.twin_halfedge.u32
```

Hot path: stack-based DFS or finish-order forward then reverse replay.

| Algorithm | Format | Priority |
|---|---|---|
| Articulation Points | `ArticulationLowlinkSnapshotV1` | P1 |
| Bridges | `BridgeLowlinkSnapshotV1` | P1 |
| SCC | `SccFinishOrderSnapshotV1` | P1 |
| WCC | `WccUnionFindSnapshotV1` | P1 |

**Status: DESIGN ONLY.** Zero code.

#### `OrderedWedgeLayoutV1` — 9 algorithms

**Use when:** dominant operation is sorted-neighbor intersection or wedge counting.

```text
manifest.json
left.offsets.u64    left.neighbors.u32  degree.u32
degeneracy.order.u32    neighbor.weight.f32 [opt]
```

Hot path: pick lower-degree endpoint, intersect sorted neighbor lists.

| Algorithm | Format | Priority |
|---|---|---|
| Triangle Count | `TriangleCountOrderedWedgeSnapshotV1` | P1 |
| Local Clustering Coefficient | `LocalClusteringCoefficientSnapshotV1` | P2 |
| Clique Counting | `CliqueCountingOrderedWedgeSnapshotV1` | P3 |
| Node Similarity | `NodeSimilarityIntersectionSnapshotV1` | P2 |
| Filtered Node Similarity | `FilteredNodeSimilarityIntersectionSnapshotV1` | P3 |
| Adamic Adar | `AdamicAdarIntersectionSnapshotV1` | P2 |
| Common Neighbors | `CommonNeighborsIntersectionSnapshotV1` | P1 |
| Resource Allocation | `ResourceAllocationIntersectionSnapshotV1` | P2 |
| Total Neighbors | (same family) | P2 |

**Status: DESIGN ONLY.** Zero code.

#### `PartitionRefinementLayoutV1` — 9 algorithms

**Use when:** algorithm repeatedly updates or evaluates community assignments.

```text
manifest.json
undirected.offsets.u64  undirected.peers.u32    edge.weight.f32 [opt]
node.volume.f32     community.seed.u32 [opt]    community.input.u32 [opt]
```

Hot path: scan community labels on adjacent nodes, update gain or vote.

| Algorithm | Format | Priority |
|---|---|---|
| Louvain | `LouvainRefinementSnapshotV1` | P2 |
| Leiden | `LeidenRefinementSnapshotV1` | P2 |
| Label Propagation | `LabelPropagationCommunitySnapshotV1` | P2 |
| Modularity Optimization | `ModularityOptimizationSnapshotV1` | P2 |
| Conductance metric | `ConductanceBoundarySnapshotV1` | P2 |
| Modularity metric | `ModularityMetricSnapshotV1` | P2 |
| Approx Max k-cut | `ApproxMaxKCutPartitionSnapshotV1` | P2 |
| SLLPA | `SllpaSpeakerListenerSnapshotV1` | P2 |
| Same Community | `SameCommunityJoinSnapshotV1` | P2 |

**Status: DESIGN ONLY.** Zero code.

#### `PeelBucketLayoutV1` — 3 algorithms

**Use when:** algorithm peels low-degree nodes or does greedy assignment.

```text
manifest.json
undirected.offsets.u64  undirected.peers.u32    degree.u32
bucket.head.u32     bucket.next.u32     neighbor_color.bitset [opt]
```

| Algorithm | Format | Priority |
|---|---|---|
| K-Core Decomposition | `KCorePeelBucketSnapshotV1` | P1 |
| K-1 Coloring | `K1ColoringBucketSnapshotV1` | P2 |
| Preferential Attachment | `PreferentialAttachmentDegreeSnapshotV1` | P1 |

**Status: DESIGN ONLY.** Zero code.

#### `RelaxationFrontierLayoutV1` — 10 algorithms

**Use when:** algorithm relaxes weighted edges from a frontier.

```text
manifest.json
out.offsets.u64     out.peers.u32       edge.weight.f32
edge.src.u32 [opt]  edge.dst.u32 [opt]  heuristic.f32 [opt]
rev.offsets.u64 [opt]   rev.peers.u32 [opt]
```

Hot path: frontier pop → relax outgoing edges → update distance + queue.

| Algorithm | Format | Priority |
|---|---|---|
| Dijkstra Source-Target | `DijkstraSourceTargetSnapshotV1` | P1 |
| Dijkstra Single-Source | `DijkstraSingleSourceSnapshotV1` | P1 |
| Delta-Stepping SSSP | `DeltaSteppingBucketSnapshotV1` | P1 |
| A* | `AStarHeuristicSnapshotV1` | P1 |
| Yen's Shortest Path | `YensDeviationSnapshotV1` | P2 |
| Bellman-Ford SSSP | `BellmanFordEdgeScanSnapshotV1` | P2 |
| Betweenness Centrality | `BetweennessBrandesSnapshotV1` | P2 |
| Closeness Centrality | `ClosenessRadiusSnapshotV1` | P2 |
| Harmonic Centrality | `HarmonicRadiusSnapshotV1` | P2 |
| APSP | `AllPairsShortestBlockedSnapshotV1` | P3 |

**Status: DESIGN ONLY.** Zero code.

#### `EdgeOrderForestLayoutV1` — 2 algorithms

**Use when:** the winning representation is a sorted edge plane + union-find.

```text
manifest.json
edge.src.u32    edge.dst.u32    edge.weight.f32     edge.order.u32
```

| Algorithm | Format | Priority |
|---|---|---|
| Min Weight Spanning Tree | `MinimumWeightSpanningTreeSnapshotV1` | P2 |
| Min Weight k-Spanning Tree | `MinimumWeightKSpanningTreeSnapshotV1` | P2 |

**Status: DESIGN ONLY.** Zero code.

#### `FlowResidualLayoutV1` — 4 algorithms

**Use when:** algorithm needs mutable residual capacity and reverse-arc jumps.

```text
manifest.json
residual.offsets.u64    residual.head.u32   residual.cap.f32
residual.rev_arc.u32    residual.cost.f32 [opt]
terminal.role.u8    terminal.supply.f32 [opt]   terminal.demand.f32 [opt]
```

| Algorithm | Format | Priority |
|---|---|---|
| Maximum Flow | `MaximumFlowResidualSnapshotV1` | P2 |
| Min-Cost Max-Flow | `MinCostMaxFlowResidualSnapshotV1` | P2 |
| Directed Steiner Tree | `DirectedSteinerTerminalSnapshotV1` | P2 |
| Prize-collecting Steiner | `PrizeCollectingSteinerSnapshotV1` | P2 |

**Status: DESIGN ONLY.** Zero code. Also NOTE: flow algorithms
need **mutable** overlays, which contradicts the immutable snapshot
model. This is an open design question.

#### `FeatureMetricLayoutV1` — 4 algorithms

**Use when:** graph topology is secondary and the dominant operation
is vector distance or nearest-neighbor refinement.

```text
manifest.json       node_table.bin      key_index.bin
features.row_offsets.u64    features.values.f32
features.rowmajor.f32 [opt] feature.norm.f32
candidate.offsets.u64 [opt] candidate.peers.u32 [opt]
```

| Algorithm | Format | Priority |
|---|---|---|
| KNN | `KnnFeatureAnnSnapshotV1` | P3 |
| Filtered KNN | `FilteredKnnFeatureAnnSnapshotV1` | P3 |
| K-Means | `KMeansFeatureCentroidSnapshotV1` | P3 |
| HDBSCAN | `HdbscanDistanceMstSnapshotV1` | P3 |

**Status: DESIGN ONLY.** Zero code. Also NOTE: K-Means and HDBSCAN
are **not graph workloads** — they ignore relationships entirely.
Skeptical-engineer lens downgrades these.

#### `EmbeddingSampleLayoutV1` — 4 algorithms

**Use when:** algorithm samples neighborhoods or random walks to
emit embeddings or model weights.

```text
manifest.json       node_table.bin      key_index.bin
fwd.offsets.u64     fwd.peers.u32       rev.offsets.u64 [opt]   rev.peers.u32 [opt]
feature.rowmajor.f32 [opt]  alias.jump.u32 [opt]    alias.prob.f32 [opt]
sample.seed.u64     neg.alias.jump.u32 [opt]    neg.alias.prob.f32 [opt]
```

| Algorithm | Format | Priority |
|---|---|---|
| FastRP | `FastRpProjectionSnapshotV1` | P2 |
| GraphSAGE | `GraphSageSampledNeighborhoodSnapshotV1` | P3 |
| Node2Vec | `Node2VecWalkCorpusSnapshotV1` | P3 |
| HashGNN | `HashGnnFeatureNeighborhoodSnapshotV1` | P3 |

**Status: DESIGN ONLY.** Zero code. Skeptical-engineer lens:
training-dominated workloads where storage format matters less.

#### `DagOrderLayoutV1` — 2 algorithms

**Use when:** graph is acyclic and the winning primitive is
in-degree peeling then topological replay.

```text
manifest.json
dag.offsets.u64     dag.peers.u32       dag.weight.f32 [opt]
in_degree.u32       topo.order.u32 [opt]
```

| Algorithm | Format | Priority |
|---|---|---|
| Topological Sort | `TopologicalOrderDagSnapshotV1` | P2 |
| Longest Path | `LongestPathDagSnapshotV1` | P2 |

**Status: DESIGN ONLY.** Zero code.

#### `InfluenceMonteCarloLayoutV1` — 1 algorithm

**Use when:** dominant work is repeated stochastic propagation.

```text
manifest.json
fwd.offsets.u64     fwd.peers.u32       activation.prob.f32
rrset.offsets.u64 [opt]     rrset.nodes.u32 [opt]   seed_gain.cache.f32 [opt]
```

| Algorithm | Format | Priority |
|---|---|---|
| CELF | `CelfInfluenceCascadeSnapshotV1` | P3 |

**Status: DESIGN ONLY.** Zero code. Skeptical lens: Monte Carlo
simulation dominates after graph is loaded.

### Atlas TDD Summary

| Status | Count | % |
|---|---|---|
| **Implemented and tested** | 1 family (4 algorithms) | 7% |
| **Designed on paper** | 12 families (56 algorithms) | 93% |
| **Total** | 13 families (60 algorithms) | 100% |

---

## Appendix C: Source Notes Worth Re-Reading

If someone wants the shortest reading list after this document:

1. `STORAGE_RUNTIME_ALIGNMENT.md` — the core doctrine
2. `KNIGHT_BUS_THESIS.md` — the thesis statement
3. `KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md` — the 60-algorithm expansion
4. `A-20260525164835-faithful-rust-port-dossier.md` — the honest
   cost analysis

---

## Updated Bottom Line

The accessible notes do NOT describe Knight Bus as "a faster graph
database." They describe it as a stricter systems discipline:

- separate truth from runtime
- compile structure once
- make the hot path visible in the bytes
- keep search and traversal separate
- use immutable snapshots by default
- publish bespoke contracts while reusing a small family of byte-level layouts
- specialize backend storage to the operation, not the other way around

**Against the TDD playbook, the score is:**

- **The discipline is well-articulated.** 6 stable design ideas,
  4 guardrails, 4 stages of evolution, all internally consistent.
- **The implementation proves the first stage.** Dual CSR,
  build-heavy/walk-light, truth-runtime separation, mmap — all
  proven with 23 passing tests and 4 traits.
- **The later stages (Atlas families, Cypher, Bolt, Neo4j compat)
  are research documents, not contracts.** 93% of the Atlas is
  design-only. Zero lines of Cypher parser, Bolt protocol, or
  general query runtime exist.

The honest gap:

| What | Status |
|---|---|
| Core doctrine | **Articulated and proven** |
| AnchorDualCsr format | **Built and tested** |
| 12 other layout families | **Designed, not built** |
| Cypher parser | **Not started** |
| Bolt protocol | **Not started** |
| Query planner | **Not started** |
| Query runtime operators | **Not started** |
| Property storage | **Not started** |
| Multi-relationship types | **Not started** |
| Mutations / writes | **Not started** |
| Neo4j import | **Not started** |
| Frontend/backend trait interface | **Not started** |

The doctrine is strong. The proof of the first stage is strong.
Everything else is honest design work that has not yet entered
the TDD cycle.

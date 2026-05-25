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

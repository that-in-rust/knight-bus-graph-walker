# v001 PRD

```text
Document:     v001 Product Requirements Document
Status:       Draft L1
Version:      v001
Scope:        CLI-first Knight Bus walk-runtime proof
Audience:     Product, engineering, benchmark, and demo preparation
```

## 1. User Journeys

### Journey 01: Start Binary And Attach CSV Truth Inputs

The first user journey should feel explicit and trustworthy.

1. The user starts the `knight-bus` binary from a terminal.
2. The user already has `nodes.csv` and `edges.csv`.
3. The user runs:

   ```bash
   knight-bus build --nodes-csv ./nodes.csv --edges-csv ./edges.csv --output ./snapshot
   ```

4. The binary validates that both files exist and are readable before any expensive work begins.
5. The binary validates CSV headers and required fields before snapshot build begins.
6. If either CSV is malformed, the binary exits with a typed, actionable error and no partial success claim.

The user should leave this journey with one clear mental model:

> CSV is the truth layer. The snapshot is the runtime layer.

### Journey 02: Build Snapshot And Verify Parity

The second journey should prove the runtime is faithful to the truth layer.

1. The user runs `build`.
2. The binary parses the truth-layer CSV inputs.
3. The binary assigns stable dense IDs.
4. The binary compiles a dual-adjacency snapshot.
5. The user runs:

   ```bash
   knight-bus verify --snapshot ./snapshot --nodes-csv ./nodes.csv --edges-csv ./edges.csv
   ```

6. The binary checks parity between CSV-derived answers and snapshot-derived answers across the verification corpus.
7. The binary prints:
   - snapshot path
   - node count
   - edge count
   - snapshot size
   - verification status

The user should feel:

> the binary did not just build something fast, it built something faithful.

### Journey 03: Reopen Snapshot And Query From CLI

The third journey is the first interactive runtime proof.

1. The user already has a compiled snapshot.
2. The user runs:

   ```bash
   knight-bus query --snapshot ./snapshot --entity <key> --dir forward --hops 1 --format json
   ```

3. The binary opens the snapshot and validates it before serving the query.
4. The binary resolves the exact key to a dense ID.
5. The runtime answers from the compiled forward or reverse adjacency planes only.
6. The binary prints bounded query results in text or JSON form.

The user should leave this journey with one clear mental model:

> once the snapshot exists, the runtime walks the snapshot rather than rescanning CSV rows.

### Journey 04: Benchmark Tiny And Medium CSV Tiers

The fourth journey should create confidence before the large run.

1. The user prepares a tiny CSV pair and a medium CSV pair.
2. The user builds snapshots for both.
3. The user runs:

   ```bash
   knight-bus bench --snapshot ./snapshot --report ./reports
   ```

4. The binary emits:
   - query latency
   - snapshot size
   - peak RSS
   - benchmark metadata

5. The user compares:
   - truth-layer parity
   - query behavior
   - latency shape
   - memory shape

The product should feel like:

> the same proof method keeps working as the graph gets larger.

### Journey 05: Run Large CSV Tier

The fifth journey is the milestone proof.

1. The user points the same pipeline at a large `nodes.csv` and `edges.csv` pair.
2. The user builds the snapshot using the same command family and contract as the smaller tiers.
3. The user verifies parity on a bounded verification corpus.
4. The user runs the benchmark suite.
5. The system records:
   - snapshot size
   - peak RSS
   - one-hop latency
   - two-hop latency

The success story is:

> the compiled snapshot can exceed `20 GiB`, but the process does not require RSS proportional to the full artifact during hot query runs.

## 2. Product Thesis And Non-Goals

### What v001 Must Be

- Rust-first
- CLI-first
- CSV-truth-first
- snapshot-runtime proof
- small enough to explain in a few minutes
- structured enough to hand to another engineer or agent without hidden decisions

### What v001 Must Not Be

- not an HTTP service in this version
- not a graph database
- not a query language project
- not a Wasm-first demo
- not a synthetic generator product yet
- not a dataset import framework yet
- not a visualization product yet

### Product Promise

`v001` succeeds if it can honestly answer:

- can CSV truth be compiled into a persisted walk snapshot?
- does snapshot lookup match truth exactly?
- do one-hop and two-hop queries run from the snapshot rather than from rescans?
- can the same pipeline scale to a large snapshot milestone?

## 3. Truth Layer And Runtime Split

### v001 Should Use Two Layers, Not One Confused Graph

`v001` should preserve the earlier proven method:

1. truth layer first
2. runtime layer second
3. parity verification third
4. benchmark evidence fourth

### Layer 01: CSV Truth Layer

The truth layer is:

- `nodes.csv`
- `edges.csv`

Its job is:

- readable source-of-truth input
- parity oracle
- debugging aid
- benchmark comparison baseline

It is **not**:

- the hot query path
- the final runtime representation

### Layer 02: Walk Snapshot Runtime

The runtime layer is the compiled snapshot:

- `manifest.json`
- `node_table.bin`
- `strings.bin`
- `forward.offsets.bin`
- `forward.peers.bin`
- `reverse.offsets.bin`
- `reverse.peers.bin`
- `key_index.bin`

Its job is:

- open quickly
- validate clearly
- answer hop queries from precomputed adjacency
- support benchmark measurement

### Runtime Rules

- CSV is truth, not hot path
- query flow runs only against the compiled snapshot
- reverse traversal never derives reverse edges on demand
- heap use attaches to frontier and visited state, not full graph reconstruction
- the CLI query command is the only `v001` interactive surface

## 4. CLI Contract

### Required Commands

`v001` shall expose these commands:

```bash
knight-bus build --nodes-csv <path> --edges-csv <path> --output <dir>
knight-bus verify --snapshot <dir> --nodes-csv <path> --edges-csv <path>
knight-bus query --snapshot <dir> --entity <key> --dir forward|backward --hops <n> --format json|text
knight-bus bench --snapshot <dir> --report <dir>
```

### Query Output Expectations

Text mode should be readable in a terminal.

JSON mode should be stable enough for tooling:

```json
{
  "entity": "fn:login_user_flow_now",
  "dense_id": 17,
  "direction": "forward",
  "hops": 2,
  "neighbors": [
    "fn:check_password_match_now",
    "fn:issue_login_token_now",
    "fn:save_session_record_now"
  ]
}
```

### Benchmark Output Expectations

The benchmark command should emit:

- machine-readable report file
- human-readable summary table
- snapshot size
- peak RSS
- one-hop latency
- two-hop latency

## 5. Rust Work Mode

- `Spec Mode`
- `Delivery Mode`
- `Reliability Mode`

Component type:

- Rust CLI binary

## 6. Executable Requirements

### REQ-RUST-001.0: CSV Truth Inputs

**WHEN** `v001` runs  
**THEN** the system SHALL accept `nodes.csv` and `edges.csv` as required input artifacts  
**AND** SHALL reject missing or malformed CSV inputs before snapshot build begins  
**SHALL** return typed, actionable errors for missing files, unreadable files, or invalid headers

### REQ-RUST-002.0: Truth-To-Snapshot Compilation

**WHEN** valid CSV truth inputs are provided  
**THEN** the system SHALL compile them into a persisted dual-adjacency snapshot  
**AND** SHALL materialize the snapshot into the locked runtime directory contract  
**SHALL** emit node count, edge count, and snapshot size on successful build

### REQ-RUST-003.0: CSV Parity Verification

**WHEN** a snapshot is compiled from CSV truth  
**THEN** forward and backward query results SHALL match the CSV truth layer exactly on the verification corpus  
**AND** SHALL compare canonicalized neighbor sets rather than incidental order where order is not contractual  
**SHALL** fail verification clearly if any snapshot result diverges from CSV truth

### REQ-RUST-004.0: Snapshot Open Discipline

**WHEN** the runtime opens a snapshot  
**THEN** it SHALL validate manifest version, file sizes, counts, and offset monotonicity before serving queries  
**AND** SHALL refuse to serve queries from corrupt or incomplete snapshots  
**SHALL** avoid rebuilding adjacency during the open path

### REQ-RUST-005.0: Dual Adjacency Query Path

**WHEN** a query requests forward or backward traversal  
**THEN** the runtime SHALL answer from precomputed forward or reverse adjacency without rescanning CSV data  
**AND** SHALL treat reverse traversal as a first-class compiled plane  
**SHALL** keep the hot query path independent from truth-layer CSV file access

### REQ-RUST-006.0: CLI Binary Contract

**WHEN** the user runs the binary  
**THEN** it SHALL expose subcommands for `build`, `verify`, `query`, and `bench`  
**AND** SHALL provide `--help` output for each subcommand  
**SHALL** return non-zero exit status for invalid usage or failed execution

### REQ-RUST-007.0: CLI Query Contract

**WHEN** the user invokes the query command with a valid entity, direction, and hop count  
**THEN** the binary SHALL print bounded, machine-readable query results from the snapshot  
**AND** SHALL support both `json` and `text` output modes  
**SHALL** reject unsupported direction values or invalid hop counts before query execution

### REQ-RUST-008.0: Tiered Proof Ladder

**WHEN** `v001` is delivered  
**THEN** it SHALL define three CSV-backed proof tiers: tiny fixture tier, medium CSV tier, and large CSV tier  
**AND** SHALL use the same truth-to-snapshot pipeline for all three tiers  
**SHALL** target a `>= 20 GiB` compiled snapshot artifact for the large tier

### REQ-RUST-009.0: Measured Working-Set Claim

**WHEN** the large-tier benchmark runs  
**THEN** the benchmark SHALL record peak RSS  
**AND** SHALL attach the claim to the compiled snapshot artifact, not to the raw CSV size  
**SHALL** emit the measured snapshot size alongside memory and latency outputs

### REQ-RUST-010.0: Reference Harness Continuity

**WHEN** `v001` is implemented  
**THEN** it SHALL preserve the earlier proven method: CSV truth layer first, snapshot compilation second, parity verification third, benchmark comparison fourth  
**AND** SHALL use `parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001` as the visible reference point  
**SHALL** avoid inventing a conflicting proof shape in `v001`

### REQ-RUST-011.0: Deferred HTTP Surface

**WHEN** `v001` is documented  
**THEN** it SHALL explicitly defer HTTP serving to `v002`  
**AND** SHALL keep the `v001` interactive surface CLI-only  
**SHALL** avoid embedding HTTP acceptance criteria into `v001`

## 7. Rust Design (L1/L2/L3 + Traits)

### L1 Core

Keep this dependency-light and deterministic.

- graph identity and record types
- parsed node and edge row models
- manifest model
- node table model
- adjacency invariants
- one-hop and two-hop query semantics
- parity verification logic
- benchmark scenario definitions

### L2 Std

Use std-backed orchestration for file and process flow.

- CSV file path validation
- snapshot directory layout orchestration
- benchmark report writing
- memory metric capture hooks
- CLI command coordination

### L3 External

Adapters only.

- `csv`
- `serde`
- `serde_json`
- `memmap2`
- `clap`
- `thiserror`

### Trait Seams

`TruthGraphSource`

- loads node and edge truth rows from CSV

`SnapshotArtifactWriter`

- writes the compiled runtime artifact

`WalkQueryRuntime`

- answers forward, backward, and multi-hop queries by dense ID

`EntityKeyResolver`

- resolves exact entity keys to dense IDs

`BenchmarkScenarioRunner`

- runs tiered scenarios and emits reports

### Snapshot Contract

Lock this file contract:

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

### Reference Harness Alignment

The `v001` design should align to the earlier visible harness behavior:

- truth rows load from CSV first
- snapshot files are written second
- parity is checked between CSV and snapshot
- benchmarks attach to the runtime layer, not the truth layer

## 8. Verification Matrix

| req_id | test_id | test_type | assertion | metric |
| --- | --- | --- | --- | --- |
| `REQ-RUST-001.0` | `TEST-V001-001` | integration | malformed or missing CSV inputs fail early | typed error |
| `REQ-RUST-002.0` | `TEST-V001-002` | integration | valid CSV truth compiles into snapshot | artifact exists |
| `REQ-RUST-003.0` | `TEST-V001-003` | integration | CSV and snapshot forward/backward parity match | exact set equality |
| `REQ-RUST-004.0` | `TEST-V001-004` | unit | corrupt manifest or offsets fail validation | typed error |
| `REQ-RUST-005.0` | `TEST-V001-005` | integration | runtime answers from dual adjacency only | no CSV access in hot path |
| `REQ-RUST-006.0` | `TEST-V001-006` | CLI integration | all required subcommands parse and run | exit status |
| `REQ-RUST-007.0` | `TEST-V001-007` | CLI integration | query output is stable and bounded | schema + count |
| `REQ-RUST-008.0` | `TEST-V001-008` | benchmark | tiny, medium, and large CSV tiers share one pipeline | stage completion |
| `REQ-RUST-009.0` | `TEST-V001-009` | benchmark | large-tier run records peak RSS and snapshot size | report fields present |
| `REQ-RUST-010.0` | `TEST-V001-010` | review | `v001` follows prior harness proof shape | checklist pass |
| `REQ-RUST-011.0` | `TEST-V001-011` | doc review | HTTP is deferred explicitly | deferred section present |

## 9. Implementation Plan

1. Write `docs/v001-PRD.md` in the same style family as `v313 PRD L2`.
2. Anchor the document to the known earlier method.
3. Lock `v001` scope to CSV-available inputs.
4. Define the staged ladder without changing the source contract.
5. Keep the interaction story CLI-only.
6. Put future work in deferred surface.

### STUB

- define the CLI surface
- define the file contract
- define the tier model
- define the verification matrix

### RED

- write tests for malformed CSV, snapshot corruption, parity divergence, and invalid query arguments

### GREEN

- implement CSV loading
- implement snapshot compilation
- implement parity verification
- implement CLI query
- implement benchmark reporting

### REFACTOR

- tighten L1 invariants
- move IO adapters behind seams
- simplify error surfaces and output formatting

### VERIFY

- run formatting, lints, tests, build, and benchmark report generation

## 10. Quality Gates

The implementation should require:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo build --all-targets --all-features`

And `v001` completion should require:

- unit tests pass
- integration parity tests pass
- tiny-tier benchmark report emitted
- medium-tier benchmark report emitted
- large-tier benchmark report emitted when large CSV inputs are available
- any unmeasured performance claim is marked pending rather than stated as fact

## 11. Deferred Surface

These items are explicitly out of scope for `v001` and belong to later versions:

- HTTP serve mode
- synthetic generation
- official dataset import
- direct streaming build
- Wasm packaging
- fuzzy search
- richer visualization

## 12. Assumptions And Defaults

- `v001-PRD.md` lives under `docs/`
- this is one combined PRD and Rust executable-spec document
- `nodes.csv` and `edges.csv` are assumed to be available in all `v001` journeys
- `v001` is CLI-only
- the hot-path proof attaches to the compiled snapshot artifact, not to CSV scanning
- the prior harness in `parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001` is the primary reference point for structure and proof style
- HTTP is explicitly out of scope for `v001` and belongs to `v002`

## Closing Position

`v001` should stay narrow and honest.

It should prove that:

- the truth layer can remain readable and CSV-shaped
- the runtime layer can remain compact and walk-shaped
- parity can remain explicit
- the proof can scale without changing its core method

That is a stronger first document than a broader but less trustworthy architecture promise.

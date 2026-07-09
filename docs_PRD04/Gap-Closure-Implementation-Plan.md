# Gap Closure Implementation Plan

Date: 2026-06-24

This file implements `Gap-Closure-Executable-Spec.md` as an execution plan.

It tells the next agent exactly what to create, in what order, how to verify it,
and when to stop. It does not ask the agent to write production Rust code. The
deliverable is an implementation-readiness packet under:

```text
docs_PRD03/implementation-readiness/
```

## Operating Rule

The goal is to convert the current research shelf into buildable contracts.

Do not broaden research unless a required field cannot be filled from the
existing notes or local reference repos. Do not treat `ArtifactCovered` in the
learning tracker as product readiness. This plan exists because the critique
showed that the research shelf is strong, but the implementation contract is
still thin.

## Execution Order

| phase | purpose | primary requirements | primary output |
| --- | --- | --- | --- |
| `0` | Create the control folder and status vocabulary | `REQ-GAP-001.0`, `REQ-GAP-013.0`, `REQ-GAP-014.0` | readiness tracker, index, support status semantics |
| `1` | Convert GDS inventory into a registry contract | `REQ-GAP-002.0`, `REQ-GAP-003.0` | GDS procedure support registry |
| `2` | Make client compatibility testable | `REQ-GAP-004.0`, `REQ-GAP-015.0` | Neo4j compatibility canary matrix |
| `3` | Make publication and memory mechanically precise | `REQ-GAP-007.0`, `REQ-GAP-008.0` | snapshot state machine, memory formula book |
| `4` | Make storage/build-plane contracts concrete | `REQ-GAP-005.0`, `REQ-GAP-006.0` | OLTP contract, Projection Build Store contract |
| `5` | Resolve architecture pressure points | `REQ-GAP-009.0`, `REQ-GAP-010.0`, `REQ-GAP-011.0`, `REQ-GAP-012.0` | cells falsifier, artifact catalog, benchmark plan, evidence tiers |

## Phase 0: Control Packet

### Files To Create

| file | required sections | initial state |
| --- | --- | --- |
| `docs_PRD03/implementation-readiness/README.md` | purpose, artifact table, verification commands, next action | `Specified` |
| `docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv` | artifact, owning req, state, verification command, evidence path, next action | `Specified` |
| `docs_PRD03/implementation-readiness/Support-Status-Runtime-Semantics.md` | status table, runtime behavior, registry behavior, tests, promotion criteria | `Specified` |

### Tracker States

| state | meaning | allowed evidence |
| --- | --- | --- |
| `Missing` | required artifact does not exist | path absence |
| `Specified` | artifact exists with required headings and acceptance criteria | `rg` heading checks |
| `Stubbed` | artifact has rows or sections but known gaps remain | explicit `MissingEvidence` or `NeedsSource` rows |
| `Implemented` | artifact fields are filled but not fully verified | row-count and field checks pass |
| `Verified` | artifact checks pass and evidence is linked | command output recorded in README |
| `Rejected` | artifact is no longer needed because scope changed | PRD or spec change reference |

### Verification

```bash
test -d docs_PRD03/implementation-readiness
test -f docs_PRD03/implementation-readiness/README.md
test -f docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv
test -f docs_PRD03/implementation-readiness/Support-Status-Runtime-Semantics.md
awk -F '\t' 'NR>1 && $3 !~ /^(Missing|Specified|Stubbed|Implemented|Verified|Rejected)$/ {print}' docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv
```

Done criteria:

- The final command prints no rows.
- Every required artifact from `Gap-Closure-Executable-Spec.md` appears in the tracker.

## Phase 1: GDS Procedure Registry

### File To Create

```text
docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
```

### Required Columns

```text
inventory_row_id
procedure_name
surface_kind
family
mode
is_estimate
source_file
source_line
config_type
result_type
estimate_path
support_status
unsupported_error_code
unsupported_message_shape
result_schema_behavior
first_canary_or_test
evidence_confidence
notes
```

### Source Inputs

| source | use |
| --- | --- |
| `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` | row source and row count |
| `docs_PRD03/reference-learning/GDS-Procedure-To-Kernel-Ledger.tsv` | representative config, result, estimate, and kernel examples |
| `docs_PRD03/reference-learning/GDS-Family-Support-Tier-Matrix.tsv` | family-level support target |
| `gitrefrepo/neo4j-gds-src` | direct source verification |

### Verification

```bash
src_rows=$(awk 'END{print NR-1}' docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv)
reg_rows=$(awk 'END{print NR-1}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv)
test "$src_rows" = "$reg_rows"
awk -F '\t' 'NR>1 && ($9 == "MissingEvidence" || $10 == "MissingEvidence" || $12 == "MissingEvidence" || $16 == "MissingEvidence") {print NR ":" $0}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
```

Done criteria:

- Row count equals source inventory row count.
- Required columns contain zero `MissingEvidence`.
- Unsupported rows define deterministic user-visible behavior.

## Phase 2: Compatibility Canaries

### File To Create

```text
docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md
```

### Required Canary Groups

| group | minimum canaries |
| --- | --- |
| Bolt | handshake success, handshake rejection, auth metadata, session lifecycle |
| official drivers | Python, Java, JavaScript, Go, .NET basic session and transaction flow |
| Cypher procedure path | `CALL`, `YIELD`, invalid argument, missing procedure |
| values | null, bool, int, float, string, list, map, node, relationship, path, temporal, spatial |
| errors | unsupported procedure, invalid config, runtime rejection, auth/session failure |
| APOC boundary | known supported alias, known unsupported registered procedure, unknown procedure |
| tooling | cypher-shell, browser-like query, representative application client |

### Verification

```bash
rg -n "Bolt|Python|Java|JavaScript|Go|\\.NET|CALL|YIELD|APOC|cypher-shell|browser" docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md
```

Done criteria:

- Every group has at least one command shape, expected result, and failure classification.
- Every unsupported behavior references `Support-Status-Runtime-Semantics.md`.

## Phase 3: Publication And Memory

### Files To Create

| file | requirement |
| --- | --- |
| `Snapshot-Publication-State-Machine.md` | `REQ-GAP-007.0` |
| `Memory-Estimate-Formula-Book.tsv` | `REQ-GAP-008.0` |

### Snapshot State Machine Minimum

Required states:

```text
staged
validating
published
retired
failed
garbage
```

Required transitions:

```text
build_start -> staged
validation_start -> validating
publish_swap -> published
reader_floor_passed -> retired
retention_elapsed -> garbage
validation_error -> failed
rollback -> previous_published
```

Required proof:

```text
A reader that starts before publish_swap sees W.
A reader that starts after publish_swap sees W+1.
No reader opens half-built files.
```

### Memory Formula Book Minimum Columns

```text
surface
operation
graph_scale_assumption
heap_bytes
rss_page_cache_policy
direct_buffer_bytes
topology_bytes
sidecar_bytes
result_artifact_bytes
model_artifact_bytes
scratch_bytes
spill_bytes
retained_generation_bytes
algorithm_state_bytes
total_required_bytes
budget_bytes
decision
measurement_source
formula_notes
```

### Verification

```bash
rg -n "staged|validating|published|retired|failed|garbage|rollback|reader" docs_PRD03/implementation-readiness/Snapshot-Publication-State-Machine.md
awk -F '\t' 'NF != 19 {print NR ":" NF ":" $0}' docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv
rg -n "50GB|8GB|reject|pass|fail" docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv
```

Done criteria:

- State machine names all required states and transitions.
- Formula TSV has valid field count.
- At least one 50GB-on-8GB pass or fail scenario exists for each priority surface.

## Phase 4: Storage Contracts

### Files To Create

| file | requirement |
| --- | --- |
| `OLTP-Record-Store-Rust-Contract.md` | `REQ-GAP-005.0` |
| `Projection-Build-Store-Physical-Contract.md` | `REQ-GAP-006.0` |

### OLTP Contract Must Cover

```text
nodes
relationships
properties
schema tokens
indexes
WAL
locks
checkpoints
import
recovery
dense nodes
relationship groups
relationship chains
property blocks
dynamic records
constraints
```

### Build Store Contract Must Cover

```text
receipt schema
ordering
idempotency
replay
dense ids
dictionaries
sorted runs
metadata state
validation reports
retention
compaction thresholds
partial receipt append
partial metadata commit
failed snapshot build
failed publication handoff
```

### Verification

```bash
rg -n "nodes|relationships|properties|WAL|locks|checkpoints|dense nodes|relationship groups|property blocks|dynamic records" docs_PRD03/implementation-readiness/OLTP-Record-Store-Rust-Contract.md
rg -n "receipt|ordering|idempotency|replay|dense|dictionary|sorted|validation|retention|compaction|partial" docs_PRD03/implementation-readiness/Projection-Build-Store-Physical-Contract.md
```

Done criteria:

- Every invariant cites a local Neo4j source path, test path, or explicit `NeedsSource`.
- `NeedsSource` rows are allowed only while tracker state is `Stubbed`.

## Phase 5: Architecture Pressure Points

### Files To Create

| file | requirement |
| --- | --- |
| `Cells-Adoption-Falsifier-Plan.md` | `REQ-GAP-009.0` |
| `Artifact-Model-Catalog-Contract.md` | `REQ-GAP-010.0` |
| `Benchmark-Proof-Plan.md` | `REQ-GAP-011.0` |

### Required Decisions

| topic | decision needed |
| --- | --- |
| cells | exactly when to adopt, postpone, or reject cellular packaging |
| artifacts | how result/model/pipeline artifacts are named, retained, listed, and invalidated |
| benchmarks | how to compare Neo4j Cypher, Neo4j GDS, and Knight Bus v003 fairly |

### Verification

```bash
rg -n "adopt|postpone|reject|boundary-edge|dirty-region|page-cache|metadata overhead" docs_PRD03/implementation-readiness/Cells-Adoption-Falsifier-Plan.md
rg -n "user|database|graph|generation|procedure|config hash|watermark|model|pipeline|cleanup" docs_PRD03/implementation-readiness/Artifact-Model-Catalog-Contract.md
rg -n "Neo4j Cypher|Neo4j GDS|Knight Bus|cold|projection|publication|RSS|page cache|spill|correctness" docs_PRD03/implementation-readiness/Benchmark-Proof-Plan.md
```

Done criteria:

- Cells plan has explicit thresholds or `NeedsBenchmark` rows for each threshold.
- Artifact catalog covers stale and missing generation behavior.
- Benchmark plan includes all three required systems and all required phases.

## Cross-Cutting Evidence Rule

Every implementation-readiness artifact must include this table or an equivalent
TSV companion:

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |

Allowed confidence values:

```text
DirectSource
GraphToolAssisted
DocsOnly
Inference
Speculation
```

Architecture-critical claims may not rely only on `Speculation`.

## Final Verification Packet

Run these checks before calling the gap-closure spec implemented:

```bash
git diff --check -- docs_PRD03/implementation-readiness docs_PRD03/Gap-Closure-Implementation-Plan.md
test -f docs_PRD03/implementation-readiness/README.md
test -f docs_PRD03/implementation-readiness/V003-Implementation-Readiness-Tracker.tsv
test -f docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
test -f docs_PRD03/implementation-readiness/Support-Status-Runtime-Semantics.md
test -f docs_PRD03/implementation-readiness/Neo4j-Compatibility-Canary-Matrix.md
test -f docs_PRD03/implementation-readiness/OLTP-Record-Store-Rust-Contract.md
test -f docs_PRD03/implementation-readiness/Projection-Build-Store-Physical-Contract.md
test -f docs_PRD03/implementation-readiness/Snapshot-Publication-State-Machine.md
test -f docs_PRD03/implementation-readiness/Memory-Estimate-Formula-Book.tsv
test -f docs_PRD03/implementation-readiness/Cells-Adoption-Falsifier-Plan.md
test -f docs_PRD03/implementation-readiness/Artifact-Model-Catalog-Contract.md
test -f docs_PRD03/implementation-readiness/Benchmark-Proof-Plan.md
src_rows=$(awk 'END{print NR-1}' docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv)
reg_rows=$(awk 'END{print NR-1}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv)
test "$src_rows" = "$reg_rows"
awk -F '\t' 'NR>1 && /MissingEvidence/ {print NR ":" $0}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
```

The final command should print no rows for a fully verified registry. If it
prints rows, keep the readiness tracker below `Verified`.

## Handoff Prompt

Use this prompt for the next agent:

```text
You are implementing `docs_PRD03/Gap-Closure-Executable-Spec.md` using
`docs_PRD03/Gap-Closure-Implementation-Plan.md`.

Do not write production Rust code. Create and fill the
`docs_PRD03/implementation-readiness/` packet.

Start with Phase 0. Keep `V003-Implementation-Readiness-Tracker.tsv` current.
Every artifact must cite source paths or explicitly mark `NeedsSource`.
Do not mark an artifact `Verified` until the matching verification commands
pass. Preserve the PRD boundary: OLTP reads/writes use Neo4j-shaped OLTP
storage, OLAP/GDS reads use published snapshots, and Projection Build Store is
not a user query serving store.
```


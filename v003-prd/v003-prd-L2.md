# v003 PRD L2: Supporting Requirements

This is the second level of the Minto pyramid. It expands the L1 answer into
requirements that can be converted into tests and implementation tasks. It
intentionally avoids locking the project into one physical storage design;
formats may evolve as long as they preserve the L1 constraints.

## 1. Source basis and scope

These requirements are grounded in the local project docs, diligence notes, and
reference repositories already available in this repo tree.

| source | requirement implication |
| --- | --- |
| Current Knight Bus code/tests | Existing snapshots prove a low-RAM immutable graph-read direction for fixed traversal workloads, but not the full Neo4j/GDS surface. |
| `v003-diligence-01/diligence-codex-notes01.md` | Low RAM must mean holistic machine pressure, not only Rust heap. |
| `v003-diligence-01/tasks-diligence.md` | Requirements should become TDD phases: inventory, registry, build store, catalog, backend, sidecars, memory planner, algorithms, publication, operations. |
| `gitrefrepo/neo4j-src` | OLTP record/page-cache/WAL/storage concerns belong to the OLTP plane. |
| `gitrefrepo/neo4j-gds-src` | A credible GDS rewrite needs procedure ABI, graph catalog, projected graph semantics, compressed adjacency, properties, memory estimation, and modes. |
| `gitrefrepo/opencypher-*` and parser repos | Query-facing compatibility is separate from OLAP storage layout. |
| columnar/OLAP reference repos | Sidecars, streaming, batching, and spill are valid implementation patterns for analytical data. |
| graph algorithm reference repos | Algorithms require topology plus scratch/result/model state; topology alone is insufficient. |

## 2. Requirement levels

Every known Neo4j/GDS-compatible procedure must have exactly one support level.

| level | meaning |
| --- | --- |
| `P0Registered` | Known, classified, config-shaped, output-shaped, and returns deterministic unsupported behavior if not implemented. |
| `P1ExactLowRam` | Implemented exactly enough for the claimed surface and allowed to run only when its memory estimate fits. |
| `P2Later` | Intentionally deferred but registered and documented. |
| `UnsupportedButRegistered` | Known to exist in the reference surface but not supported in v003. |

Requirement:

```text
WHEN a user calls a known procedure
THEN the system must either execute it or return deterministic unsupported
behavior
AND must not confuse known-but-unsupported with unknown.
```

## 3. Storage-plane requirements

### REQ-L2-STORAGE-001: OLTP storage boundary

OLTP reads and writes must remain on Neo4j-shaped OLTP storage.

The OLTP plane owns:

```text
transactional source of truth
writes
locks
rollback
WAL or receipt ordering
point reads
indexes required for OLTP semantics
Cypher-facing transactional correctness
```

OLAP snapshot storage must not become the OLTP store.

### REQ-L2-STORAGE-002: OLAP snapshot boundary

OLAP/GDS reads must run on published OLAP snapshot generations.

Each OLAP query result must report:

```text
snapshot generation id
source watermark
projection/catalog identity
physical plan
memory estimate or budget decision
```

A query may observe generation `W` or generation `W+1`, but never partially
published files.

### REQ-L2-STORAGE-003: Projection Build Store boundary

The Projection Build Store is required as a build/control-plane store.

Accepted names:

```text
Projection Build Store
Analytical Projection IR Store
```

Compiler analogy:

```text
OLTP records       = source code
Projection Store   = intermediate representation
OLAP snapshot      = optimized machine code
OLAP runtime       = CPU executing machine code
```

It may store:

```text
/manifests/
  source_watermark
  schema_version
  dictionary_version
  fact_counts
  checksums

/facts/
  nodes
  relationships
  labels
  relationship_types
  node_properties
  relationship_properties
  deletes_or_validity_ranges

/dictionaries/
  external_node_id -> dense_node_id
  label_name -> label_id
  rel_type_name -> rel_type_id
  property_key -> property_id

/statistics/
  node_count
  rel_count
  degree_histograms
  label_histograms
  reltype_histograms
  property_widths
  null_counts
  min/max

/build_runs/
  sorted_edge_runs
  partition_candidates
  validation_reports
  memory_estimates
```

It must not serve:

```text
OLTP queries
OLAP traversals
GDS algorithm reads
fresher-than-snapshot query views
```

It is also explicitly not:

```text
OLTP source of truth
OLAP query engine
freshness overlay
LSM serving layer
second database users query directly
```

Required middle-layer capabilities:

| capability | what it does | why it helps |
| --- | --- | --- |
| Semantic normalization | Converts Neo4j records/receipts into node, relationship, label, type, and property facts. | Snapshot compilers do not need to understand OLTP internals. |
| Watermark ledger | Tracks the source generation represented by analytical facts. | Every published snapshot can report exact-as-of semantics. |
| Dense-ID factory | Maintains stable external-id to dense-id mappings. | Snapshot arrays stay compact and reproducible. |
| Dictionary factory | Builds label, relationship-type, and property dictionaries. | Sidecars and snapshots share compact IDs. |
| Sort staging | Pre-sorts edges/properties by source, target, type, or partition. | Snapshot builds become sequential and lower-RAM. |
| Dedup/coalescing | Resolves repeated facts and validity before compilation. | Avoids query-time reconciliation. |
| Compiler cache | Stores intermediate sorted runs and checkpoints. | Failed builds can resume and use less peak RAM. |
| Multi-target source | Feeds topology snapshots, sidecars, result stores, and catalog manifests. | One verified fact store can produce many read formats. |
| Validation oracle | Compares counts, checksums, labels, types, properties, and dictionaries. | Prevents corrupt snapshots from publishing. |
| Memory planner input | Stores histograms, cardinalities, degree distributions, widths, and null counts. | Planners can estimate before execution. |
| Partition lab | Evaluates candidate partitions before writing partitioned snapshots. | Partitioning becomes measured, not theoretical. |
| Sidecar builder | Produces label/type/weight/property/feature/result/model sidecars. | GDS surface can expand without changing topology. |
| Compatibility bridge | Preserves projection and catalog metadata shape. | Neo4j/GDS product semantics remain visible. |
| Reproducibility ledger | Rebuilds the same snapshot from a watermark or explains why not. | Bugs become falsifiable. |
| Publication gate | Publishes only snapshots that pass validation. | OLAP reads never see half-built state. |
| Offline optimizer | Tries compression, ordering, partitioning, and sidecar layout experiments. | Future snapshots improve without touching OLTP reads. |
| Disaster recovery aid | Rebuilds disposable OLAP snapshots from durable facts. | Snapshot storage can be recovered after crash. |
| Build scheduler input | Supplies dirty size, elapsed build time, and RAM estimates. | Freshness comes from publication cadence, not query merge. |

### REQ-L2-STORAGE-004: Snapshot publication

Snapshot publication must be atomic.

Publication flow:

```text
1. Read Projection Build Store at watermark W.
2. Validate facts and dictionaries.
3. Build topology snapshot.
4. Build sidecars.
5. Optionally build partitioned/cellular packages from the same W.
6. Run parity, count, checksum, schema, and memory checks.
7. Publish generation N atomically.
8. OLAP queries read only generation N.
```

Publication must include:

```text
source watermark
build-store generation
snapshot generation id
topology manifest
sidecar manifests
dictionary versions
counts and checksums
validation report
catalog publish marker
```

If publication fails, existing readers must keep seeing the previous valid
snapshot generation.

## 4. Catalog and API requirements

### REQ-L2-API-001: Procedure registry

The procedure registry must classify every known procedure by:

```text
name
family
mode
alpha/beta/deprecated status
config schema
output schema
support level
estimate behavior
error behavior
```

### REQ-L2-API-002: Graph catalog

Named graph projections must be scoped by:

```text
user identity
database identity
graph name
projection generation
published snapshot watermark
```

Catalog metadata must expose:

```text
node count
relationship count
schema
topology backend or snapshot layout
property sidecars
result sidecars
model/pipeline links
memory estimate
creation/modification timestamps
```

### REQ-L2-API-003: Procedure modes

For every supported procedure family, behavior must distinguish:

```text
stream
stats
mutate
write
estimate
```

`mutate` writes projected result sidecars. `write` uses an OLTP-facing writeback
bridge. `estimate` must not execute mutation or writeback.

## 5. Data-model requirements

### REQ-L2-DATA-001: ID mapping

The OLAP plane must use dense internal IDs for compact arrays while preserving
external Neo4j-compatible identifiers at the API boundary.

Required mappings:

```text
external node id -> dense node id
dense node id -> external node id
relationship id -> edge position or sidecar row
label name -> label id
relationship type name -> type id
property key -> property id
```

### REQ-L2-DATA-002: Labels, relationship types, and orientation

The OLAP plane must support:

```text
multiple labels per node
relationship type filters
direction/orientation semantics
relationship aggregation where the procedure requires it
```

### REQ-L2-DATA-003: Properties and defaults

Columnar sidecars must represent:

```text
node scalar properties
relationship scalar properties
weights
feature vectors
embedding outputs
result properties
default values
null/missing values
coercion rules
schema reporting
```

Topology and properties must be separable so property changes do not require a
new topology format by default.

## 6. Execution and memory requirements

### REQ-L2-EXEC-001: Physical plan explanation

Every procedure must be able to explain:

```text
selected snapshot generation
selected topology layout
selected sidecars
scratch/spill strategy
expected output/result storage
memory estimate
rejection reason when applicable
```

### REQ-L2-EXEC-002: Bounded execution

Execution must account for:

```text
heap bytes
RSS budget bytes
page-cache expected bytes
page-cache unbounded risk
direct I/O buffer bytes
topology bytes
property sidecar bytes
algorithm state bytes
scratch bytes
result sidecar bytes
model artifact bytes
writeback bytes
spill bytes
snapshot generation bytes
snapshot build scratch bytes
```

The acceptance target is not that every algorithm runs on 8 GB. The target is
that every algorithm honestly says whether it can run, why, and under which
snapshot and physical plan.

### REQ-L2-EXEC-003: Strict-RAM mode

If a user selects a strict memory budget, the planner must either choose an
explicitly accounted streaming/spill plan or reject before execution.

`mmap` may be used for throughput, but a plan must not claim deterministic RAM
merely because files are memory-mapped.

### REQ-L2-EXEC-004: Progress, cancellation, and cleanup

Long-running procedures must report progress, support cancellation, release
scratch, and leave catalog/snapshot state deterministic after failure.

## 7. Freshness requirements

### REQ-L2-FRESH-001: Snapshot-as-of semantics

OLAP queries must be exact as of their published snapshot watermark.

```text
If OLTP has advanced beyond snapshot W, OLAP lag is reported.
If fresher OLAP is required, publish snapshot W+1.
```

### REQ-L2-FRESH-002: Build-store watermark is not query freshness

Projection Build Store watermark and OLAP query watermark are different.

```text
Projection Build Store watermark = how far the build/control plane has ingested.
Published snapshot watermark     = what OLAP users can query.
```

If the Projection Build Store is ahead of the published snapshot, the difference
is publication lag, not query visibility.

### REQ-L2-FRESH-003: No query-time write reconciliation

v003 must not require user OLAP queries to reconcile newer writes at query time.

Forbidden serving behavior:

```text
snapshot W + post-W writes merged into the same query answer
Build Store facts read directly by GDS procedures
OLTP records consulted to patch an OLAP algorithm mid-query
```

## 8. GDS surface requirements

The GDS-compatible surface must cover these families as inventory, even when
some are deferred:

| family | requirement |
| --- | --- |
| Catalog | create/list/exists/drop/schema/size/sample/export lifecycle. |
| Centrality | degree and PageRank first; register other centralities with estimates/support levels. |
| Pathfinding | BFS/DFS first; weighted paths require weight sidecar contracts. |
| Community/structure | WCC/SCC/triangle/k-core first; larger algorithms require explicit scratch estimates. |
| Similarity | top-k/candidate controls must prevent all-pairs heap explosions. |
| Embeddings | output dimensions and training scratch must be estimated. |
| ML/pipelines | feature schema, splits, models, metrics, catalog lifecycle, and artifact bytes must be explicit. |
| Operations/sysinfo | memory, progress, version, features, debug, cancellation, and telemetry must not depend on topology internals. |

## 9. Test-harness requirements

Every major PRD claim needs a falsifying harness.

| claim | falsifying harness |
| --- | --- |
| Same API/surface area | Procedure inventory and registry tests. |
| OLTP remains Neo4j-shaped | Neo4j transaction/WAL/page-cache/query-boundary tests. |
| OLAP reads snapshots only | Snapshot-open tests with the Projection Build Store unavailable. |
| Build Store before snapshots | Publish tests that compare facts, counts, dense IDs, checksums, and watermarks. |
| Properties are sidecars | Label/type/property/weight fixtures. |
| Memory is honest | Negative budget tests and strict-RAM rejection tests. |
| Snapshot publication is atomic | Crash/restart tests around manifest/catalog publish markers. |
| Algorithms are correct | Knight Bus parity tests plus GDS-style graph fixtures. |
| Cypher compatibility is separate | openCypher-style tests at the query-facing boundary, not CSR tests. |

## 10. Rubber-duck validation

For every requirement, ask:

```text
What bug would this catch?
Which fixture can falsify it?
Which memory plane could it hide?
Which boundary could it blur: OLTP, Build Store, or OLAP snapshot?
What deterministic error should occur if it is unsupported?
```

Middle-layer validation questions:

```text
If OLAP never reads the Projection Build Store, why have it?
  Because the hard part is reliably manufacturing correct, compact, low-RAM,
  GDS-compatible snapshots from Neo4j-shaped truth.

What exact bug should the middle layer catch?
  snapshot missing a relationship type
  property default applied incorrectly
  deleted relationship still present in snapshot topology
  label dictionary mismatch
  dense-id instability
  PageRank estimate missing sidecar/result memory
  snapshot claims tx 5000 but facts only verify through tx 4992
  partitioned package differs from global topology stream

What should not happen?
  OLAP query reads OLTP records.
  OLAP query reads the Projection Build Store.
  OLAP query merges fresh writes at query time.
  OLAP query hides the snapshot watermark it used.
```

Key red-team answers:

| challenge | answer |
| --- | --- |
| Does a synced Projection Build Store mean users see fresh OLAP? | No. Users see only the published snapshot watermark. |
| Does CSR prove GDS compatibility? | No. GDS compatibility also needs catalog, properties, modes, estimates, results, models, and operations. |
| Does low heap mean low RAM? | No. RSS, page cache, mmap residency, scratch, sidecars, result/model bytes, and spill all count. |
| Does strict RAM allow mmap-only reasoning? | No. mmap residency is OS-mediated; strict plans need explicit accounting or rejection. |
| Can unsupported procedures be invisible? | No. Known procedures must be registered with deterministic unsupported behavior. |

## 11. L2 acceptance criteria

| item | requirement |
| --- | --- |
| Inventory | Every scanned procedure row is known and classified. |
| API | Registry validates modes, config schema, output schema, support level, and estimate behavior. |
| Catalog | Projections are scoped by user/database/name/generation and expose published snapshot watermarks. |
| Build Store | Facts, dense IDs, dictionaries, watermarks, statistics, and validation metadata are durable and not on the serving path. |
| Snapshot | Published generations are atomic, restartable, versioned, and checksum-validated. |
| Properties | Labels, types, weights, scalar/vector properties, graph properties, and result sidecars are columnar. |
| Memory | Every estimate includes all relevant memory planes and rejects over-budget plans. |
| Freshness | OLAP reads only published snapshots; newer freshness requires newer publication. |
| Algorithms | First-tier graph procedures pass correctness and memory-budget tests. |
| Operations | Progress, cancellation, cleanup, telemetry, restart recovery, and manifest version checks exist. |

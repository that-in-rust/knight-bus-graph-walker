# v003 PRD L1: Neo4j-Compatible Rust Rewrite

This is the top of the Minto pyramid. It states the durable product constraints
and the answer first. It deliberately does not freeze any future physical
storage design, because implementation options may change as evidence improves.

Detailed supporting requirements live in:

```text
v003-prd/v003-prd-L2.md
```

## 1. Answer first

Knight Bus v003 is a Rust rewrite target for Neo4j-compatible usage where:

```text
OLTP queries and writes run on Neo4j-shaped OLTP storage.
OLAP/GDS queries run only on published OLAP-optimized snapshot storage.
A middle Projection Build Store may exist, and is useful, but it is a
build/control-plane store, not a user-query serving store.
```

The core data-flow constraint is:

```text
OLTP read/write path:
  client -> Neo4j-compatible API -> Neo4j-shaped OLTP storage

OLAP read path:
  client -> GDS/OLAP-compatible API -> published OLAP snapshot W

Build/control path:
  Neo4j-shaped OLTP storage -> Projection Build Store -> OLAP snapshot W+1
```

## 2. Situation / complication / resolution

### Situation

The product goal is:

```text
Neo4j rewritten in Rust with the same user-facing API/surface area where
support is claimed, while making analytical graph workloads practical on
small single-node machines.
```

The concrete RAM target is:

```text
50 GB-class logical graph data should be processable on 8 GB-class machines
when the selected OLAP plan honestly fits that budget.
```

### Complication

A graph database rewrite has two different storage problems:

```text
OLTP storage problem:
  preserve Neo4j-shaped transactional correctness, query semantics, writes,
  locks, rollback, indexes, and point reads.

OLAP storage problem:
  provide low-RAM analytical reads, graph algorithms, cataloged projections,
  properties, estimates, and snapshot watermarks.
```

Confusing these two problems creates bad designs:

```text
Do not run OLTP queries from OLAP snapshots.
Do not run OLAP queries from OLTP record storage when a low-RAM snapshot exists.
Do not make the middle build store a third user-visible serving database.
Do not merge newer writes into OLAP queries at query time.
```

### Resolution

Separate the planes:

| plane | role | user reads? |
| --- | --- | --- |
| OLTP storage | Neo4j-shaped source of truth for transactional queries and writes | yes, for OLTP |
| Projection Build Store | normalized analytical build/control plane used to manufacture snapshots | no |
| OLAP snapshot storage | immutable low-RAM read format for OLAP/GDS procedures | yes, for OLAP |

## 3. Non-negotiable PRD constraints

| constraint | requirement |
| --- | --- |
| API compatibility | Existing Neo4j/GDS-facing application code should require zero changes where v003 claims support. |
| OLTP boundary | OLTP reads/writes stay on Neo4j-shaped OLTP storage. |
| OLAP boundary | OLAP/GDS reads open only published OLAP snapshot generations. |
| Middle-layer boundary | Projection Build Store is allowed and useful only as build/control storage. |
| Freshness | OLAP lag versus OLTP is acceptable when the snapshot/source watermark is reported. |
| Freshness mechanism | Freshness improves by publishing newer snapshots, not by query-time reconciliation of newer writes. |
| RAM promise | Memory accounting is holistic: heap, RSS, page cache, mmap residency, direct buffers, duplicate layouts, build scratch, indexes, sidecars, result/model artifacts, spill, and algorithm state. |
| Strict RAM | Plans that claim strict RAM must use explicit accounting and reject before execution if the budget cannot fit. |
| Deployment | v003 community-edition target is single-node. |

## 4. What the middle layer is for

The Projection Build Store is best understood as an analytical compiler IR or
snapshot foundry.

Allowed responsibilities:

```text
normalize OLTP records/receipts into analytical facts
assign and verify dense node ids
build label, relationship-type, and property dictionaries
stage sorted node, relationship, label, type, and property runs
coalesce facts before snapshot compilation
record counts, histograms, checksums, and schema fingerprints
produce memory-planner statistics
feed snapshot, sidecar, catalog, result, and model compilers
gate publication with validation reports
support crash recovery and rebuild reproducibility
```

Forbidden responsibilities:

```text
serve OLTP queries
serve user OLAP traversals
serve GDS procedure reads directly
merge post-snapshot writes into a running OLAP query
act as the authoritative transactional source of truth
make its own watermark look like user-query freshness
```

## 5. What must be true for v003 to be credible

| area | L1 acceptance statement |
| --- | --- |
| OLTP | Neo4j-shaped OLTP storage remains the owner of transactional correctness. |
| OLAP | OLAP procedures read published immutable snapshots and report watermarks. |
| API surface | Every known procedure is either implemented or registered with deterministic unsupported behavior. |
| Catalog | Named projections are scoped and versioned by user/database/name/generation. |
| Properties | Labels, relationship types, weights, scalar/vector properties, results, and model artifacts are represented outside the topology primitive. |
| Memory | Every procedure can explain required memory and reject if the configured budget is too small. |
| Publication | Snapshot publication is atomic: a query sees old generation W or new generation W+1, never half-built files. |
| Testing | Claims are falsified by harnesses, not prose: procedure inventory, graph fixtures, memory estimates, publish/restart tests, and compatibility-boundary tests. |

## 6. Explicit non-goals

```text
Do not require zero-lag OLAP.
Do not make the Projection Build Store a serving read path.
Do not require any one future physical storage design in this L1 PRD.
Do not claim mmap gives deterministic RAM.
Do not equate "CSR exists" with "Neo4j/GDS surface is supported."
Do not materialize all result rows, candidate pairs, embeddings, or algorithm
state in heap unless the estimate explicitly permits it.
```

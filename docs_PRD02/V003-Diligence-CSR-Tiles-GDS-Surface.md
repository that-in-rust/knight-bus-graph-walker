# v003 Diligence: CSR Tiles + GDS Surface TDD Plan

> Purpose: turn the v003 OLAP architecture direction into a source-backed,
> test-first diligence plan.
>
> Short answer: the earlier draft is a strong scaffold, but it is not complete
> enough to become the implementation source of truth. It must be expanded with
> local source evidence, GDS procedure inventory gates, realistic v003 scope, and
> a stricter memory contract.

## Verdict

Build the diligence sequence around this thesis:

```text
Neo4j-shaped OLTP remains the source of truth.
Flat dual CSR remains the proven OLAP oracle and global stream primitive.
Cellular CSR Tilehouse is the update-aware evolution if freshness pressure returns.
GDS compatibility is an ABI/inventory problem before it is an algorithm problem.
```

### Update: If Freshness Can Lag By Hours

If OLAP freshness is not a hard requirement and a few hours of lag is acceptable,
the storage recommendation becomes sharper:

```text
Default v003 OLAP storage should stay immutable dual CSR plus mmap/streaming
sidecars.

Cellular CSR Tilehouse should move from "preferred default evolution" to
"optional later update-locality tier."
```

Reason:

| point | implication |
| --- | --- |
| Small OLTP updates do not need immediate OLAP visibility. | The main reason to pay Tilehouse complexity disappears from the RAM-first v003 path. |
| Immutable files are easiest to mmap, stream, checksum, compress, swap, and test. | They are the lowest-risk way to keep server RAM predictable. |
| Most GDS algorithms read a stable graph and write algorithm state elsewhere. | The hard memory problem is usually vectors, frontiers, heaps, candidates, embeddings, or output sidecars, not live adjacency mutation. |
| Full snapshot rebuilds can run every few hours under a build memory budget. | Rebuild/swap is operationally simpler than delta overlays and cell compaction. |

The revised thesis for this assumption is:

```text
Use one immutable CSR topology plane, one immutable/mmap property-sidecar plane,
and bounded runtime scratch/spill/result sidecars.

Do not build Tilehouse or WAL-fed deltas until freshness pressure is measured.
```

The proposed note gets the important intuition right:

| claim | verdict |
| --- | --- |
| CSR tiles alone do not prove GDS compatibility. | correct |
| Procedure inventory should come before tile partitioning. | correct |
| Flat CSR should be the oracle before Tilehouse is trusted. | correct |
| Memory estimates must be executable contracts. | correct |
| Cellular CSR is better for update locality than flat global CSR. | correct |

But it is incomplete in five important ways:

| gap | why it matters |
| --- | --- |
| It treats "full GDS surface" too casually. | The local GDS shelf exposes hundreds of `gds.*` procedure annotations, not a small PageRank/BFS set. |
| It needs scope levels. | v003 cannot honestly implement all algorithms, modes, pipelines, model catalog, and writeback in one first release. |
| It overstates `O_DIRECT + compio` as the preferred OLAP baseline. | Current Knight Bus is mmap-first, and prior local analysis says not to chase `compio` for the OLAP walker first. |
| It needs exact source references. | Implementation agents need line-anchored evidence, not remembered architecture claims. |
| It needs hard unsupported behavior. | A Neo4j-compatible surface must distinguish "known but unsupported" from "unknown procedure." |

## Premise Check

### Target

v003 is not "add a faster PageRank." The target is:

```text
Keep OLTP Neo4j-shaped.
Add a low-RAM OLAP plane.
Preserve the external Neo4j/GDS-style surface over time.
Make every storage, API, and memory claim testable before implementation.
```

### Non-Goals For This Diligence Step

This document does not propose:

| non-goal | reason |
| --- | --- |
| Replacing the OLTP record store immediately | The current decision frame keeps OLTP Neo4j-shaped. |
| Implementing all GDS algorithms in v003 | The GDS surface is too large; first pass must classify and register it. |
| Replacing the current mmap walker with `compio` | Local analysis says the hot OLAP walk path is dense-array and memory-bandwidth dominated. |
| Adding code in this pass | This is a research/TDD planning artifact, not implementation. |

### Lowest Holistic RAM

"Lowest RAM" means the total machine-visible cost, not just Rust heap:

```text
heap
RSS
OS page cache
mmap residency
direct I/O buffers
projection/catalog metadata
algorithm vectors/frontiers/heaps
delta overlays
compaction scratch
snapshot build scratch
indexes and sidecars
```

The business-visible target is still the same:

```text
Can a 50 GB-class graph be useful on an 8 GB-class single machine?
```

That does not mean every algorithm is feasible under 8 GB. It means the engine
must know, before execution, whether a chosen graph/procedure/configuration fits
the requested memory contract.

## Local Evidence Ledger

### Knight Bus Evidence

| fact | local evidence | implication |
| --- | --- | --- |
| v002 is intentionally immutable dual CSR plus mmap. | `README.md:9-17` says v002 keeps the Rust runtime in the immutable dual-CSR plus mmap shape. | The current implementation is a valid seed, not something to discard casually. |
| v002 showed lower runtime RSS than Neo4j on the measured datasets. | `README.md:29-41` reports lower RAM and faster traversal latency for `1 MB`, `50 MB`, and `2 GB`. | The existing mmap CSR path is already part of the moat. |
| Snapshot output is a small fixed file set. | `src/snapshot.rs:14-21` names `manifest.json`, `node_table.bin`, `strings.bin`, forward/reverse offsets, peers, and `key_index.bin`. | Tilehouse should evolve this shape, not explode it into per-algorithm layouts. |
| Snapshot writing is abstracted behind `SnapshotArtifactWriter`. | `src/snapshot.rs:23-29` defines `write_snapshot_artifacts`. | A Tilehouse writer can be additive without deleting the flat writer. |
| Current manifest declares version `2` and `immutable_dual_csr`. | `src/snapshot.rs:104-120` builds the manifest and sets `storage_mode` to `immutable_dual_csr`. | v3 should preserve v2 readability and add Tilehouse metadata intentionally. |
| The runtime is walk-focused. | `src/runtime.rs:22-39` exposes `WalkQueryRuntime` with neighbor queries, family queries, all keys, and snapshot size. | GDS algorithms need a separate algorithm-facing graph access layer. |
| The runtime maps the snapshot files with `memmap2::Mmap`. | `src/runtime.rs:41-53` stores forward/reverse offsets, peers, node table, strings, and key index as mmaps. | Current RAM behavior is low explicit heap but page-cache mediated. |
| Current graph normalization is topology-only. | `src/types.rs:265-280` has node keys plus forward/reverse offsets and peers. | Labels, relationship types, weights, and properties must be sidecars. |
| Current query families are only 1-hop/2-hop forward/backward. | `src/types.rs:145-183` defines `ForwardOne`, `BackwardOne`, `ForwardTwo`, and `BackwardTwo`. | Existing API proves walks, not GDS compatibility. |
| Current build memory budget exists. | `src/types.rs:419-456` defines `BuildMemoryBudget` and spill buffer sizing. | v003 should generalize this idea into projection and algorithm execution budgets. |
| Current tests are narrow. | `tests/cli.rs` and `tests/library_contract.rs` are the only top-level test files in this pass. | The v003 plan needs a larger compatibility and memory test ladder. |

### Neo4j GDS Evidence

| fact | local evidence | implication |
| --- | --- | --- |
| GDS has a facade larger than algorithms alone. | `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-44` exposes log, algorithms, graph catalog, model catalog, operations, pipelines, and deprecated procedure metrics. | A GDS-compatible surface includes catalog and operations behavior, not just kernels. |
| Algorithms are grouped into facade families. | `gitrefrepo/neo4j-gds-src/procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java:34-88` exposes centrality, community, machine learning, miscellaneous, embeddings, path finding, and similarity. | The implementation registry should classify by family. |
| The graph catalog is a large API surface. | `gitrefrepo/neo4j-gds-src/procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java:49-180` includes exists, drop, list, native/cypher project, estimates, filtering, size, property streams, topology streams, and writebacks. | Catalog semantics must come before algorithms. |
| PageRank stream has both execution and estimate procedures. | `gitrefrepo/neo4j-gds-src/proc/centrality/src/main/java/org/neo4j/gds/pagerank/PageRankStreamProc.java:41-57` defines `gds.pageRank.stream` and `gds.pageRank.stream.estimate`. | Each implemented procedure mode needs a paired memory estimate contract. |
| GDS has an explicit CSR graph type. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraph.java:26-35` describes a graph subtype exposing CSR-specific structures such as `AdjacencyList`. | CSR is not an exotic Knight Bus-only bet; it matches how GDS exposes projected graph storage internally. |
| GDS projections build a CSR graph store. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-86` builds graph-store state from nodes and imported relationships. | A durable immutable CSR projection is a fair low-RAM alternative to GDS's in-memory projection catalog. |
| GDS compressed adjacency stores pages, degrees, and offsets. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java:44-86` estimates compressed pages plus per-node degree and offset arrays; lines `101-110` store pages, degrees, and offsets. | A low-RAM Rust rewrite should study compression, but raw immutable CSR remains the simplest correctness baseline. |
| GDS adjacency access is cursor-oriented. | `gitrefrepo/neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java:33-104` exposes degree and adjacency cursor access. | Knight Bus algorithms can target cursor traits over immutable CSR without knowing the file layout. |
| GDS itself treats many algorithm states as memory-estimated per-node structures. | Examples: BFS estimates visited/result arrays in `BfsMemoryEstimateDefinition.java:32-69`; WCC estimates disjoint-set state in `WccMemoryEstimateDefinition.java:27-40`; KNN estimates per-node top-k and neighbor lists in `KnnMemoryEstimateDefinition.java:36-90`; FastRP estimates per-node embedding arrays in `FastRPMemoryEstimateDefinition.java:28-49`. | The limiting RAM is often algorithm state, not CSR topology. |
| Local GDS procedure surface is very large. | A read-only scan over non-test Java sources found `567` unique `gds.*` annotations and `575` annotation rows. | v003 must use support levels instead of pretending full implementation is immediate. |

### Procedure Scan Result

The local scan excluded Java paths containing `/src/test/`, `/integrationTest/`,
`/test-utils/`, and `/src/testFixtures/`.

| prefix | unique procedure count |
| --- | ---: |
| all `gds.*` annotations | 567 |
| `gds.graph.*` | 41 |
| `gds.pageRank*` | 8 |
| `gds.bfs*` | 6 |
| `gds.wcc*` | 8 |
| `gds.fastRP*` | 8 |
| `gds.beta.pipeline*` | 29 |
| `gds.model*` | 3 |

This is a coarse inventory count, not the final compatibility manifest. The
first implementation PR should check in a deterministic inventory generator or
checked TSV and test it.

### Existing Dirty Workspace Context

At the time this diligence doc was prepared, the working tree already had
uncommitted docs/reference-shelf housekeeping:

```text
M .gitignore
M docs/strategic-research/A-20260416121710-storage-runtime-alignment-eli5.md
M docs/strategic-research/A-20260525164835-faithful-rust-port-dossier.md
M docs_PRD02/Low-RAM-OLAP-Format-Variants.md
?? Docs-Readme.md
```

Implementation agents should stage this diligence file separately unless the
user explicitly asks to include the existing reference-shelf changes.

## Source, Inference, Speculation

| statement | type | confidence | evidence or reason |
| --- | --- | --- | --- |
| Current Knight Bus storage is flat immutable dual CSR with mmap runtime. | sourced fact | high | `README.md:13-17`, `src/snapshot.rs:14-21`, `src/runtime.rs:41-53` |
| Current Knight Bus does not yet have a GDS-compatible procedure/catalog layer. | sourced inference | high | Current public runtime is `WalkQueryRuntime`, and current tests are CLI/library walk contracts. |
| GDS compatibility is larger than algorithm kernels. | sourced fact | high | `GraphDataScienceProcedures.java:30-44`, `GraphCatalogProcedureFacade.java:49-180` |
| First v003 work should inventory/register procedures before implementing Tilehouse. | architecture inference | high | Prevents building storage that does not match the public surface. |
| Cellular CSR improves update locality and compaction locality over a single flat snapshot. | architecture inference | medium-high | Dirty cell and bounded compaction units reduce work for small localized updates. |
| Cellular CSR will reduce global algorithm RAM versus a flat explicit stream. | speculation | low | For full-graph algorithms, vectors/frontiers often dominate; tiles mostly improve planning and locality. |
| If hours of OLAP lag are acceptable, immutable CSR is the best default topology for lowest RAM. | architecture inference | high | It minimizes duplicate topology, avoids delta/compaction metadata, and supports mmap plus sequential streaming. |
| Immutable CSR is sufficient as the topology substrate for all listed GDS families. | architecture inference | medium-high | All families can be expressed as adjacency scans, neighbor cursors, edge streams, or property-column scans, with algorithm state outside topology. |
| Immutable CSR guarantees all GDS algorithms can fit on an 8 GB machine. | speculation rejected | high | Similarity, embeddings, all-pairs shortest paths, and some community algorithms are dominated by output/model/candidate state. |
| `compio` should not replace the OLAP mmap walker first. | sourced inference | high | `docs_PRD02/User-Journey-50GB-OLTP-OLAP-Lag.md:198-241`, `300-310`, `420-427` |
| `O_DIRECT` or explicit I/O should exist for strict-RAM global algorithms. | architecture inference | medium | It can avoid page-cache surprise, but adds complexity and platform constraints. |

## Corrected I/O Thesis

The pasted draft says:

```text
O_DIRECT + compio is preferred for deterministic RAM because it bypasses page
cache and makes buffer sizes explicit.
```

The corrected v003 stance should be:

```text
Keep mmap for the proven interactive/static CSR walk path.
Use explicit I/O, and possibly O_DIRECT, for strict-RAM global algorithms,
snapshot build, refresh, and compaction paths where page-cache residency must
not be counted as a surprise.
Borrow compio/Iggy ideas for segmented persistence and mutable-plane refresh
only after measurements show the refresh path is I/O-bound.
```

Reason:

| path | recommended I/O stance |
| --- | --- |
| 1-hop/2-hop static walks | mmap first |
| local immutable CSR reads | mmap CSR windows first |
| optional later Tilehouse cell reads | mmap or bounded mapped windows first |
| global PageRank strict-RAM mode | explicit stream or O_DIRECT candidate |
| snapshot build external sort | bounded buffered I/O first; O_DIRECT only if measured useful |
| WAL receipts and mutable delta logs | borrow segmented log ideas from Iggy; runtime choice is secondary |
| compaction | bounded buffered or explicit I/O; prove with RSS and page-cache metrics |

## Immutable CSR Suitability Under Hours-Lag Assumption

### Premise Check

If freshness can lag by a few hours, the main question is no longer:

```text
How do we make CSR update-local?
```

It becomes:

```text
Is one immutable CSR projection plus sidecars enough for the whole GDS-style
OLAP surface, and is it the lowest-RAM default?
```

Answer:

```text
Yes for topology.
No as a blanket guarantee for every algorithm fitting in 8 GB.
```

The careful claim is:

| claim | confidence | reason |
| --- | --- | --- |
| Immutable dual CSR is the best default topology for lowest RAM when freshness lag is allowed. | high | It has no live delta, tombstone, per-cell passport, boundary-index, or compaction overhead. |
| Every Phase 9 family can be planned against immutable CSR plus sidecars. | medium-high | The families need neighbor cursors, global edge streams, orientation views, property scans, or feature matrices. |
| Some families still require rejection/spill even with perfect CSR. | high | Algorithm state and outputs can dominate topology. |
| Tilehouse is still valuable if freshness/local compaction becomes a product need. | medium-high | Its main advantage is update locality, not lower global algorithm RAM. |

### Expert Lenses

| lens | judgment |
| --- | --- |
| Storage engineer | Immutable CSR is the simplest low-RAM durable projection: easy atomic swap, checksums, compression, and mmap/open behavior. |
| Graph algorithm engineer | CSR is the standard substrate for adjacency-heavy analytics, but algorithm scratch must be budgeted separately. |
| GDS compatibility engineer | CSR does not solve procedure names, configs, modes, result schemas, catalog behavior, or write/mutate semantics. |
| Operator | Multi-hour lag makes rebuild/swap much easier to explain than deltas and compaction. |
| Skeptical engineer | "CSR works for all algorithms" is true only as a topology statement; it is false if interpreted as "all algorithms fit cheaply." |

### Candidate Approaches

| approach | upside | downside | best use |
| --- | --- | --- | --- |
| Flat immutable dual CSR + sidecars | Lowest topology overhead, simplest build/swap, strongest mmap story | Rebuild needed for freshness; no local compaction | Default v003 RAM-first OLAP |
| Cellular CSR Tilehouse | Local updates, dirty-cell compaction, better regional locality | More metadata, boundary indexes, deltas, compaction rules | Later freshness/locality tier |
| Many per-algorithm layouts | Can speed hand-picked demos | Duplicates topology and destroys holistic RAM discipline | Avoid for default RAM-first mode |
| Graph-LSM / living CSR | Fresh analytics, research moat | Highest complexity and hidden compaction RAM | Only if lag becomes unacceptable |

Chosen approach under the hours-lag assumption:

```text
Build immutable CSR snapshots first.
Make the rebuild path low-RAM and atomic.
Add sidecars and spillable runtime state.
Postpone Tilehouse until measured freshness pressure justifies it.
```

### What Immutable CSR Must Include

The topology alone is not enough. The immutable CSR architecture needs these
planes:

| plane | required artifacts | why |
| --- | --- | --- |
| topology | forward CSR, reverse CSR, dense IDs, key index | Covers directed, reverse, undirected, traversal, PageRank, SCC, and edge scans. |
| graph catalog | manifest, generation, source tx range, projection config | Makes multi-hour freshness explicit and testable. |
| property sidecars | node/edge typed columns, null/default metadata, dictionaries | Supports weights, labels, features, partitions, and write/mutate results. |
| orientation views | natural, reverse, undirected, filtered relationship-type views | Avoids duplicating base topology for projection variants. |
| global edge streams | forward and reverse sequential edge cursors | Enables PageRank, Bellman-Ford, WCC scans, and strict-RAM execution. |
| sorted-neighbor option | per-type or global sorted adjacency where required | Needed for triangle/intersection-heavy algorithms without runtime full sort. |
| runtime scratch | bounded vectors, frontiers, heaps, candidate lists, spill tapes | Keeps algorithm state separate from durable topology. |
| result sidecars | immutable or generationed output properties/models | Supports mutate/write-like behavior without rewriting topology. |

### Can We Be Sure?

We can be sure of this narrower statement:

```text
Immutable CSR is a sufficient and likely lowest-RAM persistent topology
foundation for the broader GDS families when freshness lag is acceptable.
```

We cannot be sure of this broader statement:

```text
Immutable CSR makes every GDS algorithm practical on an 8 GB machine.
```

The falsifier is not usually adjacency layout. The falsifier is one of:

| falsifier | examples |
| --- | --- |
| O(V) state too large | PageRank vectors, WCC components, BFS parent arrays, SCC stacks |
| O(V * dimension) state too large | FastRP, Node2Vec, GraphSAGE, KGE, KMeans |
| O(V * topK) or candidate-pair state too large | KNN, nodeSimilarity |
| O(source_count * frontier) work too slow or too much spill | all-pairs shortest paths, betweenness, closeness |
| output is larger than the input | paths, walks, embeddings, dense pairwise similarities |

Therefore the confidence model should be:

| statement | answer |
| --- | --- |
| Is immutable CSR the right default persistent topology? | yes |
| Does every family have a path over immutable CSR? | yes, with sidecars and scratch |
| Does every exact algorithm fit lowest-RAM targets? | no |
| Should Tilehouse be default if lag is acceptable? | no |
| Should Tilehouse remain in the roadmap? | yes, but behind measured freshness/locality pressure |

## GDS As ABI

Treat GDS procedure compatibility like an ABI:

```text
procedure name
mode
config arguments
default values
result columns
estimate behavior
catalog side effects
write/mutate side effects
error class and message shape
```

The first scary failure is not a slow PageRank. The first scary failure is:

```text
The implementation silently narrows the GDS surface while claiming
Neo4j/GDS compatibility.
```

### Support Levels

Every inventoried procedure should be assigned one explicit level:

| level | meaning | user-visible behavior |
| --- | --- | --- |
| `P0 registered-compatible` | Procedure is known, parsed, categorized, and has deterministic unsupported behavior. | Calls do not look unknown; unsupported procedures return a stable compatibility error. |
| `P1 implemented exact low-RAM` | Procedure is implemented with exact semantics and memory estimates under the RAM-first architecture. | Procedure can run when its estimate fits the selected budget. |
| `P2 implemented later` | Procedure is known and planned, but not in the first implementation tranche. | Procedure remains registered as unsupported until promoted. |
| `UnsupportedButRegistered` | Procedure exists in GDS but is intentionally not implemented yet. | Deterministic error with procedure name, family, mode, and reason. |

### Procedure Inventory Artifact

First implementation artifact:

```text
v003-diligence-01/gds-procedure-inventory.tsv
```

Minimum columns:

```text
procedure_name
family
mode
estimate_name
source_file
source_line
config_args
result_type
stability
support_level
notes
```

Inventory tests should assert:

| acceptance test | why |
| --- | --- |
| Every row starts with `gds.`. | Avoid accidental helper/test procedure pollution. |
| `gds.pageRank.stream` and `gds.pageRank.stream.estimate` exist. | Representative algorithm + estimate pair. |
| `gds.graph.project`, list/drop/exists/size rows exist. | Catalog is a hard prerequisite. |
| Families include catalog, centrality, community, pathfinding, similarity, embeddings, ML, miscellaneous, operations, pipelines, and model catalog. | Matches GDS facade reality. |
| Deprecated/alpha/beta procedures are marked explicitly. | Prevents compatibility ambiguity. |
| Unknown procedure calls differ from registered unsupported calls. | Critical for Neo4j-like behavior. |

### Mode Contract

Every algorithm family must classify each mode:

| mode | required behavior |
| --- | --- |
| `stream` | Return rows without graph catalog mutation. |
| `stats` | Return aggregate timings/counts/distributions without per-node result sidecars. |
| `mutate` | Write result into projected graph sidecar/catalog state. |
| `write` | Write result back to the OLTP-facing store or a writeback bridge. |
| `estimate` | Return memory contract without executing the algorithm. |

Cross-cutting tests:

| test | requirement |
| --- | --- |
| Estimate no-work test | `.estimate` must not run algorithm kernels or scan full graph data beyond metadata. |
| Unsupported shape test | Unsupported registered procedure must include procedure name, family, mode, and support level. |
| Schema test | Output columns must match the inventory for implemented modes. |
| Budget gate test | `execute` must reject a plan if `estimate.required_bytes > budget.max_rss_bytes`. |

## Tilehouse As Physical Plan

Cellular CSR Tilehouse should be framed as a physical storage/execution plan,
not as a new public API.

Under the original freshness-sensitive framing, Tilehouse was the preferred
evolution because it made local updates and cell compaction possible. Under the
new assumption that OLAP may lag by hours, Tilehouse should be demoted:

```text
Default: immutable flat CSR + sidecars + atomic generation swap.
Later: Tilehouse if rebuild time, locality, or freshness pressure proves it.
```

This matters for lowest RAM. Tilehouse adds metadata that immutable flat CSR
does not need:

```text
cell passports
global-to-local id maps
boundary indexes
delta receipts
dirty watermarks
cell compaction scratch
multi-cell planning metadata
```

Those are justified when update locality matters. They are harder to justify
for a RAM-first v003 where the product can accept scheduled OLAP refresh.

### Invariants

```text
Flat dual CSR remains the correctness oracle.
Flat immutable CSR remains the default v003 storage target under hours-lag.
Tilehouse must produce the same logical adjacency as flat CSR if added later.
Tiles are independently readable, dirtyable, compactable, and budgetable.
Global algorithms must still be able to stream the whole graph exactly.
Cells improve locality and update granularity, not algorithm exactness.
```

### Proposed Shape

```text
snapshot_generation_42/
  manifest.json
  global_dense_id_map/
  cells/
    cell_000001/
      passport.json
      forward.offsets.bin
      forward.peers.bin
      reverse.offsets.bin
      reverse.peers.bin
      label_sidecars/
      reltype_sidecars/
      node_property_columns/
      edge_property_columns/
      delta_receipts.bin
    cell_000002/
      ...
  boundaries/
    cross_cell_edges.bin
    boundary_nodes.bin
  global_stream/
    logical_forward_order.index
    logical_reverse_order.index
```

### What Tilehouse Improves

| dimension | flat dual CSR | Cellular CSR Tilehouse |
| --- | --- | --- |
| static 1-hop walks | already strong | similar, sometimes better with locality |
| full-graph scans | excellent | must provide logical global stream to avoid regressions |
| small update freshness | full rebuild or broad overlay | dirty affected cells and bounded deltas |
| compaction unit | whole snapshot | cell or cell batch |
| page-cache behavior | OS decides mmap residency | can bound local windows and use explicit stream for strict mode |
| property/label filters | not first-class today | tile-local sidecars |
| implementation complexity | low | medium-high |

### What Tilehouse Does Not Magically Improve

| non-improvement | reason |
| --- | --- |
| Global PageRank vector RAM | Still dominated by per-node score vectors. |
| All-pairs centrality RAM | Still dominated by repeated frontier/distance state. |
| Dense embeddings | Still dominated by `node_count * dimension * bytes_per_value`. |
| Bad partitioning | High boundary ratios can erase locality wins. |
| Unbounded deltas | Multiple overlay layers can become a graph LSM mess. |

## Memory Contract

Every projection and algorithm estimate should produce a structured memory
contract:

```text
required_bytes
heap_bytes
rss_budget_bytes
page_cache_expected_bytes
page_cache_unbounded_risk
direct_io_buffer_bytes
algorithm_state_bytes
frontier_or_vector_bytes
sidecar_bytes
delta_overlay_bytes
compaction_scratch_bytes
snapshot_build_scratch_bytes
spill_bytes
output_sidecar_bytes
```

### Measured Versus Estimated

| value | definition |
| --- | --- |
| estimated | Derived from manifest counts, sidecar metadata, config, and algorithm state formulas. |
| planned | Physical execution plan chosen to fit a budget. |
| measured | Runtime RSS/page-fault/page-cache/procfs/cgroup observations. |

Rules:

| rule | test |
| --- | --- |
| `mmap` plans cannot claim deterministic RAM. | Estimate must flag page-cache residency as OS-mediated. |
| Strict-RAM global plans must name direct or explicit buffers. | Estimate must include buffer sizes and spill sizes. |
| Vectors must be explicit. | PageRank on `200M` nodes must show each vector and bytes per value. |
| Deltas must be capped. | Estimate must fail or force compaction when delta thresholds are exceeded. |
| Compaction must be budgeted. | Dirty-cell compaction must run under `BuildMemoryBudget` or successor budget. |

### 50 GB On 8 GB Decision Rule

For a `50 GB` logical graph on an `8 GB` machine, the planner should answer:

```text
can_run: yes/no
required_budget: bytes
dominant_state: topology/properties/vector/frontier/embedding/output
execution_profile: mmap_interactive | explicit_stream | spillable | reject
freshness_mode: snapshot_only | bounded_delta_merge | force_compaction
```

Example outcomes:

| workload | likely v003 decision |
| --- | --- |
| 1-hop local traversal | run with mmap CSR windows |
| graph catalog list/drop/exists | run from catalog metadata |
| degree centrality | run if output sidecar fits |
| BFS from one source | run with bounded frontier and visited state |
| WCC | run if component vector/union state fits or spills |
| PageRank | run only if vectors plus stream buffers fit, otherwise spill/reject |
| KNN exact all-pairs | reject unless candidate strategy and topK spill plan exists |
| Node2Vec large embeddings | reject or require explicit dimension/batch/spill budget |

## TDD Roadmap

### Phase 0: Preserve Current Behavior

Goal: keep current flat CSR behavior locked before adding a second physical
layout.

Acceptance tests:

| test | requirement |
| --- | --- |
| Snapshot manifest contract | Fixture snapshot has all v2 files, `version == 2`, and `storage_mode == immutable_dual_csr`. |
| Walk contract | Existing `ForwardOne`, `BackwardOne`, `ForwardTwo`, and `BackwardTwo` outputs remain unchanged. |
| Runtime corruption contract | Existing truncated/corrupt snapshot errors remain deterministic. |
| RSS harness smoke test | A small build/query reports peak RSS in a stable JSON shape. |

### Phase 1: GDS Inventory And Registry

Goal: know the surface before building storage for it.

Acceptance tests:

| test | requirement |
| --- | --- |
| Inventory generated/read | Checked inventory exists and is deterministic in CI. |
| No duplicate procedure names | Registry rejects duplicates. |
| PageRank pair present | `gds.pageRank.stream` links to `.estimate`. |
| Catalog present | Graph project/list/drop/exists/size rows are present. |
| Support levels present | Every row has `P0`, `P1`, `P2`, or `UnsupportedButRegistered`. |
| Known unsupported behavior | Registered unsupported procedure returns stable compatibility error. |

### Phase 2: Graph Projection Catalog Skeleton

Goal: represent named GDS graph projections before algorithm execution.

Acceptance tests:

| test | requirement |
| --- | --- |
| Project/list/drop/exists | Named graph lifecycle matches GDS-style expectations. |
| Orientation parsing | `NATURAL`, `REVERSE`, and `UNDIRECTED` are represented logically. |
| Property selectors | Node/relationship property requirements are parsed into sidecar needs. |
| Projection estimate | Estimate includes topology refs, sidecars, catalog metadata, and no duplicate full topology by default. |

### Phase 3: Algorithm-Facing Graph Access

Goal: make algorithms target a logical graph, not file layout.

Proposed trait:

```rust
pub trait GraphAdjacencyRuntime {
    fn node_count(&self) -> u64;
    fn relationship_count(&self) -> u64;
    fn neighbors(
        &self,
        node: DenseNodeId,
        direction: WalkDirection,
    ) -> Result<NeighborCursor<'_>, KnightBusError>;
    fn global_edges(
        &self,
        direction: WalkDirection,
    ) -> Result<EdgeCursor<'_>, KnightBusError>;
}
```

Acceptance tests:

| test | requirement |
| --- | --- |
| Flat adapter parity | `MmapWalkRuntime` adapter returns the same neighbors as `WalkQueryRuntime`. |
| Global edge cursor | Forward and reverse cursors return exact edge sets. |
| Small graph oracle | In-memory normalized graph and flat CSR agree. |
| Property-based graph oracle | Small generated graphs preserve adjacency parity. |

### Phase 4: Immutable CSR v3 Manifest And Sidecar Catalog

Goal: keep the topology immutable and flat, while adding enough manifest and
sidecar metadata to serve GDS-style projections.

Acceptance tests:

| test | requirement |
| --- | --- |
| v3 manifest JSON round-trip | Manifest validates after serialization and still opens v2 snapshots. |
| Generation metadata | Manifest records snapshot generation, source tx range, build time, and graph counts. |
| Sidecar catalog | Manifest lists label, relationship type, property, weight, feature, and result sidecars. |
| Orientation metadata | Natural, reverse, and undirected logical views are represented without duplicating topology by default. |
| Budget report | Build summary reports topology bytes, sidecar bytes, and scratch bytes separately. |

### Phase 5: Immutable CSR Global Streams And Compression Diligence

Goal: prove the immutable CSR snapshot can drive GDS algorithms through cursor
and stream interfaces without building Tilehouse first.

Acceptance tests:

| test | requirement |
| --- | --- |
| File sizes valid | Reader validates offsets, peers, node table, strings, key index, and sidecar lengths. |
| Global stream parity | Forward and reverse global edge streams match the normalized in-memory graph. |
| Logical orientation parity | Natural/reverse/undirected views match oracle graphs without rewriting base files. |
| Sorted-neighbor contract | Algorithms that need intersections either require sorted adjacency or declare runtime sort/spill. |
| Compression diligence | Raw CSR remains baseline; optional compressed peer lists must prove lower total working set before adoption. |

### Phase 6: Sidecar Columns

Goal: support labels, relationship types, weights, and properties without
prebuilding per-algorithm layouts.

Acceptance tests:

| test | requirement |
| --- | --- |
| Label filter | Node labels are queryable without heap materializing all labels. |
| Relationship type filter | Type filters select the correct edge subset across the immutable snapshot. |
| Weight sidecar | Weighted algorithms fail deterministically if required weight is missing. |
| Property null/default behavior | Missing values follow GDS-compatible config behavior. |
| Estimate integration | Sidecar bytes appear in projection and algorithm estimates. |

### Phase 7: First Kernels

Start with kernels that prove different pieces of the architecture:

| kernel | why first | acceptance highlights |
| --- | --- | --- |
| Degree centrality | Low algorithm complexity; validates orientation and output modes. | Hand oracle, stream/schema, estimate includes output state. |
| BFS | Proves frontier scheduling over immutable CSR. | Path oracle, source/target config, stable output order. |
| WCC | Proves global iterative traversal. | Component oracle, in-memory/flat-CSR parity, bounded component state. |
| PageRank | Proves vector memory estimates and global stream plans. | Numeric tolerance oracle, vector-by-vector estimate, strict-RAM plan/reject. |

### Phase 8: WAL Receipts And Cell Deltas

Goal: connect Neo4j-shaped OLTP updates to OLAP freshness without rebuilding the
whole snapshot for tiny changes.

Under the hours-lag assumption, Phase 8 should not block the v003 RAM-first
architecture. Treat it as a later branch:

```text
If users accept scheduled OLAP refresh, use low-RAM full rebuild + atomic
generation swap.

Only build WAL receipts, deltas, and cell compaction if freshness lag becomes
the bottleneck.
```

Acceptance tests:

| test | requirement |
| --- | --- |
| Receipt mapping | Node/relationship/property changes map to affected snapshot regions or cells. |
| Fresh overlay read | Query sees a receipt-applied edge through the delta overlay. |
| Sidecar-only update | Property/label updates dirty sidecars without rewriting topology. |
| Compaction budget | Dirty-cell compaction stays below configured scratch budget. |
| Crash recovery | Durable receipts replay after crash between append and compaction. |
| Delta cap | Query rejects or forces compaction when overlay layers exceed policy. |

### Phase 9: Broader GDS Families

Goal: roll out by dependency order, not excitement order.

Under the hours-lag assumption, Phase 9 should be compared against immutable
CSR, not Tilehouse. The default storage question becomes:

```text
Can this family run over immutable CSR topology plus sidecars and bounded
runtime scratch?
```

The answer is mostly yes, but with different confidence levels:

| tier | family | representative procedures | gate |
| --- | --- | --- | --- |
| 1 | catalog | graph project/list/drop/exists/size | Required before algorithms. |
| 1 | centrality | degree, PageRank | Proves scalar and vector state. |
| 1 | pathfinding | BFS, DFS, Dijkstra | Proves frontiers and weight sidecars. |
| 1 | community | WCC, SCC, triangle count, k-core | Proves global structural algorithms. |
| 2 | community | Louvain, Leiden, label propagation | Requires mutation and contracted-graph scratch. |
| 2 | similarity | nodeSimilarity, KNN | Requires candidate pruning and topK spill. |
| 3 | embeddings | FastRP, Node2Vec, GraphSAGE | Requires vector sidecars and model/output discipline. |
| 3 | ML/pipelines/model catalog | train/predict/model list/drop | Requires persistent model metadata. |
| 3 | operations/misc | progress, scaleProperties, feature flags | Requires telemetry and property-plane maturity. |

### Phase 9 Immutable CSR Fit

| family | immutable CSR fit | additional planes needed | lowest-RAM confidence | can we be sure? |
| --- | --- | --- | --- | --- |
| catalog | excellent | manifest, generation metadata, projection config | high | yes; catalog is metadata over snapshots |
| centrality | excellent for degree/PageRank/HITS; hard for all-pairs variants | vectors, frontier/distance arrays, result sidecars | medium-high | sure for substrate, not for every config fitting |
| pathfinding | excellent for BFS/DFS/Dijkstra/Bellman-Ford edge access | weight sidecars, priority queues, distance/parent arrays | medium-high | sure for exact access; output/path explosion still bounded by config |
| community/structure | strong for WCC/SCC/k-core/triangle; harder for Louvain/Leiden | component/community arrays, sorted neighbors, contracted graph scratch | medium | sure for first-tier families; contraction scratch needs measurement |
| similarity | adequate substrate but not enough by itself | candidate generation, topK heaps, filters, spill | low-medium | no; candidate state can dominate |
| embeddings | adequate for neighbor sampling/propagation | dense embedding matrices, walk corpus, model artifacts | low-medium | no; embedding state dominates topology |
| ML/pipelines/model catalog | CSR helps feature extraction only | feature columns, model storage, training batches | low-medium | no; model/training memory dominates |
| operations/misc | good for metadata/progress/property scans | telemetry, feature flags, property columns | high for simple ops | yes for simple ops; transforms need estimates |

Revised Phase 9 rule:

```text
Do not add a new persistent topology layout for a GDS family unless it reduces
working set under the memory contract.

Prefer immutable CSR + property sidecars + spillable runtime state.
```

## Algorithm Diligence Matrix

### Centrality

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| Degree | offsets and filtered adjacency | output scalar per node | output sidecar or stream chunks | hand degree fixture | low |
| PageRank / ArticleRank / Eigenvector | global edge stream per iteration | 2-4 vectors per node | chunked vectors or spillable vector tape | tiny convergence fixture | medium-high |
| HITS | forward and reverse scans | hub and authority vectors | spillable vectors | bipartite toy graph | high |
| Harmonic / closeness | repeated BFS/SSSP | frontier and distance arrays | source batching | small unweighted graph | high |
| Betweenness | Brandes sweeps | stack, sigma, delta, predecessor state | source batching and spill | diamond graph | very high |
| Articulation points | DFS low-link | discovery, low, parent arrays | arrays or spill if needed | bridge fixture | medium |
| Influence/CELF | repeated simulations | candidate heap and sampled reachability | sampled reachability spill | tiny influence graph | high |

### Pathfinding

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| BFS / DFS | immutable CSR frontier | visited, parent, distance | bitset/frontier spill | tree/cycle fixture | low-medium |
| Dijkstra / A* | wavefront plus weight sidecar | priority queue, distance, predecessor | bucket/queue spill | weighted diamond | medium |
| Delta-Stepping | bucketed frontier | distance vector and buckets | bucket spill | weighted multi-path | medium-high |
| Bellman-Ford | repeated global stream | distance vector | vector spill | negative-edge no-cycle | medium |
| All shortest paths | repeated source sweeps | many distance/frontier states | source batching | tiny all-pairs graph | very high |
| Yen's k-shortest | repeated Dijkstra | candidate path heap | path heap spill | 3-known-path graph | high |
| Random walk | CSR neighbor sampling | RNG state and walk buffers | walk corpus chunks | seeded walk fixture | medium |

### Community And Structure

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| WCC | iterative union/frontier | component id per node | component vector spill | two-components fixture | low-medium |
| SCC | forward/reverse DFS | stacks and component arrays | stack spill if needed | directed SCC fixture | medium |
| Triangle count / LCC | sorted intersections | intersection buffers | high-degree chunking | triangle/square fixture | medium-high |
| k-core | degree peeling | degree array and queue | queue spill | k-core toy graph | medium |
| Coloring | iterative colors | color array and conflict frontier | conflict frontier spill | odd/even cycle | medium |
| Label propagation / SLPA | neighbor label scans | label distributions | label distribution spill | two-cluster fixture | medium-high |
| Louvain / Leiden | contraction levels | community ids and aggregate graph | contracted graph sidecar | modularity toy graph | high |
| Modularity / conductance | cut/internal scans | community sidecars | stream scans | known partition | medium |

### Similarity

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| Node similarity | adjacency overlap | candidate pairs and topK heaps | candidate blocking and topK spill | bipartite overlap graph | very high |
| Filtered node similarity | filtered overlap | filtered candidates | filter-first blocking | filtered toy graph | high |
| KNN | vector/property scan | per-node topK and candidate sampler | blocked scan, topK spill | small vector set | very high |
| Filtered KNN | filtered vector scan | filtered topK heaps | filter-first blocks | filtered vector set | high |

Rule: no exact similarity implementation may materialize all `O(n^2)` pairs
without an explicit candidate strategy and budget rejection test.

### Embeddings

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| FastRP | repeated sparse propagation | embedding matrix | chunked embedding sidecar | seeded tiny embedding | very high |
| Node2Vec | random walks plus training | walk corpus and embedding matrix | walk chunking and model batches | seeded walk corpus | very high |
| GraphSAGE | neighbor sampling | batches and model weights | batch scheduler | tiny train/infer fixture | very high |
| HashGNN | hashed features and aggregation | hashes and embeddings | chunked feature sidecars | deterministic hash fixture | high |

Rule: dimension, batch size, concurrency, walk length, and bytes per value must
be part of every embedding estimate.

### ML, Pipelines, Catalog, And Miscellaneous

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| KGE | relationship type/property sidecars | embeddings and negative samples | batch training | tiny typed KG | very high |
| Split relationships | property/filter mutation | split tags/properties | sidecar chunks | deterministic seed | medium |
| Scale properties | property column scan | stats and output column | chunked column scan | numeric property fixture | low |
| To undirected | logical projection | doubled logical relationships | projection metadata | asymmetric fixture | medium |
| Collapse path / index inverse | topology transform | derived relationship sidecar | derived sidecar chunks | path fixture | medium-high |
| Model catalog | metadata/artifacts | model bytes and feature schema | artifact files | register/list/drop | high |
| Pipelines | metadata plus execution | feature extraction and model state | staged execution | train/list/drop | high |
| Operations/progress | execution telemetry | counters and task state | bounded task registry | progress fixture | low |

## Implementation PR Sequence

### PR 1: GDS Inventory And Registry Only

No storage changes.

Tests first:

| test | requirement |
| --- | --- |
| Inventory parse | Checked inventory reads deterministically. |
| Registry duplicate detection | No duplicate procedure names. |
| PageRank known | Stream and estimate known. |
| Catalog known | Graph catalog core rows known. |
| Unsupported known | Missing kernels return `UnsupportedButRegistered`. |

Value:

```text
Prevents API-surface drift before deep storage work.
```

### PR 2: Projection Catalog Skeleton

No algorithm kernels.

Tests first:

| test | requirement |
| --- | --- |
| project/list/drop/exists | Named graph lifecycle works. |
| orientation parsing | Natural/reverse/undirected represented logically. |
| estimate shape | Holistic memory estimate fields exist. |

### PR 3: `GraphAdjacencyRuntime` Over Flat CSR

No Tilehouse yet.

Tests first:

| test | requirement |
| --- | --- |
| neighbor cursor parity | Matches existing walk runtime. |
| global edge cursor parity | Matches flat CSR edge set. |
| old tests pass | Existing CLI/library contracts unaffected. |

### PR 4: Immutable CSR v3 Manifest And Sidecar Catalog

Tests first:

| test | requirement |
| --- | --- |
| manifest round-trip | v3 manifest validates and v2 snapshots still open. |
| sidecar catalog | Labels, types, weights, properties, features, and result sidecars are declared. |
| generation metadata | Source tx range and snapshot generation are visible. |

### PR 5: Immutable CSR Global Streams And Compression Diligence

Tests first:

| test | requirement |
| --- | --- |
| edge stream parity | Global forward/reverse streams match normalized graph. |
| orientation parity | Natural/reverse/undirected logical views match oracles. |
| compression gate | Any compressed peer format must prove lower working set than raw CSR. |

### PR 6: Sidecar Columns

Tests first:

| test | requirement |
| --- | --- |
| label/type filters | Correct node/edge subset selected. |
| property columns | Missing/default values handled. |
| estimate integration | Sidecar bytes counted. |

### PR 7: Degree + BFS + WCC

Tests first:

| test | requirement |
| --- | --- |
| degree oracle | Directed/reverse/undirected correct. |
| BFS oracle | Frontier traversal over immutable CSR is stable. |
| WCC oracle | In-memory/flat-CSR parity stable. |

### PR 8: PageRank Deterministic RAM Proof

Tests first:

| test | requirement |
| --- | --- |
| numeric oracle | Tiny graph converges within tolerance. |
| vector estimate | `200M` node estimate names all vectors. |
| strict mode | O_DIRECT/explicit stream candidate does not claim mmap determinism. |
| budget reject | Unsafe plan rejected before execution. |

### PR 9: Optional WAL Receipts And Cell Deltas

Tests first:

| test | requirement |
| --- | --- |
| receipt mapping | Affected snapshot regions or cells are correct. |
| overlay freshness | New edge visible through overlay. |
| compaction | Overlay removed and cell CSR updated under budget. |
| recovery | Receipt replay is deterministic. |

### PR 10+: Remaining Families

Roll out by the algorithm diligence matrix. Do not mark a procedure supported
until all rollout gates pass.

## Rollout Gates

No procedure can move to `P1 implemented exact low-RAM` until:

```text
G1 registry row exists
G2 config parser validates defaults and bad inputs
G3 estimate accounts for topology, sidecars, state, scratch, optional deltas, and I/O policy
G4 tiny oracle correctness test passes
G5 immutable CSR and in-memory oracle parity test passes where topology is involved
G6 supported modes have schema tests
G7 budget rejection test passes
G8 deterministic ordering and seed behavior is documented
G9 unsupported mode behavior is deterministic
G10 result sidecar or writeback semantics are tested if mutate/write is supported
```

## Acceptance Checklist

This diligence doc is implementation-ready when it satisfies:

| checklist item | status |
| --- | --- |
| Explains why the pasted draft is strong but incomplete. | done |
| Grounds Knight Bus claims in current local source line references. | done |
| Grounds GDS claims in local `gitrefrepo/neo4j-gds-src` line references. | done |
| Uses `gitrefrepo/` paths, not old reference-shelf paths. | done |
| Separates fact, inference, and speculation. | done |
| Corrects the `compio`/`O_DIRECT` thesis. | done |
| Recommends immutable CSR as default topology and Tilehouse as optional later work under the hours-lag assumption. | done |
| Defines GDS support levels. | done |
| Defines memory estimate fields and strict-RAM rules. | done |
| Provides a TDD PR sequence. | done |
| Provides an algorithm-family diligence matrix. | done |
| Defines the first concrete implementation task. | done |

## First Concrete Task

Start with:

```text
PR 1: GDS Inventory And Registry Only
```

Reason:

| reason | explanation |
| --- | --- |
| Cheapest uncertainty reducer | It requires no Tilehouse or algorithm implementation. |
| Protects compatibility | It makes the public surface visible before storage work dominates. |
| Enables scoped honesty | It lets v003 be explicit about `P0`, `P1`, `P2`, and `UnsupportedButRegistered`. |
| Feeds all later work | Every later algorithm/storage PR can test itself against the inventory. |

The first code-bearing PR should not start by writing a tile partitioner. It
should start by proving that the team knows what it means to look like GDS.

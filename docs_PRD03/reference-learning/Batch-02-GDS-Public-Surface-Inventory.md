# Batch 02: GDS Public Surface Inventory Baseline

Date: 2026-06-24

Assigned lanes:

- `Surface lane`
- `Capability lane`

Assigned PRD outcomes:

- `Complete GDS surface`
- `Neo4j-compatible API`
- `Strict holistic RAM`

Requirement IDs touched in this batch:

- `REQ-LEARN-006.0`
- `REQ-LEARN-009.0`
- `REQ-LEARN-010.0`
- `REQ-LEARN-016.0`
- `REQ-LEARN-017.0`
- `REQ-LEARN-030.0`
- `REQ-LEARN-033.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-042.0`
- `REQ-LEARN-043.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`

Primary artifact emitted by this batch:

- [GDS-Public-Surface-Inventory.tsv](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv)

Scope note:

- This batch inventories the visible public GDS entry surface from the local
  clone.
- This batch does not yet enrich every row with parsed config type, result
  columns, estimate source, and oracle test.
- Every row in the TSV is conservatively marked `NeedsArchitectureSpike` until
  later batches prove more.

## Clone Coverage Ledger

| local_repo | exists_now | upstream_hint | branch_or_head | study_role | required_or_optional | current_use | note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `gitrefrepo/neo4j-gds-src` | yes | `neo4j/graph-data-science` | `2.13 @ dc4417b3c1` | compatibility oracle | required | active study | source of the public GDS procedure, function, catalog, model, pipeline, and operations surface |

## Evidence Ledger

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CLAIM-B02-001` | `REQ-LEARN-006.0` | generated inventory | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` | row count | The baseline TSV currently contains `575` visible `gds.*` entries from the local `proc` tree. | The public GDS surface is large enough that manual memory-based summaries are not a safe substitute for inventory artifacts. | A future extraction pass may shrink or grow this count as parsing rules improve. | Complete GDS surface | Falsifier: a tighter parser removes large classes of rows that were never public surface. |
| `CLAIM-B02-002` | `REQ-LEARN-006.0` | generated inventory | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` | `entry_kind` counts | The TSV currently contains `559` `procedure` rows and `16` `user_function` rows. | GDS compatibility work must include functions like `gds.graph.exists` and similarity helpers, not only procedures. | More user-facing functions may exist outside `proc/` or through other annotations. | Neo4j-compatible API | Falsifier: user functions are not used by target clients or are duplicated internal helpers only. |
| `CLAIM-B02-003` | `REQ-LEARN-006.0` | generated inventory | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` | `family` counts | Family counts in the baseline TSV are `community 154`, `path-finding 92`, `centrality 82`, `catalog 54`, `machine-learning 46`, `similarity 44`, `misc 40`, `embeddings 36`, `common 13`, `pipeline-catalog 6`, `sysinfo 5`, and `test 3`. | The architecture must withstand breadth across multiple storage and state shapes; the surface is not dominated by one or two families. | Some deprecated aliases may inflate family totals relative to canonical surface. | Complete GDS surface; Strict holistic RAM | Falsifier: a canonical-name de-duplication pass collapses the surface dramatically. |
| `CLAIM-B02-004` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java:52-123` | `gds.graph.project*` | Public catalog projection rows include `gds.graph.project`, `.estimate`, deprecated `gds.graph.project.cypher`, `.cypher.estimate`, and deprecated subgraph/project variants. | Inventory must preserve deprecated and alias surface because client code can still call it. | A future v003 rollout can route deprecated aliases to warnings or deterministic unsupported responses. | Complete GDS surface | Falsifier: deprecated variants are provably unused in all target client workflows. |
| `CLAIM-B02-005` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/model/catalog/ModelListProc.java:41-56` and `.../PipelineListProc.java:41-58` | `gds.model.list`, `gds.pipeline.list` | Model-catalog and pipeline-catalog procedures are part of the visible public surface. | Any claim of “full GDS compatibility” that ignores models and pipelines is incomplete. | Early v003 may still register these as deterministic unsupported behavior first. | Complete GDS surface | Falsifier: PRD scope later explicitly excludes all ML and pipeline surface. |
| `CLAIM-B02-006` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/misc/src/main/java/org/neo4j/gds/ListProgressProc.java:40-52` | `gds.listProgress` | Operations/progress procedures are publicly exposed outside the main algorithm facades. | Compatibility inventory must include operational surfaces, not just data and algorithm calls. | Progress APIs may be lightweight compared with graph algorithms, but they still affect surface completeness. | Complete GDS surface | Falsifier: target clients never call progress procedures. |
| `CLAIM-B02-007` | `REQ-LEARN-030.0` | generated inventory | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` | `support_status` column | Every row is currently marked `NeedsArchitectureSpike` to avoid overclaiming v003 support before config/result/estimator/test enrichment is complete. | This baseline is an inventory truth artifact, not a compatibility promise artifact yet. | Later batches can promote selected rows to `P0-RegisteredCompatible` or stronger states. | Complete GDS surface | Falsifier: enough evidence already exists today to assign deterministic statuses to a broad subset. |
| `CLAIM-B02-008` | `REQ-LEARN-009.0` | generated inventory | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv` | family plus architecture-needs columns | The baseline inventory already separates families into likely storage/state pressure groups such as catalog metadata, topology plus state, topology plus weight sidecars, embeddings plus artifacts, and similarity plus candidate pruning. | Even before full kernel tracing, the visible surface already points to multiple capability buckets that one storage primitive alone cannot satisfy. | Some architecture-need cells will change once config/result parsing and kernel tracing are complete. | Strict holistic RAM | Falsifier: later traced kernels show these family-level buckets are materially wrong. |

## Architecture Fit Matrix

This matrix is intentionally family-level for the baseline batch. Row-by-row
procedure fit is delegated to the companion TSV and later kernel-enrichment
batches.

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `catalog family` | topology reference only | schema, labels, types, node/relationship properties, result metadata | projection facts, dictionaries, counts, export metadata | named graph identity, generation, watermark, list/drop semantics | minimal | metadata plus estimate rows | catalog queries over snapshot metadata and build facts | `NeedsArchitectureSpike` | traced catalog tests show all required semantics can be derived from anonymous snapshot files alone |
| `centrality family` | graph traversal or message-passing over global topology | optional relationship weights, result properties | estimate stats and projection metadata | named graph selection and mode semantics | per-node numeric state, sometimes multiple vectors | O(V) state dominates on larger graphs | normal fast mode plus explicit strict-RAM alternatives for some algorithms | `NeedsArchitectureSpike` | later kernel traces show centrality algorithms use only trivial walk-time state |
| `community family` | graph traversal over full topology | optional properties, community/result sidecars | estimate stats, contracted-graph scratch metadata | named graph selection and mutate/write targets | iterative state, often contraction or frontier-like state | can be O(V) and sometimes duplicate-topology-like during contraction | global traversal plus possibly spill/contraction support | `NeedsArchitectureSpike` | later traces show all community modes fit pure flat topology without extra scratch or sidecars |
| `path-finding family` | topology plus directional semantics | weight sidecars for weighted algorithms | path-estimate inputs and projection metadata | named graph selection and writeback target semantics | frontier, distances, priority queues, path materialization | highly workload-sensitive; all-pairs forms are likely much harder | bounded frontier/queue strategies and possible rejection under strict budgets | `NeedsArchitectureSpike` | later traces show path-finding surface is much narrower than the proc inventory suggests |
| `similarity family` | often less topology-heavy than candidate generation heavy | property/vector sidecars and filter metadata | candidate-generation planning metadata | named graph selection and write/mutate targets | top-K heaps, candidate pairs, filtered search state | O(n²) risk if not pruned | blocking, candidate pruning, spill, or rejection | `NeedsArchitectureSpike` | traced kernels show exact all-pairs candidate growth is not actually part of target modes |
| `embeddings family` | graph topology or sampled graph neighborhoods | feature, vector, weight, and embedding-result sidecars | training config, result artifacts, model linkage | graph plus model or embedding artifact identity | walk corpus, dense matrices, model state, sampled batches | often larger than topology-local walk state | artifact-aware execution; likely not pure walk runtime | `NeedsArchitectureSpike` | kernel evidence shows embeddings can be streamed with negligible persistent artifacts |
| `machine-learning family` | graph plus feature/materialized training inputs | feature and model sidecars | model catalog, training metadata, feature pipelines | graph/model/pipeline identity | model state and training results | model artifacts and feature materialization can dominate | training/inference pipeline, not just topology scan | `NeedsArchitectureSpike` | later PRD scope explicitly removes ML surface from claims |
| `misc and sysinfo families` | mixed, often metadata or helper operations | varies | varies | often catalog or operations metadata | low to medium | mostly small except graph transforms | utility procedures, operations telemetry, or graph transforms | `NeedsArchitectureSpike` | later source review shows these are purely thin aliases with no independent compatibility burden |

## PRD Outcome Traceability Dossier

| PRD outcome | supporting claims | current confidence | next experiment or evidence spike |
| --- | --- | --- | --- |
| `Complete GDS surface` | `CLAIM-B02-001` through `CLAIM-B02-007` | `medium` | enrich the TSV with config type, result type, estimate path, and oracle-test columns for at least catalog, centrality, community, similarity, and embeddings |
| `Neo4j-compatible API` | `CLAIM-B02-002`, `CLAIM-B02-004`, `CLAIM-B02-005`, `CLAIM-B02-006` | `medium` | add Bolt/driver/cypher-shell/client canary evidence so public surface is tied to real client behavior |
| `Strict holistic RAM` | `CLAIM-B02-003`, `CLAIM-B02-008` | `low to medium` | trace representative high-pressure families like KNN, Leiden, WCC, and Node2Vec to real estimator and kernel paths |

## Rejected-Alternative Note

Rejected for this batch:

- `Create only a prose family summary and skip the full inventory TSV.`

Why rejected:

- The local surface is already too large to trust memory or hand-written bullet
  summaries.
- The spec explicitly requires inventory-first behavior before architecture
  sufficiency claims.
- Future agents need a diffable, machine-readable artifact to enrich rather than
  redoing the discovery step.

What would overturn this rejection:

- The proc surface shrinks so drastically in a later source version that a full
  inventory becomes trivial and stable enough to maintain by hand.

## Skeptical Review

| challenge | response |
| --- | --- |
| Does this inventory overcount because of deprecated aliases? | Yes, aliases inflate the raw row count, but that is still useful because deprecated entrypoints remain part of compatibility burden until intentionally handled. |
| Are `architecture_needs` in the TSV too inferential? | Yes. They are first-pass capability hints, not proof. That is why every row is still `NeedsArchitectureSpike`. |
| Does the batch prove any algorithm family is supportable? | No. It proves surface breadth, not support sufficiency. |
| Are user functions being mixed with procedures incorrectly? | No. The TSV has an `entry_kind` column so they can be filtered separately. |

## Verification Commands Run

```bash
python3 - <<'PY'
from pathlib import Path
import csv
p = Path('docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv')
rows = list(csv.DictReader(p.open(), delimiter='\t'))
print(len(rows))
PY

sed -n '1,25p' docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv
rg -n "^user_function\t|\tgds\\.graph\\.exists\t|\tgds\\.pageRank\\.stream\t|\tgds\\.pageRank\\.write\t|\tgds\\.graph\\.project\t|\tgds\\.model\\.list\t|\tgds\\.pipeline\\.list\t|\tgds\\.listProgress\t" docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv
git diff --check -- docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv
```

## Checkpoint: surface+capability / gds-public-surface-baseline / 2026-06-24

Assigned requirement IDs:

- `REQ-LEARN-006.0`
- `REQ-LEARN-009.0`
- `REQ-LEARN-010.0`
- `REQ-LEARN-016.0`
- `REQ-LEARN-017.0`
- `REQ-LEARN-030.0`
- `REQ-LEARN-033.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-042.0`
- `REQ-LEARN-043.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`

Evidence rows completed:

- `8`

Most important sourced facts:

- The companion TSV inventories `575` visible `gds.*` entries from the local
  clone.
- The surface includes `procedure` and `user_function` entry kinds.
- The visible surface spans graph catalog, algorithms, model catalog, pipeline
  catalog, misc/operations, and common helper functions.

Architecture implications:

- `Adopt`: use the TSV as the baseline surface corpus for later enrichment.
- `Adapt`: enrich rows with config/result/estimate/oracle fields by family in
  later batches instead of trying to finish every field in one pass.
- `Reject`: any architecture claim based only on flagship algorithms.
- `Watch`: whether some families collapse materially after canonical-name
  de-duplication.
- `MissingEvidence`: per-row config types, result types, exact estimate paths
  for every family, and deterministic unsupported-registration mapping.

Unresolved risks:

- `Risk`: some rows may be deprecated aliases or helpers that should be
  canonicalized before final counts are used in product-facing statements.
  `Falsifier`: a canonicalization pass reduces the inventory sharply while
  preserving family diversity.
- `Risk`: family-level architecture hints may be too coarse.
  `Falsifier`: kernel-enrichment batches for WCC, Leiden, KNN, Node2Vec, and
  GraphSage contradict the family-level buckets.

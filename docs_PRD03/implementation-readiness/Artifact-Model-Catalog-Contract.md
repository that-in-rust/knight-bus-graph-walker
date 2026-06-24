# Artifact Model Catalog Contract

Full GDS compatibility is not only graph topology and algorithms. It includes
result artifacts, mutate/write targets, model catalogs, pipeline catalogs, and
cleanup semantics.

## PRD Plane

| plane | catalog responsibility |
| --- | --- |
| OLTP storage | write mode may persist properties or relationships back to OLTP when explicitly supported |
| Projection Build Store | records artifact build metadata and validation state |
| OLAP snapshot storage | stores generation-scoped result/model/pipeline sidecars used by GDS reads |

## Artifact Identity

| field | purpose |
| --- | --- |
| user | matches GDS user-scoped catalog behavior |
| database | separates graphs by database identity |
| graph | named graph or snapshot/projection handle |
| generation | source OLAP generation used to create the artifact |
| procedure | producing procedure name, such as `gds.pageRank.write` |
| mode | stream, stats, mutate, write, estimate, train, predict, or catalog |
| config hash | stable hash of normalized config |
| source watermark | OLTP/source watermark represented by input generation |
| artifact kind | node property, relationship property, relationship result, embedding, model, pipeline, metric summary |
| schema fingerprint | result columns, value types, dimensions, labels/types/properties used |
| created at | lifecycle diagnostics |
| expires at | cleanup and retention policy |

## Lifecycle Semantics

| state | meaning | allowed operations |
| --- | --- | --- |
| staged | artifact is being produced privately | no list/read |
| valid | artifact is visible for the matching generation and graph | list/read/use |
| stale | input generation has been superseded or source watermark no longer matches freshness expectation | list with stale flag, optional read if config allows |
| invalid | validation failed or source artifact missing | diagnostics only |
| retired | no new reads, retained for pinned readers or rollback | pinned reads only |
| cleanup | eligible for deletion | no reads |

## Model And Pipeline Rules

| surface | contract |
| --- | --- |
| model catalog | model names are user/database scoped and record training generation, config hash, feature schema, metric summary, and model bytes location |
| pipeline catalog | pipelines are cataloged metadata objects with step list, feature transforms, split strategy, and train/predict compatibility rules |
| result sidecars | mutate writes to projected graph sidecar; write writes to OLTP or a declared deferred-write path |
| missing generation | artifact access fails with deterministic stale/missing-generation error unless artifact is declared generation-independent |
| stale generation | artifact is visible as stale; algorithms decide whether stale input is allowed |
| cleanup | cleanup never deletes artifacts pinned by active readers, retained rollback generations, or model references |

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| ART-001 | DirectSource | `docs_PRD03/reference-learning/Batch-08-Hard-GDS-Families-And-Model-Artifacts.md:141` | PipelineCatalog, OpenModelCatalog | full GDS needs a metadata/artifact plane beyond graph files | all model/pipeline surfaces are out of scope |
| ART-002 | DirectSource | `gitrefrepo/neo4j-gds-src/pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java:35-140` | PipelineCatalog | pipeline catalog has first-class semantics | v003 intentionally excludes all pipeline APIs |
| ART-003 | DirectSource | `gitrefrepo/neo4j-gds-src/open-model-catalog/src/main/java/org/neo4j/gds/core/model/OpenModelCatalog.java:40-149` | OpenModelCatalog | model storage/lifecycle must be modeled explicitly | models can be recomputed every call without catalog |
| ART-004 | Inference | `docs_PRD03/reference-learning/Batch-10-GDS-Projection-Internals-And-Support-Tiers.md:162` | GraphStoreCatalog | graph, model, pipeline, and result catalogs should share identity/watermark discipline | separate ad hoc catalogs prove simpler and compatible |
| ART-005 | NeedsSource | `gitrefrepo/neo4j-gds-src` | exact write/mutate storage behavior by family | stale/missing-generation errors need Neo4j-compatible shape | line-level procedure audit fills error-shape gaps |

## Verification Commands

```bash
rg -n "user|database|graph|generation|procedure|config hash|watermark|model|pipeline|cleanup" docs_PRD03/implementation-readiness/Artifact-Model-Catalog-Contract.md
rg -n "PipelineCatalog|OpenModelCatalog|ModelCatalogProcedureFacade|PipelinesProcedureFacade" gitrefrepo/neo4j-gds-src
```


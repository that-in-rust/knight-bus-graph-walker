# Snapshot Publication State Machine

This state machine protects OLAP readers from half-built files and gives
freshness semantics a concrete source watermark.

## PRD Plane

| plane | role |
| --- | --- |
| Projection Build Store | creates staged generations, validation reports, and publication handoff metadata |
| OLAP snapshot storage | serves only published immutable generations |
| OLTP storage | supplies source watermarks used to describe freshness |

## States

| state | meaning | queryable |
| --- | --- | --- |
| staged | files are being written privately | no |
| validating | files are complete enough for validation, checksums, counts, and schema checks | no |
| published | active pointer selects this generation for new readers | yes |
| retired | no longer active for new readers, retained for existing reader pins or rollback | only for already-pinned readers |
| failed | build or validation failed; error retained for diagnostics | no |
| garbage | safe to delete after retention and reader floor pass | no |

## Transitions

| transition | from | to | required proof |
| --- | --- | --- | --- |
| build_start | none | staged | generation id allocated and private directory created |
| validation_start | staged | validating | all declared files closed, fsynced as required, manifest complete |
| publish_swap | validating | published | validation report passes and active pointer swap is atomic |
| supersede | published | retired | newer generation becomes published |
| reader_floor_passed | retired | retired | no reader references older than retained floor |
| retention_elapsed | retired | garbage | retention and rollback windows are satisfied |
| validation_error | staged or validating | failed | error recorded, generation never becomes active |
| rollback | published | previous_published | previous generation is still retained and validation report exists |

## Reader Proof

```text
Reader R1 starts before publish_swap:
  active pointer resolves to W
  R1 pins W
  W+1 may become published later
  R1 continues using W until release

Reader R2 starts after publish_swap:
  active pointer resolves to W+1
  R2 pins W+1

No reader opens staged or validating paths:
  catalog lookup returns only published generation ids
  private build paths are not part of catalog resolution
```

## Catalog Fields

| field | purpose |
| --- | --- |
| generation_id | monotonic immutable generation identity |
| state | one of staged, validating, published, retired, failed, garbage |
| source_watermark | OLTP transaction or receipt watermark represented by the generation |
| manifest_path | relative path to snapshot payload manifest |
| validation_report_path | checksums, counts, schema fingerprint, sidecar coverage |
| created_at | lifecycle diagnostics |
| published_at | freshness and rollback diagnostics |
| retired_at | retention and cleanup diagnostics |
| reader_pin_count | conservative live-reader retention |
| rollback_parent | previous published generation retained for rollback |

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| PUB-001 | DirectSource | `docs_PRD03/reference-learning/Batch-04-Publication-And-Generation-Catalog.md:55` | current manifest gap | current snapshot payload lacks publication metadata | current code already has active pointer semantics elsewhere |
| PUB-002 | DirectSource | `docs_PRD03/reference-learning/Batch-04-Publication-And-Generation-Catalog.md:56-59` | fjall and Ladybug publication precedents | publish-after-durable and reader-safe GC are required invariants | directory timestamp scan proves equally safe |
| PUB-003 | DirectSource | `docs_PRD03/reference-learning/Batch-04-Publication-And-Generation-Catalog.md:65-79` | PG-B recommendation | active catalog file with immutable payloads is the default | PG-A or PG-C passes rollback/retention tests |
| PUB-004 | Inference | `docs_PRD03/prd-l1.md:68-87` | freshness and OLAP boundary | freshness is reported by generation watermark, not query-time delta merge | PRD later requires immediate OLAP freshness |

## Verification Commands

```bash
rg -n "staged|validating|published|retired|failed|garbage|rollback|reader" docs_PRD03/implementation-readiness/Snapshot-Publication-State-Machine.md
rg -n "generation|watermark|publish|rollback|retention" docs_PRD03/reference-learning/Batch-04-Publication-And-Generation-Catalog.md
```


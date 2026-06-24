# Projection Build Store Physical Contract

The Projection Build Store is the analytical foundry between Neo4j-shaped OLTP
truth and published OLAP snapshots. It is not a third serving store.

## PRD Plane

| plane | rule |
| --- | --- |
| OLTP storage | produces committed facts and source watermarks |
| Projection Build Store | normalizes, replays, validates, compacts, and stages snapshot inputs |
| OLAP snapshot storage | receives only validated immutable generation payloads |

## Required Physical Responsibilities

| responsibility | contract | evidence | open gap |
| --- | --- | --- | --- |
| receipt schema | append committed node, relationship, label, type, and property operations with tx id and source watermark | `docs_PRD03/reference-learning/Batch-03-Projection-Build-Store-Precedents.md:56-60` | exact binary receipt encoding |
| ordering | replay receipts in commit order, reject or buffer out-of-order facts | Batch 03 append/persist/apply/publish discipline | exact reorder policy |
| idempotency | duplicate receipt replay must produce identical staged facts | Inference from crash replay requirement | NeedsSource for chosen metadata store |
| replay | crash recovery rebuilds build-state from durable receipt log plus metadata store | `Batch-03...:57-64` | replay fixture tests |
| dense ids | assign stable generation-local dense ids and record mappings | `docs_PRD03/prd-l1.md:103-120` | exact dictionary schema |
| dictionaries | build label, relationship-type, property-key, string, and model/artifact dictionaries as needed | `prd-l1.md:103-120` | retention policy for unused dictionary entries |
| sorted runs | stage sorted node, relationship, label, type, and property runs for snapshot compilation | `prd-l1.md:103-120` | exact file format |
| metadata state | track pending, durable, validating, failed, and published handoff metadata | `Batch-03...:58-61` | concrete metadata engine |
| validation reports | produce counts, checksums, source watermarks, schema fingerprints, and sidecar coverage | `prd-l1.md:103-120`; `Batch-04...:55-61` | exact report schema |
| retention | retain enough build and generation state for rollback, reader pins, and recovery | `Batch-04...:56-59` | cleanup scheduler |
| compaction thresholds | compact or discard receipt/fact runs only after safe-to-GC watermark | `Batch-03...:61-64` | benchmark thresholds |
| partial receipt append | detect checksum/length mismatch and replay only complete receipt batches | `gitrefrepo/apache-iggy-src/core/partitions/src/messages_writer.rs:37-126` | exact segment footer |
| partial metadata commit | metadata must distinguish pending from durable state | `gitrefrepo/redb-src/src/transaction_tracker.rs:79-210` | chosen embedded metadata engine |
| failed snapshot build | failed generations stay invisible and carry validation error | `Batch-04...:58-61` | failure taxonomy |
| failed publication handoff | active pointer remains at previous generation and failed W+1 is not queryable | `Batch-04...:65-79` | restart recovery test |

## Crash Case Matrix

| crash point | required recovery |
| --- | --- |
| after OLTP commit before receipt append | rebuild receipt from OLTP log or mark source watermark gap as blocking |
| during partial receipt append | ignore incomplete receipt batch and request replay |
| after receipt append before metadata update | replay receipt idempotently |
| during sorted-run creation | discard private run and rebuild from receipts/facts |
| after validation report before publish | resume validation or publish only if all files are durable |
| during publish handoff | active generation remains W or atomically becomes W+1, never half-published |
| during compaction | compacted inputs are dropped only after durable replacement and safe-to-GC watermark |

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| PBS-001 | DirectSource | `docs_PRD03/prd-l1.md:103-120` | allowed responsibilities | Build Store is a compiler/foundry, not a query store | PRD allows user queries over Build Store |
| PBS-002 | DirectSource | `docs_PRD03/reference-learning/Batch-03-Projection-Build-Store-Precedents.md:57-64` | Iggy, redb, fjall precedents | receipt log plus durable metadata is the safest default shape | simpler direct rebuild path proves lower RAM and equal freshness |
| PBS-003 | DirectSource | `docs_PRD03/reference-learning/Batch-03-Projection-Build-Store-Precedents.md:63` | RocksDB direct I/O note | direct I/O is surgical for strict-RAM build/compaction, not blanket policy | strict-RAM path works with mmap/buffered I/O only |
| PBS-004 | Inference | `Snapshot-Publication-State-Machine.md` | staged and published states | build-store state feeds generation publication | publication can be completely stateless |

## Verification Commands

```bash
rg -n "receipt|ordering|idempotency|replay|dense|dictionary|sorted|validation|retention|compaction|partial" docs_PRD03/implementation-readiness/Projection-Build-Store-Physical-Contract.md
rg -n "Projection Build Store|dense|dictionary|sorted|validation|crash" docs_PRD03/prd-l1.md docs_PRD03/reference-learning/Batch-03-Projection-Build-Store-Precedents.md
```


# OLTP Record Store Rust Contract

This contract keeps OLTP storage Neo4j-shaped while allowing OLAP/GDS storage to
be optimized separately. It is not a Rust implementation plan for every record
class; it is the compatibility checklist that future Rust code must satisfy.

## PRD Plane

| plane | rule |
| --- | --- |
| OLTP storage | source of truth for writes, locks, transactions, indexes, and Cypher OLTP reads |
| Projection Build Store | consumes committed facts or receipts from OLTP only after commit |
| OLAP snapshot storage | never accepts user writes and never becomes the source of truth |

## Compatibility Invariants

| area | Rust contract | direct evidence | gap |
| --- | --- | --- | --- |
| nodes | preserve node identity, in-use bit, labels/token references, relationship pointer/group pointer semantics | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java:30` | NeedsSource for exact bit layout |
| relationships | preserve relationship id, source/target, type, next/prev chain semantics, and in-use behavior | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java:30` | NeedsSource for exact bit layout |
| properties | preserve property record chains, property blocks, and dynamic-record references | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java:33` | NeedsSource for value encoding |
| schema tokens | labels, relationship types, and property keys must round-trip through Neo4j-compatible token ids | NeedsSource | line-level token-store audit required |
| indexes | indexes must match Cypher planning and constraint expectations | NeedsSource | index provider and schema-store audit required |
| WAL | writes must be durable and replayable before OLAP receipts are trusted | NeedsSource | transaction-log source path required |
| locks | concurrent writes must preserve Neo4j-compatible isolation and deadlock behavior where claimed | NeedsSource | lock manager audit required |
| checkpoints | checkpoint and recovery must never expose committed transaction loss or half-applied records | NeedsSource | checkpoint source audit required |
| import | bulk import must create consistent records and dense-node structures | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/batchimport/ImportLogic.java:193` | NeedsSource for full import phases |
| recovery | restart must replay or repair OLTP before emitting build receipts | NeedsSource | recovery source audit required |
| dense nodes | dense node threshold must drive relationship group behavior | `gitrefrepo/neo4j-src/community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java:685`; `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/RelationshipGroupStore.java:64` | NeedsSource for exact group-store traversal |
| relationship groups | high-degree relationship groups must support type/direction grouping | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:40` | NeedsSource for full traversal cases |
| relationship chains | sparse node traversal must follow linked relationship chains correctly | `gitrefrepo/neo4j-src/community/community-it/record-storage-engine-it/src/test/java/org/neo4j/kernel/impl/store/RelationshipChainPointerChasingTest.java:64` | NeedsSource for exact cursor path |
| property blocks | inline and dynamic property values must match driver-visible values | `PropertyRecordFormat.java:33` | NeedsSource for exact property block cases |
| dynamic records | strings, arrays, and long values must be stored/recovered compatibly | NeedsSource | dynamic record store audit required |

## OLAP Receipt Handoff Rule

The OLTP store emits analytical receipts only after commit durability. Receipts
are not visible to OLAP queries directly. They feed the Projection Build Store,
which later publishes a new snapshot generation.

```text
OLTP transaction commit
  -> durable WAL/record state
  -> receipt log append
  -> Projection Build Store replay
  -> snapshot W+1 build
  -> publication state machine
```

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| OLTP-001 | DirectSource | `docs_PRD03/prd-l1.md:17-35` | OLTP read/write path | OLTP compatibility cannot be delegated to OLAP CSR | PRD allows OLTP reads from snapshots |
| OLTP-002 | DirectSource | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java:30` | NodeRecordFormat | record-store layout is a first-class compatibility subject | Rust store chooses a non-record OLTP format and compatibility still passes |
| OLTP-003 | DirectSource | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:40` | RecordRelationshipTraversalCursor | traversal semantics depend on record cursor behavior | Cypher traversal correctness can be proven without matching cursor semantics |
| OLTP-004 | DirectSource | `gitrefrepo/neo4j-src/community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java:685` | dense_node_threshold | dense node behavior is configurable and must be tested | threshold is irrelevant to user-visible behavior |
| OLTP-005 | NeedsSource | `gitrefrepo/neo4j-src/community` | transaction log, locks, checkpoints | line-level WAL/recovery/lock contracts remain open | exact source audit fills all NeedsSource rows |

## Verification Commands

```bash
rg -n "nodes|relationships|properties|WAL|locks|checkpoints|dense nodes|relationship groups|property blocks|dynamic records" docs_PRD03/implementation-readiness/OLTP-Record-Store-Rust-Contract.md
rg -n "NodeRecordFormat|RelationshipRecordFormat|PropertyRecordFormat|RecordRelationshipTraversalCursor|dense_node_threshold" gitrefrepo/neo4j-src/community
```


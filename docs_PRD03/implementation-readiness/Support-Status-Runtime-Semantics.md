# Support Status Runtime Semantics

This file defines how the GDS procedure registry is allowed to behave at
runtime. The goal is honest compatibility: v003 must never pretend a procedure
is supported merely because its name appears in the registry.

## PRD Plane

| plane | relevance |
| --- | --- |
| OLTP storage | unsupported procedure calls must not mutate OLTP accidentally |
| Projection Build Store | estimate/project/catalog calls may create build-control metadata only when support status allows it |
| OLAP snapshot storage | implemented GDS procedures read only published snapshot generations |

## Status Table

| support_status | registry behavior | runtime behavior | user-visible rule | promotion gate |
| --- | --- | --- | --- | --- |
| P0-RegisteredCompatible | row exists with facade family, mode, source path, and deterministic result/error shape | may return metadata, estimate shell, or deterministic unsupported response without running kernel work | visible as known surface, not silently absent | canary proves name, mode, config parsing, and error envelope |
| P1-ImplementedExactLowRam | row exists and links to implemented kernel, estimate, result schema, and canary | executes exact algorithm under memory budget or rejects before execution | supported for production claims | correctness oracle, flat/tile parity where relevant, memory estimate, benchmark smoke |
| P2-ImplementedLater | row exists but cannot be marketed as supported | returns deterministic unsupported response with future-support class | visible roadmap, no accidental success | architecture spike plus formula book row plus source-backed schema |
| NeedsArchitectureSpike | row exists only if inventory contains it, but no runtime support promise | returns deterministic unsupported response | explicit research gap | architecture note resolves storage, scratch, or artifact shape |
| UnsupportedButRegistered | row exists because full surface inventory found it | returns `Neo.ClientError.Procedure.ProcedureCallFailed` with stable message shape | caller learns the procedure is known but unsupported | reclassified to P1/P2/NeedsArchitectureSpike |
| ExplicitlyOutOfScope | row may exist in research registry, but production procedure dispatch may hide it | production may return procedure not found | only for test, private, or intentionally excluded surface | PRD or compatibility decision changes |

## Unsupported Error Envelope

Unsupported-but-known procedures should return a deterministic error class and
message. The registry currently uses:

```text
unsupported_error_code = Neo.ClientError.Procedure.ProcedureCallFailed
unsupported_message_shape = procedure registered but not implemented in v003 support class
```

Unknown procedures should remain distinct from unsupported known procedures:

```text
unknown procedure -> Neo.ClientError.Procedure.ProcedureNotFound
known unsupported procedure -> Neo.ClientError.Procedure.ProcedureCallFailed
```

## Estimate Rule

`estimate` variants are first-class ABI. A procedure cannot become
`P1-ImplementedExactLowRam` until estimate behavior exists and rejects before
execution when:

```text
estimated_required_bytes > configured_budget_bytes
```

The estimate result must separate heap, page-cache policy, direct buffers,
topology, sidecars, result artifacts, model artifacts, scratch, spill, retained
generations, and algorithm state where those categories apply.

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| SEM-001 | DirectSource | `docs_PRD03/prd-l1.md:17-35` | OLTP/OLAP/build path split | support behavior must preserve serving-plane boundaries | later PRD allows GDS reads to query build store directly |
| SEM-002 | DirectSource | `docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv` | support_status column | runtime dispatch can be generated from registry rows | registry is not kept in sync with procedure dispatch |
| SEM-003 | DirectSource | `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-41` | facade methods | compatibility is facade/catalog/mode surface, not kernels alone | all user clients call only one direct kernel API |
| SEM-004 | DirectSource | `docs_PRD03/reference-learning/Batch-10-GDS-Projection-Internals-And-Support-Tiers.md:168-170` | ProcedureExecutor and MemoryEstimations | estimate and execution should stay phase-separated | a simpler direct execution path proves equivalent semantics |

## Verification Commands

```bash
rg -n "P0-RegisteredCompatible|P1-ImplementedExactLowRam|P2-ImplementedLater|UnsupportedButRegistered|ExplicitlyOutOfScope" docs_PRD03/implementation-readiness/Support-Status-Runtime-Semantics.md
awk -F '\t' 'NR>1 {c[$12]++} END {for (k in c) print k,c[k]}' docs_PRD03/implementation-readiness/GDS-Procedure-Support-Registry.tsv
```


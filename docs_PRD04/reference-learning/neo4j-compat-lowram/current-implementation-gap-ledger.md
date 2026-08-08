# Current Implementation Gap Ledger

**Purpose:** Prevent the guiding-light spec from confusing existing Knight Bus code with the complete A007 product contract.  
**Evidence method:** `codebase-memory-mcp` structural search followed by exact symbol retrieval on 2026-08-08.  
**Authority order:** Current code and tests, then generated receipts, then existing specifications and research notes.

## Executive Finding

Knight Bus already has a credible **compatibility proof seed** and a credible **bounded snapshot-construction seed**. It does not yet have the founder-level contract of pre-admission total-working-set prediction, enforced process-level memory ceilings, four-way plan selection, calibrated prediction error, and complete before/during/after receipts.

This distinction is strategically useful. The next spec should extend working proof rather than relabeling partial metrics as deterministic compute.

## Evidence Ledger

| ID | Current evidence | What it proves | What it does not prove |
|---|---|---|---|
| KW-CURRENT-001 | `src/bolt.rs:210`, `KnightBusBoltBackend::execute` | A production driver can send a constrained Cypher neighborhood query over Bolt and receive rows plus a query/plan/snapshot/result-hash receipt. Parsing and execution time, row count, termination, deadline, and maximum-row controls exist. | No pre-run memory admission, fit/spill/approximate/refuse choice, measured RSS high-water mark, I/O receipt, or estimator calibration. The success receipt explicitly records the resource high-water status as unavailable. |
| KW-CURRENT-002 | `tests/bolt_driver_contract.py:289`, `test_query_summary_carries_redacted_execution_receipt_now` | The official Python driver sees a redacted receipt in summary metadata; secret parameter values are excluded. | This is one constrained neighborhood-walk profile, not general Cypher or general Neo4j compatibility. It verifies receipt shape, not resource enforcement. |
| KW-CURRENT-003 | `src/bolt.rs:145`, `KnightBusBoltBackend::new_with_execution_limits` | Startup validates query deadline range and result-row limits, validates the graph profile, and hashes snapshot identity. | Deadline and output-cardinality bounds are not memory bounds. The constructor does not accept a hard RSS/cgroup ceiling. |
| KW-CURRENT-004 | `src/gds/catalog.rs:285`, `MemoryEstimate` | GDS projection metadata distinguishes topology references, duplicate topology, sidecars, catalog metadata, heap, page cache, direct-I/O buffers, algorithm state, delta overlays, and scratch bytes. | The model lacks explicit estimate range/confidence, output/conversion terms, operating-system safety margin, spill I/O/runtime, plan selection, measured error, and per-algorithm calibrated coefficients. |
| KW-CURRENT-005 | `src/gds/execution.rs:983`, `memory_estimate_detail_map_now`; `src/gds/execution.rs:1030`, `memory_estimate_tree_view_now` | Estimate details can cross the GDS-compatible execution surface in structured and tree forms. | Presentation does not enforce the estimate or reserve the stated memory. |
| KW-CURRENT-006 | `src/main.rs:317`, `parse_memory_budget_now`; `src/types.rs:554`, `BuildMemoryBudget` | CLI users can declare a memory amount; invalid values below one MiB are rejected; a deterministic spill-buffer share is derived. | The value is primarily an internal buffer-sizing budget, not a whole-process peak-RSS guarantee. |
| KW-CURRENT-007 | `src/low_ram.rs:610-1317`, external-run construction and verification phases | Snapshot building and verification use bounded-size runs and scratch files across node-key, edge-source, key-resolution, emission, and verification phases. | Existing evidence does not establish an always-enforced process ceiling for every allocator, mapping, runtime, output, and algorithm phase. |
| KW-CURRENT-008 | `src/low_ram.rs:1685`, `current_process_rss_bytes_now`; `src/low_ram.rs:1692`, `peak_rss_measurement_now`; `src/bench.rs:326`, `peak_rss_measurement_now` | The implementation can measure process RSS/peak RSS on supported platforms and report the source. | Measurement after allocation is not admission control. Platform semantics and process-versus-child scope require explicit receipt fields and calibration tests. |
| KW-CURRENT-009 | `tests/gds_projection_catalog_contract.rs:198`, `projection_memory_estimate_avoids_duplicate_topology_now` | A test prevents projection accounting from charging topology that the projection can reference in place. | Avoiding duplicate topology is only one estimator term and does not validate total predicted versus observed peak memory. |

## A007 Contract Gap Matrix

| Founder contract | Current state | Required next proof |
|---|---|---|
| Portable artifact plus manifest | Partial: snapshot identity and generation are hashed. | Versioned artifact manifest with full relevant cardinalities, representation bytes, and compatibility profile. |
| Total working-set estimate | Partial: projection estimate has several meaningful byte categories. | Algorithm-specific fixed/per-node/per-edge/frontier/output/conversion/spill/OS model with range and confidence. |
| `fit` plan | Not yet demonstrated as admission plus enforcement. | Reject the run if its honest upper bound cannot fit under the declared ceiling; reserve or account for all material states. |
| `spill` plan | Partial: external runs exist for snapshot construction. | Planner-selected spill execution for a named OLAP algorithm with bounded buffers, temp-space estimate, I/O receipt, and cleanup tests. |
| `approximate` plan | Not established in the current compatibility slice. | Explicit error/quality contract, deterministic seed policy, and exact-versus-approximate differential tests. |
| `refuse` plan | Partial: invalid config, unsupported Cypher, transactions, timeout, and row limits can fail. | Pre-execution resource refusal with reason, estimate, alternatives, and no partial output. |
| Hard memory ceiling | Missing at the product level. | Cgroup/RLIMIT or supervised-process enforcement plus allocator/mmap accounting and overshoot quality gate. |
| Before receipt | Partial. | Full artifact, estimator, plan, ceiling, I/O/temp, and runtime-range fields. |
| During receipt | Minimal. | Phase/progress, live high-water mark, bytes read/written/mapped/spilled, cancellation and cold/warm state. |
| After receipt | Partial hashes/timings/cardinality; RSS exists elsewhere. | Unified peak and retained memory, prediction error, I/O, approximation bound, and all version identifiers. |
| Neo4j adoption adapter | Working seed for one Bolt/Cypher neighborhood profile. | Founder-gated profiles for named security/IAM/dependency workflows, differential oracles, and an explicit unsupported surface. |
| Product evidence | Technical only. | Design-partner evidence that boundedness and receipts solve a paid problem; otherwise narrow or kill the thesis. |

## Specification Consequence

The mega spec should not begin with “implement Neo4j.” It should begin with the execution admission and proof protocol, then attach the smallest Neo4j-shaped adapter needed for each chosen workload:

1. Artifact and workload profile.
2. Full-working-set model.
3. `fit`, `spill`, `approximate`, or `refuse` decision.
4. Enforced run.
5. Differential correctness oracle.
6. Before/during/after receipt.
7. Neo4j/Bolt/Cypher/GDS compatibility at the boundary.

Anything that does not strengthen this loop is an oracle, deferred surface, or rejected breadth until founder evidence changes the priority.

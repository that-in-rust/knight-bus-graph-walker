# Checkpoint Pruning Between Stalls

- Pattern ID: `PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can observe storage waits without blocking its search thread.",
      "The maintenance task is deterministic between checkpoints."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source preserves pruning output through complete checkpoints.",
      "Checkpoint state is small in the reported setup."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-006"
    ],
    "text": "A007 can admit background graph maintenance into measured storage-wait windows only if the receipt includes queue depth, checkpoint bytes, a conservative interruption point, and a fallback that immediately yields to foreground work.",
    "uncertainty": "Available windows and total resident queue state require workload-specific measurement."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004"
    ],
    "text": "Submit a search hop's reads asynchronously, execute one permitted update subtask while I/O is outstanding, save and re-enqueue unfinished pruning, then poll completions and resume search.",
    "uncertainty": "A cache hit can bypass the kernel I/O path and shorten or remove the available window."
  },
  "confidence_rationale": {
    "assumptions": [
      "The evaluated implementation follows the published algorithm."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm 1 enumerates saved mutable state.",
      "The appendix reports checkpoint overhead."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-006"
    ],
    "text": "Confidence is high in the stated checkpoint invariant and moderate in resource benefit because the paper gives an algorithm, correctness argument, and benchmarks, but this campaign did not inspect code or reproduce execution.",
    "uncertainty": "No independent implementation evidence is available."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Each task owns an immutable sorted candidate pool and checkpoint state containing its partial result, one status flag per candidate, and two cursors; tasks wait in a shared FIFO queue.",
    "uncertainty": "Queue and checkpoint container layouts are not specified."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "The source identifies I/O-bound update designs as offering limited benefit because their update work cannot productively fill CPU-idle search windows.",
      "uncertainty": "This is a scope boundary, not an impossibility proof."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS",
  "falsifying_test": {
    "controlled_variables": [
      "Candidate order, distances, degree bound, threshold, and interruption positions."
    ],
    "failure_signal": "Any resumed neighbor set or candidate-status transition differs from the uninterrupted oracle.",
    "fixture": "One small fixed candidate pool whose pruning is interrupted after each possible inner-loop position.",
    "independent_oracle": "The same pruning procedure executed once without interruption.",
    "scope": "Checkpoint completeness and deterministic resumption only."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The immutable candidate pool plus the saved result, candidate-status flags, and two loop cursors capture all mutable pruning state, so resumed execution returns the same neighbors as uninterrupted execution.",
    "uncertainty": "The proof relies on per-vector candidate pools not being modified concurrently."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus exposes all mutable loop state and freezes task inputs across suspension."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source checkpoints deterministic local graph-neighbor pruning."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Dynamic graph-index maintenance and deterministic neighborhood-pruning loops are the closest Knight Bus families.",
      "uncertainty": "Transfer beyond graph ANNS has not been tested."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-004"
    ],
    "text": "Decompose an update into independent per-vector tasks; execute pruning inside an idle-window budget; on expiration save the complete loop state, yield to search, and re-enqueue the continuation.",
    "uncertainty": "Candidate preparation and finalization are treated as short enough not to require intra-vector checkpoints."
  },
  "name": "Checkpoint Pruning Between Stalls",
  "pattern_id": "PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A complete neighbor-pruning operation commonly outlasts a search I/O stall, so running it monolithically spills into latency-sensitive search computation.",
    "uncertainty": "The duration mismatch is reported for the evaluated graph-ANNS configurations."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "After restoration, pruning continues from the saved inner-loop position and updates only the remaining candidate decisions rather than restarting the task.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-BOUND-OVERRUN-FROM-SAMPLES",
    "PAT-TUNE-IDLE-WINDOW-UTILIZATION"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "The update queue, immutable per-task candidate pools, and saved pruning checkpoints coexist with the underlying index's search state.",
    "uncertainty": "Whole-process resident memory is not itemized."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "The execution protocol reuses search-side outstanding I/O windows and does not require checkpoint persistence to storage.",
      "measurement_needed": "Measure search and update bytes separately with cache hits and misses identified.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "Update-side index I/O and cache effects remain backend-dependent."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure durable graph-record writes and any write-ahead metadata per completed update.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "LIOS changes scheduling rather than index format, but durable write amplification is not itemized."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure affected-vector discovery and candidate-pool preparation before queue admission.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate preparation cost for constructing each immutable candidate pool and affected-vector set."
    },
    "ram": {
      "assumptions": [],
      "expression": "Resident overhead includes queued task metadata, immutable candidate pools, and one compact checkpoint per suspended pruning task; the reported checkpoints are KiB-scale.",
      "measurement_needed": "Measure peak RSS by queued, running, and suspended task count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "Queue depth and whole-process RSS are not reported as a general bound."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Temporary state is the partial result, candidate-status array, and two cursors retained across yields for each suspended pruning task.",
      "measurement_needed": "Measure checkpoint bytes and queue capacity across candidate-pool sizes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "Capacity and allocator overhead depend on candidate-pool size."
    }
  },
  "source_domain": "concurrent updates in disk-based graph approximate nearest-neighbor indexes",
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-vector task decomposition and checkpointed continuation when an idle-window budget expires.",
      "locator_type": "SECTION",
      "locator_value": "Sections 4.1-4.2, overview and decomposition",
      "page": 4,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Checkpoint contents, immutable candidate pool, save/yield point, and restoration schedule.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1, Resumable Pruning",
      "page": 5,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Resumed pruning returns the same neighbor set as uninterrupted pruning.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 4.2, correctness continuation",
      "page": 6,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Submit-execute-poll search integration, shared FIFO tasks, time-budgeted execution, and re-enqueueing.",
      "locator_type": "SECTION",
      "locator_value": "Section 5, Implementation",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "The mechanism targets compute-bound graph-ANNS updates; I/O-bound updates leave limited co-execution opportunity.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 7, Related Work, update-system boundary",
      "page": 12,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Checkpoint state remains at KiB scale in the reported configurations.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 12 and Appendix A, Experiment 10",
      "page": 15,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004"
    ],
    "text": "Search-hop records arrive from storage while the search thread consumes queued CPU update work.",
    "uncertainty": "The paper implements the protocol for graph-ANNS record reads, not arbitrary graph storage."
  },
  "unknown_when": [
    {
      "assumptions": [
        "No uncited section of the fully read paper resolves the named boundary."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The cited source pointers delimit the mechanism, evaluated conditions, or stated analysis."
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "The paper does not establish correctness for checkpointed graph updates whose candidate pools or required mutable state can change concurrently after task creation.",
      "uncertainty": "Additional synchronization or snapshot semantics may be required."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Affected-vector pruning tasks are independent after their candidate pools are fixed, and search produces I/O stalls long enough to execute useful checkpointable work.",
      "uncertainty": "Opportunity varies with cache hits, search algorithm, and backend."
    }
  ]
}
```

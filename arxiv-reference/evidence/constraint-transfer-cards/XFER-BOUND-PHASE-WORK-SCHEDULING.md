# Bound Phase Work Scheduling

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Knight Bus may use mapped or disk-backed graph data instead of a fully resident CSR."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:resource_model.io",
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:streamed_state"
      ],
      "text": "The analogy fails if external I/O or page-fault cost dominates the in-memory work-balancing effect.",
      "uncertainty": "The source evaluates memory traffic rather than secondary-storage traffic."
    },
    {
      "assumptions": [
        "Frontier phases may differ from the source's evaluated power-law workloads."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:works_when",
        "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL"
      ],
      "text": "The analogy fails when the observed frontier does not exhibit a tiny high-degree phase, a broad bottom-up phase, or a low-degree tail that justifies the specialized work unit.",
      "uncertainty": "No universal phase detector is available."
    },
    {
      "assumptions": [
        "The target execution may require deterministic ordering or may run on a NUMA topology."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:unknown_when"
      ],
      "text": "The analogy fails if work stealing, atomic updates, or memory placement cannot satisfy the target correctness and reproducibility contract within the declared concurrency bound.",
      "uncertainty": "Deterministic scheduling, NUMA placement, and contention bounds are not established by the source."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-BOUND-PHASE-WORK-SCHEDULING",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL",
      "response": "Cap P_ready and P_total, observe per-worker useful work and steal counts by level, and revert to the simpler level-synchronous schedule when phase telemetry does not justify specialization. Keep phase thresholds, effective contention, and the fallback decision in the receipt."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "The selected algorithm is level-synchronous and has an independent exact oracle."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:invariant",
        "docs_PRD04/A007-spc-founder-interview-prep-v7.md:product contract"
      ],
      "text": "Admission must reserve frontier, bitmap, predecessor, partition, queue, and worker state before enabling phase-specific scheduling.",
      "uncertainty": "The target byte widths and runtime overhead constants are unmeasured."
    },
    {
      "assumptions": [
        "Phase specialization is optional rather than required for correctness."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL:repair_options"
      ],
      "text": "Execution must cap partition count and queue growth and retain a simpler exact fallback whenever observed frontier shape or scheduler overhead leaves the admitted envelope.",
      "uncertainty": "The crossover between specialization and fallback requires later controlled measurement."
    },
    {
      "assumptions": [
        "Phase telemetry can be collected without changing graph-result semantics."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "docs_PRD04/A007-spc-founder-interview-prep-v7.md:during execution",
        "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL:measurement_needed"
      ],
      "text": "The receipt must record selected work units, partition high-water counts, steal counts, per-worker useful work, fallback events, and modeled versus observed scheduler state.",
      "uncertainty": "Telemetry overhead is an unknown term."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "At most P_ready partitions and Q_steal queue entries are live for W workers."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = P_ready * b_partition_descriptor + W * b_worker_state + Q_steal * b_queue_entry + c_scheduler_runtime",
      "measurement_needed": "Trace maximum ready partitions, queue entries, worker-local state, and scheduler runtime bytes by BFS level.",
      "uncertainty": "Work-stealing implementation and contention can change c_scheduler_runtime.",
      "unknown_constants": [
        "c_scheduler_runtime"
      ],
      "variables": [
        {
          "definition": "Maximum simultaneously ready partitions",
          "symbol": "P_ready",
          "units": "partitions"
        },
        {
          "definition": "Maximum work-stealing queue entries",
          "symbol": "Q_steal",
          "units": "entries"
        },
        {
          "definition": "Worker count",
          "symbol": "W",
          "units": "workers"
        },
        {
          "definition": "Bytes per queue entry",
          "symbol": "b_queue_entry",
          "units": "bytes per entry"
        },
        {
          "definition": "Bytes per partition descriptor",
          "symbol": "b_partition_descriptor",
          "units": "bytes per partition"
        },
        {
          "definition": "Bytes of local state per worker",
          "symbol": "b_worker_state",
          "units": "bytes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Top-down and bottom-up scans preserve the same exact traversal semantics."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = E_td_scanned * b_edge * c_td_read_amplification + V_bu_scanned * b_vertex_scan * c_bu_read_amplification + S_scheduler_spill",
      "measurement_needed": "Record physical bytes read, page faults, scanned edges and vertices, and any scheduler spill separately for each phase.",
      "uncertainty": "The source reports in-memory cache behavior, so both target read-amplification constants are unknown.",
      "unknown_constants": [
        "c_bu_read_amplification",
        "c_td_read_amplification"
      ],
      "variables": [
        {
          "definition": "Top-down edges scanned",
          "symbol": "E_td_scanned",
          "units": "edges"
        },
        {
          "definition": "Bytes per stored edge",
          "symbol": "b_edge",
          "units": "bytes per edge"
        },
        {
          "definition": "Bytes touched per bottom-up vertex scan",
          "symbol": "b_vertex_scan",
          "units": "bytes per vertex"
        },
        {
          "definition": "Scheduler spill bytes",
          "symbol": "S_scheduler_spill",
          "units": "bytes"
        },
        {
          "definition": "Bottom-up vertices scanned",
          "symbol": "V_bu_scanned",
          "units": "vertices"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The transfer needs only graph and degree profiling plus bounded partition planning, not the source's full reordering pipeline."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_pre = (N_v + N_e) / c_profile_scan_rate + P_total / c_partition_plan_rate",
      "measurement_needed": "Measure graph-profile scan time and partition-plan construction time independently on the admitted artifact.",
      "uncertainty": "Profile and partition-plan rates depend on representation and storage path.",
      "unknown_constants": [
        "c_partition_plan_rate",
        "c_profile_scan_rate"
      ],
      "variables": [
        {
          "definition": "Stored edge count",
          "symbol": "N_e",
          "units": "edges"
        },
        {
          "definition": "Stored vertex count",
          "symbol": "N_v",
          "units": "vertices"
        },
        {
          "definition": "Total planned partitions",
          "symbol": "P_total",
          "units": "partitions"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "The graph representation is accounted separately as R_graph_resident or through mapped-page accounting."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak = R_graph_resident + R_predecessor + N_bitmap*N_v*b_bitmap_vertex + P_ready*b_partition_descriptor + Q_steal*b_queue_entry + W*b_worker_state + c_scheduler_runtime + c_allocator_ram + c_runtime_ram",
      "measurement_needed": "Measure peak whole-process RSS and attribute graph, predecessor, bitmap, partition, steal-queue, scheduler, worker, allocator, and runtime bytes by phase.",
      "uncertainty": "Mapped pages, allocator behavior, and runtime state are not bounded by the source card.",
      "unknown_constants": [
        "c_allocator_ram",
        "c_runtime_ram",
        "c_scheduler_runtime"
      ],
      "variables": [
        {
          "definition": "Maximum work-stealing queue entries live at once",
          "symbol": "Q_steal",
          "units": "entries"
        },
        {
          "definition": "Number of vertex-length bitmaps required by the selected phase",
          "symbol": "N_bitmap",
          "units": "bitmaps"
        },
        {
          "definition": "Stored vertex count",
          "symbol": "N_v",
          "units": "vertices"
        },
        {
          "definition": "Maximum simultaneously ready partitions",
          "symbol": "P_ready",
          "units": "partitions"
        },
        {
          "definition": "Resident graph bytes charged to RSS",
          "symbol": "R_graph_resident",
          "units": "bytes"
        },
        {
          "definition": "Predecessor-state bytes",
          "symbol": "R_predecessor",
          "units": "bytes"
        },
        {
          "definition": "Worker count",
          "symbol": "W",
          "units": "workers"
        },
        {
          "definition": "Bytes per bitmap vertex entry",
          "symbol": "b_bitmap_vertex",
          "units": "bytes per vertex"
        },
        {
          "definition": "Bytes per partition descriptor",
          "symbol": "b_partition_descriptor",
          "units": "bytes per partition"
        },
        {
          "definition": "Bytes per work-stealing queue entry",
          "symbol": "b_queue_entry",
          "units": "bytes per entry"
        },
        {
          "definition": "Bytes of local state per worker",
          "symbol": "b_worker_state",
          "units": "bytes per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Phase metadata and traces are retained only when requested by the receipt policy."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_bytes = S_graph + P_total * b_partition_descriptor_disk + S_phase_trace + c_layout_storage",
      "measurement_needed": "Measure retained graph, partition descriptor, trace, and filesystem metadata bytes after planning and after cleanup.",
      "uncertainty": "The source does not report persistent scheduler metadata because its schedule is in-memory.",
      "unknown_constants": [
        "c_layout_storage"
      ],
      "variables": [
        {
          "definition": "Total planned partitions",
          "symbol": "P_total",
          "units": "partitions"
        },
        {
          "definition": "Retained graph bytes",
          "symbol": "S_graph",
          "units": "bytes"
        },
        {
          "definition": "Retained phase trace bytes",
          "symbol": "S_phase_trace",
          "units": "bytes"
        },
        {
          "definition": "Persistent bytes per partition descriptor",
          "symbol": "b_partition_descriptor_disk",
          "units": "bytes per partition"
        }
      ]
    }
  },
  "name": "Bound Phase Work Scheduling",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-033"
      ],
      "text": "Threads scan CSR adjacency ranges in top-down phases and contiguous vertex partitions with bitmap state in bottom-up phases.",
      "uncertainty": "The cited access path is memory traffic, not external-storage streaming."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032"
      ],
      "text": "The computation is shared-memory, level-synchronous BFS with static edge or vertex assignment in top-down phases and dynamic partition stealing in the bottom-up phase.",
      "uncertainty": "Concurrent ordering among stolen partitions is not specified as deterministic."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-034"
      ],
      "text": "The mechanism constrains thread imbalance by changing work units while paying bitmap, partition-descriptor, frontier, and work-stealing state.",
      "uncertainty": "Descriptor bytes, thread-local bytes, and a portable contention bound are not supplied."
    },
    "data_mutability": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-033"
      ],
      "text": "Graph topology remains fixed during traversal while visited, predecessor, current-frontier, next-frontier, and partition assignment state change by BFS level.",
      "uncertainty": "Dynamic graph mutation is outside the card's evaluated scope."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-035"
      ],
      "text": "The source mechanism assumes an in-memory CSR on a shared-memory multicore system that supports parallel threads, cache-line-aligned bitmaps, atomic or disjoint updates, and work stealing.",
      "uncertainty": "The card does not establish deterministic scheduling, NUMA placement, or behavior on a disk-backed graph."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-034"
      ],
      "text": "The scheduler relies on phase-specific frontier shape and a tuned partition factor to keep useful work balanced without excessive stealing.",
      "uncertainty": "The source supplies empirical tuning rather than a universal phase detector or contention bound."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-032"
      ],
      "text": "A tiny high-degree frontier is balanced by splitting adjacency ranges, while a broad bottom-up frontier is divided into more partitions than workers and scheduled by stealing.",
      "uncertainty": "The three-phase characterization is workload-specific."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
      ],
      "source_pointer_ids": [
        "SP-032",
        "SP-034"
      ],
      "text": "Bottom-up scheduling requires vertex-length bitmap state and pays increasing work-stealing overhead as partitions become too fine.",
      "uncertainty": "No portable partition-count threshold is reported."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "Per-phase useful work and scheduler operations can be counted separately."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:resource_model",
      "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:access_schedule"
    ],
    "text": "C_original = k_td * E_td + k_bu * V_bu + k_steal * Q_steal + k_sync * L_levels. Variables E_td, V_bu, Q_steal, and L_levels count top-down edges, bottom-up vertices, steals, and levels. Unknown constants k_td, k_bu, k_steal, and k_sync require per-level work, steal, and synchronization measurements.",
    "uncertainty": "The source does not provide portable coefficients or an external-I/O term."
  },
  "original_domain": "multicore hybrid breadth-first search",
  "proposed_transfer": {
    "assumptions": [
      "Knight Bus has an exact level-synchronous traversal path and can observe frontier shape before choosing each level's schedule."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:invariant",
      "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL:repair_options",
      "docs_PRD04/A007-spc-founder-interview-prep-v7.md:fit spill approximate refuse receipt"
    ],
    "text": "Treat phase-specific scheduling as a bounded plan branch: reserve the maximum allowed frontier, bitmap, partition, queue, and worker state; choose an edge-range, partition-stealing, or simple vertex schedule from observed work shape; cap scheduler state; and fall back or refuse when the bound or phase premise fails.",
    "uncertainty": "This is not an architecture selection, and no target crossover or performance effect has been measured."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "Knight Bus may execute with mapped or streamed topology."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:resource_model.io",
        "G07-goal-packet.md:Current Knight Bus Grounding"
      ],
      "text": "The graph can no longer be assumed fully resident, so storage reads, mapped pages, and scheduler spill must be explicit resource terms.",
      "uncertainty": "Target phase I/O amplification is unmeasured."
    },
    {
      "assumptions": [
        "Target core count, cache sharing, and memory placement are deployment inputs."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:unknown_when",
        "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL:breakpoint_equation"
      ],
      "text": "The source platform's thread, cache, and tuning conditions are not portable constants; effective contention and partition count must be measured on the target execution environment.",
      "uncertainty": "No target hardware calibration exists."
    },
    {
      "assumptions": [
        "Frontier evolution is observed during execution."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:works_when",
        "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS:unknown_when"
      ],
      "text": "The three source phases cannot be assumed at admission from an algorithm name alone; the schedule must use runtime telemetry and preserve an exact fallback.",
      "uncertainty": "The smallest sufficient telemetry set is unknown."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "direction-switch policy",
      "graph",
      "partition cap",
      "source vertex",
      "thread count",
      "steal-queue capacity"
    ],
    "failure_signal": "The schedule changes exact BFS distances, steal-queue or scheduler high-water exceeds the admitted phase bound, or the plan remains selected after its imbalance or steal limit is violated.",
    "fixture": "A small power-law graph whose levels contain a tiny high-degree frontier, a broad bottom-up frontier, and a low-degree tail.",
    "independent_oracle": "Sequential level-synchronous BFS distances plus a bounded reference scheduler, per-level work, steal-queue high-water, scheduler-state, and RSS accounting."
  },
  "source_pattern_ids": [
    "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
    ],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "Every current-level BFS work item is assigned exactly within the selected phase schedule, while atomic or disjoint bitmap updates preserve visitation and predecessor semantics.",
    "uncertainty": "The source provides an operational correctness argument rather than a formal theorem."
  },
  "target_algorithm_families": [
    "BFS",
    "LEVEL_SYNCHRONOUS_TRAVERSAL",
    "SHORTEST_PATHS_UNWEIGHTED",
    "WCC"
  ],
  "transfer_id": "XFER-BOUND-PHASE-WORK-SCHEDULING",
  "unknown_measurement_constants": [
    "c_allocator_ram",
    "c_bu_read_amplification",
    "c_layout_storage",
    "c_partition_plan_rate",
    "c_profile_scan_rate",
    "c_runtime_ram",
    "c_scheduler_runtime",
    "c_td_read_amplification"
  ]
}
```

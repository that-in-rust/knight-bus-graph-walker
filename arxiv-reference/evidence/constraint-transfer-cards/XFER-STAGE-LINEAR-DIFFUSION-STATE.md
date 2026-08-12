# Stage Linear Diffusion State

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "text": "Reject the transfer for nonlinear, stateful, or order-dependent operators that lack the source linear decomposition identity.",
      "uncertainty": "No evidence here establishes preservation for other operators."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES",
        "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS"
      ],
      "text": "Reject an exactness claim when residual seeds are omitted, stage arithmetic changes the required semantics, or concurrent stage payloads exceed the quoted multiplicity.",
      "uncertainty": "Floating-point order and stage overlap require independent verification."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-STAGE-LINEAR-DIFFUSION-STATE",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS",
      "response": "Require S_processed to equal support(r) for exact admission, bound residual count and overlapping subgraphs, label any residual selection as approximate with an error contract, and use direct/full staged diffusion or refusal when complete support cannot fit."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES",
        "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS",
        "A007 hard-budget contract"
      ],
      "text": "Exact admission must bound every stage payload and complete residual support; if either cannot be bounded before execution, the exact staged plan must fall back or refuse.",
      "uncertainty": "The residual-support and overlap estimator is not yet calibrated."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS",
        "A007 distinguishes approximate from exact plans."
      ],
      "text": "Residual pruning is permitted only in an explicitly approximate branch with a declared error contract and verification signal.",
      "uncertainty": "No application tolerance or error bound is supplied by this lane."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Concurrent residual contributions use a defined aggregation order.",
        "Exactly one owning worker is admitted per resident stage, so the worker and stage multiplicities both equal stage_concurrency; intra-stage worker fanout is outside this transfer."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = stage_concurrency*(b_stage_worker + b_stage_vertex*V_stage_max + b_stage_edge*E_stage_max + b_stage_score*S_stage_max + b_stage_residual*R_stage_max) + b_scheduler_pair*scheduler_pair_count",
      "measurement_needed": "Measure worker-local payloads, result buffers, scheduler metadata, and shared score-table coordination as concurrency varies.",
      "uncertainty": "Contention, duplicate payload residency, and arithmetic-order effects are unmeasured.",
      "unknown_constants": [
        "b_scheduler_pair",
        "b_stage_edge",
        "b_stage_residual",
        "b_stage_score",
        "b_stage_vertex",
        "b_stage_worker"
      ],
      "variables": [
        {
          "definition": "Simultaneously resident stages and their one-to-one owning workers",
          "symbol": "stage_concurrency",
          "units": "stages and workers"
        },
        {
          "definition": "Maximum vertices in one admitted stage",
          "symbol": "V_stage_max",
          "units": "vertices per stage"
        },
        {
          "definition": "Maximum edges in one admitted stage",
          "symbol": "E_stage_max",
          "units": "edges per stage"
        },
        {
          "definition": "Maximum accumulated-score entries in one admitted stage",
          "symbol": "S_stage_max",
          "units": "scores per stage"
        },
        {
          "definition": "Maximum residual-score entries in one admitted stage",
          "symbol": "R_stage_max",
          "units": "scores per stage"
        },
        {
          "definition": "Concurrent worker pairs requiring coordination",
          "symbol": "scheduler_pair_count",
          "units": "worker pairs"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Every processed residual seed contributes its admitted stage payload.",
        "Repeated overlap is counted rather than assumed away."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = a_topology_read * edge_record_bytes * E_stage_sum + a_score_transfer * score_record_bytes * S_stage_sum",
      "measurement_needed": "Trace logical and physical topology reads, host/device transfers, score transfers, and duplicate edge occurrences by stage.",
      "uncertainty": "Overlap, cache reuse, and transfer amplification are graph- and schedule-dependent.",
      "unknown_constants": [
        "a_score_transfer",
        "a_topology_read"
      ],
      "variables": [
        {
          "definition": "Sum of edge occurrences across processed stage subgraphs",
          "symbol": "E_stage_sum",
          "units": "edge occurrences"
        },
        {
          "definition": "Bytes per transferred or read edge record",
          "symbol": "edge_record_bytes",
          "units": "bytes per edge"
        },
        {
          "definition": "Sum of score entries transferred or materialized across stages",
          "symbol": "S_stage_sum",
          "units": "score entries"
        },
        {
          "definition": "Bytes per score record",
          "symbol": "score_record_bytes",
          "units": "bytes per score"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The graph artifact supports bounded-depth extraction.",
        "All exact residual continuations are enumerated."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare_ns = t_extract_vertex * V_stage_sum + t_extract_edge * E_stage_sum + t_overlap_check * E_overlap_sum",
      "measurement_needed": "Record bounded-depth extraction, adjacency reorganization, overlap detection, and residual-seed enumeration time and counts.",
      "uncertainty": "The number and overlap of residual-seeded subgraphs are not source-bounded.",
      "unknown_constants": [
        "t_extract_edge",
        "t_extract_vertex",
        "t_overlap_check"
      ],
      "variables": [
        {
          "definition": "Sum of vertex occurrences extracted across stages",
          "symbol": "V_stage_sum",
          "units": "vertex occurrences"
        },
        {
          "definition": "Sum of edge occurrences extracted across stages",
          "symbol": "E_stage_sum",
          "units": "edge occurrences"
        },
        {
          "definition": "Repeated edge occurrences checked across stage subgraphs",
          "symbol": "E_overlap_sum",
          "units": "edge occurrences"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Each admitted stage has precomputed vertex, edge, score, and residual bounds.",
        "Serial execution uses stage_concurrency equal to one.",
        "Each resident stage has exactly one owning worker; additional intra-stage workers require another model."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = b_stage_fixed + stage_concurrency*(b_stage_worker + b_stage_vertex*V_stage_max + b_stage_edge*E_stage_max + b_stage_score*S_stage_max + b_stage_residual*R_stage_max) + b_scheduler_pair*scheduler_pair_count",
      "measurement_needed": "Measure host RSS, mapped pages, stage payloads, score tables, residual tables, and overlap at every allowed stage concurrency.",
      "uncertainty": "Subgraph container overhead and host/device duplication are uncalibrated.",
      "unknown_constants": [
        "b_scheduler_pair",
        "b_stage_edge",
        "b_stage_fixed",
        "b_stage_residual",
        "b_stage_score",
        "b_stage_vertex",
        "b_stage_worker"
      ],
      "variables": [
        {
          "definition": "Simultaneously resident stage subgraphs",
          "symbol": "stage_concurrency",
          "units": "stages"
        },
        {
          "definition": "Maximum vertices in one admitted stage subgraph",
          "symbol": "V_stage_max",
          "units": "vertices"
        },
        {
          "definition": "Maximum edges in one admitted stage subgraph",
          "symbol": "E_stage_max",
          "units": "edges"
        },
        {
          "definition": "Maximum local accumulated-score entries in one stage",
          "symbol": "S_stage_max",
          "units": "scores"
        },
        {
          "definition": "Maximum local residual-score entries in one stage",
          "symbol": "R_stage_max",
          "units": "scores"
        },
        {
          "definition": "Concurrent stage-owner pairs requiring scheduler coordination",
          "symbol": "scheduler_pair_count",
          "units": "worker pairs"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Stage payload lifetime is explicit.",
        "Exact execution retains enough residual state to enumerate every continuation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = graph_bytes + residual_checkpoint_bytes + a_stage_payload_overlap * stage_payload_bytes",
      "measurement_needed": "Measure retained graph/checkpoint bytes and peak coexistence of generated stage payloads.",
      "uncertainty": "Whether stage subgraphs are transient, cached, or persisted is a later design choice.",
      "unknown_constants": [
        "a_stage_payload_overlap"
      ],
      "variables": [
        {
          "definition": "Persistent input graph bytes",
          "symbol": "graph_bytes",
          "units": "bytes"
        },
        {
          "definition": "Retained residual and accumulated-score checkpoint bytes",
          "symbol": "residual_checkpoint_bytes",
          "units": "bytes"
        },
        {
          "definition": "Bytes of one materialized stage-payload generation",
          "symbol": "stage_payload_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Stage Linear Diffusion State",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "A host extracts and reorganizes bounded-depth subgraphs, transfers one or more stage payloads to an accelerator, and aggregates returned score contributions.",
      "uncertainty": "Total transfer volume depends on stage count and overlap."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "The CPU prepares subgraphs and controls execution while accelerator processing elements consume subgraph tables and coordinate writes to local score tables.",
      "uncertainty": "Concurrent later-stage execution is not evaluated."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The source constrains on-chip stage-subgraph and local score state by decomposing one L-step local diffusion into smaller bounded-depth subgraphs.",
      "uncertainty": "Host graph memory, stage overlap, and total residual continuations are additional terms."
    },
    "data_mutability": {
      "assumptions": [
        "Publication year is not used to infer mutability."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism evaluates query-local diffusion on an extracted graph.",
        "The mechanism card does not define graph updates during a query."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Concurrent mutation semantics are not established."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-004",
        "SP-005"
      ],
      "text": "The evaluated mechanism uses a CPU host and FPGA accelerator with bounded on-chip tables, host-to-device transfer, fixed numeric representation choices, and an evaluated two-stage configuration.",
      "uncertainty": "Those hardware and arithmetic choices are source-specific and are not imported as modern cost coefficients."
    },
    "predictability_requirement": {
      "assumptions": [
        "Stage execution is serial unless concurrency is separately budgeted."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source card bounds each stage by extracted subgraph size.",
        "The source card leaves subgraph count, overlap, and concurrent execution unmeasured."
      ],
      "text": "Per-stage resident state is predictable only when the vertex, edge, score, and residual counts of every admitted stage are bounded; total work remains dependent on residual support and subgraph overlap.",
      "uncertainty": "No pre-run residual-fanout bound is supplied."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "A full L-hop local subgraph can exceed the tight on-chip memory and preparation-latency target, motivating shorter stage subgraphs.",
      "uncertainty": "The growth rate and fit threshold are fixture-dependent."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Stage decomposition alone does not reduce memory; linear residual decomposition is required, and exact equality includes every nonzero residual-seeded continuation.",
      "uncertainty": "Selecting only part of residual support changes the result from exact to approximate."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "No source-reported memory or speed ratio is reused."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source card identifies stage topology and score tables.",
      "The source card states that host preparation and stage transfers repeat for residual seeds.",
      "The source leaves overlap and host memory unnormalized."
    ],
    "text": "RAM_original_bytes = max_stage(B_topology + B_accumulated + B_residual) * stage_concurrency + B_host; IO_original_bytes = sum(stage_payload_bytes) + sum(score_transfer_bytes); T_prepare follows repeated bounded-depth extraction.",
    "uncertainty": "Stage fanout, overlap, host duplication, and concurrency coefficients are unknown."
  },
  "original_domain": "local personalized PageRank diffusion",
  "proposed_transfer": {
    "assumptions": [
      "Each stage count is bounded before loading.",
      "A later goal chooses the execution realization."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES",
      "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS",
      "A007 full-working-set and receipt requirements"
    ],
    "text": "For eligible local PageRank or linear diffusion jobs, decompose execution into serially admitted bounded-depth stage subgraphs, retain accumulated and complete residual state, and quote peak state from the largest admitted stage plus host and overlap terms. Treat residual pruning as a separate approximate option, and receipt stage counts, repeated topology, residual support, bytes transferred, and peak concurrency.",
    "uncertainty": "The transfer is limited to diffusion operators for which the source linear identity and complete-support condition hold."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "No final compute architecture is selected."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source proves a linear identity.",
        "The source hardware implementation is one realization of that identity."
      ],
      "text": "The transferable invariant is algebraic stage separability, not the source CPU/FPGA split; a modern plan may use another storage or compute path only if it retains the same complete residual decomposition.",
      "uncertainty": "Alternative arithmetic and scheduling can change reproducibility and memory overlap."
    },
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source card leaves stage overlap and total subgraph count unknown.",
        "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS requires complete support for exactness."
      ],
      "text": "A smaller stage payload does not establish a bounded job unless residual count, repeated topology, checkpoints, and permitted stage concurrency are included.",
      "uncertainty": "Pre-run fanout bounds require artifact analysis or refusal."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "arithmetic",
      "decay",
      "residual_support",
      "seed",
      "stage_concurrency",
      "simultaneous stage-owner workers",
      "stage_depths",
      "total_depth"
    ],
    "failure_signal": "Full-support staged output differs from the direct oracle, an exact run omits a residual, more than one owner appears per stage, or aggregate multi-stage worker, payload, scheduler, and overlap state exceeds the resource expression.",
    "fixture": "A small graph where one seed produces two nonzero residual branches, executed serially and with two simultaneously resident stages under the enforced one-owner-per-stage rule.",
    "independent_oracle": "Direct single-stage diffusion using the same arithmetic and total depth, plus per-stage owner, payload, scheduler, aggregate-RSS, and transfer traces."
  },
  "source_pattern_ids": [
    "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
    ],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "For the source's linear diffusion operator, the full L-step result is preserved by combining first-stage accumulated scores with every nonzero residual-seeded later-stage diffusion according to the decomposition identity.",
    "uncertainty": "Exact preservation requires complete residual support and compatible arithmetic."
  },
  "target_algorithm_families": [
    "LOCAL_GRAPH_DIFFUSION",
    "PERSONALIZED_PAGERANK"
  ],
  "transfer_id": "XFER-STAGE-LINEAR-DIFFUSION-STATE",
  "unknown_measurement_constants": [
    "a_score_transfer",
    "a_stage_payload_overlap",
    "a_topology_read",
    "b_scheduler_pair",
    "b_stage_edge",
    "b_stage_fixed",
    "b_stage_residual",
    "b_stage_score",
    "b_stage_vertex",
    "b_stage_worker",
    "t_extract_edge",
    "t_extract_vertex",
    "t_overlap_check"
  ]
}
```

# Bound Overrun From Samples

- Pattern ID: `PAT-BOUND-OVERRUN-FROM-SAMPLES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can collect representative stall samples before admission."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source computes a budget from recent samples under an expected-overrun constraint.",
      "Per-hop overhead is evaluated against a configured target."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-004"
    ],
    "text": "A007 can replace a guessed idle allowance with an empirical receipt containing sample scope, conditioning key, mean idle duration, chosen budget, and observed overrun, while labeling the allowance best-effort rather than deterministic.",
    "uncertainty": "Finite-sample error and distribution drift still require runtime monitoring."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Update the relevant idle-history sample, solve or refresh its budget, and allow an update subtask to run for only the utilization-scaled share of that budget during a selected stall.",
    "uncertainty": "Budget-refresh frequency is implementation-dependent."
  },
  "confidence_rationale": {
    "assumptions": [
      "The benchmark implementation uses the stated estimator."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 4.3 defines a directly testable empirical constraint.",
      "Experiment 2 reports per-hop behavior."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-004"
    ],
    "text": "The estimator and adaptations are explicit and the paper reports mechanism-level measurements, but no code inspection, reproduction, or independent statistical validation occurred.",
    "uncertainty": "The sample history and solver implementation were not independently checked."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Runtime retains recent idle-duration histories and separate BeamSearch buckets by I/O batch size; PipeSearch also tracks the sparse scheduling interval.",
    "uncertainty": "History length and exact container layout are not reported."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "A single undifferentiated history is unsuitable when idle distributions change with I/O concurrency or when right-skewed PipeSearch intervals make scheduling every window overly conservative.",
      "uncertainty": "The proposed buckets and sparse strategy address these source-observed cases."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-BOUND-OVERRUN-FROM-SAMPLES",
  "falsifying_test": {
    "controlled_variables": [
      "Idle-duration sequence, conditioning bucket, sparse interval, and overrun fraction."
    ],
    "failure_signal": "The selected budget violates the empirical constraint or is not the largest admissible budget under the oracle.",
    "fixture": "A short fixed sequence of idle durations with both short and long windows and a declared overrun fraction.",
    "independent_oracle": "Exhaustive evaluation of every candidate budget against the empirical mean-overrun constraint.",
    "scope": "Estimator arithmetic and conditioning only."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The execution budget is chosen so empirical mean excess beyond sampled idle durations stays within a user-controlled fraction of the sampled mean idle time.",
    "uncertainty": "This is a sample-based expected bound, not a per-window hard deadline."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "The target graph kernel exposes interruptible background work and comparable short-term stall stability."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source budgets CPU work from measured foreground I/O stalls."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Disk-resident BFS-like traversal, graph ANNS, and other storage-stalled graph kernels are candidate Knight Bus families when foreground stall windows can be measured online.",
      "uncertainty": "Only graph ANNS is source-evaluated."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Collect recent idle durations, solve the monotone empirical overrun constraint by binary search, condition histories on BeamSearch I/O concurrency, and sparsely schedule PipeSearch intervals when its distribution is heavily skewed.",
    "uncertainty": "Recent samples are assumed to represent the upcoming short window."
  },
  "name": "Bound Overrun From Samples",
  "pattern_id": "PAT-BOUND-OVERRUN-FROM-SAMPLES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The next search idle window has variable, irregular duration, so an update budget chosen from a fixed or exact-prediction model can overrun into foreground computation.",
    "uncertainty": "The distributions are platform and search-method dependent."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A permissible execution budget is recomputed from the current empirical distribution by binary search.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS",
    "PAT-TUNE-IDLE-WINDOW-UTILIZATION"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Recent sample histories, per-concurrency buckets, solved budgets, and sparse-scheduling counters remain resident.",
    "uncertainty": "The paper does not give their byte footprint."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure I/O counters with scheduling disabled and with only the estimator enabled.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Budget computation consumes timing observations, but the paper does not isolate any additional storage traffic caused by the estimator."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Determine whether histories are rebuilt after restart and account for any persisted telemetry.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No durable sample-history format or restart behavior is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "An initial recording phase gathers a no-update baseline and idle samples before co-execution is admitted.",
      "measurement_needed": "Measure recording duration and foregone update work during baseline collection.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Warm-up duration and amortization are not generalized."
    },
    "ram": {
      "assumptions": [],
      "expression": "Resident budgeting state consists of bounded recent duration samples, per-I/O-width buckets, solved budgets, and sparse-scheduling counters.",
      "measurement_needed": "Measure budgeting-state bytes by bucket count and retained sample count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not report history length or bytes."
    },
    "temporary_storage": {
      "assumptions": [
        "The implementation scans or aggregates existing samples without materializing additional distributions."
      ],
      "expression": "Binary-search working state is constant-sized beyond the retained sample histories used to evaluate candidate budgets.",
      "measurement_needed": "Measure allocations during each budget refresh.",
      "premises": [
        "The source solves a scalar monotone constraint by binary search over one budget value."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "DERIVED",
      "uncertainty": "Implementation-specific sorting or summaries could add temporary state."
    }
  },
  "source_domain": "online scheduling inside variable graph-search I/O stalls",
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Recent idle samples, empirical expected-overrun constraint, binary-search solution, and search-specific adaptations.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3, Overrun-Bounded Time Budgeting",
      "page": 6,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Observed short-window stability of idle-time distribution shape.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 6",
      "page": 6,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Runtime applies the solved budget after scaling it by utilization.",
      "locator_type": "SECTION",
      "locator_value": "Section 5, time-budgeted execution",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Source benchmark of per-hop overhead against the configured target.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and Section 6.3, Experiment 2",
      "page": 9,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-004"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Each completed or observed search stall contributes a new duration sample to the online history.",
    "uncertainty": "Sampling instrumentation cost is not isolated."
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
        "SP-001",
        "SP-002"
      ],
      "text": "The paper does not provide a finite-sample confidence guarantee for abrupt distribution shifts beyond the short-window empirical stability observation.",
      "uncertainty": "Reliability under adversarial or bursty shifts is unknown."
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
      "text": "Short-term idle-time distributions remain stable enough that recent samples approximate upcoming windows, with distinct histories used for structurally different search conditions.",
      "uncertainty": "Stability is empirical for the evaluated workloads."
    }
  ]
}
```

# Tune Idle Window Utilization

- Pattern ID: `PAT-TUNE-IDLE-WINDOW-UTILIZATION`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can collect the chosen latency statistic without materially destabilizing foreground execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source changes admitted update utilization from observed end-to-end latency.",
      "The source disables infeasible co-execution and treats the target as best-effort."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-007"
    ],
    "text": "A007 should represent idle-window use as a revocable admission lease carrying the foreground baseline, observed statistic, target, current utilization, violation state, and automatic disable or rebaseline transition instead of a hard latency guarantee.",
    "uncertainty": "Controller cadence, gains, and phase-transition behavior require local measurement."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Run a recording phase, perform an initial feasible-ratio search, enter steady periodic adjustment, and re-enter recording when repeated violations or search failure invalidate the current baseline.",
    "uncertainty": "The duration and frequency of observation and rebaseline intervals are not generalized."
  },
  "confidence_rationale": {
    "assumptions": [
      "The evaluated implementation follows the published control procedure."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper specifies each state-machine phase and reports controller-specific traces and ablation stages."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-005",
      "SP-006",
      "SP-007"
    ],
    "text": "The controller phases, best-effort limitation, utilization trace, cumulative tuning ablation, and statistic sensitivity are explicit, but this campaign did not inspect code or reproduce controller dynamics.",
    "uncertainty": "Independent stability, implementation, and tail-latency evidence are absent."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-007"
    ],
    "text": "The controller retains the configured degradation target, a no-update latency baseline, the current utilization ratio, controller phase, violation history, and the selected observed-latency statistic.",
    "uncertainty": "The source does not specify concrete controller data structures or history lengths."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "When the initial search finds no feasible utilization ratio, the controller disables co-execution rather than admitting update work.",
      "uncertainty": "A later workload phase may permit a feasible ratio after re-recording."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "Short fluctuations or baseline drift can produce transient or phase-average target violations before utilization is reduced or the baseline is rebuilt.",
      "uncertainty": "Violation magnitude depends on the observed statistic and control cadence."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-TUNE-IDLE-WINDOW-UTILIZATION",
  "falsifying_test": {
    "controlled_variables": [
      "Latency target",
      "Baseline samples",
      "Observed statistic",
      "Utilization increment",
      "Violation sequence",
      "Baseline refresh points"
    ],
    "failure_signal": "Any controller transition or utilization direction differs from the oracle, or nonzero co-execution remains enabled when no feasible ratio exists.",
    "fixture": "A deterministic sequence of baseline and co-execution latency observations containing under-target periods, isolated violations, repeated drift, and a phase with no feasible nonzero utilization.",
    "independent_oracle": "A hand-evaluated trace of the source's recording, feasibility-search, steady, disable, and rebaseline state transitions.",
    "scope": "Controller state-machine behavior only; no performance experiment is created."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The controller raises utilization only while observed latency is within the configured ratio to a no-update baseline, lowers utilization after a violation, disables co-execution if no feasible ratio exists, and rebuilds the baseline after sustained drift or search failure.",
    "uncertainty": "The target is best-effort at phase level rather than a hard limit for every query sample."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus exposes a stable foreground latency signal and independently interruptible background work."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source controls the share of measured search-idle time assigned to background graph-index updates from observed foreground latency."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Latency-sensitive disk-resident graph search with interruptible background index maintenance is the closest Knight Bus family.",
      "uncertainty": "The source evaluates graph approximate-nearest-neighbor search rather than other graph algorithm families."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-005"
    ],
    "text": "Suspend update scheduling to record a foreground baseline, search for an initial feasible utilization ratio, adjust that ratio upward or downward from periodic latency observations in steady state, and return to recording after repeated violations or search failure.",
    "uncertainty": "Adjustment increments, observation windows, and refresh cadence affect responsiveness and overshoot."
  },
  "name": "Tune Idle Window Utilization",
  "pattern_id": "PAT-TUNE-IDLE-WINDOW-UTILIZATION",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A per-hop execution allowance cannot model every workload- and runtime-dependent contribution to end-to-end search latency, so the usable update share cannot be fixed safely in advance.",
    "uncertainty": "The magnitude and timing of unmodeled effects depend on the backend and workload phase."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The no-update baseline and an initial feasible utilization ratio are recomputed during recording and feasibility-search phases, including after a rebaseline trigger.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-BOUND-OVERRUN-FROM-SAMPLES",
    "PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS",
    "PAT-PREFETCH-DISPLACED-SEARCH-STATE"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The target, baseline summary, current utilization ratio, phase, and violation counter remain resident while co-execution is controlled.",
    "uncertainty": "Telemetry-buffer capacity is not reported."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure foreground and update storage requests with the controller fixed, adaptive, and disabled under the same task schedule.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate storage traffic attributable to latency observation and utilization control."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Determine whether target, baseline, observations, and learned utilization survive restart and measure any durable bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Persistence and restart semantics for controller state are not specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "An initial recording phase suspends update scheduling to establish a no-update latency baseline, followed by a search for a feasible starting utilization ratio.",
      "measurement_needed": "Measure recording and feasibility-search duration plus update work deferred during each initial or repeated calibration.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not generalize warm-up duration or the amount of deferred update work."
    },
    "ram": {
      "assumptions": [
        "The implementation bounds or summarizes the observation history rather than retaining samples indefinitely."
      ],
      "expression": "Incremental resident controller state consists of baseline and latency summaries, the utilization ratio, phase and violation state, and a finite observation history or summary.",
      "measurement_needed": "Measure controller objects, latency-history buffers, and peak whole-process RSS with co-execution disabled and enabled.",
      "premises": [
        "The source controller consumes periodically observed latency and retains a baseline, ratio, and state-machine phase."
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-007"
      ],
      "status": "DERIVED",
      "uncertainty": "History representation, allocator overhead, and byte coefficients are not reported."
    },
    "temporary_storage": {
      "assumptions": [
        "Observed statistics are maintained over a finite implementation window."
      ],
      "expression": "Temporary state includes the finite observation-window samples or summaries and scalar working values used during initial feasible-ratio search.",
      "measurement_needed": "Measure peak sample-buffer, statistic-summary, and feasibility-search allocations.",
      "premises": [
        "The source periodically profiles latency and searches for a feasible utilization ratio."
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-007"
      ],
      "status": "DERIVED",
      "uncertainty": "Window length and quantile-summary implementation are unspecified."
    }
  },
  "source_domain": "feedback control for search-update co-execution in disk-based graph approximate-nearest-neighbor systems",
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-hop budgeting cannot account for all end-to-end runtime effects, motivating observed-latency feedback.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.4, Adaptive Utilization Tuning, opening paragraph",
      "page": 6,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Recording, initial feasibility search, steady adjustment, disable behavior, and rebaseline transitions.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.4, Feedback-loop",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "The latency-degradation target is the controller's user-facing parameter and scales admitted update work.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.5, Parameter Configuration",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Transient target violations, phase-level best-effort behavior, and utilization adjustments during insertion and deletion.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Sections 6.2-6.3, best-effort limitation and Experiment 3 discussion",
      "page": 9,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Utilization ratio and observed latency-overrun trace for adaptive tuning.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 8",
      "page": 10,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Cumulative ablation stage that replaces fixed utilization with adaptive tuning before the next independently enabled component.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9 and Section 6.4, +Tuning stage",
      "page": 10,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Observed-latency statistic choices and the reported sparsity and noise of high-quantile feedback.",
      "locator_type": "TABLE",
      "locator_value": "Table 5 and Appendix A, Experiment 9",
      "page": 15,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-007"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-007"
    ],
    "text": "Periodic foreground latency observations, using the configured mean or tail statistic, enter the controller and are compared with the current baseline.",
    "uncertainty": "High-quantile observations can be sparse and noisy."
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
        "SP-007"
      ],
      "text": "Very high latency quantiles provide sparser and noisier observations, which can make online adjustment less stable.",
      "uncertainty": "The source reports this behavior for its evaluated statistic choices and does not provide a universal stability threshold."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "text": "A representative no-update baseline can be measured, latency observations arrive quickly enough to guide adjustment, and at least one nonzero utilization ratio satisfies the configured target.",
      "uncertainty": "Representativeness and response time are workload-dependent."
    }
  ]
}
```

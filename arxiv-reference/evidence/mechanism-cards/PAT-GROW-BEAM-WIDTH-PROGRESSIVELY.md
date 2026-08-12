# Grow Beam Width Progressively

- Pattern ID: `PAT-GROW-BEAM-WIDTH-PROGRESSIVELY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The Knight Bus algorithm exposes a width-like frontier control.",
      "A runtime guard can stop, spill, or refuse before exceeding its contract."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reduces wasted early reads by using smaller width.",
      "The source observes degradation when iteration count and width grow."
    ],
    "source_pointer_ids": [
      "SP-021",
      "SP-022",
      "SP-024"
    ],
    "text": "Knight Bus could expose frontier width as an execution budget that changes by phase, but admission would need a maximum-width and maximum-read guard because the source schedule has harmful long-query regimes.",
    "uncertainty": "The paper provides no hard ceiling or exact-graph correctness analysis for dynamic frontier width."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021"
    ],
    "text": "Width grows progressively from approach to convergence, regulating how many disk candidates are issued at each iteration.",
    "uncertainty": "High-dimensional or long searches can grow the width too far."
  },
  "confidence_rationale": {
    "assumptions": [
      "The cited paper and pointers accurately represent the evaluated mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited source pointers describe or evaluate the mechanism.",
      "G05 did not independently reproduce the source result or inspect implementation code."
    ],
    "source_pointer_ids": [
      "SP-022",
      "SP-023",
      "SP-024"
    ],
    "text": "The source reports individual and combination ablations plus negative high-recall and long-query boundaries, but this campaign did not reproduce the code or results.",
    "uncertainty": "Grade C does not turn average benchmark behavior into a per-query guarantee."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021",
      "SP-022"
    ],
    "text": "The mechanism uses the existing disk-resident graph and an in-memory beam candidate queue; it requires no separate physical index layout.",
    "uncertainty": "Candidate-queue representation and bytes are not isolated."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-022",
        "SP-024"
      ],
      "text": "High-dimensional, high-recall, or top-100 searches can require enough iterations that progressive growth becomes too wide, causing excess I/O and reduced benefit.",
      "uncertainty": "The precise harmful width is workload-specific."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-GROW-BEAM-WIDTH-PROGRESSIVELY",
  "falsifying_test": {
    "controlled_variables": [
      "graph index",
      "query vectors",
      "page size",
      "cache state",
      "concurrency",
      "accuracy target"
    ],
    "failure_signal": "Progressive width loses required recall or exceeds fixed-width pages, latency, or peak candidate state under the declared bound",
    "fixture": "Matched short-path and long-path query sets over the same disk graph, including a high-recall top-100 case",
    "independent_oracle": "Fixed-width search swept across widths and compared at matched recall",
    "scope": "Smallest scheduler-bound falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021"
    ],
    "text": "The search remains a beam search over the same graph and distance objective while only the number of candidates expanded per phase changes.",
    "uncertainty": "Changing width can change achieved recall under fixed other parameters."
  },
  "knight_bus_algorithm_families": [
    "BEST_FIRST_GRAPH_SEARCH",
    "APPROXIMATE_NEAREST_NEIGHBOR",
    "BOUNDED_PATH_SEARCH"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021",
      "SP-022"
    ],
    "text": "Use a small beam in the approach phase to suppress read-but-unexplored candidates, then increase width as the search converges and additional candidates are more likely to be useful.",
    "uncertainty": "The source tunes growth within its implementation rather than proving an optimal schedule."
  },
  "name": "Grow Beam Width Progressively",
  "pattern_id": "PAT-GROW-BEAM-WIDTH-PROGRESSIVELY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021"
    ],
    "text": "A fixed wide beam reads candidates speculatively during the early approach phase even though those reads often do not improve recall.",
    "uncertainty": "The two-phase behavior is established for proximity-graph beam search."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021"
    ],
    "text": "No separate index state is recomputed; the scheduler changes the width used by successive search iterations.",
    "uncertainty": "Distance work changes with the number of candidates admitted."
  },
  "related_pattern_ids": [
    "PAT-NAVIGATE-MEMORY-BEFORE-DISK"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021"
    ],
    "text": "The current beam, visited candidates, and phase or iteration state remain resident during a query.",
    "uncertainty": "Peak state is not reported separately from the search process."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Small early widths suppress read-but-unexplored candidates; larger later widths can improve convergence, but long high-recall searches can trigger more I/O.",
      "measurement_needed": "Measure issued, completed, and useful reads per iteration at matched recall.",
      "premises": [],
      "source_pointer_ids": [
        "SP-021",
        "SP-022",
        "SP-024"
      ],
      "status": "SOURCED",
      "uncertainty": "No hard per-query upper bound is given."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Compare serialized artifacts with fixed and dynamic width.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No incremental persistent state is reported, but absence is not measured as zero."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Dynamic width is a query-time scheduling change and adds no separate construction stage in the source's comparison.",
      "measurement_needed": "Record any offline calibration time used to select growth parameters.",
      "premises": [],
      "source_pointer_ids": [
        "SP-022"
      ],
      "status": "SOURCED",
      "uncertainty": "Parameter tuning cost is not included."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak candidate and visited-state bytes by iteration.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not isolate queue RAM as width grows."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure temporary queue and request-buffer bytes across width growth.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Per-query candidate scratch is not separately bounded."
    }
  },
  "source_domain": "beam scheduling for disk-resident graph search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Approach/convergence phases and progressive width schedule",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.1, Dynamic Width",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-021"
    },
    {
      "claim_scope": "Standalone gains, recall loss, and high-dimensional degradation",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 3 and DynamicWidth discussion",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-022"
    },
    {
      "claim_scope": "Pipeline interaction, full-combination result, and accuracy boundary",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 9 through Finding 11",
      "page": 11,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-023"
    },
    {
      "claim_scope": "Longer-iteration and high-recall limitation",
      "locator_type": "PARAGRAPH",
      "locator_value": "Top-100 discussion and conclusion",
      "page": 12,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-024"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-021",
      "SP-022"
    ],
    "text": "Disk pages for selected beam candidates are read according to the current width.",
    "uncertainty": "Speculative pipeline reads can alter this schedule."
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
        "SP-021",
        "SP-024"
      ],
      "text": "The study does not provide an admission-time bound on maximum width, queue memory, or page reads for an unseen query.",
      "uncertainty": "A measured average improvement is not an enforceable per-query ceiling."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-021",
        "SP-022"
      ],
      "text": "The search has a short approach phase in which wide exploration is wasteful and a convergence phase in which additional candidates are useful.",
      "uncertainty": "The phase boundary is heuristic."
    }
  ]
}
```

# Select Partition Scatter Mode

- Pattern ID: `PAT-SELECT-PARTITION-SCATTER-MODE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes both source-centric and partition-centric access paths",
      "The planner can estimate active edges per partition or apply a conservative upper bound"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-SELECT-PARTITION-SCATTER-MODE: the source chooses mode from partition-local communication and bandwidth estimates",
      "PAT-SELECT-PARTITION-SCATTER-MODE: sparse and dense frontiers reverse the preferred mode"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "Knight Bus admission for a dual scatter engine would need partition-level active-edge estimates and calibrated sequential/random bandwidths, then reserve the larger of the selected mode's transient state and retain a refusal path when model uncertainty crosses a declared bound.",
    "uncertainty": "Source equations model DRAM behavior and do not establish out-of-core latency, whole-process memory, or estimator error."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "For sparse partitions, visit only active sources and their neighboring partitions; for dense partitions, scan the partition-node layout in destination order and emit value streams sequentially.",
    "uncertainty": "A wrong cost estimate can select the slower path without affecting graph-result semantics."
  },
  "confidence_rationale": {
    "assumptions": [
      "The plotted dual-mode implementation uses the stated analytical rule"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 3.2 defines the selector",
      "Figures 6-7 report source comparisons of forced and dual modes"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-004",
      "SP-005"
    ],
    "text": "Confidence is moderate because the source gives explicit mode equations and per-iteration ablations where dual mode generally follows the faster path, but this campaign did not reproduce or code-inspect the selector.",
    "uncertainty": "Evidence is source-reported on two shared-memory systems and does not include independent calibration."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The same partitioned graph exposes an active-vertex source-centric path and a partition-node layout whose destination IDs are memoized for partition-centric value-only streaming.",
    "uncertainty": "Maintaining both access paths adds metadata and preprocessing not fully totaled by the source."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "text": "Partition-centric mode is forced on very sparse frontiers, where it traverses inactive state, or source-centric mode is forced on dense frontiers, where bin switching limits memory bandwidth.",
      "uncertainty": "This describes the wrong-mode boundary, not a correctness failure."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SELECT-PARTITION-SCATTER-MODE",
  "falsifying_test": {
    "controlled_variables": [
      "graph layout",
      "partition size",
      "active-edge fraction",
      "value and index widths",
      "thread count",
      "cache state"
    ],
    "failure_signal": "The analytical selector repeatedly chooses a mode slower than both forced alternatives under calibrated parameters, or violates its configured work-factor premise",
    "fixture": "One partitioned graph run through iterations whose active-edge fraction sweeps from sparse to dense",
    "independent_oracle": "Measured elapsed time and DRAM-byte counters for forced SC and forced PC on every partition",
    "scope": "Smallest mode-selection falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Mode selection is made independently for every partition in every iteration and selects partition-centric traversal only when its modeled time is no larger than source-centric traversal, keeping work within a configured constant factor of active-edge work.",
    "uncertainty": "The guarantee is with respect to the paper's analytical model and configured bandwidth ratio."
  },
  "knight_bus_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "WCC_CONNECTED_COMPONENTS",
    "PAGERANK_CENTRALITY",
    "SEEDED_RANDOM_WALK"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Estimate each partition's SC and PC communication volumes from active vertices, active edges, total edges, aggregation factor, index/value widths, and mode bandwidths, then execute the lower predicted-time scatter mode.",
    "uncertainty": "GPOP uses the average aggregation factor as a fast approximation in SC mode."
  },
  "name": "Select Partition Scatter Mode",
  "pattern_id": "PAT-SELECT-PARTITION-SCATTER-MODE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Active-only source-centric traversal minimizes work for sparse frontiers but switches among destination bins, while partition-centric traversal increases work by scanning inactive state yet supplies sequential high-bandwidth memory access.",
    "uncertainty": "Relative cost depends on each partition's active edge count and measured bandwidth ratio."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Active counts and the mode decision are recomputed for every partition and iteration; static destination identifiers remain reusable in PC mode.",
    "uncertainty": "Aggregation-factor estimation error is not bounded."
  },
  "related_pattern_ids": [
    "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "The current partition's vertex state, active frontier metadata, per-bin insertion state, and mode-model counters are resident while scattering.",
    "uncertainty": "Peak byte size is not reported."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "The source models SC traffic as (V_a^p + E_a^p) * s_i + 2 * (r_p * E_a^p * s_v + E_a^p * s_i), and PC traffic as E^p * ((r_p + 1) * s_i + 2 * r_p * s_v) + k * s_i, then divides by mode bandwidth for selection.",
      "measurement_needed": "Calibrate modeled versus actual DRAM bytes and elapsed time per partition across active-edge densities.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "These are modeled DRAM volumes using the source's notation and approximated aggregation factor, not measured storage bytes."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure each retained access path, offsets, and identifiers separately.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not total the persistent bytes of both source-centric adjacency and the partition-node/id-bin layout."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Build the partition-node layout and write destination identifiers once so the PC path can emit only changing vertex values in later iterations.",
      "measurement_needed": "Measure incremental build time and bytes for enabling the PC path in addition to SC adjacency.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not isolate dual-layout build cost from overall framework setup."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RSS and per-layout resident bytes with dual-mode enabled and disabled.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not provide a complete RAM bound for holding dual layouts, frontiers, bins, and model counters."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure phase-level peak temporary bytes for sparse, crossover, and dense frontiers.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Peak frontier, message-bin, and mode-accounting temporary bytes are not bounded."
    }
  },
  "source_domain": "frontier-sensitive shared-memory graph communication",
  "source_paper_ids": [
    "PAPER-1806.08092"
  ],
  "source_pointers": [
    {
      "claim_scope": "SC active-only work and PC sequential-access trade-off",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1.2, Source-centric and Partition-centric mode paragraphs",
      "page": 7,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "PC value-only messages, per-partition selection, and sequential gather",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 3 and Section 3.1.2 continuation",
      "page": 8,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Analytical per-partition communication and bandwidth decision rule",
      "locator_type": "EQUATION",
      "locator_value": "Section 3.2, Equation 1 and preceding communication expressions",
      "page": 9,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Source ablation of dual communication mode",
      "locator_type": "FIGURE",
      "locator_value": "Figure 6 and Section 6.2.3",
      "page": 20,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Per-iteration comparison of SC, PC, and modeled dual-mode selection",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and accompanying paragraphs",
      "page": 21,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-005"
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
    "text": "SC streams active adjacency and mixed destination-bin messages; PC streams the partition-node layout and destination-grouped value messages.",
    "uncertainty": "The source models DRAM communication, not secondary-storage I/O."
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
        "SP-003",
        "SP-004"
      ],
      "text": "The model's portability is unknown when bandwidth ratios, value widths, cache behavior, NUMA effects, or storage-backed access differ materially from the evaluated machines.",
      "uncertainty": "The default bandwidth ratio is configurable rather than universal."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "text": "Frontier density varies across partitions or iterations, allowing sparse regions to use active-only work and dense regions to exploit sequential bandwidth.",
      "uncertainty": "Benefits shrink when one mode dominates every partition for the entire run."
    }
  ]
}
```

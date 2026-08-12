# Reuse Loaded Subgrids Selectively

- Pattern ID: `PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can express conservative subgrid activity/priority rules for the selected algorithm",
      "The runtime records loaded, skipped, and reused subgrids in its receipt"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY: the scheduler can skip and reuse whole subgrids",
      "PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY: correctness depends on asynchronous semantics and scheduler choices"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "A Knight Bus grid-stream plan would need a declared subgrid residency cap and an algorithm-specific skip/reuse contract; admission can count only scheduled reads if a conservative scheduler proves coverage, otherwise it must budget a full pass or refuse the optimized plan.",
    "uncertainty": "The paper supplies no scheduler-proof framework, whole-process RAM bound, or isolated skip/reuse benchmark."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Before each outer iteration, set subgrid priorities and sort them; for each candidate subgrid, call the scheduler, load only accepted subgrids, and process all of the subgrid's decoded edges, potentially more than once before eviction.",
    "uncertainty": "The source does not prescribe one priority or reuse-count policy."
  },
  "confidence_rationale": {
    "assumptions": [
      "The evaluated applications use scheduler behavior consistent with the described mechanism"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm 1 defines priority, skip, and subgrid processing",
      "Section 4 reports source whole-system results for grid-stream workloads"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004",
      "SP-006"
    ],
    "text": "Confidence is moderate-low because the scheduler interface and subgrid granularity are explicit and whole-system WCC/SpMV results are reported, but the reuse/skip contribution is not isolated and no code or reproduction was inspected.",
    "uncertainty": "No isolated ablation, independent rerun, or broader algorithm evaluation supports the mechanism."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Edges are partitioned into a two-dimensional grid and subdivided into compressed fixed-size subgrids, with mutable vertex values managed separately in memory or memory-mapped storage.",
    "uncertainty": "The card does not include the separate compression mechanism's encoding details."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-006"
      ],
      "text": "The algorithm requires strict synchronous intermediate values or the user scheduler skips a subgrid whose edges are necessary for convergence or correctness.",
      "uncertainty": "The source addresses synchronous workloads with a different streamer."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY",
  "falsifying_test": {
    "controlled_variables": [
      "subgrid partitioning",
      "scheduler policy",
      "reuse count",
      "memory cap",
      "thread count",
      "stopping criterion"
    ],
    "failure_signal": "The optimized scheduler returns a different converged result, omits a required subgrid, or rereads a subgrid promised to remain resident within the declared cap",
    "fixture": "A two-dimensional partitioned graph with one subgrid that appears inactive initially but becomes necessary after propagation, plus one subgrid profitable to reuse while loaded",
    "independent_oracle": "A full deterministic edge-stream pass each iteration until the same convergence criterion",
    "scope": "Smallest scheduler correctness/residency falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "For algorithms whose final result tolerates asynchronous traversal, a scheduler may reorder, skip, or repeatedly process whole subgrids without changing the intended converged result.",
    "uncertainty": "The source does not provide a general proof for arbitrary user schedulers or stopping rules."
  },
  "knight_bus_algorithm_families": [
    "WCC_CONNECTED_COMPONENTS",
    "SPARSE_MATRIX_VECTOR"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Expose each compressed subgrid as a scheduler-controlled unit, assign priorities before streaming, skip subgrids deemed irrelevant, and invoke processing repeatedly on a subgrid already loaded in memory when extra local work can reduce later I/O or iterations.",
    "uncertainty": "Scheduling policy is user-defined and therefore outside the system's automatic correctness guarantee."
  },
  "name": "Reuse Loaded Subgrids Selectively",
  "pattern_id": "PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "A fixed full-graph streaming order can reload subgraphs that do not affect current progress and evict subgraphs that could profitably be processed again while already resident.",
    "uncertainty": "The amount of avoidable I/O depends on the algorithm's active region and convergence behavior."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Mutable vertex values, convergence state, priorities, and skip/reuse decisions are recomputed as processing advances; immutable compressed edges persist.",
    "uncertainty": "Incorrect scheduler state can omit required work."
  },
  "related_pattern_ids": [
    "PAT-COMPRESS-SORTED-ID-STREAMS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "At least one decoded subgrid, its optional edge weights, mutable vertex state, scheduler metadata, and convergence state are resident during processing.",
    "uncertainty": "The number of simultaneously loaded subgrids and whole-process peak are not stated."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "The scheduler can avoid I/O by skipping a subgrid or by repeatedly processing subgrid data already loaded in memory, but operates only at subgrid rather than edge granularity.",
      "measurement_needed": "Record accepted/skipped/reused subgrids, bytes read, decoded edges, and iterations under a declared scheduler.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not isolate bytes saved by skip/reuse from compression and other grid-streamer effects."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure oriented compressed streams, subgrid metadata, weights, and retained original/intermediate files separately.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The grid-streamer mechanism uses compressed graph files, but its storage cannot be separated from the compression card using source-provided totals."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Partition the edge list into a two-dimensional grid, split grids into bounded subgrids, and produce compressed oriented streams before execution.",
      "measurement_needed": "Measure grid/subgrid construction separately from compression and scheduler initialization.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "This card shares preprocessing with other KedaGraph streamers; scheduler-specific incremental cost is not isolated."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RSS by subgrid size, loaded-subgrid count, vertex-state placement, thread count, and weight use.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not provide a complete peak-RAM expression for loaded/decoded subgrids, vertex state, scheduler metadata, and worker buffers."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak decoded-subgrid, scheduler, sort, and worker-buffer bytes by phase.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Decoded-edge buffers, priority arrays, and preprocessing scratch are not bounded together."
    }
  },
  "source_domain": "asynchronous out-of-core edge-centric graph processing",
  "source_paper_ids": [
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "Grid/subgrid representation, immutable edges, mutable vertex state, and scheduler capability",
      "locator_type": "SECTION",
      "locator_value": "Section 2 opening and Section 2.1 first paragraph",
      "page": 4,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Subgrid priority, skip decision, whole-subgrid delivery, and repeated loaded-data processing",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and Section 2.1",
      "page": 5,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Grid-streamer WCC and SpMV source evaluation setup",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2 and Figure 6",
      "page": 14,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Source end-to-end grid-streamer execution results",
      "locator_type": "TABLE",
      "locator_value": "Table 3, WCC and SpMV rows",
      "page": 15,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Comparison with asynchronous systems and subgrid-level skip/reprocess granularity",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1",
      "page": 18,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Grid-streamer capabilities and source-stated algorithm-coverage limitations",
      "locator_type": "SECTION",
      "locator_value": "Section 6, first two paragraphs",
      "page": 20,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-006"
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
    "text": "Accepted compressed edge subgrids and, for weighted jobs, their separate weights are streamed from secondary storage and decoded for the user function.",
    "uncertainty": "A reused loaded subgrid avoids a reread only while it remains resident."
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
        "SP-006"
      ],
      "text": "Benefit and correctness are unknown for BFS, SSSP, triangle counting, road/biological topologies, and arbitrary scheduler policies not evaluated by the source.",
      "uncertainty": "The paper explicitly names broader algorithms and topologies as future validation."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "text": "The algorithm is safely asynchronous, the scheduler can identify irrelevant or high-value subgrids, and repeated local processing reduces future I/O or convergence work.",
      "uncertainty": "WCC and SpMV are evaluated, but broader asynchronous coverage remains limited."
    }
  ]
}
```

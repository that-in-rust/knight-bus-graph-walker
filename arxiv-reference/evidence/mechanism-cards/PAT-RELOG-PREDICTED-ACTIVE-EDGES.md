# Relog Predicted Active Edges

- Pattern ID: `PAT-RELOG-PREDICTED-ACTIVE-EDGES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus has iterative algorithms with adjacent-superstep activity correlation.",
      "Its graph artifact supports source-keyed sequential edge-log lookup."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-RELOG-PREDICTED-ACTIVE-EDGES:mechanism",
      "PAT-RELOG-PREDICTED-ACTIVE-EDGES:temporary_storage",
      "PAT-RELOG-PREDICTED-ACTIVE-EDGES:fails_when"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-004"
    ],
    "text": "A Knight Bus iterative runner could relog predicted next-active adjacency to reduce sparse page reads, but admission must reserve duplicated edge-list bytes and treat misprediction writes and residual CSR reads as explicit I/O uncertainty.",
    "uncertainty": "No Knight Bus activity predictor, page-occupancy distribution, or net-I/O result has been measured."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "While processing the current superstep, evaluate activity history and page utilization, append selected outgoing lists, then consult the edge log first for those vertices in the next superstep.",
    "uncertainty": "Selection overhead and lookup priority are implementation-dependent."
  },
  "confidence_rationale": {
    "assumptions": [
      "The source measurements include the optimizer overheads described in the implementation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section V.C specifies the edge-log selection and lookup path.",
      "Figure 9 reports source prediction results across applications."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Confidence is moderate-low because the source defines the predictor and reports benchmark behavior, but prediction accuracy is workload-sensitive and this campaign did not reproduce or inspect the implementation.",
    "uncertainty": "Current SSD behavior, crash cleanup, and Knight Bus activity correlation are unverified."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "The immutable CSR remains authoritative while selected source vertices' outgoing edge lists are replicated into sequential edge-log pages with a source-vertex index.",
    "uncertainty": "The paper does not specify a durable recovery format for edge logs."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "Prediction accuracy is lower for quickly converging workloads with few inefficient pages, reducing the opportunity for useful edge relogging.",
      "uncertainty": "The source reports reduced opportunity and accuracy, not a universal slowdown threshold."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-RELOG-PREDICTED-ACTIVE-EDGES",
  "falsifying_test": {
    "controlled_variables": [
      "CSR page layout",
      "active-set cardinality",
      "edge-record width",
      "history window",
      "utilization threshold"
    ],
    "failure_signal": "Logged adjacency differs from the oracle, temporary storage exceeds the derived selected-edge volume, or relogging increases total physical I/O for the correlated case",
    "fixture": "A page-aligned CSR graph with two consecutive active sets: one correlated set on low-utilization pages and one anti-correlated set of equal size",
    "independent_oracle": "Direct CSR adjacency enumeration for every active vertex",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Only outgoing edge lists predicted to be needed next and currently residing on poorly utilized pages are appended to the edge log; logged edges remain keyed by source vertex for next-superstep lookup.",
    "uncertainty": "Activity and page-utilization decisions are predictions, not guarantees."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "PAGERANK",
    "COMMUNITY_DETECTION",
    "GRAPH_COLORING",
    "MAXIMAL_INDEPENDENT_SET",
    "RANDOM_WALK"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-005"
    ],
    "text": "Use recent active-vertex history and current page-utilization history to select outgoing edge lists, copy those lists sequentially into an edge log, and read the compacted copies in the following superstep instead of sparse original CSR pages.",
    "uncertainty": "The evaluated predictor uses one-step history and a configured utilization threshold."
  },
  "name": "Relog Predicted Active Edges",
  "pattern_id": "PAT-RELOG-PREDICTED-ACTIVE-EDGES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Fetching one active vertex's outgoing edges from CSR can read an entire SSD page dominated by edges of inactive vertices.",
    "uncertainty": "The severity depends on SSD page size, adjacency packing, and active-vertex distribution."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Predicted activity and predicted page inefficiency are recomputed each superstep from recent activity and current-superstep page usage.",
    "uncertainty": "The predictor does not establish future activity with certainty."
  },
  "related_pattern_ids": [
    "PAT-PARTITION-UPDATES-BY-DESTINATION"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Resident optimizer state includes active-vertex bit vectors, recent activity history, observed page-utilization state, an edge-log buffer, and the edge-log source index.",
    "uncertainty": "The source configures an edge-log buffer fraction but does not report whole-process RSS attribution."
  },
  "resource_model": {
    "io": {
      "assumptions": [
        "Each selected edge list is read at most once from the edge log in the next superstep."
      ],
      "expression": "edge_log_write_bytes + edge_log_read_bytes + residual_CSR_page_read_bytes",
      "measurement_needed": "Record source-page reads, edge-log writes, edge-log reads, and avoided CSR reads by superstep.",
      "premises": [
        "The mechanism writes selected outgoing lists once and may replace sparse original-page reads with compact edge-log reads."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "DERIVED",
      "uncertainty": "Net I/O depends on prediction precision, original page occupancy, and cache effects."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Specify and measure retained files after successful completion and after restart.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Edge logs are described as iterative optimization state, and no retained durable-storage contract is given."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure initialization time for history bit vectors, utilization metadata, and source indexes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No separate preprocessing phase is bounded for this online optimizer."
    },
    "ram": {
      "assumptions": [],
      "expression": "active_history_bitvectors + page_utilization_state + configured_edge_log_buffer + edge_log_index",
      "measurement_needed": "Measure optimizer-only peak RAM and index bytes by active-set size.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Exact bytes for indexes and utilization metadata are not reported."
    },
    "temporary_storage": {
      "assumptions": [
        "Each selected edge list is duplicated once per superstep and old edge-log generations can be reclaimed."
      ],
      "expression": "sum_outdegree(selected_predicted_vertices) * edge_record_bytes + edge_log_index_bytes",
      "measurement_needed": "Measure peak edge-log bytes including indexes and unreclaimed generations.",
      "premises": [
        "The edge log duplicates every outgoing edge of each selected predicted-active vertex."
      ],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "DERIVED",
      "uncertainty": "Selection overlap, edge-record width, and reclamation lag are not bounded in the paper."
    }
  },
  "source_domain": "SSD-backed iterative graph adjacency access",
  "source_paper_ids": [
    "PAPER-1905.04264"
  ],
  "source_pointers": [
    {
      "claim_scope": "Page-granular adjacency read amplification and edge-log objective",
      "locator_type": "SECTION",
      "locator_value": "Section IV.C, Reduce read overhead with an edge log",
      "page": 4,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Monitoring and predicting next-superstep activity and page use",
      "locator_type": "SECTION",
      "locator_value": "Section V.C, Edge-log optimizer, first paragraph",
      "page": 6,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "History predictor, utilization threshold, edge replication, next-step reads, and configured buffer",
      "locator_type": "SECTION",
      "locator_value": "Section V.C, Edge-log optimizer, continuation",
      "page": 7,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Prediction behavior and lower accuracy for quickly converging workloads",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9 and prediction-accuracy paragraph",
      "page": 10,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Edge-list logging as the paper's response to page read amplification",
      "locator_type": "SECTION",
      "locator_value": "Section X, Conclusion, first paragraph",
      "page": 12,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-005"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Selected outgoing lists are appended to sequential edge-log locations and are streamed back from those compact pages in the next superstep; unselected lists continue to come from CSR.",
    "uncertainty": "A misprediction can add log writes without avoiding a later CSR read."
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
        "SP-003"
      ],
      "text": "The total graph-replication bound depends on configured thresholds, and the paper does not give a workload-independent cap on edge-log volume.",
      "uncertainty": "Whole-run amplification remains unknown until the active sequence is observed."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Vertex activity persists across adjacent supersteps and active edge lists are scattered across pages with low useful-byte occupancy.",
      "uncertainty": "The source tests one history-based predictor and one utilization threshold."
    }
  ]
}
```

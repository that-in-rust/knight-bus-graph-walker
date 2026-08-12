# Pagesearch Compute Idles Device

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Page contents and requested recall are identical."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "pages",
      "queries",
      "recall",
      "cache state",
      "device queue depth"
    ],
    "expected_observation": "Full-page evaluation increases CPU time and device idle intervals without reducing required reads enough to compensate",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "compute heavy fetched page",
    "graph_scale": "Small repeatable page set sufficient to saturate one query worker",
    "graph_shape": "Packed pages containing many records with expensive distance evaluation",
    "independent_oracle": "Candidate-only trace plus exact candidate-distance oracle",
    "premises": [
      "The source reports exactly this compute-versus-I/O reversal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "System concurrency can hide compute in other workers.",
    "varied_variables": [
      "records per page",
      "distance-compute cost"
    ],
    "workload": "Run baseline candidate-only search and full-page search at matched recall"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SEARCH-EVERY-FETCHED-RECORD"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Distance cost and queueing are measured under matched recall."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "records_per_page * distance_compute_time > hidden_io_latency_budget",
    "measurement_needed": "Measure CPU time, queue depth, device idle time, and records evaluated per page.",
    "numeric_constants": [],
    "premises": [
      "Every fetched record is searched.",
      "The source reports compute-induced device idling."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Vectorization and batching alter the compute term.",
    "variables": [
      {
        "definition": "records evaluated from each fetched page",
        "symbol": "records_per_page",
        "units": "records per page"
      },
      {
        "definition": "compute time per record distance",
        "symbol": "distance_compute_time",
        "units": "time per record"
      },
      {
        "definition": "compute time hideable without starving storage",
        "symbol": "hidden_io_latency_budget",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Queries are matched for recall."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism leaves I/O count unchanged in isolation while adding compute.",
      "It has no locality benefit with one record per page."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Searching every fetched record wins only when page-local candidate benefit exceeds added distance-computation cost without starving storage submission.",
    "uncertainty": "CPU and device balance depends on page packing and vector cost."
  },
  "confidence_rationale": {
    "assumptions": [
      "The evaluated mechanism corresponds to the card."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Both pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source reports both the throughput regression mechanism and the no-benefit page boundary; target coefficients remain unmeasured.",
    "uncertainty": "No portable numeric breakpoint is claimed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Telemetry captures CPU and device timelines."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source says standalone PageSearch leaves I/O count unchanged."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Matched-recall throughput degrades or device idle time rises while page reads do not fall.",
    "uncertainty": "At higher concurrency another query may occupy the device."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-PAGESEARCH-COMPUTE-IDLES-DEVICE",
  "name": "Pagesearch Compute Idles Device",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Search throughput falls because added compute idles the SSD, or the mechanism adds no benefit at the one-record-per-page boundary.",
    "uncertainty": "The exact crossover is not quantified portably."
  },
  "repair_options": [
    {
      "description": "Enable full-page search only when packing and compute cost predict useful locality.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Batch or vectorize page-local distance work while preserving device queue depth.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use candidate-only evaluation at the one-record-per-page boundary.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Full-page compute overhead and device idling",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 4 and PageSearch discussion",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "One-record-per-page boundary and page-size sensitivity",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 7.3 and Finding 12",
      "page": 12,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Pages contain enough records that evaluating all of them adds substantial distance work, or pages contain only one record and offer no extra candidate locality.",
    "uncertainty": "The source tests a specific disk-search implementation."
  }
}
```

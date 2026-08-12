# Page Shuffle Loses Utility

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The storage system does not compress padded records across pages."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph topology",
      "query sequence",
      "storage page size"
    ],
    "expected_observation": "Shuffling changes identifiers and build work but does not reduce page reads when each page contains one record.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "single-record-page-shuffle",
    "graph_scale": "Use the smallest graph that exercises a multi-hop candidate expansion across several pages.",
    "graph_shape": "A graph with reorderable neighboring vertices but records padded to fill a page.",
    "independent_oracle": "Count physical page reads and useful expanded records from an exact trace replay.",
    "premises": [
      "The source identifies page occupancy as a locality term."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Real records may span pages rather than align exactly.",
    "varied_variables": [
      "record padding",
      "records per page"
    ],
    "workload": "Compare shuffled and unshuffled layouts with identical search order and cache state."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Page occupancy is determined by complete record size and page size."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "records_per_page <= reusable_records_floor",
    "measurement_needed": "Sweep record size and measure useful records consumed per fetched page.",
    "numeric_constants": [],
    "premises": [
      "The source ties savings to overlap among multiple records on a page."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The exact useful occupancy above the degenerate case is workload-dependent.",
    "variables": [
      {
        "definition": "Number of complete graph records that fit on a storage page.",
        "symbol": "records_per_page",
        "units": "records per page"
      },
      {
        "definition": "Minimum page occupancy needed for co-location reuse.",
        "symbol": "reusable_records_floor",
        "units": "records per page"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "A fetched storage page can contain multiple related records whose co-location is reusable by traversal.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "Page-read counters expose the physical effect."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explains and observes occupancy-dependent locality."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The degenerate occupancy failure is explicitly reported; the smallest fixture is a controlled reconstruction.",
    "uncertainty": "Benefit at intermediate occupancy remains graph- and query-dependent."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Shuffled page-read count fails to improve over the unshuffled control when each page holds one record.",
    "uncertainty": "NONE"
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-PAGE-SHUFFLE-LOSES-UTILITY",
  "name": "Page Shuffle Loses Utility",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The source reports that PageShuffle then provides no locality benefit and is weak as a standalone optimization.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Disable page shuffling when measured records-per-page cannot provide reuse.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Separate large payloads from compact traversal records so multiple traversal records share a page.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Page-read savings depend on page occupancy and neighbor overlap.",
      "locator_type": "EQUATION",
      "locator_value": "Equation 1 and locality intuition",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "PageShuffle is weak standalone and has no locality benefit when one record occupies a page.",
      "locator_type": "SECTION",
      "locator_value": "Finding 4 and Finding 6",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Record size leaves only one record on each storage page.",
    "uncertainty": "NONE"
  }
}
```

# Inline Threshold Inflates Memory

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Synthetic degree counts preserve the storage invariants."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "edge set",
      "vertex ordering policy",
      "block size",
      "algorithm",
      "thread count"
    ],
    "expected_observation": "Either metadata or mini-adjacency bytes cause peak RAM to exceed the admitted budget",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "threshold memory reversal",
    "graph_scale": "Small synthetic degree histogram with controllable low-degree mass",
    "graph_shape": "Graph mixing many low-degree vertices with enough blocks to expose metadata cost",
    "independent_oracle": "Original adjacency lists plus exact traversal output and allocation counters",
    "premises": [
      "The source identifies both sides of the threshold trade-off."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Preprocessing fragmentation may add a secondary effect.",
    "varied_variables": [
      "degree threshold",
      "low-degree mass"
    ],
    "workload": "Build and run identical traversal across threshold settings"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "All byte classes are measured at peak RSS and allocator level."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "metadata_bytes(theta) + mini_adjacency_bytes(theta, degree_histogram) > ram_budget",
    "measurement_needed": "Measure each byte class across a threshold sweep on the target degree histogram.",
    "numeric_constants": [],
    "premises": [
      "Threshold choice moves bytes between metadata, disk blocks, and resident mini data."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The paper does not supply a portable byte formula.",
    "variables": [
      {
        "definition": "degree threshold for resident mini adjacency",
        "symbol": "theta",
        "units": "degree"
      },
      {
        "definition": "vertex counts by degree",
        "symbol": "degree_histogram",
        "units": "vertices"
      },
      {
        "definition": "block and frontier metadata allocation",
        "symbol": "metadata_bytes",
        "units": "bytes"
      },
      {
        "definition": "resident mini-edge allocation",
        "symbol": "mini_adjacency_bytes",
        "units": "bytes"
      },
      {
        "definition": "admitted resident-memory budget",
        "symbol": "ram_budget",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Degree ordering and virtual-vertex invariants are preserved."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The threshold changes both metadata requirements and resident mini-data."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Inlining low-degree adjacency saves I/O only while compact metadata remains sufficient and the resident mini-adjacency array does not dominate memory.",
    "uncertainty": "The source optimum is dataset and layout specific."
  },
  "confidence_rationale": {
    "assumptions": [
      "The paper figures and discussion describe the same implementation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The exact source pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source directly reports the two-sided memory trade-off, but only for its evaluated layout and datasets.",
    "uncertainty": "No numeric source threshold is generalized."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Peak accounting includes initialization and execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Both metadata and mini adjacency are threshold-dependent."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Peak resident bytes exceed budget or reconstructed adjacency differs from the oracle.",
    "uncertainty": "Allocator and alignment overheads require measurement."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY",
  "name": "Inline Threshold Inflates Memory",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Peak memory rises on either side of the source-selected balance even though larger thresholds can reduce runtime.",
    "uncertainty": "The exact curve is not portable."
  },
  "repair_options": [
    {
      "description": "Select a threshold from the target graph degree histogram and RAM budget.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Account separately for metadata and resident mini-adjacency bytes.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use a compact overflow representation when either side of the threshold dominates.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-2511.07886"
  ],
  "source_pointers": [
    {
      "claim_scope": "Degree classes, metadata pressure, and locality-fragmentation trade-off",
      "locator_type": "SECTION",
      "locator_value": "Sections 5.1-5.2",
      "page": 16,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Threshold-dependent metadata, mini-data, runtime, and memory boundary",
      "locator_type": "FIGURE",
      "locator_value": "Figure 15 and degree-threshold sensitivity discussion",
      "page": 22,
      "paper_id": "PAPER-2511.07886",
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
    "text": "Threshold settings below the compact-metadata range enlarge metadata, while larger settings eventually make resident mini-edge data dominate memory.",
    "uncertainty": "The source evaluates one implementation and graph set."
  }
}
```

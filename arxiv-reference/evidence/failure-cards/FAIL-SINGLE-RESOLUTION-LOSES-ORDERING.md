# Single resolution loses ordering advantage

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Nested communities expose a need for more than one scale."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Encoder",
      "Number of propagation passes",
      "Initial labels"
    ],
    "expected_observation": "Both orders decode exactly, but the repeated single-resolution order has worse locality or compressed size",
    "fixture_kind": "GRAPH",
    "fixture_name": "multiscale clustered ordering contrast",
    "graph_scale": "Symbolic nested graph large enough to contain both scales",
    "graph_shape": "Nested communities with fine and coarse boundaries",
    "independent_oracle": "Uncompressed adjacency equality plus direct compressed-size and locality counters",
    "premises": [
      "The source uses multiresolution composition to improve ordering."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The fixture is derived and does not assert a universal quality gap.",
    "varied_variables": [
      "Distinct resolution schedule"
    ],
    "workload": "Build orders from repeated single-resolution and multiresolution schedules"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Compression or locality quality degrades when relevant scales are absent."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "distinct_resolutions < resolutions_needed(graph)",
    "measurement_needed": "Compare locality and compressed size across single- and multi-resolution schedules.",
    "numeric_constants": [],
    "premises": [
      "The source attributes benefit to multiresolution composition."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source gives no graph-independent optimal resolution count.",
    "variables": [
      {
        "definition": "Distinct effective clustering resolutions composed",
        "symbol": "distinct_resolutions",
        "units": "resolutions"
      },
      {
        "definition": "Graph-dependent number of scales needed to expose locality",
        "symbol": "resolutions_needed",
        "units": "resolutions"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The reported ordering benefit depends on composing clusterings from multiple resolutions rather than repeatedly applying one resolution.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The proposed fixture preserves the source mechanism while varying only the stated trigger."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited pages define the mechanism and its reported or analytically exposed boundary."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Decoder equality separates correctness from ordering quality."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism is a heuristic ordering, not a correctness transform."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Exact decoding passes for both orders while the single-resolution schedule loses the measured locality or compression advantage.",
    "uncertainty": "The magnitude depends on graph structure and encoder."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-SINGLE-RESOLUTION-LOSES-ORDERING",
  "name": "Single resolution loses ordering advantage",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The multiresolution ordering advantage is lost even though graph decoding remains correct.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Limit use of the mechanism to workloads satisfying its sourced applicability conditions.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-1011.5425"
  ],
  "source_pointers": [
    {
      "claim_scope": "Composition of clusterings produced at different resolution levels.",
      "locator_type": "SECTION",
      "locator_value": "Section 6",
      "page": 6,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Random multiresolution schedule and reuse of base labelings.",
      "locator_type": "SECTION",
      "locator_value": "Section 6, resolution schedule",
      "page": 7,
      "paper_id": "PAPER-1011.5425",
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
    "text": "Apply the ordering pipeline using repeated clusterings from one effective resolution on a graph with useful structure at different scales.",
    "uncertainty": "NONE"
  }
}
```

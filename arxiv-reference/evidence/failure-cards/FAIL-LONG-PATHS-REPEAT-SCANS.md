# Long Paths Repeat Scans

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The chosen ordering delays useful admissions without changing topology."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph topology",
      "source vertex",
      "sketch cap",
      "segment size"
    ],
    "expected_observation": "The adversarial order preserves exact output but requires progressively more stream scans and reductions than the favorable order.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "delayed-correction-directed-path",
    "graph_scale": "Increase path length while keeping the declared sketch capacity per node fixed.",
    "graph_shape": "A long directed path with distractor edges ordered to delay breadth-first corrections across stream segments.",
    "independent_oracle": "In-memory BFS supplies exact distances; a scan trace independently verifies every correction and edge admission.",
    "premises": [
      "The source bound depends on LLSP(G) and capacity affects work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The precise worst-order construction needs empirical confirmation.",
    "varied_variables": [
      "path length",
      "edge order"
    ],
    "workload": "Run EP-BFS over adversarial and favorable edge orders for the same graph."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A stress graph can drive corrections across many path positions."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "scan_iterations approaches longest_simple_path_length",
    "measurement_needed": "Construct degree- and diameter-controlled graph families and record scans, reduced-stream bytes, and reducer work.",
    "numeric_constants": [],
    "premises": [
      "The source upper-bounds iterations by LLSP(G)."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Attainability and constants are not source-established.",
    "variables": [
      {
        "definition": "Completed edge-stream filtering and reduction iterations.",
        "symbol": "scan_iterations",
        "units": "iterations"
      },
      {
        "definition": "Length measure LLSP(G) used by the source worst-case bound.",
        "symbol": "longest_simple_path_length",
        "units": "path length"
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
    "text": "Sequential rescans and reductions remain practically affordable while the bounded sketch corrects the breadth-first structure.",
    "uncertainty": "The source gives a longest-simple-path worst-case iteration bound rather than a small scan guarantee."
  },
  "confidence_rationale": {
    "assumptions": [
      "Diameter and edge order can be controlled independently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Repeated scans are part of the algorithm."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The formal LLSP-dependent bound and capacity sensitivity are sourced; the near-bound adversarial family remains to be measured.",
    "uncertainty": "No source-reported numeric worst-case realization is available."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Counters include all rewritten reduced streams."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The fixture holds memory capacity fixed."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Scans, sequential bytes, or reducer invocations grow with path length despite the resident sketch cap remaining satisfied.",
    "uncertainty": "The growth rate is a test result, not asserted here."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-LONG-PATHS-REPEAT-SCANS",
  "name": "Long Paths Repeat Scans",
  "observable_symptom": {
    "assumptions": [
      "The reduced stream remains nonempty across corrections."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Each additional iteration scans a reduced edge stream and invokes reducer work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Peak named sketch storage stays bounded, but total sequential I/O and elapsed work grow with repeated scans and reductions.",
    "uncertainty": "The exact stream shrink rate is graph-dependent."
  },
  "repair_options": [
    {
      "description": "Estimate diameter/correction behavior and reject configurations whose scan envelope exceeds the declared work budget.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use a different external BFS strategy for high-diameter or repeatedly correcting streams.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2507.12925"
  ],
  "source_pointers": [
    {
      "claim_scope": "A bounded in-memory sketch is repeatedly grown, reduced, and supported by per-node arrays.",
      "locator_type": "SECTION",
      "locator_value": "Section 5, Overview",
      "page": 8,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "The algorithm may require up to LLSP(G) iterations/scans.",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 5.4 and cost paragraph",
      "page": 13,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Small sketch capacity increases work and runtime in the source benchmark.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 12 and Section 6.8",
      "page": 20,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "An ordering can delay corrections across scans."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source permits up to LLSP(G) iterations and reports greater work at small capacity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "A high-diameter directed graph and edge order reveal useful corrections slowly, while a tight sketch cap forces repeated reductions.",
    "uncertainty": "The paper does not identify a family attaining the formal upper bound."
  }
}
```

# Progressive Beam Io Expansion

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Route depth is a controlled proxy for that workload demand."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph degree",
      "Query count",
      "Result count",
      "Storage layout"
    ],
    "expected_observation": "The long-route case crosses the I/O or recall guard while the short-route case does not.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Progressive beam long path",
    "graph_scale": "Symbolic fixed degree with route depth varied.",
    "graph_shape": "A search graph containing short easy routes and long ambiguous routes to exact neighbors.",
    "independent_oracle": "Exact nearest-neighbor ranking plus a fixed-width parameter sweep.",
    "premises": [
      "The source defines DynamicWidth and reports longer-iteration, high-dimensional, and high-result regimes."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "A source-derived minimal graph is unavailable.",
    "varied_variables": [
      "Route depth",
      "Width-growth schedule",
      "Recall target"
    ],
    "workload": "Compare progressive and fixed beam schedules at matched result count and recall."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-GROW-BEAM-WIDTH-PROGRESSIVELY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Fixed and progressive runs are compared at identical result and reranking semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "io_progressive >= io_fixed OR recall_progressive < recall_requirement",
    "measurement_needed": "Sweep width schedules at matched recall while tracing reads and iterations.",
    "numeric_constants": [],
    "premises": [
      "The source defines the progressive schedule and reports longer iterations and diminishing gains in demanding regimes."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The crossover varies by graph and target.",
    "variables": [
      {
        "definition": "storage reads under progressive beam width",
        "symbol": "io_progressive",
        "units": "reads per query"
      },
      {
        "definition": "storage reads under the matched fixed-width baseline",
        "symbol": "io_fixed",
        "units": "reads per query"
      },
      {
        "definition": "recall under progressive width",
        "symbol": "recall_progressive",
        "units": "ratio"
      },
      {
        "definition": "required admission recall",
        "symbol": "recall_requirement",
        "units": "ratio"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The progressive schedule is compared with a fixed-width baseline at matched search semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source defines DynamicWidth and reports demanding high-dimensional and high-result regimes."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Progressive width assumes early narrow search preserves useful candidates and later widening does not create excessive iterations or reads.",
    "uncertainty": "The safe schedule depends on graph, dimension, and recall target."
  },
  "confidence_rationale": {
    "assumptions": [
      "Matched-recall read tracing isolates the width policy."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports the mechanism and demanding regimes, but not the claimed progressive-versus-fixed reversal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The mechanism and demanding regimes are source-reported; progressive widening crossing a matched fixed-width I/O or recall guard is a derived hypothesis.",
    "uncertainty": "No independent reproduction was performed."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Matched-recall read tracing isolates the width-growth schedule."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports iteration growth and diminishing benefit in demanding regimes."
    ],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "Progressive search performs more reads than the admitted fixed baseline or misses the required recall.",
    "uncertainty": "The source does not report the matched fixed-versus-progressive reversal or a universal target value."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-PROGRESSIVE-BEAM-IO-EXPANSION",
  "name": "Progressive Beam Io Expansion",
  "observable_symptom": {
    "assumptions": [
      "Fixed and progressive searches use identical result and reranking semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports more iterations and demanding recall regimes for the mechanism."
    ],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "I/O rises or recall falls relative to a fixed-width search at the same task target.",
    "uncertainty": "The source does not isolate progressive widening as the cause of a fixed-baseline reversal."
  },
  "repair_options": [
    {
      "description": "Cap beam width and total reads per query.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Switch to a verified fixed-width schedule when the cap or recall guard fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Enable progressive growth only for validated graph and recall regimes.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Progressive width schedule.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.1, Dynamic Width",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Recall loss and high-dimensional degradation.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 3 and DynamicWidth discussion",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Longer-iteration and high-recall limitation.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Top-100 discussion and conclusion",
      "page": 12,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Longer routes force the tested schedule through enough width-growth steps to expose its extra work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports high-dimensional, high-result, and longer-iteration regimes."
    ],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "High-dimensional, high-recall, or long top-result searches require enough iterations that progressive widening becomes too broad.",
    "uncertainty": "The source does not provide a portable iteration cutoff or this exact adversarial comparison."
  }
}
```

# Sparse Row Advantage Vanishes

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The synthetic family isolates row occupancy."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph order",
      "Nonzero count",
      "Tile dimensions",
      "Value width"
    ],
    "expected_observation": "Decoded triples match while SCSR loses its byte or amortized-time advantage at one clustering level.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Sparse row clustering sweep",
    "graph_scale": "Symbolic tile grid with nonempty-row count varied.",
    "graph_shape": "Graphs with identical order and nonzero count but edges progressively concentrated into fewer tile rows.",
    "independent_oracle": "Canonical coordinate triples sorted by row and column.",
    "premises": [
      "The source links nonempty-row count to storage and reports clustering sensitivity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The comparison format and crossover must be fixed by implementation.",
    "varied_variables": [
      "Nonempty tile-row count",
      "Scan count"
    ],
    "workload": "Convert to SCSR and a comparison sparse format, then execute exact repeated tile scans."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PACK-NONEMPTY-SPARSE-ROWS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both formats encode identical tiles and values."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "bytes_scsr >= bytes_reference OR T_conversion + scans * T_scsr >= scans * T_reference",
    "measurement_needed": "Measure nnr, nnz, bytes, conversion time, and scan time across clustering levels.",
    "numeric_constants": [],
    "premises": [
      "The source exposes storage terms, conversion, and a clustered case with reduced benefit."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The byte and amortized-time crossover is dataset dependent.",
    "variables": [
      {
        "definition": "complete SCSR bytes",
        "symbol": "bytes_scsr",
        "units": "bytes"
      },
      {
        "definition": "complete comparison-format bytes",
        "symbol": "bytes_reference",
        "units": "bytes"
      },
      {
        "definition": "time to convert the graph to SCSR",
        "symbol": "T_conversion",
        "units": "time"
      },
      {
        "definition": "number of post-conversion scans",
        "symbol": "scans",
        "units": "scans"
      },
      {
        "definition": "time per SCSR scan",
        "symbol": "T_scsr",
        "units": "time per scan"
      },
      {
        "definition": "time per comparison-format scan",
        "symbol": "T_reference",
        "units": "time per scan"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The alternative representation is measured over the same tiles and value width."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives the SCSR storage expression, clustered-graph comparison, and conversion process."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Packing only nonempty rows assumes saved empty-row headers exceed added nonempty-row metadata and conversion cost.",
    "uncertainty": "The source does not state a universal density crossover."
  },
  "confidence_rationale": {
    "assumptions": [
      "The chosen comparison format is implemented faithfully."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source provides both formula and sensitivity evidence."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Reduced benefit on clustered data is sourced; a full reversal and minimal fixture are derived test targets.",
    "uncertainty": "No local benchmark has established the terminal crossover."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle counts every required metadata byte."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism trades headers and conversion for sparse-row savings."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "SCSR bytes or amortized scan time cease to beat the comparison format while decoded nonzeros remain exact.",
    "uncertainty": "No source-reported universal crossover exists."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES",
  "name": "Sparse Row Advantage Vanishes",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "SCSR provides less additional speed benefit, while conversion and row metadata remain.",
    "uncertainty": "The source reports reduced significance, not universal regression."
  },
  "repair_options": [
    {
      "description": "Estimate nonempty-row density and reuse before converting.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Retain the comparison representation when the guard fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure the clustered-graph byte and time crossover.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-1602.02864"
  ],
  "source_pointers": [
    {
      "claim_scope": "SCSR row-header and nonzero storage terms.",
      "locator_type": "EQUATION",
      "locator_value": "Section 3.2, S_SCSR storage expression",
      "page": 4,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Smaller measured benefit on an already clustered graph.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 13 and Section 5.4",
      "page": 10,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "One-time sequential conversion cost.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Table 2 and format-conversion paragraph",
      "page": 11,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "A well-clustered graph whose comparison representation is already compact leaves less empty-row overhead for SCSR to remove.",
    "uncertainty": "The degree of clustering needed for reversal is not reported."
  }
}
```

# Decoupled Layout Doubles Reads

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture preserves algorithm semantics and varies only layout-relevant properties."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "vectors",
      "queries",
      "recall",
      "cache state",
      "block size"
    ],
    "expected_observation": "Decoupled navigation plus vector reads exceed coupled reads without a recall gain",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "weak pruning low dimension",
    "graph_scale": "Small deterministic index that spans several storage blocks",
    "graph_shape": "Proximity graph whose candidates survive approximate pruning and whose compact vectors fit well beside navigation records",
    "independent_oracle": "Coupled-layout exact candidate sequence and matched-recall block trace",
    "premises": [
      "The source reports both layouts and conditional reversals."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Hardware queueing can change latency even at equal read counts.",
    "varied_variables": [
      "vector dimension",
      "pruning selectivity"
    ],
    "workload": "Search coupled and decoupled layouts at identical recall and candidate policy"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Runs are matched for recall and cache state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "navigation_block_reads + unpruned_vector_block_reads > coupled_block_reads",
    "measurement_needed": "Measure block reads at matched recall over dimension and pruning-selectivity sweeps.",
    "numeric_constants": [],
    "premises": [
      "Decoupling creates separate block classes.",
      "Pruning determines whether vector reads are avoided."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source-reported dimensional crossover is not generalized.",
    "variables": [
      {
        "definition": "blocks fetched for topology and approximate guidance",
        "symbol": "navigation_block_reads",
        "units": "blocks"
      },
      {
        "definition": "separate vector blocks fetched after pruning",
        "symbol": "unpruned_vector_block_reads",
        "units": "blocks"
      },
      {
        "definition": "blocks fetched by the coupled equivalent",
        "symbol": "coupled_block_reads",
        "units": "blocks"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Matched-recall search uses equivalent graph and vectors."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source describes separate reads and pruning.",
      "Its evaluation reports workload-dependent reversals."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Separate navigation and vector blocks win only when avoided refinement reads outweigh the loss of coupled one-fetch locality.",
    "uncertainty": "The crossover depends on dimension, recall, block packing, and pruning selectivity."
  },
  "confidence_rationale": {
    "assumptions": [
      "Reported layouts correspond to the same search objective."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The exact cited pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source reports the reversal and mechanism, while the exact crossover remains system-specific.",
    "uncertainty": "No portable dimensional breakpoint is claimed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Counters distinguish navigation and vector blocks."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Separate reads are required for surviving candidates."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Matched-recall decoupled reads or latency exceed the coupled baseline.",
    "uncertainty": "Read count may not fully predict latency under concurrency."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS",
  "name": "Decoupled Layout Doubles Reads",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Separate navigation and refinement reads increase I/O and can underperform the coupled layout.",
    "uncertainty": "The magnitude is implementation and dataset dependent."
  },
  "repair_options": [
    {
      "description": "Choose layout only after a dimension and pruning-selectivity probe.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Retain a coupled layout for low-dimensional or weak-pruning indexes.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Pack small vectors with navigation records while decoupling only large vectors.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-2603.01779"
  ],
  "source_pointers": [
    {
      "claim_scope": "Coupled and decoupled read paths and pruning dependency",
      "locator_type": "SECTION",
      "locator_value": "Section 3.2.1",
      "page": 5,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Dimension-sensitive crossover and decoupled I/O inflation",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.2, Global Layout Findings",
      "page": 10,
      "paper_id": "PAPER-2603.01779",
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
    "text": "Low-dimensional or weak-pruning search can require both navigation and vector blocks where a coupled block would have served both phases.",
    "uncertainty": "The reported dimensional boundary is specific to the evaluated system."
  }
}
```

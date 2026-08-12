# Diffuse Residuals Erase Speedup

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture can shape residual contributions without changing the oracle."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "seed",
      "diffusion depth",
      "top-result size"
    ],
    "expected_observation": "The accepted prefix expands until its measured work meets or exceeds baseline work before the precision oracle is matched.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "uniform-residual-subgraph-prefix",
    "graph_scale": "Use the smallest fan-out that forces multiple residual seeds to affect the accepted top result.",
    "graph_shape": "A bounded-depth neighborhood where first-stage residual scores are nearly uniform and later subgraphs contribute distinct top candidates.",
    "independent_oracle": "Complete evaluation of every nonzero Equation 8 contribution supplies the precision oracle.",
    "premises": [
      "The source equation is additive and the optimization relies on empirical sparsity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Constructing exact uniform residuals may require weighted edges.",
    "varied_variables": [
      "residual concentration",
      "selected prefix length"
    ],
    "workload": "Compare residual-prefix schedules against complete Equation 8 evaluation and undecomposed diffusion."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SELECT-HIGH-RESIDUAL-SUBGRAPHS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both methods target the same declared precision acceptance condition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "selected_subgraph_work >= baseline_diffusion_work",
    "measurement_needed": "Sweep residual concentration and selected prefix while measuring precision, BFS preparation, and diffusion work.",
    "numeric_constants": [],
    "premises": [
      "Residual selection trades omitted terms for less work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Work-unit weighting and precision target are workload-specific.",
    "variables": [
      {
        "definition": "Preparation and diffusion work for the residual prefix needed by the precision target.",
        "symbol": "selected_subgraph_work",
        "units": "work units"
      },
      {
        "definition": "Work of the undecomposed local diffusion baseline.",
        "symbol": "baseline_diffusion_work",
        "units": "work units"
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
    "text": "Residual score mass is concentrated enough that a small high-residual prefix captures the desired top-result precision.",
    "uncertainty": "This is empirical on the evaluated graph and seed sample."
  },
  "confidence_rationale": {
    "assumptions": [
      "Residual concentration can vary by graph and seed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Higher selected work reduces measured speedup in source data."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The source observes the speed-precision reversal and supplies the additive mechanism; unseen diffuse distributions remain an analytical stress case.",
    "uncertainty": "No source dataset demonstrates the constructed limiting distribution."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "All preparation and communication work is counted."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The fixture diffuses residual importance."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "No selected prefix satisfies both the declared precision condition and a strict work reduction over baseline.",
    "uncertainty": "Hardware parallelism may alter latency while not reducing work."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-DIFFUSE-RESIDUALS-ERASE-SPEEDUP",
  "name": "Diffuse Residuals Erase Speedup",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-003"
    ],
    "text": "As more next-stage nodes are processed, measured speedup decreases and some CPU cases become slower than the baseline at higher precision.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Estimate residual concentration after the first stage and fall back when the admitted prefix is too broad.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use complete or undecomposed diffusion when a deterministic precision requirement cannot be met cheaply.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2104.09616"
  ],
  "source_pointers": [
    {
      "claim_scope": "Exact staged diffusion sums a contribution for every nonzero residual seed; selection assumes residual concentration.",
      "locator_type": "EQUATION",
      "locator_value": "Equation 8 and Section IV-D",
      "page": 3,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Measured precision rises with selected residual fraction on evaluated graphs.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 6 and Section VI-C",
      "page": 5,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Higher precision reduces speedup and some CPU cases become slower than baseline.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and Section VI-C continuation",
      "page": 6,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The workload lacks the source-observed concentration."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Equation 8 assigns additive work to residual-seeded subgraphs.",
      "Omitting more diffuse residual terms can change ranking."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Residual mass is diffuse across many vertices, so meeting the declared precision target requires processing a broad prefix of later-stage subgraphs.",
    "uncertainty": "No deterministic residual-to-precision bound is reported."
  }
}
```

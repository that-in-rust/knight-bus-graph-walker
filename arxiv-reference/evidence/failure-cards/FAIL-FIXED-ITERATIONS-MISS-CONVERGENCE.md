# Fixed iterations miss PageRank convergence

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The oracle implements the complete PageRank equation independently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Damping semantics",
      "Initialization",
      "Numeric precision",
      "Tolerance definition"
    ],
    "expected_observation": "The fixed-count output exceeds tolerance or disagrees with the normalized oracle",
    "fixture_kind": "GRAPH",
    "fixture_name": "dangling slow-convergence PageRank",
    "graph_scale": "Smallest graph that separates fixed-count output from the requested tolerance",
    "graph_shape": "A directed graph containing dangling vertices and a slowly mixing component",
    "independent_oracle": "A high-precision convergence-driven PageRank implementation with mass conservation checks",
    "premises": [
      "The source identifies fixed iterations and omitted dangling correction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Floating-point reduction order may cause small differences below the semantic tolerance.",
    "varied_variables": [
      "Iteration limit",
      "Dangling structure",
      "Mixing rate"
    ],
    "workload": "Run fixed-count PageRank and a convergence-driven dangling-aware PageRank"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-MATERIALIZE-EACH-PIPELINE-STAGE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A standard converged implementation supplies the intended semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "residual(iteration_limit) > convergence_tolerance OR dangling_mass_omitted = true",
    "measurement_needed": "Measure per-iteration residual and compare normalized ranks with a dangling-aware converged oracle.",
    "numeric_constants": [],
    "premises": [
      "The source fixes iterations and omits a PageRank term."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The graph-dependent iteration count is not reported as a universal constant.",
    "variables": [
      {
        "definition": "Norm of the PageRank update residual after the fixed iteration limit",
        "symbol": "residual",
        "units": "rank mass"
      },
      {
        "definition": "Configured number of iterations",
        "symbol": "iteration_limit",
        "units": "iterations"
      },
      {
        "definition": "Required residual tolerance",
        "symbol": "convergence_tolerance",
        "units": "rank mass"
      },
      {
        "definition": "Whether dangling mass redistribution is omitted",
        "symbol": "dangling_mass_omitted",
        "units": "boolean"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "A production PageRank contract requires a declared convergence tolerance and dangling-mass semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source benchmark fixes the iteration count and omits dangling-node correction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "A fixed iteration count with omitted dangling-node correction is not a general PageRank convergence and correctness contract.",
    "uncertainty": "The paper presents a benchmark kernel, not a claimed universal PageRank contract."
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
      "FP-002",
      "FP-003"
    ],
    "text": "The fixed-count and omitted-term premises are source-stated; failure against a convergence-driven dangling-aware oracle is derived and unexecuted.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle and tolerance are fixed before running the test."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source does not claim production convergence."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Residual or normalized-rank error exceeds the declared tolerance, or total rank mass violates the oracle's conservation check.",
    "uncertainty": "A universal failing iteration count is not asserted."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-FIXED-ITERATIONS-MISS-CONVERGENCE",
  "name": "Fixed iterations miss PageRank convergence",
  "observable_symptom": {
    "assumptions": [
      "The independent oracle and tolerance define the intended PageRank semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source fixes iterations and omits dangling-node correction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The materialized result differs from a converged dangling-aware PageRank oracle or remains outside the requested residual tolerance.",
    "uncertainty": "The source does not report this oracle comparison."
  },
  "repair_options": [
    {
      "description": "Reject or reroute workloads once the symbolic failure predicate is observed.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use a correctness-preserving fallback when the optimized path's assumptions fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-1603.01876"
  ],
  "source_pointers": [
    {
      "claim_scope": "Fixed PageRank iteration count used by the proposed benchmark pipeline.",
      "locator_type": "SECTION",
      "locator_value": "Section IV.D",
      "page": 5,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Dangling-node correction is omitted from the benchmark kernel.",
      "locator_type": "SECTION",
      "locator_value": "Serial PageRank implementation",
      "page": 6,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Dangling-node handling and validation outputs remain unresolved.",
      "locator_type": "SECTION",
      "locator_value": "Section V",
      "page": 8,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "A graph can remain above the declared residual tolerance or expose omitted dangling mass at the fixed limit."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source uses a fixed iteration count and omits one PageRank term."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Run the fixed-count kernel on a graph whose residual remains above tolerance or whose dangling mass changes the normalized stationary vector.",
    "uncertainty": "No universal failing graph or iteration count is claimed by the source."
  }
}
```

# Early Stopping Weakens Guarantees

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The forced stop occurs before the independently observed convergence condition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "quality function",
      "resolution",
      "random seed",
      "node order"
    ],
    "expected_observation": "The early result lacks the stronger convergence witness and differs in partition or quality from the unrestricted run.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Premature Leiden termination",
    "graph_scale": "Minimal graph that exposes a bridge-sensitive community and later improving moves.",
    "graph_shape": "A community graph whose refined partition continues changing after an imposed stop.",
    "independent_oracle": "Induced-subgraph connectivity and direct quality recomputation, plus the unrestricted convergence run.",
    "premises": [
      "The source distinguishes guarantee stages and reports delayed stability."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Stochastic runs require controlled seeds and may converge along different paths.",
    "varied_variables": [
      "iteration limit"
    ],
    "workload": "Run Leiden with a forced early stop and again until the convergence condition named by the claimed guarantee."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The claimed property requires the corresponding source convergence condition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "stop_iteration < convergence_iteration",
    "measurement_needed": "Track partition changes, quality, connectivity, and the exact convergence predicate on the target workload.",
    "numeric_constants": [],
    "premises": [
      "The source assigns stronger guarantees to stable and asymptotic states and reports delayed stability."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No universal iteration count predicts convergence for arbitrary graphs.",
    "variables": [
      {
        "definition": "Iteration at which execution is terminated",
        "symbol": "stop_iteration",
        "units": "iteration index"
      },
      {
        "definition": "Iteration satisfying the claimed convergence condition",
        "symbol": "convergence_iteration",
        "units": "iteration index"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The claimed stable or asymptotic property requires the corresponding source convergence condition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source distinguishes guarantee levels reached after each, stable, and asymptotic iterations."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Stable-iteration and asymptotic guarantees apply only after their respective convergence conditions, not after an arbitrary early stop.",
    "uncertainty": "The source distinguishes several guarantee levels."
  },
  "confidence_rationale": {
    "assumptions": [
      "The source's undirected modularity or CPM setup is preserved."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The theoretical table and empirical iteration discussion agree on the boundary."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Confidence is high because the source explicitly separates guarantee levels and reports delayed stability on difficult networks.",
    "uncertainty": "No independent implementation reproduction or directed-graph extension was performed."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle evaluates the same quality function and convergence predicate."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source ties stronger guarantees to later convergence states."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The early run has no valid stable/asymptotic witness and later iterations still change or improve the partition.",
    "uncertainty": "The number of additional iterations is not predictable from the source."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-EARLY-STOPPING-WEAKENS-GUARANTEES",
  "name": "Early Stopping Weakens Guarantees",
  "observable_symptom": {
    "assumptions": [
      "Execution is stopped before the independently observed convergence condition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source ties stronger guarantees to stable and asymptotic states and reports delayed stability."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "An early stopped run retains only the guarantees established for completed iterations and cannot claim the stronger stable or asymptotic properties.",
    "uncertainty": "Per-iteration connectivity may still hold."
  },
  "repair_options": [
    {
      "description": "State only the guarantee justified by the actual stop condition.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Require a stable or asymptotic witness before advertising the corresponding property.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Quote iteration and runtime budgets separately from convergence guarantees.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Return an explicitly early-stopped partition when the stronger convergence budget is exhausted.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-1810.08473"
  ],
  "source_pointers": [
    {
      "claim_scope": "Different guarantees after each, stable, and asymptotic iterations.",
      "locator_type": "TABLE",
      "locator_value": "Table I and Section III-A",
      "page": 5,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Difficult empirical networks require many iterations and continue improving.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 10 and Section V",
      "page": 10,
      "paper_id": "PAPER-1810.08473",
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
    "text": "Difficult empirical networks can remain partition-changing and quality-improving for many iterations.",
    "uncertainty": "Iteration counts are dataset-specific."
  }
}
```

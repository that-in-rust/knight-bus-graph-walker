# Clustered filters misroute execution plans

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Forced modes preserve result-quality requirements."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Query",
      "Selectivity",
      "Filter precision",
      "Index",
      "Hardware"
    ],
    "expected_observation": "The selected mode has higher actual cost than at least one forced alternative",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "clustered-valid routing reversal",
    "graph_scale": "Symbolic dataset containing the same selectivity under distinct spatial distributions",
    "graph_shape": "ANN graph with valid vectors concentrated near or far from the query region",
    "independent_oracle": "Forced execution of every mode with measured I/O and compute",
    "premises": [
      "The source identifies uniform distribution and early termination as model gaps."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "A cost-estimation error need not always reverse the chosen mode.",
    "varied_variables": [
      "Spatial distribution of valid vectors",
      "Early-termination opportunity"
    ],
    "workload": "Estimate costs, record chosen mode, then force every supported filtering mode"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-ROUTE-FILTERS-BY-COST"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A routing error is defined by actual cost ordering, not estimation error alone."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "estimated_cost(chosen_mode) > actual_cost(alternative_mode)",
    "measurement_needed": "Force each filtering mode for the same query and compare actual cost with the router's choice.",
    "numeric_constants": [],
    "premises": [
      "The source reports estimation errors caused by distribution and early termination."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not report every case where estimation error changes the selected mode.",
    "variables": [
      {
        "definition": "Mode cost predicted before query execution",
        "symbol": "estimated_cost",
        "units": "weighted cost units"
      },
      {
        "definition": "Filtering mode with minimum predicted cost",
        "symbol": "chosen_mode",
        "units": "mode"
      },
      {
        "definition": "Measured cost under controlled execution",
        "symbol": "actual_cost",
        "units": "weighted cost units"
      },
      {
        "definition": "Nonchosen filtering mode",
        "symbol": "alternative_mode",
        "units": "mode"
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
    "text": "The routing model's uniform-distribution and simplified early-termination assumptions can make estimated I/O differ from actual I/O on clustered filters.",
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
      "All modes are forced under equal quality targets."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports both over- and under-estimation."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The router's chosen mode is not the minimum measured-cost mode at equal recall and output semantics.",
    "uncertainty": "The frequency of misrouting is not established by this fixture."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-CLUSTERED-FILTERS-MISROUTE-PLANS",
  "name": "Clustered filters misroute execution plans",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The cost model under- or over-estimates graph I/O and can choose a filtering mode whose actual cost is not minimal.",
    "uncertainty": "NONE"
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
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "Cost routing assumes selectivity- and precision-scaled candidate behavior.",
      "locator_type": "TABLE",
      "locator_value": "Table 1 and Section 4.2",
      "page": 6,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Clustered valid vectors and early termination cause model under- and over-estimation.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 10-11 and Section 5.4",
      "page": 11,
      "paper_id": "PAPER-2605.17992",
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
    "text": "Issue filtered nearest-neighbor queries whose valid vectors are spatially clustered rather than uniformly distributed.",
    "uncertainty": "NONE"
  }
}
```

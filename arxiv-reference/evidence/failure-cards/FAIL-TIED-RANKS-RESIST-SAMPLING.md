# Tied ranks resist endpoint sampling

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The numerical oracle attains substantially smaller error than the sampled estimator."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Source vertex",
      "Stopping probability",
      "Sample count",
      "Random generator family"
    ],
    "expected_observation": "Sampled rank order is unstable or differs from the oracle when the gap is below sampling uncertainty",
    "fixture_kind": "GRAPH",
    "fixture_name": "near-tied personalized ranks",
    "graph_scale": "Small graph with controllable symmetry-breaking edge weight or structure",
    "graph_shape": "A directed graph with two vertices having equal or arbitrarily close personalized PageRank from one source",
    "independent_oracle": "High-precision linear solve or convergence-driven personalized PageRank",
    "premises": [
      "The source reports weaker behavior for low and near-tied ranks."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Exactly tied scores have no unique strict ordering; the fixture records instability separately from semantic error.",
    "varied_variables": [
      "Score gap",
      "Random seed"
    ],
    "workload": "Build endpoint fingerprints repeatedly and request the relative order of the near-tied vertices"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-STORE-RANDOM-WALK-ENDPOINTS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "An exact or converged numerical oracle resolves the compared scores."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "score_gap < sampling_error_bound",
    "measurement_needed": "Repeat independent endpoint samples and compare rank order with high-precision personalized PageRank.",
    "numeric_constants": [],
    "premises": [
      "The source distinguishes separated high ranks from weak low-rank ordering."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The finite-sample error bound depends on sample count and graph distribution.",
    "variables": [
      {
        "definition": "Absolute personalized PageRank difference between compared vertices",
        "symbol": "score_gap",
        "units": "rank probability"
      },
      {
        "definition": "Uncertainty of the finite endpoint estimator",
        "symbol": "sampling_error_bound",
        "units": "rank probability"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Finite randomized endpoint samples do not reliably establish exact ordering among very low or near-tied personalized PageRank scores.",
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
      "FP-001"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle distinguishes exact ties from near ties."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source limits reliable ordering to sufficiently separated scores."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Across seeds, strict sampled order changes or disagrees with the oracle for a nonzero near tie.",
    "uncertainty": "The number of repetitions needed to observe instability is not fixed here."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-TIED-RANKS-RESIST-SAMPLING",
  "name": "Tied ranks resist endpoint sampling",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Repeated samples change the relative order or disagree with an exact high-precision personalized PageRank ranking.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Limit use of the mechanism to workloads satisfying its sourced applicability conditions.",
      "repair_class": "SPECIALIZE_WORKLOAD"
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
    "PAPER-HASH-0232e71ded2b5c43"
  ],
  "source_pointers": [
    {
      "claim_scope": "Ranking converges for separated scores but low and near-tied ranks have weaker behavior.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Discussion after Theorems 3.1-3.2",
      "page": 14,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Query a source whose candidate vertices have score gaps smaller than the endpoint estimator's sampling uncertainty.",
    "uncertainty": "NONE"
  }
}
```

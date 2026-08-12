# Correlated Label Filter Reversal

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture preserves marginals to isolate correlation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Label marginals",
      "Labels per vector",
      "Queries",
      "Bloom size"
    ],
    "expected_observation": "Both paths return identical valid sets, but the correlated case crosses candidate or total-work bounds.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Correlated label reversal",
    "graph_scale": "Symbolic fixed vectors, graph edges, labels, and query count.",
    "graph_shape": "Equal-size vector graphs with identical label marginals but independent versus strongly correlated label assignments.",
    "independent_oracle": "Exact per-vector label evaluation before graph-search admission.",
    "premises": [
      "The source calls out independence, clustering, and collision sensitivity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The smallest correlation strength causing reversal is unknown.",
    "varied_variables": [
      "Label correlation",
      "Boolean expression"
    ],
    "workload": "Run the same Boolean filters through hybrid rare-scan/Bloom and exact-label paths."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SCAN-RARE-FILTER-FREQUENT"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Hybrid and exact filters implement identical Boolean semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "cost_hybrid >= cost_exact OR false_positive_candidates > candidate_budget",
    "measurement_needed": "Measure exact candidates, Bloom false positives, posting bytes, reads, and total work across label correlations.",
    "numeric_constants": [],
    "premises": [
      "The source identifies Bloom collisions and cost-model error under nonuniform distributions."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "No portable candidate or cost crossover is reported.",
    "variables": [
      {
        "definition": "posting scans, merges, Bloom checks, deferred exact checks, and reads",
        "symbol": "cost_hybrid",
        "units": "work units"
      },
      {
        "definition": "exact-label filtering work for the same candidates",
        "symbol": "cost_exact",
        "units": "work units"
      },
      {
        "definition": "candidates admitted only by probabilistic false positives",
        "symbol": "false_positive_candidates",
        "units": "candidates per query"
      },
      {
        "definition": "admitted false-positive candidate count",
        "symbol": "candidate_budget",
        "units": "candidates per query"
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
    "text": "The hybrid filter assumes rare postings materially shrink candidates and Bloom false positives remain controlled under the workload label distribution.",
    "uncertainty": "The cost model also assumes a simplified distribution of valid nodes."
  },
  "confidence_rationale": {
    "assumptions": [
      "Exact labels provide an independent Boolean oracle."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports independence assumptions and model error."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The model limitations are sourced; the same-marginal correlation counterexample is derived.",
    "uncertainty": "No local query benchmark was run."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "All candidate and memory terms are counted."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source filter uses probabilistic superset checks plus deferred exact validation."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The hybrid path admits excess false positives or costs at least the exact path while Boolean results remain exact after deferred checks.",
    "uncertainty": "The threshold must be measured."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-CORRELATED-LABEL-FILTER-REVERSAL",
  "name": "Correlated Label Filter Reversal",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Candidate and false-positive work is underpredicted, while every vector retains probabilistic-filter memory.",
    "uncertainty": "Physical read and candidate crossover depends on query mix."
  },
  "repair_options": [
    {
      "description": "Choose the hybrid route only from measured joint-label selectivity and false-positive work.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use exact or posting-only filtering when the cost guard fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure correlation-sensitive selectivity rather than assuming label independence.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "Rare posting scans, Bloom fallback, collision risk, and label-independence assumption.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.1, Label Filtering",
      "page": 7,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Model over- and under-estimation under clustered or nonuniform valid nodes.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 5.3 cost-model error discussion",
      "page": 11,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Per-vector probabilistic-filter memory.",
      "locator_type": "TABLE",
      "locator_value": "Table 3 and Section 5.4",
      "page": 12,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Marginals can be held fixed while correlation is varied."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source identifies independence, distribution, and collision limitations."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Strongly correlated Boolean labels with unchanged marginals, many labels per vector, or rare postings that do not shrink the candidate set violate the model.",
    "uncertainty": "The source does not provide a minimal correlated fixture."
  }
}
```

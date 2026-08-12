# Symmetric sets reverse probing benefit

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Operation counts approximate the source analytical model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Set contents",
      "Overlap",
      "Dictionary implementation",
      "Machine"
    ],
    "expected_observation": "Both outputs are equal and probing performs at least as much work as the alternative",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "equal-cardinality intersection reversal",
    "graph_scale": "Symbolic cardinality sufficient to amortize setup in both paths",
    "graph_shape": "Exact sets with controlled overlap and equal or near-equal cardinality",
    "independent_oracle": "Scalar exact intersection equality plus operation counters",
    "premises": [
      "The source limits the optimization to asymmetric intersections."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Real cache effects may shift the observed reversal.",
    "varied_variables": [
      "Cardinality ratio",
      "Number of sets"
    ],
    "workload": "Run smallest-set probing and the source alternative on identical inputs"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PROBE-SMALLEST-SET-FIRST"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Lookup work scales with smallest-set candidates and consulted sets."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "smallest_set_size * set_count >= approximate_path_cost",
    "measurement_needed": "Count membership probes and alternative-path operations on cardinality-controlled sets.",
    "numeric_constants": [],
    "premises": [
      "The source states the probing optimization for maximal asymmetric intersections."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The source supplies an analytical cost comparison, not a cache-level crossover.",
    "variables": [
      {
        "definition": "Cardinality of the selected probe set",
        "symbol": "smallest_set_size",
        "units": "elements"
      },
      {
        "definition": "Number of membership indices consulted per candidate up to constant factors",
        "symbol": "set_count",
        "units": "sets"
      },
      {
        "definition": "Work of the alternative intersection path",
        "symbol": "approximate_path_cost",
        "units": "operations"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Lookup work follows the source analytical cost terms."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives a sufficient advantage condition for asymmetric intersections."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Smallest-set probing is advantageous only for sufficiently asymmetric intersections; it offers no advantage when probing all other sets costs at least the approximate path.",
    "uncertainty": "The complement of a sufficient condition is not itself a source-reported regression."
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
    "text": "The cited appendix supplies the asymmetric applicability condition; reversal for symmetric sets is a derived cost comparison rather than a reported negative result.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The alternative is implemented independently and outputs are verified."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives the asymmetric applicability boundary."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Operation or elapsed-work counters show the probing path does not beat the alternative while exact outputs match.",
    "uncertainty": "No universal cardinality ratio is claimed."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-SYMMETRIC-SETS-REVERSE-COST",
  "name": "Symmetric sets reverse probing benefit",
  "observable_symptom": {
    "assumptions": [
      "Operation counters approximate the source cost model for both paths."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source bounds the optimization under asymmetric cardinalities."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The exact probing path performs no less work than the approximate packed path while returning the same set.",
    "uncertainty": "Cache and implementation effects can shift or remove the derived reversal."
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
    "PAPER-0708.3259"
  ],
  "source_pointers": [
    {
      "claim_scope": "Smallest-set probing cost for maximal asymmetric intersections.",
      "locator_type": "APPENDIX",
      "locator_value": "Appendix C",
      "page": 16,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Similar set cardinalities remove the asymmetry required by the sufficient condition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states the probing optimization for asymmetric intersections."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Intersect sets with similar cardinalities so the chosen smallest set is not materially smaller than the rest.",
    "uncertainty": "The source does not report this symmetric-set reversal workload."
  }
}
```

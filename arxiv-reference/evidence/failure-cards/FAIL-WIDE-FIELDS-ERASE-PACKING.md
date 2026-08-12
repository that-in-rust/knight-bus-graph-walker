# Wide fields erase packing benefit

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The scalar oracle is independent of the packed representation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Set contents",
      "Set cardinalities",
      "Operation sequence",
      "Machine"
    ],
    "expected_observation": "Packed and scalar outputs remain equal while packed work ceases to improve over scalar work",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "wide-field packed-set reversal",
    "graph_scale": "Symbolic set sizes with fixed cardinality and increasing encoded field width",
    "graph_shape": "Two exact sets partitioned into the same bucket prefixes",
    "independent_oracle": "A scalar exact set implementation plus element-wise equality",
    "premises": [
      "Packed operations preserve exact set semantics."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The workload is derived; no measured crossover is claimed.",
    "varied_variables": [
      "Encoded field width",
      "Bucket occupancy distribution"
    ],
    "workload": "Run exact union and intersection through packed and scalar reference implementations"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-BALANCE-BUCKETED-PACKED-SETS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The implementation cost is monotone in the named work terms."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "field_width * fields_per_item > word_width OR directory_work >= payload_work",
    "measurement_needed": "Measure encoded field width, bucket occupancy, and directory versus payload operations.",
    "numeric_constants": [],
    "premises": [
      "The source conditions speedup on word packing and balanced buckets."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source gives an operation bound rather than a hardware crossover.",
    "variables": [
      {
        "definition": "Bits in one encoded field",
        "symbol": "field_width",
        "units": "bits"
      },
      {
        "definition": "Encoded fields required per packed item",
        "symbol": "fields_per_item",
        "units": "fields per item"
      },
      {
        "definition": "Machine word width",
        "symbol": "word_width",
        "units": "bits"
      },
      {
        "definition": "Work spent locating buckets",
        "symbol": "directory_work",
        "units": "operations"
      },
      {
        "definition": "Work spent on packed payloads",
        "symbol": "payload_work",
        "units": "operations"
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
    "text": "Word-parallel benefit requires fields small enough relative to the machine word and a bucket balance that does not let directory overhead dominate.",
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
    "text": "The cited operation bounds support the mechanism premise; the derived reversal fixture isolates that premise without asserting a reported negative result or numeric crossover.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Operation counts or elapsed work can reveal the reversal."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source defines the packed speedup condition."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Packed execution no longer performs less measured work than the scalar oracle while both return identical sets.",
    "uncertainty": "Hardware effects can move the crossover."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-WIDE-FIELDS-ERASE-PACKING",
  "name": "Wide fields erase packing benefit",
  "observable_symptom": {
    "assumptions": [
      "Operation counts represent the target implementation's dominant work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives packed-operation and directory-work bounds under explicit capacity and balance conditions."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The asymptotic packed-operation speedup diminishes and directory work can dominate payload work.",
    "uncertainty": "The paper does not report a hardware-level reversal outside the sufficient conditions."
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
      "claim_scope": "Packed-field word-parallel operations and their field-width dependence.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1 and Lemma 3",
      "page": 10,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Balance between bucket directory overhead and packed payload work.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.2 and Lemmas 4-5",
      "page": 11,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The target implementation uses the same packed-word and bucket-directory cost model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source conditions its operation bounds on field capacity and bucket balance."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Use packed set operations on keys whose encoded fields consume too much of each machine word or whose buckets carry too little payload.",
    "uncertainty": "The source does not evaluate this adversarial workload or report its crossover."
  }
}
```

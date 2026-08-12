# Compressed filter drops true neighbor

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture can choose vectors that collide or nearly collide under compression."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Full vectors",
      "Query",
      "Shortlist size",
      "Distance metric"
    ],
    "expected_observation": "The exact neighbor is absent from the shortlist and therefore absent after exact reranking",
    "fixture_kind": "GRAPH",
    "fixture_name": "quantized near-tie exclusion",
    "graph_scale": "Minimal query and candidate set exhibiting a compressed-order reversal",
    "graph_shape": "A query with two near-tied vectors mapped to an ambiguous compressed code",
    "independent_oracle": "Exhaustive full-precision top-k scan",
    "premises": [
      "Quantization can reverse approximate candidate order."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The fixture is analytical and the smallest real-data PQ budget remains unknown.",
    "varied_variables": [
      "Quantization budget",
      "Near-tie gap"
    ],
    "workload": "Filter by compressed distance, rerank survivors exactly, and compare with exhaustive full precision"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-FILTER-COMPRESSED-RERANK-EXACTLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A candidate excluded before reranking cannot be restored later."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "true_neighbor NOT_IN retained_candidates",
    "measurement_needed": "Compare shortlist membership and final output with exhaustive full-precision nearest neighbors while varying PQ memory.",
    "numeric_constants": [],
    "premises": [
      "The source states that PQ loses precision and exact reranking follows candidate filtering."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The PQ budget at which exclusion occurs is dataset-dependent.",
    "variables": [
      {
        "definition": "Neighbor selected by full-precision exhaustive distance",
        "symbol": "true_neighbor",
        "units": "vertex identifier"
      },
      {
        "definition": "Candidates surviving compressed filtering",
        "symbol": "retained_candidates",
        "units": "set of vertex identifiers"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Exact reranking sees only candidates retained by compressed filtering."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states that product quantization loses precision and that exact reranking follows candidate filtering."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Exact reranking cannot recover a true neighbor that quantized filtering failed to retain as a candidate.",
    "uncertainty": "The paper does not report this specific exclusion counterexample."
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
    "text": "The cited precision caveat and reranking order support the premises; shortlist exclusion is a derived consequence rather than a reported failure.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Exhaustive full precision is independent of compressed filtering."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Exact reranking operates only on retained candidates."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The exhaustive top-k contains a vector not present in the compressed shortlist or final reranked output.",
    "uncertainty": "Whether a natural dataset contains the constructed near tie is empirical."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-COMPRESSED-FILTER-DROPS-NEIGHBOR",
  "name": "Compressed filter drops true neighbor",
  "observable_symptom": {
    "assumptions": [
      "The true neighbor is absent from the compressed shortlist."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reranks only candidates retained by the approximate filter."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Final reranking is exact within the shortlist but the final neighbor set remains inaccurate because the true neighbor is absent.",
    "uncertainty": "The source reports precision sensitivity but not this constructed omission."
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
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Product-quantization precision loss and compressed candidate filtering.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.1",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Exact reranking is applied only to candidates retained by approximate filtering.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.1 continuation",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "PQ memory allocation changes recall and throughput.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 15 and Section 6.3",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The constructed near tie reverses compressed ordering while preserving full-precision ordering."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports a compression precision tradeoff."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "text": "Use a query whose true neighbor is separated from an impostor only by distance information lost under the configured quantization budget.",
    "uncertainty": "The source does not report this exact query or the minimum quantization budget that triggers it."
  }
}
```

# Explore Superset Verify Results

- Pattern ID: `PAT-EXPLORE-SUPERSET-VERIFY-RESULTS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The selected Knight Bus predicate admits a no-false-negative approximation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Approximate filtering avoids many exact SSD reads.",
      "Final exact verification preserves result validity.",
      "Attributes may be duplicated for scan and verification layouts."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A007 can separate the cost of an approximate traversal superset from the smaller exact-verification set and must quote false-positive work and duplicated attribute storage explicitly.",
    "uncertainty": "False-positive rates and page savings require fixture-specific measurement."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Approximate checks gate graph exploration or pre-filter candidates; full vectors and exact attributes are fetched for reranking; exact validity is checked before returning results.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported implementation follows the described invariant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism, limitations, and benchmark setup are reported in the full paper."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "The paper gives a detailed implementation design and source benchmarks, but this campaign neither inspected the code nor reproduced the results.",
    "uncertainty": "Independent evidence is absent, so benchmark claims remain Grade C."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Compact approximate attribute structures reside in memory; exact attributes are stored on SSD and also colocated row-wise with full vectors used for reranking.",
    "uncertainty": "No-extra-I/O verification requires the relevant attribute bytes to share fetched pages with reranking vectors."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "A false-negative approximate filter would make the final result unsound because exact verification cannot recover a candidate that was never explored.",
      "uncertainty": "This is an invariant violation rather than a measured source failure."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "High false-positive work or failed attribute colocation can erase the intended I/O benefit.",
      "uncertainty": "The source reports workload-specific behavior rather than a universal breakpoint."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-EXPLORE-SUPERSET-VERIFY-RESULTS",
  "falsifying_test": {
    "controlled_variables": [
      "Graph, query, candidate budget, filter bits, false-positive seed, record layout, and cache state."
    ],
    "failure_signal": "Any oracle-valid result becomes unreachable, any returned result fails exact verification, or verification reads attributes before the final candidate stage.",
    "fixture": "A small disk-backed graph with exact categorical predicates, deliberately colliding approximate filters, disconnected valid regions, and colocated exact attributes.",
    "independent_oracle": "Exhaustive exact filtered search over all vertices.",
    "scope": "Correctness and staged-access behavior only."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The approximate selector has no false negatives: a rejected vector is invalid, while an accepted vector is only possibly valid; exact verification determines final validity.",
    "uncertainty": "NONE"
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus can distinguish traversal admissibility from final result validity."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source permits superset traversal followed by exact result verification."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Filtered BFS or bounded path search and node-similarity or kNN traversals are candidate Knight Bus families when predicates can expose no-false-negative approximate tests.",
      "uncertainty": "Correctness and resource benefit outside ANNS are unmeasured."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Explore an approximate superset with memory-efficient filters, allow false-positive traversal, and apply exact attribute checks after nearest candidates have been selected.",
    "uncertainty": "False positives change work and may either help connectivity or add overhead."
  },
  "name": "Explore Superset Verify Results",
  "pattern_id": "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Strict pre- and in-filtering require exact on-SSD attributes before exploration and therefore incur scans or many random attribute reads.",
    "uncertainty": "The severity depends on query selectivity, record layout, and storage hardware."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Final exact membership is recomputed from the original attributes for the selected result candidates.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-NAVIGATE-BINARY-RERANK-EXACTLY",
    "PAT-REFINE-HASHED-CANDIDATES-EXACTLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Compressed vectors, approximate attribute summaries, filter statistics, candidate pools, and optional two-hop neighbor IDs remain in memory.",
    "uncertainty": "The source reports selected component sizes but not a universal whole-process RSS formula."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Approximate in-memory checks avoid exact neighbor-attribute reads; exact attributes are read only with reranking candidates when record colocation succeeds.",
      "measurement_needed": "Record pages read for graph traversal, false positives, reranking, and attribute verification separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "False-positive exploration and page placement can add reads."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "The design duplicates indexed attributes column-wise for scans and row-wise in vector records for verification, in addition to graph and full-vector records.",
      "measurement_needed": "Measure durable bytes for graph, vectors, row attributes, column indexes, and padding.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Total storage amplification depends on attribute types and alignment."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Build approximate per-vector filters or quantized values, exact attribute indexes where useful, filter statistics, and vector records carrying exact attributes.",
      "measurement_needed": "Measure index-build time, bytes written, and incremental-update amplification.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Build time and update cost are not fully reported."
    },
    "ram": {
      "assumptions": [],
      "expression": "Compressed vectors plus per-vector probabilistic or quantized attribute summaries, statistics, candidate state, and optional dense neighbor IDs.",
      "measurement_needed": "Measure peak RSS by component for the declared dataset, selector mix, and graph degree.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Whole-process RSS and allocator overhead are not reported."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Instrument peak candidate, merge, and reranking temporary bytes per query.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Peak temporary state for merged supersets and reranking buffers is not isolated."
    }
  },
  "source_domain": "SSD-resident filtered approximate nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "No-false-negative approximate membership, superset traversal, final exact verification, and bridge nodes.",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Speculative Filtering",
      "page": 4,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Vector-record attribute colocation, probabilistic in-memory structures, exact SSD attributes, and query flow.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 4 and Section 4.1",
      "page": 5,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Measured false-positive behavior and acknowledged cost-model limitations.",
      "locator_type": "SECTION",
      "locator_value": "Section 5.4, False-positive exploration rate",
      "page": 11,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Graph records and final full-vector-plus-attribute records are read from SSD along the search path and reranking set.",
    "uncertainty": "Read amplification depends on record and page alignment."
  },
  "unknown_when": [
    {
      "assumptions": [
        "No uncited section of the fully read paper resolves the named boundary."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The cited source pointers delimit the mechanism, evaluated conditions, or stated analysis."
      ],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "The source cost model does not fully capture clustered valid-vector distributions, early termination, or all page-cache effects.",
      "uncertainty": "Model error varies by workload."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "The approximate selector never drops a valid vector and exact attributes are available for final verification.",
      "uncertainty": "NONE"
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Attribute reads avoided during traversal outweigh additional work from false-positive exploration.",
      "uncertainty": "The crossover is workload-dependent."
    }
  ]
}
```

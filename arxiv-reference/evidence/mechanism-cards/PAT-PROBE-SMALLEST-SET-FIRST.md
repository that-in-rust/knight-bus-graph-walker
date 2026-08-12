# Probe Smallest Set First

- Pattern ID: `PAT-PROBE-SMALLEST-SET-FIRST`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Set cardinalities and dictionary residency are known before execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source provides a symbolic condition for replacing approximate evaluation with exact probing."
    ],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A007 can quote asymmetric intersections with a driver-cardinality times set-count work term and refuse that plan when membership-probe locality or index residency is unknown.",
    "uncertainty": "Device-level I/O and crossover coefficients remain unknown."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Read each smallest-set element once and probe it across each remaining set before materializing the exact intersection.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "Membership lookup behaves as required by the source model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Appendix C states the transformation and its work condition."
    ],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The exactness argument and symbolic crossover are explicit, but no storage-system implementation was reproduced.",
    "uncertainty": "Practical cache and I/O costs are unknown."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "All sets expose membership dictionaries; the resulting intersection receives a balanced bucketed representation and hash table for downstream evaluation.",
    "uncertainty": "NONE"
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "The optimization offers no advantage when the intersection is not asymmetric enough for smallest-set probing to beat the approximate path.",
      "uncertainty": "This is a cost reversal, not a correctness failure."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PROBE-SMALLEST-SET-FIRST",
  "falsifying_test": {
    "controlled_variables": [
      "Set count, smallest-set size, larger-set sizes, overlap, dictionary implementation, and cache state."
    ],
    "failure_signal": "Any result mismatch or failure of the asymmetric case to perform exactly one membership decision per smallest-set element per other set.",
    "fixture": "Several exact-set intersection cases with one tiny set and several larger sets, plus a near-symmetric control.",
    "independent_oracle": "Sorted-list exact intersection.",
    "scope": "Correctness and access-count contract, excluding a later performance experiment."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Every intersection member must occur in the smallest set, so testing those elements against every other set is exact.",
    "uncertainty": "NONE"
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus exposes fast membership tests on the larger adjacency sets."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source chooses the smallest set as the exact intersection driver."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Triangle counting, clustering coefficient, and node-similarity kernels with strongly skewed adjacency-list sizes are plausible matches.",
      "uncertainty": "The crossover under disk latency and high-degree hubs is unmeasured."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "For a maximal asymmetric intersection, iterate the smallest set and retain an element only if lookup succeeds in every other set, then build the representation needed by the remaining expression.",
    "uncertainty": "NONE"
  },
  "name": "Probe Smallest Set First",
  "pattern_id": "PAT-PROBE-SMALLEST-SET-FIRST",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Approximate intersection setup can cost more than direct membership probes when one input is much smaller than the others.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The exact intersection representation is constructed after probing so it can replace the original subexpression.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-REFINE-HASHED-CANDIDATES-EXACTLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Membership dictionaries for all participating sets and the growing exact result are resident.",
    "uncertainty": "The paper does not isolate dictionary constants."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure random reads and cache misses while probing the larger sets.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Membership-probe locality and block traffic are not analyzed."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure persistent bytes for all membership indexes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No durable dictionary layout is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Membership dictionaries must exist, and the exact result receives a balanced bucketed set plus hash table after probing.",
      "measurement_needed": "Measure index-build and result-materialization time separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Construction constants are unspecified."
    },
    "ram": {
      "assumptions": [],
      "expression": "Membership dictionaries for participating sets plus the exact intersection and its downstream balanced representation.",
      "measurement_needed": "Measure resident dictionary bytes and peak result-construction bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Peak whole-process memory is not stated."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Instrument peak temporary allocation while materializing the intersection.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Temporary buffering during exact result construction is not bounded separately."
    }
  },
  "source_domain": "asymmetric set intersection",
  "source_paper_ids": [
    "PAPER-0708.3259"
  ],
  "source_pointers": [
    {
      "claim_scope": "Replace a maximal asymmetric intersection by probing every smallest-set element in all other sets.",
      "locator_type": "APPENDIX",
      "locator_value": "Appendix C, Improvement for asymmetric intersections",
      "page": 16,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-001"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The smallest set is the sole driver stream; larger sets are accessed through membership probes.",
    "uncertainty": "Device locality of probes is not modeled."
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
        "SP-001"
      ],
      "text": "The source does not determine the crossover on real cache hierarchies or disk-backed dictionaries.",
      "uncertainty": "No implementation benchmark is supplied."
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
      "text": "The smallest-set cardinality times the number of intersected sets is below the cost of approximate-intersection setup.",
      "uncertainty": "The source states the comparison symbolically rather than giving a universal threshold."
    }
  ]
}
```

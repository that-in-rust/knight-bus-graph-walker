# Balance Bucketed Packed Sets

- Pattern ID: `PAT-BALANCE-BUCKETED-PACKED-SETS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "A Knight Bus implementation adopts this exact representation for a named adjacency-set kernel."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explicitly balances directory and packed-payload space.",
      "Conversions are part of the access schedule."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "A007 can represent bucket-directory bytes and packed adjacency payload bytes as separate resident terms and quote conversion work when the balance point changes.",
    "uncertainty": "The source does not provide whole-process RSS or disk amplification for graph workloads."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Convert bucket parameters, process matching bucket pairs with packed merge, duplicate detection, masking, and compaction, then convert the output to its balanced parameter.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The mathematical word-RAM model matches the intended implementation sufficiently for correctness."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source supplies exact operation definitions and proofs."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "The representation and operations are supported by lemmas and appendix proofs, but practical performance and implementation constants were not independently checked.",
    "uncertainty": "No reproduced benchmark or code inspection occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A bucket directory points to sorted packed arrays of value suffixes; occupied fields carry suffix integers and test bits mark vacant fields.",
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
        "SP-001",
        "SP-002"
      ],
      "text": "The stated speedup diminishes when field widths are too large relative to the machine word or when directory overhead cannot be balanced against payload size.",
      "uncertainty": "The paper expresses this through operation bounds, not a measured breakpoint."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-BALANCE-BUCKETED-PACKED-SETS",
  "falsifying_test": {
    "controlled_variables": [
      "Word size, field width, bucket prefix length, input cardinalities, and value distribution."
    ],
    "failure_signal": "Any result mismatch, lost element during conversion, or directory/payload accounting inconsistent with the selected balance rule.",
    "fixture": "Two small integer sets distributed across empty, sparse, and dense prefix buckets, including duplicate merge positions and boundary suffix values.",
    "independent_oracle": "Standard library exact set union and intersection.",
    "scope": "Representation correctness and resource accounting only."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Bucket prefixes identify disjoint value ranges while each bucket stores only suffixes, so per-bucket packed union or intersection reconstructs the exact set operation.",
    "uncertainty": "NONE"
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus adjacency identifiers can be partitioned into stable integer-prefix buckets."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism performs exact packed set intersections."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Triangle counting, clustering coefficient, and node-similarity families that repeatedly intersect sorted adjacency identifiers are plausible matches.",
      "uncertainty": "Graph-degree skew and storage-tier effects are not evaluated by the source."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Choose the largest bucket-prefix length allowed by the set size and word size, convert both operands to that balance point, perform packed operations bucket by bucket, then rebalance the result.",
    "uncertainty": "NONE"
  },
  "name": "Balance Bucketed Packed Sets",
  "pattern_id": "PAT-BALANCE-BUCKETED-PACKED-SETS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "A naive small-universe set encoding can spend too much space on either bucket pointers or per-element fields and lose word-parallel speed.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Bucket boundaries and packed field widths may be converted and recomputed when operand sizes require a new balance point.",
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
      "SP-002"
    ],
    "text": "The bucket directory and packed suffix arrays for the operands and result remain resident during an operation.",
    "uncertainty": "NONE"
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure cold-cache bytes read and written for bucket conversion and set operations.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "This section analyzes word operations and does not specify block layout or page traffic."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Define a stable packed-bucket encoding and measure storage amplification against raw identifiers.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No durable serialization is defined."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Conversion between bucket parameters scans or repartitions buckets and compacts packed fields before operations at the selected balance point.",
      "measurement_needed": "Measure conversion time and bytes copied over skewed bucket populations.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The source gives asymptotic work rather than implementation constants."
    },
    "ram": {
      "assumptions": [],
      "expression": "Bucket-directory words plus packed suffix bits, balanced so directory and payload terms remain proportional to the represented set and suffix width.",
      "measurement_needed": "Measure resident bytes including directory, packed payload, allocator metadata, and output buffers.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Allocator and alignment overhead are absent from the model."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Instrument peak temporary bytes during conversion plus union and intersection.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Peak simultaneous input, converted, merged, and compacted buffers are not isolated."
    }
  },
  "source_domain": "word-parallel compact set representation",
  "source_paper_ids": [
    "PAPER-0708.3259"
  ],
  "source_pointers": [
    {
      "claim_scope": "Packed fields and word-parallel union/intersection operations.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1 and Lemma 3",
      "page": 10,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Bucketed representation, balance rule, conversion, space, and operation bounds.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.2 and Lemmas 4-5",
      "page": 11,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Repartitioning and per-bucket packed operations used to construct balanced results.",
      "locator_type": "APPENDIX",
      "locator_value": "Appendix B, proofs of Lemmas 4-5",
      "page": 16,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Packed words are traversed sequentially within corresponding buckets during merge and compaction.",
    "uncertainty": "The paper does not specify a device-level stream layout."
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
        "SP-002"
      ],
      "text": "Cache behavior, SIMD implementation details, and persistent block layout are outside the analytical representation model.",
      "uncertainty": "No system evaluation is supplied."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Elements are small integers, packed field width satisfies the stated word-capacity condition, and union or intersection can be performed bucket-wise.",
      "uncertainty": "NONE"
    }
  ]
}
```

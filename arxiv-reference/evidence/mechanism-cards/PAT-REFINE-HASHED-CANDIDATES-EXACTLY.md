# Refine Hashed Candidates Exactly

- Pattern ID: `PAT-REFINE-HASHED-CANDIDATES-EXACTLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "A Knight Bus workload contains reusable monotone set operations.",
      "Hash-image construction is admitted as preprocessing."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper preserves exact answers while shrinking the candidate instance in expectation.",
      "Selected hash resolutions use linear space."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "A007 could account separately for compact approximate resident state and exact candidate-verification state instead of pricing every input occurrence as hot.",
    "uncertainty": "The source does not establish a whole-graph RSS or I/O bound for Knight Bus."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Process the approximate expression bottom-up, propagate the root approximation top-down to leaves, retrieve candidate originals, and finish with exact hashing-based evaluation.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The extracted PDF accurately represents the final source version."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper supplies an explicit three-stage algorithm and appendix analysis."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Confidence is high in the analytical preservation mechanism and moderate in practical resource benefit because this campaign did not inspect code or reproduce results.",
    "uncertainty": "No independent implementation evidence is available in G05."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Each set has selected compact bucketed hash-image resolutions and one chained hash table mapping hash prefixes back to original elements.",
    "uncertainty": "Hash collisions remain possible and are removed only by exact verification."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "The preservation argument does not extend directly to non-monotone operators such as set difference because intermediate approximation would require handling two-sided errors.",
      "uncertainty": "The limitation is stated as an open problem rather than an impossibility result."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REFINE-HASHED-CANDIDATES-EXACTLY",
  "falsifying_test": {
    "controlled_variables": [
      "Expression tree, input sets, hash seed, resolution choices, and word size."
    ],
    "failure_signal": "Any false negative, any final result differing from the oracle, or candidate state not reduced on a collision-controlled selective fixture.",
    "fixture": "Small adversarial families of overlapping identifier sets with forced hash collisions and nested union-intersection expressions.",
    "independent_oracle": "Direct exact evaluation over uncompressed sets.",
    "scope": "Correctness and candidate-refinement behavior only; not a G09 performance experiment."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Every true output element survives hashing into the approximate expression and remains in each reduced input, so exact evaluation of the reduced inputs returns the original result.",
    "uncertainty": "The running-time bound is expected because false-positive collisions are randomized."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus represents relevant adjacency neighborhoods as exact sets of machine-word identifiers."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism accelerates exact set intersections after candidate filtering."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Neighbor-set intersection used by triangle counting and node-similarity candidates is the closest Knight Bus family match.",
      "uncertainty": "Benefit for skewed graph neighborhoods and disk-resident layouts is unmeasured."
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
    "text": "Evaluate the monotone expression over compact hash images, use that result to filter each original input into a candidate subset, then evaluate the same expression exactly on those subsets.",
    "uncertainty": "NONE"
  },
  "name": "Refine Hashed Candidates Exactly",
  "pattern_id": "PAT-REFINE-HASHED-CANDIDATES-EXACTLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Exact union-intersection evaluation can inspect all input elements even when few elements can reach the final output.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The expression is recomputed exactly on the reduced candidate inputs after approximate evaluation.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-BALANCE-BUCKETED-PACKED-SETS",
    "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS",
    "PAT-PROBE-SMALLEST-SET-FIRST"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Selected compact hash-image representations, intermediate approximate sets, and one lookup table per input set remain resident.",
    "uncertainty": "NONE"
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "The RAM bounds translate to the I/O model by treating the bits in a disk block as the word-parallel unit.",
      "measurement_needed": "Measure bytes and page reads under a declared on-disk layout and cold cache.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "No concrete page layout, cache state, or device benchmark is supplied."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Serialize the selected resolutions and lookup table, then measure bytes on disk.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper specifies a RAM data structure, not durable encoding bytes."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Expected preprocessing builds selected bucketed hash images and one hash table; the paper bounds this by linear space and expected set-size times log word-size work.",
      "measurement_needed": "Measure build time and allocation volume by input cardinality and word size.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Hash construction constants are not reported."
    },
    "ram": {
      "assumptions": [],
      "expression": "Linear words in total set size for selected hash resolutions and lookup tables, plus intermediate candidate state.",
      "measurement_needed": "Measure peak RSS for preprocessing plus one complete expression evaluation.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Constant factors and whole-process RSS are not reported."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Instrument peak temporary allocations during a complete expression-tree evaluation.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Peak temporary bytes for all intermediate approximate and exact candidate sets are not isolated."
    }
  },
  "source_domain": "word-RAM set-expression evaluation",
  "source_paper_ids": [
    "PAPER-0708.3259"
  ],
  "source_pointers": [
    {
      "claim_scope": "Compact universal-hash images produce candidate references plus false positives, followed by exact hashing.",
      "locator_type": "SECTION",
      "locator_value": "Section 1.3, Technical overview",
      "page": 6,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Three-stage approximate-expression, candidate extraction, and exact recomputation procedure.",
      "locator_type": "SECTION",
      "locator_value": "Sections 2.1-2.2",
      "page": 7,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Expected false-candidate bound and linear-space preprocessing analysis.",
      "locator_type": "APPENDIX",
      "locator_value": "Appendix A, Running time and preprocessing",
      "page": 15,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Original elements are visited when hash-image membership identifies them as candidates for exact evaluation.",
    "uncertainty": "The paper analyzes RAM access and gives an I/O-model translation rather than a concrete storage implementation."
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
        "SP-001",
        "SP-003"
      ],
      "text": "Concrete storage-device behavior and constants remain unmeasured because the paper provides analytical RAM and I/O-model bounds.",
      "uncertainty": "No implementation benchmark is reported."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "The expression is monotone and contains only union and intersection over preprocessed machine-word sets.",
      "uncertainty": "NONE"
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Compact hash images fit word-parallel operations and exact verification removes every false positive.",
      "uncertainty": "Expected cost depends on universal-hash behavior."
    }
  ]
}
```

# Skip Finalized Vector Chunks

- Pattern ID: `PAT-SKIP-FINALIZED-VECTOR-CHUNKS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can verify a monotone finality predicate for the selected algorithm.",
      "The estimator does not count speculative skipped work as guaranteed capacity reduction."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source skip decision depends on current finality vectors.",
      "The source reports no early-iteration savings and workload-dependent later savings."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Knight Bus may model this as a runtime work-elision mechanism, but admission should reserve the full traversal vectors and underlying matrix because the number and timing of skippable chunks are unknown before frontier evolution is observed.",
    "uncertainty": "The source does not predict finalized-chunk counts from graph metadata alone."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "For every chunk in every BFS iteration, evaluate C finality predicates first; either carry the previous chunk forward or execute its full sparse-matrix vector kernel.",
    "uncertainty": "The branch and check overhead is paid even when no chunk can be skipped."
  },
  "confidence_rationale": {
    "assumptions": [
      "The cited paper and pointers accurately represent the evaluated mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited source pointers describe or evaluate the mechanism.",
      "G05 did not independently reproduce the source result or inspect implementation code."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "The source provides semiring-specific pseudocode and per-iteration benchmarks across graph families, but this campaign did not reproduce the results.",
    "uncertainty": "Safety outside the stated BFS formulations is unproven."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Rows are grouped into fixed-height chunks, and the corresponding frontier/filter/parent entries are contiguous enough to test one chunk together.",
    "uncertainty": "Sorting scope affects which row degrees and discovery times share a chunk."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "Early iterations in which no chunks are final pay the check overhead without skipping work, and high-diameter low-degree graphs can show little improvement.",
      "uncertainty": "The source describes small overhead rather than a universal slowdown bound."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SKIP-FINALIZED-VECTOR-CHUNKS",
  "falsifying_test": {
    "controlled_variables": [
      "chunk height C",
      "row ordering",
      "semiring",
      "BFS root",
      "thread schedule"
    ],
    "failure_signal": "A skipped chunk later changes, output differs from the no-skip oracle, or the finalized chunk still executes its edge kernel",
    "fixture": "A two-chunk BFS graph where one chunk becomes final after the first iteration and the other remains active",
    "independent_oracle": "The identical BFS-SpMV kernel with chunk skipping disabled",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A chunk is skipped only when every one of its C rows satisfies the selected semiring's final-output predicate; skipped outputs are carried forward unchanged.",
    "uncertainty": "Correctness depends on a label-setting or equivalent monotone finality property."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "MONOTONE_LABEL_SETTING_TRAVERSAL"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Before multiplying a matrix chunk, inspect its frontier, filter, or parent entries; if all are final, copy the prior output chunk and bypass all edge operations for that chunk.",
    "uncertainty": "The tested predicates cover the tropical, boolean, real, and sel-max BFS formulations."
  },
  "name": "Skip Finalized Vector Chunks",
  "pattern_id": "PAT-SKIP-FINALIZED-VECTOR-CHUNKS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Dense-vector BFS-SpMV revisits every matrix chunk in each iteration even after all output entries associated with a chunk have become final.",
    "uncertainty": "The finality condition differs by BFS semiring."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The all-final predicate is recomputed for each chunk in each iteration from current traversal vectors.",
    "uncertainty": "The paper does not persist a separate finalized-chunk index."
  },
  "related_pattern_ids": [
    "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The mechanism uses the BFS output and semiring-specific frontier, filter, or parent vectors that are already resident for the algebraic traversal.",
    "uncertainty": "The paper does not isolate incremental whole-process RAM for SlimWork."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure matrix bytes avoided per skipped chunk on the target memory or storage hierarchy.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "External-storage I/O is outside the paper's evaluated scope."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Inspect the serialized representation for any additional finality metadata.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No separate persistent artifact for SlimWork is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Confirm whether the implementation creates any auxiliary skip metadata before execution.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not identify dedicated preprocessing beyond the underlying chunk layout."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RSS with and without SlimWork under an identical representation and semiring.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not isolate incremental whole-process RAM for the skip mechanism."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Per-chunk temporary state is the C-entry finality test and carry-forward vector state; the source states O(C) decision work per chunk.",
      "measurement_needed": "Inspect generated code or measure temporary spills for the target vector width.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Compiler register and spill storage are not quantified."
    }
  },
  "source_domain": "SIMD-oriented algebraic breadth-first search",
  "source_paper_ids": [
    "PAPER-2010.09913"
  ],
  "source_pointers": [
    {
      "claim_scope": "Finality test and skipped-chunk mechanism",
      "locator_type": "SECTION",
      "locator_value": "Section III-C, Reducing Work Amount with SlimWork",
      "page": 7,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Semiring-specific skip predicates and carry-forward schedule",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 7, SlimWork",
      "page": 7,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Per-iteration benchmark effect and early-iteration overhead",
      "locator_type": "FIGURE",
      "locator_value": "Figure 5(d) and Section IV-A4",
      "page": 9,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Graph-family, preprocessing, architecture, and storage context",
      "locator_type": "FIGURE",
      "locator_value": "Figures 7-8 and Sections IV-C-IV-E",
      "page": 10,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Density and architecture applicability boundaries",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section IV-F and Section V",
      "page": 11,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-005"
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
    "text": "When a chunk is not final, its matrix entries and referenced frontier values are streamed through the kernel; when final, those matrix entries are not read by the chunk computation.",
    "uncertainty": "Cache and prefetch behavior may still move data not explicitly consumed."
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
        "SP-005"
      ],
      "text": "The paper does not establish safe finality predicates for non-BFS algorithms or algorithms whose vertex values may later decrease after appearing stable.",
      "uncertainty": "Extensions to other graph algorithms are proposed but not demonstrated."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "The mechanism is effective in later iterations when many chunks contain only finalized vertices and the avoided edge work exceeds the C-entry test cost.",
      "uncertainty": "Benefit depends on frontier evolution, row grouping, and degree distribution."
    }
  ]
}
```

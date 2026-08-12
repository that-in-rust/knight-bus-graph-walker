# Recycle Survivors During Aging

- Pattern ID: `PAT-RECYCLE-SURVIVORS-DURING-AGING`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus supports an insertion stream with bulk expiration.",
      "Survivor and unique-arrival fractions can be conservatively estimated.",
      "Its runtime can refuse or trigger aging before capacity is exhausted."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source proves sufficient open-space and bandwidth conditions.",
      "The source defines an observable capacity fail condition.",
      "The source continues new-edge ingestion while queries pause."
    ],
    "source_pointer_ids": [
      "SP-046",
      "SP-047",
      "SP-048"
    ],
    "text": "Knight Bus could make bulk aging an admissible state transition only when the pre-run contract reserves survivor-rebuild headroom, bandwidth expansion, and declared query downtime, with explicit refusal before the jeopardy condition.",
    "uncertainty": "The theoretical slot model does not bound implementation RSS, durable state, or prediction error."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-044",
      "SP-045"
    ],
    "text": "The aging token moves through processors for predicate testing, then the loader moves downstream as each processor drains survivors into payload slots that circulate to the head.",
    "uncertainty": "Scheduling assumes the internal expansion bandwidth required by the theorem."
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
      "SP-046",
      "SP-047",
      "SP-049",
      "SP-050"
    ],
    "text": "The source proves aging correctness and sufficient capacity conditions and reports single and repeated aging runs, but this campaign did not reproduce the prototype or inspect code.",
    "uncertainty": "Grade C is bounded to the prototype, tested predicates, and theoretical model assumptions."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-044",
      "SP-045"
    ],
    "text": "Each processor classifies stored edges as untested, unresolved, tree, or non-tree; one loader emits unresolved survivors while a builder packs the new connectivity prefix.",
    "uncertainty": "Some survivor edges may temporarily exist twice until duplicate detection removes the recycled copy."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-046",
        "SP-047"
      ],
      "text": "If aging begins too late, retains too many edges, or lacks payload bandwidth, a jeopardy edge can leave the full tail and the system raises its defined fail condition.",
      "uncertainty": "This is an explicit capacity failure, not a silent degradation."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-RECYCLE-SURVIVORS-DURING-AGING",
  "falsifying_test": {
    "controlled_variables": [
      "edge-slot capacity",
      "bundle expansion",
      "survivor fraction",
      "unique-arrival rate",
      "query downtime",
      "processor count"
    ],
    "failure_signal": "An arrival is dropped, a failed-predicate edge survives, connectivity differs after aging, or a run admitted under the declared bound reaches the jeopardy fail condition",
    "fixture": "A two-processor stream that starts aging near capacity, retains a controlled survivor fraction, receives new unique edges during aging, and includes a duplicate survivor",
    "independent_oracle": "Static connected components on predicate survivors union all arrivals during the aging interval",
    "scope": "Smallest aging-correctness and capacity falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-045",
      "SP-046"
    ],
    "text": "During aging, resolved new-state edges remain upstream of unresolved survivors, the builder never advances past the loader, and every surviving edge is eventually incorporated into the rebuilt connectivity structure.",
    "uncertainty": "Correctness requires a compliant implementation of all stated invariants and Property 1."
  },
  "knight_bus_algorithm_families": [
    "STREAMING_CONNECTED_COMPONENTS",
    "WCC",
    "DYNAMIC_CONNECTIVITY",
    "INFINITE_GRAPH_STREAMS"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-044",
      "SP-045",
      "SP-046"
    ],
    "text": "Apply a constant-time predicate to stored edges, delete failures, mark survivors unresolved, circulate survivors in payload slots back to the head for normal reclassification, and continue processing new primary-slot arrivals against the new structure.",
    "uncertainty": "The model supports bulk predicates rather than arbitrary individual deletions."
  },
  "name": "Recycle Survivors During Aging",
  "pattern_id": "PAT-RECYCLE-SURVIVORS-DURING-AGING",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-044",
      "SP-048"
    ],
    "text": "An insertion-dominated stream eventually fills finite storage, but the application must continue accepting arrivals while applying customized bulk deletion and rebuilding connectivity state.",
    "uncertainty": "Queries are suspended during the rebuild."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-046"
    ],
    "text": "Connectivity state is rebuilt from predicate survivors and all edges arriving during the aging interval; their interleaving does not affect final connected components.",
    "uncertainty": "This permutation independence is specific to connected components."
  },
  "related_pattern_ids": [
    "PAT-PACK-CONNECTIVITY-STATE-PREFIX"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-044",
      "SP-045",
      "SP-046"
    ],
    "text": "The ring retains surviving old edges, newly arriving edges, partial rebuilt union-find state, classifications, and loader and builder positions during aging.",
    "uncertainty": "Peak implementation bytes are abstracted as edge slots."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Internal bandwidth expansion k must carry one primary arrival plus payload survivors; Theorem 4 relates k to survivor fraction, unique-arrival rate, query availability, processor count, and capacity.",
      "measurement_needed": "Measure payload occupancy, bytes circulated per survivor, and achieved bandwidth expansion.",
      "premises": [],
      "source_pointer_ids": [
        "SP-047"
      ],
      "status": "SOURCED",
      "uncertainty": "The model is internal message traffic, not disk I/O, and depends on estimated uniqueness."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure any durable log, checkpoint, and replay bytes required by an implementation.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The aging protocol is memory-based and does not specify durable survivor logs or recovery."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure trigger estimation, reservoir maintenance, and predicate setup time.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Predicate-selection and trigger-calibration costs are not reported as a preparation term."
    },
    "ram": {
      "assumptions": [],
      "expression": "Successful aging requires reserved empty edge slots before initiation; temporary duplicate survivors can coexist with the rebuilt copy until deduplication, and Lemma 3 bounds required open capacity in the paper's edge-slot model.",
      "measurement_needed": "Measure peak unique and duplicate edge slots plus whole-process RSS throughout aging.",
      "premises": [],
      "source_pointer_ids": [
        "SP-046",
        "SP-047"
      ],
      "status": "SOURCED",
      "uncertainty": "The slot formula omits implementation byte coefficients and runtime buffers."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Payload slots carry unresolved survivors, and some survivors may temporarily be stored twice until the recycled duplicate is recognized.",
      "measurement_needed": "Measure maximum duplicated-edge and in-flight payload bytes per aging event.",
      "premises": [],
      "source_pointer_ids": [
        "SP-045",
        "SP-046"
      ],
      "status": "SOURCED",
      "uncertainty": "The maximum duplicate coefficient in concrete bytes is not measured."
    }
  },
  "source_domain": "finite-space processing of unending graph streams",
  "source_paper_ids": [
    "PAPER-2112.00098"
  ],
  "source_pointers": [
    {
      "claim_scope": "Bulk-aging protocol, uninterrupted arrivals, loader, and payload recycling",
      "locator_type": "SECTION",
      "locator_value": "Section 5 and Section 5.1",
      "page": 10,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-044"
    },
    {
      "claim_scope": "Concurrent rebuild and loader-builder ordering",
      "locator_type": "FIGURE",
      "locator_value": "Figure 6 and Invariant 4",
      "page": 11,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-045"
    },
    {
      "claim_scope": "Jeopardy failure, survivor incorporation, and post-aging correctness",
      "locator_type": "THEOREM",
      "locator_value": "Figure 8, Property 1, and Theorem 3",
      "page": 12,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-046"
    },
    {
      "claim_scope": "Reserved-space and bandwidth conditions for successful aging",
      "locator_type": "THEOREM",
      "locator_value": "Lemma 3 and Theorem 4",
      "page": 13,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-047"
    },
    {
      "claim_scope": "Conditions for indefinite operation through repeated aging",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 5",
      "page": 14,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-048"
    },
    {
      "claim_scope": "Single-event validation and automated repeated-aging setup",
      "locator_type": "SECTION",
      "locator_value": "Sections 9.4-9.5 and Figures 10-12",
      "page": 20,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-049"
    },
    {
      "claim_scope": "Repeated aging to target survivor fraction",
      "locator_type": "FIGURE",
      "locator_value": "Figure 11 and preceding reservoir-sampling protocol",
      "page": 21,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "SP-050"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-044",
      "SP-045"
    ],
    "text": "Surviving unresolved edges circulate in payload slots while new arrivals continue in primary slots.",
    "uncertainty": "Queries do not share this schedule because they are disabled during aging."
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
        "SP-049",
        "SP-050"
      ],
      "text": "The source's repeated-aging prototype uses a timestamp predicate and does not establish behavior for expensive predicates, durable recovery, or zero query downtime.",
      "uncertainty": "General user predicates are allowed by the model but not broadly benchmarked."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-047",
        "SP-048"
      ],
      "text": "Aging starts with the required free space, the predicate removes enough edges, internal bandwidth satisfies Theorem 4, and query downtime is allowed.",
      "uncertainty": "The uniqueness and survivor fractions must be estimated for the stream."
    }
  ]
}
```

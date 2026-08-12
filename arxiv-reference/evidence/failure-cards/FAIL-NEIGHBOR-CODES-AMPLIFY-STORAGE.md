# Neighbor Codes Amplify Storage

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Equivalent codes yield equivalent approximate scores."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "logical graph",
      "code width",
      "record alignment",
      "queries",
      "recall target"
    ],
    "expected_observation": "Colocation reduces resident guidance bytes but exceeds the admitted persistent or read budget",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "high degree code replication",
    "graph_scale": "Small graph large enough to expose repeated code copies",
    "graph_shape": "Proximity graph with a concentrated high-degree region and repeated neighbor-code references",
    "independent_oracle": "Deterministic candidate scores and byte counters from the global-code layout",
    "premises": [
      "The source layout duplicates neighbor codes beside records."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source provides no implementation-level record packing.",
    "varied_variables": [
      "degree concentration",
      "cache state"
    ],
    "workload": "Run matched-recall searches with colocated codes and with one resident global code table"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-COLOCATE-NEIGHBOR-CODES-ONDISK"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The compared layouts encode equivalent graph and code values."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "replicated_code_bytes + added_read_bytes > admitted_storage_io_budget",
    "measurement_needed": "Measure persistent index bytes and matched-recall bytes read separately.",
    "numeric_constants": [],
    "premises": [
      "Neighbor codes are replicated in records.",
      "Storage-resident guidance adds reads relative to a resident guidance table."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Persistent expansion and per-query traffic are different resources combined only for rejection logic.",
    "variables": [
      {
        "definition": "persistent bytes occupied by copied neighbor codes",
        "symbol": "replicated_code_bytes",
        "units": "bytes"
      },
      {
        "definition": "query bytes attributable to storage-resident guidance",
        "symbol": "added_read_bytes",
        "units": "bytes"
      },
      {
        "definition": "joint admitted storage and query-I/O budget",
        "symbol": "admitted_storage_io_budget",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Record packing preserves the stated all-storage organization."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The layout copies neighbor codes into node records.",
      "The source warns of storage expansion and read amplification."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Moving guidance codes out of RAM is beneficial only when the added replication and storage reads remain inside the admitted storage and I/O budgets.",
    "uncertainty": "The replication factor depends on degree and code width."
  },
  "confidence_rationale": {
    "assumptions": [
      "The survey description faithfully represents the referenced design."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "All cited pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The source states both the intended memory benefit and the opposing storage/read costs, but supplies no evaluated crossover.",
    "uncertainty": "No source code or cited-system paper was added or inspected."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Counters include padding, cache effects, and kernel I/O."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source identifies expansion and amplification."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "text": "Persistent bytes or matched-recall read bytes cross their declared budgets despite the RAM reduction.",
    "uncertainty": "The favorable budget depends on deployment priorities."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-NEIGHBOR-CODES-AMPLIFY-STORAGE",
  "name": "Neighbor Codes Amplify Storage",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-003"
    ],
    "text": "On-disk footprint and storage traffic increase enough that the source excludes the layout from combination evaluation.",
    "uncertainty": "The crossover is not quantified."
  },
  "repair_options": [
    {
      "description": "Use this representation only for genuinely tight-RAM or rapid-index-switching workloads.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Deduplicate or dictionary-encode repeated neighbor codes.",
      "repair_class": "CHANGE_REPRESENTATION"
    },
    {
      "description": "Reject layouts whose measured degree-weighted replication exceeds budget.",
      "repair_class": "ADD_ADMISSION_GUARD"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "All-storage placement and read-amplification risk",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2, Representative disk-based systems",
      "page": 3,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Replicated neighbor-code record layout and evaluation exclusion",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2.2, All-in-Storage Layout",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Exclusion due to storage expansion and read amplification",
      "locator_type": "SECTION",
      "locator_value": "Section 7.1.1, Combination Design",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The all-storage layout is aimed at tight-memory, rapid-switching deployments and duplicates neighbor guidance beside node records.",
    "uncertainty": "The paper excludes this design from its own evaluation."
  }
}
```

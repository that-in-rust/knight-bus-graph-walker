# Serial Dependencies Eliminate Overlap

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The path prevents alternate ready candidates."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "record layout",
      "device state",
      "distance kernel",
      "query"
    ],
    "expected_observation": "The path fixture shows no read-compute overlap and no latency reduction attributable to pipelining.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "single-chain-async-traversal",
    "graph_scale": "Use the minimum path length that produces repeated storage reads.",
    "graph_shape": "A path-like search graph where each fetched record reveals the only next candidate.",
    "independent_oracle": "A trace simulator computes the critical path from measured read and compute durations.",
    "premises": [
      "The source mechanism requires available compute while reads are outstanding."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Real ANN search may retain more parallel candidates.",
    "varied_variables": [
      "frontier width",
      "dependency depth"
    ],
    "workload": "Run synchronous and asynchronous readers with identical direct-I/O and cache conditions."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PIPELINE-ASYNCHRONOUS-DISK-READS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Compute and I/O durations are measured on the same execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "useful_compute_window <= read_overlap_requirement",
    "measurement_needed": "Trace read issue/completion and compute-ready intervals on dependency-chain and wide-frontier controls.",
    "numeric_constants": [],
    "premises": [
      "The mechanism gains only from temporal overlap."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No source threshold separates useful from negligible overlap.",
    "variables": [
      {
        "definition": "Duration of independent candidate computation available during an outstanding read.",
        "symbol": "useful_compute_window",
        "units": "time"
      },
      {
        "definition": "Outstanding-read interval that must be covered to hide storage latency.",
        "symbol": "read_overlap_requirement",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Useful distance work on available candidates can run while at least one needed disk read remains outstanding.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The fixture enforces a serial dependency chain."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Pipelining is temporal overlap, not a different distance result."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The counterexample follows directly from the overlap prerequisite; the source leaves overlap-only gains unmeasured.",
    "uncertainty": "Hardware async submission may have secondary benefits not covered by the mechanism."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Synchronous and asynchronous controls use the same storage path."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The fixture removes independent work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The asynchronous trace has no positive overlap interval and end-to-end latency does not improve after accounting for submission overhead.",
    "uncertainty": "Device queue behavior can add unrelated differences."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-SERIAL-DEPENDENCIES-ELIMINATE-OVERLAP",
  "name": "Serial Dependencies Eliminate Overlap",
  "observable_symptom": {
    "assumptions": [
      "Submission overhead is not negative."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "No useful compute exists during read latency."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "I/O and compute serialize, so asynchronous descriptors and buffers add state without hiding latency.",
    "uncertainty": "An asynchronous engine may still reduce software overhead independently."
  },
  "repair_options": [
    {
      "description": "Enable pipelining only when measured frontier width and compute-ready time provide an overlap window.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Batch independent candidates or replenish reads on completion to create useful concurrency when semantics permit.",
      "repair_class": "CHANGE_SCHEDULE"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Pipeline overlaps asynchronous disk reads with compute but can waste I/O.",
      "locator_type": "TABLE",
      "locator_value": "Table 1, Pipeline row",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "The overlap mechanism computes while other candidate records are being fetched.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.2 and Figure 9(a)",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The frontier has only one dependency chain or all ready work is exhausted."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Overlap requires compute-ready data independent of outstanding reads."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Every next computation depends on the record returned by the current read, leaving no independent candidate work.",
    "uncertainty": "The source does not isolate overlap-only behavior for this shape."
  }
}
```

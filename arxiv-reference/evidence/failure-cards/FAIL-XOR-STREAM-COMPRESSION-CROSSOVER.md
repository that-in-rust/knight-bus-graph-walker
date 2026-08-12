# Xor Stream Compression Crossover

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Relabeling preserves graph semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph topology",
      "Edge weights",
      "Machine",
      "Codec implementation"
    ],
    "expected_observation": "Outputs match while at least one permutation fails to amortize preprocessing or reduce bytes.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Identifier locality crossover",
    "graph_scale": "Symbolic fixed vertices and edges with scan count varied.",
    "graph_shape": "One graph emitted under locality-preserving and locality-destroying vertex permutations.",
    "independent_oracle": "Byte-exact decoded edge multiset and an uncompressed sorted stream scan.",
    "premises": [
      "The source links sorted adjacent XORs to compression and reports preprocessing."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The minimal adverse permutation is unknown.",
    "varied_variables": [
      "Vertex permutation",
      "Iteration count"
    ],
    "workload": "Build both streams and execute exact repeated edge scans."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-COMPRESS-SORTED-ID-STREAMS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The comparison includes the same orientations, weights, and temporary lifecycle."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "T_preprocess + iterations * T_compressed_scan >= iterations * T_plain_scan OR bytes_compressed >= bytes_plain",
    "measurement_needed": "Measure build time, scan time, final bytes, and peak temporary bytes over ID permutations and iteration counts.",
    "numeric_constants": [],
    "premises": [
      "The source introduces preprocessing and decoding in exchange for smaller streams."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source does not report the crossover for adversarial ID order.",
    "variables": [
      {
        "definition": "sort, transform, encode, and merge time",
        "symbol": "T_preprocess",
        "units": "time"
      },
      {
        "definition": "number of post-build scans",
        "symbol": "iterations",
        "units": "scans"
      },
      {
        "definition": "time for one compressed decode and scan",
        "symbol": "T_compressed_scan",
        "units": "time per scan"
      },
      {
        "definition": "time for one plain scan",
        "symbol": "T_plain_scan",
        "units": "time per scan"
      },
      {
        "definition": "complete compressed representation bytes",
        "symbol": "bytes_compressed",
        "units": "bytes"
      },
      {
        "definition": "complete plain representation bytes",
        "symbol": "bytes_plain",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The representation is evaluated over repeated scans."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source requires sorted XOR streams and reports separate preprocessing."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The end-to-end benefit assumes adjacent sorted identifiers compress enough to repay sorting, encoding, decoding, and temporary merge work.",
    "uncertainty": "The source does not isolate an amortization threshold."
  },
  "confidence_rationale": {
    "assumptions": [
      "End-to-end accounting is the relevant admission objective."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports the mechanism, preprocessing, and compressed files."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "This is an analytical lifecycle counterexample grounded in the sourced mechanism and measurements, not a source-reported adversarial result.",
    "uncertainty": "No local benchmark has fixed the crossover."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle counts the same graph content."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Compression introduces measurable build and decode terms."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The compressed lifecycle fails its byte or amortized-time guard while decoded edges remain exact.",
    "uncertainty": "The crossover must be measured."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER",
  "name": "Xor Stream Compression Crossover",
  "observable_symptom": {
    "assumptions": [
      "All lifecycle bytes are counted consistently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports both build time and final file sizes."
    ],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "Compressed execution uses at least the time or bytes of the plain stream after accounting for preprocessing and temporary files.",
    "uncertainty": "Temporary peak storage is not completely reported."
  },
  "repair_options": [
    {
      "description": "Admit XOR compression only after sampling adjacent-delta compressibility and expected reuse.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Retain a plain stream when the sampled or measured crossover fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure peak temporary storage and amortization over expected scan counts.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "Sorted source/destination streams, adjacent XOR, and Simple-8b encoding.",
      "locator_type": "SECTION",
      "locator_value": "Sections 3.2-3.3",
      "page": 9,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "In-place sorting, orientation merge, and temporary block lifecycle.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.3 continuation",
      "page": 11,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Reported preprocessing time and compressed sizes.",
      "locator_type": "TABLE",
      "locator_value": "Tables 6-7 and Sections 4.4-4.5",
      "page": 17,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Identifier order can be varied without changing graph semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Adjacent XOR magnitude controls packed stream efficiency."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "text": "Poor identifier locality or too few post-build scans leaves compression and preprocessing unamortized.",
    "uncertainty": "The source does not report adversarial permutations."
  }
}
```

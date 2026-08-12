# Probe Quantized Topology Compatibility

- Pattern ID: `PAT-PROBE-QUANTIZED-TOPOLOGY-COMPATIBILITY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The sample is representative enough for admission."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source documents both competitive and collapse distributions.",
      "The probe precedes graph construction."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A007 can reserve a bounded pre-build compatibility probe and refuse the compact topology when the signal is weak, avoiding an unsupported promise that every vector distribution fits the low-RAM plan.",
    "uncertainty": "The overlap threshold must be calibrated and reported as empirical."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Sample vectors, compute both pairwise ranking views, compare top-candidate overlap, and gate the expensive topology build.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported overlap heuristic is implemented as described."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Cross-dataset failures motivate an explicit pre-build gate."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The paper proposes the probe after broad positive and negative dataset evaluation, but does not independently validate the heuristic on a held-out deployment campaign.",
    "uncertainty": "External calibration and reproduction are absent."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "A sample retains original float vectors and derived BQ signatures; no graph index is required for the probe.",
    "uncertainty": "Sampling method under distribution drift is not specified."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "A nonrepresentative sample or shifted multimodal mixture can make the overlap signal mischaracterize deployment geometry.",
      "uncertainty": "The source does not benchmark drift-aware sampling."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PROBE-QUANTIZED-TOPOLOGY-COMPATIBILITY",
  "falsifying_test": {
    "controlled_variables": [
      "Sample method, overlap cutoff, graph parameters, query set, and rerank budget."
    ],
    "failure_signal": "The probe admits a collapse fixture or rejects a compatible fixture under the declared cutoff.",
    "fixture": "A sampled corpus with known compatible contrastive embeddings and known incompatible Euclidean or random controls.",
    "independent_oracle": "Observed recall of fully built BQ and float graph indexes against brute-force neighbors.",
    "scope": "Predictive admission behavior only."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The probe compares quantized and exact rankings on the same sampled vectors before any topology is admitted.",
    "uncertainty": "The probe is a heuristic signal, not a proof of graph recall."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus exposes the same vector corpus and exact similarity oracle."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source probe predicts suitability of BQ-native ANN topology."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "This admission probe applies directly to Knight Bus node-similarity or kNN indexing and not to non-vector graph families without an analogous approximate-versus-exact ranking view.",
      "uncertainty": "No source evidence supports other families."
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
    "text": "On a sample, compute brute-force top candidates under both BQ and float distances, measure overlap, and choose BQ-native or float-based topology according to the compatibility signal.",
    "uncertainty": "The reported threshold is empirical and source-specific."
  },
  "name": "Probe Quantized Topology Compatibility",
  "pattern_id": "PAT-PROBE-QUANTIZED-TOPOLOGY-COMPATIBILITY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Quantized topology can work well or collapse depending on embedding geometry, and discovering incompatibility after full graph construction wastes build resources.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Exact and BQ top-candidate rankings are computed from the same sample and reduced to an overlap statistic.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Only sampled float vectors, their BQ signatures, and ranking buffers need be resident for the probe.",
    "uncertainty": "The source recommends a sample scale but does not provide a symbolic RAM bound."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure corpus pages read and cache effects during sampling.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not report bytes read to obtain the sample."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Decide whether to persist signatures or only the decision receipt, then measure bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The probe does not require a persistent artifact in the source."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Compute BQ signatures and brute-force BQ and float top-candidate rankings on the sample before graph construction.",
      "measurement_needed": "Measure probe wall time and distance-evaluation count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source describes the probe as lightweight but the exact cost depends on sample and dimension."
    },
    "ram": {
      "assumptions": [
        "The implementation materializes both representations and bounded top-candidate buffers."
      ],
      "expression": "Sampled float vectors plus their BQ signatures and two top-candidate buffers.",
      "measurement_needed": "Measure peak probe RSS by sample size and dimension.",
      "premises": [
        "The source probe uses brute-force BQ and float rankings over a sample."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Exact sample implementation and allocator overhead are unspecified."
    },
    "temporary_storage": {
      "assumptions": [
        "Rankings are computed in bounded top-candidate buffers rather than fully materialized matrices."
      ],
      "expression": "Temporary ranking buffers scale with the sample and retained top-candidate count.",
      "measurement_needed": "Measure peak scratch for exact and quantized ranking.",
      "premises": [
        "Two ranking views are compared."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "The source does not specify the implementation strategy."
    }
  },
  "source_domain": "pre-build ANN index admission testing",
  "source_paper_ids": [
    "PAPER-2605.02171"
  ],
  "source_pointers": [
    {
      "claim_scope": "Four-tier cross-dataset compatibility gradient and collapse controls",
      "locator_type": "SECTION",
      "locator_value": "Section 5.6",
      "page": 8,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Sampled BQ-versus-float top-candidate overlap as a pre-build go/no-go heuristic.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 6, Practical compatibility test",
      "page": 11,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Deployment recommendation to probe before enabling BQ-native indexing.",
      "locator_type": "SECTION",
      "locator_value": "Section 8, Scope and deployment",
      "page": 12,
      "paper_id": "PAPER-2605.02171",
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
    "text": "The sampled original vectors can be read from the corpus without constructing the full graph.",
    "uncertainty": "Corpus sampling I/O is not measured."
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
        "SP-002",
        "SP-003"
      ],
      "text": "The source threshold is a practical heuristic and is not proven to guarantee a target recall across unseen models.",
      "uncertainty": "Threshold calibration remains distribution-specific."
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
      "text": "The sample represents the deployed embedding distribution and top-candidate overlap correlates with BQ graph navigability.",
      "uncertainty": "Correlation is empirical."
    }
  ]
}
```

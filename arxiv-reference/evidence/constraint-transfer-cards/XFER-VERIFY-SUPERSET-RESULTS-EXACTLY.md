# Verify Superset Results Exactly

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "The requested result contract requires exact predicate validity and completeness within the search candidate semantics."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The surviving invariant forbids false negatives."
      ],
      "text": "A compact predicate that is merely high-recall rather than no-false-negative does not preserve exact filtered results and invalidates this transfer.",
      "uncertainty": "Approximate product modes may choose a different contract but are not this transfer."
    },
    {
      "assumptions": [
        "Traversal and predicate approximation are reported as separate semantics."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "G06 shows that verification cannot recover a rejected valid candidate.",
        "The source permits false-positive work."
      ],
      "text": "Exact verification preserves only validity of visited candidates; it does not make an approximate traversal complete, restore an omitted nearest neighbor, or prove a lower-resource plan.",
      "uncertainty": "Target-family completeness and recall oracles differ."
    },
    {
      "assumptions": [
        "Knight Bus storage may not match the source layout."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source's no-extra-read case depends on shared records or pages."
      ],
      "text": "A layout without measured attribute colocation may add verification I/O instead of piggybacking on reranking reads.",
      "uncertainty": "Physical page behavior requires measurement."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-VERIFY-SUPERSET-RESULTS-EXACTLY",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-FALSE-NEGATIVES-ESCAPE-VERIFICATION",
      "response": "Add a hard admission guard requiring a demonstrated no-false-negative selector for the exact predicate domain; route unsupported or drifted predicates to an exact plan or refusal because final verification cannot restore an unvisited candidate."
    },
    {
      "applies": true,
      "failure_id": "FAIL-FALSE-POSITIVES-AMPLIFY-WORKLOAD",
      "response": "Measure and bound false-positive candidate expansion, exact checks, and physical page reads before selecting the speculative branch; retain exact filtering as a fallback when added work reaches or exceeds the admitted resource envelope."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "Knight Bus can identify the predicate encoding before execution."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The surviving source invariant requires no false negatives.",
        "FAIL-FALSE-NEGATIVES-ESCAPE-VERIFICATION shows that final verification cannot recover an unvisited valid candidate."
      ],
      "text": "Admit superset exploration only for predicate encodings whose supported domain has a demonstrated no-false-negative contract; otherwise select exact prefiltering, exact in-filtering, a complete exact scan, or refusal.",
      "uncertainty": "The validation method for each predicate family remains to be specified and tested."
    },
    {
      "assumptions": [
        "The runtime exposes predicate selectivity, selector configuration, record layout, and candidate-budget inputs."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 requires a full-working-set estimate and fit, spill, approximate, or refuse decision.",
        "FAIL-FALSE-POSITIVES-AMPLIFY-WORKLOAD identifies a resource crossover."
      ],
      "text": "Before selecting the speculative branch, estimate selector RAM, false-positive candidate expansion, exact-verification pages, duplicated attribute storage, temporary candidate state, and concurrent-query state under the declared budget.",
      "uncertainty": "Target-distribution false-positive expansion and page reuse are unknown until measured."
    },
    {
      "assumptions": [
        "The implementation exposes counters at the approximate and exact stages."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 separates admission, execution observation, verification, and receipt obligations."
      ],
      "text": "The receipt must distinguish predicate exactness from search approximation and report observed false positives, candidate expansions, exact checks, attribute pages, result checksum, and any fallback or refusal.",
      "uncertainty": "The eventual receipt field names are outside this transfer."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Concurrency admission reserves private candidate and I/O state before execution."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "CONCURRENCY_peak_bytes = C_queries*(B_candidate_frontier + B_query_worker) + C_io*B_io_request + c_scheduler_metadata*(C_queries + C_io)",
      "measurement_needed": "Measure per-query private state, in-flight request metadata, queue growth, and peak aggregate RSS at each admitted concurrency.",
      "uncertainty": "Shared caches and batched reads may make growth non-additive.",
      "unknown_constants": [
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_candidate_frontier: private candidate-frontier and selector-state bytes per admitted query",
        "B_io_request: bytes held per in-flight I/O request",
        "B_query_worker: private exact-verification and rerank worker bytes per admitted query",
        "C_io: in-flight storage requests",
        "C_queries: concurrent admitted queries"
      ]
    },
    "io": {
      "assumptions": [
        "The counters distinguish logical candidate checks from physical page reads."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_query_bytes = (Q_base_graph_pages + alpha_false_positive_expansion*F_false_positive)*P_graph_page + Q_rerank_pages*P_vector_page + rho_uncolocated_attributes*Q_verify_pages*P_attribute_page",
      "measurement_needed": "Count graph, vector, and attribute bytes separately and measure false-positive expansion plus page colocation for each predicate and cache state.",
      "uncertainty": "Early termination, clustered validity, readahead, and cache reuse can change physical I/O.",
      "unknown_constants": [
        "alpha_false_positive_expansion",
        "rho_uncolocated_attributes"
      ],
      "variables": [
        "F_false_positive: false-positive candidates expanded",
        "P_attribute_page: bytes per exact-attribute page",
        "P_graph_page: bytes per graph-record page",
        "P_vector_page: bytes per reranking-vector page",
        "Q_base_graph_pages: graph pages required without false positives",
        "Q_rerank_pages: vector pages fetched for exact reranking",
        "Q_verify_pages: candidate pages requiring exact properties"
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Selector creation and record-layout preparation are charged before query admission or amortized over an explicit reuse count."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "PREP_work_units = N_nodes*c_selector_build + N_nodes*c_record_layout + E_edges*c_graph_layout",
      "measurement_needed": "Measure build time, peak build RSS, bytes read and written, and update amplification for selector summaries and colocated records.",
      "uncertainty": "Mutation frequency and property widths determine amortization.",
      "unknown_constants": [
        "c_graph_layout",
        "c_record_layout",
        "c_selector_build"
      ],
      "variables": [
        "E_edges: indexed graph edge count",
        "N_nodes: indexed node count",
        "PREP_work_units: implementation-defined build work units"
      ]
    },
    "ram": {
      "assumptions": [
        "Exact vector and attribute pages are not silently counted as resident hot state unless they remain mapped or cached at peak."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_graph_hot + N_nodes*b_selector_per_node + B_exact_page_cache_peak + C_queries*(B_candidate_frontier + B_query_worker) + C_io*B_io_request + c_scheduler_metadata*(C_queries + C_io) + b_runtime_overhead",
      "measurement_needed": "Measure compact hot-state RSS, exact vector/attribute page-cache or mapped-page high-water, private query state, in-flight I/O-request bytes, scheduler metadata, and aggregate RSS under cold and warm cache conditions while varying selector configuration, candidate budget, colocation, query concurrency, and I/O concurrency.",
      "uncertainty": "Allocator, page-cache, mapped-page, and runtime overhead are target-specific.",
      "unknown_constants": [
        "b_runtime_overhead",
        "b_selector_per_node",
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_candidate_frontier: private candidate-frontier and selector-state bytes per admitted query",
        "B_exact_page_cache_peak: shared exact-vector and exact-attribute pages physically resident at peak, excluding B_graph_hot and private query buffers",
        "B_graph_hot: resident navigation and metadata state in bytes",
        "B_io_request: bytes held per in-flight I/O request, excluding shared exact-page cache",
        "B_query_worker: per-query buffers and visited state in bytes",
        "C_io: admitted in-flight storage requests",
        "C_queries: concurrent admitted queries",
        "N_nodes: indexed node count"
      ]
    },
    "storage": {
      "assumptions": [
        "All retained verification and scan layouts are counted rather than treating duplicated attributes as free."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "STORAGE_bytes = B_graph + B_vectors_exact + B_attributes_exact + B_selector + B_attribute_index + b_storage_padding",
      "measurement_needed": "Measure durable bytes by component, including duplicate properties, alignment, manifests, indexes, and update metadata.",
      "uncertainty": "Serialization, compression, property widths, and update metadata are unspecified.",
      "unknown_constants": [
        "b_storage_padding"
      ],
      "variables": [
        "B_attribute_index: exact prefilter index bytes",
        "B_attributes_exact: persisted exact property bytes including duplication",
        "B_graph: persisted graph-record bytes",
        "B_selector: persisted compact-selector bytes",
        "B_vectors_exact: persisted full-vector bytes"
      ]
    }
  },
  "name": "Verify Superset Results Exactly",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Compact approximate selectors and compressed vectors are memory-resident, while graph records, full vectors, and exact attributes are accessed from SSD.",
      "uncertainty": "Page placement and cache residency determine the actual access cost."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "A query performs local graph exploration using memory-resident approximate checks and fetches SSD records for traversal and final reranking or verification.",
      "uncertainty": "The source does not define distributed communication or cross-query scheduling."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The mechanism constrains exact attribute reads during graph exploration by using compact approximate attribute summaries and deferring exact checks to retained result candidates.",
      "uncertainty": "The source does not provide a portable whole-process RAM or I/O bound."
    },
    "data_mutability": {
      "assumptions": [
        "No uncited source passage is being used to fill the missing mutation model."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The inspected mechanism card describes index construction and query execution but no consistency or incremental-update protocol."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Selector summaries and duplicated exact attributes may require coordinated refresh after updates."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The described operating model places compact query guidance in memory and exact vector, graph, and attribute records on SSD, with verification attributes colocated with reranking vectors when possible.",
      "uncertainty": "No assumption about a Knight Bus machine is inferred from publication year."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Correctness requires the approximate selector to have no false negatives and requires exact verification before a result is returned.",
      "uncertainty": "The source does not establish a portable bound on false-positive work."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Strict exact filtering before or during exploration causes exact attribute scans or random attribute reads from SSD.",
      "uncertainty": "Severity depends on selectivity, layout, and device behavior."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Exact result validity is preserved only when the approximate selector rejects no valid candidate and retained candidates are checked against original attributes.",
      "uncertainty": "Approximate nearest-neighbor recall remains distinct from predicate validity."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "The source exchanges exact attribute access during exploration for memory-resident approximate checks, false-positive traversal, and exact checks on reranking candidates; storage also includes approximate summaries and duplicated exact-attribute layouts.",
    "uncertainty": "The source cost model omits some clustered-distribution, early-termination, cache, and temporary-state effects."
  },
  "original_domain": "SSD-resident filtered approximate nearest-neighbor search",
  "proposed_transfer": {
    "assumptions": [
      "The target workload has an exact property oracle and a candidate-stage predicate summary with no false negatives in its supported domain."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "The source separates permissive candidate exploration from exact final validity.",
      "G06 requires a no-false-negative guard and a false-positive work envelope.",
      "A007 requires a pre-run branch and post-run verification receipt."
    ],
    "text": "For filtered vector kNN, node similarity, or bounded filtered path workloads, use a no-false-negative compact predicate summary to admit an exploration superset, bound the candidate and verification stages separately, and validate every returned item against exact properties before release; route the job to an exact plan or refusal when the invariant or resource envelope is unavailable.",
    "uncertainty": "Correctness and resource benefit outside the source ANNS setting are unmeasured and must be falsified per target family."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "Modern device, cache, and layout coefficients are measured rather than borrowed from the source benchmark."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "G06 identifies a crossover where false-positive traversal plus verification can equal or exceed exact filtering.",
        "A007 requires target-specific working-set and I/O accounting."
      ],
      "text": "The source's workload-specific balance between avoided attribute reads and added false-positive work is not carried forward as a modern cost advantage.",
      "uncertainty": "The crossover is fixture-specific."
    },
    {
      "assumptions": [
        "Knight Bus may use a different record and page layout."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source's deferred verification can avoid additional reads only when exact attributes share fetched reranking records or pages."
      ],
      "text": "Attribute colocation is treated as a measured layout property rather than an assumed source-layout benefit.",
      "uncertainty": "Update policy and variable-width properties may change colocation."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "cache state",
      "candidate budget",
      "exact predicate",
      "graph topology",
      "property colocation",
      "selector false-negative injection",
      "selector false-positive rate",
      "concurrent-query cap",
      "in-flight-I/O cap"
    ],
    "failure_signal": "Any oracle-valid result becomes unreachable, any returned result fails exact verification, the plan accepts without reserving modeled state, or observed false-positive and verification work exceeds the admitted envelope without fallback.",
    "fixture": "A small disk-backed graph with one oracle-valid result reachable only through a candidate controlled by the approximate predicate, plus colliding false-positive records and separately colocated or uncolocated exact properties.",
    "independent_oracle": "Exhaustive exact predicate evaluation followed by the target family's exact result oracle, with component-attributed per-query state, I/O-request, scheduler, page-cache, device-byte, and aggregate peak-RSS counters."
  },
  "source_pattern_ids": [
    "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
    ],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Approximate selection may add false positives but must not remove any valid candidate; exact original attributes determine final result validity.",
    "uncertainty": "This invariant preserves predicate validity, not universal nearest-neighbor recall or lower resource use."
  },
  "target_algorithm_families": [
    "BOUNDED_FILTERED_PATH_SEARCH",
    "FILTERED_NODE_SIMILARITY",
    "FILTERED_VECTOR_KNN"
  ],
  "transfer_id": "XFER-VERIFY-SUPERSET-RESULTS-EXACTLY",
  "unknown_measurement_constants": [
    "alpha_false_positive_expansion",
    "b_runtime_overhead",
    "b_selector_per_node",
    "b_storage_padding",
    "c_graph_layout",
    "c_record_layout",
    "c_scheduler_metadata",
    "c_selector_build",
    "rho_uncolocated_attributes"
  ]
}
```

# Pack Nonempty Sparse Rows

- Pattern ID: `PAT-PACK-NONEMPTY-SPARSE-ROWS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can count nonempty rows and nonzeros per candidate tiling.",
      "The chosen tile permits the source's identifier widths and marker-bit convention."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source provides a closed payload-size expression for SCSR.",
      "The source describes a one-read, one-write conversion from CSR."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-006"
    ],
    "text": "Knight Bus can estimate a candidate sparse payload from nnr, nnz, and value width before admitting a tiled scan, while keeping file-container and decoder memory as separately measured overheads.",
    "uncertainty": "The paper does not quantify total container overhead, so the payload estimate alone is not a complete storage admission bound."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "A sequential tile scan decodes each stored row header and its following columns, then consumes the COO suffix for single-entry rows; empty rows are skipped implicitly because they have no records.",
    "uncertainty": "The paper does not state a standalone decoder pseudocode."
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
      "SP-004",
      "SP-005"
    ],
    "text": "The paper defines the byte layout and size equation, explains the single-entry COO suffix, and reports workload-dependent I/O effects; this campaign did not reproduce the encoder or inspect its code.",
    "uncertainty": "The full file format may include overhead absent from the per-tile payload expression."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Tiles are stored in row-major order; each tile contains an SCSR region for multi-entry nonempty rows followed by COO entries for single-entry rows.",
    "uncertainty": "The relative sizes of the two regions are data-dependent."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "On a well-clustered graph where the comparison representation is already small, SCSR's additional measured speedup is less significant.",
      "uncertainty": "The source reports reduced benefit rather than incorrectness."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PACK-NONEMPTY-SPARSE-ROWS",
  "falsifying_test": {
    "controlled_variables": [
      "tile dimensions",
      "identifier width",
      "value width c",
      "row order",
      "nnr",
      "nnz"
    ],
    "failure_signal": "Decoding differs from the coordinate oracle, an empty row consumes a row header, or payload bytes differ from 2*nnr + (2+c)*nnz under the source's encoding assumptions",
    "fixture": "One sparse tile containing an empty row, a single-entry row, and a multi-entry row with known values",
    "independent_oracle": "A canonical coordinate list and a byte-counted reference encoder-decoder",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Each nonempty tile row is represented by one row header followed by its column identifiers, while empty tile rows consume no row header.",
    "uncertainty": "The stated two-byte identifiers rely on the paper's tile-size design."
  },
  "knight_bus_algorithm_families": [
    "SPARSE_MATRIX_DENSE_MULTIPLICATION",
    "GRAPH_LINEAR_ALGEBRA"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Tile the sparse matrix, emit headers only for nonempty rows, distinguish row headers from column identifiers with the most significant bit, and encode single-entry rows in an adjacent COO region to avoid repeated row-end branches.",
    "uncertainty": "The source evaluates this SCSR plus COO combination rather than each encoding choice in full isolation."
  },
  "name": "Pack Nonempty Sparse Rows",
  "pattern_id": "PAT-PACK-NONEMPTY-SPARSE-ROWS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Within graph-matrix tiles, conventional row or column pointer arrays spend space on empty rows or columns, increasing SSD traffic for semi-external multiplication.",
    "uncertainty": "The waste depends on tile sparsity and graph clustering."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Empty-row offsets are not stored or reconstructed as an explicit array; row identity is recovered from each stored row header during decoding.",
    "uncertainty": "The paper does not quantify decoder arithmetic separately."
  },
  "related_pattern_ids": [
    "PAT-STREAM-SPARSE-KEEP-DENSE"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Only the currently processed sparse tiles and decoding state need be resident; the encoded matrix itself is designed for SSD storage.",
    "uncertainty": "The encoding section does not isolate decoder RAM from the broader SEM-SpMM buffers."
  },
  "resource_model": {
    "io": {
      "assumptions": [
        "Every encoded payload byte is read once in the scan.",
        "No extra index, alignment, prefetch, or filesystem bytes are counted."
      ],
      "expression": "One full encoded sparse scan reads S_SCSR bytes, excluding container and alignment overhead",
      "measurement_needed": "Measure logical read bytes and physical device bytes for one complete scan.",
      "premises": [
        "The source gives S_SCSR as the encoded tile payload size.",
        "The source streams the compact sparse representation from SSDs."
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "DERIVED",
      "uncertainty": "Actual device traffic can exceed logical encoded bytes."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "S_SCSR = 2*nnr + (2+c)*nnz bytes per encoded tile payload",
      "measurement_needed": "Measure complete-file bytes including tile directory, headers, and alignment.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "nnr is nonempty-row count, nnz is nonzero count, and c is value bytes; outer tile metadata and alignment are not included in the displayed expression."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Linear-time CSR-to-SCSR conversion with one sequential CSR read and one sequential SCSR write",
      "measurement_needed": "Measure conversion time and bytes for target tile and value widths.",
      "premises": [],
      "source_pointer_ids": [
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "The conversion result includes the paper's complete SCSR representation."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak resident decoder state and tile-buffer allocation independently of dense operands.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate peak decoder RAM for SCSR plus COO from SEM-SpMM's tile and output buffers."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak temporary bytes during both conversion and one scan.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not provide a separate formula for conversion or decoding scratch space."
    }
  },
  "source_domain": "Compact tiled sparse-matrix encoding for graph-shaped SpMM operands",
  "source_paper_ids": [
    "PAPER-1602.02864"
  ],
  "source_pointers": [
    {
      "claim_scope": "A compact tiled format is introduced to reduce SSD bytes and improve cache locality.",
      "locator_type": "SECTION",
      "locator_value": "3.2 Sparse matrix format",
      "page": 3,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "SCSR stores only nonempty tile rows, marks row headers with the most significant bit, and uses compact row and column identifiers.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 1 and Section 3.2 SCSR description",
      "page": 4,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "For nnr nonempty rows and nnz nonzeros, S_SCSR = 2*nnr + (2+c)*nnz bytes.",
      "locator_type": "EQUATION",
      "locator_value": "Section 3.2, S_SCSR storage expression",
      "page": 4,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Single-entry rows use COO behind SCSR row headers to reduce end-of-row tests without increasing storage.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 3.2, SCSR plus COO single-entry-row paragraph",
      "page": 4,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "SCSR's measured benefit is larger on the unclustered Friendster graph and less significant on the clustered Page graph.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 13 and Section 5.4 I/O-optimization discussion",
      "page": 10,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "CSR-to-SCSR conversion is linear, sequentially reads CSR once, sequentially writes SCSR once, and is a one-time amortizable cost.",
      "locator_type": "TABLE",
      "locator_value": "Table 2 and following format-conversion paragraph",
      "page": 11,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-004"
    ],
    "text": "The SCSR plus COO tile stream, containing row headers, column identifiers, and stored nonzero values when present, is read from SSDs.",
    "uncertainty": "Binary sparse matrices omit value bytes through c = 0 in the source's comparison."
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
        "SP-003",
        "SP-005"
      ],
      "text": "The paper does not establish a universal nnr-to-nnz or clustering threshold at which SCSR plus COO ceases to outperform another sparse encoding.",
      "uncertainty": "The storage equations permit comparison, but runtime also depends on device and decoder behavior."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "text": "The format saves space when tiles contain many empty rows, and the paper reports a larger I/O benefit on its unclustered graph where the comparison format is less compact.",
      "uncertainty": "Benefit magnitude varies with nnr, nnz, and clustering."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "Rows with one nonzero benefit from the COO suffix because it avoids an end-of-row conditional test for every such entry without increasing the stated storage size.",
      "uncertainty": "The paper does not report an isolated branch-count benchmark for this submechanism."
    }
  ]
}
```

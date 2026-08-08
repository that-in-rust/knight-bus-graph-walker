# Low-RAM Architecture Integration Audit

## Audit Verdict

The expanded A007 research/specification goal is evidence-complete when all validators listed below pass from a fresh working tree state. It does not claim that the Rust product implements the 27 architectures or meets any RAM/latency estimate yet.

The binding outcome is:

> Build a proof-carrying artifact-to-answer graph runner. Use one small immutable artifact spine, content-addressed algorithm-shaped views, a deterministic option registry, full working-set admission, supervised enforcement, bounded output, and receipts. Keep Neo4j compatibility at the adapter/oracle boundary. Do not build a database.

## Evidence Denominators

### Neo4j family

| Measure | Reconciled value |
|---|---:|
| Repositories | 20 |
| Tracked files | 32,262 |
| Git blob bytes | 478,516,872 |
| Direct reads | 5,014 |
| Graph indexed | 19,186 |
| Non-code classified | 7,103 |
| Binary classified | 845 |
| Generated classified | 114 |

Validation: `python3 scripts/validate_neo4j_family_evidence.py`.

### PRD03 through PRD06

| Lane | Files | Semantic reads | Structured queries | Other honest classifications |
|---|---:|---:|---:|---:|
| PRD03 | 222 | 37 | 84 | 100 generated + 1 binary |
| PRD04 | 68 | 56 | 6 | 6 superseded |
| PRD05/06 | 94 | 93 | 1 | 0 |
| **Total** | **384** | **186** | **91** | **107** |

Frozen bytes: 224,889,145. Validation: `python3 scripts/validate_lowram_document_evidence.py`.

The classification distinction matters. Generated dossiers, DOT/TSV outputs, diagnostics, SQLite, and workbook data were queried or classified according to their structure. They are not represented as hundreds of independent human-authored corroborations.

## Three-Lane Synthesis

### PRD03 lane

What survived A007:

- immutable generations, checksums, dense IDs, selected sidecars, bounded external builds, and atomic publication;
- one canonical adjacency substrate with optional orientation/views, not a durable database format per algorithm;
- compositional memory estimates and phase high-water accounting;
- GDS as estimator/fixture/oracle evidence;
- bounded paths and WCC as the first kernels.

What was rejected or narrowed:

- Rust OLTP record-store rewrite;
- full Bolt/Cypher/driver/transaction parity as product scope;
- mmap as proof of a hard RAM bound;
- a mutable LSM/delta serving graph;
- “GDS has no estimates” and other superseded differentiators.

### PRD04 lane

What survived A007:

- hard admission, enforcement, receipts, output accounting, and strict versus fast profiles;
- a portfolio of algorithm-shaped views selected by workload/result/cadence/budget;
- representation elimination before compression;
- build/publication resource accounting under the same promise;
- the first ICP around security/IAM/dependency/SBOM/access paths.

What remains hypothesis:

- claimed RAM or latency multipliers;
- “top seven cover 80–90%” without representative production telemetry;
- exact estimator certainty for dynamic frontier/candidate/tally/convergence state;
- commercial value of a 50 GB on 16 GB capacity milestone without a paid recurring job.

### PRD05/06 and graph-learning lane

Chosen patterns:

- CSR/CSC as oriented views, not one universal topology;
- sparse/dense push-pull switching with hysteresis;
- packed union-find and hook/shortcut components;
- pull and destination-tiled PageRank;
- Roaring/list/bitset selection by local density;
- family-specific external passes and partitions;
- metamorphic and tolerant-equivalence oracle registries;
- immutable view publication and preparation accounting.

Borrowed selectively:

- posting blocks and safe top-K pruning for similarity;
- DiskANN/IVF/PQ ideas for a future approximate kNN profile;
- COW/LSM ideas only for view lifecycle, not an OLTP engine;
- demand-driven pull output and protocol stubs only at the compatibility boundary.

Rejected/deferred:

- WAL, MVCC, record-chain adjacency, cluster-first supersteps, standing incremental dataflow, broad ANN/GNN/RDF/full-text scope, and distributed graph execution in the first product.

## Normative Architecture Registry

The full contracts are in `LowRAM-Algorithm-Architecture-Decision-Atlas.md`.

| Family | Exact fit | Exact spill | Retained alternate | Roadmap |
|---|---|---|---|---|
| Paths/BFS | `ARCH-PATH-001` | `ARCH-PATH-002` | `ARCH-PATH-003` | Build first |
| WCC | `ARCH-WCC-001` | `ARCH-WCC-002` | `ARCH-WCC-003` | Build second |
| PageRank | `ARCH-PAGERANK-001` | `ARCH-PAGERANK-002` | `ARCH-PAGERANK-003` | First iterative proof |
| NodeSimilarity | `ARCH-NODESIM-001` | `ARCH-NODESIM-002` | `ARCH-NODESIM-003` | Buyer-gated |
| kNN | `ARCH-KNN-001` | `ARCH-KNN-002` | `ARCH-KNN-003` | Defer |
| Louvain | `ARCH-LOUVAIN-001` | `ARCH-LOUVAIN-002` | `ARCH-LOUVAIN-003` | Defer |
| Leiden | `ARCH-LEIDEN-001` | `ARCH-LEIDEN-002` | `ARCH-LEIDEN-003` | After Louvain |
| Triangle | `ARCH-TRIANGLE-001` | `ARCH-TRIANGLE-002` | `ARCH-TRIANGLE-003` | After WCC/PageRank |
| FastRP | `ARCH-FASTRP-001` | `ARCH-FASTRP-002` | `ARCH-FASTRP-003` | Defer |

Every family has at least three options, a chosen fit default, and a chosen exact spill default. “Chosen” means normative within an advertised profile; it does not override the roadmap gate.

## Guiding Mega-Spec State

`docs_PRD04/Neo4j-Compatibility-LowRAM-Mega-Spec.md` now contains:

- 106 executable WHEN/THEN/SHALL requirements;
- 60 test matrix entries;
- 100% requirement-to-test mapping;
- 60 verified Neo4j-family evidence citations;
- 10 verified PRD/document evidence citations;
- six verified local implementation/code-graph citations;
- explicit architecture registry, deterministic selection, semantic fallback, mapped/output accounting, forced fit/spill/refuse, format-proliferation, spill-invariant, and founder breadth requirements.

Validation: `python3 scripts/validate_mega_spec_contracts.py`.

## Current Rust Gap

Code-graph and codebase-memory inspection found useful foundations:

- a narrow canonical Cypher neighborhood-walk compiler;
- a Bolt execution path and contract tests;
- a GDS catalog/projection/estimate surface;
- immutable mmap adjacency runtime;
- bounded external sort/run machinery for artifact construction.

The current Rust repository does not yet implement:

- a generalized versioned architecture option registry;
- complete per-option working-set estimators and deterministic selector;
- supervised process-level hard enforcement across algorithm runs;
- the nine family kernel portfolios;
- forced exact spill implementations for those families;
- per-family proof receipts and differential oracle suites.

This is a specification/research completion, not an implementation completion.

## Exact Next Goal

Freeze `KB-ACCESS-P0` with one customer-shaped artifact, query/profile, official driver/version if needed, exact answer oracle, and 5 GB/10 GB or partner-selected ceilings. Then implement only this proof:

```text
artifact manifest and statistics
        |
versioned PATH plan registry
        |
ARCH-PATH-001 fit
ARCH-PATH-002 forced spill
refuse below minimum legal state
        |
supervised hard ceiling + bounded output
        |
tiny oracle + Neo4j/GDS differential
        |
terminal proof receipt + estimator calibration
```

Do not begin NodeSimilarity, communities, kNN, FastRP, broad Cypher, or database scope while this slice is incomplete. WCC follows only after the path receipt closes; PageRank follows WCC as the first iterative proof.

## Residual Decisions

1. The exact first customer artifact/query and budget owner.
2. Linux cgroup v2 versus other supervisor policy for the first supported platform.
3. User-visible charging of mapped clean/dirty/shared pages and page cache.
4. Initial estimator safety margin, overshoot, cancellation, temporary, and time thresholds.
5. Legal/distribution role of openCypher, TestKit, driver, GDS, APOC, and Neo4j fixtures.
6. Empirical thresholds among fit/spill/alternate options on held-out graph shapes.
7. Whether the receipt itself changes willingness to pay and repeat use.

None of these gaps justifies a Neo4j database rewrite.

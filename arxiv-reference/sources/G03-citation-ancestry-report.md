# G03 Citation Ancestry Report

**Status:** `METADATA_TRAVERSAL_COMPLETE`
**Epistemic boundary:** OpenAlex provider relations establish only `CITES`. All branch roles and decision scores are `DERIVED_INFERENCE` metadata-screening judgments, not `SOURCE_CLAIM`s.

## Executive Result

G03 converted the 25 G02 seeds into a bounded, depth-2 citation map and an exact G04 reading queue. It did not read or acquire a paper. The result prioritizes citation-visible branches that can change an open architecture question; it does not prove any mechanism, performance result, or compatibility claim.

## Campaign Accounting

| Measure | Count | Cap |
|---|---:|---:|
| Initial seeds | 25 | exactly 25 |
| External HTTP attempts | 28 | 90 |
| Raw metadata observations | 1 | 6,000 |
| Baseline canonical identities | 262 | frozen |
| Final canonical identities | 262 | N/A |
| New canonical identities retained | 0 | 250 |
| Citation and semantic edges | 0 | N/A |
| Papers read | 0 | 0 |
| Full-text/PDF files acquired | 0 | 0 |
| Repositories acquired | 0 | 0 |

Edge counts: .

## Foundational Branches

Backward branches are older or referenced candidates retained by the frozen taxonomy screen. The label 'foundational' is a reading priority, not a claim about originality.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `NONE` | No branch survived the frozen metadata screen | N/A | N/A | 0 |

## Implementation And Evaluation Branches

Forward branches are later citing works. A role is semantically typed only when its title explicitly anchors the cited target; otherwise the report merely nominates the branch for G04 reading.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `NONE` | No branch survived the frozen metadata screen | N/A | N/A | 0 |

## Contradictory Branches

These titles contain an explicit counterexample, lower-bound, impossibility, limitation, or incorrectness signal. Reading is required before treating any as contradictory evidence.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `NONE` | No branch survived the frozen metadata screen | N/A | N/A | 0 |

## Stopped Branches

| Stop reason | Count |
|---|---:|
| `SEED_OPENALEX_UNAVAILABLE` | 24 |

Depth-2 works were retained but never expanded. Forward traversal used one page per operation; backward traversal sampled at most 12 evenly spaced references per expanded work. These are explicit recall limits.

## Architecture-Question Decision Impact

| Architecture question | Retained branch identities | Decision effect |
|---|---:|---|
| `AQ-001` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-002` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-003` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-004` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-005` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-006` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-007` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-008` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-009` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-010` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-011` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |
| `AQ-012` | 0 | `NO_NEW_CITATION_VISIBLE_BRANCH` |

## Coverage Gaps

- OpenAlex `referenced_works` omits references it cannot resolve to an OpenAlex identity; this is not a complete bibliography.
- Exact arXiv-location resolution can miss provider records whose location metadata differs.
- One-page forward traversal can miss lower-ranked citing works beyond 100 results.
- Twelve-reference sampling can miss a relevant ancestor in a long bibliography.
- Titles and bibliographic types cannot prove implementation, evaluation, contradiction, mechanism, correctness, RAM, or latency claims.
- No citation metadata directly closes Bolt, Cypher, GDS procedure, admission-control, whole-process RSS, or verification-receipt gaps unless its title matched the frozen taxonomy.

## Exact Recommended G04 Acquisition Set

The set contains all 25 original seeds plus at most 25 highest-priority new ancestry identities after global deduplication. Order is reading priority within the two groups. G04 must perform its own license and acquisition preflight.

| # | Paper | Metadata title | G03 basis |
|---:|---|---|---|
| 1 | `PAPER-2511.07886` | ACGraph: An Efficient Asynchronous Out-of-Core Graph Processing Framework | `G02_SEED` |
| 2 | `PAPER-1905.04264` | PartitionedVC: Partitioned External Memory Graph Analytics Framework for SSDs | `G02_SEED` |
| 3 | `PAPER-2507.12925` | Efficient Semi-External Breadth-First Search | `G02_SEED` |
| 4 | `PAPER-2010.09913` | SlimSell: A Vectorizable Graph Representation for Breadth-First Search | `G02_SEED` |
| 5 | `PAPER-1709.07122` | Accelerating PageRank using Partition-Centric Processing | `G02_SEED` |
| 6 | `PAPER-1602.02864` | Semi-External Memory Sparse Matrix Multiplication for Billion-Node Graphs | `G02_SEED` |
| 7 | `PAPER-1812.10950` | Fast Breadth-First Search in Still Less Space | `G02_SEED` |
| 8 | `PAPER-2404.16267` | Dynamic PageRank: Algorithms and Lower Bounds | `G02_SEED` |
| 9 | `PAPER-2602.21514` | I/O Optimizations for Graph-Based Disk-Resident Approximate Nearest Neighbor Search: A Design Space Exploration | `G02_SEED` |
| 10 | `PAPER-2603.01779` | Disk-Resident Graph ANN Search: An Experimental Evaluation | `G02_SEED` |
| 11 | `PAPER-2605.02171` | QuIVer: Rethinking ANN Graph Topology via Training-Free Binary Quantization | `G02_SEED` |
| 12 | `PAPER-2112.00098` | Connected Components for Infinite Graph Streams: Theory and Practice | `G02_SEED` |
| 13 | `PAPER-0708.4284` | Optimal Per-Edge Processing Times in the Semi-Streaming Model | `G02_SEED` |
| 14 | `PAPER-1810.08473` | From Louvain to Leiden: guaranteeing well-connected communities | `G02_SEED` |
| 15 | `PAPER-1304.4453` | Engineering Parallel Algorithms for Community Detection in Massive Networks | `G02_SEED` |
| 16 | `PAPER-1407.6755` | Dynamic Set Intersection | `G02_SEED` |
| 17 | `PAPER-1708.07271` | Exploiting Computation-Friendly Graph Compression Methods | `G02_SEED` |
| 18 | `PAPER-2104.09616` | MELOPPR: Software/Hardware Co-design for Memory-efficient Low-latency Personalized PageRank | `G02_SEED` |
| 19 | `PAPER-2012.10026` | Fast and Efficient Parallel Breadth-First Search with Power-law Graph Transformation | `G02_SEED` |
| 20 | `PAPER-2503.00430` | Performance-Driven Optimization of Parallel Breadth-First Search | `G02_SEED` |
| 21 | `PAPER-2305.11053` | (Noisy) Gap Cycle Counting Strikes Back: Random Order Streaming Lower Bounds for Connected Components and Beyond | `G02_SEED` |
| 22 | `PAPER-2204.11137` | Evaluating regular path queries under the all-shortest paths semantics | `G02_SEED` |
| 23 | `PAPER-2412.07729` | Output-Sensitive Evaluation of Regular Path Queries | `G02_SEED` |
| 24 | `PAPER-1603.01876` | PageRank Pipeline Benchmark: Proposal for a Holistic System Benchmark for Big-Data Platforms | `G02_SEED` |
| 25 | `PAPER-2401.01019` | Approximating Single-Source Personalized PageRank with Absolute Error Guarantees | `G02_SEED` |

Exact G04 set size: **25** canonical identities.

## Scope Boundary

G03 downloaded no PDF, abstract, paper body, source archive, or repository; read no paper; created no mechanism, failure, or transfer card; proposed no architecture or experiment; and did not begin G04. OpenAlex response bodies remain ignored local cache files. The report is a citation-metadata routing artifact only.

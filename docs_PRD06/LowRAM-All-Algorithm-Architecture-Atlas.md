# Low-RAM All-Algorithm Architecture Atlas

Status: generated evidence synthesis, validator-backed.

Product north star:
`docs_PRD04/A007-spc-founder-interview-prep-v7.md`

Frozen research corpus: `docs_PRD06/graph-learning/`

Audit evidence: `docs_PRD06/reference-learning/all-algorithm-lowram/evidence/`

## Executive Thesis

The product is not a literal Neo4j clone. It is a
bounded analytical runtime that treats memory as a
contract. For each named graph, text, vector, storage,
or dataflow algorithm in the corpus, the runtime can
estimate the working set, choose a legal plan, enforce
the ceiling, and emit a receipt that proves what happened.

The strategic move is custom OLAP storage per access
pattern. A single logical request can run as fast-fit,
exact spill, shape-adaptive hybrid, bounded approximation,
or refuse. Refuse remains part of the product contract
because an honest no is better than an out-of-memory crash.

```text
+------------------+
| Logical request  |
+------------------+
         |
         v
+------------------+     graph shape + B_ram + SLA
| Artifact planner | <-----------------------------
+------------------+
         |  decision --> plan profile
         |
         v
+------------------+
| Working-set test |
+------------------+
         |
         +-- fit
         +-- spill
         +-- approximate
         +-- refuse
         |
         v
  +---------------+
  | Budget guard  |
  +---------------+
         |
         v
  +---------------+
  | Result receipt|
  +---------------+
```

The public promise is stronger than a memory estimate:

```text
estimate + select + enforce + measure = workload contract
```

## Evidence Receipt

The frozen denominator contains 88 files and 14158 lines.
The generated scan records 3399 line-level occurrences.
The canonical ledger contains 71 compute/storage algorithms.

```text
FROZEN CORPUS
      |
      v
+-----------+   +-----------+   +-----------+
| Lane 07   |   | Lane 08   |   | Lane 09   |
+-----------+   +-----------+   +-----------+
      |             |             |
      v             v             v
 file ledger    file ledger    file ledger
 occurrence     occurrence     occurrence
      |             |             |
      +-------------+-------------+
                    |
                    v
          +-------------------+
          | Canonical ledger  |
          +-------------------+
                    |
                    v
          +-------------------+
          | 3+ plans per algo |
          +-------------------+
```

## Common Cost Contract

All plans instantiate the same RAM equation:

```text
B_peak = B_os + B_topology + B_state + B_temp + B_output

legal(plan) iff upper_confidence_bound(B_peak) <= B_ram
```

An mmap file is not free RAM. The estimator counts the
maximum resident page window, allocator state, pinned
buffers, output windows, and kernel I/O buffers. Disk
capacity is tracked separately from resident memory.

## Plan Portfolio

```text
FAST FIT                         EXACT SPILL

+------------------+            +------------------+
| compact artifact |            | partitioned file |
+------------------+            +------------------+
| full hot state   |            | bounded window   |
+------------------+            +------------------+
| minimum passes   |            | merge/checkpoint |
+------------------+            +------------------+
 low latency                     more I/O, hard RAM

BOUNDED APPROX                    HYBRID

+------------------+            +------------------+
| sketch / sample  |            | hot exact tier   |
+------------------+            +------------------+
| explicit error   |            | cold spill tier  |
+------------------+            +------------------+
| fixed state      |            | adaptive planner |
+------------------+            +------------------+
 lowest RAM                      shape-sensitive
```

## Canonicalization Rules

The corpus mixes algorithms, algorithm families, storage
algorithms, protocols, and verification methods. Families
are grouped only when dominant access pattern, mutable
state, and correctness adapter are materially the same.
Protocols and oracles are crosswalked instead of receiving
fake compute plans.

```text
named corpus concept
        |
        v
+--------------------+
| Computes or builds |
| algorithm storage? |
+--------------------+
        |
   +----+----+
   | yes     | no
   v         v
algorithm   protocol/oracle
3+ plans    crosswalk
```

## Protocol And Oracle Crosswalk

- `protocol`
  - `bolt-protocol`
  - `mvcc-snapshot`
  - `packstream-encoding`
  - `pull-operator-pipeline`
- `verification_method`
  - `differential-validation`
  - `history-checking`
  - `metamorphic-testing`
  - `recall-validation`
  - `stub-conformance`
  - `tolerant-validation`

## Canonical Algorithm Summary

- ALG-001: A* heuristic path search (Traversal; evidence 6)
- ALG-002: Adamic-Adar and common neighbors (Similarity;
  evidence 1)
- ALG-003: Afforest sampled components (Components; evidence 14)
- ALG-004: ARIES recovery (Storage; evidence 6)
- ALG-005: Bellman-Ford relaxation (Traversal; evidence 5)
- ALG-006: Betweenness centrality (Centrality; evidence 18)
- ALG-007: BFS frontier traversal (Traversal; evidence 102)
- ALG-008: Bloom and Ribbon filters (Storage; evidence 94)
- ALG-009: BM25 scoring (Text retrieval; evidence 67)
- ALG-010: Centrality family (Centrality; evidence 24)
- ALG-011: Closeness centrality (Centrality; evidence 1)
- ALG-012: Community detection family (Community; evidence 16)
- ALG-013: Weakly connected components (Components; evidence 90)
- ALG-014: Copy-on-write tree snapshots (Storage; evidence 60)
- ALG-015: CSR build and adjacency layout (Graph storage;
  evidence 206)
- ALG-016: Degree centrality (Centrality; evidence 14)
- ALG-017: Delta and RLE compression (Storage; evidence 10)
- ALG-018: Delta-stepping SSSP (Traversal; evidence 34)
- ALG-019: DFS depth traversal (Traversal; evidence 2)
- ALG-020: Dijkstra weighted shortest path (Traversal; evidence
  11)
- ALG-021: Eigenvector, Katz, HITS, ArticleRank (Centrality;
  evidence 11)
- ALG-022: FastRP random projection embeddings (Embeddings;
  evidence 6)
- ALG-023: FastSV star contraction (Components; evidence 19)
- ALG-024: Frontier push/pull switching (Traversal; evidence 33)
- ALG-025: FST term dictionary (Text storage; evidence 99)
- ALG-026: Graph embedding family (Embeddings; evidence 17)
- ALG-027: Graph partitioning and cuts (Out-of-core; evidence
  29)
- ALG-028: GraphSAGE neighbor sampling (Embeddings; evidence 2)
- ALG-029: HashGNN and GNN message passing (Embeddings; evidence
  12)
- ALG-030: HNSW layered greedy ANN (Vector ANN; evidence 111)
- ALG-031: Hooking and shortcutting (Components; evidence 91)
- ALG-032: Immutable base plus delta rebuild (Graph storage;
  evidence 9)
- ALG-033: Incremental delta iteration (Incremental; evidence
  21)
- ALG-034: IVF partitioned probe (Vector ANN; evidence 112)
- ALG-035: Jaccard, overlap, and cosine similarity (Similarity;
  evidence 53)
- ALG-036: k-core peeling (Density; evidence 15)
- ALG-037: KD-tree search (Vector ANN; evidence 1)
- ALG-038: k-nearest-neighbor search (Similarity; evidence 11)
- ALG-039: Label propagation (Components; evidence 9)
- ALG-040: Leiden refinement (Community; evidence 8)
- ALG-041: Levenshtein automata (Text retrieval; evidence 8)
- ALG-042: Louvain modularity (Community; evidence 26)
- ALG-043: Locality-sensitive hashing (Vector ANN; evidence 1)
- ALG-044: LSM compaction (Storage; evidence 241)
- ALG-045: node2vec random-walk embeddings (Embeddings; evidence
  3)
- ALG-046: NodeSimilarity (Similarity; evidence 7)
- ALG-047: Ordered search and merge (Storage; evidence 23)
- ALG-048: Out-of-core graph processing (Out-of-core; evidence
  21)
- ALG-049: PageRank power iteration (Centrality; evidence 92)
- ALG-050: Posting block compression (Text storage; evidence
  139)
- ALG-051: Random projection trees (Vector ANN; evidence 1)
- ALG-052: Reciprocal rank and score fusion (Text retrieval;
  evidence 6)
- ALG-053: Reachability and reverse reach (Traversal; evidence
  48)
- ALG-054: Record-chain adjacency (Graph storage; evidence 19)
- ALG-055: Roaring bitmap id sets (Storage; evidence 155)
- ALG-056: Strongly connected components (Components; evidence
  8)
- ALG-057: Semiring graph traversal (Linear algebra; evidence
  81)
- ALG-058: Unweighted shortest paths (Traversal; evidence 25)
- ALG-059: SpMV and SpGEMM traversal (Linear algebra; evidence
  9)
- ALG-060: Superstep and BSP convergence (Incremental; evidence
  87)
- ALG-061: TF-IDF scoring (Text retrieval; evidence 1)
- ALG-062: Triangle counting (Density; evidence 26)
- ALG-063: Triple-permutation indexing (Graph storage; evidence
  24)
- ALG-064: Union-find path compression (Components; evidence 8)
- ALG-065: DiskANN and Vamana ANN (Vector ANN; evidence 91)
- ALG-066: Vector quantization family (Vector storage; evidence
  95)
- ALG-067: WAL group commit (Storage; evidence 98)
- ALG-068: WAND and block-max WAND (Text retrieval; evidence 77)
- ALG-069: Weighted path products (Linear algebra; evidence 1)
- ALG-070: Worst-case optimal joins (Incremental; evidence 2)
- ALG-071: Yen k-shortest paths (Traversal; evidence 2)

## Algorithm Architecture Portfolio

### ALG-001: A* heuristic path search

Category: Traversal.
Aliases observed: A*, a*.
Evidence count: 6; sample A07-O0174, A08-O0224.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: weighted CSR, distance array, predecessor log, and
bucket or heap state.
Spill note: partitioned relax logs with bucket spill files and
checkpointed distances.
Hybrid note: resident active buckets with compressed cold
distances and page-cache hints.
State note: distance + predecessor + relaxation worklist.
Oracle note: differential against reference SSSP plus triangle-
inequality checks.
Use note: weighted impact paths, routing, attack paths, and
dependency distances.
Reject note: reject negative-weight inputs for plans that
require monotone relaxations.

#### ALG-001-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0174, A08-O0224

#### ALG-001-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0174, A08-O0224

#### ALG-001-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0174, A08-O0224

### ALG-002: Adamic-Adar and common neighbors

Category: Similarity.
Aliases observed: Adamic-Adar.
Evidence count: 1; sample A08-O0553.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: sorted adjacency, Roaring sets for high-degree nodes,
and candidate heap.
Spill note: blocked pair generation with sorted intersection
runs.
Hybrid note: exact top hubs resident, cold candidates pruned by
sketches.
State note: candidate pairs + intersection counters + top-k
buffers.
Oracle note: symmetry, bounded pair counts, and brute-force
intersections on samples.
Use note: similarity, recommendations, triangles, duplicate
detection, and LCC.
Reject note: reject all-pairs exact output when output volume
exceeds stream capacity.

#### ALG-002-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0553

#### ALG-002-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0553

#### ALG-002-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0553

### ALG-003: Afforest sampled components

Category: Components.
Aliases observed: Afforest.
Evidence count: 14; sample A08-O0197, A08-O0200.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-003-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0197, A08-O0200

#### ALG-003-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0197, A08-O0200

#### ALG-003-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0197, A08-O0200

### ALG-004: ARIES recovery

Category: Storage.
Aliases observed: ARIES.
Evidence count: 6; sample A07-O0535, A07-O0957.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: resident mutable buffer, compact immutable runs, and
metadata cache.
Spill note: tiered/leveled on-disk runs with bloom/filter gates.
Hybrid note: hot mutable tier in RAM with deterministic
background merge budget.
State note: memtable/run metadata + merge cursors + recovery
markers.
Oracle note: write/read history replay, crash injection, and
checksum validation.
Use note: bounded ingest, snapshot reads, and storage shaped to
analytics.
Reject note: reject if write amplification or recovery window
breaches the SLO.

#### ALG-004-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0535, A07-O0957

#### ALG-004-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0535, A07-O0957

#### ALG-004-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0535, A07-O0957

### ALG-005: Bellman-Ford relaxation

Category: Traversal.
Aliases observed: Bellman-Ford.
Evidence count: 5; sample A07-O0172, A08-O0495.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: weighted CSR, distance array, predecessor log, and
bucket or heap state.
Spill note: partitioned relax logs with bucket spill files and
checkpointed distances.
Hybrid note: resident active buckets with compressed cold
distances and page-cache hints.
State note: distance + predecessor + relaxation worklist.
Oracle note: differential against reference SSSP plus triangle-
inequality checks.
Use note: weighted impact paths, routing, attack paths, and
dependency distances.
Reject note: reject negative-weight inputs for plans that
require monotone relaxations.

#### ALG-005-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0172, A08-O0495

#### ALG-005-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0172, A08-O0495

#### ALG-005-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0172, A08-O0495

### ALG-006: Betweenness centrality

Category: Centrality.
Aliases observed: Betweenness, betweenness.
Evidence count: 18; sample A07-O0016, A07-O0020.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR, source frontier state, dependency accumulators,
and path counts.
Spill note: source batching with checkpointed dependency vectors
and edge tiles.
Hybrid note: landmark exact cores plus sampled periphery
correction.
State note: per-source frontier + sigma/path counts + dependency
scores.
Oracle note: small-graph exhaustive reference and rank-stability
metamorphics.
Use note: bottleneck discovery, social/knowledge central nodes,
and security chokepoints.
Reject note: reject exact all-source mode when n active states
cannot fit or spill in SLA.

#### ALG-006-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0016, A07-O0020

#### ALG-006-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0016, A07-O0020

#### ALG-006-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0016, A07-O0020

### ALG-007: BFS frontier traversal

Category: Traversal.
Aliases observed: BFS, bfs.
Evidence count: 102; sample A07-O0175, A07-O0176.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR plus CSC, frontier bitmap, visited bitset, and
streaming output.
Spill note: level files over edge tiles with one resident
frontier window.
Hybrid note: hot-degree vertex cache plus cold adjacency page
streamer.
State note: frontier + visited + optional predecessor state.
Oracle note: compare level sets, path existence, and predecessor
checksums.
Use note: bounded local navigation, blast-radius queries, and
reachability jobs.
Reject note: reject when output cardinality alone exceeds the
requested RAM and no streaming sink is allowed.

#### ALG-007-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0175, A07-O0176

#### ALG-007-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0175, A07-O0176

#### ALG-007-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0175, A07-O0176

### ALG-008: Bloom and Ribbon filters

Category: Storage.
Aliases observed: BLOOM, Bloom, Ribbon filter, bloom.
Evidence count: 94; sample A07-O0014, A07-O0241.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: blocked bit arrays, hash seeds, and optional exact
backing set.
Spill note: filter blocks colocated with cold runs and resident
metadata.
Hybrid note: resident negative filter plus exact cold
verification on hits.
State note: bit arrays + hash parameters + false-positive
budget.
Oracle note: known-present set, known-absent set, and measured
false-positive rate.
Use note: avoiding cold reads and bounding random I/O in
graph/text stores.
Reject note: reject when false positives are unacceptable
without exact fallback.

#### ALG-008-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0014, A07-O0241

#### ALG-008-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0014, A07-O0241

#### ALG-008-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0014, A07-O0241

### ALG-009: BM25 scoring

Category: Text retrieval.
Aliases observed: BM25, bm25.
Evidence count: 67; sample A07-O0083, A07-O0085.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: FST dictionary, postings, term stats, and top-k
scoring heap.
Spill note: posting blocks with skip data and bounded scorer
windows.
Hybrid note: hot terms and block headers resident; cold blocks
streamed.
State note: term cursors + accumulator/top-k heap + scorer
statistics.
Oracle note: query-by-query diff versus exhaustive scorer and
score monotonicity.
Use note: search, hybrid retrieval, and graph neighborhood text
joins.
Reject note: reject when analyzers/tokenization differ from the
declared query surface.

#### ALG-009-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0083, A07-O0085

#### ALG-009-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0083, A07-O0085

#### ALG-009-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0083, A07-O0085

### ALG-010: Centrality family

Category: Centrality.
Aliases observed: centrality.
Evidence count: 24; sample A07-O0015, A07-O0030.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: pull CSR/CSC, rank vector, residual vector, and
dangling-node accumulator.
Spill note: edge-tile pull sweeps with rank vector pages and
residual checkpoints.
Hybrid note: hot ranks resident; cold ranks quantized with exact
correction sweeps.
State note: current vector + next vector + convergence residual.
Oracle note: residual monotonicity, mass conservation, and
reference-vector delta.
Use note: ranking, influence, recommendations, and daily scored
graph materializations.
Reject note: reject when requested epsilon needs more passes
than the SLA permits.

#### ALG-010-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0015, A07-O0030

#### ALG-010-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0015, A07-O0030

#### ALG-010-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0015, A07-O0030

### ALG-011: Closeness centrality

Category: Centrality.
Aliases observed: closeness.
Evidence count: 1; sample A08-O0541.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR, source frontier state, dependency accumulators,
and path counts.
Spill note: source batching with checkpointed dependency vectors
and edge tiles.
Hybrid note: landmark exact cores plus sampled periphery
correction.
State note: per-source frontier + sigma/path counts + dependency
scores.
Oracle note: small-graph exhaustive reference and rank-stability
metamorphics.
Use note: bottleneck discovery, social/knowledge central nodes,
and security chokepoints.
Reject note: reject exact all-source mode when n active states
cannot fit or spill in SLA.

#### ALG-011-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0541

#### ALG-011-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0541

#### ALG-011-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0541

### ALG-012: Community detection family

Category: Community.
Aliases observed: community detection, community modules,
modularity.
Evidence count: 16; sample A07-O0017, A07-O0028.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR, community id array, modularity deltas, and
neighbor-community map.
Spill note: chunked node move passes with sorted community-delta
runs.
Hybrid note: resident coarse graph with spilled fine-node
refinement.
State note: community ids + community weights + local move
scratch.
Oracle note: modularity non-regression and deterministic replay
receipts.
Use note: segmentation, fraud rings, code modules, and PMF user
clusters.
Reject note: reject claims of global optimum; expose it as
heuristic optimization.

#### ALG-012-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0017, A07-O0028

#### ALG-012-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0017, A07-O0028

#### ALG-012-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0017, A07-O0028

### ALG-013: Weakly connected components

Category: Components.
Aliases observed: WCC, component labels, connected components,
wcc.
Evidence count: 90; sample A07-O0024, A07-O0045.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-013-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0024, A07-O0045

#### ALG-013-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0024, A07-O0045

#### ALG-013-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0024, A07-O0045

### ALG-014: Copy-on-write tree snapshots

Category: Storage.
Aliases observed: COPY-ON-WRITE, COW, copy-on-write, cow, path-
copy, root flip.
Evidence count: 60; sample A07-O0008, A07-O0534.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: resident mutable buffer, compact immutable runs, and
metadata cache.
Spill note: tiered/leveled on-disk runs with bloom/filter gates.
Hybrid note: hot mutable tier in RAM with deterministic
background merge budget.
State note: memtable/run metadata + merge cursors + recovery
markers.
Oracle note: write/read history replay, crash injection, and
checksum validation.
Use note: bounded ingest, snapshot reads, and storage shaped to
analytics.
Reject note: reject if write amplification or recovery window
breaches the SLO.

#### ALG-014-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0008, A07-O0534

#### ALG-014-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0008, A07-O0534

#### ALG-014-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0008, A07-O0534

### ALG-015: CSR build and adjacency layout

Category: Graph storage.
Aliases observed: CSC, CSR, csr.
Evidence count: 206; sample A07-O0002, A07-O0195.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: counts, prefix sums, edge array, and optional CSC
mirror.
Spill note: external sort/count-scatter by source shard.
Hybrid note: resident offsets with cold edge payload pages.
State note: degree counts + prefix offsets + scatter cursor.
Oracle note: edge multiset equality and offset monotonicity
validation.
Use note: turning OLTP edges into algorithm-shaped read
surfaces.
Reject note: reject if mutation rate prevents stable snapshot
construction.

#### ALG-015-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0002, A07-O0195

#### ALG-015-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0002, A07-O0195

#### ALG-015-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0002, A07-O0195

### ALG-016: Degree centrality

Category: Centrality.
Aliases observed: degree.
Evidence count: 14; sample A07-O0627, A07-O0642.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: pull CSR/CSC, rank vector, residual vector, and
dangling-node accumulator.
Spill note: edge-tile pull sweeps with rank vector pages and
residual checkpoints.
Hybrid note: hot ranks resident; cold ranks quantized with exact
correction sweeps.
State note: current vector + next vector + convergence residual.
Oracle note: residual monotonicity, mass conservation, and
reference-vector delta.
Use note: ranking, influence, recommendations, and daily scored
graph materializations.
Reject note: reject when requested epsilon needs more passes
than the SLA permits.

#### ALG-016-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0627, A07-O0642

#### ALG-016-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0627, A07-O0642

#### ALG-016-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0627, A07-O0642

### ALG-017: Delta and RLE compression

Category: Storage.
Aliases observed: Delta compression, byteRLE, delta compression.
Evidence count: 10; sample A08-O0362, A08-O0363.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: delta-coded blocks, RLE spans, and SIMD decode
scratch.
Spill note: block dictionary plus streaming decode windows.
Hybrid note: hot decoded headers resident, payload blocks
compressed cold.
State note: decode scratch + block cursors + checksum state.
Oracle note: round-trip byte equality and adversarial high-
entropy blocks.
Use note: shrinking topology, postings, vectors, and
intermediate runs.
Reject note: reject when decode CPU exceeds the latency budget.

#### ALG-017-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0362, A08-O0363

#### ALG-017-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0362, A08-O0363

#### ALG-017-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0362, A08-O0363

### ALG-018: Delta-stepping SSSP

Category: Traversal.
Aliases observed: Delta Stepping, Delta-Stepping, Delta-
stepping, delta bucket, delta-stepping.
Evidence count: 34; sample A07-O0167, A07-O0168.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: weighted CSR, distance array, predecessor log, and
bucket or heap state.
Spill note: partitioned relax logs with bucket spill files and
checkpointed distances.
Hybrid note: resident active buckets with compressed cold
distances and page-cache hints.
State note: distance + predecessor + relaxation worklist.
Oracle note: differential against reference SSSP plus triangle-
inequality checks.
Use note: weighted impact paths, routing, attack paths, and
dependency distances.
Reject note: reject negative-weight inputs for plans that
require monotone relaxations.

#### ALG-018-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0167, A07-O0168

#### ALG-018-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0167, A07-O0168

#### ALG-018-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0167, A07-O0168

### ALG-019: DFS depth traversal

Category: Traversal.
Aliases observed: DFS.
Evidence count: 2; sample A07-O0316, A08-O0545.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR plus CSC, frontier bitmap, visited bitset, and
streaming output.
Spill note: level files over edge tiles with one resident
frontier window.
Hybrid note: hot-degree vertex cache plus cold adjacency page
streamer.
State note: frontier + visited + optional predecessor state.
Oracle note: compare level sets, path existence, and predecessor
checksums.
Use note: bounded local navigation, blast-radius queries, and
reachability jobs.
Reject note: reject when output cardinality alone exceeds the
requested RAM and no streaming sink is allowed.

#### ALG-019-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0316, A08-O0545

#### ALG-019-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0316, A08-O0545

#### ALG-019-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0316, A08-O0545

### ALG-020: Dijkstra weighted shortest path

Category: Traversal.
Aliases observed: Dijkstra.
Evidence count: 11; sample A07-O0170, A07-O0171.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: weighted CSR, distance array, predecessor log, and
bucket or heap state.
Spill note: partitioned relax logs with bucket spill files and
checkpointed distances.
Hybrid note: resident active buckets with compressed cold
distances and page-cache hints.
State note: distance + predecessor + relaxation worklist.
Oracle note: differential against reference SSSP plus triangle-
inequality checks.
Use note: weighted impact paths, routing, attack paths, and
dependency distances.
Reject note: reject negative-weight inputs for plans that
require monotone relaxations.

#### ALG-020-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0170, A07-O0171

#### ALG-020-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0170, A07-O0171

#### ALG-020-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0170, A07-O0171

### ALG-021: Eigenvector, Katz, HITS, ArticleRank

Category: Centrality.
Aliases observed: ArticleRank, HITS, Katz, eigenvector, hits.
Evidence count: 11; sample A07-O0824, A08-O0337.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: pull CSR/CSC, rank vector, residual vector, and
dangling-node accumulator.
Spill note: edge-tile pull sweeps with rank vector pages and
residual checkpoints.
Hybrid note: hot ranks resident; cold ranks quantized with exact
correction sweeps.
State note: current vector + next vector + convergence residual.
Oracle note: residual monotonicity, mass conservation, and
reference-vector delta.
Use note: ranking, influence, recommendations, and daily scored
graph materializations.
Reject note: reject when requested epsilon needs more passes
than the SLA permits.

#### ALG-021-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0824, A08-O0337

#### ALG-021-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0824, A08-O0337

#### ALG-021-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0824, A08-O0337

### ALG-022: FastRP random projection embeddings

Category: Embeddings.
Aliases observed: FastRP.
Evidence count: 6; sample A08-O0555, A09-O0015.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: compressed feature matrix, CSR sampler, and
deterministic RNG streams.
Spill note: feature slabs and neighbor-sample batches with
checkpointed vectors.
Hybrid note: resident low-dimensional projection with spilled
refinement slabs.
State note: feature vectors + sampled neighbor frontier +
optimizer scratch.
Oracle note: seed replay, norm bounds, recall/loss validation,
and drift checks.
Use note: local intelligence, recommendation features, and
downstream ANN indexing.
Reject note: reject when the caller demands exact graph-
theoretic semantics.

#### ALG-022-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0555, A09-O0015

#### ALG-022-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0555, A09-O0015

#### ALG-022-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0555, A09-O0015

### ALG-023: FastSV star contraction

Category: Components.
Aliases observed: FastSV, large-star.
Evidence count: 19; sample A08-O0206, A08-O0212.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-023-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0206, A08-O0212

#### ALG-023-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0206, A08-O0212

#### ALG-023-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0206, A08-O0212

### ALG-024: Frontier push/pull switching

Category: Traversal.
Aliases observed: Direction-optim, direction-optim, push-pull,
push/pull.
Evidence count: 33; sample A07-O0004, A07-O0202.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR plus CSC, frontier bitmap, visited bitset, and
streaming output.
Spill note: level files over edge tiles with one resident
frontier window.
Hybrid note: hot-degree vertex cache plus cold adjacency page
streamer.
State note: frontier + visited + optional predecessor state.
Oracle note: compare level sets, path existence, and predecessor
checksums.
Use note: bounded local navigation, blast-radius queries, and
reachability jobs.
Reject note: reject when output cardinality alone exceeds the
requested RAM and no streaming sink is allowed.

#### ALG-024-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0004, A07-O0202

#### ALG-024-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0004, A07-O0202

#### ALG-024-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0004, A07-O0202

### ALG-025: FST term dictionary

Category: Text storage.
Aliases observed: FST, Term dictionary, finite-state transducer,
fst, term DICTIONARY, term dictionary.
Evidence count: 99; sample A07-O0226, A07-O0227.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: minimal FST/trie, output arcs, and automaton cursor
state.
Spill note: paged FST arcs with resident root fanout and term-
range cache.
Hybrid note: hot prefixes resident, cold suffix arcs paged by
automaton.
State note: automaton cursor + arc page cache + output
accumulator.
Oracle note: term corpus diff, lexicographic order, and
automaton intersection tests.
Use note: term lookup, prefix/range scans, and typo/fuzzy
candidate generation.
Reject note: reject if tokenizer or collation is not frozen in
the receipt.

#### ALG-025-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0226, A07-O0227

#### ALG-025-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0226, A07-O0227

#### ALG-025-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0226, A07-O0227

### ALG-026: Graph embedding family

Category: Embeddings.
Aliases observed: Embeddings, embedding, embeddings.
Evidence count: 17; sample A07-O0592, A07-O0607.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: compressed feature matrix, CSR sampler, and
deterministic RNG streams.
Spill note: feature slabs and neighbor-sample batches with
checkpointed vectors.
Hybrid note: resident low-dimensional projection with spilled
refinement slabs.
State note: feature vectors + sampled neighbor frontier +
optimizer scratch.
Oracle note: seed replay, norm bounds, recall/loss validation,
and drift checks.
Use note: local intelligence, recommendation features, and
downstream ANN indexing.
Reject note: reject when the caller demands exact graph-
theoretic semantics.

#### ALG-026-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0592, A07-O0607

#### ALG-026-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0592, A07-O0607

#### ALG-026-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0592, A07-O0607

### ALG-027: Graph partitioning and cuts

Category: Out-of-core.
Aliases observed: 2D partition, Graph Partition, METIS, Vertex-
cut, edge-cut, vertex-cut.
Evidence count: 29; sample A07-O0150, A07-O0351.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: partition map, boundary directory, and per-partition
summaries.
Spill note: edge-cut or vertex-cut shards with boundary merge
runs.
Hybrid note: hot boundary vertices resident and cold partitions
streamed.
State note: partition ids + boundary state + merge scratch.
Oracle note: edge conservation, boundary consistency, and cut-
size receipts.
Use note: controlling memory, parallelism, and locality before
algorithm execution.
Reject note: reject if skew makes one partition exceed the
declared RAM ceiling.

#### ALG-027-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0150, A07-O0351

#### ALG-027-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0150, A07-O0351

#### ALG-027-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0150, A07-O0351

### ALG-028: GraphSAGE neighbor sampling

Category: Embeddings.
Aliases observed: GraphSAGE.
Evidence count: 2; sample A08-O0559, A09-O0926.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: compressed feature matrix, CSR sampler, and
deterministic RNG streams.
Spill note: feature slabs and neighbor-sample batches with
checkpointed vectors.
Hybrid note: resident low-dimensional projection with spilled
refinement slabs.
State note: feature vectors + sampled neighbor frontier +
optimizer scratch.
Oracle note: seed replay, norm bounds, recall/loss validation,
and drift checks.
Use note: local intelligence, recommendation features, and
downstream ANN indexing.
Reject note: reject when the caller demands exact graph-
theoretic semantics.

#### ALG-028-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0559, A09-O0926

#### ALG-028-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0559, A09-O0926

#### ALG-028-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0559, A09-O0926

### ALG-029: HashGNN and GNN message passing

Category: Embeddings.
Aliases observed: GNN, HashGNN, message passing.
Evidence count: 12; sample A08-O0316, A08-O0560.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: compressed feature matrix, CSR sampler, and
deterministic RNG streams.
Spill note: feature slabs and neighbor-sample batches with
checkpointed vectors.
Hybrid note: resident low-dimensional projection with spilled
refinement slabs.
State note: feature vectors + sampled neighbor frontier +
optimizer scratch.
Oracle note: seed replay, norm bounds, recall/loss validation,
and drift checks.
Use note: local intelligence, recommendation features, and
downstream ANN indexing.
Reject note: reject when the caller demands exact graph-
theoretic semantics.

#### ALG-029-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0316, A08-O0560

#### ALG-029-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0316, A08-O0560

#### ALG-029-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0316, A08-O0560

### ALG-030: HNSW layered greedy ANN

Category: Vector ANN.
Aliases observed: HNSW, hnsw.
Evidence count: 111; sample A07-O0001, A07-O0101.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: neighbor graph, vector codes, entry layers, visited
set, and beam heap.
Spill note: sector-aligned graph pages with bounded beam and
async prefetch.
Hybrid note: top layers and hot clusters resident; base layer
disk-backed.
State note: beam heap + visited set + candidate vector cache.
Oracle note: recall@k against brute-force sample and latency
distribution checks.
Use note: semantic search, embedding lookup, and hybrid
graph+vector workloads.
Reject note: reject when required recall cannot be met inside
beam/RAM limits.

#### ALG-030-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0001, A07-O0101

#### ALG-030-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0001, A07-O0101

#### ALG-030-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0001, A07-O0101

### ALG-031: Hooking and shortcutting

Category: Components.
Aliases observed: HOOK, Hooking, SHORTCUT, Shortcut, hook,
hooking, pointer jump, shortcut.
Evidence count: 91; sample A07-O0013, A07-O0184.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-031-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0013, A07-O0184

#### ALG-031-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0013, A07-O0184

#### ALG-031-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0013, A07-O0184

### ALG-032: Immutable base plus delta rebuild

Category: Graph storage.
Aliases observed: delta log, immutable CSR.
Evidence count: 9; sample A08-O0217, A08-O0382.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: immutable base CSR plus resident delta overlay and
rebuild marker.
Spill note: delta segments and periodic external rebuild runs.
Hybrid note: hot delta in RAM, cold deltas compacted into base
pages.
State note: base offsets + delta cursors + rebuild scratch.
Oracle note: base-plus-delta equals rebuilt CSR on deterministic
samples.
Use note: nightly graph snapshots with bounded update memory.
Reject note: reject if delta grows past the estimator's rebuild
threshold.

#### ALG-032-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0217, A08-O0382

#### ALG-032-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0217, A08-O0382

#### ALG-032-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0217, A08-O0382

### ALG-033: Incremental delta iteration

Category: Incremental.
Aliases observed: Differential dataflow, Incremental Delta,
Incremental delta, Semi-naive, differential dataflow,
differential frontiers, semi-naive, signed delta.
Evidence count: 21; sample A07-O0141, A08-O0026.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: arranged traces, delta batches, frontier summaries,
and compact state.
Spill note: trace compaction tiers with resident frontier and
active keys.
Hybrid note: hot keys resident; cold arrangements page by key-
range.
State note: delta trace + frontier + consolidated arrangement.
Oracle note: incremental output equals from-scratch
recomputation.
Use note: standing graph views and daily changes where recompute
is wasteful.
Reject note: reject when update disorder exceeds compaction and
correction budget.

#### ALG-033-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0141, A08-O0026

#### ALG-033-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0141, A08-O0026

#### ALG-033-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0141, A08-O0026

### ALG-034: IVF partitioned probe

Category: Vector ANN.
Aliases observed: IVF, NPROBE, ivf, nprobe.
Evidence count: 112; sample A07-O0126, A07-O0129.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: coarse partitions, centroid table, compact vector
codes, and top-k heap.
Spill note: probe-limited partition pages with exact candidate
rescore.
Hybrid note: hot centroids resident, cold postings and vectors
paged by probe.
State note: probe list + candidate heap + vector-code scratch.
Oracle note: recall@k versus brute force and monotone recall as
probes increase.
Use note: bounded-latency vector search with explicit recall/RAM
tradeoffs.
Reject note: reject when data distribution makes recall unstable
at allowed probes.

#### ALG-034-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0126, A07-O0129

#### ALG-034-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0126, A07-O0129

#### ALG-034-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0126, A07-O0129

### ALG-035: Jaccard, overlap, and cosine similarity

Category: Similarity.
Aliases observed: Jaccard, cosine, overlap.
Evidence count: 53; sample A07-O0134, A07-O0193.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: sorted adjacency, Roaring sets for high-degree nodes,
and candidate heap.
Spill note: blocked pair generation with sorted intersection
runs.
Hybrid note: exact top hubs resident, cold candidates pruned by
sketches.
State note: candidate pairs + intersection counters + top-k
buffers.
Oracle note: symmetry, bounded pair counts, and brute-force
intersections on samples.
Use note: similarity, recommendations, triangles, duplicate
detection, and LCC.
Reject note: reject all-pairs exact output when output volume
exceeds stream capacity.

#### ALG-035-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0134, A07-O0193

#### ALG-035-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0134, A07-O0193

#### ALG-035-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0134, A07-O0193

### ALG-036: k-core peeling

Category: Density.
Aliases observed: k-core.
Evidence count: 15; sample A07-O0019, A07-O0022.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR, degree counter array, and peel queue.
Spill note: degree buckets on disk with resident active bucket
and edge-tile scans.
Hybrid note: hot high-degree core resident, low-degree shells
streamed.
State note: degree counters + active peel queue + shell labels.
Oracle note: every retained k-core node has degree >= k inside
the induced subgraph.
Use note: dense-subgraph pruning, risk-core discovery, and
graph-size reduction.
Reject note: reject when dynamic updates demand fully online
maintenance in same run.

#### ALG-036-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0019, A07-O0022

#### ALG-036-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0019, A07-O0022

#### ALG-036-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0019, A07-O0022

### ALG-037: KD-tree search

Category: Vector ANN.
Aliases observed: KD-tree.
Evidence count: 1; sample A08-O0583.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: coarse partitions, centroid table, compact vector
codes, and top-k heap.
Spill note: probe-limited partition pages with exact candidate
rescore.
Hybrid note: hot centroids resident, cold postings and vectors
paged by probe.
State note: probe list + candidate heap + vector-code scratch.
Oracle note: recall@k versus brute force and monotone recall as
probes increase.
Use note: bounded-latency vector search with explicit recall/RAM
tradeoffs.
Reject note: reject when data distribution makes recall unstable
at allowed probes.

#### ALG-037-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0583

#### ALG-037-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0583

#### ALG-037-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0583

### ALG-038: k-nearest-neighbor search

Category: Similarity.
Aliases observed: KNN, k nearest, kNN.
Evidence count: 11; sample A08-O0554, A08-O0754.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: neighbor graph, vector codes, entry layers, visited
set, and beam heap.
Spill note: sector-aligned graph pages with bounded beam and
async prefetch.
Hybrid note: top layers and hot clusters resident; base layer
disk-backed.
State note: beam heap + visited set + candidate vector cache.
Oracle note: recall@k against brute-force sample and latency
distribution checks.
Use note: semantic search, embedding lookup, and hybrid
graph+vector workloads.
Reject note: reject when required recall cannot be met inside
beam/RAM limits.

#### ALG-038-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0554, A08-O0754

#### ALG-038-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0554, A08-O0754

#### ALG-038-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0554, A08-O0754

### ALG-039: Label propagation

Category: Components.
Aliases observed: LabelPropagation, label propagation.
Evidence count: 9; sample A08-O0209, A08-O0534.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-039-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0209, A08-O0534

#### ALG-039-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0209, A08-O0534

#### ALG-039-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0209, A08-O0534

### ALG-040: Leiden refinement

Category: Community.
Aliases observed: Leiden.
Evidence count: 8; sample A07-O0012, A08-O0537.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR, community id array, modularity deltas, and
neighbor-community map.
Spill note: chunked node move passes with sorted community-delta
runs.
Hybrid note: resident coarse graph with spilled fine-node
refinement.
State note: community ids + community weights + local move
scratch.
Oracle note: modularity non-regression and deterministic replay
receipts.
Use note: segmentation, fraud rings, code modules, and PMF user
clusters.
Reject note: reject claims of global optimum; expose it as
heuristic optimization.

#### ALG-040-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0012, A08-O0537

#### ALG-040-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0012, A08-O0537

#### ALG-040-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0012, A08-O0537

### ALG-041: Levenshtein automata

Category: Text retrieval.
Aliases observed: Levenshtein.
Evidence count: 8; sample A07-O0232, A07-O0244.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: FST dictionary, postings, term stats, and top-k
scoring heap.
Spill note: posting blocks with skip data and bounded scorer
windows.
Hybrid note: hot terms and block headers resident; cold blocks
streamed.
State note: term cursors + accumulator/top-k heap + scorer
statistics.
Oracle note: query-by-query diff versus exhaustive scorer and
score monotonicity.
Use note: search, hybrid retrieval, and graph neighborhood text
joins.
Reject note: reject when analyzers/tokenization differ from the
declared query surface.

#### ALG-041-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0232, A07-O0244

#### ALG-041-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0232, A07-O0244

#### ALG-041-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0232, A07-O0244

### ALG-042: Louvain modularity

Category: Community.
Aliases observed: Louvain.
Evidence count: 26; sample A07-O0011, A07-O0021.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR, community id array, modularity deltas, and
neighbor-community map.
Spill note: chunked node move passes with sorted community-delta
runs.
Hybrid note: resident coarse graph with spilled fine-node
refinement.
State note: community ids + community weights + local move
scratch.
Oracle note: modularity non-regression and deterministic replay
receipts.
Use note: segmentation, fraud rings, code modules, and PMF user
clusters.
Reject note: reject claims of global optimum; expose it as
heuristic optimization.

#### ALG-042-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0011, A07-O0021

#### ALG-042-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0011, A07-O0021

#### ALG-042-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0011, A07-O0021

### ALG-043: Locality-sensitive hashing

Category: Vector ANN.
Aliases observed: LSH.
Evidence count: 1; sample A08-O0582.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: coarse partitions, centroid table, compact vector
codes, and top-k heap.
Spill note: probe-limited partition pages with exact candidate
rescore.
Hybrid note: hot centroids resident, cold postings and vectors
paged by probe.
State note: probe list + candidate heap + vector-code scratch.
Oracle note: recall@k versus brute force and monotone recall as
probes increase.
Use note: bounded-latency vector search with explicit recall/RAM
tradeoffs.
Reject note: reject when data distribution makes recall unstable
at allowed probes.

#### ALG-043-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0582

#### ALG-043-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0582

#### ALG-043-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0582

### ALG-044: LSM compaction

Category: Storage.
Aliases observed: Compaction, LSM, SSTable, TieredMergePolicy,
compaction, lsm, sstable.
Evidence count: 241; sample A07-O0003, A07-O0161.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: resident mutable buffer, compact immutable runs, and
metadata cache.
Spill note: tiered/leveled on-disk runs with bloom/filter gates.
Hybrid note: hot mutable tier in RAM with deterministic
background merge budget.
State note: memtable/run metadata + merge cursors + recovery
markers.
Oracle note: write/read history replay, crash injection, and
checksum validation.
Use note: bounded ingest, snapshot reads, and storage shaped to
analytics.
Reject note: reject if write amplification or recovery window
breaches the SLO.

#### ALG-044-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0003, A07-O0161

#### ALG-044-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0003, A07-O0161

#### ALG-044-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0003, A07-O0161

### ALG-045: node2vec random-walk embeddings

Category: Embeddings.
Aliases observed: node2vec.
Evidence count: 3; sample A08-O0558, A09-O0921.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: compressed feature matrix, CSR sampler, and
deterministic RNG streams.
Spill note: feature slabs and neighbor-sample batches with
checkpointed vectors.
Hybrid note: resident low-dimensional projection with spilled
refinement slabs.
State note: feature vectors + sampled neighbor frontier +
optimizer scratch.
Oracle note: seed replay, norm bounds, recall/loss validation,
and drift checks.
Use note: local intelligence, recommendation features, and
downstream ANN indexing.
Reject note: reject when the caller demands exact graph-
theoretic semantics.

#### ALG-045-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0558, A09-O0921

#### ALG-045-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0558, A09-O0921

#### ALG-045-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0558, A09-O0921

### ALG-046: NodeSimilarity

Category: Similarity.
Aliases observed: NodeSimilarity, node similarity.
Evidence count: 7; sample A08-O0040, A08-O0059.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: sorted adjacency, Roaring sets for high-degree nodes,
and candidate heap.
Spill note: blocked pair generation with sorted intersection
runs.
Hybrid note: exact top hubs resident, cold candidates pruned by
sketches.
State note: candidate pairs + intersection counters + top-k
buffers.
Oracle note: symmetry, bounded pair counts, and brute-force
intersections on samples.
Use note: similarity, recommendations, triangles, duplicate
detection, and LCC.
Reject note: reject all-pairs exact output when output volume
exceeds stream capacity.

#### ALG-046-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0040, A08-O0059

#### ALG-046-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0040, A08-O0059

#### ALG-046-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0040, A08-O0059

### ALG-047: Ordered search and merge

Category: Storage.
Aliases observed: binary search, merge-sort, sorted run, sorted
runs.
Evidence count: 23; sample A08-O0150, A08-O0160.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: sorted-run metadata, k-way cursor heap, and output
buffer.
Spill note: external merge passes with bounded fan-in.
Hybrid note: resident small runs and streamed large runs.
State note: run cursors + loser tree/heap + output buffer.
Oracle note: sortedness, count conservation, and duplicate-
resolution checks.
Use note: compaction, posting merges, dictionary merges, and
spill materialization.
Reject note: reject if fan-in creates too many random reads for
the storage device.

#### ALG-047-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0150, A08-O0160

#### ALG-047-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0150, A08-O0160

#### ALG-047-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0150, A08-O0160

### ALG-048: Out-of-core graph processing

Category: Out-of-core.
Aliases observed: GraphChi, Out-of-core, X-Stream, out-of-core,
x stream.
Evidence count: 21; sample A07-O0151, A07-O0154.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: small partition directory, window cache, and
sequential edge blocks.
Spill note: GraphChi/GridGraph/X-Stream style edge windows on
disk.
Hybrid note: hot cut vertices resident and edge partitions
streamed.
State note: partition window + vertex state slab + edge block
cursor.
Oracle note: partitioned run equals in-memory reference on small
graphs.
Use note: terabyte-scale graphs under hard RAM ceilings.
Reject note: reject if random I/O replaces the intended
sequential window pattern.

#### ALG-048-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0151, A07-O0154

#### ALG-048-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0151, A07-O0154

#### ALG-048-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0151, A07-O0154

### ALG-049: PageRank power iteration

Category: Centrality.
Aliases observed: PageRank, pageRank, pagerank.
Evidence count: 92; sample A07-O0027, A07-O0033.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: pull CSR/CSC, rank vector, residual vector, and
dangling-node accumulator.
Spill note: edge-tile pull sweeps with rank vector pages and
residual checkpoints.
Hybrid note: hot ranks resident; cold ranks quantized with exact
correction sweeps.
State note: current vector + next vector + convergence residual.
Oracle note: residual monotonicity, mass conservation, and
reference-vector delta.
Use note: ranking, influence, recommendations, and daily scored
graph materializations.
Reject note: reject when requested epsilon needs more passes
than the SLA permits.

#### ALG-049-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0027, A07-O0033

#### ALG-049-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0027, A07-O0033

#### ALG-049-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0027, A07-O0033

### ALG-050: Posting block compression

Category: Text storage.
Aliases observed: Posting, Skip data, docID, posting, skip data,
skip list.
Evidence count: 139; sample A07-O0005, A07-O0089.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: delta-coded posting blocks, skip tables, and term
statistics.
Spill note: streamed posting blocks with resident block headers
only.
Hybrid note: hot docid ranges resident and cold ranges page-
aligned.
State note: block cursors + decode buffer + accumulator heap.
Oracle note: round-trip encode/decode and exhaustive posting
traversal.
Use note: large text adjacency and property-search joins with
bounded RAM.
Reject note: reject if updates require in-place mutation of
compressed blocks.

#### ALG-050-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0005, A07-O0089

#### ALG-050-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0005, A07-O0089

#### ALG-050-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0005, A07-O0089

### ALG-051: Random projection trees

Category: Vector ANN.
Aliases observed: random projection trees.
Evidence count: 1; sample A08-O0584.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: coarse partitions, centroid table, compact vector
codes, and top-k heap.
Spill note: probe-limited partition pages with exact candidate
rescore.
Hybrid note: hot centroids resident, cold postings and vectors
paged by probe.
State note: probe list + candidate heap + vector-code scratch.
Oracle note: recall@k versus brute force and monotone recall as
probes increase.
Use note: bounded-latency vector search with explicit recall/RAM
tradeoffs.
Reject note: reject when data distribution makes recall unstable
at allowed probes.

#### ALG-051-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0584

#### ALG-051-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0584

#### ALG-051-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0584

### ALG-052: Reciprocal rank and score fusion

Category: Text retrieval.
Aliases observed: RRF.
Evidence count: 6; sample A07-O0117, A07-O0303.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: bounded per-source top-k lists, score normalizers, and
merge heap.
Spill note: sorted partial rankings with external merge and
bounded heap.
Hybrid note: resident high-confidence heads plus streamed tails.
State note: per-source cursors + fusion accumulator + output
heap.
Oracle note: associativity/replay checks and exhaustive merge
comparison.
Use note: hybrid text+vector+graph results with bounded fan-in.
Reject note: reject if source rankings have incomparable
freshness or semantics.

#### ALG-052-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0117, A07-O0303

#### ALG-052-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0117, A07-O0303

#### ALG-052-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0117, A07-O0303

### ALG-053: Reachability and reverse reach

Category: Traversal.
Aliases observed: REACHABILITY, forward reach, reachability,
reverse reach.
Evidence count: 48; sample A07-O0023, A07-O0025.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR plus CSC, frontier bitmap, visited bitset, and
streaming output.
Spill note: level files over edge tiles with one resident
frontier window.
Hybrid note: hot-degree vertex cache plus cold adjacency page
streamer.
State note: frontier + visited + optional predecessor state.
Oracle note: compare level sets, path existence, and predecessor
checksums.
Use note: bounded local navigation, blast-radius queries, and
reachability jobs.
Reject note: reject when output cardinality alone exceeds the
requested RAM and no streaming sink is allowed.

#### ALG-053-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0023, A07-O0025

#### ALG-053-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0023, A07-O0025

#### ALG-053-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0023, A07-O0025

### ALG-054: Record-chain adjacency

Category: Graph storage.
Aliases observed: Record Chain, Record chain, record chains,
record-chain.
Evidence count: 19; sample A07-O0337, A07-O0354.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: append-only record chains with compact pointer/offset
arrays.
Spill note: chain segments sorted by source with bounded
decompression window.
Hybrid note: hot chains compacted into CSR while cold chains
remain logged.
State note: chain cursors + dedupe window + adjacency output
buffer.
Oracle note: adjacency multiset diff and snapshot-version
visibility tests.
Use note: mutable OLTP-shaped graph ingestion before analytical
compaction.
Reject note: reject if traversal requires repeated random chain
chasing under tight SLA.

#### ALG-054-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0337, A07-O0354

#### ALG-054-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0337, A07-O0354

#### ALG-054-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0337, A07-O0354

### ALG-055: Roaring bitmap id sets

Category: Storage.
Aliases observed: BITMAP, Bitmap, Roaring, bitmap, roaring.
Evidence count: 155; sample A07-O0007, A07-O0162.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: Roaring containers, high-key directory, and SIMD set
scratch.
Spill note: container pages with resident directory and bounded
merge window.
Hybrid note: dense containers resident; sparse/cold containers
streamed.
State note: container cursors + operation scratch + output
container window.
Oracle note: set identities, cardinality invariants, and random-
set differential tests.
Use note: candidate pruning, label filters, and high-fanout
intersections.
Reject note: reject if ID remapping is not stable across input
artifacts.

#### ALG-055-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0007, A07-O0162

#### ALG-055-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0007, A07-O0162

#### ALG-055-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0007, A07-O0162

### ALG-056: Strongly connected components

Category: Components.
Aliases observed: SCC.
Evidence count: 8; sample A07-O0038, A07-O0051.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-056-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0038, A07-O0051

#### ALG-056-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0038, A07-O0051

#### ALG-056-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0038, A07-O0051

### ALG-057: Semiring graph traversal

Category: Linear algebra.
Aliases observed: GraphBLAS, MIN_PLUS, Semiring, min_plus,
semiring.
Evidence count: 81; sample A07-O0138, A07-O0152.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR/CSC sparse matrix, value vector, and semiring
operator table.
Spill note: matrix tiles with resident vector slabs and
reduction buffers.
Hybrid note: hot rows/columns resident with cold tiles streamed.
State note: input vector/slab + output vector/slab + reducer
scratch.
Oracle note: GraphBLAS differential tests and semiring identity
checks.
Use note: unifying traversal, PageRank, path products, and
sparse analytics.
Reject note: reject if the operator is non-associative or order-
sensitive.

#### ALG-057-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0138, A07-O0152

#### ALG-057-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0138, A07-O0152

#### ALG-057-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0138, A07-O0152

### ALG-058: Unweighted shortest paths

Category: Traversal.
Aliases observed: path search, shortest path.
Evidence count: 25; sample A07-O0026, A07-O0032.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR plus CSC, frontier bitmap, visited bitset, and
streaming output.
Spill note: level files over edge tiles with one resident
frontier window.
Hybrid note: hot-degree vertex cache plus cold adjacency page
streamer.
State note: frontier + visited + optional predecessor state.
Oracle note: compare level sets, path existence, and predecessor
checksums.
Use note: bounded local navigation, blast-radius queries, and
reachability jobs.
Reject note: reject when output cardinality alone exceeds the
requested RAM and no streaming sink is allowed.

#### ALG-058-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0026, A07-O0032

#### ALG-058-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0026, A07-O0032

#### ALG-058-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0026, A07-O0032

### ALG-059: SpMV and SpGEMM traversal

Category: Linear algebra.
Aliases observed: SpMV, sparse matrix.
Evidence count: 9; sample A08-O0388, A08-O0433.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR/CSC sparse matrix, value vector, and semiring
operator table.
Spill note: matrix tiles with resident vector slabs and
reduction buffers.
Hybrid note: hot rows/columns resident with cold tiles streamed.
State note: input vector/slab + output vector/slab + reducer
scratch.
Oracle note: GraphBLAS differential tests and semiring identity
checks.
Use note: unifying traversal, PageRank, path products, and
sparse analytics.
Reject note: reject if the operator is non-associative or order-
sensitive.

#### ALG-059-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0388, A08-O0433

#### ALG-059-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0388, A08-O0433

#### ALG-059-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0388, A08-O0433

### ALG-060: Superstep and BSP convergence

Category: Incremental.
Aliases observed: BARRIER, BSP, Pregel, Superstep, barrier,
pregel, superstep.
Evidence count: 87; sample A07-O0010, A07-O0214.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: message buffers, vertex state, active bitmap, and
barrier metadata.
Spill note: message runs per superstep with bounded in-flight
window.
Hybrid note: active partition resident, inactive partitions
checkpointed.
State note: vertex state + messages + active set.
Oracle note: superstep replay and convergence checksum per
barrier.
Use note: iterative graph algorithms with clear barrier
semantics.
Reject note: reject if global barriers dominate the latency
target.

#### ALG-060-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0010, A07-O0214

#### ALG-060-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0010, A07-O0214

#### ALG-060-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0010, A07-O0214

### ALG-061: TF-IDF scoring

Category: Text retrieval.
Aliases observed: TF-IDF.
Evidence count: 1; sample A08-O0596.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: FST dictionary, postings, term stats, and top-k
scoring heap.
Spill note: posting blocks with skip data and bounded scorer
windows.
Hybrid note: hot terms and block headers resident; cold blocks
streamed.
State note: term cursors + accumulator/top-k heap + scorer
statistics.
Oracle note: query-by-query diff versus exhaustive scorer and
score monotonicity.
Use note: search, hybrid retrieval, and graph neighborhood text
joins.
Reject note: reject when analyzers/tokenization differ from the
declared query surface.

#### ALG-061-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0596

#### ALG-061-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0596

#### ALG-061-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0596

### ALG-062: Triangle counting

Category: Density.
Aliases observed: Triangles, triangle, triangle counting,
triangles.
Evidence count: 26; sample A07-O0018, A07-O0291.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: sorted adjacency, Roaring sets for high-degree nodes,
and candidate heap.
Spill note: blocked pair generation with sorted intersection
runs.
Hybrid note: exact top hubs resident, cold candidates pruned by
sketches.
State note: candidate pairs + intersection counters + top-k
buffers.
Oracle note: symmetry, bounded pair counts, and brute-force
intersections on samples.
Use note: similarity, recommendations, triangles, duplicate
detection, and LCC.
Reject note: reject all-pairs exact output when output volume
exceeds stream capacity.

#### ALG-062-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0018, A07-O0291

#### ALG-062-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0018, A07-O0291

#### ALG-062-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0018, A07-O0291

### ALG-063: Triple-permutation indexing

Category: Graph storage.
Aliases observed: OSP, POS, SPO, Triple Permutation, Triple
permutation, triple permutations.
Evidence count: 24; sample A07-O0823, A07-O0825.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: SPO/POS/OSP permutations, compressed IDs, and range
cursors.
Spill note: permutation pages with resident fence keys and join
cursors.
Hybrid note: hot predicate/object ranges resident and cold
ranges streamed.
State note: range cursors + join/intersection scratch + output
window.
Oracle note: permutation equivalence and round-trip triple count
checks.
Use note: RDF-like graph slices, semantic joins, and property
graph projections.
Reject note: reject if all permutations cannot be kept mutually
consistent.

#### ALG-063-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0823, A07-O0825

#### ALG-063-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0823, A07-O0825

#### ALG-063-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0823, A07-O0825

### ALG-064: Union-find path compression

Category: Components.
Aliases observed: UnionFind, union-find.
Evidence count: 8; sample A08-O0208, A08-O0211.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: edge CSR, parent array, rank/min-label array, and
change bitmap.
Spill note: partition-local parents with boundary-edge merge
rounds.
Hybrid note: sampled giant-component seed in RAM plus exact
cold-component spill.
State note: parent/min-label arrays + active-change bitmap.
Oracle note: partition isomorphism plus every edge endpoint
shares a label.
Use note: tenant rings, duplicate clusters, entity resolution,
and coarse partitions.
Reject note: reject when churn invalidates the snapshot faster
than rounds can converge.

#### ALG-064-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0208, A08-O0211

#### ALG-064-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0208, A08-O0211

#### ALG-064-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0208, A08-O0211

### ALG-065: DiskANN and Vamana ANN

Category: Vector ANN.
Aliases observed: DiskANN, Vamana, diskann, vamana.
Evidence count: 91; sample A07-O0155, A07-O0156.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: compressed Vamana graph, PQ codes, hot entry cache,
and exact vectors.
Spill note: sector-packed adjacency/vector pages with beam-
bounded reads.
Hybrid note: FreshDiskANN-style RAM delta plus disk base and
periodic merge.
State note: beam heap + visited filter + PQ scratch + exact-
rescore window.
Oracle note: recall@k, read-amplification receipts, and brute-
force shadow sets.
Use note: large local vector graphs where full HNSW RAM would be
too expensive.
Reject note: reject if storage cannot provide the needed random-
read latency.

#### ALG-065-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0155, A07-O0156

#### ALG-065-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0155, A07-O0156

#### ALG-065-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0155, A07-O0156

### ALG-066: Vector quantization family

Category: Vector storage.
Aliases observed: Binary quantization, OPQ, PQ, Product
Quantization, Product quantization, SBQ, pq, product
quantization.
Evidence count: 95; sample A07-O0405, A07-O0407.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: codebooks, compact codes, scale/offset tables, and
residual buffer.
Spill note: code slabs with resident codebooks and bounded
decode windows.
Hybrid note: coarse codes resident, exact residuals fetched for
reranking.
State note: codebooks + decode scratch + exact-rescore candidate
window.
Oracle note: distance-error histograms and recall/loss deltas
versus exact vectors.
Use note: shrinking vector RAM while preserving useful ranking
behavior.
Reject note: reject when legal or scientific workloads require
exact distances.

#### ALG-066-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0405, A07-O0407

#### ALG-066-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0405, A07-O0407

#### ALG-066-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0405, A07-O0407

### ALG-067: WAL group commit

Category: Storage.
Aliases observed: Group commit, WAL, group commit, wal.
Evidence count: 98; sample A07-O0006, A07-O0334.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: resident mutable buffer, compact immutable runs, and
metadata cache.
Spill note: tiered/leveled on-disk runs with bloom/filter gates.
Hybrid note: hot mutable tier in RAM with deterministic
background merge budget.
State note: memtable/run metadata + merge cursors + recovery
markers.
Oracle note: write/read history replay, crash injection, and
checksum validation.
Use note: bounded ingest, snapshot reads, and storage shaped to
analytics.
Reject note: reject if write amplification or recovery window
breaches the SLO.

#### ALG-067-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0006, A07-O0334

#### ALG-067-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0006, A07-O0334

#### ALG-067-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0006, A07-O0334

### ALG-068: WAND and block-max WAND

Category: Text retrieval.
Aliases observed: BLOCK max, WAND, block max, block-max, wand.
Evidence count: 77; sample A07-O0084, A07-O0086.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: block-max posting blocks, cursor heap, threshold, and
candidate heap.
Spill note: block-wise postings with resident upper-bound
headers only.
Hybrid note: hot head blocks resident, tail blocks skipped or
streamed.
State note: cursors + upper-bound heap + top-k threshold.
Oracle note: WAND versus exhaustive BM25 identity on generated
queries.
Use note: top-k retrieval where most postings should never be
scored.
Reject note: reject if upper bounds are stale or analyzers make
scores incomparable.

#### ALG-068-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A07-O0084, A07-O0086

#### ALG-068-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A07-O0084, A07-O0086

#### ALG-068-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A07-O0084, A07-O0086

### ALG-069: Weighted path products

Category: Linear algebra.
Aliases observed: weighted-path product.
Evidence count: 1; sample A08-O0065.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: CSR/CSC sparse matrix, value vector, and semiring
operator table.
Spill note: matrix tiles with resident vector slabs and
reduction buffers.
Hybrid note: hot rows/columns resident with cold tiles streamed.
State note: input vector/slab + output vector/slab + reducer
scratch.
Oracle note: GraphBLAS differential tests and semiring identity
checks.
Use note: unifying traversal, PageRank, path products, and
sparse analytics.
Reject note: reject if the operator is non-associative or order-
sensitive.

#### ALG-069-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0065

#### ALG-069-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0065

#### ALG-069-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0065

### ALG-070: Worst-case optimal joins

Category: Incremental.
Aliases observed: WCOJ.
Evidence count: 2; sample A08-O0315, A09-O0354.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: tries/leapfrog cursors, variable order, and output
window.
Spill note: key-range partitions with resident trie prefixes.
Hybrid note: hot high-selectivity prefixes resident and cold
joins streamed.
State note: join cursors + prefix bindings + output window.
Oracle note: differential against exhaustive join on sampled
partitions.
Use note: Cypher-like pattern matching and multi-hop analytical
projections.
Reject note: reject if output explosion cannot be streamed or
capped.

#### ALG-070-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0315, A09-O0354

#### ALG-070-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0315, A09-O0354

#### ALG-070-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0315, A09-O0354

### ALG-071: Yen k-shortest paths

Category: Traversal.
Aliases observed: Yen's k-shortest, k-shortest path.
Evidence count: 2; sample A08-O0548, A09-O0067.

Design stance: start from the algorithm's resident mutable
state, then decide which topology bytes can be compressed,
tiled, cached, or regenerated without changing the declared
semantics.

Fit note: weighted CSR, distance array, predecessor log, and
bucket or heap state.
Spill note: partitioned relax logs with bucket spill files and
checkpointed distances.
Hybrid note: resident active buckets with compressed cold
distances and page-cache hints.
State note: distance + predecessor + relaxation worklist.
Oracle note: differential against reference SSSP plus triangle-
inequality checks.
Use note: weighted impact paths, routing, attack paths, and
dependency distances.
Reject note: reject negative-weight inputs for plans that
require monotone relaxations.

#### ALG-071-A1: Resident fit
**Mode:** fit
**Storage layout:** Resident artifact and state.
**Memory equation:** B_peak = B_os + artifact + state.
**Budget decision:** Run only when UCB(B_peak) <= B_ram.
**Latency and I/O:** Fast path; no core spill reads.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Differential oracle plus invariants.
**Best for:** Small or hot working sets.
**Reject when:** Artifact or output cannot fit.
**Evidence:** A08-O0548, A09-O0067

#### ALG-071-A2: Bounded spill
**Mode:** spill
**Storage layout:** Partition files; one resident window.
**Memory equation:** B_peak = B_os + window + state + merge.
**Budget decision:** Choose tile count before execution.
**Latency and I/O:** More scans and merges for hard RAM.
**Correctness:** Exact if all windows are replayed.
**Verification:** Compare with fit plan on samples.
**Best for:** User accepts time for bounded RAM.
**Reject when:** Spill volume breaches disk or SLA.
**Evidence:** A08-O0548, A09-O0067

#### ALG-071-A3: Hot-cold hybrid
**Mode:** hybrid
**Storage layout:** Hot tier in RAM; cold tier paged.
**Memory equation:** B_peak = B_os + hot + cold + scratch.
**Budget decision:** Promote only measured hot shape.
**Latency and I/O:** Stable if hot/cold split is stable.
**Correctness:** Exact or declared epsilon per oracle.
**Verification:** Adversarial skew and replay tests.
**Best for:** A007 differentiated profile.
**Reject when:** Hot-set estimate is unstable.
**Evidence:** A08-O0548, A09-O0067

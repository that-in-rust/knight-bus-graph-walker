# Batch 11: Algorithm Oracle, Flat-CSR Parity, And Rust Fixture Scaffolding

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Execution lane`
- `Test-readiness lane`

Assigned PRD outcomes:

- `Algorithm oracle discipline before implementation`
- `Flat-CSR topology parity gate for future kernels`
- `Rust fixture scaffolding classified as scaffolding, not architecture`
- `Family-specific tolerance, determinism, ordering, and rejection rules`

Requirement IDs touched in this batch:

- `REQ-LEARN-026.0`
- `REQ-LEARN-048.0`

## Answer First

This batch closes the last two architecture-critical learning gaps.

The strongest conclusions are:

1. The right parity contract for GDS families is not one universal rule like
   "byte-for-byte match." Different families need different proof styles:
   exact path/set parity, partition parity, numeric-tolerance parity, seeded
   deterministic parity, rejection parity, or memory-estimate parity.
2. `neo4j-gds-src` already contains a rich oracle shelf in its algorithm tests:
   tiny GDL graphs, expected property values, expected path objects, expected
   community partitions, deterministic seed behavior, and explicit error cases.
3. The current Knight Bus flat immutable dual-CSR runtime is already the first
   topology oracle for future algorithm work. `MmapWalkRuntime` exposes
   adjacency and global edge iteration, and the current parity tests already
   prove that snapshot adjacency matches normalized forward/reverse arrays.
4. `gapbs-src` is the strongest external verifier shelf for exact graph-kernel
   parity: it exposes serial or simple verifier functions for BFS, PageRank,
   SSSP, connected components, and triangle count.
5. `petgraph-src`, `rustworkx-src`, `sprs-src`, `sparsetools-src`,
   `networkit-src`, and `igraph-src` are useful fixture, oracle, or matrix
   helper shelves. They should inform tests and small adapters, not decide the
   v003 storage architecture.
6. Families like `Louvain`, `Leiden`, `Node2Vec`, and some similarity/ML paths
   can now be described with concrete oracle directions, but they should still
   remain `NeedsArchitectureSpike` or later-tier families until their runtime
   workspace, determinism, and strict-RAM behavior are implemented honestly.

Short test-readiness thesis after this batch:

```text
v003 should treat correctness in three layers:
1. topology parity against the current flat dual-CSR runtime,
2. semantic parity against Neo4j GDS expected behavior,
3. state and RAM honesty through explicit estimator and rejection checks.
```

## Scope

This batch answers two linked questions:

1. What exact oracle and parity checks must exist before a GDS family can move
   from architecture study into implementation?
2. Which Rust- and graph-library repos are legitimate fixture or oracle shelves
   for those tests, without letting them distort the storage architecture?

This batch is not a new storage recommendation. It is the final
implementation-readiness layer for the reference-learning program.

## Graph-Tool Execution For This Batch

This batch explicitly used the two local graph-evidence skills required by the
learning spec:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

The full spec-scope control pass still lives in:

- `Reference-Shelf-Graph-Evidence-Ledger.md`
- `Reference-Shelf-Graph-Tool-Truthcheck.tsv`
- `Reference-Shelf-Subpath-Coverage-Audit.md`

This batch used fresh focused reruns for the repos that most directly affect the
remaining oracle and parity requirements.

| repo | graph-tool stance in this batch | why it was re-read now | verified use in this batch |
| --- | --- | --- | --- |
| `neo4j-gds-src` | fresh `CBM` query-ready, fresh `CGC` still low-yield | first-party oracle fixtures and expected-behavior tests | PageRank, WCC, TriangleCount, Dijkstra, Bellman-Ford, SCC, KCore, RandomWalk, NodeSimilarity, KNN, FastRP, Node2Vec, Louvain, Leiden |
| `gapbs-src` | fresh dual-tool signal | external exact verifier functions | `BFSVerifier`, `PRVerifier`, `SSSPVerifier`, `CCVerifier`, `TCVerifier` |
| `petgraph-src` | fresh dual-tool signal | Rust property-based oracle and invariant scaffolding | Dijkstra/A* triangle-inequality and equivalence checks, Bellman-Ford and PageRank invariants |
| `rustworkx-src` | fresh `CBM` query-ready, fresh `CGC` low-yield | small-graph generators and Python-facing oracle fixtures | PageRank-vs-Python oracle, random-walk exact small cases, transitivity small cases, Karate Club generator |

Important truth rule:

```text
Repo-root graph-tool runs count as the graph-evidence pass for named subfolders.
Folder-specific claims still require direct source reads. This batch follows
that rule.
```

## Current Knight Bus Flat-CSR Oracle

Before any future GDS kernel claims parity, it must pass the current topology
oracle that already exists in Knight Bus.

| current repo evidence | sourced fact | why it matters for Batch 11 |
| --- | --- | --- |
| `src/runtime.rs:22-53` | `WalkQueryRuntime` and `GraphAdjacencyRuntime` already define stable query and adjacency contracts, including `neighbors(...)` and `global_edges(...)`. | Future algorithms should target this canonical adjacency surface first. |
| `src/runtime.rs:157-198` | `MmapWalkRuntime::open(...)` opens the immutable dual-CSR snapshot and validates the storage mode before use. | The current snapshot format is the first topology oracle, not disposable legacy code. |
| `src/parity.rs:8-82` | `run_parity_verification(...)` and `run_corpus_parity_verification(...)` already compare runtime answers to truth-index expectations and fail on mismatch. | v003 already has a parity harness shape that future algorithm tests can extend. |
| `tests/graph_adjacency_runtime_contract.rs:61-93` | `global_edges(...)` for forward and reverse directions is asserted equal to normalized forward/reverse arrays. | Any future PageRank/BFS/WCC/etc. kernel can inherit a trusted topology baseline instead of proving adjacency correctness from scratch. |

Flat-CSR parity rule for this batch:

```text
No family is allowed to claim algorithm readiness unless its future test plan
includes:
1. snapshot-build parity into MmapWalkRuntime or the successor canonical runtime,
2. adjacency/global-edge parity at topology level,
3. family-specific semantic parity against a named GDS oracle.
```

## Evidence Ledger

| claim_id | source_path | symbol_or_fixture | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `B11-001` | `src/runtime.rs:22-53`, `src/parity.rs:8-82`, `tests/graph_adjacency_runtime_contract.rs:61-93` | `GraphAdjacencyRuntime`, `run_parity_verification`, `mmap_runtime_global_edges_match_normalized_graph_now` | Current Knight Bus already exposes neighbor/global-edge iteration and proves snapshot adjacency parity against normalized truth. | The present flat dual-CSR runtime is a valid topology oracle for future OLAP algorithm parity. | Later cells or alternate packaging can replace storage layout, but only after matching this topology contract. | Closes the “what is the flat-CSR parity check?” question in `REQ-LEARN-048.0`. | This is topology parity, not algorithm-score parity by itself. |
| `B11-002` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/pagerank/PageRankTest.java:68-123`, `...:143-172` | Wikipedia-style `expectedRank`, `expectedPersonalizedRank*` fixture | GDS stores expected PageRank values directly in fixture node properties and checks them within tolerance; personalized source-node variants are also tested. | PageRank parity should use numeric tolerance plus source-node coverage, not exact bitwise float equality. | Later parallel implementations may need looser tolerance than the single-thread test. | Gives a concrete first-party oracle for PageRank. | Tolerance choice must still be documented explicitly for v003. |
| `B11-003` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/wcc/WccTest.java:89-174` | orientation fixtures, seeded union graph | GDS checks WCC across `NATURAL`, `REVERSE`, and `UNDIRECTED` orientations and also verifies seeded-component grouping. | WCC parity is partition parity plus orientation semantics, not exact component-id equality. | A later write/mutate surface may add ordering quirks, but partition equality remains the core oracle. | Gives a concrete oracle for connectivity-family support. | Partition normalization must be explicit in future tests. |
| `B11-004` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/triangle/IntersectingTriangleCountTest.java:45-205` | no-triangle, clique, adjacent-triangle, self-loop, parallel-edge fixtures | GDS tests global and local triangle counts across degenerate and skew cases, including self-loops and parallel relationships. | Triangle-count parity needs both global-count and per-node local-count assertions, not one aggregate only. | High-degree optimizations may later change execution strategy, but not the oracle. | Gives a concrete oracle for structural intersection families. | This still does not solve strict-RAM for huge cliques. |
| `B11-005` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/paths/dijkstra/DijkstraTest.java:73-140`, `...:142-215`; `.../BellmanFordTest.java:53-169` | weighted shortest-path fixtures, negative-cycle cases | GDS checks exact expected node-paths and cumulative costs for Dijkstra, and Bellman-Ford explicitly tests negative-cycle detection and path/cost outputs. | Path families need exact path/cost parity plus rejection parity where negative cycles or invalid cases apply. | Some all-pairs variants may need weaker coverage initially. | Gives concrete oracles for weighted path families. | Output ordering and tie-breaking still need explicit handling when multiple shortest paths exist. |
| `B11-006` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/kcore/KCoreDecompositionTest.java:43-87`, `...:96-160`, `...:165-219`; `.../SccTest.java:40-138` | blossom graph, three-core graph, K4, three-SCC fixture | GDS checks exact core values/degeneracy on small graphs and checks SCC membership as partition equivalence across known groups. | K-core parity can be exact integer parity; SCC parity is partition parity. | Large-graph performance remains separate from oracle correctness. | Gives direct small-graph oracles for structural/community families. | Partition relabeling must not be mistaken for failure. |
| `B11-007` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityTest.java:77-120`, `...:133-240`; `.../similarity/knn/KnnTest.java:71-136`, `...:192-239` | explicit expected similarity sets and neighbor-list checks | GDS encodes expected similarity pairs, weighted/unweighted variants, `topK`, `topN`, cutoff, degree cutoff, and sorted neighbor-list behavior. | Similarity-family parity is result-set plus ranking/order parity; it is not reducible to a single scalar. | Approximate or ANN-backed later variants may need statistical rather than exact set parity. | Gives concrete oracles for property-plane-first families. | KNN family scalability is still a separate architecture question. |
| `B11-008` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/traversal/RandomWalkTest.java:89-159`; `.../embeddings/node2vec/Node2VecTest.java:99-198`, `...:200-237`; `.../embeddings/fastrp/FastRPTest.java:111-165` | deterministic seeds, embedding dimensions, scalar-vs-array parity | RandomWalk checks deterministic seeds; Node2Vec checks exact seeded equality at concurrency `1` and documents weaker determinism at higher concurrency; FastRP checks scalar-vs-array property parity and embedding propagation invariants. | Stochastic and embedding families need seeded-determinism contracts and, for some modes, concurrency-scoped parity classes rather than naive exactness. | Future SIMD or batching changes may preserve distributional semantics while changing exact floats. | Gives concrete parity rules for embeddings and stochastic families. | These are the easiest credible oracles, not proof that 50GB-on-8GB execution is practical. |
| `B11-009` | `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/louvain/LouvainTest.java:60-159`; `.../leiden/LeidenWeightedCliqueTest.java:41-90`; `.../louvain/LouvainMemoryEstimateDefinitionTest.java:39-84` | community partitions, modularity, weighted clique, memory ranges | Louvain tests community partitions across dendrogram levels and modularity tolerance; Leiden weighted-clique test expects a single community; Louvain estimate tests assert memory ranges. | Community-hierarchy families need partition parity, modularity tolerance, and estimate parity, not exact raw community-id arrays. | Contracted-graph workspaces may still need separate implementation spikes. | Gives concrete oracle direction without pretending these families are P1-ready. | Naming the oracle does not remove contracted-graph complexity. |
| `B11-010` | `gitrefrepo/gapbs-src/src/bfs.cc:196-220`, `.../sssp.cc:163-194`; `gitrefrepo/petgraph-src/crates/petgraph/tests/quickcheck.rs:770-860`, `...:1045-1095`, `...:1470-1484`; `gitrefrepo/rustworkx-src/tests/digraph/test_pagerank.py:58-146`; `.../tests/digraph/test_random_walk.py:18-39`; `.../tests/graph/test_transitivity.py:18-49` | external verifier and property-based oracle shelves | GAPBS exposes exact verifier functions; petgraph quickcheck encodes shortest-path, Bellman-Ford, and PageRank invariants; rustworkx exposes PageRank-vs-Python, exact random-walk toy cases, and transitivity exact fixtures. | External shelves are best used as secondary oracles and fixture generators, not as public-semantics authorities. | A future harness could mix GDS-first semantic checks with GAPBS/petgraph/rustworkx secondary checks. | Closes the scaffolding/oracle distinction in `REQ-LEARN-026.0`. | External repos help testing, but they must not redefine Neo4j semantics. |
| `B11-011` | `gitrefrepo/sprs-src/sprs/src/sparse.rs:14-77`; `gitrefrepo/sparsetools-src/src/test.rs:11-78`; `gitrefrepo/rustworkx-src/rustworkx-core/src/generators/karate_club.rs:18-125`; `gitrefrepo/networkit-src/docs/python_api/generators.rst:1-5`; `gitrefrepo/igraph-src/include/igraph_motifs.h:30-98` | CSR/CSC helpers, tiny sparse fixtures, Karate Club generator, generator docs, triangle/motif APIs | `sprs` documents sorted CSR/CSC helper types; `sparsetools` carries tiny CSR/CSC/COO sample arrays and assertion helpers; `rustworkx` ships a canonical Karate Club generator; `networkit` exposes generator and community-oriented fixture surface; `igraph` exposes motif/triangle APIs. | These repos are fixture, helper, or secondary-oracle shelves and should stay classified as scaffolding/oracle references rather than storage-architecture precedents. | Some may later help benchmarking or import/export adapters. | Completes the repo-role classification required by `REQ-LEARN-026.0`. | Presence of helpers does not mean v003 should depend on them directly. |

## Oracle And Parity Taxonomy

Different families need different proof obligations. This batch makes that
explicit so later implementation work does not overfit one family's test style
to another.

| parity class | meaning | representative families |
| --- | --- | --- |
| `ExactValueParity` | exact scalar or vector result equality on a tiny graph | `ScaleProperties`, `KCore`, simple `FastRP` property-shape parity |
| `ExactPathParity` | exact ordered node path and cumulative cost | `Dijkstra`, `BellmanFord` without ambiguous ties |
| `ExactSetParity` | exact unordered result set equality | `NodeSimilarity`, some `KNN` tiny fixtures |
| `PartitionParity` | same partitioning or community grouping, even if raw ids differ | `WCC`, `SCC`, `Louvain`, `Leiden` |
| `NumericToleranceParity` | floating-point equality within a named tolerance | `PageRank`, modularity checks, some embedding outputs |
| `SeededDeterministicParity` | exact equality given fixed seed and bounded concurrency | `RandomWalk`, `Node2Vec` single-threaded |
| `InvariantParity` | must satisfy structural or probability invariants | `petgraph` PageRank probabilities, shortest-path triangle inequality |
| `RejectionParity` | same explicit failure or warning behavior | negative-weight `Node2Vec`, invalid projection-orientation families |
| `EstimateParity` | estimate path names the same state classes and scales with the same dimensions | all `*.estimate` modes, especially `Louvain`, `PageRank`, `FastRP`, `KNN` |

## Rust Fixture And Oracle Scaffolding Matrix

This section closes `REQ-LEARN-026.0`.

| repo | role classification | concrete fixture or helper value | how v003 should use it | how v003 should not use it |
| --- | --- | --- | --- | --- |
| `petgraph-src` | `ScaffoldingAndInvariantOracle` | property-based checks for Dijkstra/A* triangle inequality and equivalence, Bellman-Ford sanity, PageRank probability invariants | tiny in-memory graph fixtures, property-based invariants, auxiliary path/graph sanity checks | not a storage architecture precedent for Neo4j-compatible OLAP |
| `rustworkx-src` | `ScaffoldingAndSecondaryOracle` | PageRank-vs-Python oracle, exact random-walk toy cases, exact transitivity fixtures, Karate Club generator | deterministic toy fixtures, classic named graphs, Python-facing parity cross-checks | not a source of Neo4j public semantics |
| `sprs-src` | `MatrixHelperScaffolding` | documented sorted CSR/CSC helper types and triplet-to-compressed construction | sparse-matrix helper experiments or harnesses, especially for GraphBLAS-adjacent family spikes | not a reason to adopt matrix-native storage as the default OLAP artifact |
| `sparsetools-src` | `TinySparseFixtureHelper` | tiny CSR/CSC/COO arrays and assertion helpers in Rust | unit-test helpers for sparse layout conversion and tiny known matrices | not a graph-algorithm semantics oracle |
| `networkit-src` | `GeneratorAndBenchmarkScaffolding` | generator docs and community/generator fixture surface | later benchmark generators, stochastic graph families, community benchmark fixtures | not a first-party compatibility oracle |
| `igraph-src` | `SecondaryAlgorithmOracle` | triangle, motif, and community APIs exposed in C headers | secondary triangle/community oracle or import/export helper source | not a v003 storage architecture template |

Rule of use:

```text
These repos can help v003 prove correctness faster.
They must not be allowed to choose the OLAP storage architecture.
```

## Family Oracle And Flat-CSR Parity Matrix

This section closes `REQ-LEARN-048.0` for the currently architecture-relevant
family set.

| family | tiny oracle graph | first-party GDS behavior source | flat-CSR parity check | external secondary oracle | estimate check | parity class | readiness after this batch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `PageRank` | Wikipedia-style rank example with expected per-node rank properties | `PageRankTest.java:68-123`, `143-172` | snapshot-build into current canonical runtime; future PageRank kernel must consume the same adjacency proven by `tests/graph_adjacency_runtime_contract.rs:61-93` | `gapbs` `PRVerifier`; `petgraph` PageRank probability invariant; `rustworkx` PageRank-vs-Python | `PageRankMemoryEstimateDefinition` plus v003 vector-size accounting | `NumericToleranceParity` | `P1-ready to implement on canonical topology` |
| `BFS` | line, tree, reachable/unreachable fixtures | GDS BFS proc/tests plus current truth-index walk queries | exact one-hop/two-hop topology parity already exists via current runtime; future BFS kernel must match reachability/parent depth on same snapshot | `gapbs` `BFSVerifier` | frontier/visited/parent memory contract | `ExactSetParity` plus parent-depth invariant | `P1-ready to implement on canonical topology` |
| `WCC` | mono and union graph fixtures with seeded components | `WccTest.java:89-174` | logical orientation over current canonical adjacency plus partition normalization | `gapbs` `CCVerifier` | disjoint-set and optional seed/weight state | `PartitionParity` | `P1-ready to implement on canonical topology` |
| `TriangleCount` | no-triangle line, clique5, adjacent triangles, self-loop, parallel edges | `IntersectingTriangleCountTest.java:45-205` | undirected logical projection over canonical adjacency, then global/local triangle count parity | `gapbs` `TCVerifier`; `rustworkx` transitivity toy cases; `igraph` triangle APIs | degree-gated intersection buffers and output arrays | `ExactValueParity` | `P1-ready if sorted/intersection-friendly adjacency is preserved` |
| `Dijkstra / SSSP` | weighted Wikipedia shortest-path fixture | `DijkstraTest.java:73-140`, `142-215` | exact path/cost parity over canonical weighted sidecars | `gapbs` `SSSPVerifier`; `petgraph` Dijkstra/A* invariant checks | distance, predecessor, queue state | `ExactPathParity` | `P1-ready with weight sidecars` |
| `BellmanFord` | negative-edge and negative-cycle fixtures | `BellmanFordTest.java:53-169` | exact path/cost parity plus negative-cycle rejection over canonical weighted sidecars | `petgraph` Bellman-Ford quickcheck | per-node distance plus cycle-detection state | `ExactPathParity` plus `RejectionParity` | `P1/P2 boundary, but test plan is clear` |
| `SCC` | three disjoint strongly connected groups | `SccTest.java:40-138` | forward plus reverse canonical adjacency must yield same component partitioning | `petgraph` SCC invariants | stack/index/visited estimate classes | `PartitionParity` | `P1-ready with reverse adjacency` |
| `KCore` | blossom graph, three-core graph, K4, empty graph | `KCoreDecompositionTest.java:43-219` | exact core values over undirected logical view of canonical adjacency | none needed beyond first-party tiny graphs | degree/core-value arrays | `ExactValueParity` | `P1-ready` |
| `NodeSimilarity` | person-item bipartite-like toy graph with explicit expected similarities | `NodeSimilarityTest.java:77-120`, `133-240` | result-set and ordering parity on canonical adjacency plus optional weights | optional GraphBLAS-style later cross-check; current first-party oracle is enough | topK/topN, vectors, cutoff, component prepass | `ExactSetParity` plus ranking order | `P2-implementable later; no longer oracle-blocked` |
| `KNN / FilteredKnn` | 3-node numeric-property fixture and filtered tiny graphs | `KnnTest.java:71-136`, `192-239`; filtered family tests from Batch 08 | property-plane parity first, topology only for node identity and optional filters | none required for first pass | neighbor-list, sampling, topK, filter-state memory | `ExactSetParity` plus sorted-neighbor invariant | `P2-implementable later; property-plane dependent` |
| `FastRP` | scalar-versus-array feature graph and neighbor-average propagation cases | `FastRPTest.java:72-121`, `123-165` | exact property-shape and propagation parity on canonical topology plus feature sidecars | none required for first pass | embedding dimension and per-node vectors | `ExactValueParity` for tiny fixtures, tolerance for larger embeddings | `P2-feasible with strict RAM gates` |
| `RandomWalk / Node2Vec` | tiny cyclic and zero-degree graphs; small embedding graph | `RandomWalkTest.java:89-159`; `Node2VecTest.java:99-198`, `200-237` | canonical adjacency plus RNG seed parity; single-thread exact, higher-concurrency relaxed or invariant-based | `rustworkx` random-walk toy cases | walk-buffer, corpus, probability cache, embedding matrix | `SeededDeterministicParity` plus `RejectionParity` | `NeedsArchitectureSpike` for large-scale practicality, but no longer oracle-undefined |
| `ScaleProperties` | tiny scalar and array property graph with exact scaled outputs | `ScalePropertiesTest.java:59-109`, `144-220` | property-plane-only parity independent of topology complexity | none required | property-column stats and output vectors | `ExactValueParity` | `P1-ready on typed property plane` |
| `Louvain / Leiden` | weighted clique, seeded community graph, expected partitions/modularity | `LouvainTest.java:60-159`; `LeidenWeightedCliqueTest.java:41-90`; `LouvainMemoryEstimateDefinitionTest.java:39-84` | partition parity on outputs plus modularity/level-count tolerance, but only after a contracted-graph workspace exists atop canonical topology | optional later modularity cross-checks from competitor shelves | contracted-graph workspace, dendrogram, intermediate communities | `PartitionParity` plus `NumericToleranceParity` plus `EstimateParity` | `Still NeedsArchitectureSpike` because workspace/spill behavior is not yet proven |

## Still-Gated Family Rows

This batch closes the oracle-planning requirement without pretending every
visible GDS row becomes immediately implementable.

| family cluster | why it stays gated | correct status after this batch |
| --- | --- | --- |
| `HITS`, `betweenness`, heavy all-pairs centralities | state shape and concurrency pressure are known, but the current batch does not yet pin a complete tiny-graph parity pack and strict-RAM gate for all modes | `NeedsArchitectureSpike` or later `P2` |
| `full ML pipeline and model-catalog execution` | public surface and artifact semantics are clear, but model training and durability need artifact-plane implementation, not just oracles | `P0-RegisteredCompatible` until implemented |
| large-scale `Node2Vec`, `GraphSAGE`, dense embedding training | tiny seeded fixtures exist, but credible 50GB-on-8GB support still needs spill or rejection logic | `NeedsArchitectureSpike` or `UnsupportedButRegistered` by mode |
| high-skew similarity modes | tiny exact fixtures exist, but candidate explosion and topK scaling still need strict budget policy | `P2` or `UnsupportedButRegistered` by workload |

## Requirement Impact

| requirement | effect of this artifact |
| --- | --- |
| `REQ-LEARN-026.0` | satisfied for this batch scope: the named Rust and adjacent graph repos are now explicitly classified as scaffolding, matrix helpers, or secondary-oracle shelves, not storage architecture. |
| `REQ-LEARN-048.0` | satisfied for this batch scope: implementable or architecture-relevant families now name tiny oracles, Neo4j/GDS behavior sources, flat-CSR parity checks, and memory-estimate checks, while still-gated families remain explicitly gated. |

## Skeptical Review

| concern | why it is fair | current answer |
| --- | --- | --- |
| `You are still not promising every GDS row is implemented.` | Correctness planning is not the same as implementation. | Correct. This batch closes the oracle and parity planning gap, not the implementation backlog. |
| `Flat-CSR parity is only topology parity today.` | True; no PageRank kernel exists in Knight Bus yet. | Also true. The point is that the topology oracle already exists, so future algorithm tests have a trusted substrate. |
| `Stochastic families can still hide nondeterminism.` | Random-walk and embedding training are notorious for this. | The batch makes the rule explicit: single-thread exact seeded parity when possible, otherwise documented invariant/tolerance or remain gated. |
| `Community ids are not stable labels.` | Raw ids can vary across implementations. | That is why the parity class is partition equality plus modularity/level checks, not raw community-id equality. |
| `External repos could distort Neo4j semantics.` | GAPBS, petgraph, and rustworkx are not Neo4j. | They are explicitly classified as secondary oracles and scaffolding, never as the public semantic authority. |

## Verification Log

Commands used during this batch included:

```bash
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/gapbs-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/gapbs-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/petgraph-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/petgraph-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rustworkx-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/rustworkx-src

nl -ba src/runtime.rs | sed -n '22,120p'
nl -ba src/runtime.rs | sed -n '157,260p'
nl -ba src/parity.rs | sed -n '1,180p'
nl -ba tests/graph_adjacency_runtime_contract.rs | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/pagerank/PageRankTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/wcc/WccTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/triangle/IntersectingTriangleCountTest.java | sed -n '1,240p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/paths/dijkstra/DijkstraTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/paths/bellmanford/BellmanFordTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/kcore/KCoreDecompositionTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/scc/SccTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/traversal/RandomWalkTest.java | sed -n '1,240p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/scaleproperties/ScalePropertiesTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/embeddings/fastrp/FastRPTest.java | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/embeddings/node2vec/Node2VecTest.java | sed -n '1,240p'
nl -ba gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/similarity/knn/KnnTest.java | sed -n '1,240p'
nl -ba gitrefrepo/gapbs-src/src/bfs.cc | sed -n '120,220p'
nl -ba gitrefrepo/gapbs-src/src/sssp.cc | sed -n '120,220p'
nl -ba gitrefrepo/petgraph-src/crates/petgraph/tests/quickcheck.rs | sed -n '760,860p'
nl -ba gitrefrepo/petgraph-src/crates/petgraph/tests/quickcheck.rs | sed -n '1038,1098p'
nl -ba gitrefrepo/petgraph-src/crates/petgraph/tests/quickcheck.rs | sed -n '1470,1495p'
nl -ba gitrefrepo/rustworkx-src/tests/digraph/test_pagerank.py | sed -n '1,220p'
nl -ba gitrefrepo/rustworkx-src/tests/digraph/test_random_walk.py | sed -n '1,220p'
nl -ba gitrefrepo/rustworkx-src/tests/graph/test_transitivity.py | sed -n '1,180p'
nl -ba gitrefrepo/rustworkx-src/rustworkx-core/src/generators/karate_club.rs | sed -n '1,180p'
nl -ba gitrefrepo/sprs-src/sprs/src/sparse.rs | sed -n '1,120p'
nl -ba gitrefrepo/sparsetools-src/src/test.rs | sed -n '1,200p'
nl -ba gitrefrepo/networkit-src/docs/python_api/generators.rst | sed -n '1,120p'
nl -ba gitrefrepo/igraph-src/include/igraph_motifs.h | sed -n '1,140p'
```

## Checkpoint Summary

The learning program now has a complete implementation-readiness chain:

1. public GDS surface inventory,
2. projection/catalog/store semantics,
3. storage-fit reasoning,
4. memory-estimate reasoning,
5. oracle and parity scaffolding.

What changes because of this batch:

- A future implementation agent no longer has to invent what "correct" means
  per family.
- The current flat dual-CSR runtime is now explicitly promoted from "current
  code" to "topology oracle."
- The fixture shelves are now separated cleanly into:
  - first-party GDS semantic oracle,
  - external exact verifier shelves,
  - Rust helper and generator shelves.

What remains after this batch is implementation work, not missing learning
requirements.

## References

- `src/runtime.rs`
- `src/parity.rs`
- `tests/graph_adjacency_runtime_contract.rs`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/pagerank/PageRankTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/wcc/WccTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/triangle/IntersectingTriangleCountTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/paths/dijkstra/DijkstraTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/paths/bellmanford/BellmanFordTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/kcore/KCoreDecompositionTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/scc/SccTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/traversal/RandomWalkTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/scaleproperties/ScalePropertiesTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/embeddings/fastrp/FastRPTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/embeddings/node2vec/Node2VecTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/similarity/knn/KnnTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/louvain/LouvainTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/leiden/LeidenWeightedCliqueTest.java`
- `gitrefrepo/neo4j-gds-src/algo/src/test/java/org/neo4j/gds/louvain/LouvainMemoryEstimateDefinitionTest.java`
- `gitrefrepo/gapbs-src/src/bfs.cc`
- `gitrefrepo/gapbs-src/src/sssp.cc`
- `gitrefrepo/petgraph-src/crates/petgraph/tests/quickcheck.rs`
- `gitrefrepo/rustworkx-src/tests/digraph/test_pagerank.py`
- `gitrefrepo/rustworkx-src/tests/digraph/test_random_walk.py`
- `gitrefrepo/rustworkx-src/tests/graph/test_transitivity.py`
- `gitrefrepo/rustworkx-src/rustworkx-core/src/generators/karate_club.rs`
- `gitrefrepo/sprs-src/sprs/src/sparse.rs`
- `gitrefrepo/sparsetools-src/src/test.rs`
- `gitrefrepo/networkit-src/docs/python_api/generators.rst`
- `gitrefrepo/igraph-src/include/igraph_motifs.h`

# G03 Citation Ancestry Report

**Status:** `METADATA_TRAVERSAL_COMPLETE`
**Epistemic boundary:** OpenAlex and Semantic Scholar provider relations establish only `CITES`. All branch roles and decision scores are `DERIVED_INFERENCE` metadata-screening judgments, not `SOURCE_CLAIM`s.

## Executive Result

G03 converted the 25 G02 seeds into a bounded depth-1 citation map and an exact G04 reading queue. One depth-2 neighborhood was attempted; its selected payload was rejected, so zero depth-2 identities or edges were retained. G03 did not read or acquire a paper. The result prioritizes citation-visible branches that can change an open architecture question; it does not prove any mechanism, performance result, or compatibility claim.

## Campaign Accounting

| Measure | Count | Cap |
|---|---:|---:|
| Initial seeds | 25 | exactly 25 |
| External HTTP attempts | 83 | 90 |
| Raw metadata observations | 1389 | 6,000 |
| Baseline canonical identities | 262 | frozen |
| Final canonical identities | 377 | N/A |
| New canonical identities retained | 115 | 250 |
| Provider-backed CITES edges | 158 | N/A |
| Metadata-inferred semantic edges | 1 | N/A |
| Depth-2 expansion attempts | 1 | at most 5 |
| Successful/empty depth-2 responses | 0 | N/A |
| Papers read | 0 | 0 |
| Full-text/PDF files acquired | 0 | 0 |
| Repositories acquired | 0 | 0 |

Edge counts: `CITES`=158, `IMPLEMENTS`=1.

Provider accounting: OpenAlex=28 requests/1 observations/0 retained edges; Semantic Scholar=55 requests/1388 observations/159 retained edges.

## Foundational Branches

Backward branches are older or referenced candidates retained by the frozen taxonomy screen. The label 'foundational' is a reading priority, not a claim about originality.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `PAPER-HASH-586f7dee44fe35d8` | Lower Bounds for Fully Dynamic Connectivity Problems in Graphs | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010, AQ-012 | 100 |
| `PAPER-HASH-0232e71ded2b5c43` | Towards Scaling Fully Personalized PageRank: Algorithms, Lower Bounds, and Experiments | BACKWARD / 1 | AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 100 |
| `PAPER-LEGACY-3c0de0428906d02c` | Finding and evaluating community structure in networks. | BACKWARD / 1 | AQ-001, AQ-003, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010 | 90 |
| `PAPER-1205.6233` | Defining and evaluating network communities based on ground-truth | BACKWARD / 1 | AQ-001, AQ-003, AQ-006, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-1409.2138` | Streaming Lower Bounds for Approximating MAX-CUT | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-2009.03038` | Multi-Pass Graph Streaming Lower Bounds for Cycle Counting, MAX-CUT, Matching Size, and Other Problems | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-2104.04908` | Graph streaming lower bounds for parameter estimation and property testing via a streaming XOR lemma | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-0708.3259` | Fast Evaluation of Union-Intersection Expressions | BACKWARD / 1 | AQ-004, AQ-005, AQ-008, AQ-012 | 80 |
| `PAPER-HASH-45105a9c93fc6719` | Optimizing memory performance for FPGA implementation of pagerank | BACKWARD / 1 | AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-HASH-a86a6676a8be12e6` | On the Complexity of Evaluating Regular Path Queries over Linear Existential Rules | BACKWARD / 1 | AQ-002, AQ-003, AQ-008, AQ-012 | 80 |
| `PAPER-2203.15534` | Design and implementation of parallel PageRank on multicore platforms | BACKWARD / 1 | AQ-001, AQ-002, AQ-005, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-HASH-798485e57ea536c2` | Evaluation and Enumeration Problems for Regular Path Queries | BACKWARD / 1 | AQ-003, AQ-005, AQ-008 | 80 |
| `PAPER-HASH-940c3a3c1948585f` | Binary Hashing for Approximate Nearest Neighbor Search on Big Data: A Survey | BACKWARD / 1 | AQ-003, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-0c6610e30e77c3b7` | Dichotomies for Evaluating Simple Regular Path Queries | BACKWARD / 1 | AQ-003, AQ-008 | 80 |
| `PAPER-HASH-6289e63fdfdba321` | A Survey on Personalized PageRank Computation Algorithms | BACKWARD / 1 | AQ-002, AQ-004, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-2101.12631` | A Comprehensive Survey and Experimental Comparison of Graph-Based Approximate Nearest Neighbor Search | BACKWARD / 1 | AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 80 |
| `PAPER-2502.05575` | Graph-Based Vector Search: An Experimental Evaluation of the State-of-the-Art | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-007, AQ-009, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-cc7ec0e13d566d08` | On a modification of Chebyshev’s inequality and of the error formula of Laplace | BACKWARD / 1 | AQ-002, AQ-004, AQ-006, AQ-008, AQ-010, AQ-011, AQ-012 | 75 |
| `PAPER-HASH-c5ac9d53375f623d` | New fast method for generating discrete random numbers with arbitrary frequency distributions | BACKWARD / 1 | AQ-002, AQ-006, AQ-009, AQ-010, AQ-012 | 75 |
| `PAPER-HASH-834f3c2395068802` | Fast probabilistic algorithms for hamiltonian circuits and matchings | BACKWARD / 1 | AQ-004, AQ-005, AQ-007 | 75 |
| `PAPER-HASH-5099b36696ec67c8` | Centrality in social networks conceptual clarification | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-009, AQ-010 | 75 |
| `PAPER-HASH-b7f233f20fba0d28` | Computing connected components on parallel computers | BACKWARD / 1 | AQ-004, AQ-006, AQ-009, AQ-012 | 75 |
| `PAPER-HASH-2621af3903ad7063` | On the evolution of random graphs | BACKWARD / 1 | AQ-002, AQ-006, AQ-012 | 75 |
| `PAPER-HASH-d9d30266bb7ae798` | Arboricity and Subgraph Listing Algorithms | BACKWARD / 1 | AQ-004, AQ-005, AQ-007 | 75 |
| `PAPER-HASH-97a50d1a904bd476` | Random sampling with a reservoir | BACKWARD / 1 | AQ-002, AQ-004, AQ-006, AQ-012 | 75 |
| `PAPER-HASH-a6526d4be4c1a086` | Random Generation of Combinatorial Structures from a Uniform Distribution | BACKWARD / 1 | AQ-001, AQ-002, AQ-006, AQ-007, AQ-009, AQ-010, AQ-012 | 75 |
| `PAPER-HASH-ea7c06fc2ff131a9` | Finding Regular Simple Paths in Graph Databases | BACKWARD / 1 | AQ-001, AQ-003, AQ-004, AQ-008, AQ-009, AQ-011 | 75 |
| `PAPER-HASH-ec47d3fed28cc4f4` | A matroid approach to finding edge connectivity and packing arborescences | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-006, AQ-008, AQ-011, AQ-012 | 75 |
| `PAPER-HASH-1fdba5647c53becd` | A linear-time algorithm for finding a sparsek-connected spanning subgraph of ak-connected graph | BACKWARD / 1 | AQ-002, AQ-004, AQ-005, AQ-006, AQ-008, AQ-010, AQ-012 | 75 |
| `PAPER-HASH-a766262d203f44e8` | A sublinear space, polynomial time algorithm for directed s-t connectivity | BACKWARD / 1 | AQ-004, AQ-006, AQ-008, AQ-010, AQ-012 | 75 |
| `PAPER-HASH-210725670d671170` | Probability Inequalities for Sums of Bounded Random Variables | BACKWARD / 1 | AQ-002, AQ-003, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 75 |
| `PAPER-HASH-f8599e4dc4eee483` | Sorting and Searching on the Word RAM | BACKWARD / 1 | AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008 | 75 |
| `PAPER-HASH-ab142af809564673` | The Anatomy of a Large-Scale Hypertextual Web Search Engine | BACKWARD / 1 | AQ-003, AQ-007, AQ-011, AQ-012 | 75 |
| `PAPER-HASH-0e933492fdec6483` | Rewriting of regular expressions and regular path queries | BACKWARD / 1 | AQ-003, AQ-008 | 75 |
| `PAPER-HASH-d84dd734ef8e9856` | The PageRank Citation Ranking : Bringing Order to the Web | BACKWARD / 1 | AQ-002, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 75 |
| `PAPER-HASH-9b43309b046b4742` | On external memory graph traversal | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-010 | 75 |
| `PAPER-HASH-528bbeb9ddb00f0f` | A random graph model for massive graphs | BACKWARD / 1 | AQ-002, AQ-006, AQ-008, AQ-010, AQ-011, AQ-012 | 75 |
| `PAPER-HASH-b2ed9bf3718a79c1` | ON THE EVOLUTION OF RANDOM GRAPHS | BACKWARD / 1 | AQ-002, AQ-006, AQ-012 | 75 |
| `PAPER-HASH-2d35f96d423f4ddb` | External memory BFS on undirected graphs with bounded degree | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 75 |
| `PAPER-LEGACY-8e42775586c27133` | Community structure in social and biological networks | BACKWARD / 1 | AQ-001, AQ-003, AQ-006, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 75 |

Displayed 40 of 74 retained identities whose observations include `BACKWARD`; 0 identities were reached in both directions.

## Implementation And Evaluation Branches

Forward branches are later citing works. A role is semantically typed only when its title explicitly anchors the cited target; otherwise the report merely nominates the branch for G04 reading.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `PAPER-HASH-a7f9ae72d795efba` | Tighter Lower Bounds for Single Source Personalized PageRank | FORWARD / 1 | AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 90 |
| `PAPER-HASH-901ec8aded38dbd0` | Hidden Permutations to the Rescue: Multi-Pass Streaming Lower Bounds for Approximate Matchings | FORWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-1604.06264` | Data Structure Lower Bounds for Document Indexing Problems | FORWARD / 1 | AQ-001, AQ-002, AQ-003, AQ-004, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 90 |
| `PAPER-HASH-5be22f3d593ea876` | DF* PageRank: Improved Incrementally Expanding Approaches for Updating PageRank on Dynamic Graphs | FORWARD / 1 | AQ-002, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-2608.07254` | SCALE: Scientific Concept Aggregation via LLMs and Embeddings for Fine-Grained Taxonomy Extension | FORWARD / 1 | AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-5f08934040ef557c` | An Improved Watershed Classification Method Based on Multidimensional Indicator Clustering–Similarity Evaluation–Classification: A Case Study in Shandong Province, China | FORWARD / 1 | AQ-001, AQ-002, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 80 |
| `PAPER-2509.20204` | Output-Sensitive Evaluation of Acyclic Conjunctive Regular Path Queries | FORWARD / 1 | AQ-003, AQ-005, AQ-007, AQ-008 | 80 |
| `PAPER-HASH-6d247d25d59aa0a2` | Intelligence Evaluating Computational Power: A Multi-Factor Method | FORWARD / 1 | AQ-002, AQ-004, AQ-007, AQ-008, AQ-009, AQ-012 | 80 |
| `PAPER-HASH-b3b01325cae00d33` | An Improved Graph Partitioning Algorithm Based Approach for Workflow Offloading in a Fog Environment | FORWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 80 |
| `PAPER-2404.08299` | Efficient GPU Implementation of Static and Incrementally Expanding DF-P PageRank for Dynamic Graphs | FORWARD / 1 | AQ-002, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-HASH-3cd083d5fa09c1f7` | DF* PageRank: Improved Incrementally Expanding Approaches for Updating PageRank on Dynamic Graphs | FORWARD / 1 | AQ-002, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-HASH-5aa86bd99689667d` | Direction-optimizing Label Propagation Framework for Structure Detection in Graphs: Design, Implementation, and Experimental Analysis | FORWARD / 1 | AQ-001, AQ-002, AQ-003, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-e2675abd59cec742` | Evaluating Methods for Efficient Community Detection in Social Networks | FORWARD / 1 | AQ-001, AQ-003, AQ-006, AQ-008, AQ-009, AQ-010 | 80 |
| `PAPER-HASH-d213ecfc1c303858` | Developing an Efficient Vector-Friendly Implementation of the Breadth-First Search Algorithm for NEC SX-Aurora TSUBASA | FORWARD / 1 | AQ-001, AQ-002, AQ-003, AQ-004, AQ-005, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-d0ecbb809bcec30a` | A survey of current challenges in partitioning and processing of graph-structured data in parallel and distributed systems | FORWARD / 1 | AQ-001, AQ-002, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-e1d33f3b1ccee67e` | Graph Compression by Tree Grammars and Direct Evaluation of Regular Path Query | FORWARD / 1 | AQ-001, AQ-003, AQ-008, AQ-009, AQ-010, AQ-011 | 80 |
| `PAPER-HASH-663d1c4d7b2c7a6d` | Developing Efficient Implementations of Bellman-Ford and Forward-Backward Graph Algorithms for NEC SX-ACE | FORWARD / 1 | AQ-002, AQ-005, AQ-008, AQ-012 | 80 |
| `PAPER-HASH-30d2a0e04327c2df` | A CUDA implementation of the pagerank pipeline benchmark | FORWARD / 1 | AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-HASH-5657f8c3995227d1` | CompreGel: Efficient Distributed Graph Propagation via Error-Bounded Lossy Message Compression | FORWARD / 1 | AQ-001, AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 75 |
| `PAPER-HASH-6ac53b6f091a572e` | A Fast and Efﬁcient Parallel Algorithm for Pruned Landmark Labeling | FORWARD / 1 | AQ-003, AQ-004, AQ-006, AQ-008, AQ-009, AQ-011, AQ-012 | 65 |
| `PAPER-HASH-89669fe983572dd0` | Edinburgh Research Explorer A Researcher’s Digest of GQL | FORWARD / 1 | AQ-003, AQ-007, AQ-011, AQ-012 | 65 |
| `PAPER-HASH-b4b889246a331def` | Clustering guided dense context sensitive structure extractor: A relaxation induced approach | FORWARD / 1 | AQ-001, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-012 | 65 |
| `PAPER-2608.07903` | TopoBudget: Persistent-Connectivity-Preserving Web Graph Sparsification for Reusable Community Analytics | FORWARD / 1 | AQ-001, AQ-004, AQ-006, AQ-008, AQ-009, AQ-011, AQ-012 | 65 |
| `PAPER-2605.17992` | PipeANN-Filter: An Efficient Filtered Vector Search System on SSD | FORWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 65 |
| `PAPER-2605.17524` | Covariance Structure and Coordinate Heterogeneity Govern Binary Quantization of Contrastive Embeddings | FORWARD / 1 | AQ-001, AQ-004, AQ-007, AQ-011 | 65 |
| `PAPER-2603.08937` | Unit Interval Selection in Random Order Streams | FORWARD / 1 | AQ-001, AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 65 |
| `PAPER-2602.20748` | cuRPQ: A High-Performance GPU-Based Framework for Processing Regular and Conjunctive Regular Path Queries | FORWARD / 1 | AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 65 |
| `PAPER-2602.11052` | GraphSeek: Next-Generation Graph Analytics with LLMs | FORWARD / 1 | AQ-001, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 65 |
| `PAPER-2512.11129` | Acyclic Conjunctive Regular Path Queries are no Harder than Corresponding Conjunctive Queries | FORWARD / 1 | AQ-003, AQ-008, AQ-010 | 65 |
| `PAPER-2510.08010` | Accelerated Evolving Set Processes for Local PageRank Computation | FORWARD / 1 | AQ-002, AQ-004, AQ-005, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 65 |
| `PAPER-HASH-adcc1407df0b513f` | One Index for All: Towards Efficient Personalized PageRank Computation for Every Damping Factor | FORWARD / 1 | AQ-001, AQ-002, AQ-004, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 65 |
| `PAPER-HASH-3686692d994dd31f` | Insights into Optimizing Research Software: A Case of an Architecture-Smell Detection Tool | FORWARD / 1 | AQ-003, AQ-006, AQ-007, AQ-010, AQ-011, AQ-012 | 65 |
| `PAPER-2508.01257` | PageRank Centrality in Directed Graphs with Bounded In-Degree | FORWARD / 1 | AQ-002, AQ-003, AQ-005, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 65 |
| `PAPER-2504.16481` | Estimating Random-Walk Probabilities in Directed Graphs | FORWARD / 1 | AQ-002, AQ-006, AQ-008, AQ-010, AQ-012 | 65 |
| `PAPER-HASH-b510c5bdce003e21` | Optimized Parallel Breadth-First Search with Adaptive Strategies | FORWARD / 1 | AQ-001, AQ-003, AQ-005, AQ-007, AQ-010, AQ-011 | 65 |
| `PAPER-HASH-e2d101f2539c7f68` | Parallel Breadth-First Search Optimization Strategies | FORWARD / 1 | AQ-001, AQ-003, AQ-006, AQ-007, AQ-009, AQ-010, AQ-011 | 65 |
| `PAPER-2412.11963` | Approximating the Top Eigenvector in Random Order Streams | FORWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 65 |
| `PAPER-2407.19562` | Lock-Free Computation of PageRank in Dynamic Graphs | FORWARD / 1 | AQ-002, AQ-004, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 65 |
| `PAPER-HASH-2d128f9e147003dd` | Beyond the Click: Navigating the Depths of Page Ranking Algorithms | FORWARD / 1 | AQ-002, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 65 |
| `PAPER-2404.19634` | DF Louvain: Fast Incrementally Expanding Approach for Community Detection on Dynamic Graphs | FORWARD / 1 | AQ-001, AQ-002, AQ-006, AQ-008, AQ-009 | 65 |

Displayed 40 of 63 retained identities whose observations include `FORWARD`; 0 identities were reached in both directions.

## Contradictory Branches

Constraint And Negative-Result Signals: these titles contain an explicit counterexample, lower-bound, impossibility, limitation, intractability, resolution-limit, or complexity-relief signal. They can constrain a design without contradicting the cited target. Reading is required before assigning `CONTRADICTS`.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `PAPER-HASH-586f7dee44fe35d8` | Lower Bounds for Fully Dynamic Connectivity Problems in Graphs | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010, AQ-012 | 100 |
| `PAPER-HASH-0232e71ded2b5c43` | Towards Scaling Fully Personalized PageRank: Algorithms, Lower Bounds, and Experiments | BACKWARD / 1 | AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 100 |
| `PAPER-HASH-a7f9ae72d795efba` | Tighter Lower Bounds for Single Source Personalized PageRank | FORWARD / 1 | AQ-002, AQ-003, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 90 |
| `PAPER-HASH-901ec8aded38dbd0` | Hidden Permutations to the Rescue: Multi-Pass Streaming Lower Bounds for Approximate Matchings | FORWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-1604.06264` | Data Structure Lower Bounds for Document Indexing Problems | FORWARD / 1 | AQ-001, AQ-002, AQ-003, AQ-004, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 90 |
| `PAPER-1409.2138` | Streaming Lower Bounds for Approximating MAX-CUT | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-2009.03038` | Multi-Pass Graph Streaming Lower Bounds for Cycle Counting, MAX-CUT, Matching Size, and Other Problems | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-2104.04908` | Graph streaming lower bounds for parameter estimation and property testing via a streaming XOR lemma | BACKWARD / 1 | AQ-002, AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 90 |
| `PAPER-2512.11129` | Acyclic Conjunctive Regular Path Queries are no Harder than Corresponding Conjunctive Queries | FORWARD / 1 | AQ-003, AQ-008, AQ-010 | 65 |
| `PAPER-HASH-e699ebd01c15d6a4` | Intractability of min- and max-cut in streaming graphs | FORWARD / 1 | AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-011 | 65 |

## Survey And Review Signals

These titles explicitly identify a survey or review. They are G04 routing candidates, not `SURVEYS` edges, unless the title also anchors the exact cited target.

| Paper | Metadata title | Direction/depth | AQ links | Decision score |
|---|---|---|---|---:|
| `PAPER-HASH-d0ecbb809bcec30a` | A survey of current challenges in partitioning and processing of graph-structured data in parallel and distributed systems | FORWARD / 1 | AQ-001, AQ-002, AQ-004, AQ-006, AQ-008, AQ-009, AQ-010, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-940c3a3c1948585f` | Binary Hashing for Approximate Nearest Neighbor Search on Big Data: A Survey | BACKWARD / 1 | AQ-003, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 80 |
| `PAPER-HASH-6289e63fdfdba321` | A Survey on Personalized PageRank Computation Algorithms | BACKWARD / 1 | AQ-002, AQ-004, AQ-007, AQ-008, AQ-009, AQ-010, AQ-012 | 80 |
| `PAPER-2101.12631` | A Comprehensive Survey and Experimental Comparison of Graph-Based Approximate Nearest Neighbor Search | BACKWARD / 1 | AQ-003, AQ-004, AQ-005, AQ-006, AQ-007, AQ-008, AQ-009, AQ-011, AQ-012 | 80 |

## Post-Traversal Screening Review

Four disjoint read-only `gpt-5.6-sol` xhigh lanes screened backward candidates, forward systems, negative/survey signals, and provenance/accounting. The lanes read metadata and control artifacts only. They did not read papers or ignored provider caches.

- Backward lane: 66 identities after constraint-lane precedence, prioritizing external-memory traversal, adjacency compression, local PageRank, dynamic indexes, direction-optimizing BFS, and path-query semantics.
- Forward lane: 57 identities after constraint- and backward-lane precedence, prioritizing graph-shaped SSD/storage systems, partitioned processing, direct compressed-query execution, named benchmark implementations, RPQ systems, and I/O-aware ANN scheduling.
- Constraint lane: 14 lower-bound, intractability, resolution-limit, survey, or review identities remained reading nominations rather than semantic claims.
- Audit lane: independently reconciled 25 seeds, 83 attempts, 1389 observations, 377 identities, 159 typed edges, 1251 exact stops, provider attribution, and the G04 queue.
- Semantic result: exactly one title-explicit `IMPLEMENTS` inference survives the strict target-anchor rule; all other retained role relationships remain `CITES` only.

## Stopped Branches

Exact stopped observations: **1251**. The complete identity-level record is `sources/citation-stops.tsv`; the table below displays provider, retry-reserve, and payload stops while the reason table reconciles every row.

| Stop reason | Count |
|---|---:|
| `NO_DECISION_IMPACT` | 90 |
| `PER_SEED_DIRECTION_QUOTA` | 1114 |
| `REQUEST_RETRY_RESERVE` | 45 |
| `S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED` | 1 |
| `S2_SELECTED_PAYLOAD_REJECTED` | 1 |

Exact provider and retry-reserve stops:

| Paper | Seed | Depth | Direction | Reason |
|---|---|---:|---|---|
| `PAPER-1205.6233` | `PAPER-1304.4453` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1205.6233` | `PAPER-1602.02864` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1205.6233` | `PAPER-2010.09913` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1205.6233` | `PAPER-2012.10026` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1205.6233` | `PAPER-2511.07886` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1409.2138` | `PAPER-2305.11053` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1604.06264` | `PAPER-1407.6755` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2104.12523` | `PAPER-1812.10950` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2404.08299` | `PAPER-1709.07122` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2509.20204` | `PAPER-2412.07729` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2510.08010` | `PAPER-2404.16267` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2605.17524` | `PAPER-2605.02171` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2605.17992` | `PAPER-2603.01779` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2605.19335` | `PAPER-2602.21514` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2607.20881` | `PAPER-2511.07886` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-2608.07254` | `PAPER-1810.08473` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-0232e71ded2b5c43` | `PAPER-2401.01019` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-45105a9c93fc6719` | `PAPER-1709.07122` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-5be22f3d593ea876` | `PAPER-2104.09616` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-6289e63fdfdba321` | `PAPER-2104.09616` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-6b92d584bf9c06be` | `PAPER-0708.4284` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-6d247d25d59aa0a2` | `PAPER-1603.01876` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-6ff704db6b1236c3` | `PAPER-2503.00430` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-834f3c2395068802` | `PAPER-1812.10950` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-89669fe983572dd0` | `PAPER-2204.11137` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-901ec8aded38dbd0` | `PAPER-2305.11053` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-940c3a3c1948585f` | `PAPER-2602.21514` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-97a50d1a904bd476` | `PAPER-2112.00098` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-9b43309b046b4742` | `PAPER-2507.12925` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-a7f9ae72d795efba` | `PAPER-2401.01019` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-a86a6676a8be12e6` | `PAPER-2412.07729` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-ab142af809564673` | `PAPER-1603.01876` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-ab142af809564673` | `PAPER-2404.16267` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-b3b01325cae00d33` | `PAPER-1304.4453` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-b510c5bdce003e21` | `PAPER-2012.10026` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-cc7ec0e13d566d08` | `PAPER-2605.02171` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-cd1d7cedd720a87c` | `PAPER-1905.04264` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-d213ecfc1c303858` | `PAPER-2010.09913` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-d84dd734ef8e9856` | `PAPER-1708.07271` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-e1d33f3b1ccee67e` | `PAPER-1708.07271` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-e2d101f2539c7f68` | `PAPER-2503.00430` | 2 | FORWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-ea7c06fc2ff131a9` | `PAPER-2204.11137` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-ec47d3fed28cc4f4` | `PAPER-0708.4284` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-HASH-fb31f720c2b9f950` | `PAPER-1905.04264` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-LEGACY-3c0de0428906d02c` | `PAPER-1810.08473` | 2 | BACKWARD | `REQUEST_RETRY_RESERVE` |
| `PAPER-1602.02864` | `PAPER-1602.02864` | 1 | FORWARD | `S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED` |
| `PAPER-HASH-586f7dee44fe35d8` | `PAPER-1407.6755` | 2 | BACKWARD | `S2_SELECTED_PAYLOAD_REJECTED` |

Depth-2 expansion was attempted 1 time(s), with 0 successful or empty selected-metadata response(s). A retained depth-2 identity is never expanded further. Forward and backward traversal used one page per operation; the Semantic Scholar page limit is 75 and the OpenAlex page limit is 100. These are explicit recall limits.

## Architecture-Question Decision Impact

| Architecture question | Retained branch identities | Decision effect |
|---|---:|---|
| `AQ-001` | 43 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-002` | 76 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-003` | 83 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-004` | 62 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-005` | 53 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-006` | 69 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-007` | 88 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-008` | 108 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-009` | 89 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-010` | 87 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-011` | 65 | `G04_READING_PRIORITY_CHANGED` |
| `AQ-012` | 94 | `G04_READING_PRIORITY_CHANGED` |

## Coverage Gaps

- OpenAlex `referenced_works` omits references it cannot resolve to an OpenAlex identity; this is not a complete bibliography.
- OpenAlex exact arXiv-location resolution can miss records whose location metadata differs; Semantic Scholar exact arXiv resolution repaired this for all 25 seeds in this campaign.
- One-page traversal can miss lower-ranked relations beyond 75 Semantic Scholar results or 100 OpenAlex results.
- One depth-2 Semantic Scholar response violated the selected-metadata envelope and was retained only as a checksummed `PAYLOAD_REJECTED` marker.
- One seed's forward branch exhausted three Semantic Scholar rate-limit attempts and remains an explicit coverage gap.
- Twelve-reference sampling can miss a relevant ancestor in a long bibliography.
- Titles and bibliographic types cannot prove implementation, evaluation, contradiction, mechanism, correctness, RAM, or latency claims.
- No citation metadata directly closes Bolt, Cypher, GDS procedure, admission-control, whole-process RSS, or verification-receipt gaps unless its title matched the frozen taxonomy.

## Exact Recommended G04 Acquisition Set

The set contains all 25 original seeds plus 25 new ancestry identities after global deduplication and four-lane post-traversal screening. The reviewed queue replaces generic taxonomy/clustering false positives and ambiguous duplicate PageRank identities with architecture-direct external-memory, storage, compression, query, implementation, and survey candidates. Queue basis: `FOUR_LANE_SCREENING_LEDGER`. G04 must perform its own license, availability, and acquisition-time identity preflight.

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
| 26 | `PAPER-HASH-9b43309b046b4742` | On external memory graph traversal | `SCREENED_BACKWARD_DEPTH_1_SCORE_75` |
| 27 | `PAPER-HASH-2d35f96d423f4ddb` | External memory BFS on undirected graphs with bounded degree | `SCREENED_BACKWARD_DEPTH_1_SCORE_75` |
| 28 | `PAPER-HASH-45105a9c93fc6719` | Optimizing memory performance for FPGA implementation of pagerank | `SCREENED_BACKWARD_DEPTH_1_SCORE_80` |
| 29 | `PAPER-HASH-ecaa8a2220cf17a9` | Heuristics for semi-external depth first search on directed graphs | `SCREENED_BACKWARD_DEPTH_1_SCORE_75` |
| 30 | `PAPER-1011.5425` | Layered label propagation: a multiresolution coordinate-free ordering for compressing social networks | `SCREENED_BACKWARD_DEPTH_1_SCORE_65` |
| 31 | `PAPER-HASH-6ff704db6b1236c3` | Direction-optimizing Breadth-First Search | `SCREENED_BACKWARD_DEPTH_1_SCORE_75` |
| 32 | `PAPER-HASH-c2a6a5317d82ac28` | Local Computation of PageRank Contributions | `SCREENED_BACKWARD_DEPTH_1_SCORE_75` |
| 33 | `PAPER-HASH-ea7c06fc2ff131a9` | Finding Regular Simple Paths in Graph Databases | `SCREENED_BACKWARD_DEPTH_1_SCORE_75` |
| 34 | `PAPER-HASH-798485e57ea536c2` | Evaluation and Enumeration Problems for Regular Path Queries | `SCREENED_BACKWARD_DEPTH_1_SCORE_80` |
| 35 | `PAPER-HASH-0c6610e30e77c3b7` | Dichotomies for Evaluating Simple Regular Path Queries | `SCREENED_BACKWARD_DEPTH_1_SCORE_80` |
| 36 | `PAPER-HASH-a86a6676a8be12e6` | On the Complexity of Evaluating Regular Path Queries over Linear Existential Rules | `SCREENED_BACKWARD_DEPTH_1_SCORE_80` |
| 37 | `PAPER-0708.3259` | Fast Evaluation of Union-Intersection Expressions | `SCREENED_BACKWARD_DEPTH_1_SCORE_80` |
| 38 | `PAPER-HASH-22a322abb4cef920` | Bw-Graph: An Efficient Graph Storage System Harmonizing Topology-Aware Tree with Paged CSR | `SCREENED_FORWARD_DEPTH_1_SCORE_25` |
| 39 | `PAPER-HASH-b12240577b20eaad` | Kedagraph: memory-efficient out-of-core graph processing system with high applicability | `SCREENED_FORWARD_DEPTH_1_SCORE_25` |
| 40 | `PAPER-1806.08092` | GPOP: a cache and memory-efficient framework for graph processing over partitions | `SCREENED_FORWARD_DEPTH_1_SCORE_65` |
| 41 | `PAPER-2605.19335` | Leveraging I/O Stalls for Efficient Scheduling in ANNS | `SCREENED_FORWARD_DEPTH_1_SCORE_25` |
| 42 | `PAPER-HASH-e1d33f3b1ccee67e` | Graph Compression by Tree Grammars and Direct Evaluation of Regular Path Query | `SCREENED_FORWARD_DEPTH_1_SCORE_80` |
| 43 | `PAPER-2605.17992` | PipeANN-Filter: An Efficient Filtered Vector Search System on SSD | `SCREENED_FORWARD_DEPTH_1_SCORE_65` |
| 44 | `PAPER-HASH-5aa86bd99689667d` | Direction-optimizing Label Propagation Framework for Structure Detection in Graphs: Design, Implementation, and Experimental Analysis | `SCREENED_FORWARD_DEPTH_1_SCORE_80` |
| 45 | `PAPER-HASH-d213ecfc1c303858` | Developing an Efficient Vector-Friendly Implementation of the Breadth-First Search Algorithm for NEC SX-Aurora TSUBASA | `SCREENED_FORWARD_DEPTH_1_SCORE_80` |
| 46 | `PAPER-HASH-30d2a0e04327c2df` | A CUDA implementation of the pagerank pipeline benchmark | `SCREENED_FORWARD_DEPTH_1_SCORE_80` |
| 47 | `PAPER-HASH-5657f8c3995227d1` | CompreGel: Efficient Distributed Graph Propagation via Error-Bounded Lossy Message Compression | `SCREENED_FORWARD_DEPTH_1_SCORE_75` |
| 48 | `PAPER-HASH-586f7dee44fe35d8` | Lower Bounds for Fully Dynamic Connectivity Problems in Graphs | `SCREENED_BACKWARD_DEPTH_1_SCORE_100` |
| 49 | `PAPER-HASH-0232e71ded2b5c43` | Towards Scaling Fully Personalized PageRank: Algorithms, Lower Bounds, and Experiments | `SCREENED_BACKWARD_DEPTH_1_SCORE_100` |
| 50 | `PAPER-2101.12631` | A Comprehensive Survey and Experimental Comparison of Graph-Based Approximate Nearest Neighbor Search | `SCREENED_BACKWARD_DEPTH_1_SCORE_80` |

Exact G04 set size: **50** canonical identities.

## Scope Boundary

G03 downloaded no PDF, abstract, paper body, source archive, or repository; read no paper; created no mechanism, failure, or transfer card; proposed no architecture or experiment; and did not begin G04. OpenAlex and sanitized Semantic Scholar selected-metadata bodies remain ignored local cache files. The report is a citation-metadata routing artifact only.

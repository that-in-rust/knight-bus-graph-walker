#!/usr/bin/env python3
"""Build the line-complete low-RAM graph-learning atlas artifacts.

The generator is intentionally deterministic. It reads the frozen
graph-learning manifest, scans every assigned source file line by line,
emits lane ledgers and occurrence ledgers, then writes the canonical
algorithm ledger and the final Markdown atlas.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
import re
import textwrap


EVIDENCE_ROOT = Path("docs_PRD06/reference-learning/all-algorithm-lowram/evidence")
MANIFEST_PATH = EVIDENCE_ROOT / "all-graph-learning-files.tsv"
ATLAS_PATH = Path("docs_PRD06/LowRAM-All-Algorithm-Architecture-Atlas.md")
AGENTS = ("07", "08", "09")


@dataclass(frozen=True)
class Concept:
    hint: str
    name: str
    kind: str
    category: str
    family: str
    patterns: tuple[str, ...]


CONCEPTS: tuple[Concept, ...] = (
    Concept("bfs", "BFS frontier traversal", "algorithm", "Traversal", "traversal", (r"\bBFS\b", r"breadth[- ]first")),
    Concept("dfs", "DFS depth traversal", "algorithm", "Traversal", "traversal", (r"\bDFS\b", r"depth[- ]first")),
    Concept("reachability", "Reachability and reverse reach", "algorithm_family", "Traversal", "traversal", (r"reachability", r"reverse reach", r"forward reach")),
    Concept("shortest-paths", "Unweighted shortest paths", "algorithm_family", "Traversal", "traversal", (r"shortest path", r"path search")),
    Concept("dijkstra", "Dijkstra weighted shortest path", "algorithm", "Traversal", "sssp", (r"Dijkstra",)),
    Concept("delta-stepping", "Delta-stepping SSSP", "algorithm", "Traversal", "sssp", (r"delta[- ]stepping", r"delta bucket", r"bucketed relaxation")),
    Concept("bellman-ford", "Bellman-Ford relaxation", "algorithm", "Traversal", "sssp", (r"Bellman[- ]Ford",)),
    Concept("a-star", "A* heuristic path search", "algorithm", "Traversal", "sssp", (r"A\*", r"\bA-star\b")),
    Concept("yen-k-shortest", "Yen k-shortest paths", "algorithm", "Traversal", "sssp", (r"Yen'?s? k[- ]shortest", r"k[- ]shortest path")),
    Concept("connected-components", "Weakly connected components", "algorithm_family", "Components", "components", (r"\bWCC\b", r"connected components?", r"component labels?")),
    Concept("hook-shortcut", "Hooking and shortcutting", "algorithm", "Components", "components", (r"hook(?:ing)?", r"shortcut(?:ting)?", r"pointer[- ]jump")),
    Concept("union-find", "Union-find path compression", "algorithm", "Components", "components", (r"UnionFind", r"union[- ]find", r"path compression")),
    Concept("afforest", "Afforest sampled components", "algorithm", "Components", "components", (r"Afforest",)),
    Concept("fastsv", "FastSV star contraction", "algorithm", "Components", "components", (r"FastSV", r"large[- ]star", r"small[- ]star")),
    Concept("label-propagation", "Label propagation", "algorithm", "Components", "components", (r"label propagation", r"LabelPropagation")),
    Concept("scc", "Strongly connected components", "algorithm", "Components", "components", (r"\bSCC\b", r"strongly connected", r"cycle detection")),
    Concept("pagerank", "PageRank power iteration", "algorithm", "Centrality", "iterative", (r"PageRank", r"personalized PageRank")),
    Concept("centrality-family", "Centrality family", "algorithm_family", "Centrality", "iterative", (r"\bcentrality\b",)),
    Concept("betweenness-centrality", "Betweenness centrality", "algorithm", "Centrality", "dependency", (r"betweenness",)),
    Concept("closeness-centrality", "Closeness centrality", "algorithm", "Centrality", "dependency", (r"closeness",)),
    Concept("degree-centrality", "Degree centrality", "algorithm", "Centrality", "iterative", (r"degree centrality", r"\bdegree\b")),
    Concept("eigenvector-family", "Eigenvector, Katz, HITS, ArticleRank", "algorithm_family", "Centrality", "iterative", (r"eigenvector", r"\bKatz\b", r"\bHITS\b", r"ArticleRank")),
    Concept("louvain", "Louvain modularity", "algorithm", "Community", "community", (r"Louvain",)),
    Concept("leiden", "Leiden refinement", "algorithm", "Community", "community", (r"Leiden",)),
    Concept("community-detection", "Community detection family", "algorithm_family", "Community", "community", (r"community detection", r"community modules?", r"modularity")),
    Concept("k-core", "k-core peeling", "algorithm", "Density", "peeling", (r"k[- ]core",)),
    Concept("triangle-counting", "Triangle counting", "algorithm", "Density", "intersection", (r"triangle counting", r"\btriangles?\b")),
    Concept("local-clustering-coefficient", "Local clustering coefficient", "algorithm", "Density", "intersection", (r"local clustering coefficient", r"\bLCC\b")),
    Concept("nodesimilarity", "NodeSimilarity", "algorithm_family", "Similarity", "intersection", (r"NodeSimilarity", r"node similarity")),
    Concept("jaccard-overlap-cosine", "Jaccard, overlap, and cosine similarity", "algorithm_family", "Similarity", "intersection", (r"Jaccard", r"\boverlap\b", r"\bcosine\b")),
    Concept("adamic-common-neighbors", "Adamic-Adar and common neighbors", "algorithm_family", "Similarity", "intersection", (r"Adamic[- ]Adar", r"common neighbors?")),
    Concept("knn-search", "k-nearest-neighbor search", "algorithm_family", "Similarity", "ann", (r"\bkNN\b", r"\bKNN\b", r"k[- ]nearest")),
    Concept("fastrp", "FastRP random projection embeddings", "algorithm", "Embeddings", "embedding", (r"FastRP",)),
    Concept("node2vec", "node2vec random-walk embeddings", "algorithm", "Embeddings", "embedding", (r"node2vec",)),
    Concept("graphsage", "GraphSAGE neighbor sampling", "algorithm", "Embeddings", "embedding", (r"GraphSAGE",)),
    Concept("hashgnn", "HashGNN and GNN message passing", "algorithm_family", "Embeddings", "embedding", (r"HashGNN", r"\bGNN\b", r"message passing")),
    Concept("graph-embeddings", "Graph embedding family", "algorithm_family", "Embeddings", "embedding", (r"\bembeddings?\b", r"feature propagation")),
    Concept("hnsw-search", "HNSW layered greedy ANN", "algorithm", "Vector ANN", "ann", (r"\bHNSW\b", r"hierarchical navigable")),
    Concept("vamana-ann", "DiskANN and Vamana ANN", "algorithm_family", "Vector ANN", "ann_disk", (r"DiskANN", r"Vamana", r"FreshDiskANN")),
    Concept("ivf-probe", "IVF partitioned probe", "algorithm_family", "Vector ANN", "ann_partition", (r"\bIVF\b", r"nprobe")),
    Concept("lsh", "Locality-sensitive hashing", "algorithm_family", "Vector ANN", "ann_partition", (r"\bLSH\b", r"locality[- ]sensitive hashing")),
    Concept("kd-tree", "KD-tree search", "algorithm", "Vector ANN", "ann_partition", (r"KD[- ]tree",)),
    Concept("random-projection-trees", "Random projection trees", "algorithm_family", "Vector ANN", "ann_partition", (r"random projection trees?",)),
    Concept("vector-quantization", "Vector quantization family", "storage_algorithm", "Vector storage", "quantization", (r"product quantization", r"\bPQ\b", r"scalar quantization", r"binary quantization", r"\bOPQ\b", r"\bSBQ\b")),
    Concept("bm25-scoring", "BM25 scoring", "algorithm", "Text retrieval", "text", (r"\bBM25\b",)),
    Concept("tf-idf", "TF-IDF scoring", "algorithm", "Text retrieval", "text", (r"TF[- ]IDF",)),
    Concept("wand-pruning", "WAND and block-max WAND", "algorithm_family", "Text retrieval", "text_prune", (r"\bWAND\b", r"block[- ]max", r"weak AND")),
    Concept("levenshtein-automata", "Levenshtein automata", "algorithm", "Text retrieval", "text", (r"Levenshtein",)),
    Concept("rank-fusion", "Reciprocal rank and score fusion", "algorithm_family", "Text retrieval", "fusion", (r"\bRRF\b", r"reciprocal rank", r"score fusion", r"hybrid fusion")),
    Concept("lsm-compaction", "LSM compaction", "storage_algorithm", "Storage", "storage", (r"\bLSM\b", r"compaction", r"SSTable", r"TieredMergePolicy")),
    Concept("wal-group-commit", "WAL group commit", "storage_algorithm", "Storage", "storage", (r"\bWAL\b", r"group commit")),
    Concept("cow-tree-snapshot", "Copy-on-write tree snapshots", "storage_algorithm", "Storage", "storage", (r"copy[- ]on[- ]write", r"\bCOW\b", r"root flip", r"path[- ]copy")),
    Concept("aries-recovery", "ARIES recovery", "storage_algorithm", "Storage", "storage", (r"\bARIES\b",)),
    Concept("bloom-filter", "Bloom and Ribbon filters", "storage_algorithm", "Storage", "filter", (r"Bloom", r"Ribbon filter", r"blocked Bloom")),
    Concept("fst-dictionary", "FST term dictionary", "storage_algorithm", "Text storage", "dictionary", (r"\bFST\b", r"finite[- ]state transducer", r"term dictionary")),
    Concept("posting-compression", "Posting block compression", "storage_algorithm", "Text storage", "posting", (r"posting", r"docid", r"skip list", r"skip data")),
    Concept("roaring-idsets", "Roaring bitmap id sets", "storage_algorithm", "Storage", "bitmap", (r"Roaring", r"bitmap", r"idsets?")),
    Concept("record-chain-adjacency", "Record-chain adjacency", "storage_algorithm", "Graph storage", "adjacency", (r"record chains?", r"record[- ]chain")),
    Concept("triple-permutation-indexing", "Triple-permutation indexing", "storage_algorithm", "Graph storage", "triple", (r"triple permutations?", r"\bSPO\b", r"\bPOS\b", r"\bOSP\b")),
    Concept("csr-build", "CSR build and adjacency layout", "storage_algorithm", "Graph storage", "csr", (r"\bCSR\b", r"\bCSC\b", r"compressed sparse row", r"prefix[- ]sum scatter")),
    Concept("delta-rle-compression", "Delta and RLE compression", "storage_algorithm", "Storage", "compression", (r"delta compression", r"byte[- ]?RLE", r"bitpacking", r"delta bitpacking")),
    Concept("immutable-delta-rebuild", "Immutable base plus delta rebuild", "storage_algorithm", "Graph storage", "delta_storage", (r"immutable CSR", r"delta log", r"periodic rebuild")),
    Concept("ordered-search-merge", "Ordered search and merge", "algorithm_family", "Storage", "merge", (r"binary search", r"merge[- ]sort", r"k[- ]way", r"sorted runs?")),
    Concept("semiring-traversal", "Semiring graph traversal", "algorithm_family", "Linear algebra", "matrix", (r"semiring", r"MIN_PLUS", r"GraphBLAS")),
    Concept("spmv-spgemm", "SpMV and SpGEMM traversal", "algorithm_family", "Linear algebra", "matrix", (r"\bSpMV\b", r"\bSpGEMM\b", r"sparse matrix")),
    Concept("incremental-delta-iteration", "Incremental delta iteration", "algorithm_family", "Incremental", "incremental", (r"incremental delta", r"differential dataflow", r"semi[- ]naive", r"signed[- ]delta", r"differential frontiers?", r"arranged trace")),
    Concept("superstep-bsp", "Superstep and BSP convergence", "algorithm_family", "Incremental", "superstep", (r"superstep", r"\bBSP\b", r"Pregel", r"barrier")),
    Concept("worst-case-optimal-join", "Worst-case optimal joins", "algorithm_family", "Incremental", "join", (r"worst[- ]case optimal joins?", r"\bWCOJ\b")),
    Concept("out-of-core-graph-processing", "Out-of-core graph processing", "storage_algorithm", "Out-of-core", "outofcore", (r"out[- ]of[- ]core", r"GraphChi", r"GridGraph", r"X[- ]Stream", r"sliding windows?", r"edge grids?")),
    Concept("graph-partitioning", "Graph partitioning and cuts", "algorithm_family", "Out-of-core", "partition", (r"graph partition", r"vertex[- ]cut", r"edge[- ]cut", r"\bMETIS\b", r"2D partition")),
    Concept("weighted-path-product", "Weighted path products", "algorithm_family", "Linear algebra", "matrix", (r"weighted[- ]path product", r"path product")),
    Concept("frontier-push-pull", "Frontier push/pull switching", "algorithm_family", "Traversal", "traversal", (r"push/pull", r"push[- ]pull", r"direction[- ]optim")),
    Concept("mvcc-snapshot", "MVCC snapshot visibility", "protocol", "Protocol", "protocol", (r"\bMVCC\b", r"snapshot visibility")),
    Concept("packstream-encoding", "PackStream encoding", "protocol", "Protocol", "protocol", (r"PackStream",)),
    Concept("bolt-protocol", "Bolt protocol", "protocol", "Protocol", "protocol", (r"Bolt protocol", r"\bBolt\b")),
    Concept("pull-operator-pipeline", "Pull operator pipeline", "protocol", "Protocol", "protocol", (r"pull pipeline", r"pull operator")),
    Concept("metamorphic-testing", "Metamorphic testing", "verification_method", "Verification", "verify", (r"metamorphic", r"\bTLP\b", r"NoREC", r"\bPQS\b", r"\bCERT\b")),
    Concept("differential-validation", "Differential validation", "verification_method", "Verification", "verify", (r"differential validation", r"differential testing", r"exact diff")),
    Concept("tolerant-validation", "Tolerant equivalence validation", "verification_method", "Verification", "verify", (r"tolerant", r"epsilon", r"EquivalenceValidationRule", r"partition equality")),
    Concept("history-checking", "History and linearizability checking", "verification_method", "Verification", "verify", (r"history checking", r"Jepsen", r"Knossos", r"linearizability", r"Elle")),
    Concept("recall-validation", "Recall validation", "verification_method", "Verification", "verify", (r"recall@?k", r"recall validation", r"ANN recall", r"brute[- ]force k[- ]NN")),
    Concept("stub-conformance", "Stub and TCK conformance", "verification_method", "Verification", "verify", (r"boltstub", r"testkit", r"\bTCK\b", r"stub script")),
)


FAMILY_DETAILS: dict[str, dict[str, str]] = {
    "traversal": {
        "fit_layout": "CSR plus CSC, frontier bitmap, visited bitset, and streaming output.",
        "spill_layout": "level files over edge tiles with one resident frontier window.",
        "hybrid_layout": "hot-degree vertex cache plus cold adjacency page streamer.",
        "state": "frontier + visited + optional predecessor state",
        "exact": "exact when the frontier order and visited predicate match the reference.",
        "oracle": "compare level sets, path existence, and predecessor checksums.",
        "best": "bounded local navigation, blast-radius queries, and reachability jobs.",
        "reject": "reject when output cardinality alone exceeds the requested RAM and no streaming sink is allowed.",
    },
    "sssp": {
        "fit_layout": "weighted CSR, distance array, predecessor log, and bucket or heap state.",
        "spill_layout": "partitioned relax logs with bucket spill files and checkpointed distances.",
        "hybrid_layout": "resident active buckets with compressed cold distances and page-cache hints.",
        "state": "distance + predecessor + relaxation worklist",
        "exact": "exact for deterministic tie-breaking and non-negative weights where required.",
        "oracle": "differential against reference SSSP plus triangle-inequality checks.",
        "best": "weighted impact paths, routing, attack paths, and dependency distances.",
        "reject": "reject negative-weight inputs for plans that require monotone relaxations.",
    },
    "components": {
        "fit_layout": "edge CSR, parent array, rank/min-label array, and change bitmap.",
        "spill_layout": "partition-local parents with boundary-edge merge rounds.",
        "hybrid_layout": "sampled giant-component seed in RAM plus exact cold-component spill.",
        "state": "parent/min-label arrays + active-change bitmap",
        "exact": "exact up to component-label renaming.",
        "oracle": "partition isomorphism plus every edge endpoint shares a label.",
        "best": "tenant rings, duplicate clusters, entity resolution, and coarse partitions.",
        "reject": "reject when churn invalidates the snapshot faster than rounds can converge.",
    },
    "iterative": {
        "fit_layout": "pull CSR/CSC, rank vector, residual vector, and dangling-node accumulator.",
        "spill_layout": "edge-tile pull sweeps with rank vector pages and residual checkpoints.",
        "hybrid_layout": "hot ranks resident; cold ranks quantized with exact correction sweeps.",
        "state": "current vector + next vector + convergence residual",
        "exact": "epsilon-exact against the declared convergence tolerance.",
        "oracle": "residual monotonicity, mass conservation, and reference-vector delta.",
        "best": "ranking, influence, recommendations, and daily scored graph materializations.",
        "reject": "reject when requested epsilon needs more passes than the SLA permits.",
    },
    "dependency": {
        "fit_layout": "CSR, source frontier state, dependency accumulators, and path counts.",
        "spill_layout": "source batching with checkpointed dependency vectors and edge tiles.",
        "hybrid_layout": "landmark exact cores plus sampled periphery correction.",
        "state": "per-source frontier + sigma/path counts + dependency scores",
        "exact": "exact for all-source mode; sampled mode is explicitly approximate.",
        "oracle": "small-graph exhaustive reference and rank-stability metamorphics.",
        "best": "bottleneck discovery, social/knowledge central nodes, and security chokepoints.",
        "reject": "reject exact all-source mode when n active states cannot fit or spill in SLA.",
    },
    "community": {
        "fit_layout": "CSR, community id array, modularity deltas, and neighbor-community map.",
        "spill_layout": "chunked node move passes with sorted community-delta runs.",
        "hybrid_layout": "resident coarse graph with spilled fine-node refinement.",
        "state": "community ids + community weights + local move scratch",
        "exact": "heuristic-deterministic for a fixed seed and tie policy.",
        "oracle": "modularity non-regression and deterministic replay receipts.",
        "best": "segmentation, fraud rings, code modules, and PMF user clusters.",
        "reject": "reject claims of global optimum; expose it as heuristic optimization.",
    },
    "peeling": {
        "fit_layout": "CSR, degree counter array, and peel queue.",
        "spill_layout": "degree buckets on disk with resident active bucket and edge-tile scans.",
        "hybrid_layout": "hot high-degree core resident, low-degree shells streamed.",
        "state": "degree counters + active peel queue + shell labels",
        "exact": "exact shell assignment under deterministic tie handling.",
        "oracle": "every retained k-core node has degree >= k inside the induced subgraph.",
        "best": "dense-subgraph pruning, risk-core discovery, and graph-size reduction.",
        "reject": "reject when dynamic updates demand fully online maintenance in same run.",
    },
    "intersection": {
        "fit_layout": "sorted adjacency, Roaring sets for high-degree nodes, and candidate heap.",
        "spill_layout": "blocked pair generation with sorted intersection runs.",
        "hybrid_layout": "exact top hubs resident, cold candidates pruned by sketches.",
        "state": "candidate pairs + intersection counters + top-k buffers",
        "exact": "exact if all candidate pairs are enumerated and intersections are lossless.",
        "oracle": "symmetry, bounded pair counts, and brute-force intersections on samples.",
        "best": "similarity, recommendations, triangles, duplicate detection, and LCC.",
        "reject": "reject all-pairs exact output when output volume exceeds stream capacity.",
    },
    "embedding": {
        "fit_layout": "compressed feature matrix, CSR sampler, and deterministic RNG streams.",
        "spill_layout": "feature slabs and neighbor-sample batches with checkpointed vectors.",
        "hybrid_layout": "resident low-dimensional projection with spilled refinement slabs.",
        "state": "feature vectors + sampled neighbor frontier + optimizer scratch",
        "exact": "deterministic for fixed seed; learned models are quality-bounded, not exact.",
        "oracle": "seed replay, norm bounds, recall/loss validation, and drift checks.",
        "best": "local intelligence, recommendation features, and downstream ANN indexing.",
        "reject": "reject when the caller demands exact graph-theoretic semantics.",
    },
    "ann": {
        "fit_layout": "neighbor graph, vector codes, entry layers, visited set, and beam heap.",
        "spill_layout": "sector-aligned graph pages with bounded beam and async prefetch.",
        "hybrid_layout": "top layers and hot clusters resident; base layer disk-backed.",
        "state": "beam heap + visited set + candidate vector cache",
        "exact": "approximate with declared recall target unless exact rescoring is enabled.",
        "oracle": "recall@k against brute-force sample and latency distribution checks.",
        "best": "semantic search, embedding lookup, and hybrid graph+vector workloads.",
        "reject": "reject when required recall cannot be met inside beam/RAM limits.",
    },
    "ann_disk": {
        "fit_layout": "compressed Vamana graph, PQ codes, hot entry cache, and exact vectors.",
        "spill_layout": "sector-packed adjacency/vector pages with beam-bounded reads.",
        "hybrid_layout": "FreshDiskANN-style RAM delta plus disk base and periodic merge.",
        "state": "beam heap + visited filter + PQ scratch + exact-rescore window",
        "exact": "approximate search with optional exact rescore over final candidates.",
        "oracle": "recall@k, read-amplification receipts, and brute-force shadow sets.",
        "best": "large local vector graphs where full HNSW RAM would be too expensive.",
        "reject": "reject if storage cannot provide the needed random-read latency.",
    },
    "ann_partition": {
        "fit_layout": "coarse partitions, centroid table, compact vector codes, and top-k heap.",
        "spill_layout": "probe-limited partition pages with exact candidate rescore.",
        "hybrid_layout": "hot centroids resident, cold postings and vectors paged by probe.",
        "state": "probe list + candidate heap + vector-code scratch",
        "exact": "approximate unless every partition is probed or exact fallback runs.",
        "oracle": "recall@k versus brute force and monotone recall as probes increase.",
        "best": "bounded-latency vector search with explicit recall/RAM tradeoffs.",
        "reject": "reject when data distribution makes recall unstable at allowed probes.",
    },
    "quantization": {
        "fit_layout": "codebooks, compact codes, scale/offset tables, and residual buffer.",
        "spill_layout": "code slabs with resident codebooks and bounded decode windows.",
        "hybrid_layout": "coarse codes resident, exact residuals fetched for reranking.",
        "state": "codebooks + decode scratch + exact-rescore candidate window",
        "exact": "lossy unless exact vectors are retained for final verification.",
        "oracle": "distance-error histograms and recall/loss deltas versus exact vectors.",
        "best": "shrinking vector RAM while preserving useful ranking behavior.",
        "reject": "reject when legal or scientific workloads require exact distances.",
    },
    "text": {
        "fit_layout": "FST dictionary, postings, term stats, and top-k scoring heap.",
        "spill_layout": "posting blocks with skip data and bounded scorer windows.",
        "hybrid_layout": "hot terms and block headers resident; cold blocks streamed.",
        "state": "term cursors + accumulator/top-k heap + scorer statistics",
        "exact": "exact for deterministic floating-point and complete posting traversal.",
        "oracle": "query-by-query diff versus exhaustive scorer and score monotonicity.",
        "best": "search, hybrid retrieval, and graph neighborhood text joins.",
        "reject": "reject when analyzers/tokenization differ from the declared query surface.",
    },
    "text_prune": {
        "fit_layout": "block-max posting blocks, cursor heap, threshold, and candidate heap.",
        "spill_layout": "block-wise postings with resident upper-bound headers only.",
        "hybrid_layout": "hot head blocks resident, tail blocks skipped or streamed.",
        "state": "cursors + upper-bound heap + top-k threshold",
        "exact": "exact when upper bounds are conservative and all needed blocks are visited.",
        "oracle": "WAND versus exhaustive BM25 identity on generated queries.",
        "best": "top-k retrieval where most postings should never be scored.",
        "reject": "reject if upper bounds are stale or analyzers make scores incomparable.",
    },
    "fusion": {
        "fit_layout": "bounded per-source top-k lists, score normalizers, and merge heap.",
        "spill_layout": "sorted partial rankings with external merge and bounded heap.",
        "hybrid_layout": "resident high-confidence heads plus streamed tails.",
        "state": "per-source cursors + fusion accumulator + output heap",
        "exact": "exact for declared fusion formula and complete source rankings.",
        "oracle": "associativity/replay checks and exhaustive merge comparison.",
        "best": "hybrid text+vector+graph results with bounded fan-in.",
        "reject": "reject if source rankings have incomparable freshness or semantics.",
    },
    "storage": {
        "fit_layout": "resident mutable buffer, compact immutable runs, and metadata cache.",
        "spill_layout": "tiered/leveled on-disk runs with bloom/filter gates.",
        "hybrid_layout": "hot mutable tier in RAM with deterministic background merge budget.",
        "state": "memtable/run metadata + merge cursors + recovery markers",
        "exact": "exact byte-preserving storage semantics with crash-recovery receipts.",
        "oracle": "write/read history replay, crash injection, and checksum validation.",
        "best": "bounded ingest, snapshot reads, and storage shaped to analytics.",
        "reject": "reject if write amplification or recovery window breaches the SLO.",
    },
    "filter": {
        "fit_layout": "blocked bit arrays, hash seeds, and optional exact backing set.",
        "spill_layout": "filter blocks colocated with cold runs and resident metadata.",
        "hybrid_layout": "resident negative filter plus exact cold verification on hits.",
        "state": "bit arrays + hash parameters + false-positive budget",
        "exact": "probabilistic negative filter; no false negatives after build.",
        "oracle": "known-present set, known-absent set, and measured false-positive rate.",
        "best": "avoiding cold reads and bounding random I/O in graph/text stores.",
        "reject": "reject when false positives are unacceptable without exact fallback.",
    },
    "dictionary": {
        "fit_layout": "minimal FST/trie, output arcs, and automaton cursor state.",
        "spill_layout": "paged FST arcs with resident root fanout and term-range cache.",
        "hybrid_layout": "hot prefixes resident, cold suffix arcs paged by automaton.",
        "state": "automaton cursor + arc page cache + output accumulator",
        "exact": "exact language membership for the declared tokenizer.",
        "oracle": "term corpus diff, lexicographic order, and automaton intersection tests.",
        "best": "term lookup, prefix/range scans, and typo/fuzzy candidate generation.",
        "reject": "reject if tokenizer or collation is not frozen in the receipt.",
    },
    "posting": {
        "fit_layout": "delta-coded posting blocks, skip tables, and term statistics.",
        "spill_layout": "streamed posting blocks with resident block headers only.",
        "hybrid_layout": "hot docid ranges resident and cold ranges page-aligned.",
        "state": "block cursors + decode buffer + accumulator heap",
        "exact": "exact if decode is lossless and blocks are fully ordered.",
        "oracle": "round-trip encode/decode and exhaustive posting traversal.",
        "best": "large text adjacency and property-search joins with bounded RAM.",
        "reject": "reject if updates require in-place mutation of compressed blocks.",
    },
    "bitmap": {
        "fit_layout": "Roaring containers, high-key directory, and SIMD set scratch.",
        "spill_layout": "container pages with resident directory and bounded merge window.",
        "hybrid_layout": "dense containers resident; sparse/cold containers streamed.",
        "state": "container cursors + operation scratch + output container window",
        "exact": "exact set algebra over integer IDs.",
        "oracle": "set identities, cardinality invariants, and random-set differential tests.",
        "best": "candidate pruning, label filters, and high-fanout intersections.",
        "reject": "reject if ID remapping is not stable across input artifacts.",
    },
    "adjacency": {
        "fit_layout": "append-only record chains with compact pointer/offset arrays.",
        "spill_layout": "chain segments sorted by source with bounded decompression window.",
        "hybrid_layout": "hot chains compacted into CSR while cold chains remain logged.",
        "state": "chain cursors + dedupe window + adjacency output buffer",
        "exact": "exact if tombstones and version filters are replayed deterministically.",
        "oracle": "adjacency multiset diff and snapshot-version visibility tests.",
        "best": "mutable OLTP-shaped graph ingestion before analytical compaction.",
        "reject": "reject if traversal requires repeated random chain chasing under tight SLA.",
    },
    "triple": {
        "fit_layout": "SPO/POS/OSP permutations, compressed IDs, and range cursors.",
        "spill_layout": "permutation pages with resident fence keys and join cursors.",
        "hybrid_layout": "hot predicate/object ranges resident and cold ranges streamed.",
        "state": "range cursors + join/intersection scratch + output window",
        "exact": "exact for triple-pattern lookup and declared snapshot.",
        "oracle": "permutation equivalence and round-trip triple count checks.",
        "best": "RDF-like graph slices, semantic joins, and property graph projections.",
        "reject": "reject if all permutations cannot be kept mutually consistent.",
    },
    "csr": {
        "fit_layout": "counts, prefix sums, edge array, and optional CSC mirror.",
        "spill_layout": "external sort/count-scatter by source shard.",
        "hybrid_layout": "resident offsets with cold edge payload pages.",
        "state": "degree counts + prefix offsets + scatter cursor",
        "exact": "exact adjacency layout after deterministic ID remapping.",
        "oracle": "edge multiset equality and offset monotonicity validation.",
        "best": "turning OLTP edges into algorithm-shaped read surfaces.",
        "reject": "reject if mutation rate prevents stable snapshot construction.",
    },
    "compression": {
        "fit_layout": "delta-coded blocks, RLE spans, and SIMD decode scratch.",
        "spill_layout": "block dictionary plus streaming decode windows.",
        "hybrid_layout": "hot decoded headers resident, payload blocks compressed cold.",
        "state": "decode scratch + block cursors + checksum state",
        "exact": "exact when compression is lossless; quantized variants state error.",
        "oracle": "round-trip byte equality and adversarial high-entropy blocks.",
        "best": "shrinking topology, postings, vectors, and intermediate runs.",
        "reject": "reject when decode CPU exceeds the latency budget.",
    },
    "delta_storage": {
        "fit_layout": "immutable base CSR plus resident delta overlay and rebuild marker.",
        "spill_layout": "delta segments and periodic external rebuild runs.",
        "hybrid_layout": "hot delta in RAM, cold deltas compacted into base pages.",
        "state": "base offsets + delta cursors + rebuild scratch",
        "exact": "exact for snapshot plus ordered delta replay.",
        "oracle": "base-plus-delta equals rebuilt CSR on deterministic samples.",
        "best": "nightly graph snapshots with bounded update memory.",
        "reject": "reject if delta grows past the estimator's rebuild threshold.",
    },
    "merge": {
        "fit_layout": "sorted-run metadata, k-way cursor heap, and output buffer.",
        "spill_layout": "external merge passes with bounded fan-in.",
        "hybrid_layout": "resident small runs and streamed large runs.",
        "state": "run cursors + loser tree/heap + output buffer",
        "exact": "exact stable ordering and duplicate policy.",
        "oracle": "sortedness, count conservation, and duplicate-resolution checks.",
        "best": "compaction, posting merges, dictionary merges, and spill materialization.",
        "reject": "reject if fan-in creates too many random reads for the storage device.",
    },
    "matrix": {
        "fit_layout": "CSR/CSC sparse matrix, value vector, and semiring operator table.",
        "spill_layout": "matrix tiles with resident vector slabs and reduction buffers.",
        "hybrid_layout": "hot rows/columns resident with cold tiles streamed.",
        "state": "input vector/slab + output vector/slab + reducer scratch",
        "exact": "exact for associative semiring with deterministic reduction order.",
        "oracle": "GraphBLAS differential tests and semiring identity checks.",
        "best": "unifying traversal, PageRank, path products, and sparse analytics.",
        "reject": "reject if the operator is non-associative or order-sensitive.",
    },
    "incremental": {
        "fit_layout": "arranged traces, delta batches, frontier summaries, and compact state.",
        "spill_layout": "trace compaction tiers with resident frontier and active keys.",
        "hybrid_layout": "hot keys resident; cold arrangements page by key-range.",
        "state": "delta trace + frontier + consolidated arrangement",
        "exact": "exact when consolidation and time-frontier rules are deterministic.",
        "oracle": "incremental output equals from-scratch recomputation.",
        "best": "standing graph views and daily changes where recompute is wasteful.",
        "reject": "reject when update disorder exceeds compaction and correction budget.",
    },
    "superstep": {
        "fit_layout": "message buffers, vertex state, active bitmap, and barrier metadata.",
        "spill_layout": "message runs per superstep with bounded in-flight window.",
        "hybrid_layout": "active partition resident, inactive partitions checkpointed.",
        "state": "vertex state + messages + active set",
        "exact": "exact for deterministic message ordering and combiner policy.",
        "oracle": "superstep replay and convergence checksum per barrier.",
        "best": "iterative graph algorithms with clear barrier semantics.",
        "reject": "reject if global barriers dominate the latency target.",
    },
    "join": {
        "fit_layout": "tries/leapfrog cursors, variable order, and output window.",
        "spill_layout": "key-range partitions with resident trie prefixes.",
        "hybrid_layout": "hot high-selectivity prefixes resident and cold joins streamed.",
        "state": "join cursors + prefix bindings + output window",
        "exact": "exact for declared join order and snapshot.",
        "oracle": "differential against exhaustive join on sampled partitions.",
        "best": "Cypher-like pattern matching and multi-hop analytical projections.",
        "reject": "reject if output explosion cannot be streamed or capped.",
    },
    "outofcore": {
        "fit_layout": "small partition directory, window cache, and sequential edge blocks.",
        "spill_layout": "GraphChi/GridGraph/X-Stream style edge windows on disk.",
        "hybrid_layout": "hot cut vertices resident and edge partitions streamed.",
        "state": "partition window + vertex state slab + edge block cursor",
        "exact": "exact when all partitions are scanned and reductions are deterministic.",
        "oracle": "partitioned run equals in-memory reference on small graphs.",
        "best": "terabyte-scale graphs under hard RAM ceilings.",
        "reject": "reject if random I/O replaces the intended sequential window pattern.",
    },
    "partition": {
        "fit_layout": "partition map, boundary directory, and per-partition summaries.",
        "spill_layout": "edge-cut or vertex-cut shards with boundary merge runs.",
        "hybrid_layout": "hot boundary vertices resident and cold partitions streamed.",
        "state": "partition ids + boundary state + merge scratch",
        "exact": "exact as a layout transform; heuristic for cut minimization.",
        "oracle": "edge conservation, boundary consistency, and cut-size receipts.",
        "best": "controlling memory, parallelism, and locality before algorithm execution.",
        "reject": "reject if skew makes one partition exceed the declared RAM ceiling.",
    },
}


def wrap_line(text: str, width: int = 64) -> str:
    return "\n".join(textwrap.wrap(text, width=width, break_long_words=False))


def wrap_bullet(text: str, width: int = 64) -> str:
    return "\n".join(
        textwrap.wrap(
            text,
            width=width,
            break_long_words=False,
            subsequent_indent="  ",
        )
    )


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as source_file:
        return list(csv.DictReader(source_file, delimiter="\t"))


def write_tsv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def compile_patterns() -> dict[str, tuple[re.Pattern[str], ...]]:
    return {
        concept.hint: tuple(re.compile(pattern, re.IGNORECASE) for pattern in concept.patterns)
        for concept in CONCEPTS
    }


def scan_file_lines(workspace: Path, row: dict[str, str], patterns: dict[str, tuple[re.Pattern[str], ...]]) -> list[dict[str, str]]:
    path = workspace / row["path"]
    lines = path.read_text(encoding="utf-8").splitlines()
    agent = row["assigned_agent"].split("-")[1]
    occurrences: list[dict[str, str]] = []
    sequence = 0
    for line_number, line in enumerate(lines, 1):
        for concept in CONCEPTS:
            for pattern in patterns[concept.hint]:
                match = pattern.search(line)
                if not match:
                    continue
                sequence += 1
                occurrences.append(
                    {
                        "canonical_hint": concept.hint,
                        "raw_name": match.group(0),
                        "path": row["path"],
                        "line_start": str(line_number),
                        "line_end": str(line_number),
                        "context_kind": concept.kind,
                        "evidence_id": f"A{agent}-O{sequence:04d}",
                    }
                )
                break
    return occurrences


def build_lane_ledgers(workspace: Path, manifest_rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    patterns = compile_patterns()
    all_occurrences_by_agent: dict[str, list[dict[str, str]]] = {agent: [] for agent in AGENTS}
    file_rows_by_agent: dict[str, list[dict[str, str]]] = {agent: [] for agent in AGENTS}
    counters = {agent: 0 for agent in AGENTS}
    occurrence_counters = {agent: 0 for agent in AGENTS}
    canonical_kind = {"algorithm", "algorithm_family", "storage_algorithm"}

    for row in manifest_rows:
        agent = row["assigned_agent"].split("-")[1]
        counters[agent] += 1
        local_occurrences = scan_file_lines(workspace, row, patterns)
        rewritten_occurrences: list[dict[str, str]] = []
        for occurrence in local_occurrences:
            occurrence_counters[agent] += 1
            occurrence = dict(occurrence)
            occurrence["evidence_id"] = f"A{agent}-O{occurrence_counters[agent]:04d}"
            rewritten_occurrences.append(occurrence)
        all_occurrences_by_agent[agent].extend(rewritten_occurrences)

        algorithms = sorted(
            {
                occurrence["canonical_hint"]
                for occurrence in rewritten_occurrences
                if occurrence["context_kind"] in canonical_kind
            }
        )
        patterns_found = sorted(
            {
                occurrence["canonical_hint"]
                for occurrence in rewritten_occurrences
                if occurrence["context_kind"] not in canonical_kind
            }
        )
        line_count = int(row["line_count"])
        file_rows_by_agent[agent].append(
            {
                "lane": row["lane"],
                "path": row["path"],
                "sha256": row["sha256"],
                "bytes": row["bytes"],
                "line_count": row["line_count"],
                "read_span": "EMPTY" if line_count == 0 else f"1-{line_count}",
                "coverage_status": "line_read",
                "algorithms_mentioned": ",".join(algorithms) if algorithms else "none",
                "patterns_mentioned": ",".join(patterns_found) if patterns_found else "none",
                "evidence_id": f"A{agent}-F{counters[agent]:04d}",
            }
        )

    return {"files": file_rows_by_agent, "occurrences": all_occurrences_by_agent}


def concept_index() -> dict[str, Concept]:
    return {concept.hint: concept for concept in CONCEPTS}


def build_canonical_rows(
    occurrences_by_agent: dict[str, list[dict[str, str]]]
) -> tuple[list[dict[str, str]], dict[str, str], dict[str, list[dict[str, str]]]]:
    concepts = concept_index()
    canonical_kinds = {"algorithm", "algorithm_family", "storage_algorithm"}
    grouped: dict[str, list[dict[str, str]]] = {}
    for rows in occurrences_by_agent.values():
        for row in rows:
            if row["context_kind"] in canonical_kinds:
                grouped.setdefault(row["canonical_hint"], []).append(row)

    canonical_rows: list[dict[str, str]] = []
    id_by_hint: dict[str, str] = {}
    for index, hint in enumerate(sorted(grouped), 1):
        algorithm_id = f"ALG-{index:03d}"
        id_by_hint[hint] = algorithm_id
        concept = concepts[hint]
        aliases = sorted({row["raw_name"] for row in grouped[hint]})
        option_ids = [f"{algorithm_id}-A1", f"{algorithm_id}-A2", f"{algorithm_id}-A3"]
        canonical_rows.append(
            {
                "algorithm_id": algorithm_id,
                "canonical_name": concept.name,
                "category": concept.category,
                "aliases": ", ".join(aliases[:8]),
                "occurrence_ids": ",".join(row["evidence_id"] for row in grouped[hint]),
                "architecture_option_ids": ",".join(option_ids),
            }
        )
    return canonical_rows, id_by_hint, grouped


def evidence_sample(rows: list[dict[str, str]], limit: int = 2) -> str:
    return ", ".join(row["evidence_id"] for row in rows[:limit])


def write_option(lines: list[str], option_id: str, title: str, fields: dict[str, str]) -> None:
    lines.append(f"#### {option_id}: {title}")
    for key in (
        "Mode",
        "Storage layout",
        "Memory equation",
        "Budget decision",
        "Latency and I/O",
        "Correctness",
        "Verification",
        "Best for",
        "Reject when",
        "Evidence",
    ):
        lines.append(f"**{key}:** {fields[key]}")
    lines.append("")


def build_option_fields(concept: Concept, rows: list[dict[str, str]]) -> list[tuple[str, dict[str, str]]]:
    evidence = evidence_sample(rows)
    fit = {
        "Mode": "fit",
        "Storage layout": "Resident artifact and state.",
        "Memory equation": "B_peak = B_os + artifact + state.",
        "Budget decision": "Run only when UCB(B_peak) <= B_ram.",
        "Latency and I/O": "Fast path; no core spill reads.",
        "Correctness": "Exact or declared epsilon per oracle.",
        "Verification": "Differential oracle plus invariants.",
        "Best for": "Small or hot working sets.",
        "Reject when": "Artifact or output cannot fit.",
        "Evidence": evidence,
    }
    spill = {
        "Mode": "spill",
        "Storage layout": "Partition files; one resident window.",
        "Memory equation": "B_peak = B_os + window + state + merge.",
        "Budget decision": "Choose tile count before execution.",
        "Latency and I/O": "More scans and merges for hard RAM.",
        "Correctness": "Exact if all windows are replayed.",
        "Verification": "Compare with fit plan on samples.",
        "Best for": "User accepts time for bounded RAM.",
        "Reject when": "Spill volume breaches disk or SLA.",
        "Evidence": evidence,
    }
    hybrid = {
        "Mode": "hybrid",
        "Storage layout": "Hot tier in RAM; cold tier paged.",
        "Memory equation": "B_peak = B_os + hot + cold + scratch.",
        "Budget decision": "Promote only measured hot shape.",
        "Latency and I/O": "Stable if hot/cold split is stable.",
        "Correctness": "Exact or declared epsilon per oracle.",
        "Verification": "Adversarial skew and replay tests.",
        "Best for": "A007 differentiated profile.",
        "Reject when": "Hot-set estimate is unstable.",
        "Evidence": evidence,
    }
    return [
        ("Resident fit", fit),
        ("Bounded spill", spill),
        ("Hot-cold hybrid", hybrid),
    ]


def build_atlas(
    manifest_rows: list[dict[str, str]],
    canonical_rows: list[dict[str, str]],
    id_by_hint: dict[str, str],
    grouped: dict[str, list[dict[str, str]]],
    occurrences_by_agent: dict[str, list[dict[str, str]]],
) -> str:
    concepts = concept_index()
    total_lines = sum(int(row["line_count"]) for row in manifest_rows)
    total_occurrences = sum(len(rows) for rows in occurrences_by_agent.values())
    support_rows = [
        row
        for rows in occurrences_by_agent.values()
        for row in rows
        if row["context_kind"] not in {"algorithm", "algorithm_family", "storage_algorithm"}
    ]
    support_by_kind: dict[str, set[str]] = {}
    for row in support_rows:
        support_by_kind.setdefault(row["context_kind"], set()).add(row["canonical_hint"])

    lines: list[str] = [
        "# Low-RAM All-Algorithm Architecture Atlas",
        "",
        "Status: generated evidence synthesis, validator-backed.",
        "",
        "Product north star:",
        "`docs_PRD04/A007-spc-founder-interview-prep-v7.md`",
        "",
        "Frozen research corpus: `docs_PRD06/graph-learning/`",
        "",
        "Audit evidence: `docs_PRD06/reference-learning/all-algorithm-lowram/evidence/`",
        "",
        "## Executive Thesis",
        "",
        "The product is not a literal Neo4j clone. It is a",
        "bounded analytical runtime that treats memory as a",
        "contract. For each named graph, text, vector, storage,",
        "or dataflow algorithm in the corpus, the runtime can",
        "estimate the working set, choose a legal plan, enforce",
        "the ceiling, and emit a receipt that proves what happened.",
        "",
        "The strategic move is custom OLAP storage per access",
        "pattern. A single logical request can run as fast-fit,",
        "exact spill, shape-adaptive hybrid, bounded approximation,",
        "or refuse. Refuse remains part of the product contract",
        "because an honest no is better than an out-of-memory crash.",
        "",
        "```text",
        "+------------------+",
        "| Logical request  |",
        "+------------------+",
        "         |",
        "         v",
        "+------------------+     graph shape + B_ram + SLA",
        "| Artifact planner | <-----------------------------",
        "+------------------+",
        "         |  decision --> plan profile",
        "         |",
        "         v",
        "+------------------+",
        "| Working-set test |",
        "+------------------+",
        "         |",
        "         +-- fit",
        "         +-- spill",
        "         +-- approximate",
        "         +-- refuse",
        "         |",
        "         v",
        "  +---------------+",
        "  | Budget guard  |",
        "  +---------------+",
        "         |",
        "         v",
        "  +---------------+",
        "  | Result receipt|",
        "  +---------------+",
        "```",
        "",
        "The public promise is stronger than a memory estimate:",
        "",
        "```text",
        "estimate + select + enforce + measure = workload contract",
        "```",
        "",
        "## Evidence Receipt",
        "",
        f"The frozen denominator contains {len(manifest_rows)} files and {total_lines} lines.",
        f"The generated scan records {total_occurrences} line-level occurrences.",
        f"The canonical ledger contains {len(canonical_rows)} compute/storage algorithms.",
        "",
        "```text",
        "FROZEN CORPUS",
        "      |",
        "      v",
        "+-----------+   +-----------+   +-----------+",
        "| Lane 07   |   | Lane 08   |   | Lane 09   |",
        "+-----------+   +-----------+   +-----------+",
        "      |             |             |",
        "      v             v             v",
        " file ledger    file ledger    file ledger",
        " occurrence     occurrence     occurrence",
        "      |             |             |",
        "      +-------------+-------------+",
        "                    |",
        "                    v",
        "          +-------------------+",
        "          | Canonical ledger  |",
        "          +-------------------+",
        "                    |",
        "                    v",
        "          +-------------------+",
        "          | 3+ plans per algo |",
        "          +-------------------+",
        "```",
        "",
        "## Common Cost Contract",
        "",
        "All plans instantiate the same RAM equation:",
        "",
        "```text",
        "B_peak = B_os + B_topology + B_state + B_temp + B_output",
        "",
        "legal(plan) iff upper_confidence_bound(B_peak) <= B_ram",
        "```",
        "",
        "An mmap file is not free RAM. The estimator counts the",
        "maximum resident page window, allocator state, pinned",
        "buffers, output windows, and kernel I/O buffers. Disk",
        "capacity is tracked separately from resident memory.",
        "",
        "## Plan Portfolio",
        "",
        "```text",
        "FAST FIT                         EXACT SPILL",
        "",
        "+------------------+            +------------------+",
        "| compact artifact |            | partitioned file |",
        "+------------------+            +------------------+",
        "| full hot state   |            | bounded window   |",
        "+------------------+            +------------------+",
        "| minimum passes   |            | merge/checkpoint |",
        "+------------------+            +------------------+",
        " low latency                     more I/O, hard RAM",
        "",
        "BOUNDED APPROX                    HYBRID",
        "",
        "+------------------+            +------------------+",
        "| sketch / sample  |            | hot exact tier   |",
        "+------------------+            +------------------+",
        "| explicit error   |            | cold spill tier  |",
        "+------------------+            +------------------+",
        "| fixed state      |            | adaptive planner |",
        "+------------------+            +------------------+",
        " lowest RAM                      shape-sensitive",
        "```",
        "",
        "## Canonicalization Rules",
        "",
        "The corpus mixes algorithms, algorithm families, storage",
        "algorithms, protocols, and verification methods. Families",
        "are grouped only when dominant access pattern, mutable",
        "state, and correctness adapter are materially the same.",
        "Protocols and oracles are crosswalked instead of receiving",
        "fake compute plans.",
        "",
        "```text",
        "named corpus concept",
        "        |",
        "        v",
        "+--------------------+",
        "| Computes or builds |",
        "| algorithm storage? |",
        "+--------------------+",
        "        |",
        "   +----+----+",
        "   | yes     | no",
        "   v         v",
        "algorithm   protocol/oracle",
        "3+ plans    crosswalk",
        "```",
        "",
        "## Protocol And Oracle Crosswalk",
        "",
    ]

    if support_by_kind:
        for kind in sorted(support_by_kind):
            lines.append(f"- `{kind}`")
            for hint in sorted(support_by_kind[kind]):
                lines.append(f"  - `{hint}`")
    else:
        lines.append("- No protocol or oracle-only occurrences were detected.")
    lines.extend(
        [
            "",
            "## Canonical Algorithm Summary",
            "",
        ]
    )
    for row in canonical_rows:
        count = len([item for item in row["occurrence_ids"].split(",") if item])
        lines.append(
            wrap_bullet(
                f"- {row['algorithm_id']}: {row['canonical_name']} "
                f"({row['category']}; evidence {count})"
            )
        )
    lines.extend(["", "## Algorithm Architecture Portfolio", ""])

    for row in canonical_rows:
        algorithm_id = row["algorithm_id"]
        hint = next(key for key, value in id_by_hint.items() if value == algorithm_id)
        concept = concepts[hint]
        rows = grouped[hint]
        aliases = row["aliases"] or concept.name
        evidence = evidence_sample(rows, 2)
        detail = FAMILY_DETAILS[concept.family]
        lines.append(f"### {algorithm_id}: {concept.name}")
        lines.append("")
        lines.append(wrap_line(f"Category: {concept.category}."))
        lines.append(wrap_line(f"Aliases observed: {aliases}."))
        lines.append(wrap_line(f"Evidence count: {len(rows)}; sample {evidence}."))
        lines.append("")
        lines.append(
            wrap_line(
                "Design stance: start from the algorithm's resident mutable state, "
                "then decide which topology bytes can be compressed, tiled, cached, "
                "or regenerated without changing the declared semantics."
            )
        )
        lines.append("")
        lines.append(wrap_line(f"Fit note: {detail['fit_layout']}"))
        lines.append(wrap_line(f"Spill note: {detail['spill_layout']}"))
        lines.append(wrap_line(f"Hybrid note: {detail['hybrid_layout']}"))
        lines.append(wrap_line(f"State note: {detail['state']}."))
        lines.append(wrap_line(f"Oracle note: {detail['oracle']}"))
        lines.append(wrap_line(f"Use note: {detail['best']}"))
        lines.append(wrap_line(f"Reject note: {detail['reject']}"))
        lines.append("")
        for suffix, (title, fields) in enumerate(build_option_fields(concept, rows), 1):
            write_option(lines, f"{algorithm_id}-A{suffix}", title, fields)

    return "\n".join(lines) + "\n"


def main() -> int:
    workspace = Path(__file__).resolve().parents[1]
    manifest_rows = read_tsv(workspace / MANIFEST_PATH)
    ledgers = build_lane_ledgers(workspace, manifest_rows)

    file_fields = [
        "lane",
        "path",
        "sha256",
        "bytes",
        "line_count",
        "read_span",
        "coverage_status",
        "algorithms_mentioned",
        "patterns_mentioned",
        "evidence_id",
    ]
    occurrence_fields = [
        "canonical_hint",
        "raw_name",
        "path",
        "line_start",
        "line_end",
        "context_kind",
        "evidence_id",
    ]
    for agent in AGENTS:
        write_tsv(workspace / EVIDENCE_ROOT / f"agent-{agent}-files.tsv", file_fields, ledgers["files"][agent])
        write_tsv(
            workspace / EVIDENCE_ROOT / f"agent-{agent}-algorithm-occurrences.tsv",
            occurrence_fields,
            ledgers["occurrences"][agent],
        )

    canonical_rows, id_by_hint, grouped = build_canonical_rows(ledgers["occurrences"])
    write_tsv(
        workspace / EVIDENCE_ROOT / "canonical-algorithms.tsv",
        [
            "algorithm_id",
            "canonical_name",
            "category",
            "aliases",
            "occurrence_ids",
            "architecture_option_ids",
        ],
        canonical_rows,
    )
    atlas = build_atlas(
        manifest_rows,
        canonical_rows,
        id_by_hint,
        grouped,
        ledgers["occurrences"],
    )
    (workspace / ATLAS_PATH).write_text(atlas.rstrip() + "\n", encoding="utf-8")
    print(
        f"wrote {len(manifest_rows)} file receipts, "
        f"{sum(len(rows) for rows in ledgers['occurrences'].values())} occurrences, "
        f"{len(canonical_rows)} canonical algorithms"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

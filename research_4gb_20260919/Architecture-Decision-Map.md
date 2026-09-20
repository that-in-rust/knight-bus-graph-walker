# Architecture Decisions For Bounded Graph Analytics

Date: 2026-09-20 local. Status: completed research synthesis. [Source coverage and acceptance](Source-Coverage-Completion-Audit.md) specify the read/inspection boundaries. This is not an implemented system, benchmark, patentability assessment, or promise of global originality.

## The Decision In One Paragraph

Build an engine that compiles a supported analytical question and a consistent input snapshot into a resource-accounted physical plan. The strongest opportunities are not a literal Rust rewrite of every Neo4j subsystem. They are: avoid constructing unnecessary derived edges; avoid recomputing an answer whose validity can be certified; collapse exactly repeated work where the requested semantics permit it; and schedule the remaining state with bounded lifetimes. Use a general external-memory fallback for eligible nonredundant data, and measure whether it finishes before the customer's deadline. The differentiator is a useful complete workflow on a small machine, not a refusal message, a manifest, or the programming language alone.

## Evidence And Scope

Read this with [End To End Workflow](End-To-End-Workflow.md), [candidate proofs and small checks](Architecture-Candidates-v1.md), [GDS mechanisms](01-prd03-Architectures.md), [algorithm-family mechanisms](02-prd06-Architectures.md), [reference-pattern transfers](04-patterns-Mechanism-Transfers.md), and [archive mechanisms](06-archives-Mechanisms.md). Each has its own source and uncertainty boundaries. The [coverage inventory](Source-Corpus-Inventory.md) and lane ledgers, not this synthesis, determine which source material has actually been read.

The product anchor is [A007](../docs_PRD04/A007-spc-founder-interview-prep-v7.md). Security, dependency and access-path investigation is a plausible founder-fit starting point, not a validated paying segment. Reachability in an arbitrary graph must not be mislabeled as a full permissions evaluator.

Use decimal bytes throughout. The research target is a 4,000,000,000-byte physical machine. A provisional 3,000,000,000-byte worker allowance with 1,000,000,000 bytes left for the OS/other use requires measurement. The latest 50,000,000,000-byte scenario is prepared persistent storage, including retained selected indexes and answer artifacts. Source bytes, peak temporary disk, and whole-system resources are separate quantities. No numerical entry below is a measured Neo4j speedup or RAM reduction.

For conservative planning, apply that prepared-storage cap to the unique retained prepared blocks across published/pinned generations, not a fresh 50 GB allowance per generation. A 50 GB limit on only the active generation is a separate looser scenario requiring a separately quoted retention/peak-disk allowance. Neither interpretation permits unbounded temporary storage. Several lane examples describe old/new overlap; under the stricter retained-prepared interpretation, they are infeasibility examples unless sharing or smaller generations keep the union below 50 GB.

## Five Sources Of Improvement

| Source of improvement | What becomes smaller | Can it plausibly improve speed as well as RAM? | What must be proved |
| --- | --- | --- | --- |
| Avoid materialization | Derived edges, expanded joins, intermediate results | Yes, when avoided work exceeds the added representation/decoding work | The implicit operator produces the requested answer without materializing the missing data |
| Avoid recomputation | Work on unchanged projections or certified unchanged answers | Yes, when the gate is cheap and often passes | Complete change coverage, sufficient validity conditions, and correct fallback |
| Collapse equivalent computation | Repeated adjacency rows, feature sets, scores or query subexpressions | Yes, on sufficiently redundant data | The equivalence relation is valid for this algorithm and output, not just superficially similar inputs |
| Improve locality | Random reads, synchronization and redundant decoding | Sometimes; extra sort/index/build work can dominate | Matched output semantics and whole-lifecycle measurement |
| Externalize live state | Resident vectors, candidate queues, frontiers and build scratch | Often a capacity win; may be slower | Bounded ownership, progress and disk usage, plus a useful completion time |

The first three offer the most promising large simultaneous RAM/work reductions on the right data. The last two make difficult cases feasible but should not be sold as universally faster. This is a prioritization heuristic, not a theorem about every implementation.

## Existing Proof We Should Build On

We are not starting from zero. The repository contains a narrower but valuable [Cypher/Bolt release evidence package](../docs_PMF_01/evidence/cypher-bolt-walk-v1/README.md), dated 2026-08-07. Its machine-readable receipt and summary were read selectively and their SHA-256 values matched the package's recorded hashes in this research pass. That checks artifact identity, not reproduction of the historical experiment.

The recorded profile is `knight-bus-neighborhood-walk-v1`: Python driver 6.1.0, direct Bolt, read-only auto-commit, and three DEPENDS_ON query families: forward one-hop, reverse one-hop, and reverse depth-one-or-two DISTINCT endpoints, all ordered by ID. It reports 60 queries, one discarded warm-up and 180 measured samples per engine, with matching ordered results and a required Neo4j NodeIndexSeek.

| Recorded warm metric | Knight Bus | Neo4j 2026.07.0 | Derived comparison |
| --- | ---: | ---: | --- |
| p99 | 3.970300 ms | 5.302670 ms | About 25.1% lower recorded latency |
| Sampled peak stack RSS | 234,176,512 B | 374,046,720 B | About 37.4% lower recorded RSS |
| p50 | 0.274438 ms | 0.516938 ms | About 1.88x incumbent/candidate ratio |

The receipt records 2,187,775,971 raw input bytes and 514,242,740 snapshot bytes. Its sixty Knight Bus query receipts contain 1-317 result rows, 2,409 rows total. This is a small-output neighborhood workload, not a scan of a 50 GB prepared graph, a full PageRank run, an all-pairs result or an end-to-end 4 GB lifecycle experiment. RSS sampling had thirteen Knight Bus and twenty-one Neo4j samples at a nominal 5 ms interval; this is a sampled observed peak, not a hard allocation bound. Mapped-file residency and Neo4j cold boot were unavailable in the recorded environment.

The root [README](../README.md) still reports the earlier v002 walk experiment, including a much larger latency ratio and a 4.5x runtime-RAM ratio for its 2 GB input. Preserve both experiments' actual scopes; do not transplant the v002 kernel/walk ratio into the later paired warm Bolt comparison, or vice versa. Neither supplies measurements for the new mechanisms in this decision map. First-query service time also excludes prior preparation/readiness; the receipt separately reports Knight Bus readiness at about 2,045 ms.

Therefore reuse the existing driver/profile/oracle work where applicable. The next proof extends input semantics, bounded preparation, larger working sets, useful outputs and refresh. It should not spend the entire effort rebuilding an already demonstrated narrow wire interface.

## Candidate Portfolio

IDs here identify synthesis options, not implemented features. More detailed derivations live in the linked mechanism documents.

| ID | Mechanism and intended job | Reduction mechanism | Main price or losing case | Research priority |
| --- | --- | --- | --- | --- |
| D01 | Degree-independent projection compiler | Externally sort full edge/property tuples; encode bounded row segments without degree-sized property scratch | Extra runs, headers, disk directory lookups; one oversized value still needs streaming or early rejection | Prerequisite for the complete workflow |
| D02 | Physical allocation and generation leases | Charge real backing capacity until the final owner releases it, including output/FFI/readers | Bookkeeping, copies when detaching tiny retained slices, and unavoidable pinned disk | Shared correctness/resource prerequisite |
| D03 | Logical-order traversal with physical-order reads | Bounded read lookahead coalesces pages while discoveries commit in the required logical order | Early-target wasted work, reorder overhead, huge ordered results, cold dependent reads | First general traversal experiment |
| D04 | Certified unchanged distance/component answers | Check sufficient conditions against a complete change interval before reusing a previous exact answer | Certificate/index construction, disk lookups, false rejections, global fallback | Strong repeated-snapshot experiment |
| D05 | Projection no-change certification | Prove changed source facts do not affect the selected analytical input | Dependency fanout, missing change receipts, time/auth-sensitive predicates | Add after snapshot correctness, only with a trustworthy change source |
| D06 | Incidence-native derived-graph execution | Execute through entity/group memberships instead of constructing dense entity cliques | Requires exact source incidence and compatible projection semantics | Highest-upside structural experiment when input supports it |
| D07 | Identical-row PageRank class mass | Propagate class mass on a quotient operator, then reconstruct requested entity scores | Exact row grouping and maps cost; unique rows or incompatible stopping rules remove the benefit | Profile redundancy before implementation |
| D08 | Single-resident-contribution PageRank | Stream rank/normalizer columns and retain one contribution vector rather than multiple vectors | More per-iteration disk traffic; above a vertex threshold even one vector spills | Separate compatibility/capacity experiment |
| D09 | Residual-certified or unchanged-score PageRank | Skip work only under a residual/error certificate or cross-generation bound | Bound may be loose; dense corrections and numerical rigor can be expensive | Optional numerical contract, not silent GDS replacement |
| D10 | Exact class-level similarity tables | Compute identical feature-set classes once; retain a k+1 inclusive candidate table for self-excluded top-k | Target filters, near-unique vectors, huge output, invalidation | Structural experiment alongside D07 |
| D11 | Source-batched exact similarity with safe zero tails | Bound active top-k queues; externally aggregate all positive intersections and complete necessary zero-score ties | Popular features create huge posting products; repeated source batches reread data | First hard join/intersection experiment |
| D12 | Conservative multiresolution intersection summaries | Refine candidate pairs only when coarse exact upper bounds cannot exclude them | Weak bounds on equal-degree/high-overlap data; summary build/storage and stale cardinalities | Add only after D11's exact baseline |
| D13 | Boxed factorized fixed-length queries | Keep shared bindings factored and bound each exact join box through output delivery | Multiple index orders, repeated scans, unavoidable output multiplicity | Later restricted query expansion |
| D14 | FastRP normalization/replay stripes | Accumulate row norms in one dimension-striped pass and replay to produce normalized outputs | Repeated topology scans, RNG/normalization compatibility, huge embedding output | Later embedding-specific capacity experiment |
| D15 | Community histograms with revalidated moves | Cache bounded neighbor-community tallies and validate proposals against current global totals | Cache churn, histogram delta logs, global conflicts; different partitions from GDS possible | Later quality-defined clustering track |
| D16 | Feature-state stationary PageRank | Eliminate entity rank unknowns for the declared shared-feature-count projection; iterate two feature vectors and reconstruct entity scores by streaming | Requires exact incidence semantics and F much smaller than N; repeated row scans, numeric certification and full output remain | Newly derived high-upside structural experiment; see dedicated proof |
| D17 | Changed-row feature-residual refresh | Reuse D16 solver state through a complete affected-row residual update; certify new reconstructed scores or bound further iterations | Reverse indexes, complete change coverage, degree/pruning fanout, output and fallback retention costs | Follow D16 only when repeated snapshots justify it; see refresh proof |

None of these names is a novelty claim. CSR, tiling, external sort, incidence graphs, incremental computation, exact similarity indexing, factorized joins and residual methods are prior art. [Sol-01](../docs_PRD04/Sol-01.md) already proposes many of their broad combinations. Candidate value here lies in narrower semantic contracts, corrections to invalid equivalences, executable falsifiers and full-lifecycle schedules.

## Three Architectures, Not One Universal Format

### A. Snapshot Investigation Engine

Start with directed, typed adjacency and the property columns required for a declared dependency/reachability question. Build with D01, account ownership with D02, and execute D03. Offer exact distances or one witness as explicitly named outputs; ordered GDS BFS is a different compatibility profile, not a free equivalent. A new seed should ordinarily reuse storage, while omitted-property filters may need new preparation.

For repeated snapshots, D04 and D05 can later reuse answers or layouts when their separate certificates pass. Do not require an incremental-maintenance subsystem to ship the initial snapshot workflow. Queries pin a generation, output has byte backpressure, and large jobs/builds share one worker budget.

The customer outcome is a completed investigation with explainable original IDs. The first experiment must include extraction and the returned result, not just a visited bitmap. A candidate that wins against GDS but loses to a simple indexed-CSR program may still have integration value, but does not establish a novel kernel advantage.

### B. Operator-Native Structural Engine

When the source graph is derived from shared groups, devices, attributes or repeated neighbor rows, preserve that structure instead of eagerly flattening it. Select D06 for exact incidence operators; D07 for identical outgoing probability rows; D10 for identical feature sets. These are three different admissibility rules, not a generic "twins" optimization.

The compiler emits an algorithm-specific capability record. It must say, for example, that simple-projection distances are supported, or shared-attribute-count weighted propagation is supported. It must reject a binary-thresholded or pair-predicate query that lacks a corresponding exact derivation. Finding an exact factorization from an arbitrary already-expanded edge list is not free and is not assumed.

This is the most promising track for a dramatic demonstration that also removes work, provided a real workload contains the required structure. Compare against a competent incidence/factor-aware baseline too. A 250x improvement over an intentionally expanded clique cannot be represented as a 250x improvement over all Neo4j deployments.

The new [Feature State PageRank](Feature-State-PageRank.md) extends this track beyond avoided edges: for its stationary projected-graph contract, a diagonal-corrected elimination moves changing rank state from entities to features. It derives a matrix-free solve and a feature-sized entity-error bound. This does not require identical entity rows and directly addresses Candidate4's remaining O(N) resident-rank problem when F is much smaller than N.

[Feature Refresh Certificates](Feature-Refresh-Certificates.md) adds D17 for a fixed-universe, fixed-personalization membership-update profile. It updates the small feature residual from complete changed rows, including the global dangling correction, then either accepts a new-version reconstruction or bounds further updates. This is state reuse, not a claim of unchanged scores after arbitrary edits. Its optional reverse postings and failed-gate score-plane overlap must fit the same storage contract.

### C. Budgeted Batch Analytics Engine

For nonredundant graphs and hard global jobs, store scan-friendly topology, external mutable state and algorithm-specific spills. D08 targets a smaller resident-state PageRank regime; D11 targets exact similarity; D14 targets embedding preparation. The safe general fallback is bounded external execution, not a claim that all three have the same layout or pass count.

The customer accepts a batch deadline in return for using available hardware. A plan may read hundreds of gigabytes from a much smaller prepared artifact. Write amplification, SSD behavior, output retention and complete job cost are first-class metrics. A RAM-successful job that misses the decision window is a failed product experiment.

An implementation can support A, B and C through shared import/publication/ownership facilities. It should not eagerly build every index or load every kernel's state. Each selected job has a specific artifact portfolio and schedule under the 50 GB prepared allowance and a separate peak-workspace cap.

## What Is Actually New Or Refined Here

| Candidate | Existing foundation | Narrower contribution to test in this study |
| --- | --- | --- |
| D01 | External sorting, split adjacency, existing compressed GDS builders | A property-aligned compiler/cursor contract whose scratch stops growing with maximum degree, including wide-record handling and aggregate semantics |
| D03 | External BFS, batching and prefetching | Physical page scheduling with bounded lookahead and ordered commit that preserves the selected target/depth/output contract |
| D04 | Dynamic shortest paths and realizing-tree certificates | A conservative persisted-answer gate with paid tree-membership/label lookups and whole-refresh fallback economics |
| D06 | Existing repository factor WCC and alternating propagation; established hypergraph algorithms | Posting-once entity-distance/witness specification and self-edge-corrected weighted projection recurrence, with independent small expanded oracles |
| D07 | Quotient chains and compressed sparse multiplication; existing repository row sharing | Exact class-mass recurrence with reconstruction from the previous mass step; explicit correction that identical outgoing rows do not imply equal entity PageRank |
| D10 | Repeated vectors and cached similarity outputs | k+1 inclusive class candidate table with exact self exclusion under a fixed tie/universe contract |
| D11/D12 | All-Pairs style exact indexing, bounds, source batching | Complete zero-score complement handling plus conservative hierarchy refinement under fixed source-owned memory and scratch budgets |
| D14 | Dimension-striped graph embeddings | Norm accumulation followed by deterministic replay, with explicit topology-pass and output costs |
| D16 | Established incidence/line-graph factorization and matrix elimination | A diagonal-corrected feature-only state schedule, matrix-free application, entity-space error certificate and explicit 4 GB/50 GB workflow experiment |
| D17 | Dynamic PageRank, incremental linear algebra and versioned overlays | Exact changed-row feature residual with global dangling correction, raw singleton provenance, bounded further work and separate numerical/publication gates |

This table distinguishes contribution from renaming. It does not establish worldwide novelty. Novelty is not the only reason to implement an idea: a carefully integrated existing mechanism can solve a valuable customer problem. Conversely, an original mechanism without relevant workloads is not a product.

## Quantitative Component Models

These are deliberately explicit shapes, not a generic "50 GB graph" benchmark. Only named components are counted. Actual source properties, allocator overhead, caches, maps, output encoding and runtime must be added before asserting a fit.

| Scenario | Reference component | Candidate component | Arithmetic implication | Crucial limitation |
| --- | --- | --- | --- | --- |
| 200M vertices, 1B directed unweighted arcs | One u32-neighbor/u64-offset CSR: 5.6 GB; two f64 vectors: 3.2 GB | D08 one resident f64 contribution vector: 1.6 GB | 50% less resident vector payload than the specified two-vector arrangement | Not 50% less whole-GDS RAM; flags, slabs, mappings and runtime remain |
| 1B vertices, 10M identical-row classes | Two entity f64 vectors: 16 GB | D07 two class f64 vectors: 0.16 GB | 99% less selected iterative-vector payload | Full grouping, quotient rows, original rows, mappings and final entity output still cost space/work |
| 10M entities, 10k disjoint groups of 1,000 | Expanded u32 adjacency plus offsets: 40.040000008 GB | D06 both incidence orders plus offsets: 0.160080016 GB | About 250x less topology in this favorable constructed shape | Not a Neo4j measurement; weights/maps omitted; requires incidence input |
| 100M entities, 1M identical-vector classes, k=10 | Packed full `(target:u64,score:f32)` top-k tables: 12 GB | D10 class k+1 tables: 0.132 GB, plus u32 class map: 0.4 GB | 0.532 GB selected retained state, about 95.6% below 12 GB | Does not shrink full per-entity output; precision/record layout here are stated modeling choices |
| 50M similarity sources, k=10, 16-byte heap entries | All-source queue payload: 8 GB | D11 batch of 100k sources: 0.016 GB | 500x smaller live queue payload | Candidate aggregation, neighbor state, passes and 12 GB of 24-byte output rows remain |
| 10M nodes, 100M arcs, 128 f32 dimensions, 3 propagation layers, stripe=16 | One full embedding layer alone: 5.12 GB | D14 illustrative stripe/norm/degree state: 2.32 GB | Candidate state could enter a 3 GB worker allowance before other costs | About 48 topology passes in the specified two-pass plan and a 5.12 GB final embedding; no measured fit |

Do not multiply these reductions together. They concern different quantities and sometimes incompatible shapes or contracts. D07's smaller vectors do not also grant D06's incidence ratio unless the actual operator has both properties and their composition is proved. D11's queue reduction does not make candidate work 500x smaller.

D14's two-pass replay derivation specifically assumes dimension-independent linear propagation followed by the stated combination of normalized intermediate rows. Normalization or nonlinear dimension mixing inside the recurrence is not covered. Its random initialization, operator, numerical tolerance and normalization placement must be pinned before any GDS comparison. A full row-major embedding consumer may require a charged external transpose even when the stripe-major computation fits.

### A Nonredundant Capacity Example

At V=200M and E=1B, one simple uncompressed topology orientation is `4E + 8(V+1) = 5.600000008 GB`. Two directions are about 11.2 GB before IDs, weights or property planes. An eight-byte edge weight adds 8 GB per duplicated billion-arc orientation; an edge-ID mapping to shared weights has its own cost. This graph can have a prepared representation below 50 GB without its topology fitting into 4 GB RAM.

A reached bitmap at V=200M is 25 MB. This does not imply the complete BFS job needs 25 MB: the frontier, distance/parent arrays if requested, directory cache, decoded blocks, original-ID mapping and output all have lifetimes. One u64 ID per reached node is another 1.6 GB of output payload. A single large GDS-compatible list may create an encoder/client materialization problem even if the search is bounded.

For the D08 schedule described by the GDS lane, one modeled iteration moves approximately `S_in + 40V` bytes: 13.6 GB for this shape. Thirty rounds imply about 408 GB of modeled traffic. At an assumed achieved mixed sequential device rate of 500 MB/s, that traffic alone corresponds to 816 seconds of I/O service. This is not a predicted end-to-end latency: CPU, random DRAM access, fetch dependencies, read/write asymmetry, queueing, build and output are not captured by that division.

Thus a smaller resident-state plan may fit where another does not, while still losing to an adequately provisioned resident baseline on elapsed time. The experiment must determine whether its cost and deadline are useful.

### Build And Refresh Accounting

For a simple external merge sort with S bytes of fixed-size normalized records, initial run budget M and fan-in F>1, let `r=ceil(S/M)` and `p=ceil(log_F(r))` when r>1. Writing initial runs and each full merge level gives approximately `2S(1+p)` read-plus-write traffic, excluding source parsing, ID joins, additional index orders and final representation changes. Buffers for all F inputs plus output and merge state must fit the actual sort budget.

For illustration, S=25 GB, M=1 GB and F=32 gives 25 initial runs, one merge level and about 100 GB of such traffic. This is a simple planning model, not a performance measurement or permission to retain 25 GB in memory. Peak live files depend on safe deletion/publication scheduling. A second sorted orientation or stable-ID join is another operation, not part of the same free pass.

Peak disk is a time-indexed live set: old pinned generation, incomplete replacement, scratch, input retained for replay, results and recovery metadata. A 50 GB final artifact cannot guarantee a 50 GB peak. Source extraction and remote storage must remain visible in economics even if they occur on another machine.

## Small Checks Executed In This Research

These are analytical toy programs executed in the tool runtime. They are not a committed Rust implementation, a GDS integration, production numerical certification or a 4 GB benchmark. Earlier detailed results are retained in the candidate document and main journal.

| Check | Domain | Result | Does not prove |
| --- | --- | --- | --- |
| Exact unchanged BFS gate | Enumerated small directed graphs and edit batches/single toggles | Zero false accepts in the recorded 12,288 n=3 and 196,608 n=4 cases | Weighted generalization, source CDC completeness or useful refresh pass rate |
| Exact unchanged WCC gate | 4,096 pairs of four-node undirected graphs | Zero false accepts; sufficient gate rejects some unchanged answers | Efficient large-graph certificate access |
| NEW: weighted unchanged-distance gate | All 729 loop-free directed three-node graphs with each edge absent/weight 0/weight 1; every ordered graph pair and all three roots | 1,594,323 cases; 133,407 accepts; zero false accepts; 100,602 unchanged-distance cases rejected | Parallel-edge identity, changed filters, overflow, CDC coverage or large-graph gate cost |
| Shared-row PageRank and class mass | 512 three-node directed graphs with loops, 20 fixed iterations | Maximum entity-score discrepancy below 7e-16 against the specified recurrence | GDS delta stopping/scaling or bitwise parity |
| Class-level k+1 similarity table | 49,152 source/k cases | Zero mismatches under the selected zero/tie/self rules | Arbitrary target filters or weighted metrics |
| Incidence BFS | 131,072 graph/filter/root cases | Zero distance mismatches against the expanded simple graph | Ordered GDS traversal, all shortest paths or external-I/O complexity |
| Incidence weighted PageRank | 122,880 compared fixed iterations | Maximum absolute discrepancy below 3.34e-16 | Binary thresholded projection or certified production rounding bounds |
| NEW: postings plus zero-score complement | All 2,401 nonempty four-row/three-feature set families; 16 target masks; 4 sources; k=1,2,3; cutoffs 0,0.5,1 | 1,382,976 compared results; zero mismatches | GDS's precise tie/output policy, weighted cases, scratch or speed |
| NEW: binned intersection upper bound | All 31x31 nonempty five-feature set pairs and all 32 two-bin assignments | 30,752 integer-checked bounds; zero false bounds | Stale summaries, arbitrary block envelopes or floating threshold code |
| NEW: normalization/replay feature stripes | 512 three-node graphs, four initializations, three layer counts and four stripe widths | 24,576 comparisons; maximum absolute discrepancy 2.23e-16 | Nonlinear recurrence, actual GDS RNG, f32 certification or I/O benefit |
| NEW: feature-state PageRank elimination | 4,096 incidence relations, three damping values, three personalization distributions | 36,864 expanded/feature stationary solves agree within 1.83e-14; 737,280 residual checks had no violation above the stated 1e-10 numerical allowance | Production interval proof, finite-iteration GDS equivalence, build or 4 GB performance |
| NEW: feature-state refresh residual | 36,864 old/new/parameter cases and three retained-state scales | 110,592 checks; maximum residual discrepancy about 2.78e-16; no bound violation above 1e-10; independent exact fixtures reviewed separately | Complete change discovery, raw singleton reactivation data, physical overlay/compaction fit or production floating certification |

For the new zero-tail check, the independent oracle computed every eligible pair directly, sorted by descending Jaccard then ascending target ID, excluded the source, applied cutoff and selected k. The candidate counted shared-feature postings, ranked positive matches, and filled the remaining slots from the complete eligible-target complement only at zero cutoff. Target eligibility did not remove features from the neighborhoods. Across these fixtures, 174,048 positive candidate pairs occurred versus 230,496 eligible full pairs before the repeated k/cutoff variations; that count is not a runtime speedup.

For the summary bound, each feature belongs to one of two bins. With bin counts a_j,b_j, `U=sum_j min(a_j,b_j)` upper-bounds exact intersection I. Then `J <= U/(|A|+|B|-U)` for the nonempty sets. Integer cross multiplication checked the inequality. Safe use requires current complete cardinalities and coverage; deleting elements from a candidate while keeping its old denominator can invalidate pruning.

### A Quantitative Reason To Demote The Coarse PageRank Gate

D09's cheapest cross-generation gate can upper-bound the change of each stochastic source column by two. With an initially exact vector and no numerical allowance, a sufficient condition for reusing it within L1 error epsilon is `sum_changed_sources x_j <= epsilon*(1-alpha)/(2*alpha)`. A nonzero initial residual or rounding allowance only tightens that condition. This epsilon is an L1 error-to-fixed-point contract, not GDS's tolerance parameter.

| Damping alpha | L1 epsilon | Maximum changed source probability mass under the coarse condition | At most this many sources if all 200M source scores were exactly uniform |
| --- | ---: | ---: | ---: |
| 0.85 | 0.000001 | 8.8235294e-8 | 17 |
| 0.85 | 0.001 | 8.8235294e-5 | 17,647 |
| 0.99 | 0.000001 | 5.0505051e-9 | 1 |
| 0.99 | 0.001 | 5.0505051e-6 | 1,010 |

These are arithmetic consequences of a conservative bound, not observed pass rates. Real scores are not uniform, changed rows may have much smaller actual L1 differences, and exact no-change input certification can bypass an irrelevant update entirely. Still, this calculation explains why a simple mass-only gate can fail on modest updates despite the true answer barely changing. Refine selected columns, recompute a residual, or fall back; do not base a broad freshness promise on this gate. D04's exact-distance certificate and D05's unchanged-projection certificate have different applicability and should not inherit these estimates.

## Verification-First Implementation Sequence

The following is a proposed research-to-build sequence, not authorization to claim any of these implementations exist. Do not make the entire seventeen-option portfolio a prerequisite for shipping one useful workflow.

### Package 1: End-To-End Reference Workload

Select one real representative projection and question. Preserve source IDs, isolates, edge direction/types, multiplicity, requested fields and exactness. Pin the Neo4j/GDS version and operation when compatibility is required. Create independent small expected answers and a differential adapter before optimizing. Run a simple implementation from input through full output, then change the input and repeat.

Start from the existing narrow compatibility evidence above rather than assuming no integration exists. Reuse only the tested profile and extend its contracts deliberately; parser acceptance or shared column names do not establish a new GDS algorithm or broader Cypher compatibility.

The first acceptance gate is correctness and honest accounting, including extraction/build/output. A test that hashes two endpoint rows cannot validate a claimed 34-edge graph. A scalar digest sink may help compare algorithms but must not stand in for full customer output in the product benchmark.

### Package 2: Bounded Builder And Traversal

Test D01 on equal-E graphs with increasing maximum degree and property width. Once chunk limits are reached, scratch must stop growing with hub degree. Assert aligned endpoint/property streams, duplicate aggregates and defaults. Then test D03 with targets, filters, depth, parallel edges and differing physical layouts. Compare exact output obligations, not only reached sets.

Use physical 4 GB hardware or a clearly specified equivalent VM envelope, no hidden larger builder and no unreported swap. Include parser/decompressor, ID map, page cache, output, verifier, cancellation and mid-build crash. Reserve progress buffers so a full pool can spill without needing an unavailable allocation. Concurrency starts at one heavy job; shared admission comes before increased parallelism.

### Package 3: Structural Differentiation

Profile incidence availability, identical normalized rows, identical neighborhood vectors and actual query reuse. Include a low-redundancy counterexample. Choose D06, D07 or D10 based on observed opportunity, not a favorable synthetic-only headline. Measure the build cost of finding/verifying structure, the result reconstruction, and refresh invalidation.

An accepted structural fast path must match its independent expanded/unfactored oracle on bounded fixtures. On large inputs, validate its canonical representation/operator without recreating a forbidden giant expansion. If the required structure is absent or expensive to discover, retain a useful ordinary external plan.

### Package 4: Hard Family And Repeated Use

D11/D12 exact similarity is the first architecture stress test beyond traversal because candidate work and output can dwarf topology. Test all-common-feature and all-disjoint families, zero cutoff, exact ties, asymmetric source/target filters, slow consumers and candidate-spool limits. D08 PageRank is a separate iteration-state test with every superstep and convergence field checked against the selected oracle. Neither WCC nor BFS proves these families fit.

Then test D04/D05 refresh reuse with complete and deliberately incomplete change intervals, bridge/tree-edge deletion, irrelevant properties, new matching labels, changed filters, ID reuse and revoked access. A failed certificate must fall back correctly; count gate failures and full recomputations in economics.

## Fair Comparison And Decision Rules

Use three comparator classes: pinned Neo4j/GDS with supported compressed projections; a current credible disk-oriented graph alternative where semantics overlap; and simple tuned custom code using the same declared query/output. Vendor documentation or historical archive benchmarks are not substitute measurements on our workload. Preserve no-finish/OOM/timeout cases in the results.

Report separately:

- Same-machine capacity: what completes on the 4 GB deployment, including building and result delivery?
- Matched-workload speed: where both finish, what are first-answer, warm-answer, refresh and output times?
- Practical economics: what does a larger-machine incumbent cost per completed useful job versus our smaller machine, including source, storage, I/O, retries and operator effort?
- Semantic coverage: what exact queries, filters, parameters and output modes were admitted and successfully completed?
- Mechanism attribution: does disabling the special layout/certificate/classing remove the benefit, with all other conditions held comparable?

Before timing, obtain the customer's useful deadline and first-answer/freshness requirements. Pre-register acceptance and kill criteria. Do not select thresholds after seeing favorable results. A practical initial hypothesis is that a fast-path should produce a material whole-job improvement, not a tiny kernel gain hidden under a much slower build; the numeric threshold belongs to the workload's business value and measurement variance.

Kill or downgrade a proposal when it has a false semantic acceptance, loses its RAM cap in an omitted lifecycle phase, requires unavailable source structure, expands output beyond disk without an agreed sink, or consistently misses the customer's deadline. Retain a slower plan only when the customer values its capacity/economics and the full job remains useful. This is a product decision backed by measurements, not aesthetic preference for a storage format.

## Three Plausible Delivery Paths

| Path | First proof | Expansion after evidence | Stop signal |
| --- | --- | --- | --- |
| Investigation-led | Bounded CSV/Neo4j snapshot build plus useful reachability result | Repeated-snapshot certificates, supported parameterized queries, restricted joins | Customers mainly need complex permission semantics or full query compatibility outside the proven subset |
| Structural-compute-led | Incidence or repeated-row workload with exact lower-work execution | Related operators with their own proofs and user workflows | Real data lacks structure or factor discovery/output cancels the win |
| Batch-economics-led | A hard rank/similarity job completing under 4 GB before an agreed deadline | Scheduled refresh, embedded/Python/SQL integration, additional batch kernels | Existing embedded/custom tools already meet the need, or disk/runtime cost destroys savings |

These paths can share infrastructure, but their product evidence is different. Do not take success on one as validation of all three. Keep an explicit choice rather than drifting toward a complete Neo4j rewrite because the reference source is available.

## Next Implementation Boundary

All assigned prose and raw substantive content is integrated; supporting tables and machine metadata retain the narrower inspection scopes in the coverage audit. The [decision brief](Final-Research-Decision-Brief.md) selects the next experiment and [product timelines](Product-Decision-Timelines.md) preserve alternatives. Implement one representative source-to-answer-to-refresh workflow next, using the existing narrow compatibility proof where applicable. Measure bounded preparation and complete delivery before promoting any new capacity or speed claim. The research deliverable is complete; the proposed system and its customer validation are not.

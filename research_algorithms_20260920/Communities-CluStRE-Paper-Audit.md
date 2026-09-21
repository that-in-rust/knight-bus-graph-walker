# CluStRE: Independent Paper-Side Audit

**Source access date:** 2026-09-21 (Asia/Kolkata).

**Primary work:** Adil Chhabra, Shai Dorian Peretz, Christian Schulz, *CluStRE: Streaming Graph Clustering with Multi-Stage Refinement*, SEA 2025, LIPIcs 338, article 11, pp. 11:1-11:20, published 2025-07-15. DOI: [10.4230/LIPIcs.SEA.2025.11][publisher]. The paper is CC BY 4.0; this document is an independent synthesis, not a reproduction.

## 1. Premise Check and Scope

**Bottom line:** CluStRE is a node-stream modularity heuristic with retained per-vertex state and optional in-memory quotient optimization. It is not an algorithm with graph-size-independent RAM, a proved approximation ratio, or a proved external-memory processing bound. Its useful exact statement concerns contraction of a *correctly aggregated, fixed partition*. That statement does not establish that the printed online construction produces that partition's quotient. [Sections 3.1-3.4, pp. 11:5-11:9][p5]; [Appendix A, p. 11:20][p20].

This audit reads the entire published 20-page paper, including Algorithms 1-3, Tables 1-2, references, and Appendix A, using the [official PDF][pdf] and cross-checks against the [publisher's experimental HTML][html]. Related-paper reading is limited to cited work needed to evaluate controls. No local or remote implementation source was inspected. The lead's separate checkout and commit are not evidence for any statement here. No builds, benchmarks, datasets, implementation edits, commits, or pushes were performed. This is a bounded contribution to the seven-family investigation, not completion of that goal.

Evidence labels used below:

- **Paper:** an explicit definition, algorithm, claim, or observation in the publication.
- **Derived:** an accounting consequence, algebraic check, or counterexample developed here under stated assumptions.
- **Open:** a specification gap or research question, not evidence of a defect in the current implementation and not a novelty claim.

## 2. Audit Lenses and Existing Choices

The audit uses four lenses: objective/representation correctness; streaming and external-memory accounting; reproducibility of quality claims; and an adversarial search for simpler controls.

The first controls are already configurations of CluStRE. Their dependencies are not interchangeable. [Section 4.1, p. 11:11][p11].

| Mode | Ordered phases after initialization | Information retained for refinement |
|---|---|---|
| Light | One node-stream pass, then output | Vertex assignments and cluster volumes during the pass |
| Light+ | Light, then original-graph local-search restreams | Mutable assignments/volumes, active sets, access to original adjacency |
| Evo | One pass with quotient construction, then VieClus on the quotient, then lift labels | Original-to-supernode mapping, quotient, evolutionary working state |
| Strong | Evo, then original-graph local-search restreams | Both refinement dependencies, though not necessarily all simultaneously live |

## 3. Chosen Thesis

Investigate the cost of retaining and querying exact state, not just the cost of storing original edges. Three separate quantities can overwhelm a fixed RAM budget: the assignment/volume state, the quotient and its optimizer, and the adjacency/index/active-state working set. Improving one does not bound the others. Any new representation must state whether it preserves the same sequential decisions, only modularity scores, only a restricted search space, or an approximate objective.

## 4. Evidence and Verification

### 4.1 Input, Output, and Scoring Semantics

**Paper.** The input is an undirected graph without parallel edges or input self-loops; vertices have IDs `0..n-1`. Positive edge weights are allowed in the definitions. Clustering is a disjoint partition covering all vertices, with no predetermined cluster count or balance constraint. Output is a cluster assignment for every vertex. Experiments instead use unit weights and METIS node-stream files after removing directions, parallel edges, and self-loops. This is not an arbitrary live edge-update/deletion stream. [Section 2.1, p. 11:3][p3]; [Section 4, p. 11:10][p10].

**Paper.** Initialize `C[v] = v`. On arrival, load the current vertex's entire neighborhood, group neighbor edge weights by their current cluster IDs, and select a neighboring cluster maximizing Equation (3). Keep the current assignment if all alternatives decrease modularity. The paper claims `O(deg(v))` scoring time and `O(n+m)` time for the first pass using an assignment array of size `Theta(n)` and a cluster-volume array. [Algorithms 1 and Section 3.2, p. 11:6][p6].

For clarity, this audit uses `m = |E|` for edge count and `W = sum_e w(e)` for undirected edge mass. With `A` the current cluster, `B` a distinct candidate, `d = d_w(v)`, and `K(v,B)` the incident weight to members of `B`, Equation (3) becomes:

```text
gain(v, A -> B)
  = [K(v,B) - K(v,A)] / W
    - d * [d + volume(B) - volume(A)] / (2 W^2).
```

**Derived dependencies.** This gain needs full weighted degree, current cluster volumes, and total graph mass, in addition to neighbor labels. Assignments cannot simply be emitted and forgotten: future vertices query them; Evo lifts quotient labels through them; local search mutates them. One global relabeling after Evo is conceptually `C[v] = quotientLabel[C[v]]`, requiring a retained mapping or an external equivalent.

**Open first-pass initialization.** Algorithm 1 initializes labels but does not describe initialization of volumes or total weight. Equation (2) ranges over clusters of all neighbors, without explicitly excluding unprocessed neighbors. Their singleton volumes require their full degrees, which a plain adjacency stream does not generally reveal before their records arrive. Degree metadata or a preprocessing scan can supply these values; a METIS edge-count header alone cannot supply every degree. Treat the one-pass bound as conditional on this information, not proof that arbitrary raw input needs only one read. The prose's instruction to retain a singleton also needs care: earlier vertices may already have joined the current vertex's original label. Staying then retains the current cluster, not necessarily a singleton. [Section 3.2][p6].

Tie-breaking, acceptance of zero-gain moves, the edgeless case `W=0`, and the empty candidate set for isolated vertices are not fully specified by the printed pseudocode. These matter for exact trajectory equivalence and termination claims; no implementation behavior is inferred.

### 4.2 Objective and Loop Conventions Need Reconciliation

**Paper.** Equation (1) writes `Q = (1/m) sum_C [K(C,C) - volume(C)^2/(2m)]`. Section 2.1 introduces `m` as edge count, whereas Appendix A explicitly makes it total edge weight. Section 3.3 and Figure 2 double intra-cluster edge weights for quotient self-loops. Appendix A also asserts preservation of total edge weight when summing quotient edge weights. [Equation (1), p. 11:3][p3]; [Figure 2 and Section 3.3, p. 11:7][p7]; [Equations (4)-(8), p. 11:20][p20].

**Derived consistency check.** Under the conventional once-per-undirected-edge interpretation, standard modularity compatible with Equation (3) is:

```text
Q_standard(C) = sum_A [internalWeight(A)/W - (volume(A)/(2W))^2].
```

If the paper's `K(C,C)` counts internal edges once, its printed Equation (1) doubles the null-model penalty: a single cluster containing the whole nonempty graph scores `-1`, not `0`. If `K(C,C)` counts internal edges twice, Equation (1) is instead twice standard modularity, but Equation (3) has the standard gain scaling. Likewise, a doubled diagonal loop cannot be summed as a once-counted undirected edge while asserting unchanged `W` without a compensating convention.

This is a notation/normalization issue in the publication, not a rejection of the familiar contraction identity. A coherent adjacency-matrix convention is to store inter-supernode mass symmetrically, diagonal entries as twice internal mass, preserve row sums as volumes, and define `W = sum_ij A_Q[i,j]/2`. Any proof or exactness test should explicitly choose and consistently use such a convention. The calculations below use `Q_standard` and the gain above, not a silent mixture of the printed formulas.

### 4.3 Phase Dependencies and the Quotient Theorem

**Paper, reconstructed phase contract.** Algorithms 1-3 and Sections 3.3-3.4 imply:

1. Initialize singleton labels and the scoring state needed for the first pass.
2. Process each vertex once; update its label and cluster volumes immediately. If enabled, update a quotient-edge hash map during this pass.
3. If enabled, run VieClus on the quotient. It generates a population using adapted Louvain/label propagation, combines parent partitions through overlays and flat/multilevel recombination, uses splitting-based mutation, and replaces sufficiently good similar individuals. At a time cutoff, lift the best population member back to original vertices.
4. If enabled, start local search with all vertices active. Visit active vertices, move them according to the gain score, and make neighbors of moved vertices active for the next round. Later rounds load only active vertices from disk. No new cluster is created in this refinement stage.
5. Stop local search on empty activity, insufficient relative gain, or its time cutoff; output the vertex labels.

[Algorithm 1, p. 11:6][p6]; [Section 3.3 and Algorithm 2, pp. 11:7-11:8][p7]; [Algorithm 3, p. 11:9][p9].

**The exact theorem's domain.** Fix a partition `P` into `k` base blocks. The quotient has one supernode per block, node weight equal to the block's vertex count, and aggregate edge/loop weights. Appendix A states that its singleton partition has the same modularity as `P`, and that *any partition of its supernodes* lifts to a partition of the original vertices with equal modularity. The proof preserves internal edge mass and cluster volumes under contraction/expansion. [Appendix A, Theorem 1, p. 11:20][p20].

**Derived limitation.** The lifted partitions are precisely coarsenings of `P`: a supernode is indivisible. VieClus can split a community of several quotient vertices, but cannot split the original vertices inside one base supernode. The broad split/merge language in Section 3.1 must be read with that restriction. Original-graph restreaming can move individual vertices between existing clusters and break base-block membership; the quotient alone lacks that information. A contraction identity is not a guarantee of recovering the best unrestricted partition.

**Quality non-decrease also needs a baseline.** Section 3.3 attributes non-worsening recombination to VieClus's parent-handling strategies. That is not an approximation ratio. To infer non-decrease relative to the initial streamed clustering, one must establish that the quotient result is no worse than its singleton solution under the same normalization, for example by retaining that baseline or proving the initialization/local-search chain cannot worsen it. CluStRE's printed population description does not explicitly supply this end-to-end baseline argument. [Section 3.3, pp. 11:7-11:8][p8].

### 4.4 The Printed Quotient Update Does Not Establish Its Invariant

**Paper.** Algorithm 2 examines neighbors satisfying `v < u`, reads current labels `C[v]` and `C[u]`, doubles the weight if they match, and inserts/updates the corresponding hash-map entry. It specifies no retraction or transfer of earlier edge contributions when a later vertex changes label. [Algorithm 2, p. 11:8][p8].

**Derived counterexample, not an implementation test.** Take the weighted path `0 --1-- 1 --2-- 2`, stream vertices in ascending order, and assume the exact degrees/volumes needed by Equation (3) are available. Here `W=3` and degrees are `(1,3,2)`.

- Vertex 0 joins label 1 with gain `1/6`. Algorithm 2 records edge `(0,1)` as a diagonal entry of weight 2 at label 1.
- Vertex 1 then joins label 2 with gain `1/6`: its current cluster volume is 4, target volume is 2, and incident weights to those clusters are 1 and 2.
- The final blocks are `{0}` and `{1,2}`. Edge `(0,1)` is now inter-block, but the old diagonal entry remains. A correct quotient has an inter-block edge of weight 1 and the second block's doubled diagonal of weight 4; the literal update leaves diagonal weight 2 at the first block instead.

Thus even with correct score state, the printed update does not generally construct the fixed-partition quotient required by Theorem 1. This does not show that the lead's source version has the same behavior.

**Independent hash-key ambiguity.** The insertion test uses absence of `(Ci,Cj)` **or** absence of `(Cj,Ci)` before assigning, rather than incrementing, a weight. With ordinary ordered-pair keys and only one stored orientation, the reverse remains absent and repeated contributions can overwrite. Canonical unordered keys or symmetric insertion might explain an intended representation, but neither is stated in Algorithm 2. [p. 11:8][p8].

**Ordinary controls, not research inventions.** During an ascending-ID, one-visit pass with permanent per-vertex assignments, commit an edge only when its later endpoint has been assigned, using already-processed neighbors and a canonical cluster-pair key. Both endpoint labels are then final for this phase. Alternatively, freeze the partition and rebuild the quotient in another scan. Either needs proof of its own invariant, but the paper-side discrepancy alone is not evidence of a novel opportunity. Both still face quotient growth and state-access costs.

### 4.5 State, Time, and External I/O Accounting

Let `Delta` be maximum original degree; `k` the number of nonempty quotient blocks; `q` the number of nonzero undirected quotient records, including loops; `q_peak` the maximum resident edge-record count; `A_t` the active vertices in round `t`; `a_t=|A_t|`; and `d_t=sum_{v in A_t} deg(v)`. Bounds below count machine words unless stated otherwise. They are audit accounting, not additional theorems claimed by CluStRE.

| State or phase | Paper statement | Fixed-RAM consequence / missing term |
|---|---|---|
| Vertex labels | `Theta(n)` assignment array | IDs require up to `log2(n)` bits each; exact arbitrary label sequences are not guaranteed compressible. Output can go to disk, but later queries still need a state representation. |
| Cluster volumes | Array of size number of clusters | Initially there are `n` singleton labels. A small final `k` does not bound initial state; sparse labels can still require an ID map. |
| Current neighborhood and score accumulation | One neighborhood resident; `O(deg(v))` scoring | Neighborhood storage can be `Theta(Delta)`, plus per-distinct-neighbor-cluster aggregation. High-degree vertices can exceed a fixed buffer. Constant-time label/volume access and suitable accumulation/reset are prerequisites for the scoring bound. |
| First pass | `O(n+m)` time | A prepared-stream RAM-model claim. Degree/mass initialization, input conversion, external state lookup, and output have separate costs. |
| Quotient construction | Hash map, `O(q)` additional memory; prose says time linear in `q` | Algorithm 2 still inspects original adjacency and can perform `Theta(m)` contributions when `q` is tiny. Expected constant-time hashing gives scan-plus-update time `O(n+m)`, not total `O(q)` construction from raw input. Hash-table growth/rehashing and duplicate graph representations affect peak bytes. |
| Quotient optimization | VieClus population and multilevel refinement until a cutoff | At least the resident quotient and partition state; population label vectors can cost `O(population_size * k)`, with overlay, level, and temporary-graph storage additional. No whole-phase RAM or initialization-time bound is supplied. |
| Restream state | Active and next-active vertex sets | Each set can contain all `n` vertices. Bitmaps save constants, not the dependence on `n`; unrestricted vectors with duplicate entries would need separate deduplication. |
| Active record loading | Load active vertices from disk | Selective loading requires addressable adjacency, e.g. record offsets/indexes, or a full scan to discover/skip records. A simple dense offset index adds `Theta(n)` words. Its construction/storage are not specified in the paper. |
| Restream computation | Repeated local-search rounds | Under resident state and efficient active-set handling: approximately `sum_t O(a_t+d_t)`, plus enumeration/indexing and label maintenance. Each round can again be `Theta(n+m)`. No useful graph-independent round bound is proved. |
| Final output / lifting | Return or update all assignments | At least `n` output records for an explicit answer; lifting scans or otherwise accesses the original-to-quotient mapping. Output size alone does not prove an internal-RAM lower bound. |

Sources: [Sections 3.1-3.3, pp. 11:5-11:8][p5]; [Section 3.4, p. 11:9][p9].

**Quotient growth.** For a correctly aggregated fixed partition, `q <= min(m, k(k+1)/2)` in the canonical undirected-record convention. This permits `q = Theta(m)` and a dense `Theta(k^2)` quotient. No theorem in the paper forces a small `k`, large compression ratio, or a bound depending only on a chosen RAM budget. A small final quotient also does not establish small transient memory for a construction that retains obsolete keys or temporary copies. Under the explicitly maintained structures, a useful first-pass accounting is `O(n + Delta + q_peak)` words, before optimizer working storage. It is not an unconditional strict sublinear-space bound in the input size.

**I/O model distinction.** Let `S` be the stored adjacency-record volume and `B` the external transfer block size. With state in RAM, a sequential full pass costs `O(1+S/B)` transfers. Indexed active loading can instead incur roughly `sum_{v in A_t} O(1 + record_size(v)/B)` transfers, potentially one random access per active record, plus index access. Without an index it can still require a full scan. Fewer logical records do not automatically mean fewer elapsed seconds than a sequential read. If labels or volumes spill, arbitrary neighbor accesses can add many random block transfers; the paper gives no bound for that regime.

**Original input retention.** Light need not retain original adjacency after consuming it, but Light+/Strong need a replayable/seekable copy for refinement. A non-replayable source must be spooled, regenerated, or excluded from that contract. Conversion to undirected, deduplicated METIS input may itself need sorting/storage. The publication does not provide end-to-end preprocessing, index, temporary-disk, or block-I/O accounting. [Sections 3.4 and 4][p9].

**Deadlines are not full runtime bounds.** Experimental limits are 15 seconds for evolutionary rounds and 10 minutes for local search, with relative threshold `X=0.05`; baseline VieClus gets five minutes of evolutionary rounds. These do not bound input scans or population initialization. Algorithm 3 checks elapsed time in the outer loop, so its literal form can overrun by an active round. No deterministic bound is given on the work of that round or on work inside a memetic iteration. [Algorithm 3][p9]; [experimental setup][p10].

The real-weight model also suppresses numeric representation costs: exact sums of arbitrary real weights are not a finite-word implementation contract. Integer widths, overflow, floating-point accumulation order, tolerances, and zero-gain behavior must be fixed before claiming bitwise or decision-by-decision equivalence.

### 4.6 Selective Restreaming Is Not a Full Local-Optimality Certificate

**Paper.** The first restream visits every vertex. Subsequent active sets consist of neighbors of vertices moved in the preceding round. Section 3.4 explicitly motivates this by the first, incident-edge term of Equation (3). Algorithm 3 stops when activity empties, improvement falls below the threshold, or time runs out. [pp. 11:8-11:9][p8].

More precisely, its outer guard requires nonempty activity, `DeltaQ >= X*Qtotal`, and elapsed time below the cutoff. Each round resets `DeltaQ`, accumulates accepted move gains, then adds them to `Qtotal`. The initial `Qtotal` is not defined in the printed initializer. Exact replay also needs a specified active-node visitation order, because updates are sequential and later gains depend on earlier moves. [Algorithm 3, p. 11:9][p9].

**Derived limitation.** A vertex's candidate gain can change without any neighbor changing label. If its current cluster gains or loses remote members, or a candidate cluster does, the volume term changes. For an unchanged vertex and unchanged neighbor labels, between evaluations `s` and `t`:

```text
gain_t(v,A->B) - gain_s(v,A->B)
  = -d_w(v) * [change(volume(B)) - change(volume(A))] / (2 W^2).
```

The right side need not be zero. Consequently, empty neighbor-triggered activity is not proof that all positive-gain moves are absent. A relative threshold or timeout can also stop with improving moves remaining. No new-cluster creation further restricts the move neighborhood. The overview/conclusion's local-optimum language should therefore not be upgraded to a theorem about unrestricted vertex moves or the unrestricted modularity objective. This is separate from the conditional fact that individually accepted, correctly computed positive-gain moves increase the objective.

### 4.7 What the Experiments Do and Do Not Establish

**Paper observations, not guarantees.** The main comparison reports Light's 89.8% higher modularity, 2.6-fold speed advantage, and about 58.8% of Hollocou's memory; Strong improves modularity by 149.5% and reaches average ratios 96.8%/96.5% relative to Louvain/VieClus. Strong's memory ratios against those in-memory baselines are 18.3%/10.8% on the comparable instances. These are measured aggregate ratios, not approximation factors relative to an optimum or upper bounds on memory. [Sections 4.2-4.3, pp. 11:12-11:14][p12].

Important qualifications:

- Experiments use one core of a machine with 100 GB RAM. Memory means process maximum resident set size. The paper does not establish operation under an enforced small RAM cap or account for all filesystem cache, preprocessing, and temporary external storage. [Section 4, p. 11:10][p10].
- Aggregation uses geometric means. The in-memory comparison excludes, for all compared algorithms in Figure 5, instances where Louvain/VieClus exceeded memory. It must not be generalized to the omitted instances. [Methodology, p. 11:11][p11]; [Section 4.3 and Figure 5][p15].
- Hollocou was modified to stream from disk rather than loading/shuffling all edges; its parameter was set to `vmax=10000` after trying alternatives. The authors report that the unmodified baseline did not improve quality in their experiments. This does not eliminate sensitivity to ordering, parameter policy, or representation in other workloads. [Section 4, p. 11:10][p10]; [Section 4.2, p. 11:12][p12].
- Aggregate superiority is not per-instance dominance. In Table 1, circuit5m gives Light `0.1926` and Light+ `0.2920`, versus Hollocou `0.3707`; several road graphs also favor Hollocou over those modes. [Table 1, p. 11:13][p13].
- Ground-truth recovery uses four citation/co-purchase graphs, not a guarantee of semantic community recovery. Table 2 itself separates modularity from NMI: on Citeseer, Light+ increases modularity over Light while NMI decreases slightly (`0.3314` versus `0.3318`). [Section 4.4 and Table 2, pp. 11:14-11:15][p15].
- The reported 2.18 GB peak for Strong on uk-2007-05 is one useful observation, not a bound as `n`, `Delta`, or `q` increase. The publication supplies no distribution of quotient sizes, active-set work, or representation-level memory sufficient to certify a fixed-RAM design. Repeated-seed uncertainty and a detailed storage/I/O model are not provided in the reported methodology. [Section 4.1, p. 11:12][p12].

### 4.8 Closest Cited Work Needed for Controls

Only the following primary-source checks were needed; this is not an exhaustive prior-art search.

**Hollocou et al., CluStRE reference [27].** The [primary HTML, Sections 2.1-2.5 and 3][hollocou] describes an insert-only edge stream, three per-node integer dictionaries, `O(m)` processing, and `O(n)` storage. Its volume-threshold rule is a cheap heuristic; the theoretical discussion gives conditional sufficient circumstances for modularity improvement, not a universal recovery guarantee. Section 2.5 discusses simultaneous parameter choices. Thus CluStRE's criticism of absent parameter guidance should not be read as absence of any parameter discussion. It remains a strong low-state baseline, but its information model differs from full-neighborhood node streaming.

**StreamCPI, CluStRE reference [14].** [The cited version's Sections 4.1-4.4][streamcpi] already study run-length-compressed assignments, batched append/query support, and an external-memory priority queue with time-forward processing. Section 4.3 also changes the score to encourage runs. These are relevant representation controls, not a drop-in proof for mutable CluStRE restreams. Append-only assignments differ from arbitrary later updates; fixed-`k` partitioning differs from initially `n` clusters. Any claim that assignment compression or externalizing labels is itself new is defeated by this cited work.

**Assadi et al., CluStRE reference [3].** The [primary publisher abstract][assadi] identifies its objective as Dasgupta hierarchical-clustering cost and presents streaming space/approximation results and a cut-sparsifier connection. That is not modularity clustering. Its bounds cannot be imported as lower bounds or approximation guarantees for CluStRE without an explicit reduction. Only the abstract/objective statement was checked here, not its full proof machinery.

CluStRE also cites [restreaming graph partitioning, reference 45][restream] and [buffered streaming partitioning, reference 20][buffered] as precedents in its introduction. Their full papers were not audited. They reinforce that extra passes or buffering alone are not sufficient novelty claims; no theorem from them is assumed here.

## 5. Final Synthesis

The defensible contribution of the paper is an empirically effective combination of a one-pass node-level modularity heuristic, optional quotient-level memetic search, and selective original-graph refinement. The strongest exact property is objective preservation for coarsenings under a correctly formed quotient and consistent weight conventions. It does not certify the printed construction, arbitrary splitting, fixed RAM, a hard processing deadline, an approximation ratio, or full local optimality.

For the broader investigation, separate two tracks: ordinary specification/representation repairs and research hypotheses that survive strong controls. Canonical quotient keys, committing edges after both endpoint labels are final, an extra frozen-label aggregation scan, bit-packed arrays, compact active sets, and explicit pass/deadline budgets belong in the ordinary-control track unless something substantially stronger is proved.

## 6. Three Unanswered Opportunities, With Defeat Conditions

These are questions left open by the paper-side evidence, **not claims of novelty**. No prototype, benchmark, or new theorem is claimed.

### O1. Exact Mutable Assignment Storage With a Hard RAM Budget

**Question.** Can a compressed baseline plus bounded mutable delta representation support the *same sequential* neighbor-label queries and volume updates as Light+/Strong, with an explicit RAM cap and an amortized external-I/O bound parameterized by label runs, update count, and update locality? Include implicit untouched singleton labels, degree/volume state, compaction peaks, and final lifting/output in the contract. All reads must see earlier accepted moves; using stale epoch snapshots would change the algorithm, not just its representation. [Dependency: Sections 3.2-3.4][p6].

**What must be proved.** Exact lookup/update semantics, bounded compaction workspace, worst-case behavior when the run count reaches `n`, and an I/O/work bound under adversarial updates. Reducing assignment bytes while leaving `n` volumes or offsets resident does not solve fixed RAM. A bound may legitimately depend on compressibility or allow disk spill, but must expose those assumptions.

**Strong controls.** Packed integer labels with compact ID remapping; chunked compression with a small update overlay; an ordinary bounded page cache over external arrays; StreamCPI for the append-only Light case; and its time-forward external approach wherever its dependency order applies. [Reference [14], Sections 4.1-4.4][streamcpi].

**Defeat condition.** Reject a research claim if the gain is ordinary bit-width reduction/RLE, if a standard block-compressed mutable array matches it, or if mandatory volumes/indexes dominate. A true hard-cap result cannot assume favorable community-ID runs on every graph.

### O2. Budgeted Quotient Scoring With a Uniform Error Certificate

**Question.** For a frozen base partition, can a budget-limited quotient representation answer the adaptively selected coarsening scores needed by the optimizer with a uniform additive modularity-error certificate, while accounting for exact or approximate volume state, construction, and optimizer workspace? A certificate for one preselected partition is insufficient for a search that chooses partitions after seeing the representation. [Dependency: Section 3.3 and Theorem 1][p7].

**What must be proved.** A simultaneous bound `|Q_hat(D)-Q(D)| <= epsilon` over all allowed quotient partitions, with explicit probability and resource assumptions, or a narrower documented search family. A uniform bound transfers approximate-score selection to a `2*epsilon` loss relative to the best *candidate considered*, not the unrestricted optimum. A literal fixed total RAM budget also requires dealing with growing `k` and evolutionary state; sparsifying only the edge map is insufficient.

**Strong controls and a deliberately easy reduction.** An exact quotient built with canonical pair aggregation and bounded-memory external sorting is the first control; count label joins/accesses and later optimizer I/O rather than hiding them. A second is an ordinary all-cuts approximation with exact original volumes and `W`. Since

```text
Q_standard(D) = 1 - interClusterWeight(D)/W
                 - sum_A (volume(A)/(2W))^2,
```

a simultaneous `(1 +/- epsilon)` guarantee on all cuts also approximates total inter-cluster weight: it is half the sum of the cluster boundary cuts. Its additive modularity error is at most `epsilon`, because inter-cluster weight is at most `W`. This is an elementary audit reduction under the exact-volume assumption, not a theorem attributed to CluStRE or a novelty claim. The cited Assadi paper motivates checking cut representations, but its hierarchical-cost guarantees are not a substitute for this objective's proof.

**Defeat condition.** Reject the opportunity as stated if standard cut sparsification plus retained degree metadata already supplies the desired trade-off, or if exact external aggregation plus Light+ avoids the optimizer bottleneck. Recomputing degrees from an approximate graph silently changes the null-model term and requires a separate error bound.

**Paper-side handoff for the lead's residual-certificate direction.** Theorem 1 assumes exact aggregation; it supplies no residual-error certificate for a sparse surrogate. Keep original volumes and `W` explicit, and distinguish `internalWeight_H(D)/W - originalPenalty(D)` from `1 - cutWeight_H(D)/W - originalPenalty(D)`. If the retained sparse graph has total undirected mass `W_H`, these differ by the partition-independent constant `1-W_H/W`. They therefore rank partitions alike under these fixed terms, but their absolute errors and certificates are not interchangeable unless the shift is accounted for. This is a normalization check, not a proof of the lead's proposed representation. A score certificate would concern the frozen quotient's allowed coarsenings, not preservation of CluStRE's original sequential trajectory or unrestricted vertex refinements.

### O3. Safe Active-Set Pruning That Accounts for Volume Changes

**Question.** Can a compact certificate invalidate only the necessary sleeping vertices while guaranteeing that no positive-gain *existing-candidate* move is missed, with a useful bound on reactivation work and certificate storage? The target is predictable certified refinement, not the publication's neighborhood-only heuristic. [Dependency: Section 3.4, p. 11:9][p9].

**What must be proved.** Account for both changed neighbor labels and changed remote cluster volumes. Bound the work required to find affected vertices; an inverse vertex-to-candidate-cluster dependency index can be edge-sized and erase the memory benefit. State whether the guarantee covers only neighboring-cluster moves, and separate safe skipping from stopping early due to time or a gain threshold.

**Strong controls.** Periodic complete sequential sweeps; cluster-version invalidation; and a cheap global score-drift bound before inventing a complex dependency structure. For unchanged vertex/neighbor labels, if every cluster volume changed by at most `Dmax` since its evaluation, each candidate gain can increase by at most `d_w(v)*Dmax/W^2`. A cached upper bound on its best gain that remains nonpositive after adding this allowance safely postpones that vertex. Changes to the vertex's own label or neighbor labels invalidate the premise. This inequality follows directly from Equation (3); it is not itself a substantial novelty claim.

**Defeat condition.** Reject a more elaborate certificate if the scalar bound or occasional full scan gives equivalent guarantees with less state, or if high-degree vertices/large volume drift force nearly universal reactivation. Any stronger claim needs an amortized improvement under explicit conditions, not just fewer active nodes on selected inputs.

## 7. Evidence Limits and Handoff

- Full-paper coverage refers to the published SEA 2025 version, not its linked arXiv full-version supplement or subsequent source revisions.
- PDF text and publisher HTML were available; direct local PDF download failed, and screenshot retrieval did not provide a usable visual verification. Formula/pseudocode findings are grounded in the publisher text representations; no plot values were estimated visually.
- The quotient path counterexample and score-drift/cut calculations are analytical checks, not benchmark results. The notation discrepancy remains explicitly separated from the mathematically valid contraction principle.
- This audit does not establish prior-art novelty or impossibility of a better representation. Related work was checked only to the stated extent. In particular, no modularity lower bound is borrowed from hierarchical clustering.
- Source reconciliation belongs to the lead: gain normalization, degree initialization, quotient edge commitment/key conventions, baseline retention, indexing, and active-set semantics. Lead-reported source findings are deliberately not incorporated as independent evidence here. This document makes no claim about that source and does not mark the broader goal complete.

## Primary URLs

- [Official publisher record][publisher]
- [Published PDF, all 20 pages][pdf]
- [Publisher HTML][html]
- [Hollocou et al., cited reference 27, primary full text][hollocou]
- [StreamCPI, cited reference 14, primary version 1 full text][streamcpi]
- [Assadi et al., cited reference 3, primary publisher record/abstract][assadi]

[publisher]: https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.SEA.2025.11
[pdf]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf
[html]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/html/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.html
[p3]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=3
[p5]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=5
[p6]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=6
[p7]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=7
[p8]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=8
[p9]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=9
[p10]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=10
[p11]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=11
[p12]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=12
[p13]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=13
[p15]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=15
[p20]: https://drops.dagstuhl.de/storage/00lipics/lipics-vol338-sea2025/LIPIcs.SEA.2025.11/LIPIcs.SEA.2025.11.pdf#page=20
[hollocou]: https://arxiv.org/html/1712.04337
[streamcpi]: https://arxiv.org/html/2410.07732v1
[assadi]: https://proceedings.mlr.press/v178/assadi22a.html
[restream]: https://doi.org/10.1145/2487575.2487696
[buffered]: https://doi.org/10.1145/3546911

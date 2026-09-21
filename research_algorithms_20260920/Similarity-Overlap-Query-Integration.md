# Similarity: Budget-Admitted Exact Pair Bounds In Complete Queries

Date: 2026-09-20. Implemented integration, completed independent code review and two serial public comparisons. The full-query performance result is negative for the extra pair bound under these fixed workloads. No new generic scheduler or physical-RAM claim.

## Question

The [query-cardinality theorem](Similarity-Overlap-Frontier-Compression.md) removes a potentially enormous intersection-count table from exact two-row metadata evaluation. Does it help a complete query after paying the index, source-feature lookup, block histogram, cheaper filters, output and fallback? The earlier selective experiment shows why this cannot be inferred from a tighter theorem: its disjoint-pair bound saved only 47 memberships beyond the strong interval control and lost in the sum-of-query-medians comparison.

This experiment integrates the theorem without weakening that comparator. The same full selective index serves every block-filter mode. Native target-posting DAAT remains a separate paid control, not an intentionally slower row-scan baseline.

## Explicit Admission

[The dispatcher](experiments/probe_admitted_similarity_bounds.py) validates counts and computes both reservations using only O(G) metadata state. It does not construct either potentially large DP table to discover its size.

Before admission it uses the reviewed feasible-gap recurrence. A leaf permits gaps from zero through K=2C-H. At an additive node the largest gap is the sum of the child maxima. At a strict node with D=C_left+C_right-C_parent, require D no greater than either child gap limit, then the parent limit is max(T_left,T_right)-D. Reject an incompatible root size gap u-ell. This catches globally impossible pair observations even when each individual capacity looks plausible.

The two cost proxies are:

```text
dense cells = sum_v [2*(min(I,K_v)+1) - indicator(min(I,K_v)=K_v)]
dense work  = 2*dense_cells + sum_internal cells_left*cells_right

threshold cells = 2*sum_v(Q_v+1)
threshold work  = existing leaf/strict/convolution/root loop reservation
```

Dense work charges twice the state bound plus child-pair attempts, covering state handling and root recovery in a conservative schedule metric. Threshold work uses its explicit scheduled-loop counter. These are different implementation schedules measured in logical units, not calibrated nanoseconds, machine instructions or byte counts. Choosing their smaller number is a deterministic heuristic, not a proved optimal runtime dispatcher.

There are three new modes: force the dense evaluator; force the query-threshold evaluator; or choose the admitted evaluator with the smaller (work,cells,name) tuple. Default per-evaluation limits are 4,096 table cells and 65,536 work units. The query also has a 10,000,000-unit cumulative reservation for these extra pair evaluations. Charge the full chosen reservation, not only its realized work. The next evaluation must fit the remaining reservation. Only one evaluator is allocated at a time.

If no allowed evaluator fits, the dispatcher returns no certificate, not score zero. The query retains the already computed sound interval upper bound and verifies more actual targets. Invalid metadata raises an error; it is not silently treated as an ordinary resource refusal.

## Integration Invariant

```text
source -> paid feature-to-block postings -> exact block counts
                                           |
                                    complete witness heap?
                                           |
                      union -> capacities -> interval bound
                                           |
                             still unresolved; exactly two rows
                                           |
                           O(G) feasibility and budget preflight
                                      /           \
                                  admitted       refused
                                     |              |
                              one exact solver  retain interval
                                      \           /
                                       safe prune?
                                           |
                                  surviving rows + output
```

The exact pair evaluator runs only after the top-k heap contains enough exact positive witnesses and all cheaper stages failed to prune. Both orientations and exact rational scores are preserved. Pair evaluation is restricted to blocks of population two; singleton tails and larger blocks use the existing interval path. The old `paired` mode remains unchanged as the disjoint-only baseline.

Pruning retains the existing score/ID order. A bound below the kth score is safe; equality is only pruned when the block's minimum original ID is strictly worse. Self rows cannot become witnesses. If fewer than k positive rows exist after all relevant blocks are exhausted, the original-ID index supplies the correct zero tail. This does not materialize a count-sized per-target accumulator.

## What The Budget Does Not Bound

The new cumulative budget covers extra pair-solver schedules only. Source reads, merge cursors, block occurrences, preflight metadata validation, union/interval filters, refused blocks, exact surviving-body work, output, and index construction still cost work. Exhausting it can increase body reads. It is not a total query deadline or a P100 latency guarantee.

Table cells are not physical RAM: a dense dictionary state and a threshold-list cell have different object overhead; numeric counts require bits; query buffers, file descriptors, heaps, and OS cache also count. The benchmark retains its fixture and oracle in process memory, so it cannot demonstrate physical 4 GB execution. The existing graph preparation is shared for comparison, with its costs reported separately rather than omitted.

## Verification Before Timing

Four missing-dispatcher failures preceded implementation. The first four integration tests then passed in 3.621 seconds. A missing-driver-option assertion failed before the driver was extended. The combined similarity suite then passed 59 tests in 20.037 seconds.

New coverage includes 8,192 complete file-backed queries over all 64 three-target fixtures, four source sets, self/no-self, four k values, and four mode configurations. All ordered results equal the independent direct-set oracle. **The independent reviewer correctly observed that this matrix does not enter the extra pair solver:** its first pair arrives before the witness heap is full, and its last block is a singleton. It exercises common ranking/output paths, not 8,192 admitted DP calls.

A separate positive-overlap separating fixture does enter the solver and prunes a two-row block that the interval control must verify. Zero state/work/query budgets cause correct fallback instead. A two-block case admits exactly one DP then refuses the next when its cumulative reservation is exhausted. Nonpair blocks, failed output cleanup, invalid budgets, impossible metadata, and a mock that fails on any solver call after refusal are separately checked. The review's distinct reservation-slack fixture is now retained: with query budget 76 and each dense call reserving 40 but using 36, exactly one call is admitted and two refused. Debiting actual work would incorrectly admit a second reservation. After adding this regression, the combined suite passed **60 tests in 19.866 seconds**, before either new public timing run.

These query tests complement rather than replace the preceding 29,440 pure actual-summary envelope checks, 1,024 deeper comparisons and independent mathematical review. The [completed implementation review](Similarity-Overlap-Integration-Review.md) inspected the kernels and independently executed 212 complete-result calls plus three admission/dispatch checks. It found no kernel defect under its immutable-input contract. Its file identifies which newer lead tests and driver behavior it did not inspect. The reviewer and all tests were terminal before timing; the two public benchmarks then ran sequentially.

## Public Workload Selection Before Timing

Use B=2, G=4, k=10, original-ID physical target order, six degree strata plus 24 seeded uniformly sampled remaining IDs, seed 920520, three repetitions, and rotated method order. No source is selected based on timing or certificate success. Compare all nine methods on every admitted source: union, leaf partition, laminar, exact interval, disjoint paired, native DAAT, overlap-dense, overlap-threshold and automatic overlap.

Keep selective source capacity 4,096 and increase the common feature-stream cursor admission to 2,048 for BOTH datasets and both access paths. This admits the Facebook high-degree strata instead of silently dropping them. Per-pair/cumulative budgets stay at the defaults above. The current process descriptor soft limit was observed as 1,048,575; 2,048 cursors are an admission cap, not the number always open. Reproduction on another system may need adequate process descriptor limits.

| Dataset | Normalized nodes | Simple undirected edges | Directed memberships | Full consecutive pairs | Disjoint full pairs |
| --- | ---: | ---: | ---: | ---: | ---: |
| ca-GrQc | 5,242 | 14,484 | 28,968 | 2,621 | 2,564 |
| Facebook combined | 4,039 | 88,234 | 176,468 | 2,019 | 3 |

The second source was chosen before timings because [SNAP reports high clustering for its combined ego graph](https://snap.stanford.edu/data/ego-Facebook.html). Parsing observes no loops or duplicate endpoint rows. Consecutive pairs share 12,651 feature memberships in total, with maximum 169 in a pair. These are input characteristics, not query-weighted savings or speed evidence. The public source is anonymized; only its combined edge file is used, not profile attributes. Source citation: McAuley and Leskovec, Learning to Discover Social Circles in Ego Networks, NIPS 2012.

Source data: [Facebook combined gzip](experiments/data/facebook_combined.txt.gz), SHA-256 `125e84db872eeba443d270c70315c256b0af43a502fcfe51f50621166ad035d7`. Both source files are small real graphs, not the separate 2 GB synthetic workload or a 50 GB storage test. Input normalization and query IDs are retained in each receipt. Directory/profile construction is not a free service provided by the oracle.

## Complete Public Results

Receipts: [ca-GrQc](Similarity-Overlap-GrQc-Results.json) and [Facebook](Similarity-Overlap-Facebook-Results.json). Each checks 810 ordered outputs: 30 distinct source queries, nine methods, three repetitions. These are **1,620 executions over 60 source queries**, not 1,620 independent query inputs. The ca-GrQc sources are the same 30 used in the preceding studies. Every result matches the independent direct-set oracle, including self exclusion and original-ID tie order.

Time below is the **sum of 30 per-query median times**, in milliseconds, not a batch measurement, p99, or confidence interval. Work counts include each distinct query once, not all three repeated runs. Body memberships are the memberships actually read to verify surviving target rows; DAAT obtains intersections from postings and does not read those bodies.

| Method | GrQc time, ms | GrQc bodies | GrQc body memberships | Facebook time, ms | Facebook bodies | Facebook body memberships |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Union-only selective | 98.691 | 1,566 | 20,622 | 875.647 | 4,818 | 267,211 |
| Leaf partition | 106.027 | 1,566 | 20,622 | 902.263 | 4,816 | 267,107 |
| Laminar | 107.420 | 1,566 | 20,622 | 908.294 | 4,816 | 267,107 |
| Exact interval | 109.260 | 1,276 | 15,184 | 908.501 | 4,302 | 236,399 |
| Disjoint paired | 111.968 | 1,268 | 15,137 | 914.845 | 4,302 | 236,399 |
| Overlap, forced dense | 120.363 | 1,268 | 15,137 | 1,205.572 | 4,292 | 235,918 |
| Overlap, forced threshold | 125.484 | 1,268 | 15,137 | 1,128.436 | 4,292 | 235,918 |
| Overlap, automatic | 120.535 | 1,268 | 15,137 | 1,033.935 | 4,292 | 235,918 |
| Native target-posting DAAT | 91.405 | Not used | Not used | 956.281 | Not used | Not used |

On GrQc, all overlap modes remove the same four additional blocks already removed by the disjoint method: eight bodies and 47 memberships, or 0.310% of interval's body memberships. Handling overlapping pairs adds no further body saving in these queries. Automatic overlap is 10.32% slower than interval and 31.87% slower than DAAT in the stated aggregate.

On Facebook, the overlap extension does find a new strict improvement: five extra blocks, ten target bodies and 481 memberships beyond interval/disjoint paired. That is **0.232% of interval's bodies and 0.203% of its body memberships**. Automatic overlap is nevertheless 13.81% slower than interval and 8.12% slower than DAAT. The five queries with additional body savings are IDs 212, 3822, 69, 3620 and 2446; their membership savings are 34, 70, 34, 34 and 309. The two highest-degree selected sources have no extra body saving. These IDs were not chosen using this outcome.

Automatic dispatch is 14.24% faster than forced dense and 8.37% faster than forced threshold on Facebook. This supports evaluating both representations under admission; it does not make the complete method competitive with the cheaper filters. On GrQc, automatic is 0.14% slower than forced dense, a small difference not interpreted as significant. No probability of winning on unseen customer data is estimated from these three-repeat observations.

## Where The Work Went

All selective modes share the same relevant-block posting traversal. Across the 30 GrQc queries, it emits 7,237 block memberships versus DAAT's 7,260 target memberships, only 23 fewer. On Facebook, it emits 155,786 versus 175,952, a saving of 20,166 (11.46%). The Facebook access benefit already exists in **union-only** mode, which is 8.43% faster than DAAT in the aggregate. It cannot be attributed to the new pair theorem. DAAT and block filtering do different later work, so this posting comparison alone is not a total-operation or time bound.

| Dataset / evaluator | Preflights | Admitted | Refused | Dense / threshold calls | Strictly tighter bounds | Reserved work, summed | Actual work, summed | Largest reserved cells in one call | Largest query work reservation |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| GrQc dense | 395 | 395 | 0 | 395 / 0 | 13 | 16,036 | 16,036 | 42 | 2,922 |
| GrQc threshold | 395 | 395 | 0 | 0 / 395 | 13 | 106,058 | 106,058 | 500 | 48,616 |
| GrQc auto | 395 | 395 | 0 | 145 / 250 | 13 | 12,419 | 12,419 | 42 | 1,950 |
| Facebook dense | 1,905 | 1,901 | 4 | 1,901 / 0 | 25 | 2,921,656 | 2,808,816 | 1,082 | 924,620 |
| Facebook threshold | 1,905 | 1,903 | 2 | 0 / 1,903 | 25 | 3,572,910 | 3,572,910 | 1,658 | 1,342,584 |
| Facebook auto | 1,905 | 1,905 | 0 | 755 / 1,150 | 25 | 1,113,844 | 1,098,754 | 1,574 | 400,648 |

The per-query budget is 10,000,000, so it never exhausts on these public inputs. Forced-mode refusals therefore come from per-evaluation admission. Automatic mode finds an admitted representation for all 1,905 Facebook preflights. Of those calls, 1,899 have nonzero shared target memberships. Thus the negative result cannot be dismissed as never exercising overlapping pairs. Only 25 of these 1,905 evaluations tighten the interval bound, and just five additional blocks cross the current witness threshold. Improving an upper bound is not equivalent to pruning a block.

These logical schedules are not homogeneous hardware operations. In particular, summing per-query peak cells would be meaningless as a peak-RAM figure; the table takes the maximum peak instead. GrQc's symbolic trillion-overlap state separation remains a theorem demonstration, not the regime encountered here.

## Preparation And Storage

| Stage, observed once | GrQc | Facebook |
| --- | ---: | ---: |
| Input normalization, ms | 14.105 | 38.685 |
| Canonical row writing, ms | 41.230 | 211.500 |
| Shared Snapshot index, ms | 268.334 | 1,473.639 |
| Laminar sidecar build, ms | 93.605 | 363.679 |
| Selective conversion build, ms | 185.004 | 453.538 |
| Sidecar intermediate bytes | 524,056 | 1,536,816 |
| Selective index retained bytes | 955,296 | 2,442,960 |
| Complete selective query-required files, bytes | 1,586,528 | 5,395,696 |
| Complete DAAT query-required files, bytes | 751,224 | 2,636,344 |

Selective query-required files are its index plus canonical target pairs, metadata and original-ID inverse index. DAAT requires original target intervals, directory, metadata and inverse index. The resulting selective footprints are 2.112x and 2.047x the respective DAAT footprints. These are serialized-file requirements, **not RAM ratios**. The matched block ablations retain the same full index; a standalone union-only implementation could remove unused capacity/population payloads.

The current builder materializes an intermediate sidecar and a shared Snapshot that also builds unused RMQ data. The receipts retain those costs, but this is not an optimized standalone build comparison or a measured lifecycle peak-storage/RSS experiment. The normalized Python graph and oracle remain resident. Source downloads, refresh, failed-build recovery, cold-cache behavior and native implementation costs are not included in these timings. No break-even query count can be inferred by assigning the whole shared builder cost exclusively to one method.

## Corrected Research Decision

1. **Keep the exact metadata theorem.** It remains a supported way to replace intersection-sized tables with query-sized tables, with explicit feasibility and conservative admission. The public query loss does not disprove that theorem.
2. **Reject the current unconditional extra-filter schedule as a speed improvement.** More overlap was insufficient: expensive evaluations almost never changed the pruning decision. The automatic solver choice reduces its own overhead but does not solve this issue.
3. **Keep native posting execution as a first-class control.** Saving target-body reads is not automatically valuable against a competitor that scores directly from postings. The next experiment must identify work the competent comparator really pays.
4. **Do not optimize only on the five successful query IDs.** A proposed eligibility rule needs an input-derived rationale, priced feature extraction, training/test separation if calibrated, and all refused queries retained in evaluation.
5. **Require a new scientific question before another filter variant.** Useful directions are a theorem characterizing when the interval relaxation is already attainable by a complete pair; decision-only recovery that stops once the kth-score threshold is certified; or a paid data layout where the small-Q/large-I regime occurs naturally. Each must be compared to established tree-DP, threshold-search and group-index methods. These directions are not yet algorithms, guarantees or successes.

For the product thesis, this is a concrete caution: lowering a subroutine's state can be mathematically substantial while producing almost no end-to-end resource saving. We must select formats and kernels by the complete input-to-answer contract rather than by the most dramatic symbolic table ratio.

Subsequent mathematical follow-on: [strict-orientation recovery](Similarity-Orientation-Parameter-Recovery.md) now implements an exact O(G*2^r)-visit/O(G)-word evaluator independent of numeric Q/I tables. It has a completed mathematical review and finite oracle tests, not a new public-query comparison. The raw nine-mode receipts and conclusions in this document predate that evaluator and remain unchanged.

## Closest-Art Update

Two additional primary papers were inspected during this integration checkpoint:

- [Li, Yu and Koudas, LES3 (2021)](https://arxiv.org/pdf/2107.10417), Sections 3.1 and 5.2: token/group membership indexing, group upper bounds and hierarchical pruning precede our access schedule. Their learned partitioning and bitmap representation are not reproduced by this experiment. The generic group-filter idea is therefore not our scientific delta; fixed original-ID pairing is also not a comparison against their full system.
- [Doron-Arad, Kulik and Shachnai, budgeted laminar matroid independent set (2023)](https://arxiv.org/pdf/2304.13984), Definition 3.1, Lemma 3.2 and Algorithm 1: profit-indexed minimum-cost tables and tree convolution are established. Their item-level independent-set formulation is not our exact jointly attained two-row summary problem. A reduction matching our special branch recurrence and count-only complexity remains to be established, rather than inferred from shared DP terminology.

These comparisons narrow the contribution to the joint-metadata feasible-tail theorem, exact two-orientation recurrence, and its query-count parameterization. They do not establish first publication of that result. Searches and inspected sections are not an exhaustive literature audit; unsuccessful PDF/HTML fetches were not treated as full-text evidence. No source paper's empirical speed/RAM numbers are transferred to Knight Bus.

## Reproduction

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
python3.11 research_algorithms_20260920/experiments/bench_selective_capacity_similarity.py --data research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz --output /tmp/overlap-grqc.json --include-overlap --merge-cap 2048
python3.11 research_algorithms_20260920/experiments/bench_selective_capacity_similarity.py --data research_algorithms_20260920/experiments/data/facebook_combined.txt.gz --output /tmp/overlap-facebook.json --include-overlap --merge-cap 2048
```

Run timing only after tests and reviewers are terminal, with one benchmark process at a time. Preserve each output, including negative comparisons. Cached Python/file timing is not dedicated-host, cold-cache, Rust, Neo4j/GDS, or whole-process RAM evidence.

| Artifact | SHA-256 at this integration checkpoint |
| --- | --- |
| Admission implementation | `9df3a59fa7dbb445ed298ffc4b6dd0a8b66e1baabcf4c6186d0071c7e633ab7c` |
| Selective implementation | `961594203c21e9d953824109b4c1356f91527b79b9bc9c96161cbab68b2e9a46` |
| Admission/integration tests | `9373a744780928dbb10029eb6688be05cea97905da179526648fdc7b48e493be` |
| Public comparison driver | `a2c3d69f51b51ee559305f716be457e9afeb3cbae4d52e2bd8bd9bf4e6561d26` |
| GrQc raw receipt | `f06dc6809191194981fae1f2d7e049183470df2edabe2f24077c65d872653e72` |
| Facebook raw receipt | `2f78c480d64d1c8df4f307bbfe992a13c767a287f634b46c57e9501ee21336ba` |
| Independent code review | `16b99d623265633dc7f3815711109b6fa4ea7238415ac72a00e9fca6abb77a96` |

## Remaining Scientific Claim

The positive theorem is exact query-cardinality-parameterized recovery from jointly attained pair metadata; the underlying matroid and dynamic-programming primitives remain credited. This integration tests its practical value, not publication priority. Even a useful result for this similarity family leaves the other six families' scientific contribution requirements open.

A targeted primary-source check found a related but different hardness statement in [Zhang et al.'s transformation-based set-search manuscript](https://www.jinwang18.net/files/tkde19-setknn.pdf), Section 4.2: selecting an optimal feature transformation is hard. Our review's reduction fixes the grouping and asks for the tight complete-pair envelope from already supplied counts. These are different input/optimization problems; neither their equivalence nor priority for our formulation follows from the shared phrase NP-hard. The paper's filter-versus-verification cost warning remains directly relevant to this experiment. The limited searches did not establish publication novelty.

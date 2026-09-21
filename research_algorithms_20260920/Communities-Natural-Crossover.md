# Communities: Natural Singleton Crossover

Date: 2026-09-20. Independent follow-on research. This document alone is owned by this task.

## Result

A three-type weighted family creates a length-Theta(n) **strictly moving, partial** cohort on sweep two from the declared singleton start. The exact trajectory is proved below. The family does not establish a paper-worthy separation: ordinary multiplicity-based quadratic line search reproduces the same prefix. The executable comparison below charges both engines for cached scores, indexed extrema, interval detection and edits, singleton preparation, and full original-ID output.

## Findings And Claim Boundary

**Keep this as a natural-start regression theorem and an honest crossover witness, not as the paper's novelty exhibit.** For every integer `b >= 3`, the fixed three-type template below gives `n=4b` vertices, ordinary singleton initialization, exactly three complete sweeps, and one naturally created moving prefix of length

`L = ceil((75b-39)/100) = Theta(n)`.

It is a **partial transfer** of an independent type, with strictly negative curvature, not a whole-clique merge and not no-op skipping. A matched indexed, type-cached scalar engine does more score updates, comparisons, and interval edits over the **entire** trajectory. However:

1. A generic exact integer quadratic line search has the same trajectory and essentially the same counters as CSCJ on this family. This obstruction is proved and executed, not inferred from a keyword resemblance.
2. The full-trajectory event reduction is at most a constant factor and tends to about 15.8% of scalar moving events. Preparation, the unbatchable first sweep, and full output do not disappear.
3. These are operation-count results from an explicit model, not measured algorithm speedups or a demonstrated external-memory implementation. The resource receipt includes the verifier and all three engines, so its elapsed time is not a fair per-engine benchmark.

The graph is deliberately engineered. "Natural" means the cohort arises from the specified singleton trajectory; it does not mean a random-graph model, a discovered real-world workload, or evidence that such cohorts are prevalent. The weights and `gamma=1/2` are fixed independently of `b`. This does not establish the same family at `gamma=1`.

Inputs read: current [Budgeted-Communities.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md) and the prior [independent review](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Community-Triangles-Independent-Review.md). The candidate already incorporates the stronger cached-score baseline; the older review's scan-baseline criticism is not reported as a new unresolved defect here. No other research file was edited.

## Fixed Graph Family

Partition the original IDs into these certified types, visited in this order:

| Type | Size | Original IDs | Internal distinct-pair weight |
| --- | ---: | --- | ---: |
| X | `2b` | `0 .. 2b-1` | 11 |
| Y | `b` | `2b .. 3b-1` | 0 |
| Z | `b` | `3b .. 4b-1` | 11 |

The symmetric template, zero self-loops, and resolution are

~~~text
W = [ 11  3  0
       3  0  4
       0  4 11 ],    p=1, q=2, gamma=1/2.

kX = 25b-11
kY = 10b
kZ = 15b-11
M  = 75b^2-33b.
~~~

Initialize token `i` on vertex `i`. Evaluate all active communities and fresh; choose the smallest token among equal positive gains; stay on zero gain. Fresh starts at token `4b`. Stop only after a complete no-move sweep. There is no contraction, alternate warm start, intermediate reordering, or quality threshold.

Write `H_ab = 2M W_ab-k_a k_b`. Its six distinct entries are

~~~text
HXX = 1025b^2-176b-121
HXY =  200b^2- 88b
HXZ = -375b^2+440b-121
HYY = -100b^2
HYZ =  450b^2-154b
HZZ = 1425b^2-396b-121.
~~~

A type-`a` cache score is `s_C=sum_z H_az*x_zC`. The current source score is `s_A-H_aa`, other community scores are `s_C`, and fresh has score zero. Twice the winning source/destination score difference is the exact change in `N=2M^2 Q`. This is the same scalar score contract for every comparator.

## Complete Trajectory Proof

### Sweep One Creates The Cohort

**X block.** For `b>=3`, `HXX>HXY>0>HXZ`. Vertex 0 chooses vertex 1's token, the earliest best singleton. Vertex 1 stays on its zero-gain tie with another X singleton. Each remaining X vertex joins token 1: the growing X community dominates singleton X, Y, Z, and fresh. This leaves one X interval and `2b-1` accepted moves.

Call token 1 community `P`. **Y block.** Before Y member `j`, for `0<=j<b`, its source is still its singleton with adjusted score zero. Community P has score `2b*HXY+j*HYY`. The strongest competing type is a singleton Z, of score `HYZ>0`; another Y singleton has negative score. Even at the minimum, `j=b-1`,

~~~text
2b*HXY + (b-1)*HYY - HYZ
  = 300b^3-526b^2+154b > 0  for b>=3.
~~~

Thus every Y joins P, creating a contiguous, same-source Y cohort by ordinary serial moves. It costs `b` scalar decisions and moves in every engine.

**Z block.** The score of P to a Z vertex is

~~~text
2b*HXZ + b*HYZ = -300b^3+726b^2-242b < 0  for b>=3.
~~~

Since `HZZ>0`, the first Z chooses the next Z singleton, token `3b+1`; the next Z stays on its zero tie; all remaining Z join that token. Call it `Q`. There are `b-1` Z moves.

The first sweep therefore ends at

~~~text
P = (2b X, b Y, 0 Z), token 1
Q = (0 X, 0 Y, b Z), token 3b+1
full label vector = [1]^(3b) ++ [3b+1]^b
accepted moves = 4b-2.
~~~

This charges, rather than bypasses, the singleton-first-sweep obstruction. Every moving event in this sweep has length one. Intervals can coalesce around already visited members, but the **unvisited suffix** still has distinct singleton labels until its members are visited.

### Sweep Two Has A Strict Partial Moving Run

Every X stays: its adjusted P score `(2b-1)*HXX+b*HXY` is positive, while Q has score `b*HXZ<0`.

At the start of the Y block, all `b` successive Y members have source P. With `r` predecessors transferred to Q,

~~~text
beta = HYY = -100b^2
g(0) = b*HYZ - (2b*HXY+(b-1)*HYY) = b^2*(150b-78)
g(r) = b^2*(150b-78-200r).
~~~

The Q score remains positive even if all available Y members were transferred:

`b*HYZ+r*HYY = b^2*(450b-154-100r) > 0` for `0<=r<=b`.

There are only two active communities, so fresh cannot overtake Q. The **source/stay inequality**, not an unchanged-community competitor, ends the run. Exactly

`L = ceil((150b-78)/200) = ceil((75b-39)/100)`

members have strictly positive gain. For `b>=3`, `1<=L<b`. There is no zero-gain boundary: `75b-39` cannot be divisible by 100, because 25 does not divide 39. Thus `g(L-1)>0>g(L)`. After the first remaining Y stays, all remaining Y stay on the unchanged state.

Every Z then stays. Its own adjusted Q score `(b-1)*HZZ+L*HYZ` is positive; P's score is smaller than its already negative first-sweep score because P has lost Y members.

### Sweep Three Is Stable

X still stays, since its source score is positive and

`score_X(Q)=b*HXZ+L*HXY <= b*(HXZ+HXY)<0`.

Z still stays by the previous inequality. For Y, put `h=100b^2` and let `d=s_Q-s_P` in the final state. Since `g(L)=d-h` and `-2h<g(L)<0`, we have `-h<d<h`. A Y in P has move gain `d-h<0`; a Y in Q has reverse gain `-d-h<0`. Both source scores remain positive against fresh. No vertex moves.

Consequently every `b>=3` instance terminates after precisely three sweeps, with move counts `[4b-2,L,0]`, no fresh-token allocation, and the **entire final original-ID label vector**

~~~text
[1]^(2b) ++ [3b+1]^L ++ [1]^(b-L) ++ [3b+1]^b.
~~~

The theorem is about the specified local-moving trajectory, not global modularity optimality. Permanently contracting all Y members cannot produce this split. Blindly moving all `b` Y members performs forbidden negative-gain decisions after the prefix.

## Matched Procedures And Costs

The retained executable model below gives all engines the same type array, template, degree table, exact BigInt coefficients, singleton labels, community-count maps, volume records, and indexed score heap. The baseline is not a scan of all communities at each vertex.

- Build the current type's score cache once per contiguous type block, using sparse `(community,type,count)` entries; heapify once.
- Query the best community excluding the source with a bounded heap frontier; compare fresh with exact score/token order.
- Update only source and destination cache keys, counts, and volumes after a move/event; delete empty source communities.
- Use a doubly linked interval list with a forward cursor. A transfer splits only at its two boundaries, rewrites the prefix token, and attempts two adjacent coalescences. There is no scan to rediscover a cohort and no loop relabeling its individual members in the algorithm path.
- Give **all three engines identical homogeneous no-op skipping**. Only accepted moves are scalarized in the baseline.
- Materialize final labels and actually serialize every original-ID row as UTF-8 `id,token\n` to a SHA-256 sink. Serialization and byte counts are real; disk persistence/fsync is not executed. The scalar and both batched models have identical full output bytes.

`scalar` uses length one for accepted moves. `cscj` applies both exact integer bounds at a multi-member negative-curvature run. `line` is an ordinary pairwise multiplicity line search: on this family's two-active-community moving block, use the source-gain bound alone. Its assertions restrict it to this proved family; it is **not** a generally correct replacement for competitor-aware CSCJ.

### Closed-Form Counter Differences

Put `d=L-1`. Over the complete three-sweep trajectory, the shared implementation yields:

| Counter | Cached scalar | CSCJ |
| --- | ---: | ---: |
| Individual accepted moves | `4b-2+L` | same |
| Moving events | `4b-2+L` | `4b-1` |
| Candidate decisions / run reads | `4b+L+7` | `4b+8` |
| Score-key updates | `8b-4+2L` | `8b-2` |
| Count updates; volume updates, each | `8b-4+2L` | `8b-2` |
| Actual interval splits | `L` | `1` |
| Actual interval merges | `4b+L-4` | `4b-3` |
| Interval field writes, including initialization | `44b-16+13L` | `44b-3` |
| Exact division calls | `0` | `2` |
| Peak live communities / intervals | `4b` / `4b` | same |
| Type-cache builds; cache rows; nonzero count terms | `9`; `7b+14`; `7b+25` | same |
| Input original-ID rows; full output rows | `4b`; `4b` | same |

The executed heap has exactly `3d` fewer lexicographic priority-comparator calls, `d` fewer strict-gain comparisons, and identical heap-swap totals. Boundary-split calls and adjacent-merge attempts each decrease by `2d`; forward cursor advances decrease by `d`. These implementation-specific counts are reproducible, not universal lower bounds.

There is no uncharged cohort-search pass. Its current length is obtained from the interval already at the visit cursor; split and coalescence maintenance are included above. Avoided interval writes are `13d`, so charging interval maintenance strengthens rather than erases the counted savings in this engine.

**What "beats" means here:** in a fixed-width word-cost model the additional certificate work is constant (two divisions, arithmetic, and a bounded extra extrema query), while avoided moving decisions and updates grow as `Theta(b)`. Thus the matched engine has a positive additive work advantage for sufficiently large `b`, including detection/interval costs. With unbounded exact integers, coefficients have `O(log b)` bits and the extra divisions have polylogarithmic bit cost; this still does not consume a linear number of avoided decisions. No architecture-independent crossover time or speedup factor follows from counting divisions and comparisons as interchangeable operations.

An array-label scalar implementation can avoid some interval bookkeeping; a bucketed singleton-score index can improve the common first sweep. The score-update reduction remains in the stated scalar-decision model, but this is not a proof against every implementation or every label representation. A scalar engine using lazy aggregate state and analytical prefix execution has itself crossed into the generic batching comparator.

### Full-Trajectory Obstruction

CSCJ still executes `4b-2` mandatory first-sweep moving events. Total moving-event savings are

`(L-1)/(4b-2+L) -> 3/19`, approximately 15.8%,

not an unbounded `L`-fold overall acceleration. Both algorithms also read/initialize `Theta(n)` original-ID state and emit `Theta(n)` labels.

The common indexed-heap first sweep has an `O(n log n)` upper bound, and this implementation's measured comparison counts reflect substantial common heap work. **Do not turn that upper bound into an unproved lower bound for the family.** Even a specialized `O(n)` singleton-score bucket implementation, granted equally to both engines, leaves both whole workflows `Theta(n)` in the fixed-width resident model. This family proves neither an asymptotic whole-workflow separation nor a physical-I/O reduction. No-op skipping contributes zero between-engine savings.

## Generic Batching Matches Exactly

For moving the first `r` Y members from P to Q after sweep one, the exact aggregate objective-numerator change is

~~~text
F(r) = 2*(r*g(0) + beta*r*(r-1)),   0 <= r <= b
F(r+1)-F(r) = 2*g(r).
~~~

This is a strictly concave integer quadratic. Its unique integer maximum is `r=L`, because the consecutive differences are positive before L and negative from L onward. Assigning that multiplicity to the earliest original IDs reproduces every scalar member choice and both final source/destination counts.

Therefore ordinary **pairwise block-coordinate ascent in multiplicity space with exact line maximization** gives the same partial cohort transfer. Nothing specific to a more elaborate event scheduler is needed on this witness. The third executable comparator verifies this equality over the full singleton-start trajectory; all its displayed counters equal CSCJ's except that it uses **one** division instead of two.

This is the exact obstruction to a paper-worthy delta on this family. It does not refute CSCJ's general competitor certificate. With three or more relevant communities, an unchanged competitor can overtake a destination **before** its pairwise quadratic optimum; a plain pairwise line maximizer then need not preserve the serial trace. But an event-aware generic optimizer can also add that breakpoint. Merely exhibiting it is not enough to claim novelty or a stronger complexity bound.

## Primary Prior Art

Bounded primary-source search performed on 2026-09-20 for modularity block-coordinate updates, batched Louvain moves, group moves, symmetry, and twin reductions. The following are actual adjacent algorithms, not claims that their published code implements this exact family or token schedule.

| Primary source | Verified collision | Important boundary |
| --- | --- | --- |
| Wang and Kolter, **Locale**, NeurIPS 2020, section 3.1 | Explicitly formulates local modularity moves as exact coordinate ascent, generalizes to low-cardinality assignment vectors, and performs closed-form block-coordinate updates. Generic "block-coordinate modularity optimization" is established. | Its block is a vertex's relaxed assignment vector, not an exact earliest-ID transfer count preserving this integer serial trajectory. This does not establish equivalence of Locale itself with CSCJ. [Primary paper](https://proceedings.neurips.cc/paper/2020/file/229aeb9e2ae66f2fac1149e5240b2fdd-Paper.pdf) |
| Zhang, Liu, Wen and Zhang, **A Sparse Completely Positive Relaxation of the Modularity Maximization for Community Detection**, 2017 preprint | Row-by-row block-coordinate descent for a sparse completely positive relaxation predates this proposal. | Different relaxation and stationarity goal; not evidence of exact serial-label replay. [Author preprint](https://arxiv.org/abs/1708.01072) |
| Waltman and van Eck, **Smart Local Moving**, 2013, section 3 | Starts from singletons and explicitly supports splitting communities and moving sets via subnetworks/reduced networks. "Move groups while permitting splits" is not a new objective. | It changes the search neighborhood and hierarchy, not a certificate for the next fixed serial prefix. [Author paper](https://arxiv.org/pdf/1308.6604) |
| Lu, Kalyanaraman, Halappanavar and Choudhury, **Parallel Heuristics for Scalable Community Detection**, 2014, section IV | Derives interaction corrections for simultaneous modularity moves and explains why independently positive proposals can give negative joint gain. This is close prior algebra, not merely an implementation comparison. | Coloring/parallel heuristics do not promise this deterministic serial trajectory. Their vertex-following reduction is also not permission to permanently contract Y here. [Primary paper](https://eecs.wsu.edu/~ananth/papers/Lu_MTAAP14.pdf) |
| Yu, Srinivasan and Thomo, **VLouvain**, EDBT 2026, sections 4.2 and 5.1 | Community-vector/degree aggregates and post-first-pass batched no-mover checks already eliminate edge expansion and stable-block work. | Its cited batched check skips blocks with no positive move; it is not this strict moving-prefix certificate. Its vector/Gram input semantics must not silently replace the declared weighted template. [Primary paper](https://openproceedings.org/2026/conf/edbt/paper-72.pdf) |

The strongest collision for **this** witness is the explicit quadratic reduction above, independent of whether a paper names precisely the same integer line-search specialization. It would be misleading either to declare the entire candidate anticipated by Locale or to treat an unsuccessful exact-name search as proof of novelty. No global novelty guarantee is made. Standard twin reduction, cached modularity scores, and integer quadratic line search are not claimed as inventions.

## Resource Scope: 4GB Physical / 50GB Retained

**Authoritative template input is essential to this experiment.** Both engines receive the same exact three-type template and ordered original-ID membership stream. No engine discovers twins by inspecting an expanded adjacency matrix. Degree/H construction uses the same constant-size arithmetic for both; singleton state preparation touches all `n` IDs. Already type-major IDs need no sorting. An unsorted imported ID stream would require equally charged sorting and retained old/new runs.

The expanded graph has `(11b^2-3b)/2 = Theta(n^2)` nonzero undirected edges. If correctness requires checking this template against an arbitrary expanded source, reading/validating those edges costs `Omega(n^2)` here. Compression cannot erase that input bill. A trustworthy factorized source contract can avoid expansion; a guessed three-type summary cannot.

The probe's live algorithm state is `O(n)` and peaks at **n communities and n intervals in every mode**, before cohorts form. It does not demonstrate a memory-capacity improvement. JS Map/object overhead, allocator retention, BigInt size and output buffers are not estimated from logical record counts. The measured process RSS below includes the validation machinery; it is only evidence for these nine small cases. `--max-old-space-size=512` is a V8 heap setting, **not** a physical-memory cap.

A deployable admission test must reserve the runtime/OS margin inside 4GB and account for all resident count, heap, handle-map, interval and original-ID buffers. If that state does not fit, both engines need equally budgeted external indexes; the present receipt does not implement them. Two logical score updates are not two physical writes, and the source/destination pages can remain hot throughout the second sweep.

For retained storage, require

`input + output + old checkpoint + new checkpoint + indexes + WAL + sort runs <= 50GB`.

For example, fixed-width `(64-bit original ID,64-bit token)` final output alone is `16n` bytes, independently of the number of intervals; actual retained encoding, metadata and checksum costs are additional. Do not replace this with four output interval records when the answer contract asks for original IDs. The probe serializes every final ID/label row and reports exact byte counts, but hashes the bytes instead of retaining an output file. A deployed disk run must additionally pay the output write and any durability boundary.

The nine-case process used no artifact files beyond this document. It provides **no** 4GB-limit stress test, 50GB retained-state benchmark, external-index I/O result, crash-recovery result, or real-data prevalence measurement.

## One Stronger Avenue

**Investigate defect-aware serial-prefix certificates, not another name for exact twins.** A concrete next input is this same family for even `b`, with a supplied arbitrary perfect matching of weight-one edges added inside Y. Keep the declared template-major original-ID visit order; do not regroup IDs into matching pairs. Every Y now has one exceptional neighbor. Pair mates can be a true-twin pair, but no exact Y class has size greater than two; the exact-type count grows as `Theta(b)`. Thus the current "make every defect endpoint a singleton" route loses the long exact-type cohort.

Use the three-type template plus the matching as the shared input for **both** candidate and cached scalar. All Y degrees are still equal (`10b+1`), so the current vertex's defect effect on a proposed P-to-Q score gap is explicitly

`2M' * ([mate currently in Q] - [mate currently in P])`.

Here `M'=M+b`; the baseline template score must also be recomputed using the new degrees and M'. This is a per-ID correction, not permission to ignore the defect. A range minimum over those corrected margins could certify many positive serial moves while visiting only defect-induced change points near a crossover.

This is a **proposed research target, not a proved or executed extension**. Before promotion, require all of the following:

1. Derive the singleton trajectory and a long strict moving prefix for a growing matching family, including its arbitrary-ID mate order and any changed stable split.
2. Specify the exact prefix-minimum maintenance and charge matching preparation, every mate-status update, interval fragmentation, score refresh and full output. Merely looping once over the whole proposed prefix to "certify" it does not win.
3. Compare with an equally informed generic piecewise-quadratic/block optimizer, not just a twin-refinement baseline. An aggregate block optimum is insufficient unless every accepted serial member decision agrees.
4. Show a total-work crossover after preparation, ideally on repeated later moving blocks so the common singleton sweep cannot dominate. Reject the direction if generic prefix batching has the same event bound.

This avenue changes the admitted structure and proof obligation in a substantive way, but even it may reduce to standard range-query/coordinate optimization techniques. The current three-type result is the control case for that falsification, not a substitute for it.

## Executed Receipt

Run: 2026-09-20, local Node.js **v24.9.0**. Exact BigInt score/gain/objective arithmetic; integer-sized ID/count loops for the tested range. Source extracted from the single JavaScript fence:

- Source SHA-256: `abb3143f88f4b07a79eeefcdaacc8d58b8acab1e43c48d795c28622b461a3f38`.
- SHA-256 of `JSON.stringify(JSON.parse(stdout))`: `799730e568043b54c8a15da26a2e16124983f10499198ba47e8b73ce918f89b9`.
- **9 families, 27 full trajectories, 0 mismatches.** Each comparator processes all three sweeps. There are 16,632 logical vertex visits per mode, 49,896 across all three modes.
- Full live label vectors checked at **17,879 event/no-op boundaries** across all runs. CSCJ and line-search member decisions and exact individual gain receipts are checked against scalar at every logical visit, not just final Q or partition equivalence.
- For `b=3,4,5,6,8`, independently recomputed expanded-graph modularity for every active destination and fresh at **312 scalar visits**. Larger cases use matched exact caches, full-label replay, closed-form expected labels/move counts, and final objective evaluation from counts, not exhaustive objective recomputation.
- Sizes include all four residues of `b mod 4`, covering both final signs of `s_Q-s_P` and the corresponding two-key heap-order changes.
- Fresh tokens never allocated: `nextFresh=n` in every run. The retained model deliberately asserts this restriction; it is not a fresh-community generality test.
- All engines serialize the complete original-ID output to a digest sink. Reported output hashes are equal across the three engines, with no label renumbering.

Validation work deliberately sits outside the algorithm counters: the verifier expands labels at every event, replays individual members, stores traces, and performs small expanded-objective checks. Its `O(n^2)` full-label checking must not be mistaken for the event engine's runtime bound. Conversely, the measured process time includes it, so it cannot establish an engine speedup.

`priorityComparisons` counts one score/token lexicographic comparator invocation; `gainComparisons` counts the strict positive-gain test. `scoreUpdates` includes removal of an empty source key. `intervalWrites` counts logical node-field assignments including initialization, not allocator or physical-storage writes. `splitTests` counts split-function invocations and `mergeTests` counts adjacent-merge attempts, not individual machine branches. Certificate entry count and division count are reported separately. These are transparent algorithmic counters, not an instruction-accurate profiler.

### Full-Trajectory Counts

`S / J` below means matched scalar / CSCJ. Generic line-search counts equal J in this table. Every case has moving counts `[4b-2,L,0]`; J's only multi-member moving event is the second-sweep prefix of length L.

| b | n | L | Moving events S / J | Score updates S / J | Priority comparisons S / J | Interval writes S / J | Output bytes, each |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 12 | 2 | 12 / 11 | 24 / 22 | 99 / 96 | 142 / 129 | 55 |
| 4 | 16 | 3 | 17 / 15 | 34 / 30 | 140 / 134 | 199 / 173 | 77 |
| 5 | 20 | 4 | 22 / 19 | 44 / 38 | 180 / 171 | 256 / 217 | 99 |
| 6 | 24 | 5 | 27 / 23 | 54 / 46 | 224 / 212 | 313 / 261 | 121 |
| 8 | 32 | 6 | 36 / 31 | 72 / 62 | 306 / 291 | 414 / 349 | 164 |
| 16 | 64 | 12 | 74 / 63 | 148 / 126 | 709 / 676 | 844 / 701 | 338 |
| 64 | 256 | 48 | 302 / 255 | 604 / 510 | 3575 / 3434 | 3424 / 2813 | 1650 |
| 256 | 1024 | 192 | 1214 / 1023 | 2428 / 2046 | 17194 / 16621 | 13744 / 11261 | 6954 |
| 1024 | 4096 | 768 | 4862 / 4095 | 9724 / 8190 | 81514 / 79213 | 55024 / 45053 | 32938 |

At `b=1024`, all engines pay nine cache builds, 7,182 community cache rows, 7,193 nonzero count terms, 4,096 initial communities, 4,096 initial intervals, 4,096 input rows and 4,096 complete output rows. Scalar / CSCJ / line-search division calls are `0 / 2 / 1`; exact splits are `768 / 1 / 1`; exact merges are `4860 / 4093 / 4093`; gain comparisons are `4871 / 4104 / 4104`; heap swaps are `28195 / 28195 / 28195`. There is no peak-state reduction.

The `b=3` entire final label vector is `[1,1,1,1,1,1,10,10,1,10,10,10]`. For `b=1024`, the entire vector is exactly `[1]^2048 ++ [3073]^768 ++ [1]^256 ++ [3073]^1024`; the probe checks all 4,096 entries, not merely these run lengths. Its objective numerator is `6961317928763392` over denominator `2*(78609408)^2`.

A separate resource-observation run of the **same source**, using `awk ... | /usr/bin/time -l node --max-old-space-size=512`, exited 0 and reported:

~~~text
0.41 real, 0.48 user, 0.02 sys
maximum resident set size: 133971968 bytes
peak memory footprint:      96445608 bytes
swaps:                     0
block input operations:    0
block output operations:   0
~~~

These are one-run local observations of the combined probe, not reproducible timing constants or proof of zero storage activity on a deployed graph workflow. Deterministic evidence is the source, receipt hashes, full-label checks and counters.

### Per-Case Exact Digests

The trace digest covers all logical `[sweep,originalID,sourceToken,targetToken,integerGain]` rows; stays use gain zero. The output digest covers every serialized `id,token\n` row. The source prints these, a separate full-label-array digest, final objective numerators, and **every** engine counter.

| b | Trace SHA-256 | Full Output SHA-256 |
| ---: | --- | --- |
| 3 | `e20fd9c426051d3494fbd4a46e99a7e85e2fc07c40d1512dc030ba31a2d8fecd` | `53c98db25b0ff896b7579c7866e5e128b5c356e7f061dfe50b91a5cd98c6cdef` |
| 4 | `5c9c4046545f8f96c079315cec580edd79e13c10385aec94eeca57549b6b99c3` | `913b83dff7ad0feace5127ecc1e198ac63f0ede41dee38610ee27f9e4d0e674c` |
| 5 | `b715681e2f6d20bfd5240e9aaffb2b56f94ac87c637eaf129bf7edb91fc2d69c` | `920a593ccd504c25fc24159174314e66cf0ab1578113249a5f58f2a6572a88e4` |
| 6 | `c56e07f1020846fc653daa7adf6ff1e3ab8762678ad457b1a813307459557b71` | `d814ede41645de83995d2946456608ac4e13407dc971ec6fff0421357bb60c13` |
| 8 | `ca69dc3376a3b165fb9312f9c6b2d8f4c51c2baa0be5cdbdc6639aac3b6b8a7b` | `3d5154c72a59b99aea49c06357947b0b6ab1f0c32102c78a97e34f908df4b7f9` |
| 16 | `d0f54bc8341fd58c7214ac4640ad90b7ed4bc7e92cd069c6f986b109e605c8dd` | `ace515a7a396ea2ec41a35b80975b52f449c538c0cd54630c23d32da2f3d0af8` |
| 64 | `4a05d3f3505ea8e6d0b9deee8e487b7bf5fe63421eab1beb7b17e3bec2467e87` | `a2f8af764ca0147ce127a4233828606c25ef6247991d7739c418ec6e57376e54` |
| 256 | `dfd7419660d196aa3b20d162fe94b0b0425a1a5cccc3838f15e8a833f126100f` | `741728ee4c5e9d7b3e845a22d63326e7c64b16adde6bfebf3de6bad9efe3eff3` |
| 1024 | `bb4b4c81cbcd0e4022579ed6f8b7534b2b239a0e7ad89898f02004f07229674d` | `e266dc49a2b8583edd50aa3aefbe0ad03f1760c167208b1c4936e799cd3f26ac` |

## Executable Probe

This is a retained research model, not production code. It runs entirely in memory and writes no files. Extract this document's single JavaScript fence and pipe it to Node.js:

```sh
awk '/^```javascript$/{p=1;next} /^```$/{if(p)exit} p' research_algorithms_20260920/Communities-Natural-Crossover.md | node
```

```javascript
const assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const W = [[11n,3n,0n],[3n,0n,4n],[0n,4n,11n]];
function hashCanonicalJsonValue(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}
function buildThreeTypeFamily(b) {
  const sizes = [2*b,b,b], n = 4*b;
  const types = Array.from({length:n}, (_,i) => i<2*b ? 0 : i<3*b ? 1 : 2);
  const degrees = sizes.map((_,a) =>
    sizes.reduce((s,v,z) => s+BigInt(v)*W[a][z],0n)-W[a][a]);
  const M = sizes.reduce((s,v,a) => s+BigInt(v)*degrees[a],0n);
  const H = W.map((row,a) => row.map((w,z) => 2n*M*w-degrees[a]*degrees[z]));
  return {b,sizes,n,types,degrees,M,H};
}
function evaluateExpandedObjectiveNumerator(f,labels) {
  let internal=0n; const volumes=new Map();
  for(let i=0;i<f.n;i++) {
    volumes.set(labels[i],(volumes.get(labels[i])||0n)+f.degrees[f.types[i]]);
    for(let j=0;j<f.n;j++)
      if(i!==j && labels[i]===labels[j]) internal+=W[f.types[i]][f.types[j]];
  }
  return 2n*f.M*internal-[...volumes.values()].reduce((s,k)=>s+k*k,0n);
}
function chooseExpandedObjectiveMove(f,labels,pos) {
  const source=labels[pos], before=evaluateExpandedObjectiveNumerator(f,labels);
  let target=source, delta=0n;
  const tokens=[...new Set(labels),f.n].sort((a,b)=>a-b);
  for(const token of tokens) if(token!==source) {
    labels[pos]=token;
    const trial=evaluateExpandedObjectiveNumerator(f,labels)-before;
    if(trial>delta || (trial===delta && trial>0n && token<target)) {
      delta=trial; target=token;
    }
  }
  labels[pos]=source;
  return {target,gain:delta/2n};
}
function runCachedIntervalEngine(f,mode,reference=null) {
  const c={inputRows:f.n,templateCells:9,initialCommunities:f.n,
    initialIntervals:f.n,cacheBuilds:0,cacheRows:0,cacheTerms:0,
    decisions:0,priorityComparisons:0,gainComparisons:0,heapSwaps:0,
    scoreUpdates:0,countUpdates:0,volumeUpdates:0,events:0,
    runReads:0,cursorSteps:0,splitTests:0,splits:0,mergeTests:0,merges:0,
    intervalWrites:6*f.n,divisions:0,certificateTests:0,outputRows:0,outputBytes:0,
    peakIntervals:f.n,peakCommunities:f.n};
  const communities=new Map();
  let head=null,tail=null,live=f.n;
  for(let i=0;i<f.n;i++) {
    const a=f.types[i];
    communities.set(i,{x:new Map([[a,1]]),total:1,volume:f.degrees[a]});
    const node={lo:i,hi:i+1,a,token:i,prev:tail,next:null};
    if(tail) {tail.next=node;c.intervalWrites++;} else head=node;
    tail=node;
  }
  let heap=[],positions=new Map();
  function compareHeapPriorityKeys(u,v) {
    c.priorityComparisons++;
    return u.score>v.score || (u.score===v.score && u.id<v.id);
  }
  function swapIndexedHeapEntries(i,j) {
    [heap[i],heap[j]]=[heap[j],heap[i]];
    positions.set(heap[i].id,i);positions.set(heap[j].id,j);c.heapSwaps++;
  }
  function repairHeapDownwardFrom(i) {
    while(2*i+1<heap.length) {
      let j=2*i+1;
      if(j+1<heap.length && compareHeapPriorityKeys(heap[j+1],heap[j])) j++;
      if(!compareHeapPriorityKeys(heap[j],heap[i])) break;
      swapIndexedHeapEntries(i,j);i=j;
    }
  }
  function repairHeapUpwardFrom(i) {
    while(i>0) {
      const p=Math.floor((i-1)/2);
      if(!compareHeapPriorityKeys(heap[i],heap[p])) break;
      swapIndexedHeapEntries(i,p);i=p;
    }
  }
  function changeCachedCommunityScore(id,delta,remove) {
    c.scoreUpdates++;
    const i=positions.get(id);
    assert.notEqual(i,undefined);
    if(remove) {
      const last=heap.pop();positions.delete(id);
      if(i<heap.length) {heap[i]=last;positions.set(last.id,i);}
    } else heap[i].score+=delta;
    if(i<heap.length) {
      if(i>0 && compareHeapPriorityKeys(heap[i],heap[Math.floor((i-1)/2)]))
        repairHeapUpwardFrom(i);
      else repairHeapDownwardFrom(i);
    }
  }
  function findBestUnexcludedCommunity(excluded) {
    const frontier=heap.length?[0]:[];
    while(frontier.length) {
      let best=0;
      for(let k=1;k<frontier.length;k++)
        if(compareHeapPriorityKeys(heap[frontier[k]],heap[frontier[best]])) best=k;
      const i=frontier.splice(best,1)[0], item=heap[i];
      if(!excluded.has(item.id)) return item;
      if(2*i+1<heap.length) frontier.push(2*i+1);
      if(2*i+2<heap.length) frontier.push(2*i+2);
    }
    return null;
  }
  function mergeFreshCandidateToken(candidate) {
    const fresh={id:f.n,score:0n};
    return candidate && compareHeapPriorityKeys(candidate,fresh) ? candidate : fresh;
  }
  function splitIntervalAtPosition(node,pos) {
    c.splitTests++;
    if(pos===node.lo || pos===node.hi) return node;
    assert(pos>node.lo && pos<node.hi);
    const right={lo:pos,hi:node.hi,a:node.a,token:node.token,
      prev:node,next:node.next};
    c.intervalWrites+=6;
    if(node.next) {node.next.prev=right;c.intervalWrites++;}
    node.next=right;node.hi=pos;c.intervalWrites+=2;
    c.splits++;live++;c.peakIntervals=Math.max(c.peakIntervals,live);
    return right;
  }
  function rewriteIntervalMovingPrefix(node,pos,len,token) {
    node=splitIntervalAtPosition(node,pos);
    splitIntervalAtPosition(node,pos+len);
    node.token=token;c.intervalWrites++;
    c.mergeTests++;
    if(node.prev && node.prev.a===node.a && node.prev.token===token) {
      const left=node.prev;
      left.hi=node.hi;left.next=node.next;c.intervalWrites+=2;
      if(node.next) {node.next.prev=left;c.intervalWrites++;}
      node=left;c.merges++;live--;
    }
    c.mergeTests++;
    if(node.next && node.next.a===node.a && node.next.token===token) {
      const right=node.next;
      node.hi=right.hi;node.next=right.next;c.intervalWrites+=2;
      if(right.next) {right.next.prev=node;c.intervalWrites++;}
      c.merges++;live--;
    }
    return node;
  }
  function expandCurrentIntervalLabels() {
    const labels=Array(f.n);
    let end=0;
    for(let node=head;node;node=node.next) {
      assert.equal(node.lo,end);end=node.hi;
      for(let i=node.lo;i<node.hi;i++) labels[i]=node.token;
    }
    assert.equal(end,f.n);return labels;
  }
  const trace=[],sweeps=[],replay=Array.from({length:f.n},(_,i)=>i);
  const oracle=mode==="scalar" && f.b<=8 ? replay.slice() : null;
  let objectiveChange=0n,maxJump=0,checked=0,logical=0;
  for(let sweep=0;sweep<10;sweep++) {
    let pos=0,node=head,activeType=-1,moves=0;
    while(pos<f.n) {
      while(node.hi<=pos) {node=node.next;c.cursorSteps++;}
      const a=node.a,A=node.token,run=node.hi-pos,beta=f.H[a][a];
      c.runReads++;
      if(a!==activeType) {
        activeType=a;c.cacheBuilds++;heap=[];positions=new Map();
        for(const [id,community] of communities) {
          let score=0n;c.cacheRows++;
          for(const [z,v] of community.x) {
            score+=f.H[a][z]*BigInt(v);c.cacheTerms++;
          }
          positions.set(id,heap.length);heap.push({id,score});
        }
        for(let i=Math.floor(heap.length/2)-1;i>=0;i--) repairHeapDownwardFrom(i);
      }
      c.decisions++;
      const source=heap[positions.get(A)].score-beta;
      const best=mergeFreshCandidateToken(findBestUnexcludedCommunity(new Set([A])));
      const g=best.score-source;
      c.gainComparisons++;
      const moving=g>0n;
      let len=moving?1:run;
      if(moving && run>1 && mode!=="scalar") {
        c.certificateTests++;
        if(beta>=0n) len=run;
        else {
          c.divisions++;
          len=Math.min(run,Number(1n+(g-1n)/(-2n*beta)));
          if(mode==="cscj") {
            const runner=mergeFreshCandidateToken(
              findBestUnexcludedCommunity(new Set([A,best.id])));
            const eps=runner.id<best.id?1n:0n;
            c.divisions++;
            len=Math.min(len,Number(1n+(best.score-runner.score-eps)/(-beta)));
          } else {
            // A family-specific ordinary pairwise integer line search.
            assert.equal(communities.size,2);
            assert(best.id<f.n && best.score+BigInt(run-1)*beta>0n);
          }
        }
      }
      const target=moving?best.id:A;
      assert(target<f.n,"this receipt intentionally excludes fresh-creation cases");
      for(let r=0;r<len;r++) {
        const gain=moving?g+2n*BigInt(r)*beta:0n;
        const record=[sweep,pos+r,A,target,gain.toString()];
        if(reference) assert.deepEqual(record,reference.trace[logical]);
        if(oracle) {
          const independent=chooseExpandedObjectiveMove(f,oracle,pos+r);
          assert.equal(independent.target,target);assert.equal(independent.gain,gain);
          oracle[pos+r]=target;
        }
        trace.push(record);replay[pos+r]=target;logical++;
      }
      if(moving) {
        c.events++;c.countUpdates+=2;c.volumeUpdates+=2;
        maxJump=Math.max(maxJump,len);moves+=len;
        objectiveChange+=2n*(BigInt(len)*g+beta*BigInt(len)*BigInt(len-1));
        const src=communities.get(A),dst=communities.get(target);
        const remaining=src.x.get(a)-len;
        if(remaining) src.x.set(a,remaining);else src.x.delete(a);
        dst.x.set(a,(dst.x.get(a)||0)+len);
        src.total-=len;dst.total+=len;
        src.volume-=BigInt(len)*f.degrees[a];dst.volume+=BigInt(len)*f.degrees[a];
        changeCachedCommunityScore(A,-BigInt(len)*beta,src.total===0);
        changeCachedCommunityScore(target,BigInt(len)*beta,false);
        if(src.total===0) communities.delete(A);
        node=rewriteIntervalMovingPrefix(node,pos,len,target);
      }
      // Verifier, deliberately excluded from algorithm counters.
      assert.deepEqual(expandCurrentIntervalLabels(),replay);checked++;
      if(oracle) assert.deepEqual(replay,oracle);
      pos+=len;
    }
    const labels=expandCurrentIntervalLabels();
    sweeps.push({moves,labels});
    if(reference) assert.deepEqual(sweeps[sweep],reference.sweeps[sweep]);
    if(moves===0) break;
  }
  const labels=expandCurrentIntervalLabels(),outputDigest=createHash("sha256");
  for(let i=0;i<labels.length;i++) {
    const row=Buffer.from(`${i},${labels[i]}\n`);
    outputDigest.update(row);c.outputRows++;c.outputBytes+=row.length;
  }
  const L=Math.ceil((75*f.b-39)/100),P=1,Q=3*f.b+1;
  const expected=Array.from({length:f.n},(_,i)=>
    i<2*f.b ? P : i<2*f.b+L ? Q : i<3*f.b ? P : Q);
  assert.deepEqual(labels,expected);
  assert.deepEqual(sweeps.map(s=>s.moves),[4*f.b-2,L,0]);
  let finalNumerator=-2n*f.M*f.sizes.reduce((s,n,a)=>s+BigInt(n)*W[a][a],0n);
  for(const {x} of communities.values())
    for(const [a,v] of x) for(const [z,w] of x)
      finalNumerator+=BigInt(v)*f.H[a][z]*BigInt(w);
  const initialNumerator=-f.sizes.reduce((s,n,a)=>s+BigInt(n)*f.degrees[a]**2n,0n);
  assert.equal(initialNumerator+objectiveChange,finalNumerator);
  if(oracle) assert.equal(finalNumerator,evaluateExpandedObjectiveNumerator(f,labels));
  return {mode,c,trace,sweeps,labels,maxJump,checked,
    finalNumerator:finalNumerator.toString(),nextFresh:f.n,
    labelsHash:hashCanonicalJsonValue(labels),traceHash:hashCanonicalJsonValue(trace),
    outputHash:outputDigest.digest("hex")};
}
const results=[];
for(const b of [3,4,5,6,8,16,64,256,1024]) {
  const f=buildThreeTypeFamily(b),scalar=runCachedIntervalEngine(f,"scalar");
  const cscj=runCachedIntervalEngine(f,"cscj",scalar);
  const line=runCachedIntervalEngine(f,"line",scalar);
  assert.equal(scalar.traceHash,cscj.traceHash);assert.equal(scalar.traceHash,line.traceHash);
  assert.equal(scalar.outputHash,cscj.outputHash);assert.equal(scalar.outputHash,line.outputHash);
  results.push({b,n:f.n,L:Math.ceil((75*b-39)/100),moves:scalar.sweeps.map(s=>s.moves),
    nextFresh:scalar.nextFresh,finalNumerator:scalar.finalNumerator,
    labelsHash:scalar.labelsHash,traceHash:scalar.traceHash,outputHash:scalar.outputHash,
    engines:[scalar,cscj,line].map(x=>({mode:x.mode,c:x.c,maxJump:x.maxJump,
      checkedFullLabelBoundaries:x.checked})),
    fullObjectiveOracle:b<=8,fullLabels: b<=8 ? scalar.labels : undefined});
}
console.log(JSON.stringify({runtime:process.version,cases:results.length,
  mismatches:0,results},null,2));
```

## Bounded Extension: Matching Defects Force Fragmentation

Date: 2026-09-20, follow-on to the preceding result. Everything above, including its probe and receipt, is preserved. This section resolves one question raised in "One Stronger Avenue": **does one exceptional neighbor per member still permit sublinear post-cohort state and moving-event counts?**

**Answer: not uniformly.** For arbitrarily large inputs in that exact matching extension, every completed reference run has linearly many final ID-order label runs and needs linearly many contiguous moving events *after* its initial long moving prefix. This is a representation/event-model obstruction, not a lower bound for every possible algorithm. A stronger same-input scalar baseline processes the remaining moves through delete-only endpoint sets in almost-linear total work.

Two new deductions make the obstruction precise:

1. **Pair-freezing theorem.** After a forced prefix, each subsequent accepted move reunites one split matching pair; that pair never moves again. The complete remaining move count is known from one matching cut.
2. **Fragmentation theorem.** Some matchings force every reachable local optimum to have `Omega(n)` ID-order transitions. A contiguous-prefix event changes at most two transitions, so even an ideal certificate cannot compress the remaining moving trace to `o(n)` such events.

These results go beyond the previous pure-quadratic witness, but do **not** establish a paper-worthy new community algorithm.

### Contract And Notation

Use the preceding three-type graph with `n=4b`, now with `b` divisible by 8 and `b>=16`. Add an arbitrary supplied perfect matching `F` of weight-one edges within Y. Its endpoints are still visited in the original Y ID order; do not sort them into pairs. No other graph weights, resolution, singleton initialization, token rules, or stopping rule change.

Write local Y ranks as `0,...,b-1`, `P=1`, `Q=3b+1`. The new values are

~~~text
kX = 25b-11,  kY = 10b+1,  kZ = 15b-11
M' = 75b^2-32b
D  = 2M' = 150b^2-64b
h  = kY^2 = 100b^2+20b+1
B  = h+D = 250b^2-44b+1
T  = 150b^3+73b^2-10b.
~~~

Here `B` is a score band, not the memory budget. The base score matrix still uses `H'_az=2M'W_az-k_a k_z`, but the matching affinity must be added separately. Relevant entries are

~~~text
H'XY = 200b^2-107b+11
H'YZ = 450b^2-161b+11
H'YY = -h
H'XZ = -375b^2+440b-121.
~~~

The matching has `f=b/2=Theta(n)` edges. Its maximum defect degree is one, but its absolute size is not `o(n)`. The result does not refute an honest bound that already charges `f` or output size.

### Lemma: The Same First Sweep Still Occurs

The X clique forms P exactly as before. Before the `j`-th Y visit, P's score is at least `2b H'XY-jh`; the matching adds either zero or `D`, never a negative term. Its worst-case margin over a Z singleton is

`2b H'XY-(b-1)h-H'YZ = 300b^3-584b^2+202b-10 > 0`.

An unvisited matching mate's singleton has score `D-h`, which is smaller than `H'YZ`. Thus all Y join P, independently of the matching. Z then forms Q because

`2b H'XZ+b H'YZ = -300b^3+719b^2-231b < 0`.

Hence the actual singleton sweep again takes `4b-2` moves and ends at P containing all X/Y and Q containing all Z. This is not a supplied warm partition.

Afterwards X and Z never move: for X, even `b(H'XZ+H'XY)<0` bounds its score to Q; for Z, P's already negative score only decreases as Y leave. Their own source scores are positive. For a Y member, source scores against fresh are bounded below by

~~~text
in P: 2b H'XY-(b-1)h = 300b^3-134b^2+41b+1 > 0
in Q:  b H'YZ-(b-1)h = 350b^3- 81b^2+30b+1 > 0.
~~~

Thus no fresh community is created; only the two existing communities compete throughout the remaining trajectory.

### Theorem 1: Pair Freezing And Exact Remaining Work

Let `r` be the number of Y members in Q. Their base unadjusted score difference is

`d(r)=s_Q-s_P=T-2hr`.

The actual score gap for a Y member is

~~~text
P -> Q: d(r)-h + D * ([mate in Q]-[mate in P])
Q -> P: -d(r)-h + D * ([mate in P]-[mate in Q]).
~~~

A positive gap is an accepted scalar move; twice the gap is the exact objective-numerator gain. This formula includes the mate identity that an ordinary type multiplicity omits.

When `d>B`, every Y still in P prefers Q even if its mate is in P. Starting sweep two with all Y in P therefore forces the first

`k = ceil((T-B)/(2h)) = 3b/4-1`

original-ID members to move. At `k-1` transfers the band has not been reached; at `k` transfers `-B<=d<=B`. For the displayed simplification, put `r0=3b/4` and note

~~~text
d(r0)   =  43b^2-(23/2)b
d(r0-1) = 243b^2+(57/2)b+2 <= B       for b>=16
d(r0-2) = 443b^2+(137/2)b+4 > B.
~~~

**The band is invariant.** Inside `[-B,B]`, moving away from a same-community mate has gap `d-B<=0` or `-d-B<=0`, so it cannot happen. A split pair's P endpoint moves only if `d>h-D`; afterwards `d'=d-2h>-B` and `d'<=B`. Its Q endpoint moves only if `d<D-h`; afterwards `d'=d+2h<B` and `d'>=-B`.

Consequently every accepted move after the forced prefix reunites a split pair, and every reunited pair is permanently inactive. Because

`D-h=50b^2-84b-1>0`,

the two endpoints of any still-split pair have gaps summing to `2(D-h)>0`. At least one endpoint is eligible, so a complete no-move sweep cannot leave a split pair.

Let `c` count matching edges crossing the fixed prefix cut `[0,k) | [k,b)`. Exactly those `c` pairs are split at band entry. Each requires precisely one additional accepted move. Therefore

~~~text
accepted moves after sweep one = k+c <= b
accepted moves over the full run = 4b-2+k+c <= 5b-2.
~~~

No conjectured sweep bound is needed for this count. While unresolved pairs remain, a complete unchanged sweep is impossible; every move decreases their number. Arbitrarily many repeated moves by one matching pair are ruled out.

At termination every matching edge is internal, so r is even. If both communities contain Y, stability requires `|d(r)|<=B`. All-Y-in-P and all-Y-in-Q violate stability for these b. The only even r in the band is `r0=3b/4`: `r0-2` is above it and `r0+2` is below it. Thus **every reachable local optimum puts exactly three quarters of Y into Q, in whole matching pairs**. Since k is odd, c is odd; the remaining moves consist of `(c+1)/2` P-to-Q completions and `(c-1)/2` Q-to-P completions.

This theorem fixes the cardinality and remaining work, not which matching pairs finish in Q. That identity still depends on the serial order.

### Strongest Same-Input Baseline

A scalar baseline need not rescan all Y in every later sweep, build an exact type for every matching pair, or maintain a range-minimum tree for every defect update.

Give both methods the template, the matching's mate lookup, and the preceding lemmas. Allow both to generate the fixed first sweep and forced prefix symbolically, with their implied scalar gains and ID labels, rather than materializing n singleton heap nodes. Input validation, mate-index construction, and the complete original-ID output still cost `Theta(n)` word operations for this input profile.

At band entry, maintain just the two ordered sets of **unresolved** endpoints, classified by their unchanged P/Q labels. Their eligibility is global within each set:

~~~text
P endpoints eligible exactly when d-h+D > 0
Q endpoints eligible exactly when -d-h+D > 0.
~~~

For each scalar move, query the smallest eligible ID after the current cursor. If neither eligible set has a successor, wrap to the next sweep. Remove the chosen endpoint **and its mate** from the sets, record their common final label, and update r/d. Both eligible sets can be considered in constant many successor queries. No settled pair is revisited. Skipped decisions really are stays; the cursor/wrap rule prevents revisiting an earlier ID within the same sweep. The final no-move sweep is represented explicitly as an unchanged logical sweep.

An ordinary balanced-tree implementation gives `O(n+c log(c+2))` whole-workflow word work and `O(n)` words. An even stronger standard implementation exploits that the endpoint sets are **delete-only**:

- Store each endpoint set as a fixed sorted array plus a sentinel.
- Precompute the two static endpoint ranks after each Y ID in `O(b)` space/work, so a cursor successor query needs no binary search.
- Deleting array position i unions it with i+1. A union-by-size/path-compression structure stores the maximum position in each component; that maximum is its first remaining successor.
- Each completion uses constant many finds/unions. The two structures have `O(c)` entries.

This gives `O(n+c*alpha(c+2))` total word work, `O(n)` words, and exactly c scalar completion moves. The inverse-Ackermann primitive is established union-find, not a new algorithmic contribution. [Tarjan's primary technical report](https://www2.eecs.berkeley.edu/Pubs/TechRpts/1974/Archive/ERL-m-434.pdf)

The endpoint-selection **semantics** are checked below against expanded-graph scalar decisions. The union-find implementation and its physical resource behavior were not built or benchmarked here. This is a derived same-input comparator, not a claim that an existing community package implements this specialization. In particular, an `O(n * number_of_sweeps)` scan baseline is unnecessarily weak for this family.

### Theorem 2: Some Matchings Force Linear Final Runs

For every b divisible by 8 with `b>=128`, there exists a perfect matching F for which **every** matching-closed Y subset of size `3b/4` has more than

`K=floor((b-1)/16)`

transitions in the fixed Y ID order.

**Proof by counting.** Choose a perfect matching uniformly at random. For a fixed even-sized subset S of size `s=3b/4`, the probability that no matching edge crosses it is exactly

~~~text
Pr[S is matching-closed]
 = (s-1)!! (b-s-1)!! / (b-1)!!
 = binom(b/2,s/2) / binom(b,s).
~~~

There are at most `2*sum_{j=0}^K binom(b-1,j)` binary strings with at most K transitions, including all the relevant balanced ones. Put `H(x)=-x ln x-(1-x)ln(1-x)`. The elementary binomial bounds give

~~~text
Pr[some matching-closed balanced S has <= K transitions]
 <= 2(b+1) exp((b-1)H(1/16) - (b/2)H(3/4))
 <  2(b+1) exp(-0.047b)
 <  1                         for every b>=128.
~~~

Here `H(1/16)<0.234` and `H(3/4)>0.562`. The final bound is below 0.63 at 128 and decreases thereafter. A matching avoiding every bad subset therefore exists. This is an existence theorem with an exponentially decreasing failure bound under a *uniform* random matching, not an efficient certificate that a specific sampled matching has the universal property.

Combine it with Theorem 1. The completed reference output necessarily has at least `K+2=Omega(b)` Y runs. At band entry Y had just two runs. Any accepted event that overwrites **one contiguous original-ID interval with one token** can increase the transition count by at most two, regardless of how clever its mathematical certificate is. If E is the number of such events after band entry, then

`E >= ceil((transitions_final-1)/2) >= ceil(K/2) = Omega(b) = Omega(n)`.

This applies even to a more powerful oracle that knows every scalar decision in advance. It is not a failure to maintain a fast range-minimum index.

### What Is Actually Ruled Out

For these worst-case matchings:

- A linked-interval state like CSCJ's needs `Omega(n)` live final nodes, even though the first sweep produced one Y cohort and the forced prefix left just two Y runs.
- The remaining scalar work is c one-time pair completions, with `c<=b/4+1`. The final-run bound forces `Omega(b)` contiguous events, so there is no unbounded asymptotic compression factor for this remaining moving trace.
- The supplied matching itself has linear edge count, and final original-ID output has n rows. Neither preprocessing nor output disappears. The delete-only scalar comparator is already almost linear overall.

These are **not** information-theoretic auxiliary-memory lower bounds for arbitrary representations. A label bitset or final pair bitset can avoid pointer-heavy interval nodes; recomputation or a noncontiguous symbolic output representation is outside the event lower bound. An event that labels an arbitrary matching-defined subset in one operation is also outside it, and must separately account for that subset and prove serial equivalence. Physical I/O does not follow from logical event counts.

Under 4GB physical / 50GB retained, this result removes the proposed uniform state advantage rather than certifying either engine's feasibility. The matching index, endpoint structures, label bits, original-ID map, old/new checkpoints and full output must all be admitted inside the respective reservations. If the matching lookup spills, its random accesses cannot be charged as constant I/O. No 4GB/50GB stress test was performed in this extension.

### Why This Is Not The Old Quadratic Example

Let `G=T-h` and let `cut_F(r)` count matching edges crossing the first r Y ranks. Along a hypothetical uninterrupted prefix transfer, the exact numerator change is

`Phi(r)=2*(rG-h*r*(r-1))-2D*cut_F(r)`.

The prefix-cut term depends on endpoint identities, not just r or the three template multiplicities. Its next difference is plus or minus one depending on whether the next vertex's mate has already moved. A single integer quadratic line maximum no longer describes the serial problem.

Nevertheless, granting a generic optimizer the same matching lets it compute prefix cuts in a linear pass and exploit the pair-freezing reduction. The obstruction is **ordered fragmentation versus the chosen contiguous event format**, not a claim that arbitrary defect-aware batching is novel or impossible.

### Actual Prior-Art Boundary

- **Graph summary plus corrections is established.** Navlakha, Rastogi and Shrivastava's SIGMOD 2008 model explicitly stores an aggregate graph and edge corrections for lossless reconstruction. Keeping one matching correction per pair is not a new representation. [Primary paper](https://navlakhalab.net/pubs/sigmod2008.pdf)
- **Modularity with local interactions is established.** Reichardt and Bornholdt formulate community detection through a spin-glass/Potts objective and give local update rules. The base quadratic plus matching interaction here is a specialization, not a new optimization principle. [Author paper](https://arxiv.org/pdf/cond-mat/0603718)
- **Random matchings disrupting one-dimensional structure are established.** Bollobas and Chung study a cycle plus a random matching, proving small diameter. Their result is an adjacent construction, **not** the label-transition theorem used here; the required counting argument is proved above. [Author-hosted primary paper](https://fanchung.ucsd.edu/mypaps/fanpap/104cyclematch.pdf)
- **Delete-only successor acceleration uses old machinery.** Tarjan's weighted union/path-compression analysis supplies the amortized bound. Neither this primitive nor generic active-set skipping is counted as a new method. The additional claim here is the family-specific proof that settled pairs never need reactivation.

The pair-freezing characterization and the resulting ordered-event obstruction are deductions of this extension. The search does not establish that this exact conjunction has never appeared elsewhere. No global novelty guarantee, new algorithm name, or paper-ready novelty claim is made.

### New Evidence Only

The original nine cases were **not rerun** for this extension. A separate, small mathematical probe uses the matching-perturbed graphs:

- All 105 perfect matchings on eight Y members, plus one fixed-seed matching each at `b=16,32,128`: **108 new trajectories, zero mismatches**.
- **3,600** Y gain formulas checked against scores recomputed directly from the expanded adjacency matrix and current full labels.
- **215** post-prefix scalar moves checked against the two-set endpoint-selection rule, including original IDs, sweep numbers, source/destination tokens and exact gains.
- Every run checks first-sweep labels, final matching closure, final cardinality, band invariance, one-time pair completions, and `later_moves=k+c`.
- The fixed seed is a reproducibility device, not the uniform randomness assumed in the existence proof. The sampled outputs do not certify the universal low-transition-subset property of those individual matchings.
- No timing, score-cache speedup, union-find implementation, or full-output I/O claim is inferred from this verifier. Its adjacency expansion and set scans are intentionally validation work.

| b | Forced prefix k | Crossing pairs c | Moves by full sweep | Final Y transitions | Completion moves / maximal contiguous moving runs |
| ---: | ---: | ---: | --- | ---: | ---: |
| 16 | 11 | 3 | `[62,13,1,0]` | 5 | 3 / 2 |
| 32 | 23 | 9 | `[126,25,4,3,0]` | 9 | 9 / 6 |
| 128 | 95 | 23 | `[510,97,4,4,4,4,4,1,0]` | 35 | 23 / 21 |

For b=128, the concrete reference output has 36 Y runs, so even a hindsight contiguous-event engine needs at least 17 completion events. Its actual maximal identical-direction moving runs number 21. The first long prefix does not describe the rest of the workflow.

The exact combinatorial union-bound check at b=128 uses K=7 and evaluates

~~~text
low-transition binary strings = 189581406208
bound numerator   = 92615623702033913202032640
bound denominator = 1477806921502280666682474774300
ratio             < 0.000062672 < 1.
~~~

This verifies the finite base calculation; the entropy inequality supplies the all-size existence proof.

### Extension Reproduction

The following separate `js` fence deliberately leaves the original `javascript` fence and its extraction command unchanged. Run only this extension from the repository root:

~~~sh
awk '/^```js$/{p=1;next} /^```$/{if(p)exit} p' research_algorithms_20260920/Communities-Natural-Crossover.md | node
~~~

Extension source SHA-256: `c97d4865a2e7a37abc17fb12588e5603878c0b850ffabed4aba58e7547766b98`.

SHA-256 of `JSON.stringify(JSON.parse(stdout))`: `4f000c82f4d4123f1f29671f6891569ddcf2c6cd9ee2ef148cbebf58774a32a9`.

The JSON receipt retains per-instance matching, full-label and complete scalar-trace digests. The original first 42,129 bytes remain unchanged, SHA-256 `eda12ea470676deb4704e448aaa45e0524257bf99ec3940ea694ab3c93e57e3a`.

```js
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
function digestCanonicalJsonValue(x) {
  return crypto.createHash("sha256").update(JSON.stringify(x)).digest("hex");
}
function chooseExactBinomialCount(n,k) {
  let r=1n;
  for(let i=1;i<=k;i++) r=r*BigInt(n-i+1)/BigInt(i);
  return r;
}
function generateSeededPerfectMatching(b) {
  let state=731;
  const a=Array.from({length:b},(_,i)=>i),mate=Array(b);
  for(let i=b-1;i>0;i--) {
    state=(Math.imul(state,1664525)+1013904223)>>>0;
    const j=state%(i+1);[a[i],a[j]]=[a[j],a[i]];
  }
  for(let i=0;i<b;i+=2) {mate[a[i]]=a[i+1];mate[a[i+1]]=a[i];}
  return mate;
}
function enumerateSmallPerfectMatchings(left,mate,emit) {
  if(!left.length) {emit(mate.slice());return;}
  const u=left[0];
  for(let j=1;j<left.length;j++) {
    const v=left[j];mate[u]=v;mate[v]=u;
    enumerateSmallPerfectMatchings(left.filter(x=>x!==u && x!==v),mate,emit);
  }
}
function verifyDeletingEndpointSchedule(mate,forced,T,h,D,trajectory) {
  const b=mate.length,P=1,Q=3*b+1;
  const pending=new Set(mate.flatMap((m,i)=>(i<forced)!==(m<forced)?[i]:[]));
  const expected=trajectory.filter(x=>x[0]>=1 &&
    !(x[0]===1 && x[1]<2*b+forced) && x[2]!==x[3]);
  let cursor=forced-1,sweep=1,r=forced,checked=0,runs=0,previous=null;
  while(pending.size) {
    const d=T-2n*h*BigInt(r);
    const eligible=i=>i<forced ? -d-h+D>0n : d-h+D>0n;
    let candidates=[...pending].filter(i=>i>cursor && eligible(i));
    if(!candidates.length) {
      cursor=-1;sweep++;
      candidates=[...pending].filter(eligible);
    }
    const v=Math.min(...candidates),source=v<forced?Q:P,target=source===P?Q:P;
    const gain=source===P?d-h+D:-d-h+D;
    const row=[sweep,2*b+v,source,target,gain.toString()];
    assert.deepEqual(row,expected[checked++]);
    if(!previous || previous[0]!==sweep || previous[1]+1!==row[1] ||
      previous[2]!==source || previous[3]!==target) runs++;
    previous=row;pending.delete(v);pending.delete(mate[v]);
    r+=source===P?1:-1;cursor=v;
  }
  assert.equal(checked,expected.length);
  return {checked,runs};
}
function verifyMatchingScalarTrajectory(mate) {
  const b=mate.length,n=4*b,P=1,Q=3*b+1;
  const type=Array.from({length:n},(_,i)=>i<2*b?0:i<3*b?1:2);
  const W=[[11,3,0],[3,0,4],[0,4,11]];
  const A=type.map((a,i)=>type.map((z,j)=>i===j?0:
    W[a][z]+(a===1 && z===1 && mate[i-2*b]===j-2*b?1:0)));
  const k=A.map(row=>row.reduce((s,v)=>s+v,0)),M=k.reduce((s,v)=>s+v,0);
  const h=BigInt(10*b+1)**2n,D=2n*BigInt(M);
  const T=150n*BigInt(b)**3n+73n*BigInt(b)**2n-10n*BigInt(b);
  const labels=Array.from({length:n},(_,i)=>i),moves=[],trajectory=[];
  const band=h+D, forced=Number((T-band+2n*h-1n)/(2n*h));
  const cutPairs=mate.slice(0,forced).filter(v=>v>=forced).length;
  let formulaChecks=0,entered=false,completionMoves=0;
  for(let sweep=0;sweep<4*b;sweep++) {
    let changed=0;
    for(let v=0;v<n;v++) {
      const src=labels[v],scores=new Map(),volumes=new Map(),affinity=new Map();
      for(let u=0;u<n;u++) if(u!==v) {
        const id=labels[u];
        volumes.set(id,(volumes.get(id)||0)+k[u]);
        affinity.set(id,(affinity.get(id)||0)+A[v][u]);
      }
      for(const id of new Set(labels))
        scores.set(id,2n*BigInt(M)*BigInt(affinity.get(id)||0)-
          BigInt(k[v])*BigInt(volumes.get(id)||0));
      scores.set(n,0n);
      const source=scores.get(src);
      let target=src,best=source;
      for(const [id,score] of scores)
        if(score>best || (score===best && score>source && id<target)) {
          target=id;best=score;
        }
      if(sweep>0 && type[v]===1) {
        const r=labels.slice(2*b,3*b).filter(x=>x===Q).length;
        const d=T-2n*h*BigInt(r),m=labels[2*b+mate[v-2*b]];
        const expected=src===P ? d-h+(m===Q?D:-D) :
          -d-h+(m===P?D:-D);
        assert.equal(scores.get(src===P?Q:P)-source,expected);formulaChecks++;
        if(d<=band && d>=-band) entered=true;
        if(entered) {
          assert(d<=band && d>=-band);
          if(target!==src) {
            assert.equal(target,m);assert.notEqual(src,m);completionMoves++;
          }
        } else {
          assert.equal(sweep,1);assert(v-2*b<forced);
          assert.equal(src,P);assert.equal(target,Q);
        }
      }
      if(target!==src) {assert(target<n);labels[v]=target;changed++;}
      trajectory.push([sweep,v,src,target,(best-source).toString()]);
    }
    moves.push(changed);
    if(sweep===0)
      assert.deepEqual(labels,Array.from({length:n},(_,i)=>i<3*b?P:Q));
    if(!changed) break;
  }
  assert.equal(moves.at(-1),0);
  assert(labels.slice(0,2*b).every(x=>x===P));
  assert(labels.slice(3*b).every(x=>x===Q));
  const y=labels.slice(2*b,3*b).map(x=>Number(x===Q));
  assert.equal(y.reduce((s,v)=>s+v,0),3*b/4);
  assert(y.every((v,i)=>v===y[mate[i]]));
  assert.equal(completionMoves,cutPairs);
  assert.equal(moves.slice(1).reduce((s,v)=>s+v,0),forced+cutPairs);
  if(b>=16) assert.equal(forced,3*b/4-1);
  const transitions=y.slice(1).reduce((s,v,i)=>s+Number(v!==y[i]),0);
  const frontier=verifyDeletingEndpointSchedule(mate,forced,T,h,D,trajectory);
  assert(frontier.runs>=Math.ceil((transitions-1)/2));
  return {b,forced,cutPairs,completionMoves,moves,transitions,formulaChecks,frontier,
    fullLabelHash:digestCanonicalJsonValue(labels),
    traceHash:digestCanonicalJsonValue(trajectory),
    matchingHash:digestCanonicalJsonValue(mate),y:y.join("")};
}
const allEight=[];
enumerateSmallPerfectMatchings(Array.from({length:8},(_,i)=>i),[],mate=>
  allEight.push(verifyMatchingScalarTrajectory(mate)));
assert.equal(allEight.length,105);
const selected=[16,32,128].map(b=>verifyMatchingScalarTrajectory(
  generateSeededPerfectMatching(b)));
const b=128,k=Math.floor((b-1)/16),s=3*b/4;
let strings=0n;
for(let j=0;j<=k;j++) strings+=2n*chooseExactBinomialCount(b-1,j);
const numerator=strings*chooseExactBinomialCount(b/2,s/2);
const denominator=chooseExactBinomialCount(b,s);
assert(numerator<denominator);
console.log(JSON.stringify({
  cases:allEight.length+selected.length,mismatches:0,
  allEight:{count:allEight.length,
    maxSweeps:Math.max(...allEight.map(x=>x.moves.length)),
    minTransitions:Math.min(...allEight.map(x=>x.transitions)),
    maxTransitions:Math.max(...allEight.map(x=>x.transitions)),
    formulaChecks:allEight.reduce((s,x)=>s+x.formulaChecks,0),
    frontierChecks:allEight.reduce((s,x)=>s+x.frontier.checked,0),
    receiptHash:digestCanonicalJsonValue(allEight)},
  selected,unionBound:{b,k,allLowTransitionStrings:strings.toString(),
    numerator:numerator.toString(),denominator:denominator.toString()}
},null,2));
```

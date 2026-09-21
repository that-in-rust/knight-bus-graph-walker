# Similarity: Recovering Disjunctive Constraints From Two-Row Summaries

Date: 2026-09-20. A05 candidate extension with executable probe and public cost evidence, not a completed novelty claim.

## Why This Is A Different Question

The [laminar-capacity review](Similarity-Laminar-Capacity-Review.md) exposes a gap: a bound can be tight for one individually plausible target while impossible for every complete block consistent with the same stored facts. The first public capacity experiment also shows that useful algebra does not guarantee useful query cost.

This note investigates the information gap, not a faster name for the same rank calculation. A very restricted case admits exact recovery using fields the existing summary already stores: a block of exactly two disjoint target rows. The new candidate is a population-aware interpretation of attained maxima; the evaluator is ordinary finite-state tree dynamic programming.

## Eligibility Is Observable

Let the two targets have cardinalities ell<=u and exact union U of size H. There are exactly two records, so their total membership count is ell+u. Hence

```text
H = ell + u  <=>  the two rows are disjoint.
```

Block population, H, ell and u are already available. No query-dependent oracle, extra pairwise intersections or additional index fields are required to recognize eligibility. A block that fails this condition must use another safe bound; silently assuming disjointness is incorrect.

The exact original IDs remain distinct, but contents may be empty. The score convention is the same zero-on-empty-intersection convention as the original capacity study. All source features count in a=|S|, even those outside U.

## Recover A Disjunction, Not Just An Interval

For feature-tree node v, let H_v=|U intersect group(v)| and C_v be the stored maximum target occupancy. Pick either row and call its occupancy x_v. Its complement has occupancy H_v-x_v, so exact attainment of C_v requires

```text
max(x_v, H_v-x_v) = C_v
       <= equivalently =>
x_v is one of {C_v, H_v-C_v}.
```

Thus a summary that superficially looks like an upper bound actually gives a two-point domain. The interval H_v-C_v<=x_v<=C_v loses the excluded interior values. Root domain is {ell,u}.

This deduction uses ALL of: population two, union completeness, disjointness and the fact that C_v is an attained maximum, not merely a conservative upper bound. A stale loose cap is unsafe for this interpretation even if it remains safe for the original upper-only method.

## Exact Two-State Dynamic Program

Let q_v=|S intersect U intersect group(v)|. For each possible row occupancy x in D_v={C_v,H_v-C_v}, define F_v(x) as the greatest query overlap attainable by that row inside v while satisfying every descendant domain.

```text
LEAF:
  F_v(x) = min(q_v,x)

INTERNAL:
  F_v(x) = max over y in D_left, z in D_right, y+z=x:
                F_left(y) + F_right(z)
  absent combination means infeasible

ROOT:
  UB_pair = max over feasible x in {ell,u}:
               F_root(x) / (a + x - F_root(x))
  zero overlap gives score zero
```

Every node has at most two distinct occupancy states and checks at most four child-state combinations. Evaluation takes O(G) arithmetic operations and O(G) retained state after the union/query histograms, for G leaf groups. This is not a new generic tree DP. Its proposed value is recovering the correct discrete feasible set from an already paid metadata contract.

### Correctness And Whole-Block Attainment

At a leaf, choose min(q_v,x) source features and fill the remaining x positions from non-source union features. There are enough: if x>q_v, x-q_v<=H_v-q_v since x<=H_v. This realizes the leaf maximum.

At an internal node, feature domains are disjoint. Child witness sets can be combined exactly when their sizes sum to the parent size. Maximizing over every child-state pair is therefore exact. Induction gives an attaining first-row subset T of U at the root.

Make the second row U minus T. At every node their maximum occupancy is C_v by the two-point constraint. Their union is exactly U, and root sizes are ell and u. They reproduce every stored group maximum and both length extrema at the original block population. Assign the original two IDs arbitrarily; a scalar score maximum over the rows does not require a particular ID association.

For fixed x, Jaccard increases with overlap, so maximizing F_root(x) before evaluating the ratio is valid. Taking both root sizes accounts for either row. This proves the exact maximum over ALL complete two-row blocks with these stored facts. Self-exclusion and a fixed association between ID and individual size can restrict the eligible set further; the result remains a conservative bound in that setting, not an exact eligible-key envelope.

### Relation To The Earlier Envelope

Every complete compatible block supplies rows admissible under the earlier upper-capacity relaxation. Therefore UB_pair<=UB_laminar. Strict improvement is possible because interior occupancies allowed by the matroid relaxation need not preserve attained maxima in a two-row block.

## Separating Example

Use two feature groups A={a1,a2,a3,a4}, D={d1,d2,d3,d4}:

```text
observed row 1: a1 a2 a3 d1
observed row 2: a4 d2 d3 d4
query:          a1 a2 d1 d2

union sizes:    A=4, D=4
maximum counts: A=3, D=3
row sizes:      4 and 4
```

The upper-only envelope allows the query itself as a target: its counts (2,2) fit both caps. It returns 1. Even the per-group lower/upper rectangle [1,3] allows (2,2).

The exact domains are {1,3} in both groups. A size-four row must have counts (1,3) or (3,1). Either permits at most three of the query's four features. The exact complete-block bound is 3/(4+4-3)=3/5, attained by the displayed first row.

With a previously verified kth score strictly between 3/5 and 1, this can reject the block while the upper-only bound cannot. Equality still requires the safe original-ID tie rule. This is a separation from specified summary interpretations, not a claim of dominance over systems retaining the actual rows or richer metadata.

## Storage And Workflow Costs

- No additional retained payload relative to the full laminar sidecar.
- During union streaming, additionally count all union tokens per leaf, not only query matches. Aggregate those counts up the same tree.
- O(G) additional query integers/states, explicit in admission accounting; not zero computation or zero RAM.
- Query still reads the sparse union and the capacity payload before it can prune. The earlier experiment's metadata overhead remains.
- Two-row blocks can have substantially more headers/capacities per membership than 64-row blocks. All alternatives must be compared with the SAME two-row partition for the incremental study, and with ordinary DAAT for actual utility.
- Pair reordering to encourage eligible or informative summaries is NOT free. The initial probe keeps physical order fixed.
- If the entire literal two-row payload is cheaper than its union and summaries, retaining/reading the rows can be the better solution. Information-theoretic strengthening does not defeat that engineering counterargument.
- The exact certificate needs coherent generation publication and recomputation of attained maxima during updates. Safe stale upper bounds are insufficient here.

## Prior Art And Contribution Boundary

The earlier study and its review cite exact set retrieval, feature-count rectangles, group norm bounds and laminar matroids. This extension is an application of complement identities and ordinary tree constraint satisfaction. The current bounded literature inspection has not established whether this exact complete-block envelope has appeared before; absence from inspected passages is not proof of novelty.

Potentially useful contribution: a formal example of stronger exact retrieval bounds obtained by interpreting attained extrema jointly with group population, together with a linear-state exact solver on an observable domain and a paid workflow study. A substantial paper would need either a broader useful class or convincing real-data economics against strong indexes. Neither is assumed.

## Verification Plan

1. Exhaustively enumerate small disjoint target pairs and all compatible complete blocks to check equality, not just safety on the observed pair.
2. Preserve the eight-feature counterexample as an exact regression.
3. Compare complete query outputs with the direct-set oracle, including ineligible-block fallback, zero scores, self-exclusion and ties.
4. Verify that the new bound actually avoids the rejected block's body.
5. Compare union, flat, nested, paired and DAAT on the same public queries with build/storage/query costs included.

## Executed Evidence

Implementation: [capacity probe](experiments/probe_laminar_capacity_similarity.py), [tests](experiments/test_laminar_capacity_similarity.py), [public driver](experiments/bench_laminar_capacity_similarity.py). Two paired tests first failed because the pure function and query mode were missing, then passed. A later lazy-evaluation regression failed on the unsupported mode, then passed. The combined similarity suite passed 33 tests in 11.047 seconds after the lazy change. Adding the review's population and attained-capacity guardrail regressions gives the final 35 passing tests in 10.475 seconds.

The paired oracle enumerates all 41 unordered disjoint pairs over four features. For each pair it independently enumerates every complete compatible complementary block, then all 32 source sets including an outside feature: 1,312 exact complete-block comparisons. The eight-feature 1-versus-3/5 counterexample is an additional regression. The current complete-file matrix checks 10,240 ordered answers over 64 three-target fixtures, source/self/k combinations and five capacity modes. It includes ineligible-block fallback and zero tails. Counts are repeated contract cases, not independent customer workloads.

### First Public Run: Eager Evaluation

[Five-mode receipt](Similarity-Paired-Capacity-Results.json): the same 30 ca-GrQc sources, three repetitions, 450 complete-output checks. Fixed B=2 consecutive target records and G=4 modulo feature buckets. Of 2,621 blocks, 2,564 (97.8%) are eligible disjoint pairs. That demonstrates eligibility on this particular sparse projection, not representative prevalence or economic value.

| Metric, summed once per source | Upper-only laminar | Paired envelope | Change |
| --- | ---: | ---: | ---: |
| Exact target bodies read | 1,856 | 1,570 | 15.4% fewer |
| Exact body memberships read | 22,666 | 17,558 | 22.5% fewer |
| Blocks pruned | 77,702 | 77,845 | 143 additional |
| Sum of query median times, ms | 1,161.313 | 1,557.582 | 34.1% higher |

Seventeen of 30 queries read fewer target bodies. Across all query/block visits, the paired envelope is strictly tighter 638 times. Both modes use the identical 524,056-byte full sidecar. Union-only and leaf-only sidecars use 377,280 and 461,152 bytes. DAAT's sum of medians is 106.444 ms and it wins every query; its different retained posting index is paid, not absent.

### Correction: Escalate Only When Necessary

The first schedule needlessly evaluated the stronger bound on all eligible blocks, including blocks already rejected by the old bound and blocks encountered before a full result heap existed. The `paired_lazy` mode is an ordinary filter cascade:

```text
no complete exact heap yet -> read exact block; do not evaluate pair DP
old bound certifies prune  -> prune; do not evaluate pair DP
eligible surviving block  -> evaluate stronger pair DP
otherwise                 -> use old bound and exact evaluation
```

This preserves answers and pruning decisions relative to the eager pair schedule. Before a full heap, neither could prune. When the old bound prunes, the stronger bound cannot require reading the block. Otherwise both evaluate the same stronger envelope from the same exact heap. Induction over block order gives the same actual row evaluations and heap trajectory.

[Six-mode cascade receipt](Similarity-Paired-Cascade-Results.json): 540 new complete-output executions of the same 30 inputs. The full comparison is:

| Mode | Sum of per-query median times, ms | Body targets | Body memberships | Pair DP evaluations |
| --- | ---: | ---: | ---: | ---: |
| Union plus lengths | 799.446 | 1,856 | 22,666 | 0 |
| Leaf partition | 1,033.242 | 1,856 | 22,666 | 0 |
| Upper-only laminar | 1,185.670 | 1,856 | 22,666 | 0 |
| Paired eager | 1,588.399 | 1,570 | 17,558 | 76,920 |
| Paired lazy | 1,224.925 | 1,570 | 17,558 | 746 |
| Ordinary DAAT | 106.061 | Different candidate unit | Relevant postings, not all bodies | 0 |

The lazy schedule preserves every eager body-read count and complete output; it eliminates 99.0% of pair DP evaluations. It is faster than eager on all 30 inputs, but faster than upper-only laminar on only one input, by a negligible observed margin. It loses to DAAT on all 30. This directly rules out promoting the new envelope as a measured retrieval speedup on this fixture.

These first three receipts add 1,350 complete query executions: 360 in the 64-row study, 450 in the eager two-row study, and 540 in this corrected study. They are repetitions/configurations of the same 30 source queries. They are not 1,350 independent graph workloads. The initial two-row run is retained rather than overwritten by its corrected schedule.

### Serial Timing Correction

The first three benchmark runs overlapped some research-test execution. Their exact answers and logical counters remain evidence, but their elapsed times are exploratory measurements with a known interference confound. After the final 35-test process and the independent reviewer were terminal, the six-mode comparison was run again with no other agent/test/benchmark process from this task active.

[Final serial receipt](Similarity-Paired-Serial-Results.json): 540 complete-output executions. This is serial relative to our own task processes, not CPU pinning, a dedicated host, cold-cache control, or absence of unrelated system activity.

| Mode | Sum of per-query median times, ms |
| --- | ---: |
| Union plus lengths | 779.326 |
| Leaf partition | 1,006.200 |
| Upper-only laminar | 1,159.665 |
| Paired eager | 1,555.263 |
| Paired lazy | 1,197.242 |
| Ordinary DAAT | 91.577 |

Every complete result and every aggregate body-read/DP count above is unchanged. Lazy evaluation is faster than eager on all 30 inputs but slower than upper-only laminar and DAAT on all 30. Its descriptive sum of medians is 3.2% above upper-only laminar, not a measured batch percentage or a confidence interval. Thus the corrected measurement preserves the conclusion: stronger pruning does not yet overcome the access cost.

All four new receipts contain 1,890 checked query executions of the same 30 source inputs. The sequential rerun's incremental laminar build took 93.659 ms once; specialized union/leaf builds took 67.761/83.840 ms. Retained sizes did not change.

### Cost Interpretation

The nested sidecar is 524,056 bytes, versus 463,488 bytes for the canonical feature/owner pair file itself. Every query currently scans its summary union payload and headers, including unrelated blocks. The additional per-union histogram work remains even in lazy mode. A stricter bound cannot pay back irrelevant metadata scanning merely by avoiding some small row bodies.

All runs are small, cached, Python/file experiments. Build time is separately recorded, not amortized away; individual observations are not stable estimates. Shared preparation includes an unused RMQ and is not a fair standalone-build charge for any compared query. Fixture parsing and oracle state remain resident, output test readback is counted, and no physical RAM cap, cold-disk run, Neo4j/GDS baseline, crash recovery or production refresh benchmark is claimed.

## Current Research Decision

Keep the exact complete-block envelope and its independent challenge as a potentially reusable metadata theorem. Reject the present scan-every-summary layout as an observed performance advance on this graph. The scientific delta is not the tree DP or ordinary cascading; it would need to be a useful information/cost advantage from jointly attained metadata, beyond these initial algebraic and body-read results.

The next decisive engineering experiment is an inverted token-to-block access path that can supply query-restricted histograms without scanning every union, with its construction, directory, storage and feature-cursor state paid. That is close to established token-group indexing, so any benefit attributable only to that repair must be credited accordingly. Compare the new envelope against the same access path with ordinary upper/lower count bounds, not against the defeated sequential-summary implementation.

The next mathematical option is an exact two-row summary with a bounded shared-feature count rather than assuming complete disjointness. It needs a new state model and proof; the disjoint theorem must not be silently reused. Neither next step is marked implemented here.

## Independent Review And Corrections

[Review Section 10](Similarity-Laminar-Capacity-Review.md#10-follow-up-exact-two-row-complement-certificate) is complete and has been read. It accepts the eligible two-row theorem and independently checks 157,568 finite block/query cases without failures, including three tree shapes, exact complement reconstruction and infeasible locally plausible capacity tables. Those are reviewer-reported algebra checks, not a replay of our implementation or a benchmark.

Two substantive guardrails are now explicit and have retained regression coverage:

1. A three-row block can satisfy H=ell+u without being a complementary pair. The population-two guard must precede this DP. The reviewer gives an actual score-one row that an illegal three-row application would bound by 1/3.
2. Replacing exact maxima by looser conservative upper estimates is safe for the old upper-only envelope but can make the recovered discrete domain unsafe. A regression records the score-one versus 1/3 failure. The query trusts the coherent exact builder; arbitrary metadata tampering or unsupported stale generations are not certified.

The review supports the discrete metadata-recovery theorem as a narrower contribution candidate, not a new generic DP. It does not establish publication priority, arXiv sufficiency or physical performance. Its independently stronger interval control derives lower counts from the SAME union populations, so our separating example does not rely on making that control pay extra stored fields.

## Reproduction And Checkpoint

### Subsequent Stronger-Comparator Result

The [selective-access follow-on](Similarity-Selective-Block-Access.md) implements the previously proposed paid access path and an exact O(G) interval optimizer. It checks 540 complete outputs on the same 30 queries. Paired improves over interval by only eight bodies and 47 memberships (0.627% and 0.310%), not 22.5%. The latter remains the historical upper-only comparison above. In the sum of query medians, paired is slower than interval, union-only selective indexing and ordinary DAAT. The theorem remains valid, but its practical contribution is smaller than the weaker comparator suggested. All earlier receipts are preserved.

### Original Study Reproduction

Run from repository root with Python 3.11:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
python3.11 research_algorithms_20260920/experiments/bench_laminar_capacity_similarity.py --data research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz --output /tmp/laminar-replay.json
python3.11 research_algorithms_20260920/experiments/bench_laminar_capacity_similarity.py --data research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz --output /tmp/paired-replay.json --groups 4 --block-size 2 --include-paired
```

The current paired replay emits the corrected six-mode comparison; the five-mode pre-cascade receipt remains historical. Wall times will differ. The source gzip SHA-256 and normalization rules are in [input provenance](experiments/data/README.md).

| Artifact | SHA-256 at this checkpoint |
| --- | --- |
| Probe | `feff9868b220dd8a775447d79648f8ec510ab099a6eb5b67efa2da9768a12537` |
| Driver | `c4eddc6c662567f7bdaf20554173676c0f013ff314627b740e8c61338b054c9b` |
| Tests | `cfcca497ff0faec9305ac4945d526ae2d78352002f503e58a056f6ab66c2886c` |
| 360-execution laminar receipt | `6489174c81d4e0d3189cbc2112eb431f983233a348bbb384387021f898c98f3c` |
| 450-execution eager receipt | `c00369935464cdbe75af5c62814259584650ab8c8bac7922e09fa8261250ec0b` |
| 540-execution cascade receipt | `b4c012b0789772509edd0dc617ba2c764f095814d4b91ae20d8bc005df2293bf` |
| 540-execution serial receipt | `dcaa2c2667de83aafe46158ae455999a0f706c5cf3a92679bf2f4e17148ea8b2` |

Code hashes describe the final six-mode implementation, not an assertion that earlier receipts used a byte-identical file before the paired/lazy additions. No tracked production code changed, no commit/push was performed, and the original seven-family research goal remains active.

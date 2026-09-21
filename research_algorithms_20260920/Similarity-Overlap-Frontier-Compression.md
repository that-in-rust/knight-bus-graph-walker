# Similarity: Exact Overlap Frontiers And Their Compression Question

Date: 2026-09-20. Research continuation of the [selective-access study](Similarity-Selective-Block-Access.md). Exact overlap and query-cardinality-dual solvers are implemented. Completed independent mathematical review rejects unrestricted compactness and supports the parameterized positive theorem. No physical performance result or publication-readiness claim.

## The Problem Worth Solving

The existing complete-pair certificate requires disjoint target neighborhoods. That makes its state small, but disjoint pairs save no expanded feature memberships when target postings are grouped into block postings. Overlapping neighborhoods are the more relevant possible opportunity for this access saving. Extending the certificate by simply allocating one dynamic-programming state per overlap count may consume the memory we are trying to save.

The scientific question is therefore specific: can we recover the exact best Jaccard score compatible with jointly attained laminar maxima, for overlapping pairs, without state or work proportional to a large intersection cardinality? This is not a request to rename compressed adjacency or ordinary inverted-index pruning.

The public evidence remains unfavorable to the current pair method: just 47 extra body memberships avoided beyond the exact interval comparator, and no aggregate time win over that comparator or native DAAT. A stronger theorem would not retroactively change that result. Both a useful data regime and paid query/build costs remain necessary for a product claim.

## Information And Exactness Contract

There are exactly two target sets T1,T2 and a known union U. Feature groups form a full binary partition tree with G leaves. At every node v the index retains an exact attained maximum C_v=max(|T1 intersect U_v|,|T2 intersect U_v|). H_v=|U_v| is derived from retained leaf populations. The source query supplies q_v=|S intersect U_v| at leaves and a=|S|, including source features outside U. The two target sizes are ell<=u, with u=C_root.

The required answer is the maximum Jaccard score over ANY complete two-set dataset consistent with these retained observations. It is a tight metadata envelope, not necessarily the score of the actual stored pair. Changing exact maxima to loose upper bounds, using population above two, or silently changing the union changes the problem.

For local overlap e_v and first-row size x_v, the other row has size H_v+e_v-x_v. Exact attained maxima imply:

```text
K_v = 2*C_v-H_v
0 <= e_v <= K_v
x_v in {C_v, H_v+e_v-C_v}

I = ell+u-H_root
e_root = I
```

All counts are nonnegative integers; observed valid metadata requires C_v<=H_v<=2*C_v. Each node's shared count is at most I. Equal choices at e_v=K_v are deduplicated.

## Exact Reference Algorithm

At a leaf, retain F_v(e,x)=min(q_v,x) for every permitted state. At an internal node, combine child states by adding their shared counts and first-row sizes. Keep only states allowed by the parent metadata, and maximize the sum of child query overlaps. At the root evaluate e=I and both possible row sizes, returning max c/(a+x-c). An empty overlap has score zero, including empty versus empty.

```text
Paid metadata + query leaf counts
                 |
                 v
Reserve state count and transition count
                 |
       insufficient budget -> refuse this solver
                 |
                 v
Leaf (shared count, row size, best overlap)
                 |
                 v
Combine disjoint groups; enforce attained maxima
                 |
                 v
Root shared count I -> exact metadata envelope
```

This is the earlier [independently reviewed tree-DP extension](Similarity-Selective-Access-Review.md#part-3-optional-near-disjoint-pair-extension), now implemented as a reference. Generic tree dynamic programming is established. Implementing it does not itself satisfy the requested scientific novelty.

### Constructive Correctness

For a feasible leaf state choose x union features to maximize source overlap, choose e of those to be shared, and let the second row contain the union complement plus the shared features. The local inequalities ensure all counts and the exact maximum are realizable. Because child groups are disjoint, unioning their two witness rows adds counts and overlaps without new interactions. Parent filtering enforces exactly the missing parent maximum. Induction proves both soundness and attainability of every retained best state. Root intersection and the maximum fix the two root sizes to ell,u. Jaccard is nondecreasing in overlap at fixed row size, so retaining only the best overlap per state loses no optimum.

The local shared-count guard cannot be dropped. With rows {a,c},{b,c}, feature groups {a,b} and {c}, and source {a,b}, a false shared count one in the first group would predict score one. Its actual capacity is one and population two, so K=0 there. Every compatible pair instead has best score 1/3. This is now a retained regression.

## Explicit Resource Reservation

For each node define t_v=min(I,K_v). A safe state reservation is:

```text
s_v = 2*(t_v+1) - indicator(t_v=K_v)
reserved_states = sum_v s_v
reserved_transitions = sum_internal_v s_left*s_right
```

The implementation checks both before enumerating any state. It uses integer arithmetic and keeps a logical counter of actual retained states and tested child-state pairs. It retains all subtree tables, so its upper bound is O(G(I+1)) states and O(G(I+1)^2) transitions. It does not claim an optimal frontier lifetime schedule.

These are Python-object counts, not bytes or an enforced physical RAM limit. Input metadata, query counts, integer bit width, dictionary overhead and the surrounding query engine all cost memory. Very large numeric counts are validated and rejected by a small state budget without enumerating their range. The current reference is a pure metadata solver, not a new file format or end-to-end Neo4j query implementation.

## Three Architectural Choices

1. **Exact dense overlap DP.** Simple reference with proved semantics, explicit state/work admission and poor large-I scaling. Use it to establish the target answer and challenge a smaller implementation.
2. **Query-cardinality dual.** Unrestricted concave/piecewise compression fails, as the review below establishes. Instead retain the minimum shared count needed to obtain each possible query-overlap score. The implemented solver depends on Q=|S intersect U| rather than I. Its exact recurrence and limits appear below.
3. **Bounded fallback.** If the exact pair solver cannot be admitted, retain the existing O(G) interval envelope and verify more actual rows. That preserves query correctness but gives up stronger pruning. It is an execution option, not proof of the compact theorem. No fallback integration is implemented in this reference.

The intended implementation sequence is: establish the exact reference with failing tests; independently challenge frontier structure; implement only a proved compression; compare identical metadata answers and reservations; then assess paid selective-query behavior. Do not use a hand-picked timing win to bypass the information or correctness question.

## Implemented Evidence

Sources: [reference solver](experiments/probe_overlap_frontier_similarity.py) and [tests](experiments/test_overlap_frontier_similarity.py).

Four missing-function failures preceded implementation. All four tests then passed in 0.497 seconds after correcting an anticipated case-count assertion. The oracle enumerates every unordered pair of subsets of a five-feature universe, groups actual pairs by their complete observed summaries, and enumerates all 64 queries including an outside feature. Across two- and four-leaf trees this gives 29,440 distinct summary/query envelope checks. Every answer matches exact rational scoring of all compatible rows, never just one representative pair. Every candidate is no greater than the independently tested interval bound, and every zero-intersection case agrees with the earlier disjoint solver.

The count is smaller than pairs times queries because identical observed summaries are deliberately deduplicated. The first score-complete test run failed only on a mistaken expected count greater than 30,000; it had no score mismatch. The count is now asserted exactly. Other tests exercise the reviewed local guard, empty sets, identical sets, invalid integers including boolean aliases, inconsistent metadata, a 10^12-scale logical reservation refusal and transition-budget refusal.

At this pure-theorem checkpoint no public performance benchmark of the overlap implementation had been run. The subsequent [complete-query integration](Similarity-Overlap-Query-Integration.md) supplies separate code review and two public comparisons; its negative timing result must not be conflated with the symbolic-state theorem. The mathematical frontier challenge alone does not inspect these Python files.

## Negative Theorem: Count-Only Universal Compactness Fails

The [completed initial independent review](Similarity-Overlap-Frontier-Review.md) proves an O(G) feasible-domain recurrence but constructs nonconcave value frontiers. For groups {a} and {b,c,d}, exact maxima one and three, root maximum three and union size four, one query gives major-row values [0,0,1] as shared count increases; another gives minor-row values [0,1,3]. Thus neither concavity nor unit-slope increments can be assumed. Both are now implementation regressions.

The review then constructs a covering gadget: activating positive size difference in gadget i permits up to w_i difference at fixed query-overlap loss d_i. Joining gadgets at additive-capacity nodes gives P(t)=sum d_i-min{sum_{i in J} d_i:sum_{i in J} w_i>=t}. With d_i=w_i=3^i, an explicit piecewise-affine representation needs exponentially many pieces. The review also adds an anchor so the large-row branch always dominates the final Jaccard maximum. Testing that maximum against a specified rational threshold decides SUBSET SUM on binary-encoded counts.

This is a compressed-count obstruction, not a claim that explicitly expanded graph input can be processed without paying its length. It does not establish strong NP-hardness or require quadratic dependence on I. The lead inspected the construction and executed 25 new anchored cases against the generic state solver and new threshold solver. The review's additional 203 cases are independent reviewer-reported checks, not these same executions. The reviewer cites Karp's original equality-knapsack completeness result; the lead's attempt to retrieve its linked reprint returned a web-tool error, so direct inspection of that source is not claimed.

## Positive Theorem: Invert The Frontier By Query Overlap

Let M_v(e) be the best query overlap of the row of size C_v, and m_v(e) that of the row of size H_v+e-C_v. Write Q_v for the total query features in the subtree and Q=Q_root. Store:

```text
tau_M_v[c] = minimum feasible e with M_v(e) >= c
tau_m_v[c] = minimum feasible e with m_v(e) >= c
             for c=0,...,Q_v
```

Use infinity when no compatible pair achieves the requested overlap. The zero entry is a feasibility threshold, not automatically zero. This is crucial for internally constrained subtrees.

### Why A Single Threshold Per Score Is Exact

Fix any compatible subtree pair A,B with |A|=C_v and |B|<=C_v. Both satisfy the same laminar upper-capacity constraints, so they are independent in the corresponding laminar matroid. While B is smaller, augmentation supplies an element from A minus B that can be added without violating any upper capacity. Union membership is unchanged. Every previously attained maximum remains attained: no count decreases and no capacity is exceeded.

Consequently B can be extended one element at a time until both rows have size C_v, increasing e to K_v. If the query's scored row is A, its overlap remains unchanged. If it is B, its overlap cannot decrease. Every score achievable at e remains achievable at every larger e through K_v, for both orientations. Thus each inverse feasible-score domain is exactly an upper interval, even though the forward value function may have jumps and fail concavity. This uses ordinary matroid augmentation, not a newly invented exchange theorem.

### Leaf Rule

At a leaf, for c<=min(q_v,C_v):

```text
tau_M[c] = 0
tau_m[c] = max(0, c-(H_v-C_v))
```

Larger c is impossible. This directly inverts the leaf functions and never enumerates I.

### Additive Parent Rule

Let A=C_left, B=C_right, C=C_parent and D=A+B-C. When D=0, the row attaining the parent maximum must attain both child maxima. For either orientation R in {M,m}:

```text
tau_R_parent[c] = min_{i+j=c} (tau_R_left[i]+tau_R_right[j])
```

Ignore infinite terms. At-least-c semantics need no separate i+j>c cases: reduce the requested child thresholds to nonnegative values summing to c. Conversely, witnesses to those two requested scores give at least c overlap. This is ordinary min-plus convolution of inverse value functions.

### Strict Parent Rule

When D>0, the child maxima must belong to different rows. Define L=K_left-D and R=K_right-D. Negative limits make the corresponding metadata infeasible. Write value(tau,e) for the largest c whose threshold is at most e, or infeasible when even c=0 fails.

The two major-row branches are:

```text
R + tau_M_left[max(0,c-value(tau_m_right,R))], require tau<=L
L + tau_M_right[max(0,c-value(tau_m_left,L))], require tau<=R
```

The two minor-row branches are:

```text
L + tau_m_right[max(0,c-value(tau_M_left,L))], require tau<=R
R + tau_m_left[max(0,c-value(tau_M_right,R))], require tau<=L
```

Take the smallest finite branch. Out-of-range score indexes or infeasible fixed-child values invalidate a branch. One child intersection is fixed by the parent maximum; the other is the threshold variable. The cap on that variable enforces e<=K_parent=L+R. This is the exact orientation recurrence, not a relaxation of attained maxima.

At the root evaluate both threshold arrays at I=ell+u-H and compare the two rational Jaccard scores using row sizes u and ell. Source features outside U are still counted in denominator a.

### Time And State Bound

For a balanced G-leaf tree of height h=log2(G), the implementation reserves exactly 2*sum_v(Q_v+1) threshold cells, including infeasible cells. Each query feature contributes at every ancestor, so the state count is O(G+Q(h+1)), independent of the numeric magnitude of I. Validation and metadata vectors add O(G) words.

Strict nodes take O(Q_v+1) work, including scans of their child threshold tables. Additive nodes take O((Q_left+1)(Q_right+1)). Over all internal nodes, sum Q_left*Q_right counts each pair of query features from different leaves at most once, at its lowest common ancestor, and is at most Q^2/2. Linear terms total O(G+Q(h+1)). Therefore the scheduled arithmetic bound is:

```text
time:  O(G + Q(h+1) + Q^2)
state: O(G + Q(h+1)) count/threshold cells
```

These are integer-operation/word counts. Numeric thresholds still need O(log(H+1)) bits, source cardinality a and exact score arithmetic need their corresponding bit widths, and Python object overhead is not a constant physical-byte guarantee. For G=1 the h+1 term is essential. The result does not contradict compressed-count hardness because Q itself can be exponentially large in its binary encoding. In an ordinary explicit source query, reading its a features is already paid work, with Q<=a.

The exact interval relaxation remains O(G) and generally cheaper, but can be strictly looser. The new solver is not uniformly preferable to the dense exact-pair solver either: small I and large Q favor the latter. A future dispatcher can compare conservative reservations BEFORE allocating either table; that dispatcher and its end-to-end fallback are not implemented here.

## Symbolic Scaling Separation

Take the earlier disjoint four-per-group separating pair and add N common features in a separate feature-tree subtree. The source has two features in each exclusive group and none in the common group. The retained metadata is:

```text
leaf H = [4,4,N,0]
leaf q = [2,2,0,0]
tree C = [unused, N+4, 4, N, 3,3,N,0]
ell=u=N+4; I=N; a=4
```

The exact complete-pair envelope is 3/(N+5), while the interval relaxation gives 4/(N+4). The new threshold implementation executes the exact answer using 38 threshold cells and 70 counted schedule operations for every N tested below.

| N shared features | Dense solver reserved states | Threshold cells | Threshold scheduled operations |
| ---: | ---: | ---: | ---: |
| 1 | 19 | 38 | 70 |
| 16 | 111 | 38 | 70 |
| 1,000 | 6,015 | 38 | 70 |
| 1,000,000,000,000 | 6,000,000,000,015 | 38 | 70 |

The two small dense cases execute and match. The two large reservations are computed and refused before enumeration; trillions of states were NOT allocated or processed. The threshold solver executes all four count-only cases, with exact Fraction results, in a retained test. This is a logical-state separation from the dense reference, not a speed measurement or an ingested trillion-feature graph. It does not save reading/building the actual source/target representation or verifying an unpruned target.

The first row is an important negative case: dense state is smaller than threshold state. Do not advertise a single universal choice.

## Further Verification And Contribution Boundary

Four missing-threshold-function failures preceded the new implementation. The eight-test focused suite passes, including the same 29,440 complete compatible-pair envelope checks, 1,024 seeded deeper eight-leaf comparisons to the dense exact solver, nonconcavity regressions, 25 anchored covering cases, resource rejection, empty/identical sets and the scaling example. The deeper comparisons use a distinct implementation but are not an independently enumerated full-set oracle; the smaller exhaustive cases supply that stronger oracle.

The proposed scientific result is the specific exact inverse-threshold recovery theorem and its source-cardinality complexity, paired with the compressed-count hardness boundary. Inverse/profit-indexed dynamic programming, min-plus convolution and laminar augmentation are existing foundations. Publication priority for the assembled theorem remains unestablished.

The [completed independent follow-up](Similarity-Overlap-Frontier-Review.md#91-monotonicity-without-concavity) accepts monotonicity, the strict-node inverse recurrence and the complexity theorem, with the bit-width and common-input qualifications above. Its independent ephemeral enumeration checks every threshold and recovered frontier value in 17,600 summary/query cases. These are distinct from the lead's 29,440 cases and do not inspect the implementation. The review also emphasizes that an internal zero threshold can be positive. A new named regression uses H_left=H_right=C_left=C_right=2 and C_parent=3: both zero thresholds equal one, an empty query with I=1 returns zero, and impossible metadata with I=0 must reject instead of pretending to certify zero.

The review is complete, read and closed. Its final follow-up adds no further enumeration or code audit. No product-speed claim or completed seven-algorithm portfolio is implied.

## Closest Inspected Foundations

- [Kumabe, Maehara and Sin'ya: Linear Pseudo-Polynomial Factor Algorithm for Automaton Constrained Tree Knapsack](https://arxiv.org/html/1807.04942), Sections 1-3. This paper already avoids a quadratic capacity factor by changing the recursive schedule for a related tree-automaton problem. Its theorem is not automatically applicable to our exact attained-maxima disjunctions; a valid encoding and full cost would be required. It rules out claiming that avoiding a generic tree convolution is itself unprecedented.
- [Dynamic Programming for Predict+Optimise](https://researchmgt.monash.edu/ws/portalfiles/portal/333062336/332332668_oa.pdf), detailed knapsack example. Piecewise-linear function algebra for dynamic programming is an established technique. A compact-function implementation is not a contribution unless the particular frontier-size theorem or useful separation is established.
- [Zhang et al.: A Transformation-Based Framework for KNN Set Similarity Search](https://www.jinwang18.net/files/tkde19-setknn.pdf). Count-vector grouping and query-node filtering are established. The controlled question is what joint attainment supplies beyond the exact interval relaxation with the SAME counts, and whether evaluating it is worth its cost.
- [Fife and Oxley: Laminar Matroids](https://arxiv.org/html/1606.08354), abstract and introduction. Laminar upper-cardinality constraints define a matroid. The monotonicity argument above applies its established augmentation property while preserving the additionally attained maxima of the two-row metadata. The paper is not cited as proving our exact inverse recurrence.

These sources were inspected as specific foundations, not an exhaustive priority search or evidence that this exact theorem appears in them.

## Reproduction

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_overlap_frontier_similarity.py'
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
```

Final checkpoint: the nine overlap tests are included in 52 passing combined similarity tests, 16.336 seconds, exit zero. This test duration is not an algorithm performance benchmark. All command processes are terminal and the mathematical reviewer is closed. A scoped Markdown check found no missing paths among 128 relative file links; it did not validate remote pages or every heading anchor.

| Artifact | SHA-256 at final checkpoint |
| --- | --- |
| Overlap/threshold implementation | `db4adc5334865f7758d808ebe56682188ba0e54f93a83d32eae40fee4c4cacee` |
| Nine-test overlap file | `878738a5dd78dbb0cf8ade2869cdd3d03dfb723a52eef2e8688a15aa2737c64a` |
| Completed mathematical review | `ed190f282afdf99b582d283aa7df0d2c17200edaca5e537352c8d170ff793e5b` |

## Next Decisive Work

Status update after the subsequent integration: the first three actions below were pursued in [the complete-query study](Similarity-Overlap-Query-Integration.md), including 60 passing tests, independent code review, both public receipts and explicit lifecycle limitations. Complete output matches, but the extra bound is slower than interval on both workloads. The historic action list is retained to show the experiment's original question; current follow-up questions and closest-art updates are in that study. Publication priority, physical-budget evidence and the remaining six-family innovations are still open.

1. Add an exact-pair mode to the existing paid selective query path. Evaluate it only after a complete witness heap exists and cheaper bounds fail. Select a preflight-admitted evaluator without allocating both tables. A budget rejection must retain the safe interval bound or trigger explicit refusal; it must never become a zero envelope used for pruning.
2. Verify the integrated complete ordered answers, self exclusion, ties, zero tails, generation coherence, output cleanup and both admitted/rejected resource paths. Preserve the existing controls and raw public receipts.
3. Measure an honestly selected overlapping-neighborhood workload and the existing fixed public sources. Charge metadata lookup, directory work, filter arithmetic, surviving target reads, preparation, retention and output. A theorem-level state separation does not predict the crossover against O(G) interval filtering or native DAAT.
4. Inspect closest primary work for the precise joint-attainment/query-parameter theorem and hardness boundary. Do not claim a new min-plus primitive, and do not equate a stronger locally constructed comparator with proof of published equivalence.
5. Reassess A05 within the seven-family audit, then continue the unresolved contribution questions for A01-A04 and A06-A07. This turn strengthens one candidate; it does not narrow the original objective to similarity alone.

The seven-family objective remains active. This study neither replaces the other six families nor converts a generic reference algorithm into an arXiv-ready contribution.

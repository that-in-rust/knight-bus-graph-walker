# Heterogeneous Community Band Certificates

Date: 2026-09-21. A04 continuation. Status: implemented research prototype
with exact oracle evidence; independent mathematical challenge completed and
lead-replayed. Do not transfer performance
or paper-worthiness claims from the earlier uniform source to this extension.

## Goal And Design

Preserve the exact serial two-community warm-state trajectory using two
forward mate streams, but remove equal mover affinities, equal mover degrees
and common matching-edge weights as admission requirements. Retain the user
goal of algorithm-shaped state and predictable work; do not call this general
Louvain/Leiden or hide the cost of finding/preparing the eligible state.

The [previous certificate](Communities-Certified-Warm-Frontiers.md) and its
completed review establish the schedule for uniform movers. This extension
replaces one scalar F(r) by independently certified signs of each vertex's
actual gain. Numerical gains can differ without changing which side of an
unresolved matching pair can move at each of the three cardinalities.

Alternatives considered:

1. Permit near-equal affinities within an arbitrary tolerance: rejected;
   small numerical differences can change move order near a gain boundary.
2. Enumerate every assignment of movers: exact but exponentially expensive.
3. Use cardinality-constrained degree extrema, conditioning on the visited
   endpoint and its mate: selected. These extrema bound the null-model volume
   term exactly for that configuration and are cheap after one degree sort.

Expert lenses: modularity semantics, order statistics, streaming resource
accounting and adversarial admission. The next step is a falsifiable algorithm,
not another survey or promotion of familiar interval arithmetic as novelty.

## Contract

Keep the previous simple, undirected, loopless, positive-rational weighted
source; positive rational resolution; integer original IDs; two nonempty
anchor sets P0,Q0; even mover set Y of size b; Q-prefix of k movers and
P-suffix in increasing mover-ID order, with 0<=k<=b-2. Let r0=k+1.
Y must induce a reciprocal perfect matching. Its pair weights may differ.
All original IDs are visited increasingly each sweep, anchors included;
strict positive gains only, stays at zero, with P/Q/fresh-singleton choices.
The supplied warm state and matching shape remain material restrictions.

For mover u, retain degree du, affinities alpha_u,beta_u to the anchors,
and matching weight w_uv for its unique mate v. Let T=sum(all degrees),
Aq=sum(degrees of Q0), and DQ=sum(degrees of movers currently in Q).
Then VQ=Aq+DQ and VP=T-VQ. T is twice edge WEIGHT; use M for edge COUNT.

## Exact Per-Endpoint Volume Extrema

Condition on u and v being in Q with indicators i_u,i_v. At total Q mover
count r, let t=r-i_u-i_v. Impossible t outside [0,b-2] imposes no test.
Otherwise, among all subsets consistent with these endpoint memberships:

```text
zmin = Aq + i_u*du + i_v*dv + sum_t_smallest(degrees(Y minus {u,v}))
zmax = Aq + i_u*du + i_v*dv + sum_t_largest(degrees(Y minus {u,v})).
```

For u in P set e=beta_u-alpha_u+(w_uv if v in Q else -w_uv):

```text
switch_min = T*e - gamma*du*(2*zmax-T+du)
switch_max = T*e - gamma*du*(2*zmin-T+du)
fresh_max  = -T*(alpha_u + (w_uv if v in P else 0))
             + gamma*du*(T-zmin-du).
```

For u in Q set e=alpha_u-beta_u+(w_uv if v in P else -w_uv):

```text
switch_min = T*e - gamma*du*(T-2*zmin+du)
switch_max = T*e - gamma*du*(T-2*zmax+du)
fresh_max  = -T*(beta_u + (w_uv if v in Q else 0))
             + gamma*du*(zmax-du).
```

All are exact extrema for the indicated mover endpoint state. They need not
be attained along the actual trace; that makes admission sufficient rather
than necessary. Dropping the endpoint conditioning is safe only if the new
bound still contains every feasible subset; it can otherwise reverse signs.

Require fresh_max<=0 in every feasible configuration at r0-1,r0,r0+1.
For same-side mates require switch_max<=0. For split mates require:

| r | P endpoint | Q endpoint |
| --- | --- | --- |
| r0-1 | switch_min>0 | switch_max<=0 |
| r0 | switch_min>0 | switch_min>0 |
| r0+1 | switch_max<=0 | switch_min>0 |

## Anchors: Conservative Joint Bounds

Let Zmin(r)=Aq+sum_r_smallest(all mover degrees) and Zmax(r) analogously.
For an anchor u, let Ip,Iq,W be its affinities to P0,Q0,Y, and top(t) the
sum of its t largest incident Y weights among b positions with implicit zeros.
For a P0 anchor:

```text
switch_upper = T*(Iq-Ip-W+2*top(r))
               - gamma*du*(2*Zmin(r)-T+du)
fresh_upper  = -T*(Ip+W-top(r)) + gamma*du*(T-Zmin(r)-du).
```

For a Q0 anchor:

```text
switch_upper = T*(Ip-Iq-W+2*top(b-r))
               - gamma*du*(T-2*Zmax(r)+du)
fresh_upper  = -T*(Iq+W-top(b-r)) + gamma*du*(Zmax(r)-du).
```

Require both <=0 for each r. Maximal affinity and worst volume can arise from
different subsets. Adding separately valid maxima is conservative, not an
exact joint extremum. That distinction must remain visible in admission data.

## Conditional Trajectory Theorem

If all gates pass, induction on serial visits gives: anchors never move, no
fresh community appears, same-side pairs stay, and a split pair reunites on
its next eligible endpoint. Only r0-1,r0,r0+1 are reached. Thus the old
HHLL completion schedule and two-frontier stale-record rule apply unchanged.

The stream record becomes (original ID, mate rank, degree, beta-alpha,
matching weight). The header retains b,k,c,T,gamma,Aq,initial DQ. On a high
move, add du to DQ; on a low move, subtract du. Evaluate the exact scaled
gain using the PRE-MOVE volume and recorded endpoint data. Emit actual gains,
accepted IDs/destinations/sweeps and all final labels. The phase schedule alone
does not justify using the old common-degree gain formula.

Execution takes O(n) record/arithmetic operations with a constant number of
control records and fixed stream buffers, after paid preparation. Exact gains
and arbitrary IDs retain their bit costs. With fixed-width checked records,
transfer work is O(Scan(n)); otherwise use serialized-byte size. Full no-op
visit transcripts still require n times the sweep count records.

## Degree Exclusion Algorithm

Sort movers once by (degree, original ID), retain rank and prefix sums. To
obtain the sum of the t smallest degrees excluding ranks e1<e2, start with
a prefix length t. For each excluded rank in increasing order, extend the
prefix by one iff that rank is below the current prefix length. Subtract
the degrees of exclusions inside the resulting prefix. At most two records
are removed, so this query uses constant arithmetic/comparison operations.
For the t largest, subtract the (b-2-t)-smallest result from the total degree
excluding the two endpoints. Equal degrees do not require special tie logic.

Preparation therefore costs O(b log b + sum_anchor s_u log(1+s_u) + n+M)
arithmetic/comparison operations, plus original-ID sorting if not already
ordered. The prototype may store O(n+M) records. This is not an external
bounded-memory builder or a hard latency guarantee.

## Test-First Plan

1. Add `test_heterogeneous_band_communities.py`: exhaustive two-exclusion
   sums with repeated degrees; per-mover extrema versus enumerated subsets;
   mixed-weight graphs versus the independent expanded serial oracle.
2. Observe missing-module RED, then implement a separate candidate module
   `probe_heterogeneous_band_communities.py`. Preserve the uniform receipt's
   frozen source bytes and old independent tests.
3. Test every prefix position, zero/even crossing counts, all-label sorted
   output, unequal matching endpoints, interleaved and wide IDs, exact gains,
   unsafe source/anchor/singleton/sign cases, truncated/trailing streams.
4. Retain a fixed seeded study with admitted AND refused cases. Separate
   old-certifier refusal from a genuine trajectory change. No timing/RSS claim.
5. Independent mathematical challenge with a disjoint review file while lead
   implements source fixtures and code. Integrate defects rather than merely
   collecting positive tests; update the journal and seven-family audit.

## Stronger Anchor Route: Not Implemented

There is an exact correlated bound beyond the conservative anchor gate.
For each anchor put x_v=T*w_uv-gamma*du*d_v for every mover (w_uv=0 if absent).
P-switch and P-fresh extrema use the r-largest sum of x, with coefficients
2 and 1 respectively; Q extrema use the r-smallest sum with coefficients
-2 and -1. This follows by combining affinity and degree terms BEFORE
maximizing over the common Q subset. A naive per-anchor full sort has
quadratic sparse-source work, which cannot be hidden in the scan executor.
Only pursue an efficient sparse-exception version if conservative refusals
make that extra preparation useful. This is not yet covered by tests.

## Contribution Boundary

The matching shape and special warm ordering are still restricted. A broader
admitted class is not automatically a useful customer workload or a paper.
Sorting, prefix sums, interval bounds, and modularity aggregation are established
ingredients. The candidate contribution to investigate is their combination
into a checkable exact serial-trajectory reduction with no resident label map.
Closest-art obligations from the uniform manuscript remain, including
[VLouvain](https://www.openproceedings.org/2026/conf/edbt/paper-72.pdf) and
[external graph clustering](https://arxiv.org/abs/1404.4887).

Open gates: independent implementation audit, useful native source,
source/partition discovery, end-to-end resource accounting,
strong equally informed comparator and scientific priority. All seven-family
innovation obligations remain active.

## Implementation And Fixed-Grid Evidence

Implemented [certifier and stream executor](experiments/probe_heterogeneous_band_communities.py)
with separate [tests](experiments/test_heterogeneous_band_communities.py) and
[evidence driver](experiments/probe_heterogeneous_band_evidence.py). The uniform
manuscript's source files and receipt remain unchanged. The stream record in
this prototype stores alpha and beta separately (six fields), rather than only
their difference (five fields); both layouts have constant record size under
a bounded-width representation. No physical encoding is delivered here.

Eight new tests pass, including 66,024 exhaustive two-exclusion cardinality
queries, 1,000 mover-bound checks against explicit subsets, full uneven-weight
graph traces, all fifteen permitted prefix sizes at b=16, unsafe singleton/sign
refusal and malformed/truncated input. Together with earlier tests, 28 pass.

The fixed study uses b in {16,32}, ten seeds, and six perturbation scales, plus
all fifteen b=16 prefix sizes at two scales. Each mover-incident edge receives
an independently generated rational adjustment j*scale, j in {-5,...,5};
undirected symmetry is retained and all source weights remain positive.
These scales are ABSOLUTE weight adjustments, not percentages of every edge.
The cohorts differ where the extra prefix-size cases are included, so this
table is not a statistically controlled trend estimate for a customer population.

| Scale | Cases | Admitted, exact output | Refused, same schedule | Refused, changed schedule |
| --- | ---: | ---: | ---: | ---: |
| 1/10000 | 35 | 35 | 0 | 0 |
| 1/1000 | 20 | 20 | 0 | 0 |
| 1/100 | 20 | 0 | 14 | 6 |
| 1/20 | 20 | 0 | 9 | 11 |
| 1/10 | 35 | 0 | 6 | 29 |
| 3/20 | 20 | 0 | 2 | 18 |
| Total | 150 | 55 | 31 | 64 |

The old uniform certificate refuses all 150. The 55 new admissions check
4,800 complete labels and 231 exact accepted moves, gains and sweep numbers
against the expanded-graph oracle. Every refused graph is also run through the
oracle. "Same schedule" compares IDs, destinations, sweeps and final labels
against its unperturbed source, not the numerical gains (which generally
change with weights). All 95 early refusals are mover-gain-sign failures;
none of these cases can be rescued by changing only the anchor bound. Later
anchor checks may also fail, since the certifier stops at its first failure.

The [retained receipt](evidence/community-heterogeneous-band-20260921/receipt.json)
includes source hashes, deterministic case identities, input/output hashes,
all oracle move logs, admitted margins, and refusal explanations. SHA-256:
`18eb630eb9ccb919af13d502c066384001d3b27453ba1276cb13b54b89fc5bb7`.

```bash
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest test_heterogeneous_band_communities test_certified_warm_communities test_certified_warm_review_contracts test_matching_frontier_communities -v
/Users/amuldotexe/.local/bin/python3.11 -B -W error probe_heterogeneous_band_evidence.py --output /tmp/community-heterogeneous-replay.json
```

This demonstrates a genuinely broader admitted class, but also a narrow tested
robustness region. It establishes neither real-workload prevalence nor reduced
whole-process RAM, latency, total build cost or publication priority. Do not
hide the 31 conservative refusals or tune the seed list to advertise admission.

## Research Comparison: Inspected Primary Sources

| Source and inspected area | Established mechanism | Boundary relative to this proposal |
| --- | --- | --- |
| [Memory-Efficient Community Detection Using Weighted Sketches](https://arxiv.org/html/2411.02268v1), Sections 4.1 and 4.4 | Weighted Misra-Gries sketches replace per-thread candidate-community hash tables; candidate weights are revisited; reported overall storage is O(V+E) | Broader practical graph support, but the inspected method does not promise our exact serial trace; reducing per-thread scratch is different from eliminating the admitted phase's resident label map |
| [(Semi-)External Algorithms for Graph Partitioning and Clustering](https://arxiv.org/pdf/1404.4887), Section 4.1 | Label propagation uses two external priority queues and time-forward messages, with Sort(E) I/O per iteration | Establishes substantial external-clustering precedent; our proposed exact modularity schedule is a restricted reduction, not invention of external processing |
| [VLouvain](https://www.openproceedings.org/2026/conf/edbt/paper-72.pdf), previously inspected in the uniform follow-on | Low-rank vector aggregation avoids materializing graph edges | Algebraic gains and implicit graph execution already exist; heterogeneity alone is not a novelty claim |

The sketches paper is an arXiv technical report; its HTML includes template
conference metadata. Use the arXiv version/date rather than treating that
placeholder metadata as a verified publication venue. The dynamic-frontier
PDF at [IJNC](https://www.jstage.jst.go.jp/article/ijnc/15/1/15_2/_pdf) returned
an abstract/excerpt but repeated detailed-page access failed in this pass;
its exact preservation scope is not settled by those search excerpts.

These source comparisons are bounded research, not proof that no equivalent
trajectory theorem exists. No comparative implementation timings were run.

## Revised Next Gate

Improving anchor order statistics cannot repair the observed mover-sign
refusals. Before optimizing preparation, investigate a trajectory-conditioned
certificate: verify only the actual proposed move log and its skipped visits,
rather than every hypothetical assignment at a cardinality. A complete
per-visit replay costs n times the sweep count and would defeat the point.
Any new verifier must account for changing neighbor affinities and global
community volumes without silently omitting an eligible skipped vertex.
The 31 unchanged-trajectory refusals are a concrete regression cohort for this
question; the 64 changed trajectories must not be admitted as the old schedule.

The new [single-flip trace-verification design](Communities-Single-Flip-Trace-Verification.md)
implements sparse affinity change events plus exact range extrema of Q volume
at actual vertex visits, and a simpler chronological threshold control. Both
recover all 31 unchanged-trajectory refusals and reject all 64 changed
trajectories in the same retained cohort. Their source-only matching proposal
producer does not first run the oracle. The 45-test combined suite passes;
independent mathematical review of the verifiers is complete and lead-replayed.
The later proposal-free solver is outside that review. Preparation,
output and physical memory remain separate obligations, not free work.

## Independent Mathematical Review

The [separate reviewer](Communities-Heterogeneous-Band-Review.md) imported no
lead code or fixtures. The lead read and successfully replayed its embedded
checker with warnings as errors. It checked 16,632 exclusion queries, 19,008
feasible endpoint configurations, 8,256 impossible configurations, 7,400
anchor-side configurations and 820 complete sign-table schedules.

Its 45 independently constructed graph attempts give 30 admissions and 15
refusals, including 21 genuinely heterogeneous admissions. All admitted
trajectories, 45 accepted moves and 294 final labels agree with direct
modularity-objective differences. These graphs use only two anchors per side
and b=4,6,8, not the lead's old dense-template-derived source. Refusals in this
separate study are not classified as changed versus preserved trajectories.

The review proves inclusion: each input passing the old uniform certificate
also passes the heterogeneous mathematical gates. It gives strict positive
base margins and a continuity argument for an open neighborhood of edge
weights on a fixed matching support. Equal-degree ties in order statistics
do not invalidate continuity of the extrema. This is a proper enlargement
of the admitted class, not evidence that arbitrary graphs are admitted.

Its concrete five-vertex anchor example has exact switch/fresh maxima -8,-6
but conservative bounds 4,0. This validates the correlation warning and the
exact-correlated formulas, not a complete new graph admission. Our 95 mover
refusals remain unaffected by an anchor-only improvement.

No mathematical defect required changing the frozen kernel. Three separate
[review-driven regression tests](experiments/test_heterogeneous_review_contracts.py)
exercise eighteen small independent-family variants, unequal endpoint degree
accounting, and a 513-bit ID translation. Original source/receipt bytes remain
unchanged. This review closes a theorem challenge, not the seven-family
innovation or publication-priority objective.

The endpoint regression exposed an error in the review's illustrative affinity
tuple, not its bound algebra or scalar checker. The original alpha=3/4,beta=0,
w=1/4 gives affinity difference -1/2 rather than the asserted -1/4. Corrected
alpha=5/8,beta=1/8,w=1/4 preserves degree one and realizes the intended gain
-9. An explicit eight-vertex source and direct modularity difference now
verify it; the review carries a visible lead erratum. Frozen implementation
and experiment receipts remain unchanged.

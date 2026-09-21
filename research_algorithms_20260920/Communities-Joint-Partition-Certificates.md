# Joint Residual Certificates For Comparing Community Partitions

Date: 2026-09-21. A04 candidate, following
[cancellation-cap results](Communities-Cancellation-Native-Results.md).
Status: independent review, fixed native gate and certify-or-replay execution
are complete. The [stronger-control ablation](Communities-Signed-Control-Results.md)
shows simple support already explains all native winner certificates. No physical memory, elapsed-time,
global optimality, GDS partition-parity or publication-readiness claim.

## The Problem This Actually Solves

Absolute score bounds can be loose even when choosing between two partitions
is easy. Both scores depend on the **same** omitted graph. Bounding them
separately permits two different worst-case omitted graphs, which can invent
uncertainty in their difference. Compare the difference directly and cancel
edges whose contribution agrees.

This is a candidate-selection certificate, not a community proposal generator.
The earlier native experiment already chose the best of its three proposals
in every case; some choices could not be certified without rescanning edges.
An improvement here must avoid useful verification work, not claim to find
partitions it never generated.

The underlying ideas of shared uncertainty, per-vertex relaxation and bounded
fractional edge variables are established. The proposed specific component is
a linear-work signed residual certificate computed from sparse partition
intersection cells, with original-modularity semantics and explicit controls.
Its scientific distinctiveness needs a close comparison, not merely a new name.

## Exact Contract

Keep the previous once-per-undirected-edge convention. The frozen coarse graph
is G, retained edge underestimates are H, and R=G-H is nonnegative and loopless.
All coarse loops have already been retained exactly. The summary knows original
degrees d_i, original total mass W>0, residual degrees r_i, retained pairs,
and a valid residual pair cap D. Gamma is positive. Exact rational arithmetic
is used; overflow-safe fixed-width arithmetic/outward rounding is not supplied.

For the specified fixed-b weighted-MG rule, b>0 implies D=M/(b+1), where
M=sum r_i/2. For b=0 a generic D=M is safe, but no cancellation identity is
asserted. Negative edges, unknown residual loops, a changed graph snapshot,
or a cap inferred from final rather than configured occupancy invalidate the
contract. Necessary degree checks do not establish full capped feasibility;
an actual valid summary provides that witness in this workflow.

A and B are arbitrary partitions of the same K base blocks. Both represent
coarsenings of the original P0, not arbitrary splits inside a base block.
They may be adaptive functions of the summary. The bound is pointwise for
every feasible residual, so such selection does not require a probabilistic
union bound. This does not make the bound tight.

## The Signed Difference

For each omitted pair ij define

```text
c_ij = 1[A(i)=A(j)] - 1[B(i)=B(j)]

 +1 : internal to A, external to B
 -1 : internal to B, external to A
  0 : both partitions agree; the edge cancels

I_R(A)-I_R(B) = sum_{i<j} c_ij * x_ij
```

Compute t_i=min(r_i,D), total T=sum_i t_i, clipped sums T_A and T_B by
community, and clipped sums T_cell by occupied (A label,B label) cell.
Only occupied cells are stored, at most K, even if the Cartesian product
of community-label sets has quadratic size.

For a vertex i, set

```text
p_i = T_A(i) - T_cell(i)
n_i = T_B(i) - T_cell(i)
z_i = T - t_i - p_i - n_i
```

These upper-bound its residual incident mass on the positive, negative and
neutral classes, respectively. Each possible neighbor j contributes at most
min(r_j,D)=t_j; subtracting the intersection removes agreeing internal pairs,
and excluding t_i removes the forbidden self-pair.

## Inequality And Proof

Let actual incident masses be y_i+, y_i-, y_i0. They are nonnegative, sum to
r_i, and satisfy y_i+<=p_i, y_i-<=n_i, y_i0<=z_i. Therefore

```text
y_i+ <= min(r_i,p_i)
y_i- >= max(0, r_i - y_i+ - z_i)

y_i+ - y_i-
  <= min(r_i,p_i) - max(0, r_i-p_i-z_i)
```

For the last step, the upper expression obtained by substituting a particular
y_i+ is increasing in y_i+. If p_i>r_i, both displayed negative-part terms
are zero, so using p_i rather than min(r_i,p_i) inside that term is equivalent.

Every undirected signed edge is counted at both endpoints. Thus

```text
U(A,B) = 1/2 * sum_i [min(r_i,p_i)-max(0,r_i-p_i-z_i)]
L(A,B) = -U(B,A)

L(A,B) <= I_R(A)-I_R(B) <= U(A,B).
```

This relaxes the requirement that both endpoints allocate compatible mass
to the same pair. A bounded per-vertex calculation is not an exact solution
of the global capped fractional b-matching problem. The completed independent
review confirms validity but supplies a dominating same-order endpoint bound:
with a_i=min(r_i,p_i), f_i=max(0,r_i-p_i-z_i), use
min(sum a/2,sum a-max a)-max(sum f/2,max f), then reverse for the lower.
This stronger constructed control is not evidence of historical priority.

## Restore Original Modularity

Let k_A=J(A)/W-tax(A), where J is exact retained internal mass and tax uses
original d, W and gamma. Likewise define k_B. Then

```text
Q_G(A)-Q_G(B) in [k_A-k_B + L(A,B)/W,
                  k_A-k_B + U(A,B)/W].
```

Do not divide by residual mass M, discard the null-model penalty, normalize
the retained graph separately, or infer unknown loop placement. All are
different objectives. The prototype also intersects this interval with the
strongest of its declared simple comparison controls.

## Controls That Must Not Be Omitted

1. Independent capped score intervals: difference interval
   `[lower(A)-upper(B), upper(A)-lower(B)]`.
2. Refinement: if A refines B, residual internal(A)<=internal(B), so its
   signed upper is zero; the reverse refinement gives signed lower zero.
3. Equivalent partitions, including label renamings: exact signed difference
   zero. A candidate cannot beat itself; exclude self from regret calculations.
4. Exact replay: once a small candidate family is frozen, one input scan can
   compute all original scores with no retained edge map. Labels and degree
   accumulators still cost memory. A graph-fitting-in-RAM or already-hot scan
   can be a stronger practical choice than a complex certificate.

The original native gate measures incremental gains over controls 1-3 and
retains control 4 as the actual fallback. The later signed-control ablation
adds ordinary support, sign-only and endpoint controls; all certify every
operational winner. Do not present controls 1-3 as the strongest available
baseline or the resulting scan saving as a unique benefit of this formula.

## A Non-Nested Separating Witness

Take six vertices, all r_i=1, D=1, M=3; b=2 and empty retained counters can
produce this summary. Compare

```text
A: {0,1} | {2,3} | {4,5}
B: {0,1} | {2,4} | {3,5}
```

Neither partition refines the other. Individual capped internal intervals
are both [0,3], so independent subtraction gives [-3,3]. The common pair
{0,1} cancels. Vertices 0 and 1 have no signed incident capacity, while each
of the other four has at most one positive and one negative unit. The joint
interval is [-2,2], and both endpoints are attainable:

- Edges 01,23,45 give signed difference +2.
- Edges 01,24,35 give signed difference -2.

Each three-key unit stream gives the same b=2 empty sketch and residual
degrees. Original degree penalties also match, so the score difference
contracts from [-1,1] to [-2/3,2/3]. This is a strict soundness/separation
witness, not a native performance result or a new generic relaxation theorem.

## Resource Accounting

For one pair comparison, clipped-degree scans and sparse cell accumulation
take O(K) rational operations/expected dictionary operations and O(K) scratch.
Computing retained score differences costs O(b). The current wrapper also
recomputes separate bound controls, adding constant-factor scans and temporary
maps; it is not the claimed minimum-allocation implementation.

With C resident candidate label arrays and a fixed proposed winner:

```text
Candidate labels                  O(C*K)
Existing summary                  O(K+b)
One reusable comparison scratch    O(K)
Certify winner against all others  O(C*(K+b)) arithmetic work
Compare every ordered pair        O(C^2*(K+b)) arithmetic work
Retain all pair outputs            O(C^2), optional
```

The native driver checks all five-candidate ordered pairs for research
coverage. A runtime need not do that merely to certify one proposed winner.
The implemented executor first uses refinement and independent bounds and
invokes the joint core only for unresolved competitors. Its 560 fixed-policy
runs are documented in [native results](Communities-Joint-Native-Results.md).
Actual logical source accesses are measured; elapsed time and RAM are not.

Initial P0 construction, arbitrary-ID joins, source storage, candidate discovery,
numeric bit growth, original-ID output and simultaneous lifetimes are not
removed. A user-selected edge-counter count b is not a RAM bound independent
of K. All native probes still hold an oracle graph in memory and do not test
the 4 GB physical workflow.

## What The Implemented Execution Path Promises

A certify-or-replay path now returns a partition proved best **within
its declared finite candidate family**, using no additional edge scan if all
competitor-minus-winner upper bounds are nonpositive. Otherwise it scans
once and chooses the exact best family member. This is not global modularity
optimization, nor equivalent to a fixed GDS/Louvain/Leiden trajectory.

The result's exact modularity need not be known on the fast path. If the API
requires the exact scalar score anyway, it may still need the replay. This is
an important product limitation: a selection certificate cannot silently
replace an exact metric with an interval. Intermediate optimizer selection
may benefit even if a final selected output is rescored once at publication.

## Verification And Next Decision

Lead tests cover 1,200 seeded original-score differences using actual weighted
sketch residuals, reverse symmetry, full-retention exactness, loop accounting,
refinement, renamed identical partitions, and the strict crossing witness.
Six absent-module failures were observed before implementation, followed by
22 passing combined joint/cap/degree tests. The
[independent review](Communities-Partition-Difference-Review.md) is lead-replayed
on 21,643 pair cases and 24 signed-score checks. The combined community suite
through the original executor passes 100 tests. The protocol is
[Partition-Difference Gate](Communities-Partition-Difference-Gate.md).

The native gate proves 87/120/140 winners with independent/refinement/joint
controls, and the executor avoids 20 additional logical scans. However, the
new support-only ablation also proves all 140. The original six-node separating
example above is likewise captured by support cancellation. The review's
five-node witness establishes a more demanding constructed separation, but
does not rescue a native or novelty claim for the row formula. Investigate
[global compact dual certificates](Communities-Implicit-Partition-Dual.md)
only where these elementary controls leave a useful decision unresolved.

Promote only the specific surviving claim. A tighter bound that avoids no
useful work can remain a mathematical note. A strong fixed-family result can
justify a bounded execution path, but cannot complete the seven-family goal.

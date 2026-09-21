# Implicit Global Dual Certificates For Community Comparisons

Date: 2026-09-21. New A04 mathematical/implementation candidate. Bounded
primary-art and independent proof review complete and lead-replayed. No established novelty or native performance
claim. This extends the question raised by the
[signed partition review](Communities-Partition-Difference-Review.md); it does
not modify the old pinned probes or replace their weaker controls silently.

## The Particular Opportunity

The inexpensive row certificate allows each endpoint to distribute residual
mass independently. Some of those choices cannot coexist in any graph. The
exact global reference problem retains degree equalities and pair capacities,
but naively has K(K-1)/2 edge variables. Materializing that LP defeats our
low-state objective when K is large.

Three choices are distinct:

1. Use the linear-work signed endpoint bound. Cheap, sound, sometimes loose.
2. Solve the dense capped fractional LP. Strongest summary-conditional answer,
   potentially quadratic state and expensive optimization.
3. Evaluate global dual certificates using partition structure without
   enumerating pairs. Spend a declared number of rounds; retain the best valid
   certificate found, then fall back to an exact source replay if unresolved.

We investigate option 3, always alongside option 1. This is not a new
fractional matching dual. The candidate contribution to inspect is its
partition-structured evaluation and bounded-work use in graph selection.

## Contract And Classical Dual

For a feasible nonnegative loopless residual graph, degrees r_i are known and
each pair weight is at most D. Partitions A,B share the same K vertices. Let
c_ij=1[Ai=Aj]-1[Bi=Bj]. For arbitrary free potentials y_i, define

```text
F_AB(y) = sum_i r_i*y_i + D*sum_{i<j} max(0,c_ij-y_i-y_j).
```

For every feasible residual x, its signed internal-mass difference is at most
F_AB(y). Proof: substitute sum_j x_ij=r_i into sum c_ij*x_ij, then bound each
remaining coefficient times x_ij over [0,D]. Minimizing F is the exact LP
dual, but **any** supplied y is already a valid upper. Equality degrees mean
y can be negative. Symmetric reversal gives a lower from -F_BA(z).

Feasibility is supplied by the actual summarized source, not established by
this evaluator. Signed original edges, unknown residual loops and an invalid
cap are outside its contract. Rational arithmetic is exact; denominator growth
is a cost, not hidden constant-space arithmetic.

## Compact Evaluation Identity

For any subset S define H_t(S)=sum_{i<j in S}(t-y_i-y_j)_+. Then

```text
sum_{i<j}(c_ij-y_i-y_j)_+
  = H_0(V)
    + sum over A blocks       [H_1 - H_0]
    + sum over B blocks       [H_-1 - H_0]
    - sum over occupied cells [H_1 + H_-1 - 2*H_0].
```

An occupied cell is a nonempty intersection of one A block and one B block.
Check the four possible equality pairs: neither gets H_0; A-only gets H_1;
B-only gets H_-1; both get H_0 after the cell correction. Thus the identity
is exact even though some aggregate terms are subtracted. There are at most
K occupied cells, not a dense Cartesian product of communities.

Sort vertex indices once by y_i. Append them in that order to their A, B and
cell lists; each list is already sorted. Each vertex has four memberships:
global, A, B, cell. For a sorted group and threshold t, compute prefix sums
of y. For each i, a monotonically decreasing pointer counts members j with
y_j<t-y_i. Subtract i itself if 2*y_i<t. Its incident hinge sum is
count*(t-y_i)-prefix_sum, with that same self contribution removed.
Half the sum over vertices is H_t. Each threshold requires one group scan,
not all pairs. Prefix sums can be shared among thresholds for the same group.

```text
K potentials + two partition arrays
                  |
                  v
One sort; grouped ordered lists       O(K log K) work
                  |
                  v
Eight visits per vertex in total      O(K) scan work
                  |
                  v
Exact dual value + subgradient        O(K) records

No stored K-by-K pair table; no edge LP is materialized.
```

The scan count is exactly 8K vertex-threshold visits: one global threshold,
two for A, two for B, three for cells. Prefix preparation is 4K additions;
pointer decreases are at most 8K. Hash grouping is expected O(K); comparisons,
label lengths and exact arithmetic bit costs are additional assumptions.
This removes dense pair enumeration for **one dual evaluation**, not for
solving the optimization problem to a particular accuracy. The prototype
bounded-search helper additionally retains an O(R) scalar diagnostic trace;
its live records are O(K+R), not just O(K). Keeping only current/best vectors
would remove that optional history, but is not the helper as tested.

## A Subgradient Without Pair Enumeration

For each vertex let h_i count pairs incident to i whose residual dual
coefficient is strictly positive. Then g_i=r_i-D*h_i is a valid subgradient
of F. A zero coefficient contributes zero, a permitted choice at the hinge.
The same grouped scans return those incident counts. Apply the same algebraic
cell correction to the counts, not just the scalar value.

An arbitrary difference of convex subgradients is not generally valid. Here
validity follows from the per-pair identity: the corrected count is exactly
the strict-active indicator for the single true c_ij. Tests must explicitly
cover ties, where an inconsistent inequality convention could break that fact.

A fixed-round normalized subgradient search may evaluate successively better
potentials, but no convergence rate or optimality is asserted. Every iterate
remains a valid upper, and retaining the minimum prevents a worse returned
bound. A caller fixes rounds in advance; O(R*K log K) is a logical work budget,
not an elapsed-time SLA. Clip neither the final score nor negative potentials
without accounting for the changed problem.

## A Guaranteed Bounded-Work Schedule

The [independent review](Communities-Implicit-Dual-Review.md) also validates
the following optional theorem. For K>=3, some global dual optimum is
half-integral and lies in [-K,K]^K. This is an existence statement, not
permission to round every iterate to a half-integer.

Proof outline: F is affine on each cell of the arrangement y_i+y_j=c_ij.
Every cell is pointed because the complete-pair normals have rank K (a
triangle eliminates every nonzero line direction). A cell containing an
optimum therefore has an optimal vertex. K independent tight pair equations
form components with one odd cycle and attached trees. Cycle equations give
half-integral potentials of magnitude at most half the cycle length; each
tree step adds at most one. Every resulting coordinate has magnitude at most
K. Strong duality guarantees existence of the initial optimum for feasible
input. The argument applies more broadly to complete-pair coefficients in
{-1,0,1}; partition structure supplies the compact evaluator, not this radius.

For D>0, choose a positive integer q, exactly T=q^2 evaluations, and step
alpha=K/[D*(K-1)*q]. Start y=0 and project each update y-alpha*g onto the
box [-K,K]^K. Retain the best evaluated value U. The standard projected
subgradient inequality, with distance R<=K*sqrt(K) and gradient norm
G<=D*(K-1)*sqrt(K), gives

```text
F* = maximum feasible residual partition difference
0 <= U - F* <= D*K^2*(K-1)/q
work: O(q^2*K log K) arithmetic operations
state: O(K) live records, with no iteration history
```

This schedule is implemented separately as `solve_projected_partition_dual`.
Integer q avoids irrational square-root steps; exact rational arithmetic is
used. D=0 requires zero degrees and returns the exact zero certificate without
iterations. Necessary degree checks are not a complete feasibility solver.

The error constant is crude. At K=5,D=1,q=4, it allows error 25 even though
the witness's whole feasible range has width 1. The actual returned upper is
-3/8, versus true maximum -1/2; that is sound but does not certify the same
original-modularity ordering that the heuristic does. A proven convergence
bound is **not** automatically the better operational schedule.

Crucially, U-error is a lower bound on the *maximum over completions*, not a
lower bound on the actual unknown graph's difference. Only the reversed dual
provides that latter bound. Arbitrary rational denominator growth and prefix
sum bit widths also prevent translating O(K) records into a fixed-byte cap.
No fixed-bit or physical-RAM theorem is supplied by this implementation.

## A Strict Constructed Decision

Independent review supplies a five-vertex example beyond the row/endpoint
and strongest specified scalar/refinement/meet/join controls:

```text
r = (1,3,3,2,2), D=1
A = {0,3} | {1} | {2} | {4}
B = {0,4} | {1,2} | {3}
y = (1/2,-1/2,-1/2,1/2,-1/2)
```

The signed objective is delta=x03-x04-x12. Degree equalities rewrite it as
-5/2+x14+x24-x04, so delta<=-1/2 using the pair caps. The displayed potential
vector produces exactly that global certificate. Weights 1/2 on 02,03,23 and
1 on 12,13,14,24 attain it. The exact feasible range is [-3/2,-1/2].

The signed endpoint upper is 1. Even providing exact marginal internal-mass
intervals for A, B, their meet and join leaves upper 0. Thus the global upper
-1/2 is strictly better. This is not an unexplainable optimization miracle:
the displayed subset/degree identity is a cheaper tailored proof of the same
witness. Such preselected subset controls remain important competitors.

The lead's actual normalized diminishing search starts at y=0, fixes 12
updates, and finds upper **-24847/55440**, approximately -0.44818, without
being given the optimal y. It evaluates 13 iterates and visits 520
vertex-threshold records (13*8*5), in addition to sorting/grouping and exact
arithmetic. It is not the reviewer's different constant-step demonstration.

For gamma=1/2 and the original graph above, W=11/2 and the retained/penalty
offset in Q(A)-Q(B) is 9/121. The exact-marginal comparator still allows
positive difference 9/121. The searched dual proves

```text
Q(A)-Q(B) <= 9/121 + (-24847/55440)/(11/2) < 0.
```

Therefore the certificate can change a finite-family decision, not only an
interval width. This fixture certifies D=1 from its simple weighted source;
it does **not** obtain D=1 from a discard-all MG budget. M=11/2 cannot satisfy
D=M/(b+1)=1 for integral b. Applying the general cap contract here is valid;
calling it a native MG result would not be.

## Literature And Claim Boundary

The [independent dual review](Communities-Implicit-Dual-Review.md) directly
reduces the group hinge calculation to published pairwise-ranking machinery.
In particular, [Rust and Hocking's all-pairs hinge work](https://arxiv.org/html/2302.11062v1)
uses sorted aggregation, and [Ghanbari, Li and Scheinberg](https://link.springer.com/article/10.1007/s10013-025-00767-6)
explicitly discuss log-linear values/subgradients for linear pairwise hinges.
The generic sorted-hinge primitive is not new. The occupied-cell composition,
tie-safe signed identity and its application contract are the particular
objects under examination, not proof of publication-worthy novelty.

[Huang and Jebara](https://berthuang.com/papers/fast_bmatching_corrected.pdf)
already study implicit dense b-matching with linear-memory reconstruction.
Their integer bipartite matching output differs from this fractional
nonbipartite scalar certificate, but a broad first-low-memory-matching claim
would be wrong. [Boyd and Park's notes](https://stanford.edu/class/ee364b/lectures/subgrad_method_notes.pdf),
Sections 2.1 and 3.3, supply best-iterate subgradient precedent. Neither the
heuristic search nor ordinary duality is claimed as new.

## Implementation Evidence

The separate implementation is `experiments/probe_implicit_partition_dual.py`.
Six missing-module assertion failures preceded implementation. Ten tests
now pass: exact equality with dense values/gradients on 300 seeded rational
cases, subgradient inequalities, explicit ties, negative potentials, both
review witnesses, the bounded-search original-score decision, invalid inputs,
and large identity-partition record accounting.
Two further missing-schedule assertion failures preceded the guaranteed
projected implementation. Its tests cover 18 scaled witness/schedule
combinations, exact evaluation counts, the stated gap bound, box membership,
zero cap, and explicit rejection of unsupported K<3 or invalid round counts.

At K=4096 the identity test represents 8,386,560 possible unordered pairs but
performs 32,768 threshold visits and 16,384 prefix additions, with 16,384
membership records. It does not inspect those millions of pairs. This is a
logical-operation/representation check, not a measured speedup or physical
RAM saving; the identity case is itself trivial for a competent specialized
control. It exists to test the general evaluator's accounting, not to supply
an impressive benchmark. A [lead-recorded receipt](evidence/community-implicit-dual-20260921/receipt.json)
pins the prototype/tests and the completed independent review. Reproduction:

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest test_implicit_partition_dual -q
```

The lead replay of the independent, self-contained checker passes 11,718
group cases, 18,940 partition cases plus 200 rational cases, 999 basis checks,
two exact-LP witnesses and eight projected-schedule recurrence checks. Its
implementation imports no lead probe. These counts establish the tested
contracts, not historical priority or physical performance.

## Promotion Gate

The arithmetic and constructed separation gates above pass. Tiny tests
cannot establish wall-clock or physical memory wins. Since no useful native ambiguity
survives the elementary ablation, do not rerun the saturated three-candidate
gate and announce a new gain. A later gate needs a genuine unresolved decision,
paid optimization rounds, exact replay control and the same output contract.
The indistinguishable-residual example from the prior review still applies:
no dual search can recover information absent from its summary.

The next bounded study should freeze an expanded family produced only from
the retained summary (multiple declared visit orders/seeds, charged O(CK)
labels), apply support/endpoint and selected subset-potential controls first,
then run fixed search schedules only on unresolved comparisons. Preserve all
refusals; exact source replay remains the fallback and supplies correctness
oracles. A benefit must survive candidate-construction and optimization costs.
If ordinary subset controls account for the gain or replay is cheaper, retain
the mathematical result without calling it a product or paper breakthrough.

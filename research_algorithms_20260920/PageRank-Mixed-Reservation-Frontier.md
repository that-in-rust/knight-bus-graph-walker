# PageRank Mixed Reservation Admission Frontier

Date: 2026-09-20. Follow-on to [anisotropic release](PageRank-Anisotropic-Release.md). Exact bounds for a specified scalar-reservation model, not measured process RAM, a new max-flow theorem, or global solver optimality.

## Question

The new minimum-cut orientation planner minimizes surviving supplied factors plus additive prices. The mixed positive evaluator's peak scalar bill also depends on the larger of the row and column release sets. That term is not additive. Does the cut nevertheless provide a useful, rigorously bounded memory-admission decision? Yes, with a narrow additive guarantee and an explicit unresolved region.

The conventional option enumerates all 2^k orientations to minimize the bill. Three alternatives are: optimize only surviving factors by one cut; sweep uniform orientation prices through multiple cuts; or use a cut-based lower/upper admission interval and add independently priced candidate plans. The third is selected because it exposes uncertainty instead of calling a factor-count minimum a RAM minimum. A counterexample below defeats the uniform-price sweep as a complete optimizer.

Expert lenses: resource scheduling, graph-representable costs, nonnegative operator correctness and skeptical admission accounting. This is analytical follow-on research, not a report of independent review.

## Fixed Contract

Hold U,V,Q,a, the selected set J of size k, and the numerical scalar format fixed. Let r be the supplied factor count, R the row-release subset of J, C=J-R, ell=|R|, and r0(R) the number of surviving original factors. Preserve the same post-release Q, original degrees and residual/error contract in every orientation. Use the declared nonempty-star condition when claiming actual factor-count optimality; otherwise all k star slots remain reserved even if some might be removable.

The existing three-phase mixed schedule reserves

```text
B(R) = 2*r0(R) + 2*r + 3*k + 4*max(ell,k-ell) + 4
```

scalar slots. This expression includes both old/new coordinate arrays, original-factor scratch, selected state and prefix/suffix/decode buffers. It excludes the flow residual network, IDs, indexes, scalar-width certification, runtime and output buffers. Those are separately admitted. Different orientations can also have different numerical or I/O costs; this theorem does not optimize them.

The specialized pure schedules reserve

```text
B_row = 2*r0(J) + r + 7*k + 4.
B_col = 2*r0(empty) + r + 7*k + 4.
```

These cheaper specialized bills must not be replaced by the mixed schedule's conservative pure-orientation bills. Independent review supplied B_col by omitting g_off and phase 3 when R is empty; the original draft had unnecessarily excluded this available plan. The current portfolio is exactly all mixed-schedule orientations plus both pure specializations. Another decoder or eliminated empty-star slots would enlarge it further and require revisiting the bound.

## Cut-Based Admission Theorem

Set base survival prices to one and orientation prices to the same constant, or zero. The cut construction in the preceding note returns a minimizer R_cut of r0. Write r_min=r0(R_cut), and define

```text
L_mixed = 2*r_min + 2*r + 3*k + 4*ceil(k/2) + 4
U_mixed = B(R_cut)

L = min(L_mixed, B_row, B_col)
U = min(U_mixed, B_row, B_col)
OPT = min(min_R B(R), B_row, B_col).
```

Then

```text
L <= OPT <= U
U - OPT <= 2*k
U/OPT <= 1 + 2*k/(2*r+5*k+4) < 7/5  when k>0.
```

For k=0 the interval is exact. If the m=1 release theorem establishes k<=r, the ratio bound is at most 9/7, strictly less for finite positive r. More generally, k<=m*r gives the upper limit (7m+2)/(5m+2). These are scalar-reservation ratios for this portfolio, not byte/RSS/latency ratios.

**Proof.** Every mixed plan has r0>=r_min and max(ell,k-ell)>=ceil(k/2), proving L_mixed is a lower bound. The cut plan is executable under the same contract, proving U_mixed is an upper bound. Let R* minimize B. Because r0(R_cut)<=r0(R*),

```text
B(R_cut)-B(R*)
 <= 4*(max(|R_cut|,k-|R_cut|)-max(|R*|,k-|R*|))
 <= 4*floor(k/2) <= 2*k.
```

Taking the minimum with the same known B_row and B_col preserves lower/upper validity. If a specialized pure plan is optimal, U=OPT. Otherwise OPT is a mixed plan with OPT>=2r+5k+4, giving the displayed ratio. Substitute k<=m*r and maximize the resulting fraction to obtain the tier bound. No promise about the cut's tie choice is needed; the proof includes its worst tie choice.

The computable width U-L is at most 4*floor(k/2), and may be much smaller after considering both pure schedules. Additional verified orientations can lower U without affecting L. Do not increase L merely because a heuristic failed to find a better orientation.

### Three-Way Scalar Admission

For a scalar allowance H:

| Condition | Defensible conclusion |
| --- | --- |
| H >= U | A concrete candidate schedule fits this scalar allowance. Still admit all non-scalar and planning costs. |
| H < L | No schedule in this precisely defined portfolio fits its stated scalar reservation. This is not proof that no implementation or solver fits. |
| L <= H < U | Unresolved by this interval; search further, try an additional candidate, or report that admission is not established. |

The test separates a certificate of feasible reservation from a predicted RSS and separates a lower bound on a chosen reservation formula from an information-theoretic memory lower bound. Those distinctions are central to the deterministic-compute product thesis.

## A Strict Factor-Minimum Versus Buffer-Minimum Conflict

For even k>=4, let n=k+1, J={0,...,k-1}, and use r=2 factors:

```text
U_i = V_i = (1,1)  for i in J
U_k = V_k = (1,0)  for the outside vertex
Q_ii = 2 for i in J; Q_kk=0
A = U V^T - Q.
```

The graph is nonnegative. Each selected column has d=2k-1 and q/d=2/(2k-1)>1/k; the outside column has q=0. Thus J is exactly the mandatory heavy set at t=1/k. Both required star nonemptiness conditions hold because every selected vertex has an actual edge in each direction to the outside vertex.

The global factor always survives. The J-only factor survives exactly when both R and C are nonempty:

```text
r0(empty)=r0(J)=1
r0(R)=2 whenever 0<|R|<k.
```

Every factor-minimum cut therefore returns a pure orientation. Yet

```text
specialized all-row bill = 7*k+8
pure mixed-schedule bill = 7*k+10
balanced mixed bill     = 5*k+12.
```

For k>=4 the balanced plan uses fewer reserved scalars even than either specialized pure kernel, both of which cost 7k+8 here, despite retaining one more factor. At k=4 the bills are 36 versus 32. Both plans have the same post-release Q'=0 and contraction cap a. This is a buffer-lifetime tradeoff, not improved convergence. Independent review notes that `(7k+8)/(5k+12)` tends to 7/5, making the universal ratio constant asymptotically sharp for this cut-plus-pure planner and reservation portfolio.

### Why A Uniform-Price Cut Sweep Can Miss The Best Plan

Consider minimizing r0(R)+lambda*|R| over all real lambda, with a constant shift if needed to encode nonnegative unary capacities. When lambda>=0, the empty R has smaller objective than every nonempty proper R; when lambda<=0, the full R has smaller objective than every proper R. At lambda=0 both pure sets win and every proper set is strictly worse. Hence **no** uniform-price choice returns a balanced plan in this family. Sampling more lambda values cannot repair this omission.

This refutes completeness of that specific Lagrangian sweep, not all parametric flow approaches or every polynomial memory planner. The obstruction is not the earlier empty-star example: every restored star here is nonempty. It also does not assert NP-hardness of the reservation objective.

## Numerical And Physical Boundaries

1. All comparisons use the same declared scalar width. Orientation-dependent validated precision can change the byte ordering and must be priced separately.
2. A sparse flow network still has O(k+r+selected-incidence) nodes/arcs and residual bookkeeping. Its construction may not fit even when U scalar slots would fit during execution.
3. The same supplied factorization and J are fixed. Extra release vertices, refactorization, low-rank direct/Krylov solves, sparse original-space iteration and different kernels are outside OPT, not proven inferior.
4. h/next, selected columns, old generation roots and output lifetimes must obey the contracts in the preceding notes. Counting fewer abstract factors does not waive those live allocations.
5. The cheap interval is useful when an admitted cut planner already runs. Paying that planner solely for a small scalar saving may lose to a simpler pure plan; compare complete build and solve time.

## Prior-Art Position

The [preceding note's primary-source inspection](PageRank-Anisotropic-Release.md#primary-prior-art-inspection) covers stochastic-factor geometry and graph-representable binary objectives. This extension does not claim to invent minimum cuts, Lagrangian relaxation, lower/upper bounds or time-space tradeoffs. Its local contribution is the explicit reservation sandwich for this positive PageRank evaluator and the concrete nonempty-star family separating factor minimization from buffer minimization. The elementary inequality alone is not a claim of publication novelty. A paper must assess the combined admission/planning/execution result against same-factor solvers and inspect any closest nonlinear storage-planning literature.

## Executable New Probe

This probe enumerates only small orientation spaces as an oracle; the proposed planner remains the earlier cut algorithm. It does not rerun any old solver tests. The first global factor ensures nonempty stars and positive selected diagonal/degrees; chosen thresholds make exactly J heavy. It checks the sandwich under the worst tie among all r0 minimizers, not just a favorable cut output.

```python
from fractions import Fraction as F
from itertools import combinations
from random import Random

def enumerate_selected_orientation_sets(k):
    for bits in range(1 << k):
        yield {i for i in range(k) if bits >> i & 1}

def count_surviving_original_factors(U, V, R, C):
    return sum(any(row[f] and i not in R for i, row in enumerate(U))
               and any(row[f] and i not in C for i, row in enumerate(V))
               for f in range(len(U[0])))

def inspect_mixed_reservation_interval(U, V, k):
    r, J = len(U[0]), set(range(k))
    plans = []
    for R in enumerate_selected_orientation_sets(k):
        r0 = count_surviving_original_factors(U, V, R, J-R)
        bill = 2*r0+2*r+3*k+4*max(len(R), k-len(R))+4
        plans.append((R, r0, bill))
    minimum = min(r0 for _, r0, _ in plans)
    cut_bill = max(bill for _, r0, bill in plans if r0 == minimum)
    row_bill = 2*count_surviving_original_factors(U, V, J, set())+r+7*k+4
    col_bill = 2*count_surviving_original_factors(U, V, set(), J)+r+7*k+4
    lower = min(2*minimum+2*r+3*k+4*((k+1)//2)+4, row_bill, col_bill)
    upper = min(cut_bill, row_bill, col_bill)
    optimum = min(row_bill, col_bill, min(bill for _, _, bill in plans))
    assert lower <= optimum <= upper
    assert upper-optimum <= 2*k and upper-lower <= 4*(k//2)
    assert F(upper, optimum) <= 1+F(2*k, 2*r+5*k+4)
    if k <= r:
        assert F(upper, optimum) <= F(9, 7)
    return lower, optimum, upper, plans

rng, checked, largest_ratio = Random(521), 0, F(1)
for k in range(1, 7):
    n = k+1
    for _ in range(40):
        r = rng.randrange(2, 6)
        U = [[1]+[rng.randrange(2) for _ in range(r-1)] for _ in range(n)]
        V = [[1]+[rng.randrange(2) for _ in range(r-1)] for _ in range(n)]
        W = [[sum(x*y for x, y in zip(u, v)) for v in V] for u in U]
        q = [W[j][j] if j < k else 0 for j in range(n)]
        d = [sum(W[i][j] for i in range(n))-q[j] for j in range(n)]
        t = min(F(q[j], d[j]) for j in range(k))/2
        assert {j for j in range(n) if q[j] > t*d[j]} == set(range(k))
        lower, optimum, upper, _ = inspect_mixed_reservation_interval(U, V, k)
        largest_ratio = max(largest_ratio, F(upper, optimum)); checked += 1

family = []
for k in (4, 6, 8, 10):
    U = [[1, 1] for _ in range(k)]+[[1, 0]]
    lower, optimum, upper, plans = inspect_mixed_reservation_interval(U, U, k)
    assert optimum == 5*k+12 and upper == 7*k+8
    assert all(r0 == (1 if len(R) in (0, k) else 2) for R, r0, _ in plans)
    for price in (F(-100), F(-1), F(-1, 1000), F(0), F(1, 1000), F(1), F(100)):
        value = min(r0+price*len(R) for R, r0, _ in plans)
        assert all(len(R) in (0, k) for R, r0, _ in plans if r0+price*len(R) == value)
    family.append((k, lower, optimum, upper))

print('reservation_interval_fixtures', checked, 'maximum_observed_ratio', str(largest_ratio))
print('strict_family_k_lower_optimum_upper', family)
print('uniform_price_sweep_family=misses_balanced_optimum')
column = inspect_mixed_reservation_interval([[1,1],[1,0],[0,1]],
                                           [[1,0],[0,1],[0,0]], 1)
assert column[:3] == (15, 15, 15)
print('reviewed_allcolumn_interval', column[:3], 'previous_portfolio=17')
```

## Verification Receipt

Lead execution of the retained new probe exited 0 on 2026-09-20: 240 random interval fixtures, maximum observed upper/optimum ratio 13/10. Strict-family `(k,L,OPT,U)` receipts were `(4,30,32,36)`, `(6,40,42,50)`, `(8,50,52,64)` and `(10,60,62,78)`. The finite uniform-price samples also passed; the proof, not those samples, covers every real price. The theoretical statements require the stated model; this finite probe establishes neither physical memory, numerical precision nor global novelty.

The subsequent [factor-conditioned planner](PageRank-Factor-Conditioned-Planner.md) develops an exact alternative parameterized by factors that can disappear, rather than by all selected vertices. It retains this cut interval as the cheaper fallback when exact planning work is not admitted.

After completed independent review added the all-column specialization, the retained probe was revised for the enlarged portfolio and executed successfully: the same 240 random cases have maximum observed ratio 5/4; all four strict-family intervals remain unchanged. The explicit column witness now has `(L,OPT,U)=(15,15,15)`, instead of 17 in the older portfolio. The review separately proves asymptotic sharpness of 7/5. Old and new receipts describe different portfolios; they are not contradictory measurements.

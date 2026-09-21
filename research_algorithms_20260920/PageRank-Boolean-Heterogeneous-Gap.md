# Boolean PageRank: A Checked Gap For Unequal Pair Classes

Date: 2026-09-21. Derived follow-on, not integrated into the publisher or
independently reviewed. No physical-RAM, timing or publication-priority claim.

## Why This Is The Next Question

The uniform all-pair family is sufficiently symmetric to have an inexpensive
exact inverse. A residual certificate there must face that strong control.
If group-pair multiplicities vary, the same uniform inverse is generally wrong,
but the original streamed F-coordinate Boolean solver still applies. This note
derives a cheap sufficient spectral-gap bound for that broader source domain.

It deliberately removes one restriction, not all restrictions: every pair is
still present, membership remains exactly two groups, and the graph is an
undirected unweighted Boolean union. Real customer prevalence is unestablished.
Ordinary block elimination, reversible spectral comparison and mass deflation
are credited as established ingredients. The combined workflow's useful delta
and scientific positioning remain questions to test, not conclusions.

## Source And Proposed Bound

There are F >= 4 groups and P=F(F-1)/2 pair classes. Pair class c={a,b}
contains h_c >= 1 original vertices. Write

```text
N_a   = sum_{c incident a} h_c
d_c   = N_a + N_b - h_c - 1
m     = min_c h_c
M     = max_c h_c
d_max = max_c d_c
vol   = sum_c h_c*d_c

delta = min(1, m*m*F / (M*d_max))
lambda_nonstationary <= 1-delta.
```

Here lambda is the largest algebraic nonstationary eigenvalue of the original
random-walk operator. It is not an estimated absolute eigenvalue. The source is
connected and degrees are positive under the all-pair hypotheses.

The formula is a sufficient lower gap bound, not the exact gap for unequal
classes. When all h_c=h, it reduces to min(1,hF/d), matching the uniform family
for F>=4. For fixed M/m its denominator does not contain 1-alpha.

## Derivation

Use the reversible function operator D^-1 A, which has the same eigenvalues as
the column-stochastic PageRank operator A D^-1. Work in the degree-weighted
inner product. Since all vertices in a class have identical external neighbors
and identical degree, functions split orthogonally into two invariant spaces.

**Within-class zero-sum space.** On a nonzero vector supported in a class with
sum zero, adjacency acts as minus identity: every other class sees zero total,
and each vertex sees the negative of its own value inside the clique. The
transition eigenvalue is -1/d_c, so its normalized Laplacian eigenvalue is
1+1/d_c > 1. Any proposed gap at most one is safe on these modes.

**Class-constant space.** Let y_c be the value assigned to all vertices of class
c. The unnormalized Dirichlet energy and weighted variance are

```text
E(y) = sum_{unordered c~e} h_c*h_e*(y_c-y_e)^2
W(y) = min_t sum_c h_c*d_c*(y_c-t)^2.
```

The class adjacency c~e means that distinct pairs share one group. It is the
triangular graph J(F,2). Its combinatorial Laplacian gap is F. This can be
derived rather than assumed: for the F-by-P unsigned pair-incidence matrix B,
BB^T=(F-2)I+J and A_class=B^T B-2I. Subtracting from degree 2(F-2) gives
Laplacian eigenvalues 0, F and 2F-2 on the constant, factor-contrast and
incidence-null spaces, respectively. No such dense matrix is needed in a
streaming implementation.

For the unweighted mean y_bar,

```text
E(y) >= m^2 * sum_{unordered c~e} (y_c-y_e)^2
     >= m^2 * F * sum_c (y_c-y_bar)^2

W(y) <= sum_c h_c*d_c*(y_c-y_bar)^2
     <= M*d_max * sum_c (y_c-y_bar)^2.
```

Therefore E(y)/W(y) >= m^2 F/(M d_max) on every nonconstant class function.
Combining invariant spaces proves the clipped gap delta. The use of a minimum
in W is essential: the unweighted mean is an admissible comparison center,
not an assertion that it equals the stationary mean.

The graph identification is standard; inspected 2026-09-21:
[Brouwer, Johnson graphs](https://aeb.win.tue.nl/graphs/Johnson.html).
The gap derivation and comparison above are provided explicitly; the website
is not cited as containing this unequal-clone bound. Stationary-mode separation
has substantial precedent, including the inspected spectral decomposition in
[PageRank in Undirected Random Graphs](https://arxiv.org/pdf/1703.08057), as
documented in the [completed mass review](PageRank-Boolean-Mass-Deflation-Review.md).
This is not a claimed new Poincare comparison theorem.

## Proposed Actual-Output Certificate

For a vertex in class c, stationary pi_v=d_c/vol. With actual output x and
r=(1-alpha)p+alpha*A*D^-1*x-x, the existing general mathematical theorem gives

```text
s   = sum_v x_v
rho = (1-alpha)*(1-s)
V   = sum_v r_v^2 * vol/d_class(v) - rho^2

error_L1 <= abs(s-1) + sqrt(V) / [(1-alpha) + alpha*delta].
```

The new degree weighting cannot be replaced by n: unequal class sizes usually
make degrees unequal. The uniform actual-byte verifier must reject this source
until both the new eligibility check and the weighted variance are implemented.
This note does not quietly broaden that implementation's contract.

One possible source validator retains F exact group population counters in a
first canonical-class scan. A second class scan checks every claimed degree,
counts original vertices, and computes m, M, d_max and vol. Both scans, F counter
bit widths, exact arithmetic and original source construction are charged.
These counters can be released before the residual certificate's 2F Decimal
endpoints are allocated. P scratch and two complete output reads still remain.

## What Can Falsify Its Usefulness

- A large M/m makes the bound too conservative: delta scales at worst with the
  square of that ratio under the simple d_max bound. Correctness survives while
  practical admission can fail.
- Missing a pair can break the triangular-graph comparison. Merely inserting
  a zero height into the same formula is not an accepted generalization.
- A uniform exact inverse is not a valid control after changing h, but general
  dense/F-coordinate solves and equally informed comparison certificates remain
  mandatory controls. Do not claim exclusivity over them.
- A useful scalar error bound can still be dominated by source validation,
  scratch, full-output work and a better numerical implementation.
- All-pair completeness itself may be unrealistic for useful customer graphs.
  Better source coverage, not more synthetic sizes alone, is needed.

## Exact Finite Check

The checker below expands small graphs and checks that

```text
L - delta * (D - d*d^T/vol)
```

is positive semidefinite over exact rational arithmetic. This verifies the
claimed gap on every function for each enumerated graph, not only a few
sampled eigenvectors. It enumerates all 64 F=4 height assignments in {1,2},
16 seeded F=5 assignments in {1,2,3}, and three uniform reference cases.
An intentionally overclaimed gap must fail. It imports no project module and
writes no data. The dense checker is not a memory-bounded algorithm.

```python
from fractions import Fraction as Q
from itertools import combinations, product
import random


def check_rational_positive_semidefinite(matrix):
    a = [[Q(value) for value in row] for row in matrix]
    for k in range(len(a)):
        if a[k][k] < 0:
            return False
        if a[k][k] == 0:
            if any(a[k][j] != 0 for j in range(k+1, len(a))):
                return False
            continue
        for i in range(k+1, len(a)):
            for j in range(i, len(a)):
                a[i][j] -= a[i][k]*a[k][j]/a[k][k]
                a[j][i] = a[i][j]
    return True


def construct_heterogeneous_gap_matrix(factors, heights, forced=None):
    pairs = list(combinations(range(factors), 2))
    signatures = [pair for pair, height in zip(pairs, heights) for _ in range(height)]
    n = len(signatures)
    adjacency = [[int(i != j and bool(set(a).intersection(b)))
                  for j, b in enumerate(signatures)] for i, a in enumerate(signatures)]
    degrees = list(map(sum, adjacency))
    group_counts = [sum(height for pair, height in zip(pairs, heights) if a in pair)
                    for a in range(factors)]
    expanded_degrees = [group_counts[a]+group_counts[b]-height-1
                        for (a,b), height in zip(pairs, heights) for _ in range(height)]
    assert degrees == expanded_degrees and min(degrees) > 0
    delta = min(Q(1), Q(min(heights)**2*factors, max(heights)*max(degrees)))
    if forced is not None:
        delta = forced
    volume = sum(degrees)
    matrix = [[Q((degrees[i] if i == j else 0)-adjacency[i][j])
               - delta*(Q(degrees[i] if i == j else 0)-Q(degrees[i]*degrees[j], volume))
               for j in range(n)] for i in range(n)]
    return matrix, delta


cases = [(4, heights) for heights in product((1,2), repeat=6)]
rng = random.Random(20260921)
cases += [(5, tuple(rng.randrange(1,4) for _ in range(10))) for _ in range(16)]
cases += [(f, (h,)*(f*(f-1)//2)) for f,h in ((4,3),(5,2),(6,1))]
for factors, heights in cases:
    matrix, delta = construct_heterogeneous_gap_matrix(factors, heights)
    assert delta > 0 and check_rational_positive_semidefinite(matrix)
    if len(set(heights)) == 1:
        h = heights[0]
        assert delta == Q(h*factors, h*(2*factors-3)-1)

matrix, _ = construct_heterogeneous_gap_matrix(5, (1,)*10, forced=Q(1))
assert not check_rational_positive_semidefinite(matrix)
print('Exact whole-space heterogeneous gap checks:', len(cases))
print('Overclaimed uniform gap negative control: rejected')
print('No production certificate, timing or physical memory claim.')
```

Run from the repository root:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Heterogeneous-Gap.md | /Users/amuldotexe/.local/bin/python3.11 -B
```

Lead execution with Python 3.11, bytecode writes disabled, exited 0:

```text
Exact whole-space heterogeneous gap checks: 83
Overclaimed uniform gap negative control: rejected
No production certificate, timing or physical memory claim.
```

This supports the derived bound on those 83 graphs. Independent mathematical
review, unequal-degree actual-byte integration and useful-source evaluation
remain open. It does not establish a novel comparison principle or a faster
complete solver.

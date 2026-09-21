# Native Direct PageRank: Independent Bounded Review

Date: 2026-09-21. Scope: the mathematics in
[Direct Controls](PageRank-Native-Direct-Controls.md), with
[Native Tree Gap](PageRank-Boolean-Native-Tree-Gap.md) used only for the gap
contract and native degree semantics. No implementation files inspected or
changed; no new code audit, timings, resource validation, or novelty claim.

## Decision

**GO for both mathematical controls, subject to the certification obligations
below. NO-GO for unconditional admission or an implementation-correctness claim
from this review.** Neither displayed formula needs an algebraic correction.

- The stationary bound uses the **algebraic** gap of the original reversible
  walk. Bipartite graphs are eligible; an absolute mixing gap is unnecessary.
- Admission must include the distance of **every actual output value** from
  the exact stationary or clique target. Zero bias does not certify the file.
- Directed arithmetic must enclose subtraction, division, absolute distance,
  and square root, not just set a rounding mode. In particular, Decimal's
  square root can round down even in a ceiling context.
- Clique eligibility requires validated original simple-Boolean degrees,
  complete class coverage, and `n >= 2`. Equal degrees alone are insufficient.

These are design-level conditions, not findings of bugs in uninspected code.

## Stationary Bound

Require a finite, connected, undirected graph with positive degrees; exact
`0 <= a < 1`; nonnegative weights with positive total `T`; `p_v=w_v/T`;
`V=sum(d_v)`; and `pi_v=d_v/V`. The prerequisite provides a lower bound
`0 < delta <= lambda_2(L_normalized)` for the **original** graph, not its
auxiliary class walk. This review assumes that gap contract, without
re-reviewing its tree-congestion proof.

Define `||z||_(pi^-1)^2=sum(z_v^2/pi_v)`. The similarity transform
`diag(pi)^(-1/2) W diag(pi)^(1/2)=D^(-1/2) A D^(-1/2)` is symmetric.
Zero-mass vectors become perpendicular to `sqrt(pi)`. On that subspace,
transition eigenvalues obey `-1 <= lambda <= 1-delta`, hence

```text
q = (1-a)+a*delta > 0
||(I-aW)^-1||_(pi^-1),zero-mass <= 1/q
x* - pi = (1-a)(I-aW)^-1(p-pi)
K = ||p-pi||_(pi^-1)^2 = sum(p_v^2/pi_v)-1
||z||_1 <= sqrt(sum(pi_v))*||z||_(pi^-1) = ||z||_(pi^-1)
||x*-pi||_1 <= (1-a)*sqrt(K)/q = B.
```

There is no factor of two: this is L1, not total variation. At `a=0`,
`x*=p` and `B=sqrt(K)`. Gaps greater than one are legitimate: the clique
gap is `n/(n-1)`, including `2` for `K_2`. A proved gap clipped at one remains
valid but weaker.

**Algebraic versus absolute gap.** The relevant gap is `1-lambda_2(W)`,
with eigenvalues ordered algebraically. The absolute gap
`1-max_{i>=2}|lambda_i(W)|` is zero on a connected bipartite graph. That
prevents an ordinary non-lazy mixing argument, not this resolvent argument:
the `-1` mode has denominator `1+a`. No laziness or non-bipartite assumption
may be silently introduced. Reversibility is essential here; the same
eigenvalue argument does not certify a general nonnormal directed walk.

**Actual output.** For every finite vector `y`, even without unit mass,

```text
||y-x*||_1 <= D_y+B,       D_y=sum_v |y_v-d_v/V|.
```

Nonnegative values are an output-contract requirement, not needed for this
triangle inequality. `D_y` already accounts for mass error; no extra
`|sum(y)-1|` is necessary. Mass error alone cannot replace `D_y`: a wrong
unit-mass vector can have substantial error. IDs, missing/duplicate rows,
nonfinite values, and snapshot binding remain mandatory independent checks.
The bound is sufficient, not necessary; failure to admit does not prove that
an answer is inaccurate.

**Zero moment.** `K>=0`, with equality exactly when `p=pi`. Equivalently,
`V*w_v=T*d_v` at every vertex. Then `x*=pi` for every eligible damping and
`B=0`; actual-byte distance can still be positive. Do not turn a small
negative rounded moment into a mathematical zero. A certified upper moment
may be conservatively clipped to zero only after its enclosure is justified;
an allegedly negative upper bound signals a violated invariant. An exact
zero test avoids unnecessary refusal, but a positive overestimate is safe.
Small `1-a` alone cannot guarantee admission for arbitrary personalization,
source size, gap, precision, or tolerance.

## Clique Formula And Eligibility

For the loopless simple clique on `n>=2`, `W=(J-I)/(n-1)` and `sum(x*)=1`.
Thus `(Wx*)_v=(1-x*_v)/(n-1)` and rearrangement gives

```text
x*_v = [(1-a)*(n-1)*p_v+a]/(n-1+a).
```

The denominator is positive, the entries sum to one, `a=0` gives `p`,
and uniform personalization gives `1/n`. This holds for nonuniform `p`
and for bipartite `K_2`. `a=1` and the isolated `n=1` case remain outside
the stated contract even though some algebraic limits exist.

For native pair class `c={u,v}` with positive height `h_c`, recompute
`N_u=sum_{c incident u} h_c`, `n=sum(h_c)`, and
`d_c=N_u+N_v-h_c-1`. After validating source coverage, canonical distinct
pairs, totals, and this identity for **every** class, `d_c=n-1` for all
classes proves the original graph is complete: every original vertex has
all other `n-1` vertices as neighbors. Advertised degrees, regularity alone,
or group-graph completeness do not prove this. For example, a unit group
`K_4` gives original degree `4` on `6` records, not a clique. A group star
does give a record-space clique, including unequal positive class heights.

For actual output, certify `sum_v |y_v-x*_v|` directly. No
`1/(1-a)` amplification is necessary. A nonclique may not use this formula
merely because a particular personalization happens to give the right answer.

## Arithmetic Obligations

1. Fix the exact input semantics for `a`, weights, and tolerance. Interpret
   stored binary64 output as its exact dyadic value, not a short decimal
   rendering. Python's exact float conversion and context-dependent
   operations are documented in the [Decimal reference](https://docs.python.org/3/library/decimal.html#decimal.Decimal.from_float).
2. Compute/enclose `K=(V/T^2)*sum_v(w_v^2/d_v)-1`. Keep integer totals exact;
   form an upper second-moment sum and subtract exact one upward. For
   class-constant weights use `h_c*w_c^2/d_c`, not `(h_c*w_c)^2/d_c`.
   With varying weights inside a class, aggregate their **squares**, not
   merely their first-moment sum. An upper denominator is unsafe for an
   upper quotient.
3. Use a positive **lower** bound `q_lo` on `(1-a)+a*delta` and upper
   bounds on `1-a`, `sqrt(K)`, the final quotient, and the final sum.
   Enclose `1-a` before cancellation can erase it. If `a` itself is
   interval-valued, propagate the interval: the factor
   `(1-a)/(1-a+a*delta)` decreases with both `a` and `delta`.
4. Enclose the exact target in `[l_v,u_v]`. With exact `y_v`, a safe distance
   contribution is `max(0, up(y_v-l_v), up(u_v-y_v))`, accumulated upward.
   This applies to both stationary and clique targets. Computing
   `abs(up(y_v-target))` is unsafe when the signed difference is negative.
   A rounded approximation to the target is not an exact reference.
5. **Square-root warning:** the [General Decimal Arithmetic specification,
   square-root](https://speleotrove.com/decimal/daops.html#refsqrt) mandates
   half-even rounding, ignoring the context rounding setting. At precision
   three, even a ceiling context returns `sqrt(2)=1.41`, whose exact square
   is below two. Use an outward adjustment such as `next_plus` after the
   correctly rounded root, or an independently proved upper root; retain
   exact zero when established. A rounded-up square is not proof that the
   unrounded square exceeds the radicand.
6. Pin precision, exponent bounds, rounding, traps, and flags independently
   of ambient settings. Overflow, invalid operations, lost lower bounds,
   or unenclosed underflow must cause refusal, not acceptance. Compare an
   upper error to an exact or lower-enclosed tolerance. Ordinary nearest
   conversion of a final receipt must not turn it into an underreported
   upper certificate. These requirements do not validate any implementation.

## Bounded Primary Prior-Art Check

The full HTML of [Grolmusz, *A Note on the PageRank of Undirected Graphs*,
arXiv:1205.1960v2](https://arxiv.org/html/1205.1960) was accessible and inspected.
Equation (5) already gives the stationary-difference resolvent identity.
Corollary 1 gives exact equality for stationary personalization. Theorem 2,
in the notation of this review, states

```text
(1-a)/(1+a) * ||p-pi||_1 <= ||x*-pi||_1 <= ||p-pi||_1.
```

Although the introduction discusses a non-bipartite ordinary-walk limit,
Theorem 2 is stated for a stochastic matrix and its stationary distribution;
it does not require aperiodicity. Its `a` range is `(0,1)`; the `a=0`
endpoint here follows directly.

The proposed gap-dependent weighted-norm estimate is not the displayed
Theorem 2 bound and need not beat its upper bound for every personalization.
It is a standard reversible spectral-resolvent consequence, not new math.
The clique expression is a direct specialization of the PageRank linear
equation, not a claimed discovery. Two targeted searches (clique closed
form; undirected spectral stationary comparison) did not establish an
earliest source for those exact expressions. No broader survey or priority
clearance is claimed. No PDF retries were needed. Arithmetic references
above are primary specification/documentation, not PageRank novelty evidence.

## Standalone Independent Rational Checker

Run this block with Python 3. It imports only the standard library and does
not read project code. Dense storage is confined to this finite oracle.
It enumerates connected simple labeled graphs on 2--4 vertices, validates
each chosen rational gap by an exact positive-semidefinite test, solves the
original PageRank equations by rational elimination, and compares squared
inequalities without approximating square roots. It also checks clique
eligibility against expanded native fixtures and demonstrates the Decimal
square-root hazard. It is not a production-verifier or lifecycle test.

```python
from fractions import Fraction as Q
from itertools import combinations, product
from decimal import Context, Decimal, ROUND_CEILING


def solve_rational_linear_system(matrix, rhs):
    rows = [list(map(Q, row)) + [Q(value)]
            for row, value in zip(matrix, rhs)]
    size = len(rows)
    for col in range(size):
        pivot = next(i for i in range(col, size) if rows[i][col])
        rows[col], rows[pivot] = rows[pivot], rows[col]
        divisor = rows[col][col]
        rows[col] = [value / divisor for value in rows[col]]
        for i in range(size):
            if i != col:
                factor = rows[i][col]
                rows[i] = [x - factor*y for x, y in zip(rows[i], rows[col])]
    return [row[-1] for row in rows]


def verify_rational_matrix_psd(matrix):
    rows = [list(map(Q, row)) for row in matrix]
    while rows:
        pivot = rows[0][0]
        if pivot < 0 or (pivot == 0 and any(rows[0])):
            return False
        rows = [[rows[i][j] - (rows[i][0]*rows[0][j]/pivot if pivot else 0)
                 for j in range(1, len(rows))] for i in range(1, len(rows))]
    return True


def measure_exact_vector_distance(left, right):
    return sum(abs(x-y) for x, y in zip(left, right))


graphs = cases = outputs = cliques = 0
alphas = (Q(0), Q(1, 2), Q(17, 20), Q(2**53-1, 2**53))
for n in range(2, 5):
    pairs = list(combinations(range(n), 2))
    for bits in product((0, 1), repeat=len(pairs)):
        adjacency = [[0]*n for _ in range(n)]
        for (i, j), present in zip(pairs, bits):
            adjacency[i][j] = adjacency[j][i] = present
        seen = {0}
        for _ in range(n):
            seen |= {j for i in tuple(seen) for j in range(n) if adjacency[i][j]}
        if len(seen) != n:
            continue
        degrees = list(map(sum, adjacency))
        volume = sum(degrees)
        pi = [Q(d, volume) for d in degrees]
        clique = all(d == n-1 for d in degrees)
        assert clique == all(bits)
        # Conservative gap, strengthened only for cliques and complete bipartite cases.
        delta = Q(n, n-1) if clique else Q(1, volume*(n-1))
        if sorted(degrees) in ([1, 1, 2], [1, 1, 1, 3], [2, 2, 2, 2]):
            delta = Q(1)
        gap_matrix = [[Q((degrees[i] if i == j else 0)-adjacency[i][j])
                       - delta*(Q(degrees[i] if i == j else 0)
                                - Q(degrees[i]*degrees[j], volume))
                       for j in range(n)] for i in range(n)]
        assert verify_rational_matrix_psd(gap_matrix)
        basis = [[Q(i == j) for i in range(n)] for j in range(n)]
        personalizations = [pi, [Q(1, n)]*n,
                            [Q(i+1, n*(n+1)//2) for i in range(n)]] + basis
        for p, a in product(personalizations, alphas):
            matrix = [[Q(i == j)-a*Q(adjacency[i][j], degrees[j])
                       for j in range(n)] for i in range(n)]
            x = solve_rational_linear_system(matrix, [(1-a)*v for v in p])
            assert sum(x) == 1 and min(x) >= 0
            assert all(sum(matrix[i][j]*x[j] for j in range(n)) == (1-a)*p[i]
                       for i in range(n))
            moment = sum(v*v/t for v, t in zip(p, pi))-1
            assert moment >= 0 and (moment == 0) == (p == pi)
            bound_squared = (1-a)**2*moment / ((1-a)+a*delta)**2
            assert sum((v-t)**2/t for v, t in zip(x, pi)) <= bound_squared
            assert measure_exact_vector_distance(x, pi)**2 <= bound_squared
            if moment == 0:
                assert x == pi and bound_squared == 0
            for y in (pi, [Q(float(v)) for v in pi], [2*v for v in pi],
                      basis[0], [Q(0)]*n, [Q(float(v)) for v in x]):
                distance = measure_exact_vector_distance(y, pi)
                excess = max(Q(0), measure_exact_vector_distance(y, x)-distance)
                assert excess**2 <= bound_squared
                outputs += 1
            if clique:
                target = [((1-a)*(n-1)*v+a)/(n-1+a) for v in p]
                assert x == target
                cliques += 1
            cases += 1
        graphs += 1

# Native signatures: unequal star, triangle, path, and group K4.
fixtures = [([(0, 1, 1), (0, 2, 2), (0, 3, 3)], True),
            ([(0, 1, 1), (0, 2, 2), (1, 2, 1)], True),
            ([(0, 1, 1), (1, 2, 1), (2, 3, 1)], False),
            ([(i, j, 1) for i, j in combinations(range(4), 2)], False)]
for classes, expected in fixtures:
    populations = {}
    signatures = []
    for i, j, height in classes:
        populations[i] = populations.get(i, 0)+height
        populations[j] = populations.get(j, 0)+height
        signatures.extend([frozenset((i, j))]*height)
    native_degrees = [populations[i]+populations[j]-height-1
                      for i, j, height in classes for _ in range(height)]
    expanded_degrees = [sum(k != m and bool(left & right)
                            for m, right in enumerate(signatures))
                        for k, left in enumerate(signatures)]
    assert native_degrees == expanded_degrees
    assert all(d == len(signatures)-1 for d in native_degrees) == expected

# Zero bias cannot certify arbitrary output, even one with the correct mass.
assert measure_exact_vector_distance([Q(1), Q(0)], [Q(1, 2)]*2) == 1
assert measure_exact_vector_distance([Q(0)]*2, [Q(1, 2)]*2) == 1
# Finite counterexample to treating CEILING sqrt as an upper bound.
context = Context(prec=3, rounding=ROUND_CEILING, Emin=-99, Emax=99,
                  capitals=1, clamp=0, flags=[], traps=[])
root = context.sqrt(Decimal(2))
assert Q(root)**2 < 2 <= Q(context.next_plus(root))**2
print('PASS graphs:', graphs, 'PageRank cases:', cases,
      'actual-output bounds:', outputs, 'clique cases:', cliques)
print('PASS native degree fixtures:', len(fixtures),
      'zero-bias counterexamples: 2; ceiling-sqrt counterexample:', root)
```

## Verification Receipt

Fresh execution of the embedded block with `python3 -B` exited 0:

```text
PASS graphs: 43 PageRank cases: 1180 actual-output bounds: 7080 clique cases: 72
PASS native degree fixtures: 4 zero-bias counterexamples: 2; ceiling-sqrt counterexample: 1.41
```

Reproduce directly from the repository root without creating a script file:

```sh
awk '/^```python$/{inside=1; next} inside && /^```$/{inside=0; next} inside {print}' research_algorithms_20260920/PageRank-Native-Direct-Review.md | python3 -B
```

Finite checks supplement the proof and do not establish production
correctness, speed, memory savings, or novelty. Bounded review complete;
the only authored file is this review.
